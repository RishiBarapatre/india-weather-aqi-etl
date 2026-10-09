"""
include/transform/city_resolver.py
------------------------------------
Resolves a raw city name (messy, historical, or aliased) to a canonical
city_id in dim_city.  This is the four-step algorithm from the spec:

  Step 1 — Normalize   : strip whitespace, lowercase
  Step 2 — Exact match : look up dim_city_alias (fast O(1) dict lookup)
  Step 3 — Fuzzy match : rapidfuzz token_sort_ratio >= 90 against dim_city
                         On match → write back to dim_city_alias so future
                         runs resolve instantly (the "learning" part)
  Step 4 — Quarantine  : write to quarantine_unmatched_city_names and return None

Usage:
    resolver = CityResolver(conn)
    city_id = resolver.resolve("Bombay")      # returns 1 (Mumbai's id)
    city_id = resolver.resolve("Bengaluru")   # fuzzy match → Bengaluru
    city_id = resolver.resolve("Navi Mumbai") # → None, quarantined
"""

import logging
from typing import Optional

from rapidfuzz import process as fuzz_process, fuzz

logger = logging.getLogger(__name__)

# Minimum score (0-100) for a fuzzy match to be accepted.
# token_sort_ratio handles word-order differences: "Mumbai City" ≈ "City Mumbai"
FUZZY_THRESHOLD = 90


class CityResolver:
    """
    Stateful resolver that caches the alias table and city name list
    for the lifetime of a single task run — avoids repeated DB queries.

    Instantiate once per Airflow task, not once per row.
    """

    def __init__(self, conn):
        """
        Parameters
        ----------
        conn : psycopg2 connection to the warehouse database.
               CityResolver does NOT own this connection — the caller is
               responsible for closing it.
        """
        self._conn = conn
        # Load lookup tables into memory once at construction time
        self._alias_map: dict[str, int] = self._load_alias_map()
        self._city_names: dict[str, int] = self._load_city_names()

    # ── Public API ───────────────────────────────────────────────────────────

    def resolve(self, raw_name: str) -> Optional[int]:
        """
        Resolve a raw city name to a city_id.

        Returns city_id (int) on success, None if unresolvable (quarantined).
        """
        if not raw_name or not raw_name.strip():
            return None

        normalised = raw_name.strip().lower()

        # Step 2 — Exact alias lookup
        city_id = self._alias_map.get(normalised)
        if city_id is not None:
            logger.debug("Exact alias match: '%s' → city_id=%d", raw_name, city_id)
            return city_id

        # Step 3 — Fuzzy match
        city_id = self._fuzzy_match(raw_name, normalised)
        if city_id is not None:
            return city_id

        # Step 4 — Quarantine
        self._quarantine(raw_name)
        return None

    # ── Private helpers ──────────────────────────────────────────────────────

    def _load_alias_map(self) -> dict[str, int]:
        """
        Load dim_city_alias into a plain dict: normalised_alias → city_id.
        Also includes city names from dim_city itself so canonical names
        resolve without needing an alias row.
        """
        alias_map: dict[str, int] = {}

        with self._conn.cursor() as cur:
            # Load existing aliases
            cur.execute("SELECT raw_alias, canonical_city_id FROM dim_city_alias")
            for row in cur.fetchall():
                alias_map[row[0].strip().lower()] = row[1]

            # Also map canonical names directly (e.g. "mumbai" → 1)
            cur.execute("SELECT city_id, city_name FROM dim_city WHERE is_active = TRUE")
            for row in cur.fetchall():
                alias_map[row[1].strip().lower()] = row[0]

        logger.debug("Loaded %d alias entries into CityResolver cache", len(alias_map))
        return alias_map

    def _load_city_names(self) -> dict[str, int]:
        """
        Load dim_city names into a dict: canonical_city_name → city_id.
        Used as the candidate pool for rapidfuzz matching.
        """
        city_names: dict[str, int] = {}
        with self._conn.cursor() as cur:
            cur.execute("SELECT city_id, city_name FROM dim_city WHERE is_active = TRUE")
            for row in cur.fetchall():
                city_names[row[1]] = row[0]
        return city_names

    def _fuzzy_match(self, raw_name: str, normalised: str) -> Optional[int]:
        """
        Use rapidfuzz to find the closest canonical city name.

        rapidfuzz.process.extractOne returns (match, score, key).
        token_sort_ratio is robust to word-order differences:
          "Nagar Haveli" ≈ "Haveli Nagar"  (score ~100)
          "Bengaluru"    ≈ "Bengaluru"     (score 100 vs. "Bangalore" → ~85)
        """
        candidates = list(self._city_names.keys())
        result = fuzz_process.extractOne(
            raw_name,
            candidates,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=FUZZY_THRESHOLD,
        )

        if result is None:
            logger.debug("No fuzzy match found for '%s'", raw_name)
            return None

        matched_name, score, _ = result
        city_id = self._city_names[matched_name]

        logger.info(
            "Fuzzy match: '%s' → '%s' (score=%d, city_id=%d)",
            raw_name, matched_name, score, city_id,
        )

        # Write back to alias table so the NEXT run resolves this instantly
        self._write_fuzzy_alias(raw_name, normalised, city_id)

        # Update in-memory cache too so subsequent rows in THIS run benefit
        self._alias_map[normalised] = city_id

        return city_id

    def _write_fuzzy_alias(self, raw_name: str, normalised: str, city_id: int) -> None:
        """
        Insert a new alias row so this fuzzy match becomes an exact lookup
        in all future pipeline runs.  ON CONFLICT DO NOTHING = safe to call
        multiple times.
        """
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dim_city_alias (raw_alias, canonical_city_id, match_method)
                VALUES (%s, %s, 'fuzzy')
                ON CONFLICT (raw_alias) DO NOTHING
                """,
                (raw_name, city_id),
            )
        self._conn.commit()

    def _quarantine(self, raw_name: str) -> None:
        """
        Record an unresolvable city name in the quarantine table.
        Uses INSERT ... ON CONFLICT to increment the occurrence counter
        rather than inserting a duplicate row.
        """
        logger.warning("Could not resolve city name '%s' — quarantining", raw_name)

        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO quarantine_unmatched_city_names
                    (raw_name, source, occurrence_count, last_seen_at)
                VALUES (%s, 'pipeline', 1, NOW())
                ON CONFLICT (raw_name) DO UPDATE SET
                    occurrence_count = quarantine_unmatched_city_names.occurrence_count + 1,
                    last_seen_at     = NOW()
                """,
                (raw_name,),
            )
        self._conn.commit()
