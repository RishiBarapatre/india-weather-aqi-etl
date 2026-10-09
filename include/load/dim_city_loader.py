"""
include/load/dim_city_loader.py
--------------------------------
Loads cleaned city data into dim_city and dim_city_alias.

Design principles:
  - Every INSERT uses ON CONFLICT ... DO UPDATE (upsert) so the DAG is
    fully idempotent — running it twice produces the same result as once.
  - Returns a name → city_id mapping so the DAG can link aliases to their
    canonical city without a second round-trip to the database.
"""

import logging
from typing import Any

import psycopg2.extras

from include.utils.db import get_warehouse_conn, get_warehouse_cursor

logger = logging.getLogger(__name__)


def load_cities(cleaned_cities: list[dict]) -> dict[str, int]:
    """
    Upsert a list of cleaned city dicts into dim_city.

    Parameters
    ----------
    cleaned_cities : list[dict]
        Output of include.transform.cities.clean_cities().
        Each dict must have: city_name, state, latitude, longitude,
        tier (optional), population (optional).

    Returns
    -------
    city_name_to_id : dict[str, int]
        Maps canonical city_name → city_id assigned by the database.
        Used by load_aliases() to link aliases to their city.
    """
    if not cleaned_cities:
        logger.warning("load_cities called with empty list — nothing to do")
        return {}

    # The upsert SQL:
    # - ON CONFLICT (city_name, state): if a row with this name+state already
    #   exists, UPDATE it rather than raising a duplicate-key error.
    # - DO UPDATE SET ...: refresh coordinates and population in case they
    #   changed in the source CSV (e.g. updated census figures).
    # - RETURNING city_id, city_name: get back the IDs so we can build the
    #   name→id map without a second SELECT query.
    upsert_sql = """
        INSERT INTO dim_city
            (city_name, state, latitude, longitude, tier, population)
        VALUES
            (%(city_name)s, %(state)s, %(latitude)s, %(longitude)s,
             %(tier)s, %(population)s)
        ON CONFLICT (city_name, state) DO UPDATE SET
            latitude   = EXCLUDED.latitude,
            longitude  = EXCLUDED.longitude,
            tier       = EXCLUDED.tier,
            population = EXCLUDED.population,
            updated_at = NOW()
        RETURNING city_id, city_name
    """

    city_name_to_id: dict[str, int] = {}

    with get_warehouse_conn() as conn:
        with get_warehouse_cursor(conn) as cur:
            # executemany sends all rows in a single round-trip — much faster
            # than calling execute() in a Python loop.
            psycopg2.extras.execute_batch(cur, upsert_sql, cleaned_cities, page_size=100)

            # Fetch back all city_id values we just upserted
            # (execute_batch doesn't return results, so we SELECT after)
            cur.execute("SELECT city_id, city_name FROM dim_city WHERE is_active = TRUE")
            for row in cur.fetchall():
                city_name_to_id[row["city_name"]] = row["city_id"]

        conn.commit()

    logger.info("Upserted %d cities into dim_city", len(cleaned_cities))
    return city_name_to_id


def load_aliases(
    alias_seeds: list[dict],
    city_name_to_id: dict[str, int],
) -> int:
    """
    Pre-seed dim_city_alias with historical name → canonical city mappings.

    Parameters
    ----------
    alias_seeds : list[dict]
        Each dict has: raw_alias (str), canonical (str), match_method (str).
        Output of include.transform.cities.clean_cities().
    city_name_to_id : dict[str, int]
        Map from load_cities() — used to resolve canonical name → city_id.

    Returns
    -------
    int : number of aliases inserted / updated.
    """
    if not alias_seeds:
        logger.info("No alias seeds to load")
        return 0

    # Resolve canonical name → city_id; skip any we can't resolve
    rows_to_insert = []
    for seed in alias_seeds:
        canonical = seed["canonical"]
        city_id = city_name_to_id.get(canonical)
        if city_id is None:
            logger.warning(
                "Alias seed '%s' → '%s': canonical city not found in dim_city, skipping",
                seed["raw_alias"], canonical,
            )
            continue
        rows_to_insert.append({
            "raw_alias":         seed["raw_alias"],
            "canonical_city_id": city_id,
            "match_method":      seed["match_method"],
        })

    if not rows_to_insert:
        return 0

    # ON CONFLICT (raw_alias): if this alias already exists, do nothing.
    # We never want to overwrite a manually-corrected alias with an auto one.
    upsert_sql = """
        INSERT INTO dim_city_alias (raw_alias, canonical_city_id, match_method)
        VALUES (%(raw_alias)s, %(canonical_city_id)s, %(match_method)s)
        ON CONFLICT (raw_alias) DO NOTHING
    """

    with get_warehouse_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, upsert_sql, rows_to_insert, page_size=100)
        conn.commit()

    logger.info("Seeded %d aliases into dim_city_alias", len(rows_to_insert))
    return len(rows_to_insert)
