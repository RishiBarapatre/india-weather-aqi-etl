"""
include/quality/checks.py
--------------------------
The 10 automated Data Quality checks from the spec.

Each check is a standalone function with the same signature:
    check_*(conn, load_result) -> dict

Return shape:
    {
      "check_name":  str,           e.g. "range_temperature"
      "table_name":  str | None,    which table was inspected
      "status":      "pass" | "warn" | "fail",
      "details":     str            human-readable explanation
    }

Severity guide:
  pass — everything looks correct
  warn — anomaly detected but data is usable; investigate when convenient
  fail — data is unreliable; pipeline should alert and the task should fail

Checks catalogue:
  1.  schema_presence      — required columns exist in fact tables
  2.  freshness            — latest reading is recent enough
  3.  completeness         — enough cities have data this run
  4.  null_critical_fields — no NULLs in NOT NULL columns (defense-in-depth)
  5.  range_temperature    — temperature_c within [-20, 60] °C
  6.  range_humidity       — humidity_pct within [0, 100] %
  7.  range_aqi            — aqi_value within [0, 500]
  8.  row_count_sanity     — rows loaded meet minimum threshold
  9.  city_resolution      — quarantine table is under control
  10. uniqueness           — no duplicate (city_id, reading_ts) pairs
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Any

logger = logging.getLogger(__name__)

# ── Thresholds (easy to tune without touching logic) ─────────────────────────
MIN_CITIES_PCT_WARN  = 0.70   # warn if fewer than 70% of cities have data
MIN_CITIES_PCT_FAIL  = 0.50   # fail if fewer than 50%
MIN_ROWS_PER_RUN     = 10     # absolute minimum rows expected per run
FRESHNESS_WARN_HOURS = 4      # warn if latest reading is older than this
FRESHNESS_FAIL_HOURS = 6      # fail if older than this
MAX_QUARANTINE_WARN  = 3      # warn if more than 3 unresolved city names
MAX_QUARANTINE_FAIL  = 10     # fail if more than 10


# ── 1. Schema presence ────────────────────────────────────────────────────────

def check_schema_presence(conn, load_result: dict) -> dict:
    """
    Verify that the expected columns exist in both fact tables.
    Guards against accidental schema changes or a failed migration.
    """
    expected = {
        "fact_weather_reading": [
            "reading_id", "city_id", "date_key", "reading_ts",
            "temperature_c", "humidity_pct", "precipitation_mm",
            "pressure_hpa", "wind_speed_kmh",
        ],
        "fact_air_quality_reading": [
            "reading_id", "city_id", "date_key", "reading_ts",
            "aqi_value", "pm25", "pm10", "no2", "so2", "co", "o3",
        ],
    }

    missing: list[str] = []
    with conn.cursor() as cur:
        for table, columns in expected.items():
            cur.execute(
                """
                SELECT column_name
                FROM   information_schema.columns
                WHERE  table_name = %s
                """,
                (table,),
            )
            actual = {row[0] for row in cur.fetchall()}
            for col in columns:
                if col not in actual:
                    missing.append(f"{table}.{col}")

    if missing:
        return _result("schema_presence", None, "fail",
                       f"Missing columns: {missing}")
    return _result("schema_presence", None, "pass",
                   f"All expected columns present in both fact tables")


# ── 2. Freshness ──────────────────────────────────────────────────────────────

def check_freshness(conn, load_result: dict) -> dict:
    """
    Verify that the latest reading in fact_weather_reading is recent.

    The DAG runs every 3 hours, so any reading older than 4 hours indicates
    the pipeline missed a run or the API returned stale data.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(reading_ts) FROM fact_weather_reading")
        latest = cur.fetchone()[0]

    if latest is None:
        return _result("freshness", "fact_weather_reading", "fail",
                       "fact_weather_reading is empty — no readings at all")

    age = datetime.now(timezone.utc) - latest
    hours = age.total_seconds() / 3600

    if hours > FRESHNESS_FAIL_HOURS:
        return _result("freshness", "fact_weather_reading", "fail",
                       f"Latest reading is {hours:.1f}h old (threshold: {FRESHNESS_FAIL_HOURS}h)")
    if hours > FRESHNESS_WARN_HOURS:
        return _result("freshness", "fact_weather_reading", "warn",
                       f"Latest reading is {hours:.1f}h old (warn threshold: {FRESHNESS_WARN_HOURS}h)")

    return _result("freshness", "fact_weather_reading", "pass",
                   f"Latest reading is {hours:.1f}h old ✓")


# ── 3. Completeness ───────────────────────────────────────────────────────────

def check_completeness(conn, load_result: dict) -> dict:
    """
    Check what fraction of active cities have a weather reading in the
    last 4 hours (the current pipeline window).

    A city missing from recent readings could mean its coordinates are
    wrong, or a batch failed silently.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM dim_city WHERE is_active = TRUE")
        total_cities = cur.fetchone()[0]

        if total_cities == 0:
            return _result("completeness", "fact_weather_reading", "fail",
                           "dim_city has no active cities")

        cur.execute(
            """
            SELECT COUNT(DISTINCT city_id)
            FROM   fact_weather_reading
            WHERE  reading_ts >= NOW() - INTERVAL '4 hours'
            """
        )
        cities_with_data = cur.fetchone()[0]

    pct = cities_with_data / total_cities
    detail = f"{cities_with_data}/{total_cities} cities have recent data ({pct:.0%})"

    if pct < MIN_CITIES_PCT_FAIL:
        return _result("completeness", "fact_weather_reading", "fail", detail)
    if pct < MIN_CITIES_PCT_WARN:
        return _result("completeness", "fact_weather_reading", "warn", detail)
    return _result("completeness", "fact_weather_reading", "pass", detail)


# ── 4. Null critical fields ───────────────────────────────────────────────────

def check_null_critical_fields(conn, load_result: dict) -> dict:
    """
    Defense-in-depth: confirm no NULLs exist in columns declared NOT NULL.

    The schema constraints would normally prevent this, but if a migration
    failed or a constraint was dropped, this check catches it.
    """
    checks = [
        ("fact_weather_reading",      "city_id"),
        ("fact_weather_reading",      "reading_ts"),
        ("fact_weather_reading",      "date_key"),
        ("fact_air_quality_reading",  "city_id"),
        ("fact_air_quality_reading",  "reading_ts"),
        ("fact_air_quality_reading",  "aqi_value"),
    ]

    violations: list[str] = []
    with conn.cursor() as cur:
        for table, col in checks:
            cur.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL"
            )
            count = cur.fetchone()[0]
            if count > 0:
                violations.append(f"{table}.{col}: {count} NULLs")

    if violations:
        return _result("null_critical_fields", None, "fail",
                       f"NULL violations found: {violations}")
    return _result("null_critical_fields", None, "pass",
                   "No NULLs in critical columns")


# ── 5. Range: temperature ─────────────────────────────────────────────────────

def check_range_temperature(conn, load_result: dict) -> dict:
    """
    Flag readings where temperature_c is outside the plausible range
    for any Indian city: -20°C to 60°C.

    India's recorded extremes: -55°C (Dras, J&K) to 51°C (Phalodi, 2016).
    We use -20/60 to give a generous buffer for sensor noise.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM fact_weather_reading
            WHERE  temperature_c IS NOT NULL
              AND  (temperature_c < -20 OR temperature_c > 60)
            """
        )
        bad_count = cur.fetchone()[0]

    if bad_count > 0:
        return _result("range_temperature", "fact_weather_reading", "warn",
                       f"{bad_count} readings outside [-20, 60]°C — possible sensor error")
    return _result("range_temperature", "fact_weather_reading", "pass",
                   "All temperature readings within valid range")


# ── 6. Range: humidity ────────────────────────────────────────────────────────

def check_range_humidity(conn, load_result: dict) -> dict:
    """Relative humidity must be 0–100%. Anything outside is physically impossible."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM fact_weather_reading
            WHERE  humidity_pct IS NOT NULL
              AND  (humidity_pct < 0 OR humidity_pct > 100)
            """
        )
        bad_count = cur.fetchone()[0]

    if bad_count > 0:
        return _result("range_humidity", "fact_weather_reading", "fail",
                       f"{bad_count} readings with humidity outside [0, 100]% — data error")
    return _result("range_humidity", "fact_weather_reading", "pass",
                   "All humidity readings within valid range")


# ── 7. Range: AQI ─────────────────────────────────────────────────────────────

def check_range_aqi(conn, load_result: dict) -> dict:
    """
    AQI scale is 0–500. Values above 500 are not defined by any standard
    and indicate a data error rather than extreme pollution.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM fact_air_quality_reading
            WHERE  aqi_value IS NOT NULL
              AND  (aqi_value < 0 OR aqi_value > 500)
            """
        )
        bad_count = cur.fetchone()[0]

    if bad_count > 0:
        return _result("range_aqi", "fact_air_quality_reading", "fail",
                       f"{bad_count} readings with AQI outside [0, 500] — data error")
    return _result("range_aqi", "fact_air_quality_reading", "pass",
                   "All AQI readings within valid range")


# ── 8. Row count sanity ───────────────────────────────────────────────────────

def check_row_count_sanity(conn, load_result: dict) -> dict:
    """
    Confirm that this run loaded at least MIN_ROWS_PER_RUN rows.
    A very low count suggests the extraction or transform step mostly failed.
    """
    weather_loaded = load_result.get("weather_loaded", 0)
    aqi_loaded     = load_result.get("aqi_loaded", 0)

    issues: list[str] = []
    if weather_loaded < MIN_ROWS_PER_RUN:
        issues.append(f"weather: only {weather_loaded} rows (min {MIN_ROWS_PER_RUN})")
    if aqi_loaded < MIN_ROWS_PER_RUN:
        issues.append(f"aqi: only {aqi_loaded} rows (min {MIN_ROWS_PER_RUN})")

    if issues:
        return _result("row_count_sanity", None, "warn",
                       f"Low row counts this run — {'; '.join(issues)}")
    return _result("row_count_sanity", None, "pass",
                   f"Row counts OK — weather: {weather_loaded}, aqi: {aqi_loaded}")


# ── 9. City resolution ────────────────────────────────────────────────────────

def check_city_resolution(conn, load_result: dict) -> dict:
    """
    Check how many unresolved city names are sitting in the quarantine table.
    A growing quarantine table means a real city is missing from dim_city.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM quarantine_unmatched_city_names WHERE resolved = FALSE"
        )
        unresolved = cur.fetchone()[0]

    if unresolved >= MAX_QUARANTINE_FAIL:
        return _result("city_resolution", "quarantine_unmatched_city_names", "fail",
                       f"{unresolved} unresolved city names — add them to dim_city_alias")
    if unresolved >= MAX_QUARANTINE_WARN:
        return _result("city_resolution", "quarantine_unmatched_city_names", "warn",
                       f"{unresolved} unresolved city names — investigate quarantine table")
    return _result("city_resolution", "quarantine_unmatched_city_names", "pass",
                   f"{unresolved} unresolved city names (below threshold)")


# ── 10. Uniqueness ────────────────────────────────────────────────────────────

def check_uniqueness(conn, load_result: dict) -> dict:
    """
    Verify that the UNIQUE (city_id, reading_ts) constraint was honoured.
    COUNT(*) should equal COUNT(DISTINCT city_id, reading_ts) in both tables.

    This would catch a case where a DB migration dropped the constraint
    and upserts silently created duplicates.
    """
    violations: list[str] = []
    with conn.cursor() as cur:
        for table in ("fact_weather_reading", "fact_air_quality_reading"):
            cur.execute(
                f"""
                SELECT
                    COUNT(*)                               AS total,
                    COUNT(DISTINCT (city_id, reading_ts))  AS unique_pairs
                FROM {table}
                """
            )
            row = cur.fetchone()
            total, unique_pairs = row[0], row[1]
            if total != unique_pairs:
                violations.append(
                    f"{table}: {total - unique_pairs} duplicate (city_id, reading_ts) pairs"
                )

    if violations:
        return _result("uniqueness", None, "fail",
                       f"Uniqueness violations: {violations}")
    return _result("uniqueness", None, "pass",
                   "No duplicate (city_id, reading_ts) pairs in either fact table")


# ── Helper ────────────────────────────────────────────────────────────────────

def _result(check_name: str, table_name: Any, status: str, details: str) -> dict:
    """Construct a standardised check result dict."""
    icon = {"pass": "✅", "warn": "⚠️", "fail": "❌"}.get(status, "")
    logger.info("%s %s — %s", icon, check_name, details)
    return {
        "check_name":  check_name,
        "table_name":  table_name,
        "status":      status,
        "details":     details,
    }


# Ordered list used by the runner — order matters for readability in logs
ALL_CHECKS = [
    check_schema_presence,
    check_freshness,
    check_completeness,
    check_null_critical_fields,
    check_range_temperature,
    check_range_humidity,
    check_range_aqi,
    check_row_count_sanity,
    check_city_resolution,
    check_uniqueness,
]
