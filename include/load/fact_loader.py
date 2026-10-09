"""
include/load/fact_loader.py
-----------------------------
Loads clean transformed rows into fact_weather_reading and
fact_air_quality_reading using idempotent upserts.

Both loaders follow the same pattern:
  1. Use psycopg2.extras.execute_batch for efficient bulk inserts
  2. ON CONFLICT (city_id, reading_ts) DO UPDATE → upsert semantics
  3. Return a summary dict so the DAG can log and pass stats downstream

The UNIQUE constraint on (city_id, reading_ts) in both fact tables is
what makes the upsert work — if the same city+timestamp pair is loaded
twice (e.g., DAG retry, backfill), the row is updated instead of
duplicated.  The warehouse always converges to the correct state.
"""

import logging
from typing import Any

import psycopg2.extras

from include.utils.db import get_warehouse_conn

logger = logging.getLogger(__name__)


# ── Weather facts ────────────────────────────────────────────────────────────

def load_weather_facts(rows: list[dict]) -> dict[str, Any]:
    """
    Upsert clean weather rows into fact_weather_reading.

    Parameters
    ----------
    rows : list[dict]
        Output of include.transform.weather.transform_weather_run().
        Each dict must have: city_id, date_key, reading_ts, temperature_c,
        humidity_pct, precipitation_mm, pressure_hpa, wind_speed_kmh, fetched_at.

    Returns
    -------
    dict : {"rows_loaded": int, "rows_rejected": int}
    """
    if not rows:
        logger.info("No weather rows to load")
        return {"rows_loaded": 0, "rows_rejected": 0}

    valid_rows, rejected = _validate_rows(
        rows,
        required_fields=["city_id", "date_key", "reading_ts"],
    )

    if not valid_rows:
        logger.warning("All %d weather rows failed validation", len(rows))
        return {"rows_loaded": 0, "rows_rejected": rejected}

    upsert_sql = """
        INSERT INTO fact_weather_reading
            (city_id, date_key, reading_ts,
             temperature_c, humidity_pct, precipitation_mm,
             pressure_hpa, wind_speed_kmh, fetched_at)
        VALUES
            (%(city_id)s, %(date_key)s, %(reading_ts)s,
             %(temperature_c)s, %(humidity_pct)s, %(precipitation_mm)s,
             %(pressure_hpa)s, %(wind_speed_kmh)s, %(fetched_at)s)
        ON CONFLICT (city_id, reading_ts) DO UPDATE SET
            temperature_c    = EXCLUDED.temperature_c,
            humidity_pct     = EXCLUDED.humidity_pct,
            precipitation_mm = EXCLUDED.precipitation_mm,
            pressure_hpa     = EXCLUDED.pressure_hpa,
            wind_speed_kmh   = EXCLUDED.wind_speed_kmh,
            fetched_at       = EXCLUDED.fetched_at
    """

    with get_warehouse_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, upsert_sql, valid_rows, page_size=100)
        conn.commit()

    logger.info(
        "Loaded %d weather rows into fact_weather_reading (%d rejected)",
        len(valid_rows), rejected,
    )
    return {"rows_loaded": len(valid_rows), "rows_rejected": rejected}


# ── AQI facts ────────────────────────────────────────────────────────────────

def load_aqi_facts(rows: list[dict]) -> dict[str, Any]:
    """
    Upsert clean AQI rows into fact_air_quality_reading.

    Parameters
    ----------
    rows : list[dict]
        Output of include.transform.aqi.transform_aqi_run().
        Each dict must have: city_id, date_key, reading_ts, aqi_value,
        and optionally: pm25, pm10, no2, so2, co, o3, dominant_pollutant.

    Returns
    -------
    dict : {"rows_loaded": int, "rows_rejected": int}
    """
    if not rows:
        logger.info("No AQI rows to load")
        return {"rows_loaded": 0, "rows_rejected": 0}

    valid_rows, rejected = _validate_rows(
        rows,
        required_fields=["city_id", "date_key", "reading_ts", "aqi_value"],
    )

    if not valid_rows:
        logger.warning("All %d AQI rows failed validation", len(rows))
        return {"rows_loaded": 0, "rows_rejected": rejected}

    upsert_sql = """
        INSERT INTO fact_air_quality_reading
            (city_id, date_key, reading_ts,
             aqi_value, pm25, pm10, no2, so2, co, o3,
             dominant_pollutant, fetched_at)
        VALUES
            (%(city_id)s, %(date_key)s, %(reading_ts)s,
             %(aqi_value)s, %(pm25)s, %(pm10)s, %(no2)s,
             %(so2)s, %(co)s, %(o3)s,
             %(dominant_pollutant)s, %(fetched_at)s)
        ON CONFLICT (city_id, reading_ts) DO UPDATE SET
            aqi_value          = EXCLUDED.aqi_value,
            pm25               = EXCLUDED.pm25,
            pm10               = EXCLUDED.pm10,
            no2                = EXCLUDED.no2,
            so2                = EXCLUDED.so2,
            co                 = EXCLUDED.co,
            o3                 = EXCLUDED.o3,
            dominant_pollutant = EXCLUDED.dominant_pollutant,
            fetched_at         = EXCLUDED.fetched_at
    """

    with get_warehouse_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, upsert_sql, valid_rows, page_size=100)
        conn.commit()

    logger.info(
        "Loaded %d AQI rows into fact_air_quality_reading (%d rejected)",
        len(valid_rows), rejected,
    )
    return {"rows_loaded": len(valid_rows), "rows_rejected": rejected}


# ── Shared helpers ────────────────────────────────────────────────────────────

def _validate_rows(
    rows: list[dict],
    required_fields: list[str],
) -> tuple[list[dict], int]:
    """
    Filter out rows that are missing required fields.

    Returns (valid_rows, rejected_count).
    Logs a warning for each rejected row so we can investigate later.
    """
    valid: list[dict] = []
    rejected = 0

    for row in rows:
        missing = [f for f in required_fields if row.get(f) is None]
        if missing:
            logger.warning(
                "Rejecting row city_id=%s — missing required fields: %s",
                row.get("city_id"), missing,
            )
            rejected += 1
        else:
            valid.append(row)

    return valid, rejected
