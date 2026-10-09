"""
include/transform/weather.py
-----------------------------
Reads raw weather documents from MongoDB and transforms them into clean
dicts ready for INSERT into fact_weather_reading.

Raw Open-Meteo document shape (stored in MongoDB):
  {
    "city_id":     1,
    "city_name":   "Mumbai",
    "fetched_at":  ISODate("2024-09-26T08:30:00Z"),
    "source_api":  "open_meteo",
    "airflow_run_id": "scheduled__2024-09-26T03:00:00+00:00",
    "raw_response": {
      "latitude":  19.076,
      "longitude": 72.877,
      "timezone":  "Asia/Kolkata",
      "current": {
        "time":                 "2024-09-26T14:30",   ← local time, NO offset
        "temperature_2m":       32.5,
        "relative_humidity_2m": 78,
        "precipitation":        0.0,
        "surface_pressure":     1008.5,
        "wind_speed_10m":       15.2
      }
    }
  }

Output dict shape (one per document, ready for fact_weather_reading):
  {
    "city_id":          1,
    "date_key":         20240926,       ← YYYYMMDD integer
    "reading_ts":       datetime(..., tzinfo=Asia/Kolkata),
    "temperature_c":    32.5,
    "humidity_pct":     78.0,
    "precipitation_mm": 0.0,
    "pressure_hpa":     1008.5,
    "wind_speed_kmh":   15.2,
    "fetched_at":       datetime(...),
  }
"""

import logging
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from include.utils.db import get_mongo_db

logger = logging.getLogger(__name__)


def transform_weather_run(airflow_run_id: str) -> list[dict]:
    """
    Read all raw weather documents for a given Airflow run from MongoDB
    and transform them into clean dicts for the fact table.

    Documents that fail to parse are skipped with a warning — a single
    malformed API response should not abort the entire transform step.

    Parameters
    ----------
    airflow_run_id : str
        Filters MongoDB to only process documents from this DAG run.

    Returns
    -------
    list[dict] : Clean fact rows.  May be shorter than the raw document
                 count if some documents failed to parse.
    """
    db = get_mongo_db()
    raw_docs = list(
        db["raw_weather_readings"].find({"airflow_run_id": airflow_run_id})
    )
    logger.info(
        "Found %d raw weather documents for run '%s'",
        len(raw_docs), airflow_run_id,
    )

    clean_rows: list[dict] = []
    for doc in raw_docs:
        try:
            row = _parse_weather_doc(doc)
            if row:
                clean_rows.append(row)
        except Exception as exc:
            logger.warning(
                "Failed to parse weather doc for city_id=%s: %s",
                doc.get("city_id"), exc,
            )

    logger.info(
        "Weather transform: %d / %d documents parsed successfully",
        len(clean_rows), len(raw_docs),
    )
    return clean_rows


# ── Private helpers ──────────────────────────────────────────────────────────

def _parse_weather_doc(doc: dict) -> Optional[dict]:
    """
    Extract and clean one raw weather MongoDB document.

    Returns None if the document is structurally invalid (missing `current`
    block) so the caller can skip it.
    """
    raw = doc.get("raw_response", {})
    current = raw.get("current")
    if not current:
        logger.warning(
            "city_id=%s has no 'current' block in raw_response — skipping",
            doc.get("city_id"),
        )
        return None

    reading_ts = _parse_timestamp(
        time_str=current.get("time", ""),
        timezone_str=raw.get("timezone", "UTC"),
    )
    if reading_ts is None:
        return None

    return {
        "city_id":          doc["city_id"],
        "date_key":         int(reading_ts.strftime("%Y%m%d")),
        "reading_ts":       reading_ts,
        "temperature_c":    _safe_float(current.get("temperature_2m")),
        "humidity_pct":     _safe_float(current.get("relative_humidity_2m")),
        "precipitation_mm": _safe_float(current.get("precipitation")),
        "pressure_hpa":     _safe_float(current.get("surface_pressure")),
        "wind_speed_kmh":   _safe_float(current.get("wind_speed_10m")),
        "fetched_at":       doc.get("fetched_at"),
    }


def _parse_timestamp(time_str: str, timezone_str: str) -> Optional[datetime]:
    """
    Parse Open-Meteo's local time string into a timezone-aware datetime.

    Open-Meteo returns `current.time` as a local time string with NO offset:
      "2024-09-26T14:30"
    and the timezone separately:
      "Asia/Kolkata"

    We use Python's built-in `zoneinfo` (Python 3.9+) to attach the
    timezone, producing a proper timezone-aware datetime.

    Why not just store it as UTC?  We store it WITH the timezone so
    that PostgreSQL's TIMESTAMPTZ can handle any display conversion later.
    """
    if not time_str:
        logger.warning("Empty time string — cannot parse timestamp")
        return None

    try:
        tz = ZoneInfo(timezone_str)
    except ZoneInfoNotFoundError:
        logger.warning("Unknown timezone '%s', falling back to UTC", timezone_str)
        tz = ZoneInfo("UTC")

    try:
        naive_dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M")
        return naive_dt.replace(tzinfo=tz)
    except ValueError as exc:
        logger.warning("Could not parse time '%s': %s", time_str, exc)
        return None


def _safe_float(value) -> Optional[float]:
    """
    Convert a value to float, returning None for missing / non-numeric values.
    Prevents a single bad measurement from crashing the entire row parse.
    """
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
