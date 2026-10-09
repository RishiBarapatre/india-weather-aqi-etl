"""
include/transform/aqi.py
-------------------------
Reads raw AQI documents from MongoDB and transforms them into clean
dicts ready for INSERT into fact_air_quality_reading.

Raw WAQI document shape (stored in MongoDB):
  {
    "city_id":     1,
    "city_name":   "Mumbai",
    "fetched_at":  ISODate("2024-09-26T08:30:00Z"),
    "source_api":  "waqi",
    "airflow_run_id": "scheduled__...",
    "raw_response": {
      "status": "ok",
      "data": {
        "aqi": 87,
        "dominentpol": "pm25",
        "iaqi": {
          "pm25": {"v": 32.5},
          "pm10": {"v": 45.0},
          "no2":  {"v": 12.3},
          "so2":  {"v": 5.1},
          "co":   {"v": 0.8},
          "o3":   {"v": 22.1}
        },
        "time": {
          "s":  "2024-09-26 14:00:00",   ← station local time
          "tz": "+05:30"                  ← UTC offset string (WITH colon)
        }
      }
    }
  }

Output dict shape (ready for fact_air_quality_reading):
  {
    "city_id":           1,
    "date_key":          20240926,
    "reading_ts":        datetime(..., tzinfo=+05:30),
    "aqi_value":         87,
    "pm25":              32.5,
    "pm10":              45.0,
    "no2":               12.3,
    "so2":               5.1,
    "co":                0.8,
    "o3":                22.1,
    "dominant_pollutant": "pm25",
    "fetched_at":        datetime(...),
  }
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from include.utils.db import get_mongo_db

logger = logging.getLogger(__name__)


def transform_aqi_run(airflow_run_id: str) -> list[dict]:
    """
    Read all raw AQI documents for a given Airflow run from MongoDB
    and transform them into clean dicts for the fact table.

    Only processes documents where `status = "ok"` and `stored = True`
    were set by the extractor (i.e., documents with a valid AQI reading).

    Parameters
    ----------
    airflow_run_id : str
        Filters MongoDB to only documents from this specific DAG run.

    Returns
    -------
    list[dict] : Clean fact rows ready for fact_air_quality_reading.
    """
    db = get_mongo_db()
    raw_docs = list(
        db["raw_aqi_readings"].find({"airflow_run_id": airflow_run_id})
    )
    logger.info(
        "Found %d raw AQI documents for run '%s'",
        len(raw_docs), airflow_run_id,
    )

    clean_rows: list[dict] = []
    for doc in raw_docs:
        try:
            row = _parse_aqi_doc(doc)
            if row:
                clean_rows.append(row)
        except Exception as exc:
            logger.warning(
                "Failed to parse AQI doc for city_id=%s: %s",
                doc.get("city_id"), exc,
            )

    logger.info(
        "AQI transform: %d / %d documents parsed successfully",
        len(clean_rows), len(raw_docs),
    )
    return clean_rows


# ── Private helpers ──────────────────────────────────────────────────────────

def _parse_aqi_doc(doc: dict) -> Optional[dict]:
    """
    Extract and clean one raw AQI MongoDB document.

    Returns None for:
      - Missing or non-ok status
      - AQI value of "-" (station offline)
      - Unparseable timestamp
    """
    raw = doc.get("raw_response", {})

    if raw.get("status") != "ok":
        return None  # already filtered by extractor, but defensive check

    data = raw.get("data", {})
    aqi_value = data.get("aqi")

    if aqi_value == "-" or aqi_value is None:
        logger.debug("Skipping city_id=%s — no AQI reading", doc.get("city_id"))
        return None

    reading_ts = _parse_timestamp(data.get("time", {}))
    if reading_ts is None:
        return None

    # Extract individual pollutants — each is nested as {"v": float}
    # `.get("pm25", {}).get("v")` returns None if the key is missing entirely
    iaqi = data.get("iaqi", {})

    return {
        "city_id":            doc["city_id"],
        "date_key":           int(reading_ts.strftime("%Y%m%d")),
        "reading_ts":         reading_ts,
        "aqi_value":          _safe_int(aqi_value),
        "pm25":               _safe_float(iaqi.get("pm25", {}).get("v")),
        "pm10":               _safe_float(iaqi.get("pm10", {}).get("v")),
        "no2":                _safe_float(iaqi.get("no2",  {}).get("v")),
        "so2":                _safe_float(iaqi.get("so2",  {}).get("v")),
        "co":                 _safe_float(iaqi.get("co",   {}).get("v")),
        "o3":                 _safe_float(iaqi.get("o3",   {}).get("v")),
        "dominant_pollutant": data.get("dominentpol"),  # note: API typo "dominent"
        "fetched_at":         doc.get("fetched_at"),
    }


def _parse_timestamp(time_block: dict) -> Optional[datetime]:
    """
    Parse WAQI's time block into a timezone-aware datetime.

    WAQI provides two fields:
      "s":  "2024-09-26 14:00:00"   ← local time at the station
      "tz": "+05:30"                 ← UTC offset with colon separator

    We combine them into a timezone-aware datetime using Python's
    `datetime.strptime` with the `%z` directive, which supports
    the `+HH:MM` format in Python 3.7+.
    """
    time_str  = time_block.get("s", "")
    tz_offset = time_block.get("tz", "+00:00")

    if not time_str:
        logger.warning("Empty time string in WAQI response")
        return None

    try:
        # Remove the colon from offset for Python's %z directive
        # "+05:30" → "+0530"  (strptime %z requires ±HHMM on some platforms)
        tz_clean = tz_offset.replace(":", "")
        combined = f"{time_str} {tz_clean}"
        return datetime.strptime(combined, "%Y-%m-%d %H:%M:%S %z")
    except ValueError as exc:
        logger.warning("Could not parse WAQI timestamp '%s %s': %s", time_str, tz_offset, exc)
        return None


def _safe_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _safe_int(value) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None
