"""
include/extract/aqi.py
-----------------------
Fetches Air Quality Index (AQI) data from the WAQI / aqicn.org API
for a single city and stores the verbatim raw JSON in MongoDB.

API used : https://api.waqi.info/feed/geo:{lat};{lng}/?token={token}
Cost     : Free tier — token required (sign up at https://aqicn.org/data-platform/token/)
Docs     : https://aqicn.org/json-api/doc/

Why one city at a time (unlike Open-Meteo batching)?
  The WAQI API has NO multi-location batch endpoint.  Every city requires
  its own HTTP request.  This is the reason this extractor is designed to
  be called via Airflow's dynamic task mapping — each city becomes its own
  Airflow task, running independently and retrying independently.

WAQI response shape (the parts we care about):
  {
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
        "s":  "2024-09-26 14:00:00",   ← station's local time as a string
        "tz": "+05:30"                  ← station's UTC offset
      }
    }
  }

Edge cases handled:
  - status != "ok"           → AQI station not found / API error
  - data == "Unknown station" → city has no monitoring station nearby
  - aqi == "-"               → station reported no value this hour
  - Missing iaqi keys        → not all pollutants are measured everywhere
"""

import logging
import os
from datetime import datetime, timezone
from typing import Any

import requests

from include.utils.db import get_mongo_db
from include.utils.retry import retry_on_http_error

logger = logging.getLogger(__name__)

WAQI_BASE_URL    = "https://api.waqi.info/feed/geo:{lat};{lng}/"
COLLECTION_NAME  = "raw_aqi_readings"


# ── Public entry point ───────────────────────────────────────────────────────

def fetch_and_store_aqi(city: dict, airflow_run_id: str) -> dict[str, Any]:
    """
    Fetch AQI for a single city and store the raw response in MongoDB.

    This function is intentionally scoped to ONE city so it can be called
    via Airflow's dynamic task mapping — one Airflow task per city.

    Parameters
    ----------
    city : dict
        Must have: city_id (int), city_name (str),
                   latitude (float), longitude (float)
    airflow_run_id : str
        Airflow run ID for document traceability.

    Returns
    -------
    dict:
        {
          "city_id":   int,
          "city_name": str,
          "status":    "ok" | "no_station" | "no_data" | "error",
          "stored":    bool   — whether a document was written to MongoDB
        }
    """
    fetched_at = datetime.now(timezone.utc)

    try:
        raw_response = _fetch_aqi(city["latitude"], city["longitude"])
    except Exception as exc:
        logger.error(
            "AQI fetch failed for %s after all retries: %s",
            city["city_name"], exc,
        )
        return {
            "city_id":   city["city_id"],
            "city_name": city["city_name"],
            "status":    "error",
            "stored":    False,
        }

    # ── Validate the response ────────────────────────────────────────────────
    status = raw_response.get("status")

    if status != "ok":
        # Covers "error" status and "Unknown station" data values
        logger.warning(
            "WAQI returned non-ok status for %s: %s",
            city["city_name"], raw_response,
        )
        return {
            "city_id":   city["city_id"],
            "city_name": city["city_name"],
            "status":    "no_station",
            "stored":    False,
        }

    aqi_value = raw_response.get("data", {}).get("aqi")
    if aqi_value == "-" or aqi_value is None:
        # Station exists but reported no reading this hour
        logger.warning("No AQI data available for %s this hour", city["city_name"])
        return {
            "city_id":   city["city_id"],
            "city_name": city["city_name"],
            "status":    "no_data",
            "stored":    False,
        }

    # ── Store verbatim in MongoDB ────────────────────────────────────────────
    _store_document(city, raw_response, fetched_at, airflow_run_id)

    logger.info(
        "AQI stored for %s — AQI value: %s",
        city["city_name"], aqi_value,
    )
    return {
        "city_id":   city["city_id"],
        "city_name": city["city_name"],
        "status":    "ok",
        "stored":    True,
    }


# ── Private helpers ──────────────────────────────────────────────────────────

@retry_on_http_error    # exponential back-off on transient failures
def _fetch_aqi(lat: float, lng: float) -> dict:
    """
    Make a single GET request to the WAQI API for the given coordinates.

    The token is read from the environment variable WAQI_TOKEN, which is
    injected by docker-compose.yaml from the .env file.  It is never
    hardcoded.

    Raises requests.exceptions.HTTPError for non-2xx responses (triggers retry).
    """
    token = os.environ.get("WAQI_TOKEN", "")
    if not token:
        raise EnvironmentError(
            "WAQI_TOKEN environment variable is not set. "
            "Add it to your .env file (get a free token at https://aqicn.org/data-platform/token/)"
        )

    url = WAQI_BASE_URL.format(lat=lat, lng=lng)
    response = requests.get(url, params={"token": token}, timeout=30)
    response.raise_for_status()
    return response.json()


def _store_document(
    city: dict,
    raw_response: dict,
    fetched_at: datetime,
    airflow_run_id: str,
) -> None:
    """
    Upsert one raw AQI document into MongoDB.

    Upsert key: (city_id, fetched_hour)
    Same idempotency pattern as the weather extractor — DAG retries within
    the same hour update the existing document instead of creating duplicates.
    """
    fetched_hour = fetched_at.replace(minute=0, second=0, microsecond=0)

    document = {
        "city_id":        city["city_id"],
        "city_name":      city["city_name"],
        "fetched_at":     fetched_at,
        "source_api":     "waqi",
        "airflow_run_id": airflow_run_id,
        "raw_response":   raw_response,   # verbatim — zero transformation
    }

    filter_key = {
        "city_id":      city["city_id"],
        "fetched_hour": fetched_hour,
    }

    db = get_mongo_db()
    db[COLLECTION_NAME].update_one(
        filter=filter_key,
        update={"$set": {**document, "fetched_hour": fetched_hour}},
        upsert=True,
    )
