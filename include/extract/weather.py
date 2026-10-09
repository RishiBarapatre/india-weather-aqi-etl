"""
include/extract/weather.py
---------------------------
Fetches current weather readings from the Open-Meteo API and stores
verbatim raw JSON into MongoDB (raw zone).

API used : https://api.open-meteo.com/v1/forecast
Cost     : Free, no API key required, no rate-limit for reasonable use
Docs     : https://open-meteo.com/en/docs

Batching strategy:
  Open-Meteo supports multi-location requests — you can pass up to ~50
  lat/lon pairs in a single GET request as comma-separated values:
    ?latitude=19.076,28.704,12.971
    &longitude=72.877,77.102,77.594

  This gives us ONE network round-trip for all cities instead of one
  per city (~35 round-trips).  The API returns a JSON array, one element
  per location, in the same order as the input coordinates.

  We split cities into batches of BATCH_SIZE (default 10) to stay well
  within URL length limits and give us partial-failure isolation
  (if one batch fails, others still succeed).

MongoDB document shape (stored verbatim — no transformation):
  {
    "_id":           ObjectId (auto),
    "city_id":       int,
    "city_name":     str,
    "fetched_at":    datetime (UTC),
    "source_api":    "open_meteo",
    "airflow_run_id": str,
    "raw_response":  { ...verbatim API JSON for this city... }
  }
"""

import logging
from datetime import datetime, timezone
from typing import Any

import requests

from include.utils.db import get_mongo_db
from include.utils.retry import retry_on_http_error

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# Variables we ask the API to return under the "current" key
CURRENT_VARIABLES = [
    "temperature_2m",        # air temperature at 2 m above ground (°C)
    "relative_humidity_2m",  # relative humidity at 2 m (%)
    "precipitation",         # precipitation in the last hour (mm)
    "surface_pressure",      # atmospheric pressure at surface (hPa)
    "wind_speed_10m",        # wind speed at 10 m above ground (km/h)
]

# How many cities to include in one API request
BATCH_SIZE = 10

# MongoDB collection name for raw weather documents
COLLECTION_NAME = "raw_weather_readings"


# ── Public entry point ───────────────────────────────────────────────────────

def fetch_and_store_weather(
    cities: list[dict],
    airflow_run_id: str,
) -> dict[str, Any]:
    """
    Fetch current weather for all cities and store raw responses in MongoDB.

    Parameters
    ----------
    cities : list[dict]
        Each dict must have: city_id (int), city_name (str),
        latitude (float), longitude (float).
        Comes from a prior Airflow task that queries dim_city.

    airflow_run_id : str
        The Airflow run ID (e.g. "scheduled__2024-09-26T01:00:00+00:00").
        Stored on every document for cross-referencing with the Airflow UI.

    Returns
    -------
    dict with:
        rows_stored  : int   — number of documents written to MongoDB
        cities_failed: list  — city names that raised an exception
    """
    fetched_at = datetime.now(timezone.utc)  # single timestamp for this run
    rows_stored = 0
    cities_failed: list[str] = []

    # Split into batches
    batches = _make_batches(cities, BATCH_SIZE)
    logger.info(
        "Fetching weather for %d cities in %d batch(es)",
        len(cities), len(batches),
    )

    db = get_mongo_db()
    collection = db[COLLECTION_NAME]

    for batch_index, batch in enumerate(batches, start=1):
        logger.info("Processing batch %d / %d", batch_index, len(batches))
        try:
            raw_responses = _fetch_batch(batch)
            stored = _store_batch(collection, batch, raw_responses, fetched_at, airflow_run_id)
            rows_stored += stored

        except Exception as exc:
            # One bad batch should NOT abort the whole run.
            # Record which cities failed and continue with the next batch.
            failed_names = [c["city_name"] for c in batch]
            logger.error(
                "Batch %d failed (%s). Cities affected: %s",
                batch_index, exc, failed_names,
            )
            cities_failed.extend(failed_names)

    logger.info(
        "Weather extraction complete — %d documents stored, %d cities failed",
        rows_stored, len(cities_failed),
    )
    return {"rows_stored": rows_stored, "cities_failed": cities_failed}


# ── Private helpers ──────────────────────────────────────────────────────────

def _make_batches(cities: list[dict], size: int) -> list[list[dict]]:
    """Split a flat list into chunks of `size`."""
    return [cities[i : i + size] for i in range(0, len(cities), size)]


@retry_on_http_error          # exponential back-off on transient failures
def _fetch_batch(batch: list[dict]) -> list[dict]:
    """
    Make ONE GET request to Open-Meteo for all cities in this batch.

    Open-Meteo returns a JSON array when multiple locations are requested,
    one element per location, in the same order as the input coordinates.
    When only one location is in the batch, it returns a plain JSON object
    (not an array) — we normalise both cases to a list.

    Raises requests.exceptions.HTTPError for non-2xx responses.
    """
    params = {
        "latitude":  ",".join(str(c["latitude"])  for c in batch),
        "longitude": ",".join(str(c["longitude"]) for c in batch),
        "current":   ",".join(CURRENT_VARIABLES),
        "timezone":  "auto",   # API uses the city's local timezone for labels
    }

    response = requests.get(OPEN_METEO_URL, params=params, timeout=30)
    response.raise_for_status()   # raises HTTPError for 4xx / 5xx

    data = response.json()

    # Normalise: single-city response is a dict; multi-city is a list
    if isinstance(data, dict):
        return [data]
    return data


def _store_batch(
    collection,
    batch: list[dict],
    raw_responses: list[dict],
    fetched_at: datetime,
    airflow_run_id: str,
) -> int:
    """
    Upsert raw API responses into MongoDB — one document per city.

    Upsert key: (city_id, fetched_at_hour)
    Why round to the hour?  The DAG runs every 3 hours.  If it retries,
    the retry happens within the same hour as the original attempt, so the
    upsert key matches and we update instead of duplicating.

    Uses update_one(..., upsert=True) — MongoDB's equivalent of
    PostgreSQL's ON CONFLICT DO UPDATE.
    """
    if len(raw_responses) != len(batch):
        raise ValueError(
            f"API returned {len(raw_responses)} responses for {len(batch)} cities. "
            "Cannot safely pair cities to responses."
        )

    # Round fetched_at to the nearest hour for the upsert key
    fetched_hour = fetched_at.replace(minute=0, second=0, microsecond=0)

    stored = 0
    for city, raw in zip(batch, raw_responses):
        document = {
            "city_id":        city["city_id"],
            "city_name":      city["city_name"],
            "fetched_at":     fetched_at,
            "source_api":     "open_meteo",
            "airflow_run_id": airflow_run_id,
            "raw_response":   raw,   # verbatim — no transformation applied
        }

        # Compound filter for idempotent upsert
        filter_key = {
            "city_id":      city["city_id"],
            "fetched_hour": fetched_hour,  # stored separately for the filter
        }

        collection.update_one(
            filter=filter_key,
            update={"$set": {**document, "fetched_hour": fetched_hour}},
            upsert=True,
        )
        stored += 1

    return stored
