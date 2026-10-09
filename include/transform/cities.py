"""
include/transform/cities.py
----------------------------
Cleans the raw Indian cities CSV before it is loaded into dim_city.

The source CSV (data/raw/indian_cities.csv) deliberately contains 8 messy
data patterns that mirror real-world legacy data problems:

  1. Leading / trailing whitespace       →  " Mumbai "
  2. Inconsistent capitalisation         →  "DELHI", "bangalore"
  3. Historical / old city names         →  "Bombay", "Calcutta", "Madras"
  4. Missing coordinates                 →  blank latitude / longitude
  5. Invalid numeric strings             →  "N/A", "null" in numeric columns
  6. Duplicate rows                      →  same city twice with minor diffs
  7. Malformed CSV lines                 →  extra commas (handled by pandas)
  8. Fully blank rows                    →  empty lines

This module returns:
  - cleaned_cities  : list[dict]  → ready to INSERT into dim_city
  - alias_seeds     : list[dict]  → historical name → canonical name mappings
                                    ready to INSERT into dim_city_alias
"""

import logging
from typing import Any
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Historical name → canonical name map
# Source: known Indian city renames (official Government of India changes)
# ---------------------------------------------------------------------------
HISTORICAL_NAMES: dict[str, str] = {
    "bombay":       "Mumbai",
    "calcutta":     "Kolkata",
    "madras":       "Chennai",
    "poona":        "Pune",
    "trivandrum":   "Thiruvananthapuram",
    "gurgaon":      "Gurugram",
    "bangalore":    "Bengaluru",
    "mysore":       "Mysuru",
    "baroda":       "Vadodara",
    "cuttack":      "Bhubaneswar",   # common mislabelling
}

# Path to the CSV — resolved relative to this file so it works inside Docker
# (Airflow mounts the repo at /opt/airflow, so Path(__file__) works correctly)
_DEFAULT_CSV_PATH = Path(__file__).parents[2] / "data" / "raw" / "indian_cities.csv"


def clean_cities(csv_path: Path = _DEFAULT_CSV_PATH) -> tuple[list[dict], list[dict]]:
    """
    Read and clean the cities CSV.

    Returns
    -------
    cleaned_cities : list[dict]
        One dict per canonical city, ready for dim_city INSERT.
    alias_seeds : list[dict]
        Historical name → canonical city mappings for dim_city_alias.
    """
    logger.info("Reading cities CSV from %s", csv_path)
    df = _read_csv(csv_path)
    df = _fix_whitespace(df)
    df = _fix_capitalisation(df)
    df, alias_seeds = _resolve_historical_names(df)
    df = _coerce_numeric_columns(df)
    df = _drop_invalid_rows(df)
    df = _deduplicate(df)

    cleaned_cities = df.to_dict(orient="records")
    logger.info(
        "Cleaning complete — %d canonical cities, %d alias seeds",
        len(cleaned_cities),
        len(alias_seeds),
    )
    return cleaned_cities, alias_seeds


# ---------------------------------------------------------------------------
# Private helpers — one function per data quality problem
# ---------------------------------------------------------------------------

def _read_csv(path: Path) -> pd.DataFrame:
    """
    Read the CSV, skipping fully blank rows automatically.
    pandas' read_csv handles extra commas (malformed lines) gracefully when
    using the Python engine — it fills missing columns with NaN.
    """
    df = pd.read_csv(
        path,
        engine="python",       # more lenient with malformed lines
        skip_blank_lines=True, # problem 8: blank rows
        dtype=str,             # read everything as string first; we cast later
    )
    logger.info("Raw CSV loaded — %d rows, %d columns", len(df), len(df.columns))
    return df


def _fix_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """
    Strip leading/trailing whitespace from all string columns.
    Problem 1: " Mumbai " → "Mumbai"
    """
    str_cols = df.select_dtypes(include=["object", "string"]).columns
    df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())
    return df


def _fix_capitalisation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise city_name and state to Title Case.
    Problem 2: "DELHI" → "Delhi", "bangalore" → "Bangalore"
    Note: we do this BEFORE historical name resolution so that the lookup
    keys in HISTORICAL_NAMES (lowercase) match correctly.
    """
    df["city_name"] = df["city_name"].str.title()
    df["state"] = df["state"].str.title()
    return df


def _resolve_historical_names(df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    """
    Map old city names to their modern canonical equivalents.
    Problem 3: "Bombay" → "Mumbai", "Calcutta" → "Kolkata", etc.

    Also builds alias_seeds: a list of (raw_alias, canonical_name) pairs
    that will be pre-seeded into dim_city_alias so future pipeline runs can
    resolve these names instantly via an exact lookup.
    """
    alias_seeds: list[dict] = []

    def _resolve(name: Any) -> Any:
        if not isinstance(name, str) or not name:
            return name
        canonical = HISTORICAL_NAMES.get(name.lower())
        if canonical:
            alias_seeds.append({
                "raw_alias":    name,       # original form e.g. "Bombay"
                "canonical":    canonical,  # modern form  e.g. "Mumbai"
                "match_method": "exact",
            })
            return canonical
        return name

    df["city_name"] = df["city_name"].apply(_resolve)
    return df, alias_seeds


def _coerce_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert latitude, longitude, population to proper numeric types.
    Problem 5: "N/A", "null", "" → NaN (these rows get dropped later).
    Problem 5: "2857.000" in tier column → coerced or NaN.
    """
    df["latitude"]   = pd.to_numeric(df["latitude"],   errors="coerce")
    df["longitude"]  = pd.to_numeric(df["longitude"],  errors="coerce")
    df["population"] = pd.to_numeric(df["population"], errors="coerce").astype("Int64")

    # tier should be 1, 2, or 3 — anything else becomes NaN
    df["tier"] = pd.to_numeric(df["tier"], errors="coerce")
    valid_tiers = df["tier"].isin([1, 2, 3])
    df.loc[~valid_tiers, "tier"] = pd.NA
    df["tier"] = df["tier"].astype("Int64")

    return df


def _drop_invalid_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop rows that cannot produce a valid dim_city record.

    Rules:
      - city_name must be present and non-empty
      - latitude and longitude must be valid numbers   (problem 4)
      - latitude  must be in [-90,  90]
      - longitude must be in [-180, 180]
    """
    before = len(df)

    # Drop rows with no city name
    df = df.dropna(subset=["city_name"])
    df = df[df["city_name"].str.strip() != ""]

    # Drop rows with missing or out-of-range coordinates (problem 4)
    df = df.dropna(subset=["latitude", "longitude"])
    df = df[df["latitude"].between(-90, 90)]
    df = df[df["longitude"].between(-180, 180)]

    dropped = before - len(df)
    if dropped:
        logger.warning("Dropped %d rows that failed validation", dropped)

    return df


def _deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate city rows.
    Problem 6: same city appears twice with slightly different population values.

    Strategy:
      - Deduplicate on (city_name, state) — the natural unique key.
      - Keep the LAST occurrence (assumes later rows in the CSV are corrections).
      - Log what was removed so we have an audit trail.
    """
    before = len(df)
    df = df.drop_duplicates(subset=["city_name", "state"], keep="last")
    dropped = before - len(df)
    if dropped:
        logger.warning("Dropped %d duplicate city rows (kept last occurrence)", dropped)
    return df
