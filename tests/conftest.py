"""
tests/conftest.py
------------------
Shared pytest fixtures used across the test suite.

Fixtures here are automatically discovered by pytest — no import needed
in individual test files.
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone


# ── Database mock fixtures ────────────────────────────────────────────────────

@pytest.fixture
def mock_cursor():
    """
    A mock psycopg2 cursor.  Tests configure its return values like:
        mock_cursor.fetchone.return_value = (42,)
        mock_cursor.fetchall.return_value = [("Mumbai", 1), ...]
    """
    cursor = MagicMock()
    cursor.__enter__ = lambda s: s
    cursor.__exit__ = MagicMock(return_value=False)
    return cursor


@pytest.fixture
def mock_conn(mock_cursor):
    """
    A mock psycopg2 connection that yields mock_cursor from .cursor().
    Also supports context manager usage (with get_warehouse_conn() as conn:).
    """
    conn = MagicMock()
    conn.cursor.return_value = mock_cursor
    conn.__enter__ = lambda s: s
    conn.__exit__ = MagicMock(return_value=False)
    return conn


@pytest.fixture
def fixed_utc_now():
    """A fixed UTC datetime for deterministic timestamp tests."""
    return datetime(2024, 9, 26, 8, 30, 0, tzinfo=timezone.utc)


# ── Sample data fixtures ──────────────────────────────────────────────────────

@pytest.fixture
def sample_weather_raw_response():
    """A valid verbatim Open-Meteo API response for one city."""
    return {
        "latitude":  19.076,
        "longitude": 72.877,
        "timezone":  "Asia/Kolkata",
        "current": {
            "time":                 "2024-09-26T14:00",
            "temperature_2m":       32.5,
            "relative_humidity_2m": 78,
            "precipitation":        0.0,
            "surface_pressure":     1008.5,
            "wind_speed_10m":       15.2,
        },
    }


@pytest.fixture
def sample_weather_mongo_doc(sample_weather_raw_response, fixed_utc_now):
    """A complete MongoDB document as stored by the weather extractor."""
    return {
        "city_id":        1,
        "city_name":      "Mumbai",
        "fetched_at":     fixed_utc_now,
        "source_api":     "open_meteo",
        "airflow_run_id": "scheduled__2024-09-26T03:00:00+00:00",
        "raw_response":   sample_weather_raw_response,
    }


@pytest.fixture
def sample_aqi_raw_response():
    """A valid verbatim WAQI API response for one city."""
    return {
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
                "o3":   {"v": 22.1},
            },
            "time": {
                "s":  "2024-09-26 14:00:00",
                "tz": "+05:30",
            },
        },
    }


@pytest.fixture
def sample_aqi_mongo_doc(sample_aqi_raw_response, fixed_utc_now):
    """A complete MongoDB document as stored by the AQI extractor."""
    return {
        "city_id":        1,
        "city_name":      "Mumbai",
        "fetched_at":     fixed_utc_now,
        "source_api":     "waqi",
        "airflow_run_id": "scheduled__2024-09-26T03:00:00+00:00",
        "raw_response":   sample_aqi_raw_response,
    }
