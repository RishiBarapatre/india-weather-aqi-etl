"""
tests/test_dq_checks.py
------------------------
Unit tests for include/quality/checks.py

Each check function is tested by injecting a mock psycopg2 cursor
that returns controlled values.  No real database needed.

Pattern for each test:
  1. Configure mock_cursor.fetchone / fetchall return values
  2. Call the check function with mock_conn
  3. Assert status ("pass", "warn", or "fail") and details string

Run with:  pytest tests/test_dq_checks.py -v
"""

import pytest
from unittest.mock import MagicMock, patch, call
from datetime import datetime, timezone, timedelta

from include.quality.checks import (
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
)

# Empty load_result used for checks that don't need it
EMPTY_LOAD = {"weather_loaded": 35, "aqi_loaded": 30,
              "weather_rejected": 0, "aqi_rejected": 0}


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_cursor(*fetchone_values):
    """
    Create a mock cursor whose fetchone() returns values in sequence.
    Use when a check calls cur.fetchone() multiple times.
    """
    cursor = MagicMock()
    cursor.__enter__ = lambda s: s
    cursor.__exit__ = MagicMock(return_value=False)
    cursor.fetchone.side_effect = list(fetchone_values)
    return cursor


def make_conn(cursor):
    conn = MagicMock()
    conn.cursor.return_value = cursor
    conn.__enter__ = lambda s: s
    conn.__exit__ = MagicMock(return_value=False)
    return conn


# ── check_schema_presence ─────────────────────────────────────────────────────

class TestCheckSchemaPresence:

    def test_pass_when_all_columns_present(self):
        cursor = MagicMock()
        cursor.__enter__ = lambda s: s
        cursor.__exit__ = MagicMock(return_value=False)
        # Return all required column names for both tables
        cursor.fetchall.return_value = [
            ("reading_id",), ("city_id",), ("date_key",), ("reading_ts",),
            ("temperature_c",), ("humidity_pct",), ("precipitation_mm",),
            ("pressure_hpa",), ("wind_speed_kmh",),
            ("aqi_value",), ("pm25",), ("pm10",), ("no2",),
            ("so2",), ("co",), ("o3",), ("dominant_pollutant",), ("fetched_at",),
        ]
        conn = make_conn(cursor)
        result = check_schema_presence(conn, EMPTY_LOAD)
        assert result["status"] == "pass"

    def test_fail_when_column_missing(self):
        cursor = MagicMock()
        cursor.__enter__ = lambda s: s
        cursor.__exit__ = MagicMock(return_value=False)
        # Return only a subset — temperature_c is missing
        cursor.fetchall.return_value = [
            ("reading_id",), ("city_id",), ("date_key",), ("reading_ts",),
        ]
        conn = make_conn(cursor)
        result = check_schema_presence(conn, EMPTY_LOAD)
        assert result["status"] == "fail"
        assert "temperature_c" in result["details"]


# ── check_freshness ───────────────────────────────────────────────────────────

class TestCheckFreshness:

    def test_pass_when_recent(self):
        recent_ts = datetime.now(timezone.utc) - timedelta(hours=1)
        cursor = make_cursor((recent_ts,))
        result = check_freshness(make_conn(cursor), EMPTY_LOAD)
        assert result["status"] == "pass"

    def test_warn_when_moderately_stale(self):
        stale_ts = datetime.now(timezone.utc) - timedelta(hours=5)
        cursor = make_cursor((stale_ts,))
        result = check_freshness(make_conn(cursor), EMPTY_LOAD)
        assert result["status"] == "warn"

    def test_fail_when_very_stale(self):
        very_stale_ts = datetime.now(timezone.utc) - timedelta(hours=8)
        cursor = make_cursor((very_stale_ts,))
        result = check_freshness(make_conn(cursor), EMPTY_LOAD)
        assert result["status"] == "fail"

    def test_fail_when_table_is_empty(self):
        cursor = make_cursor((None,))   # MAX() on empty table = None
        result = check_freshness(make_conn(cursor), EMPTY_LOAD)
        assert result["status"] == "fail"
        assert "empty" in result["details"].lower()


# ── check_completeness ────────────────────────────────────────────────────────

class TestCheckCompleteness:

    def test_pass_when_most_cities_have_data(self):
        # 34 out of 35 cities = 97%
        cursor = make_cursor((35,), (34,))
        result = check_completeness(make_conn(cursor), EMPTY_LOAD)
        assert result["status"] == "pass"

    def test_warn_when_some_cities_missing(self):
        # 20 out of 35 = 57% (below 70% warn threshold)
        cursor = make_cursor((35,), (20,))
        result = check_completeness(make_conn(cursor), EMPTY_LOAD)
        assert result["status"] == "warn"

    def test_fail_when_many_cities_missing(self):
        # 10 out of 35 = 28% (below 50% fail threshold)
        cursor = make_cursor((35,), (10,))
        result = check_completeness(make_conn(cursor), EMPTY_LOAD)
        assert result["status"] == "fail"

    def test_fail_when_no_cities_in_dim_city(self):
        cursor = make_cursor((0,))   # no active cities
        result = check_completeness(make_conn(cursor), EMPTY_LOAD)
        assert result["status"] == "fail"


# ── check_null_critical_fields ────────────────────────────────────────────────

class TestCheckNullCriticalFields:

    def test_pass_when_no_nulls(self):
        # 6 checks, each returns (0,) = no nulls
        cursor = make_cursor(*[(0,)] * 6)
        result = check_null_critical_fields(make_conn(cursor), EMPTY_LOAD)
        assert result["status"] == "pass"

    def test_fail_when_null_found(self):
        # First check (fact_weather_reading.city_id) has 3 NULLs
        cursor = make_cursor((3,), *[(0,)] * 5)
        result = check_null_critical_fields(make_conn(cursor), EMPTY_LOAD)
        assert result["status"] == "fail"
        assert "3 NULLs" in result["details"]


# ── check_range_temperature ───────────────────────────────────────────────────

class TestCheckRangeTemperature:

    def test_pass_when_all_in_range(self):
        cursor = make_cursor((0,))   # 0 out-of-range rows
        result = check_range_temperature(make_conn(cursor), EMPTY_LOAD)
        assert result["status"] == "pass"

    def test_warn_when_outliers_exist(self):
        cursor = make_cursor((3,))   # 3 readings outside [-20, 60]
        result = check_range_temperature(make_conn(cursor), EMPTY_LOAD)
        assert result["status"] == "warn"


# ── check_range_humidity ──────────────────────────────────────────────────────

class TestCheckRangeHumidity:

    def test_pass_when_all_in_range(self):
        cursor = make_cursor((0,))
        result = check_range_humidity(make_conn(cursor), EMPTY_LOAD)
        assert result["status"] == "pass"

    def test_fail_when_impossible_humidity(self):
        cursor = make_cursor((2,))   # 2 readings outside [0, 100]
        result = check_range_humidity(make_conn(cursor), EMPTY_LOAD)
        assert result["status"] == "fail"


# ── check_range_aqi ───────────────────────────────────────────────────────────

class TestCheckRangeAqi:

    def test_pass_when_all_in_range(self):
        cursor = make_cursor((0,))
        result = check_range_aqi(make_conn(cursor), EMPTY_LOAD)
        assert result["status"] == "pass"

    def test_fail_when_aqi_above_500(self):
        cursor = make_cursor((1,))   # 1 reading above 500
        result = check_range_aqi(make_conn(cursor), EMPTY_LOAD)
        assert result["status"] == "fail"


# ── check_row_count_sanity ────────────────────────────────────────────────────

class TestCheckRowCountSanity:

    def test_pass_with_healthy_counts(self):
        result = check_row_count_sanity(
            MagicMock(),
            {"weather_loaded": 35, "aqi_loaded": 30}
        )
        assert result["status"] == "pass"

    def test_warn_when_weather_low(self):
        result = check_row_count_sanity(
            MagicMock(),
            {"weather_loaded": 3, "aqi_loaded": 30}   # below MIN_ROWS_PER_RUN=10
        )
        assert result["status"] == "warn"
        assert "weather" in result["details"]

    def test_warn_when_aqi_low(self):
        result = check_row_count_sanity(
            MagicMock(),
            {"weather_loaded": 35, "aqi_loaded": 0}
        )
        assert result["status"] == "warn"
        assert "aqi" in result["details"]


# ── check_city_resolution ─────────────────────────────────────────────────────

class TestCheckCityResolution:

    def test_pass_when_few_unresolved(self):
        cursor = make_cursor((1,))   # 1 unresolved city (below warn threshold of 3)
        result = check_city_resolution(make_conn(cursor), EMPTY_LOAD)
        assert result["status"] == "pass"

    def test_warn_when_moderate_unresolved(self):
        cursor = make_cursor((5,))   # 5 unresolved (above warn=3, below fail=10)
        result = check_city_resolution(make_conn(cursor), EMPTY_LOAD)
        assert result["status"] == "warn"

    def test_fail_when_many_unresolved(self):
        cursor = make_cursor((12,))  # 12 unresolved (above fail=10)
        result = check_city_resolution(make_conn(cursor), EMPTY_LOAD)
        assert result["status"] == "fail"


# ── check_uniqueness ──────────────────────────────────────────────────────────

class TestCheckUniqueness:

    def test_pass_when_no_duplicates(self):
        # total == unique_pairs for both tables
        cursor = make_cursor((100, 100), (80, 80))
        result = check_uniqueness(make_conn(cursor), EMPTY_LOAD)
        assert result["status"] == "pass"

    def test_fail_when_duplicates_exist(self):
        # weather table has 105 rows but only 100 unique (city_id, reading_ts) pairs
        cursor = make_cursor((105, 100), (80, 80))
        result = check_uniqueness(make_conn(cursor), EMPTY_LOAD)
        assert result["status"] == "fail"
        assert "5 duplicate" in result["details"]
