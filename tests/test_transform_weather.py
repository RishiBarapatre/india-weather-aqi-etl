"""
tests/test_transform_weather.py
---------------------------------
Unit tests for include/transform/weather.py

Tests the JSON-parsing logic with known inputs, covering:
  - Happy path (valid complete response)
  - Missing 'current' block
  - Unknown timezone fallback
  - Malformed timestamp
  - Missing individual measurement fields (should store None, not crash)

All tests are pure unit tests — no DB or network calls.

Run with:  pytest tests/test_transform_weather.py -v
"""

import pytest
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from include.transform.weather import _parse_weather_doc, _parse_timestamp, _safe_float


# ── _parse_timestamp ──────────────────────────────────────────────────────────

class TestParseTimestamp:

    def test_valid_timestamp_with_known_timezone(self):
        ts = _parse_timestamp("2024-09-26T14:00", "Asia/Kolkata")
        assert ts is not None
        # Should be timezone-aware
        assert ts.tzinfo is not None
        # Hour in local time should be 14
        assert ts.hour == 14

    def test_unknown_timezone_falls_back_to_utc(self):
        ts = _parse_timestamp("2024-09-26T14:00", "Fake/Timezone")
        assert ts is not None
        assert str(ts.tzinfo) == "UTC"

    def test_empty_time_string_returns_none(self):
        ts = _parse_timestamp("", "Asia/Kolkata")
        assert ts is None

    def test_malformed_time_string_returns_none(self):
        ts = _parse_timestamp("not-a-date", "Asia/Kolkata")
        assert ts is None

    def test_utc_timezone(self):
        ts = _parse_timestamp("2024-09-26T08:30", "UTC")
        assert ts is not None
        assert ts.hour == 8


# ── _safe_float ───────────────────────────────────────────────────────────────

class TestSafeFloat:

    def test_valid_number_converted(self):
        assert _safe_float(32.5) == pytest.approx(32.5)

    def test_string_number_converted(self):
        assert _safe_float("15.2") == pytest.approx(15.2)

    def test_none_returns_none(self):
        assert _safe_float(None) is None

    def test_non_numeric_string_returns_none(self):
        assert _safe_float("N/A") is None

    def test_integer_converted_to_float(self):
        assert _safe_float(78) == pytest.approx(78.0)


# ── _parse_weather_doc ────────────────────────────────────────────────────────

class TestParseWeatherDoc:

    def test_valid_doc_returns_clean_dict(self, sample_weather_mongo_doc):
        result = _parse_weather_doc(sample_weather_mongo_doc)

        assert result is not None
        assert result["city_id"] == 1
        assert result["temperature_c"] == pytest.approx(32.5)
        assert result["humidity_pct"] == pytest.approx(78.0)
        assert result["precipitation_mm"] == pytest.approx(0.0)
        assert result["pressure_hpa"] == pytest.approx(1008.5)
        assert result["wind_speed_kmh"] == pytest.approx(15.2)

    def test_date_key_is_yyyymmdd_integer(self, sample_weather_mongo_doc):
        result = _parse_weather_doc(sample_weather_mongo_doc)
        assert result["date_key"] == 20240926

    def test_reading_ts_is_timezone_aware(self, sample_weather_mongo_doc):
        result = _parse_weather_doc(sample_weather_mongo_doc)
        assert result["reading_ts"].tzinfo is not None

    def test_missing_current_block_returns_none(self, sample_weather_mongo_doc):
        doc = dict(sample_weather_mongo_doc)
        doc["raw_response"] = {}  # no 'current' key
        result = _parse_weather_doc(doc)
        assert result is None

    def test_missing_individual_field_becomes_none(self, sample_weather_mongo_doc):
        """A missing measurement field should be None, not raise an exception."""
        doc = dict(sample_weather_mongo_doc)
        raw = dict(doc["raw_response"])
        current = dict(raw["current"])
        del current["wind_speed_10m"]   # remove one field
        raw["current"] = current
        doc["raw_response"] = raw

        result = _parse_weather_doc(doc)
        assert result is not None
        assert result["wind_speed_kmh"] is None   # None, not crash

    def test_non_numeric_measurement_becomes_none(self, sample_weather_mongo_doc):
        doc = dict(sample_weather_mongo_doc)
        raw = dict(doc["raw_response"])
        current = dict(raw["current"])
        current["temperature_2m"] = "sensor_error"
        raw["current"] = current
        doc["raw_response"] = raw

        result = _parse_weather_doc(doc)
        assert result is not None
        assert result["temperature_c"] is None
