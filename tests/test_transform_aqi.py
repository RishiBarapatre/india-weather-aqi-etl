"""
tests/test_transform_aqi.py
-----------------------------
Unit tests for include/transform/aqi.py

Covers:
  - Happy path (all pollutants present)
  - Missing individual pollutants (sparse iaqi)
  - AQI value "-" (station no data)
  - Non-ok status
  - Timestamp parsing with UTC offset
  - The API's "dominentpol" typo

Run with:  pytest tests/test_transform_aqi.py -v
"""

import pytest
from include.transform.aqi import _parse_aqi_doc, _parse_timestamp, _safe_float, _safe_int


# ── _parse_timestamp ──────────────────────────────────────────────────────────

class TestParseAqiTimestamp:

    def test_valid_timestamp_with_positive_offset(self):
        time_block = {"s": "2024-09-26 14:00:00", "tz": "+05:30"}
        ts = _parse_timestamp(time_block)
        assert ts is not None
        assert ts.hour == 14
        # UTC offset should be +05:30 = 19800 seconds
        assert ts.utcoffset().total_seconds() == 5.5 * 3600

    def test_valid_timestamp_with_utc(self):
        time_block = {"s": "2024-09-26 08:30:00", "tz": "+00:00"}
        ts = _parse_timestamp(time_block)
        assert ts is not None
        assert ts.utcoffset().total_seconds() == 0

    def test_empty_time_string_returns_none(self):
        ts = _parse_timestamp({"s": "", "tz": "+05:30"})
        assert ts is None

    def test_missing_time_key_returns_none(self):
        ts = _parse_timestamp({})
        assert ts is None


# ── _parse_aqi_doc ────────────────────────────────────────────────────────────

class TestParseAqiDoc:

    def test_valid_doc_returns_clean_dict(self, sample_aqi_mongo_doc):
        result = _parse_aqi_doc(sample_aqi_mongo_doc)

        assert result is not None
        assert result["city_id"] == 1
        assert result["aqi_value"] == 87
        assert result["pm25"] == pytest.approx(32.5)
        assert result["pm10"] == pytest.approx(45.0)
        assert result["no2"]  == pytest.approx(12.3)
        assert result["so2"]  == pytest.approx(5.1)
        assert result["co"]   == pytest.approx(0.8)
        assert result["o3"]   == pytest.approx(22.1)
        assert result["dominant_pollutant"] == "pm25"

    def test_date_key_is_yyyymmdd(self, sample_aqi_mongo_doc):
        result = _parse_aqi_doc(sample_aqi_mongo_doc)
        assert result["date_key"] == 20240926

    def test_reading_ts_is_timezone_aware(self, sample_aqi_mongo_doc):
        result = _parse_aqi_doc(sample_aqi_mongo_doc)
        assert result["reading_ts"].tzinfo is not None

    def test_aqi_dash_returns_none(self, sample_aqi_mongo_doc):
        """AQI value of '-' means station has no data — skip the row."""
        doc = dict(sample_aqi_mongo_doc)
        raw = dict(doc["raw_response"])
        data = dict(raw["data"])
        data["aqi"] = "-"
        raw["data"] = data
        doc["raw_response"] = raw
        result = _parse_aqi_doc(doc)
        assert result is None

    def test_non_ok_status_returns_none(self, sample_aqi_mongo_doc):
        doc = dict(sample_aqi_mongo_doc)
        doc["raw_response"] = {"status": "error", "data": "Unknown station"}
        result = _parse_aqi_doc(doc)
        assert result is None

    def test_missing_pollutant_stored_as_none(self, sample_aqi_mongo_doc):
        """
        Not all stations measure every pollutant.
        Missing iaqi keys should produce None columns, not a crash.
        """
        doc = dict(sample_aqi_mongo_doc)
        raw = dict(doc["raw_response"])
        data = dict(raw["data"])
        # Remove so2 and co — some stations don't measure these
        data["iaqi"] = {"pm25": {"v": 32.5}, "pm10": {"v": 45.0}}
        raw["data"] = data
        doc["raw_response"] = raw

        result = _parse_aqi_doc(doc)
        assert result is not None
        assert result["pm25"] == pytest.approx(32.5)
        assert result["so2"] is None   # absent → None
        assert result["co"]  is None   # absent → None

    def test_api_dominentpol_typo_handled(self, sample_aqi_mongo_doc):
        """The WAQI API spells 'dominant' as 'dominent' — we handle this."""
        result = _parse_aqi_doc(sample_aqi_mongo_doc)
        # dominant_pollutant should be populated despite the API typo
        assert result["dominant_pollutant"] is not None
