"""
tests/test_transform_cities.py
--------------------------------
Unit tests for include/transform/cities.py

These tests use real pandas DataFrames — no mocking needed because the
cleaning functions are pure data transformations with no external dependencies.

Run with:  pytest tests/test_transform_cities.py -v
"""

import pandas as pd
import pytest

# Import the private helpers directly so we can test each step in isolation
from include.transform.cities import (
    _fix_whitespace,
    _fix_capitalisation,
    _resolve_historical_names,
    _coerce_numeric_columns,
    _drop_invalid_rows,
    _deduplicate,
    HISTORICAL_NAMES,
)


# ── _fix_whitespace ───────────────────────────────────────────────────────────

class TestFixWhitespace:

    def test_strips_leading_and_trailing_spaces(self):
        df = pd.DataFrame({"city_name": [" Mumbai "], "state": ["Maharashtra"]})
        result = _fix_whitespace(df)
        assert result["city_name"][0] == "Mumbai"

    def test_strips_tabs_and_newlines(self):
        df = pd.DataFrame({"city_name": ["\tDelhi\n"], "state": ["Delhi"]})
        result = _fix_whitespace(df)
        assert result["city_name"][0] == "Delhi"

    def test_leaves_clean_values_unchanged(self):
        df = pd.DataFrame({"city_name": ["Chennai"], "state": ["Tamil Nadu"]})
        result = _fix_whitespace(df)
        assert result["city_name"][0] == "Chennai"


# ── _fix_capitalisation ───────────────────────────────────────────────────────

class TestFixCapitalisation:

    def test_uppercases_become_title_case(self):
        df = pd.DataFrame({"city_name": ["DELHI"], "state": ["DELHI"]})
        result = _fix_capitalisation(df)
        assert result["city_name"][0] == "Delhi"

    def test_lowercase_becomes_title_case(self):
        df = pd.DataFrame({"city_name": ["bangalore"], "state": ["karnataka"]})
        result = _fix_capitalisation(df)
        assert result["city_name"][0] == "Bangalore"
        assert result["state"][0] == "Karnataka"

    def test_already_title_case_unchanged(self):
        df = pd.DataFrame({"city_name": ["Mumbai"], "state": ["Maharashtra"]})
        result = _fix_capitalisation(df)
        assert result["city_name"][0] == "Mumbai"


# ── _resolve_historical_names ─────────────────────────────────────────────────

class TestResolveHistoricalNames:

    def test_bombay_maps_to_mumbai(self):
        df = pd.DataFrame({"city_name": ["Bombay"], "state": ["Maharashtra"]})
        result_df, aliases = _resolve_historical_names(df)
        assert result_df["city_name"][0] == "Mumbai"

    def test_calcutta_maps_to_kolkata(self):
        df = pd.DataFrame({"city_name": ["Calcutta"], "state": ["West Bengal"]})
        result_df, aliases = _resolve_historical_names(df)
        assert result_df["city_name"][0] == "Kolkata"

    def test_madras_maps_to_chennai(self):
        df = pd.DataFrame({"city_name": ["Madras"], "state": ["Tamil Nadu"]})
        result_df, aliases = _resolve_historical_names(df)
        assert result_df["city_name"][0] == "Chennai"

    def test_alias_seed_is_created_for_historical_name(self):
        df = pd.DataFrame({"city_name": ["Bombay"], "state": ["Maharashtra"]})
        _, aliases = _resolve_historical_names(df)
        assert len(aliases) == 1
        assert aliases[0]["raw_alias"] == "Bombay"
        assert aliases[0]["canonical"] == "Mumbai"
        assert aliases[0]["match_method"] == "exact"

    def test_modern_name_passes_through_unchanged(self):
        df = pd.DataFrame({"city_name": ["Pune"], "state": ["Maharashtra"]})
        result_df, aliases = _resolve_historical_names(df)
        assert result_df["city_name"][0] == "Pune"
        assert aliases == []

    def test_all_historical_names_covered(self):
        """Verify every entry in HISTORICAL_NAMES gets resolved."""
        for old_name, new_name in HISTORICAL_NAMES.items():
            df = pd.DataFrame({
                "city_name": [old_name.title()],
                "state": ["TestState"],
            })
            result_df, _ = _resolve_historical_names(df)
            assert result_df["city_name"][0] == new_name, \
                f"Expected {old_name!r} → {new_name!r}"


# ── _coerce_numeric_columns ───────────────────────────────────────────────────

class TestCoerceNumericColumns:

    def _make_df(self, lat, lon, pop="1000000", tier="1"):
        return pd.DataFrame({
            "city_name":  ["TestCity"],
            "state":      ["TestState"],
            "latitude":   [lat],
            "longitude":  [lon],
            "population": [pop],
            "tier":       [tier],
        })

    def test_valid_strings_converted_to_numbers(self):
        df = self._make_df("19.076", "72.877")
        result = _coerce_numeric_columns(df)
        assert result["latitude"][0] == pytest.approx(19.076)
        assert result["longitude"][0] == pytest.approx(72.877)

    def test_na_string_becomes_nan(self):
        df = self._make_df("N/A", "72.877")
        result = _coerce_numeric_columns(df)
        assert pd.isna(result["latitude"][0])

    def test_null_string_becomes_nan(self):
        df = self._make_df("19.076", "null")
        result = _coerce_numeric_columns(df)
        assert pd.isna(result["longitude"][0])

    def test_invalid_tier_becomes_nan(self):
        df = self._make_df("19.076", "72.877", tier="N/A")
        result = _coerce_numeric_columns(df)
        assert pd.isna(result["tier"][0])

    def test_tier_5_out_of_range_becomes_nan(self):
        df = self._make_df("19.076", "72.877", tier="5")
        result = _coerce_numeric_columns(df)
        assert pd.isna(result["tier"][0])


# ── _drop_invalid_rows ────────────────────────────────────────────────────────

class TestDropInvalidRows:

    def _make_df(self, **overrides):
        base = {
            "city_name": "Mumbai",
            "state":     "Maharashtra",
            "latitude":  19.076,
            "longitude": 72.877,
            "tier":      1,
            "population": 20000000,
        }
        base.update(overrides)
        return pd.DataFrame([base])

    def test_valid_row_is_kept(self):
        df = self._make_df()
        result = _drop_invalid_rows(df)
        assert len(result) == 1

    def test_row_with_missing_latitude_is_dropped(self):
        import numpy as np
        df = self._make_df(latitude=np.nan)
        result = _drop_invalid_rows(df)
        assert len(result) == 0

    def test_row_with_empty_city_name_is_dropped(self):
        df = self._make_df(city_name="")
        result = _drop_invalid_rows(df)
        assert len(result) == 0

    def test_row_with_out_of_range_latitude_is_dropped(self):
        df = self._make_df(latitude=200.0)  # impossible latitude
        result = _drop_invalid_rows(df)
        assert len(result) == 0


# ── _deduplicate ──────────────────────────────────────────────────────────────

class TestDeduplicate:

    def test_duplicate_rows_reduced_to_one(self):
        df = pd.DataFrame({
            "city_name":  ["Mumbai", "Mumbai"],
            "state":      ["Maharashtra", "Maharashtra"],
            "latitude":   [19.076, 19.076],
            "longitude":  [72.877, 72.877],
            "population": [20000000, 20500000],   # slightly different
            "tier":       [1, 1],
        })
        result = _deduplicate(df)
        assert len(result) == 1

    def test_keeps_last_occurrence(self):
        """The last row (assumed to be the correction) should be kept."""
        df = pd.DataFrame({
            "city_name":  ["Mumbai", "Mumbai"],
            "state":      ["Maharashtra", "Maharashtra"],
            "latitude":   [19.076, 19.076],
            "longitude":  [72.877, 72.877],
            "population": [20000000, 20500000],
            "tier":       [1, 1],
        })
        result = _deduplicate(df)
        assert result["population"].iloc[0] == 20500000  # last value kept

    def test_different_cities_not_deduplicated(self):
        df = pd.DataFrame({
            "city_name":  ["Mumbai", "Delhi"],
            "state":      ["Maharashtra", "Delhi"],
            "latitude":   [19.076, 28.704],
            "longitude":  [72.877, 77.102],
            "population": [20000000, 32000000],
            "tier":       [1, 1],
        })
        result = _deduplicate(df)
        assert len(result) == 2
