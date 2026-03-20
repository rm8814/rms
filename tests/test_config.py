"""
tests/test_config.py — Unit tests for PropertyConfig.from_db_row and to_wib_str
"""

import pytest
from config import PropertyConfig, to_wib_str


class TestFromDbRow:
    def test_basic_round_trip(self):
        row = {
            "id": "p1", "name": "Hotel A", "contract_type": "revshare_revenue",
            "join_date": "2024-01-01", "room_count": 50, "active": 1,
            "city": "Jakarta", "api_key": "key123", "revshare_pct": 20.0,
            "revshare_gop_pct": None, "lease_monthly": None,
            "advance_total": None, "contract_months": None,
        }
        prop = PropertyConfig.from_db_row(row)
        assert prop.id == "p1"
        assert prop.name == "Hotel A"
        assert prop.revshare_pct == 20.0
        assert prop.api_key == "key123"

    def test_extra_keys_ignored(self):
        row = {
            "id": "p1", "name": "Hotel A", "contract_type": "lease",
            "join_date": "2024-01-01", "room_count": 30, "active": 1,
            "unknown_column": "should be ignored",
        }
        prop = PropertyConfig.from_db_row(row)
        assert prop.id == "p1"

    def test_missing_optional_fields_get_defaults(self):
        row = {
            "id": "p1", "name": "Hotel A", "contract_type": "lease",
            "join_date": "2024-01-01", "room_count": 30, "active": 1,
        }
        prop = PropertyConfig.from_db_row(row)
        assert prop.city is None
        assert prop.api_key is None
        assert prop.revshare_pct is None


class TestToWibStr:
    def test_iso_string_converted(self):
        result = to_wib_str("2024-03-15T10:00:00")
        assert result == "2024-03-15 17:00 WIB"

    def test_midnight_utc(self):
        result = to_wib_str("2024-03-15T00:00:00")
        assert result == "2024-03-15 07:00 WIB"

    def test_invalid_string_returned_as_is(self):
        result = to_wib_str("not-a-date")
        assert result == "not-a-date"

    def test_space_separator_also_works(self):
        result = to_wib_str("2024-03-15 10:00:00")
        assert result == "2024-03-15 17:00 WIB"
