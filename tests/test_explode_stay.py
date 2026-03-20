"""
tests/test_explode_stay.py — Unit tests for _explode_stay

Covers: multi-night stay, stay starting before today, cancelled stays,
non-IDR currency, bad dates, check-in == check-out (0 nights).
"""

import pytest
from datetime import date, timedelta
from ingestion.bookings import _explode_stay

TODAY = date(2024, 3, 15)
PERIOD_TO = TODAY + timedelta(days=90)
RAN_AT = "2024-03-15T00:00:00"


def _stay(**kwargs):
    defaults = {
        "checkInDateTime": "2024-03-15",
        "checkOutDateTime": "2024-03-17",
        "status": "New",
        "bookingStatus": "",
        "roomTypeId": 1,
        "totalPrice": {"amount": 2_000_000},
        "_currencyId": "IDR",
    }
    return {**defaults, **kwargs}


class TestExplodeStay:
    def test_two_night_stay(self):
        rows = _explode_stay("p1", "BK001", _stay(), TODAY, PERIOD_TO, RAN_AT)
        assert len(rows) == 2
        dates = [r[1] for r in rows]
        assert "2024-03-15" in dates
        assert "2024-03-16" in dates

    def test_checkout_date_not_included(self):
        rows = _explode_stay("p1", "BK001", _stay(), TODAY, PERIOD_TO, RAN_AT)
        dates = [r[1] for r in rows]
        assert "2024-03-17" not in dates

    def test_stay_starting_before_today(self):
        # Check-in was 3 days ago — only future nights from today onward
        rows = _explode_stay(
            "p1", "BK001",
            _stay(checkInDateTime="2024-03-12", checkOutDateTime="2024-03-17"),
            TODAY, PERIOD_TO, RAN_AT,
        )
        dates = [r[1] for r in rows]
        assert "2024-03-12" not in dates
        assert "2024-03-13" not in dates
        assert "2024-03-15" in dates
        assert "2024-03-16" in dates

    def test_cancelled_status_returns_empty(self):
        rows = _explode_stay("p1", "BK001", _stay(status="Cancelled"), TODAY, PERIOD_TO, RAN_AT)
        assert rows == []

    def test_cancelled_booking_status_returns_empty(self):
        rows = _explode_stay("p1", "BK001", _stay(bookingStatus="Cancelled"), TODAY, PERIOD_TO, RAN_AT)
        assert rows == []

    def test_non_idr_currency_nightly_rate_is_none(self):
        rows = _explode_stay("p1", "BK001", _stay(_currencyId="USD"), TODAY, PERIOD_TO, RAN_AT)
        assert len(rows) == 2
        for row in rows:
            nightly_rate = row[7]   # index 7 = nightly_rate_idr
            assert nightly_rate is None

    def test_idr_currency_nightly_rate_calculated(self):
        rows = _explode_stay("p1", "BK001", _stay(totalPrice={"amount": 2_000_000}, _currencyId="IDR"), TODAY, PERIOD_TO, RAN_AT)
        assert len(rows) == 2
        for row in rows:
            assert row[7] == pytest.approx(1_000_000)   # 2_000_000 / 2 nights

    def test_bad_dates_returns_empty(self):
        rows = _explode_stay("p1", "BK001", _stay(checkInDateTime="not-a-date"), TODAY, PERIOD_TO, RAN_AT)
        assert rows == []

    def test_missing_dates_returns_empty(self):
        rows = _explode_stay("p1", "BK001", _stay(checkInDateTime="", checkOutDateTime=""), TODAY, PERIOD_TO, RAN_AT)
        assert rows == []

    def test_same_day_checkin_checkout_returns_empty(self):
        # 0 nights — no rows
        rows = _explode_stay(
            "p1", "BK001",
            _stay(checkInDateTime="2024-03-15", checkOutDateTime="2024-03-15"),
            TODAY, PERIOD_TO, RAN_AT,
        )
        assert rows == []

    def test_stay_beyond_period_to_clipped(self):
        far_future = str(PERIOD_TO + timedelta(days=10))
        rows = _explode_stay(
            "p1", "BK001",
            _stay(checkInDateTime="2024-03-15", checkOutDateTime=far_future),
            TODAY, PERIOD_TO, RAN_AT,
        )
        dates = [r[1] for r in rows]
        assert all(d <= str(PERIOD_TO) for d in dates)
