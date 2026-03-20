"""
tests/test_contract_revenue.py — Unit tests for _calc_rehat_revenue

Tests all 5 contract types plus the unknown-contract-type guard.
Uses an in-memory SQLite DB with no monthly_costs (daily_costs = 0)
to keep tests simple and deterministic.
"""

import pytest
import sqlite3
from unittest.mock import patch
from config import PropertyConfig
from ingestion.services import _calc_rehat_revenue, _days_in_month


def _make_prop(**kwargs) -> PropertyConfig:
    defaults = dict(
        id="p1", name="Test Hotel", contract_type="revshare_revenue",
        join_date="2024-01-01", room_count=50, active=True,
    )
    return PropertyConfig(**{**defaults, **kwargs})


def _empty_conn():
    """In-memory DB with a stub monthly_costs table (no rows = zero costs)."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE monthly_costs (
            property_id TEXT, year INTEGER, month INTEGER,
            category TEXT, amount REAL, updated_at TEXT,
            PRIMARY KEY (property_id, year, month, category)
        )
    """)
    return conn


class TestRevshareRevenue:
    def test_basic(self):
        prop = _make_prop(contract_type="revshare_revenue", revshare_pct=20.0)
        conn = _empty_conn()
        result = _calc_rehat_revenue(prop, 1_000_000, "2024-03-15", conn)
        assert result == pytest.approx(200_000)

    def test_zero_pct(self):
        prop = _make_prop(contract_type="revshare_revenue", revshare_pct=0.0)
        conn = _empty_conn()
        assert _calc_rehat_revenue(prop, 1_000_000, "2024-03-15", conn) == 0

    def test_none_pct_treated_as_zero(self):
        prop = _make_prop(contract_type="revshare_revenue", revshare_pct=None)
        conn = _empty_conn()
        assert _calc_rehat_revenue(prop, 1_000_000, "2024-03-15", conn) == 0


class TestRevshareGop:
    def test_basic(self):
        # No costs → GOP = revenue → rehat = revenue * pct
        prop = _make_prop(contract_type="revshare_gop", revshare_gop_pct=30.0)
        conn = _empty_conn()
        result = _calc_rehat_revenue(prop, 1_000_000, "2024-03-15", conn)
        assert result == pytest.approx(300_000)

    def test_with_costs(self):
        prop = _make_prop(contract_type="revshare_gop", revshare_gop_pct=30.0)
        conn = _empty_conn()
        # Inject 31 * 100_000 total monthly costs → 100_000/day
        conn.execute(
            "INSERT INTO monthly_costs VALUES ('p1', 2024, 3, 'rooms', 3100000, '2024-01-01')"
        )
        conn.commit()
        # March has 31 days → daily_costs = 100_000; GOP = 1_000_000 - 100_000 = 900_000
        result = _calc_rehat_revenue(prop, 1_000_000, "2024-03-15", conn)
        assert result == pytest.approx(270_000)   # 900_000 * 0.30


class TestRevshareRevenueGop:
    def test_basic(self):
        prop = _make_prop(
            contract_type="revshare_revenue_gop",
            revshare_pct=10.0, revshare_gop_pct=20.0
        )
        conn = _empty_conn()
        # No costs → GOP = revenue; result = 10% of rev + 20% of rev = 30%
        result = _calc_rehat_revenue(prop, 1_000_000, "2024-03-15", conn)
        assert result == pytest.approx(300_000)


class TestLease:
    def test_no_costs(self):
        prop = _make_prop(contract_type="lease")
        conn = _empty_conn()
        result = _calc_rehat_revenue(prop, 1_000_000, "2024-03-15", conn)
        assert result == pytest.approx(1_000_000)

    def test_with_costs(self):
        prop = _make_prop(contract_type="lease")
        conn = _empty_conn()
        conn.execute(
            "INSERT INTO monthly_costs VALUES ('p1', 2024, 3, 'rooms', 3100000, '2024-01-01')"
        )
        conn.commit()
        result = _calc_rehat_revenue(prop, 1_000_000, "2024-03-15", conn)
        assert result == pytest.approx(900_000)


class TestAdvancePayment:
    def test_same_as_lease(self):
        prop = _make_prop(contract_type="advance_payment")
        conn = _empty_conn()
        result = _calc_rehat_revenue(prop, 1_000_000, "2024-03-15", conn)
        assert result == pytest.approx(1_000_000)


class TestUnknownContractType:
    def test_raises_value_error(self):
        prop = _make_prop(contract_type="typo_contract")
        conn = _empty_conn()
        with pytest.raises(ValueError, match="Unknown contract_type"):
            _calc_rehat_revenue(prop, 1_000_000, "2024-03-15", conn)


class TestDaysInMonth:
    def test_march(self):
        assert _days_in_month(2024, 3) == 31

    def test_february_leap(self):
        assert _days_in_month(2024, 2) == 29

    def test_february_nonleap(self):
        assert _days_in_month(2023, 2) == 28

    def test_december_boundary(self):
        assert _days_in_month(2024, 12) == 31
