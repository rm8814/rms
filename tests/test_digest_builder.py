"""
tests/test_digest_builder.py — Unit tests for notifications/digest_builder.py

Uses in-memory SQLite with seeded data to test query_owner_digest
and format_owner_message across all branching paths.
"""

import sqlite3
import pytest
from datetime import date, timedelta
from notifications.digest_builder import query_owner_digest, format_owner_message


# ── In-memory DB fixture ───────────────────────────────────────────────────────

def _make_conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE properties (
            id TEXT PRIMARY KEY, name TEXT
        );
        CREATE TABLE daily_snapshot (
            property_id TEXT, date TEXT,
            revenue_total REAL, occupancy_pct REAL, adr REAL,
            rehat_revenue REAL,
            PRIMARY KEY (property_id, date)
        );
        CREATE TABLE budgets (
            property_id TEXT, year INTEGER, month INTEGER,
            revenue_target REAL, updated_at TEXT,
            PRIMARY KEY (property_id, year, month)
        );
        CREATE TABLE bookings_on_books (
            property_id TEXT, stay_date TEXT, booking_number TEXT,
            PRIMARY KEY (property_id, stay_date, booking_number)
        );
    """)
    conn.execute("INSERT INTO properties VALUES ('p1', 'Hotel Bali')")
    conn.commit()
    return conn


def _add_snapshot(conn, prop_id, dt, revenue=1_000_000, occ=80.0, adr=250_000, rehat=100_000):
    conn.execute(
        "INSERT OR REPLACE INTO daily_snapshot VALUES (?,?,?,?,?,?)",
        (prop_id, str(dt), revenue, occ, adr, rehat),
    )
    conn.commit()


def _add_budget(conn, prop_id, year, month, target):
    conn.execute(
        "INSERT OR REPLACE INTO budgets VALUES (?,?,?,?,'2024-01-01')",
        (prop_id, year, month, target),
    )
    conn.commit()


def _add_bob(conn, prop_id, stay_date, booking_number="BK001"):
    conn.execute(
        "INSERT OR REPLACE INTO bookings_on_books VALUES (?,?,?)",
        (prop_id, str(stay_date), booking_number),
    )
    conn.commit()


# ── week_start helper: Monday of a given week_end (Sunday) ────────────────────

def _week_start(week_end: date) -> date:
    return week_end - timedelta(days=6)


# ── Tests for query_owner_digest ──────────────────────────────────────────────

class TestQueryOwnerDigestHappyPath:
    def test_full_week_all_fields(self):
        conn = _make_conn()
        week_end = date(2024, 3, 10)   # Sunday
        ws = _week_start(week_end)
        for i in range(7):
            _add_snapshot(conn, "p1", ws + timedelta(days=i))
        _add_budget(conn, "p1", 2024, 3, 31_000_000)   # 31 days in March
        _add_bob(conn, "p1", week_end + timedelta(days=1))

        data = query_owner_digest(conn, "p1", ws)

        assert data["prop_name"] == "Hotel Bali"
        assert data["days_present"] == 7
        assert data["data_missing"] is False
        assert data["data_partial"] is False
        assert data["week_revenue"] == pytest.approx(7_000_000)
        assert data["bob_30d_rooms"] == 1

    def test_prop_name_fallback_to_id(self):
        conn = _make_conn()
        week_end = date(2024, 3, 10)
        ws = _week_start(week_end)
        data = query_owner_digest(conn, "unknown_prop", ws)
        assert data["prop_name"] == "unknown_prop"


class TestQueryOwnerDigestMissingData:
    def test_data_missing_when_no_rows(self):
        conn = _make_conn()
        week_end = date(2024, 3, 10)
        ws = _week_start(week_end)
        data = query_owner_digest(conn, "p1", ws)
        assert data["data_missing"] is True
        assert data["data_partial"] is False
        assert data["week_revenue"] == 0.0
        assert data["avg_occ_pct"] == 0.0

    def test_data_partial_when_some_rows(self):
        conn = _make_conn()
        week_end = date(2024, 3, 10)
        ws = _week_start(week_end)
        for i in range(4):   # only 4 of 7 days
            _add_snapshot(conn, "p1", ws + timedelta(days=i))
        data = query_owner_digest(conn, "p1", ws)
        assert data["data_partial"] is True
        assert data["data_missing"] is False
        assert data["days_present"] == 4


class TestQueryOwnerDigestBudget:
    def test_no_budget_row_returns_zero_budget_fields(self):
        conn = _make_conn()
        week_end = date(2024, 3, 10)
        ws = _week_start(week_end)
        for i in range(7):
            _add_snapshot(conn, "p1", ws + timedelta(days=i))
        data = query_owner_digest(conn, "p1", ws)
        assert data["week_budget"] == 0.0
        assert data["mtd_budget"] == 0.0
        assert data["week_vs_budget_pct"] == 0.0
        assert data["mtd_attainment_pct"] == 0.0

    def test_budget_proration_weekly(self):
        conn = _make_conn()
        week_end = date(2024, 3, 10)   # March has 31 days
        ws = _week_start(week_end)
        _add_budget(conn, "p1", 2024, 3, 31_000_000)   # 1M/day
        for i in range(7):
            _add_snapshot(conn, "p1", ws + timedelta(days=i))
        data = query_owner_digest(conn, "p1", ws)
        assert data["week_budget"] == pytest.approx(7_000_000)  # 7 days × 1M

    def test_mtd_budget_uses_week_end_day(self):
        conn = _make_conn()
        # week_end = March 10, so MTD = March 1–10 = 10 days
        week_end = date(2024, 3, 10)
        ws = _week_start(week_end)
        _add_budget(conn, "p1", 2024, 3, 31_000_000)
        for i in range(7):
            _add_snapshot(conn, "p1", ws + timedelta(days=i))
        data = query_owner_digest(conn, "p1", ws)
        assert data["mtd_budget"] == pytest.approx(10_000_000)   # 10 days × 1M


class TestQueryOwnerDigestMonthBoundary:
    def test_mtd_anchored_to_week_end_month(self):
        """Week Jan 28 – Feb 3: MTD window = Feb 1–3 (not Jan)."""
        conn = _make_conn()
        week_end = date(2024, 2, 4)   # Sunday Feb 4
        ws = _week_start(week_end)    # Mon Jan 29
        # Add revenue in Jan (should not appear in MTD)
        for i in range(3):
            _add_snapshot(conn, "p1", ws + timedelta(days=i), revenue=999_999)
        # Add revenue in Feb (should appear)
        for i in range(4):
            _add_snapshot(conn, "p1", date(2024, 2, 1) + timedelta(days=i), revenue=500_000)
        data = query_owner_digest(conn, "p1", ws)
        # MTD = Feb 1–4 = 4 days × 500K = 2M
        assert data["mtd_revenue"] == pytest.approx(2_000_000)


class TestQueryOwnerDigestLeapYear:
    def test_leap_year_feb29_does_not_raise(self):
        """Feb 29 2032 (leap year Sunday): LY calculation must not raise ValueError."""
        conn = _make_conn()
        # Feb 29 2032 is a Sunday (weekday=6) — verified
        week_end = date(2032, 2, 29)
        ws = _week_start(week_end)
        # No data needed — just verify no exception
        data = query_owner_digest(conn, "p1", ws)
        assert data["data_missing"] is True   # no data, but no crash

    def test_non_leap_ly_uses_feb28(self):
        """week_end = Feb 29, 2032 → LY end should be Feb 28, 2031."""
        conn = _make_conn()
        week_end = date(2032, 2, 29)
        ws = _week_start(week_end)
        # Add LY data on Feb 28, 2031 — should be included
        _add_snapshot(conn, "p1", date(2031, 2, 1), revenue=777_777)
        _add_snapshot(conn, "p1", date(2031, 2, 28), revenue=777_777)
        data = query_owner_digest(conn, "p1", ws)
        # LY MTD = Feb 1–28, 2031 = both rows = 1_555_554
        assert data["lyy_mtd_revenue"] == pytest.approx(1_555_554)


# ── Tests for format_owner_message ────────────────────────────────────────────

def _base_data(**overrides):
    d = {
        "prop_name": "Hotel Bali",
        "week_start": date(2024, 3, 4),
        "week_end": date(2024, 3, 10),
        "week_revenue": 7_000_000,
        "week_budget": 7_000_000,
        "week_vs_budget_pct": 0.0,
        "mtd_revenue": 10_000_000,
        "mtd_budget": 10_000_000,
        "mtd_attainment_pct": 100.0,
        "lyy_mtd_revenue": 8_000_000,
        "ly_delta_pct": 25.0,
        "avg_occ_pct": 80.0,
        "avg_adr": 250_000,
        "rehat_mtd_fee": 1_000_000,
        "bob_30d_rooms": 45,
        "days_present": 7,
        "data_partial": False,
        "data_missing": False,
    }
    d.update(overrides)
    return d


class TestFormatOwnerMessage:
    def test_data_missing_shows_warning(self):
        msg = format_owner_message(_base_data(data_missing=True, days_present=0))
        assert "No data available" in msg
        assert "Revenue" not in msg

    def test_data_partial_shows_warning(self):
        msg = format_owner_message(_base_data(data_partial=True, days_present=4))
        assert "4/7 days" in msg
        assert "Revenue" in msg   # metrics still shown

    def test_rehat_fee_omitted_when_zero(self):
        msg = format_owner_message(_base_data(rehat_mtd_fee=0.0))
        assert "REHAT fee" not in msg

    def test_rehat_fee_shown_when_positive(self):
        msg = format_owner_message(_base_data(rehat_mtd_fee=1_000_000))
        assert "REHAT fee" in msg

    def test_ly_line_omitted_when_zero(self):
        msg = format_owner_message(_base_data(lyy_mtd_revenue=0.0))
        assert "vs LY" not in msg

    def test_ly_line_shown_when_present(self):
        msg = format_owner_message(_base_data(lyy_mtd_revenue=8_000_000))
        assert "vs LY" in msg

    def test_prop_name_asterisk_stripped(self):
        # Input has internal *; after stripping, name = "Hotel Bali Indah"
        # Template wraps with * for bold: *Hotel Bali Indah* — no double-stars inside
        msg = format_owner_message(_base_data(prop_name="Hotel *Bali* Indah"))
        assert "Hotel Bali Indah" in msg
        assert "Hotel **Bali**" not in msg   # no double-star artifact

    def test_prop_name_underscore_replaced(self):
        msg = format_owner_message(_base_data(prop_name="Hotel_Bali_2"))
        assert "Hotel Bali 2" in msg

    def test_bob_rooms_present(self):
        msg = format_owner_message(_base_data(bob_30d_rooms=45))
        assert "45 room-nights" in msg

    def test_revenue_formatted_in_millions(self):
        msg = format_owner_message(_base_data(week_revenue=7_500_000))
        assert "7.5M" in msg

    def test_adr_formatted_in_thousands(self):
        msg = format_owner_message(_base_data(avg_adr=250_000))
        assert "250K" in msg

    def test_large_adr_uses_millions(self):
        msg = format_owner_message(_base_data(avg_adr=1_500_000))
        assert "1.5M" in msg

    def test_positive_budget_delta_has_plus_sign(self):
        msg = format_owner_message(_base_data(week_vs_budget_pct=15.0))
        assert "+15%" in msg

    def test_negative_budget_delta_has_minus_sign(self):
        msg = format_owner_message(_base_data(week_vs_budget_pct=-8.0))
        assert "-8%" in msg

    def test_footer_present(self):
        msg = format_owner_message(_base_data())
        assert "REHAT Command Center" in msg
