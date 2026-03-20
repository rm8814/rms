"""
ingestion/bookings.py — Ingest forward bookings into bookings_on_books

Flow:
  1. Search bookings affecting next 90 days via /bookings?affectsPeriodFrom/To
  2. For each booking number, fetch full detail via /bookings/{number}
  3. Explode each stay into per-night rows → bookings_on_books
     (one row per property+stay_date+booking_number)

This gives us pickup/pace: for any future date we can count
rooms on books and compare vs same date last year.
"""

import logging
from datetime import date, datetime, time, timedelta
from typing import Optional

from config import PropertyConfig
from db import get_db, get_connection
from ingestion.exely_client import ExelyClient, ExelyAPIError

log = logging.getLogger(__name__)

FORWARD_DAYS = 90   # how far ahead to fetch


def ingest_bookings(prop: PropertyConfig, forward_days: int = FORWARD_DAYS):
    """Fetch active future bookings and explode into nightly rows."""
    client = ExelyClient(prop.api_key, prop.id)
    ran_at = datetime.utcnow().isoformat()
    today  = date.today()
    period_to = today + timedelta(days=forward_days)

    # ── Step 1: Search for booking numbers ───────────────────────────────────
    try:
        booking_numbers = client.search_bookings(
            affects_from=datetime.combine(today, time.min),
            affects_to=datetime.combine(period_to, time(23, 59)),
            state="Active",
        )
    except ExelyAPIError as e:
        _log_ingest(prop.id, "bookings_search", today, period_to, "error", 0, str(e), ran_at)
        log.error(f"[{prop.id}] bookings search failed: {e}")
        return

    if not booking_numbers:
        _log_ingest(prop.id, "bookings_search", today, period_to, "ok", 0, None, ran_at)
        log.info(f"[{prop.id}] No forward bookings found")
        return

    log.info(f"[{prop.id}] Found {len(booking_numbers)} forward bookings")

    # ── Step 2: Fetch each booking and explode into nightly rows ─────────────
    rows_upserted = 0
    errors = 0

    # Clear stale future data for this property before reinserting
    # (cancellations need to disappear — simpler to replace than diff)
    with get_db() as conn:
        conn.execute("""
            DELETE FROM bookings_on_books
            WHERE property_id=? AND stay_date >= ?
        """, (prop.id, str(today)))

    for number in booking_numbers:
        try:
            booking = client.get_booking(number)
        except ExelyAPIError as e:
            log.warning(f"[{prop.id}] Failed to fetch booking {number}: {e}")
            errors += 1
            continue

        # API spec: /bookings/{number} returns currencyId (code) only — no currencyRate.
        # currencyRate exists only on /analytics/services reservations rows.
        # We cannot do IDR conversion here; store raw amount + currency for downstream use.
        booking_currency_id = booking.get("currencyId")
        for stay in booking.get("roomStays", []):
            stay["_currencyId"] = booking_currency_id
            stay_rows = _explode_stay(prop.id, number, stay, today, period_to, ran_at)
            if not stay_rows:
                continue
            with get_db() as conn:
                conn.executemany("""
                    INSERT INTO bookings_on_books
                        (property_id, stay_date, booking_number, room_type_id,
                         check_in, check_out, status, nightly_rate_idr, fetched_at)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(property_id, stay_date, booking_number) DO UPDATE SET
                        status=excluded.status,
                        nightly_rate_idr=excluded.nightly_rate_idr,
                        fetched_at=excluded.fetched_at
                """, stay_rows)
            rows_upserted += len(stay_rows)

    status = "ok" if errors == 0 else "partial"
    _log_ingest(prop.id, "bookings", today, period_to, status, rows_upserted,
                f"{errors} fetch errors" if errors else None, ran_at)
    log.info(f"[{prop.id}] bookings: {rows_upserted} nightly rows, {errors} errors")


def _explode_stay(prop_id, booking_number, stay, today, period_to, ran_at):
    """
    Turn a RoomStay into one row per night that falls within [today, period_to].
    Returns list of tuples matching bookings_on_books columns.
    """
    status = stay.get("status", "New")
    booking_status = stay.get("bookingStatus", "")
    # Per API spec: status is the stay-level status; bookingStatus is the booking-level status.
    # Skip if either is Cancelled.
    if status == "Cancelled" or booking_status == "Cancelled":
        return []

    check_in_str  = stay.get("checkInDateTime", "")[:10]   # yyyy-MM-dd
    check_out_str = stay.get("checkOutDateTime", "")[:10]

    if not check_in_str or not check_out_str:
        return []

    try:
        ci = date.fromisoformat(check_in_str)
        co = date.fromisoformat(check_out_str)
    except ValueError:
        return []

    room_type_id = stay.get("roomTypeId")

    # Extract nightly rate.
    # Per API spec: RoomStay.totalPrice.amount = total for entire stay (excl. tax).
    # /bookings/{number} provides currencyId (code string) only — no exchange rate.
    # We cannot convert to IDR here; store raw per-night amount in booking currency.
    nightly_rate_idr = None
    nights = max((co - ci).days, 1)

    total_price = stay.get("totalPrice") or {}
    total_amount = total_price.get("amount")
    currency_id = stay.get("_currencyId")  # e.g. "IDR", "USD"

    if total_amount is not None:
        raw_nightly = round(float(total_amount) / nights, 2)
        # Only treat as IDR if booking currency is IDR; otherwise None to avoid silent wrong values.
        nightly_rate_idr = raw_nightly if currency_id == "IDR" else None

    rows = []
    cursor = max(ci, today)   # don't backfill past dates
    while cursor < co and cursor <= period_to:
        rows.append((
            prop_id,
            str(cursor),
            booking_number,
            str(room_type_id) if room_type_id else None,
            check_in_str,
            check_out_str,
            status,
            nightly_rate_idr,
            ran_at,
        ))
        cursor += timedelta(days=1)
    return rows


def get_bob_series(conn, prop_id: str, date_from: date, date_to: date) -> dict:
    """
    Return rooms-on-books and revenue-on-books per date for a property.
    Result: {date_str: {"rooms": int, "revenue": float}}
    Revenue = SUM(nightly_rate_idr) per stay_date (one rate per booking per night).
    Falls back to 0 revenue if rates not yet populated.
    """
    rows = conn.execute("""
        SELECT stay_date,
               COUNT(*)                         AS rooms,
               SUM(COALESCE(nightly_rate_idr,0)) AS revenue
        FROM bookings_on_books
        WHERE property_id=? AND stay_date BETWEEN ? AND ?
        GROUP BY stay_date
        ORDER BY stay_date
    """, (prop_id, str(date_from), str(date_to))).fetchall()
    return {r["stay_date"]: {"rooms": r["rooms"], "revenue": r["revenue"] or 0} for r in rows}


def get_bob_summary(conn, prop_id: str, date_from: date, date_to: date) -> dict:
    """Aggregate BOB stats for a date window."""
    row = conn.execute("""
        SELECT
            COUNT(DISTINCT stay_date)        AS days_with_bookings,
            COUNT(DISTINCT booking_number)   AS total_bookings,
            COUNT(*)                         AS total_room_nights
        FROM bookings_on_books
        WHERE property_id=? AND stay_date BETWEEN ? AND ?
    """, (prop_id, str(date_from), str(date_to))).fetchone()
    return dict(row) if row else {}


def _log_ingest(property_id, endpoint, date_from, date_to, status, rows, error, ran_at):
    with get_db() as conn:
        conn.execute("""
            INSERT INTO ingest_log
                (property_id, endpoint, date_from, date_to, status, rows_upserted, error_msg, ran_at)
            VALUES (?,?,?,?,?,?,?,?)
        """, (property_id, endpoint, str(date_from), str(date_to), status, rows, error, ran_at))


def snapshot_bob_today(prop_id: str):
    """
    Write today's BOB room counts into bob_snapshots.
    Call once per day after bookings ingest.
    Overwrites today's capture if already exists (idempotent).
    """
    today = str(date.today())
    with get_db() as conn:
        rows = conn.execute("""
            SELECT stay_date, COUNT(*) AS rooms
            FROM bookings_on_books
            WHERE property_id=? AND stay_date >= ?
            GROUP BY stay_date
        """, (prop_id, today)).fetchall()

        # Delete today's existing snapshot first (clean re-run)
        conn.execute(
            "DELETE FROM bob_snapshots WHERE property_id=? AND capture_date=?",
            (prop_id, today)
        )
        conn.executemany(
            "INSERT INTO bob_snapshots (property_id, capture_date, stay_date, rooms) VALUES (?,?,?,?)",
            [(prop_id, today, r["stay_date"], r["rooms"]) for r in rows]
        )
