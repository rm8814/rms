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
from db import get_db, log_ingest
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
        log_ingest(prop.id, "bookings_search", today, period_to, "error", 0, str(e), ran_at)
        log.error(f"[{prop.id}] bookings search failed: {e}")
        return

    if not booking_numbers:
        log_ingest(prop.id, "bookings_search", today, period_to, "ok", 0, None, ran_at)
        log.info(f"[{prop.id}] No forward bookings found")
        return

    log.info(f"[{prop.id}] Found {len(booking_numbers)} forward bookings")

    # ── Step 2: Fetch each booking and explode into nightly rows ─────────────
    rows_upserted = 0
    errors = 0
    all_stay_rows = []

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
            all_stay_rows.extend(stay_rows)

    # Write all rows in a single transaction (Issue 3A)
    with get_db() as conn:
        # Clear stale future data first (cancellations need to disappear)
        conn.execute("""
            DELETE FROM bookings_on_books
            WHERE property_id=? AND stay_date >= ?
        """, (prop.id, str(today)))
        conn.executemany("""
            INSERT INTO bookings_on_books
                (property_id, stay_date, booking_number, room_type_id,
                 check_in, check_out, status, nightly_rate_idr, fetched_at)
            VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT(property_id, stay_date, booking_number) DO UPDATE SET
                status=excluded.status,
                nightly_rate_idr=excluded.nightly_rate_idr,
                fetched_at=excluded.fetched_at
        """, all_stay_rows)
    rows_upserted = len(all_stay_rows)

    status = "ok" if errors == 0 else "partial"
    log_ingest(prop.id, "bookings", today, period_to, status, rows_upserted,
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


