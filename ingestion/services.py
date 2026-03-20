"""
ingestion/services.py — Ingest /analytics/services into raw_services + rebuild daily_snapshot

Flow:
  1. Fetch from Exely API (dateKind=1 — by arrival date)
     Per API spec: dateKind=1 uploads "accruals which dates are within the requested period only."
     Since the API splits each stay into daily rows, this means every service row whose
     service DATE falls within the window is returned — regardless of when the guest
     arrived or departs. This is the correct match for the Exely YieldAndLoad report.
     dateKind=0 (by departure) returns bookings whose departure date falls in the window,
     which would miss stays still in-house whose departure is beyond the window end.
  2. Upsert into raw_services and raw_reservations
  3. Rebuild daily_snapshot for affected dates
"""

import logging
from datetime import date, datetime, timedelta
from typing import Optional

from config import PropertyConfig
from db import get_db, log_ingest
from ingestion.exely_client import ExelyClient, ExelyAPIError

log = logging.getLogger(__name__)


def ingest_services(prop: PropertyConfig, start_date: date, end_date: date):
    """Main entry point. Called by scheduler."""
    client = ExelyClient(prop.api_key, prop.id)
    ran_at = datetime.utcnow().isoformat()

    try:
        data = client.fetch_services_chunked(start_date, end_date, date_kind=1)
    except ExelyAPIError as e:
        log_ingest(prop.id, "services", start_date, end_date, "error", 0, str(e), ran_at)
        log.error(f"[{prop.id}] services fetch failed: {e}")
        return

    services     = data.get("services") or []
    reservations = data.get("reservations") or []
    room_types   = {rt["id"]: rt["name"] for rt in (data.get("roomTypes") or [])}
    agents_by_index = {
        a["index"]: a["name"]
        for a in (data.get("agents") or [])
        if a is not None and "index" in a and "name" in a
    }

    # Log distinct kind values on first real ingest to help calibrate revenue split
    if services:
        kinds = set(s.get("kind") for s in services if s)
        log.info(f"[{prop.id}] service kind values in this batch: {sorted(k for k in kinds if k is not None)}")

    # Build lookup: reservationId → reservation row (filter out None values)
    res_map = {r["id"]: r for r in reservations if r is not None}

    rows_upserted = 0
    affected_dates = set()

    with get_db() as conn:
        for svc in services:
            if svc is None:
                continue
            res = res_map.get(svc.get("reservationId")) or {}
            currency_rate = res.get("currencyRate", 1.0) or 1.0
            amount_idr = (svc.get("amount") or 0) * currency_rate
            svc_date = _parse_date(svc.get("date"))

            # Parse booking creation date from creationDateTime (yyyyMMddHHmm)
            raw_creation = res.get("creationDateTime") or ""
            creation_date = f"{raw_creation[:4]}-{raw_creation[4:6]}-{raw_creation[6:8]}" if len(raw_creation) >= 8 else None

            conn.execute("""
                INSERT INTO raw_services (
                    service_id, property_id, date, reservation_id, booking_number,
                    kind, name, amount, discount, quantity, currency, currency_rate,
                    amount_idr, room_type_id, room_number, guest_name,
                    check_in, check_out, is_arrived, is_departed,
                    payment_method, booking_source, market_code, market_code_name,
                    is_included, agent_name, creation_date, fetched_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(service_id, property_id, date, kind) DO UPDATE SET
                    date=excluded.date,
                    amount=excluded.amount,
                    discount=excluded.discount,
                    quantity=excluded.quantity,
                    amount_idr=excluded.amount_idr,
                    is_arrived=excluded.is_arrived,
                    is_departed=excluded.is_departed,
                    is_included=excluded.is_included,
                    agent_name=excluded.agent_name,
                    creation_date=excluded.creation_date,
                    fetched_at=excluded.fetched_at
            """, (
                svc.get("id"), prop.id,
                svc_date, svc.get("reservationId"), res.get("bookingNumber"),
                svc.get("kind"), svc.get("name"),
                svc.get("amount"), svc.get("discount"), svc.get("quantity"),
                res.get("currency"), currency_rate, amount_idr,
                svc.get("roomTypeId") or res.get("roomTypeId"),
                res.get("roomNumber"), res.get("guestName"),
                res.get("checkInDateTime"), res.get("checkOutDateTime"),
                int(res.get("isArrived", False)), int(res.get("isDeparted", False)),
                res.get("paymentMethod"),
                res.get("bookingSource"),
                res.get("marketCode", {}).get("code") if isinstance(res.get("marketCode"), dict) else None,
                res.get("marketCode", {}).get("name") if isinstance(res.get("marketCode"), dict) else None,
                int(svc.get("isIncluded") or False),
                agents_by_index.get(res.get("agentIndex")),
                creation_date,
                ran_at,
            ))
            rows_upserted += 1
            if svc_date:
                affected_dates.add(svc_date)

        # Upsert channel_mappings defaults for newly seen agent names
        _upsert_channel_mappings(conn, prop.id, agents_by_index)

        # Debug: log per-date kind=0 count so we can verify vs Exely YieldAndLoad
        from collections import Counter
        date_room_counts = Counter()
        for svc in services:
            if svc and svc.get("kind") == 0:
                d = _parse_date(svc.get("date"))
                if d:
                    date_room_counts[d] += 1
        if date_room_counts:
            for d in sorted(date_room_counts):
                log.info(f"[{prop.id}] API kind=0 rows for {d}: {date_room_counts[d]}")

        # Upsert reservations
        for res in reservations:
            currency_rate = res.get("currencyRate", 1.0) or 1.0
            conn.execute("""
                INSERT INTO raw_reservations (
                    reservation_id, property_id, booking_number, room_number, room_type_id,
                    guest_id, guest_name, guest_count, check_in, check_out,
                    is_arrived, is_departed, payment_method, booking_source, market_code,
                    total, paid, balance, currency, currency_rate, created_at, fetched_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(reservation_id, property_id) DO UPDATE SET
                    is_arrived=excluded.is_arrived,
                    is_departed=excluded.is_departed,
                    total=excluded.total,
                    paid=excluded.paid,
                    balance=excluded.balance,
                    fetched_at=excluded.fetched_at
            """, (
                res.get("id"), prop.id, res.get("bookingNumber"),
                res.get("roomNumber"), res.get("roomTypeId"),
                res.get("guestId"), res.get("guestName"), res.get("guestCount"),
                res.get("checkInDateTime"), res.get("checkOutDateTime"),
                int(res.get("isArrived", False)), int(res.get("isDeparted", False)),
                res.get("paymentMethod"), res.get("bookingSource"),
                res.get("marketCode", {}).get("code") if isinstance(res.get("marketCode"), dict) else None,
                res.get("total"), res.get("paid"), res.get("balance"),
                res.get("currency"), currency_rate,
                res.get("creationDateTime"), ran_at,
            ))

    # Rebuild snapshot for all affected dates in one pass (Issue 9A)
    if affected_dates:
        _rebuild_snapshots_batch(prop, sorted(affected_dates))

    log_ingest(prop.id, "services", start_date, end_date, "ok", rows_upserted, None, ran_at)
    log.info(f"[{prop.id}] services: {rows_upserted} rows, {len(affected_dates)} dates rebuilt")



def _upsert_channel_mappings(conn, property_id: str, agents_by_index: dict):
    """
    Auto-populate channel_mappings with defaults for any agent_name not yet seen.
    User can override display_name and channel_type via Settings UI.
    Default logic:
      - "ChannelManager: ..." → indirect, strip prefix for display_name
      - "Exely"               → direct, display_name = "Official Website"
      - NULL / Front desk     → handled separately in Channel Mix as "Front Desk" (direct)
    """
    from datetime import datetime
    now = datetime.utcnow().isoformat()
    for agent_name in agents_by_index.values():
        if not agent_name:
            continue
        raw = agent_name.strip()
        # Normalize display names for known OTAs
        OTA_NAMES = {
            'agoda.com':    'Agoda',
            'traveloka':    'Traveloka',
            'tiket.com':    'Tiket',
            'trip.com group': 'Trip.com',
            'booking.com':  'Booking.com',
            'expedia.com':  'Expedia',
            'airbnb.com':   'Airbnb',
        }
        if raw.startswith('ChannelManager:'):
            raw_ota = raw.replace('ChannelManager:', '').strip().strip('"').lower()
            display = next((v for k, v in OTA_NAMES.items() if k in raw_ota), raw.replace('ChannelManager:', '').strip().strip('"'))
            ctype = 'indirect'
        elif raw == 'Exely':
            display = 'Official Website'
            ctype = 'booking_engine'
        else:
            display = raw
            ctype = 'indirect'
        conn.execute("""
            INSERT INTO channel_mappings (property_id, raw_agent_name, display_name, channel_type, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(property_id, raw_agent_name) DO NOTHING
        """, (property_id, raw, display, ctype, now))



def rebuild_snapshots_from_raw(prop: PropertyConfig, start_date: date, end_date: date):
    """
    Rebuild daily_snapshot for all dates in range that have raw_services data.
    Use this to fill gaps (e.g. dates missing from snapshot but present in raw_services).
    """
    with get_db() as conn:
        rows = conn.execute("""
            SELECT DISTINCT date FROM raw_services
            WHERE property_id=? AND date BETWEEN ? AND ?
            ORDER BY date
        """, (prop.id, str(start_date), str(end_date))).fetchall()
    dates = [r["date"] for r in rows]
    if dates:
        _rebuild_snapshots_batch(prop, dates)
    return len(dates)


def _rebuild_snapshots_batch(prop: PropertyConfig, date_strs: list[str]):
    """
    Rebuild daily_snapshot for multiple dates in a single DB transaction.
    Called after ingest to atomically update all affected dates at once.
    """
    updated_at = datetime.utcnow().isoformat()

    with get_db() as conn:
        for date_str in date_strs:
            row = conn.execute("""
                SELECT
                    SUM(CASE WHEN kind=0 THEN COALESCE(quantity, 1) ELSE 0 END) AS rooms_sold,
                    SUM(CASE WHEN NOT (kind!=0 AND is_included=1) THEN amount_idr ELSE 0 END) AS revenue_total,
                    SUM(CASE WHEN kind=0 THEN amount_idr ELSE 0 END)         AS revenue_rooms,
                    SUM(CASE WHEN kind!=0 AND COALESCE(is_included,0)=0 THEN amount_idr ELSE 0 END) AS revenue_extras,
                    COUNT(DISTINCT booking_number)                            AS bookings_count
                FROM raw_services
                WHERE property_id=? AND date=?
            """, (prop.id, date_str)).fetchone()

            if not row or row["revenue_total"] is None:
                continue

            rooms_sold      = row["rooms_sold"] or 0
            rooms_available = prop.room_count
            revenue_rooms   = row["revenue_rooms"] or 0
            revenue_total   = row["revenue_total"] or 0

            occupancy_pct = (rooms_sold / rooms_available * 100) if rooms_available else 0
            adr    = (revenue_rooms / rooms_sold)      if rooms_sold      else 0
            revpar = (revenue_rooms / rooms_available)  if rooms_available else 0
            rehat_revenue = _calc_rehat_revenue(prop, revenue_total, date_str, conn)

            conn.execute("""
                INSERT INTO daily_snapshot (
                    property_id, date, rooms_sold, rooms_available, occupancy_pct,
                    revenue_total, revenue_rooms, revenue_extras, adr, revpar,
                    rehat_revenue, bookings_count, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(property_id, date) DO UPDATE SET
                    rooms_sold=excluded.rooms_sold,
                    occupancy_pct=excluded.occupancy_pct,
                    revenue_total=excluded.revenue_total,
                    revenue_rooms=excluded.revenue_rooms,
                    revenue_extras=excluded.revenue_extras,
                    adr=excluded.adr,
                    revpar=excluded.revpar,
                    rehat_revenue=excluded.rehat_revenue,
                    bookings_count=excluded.bookings_count,
                    updated_at=excluded.updated_at
            """, (
                prop.id, date_str, rooms_sold, rooms_available, occupancy_pct,
                revenue_total, revenue_rooms, row["revenue_extras"] or 0,
                adr, revpar, rehat_revenue, row["bookings_count"] or 0, updated_at,
            ))


def _calc_rehat_revenue(prop: PropertyConfig, revenue_total: float, date_str: str, conn) -> float:
    """
    Compute REHAT's gross share for one day — before REHAT's own expenses.
    REHAT expenses (lease, advance amortization, system fee, misc) are deducted
    at the monthly P&L level, not here.

    revshare_*    : gross = revenue × revshare_%  (or % of GOP)
    lease         : gross = revenue - hotel opex  (REHAT is the operator; pays rent separately)
    advance_payment: gross = revenue - hotel opex (advance amortization deducted in P&L)
    """
    ct = prop.contract_type
    d = datetime.strptime(date_str, "%Y-%m-%d")
    days_in_month = _days_in_month(d.year, d.month)

    if ct == "revshare_revenue":
        return revenue_total * ((prop.revshare_pct or 0) / 100)

    if ct == "revshare_gop":
        daily_costs = _get_daily_costs(conn, prop.id, d.year, d.month, days_in_month)
        gop = revenue_total - daily_costs
        return gop * ((prop.revshare_gop_pct or 0) / 100)

    if ct == "revshare_revenue_gop":
        daily_costs = _get_daily_costs(conn, prop.id, d.year, d.month, days_in_month)
        gop = revenue_total - daily_costs
        return (revenue_total * ((prop.revshare_pct or 0) / 100)) + (gop * ((prop.revshare_gop_pct or 0) / 100))

    if ct == "lease":
        # REHAT operates the hotel: gross share = revenue after hotel opex.
        # Rent paid to owner is deducted in monthly P&L.
        daily_costs = _get_daily_costs(conn, prop.id, d.year, d.month, days_in_month)
        return revenue_total - daily_costs

    if ct == "advance_payment":
        # Same as lease: REHAT operates hotel. Advance amortization deducted in monthly P&L.
        daily_costs = _get_daily_costs(conn, prop.id, d.year, d.month, days_in_month)
        return revenue_total - daily_costs

    raise ValueError(f"Unknown contract_type '{ct}' for property {prop.id} — check VALID_CONTRACT_TYPES in config.py")


def _get_daily_costs(conn, property_id: str, year: int, month: int, days_in_month: int) -> float:
    row = conn.execute("""
        SELECT SUM(amount) as total FROM monthly_costs
        WHERE property_id=? AND year=? AND month=?
    """, (property_id, year, month)).fetchone()
    total = row["total"] or 0 if row else 0
    return total / days_in_month


def _days_in_month(year: int, month: int) -> int:
    if month == 12:
        return (date(year + 1, 1, 1) - date(year, 12, 1)).days
    return (date(year, month + 1, 1) - date(year, month, 1)).days


def _parse_date(date_str: Optional[str]) -> Optional[str]:
    """Convert yyyyMMdd → YYYY-MM-DD."""
    if not date_str:
        return None
    try:
        return datetime.strptime(str(date_str), "%Y%m%d").strftime("%Y-%m-%d")
    except ValueError:
        return date_str   # already formatted


