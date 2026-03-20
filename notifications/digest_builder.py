"""
notifications/digest_builder.py — Build owner digest content from SQLite data.

Data flow for query_owner_digest:
  properties             → prop_name
  daily_snapshot (week)  → revenue, adr, occupancy, rehat_revenue
  daily_snapshot (MTD)   → mtd_revenue, rehat_mtd_fee
  daily_snapshot (LY MTD)→ lyy_mtd_revenue
  budgets                → monthly revenue_target → pro-rate to week + MTD
  bookings_on_books      → bob_30d_rooms (COUNT of room-nights, next 30 days)
"""

import calendar
from datetime import date, timedelta


def query_owner_digest(conn, prop_id: str, week_start: date) -> dict:
    """
    Build the data dict for the owner weekly digest.

    week_start: Monday of the week being reported.
                Caller computes: date.today() - timedelta(days=6) (job fires Sunday).
    week_end:   week_start + 6 days (the Sunday the job fires on).

    MTD window: 1st of week_end's month through week_end.
    When a week straddles a month boundary (e.g. Jan 28–Feb 3), MTD is anchored
    to week_end's month (Feb). Budget pro-ration uses week_end's year/month.

    LY MTD: same calendar range with year-1, using explicit date() to handle
            leap years (Feb 29 → Feb 28 in non-leap year).
    """
    week_end = week_start + timedelta(days=6)
    today = week_end  # job fires on week_end date

    # ── prop_name ─────────────────────────────────────────────────────────────
    row = conn.execute(
        "SELECT name FROM properties WHERE id=?", (prop_id,)
    ).fetchone()
    prop_name = row["name"] if row else prop_id

    # ── Week revenue / occ / adr ──────────────────────────────────────────────
    week_rows = conn.execute("""
        SELECT revenue_total, occupancy_pct, adr, rehat_revenue
        FROM daily_snapshot
        WHERE property_id=? AND date BETWEEN ? AND ?
    """, (prop_id, str(week_start), str(week_end))).fetchall()

    days_present = len(week_rows)
    week_revenue    = sum(r["revenue_total"] or 0 for r in week_rows)
    avg_occ_pct     = (sum(r["occupancy_pct"] or 0 for r in week_rows) / days_present) if days_present else 0.0
    avg_adr         = (sum(r["adr"] or 0 for r in week_rows) / days_present) if days_present else 0.0

    # ── MTD ───────────────────────────────────────────────────────────────────
    mtd_start = date(week_end.year, week_end.month, 1)
    mtd_rows = conn.execute("""
        SELECT revenue_total, rehat_revenue
        FROM daily_snapshot
        WHERE property_id=? AND date BETWEEN ? AND ?
    """, (prop_id, str(mtd_start), str(week_end))).fetchall()

    mtd_revenue   = sum(r["revenue_total"] or 0 for r in mtd_rows)
    rehat_mtd_fee = sum(r["rehat_revenue"] or 0 for r in mtd_rows)

    # ── LY MTD ────────────────────────────────────────────────────────────────
    try:
        ly_end = date(week_end.year - 1, week_end.month, week_end.day)
    except ValueError:
        # Feb 29 in a leap year → fall back to Feb 28 in prior year
        ly_end = date(week_end.year - 1, week_end.month, 28)
    ly_start = date(ly_end.year, ly_end.month, 1)

    lyy_mtd_revenue = conn.execute("""
        SELECT COALESCE(SUM(revenue_total), 0)
        FROM daily_snapshot
        WHERE property_id=? AND date BETWEEN ? AND ?
    """, (prop_id, str(ly_start), str(ly_end))).fetchone()[0] or 0.0

    # ── Budget ────────────────────────────────────────────────────────────────
    budget_row = conn.execute("""
        SELECT revenue_target FROM budgets
        WHERE property_id=? AND year=? AND month=?
    """, (prop_id, week_end.year, week_end.month)).fetchone()

    days_in_month = calendar.monthrange(week_end.year, week_end.month)[1]
    if budget_row and budget_row["revenue_target"]:
        monthly_budget = float(budget_row["revenue_target"])
        week_budget    = monthly_budget / days_in_month * 7
        mtd_budget     = monthly_budget / days_in_month * week_end.day
    else:
        monthly_budget = week_budget = mtd_budget = 0.0

    # ── BOB 30d ───────────────────────────────────────────────────────────────
    period_end_30 = today + timedelta(days=29)
    bob_30d_rooms = conn.execute("""
        SELECT COUNT(*) FROM bookings_on_books
        WHERE property_id=? AND stay_date BETWEEN ? AND ?
    """, (prop_id, str(today), str(period_end_30))).fetchone()[0] or 0

    # ── Computed percentages (safe div) ───────────────────────────────────────
    week_vs_budget_pct  = ((week_revenue / week_budget) - 1) * 100 if week_budget else 0.0
    mtd_attainment_pct  = (mtd_revenue / mtd_budget) * 100 if mtd_budget else 0.0
    ly_delta_pct        = ((mtd_revenue / lyy_mtd_revenue) - 1) * 100 if lyy_mtd_revenue else 0.0

    return {
        "prop_name":          prop_name,
        "week_start":         week_start,
        "week_end":           week_end,
        "week_revenue":       week_revenue,
        "week_budget":        week_budget,
        "week_vs_budget_pct": week_vs_budget_pct,
        "mtd_revenue":        mtd_revenue,
        "mtd_budget":         mtd_budget,
        "mtd_attainment_pct": mtd_attainment_pct,
        "lyy_mtd_revenue":    lyy_mtd_revenue,
        "ly_delta_pct":       ly_delta_pct,
        "avg_occ_pct":        avg_occ_pct,
        "avg_adr":            avg_adr,
        "rehat_mtd_fee":      rehat_mtd_fee,
        "bob_30d_rooms":      bob_30d_rooms,
        "days_present":       days_present,
        "data_partial":       0 < days_present < 7,
        "data_missing":       days_present == 0,
    }


def format_owner_message(data: dict) -> str:
    """
    Format the owner weekly digest as a Telegram Markdown string.
    parse_mode="Markdown" (legacy) — no MarkdownV2 escaping needed.

    Prop name: strip '*' and '_' to prevent unmatched Markdown tokens.
    """
    safe_name = data["prop_name"].replace("*", "").replace("_", " ")
    ws = data["week_start"]
    we = data["week_end"]

    header = (
        f"🏨 *{safe_name}* — Weekly Report\n"
        f"Mon {ws.strftime('%d %b')} – Sun {we.strftime('%d %b %Y')}"
    )

    if data["data_missing"]:
        return f"{header}\n\n⚠️ No data available for this week.\n\n_REHAT Command Center_"

    lines = [header, ""]

    if data["data_partial"]:
        lines.append(f"⚠️ Only {data['days_present']}/7 days of data available.")

    def fmt_m(v):
        return f"{v / 1_000_000:.1f}M"

    def fmt_k(v):
        if v >= 1_000_000:
            return f"{v / 1_000_000:.1f}M"
        return f"{v / 1_000:.0f}K"

    lines.append(
        f"💰 Revenue:    Rp {fmt_m(data['week_revenue'])}  "
        f"({data['week_vs_budget_pct']:+.0f}% vs budget)"
    )

    if data["rehat_mtd_fee"] > 0.01:
        lines.append(f"🏷️ REHAT fee:  Rp {fmt_m(data['rehat_mtd_fee'])} (MTD)")

    lines.append(
        f"📊 Occupancy:  {data['avg_occ_pct']:.0f}%  |  "
        f"ADR: Rp {fmt_k(data['avg_adr'])}"
    )
    lines.append(
        f"📅 MTD:        Rp {fmt_m(data['mtd_revenue'])} / "
        f"Rp {fmt_m(data['mtd_budget'])} budget "
        f"({data['mtd_attainment_pct']:.0f}%)"
    )

    if data["lyy_mtd_revenue"] > 0.01:
        lines.append(
            f"📅 vs LY MTD:  Rp {fmt_m(data['lyy_mtd_revenue'])} "
            f"({data['ly_delta_pct']:+.0f}%)"
        )

    lines.append(f"🔮 BOB 30d:    {data['bob_30d_rooms']} room-nights")
    lines.append("")
    lines.append("_REHAT Command Center_")

    return "\n".join(lines)
