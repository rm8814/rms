"""
modules/pnl.py — Property P&L

Shows P&L statement per property: revenue breakdown, costs, GOP, REHAT net revenue, margins.
Two tabs: Monthly view and YTD view.

Contract logic:
  revshare_*    : REHAT revenue = total_revenue * revshare_pct%. No costs.
  lease         : REHAT revenue = total_revenue - lease_monthly - opex costs
  advance_payment: REHAT revenue = total_revenue - opex costs (advance is sunk)
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import date, datetime
from calendar import monthrange

from db import get_connection, get_db, get_active_properties
from modules.property_kpis import _fmt_idr, _month_range
from modules.budgeting import _get_monthly_actuals

COST_CATEGORIES = [
    "rooms", "fnb", "sales_marketing", "admin_general",
    "maintenance", "utilities", "salary", "others"
]
COST_LABELS = {
    "rooms":           "Rooms",
    "fnb":             "F&B",
    "sales_marketing": "Sales & Marketing",
    "admin_general":   "Admin & General",
    "maintenance":     "Maintenance & Repairs",
    "utilities":       "Utilities",
    "salary":          "Salary",
    "others":          "Others",
}
REVSHARE_TYPES = {"revshare_revenue", "revshare_gop", "revshare_revenue_gop"}


# ── DB helpers ────────────────────────────────────────────────────────────────

def _get_costs(conn, prop_id, year, month):
    """Return dict {category: amount} for a property/month."""
    rows = conn.execute("""
        SELECT category, amount FROM monthly_costs
        WHERE property_id=? AND year=? AND month=?
    """, (prop_id, year, month)).fetchall()
    return {r["category"]: r["amount"] for r in rows}


def _set_costs(prop_id, year, month, costs: dict):
    """Upsert cost line items."""
    now = datetime.utcnow().isoformat()
    with get_db() as conn:
        for cat, amt in costs.items():
            if amt and amt > 0:
                conn.execute("""
                    INSERT INTO monthly_costs
                        (property_id, year, month, category, amount, updated_at)
                    VALUES (?,?,?,?,?,?)
                    ON CONFLICT(property_id, year, month, category) DO UPDATE SET
                        amount=excluded.amount, updated_at=excluded.updated_at
                """, (prop_id, year, month, cat, amt, now))
            else:
                conn.execute("""
                    DELETE FROM monthly_costs
                    WHERE property_id=? AND year=? AND month=? AND category=?
                """, (prop_id, year, month, cat))


def _get_revenue_breakdown(conn, prop_id, year, month):
    first, last = _month_range(year, month)
    row = conn.execute("""
        SELECT
            SUM(revenue_rooms)  AS revenue_rooms,
            SUM(revenue_extras) AS revenue_extras,
            SUM(revenue_total)  AS revenue_total,
            SUM(rehat_revenue)  AS rehat_revenue
        FROM daily_snapshot
        WHERE property_id=? AND date BETWEEN ? AND ?
    """, (prop_id, str(first), str(last))).fetchone()
    return dict(row) if row else {}


def _get_ytd_revenue(conn, prop_id, year, through_month):
    row = conn.execute("""
        SELECT
            SUM(revenue_rooms)  AS revenue_rooms,
            SUM(revenue_extras) AS revenue_extras,
            SUM(revenue_total)  AS revenue_total,
            SUM(rehat_revenue)  AS rehat_revenue
        FROM daily_snapshot
        WHERE property_id=? AND date BETWEEN ? AND ?
    """, (prop_id, f"{year}-01-01", str(_month_range(year, through_month)[1]))).fetchone()
    return dict(row) if row else {}


def _get_ytd_costs(conn, prop_id, year, through_month):
    rows = conn.execute("""
        SELECT category, SUM(amount) as total
        FROM monthly_costs
        WHERE property_id=? AND year=? AND month<=?
        GROUP BY category
    """, (prop_id, year, through_month)).fetchall()
    return {r["category"]: r["total"] for r in rows}


def _days_in_month(year, month):
    return monthrange(year, month)[1]


def _compute_pnl(prop, revenue: dict, costs: dict, year: int, month: int,
                 is_ytd: bool = False, through_month: int = None):
    """
    Compute P&L lines given revenue dict and costs dict.
    Returns ordered list of (label, value, is_subtotal, indent).
    """
    rev_rooms  = revenue.get("revenue_rooms") or 0
    rev_extras = revenue.get("revenue_extras") or 0
    rev_total  = revenue.get("revenue_total") or 0
    ct = prop["contract_type"]

    lines = []

    # Revenue
    lines.append(("REVENUE", None, True, 0))
    lines.append(("Room Revenue", rev_rooms, False, 1))
    lines.append(("Other Revenue (F&B, Extras)", rev_extras, False, 1))
    lines.append(("Total Revenue", rev_total, True, 0))

    # Costs (only for lease + advance_payment)
    total_costs = sum(costs.values()) if costs else 0

    if ct in REVSHARE_TYPES:
        # No costs — owner pays opex
        lines.append(("COSTS", None, True, 0))
        lines.append(("Operating Costs", 0, False, 1))
        lines.append(("  (Owner bears all costs)", None, False, 2))
        total_costs = 0
    else:
        lines.append(("COSTS", None, True, 0))
        for cat in COST_CATEGORIES:
            amt = costs.get(cat, 0) or 0
            if amt > 0:
                lines.append((COST_LABELS[cat], amt, False, 1))

        # For lease: add fixed rent
        if ct == "lease":
            if is_ytd and through_month:
                months_count = through_month
                lease_amt = (prop.get("lease_monthly") or 0) * months_count
            else:
                lease_amt = prop.get("lease_monthly") or 0
            lines.append(("Fixed Lease (Rent)", lease_amt, False, 1))
            total_costs += lease_amt

        lines.append(("Total Costs", total_costs, True, 0))

    # GOP
    gop = rev_total - total_costs
    gop_margin = (gop / rev_total * 100) if rev_total else 0
    lines.append(("GROSS OPERATING PROFIT", None, True, 0))
    lines.append(("GOP", gop, True, 0))
    lines.append(("GOP Margin %", gop_margin, False, 1))  # stored as pct

    # REHAT net revenue
    rehat_rev = revenue.get("rehat_revenue") or 0
    rehat_margin = (rehat_rev / rev_total * 100) if rev_total else 0
    lines.append(("REHAT NET REVENUE", None, True, 0))
    lines.append(("REHAT Revenue", rehat_rev, True, 0))
    lines.append(("REHAT Margin %", rehat_margin, False, 1))  # stored as pct

    return lines


# ── Chart helpers ─────────────────────────────────────────────────────────────

def _waterfall_chart(lines):
    labels, values, measures = [], [], []
    for label, val, is_subtotal, indent in lines:
        if val is None:
            continue
        if "%" in label:
            continue
        measure = "total" if is_subtotal else "relative"
        # Cost lines are negative
        if any(c in label for c in ["Cost", "Lease", "Rent"]) and val > 0 and not is_subtotal:
            val = -val
            measure = "relative"
        labels.append(label)
        values.append(val)
        measures.append(measure)

    fig = go.Figure(go.Waterfall(
        x=labels, y=values, measure=measures,
        textposition="outside",
        text=[_fmt_idr(abs(v)) for v in values],
        connector={"line": {"color": "rgba(63,63,63,0.2)"}},
    ))
    fig.update_layout(
        height=380,
        margin=dict(l=0, r=0, t=20, b=0),
        showlegend=False,
        xaxis_tickangle=-30,
    )
    return fig


def _monthly_trend_chart(conn, prop, year, through_month):
    """Revenue vs costs vs REHAT revenue by month."""
    months = list(range(1, through_month + 1))
    rev_list, cost_list, rehat_list = [], [], []

    for m in months:
        rev = _get_revenue_breakdown(conn, prop["id"], year, m)
        costs = _get_costs(conn, prop["id"], year, m)
        total_costs = sum(costs.values()) if costs else 0
        if prop["contract_type"] == "lease":
            total_costs += prop.get("lease_monthly") or 0
        rev_list.append(rev.get("revenue_total") or 0)
        cost_list.append(total_costs)
        rehat_list.append(rev.get("rehat_revenue") or 0)

    from modules.budgeting import MONTHS
    labels = [MONTHS[m - 1] for m in months]

    fig = go.Figure()
    fig.add_trace(go.Bar(x=labels, y=rev_list, name="Total Revenue", opacity=0.7))
    fig.add_trace(go.Bar(x=labels, y=cost_list, name="Total Costs", opacity=0.7))
    fig.add_trace(go.Scatter(
        x=labels, y=rehat_list, name="REHAT Revenue",
        mode="lines+markers", line=dict(width=2), marker=dict(size=5),
    ))
    fig.update_layout(
        height=320, barmode="group",
        margin=dict(l=0, r=0, t=20, b=0),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        hovermode="x unified",
    )
    return fig


# ── P&L statement renderer ────────────────────────────────────────────────────

def _render_pnl_statement(lines):
    """Render P&L as a clean table."""
    rows = []
    for label, val, is_subtotal, indent in lines:
        if val is None and is_subtotal:
            rows.append({"Line Item": f"**{label}**", "Amount": ""})
            continue
        if val is None:
            rows.append({"Line Item": f"{'　' * indent}{label}", "Amount": ""})
            continue
        if "%" in label:
            fmt = f"{val:.1f}%"
        else:
            fmt = _fmt_idr(val)
        prefix = "　" * indent
        item = f"**{prefix}{label}**" if is_subtotal else f"{prefix}{label}"
        rows.append({"Line Item": item, "Amount": fmt})

    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True, height=420)


# ── Cost input form ───────────────────────────────────────────────────────────

def _render_cost_input(prop, year, month):
    from modules.budgeting import MONTHS
    ct = prop["contract_type"]

    if ct in REVSHARE_TYPES:
        st.info(f"**{prop['name']}** is on a **{ct}** contract. Owner bears all operating costs — no cost input needed.")
        return

    st.markdown(f"**Operating Costs — {prop['name']} · {MONTHS[month-1]} {year}**")

    conn = get_connection()
    existing = _get_costs(conn, prop["id"], year, month)
    conn.close()

    if ct == "lease":
        lease = prop.get("lease_monthly") or 0
        st.caption(f"Fixed lease: **{_fmt_idr(lease)} / month** (auto-included in P&L)")

    with st.form(f"cost_form_{prop['id']}_{year}_{month}"):
        inputs = {}
        col1, col2 = st.columns(2)
        cats = COST_CATEGORIES
        for i, cat in enumerate(cats):
            col = col1 if i % 2 == 0 else col2
            with col:
                inputs[cat] = st.number_input(
                    COST_LABELS[cat],
                    min_value=0.0,
                    value=float(existing.get(cat) or 0),
                    step=500_000.0,
                    format="%.0f",
                    key=f"cost_{prop['id']}_{year}_{month}_{cat}",
                )
        submitted = st.form_submit_button("💾 Save Costs", type="primary", use_container_width=True)

    if submitted:
        _set_costs(prop["id"], year, month, inputs)
        st.success("✓ Costs saved.")
        st.rerun()


# ── Main render ───────────────────────────────────────────────────────────────

def render():
    st.subheader("Property P&L")

    props = sorted(get_active_properties(), key=lambda p: str(p["id"]))
    if not props:
        st.warning("No active properties. Add properties in 🔧 Settings.")
        return

    prop_options = {p["id"]: p["name"] for p in props}
    today = date.today()

    # Property selector
    selected_id = st.selectbox(
        "Property",
        options=list(prop_options.keys()),
        format_func=lambda k: prop_options[k],
        key="pnl_prop",
    )
    prop = next(p for p in props if p["id"] == selected_id)

    tab_monthly, tab_ytd, tab_costs = st.tabs(["Monthly", "YTD", "Input Costs"])

    # ── Monthly tab ───────────────────────────────────────────────────────────
    with tab_monthly:
        col_y, col_m, _ = st.columns([1, 1, 3])
        with col_y:
            year = st.number_input("Year", min_value=2020, max_value=today.year,
                                   value=today.year, step=1, key="pnl_m_year")
        with col_m:
            month = st.selectbox("Month", options=list(range(1, 13)),
                                 format_func=lambda m: ["Jan","Feb","Mar","Apr","May","Jun",
                                                         "Jul","Aug","Sep","Oct","Nov","Dec"][m-1],
                                 index=today.month - 1, key="pnl_m_month")

        conn = get_connection()
        revenue = _get_revenue_breakdown(conn, selected_id, year, month)
        costs   = _get_costs(conn, selected_id, year, month)
        conn.close()

        if not revenue.get("revenue_total"):
            st.info("No revenue data for this period. Run ingestion first.")
        else:
            lines = _compute_pnl(prop, revenue, costs, year, month)
            col_l, col_r = st.columns([1, 1])
            with col_l:
                _render_pnl_statement(lines)
            with col_r:
                st.plotly_chart(_waterfall_chart(lines), use_container_width=True)

    # ── YTD tab ───────────────────────────────────────────────────────────────
    with tab_ytd:
        col_y2, col_m2, _ = st.columns([1, 1, 3])
        with col_y2:
            ytd_year = st.number_input("Year", min_value=2020, max_value=today.year,
                                       value=today.year, step=1, key="pnl_ytd_year")
        with col_m2:
            ytd_month = st.selectbox(
                "Through month",
                options=list(range(1, 13)),
                format_func=lambda m: ["Jan","Feb","Mar","Apr","May","Jun",
                                        "Jul","Aug","Sep","Oct","Nov","Dec"][m-1],
                index=today.month - 1,
                key="pnl_ytd_month",
            )

        conn = get_connection()
        ytd_revenue = _get_ytd_revenue(conn, selected_id, ytd_year, ytd_month)
        ytd_costs   = _get_ytd_costs(conn, selected_id, ytd_year, ytd_month)

        if not ytd_revenue.get("revenue_total"):
            conn.close()
            st.info("No revenue data for this period.")
        else:
            lines_ytd = _compute_pnl(prop, ytd_revenue, ytd_costs, ytd_year,
                                     ytd_month, is_ytd=True, through_month=ytd_month)
            col_l2, col_r2 = st.columns([1, 1])
            with col_l2:
                _render_pnl_statement(lines_ytd)
            with col_r2:
                st.plotly_chart(_waterfall_chart(lines_ytd), use_container_width=True)

            st.markdown("**Monthly Trend**")
            st.plotly_chart(
                _monthly_trend_chart(conn, prop, ytd_year, ytd_month),
                use_container_width=True,
            )
        conn.close()

    # ── Cost input tab ────────────────────────────────────────────────────────
    with tab_costs:
        col_cy, col_cm, _ = st.columns([1, 1, 3])
        with col_cy:
            cost_year = st.number_input("Year", min_value=2020, max_value=today.year,
                                        value=today.year, step=1, key="pnl_cost_year")
        with col_cm:
            cost_month = st.selectbox(
                "Month",
                options=list(range(1, 13)),
                format_func=lambda m: ["Jan","Feb","Mar","Apr","May","Jun",
                                        "Jul","Aug","Sep","Oct","Nov","Dec"][m-1],
                index=today.month - 1,
                key="pnl_cost_month",
            )
        _render_cost_input(prop, cost_year, cost_month)
