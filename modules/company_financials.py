"""
modules/company_financials.py — Company Financials

Portfolio-level P&L from REHAT's perspective.
Aggregates across all active properties applying contract logic per property.

Lines:
  Total Portfolio Revenue
  Total Costs (lease + advance_payment properties only)
  GOP across portfolio
  Total REHAT Revenue (after contract splits)
  REHAT Margin %
  Revenue by contract type
  Top performing properties
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import date, datetime
from calendar import monthrange

from db import get_connection, get_active_properties
from modules.property_kpis import _fmt_idr, _fmt_pct, _month_range, _delta_str
from modules.pnl import (
    _get_revenue_breakdown, _get_costs, _get_ytd_revenue, _get_ytd_costs,
    _days_in_month, REVSHARE_TYPES
)
from modules.budgeting import _get_year_budgets, MONTHS


# ── Aggregation helpers ───────────────────────────────────────────────────────

def _build_company_pnl(conn, props, year, month, is_ytd=False, through_month=None):
    """
    Aggregate P&L across all properties for a given period.
    Returns dict of company-level line items.
    """
    total_revenue    = 0.0
    total_costs      = 0.0
    total_rehat_rev  = 0.0
    by_contract      = {}
    property_rows    = []

    for p in props:
        pid = p["id"]
        ct  = p["contract_type"]

        if is_ytd:
            rev = _get_ytd_revenue(conn, pid, year, through_month)
            costs = _get_ytd_costs(conn, pid, year, through_month)
        else:
            rev   = _get_revenue_breakdown(conn, pid, year, month)
            costs = _get_costs(conn, pid, year, month)

        rev_total   = rev.get("revenue_total") or 0
        rehat_rev   = rev.get("rehat_revenue") or 0
        prop_costs  = sum(costs.values()) if costs else 0

        # Add fixed costs for lease/advance
        if ct == "lease" and p.get("lease_monthly"):
            if is_ytd:
                prop_costs += p["lease_monthly"] * (through_month or month)
            else:
                prop_costs += p["lease_monthly"]

        gop = rev_total - prop_costs

        total_revenue   += rev_total
        if rev_total > 0:
            total_costs += prop_costs
        total_rehat_rev += rehat_rev

        by_contract[ct] = by_contract.get(ct, {"revenue": 0, "rehat": 0, "count": 0})
        by_contract[ct]["revenue"] += rev_total
        by_contract[ct]["rehat"]   += rehat_rev
        by_contract[ct]["count"]   += 1

        if rev_total > 0:
            property_rows.append({
                "id":            pid,
                "name":          p["name"],
                "city":          p.get("city") or "—",
                "contract_type": ct,
                "revenue":       rev_total,
                "costs":         prop_costs,
                "gop":           gop,
                "rehat_revenue": rehat_rev,
                "rehat_margin":  rehat_rev / rev_total * 100 if rev_total else 0,
            })

    total_gop    = total_revenue - total_costs
    rehat_margin = total_rehat_rev / total_revenue * 100 if total_revenue else 0

    return {
        "total_revenue":    total_revenue,
        "total_costs":      total_costs,
        "total_gop":        total_gop,
        "total_rehat_rev":  total_rehat_rev,
        "rehat_margin":     rehat_margin,
        "by_contract":      by_contract,
        "property_rows":    sorted(property_rows, key=lambda r: r["rehat_revenue"], reverse=True),
    }


def _get_total_budget(conn, props, year, month=None, through_month=None):
    """Sum all property budgets for a period."""
    total = 0.0
    for p in props:
        budgets = _get_year_budgets(conn, p["id"], year)
        if through_month:
            total += sum(v for m, v in budgets.items() if m <= through_month and v)
        elif month:
            total += budgets.get(month) or 0
    return total


def _get_monthly_trend(conn, props, year, through_month):
    """Build monthly series for trend chart."""
    months = list(range(1, through_month + 1))
    rows = []
    for m in months:
        pnl = _build_company_pnl(conn, props, year, m)
        bud = _get_total_budget(conn, props, year, month=m)
        rows.append({
            "month":         MONTHS[m - 1],
            "revenue":       pnl["total_revenue"],
            "costs":         pnl["total_costs"],
            "rehat_revenue": pnl["total_rehat_rev"],
            "gop":           pnl["total_gop"],
            "budget":        bud,
        })
    return pd.DataFrame(rows)


# ── Statement renderer ────────────────────────────────────────────────────────

def _render_company_statement(pnl, budget_revenue=None, yoy=None):
    """Render company P&L as metric rows with comparisons."""

    def _row(label, value, is_header=False, yoy_val=None, bud_val=None, is_pct=False):
        fmt = (lambda v: f"{v:.1f}%") if is_pct else _fmt_idr
        if is_header:
            st.markdown(f"**{label}**")
            return
        cols = st.columns([3, 2, 2, 2])
        cols[0].markdown(f"{'&nbsp;' * 4}{label}" if not is_header else f"**{label}**",
                         unsafe_allow_html=True)
        cols[1].markdown(f"**{fmt(value)}**" if value is not None else "—")

        if yoy_val is not None and value is not None:
            d, p = _delta_str(value, yoy_val)
            color = "green" if p else "red"
            cols[2].markdown(f":{color}[{d}]" if d else "—")
        else:
            cols[2].markdown("—")

        if bud_val is not None and value is not None and not is_pct:
            d, p = _delta_str(value, bud_val)
            color = "green" if p else "red"
            cols[3].markdown(f":{color}[{d}]" if d else "—")
        else:
            cols[3].markdown("—")

    yoy_rev   = yoy.get("total_revenue")   if yoy else None
    yoy_costs = yoy.get("total_costs")     if yoy else None
    yoy_gop   = yoy.get("total_gop")       if yoy else None
    yoy_rehat = yoy.get("total_rehat_rev") if yoy else None
    yoy_margin= yoy.get("rehat_margin")    if yoy else None

    # Header row
    hcols = st.columns([3, 2, 2, 2])
    hcols[1].caption("Actual")
    hcols[2].caption("vs YoY")
    hcols[3].caption("vs Budget")

    st.divider()
    _row("Total Portfolio Revenue", pnl["total_revenue"],
         yoy_val=yoy_rev, bud_val=budget_revenue)
    _row("Total Operating Costs", pnl["total_costs"],
         yoy_val=yoy_costs)
    st.divider()
    _row("Gross Operating Profit (GOP)", pnl["total_gop"],
         yoy_val=yoy_gop)
    st.divider()
    _row("Total REHAT Revenue", pnl["total_rehat_rev"],
         yoy_val=yoy_rehat, bud_val=budget_revenue)
    _row("REHAT Margin %", pnl["rehat_margin"],
         yoy_val=yoy_margin, is_pct=True)
    st.divider()


def _render_contract_breakdown(by_contract):
    if not by_contract:
        return
    st.markdown("**Revenue by Contract Type**")
    col_l, col_r = st.columns(2)
    df = pd.DataFrame([
        {"Contract": k, "Properties": v["count"],
         "Revenue": v["revenue"], "REHAT Revenue": v["rehat"]}
        for k, v in by_contract.items()
    ]).sort_values("Revenue", ascending=False)

    with col_l:
        fig = px.pie(df, names="Contract", values="Revenue", hole=0.4)
        fig.update_layout(height=280, margin=dict(l=0,r=0,t=20,b=0))
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        display = df.copy()
        display["Revenue"]       = display["Revenue"].map(_fmt_idr)
        display["REHAT Revenue"] = display["REHAT Revenue"].map(_fmt_idr)
        st.dataframe(display, use_container_width=True, hide_index=True)


def _render_top_properties(property_rows):
    if not property_rows:
        return
    st.markdown("**Property Performance**")
    df = pd.DataFrame(property_rows)
    display = df.copy()
    display["revenue"]       = display["revenue"].map(_fmt_idr)
    display["costs"]         = display["costs"].map(_fmt_idr)
    display["gop"]           = display["gop"].map(_fmt_idr)
    display["rehat_revenue"] = display["rehat_revenue"].map(_fmt_idr)
    display["rehat_margin"]  = display["rehat_margin"].map(lambda v: f"{v:.1f}%")
    display.columns = ["ID","Property","City","Contract",
                       "Revenue","Costs","GOP","REHAT Revenue","REHAT Margin"]
    st.dataframe(display, use_container_width=True, hide_index=True)


# ── Main render ───────────────────────────────────────────────────────────────

def render():
    st.subheader("Company Financials")

    props = sorted(get_active_properties(), key=lambda p: str(p["id"]))
    if not props:
        st.warning("No active properties. Add properties in 🔧 Settings.")
        return

    today = date.today()
    tab_monthly, tab_ytd = st.tabs(["Monthly", "YTD"])

    # ── Monthly tab ───────────────────────────────────────────────────────────
    with tab_monthly:
        col_y, col_m, _ = st.columns([1, 1, 3])
        with col_y:
            year = st.number_input("Year", min_value=2020, max_value=today.year,
                                   value=today.year, step=1, key="cf_m_year")
        with col_m:
            month = st.selectbox(
                "Month", options=list(range(1, 13)),
                format_func=lambda m: MONTHS[m - 1],
                index=today.month - 1, key="cf_m_month",
            )

        conn = get_connection()
        pnl = _build_company_pnl(conn, props, year, month)

        # YoY
        yoy_year = year - 1
        pnl_yoy  = _build_company_pnl(conn, props, yoy_year, month)
        yoy = pnl_yoy if pnl_yoy["total_revenue"] > 0 else None

        # Budget
        bud = _get_total_budget(conn, props, year, month=month)

        if pnl["total_revenue"] == 0:
            conn.close()
            st.info("No revenue data for this period. Run ingestion first.")
        else:
            _render_company_statement(pnl, bud or None, yoy)
            _render_contract_breakdown(pnl["by_contract"])
            _render_top_properties(pnl["property_rows"])
        conn.close()

    # ── YTD tab ───────────────────────────────────────────────────────────────
    with tab_ytd:
        col_y2, col_m2, _ = st.columns([1, 1, 3])
        with col_y2:
            ytd_year = st.number_input("Year", min_value=2020, max_value=today.year,
                                       value=today.year, step=1, key="cf_ytd_year")
        with col_m2:
            ytd_month = st.selectbox(
                "Through month", options=list(range(1, 13)),
                format_func=lambda m: MONTHS[m - 1],
                index=today.month - 1, key="cf_ytd_month",
            )

        conn = get_connection()
        pnl_ytd = _build_company_pnl(conn, props, ytd_year, None,
                                      is_ytd=True, through_month=ytd_month)
        pnl_yoy_ytd = _build_company_pnl(conn, props, ytd_year - 1, None,
                                          is_ytd=True, through_month=ytd_month)
        yoy_ytd = pnl_yoy_ytd if pnl_yoy_ytd["total_revenue"] > 0 else None
        bud_ytd = _get_total_budget(conn, props, ytd_year, through_month=ytd_month)

        if pnl_ytd["total_revenue"] == 0:
            conn.close()
            st.info("No revenue data for this period.")
        else:
            _render_company_statement(pnl_ytd, bud_ytd or None, yoy_ytd)

            # Monthly trend chart
            st.markdown("**Monthly Trend**")
            df_trend = _get_monthly_trend(conn, props, ytd_year, ytd_month)

            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=df_trend["month"], y=df_trend["revenue"],
                name="Portfolio Revenue", opacity=0.6,
            ))
            fig.add_trace(go.Bar(
                x=df_trend["month"], y=df_trend["costs"],
                name="Total Costs", opacity=0.6,
            ))
            fig.add_trace(go.Scatter(
                x=df_trend["month"], y=df_trend["rehat_revenue"],
                name="REHAT Revenue", mode="lines+markers",
                line=dict(width=2), marker=dict(size=5),
            ))
            if df_trend["budget"].sum() > 0:
                fig.add_trace(go.Scatter(
                    x=df_trend["month"], y=df_trend["budget"],
                    name="Budget", mode="lines",
                    line=dict(dash="dash", width=2),
                ))
            fig.update_layout(
                height=320, barmode="group",
                margin=dict(l=0, r=0, t=20, b=0),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
                hovermode="x unified",
            )
            st.plotly_chart(fig, use_container_width=True)

            _render_contract_breakdown(pnl_ytd["by_contract"])
            _render_top_properties(pnl_ytd["property_rows"])

        conn.close()
