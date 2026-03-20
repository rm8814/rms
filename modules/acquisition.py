"""
modules/acquisition.py — Acquisition Analytics

Analyze potential new properties before signing.
Inputs: manual assumptions (revenue, occupancy, room count, contract terms).
Outputs: projected REHAT revenue, break-even, payback period, benchmark vs portfolio.

Targets are saved to DB so you can revisit and compare.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import date, datetime

from db import get_connection, get_db, get_active_properties
from modules.property_kpis import _fmt_idr, _fmt_pct, _delta_str
from modules.budgeting import MONTHS

CONTRACT_TYPES = [
    "revshare_revenue",
    "revshare_gop",
    "revshare_revenue_gop",
    "lease",
    "advance_payment",
]
REVSHARE_TYPES = {"revshare_revenue", "revshare_gop", "revshare_revenue_gop"}

COST_CATEGORIES = [
    "rooms", "fnb", "sales_marketing", "admin_general",
    "maintenance", "utilities", "salary", "others"
]
COST_LABELS = {
    "rooms": "Rooms", "fnb": "F&B", "sales_marketing": "Sales & Marketing",
    "admin_general": "Admin & General", "maintenance": "Maintenance & Repairs",
    "utilities": "Utilities", "salary": "Salary", "others": "Others",
}


# ── DB helpers ────────────────────────────────────────────────────────────────

def _init_targets_table():
    """Ensure acquisition_targets table exists."""
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS acquisition_targets (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                name            TEXT NOT NULL,
                city            TEXT,
                room_count      INTEGER,
                contract_type   TEXT,
                revshare_pct    REAL,
                lease_monthly   REAL,
                advance_total   REAL,
                exp_occ_pct     REAL,
                exp_adr         REAL,
                exp_monthly_costs REAL,
                notes           TEXT,
                created_at      TEXT,
                updated_at      TEXT
            )
        """)


def _save_target(data: dict) -> int:
    now = datetime.utcnow().isoformat()
    with get_db() as conn:
        cur = conn.execute("""
            INSERT INTO acquisition_targets
                (name, city, room_count, contract_type, revshare_pct,
                 lease_monthly, advance_total, exp_occ_pct, exp_adr,
                 exp_monthly_costs, notes, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (data["name"], data.get("city"), data["room_count"],
              data["contract_type"], data.get("revshare_pct"),
              data.get("lease_monthly"), data.get("advance_total"),
              data["exp_occ_pct"], data["exp_adr"],
              data.get("exp_monthly_costs", 0),
              data.get("notes"), now, now))
        return cur.lastrowid


def _update_target(tid: int, data: dict):
    now = datetime.utcnow().isoformat()
    with get_db() as conn:
        conn.execute("""
            UPDATE acquisition_targets SET
                name=?, city=?, room_count=?, contract_type=?,
                revshare_pct=?, lease_monthly=?, advance_total=?,
                exp_occ_pct=?, exp_adr=?, exp_monthly_costs=?,
                notes=?, updated_at=?
            WHERE id=?
        """, (data["name"], data.get("city"), data["room_count"],
              data["contract_type"], data.get("revshare_pct"),
              data.get("lease_monthly"), data.get("advance_total"),
              data["exp_occ_pct"], data["exp_adr"],
              data.get("exp_monthly_costs", 0),
              data.get("notes"), now, tid))


def _delete_target(tid: int):
    with get_db() as conn:
        conn.execute("DELETE FROM acquisition_targets WHERE id=?", (tid,))


def _get_targets() -> list[dict]:
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM acquisition_targets ORDER BY created_at DESC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── Financial model ───────────────────────────────────────────────────────────

def _project(t: dict, months: int = 12) -> dict:
    """
    Project financials for a target property.
    Returns monthly and annual figures.
    """
    rooms        = t["room_count"] or 1
    occ          = (t["exp_occ_pct"] or 0) / 100
    adr          = t["exp_adr"] or 0
    ct           = t["contract_type"]
    revshare_pct = (t.get("revshare_pct") or 0) / 100
    lease_mo     = t.get("lease_monthly") or 0
    advance      = t.get("advance_total") or 0
    opex_mo      = t.get("exp_monthly_costs") or 0

    # Monthly revenue
    days_in_month   = 30.44   # avg
    rooms_sold_mo   = rooms * occ * days_in_month
    rev_rooms_mo    = rooms_sold_mo * adr
    rev_total_mo    = rev_rooms_mo   # assume revenue = room revenue for projections

    # Monthly REHAT revenue by contract
    if ct in REVSHARE_TYPES:
        rehat_mo = rev_total_mo * revshare_pct
        costs_mo = 0
    elif ct == "lease":
        costs_mo = lease_mo + opex_mo
        rehat_mo = rev_total_mo - costs_mo
    else:  # advance_payment
        costs_mo = opex_mo
        rehat_mo = rev_total_mo - costs_mo

    gop_mo     = rev_total_mo - costs_mo
    margin_mo  = rehat_mo / rev_total_mo * 100 if rev_total_mo else 0

    # Annual
    rev_annual   = rev_total_mo * months
    rehat_annual = rehat_mo * months
    costs_annual = costs_mo * months
    gop_annual   = gop_mo * months

    # Break-even (monthly revenue needed for REHAT to break even)
    if ct == "lease":
        breakeven_rev = costs_mo   # need at least costs covered
    elif ct == "advance_payment":
        breakeven_rev = opex_mo
    else:
        breakeven_rev = 0   # revshare always positive if revenue > 0

    # Payback period (advance payment only)
    payback_months = None
    if ct == "advance_payment" and rehat_mo > 0:
        payback_months = advance / rehat_mo

    # Monthly series for chart
    monthly = []
    cumulative_rehat = -advance if ct == "advance_payment" else 0
    for m in range(1, months + 1):
        cumulative_rehat += rehat_mo
        monthly.append({
            "month":             MONTHS[m - 1] if m <= 12 else f"M{m}",
            "revenue":           rev_total_mo,
            "costs":             costs_mo,
            "rehat_revenue":     rehat_mo,
            "cumulative_rehat":  cumulative_rehat,
        })

    return {
        "rev_total_mo":   rev_total_mo,
        "rehat_mo":       rehat_mo,
        "costs_mo":       costs_mo,
        "gop_mo":         gop_mo,
        "margin_mo":      margin_mo,
        "rev_annual":     rev_annual,
        "rehat_annual":   rehat_annual,
        "costs_annual":   costs_annual,
        "gop_annual":     gop_annual,
        "breakeven_rev":  breakeven_rev,
        "payback_months": payback_months,
        "monthly":        monthly,
    }


def _portfolio_benchmarks(conn) -> dict:
    """Compute portfolio-level averages for benchmarking against last 90 days."""
    row = conn.execute("""
        SELECT
            AVG(occupancy_pct)                               AS avg_occ,
            SUM(revenue_rooms) / NULLIF(SUM(rooms_sold), 0) AS avg_adr,
            AVG(revpar)                                      AS avg_revpar,
            SUM(rehat_revenue) / NULLIF(SUM(revenue_total), 0) * 100 AS avg_rehat_margin
        FROM daily_snapshot
        WHERE date >= date('now', '-90 days')
    """).fetchone()
    return dict(row) if row else {}


# ── Form ──────────────────────────────────────────────────────────────────────

def _target_form(key_prefix: str, defaults: dict = None) -> dict | None:
    """Render target input form. Returns submitted data dict or None."""
    d = defaults or {}

    with st.form(f"target_form_{key_prefix}"):
        col1, col2 = st.columns(2)

        with col1:
            name       = st.text_input("Property Name *", value=d.get("name",""))
            city       = st.text_input("City", value=d.get("city",""))
            room_count = st.number_input("Room Count *", min_value=1,
                                         value=int(d.get("room_count") or 30), step=1)
            exp_occ    = st.slider("Expected Occupancy %", 0, 100,
                                   value=int(d.get("exp_occ_pct") or 65))
            exp_adr    = st.number_input("Expected ADR (IDR/night)", min_value=0.0,
                                         value=float(d.get("exp_adr") or 300_000),
                                         step=10_000.0, format="%.0f")

        with col2:
            ct_idx = CONTRACT_TYPES.index(d["contract_type"]) if d.get("contract_type") in CONTRACT_TYPES else 0
            contract_type = st.selectbox("Contract Type *", CONTRACT_TYPES, index=ct_idx)

            revshare_pct = st.number_input(
                "RevShare % (REHAT cut)", min_value=0.0, max_value=100.0,
                value=float(d.get("revshare_pct") or 15.0), step=0.5,
                disabled=contract_type not in REVSHARE_TYPES,
            )
            lease_monthly = st.number_input(
                "Monthly Lease (IDR)", min_value=0.0, step=1_000_000.0, format="%.0f",
                value=float(d.get("lease_monthly") or 0),
                disabled=contract_type != "lease",
            )
            advance_total = st.number_input(
                "Advance Payment Total (IDR)", min_value=0.0, step=10_000_000.0, format="%.0f",
                value=float(d.get("advance_total") or 0),
                disabled=contract_type != "advance_payment",
            )
            exp_costs = st.number_input(
                "Expected Monthly Opex (IDR)",
                help="Total operating costs per month (excl. rent). Relevant for lease and advance payment.",
                min_value=0.0, step=1_000_000.0, format="%.0f",
                value=float(d.get("exp_monthly_costs") or 0),
                disabled=contract_type in REVSHARE_TYPES,
            )

        notes = st.text_area("Notes", value=d.get("notes",""))
        submitted = st.form_submit_button("💾 Save & Analyze", type="primary", width="stretch")

    if submitted:
        if not name.strip():
            st.error("Property name is required.")
            return None
        return {
            "name": name.strip(), "city": city.strip(),
            "room_count": room_count, "contract_type": contract_type,
            "revshare_pct": revshare_pct if contract_type in REVSHARE_TYPES else None,
            "lease_monthly": lease_monthly if contract_type == "lease" else None,
            "advance_total": advance_total if contract_type == "advance_payment" else None,
            "exp_occ_pct": exp_occ, "exp_adr": exp_adr,
            "exp_monthly_costs": exp_costs if contract_type not in REVSHARE_TYPES else 0,
            "notes": notes.strip(),
        }
    return None


# ── Lease Calculator ──────────────────────────────────────────────────────────

def _lease_calc_rating(lease_pct_of_gop: float) -> tuple[str, str]:
    """Return (label, color) based on lease as % of GOP."""
    if lease_pct_of_gop <= 0.50:
        return "EXCELLENT", "green"
    elif lease_pct_of_gop <= 0.65:
        return "GOOD", "green"
    elif lease_pct_of_gop <= 0.80:
        return "MARGINAL", "orange"
    else:
        return "POOR", "red"


def _render_lease_calculator():
    st.markdown("Quick lease viability check — mirrors the REHAT Lease Valuation Calculator.")

    col_in, col_out = st.columns([1, 1], gap="large")

    with col_in:
        st.markdown("**Inputs**")
        rooms      = st.number_input("Rooms Available", min_value=1, value=27, step=1,
                                     key="lc_rooms")
        target_adr = st.number_input("Target ADR (IDR/night)", min_value=0.0,
                                     value=150_000.0, step=5_000.0, format="%.0f",
                                     key="lc_adr")
        cost_ratio = st.slider("Cost Ratio (opex as % of revenue)", 0, 100, 40,
                               format="%d%%", key="lc_cost_ratio") / 100
        asking     = st.number_input("Asking Monthly Lease (IDR)", min_value=0.0,
                                     value=20_000_000.0, step=1_000_000.0, format="%.0f",
                                     key="lc_asking")

        st.markdown("**Seasonal Occupancy**")
        c1, c2, c3 = st.columns(3)
        with c1:
            st.caption("Low Season")
            low_occ    = st.slider("Occ %", 0, 100, 60, key="lc_low_occ",
                                   format="%d%%", label_visibility="collapsed")
            low_months = st.number_input("Months", 1, 12, 3, key="lc_low_mo")
        with c2:
            st.caption("Normal Season")
            norm_occ    = st.slider("Occ %", 0, 100, 65, key="lc_norm_occ",
                                    format="%d%%", label_visibility="collapsed")
            norm_months = st.number_input("Months", 1, 12, 6, key="lc_norm_mo")
        with c3:
            st.caption("High Season")
            high_occ    = st.slider("Occ %", 0, 100, 70, key="lc_high_occ",
                                    format="%d%%", label_visibility="collapsed")
            high_months = st.number_input("Months", 1, 12, 3, key="lc_high_mo")

        total_months = low_months + norm_months + high_months
        if total_months != 12:
            st.warning(f"Season months sum to {total_months}, not 12.")

    # ── Calculations ──────────────────────────────────────────────────────────
    days = 30.44  # avg days/month

    def _monthly_rev(occ_pct):
        return rooms * (occ_pct / 100) * days * target_adr

    rev_low   = _monthly_rev(low_occ)
    rev_norm  = _monthly_rev(norm_occ)
    rev_high  = _monthly_rev(high_occ)

    # Weighted normal month revenue (for summary card)
    if total_months > 0:
        rev_weighted = (rev_low * low_months + rev_norm * norm_months + rev_high * high_months) / total_months
        annual_rev   = rev_low * low_months + rev_norm * norm_months + rev_high * high_months
    else:
        rev_weighted = rev_norm
        annual_rev   = rev_norm * 12

    opex_low  = rev_low  * cost_ratio
    opex_norm = rev_norm * cost_ratio
    opex_high = rev_high * cost_ratio

    gop_low   = rev_low  - opex_low
    gop_norm  = rev_norm - opex_norm
    gop_high  = rev_high - opex_high

    net_low   = gop_low  - asking
    net_norm  = gop_norm - asking
    net_high  = gop_high - asking

    lease_pct_gop   = asking / gop_norm if gop_norm > 0 else float("inf")
    max_offer       = gop_norm * 0.70
    rating, r_color = _lease_calc_rating(lease_pct_gop)

    with col_out:
        st.markdown("**Results**")

        # Rating badge
        st.markdown(
            f"<div style='font-size:1.4rem;font-weight:700;color:{r_color}'>"
            f"{rating}</div>"
            f"<div style='font-size:0.85rem;color:gray;margin-bottom:12px'>"
            f"Lease is {lease_pct_gop*100:.1f}% of normal GOP</div>",
            unsafe_allow_html=True,
        )

        m1, m2 = st.columns(2)
        m1.metric("Annual Revenue",   _fmt_idr(annual_rev))
        m2.metric("Normal Month Rev", _fmt_idr(rev_norm))
        m3, m4 = st.columns(2)
        m3.metric("Normal Month GOP", _fmt_idr(gop_norm))
        m4.metric("Normal Net Profit",_fmt_idr(net_norm),
                  delta=_fmt_idr(net_norm - 0),
                  delta_color="normal")
        m5, m6 = st.columns(2)
        m5.metric("Max Offer (70% GOP)", _fmt_idr(max_offer),
                  delta=_fmt_idr(asking - max_offer),
                  delta_color="inverse",
                  help="Asking vs max recommended offer")
        m6.metric("Asking Lease", _fmt_idr(asking))

        # Scenario table
        st.markdown("**Scenario Breakdown**")
        df_scen = pd.DataFrame([
            {"Season": "Low",    "Months": low_months,
             "Occ": f"{low_occ}%",   "Revenue": _fmt_idr(rev_low),
             "Opex": _fmt_idr(opex_low), "GOP": _fmt_idr(gop_low),
             "Net Profit": _fmt_idr(net_low)},
            {"Season": "Normal", "Months": norm_months,
             "Occ": f"{norm_occ}%",  "Revenue": _fmt_idr(rev_norm),
             "Opex": _fmt_idr(opex_norm), "GOP": _fmt_idr(gop_norm),
             "Net Profit": _fmt_idr(net_norm)},
            {"Season": "High",   "Months": high_months,
             "Occ": f"{high_occ}%",  "Revenue": _fmt_idr(rev_high),
             "Opex": _fmt_idr(opex_high), "GOP": _fmt_idr(gop_high),
             "Net Profit": _fmt_idr(net_high)},
        ])
        st.dataframe(df_scen, hide_index=True, width="stretch",
                     height=35 * 3 + 38)

    # ── Annual cashflow chart ─────────────────────────────────────────────────
    st.markdown("**Annual Cashflow Profile**")
    season_map = (
        [(low_occ,  rev_low,  gop_low,  net_low)]  * low_months +
        [(norm_occ, rev_norm, gop_norm, net_norm)]  * norm_months +
        [(high_occ, rev_high, gop_high, net_high)]  * high_months
    )
    # reorder: assume low → norm → high ordering not guaranteed, just use input order
    monthly_labels = MONTHS[:total_months] if total_months <= 12 else MONTHS
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=monthly_labels,
        y=[r[1] for r in season_map[:len(monthly_labels)]],
        name="Revenue", opacity=0.5,
    ))
    fig.add_trace(go.Bar(
        x=monthly_labels,
        y=[r[2] for r in season_map[:len(monthly_labels)]],
        name="GOP", opacity=0.6,
    ))
    fig.add_trace(go.Scatter(
        x=monthly_labels,
        y=[r[3] for r in season_map[:len(monthly_labels)]],
        name="Net Profit (after lease)", mode="lines+markers",
        line=dict(width=2), marker=dict(size=6),
    ))
    fig.add_hline(y=0, line_dash="dash", line_color="gray")
    fig.add_hline(y=asking, line_dash="dot", line_color="orange",
                  annotation_text=f"Asking lease {_fmt_idr(asking)}", annotation_position="bottom right")
    fig.update_layout(
        height=300, barmode="group",
        margin=dict(l=0, r=0, t=20, b=0),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        hovermode="x unified",
    )
    st.plotly_chart(fig, width="stretch", key="lc_cashflow_chart")



def _render_analysis(t: dict, benchmarks: dict):
    p = _project(t)
    ct = t["contract_type"]

    st.markdown(f"### {t['name']}  ·  {t.get('city','—')}  ·  `{ct}`")
    st.caption(f"{t['room_count']} rooms  ·  {t['exp_occ_pct']:.0f}% occ  ·  ADR {_fmt_idr(t['exp_adr'])}")

    # ── Summary cards ─────────────────────────────────────────────────────────
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Monthly Revenue",  _fmt_idr(p["rev_total_mo"]))
    c2.metric("Monthly Costs",    _fmt_idr(p["costs_mo"]))
    c3.metric("Monthly GOP",      _fmt_idr(p["gop_mo"]))
    c4.metric("REHAT Revenue/mo", _fmt_idr(p["rehat_mo"]))
    c5.metric("REHAT Margin",     f"{p['margin_mo']:.1f}%")

    c6, c7, c8, c9, c10 = st.columns(5)
    c6.metric("Annual Revenue",   _fmt_idr(p["rev_annual"]))
    c7.metric("Annual Costs",     _fmt_idr(p["costs_annual"]))
    c8.metric("Annual GOP",       _fmt_idr(p["gop_annual"]))
    c9.metric("Annual REHAT Rev", _fmt_idr(p["rehat_annual"]))
    if p["payback_months"] is not None:
        years  = int(p["payback_months"] // 12)
        months = int(p["payback_months"] % 12)
        label  = f"{years}y {months}m" if years else f"{months}m"
        c10.metric("Payback Period", label,
                   help=f"Advance of {_fmt_idr(t.get('advance_total',0))} / {_fmt_idr(p['rehat_mo'])}/mo")

    # ── Break-even ────────────────────────────────────────────────────────────
    if ct not in REVSHARE_TYPES and p["breakeven_rev"] > 0:
        margin_of_safety = ((p["rev_total_mo"] - p["breakeven_rev"])
                            / p["rev_total_mo"] * 100) if p["rev_total_mo"] else 0
        be_occ = (p["breakeven_rev"] / (t["exp_adr"] * t["room_count"] * 30.44) * 100
                  if t["exp_adr"] and t["room_count"] else 0)
        st.info(
            f"**Break-even:** Monthly revenue must exceed **{_fmt_idr(p['breakeven_rev'])}**  "
            f"(implied occupancy ≥ **{be_occ:.1f}%**)  ·  "
            f"Margin of safety: **{margin_of_safety:.1f}%** above break-even"
        )

    col_chart, col_bench = st.columns([3, 2])

    # ── Monthly projection chart ──────────────────────────────────────────────
    with col_chart:
        df_m = pd.DataFrame(p["monthly"])
        fig  = go.Figure()
        fig.add_trace(go.Bar(x=df_m["month"], y=df_m["revenue"],
                             name="Revenue", opacity=0.6))
        fig.add_trace(go.Bar(x=df_m["month"], y=df_m["costs"],
                             name="Costs", opacity=0.6))
        fig.add_trace(go.Scatter(x=df_m["month"], y=df_m["rehat_revenue"],
                                 name="REHAT Revenue",
                                 mode="lines+markers", line=dict(width=2)))
        if ct == "advance_payment":
            fig.add_trace(go.Scatter(x=df_m["month"], y=df_m["cumulative_rehat"],
                                     name="Cumulative REHAT (net of advance)",
                                     mode="lines", line=dict(dash="dot", width=2)))
            fig.add_hline(y=0, line_dash="dash", line_color="gray",
                         annotation_text="Break-even on advance")
        fig.update_layout(
            height=300, barmode="group",
            margin=dict(l=0, r=0, t=20, b=0),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            hovermode="x unified",
        )
        st.plotly_chart(fig, width="stretch", key="acquisition_fig_1")

    # ── Benchmark vs portfolio ────────────────────────────────────────────────
    with col_bench:
        st.markdown("**vs Portfolio Avg (last 90 days)**")
        if not any(benchmarks.values()):
            st.caption("No portfolio data yet.")
        else:
            bench_rows = []
            metrics = [
                ("Occupancy %", t["exp_occ_pct"], benchmarks.get("avg_occ"),
                 lambda v: f"{v:.1f}%"),
                ("ADR",         t["exp_adr"],      benchmarks.get("avg_adr"),
                 _fmt_idr),
                ("RevPAR",
                 t["exp_adr"] * t["exp_occ_pct"] / 100,
                 benchmarks.get("avg_revpar"),
                 _fmt_idr),
                ("REHAT Margin %", p["margin_mo"], benchmarks.get("avg_rehat_margin"),
                 lambda v: f"{v:.1f}%"),
            ]
            for label, target_val, port_val, fmt in metrics:
                d, pos = _delta_str(target_val, port_val)
                bench_rows.append({
                    "Metric":    label,
                    "Target":    fmt(target_val) if target_val else "—",
                    "Portfolio": fmt(port_val) if port_val else "—",
                    "vs Port":   d or "—",
                })
            st.dataframe(pd.DataFrame(bench_rows), width="stretch", hide_index=True, height=35*len(bench_rows)+38)

    if t.get("notes"):
        st.caption(f"📝 {t['notes']}")


# ── Main render ───────────────────────────────────────────────────────────────

def render():
    st.subheader("Acquisition Analytics")
    _init_targets_table()

    conn = get_connection()
    benchmarks = _portfolio_benchmarks(conn)
    conn.close()

    targets = _get_targets()

    tab_lease, tab_new, tab_saved, tab_compare = st.tabs([
        "Lease Calculator", "New Analysis", "Saved Targets", "Compare"
    ])

    # ── Tab 0: Lease Calculator ───────────────────────────────────────────────
    with tab_lease:
        _render_lease_calculator()

    # ── Tab 1: New Analysis ───────────────────────────────────────────────────
    with tab_new:
        data = _target_form("new")
        if data:
            tid = _save_target(data)
            st.success(f"✓ Saved as target #{tid}")
            st.rerun()

    # ── Tab 2: Saved Targets ──────────────────────────────────────────────────
    with tab_saved:
        targets = _get_targets()
        if not targets:
            st.info("No saved targets yet. Use 'New Analysis' to add one.")
        else:
            for t in targets:
                label = f"#{t['id']}  {t['name']}  ·  {t.get('city','—')}  ·  {t['contract_type']}"
                with st.expander(label, expanded=len(targets) == 1):
                    inner_tab_view, inner_tab_edit = st.tabs(["Analysis", "Edit"])

                    with inner_tab_view:
                        _render_analysis(t, benchmarks)

                    with inner_tab_edit:
                        updated = _target_form(f"edit_{t['id']}", defaults=t)
                        if updated:
                            _update_target(t["id"], updated)
                            st.success("✓ Updated.")
                            st.rerun()
                        if st.button(f"🗑 Delete #{t['id']}", key=f"del_{t['id']}"):
                            _delete_target(t["id"])
                            st.rerun()

    # ── Tab 3: Compare ────────────────────────────────────────────────────────
    with tab_compare:
        targets = _get_targets()
        if len(targets) < 2:
            st.info("Add at least 2 saved targets to compare.")
        else:
            selected = st.multiselect(
                "Select targets to compare",
                options=[t["id"] for t in targets],
                format_func=lambda tid: next(
                    f"#{t['id']} {t['name']}" for t in targets if t["id"] == tid
                ),
                default=[t["id"] for t in targets[:min(4, len(targets))]],
            )

            if len(selected) < 2:
                st.caption("Select at least 2 targets.")
            else:
                selected_targets = [t for t in targets if t["id"] in selected]
                projs = [_project(t) for t in selected_targets]
                names = [t["name"] for t in selected_targets]

                # Comparison table
                rows = []
                for t, p in zip(selected_targets, projs):
                    rows.append({
                        "Name":           t["name"],
                        "City":           t.get("city","—"),
                        "Contract":       t["contract_type"],
                        "Rooms":          t["room_count"],
                        "Occ %":          f"{t['exp_occ_pct']:.0f}%",
                        "ADR":            _fmt_idr(t["exp_adr"]),
                        "Mo. Revenue":    _fmt_idr(p["rev_total_mo"]),
                        "Mo. REHAT Rev":  _fmt_idr(p["rehat_mo"]),
                        "REHAT Margin":   f"{p['margin_mo']:.1f}%",
                        "Annual REHAT":   _fmt_idr(p["rehat_annual"]),
                        "Payback":        (
                            f"{int(p['payback_months']//12)}y {int(p['payback_months']%12)}m"
                            if p["payback_months"] else "—"
                        ),
                    })
                st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True, height=35*len(rows)+38)

                # Bar chart: REHAT revenue comparison
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=names,
                    y=[p["rehat_mo"] for p in projs],
                    name="Monthly REHAT Revenue",
                    text=[_fmt_idr(p["rehat_mo"]) for p in projs],
                    textposition="outside",
                ))
                fig.add_trace(go.Bar(
                    x=names,
                    y=[p["rehat_annual"] for p in projs],
                    name="Annual REHAT Revenue",
                    text=[_fmt_idr(p["rehat_annual"]) for p in projs],
                    textposition="outside",
                    visible="legendonly",
                ))
                fig.update_layout(
                    height=340, barmode="group",
                    margin=dict(l=0, r=0, t=20, b=0),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                st.plotly_chart(fig, width="stretch", key="acquisition_fig_2")

                # Radar chart: normalized metrics
                categories = ["Occ %", "ADR", "REHAT Margin", "Annual REHAT"]
                # Normalize each metric to 0–100 scale across selected targets
                def _norm(values):
                    mx = max(values) if max(values) > 0 else 1
                    return [v / mx * 100 for v in values]

                occ_n    = _norm([t["exp_occ_pct"] for t in selected_targets])
                adr_n    = _norm([t["exp_adr"] for t in selected_targets])
                margin_n = _norm([max(p["margin_mo"], 0) for p in projs])
                rehat_n  = _norm([max(p["rehat_annual"], 0) for p in projs])

                fig_r = go.Figure()
                for i, (t, name) in enumerate(zip(selected_targets, names)):
                    vals = [occ_n[i], adr_n[i], margin_n[i], rehat_n[i]]
                    fig_r.add_trace(go.Scatterpolar(
                        r=vals + [vals[0]],
                        theta=categories + [categories[0]],
                        name=name, fill="toself", opacity=0.5,
                    ))
                fig_r.update_layout(
                    polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
                    margin=dict(l=20, r=20, t=40, b=20),
                    legend=dict(orientation="h", yanchor="bottom", y=-0.15),
                )
                st.caption("Radar: normalized scores (100 = best among selected targets)")
                st.plotly_chart(fig_r, width="stretch", key="acquisition_fig_r_3")
