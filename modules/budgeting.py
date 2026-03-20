"""
modules/budgeting.py — Budgeting

Input: revenue targets per property per month.
View: budget vs actual, attainment %, trend chart, remaining budget.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import date, datetime
from calendar import monthrange

from db import get_connection, get_db, get_active_properties
from modules.property_kpis import _fmt_idr, _month_range

MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
          "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


# ── DB helpers ────────────────────────────────────────────────────────────────

def _get_budget(conn, prop_id, year, month):
    row = conn.execute(
        "SELECT revenue_target FROM budgets WHERE property_id=? AND year=? AND month=?",
        (prop_id, year, month)
    ).fetchone()
    return row["revenue_target"] if row else None


def _set_budget(prop_id, year, month, revenue_target):
    with get_db() as conn:
        conn.execute("""
            INSERT INTO budgets (property_id, year, month, revenue_target, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(property_id, year, month) DO UPDATE SET
                revenue_target=excluded.revenue_target,
                updated_at=excluded.updated_at
        """, (prop_id, year, month, revenue_target, datetime.utcnow().isoformat()))


def _get_actual(conn, prop_id, year, month):
    first, last = _month_range(year, month)
    row = conn.execute("""
        SELECT SUM(revenue_total) as revenue
        FROM daily_snapshot
        WHERE property_id=? AND date BETWEEN ? AND ?
    """, (prop_id, str(first), str(last))).fetchone()
    return row["revenue"] if row and row["revenue"] else 0.0


def _get_year_budgets(conn, prop_id, year):
    rows = conn.execute(
        "SELECT month, revenue_target FROM budgets WHERE property_id=? AND year=? ORDER BY month",
        (prop_id, year)
    ).fetchall()
    return {r["month"]: r["revenue_target"] for r in rows}


def _get_monthly_actuals(conn, prop_id, year):
    """Return dict {month: revenue} for all months in a year."""
    rows = conn.execute("""
        SELECT CAST(strftime('%m', date) AS INTEGER) as month,
               SUM(revenue_total) as revenue
        FROM daily_snapshot
        WHERE property_id=? AND date BETWEEN ? AND ?
        GROUP BY month
    """, (prop_id, f"{year}-01-01", f"{year}-12-31")).fetchall()
    return {r["month"]: r["revenue"] for r in rows}


def _seed_from_last_year(prop_id, year):
    """Copy last year's actuals as budget for the given year."""
    conn = get_connection()
    actuals = _get_monthly_actuals(conn, prop_id, year - 1)
    conn.close()
    count = 0
    for month, revenue in actuals.items():
        if revenue and revenue > 0:
            _set_budget(prop_id, year, month, revenue)
            count += 1
    return count


# ── Render ────────────────────────────────────────────────────────────────────

def render():
    st.subheader("Budgeting")

    props = sorted(get_active_properties(), key=lambda p: str(p["id"]))
    if not props:
        st.warning("No active properties. Add properties in 🔧 Settings.")
        return

    today = date.today()
    prop_options = {p["id"]: p["name"] for p in props}

    tab_input, tab_overview = st.tabs(["Input Budgets", "Budget vs Actual"])

    # ── Tab 1: Input ──────────────────────────────────────────────────────────
    with tab_input:
        col_p, col_y = st.columns([2, 1])
        with col_p:
            selected_id = st.selectbox(
                "Property",
                options=list(prop_options.keys()),
                format_func=lambda k: prop_options[k],
                key="budget_input_prop",
            )
        with col_y:
            input_year = st.number_input(
                "Year", min_value=2020, max_value=today.year + 2,
                value=today.year, step=1, key="budget_input_year",
            )

        conn = get_connection()
        existing = _get_year_budgets(conn, selected_id, input_year)
        conn.close()

        st.markdown(f"**Set monthly revenue targets for {prop_options[selected_id]} — {input_year}**")
        st.caption("Enter 0 or leave blank to clear a month's budget.")

        with st.form(f"budget_form_{selected_id}_{input_year}"):
            # 2 rows of 6 months
            cols_top = st.columns(6)
            cols_bot = st.columns(6)
            inputs = {}

            for i, (label, cols) in enumerate(
                [(MONTHS[:6], cols_top), (MONTHS[6:], cols_bot)]
            ):
                for j, month_label in enumerate(label):
                    month_num = i * 6 + j + 1
                    current_val = existing.get(month_num, 0.0) or 0.0
                    with cols[j]:
                        inputs[month_num] = st.number_input(
                            month_label,
                            min_value=0.0,
                            value=float(current_val),
                            step=1_000_000.0,
                            format="%.0f",
                            key=f"bud_{selected_id}_{input_year}_{month_num}",
                        )

            col_save, col_seed = st.columns([2, 1])
            with col_save:
                submitted = st.form_submit_button("💾 Save Budgets", type="primary", use_container_width=True)
            with col_seed:
                seed = st.form_submit_button(
                    f"📋 Seed from {input_year - 1} actuals",
                    use_container_width=True,
                )

        if submitted:
            saved = 0
            for month, val in inputs.items():
                if val > 0:
                    _set_budget(selected_id, input_year, month, val)
                    saved += 1
                else:
                    # clear if explicitly set to 0
                    with get_db() as conn:
                        conn.execute(
                            "DELETE FROM budgets WHERE property_id=? AND year=? AND month=?",
                            (selected_id, input_year, month)
                        )
            st.success(f"✓ Saved {saved} monthly budgets for {prop_options[selected_id]} {input_year}.")
            st.rerun()

        if seed:
            count = _seed_from_last_year(selected_id, input_year)
            if count == 0:
                st.warning(f"No actuals found for {input_year - 1}. Ingest historical data first.")
            else:
                st.success(f"✓ Seeded {count} months from {input_year - 1} actuals. Review and save.")
            st.rerun()

    # ── Tab 2: Budget vs Actual ───────────────────────────────────────────────
    with tab_overview:
        col_p2, col_y2 = st.columns([2, 1])
        with col_p2:
            view_id = st.selectbox(
                "Property",
                options=list(prop_options.keys()),
                format_func=lambda k: prop_options[k],
                key="budget_view_prop",
            )
        with col_y2:
            view_year = st.number_input(
                "Year", min_value=2020, max_value=today.year + 1,
                value=today.year, step=1, key="budget_view_year",
            )

        conn = get_connection()
        budgets = _get_year_budgets(conn, view_id, view_year)
        actuals = _get_monthly_actuals(conn, view_id, view_year)
        conn.close()

        if not budgets:
            st.info(f"No budgets set for {prop_options[view_id]} {view_year}. Use the Input tab to add targets.")
            return

        # Build comparison DataFrame
        rows = []
        for m in range(1, 13):
            bud = budgets.get(m)
            act = actuals.get(m, 0.0)
            is_future = date(view_year, m, 1) > today
            attainment = (act / bud * 100) if bud and bud > 0 and not is_future else None
            rows.append({
                "month_num": m,
                "month":     MONTHS[m - 1],
                "budget":    bud,
                "actual":    act if not is_future else None,
                "attainment": attainment,
                "is_future": is_future,
            })
        df = pd.DataFrame(rows)

        # ── Current month remaining ───────────────────────────────────────────
        if view_year == today.year:
            cm_bud = budgets.get(today.month)
            cm_act = actuals.get(today.month, 0.0)
            if cm_bud and cm_bud > 0:
                remaining = cm_bud - cm_act
                pct_done  = cm_act / cm_bud
                days_total = monthrange(today.year, today.month)[1]
                days_elapsed = today.day
                pct_month_elapsed = days_elapsed / days_total

                col_a, col_b, col_c = st.columns(3)
                with col_a:
                    st.metric("This Month — Actual", _fmt_idr(cm_act))
                with col_b:
                    st.metric("Budget", _fmt_idr(cm_bud))
                with col_c:
                    st.metric(
                        "Remaining",
                        _fmt_idr(remaining),
                        delta=f"{pct_done*100:.1f}% attained",
                        delta_color="normal",
                    )
                st.progress(
                    min(pct_done, 1.0),
                    text=f"{pct_done*100:.1f}% of budget · {days_elapsed}/{days_total} days elapsed ({pct_month_elapsed*100:.0f}%)"
                )
                st.divider()

        # ── Trend chart ───────────────────────────────────────────────────────
        df_chart = df[df["budget"].notna()]
        fig = go.Figure()

        fig.add_trace(go.Bar(
            x=df_chart["month"],
            y=df_chart["actual"],
            name="Actual",
            marker_opacity=0.8,
        ))
        fig.add_trace(go.Scatter(
            x=df_chart["month"],
            y=df_chart["budget"],
            name="Budget",
            mode="lines+markers",
            line=dict(dash="dash", width=2),
            marker=dict(size=6),
        ))
        fig.update_layout(
            height=320,
            margin=dict(l=0, r=0, t=20, b=0),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            yaxis_title="Revenue (IDR)",
            hovermode="x unified",
            barmode="overlay",
        )
        st.plotly_chart(fig, use_container_width=True)

        # ── Attainment % per property (current year, all props) ───────────────
        st.markdown("**Attainment % — All Properties**")
        conn = get_connection()
        att_rows = []
        for p in props:
            p_budgets = _get_year_budgets(conn, p["id"], view_year)
            p_actuals = _get_monthly_actuals(conn, p["id"], view_year)
            total_bud = sum(v for m, v in p_budgets.items()
                           if v and date(view_year, m, 1) <= today)
            total_act = sum(p_actuals.get(m, 0) for m in p_budgets
                           if date(view_year, m, int(1)) <= today)
            att = (total_act / total_bud * 100) if total_bud > 0 else None
            att_rows.append({
                "Property":    p["name"],
                "Budget YTD":  _fmt_idr(total_bud) if total_bud else "—",
                "Actual YTD":  _fmt_idr(total_act),
                "Attainment":  f"{att:.1f}%" if att is not None else "—",
            })
        conn.close()

        st.dataframe(
            pd.DataFrame(att_rows),
            use_container_width=True,
            hide_index=True,
        )

        # ── Monthly detail table ──────────────────────────────────────────────
        st.markdown(f"**Monthly Detail — {prop_options[view_id]} {view_year}**")
        display = df[df["budget"].notna()].copy()
        display["Budget"]     = display["budget"].map(_fmt_idr)
        display["Actual"]     = display["actual"].map(lambda v: _fmt_idr(v) if v is not None else "—")
        display["Attainment"] = display["attainment"].map(lambda v: f"{v:.1f}%" if v is not None else "—")
        display["Variance"]   = display.apply(
            lambda r: _fmt_idr((r["actual"] or 0) - r["budget"])
            if r["budget"] and r["actual"] is not None else "—", axis=1
        )
        st.dataframe(
            display[["month", "Budget", "Actual", "Variance", "Attainment"]].rename(
                columns={"month": "Month"}
            ),
            use_container_width=True,
            hide_index=True,
        )
