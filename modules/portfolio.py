"""
modules/portfolio.py — Portfolio Analytics

Cross-property comparison with property selector, date range, and compare-to period.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import date, timedelta
from calendar import monthrange

from db import get_connection, get_active_properties
from modules.property_kpis import (
    _month_range, _get_snapshot, _fmt_pct, _delta_str,
    _get_unified_daily_series, _get_channel_mix, _get_dow_pattern,
    _fmt_num as _fmt_num_pkpi, _fmt_idr,
)


def _fmt_num(v):
    """Thousand-separated integer, no currency symbol."""
    if v is None:
        return "—"
    try:
        f = float(v)
    except (TypeError, ValueError):
        return "—"
    if f != f:  # NaN check
        return "—"
    return f"{int(round(f)):,}"


def _get_budget_for_period(conn, prop_id, date_from, date_to):
    """Sum budgets for all months that overlap the date range."""
    rows = conn.execute("""
        SELECT year, month, revenue_target FROM budgets
        WHERE property_id=?
          AND (year * 100 + month) BETWEEN ? AND ?
    """, (
        prop_id,
        date_from.year * 100 + date_from.month,
        date_to.year * 100 + date_to.month,
    )).fetchall()
    total = sum(r["revenue_target"] for r in rows if r["revenue_target"])
    return total or None


def _get_all_snapshots(conn, props, date_from, date_to):
    from datetime import date as date_type
    d_from_base = date_type.fromisoformat(str(date_from))
    d_to        = date_type.fromisoformat(str(date_to))
    today       = date_type.today()
    yesterday   = today - timedelta(days=1)
    rows = []
    for p in props:
        # Clamp start to join_date
        join = date_type.fromisoformat(p["join_date"]) if p.get("join_date") else d_from_base
        d_from = max(d_from_base, join)
        if d_from >= d_to:
            continue
        total_nights = (d_to - d_from).days

        # _get_snapshot handles past actuals + BOB for future dates internally
        s = _get_snapshot(conn, p["id"], d_from, d_to)

        total_rooms_sold = s.get("rooms_sold") or 0
        total_revenue    = s.get("revenue_total") or 0
        room_count       = p["room_count"] or 1
        # Use capacity and ADR/RevPAR already computed correctly in _get_snapshot
        total_capacity   = room_count * total_nights   # for occ denominator display
        merged_occ       = s.get("occupancy_pct") or 0
        merged_adr       = s.get("adr") or None
        merged_revpar    = s.get("revpar") or None

        if total_rooms_sold == 0 and s.get("days_with_data", 0) == 0:
            continue

        bud = _get_budget_for_period(conn, p["id"], date_from, date_to)
        rows.append({
            "id":            p["id"],
            "name":          p["name"],
            "city":          p["city"] or "—",
            "contract_type": p["contract_type"],
            "room_count":    room_count,
            "total_nights":  total_nights,
            "occupancy_pct": round(merged_occ, 1),
            "adr":           merged_adr,
            "revpar":        merged_revpar,
            "revenue_total": total_revenue,
            "revenue_rooms": s.get("revenue_rooms") or 0,
            "rehat_revenue": s.get("rehat_revenue") or 0,
            "rooms_sold":    total_rooms_sold,
            "bookings_count":s.get("bookings_count") or 0,
            "budget":        bud,
        })
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _get_portfolio_daily_series(conn, prop_ids, date_from, date_to):
    """
    Portfolio-level daily series: sum rooms_sold + revenue across all prop_ids per date.
    Recomputes occ/adr/revpar from aggregated totals (not average of per-property values).
    Past dates from daily_snapshot; today+ from bookings_on_books.
    """
    from datetime import date as date_type
    today     = date_type.today()
    yesterday = today - timedelta(days=1)

    placeholders = ",".join("?" * len(prop_ids))

    snap_rows = conn.execute(f"""
        SELECT date,
               SUM(rooms_sold)      AS rooms_sold,
               SUM(rooms_available) AS rooms_available,
               SUM(revenue_total)   AS revenue_total,
               SUM(revenue_rooms)   AS revenue_rooms
        FROM daily_snapshot
        WHERE property_id IN ({placeholders}) AND date BETWEEN ? AND ?
        GROUP BY date ORDER BY date
    """, (*prop_ids, str(date_from), str(min(date_to, yesterday)))).fetchall()

    bob_rows = conn.execute(f"""
        SELECT stay_date AS date,
               COUNT(*)                           AS rooms_sold,
               SUM(COALESCE(nightly_rate_idr, 0)) AS revenue
        FROM bookings_on_books
        WHERE property_id IN ({placeholders}) AND stay_date BETWEEN ? AND ?
        GROUP BY stay_date ORDER BY stay_date
    """, (*prop_ids, str(max(date_from, today)), str(date_to))).fetchall()

    # Total room capacity across selected properties for BOB occ calculation
    cap_row = conn.execute(f"""
        SELECT SUM(room_count) AS total FROM properties WHERE id IN ({placeholders})
    """, (*prop_ids,)).fetchone()
    total_room_count = (cap_row["total"] or 1) if cap_row else 1

    records = []
    for r in snap_rows:
        rooms    = r["rooms_sold"] or 0
        rev_r    = r["revenue_rooms"] or 0
        capacity = r["rooms_available"] or total_room_count
        records.append({
            "date":          r["date"],
            "rooms_sold":    rooms,
            "occupancy_pct": round(rooms / capacity * 100, 1) if capacity else 0,
            "revenue_total": r["revenue_total"] or 0,
            "revenue_rooms": rev_r,
            "adr":           round(rev_r / rooms, 1) if rooms else 0,
            "revpar":        round(rev_r / capacity, 1) if capacity else 0,
            "source":        "actual",
        })

    for r in bob_rows:
        rooms   = r["rooms_sold"] or 0
        revenue = r["revenue"] or 0
        occ     = round(rooms / total_room_count * 100, 1) if total_room_count else 0
        records.append({
            "date":          r["date"],
            "rooms_sold":    rooms,
            "occupancy_pct": occ,
            "revenue_total": revenue,
            "revenue_rooms": revenue,
            "adr":           round(revenue / rooms, 1) if rooms else 0,
            "revpar":        round(revenue / total_room_count, 1) if total_room_count else 0,
            "source":        "bob",
        })

    all_dates = [str(date_from + timedelta(days=i)) for i in range((date_to - date_from).days + 1)]
    df = pd.DataFrame(records).sort_values("date").reset_index(drop=True) if records else pd.DataFrame()
    existing = set(df["date"].tolist()) if not df.empty else set()
    missing  = [d for d in all_dates if d not in existing]
    if missing:
        fill = [{"date": d, "rooms_sold": 0, "occupancy_pct": 0.0,
                 "revenue_total": 0.0, "revenue_rooms": 0.0,
                 "adr": 0.0, "revpar": 0.0,
                 "source": "bob" if d >= str(today) else "actual"} for d in missing]
        df = pd.concat([df, pd.DataFrame(fill)], ignore_index=True).sort_values("date").reset_index(drop=True)
    return df


def _get_portfolio_channel_mix(conn, prop_ids, date_from, date_to):
    """Aggregate channel mix across multiple properties."""
    frames = []
    for pid in prop_ids:
        ch = _get_channel_mix(conn, pid, date_from, date_to)
        if not ch.empty:
            frames.append(ch[["display_name", "channel_type", "bookings", "revenue"]].rename(columns={"bookings": "rooms_sold"}))
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    agg = combined.groupby(["display_name", "channel_type"]).agg(
        rooms_sold=("rooms_sold", "sum"),
        revenue=("revenue", "sum"),
    ).reset_index()
    total_rev = agg["revenue"].sum() or 1
    agg["rev_share"] = agg["revenue"] / total_rev * 100
    agg["adr"] = agg.apply(
        lambda r: r["revenue"] / r["rooms_sold"] if r["rooms_sold"] else 0, axis=1
    )
    return agg.sort_values("revenue", ascending=False).reset_index(drop=True)


def _get_portfolio_dow_pattern(conn, prop_ids, date_from, date_to):
    """
    Portfolio DOW: sum rooms_sold + revenue per DOW across all properties,
    then compute averages from aggregated totals (not average of averages).
    """
    placeholders = ",".join("?" * len(prop_ids))
    rows = conn.execute(f"""
        SELECT
            CAST(strftime('%w', date) AS INTEGER) AS dow,
            SUM(rooms_sold)      AS total_rooms,
            SUM(revenue_total)   AS total_revenue,
            SUM(revenue_rooms)   AS total_rev_rooms,
            SUM(rooms_available) AS total_capacity,
            COUNT(*)             AS n_days
        FROM daily_snapshot
        WHERE property_id IN ({placeholders}) AND date BETWEEN ? AND ?
        GROUP BY dow ORDER BY dow
    """, (*prop_ids, str(date_from), str(date_to))).fetchall()
    if not rows:
        return pd.DataFrame()
    dow_map = {0: "Sun", 1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat"}
    records = []
    for r in rows:
        rooms    = r["total_rooms"] or 0
        rev_r    = r["total_rev_rooms"] or 0
        capacity = r["total_capacity"] or 1
        n        = r["n_days"] or 1
        records.append({
            "dow":         r["dow"],
            "day":         dow_map[r["dow"]],
            "avg_occ":     rooms / capacity * 100,
            "avg_rooms":   rooms / n,
            "avg_adr":     rev_r / rooms if rooms else 0,
            "avg_revpar":  rev_r / capacity,
            "avg_revenue": (r["total_revenue"] or 0) / n,
            "n_days":      n,
        })
    return pd.DataFrame(records)


def _portfolio_totals(df):
    if df.empty:
        return {}
    total_rooms_sold  = df["rooms_sold"].sum()
    total_capacity    = (df["room_count"] * df["total_nights"]).sum()
    revenue_total     = df["revenue_total"].sum()
    revenue_rooms     = df["revenue_rooms"].sum() if "revenue_rooms" in df.columns else revenue_total
    return {
        "revenue_total":  revenue_total,
        "revenue_rooms":  revenue_rooms,
        "rehat_revenue":  df["rehat_revenue"].sum(),
        "rooms_sold":     total_rooms_sold,
        "bookings_count": df["bookings_count"].sum(),
        "adr":            (revenue_rooms / total_rooms_sold if total_rooms_sold else None),
        "revpar":         (revenue_rooms / total_capacity   if total_capacity   else None),
        "occupancy_pct":  (total_rooms_sold / total_capacity * 100 if total_capacity else 0),
    }


def render():
    st.subheader("Portfolio Analytics")

    all_props = get_active_properties()
    if not all_props:
        st.warning("No active properties. Add properties in 🔧 Property Config.")
        return

    today = date.today()
    default_from, default_to = _month_range(today.year, today.month)

    # ── Controls ──────────────────────────────────────────────────────────────
    col_props, col_from, col_to, col_cmp = st.columns([3, 1, 1, 1])

    with col_props:
        prop_ids = st.multiselect(
            "Properties",
            options=[p["id"] for p in all_props],
            default=[],
            placeholder="Leave empty for all properties",
            format_func=lambda k: next(p["name"] for p in all_props if p["id"] == k),
        )
    # Empty = all properties
    props = [p for p in all_props if p["id"] in prop_ids] if prop_ids else all_props

    with col_from:
        date_from = st.date_input("From", value=default_from, key="port_from")
    with col_to:
        date_to = st.date_input("To", value=default_to, key="port_to")
    with col_cmp:
        cmp_label = st.selectbox("Compare to", ["None", "vs Last Month", "vs Last Year", "Custom"])

    # Custom comparison date inputs appear below the main row
    cmp_from = cmp_to = None
    days = (date_to - date_from).days + 1

    if cmp_label == "vs Last Year":
        try:
            cmp_from = date_from.replace(year=date_from.year - 1)
            cmp_to   = date_to.replace(year=date_to.year - 1)
        except ValueError:
            pass
    elif cmp_label == "vs Last Month":
        cmp_to   = date_from - __import__('datetime').timedelta(days=1)
        cmp_from = cmp_to - __import__('datetime').timedelta(days=days - 1)
    elif cmp_label == "Custom":
        col_cf, col_ct = st.columns(2)
        with col_cf:
            cmp_from = st.date_input("Compare From", value=date_from.replace(year=date_from.year - 1), key="cmp_from")
        with col_ct:
            cmp_to = st.date_input("Compare To", value=date_to.replace(year=date_to.year - 1), key="cmp_to")

    cmp_delta_label = cmp_label if cmp_label != "None" else ""

    if date_from > date_to:
        st.error("From date must be before To date.")
        return

    conn = get_connection()
    df     = _get_all_snapshots(conn, props, date_from, date_to)
    df_cmp = (_get_all_snapshots(conn, props, cmp_from, cmp_to)
              if cmp_from else pd.DataFrame())
    conn.close()

    if df.empty:
        st.info("No data for this period. Run ingestion from ⚙️ System Status.")
        return

    df = df.sort_values("revenue_total", ascending=False).reset_index(drop=True)

    totals     = _portfolio_totals(df)
    totals_cmp = _portfolio_totals(df_cmp) if not df_cmp.empty else {}

    st.caption(f"{len(df)} properties · {date_from} → {date_to}"
               + (f" · compared to {cmp_from} → {cmp_to}" if cmp_from else ""))

    # ── KPI Cards: Room Nights, Occupancy, ADR, RevPAR, Revenue ──────────────
    c1, c2, c3, c4, c5 = st.columns(5)

    def _card(col, label, val_str, curr_val, cmp_val):
        d, p = _delta_str(curr_val, cmp_val) if cmp_val else (None, None)
        with col:
            st.markdown(f"**{label}**")
            st.markdown(f"### {val_str}")
            if d:
                color = "green" if p else "red"
                st.markdown(f":{color}[{d} {cmp_delta_label}]")
            st.divider()

    _card(c1, "Room Nights",
          _fmt_num(totals.get("rooms_sold")),
          totals.get("rooms_sold"), totals_cmp.get("rooms_sold"))

    _card(c2, "Occupancy",
          _fmt_pct(totals.get("occupancy_pct")),
          totals.get("occupancy_pct"), totals_cmp.get("occupancy_pct"))

    _card(c3, "ADR",
          _fmt_num(totals.get("adr")),
          totals.get("adr"), totals_cmp.get("adr"))

    _card(c4, "RevPAR",
          _fmt_num(totals.get("revpar")),
          totals.get("revpar"), totals_cmp.get("revpar"))

    _card(c5, "Revenue",
          _fmt_num(totals.get("revenue_total")),
          totals.get("revenue_total"), totals_cmp.get("revenue_total"))

    st.divider()

    # ── Tabs ──────────────────────────────────────────────────────────────────
    tab_table, tab_bar, tab_trend, tab_channel, tab_dow = st.tabs([
        "Property Table", "Bar Comparison", "Daily Trend", "Channel Mix", "Day of Week"
    ])

    # ── Tab 1: Property Table ─────────────────────────────────────────────────
    with tab_table:
        total_revenue = df["revenue_total"].sum() or 1

        display_rows = []
        for _, row in df.iterrows():
            rev = row["revenue_total"]
            bud = row["budget"]
            vs_bud = (f"{rev / bud * 100:.1f}%" if bud and bud > 0 else "—")
            contrib = f"{rev / total_revenue * 100:.1f}%"
            display_rows.append({
                "Property":       row["name"],
                "City":           row["city"],
                "Room Nights":    _fmt_num(row["rooms_sold"]),
                "Occ %":          _fmt_pct(row["occupancy_pct"]),
                "ADR":            _fmt_num(row["adr"]),
                "RevPAR":         _fmt_num(row["revpar"]),
                "Revenue":        _fmt_num(rev),
                "vs Budget %":    vs_bud,
                "Contribution %": contrib,
            })

        st.dataframe(pd.DataFrame(display_rows), width="stretch", hide_index=True,
                     height=35*len(display_rows)+38)

    # ── Tab 2: Bar Comparison ─────────────────────────────────────────────────
    with tab_bar:
        metric_opt = st.radio(
            "Metric",
            ["revenue_total", "rooms_sold", "occupancy_pct", "adr", "revpar"],
            format_func=lambda x: {
                "revenue_total": "Revenue",
                "rooms_sold":    "Room Nights",
                "occupancy_pct": "Occupancy %",
                "adr":           "ADR",
                "revpar":        "RevPAR",
            }[x],
            horizontal=True,
            key="portfolio_bar_metric",
        )
        df_bar = df.sort_values(metric_opt, ascending=True)
        vals = df_bar[metric_opt].tolist()
        max_val = max(vals) if vals else 1
        bar_colors = [
            "#6C63FF" if v == max_val else
            "#8B85FF" if v >= max_val * 0.85 else
            "#A5A0FF" if v >= max_val * 0.70 else
            "#C8C5FF"
            for v in vals
        ]
        fig = go.Figure(go.Bar(
            x=df_bar[metric_opt],
            y=df_bar["name"],
            orientation="h",
            marker_color=bar_colors,
            marker_line_width=0,
            text=df_bar[metric_opt].map(
                lambda v: _fmt_pct(v) if metric_opt == "occupancy_pct" else _fmt_num(v)
            ),
            textposition="outside",
        ))
        bar_max = max_val * 1.18 if max_val else 1
        fig.update_layout(
            height=max(300, len(df) * 45),
            margin=dict(l=0, r=80, t=20, b=0),
            xaxis=dict(title=None, range=[0, bar_max]),
            yaxis_title=None, showlegend=False,
        )
        st.plotly_chart(fig, width="stretch", key="portfolio_fig_1")

    # ── Tab 3: Daily Trend (portfolio) ───────────────────────────────────────
    with tab_trend:
        conn2 = get_connection()
        daily = _get_portfolio_daily_series(conn2, [p["id"] for p in props], date_from, date_to)
        conn2.close()

        if daily.empty:
            st.info("No data for this period.")
        else:
            metric = st.radio(
                "Metric", ["Occ %", "Room Sold", "ADR", "RevPAR", "Revenue"],
                horizontal=True, key="port_trend_metric",
            )
            col_map = {
                "Occ %":     "occupancy_pct",
                "Room Sold": "rooms_sold",
                "ADR":       "adr",
                "RevPAR":    "revpar",
                "Revenue":   "revenue_total",
            }
            y_col = col_map[metric]
            actual_trend = daily[daily["source"] == "actual"]
            bob_trend    = daily[daily["source"] == "bob"]

            def _fmt_label(v):
                if not v:
                    return ""
                if metric == "Occ %":
                    return f"{v:.1f}"
                elif metric in ("ADR", "RevPAR", "Revenue"):
                    return _fmt_num_pkpi(v, short=True)
                return f"{int(v):,}"

            fig_t = go.Figure()
            if not actual_trend.empty:
                fig_t.add_trace(go.Bar(
                    x=actual_trend["date"], y=actual_trend[y_col],
                    name="Actual", marker_color="#6C63FF", marker_line_width=0,
                    text=actual_trend[y_col].map(_fmt_label),
                    textposition="outside", textfont=dict(size=10),
                ))
            if not bob_trend.empty:
                fig_t.add_trace(go.Bar(
                    x=bob_trend["date"], y=bob_trend[y_col],
                    name="BOB", marker_color="#A5A0FF", marker_line_width=0,
                    text=bob_trend[y_col].map(_fmt_label),
                    textposition="outside", textfont=dict(size=10),
                ))
            all_vals = daily[y_col].dropna().tolist()
            ymax = max(all_vals) * 1.18 if all_vals else 1
            fig_t.update_layout(
                height=360,
                margin=dict(l=0, r=0, t=30, b=0),
                xaxis=dict(type="date", range=[str(date_from - timedelta(days=1)), str(date_to + timedelta(days=1))]),
                xaxis_title=None,
                yaxis=dict(title=metric, range=[0, ymax]),
                barmode="group", bargap=0.2, bargroupgap=0.05,
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            st.plotly_chart(fig_t, width="stretch", key="portfolio_fig_trend")

    # ── Tab 4: Channel Mix (portfolio) ────────────────────────────────────────
    with tab_channel:
        conn3 = get_connection()
        ch = _get_portfolio_channel_mix(conn3, [p["id"] for p in props], date_from, date_to)
        conn3.close()

        if ch.empty:
            st.info("No booking source data available for this period.")
        else:
            PALETTE = [
                "#FF9800","#F44336","#9C27B0","#009688",
                "#CDDC39","#FF5722","#607D8B","#E91E63","#00BCD4","#795548",
            ]
            TYPE_BASE_COLORS = {"booking_engine": "#4CAF50", "front_desk": "#2196F3"}
            indirect_rows = ch[ch["channel_type"] == "indirect"].reset_index(drop=True)
            colors = {}
            for i, row in indirect_rows.iterrows():
                colors[row["display_name"]] = PALETTE[i % len(PALETTE)]
            for _, row in ch[ch["channel_type"] != "indirect"].iterrows():
                colors[row["display_name"]] = TYPE_BASE_COLORS.get(row["channel_type"], "#9E9E9E")
            ch["color"] = ch["display_name"].map(colors)

            table_rows_ch = []
            for _, row in ch.iterrows():
                rooms = row.get("rooms_sold") or 0
                adr_val = row.get("adr") or 0
                table_rows_ch.append({
                    "Channel":     row["display_name"],
                    "Rooms Sold":  int(rooms),
                    "Revenue":     _fmt_num(row["revenue"]),
                    "Rev Share %": f"{row['rev_share']:.1f}%",
                    "ADR":         f"{int(adr_val):,}" if rooms else "—",
                })
            display_df_ch = pd.DataFrame(table_rows_ch)
            shared_height_ch = 35 * len(display_df_ch) + 38

            TYPE_LABELS = {"booking_engine": "Booking Engine", "front_desk": "Front Desk", "indirect": "Indirect (OTA)"}
            TYPE_COLORS = {"booking_engine": "#4CAF50", "front_desk": "#2196F3", "indirect": "#FF9800"}
            summary_ch = ch.groupby("channel_type").agg(
                revenue=("revenue", "sum"), rooms_sold=("rooms_sold", "sum")
            ).reset_index()
            summary_ch["share"] = summary_ch["revenue"] / (ch["revenue"].sum() or 1) * 100
            summary_ch["label"] = summary_ch["channel_type"].map(TYPE_LABELS).fillna(summary_ch["channel_type"])
            summary_ch["color"] = summary_ch["channel_type"].map(TYPE_COLORS).fillna("#9E9E9E")
            type_order = ["booking_engine", "front_desk", "indirect"]
            summary_ch["_order"] = summary_ch["channel_type"].map({t: i for i, t in enumerate(type_order)}).fillna(99)
            summary_ch = summary_ch.sort_values("_order")

            col_tbl, col_pie, col_bar_ch = st.columns([4, 3, 3])
            with col_tbl:
                st.dataframe(display_df_ch, width="stretch", hide_index=True, height=shared_height_ch)
            with col_pie:
                fig_ch = px.pie(ch, names="display_name", values="revenue",
                                color="display_name", color_discrete_map=colors, hole=0.45)
                fig_ch.update_traces(textposition="inside", textinfo="percent+label")
                fig_ch.update_layout(
                    height=shared_height_ch, margin=dict(l=0, r=0, t=10, b=80),
                    showlegend=True,
                    legend=dict(orientation="h", xanchor="center", x=0.5, y=-0.15),
                )
                st.plotly_chart(fig_ch, width="stretch", key="portfolio_fig_ch")
            with col_bar_ch:
                bar_max_ch = summary_ch["revenue"].max() if not summary_ch.empty else 1
                fig_sum_ch = go.Figure(go.Bar(
                    x=summary_ch["label"], y=summary_ch["revenue"],
                    text=summary_ch["share"].map(lambda v: f"{v:.1f}%"),
                    textposition="outside",
                    marker_color=list(summary_ch["color"]),
                    marker_line_width=0,
                ))
                fig_sum_ch.update_layout(
                    height=shared_height_ch, margin=dict(l=0, r=0, t=10, b=0),
                    xaxis_title=None, yaxis_title="Revenue", showlegend=False,
                    yaxis=dict(range=[0, bar_max_ch * 1.18]),
                )
                st.plotly_chart(fig_sum_ch, width="stretch", key="portfolio_fig_ch_bar")

    # ── Tab 5: Day of Week (portfolio) ────────────────────────────────────────
    with tab_dow:
        conn4 = get_connection()
        dow = _get_portfolio_dow_pattern(conn4, [p["id"] for p in props], date_from, date_to)
        conn4.close()

        if dow.empty:
            st.info("No day-of-week data available.")
        else:
            DOW_METRICS = {
                "avg_occ":     ("Avg Occ %",     lambda v: f"{v:.1f}%"),
                "avg_rooms":   ("Avg Rooms Sold", lambda v: f"{v:.1f}"),
                "avg_adr":     ("Avg ADR",        lambda v: f"{int(v):,}"),
                "avg_revpar":  ("Avg RevPAR",     lambda v: f"{int(v):,}"),
                "avg_revenue": ("Avg Revenue",    lambda v: _fmt_num(v)),
            }
            dow_metric = st.radio(
                "Metric", list(DOW_METRICS.keys()),
                format_func=lambda k: DOW_METRICS[k][0],
                horizontal=True, key="port_dow_metric",
            )
            label_fn = DOW_METRICS[dow_metric][1]
            col_chart_dw, col_tbl_dw = st.columns([1, 1])
            with col_chart_dw:
                dw_vals = dow[dow_metric].tolist()
                dw_max  = max(dw_vals) if dw_vals else 1
                dw_colors = [
                    "#6C63FF" if v == dw_max else
                    "#8B85FF" if v >= dw_max * 0.85 else
                    "#A5A0FF" if v >= dw_max * 0.70 else
                    "#C8C5FF"
                    for v in dw_vals
                ]
                fig_dw = go.Figure(go.Bar(
                    x=dow["day"], y=dw_vals,
                    text=dow[dow_metric].map(label_fn),
                    textposition="outside",
                    marker_color=dw_colors, marker_line_width=0,
                ))
                fig_dw.update_layout(
                    height=35*len(dow)+38,
                    margin=dict(l=0, r=0, t=10, b=0),
                    xaxis_title=None, showlegend=False,
                    yaxis_title=DOW_METRICS[dow_metric][0],
                    yaxis=dict(range=[0, dw_max * 1.18]),
                )
                st.plotly_chart(fig_dw, width="stretch", key="portfolio_fig_dw")
            with col_tbl_dw:
                tbl_dw = dow[["day", "avg_occ", "avg_rooms", "avg_adr", "avg_revpar", "avg_revenue", "n_days"]].copy()
                tbl_dw["avg_occ"]     = tbl_dw["avg_occ"].map(lambda v: f"{v:.1f}%")
                tbl_dw["avg_rooms"]   = tbl_dw["avg_rooms"].map(lambda v: f"{v:.1f}")
                tbl_dw["avg_adr"]     = tbl_dw["avg_adr"].map(lambda v: f"{int(v):,}")
                tbl_dw["avg_revpar"]  = tbl_dw["avg_revpar"].map(lambda v: f"{int(v):,}")
                tbl_dw["avg_revenue"] = tbl_dw["avg_revenue"].map(_fmt_num)
                tbl_dw.columns = ["Day", "Occ %", "Rooms", "ADR", "RevPAR", "Revenue", "n"]
                st.dataframe(tbl_dw, width="stretch", hide_index=True, height=35*len(dow)+38)
