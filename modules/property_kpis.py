"""
modules/property_kpis.py — Property Details

Single property view: KPI cards + daily trend (bar) + channel mix + DOW pattern.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import date, timedelta
from calendar import monthrange

from db import get_connection, get_active_properties, get_db


# ── Helpers ───────────────────────────────────────────────────────────────────

def _month_range(y, m):
    last = monthrange(y, m)[1]
    return date(y, m, 1), date(y, m, last)


def _prev_month(y, m):
    if m == 1:
        return y - 1, 12
    return y, m - 1


def _fmt_idr(v):
    if v is None:
        return "—"
    if abs(v) >= 1_000_000_000:
        return f"{v/1_000_000_000:.1f}B"
    if abs(v) >= 1_000_000:
        return f"{v/1_000_000:.1f}M"
    return f"{v:,.0f}"


def _fmt_num(v):
    if v is None:
        return "—"
    try:
        f = float(v)
    except (TypeError, ValueError):
        return "—"
    if f != f:
        return "—"
    return f"{int(round(f)):,}"


def _fmt_pct(v):
    if v is None:
        return "—"
    try:
        f = float(v)
    except (TypeError, ValueError):
        return "—"
    if f != f:
        return "—"
    return f"{f:.1f}%"


def _delta_str(current, compare):
    if compare is None or compare == 0 or current is None:
        return None, None
    delta = ((current - compare) / abs(compare)) * 100
    sign  = "▲" if delta >= 0 else "▼"
    return f"{sign} {abs(delta):.1f}%", delta >= 0


def _get_snapshot(conn, prop_id, date_from, date_to):
    from datetime import date as date_type

    d_from = date_type.fromisoformat(str(date_from))
    d_to   = date_type.fromisoformat(str(date_to))
    today  = date_type.today()

    # Clamp date_from to property join_date
    prop_row = conn.execute(
        "SELECT room_count, join_date FROM properties WHERE id=?", (prop_id,)
    ).fetchone()
    room_count = prop_row["room_count"] if prop_row else 0
    if prop_row and prop_row["join_date"]:
        join_date = date_type.fromisoformat(prop_row["join_date"])
        d_from = max(d_from, join_date)

    if d_from >= d_to:
        return {}

    total_nights = (d_to - d_from).days

    # Actuals from daily_snapshot (dates before today only)
    snap_to = min(d_to, today - timedelta(days=1))
    row = conn.execute("""
        SELECT
            SUM(rooms_sold)     AS rooms_sold,
            SUM(revenue_total)  AS revenue_total,
            SUM(revenue_rooms)  AS revenue_rooms,
            SUM(rehat_revenue)  AS rehat_revenue,
            SUM(bookings_count) AS bookings_count
        FROM daily_snapshot
        WHERE property_id=? AND date BETWEEN ? AND ?
    """, (prop_id, str(d_from), str(snap_to))).fetchone()

    r = dict(row) if row else {}
    rooms_sold    = r.get("rooms_sold") or 0
    revenue_rooms = r.get("revenue_rooms") or 0

    # BOB for today and future dates within the range
    bob_rooms = 0
    bob_revenue = 0
    if d_to >= today:
        bob_from = max(d_from, today)
        bob_row = conn.execute("""
            SELECT COUNT(DISTINCT booking_number || stay_date) AS room_nights,
                   SUM(COALESCE(nightly_rate_idr, 0))          AS revenue
            FROM bookings_on_books
            WHERE property_id=? AND stay_date BETWEEN ? AND ?
        """, (prop_id, str(bob_from), str(d_to))).fetchone()
        bob_rooms   = bob_row["room_nights"] or 0 if bob_row else 0
        bob_revenue = bob_row["revenue"]     or 0 if bob_row else 0

    total_rooms_sold  = rooms_sold + bob_rooms
    total_revenue     = (r.get("revenue_total") or 0) + bob_revenue
    total_capacity    = room_count * total_nights

    result = {
        "rooms_sold":     total_rooms_sold,
        "revenue_total":  total_revenue,
        "revenue_rooms":  revenue_rooms,
        "rehat_revenue":  r.get("rehat_revenue") or 0,
        "bookings_count": r.get("bookings_count") or 0,
        "room_count":     room_count,
        "occupancy_pct":  (total_rooms_sold / total_capacity * 100) if total_capacity else 0,
        "adr":            (total_revenue / total_rooms_sold)         if total_rooms_sold else 0,
        "revpar":         (total_revenue / total_capacity)           if total_capacity   else 0,
        "days_with_data": total_nights,
    }
    return result


def _get_budget(conn, prop_id, year, month):
    row = conn.execute("""
        SELECT revenue_target FROM budgets
        WHERE property_id=? AND year=? AND month=?
    """, (prop_id, year, month)).fetchone()
    return row["revenue_target"] if row else None


def _get_daily_series(conn, prop_id, date_from, date_to):
    rows = conn.execute("""
        SELECT date, rooms_sold, occupancy_pct, revenue_total,
               revenue_rooms, adr, revpar, rehat_revenue, bookings_count
        FROM daily_snapshot
        WHERE property_id=? AND date BETWEEN ? AND ?
        ORDER BY date
    """, (prop_id, str(date_from), str(date_to))).fetchall()
    return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()


def _get_unified_daily_series(conn, prop_id, date_from, date_to, room_count):
    """
    Unified daily series for the full date range:
    - Past dates (before today): from daily_snapshot (actuals)
    - Today + future: from bookings_on_books (current holdings)
    Used by both Daily Trend and Statistical Forecast so they always match.
    """
    today = date.today()
    yesterday = today - timedelta(days=1)

    # Actuals from daily_snapshot (dates strictly before today)
    snapshot_rows = conn.execute("""
        SELECT date, rooms_sold, occupancy_pct, revenue_total,
               revenue_rooms, adr, revpar, rehat_revenue, bookings_count,
               'actual' AS source
        FROM daily_snapshot
        WHERE property_id=? AND date BETWEEN ? AND ?
        ORDER BY date
    """, (prop_id, str(date_from), str(min(date_to, yesterday)))).fetchall()

    # BOB from bookings_on_books (today and future) — with real rates if available
    bob_rows = conn.execute("""
        SELECT stay_date AS date,
               COUNT(DISTINCT booking_number)    AS rooms_sold,
               SUM(COALESCE(nightly_rate_idr,0)) AS revenue
        FROM bookings_on_books
        WHERE property_id=? AND stay_date BETWEEN ? AND ?
        GROUP BY stay_date
        ORDER BY stay_date
    """, (prop_id, str(max(date_from, today)), str(date_to))).fetchall()

    records = [dict(r) for r in snapshot_rows]
    snapshot_dates = {r["date"] for r in snapshot_rows}

    for r in bob_rows:
        d = r["date"]
        if d in snapshot_dates:
            continue
        rooms   = r["rooms_sold"] or 0
        revenue = r["revenue"] or 0
        occ     = (rooms / room_count * 100) if room_count else 0
        adr     = round(revenue / rooms)    if rooms > 0 and revenue > 0 else 0
        revpar  = round(revenue / room_count) if room_count > 0 and revenue > 0 else 0
        records.append({
            "date":           d,
            "rooms_sold":     rooms,
            "occupancy_pct":  round(occ, 1),
            "revenue_total":  revenue,
            "revenue_rooms":  revenue,
            "adr":            adr,
            "revpar":         revpar,
            "rehat_revenue":  0,
            "bookings_count": rooms,
            "source":         "bob",
        })

    if not records:
        df = pd.DataFrame()
    else:
        df = pd.DataFrame(records).sort_values("date").reset_index(drop=True)

    # Fill missing dates with zeros so chart always shows full range
    all_dates = [str(date_from + timedelta(days=i)) for i in range((date_to - date_from).days + 1)]
    existing  = set(df["date"].tolist()) if not df.empty else set()
    missing   = [d for d in all_dates if d not in existing]
    if missing:
        fill = [{
            "date": d, "rooms_sold": 0, "occupancy_pct": 0.0,
            "revenue_total": 0.0, "revenue_rooms": 0.0,
            "adr": 0.0, "revpar": 0.0, "rehat_revenue": 0.0,
            "bookings_count": 0,
            "source": "bob" if d >= str(today) else "actual",
        } for d in missing]
        df = pd.concat([df, pd.DataFrame(fill)], ignore_index=True).sort_values("date").reset_index(drop=True)

    return df


# ── Channel mapping ───────────────────────────────────────────────────────────
def _get_channel_mix(conn, prop_id, date_from, date_to):
    """
    Query channel mix. Handles two states:
    - agent_name column exists (post-migration): uses OTA-level breakdown
    - agent_name column missing (pre-migration): falls back to booking_source only
    Aggregates by display_name to avoid duplicates from multiple raw sources.
    """
    # Check if agent_name column exists
    cols = [r[1] for r in conn.execute("PRAGMA table_info(raw_services)").fetchall()]
    has_agent = "agent_name" in cols
    has_mappings = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='channel_mappings'"
    ).fetchone()[0] > 0

    if has_agent and has_mappings:
        # Two-step: resolve each row to a canonical display_name, then aggregate.
        # Step 1 — per-row resolved name (handles NULL agent_name via booking_source fallback)
        # Step 2 — GROUP BY the resolved name so duplicates merge cleanly.
        rows = conn.execute("""
            SELECT
                resolved_name  AS display_name,
                resolved_type  AS channel_type,
                COUNT(DISTINCT booking_number) AS bookings,
                SUM(amount_idr)                AS revenue,
                SUM(amount_idr) / NULLIF(COUNT(DISTINCT reservation_id), 0) AS adr
            FROM (
                SELECT
                    s.booking_number,
                    s.reservation_id,
                    s.amount_idr,
                    -- Canonical display name: mapping > synthetic key > booking_source
                    COALESCE(
                        cm.display_name,
                        CASE s.agent_name
                            WHEN '__front_desk__'    THEN 'Walk In'
                            WHEN '__official_site__' THEN 'Official Website'
                        END,
                        CASE s.booking_source
                            WHEN 'Front desk'      THEN 'Walk In'
                            WHEN 'Official site'   THEN 'Official Website'
                            WHEN 'Mobile extranet' THEN 'Mobile'
                            WHEN 'Channel manager' THEN 'Channel Manager'
                        END,
                        COALESCE(s.agent_name, s.booking_source, 'Unknown')
                    ) AS resolved_name,
                    -- Canonical channel type
                    COALESCE(
                        cm.channel_type,
                        CASE s.agent_name
                            WHEN '__front_desk__'    THEN 'front_desk'
                            WHEN '__official_site__' THEN 'booking_engine'
                        END,
                        CASE s.booking_source
                            WHEN 'Front desk'    THEN 'front_desk'
                            WHEN 'Official site' THEN 'booking_engine'
                            ELSE 'indirect'
                        END
                    ) AS resolved_type
                FROM raw_services s
                LEFT JOIN channel_mappings cm
                    ON cm.property_id = s.property_id
                    AND s.agent_name IS NOT NULL
                    AND cm.raw_agent_name = s.agent_name
                WHERE s.property_id=? AND s.date BETWEEN ? AND ? AND s.kind=0
            )
            GROUP BY resolved_name, resolved_type
            ORDER BY revenue DESC
        """, (prop_id, str(date_from), str(date_to))).fetchall()
    else:
        # Fallback: booking_source only
        rows = conn.execute("""
            SELECT
                CASE
                    WHEN booking_source = 'Front desk'      THEN 'Walk In'
                    WHEN booking_source = 'Official site'   THEN 'Official Website'
                    WHEN booking_source = 'Channel manager' THEN 'Channel Manager'
                    ELSE COALESCE(booking_source, 'Unknown')
                END AS display_name,
                CASE
                    WHEN booking_source = 'Front desk'    THEN 'front_desk'
                    WHEN booking_source = 'Official site' THEN 'booking_engine'
                    ELSE 'indirect'
                END AS channel_type,
                COUNT(DISTINCT booking_number)                               AS bookings,
                SUM(amount_idr)                                              AS revenue,
                SUM(amount_idr) / NULLIF(COUNT(DISTINCT reservation_id), 0) AS adr
            FROM raw_services
            WHERE property_id=? AND date BETWEEN ? AND ? AND kind=0
            GROUP BY display_name, channel_type
            ORDER BY revenue DESC
        """, (prop_id, str(date_from), str(date_to))).fetchall()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame([dict(r) for r in rows])
    total_rev = df["revenue"].sum() or 1
    df["rev_share"] = df["revenue"] / total_rev * 100
    return df



def _get_dow_pattern(conn, prop_id, date_from, date_to):
    rows = conn.execute("""
        SELECT
            CAST(strftime('%w', date) AS INTEGER) AS dow,
            AVG(occupancy_pct) AS avg_occ,
            AVG(rooms_sold)    AS avg_rooms,
            AVG(adr)           AS avg_adr,
            AVG(revpar)        AS avg_revpar,
            AVG(revenue_total) AS avg_revenue,
            COUNT(*)           AS n_days
        FROM daily_snapshot
        WHERE property_id=? AND date BETWEEN ? AND ?
        GROUP BY dow ORDER BY dow
    """, (prop_id, str(date_from), str(date_to))).fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame([dict(r) for r in rows])
    dow_map = {0: "Sun", 1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat"}
    df["day"] = df["dow"].map(dow_map)
    return df


# ── KPI Card ──────────────────────────────────────────────────────────────────

def _kpi_card(col, label, value_str, delta_yoy=None, pos_yoy=None,
              delta_mom=None, pos_mom=None, delta_budget=None, pos_budget=None):
    with col:
        lines = [f"**{label}**", f"### {value_str}"]
        if delta_yoy:
            color = "green" if pos_yoy else "red"
            lines.append(f":{color}[{delta_yoy} YoY]")
        if delta_mom:
            color = "green" if pos_mom else "red"
            lines.append(f":{color}[{delta_mom} MoM]")
        if delta_budget:
            color = "green" if pos_budget else "red"
            lines.append(f":{color}[{delta_budget} vs Budget]")
        st.markdown("  \n".join(lines))
        st.divider()


# ── Main render ───────────────────────────────────────────────────────────────

def render():
    st.subheader("Property Details")

    props = sorted(get_active_properties(), key=lambda p: str(p["id"]))
    if not props:
        st.warning("No active properties. Add properties in 🔧 Property Config.")
        return

    col_prop, col_from, col_to = st.columns([2, 1, 1])

    with col_prop:
        prop_options = {p["id"]: p["name"] for p in props}
        selected_id  = st.selectbox(
            "Property",
            options=list(prop_options.keys()),
            format_func=lambda k: prop_options[k],
        )

    today = date.today()
    default_from, default_to = _month_range(today.year, today.month)

    with col_from:
        date_from = st.date_input("From", value=default_from, key=f"prop_from_{selected_id}")
    with col_to:
        date_to = st.date_input("To", value=default_to, key=f"prop_to_{selected_id}")

    if date_from > date_to:
        st.error("From date must be before To date.")
        return

    prop = next(p for p in props if p["id"] == selected_id)
    conn = get_connection()

    curr = _get_snapshot(conn, selected_id, date_from, date_to)

    if not curr or curr.get("days_with_data", 0) == 0:
        conn.close()
        st.info(f"No data yet for **{prop['name']}** in this period. Run ingestion from ⚙️ System Status.")
        return

    yoy_from = date_from.replace(year=date_from.year - 1)
    yoy_to   = date_to.replace(year=date_to.year - 1)
    yoy = _get_snapshot(conn, selected_id, yoy_from, yoy_to)

    days_in_range = (date_to - date_from).days + 1
    mom_to   = date_from - timedelta(days=1)
    mom_from = mom_to - timedelta(days=days_in_range - 1)
    mom = _get_snapshot(conn, selected_id, mom_from, mom_to)

    budget_revenue = _get_budget(conn, selected_id, date_from.year, date_from.month)

    # ── KPI Cards: Room Nights, Occupancy, ADR, RevPAR, Revenue ──────────────
    st.markdown(f"**{prop['name']}** · {date_from} → {date_to} · `{prop['contract_type']}`")

    c1, c2, c3, c4, c5 = st.columns(5)

    d_yoy, p_yoy = _delta_str(curr.get("rooms_sold"), yoy.get("rooms_sold"))
    d_mom, p_mom = _delta_str(curr.get("rooms_sold"), mom.get("rooms_sold"))
    _kpi_card(c1, "Room Nights", _fmt_num(curr.get("rooms_sold")),
              d_yoy, p_yoy, d_mom, p_mom)

    d_yoy, p_yoy = _delta_str(curr.get("occupancy_pct"), yoy.get("occupancy_pct"))
    d_mom, p_mom = _delta_str(curr.get("occupancy_pct"), mom.get("occupancy_pct"))
    _kpi_card(c2, "Occupancy", _fmt_pct(curr.get("occupancy_pct")),
              d_yoy, p_yoy, d_mom, p_mom)

    d_yoy, p_yoy = _delta_str(curr.get("adr"), yoy.get("adr"))
    d_mom, p_mom = _delta_str(curr.get("adr"), mom.get("adr"))
    _kpi_card(c3, "ADR", _fmt_num(curr.get("adr")),
              d_yoy, p_yoy, d_mom, p_mom)

    d_yoy, p_yoy = _delta_str(curr.get("revpar"), yoy.get("revpar"))
    d_mom, p_mom = _delta_str(curr.get("revpar"), mom.get("revpar"))
    _kpi_card(c4, "RevPAR", _fmt_num(curr.get("revpar")),
              d_yoy, p_yoy, d_mom, p_mom)

    d_yoy, p_yoy = _delta_str(curr.get("revenue_total"), yoy.get("revenue_total"))
    d_mom, p_mom = _delta_str(curr.get("revenue_total"), mom.get("revenue_total"))
    _kpi_card(c5, "Revenue", _fmt_num(curr.get("revenue_total")),
              d_yoy, p_yoy, d_mom, p_mom)

    if budget_revenue:
        pct = (curr.get("revenue_total") or 0) / budget_revenue
        st.markdown(f"**Budget Progress** — {_fmt_num(curr.get('revenue_total'))} of {_fmt_num(budget_revenue)} target")
        st.progress(min(pct, 1.0), text=f"{pct*100:.1f}% of monthly budget")

    st.divider()

    room_count_prop = prop.get("room_count") or 1
    daily = _get_unified_daily_series(conn, selected_id, date_from, date_to, room_count_prop)

    if not daily.empty:
        tab_trend, tab_channel, tab_dow = st.tabs(["Daily Trend", "Channel Mix", "Day of Week"])

        # ── Daily Trend: bar chart, metric order: Occ%, Rooms, ADR, RevPAR, Revenue
        with tab_trend:
            metric = st.radio(
                "Metric",
                ["Occ %", "Room Sold", "ADR", "RevPAR", "Revenue"],
                horizontal=True,
            )
            col_map = {
                "Occ %":     "occupancy_pct",
                "Room Sold": "rooms_sold",
                "ADR":       "adr",
                "RevPAR":    "revpar",
                "Revenue":   "revenue_total",
            }
            y_col = col_map[metric]

            # Split by source: actual (dark blue) vs BOB (light blue)
            actual_trend = daily[daily["source"] == "actual"] if "source" in daily.columns else daily
            bob_trend    = daily[daily["source"] == "bob"]    if "source" in daily.columns else daily.iloc[0:0]

            def _fmt_label(v):
                if v is None:
                    return ""
                if metric == "Occ %":
                    return f"{v:.1f}"
                elif metric in ("ADR", "RevPAR", "Revenue"):
                    return _fmt_num(v)
                return f"{int(v):,}"

            fig = go.Figure()
            if not actual_trend.empty:
                fig.add_trace(go.Bar(
                    x=actual_trend["date"], y=actual_trend[y_col],
                    name="Actual", marker_color="#6C63FF", opacity=0.85,
                    marker_line_width=0,
                    text=actual_trend[y_col].map(_fmt_label),
                    textposition="outside", textfont=dict(size=10),
                ))
            if not bob_trend.empty:
                fig.add_trace(go.Bar(
                    x=bob_trend["date"], y=bob_trend[y_col],
                    name="BOB", marker_color="#C4B5FD", opacity=0.7,
                    marker_line_width=0,
                    text=bob_trend[y_col].map(_fmt_label),
                    textposition="outside", textfont=dict(size=10),
                ))
            fig.update_layout(
                height=360,
                margin=dict(l=0, r=0, t=30, b=0),
                xaxis_title=None,
                yaxis_title=metric,
                barmode="overlay",
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
                uniformtext_minsize=8,
                uniformtext_mode="hide",
            )
            st.plotly_chart(fig, use_container_width=True)

        # ── Channel Mix ───────────────────────────────────────────────────────
        with tab_channel:
            ch = _get_channel_mix(conn, selected_id, date_from, date_to)

            if ch.empty:
                st.info("No booking source data available for this period.")
            else:
                # Categorical palette — distinct colors per channel
                PALETTE = [
                    "#06B6D4","#8B5CF6","#10B981","#F59E0B",
                    "#EF4444","#EC4899","#14B8A6","#F97316","#6366F1","#84CC16",
                ]
                TYPE_BASE_COLORS = {
                    "booking_engine": "#6366F1",
                    "front_desk":     "#0F766E",
                }
                indirect_rows = ch[ch["channel_type"] == "indirect"].reset_index(drop=True)
                colors = {}
                for i, row in indirect_rows.iterrows():
                    colors[row["display_name"]] = PALETTE[i % len(PALETTE)]
                for _, row in ch[ch["channel_type"] != "indirect"].iterrows():
                    colors[row["display_name"]] = TYPE_BASE_COLORS.get(row["channel_type"], "#9E9E9E")

                ch["color"] = ch["display_name"].map(colors)

                col_table, col_donut, col_bar = st.columns([45, 30, 25])

                with col_table:
                    table_rows = []
                    for _, row in ch.iterrows():
                        adr_val = row.get("adr") or 0
                        table_rows.append({
                            "Channel":      row["display_name"],
                            "Bookings":     int(row["bookings"]),
                            "Revenue":      _fmt_num(row["revenue"]),
                            "Rev Share %":  f"{row['rev_share']:.1f}%",
                            "ADR":          f"{int(adr_val):,}",
                        })
                    display_df = pd.DataFrame(table_rows)
                    st.dataframe(display_df, use_container_width=True, hide_index=True, height=360)

                with col_donut:
                    fig_ch = px.pie(
                        ch, names="display_name", values="revenue",
                        color="display_name",
                        color_discrete_map=colors,
                        hole=0.45,
                    )
                    fig_ch.update_traces(textposition="inside", textinfo="percent+label")
                    fig_ch.update_layout(
                        height=380, margin=dict(l=0, r=0, t=10, b=0),
                        showlegend=True,
                        legend=dict(orientation="v", x=1.02, y=0.5),
                    )
                    st.plotly_chart(fig_ch, use_container_width=True)

                with col_bar:
                    # Channel type summary bar (Booking Engine / Front Desk / Indirect)
                    TYPE_LABELS = {
                        "booking_engine": "Booking Engine",
                        "front_desk":     "Front Desk",
                        "indirect":       "Indirect (OTA)",
                    }
                    TYPE_COLORS = {
                        "booking_engine": "#6366F1",
                        "front_desk":     "#0F766E",
                        "indirect":       "#06B6D4",
                    }
                    summary = ch.groupby("channel_type").agg(
                        revenue=("revenue", "sum"), bookings=("bookings", "sum")
                    ).reset_index()
                    summary["share"] = summary["revenue"] / (ch["revenue"].sum() or 1) * 100
                    summary["label"] = summary["channel_type"].map(TYPE_LABELS).fillna(summary["channel_type"])
                    summary["color"] = summary["channel_type"].map(TYPE_COLORS).fillna("#9E9E9E")
                    type_order = ["booking_engine", "front_desk", "indirect"]
                    summary["_order"] = summary["channel_type"].map({t: i for i, t in enumerate(type_order)}).fillna(99)
                    summary = summary.sort_values("_order")

                    fig_sum = go.Figure(go.Bar(
                        x=summary["label"], y=summary["revenue"],
                        text=summary["share"].map(lambda v: f"{v:.1f}%"),
                        textposition="outside",
                        marker_color=list(summary["color"]),
                        marker_line_width=0,
                    ))
                    fig_sum.update_layout(
                        height=380, margin=dict(l=0, r=0, t=10, b=0),
                        xaxis_title=None, yaxis_title="Revenue", showlegend=False,
                    )
                    st.plotly_chart(fig_sum, use_container_width=True)

        # ── Day of Week ───────────────────────────────────────────────────────
        with tab_dow:
            dow = _get_dow_pattern(conn, selected_id, date_from, date_to)
            if dow.empty:
                st.info("No day-of-week data available.")
            else:
                DOW_METRICS = {
                    "avg_occ":     ("Avg Occ %",      lambda v: f"{v:.1f}%"),
                    "avg_rooms":   ("Avg Rooms Sold",  lambda v: f"{v:.1f}"),
                    "avg_adr":     ("Avg ADR",         lambda v: f"{int(v):,}"),
                    "avg_revpar":  ("Avg RevPAR",      lambda v: f"{int(v):,}"),
                    "avg_revenue": ("Avg Revenue",     lambda v: _fmt_num(v)),
                }
                dow_metric = st.radio(
                    "Metric",
                    list(DOW_METRICS.keys()),
                    format_func=lambda k: DOW_METRICS[k][0],
                    horizontal=True,
                    key="dow_metric",
                )
                label_fn = DOW_METRICS[dow_metric][1]
                col_chart, col_table = st.columns([1, 1])

                with col_chart:
                    WEEKEND = {"Fri", "Sat", "Sun"}
                    bar_colors = [
                        "#6366F1" if d in WEEKEND else "#CBD5E1"
                        for d in dow["day"]
                    ]
                    fig_dow = go.Figure(go.Bar(
                        x=dow["day"],
                        y=dow[dow_metric],
                        text=dow[dow_metric].map(label_fn),
                        textposition="outside",
                        marker_color=bar_colors,
                        marker_line_width=0,
                    ))
                    fig_dow.update_layout(
                        height=300, margin=dict(l=0, r=0, t=24, b=0),
                        xaxis_title=None, showlegend=False,
                        yaxis_title=DOW_METRICS[dow_metric][0],
                        plot_bgcolor="white",
                        yaxis=dict(gridcolor="#F1F5F9"),
                    )
                    st.plotly_chart(fig_dow, use_container_width=True)

                with col_table:
                    tbl = dow[["day", "avg_occ", "avg_rooms", "avg_adr", "avg_revpar", "avg_revenue", "n_days"]].copy()
                    tbl["avg_occ"]     = tbl["avg_occ"].map(lambda v: f"{v:.1f}%")
                    tbl["avg_rooms"]   = tbl["avg_rooms"].map(lambda v: f"{v:.1f}")
                    tbl["avg_adr"]     = tbl["avg_adr"].map(lambda v: f"{int(v):,}")
                    tbl["avg_revpar"]  = tbl["avg_revpar"].map(lambda v: f"{int(v):,}")
                    tbl["avg_revenue"] = tbl["avg_revenue"].map(_fmt_num)
                    tbl.columns = ["Day", "Occ %", "Rooms", "ADR", "RevPAR", "Revenue", "n"]
                    st.dataframe(tbl, use_container_width=True, hide_index=True, height=300)

    st.divider()
    st.subheader("Forecast")
    tab_forecast, tab_overview, tab_events = st.tabs([
        "Statistical Forecast", "90 Day Overview", "Calendar Events",
    ])

    # ── Statistical Forecast ──────────────────────────────────────────────────
    with tab_forecast:
        from modules.forecasting import _build_forecast
        # Uses same date range as the top-level pickers — no separate date input
        fc_from = date_from
        fc_to   = date_to
        st.caption(f"Showing {fc_from} → {fc_to} · BOB-anchored where holdings exist, DOW stat elsewhere.")

        hist_row = conn.execute(
            "SELECT MIN(date) AS earliest, COUNT(*) as n FROM daily_snapshot WHERE property_id=?",
            (selected_id,)
        ).fetchone()
        earliest = hist_row["earliest"] if hist_row else None

        if not earliest:
            st.warning("No historical data yet. Ingest a few weeks of data first.")
        else:
            st.caption(f"Historical data from: **{earliest}** ({hist_row['n']} days)")
            df_fc = _build_forecast(conn, selected_id, prop, fc_from, fc_to)
            if df_fc.empty:
                st.info("No forecast data generated.")
            else:
                actual_days = (df_fc["source"] == "actual").sum()
                bob_days    = (df_fc["source"] == "bob").sum()
                stat_days   = (df_fc["source"] == "stat").sum()
                st.caption(f"Actual {actual_days}d · BOB {bob_days}d · Stat {stat_days}d")

                room_count_fc     = prop.get("room_count") or 1
                total_rooms_avail = room_count_fc * len(df_fc)
                fcst_rooms  = df_fc["forecast_rooms"].sum()
                fcst_occ    = (fcst_rooms / total_rooms_avail * 100) if total_rooms_avail else 0
                fcst_adr    = df_fc.loc[df_fc["forecast_rooms"] > 0, "forecast_adr"].mean() if (df_fc["forecast_rooms"] > 0).any() else 0
                fcst_revpar = fcst_adr * fcst_occ / 100
                fcst_rev    = df_fc["forecast_revenue"].sum()

                c1, c2, c3, c4, c5 = st.columns(5)
                c1.metric("Fcst Rooms",   f"{int(fcst_rooms):,}")
                c2.metric("Fcst Occ %",   f"{fcst_occ:.1f}%")
                c3.metric("Fcst ADR",     _fmt_idr(fcst_adr))
                c4.metric("Fcst RevPAR",  _fmt_idr(fcst_revpar))
                c5.metric("Fcst Revenue", _fmt_idr(fcst_rev))

                actual_df = df_fc[df_fc["source"] == "actual"]
                bob_df    = df_fc[df_fc["source"] == "bob"]
                future_df = df_fc[df_fc["source"] != "actual"]

                fig_fc = go.Figure()
                if not actual_df.empty:
                    fig_fc.add_trace(go.Bar(
                        x=actual_df["date"], y=actual_df["bob_occ"],
                        name="Actual Occ %", opacity=0.85,
                        marker_color="#6C63FF",
                        text=actual_df["bob_occ"].map(lambda v: f"{v:.1f}"),
                        textposition="outside", textfont=dict(size=10),
                    ))
                if not bob_df.empty:
                    fig_fc.add_trace(go.Bar(
                        x=bob_df["date"], y=bob_df["bob_occ"],
                        name="BOB (Holdings %)", opacity=0.7,
                        marker_color="#C4B5FD",
                        text=bob_df["bob_occ"].map(lambda v: f"{v:.1f}"),
                        textposition="outside", textfont=dict(size=10),
                    ))
                if not future_df.empty:
                    fig_fc.add_trace(go.Scatter(
                        x=future_df["date"], y=future_df["forecast_occ"],
                        name="Forecast Occ %", mode="lines+markers",
                        line=dict(dash="dash", width=2, color="#FF9800"),
                        marker=dict(size=4),
                    ))
                events_df = df_fc[df_fc["event"].notna()].copy()
                if not events_df.empty:
                    events_df["marker_y"] = events_df.apply(
                        lambda r: min(r["bob_occ"] if r["source"] == "actual" else r["forecast_occ"], 105),
                        axis=1
                    )
                    fig_fc.add_trace(go.Scatter(
                        x=events_df["date"], y=events_df["marker_y"],
                        mode="markers", marker=dict(symbol="star", size=14, color="#E91E63"),
                        name="Event", text=events_df["event"],
                        hovertemplate="%{x}<br>%{text}<extra></extra>",
                    ))
                fig_fc.update_layout(
                    height=380, margin=dict(l=0, r=0, t=30, b=0),
                    yaxis=dict(range=[0, 115], title="Occupancy %"),
                    barmode="overlay", hovermode="x unified",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                st.plotly_chart(fig_fc, use_container_width=True)

                # Pickup d-1: bookings created YESTERDAY, grouped by check_in (arrival date)
                yesterday_str_fc = str(date.today() - timedelta(days=1))
                d1_rows_fc = conn.execute("""
                    SELECT 
                        substr(check_in,1,4)||'-'||substr(check_in,5,2)||'-'||substr(check_in,7,2) AS arrival_date,
                        COUNT(DISTINCT reservation_id) AS rooms_picked_up
                    FROM raw_services
                    WHERE property_id=?
                      AND kind=0
                      AND creation_date=?
                      AND substr(check_in,1,4)||'-'||substr(check_in,5,2)||'-'||substr(check_in,7,2) BETWEEN ? AND ?
                    GROUP BY arrival_date
                """, (selected_id, yesterday_str_fc, str(fc_from), str(fc_to))).fetchall()
                d1_map_fc = {r["arrival_date"]: r["rooms_picked_up"] for r in d1_rows_fc}

                show = df_fc[["date","dow",
                              "bob_rooms","bob_occ","bob_adr","bob_revenue",
                              "forecast_rooms","forecast_occ","forecast_adr","forecast_revpar","forecast_revenue",
                              "event"]].copy()
                show.columns = ["Date","DOW",
                                "BOB Rooms","BOB Occ%","BOB ADR","BOB Revenue",
                                "Fcst Rooms","Fcst Occ%","Fcst ADR","Fcst RevPAR","Fcst Revenue","Event"]
                show["Pickup d-1"] = show["Date"].map(lambda d: f"+{d1_map_fc[d]}" if d1_map_fc.get(d, 0) > 0 else str(d1_map_fc.get(d, 0)))
                show["BOB Occ%"]     = show["BOB Occ%"].map(lambda v: f"{v:.1f}%")
                show["BOB ADR"]      = show["BOB ADR"].map(_fmt_idr)
                show["BOB Revenue"]  = show["BOB Revenue"].map(_fmt_idr)
                show["Fcst Occ%"]    = show["Fcst Occ%"].map(lambda v: f"{v:.1f}%")
                show["Fcst ADR"]     = show["Fcst ADR"].map(_fmt_idr)
                show["Fcst RevPAR"]  = show["Fcst RevPAR"].map(_fmt_idr)
                show["Fcst Revenue"] = show["Fcst Revenue"].map(_fmt_idr)
                show["Event"]        = show["Event"].fillna("—")
                show = show[["Date","DOW",
                             "BOB Rooms","BOB Occ%","BOB ADR","BOB Revenue",
                             "Fcst Rooms","Fcst Occ%","Fcst ADR","Fcst RevPAR","Fcst Revenue",
                             "Pickup d-1","Event"]]
                st.dataframe(show, use_container_width=True, hide_index=True,
                             height=min(35 * len(show) + 38, 600))

    # ── 90 Day Overview ──────────────────────────────────────────────────────────
    with tab_overview:
        from modules.forecasting import _build_forecast
        ov_from = date.today()
        ov_to   = ov_from + timedelta(days=89)

        hist_row_ov = conn.execute(
            "SELECT MIN(date) AS earliest FROM daily_snapshot WHERE property_id=?",
            (selected_id,)
        ).fetchone()

        if not hist_row_ov or not hist_row_ov["earliest"]:
            st.warning("No historical data yet. Ingest first.")
        else:
            df_ov = _build_forecast(conn, selected_id, prop, ov_from, ov_to)

            if df_ov.empty:
                st.info("No forecast data available.")
            else:
                # Pickup d-1: rooms booked today, shown on their ARRIVAL date (check_in)
                # A 3-night booking made today counts as +1 on check_in date only
                yesterday_str = str(date.today() - timedelta(days=1))

                d1_rows = conn.execute("""
                    SELECT 
                        substr(check_in,1,4)||'-'||substr(check_in,5,2)||'-'||substr(check_in,7,2) AS arrival_date,
                        COUNT(DISTINCT reservation_id) AS rooms_picked_up
                    FROM raw_services
                    WHERE property_id=?
                      AND kind=0
                      AND creation_date=?
                      AND substr(check_in,1,4)||'-'||substr(check_in,5,2)||'-'||substr(check_in,7,2) BETWEEN ? AND ?
                    GROUP BY arrival_date
                """, (selected_id, yesterday_str, str(ov_from), str(ov_to))).fetchall()
                d1_map = {r["arrival_date"]: r["rooms_picked_up"] for r in d1_rows}

                # Build display table
                rows_out = []
                for _, r in df_ov.iterrows():
                    d = r["date"]
                    bob_now   = r["bob_rooms"]
                    pickup_d1 = d1_map.get(d, 0)  # rooms actually booked yesterday for this stay date
                    rows_out.append({
                        "Date":         d,
                        "DOW":          r["dow"],
                        "BOB Rooms":    int(bob_now),
                        "BOB Occ%":     f"{r['bob_occ']:.1f}%",
                        "BOB ADR":      _fmt_idr(r["bob_adr"]) if r.get("bob_adr", 0) > 0 else "—",
                        "BOB Revenue":  _fmt_idr(r["bob_revenue"]) if r.get("bob_revenue", 0) > 0 else "—",
                        "Fcst Rooms":   int(r["forecast_rooms"]),
                        "Fcst Occ%":    f"{r['forecast_occ']:.1f}%",
                        "Fcst ADR":     _fmt_idr(r["forecast_adr"]),
                        "Fcst RevPAR":  _fmt_idr(r.get("forecast_revpar", 0)),
                        "Fcst Revenue": _fmt_idr(r["forecast_revenue"]),
                        "Pickup d-1":    f"+{pickup_d1}" if pickup_d1 > 0 else str(pickup_d1),
                        "Event":        r["event"] if r["event"] else "—",
                    })

                df_disp = pd.DataFrame(rows_out)
                st.caption(f"Rolling 90 days: {ov_from} → {ov_to}")
                st.dataframe(
                    df_disp,
                    use_container_width=True,
                    hide_index=True,
                    height=35 * len(df_disp) + 38,  # show all 90 rows, no scroll cap
                )

    # ── Calendar Events ───────────────────────────────────────────────────────
    with tab_events:
        from modules.forecasting import _render_event_manager
        _render_event_manager(conn)

    conn.close()
