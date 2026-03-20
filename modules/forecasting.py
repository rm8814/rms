"""
modules/forecasting.py — Forecasting

Three components:
  1. Bookings on Books (BOB) — actual future reservations already in Exely
  2. Pickup Pace — BOB vs same window last year (how fast are we filling?)
  3. Statistical Forecast — DOW-based historical averages + calendar event adjustments
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import date, timedelta
from calendar import monthrange

from db import get_connection, get_active_properties
from modules.property_kpis import _fmt_idr, _fmt_pct, _month_range
from ingestion.bookings import get_bob_series, get_bob_summary

MONTHS = ["Jan","Feb","Mar","Apr","May","Jun",
          "Jul","Aug","Sep","Oct","Nov","Dec"]
DOW_LABELS = {0:"Sun",1:"Mon",2:"Tue",3:"Wed",4:"Thu",5:"Fri",6:"Sat"}


# ── Statistical forecast helpers ──────────────────────────────────────────────

def _get_dow_averages(conn, prop_id: str) -> dict:
    """
    Compute average occupancy_pct, adr, revenue_total per day-of-week
    from ALL available historical daily_snapshot data.
    Returns {dow_int: {occ, adr, revenue}}
    """
    rows = conn.execute("""
        SELECT
            CAST(strftime('%w', date) AS INTEGER) AS dow,
            AVG(occupancy_pct)   AS avg_occ,
            AVG(adr)             AS avg_adr,
            AVG(revenue_total)   AS avg_revenue,
            COUNT(*)             AS n
        FROM daily_snapshot
        WHERE property_id=? AND occupancy_pct IS NOT NULL
        GROUP BY dow
    """, (prop_id,)).fetchall()
    return {r["dow"]: dict(r) for r in rows}


def _get_calendar_events(conn, date_from: date, date_to: date) -> dict:
    """Return {date_str: event_name} for events in range."""
    rows = conn.execute("""
        SELECT date, name, impact FROM calendar_events
        WHERE date BETWEEN ? AND ?
        ORDER BY date
    """, (str(date_from), str(date_to))).fetchall()
    return {r["date"]: {"name": r["name"], "impact": r["impact"]} for r in rows}


def _get_current_adr(conn, prop_id: str, date_from: date, date_to: date) -> float:
    """Get average ADR from actuals in the selected period for BOB revenue estimation."""
    row = conn.execute("""
        SELECT AVG(adr) as avg_adr FROM daily_snapshot
        WHERE property_id=? AND date BETWEEN ? AND ? AND adr > 0
    """, (prop_id, str(date_from), str(date_to))).fetchone()
    if row and row["avg_adr"]:
        return row["avg_adr"]
    # Fall back to all-time ADR
    row2 = conn.execute("""
        SELECT AVG(adr) as avg_adr FROM daily_snapshot
        WHERE property_id=? AND adr > 0
    """, (prop_id,)).fetchone()
    return (row2["avg_adr"] or 0) if row2 else 0


def _get_actuals_series(conn, prop_id: str, date_from: date, date_to: date) -> dict:
    """
    Get actual rooms_sold, occ%, adr, revenue from daily_snapshot for the period.
    Returns {date_str: {rooms_sold, occupancy_pct, adr, revenue_total}}
    """
    rows = conn.execute("""
        SELECT date, rooms_sold, occupancy_pct, adr, revenue_total
        FROM daily_snapshot
        WHERE property_id=? AND date BETWEEN ? AND ?
    """, (prop_id, str(date_from), str(date_to))).fetchall()
    return {r["date"]: dict(r) for r in rows}


def _build_forecast(conn, prop_id: str, prop: dict,
                    date_from: date, date_to: date) -> pd.DataFrame:
    """
    Build a day-by-day forecast:
    - Past dates: use daily_snapshot actuals as the "holding" (source='actual')
    - Future dates with BOB: use bookings_on_books as floor (source='bob')
    - Future dates without BOB: DOW statistical fallback (source='stat')
    - Calendar event multiplier applied to statistical/remaining forecast component only
    """
    today         = date.today()
    yesterday     = today - timedelta(days=1)
    dow_avgs      = _get_dow_averages(conn, prop_id)
    events        = _get_calendar_events(conn, date_from, date_to)
    bob           = get_bob_series(conn, prop_id, today, date_to)   # today onward
    actuals       = _get_actuals_series(conn, prop_id, date_from, min(date_to, yesterday))
    period_adr    = _get_current_adr(conn, prop_id, date_from, date_to)
    room_count    = prop.get("room_count") or 1

    rows = []
    cursor = date_from
    while cursor <= date_to:
        dow   = cursor.weekday()
        dow_w = (dow + 1) % 7             # strftime %w: 0=Sun
        avg   = dow_avgs.get(dow_w, {})

        event = events.get(str(cursor))
        if event:
            try:
                mult = float(event["impact"])
            except (TypeError, ValueError):
                mult = {"high": 1.25, "medium": 1.10, "low": 1.05}.get(event["impact"], 1.0)
        else:
            mult = 1.0

        actual = actuals.get(str(cursor))

        if actual:
            # Past date — use actuals as the definitive holding signal
            bob_rooms   = actual["rooms_sold"] or 0
            bob_occ     = actual["occupancy_pct"] or 0
            bob_adr     = actual["adr"] or 0
            bob_revenue = round(bob_rooms * bob_adr)
            # Forecast = actuals (no adjustment for past)
            forecast_occ     = bob_occ
            forecast_rooms   = bob_rooms
            forecast_adr     = bob_adr
            forecast_revenue = round(actual["revenue_total"] or bob_revenue)
            source = "actual"

        else:
            # Future date
            bob_entry   = bob.get(str(cursor), {})
            bob_rooms   = bob_entry.get("rooms", 0) if isinstance(bob_entry, dict) else bob_entry
            bob_rev_raw = bob_entry.get("revenue", 0) if isinstance(bob_entry, dict) else 0
            bob_occ     = (bob_rooms / room_count * 100) if room_count else 0
            # Use real rate if available, fall back to DOW avg
            if bob_rooms > 0 and bob_rev_raw > 0:
                bob_adr = round(bob_rev_raw / bob_rooms)
            else:
                bob_adr = period_adr or avg.get("avg_adr") or 0
            bob_revenue = bob_rev_raw if bob_rev_raw > 0 else round(bob_rooms * bob_adr)

            if bob_rooms > 0:
                remaining_occ    = max((avg.get("avg_occ") or 65.0) - bob_occ, 0)
                forecast_occ     = min(bob_occ + remaining_occ * mult, 100.0)
                forecast_rooms   = round(room_count * forecast_occ / 100)
                forecast_adr     = bob_adr
                forecast_revenue = round(forecast_rooms * forecast_adr)
                source = "bob"
            else:
                base_occ         = avg.get("avg_occ") or 65.0
                forecast_occ     = min(base_occ * mult, 100.0)
                forecast_rooms   = round(room_count * forecast_occ / 100)
                forecast_adr     = (avg.get("avg_adr") or 0) * mult
                forecast_revenue = round((avg.get("avg_revenue") or 0) * mult)
                bob_adr          = 0
                source = "stat"

        rows.append({
            "date":             str(cursor),
            "dow":              DOW_LABELS.get(dow_w, "?"),
            "bob_rooms":        bob_rooms,
            "bob_occ":          round(bob_occ, 1),
            "bob_adr":          round(bob_adr) if bob_rooms > 0 else 0,
            "bob_revenue":      bob_revenue,
            "forecast_occ":     round(forecast_occ, 1),
            "forecast_rooms":   forecast_rooms,
            "forecast_adr":     round(forecast_adr),
            "forecast_revpar":  round(forecast_adr * forecast_occ / 100) if forecast_occ else 0,
            "forecast_revenue": forecast_revenue,
            "source":           source,
            "event":            event["name"] if event else None,
            "has_data":         bool(avg),
        })
        cursor += timedelta(days=1)

    return pd.DataFrame(rows)


def _get_yoy_bob(conn, prop_id: str, date_from: date, date_to: date) -> dict:
    """
    Get last year's actual occupancy for same period (pace comparison).
    Returns {date_str: occ_pct}
    """
    yoy_from = date_from.replace(year=date_from.year - 1)
    yoy_to   = date_to.replace(year=date_to.year - 1)
    rows = conn.execute("""
        SELECT date, occupancy_pct FROM daily_snapshot
        WHERE property_id=? AND date BETWEEN ? AND ?
    """, (prop_id, str(yoy_from), str(yoy_to))).fetchall()
    # Shift dates forward 1 year for overlay
    result = {}
    for r in rows:
        try:
            d = date.fromisoformat(r["date"])
            shifted = d.replace(year=d.year + 1)
            result[str(shifted)] = r["occupancy_pct"]
        except ValueError:
            pass
    return result


# ── Calendar event management ─────────────────────────────────────────────────

def _render_event_manager(conn):
    st.markdown("**Calendar Events** — holidays and local events that affect demand")

    from db import get_db

    events = conn.execute("""
        SELECT id, date, name, event_type, impact, applies_to
        FROM calendar_events ORDER BY date
    """).fetchall()

    if events:
        df = pd.DataFrame([dict(e) for e in events])
        df["delete"] = False
        df["impact"] = df["impact"].astype(str)
        # Convert date string to actual date for DateColumn
        df["date"] = pd.to_datetime(df["date"]).dt.date

        edited = st.data_editor(
            df[["id","delete","date","name","event_type","impact","applies_to"]],
            column_config={
                "id":         st.column_config.NumberColumn("ID", disabled=True, width="small"),
                "delete":     st.column_config.CheckboxColumn("🗑 Del", width="small"),
                "date":       st.column_config.DateColumn("Date", format="YYYY-MM-DD", width="small"),
                "name":       st.column_config.TextColumn("Event Name"),
                "event_type": st.column_config.SelectboxColumn(
                    "Type",
                    options=["holiday","local_event","school_holiday","other"],
                    width="medium",
                ),
                "impact":     st.column_config.TextColumn("Multiplier", width="small",
                                  help="Numeric multiplier e.g. 1.2 = +20% demand"),
                "applies_to": st.column_config.TextColumn("Applies To", width="small",
                                  help="Property IDs comma-separated, or 'all'"),
            },
            use_container_width=True,
            hide_index=True,
            num_rows="fixed",
            key="event_editor",
        )

        if st.button("💾 Save Changes", type="primary"):
            to_delete = edited[edited["delete"] == True]["id"].tolist()
            to_update = edited[edited["delete"] == False]
            errors = []
            with get_db() as c:
                for row_id in to_delete:
                    c.execute("DELETE FROM calendar_events WHERE id=?", (int(row_id),))
                for _, row in to_update.iterrows():
                    # Validate multiplier is numeric
                    try:
                        float(row["impact"])
                    except (ValueError, TypeError):
                        errors.append(f"Row {int(row['id'])}: '{row['impact']}' is not a valid multiplier")
                        continue
                    c.execute("""
                        UPDATE calendar_events
                        SET date=?, name=?, event_type=?, impact=?, applies_to=?
                        WHERE id=?
                    """, (str(row["date"]), row["name"], row["event_type"],
                          str(row["impact"]), row["applies_to"], int(row["id"])))
            if errors:
                for e in errors:
                    st.error(e)
            else:
                n_del = len(to_delete)
                st.success(f"✓ Saved — {n_del} deleted" if n_del else "✓ Saved")
                st.rerun()
    else:
        st.caption("No events added yet.")

    st.divider()
    st.markdown("**Add New Event**")
    with st.form("add_event"):
        col1, col2 = st.columns(2)
        with col1:
            ev_date   = st.date_input("Date", value=date.today())
            ev_name   = st.text_input("Event Name", placeholder="e.g. Eid al-Fitr")
        with col2:
            ev_type   = st.selectbox("Type", ["holiday","local_event","school_holiday","other"])
            ev_impact = st.number_input(
                "Demand multiplier",
                min_value=0.5, max_value=5.0, value=1.2, step=0.1, format="%.1f",
                help="1.0 = no change · 1.2 = +20% · 2.0 = double demand"
            )
            ev_impact = str(round(ev_impact, 2))
        ev_applies = st.text_input("Applies to (property IDs comma-separated, or 'all')", value="all")
        if st.form_submit_button("➕ Add Event", type="primary"):
            if not ev_name.strip():
                st.error("Event name is required.")
            else:
                with get_db() as c:
                    c.execute("""
                        INSERT INTO calendar_events (date, name, event_type, impact, applies_to)
                        VALUES (?,?,?,?,?)
                    """, (str(ev_date), ev_name.strip(), ev_type, ev_impact, ev_applies.strip()))
                st.success(f"✓ '{ev_name}' added.")
                st.rerun()


# ── Main render ───────────────────────────────────────────────────────────────

def render():
    st.subheader("Forecasting")

    props = sorted(get_active_properties(), key=lambda p: str(p["id"]))
    if not props:
        st.warning("No active properties. Add properties in 🔧 Settings.")
        return

    prop_options = {p["id"]: p["name"] for p in props}
    today = date.today()

    selected_id = st.selectbox(
        "Property",
        options=list(prop_options.keys()),
        format_func=lambda k: prop_options[k],
    )
    prop = next(p for p in props if p["id"] == selected_id)

    tab_forecast, tab_pace, tab_events = st.tabs([
        "Statistical Forecast", "Pickup Pace", "Calendar Events"
    ])

    # ── Tab 1: Statistical Forecast ───────────────────────────────────────────
    with tab_forecast:
        st.caption("BOB-anchored where holdings exist, DOW historical average elsewhere. Uses all available history.")

        col1, col2 = st.columns(2)
        with col1:
            fc_from = st.date_input("From", value=today, key="fc_from")
        with col2:
            fc_to = st.date_input("To", value=today + timedelta(days=30), key="fc_to")

        conn = get_connection()
        hist_row = conn.execute(
            "SELECT MIN(date) as earliest, COUNT(*) as n FROM daily_snapshot WHERE property_id=?",
            (selected_id,)
        ).fetchone()
        earliest = hist_row["earliest"] if hist_row else None

        if not earliest:
            conn.close()
            st.warning("No historical data available for forecasting. Ingest at least a few weeks of data first.")
        else:
            st.caption(f"Historical data from: **{earliest}** ({hist_row['n']} days)")

            df_fc = _build_forecast(conn, selected_id, prop, fc_from, fc_to)
            conn.close()

            if df_fc.empty:
                st.info("No forecast data generated.")
            else:
                actual_days = (df_fc["source"] == "actual").sum()
                bob_days    = (df_fc["source"] == "bob").sum()
                stat_days   = (df_fc["source"] == "stat").sum()
                st.caption(f"Actual {actual_days}d · BOB {bob_days}d · Stat {stat_days}d")

                # 5 KPI cards — forecast totals/averages
                c1, c2, c3, c4, c5 = st.columns(5)
                room_count        = prop.get("room_count") or 1
                total_rooms_avail = room_count * len(df_fc)
                fcst_rooms  = df_fc["forecast_rooms"].sum()
                fcst_occ    = (fcst_rooms / total_rooms_avail * 100) if total_rooms_avail else 0
                fcst_adr    = df_fc.loc[df_fc["forecast_rooms"] > 0, "forecast_adr"].mean() if (df_fc["forecast_rooms"] > 0).any() else 0
                fcst_revpar = fcst_adr * fcst_occ / 100
                fcst_rev    = df_fc["forecast_revenue"].sum()
                c1.metric("Fcst Rooms",   f"{int(fcst_rooms):,}")
                c2.metric("Fcst Occ %",   f"{fcst_occ:.1f}%")
                c3.metric("Fcst ADR",     _fmt_idr(fcst_adr))
                c4.metric("Fcst RevPAR",  _fmt_idr(fcst_revpar))
                c5.metric("Fcst Revenue", _fmt_idr(fcst_rev))

                # Separate series by source for distinct colors
                actual_df = df_fc[df_fc["source"] == "actual"]
                bob_df    = df_fc[df_fc["source"] == "bob"]
                future_df = df_fc[df_fc["source"] != "actual"]

                fig = go.Figure()
                # Past actuals — solid blue bars with labels
                if not actual_df.empty:
                    fig.add_trace(go.Bar(
                        x=actual_df["date"], y=actual_df["bob_occ"],
                        name="Actual Occ %", opacity=0.85,
                        marker_color="#6C63FF",
                        text=actual_df["bob_occ"].map(lambda v: f"{v:.1f}"),
                        textposition="outside", textfont=dict(size=10),
                    ))
                # Future BOB holdings — lighter purple bars with labels
                if not bob_df.empty:
                    fig.add_trace(go.Bar(
                        x=bob_df["date"], y=bob_df["bob_occ"],
                        name="BOB (Holdings %)", opacity=0.7,
                        marker_color="#C4B5FD",
                        text=bob_df["bob_occ"].map(lambda v: f"{v:.1f}"),
                        textposition="outside", textfont=dict(size=10),
                    ))
                # Forecast line — only for future dates
                if not future_df.empty:
                    fig.add_trace(go.Scatter(
                        x=future_df["date"], y=future_df["forecast_occ"],
                        name="Forecast Occ %",
                        mode="lines+markers",
                        line=dict(dash="dash", width=2, color="#FF9800"),
                        marker=dict(size=4),
                    ))
                # Event markers — show on both actual and forecast dates, clamped to chart range
                events_df = df_fc[df_fc["event"].notna()].copy()
                if not events_df.empty:
                    # Y position: use the bar/line value for that date, clamped to 105
                    events_df["marker_y"] = events_df.apply(
                        lambda r: min(r["bob_occ"] if r["source"] == "actual" else r["forecast_occ"], 105),
                        axis=1
                    )
                    fig.add_trace(go.Scatter(
                        x=events_df["date"], y=events_df["marker_y"],
                        mode="markers", marker=dict(symbol="star", size=14, color="#E91E63"),
                        name="Event",
                        text=events_df["event"],
                        hovertemplate="%{x}<br>%{text}<extra></extra>",
                    ))
                fig.update_layout(
                    height=380, margin=dict(l=0,r=0,t=30,b=0),
                    yaxis=dict(range=[0, 115], title="Occupancy %"),
                    barmode="overlay", hovermode="x unified",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                st.plotly_chart(fig, use_container_width=True)

                # Pickup d-1: bookings created YESTERDAY, grouped by check_in (arrival date)
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
                """, (selected_id, yesterday_str, str(fc_from), str(fc_to))).fetchall()
                d1_map = {r["arrival_date"]: r["rooms_picked_up"] for r in d1_rows}

                # Detail table — all rows, requested column order
                display = df_fc[["date","dow",
                                  "bob_rooms","bob_occ","bob_adr","bob_revenue",
                                  "forecast_rooms","forecast_occ","forecast_adr","forecast_revpar","forecast_revenue",
                                  "event"]].copy()
                display.columns = ["Date","DOW",
                                   "BOB Rooms","BOB Occ%","BOB ADR","BOB Revenue",
                                   "Fcst Rooms","Fcst Occ%","Fcst ADR","Fcst RevPAR","Fcst Revenue",
                                   "Event"]
                display["Pickup d-1"] = display["Date"].map(lambda d: f"+{d1_map[d]}" if d1_map.get(d, 0) > 0 else str(d1_map.get(d, 0)))
                display["BOB Occ%"]      = display["BOB Occ%"].map(lambda v: f"{v:.1f}%")
                display["BOB ADR"]       = display["BOB ADR"].map(_fmt_idr)
                display["BOB Revenue"]   = display["BOB Revenue"].map(_fmt_idr)
                display["Fcst Occ%"]     = display["Fcst Occ%"].map(lambda v: f"{v:.1f}%")
                display["Fcst ADR"]      = display["Fcst ADR"].map(_fmt_idr)
                display["Fcst RevPAR"]   = display["Fcst RevPAR"].map(_fmt_idr)
                display["Fcst Revenue"]  = display["Fcst Revenue"].map(_fmt_idr)
                display["Event"]         = display["Event"].fillna("—")
                display = display[["Date","DOW",
                                   "BOB Rooms","BOB Occ%","BOB ADR","BOB Revenue",
                                   "Fcst Rooms","Fcst Occ%","Fcst ADR","Fcst RevPAR","Fcst Revenue",
                                   "Pickup d-1","Event"]]
                st.dataframe(display, use_container_width=True, hide_index=True,
                             height=min(35 * len(display) + 38, 600))

    # ── Tab 2: Pickup Pace ────────────────────────────────────────────────────
    with tab_pace:
        st.caption("BOB this year vs actual occupancy same period last year.")

        col1, col2 = st.columns(2)
        with col1:
            pace_from = st.date_input("From", value=today, key="pace_from")
        with col2:
            pace_to = st.date_input("To", value=today + timedelta(days=30), key="pace_to")

        conn = get_connection()
        bob_series = get_bob_series(conn, selected_id, pace_from, pace_to)
        yoy_actual = _get_yoy_bob(conn, selected_id, pace_from, pace_to)
        conn.close()

        if not bob_series and not yoy_actual:
            st.info("No BOB or historical data for pace comparison.")
        else:
            room_count = prop.get("room_count") or 1
            all_dates  = sorted(set(list(bob_series.keys()) + list(yoy_actual.keys())))

            bob_pct = [bob_series.get(d, 0) / room_count * 100 for d in all_dates]
            yoy_pct = [yoy_actual.get(d) for d in all_dates]

            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=all_dates, y=bob_pct,
                name="BOB This Year (Confirmed)",
                mode="lines+markers", line=dict(width=2),
            ))
            fig.add_trace(go.Scatter(
                x=all_dates, y=yoy_pct,
                name="Actual Last Year",
                mode="lines", line=dict(dash="dot", width=2),
            ))
            fig.update_layout(
                height=340, margin=dict(l=0,r=0,t=20,b=0),
                yaxis=dict(range=[0,110], title="Occupancy %"),
                hovermode="x unified",
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            st.plotly_chart(fig, use_container_width=True)

            # Pace summary
            bob_total  = sum(bob_series.values())
            yoy_nights = sum(v for v in yoy_actual.values() if v)
            yoy_avg    = (yoy_nights / len([v for v in yoy_actual.values() if v])
                         if yoy_actual else None)
            bob_avg    = (bob_total / len(bob_series)) if bob_series else 0

            c1, c2, c3 = st.columns(3)
            c1.metric("BOB Room Nights (confirmed)", f"{bob_total:,}")
            c2.metric("Avg BOB Occupancy %", f"{bob_avg/room_count*100:.1f}%")
            if yoy_avg:
                c3.metric("Last Year Avg Occupancy %", f"{yoy_avg:.1f}%")

    # ── Tab 3: Calendar Events ────────────────────────────────────────────────
    with tab_events:
        conn = get_connection()
        _render_event_manager(conn)
        conn.close()
