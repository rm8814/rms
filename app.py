"""
app.py — REHAT Command Center
Streamlit entry point. Starts background scheduler, renders nav + active module.
"""

import streamlit as st
import logging
from datetime import date, timedelta

# ── Page config (must be first Streamlit call) ────────────────────────────────
st.set_page_config(
    page_title="REHAT Command Center",
    page_icon="🏨",
    layout="wide",
    initial_sidebar_state="expanded",
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

# ── Auth (simple password gate for internal access) ───────────────────────────
import os
_APP_PASSWORD = os.environ.get("REHAT_PASSWORD", "rehat2026")

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    st.markdown("## 🏨 REHAT Command Center")
    pw = st.text_input("Password", type="password", key="login_pw")
    if st.button("Login", type="primary"):
        if pw == _APP_PASSWORD:
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("Incorrect password")
    st.stop()

# ── Ensure DB is ready before anything else ───────────────────────────────────
from db import init_db, migrate_db, get_connection, get_db
init_db()
migrate_db()

# ── Start scheduler once per process ─────────────────────────────────────────
if "scheduler_started" not in st.session_state:
    from scheduler import start_scheduler
    start_scheduler()
    st.session_state.scheduler_started = True

from config import scheduler as scheduler_config

# ── Styling ───────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500;600&display=swap');

    html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
    .mono                      { font-family: 'Space Mono', monospace; }
    .block-container           { padding-top: 1.5rem; }

    /* Sidebar nav buttons — left-aligned text */
    [data-testid="stSidebar"] .stButton > button {
        font-family: 'DM Sans', sans-serif;
        font-weight: 500;
        text-align: left !important;
        justify-content: flex-start !important;
        padding-left: 0.75rem;
    }
</style>
""", unsafe_allow_html=True)


# ── System Status renderer (defined before routing) ───────────────────────────
def render_system_status():
    st.subheader("System Status")

    conn = get_connection()

    from db import get_active_properties
    active_props = get_active_properties()

    st.markdown("**Active Properties**")
    cols = st.columns(min(len(active_props), 3)) if active_props else [st.container()]
    for i, p in enumerate(active_props):
        last = conn.execute("""
            SELECT MAX(date) as last_date, COUNT(*) as rows
            FROM raw_services WHERE property_id=?
        """, (p["id"],)).fetchone()
        last_date = last["last_date"] or "—"
        rows = last["rows"] or 0
        with cols[i % 3]:
            status_icon = "✅" if rows > 0 else "⚠️"
            st.markdown(f"**{p['name']}**")
            st.caption(f"{p['contract_type']} · {p['room_count']} rooms · joined {p['join_date']}")
            st.caption(f"Last data: **{last_date}** · {status_icon} {rows:,} service rows")
            st.divider()

    # Ingestion log
    st.markdown("---")
    st.markdown("**Ingestion Log**")
    logs = conn.execute("""
        SELECT property_id, endpoint, date_from, date_to, status,
               rows_upserted, error_msg, ran_at
        FROM ingest_log ORDER BY ran_at DESC LIMIT 50
    """).fetchall()

    # Check if schema was just migrated (daily_snapshot empty but ingest_log has prior runs)
    snap_count = conn.execute("SELECT COUNT(*) as n FROM daily_snapshot").fetchone()["n"]
    log_count  = conn.execute("SELECT COUNT(*) as n FROM ingest_log").fetchone()["n"]
    if snap_count == 0 and log_count > 0:
        st.error(
            "⚠️ **Schema migration applied: raw_services PRIMARY KEY was corrected.** "
            "All historical data has been cleared because it was stored incorrectly "
            "(multi-night stays were collapsed to a single row, causing wrong room counts and revenue). "
            "**Use 'Purge Raw Data & Reingest' below to re-fetch correct data from Exely.**"
        )

    if not logs:
        st.warning("No ingestion runs yet. Scheduler fires within 5 minutes, or trigger manually below.")
    else:
        import pandas as pd
        from datetime import datetime, timedelta
        from config import to_wib_str
        df = pd.DataFrame([dict(r) for r in logs])
        df["ran_at"] = df["ran_at"].map(to_wib_str)
        st.dataframe(df, width="stretch", hide_index=True, height=400)

    # DB row counts
    st.markdown("---")
    st.markdown("**Database Row Counts**")
    tables = ["raw_services", "raw_payments", "raw_reservations",
              "daily_snapshot", "bookings_on_books", "budgets", "monthly_costs"]
    import pandas as pd
    counts = {t: conn.execute(f"SELECT COUNT(*) as n FROM {t}").fetchone()["n"] for t in tables}
    st.dataframe(
        pd.DataFrame(list(counts.items()), columns=["Table", "Rows"]),
        width="stretch", hide_index=True,
    )

    conn.close()

    # Manual trigger
    st.markdown("---")
    st.markdown("**Manual Ingest Trigger**")
    col1, col2 = st.columns(2)
    with col1:
        trigger_start = st.date_input("From", value=date.today() - timedelta(days=7))
    with col2:
        trigger_end = st.date_input("To", value=date.today())

    selected_props = st.multiselect(
        "Properties (empty = all active)",
        options=[p["id"] for p in active_props],
        format_func=lambda pid: next(p["name"] for p in active_props if p["id"] == pid),
    )

    if st.button("▶ Run Ingest Now", type="primary"):
        from ingestion.services import ingest_services
        from config import PropertyConfig
        props_to_run = (
            [p for p in active_props if p["id"] in selected_props]
            if selected_props else active_props
        )
        progress = st.progress(0)
        for i, prop_dict in enumerate(props_to_run):
            prop = PropertyConfig.from_db_row(prop_dict)
            with st.spinner(f"Ingesting {prop.name}..."):
                try:
                    ingest_services(prop, trigger_start, trigger_end)
                    st.success(f"✓ {prop.name}")
                except Exception as e:
                    st.error(f"✗ {prop.name}: {e}")
            progress.progress((i + 1) / len(props_to_run))
        st.rerun()

    st.markdown("**Rebuild Snapshot from Existing Raw Data**")
    st.caption("Use this to fill gaps in daily_snapshot without re-fetching from Exely. Rebuilds from raw_services already in DB.")
    if st.button("🔄 Rebuild Snapshot (full date range above)"):
        from ingestion.services import rebuild_snapshots_from_raw
        from config import PropertyConfig
        props_to_run = (
            [p for p in active_props if p["id"] in selected_props]
            if selected_props else active_props
        )
        progress = st.progress(0)
        for i, prop_dict in enumerate(props_to_run):
            prop = PropertyConfig.from_db_row(prop_dict)
            with st.spinner(f"Rebuilding {prop.name}..."):
                try:
                    n = rebuild_snapshots_from_raw(prop, trigger_start, trigger_end)
                    st.success(f"✓ {prop.name} — {n} dates rebuilt")
                except Exception as e:
                    st.error(f"✗ {prop.name}: {e}")
            progress.progress((i + 1) / len(props_to_run))
        st.rerun()

    st.markdown("**Purge & Reingest**")
    st.caption("⚠️ Deletes ALL raw_services and daily_snapshot for the selected properties+date range, then re-fetches fresh from Exely. Use when numbers are stuck wrong.")
    if st.button("🗑 Purge Raw Data & Reingest", type="secondary"):
        from ingestion.services import ingest_services
        from config import PropertyConfig
        props_to_run = (
            [p for p in active_props if p["id"] in selected_props]
            if selected_props else active_props
        )
        progress = st.progress(0)
        for i, prop_dict in enumerate(props_to_run):
            prop = PropertyConfig.from_db_row(prop_dict)
            with st.spinner(f"Purging {prop.name}..."):
                try:
                    with get_db() as conn:
                        conn.execute(
                            "DELETE FROM raw_services WHERE property_id=? AND date BETWEEN ? AND ?",
                            (prop.id, str(trigger_start), str(trigger_end))
                        )
                        conn.execute(
                            "DELETE FROM daily_snapshot WHERE property_id=? AND date BETWEEN ? AND ?",
                            (prop.id, str(trigger_start), str(trigger_end))
                        )
                    ingest_services(prop, trigger_start, trigger_end)
                    st.success(f"✓ {prop.name} — purged and reingested")
                except Exception as e:
                    st.error(f"✗ {prop.name}: {e}")
            progress.progress((i + 1) / len(props_to_run))
        st.rerun()


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("<span class='mono' style='font-size:18px; font-weight:700;'>REHAT</span>", unsafe_allow_html=True)
    st.caption("Command Center")

    st.divider()

    MODULES = {
        "📊  Portfolio Analytics":  "portfolio",
        "🏨  Property Details":     "property_kpis",
        "🎯  Budgeting":            "budgeting",
        "💰  Profit & Loss":        "pnl",
        "🏦  Company Financials":   "company_financials",
        "🔍  Acquisition":          "acquisition",
        "🔧  Property Config":      "settings",
        "⚙️  System Status":        "system_status",
    }

    if "active_module" not in st.session_state:
        st.session_state.active_module = "portfolio"

    for label, key in MODULES.items():
        is_active = st.session_state.active_module == key
        if st.button(label, key=f"nav_{key}", width="stretch",
                     type="primary" if is_active else "secondary"):
            st.session_state.active_module = key
            st.rerun()

    st.divider()

    st.caption("Last Sync")
    try:
        conn = get_connection()
        last_log = conn.execute(
            "SELECT property_id, status, ran_at FROM ingest_log ORDER BY ran_at DESC LIMIT 1"
        ).fetchone()
        conn.close()
        if last_log:
            from config import to_wib_str
            ran_at = to_wib_str(last_log["ran_at"])
            icon = "✅" if last_log["status"] == "ok" else "❌"
            st.caption(f"{icon} {ran_at} · {last_log['property_id']}")
        else:
            st.caption("⏳ Pending first run")
    except Exception:
        st.caption("❌ DB error")

    from db import get_active_properties
    prop_count = len(get_active_properties())
    st.caption(f"{prop_count} properties · {scheduler_config.interval_minutes} min refresh")



# ── Route ─────────────────────────────────────────────────────────────────────
module = st.session_state.active_module

if module == "system_status":
    render_system_status()

elif module == "property_kpis":
    try:
        from modules.property_kpis import render
        render()
    except ImportError:
        st.info("Property KPIs module — coming next.")

elif module == "portfolio":
    from modules.portfolio import render as render_portfolio
    render_portfolio()

elif module == "budgeting":
    from modules.budgeting import render as render_budgeting
    render_budgeting()

elif module == "pnl":
    from modules.pnl import render as render_pnl
    render_pnl()

elif module == "company_financials":
    from modules.company_financials import render as render_cf
    render_cf()

elif module == "acquisition":
    from modules.acquisition import render as render_acquisition
    render_acquisition()

elif module == "settings":
    from modules.settings import render as render_settings
    render_settings()
