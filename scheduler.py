"""
scheduler.py — Background ingestion scheduler

Runs inside the Streamlit process as a daemon thread.
Call start_scheduler() once from app.py.

On each tick:
  - For each active property, fetch last `lookback_days` of services
  - Fetch payments for same window
  - Fetch forward bookings (next 90 days) for BOB/pace
"""

import logging
import threading
from datetime import date, datetime, timedelta

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from config import scheduler as scheduler_config, PropertyConfig
from db import init_db, migrate_db, get_active_properties
from ingestion.services import ingest_services, rebuild_snapshots_from_raw
from ingestion.bookings import ingest_bookings

log = logging.getLogger(__name__)

_scheduler: BackgroundScheduler = None
_lock = threading.Lock()


def _run_ingest():
    """Single ingestion tick — runs for all active properties from DB.

    Fetches the full current month (month_start → today) using dateKind=1 (by service provision date).
    This ensures every occupied night in the month is always captured, matching the
    Exely YieldAndLoad report exactly. The 31-day API limit is handled by chunking.
    """
    today = date.today()
    month_start = today.replace(day=1)

    for prop_dict in get_active_properties():
        if not prop_dict.get("api_key"):
            log.warning(f"[{prop_dict['id']}] Skipping — no API key set")
            continue

        prop = PropertyConfig.from_db_row(prop_dict)

        effective_start = max(month_start, date.fromisoformat(prop.join_date))
        if effective_start > today:
            continue

        # dateKind=1 returns accruals whose service DATE falls within the window.
        # Extend back 30 days so long-stay guests who arrived before month_start
        # still have their room-night rows captured for each night in the current month.
        fetch_start = max(effective_start - timedelta(days=30), date.fromisoformat(prop.join_date))

        log.info(f"[{prop.id}] Ingesting services {fetch_start} → {today}")
        try:
            ingest_services(prop, fetch_start, today)
        except Exception as e:
            log.error(f"[{prop.id}] Services ingest failed: {e}", exc_info=True)

        try:
            ingest_bookings(prop)
        except Exception as e:
            log.error(f"[{prop.id}] Bookings ingest failed: {e}", exc_info=True)


def start_scheduler():
    """Call once from app.py. Idempotent — won't start a second scheduler."""
    global _scheduler
    with _lock:
        if _scheduler is not None and _scheduler.running:
            return

        _scheduler = BackgroundScheduler(daemon=True)
        _scheduler.add_job(
            _run_ingest,
            trigger=IntervalTrigger(minutes=scheduler_config.interval_minutes),
            id="main_ingest",
            next_run_time=datetime.now(),
            max_instances=1,
            coalesce=True,
        )
        _scheduler.start()
        log.info(f"Scheduler started — every {scheduler_config.interval_minutes} min")


def stop_scheduler():
    global _scheduler
    with _lock:
        if _scheduler and _scheduler.running:
            _scheduler.shutdown(wait=False)
            log.info("Scheduler stopped")


if __name__ == "__main__":
    import time
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    init_db()
    migrate_db()
    start_scheduler()
    log.info("Scheduler running standalone — press Ctrl+C to stop")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        stop_scheduler()
        log.info("Scheduler stopped")
