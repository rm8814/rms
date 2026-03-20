"""
scheduler.py — Background ingestion + digest scheduler

Runs inside the Streamlit process as a daemon thread (or standalone via __main__).
Call start_scheduler() once from app.py.

Jobs:
  ┌─────────────────────────────────────────────────────────────┐
  │ main_ingest  IntervalTrigger(5 min)                          │
  │   → _run_ingest() → ingest_services + ingest_bookings        │
  │   → all active properties                                    │
  ├─────────────────────────────────────────────────────────────┤
  │ owner_digest  CronTrigger(Sun 19:00 Asia/Jakarta)            │
  │   → send_owner_digests()                                     │
  │   → notifications WHERE audience='owner' AND enabled=1       │
  │   → query_owner_digest → format_owner_message                │
  │   → Telegram Bot API → log_ingest (ingest_log)               │
  └─────────────────────────────────────────────────────────────┘
"""

import logging
import os
import threading
from datetime import date, datetime, timedelta

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from config import scheduler as scheduler_config, PropertyConfig
from db import init_db, migrate_db, get_active_properties, get_connection, log_ingest
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


def send_owner_digests():
    """
    APScheduler job — fires every Sunday 19:00 Asia/Jakarta.
    Sends the weekly owner digest to all enabled notification rows.
    Results are recorded in ingest_log (endpoint='telegram_digest').
    """
    from notifications.digest_builder import query_owner_digest, format_owner_message
    from notifications.telegram_client import send_message, SendResult

    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        log.error("[digest] TELEGRAM_BOT_TOKEN not set — skipping owner digest")
        return

    today = date.today()
    week_start = today - timedelta(days=6)   # Sunday − 6 = Monday of this week
    ran_at = datetime.utcnow().isoformat()

    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT id, property_id, chat_id, label FROM notifications "
            "WHERE audience='owner' AND enabled=1"
        ).fetchall()

        for row in rows:
            try:
                data = query_owner_digest(conn, row["property_id"], week_start)
                text = format_owner_message(data)
                result = send_message(token, row["chat_id"], text)

                if result in (SendResult.BAD_ID, SendResult.BLOCKED):
                    log.error(
                        f"[digest] disabling notification {row['id']} "
                        f"({row['label']}): {result.value}"
                    )
                    conn.execute(
                        "UPDATE notifications SET enabled=0 WHERE id=?", (row["id"],)
                    )
                    conn.commit()
                    log_ingest(
                        row["property_id"], "telegram_digest",
                        week_start, today, "error", 0,
                        f"chat_id disabled: {result.value}", ran_at,
                    )
                else:
                    status = "partial" if data["data_partial"] or data["data_missing"] else "ok"
                    log_ingest(
                        row["property_id"], "telegram_digest",
                        week_start, today, status, 1, None, ran_at,
                    )
                    log.info(
                        f"[digest] sent to {row['label']} ({row['property_id']}): {status}"
                    )

            except Exception as e:
                log.error(
                    f"[digest] failed for notification {row['id']} ({row['label']}): {e}"
                )
                log_ingest(
                    row["property_id"], "telegram_digest",
                    week_start, today, "error", 0, str(e), ran_at,
                )
    finally:
        conn.close()


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
        _scheduler.add_job(
            send_owner_digests,
            trigger=CronTrigger(
                day_of_week="sun", hour=19, minute=0,
                timezone=pytz.timezone("Asia/Jakarta"),
            ),
            id="owner_digest",
            replace_existing=True,
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
