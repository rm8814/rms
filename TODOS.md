# TODOS

## Migration version tracking
Add a `schema_migrations` table to `db.py` so each migration runs exactly once
and is permanently recorded. Currently `migrate_db()` re-checks ~10 conditions on
every startup with no audit trail. Tipping point is ~15-20 migrations.

**Why:** Prevents destructive migrations from re-running; provides audit log; makes
`migrate_db()` readable as it grows.

**Depends on:** The safety guard on the destructive raw_services migration (already done).

**How to start:** Add `CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, ran_at TEXT)`.
Convert each existing `if col not in cols:` block to a versioned migration entry.

---

## APScheduler job persistence for digest reliability
Add `SQLAlchemyJobStore` to the `BackgroundScheduler` so the Sunday digest
(and ingest) jobs survive process restarts and missed fires auto-retry on startup.

**Why:** At scale (10+ properties, management contracts), a missed owner digest
is a trust problem — exactly what the digest feature is meant to prevent. Current
`BackgroundScheduler(daemon=True)` drops all in-flight jobs on restart with no retry.

**Pros:** Zero missed digests on deploy day; automatic recovery; no manual intervention.
**Cons:** Adds SQLAlchemy as a direct dependency; mild scheduler setup complexity.

**How to start:** Replace `BackgroundScheduler(daemon=True)` in `scheduler.py:76`
with `BackgroundScheduler(jobstores={"default": SQLAlchemyJobStore(url="sqlite:///...")}, daemon=True)`.
Use the same `REHAT_DB_PATH` env var for the store URL.

**Trigger:** After Phase 1 pilot proves digest value. Not worth before.

**Depends on:** Nothing (but do Phase 1 pilot first).

---

## GM daily digest (Phase 2)
Implement the hotel GM daily digest: `query_gm_digest`, `format_gm_message`,
daily `CronTrigger` job at 07:00 Asia/Jakarta, and a full test suite.

**Why:** GMs need daily occupancy/ADR/pace visibility to make same-day pricing
decisions. The `notifications` table already supports `audience='gm'` rows.

**Pros:** Completes the external reporting layer; serves a different audience with
different needs than the weekly owner digest.
**Cons:** Requires defining and validating the GM message format independently.

**Context:** GM content (from design doc): yesterday's revenue + occupancy, MTD
pace vs budget, rooms on books for tonight and next 7 days. No anomaly flagging
yet — too complex for Phase 2. Structure mirrors `query_owner_digest` and
`format_owner_message` — low implementation risk.

**How to start:** Add `query_gm_digest(conn, prop_id, report_date)` to
`notifications/digest_builder.py`. Add `send_gm_digests()` to `scheduler.py`.
Register `CronTrigger(hour=7, minute=0, timezone="Asia/Jakarta")`.

**Depends on / blocked by:** Phase 1 owner digest pilot (validate format + delivery
first, then extend to GMs).

---

## Extract DOW_METRICS to shared module
`DOW_METRICS` dict (metric key → label + format function) is copy-pasted
identically in `modules/portfolio.py:597` and `modules/property_kpis.py:635`.

**Why:** Label drift — if someone renames "Avg Occ %" in one file, the other
silently shows the old label. Also duplication will grow when GM digest or
other modules need the same format functions.

**Pros:** Single source of truth for metric labels; easy to add new metrics.
**Cons:** Adds an import dependency between modules — minor.

**How to start:** Add `DOW_METRICS` to `modules/shared.py` (create if needed)
or to `config.py`. Import it in both `portfolio.py` and `property_kpis.py`.

**Priority:** Low — only relevant when a label changes or a third module needs it.

---

## Test coverage for send_owner_digests()
Add `tests/test_send_owner_digests.py` covering: happy path (digest sent, log_ingest ok),
auto-disable path (BAD_ID/BLOCKED → UPDATE enabled=0), missing TELEGRAM_BOT_TOKEN
early return, and per-row exception isolation (one bad row doesn't stop others).

**Why:** The auto-disable logic is silent production behavior — if it broke,
owners would stop receiving digests with no alert.

**How to start:** Mock `get_connection`, `query_owner_digest`, `format_owner_message`,
`send_message`, and `log_ingest`. Use an in-memory SQLite `notifications` table.

**Priority:** Medium — important before scaling to 5+ properties.

---

## Batch snapshot rebuild into one transaction
Refactor `ingestion/services.py:_rebuild_snapshot` to rebuild all affected dates
in a single DB transaction rather than one connection open/commit per date.

**Why:** Eliminates the stale-snapshot window between raw_services write and
daily_snapshot update. Also faster at scale (100+ round-trips at 5+ properties).

**Trigger:** When property count exceeds ~10 or ingest latency becomes noticeable.

**How to start:** Change `_rebuild_snapshot(prop, date_str)` signature to accept
`conn` as an optional parameter, or add a `_rebuild_snapshots_batch(prop, dates, conn)`
variant. Call from `ingest_services` within the same `get_db()` context as the upserts.
