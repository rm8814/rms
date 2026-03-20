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

## Batch snapshot rebuild into one transaction
Refactor `ingestion/services.py:_rebuild_snapshot` to rebuild all affected dates
in a single DB transaction rather than one connection open/commit per date.

**Why:** Eliminates the stale-snapshot window between raw_services write and
daily_snapshot update. Also faster at scale (100+ round-trips at 5+ properties).

**Trigger:** When property count exceeds ~10 or ingest latency becomes noticeable.

**How to start:** Change `_rebuild_snapshot(prop, date_str)` signature to accept
`conn` as an optional parameter, or add a `_rebuild_snapshots_batch(prop, dates, conn)`
variant. Call from `ingest_services` within the same `get_db()` context as the upserts.
