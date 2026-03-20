"""
db.py — SQLite schema and connection management

Design rules:
- raw_* tables store API responses as-is (append/upsert only, never delete)
- daily_snapshot is materialized from raw_services, rebuilt on ingestion
- budgets and costs are manual inputs, never touched by ingestion
- all monetary values in IDR (raw API may be in property currency — converted on ingest)
"""

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

_env_path = os.environ.get("REHAT_DB_PATH")
DB_PATH = Path(_env_path) if _env_path else Path(__file__).parent / "rehat.db"

DDL = """
-- ── Properties (source of truth — managed via Settings UI) ──────────────────
CREATE TABLE IF NOT EXISTS properties (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    city            TEXT,
    contract_type   TEXT NOT NULL,
    join_date       TEXT NOT NULL,   -- YYYY-MM-DD
    room_count      INTEGER NOT NULL,
    revshare_pct    REAL,
    revshare_gop_pct REAL,            -- only for revshare_revenue_gop contract
    lease_monthly   INTEGER,
    advance_total   INTEGER,
    contract_months INTEGER,         -- for advance_payment: amortization period in months
    active          INTEGER NOT NULL DEFAULT 1
);

-- ── Raw accruals from /analytics/services ────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_services (
    service_id      TEXT NOT NULL,   -- Exely's unique service id
    property_id     TEXT NOT NULL,
    date            TEXT NOT NULL,   -- YYYY-MM-DD service provision date
    reservation_id  INTEGER,
    booking_number  TEXT,
    kind            INTEGER,         -- 0=accommodation,1=extra,2=transfer,3=early_ci,4=late_co
    name            TEXT,
    amount          REAL,            -- after discount
    discount        REAL,
    quantity        INTEGER,
    currency        TEXT,
    currency_rate   REAL,
    amount_idr      REAL,            -- amount * currency_rate
    room_type_id    INTEGER,
    room_number     TEXT,
    guest_name      TEXT,
    check_in        TEXT,
    check_out       TEXT,
    is_arrived      INTEGER,
    is_departed     INTEGER,
    payment_method  INTEGER,
    booking_source  TEXT,
    market_code     TEXT,
    is_included     INTEGER,         -- 1 if extra is bundled into room rate (already counted in kind=0 amount)
    agent_name      TEXT,            -- resolved from agents array (e.g. 'ChannelManager: "agoda.com"')
    creation_date   TEXT,            -- booking creation date YYYY-MM-DD (from creationDateTime)
    fetched_at      TEXT NOT NULL,   -- when we pulled this
    PRIMARY KEY (service_id, property_id, date, kind)
);

-- ── Channel mappings (per-property OTA display names + direct/indirect) ───────
-- Populated automatically from seen agent_name values; user can override via Settings
CREATE TABLE IF NOT EXISTS channel_mappings (
    property_id     TEXT NOT NULL,
    raw_agent_name  TEXT NOT NULL,   -- as stored in raw_services.agent_name
    display_name    TEXT NOT NULL,   -- user-friendly label (e.g. "Agoda")
    channel_type    TEXT NOT NULL DEFAULT 'indirect',  -- 'direct' | 'indirect'
    updated_at      TEXT NOT NULL,
    PRIMARY KEY (property_id, raw_agent_name)
);

-- ── Raw payments from /analytics/payments ────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_payments (
    payment_id      INTEGER NOT NULL,
    property_id     TEXT NOT NULL,
    booking_number  TEXT,
    service_id      TEXT,
    action_kind     INTEGER,         -- 0=payment,1=refund,2=pay_cancel,3=refund_cancel
    payment_method  INTEGER,
    amount          REAL,
    currency        TEXT,
    currency_rate   REAL,
    amount_idr      REAL,
    datetime        TEXT,
    username        TEXT,
    fetched_at      TEXT NOT NULL,
    PRIMARY KEY (payment_id, property_id)
);

-- ── Raw reservations (from services response, reservation-level data) ─────────
CREATE TABLE IF NOT EXISTS raw_reservations (
    reservation_id      INTEGER NOT NULL,
    property_id         TEXT NOT NULL,
    booking_number      TEXT,
    room_number         TEXT,
    room_type_id        INTEGER,
    guest_id            TEXT,
    guest_name          TEXT,
    guest_count         INTEGER,
    check_in            TEXT,
    check_out           TEXT,
    is_arrived          INTEGER,
    is_departed         INTEGER,
    payment_method      INTEGER,
    booking_source      TEXT,
    market_code         TEXT,
    total               REAL,
    paid                REAL,
    balance             REAL,
    currency            TEXT,
    currency_rate       REAL,
    created_at          TEXT,
    fetched_at          TEXT NOT NULL,
    PRIMARY KEY (reservation_id, property_id)
);

-- ── Daily snapshot (materialized, rebuilt from raw_services on each ingest) ───
-- One row per property per date. This is what all dashboard modules query.
CREATE TABLE IF NOT EXISTS daily_snapshot (
    property_id         TEXT NOT NULL,
    date                TEXT NOT NULL,   -- YYYY-MM-DD
    rooms_sold          INTEGER,
    rooms_available     INTEGER,         -- room_count (from properties table)
    occupancy_pct       REAL,            -- rooms_sold / rooms_available * 100
    revenue_total       REAL,            -- all service kinds, IDR
    revenue_rooms       REAL,            -- kind=0 only, IDR
    revenue_extras      REAL,            -- kind=1,2,3,4, IDR
    adr                 REAL,            -- revenue_rooms / rooms_sold
    revpar              REAL,            -- revenue_rooms / rooms_available
    rehat_revenue       REAL,            -- REHAT's gross share (before REHAT's own expenses)
    bookings_count      INTEGER,         -- distinct booking_numbers
    updated_at          TEXT NOT NULL,
    PRIMARY KEY (property_id, date)
);

-- ── Forward bookings (pickup/pace) ────────────────────────────────────────────
-- Populated from /bookings search with affectsPeriodFrom/To for future dates
CREATE TABLE IF NOT EXISTS bookings_on_books (
    property_id     TEXT NOT NULL,
    stay_date       TEXT NOT NULL,   -- each night of a future reservation
    booking_number  TEXT NOT NULL,
    room_type_id    TEXT,
    check_in        TEXT,
    check_out       TEXT,
    status          TEXT,
    nightly_rate_idr REAL,           -- per-night room rate in IDR (from Exely roomStay)
    fetched_at      TEXT NOT NULL,
    PRIMARY KEY (property_id, stay_date, booking_number)
);

-- ── BOB daily snapshots — rooms-on-books per stay_date captured each day ─────
CREATE TABLE IF NOT EXISTS bob_snapshots (
    property_id  TEXT NOT NULL,
    capture_date TEXT NOT NULL,   -- date this snapshot was taken (today at fetch time)
    stay_date    TEXT NOT NULL,   -- the future stay night
    rooms        INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (property_id, capture_date, stay_date)
);
CREATE INDEX IF NOT EXISTS idx_bob_snap ON bob_snapshots(property_id, capture_date);

-- ── Budget (manual input) ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS budgets (
    property_id     TEXT NOT NULL,
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,   -- 1–12
    revenue_target  REAL NOT NULL,      -- IDR
    updated_at      TEXT NOT NULL,
    PRIMARY KEY (property_id, year, month)
);

-- ── Monthly costs (manual input, only meaningful for lease + advance_payment) ─
CREATE TABLE IF NOT EXISTS monthly_costs (
    property_id     TEXT NOT NULL,
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    category        TEXT NOT NULL,   -- rooms|fnb|sales_marketing|admin_general|maintenance|utilities|salary|others
    amount          REAL NOT NULL,   -- IDR
    updated_at      TEXT NOT NULL,
    PRIMARY KEY (property_id, year, month, category)
);


-- ── REHAT company expenses (system_fee auto-calculated; misc manual, company-wide) ──────
-- category: system_fee (auto, 2% of gross revenue) | misc (manual IDR, company-wide)
-- scope: property = per-property | company = company-wide (property_id is NULL)
CREATE TABLE IF NOT EXISTS rehat_expenses (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    category        TEXT NOT NULL,   -- system_fee | misc
    scope           TEXT NOT NULL,   -- property | company
    property_id     TEXT,            -- NULL for company-wide entries
    amount          REAL NOT NULL,   -- IDR
    note            TEXT,
    updated_at      TEXT NOT NULL
);

-- ── Calendar events (manual, affects forecasting) ────────────────────────────
CREATE TABLE IF NOT EXISTS calendar_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    date            TEXT NOT NULL,   -- YYYY-MM-DD
    name            TEXT NOT NULL,
    event_type      TEXT,            -- holiday|local_event|school_holiday|other
    impact          TEXT,            -- high|medium|low
    applies_to      TEXT DEFAULT 'all'   -- 'all' or comma-separated property_ids
);

-- ── Ingestion log ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ingest_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    property_id     TEXT NOT NULL,
    endpoint        TEXT NOT NULL,   -- services|payments|bookings
    date_from       TEXT,
    date_to         TEXT,
    status          TEXT,            -- ok|error
    rows_upserted   INTEGER,
    error_msg       TEXT,
    ran_at          TEXT NOT NULL
);
"""

INDEXES = """
CREATE INDEX IF NOT EXISTS idx_raw_services_prop_date   ON raw_services(property_id, date);
CREATE INDEX IF NOT EXISTS idx_raw_services_booking     ON raw_services(booking_number);
CREATE INDEX IF NOT EXISTS idx_raw_payments_prop_date   ON raw_payments(property_id, datetime);
CREATE INDEX IF NOT EXISTS idx_raw_reservations_prop    ON raw_reservations(property_id, check_in, check_out);
CREATE INDEX IF NOT EXISTS idx_snapshot_prop_date       ON daily_snapshot(property_id, date);
CREATE INDEX IF NOT EXISTS idx_bob_prop_date            ON bookings_on_books(property_id, stay_date);
"""


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")   # safe for concurrent reads
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def get_db():
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: Path = DB_PATH):
    """Create all tables and indexes. Safe to call on every startup."""
    conn = get_connection(db_path)
    conn.executescript(DDL)
    conn.executescript(INDEXES)
    conn.commit()
    conn.close()
    print(f"DB initialized: {db_path}")


def migrate_db(db_path: Path = DB_PATH):
    """Run safe migrations for schema changes on existing DBs."""
    conn = get_connection(db_path)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(properties)").fetchall()]
    if "api_key" not in cols:
        conn.execute("ALTER TABLE properties ADD COLUMN api_key TEXT")
        conn.commit()
        print("Migration: added api_key column to properties")
    if "revshare_gop_pct" not in cols:
        conn.execute("ALTER TABLE properties ADD COLUMN revshare_gop_pct REAL")
        conn.commit()
        print("Migration: added revshare_gop_pct column to properties")
    if "contract_months" not in cols:
        conn.execute("ALTER TABLE properties ADD COLUMN contract_months INTEGER")
        conn.commit()
        print("Migration: added contract_months column to properties")
    # raw_services / raw_reservations: add market_code_name for per-OTA channel mix
    svc_cols = [r[1] for r in conn.execute("PRAGMA table_info(raw_services)").fetchall()]
    if "market_code_name" not in svc_cols:
        conn.execute("ALTER TABLE raw_services ADD COLUMN market_code_name TEXT")
        conn.commit()
        print("Migration: added market_code_name to raw_services")
    res_cols = [r[1] for r in conn.execute("PRAGMA table_info(raw_reservations)").fetchall()]
    if "market_code_name" not in res_cols:
        conn.execute("ALTER TABLE raw_reservations ADD COLUMN market_code_name TEXT")
        conn.commit()
        print("Migration: added market_code_name to raw_reservations")
    if "agent_name" not in svc_cols:
        conn.execute("ALTER TABLE raw_services ADD COLUMN agent_name TEXT")
        conn.commit()
        print("Migration: added agent_name to raw_services")
    if "creation_date" not in svc_cols:
        conn.execute("ALTER TABLE raw_services ADD COLUMN creation_date TEXT")
        conn.commit()
        print("Migration: added creation_date to raw_services")
    if "is_included" not in svc_cols:
        conn.execute("ALTER TABLE raw_services ADD COLUMN is_included INTEGER")
        conn.commit()
        print("Migration: added is_included to raw_services")
    try:
        conn.execute("ALTER TABLE bookings_on_books ADD COLUMN nightly_rate_idr REAL")
        conn.commit()
        print("Migration: added nightly_rate_idr to bookings_on_books")
    except Exception:
        pass  # column already exists

    # channel_mappings — new table, safe to CREATE IF NOT EXISTS
    conn.execute("""
        CREATE TABLE IF NOT EXISTS channel_mappings (
            property_id     TEXT NOT NULL,
            raw_agent_name  TEXT NOT NULL,
            display_name    TEXT NOT NULL,
            channel_type    TEXT NOT NULL DEFAULT 'indirect',
            updated_at      TEXT NOT NULL,
            PRIMARY KEY (property_id, raw_agent_name)
        )
    """)
    conn.commit()

    # Fix raw_services PRIMARY KEY — was (service_id, property_id), must be (service_id, property_id, date, kind).
    # Per API spec, the same service_id is reused for every nightly row of the same stay,
    # and also shared across kind=0/3/4 for the same room. The old PK collapsed all nights
    # of a stay into a single row, causing severe undercounting of rooms_sold and revenue.
    # SQLite can't ALTER a PK, so we recreate the table.
    pk_cols = [r[0] for r in conn.execute(
        "SELECT name FROM pragma_table_info('raw_services') WHERE pk > 0 ORDER BY pk"
    ).fetchall()]
    if pk_cols != ['service_id', 'property_id', 'date', 'kind']:
        print("Migration: rebuilding raw_services with correct PRIMARY KEY (service_id, property_id, date, kind)")
        print("WARNING: existing raw_services data was stored with wrong PK and is unreliable.")
        print("         All properties must be purged and reingested after this migration.")
        conn.executescript("""
            ALTER TABLE raw_services RENAME TO raw_services_old;
            CREATE TABLE raw_services (
                service_id      TEXT NOT NULL,
                property_id     TEXT NOT NULL,
                date            TEXT NOT NULL,
                reservation_id  INTEGER,
                booking_number  TEXT,
                kind            INTEGER,
                name            TEXT,
                amount          REAL,
                discount        REAL,
                quantity        INTEGER,
                currency        TEXT,
                currency_rate   REAL,
                amount_idr      REAL,
                room_type_id    INTEGER,
                room_number     TEXT,
                guest_name      TEXT,
                check_in        TEXT,
                check_out       TEXT,
                is_arrived      INTEGER,
                is_departed     INTEGER,
                payment_method  INTEGER,
                booking_source  TEXT,
                market_code     TEXT,
                market_code_name TEXT,
                is_included     INTEGER,
                agent_name      TEXT,
                creation_date   TEXT,
                fetched_at      TEXT NOT NULL,
                PRIMARY KEY (service_id, property_id, date, kind)
            );
            DROP TABLE raw_services_old;
            DELETE FROM daily_snapshot;
        """)
        conn.commit()
        print("Migration: raw_services rebuilt. daily_snapshot cleared — reingest required.")

    conn.close()


def sync_properties_to_db(config):
    """Legacy seed helper — no longer used. Kept for backwards compatibility."""
    with get_db() as conn:
        for p in config.properties:
            conn.execute("""
                INSERT OR IGNORE INTO properties
                    (id, name, city, contract_type, join_date, room_count,
                     revshare_pct, lease_monthly, advance_total, api_key, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (p.id, p.name, p.city, p.contract_type, p.join_date, p.room_count,
                  p.revshare_pct, p.lease_monthly, p.advance_total,
                  getattr(p, "api_key", None), int(p.active)))


def get_active_properties(conn=None) -> list[dict]:
    """Return all active properties from DB as list of dicts. Use instead of config.active_properties."""
    close_after = conn is None
    if conn is None:
        conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM properties WHERE active=1 ORDER BY name"
    ).fetchall()
    result = [dict(r) for r in rows]
    if close_after:
        conn.close()
    return result


def get_all_properties(conn=None) -> list[dict]:
    """Return all properties (active + inactive) from DB."""
    close_after = conn is None
    if conn is None:
        conn = get_connection()
    rows = conn.execute("SELECT * FROM properties ORDER BY name").fetchall()
    result = [dict(r) for r in rows]
    if close_after:
        conn.close()
    return result


if __name__ == "__main__":
    init_db()
    migrate_db()
    from config import config
    sync_properties_to_db(config)
    print("Properties seeded.")

    conn = get_connection()
    rows = conn.execute("SELECT name, type FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
    print(f"\nTables ({len(rows)}):")
    for r in rows:
        print(f"  {r['name']}")
    props = get_active_properties(conn)
    print(f"\nActive properties: {len(props)}")
    # rehat_expenses table
    conn.execute(
        "CREATE TABLE IF NOT EXISTS rehat_expenses ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "year INTEGER NOT NULL, month INTEGER NOT NULL, "
        "category TEXT NOT NULL, scope TEXT NOT NULL, "
        "property_id TEXT, amount REAL NOT NULL, note TEXT, updated_at TEXT NOT NULL)"
    )
    conn.commit()

    conn.close()
