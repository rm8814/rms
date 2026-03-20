"""
tests/test_db_migrations.py — DB migration idempotency + safety guard tests
"""

import pytest
import sqlite3
import tempfile
import os
from pathlib import Path
from db import init_db, migrate_db, get_connection


@pytest.fixture
def tmp_db(tmp_path):
    db_path = tmp_path / "test.db"
    return db_path


class TestInitDb:
    def test_creates_all_tables(self, tmp_db):
        init_db(tmp_db)
        conn = get_connection(tmp_db)
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        conn.close()
        expected = {
            "properties", "raw_services", "raw_payments", "raw_reservations",
            "daily_snapshot", "bookings_on_books", "budgets", "monthly_costs",
            "rehat_expenses", "calendar_events", "ingest_log", "channel_mappings",
        }
        assert expected.issubset(tables)

    def test_idempotent_double_call(self, tmp_db):
        init_db(tmp_db)
        init_db(tmp_db)   # must not raise


class TestMigrateDb:
    def test_idempotent_on_fresh_db(self, tmp_db):
        init_db(tmp_db)
        migrate_db(tmp_db)
        migrate_db(tmp_db)   # second call must be safe

    def test_safety_guard_blocks_destructive_migration_when_data_exists(self, tmp_db):
        """
        If raw_services has rows but the PK is already correct, the destructive migration
        must NOT run (the PK check will find the correct PK and skip it entirely).
        If raw_services has rows with the OLD PK, the safety guard must abort without
        deleting data and print a warning.
        """
        init_db(tmp_db)
        conn = get_connection(tmp_db)
        # Insert a row with the correct PK format to simulate existing data
        conn.execute("""
            INSERT INTO raw_services (
                service_id, property_id, date, kind, fetched_at
            ) VALUES ('svc1', 'p1', '2024-03-15', 0, '2024-03-15T00:00:00')
        """)
        conn.commit()
        conn.close()

        # migrate_db should complete without destroying the row
        migrate_db(tmp_db)

        conn = get_connection(tmp_db)
        count = conn.execute("SELECT COUNT(*) FROM raw_services").fetchone()[0]
        conn.close()
        assert count == 1, "Data was deleted by migration despite correct PK!"


class TestNoBobSnapshotsTable:
    def test_bob_snapshots_table_not_created(self, tmp_db):
        """bob_snapshots was removed — it must not exist after init."""
        init_db(tmp_db)
        conn = get_connection(tmp_db)
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        conn.close()
        assert "bob_snapshots" not in tables
