"""
tests/test_notifications_migration.py — Notifications table migration tests
"""

import pytest
from db import init_db, migrate_db, get_connection


@pytest.fixture
def tmp_db(tmp_path):
    return tmp_path / "test.db"


class TestNotificationsMigration:
    def test_table_created_by_migrate_db(self, tmp_db):
        init_db(tmp_db)
        migrate_db(tmp_db)
        conn = get_connection(tmp_db)
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        conn.close()
        assert "notifications" in tables

    def test_idempotent_double_migrate(self, tmp_db):
        init_db(tmp_db)
        migrate_db(tmp_db)
        migrate_db(tmp_db)   # must not raise

    def test_can_insert_and_read_row(self, tmp_db):
        init_db(tmp_db)
        migrate_db(tmp_db)
        conn = get_connection(tmp_db)
        conn.execute(
            "INSERT INTO notifications (property_id, audience, chat_id, label) "
            "VALUES (?, ?, ?, ?)",
            ("p1", "owner", "-100123", "Test Group"),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM notifications WHERE property_id='p1'").fetchone()
        conn.close()
        assert row["chat_id"] == "-100123"
        assert row["enabled"] == 1
        assert row["audience"] == "owner"

    def test_enabled_defaults_to_1(self, tmp_db):
        init_db(tmp_db)
        migrate_db(tmp_db)
        conn = get_connection(tmp_db)
        conn.execute(
            "INSERT INTO notifications (property_id, audience, chat_id) VALUES (?,?,?)",
            ("p1", "owner", "99999"),
        )
        conn.commit()
        row = conn.execute("SELECT enabled FROM notifications").fetchone()
        conn.close()
        assert row["enabled"] == 1
