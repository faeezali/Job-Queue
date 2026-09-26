"""Tests for schema creation, WAL mode, and the schema-version guard."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from taskqueue.db import SCHEMA_VERSION, connect, init_db, transaction


def test_init_db_twice_is_harmless(tmp_path: Path) -> None:
    path = tmp_path / "jobs.db"
    init_db(path)
    init_db(path)
    with closing(connect(path)) as conn:
        tables = [
            r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        ]
        assert tables == ["jobs"]
        assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION


def test_init_db_enables_wal(db_path: Path) -> None:
    with closing(connect(db_path)) as conn:
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"


def test_connect_returns_rows_by_name_in_autocommit_mode(db_path: Path) -> None:
    with closing(connect(db_path)) as conn:
        assert conn.isolation_level is None
        row = conn.execute("SELECT 1 AS one").fetchone()
        assert row["one"] == 1


def test_old_schema_raises_with_instructions(tmp_path: Path) -> None:
    path = tmp_path / "old.db"
    with closing(sqlite3.connect(path)) as conn:  # the pre-rename TaskForge schema, version 0
        conn.execute("CREATE TABLE jobs (id INTEGER PRIMARY KEY, task TEXT NOT NULL)")
        conn.commit()
    with pytest.raises(RuntimeError, match="delete the local DB file"):
        init_db(path)


def test_transaction_rolls_back_and_reraises(db_path: Path) -> None:
    with closing(connect(db_path)) as conn:
        with pytest.raises(KeyError):
            with transaction(conn):
                conn.execute(
                    "INSERT INTO jobs (type, payload_json, available_at, created_at)"
                    " VALUES ('sleep', '{}', 0, 0)"
                )
                raise KeyError("boom")
        assert not conn.in_transaction
        assert conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 0


def test_schema_rejects_invalid_status_and_max_attempts(db_path: Path) -> None:
    with closing(connect(db_path)) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO jobs (type, payload_json, available_at, created_at, status)"
                " VALUES ('sleep', '{}', 0, 0, ?)",
                ("lost",),
            )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO jobs (type, payload_json, available_at, created_at, max_attempts)"
                " VALUES ('sleep', '{}', 0, 0, ?)",
                (0,),
            )
