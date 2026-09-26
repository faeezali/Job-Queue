"""SQLite storage: connections, the schema, and the write-transaction helper.

The database file is the only shared state between producers and workers, so
every process opens its own short-lived connections through `connect` and
performs every write inside `transaction`.
"""

from __future__ import annotations

import os
import sqlite3
from collections.abc import Iterator
from contextlib import closing, contextmanager

DbPath = str | os.PathLike[str]

SCHEMA_VERSION = 1

# Executed statement by statement rather than with executescript(), because
# executescript() commits any open transaction first and would drop the lock
# that makes schema creation safe when several processes start at once.
SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE jobs (
      id INTEGER PRIMARY KEY,
      type TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','running','done','failed')),
      attempts INTEGER NOT NULL DEFAULT 0,
      max_attempts INTEGER NOT NULL DEFAULT 1 CHECK (max_attempts >= 1),
      available_at REAL NOT NULL,
      lease_expires_at REAL,
      worker_id TEXT,
      result_json TEXT,
      last_error TEXT,
      created_at REAL NOT NULL,
      started_at REAL,
      finished_at REAL
    )
    """,
    "CREATE INDEX idx_jobs_status_available ON jobs (status, available_at)",
    # PRAGMA arguments cannot be bound with `?`; this is a trusted integer constant.
    f"PRAGMA user_version = {SCHEMA_VERSION}",
)


def connect(path: DbPath) -> sqlite3.Connection:
    """Open a connection in autocommit mode with rows addressable by column name."""
    # isolation_level=None stops the sqlite3 module from issuing its own
    # implicit BEGINs, so `transaction` fully controls when locks are taken.
    conn = sqlite3.connect(path, timeout=10.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Run the block in a write transaction: BEGIN IMMEDIATE, then COMMIT or ROLLBACK.

    IMMEDIATE takes the database write lock before the first read, so a
    select-then-update (like claiming a job) cannot interleave with another
    process doing the same, and a read never has to upgrade to a write lock
    mid-transaction (which is where SQLITE_BUSY deadlocks come from).
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        # Some errors (e.g. SQLITE_FULL) make SQLite roll back on its own; a
        # second ROLLBACK would raise and hide the original exception.
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


def init_db(path: DbPath) -> None:
    """Create the schema if needed, enable WAL, and reject databases from other versions.

    Safe to call repeatedly and from several processes at once.
    """
    with closing(connect(path)) as conn:
        # WAL is stored in the file itself, so setting it once is enough; it lets
        # readers (list, stats) run while a worker holds the write lock.
        conn.execute("PRAGMA journal_mode=WAL")
        with transaction(conn):
            has_jobs = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", ("jobs",)
            ).fetchone()
            version = conn.execute("PRAGMA user_version").fetchone()[0]
            if has_jobs is not None:
                if version != SCHEMA_VERSION:
                    raise RuntimeError(
                        f"{os.fspath(path)} has schema version {version}, but this TaskQueue "
                        f"needs version {SCHEMA_VERSION}. There are no migrations: delete the "
                        "local DB file and run the command again."
                    )
                return
            for statement in SCHEMA_STATEMENTS:
                conn.execute(statement)
