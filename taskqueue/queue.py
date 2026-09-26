"""Queue operations on the jobs table: enqueue, claim, record outcomes, and inspect.

Every public function opens its own connection and closes it before returning,
so callers never share a connection across processes or long waits.
"""

from __future__ import annotations

import json
import logging
import math
import sqlite3
import time
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from typing import Any

from taskqueue.db import DbPath, connect, transaction
from taskqueue.handlers import validate_payload

log = logging.getLogger(__name__)

STATUSES: tuple[str, ...] = ("pending", "running", "done", "failed")


@dataclass(frozen=True)
class Job:
    """One row of the jobs table, with the JSON columns decoded."""

    id: int
    type: str
    payload: dict[str, Any]
    status: str
    attempts: int
    max_attempts: int
    available_at: float
    lease_expires_at: float | None
    worker_id: str | None
    result: dict[str, Any] | None
    last_error: str | None
    created_at: float
    started_at: float | None
    finished_at: float | None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> Job:
        result_json = row["result_json"]
        return cls(
            id=row["id"],
            type=row["type"],
            payload=json.loads(row["payload_json"]),
            status=row["status"],
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            available_at=row["available_at"],
            lease_expires_at=row["lease_expires_at"],
            worker_id=row["worker_id"],
            result=None if result_json is None else json.loads(result_json),
            last_error=row["last_error"],
            created_at=row["created_at"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
        )


def _is_real(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def enqueue(
    db: DbPath,
    job_type: str,
    payload: Any,
    *,
    max_attempts: int = 1,
    delay: float = 0.0,
    now_fn: Callable[[], float] = time.time,
) -> int:
    """Validate and insert a pending job; return its id.

    Raises ValueError (nothing is inserted) for an unknown type, a bad payload,
    max_attempts < 1, or a negative delay.
    """
    normalized = validate_payload(job_type, payload)
    if not isinstance(max_attempts, int) or isinstance(max_attempts, bool) or max_attempts < 1:
        raise ValueError(f"max_attempts must be an integer >= 1, got {max_attempts!r}")
    if not _is_real(delay) or delay < 0:
        raise ValueError(f"delay must be a finite number >= 0, got {delay!r}")
    payload_json = json.dumps(normalized, sort_keys=True)

    now = now_fn()
    with closing(connect(db)) as conn:
        with transaction(conn):
            cursor = conn.execute(
                "INSERT INTO jobs (type, payload_json, max_attempts, available_at, created_at)"
                " VALUES (?, ?, ?, ?, ?)",
                (job_type, payload_json, max_attempts, now + delay, now),
            )
    job_id = cursor.lastrowid
    assert job_id is not None  # always set after a successful INSERT
    log.debug("enqueued job %d (%s)", job_id, job_type)
    return job_id


def claim_next(db: DbPath, *, worker_id: str, now: float, lease_seconds: float) -> Job | None:
    """Atomically take the oldest ready pending job and lease it to `worker_id`.

    Returns None when no job is ready at `now`.
    """
    if not _is_real(lease_seconds) or lease_seconds <= 0:
        raise ValueError(f"lease_seconds must be a positive number, got {lease_seconds!r}")
    with closing(connect(db)) as conn:
        with transaction(conn):
            row = conn.execute(
                "SELECT id FROM jobs WHERE status = 'pending' AND available_at <= ?"
                " ORDER BY id LIMIT 1",
                (now,),
            ).fetchone()
            if row is None:
                return None
            conn.execute(
                "UPDATE jobs SET status = 'running', attempts = attempts + 1, worker_id = ?,"
                " lease_expires_at = ?, started_at = ? WHERE id = ?",
                (worker_id, now + lease_seconds, now, row["id"]),
            )
            claimed = conn.execute("SELECT * FROM jobs WHERE id = ?", (row["id"],)).fetchone()
    job = Job.from_row(claimed)
    log.debug("worker %s claimed job %d attempt %d", worker_id, job.id, job.attempts)
    return job


# complete_job and record_failure are fenced: the WHERE clause matches only if
# this worker still holds this exact attempt. If the lease expired and another
# worker reclaimed the job, attempts has moved on and the stale write is a no-op.


def complete_job(
    db: DbPath, job_id: int, *, worker_id: str, attempt: int, result: dict[str, Any], now: float
) -> bool:
    """Mark the attempt done with `result`; return False if the lease was lost."""
    result_json = json.dumps(result, allow_nan=False)
    with closing(connect(db)) as conn:
        with transaction(conn):
            cursor = conn.execute(
                "UPDATE jobs SET status = 'done', result_json = ?, last_error = NULL,"
                " lease_expires_at = NULL, finished_at = ?"
                " WHERE id = ? AND status = 'running' AND worker_id = ? AND attempts = ?",
                (result_json, now, job_id, worker_id, attempt),
            )
    return cursor.rowcount == 1


def record_failure(
    db: DbPath, job_id: int, *, worker_id: str, attempt: int, error: str, now: float
) -> bool:
    """Mark the attempt failed with `error`; return False if the lease was lost."""
    with closing(connect(db)) as conn:
        with transaction(conn):
            cursor = conn.execute(
                "UPDATE jobs SET status = 'failed', last_error = ?, lease_expires_at = NULL,"
                " worker_id = NULL, finished_at = ?"
                " WHERE id = ? AND status = 'running' AND worker_id = ? AND attempts = ?",
                (error, now, job_id, worker_id, attempt),
            )
    return cursor.rowcount == 1


def get_job(db: DbPath, job_id: int) -> Job | None:
    """Return the job with this id, or None."""
    with closing(connect(db)) as conn:
        row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    return None if row is None else Job.from_row(row)


def list_jobs(db: DbPath, *, status: str | None = None, limit: int = 50) -> list[Job]:
    """Return up to `limit` jobs, newest first, optionally filtered by status."""
    if status is not None and status not in STATUSES:
        raise ValueError(f"status must be one of {', '.join(STATUSES)}, got {status!r}")
    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
        raise ValueError(f"limit must be an integer >= 1, got {limit!r}")
    with closing(connect(db)) as conn:
        if status is None:
            rows = conn.execute("SELECT * FROM jobs ORDER BY id DESC LIMIT ?", (limit,))
        else:
            rows = conn.execute(
                "SELECT * FROM jobs WHERE status = ? ORDER BY id DESC LIMIT ?", (status, limit)
            )
        return [Job.from_row(row) for row in rows]


def has_unfinished(db: DbPath) -> bool:
    """True if any job is pending (even if not yet ready) or running."""
    with closing(connect(db)) as conn:
        row = conn.execute(
            "SELECT EXISTS (SELECT 1 FROM jobs WHERE status IN ('pending', 'running'))"
        ).fetchone()
    return bool(row[0])
