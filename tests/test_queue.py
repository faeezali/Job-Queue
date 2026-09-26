"""Tests for enqueue validation, claiming order and timing, and the read helpers."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path

import pytest
from conftest import FakeClock

from taskqueue.db import connect
from taskqueue.errors import UnknownJobTypeError
from taskqueue.queue import (
    Job,
    claim_next,
    complete_job,
    count_by_status,
    enqueue,
    get_job,
    has_unfinished,
    list_jobs,
    record_failure,
    requeue,
)


def row_count(db_path: Path) -> int:
    with closing(connect(db_path)) as conn:
        return conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]


def test_enqueue_defaults(db_path: Path, clock: FakeClock) -> None:
    job_id = enqueue(db_path, "sleep", {"seconds": 1}, now_fn=clock)
    job = get_job(db_path, job_id)
    assert job is not None
    assert (job.type, job.payload, job.status) == ("sleep", {"seconds": 1}, "pending")
    assert (job.attempts, job.max_attempts) == (0, 1)
    assert job.available_at == job.created_at == 100.0
    assert job.lease_expires_at is job.worker_id is job.result is job.last_error is None
    assert job.started_at is job.finished_at is None


def test_enqueue_stores_an_absolute_csv_path(
    db_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    job = get_job(db_path, enqueue(db_path, "csv_summary", {"path": "in.csv"}))
    assert job is not None
    assert job.payload == {"path": str((tmp_path / "in.csv").resolve())}


def test_enqueue_delay_sets_available_at(db_path: Path, clock: FakeClock) -> None:
    job = get_job(db_path, enqueue(db_path, "sleep", {"seconds": 0}, delay=5, now_fn=clock))
    assert job is not None and job.available_at == 105.0


@pytest.mark.parametrize(
    ("job_type", "payload", "kwargs", "error"),
    [
        ("nope", {}, {}, UnknownJobTypeError),
        ("sleep", {"seconds": -1}, {}, ValueError),
        ("sleep", [], {}, ValueError),
        ("sleep", None, {}, ValueError),
        ("flaky", {"fail_times": "1"}, {}, ValueError),
        ("csv_summary", {"path": ""}, {}, ValueError),
        ("sleep", {"seconds": 0}, {"max_attempts": 0}, ValueError),
        ("sleep", {"seconds": 0}, {"max_attempts": -2}, ValueError),
        ("sleep", {"seconds": 0}, {"max_attempts": 1.5}, ValueError),
        ("sleep", {"seconds": 0}, {"max_attempts": True}, ValueError),
        ("sleep", {"seconds": 0}, {"delay": -0.01}, ValueError),
        ("sleep", {"seconds": 0}, {"delay": float("nan")}, ValueError),
        ("sleep", {"seconds": 0}, {"delay": float("inf")}, ValueError),
        ("sleep", {"seconds": 0}, {"delay": "5"}, ValueError),
    ],
)
def test_enqueue_rejects_invalid_input_without_inserting(
    db_path: Path, job_type: str, payload: object, kwargs: dict, error: type[Exception]
) -> None:
    with pytest.raises(error):
        enqueue(db_path, job_type, payload, **kwargs)
    assert row_count(db_path) == 0


def test_jobs_persist_across_connections(db_path: Path) -> None:
    job_id = enqueue(db_path, "flaky", {"fail_times": 2}, max_attempts=3)
    # A raw connection, independent of the queue module, sees the committed row.
    with closing(sqlite3.connect(db_path)) as conn:
        row = conn.execute(
            "SELECT type, payload_json, status, max_attempts FROM jobs WHERE id = ?", (job_id,)
        ).fetchone()
    assert row == ("flaky", '{"fail_times": 2}', "pending", 3)


def test_claims_are_fifo(db_path: Path, clock: FakeClock) -> None:
    ids = [enqueue(db_path, "sleep", {"seconds": i}, now_fn=clock) for i in range(3)]
    claimed = [claim_next(db_path, worker_id="w", now=clock(), lease_seconds=30) for _ in ids]
    assert [job.id for job in claimed if job] == ids


def test_claim_sets_running_state(db_path: Path, clock: FakeClock) -> None:
    job_id = enqueue(db_path, "sleep", {"seconds": 0}, now_fn=clock)
    job = claim_next(db_path, worker_id="w1", now=clock(), lease_seconds=30)
    assert job is not None and job.id == job_id
    assert (job.status, job.attempts, job.worker_id) == ("running", 1, "w1")
    assert (job.lease_expires_at, job.started_at) == (130.0, 100.0)


def test_claim_on_empty_queue_returns_none(db_path: Path, clock: FakeClock) -> None:
    assert claim_next(db_path, worker_id="w", now=clock(), lease_seconds=30) is None


def test_a_claimed_job_is_not_claimed_again(db_path: Path, clock: FakeClock) -> None:
    enqueue(db_path, "sleep", {"seconds": 0}, now_fn=clock)
    assert claim_next(db_path, worker_id="a", now=clock(), lease_seconds=30) is not None
    assert claim_next(db_path, worker_id="b", now=clock(), lease_seconds=30) is None


def test_delayed_job_is_claimable_exactly_at_available_at(db_path: Path, clock: FakeClock) -> None:
    job_id = enqueue(db_path, "sleep", {"seconds": 0}, delay=10, now_fn=clock)
    assert claim_next(db_path, worker_id="w", now=109.999, lease_seconds=30) is None
    job = claim_next(db_path, worker_id="w", now=110.0, lease_seconds=30)
    assert job is not None and job.id == job_id


def test_claim_rejects_non_positive_lease(db_path: Path) -> None:
    with pytest.raises(ValueError):
        claim_next(db_path, worker_id="w", now=0, lease_seconds=0)


def test_complete_job_stores_result(db_path: Path, clock: FakeClock) -> None:
    enqueue(db_path, "sleep", {"seconds": 0}, now_fn=clock)
    job = claim_next(db_path, worker_id="w", now=clock(), lease_seconds=30)
    assert job is not None
    assert complete_job(db_path, job.id, worker_id="w", attempt=1, result={"ok": 1}, now=101.0)
    done = get_job(db_path, job.id)
    assert done is not None
    assert (done.status, done.result, done.finished_at, done.lease_expires_at) == (
        "done",
        {"ok": 1},
        101.0,
        None,
    )


def claimed(db_path: Path, clock: FakeClock, max_attempts: int = 1) -> Job:
    enqueue(db_path, "sleep", {"seconds": 0}, max_attempts=max_attempts, now_fn=clock)
    job = claim_next(db_path, worker_id="w", now=clock(), lease_seconds=30)
    assert job is not None
    return job


def test_record_failure_without_retry_at_is_terminal(db_path: Path, clock: FakeClock) -> None:
    job = claimed(db_path, clock)
    assert record_failure(
        db_path, job.id, worker_id="w", attempt=1, error="X: y", retry_at=None, now=101.0
    )
    failed = get_job(db_path, job.id)
    assert failed is not None
    assert (failed.status, failed.last_error, failed.finished_at) == ("failed", "X: y", 101.0)
    assert failed.worker_id is failed.lease_expires_at is None


def test_record_failure_with_retry_at_goes_back_to_pending(db_path: Path, clock: FakeClock) -> None:
    job = claimed(db_path, clock, max_attempts=2)
    assert record_failure(
        db_path, job.id, worker_id="w", attempt=1, error="X: y", retry_at=150.0, now=101.0
    )
    waiting = get_job(db_path, job.id)
    assert waiting is not None
    assert (waiting.status, waiting.attempts, waiting.available_at) == ("pending", 1, 150.0)
    assert waiting.last_error == "X: y"
    assert waiting.worker_id is waiting.lease_expires_at is waiting.finished_at is None


def test_requeue_resets_a_failed_job(db_path: Path, clock: FakeClock) -> None:
    job = claimed(db_path, clock, max_attempts=2)
    record_failure(db_path, job.id, worker_id="w", attempt=1, error="E", retry_at=None, now=101.0)
    assert requeue(db_path, job.id, now=200.0) is True
    fresh = get_job(db_path, job.id)
    assert fresh is not None
    assert (fresh.status, fresh.attempts, fresh.max_attempts, fresh.available_at) == (
        "pending",
        0,
        2,
        200.0,
    )
    assert fresh.last_error is fresh.result is fresh.started_at is fresh.finished_at is None
    assert fresh.worker_id is fresh.lease_expires_at is None
    assert fresh.created_at == 100.0


def test_requeue_only_accepts_failed_jobs(db_path: Path, clock: FakeClock) -> None:
    running = claimed(db_path, clock)
    done = claimed(db_path, clock)
    complete_job(db_path, done.id, worker_id="w", attempt=1, result={}, now=clock())
    pending_id = enqueue(db_path, "sleep", {"seconds": 0}, now_fn=clock)
    ids = (running.id, done.id, pending_id)
    before = [get_job(db_path, job_id) for job_id in ids]
    for job_id in (*ids, 999):
        assert requeue(db_path, job_id, now=200.0) is False
    assert [get_job(db_path, job_id) for job_id in ids] == before


def test_count_by_status_is_zero_filled(db_path: Path, clock: FakeClock) -> None:
    assert count_by_status(db_path) == {"pending": 0, "running": 0, "done": 0, "failed": 0}
    claimed(db_path, clock)
    enqueue(db_path, "sleep", {"seconds": 0}, now_fn=clock)
    assert count_by_status(db_path) == {"pending": 1, "running": 1, "done": 0, "failed": 0}


def test_get_job_missing_returns_none(db_path: Path) -> None:
    assert get_job(db_path, 999) is None


def test_list_jobs_newest_first_with_filter_and_limit(db_path: Path, clock: FakeClock) -> None:
    ids = [enqueue(db_path, "sleep", {"seconds": 0}, now_fn=clock) for _ in range(3)]
    claim_next(db_path, worker_id="w", now=clock(), lease_seconds=30)  # takes ids[0]
    assert [j.id for j in list_jobs(db_path)] == ids[::-1]
    assert [j.id for j in list_jobs(db_path, limit=2)] == [ids[2], ids[1]]
    assert [j.id for j in list_jobs(db_path, status="running")] == [ids[0]]
    with pytest.raises(ValueError):
        list_jobs(db_path, status="lost")
    with pytest.raises(ValueError):
        list_jobs(db_path, limit=0)


def test_has_unfinished_tracks_pending_and_running(db_path: Path, clock: FakeClock) -> None:
    assert not has_unfinished(db_path)
    enqueue(db_path, "sleep", {"seconds": 0}, delay=60, now_fn=clock)  # pending, not yet ready
    assert has_unfinished(db_path)
    job = claim_next(db_path, worker_id="w", now=clock.advance(60), lease_seconds=30)
    assert job is not None and has_unfinished(db_path)
    complete_job(db_path, job.id, worker_id="w", attempt=1, result={}, now=clock())
    assert not has_unfinished(db_path)
