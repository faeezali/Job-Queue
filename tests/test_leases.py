"""Tests for lease expiry, reclaiming jobs from dead workers, and fenced writes."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from conftest import FakeClock

from taskqueue.handlers import HandlerSpec, JobContext
from taskqueue.queue import Job, claim_next, complete_job, enqueue, get_job, record_failure
from taskqueue.worker import run_once

LEASE = 30.0


def job(db_path: Path, job_id: int) -> Job:
    found = get_job(db_path, job_id)
    assert found is not None
    return found


def claim(db_path: Path, worker_id: str, now: float) -> Job | None:
    return claim_next(db_path, worker_id=worker_id, now=now, lease_seconds=LEASE)


def test_a_live_lease_is_not_reclaimable(db_path: Path, clock: FakeClock) -> None:
    enqueue(db_path, "sleep", {"seconds": 0}, max_attempts=2, now_fn=clock)
    assert claim(db_path, "w1", clock()) is not None  # lease runs until 130.0
    assert claim(db_path, "w2", 129.999) is None


def test_an_expired_lease_is_reclaimed_as_attempt_2(
    db_path: Path, clock: FakeClock, caplog: pytest.LogCaptureFixture
) -> None:
    job_id = enqueue(db_path, "sleep", {"seconds": 0}, max_attempts=2, now_fn=clock)
    claim(db_path, "w1", clock())
    with caplog.at_level("WARNING", logger="taskqueue.queue"):
        reclaimed = claim(db_path, "w2", 130.0)  # expiry is inclusive: lease_expires_at <= now
    assert reclaimed is not None and reclaimed.id == job_id
    assert (reclaimed.status, reclaimed.attempts, reclaimed.worker_id) == ("running", 2, "w2")
    assert (reclaimed.lease_expires_at, reclaimed.started_at) == (160.0, 130.0)
    assert "lease held by w1 expired; reclaimed by w2 as attempt 2/2" in caplog.text


def test_reclaim_respects_fifo_order(db_path: Path, clock: FakeClock) -> None:
    first = enqueue(db_path, "sleep", {"seconds": 0}, max_attempts=2, now_fn=clock)
    claim(db_path, "w1", clock())
    second = enqueue(db_path, "sleep", {"seconds": 0}, now_fn=clock)
    later = claim(db_path, "w2", 130.0)
    assert later is not None and later.id == first  # the older, expired job goes first
    assert (claimed := claim(db_path, "w2", 130.0)) is not None and claimed.id == second


def stale_and_current(db_path: Path, clock: FakeClock) -> tuple[Job, Job]:
    """w1 claims the job, stalls past its lease, and w2 reclaims it."""
    enqueue(db_path, "sleep", {"seconds": 0}, max_attempts=3, now_fn=clock)
    stale = claim(db_path, "w1", clock())
    current = claim(db_path, "w2", clock.advance(LEASE))
    assert stale is not None and current is not None
    return stale, current


def test_stale_complete_job_is_fenced_off(db_path: Path, clock: FakeClock) -> None:
    stale, current = stale_and_current(db_path, clock)
    before = job(db_path, stale.id)
    assert not complete_job(
        db_path, stale.id, worker_id="w1", attempt=1, result={"stale": True}, now=clock()
    )
    assert job(db_path, stale.id) == before
    assert complete_job(db_path, current.id, worker_id="w2", attempt=2, result={}, now=clock())
    assert job(db_path, stale.id).status == "done"


def test_stale_record_failure_is_fenced_off(db_path: Path, clock: FakeClock) -> None:
    stale, current = stale_and_current(db_path, clock)
    before = job(db_path, stale.id)
    for retry_at in (None, clock() + 2):
        assert not record_failure(
            db_path,
            stale.id,
            worker_id="w1",
            attempt=1,
            error="RuntimeError: late",
            retry_at=retry_at,
            now=clock(),
        )
    assert job(db_path, stale.id) == before


def test_fence_checks_the_attempt_not_just_the_worker(db_path: Path, clock: FakeClock) -> None:
    # The same worker id can reclaim its own job (e.g. a restarted process that
    # reuses its id); its write for the abandoned attempt must still be rejected.
    enqueue(db_path, "sleep", {"seconds": 0}, max_attempts=3, now_fn=clock)
    claim(db_path, "w1", clock())
    again = claim(db_path, "w1", clock.advance(LEASE))
    assert again is not None and again.attempts == 2
    assert not complete_job(db_path, again.id, worker_id="w1", attempt=1, result={}, now=clock())
    assert complete_job(db_path, again.id, worker_id="w1", attempt=2, result={}, now=clock())


def test_fenced_writes_reject_jobs_that_are_not_running(db_path: Path, clock: FakeClock) -> None:
    job_id = enqueue(db_path, "sleep", {"seconds": 0}, now_fn=clock)
    assert not complete_job(db_path, job_id, worker_id="w1", attempt=0, result={}, now=clock())
    assert job(db_path, job_id).status == "pending"


def test_expired_lease_on_the_last_attempt_fails_with_lease_expired(
    db_path: Path, clock: FakeClock
) -> None:
    job_id = enqueue(db_path, "sleep", {"seconds": 0}, max_attempts=1, now_fn=clock)
    claim(db_path, "w1", clock())
    assert claim(db_path, "w2", 130.0) is None
    failed = job(db_path, job_id)
    assert (failed.status, failed.attempts, failed.finished_at) == ("failed", 1, 130.0)
    assert failed.last_error == (
        "LeaseExpired: worker w1 did not finish attempt 1/1 before its lease expired"
    )
    assert failed.lease_expires_at is failed.worker_id is None
    assert not complete_job(db_path, job_id, worker_id="w1", attempt=1, result={}, now=131.0)


def test_exhausted_lease_is_failed_even_when_another_job_is_claimed(
    db_path: Path, clock: FakeClock
) -> None:
    doomed = enqueue(db_path, "sleep", {"seconds": 0}, now_fn=clock)
    claim(db_path, "w1", clock())
    ready = enqueue(db_path, "sleep", {"seconds": 0}, now_fn=clock)
    claimed = claim(db_path, "w2", 130.0)
    assert claimed is not None and claimed.id == ready
    assert job(db_path, doomed).status == "failed"


def test_worker_logs_a_warning_when_its_lease_was_lost(
    db_path: Path, clock: FakeClock, caplog: pytest.LogCaptureFixture
) -> None:
    job_id = enqueue(db_path, "sleep", {"seconds": 0}, max_attempts=2, now_fn=clock)

    def slow_run(payload: dict[str, Any], ctx: JobContext) -> dict[str, Any]:
        # While this attempt "runs", its lease expires and another worker takes over.
        assert claim_next(db_path, worker_id="w2", now=clock.advance(LEASE), lease_seconds=LEASE)
        return {"from": "w1"}

    handlers = {"sleep": HandlerSpec(run=slow_run, validate=lambda p: p)}
    with caplog.at_level("INFO", logger="taskqueue"):
        assert run_once(db_path, worker_id="w1", now_fn=clock, handlers=handlers) is True
    assert f"job {job_id}: lease lost before attempt 1 was recorded" in caplog.text
    current = job(db_path, job_id)
    assert (current.status, current.worker_id, current.attempts, current.result) == (
        "running",
        "w2",
        2,
        None,
    )
