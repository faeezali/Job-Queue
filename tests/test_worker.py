"""Tests for run_once outcomes and the run_worker loop, driven by a fake clock."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest
from conftest import SAMPLES, FakeClock

from taskqueue.handlers import HANDLERS, HandlerSpec, JobContext
from taskqueue.queue import Job, enqueue, get_job, list_jobs
from taskqueue.worker import default_worker_id, run_once, run_worker


def work(db_path: Path, clock: FakeClock, **kwargs: Any) -> bool:
    """One run_once call with the deterministic clock and randomness."""
    return run_once(db_path, worker_id="w1", now_fn=clock, **kwargs)


def job(db_path: Path, job_id: int) -> Job:
    found = get_job(db_path, job_id)
    assert found is not None
    return found


def returning(value: Any) -> dict[str, HandlerSpec]:
    """A handler table whose `sleep` handler returns `value` instead of sleeping."""
    return {"sleep": HandlerSpec(run=lambda payload, ctx: value, validate=lambda p: p)}


def raising(exc: Exception) -> dict[str, HandlerSpec]:
    def run(payload: dict[str, Any], ctx: JobContext) -> dict[str, Any]:
        raise exc

    return {"sleep": HandlerSpec(run=run, validate=lambda p: p)}


def test_run_once_on_empty_queue_returns_false(db_path: Path, clock: FakeClock) -> None:
    assert work(db_path, clock) is False


def test_success_stores_the_exact_result(db_path: Path, clock: FakeClock) -> None:
    job_id = enqueue(db_path, "csv_summary", {"path": str(SAMPLES / "orders.csv")}, now_fn=clock)
    assert work(db_path, clock) is True
    done = job(db_path, job_id)
    assert (done.status, done.attempts, done.last_error) == ("done", 1, None)
    assert done.result == {
        "row_count": 3,
        "columns": ["item", "quantity", "price"],
        "missing_by_column": {"item": 0, "quantity": 1, "price": 1},
    }
    assert done.finished_at == 100.0 and done.lease_expires_at is None


def test_malformed_csv_fails_on_first_attempt(db_path: Path, clock: FakeClock) -> None:
    job_id = enqueue(db_path, "csv_summary", {"path": str(SAMPLES / "malformed.csv")}, now_fn=clock)
    work(db_path, clock)
    failed = job(db_path, job_id)
    assert (failed.status, failed.attempts) == ("failed", 1)
    assert failed.last_error == "ValueError: line 2: row has 4 fields but the header has 3"


def test_unknown_job_type_is_a_permanent_failure(db_path: Path, clock: FakeClock) -> None:
    job_id = enqueue(db_path, "sleep", {"seconds": 0}, max_attempts=3, now_fn=clock)
    work(db_path, clock, handlers={})
    failed = job(db_path, job_id)
    assert (failed.status, failed.attempts) == ("failed", 1)
    assert failed.last_error is not None
    assert failed.last_error.startswith("UnknownJobTypeError: ")


@pytest.mark.parametrize(
    ("value", "reason"),
    [
        (["not", "a", "dict"], "handler returned list, expected a dict"),
        (None, "handler returned NoneType, expected a dict"),
        ({"when": object()}, "result is not JSON-serializable"),
        ({"x": float("nan")}, "result is not JSON-serializable"),
    ],
)
def test_bad_result_is_a_permanent_invalid_result_failure(
    db_path: Path, clock: FakeClock, value: Any, reason: str
) -> None:
    job_id = enqueue(db_path, "sleep", {"seconds": 0}, max_attempts=3, now_fn=clock)
    work(db_path, clock, handlers=returning(value))
    failed = job(db_path, job_id)
    assert (failed.status, failed.attempts, failed.result) == ("failed", 1, None)
    assert failed.last_error is not None
    assert failed.last_error.startswith(f"InvalidResult: {reason}")


def test_handler_receives_the_job_context(db_path: Path, clock: FakeClock) -> None:
    seen: list[JobContext] = []

    def run(payload: dict[str, Any], ctx: JobContext) -> dict[str, Any]:
        seen.append(ctx)
        return {}

    job_id = enqueue(db_path, "sleep", {"seconds": 0}, max_attempts=4, now_fn=clock)
    work(db_path, clock, handlers={"sleep": HandlerSpec(run=run, validate=lambda p: p)})
    assert seen == [JobContext(job_id=job_id, attempt=1, max_attempts=4)]


def test_each_attempt_logs_one_info_line(
    db_path: Path, clock: FakeClock, caplog: pytest.LogCaptureFixture
) -> None:
    job_id = enqueue(db_path, "sleep", {"seconds": 0}, now_fn=clock)
    with caplog.at_level("INFO", logger="taskqueue.worker"):
        work(db_path, clock)
    lines = [r.getMessage() for r in caplog.records if r.name == "taskqueue.worker"]
    assert len(lines) == 1
    assert lines[0].startswith(f"job {job_id} sleep attempt 1/1 done in ")
    assert lines[0].endswith(" ms")


# --- run_worker --------------------------------------------------------------


def make_jobs(db_path: Path, clock: FakeClock, count: int) -> list[int]:
    return [enqueue(db_path, "sleep", {"seconds": 0}, now_fn=clock) for _ in range(count)]


def test_run_worker_exit_when_idle_drains_the_queue(db_path: Path, clock: FakeClock) -> None:
    make_jobs(db_path, clock, 3)
    sleeps: list[float] = []
    processed = run_worker(
        db_path, worker_id="w", exit_when_idle=True, now_fn=clock, sleep_fn=sleeps.append
    )
    assert processed == 3 and sleeps == []
    assert {j.status for j in list_jobs(db_path)} == {"done"}


def test_run_worker_honors_max_jobs(db_path: Path, clock: FakeClock) -> None:
    make_jobs(db_path, clock, 5)
    assert run_worker(db_path, worker_id="w", max_jobs=2, now_fn=clock) == 2
    statuses = [j.status for j in list_jobs(db_path)]
    assert statuses.count("done") == 2 and statuses.count("pending") == 3


def test_run_worker_honors_should_stop(db_path: Path, clock: FakeClock) -> None:
    make_jobs(db_path, clock, 5)
    checks = iter([False, False, True])
    assert run_worker(db_path, worker_id="w", should_stop=lambda: next(checks), now_fn=clock) == 2


def test_run_worker_polls_while_idle_until_stopped(db_path: Path, clock: FakeClock) -> None:
    sleeps: list[float] = []

    def stop() -> bool:
        return len(sleeps) >= 3

    processed = run_worker(
        db_path, worker_id="w", poll_interval=0.5, should_stop=stop, sleep_fn=sleeps.append
    )
    assert processed == 0 and sleeps == [0.5, 0.5, 0.5]


def test_run_worker_waits_for_a_delayed_job_before_exiting_when_idle(
    db_path: Path, clock: FakeClock
) -> None:
    job_id = enqueue(db_path, "sleep", {"seconds": 0}, delay=1.0, now_fn=clock)
    processed = run_worker(
        db_path,
        worker_id="w",
        exit_when_idle=True,
        poll_interval=0.25,
        now_fn=clock,
        sleep_fn=clock.advance,  # "sleeping" moves the fake clock forward
    )
    assert processed == 1
    assert job(db_path, job_id).status == "done"
    assert clock() == 101.0


def test_run_worker_rejects_bad_arguments(db_path: Path) -> None:
    with pytest.raises(ValueError):
        run_worker(db_path, worker_id="w", poll_interval=-1)
    with pytest.raises(ValueError):
        run_worker(db_path, worker_id="w", max_jobs=0)


def test_default_worker_id_includes_the_pid() -> None:
    assert default_worker_id().endswith(f"-{os.getpid()}")


def test_handlers_registry_has_the_three_demo_types() -> None:
    assert sorted(HANDLERS) == ["csv_summary", "flaky", "sleep"]
