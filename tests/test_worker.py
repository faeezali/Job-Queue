"""Tests for run_once outcomes and the run_worker loop, driven by a fake clock."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest
from conftest import SAMPLES, FakeClock

from taskqueue.errors import PermanentJobError, TransientDemoError
from taskqueue.handlers import HANDLERS, HandlerSpec, JobContext
from taskqueue.queue import Job, enqueue, get_job, list_jobs
from taskqueue.retry import RetryPolicy, is_retryable
from taskqueue.worker import default_worker_id, run_once, run_worker


def half() -> float:
    """rand_fn that makes jitter neutral: next_delay returns exactly the capped delay."""
    return 0.5


def work(db_path: Path, clock: FakeClock, **kwargs: Any) -> bool:
    """One run_once call with the deterministic clock and randomness."""
    return run_once(db_path, worker_id="w1", now_fn=clock, rand_fn=half, **kwargs)


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
    assert lines[0].startswith(f"job {job_id} sleep attempt 1/1 took ")
    assert lines[0].endswith(" ms: done")


# --- retries -----------------------------------------------------------------

PERMANENT_ERRORS = [
    PermanentJobError("nope"),
    ValueError("bad"),
    TypeError("bad"),
    KeyError("k"),
    FileNotFoundError("gone"),
    IsADirectoryError("dir"),
    NotADirectoryError("notdir"),
    PermissionError("denied"),
]


@pytest.mark.parametrize("exc", PERMANENT_ERRORS, ids=lambda e: type(e).__name__)
def test_permanent_errors_fail_on_attempt_1_despite_max_3(
    db_path: Path, clock: FakeClock, exc: Exception
) -> None:
    job_id = enqueue(db_path, "sleep", {"seconds": 0}, max_attempts=3, now_fn=clock)
    work(db_path, clock, handlers=raising(exc))
    failed = job(db_path, job_id)
    assert (failed.status, failed.attempts) == ("failed", 1)
    assert failed.last_error == f"{type(exc).__name__}: {exc}"
    assert not is_retryable(exc)


@pytest.mark.parametrize("name", ["malformed.csv", "does-not-exist.csv"])
def test_bad_csv_input_fails_on_attempt_1_despite_max_3(
    db_path: Path, clock: FakeClock, name: str
) -> None:
    job_id = enqueue(
        db_path, "csv_summary", {"path": str(SAMPLES / name)}, max_attempts=3, now_fn=clock
    )
    work(db_path, clock)
    assert (job(db_path, job_id).status, job(db_path, job_id).attempts) == ("failed", 1)


@pytest.mark.parametrize(
    "exc", [TransientDemoError("x"), RuntimeError("x"), OSError("x"), TimeoutError("x")]
)
def test_other_errors_are_retryable(exc: Exception) -> None:
    assert is_retryable(exc)


def test_transient_failure_schedules_a_retry_2s_later(db_path: Path, clock: FakeClock) -> None:
    job_id = enqueue(db_path, "flaky", {"fail_times": 1}, max_attempts=2, now_fn=clock)
    assert work(db_path, clock) is True
    waiting = job(db_path, job_id)
    assert (waiting.status, waiting.attempts) == ("pending", 1)
    assert waiting.available_at == 100.0 + 2.0
    assert waiting.last_error == "TransientDemoError: planned failure 1 of 1"
    assert waiting.worker_id is waiting.lease_expires_at is waiting.finished_at is None


def test_retry_is_not_claimable_until_its_backoff_elapses(db_path: Path, clock: FakeClock) -> None:
    job_id = enqueue(db_path, "flaky", {"fail_times": 1}, max_attempts=2, now_fn=clock)
    work(db_path, clock)
    clock.now = 101.9
    assert work(db_path, clock) is False
    clock.now = 102.0
    assert work(db_path, clock) is True
    assert job(db_path, job_id).attempts == 2


def test_backoff_doubles_2_4_8_16(db_path: Path, clock: FakeClock) -> None:
    job_id = enqueue(db_path, "flaky", {"fail_times": 10}, max_attempts=5, now_fn=clock)
    delays = []
    for _ in range(4):
        assert work(db_path, clock) is True
        waiting = job(db_path, job_id)
        delays.append(waiting.available_at - clock())
        clock.now = waiting.available_at
    assert delays == [2.0, 4.0, 8.0, 16.0]


def test_backoff_is_capped_at_max_delay(db_path: Path, clock: FakeClock) -> None:
    policy = RetryPolicy(base_delay=2.0, max_delay=5.0)
    job_id = enqueue(db_path, "flaky", {"fail_times": 10}, max_attempts=5, now_fn=clock)
    delays = []
    for _ in range(4):
        work(db_path, clock, retry_policy=policy)
        waiting = job(db_path, job_id)
        delays.append(waiting.available_at - clock())
        clock.now = waiting.available_at
    assert delays == [2.0, 4.0, 5.0, 5.0]


def test_next_delay_formula() -> None:
    policy = RetryPolicy()
    assert [policy.next_delay(n, 0.5) for n in (1, 2, 3, 4, 5, 6, 7)] == [
        2.0,
        4.0,
        8.0,
        16.0,
        32.0,
        60.0,
        60.0,
    ]
    assert policy.next_delay(10_000, 0.5) == 60.0  # no float overflow for huge attempts


def test_jitter_stays_within_20_percent() -> None:
    policy = RetryPolicy()
    assert policy.next_delay(1, 0.0) == pytest.approx(1.6)
    assert policy.next_delay(1, 1.0) == pytest.approx(2.4)
    assert policy.next_delay(6, 0.0) == pytest.approx(48.0)
    assert policy.next_delay(6, 1.0) == pytest.approx(72.0)
    for i in range(101):
        assert 1.6 - 1e-9 <= policy.next_delay(1, i / 100) <= 2.4 + 1e-9
    assert RetryPolicy(jitter=0.0).next_delay(3, 0.9) == 8.0


def test_retry_policy_rejects_bad_values() -> None:
    for kwargs in ({"base_delay": -1}, {"max_delay": float("inf")}, {"jitter": 1.5}):
        with pytest.raises(ValueError):
            RetryPolicy(**kwargs)
    with pytest.raises(ValueError):
        RetryPolicy().next_delay(0, 0.5)
    with pytest.raises(ValueError):
        RetryPolicy().next_delay(1, 1.5)


def test_retries_exhaust_at_max_attempts_and_the_job_is_never_claimed_again(
    db_path: Path, clock: FakeClock
) -> None:
    job_id = enqueue(db_path, "flaky", {"fail_times": 5}, max_attempts=3, now_fn=clock)
    for expected_status in ("pending", "pending", "failed"):
        assert work(db_path, clock) is True
        assert job(db_path, job_id).status == expected_status
        clock.advance(1000)
    failed = job(db_path, job_id)
    assert (failed.attempts, failed.last_error) == (3, "TransientDemoError: planned failure 3 of 5")
    clock.advance(1_000_000)
    assert work(db_path, clock) is False
    assert job(db_path, job_id) == failed


def test_flaky_1_succeeds_on_attempt_2_and_clears_the_error(
    db_path: Path, clock: FakeClock
) -> None:
    job_id = enqueue(db_path, "flaky", {"fail_times": 1}, max_attempts=3, now_fn=clock)
    work(db_path, clock)
    assert job(db_path, job_id).last_error is not None
    clock.advance(2.0)
    work(db_path, clock)
    done = job(db_path, job_id)
    assert (done.status, done.attempts, done.last_error) == ("done", 2, None)
    assert done.result == {"succeeded_on_attempt": 2}


def test_a_waiting_retry_does_not_block_a_ready_job(db_path: Path, clock: FakeClock) -> None:
    retrying = enqueue(db_path, "flaky", {"fail_times": 1}, max_attempts=2, now_fn=clock)
    work(db_path, clock)  # retrying now waits until 102.0
    ready = enqueue(db_path, "sleep", {"seconds": 0}, now_fn=clock)
    assert work(db_path, clock) is True
    assert job(db_path, ready).status == "done"
    assert job(db_path, retrying).status == "pending"


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


def test_run_worker_exit_when_idle_waits_out_a_retry(db_path: Path, clock: FakeClock) -> None:
    job_id = enqueue(db_path, "flaky", {"fail_times": 1}, max_attempts=2, now_fn=clock)
    sleeps: list[float] = []

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock.advance(seconds)

    processed = run_worker(
        db_path,
        worker_id="w",
        exit_when_idle=True,
        poll_interval=0.5,
        now_fn=clock,
        rand_fn=half,
        sleep_fn=fake_sleep,
    )
    assert processed == 2
    assert job(db_path, job_id).status == "done"
    assert sleeps == [0.5] * 4  # idle from 100.0 until the retry is ready at 102.0


def test_run_worker_rejects_bad_arguments(db_path: Path) -> None:
    with pytest.raises(ValueError):
        run_worker(db_path, worker_id="w", poll_interval=-1)
    with pytest.raises(ValueError):
        run_worker(db_path, worker_id="w", max_jobs=0)


def test_default_worker_id_includes_the_pid() -> None:
    assert default_worker_id().endswith(f"-{os.getpid()}")


def test_handlers_registry_has_the_three_demo_types() -> None:
    assert sorted(HANDLERS) == ["csv_summary", "flaky", "sleep"]
