"""Multi-process tests: real worker subprocesses draining a shared database, and signals.

These use real time and real processes, so they are the slow part of the suite.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from typing import IO

from conftest import REPO_ROOT

from taskqueue.queue import enqueue, get_job, list_jobs

JOBS = 200
WORKERS = 4


def worker_process(db: Path, stderr: IO[bytes], *extra: str) -> subprocess.Popen[bytes]:
    """Start `python -m taskqueue --db DB worker ...` with this checkout importable."""
    pythonpath = os.pathsep.join(filter(None, [str(REPO_ROOT), os.environ.get("PYTHONPATH")]))
    env = {**os.environ, "PYTHONPATH": pythonpath}
    return subprocess.Popen(
        [sys.executable, "-m", "taskqueue", "--db", str(db), "worker", *extra],
        stdout=subprocess.DEVNULL,
        stderr=stderr,
        env=env,
    )


def status(db: Path, job_id: int) -> str | None:
    job = get_job(db, job_id)
    return None if job is None else job.status


def wait_for(condition: Callable[[], bool], timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError("timed out waiting for condition")
        time.sleep(0.02)


def test_four_worker_processes_drain_200_jobs_exactly_once(db_path: Path, tmp_path: Path) -> None:
    for _ in range(JOBS):
        enqueue(db_path, "sleep", {"seconds": 0})

    logs = [(tmp_path / f"worker-{i}.log").open("wb") for i in range(WORKERS)]
    procs = [
        worker_process(db_path, log, "--exit-when-idle", "--worker-id", f"w{i}")
        for i, log in enumerate(logs)
    ]
    try:
        codes = [proc.wait(timeout=60) for proc in procs]
    finally:
        for proc in procs:
            if proc.poll() is None:
                proc.kill()
        for log in logs:
            log.close()
    errors = [p.read_text() for p in sorted(tmp_path.glob("worker-*.log"))]
    assert codes == [0] * WORKERS, errors

    jobs = list_jobs(db_path, limit=JOBS + 1)
    assert len(jobs) == JOBS
    assert all(job.status == "done" and job.attempts == 1 for job in jobs)
    assert all(job.result == {"slept": 0} for job in jobs)
    # Not an assertion (a fast worker may legitimately take most jobs), but
    # printed so `pytest -s` shows how the work was spread.
    print("jobs per worker:", dict(sorted(Counter(job.worker_id for job in jobs).items())))


def test_sigterm_finishes_the_current_job_then_exits_0(db_path: Path, tmp_path: Path) -> None:
    current = enqueue(db_path, "sleep", {"seconds": 0.5})
    queued = enqueue(db_path, "sleep", {"seconds": 0})
    log_path = tmp_path / "worker.log"
    with log_path.open("wb") as log:
        proc = worker_process(db_path, log, "--poll-interval", "0.05")
        try:
            wait_for(lambda: status(db_path, current) == "running")
            proc.send_signal(signal.SIGTERM)
            assert proc.wait(timeout=10) == 0, log_path.read_text()
        finally:
            if proc.poll() is None:
                proc.kill()
    stderr = log_path.read_text()
    assert "stop requested" in stderr and "Traceback" not in stderr
    finished = get_job(db_path, current)
    assert finished is not None and (finished.status, finished.attempts) == ("done", 1)
    untouched = get_job(db_path, queued)
    assert untouched is not None and untouched.status == "pending"  # no new job after the stop


def test_second_sigint_aborts_without_a_traceback(db_path: Path, tmp_path: Path) -> None:
    job_id = enqueue(db_path, "sleep", {"seconds": 30})
    log_path = tmp_path / "worker.log"
    with log_path.open("wb") as log:
        proc = worker_process(db_path, log, "--poll-interval", "0.05")
        try:
            wait_for(lambda: status(db_path, job_id) == "running")
            started = time.monotonic()
            proc.send_signal(signal.SIGINT)
            # Wait until the first signal was handled; two quick signals can
            # be merged by the OS into one delivery.
            wait_for(lambda: "stop requested" in log_path.read_text())
            proc.send_signal(signal.SIGINT)
            assert proc.wait(timeout=10) == 130
            assert time.monotonic() - started < 10  # did not wait for the 30 s job
        finally:
            if proc.poll() is None:
                proc.kill()
    stderr = log_path.read_text()
    assert "aborted" in stderr and "Traceback" not in stderr
    abandoned = get_job(db_path, job_id)
    # Left running: another worker reclaims it once the lease expires.
    assert abandoned is not None and (abandoned.status, abandoned.attempts) == ("running", 1)
