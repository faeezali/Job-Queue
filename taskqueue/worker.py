"""The worker: claim one job, run its handler, record the outcome, and loop.

Time and randomness are injected (`now_fn`, `rand_fn`, `sleep_fn`) so tests can
drive retries and leases with a fake clock instead of real waiting.
"""

from __future__ import annotations

import json
import logging
import os
import socket
import time
from collections.abc import Callable, Mapping
from typing import Any

from taskqueue.db import DbPath
from taskqueue.errors import InvalidResult, UnknownJobTypeError
from taskqueue.handlers import HANDLERS, HandlerSpec, JobContext
from taskqueue.queue import Job, claim_next, complete_job, has_unfinished, record_failure

log = logging.getLogger(__name__)


def default_worker_id() -> str:
    """A worker id that is unique per process on this machine."""
    return f"{socket.gethostname()}-{os.getpid()}"


def _execute(job: Job, handlers: Mapping[str, HandlerSpec]) -> dict[str, Any]:
    """Run the job's handler and return its result, raising on any failure."""
    spec = handlers.get(job.type)
    if spec is None:
        raise UnknownJobTypeError(f"no handler registered for job type {job.type!r}")
    ctx = JobContext(job_id=job.id, attempt=job.attempts, max_attempts=job.max_attempts)
    # Re-validate because rows can be written by other producers (or an older
    # version of this code); a bad payload then fails as a ValueError.
    result = spec.run(spec.validate(job.payload), ctx)
    if not isinstance(result, dict):
        raise InvalidResult(f"handler returned {type(result).__name__}, expected a dict")
    try:
        json.dumps(result, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise InvalidResult(f"result is not JSON-serializable: {exc}") from exc
    return result


def run_once(
    db: DbPath,
    *,
    worker_id: str,
    now_fn: Callable[[], float] = time.time,
    lease_seconds: float = 30.0,
    handlers: Mapping[str, HandlerSpec] = HANDLERS,
) -> bool:
    """Claim and process one job. Return False if no job was ready."""
    job = claim_next(db, worker_id=worker_id, now=now_fn(), lease_seconds=lease_seconds)
    if job is None:
        return False

    started = time.perf_counter()  # wall-clock duration for the log, independent of now_fn
    try:
        result = _execute(job, handlers)
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        outcome = f"failed ({error})"
        written = record_failure(
            db, job.id, worker_id=worker_id, attempt=job.attempts, error=error, now=now_fn()
        )
    else:
        outcome = "done"
        written = complete_job(
            db, job.id, worker_id=worker_id, attempt=job.attempts, result=result, now=now_fn()
        )

    elapsed_ms = (time.perf_counter() - started) * 1000
    log.info(
        "job %d %s attempt %d/%d %s in %.1f ms",
        job.id,
        job.type,
        job.attempts,
        job.max_attempts,
        outcome,
        elapsed_ms,
    )
    if not written:
        log.warning(
            "job %d: lease lost before attempt %d was recorded; another worker owns it now,"
            " so this outcome was discarded",
            job.id,
            job.attempts,
        )
    return True


def run_worker(
    db: DbPath,
    *,
    worker_id: str,
    poll_interval: float = 0.25,
    exit_when_idle: bool = False,
    max_jobs: int | None = None,
    should_stop: Callable[[], bool] = lambda: False,
    sleep_fn: Callable[[float], Any] = time.sleep,
    **run_once_kwargs: Any,
) -> int:
    """Process jobs until stopped; return how many were processed.

    Stops when `should_stop()` is true, after `max_jobs` jobs, or, with
    `exit_when_idle`, once nothing is pending or running.
    """
    if poll_interval < 0:
        raise ValueError(f"poll_interval must be >= 0, got {poll_interval!r}")
    if max_jobs is not None and max_jobs < 1:
        raise ValueError(f"max_jobs must be >= 1, got {max_jobs!r}")

    log.info("worker %s started", worker_id)
    processed = 0
    reason = "stop requested"
    while not should_stop():
        if max_jobs is not None and processed >= max_jobs:
            reason = f"reached max_jobs={max_jobs}"
            break
        if run_once(db, worker_id=worker_id, **run_once_kwargs):
            processed += 1
            continue
        # Nothing is ready right now. Pending jobs may be waiting out a delay,
        # and running ones may still come back if their worker dies, so "idle"
        # means nothing unfinished at all.
        if exit_when_idle and not has_unfinished(db):
            reason = "queue drained"
            break
        sleep_fn(poll_interval)
    log.info("worker %s stopped (%s) after %d job(s)", worker_id, reason, processed)
    return processed
