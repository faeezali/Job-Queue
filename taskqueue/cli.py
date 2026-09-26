"""Command-line interface: `taskqueue submit | list | show | retry | stats | worker`.

The only module that prints. Exit codes: 0 success, 1 job not found or a
database/OS error, 2 bad user input.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import asdict

from taskqueue.db import init_db
from taskqueue.queue import (
    STATUSES,
    Job,
    count_by_status,
    enqueue,
    get_job,
    list_jobs,
    requeue,
)
from taskqueue.retry import RetryPolicy
from taskqueue.worker import default_worker_id, run_worker

EXIT_OK = 0
EXIT_NOT_FOUND = 1
EXIT_USAGE = 2

ERROR_WIDTH = 50


def _error(message: str) -> None:
    print(f"taskqueue: error: {message}", file=sys.stderr)


def _truncate(text: str, width: int) -> str:
    text = " ".join(text.split())  # tracebacks and CSV errors can contain newlines
    return text if len(text) <= width else text[: width - 3] + "..."


def _when(job: Job, now: float) -> str:
    """Describe when a job will next change state."""
    if job.status == "pending":
        wait = job.available_at - now
        return "ready" if wait <= 0 else f"ready in {wait:.1f}s"
    if job.status == "running" and job.lease_expires_at is not None:
        left = job.lease_expires_at - now
        return "lease expired" if left <= 0 else f"lease {left:.1f}s left"
    return "-"


def _format_table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    widths = [max(len(row[i]) for row in [headers, *rows]) for i in range(len(headers))]
    lines = [
        "  ".join(cell.ljust(width) for cell, width in zip(row, widths, strict=True)).rstrip()
        for row in [headers, *rows]
    ]
    return "\n".join(lines)


def cmd_submit(args: argparse.Namespace) -> int:
    if args.path is not None:
        payload: object = {"path": args.path}
    elif args.payload is not None:
        try:
            payload = json.loads(args.payload)
        except json.JSONDecodeError as exc:
            _error(f"--payload is not valid JSON: {exc}")
            return EXIT_USAGE
    else:
        payload = {}
    job_id = enqueue(args.db, args.type, payload, max_attempts=args.max_attempts, delay=args.delay)
    print(f"submitted job {job_id} ({args.type})")
    return EXIT_OK


def cmd_list(args: argparse.Namespace) -> int:
    jobs = list_jobs(args.db, status=args.status, limit=args.limit)
    if not jobs:
        print("no jobs")
        return EXIT_OK
    now = time.time()
    headers = ("ID", "TYPE", "STATUS", "ATTEMPTS", "WHEN", "LAST_ERROR")
    rows = [
        (
            str(job.id),
            job.type,
            job.status,
            f"{job.attempts}/{job.max_attempts}",
            _when(job, now),
            _truncate(job.last_error or "", ERROR_WIDTH),
        )
        for job in jobs
    ]
    print(_format_table(headers, rows))
    return EXIT_OK


def cmd_show(args: argparse.Namespace) -> int:
    job = get_job(args.db, args.id)
    if job is None:
        _error(f"job {args.id} not found")
        return EXIT_NOT_FOUND
    print(json.dumps(asdict(job), indent=2))
    return EXIT_OK


def cmd_retry(args: argparse.Namespace) -> int:
    if requeue(args.db, args.id, now=time.time()):
        print(f"job {args.id} requeued")
        return EXIT_OK
    job = get_job(args.db, args.id)
    if job is None:
        _error(f"job {args.id} not found")
        return EXIT_NOT_FOUND
    _error(f"job {args.id} is {job.status}; only failed jobs can be retried")
    return EXIT_USAGE


def cmd_stats(args: argparse.Namespace) -> int:
    counts = count_by_status(args.db)
    rows = [(status, str(count)) for status, count in counts.items()]
    rows.append(("total", str(sum(counts.values()))))
    width = max(len(status) for status, _ in rows)
    for status, count in rows:
        print(f"{status.ljust(width)}  {count}")
    return EXIT_OK


def cmd_worker(args: argparse.Namespace) -> int:
    retry_policy = RetryPolicy(base_delay=args.retry_base, max_delay=args.retry_max)
    run_worker(
        args.db,
        worker_id=args.worker_id or default_worker_id(),
        poll_interval=args.poll_interval,
        exit_when_idle=args.exit_when_idle,
        max_jobs=args.max_jobs,
        lease_seconds=args.lease_seconds,
        retry_policy=retry_policy,
    )
    return EXIT_OK


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="taskqueue", description="A persistent background job queue backed by SQLite."
    )
    parser.add_argument(
        "--db",
        default=os.environ.get("TASKQUEUE_DB", "taskqueue.db"),
        help="SQLite database file (default: $TASKQUEUE_DB or taskqueue.db)",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="log debug details")
    commands = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")

    submit = commands.add_parser("submit", help="add a job to the queue")
    submit.add_argument("type", help="job type: csv_summary, flaky, or sleep")
    payload = submit.add_mutually_exclusive_group()
    payload.add_argument("--path", help="shorthand for --payload '{\"path\": PATH}'")
    payload.add_argument("--payload", help="job payload as a JSON object")
    submit.add_argument("--max-attempts", type=int, default=1, help="default: 1")
    submit.add_argument("--delay", type=float, default=0.0, help="seconds before it may run")
    submit.set_defaults(func=cmd_submit)

    list_ = commands.add_parser("list", help="show recent jobs, newest first")
    list_.add_argument("--status", choices=STATUSES)
    list_.add_argument("--limit", type=int, default=50, help="default: 50")
    list_.set_defaults(func=cmd_list)

    show = commands.add_parser("show", help="print one job as JSON")
    show.add_argument("id", type=int)
    show.set_defaults(func=cmd_show)

    retry = commands.add_parser("retry", help="requeue a failed job with a fresh attempt budget")
    retry.add_argument("id", type=int)
    retry.set_defaults(func=cmd_retry)

    stats = commands.add_parser("stats", help="count jobs by status")
    stats.set_defaults(func=cmd_stats)

    worker = commands.add_parser("worker", help="process jobs until stopped")
    worker.add_argument("--worker-id", help="default: HOSTNAME-PID")
    worker.add_argument("--lease-seconds", type=float, default=30.0, help="default: 30")
    worker.add_argument("--poll-interval", type=float, default=0.25, help="default: 0.25")
    worker.add_argument(
        "--exit-when-idle", action="store_true", help="exit once nothing is pending or running"
    )
    worker.add_argument("--max-jobs", type=int, help="exit after processing this many jobs")
    worker.add_argument(
        "--retry-base", type=float, default=2.0, help="first retry delay in seconds (default: 2)"
    )
    worker.add_argument(
        "--retry-max", type=float, default=60.0, help="longest retry delay (default: 60)"
    )
    worker.set_defaults(func=cmd_worker)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI and return its exit code."""
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:  # argparse exits on --help (0) and on bad arguments (2)
        return exc.code if isinstance(exc.code, int) else EXIT_USAGE

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    func: Callable[[argparse.Namespace], int] = args.func
    try:
        try:
            init_db(args.db)
        except RuntimeError as exc:
            # Only init_db's schema-version check counts as user error; a
            # RuntimeError from anywhere else is a bug and should not be masked.
            _error(str(exc))
            return EXIT_USAGE
        return func(args)
    except ValueError as exc:
        _error(str(exc))
        return EXIT_USAGE
    except (sqlite3.Error, OSError) as exc:
        _error(f"{args.db}: {exc}")
        return EXIT_NOT_FOUND
    except KeyboardInterrupt:
        _error("interrupted")
        return 130
