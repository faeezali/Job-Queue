"""Throughput benchmark: time W real worker processes draining N `sleep` jobs.

    python -m scripts.bench --jobs 500 --workers 1 2 4 --work-ms 0 10

For every (workers, work_ms) pair: a fresh temp database, N sleep jobs of
work_ms each, W `taskqueue worker --exit-when-idle` subprocesses, the wall time
from starting the workers until the last one exits (process startup included),
and a check that every job ended done on its first attempt.
"""

from __future__ import annotations

import argparse
import itertools
import subprocess
import sys
import tempfile
import time
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

from taskqueue.db import connect, init_db
from taskqueue.queue import enqueue


@dataclass(frozen=True)
class BenchResult:
    workers: int
    work_ms: float
    jobs: int
    seconds: float
    verified: bool

    @property
    def jobs_per_sec(self) -> float:
        return self.jobs / self.seconds


def verify(db: Path, jobs: int) -> bool:
    """True if exactly `jobs` jobs exist and all finished done on attempt 1."""
    with closing(connect(db)) as conn:
        total, good = conn.execute(
            "SELECT COUNT(*), SUM(status = 'done' AND attempts = 1) FROM jobs"
        ).fetchone()
    return total == jobs and good == jobs


def run_config(jobs: int, workers: int, work_ms: float) -> BenchResult:
    with tempfile.TemporaryDirectory(prefix="taskqueue-bench-") as tmp:
        db = Path(tmp) / "bench.db"
        init_db(db)
        for _ in range(jobs):
            enqueue(db, "sleep", {"seconds": work_ms / 1000})

        command = [sys.executable, "-m", "taskqueue", "--db", str(db), "worker", "--exit-when-idle"]
        started = time.perf_counter()
        procs = [
            subprocess.Popen(
                [*command, "--worker-id", f"bench-{i}"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            for i in range(workers)
        ]
        codes = [proc.wait() for proc in procs]
        seconds = time.perf_counter() - started
        verified = all(code == 0 for code in codes) and verify(db, jobs)
    return BenchResult(workers, work_ms, jobs, seconds, verified)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark TaskQueue worker throughput.")
    parser.add_argument("--jobs", type=int, default=500)
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4])
    parser.add_argument("--work-ms", type=float, nargs="+", default=[0, 10])
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    header = (
        f"{'workers':>7}  {'work_ms':>7}  {'jobs':>5}  {'seconds':>7}  {'jobs/sec':>8}  verified"
    )
    print(header)
    all_verified = True
    for work_ms, workers in itertools.product(args.work_ms, args.workers):
        result = run_config(args.jobs, workers, work_ms)
        all_verified &= result.verified
        print(
            f"{result.workers:>7}  {result.work_ms:>7g}  {result.jobs:>5}  {result.seconds:>7.2f}"
            f"  {result.jobs_per_sec:>8.1f}  {'yes' if result.verified else 'NO'}",
            flush=True,
        )
    return 0 if all_verified else 1


if __name__ == "__main__":
    raise SystemExit(main())
