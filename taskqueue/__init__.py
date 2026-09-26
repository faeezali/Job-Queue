"""TaskQueue: a persistent, single-machine background job queue backed by SQLite."""

from __future__ import annotations

import logging

from taskqueue.db import init_db
from taskqueue.handlers import HANDLERS, HandlerSpec, JobContext, summarize_csv
from taskqueue.queue import (
    Job,
    claim_next,
    complete_job,
    enqueue,
    get_job,
    has_unfinished,
    list_jobs,
    record_failure,
)
from taskqueue.worker import run_once, run_worker

__version__ = "0.1.0"

# Library modules only log; applications (like cli.py) decide where logs go.
logging.getLogger(__name__).addHandler(logging.NullHandler())

__all__ = [
    "HANDLERS",
    "HandlerSpec",
    "Job",
    "JobContext",
    "__version__",
    "claim_next",
    "complete_job",
    "enqueue",
    "get_job",
    "has_unfinished",
    "init_db",
    "list_jobs",
    "record_failure",
    "run_once",
    "run_worker",
    "summarize_csv",
]
