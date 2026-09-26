"""Job handlers: the CSV summarizer, two demo handlers, and the registry that names them.

Every handler is a pair of plain functions. `validate` runs at submit time so
bad input is rejected before it reaches the database; `run` does the work
inside a worker and returns a JSON-serializable dict.
"""

from __future__ import annotations

import csv
import math
import os
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from taskqueue.errors import TransientDemoError, UnknownJobTypeError

Payload = dict[str, Any]
Result = dict[str, Any]

MAX_SLEEP_SECONDS = 300


@dataclass(frozen=True)
class JobContext:
    """What a handler knows about the attempt it is running."""

    job_id: int
    attempt: int
    max_attempts: int


@dataclass(frozen=True)
class HandlerSpec:
    """A job type's implementation: `validate` normalizes input, `run` executes it."""

    run: Callable[[Payload, JobContext], Result]
    validate: Callable[[Payload], Payload]


def summarize_csv(path: str | os.PathLike[str]) -> Result:
    """Count rows and missing cells per column in a CSV file with a header row.

    Raises ValueError for an empty file, a blank or duplicate header name, or a
    row whose field count differs from the header. OSError and
    UnicodeDecodeError propagate unchanged.
    """
    # utf-8-sig strips a leading BOM (common in Excel exports) so it doesn't
    # end up glued to the first column name; newline="" is what csv requires.
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        try:
            # Reading fieldnames consumes the header row; None/[] means no header.
            columns = reader.fieldnames
            if not columns:
                raise ValueError("CSV file is empty: expected a header row")
            header_line = reader.line_num
            for position, name in enumerate(columns, start=1):
                if not name.strip():
                    raise ValueError(f"line {header_line}: column {position} has a blank header")
            duplicates = sorted({name for name in columns if columns.count(name) > 1})
            if duplicates:
                raise ValueError(
                    f"line {header_line}: duplicate header name(s): {', '.join(duplicates)}"
                )

            missing = dict.fromkeys(columns, 0)
            row_count = 0
            for row in reader:
                # DictReader files surplus fields under the key None and fills
                # absent fields with the value None; both mean a ragged row.
                if None in row:
                    field_count = len(columns) + len(row[None])
                    raise ValueError(
                        f"line {reader.line_num}: row has {field_count} fields "
                        f"but the header has {len(columns)}"
                    )
                present = [name for name in columns if row[name] is not None]
                if len(present) != len(columns):
                    raise ValueError(
                        f"line {reader.line_num}: row has {len(present)} fields "
                        f"but the header has {len(columns)}"
                    )
                row_count += 1
                for name in columns:
                    if not row[name].strip():
                        missing[name] += 1
        except csv.Error as exc:
            # Malformed quoting won't fix itself on retry, so report it as bad input.
            raise ValueError(f"line {reader.line_num}: {exc}") from exc

    return {"row_count": row_count, "columns": list(columns), "missing_by_column": missing}


def _require_keys(payload: Any, job_type: str, keys: set[str]) -> Payload:
    """Check that the payload is a JSON object with exactly `keys`."""
    if not isinstance(payload, dict):
        raise ValueError(f"{job_type}: payload must be a JSON object, got {type(payload).__name__}")
    missing = keys - payload.keys()
    if missing:
        raise ValueError(f"{job_type}: payload is missing {', '.join(sorted(missing))}")
    # Rejecting unknown keys catches typos like "fail_time" at submit time.
    unexpected = payload.keys() - keys
    if unexpected:
        raise ValueError(
            f"{job_type}: unexpected payload key(s): {', '.join(sorted(map(str, unexpected)))}"
        )
    return payload


def _is_number(value: Any) -> bool:
    # bool is a subclass of int, but `true` is never a sensible count or duration.
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _validate_csv_summary(payload: Payload) -> Payload:
    path = _require_keys(payload, "csv_summary", {"path"})["path"]
    if not isinstance(path, str) or not path.strip():
        raise ValueError("csv_summary: 'path' must be a non-empty string")
    # Resolve now, against the submitter's working directory, because the
    # worker may run from a different one. The file need not exist yet.
    return {"path": str(Path(path).resolve())}


def _run_csv_summary(payload: Payload, ctx: JobContext) -> Result:
    return summarize_csv(payload["path"])


def _validate_flaky(payload: Payload) -> Payload:
    fail_times = _require_keys(payload, "flaky", {"fail_times"})["fail_times"]
    if not isinstance(fail_times, int) or isinstance(fail_times, bool) or fail_times < 0:
        raise ValueError("flaky: 'fail_times' must be an integer >= 0")
    return {"fail_times": fail_times}


def _run_flaky(payload: Payload, ctx: JobContext) -> Result:
    fail_times = payload["fail_times"]
    if ctx.attempt <= fail_times:
        raise TransientDemoError(f"planned failure {ctx.attempt} of {fail_times}")
    return {"succeeded_on_attempt": ctx.attempt}


def _validate_sleep(payload: Payload) -> Payload:
    seconds = _require_keys(payload, "sleep", {"seconds"})["seconds"]
    # math.isfinite rejects NaN, which would otherwise slip past both comparisons.
    if not _is_number(seconds) or not math.isfinite(seconds):
        raise ValueError("sleep: 'seconds' must be a number")
    if not 0 <= seconds <= MAX_SLEEP_SECONDS:
        raise ValueError(f"sleep: 'seconds' must be between 0 and {MAX_SLEEP_SECONDS}")
    return {"seconds": seconds}


def _run_sleep(payload: Payload, ctx: JobContext) -> Result:
    time.sleep(payload["seconds"])
    return {"slept": payload["seconds"]}


HANDLERS: dict[str, HandlerSpec] = {
    "csv_summary": HandlerSpec(run=_run_csv_summary, validate=_validate_csv_summary),
    "flaky": HandlerSpec(run=_run_flaky, validate=_validate_flaky),
    "sleep": HandlerSpec(run=_run_sleep, validate=_validate_sleep),
}


def validate_payload(
    job_type: str, payload: Any, handlers: Mapping[str, HandlerSpec] = HANDLERS
) -> Payload:
    """Return the normalized payload for `job_type`, or raise ValueError.

    Raises UnknownJobTypeError (a ValueError) if no handler has that name.
    """
    spec = handlers.get(job_type)
    if spec is None:
        known = ", ".join(sorted(handlers))
        raise UnknownJobTypeError(f"unknown job type {job_type!r} (known types: {known})")
    return spec.validate(payload)
