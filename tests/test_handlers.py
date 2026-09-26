"""Tests for summarize_csv, the demo handlers, and payload validation."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest
from conftest import SAMPLES

from taskqueue.errors import TransientDemoError, UnknownJobTypeError
from taskqueue.handlers import HANDLERS, JobContext, summarize_csv, validate_payload

WriteCsv = Callable[..., Path]


def ctx(attempt: int = 1, max_attempts: int = 1) -> JobContext:
    return JobContext(job_id=1, attempt=attempt, max_attempts=max_attempts)


# --- summarize_csv -----------------------------------------------------------


def test_orders_sample_gives_the_exact_summary() -> None:
    assert summarize_csv(SAMPLES / "orders.csv") == {
        "row_count": 3,
        "columns": ["item", "quantity", "price"],
        "missing_by_column": {"item": 0, "quantity": 1, "price": 1},
    }


def test_header_only_file_has_zero_rows(write_csv: WriteCsv) -> None:
    assert summarize_csv(write_csv("a,b\n")) == {
        "row_count": 0,
        "columns": ["a", "b"],
        "missing_by_column": {"a": 0, "b": 0},
    }


def test_whitespace_only_cells_count_as_missing(write_csv: WriteCsv) -> None:
    result = summarize_csv(write_csv("a,b\n  ,x\n\t,\n"))
    assert result["missing_by_column"] == {"a": 2, "b": 1}


def test_utf8_bom_is_not_part_of_the_first_column(write_csv: WriteCsv) -> None:
    path = write_csv("item,qty\npen,1\n", encoding="utf-8-sig")
    assert path.read_bytes().startswith(b"\xef\xbb\xbf")
    assert summarize_csv(path)["columns"] == ["item", "qty"]


def test_blank_lines_between_rows_are_skipped(write_csv: WriteCsv) -> None:
    assert summarize_csv(write_csv("a\n1\n\n2\n"))["row_count"] == 2


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("", "empty"),
        ("\n", "empty"),
        ("a,,c\n", "line 1: column 2 has a blank header"),
        ("a, ,c\n", "line 1: column 2 has a blank header"),
        ("a,b,a\n", "line 1: duplicate header name.*: a"),
        ("a,b\n1,2\n3,4,5\n", "line 3: row has 3 fields but the header has 2"),
        ("a,b,c\n1,2,3\n4,5\n", "line 3: row has 2 fields but the header has 3"),
    ],
    ids=["empty", "blank-line", "blank-header", "space-header", "dup-header", "extra", "missing"],
)
def test_malformed_csv_raises_value_error(write_csv: WriteCsv, text: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        summarize_csv(write_csv(text))


def test_malformed_sample_fails_on_its_extra_field() -> None:
    with pytest.raises(ValueError, match="line 2: row has 4 fields but the header has 3"):
        summarize_csv(SAMPLES / "malformed.csv")


def test_missing_file_propagates(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        summarize_csv(tmp_path / "nope.csv")


def test_undecodable_file_propagates(tmp_path: Path) -> None:
    path = tmp_path / "latin1.csv"
    path.write_bytes("name\ncafé\n".encode("latin-1"))
    with pytest.raises(UnicodeDecodeError):
        summarize_csv(path)


# --- validators --------------------------------------------------------------


def test_csv_summary_resolves_a_relative_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    normalized = validate_payload("csv_summary", {"path": "later.csv"})  # need not exist yet
    assert normalized == {"path": str((tmp_path / "later.csv").resolve())}
    assert Path(normalized["path"]).is_absolute()


@pytest.mark.parametrize(
    "payload",
    [{}, {"path": ""}, {"path": "   "}, {"path": 3}, {"path": "a.csv", "extra": 1}, ["a.csv"]],
)
def test_csv_summary_rejects_bad_payloads(payload: object) -> None:
    with pytest.raises(ValueError):
        validate_payload("csv_summary", payload)


@pytest.mark.parametrize("fail_times", [0, 1, 5])
def test_flaky_accepts_non_negative_ints(fail_times: int) -> None:
    assert validate_payload("flaky", {"fail_times": fail_times}) == {"fail_times": fail_times}


@pytest.mark.parametrize(
    "payload",
    [{}, {"fail_times": -1}, {"fail_times": 1.5}, {"fail_times": "2"}, {"fail_times": True}],
)
def test_flaky_rejects_bad_payloads(payload: object) -> None:
    with pytest.raises(ValueError):
        validate_payload("flaky", payload)


@pytest.mark.parametrize("seconds", [0, 0.25, 300])
def test_sleep_accepts_0_to_300(seconds: float) -> None:
    assert validate_payload("sleep", {"seconds": seconds}) == {"seconds": seconds}


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"seconds": -0.1},
        {"seconds": 300.5},
        {"seconds": "1"},
        {"seconds": True},
        {"seconds": float("nan")},
        {"seconds": float("inf")},
        {"seconds": 1, "extra": 2},
    ],
)
def test_sleep_rejects_bad_payloads(payload: object) -> None:
    with pytest.raises(ValueError):
        validate_payload("sleep", payload)


def test_unknown_type_raises_unknown_job_type_error() -> None:
    with pytest.raises(UnknownJobTypeError, match="unknown job type 'nope'"):
        validate_payload("nope", {})
    assert issubclass(UnknownJobTypeError, ValueError)


# --- run functions -----------------------------------------------------------


def test_flaky_fails_through_fail_times_then_succeeds() -> None:
    run = HANDLERS["flaky"].run
    for attempt in (1, 2):
        with pytest.raises(TransientDemoError):
            run({"fail_times": 2}, ctx(attempt=attempt, max_attempts=3))
    assert run({"fail_times": 2}, ctx(attempt=3, max_attempts=3)) == {"succeeded_on_attempt": 3}


def test_sleep_returns_seconds_slept() -> None:
    assert HANDLERS["sleep"].run({"seconds": 0}, ctx()) == {"slept": 0}


def test_csv_summary_run_summarizes_the_file() -> None:
    payload = validate_payload("csv_summary", {"path": str(SAMPLES / "orders.csv")})
    assert HANDLERS["csv_summary"].run(payload, ctx())["row_count"] == 3
