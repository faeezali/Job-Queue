"""Tests for the CLI, run in-process through main() with captured output."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest
from conftest import SAMPLES

from taskqueue.cli import main
from taskqueue.queue import claim_next, enqueue, get_job, list_jobs


def run(db: Path, *argv: str) -> int:
    return main(["--db", str(db), *argv])


def test_submit_with_path_stores_an_absolute_path(
    db_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert run(db_path, "submit", "csv_summary", "--path", str(SAMPLES / "orders.csv")) == 0
    assert capsys.readouterr().out == "submitted job 1 (csv_summary)\n"
    job = get_job(db_path, 1)
    assert job is not None and job.payload == {"path": str((SAMPLES / "orders.csv").resolve())}


def test_submit_with_payload_and_options(db_path: Path) -> None:
    code = run(
        db_path, "submit", "flaky", "--payload", '{"fail_times": 2}', "--max-attempts", "3",
        "--delay", "5",
    )  # fmt: skip
    assert code == 0
    job = get_job(db_path, 1)
    assert job is not None
    assert (job.payload, job.max_attempts) == ({"fail_times": 2}, 3)
    assert job.available_at == pytest.approx(job.created_at + 5)


@pytest.mark.parametrize(
    ("argv", "message"),
    [
        (["submit", "nope", "--payload", "{}"], "unknown job type 'nope'"),
        (["submit", "sleep", "--payload", "{not json"], "--payload is not valid JSON"),
        (["submit", "sleep", "--payload", '{"seconds": 999}'], "between 0 and 300"),
        (["submit", "sleep", "--payload", '{"seconds": 1}', "--max-attempts", "0"], "max_attempts"),
        (["submit", "sleep", "--payload", '{"seconds": 1}', "--delay", "-1"], "delay"),
        (["submit", "sleep"], "missing seconds"),
    ],
)
def test_submit_user_errors_exit_2_without_traceback(
    db_path: Path, capsys: pytest.CaptureFixture[str], argv: list[str], message: str
) -> None:
    assert run(db_path, *argv) == 2
    err = capsys.readouterr().err
    assert message in err and "Traceback" not in err
    assert list_jobs(db_path) == []


def test_argparse_errors_exit_2(db_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    both = ["submit", "csv_summary", "--path", "a.csv", "--payload", "{}"]
    assert run(db_path, *both) == 2
    assert "not allowed with argument" in capsys.readouterr().err
    assert run(db_path, "list", "--status", "lost") == 2
    assert run(db_path, "bogus") == 2
    assert main([]) == 2


def test_help_exits_0(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["--help"]) == 0
    assert "submit" in capsys.readouterr().out


def test_list_shows_an_aligned_table(db_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    run(db_path, "submit", "csv_summary", "--path", str(SAMPLES / "malformed.csv"))
    run(db_path, "worker", "--max-jobs", "1")  # job 1 fails
    # Job 2 is held by a lease that ended long ago, as a crashed worker's would be.
    enqueue(db_path, "sleep", {"seconds": 0}, now_fn=lambda: 0.0)
    claim_next(db_path, worker_id="gone", now=0.0, lease_seconds=1.0)
    run(db_path, "submit", "sleep", "--payload", '{"seconds": 0}', "--delay", "120")
    capsys.readouterr()

    assert run(db_path, "list") == 0
    lines = capsys.readouterr().out.splitlines()
    assert lines[0].split() == ["ID", "TYPE", "STATUS", "ATTEMPTS", "WHEN", "LAST_ERROR"]
    by_id = {line.split()[0]: line for line in lines[1:]}
    assert list(by_id) == ["3", "2", "1"]  # newest first
    assert "pending" in by_id["3"] and "ready in 1" in by_id["3"]
    assert "running" in by_id["2"] and "lease expired" in by_id["2"]
    assert "failed" in by_id["1"] and "ValueError: line 2: row has 4 fields" in by_id["1"]
    assert by_id["1"].rstrip().endswith("...")  # the error is truncated
    # Aligned: every row's STATUS column starts where the header's does.
    status_col = lines[0].index("STATUS")
    assert all(
        line[status_col:].split()[0] in {"pending", "running", "failed"} for line in lines[1:]
    )


def test_list_filters_and_handles_empty(db_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert run(db_path, "list") == 0
    assert capsys.readouterr().out == "no jobs\n"
    run(db_path, "submit", "sleep", "--payload", '{"seconds": 0}')
    capsys.readouterr()
    assert run(db_path, "list", "--status", "done") == 0
    assert capsys.readouterr().out == "no jobs\n"
    assert run(db_path, "list", "--limit", "0") == 2


def test_show_prints_pretty_json(db_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    run(db_path, "submit", "flaky", "--payload", '{"fail_times": 0}')
    capsys.readouterr()
    assert run(db_path, "show", "1") == 0
    out = capsys.readouterr().out
    shown = json.loads(out)
    assert shown["id"] == 1 and shown["payload"] == {"fail_times": 0}
    assert shown["status"] == "pending"
    assert out.startswith('{\n  "id": 1,')


def test_show_missing_job_exits_1(db_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert run(db_path, "show", "42") == 1
    assert "job 42 not found" in capsys.readouterr().err


def test_worker_processes_jobs_and_exits_0(db_path: Path) -> None:
    run(db_path, "submit", "csv_summary", "--path", str(SAMPLES / "orders.csv"))
    run(db_path, "submit", "sleep", "--payload", '{"seconds": 0}')
    assert run(db_path, "worker", "--exit-when-idle", "--worker-id", "cli-test") == 0
    jobs = list_jobs(db_path)
    assert [(j.status, j.worker_id) for j in jobs] == [("done", "cli-test")] * 2


def test_worker_rejects_bad_options(db_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert run(db_path, "worker", "--max-jobs", "0") == 2
    assert run(db_path, "worker", "--poll-interval", "-1") == 2
    run(db_path, "submit", "sleep", "--payload", '{"seconds": 0}')
    assert run(db_path, "worker", "--lease-seconds", "0", "--exit-when-idle") == 2
    assert "Traceback" not in capsys.readouterr().err


def test_db_defaults_to_taskqueue_db_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    env_db = tmp_path / "from-env.db"
    monkeypatch.setenv("TASKQUEUE_DB", str(env_db))
    assert main(["submit", "sleep", "--payload", '{"seconds": 0}']) == 0
    assert get_job(env_db, 1) is not None


def test_every_command_initializes_the_db(tmp_path: Path) -> None:
    fresh = tmp_path / "fresh.db"
    assert main(["--db", str(fresh), "list"]) == 0
    assert list_jobs(fresh) == []


def test_old_schema_db_exits_2_with_instructions(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    old = tmp_path / "old.db"
    with closing(sqlite3.connect(old)) as conn:
        conn.execute("CREATE TABLE jobs (id INTEGER PRIMARY KEY)")
    assert main(["--db", str(old), "list"]) == 2
    err = capsys.readouterr().err
    assert "delete the local DB file" in err and "Traceback" not in err


def test_unopenable_db_exits_1(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["--db", str(tmp_path), "list"]) == 1  # a directory, not a file
    err = capsys.readouterr().err
    assert "error" in err and "Traceback" not in err
