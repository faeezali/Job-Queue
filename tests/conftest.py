"""Shared fixtures: a fresh database, a controllable clock, and a CSV writer."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from taskqueue.db import init_db

REPO_ROOT = Path(__file__).resolve().parent.parent
SAMPLES = REPO_ROOT / "samples"


class FakeClock:
    """A callable stand-in for time.time that only moves when told to."""

    def __init__(self, start: float = 100.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> float:
        self.now += seconds
        return self.now


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """An initialized database file in a per-test temp directory."""
    path = tmp_path / "jobs.db"
    init_db(path)
    return path


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def write_csv(tmp_path: Path) -> Callable[..., Path]:
    """Write text to a CSV file byte-for-byte (so tests control newlines and BOMs)."""

    def write(text: str, name: str = "data.csv", encoding: str = "utf-8") -> Path:
        path = tmp_path / name
        path.write_bytes(text.encode(encoding))
        return path

    return write
