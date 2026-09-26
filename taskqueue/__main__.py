"""Allow `python -m taskqueue ...` as an alias for the `taskqueue` console script."""

from __future__ import annotations

from taskqueue.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
