# TaskQueue

A persistent, single-machine background job queue backed by SQLite, using only the
Python standard library.

Producers submit jobs (a type plus a JSON payload) to a SQLite file. Separate worker
processes claim jobs atomically, run a handler, and store the result.

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest -q

taskqueue submit csv_summary --path samples/orders.csv
taskqueue worker --exit-when-idle
taskqueue list
taskqueue show 1
```

## License

MIT — see [LICENSE](LICENSE).
