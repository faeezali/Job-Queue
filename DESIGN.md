# TaskQueue design notes

TaskQueue is a background job queue for one machine: producers write jobs to a SQLite
file, and separate worker processes claim them, run them, and record the outcome. This
file explains the decisions that shape that design. Each one gives the decision, why it
was made, the main alternative, and what it costs.

## 1. SQLite as the only moving part

**Decision.** Store the queue in one SQLite file, accessed only through Python's
standard-library `sqlite3` module.

**Why.** The target is a single machine with modest throughput. SQLite gives durable,
transactional storage with zero setup: no server to install, run, secure, or back up, and
no runtime dependencies. A job survives crashes and reboots because it is a committed row.

**Alternative.** Redis (with lists or streams) or Postgres (with `SELECT ... FOR UPDATE
SKIP LOCKED`).

**Trade-off.** SQLite allows one writer at a time for the whole database, so write-heavy
throughput stops improving after a few workers (see the benchmark in the README), and
the file cannot safely be shared across machines. Redis or Postgres would scale further
and work across hosts, at the cost of running and operating a server.

## 2. Every write in a `BEGIN IMMEDIATE` transaction, on a WAL database

**Decision.** `transaction()` opens every write with `BEGIN IMMEDIATE`, and `init_db`
puts the database in WAL mode. Connections use `isolation_level=None`, so the `sqlite3`
module never starts transactions on its own.

**Why.** Claiming a job is a read followed by a write: find the oldest claimable row,
then mark it running. `IMMEDIATE` takes the write lock before that first read, so two
workers cannot both see the same row as free. The default (`DEFERRED`) would let both
read, and then one would fail with `SQLITE_BUSY` when it tried to upgrade to a write
lock, which the busy timeout cannot fix. WAL lets readers such as `list`, `stats`, and
the workers' idle checks proceed while a writer holds the lock, and each commit only
appends to the log.

**Alternative.** Optimistic claiming: a plain `UPDATE ... WHERE id = ? AND status =
'pending'` in autocommit mode, retried when it matches no row. Or rollback-journal mode.

**Trade-off.** Writers are strictly serialized and wait on each other (up to the 10 s
busy timeout). WAL adds `-wal` and `-shm` files next to the database and requires every
process to be on the same host, which is already a constraint here.

## 3. Leases instead of heartbeats

**Decision.** A claim sets `lease_expires_at = now + lease_seconds` (30 s by default).
Once the lease expires, any worker may reclaim the job, and that counts as a new attempt.
Workers never extend a lease.

**Why.** Leases are the only way to recover a job from a worker that was `kill -9`'d,
lost power, or hung, because such a worker cannot report anything. A fixed lease needs
no background thread, which keeps the "no threads" rule and makes a worker one simple
loop.

**Alternative.** Heartbeats: a thread (or periodic checks inside handlers) extends the
lease while the job runs, so the lease can be short.

**Trade-off.** The lease must be longer than the slowest job's runtime. If a healthy job
runs past its lease, a second worker starts the same job while the first is still going.
Recovery from a crash also takes up to one full lease. Heartbeats would allow short
leases and fast recovery, but need concurrency inside the worker.

## 4. Fenced completion writes

**Decision.** `complete_job` and `record_failure` update only
`WHERE id = ? AND status = 'running' AND worker_id = ? AND attempts = ?`, and return
whether a row changed. The worker logs a WARNING when it did not.

**Why.** Because of decision 3, a slow worker can finish after its job has been
reclaimed. Without a fence, its late write would overwrite the new owner's state, for
example marking a job done while attempt 2 is still running, or clobbering attempt 2's
result. `attempts` works as a fencing token: it increases on every claim, so it still
rejects a stale write even when a restarted worker reuses the same `worker_id`.

**Alternative.** Trust whoever writes last, or fence on `worker_id` alone.

**Trade-off.** The stale worker's work is discarded even if it was correct, and its side
effects have already happened (see decision 5). The fence protects the database, not the
outside world.

## 5. At-least-once delivery

**Decision.** TaskQueue promises that every job runs at least once. It does not promise
that a job runs exactly once.

**Why.** A worker can crash after a handler has done its work but before the outcome is
committed. Without a commit protocol shared with the handler's side effects,
at-least-once is the strongest guarantee available. Given the choice, running a job
twice is better than silently losing it.

**Alternative.** At-most-once (mark the job done before running it), or exactly-once
through idempotency keys or transactional outboxes built into the handlers.

**Trade-off.** Handlers must be idempotent, or their duplicates must be harmless. The
demo handlers are: `csv_summary` only reads, `sleep` has no effects, and `flaky` depends
only on the attempt number.

## 6. Classify errors by exception type

**Decision.** `is_retryable` returns False for `PermanentJobError`, `ValueError`,
`TypeError`, `KeyError`, `FileNotFoundError`, `IsADirectoryError`, `NotADirectoryError`,
and `PermissionError`, and True for everything else. An unknown job type, an invalid
payload, and an invalid result (`InvalidResult`, a `PermanentJobError`) all fall on the
permanent side.

**Why.** Retrying only helps when the cause is temporary, such as a busy resource or a
flaky network. Bad input or a bug fails the same way every time, and retrying it just
burns attempts and delays the failure report. Classifying by exception type needs no
cooperation from handlers, and `PermanentJobError` gives them an explicit way to opt out
of retries.

**Alternative.** Retry everything up to `max_attempts`, or have handlers return a status
code.

**Trade-off.** The classification is a heuristic. A `KeyError` caused by a transient race
will not be retried, and a `RuntimeError` caused by a bug will be retried until attempts
run out. `UnicodeDecodeError` is a `ValueError`, so an undecodable CSV fails at once,
which is the intended behavior.

## 7. Exponential backoff with jitter, stored in the row

**Decision.** After failed attempt *n*, the worker computes
`min(max_delay, base_delay * 2**(n-1))`, scales it by a random factor in
`[1 - jitter, 1 + jitter]` (defaults 2 s, 60 s, 0.2), and stores `available_at = now +
delay`. The worker itself never sleeps on a failure.

**Why.** Growing delays give a struggling dependency time to recover. Jitter keeps many
jobs that failed together from retrying at the same instant. Keeping the wait in the
database means a waiting retry blocks neither the worker nor other ready jobs, and the
wait survives restarts.

**Alternative.** A fixed delay, or sleeping inside the worker before retrying.

**Trade-off.** Retry timing is only as precise as the poll interval (0.25 s by default),
and the backoff settings belong to whichever worker handled the failure, not to the job.

## 8. Inject the clock, randomness, and sleep

**Decision.** `run_once` and `run_worker` take `now_fn`, `rand_fn`, and `sleep_fn`, and
the queue functions take `now` as an explicit argument. Tests pass a `FakeClock` that
starts at 100.0 and `rand_fn=lambda: 0.5`, which makes jitter neutral.

**Why.** Retries and leases are about time. With injected time, the tests can check exact
boundaries (a retry due at 102.0 is not claimable at 101.9 but is at 102.0) and exact
backoff sequences (2, 4, 8, 16) without real sleeps, so they are fast and deterministic.

**Alternative.** Patching `time.time` with a mocking library, or tests that really sleep.

**Trade-off.** The signatures carry a few extra parameters, and library code has to
call the injected functions instead of `time` directly. The one exception is the
duration in the worker's log line, which uses `time.perf_counter` because it reports
real elapsed time.

## 9. One short-lived connection per operation

**Decision.** Every public queue function opens its own connection and closes it in a
`finally` block, through `contextlib.closing`.

**Why.** No connection is ever held across a handler run (which can take minutes) or a
poll sleep, so a worker never holds a lock or a read snapshot while it is not using the
database. The functions stay self-contained and take only a path, which suits separate
processes.

**Alternative.** A long-lived connection per worker, or a connection pool.

**Trade-off.** Each call pays the cost of opening a connection and reading the schema,
and a job makes at least two calls (claim and complete). That cost is included in the
roughly 1 to 1.7 ms per job that a single worker measured at 0 ms of work (587 to 925
jobs/s across runs); it was not measured separately.

## 10. No migrations: a version check instead

**Decision.** The schema is version 1, recorded in `PRAGMA user_version`. `init_db`
creates it on a fresh file and raises a `RuntimeError` that says to delete the local DB
file when a `jobs` table exists with any other version, such as the old TaskForge
schema.

**Why.** Queue data is transient working state, and this is version 0.1.0. A refusal
with a clear message is simple and safe, while a migration framework would be most of the
code in the project.

**Alternative.** Versioned migration scripts that upgrade existing files in place.

**Trade-off.** A future schema change means draining the queue (or giving up its
contents) before upgrading.

## Smaller choices made while building

These choices filled gaps in the spec. In each case the simplest correct option was
taken.

- **Strict payloads.** Validators also reject unknown keys, so a typo like
  `{"fail_time": 2}` fails at submit time instead of running with defaults. `bool` is
  rejected wherever a number is expected, and `sleep` rejects NaN and infinity.
- **Re-validation in the worker.** The worker runs `spec.validate` on the stored payload
  before `spec.run`, because rows can come from other producers. A bad row then fails
  permanently as a `ValueError` instead of crashing the handler.
- **`csv.Error` becomes `ValueError`.** Malformed quoting will not fix itself, so
  `summarize_csv` reports it as bad input, with the line number.
- **`InvalidResult` is an exception class.** It is a `PermanentJobError` defined in
  `errors.py`, so an invalid result goes through the same `"{type}: {message}"` path as
  every other failure. A result also has to survive `json.dumps(..., allow_nan=False)`,
  which keeps the stored JSON standard.
- **`worker_id` is kept on done jobs,** so `show` reports who finished the job. A
  failure, a retry, or `LeaseExpired` clears it, as the spec requires for
  `record_failure`.
- **`list` shows the newest jobs first,** so its default `--limit 50` shows recent
  activity instead of the oldest history.
- **Logging.** The CLI logs at INFO by default, so the worker's one line per attempt is
  visible; `-v` switches to DEBUG. Library modules log only DEBUG, WARNING (reclaims,
  `LeaseExpired`, lost leases), and the worker's INFO lines.
- **Exit codes.** 0 for success. 1 for a missing job id, or for a database or OS error
  such as an unopenable file. 2 for bad input, including argparse errors, invalid values,
  `retry` on a job that is not failed, and a schema mismatch. 130 when a second Ctrl-C
  aborts the worker. `main()` returns argparse's exit codes instead of letting
  `SystemExit` escape.
- **Signal handling.** The handler sets a `threading.Event` (used only as a flag, with no
  threads involved) and writes its notice with `os.write(2, ...)`. `print` and `logging`
  are unsafe there, because the handler can interrupt a write already in progress on
  the same stream. The worker checks the flag between jobs and never blocks in
  `Event.wait()`. The previous signal handlers are restored when the worker returns.
- **`RetryPolicy` validates its fields.** Delays must be finite and at least 0, and
  jitter must be between 0 and 1. It caps the exponent at 64 so huge attempt numbers
  cannot overflow. A base delay of 0 is allowed, and the CLI tests use it to retry
  without waiting.
- **`enqueue`'s second parameter is named `job_type`,** not `type`, to avoid shadowing
  the builtin. It is positional, so calls look the same as in the spec.
- **SQL.** Every runtime value is bound with `?`. The fixed status names (`'running'`
  and so on) are literals in the SQL text. `PRAGMA user_version = 1` is built from a code
  constant, because PRAGMAs cannot take bound parameters. The DDL runs statement by
  statement inside the `IMMEDIATE` transaction, because `executescript()` would commit
  first and let two processes race to create the schema.
- **Packaging.** `scripts/__init__.py` makes `python -m scripts.bench` import this
  repository's `scripts` rather than any other installed package with that name.
  `pyproject.toml` uses the SPDX `license = "MIT"` form, which needs setuptools 77 or
  later. `.gitignore` was replaced with exactly the entries listed in the spec.
