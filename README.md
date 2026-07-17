# QueueCTL

A CLI-based background job queue system with multi-worker support, automatic
retries with exponential backoff, a Dead Letter Queue (DLQ), and persistent
storage across restarts.

Built for the Flam Backend Developer Internship Assignment.

---

## 1. Setup Instructions

**Requirements:** Node.js 18+ (uses `better-sqlite3`, which ships prebuilt
binaries for common platforms).

```bash
git clone <this-repo-url>
cd queuectl
npm install

# Run the CLI directly:
node bin/queuectl.js --help

# Or install it globally on your PATH as `queuectl`:
npm link
queuectl --help
```

All commands operate on a local `.queuectl/` folder created in the current
working directory, containing:
- `queuectl.db` — the SQLite database (jobs, config, worker registry)
- `worker-pids.json` — PIDs of currently running worker processes
- `logs/<worker_id>.log` — stdout/stderr log per worker process

Run all commands from the same directory so they operate on the same queue.

---

## 2. Usage Examples

### Enqueue a job
```bash
$ queuectl enqueue '{"id":"job1","command":"sleep 2"}'
Enqueued job "job1" (state: pending)
```
`id` is optional (auto-generated if omitted). Extra options:
```bash
queuectl enqueue '{"command":"curl https://example.com"}' \
  --max-retries 5 --priority 10 --timeout-ms 5000 --run-at 2026-08-01T00:00:00Z
```

### Start / stop workers
```bash
$ queuectl worker start --count 3
Started 3 worker(s):
  - worker_...  (pid 4821) -- log: .queuectl/logs/worker_....log
  - worker_...  (pid 4822) -- log: .queuectl/logs/worker_....log
  - worker_...  (pid 4823) -- log: .queuectl/logs/worker_....log

$ queuectl worker stop
Sent shutdown signal to 3 worker(s):
  - worker_... (pid 4821)
  ...
```
`worker stop` sends `SIGTERM` to every tracked worker. Each worker finishes
its **current** job before exiting — no in-flight job is ever abandoned.

### Status
```bash
$ queuectl status
Job states:
  pending    2
  processing 1
  completed  14
  failed     0
  dead       1

Active workers: 3
  - worker_a (pid 4821) processing job7
  - worker_b (pid 4822) idle
  - worker_c (pid 4823) idle
```

### List jobs
```bash
$ queuectl list --state pending
ID             STATE       ATTEMPTS      COMMAND                        UPDATED_AT
------------------------------------------------------------------------------------------------
job1           pending     0/3           sleep 2                       2026-07-17T04:13:14.231Z
```

### Dead Letter Queue
```bash
$ queuectl dlq list
ID             STATE       ATTEMPTS      COMMAND                        UPDATED_AT
------------------------------------------------------------------------------------------------
job-fail       dead        3/3           exit 1                        2026-07-17T04:24:59.930Z
    last_error: Command exited with code 1: Command failed: exit 1

$ queuectl dlq retry job-fail
Job "job-fail" requeued (state: pending)
```

### Configuration
```bash
$ queuectl config set max-retries 5
Config "max-retries" set to 5

$ queuectl config set backoff-base 3
Config "backoff-base" set to 3

$ queuectl config get
max-retries = 5
backoff-base = 3
poll-interval-ms = 500
```
Config changes apply to **new** jobs (and to jobs' next retry calculation);
existing job rows already store their own `max_retries` snapshot.

---

## 3. Architecture Overview

```
bin/queuectl.js       CLI entry point (commander) — thin, delegates to src/
src/db.js             SQLite connection + schema (WAL mode, busy_timeout)
src/config.js         Get/set persisted config (max-retries, backoff-base, ...)
src/backoff.js        delay = base ^ attempts (in seconds)
src/jobs.js           Job lifecycle: enqueue, atomic claim, complete, fail, DLQ
src/executor.js       Runs a job's shell command, captures output/exit code
src/worker.js         The actual worker process loop (spawned as a child process)
src/workerManager.js  Spawns/stops worker child processes, tracks PIDs
```

### Job lifecycle
```
pending --(claimed by worker)--> processing --(exit 0)--> completed
                                       |
                                (non-zero exit / error)
                                       v
                              attempts < max_retries?
                                 /            \
                              yes              no
                               |                |
                          pending            dead (DLQ)
                     (after backoff delay)
```

### Concurrency / locking
Multiple worker **processes** (not just threads) share one SQLite file. To
avoid two workers claiming the same job:

- SQLite is opened in **WAL mode** so readers/writers don't block each other
  needlessly, with `busy_timeout` so a writer that finds the DB locked
  retries instead of erroring immediately.
- Claiming a job is one `BEGIN IMMEDIATE` transaction (`better-sqlite3`'s
  `.immediate()` transaction mode) that does `SELECT ... LIMIT 1` then
  `UPDATE ... WHERE state = 'pending'` and checks `changes === 1`. Taking the
  write lock immediately (rather than at first write) closes the
  select-then-update race window, so only one worker process can ever win a
  given row — this is what prevents duplicate/overlapping processing (see
  test scenario 3: 12 jobs / 4 workers, no duplicates).

### Data persistence
All job, config, and worker-registry data lives in `.queuectl/queuectl.db`
(SQLite via `better-sqlite3`). Because every `queuectl` invocation is a
fresh Node process reopening the same file, job state genuinely survives
process restarts, machine reboots, etc. — nothing is held only in memory.

### Worker process model
`queuectl worker start --count N` spawns N detached, backgrounded Node
processes (`src/worker.js`), each running its own poll loop:
1. Try to claim a pending, due (`next_run_at <= now`) job (highest priority
   first).
2. If none, sleep `poll-interval-ms` and retry.
3. If claimed, run the job's `command` via `child_process.exec` (optionally
   under a timeout), then mark it `completed` or `failed`/`dead` based on
   the exit code.
4. On `SIGTERM`/`SIGINT` (from `worker stop` or Ctrl+C), finish the
   in-progress job, then exit and mark itself `stopped` in the worker
   registry — this is the graceful shutdown requirement.

Each worker's stdout/stderr is redirected to `.queuectl/logs/<worker_id>.log`
for debugging, since the worker runs detached from the terminal.

### Retry & backoff
`delay = backoff_base ^ attempts` seconds, computed after each failed
attempt (so the 1st retry waits `base^1`s, the 2nd `base^2`s, etc.), then
the job is set back to `pending` with `next_run_at = now + delay`. Once
`attempts >= max_retries`, the job moves to `dead` (the DLQ) instead.

---

## 4. Assumptions & Trade-offs

- **Storage: SQLite over JSON files.** SQLite (via `better-sqlite3`) gives
  transactional, file-locked atomic updates "for free" across multiple OS
  processes, which is exactly what's needed to prevent duplicate job
  claiming. A hand-rolled JSON-file queue would need its own file-locking
  scheme to be safe with real concurrent workers.
- **Workers are OS processes, not threads/async tasks**, matching the
  assignment's "worker processes" language and giving true parallelism and
  isolation (a crashing job command can't take down other workers).
- **Locking granularity:** the whole "claim next job" operation is one
  short SQL transaction; job *execution* itself happens outside any lock,
  so long-running jobs don't block other workers from claiming other jobs.
- **`worker stop` is registry-based**, tracked via `.queuectl/worker-pids.json`.
  It only knows about workers started by `queuectl worker start` in the same
  directory. If a worker process is killed externally (e.g. `kill -9`), the
  registry can go stale; `status` doesn't currently ping liveness on every
  read (a `SIGKILL`'d worker will show as the last state it wrote).
- **Commands run via the shell** (`child_process.exec` with `shell: true`),
  so pipes/`&&`/redirection in job commands work as expected, at the cost of
  the usual shell-injection caveat — acceptable here since job commands are
  supplied by the queue owner, not untrusted external input.
- **`max_retries`, `priority`, `timeout_ms`, `run_at` are captured per-job**
  at enqueue time (falling back to global config for `max_retries`), so
  changing global config later doesn't retroactively change already-enqueued
  jobs' retry budgets — this felt like the least surprising behavior.
- **No distributed/multi-machine support.** This is a single-machine,
  single-SQLite-file design, matching the assignment's scope (a CLI tool,
  not a distributed system).

---

## 5. Testing Instructions

An automated end-to-end test script is included, covering all 5 scenarios
from the assignment:

```bash
chmod +x test/run-tests.sh
./test/run-tests.sh
```

It runs each scenario in an isolated temp directory and prints PASS/FAIL:
1. Basic job completes successfully
2. Failed job retries with backoff and moves to DLQ
3. Multiple workers (4) process 12 jobs without overlap/duplication
4. Invalid commands fail gracefully (moved to DLQ, no crash)
5. Job data survives restart (persisted SQLite file re-read by a fresh process)

Expected output ends with:
```
Results: 5 passed, 0 failed
```

You can also exercise the CLI manually — see the Usage Examples section
above for copy-pasteable commands.

---

## Bonus features implemented

- **Job priority queues** — `--priority <n>`, higher runs first
- **Scheduled/delayed jobs** — `--run-at <isoDate>`
- **Job timeout handling** — `--timeout-ms <n>`, kills long-running commands
- **Job output logging** — job stdout/stderr captured in the DB (`last_error`,
  `output_log`) and per-worker logs under `.queuectl/logs/`
# QueueCTL
