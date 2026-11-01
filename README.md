# orderflow: a reliable, observable batch data pipeline

Ingests two CSV files (`customers`, `orders`), validates them, quarantines bad rows, transforms the good
ones and publishes them **atomically** to a SQLite analytical database. A failed run never touches the last
good published output. Standard library only (no runtime dependencies); `pytest` is the only dev dependency.

* **Stack:** Python 3.11+ (tested on 3.12.3), SQLite (standard library), a ~100-line DAG runner, `pytest`.
  Why: for a learner every extra service (Airflow, Spark, Postgres) adds setup cost without helping a
  requirement that a few hundred lines of plain Python meet. See [docs/DESIGN.md](docs/DESIGN.md).
* **Domain:** synthetic orders and customers. All data in this repository is invented; there is no personal data.

## Quick start (clean environment)

```bash
python3 -m venv .venv && . .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements-dev.lock.txt                # pinned: pytest 9.2.0, pip-audit 2.10.1 (+ transitive pins)
export PYTHONPATH=src                                   # or: pip install -e .   (Windows: set PYTHONPATH=src)

# 1. Run the sample workflow (one command)
python -m orderflow run --input-dir data/sample/input --output-dir output

# 2. Run the automated tests (one command; 149 tests, no network)
python -m pytest

# 3. Generate and process a larger dataset (configurable size and seed)
python -m orderflow generate --out-dir generated --customers 20000 --orders 100000 --seed 42
python -m orderflow run --input-dir generated/input --output-dir output_big --max-reject-ratio 0.1
python -m orderflow bench --customers 40000 --orders 100000 --seed 42      # generate + run + timings

# Inspect results
python -m orderflow report --output-dir output            # reconciliation report of the last run
python -m orderflow status --output-dir output            # run + task status from the run history
sqlite3 output/warehouse.sqlite < docs/example_queries.sql   # or any SQLite client
```

Exit codes: `0` success (also success-with-warnings) · `1` run failed · `2` bad usage/config · `3` another run in progress.

## Architecture

```
 customers.csv ─► ingest_customers ─┐
                                    ├─► schema_validate ─► business_validate ─┬─► quarantine ───────────┐
 orders.csv    ─► ingest_orders ────┘   (types, formats,     (FK, cross-field    │  (rejects/<run>.jsonl)  │
                                         dedupe)               rules)            └─► transform ─► quality_gate ─┴─► publish ─► report*
                                                                                                                  (staging+swap)   (*non-critical)
 output/  warehouse.sqlite (published) · warehouse.sqlite.prev (previous good) · run_history.sqlite
          quarantine/<run_id>.jsonl · reports/<run_id>.{md,json} · last_failure.json
```

A task runs only if **all** its dependencies succeeded; otherwise it is `SKIPPED`. `publish` depends on
`quality_gate` and `quarantine`, so no failed validation, failed gate or missing reject file can publish.

### Orchestration behaviour
| Property | Behaviour |
|---|---|
| Retries | up to `max_attempts` (default 3) per task, **only** for retryable errors (transient I/O, timeouts) |
| Backoff | `base * 2^(attempt-1)`, capped (defaults 1s, 2s, ... max 30s) |
| Permanent failures | bad data, schema drift, failed quality gate, bugs: **never retried** |
| Timeouts | per task (`default_timeout_s`, override per task via `task_timeouts`); see limits below |
| Status | task: `SUCCESS / FAILED / TIMED_OUT / SKIPPED`; run: `SUCCESS / SUCCESS_WITH_WARNINGS / FAILED / ABANDONED` |
| Notification | structured JSON log (`run_failed`), `output/last_failure.json`, non-zero exit; optional https webhook |

## Rules at a glance (full detail in [docs/DATA_DICTIONARY.md](docs/DATA_DICTIONARY.md))
* **Batch-level failures** (whole run fails, nothing published): unsupported file type, missing/unreadable file,
  invalid UTF-8/CSV structure, NUL bytes, header with missing/unexpected/duplicate columns, file with no data rows,
  failed quality gate, snapshot older than the published one.
* **Record-level rejects** (row quarantined, run continues): malformed row (wrong field count), missing required
  value, bad type/format/enum, duplicate key (older version loses), invalid customer reference, and business
  rules (order before signup, discount above gross, future timestamp, `updated_at` before creation).
* **Duplicates:** for a repeated key the greatest `updated_at` wins (tie: later row). Losers are quarantined as
  `DUPLICATE_SUPERSEDED`. File order is irrelevant, so out-of-order/late versions are deterministic.
* **Quality gate:** per-entity `input = accepted + rejected`, outputs equal accepted rows, net total recomputed
  independently, reject ratio <= `max_reject_ratio` (default 0.30), no empty input, no older snapshot.

## Operations
* **Idempotency:** each run is a full-snapshot replace. Re-running the same input yields identical table contents
  and the same `content_sha256` (only `publication.run_id/published_at_utc` change). Tested.
* **Restart / recovery:** a failed run leaves the published file untouched. Fix the cause and run again; no manual
  cleanup is needed. Stale staging files are removed at the next start, runs left `RUNNING` by a crashed process are
  marked `ABANDONED`, and a stale lock file from a dead PID is taken over.
* **Concurrency:** one run per output directory (lock file with PID). A second run exits with code 3.
* **Late-arriving data:** inside a snapshot, late versions are resolved by `updated_at`. Across runs, publish the
  newer full snapshot. A snapshot whose latest `updated_at` is *older* than the published one is refused, because it
  would silently roll the warehouse back. For an intentional **backfill**, pass `--allow-older-snapshot`.
* **Manual rollback:** `warehouse.sqlite.prev` is the previous good file (hard link/copy). To roll back, stop runs and
  replace `warehouse.sqlite` with it.
* **Retention:** `quarantine/`, `reports/` and `run_history.sqlite` are never deleted automatically. Delete old
  `quarantine/<run_id>.jsonl` files when no longer needed (e.g. `find output/quarantine -mtime +30 -delete`);
  deleting a run's file does not affect the published data.

### Troubleshooting
| Symptom | Likely cause / action |
|---|---|
| exit 1, `BatchSchemaError` | header differs from the contract; fix the file or update the contract deliberately |
| exit 1, `QualityGateError` | too many rejects; inspect `quarantine/<run_id>.jsonl`, fix the source, or raise `--max-reject-ratio` consciously |
| exit 1, `StaleSnapshotError` | input is older than the published snapshot; use the newer file, or `--allow-older-snapshot` for backfill |
| exit 1, `EmptyInputError` | a file has only a header; the previous output was kept on purpose |
| exit 3 | another run is active; if none is, delete `output/.run.lock` |
| `SUCCESS_WITH_WARNINGS` | only the non-critical `report` task failed; data was published; see logs |

## Security notes
* No secrets in the repo; `.env.example` has placeholders only; `.env`, outputs and `*.sqlite` are git-ignored.
* Optional credentials (webhook URL) come from environment variables, must be `https://`, and are never logged
  or placed in notifications. Notifications contain run id, task name, counts and a sanitized error only.
* Only own-code exception messages are stored/logged. Unexpected exceptions are reduced to their type name
  (set `ORDERFLOW_DEBUG=1` locally to see detail) because library errors can echo input values.
* Rejected rows are written **only** to `quarantine/<run_id>.jsonl` (file mode 0600, directory 0700 on POSIX).
  `--no-raw-quarantine` stores just a SHA-256 of each rejected row. Logs and run history never contain row values.
* The warehouse stores the e-mail **domain** only, not the address (data minimisation).
* All SQL with data values is parameterized; table/column names come from constants. No `eval`, `pickle`,
  shell calls or network access (except the optional webhook). Source file names must be plain names inside the
  input directory (no `../`, absolute paths or symlink escapes).
* `python -m pip_audit -r requirements-dev.lock.txt` reported *no known vulnerabilities* when run
  (2026-10-30). That checks dependencies against a public advisory database; it is not a security guarantee.

## Known limitations
* Timeouts are cooperative: a timed-out Python thread cannot be killed. It is told to stop (`cancel` event), its
  result is discarded and `publish` refuses to swap after cancellation, but CPU-bound work in it may continue until
  it finishes. Tasks run one at a time (no parallelism).
* Single machine, single writer; SQLite file swap is atomic on POSIX/NTFS local disks, not on network shares.
* Full-snapshot refresh only (no incremental merge). Whole input is held in memory (fine for ~10^5-10^6 rows).
* No FX conversion: revenue is aggregated per currency. Quarantine files contain raw (synthetic) values by default.
* Permissions are POSIX-style; on Windows the mode bits are not enforced.
* Not verified: Windows/macOS, Python 3.11, cloud deployment (none is included).
