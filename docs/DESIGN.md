# Design and trade-offs

## Assumptions
* Each run receives a **complete snapshot** of customers and orders (not deltas) as CSV files in one directory.
* `updated_at` is a reliable version marker per record; later `updated_at` means newer truth.
* The business only needs revenue per day and currency; FX conversion is out of scope.
* A wrong-but-published dataset is worse than a stale one, so the pipeline prefers failing and keeping the old data.

## Technology choices
| Choice | Why | Alternative not used |
|---|---|---|
| Plain Python + stdlib | zero install friction, easy to read/debug for a beginner | pandas/PySpark: heavier, not needed at this size |
| SQLite as target | queryable SQL, single file, atomic file swap, FK + integrity checks | Postgres/warehouse: needs a service; documented as the natural next step |
| Custom ~100-line DAG runner | the requirements (dependencies, bounded retries, timeouts, status) are small and explicit | Airflow/Prefect/Dagster: great in production, large for a learning project |
| Integer cents + `Decimal` parsing | exact money arithmetic | float: rounding errors |

## Publication strategy: staging and swap
Build `warehouse.sqlite.staging-<id>` -> verify (integrity, FK check, counts, aggregates, hash) -> hard-link current file
to `.prev` -> `os.replace` (atomic) -> verify the published file -> on failure restore `.prev`. Any failure before the swap leaves the
published file byte-identical (tested). A publish attempt that timed out is cancelled and refuses to swap (tested).

## Failure semantics
* Retryable (retried, bounded, backoff): `TransientError` (I/O), `TaskTimeout`, `ConnectionError`, `TimeoutError`.
* Everything else is permanent and fails immediately, so a data problem is never "retried until it works".
* The reject-ratio gate turns "too much bad data" into a failure instead of silently publishing a thin snapshot.
* The stale-snapshot guard protects against publishing yesterday's file over today's data.

## Trade-offs and what would change in production
* Full refresh is simple and idempotent but O(all data) per run; incremental MERGE/upsert would need change tracking.
* Cooperative timeouts (threads) are simple but cannot kill CPU-bound work; process isolation would fix that at the cost of serialising task data.
* Sequential execution keeps ordering and failure analysis simple; the DAG already expresses what could run in parallel (the two ingests).
* Quarantine keeps raw values for diagnosis (synthetic here). With real PII, switch to hash-only (`--no-raw-quarantine`) or encrypt at rest and apply retention.
* Cloud option: none shipped (avoids credentials/network). The `publish` function is the seam to replace with e.g. S3 + a manifest pointer swap.
