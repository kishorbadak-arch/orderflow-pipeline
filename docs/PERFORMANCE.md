# Performance measurement

Reproduce: `python -m orderflow bench --customers 40000 --orders 100000 --seed 42` (add `--no-fast-staging` for the PRAGMA variant).
It generates the dataset (fixed seed), runs the whole pipeline once into a temp directory and prints rows/s and per-task seconds.
Input: 140,000 valid + 2,800 deliberately invalid rows = **142,800 input rows**, 140,000 published.

Environment actually used: Linux container, **1 CPU**, Python 3.12.3, local disk; timings are noisy (+-10%). Three runs each:

| variant | pipeline total (s) | rows/s | publish task (s) |
|---|---|---|---|
| before optimization (hash re-computed 3x) | 7.25, 7.70, 8.28 (earlier runs also 7.29-7.82) | 17.2k-19.7k | 3.18-3.29 |
| **after: skip redundant staging re-hash** | 6.19, 6.25, 7.01 | 20.4k-23.1k | 2.20-2.69 |

Typical per-task seconds (one run): ingest 0.6, schema_validate 1.5, business_validate 0.2, transform 2.0, quality_gate 0.06, publish 2.2-3.8, report 0.14.

## Optimization adopted
Profiling `publish` showed the SHA-256 content hash over all rows was computed three times (build, staging check, post-swap check),
~0.65 s each. The staging check no longer recomputes it; the hash is still verified on the *final published file*, and a mismatch
there triggers automatic rollback, so the safety guarantee is unchanged (tests: `test_failed_post_publication_verification_restores_previous_output`).
Gain: about 0.6 s per 140k rows (~10% of total), within measurement noise on a single run but consistent across three.

## Tried, no measurable effect (kept as option)
`fast_staging` (`PRAGMA journal_mode=OFF; synchronous=OFF` on the disposable staging file): 7.29-7.82 s vs 7.25-8.28 s, i.e. indistinguishable from noise here. Left on by default because it is safe for a file that is rebuilt on failure; disable with `--no-fast-staging` when benchmarking.
Replacing per-row hashing by one bulk `repr` per table: no gain (cost is dominated by fetching rows), reverted.

## Next idea (not implemented)
`transform` and `schema_validate` (~3.5 s together) are pure-Python per-row loops; process-level parallelism or batching into SQLite would help only for much larger inputs and add complexity.
