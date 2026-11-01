"""Command line interface.

Exit codes: 0 success (also with warnings) | 1 run failed | 2 usage/config error | 3 another run in progress
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from .bench import run_bench
from .config import Config
from .errors import ConfigError, LockHeldError
from .generate import generate
from .logging_utils import configure_logging
from .pipeline import run_pipeline
from .reporting import render_markdown
from .runtracker import RunTracker


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="orderflow", description="Reliable, observable orders/customers pipeline")
    sub = p.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("run", help="run the pipeline once")
    r.add_argument("--input-dir", required=True)
    r.add_argument("--output-dir", required=True)
    r.add_argument("--run-id")
    r.add_argument("--max-reject-ratio", type=float)
    r.add_argument("--allow-older-snapshot", action="store_true", help="intentional backfill of an older snapshot")
    r.add_argument("--no-raw-quarantine", action="store_true", help="store only SHA-256 of rejected rows")
    r.add_argument("--max-attempts", type=int)

    g = sub.add_parser("generate", help="generate synthetic data")
    g.add_argument("--out-dir", required=True)
    g.add_argument("--customers", type=int, default=1000)
    g.add_argument("--orders", type=int, default=5000)
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--invalid-rate", type=float, default=0.02)

    b = sub.add_parser("bench", help="generate a larger dataset, run once, print timings")
    b.add_argument("--customers", type=int, default=20_000)
    b.add_argument("--orders", type=int, default=100_000)
    b.add_argument("--seed", type=int, default=42)
    b.add_argument("--no-fast-staging", action="store_true")

    rp = sub.add_parser("report", help="print the report of the latest (or given) successful run")
    rp.add_argument("--output-dir", required=True)
    rp.add_argument("--run-id")

    st = sub.add_parser("status", help="show a run and its tasks from the run history")
    st.add_argument("--output-dir", required=True)
    st.add_argument("--run-id")
    return p


def main(argv=None) -> int:
    args = _parser().parse_args(argv)
    configure_logging(logging.INFO)
    try:
        if args.cmd == "run":
            cfg = Config.from_env(
                args.input_dir, args.output_dir, max_reject_ratio=args.max_reject_ratio,
                max_attempts=args.max_attempts,
                allow_older_snapshot=True if args.allow_older_snapshot else None,
                quarantine_store_raw=False if args.no_raw_quarantine else None,
            )  # fmt: skip
            res = run_pipeline(cfg, run_id=args.run_id)
            print(json.dumps({"run_id": res.run_id, "status": res.status, "elapsed_s": round(res.elapsed_s, 3),
                              "failed_task": res.failed_task, **{k: res.metrics[k] for k in
                              ("input_rows", "accepted_rows", "rejected_rows", "output_rows")}}, indent=2))  # fmt: skip
            return res.exit_code
        if args.cmd == "generate":
            m = generate(args.out_dir, args.customers, args.orders, args.seed, args.invalid_rate)
            print(json.dumps({k: m[k] for k in ("seed", "customers_rows", "orders_rows",
                                                "expected_customers_accepted", "expected_orders_accepted")}, indent=2))  # fmt: skip
            return 0
        if args.cmd == "bench":
            print(json.dumps(run_bench(args.customers, args.orders, args.seed, not args.no_fast_staging), indent=2))
            return 0
        if args.cmd in ("report", "status"):
            out = Path(args.output_dir)
            tracker = RunTracker(out / "run_history.sqlite")
            run_id = args.run_id or tracker.last_run_id()
            if args.cmd == "status":
                print(json.dumps({"run": tracker.get_run(run_id), "tasks": tracker.get_tasks(run_id)}, indent=2))
                return 0
            rep = out / "reports" / f"{run_id}.json"
            if not rep.is_file():
                print(f"no report for run {run_id}", file=sys.stderr)
                return 1
            print(render_markdown(json.loads(rep.read_text())))
            return 0
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    except LockHeldError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 3
    return 2
