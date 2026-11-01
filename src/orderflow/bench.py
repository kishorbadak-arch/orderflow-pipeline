"""Reproducible timing: generate a dataset, run the pipeline once, report per-task and overall timing."""

from __future__ import annotations

import tempfile
import time
from datetime import datetime
from pathlib import Path

from .config import Config
from .generate import generate
from .pipeline import run_pipeline
from .runtracker import RunTracker


def _secs(a: str, b: str) -> float:
    f = "%Y-%m-%dT%H:%M:%S.%fZ"
    return (datetime.strptime(b, f) - datetime.strptime(a, f)).total_seconds()


def run_bench(customers: int, orders: int, seed: int, fast_staging: bool = True, workdir: str | None = None) -> dict:
    with tempfile.TemporaryDirectory(dir=workdir) as tmp:
        tmp = Path(tmp)
        t0 = time.perf_counter()
        manifest = generate(tmp, customers, orders, seed)
        gen_s = time.perf_counter() - t0
        cfg = Config(input_dir=tmp / "input", output_dir=tmp / "out", fast_staging=fast_staging,
                     max_reject_ratio=0.5, as_of=datetime.fromisoformat("2026-01-01T00:00:00+00:00"))  # fmt: skip
        res = run_pipeline(cfg, run_id="bench")
        tasks = {t["task"]: round(_secs(t["started_at_utc"], t["finished_at_utc"]), 4)
                 for t in RunTracker(cfg.output_dir / "run_history.sqlite").get_tasks("bench")
                 if t["started_at_utc"] and t["finished_at_utc"]}  # fmt: skip
        m = res.metrics
        return {
            "status": res.status, "seed": seed, "fast_staging": fast_staging,
            "input_rows": m["input_rows"], "accepted_rows": m["accepted_rows"],
            "rejected_rows": m["rejected_rows"], "output_rows": m["output_rows"],
            "generate_s": round(gen_s, 3), "pipeline_s": round(res.elapsed_s, 3),
            "rows_per_s": int(m["input_rows"] / res.elapsed_s) if res.elapsed_s else 0,
            "task_seconds": tasks,
            "matches_manifest": (m["accepted_rows"] == manifest["expected_customers_accepted"] + manifest["expected_orders_accepted"]),
        }  # fmt: skip
