"""Wires the tasks into a DAG and runs it.

DAG (arrow = "must succeed before"):

  ingest_customers ─┐
                    ├→ schema_validate → business_validate ─┬→ quarantine ───────────────┐
  ingest_orders ────┘                                        └→ transform → quality_gate ─┴→ publish → report (non-critical)
"""

from __future__ import annotations

import logging
import os
import re
import secrets
import time
from dataclasses import dataclass, field
from decimal import Decimal

from .config import Config
from .errors import ConfigError, EmptyInputError, LockHeldError, QualityGateError, StaleSnapshotError
from .fsutil import atomic_write_text, ensure_private_dir
from .ingest import CUSTOMER_COLUMNS, ORDER_COLUMNS, ingest_file, resolve_source
from .logging_utils import log, safe_error
from .notify import notify_failure
from .orchestrator import Orchestrator, RunContext, Task
from .publish import cleanup_stale_staging, publish, read_published_meta
from .quarantine import write_quarantine
from .reporting import build_report, render_markdown, run_example_queries
from .runtracker import RunTracker
from .timeutil import format_utc, utc_now, utc_now_iso
from .transform import to_cents, transform
from .validate import business_stage, schema_stage

_RUN_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
TASK_NAMES = ("ingest_customers", "ingest_orders", "schema_validate", "business_validate",
              "quarantine", "transform", "quality_gate", "publish", "report")  # fmt: skip


# ---------------------------------------------------------------- task functions
def t_ingest_customers(ctx, cancel):
    cfg = ctx.config
    path = resolve_source(cfg.input_dir, cfg.customers_file)
    return ingest_file(path, CUSTOMER_COLUMNS, "customers", ctx.run_id, cfg.max_file_bytes)


def t_ingest_orders(ctx, cancel):
    cfg = ctx.config
    path = resolve_source(cfg.input_dir, cfg.orders_file)
    return ingest_file(path, ORDER_COLUMNS, "orders", ctx.run_id, cfg.max_file_bytes)


def t_schema_validate(ctx, cancel):
    a = ctx.artifacts
    return schema_stage(a["ingest_customers"], a["ingest_orders"], ctx.config.quarantine_store_raw)


def t_business_validate(ctx, cancel):
    a = ctx.artifacts
    return business_stage(a["schema_validate"], a["ingest_customers"], a["ingest_orders"],
                          ctx.config.as_of, ctx.config.quarantine_store_raw)  # fmt: skip


def t_quarantine(ctx, cancel):
    return write_quarantine(ctx.config.output_dir, ctx.run_id, ctx.artifacts["business_validate"].rejects)


def t_transform(ctx, cancel):
    v = ctx.artifacts["business_validate"]
    return transform(v.customers, v.orders, ctx.run_id)


def t_quality_gate(ctx, cancel):
    cfg, a = ctx.config, ctx.artifacts
    v, tr = a["business_validate"], a["transform"]
    counts = v.counts
    total_in = sum(c.input_rows for c in counts.values())
    total_rej = sum(c.rejected for c in counts.values())
    if any(c.input_rows == 0 for c in counts.values()):
        raise EmptyInputError("an input file has no data rows; refusing to publish an empty snapshot")

    checks = {}
    checks["input = accepted + rejected (per entity)"] = all(c.input_rows == c.accepted + c.rejected for c in counts.values())
    checks["accepted rows equal transformed rows"] = (
        counts["customers"].accepted == len(tr.dim_customers) and counts["orders"].accepted == len(tr.fact_orders)
    )
    independent = sum(to_cents(o.quantity * o.unit_price - o.discount) for o in v.orders)  # separate path from transform()
    checks["net total recomputed independently"] = independent == tr.net_cents_total
    checks["no rejected row lost in quarantine"] = a["quarantine"]["rows"] == total_rej
    ratio = total_rej / total_in
    checks[f"reject ratio <= {cfg.max_reject_ratio}"] = ratio <= cfg.max_reject_ratio
    gate = {"checks": checks, "reject_ratio": ratio, "max_reject_ratio": cfg.max_reject_ratio}
    failed = [name for name, ok in checks.items() if not ok]
    if failed:
        raise QualityGateError("quality gate failed: " + "; ".join(failed))

    prior = read_published_meta(cfg.output_dir)
    if prior and tr.max_updated_at_utc < prior["max_updated_at_utc"] and not cfg.allow_older_snapshot:
        raise StaleSnapshotError(
            "snapshot is older than the published one (max updated_at is earlier); "
            "use --allow-older-snapshot for an intentional backfill"
        )
    gate["stale_check"] = "skipped (backfill allowed)" if cfg.allow_older_snapshot else "passed"
    return gate


def t_publish(ctx, cancel):
    a, cfg = ctx.artifacts, ctx.config
    hashes = (a["ingest_customers"].sha256, a["ingest_orders"].sha256)
    return publish(cfg.output_dir, a["transform"], ctx.run_id, hashes, cancel, cfg.fast_staging)


def collect_metrics(artifacts: dict, published: bool) -> dict:
    """Counts from whatever stages completed (so failed runs still report what they saw)."""
    entities, input_rows = {}, 0
    for ent in ("customers", "orders"):
        res = artifacts.get(f"ingest_{ent}")
        if res is not None:
            input_rows += len(res.records)
    v = artifacts.get("business_validate")
    if v is not None:
        for ent, c in v.counts.items():
            entities[ent] = dict(vars(c))
    accepted = sum(e["accepted"] for e in entities.values())
    rejected = sum(e["schema_rejected"] + e["duplicate_rejected"] + e["business_rejected"] for e in entities.values())
    tr = artifacts.get("transform")
    output = len(tr.dim_customers) + len(tr.fact_orders) if (published and tr) else 0
    return {
        "input_rows": input_rows,
        "accepted_rows": accepted if v else None,
        "rejected_rows": rejected if v else None,
        "output_rows": output,
        "output_agg_rows": len(tr.agg_daily) if (published and tr) else 0,
        "entities": entities,
    }


def t_report(ctx, cancel):
    cfg, a = ctx.config, ctx.artifacts
    metrics = collect_metrics(a, published=True)
    rep = build_report(ctx.run_id, metrics, a["quarantine"], a["quality_gate"], run_example_queries(cfg.output_dir))
    rdir = ensure_private_dir(cfg.output_dir / "reports")
    import json
    atomic_write_text(rdir / f"{ctx.run_id}.json", json.dumps(rep, indent=2, sort_keys=True) + "\n")
    atomic_write_text(rdir / f"{ctx.run_id}.md", render_markdown(rep) + "\n")
    return {"path": str(rdir / f"{ctx.run_id}.md")}


def build_tasks(cfg: Config) -> list[Task]:
    def mk(name, fn, deps=(), critical=True, attempts=None):
        return Task(name, fn, tuple(deps), attempts or cfg.max_attempts, cfg.timeout_for(name), critical,
                    cfg.backoff_base_s, cfg.backoff_max_s)  # fmt: skip

    # Every task gets up to cfg.max_attempts, but the orchestrator only retries *retryable* errors
    # (transient I/O, timeouts). Validation failures and failed quality gates are never retried.
    return [
        mk("ingest_customers", t_ingest_customers),
        mk("ingest_orders", t_ingest_orders),
        mk("schema_validate", t_schema_validate, ["ingest_customers", "ingest_orders"]),
        mk("business_validate", t_business_validate, ["schema_validate"]),
        mk("quarantine", t_quarantine, ["business_validate"]),
        mk("transform", t_transform, ["business_validate"]),
        mk("quality_gate", t_quality_gate, ["transform", "quarantine"]),
        mk("publish", t_publish, ["quality_gate", "quarantine"]),
        mk("report", t_report, ["publish"], critical=False),
    ]


# ---------------------------------------------------------------- run
@dataclass
class RunResult:
    run_id: str
    status: str
    metrics: dict
    elapsed_s: float
    tasks: dict
    failed_task: str | None = None
    error_type: str | None = None
    error_message: str | None = None

    @property
    def ok(self) -> bool:
        return self.status in ("SUCCESS", "SUCCESS_WITH_WARNINGS")

    @property
    def exit_code(self) -> int:
        return 0 if self.ok else 1


def new_run_id() -> str:
    return utc_now().strftime("%Y%m%dT%H%M%SZ") + "-" + secrets.token_hex(4)


class _Lock:
    """Single-writer guard: one run per output directory at a time (O_EXCL lock file with PID)."""

    def __init__(self, path):
        self.path = path

    def __enter__(self):
        for _ in range(2):
            try:
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                os.write(fd, str(os.getpid()).encode())
                os.close(fd)
                return self
            except FileExistsError:
                try:
                    pid = int(self.path.read_text() or "0")
                    os.kill(pid, 0)
                    alive = True
                except (ValueError, ProcessLookupError):
                    alive = False
                except PermissionError:
                    alive = True
                if alive:
                    raise LockHeldError("another run is in progress (lock file present)") from None
                self.path.unlink(missing_ok=True)  # stale lock from a dead process
        raise LockHeldError("could not acquire run lock")

    def __exit__(self, *exc):
        self.path.unlink(missing_ok=True)


def run_pipeline(cfg: Config, run_id: str | None = None, sleep=time.sleep) -> RunResult:
    run_id = run_id or new_run_id()
    if not _RUN_ID.match(run_id):
        raise ConfigError("run_id may only contain letters, digits, '-' and '_' (max 64)")
    ensure_private_dir(cfg.output_dir)
    start = time.perf_counter()
    cfg = cfg.with_(as_of=cfg.as_of or utc_now())

    with _Lock(cfg.output_dir / ".run.lock"):
        removed = cleanup_stale_staging(cfg.output_dir)
        tracker = RunTracker(cfg.output_dir / "run_history.sqlite")
        abandoned = tracker.mark_abandoned()
        tracker.start_run(run_id, f"{cfg.input_dir.name}/{cfg.customers_file},{cfg.orders_file}", TASK_NAMES)
        log(logging.INFO, "run_started", run_id=run_id, as_of=format_utc(cfg.as_of),
            stale_staging_removed=removed, abandoned_runs_marked=abandoned)  # fmt: skip

        def on_task(res):
            tracker.update_task(run_id, res.name, res.status, res.attempts, res.started_at,
                                res.finished_at, res.error_type, res.error_message)  # fmt: skip

        ctx = RunContext(run_id, cfg)
        try:
            status, results = Orchestrator(build_tasks(cfg), on_task, sleep).run(ctx)
        except BaseException as exc:  # interrupted / orchestration bug: record it, then re-raise
            et, em = safe_error(exc)
            tracker.finish_run(run_id, "FAILED", collect_metrics(ctx.artifacts, False),
                               time.perf_counter() - start, None, et, em)  # fmt: skip
            raise
        published = results["publish"].status == "SUCCESS"
        metrics = collect_metrics(ctx.artifacts, published)
        elapsed = time.perf_counter() - start
        metrics["elapsed_s"] = round(elapsed, 4)
        failed = next((r for r in results.values() if r.critical and r.status != "SUCCESS" and r.status != "SKIPPED"), None)
        if failed is None and status == "FAILED":
            failed = next(r for r in results.values() if r.critical and r.status != "SUCCESS")
        tracker.finish_run(run_id, status, metrics, elapsed, failed.name if failed else None,
                           failed.error_type if failed else None, failed.error_message if failed else None)  # fmt: skip
        result = RunResult(run_id, status, metrics, elapsed, results, failed.name if failed else None,
                           failed.error_type if failed else None, failed.error_message if failed else None)  # fmt: skip
        log(logging.INFO, "run_finished", run_id=run_id, status=status, elapsed_s=round(elapsed, 4),
            input_rows=metrics["input_rows"], accepted_rows=metrics["accepted_rows"],
            rejected_rows=metrics["rejected_rows"], output_rows=metrics["output_rows"])  # fmt: skip
        if status == "SUCCESS_WITH_WARNINGS":
            log(logging.WARNING, "run_succeeded_with_warnings", run_id=run_id,
                warnings=[r.name for r in results.values() if r.status != "SUCCESS"])  # fmt: skip
        if status == "FAILED":
            notify_failure(cfg.output_dir, cfg.webhook_url, {
                "run_id": run_id, "status": status, "failed_task": result.failed_task,
                "error_type": result.error_type, "error_message": result.error_message,
                "input_rows": metrics["input_rows"], "finished_at_utc": utc_now_iso(),
                "note": "published output (if any) is the last known good snapshot",
            })  # fmt: skip
        return result
