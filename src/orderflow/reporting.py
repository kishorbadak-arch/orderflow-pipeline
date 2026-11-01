"""Validation / reconciliation report built from run metrics and the published database."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from .publish import target_path

EXAMPLE_QUERIES = {
    "revenue_by_currency": (
        "SELECT currency, COUNT(*) AS orders, SUM(net_cents) AS net_cents, "
        "SUM(recognized_revenue_cents) AS recognized_cents FROM fact_orders GROUP BY currency ORDER BY currency"
    ),
    "orders_by_status": "SELECT status, COUNT(*) AS orders FROM fact_orders GROUP BY status ORDER BY status",
    "orders_by_country": (
        "SELECT c.country, COUNT(*) AS orders FROM fact_orders o JOIN dim_customer c USING (customer_id) "
        "GROUP BY c.country ORDER BY orders DESC, c.country LIMIT 10"
    ),
    "busiest_days": (
        "SELECT order_date, currency, orders, recognized_revenue_cents FROM agg_daily_revenue "
        "ORDER BY orders DESC, order_date, currency LIMIT 5"
    ),
}  # fmt: skip


def run_example_queries(output_dir: Path) -> dict:
    conn = sqlite3.connect(f"file:{target_path(output_dir)}?mode=ro", uri=True)
    try:
        out = {}
        for name, sql in EXAMPLE_QUERIES.items():
            cur = conn.execute(sql)
            cols = [d[0] for d in cur.description]
            out[name] = [dict(zip(cols, row)) for row in cur.fetchall()]
        return out
    finally:
        conn.close()


def build_report(run_id: str, metrics: dict, quarantine: dict, gate: dict, queries: dict) -> dict:
    return {"run_id": run_id, "metrics": metrics, "quarantine": quarantine, "reconciliation": gate, "example_queries": queries}


def render_markdown(rep: dict) -> str:
    m, g = rep["metrics"], rep["reconciliation"]
    lines = [f"# Validation & reconciliation report: run `{rep['run_id']}`", ""]
    lines += ["## Row counts", "", "| entity | input | schema rejected | duplicates superseded | business rejected | accepted |",
              "|---|---:|---:|---:|---:|---:|"]  # fmt: skip
    for ent, c in m["entities"].items():
        lines.append(f"| {ent} | {c['input_rows']} | {c['schema_rejected']} | {c['duplicate_rejected']} | "
                     f"{c['business_rejected']} | {c['accepted']} |")  # fmt: skip
    lines += ["", f"Total: input **{m['input_rows']}** = accepted **{m['accepted_rows']}** + rejected "
              f"**{m['rejected_rows']}**; published output rows (customers + orders) = **{m['output_rows']}**.", ""]
    lines += ["## Checks", ""] + [f"- {'PASS' if ok else 'FAIL'}: {name}" for name, ok in g["checks"].items()]
    lines += ["", f"Reject ratio: {g['reject_ratio']:.4f} (limit {g['max_reject_ratio']})", ""]
    lines += ["## Rejects by primary reason", ""]
    for code, n in rep["quarantine"]["by_primary_reason"].items():
        lines.append(f"- `{code}`: {n}")
    lines += ["", "## Example queries on the published target", ""]
    for name, rows in rep["example_queries"].items():
        lines += [f"### {name}", "", "```", EXAMPLE_QUERIES[name], "```", "", "```json", json.dumps(rows, indent=1), "```", ""]
    return "\n".join(lines)
