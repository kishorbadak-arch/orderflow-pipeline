"""Run history in a separate SQLite file (never swapped, so history survives failed runs)."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from .timeutil import utc_now_iso

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY, started_at_utc TEXT NOT NULL, finished_at_utc TEXT,
    status TEXT NOT NULL, source TEXT NOT NULL,
    input_rows INTEGER, accepted_rows INTEGER, output_rows INTEGER, rejected_rows INTEGER,
    elapsed_s REAL, failed_task TEXT, error_type TEXT, error_message TEXT, metrics_json TEXT
);
CREATE TABLE IF NOT EXISTS task_runs (
    run_id TEXT NOT NULL, task TEXT NOT NULL, status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
    started_at_utc TEXT, finished_at_utc TEXT, error_type TEXT, error_message TEXT,
    PRIMARY KEY (run_id, task)
);
"""


class RunTracker:
    def __init__(self, path: Path):
        self.path = path
        with self._conn() as c:
            c.executescript(SCHEMA)

    def _conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path, timeout=10)

    def mark_abandoned(self) -> int:
        """A RUNNING row at the start of a new run means an earlier process died (we hold the lock)."""
        with self._conn() as c:
            cur = c.execute(
                "UPDATE runs SET status='ABANDONED', finished_at_utc=? WHERE status='RUNNING'", (utc_now_iso(),)
            )
            c.execute("UPDATE task_runs SET status='ABANDONED' WHERE status IN ('RUNNING','PENDING')")
            return cur.rowcount

    def start_run(self, run_id: str, source: str, task_names) -> None:
        with self._conn() as c:
            c.execute("INSERT INTO runs (run_id, started_at_utc, status, source) VALUES (?,?,?,?)",
                      (run_id, utc_now_iso(), "RUNNING", source))  # fmt: skip
            c.executemany("INSERT INTO task_runs (run_id, task, status) VALUES (?,?,'PENDING')",
                          [(run_id, t) for t in task_names])  # fmt: skip

    def update_task(self, run_id, task, status, attempts, started=None, finished=None, etype=None, emsg=None):
        with self._conn() as c:
            c.execute(
                "UPDATE task_runs SET status=?, attempts=?, started_at_utc=COALESCE(?, started_at_utc), "
                "finished_at_utc=?, error_type=?, error_message=? WHERE run_id=? AND task=?",
                (status, attempts, started, finished, etype, emsg, run_id, task),
            )

    def finish_run(self, run_id, status, metrics: dict, elapsed_s, failed_task=None, etype=None, emsg=None):
        with self._conn() as c:
            c.execute(
                "UPDATE runs SET status=?, finished_at_utc=?, input_rows=?, accepted_rows=?, output_rows=?, "
                "rejected_rows=?, elapsed_s=?, failed_task=?, error_type=?, error_message=?, metrics_json=? "
                "WHERE run_id=?",
                (status, utc_now_iso(), metrics.get("input_rows"), metrics.get("accepted_rows"),
                 metrics.get("output_rows"), metrics.get("rejected_rows"), elapsed_s, failed_task, etype,
                 emsg, json.dumps(metrics, sort_keys=True), run_id),
            )  # fmt: skip

    def get_run(self, run_id: str) -> dict | None:
        with self._conn() as c:
            c.row_factory = sqlite3.Row
            r = c.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            return dict(r) if r else None

    def get_tasks(self, run_id: str) -> list[dict]:
        with self._conn() as c:
            c.row_factory = sqlite3.Row
            rows = c.execute("SELECT * FROM task_runs WHERE run_id=? ORDER BY rowid", (run_id,)).fetchall()
            return [dict(r) for r in rows]

    def last_run_id(self) -> str | None:
        with self._conn() as c:
            r = c.execute("SELECT run_id FROM runs ORDER BY started_at_utc DESC, rowid DESC LIMIT 1").fetchone()
            return r[0] if r else None
