"""Shared fixtures. Every test uses its own temp dirs; nothing touches the network or real data."""

from __future__ import annotations

import csv
import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from orderflow.config import Config
from orderflow.ingest import CUSTOMER_COLUMNS, ORDER_COLUMNS

ROOT = Path(__file__).resolve().parent.parent
SAMPLE_INPUT = ROOT / "data" / "sample" / "input"
AS_OF = datetime(2025, 12, 31, tzinfo=timezone.utc)

GOOD_CUSTOMERS = [
    ["C000001", "alice@example.com", "US", "CONSUMER", "2025-01-05T10:00:00Z", "2025-01-05T10:00:00Z"],
    ["C000002", "bob@example.com", "GB", "SMB", "2025-01-10T09:30:00Z", "2025-01-10T09:30:00Z"],
    ["C000003", "carol@example.com", "IN", "", "2025-02-01T00:00:00Z", "2025-02-01T00:00:00Z"],
]
GOOD_ORDERS = [
    ["O00000001", "C000001", "2025-02-01T10:00:00Z", "2025-02-01T10:00:00Z", "PLACED", "USD", "2", "10.00", "0.00"],
    ["O00000002", "C000002", "2025-02-02T10:00:00Z", "2025-02-02T10:00:00Z", "SHIPPED", "GBP", "1", "25.50", "5.50"],
    ["O00000003", "C000003", "2025-02-03T10:00:00Z", "2025-02-03T10:00:00Z", "CANCELLED", "INR", "3", "100.00", "0.00"],
]


def write_csv(path: Path, header, rows) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)
    return path


@pytest.fixture
def make_input(tmp_path):
    """make_input(customers=..., orders=..., name='in') -> input dir path."""

    def _make(customers=None, orders=None, name="in", c_header=CUSTOMER_COLUMNS, o_header=ORDER_COLUMNS):
        d = tmp_path / name
        write_csv(d / "customers.csv", c_header, GOOD_CUSTOMERS if customers is None else customers)
        write_csv(d / "orders.csv", o_header, GOOD_ORDERS if orders is None else orders)
        return d

    return _make


@pytest.fixture
def cfg_factory(tmp_path):
    def _make(input_dir, **kw):
        base = dict(input_dir=input_dir, output_dir=tmp_path / "out", as_of=AS_OF, backoff_base_s=0.01,
                    backoff_max_s=0.02, max_attempts=3, default_timeout_s=30.0)  # fmt: skip
        base.update(kw)
        return Config(**base)

    return _make


@pytest.fixture
def no_sleep():
    calls = []
    return calls.append, calls  # (sleep function, recorded delays)


class Db:
    """Read-only helper around the published SQLite file."""

    def __init__(self, path: Path):
        self.path = path

    def q(self, sql, params=()):
        conn = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)
        try:
            return conn.execute(sql, params).fetchall()
        finally:
            conn.close()

    def snapshot(self):
        return self.path.read_bytes()


@pytest.fixture
def db_of():
    return lambda cfg: Db(cfg.output_dir / "warehouse.sqlite")


def read_quarantine(cfg, run_id):
    p = cfg.output_dir / "quarantine" / f"{run_id}.jsonl"
    return [json.loads(line) for line in p.read_text().splitlines()]


@pytest.fixture
def quarantine_of():
    return read_quarantine


@pytest.fixture(autouse=True)
def _reset_logger():
    yield
    lg = logging.getLogger("orderflow")
    lg.handlers = [logging.NullHandler()]
    lg.propagate = True
    lg.setLevel(logging.NOTSET)
