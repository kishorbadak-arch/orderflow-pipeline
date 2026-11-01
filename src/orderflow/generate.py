"""Synthetic data generator (deterministic for a given seed). No real or personal data.

Writes   <out_dir>/input/customers.csv, <out_dir>/input/orders.csv   (pipeline input)
and      <out_dir>/expected/manifest.json                            (oracle for tests/benchmarks)

Fault injection: about `invalid_rate` of the rows get ONE deliberate fault, each with a known primary
rejection code, so the manifest can state exactly which source lines must be rejected and why.
Faulty customers are extra customers that no order references, so faults never cascade.
"""

from __future__ import annotations

import csv
import json
import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from .ingest import CUSTOMER_COLUMNS, ORDER_COLUMNS

BASE = datetime(2025, 1, 1, tzinfo=timezone.utc)
COUNTRIES = ["US", "GB", "DE", "FR", "IN", "CA", "AU", "JP", "BR", "SG"]
SEGMENTS = ["CONSUMER", "SMB", "ENTERPRISE", ""]
STATUSES = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"]
CURRENCIES = ["USD", "EUR", "GBP", "INR"]

CUSTOMER_FAULTS = ["MISSING_REQUIRED", "INVALID_VALUE", "MALFORMED_ROW", "DUPLICATE_SUPERSEDED"]
ORDER_FAULTS = ["INVALID_REFERENCE", "INVALID_VALUE", "INVALID_DECIMAL", "DISCOUNT_EXCEEDS_GROSS",
                "FUTURE_TIMESTAMP", "INVALID_TIMESTAMP", "UPDATED_BEFORE_CREATED", "ORDER_BEFORE_SIGNUP",
                "MALFORMED_ROW", "DUPLICATE_SUPERSEDED"]  # fmt: skip


def _ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _money(cents: int) -> str:
    return f"{cents // 100}.{cents % 100:02d}"


def generate(out_dir, customers: int = 1000, orders: int = 5000, seed: int = 42, invalid_rate: float = 0.02) -> dict:
    if customers < 1 or orders < 1 or not 0 <= invalid_rate <= 0.5:
        raise ValueError("customers/orders must be >= 1 and 0 <= invalid_rate <= 0.5")
    rng = random.Random(seed)
    out_dir = Path(out_dir)
    (out_dir / "input").mkdir(parents=True, exist_ok=True)
    (out_dir / "expected").mkdir(parents=True, exist_ok=True)

    # ---- valid customers (rows are lists of strings; tag = expected primary reject code or None)
    c_rows, c_info = [], {}
    for i in range(1, customers + 1):
        signup = BASE + timedelta(minutes=rng.randrange(0, 60 * 24 * 120))
        updated = signup + timedelta(days=1)  # strictly newer, so a stale duplicate can be older
        cid = f"C{i:06d}"
        seg = rng.choice(SEGMENTS)
        c_rows.append(([cid, f"user{i}@example.com", rng.choice(COUNTRIES), seg, _ts(signup), _ts(updated)], None))
        c_info[cid] = signup

    valid_customers = list(c_rows)  # snapshot: faults below must only copy from valid base rows

    # ---- valid orders
    o_rows, expected_net, expected_rec = [], 0, 0
    cids = list(c_info)
    for i in range(1, orders + 1):
        cid = rng.choice(cids)
        order_ts = c_info[cid] + timedelta(minutes=rng.randrange(0, 60 * 24 * 200))
        updated = order_ts + timedelta(minutes=rng.randrange(1, 1000))
        qty, price_c = rng.randint(1, 20), rng.randint(100, 50000)
        gross = qty * price_c
        disc = rng.randint(0, gross) if rng.random() < 0.3 else 0
        status = rng.choice(STATUSES)
        o_rows.append(([f"O{i:08d}", cid, _ts(order_ts), _ts(updated), status, rng.choice(CURRENCIES),
                        str(qty), _money(price_c), _money(disc)], None))  # fmt: skip
        expected_net += gross - disc
        expected_rec += 0 if status == "CANCELLED" else gross - disc
    valid_orders = list(o_rows)  # snapshot before faults are added

    # ---- fault injection
    n_c_faults = round(invalid_rate * customers)
    n_o_faults = round(invalid_rate * orders)
    extra_c = customers
    for _ in range(n_c_faults):
        kind = rng.choice(CUSTOMER_FAULTS)
        if kind == "DUPLICATE_SUPERSEDED":
            row, _tag = valid_customers[rng.randrange(customers)]
            stale = list(row)
            stale[3] = "SMB"
            stale[5] = stale[4]  # updated_at == signup_ts: valid but older than the base row
            c_rows.insert(rng.randrange(len(c_rows) + 1), (stale, kind))
            continue
        extra_c += 1
        cid = f"C{extra_c:06d}"
        signup = BASE + timedelta(days=5)
        row = [cid, f"extra{extra_c}@example.com", "US", "SMB", _ts(signup), _ts(signup)]
        if kind == "MISSING_REQUIRED":
            row[1] = ""
        elif kind == "INVALID_VALUE":
            row[2] = "ZZ"
        elif kind == "MALFORMED_ROW":
            row = row[:5]
        c_rows.insert(rng.randrange(len(c_rows) + 1), (row, kind))

    n_valid_orders = len(valid_orders)
    for _ in range(n_o_faults):
        kind = rng.choice(ORDER_FAULTS)
        src, _tag = valid_orders[rng.randrange(n_valid_orders)]
        row = list(src)
        row[0] = f"O{orders + len(o_rows) - n_valid_orders + 1:08d}"  # fresh id unless a duplicate
        order_ts = datetime.strptime(src[2], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        if kind == "INVALID_REFERENCE":
            row[1] = "C999999"
        elif kind == "INVALID_VALUE":
            row[6] = rng.choice(["0", "1001"])
        elif kind == "INVALID_DECIMAL":
            row[7] = "12.345"
        elif kind == "DISCOUNT_EXCEEDS_GROSS":
            row[8] = _money(int(row[6]) * int(Decimal(row[7]) * 100) + 1)
        elif kind == "FUTURE_TIMESTAMP":
            row[2] = row[3] = "2099-01-01T00:00:00Z"
        elif kind == "INVALID_TIMESTAMP":
            row[2] = "2025-03-01 10:00:00"  # naive timestamp
        elif kind == "UPDATED_BEFORE_CREATED":
            row[3] = _ts(order_ts - timedelta(minutes=1))
        elif kind == "ORDER_BEFORE_SIGNUP":
            signup = c_info[row[1]]
            row[2] = row[3] = _ts(signup - timedelta(days=1))
        elif kind == "MALFORMED_ROW":
            row = row[:8]
        elif kind == "DUPLICATE_SUPERSEDED":
            row = list(src)  # same id, strictly older updated_at than the base row, still valid
            row[3] = row[2]
            row[4] = "PLACED"
        o_rows.insert(rng.randrange(len(o_rows) + 1), (row, kind))

    def write(name, header, rows):
        path = out_dir / "input" / name
        with open(path, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh, lineterminator="\n")
            w.writerow(header)
            expected = {}
            for line_no, (row, tag) in enumerate(rows, start=2):
                w.writerow(row)
                if tag:
                    expected[line_no] = tag
        return expected

    exp_c = write("customers.csv", CUSTOMER_COLUMNS, c_rows)
    exp_o = write("orders.csv", ORDER_COLUMNS, o_rows)
    manifest = {
        "seed": seed, "invalid_rate": invalid_rate,
        "customers_rows": len(c_rows), "orders_rows": len(o_rows),
        "expected_rejects": {"customers.csv": {str(k): v for k, v in exp_c.items()},
                             "orders.csv": {str(k): v for k, v in exp_o.items()}},
        "expected_customers_accepted": len(c_rows) - len(exp_c),
        "expected_orders_accepted": len(o_rows) - len(exp_o),
        "expected_net_cents_total": expected_net,
        "expected_recognized_cents_total": expected_rec,
    }  # fmt: skip
    (out_dir / "expected" / "manifest.json").write_text(json.dumps(manifest, indent=1, sort_keys=True) + "\n")
    return manifest
