"""Deterministic transformation of validated records into target rows.

Input : lists of Customer / Order (already validated, unique keys, valid references).
Output: tuples in the exact column order of schemas.py, sorted by primary key.

Rules
  * money -> integer cents (exact; inputs have at most 2 decimals)
  * gross = quantity * unit_price ; net = gross - discount ; never negative (validated upstream)
  * recognized_revenue = 0 for CANCELLED orders, else net
  * timestamps -> UTC; order_date = UTC calendar date of order_ts
  * email is reduced to its domain (data minimisation: the target holds no full e-mail addresses)
  * missing segment -> 'UNKNOWN'
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from .timeutil import format_utc


def to_cents(value: Decimal) -> int:
    return int((value * 100).to_integral_exact())


@dataclass
class TransformResult:
    dim_customers: list[tuple]
    fact_orders: list[tuple]
    agg_daily: list[tuple]
    net_cents_total: int
    recognized_cents_total: int
    max_updated_at_utc: str


def transform(customers, orders, run_id: str) -> TransformResult:
    dim = sorted(
        (
            c.customer_id, c.email.rsplit("@", 1)[1].lower(), c.country, c.segment or "UNKNOWN",
            format_utc(c.signup_ts), format_utc(c.updated_at),
        )
        for c in customers
    )  # fmt: skip

    fact, agg = [], {}
    for o in sorted(orders, key=lambda x: x.order_id):
        unit = to_cents(o.unit_price)
        gross = o.quantity * unit
        disc = to_cents(o.discount)
        net = gross - disc
        rec = 0 if o.status == "CANCELLED" else net
        odate = o.order_ts.date().isoformat()
        fact.append((
            o.order_id, o.customer_id, format_utc(o.order_ts), odate, o.status, o.currency,
            o.quantity, unit, gross, disc, net, rec, format_utc(o.updated_at), run_id,
        ))  # fmt: skip
        a = agg.setdefault((odate, o.currency), [0, 0, 0, 0, 0, 0])
        a[0] += 1
        a[1] += o.quantity
        a[2] += gross
        a[3] += disc
        a[4] += net
        a[5] += rec
    agg_rows = [(d, cur, *vals) for (d, cur), vals in sorted(agg.items())]
    stamps = [format_utc(c.updated_at) for c in customers] + [format_utc(o.updated_at) for o in orders]
    return TransformResult(
        dim, fact, agg_rows,
        sum(r[10] for r in fact), sum(r[11] for r in fact),
        max(stamps) if stamps else "",
    )  # fmt: skip
