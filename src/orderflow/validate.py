"""Validation stages that combine parsing, de-duplication, business rules and reject creation."""

from __future__ import annotations

from datetime import datetime

from .business import customer_reasons, order_reasons
from .dedupe import resolve_duplicates
from .models import IngestResult, StageCounts, ValidationResult
from .schema import make_reject, parse_customer, parse_order


def _schema_one(res: IngestResult, parser, key_attr: str, store_raw: bool):
    """Parse one entity, then resolve duplicates. Returns (accepted, rejects, counts)."""
    by_row = {r.row_number: r for r in res.records}
    counts = StageCounts(input_rows=len(res.records))
    parsed, rejects = [], []
    for rec in res.records:
        item, reasons = parser(rec, res.header)
        if reasons:
            key = None
            if not rec.malformed:
                idx = res.header.index(key_attr) if key_attr in res.header else None
                key = rec.values[idx].strip() or None if idx is not None else None
            rejects.append(make_reject(res.entity, "schema", rec, res.header, reasons, key, store_raw))
            counts.schema_rejected += 1
        else:
            parsed.append(item)
    winners, losers = resolve_duplicates(parsed, lambda x: getattr(x, key_attr))
    for lo in losers:
        rec = by_row[lo.row_number]
        rejects.append(
            make_reject(res.entity, "duplicate", rec, res.header,
                        [("DUPLICATE_SUPERSEDED", key_attr)], getattr(lo, key_attr), store_raw)
        )  # fmt: skip
        counts.duplicate_rejected += 1
    return winners, rejects, counts


def schema_stage(customers_in: IngestResult, orders_in: IngestResult, store_raw: bool = True) -> ValidationResult:
    """Record-level schema validation + duplicate resolution (no business rules yet)."""
    customers, c_rej, c_counts = _schema_one(customers_in, parse_customer, "customer_id", store_raw)
    orders, o_rej, o_counts = _schema_one(orders_in, parse_order, "order_id", store_raw)
    return ValidationResult(customers, orders, c_rej + o_rej, {"customers": c_counts, "orders": o_counts})


def business_stage(
    schema_res: ValidationResult, customers_in: IngestResult, orders_in: IngestResult,
    as_of: datetime, store_raw: bool = True,
) -> ValidationResult:  # fmt: skip
    """Business rules and referential integrity on the schema-valid, de-duplicated records."""
    c_counts, o_counts = schema_res.counts["customers"], schema_res.counts["orders"]
    c_counts = StageCounts(**vars(c_counts))
    o_counts = StageCounts(**vars(o_counts))
    rejects = list(schema_res.rejects)

    c_rows = {r.row_number: r for r in customers_in.records}
    good_customers = []
    for c in schema_res.customers:
        reasons = customer_reasons(c, as_of)
        if reasons:
            rec = c_rows[c.row_number]
            rejects.append(make_reject("customers", "business", rec, customers_in.header, reasons, c.customer_id, store_raw))
            c_counts.business_rejected += 1
        else:
            good_customers.append(c)
    c_counts.accepted = len(good_customers)

    by_id = {c.customer_id: c for c in good_customers}
    o_rows = {r.row_number: r for r in orders_in.records}
    good_orders = []
    for o in schema_res.orders:
        reasons = order_reasons(o, by_id, as_of)
        if reasons:
            rec = o_rows[o.row_number]
            rejects.append(make_reject("orders", "business", rec, orders_in.header, reasons, o.order_id, store_raw))
            o_counts.business_rejected += 1
        else:
            good_orders.append(o)
    o_counts.accepted = len(good_orders)

    rejects.sort(key=lambda r: (r.entity, r.row_number))
    return ValidationResult(good_customers, good_orders, rejects, {"customers": c_counts, "orders": o_counts})
