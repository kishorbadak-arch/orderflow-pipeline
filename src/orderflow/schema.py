"""Record-level schema validation: required/nullable, types, formats, allowed values.

Each parser returns (typed_record | None, reasons). A record is accepted iff reasons is empty.
Reason codes are stable identifiers documented in docs/DATA_DICTIONARY.md.
"""

from __future__ import annotations

import hashlib
import json
import re
from decimal import Decimal

from .models import Customer, Order, RawRecord, Reason, Reject
from .timeutil import parse_ts

COUNTRIES = frozenset({"US", "GB", "DE", "FR", "IN", "CA", "AU", "JP", "BR", "SG"})
SEGMENTS = frozenset({"CONSUMER", "SMB", "ENTERPRISE"})
STATUSES = frozenset({"PLACED", "SHIPPED", "DELIVERED", "CANCELLED"})
CURRENCIES = frozenset({"USD", "EUR", "GBP", "INR"})
QTY_MIN, QTY_MAX = 1, 1000
PRICE_MIN, PRICE_MAX = Decimal("0.01"), Decimal("100000.00")

_CUSTOMER_ID = re.compile(r"^C\d{6}$")
_ORDER_ID = re.compile(r"^O\d{8}$")
_EMAIL = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
_DECIMAL = re.compile(r"^\d{1,7}(\.\d{1,2})?$")  # at most 2 decimals, no sign/exponent/NaN
_INT = re.compile(r"^\d{1,6}$")


def _fields(rec: RawRecord, header: tuple[str, ...]) -> dict[str, str]:
    return {h: v.strip() for h, v in zip(header, rec.values)}


def original_of(rec: RawRecord, header: tuple[str, ...]) -> dict:
    if rec.malformed:
        return {"_unaligned_values": list(rec.values)}
    return dict(zip(header, rec.values))


def make_reject(entity, stage, rec, header, reasons, key, store_raw=True) -> Reject:
    original = original_of(rec, header)
    digest = hashlib.sha256(json.dumps(original, sort_keys=True).encode()).hexdigest()
    return Reject(
        entity, stage, rec.source_file, rec.row_number, list(reasons), key,
        original if store_raw else None, digest,
    )  # fmt: skip


def _required(f: dict, name: str, reasons: list[Reason]) -> str | None:
    v = f.get(name, "")
    if v == "":
        reasons.append(("MISSING_REQUIRED", name))
        return None
    return v


def _timestamp(f, name, reasons):
    v = _required(f, name, reasons)
    if v is None:
        return None
    dt = parse_ts(v)
    if dt is None:
        reasons.append(("INVALID_TIMESTAMP", name))
    return dt


def _decimal(f, name, reasons, required=True) -> Decimal | None:
    v = f.get(name, "")
    if v == "":
        if required:
            reasons.append(("MISSING_REQUIRED", name))
        return None
    if not _DECIMAL.match(v):
        reasons.append(("INVALID_DECIMAL", name))
        return None
    return Decimal(v)


def parse_customer(rec: RawRecord, header: tuple[str, ...]):
    if rec.malformed:
        return None, [("MALFORMED_ROW", None)]
    f = _fields(rec, header)
    reasons: list[Reason] = []
    cid = _required(f, "customer_id", reasons)
    if cid is not None and not _CUSTOMER_ID.match(cid):
        reasons.append(("INVALID_FORMAT", "customer_id"))
    email = _required(f, "email", reasons)
    if email is not None and (len(email) > 254 or not _EMAIL.match(email)):
        reasons.append(("INVALID_FORMAT", "email"))
    country = _required(f, "country", reasons)
    if country is not None and country not in COUNTRIES:
        reasons.append(("INVALID_VALUE", "country"))
    segment = f.get("segment") or None  # nullable
    if segment is not None and segment not in SEGMENTS:
        reasons.append(("INVALID_VALUE", "segment"))
    signup = _timestamp(f, "signup_ts", reasons)
    updated = _timestamp(f, "updated_at", reasons)
    if reasons:
        return None, reasons
    return Customer(cid, email, country, segment, signup, updated, rec.source_file, rec.row_number), []


def parse_order(rec: RawRecord, header: tuple[str, ...]):
    if rec.malformed:
        return None, [("MALFORMED_ROW", None)]
    f = _fields(rec, header)
    reasons: list[Reason] = []
    oid = _required(f, "order_id", reasons)
    if oid is not None and not _ORDER_ID.match(oid):
        reasons.append(("INVALID_FORMAT", "order_id"))
    cid = _required(f, "customer_id", reasons)
    if cid is not None and not _CUSTOMER_ID.match(cid):
        reasons.append(("INVALID_FORMAT", "customer_id"))
    order_ts = _timestamp(f, "order_ts", reasons)
    updated = _timestamp(f, "updated_at", reasons)
    status = _required(f, "status", reasons)
    if status is not None and status not in STATUSES:
        reasons.append(("INVALID_VALUE", "status"))
    currency = _required(f, "currency", reasons)
    if currency is not None and currency not in CURRENCIES:
        reasons.append(("INVALID_VALUE", "currency"))
    qty = None
    qv = _required(f, "quantity", reasons)
    if qv is not None:
        if not _INT.match(qv):
            reasons.append(("INVALID_INTEGER", "quantity"))
        else:
            qty = int(qv)
            if not QTY_MIN <= qty <= QTY_MAX:
                reasons.append(("INVALID_VALUE", "quantity"))
    price = _decimal(f, "unit_price", reasons)
    if price is not None and not PRICE_MIN <= price <= PRICE_MAX:
        reasons.append(("INVALID_VALUE", "unit_price"))
    discount = _decimal(f, "discount", reasons, required=False)  # nullable -> 0.00
    if reasons:
        return None, reasons
    return (
        Order(oid, cid, order_ts, updated, status, currency, qty, price,
              discount if discount is not None else Decimal("0"), rec.source_file, rec.row_number),
        [],
    )  # fmt: skip
