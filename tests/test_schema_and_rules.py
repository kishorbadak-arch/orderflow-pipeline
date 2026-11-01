"""Unit tests: record-level schema validation, duplicate policy and business-rule boundaries."""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from orderflow.business import customer_reasons, order_reasons
from orderflow.dedupe import resolve_duplicates
from orderflow.ingest import CUSTOMER_COLUMNS, ORDER_COLUMNS
from orderflow.models import RawRecord
from orderflow.schema import parse_customer, parse_order
from orderflow.timeutil import parse_ts

GOOD_C = ["C000001", "a@example.com", "US", "SMB", "2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z"]
GOOD_O = ["O00000001", "C000001", "2025-02-01T10:00:00Z", "2025-02-01T10:00:00Z", "PLACED", "USD", "2", "10.00", "0.00"]
AS_OF = datetime(2025, 12, 31, tzinfo=timezone.utc)


def rec(values, row=2):
    return RawRecord("f.csv", row, tuple(values), False)


def codes(reasons):
    return [c for c, _ in reasons]


def with_(row, idx, value):
    r = list(row)
    r[idx] = value
    return r


def test_valid_customer_and_order_parse():
    c, reasons = parse_customer(rec(GOOD_C), CUSTOMER_COLUMNS)
    assert reasons == [] and c.customer_id == "C000001" and c.segment == "SMB"
    o, reasons = parse_order(rec(GOOD_O), ORDER_COLUMNS)
    assert reasons == [] and o.unit_price == Decimal("10.00") and o.quantity == 2


def test_null_segment_and_null_discount_are_allowed():
    c, reasons = parse_customer(rec(with_(GOOD_C, 3, "")), CUSTOMER_COLUMNS)
    assert reasons == [] and c.segment is None
    o, reasons = parse_order(rec(with_(GOOD_O, 8, "")), ORDER_COLUMNS)
    assert reasons == [] and o.discount == Decimal("0")


@pytest.mark.parametrize("idx,value,expected", [
    (0, "", "MISSING_REQUIRED"), (0, "X1", "INVALID_FORMAT"), (1, "", "MISSING_REQUIRED"),
    (1, "not-an-email", "INVALID_FORMAT"), (2, "ZZ", "INVALID_VALUE"), (3, "VIP", "INVALID_VALUE"),
    (4, "2025-01-01 00:00:00", "INVALID_TIMESTAMP"),  # naive: no timezone
    (4, "2025-13-01T00:00:00Z", "INVALID_TIMESTAMP"), (5, "", "MISSING_REQUIRED"),
])  # fmt: skip
def test_customer_schema_rejections(idx, value, expected):
    _, reasons = parse_customer(rec(with_(GOOD_C, idx, value)), CUSTOMER_COLUMNS)
    assert expected in codes(reasons)


@pytest.mark.parametrize("idx,value,expected", [
    (0, "O1", "INVALID_FORMAT"), (1, "", "MISSING_REQUIRED"), (2, "2025-02-01", "INVALID_TIMESTAMP"),
    (4, "REFUNDED", "INVALID_VALUE"), (5, "JPY", "INVALID_VALUE"), (6, "", "MISSING_REQUIRED"),
    (6, "1.5", "INVALID_INTEGER"), (6, "-1", "INVALID_INTEGER"), (6, "0", "INVALID_VALUE"),
    (6, "1001", "INVALID_VALUE"), (7, "12.345", "INVALID_DECIMAL"), (7, "1e3", "INVALID_DECIMAL"),
    (7, "NaN", "INVALID_DECIMAL"), (7, "-5.00", "INVALID_DECIMAL"), (7, "0.00", "INVALID_VALUE"),
    (8, "-1.00", "INVALID_DECIMAL"),
])  # fmt: skip
def test_order_schema_rejections(idx, value, expected):
    _, reasons = parse_order(rec(with_(GOOD_O, idx, value)), ORDER_COLUMNS)
    assert expected in codes(reasons)


@pytest.mark.parametrize("qty,price,ok", [
    ("1", "0.01", True), ("1000", "100000.00", True),  # inclusive boundaries
    ("0", "10.00", False), ("1001", "10.00", False), ("1", "100000.01", False),
])  # fmt: skip
def test_quantity_and_price_boundaries(qty, price, ok):
    row = with_(with_(GOOD_O, 6, qty), 7, price)
    _, reasons = parse_order(rec(row), ORDER_COLUMNS)
    assert (reasons == []) is ok


def test_malformed_row_is_record_level_reject():
    bad = RawRecord("f.csv", 5, ("C000001", "a@example.com"), True)
    assert codes(parse_customer(bad, CUSTOMER_COLUMNS)[1]) == ["MALFORMED_ROW"]


def test_timestamp_offsets_are_normalised_to_utc():
    assert parse_ts("2025-02-03T00:30:00+05:30") == datetime(2025, 2, 2, 19, 0, tzinfo=timezone.utc)
    assert parse_ts("2025-02-03T00:30:00") is None


# ---- business rules: boundaries are inclusive where documented
def _order(**kw):
    o, reasons = parse_order(rec(GOOD_O), ORDER_COLUMNS)
    assert not reasons
    from dataclasses import replace
    return replace(o, **kw)


def _customer(**kw):
    c, _ = parse_customer(rec(GOOD_C), CUSTOMER_COLUMNS)
    from dataclasses import replace
    return replace(c, **kw)


def test_order_before_signup_boundary():
    cust = _customer(signup_ts=datetime(2025, 2, 1, 10, 0, tzinfo=timezone.utc))
    by_id = {cust.customer_id: cust}
    assert order_reasons(_order(), by_id, AS_OF) == []  # equal timestamps are allowed
    cust2 = _customer(signup_ts=datetime(2025, 2, 1, 10, 0, 1, tzinfo=timezone.utc))
    assert codes(order_reasons(_order(), {cust2.customer_id: cust2}, AS_OF)) == ["ORDER_BEFORE_SIGNUP"]


def test_discount_equal_to_gross_is_allowed_but_one_cent_more_is_not():
    cust = _customer()
    by_id = {cust.customer_id: cust}
    assert order_reasons(_order(discount=Decimal("20.00")), by_id, AS_OF) == []
    assert codes(order_reasons(_order(discount=Decimal("20.01")), by_id, AS_OF)) == ["DISCOUNT_EXCEEDS_GROSS"]


def test_future_timestamp_boundary_uses_as_of():
    cust = _customer()
    by_id = {cust.customer_id: cust}
    at = _order(order_ts=AS_OF, updated_at=AS_OF)
    assert order_reasons(at, by_id, AS_OF) == []
    later = datetime(2025, 12, 31, 0, 0, 1, tzinfo=timezone.utc)
    assert "FUTURE_TIMESTAMP" in codes(order_reasons(_order(order_ts=later, updated_at=later), by_id, AS_OF))


def test_updated_before_created_and_invalid_reference():
    by_id = {"C000001": _customer()}
    early = datetime(2025, 2, 1, 9, 59, tzinfo=timezone.utc)
    assert codes(order_reasons(_order(updated_at=early), by_id, AS_OF)) == ["UPDATED_BEFORE_CREATED"]
    assert codes(order_reasons(_order(customer_id="C000999"), by_id, AS_OF)) == ["INVALID_REFERENCE"]


def test_customer_rules():
    assert customer_reasons(_customer(), AS_OF) == []
    assert codes(customer_reasons(_customer(updated_at=datetime(2024, 1, 1, tzinfo=timezone.utc)), AS_OF)) == ["UPDATED_BEFORE_CREATED"]
    far = datetime(2099, 1, 1, tzinfo=timezone.utc)
    assert "FUTURE_TIMESTAMP" in codes(customer_reasons(_customer(signup_ts=far, updated_at=far), AS_OF))


# ---- duplicate policy
def test_duplicate_latest_updated_at_wins_regardless_of_file_order():
    new = _order(updated_at=datetime(2025, 3, 1, tzinfo=timezone.utc), status="DELIVERED", row_number=2)
    old = _order(updated_at=datetime(2025, 2, 2, tzinfo=timezone.utc), status="PLACED", row_number=3)
    for items in ([new, old], [old, new]):
        winners, losers = resolve_duplicates(items, lambda o: o.order_id)
        assert [w.status for w in winners] == ["DELIVERED"] and [lo.status for lo in losers] == ["PLACED"]


def test_duplicate_tie_is_broken_by_higher_row_number():
    a = _order(row_number=2, status="PLACED")
    b = _order(row_number=7, status="SHIPPED")
    winners, losers = resolve_duplicates([a, b], lambda o: o.order_id)
    assert winners == [b] and losers == [a]
