"""Business-rule validation (after schema validation and de-duplication).

Rule order is fixed, and every applicable reason is recorded in that order:
  customers: FUTURE_TIMESTAMP, UPDATED_BEFORE_CREATED
  orders:    INVALID_REFERENCE (stops further checks: they need the customer),
             FUTURE_TIMESTAMP, UPDATED_BEFORE_CREATED, ORDER_BEFORE_SIGNUP, DISCOUNT_EXCEEDS_GROSS
Boundaries are inclusive where stated in docs/DATA_DICTIONARY.md (e.g. order_ts == signup_ts is OK,
discount == gross is OK, timestamp == as_of is OK).
"""

from __future__ import annotations


def customer_reasons(c, as_of):
    reasons = []
    if c.signup_ts > as_of or c.updated_at > as_of:
        reasons.append(("FUTURE_TIMESTAMP", "signup_ts" if c.signup_ts > as_of else "updated_at"))
    if c.updated_at < c.signup_ts:
        reasons.append(("UPDATED_BEFORE_CREATED", "updated_at"))
    return reasons


def order_reasons(o, customers_by_id, as_of):
    cust = customers_by_id.get(o.customer_id)
    if cust is None:
        return [("INVALID_REFERENCE", "customer_id")]
    reasons = []
    if o.order_ts > as_of or o.updated_at > as_of:
        reasons.append(("FUTURE_TIMESTAMP", "order_ts" if o.order_ts > as_of else "updated_at"))
    if o.updated_at < o.order_ts:
        reasons.append(("UPDATED_BEFORE_CREATED", "updated_at"))
    if o.order_ts < cust.signup_ts:
        reasons.append(("ORDER_BEFORE_SIGNUP", "order_ts"))
    if o.discount > o.quantity * o.unit_price:
        reasons.append(("DISCOUNT_EXCEEDS_GROSS", "discount"))
    return reasons
