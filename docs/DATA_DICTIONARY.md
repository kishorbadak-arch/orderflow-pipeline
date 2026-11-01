# Data dictionary

All timestamps: ISO-8601 **with explicit timezone** (`Z` or `+hh:mm`); naive timestamps are rejected. They are
stored in UTC as `YYYY-MM-DDTHH:MM:SS.ffffffZ` (fixed width, so text order = time order).
Money: input is a decimal with at most 2 decimals (`^\d{1,7}(\.\d{1,2})?$`, no sign/exponent/NaN); stored as integer
**cents** (no binary floating point).

## Source: `customers.csv` (grain: one row per `customer_id`, after de-duplication)
| column | type | null? | rule |
|---|---|---|---|
| customer_id | text, `C` + 6 digits | no | primary key |
| email | text | no | simple address pattern, <= 254 chars |
| country | text | no | one of US GB DE FR IN CA AU JP BR SG |
| segment | text | **yes** | CONSUMER / SMB / ENTERPRISE; empty means unknown |
| signup_ts | timestamp+tz | no | |
| updated_at | timestamp+tz | no | version marker for duplicates |

## Source: `orders.csv` (grain: one row per `order_id`, after de-duplication)
| column | type | null? | rule |
|---|---|---|---|
| order_id | text, `O` + 8 digits | no | primary key |
| customer_id | text | no | must exist among **accepted** customers (foreign key) |
| order_ts | timestamp+tz | no | |
| updated_at | timestamp+tz | no | version marker for duplicates |
| status | text | no | PLACED / SHIPPED / DELIVERED / CANCELLED |
| currency | text | no | USD EUR GBP INR |
| quantity | integer | no | 1..1000 inclusive |
| unit_price | decimal(<=2dp) | no | 0.01..100000.00 inclusive |
| discount | decimal(<=2dp) | **yes** | empty means 0; >= 0 |

Header must contain exactly these columns (any order). Missing, extra or duplicate columns fail the batch.

## Reason codes (primary code = first reason; all reasons are stored)
| code | stage | meaning |
|---|---|---|
| MALFORMED_ROW | schema | field count differs from header |
| MISSING_REQUIRED | schema | required value empty |
| INVALID_FORMAT | schema | id/e-mail pattern mismatch |
| INVALID_VALUE | schema | not in allowed set or outside numeric range (quantity 0/1001, price 0.00, bad country...) |
| INVALID_INTEGER / INVALID_DECIMAL | schema | not an integer / not a decimal with <= 2 places |
| INVALID_TIMESTAMP | schema | not strict ISO-8601 with timezone |
| DUPLICATE_SUPERSEDED | duplicate | another row with the same key has a greater `updated_at` (tie: later row wins) |
| INVALID_REFERENCE | business | `customer_id` not among accepted customers (also when that customer was itself rejected) |
| FUTURE_TIMESTAMP | business | any timestamp > `as_of` (equal is allowed) |
| UPDATED_BEFORE_CREATED | business | `updated_at` < `order_ts` / `signup_ts` (equal allowed) |
| ORDER_BEFORE_SIGNUP | business | `order_ts` < customer `signup_ts` (equal allowed) |
| DISCOUNT_EXCEEDS_GROSS | business | discount > quantity x unit_price (equal allowed, net = 0) |

Order of evaluation is fixed: schema -> duplicates -> customer rules -> order rules
(`INVALID_REFERENCE` first and it stops further checks; then FUTURE, UPDATED_BEFORE_CREATED, ORDER_BEFORE_SIGNUP,
DISCOUNT_EXCEEDS_GROSS). The winning duplicate is chosen **before** business rules: if the newest version is invalid,
the key is rejected rather than falling back to an older version (no resurrecting stale data).

## Target (`warehouse.sqlite`)
**dim_customer** (PK `customer_id`): `email_domain` (domain only), `country`, `segment` (`UNKNOWN` if null),
`signup_ts_utc`, `updated_at_utc`. One row per accepted customer.

**fact_orders** (PK `order_id`, FK `customer_id`): `order_ts_utc`, `order_date` (UTC date), `status`, `currency`,
`quantity`, `unit_price_cents`, `gross_cents = quantity*unit_price_cents`, `discount_cents`, `net_cents = gross - discount`,
`recognized_revenue_cents` (= 0 for CANCELLED else net), `updated_at_utc`, `source_run_id`. One row per accepted order.

**agg_daily_revenue** (PK `order_date, currency`): `orders, units, gross_cents, discount_cents, net_cents, recognized_revenue_cents`.

**publication** (one row): `run_id, published_at_utc, customers_rows, orders_rows, agg_rows, net_cents_total,
max_updated_at_utc, content_sha256` (hash of the three data tables in key order), `source_customers_sha256, source_orders_sha256`.

## Quarantine (`quarantine/<run_id>.jsonl`, one JSON object per rejected row)
`run_id, entity, stage, source_file, row_number` (physical line number; header = 1), `record_key, reason_code, reasons[{code,field}],
original` (original values; `null` with `--no-raw-quarantine`), `original_sha256`.

## Count relationships
`input_rows` (data rows read, excluding header and blank lines) `= accepted_rows + rejected_rows` for each entity and in total.
`output_rows = dim_customer rows + fact_orders rows = accepted_rows` (transformation is 1:1). `agg_daily_revenue` has its own count (`output_agg_rows`).
