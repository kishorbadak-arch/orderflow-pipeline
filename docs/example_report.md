# Validation & reconciliation report: run `sample-run`

## Row counts

| entity | input | schema rejected | duplicates superseded | business rejected | accepted |
|---|---:|---:|---:|---:|---:|
| customers | 11 | 3 | 1 | 0 | 7 |
| orders | 41 | 3 | 1 | 5 | 32 |

Total: input **52** = accepted **39** + rejected **13**; published output rows (customers + orders) = **39**.

## Checks

- PASS: accepted rows equal transformed rows
- PASS: input = accepted + rejected (per entity)
- PASS: net total recomputed independently
- PASS: no rejected row lost in quarantine
- PASS: reject ratio <= 0.3

Reject ratio: 0.2500 (limit 0.3)

## Rejects by primary reason

- `DISCOUNT_EXCEEDS_GROSS`: 1
- `DUPLICATE_SUPERSEDED`: 2
- `FUTURE_TIMESTAMP`: 1
- `INVALID_REFERENCE`: 2
- `INVALID_VALUE`: 2
- `MALFORMED_ROW`: 2
- `MISSING_REQUIRED`: 2
- `ORDER_BEFORE_SIGNUP`: 1

## Example queries on the published target

### busiest_days

```
SELECT order_date, currency, orders, recognized_revenue_cents FROM agg_daily_revenue ORDER BY orders DESC, order_date, currency LIMIT 5
```

```json
[
 {
  "currency": "USD",
  "order_date": "2025-02-01",
  "orders": 1,
  "recognized_revenue_cents": 2000
 },
 {
  "currency": "GBP",
  "order_date": "2025-02-02",
  "orders": 1,
  "recognized_revenue_cents": 2997
 },
 {
  "currency": "INR",
  "order_date": "2025-02-02",
  "orders": 1,
  "recognized_revenue_cents": 100000
 },
 {
  "currency": "USD",
  "order_date": "2025-02-02",
  "orders": 1,
  "recognized_revenue_cents": 2000
 },
 {
  "currency": "EUR",
  "order_date": "2025-02-20",
  "orders": 1,
  "recognized_revenue_cents": 0
 }
]
```

### orders_by_country

```
SELECT c.country, COUNT(*) AS orders FROM fact_orders o JOIN dim_customer c USING (customer_id) GROUP BY c.country ORDER BY orders DESC, c.country LIMIT 10
```

```json
[
 {
  "country": "FR",
  "orders": 6
 },
 {
  "country": "US",
  "orders": 6
 },
 {
  "country": "DE",
  "orders": 5
 },
 {
  "country": "GB",
  "orders": 5
 },
 {
  "country": "IN",
  "orders": 5
 },
 {
  "country": "CA",
  "orders": 4
 },
 {
  "country": "JP",
  "orders": 1
 }
]
```

### orders_by_status

```
SELECT status, COUNT(*) AS orders FROM fact_orders GROUP BY status ORDER BY status
```

```json
[
 {
  "orders": 1,
  "status": "CANCELLED"
 },
 {
  "orders": 2,
  "status": "DELIVERED"
 },
 {
  "orders": 26,
  "status": "PLACED"
 },
 {
  "orders": 3,
  "status": "SHIPPED"
 }
]
```

### revenue_by_currency

```
SELECT currency, COUNT(*) AS orders, SUM(net_cents) AS net_cents, SUM(recognized_revenue_cents) AS recognized_cents FROM fact_orders GROUP BY currency ORDER BY currency
```

```json
[
 {
  "currency": "EUR",
  "net_cents": 5000,
  "orders": 1,
  "recognized_cents": 0
 },
 {
  "currency": "GBP",
  "net_cents": 2997,
  "orders": 1,
  "recognized_cents": 2997
 },
 {
  "currency": "INR",
  "net_cents": 100000,
  "orders": 1,
  "recognized_cents": 100000
 },
 {
  "currency": "USD",
  "net_cents": 130004,
  "orders": 29,
  "recognized_cents": 130004
 }
]
```

