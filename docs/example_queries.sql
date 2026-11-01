-- Run with: sqlite3 output/warehouse.sqlite < docs/example_queries.sql
-- (or python -c "import sqlite3; ..."). Amounts are integer cents (divide by 100 for display).

-- 1. Revenue by currency (recognized revenue excludes CANCELLED orders)
SELECT currency, COUNT(*) AS orders, SUM(net_cents) AS net_cents, SUM(recognized_revenue_cents) AS recognized_cents
FROM fact_orders GROUP BY currency ORDER BY currency;

-- 2. Reconciliation: the daily aggregate must equal the fact table (expect one row: 1)
SELECT (SELECT SUM(orders) FROM agg_daily_revenue) = (SELECT COUNT(*) FROM fact_orders)
   AND (SELECT SUM(net_cents) FROM agg_daily_revenue) = (SELECT SUM(net_cents) FROM fact_orders) AS reconciles;

-- 3. Referential integrity: orders without a customer (expect 0)
SELECT COUNT(*) FROM fact_orders o LEFT JOIN dim_customer c USING (customer_id) WHERE c.customer_id IS NULL;

-- 4. Which run produced this snapshot, and from which source files?
SELECT run_id, published_at_utc, customers_rows, orders_rows, content_sha256, source_customers_sha256, source_orders_sha256
FROM publication;

-- 5. Orders per customer country
SELECT c.country, COUNT(*) AS orders FROM fact_orders o JOIN dim_customer c USING (customer_id)
GROUP BY c.country ORDER BY orders DESC, c.country;
