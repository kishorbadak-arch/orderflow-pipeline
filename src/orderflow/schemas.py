"""Output (target) schemas. DDL and column lists live together so they cannot drift apart."""

DIM_CUSTOMER_COLUMNS = ("customer_id", "email_domain", "country", "segment", "signup_ts_utc", "updated_at_utc")
FACT_ORDERS_COLUMNS = (
    "order_id", "customer_id", "order_ts_utc", "order_date", "status", "currency", "quantity",
    "unit_price_cents", "gross_cents", "discount_cents", "net_cents", "recognized_revenue_cents",
    "updated_at_utc", "source_run_id",
)  # fmt: skip
AGG_COLUMNS = (
    "order_date", "currency", "orders", "units", "gross_cents", "discount_cents",
    "net_cents", "recognized_revenue_cents",
)  # fmt: skip

DDL = """
CREATE TABLE dim_customer (
    customer_id   TEXT PRIMARY KEY,
    email_domain  TEXT NOT NULL,
    country       TEXT NOT NULL,
    segment       TEXT NOT NULL,
    signup_ts_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL
) WITHOUT ROWID;
CREATE TABLE fact_orders (
    order_id      TEXT PRIMARY KEY,
    customer_id   TEXT NOT NULL REFERENCES dim_customer(customer_id),
    order_ts_utc  TEXT NOT NULL,
    order_date    TEXT NOT NULL,
    status        TEXT NOT NULL,
    currency      TEXT NOT NULL,
    quantity      INTEGER NOT NULL CHECK (quantity BETWEEN 1 AND 1000),
    unit_price_cents INTEGER NOT NULL CHECK (unit_price_cents > 0),
    gross_cents   INTEGER NOT NULL,
    discount_cents INTEGER NOT NULL CHECK (discount_cents >= 0),
    net_cents     INTEGER NOT NULL CHECK (net_cents >= 0),
    recognized_revenue_cents INTEGER NOT NULL,
    updated_at_utc TEXT NOT NULL,
    source_run_id TEXT NOT NULL
) WITHOUT ROWID;
CREATE TABLE agg_daily_revenue (
    order_date TEXT NOT NULL,
    currency   TEXT NOT NULL,
    orders     INTEGER NOT NULL,
    units      INTEGER NOT NULL,
    gross_cents INTEGER NOT NULL,
    discount_cents INTEGER NOT NULL,
    net_cents  INTEGER NOT NULL,
    recognized_revenue_cents INTEGER NOT NULL,
    PRIMARY KEY (order_date, currency)
) WITHOUT ROWID;
CREATE TABLE publication (
    run_id TEXT PRIMARY KEY,
    published_at_utc TEXT NOT NULL,
    customers_rows INTEGER NOT NULL,
    orders_rows INTEGER NOT NULL,
    agg_rows INTEGER NOT NULL,
    net_cents_total INTEGER NOT NULL,
    max_updated_at_utc TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    source_customers_sha256 TEXT NOT NULL,
    source_orders_sha256 TEXT NOT NULL
);
CREATE INDEX idx_fact_orders_customer ON fact_orders(customer_id);
CREATE INDEX idx_fact_orders_date ON fact_orders(order_date);
"""


def insert_sql(table: str, columns: tuple[str, ...]) -> str:
    """Parameterized INSERT. Table/column names come from the constants above, never from data."""
    marks = ", ".join("?" for _ in columns)
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({marks})"
