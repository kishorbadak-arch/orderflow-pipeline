"""Atomic publication using staging-and-swap.

1. Build a brand-new SQLite file (staging) next to the target.
2. Verify the staging file completely (integrity, FK check, counts, aggregates).
3. Keep the previous target as `warehouse.sqlite.prev` (hard link, copy as fallback).
4. Swap with os.replace (atomic on POSIX/NTFS): readers see the old or the new file, never a mix.
5. Re-verify the published file; if that fails, restore the previous file.

Anything that fails before step 4 leaves the published file untouched.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import sqlite3
import threading
import uuid
from pathlib import Path

from .errors import PublishError
from .fsutil import fsync_dir, fsync_path, ro_uri
from .logging_utils import log
from .schemas import AGG_COLUMNS, DDL, DIM_CUSTOMER_COLUMNS, FACT_ORDERS_COLUMNS, insert_sql
from .timeutil import utc_now_iso
import logging

TARGET_NAME = "warehouse.sqlite"
PREV_NAME = "warehouse.sqlite.prev"
STAGING_PREFIX = "warehouse.sqlite.staging-"


def target_path(output_dir: Path) -> Path:
    return output_dir / TARGET_NAME


def cleanup_stale_staging(output_dir: Path) -> int:
    n = 0
    for p in output_dir.glob(STAGING_PREFIX + "*"):
        p.unlink(missing_ok=True)
        n += 1
    return n


def content_hash(conn: sqlite3.Connection) -> str:
    """SHA-256 over the data tables in primary-key order (excludes run-specific metadata)."""
    h = hashlib.sha256()
    queries = [
        ("dim", "SELECT customer_id, email_domain, country, segment, signup_ts_utc, updated_at_utc "
                "FROM dim_customer ORDER BY customer_id"),
        ("fact", "SELECT order_id, customer_id, order_ts_utc, order_date, status, currency, quantity, "
                 "unit_price_cents, gross_cents, discount_cents, net_cents, recognized_revenue_cents, "
                 "updated_at_utc FROM fact_orders ORDER BY order_id"),
        ("agg", "SELECT * FROM agg_daily_revenue ORDER BY order_date, currency"),
    ]  # fmt: skip
    for tag, sql in queries:
        h.update(tag.encode())
        for row in conn.execute(sql):
            h.update(repr(row).encode())
    return h.hexdigest()


def _build_staging(path: Path, tr, run_id, src_hashes, fast: bool) -> None:
    conn = sqlite3.connect(path)
    try:
        if fast:
            # Safe only because the staging file is disposable: on a crash it is deleted and rebuilt.
            conn.execute("PRAGMA journal_mode = OFF")
            conn.execute("PRAGMA synchronous = OFF")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript(DDL)
        with conn:
            conn.executemany(insert_sql("dim_customer", DIM_CUSTOMER_COLUMNS), tr.dim_customers)
            conn.executemany(insert_sql("fact_orders", FACT_ORDERS_COLUMNS), tr.fact_orders)
            conn.executemany(insert_sql("agg_daily_revenue", AGG_COLUMNS), tr.agg_daily)
        digest = content_hash(conn)
        with conn:
            conn.execute(
                "INSERT INTO publication VALUES (?,?,?,?,?,?,?,?,?,?)",
                (run_id, utc_now_iso(), len(tr.dim_customers), len(tr.fact_orders), len(tr.agg_daily),
                 tr.net_cents_total, tr.max_updated_at_utc, digest, src_hashes[0], src_hashes[1]),
            )  # fmt: skip
    finally:
        conn.close()


def _verify(conn: sqlite3.Connection, tr, run_id: str | None = None, check_hash: bool = True) -> None:
    """Raise PublishError if the database does not match what we meant to publish."""
    if conn.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
        raise PublishError("integrity_check failed")
    if conn.execute("PRAGMA foreign_key_check").fetchall():
        raise PublishError("foreign key check failed")
    n_dim = conn.execute("SELECT COUNT(*) FROM dim_customer").fetchone()[0]
    n_fact, net = conn.execute("SELECT COUNT(*), COALESCE(SUM(net_cents),0) FROM fact_orders").fetchone()
    if (n_dim, n_fact, net) != (len(tr.dim_customers), len(tr.fact_orders), tr.net_cents_total):
        raise PublishError("row counts or net total do not match the transformed data")
    agg = conn.execute(
        "SELECT COALESCE(SUM(orders),0), COALESCE(SUM(net_cents),0), COALESCE(SUM(recognized_revenue_cents),0) "
        "FROM agg_daily_revenue"
    ).fetchone()
    if agg != (n_fact, net, tr.recognized_cents_total):
        raise PublishError("daily aggregate does not reconcile to fact_orders")
    pub = conn.execute("SELECT run_id, content_sha256 FROM publication").fetchall()
    if len(pub) != 1 or (run_id is not None and pub[0][0] != run_id):
        raise PublishError("publication metadata mismatch")
    if check_hash and pub[0][1] != content_hash(conn):
        raise PublishError("content hash mismatch")


def _swap(staging: Path, target: Path) -> None:
    os.replace(staging, target)


def _verify_published(target: Path, tr, run_id: str) -> None:
    conn = sqlite3.connect(ro_uri(target), uri=True)
    try:
        _verify(conn, tr, run_id)
    finally:
        conn.close()


def _keep_previous(target: Path, prev: Path) -> bool:
    if not target.exists():
        return False
    tmp = prev.with_name(prev.name + ".tmp")
    tmp.unlink(missing_ok=True)
    try:
        os.link(target, tmp)
    except OSError:
        shutil.copy2(target, tmp)
    os.replace(tmp, prev)
    return True


def read_published_meta(output_dir: Path) -> dict | None:
    """Metadata of the currently published snapshot, or None if there is none/it is unreadable."""
    target = target_path(output_dir)
    if not target.exists():
        return None
    try:
        conn = sqlite3.connect(ro_uri(target), uri=True)
        try:
            row = conn.execute(
                "SELECT run_id, published_at_utc, max_updated_at_utc, content_sha256 FROM publication"
            ).fetchone()
        finally:
            conn.close()
    except sqlite3.DatabaseError:
        log(logging.WARNING, "published_target_unreadable")
        return None
    if row is None:
        return None
    return dict(zip(("run_id", "published_at_utc", "max_updated_at_utc", "content_sha256"), row))


def publish(output_dir: Path, tr, run_id: str, src_hashes: tuple[str, str], cancel: threading.Event, fast: bool, skip_staging_rehash: bool = True) -> dict:
    target = target_path(output_dir)
    prev = output_dir / PREV_NAME
    staging = output_dir / f"{STAGING_PREFIX}{uuid.uuid4().hex[:10]}"
    try:
        _build_staging(staging, tr, run_id, src_hashes, fast)
        conn = sqlite3.connect(ro_uri(staging), uri=True)
        try:
            # The content hash is computed once at build time and re-checked on the final published
            # file (with automatic rollback), so the staging check skips a third full pass.
            _verify(conn, tr, run_id, check_hash=not skip_staging_rehash)
            digest = conn.execute("SELECT content_sha256 FROM publication").fetchone()[0]
        finally:
            conn.close()
        os.chmod(staging, 0o600)
        fsync_path(staging)
        if cancel.is_set():  # this attempt timed out and was abandoned: do not swap
            raise PublishError("attempt cancelled before swap")
        had_prev = _keep_previous(target, prev)
        _swap(staging, target)
        fsync_dir(output_dir)
        try:
            _verify_published(target, tr, run_id)
        except Exception:
            if had_prev:
                os.replace(prev, target)
            else:
                target.unlink(missing_ok=True)
            fsync_dir(output_dir)
            log(logging.ERROR, "publication_rolled_back", run_id=run_id)
            raise PublishError("post-publication verification failed; previous output restored") from None
    except PublishError:
        raise
    except Exception as exc:
        raise PublishError(f"publication failed before swap ({type(exc).__name__}); previous output untouched") from None
    finally:
        staging.unlink(missing_ok=True)
    return {"content_sha256": digest, "customers_rows": len(tr.dim_customers),
            "orders_rows": len(tr.fact_orders), "agg_rows": len(tr.agg_daily)}  # fmt: skip
