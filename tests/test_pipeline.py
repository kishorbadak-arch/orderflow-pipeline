"""End-to-end scenarios against temp directories (no network, no real data)."""

import json
import shutil
import time

import pytest

import orderflow.pipeline as pl
import orderflow.publish as pub
from conftest import GOOD_CUSTOMERS, GOOD_ORDERS, SAMPLE_INPUT
from orderflow.errors import ConfigError, LockHeldError, TransientError
from orderflow.generate import generate
from orderflow.runtracker import RunTracker

EXPECTED = json.loads((__import__("pathlib").Path(__file__).parent / "expected" / "sample_expected.json").read_text())


def run(cfg, run_id="r1", sleep=lambda s: None):
    return pl.run_pipeline(cfg, run_id=run_id, sleep=sleep)


def task_status(res):
    return {n: t.status for n, t in res.tasks.items()}


# ------------------------------------------------------------------ happy path & sample
def test_happy_path(make_input, cfg_factory, db_of):
    cfg = cfg_factory(make_input())
    res = run(cfg)
    assert res.status == "SUCCESS" and res.exit_code == 0
    m = res.metrics
    assert (m["input_rows"], m["accepted_rows"], m["rejected_rows"], m["output_rows"]) == (6, 6, 0, 6)
    db = db_of(cfg)
    assert db.q("SELECT COUNT(*) FROM dim_customer") == [(3,)]
    # O1: 2*10.00; O2: 25.50-5.50; O3: cancelled -> net 30000 but recognized 0
    assert db.q("SELECT order_id, net_cents, recognized_revenue_cents FROM fact_orders ORDER BY 1") == [
        ("O00000001", 2000, 2000), ("O00000002", 2000, 2000), ("O00000003", 30000, 0)]
    assert db.q("SELECT email_domain, segment FROM dim_customer WHERE customer_id='C000003'") == [("example.com", "UNKNOWN")]


def test_sample_dataset_matches_hand_derived_expectations(cfg_factory, db_of, quarantine_of):
    cfg = cfg_factory(SAMPLE_INPUT)
    res = run(cfg)
    assert res.status == "SUCCESS"
    c = EXPECTED["counts"]
    m = res.metrics
    assert (m["input_rows"], m["accepted_rows"], m["rejected_rows"], m["output_rows"]) == (
        c["input_total"], c["accepted_total"], c["rejected_total"], c["output_rows"])
    assert m["entities"]["customers"]["accepted"] == c["customers"]["accepted"]
    assert m["entities"]["orders"]["accepted"] == c["orders"]["accepted"]

    got = {}
    for r in quarantine_of(cfg, "r1"):
        got.setdefault(r["source_file"], {})[str(r["row_number"])] = r["reason_code"]
    assert got == EXPECTED["rejects"]

    db = db_of(cfg)
    assert dict(db.q("SELECT currency, SUM(net_cents) FROM fact_orders GROUP BY 1")) == EXPECTED["net_cents_by_currency"]
    assert dict(db.q("SELECT currency, SUM(recognized_revenue_cents) FROM fact_orders GROUP BY 1")) == EXPECTED["recognized_cents_by_currency"]
    assert db.q("SELECT SUM(net_cents) FROM fact_orders") == [(EXPECTED["net_cents_total"],)]
    assert db.q("SELECT segment FROM dim_customer WHERE customer_id='C000002'") == [("ENTERPRISE",)]
    assert db.q("SELECT status FROM fact_orders WHERE order_id='O00000013'") == [("DELIVERED",)]
    tz = EXPECTED["timezone_normalisation"]["O00000004"]
    assert db.q("SELECT order_ts_utc, order_date FROM fact_orders WHERE order_id='O00000004'") == [(tz["order_ts_utc"], tz["order_date"])]
    assert [list(r) for r in db.q("SELECT currency, orders, units, net_cents FROM agg_daily_revenue WHERE order_date='2025-02-02' ORDER BY 1")] \
        == EXPECTED["daily_2025_02_02"]
    assert db.q("SELECT orders, units, net_cents FROM agg_daily_revenue WHERE order_date='2025-02-02' AND currency='USD'") == [(1, 1, 2000)]


# ------------------------------------------------------------------ input problems
def test_empty_input_fails_and_never_publishes(make_input, cfg_factory, db_of):
    cfg = cfg_factory(make_input(customers=[], orders=[]))
    res = run(cfg)
    assert res.status == "FAILED" and res.failed_task == "quality_gate" and res.error_type == "EmptyInputError"
    assert task_status(res)["publish"] == "SKIPPED"
    assert not (cfg.output_dir / "warehouse.sqlite").exists()


def test_empty_input_does_not_wipe_previous_output(make_input, cfg_factory, db_of):
    cfg = cfg_factory(make_input())
    run(cfg, "good")
    before = db_of(cfg).snapshot()
    res = run(cfg_factory(make_input(customers=[], orders=[], name="empty")), "empty")
    assert res.status == "FAILED"
    assert db_of(cfg).snapshot() == before


@pytest.mark.parametrize("c_header", [
    ["customer_id", "email", "country", "segment", "signup_ts", "updated_at", "loyalty_points"],  # extra column
    ["customer_id", "email", "country", "segment", "signup_ts"],  # missing column
])
def test_schema_change_is_a_batch_failure_without_retries(make_input, cfg_factory, db_of, c_header):
    cfg = cfg_factory(make_input())
    run(cfg, "good")
    before = db_of(cfg).snapshot()
    bad = make_input(customers=[], name="drift", c_header=c_header)
    res = run(cfg_factory(bad), "drift")
    assert res.status == "FAILED" and res.failed_task == "ingest_customers" and res.error_type == "BatchSchemaError"
    assert res.tasks["ingest_customers"].attempts == 1  # permanent: not retried
    assert task_status(res)["publish"] == "SKIPPED"
    assert db_of(cfg).snapshot() == before


def test_malformed_file_fails_batch(make_input, cfg_factory):
    d = make_input()
    (d / "orders.csv").write_bytes(b"order_id,customer_id\n\xff\xfe\x00bad")
    res = run(cfg_factory(d))
    assert res.status == "FAILED" and res.error_type in ("MalformedFileError", "BatchSchemaError")


def test_unsupported_file_type(make_input, cfg_factory):
    d = make_input()
    shutil.copy(d / "orders.csv", d / "orders.txt")
    res = run(cfg_factory(d, orders_file="orders.txt"))
    assert res.status == "FAILED" and res.error_type == "UnsupportedFileTypeError"


def test_missing_required_null_and_malformed_rows_are_quarantined_with_context(make_input, cfg_factory, quarantine_of):
    customers = GOOD_CUSTOMERS + [["C000004", "", "US", "SMB", "2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z"], ["C000005", "x@example.com"]]
    orders = GOOD_ORDERS + [["O00000004", "C000001", "2025-02-01T10:00:00Z", "2025-02-01T10:00:00Z", "PLACED", "USD", "", "10.00", "0"]]
    cfg = cfg_factory(make_input(customers, orders), max_reject_ratio=0.5)
    res = run(cfg)
    assert res.status == "SUCCESS" and res.metrics["rejected_rows"] == 3
    rows = {(r["source_file"], r["row_number"]): r for r in quarantine_of(cfg, "r1")}
    assert rows[("customers.csv", 5)]["reasons"] == [{"code": "MISSING_REQUIRED", "field": "email"}]
    assert rows[("customers.csv", 6)]["reason_code"] == "MALFORMED_ROW"
    assert rows[("orders.csv", 5)]["reasons"] == [{"code": "MISSING_REQUIRED", "field": "quantity"}]
    assert all(r["run_id"] == "r1" and len(r["original_sha256"]) == 64 for r in rows.values())


def test_duplicate_primary_key_is_resolved_deterministically(make_input, cfg_factory, quarantine_of, db_of):
    newer = list(GOOD_ORDERS[0]); newer[3] = "2025-02-05T10:00:00Z"; newer[4] = "DELIVERED"
    cfg = cfg_factory(make_input(orders=[newer] + GOOD_ORDERS), max_reject_ratio=0.5)  # newer version appears FIRST
    res = run(cfg)
    assert res.status == "SUCCESS"
    assert db_of(cfg).q("SELECT status FROM fact_orders WHERE order_id='O00000001'") == [("DELIVERED",)]
    dup = [r for r in quarantine_of(cfg, "r1") if r["stage"] == "duplicate"]
    assert [(r["row_number"], r["reason_code"]) for r in dup] == [(3, "DUPLICATE_SUPERSEDED")]


def test_invalid_reference_is_rejected_not_published(make_input, cfg_factory, quarantine_of, db_of):
    orphan = ["O00000009", "C000777", "2025-02-01T10:00:00Z", "2025-02-01T10:00:00Z", "PLACED", "USD", "1", "5.00", "0"]
    cfg = cfg_factory(make_input(orders=GOOD_ORDERS + [orphan]), max_reject_ratio=0.5)
    res = run(cfg)
    assert res.status == "SUCCESS"
    assert db_of(cfg).q("SELECT COUNT(*) FROM fact_orders WHERE order_id='O00000009'") == [(0,)]
    assert [r["reason_code"] for r in quarantine_of(cfg, "r1")] == ["INVALID_REFERENCE"]
    assert db_of(cfg).q("PRAGMA foreign_key_check") == []


# ------------------------------------------------------------------ failure handling
def test_partial_failure_then_retry_succeeds(make_input, cfg_factory, monkeypatch, db_of):
    real, state = pl.ingest_file, {"n": 0}

    def flaky(path, expected, entity, run_id, max_bytes):
        if entity == "orders":
            state["n"] += 1
            if state["n"] == 1:
                raise TransientError("simulated I/O blip")
        return real(path, expected, entity, run_id, max_bytes)

    monkeypatch.setattr(pl, "ingest_file", flaky)
    sleeps = []
    res = run(cfg_factory(make_input()), sleep=sleeps.append)
    assert res.status == "SUCCESS" and res.tasks["ingest_orders"].attempts == 2 and sleeps == [0.01]
    assert res.tasks["ingest_customers"].attempts == 1


def test_retries_are_bounded_then_run_fails_and_keeps_old_output(make_input, cfg_factory, monkeypatch, db_of):
    cfg = cfg_factory(make_input())
    run(cfg, "good")
    before = db_of(cfg).snapshot()
    monkeypatch.setattr(pl, "ingest_file", lambda *a, **k: (_ for _ in ()).throw(TransientError("down")))
    res = run(cfg, "bad")
    assert res.status == "FAILED" and res.tasks["ingest_customers"].attempts == 3
    assert db_of(cfg).snapshot() == before


def test_task_timeout_stops_run_and_blocks_publication(make_input, cfg_factory, monkeypatch, db_of):
    cfg = cfg_factory(make_input())
    run(cfg, "good")
    before = db_of(cfg).snapshot()
    monkeypatch.setattr(pl, "transform", lambda *a, **k: time.sleep(1.0))
    res = run(cfg_factory(make_input(), task_timeouts={"transform": 0.1}, max_attempts=2), "slow")
    assert res.status == "FAILED" and res.tasks["transform"].status == "TIMED_OUT" and res.tasks["transform"].attempts == 2
    assert task_status(res)["quality_gate"] == "SKIPPED" and task_status(res)["publish"] == "SKIPPED"
    assert db_of(cfg).snapshot() == before


def test_critical_upstream_failure_prevents_publish_call(make_input, cfg_factory, monkeypatch, db_of):
    called = []
    monkeypatch.setattr(pl, "publish", lambda *a, **k: called.append(1))
    bad_orders = [["O0000000" + str(i), "C000777", "2025-02-01T10:00:00Z", "2025-02-01T10:00:00Z", "PLACED", "USD", "1", "5.00", "0"] for i in range(1, 6)]
    res = run(cfg_factory(make_input(orders=bad_orders)))  # 5 of 8 rows rejected -> reject-ratio gate fails
    assert res.status == "FAILED" and res.failed_task == "quality_gate" and res.error_type == "QualityGateError"
    assert called == [] and task_status(res)["publish"] == "SKIPPED"


def test_failed_publication_preserves_previous_good_output(make_input, cfg_factory, monkeypatch, db_of):
    cfg = cfg_factory(make_input())
    run(cfg, "good")
    before = db_of(cfg).snapshot()
    changed = [list(r) for r in GOOD_ORDERS]; changed[0][3] = "2025-03-01T10:00:00Z"; changed[0][4] = "DELIVERED"
    monkeypatch.setattr(pub, "_swap", lambda s, t: (_ for _ in ()).throw(OSError("disk full")))
    res = run(cfg_factory(make_input(orders=changed, name="in2")), "boom")
    assert res.status == "FAILED" and res.failed_task == "publish" and res.error_type == "PublishError"
    assert db_of(cfg).snapshot() == before
    assert list(cfg.output_dir.glob("warehouse.sqlite.staging-*")) == []  # no leftovers
    assert json.loads((cfg.output_dir / "last_failure.json").read_text())["run_id"] == "boom"


def test_failed_post_publication_verification_restores_previous_output(make_input, cfg_factory, monkeypatch, db_of):
    cfg = cfg_factory(make_input())
    run(cfg, "good")
    before = db_of(cfg).snapshot()
    changed = [list(r) for r in GOOD_ORDERS]; changed[0][4] = "DELIVERED"; changed[0][3] = "2025-03-01T10:00:00Z"
    monkeypatch.setattr(pub, "_verify_published", lambda *a: (_ for _ in ()).throw(RuntimeError("corrupt")))
    res = run(cfg_factory(make_input(orders=changed, name="in2")), "boom")
    assert res.status == "FAILED" and res.failed_task == "publish"
    assert db_of(cfg).snapshot() == before


def test_timed_out_publish_attempt_cannot_swap_late(make_input, cfg_factory, monkeypatch, db_of):
    """An abandoned (timed-out) publish attempt sees `cancel` set and must not replace the target."""
    cfg = cfg_factory(make_input())
    run(cfg, "good")
    before = db_of(cfg).snapshot()
    real = pub._build_staging

    def slow_build(*a, **k):
        time.sleep(0.4)
        return real(*a, **k)

    monkeypatch.setattr(pub, "_build_staging", slow_build)
    changed = [list(r) for r in GOOD_ORDERS]; changed[0][4] = "DELIVERED"; changed[0][3] = "2025-03-01T10:00:00Z"
    res = run(cfg_factory(make_input(orders=changed, name="in2"), task_timeouts={"publish": 0.1}, max_attempts=1), "slowpub")
    assert res.tasks["publish"].status == "TIMED_OUT"
    time.sleep(0.8)  # let the abandoned thread finish
    assert db_of(cfg).snapshot() == before


def test_non_critical_report_failure_gives_warning_but_publishes(make_input, cfg_factory, monkeypatch, db_of):
    monkeypatch.setattr(pl, "run_example_queries", lambda *a: (_ for _ in ()).throw(RuntimeError("x")))
    cfg = cfg_factory(make_input())
    res = run(cfg)
    assert res.status == "SUCCESS_WITH_WARNINGS" and res.exit_code == 0 and res.tasks["report"].status == "FAILED"
    assert db_of(cfg).q("SELECT COUNT(*) FROM fact_orders") == [(3,)]


# ------------------------------------------------------------------ idempotency, late data, backfill
def _content(db):
    return (db.q("SELECT * FROM dim_customer ORDER BY 1"), db.q("SELECT order_id, status, net_cents FROM fact_orders ORDER BY 1"),
            db.q("SELECT * FROM agg_daily_revenue ORDER BY 1, 2"), db.q("SELECT content_sha256 FROM publication"))


def test_rerun_of_same_input_is_idempotent(cfg_factory, db_of):
    cfg = cfg_factory(SAMPLE_INPUT)
    run(cfg, "a")
    first = _content(db_of(cfg))
    res = run(cfg, "b")
    assert res.status == "SUCCESS"
    assert _content(db_of(cfg)) == first  # no duplicates, same hash
    assert db_of(cfg).q("SELECT run_id FROM publication") == [("b",)]  # only provenance changes
    assert db_of(cfg).q("SELECT COUNT(*), COUNT(DISTINCT order_id) FROM fact_orders") == [(32, 32)]


def test_failed_run_can_be_diagnosed_and_safely_rerun(make_input, cfg_factory, db_of):
    d = make_input()
    good_header = (d / "customers.csv").read_text()
    (d / "customers.csv").write_text(good_header.replace("email,", "mail,", 1))  # drift
    cfg = cfg_factory(d)
    assert run(cfg, "fail1").status == "FAILED"
    tracker = RunTracker(cfg.output_dir / "run_history.sqlite")
    assert tracker.get_run("fail1")["error_type"] == "BatchSchemaError"
    (d / "customers.csv").write_text(good_header)  # fix input, rerun
    assert run(cfg, "fix1").status == "SUCCESS"


def test_out_of_order_rows_give_identical_output(tmp_path, cfg_factory, db_of):
    lines = (SAMPLE_INPUT / "orders.csv").read_text().splitlines()
    shuffled = tmp_path / "shuffled"
    shuffled.mkdir()
    (shuffled / "orders.csv").write_text("\n".join([lines[0]] + lines[:0:-1]) + "\n")  # data rows reversed
    shutil.copy(SAMPLE_INPUT / "customers.csv", shuffled / "customers.csv")
    cfg_a, cfg_b = cfg_factory(SAMPLE_INPUT), cfg_factory(shuffled, output_dir=tmp_path / "out_b")
    assert run(cfg_a, "a").status == "SUCCESS" and run(cfg_b, "b").status == "SUCCESS"
    assert _content(db_of(cfg_a))[3] == _content(db_of(cfg_b))[3]
    assert db_of(cfg_b).q("SELECT status FROM fact_orders WHERE order_id='O00000013'") == [("DELIVERED",)]


def test_older_snapshot_is_refused_unless_backfill_is_allowed(make_input, cfg_factory, db_of):
    newer = [list(r) for r in GOOD_ORDERS]; newer[0][3] = "2025-06-01T00:00:00Z"
    cfg = cfg_factory(make_input(orders=newer, name="newer"))
    run(cfg, "new")
    before = db_of(cfg).snapshot()
    old_cfg = cfg_factory(make_input(name="older"))
    res = run(old_cfg, "old")
    assert res.status == "FAILED" and res.error_type == "StaleSnapshotError" and db_of(cfg).snapshot() == before
    res = run(cfg_factory(make_input(name="older"), allow_older_snapshot=True), "backfill")
    assert res.status == "SUCCESS"


# ------------------------------------------------------------------ reconciliation on generated data
def test_generated_dataset_reconciles_with_independent_oracle(tmp_path, cfg_factory, db_of, quarantine_of):
    manifest = generate(tmp_path / "gen", customers=300, orders=1500, seed=7, invalid_rate=0.05)
    cfg = cfg_factory(tmp_path / "gen" / "input")
    res = run(cfg)
    assert res.status == "SUCCESS", res.error_message
    m = res.metrics
    assert m["entities"]["customers"]["accepted"] == manifest["expected_customers_accepted"]
    assert m["entities"]["orders"]["accepted"] == manifest["expected_orders_accepted"]
    assert m["input_rows"] == m["accepted_rows"] + m["rejected_rows"]
    got = {}
    for r in quarantine_of(cfg, "r1"):
        got.setdefault(r["source_file"], {})[str(r["row_number"])] = r["reason_code"]
    assert got == manifest["expected_rejects"]  # every injected fault rejected with the expected code
    db = db_of(cfg)
    assert db.q("SELECT SUM(net_cents), SUM(recognized_revenue_cents) FROM fact_orders") == [
        (manifest["expected_net_cents_total"], manifest["expected_recognized_cents_total"])]
    assert db.q("SELECT SUM(orders), SUM(net_cents) FROM agg_daily_revenue") == db.q("SELECT COUNT(*), SUM(net_cents) FROM fact_orders")
    assert db.q("SELECT COUNT(*) FROM fact_orders o LEFT JOIN dim_customer c USING(customer_id) WHERE c.customer_id IS NULL") == [(0,)]


# ------------------------------------------------------------------ run tracking & concurrency
def test_run_history_records_status_counts_and_utc_timestamps(make_input, cfg_factory):
    cfg = cfg_factory(make_input())
    run(cfg)
    t = RunTracker(cfg.output_dir / "run_history.sqlite")
    r = t.get_run("r1")
    assert r["status"] == "SUCCESS" and (r["input_rows"], r["accepted_rows"], r["output_rows"], r["rejected_rows"]) == (6, 6, 6, 0)
    assert r["started_at_utc"].endswith("Z") and r["finished_at_utc"].endswith("Z") and r["elapsed_s"] > 0
    assert {x["task"]: x["status"] for x in t.get_tasks("r1")} == {n: "SUCCESS" for n in pl.TASK_NAMES}


def test_failure_details_are_recorded(make_input, cfg_factory):
    cfg = cfg_factory(make_input(customers=[], orders=[]))
    run(cfg)
    r = RunTracker(cfg.output_dir / "run_history.sqlite").get_run("r1")
    assert r["status"] == "FAILED" and r["failed_task"] == "quality_gate" and r["error_type"] == "EmptyInputError"


def test_concurrent_run_is_refused_and_stale_lock_is_recovered(make_input, cfg_factory):
    import os
    cfg = cfg_factory(make_input())
    cfg.output_dir.mkdir(parents=True)
    lock = cfg.output_dir / ".run.lock"
    lock.write_text(str(os.getpid()))  # a live process (us) holds the lock
    with pytest.raises(LockHeldError):
        run(cfg)
    lock.write_text("999999999")  # dead PID -> stale lock
    assert run(cfg).status == "SUCCESS" and not lock.exists()


def test_abandoned_running_rows_are_marked(make_input, cfg_factory):
    cfg = cfg_factory(make_input())
    run(cfg, "first")
    t = RunTracker(cfg.output_dir / "run_history.sqlite")
    t.start_run("crashed", "x", ["a"])  # simulates a process that died mid-run
    run(cfg, "second")
    assert t.get_run("crashed")["status"] == "ABANDONED"


def test_invalid_run_id_is_refused(make_input, cfg_factory):
    with pytest.raises(ConfigError):
        run(cfg_factory(make_input()), run_id="../evil")
