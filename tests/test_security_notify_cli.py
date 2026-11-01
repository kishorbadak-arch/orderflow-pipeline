"""Security properties, notifications, CLI exit codes, generator determinism."""

import json
import os
import stat
import sys

import pytest

import orderflow.notify as notify
from conftest import GOOD_CUSTOMERS, GOOD_ORDERS
from orderflow.cli import main
from orderflow.config import Config
from orderflow.errors import ConfigError
from orderflow.generate import generate
from orderflow.pipeline import run_pipeline

SENTINEL = "sentinel-pii-4711@example.com"
SECRET_URL = "https://hooks.example.invalid/services/SECRET-TOKEN-123"


def dirty_customers():
    # unique sentinel e-mail on a row that is rejected (invalid country)
    return GOOD_CUSTOMERS + [["C000009", SENTINEL, "ZZ", "SMB", "2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z"]]


def all_text(cfg, caplog):
    parts = [r.getMessage() + json.dumps(getattr(r, "fields", {}), default=str) for r in caplog.records]
    for f in cfg.output_dir.rglob("*"):
        if f.is_file() and "quarantine" not in f.parts and f.suffix not in (".sqlite", ".prev"):
            parts.append(f.read_text(errors="ignore"))
    return "\n".join(parts)


def test_rejected_row_contents_are_not_logged_or_reported(make_input, cfg_factory, caplog):
    import logging
    caplog.set_level(logging.DEBUG, logger="orderflow")
    cfg = cfg_factory(make_input(dirty_customers()), max_reject_ratio=0.5)
    res = run_pipeline(cfg, run_id="r1")
    assert res.status == "SUCCESS"
    assert SENTINEL not in all_text(cfg, caplog)  # logs, reports, run notes: no row content
    # run history DB must not contain it either
    assert SENTINEL.encode() not in (cfg.output_dir / "run_history.sqlite").read_bytes()
    # ...but the designated quarantine file does hold it (traceability), with restrictive permissions
    q = cfg.output_dir / "quarantine" / "r1.jsonl"
    assert SENTINEL in q.read_text()
    if os.name == "posix":
        assert stat.S_IMODE(q.stat().st_mode) == 0o600
        assert stat.S_IMODE(q.parent.stat().st_mode) == 0o700
        assert stat.S_IMODE(cfg.output_dir.stat().st_mode) == 0o700
        assert stat.S_IMODE((cfg.output_dir / "warehouse.sqlite").stat().st_mode) == 0o600


def test_raw_quarantine_can_be_disabled_keeping_only_a_hash(make_input, cfg_factory):
    cfg = cfg_factory(make_input(dirty_customers()), max_reject_ratio=0.5, quarantine_store_raw=False)
    run_pipeline(cfg, run_id="r1")
    text = (cfg.output_dir / "quarantine" / "r1.jsonl").read_text()
    assert SENTINEL not in text
    row = json.loads(text.splitlines()[0])
    assert row["original"] is None and len(row["original_sha256"]) == 64 and row["row_number"] == 5


def test_target_does_not_store_full_email_addresses(make_input, cfg_factory, db_of):
    cfg = cfg_factory(make_input())
    run_pipeline(cfg, run_id="r1")
    assert b"alice@example.com" not in db_of(cfg).snapshot()


def test_unexpected_exception_text_never_reaches_logs_or_history(make_input, cfg_factory, monkeypatch, caplog):
    import orderflow.pipeline as pl
    monkeypatch.setattr(pl, "transform", lambda *a, **k: (_ for _ in ()).throw(ValueError(f"bad value {SENTINEL}")))
    cfg = cfg_factory(make_input())
    res = run_pipeline(cfg, run_id="r1")
    assert res.status == "FAILED" and SENTINEL not in (res.error_message or "")
    assert SENTINEL not in all_text(cfg, caplog)


def test_failure_notification_webhook_has_no_secrets_or_rows(make_input, cfg_factory, monkeypatch):
    sent = []

    class Resp:
        def __enter__(self): return self
        def __exit__(self, *a): return False

    def fake_urlopen(req, timeout):
        sent.append((req.full_url, json.loads(req.data), timeout))
        return Resp()

    monkeypatch.setattr(notify.urllib.request, "urlopen", fake_urlopen)
    cfg = cfg_factory(make_input(dirty_customers(), orders=[]), webhook_url=SECRET_URL)  # empty orders -> run fails
    res = run_pipeline(cfg, run_id="r1")
    assert res.status == "FAILED"
    (url, payload, timeout), = sent
    assert url == SECRET_URL and timeout == 5
    assert payload["run_id"] == "r1" and payload["failed_task"] == "quality_gate"
    assert SENTINEL not in json.dumps(payload) and SECRET_URL not in json.dumps(payload)


def test_webhook_failure_does_not_change_run_outcome_or_leak_url(make_input, cfg_factory, monkeypatch, caplog):
    import logging
    caplog.set_level(logging.DEBUG, logger="orderflow")
    monkeypatch.setattr(notify.urllib.request, "urlopen", lambda *a, **k: (_ for _ in ()).throw(OSError(SECRET_URL)))
    cfg = cfg_factory(make_input(orders=[]), webhook_url=SECRET_URL)
    res = run_pipeline(cfg, run_id="r1")
    assert res.status == "FAILED" and res.exit_code == 1
    assert SECRET_URL not in all_text(cfg, caplog) and any(r.getMessage() == "webhook_failed" for r in caplog.records)


def test_webhook_must_be_https(tmp_path):
    with pytest.raises(ConfigError):
        Config(input_dir=tmp_path, output_dir=tmp_path, webhook_url="http://insecure.example.invalid/x")


def test_config_validation(tmp_path):
    for bad in ({"max_reject_ratio": 1.5}, {"max_attempts": 0}, {"default_timeout_s": 0}):
        with pytest.raises(ConfigError):
            Config(input_dir=tmp_path, output_dir=tmp_path, **bad)


def test_env_configuration_is_read_without_printing_secrets(monkeypatch, tmp_path):
    monkeypatch.setenv("ORDERFLOW_WEBHOOK_URL", SECRET_URL)
    monkeypatch.setenv("ORDERFLOW_MAX_ATTEMPTS", "5")
    cfg = Config.from_env(tmp_path, tmp_path / "o")
    assert cfg.webhook_url == SECRET_URL and cfg.max_attempts == 5
    monkeypatch.setenv("ORDERFLOW_MAX_ATTEMPTS", "abc")
    with pytest.raises(ConfigError):
        Config.from_env(tmp_path, tmp_path / "o")


def test_sql_values_are_parameterized(make_input, cfg_factory, db_of):
    """A value that looks like SQL is stored as data, not executed."""
    evil = list(GOOD_CUSTOMERS[0]); evil[1] = "x'); DROP TABLE dim_customer;--@example.com"
    cfg = cfg_factory(make_input([evil] + GOOD_CUSTOMERS[1:], orders=GOOD_ORDERS[1:]), max_reject_ratio=0.5)
    res = run_pipeline(cfg)
    assert res.status == "SUCCESS" and db_of(cfg).q("SELECT COUNT(*) FROM dim_customer") == [(2,)]  # invalid e-mail rejected


# ------------------------------------------------------------------ CLI exit codes
def test_cli_exit_codes(make_input, tmp_path, capsys):
    good, out = make_input(), tmp_path / "cli_out"
    assert main(["run", "--input-dir", str(good), "--output-dir", str(out), "--run-id", "c1", "--max-reject-ratio", "0.5"]) == 0
    printed = json.loads(capsys.readouterr().out)
    assert printed["status"] == "SUCCESS" and printed["output_rows"] == 6

    empty = make_input(customers=[], orders=[], name="empty")
    assert main(["run", "--input-dir", str(empty), "--output-dir", str(out), "--run-id", "c2"]) == 1
    assert json.loads(capsys.readouterr().out)["failed_task"] == "quality_gate"
    assert main(["run", "--input-dir", str(good), "--output-dir", str(out), "--max-reject-ratio", "7"]) == 2
    capsys.readouterr()

    (out / ".run.lock").write_text(str(os.getpid()))
    assert main(["run", "--input-dir", str(good), "--output-dir", str(out)]) == 3
    (out / ".run.lock").unlink()
    capsys.readouterr()

    assert main(["status", "--output-dir", str(out), "--run-id", "c2"]) == 0
    assert json.loads(capsys.readouterr().out)["run"]["status"] == "FAILED"
    assert main(["report", "--output-dir", str(out), "--run-id", "c1"]) == 0
    assert "Validation & reconciliation report" in capsys.readouterr().out


def test_cli_requires_valid_subcommand():
    with pytest.raises(SystemExit) as e:
        main([])
    assert e.value.code == 2


# ------------------------------------------------------------------ generator
def test_generator_is_deterministic_and_configurable(tmp_path):
    a = generate(tmp_path / "a", customers=50, orders=200, seed=1, invalid_rate=0.1)
    b = generate(tmp_path / "b", customers=50, orders=200, seed=1, invalid_rate=0.1)
    c = generate(tmp_path / "c", customers=50, orders=200, seed=2, invalid_rate=0.1)
    read = lambda d, n: (tmp_path / d / "input" / n).read_bytes()  # noqa: E731
    assert read("a", "orders.csv") == read("b", "orders.csv") and read("a", "customers.csv") == read("b", "customers.csv")
    assert read("a", "orders.csv") != read("c", "orders.csv")
    assert a == b and a["orders_rows"] >= 200
    assert (tmp_path / "a" / "expected" / "manifest.json").is_file()  # oracle lives apart from input/
    with pytest.raises(ValueError):
        generate(tmp_path / "x", customers=0)


def test_bench_small_run(tmp_path):
    from orderflow.bench import run_bench
    r = run_bench(200, 1000, 3)
    assert r["status"] == "SUCCESS" and r["matches_manifest"] and r["rows_per_s"] > 0 and "publish" in r["task_seconds"]


@pytest.mark.parametrize("seed", range(40))
def test_generator_never_crashes_and_manifest_is_consistent_across_seeds(tmp_path, seed):
    """Regression: duplicate faults must only copy valid base rows (high fault rate stresses this)."""
    m = generate(tmp_path, customers=30, orders=60, seed=seed, invalid_rate=0.5)
    n_c = sum(1 for _ in open(tmp_path / "input" / "customers.csv")) - 1
    assert n_c == m["customers_rows"] and len(m["expected_rejects"]["customers.csv"]) == m["customers_rows"] - m["expected_customers_accepted"]
