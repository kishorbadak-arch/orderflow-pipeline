"""Orchestrator: dependencies, bounded retries with backoff, timeouts, skip-on-upstream-failure."""

import time

import pytest

from orderflow.errors import BatchError, TransientError
from orderflow.orchestrator import Orchestrator, RunContext, Task, toposort


def ctx():
    return RunContext("r", None)


def make(name, fn=None, deps=(), **kw):
    return Task(name, fn or (lambda c, e: name), tuple(deps), **kw)


def test_runs_in_dependency_order_and_passes_artifacts():
    order = []
    tasks = [make("c", lambda c, e: order.append("c"), ["b"]), make("a", lambda c, e: order.append("a")),
             make("b", lambda c, e: order.append("b"), ["a"])]  # fmt: skip
    status, res = Orchestrator(tasks).run(ctx())
    assert status == "SUCCESS" and order == ["a", "b", "c"]


def test_cycles_and_unknown_dependencies_are_rejected():
    with pytest.raises(ValueError, match="cycle"):
        toposort([make("a", deps=["b"]), make("b", deps=["a"])])
    with pytest.raises(ValueError, match="unknown"):
        toposort([make("a", deps=["zzz"])])
    with pytest.raises(ValueError, match="duplicate"):
        toposort([make("a"), make("a")])


def test_transient_errors_retry_with_exponential_backoff_then_succeed():
    calls, sleeps = [], []

    def flaky(c, e):
        calls.append(1)
        if len(calls) < 3:
            raise TransientError("blip")
        return "ok"

    t = make("t", flaky, max_attempts=3, backoff_base_s=0.1, backoff_max_s=10)
    status, res = Orchestrator([t], sleep=sleeps.append).run(ctx())
    assert status == "SUCCESS" and res["t"].attempts == 3 and sleeps == [0.1, 0.2]


def test_retries_are_bounded_and_backoff_is_capped():
    sleeps = []

    def always(c, e):
        raise TransientError("blip")

    t = make("t", always, max_attempts=4, backoff_base_s=1, backoff_max_s=2)
    status, res = Orchestrator([t], sleep=sleeps.append).run(ctx())
    assert status == "FAILED" and res["t"].attempts == 4 and sleeps == [1, 2, 2]


def test_permanent_errors_are_not_retried():
    n = []

    def bad(c, e):
        n.append(1)
        raise BatchError("bad data")

    t = make("t", bad, max_attempts=5)
    status, res = Orchestrator([t], sleep=lambda s: pytest.fail("must not sleep")).run(ctx())
    assert len(n) == 1 and res["t"].status == "FAILED" and res["t"].attempts == 1


def test_unexpected_exceptions_fail_fast_and_do_not_leak_values():
    def bug(c, e):
        raise ValueError("secret-sentinel-value")

    status, res = Orchestrator([make("t", bug, max_attempts=3)], sleep=lambda s: None).run(ctx())
    assert res["t"].attempts == 1 and "secret-sentinel-value" not in res["t"].error_message


def test_timeout_marks_task_timed_out_and_retries_within_bound():
    sleeps = []
    t = make("slow", lambda c, e: time.sleep(0.5), max_attempts=2, timeout_s=0.05)
    status, res = Orchestrator([t], sleep=sleeps.append).run(ctx())
    assert status == "FAILED" and res["slow"].status == "TIMED_OUT" and res["slow"].attempts == 2 and len(sleeps) == 1


def test_timed_out_attempt_is_told_to_cancel():
    seen = {}

    def slow(c, cancel):
        time.sleep(0.2)
        seen["cancelled"] = cancel.is_set()

    Orchestrator([make("slow", slow, timeout_s=0.05)]).run(ctx())
    time.sleep(0.3)
    assert seen["cancelled"] is True


def test_downstream_of_failure_is_skipped_never_run():
    ran = []
    tasks = [make("a", lambda c, e: (_ for _ in ()).throw(BatchError("x"))),
             make("b", lambda c, e: ran.append("b"), ["a"]), make("c", lambda c, e: ran.append("c"), ["b"]),
             make("independent", lambda c, e: ran.append("i"))]  # fmt: skip
    status, res = Orchestrator(tasks).run(ctx())
    assert ran == ["i"] and res["b"].status == "SKIPPED" and res["c"].status == "SKIPPED" and status == "FAILED"


def test_non_critical_failure_gives_warning_status():
    def bad(c, e):
        raise BatchError("x")

    tasks = [make("a"), make("report", bad, ["a"], critical=False)]
    status, res = Orchestrator(tasks).run(ctx())
    assert status == "SUCCESS_WITH_WARNINGS"
