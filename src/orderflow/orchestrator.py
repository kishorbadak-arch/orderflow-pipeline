"""A tiny DAG runner: explicit dependencies, bounded retries with backoff, per-task timeouts.

Design notes
  * A task runs only if ALL its dependencies succeeded; otherwise it is SKIPPED (never run).
  * Only retryable errors (TransientError, TaskTimeout, ConnectionError, TimeoutError) are retried.
    Permanent errors (bad data, failed quality gate, bugs) fail immediately.
  * Timeouts are enforced by running each attempt in a daemon thread. Python cannot kill a thread,
    so an abandoned attempt is told to stop via its own `cancel` Event and its result is discarded.
    Tasks that have side effects (publish) must check `cancel` before the irreversible step.
  * Tasks are executed one at a time in a deterministic topological order.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from .errors import TaskTimeout
from .logging_utils import log, safe_error
from .timeutil import utc_now_iso

SUCCESS, FAILED, TIMED_OUT, SKIPPED = "SUCCESS", "FAILED", "TIMED_OUT", "SKIPPED"


@dataclass
class Task:
    name: str
    fn: Callable[[Any, threading.Event], Any]
    depends_on: tuple[str, ...] = ()
    max_attempts: int = 1
    timeout_s: float = 60.0
    critical: bool = True
    backoff_base_s: float = 1.0
    backoff_max_s: float = 30.0


@dataclass
class TaskResult:
    name: str
    status: str
    attempts: int = 0
    error_type: str | None = None
    error_message: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    critical: bool = True


@dataclass
class RunContext:
    run_id: str
    config: Any
    artifacts: dict = field(default_factory=dict)


def is_retryable(exc: BaseException) -> bool:
    return bool(getattr(exc, "retryable", False)) or isinstance(exc, (ConnectionError, TimeoutError))


def toposort(tasks: list[Task]) -> list[Task]:
    by_name = {t.name: t for t in tasks}
    if len(by_name) != len(tasks):
        raise ValueError("duplicate task names")
    for t in tasks:
        for d in t.depends_on:
            if d not in by_name:
                raise ValueError(f"task {t.name!r} depends on unknown task {d!r}")
    done: list[Task] = []
    placed: set[str] = set()
    remaining = list(tasks)
    while remaining:
        ready = [t for t in remaining if all(d in placed for d in t.depends_on)]
        if not ready:
            raise ValueError("dependency cycle detected")
        t = ready[0]  # deterministic: first declared ready task
        done.append(t)
        placed.add(t.name)
        remaining.remove(t)
    return done


def _attempt(task: Task, ctx: RunContext):
    box: dict = {}
    cancel = threading.Event()

    def target():
        try:
            box["result"] = task.fn(ctx, cancel)
        except BaseException as exc:  # noqa: BLE001 - reported to the caller
            box["error"] = exc

    th = threading.Thread(target=target, name=f"task-{task.name}", daemon=True)
    th.start()
    th.join(task.timeout_s)
    if th.is_alive():
        cancel.set()
        raise TaskTimeout(f"task {task.name} exceeded its {task.timeout_s}s timeout")
    if "error" in box:
        raise box["error"]
    return box.get("result")


class Orchestrator:
    def __init__(self, tasks: list[Task], listener=None, sleep: Callable[[float], None] = time.sleep):
        self.tasks = toposort(tasks)
        self.listener = listener or (lambda result: None)
        self.sleep = sleep

    def run(self, ctx: RunContext) -> tuple[str, dict[str, TaskResult]]:
        results: dict[str, TaskResult] = {}
        for t in self.tasks:
            res = TaskResult(t.name, "PENDING", critical=t.critical)
            results[t.name] = res
            blockers = [d for d in t.depends_on if results[d].status != SUCCESS]
            if blockers:
                res.status = SKIPPED
                res.error_message = f"upstream not successful: {', '.join(blockers)}"
                log(logging.WARNING, "task_skipped", run_id=ctx.run_id, task=t.name, blockers=blockers)
                self.listener(res)
                continue
            self._run_task(t, ctx, res)
            self.listener(res)
        failed_critical = any(r.critical and r.status != SUCCESS for r in results.values())
        failed_any = any(r.status != SUCCESS for r in results.values())
        status = "FAILED" if failed_critical else ("SUCCESS_WITH_WARNINGS" if failed_any else "SUCCESS")
        return status, results

    def _run_task(self, t: Task, ctx: RunContext, res: TaskResult) -> None:
        res.started_at = utc_now_iso()
        res.status = "RUNNING"
        self.listener(res)
        for attempt in range(1, t.max_attempts + 1):
            res.attempts = attempt
            try:
                ctx.artifacts[t.name] = _attempt(t, ctx)
                res.status, res.error_type, res.error_message = SUCCESS, None, None
                break
            except Exception as exc:  # noqa: BLE001
                etype, emsg = safe_error(exc)
                retry = is_retryable(exc) and attempt < t.max_attempts
                log(logging.WARNING if retry else logging.ERROR, "task_attempt_failed",
                    run_id=ctx.run_id, task=t.name, attempt=attempt, error_type=etype,
                    error=emsg, will_retry=retry)  # fmt: skip
                res.error_type, res.error_message = etype, emsg
                if retry:
                    self.sleep(min(t.backoff_base_s * 2 ** (attempt - 1), t.backoff_max_s))
                    continue
                res.status = TIMED_OUT if isinstance(exc, TaskTimeout) else FAILED
                break
        res.finished_at = utc_now_iso()
        log(logging.INFO, "task_finished", run_id=ctx.run_id, task=t.name, status=res.status, attempts=res.attempts)
