"""Failure notification: structured log + last_failure.json, optional https webhook.

Notifications contain only run metadata (ids, task names, counts, sanitized error type/message).
They never contain row contents. The webhook URL is read from configuration and never logged.
"""

from __future__ import annotations

import json
import logging
import urllib.request
from pathlib import Path

from .fsutil import atomic_write_text
from .logging_utils import log


def notify_failure(output_dir: Path, webhook_url: str | None, summary: dict) -> None:
    log(logging.ERROR, "run_failed", **summary)
    try:
        atomic_write_text(output_dir / "last_failure.json", json.dumps(summary, indent=2, sort_keys=True) + "\n")
    except OSError:
        log(logging.WARNING, "could_not_write_last_failure")
    if not webhook_url:
        return
    try:
        req = urllib.request.Request(
            webhook_url, data=json.dumps(summary).encode(), method="POST",
            headers={"Content-Type": "application/json"},
        )  # fmt: skip
        with urllib.request.urlopen(req, timeout=5):  # noqa: S310 - https enforced in Config
            pass
        log(logging.INFO, "webhook_delivered", run_id=summary.get("run_id"))
    except Exception as exc:  # noqa: BLE001 - never let notification mask the real failure
        log(logging.WARNING, "webhook_failed", run_id=summary.get("run_id"), error_type=type(exc).__name__)
