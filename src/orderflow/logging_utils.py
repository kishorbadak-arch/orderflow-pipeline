"""Structured (JSON) logging. Log calls must only pass metadata, never row contents."""

from __future__ import annotations

import json
import logging
import os
import sys

from .errors import PipelineError
from .timeutil import utc_now_iso

logger = logging.getLogger("orderflow")
logger.addHandler(logging.NullHandler())


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {"ts": utc_now_iso(), "level": record.levelname, "event": record.getMessage()}
        payload.update(getattr(record, "fields", {}))
        return json.dumps(payload, sort_keys=True, default=str)


def configure_logging(level: int = logging.INFO) -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter())
    logger.handlers = [handler]
    logger.setLevel(level)
    logger.propagate = False


def log(level: int, event: str, **fields) -> None:
    logger.log(level, event, extra={"fields": fields})


def safe_error(exc: BaseException) -> tuple[str, str]:
    """(error_type, message) that is safe to store/log.

    Our own exceptions are written to carry no row data. Anything else (a bug, a library error)
    could echo input values, so only its type is kept unless ORDERFLOW_DEBUG=1.
    """
    if isinstance(exc, PipelineError):
        return type(exc).__name__, str(exc)[:300]
    if os.environ.get("ORDERFLOW_DEBUG") == "1":
        return type(exc).__name__, str(exc)[:300]
    return type(exc).__name__, "unexpected internal error (set ORDERFLOW_DEBUG=1 locally to see detail)"
