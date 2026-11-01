"""Strict ISO-8601 handling. All stored timestamps are UTC with a trailing 'Z'."""

from __future__ import annotations

import re
from datetime import datetime, timezone

# Requires date, time and an explicit offset (Z or +hh:mm). Naive timestamps are rejected.
_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?(Z|[+-]\d{2}:\d{2})$")


def parse_ts(value: str) -> datetime | None:
    """Return an aware UTC datetime or None if the text is not strict ISO-8601 with timezone."""
    if not _TS_RE.match(value):
        return None
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            return None
        return dt.astimezone(timezone.utc)
    except (ValueError, OverflowError):
        return None


def format_utc(dt: datetime) -> str:
    """Fixed-width UTC text (microseconds) so that string order equals time order."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return format_utc(utc_now())
