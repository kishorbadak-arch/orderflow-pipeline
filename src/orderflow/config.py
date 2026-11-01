"""Run configuration. Everything is explicit; env vars are only read in `from_env`."""

from __future__ import annotations

import os
from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import Path

from .errors import ConfigError
from .timeutil import parse_ts


@dataclass(frozen=True)
class Config:
    input_dir: Path
    output_dir: Path
    customers_file: str = "customers.csv"
    orders_file: str = "orders.csv"
    as_of: datetime | None = None  # reference "now" for future-timestamp rules; None = run start
    max_reject_ratio: float = 0.30  # quality gate: rejected / input rows
    max_file_bytes: int = 100 * 1024 * 1024
    allow_older_snapshot: bool = False  # backfill switch, see docs/DESIGN.md
    quarantine_store_raw: bool = True  # False -> only a SHA-256 of each rejected row is kept
    fast_staging: bool = True  # the documented optimization (see docs/PERFORMANCE.md)
    max_attempts: int = 3  # per task, including the first attempt
    backoff_base_s: float = 1.0
    backoff_max_s: float = 30.0
    default_timeout_s: float = 300.0
    task_timeouts: dict = field(default_factory=dict)  # task name -> seconds
    webhook_url: str | None = None  # optional; must be https

    def __post_init__(self):
        if not 0.0 <= self.max_reject_ratio <= 1.0:
            raise ConfigError("max_reject_ratio must be between 0 and 1")
        if self.max_attempts < 1:
            raise ConfigError("max_attempts must be >= 1")
        if self.default_timeout_s <= 0 or self.backoff_base_s < 0 or self.backoff_max_s < 0:
            raise ConfigError("timeouts must be > 0 and backoff values >= 0")
        if self.webhook_url and not self.webhook_url.startswith("https://"):
            raise ConfigError("webhook URL must use https")
        if self.as_of is not None and self.as_of.tzinfo is None:
            raise ConfigError("as_of must be timezone-aware")

    def timeout_for(self, task: str) -> float:
        return float(self.task_timeouts.get(task, self.default_timeout_s))

    def with_(self, **changes) -> "Config":
        return replace(self, **changes)

    @classmethod
    def from_env(cls, input_dir: str | Path, output_dir: str | Path, **overrides) -> "Config":
        env = os.environ
        values: dict = {}
        try:
            if env.get("ORDERFLOW_MAX_REJECT_RATIO"):
                values["max_reject_ratio"] = float(env["ORDERFLOW_MAX_REJECT_RATIO"])
            if env.get("ORDERFLOW_MAX_ATTEMPTS"):
                values["max_attempts"] = int(env["ORDERFLOW_MAX_ATTEMPTS"])
            if env.get("ORDERFLOW_TASK_TIMEOUT_S"):
                values["default_timeout_s"] = float(env["ORDERFLOW_TASK_TIMEOUT_S"])
            if env.get("ORDERFLOW_BACKOFF_BASE_S"):
                values["backoff_base_s"] = float(env["ORDERFLOW_BACKOFF_BASE_S"])
            if env.get("ORDERFLOW_AS_OF"):
                as_of = parse_ts(env["ORDERFLOW_AS_OF"])
                if as_of is None:
                    raise ConfigError("ORDERFLOW_AS_OF must be ISO-8601 with timezone")
                values["as_of"] = as_of
        except ValueError as exc:
            raise ConfigError("invalid numeric value in ORDERFLOW_* environment variable") from exc
        if env.get("ORDERFLOW_WEBHOOK_URL"):
            values["webhook_url"] = env["ORDERFLOW_WEBHOOK_URL"]
        if env.get("ORDERFLOW_QUARANTINE_STORE_RAW") == "0":
            values["quarantine_store_raw"] = False
        values.update({k: v for k, v in overrides.items() if v is not None})
        return cls(input_dir=Path(input_dir), output_dir=Path(output_dir), **values)
