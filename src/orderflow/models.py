"""Typed records that flow between tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

# A rejection reason is (code, field-or-None).
Reason = tuple[str, "str | None"]


@dataclass(frozen=True)
class RawRecord:
    source_file: str
    row_number: int  # physical line number in the source file (header is line 1)
    values: tuple[str, ...]
    malformed: bool  # True when the field count differs from the header


@dataclass(frozen=True)
class IngestResult:
    entity: str
    run_id: str
    source_file: str
    sha256: str
    size_bytes: int
    header: tuple[str, ...]
    records: list[RawRecord]


@dataclass(frozen=True)
class Customer:
    customer_id: str
    email: str
    country: str
    segment: str | None
    signup_ts: datetime  # UTC
    updated_at: datetime  # UTC
    source_file: str
    row_number: int


@dataclass(frozen=True)
class Order:
    order_id: str
    customer_id: str
    order_ts: datetime  # UTC
    updated_at: datetime  # UTC
    status: str
    currency: str
    quantity: int
    unit_price: Decimal
    discount: Decimal
    source_file: str
    row_number: int


@dataclass
class Reject:
    entity: str
    stage: str  # schema | duplicate | business
    source_file: str
    row_number: int
    reasons: list[Reason]
    record_key: str | None
    original: dict | None  # original values keyed by column (None if not safe/possible)
    original_sha256: str


@dataclass
class StageCounts:
    input_rows: int = 0
    schema_rejected: int = 0
    duplicate_rejected: int = 0
    business_rejected: int = 0
    accepted: int = 0

    @property
    def rejected(self) -> int:
        return self.schema_rejected + self.duplicate_rejected + self.business_rejected


@dataclass
class ValidationResult:
    customers: list[Customer]
    orders: list[Order]
    rejects: list[Reject] = field(default_factory=list)
    counts: dict = field(default_factory=dict)  # entity -> StageCounts
