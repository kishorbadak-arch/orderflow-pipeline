"""Ingestion: read a CSV into raw records. Batch-level problems raise; row-level ones are flagged."""

from __future__ import annotations

import csv
import hashlib
from pathlib import Path

from .errors import (
    BatchError,
    BatchSchemaError,
    MalformedFileError,
    MissingSourceError,
    TransientError,
    UnsupportedFileTypeError,
)
from .models import IngestResult, RawRecord

CUSTOMER_COLUMNS = ("customer_id", "email", "country", "segment", "signup_ts", "updated_at")
ORDER_COLUMNS = (
    "order_id", "customer_id", "order_ts", "updated_at",
    "status", "currency", "quantity", "unit_price", "discount",
)  # fmt: skip


def resolve_source(base_dir: Path, name: str) -> Path:
    """Resolve `name` inside `base_dir`; refuse anything that escapes it (../, absolute, symlink)."""
    if not name or Path(name).name != name:
        raise UnsupportedFileTypeError("source file name must be a plain file name")
    base = base_dir.resolve()
    candidate = (base / name).resolve()
    if candidate.parent != base:
        raise MissingSourceError("source file resolves outside the input directory")
    return candidate


def _names(cols, limit=10):
    shown = sorted(cols)[:limit]
    return [c[:50] for c in shown]


def _sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def ingest_file(
    path: Path, expected: tuple[str, ...], entity: str, run_id: str, max_bytes: int
) -> IngestResult:
    if path.suffix.lower() != ".csv":
        raise UnsupportedFileTypeError(f"{entity}: only .csv files are supported")
    try:
        if not path.is_file():
            raise MissingSourceError(f"{entity}: source file not found")
        size = path.stat().st_size
        if size > max_bytes:
            raise BatchError(f"{entity}: source file exceeds the {max_bytes} byte limit")
        digest = _sha256_of(path)
        records: list[RawRecord] = []
        with open(path, newline="", encoding="utf-8-sig") as fh:
            reader = csv.reader(fh, strict=True)
            try:
                header = tuple(next(reader))
            except StopIteration:
                raise BatchSchemaError(f"{entity}: file is empty (no header row)") from None
            if len(set(header)) != len(header):
                raise BatchSchemaError(f"{entity}: duplicate column names in header")
            missing = set(expected) - set(header)
            unexpected = set(header) - set(expected)
            if missing or unexpected:
                raise BatchSchemaError(
                    f"{entity}: header mismatch; missing={_names(missing)} unexpected={_names(unexpected)}"
                )
            for values in reader:
                if not values:  # blank line: not a record
                    continue
                if any("\x00" in v for v in values):
                    raise MalformedFileError(f"{entity}: NUL byte near line {reader.line_num}")
                records.append(
                    RawRecord(path.name, reader.line_num, tuple(values), len(values) != len(header))
                )
    except (csv.Error, UnicodeDecodeError) as exc:
        raise MalformedFileError(f"{entity}: file is not valid UTF-8 CSV ({type(exc).__name__})") from None
    except (FileNotFoundError, PermissionError, IsADirectoryError):
        raise MissingSourceError(f"{entity}: source file is not readable") from None
    except OSError:
        raise TransientError(f"{entity}: I/O error while reading source") from None
    return IngestResult(entity, run_id, path.name, digest, size, header, records)
