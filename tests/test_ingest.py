"""Ingestion: batch-level failures raise, row-level problems are flagged (not dropped)."""

import os

import pytest

from conftest import write_csv
from orderflow.errors import (
    BatchSchemaError, MalformedFileError, MissingSourceError, UnsupportedFileTypeError,
)  # fmt: skip
from orderflow.ingest import CUSTOMER_COLUMNS, ingest_file, resolve_source

HEADER = ",".join(CUSTOMER_COLUMNS)
ROW = "C000001,a@example.com,US,SMB,2025-01-01T00:00:00Z,2025-01-01T00:00:00Z"


def ing(path):
    return ingest_file(path, CUSTOMER_COLUMNS, "customers", "run1", 10_000_000)


def test_reads_rows_with_line_numbers_and_hash(tmp_path):
    p = tmp_path / "customers.csv"
    p.write_text(f"{HEADER}\n{ROW}\n\n{ROW.replace('C000001', 'C000002')}\n")
    res = ing(p)
    assert [r.row_number for r in res.records] == [2, 4]  # blank line skipped, numbering stays physical
    assert res.run_id == "run1" and res.source_file == "customers.csv" and len(res.sha256) == 64


def test_header_only_file_has_zero_records(tmp_path):
    p = tmp_path / "customers.csv"
    p.write_text(HEADER + "\n")
    assert ing(p).records == []


def test_wrong_field_count_is_flagged_not_dropped(tmp_path):
    p = tmp_path / "customers.csv"
    p.write_text(f"{HEADER}\nC000001,only-two\n")
    (rec,) = ing(p).records
    assert rec.malformed and rec.row_number == 2


def test_bom_is_tolerated(tmp_path):
    p = tmp_path / "customers.csv"
    p.write_bytes(b"\xef\xbb\xbf" + f"{HEADER}\n{ROW}\n".encode())
    assert len(ing(p).records) == 1


@pytest.mark.parametrize("name", ["customers.txt", "customers.json", "customers"])
def test_unsupported_file_types_rejected(tmp_path, name):
    p = tmp_path / name
    p.write_text(f"{HEADER}\n{ROW}\n")
    with pytest.raises(UnsupportedFileTypeError):
        ing(p)


def test_missing_file(tmp_path):
    with pytest.raises(MissingSourceError):
        ing(tmp_path / "customers.csv")


def test_zero_byte_file_has_no_header(tmp_path):
    p = tmp_path / "customers.csv"
    p.write_bytes(b"")
    with pytest.raises(BatchSchemaError):
        ing(p)


def test_unexpected_and_missing_columns_are_batch_errors(tmp_path):
    p = tmp_path / "customers.csv"
    p.write_text(HEADER + ",loyalty_points\n")
    with pytest.raises(BatchSchemaError, match="unexpected"):
        ing(p)
    p.write_text(HEADER.replace(",email", "") + "\n")
    with pytest.raises(BatchSchemaError, match="missing"):
        ing(p)


def test_reordered_columns_are_accepted(tmp_path):
    cols = list(reversed(CUSTOMER_COLUMNS))
    write_csv(tmp_path / "customers.csv", cols, [list(reversed(ROW.split(",")))])
    assert len(ing(tmp_path / "customers.csv").records) == 1


def test_duplicate_header_names(tmp_path):
    p = tmp_path / "customers.csv"
    p.write_text(HEADER + ",email\n")
    with pytest.raises(BatchSchemaError):
        ing(p)


def test_invalid_utf8_and_nul_and_bad_quotes_are_malformed_files(tmp_path):
    p = tmp_path / "customers.csv"
    p.write_bytes(HEADER.encode() + b"\nC000001,\xff\xfe,US,SMB,x,y\n")
    with pytest.raises(MalformedFileError):
        ing(p)
    p.write_bytes(HEADER.encode() + b"\nC000001,a\x00b,US,SMB,x,y\n")
    with pytest.raises(MalformedFileError):
        ing(p)
    p.write_text(HEADER + '\nC000001,"unterminated,US,SMB,x,y\n')
    with pytest.raises(MalformedFileError):
        ing(p)


def test_size_limit(tmp_path):
    p = tmp_path / "customers.csv"
    p.write_text(f"{HEADER}\n{ROW}\n")
    with pytest.raises(Exception, match="limit"):
        ingest_file(p, CUSTOMER_COLUMNS, "customers", "r", 10)


def test_path_traversal_and_symlink_escape_refused(tmp_path):
    base = tmp_path / "in"
    base.mkdir()
    (tmp_path / "secret.csv").write_text("x")
    for bad in ["../secret.csv", "/etc/passwd", "sub/x.csv", ""]:
        with pytest.raises(UnsupportedFileTypeError):
            resolve_source(base, bad)
    link = base / "link.csv"
    try:
        os.symlink(tmp_path / "secret.csv", link)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks unavailable")
    with pytest.raises(MissingSourceError):
        resolve_source(base, "link.csv")
