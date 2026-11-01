"""Reject (quarantine) output: one JSON object per rejected row, restrictive permissions."""

from __future__ import annotations

import json
from pathlib import Path

from .fsutil import atomic_write_text, ensure_private_dir


def write_quarantine(output_dir: Path, run_id: str, rejects) -> dict:
    qdir = ensure_private_dir(output_dir / "quarantine")
    path = qdir / f"{run_id}.jsonl"
    lines = []
    for r in rejects:
        lines.append(json.dumps({
            "run_id": run_id,
            "entity": r.entity,
            "stage": r.stage,
            "source_file": r.source_file,
            "row_number": r.row_number,
            "record_key": r.record_key,
            "reason_code": r.reasons[0][0],
            "reasons": [{"code": c, "field": f} for c, f in r.reasons],
            "original": r.original,
            "original_sha256": r.original_sha256,
        }, sort_keys=True))  # fmt: skip
    atomic_write_text(path, "".join(line + "\n" for line in lines))
    by_code: dict[str, int] = {}
    for r in rejects:
        by_code[r.reasons[0][0]] = by_code.get(r.reasons[0][0], 0) + 1
    return {"path": str(path), "rows": len(rejects), "by_primary_reason": dict(sorted(by_code.items()))}
