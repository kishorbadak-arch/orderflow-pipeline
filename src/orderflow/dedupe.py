"""Duplicate policy: for a repeated key the version with the greatest `updated_at` wins.

Ties are broken by the higher source row number. Every losing row is rejected as
DUPLICATE_SUPERSEDED, so nothing is silently dropped. File order does not matter, which makes
out-of-order / late-arriving versions deterministic.
"""

from __future__ import annotations


def resolve_duplicates(items, key_fn):
    """Return (winners in original order, losers)."""
    best: dict = {}
    for it in items:
        k = key_fn(it)
        cur = best.get(k)
        if cur is None or (it.updated_at, it.row_number) > (cur.updated_at, cur.row_number):
            best[k] = it
    winner_ids = {id(v) for v in best.values()}
    winners = [it for it in items if id(it) in winner_ids]
    losers = [it for it in items if id(it) not in winner_ids]
    return winners, losers
