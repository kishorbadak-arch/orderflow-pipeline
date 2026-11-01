"""Small filesystem helpers: private permissions and atomic writes."""

from __future__ import annotations

import os
from pathlib import Path


def ensure_private_dir(path: Path) -> Path:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    return path


def atomic_write_text(path: Path, text: str, mode: int = 0o600) -> None:
    """Write to a temp file in the same directory, fsync, then rename over `path`."""
    tmp = path.with_name(path.name + f".tmp-{os.getpid()}")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()


def fsync_path(path: Path) -> None:
    """fsync a regular file. Opened read/write because Windows refuses to flush a read-only handle."""
    fd = os.open(path, os.O_RDWR)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:  # directories cannot be opened/flushed this way on Windows
        pass


def ro_uri(path: Path) -> str:
    """SQLite read-only URI that is valid for Windows paths too (file:///C:/...)."""
    return Path(path).resolve().as_uri() + "?mode=ro"
