"""Filesystem helpers: atomic writes, directory fsync, checksums, file locks.

Crash-safety contract used throughout the project:

* any JSON metadata file is written to a unique temporary name inside the
  target directory, fsynced, then ``os.replace``-d into place;
* the parent directory is fsynced afterwards so the rename survives power loss;
* data files are written fully to staging, then renamed into ``data/`` while
  the exclusive store lock is held.
"""

from __future__ import annotations

import errno
import fcntl
import json
import os
import secrets
import shutil
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def canonical_json(obj: Any) -> str:
    """Deterministic JSON used for checksums and content addressing."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def sha256_text(text: str) -> str:
    import hashlib

    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_bytes(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()


def sha256_file(path: os.PathLike | str, chunk: int = 1 << 20) -> str:
    import hashlib

    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            block = fh.read(chunk)
            if not block:
                break
            h.update(block)
    return h.hexdigest()


def fsync_dir(path: os.PathLike | str) -> None:
    fd = os.open(os.fspath(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _tmp_name(directory: Path, stem: str) -> Path:
    return directory / f".{stem}.{secrets.token_hex(6)}.tmp"


def write_bytes_atomic(path: os.PathLike | str, data: bytes) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_name(path.parent, path.name)
    try:
        with open(tmp, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
        fsync_dir(path.parent)
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)


def write_json_atomic(path: os.PathLike | str, obj: Any) -> None:
    write_bytes_atomic(path, canonical_json(obj).encode("utf-8"))


def read_json(path: os.PathLike | str) -> Any:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def fsync_file(path: os.PathLike | str) -> None:
    fd = os.open(os.fspath(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def publish_file(src: os.PathLike | str, dst: os.PathLike | str) -> None:
    """Atomically move a fully written file into its final immutable name."""
    src, dst = Path(src), Path(dst)
    dst.parent.mkdir(parents=True, exist_ok=True)
    fsync_file(src)
    os.replace(src, dst)
    fsync_dir(dst.parent)


class FileLockTimeout(TimeoutError):
    pass


class FileLock:
    """A small POSIX ``flock`` wrapper with shared/exclusive modes.

    Different ``open`` calls create different file descriptions, so this also
    serialises threads inside one process.
    """

    def __init__(self, path: os.PathLike | str, timeout: float = 15.0, poll: float = 0.02):
        self.path = Path(path)
        self.timeout = timeout
        self.poll = poll

    @contextmanager
    def held(self, shared: bool = False, timeout: float | None = None) -> Iterator[None]:
        timeout = self.timeout if timeout is None else timeout
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o644)
        mode = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
        deadline = time.monotonic() + timeout
        try:
            while True:
                try:
                    fcntl.flock(fd, mode | fcntl.LOCK_NB)
                    break
                except OSError as exc:
                    if exc.errno not in (errno.EAGAIN, errno.EACCES):
                        raise
                    if time.monotonic() >= deadline:
                        raise FileLockTimeout(
                            f"timed out waiting for {'shared' if shared else 'exclusive'} lock on {self.path}"
                        ) from exc
                    time.sleep(self.poll)
            yield
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)


def prune_dir_empty(path: Path) -> None:
    try:
        path.rmdir()
    except OSError:
        pass


def remove_file(path: os.PathLike | str) -> bool:
    try:
        Path(path).unlink()
        return True
    except FileNotFoundError:
        return False


def human_bytes(n: int) -> str:
    for unit in ("B", "KiB", "MiB", "GiB"):
        if n < 1024 or unit == "GiB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024.0
    return f"{n:.1f} GiB"


def copy_file_atomic(src: os.PathLike | str, dst: os.PathLike | str) -> None:
    tmp = _tmp_name(Path(dst).parent, Path(dst).name)
    shutil.copyfile(src, tmp)
    fsync_file(tmp)
    os.replace(tmp, dst)
    fsync_dir(Path(dst).parent)
