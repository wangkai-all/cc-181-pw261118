"""Identifier helpers.  IDs are human-readable, opaque and never reused."""

from __future__ import annotations

import secrets


def token(nbytes: int = 8) -> str:
    return secrets.token_hex(nbytes)


def snapshot_id(seq: int) -> str:
    return f"v{seq:06d}"


def data_file_uid() -> str:
    return f"f_{token(8)}"


def delete_set_uid_from(content_sha: str) -> str:
    return f"d_{content_sha[:20]}"


def op_id() -> str:
    return f"op_{token(6)}"


def job_id() -> str:
    return f"j_{token(5)}"
