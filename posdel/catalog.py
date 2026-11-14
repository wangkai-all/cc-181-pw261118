"""On-disk catalog and manifest persistence.

Layout (all paths relative to the store root)::

    posdel.json                  catalog: schema + snapshot index + head pointer
    snapshots/v000001.json       immutable snapshot manifest (full file list)
    data/f_<uid>.parquet         immutable data files
    deletes/d_<sha>.json         immutable deletion-position sets
    staging/<job>/...            work-in-progress output (never queryable)
    jobs/<job>.json              merge job records
    store.lock                   advisory lock file

The **commit point** of every operation is the atomic replacement of
``posdel.json``.  A snapshot manifest written but not yet referenced by the
catalog is an orphan and is discarded by recovery; a data file written but not
yet referenced is likewise garbage collected, never read.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from . import ids
from .errors import CorruptState, NotFound
from .fsutil import (
    read_json,
    sha256_text,
    utcnow,
    write_json_atomic,
)
from .schema import Schema, parse_schema

FORMAT = "posdel/1"
CATALOG_NAME = "posdel.json"
SNAPSHOT_DIR = "snapshots"
DATA_DIR = "data"
DELETES_DIR = "deletes"
STAGING_DIR = "staging"
JOBS_DIR = "jobs"
LOCK_NAME = "store.lock"


@dataclass
class SnapshotSummary:
    id: str
    seq: int
    created_at: str
    op: str
    summary: str
    parent: str | None
    stats: dict
    expired: bool = False

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "seq": self.seq,
            "created_at": self.created_at,
            "op": self.op,
            "summary": self.summary,
            "parent": self.parent,
            "stats": self.stats,
            "expired": self.expired,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "SnapshotSummary":
        return cls(
            id=d["id"],
            seq=d["seq"],
            created_at=d["created_at"],
            op=d["op"],
            summary=d["summary"],
            parent=d.get("parent"),
            stats=d.get("stats", {}),
            expired=bool(d.get("expired", False)),
        )


@dataclass
class Catalog:
    format: str
    created_at: str
    updated_at: str
    schema: Schema
    head: str | None
    seq: int
    snapshots: list[SnapshotSummary] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "format": self.format,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "schema": {"fields": [{"name": f.name, "type": f.type} for f in self.schema.fields],
                       "hash": self.schema.hash},
            "head": self.head,
            "seq": self.seq,
            "snapshots": [s.to_dict() for s in self.snapshots],
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Catalog":
        if d.get("format") != FORMAT:
            raise CorruptState(f"目录格式不受支持: {d.get('format')!r}（需要 {FORMAT}）")
        try:
            schema = parse_schema(d["schema"]["fields"])
        except (KeyError, TypeError) as exc:
            raise CorruptState("目录缺少 schema") from exc
        if d["schema"].get("hash") and schema.hash != d["schema"]["hash"]:
            raise CorruptState("schema 哈希不匹配，目录可能被篡改")
        return cls(
            format=d["format"],
            created_at=d["created_at"],
            updated_at=d["updated_at"],
            schema=schema,
            head=d.get("head"),
            seq=int(d.get("seq", 0)),
            snapshots=[SnapshotSummary.from_dict(s) for s in d.get("snapshots", [])],
        )

    def summary(self, snapshot_id: str) -> SnapshotSummary | None:
        for s in self.snapshots:
            if s.id == snapshot_id:
                return s
        return None

    def retained(self) -> list[SnapshotSummary]:
        return [s for s in self.snapshots if not s.expired]

    def next_seq(self) -> int:
        return self.seq + 1


def catalog_path(root) -> str:
    return f"{root}/{CATALOG_NAME}"


def snapshot_relpath(sid: str) -> str:
    return f"{SNAPSHOT_DIR}/{sid}.json"


def load_catalog(root) -> Catalog:
    import os

    path = catalog_path(root)
    if not os.path.exists(path):
        raise NotFound(f"未找到湖表目录: {path}")
    try:
        return Catalog.from_dict(read_json(path))
    except CorruptState:
        raise
    except Exception as exc:  # malformed JSON etc.
        raise CorruptState(f"目录文件损坏: {exc}") from exc


def save_catalog(root, catalog: Catalog) -> None:
    catalog.updated_at = utcnow()
    write_json_atomic(catalog_path(root), catalog.to_dict())


def load_manifest(root, sid: str) -> dict:
    import os

    path = f"{root}/{snapshot_relpath(sid)}"
    if not os.path.exists(path):
        raise CorruptState(f"快照 {sid} 的清单文件缺失: {snapshot_relpath(sid)}")
    try:
        manifest = read_json(path)
    except Exception as exc:
        raise CorruptState(f"快照 {sid} 清单损坏: {exc}") from exc
    if manifest.get("format") != FORMAT:
        raise CorruptState(f"快照 {sid} 格式不匹配")
    if manifest.get("snapshot_id") != sid:
        raise CorruptState(f"快照文件内容与文件名不一致: {sid}")
    return manifest


def save_manifest(root, manifest: dict) -> str:
    sid = manifest["snapshot_id"]
    write_json_atomic(f"{root}/{snapshot_relpath(sid)}", manifest)
    return sid


def manifest_checksum(manifest: dict) -> str:
    payload = {k: v for k, v in manifest.items() if k != "checksum"}
    import json

    return sha256_text(json.dumps(payload, sort_keys=True, separators=(",", ":")))


def new_snapshot_id(seq: int) -> str:
    return ids.snapshot_id(seq)


def build_stats(files: list[dict], deletes: dict[str, dict]) -> dict:
    raw = sum(f["rows"] for f in files)
    deleted = sum(d["positions"] for d in deletes.values())
    return {
        "raw_rows": raw,
        "deleted_rows": deleted,
        "visible_rows": raw - deleted,
        "file_count": len(files),
        "delete_set_count": len(deletes),
    }
