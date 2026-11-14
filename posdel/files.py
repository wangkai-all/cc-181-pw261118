"""Immutable Parquet data files and immutable deletion-position files.

A *data file* is a plain Parquet file.  We write it once, record its sha256
and row count, and never modify it again.  A *delete file* is a small JSON
document whose name is derived from its content hash, so identical delete
sets are shared between snapshots for free.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Iterable, Sequence

import pyarrow as pa
import pyarrow.parquet as pq

from .errors import CorruptState, ValidationError
from .fsutil import (
    canonical_json,
    sha256_file,
    sha256_text,
    utcnow,
    write_json_atomic,
)
from .schema import Schema

PARQUET_VERSION = "1.0"
DELETE_FORMAT = "posdel-deletes/1"
MAX_ROWS_PER_FILE = 200_000


@dataclass(frozen=True)
class DataFile:
    id: str
    path: str          # relative to store root
    rows: int
    created_at: str
    sha256: str
    rows_per_group: int = 0

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "path": self.path,
            "rows": self.rows,
            "created_at": self.created_at,
            "sha256": self.sha256,
            "rows_per_group": self.rows_per_group,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "DataFile":
        return cls(
            id=d["id"],
            path=d["path"],
            rows=int(d["rows"]),
            created_at=d.get("created_at", ""),
            sha256=d.get("sha256", ""),
            rows_per_group=int(d.get("rows_per_group", 0)),
        )


def write_data_file(
    root: str,
    file_id: str,
    schema: Schema,
    table: pa.Table,
    *,
    created_at: str | None = None,
    row_group_size: int = 8192,
) -> DataFile:
    """Write ``table`` to a temporary path, fsync, then publish under data/."""
    from .catalog import DATA_DIR

    if table.num_rows == 0:
        raise ValidationError("拒绝写入 0 行的数据文件")
    if table.num_rows > MAX_ROWS_PER_FILE:
        raise ValidationError(f"单个数据文件最多 {MAX_ROWS_PER_FILE} 行")
    if table.schema != schema.arrow_schema():
        raise CorruptState("写入表结构与当前 schema 不一致")

    tmp_dir = f"{root}/staging"
    os.makedirs(tmp_dir, exist_ok=True)
    tmp_path = f"{tmp_dir}/.{file_id}.parquet.tmp"
    final_path = f"{root}/{DATA_DIR}/{file_id}.parquet"
    os.makedirs(f"{root}/{DATA_DIR}", exist_ok=True)
    try:
        pq.write_table(
            table,
            tmp_path,
            compression="zstd",
            version=PARQUET_VERSION,
            row_group_size=row_group_size,
            write_statistics=True,
        )
        digest = sha256_file(tmp_path)
        os.replace(tmp_path, final_path)
        from .fsutil import fsync_dir

        fsync_dir(f"{root}/{DATA_DIR}")
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)

    return DataFile(
        id=file_id,
        path=f"{DATA_DIR}/{file_id}.parquet",
        rows=table.num_rows,
        created_at=created_at or utcnow(),
        sha256=digest,
        rows_per_group=row_group_size,
    )


def read_data_file(root: str, data_file: DataFile, columns: Sequence[str] | None = None) -> pa.Table:
    path = f"{root}/{data_file.path}"
    if not os.path.exists(path):
        raise CorruptState(f"数据文件缺失: {data_file.path}")
    try:
        table = pq.read_table(path, columns=list(columns) if columns else None)
    except Exception as exc:
        raise CorruptState(f"无法读取数据文件 {data_file.path}: {exc}") from exc
    if table.num_rows != data_file.rows:
        raise CorruptState(
            f"数据文件 {data_file.path} 行数不一致：清单记录 {data_file.rows}，实际 {table.num_rows}"
        )
    return table


def verify_data_file(root: str, data_file: DataFile) -> bool:
    path = f"{root}/{data_file.path}"
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return False
    return sha256_file(path) == data_file.sha256


# ---------------------------------------------------------------------------
# Delete sets
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DeleteSet:
    id: str            # d_<content prefix>; derived from canonical content
    positions: tuple[int, ...]
    row_count: int     # rows in the target file when this set was created
    reason: str
    predicate: str
    source_snapshot: str
    created_at: str
    path: str = ""
    derived_from: tuple[str, ...] = ()   # upstream delete-set ids (empty for a user delete)

    def to_payload(self) -> dict:
        return {
            "format": DELETE_FORMAT,
            "positions": list(self.positions),
            "row_count": self.row_count,
            "reason": self.reason,
            "predicate": self.predicate,
            "source_snapshot": self.source_snapshot,
            "created_at": self.created_at,
            "derived_from": list(self.derived_from),
        }

    def as_ref(self) -> dict:
        return {
            "id": self.id,
            "path": self.path,
            "positions": len(self.positions),
            "reason": self.reason,
            "predicate": self.predicate,
            "source_snapshot": self.source_snapshot,
            "created_at": self.created_at,
            "derived_from": list(self.derived_from),
        }


def make_delete_set(
    positions: Iterable[int],
    *,
    row_count: int,
    reason: str,
    predicate: str,
    source_snapshot: str,
    created_at: str | None = None,
    derived_from: Iterable[str] = (),
) -> DeleteSet:
    pos = sorted(set(int(p) for p in positions))
    if not pos:
        raise ValidationError("删除集为空")
    if pos[0] < 0 or pos[-1] >= row_count:
        raise ValidationError(f"删除位置越界：位置 {pos[-1]} 超出文件行数 {row_count}")
    created = created_at or utcnow()
    upstream = tuple(sorted(set(derived_from)))
    payload = {
        "format": DELETE_FORMAT,
        "positions": pos,
        "row_count": row_count,
        "reason": reason,
        "predicate": predicate,
        "source_snapshot": source_snapshot,
        "created_at": created,
        "derived_from": list(upstream),
    }
    digest = sha256_text(canonical_json(payload))
    return DeleteSet(
        id=f"d_{digest[:20]}",
        positions=tuple(pos),
        row_count=row_count,
        reason=reason,
        predicate=predicate,
        source_snapshot=source_snapshot,
        created_at=created,
        derived_from=upstream,
    )


def persist_delete_set(root: str, ds: DeleteSet) -> str:
    from .catalog import DELETES_DIR

    rel = f"{DELETES_DIR}/{ds.id}.json"
    path = f"{root}/{rel}"
    if not os.path.exists(path):
        write_json_atomic(path, ds.to_payload())
    return rel


def load_delete_set(root: str, ref: dict) -> DeleteSet:
    import json

    path = f"{root}/{ref['path']}"
    if not os.path.exists(path):
        raise CorruptState(f"删除记录文件缺失: {ref['path']}")
    try:
        with open(path, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except Exception as exc:
        raise CorruptState(f"删除记录文件损坏 {ref['path']}: {exc}") from exc
    if payload.get("format") != DELETE_FORMAT:
        raise CorruptState(f"删除记录格式不匹配: {ref['path']}")
    positions = tuple(int(p) for p in payload["positions"])
    if any(a >= b for a, b in zip(positions, positions[1:])):
        raise CorruptState(f"删除位置未去重排序: {ref['path']}")
    if positions and (positions[0] < 0 or positions[-1] >= int(payload["row_count"])):
        raise CorruptState(f"删除位置越界: {ref['path']}")
    return DeleteSet(
        id=ref["id"],
        positions=positions,
        row_count=int(payload["row_count"]),
        reason=payload.get("reason", ref.get("reason", "")),
        predicate=payload.get("predicate", ref.get("predicate", "")),
        source_snapshot=payload.get("source_snapshot", ref.get("source_snapshot", "")),
        created_at=payload.get("created_at", ref.get("created_at", "")),
        path=ref["path"],
        derived_from=tuple(payload.get("derived_from", ref.get("derived_from", []) or [])),
    )


def merge_positions(*position_lists: Iterable[int]) -> list[int]:
    merged: set[int] = set()
    for plist in position_lists:
        merged.update(plist)
    return sorted(merged)
