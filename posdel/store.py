"""The store: snapshots, appends, predicate deletes, queries, expiration.

Write path summary
------------------
Every mutation follows the same protocol while holding the exclusive
``store.lock``:

1. read the catalog and the current head snapshot;
2. build the next manifest **without touching disk**;
3. write new immutable delete / data files (they are unreachable until step 5);
4. write the manifest file (fsynced);
5. atomically replace ``posdel.json`` — **this is the commit point**;
6. only then the operation is considered visible.

A crash between 3 and 5 therefore leaves only unreferenced files behind: the
previous head still serves all queries and the garbage is reclaimable.
"""

from __future__ import annotations

import os
import threading
from dataclasses import dataclass
from typing import Any, Iterable, Sequence

import numpy as np
import pyarrow as pa

from . import ids
from .catalog import (
    DATA_DIR,
    DELETES_DIR,
    FORMAT,
    JOBS_DIR,
    LOCK_NAME,
    SNAPSHOT_DIR,
    STAGING_DIR,
    Catalog,
    SnapshotSummary,
    build_stats,
    load_catalog,
    load_manifest,
    manifest_checksum,
    new_snapshot_id,
    save_catalog,
    save_manifest,
    snapshot_relpath,
)
from .errors import Conflict, CorruptState, NotFound, ValidationError
from .files import (
    DataFile,
    DeleteSet,
    load_delete_set,
    make_delete_set,
    merge_positions,
    persist_delete_set,
    read_data_file,
    write_data_file,
)
from .fsutil import FileLock, fsync_dir, utcnow, write_json_atomic
from .schema import (
    Condition,
    Schema,
    build_conditions,
    describe_predicate,
    normalize_rows,
    predicate_mask,
    rows_to_table,
)


@dataclass
class Snapshot:
    """A loaded manifest plus its schema."""

    root: str
    manifest: dict
    schema: Schema
    summary: SnapshotSummary | None = None

    def __post_init__(self) -> None:
        self._delete_cache: dict[str, list[DeleteSet]] = {}
        self._validate()

    # -- identity ---------------------------------------------------------
    @property
    def id(self) -> str:
        return self.manifest["snapshot_id"]

    @property
    def seq(self) -> int:
        return int(self.manifest["seq"])

    @property
    def created_at(self) -> str:
        return self.manifest["created_at"]

    @property
    def op(self) -> str:
        return self.manifest["op"]

    @property
    def parent(self) -> str | None:
        return self.manifest.get("parent")

    @property
    def stats(self) -> dict:
        return self.manifest.get("stats", {})

    @property
    def operation(self) -> dict:
        return self.manifest.get("operation", {})

    @property
    def summary_text(self) -> str:
        return self.manifest.get("summary", "")

    # -- structure --------------------------------------------------------
    @property
    def files(self) -> list[DataFile]:
        return [DataFile.from_dict(d) for d in self.manifest["files"]]

    def file(self, file_id: str) -> DataFile:
        for f in self.files:
            if f.id == file_id:
                return f
        raise NotFound(f"快照 {self.id} 中不存在数据文件 {file_id}")

    def delete_refs(self, file_id: str) -> list[dict]:
        return list(self.manifest.get("deletes", {}).get(file_id, []))

    def delete_sets(self, file_id: str) -> list[DeleteSet]:
        if file_id not in self._delete_cache:
            refs = self.delete_refs(file_id)
            loaded = [load_delete_set(self.root, ref) for ref in refs]
            self._delete_cache[file_id] = loaded
        return self._delete_cache[file_id]

    def deleted_positions(self, file_id: str) -> list[int]:
        return merge_positions(*(ds.positions for ds in self.delete_sets(file_id)))

    def delete_events(self) -> list[dict]:
        """Flattened delete provenance for the UI."""
        out = []
        for f in self.files:
            for ds in self.delete_sets(f.id):
                ref = ref_for(ds)
                out.append(
                    {
                        "file_id": f.id,
                        "delete_id": ds.id,
                        "positions": len(ds.positions),
                        "position_list": list(ds.positions),
                        "reason": ds.reason,
                        "predicate": ds.predicate,
                        "source_snapshot": ds.source_snapshot,
                        "created_at": ds.created_at,
                        "derived_from": ds.derived_from,
                    }
                )
        return out

    # -- validation -------------------------------------------------------
    def _validate(self) -> None:
        problems: list[str] = []
        try:
            recorded_checksum = self.manifest.get("checksum", "")
            if recorded_checksum and manifest_checksum(self.manifest) != recorded_checksum:
                problems.append("清单校验和不匹配")
            if self.manifest.get("schema_hash") != self.schema.hash:
                problems.append("清单 schema 与目录 schema 不一致")
        except Exception as exc:  # pragma: no cover - defensive
            problems.append(f"清单校验失败: {exc}")
        seen_files: set[str] = set()
        for d in self.manifest.get("files", []):
            try:
                f = DataFile.from_dict(d)
            except Exception as exc:
                problems.append(f"数据文件条目损坏: {exc}")
                continue
            if f.id in seen_files:
                problems.append(f"数据文件重复: {f.id}")
            seen_files.add(f.id)
            if f.rows <= 0:
                problems.append(f"数据文件 {f.id} 行数非法")
            rel = f.path
            if ".." in rel or not rel.startswith(DATA_DIR + "/") or not rel.endswith(".parquet"):
                problems.append(f"数据文件路径非法: {rel}")
        for file_id, refs in (self.manifest.get("deletes") or {}).items():
            if file_id not in seen_files:
                problems.append(f"删除集引用了未知数据文件 {file_id}")
                continue
            rows = next((int(d["rows"]) for d in self.manifest["files"] if d["id"] == file_id), 0)
            seen_positions: set[int] = set()
            for ref in refs:
                rel = ref.get("path", "")
                if ".." in rel or not rel.startswith(DELETES_DIR + "/") or not rel.endswith(".json"):
                    problems.append(f"删除集路径非法: {rel}")
                    continue
                try:
                    ds = load_delete_set(self.root, ref)
                except CorruptState as exc:
                    problems.append(exc.message)
                    continue
                if ds.row_count != rows:
                    problems.append(
                        f"删除集 {ds.id} 的目标行数与数据文件 {file_id} 不一致"
                    )
                overlap = seen_positions.intersection(ds.positions)
                if overlap:
                    problems.append(f"删除集 {ds.id} 与同一文件上的其他删除集位置重叠")
                seen_positions.update(ds.positions)
        if problems:
            raise CorruptState(
                f"快照 {self.manifest.get('snapshot_id')} 完整性检查失败: " + "; ".join(problems)
            )

    # -- presentation -----------------------------------------------------
    def to_api(self, detail: bool = True) -> dict:
        data = {
            "id": self.id,
            "seq": self.seq,
            "created_at": self.created_at,
            "parent": self.parent,
            "op": self.op,
            "summary": self.summary_text,
            "stats": self.stats,
            "expired": bool(self.summary.expired) if self.summary else False,
            "operation": self.operation,
        }
        if detail:
            data["files"] = []
            for f in self.files:
                chains = self.delete_sets(f.id)
                data["files"].append(
                    {
                        **f.to_dict(),
                        "deleted_rows": sum(len(c.positions) for c in chains),
                        "delete_sets": [
                            {
                                **ref_for(ds),
                                "position_list": list(ds.positions),
                            }
                            for ds in chains
                        ],
                    }
                )
            data["delete_events"] = self.delete_events()
        return data


def ref_for(ds: DeleteSet) -> dict:
    return {
        "id": ds.id,
        "path": ds.path,
        "positions": len(ds.positions),
        "target_rows": ds.row_count,
        "reason": ds.reason,
        "predicate": ds.predicate,
        "source_snapshot": ds.source_snapshot,
        "created_at": ds.created_at,
        "derived_from": ds.derived_from,
    }


class Store:
    """A single-writer (serialised) lake-table directory."""

    def __init__(self, root: str, *, lock_timeout: float = 15.0):
        self.root = os.path.abspath(root)
        self._lock = FileLock(os.path.join(self.root, LOCK_NAME), timeout=lock_timeout)
        # Guards writers inside one process in addition to flock (flock is
        # per file description, which already serialises separate opens).
        self._mutex = threading.RLock()

    # -- helpers ----------------------------------------------------------
    def _paths(self) -> None:
        for d in (DATA_DIR, DELETES_DIR, SNAPSHOT_DIR, STAGING_DIR, JOBS_DIR):
            os.makedirs(os.path.join(self.root, d), exist_ok=True)

    def _catalog(self) -> Catalog:
        return load_catalog(self.root)

    def _snapshot_from_manifest(self, manifest: dict, schema: Schema) -> Snapshot:
        return Snapshot(self.root, manifest, schema)

    def _load_summary(self, catalog: Catalog, sid: str | None) -> SnapshotSummary:
        if sid is None:
            if catalog.head is None:
                raise NotFound("湖表还没有任何快照")
            sid = catalog.head
        summary = catalog.summary(sid)
        if summary is None:
            raise NotFound(f"快照 {sid} 不存在（可能已被回收）")
        return summary

    def _expected_head_check(self, catalog: Catalog, expected_head: str | None) -> None:
        if expected_head is not None and expected_head != catalog.head:
            raise Conflict(
                f"期望从 {expected_head} 开始写入，但当前 head 是 {catalog.head}",
                expected_head=expected_head,
                actual_head=catalog.head,
            )

    def _new_snapshot(
        self,
        catalog: Catalog,
        *,
        op: str,
        summary: str,
        parent: str | None,
        files: list[DataFile],
        deletes: dict[str, list[dict]],
        operation: dict,
    ) -> Snapshot:
        """Build + persist manifest and update catalog (caller holds lock)."""
        seq = catalog.next_seq()
        sid = new_snapshot_id(seq)
        manifest = {
            "format": FORMAT,
            "snapshot_id": sid,
            "seq": seq,
            "created_at": utcnow(),
            "parent": parent,
            "op": op,
            "summary": summary,
            "schema_hash": catalog.schema.hash,
            "files": [f.to_dict() for f in files],
            "deletes": deletes,
            "operation": operation,
            "stats": build_stats(
                [f.to_dict() for f in files],
                {fid: {"positions": sum(r.get("positions", 0) for r in refs)} for fid, refs in deletes.items()},
            ),
            "checksum": "",
        }
        manifest["checksum"] = manifest_checksum(manifest)
        snap = self._snapshot_from_manifest(manifest, catalog.schema)
        save_manifest(self.root, manifest)
        summary_obj = SnapshotSummary(
            id=sid,
            seq=seq,
            created_at=manifest["created_at"],
            op=op,
            summary=summary,
            parent=parent,
            stats=manifest["stats"],
        )
        catalog.snapshots.append(summary_obj)
        catalog.head = sid
        catalog.seq = seq
        save_catalog(self.root, catalog)
        snap.summary = summary_obj
        return snap

    # -- lifecycle --------------------------------------------------------
    def exists(self) -> bool:
        return os.path.exists(os.path.join(self.root, "posdel.json"))

    def init(self, schema: Schema, rows: Sequence[dict] | None = None, *, chunk_rows: int = 4096) -> Snapshot:
        with self._mutex, self._lock.held():
            self._paths()
            if os.path.exists(os.path.join(self.root, "posdel.json")):
                raise Conflict("该目录已存在湖表（posdel.json）；请换一个空目录或先删除")
            catalog = Catalog(
                format=FORMAT,
                created_at=utcnow(),
                updated_at=utcnow(),
                schema=schema,
                head=None,
                seq=0,
            )
            files: list[DataFile] = []
            operation: dict[str, Any] = {"type": "init", "rows": 0, "file_ids": []}
            if rows:
                normalized = normalize_rows(schema, list(rows))
                chunk = max(1, min(int(chunk_rows), 50_000))
                for start in range(0, len(normalized), chunk):
                    piece = normalized[start:start + chunk]
                    file_id = ids.data_file_uid()
                    df = write_data_file(self.root, file_id, schema, rows_to_table(schema, piece))
                    files.append(df)
                operation = {"type": "init", "rows": len(normalized), "file_ids": [f.id for f in files]}
            summary = f"初始化湖表（{len(files)} 个文件，{operation['rows']} 行）"
            catalog.head = None
            return self._new_snapshot(
                catalog,
                op="init",
                summary=summary,
                parent=None,
                files=files,
                deletes={},
                operation=operation,
            )

    # -- read -------------------------------------------------------------
    def snapshots(self, include_expired: bool = True) -> dict:
        with self._lock.held(shared=True):
            catalog = self._catalog()
            return {
                "format": FORMAT,
                "head": catalog.head,
                "seq": catalog.seq,
                "schema": catalog.schema.to_dict(),
                "created_at": catalog.created_at,
                "updated_at": catalog.updated_at,
                "snapshots": [
                    {
                        **s.to_dict(),
                        "is_head": s.id == catalog.head,
                    }
                    for s in catalog.snapshots
                    if include_expired or not s.expired
                ],
            }

    def head(self) -> Snapshot:
        with self._lock.held(shared=True):
            catalog = self._catalog()
            summary = self._load_summary(catalog, None)
            manifest = load_manifest(self.root, summary.id)
            return self._snapshot_from_manifest(manifest, catalog.schema)

    def snapshot(self, sid: str | None = None, *, allow_expired: bool = False) -> Snapshot:
        with self._lock.held(shared=True):
            catalog = self._catalog()
            summary = self._load_summary(catalog, sid)
            if summary.expired and not allow_expired:
                raise Conflict(
                    f"快照 {summary.id} 已过期，不再提供查询（其文件仍可能尚未回收）",
                    snapshot=summary.id,
                )
            manifest = load_manifest(self.root, summary.id)
            snap = self._snapshot_from_manifest(manifest, catalog.schema)
            snap.summary = summary
            return snap

    def query(
        self,
        sid: str | None = None,
        *,
        mode: str = "visible",
        conditions: Sequence[Condition] | None = None,
        match: str = "all",
        file_id: str | None = None,
        offset: int = 0,
        limit: int = 50,
    ) -> dict:
        if mode not in ("visible", "raw"):
            raise ValidationError("mode 只能是 visible 或 raw")
        if offset < 0:
            raise ValidationError("offset 不能为负数")
        if not (1 <= limit <= 1000):
            raise ValidationError("limit 范围 1..1000")
        conditions = list(conditions or [])
        catalog = self._catalog()
        if conditions and all(isinstance(c, dict) for c in conditions):
            conditions = build_conditions(catalog.schema, conditions)
        with self._lock.held(shared=True):
            summary = self._load_summary(catalog, sid)
            if summary.expired:
                raise Conflict(
                    f"快照 {summary.id} 已过期，不再提供查询",
                    snapshot=summary.id,
                )
            snap = self._snapshot_from_manifest(load_manifest(self.root, summary.id), catalog.schema)
            snap.summary = summary
            return self._scan(snap, mode=mode, conditions=conditions, match=match,
                              file_id=file_id, offset=offset, limit=limit)

    def _scan(self, snap: Snapshot, *, mode: str, conditions: Sequence[Condition],
              match: str, file_id: str | None, offset: int, limit: int) -> dict:
        files = snap.files
        if file_id is not None:
            files = [f for f in files if f.id == file_id]
            if not files:
                raise NotFound(f"快照 {snap.id} 中没有数据文件 {file_id}")
        schema = snap.schema
        rows_out: list[dict] = []
        file_report: list[dict] = []
        total_selected = 0
        cursor = 0  # global index in the selected mode
        page_start, page_end = offset, offset + limit

        for f in files:
            table = read_data_file(self.root, f)
            if conditions:
                cond_mask = predicate_mask(table, conditions, match, schema)
                cond_np = np.asarray(cond_mask.to_numpy(zero_copy_only=False), dtype=bool)
            else:
                cond_np = np.ones(f.rows, dtype=bool)
            deleted_positions = snap.deleted_positions(f.id)
            deleted_np = np.zeros(f.rows, dtype=bool)
            if deleted_positions:
                deleted_np[np.asarray(deleted_positions, dtype=np.int64)] = True
            mode_np = cond_np if mode == "raw" else (cond_np & ~deleted_np)
            selected = np.flatnonzero(mode_np)
            count = int(selected.size)
            total_selected += count
            file_report.append(
                {
                    "id": f.id,
                    "path": f.path,
                    "rows": f.rows,
                    "deleted": int(deleted_np.sum()),
                    "visible": f.rows - int(deleted_np.sum()),
                    "matched": count,
                    "sha256": f.sha256,
                    "created_at": f.created_at,
                    "delete_sets": [
                        {
                            "id": ds.id,
                            "path": ds.path,
                            "positions": len(ds.positions),
                            "position_list": list(ds.positions),
                            "reason": ds.reason,
                            "predicate": ds.predicate,
                            "source_snapshot": ds.source_snapshot,
                            "created_at": ds.created_at,
                            "derived_from": list(ds.derived_from),
                        }
                        for ds in snap.delete_sets(f.id)
                    ],
                }
            )
            lo = max(page_start - cursor, 0)
            hi = min(page_end - cursor, count)
            if hi > lo:
                page_positions = selected[lo:hi]
                indexes = [int(p) for p in page_positions]
                slice_table = table.take(pa.array(indexes, type=pa.int64()))
                data_rows = slice_table.to_pylist()
                deleted_by: dict[int, DeleteSet] = {}
                for ds in snap.delete_sets(f.id):
                    for p in ds.positions:
                        deleted_by[p] = ds
                for local_i, pos in enumerate(indexes):
                    ds = deleted_by.get(pos)
                    visible_no = int(cursor + lo + local_i) if mode == "visible" else None
                    raw_no = int(cursor + lo + local_i) if mode == "raw" else None
                    rows_out.append(
                        {
                            "data": data_rows[local_i],
                            "meta": {
                                "snapshot": snap.id,
                                "file_id": f.id,
                                "file_path": f.path,
                                "position": pos,
                                "deleted": bool(deleted_np[pos]),
                                "row_no": raw_no if mode == "raw" else None,
                                "visible_no": visible_no,
                                "delete": (
                                    {
                                        "delete_id": ds.id,
                                        "reason": ds.reason,
                                        "predicate": ds.predicate,
                                        "source_snapshot": ds.source_snapshot,
                                        "created_at": ds.created_at,
                                        "derived_from": ds.derived_from,
                                    }
                                    if ds
                                    else None
                                ),
                            },
                        }
                    )
            cursor += count

        return {
            "snapshot": snap.to_api(),
            "mode": mode,
            "match": match,
            "conditions": [c.to_dict() for c in conditions],
            "predicate": describe_predicate(conditions, match),
            "file_id": file_id,
            "offset": offset,
            "limit": limit,
            "total": total_selected,
            "files": file_report,
            "rows": rows_out,
        }

    def file_stats(self, sid: str | None = None) -> dict:
        snap = self.snapshot(sid)
        files = []
        for f in snap.files:
            deleted = len(snap.deleted_positions(f.id))
            files.append(
                {
                    **f.to_dict(),
                    "deleted_rows": deleted,
                    "visible_rows": f.rows - deleted,
                    "delete_sets": [
                        {
                            **ref_for(ds),
                            "position_list": list(ds.positions),
                        }
                        for ds in snap.delete_sets(f.id)
                    ],
                }
            )
        return {"snapshot": snap.id, "files": files, "stats": snap.stats}

    # -- write: append ----------------------------------------------------
    def append(self, rows: Sequence[dict], *, expected_head: str | None = None,
               summary_note: str | None = None) -> dict:
        with self._mutex, self._lock.held():
            catalog = self._catalog()
            self._expected_head_check(catalog, expected_head)
            head_summary = self._load_summary(catalog, None)
            head = self._snapshot_from_manifest(load_manifest(self.root, head_summary.id), catalog.schema)
            normalized = normalize_rows(catalog.schema, list(rows))
            file_id = ids.data_file_uid()
            df = write_data_file(self.root, file_id, catalog.schema,
                                 rows_to_table(catalog.schema, normalized))
            files = head.files + [df]
            deletes = {f.id: head.manifest.get("deletes", {}).get(f.id, []) for f in head.files}
            note = (summary_note or "").strip()
            summary = f"追加 {len(normalized)} 行到新文件 {df.id}" + (f"：{note}" if note else "")
            snap = self._new_snapshot(
                catalog,
                op="append",
                summary=summary,
                parent=head.id,
                files=files,
                deletes=deletes,
                operation={
                    "type": "append",
                    "rows": len(normalized),
                    "file_ids": [df.id],
                    "expected_head": expected_head,
                    "note": note,
                },
            )
            return {"changed": True, "snapshot": snap.to_api(), "file": df.to_dict(),
                    "previous_head": head.id}

    # -- write: delete ----------------------------------------------------
    def delete(
        self,
        conditions: Sequence[dict] | Sequence[Condition],
        *,
        match: str = "all",
        reason: str = "",
        expected_head: str | None = None,
        dry_run: bool = False,
    ) -> dict:
        if match not in ("all", "any"):
            raise ValidationError("match 只能是 all 或 any")
        with self._mutex, self._lock.held(shared=dry_run):
            catalog = self._catalog()
            self._expected_head_check(catalog, expected_head)
            schema = catalog.schema
            conds = build_conditions(schema, list(conditions)) if conditions and isinstance(conditions[0], dict) else list(conditions)  # type: ignore[arg-type]
            if not conds:
                raise ValidationError("必须至少提供一个删除条件（不支持无条件删除全部行）")
            if len(reason) > 500:
                raise ValidationError("reason 过长（上限 500 字符）")
            head_summary = self._load_summary(catalog, None)
            head = self._snapshot_from_manifest(load_manifest(self.root, head_summary.id), schema)
            if len(head.files) == 0:
                return {"changed": False, "snapshot": head.to_api(), "preview": {},
                        "message": "表中没有数据文件"}
            explicit_reason = reason.strip()
            predicate = describe_predicate(conds, match)
            event_reason = explicit_reason or f"条件删除：{predicate}"
            planned: list[dict] = []
            pending_sets: list[tuple[str, DeleteSet]] = []
            total = 0
            now = utcnow()
            upcoming_sid = new_snapshot_id(catalog.next_seq())
            for f in head.files:
                table = read_data_file(self.root, f)
                cond_mask = predicate_mask(table, conds, match, schema).to_numpy(zero_copy_only=False)
                deleted_positions = head.deleted_positions(f.id)
                deleted_np = np.zeros(f.rows, dtype=bool)
                if deleted_positions:
                    deleted_np[np.asarray(deleted_positions, dtype=np.int64)] = True
                selected = np.flatnonzero(np.asarray(cond_mask, dtype=bool) & ~deleted_np)
                if selected.size:
                    positions = [int(p) for p in selected]
                    ds = make_delete_set(
                        positions,
                        row_count=f.rows,
                        reason=event_reason,
                        predicate=predicate,
                        source_snapshot=upcoming_sid,
                        created_at=now,
                    )
                    pending_sets.append((f.id, ds))
                    planned.append({"file_id": f.id, "positions": positions,
                                    "count": len(positions), "delete_id": ds.id})
                    total += len(positions)
            preview = {
                "predicate": predicate,
                "match": match,
                "conditions": [c.to_dict() for c in conds],
                "reason": event_reason,
                "would_delete": total,
                "files": planned,
                "head": head.id,
            }
            if dry_run or total == 0:
                return {
                    "changed": False,
                    "snapshot": head.to_api(),
                    "preview": preview,
                    "dry_run": dry_run,
                    "message": ("预览：未写入任何删除记录" if dry_run
                                else "没有可见行匹配该条件，未生成新版本"),
                }
            # Persist immutable delete files first, then build the manifest.
            new_deletes = {f.id: list(head.manifest.get("deletes", {}).get(f.id, [])) for f in head.files}
            persisted = []
            for file_id, ds in pending_sets:
                ds = DeleteSet(**{**ds.__dict__, "path": persist_delete_set(self.root, ds)})
                new_deletes[file_id].append(ref_for(ds))
                persisted.append({"file_id": file_id, **ref_for(ds)})
            summary = f"按条件删除 {total} 行（来源位置集合）：{predicate}"
            note = explicit_reason
            if note:
                summary += f"；说明：{note}"
            snap = self._new_snapshot(
                catalog,
                op="delete",
                summary=summary,
                parent=head.id,
                files=head.files,
                deletes=new_deletes,
                operation={
                    "type": "delete",
                    "predicate": predicate,
                    "match": match,
                    "conditions": [c.to_dict() for c in conds],
                    "reason": event_reason,
                    "note": explicit_reason,
                    "deleted_rows": total,
                    "delete_sets": persisted,
                    "expected_head": expected_head,
                },
            )
            if snap.id != upcoming_sid:  # pragma: no cover - seq advance guard
                raise CorruptState("内部错误：提交的快照序号与预分配不一致")
            preview["created_snapshot"] = snap.id
            return {"changed": True, "snapshot": snap.to_api(), "preview": preview,
                    "previous_head": head.id}

    # -- maintenance ------------------------------------------------------
    def verify(self, sid: str | None = None, *, full: bool = False) -> dict:
        from .files import verify_data_file

        with self._lock.held(shared=True):
            catalog = self._catalog()
            summary = self._load_summary(catalog, sid)
            manifest = load_manifest(self.root, summary.id)
            snap = self._snapshot_from_manifest(manifest, catalog.schema)
            snap.summary = summary
            issues: list[str] = []
            checked_files = 0
            for f in snap.files:
                if not os.path.exists(os.path.join(self.root, f.path)):
                    issues.append(f"数据文件缺失: {f.path}")
                    continue
                if full:
                    if not verify_data_file(self.root, f):
                        issues.append(f"文件哈希/存在性校验失败: {f.path}")
                checked_files += 1
            deleted_positions = sum(len(snap.deleted_positions(f.id)) for f in snap.files)
            return {
                "snapshot": snap.to_api(),
                "ok": not issues,
                "issues": issues,
                "checked_files": checked_files,
                "full": full,
                "row_count": sum(f.rows for f in snap.files),
                "deleted_positions": deleted_positions,
            }

    def resolve_conditions(self, conditions: Sequence[dict], match: str = "all") -> tuple[list[Condition], str]:
        catalog = self._catalog()
        conds = build_conditions(catalog.schema, conditions)
        return conds, describe_predicate(conds, match)
