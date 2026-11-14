"""Merge (compaction) jobs: prepare → commit with conflict detection.

Why a job?
----------
Rewriting files changes every row position.  A merge prepared against snapshot
``B`` must therefore **never** apply the old delete-position sets to its new
file.  Instead it records a ``old position -> new position`` mapping while it
writes the merged file, and disposes of the base delete sets by having applied
them during preparation.

If a delete commits between preparation and commit, the merge detects it:
the new delete positions are translated through the mapping into the merged
file (only rows that were visible at ``B`` can have been selected by a later
delete).  That is the "redo the necessary part" path.  If an input file was
itself rewritten/merged away meanwhile the whole job is re-planned against
the new head, because the mapping is no longer meaningful.

Commit is idempotent and crash-safe: the merged file is published under
``data/`` before the catalog swap; a crash before the swap leaves an
unreferenced file that recovery can reuse or garbage-collect.
"""

from __future__ import annotations

import os
import shutil
from typing import Any, Callable, Sequence

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from . import ids
from .catalog import (
    DATA_DIR,
    JOBS_DIR,
    STAGING_DIR,
    build_stats,
    load_catalog,
    load_manifest,
    manifest_checksum,
    save_catalog,
    snapshot_relpath,
)
from .errors import Conflict, CorruptState, NotFound, ValidationError
from .files import (
    DataFile,
    DeleteSet,
    load_delete_set,
    make_delete_set,
    persist_delete_set,
    read_data_file,
)
from .fsutil import fsync_dir, read_json, sha256_file, utcnow, write_json_atomic
from .schema import Schema
from .store import Snapshot, ref_for

JOB_FORMAT = "posdel-job/1"
ACTIVE_STATUSES = {"prepared", "committing", "failed"}
TERMINAL_STATUSES = {"committed", "aborted"}


# ---------------------------------------------------------------------------
# Job storage
# ---------------------------------------------------------------------------

def jobs_dir(root: str) -> str:
    return f"{root}/{JOBS_DIR}"


def job_path(root: str, job_id: str) -> str:
    return f"{jobs_dir(root)}/{job_id}.json"


def staging_for(root: str, job_id: str) -> str:
    return f"{root}/{STAGING_DIR}/{job_id}"


def save_job(root: str, job: dict) -> None:
    job["updated_at"] = utcnow()
    write_json_atomic(job_path(root, job["job_id"]), job)


def load_job(root: str, job_id: str) -> dict:
    path = job_path(root, job_id)
    if not os.path.exists(path):
        raise NotFound(f"合并任务不存在: {job_id}")
    try:
        job = read_json(path)
    except Exception as exc:
        raise CorruptState(f"任务记录损坏 {job_id}: {exc}") from exc
    if job.get("format") != JOB_FORMAT:
        raise CorruptState(f"任务记录格式不匹配: {job_id}")
    return job


def list_jobs(root: str) -> list[dict]:
    out = []
    directory = jobs_dir(root)
    if not os.path.isdir(directory):
        return out
    for name in sorted(os.listdir(directory)):
        if not name.endswith(".json") or name.startswith("."):
            continue
        try:
            job = load_job(root, name[:-5])
        except CorruptState as exc:
            out.append({"job_id": name[:-5], "status": "corrupt", "error": exc.message})
            continue
        out.append(job)
    out.sort(key=lambda j: (j.get("created_at", ""), j.get("job_id", "")))
    return out


def active_jobs(root: str) -> list[dict]:
    return [j for j in list_jobs(root) if j.get("status") in ACTIVE_STATUSES]


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------

def _pick_inputs(base: Snapshot, file_ids: Sequence[str] | None) -> list[DataFile]:
    if file_ids is None:
        chosen = list(base.files)
    else:
        if not isinstance(file_ids, (list, tuple)) or not file_ids:
            raise ValidationError("file_ids 必须是非空数组")
        if len(set(file_ids)) != len(file_ids):
            raise ValidationError("file_ids 有重复")
        chosen = []
        for fid in file_ids:
            try:
                chosen.append(base.file(fid))
            except NotFound:
                raise ValidationError(f"快照 {base.id} 中不存在数据文件 {fid}")
        chosen.sort(key=lambda f: base.files.index(f))
    if not chosen:
        raise ValidationError("没有可合并的数据文件")
    return chosen


def _build_output(
    store,
    base: Snapshot,
    inputs: list[DataFile],
    *,
    job_id: str,
    schema: Schema,
) -> tuple[dict | None, dict[str, list[int]]]:
    """Read inputs, apply base deletes, write staged merged file.

    Returns ``(output record or None, mapping)`` where ``mapping[file_id] = P``
    lists the file-local old positions in merged order, i.e. old position
    ``P[k]`` lands at **global** merged position ``offset + k``.  Keeping the
    per-file offset separate avoids the classic off-by-one that would appear
    if each file's mapping restarted at 0.
    """
    tables: list[pa.Table] = []
    mapping: dict[str, list[int]] = {}
    offsets: dict[str, int] = {}
    total = 0
    for f in inputs:
        table = read_data_file(store.root, f)
        deleted = set(base.deleted_positions(f.id))
        old_positions = [p for p in range(table.num_rows) if p not in deleted]
        mapping[f.id] = old_positions
        offsets[f.id] = total
        if old_positions:
            tables.append(table.take(pa.array(old_positions, type=pa.int64())))
            total += len(old_positions)

    if total == 0:
        return None, mapping, offsets
    merged = tables[0] if len(tables) == 1 else pa.concat_tables(tables)
    if merged.num_rows != total:  # pragma: no cover - defensive
        raise CorruptState("合并结果行数与映射不一致")
    if merged.schema != schema.arrow_schema():
        merged = merged.cast(schema.arrow_schema())

    staged_dir = staging_for(store.root, job_id)
    os.makedirs(staged_dir, exist_ok=True)
    staged_path = os.path.join(staged_dir, "merged.parquet")
    tmp_path = os.path.join(staged_dir, ".merged.parquet.tmp")
    try:
        pq.write_table(merged, tmp_path, compression="zstd", version="1.0", row_group_size=8192)
        digest = sha256_file(tmp_path)
        os.replace(tmp_path, staged_path)
        fsync_dir(staged_dir)
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)

    file_id = ids.data_file_uid()
    output = {
        "file_id": file_id,
        "staging_path": f"{STAGING_DIR}/{job_id}/merged.parquet",
        "final_path": f"{DATA_DIR}/{file_id}.parquet",
        "rows": int(merged.num_rows),
        "sha256": digest,
        "created_at": utcnow(),
        "rows_per_group": 8192,
    }
    return output, mapping, offsets


def plan_merge(
    store,
    *,
    base_snapshot: str | None = None,
    file_ids: Sequence[str] | None = None,
    note: str = "",
    job_id: str | None = None,
    fault_inject: Callable[[str, dict], None] | None = None,
) -> dict:
    """Prepare a merge job without committing it.

    The base snapshot and its delete positions are captured under a shared
    lock; the (potentially slow) rewrite happens after the lock is released,
    which is exactly why a delete can slip in between prepare and commit.
    """
    with store._lock.held(shared=True):
        catalog = load_catalog(store.root)
        summary = store._load_summary(catalog, base_snapshot)
        if summary.expired:
            raise Conflict(f"快照 {summary.id} 已过期，不能作为合并基线", snapshot=summary.id)
        base_summary = summary
        base_manifest = load_manifest(store.root, summary.id)
        base = Snapshot(store.root, base_manifest, catalog.schema)
        base.summary = summary
        inputs = _pick_inputs(base, file_ids)
        schema = catalog.schema
        base_manifest_checksum = base_manifest.get("checksum", "")
        input_records = [
            {
                "file_id": f.id,
                "path": f.path,
                "rows": f.rows,
                "sha256": f.sha256,
                "delete_refs": [dict(r) for r in base.delete_refs(f.id)],
                "delete_ref_paths": [r["path"] for r in base.delete_refs(f.id)],
                "deleted_positions": base.deleted_positions(f.id),
            }
            for f in inputs
        ]
        merged_before = len(inputs)
    # Lock released: writers (delete/append) may now commit against the head.

    job_id = job_id or ids.job_id()
    if os.path.exists(job_path(store.root, job_id)):
        raise Conflict(f"任务 {job_id} 已存在")
    created = utcnow()
    output, mapping, mapping_offsets = _build_output(store, base, inputs, job_id=job_id, schema=schema)
    if fault_inject:
        fault_inject("after_output_built", {"job_id": job_id, "output": output})
    job = {
        "format": JOB_FORMAT,
        "job_id": job_id,
        "kind": "merge",
        "status": "prepared",
        "created_at": created,
        "updated_at": created,
        "note": note.strip()[:500],
        "base_snapshot": base.id,
        "base_seq": base.seq,
        "base_manifest_checksum": base_manifest_checksum,
        "input_file_ids": [f.id for f in inputs],
        "inputs": input_records,
        "mapping": mapping,
        "mapping_offsets": mapping_offsets,
        "output": output,
        "attempts": [],
        "result": None,
        "error": None,
    }
    save_job(store.root, job)
    return job


# ---------------------------------------------------------------------------
# Analysis of changes since the base snapshot
# ---------------------------------------------------------------------------

def analyze_conflict(job: dict, head: Snapshot) -> dict:
    """Compare the job's base inputs with ``head``.

    Returns ``{"mode": "clean"|"absorb"|"redo", ...}``.  ``absorb`` means the
    only changes touching the merge input files are delete sets added after
    the base snapshot, every position of which is present in the mapping.
    """
    input_ids = set(job["input_file_ids"])
    head_ids = {f.id for f in head.files}
    missing = input_ids - head_ids
    if missing:
        return {
            "mode": "redo",
            "reason": "输入文件已被其他操作合并/替换，旧位置映射失效",
            "missing_files": sorted(missing),
        }
    added: list[dict] = []
    mapping = job["mapping"]
    offsets = job.get("mapping_offsets") or {}
    for record in job["inputs"]:
        fid = record["file_id"]
        base_ref_paths = set(record.get("delete_ref_paths") or [])
        base_ref_ids = {r.get("id") for r in record.get("delete_refs", [])}
        for ref in head.delete_refs(fid):
            if ref.get("path") in base_ref_paths or ref.get("id") in base_ref_ids:
                continue
            try:
                ds = load_delete_set(head.root, ref)
            except CorruptState as exc:
                return {"mode": "redo", "reason": f"无法读取新删除集: {exc.message}"}
            old_positions = mapping.get(fid, [])
            base = int(offsets.get(fid, 0))
            # old file-local position -> global position in the merged file
            lookup = {old: base + k for k, old in enumerate(old_positions)}
            unmappable = [p for p in ds.positions if p not in lookup]
            if unmappable:
                return {
                    "mode": "redo",
                    "reason": "新删除记录包含基线不可见位置，无法安全重映射",
                    "delete_set": ds.id,
                    "unmappable": unmappable[:20],
                }
            added.append(
                {
                    "file_id": fid,
                    "source": ref_for(ds),
                    "old_positions": list(ds.positions),
                    "mapping_offset": base,
                    "remap": {str(p): lookup[p] for p in ds.positions},
                    "remapped_positions": sorted(lookup[p] for p in ds.positions),
                }
            )
    if not added:
        return {"mode": "clean", "reason": "head 与基线一致或只追加了无关文件"}
    if job.get("output") is None:
        return {"mode": "redo", "reason": "合并结果为空但出现了新的删除记录"}
    return {"mode": "absorb", "reason": "检测到基线之后的删除，已通过位置映射重映射", "added": added}

# ---------------------------------------------------------------------------
# Commit
# ---------------------------------------------------------------------------

def _ensure_output(store, job: dict, *, fault_inject: Callable[[str, dict], None] | None = None) -> dict | None:
    """Make sure the merged file exists under data/ and matches its sha."""
    output = job.get("output")
    if output is None:
        return None
    root = store.root
    final = os.path.join(root, output["final_path"])
    staged = os.path.join(root, output["staging_path"])
    if os.path.exists(final):
        if sha256_file(final) != output["sha256"]:
            raise CorruptState(f"已发布合并文件校验失败: {output['final_path']}")
        return output
    if not os.path.exists(staged):
        return None  # caller must rebuild
    if sha256_file(staged) != output["sha256"]:
        return None
    os.makedirs(os.path.dirname(final), exist_ok=True)
    os.replace(staged, final)
    fsync_dir(os.path.dirname(final))
    if fault_inject:
        fault_inject("after_output_published", {"job_id": job["job_id"], "output": output})
    return output


def _surviving_inputs(job: dict, head: Snapshot) -> list[str]:
    """Input files from ``job`` that still exist in ``head``.

    When another merge already rewrote some inputs, only the survivors can be
    re-merged; the replaced rows are already in head's own merged file.
    """
    head_ids = {f.id for f in head.files}
    survive = [fid for fid in job["input_file_ids"] if fid in head_ids]
    if survive:
        return survive
    # None of the original inputs survived: another merge replaced them all.
    # Re-merging the entire head is the only meaningful rebuild; it cannot
    # duplicate rows because the replaced rows already live in head's files.
    return None


def _rebuild_job_locked(store, job: dict, head: Snapshot, schema: Schema, reason: str, attempt: dict) -> None:
    """Re-plan the job against the current head because the old plan died.

    The stale staged output is discarded; a fresh mapping and file are built.
    """
    inputs = _pick_inputs(head, _surviving_inputs(job, head))
    old_staged = staging_for(store.root, job["job_id"])
    if os.path.isdir(old_staged):
        shutil.rmtree(old_staged, ignore_errors=True)
    output, mapping, mapping_offsets = _build_output(store, head, inputs, job_id=job["job_id"], schema=schema)
    job["base_snapshot"] = head.id
    job["base_seq"] = head.seq
    job["base_manifest_checksum"] = head.manifest.get("checksum", "")
    job["input_file_ids"] = [f.id for f in inputs]
    job["inputs"] = [
        {
            "file_id": f.id,
            "path": f.path,
            "rows": f.rows,
            "sha256": f.sha256,
            "delete_refs": [dict(r) for r in head.delete_refs(f.id)],
            "delete_ref_paths": [r["path"] for r in head.delete_refs(f.id)],
            "deleted_positions": head.deleted_positions(f.id),
        }
        for f in inputs
    ]
    job["mapping"] = mapping
    job["mapping_offsets"] = mapping_offsets
    job["output"] = output
    attempt["rebuild"] = {"from_base": attempt.get("base_snapshot"), "to_base": head.id, "reason": reason}


def _final_manifest_parts(
    store,
    job: dict,
    head: Snapshot,
    analysis: dict,
    output: dict | None,
    output_data_file: DataFile | None,
) -> tuple[list[DataFile], dict[str, list[dict]], list[dict]]:
    """Assemble file list + delete map for the merged snapshot."""
    input_ids = set(job["input_file_ids"])
    files: list[DataFile] = []
    deletes: dict[str, list[dict]] = {}
    inserted = False
    for f in head.files:
        if f.id in input_ids:
            if not inserted and output_data_file is not None:
                files.append(output_data_file)
                inserted = True
            continue
        files.append(f)
        deletes[f.id] = [dict(r) for r in head.delete_refs(f.id)]
    if not inserted and output_data_file is not None:
        files.append(output_data_file)

    absorbed: list[dict] = []
    if output_data_file is not None:
        deletes.setdefault(output_data_file.id, [])
        for item in analysis.get("added", []):
            src = item["source"]
            ds = make_delete_set(
                item["remapped_positions"],
                row_count=output_data_file.rows,
                reason=src["reason"],
                predicate=src["predicate"],
                source_snapshot=src["source_snapshot"],
                created_at=src["created_at"],
                derived_from=[src["id"], *src.get("derived_from", [])],
            )
            path = persist_delete_set(store.root, ds)
            ds = DeleteSet(**{**ds.__dict__, "path": path})
            deletes[output_data_file.id].append(ref_for(ds))
            absorbed.append(
                {
                    "from_delete_set": src["id"],
                    "from_file": item["file_id"],
                    "to_delete_set": ds.id,
                    "old_positions_count": src["positions"],
                    "old_positions": item.get("old_positions", []),
                    "remap": item.get("remap", {}),
                    "new_positions": item["remapped_positions"],
                    "reason": src["reason"],
                }
            )
    return files, deletes, absorbed


def commit_merge(
    store,
    job_id: str,
    *,
    fault_inject: Callable[[str, dict], None] | None = None,
) -> dict:
    """Commit a prepared merge job.  Idempotent; safe to retry after a crash."""
    with store._mutex, store._lock.held():
        return _commit_merge_locked(store, job_id, fault_inject=fault_inject)


def _find_commit_for_job(store, catalog, job_id: str) -> Snapshot | None:
    """Return the merge snapshot that already committed this job, if any."""
    for summary in reversed(catalog.snapshots):
        if summary.op != "merge" or summary.expired:
            continue
        try:
            manifest = load_manifest(store.root, summary.id)
        except CorruptState:
            continue
        operation = manifest.get("operation", {})
        if operation.get("type") == "merge" and operation.get("job_id") == job_id:
            snap = Snapshot(store.root, manifest, catalog.schema)
            snap.summary = summary
            return snap
    return None


def _commit_merge_locked(
    store,
    job_id: str,
    *,
    fault_inject: Callable[[str, dict], None] | None = None,
) -> dict:
    root = store.root
    job = load_job(root, job_id)
    if job["status"] == "committed":
        return {"changed": False, "job": job, "message": "任务已提交，无需重复执行"}
    if job["status"] == "aborted":
        raise Conflict(f"任务 {job_id} 已中止")
    catalog = load_catalog(root)
    if catalog.head is None:
        raise NotFound("湖表还没有快照")

    # Idempotency: a crash may have happened after the catalog swap but before
    # the job file was marked committed.  If any snapshot already records this
    # job, the merge is done.
    existing = _find_commit_for_job(store, catalog, job_id)
    if existing is not None:
        job["status"] = "committed"
        job["result"] = {
            "snapshot": existing.id,
            "disposition": existing.operation.get("disposition", "clean"),
            "absorbed": len(existing.operation.get("absorbed_delete_sets", [])),
            "output": existing.operation.get("output"),
        }
        job["error"] = None
        save_job(root, job)
        return {
            "changed": False,
            "job": job,
            "snapshot": existing.to_api(),
            "disposition": job["result"]["disposition"],
            "absorbed": existing.operation.get("absorbed_delete_sets", []),
            "message": f"任务 {job_id} 已在快照 {existing.id} 提交，本次未重复写入",
        }

    job["status"] = "committing"
    save_job(root, job)

    head_summary = catalog.summary(catalog.head)
    if head_summary is None:
        raise CorruptState("head 快照不在目录索引中")
    head = Snapshot(root, load_manifest(root, head_summary.id), catalog.schema)
    head.summary = head_summary

    attempt: dict[str, Any] = {
        "started_at": utcnow(),
        "base_snapshot": job["base_snapshot"],
        "head_at_start": head.id,
        "disposition": "clean",
    }
    job.setdefault("attempts", []).append(attempt)

    clean = head.id == job["base_snapshot"] or (
        job.get("base_manifest_checksum") and head.manifest.get("checksum") == job["base_manifest_checksum"]
    )
    disposition = "clean"
    if clean:
        analysis = {"mode": "clean", "reason": "head 未变化"}
    else:
        analysis = analyze_conflict(job, head)
    attempt["conflict"] = analysis

    if analysis["mode"] == "redo":
        _rebuild_job_locked(store, job, head, catalog.schema, analysis["reason"], attempt)
        disposition = "redone"
        attempt["disposition"] = disposition
        attempt["redo_reason"] = analysis["reason"]
        # After the rebuild the job is based on the very head we hold the lock
        # for; no further write can intervene.
        analysis = {"mode": "clean", "reason": "已在当前 head 上重建合并计划"}
        head = Snapshot(root, load_manifest(root, head_summary.id), catalog.schema)
        head.summary = head_summary
    elif analysis["mode"] == "absorb":
        disposition = "absorbed"
        attempt["disposition"] = disposition
    else:
        attempt["disposition"] = disposition

    schema = catalog.schema
    output = job.get("output")
    output_data_file: DataFile | None = None
    if output is not None:
        ensured = _ensure_output(store, job, fault_inject=fault_inject)
        if ensured is None:
            _rebuild_job_locked(store, job, head, schema, "合并输出的暂存文件丢失，需要重建", attempt)
            disposition = "redone"
            attempt["disposition"] = disposition
            output = job["output"]
            if output is not None:
                _ensure_output(store, job, fault_inject=fault_inject)
            output = job["output"]
        if output is not None:
            output_data_file = DataFile(
                id=output["file_id"],
                path=output["final_path"],
                rows=output["rows"],
                created_at=output["created_at"],
                sha256=output["sha256"],
                rows_per_group=output.get("rows_per_group", 8192),
            )
    fault_inject and fault_inject("after_output_published", {"job_id": job_id})

    files, deletes, absorbed = _final_manifest_parts(store, job, head, analysis, output, output_data_file)
    if fault_inject:
        fault_inject("after_delete_sets_persisted", {"job_id": job_id, "absorbed": absorbed})
        fault_inject("before_catalog_swap", {"job_id": job_id})

    total_in = sum(record["rows"] for record in job["inputs"])
    total_out = output["rows"] if output else 0
    dropped = total_in - total_out
    summary = (
        f"合并 {len(job['inputs'])} 个文件 → "
        + (f"1 个文件（{total_out} 行）" if output else "0 个文件（全部行已删除）")
        + f"；清除 {dropped} 个已删除位置"
        + ("；吸收了基线之后的删除并重映射位置" if disposition == "absorbed" else "")
        + ("；因冲突重做了合并计划" if disposition == "redone" else "")
    )
    if job.get("note"):
        summary += f"；说明：{job['note']}"
    operation = {
        "type": "merge",
        "job_id": job_id,
        "base_snapshot": job["base_snapshot"],
        "head_at_commit": head.id,
        "disposition": disposition,
        "conflict": analysis,
        "inputs": list(job["input_file_ids"]),
        "input_rows": total_in,
        "output": output["file_id"] if output else None,
        "output_rows": total_out,
        "dropped_deleted_positions": dropped,
        "absorbed_delete_sets": absorbed,
        "attempts": [{"started_at": a.get("started_at"), "disposition": a.get("disposition"),
                      "conflict": a.get("conflict")} for a in job["attempts"]],
    }
    snap = store._new_snapshot(
        catalog,
        op="merge",
        summary=summary,
        parent=head.id,
        files=files,
        deletes=deletes,
        operation=operation,
    )
    # Crash window: the merge is already visible (catalog swapped) but the job
    # record has not been updated yet.  Recovery recognises the committed
    # snapshot via operation.job_id and only marks the job.
    if fault_inject:
        fault_inject("after_catalog_swap", {"job_id": job_id, "snapshot": snap.id})
    job["status"] = "committed"
    job["result"] = {"snapshot": snap.id, "disposition": disposition, "absorbed": len(absorbed),
                     "output": output["file_id"] if output else None}
    job["error"] = None
    attempt["committed_snapshot"] = snap.id
    attempt["finished_at"] = utcnow()
    save_job(root, job)
    if fault_inject:
        fault_inject("after_job_marked", {"job_id": job_id, "snapshot": snap.id})
    return {
        "changed": True,
        "job": job,
        "snapshot": snap.to_api(),
        "disposition": disposition,
        "absorbed": absorbed,
        "previous_head": head.id,
        "summary": summary,
    }


def abort_merge(store, job_id: str, *, reason: str = "") -> dict:
    with store._mutex, store._lock.held():
        job = load_job(store.root, job_id)
        if job["status"] == "committed":
            raise Conflict("任务已提交，不能中止")
        job["status"] = "aborted"
        job["error"] = reason.strip()[:500] or "用户中止"
        save_job(store.root, job)
        staged = staging_for(store.root, job_id)
        if os.path.isdir(staged):
            shutil.rmtree(staged, ignore_errors=True)
        return {"changed": True, "job": job}
