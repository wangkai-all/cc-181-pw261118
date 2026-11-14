"""Recovery and garbage collection.

Two distinct concerns:

* **Recovery** — reconcile job records with the catalog after a crash.  A job
  stuck in ``committing`` is either already committed (a snapshot references
  it: just mark it), still committable (output is present: commit it again), or
  unrecoverable (mark aborted).  Stale staging directories are removed.
* **Garbage collection** — compute a *safe reclaim plan*: which snapshot
  manifests, data files and delete-set files are no longer reachable from any
  retained snapshot.  GC never runs implicitly; the UI shows the candidate set
  and the operator chooses the cutoff snapshot.
"""

from __future__ import annotations

import os
import shutil
from typing import Iterable

from .catalog import (
    DATA_DIR,
    DELETES_DIR,
    SNAPSHOT_DIR,
    STAGING_DIR,
    load_catalog,
    load_manifest,
    save_catalog,
)
from .errors import Conflict, CorruptState, NotFound
from .files import DataFile
from .merge import (
    ACTIVE_STATUSES,
    _find_commit_for_job,
    list_jobs,
    load_job,
    save_job,
    staging_for,
)
from .fsutil import fsync_dir, utcnow

DATA_PREFIX = "f_"
DELETE_PREFIX = "d_"


def recover(store, *, auto_commit: bool = True) -> dict:
    """Repair state after an interrupted run.  Returns a structured report."""
    from .merge import commit_merge

    report: dict = {"actions": [], "jobs": [], "errors": []}
    with store._mutex:
        # Ensure unfinished jobs' commits are serialised against writers.
        catalog = load_catalog(store.root)
        for job in list_jobs(store.root):
            status = job.get("status")
            if status not in ACTIVE_STATUSES:
                continue
            entry = {"job_id": job["job_id"], "before": status}
            try:
                if status in ("prepared", "failed"):
                    entry["action"] = "kept"
                    entry["note"] = "任务尚未进入提交阶段，可直接提交或中止"
                    report["jobs"].append(entry)
                    continue
                committed = _find_commit_for_job(store, catalog, job["job_id"])
                if committed is not None:
                    job["status"] = "committed"
                    job["result"] = {"snapshot": committed.id,
                                     "disposition": committed.operation.get("disposition", "clean"),
                                     "output": committed.operation.get("output")}
                    job["error"] = None
                    save_job(store.root, job)
                    entry.update(action="marked_committed", snapshot=committed.id)
                    report["actions"].append({"action": "mark_job_committed", "job_id": job["job_id"],
                                              "snapshot": committed.id})
                elif auto_commit:
                    res = commit_merge(store, job["job_id"])
                    entry.update(action="recommitted", snapshot=res.get("snapshot", {}).get("id")
                                 if isinstance(res.get("snapshot"), dict) else res.get("snapshot"))
                    report["actions"].append({"action": "recommit_job", "job_id": job["job_id"],
                                              "changed": res.get("changed")})
                    # Refresh catalog after a successful commit.
                    catalog = load_catalog(store.root)
                else:
                    job["status"] = "failed"
                    job["error"] = "崩溃后未自动提交，等待人工重试或中止"
                    save_job(store.root, job)
                    entry.update(action="left_failed")
                    report["actions"].append({"action": "leave_failed", "job_id": job["job_id"]})
            except Exception as exc:  # keep recovering the rest
                entry["error"] = str(exc)
                report["errors"].append({"job_id": job["job_id"], "error": str(exc)})
                try:
                    job = load_job(store.root, job["job_id"])
                    job["status"] = "failed"
                    job["error"] = f"恢复失败: {exc}"
                    save_job(store.root, job)
                except Exception:
                    pass
            report["jobs"].append(entry)

        # Orphan staging directories (no live job).
        live = {j["job_id"] for j in list_jobs(store.root) if j.get("status") in ACTIVE_STATUSES}
        staging_root = os.path.join(store.root, STAGING_DIR)
        if os.path.isdir(staging_root):
            for name in sorted(os.listdir(staging_root)):
                path = os.path.join(staging_root, name)
                if not os.path.isdir(path) or name in live or name.startswith("."):
                    continue
                shutil.rmtree(path, ignore_errors=True)
                report["actions"].append({"action": "remove_staging", "path": f"{STAGING_DIR}/{name}"})
        report["ok"] = not report["errors"]
        return report


def _retained_ids(catalog) -> set[str]:
    return {s.id for s in catalog.retained()}


def _live_output_paths(root: str) -> set[str]:
    """Data files a live merge job has published but not yet committed.

    GC must never reclaim these: the job's ``_ensure_output`` step may reuse
    the published file, and a concurrent recover() may still commit it.
    """
    return {
        j.get("output", {}).get("final_path")
        for j in list_jobs(root)
        if j.get("status") in ACTIVE_STATUSES and j.get("output")
    }


def _unreferenced_cleanup(root: str, keep_files: set[str]) -> list[str]:
    """Delete data files not referenced by any retained snapshot or live job.

    Recovers the crash window where a merged file was published but the
    catalog swap never happened (or was rolled back).  Never touches files a
    live job still needs.
    """
    live_outputs = _live_output_paths(root)
    removed = []
    data_dir = os.path.join(root, DATA_DIR)
    if not os.path.isdir(data_dir):
        return removed
    for name in sorted(os.listdir(data_dir)):
        if not name.endswith(".parquet") or name.startswith("."):
            continue
        rel = f"{DATA_DIR}/{name}"
        if rel in keep_files or rel in live_outputs:
            continue
        os.unlink(os.path.join(data_dir, name))
        removed.append(rel)
    return removed


def collect_garbage(store, *, keep_snapshots: int = 0, dry_run: bool = True,
                    delete: bool = False) -> dict:
    """Compute (and optionally apply) the safe reclaim set.

    ``keep_snapshots`` newest retained snapshots are protected on top of the
    head (the head is always protected).  Only files unreachable from **all**
    protected snapshots are candidates; files referenced by any older snapshot
    that is itself still retained are never candidates.
    """
    if keep_snapshots < 0:
        raise Conflict("keep_snapshots 不能为负数")
    with store._mutex, store._lock.held(shared=dry_run):
        catalog = load_catalog(store.root)
        if catalog.head is None:
            raise NotFound("湖表还没有快照")
        retained = catalog.retained()
        if keep_snapshots > len(retained):
            keep_snapshots = len(retained)
        if keep_snapshots:
            # Keep the newest N snapshots *and* the head, which may already be
            # expired only in pathological states.
            retained = retained[-keep_snapshots:]
        keep_ids = {s.id for s in retained}
        if catalog.head:
            keep_ids.add(catalog.head)
            head_summary = catalog.summary(catalog.head)
            if head_summary is not None:
                head_summary.expired = False
        if dry_run:
            # Non-destructive preview: simulate keeping the newest N snapshots
            # and compute what *would* become unreachable, without touching
            # the catalog.  Currently-expired snapshots stay expired.
            planned = [s for s in catalog.snapshots if not s.expired]
            if keep_snapshots:
                planned = planned[-keep_snapshots:]
            planned_ids = {s.id for s in planned}
            if catalog.head:
                planned_ids.add(catalog.head)
            reachable_files: set[str] = set()
            reachable_deletes: set[str] = set()
            reachable_manifests: set[str] = set()
            would_expire = []
            for s in catalog.snapshots:
                if s.expired or s.id not in planned_ids:
                    if not s.expired:
                        would_expire.append(s.id)
                    continue
                try:
                    manifest = load_manifest(store.root, s.id)
                except CorruptState:
                    continue
                reachable_manifests.add(f"{SNAPSHOT_DIR}/{s.id}.json")
                for f in manifest.get("files", []):
                    reachable_files.add(f["path"])
                for refs in (manifest.get("deletes") or {}).values():
                    for ref in refs:
                        reachable_deletes.add(ref["path"])
            plan = _plan(store, reachable_files, reachable_deletes, reachable_manifests,
                         keep_snapshots=keep_snapshots, scope="planned",
                         protected_files=_live_output_paths(store.root))
            plan["would_expire"] = would_expire
            return plan
        # Destructive GC: first expire snapshots not in keep set, then compute
        # reachability over the remaining ones, delete, save catalog last.
        expired_now = []
        for s in catalog.snapshots:
            if not s.expired and s.id not in keep_ids:
                s.expired = True
                expired_now.append(s.id)
        reachable_files: set[str] = set()
        reachable_deletes: set[str] = set()
        reachable_manifests: set[str] = set()
        for s in catalog.retained():
            manifest = load_manifest(store.root, s.id)
            reachable_manifests.add(f"{SNAPSHOT_DIR}/{s.id}.json")
            for f in manifest.get("files", []):
                reachable_files.add(f["path"])
            for refs in (manifest.get("deletes") or {}).values():
                for ref in refs:
                    reachable_deletes.add(ref["path"])
        plan = _plan(store, reachable_files, reachable_deletes, reachable_manifests,
                     keep_snapshots=keep_snapshots, scope="retained",
                     protected_files=_live_output_paths(store.root))
        total_bytes = plan["bytes"]
        removed = []
        for rel in plan["files"] + plan["delete_sets"] + plan["manifests"]:
            path = os.path.join(store.root, rel)
            try:
                os.unlink(path)
                removed.append(rel)
            except FileNotFoundError:
                removed.append(rel)
            except OSError as exc:
                plan.setdefault("errors", []).append({"path": rel, "error": str(exc)})
        # Delete unreferenced data files (crash leftovers or expired files not
        # already in the scan list), but never a live job's pending output.
        for rel in _unreferenced_cleanup(store.root, reachable_files):
            if rel not in removed:
                removed.append(rel)
        for sub in (DATA_DIR, DELETES_DIR, SNAPSHOT_DIR):
            fsync_dir(os.path.join(store.root, sub))
        # Keep expired snapshot records in the catalog (expired=True) so the
        # history stays visible; their manifests/files are already unlinked.
        save_catalog(store.root, catalog)
        plan.update({"dry_run": False, "removed": removed,
                     "expired_now": expired_now,
                     "planned_bytes": total_bytes,
                     "reclaimed_bytes": sum(os.path.getsize(os.path.join(store.root, r))
                                            for r in removed
                                            if os.path.exists(os.path.join(store.root, r)))})
        # Measure reclaim from the plan's own estimate (files were unlinked
        # before we could stat them).
        plan["reclaimed_bytes"] = total_bytes
        return plan


def _plan(store, reachable_files: set[str], reachable_deletes: set[str],
          reachable_manifests: set[str], *, keep_snapshots: int, scope: str,
          protected_files: set[str] = frozenset()) -> dict:
    """List not-reachable immutable files.

    ``protected_files`` are excluded from the *reclaim* candidates as well as
    from reachability: a live job's published-but-uncommitted output is not
    reachable from any snapshot, but deleting it would break that job.
    """
    def scan(directory: str, prefix: str, suffix: str, reachable: set[str]) -> tuple[list[str], int]:
        out = []
        total = 0
        base = os.path.join(store.root, directory)
        if not os.path.isdir(base):
            return out, total
        for name in sorted(os.listdir(base)):
            if not name.startswith(prefix) or not name.endswith(suffix):
                continue
            rel = f"{directory}/{name}"
            if rel in reachable or rel in protected_files:
                continue
            out.append(rel)
            try:
                total += os.path.getsize(os.path.join(base, name))
            except OSError:
                pass
        return out, total

    files, file_bytes = scan(DATA_DIR, DATA_PREFIX, ".parquet", reachable_files)
    deletes, delete_bytes = scan(DELETES_DIR, DELETE_PREFIX, ".json", reachable_deletes)
    manifests, manifest_bytes = scan(SNAPSHOT_DIR, "v", ".json", reachable_manifests)
    # Also list orphaned files from interrupted staging writes directly under data.
    return {
        "dry_run": True,
        "scope": scope,
        "keep_snapshots": keep_snapshots,
        "files": files,
        "delete_sets": deletes,
        "manifests": manifests,
        "count": len(files) + len(deletes) + len(manifests),
        "bytes": file_bytes + delete_bytes + manifest_bytes,
    }


def reclaim_preview(store, *, keep_snapshots: int = 0) -> dict:
    return collect_garbage(store, keep_snapshots=keep_snapshots, dry_run=True)
