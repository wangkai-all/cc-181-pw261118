"""Crash recovery and garbage-collection safety."""

import os
import shutil

import pytest

from conftest import make_rows, visible_ids
from posdel.errors import Conflict, NotFound
from posdel.maintenance import collect_garbage, recover
from posdel.merge import (
    commit_merge,
    load_job,
    plan_merge,
    save_job,
)
from posdel.store import Store


class SimulatedCrash(Exception):
    pass


FAULT_POINTS = [
    "after_output_published",
    "after_delete_sets_persisted",
    "before_catalog_swap",
]


@pytest.mark.parametrize("point", FAULT_POINTS)
def test_recovery_commits_interrupted_merge(store, point):
    job = plan_merge(store)
    visible_before = visible_ids(store.query(limit=100))

    def inject(p, info):
        if p == point:
            raise SimulatedCrash(p)

    with pytest.raises(SimulatedCrash):
        commit_merge(store, job["job_id"], fault_inject=inject)

    # A fresh Store models a new process after the crash.
    reopened = Store(store.root)
    assert visible_ids(reopened.query(limit=100)) == visible_before  # old head intact
    report = recover(reopened)
    assert report["ok"] is True
    assert load_job(reopened.root, job["job_id"])["status"] == "committed"
    head = reopened.head()
    assert head.stats["raw_rows"] == 12
    assert head.stats["file_count"] == 1
    assert visible_ids(reopened.query(limit=100)) == visible_before
    assert reopened.verify(full=True)["ok"]
    # Staging is cleaned.
    assert not os.path.exists(os.path.join(reopened.root, "staging", job["job_id"]))


def test_recovery_after_catalog_swap_marks_job_only(store):
    job = plan_merge(store)

    def inject(p, info):
        if p == "after_catalog_swap":
            raise SimulatedCrash(p)

    with pytest.raises(SimulatedCrash):
        commit_merge(store, job["job_id"], fault_inject=inject)

    reopened = Store(store.root)
    before_heads = len(reopened.snapshots()["snapshots"])
    report = recover(reopened)
    actions = [a["action"] for a in report["actions"]]
    assert "mark_job_committed" in actions
    # No duplicate snapshot was created.
    assert len(reopened.snapshots()["snapshots"]) == before_heads
    assert load_job(reopened.root, job["job_id"])["status"] == "committed"
    assert len(visible_ids(reopened.query(limit=100))) == 12


def test_recovery_is_idempotent(store):
    job = plan_merge(store)
    commit_merge(store, job["job_id"])
    first = recover(Store(store.root))
    second = recover(Store(store.root))
    # First pass may clean staging; a second pass must find nothing to do.
    assert second["actions"] == []
    assert len(Store(store.root).snapshots()["snapshots"]) == 2


def test_recovery_removes_orphan_staging(store):
    job = plan_merge(store)
    orphan = os.path.join(store.root, "staging", "j_orphan")
    os.makedirs(orphan, exist_ok=True)
    with open(os.path.join(orphan, "merged.parquet"), "wb") as fh:
        fh.write(b"junk")
    report = recover(Store(store.root))
    assert any(a.get("path", "").endswith("j_orphan") for a in report["actions"])
    assert not os.path.exists(orphan)


def test_recovery_leaves_prepared_job_alone(store):
    job = plan_merge(store)
    report = recover(Store(store.root))
    assert [j["action"] for j in report["jobs"]] == ["kept"]
    assert load_job(store.root, job["job_id"])["status"] == "prepared"
    # Staging belonging to a live prepared job is preserved.
    assert os.path.isdir(os.path.join(store.root, "staging", job["job_id"]))


def test_gc_preview_is_non_destructive_and_matches_apply(store):
    store.delete([{"column": "id", "op": "eq", "value": 2}])
    job = plan_merge(store)
    commit_merge(store, job["job_id"])
    store.append(make_rows(2, start=200))

    preview = collect_garbage(store, keep_snapshots=1, dry_run=True)
    assert preview["dry_run"] is True
    assert preview["count"] > 0
    # Preview did not expire or delete anything.
    listing = store.snapshots()
    assert not any(s["expired"] for s in listing["snapshots"])

    applied = collect_garbage(store, keep_snapshots=1, dry_run=False)
    assert applied["dry_run"] is False
    assert set(applied["removed"]) == set(preview["files"] + preview["delete_sets"] + preview["manifests"])
    assert set(applied["expired_now"]) == set(preview["would_expire"])


def test_gc_keeps_head_snapshot_readable(store):
    store.delete([{"column": "id", "op": "eq", "value": 1}])
    job = plan_merge(store)
    commit_merge(store, job["job_id"])
    before_ids = visible_ids(store.query(limit=100))
    collect_garbage(store, keep_snapshots=1, dry_run=False)
    after_ids = visible_ids(store.query(limit=100))
    assert before_ids == after_ids
    assert store.verify(full=True)["ok"]
    with pytest.raises(Conflict):
        store.query("v000001")
    # Raw data of the head is still complete.
    assert store.query(mode="raw", limit=100)["total"] == 11


def test_gc_never_removes_files_of_kept_snapshots(store):
    store.delete([{"column": "id", "op": "eq", "value": 0}])
    keep = store.head().id
    store.append(make_rows(1, start=300))
    job = plan_merge(store)
    commit_merge(store, job["job_id"])
    # v1 init, v2 delete (kept), v3 append, v4 merge → keeping 3 protects v2.
    collect_garbage(store, keep_snapshots=3, dry_run=False)
    listing = {s["id"]: s for s in store.snapshots()["snapshots"]}
    assert listing[keep]["expired"] is False
    assert len(visible_ids(store.query(keep, limit=100))) == 11


def test_gc_refuses_when_keep_exceeds_snapshots(store):
    store.append(make_rows(1, start=400))
    plan = collect_garbage(store, keep_snapshots=99, dry_run=True)
    assert plan["count"] == 0


def test_gc_never_reclaims_live_job_output(store):
    """A published-but-uncommitted merge output is not reachable from any
    snapshot, but GC must leave it alone: recovery still needs to commit it.
    """
    job = plan_merge(store)

    def inject(point, info):
        if point == "after_output_published":
            raise SimulatedCrash(point)

    with pytest.raises(SimulatedCrash):
        commit_merge(store, job["job_id"], fault_inject=inject)
    live = load_job(store.root, job["job_id"])
    assert live["status"] == "committing"
    out = live["output"]["final_path"]

    preview = collect_garbage(store, keep_snapshots=1, dry_run=True)
    assert out not in preview["files"]
    applied = collect_garbage(store, keep_snapshots=1, dry_run=False)
    assert out not in applied["removed"]
    assert os.path.exists(os.path.join(store.root, out))

    # Recovery can still commit the job from the preserved output.
    before = visible_ids(store.query(limit=100))
    report = recover(Store(store.root))
    assert report["ok"] is True
    assert load_job(store.root, job["job_id"])["status"] == "committed"
    assert visible_ids(store.query(limit=100)) == before


def test_verify_detects_tampered_data_file(store):
    file = store.head().files[0]
    path = os.path.join(store.root, file.path)
    with open(path, "ab") as fh:
        fh.write(b"corruption")
    result = store.verify(full=True)
    assert result["ok"] is False
    assert any("哈希" in issue or "校验" in issue for issue in result["issues"])


def test_verify_detects_missing_file(store):
    file = store.head().files[0]
    os.unlink(os.path.join(store.root, file.path))
    result = store.verify(full=False)
    assert result["ok"] is False
    assert result["issues"]
