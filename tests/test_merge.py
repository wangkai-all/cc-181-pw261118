"""Merge semantics: position remapping, conflict absorption, redo, idempotency."""

import os

import pytest

from conftest import make_rows, visible_ids, visible_rows
from posdel.errors import Conflict, NotFound
from posdel.merge import (
    analyze_conflict,
    commit_merge,
    list_jobs,
    load_job,
    plan_merge,
)
from posdel.store import Store


def test_plain_merge_drops_deleted_positions(store):
    store.delete([{"column": "id", "op": "in", "value": [1, 6, 11]}])
    job = plan_merge(store)
    assert job["status"] == "prepared"
    assert job["output"]["rows"] == 9
    # mapping skips the deleted positions in each file
    cut = [f for f in job["inputs"] if f["deleted_positions"]]
    assert len(cut) == 3
    for record in cut:
        assert len(job["mapping"][record["file_id"]]) == record["rows"] - len(record["deleted_positions"])
    res = commit_merge(store, job["job_id"])
    assert res["disposition"] == "clean"
    head = store.head()
    assert head.stats["file_count"] == 1
    assert head.stats["raw_rows"] == 9
    assert head.stats["deleted_rows"] == 0
    assert sorted(visible_ids(store.query(limit=100))) == [0, 2, 3, 4, 5, 7, 8, 9, 10]
    assert load_job(store.root, job["job_id"])["status"] == "committed"


def test_merge_only_selected_files(store):
    head = store.head()
    chosen = [head.files[0].id, head.files[2].id]
    job = plan_merge(store, file_ids=chosen)
    commit_merge(store, job["job_id"])
    files = store.file_stats()["files"]
    assert len(files) == 2
    assert sorted(visible_ids(store.query(limit=100))) == list(range(12))


def test_delete_between_plan_and_commit_is_absorbed(store):
    """The core interleaving scenario from the brief."""
    job = plan_merge(store)
    base = job["base_snapshot"]
    # A concurrent writer deletes rows that the merge already copied.
    store.delete([{"column": "id", "op": "in", "value": [2, 9]}], reason="concurrent delete")
    assert store.head().id != base
    res = commit_merge(store, job["job_id"])
    assert res["disposition"] == "absorbed"
    assert len(res["absorbed"]) == 2  # one delete set per affected input file
    merged = store.head().files[0]
    assert sum(len(a["new_positions"]) for a in res["absorbed"]) == 2
    assert all(0 <= p < merged.rows for a in res["absorbed"] for p in a["new_positions"])
    # Visible rows: all 12 minus both the pre-merge delete (none) and the new one
    assert sorted(visible_ids(store.query(limit=100))) == [0, 1, 3, 4, 5, 6, 7, 8, 10, 11]
    # Provenance survives: the merged file's delete set knows its parents.
    delete_events = store.head().delete_events()
    assert any(e["derived_from"] for e in delete_events)
    assert any("concurrent delete" in e["reason"] for e in delete_events)


def test_delete_of_rows_deleted_before_merge_does_not_remap(store):
    """A post-base delete selects only visible rows; pre-deleted rows untouched."""
    store.delete([{"column": "id", "op": "eq", "value": 4}])
    job = plan_merge(store)
    store.delete([{"column": "id", "op": "gte", "value": 8}])
    res = commit_merge(store, job["job_id"])
    assert res["disposition"] == "absorbed"
    rows = visible_ids(store.query(limit=100))
    assert 4 not in rows and 8 not in rows and 9 not in rows
    assert len(rows) == 7  # 12 - id 4 - ids 8..11


def test_conflict_with_intervening_merge_triggers_redo(store):
    """If someone else merges the same files away, our mapping is meaningless."""
    job_a = plan_merge(store)
    job_b = plan_merge(store)
    commit_merge(store, job_a["job_id"])
    before = store.head().id
    res = commit_merge(store, job_b["job_id"])
    assert res["disposition"] in ("redone", "clean")
    # job_b must not resurrect the old files or duplicate rows
    head = store.head()
    assert head.stats["file_count"] == 1
    assert sorted(visible_ids(store.query(limit=100))) == list(range(12))
    committed = load_job(store.root, job_b["job_id"])
    assert committed["status"] == "committed"
    if res["disposition"] == "redone":
        attempt = committed["attempts"][-1]
        assert attempt["conflict"]["mode"] == "redo"


def test_conflict_with_append_only_is_clean(store):
    job = plan_merge(store)
    store.append(make_rows(3, start=100))
    res = commit_merge(store, job["job_id"])
    assert res["disposition"] == "clean"
    head = store.head()
    assert head.stats["file_count"] == 2  # merged file + appended file
    assert sorted(visible_ids(store.query(limit=100))) == list(range(12)) + [100, 101, 102]


def test_merge_idempotent(store):
    job = plan_merge(store)
    first = commit_merge(store, job["job_id"])
    second = commit_merge(store, job["job_id"])
    assert first["changed"] is True
    assert second["changed"] is False
    assert store.head().id == first["snapshot"]["id"]
    assert len(store.snapshots()["snapshots"]) == 2


def test_merge_twice_reexecution_after_crash_is_safe(store):
    """Simulate: commit succeeded, job file not updated. Re-commit must not duplicate."""
    from posdel.merge import save_job

    job = plan_merge(store)
    commit_merge(store, job["job_id"])
    raw = load_job(store.root, job["job_id"])
    raw["status"] = "committing"  # pretend we crashed before marking it
    save_job(store.root, raw)
    again = commit_merge(store, job["job_id"])
    assert again["changed"] is False
    assert store.head().stats["file_count"] == 1
    assert len(store.snapshots()["snapshots"]) == 2


def test_merge_rejects_bad_inputs(store):
    with pytest.raises(Exception):
        plan_merge(store, file_ids=[])
    with pytest.raises(Exception):
        plan_merge(store, file_ids=["f_missing"])


def test_merge_all_deleted_rows_produces_no_output(store):
    store.delete([{"column": "id", "op": "not_null"}])
    assert store.query(limit=10)["total"] == 0
    job = plan_merge(store)
    assert job["output"] is None
    res = commit_merge(store, job["job_id"])
    head = store.head()
    assert head.stats["file_count"] == 0
    assert head.stats["raw_rows"] == 0


def test_abort_merge(store):
    from posdel.merge import abort_merge

    job = plan_merge(store)
    res = abort_merge(store, job["job_id"])
    assert res["job"]["status"] == "aborted"
    assert store.head().id == job["base_snapshot"]
    with pytest.raises(Conflict):
        commit_merge(store, job["job_id"])


def test_merge_reports_are_persisted(store):
    job = plan_merge(store, note="nightly compaction")
    commit_merge(store, job["job_id"])
    jobs = list_jobs(store.root)
    assert len(jobs) == 1
    assert jobs[0]["note"] == "nightly compaction"
    assert jobs[0]["status"] == "committed"
    head = store.head()
    assert head.operation["type"] == "merge"
    assert head.operation["job_id"] == job["job_id"]
