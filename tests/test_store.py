"""Core storage semantics: immutable snapshots, position deletes, time travel."""

import os

import pytest

from conftest import make_rows, visible_ids, visible_rows
from posdel.errors import Conflict, NotFound, ValidationError
from posdel.store import Store


def test_init_creates_snapshot_and_files(store):
    head = store.head()
    assert head.seq == 1
    assert head.stats["raw_rows"] == 12
    assert head.stats["visible_rows"] == 12
    assert head.stats["file_count"] == 3  # chunk_rows=4 over 12 rows
    for f in head.files:
        assert os.path.exists(os.path.join(store.root, f.path))
        assert f.rows == 4


def test_positions_are_per_file_and_visible_rows_are_stable(store):
    rows = visible_rows(store)
    assert [r["data"]["id"] for r in rows] == list(range(12))
    # each row carries its file-local position
    positions = [(r["meta"]["file_id"], r["meta"]["position"]) for r in rows]
    assert positions[:4] == [(rows[0]["meta"]["file_id"], i) for i in range(4)]


def test_delete_uses_position_sets_and_keeps_raw_rows(store):
    before = store.head()
    res = store.delete([{"column": "id", "op": "gte", "value": 10}], reason="cleanup")
    assert res["changed"] is True
    assert res["preview"]["would_delete"] == 2
    after = res["snapshot"]
    # data files are unchanged; only delete sets were added
    assert [f["id"] for f in after["files"]] == [f.id for f in before.files]
    assert after["stats"]["raw_rows"] == 12
    assert after["stats"]["deleted_rows"] == 2
    assert after["stats"]["visible_rows"] == 10
    assert sorted(visible_ids(store.query(limit=100))) == list(range(10))
    # raw view still shows all 12 with the deleted flag
    raw = store.query(mode="raw", limit=100)
    assert raw["total"] == 12
    deleted = [r for r in raw["rows"] if r["meta"]["deleted"]]
    assert sorted(r["data"]["id"] for r in deleted) == [10, 11]
    assert all(r["meta"]["delete"]["reason"] == "cleanup" for r in deleted)


def test_old_snapshot_still_queryable(store):
    old = store.head().id
    store.delete([{"column": "id", "op": "eq", "value": 3}])
    store.append(make_rows(1, start=99))
    assert len(visible_rows(store, old)) == 12
    assert len(visible_rows(store)) == 12
    # order in the new version: original 11 + appended row at the end
    assert visible_ids(store.query(limit=100))[-1] == 99


def test_delete_never_repeats_already_deleted_rows(store):
    store.delete([{"column": "id", "op": "lt", "value": 4}])
    second = store.delete([{"column": "id", "op": "lt", "value": 6}])
    assert second["preview"]["would_delete"] == 2  # ids 4,5 only
    assert second["snapshot"]["stats"]["deleted_rows"] == 6  # ids 0..5
    assert second["snapshot"]["stats"]["visible_rows"] == 6


def test_delete_dry_run_writes_nothing(store):
    head = store.head().id
    res = store.delete([{"column": "id", "op": "eq", "value": 1}], dry_run=True)
    assert res["changed"] is False
    assert res["preview"]["would_delete"] == 1
    assert store.head().id == head


def test_delete_with_no_match_is_a_noop(store):
    head = store.head().id
    res = store.delete([{"column": "id", "op": "eq", "value": 12345}])
    assert res["changed"] is False
    assert store.head().id == head


def test_multiple_delete_sets_accumulate(store):
    store.delete([{"column": "id", "op": "eq", "value": 0}])
    store.delete([{"column": "id", "op": "eq", "value": 5}])
    store.delete([{"column": "id", "op": "eq", "value": 11}])
    rows = visible_rows(store)
    assert sorted(r["data"]["id"] for r in rows) == [1, 2, 3, 4, 6, 7, 8, 9, 10]
    file_stats = store.file_stats()["files"]
    assert sum(len(f["delete_sets"]) for f in file_stats) == 3


def test_predicates_types(store):
    assert len(visible_rows(store, conditions=[{"column": "active", "op": "eq", "value": True}])) == 6
    assert len(visible_rows(store, conditions=[{"column": "name", "op": "starts_with", "value": "row-1"}])) == 3
    assert len(visible_rows(store, conditions=[{"column": "score", "op": "between", "value": 2.5, "to": 5.0}])) == 3
    assert len(visible_rows(store, conditions=[{"column": "id", "op": "in", "value": [1, 3, 5]}])) == 3
    two = visible_rows(store, conditions=[
        {"column": "id", "op": "gte", "value": 8},
        {"column": "active", "op": "eq", "value": True},
    ], match="any")
    assert sorted(r["data"]["id"] for r in two) == [0, 2, 4, 6, 8, 9, 10, 11]


def test_predicate_does_not_expose_deleted_rows(store):
    store.delete([{"column": "id", "op": "eq", "value": 2}])
    res = store.query(mode="visible", conditions=[{"column": "id", "op": "lt", "value": 4}], limit=100)
    assert sorted(visible_ids(res)) == [0, 1, 3]
    raw = store.query(mode="raw", conditions=[{"column": "id", "op": "lt", "value": 4}], limit=100)
    assert sorted(visible_ids(raw)) == [0, 1, 2, 3]


def test_append_expected_head_conflict(store):
    old = store.head().id
    store.append(make_rows(1, start=50))
    with pytest.raises(Conflict):
        store.append(make_rows(1, start=51), expected_head=old)


def test_append_validates_rows(store):
    with pytest.raises(ValidationError):
        store.append([{"id": 1, "name": "x", "score": 1.0}])  # missing active
    with pytest.raises(ValidationError):
        store.append([{"id": "not-int", "name": "x", "score": 1.0, "active": True}])
    with pytest.raises(ValidationError):
        store.append([])


def test_file_id_filter(store):
    fid = store.head().files[1].id
    res = store.query(file_id=fid, limit=100)
    assert res["total"] == 4
    assert all(r["meta"]["file_id"] == fid for r in res["rows"])
    with pytest.raises(NotFound):
        store.query(file_id="f_nope")


def test_pagination(store):
    seen = []
    for offset in range(0, 12, 5):
        page = store.query(offset=offset, limit=5)
        seen.extend(visible_ids(page))
        assert page["total"] == 12
    assert seen == list(range(12))


def test_snapshot_list_marks_head_and_expiry(store):
    store.delete([{"column": "id", "op": "eq", "value": 0}])
    listing = store.snapshots()
    assert [s["id"] for s in listing["snapshots"]] == ["v000001", "v000002"]
    assert [s["is_head"] for s in listing["snapshots"]] == [False, True]


def test_known_snapshot_missing(store):
    with pytest.raises(NotFound):
        store.query("v999999")


def test_init_twice_conflicts(store):
    from posdel.errors import Conflict as C

    with pytest.raises(C):
        store.init(store.head().schema)
