"""API-level tests for the web layer (no browser required)."""

import os

import pytest

fastapi = pytest.importorskip("fastapi")

from fastapi.testclient import TestClient  # noqa: E402

from posdel.store import Store  # noqa: E402


@pytest.fixture
def client(tmp_path, monkeypatch):
    root = tmp_path / "web-lake"
    monkeypatch.setenv("POSDEL_ROOT", str(root))
    monkeypatch.setenv("POSDEL_DEMO", "1")
    import web.app as app_module

    app_module.STORE_ROOT = str(root)
    app_module.DEMO_MODE = True
    with TestClient(app_module.app) as c:
        yield c


def test_health_before_init(client):
    res = client.get("/api/health").json()
    assert res["exists"] is False
    assert res["format"] == "posdel/1"
    state = client.get("/api/state").json()
    assert state["exists"] is False


def test_demo_reset_and_query(client):
    res = client.post("/api/demo/reset")
    assert res.status_code == 200
    snapshot = res.json()["snapshot"]
    assert snapshot["stats"]["raw_rows"] == 24
    assert len(snapshot["files"]) == 3

    q = client.post("/api/query", json={"limit": 10}).json()
    assert q["total"] == 24
    assert len(q["rows"]) == 10
    assert q["rows"][0]["meta"]["position"] == 0
    assert q["rows"][0]["meta"]["visible_no"] == 0


def test_delete_preview_then_apply(client):
    client.post("/api/demo/reset")
    head = client.get("/api/state").json()["head"]
    body = {
        "conditions": [{"column": "temp_c", "op": "gt", "value": 20}],
        "reason": "api test",
        "dry_run": True,
        "expected_head": head,
    }
    preview = client.post("/api/delete", json=body)
    assert preview.status_code == 200
    pdata = preview.json()
    assert pdata["changed"] is False
    assert pdata["preview"]["would_delete"] > 0

    body["dry_run"] = False
    applied = client.post("/api/delete", json=body).json()
    assert applied["changed"] is True
    assert applied["snapshot"]["stats"]["deleted_rows"] == pdata["preview"]["would_delete"]
    # Raw view exposes deleted provenance.
    raw = client.post("/api/query", json={"mode": "raw", "limit": 100}).json()
    deleted = [r for r in raw["rows"] if r["meta"]["deleted"]]
    assert len(deleted) == pdata["preview"]["would_delete"]
    assert all(r["meta"]["delete"]["reason"] == "api test" for r in deleted)


def test_stale_expected_head_returns_409(client):
    client.post("/api/demo/reset")
    stale = client.get("/api/state").json()["head"]
    client.post("/api/append", json={"rows": [
        {"id": 500, "city": "X", "temp_c": 1.0, "ok": True},
    ]})
    res = client.post("/api/delete", json={
        "conditions": [{"column": "id", "op": "eq", "value": 1}],
        "expected_head": stale,
    })
    assert res.status_code == 409
    assert res.json()["error"] == "conflict"


def test_merge_plan_commit_and_conflict_absorption(client):
    client.post("/api/demo/reset")
    client.post("/api/delete", json={"conditions": [{"column": "id", "op": "eq", "value": 2}]})
    plan = client.post("/api/merge/plan", json={"note": "web"}).json()
    job = plan["job"]
    assert job["status"] == "prepared"
    # interleave a delete
    client.post("/api/delete", json={"conditions": [{"column": "id", "op": "eq", "value": 5}]})
    commit = client.post("/api/merge/commit", json={"job_id": job["job_id"]}).json()
    assert commit["disposition"] == "absorbed"
    assert commit["absorbed"]
    visible = client.post("/api/query", json={"limit": 100}).json()
    ids = {r["data"]["id"] for r in visible["rows"]}
    assert 2 not in ids and 5 not in ids
    assert visible["total"] == 22
    raw = client.post("/api/query", json={"mode": "raw", "limit": 100}).json()
    # The rebuild used the pre-interleave deletes (id=2 physically dropped);
    # the later delete (id=5) was absorbed as a remapped position set, so the
    # merged file still holds that raw row and marks it deleted.
    assert raw["total"] == 23
    assert sum(1 for r in raw["rows"] if r["meta"]["deleted"]) == 1
    jobs = client.get("/api/jobs").json()["jobs"]
    assert jobs[0]["status"] == "committed"


def test_injected_crash_then_recover(client):
    client.post("/api/demo/reset")
    plan = client.post("/api/merge/plan", json={}).json()
    job_id = plan["job"]["job_id"]
    crashed = client.post("/api/merge/commit", json={
        "job_id": job_id, "crash_at": "before_catalog_swap",
    })
    assert crashed.status_code == 409
    assert crashed.json()["error"] == "injected_failure"
    state = client.get("/api/state").json()
    assert state["snapshots"][-1]["op"] in ("init", "delete")
    rec = client.post("/api/recover").json()
    assert any(a["action"] == "recommit_job" for a in rec["actions"])
    jobs = client.get("/api/jobs").json()["jobs"]
    assert jobs[0]["status"] == "committed"


def test_gc_requires_confirm_and_preview_matches(client):
    client.post("/api/demo/reset")
    client.post("/api/merge/plan", json={}).json()
    jobs = client.get("/api/jobs").json()["jobs"]
    client.post("/api/merge/commit", json={"job_id": jobs[0]["job_id"]})
    prev = client.get("/api/gc/preview?keep_snapshots=1").json()
    assert prev["dry_run"] is True
    bad = client.post("/api/gc/apply", json={"keep_snapshots": 1})
    assert bad.status_code == 422
    ok = client.post("/api/gc/apply", json={"keep_snapshots": 1, "confirm": True}).json()
    assert ok["removed"]
    assert set(ok["removed"]) == set(prev["files"] + prev["delete_sets"] + prev["manifests"])
    verify = client.get("/api/verify?full=true").json()
    assert verify["ok"] is True


def test_validate_errors_are_json(client):
    client.post("/api/demo/reset")
    res = client.post("/api/append", json={"rows": [{"id": "x"}]})
    assert res.status_code == 422
    payload = res.json()
    assert payload["error"] == "validation_error"
    assert "message" in payload


def test_static_assets_served(client):
    assert client.get("/").status_code == 200
    assert "query-mode" in client.get("/static/app.js").text
    assert client.get("/static/app.css").status_code == 200
