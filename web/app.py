"""FastAPI server for the posdel lake-table workbench.

Run locally::

    .venv/bin/python -m uvicorn web.app:app --port 8765
    # or: .venv/bin/python scripts/run_server.py

The store directory comes from ``POSDEL_ROOT`` (default ``var/demo-lake``).
A single process serves one store; the browser is the only client.
"""

from __future__ import annotations

import os
import shutil
import sys
from dataclasses import asdict
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

from posdel.errors import (  # noqa: E402
    Conflict,
    CorruptState,
    NotFound,
    PosdelError,
    ValidationError,
)
from posdel.files import verify_data_file  # noqa: E402
from posdel.maintenance import collect_garbage, recover  # noqa: E402
from posdel.merge import (  # noqa: E402
    abort_merge,
    commit_merge,
    list_jobs,
    load_job,
    plan_merge,
)
from posdel.schema import build_conditions, parse_schema  # noqa: E402
from posdel.store import Store  # noqa: E402

WEB_DIR = os.path.join(ROOT_DIR, "web")
STORE_ROOT = os.path.abspath(os.environ.get("POSDEL_ROOT", os.path.join(ROOT_DIR, "var", "demo-lake")))
DEMO_MODE = os.environ.get("POSDEL_DEMO", "0") == "1"

app = FastAPI(title="posdel 湖表工作台", docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory=os.path.join(WEB_DIR, "static")), name="static")


def store() -> Store:
    return Store(STORE_ROOT)


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

@app.exception_handler(PosdelError)
async def posdel_error_handler(request: Request, exc: PosdelError):
    return JSONResponse(status_code=exc.status, content=exc.to_dict())


@app.exception_handler(ValueError)
async def value_error_handler(request: Request, exc: ValueError):
    return JSONResponse(status_code=422, content={"error": "bad_request", "message": str(exc)})


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

class InitBody(BaseModel):
    fields: list[dict]
    rows: list[dict] = Field(default_factory=list)
    chunk_rows: int = 1024


class AppendBody(BaseModel):
    rows: list[dict]
    expected_head: str | None = None
    note: str | None = None


class DeleteBody(BaseModel):
    conditions: list[dict] = Field(default_factory=list)
    match: str = "all"
    reason: str = ""
    dry_run: bool = False
    expected_head: str | None = None


class QueryBody(BaseModel):
    snapshot: str | None = None
    mode: str = "visible"
    conditions: list[dict] = Field(default_factory=list)
    match: str = "all"
    file_id: str | None = None
    offset: int = 0
    limit: int = 50


class PlanMergeBody(BaseModel):
    file_ids: list[str] | None = None
    base_snapshot: str | None = None
    note: str = ""


class CommitMergeBody(BaseModel):
    job_id: str
    crash_at: str | None = None   # demo hook: raise at a named fault point


class AbortMergeBody(BaseModel):
    job_id: str
    reason: str = ""


class GcApplyBody(BaseModel):
    keep_snapshots: int = 0
    confirm: bool = False


class CrashBody(BaseModel):
    job_id: str
    point: str = "before_catalog_swap"


# ---------------------------------------------------------------------------
# State & snapshots
# ---------------------------------------------------------------------------

def _ensure_store_exists() -> Store:
    s = store()
    if not s.exists():
        raise NotFound("湖表尚未初始化。请先在页面点击“初始化示例湖表”。")
    return s


@app.get("/api/health")
def health():
    return {"ok": True, "root": STORE_ROOT, "demo_mode": DEMO_MODE,
            "format": "posdel/1", "exists": os.path.exists(os.path.join(STORE_ROOT, "posdel.json"))}


@app.get("/api/state")
def api_state():
    exists = os.path.exists(os.path.join(STORE_ROOT, "posdel.json"))
    data: dict[str, Any] = {"root": STORE_ROOT, "exists": exists, "demo_mode": DEMO_MODE}
    if not exists:
        return data
    s = store()
    listing = s.snapshots(include_expired=True)
    head = s.head()
    data.update(
        {
            "format": listing["format"],
            "head": listing["head"],
            "schema": listing["schema"],
            "snapshots": listing["snapshots"],
            "head_detail": head.to_api(detail=False),
            "jobs": [summarize_job(j) for j in list_jobs(STORE_ROOT)],
        }
    )
    return data


def summarize_job(job: dict) -> dict:
    return {
        "job_id": job.get("job_id"),
        "status": job.get("status"),
        "created_at": job.get("created_at"),
        "updated_at": job.get("updated_at"),
        "note": job.get("note", ""),
        "base_snapshot": job.get("base_snapshot"),
        "input_file_ids": job.get("input_file_ids"),
        "output_rows": (job.get("output") or {}).get("rows"),
        "attempts": job.get("attempts", []),
        "result": job.get("result"),
        "error": job.get("error"),
        "kind": job.get("kind"),
    }


@app.get("/api/snapshots")
def api_snapshots():
    return _ensure_store_exists().snapshots(include_expired=True)


@app.get("/api/snapshot/{sid}")
def api_snapshot(sid: str, allow_expired: bool = False):
    return _ensure_store_exists().snapshot(sid, allow_expired=allow_expired).to_api(detail=True)


@app.get("/api/files")
def api_files(snapshot: str | None = None):
    return _ensure_store_exists().file_stats(snapshot)


@app.get("/api/condition-ops")
def api_condition_ops():
    return {
        "ops": [
            {"op": "eq", "label": "等于", "value": "scalar"},
            {"op": "ne", "label": "不等于", "value": "scalar"},
            {"op": "lt", "label": "小于", "value": "scalar"},
            {"op": "lte", "label": "小于等于", "value": "scalar"},
            {"op": "gt", "label": "大于", "value": "scalar"},
            {"op": "gte", "label": "大于等于", "value": "scalar"},
            {"op": "between", "label": "区间", "value": "range"},
            {"op": "in", "label": "属于集合", "value": "list"},
            {"op": "not_in", "label": "不属于集合", "value": "list"},
            {"op": "contains", "label": "包含子串", "value": "text"},
            {"op": "starts_with", "label": "前缀", "value": "text"},
            {"op": "ends_with", "label": "后缀", "value": "text"},
            {"op": "is_null", "label": "为空", "value": "none"},
            {"op": "not_null", "label": "非空", "value": "none"},
        ]
    }


@app.post("/api/query")
def api_query(body: QueryBody):
    s = _ensure_store_exists()
    return s.query(
        body.snapshot,
        mode=body.mode,
        conditions=body.conditions,
        match=body.match,
        file_id=body.file_id,
        offset=body.offset,
        limit=body.limit,
    )


# ---------------------------------------------------------------------------
# Mutations
# ---------------------------------------------------------------------------

@app.post("/api/append")
def api_append(body: AppendBody):
    s = _ensure_store_exists()
    return s.append(body.rows, expected_head=body.expected_head, summary_note=body.note)


@app.post("/api/delete")
def api_delete(body: DeleteBody):
    s = _ensure_store_exists()
    return s.delete(
        body.conditions,
        match=body.match,
        reason=body.reason,
        expected_head=body.expected_head,
        dry_run=body.dry_run,
    )


@app.post("/api/init")
def api_init(body: InitBody):
    s = store()
    if s.exists():
        raise Conflict("该目录已有湖表，不允许覆盖初始化")
    schema = parse_schema(body.fields)
    return {"changed": True, "snapshot": s.init(schema, body.rows, chunk_rows=body.chunk_rows).to_api()}


# ---------------------------------------------------------------------------
# Merge jobs
# ---------------------------------------------------------------------------

@app.get("/api/jobs")
def api_jobs():
    return {"jobs": list_jobs(STORE_ROOT)}


@app.get("/api/jobs/{job_id}")
def api_job(job_id: str):
    return load_job(STORE_ROOT, job_id)


@app.post("/api/merge/plan")
def api_merge_plan(body: PlanMergeBody):
    s = _ensure_store_exists()
    job = plan_merge(s, base_snapshot=body.base_snapshot, file_ids=body.file_ids, note=body.note)
    return {"changed": True, "job": job, "summary": summarize_job(job)}


def _fault_injector(point: str | None):
    if point is None:
        return None

    class Injected(Exception):
        pass

    def inject(name: str, info: dict):
        if name == point:
            raise Injected(f"故障注入：在 {name} 处中断提交")

    return inject


@app.post("/api/merge/commit")
def api_merge_commit(body: CommitMergeBody):
    s = _ensure_store_exists()
    try:
        return commit_merge(s, body.job_id, fault_inject=_fault_injector(body.crash_at))
    except Exception as exc:
        if type(exc).__name__ == "Injected":
            return JSONResponse(
                status_code=409,
                content={
                    "error": "injected_failure",
                    "message": str(exc),
                    "details": {"job_id": body.job_id, "crash_at": body.crash_at,
                                "head": _safe_head()},
                },
            )
        raise


def _safe_head() -> str | None:
    try:
        return store().head().id
    except Exception:
        return None


@app.post("/api/merge/abort")
def api_merge_abort(body: AbortMergeBody):
    return abort_merge(_ensure_store_exists(), body.job_id, reason=body.reason)


@app.post("/api/recover")
def api_recover():
    s = store()
    if not s.exists():
        raise NotFound("湖表不存在")
    return recover(s)


# ---------------------------------------------------------------------------
# Reclaim / verify
# ---------------------------------------------------------------------------

@app.get("/api/gc/preview")
def api_gc_preview(keep_snapshots: int = 0):
    return collect_garbage(_ensure_store_exists(), keep_snapshots=keep_snapshots, dry_run=True)


@app.post("/api/gc/apply")
def api_gc_apply(body: GcApplyBody):
    if not body.confirm:
        raise ValidationError("回收会永久删除不可达文件，需要 confirm=true")
    return collect_garbage(_ensure_store_exists(), keep_snapshots=body.keep_snapshots, dry_run=False)


@app.get("/api/verify")
def api_verify(full: bool = False, snapshot: str | None = None):
    return _ensure_store_exists().verify(snapshot, full=full)


# ---------------------------------------------------------------------------
# Demo helpers (only when POSDEL_DEMO=1)
# ---------------------------------------------------------------------------

DEMO_FIELDS = [
    {"name": "id", "type": "int64"},
    {"name": "city", "type": "string"},
    {"name": "temp_c", "type": "float64"},
    {"name": "ok", "type": "bool"},
]


def demo_rows(n: int = 24, start: int = 1) -> list[dict]:
    cities = ["北京", "上海", "广州", "深圳", "成都", "杭州"]
    return [
        {
            "id": start + i,
            "city": cities[i % len(cities)],
            "temp_c": round(12.5 + ((i * 7) % 23) * 0.75, 2),
            "ok": i % 5 != 0,
        }
        for i in range(n)
    ]


@app.post("/api/demo/reset")
def api_demo_reset(seed: bool = True):
    if not DEMO_MODE:
        raise Conflict("未启用演示模式（设置 POSDEL_DEMO=1 启动服务后可重置示例）")
    import json

    with store()._mutex:
        if os.path.isdir(STORE_ROOT):
            shutil.rmtree(STORE_ROOT, ignore_errors=True)
    s = Store(STORE_ROOT)
    schema = parse_schema(DEMO_FIELDS)
    snapshot = s.init(schema, demo_rows(24) if seed else [], chunk_rows=8)
    return {"changed": True, "snapshot": snapshot.to_api(), "root": STORE_ROOT}


# ---------------------------------------------------------------------------
# Frontend
# ---------------------------------------------------------------------------

@app.get("/")
def index():
    return FileResponse(os.path.join(WEB_DIR, "static", "index.html"))


@app.get("/favicon.ico")
def favicon():
    return JSONResponse(status_code=204, content=None)
