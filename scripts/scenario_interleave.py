#!/usr/bin/env python3
"""End-to-end local scenario: merge and delete interleave, with a crash in the middle.

Run::

    .venv/bin/python scripts/scenario_interleave.py [--root var/scenario-lake]

The script prints every step and asserts the invariants at the end.  It is the
executable version of the story in docs/SCENARIOS.md.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from posdel.maintenance import collect_garbage, recover  # noqa: E402
from posdel.merge import commit_merge, load_job, plan_merge  # noqa: E402
from posdel.schema import parse_schema  # noqa: E402
from posdel.store import Store  # noqa: E402


class InjectedCrash(Exception):
    pass


def banner(step: str) -> None:
    print(f"\n=== {step} " + "=" * max(0, 66 - len(step)))


def visible(store: Store, sid: str | None = None) -> list[dict]:
    out, offset = [], 0
    while True:
        page = store.query(sid, mode="visible", offset=offset, limit=1000)
        out.extend(page["rows"])
        offset += len(page["rows"])
        if offset >= page["total"]:
            return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=os.path.join(ROOT, "var", "scenario-lake"))
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    root = os.path.abspath(args.root)
    if os.path.exists(root):
        if not args.force:
            print(f"目录已存在：{root}；加 --force 重建", file=sys.stderr)
            return 2
        shutil.rmtree(root)

    schema = parse_schema([
        {"name": "id", "type": "int64"},
        {"name": "city", "type": "string"},
        {"name": "temp_c", "type": "float64"},
        {"name": "ok", "type": "bool"},
    ])
    store = Store(root)
    rows = [
        {"id": i, "city": ["北京", "上海", "广州", "深圳"][i % 4],
         "temp_c": 10.0 + (i % 15), "ok": i % 6 != 0}
        for i in range(24)
    ]

    banner("1. 初始化：24 行写入 3 个不可变 Parquet 文件")
    snap = store.init(schema, rows, chunk_rows=8)
    print(f"快照 {snap.id}  {snap.stats}")

    banner("2. 条件删除：temp_c > 20 的行 → 独立位置集合")
    res = store.delete([{"column": "temp_c", "op": "gt", "value": 20}], reason="异常高温")
    print(f"快照 {res['snapshot']['id']}  {res['snapshot']['stats']}")
    base_ids = [f["id"] for f in res["snapshot"]["files"]]

    banner("3. 准备合并任务（基线 = 当前 head，输入 3 个文件）")
    job = plan_merge(store, note="场景演示合并")
    print(f"任务 {job['job_id']}  基线 {job['base_snapshot']}  输入 {len(job['input_file_ids'])} 文件")
    print(f"映射偏移（file → merged 起点）：{job['mapping_offsets']}")

    banner("4. 合并尚未提交时，另一个写入者又删除了一批行")
    concurrent = store.delete(
        [{"column": "temp_c", "op": "lt", "value": 12}],
        reason="并发删除：低温记录",
    )
    print(f"并发快照 {concurrent['snapshot']['id']}  {concurrent['snapshot']['stats']}")
    pre_delete_visible = {r["data"]["id"] for r in visible(store)}

    banner("5. 提交合并：基线已变化 → 检测冲突并通过位置映射吸收新删除集")
    result = commit_merge(store, job["job_id"])
    print(f"处置方式: {result['disposition']}")
    for item in result["absorbed"]:
        print(f"  删除集 {item['from_delete_set']} 自文件 {item['from_file']}: "
              f"旧位置 {item['old_positions']} → 新位置 {item['new_positions']}")
    merged_snapshot = result["snapshot"]
    print(f"合并后快照 {merged_snapshot['id']}  {merged_snapshot['stats']}")
    assert result["disposition"] == "absorbed"

    banner("6. 校验：删除语义跨合并仍成立")
    post_visible = {r["data"]["id"] for r in visible(store)}
    assert post_visible == pre_delete_visible, "合并改变了可见行集合！"
    print(f"可见行一致：{len(post_visible)} 行")
    raw_rows = store.query(mode="raw", limit=1000)["total"]
    print(f"合并后原始行 = {raw_rows}，删除位置 = {merged_snapshot['stats']['deleted_rows']}")

    banner("7. 准备第二个合并，并在“目录切换前”注入崩溃")
    job2 = plan_merge(store, note="崩溃恢复演示")

    def crash(point: str, info: dict) -> None:
        if point == "before_catalog_swap":
            raise InjectedCrash(point)

    try:
        commit_merge(store, job2["job_id"], fault_inject=crash)
    except InjectedCrash:
        print("注入崩溃：posdel.json 尚未切换")
    head_after_crash = Store(root).head().id
    print(f"崩溃后 head 仍是 {head_after_crash}（合并不可见，旧版本继续可查）")
    assert head_after_crash == merged_snapshot["id"]

    banner("8. 重启进程并执行恢复：重放提交")
    reopened = Store(root)
    report = recover(reopened)
    print(f"恢复动作: {[a['action'] for a in report['actions']]}")
    assert report["ok"], report
    assert load_job(root, job2["job_id"])["status"] == "committed"
    recovered_head = reopened.head()
    print(f"恢复后 head {recovered_head.id}  {recovered_head.stats}")
    assert {r["data"]["id"] for r in visible(reopened)} == pre_delete_visible
    assert reopened.verify(full=True)["ok"]

    banner("9. 安全回收预览 + 执行（只保留最近 2 个快照）")
    preview = collect_garbage(reopened, keep_snapshots=2, dry_run=True)
    print(f"预览：{preview['count']} 个不可达文件，{preview['bytes']} 字节，"
          f"将过期 {preview['would_expire']}")
    plan = collect_garbage(reopened, keep_snapshots=2, dry_run=False)
    print(f"已回收 {len(plan['removed'])} 个文件，释放 {plan['reclaimed_bytes']} 字节")
    assert reopened.verify(full=True)["ok"]
    assert {r["data"]["id"] for r in visible(reopened)} == pre_delete_visible

    banner("完成")
    print(f"演示库: {root}")
    print("用 .venv/bin/python scripts/run_server.py --root " + root + " 打开页面查看。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
