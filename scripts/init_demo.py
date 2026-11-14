#!/usr/bin/env python3
"""Initialise the demo lake used by the web workbench.

    .venv/bin/python scripts/init_demo.py [--root var/demo-lake] [--force]
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from posdel.files import DataFile  # noqa: E402
from posdel.schema import parse_schema  # noqa: E402
from posdel.store import Store  # noqa: E402

FIELDS = [
    {"name": "id", "type": "int64"},
    {"name": "city", "type": "string"},
    {"name": "temp_c", "type": "float64"},
    {"name": "ok", "type": "bool"},
]

CITIES = ["北京", "上海", "广州", "深圳", "成都", "杭州"]


def demo_rows(count: int, start: int = 1) -> list[dict]:
    return [
        {
            "id": start + i,
            "city": CITIES[i % len(CITIES)],
            "temp_c": round(10.0 + ((i * 13) % 37) * 0.5, 2),
            "ok": i % 7 != 0,
        }
        for i in range(count)
    ]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=os.path.join(ROOT, "var", "demo-lake"))
    parser.add_argument("--force", action="store_true", help="删除已有演示库后重建")
    args = parser.parse_args()

    root = os.path.abspath(args.root)
    if os.path.exists(root):
        if not args.force:
            print(f"目录已存在：{root}\n如需重建请加 --force（会删除该目录）", file=sys.stderr)
            return 2
        shutil.rmtree(root)

    store = Store(root)
    schema = parse_schema(FIELDS)
    snapshot = store.init(schema, demo_rows(24), chunk_rows=8)
    print(f"初始化完成：{snapshot.id}  {snapshot.stats}")
    print(f"根目录：{root}")
    print("启动页面：.venv/bin/python scripts/run_server.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
