#!/usr/bin/env python3
"""Run the local workbench server.

    .venv/bin/python scripts/run_server.py [--port 8765] [--root var/demo-lake] [--demo]

``--demo`` exposes the "reset demo lake" button.  Without an existing lake the
UI offers to create one when demo mode is on.
"""

from __future__ import annotations

import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8765")))
    parser.add_argument("--root", default=None)
    parser.add_argument("--demo", action="store_true", help="启用演示模式（可一键重置示例库）")
    args = parser.parse_args()

    if args.root:
        os.environ["POSDEL_ROOT"] = os.path.abspath(args.root)
    if args.demo:
        os.environ["POSDEL_DEMO"] = "1"

    import uvicorn

    from web.app import STORE_ROOT

    print(f"posdel 工作台: http://{args.host}:{args.port}")
    print(f"存储目录:     {STORE_ROOT}")
    if not os.path.exists(os.path.join(STORE_ROOT, "posdel.json")):
        print("提示: 湖表尚未初始化；可运行 scripts/init_demo.py --force，"
              "或在演示模式下从页面创建。")
    uvicorn.run("web.app:app", host=args.host, port=args.port, reload=False, log_level="info")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
