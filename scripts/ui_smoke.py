#!/usr/bin/env python3
"""Optional browser smoke test for the workbench (Playwright).

    .venv/bin/python scripts/ui_smoke.py [--url http://127.0.0.1:8765]

The server must already be running with ``--demo`` (so the lake can be reset).
The script drives the same flow as docs/SCENARIOS.md:

    delete → plan merge → concurrent delete → commit (absorbed)
    → crash before_catalog_swap → recover → GC preview + apply

It prints what it checked and exits non-zero on the first failed expectation.
Requires ``pip install playwright`` plus a Chromium install; on minimal Linux
images set ``LD_LIBRARY_PATH`` / ``FONTCONFIG_FILE`` if Chromium cannot start.
"""

from __future__ import annotations

import argparse
import sys

from playwright.sync_api import sync_playwright


def check(label: str, condition: bool, detail: object = "") -> None:
    print(f"{'ok  ' if condition else 'FAIL'} {label}" + (f"  [{detail}]" if detail != "" else ""))
    if not condition:
        raise SystemExit(1)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8765")
    parser.add_argument("--headed", action="store_true")
    args = parser.parse_args()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not args.headed,
                                    args=["--no-sandbox", "--disable-gpu"])
        page = browser.new_page(viewport={"width": 1500, "height": 1100})
        errors: list[str] = []
        page.on("pageerror", lambda exc: errors.append(str(exc)))
        page.on("dialog", lambda d: d.accept())

        page.goto(args.url, wait_until="networkidle")
        res = page.request.post(f"{args.url}/api/demo/reset")
        check("demo reset", res.ok, res.status)
        page.reload(wait_until="networkidle")
        page.wait_for_timeout(400)

        # 1. raw view shows 24 raw rows.
        page.select_option("#query-mode", "raw")
        page.click("#btn-query")
        page.wait_for_timeout(500)
        check("raw rows = 24", page.locator("#pageinfo").inner_text().endswith("/ 24"),
              page.locator("#pageinfo").inner_text())
        page.select_option("#query-mode", "visible")
        page.click("#btn-query")
        page.wait_for_timeout(400)

        # 2. delete by condition.
        page.click("#btn-add-condition")
        page.locator(".condition-row .c-col").select_option(label="temp_c (float64)")
        page.locator(".condition-row .c-op").select_option("gt")
        page.locator(".condition-row .c-val").fill("18")
        page.click("#btn-delete-apply")
        page.wait_for_timeout(900)
        head_after_delete = page.locator("#pill-head").inner_text()

        # 3. prepare merge, then interleave another delete.
        page.click("#btn-merge-plan")
        page.wait_for_timeout(800)
        check("merge prepared", "PREPARED" in page.locator("#job-detail").inner_text())
        page.request.post(f"{args.url}/api/delete", data={
            "conditions": [{"column": "id", "op": "lt", "value": 3}],
            "reason": "UI smoke 并发删除",
        })
        page.click("#btn-refresh")
        page.wait_for_timeout(700)
        check("head advanced by interleaved delete",
              page.locator("#pill-head").inner_text() != head_after_delete)

        # 4. commit: must absorb the concurrent delete and follow the new head.
        page.click("#btn-merge-commit")
        page.wait_for_timeout(1400)
        job_text = page.locator("#job-detail").inner_text()
        check("merge absorbed", "absorbed" in job_text or "吸收" in job_text, job_text[:80])
        check("remap tag rendered", page.locator("#file-list .tag-remap").count() > 0)
        check("query follows new head",
              page.locator("#query-snapshot").input_value() ==
              page.locator("#pill-head").inner_text().replace("head ", ""))

        # 5. crash before the catalog swap, then recover.
        page.click("#btn-merge-plan")
        page.wait_for_timeout(800)
        page.select_option("#crash-point", "before_catalog_swap")
        page.click("#btn-merge-commit")
        page.wait_for_timeout(900)
        check("crash injected", "模拟崩溃已注入" in page.locator("#job-detail").inner_text())
        head_at_crash = page.locator("#pill-head").inner_text()
        page.click("#btn-recover")
        page.wait_for_timeout(1400)
        check("recovery advanced head",
              page.locator("#pill-head").inner_text() != head_at_crash)
        check("job committed after recovery", "COMMITTED" in page.locator("#job-list").inner_text())

        # 6. GC preview then apply.
        page.fill("#keep-snapshots", "1")
        page.click("#btn-gc-preview")
        page.wait_for_timeout(700)
        download_hint = page.locator("#gc-result").inner_text()
        check("gc preview is non-destructive", "不修改任何文件" in download_hint, download_hint[:60])
        check("gc apply enabled", page.locator("#btn-gc-apply").is_enabled())
        page.click("#btn-gc-apply")
        page.wait_for_timeout(1400)
        check("gc applied", "已回收" in page.locator("#gc-result").inner_text())
        check("verify passes", "校验通过" in page.locator("#verify-result").inner_text()
              or page.request.get(f"{args.url}/api/verify?full=true").json()["ok"])

        check("no page errors", not errors, errors)
        browser.close()
    print("\nUI smoke test passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
