from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict, Tuple

import requests


def call(base_url: str, token: str, method: str, path: str, payload: Dict[str, Any] | None = None, timeout: int = 60) -> Tuple[int, Dict[str, Any]]:
    url = f"{base_url.rstrip('/')}{path}"
    headers = {
        "X-OPS-TOKEN": token,
        "X-OPS-CALLER": "selfcheck_openclaw_ops",
    }
    response = requests.request(method.upper(), url, headers=headers, json=payload, timeout=timeout)
    try:
        data = response.json()
    except Exception:
        data = {"raw": response.text}
    return response.status_code, data


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke test for OpenClaw Ops API")
    parser.add_argument("--base-url", default=os.getenv("OPS_BASE_URL", "http://127.0.0.1:8711/ops"))
    parser.add_argument("--token", default=os.getenv("OPS_TOKEN", ""))
    parser.add_argument("--skip-news", action="store_true")
    parser.add_argument("--timeout", type=int, default=int(os.getenv("OPS_TIMEOUT", "60")))
    args = parser.parse_args()

    if not args.token:
        print("OPS_TOKEN is required via --token or environment", file=sys.stderr)
        return 2

    checks = [
        ("health", "GET", "/health", None),
        ("status", "GET", "/status", None),
    ]
    if not args.skip_news:
        checks.append(("news_pull", "POST", "/news/pull_now", {"since_minutes": 60, "max_records": 20, "query": None}))

    overall_ok = True
    for name, method, path, payload in checks:
        try:
            status, body = call(args.base_url, args.token, method, path, payload, timeout=args.timeout)
            ok = 200 <= status < 300 and bool(body.get("ok", False))
            overall_ok = overall_ok and ok
            print(json.dumps({"check": name, "status_code": status, "ok": ok, "summary": body}, ensure_ascii=False, indent=2))
        except Exception as exc:
            overall_ok = False
            print(json.dumps({"check": name, "ok": False, "error": str(exc)}, ensure_ascii=False, indent=2))

    print(json.dumps({"overall_ok": overall_ok}, ensure_ascii=False))
    return 0 if overall_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
