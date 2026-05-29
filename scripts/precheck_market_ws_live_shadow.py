from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict, Iterable, List, Tuple

import requests


DEFAULT_BASE_URL = os.getenv("MARKET_WS_SHADOW_BASE_URL") or os.getenv("WEB_BASE_URL") or "http://127.0.0.1:8000"


def _join_url(base_url: str, path: str) -> str:
    return f"{str(base_url or '').strip().rstrip('/')}/" + str(path or "").strip().lstrip("/")


def _safe_json(response: requests.Response) -> Dict[str, Any]:
    try:
        payload = response.json()
    except Exception:
        return {"raw": response.text}
    return payload if isinstance(payload, dict) else {"value": payload}


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on", "y"}


def _request_json(base_url: str, token: str, path: str, timeout: float) -> Dict[str, Any]:
    headers = {"X-OPS-CALLER": "precheck_market_ws_live_shadow"}
    if token:
        headers["X-OPS-TOKEN"] = token
    url = _join_url(base_url, path)
    response = requests.request("GET", url, headers=headers, timeout=max(0.5, float(timeout or 5.0)))
    return {
        "url": url,
        "path": path,
        "status_code": int(response.status_code),
        "body": _safe_json(response),
    }


def evaluate_precheck(status_payload: Dict[str, Any], market_payload: Dict[str, Any]) -> Tuple[bool, List[str], Dict[str, Any]]:
    errors: List[str] = []
    market_ws = status_payload.get("market_ws") if isinstance(status_payload.get("market_ws"), dict) else market_payload
    market_ws = market_ws if isinstance(market_ws, dict) else {}
    trading_mode = str(status_payload.get("trading_mode") or "").strip().lower()
    market_mode = str(market_ws.get("mode") or "").strip().lower()

    if str(status_payload.get("status") or "").strip().lower() != "running":
        errors.append("api status is not running")
    if trading_mode != "live" or _as_bool(status_payload.get("paper_trading")):
        errors.append("runtime is not live")
    if market_mode != "shadow":
        errors.append(f"MARKET_WS_MODE must be shadow for live shadow, got {market_ws.get('mode')!r}")
    if market_mode in {"ui_primary", "strategy_primary"}:
        errors.append(f"{market_mode} must not be enabled before live shadow passes")
    if not _as_bool(market_ws.get("enabled")):
        errors.append("market WS stream is not enabled")
    if not _as_bool(market_ws.get("configured_enabled")):
        errors.append("MARKET_WS_ENABLED is not configured true")
    if _as_bool(market_ws.get("force_rest")):
        errors.append("MARKET_WS_FORCE_REST must be false for live shadow")
    if not _as_bool(market_ws.get("fail_closed_for_live")):
        errors.append("MARKET_WS_FAIL_CLOSED_FOR_LIVE must be true")

    summary = {
        "trading_mode": status_payload.get("trading_mode"),
        "paper_trading": status_payload.get("paper_trading"),
        "market_ws_mode": market_ws.get("mode"),
        "market_ws_enabled": market_ws.get("enabled"),
        "market_ws_configured_enabled": market_ws.get("configured_enabled"),
        "market_ws_force_rest": market_ws.get("force_rest"),
        "market_ws_fail_closed_for_live": market_ws.get("fail_closed_for_live"),
    }
    return not errors, errors, summary


def run_precheck(*, base_url: str, token: str, timeout: float) -> Dict[str, Any]:
    status = _request_json(base_url, token, "/api/status", timeout)
    market = _request_json(base_url, token, "/api/market-data/status", timeout)
    errors: List[str] = []
    if status["status_code"] != 200:
        errors.append(f"/api/status returned {status['status_code']}")
    if market["status_code"] != 200:
        errors.append(f"/api/market-data/status returned {market['status_code']}")
    ok, eval_errors, summary = evaluate_precheck(status["body"], market["body"])
    errors.extend(eval_errors)
    return {
        "ok": not errors and ok,
        "errors": errors,
        "summary": summary,
        "base_url": base_url,
    }


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Precheck Level 2 market-WS live shadow startup gates")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--token", default=os.getenv("OPS_TOKEN", ""))
    parser.add_argument("--timeout", type=float, default=float(os.getenv("MARKET_WS_SHADOW_REQUEST_TIMEOUT_SEC", "5")))
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = run_precheck(
            base_url=str(args.base_url),
            token=str(args.token or ""),
            timeout=float(args.timeout),
        )
    except Exception as exc:
        result = {"ok": False, "errors": [str(exc)], "summary": {}, "base_url": str(args.base_url)}
    print(
        f"market-ws-live-shadow precheck: {'PASS' if result.get('ok') else 'FAIL'}",
        file=sys.stderr,
    )
    for error in result.get("errors") or []:
        print(f"- {error}", file=sys.stderr)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
