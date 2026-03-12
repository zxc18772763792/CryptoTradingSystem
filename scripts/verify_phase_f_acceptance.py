"""Quick acceptance checks for CLAUDE.md Phase F.

Usage:
    python scripts/verify_phase_f_acceptance.py
    python scripts/verify_phase_f_acceptance.py --base-url http://127.0.0.1:8000 --strict
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Tuple

import httpx


def _ok(name: str, detail: str, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
    return {"name": name, "status": "ok", "detail": detail, "payload": payload or {}}


def _warn(name: str, detail: str, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
    return {"name": name, "status": "warn", "detail": detail, "payload": payload or {}}


def _err(name: str, detail: str, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
    return {"name": name, "status": "error", "detail": detail, "payload": payload or {}}


def _request_json(client: httpx.Client, path: str, timeout: float = 12.0) -> Tuple[bool, Dict[str, Any] | None, str]:
    try:
        resp = client.get(path, timeout=timeout)
        resp.raise_for_status()
        return True, resp.json(), ""
    except Exception as exc:
        return False, None, str(exc)


def check_live_signals(client: httpx.Client) -> Dict[str, Any]:
    ok, data, err = _request_json(client, "/api/ai/live-signals", timeout=25.0)
    if not ok and ("404" in err or "Not Found" in err):
        # Backward-compat path (may be shadowed by /candidates/{candidate_id} in some versions)
        ok, data, err = _request_json(client, "/api/ai/candidates/live-signals", timeout=25.0)
    if not ok or not isinstance(data, dict):
        return _err("F0a-live-signals", f"endpoint failed: {err}")
    items = data.get("items")
    if not isinstance(items, list):
        return _err("F0a-live-signals", "response missing items[]", {"response": data})
    if not items:
        return _warn("F0a-live-signals", "no running candidates; schema is reachable", {"count": 0})
    first = items[0] if isinstance(items[0], dict) else {}
    signal = first.get("signal") if isinstance(first, dict) else {}
    components = signal.get("components") if isinstance(signal, dict) else {}
    if not isinstance(signal, dict) or "direction" not in signal or not isinstance(components, dict):
        return _err("F0a-live-signals", "signal/components shape mismatch", {"sample": first})
    factor = components.get("factor") if isinstance(components, dict) else {}
    return _ok(
        "F0a-live-signals",
        "signal schema ok",
        {"count": len(items), "sample_direction": signal.get("direction"), "factor_confidence": (factor or {}).get("confidence")},
    )


def check_microstructure(client: httpx.Client) -> Dict[str, Any]:
    ok, data, err = _request_json(
        client,
        "/api/trading/market_microstructure?exchange=binance&symbol=BTC%2FUSDT&depth_limit=20",
        timeout=15.0,
    )
    if not ok or not isinstance(data, dict):
        return _err("F1-microstructure", f"endpoint failed: {err}")
    options = data.get("options") if isinstance(data, dict) else {}
    oi = data.get("oi") if isinstance(data, dict) else {}
    has_options_keys = isinstance(options, dict) and {"available", "atm_iv", "skew_25d", "put_call_ratio"}.issubset(set(options.keys()))
    has_oi_keys = isinstance(oi, dict) and {"available", "change_pct_1h"}.issubset(set(oi.keys()))
    if not has_options_keys:
        return _err("F1-microstructure", "options fields missing", {"options": options})
    if not has_oi_keys:
        return _err("F1-microstructure", "oi fields missing", {"oi": oi})
    detail = "options/oi fields present"
    if not bool(options.get("available")):
        detail = "options fields present (collector currently unavailable)"
    return _ok(
        "F1-microstructure",
        detail,
        {
            "options_available": options.get("available"),
            "skew_25d": options.get("skew_25d"),
            "put_call_ratio": options.get("put_call_ratio"),
            "oi_available": oi.get("available"),
            "oi_change_pct_1h": oi.get("change_pct_1h"),
        },
    )


def check_google_trends_file(project_root: Path) -> Dict[str, Any]:
    p = project_root / "data" / "google_trends" / "bitcoin_trends.parquet"
    if p.exists():
        return _ok("F2-google-trends-cache", "cache file exists", {"path": str(p), "size": p.stat().st_size})
    return _warn("F2-google-trends-cache", "cache file not found (worker may be pending or pytrends missing)", {"path": str(p)})


def check_macro_cache_file(project_root: Path) -> Dict[str, Any]:
    p = project_root / "data" / "macro" / "vix.parquet"
    if p.exists():
        return _ok("F3-macro-cache", "macro cache file exists", {"path": str(p), "size": p.stat().st_size})
    return _warn("F3-macro-cache", "vix cache file not found (worker may be pending)", {"path": str(p)})


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--strict", action="store_true", help="treat warnings as failure")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    results = []

    with httpx.Client(base_url=args.base_url) as client:
        results.append(check_live_signals(client))
        results.append(check_microstructure(client))

    results.append(check_google_trends_file(project_root))
    results.append(check_macro_cache_file(project_root))

    print(json.dumps({"base_url": args.base_url, "results": results}, ensure_ascii=False, indent=2))

    has_error = any(r.get("status") == "error" for r in results)
    has_warn = any(r.get("status") == "warn" for r in results)
    if has_error:
        return 2
    if args.strict and has_warn:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
