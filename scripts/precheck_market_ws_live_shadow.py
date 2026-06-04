from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict, Iterable, List, Tuple

import requests


DEFAULT_BASE_URL = os.getenv("MARKET_WS_SHADOW_BASE_URL") or os.getenv("WEB_BASE_URL") or "http://127.0.0.1:8000"
DEFAULT_MAX_WS_AGE_MS = float(os.getenv("MARKET_WS_LIVE_SHADOW_PRECHECK_MAX_WS_AGE_MS", "10000"))


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


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _as_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _has_error_text(value: Any) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() not in {"", "none", "null"}


def _feed_watch_symbols(market_ws: Dict[str, Any]) -> List[Tuple[str, str]]:
    feed_status = market_ws.get("feed_status") if isinstance(market_ws.get("feed_status"), dict) else {}
    exchanges = feed_status.get("exchanges") if isinstance(feed_status.get("exchanges"), dict) else {}
    seen: set[Tuple[str, str]] = set()
    symbols: List[Tuple[str, str]] = []
    for exchange, exchange_status in exchanges.items():
        if not isinstance(exchange_status, dict):
            continue
        raw_symbols = exchange_status.get("last_symbols") or []
        if isinstance(raw_symbols, str):
            raw_symbols = [raw_symbols]
        if not isinstance(raw_symbols, list):
            continue
        for raw_symbol in raw_symbols:
            symbol = str(raw_symbol or "").strip()
            exchange_name = str(exchange or "").strip()
            if not exchange_name or not symbol:
                continue
            key = (exchange_name, symbol)
            if key in seen:
                continue
            seen.add(key)
            symbols.append(key)
    return symbols


def _symbol_snapshot(market_ws: Dict[str, Any], exchange: str, symbol: str) -> Dict[str, Any]:
    symbols = market_ws.get("symbols") if isinstance(market_ws.get("symbols"), dict) else {}
    exchange_symbols = symbols.get(exchange) if isinstance(symbols.get(exchange), dict) else {}
    snapshot = exchange_symbols.get(symbol)
    if isinstance(snapshot, dict):
        return snapshot
    normalized = symbol.split(":", 1)[0]
    snapshot = exchange_symbols.get(normalized)
    return snapshot if isinstance(snapshot, dict) else {}


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


def evaluate_precheck(
    status_payload: Dict[str, Any],
    market_payload: Dict[str, Any],
    *,
    max_ws_age_ms: float = DEFAULT_MAX_WS_AGE_MS,
) -> Tuple[bool, List[str], Dict[str, Any]]:
    errors: List[str] = []
    market_ws = market_payload if isinstance(market_payload, dict) else {}
    if not market_ws and isinstance(status_payload.get("market_ws"), dict):
        market_ws = status_payload["market_ws"]
    trading_mode = str(status_payload.get("trading_mode") or "").strip().lower()
    market_mode = str(market_ws.get("mode") or "").strip().lower()
    hub_healthy = market_ws.get("ws_hub_healthy")
    if hub_healthy is None:
        hub_healthy = market_ws.get("hub_healthy")
    stale_symbol_count = (
        _as_int(market_ws.get("ws_stale_symbol_count"))
        if "ws_stale_symbol_count" in market_ws
        else _as_int(market_ws.get("stale_symbol_count"))
    )
    last_tick_age_ms = _as_float(market_ws.get("last_tick_age_ms"))
    watched_symbols = _feed_watch_symbols(market_ws)
    watched_symbol_errors: List[str] = []
    if _as_bool(market_ws.get("feed_healthy")) and not watched_symbols:
        watched_symbol_errors.append("market WS feed watch symbols are missing")
    if watched_symbols and not isinstance(market_ws.get("symbols"), dict):
        watched_symbol_errors.append("market WS symbol details are missing")
    for exchange, symbol in watched_symbols:
        symbol_status = _symbol_snapshot(market_ws, exchange, symbol)
        symbol_key = f"{exchange}:{symbol}"
        if not symbol_status:
            watched_symbol_errors.append(f"market WS missing watched symbol tick: {symbol_key}")
            continue
        source = str(symbol_status.get("source") or "").strip().lower()
        if source != "ws":
            watched_symbol_errors.append(
                f"market WS watched symbol is not WS sourced: {symbol_key} source={symbol_status.get('source')!r}"
            )
        if _as_bool(symbol_status.get("is_stale")):
            watched_symbol_errors.append(f"market WS watched symbol is stale: {symbol_key}")
        symbol_age_ms = _as_float(symbol_status.get("age_ms"))
        if symbol_age_ms is None:
            watched_symbol_errors.append(f"market WS watched symbol age is missing: {symbol_key}")
        elif symbol_age_ms > float(max_ws_age_ms):
            watched_symbol_errors.append(
                f"market WS watched symbol age {symbol_age_ms:.1f} ms exceeds {float(max_ws_age_ms):.1f} ms: {symbol_key}"
            )

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
    if market_ws.get("fail_closed_for_live") is None:
        errors.append("market WS status is missing fail_closed_for_live")
    elif not _as_bool(market_ws.get("fail_closed_for_live")):
        errors.append("MARKET_WS_FAIL_CLOSED_FOR_LIVE must be true")
    if not _as_bool(market_ws.get("feed_healthy")):
        errors.append("market WS feed is not healthy")
    if not _as_bool(hub_healthy):
        errors.append("market WS hub is not healthy")
    if _has_error_text(market_ws.get("feed_last_error")):
        errors.append(f"market WS feed_last_error is not empty: {market_ws.get('feed_last_error')!r}")
    if stale_symbol_count > 0:
        errors.append(f"market WS has stale symbols: {stale_symbol_count}")
    if last_tick_age_ms is None:
        errors.append("market WS last tick age is missing")
    elif last_tick_age_ms > float(max_ws_age_ms):
        errors.append(f"market WS last tick age {last_tick_age_ms:.1f} ms exceeds {float(max_ws_age_ms):.1f} ms")
    invalid_payload_count = _as_int(market_ws.get("invalid_payload_count"))
    timestamp_regression_count = _as_int(market_ws.get("timestamp_regression_count"))
    feed_watch_empty_count = _as_int(market_ws.get("feed_watch_empty_count"))
    shadow_compare_violation_count = _as_int(market_ws.get("shadow_compare_violation_count"))
    shadow_compare_stale_skip_count = _as_int(market_ws.get("shadow_compare_stale_skip_count"))
    if invalid_payload_count > 0:
        errors.append(f"market WS invalid payload count is {invalid_payload_count}")
    if timestamp_regression_count > 0:
        errors.append(f"market WS timestamp regression count is {timestamp_regression_count}")
    if feed_watch_empty_count > 0:
        errors.append(f"market WS feed empty watch count is {feed_watch_empty_count}")
    if shadow_compare_violation_count > 0:
        errors.append(f"market WS shadow compare violation count is {shadow_compare_violation_count}")
    if shadow_compare_stale_skip_count > 0:
        errors.append(f"market WS shadow stale skip count is {shadow_compare_stale_skip_count}")
    errors.extend(watched_symbol_errors)

    summary = {
        "trading_mode": status_payload.get("trading_mode"),
        "paper_trading": status_payload.get("paper_trading"),
        "market_ws_mode": market_ws.get("mode"),
        "market_ws_enabled": market_ws.get("enabled"),
        "market_ws_configured_enabled": market_ws.get("configured_enabled"),
        "market_ws_force_rest": market_ws.get("force_rest"),
        "market_ws_fail_closed_for_live": market_ws.get("fail_closed_for_live"),
        "market_ws_mark_price_enabled": market_ws.get("mark_price_enabled"),
        "market_ws_auxiliary_symbol_count": _as_int(market_ws.get("auxiliary_symbol_count")),
        "market_ws_channel_counts": market_ws.get("channel_counts") or {},
        "market_ws_feed_healthy": market_ws.get("feed_healthy"),
        "market_ws_ws_hub_healthy": hub_healthy,
        "market_ws_feed_last_error": market_ws.get("feed_last_error"),
        "market_ws_last_tick_age_ms": last_tick_age_ms,
        "market_ws_stale_symbol_count": stale_symbol_count,
        "market_ws_invalid_payload_count": invalid_payload_count,
        "market_ws_timestamp_regression_count": timestamp_regression_count,
        "market_ws_feed_watch_empty_count": feed_watch_empty_count,
        "market_ws_shadow_compare_violation_count": shadow_compare_violation_count,
        "market_ws_shadow_compare_stale_skip_count": shadow_compare_stale_skip_count,
        "market_ws_feed_watch_symbol_count": len(watched_symbols),
        "market_ws_feed_watch_symbols": [f"{exchange}:{symbol}" for exchange, symbol in watched_symbols],
        "market_ws_feed_watch_symbol_error_count": len(watched_symbol_errors),
    }
    return not errors, errors, summary


def run_precheck(*, base_url: str, token: str, timeout: float, max_ws_age_ms: float) -> Dict[str, Any]:
    status = _request_json(base_url, token, "/api/status", timeout)
    market = _request_json(base_url, token, "/api/market-data/status", timeout)
    errors: List[str] = []
    if status["status_code"] != 200:
        errors.append(f"/api/status returned {status['status_code']}")
    if market["status_code"] != 200:
        errors.append(f"/api/market-data/status returned {market['status_code']}")
    ok, eval_errors, summary = evaluate_precheck(
        status["body"],
        market["body"],
        max_ws_age_ms=float(max_ws_age_ms),
    )
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
    parser.add_argument("--max-ws-age-ms", type=float, default=DEFAULT_MAX_WS_AGE_MS)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = run_precheck(
            base_url=str(args.base_url),
            token=str(args.token or ""),
            timeout=float(args.timeout),
            max_ws_age_ms=float(args.max_ws_age_ms),
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
