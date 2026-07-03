from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

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


def _safe_get(data: Dict[str, Any] | None, *path: str, default: Any = None) -> Any:
    current: Any = data or {}
    for key in path:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
    return default if current is None else current


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


def _as_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _percentile(values: List[float], pct: float) -> Optional[float]:
    clean = sorted(float(v) for v in values if v is not None)
    if not clean:
        return None
    if len(clean) == 1:
        return clean[0]
    rank = (len(clean) - 1) * max(0.0, min(100.0, float(pct))) / 100.0
    low = int(rank)
    high = min(low + 1, len(clean) - 1)
    fraction = rank - low
    return clean[low] * (1.0 - fraction) + clean[high] * fraction


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
            exchange_name = str(exchange or "").strip()
            symbol = str(raw_symbol or "").strip()
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


def _watched_symbol_errors(market_ws: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    watched_symbols = _feed_watch_symbols(market_ws)
    errors: List[str] = []
    if _as_bool(market_ws.get("feed_healthy")) and not watched_symbols:
        errors.append("market WS feed watch symbols are missing")
    if watched_symbols and not isinstance(market_ws.get("symbols"), dict):
        errors.append("market WS symbol details are missing")
    max_age_ms = _as_float(market_ws.get("symbol_max_age_sec"))
    max_age_ms = max_age_ms * 1000.0 if max_age_ms is not None else None
    for exchange, symbol in watched_symbols:
        symbol_status = _symbol_snapshot(market_ws, exchange, symbol)
        symbol_key = f"{exchange}:{symbol}"
        if not symbol_status:
            errors.append(f"market WS missing watched symbol tick: {symbol_key}")
            continue
        source = str(symbol_status.get("source") or "").strip().lower()
        if source != "ws":
            errors.append(f"market WS watched symbol is not WS sourced: {symbol_key} source={symbol_status.get('source')!r}")
        if _as_bool(symbol_status.get("is_stale")):
            errors.append(f"market WS watched symbol is stale: {symbol_key}")
        symbol_age_ms = _as_float(symbol_status.get("age_ms"))
        if symbol_age_ms is None:
            errors.append(f"market WS watched symbol age is missing: {symbol_key}")
        elif max_age_ms is not None and symbol_age_ms > max_age_ms:
            errors.append(f"market WS watched symbol age {symbol_age_ms:.1f} ms exceeds {max_age_ms:.1f} ms: {symbol_key}")
    return [f"{exchange}:{symbol}" for exchange, symbol in watched_symbols], errors


def _request_json(
    base_url: str,
    token: str,
    path: str,
    timeout: float,
    *,
    retries: int = 0,
    retry_backoff: float = 0.75,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> Dict[str, Any]:
    headers = {"X-OPS-CALLER": "selfcheck_market_ws_shadow"}
    if token:
        headers["X-OPS-TOKEN"] = token
    url = _join_url(base_url, path)
    attempts = max(1, int(retries) + 1)
    last_exc: Optional[Exception] = None
    for attempt in range(attempts):
        try:
            response = requests.request(
                "GET", url, headers=headers, timeout=max(0.5, float(timeout or 5.0))
            )
            return {
                "url": url,
                "path": path,
                "status_code": int(response.status_code),
                "body": _safe_json(response),
            }
        except requests.exceptions.RequestException as exc:
            # A read/connect timeout or reset is a failed *measurement* (the
            # observer's own HTTP call hiccuped), not a WS-quality signal. Retry a
            # bounded number of times so an isolated stall (e.g. a GC pause or a
            # cold-start warmup blip) doesn't record a sample_error that voids a
            # multi-hour run. A non-200 status is a real reading and is NOT retried.
            last_exc = exc
            if attempt < attempts - 1:
                sleep_fn(max(0.0, float(retry_backoff)) * (attempt + 1))
                continue
            raise
    raise last_exc if last_exc is not None else RuntimeError("request failed")


def _extract_sample(
    base_url: str,
    token: str,
    timeout: float,
    *,
    retries: int = 0,
    retry_backoff: float = 0.75,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> Dict[str, Any]:
    sampled_at = datetime.now(timezone.utc).isoformat()
    health = _request_json(base_url, token, "/health", timeout, retries=retries, retry_backoff=retry_backoff, sleep_fn=sleep_fn)
    status = _request_json(base_url, token, "/api/status", timeout, retries=retries, retry_backoff=retry_backoff, sleep_fn=sleep_fn)
    market = _request_json(base_url, token, "/api/market-data/status", timeout, retries=retries, retry_backoff=retry_backoff, sleep_fn=sleep_fn)
    status_body = status["body"]
    market_body = market["body"]
    status_market_ws = status_body.get("market_ws") if isinstance(status_body.get("market_ws"), dict) else {}
    market_ws = market_body if isinstance(market_body, dict) else {}
    shadow_last = market_ws.get("shadow_last_compare") if isinstance(market_ws.get("shadow_last_compare"), dict) else {}
    watched_symbols, watched_symbol_errors = _watched_symbol_errors(market_ws)
    return {
        "sampled_at": sampled_at,
        "sample_ok": True,
        "health_status_code": health["status_code"],
        "status_status_code": status["status_code"],
        "market_status_code": market["status_code"],
        "health_status": health["body"].get("status"),
        "app_status": status_body.get("status"),
        "paper_trading": status_body.get("paper_trading"),
        "trading_mode": status_body.get("trading_mode"),
        "status_market_ws_mode": status_market_ws.get("mode"),
        "mode": market_ws.get("mode"),
        "enabled": market_ws.get("enabled"),
        "configured_enabled": market_ws.get("configured_enabled"),
        "force_rest": market_ws.get("force_rest"),
        "fail_closed_for_live": market_ws.get("fail_closed_for_live"),
        "mark_price_enabled": market_ws.get("mark_price_enabled"),
        "feed_present": market_ws.get("feed_present"),
        "feed_healthy": market_ws.get("feed_healthy"),
        "hub_healthy": market_ws.get("hub_healthy"),
        "ws_hub_healthy": market_ws.get("ws_hub_healthy"),
        "healthy_exchanges": market_ws.get("healthy_exchanges") or [],
        "ws_healthy_exchanges": market_ws.get("ws_healthy_exchanges") or [],
        "symbol_count": _as_int(market_ws.get("symbol_count")),
        "ws_symbol_count": _as_int(market_ws.get("ws_symbol_count")),
        "auxiliary_symbol_count": _as_int(market_ws.get("auxiliary_symbol_count")),
        "channel_counts": market_ws.get("channel_counts") or {},
        "stale_symbol_count": _as_int(market_ws.get("stale_symbol_count")),
        "ws_stale_symbol_count": (
            _as_int(market_ws.get("ws_stale_symbol_count"))
            if "ws_stale_symbol_count" in market_ws
            else None
        ),
        "last_tick_age_ms": _as_float(market_ws.get("last_tick_age_ms")),
        "oldest_tick_age_ms": _as_float(market_ws.get("oldest_tick_age_ms")),
        "ws_tick_count": _as_int(market_ws.get("ws_tick_count")),
        "rest_fallback_count": _as_int(market_ws.get("rest_fallback_count")),
        "rest_snapshot_count": _as_int(market_ws.get("rest_snapshot_count")),
        "invalid_payload_count": _as_int(market_ws.get("invalid_payload_count")),
        "timestamp_regression_count": _as_int(market_ws.get("timestamp_regression_count")),
        "fallback_reasons": market_ws.get("fallback_reasons") or {},
        "shadow_compare_count": _as_int(market_ws.get("shadow_compare_count")),
        "shadow_compare_violation_count": _as_int(market_ws.get("shadow_compare_violation_count")),
        "shadow_missing_ws_count": _as_int(market_ws.get("shadow_missing_ws_count")),
        "shadow_compare_stale_skip_count": _as_int(market_ws.get("shadow_compare_stale_skip_count")),
        "shadow_max_abs_diff_bps": _as_float(market_ws.get("shadow_max_abs_diff_bps")),
        "shadow_last_abs_diff_bps": _as_float(shadow_last.get("abs_diff_bps")),
        "shadow_last_ws_age_ms": _as_float(shadow_last.get("ws_age_ms")),
        "shadow_last_rest_age_ms": _as_float(shadow_last.get("rest_age_ms")),
        "shadow_last_accepted": shadow_last.get("accepted"),
        "last_error": market_ws.get("last_error"),
        "feed_watch_attempt_count": _as_int(market_ws.get("feed_watch_attempt_count")),
        "feed_watch_timeout_count": _as_int(market_ws.get("feed_watch_timeout_count")),
        "feed_watch_error_count": _as_int(market_ws.get("feed_watch_error_count")),
        "feed_watch_empty_count": _as_int(market_ws.get("feed_watch_empty_count")),
        "feed_last_error": market_ws.get("feed_last_error"),
        "feed_watch_symbols": watched_symbols,
        "feed_watch_symbol_count": len(watched_symbols),
        "feed_watch_symbol_errors": watched_symbol_errors,
        "feed_watch_symbol_error_count": len(watched_symbol_errors),
    }


def _failed_sample(error: Exception) -> Dict[str, Any]:
    return {
        "sampled_at": datetime.now(timezone.utc).isoformat(),
        "sample_ok": False,
        "sample_error": f"{type(error).__name__}: {error}",
    }


def _delta(samples: List[Dict[str, Any]], key: str) -> int:
    if not samples:
        return 0
    return _as_int(samples[-1].get(key)) - _as_int(samples[0].get(key))


def _evaluate_samples(
    samples: List[Dict[str, Any]],
    *,
    min_samples: int,
    expect_mode: str,
    expect_runtime: str,
    min_ws_tick_delta: int,
    min_shadow_compare_delta: int,
    max_shadow_violation_delta: int,
    max_invalid_payload_delta: int,
    max_timestamp_regression_delta: int,
    max_shadow_stale_skip_delta: int,
    max_feed_watch_timeout_delta: int,
    max_feed_watch_error_delta: int,
    max_feed_watch_empty_delta: int,
    max_stale_symbol_count: int,
    max_price_diff_bps: float,
    max_ws_age_p95_ms: Optional[float],
    tolerate_transient: bool = False,
    max_degraded_samples: int = 0,
    max_consecutive_degraded: int = 1,
    max_degraded_oldest_age_ms: float = 60000.0,
    max_sample_errors: int = 0,
) -> Tuple[bool, List[str], Dict[str, Any]]:
    errors: List[str] = []
    mode = str(expect_mode or "shadow").strip().lower()
    runtime = str(expect_runtime or "paper").strip().lower()
    degraded_indices: List[int] = []
    sample_error_indices: List[Tuple[int, Any]] = []
    for index, sample in enumerate(samples):
        prefix = f"sample[{index}]"
        if sample.get("sample_error"):
            # Defer: a probe failure is the observer's own HTTP call timing out,
            # not a WS-quality breach. Default is zero-tolerance; an explicit
            # bounded budget can tolerate a few *isolated* ones (handled below).
            sample_error_indices.append((index, sample.get("sample_error")))
            continue
        if sample.get("health_status_code") != 200 or str(sample.get("health_status")).lower() not in {"healthy", "running"}:
            errors.append(f"{prefix}: /health not healthy")
        if sample.get("status_status_code") != 200 or str(sample.get("app_status")).lower() != "running":
            errors.append(f"{prefix}: /api/status not running")
        if sample.get("market_status_code") != 200:
            errors.append(f"{prefix}: /api/market-data/status failed")
        trading_mode = str(sample.get("trading_mode") or "").strip().lower()
        paper_trading = _as_bool(sample.get("paper_trading"))
        if runtime == "paper":
            if not paper_trading or (trading_mode and trading_mode != "paper"):
                errors.append(f"{prefix}: expected paper runtime")
        elif runtime == "live":
            if paper_trading or trading_mode != "live":
                errors.append(f"{prefix}: expected live runtime")
        elif runtime and trading_mode != runtime:
            errors.append(f"{prefix}: expected runtime {runtime}, got {sample.get('trading_mode')!r}")
        if str(sample.get("mode") or "").strip().lower() != mode:
            errors.append(f"{prefix}: expected market ws mode {mode}, got {sample.get('mode')!r}")
        if mode != "off" and not _as_bool(sample.get("enabled")):
            errors.append(f"{prefix}: expected market ws enabled")
        if mode != "off" and not _as_bool(sample.get("configured_enabled")):
            errors.append(f"{prefix}: expected configured market ws enabled")
        if _as_bool(sample.get("force_rest")):
            errors.append(f"{prefix}: force_rest is true")
        if runtime == "live" and not _as_bool(sample.get("fail_closed_for_live")):
            errors.append(f"{prefix}: fail_closed_for_live is not true")
        if mode != "strategy_primary" and str(sample.get("mode") or "").strip().lower() == "strategy_primary":
            errors.append(f"{prefix}: strategy_primary must not be enabled during shadow check")
        stale_for_check = (
            _as_int(sample.get("ws_stale_symbol_count"))
            if sample.get("ws_stale_symbol_count") is not None
            else _as_int(sample.get("stale_symbol_count"))
        )
        sample_watch_errors = sample.get("feed_watch_symbol_errors") or []
        sample_stale = stale_for_check > max_stale_symbol_count
        if sample_stale or sample_watch_errors:
            degraded_indices.append(index)
        if not tolerate_transient:
            if sample_stale:
                errors.append(f"{prefix}: ws_stale_symbol_count>{max_stale_symbol_count}")
            for watched_symbol_error in sample_watch_errors:
                errors.append(f"{prefix}: {watched_symbol_error}")

    # Transient-degradation tolerance (only enforced when --tolerate-transient is set).
    # A "degraded" sample is one where a watched symbol was momentarily not WS-sourced
    # or stale; we allow a small bounded number of *isolated* such samples (which REST
    # fallback covers) but still hard-fail on sustained degradation.
    total_degraded = len(degraded_indices)
    longest_degraded = 0
    if degraded_indices:
        longest_degraded = _run = 1
        for _a, _b in zip(degraded_indices, degraded_indices[1:]):
            _run = _run + 1 if _b == _a + 1 else 1
            longest_degraded = max(longest_degraded, _run)
    # Use the active/watched feed freshness (last_tick_age_ms), NOT the hub's
    # global oldest_tick_age_ms — the latter false-fires on a stale *non-watched*
    # symbol that got one tick early and never updated again (can read ~hours).
    worst_degraded_oldest = max(
        (_as_float(samples[i].get("last_tick_age_ms")) or 0.0 for i in degraded_indices),
        default=0.0,
    )
    if tolerate_transient:
        if total_degraded > int(max_degraded_samples):
            errors.append(f"degraded_sample_count {total_degraded} > allowed {max_degraded_samples}")
        if longest_degraded > int(max_consecutive_degraded):
            errors.append(
                f"max_consecutive_degraded {longest_degraded} > allowed {max_consecutive_degraded} (sustained degradation)"
            )
        if worst_degraded_oldest > float(max_degraded_oldest_age_ms):
            errors.append(
                f"degraded_sample watched_tick_age_ms {worst_degraded_oldest:.0f} > allowed {max_degraded_oldest_age_ms:.0f}"
            )

    # Probe-failure tolerance. A ``sample_error`` is the observer's own HTTP probe
    # to the service timing out / erroring — a failed *measurement*, not a
    # WS-quality breach. Default (max_sample_errors=0) is zero-tolerance: identical
    # to the prior behavior. With an explicit budget AND --tolerate-transient,
    # allow up to N *isolated* probe failures but still hard-fail on any consecutive
    # pair (a sustained stall, not a blip) or on exceeding the budget. The
    # valid_sample_count floor below still applies independently, so tolerating a
    # probe miss can never drop the run under its required sample count.
    total_sample_errors = len(sample_error_indices)
    longest_sample_error_run = 0
    if sample_error_indices:
        err_idx = [i for i, _ in sample_error_indices]
        longest_sample_error_run = _run = 1
        for _a, _b in zip(err_idx, err_idx[1:]):
            _run = _run + 1 if _b == _a + 1 else 1
            longest_sample_error_run = max(longest_sample_error_run, _run)
        tolerated = (
            tolerate_transient
            and int(max_sample_errors) > 0
            and total_sample_errors <= int(max_sample_errors)
            and longest_sample_error_run <= 1
        )
        if not tolerated:
            for index, msg in sample_error_indices:
                errors.append(f"sample[{index}]: sample request failed: {msg}")

    valid_samples = [sample for sample in samples if not sample.get("sample_error")]
    if len(valid_samples) < int(min_samples):
        errors.append(f"valid_sample_count {len(valid_samples)} < required {int(min_samples)}")

    ws_tick_delta = _delta(valid_samples, "ws_tick_count")
    shadow_compare_delta = _delta(valid_samples, "shadow_compare_count")
    shadow_violation_delta = _delta(valid_samples, "shadow_compare_violation_count")
    invalid_delta = _delta(valid_samples, "invalid_payload_count")
    timestamp_regression_delta = _delta(valid_samples, "timestamp_regression_count")
    shadow_stale_skip_delta = _delta(valid_samples, "shadow_compare_stale_skip_count")
    feed_watch_timeout_delta = _delta(valid_samples, "feed_watch_timeout_count")
    feed_watch_error_delta = _delta(valid_samples, "feed_watch_error_count")
    feed_watch_empty_delta = _delta(valid_samples, "feed_watch_empty_count")
    if ws_tick_delta < int(min_ws_tick_delta):
        errors.append(f"ws_tick_delta {ws_tick_delta} < required {min_ws_tick_delta}")
    if shadow_compare_delta < int(min_shadow_compare_delta):
        errors.append(f"shadow_compare_delta {shadow_compare_delta} < required {min_shadow_compare_delta}")
    if shadow_violation_delta > int(max_shadow_violation_delta):
        errors.append(f"shadow_compare_violation_delta {shadow_violation_delta} > allowed {max_shadow_violation_delta}")
    if invalid_delta > int(max_invalid_payload_delta):
        errors.append(f"invalid_payload_delta {invalid_delta} > allowed {max_invalid_payload_delta}")
    if timestamp_regression_delta > int(max_timestamp_regression_delta):
        errors.append(
            f"timestamp_regression_delta {timestamp_regression_delta} > allowed {max_timestamp_regression_delta}"
        )
    if shadow_stale_skip_delta > int(max_shadow_stale_skip_delta):
        errors.append(
            f"shadow_compare_stale_skip_delta {shadow_stale_skip_delta} > allowed {max_shadow_stale_skip_delta}"
        )
    if int(max_feed_watch_timeout_delta) >= 0 and feed_watch_timeout_delta > int(max_feed_watch_timeout_delta):
        errors.append(
            f"feed_watch_timeout_delta {feed_watch_timeout_delta} > allowed {max_feed_watch_timeout_delta}"
        )
    if int(max_feed_watch_error_delta) >= 0 and feed_watch_error_delta > int(max_feed_watch_error_delta):
        errors.append(
            f"feed_watch_error_delta {feed_watch_error_delta} > allowed {max_feed_watch_error_delta}"
        )
    if feed_watch_empty_delta > int(max_feed_watch_empty_delta):
        errors.append(
            f"feed_watch_empty_delta {feed_watch_empty_delta} > allowed {max_feed_watch_empty_delta}"
        )

    diff_values = [
        float(value)
        for value in (_as_float(sample.get("shadow_last_abs_diff_bps")) for sample in valid_samples)
        if value is not None
    ]
    ws_age_values = [
        float(value)
        for value in (_as_float(sample.get("shadow_last_ws_age_ms")) for sample in valid_samples)
        if value is not None
    ]
    tick_age_values = [
        float(value)
        for value in (_as_float(sample.get("last_tick_age_ms")) for sample in valid_samples)
        if value is not None
    ]
    diff_p99 = _percentile(diff_values, 99.0)
    ws_age_p95 = _percentile(ws_age_values or tick_age_values, 95.0)
    if diff_p99 is not None and diff_p99 > float(max_price_diff_bps):
        errors.append(f"p99_abs_diff_bps {diff_p99:.4f} > allowed {max_price_diff_bps:.4f}")
    if max_ws_age_p95_ms is not None and ws_age_p95 is not None and ws_age_p95 > float(max_ws_age_p95_ms):
        errors.append(f"p95_ws_age_ms {ws_age_p95:.1f} > allowed {float(max_ws_age_p95_ms):.1f}")

    summary = {
        "sample_count": len(valid_samples),
        "valid_sample_count": len(valid_samples),
        "sample_attempt_count": len(samples),
        "sample_error_count": len(samples) - len(valid_samples),
        "max_consecutive_sample_errors_observed": longest_sample_error_run,
        "ws_tick_delta": ws_tick_delta,
        "shadow_compare_delta": shadow_compare_delta,
        "shadow_compare_violation_delta": shadow_violation_delta,
        "invalid_payload_delta": invalid_delta,
        "timestamp_regression_delta": timestamp_regression_delta,
        "shadow_compare_stale_skip_delta": shadow_stale_skip_delta,
        "rest_fallback_delta": _delta(valid_samples, "rest_fallback_count"),
        "rest_snapshot_delta": _delta(valid_samples, "rest_snapshot_count"),
        "shadow_missing_ws_delta": _delta(valid_samples, "shadow_missing_ws_count"),
        "feed_watch_attempt_delta": _delta(valid_samples, "feed_watch_attempt_count"),
        "feed_watch_timeout_delta": feed_watch_timeout_delta,
        "feed_watch_error_delta": feed_watch_error_delta,
        "feed_watch_empty_delta": feed_watch_empty_delta,
        "max_feed_watch_symbol_error_count_observed": max(
            (_as_int(sample.get("feed_watch_symbol_error_count")) for sample in valid_samples),
            default=0,
        ),
        "p99_abs_diff_bps": diff_p99,
        "p95_ws_age_ms": ws_age_p95,
        "tolerate_transient": bool(tolerate_transient),
        "degraded_sample_count": total_degraded,
        "max_consecutive_degraded_observed": longest_degraded,
        "worst_degraded_oldest_age_ms": worst_degraded_oldest,
        "max_stale_symbol_count_observed": max(
            (
                _as_int(sample.get("ws_stale_symbol_count"))
                if sample.get("ws_stale_symbol_count") is not None
                else _as_int(sample.get("stale_symbol_count"))
                for sample in valid_samples
            ),
            default=0,
        ),
        "final_mode": valid_samples[-1].get("mode") if valid_samples else None,
        "final_trading_mode": valid_samples[-1].get("trading_mode") if valid_samples else None,
        "final_paper_trading": valid_samples[-1].get("paper_trading") if valid_samples else None,
        "final_enabled": valid_samples[-1].get("enabled") if valid_samples else None,
        "final_configured_enabled": valid_samples[-1].get("configured_enabled") if valid_samples else None,
        "final_fail_closed_for_live": valid_samples[-1].get("fail_closed_for_live") if valid_samples else None,
        "final_mark_price_enabled": valid_samples[-1].get("mark_price_enabled") if valid_samples else None,
        "final_auxiliary_symbol_count": valid_samples[-1].get("auxiliary_symbol_count") if valid_samples else None,
        "final_channel_counts": valid_samples[-1].get("channel_counts") if valid_samples else {},
        "final_feed_healthy": valid_samples[-1].get("feed_healthy") if valid_samples else None,
        "final_ws_hub_healthy": valid_samples[-1].get("ws_hub_healthy") if valid_samples else None,
        "final_feed_watch_symbols": valid_samples[-1].get("feed_watch_symbols") if valid_samples else [],
        "final_feed_watch_symbol_errors": valid_samples[-1].get("feed_watch_symbol_errors") if valid_samples else [],
        "final_fallback_reasons": valid_samples[-1].get("fallback_reasons") if valid_samples else {},
        "final_feed_last_error": valid_samples[-1].get("feed_last_error") if valid_samples else None,
    }
    return not errors, errors, summary


def run_selfcheck(
    *,
    base_url: str,
    token: str,
    duration_sec: float,
    interval_sec: float,
    min_samples: int,
    timeout: float,
    expect_mode: str,
    expect_runtime: str,
    min_ws_tick_delta: int,
    min_shadow_compare_delta: int,
    max_shadow_violation_delta: int,
    max_invalid_payload_delta: int,
    max_timestamp_regression_delta: int,
    max_shadow_stale_skip_delta: int,
    max_feed_watch_timeout_delta: int,
    max_feed_watch_error_delta: int,
    max_feed_watch_empty_delta: int,
    max_stale_symbol_count: int,
    max_price_diff_bps: float,
    max_ws_age_p95_ms: Optional[float],
    tolerate_transient: bool = False,
    max_degraded_samples: int = 0,
    max_consecutive_degraded: int = 1,
    max_degraded_oldest_age_ms: float = 60000.0,
    max_sample_errors: int = 0,
    probe_retries: int = 0,
    probe_retry_backoff: float = 0.75,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> Dict[str, Any]:
    interval = max(0.1, float(interval_sec or 1.0))
    sample_count = max(int(min_samples or 1), int(max(0.0, float(duration_sec or 0.0)) // interval) + 1)
    started_at = datetime.now(timezone.utc).isoformat()
    samples: List[Dict[str, Any]] = []
    for index in range(sample_count):
        try:
            samples.append(
                _extract_sample(
                    base_url,
                    token,
                    timeout,
                    retries=int(probe_retries),
                    retry_backoff=float(probe_retry_backoff),
                    sleep_fn=sleep_fn,
                )
            )
        except Exception as exc:
            samples.append(_failed_sample(exc))
        if index < sample_count - 1:
            sleep_fn(interval)
    finished_at = datetime.now(timezone.utc).isoformat()
    ok, errors, summary = _evaluate_samples(
        samples,
        min_samples=int(min_samples),
        expect_mode=expect_mode,
        expect_runtime=expect_runtime,
        min_ws_tick_delta=min_ws_tick_delta,
        min_shadow_compare_delta=min_shadow_compare_delta,
        max_shadow_violation_delta=max_shadow_violation_delta,
        max_invalid_payload_delta=max_invalid_payload_delta,
        max_timestamp_regression_delta=max_timestamp_regression_delta,
        max_shadow_stale_skip_delta=max_shadow_stale_skip_delta,
        max_feed_watch_timeout_delta=max_feed_watch_timeout_delta,
        max_feed_watch_error_delta=max_feed_watch_error_delta,
        max_feed_watch_empty_delta=max_feed_watch_empty_delta,
        max_stale_symbol_count=max_stale_symbol_count,
        max_price_diff_bps=max_price_diff_bps,
        max_ws_age_p95_ms=max_ws_age_p95_ms,
        tolerate_transient=tolerate_transient,
        max_degraded_samples=max_degraded_samples,
        max_consecutive_degraded=max_consecutive_degraded,
        max_degraded_oldest_age_ms=max_degraded_oldest_age_ms,
        max_sample_errors=max_sample_errors,
    )
    return {
        "overall_ok": ok,
        "base_url": base_url,
        "started_at": started_at,
        "finished_at": finished_at,
        "expect_mode": expect_mode,
        "expect_runtime": expect_runtime,
        "summary": summary,
        "errors": errors,
        "samples": samples,
    }


def _print_human_summary(report: Dict[str, Any]) -> None:
    summary = report.get("summary") or {}
    print(f"market-ws-shadow selfcheck: {'PASS' if report.get('overall_ok') else 'FAIL'}", file=sys.stderr)
    print(f"base_url: {report.get('base_url')}", file=sys.stderr)
    print(
        "summary: "
        f"samples={summary.get('sample_count')} "
        f"ws_tick_delta={summary.get('ws_tick_delta')} "
        f"shadow_compare_delta={summary.get('shadow_compare_delta')} "
        f"violations={summary.get('shadow_compare_violation_delta')} "
        f"stale_skip_delta={summary.get('shadow_compare_stale_skip_delta')} "
        f"rest_fallback_delta={summary.get('rest_fallback_delta')} "
        f"feed_watch_attempt_delta={summary.get('feed_watch_attempt_delta')} "
        f"feed_watch_error_delta={summary.get('feed_watch_error_delta')} "
        f"feed_watch_empty_delta={summary.get('feed_watch_empty_delta')} "
        f"feed_watch_timeout_delta={summary.get('feed_watch_timeout_delta')} "
        f"watch_symbol_errors={summary.get('max_feed_watch_symbol_error_count_observed')} "
        f"p99_abs_diff_bps={summary.get('p99_abs_diff_bps')} "
        f"p95_ws_age_ms={summary.get('p95_ws_age_ms')} "
        f"final_runtime={summary.get('final_trading_mode')} "
        f"final_fail_closed_for_live={summary.get('final_fail_closed_for_live')}",
        file=sys.stderr,
    )
    for error in report.get("errors") or []:
        print(f"- {error}", file=sys.stderr)


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Market WS shadow-mode long-run self-check")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--token", default=os.getenv("OPS_TOKEN", ""))
    parser.add_argument("--duration-sec", type=float, default=float(os.getenv("MARKET_WS_SHADOW_DURATION_SEC", "60")))
    parser.add_argument("--interval-sec", type=float, default=float(os.getenv("MARKET_WS_SHADOW_INTERVAL_SEC", "10")))
    parser.add_argument("--min-samples", type=int, default=int(os.getenv("MARKET_WS_SHADOW_MIN_SAMPLES", "2")))
    parser.add_argument("--timeout", type=float, default=float(os.getenv("MARKET_WS_SHADOW_TIMEOUT", "8")))
    parser.add_argument("--expect-mode", default=os.getenv("MARKET_WS_SHADOW_EXPECT_MODE", "shadow"))
    parser.add_argument("--expect-runtime", default=os.getenv("MARKET_WS_SHADOW_EXPECT_RUNTIME", "paper"))
    parser.add_argument("--min-ws-tick-delta", type=int, default=int(os.getenv("MARKET_WS_SHADOW_MIN_WS_TICK_DELTA", "1")))
    parser.add_argument("--min-shadow-compare-delta", type=int, default=int(os.getenv("MARKET_WS_SHADOW_MIN_COMPARE_DELTA", "1")))
    parser.add_argument("--max-shadow-violation-delta", type=int, default=int(os.getenv("MARKET_WS_SHADOW_MAX_VIOLATION_DELTA", "0")))
    parser.add_argument("--max-invalid-payload-delta", type=int, default=int(os.getenv("MARKET_WS_SHADOW_MAX_INVALID_DELTA", "0")))
    parser.add_argument("--max-timestamp-regression-delta", type=int, default=int(os.getenv("MARKET_WS_SHADOW_MAX_TIMESTAMP_REGRESSION_DELTA", "0")))
    parser.add_argument("--max-shadow-stale-skip-delta", type=int, default=int(os.getenv("MARKET_WS_SHADOW_MAX_STALE_SKIP_DELTA", "0")))
    parser.add_argument("--max-feed-watch-timeout-delta", type=int, default=int(os.getenv("MARKET_WS_SHADOW_MAX_FEED_TIMEOUT_DELTA", "-1")))
    parser.add_argument("--max-feed-watch-error-delta", type=int, default=int(os.getenv("MARKET_WS_SHADOW_MAX_FEED_ERROR_DELTA", "-1")))
    parser.add_argument("--max-feed-watch-empty-delta", type=int, default=int(os.getenv("MARKET_WS_SHADOW_MAX_FEED_EMPTY_DELTA", "0")))
    parser.add_argument("--max-stale-symbol-count", type=int, default=int(os.getenv("MARKET_WS_SHADOW_MAX_STALE_SYMBOL_COUNT", "0")))
    parser.add_argument("--max-price-diff-bps", type=float, default=float(os.getenv("MARKET_WS_MAX_PRICE_DIFF_BPS", "20")))
    parser.add_argument("--max-ws-age-p95-ms", type=float, default=None)
    parser.add_argument("--tolerate-transient", action="store_true", default=_as_bool(os.getenv("MARKET_WS_SHADOW_TOLERATE_TRANSIENT", "")))
    parser.add_argument("--max-degraded-samples", type=int, default=int(os.getenv("MARKET_WS_SHADOW_MAX_DEGRADED_SAMPLES", "0")))
    parser.add_argument("--max-consecutive-degraded", type=int, default=int(os.getenv("MARKET_WS_SHADOW_MAX_CONSECUTIVE_DEGRADED", "1")))
    parser.add_argument("--max-degraded-oldest-age-ms", type=float, default=float(os.getenv("MARKET_WS_SHADOW_MAX_DEGRADED_OLDEST_AGE_MS", "60000")))
    parser.add_argument(
        "--max-sample-errors",
        type=int,
        default=int(os.getenv("MARKET_WS_SHADOW_MAX_SAMPLE_ERRORS", "0")),
        help="With --tolerate-transient, allow up to N isolated (non-consecutive) probe "
             "failures (observer HTTP timeouts), still failing on any consecutive pair. "
             "Default 0 = zero-tolerance (a probe failure is not a WS-quality breach).",
    )
    parser.add_argument(
        "--probe-retries",
        type=int,
        default=int(os.getenv("MARKET_WS_SHADOW_PROBE_RETRIES", "0")),
        help="Retry each status probe this many times on transient transport errors "
             "(read/connect timeout) before recording a sample_error. Default 0 "
             "(off); the gated launchers opt in for long live observation runs.",
    )
    parser.add_argument(
        "--probe-retry-backoff",
        type=float,
        default=float(os.getenv("MARKET_WS_SHADOW_PROBE_RETRY_BACKOFF", "0.75")),
        help="Linear backoff (seconds) multiplied by attempt index between probe retries.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        report = run_selfcheck(
            base_url=str(args.base_url).strip(),
            token=str(args.token or "").strip(),
            duration_sec=float(args.duration_sec),
            interval_sec=float(args.interval_sec),
            min_samples=int(args.min_samples),
            timeout=float(args.timeout),
            expect_mode=str(args.expect_mode).strip().lower(),
            expect_runtime=str(args.expect_runtime).strip().lower(),
            min_ws_tick_delta=int(args.min_ws_tick_delta),
            min_shadow_compare_delta=int(args.min_shadow_compare_delta),
            max_shadow_violation_delta=int(args.max_shadow_violation_delta),
            max_invalid_payload_delta=int(args.max_invalid_payload_delta),
            max_timestamp_regression_delta=int(args.max_timestamp_regression_delta),
            max_shadow_stale_skip_delta=int(args.max_shadow_stale_skip_delta),
            max_feed_watch_timeout_delta=int(args.max_feed_watch_timeout_delta),
            max_feed_watch_error_delta=int(args.max_feed_watch_error_delta),
            max_feed_watch_empty_delta=int(args.max_feed_watch_empty_delta),
            max_stale_symbol_count=int(args.max_stale_symbol_count),
            max_price_diff_bps=float(args.max_price_diff_bps),
            max_ws_age_p95_ms=args.max_ws_age_p95_ms,
            tolerate_transient=bool(args.tolerate_transient),
            max_degraded_samples=int(args.max_degraded_samples),
            max_consecutive_degraded=int(args.max_consecutive_degraded),
            max_degraded_oldest_age_ms=float(args.max_degraded_oldest_age_ms),
            max_sample_errors=int(args.max_sample_errors),
            probe_retries=int(args.probe_retries),
            probe_retry_backoff=float(args.probe_retry_backoff),
        )
    except Exception as exc:
        report = {
            "overall_ok": False,
            "base_url": str(args.base_url).strip(),
            "started_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "expect_mode": str(args.expect_mode).strip().lower(),
            "expect_runtime": str(args.expect_runtime).strip().lower(),
            "summary": {},
            "errors": [str(exc)],
            "samples": [],
        }
    _print_human_summary(report)
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0 if report.get("overall_ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
