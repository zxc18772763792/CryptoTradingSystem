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


def _request_json(base_url: str, token: str, path: str, timeout: float) -> Dict[str, Any]:
    headers = {"X-OPS-CALLER": "selfcheck_market_ws_shadow"}
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


def _extract_sample(base_url: str, token: str, timeout: float) -> Dict[str, Any]:
    sampled_at = datetime.now(timezone.utc).isoformat()
    health = _request_json(base_url, token, "/health", timeout)
    status = _request_json(base_url, token, "/api/status", timeout)
    market = _request_json(base_url, token, "/api/market-data/status", timeout)
    status_body = status["body"]
    market_body = market["body"]
    status_market_ws = status_body.get("market_ws") if isinstance(status_body.get("market_ws"), dict) else {}
    market_ws = market_body if isinstance(market_body, dict) else {}
    shadow_last = market_ws.get("shadow_last_compare") if isinstance(market_ws.get("shadow_last_compare"), dict) else {}
    return {
        "sampled_at": sampled_at,
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
        "feed_present": market_ws.get("feed_present"),
        "feed_healthy": market_ws.get("feed_healthy"),
        "hub_healthy": market_ws.get("hub_healthy"),
        "ws_hub_healthy": market_ws.get("ws_hub_healthy"),
        "healthy_exchanges": market_ws.get("healthy_exchanges") or [],
        "ws_healthy_exchanges": market_ws.get("ws_healthy_exchanges") or [],
        "symbol_count": _as_int(market_ws.get("symbol_count")),
        "ws_symbol_count": _as_int(market_ws.get("ws_symbol_count")),
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
    }


def _delta(samples: List[Dict[str, Any]], key: str) -> int:
    if not samples:
        return 0
    return _as_int(samples[-1].get(key)) - _as_int(samples[0].get(key))


def _evaluate_samples(
    samples: List[Dict[str, Any]],
    *,
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
) -> Tuple[bool, List[str], Dict[str, Any]]:
    errors: List[str] = []
    mode = str(expect_mode or "shadow").strip().lower()
    runtime = str(expect_runtime or "paper").strip().lower()
    for index, sample in enumerate(samples):
        prefix = f"sample[{index}]"
        if sample.get("health_status_code") != 200 or str(sample.get("health_status")).lower() not in {"healthy", "running"}:
            errors.append(f"{prefix}: /health not healthy")
        if sample.get("status_status_code") != 200 or str(sample.get("app_status")).lower() != "running":
            errors.append(f"{prefix}: /api/status not running")
        if sample.get("market_status_code") != 200:
            errors.append(f"{prefix}: /api/market-data/status failed")
        if runtime == "paper" and not _as_bool(sample.get("paper_trading")):
            errors.append(f"{prefix}: expected paper runtime")
        if str(sample.get("mode") or "").strip().lower() != mode:
            errors.append(f"{prefix}: expected market ws mode {mode}, got {sample.get('mode')!r}")
        if mode != "off" and not _as_bool(sample.get("enabled")):
            errors.append(f"{prefix}: expected market ws enabled")
        if _as_bool(sample.get("force_rest")):
            errors.append(f"{prefix}: force_rest is true")
        if mode != "strategy_primary" and str(sample.get("mode") or "").strip().lower() == "strategy_primary":
            errors.append(f"{prefix}: strategy_primary must not be enabled during shadow check")
        stale_for_check = (
            _as_int(sample.get("ws_stale_symbol_count"))
            if sample.get("ws_stale_symbol_count") is not None
            else _as_int(sample.get("stale_symbol_count"))
        )
        if stale_for_check > max_stale_symbol_count:
            errors.append(f"{prefix}: ws_stale_symbol_count>{max_stale_symbol_count}")

    ws_tick_delta = _delta(samples, "ws_tick_count")
    shadow_compare_delta = _delta(samples, "shadow_compare_count")
    shadow_violation_delta = _delta(samples, "shadow_compare_violation_count")
    invalid_delta = _delta(samples, "invalid_payload_count")
    timestamp_regression_delta = _delta(samples, "timestamp_regression_count")
    shadow_stale_skip_delta = _delta(samples, "shadow_compare_stale_skip_count")
    feed_watch_timeout_delta = _delta(samples, "feed_watch_timeout_count")
    feed_watch_error_delta = _delta(samples, "feed_watch_error_count")
    feed_watch_empty_delta = _delta(samples, "feed_watch_empty_count")
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
        for value in (_as_float(sample.get("shadow_last_abs_diff_bps")) for sample in samples)
        if value is not None
    ]
    ws_age_values = [
        float(value)
        for value in (_as_float(sample.get("shadow_last_ws_age_ms")) for sample in samples)
        if value is not None
    ]
    tick_age_values = [
        float(value)
        for value in (_as_float(sample.get("last_tick_age_ms")) for sample in samples)
        if value is not None
    ]
    diff_p99 = _percentile(diff_values, 99.0)
    ws_age_p95 = _percentile(ws_age_values or tick_age_values, 95.0)
    if diff_p99 is not None and diff_p99 > float(max_price_diff_bps):
        errors.append(f"p99_abs_diff_bps {diff_p99:.4f} > allowed {max_price_diff_bps:.4f}")
    if max_ws_age_p95_ms is not None and ws_age_p95 is not None and ws_age_p95 > float(max_ws_age_p95_ms):
        errors.append(f"p95_ws_age_ms {ws_age_p95:.1f} > allowed {float(max_ws_age_p95_ms):.1f}")

    summary = {
        "sample_count": len(samples),
        "ws_tick_delta": ws_tick_delta,
        "shadow_compare_delta": shadow_compare_delta,
        "shadow_compare_violation_delta": shadow_violation_delta,
        "invalid_payload_delta": invalid_delta,
        "timestamp_regression_delta": timestamp_regression_delta,
        "shadow_compare_stale_skip_delta": shadow_stale_skip_delta,
        "rest_fallback_delta": _delta(samples, "rest_fallback_count"),
        "rest_snapshot_delta": _delta(samples, "rest_snapshot_count"),
        "shadow_missing_ws_delta": _delta(samples, "shadow_missing_ws_count"),
        "feed_watch_attempt_delta": _delta(samples, "feed_watch_attempt_count"),
        "feed_watch_timeout_delta": feed_watch_timeout_delta,
        "feed_watch_error_delta": feed_watch_error_delta,
        "feed_watch_empty_delta": feed_watch_empty_delta,
        "p99_abs_diff_bps": diff_p99,
        "p95_ws_age_ms": ws_age_p95,
        "max_stale_symbol_count_observed": max(
            (
                _as_int(sample.get("ws_stale_symbol_count"))
                if sample.get("ws_stale_symbol_count") is not None
                else _as_int(sample.get("stale_symbol_count"))
                for sample in samples
            ),
            default=0,
        ),
        "final_mode": samples[-1].get("mode") if samples else None,
        "final_enabled": samples[-1].get("enabled") if samples else None,
        "final_feed_healthy": samples[-1].get("feed_healthy") if samples else None,
        "final_ws_hub_healthy": samples[-1].get("ws_hub_healthy") if samples else None,
        "final_fallback_reasons": samples[-1].get("fallback_reasons") if samples else {},
        "final_feed_last_error": samples[-1].get("feed_last_error") if samples else None,
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
    sleep_fn: Callable[[float], None] = time.sleep,
) -> Dict[str, Any]:
    interval = max(0.1, float(interval_sec or 1.0))
    sample_count = max(int(min_samples or 1), int(max(0.0, float(duration_sec or 0.0)) // interval) + 1)
    started_at = datetime.now(timezone.utc).isoformat()
    samples: List[Dict[str, Any]] = []
    for index in range(sample_count):
        samples.append(_extract_sample(base_url, token, timeout))
        if index < sample_count - 1:
            sleep_fn(interval)
    finished_at = datetime.now(timezone.utc).isoformat()
    ok, errors, summary = _evaluate_samples(
        samples,
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
        f"p99_abs_diff_bps={summary.get('p99_abs_diff_bps')} "
        f"p95_ws_age_ms={summary.get('p95_ws_age_ms')}",
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
