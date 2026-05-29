from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Optional


DEFAULT_LOG_PATTERNS = {
    "paper_false": "Paper trading mode: False",
    "paper_to_live": "scope switched: paper -> live",
    "exchange_watchdog": "exchange_watchdog",
    "gate_health": "Health check failed for gate",
}


DIAGNOSTIC_LOG_PATTERNS = {
    "connector_binance_connect_timeout": "Connector binance connect timed out",
    "exchange_manager_binance_reconnected": "exchange_manager: binance reconnected",
    "watch_tickers_timeout": "watch_tickers timeout",
    "ccxt_pro_feed_binance_watch_error": "ccxt_pro_feed[binance]: watch error",
    "coinglass_rate_limit_backoff": "coinglass: rate-limit backoff",
    "positions_live_json": "positions_live.json",
    "failed_to_persist_positions": "Failed to persist positions",
    "live_kline_fetch_timed_out": "Live kline fetch timed out",
    "get_klines_btc_15m_failed": "get_klines(BTC/USDT, 15m) failed",
    "get_ticker_call": "get_ticker(",
    "unclosed_client_session": "Unclosed client session",
    "paper_order_created": "[PAPER] Order created",
}


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _read_json(path: Path) -> Dict[str, Any]:
    text = path.read_text(encoding="utf-8-sig").strip()
    if not text:
        raise ValueError(f"empty JSON report: {path}")
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError(f"JSON report must be an object: {path}")
    return payload


def _count_log_patterns(path: Optional[Path], patterns: Dict[str, str] | None = None) -> Dict[str, int]:
    if path is None:
        return {}
    text = path.read_text(encoding="utf-8-sig", errors="replace") if path.exists() else ""
    patterns = patterns or DEFAULT_LOG_PATTERNS
    return {name: text.count(pattern) for name, pattern in patterns.items()}


def _add_error(errors: list[str], condition: bool, message: str) -> None:
    if not condition:
        errors.append(message)


def evaluate_report(
    report: Dict[str, Any],
    *,
    expect_mode: str,
    expect_runtime: str,
    min_samples: int,
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
    max_ws_age_p95_ms: float,
    log_counts: Optional[Dict[str, int]] = None,
    diagnostic_log_counts: Optional[Dict[str, int]] = None,
    max_log_count: int = 0,
) -> Dict[str, Any]:
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    errors: list[str] = []

    _add_error(errors, bool(report.get("overall_ok")) is True, "selfcheck overall_ok is not true")
    _add_error(errors, not report.get("errors"), f"selfcheck errors not empty: {report.get('errors')!r}")
    _add_error(
        errors,
        str(report.get("expect_mode") or "").strip().lower() == str(expect_mode or "").strip().lower(),
        f"expect_mode mismatch: {report.get('expect_mode')!r}",
    )
    _add_error(
        errors,
        str(report.get("expect_runtime") or "").strip().lower() == str(expect_runtime or "").strip().lower(),
        f"expect_runtime mismatch: {report.get('expect_runtime')!r}",
    )

    sample_count = _safe_int(summary.get("sample_count"))
    ws_tick_delta = _safe_int(summary.get("ws_tick_delta"))
    shadow_compare_delta = _safe_int(summary.get("shadow_compare_delta"))
    shadow_violation_delta = _safe_int(summary.get("shadow_compare_violation_delta"))
    invalid_payload_delta = _safe_int(summary.get("invalid_payload_delta"))
    timestamp_regression_delta = _safe_int(summary.get("timestamp_regression_delta"))
    shadow_stale_skip_delta = _safe_int(summary.get("shadow_compare_stale_skip_delta"))
    feed_watch_timeout_delta = _safe_int(summary.get("feed_watch_timeout_delta"))
    feed_watch_error_delta = _safe_int(summary.get("feed_watch_error_delta"))
    feed_watch_empty_delta = _safe_int(summary.get("feed_watch_empty_delta"))
    max_stale_observed = _safe_int(summary.get("max_stale_symbol_count_observed"))
    p99_abs_diff_bps = _safe_float(summary.get("p99_abs_diff_bps"))
    p95_ws_age_ms = _safe_float(summary.get("p95_ws_age_ms"))

    _add_error(errors, sample_count >= int(min_samples), f"sample_count {sample_count} < required {min_samples}")
    _add_error(errors, ws_tick_delta >= int(min_ws_tick_delta), f"ws_tick_delta {ws_tick_delta} < required {min_ws_tick_delta}")
    _add_error(
        errors,
        shadow_compare_delta >= int(min_shadow_compare_delta),
        f"shadow_compare_delta {shadow_compare_delta} < required {min_shadow_compare_delta}",
    )
    _add_error(
        errors,
        shadow_violation_delta <= int(max_shadow_violation_delta),
        f"shadow_compare_violation_delta {shadow_violation_delta} > allowed {max_shadow_violation_delta}",
    )
    _add_error(
        errors,
        invalid_payload_delta <= int(max_invalid_payload_delta),
        f"invalid_payload_delta {invalid_payload_delta} > allowed {max_invalid_payload_delta}",
    )
    _add_error(
        errors,
        timestamp_regression_delta <= int(max_timestamp_regression_delta),
        f"timestamp_regression_delta {timestamp_regression_delta} > allowed {max_timestamp_regression_delta}",
    )
    _add_error(
        errors,
        shadow_stale_skip_delta <= int(max_shadow_stale_skip_delta),
        f"shadow_compare_stale_skip_delta {shadow_stale_skip_delta} > allowed {max_shadow_stale_skip_delta}",
    )
    if int(max_feed_watch_timeout_delta) >= 0:
        _add_error(
            errors,
            feed_watch_timeout_delta <= int(max_feed_watch_timeout_delta),
            f"feed_watch_timeout_delta {feed_watch_timeout_delta} > allowed {max_feed_watch_timeout_delta}",
        )
    if int(max_feed_watch_error_delta) >= 0:
        _add_error(
            errors,
            feed_watch_error_delta <= int(max_feed_watch_error_delta),
            f"feed_watch_error_delta {feed_watch_error_delta} > allowed {max_feed_watch_error_delta}",
        )
    _add_error(
        errors,
        feed_watch_empty_delta <= int(max_feed_watch_empty_delta),
        f"feed_watch_empty_delta {feed_watch_empty_delta} > allowed {max_feed_watch_empty_delta}",
    )
    _add_error(
        errors,
        max_stale_observed <= int(max_stale_symbol_count),
        f"max_stale_symbol_count_observed {max_stale_observed} > allowed {max_stale_symbol_count}",
    )
    _add_error(errors, p99_abs_diff_bps is not None, "p99_abs_diff_bps is missing")
    if p99_abs_diff_bps is not None:
        _add_error(
            errors,
            p99_abs_diff_bps <= float(max_price_diff_bps),
            f"p99_abs_diff_bps {p99_abs_diff_bps:.4f} > allowed {float(max_price_diff_bps):.4f}",
        )
    _add_error(errors, p95_ws_age_ms is not None, "p95_ws_age_ms is missing")
    if p95_ws_age_ms is not None:
        _add_error(
            errors,
            p95_ws_age_ms <= float(max_ws_age_p95_ms),
            f"p95_ws_age_ms {p95_ws_age_ms:.1f} > allowed {float(max_ws_age_p95_ms):.1f}",
        )

    _add_error(errors, str(summary.get("final_mode") or "").lower() == str(expect_mode).lower(), "final_mode mismatch")
    final_trading_mode = str(summary.get("final_trading_mode") or "").strip().lower()
    final_paper_trading = summary.get("final_paper_trading")
    expected_runtime = str(expect_runtime or "").strip().lower()
    if expected_runtime == "paper":
        _add_error(
            errors,
            final_paper_trading is not False and (not final_trading_mode or final_trading_mode == "paper"),
            "final runtime is not paper",
        )
    elif expected_runtime == "live":
        _add_error(
            errors,
            final_paper_trading is False and final_trading_mode == "live",
            "final runtime is not live",
        )
    elif expected_runtime:
        _add_error(
            errors,
            final_trading_mode == expected_runtime,
            f"final runtime mismatch: {summary.get('final_trading_mode')!r}",
        )
    _add_error(errors, bool(summary.get("final_enabled")) is True, "final_enabled is not true")
    _add_error(errors, bool(summary.get("final_feed_healthy")) is True, "final_feed_healthy is not true")
    _add_error(errors, bool(summary.get("final_ws_hub_healthy")) is True, "final_ws_hub_healthy is not true")
    _add_error(errors, not summary.get("final_feed_last_error"), f"final_feed_last_error not empty: {summary.get('final_feed_last_error')!r}")

    log_counts = dict(log_counts or {})
    log_errors = {
        name: count
        for name, count in log_counts.items()
        if _safe_int(count) > int(max_log_count)
    }
    for name, count in sorted(log_errors.items()):
        errors.append(f"log pattern {name} count {count} > allowed {max_log_count}")

    return {
        "ok": not errors,
        "errors": errors,
        "summary": {
            "sample_count": sample_count,
            "ws_tick_delta": ws_tick_delta,
            "shadow_compare_delta": shadow_compare_delta,
            "shadow_compare_violation_delta": shadow_violation_delta,
            "invalid_payload_delta": invalid_payload_delta,
            "timestamp_regression_delta": timestamp_regression_delta,
            "shadow_compare_stale_skip_delta": shadow_stale_skip_delta,
            "feed_watch_timeout_delta": feed_watch_timeout_delta,
            "feed_watch_error_delta": feed_watch_error_delta,
            "feed_watch_empty_delta": feed_watch_empty_delta,
            "max_stale_symbol_count_observed": max_stale_observed,
            "p99_abs_diff_bps": p99_abs_diff_bps,
            "p95_ws_age_ms": p95_ws_age_ms,
            "final_mode": summary.get("final_mode"),
            "final_trading_mode": summary.get("final_trading_mode"),
            "final_paper_trading": summary.get("final_paper_trading"),
            "final_enabled": summary.get("final_enabled"),
            "final_feed_healthy": summary.get("final_feed_healthy"),
            "final_ws_hub_healthy": summary.get("final_ws_hub_healthy"),
            "final_feed_last_error": summary.get("final_feed_last_error"),
        },
        "log_counts": log_counts,
        "diagnostic_log_counts": dict(diagnostic_log_counts or {}),
    }


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate a completed market-WS shadow selfcheck report")
    parser.add_argument("--report", required=True, help="Path to selfcheck_market_ws_shadow JSON output")
    parser.add_argument("--service-err-log", default="", help="Optional service stderr log to scan for pollution")
    parser.add_argument("--expect-mode", default="shadow")
    parser.add_argument("--expect-runtime", default="paper")
    parser.add_argument("--min-samples", type=int, default=361)
    parser.add_argument("--min-ws-tick-delta", type=int, default=1)
    parser.add_argument("--min-shadow-compare-delta", type=int, default=1)
    parser.add_argument("--max-shadow-violation-delta", type=int, default=0)
    parser.add_argument("--max-invalid-payload-delta", type=int, default=0)
    parser.add_argument("--max-timestamp-regression-delta", type=int, default=0)
    parser.add_argument("--max-shadow-stale-skip-delta", type=int, default=0)
    parser.add_argument(
        "--max-feed-watch-timeout-delta",
        type=int,
        default=-1,
        help="Maximum watch_tickers timeout delta; negative means diagnostic-only.",
    )
    parser.add_argument(
        "--max-feed-watch-error-delta",
        type=int,
        default=-1,
        help="Maximum recovered watch error delta; negative means diagnostic-only.",
    )
    parser.add_argument("--max-feed-watch-empty-delta", type=int, default=0)
    parser.add_argument("--max-stale-symbol-count", type=int, default=0)
    parser.add_argument("--max-price-diff-bps", type=float, default=20.0)
    parser.add_argument("--max-ws-age-p95-ms", type=float, default=10000.0)
    parser.add_argument("--max-log-count", type=int, default=0)
    return parser.parse_args(list(argv) if argv is not None else None)


def _print_human_summary(result: Dict[str, Any], report_path: Path, service_err_log: Optional[Path]) -> None:
    summary = result.get("summary") or {}
    print(f"market-ws-shadow-report evaluation: {'PASS' if result.get('ok') else 'FAIL'}", file=sys.stderr)
    print(f"report: {report_path}", file=sys.stderr)
    if service_err_log is not None:
        print(f"service_err_log: {service_err_log}", file=sys.stderr)
    print(
        "summary: "
        f"samples={summary.get('sample_count')} "
        f"ws_tick_delta={summary.get('ws_tick_delta')} "
        f"shadow_compare_delta={summary.get('shadow_compare_delta')} "
        f"violations={summary.get('shadow_compare_violation_delta')} "
        f"feed_timeouts={summary.get('feed_watch_timeout_delta')} "
        f"feed_errors={summary.get('feed_watch_error_delta')} "
        f"p99_abs_diff_bps={summary.get('p99_abs_diff_bps')} "
        f"p95_ws_age_ms={summary.get('p95_ws_age_ms')}",
        file=sys.stderr,
    )
    for error in result.get("errors") or []:
        print(f"- {error}", file=sys.stderr)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    report_path = Path(args.report)
    service_err_log = Path(args.service_err_log) if str(args.service_err_log or "").strip() else None
    log_counts = _count_log_patterns(service_err_log)
    diagnostic_log_counts = _count_log_patterns(service_err_log, DIAGNOSTIC_LOG_PATTERNS)
    try:
        report = _read_json(report_path)
        result = evaluate_report(
            report,
            expect_mode=str(args.expect_mode),
            expect_runtime=str(args.expect_runtime),
            min_samples=int(args.min_samples),
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
            max_ws_age_p95_ms=float(args.max_ws_age_p95_ms),
            log_counts=log_counts,
            diagnostic_log_counts=diagnostic_log_counts,
            max_log_count=int(args.max_log_count),
        )
    except Exception as exc:
        result = {
            "ok": False,
            "errors": [str(exc)],
            "summary": {},
            "log_counts": log_counts,
            "diagnostic_log_counts": diagnostic_log_counts,
        }
    result["report"] = str(report_path)
    if service_err_log is not None:
        result["service_err_log"] = str(service_err_log)
    _print_human_summary(result, report_path, service_err_log)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
