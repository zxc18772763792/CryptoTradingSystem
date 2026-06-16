from __future__ import annotations

import json

import scripts.evaluate_market_ws_shadow_report as evaluator


def _report(*, expect_mode="shadow", expect_runtime="paper", **summary_overrides):
    summary = {
        "sample_count": 361,
        "ws_tick_delta": 100,
        "shadow_compare_delta": 99,
        "shadow_compare_violation_delta": 0,
        "invalid_payload_delta": 0,
        "timestamp_regression_delta": 0,
        "shadow_compare_stale_skip_delta": 0,
        "feed_watch_timeout_delta": 0,
        "feed_watch_error_delta": 0,
        "feed_watch_empty_delta": 0,
        "max_feed_watch_symbol_error_count_observed": 0,
        "max_stale_symbol_count_observed": 0,
        "p99_abs_diff_bps": 4.2,
        "p95_ws_age_ms": 300.0,
        "final_mode": "shadow",
        "final_trading_mode": "paper",
        "final_paper_trading": True,
        "final_enabled": True,
        "final_configured_enabled": True,
        "final_fail_closed_for_live": True,
        "final_mark_price_enabled": False,
        "final_auxiliary_symbol_count": 0,
        "final_channel_counts": {"ticker": 1},
        "final_feed_healthy": True,
        "final_ws_hub_healthy": True,
        "final_feed_watch_symbols": ["binance:BTC/USDT"],
        "final_feed_watch_symbol_errors": [],
        "final_feed_last_error": None,
    }
    summary.update(summary_overrides)
    return {
        "overall_ok": True,
        "expect_mode": expect_mode,
        "expect_runtime": expect_runtime,
        "summary": summary,
        "errors": [],
        "samples": [{"mode": "shadow"}, {"mode": "shadow"}],
    }


def _evaluate(report=None, **kwargs):
    params = {
        "expect_mode": "shadow",
        "expect_runtime": "paper",
        "min_samples": 361,
        "min_ws_tick_delta": 1,
        "min_shadow_compare_delta": 1,
        "max_shadow_violation_delta": 0,
        "max_invalid_payload_delta": 0,
        "max_timestamp_regression_delta": 0,
        "max_shadow_stale_skip_delta": 0,
        "max_feed_watch_timeout_delta": -1,
        "max_feed_watch_error_delta": -1,
        "max_feed_watch_empty_delta": 0,
        "max_stale_symbol_count": 0,
        "max_price_diff_bps": 20.0,
        "max_ws_age_p95_ms": 10_000.0,
    }
    params.update(kwargs)
    return evaluator.evaluate_report(report or _report(), **params)


def test_market_ws_shadow_report_eval_passes_clean_report():
    result = _evaluate(log_counts={name: 0 for name in evaluator.DEFAULT_LOG_PATTERNS})

    assert result["ok"] is True
    assert result["errors"] == []
    assert result["summary"]["sample_count"] == 361
    assert result["summary"]["p99_abs_diff_bps"] == 4.2
    assert result["summary"]["final_mark_price_enabled"] is False
    assert result["summary"]["final_auxiliary_symbol_count"] == 0
    assert result["summary"]["final_channel_counts"] == {"ticker": 1}


def test_market_ws_shadow_report_eval_fails_threshold_violations():
    report = _report(
        sample_count=300,
        ws_tick_delta=0,
        shadow_compare_violation_delta=1,
        shadow_compare_stale_skip_delta=2,
        p99_abs_diff_bps=25.0,
        final_feed_healthy=False,
    )

    result = _evaluate(report)

    assert result["ok"] is False
    assert "sample_count 300 < required 361" in result["errors"]
    assert "ws_tick_delta 0 < required 1" in result["errors"]
    assert "shadow_compare_violation_delta 1 > allowed 0" in result["errors"]
    assert "shadow_compare_stale_skip_delta 2 > allowed 0" in result["errors"]
    assert any(error.startswith("p99_abs_diff_bps") for error in result["errors"])
    assert "final_feed_healthy is not true" in result["errors"]


def test_market_ws_shadow_report_eval_allows_missing_p99_when_no_shadow_compares():
    # ui_primary/strategy_primary suppress the periodic REST reconcile while WS is
    # healthy, so a clean run legitimately has 0 shadow compares and therefore no
    # p99_abs_diff_bps. With compares not required (min=0), that must NOT fail —
    # WS price accuracy was already validated at Level-2 shadow.
    report = _report(
        expect_mode="ui_primary",
        expect_runtime="live",
        final_mode="ui_primary",
        final_trading_mode="live",
        final_paper_trading=False,
        shadow_compare_delta=0,
        p99_abs_diff_bps=None,
    )

    result = _evaluate(
        report,
        expect_mode="ui_primary",
        expect_runtime="live",
        min_shadow_compare_delta=0,
        log_counts={name: 0 for name in evaluator.DEFAULT_LOG_PATTERNS},
    )

    assert result["ok"] is True, result["errors"]
    assert not any("p99_abs_diff_bps is missing" in error for error in result["errors"])


def test_market_ws_shadow_report_eval_still_requires_p99_when_compares_present():
    # Regression guard: when shadow compares actually ran, a missing p99 is still
    # a hard failure (the accuracy signal must exist when it should).
    report = _report(shadow_compare_delta=50, p99_abs_diff_bps=None)

    result = _evaluate(report, min_shadow_compare_delta=1)

    assert result["ok"] is False
    assert any("p99_abs_diff_bps is missing" in error for error in result["errors"])


def test_market_ws_shadow_report_eval_preserves_mark_price_observability():
    result = _evaluate(
        report=_report(
            final_mark_price_enabled=True,
            final_auxiliary_symbol_count=2,
            final_channel_counts={"mark_price": 2, "ticker": 2},
        )
    )

    assert result["ok"] is True
    assert result["summary"]["final_mark_price_enabled"] is True
    assert result["summary"]["final_auxiliary_symbol_count"] == 2
    assert result["summary"]["final_channel_counts"] == {"mark_price": 2, "ticker": 2}


def test_market_ws_shadow_report_eval_fails_log_pollution():
    assert "market_ws_timeout" not in evaluator.DEFAULT_LOG_PATTERNS

    result = _evaluate(log_counts={"paper_false": 2})

    assert result["ok"] is False
    assert "log pattern paper_false count 2 > allowed 0" in result["errors"]
    assert not any("market_ws_timeout" in error for error in result["errors"])


def test_market_ws_shadow_report_eval_treats_recovered_watch_errors_as_diagnostic():
    report = _report(feed_watch_timeout_delta=3, feed_watch_error_delta=1)

    result = _evaluate(report)

    assert result["ok"] is True
    assert result["errors"] == []


def test_market_ws_shadow_report_eval_fails_live_expected_with_paper_summary():
    result = _evaluate(
        expect_runtime="live",
        report=_report(expect_runtime="live"),
    )

    assert result["ok"] is False
    assert "final runtime is not live" in result["errors"]


def test_market_ws_shadow_report_eval_passes_live_runtime_summary():
    result = _evaluate(
        expect_runtime="live",
        report=_report(
            expect_runtime="live",
            final_trading_mode="live",
            final_paper_trading=False,
        ),
    )

    assert result["ok"] is True
    assert result["errors"] == []
    assert result["summary"]["final_trading_mode"] == "live"
    assert result["summary"]["final_paper_trading"] is False


def test_market_ws_shadow_report_eval_fails_live_runtime_without_fail_closed():
    result = _evaluate(
        expect_runtime="live",
        report=_report(
            expect_runtime="live",
            final_trading_mode="live",
            final_paper_trading=False,
            final_fail_closed_for_live=False,
        ),
    )

    assert result["ok"] is False
    assert "final_fail_closed_for_live is not true" in result["errors"]
    assert result["summary"]["final_fail_closed_for_live"] is False


def test_market_ws_shadow_report_eval_fails_disabled_configured_stream():
    result = _evaluate(
        report=_report(final_configured_enabled=False),
    )

    assert result["ok"] is False
    assert "final_configured_enabled is not true" in result["errors"]
    assert result["summary"]["final_configured_enabled"] is False


def test_market_ws_shadow_report_eval_keeps_legacy_configured_enabled_compatible():
    report = _report()
    report["summary"].pop("final_configured_enabled")

    result = _evaluate(report=report)

    assert result["ok"] is True
    assert result["errors"] == []
    assert result["summary"]["final_configured_enabled"] is None


def test_market_ws_shadow_report_eval_keeps_legacy_paper_summary_compatible_by_default():
    result = _evaluate(
        report=_report(final_trading_mode=None, final_paper_trading=None),
    )

    assert result["ok"] is True
    assert result["errors"] == []


def test_market_ws_shadow_report_eval_can_require_exact_final_runtime_fields():
    result = _evaluate(
        report=_report(final_trading_mode=None, final_paper_trading=None),
        require_final_runtime_fields=True,
    )

    assert result["ok"] is False
    assert "final_trading_mode is missing" in result["errors"]
    assert "final_paper_trading is missing" in result["errors"]
    assert "final runtime is not paper" in result["errors"]


def test_market_ws_shadow_report_eval_exposes_diagnostic_log_counts_without_failing():
    result = _evaluate(
        diagnostic_log_counts={
            "unclosed_client_session": 1,
            "positions_live_json": 2,
            "get_ticker_call": 3,
        },
        log_counts={name: 0 for name in evaluator.DEFAULT_LOG_PATTERNS},
    )

    assert result["ok"] is True
    assert result["errors"] == []
    assert result["diagnostic_log_counts"]["unclosed_client_session"] == 1
    assert result["diagnostic_log_counts"]["positions_live_json"] == 2
    assert result["diagnostic_log_counts"]["get_ticker_call"] == 3


def test_market_ws_shadow_report_eval_can_strictly_fail_watch_errors_when_requested():
    report = _report(feed_watch_timeout_delta=1, feed_watch_error_delta=1)

    result = _evaluate(
        report,
        max_feed_watch_timeout_delta=0,
        max_feed_watch_error_delta=0,
    )

    assert result["ok"] is False
    assert "feed_watch_timeout_delta 1 > allowed 0" in result["errors"]
    assert "feed_watch_error_delta 1 > allowed 0" in result["errors"]


def test_market_ws_shadow_report_eval_fails_watched_symbol_errors():
    report = _report(
        max_feed_watch_symbol_error_count_observed=2,
        final_feed_watch_symbol_errors=[
            "market WS watched symbol is not WS sourced: binance:BTC/USDT source='rest_snapshot'",
        ],
    )

    result = _evaluate(report)

    assert result["ok"] is False
    assert "max_feed_watch_symbol_error_count_observed 2 > allowed 0" in result["errors"]
    assert result["summary"]["final_feed_watch_symbol_errors"] == [
        "market WS watched symbol is not WS sourced: binance:BTC/USDT source='rest_snapshot'",
    ]


def test_market_ws_shadow_report_eval_main_outputs_json(tmp_path, capsys):
    report_path = tmp_path / "shadow.json"
    log_path = tmp_path / "service.err.log"
    report_path.write_text(json.dumps(_report(), ensure_ascii=False), encoding="utf-8")
    log_path.write_text(
        "\n".join(
            [
                "ordinary log line",
                "Unclosed client session",
                "positions_live.json Permission denied",
                "get_ticker(BTC/USDT) failed",
            ]
        ),
        encoding="utf-8",
    )

    code = evaluator.main([
        "--report",
        str(report_path),
        "--service-err-log",
        str(log_path),
    ])

    captured = capsys.readouterr()
    assert code == 0
    assert "market-ws-shadow-report evaluation: PASS" in captured.err
    payload = json.loads(captured.out)
    assert payload["ok"] is True
    assert payload["report"] == str(report_path)
    assert payload["diagnostic_log_counts"]["unclosed_client_session"] == 1
    assert payload["diagnostic_log_counts"]["positions_live_json"] == 1
    assert payload["diagnostic_log_counts"]["get_ticker_call"] == 1


def test_market_ws_shadow_report_eval_main_counts_logs_for_empty_report(tmp_path, capsys):
    report_path = tmp_path / "shadow.json"
    log_path = tmp_path / "service.err.log"
    report_path.write_text("", encoding="utf-8")
    log_path.write_text(
        "\n".join(
            [
                "Paper trading mode: False",
                "Unclosed client session",
                "positions_live.json Permission denied",
            ]
        ),
        encoding="utf-8",
    )

    code = evaluator.main([
        "--report",
        str(report_path),
        "--service-err-log",
        str(log_path),
    ])

    captured = capsys.readouterr()
    assert code == 1
    assert "empty JSON report" in captured.err
    payload = json.loads(captured.out)
    assert payload["ok"] is False
    assert payload["log_counts"]["paper_false"] == 1
    assert payload["diagnostic_log_counts"]["unclosed_client_session"] == 1
    assert payload["diagnostic_log_counts"]["positions_live_json"] == 1


def test_market_ws_shadow_report_eval_demotes_paper_pollution_keys_for_live_runtime(tmp_path, capsys):
    """In a LIVE runtime, `Paper trading mode: False`, paper<->live scope flips
    and the (intentionally enabled) exchange watchdog are normal operation —
    they must be reported as diagnostics, not hard log-pollution failures."""
    report_path = tmp_path / "live_shadow.json"
    log_path = tmp_path / "service.err.log"
    report = _report(
        expect_runtime="live",
        final_trading_mode="live",
        final_paper_trading=False,
    )
    report_path.write_text(json.dumps(report, ensure_ascii=False), encoding="utf-8")
    log_path.write_text(
        "\n".join(
            [
                "Paper trading mode: False",
                "Risk manager scope switched: paper -> live",
                "exchange_watchdog: binance unhealthy, attempting reconnect",
            ]
        ),
        encoding="utf-8",
    )

    code = evaluator.main([
        "--report",
        str(report_path),
        "--service-err-log",
        str(log_path),
        "--expect-runtime",
        "live",
    ])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0, captured.err
    assert payload["ok"] is True
    assert "paper_false" not in payload["log_counts"]
    assert payload["diagnostic_log_counts"]["paper_false"] == 1
    assert payload["diagnostic_log_counts"]["paper_to_live"] == 1
    assert payload["diagnostic_log_counts"]["exchange_watchdog"] == 1


def test_market_ws_shadow_report_eval_keeps_paper_pollution_hard_for_paper_runtime(tmp_path, capsys):
    report_path = tmp_path / "paper_shadow.json"
    log_path = tmp_path / "service.err.log"
    report_path.write_text(json.dumps(_report(), ensure_ascii=False), encoding="utf-8")
    log_path.write_text("Paper trading mode: False\n", encoding="utf-8")

    code = evaluator.main([
        "--report",
        str(report_path),
        "--service-err-log",
        str(log_path),
    ])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 1
    assert payload["ok"] is False
    assert any("paper_false" in err for err in payload["errors"])
