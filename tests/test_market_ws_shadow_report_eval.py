from __future__ import annotations

import json

import scripts.evaluate_market_ws_shadow_report as evaluator


def _report(**summary_overrides):
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
        "max_stale_symbol_count_observed": 0,
        "p99_abs_diff_bps": 4.2,
        "p95_ws_age_ms": 300.0,
        "final_mode": "shadow",
        "final_enabled": True,
        "final_feed_healthy": True,
        "final_ws_hub_healthy": True,
        "final_feed_last_error": None,
    }
    summary.update(summary_overrides)
    return {
        "overall_ok": True,
        "expect_mode": "shadow",
        "expect_runtime": "paper",
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
