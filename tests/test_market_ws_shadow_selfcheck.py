from __future__ import annotations

import json
from urllib.parse import urlparse

import scripts.selfcheck_market_ws_shadow as shadow_check


class FakeResponse:
    def __init__(self, status_code: int, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = json.dumps(payload, ensure_ascii=False) if isinstance(payload, (dict, list)) else str(payload)

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


def _fake_request_factory(route_sequences, seen):
    counters = {key: 0 for key in route_sequences}

    def _fake_request(method, url, headers=None, timeout=None):
        parsed = urlparse(url)
        key = (method.upper(), parsed.path)
        seen.append({
            "method": method.upper(),
            "path": parsed.path,
            "headers": dict(headers or {}),
            "timeout": timeout,
        })
        sequence = route_sequences.get(key)
        if not sequence:
            raise AssertionError(f"unexpected request: {key}")
        idx = min(counters[key], len(sequence) - 1)
        counters[key] += 1
        return sequence[idx]

    return _fake_request


def _status_payload(
    *,
    ws_tick_count: int,
    compare_count: int,
    violations: int = 0,
    stale: int = 0,
    feed_attempts: int = 0,
    feed_timeouts: int = 0,
    feed_errors: int = 0,
    feed_empty: int = 0,
):
    return {
        "enabled": True,
        "configured_enabled": True,
        "mode": "shadow",
        "force_rest": False,
        "feed_present": True,
        "feed_healthy": True,
        "hub_healthy": True,
        "ws_hub_healthy": True,
        "healthy_exchanges": ["binance"],
        "ws_healthy_exchanges": ["binance"],
        "symbol_count": 1,
        "ws_symbol_count": 1,
        "stale_symbol_count": stale,
        "last_tick_age_ms": 250,
        "oldest_tick_age_ms": 250,
        "ws_tick_count": ws_tick_count,
        "rest_fallback_count": 1,
        "rest_snapshot_count": 2,
        "invalid_payload_count": 0,
        "timestamp_regression_count": 0,
        "fallback_reasons": {"hub_missing": 1},
        "shadow_compare_count": compare_count,
        "shadow_compare_violation_count": violations,
        "shadow_missing_ws_count": 0,
        "shadow_compare_stale_skip_count": 0,
        "shadow_max_abs_diff_bps": 1.5,
        "feed_watch_attempt_count": feed_attempts,
        "feed_watch_timeout_count": feed_timeouts,
        "feed_watch_error_count": feed_errors,
        "feed_watch_empty_count": feed_empty,
        "feed_last_error": None,
        "shadow_last_compare": {
            "abs_diff_bps": 1.5,
            "ws_age_ms": 200,
            "rest_age_ms": 300,
            "accepted": True,
        },
    }


def test_market_ws_shadow_selfcheck_passes_with_ws_and_compare_deltas(monkeypatch):
    routes = {
        ("GET", "/health"): [
            FakeResponse(200, {"status": "healthy"}),
            FakeResponse(200, {"status": "healthy"}),
        ],
        ("GET", "/api/status"): [
            FakeResponse(200, {"status": "running", "paper_trading": True, "trading_mode": "paper", "market_ws": {"mode": "shadow"}}),
            FakeResponse(200, {"status": "running", "paper_trading": True, "trading_mode": "paper", "market_ws": {"mode": "shadow"}}),
        ],
        ("GET", "/api/market-data/status"): [
            FakeResponse(200, _status_payload(ws_tick_count=10, compare_count=3, feed_attempts=4)),
            FakeResponse(200, _status_payload(ws_tick_count=14, compare_count=5, feed_attempts=7)),
        ],
    }
    seen = []
    monkeypatch.setattr(shadow_check.requests, "request", _fake_request_factory(routes, seen))

    report = shadow_check.run_selfcheck(
        base_url="http://127.0.0.1:8000",
        token="test-token",
        duration_sec=1,
        interval_sec=1,
        min_samples=2,
        timeout=3,
        expect_mode="shadow",
        expect_runtime="paper",
        min_ws_tick_delta=1,
        min_shadow_compare_delta=1,
        max_shadow_violation_delta=0,
        max_invalid_payload_delta=0,
        max_timestamp_regression_delta=0,
        max_shadow_stale_skip_delta=0,
        max_feed_watch_timeout_delta=-1,
        max_feed_watch_error_delta=-1,
        max_feed_watch_empty_delta=0,
        max_stale_symbol_count=0,
        max_price_diff_bps=20,
        max_ws_age_p95_ms=10_000,
        sleep_fn=lambda _seconds: None,
    )

    assert report["overall_ok"] is True
    assert report["summary"]["ws_tick_delta"] == 4
    assert report["summary"]["shadow_compare_delta"] == 2
    assert report["summary"]["feed_watch_attempt_delta"] == 3
    assert report["summary"]["p99_abs_diff_bps"] == 1.5
    assert any(call["headers"].get("X-OPS-TOKEN") == "test-token" for call in seen)


def test_market_ws_shadow_selfcheck_fails_without_ws_progress(monkeypatch):
    routes = {
        ("GET", "/health"): [FakeResponse(200, {"status": "healthy"}), FakeResponse(200, {"status": "healthy"})],
        ("GET", "/api/status"): [
            FakeResponse(200, {"status": "running", "paper_trading": True, "trading_mode": "paper", "market_ws": {"mode": "shadow"}}),
            FakeResponse(200, {"status": "running", "paper_trading": True, "trading_mode": "paper", "market_ws": {"mode": "shadow"}}),
        ],
        ("GET", "/api/market-data/status"): [
            FakeResponse(200, _status_payload(ws_tick_count=10, compare_count=3)),
            FakeResponse(200, _status_payload(ws_tick_count=10, compare_count=3)),
        ],
    }
    monkeypatch.setattr(shadow_check.requests, "request", _fake_request_factory(routes, []))

    report = shadow_check.run_selfcheck(
        base_url="http://127.0.0.1:8000",
        token="test-token",
        duration_sec=1,
        interval_sec=1,
        min_samples=2,
        timeout=3,
        expect_mode="shadow",
        expect_runtime="paper",
        min_ws_tick_delta=1,
        min_shadow_compare_delta=1,
        max_shadow_violation_delta=0,
        max_invalid_payload_delta=0,
        max_timestamp_regression_delta=0,
        max_shadow_stale_skip_delta=0,
        max_feed_watch_timeout_delta=-1,
        max_feed_watch_error_delta=-1,
        max_feed_watch_empty_delta=0,
        max_stale_symbol_count=0,
        max_price_diff_bps=20,
        max_ws_age_p95_ms=None,
        sleep_fn=lambda _seconds: None,
    )

    assert report["overall_ok"] is False
    assert "ws_tick_delta 0 < required 1" in report["errors"]
    assert "shadow_compare_delta 0 < required 1" in report["errors"]


def test_market_ws_shadow_selfcheck_treats_recovered_timeouts_as_diagnostic(monkeypatch):
    routes = {
        ("GET", "/health"): [FakeResponse(200, {"status": "healthy"}), FakeResponse(200, {"status": "healthy"})],
        ("GET", "/api/status"): [
            FakeResponse(200, {"status": "running", "paper_trading": True, "trading_mode": "paper", "market_ws": {"mode": "shadow"}}),
            FakeResponse(200, {"status": "running", "paper_trading": True, "trading_mode": "paper", "market_ws": {"mode": "shadow"}}),
        ],
        ("GET", "/api/market-data/status"): [
            FakeResponse(200, _status_payload(ws_tick_count=10, compare_count=3, feed_attempts=4, feed_timeouts=0, feed_errors=0)),
            FakeResponse(200, _status_payload(ws_tick_count=14, compare_count=5, feed_attempts=9, feed_timeouts=2, feed_errors=1)),
        ],
    }
    monkeypatch.setattr(shadow_check.requests, "request", _fake_request_factory(routes, []))

    report = shadow_check.run_selfcheck(
        base_url="http://127.0.0.1:8000",
        token="test-token",
        duration_sec=1,
        interval_sec=1,
        min_samples=2,
        timeout=3,
        expect_mode="shadow",
        expect_runtime="paper",
        min_ws_tick_delta=1,
        min_shadow_compare_delta=1,
        max_shadow_violation_delta=0,
        max_invalid_payload_delta=0,
        max_timestamp_regression_delta=0,
        max_shadow_stale_skip_delta=0,
        max_feed_watch_timeout_delta=-1,
        max_feed_watch_error_delta=-1,
        max_feed_watch_empty_delta=0,
        max_stale_symbol_count=0,
        max_price_diff_bps=20,
        max_ws_age_p95_ms=10_000,
        sleep_fn=lambda _seconds: None,
    )

    assert report["overall_ok"] is True
    assert report["summary"]["feed_watch_timeout_delta"] == 2
    assert report["summary"]["feed_watch_error_delta"] == 1


def test_market_ws_shadow_selfcheck_still_fails_empty_feed_batches(monkeypatch):
    routes = {
        ("GET", "/health"): [FakeResponse(200, {"status": "healthy"}), FakeResponse(200, {"status": "healthy"})],
        ("GET", "/api/status"): [
            FakeResponse(200, {"status": "running", "paper_trading": True, "trading_mode": "paper", "market_ws": {"mode": "shadow"}}),
            FakeResponse(200, {"status": "running", "paper_trading": True, "trading_mode": "paper", "market_ws": {"mode": "shadow"}}),
        ],
        ("GET", "/api/market-data/status"): [
            FakeResponse(200, _status_payload(ws_tick_count=10, compare_count=3, feed_attempts=4, feed_empty=0)),
            FakeResponse(200, _status_payload(ws_tick_count=14, compare_count=5, feed_attempts=9, feed_empty=1)),
        ],
    }
    monkeypatch.setattr(shadow_check.requests, "request", _fake_request_factory(routes, []))

    report = shadow_check.run_selfcheck(
        base_url="http://127.0.0.1:8000",
        token="test-token",
        duration_sec=1,
        interval_sec=1,
        min_samples=2,
        timeout=3,
        expect_mode="shadow",
        expect_runtime="paper",
        min_ws_tick_delta=1,
        min_shadow_compare_delta=1,
        max_shadow_violation_delta=0,
        max_invalid_payload_delta=0,
        max_timestamp_regression_delta=0,
        max_shadow_stale_skip_delta=0,
        max_feed_watch_timeout_delta=-1,
        max_feed_watch_error_delta=-1,
        max_feed_watch_empty_delta=0,
        max_stale_symbol_count=0,
        max_price_diff_bps=20,
        max_ws_age_p95_ms=10_000,
        sleep_fn=lambda _seconds: None,
    )

    assert report["overall_ok"] is False
    assert "feed_watch_empty_delta 1 > allowed 0" in report["errors"]


def test_market_ws_shadow_selfcheck_main_outputs_json(monkeypatch, capsys):
    routes = {
        ("GET", "/health"): [FakeResponse(200, {"status": "healthy"}), FakeResponse(200, {"status": "healthy"})],
        ("GET", "/api/status"): [
            FakeResponse(200, {"status": "running", "paper_trading": True, "trading_mode": "paper", "market_ws": {"mode": "shadow"}}),
            FakeResponse(200, {"status": "running", "paper_trading": True, "trading_mode": "paper", "market_ws": {"mode": "shadow"}}),
        ],
        ("GET", "/api/market-data/status"): [
            FakeResponse(200, _status_payload(ws_tick_count=1, compare_count=1)),
            FakeResponse(200, _status_payload(ws_tick_count=3, compare_count=2)),
        ],
    }
    monkeypatch.setattr(shadow_check.requests, "request", _fake_request_factory(routes, []))
    monkeypatch.setattr(shadow_check.time, "sleep", lambda _seconds: None)

    code = shadow_check.main([
        "--base-url",
        "http://127.0.0.1:8000",
        "--token",
        "test-token",
        "--duration-sec",
        "1",
        "--interval-sec",
        "1",
    ])

    captured = capsys.readouterr()
    assert code == 0
    assert "market-ws-shadow selfcheck: PASS" in captured.err
    payload = json.loads(captured.out)
    assert payload["overall_ok"] is True
    assert payload["summary"]["sample_count"] == 2
