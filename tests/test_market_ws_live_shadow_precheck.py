from __future__ import annotations

import json
from urllib.parse import urlparse

import scripts.precheck_market_ws_live_shadow as precheck


class FakeResponse:
    def __init__(self, status_code: int, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = json.dumps(payload, ensure_ascii=False) if isinstance(payload, (dict, list)) else str(payload)

    def json(self):
        return self._payload


def _status_payload(*, trading_mode="live", paper_trading=False, market_ws=None):
    return {
        "status": "running",
        "trading_mode": trading_mode,
        "paper_trading": paper_trading,
        "market_ws": market_ws or _market_ws_payload(),
    }


def _market_ws_payload(**overrides):
    payload = {
        "enabled": True,
        "configured_enabled": True,
        "mode": "shadow",
        "force_rest": False,
        "fail_closed_for_live": True,
        "mark_price_enabled": False,
        "auxiliary_symbol_count": 0,
        "channel_counts": {"ticker": 2},
        "feed_healthy": True,
        "feed_status": {
            "exchanges": {
                "binance": {
                    "last_symbols": ["BTC/USDT", "ETH/USDT"],
                }
            }
        },
        "ws_hub_healthy": True,
        "feed_last_error": None,
        "last_tick_age_ms": 500,
        "ws_stale_symbol_count": 0,
        "invalid_payload_count": 0,
        "timestamp_regression_count": 0,
        "feed_watch_empty_count": 0,
        "shadow_compare_violation_count": 0,
        "shadow_compare_stale_skip_count": 0,
        "symbols": {
            "binance": {
                "BTC/USDT": {
                    "source": "ws",
                    "is_stale": False,
                    "age_ms": 450,
                },
                "ETH/USDT": {
                    "source": "ws",
                    "is_stale": False,
                    "age_ms": 500,
                },
            }
        },
    }
    payload.update(overrides)
    return payload


def test_live_shadow_precheck_passes_expected_configuration():
    ok, errors, summary = precheck.evaluate_precheck(
        _status_payload(),
        _market_ws_payload(),
    )

    assert ok is True
    assert errors == []
    assert summary["trading_mode"] == "live"
    assert summary["market_ws_mode"] == "shadow"
    assert summary["market_ws_fail_closed_for_live"] is True
    assert summary["market_ws_mark_price_enabled"] is False
    assert summary["market_ws_auxiliary_symbol_count"] == 0
    assert summary["market_ws_channel_counts"] == {"ticker": 2}


def test_live_shadow_precheck_preserves_mark_price_observability():
    ok, errors, summary = precheck.evaluate_precheck(
        _status_payload(),
        _market_ws_payload(
            mark_price_enabled=True,
            auxiliary_symbol_count=2,
            channel_counts={"mark_price": 2, "ticker": 2},
        ),
    )

    assert ok is True
    assert errors == []
    assert summary["market_ws_mark_price_enabled"] is True
    assert summary["market_ws_auxiliary_symbol_count"] == 2
    assert summary["market_ws_channel_counts"] == {"mark_price": 2, "ticker": 2}


def test_live_shadow_precheck_rejects_paper_runtime():
    ok, errors, summary = precheck.evaluate_precheck(
        _status_payload(trading_mode="paper", paper_trading=True),
        _market_ws_payload(),
    )

    assert ok is False
    assert "runtime is not live" in errors
    assert summary["paper_trading"] is True


def test_live_shadow_precheck_prefers_market_data_status_over_status_cache():
    ok, errors, summary = precheck.evaluate_precheck(
        _status_payload(market_ws=_market_ws_payload(mode="ui_primary", force_rest=True)),
        _market_ws_payload(mode="shadow", force_rest=False),
    )

    assert ok is True
    assert errors == []
    assert summary["market_ws_mode"] == "shadow"
    assert summary["market_ws_force_rest"] is False


def test_live_shadow_precheck_rejects_primary_modes_and_force_rest():
    ok, errors, summary = precheck.evaluate_precheck(
        _status_payload(market_ws=_market_ws_payload(mode="ui_primary", force_rest=True)),
        _market_ws_payload(mode="ui_primary", force_rest=True),
    )

    assert ok is False
    assert "MARKET_WS_MODE must be shadow for live shadow, got 'ui_primary'" in errors
    assert "ui_primary must not be enabled before live shadow passes" in errors
    assert "MARKET_WS_FORCE_REST must be false for live shadow" in errors
    assert summary["market_ws_mode"] == "ui_primary"


def test_live_shadow_precheck_rejects_disabled_fail_closed():
    ok, errors, _summary = precheck.evaluate_precheck(
        _status_payload(market_ws=_market_ws_payload(fail_closed_for_live=False)),
        _market_ws_payload(fail_closed_for_live=False),
    )

    assert ok is False
    assert "MARKET_WS_FAIL_CLOSED_FOR_LIVE must be true" in errors


def test_live_shadow_precheck_rejects_missing_fail_closed_status():
    payload = _market_ws_payload()
    payload.pop("fail_closed_for_live")

    ok, errors, summary = precheck.evaluate_precheck(
        _status_payload(market_ws=payload),
        payload,
    )

    assert ok is False
    assert "market WS status is missing fail_closed_for_live" in errors
    assert summary["market_ws_fail_closed_for_live"] is None


def test_live_shadow_precheck_rejects_unhealthy_market_ws_runtime():
    ok, errors, summary = precheck.evaluate_precheck(
        _status_payload(),
        _market_ws_payload(
            feed_healthy=False,
            ws_hub_healthy=False,
            feed_last_error="watch error",
            last_tick_age_ms=20000,
            ws_stale_symbol_count=1,
            invalid_payload_count=1,
            timestamp_regression_count=1,
            feed_watch_empty_count=1,
            shadow_compare_violation_count=1,
            shadow_compare_stale_skip_count=1,
        ),
    )

    assert ok is False
    assert "market WS feed is not healthy" in errors
    assert "market WS hub is not healthy" in errors
    assert "market WS feed_last_error is not empty: 'watch error'" in errors
    assert "market WS has stale symbols: 1" in errors
    assert "market WS last tick age 20000.0 ms exceeds 10000.0 ms" in errors
    assert "market WS invalid payload count is 1" in errors
    assert "market WS timestamp regression count is 1" in errors
    assert "market WS feed empty watch count is 1" in errors
    assert "market WS shadow compare violation count is 1" in errors
    assert "market WS shadow stale skip count is 1" in errors
    assert summary["market_ws_feed_healthy"] is False
    assert summary["market_ws_ws_hub_healthy"] is False


def test_live_shadow_precheck_rejects_unhealthy_watched_symbols():
    payload = _market_ws_payload(
        feed_status={
            "exchanges": {
                "binance": {
                    "last_symbols": ["BTC/USDT", "ETH/USDT:USDT", "SOL/USDT"],
                }
            }
        },
        symbols={
            "binance": {
                "BTC/USDT": {
                    "source": "rest_snapshot",
                    "is_stale": False,
                    "age_ms": 400,
                },
                "ETH/USDT": {
                    "source": "ws",
                    "is_stale": True,
                    "age_ms": 20_000,
                },
            }
        },
    )

    ok, errors, summary = precheck.evaluate_precheck(
        _status_payload(),
        payload,
    )

    assert ok is False
    assert "market WS watched symbol is not WS sourced: binance:BTC/USDT source='rest_snapshot'" in errors
    assert "market WS watched symbol is stale: binance:ETH/USDT:USDT" in errors
    assert "market WS watched symbol age 20000.0 ms exceeds 10000.0 ms: binance:ETH/USDT:USDT" in errors
    assert "market WS missing watched symbol tick: binance:SOL/USDT" in errors
    assert summary["market_ws_feed_watch_symbol_count"] == 3
    assert summary["market_ws_feed_watch_symbol_error_count"] == 4


def test_live_shadow_precheck_main_calls_status_endpoints(monkeypatch, capsys):
    seen = []

    def fake_request(method, url, headers=None, timeout=None):
        parsed = urlparse(url)
        seen.append((method, parsed.path, dict(headers or {}), timeout))
        if parsed.path == "/api/status":
            return FakeResponse(200, _status_payload())
        if parsed.path == "/api/market-data/status":
            return FakeResponse(200, _market_ws_payload())
        raise AssertionError(f"unexpected path: {parsed.path}")

    monkeypatch.setattr(precheck.requests, "request", fake_request)

    code = precheck.main(["--base-url", "http://127.0.0.1:8012", "--token", "tok", "--timeout", "2"])

    captured = capsys.readouterr()
    assert code == 0
    assert "market-ws-live-shadow precheck: PASS" in captured.err
    payload = json.loads(captured.out)
    assert payload["ok"] is True
    assert {item[1] for item in seen} == {"/api/status", "/api/market-data/status"}
    assert all(item[2].get("X-OPS-TOKEN") == "tok" for item in seen)
