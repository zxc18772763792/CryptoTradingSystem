from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.api import trading as trading_api
from web.api import trading_balances


def _build_test_client() -> TestClient:
    app = FastAPI()
    app.include_router(trading_balances.router, prefix="/api/trading")
    return TestClient(app)


def _ops_headers() -> dict[str, str]:
    return {"X-OPS-TOKEN": "test-token", "X-OPS-CALLER": "pytest"}


def _configure_common_runtime(monkeypatch) -> None:
    monkeypatch.setattr(
        trading_api.execution_engine, "get_trading_mode", lambda: "paper"
    )
    monkeypatch.setattr(trading_api.execution_engine, "is_paper_mode", lambda: True)
    monkeypatch.setattr(
        trading_api.risk_manager,
        "get_risk_report",
        lambda *args, **kwargs: {"risk_level": "low", "equity": {"current": 1234.5}},
    )
    monkeypatch.setattr(trading_api.position_manager, "get_position_count", lambda: 0)
    monkeypatch.setattr(
        trading_api.exchange_manager, "get_connected_exchanges", lambda: ["binance"]
    )
    monkeypatch.setattr(
        trading_api.strategy_manager,
        "get_dashboard_summary",
        lambda signal_limit=10: {"running_count": 0},
    )


def _altcoin_notification_rules():
    return [
        {
            "id": "altcoin-rule-1",
            "enabled": True,
            "rule_type": "altcoin_score_above",
            "params": {
                "config_key": "cfg-1",
                "exchange": "binance",
                "timeframe": "4h",
                "symbol": "AAA/USDT",
                "universe_symbols": ["AAA/USDT", "BBB/USDT"],
            },
        }
    ]


def _altcoin_notification_context():
    return {
        "scans": {
            "cfg-1": {
                "rows": [{"symbol": "AAA/USDT"}],
                "sort_indexes": {"layout": {"AAA/USDT": 1}},
            }
        }
    }


def _patch_altcoin_notification_context(monkeypatch) -> None:
    async def fake_build_altcoin_notification_context(rules):
        return _altcoin_notification_context()

    import web.api.altcoin as altcoin_api

    monkeypatch.setattr(
        altcoin_api,
        "build_altcoin_notification_context",
        fake_build_altcoin_notification_context,
    )


def test_trading_balances_route_defers_altcoin_context_notification_evaluation(
    monkeypatch,
):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    client = _build_test_client()
    _configure_common_runtime(monkeypatch)
    monkeypatch.setattr(
        trading_api.exchange_manager, "get_exchange", lambda exchange: None
    )

    async def fake_record_snapshot(**kwargs):
        return None

    async def fake_load_rule_prices():
        return {"BTC/USDT": 65000.0}

    async def fake_list_rules():
        return _altcoin_notification_rules()

    captured = {}

    async def fake_evaluate_rules(context):
        captured["context"] = context
        return {"triggered_count": 0, "triggered": []}

    monkeypatch.setattr(
        trading_api.account_snapshot_manager, "record_snapshot", fake_record_snapshot
    )
    monkeypatch.setattr(trading_api, "_load_rule_prices", fake_load_rule_prices)
    monkeypatch.setattr(trading_api.notification_manager, "list_rules", fake_list_rules)
    monkeypatch.setattr(
        trading_api.notification_manager, "evaluate_rules", fake_evaluate_rules
    )
    _patch_altcoin_notification_context(monkeypatch)

    response = client.get("/api/trading/balances", headers=_ops_headers())
    assert response.status_code == 200
    payload = response.json()

    assert payload["notifications"]["triggered_count"] == 0
    assert payload["notifications"]["eval_deferred"] is True
    assert captured == {}


def test_trading_balances_cached_response_defers_altcoin_notifications(
    monkeypatch,
):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    client = _build_test_client()
    _configure_common_runtime(monkeypatch)

    async def fake_load_rule_prices():
        return {"BTC/USDT": 65000.0}

    async def fake_list_rules():
        return _altcoin_notification_rules()

    captured = {}

    async def fake_evaluate_rules(context):
        captured["context"] = context
        return {"triggered_count": 1, "triggered": [{"rule_id": "altcoin-rule-1"}]}

    monkeypatch.setattr(trading_api, "_load_rule_prices", fake_load_rule_prices)
    monkeypatch.setattr(trading_api.notification_manager, "list_rules", fake_list_rules)
    monkeypatch.setattr(
        trading_api.notification_manager, "evaluate_rules", fake_evaluate_rules
    )
    _patch_altcoin_notification_context(monkeypatch)

    trading_balances._BALANCE_RESPONSE_CACHE["paper"] = {
        "ts": trading_balances.time.time(),
        "payload": {
            "active_account_usd_estimate": 1000.0,
            "total_usd_estimate": 1000.0,
            "risk_report": {"risk_level": "low"},
            "connected_exchanges": ["binance"],
            "notifications": {"triggered_count": 0},
        },
    }

    response = client.get("/api/trading/balances", headers=_ops_headers())
    assert response.status_code == 200
    payload = response.json()

    assert payload["from_cache"] is True
    assert payload["notifications"]["triggered_count"] == 0
    assert payload["notifications"]["eval_deferred"] is True
    assert captured == {}

    trading_balances._BALANCE_RESPONSE_CACHE.clear()


def test_trading_balances_timeout_fallback_defers_altcoin_notifications(
    monkeypatch,
):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    client = _build_test_client()
    _configure_common_runtime(monkeypatch)

    async def fake_load_rule_prices():
        return {"BTC/USDT": 65000.0}

    async def fake_list_rules():
        return _altcoin_notification_rules()

    captured = {}

    async def fake_evaluate_rules(context):
        captured["context"] = context
        return {"triggered_count": 1, "triggered": [{"rule_id": "altcoin-rule-1"}]}

    async def fake_build_all_balances_payload():
        await trading_api.asyncio.sleep(0.05)
        return {"notifications": {"triggered_count": 0}}

    monkeypatch.setattr(trading_api, "_load_rule_prices", fake_load_rule_prices)
    monkeypatch.setattr(trading_api.notification_manager, "list_rules", fake_list_rules)
    monkeypatch.setattr(
        trading_api.notification_manager, "evaluate_rules", fake_evaluate_rules
    )
    monkeypatch.setattr(
        trading_balances, "_build_all_balances_payload", fake_build_all_balances_payload
    )
    monkeypatch.setattr(trading_balances, "_BALANCE_RESPONSE_TIMEOUT_SEC", 0.01)
    _patch_altcoin_notification_context(monkeypatch)

    response = client.get("/api/trading/balances", headers=_ops_headers())
    assert response.status_code == 200
    payload = response.json()

    assert payload["stale"] is True
    assert payload["notifications"]["triggered_count"] == 0
    assert payload["notifications"]["eval_deferred"] is True
    assert captured == {}


def test_live_display_equity_uses_live_balance_snapshot_when_risk_is_guarded():
    payload = trading_balances._with_live_display_equity(
        {
            "risk_level": "low",
            "equity": {
                "current": 9898.2409,
                "daily_total_pnl_usd": -16.74,
            },
        },
        display_total_usd=5059.45,
        risk_equity_input=9898.2409,
    )

    equity = payload["equity"]
    assert equity["current"] == 5059.45
    assert equity["risk_current"] == 9898.2409
    assert equity["risk_equity_input"] == 9898.2409
    assert equity["current_source"] == "live_balance_snapshot"
    assert payload["display_account_type"] == "live"
