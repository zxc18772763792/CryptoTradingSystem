from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.api import trading as trading_api
from web.api import trading_analytics


def test_pnl_heatmap_route_passes_mode(monkeypatch):
    app = FastAPI()
    app.include_router(trading_analytics.router, prefix="/api/trading")
    client = TestClient(app)

    async def fake_heatmap(*, days: int, bucket: str, mode: str | None):
        return {
            "days": days,
            "bucket": bucket,
            "mode": mode,
            "times": [],
            "symbols": [],
            "matrix": [],
            "trade_count": 0,
            "display_mode": "empty",
            "value_title": "PnL",
            "value_hover": "PnL",
            "note": "ok",
        }

    monkeypatch.setattr(trading_api, "get_pnl_heatmap", fake_heatmap)

    response = client.get("/api/trading/pnl/heatmap?days=14&bucket=hour&mode=live")
    assert response.status_code == 200
    assert response.json()["days"] == 14
    assert response.json()["bucket"] == "hour"
    assert response.json()["mode"] == "live"


def test_get_pnl_heatmap_normalizes_mixed_timestamp_awareness(monkeypatch):
    monkeypatch.setattr(trading_api.execution_engine, "get_trading_mode", lambda: "paper")
    monkeypatch.setattr(trading_api.execution_engine, "is_paper_mode", lambda: True)
    _day = (datetime.now(timezone.utc) - timedelta(days=3)).replace(
        hour=10, minute=0, second=0, microsecond=0
    )
    monkeypatch.setattr(
        trading_api.position_manager,
        "get_closed_positions",
        lambda limit=20000, scope=None: [
            SimpleNamespace(
                updated_at=_day.replace(tzinfo=None),
                opened_at=None,
                quantity=1.0,
                entry_price=100.0,
                current_price=101.0,
                symbol="BTC/USDT",
                strategy="paper_alpha",
                realized_pnl=5.0,
            )
        ],
    )
    monkeypatch.setattr(
        trading_api.risk_manager,
        "get_trade_history",
        lambda limit=30000, scope=None: [
            {
                "timestamp": _day.replace(hour=11),
                "symbol": "BTC/USDT",
                "strategy": "paper_beta",
                "pnl": 1.0,
                "notional": 100.0,
            }
        ],
    )
    monkeypatch.setattr(trading_api.order_manager, "get_recent_orders", lambda limit=5000: [])
    monkeypatch.setattr(trading_api.audit_logger, "list_logs", AsyncMock(return_value=[]))

    payload = asyncio.run(trading_api.get_pnl_heatmap(days=30, bucket="day", mode="paper"))

    assert payload["mode"] == "paper"
    assert payload["trade_count"] == 2
    assert payload["symbols"] == ["BTC/USDT"]
    assert payload["matrix"] == [[6.0]]


def test_get_pnl_heatmap_separates_paper_and_live_fallback_orders(monkeypatch):
    monkeypatch.setattr(trading_api.execution_engine, "get_trading_mode", lambda: "paper")
    monkeypatch.setattr(trading_api.execution_engine, "is_paper_mode", lambda: True)
    monkeypatch.setattr(
        trading_api.position_manager,
        "get_closed_positions",
        lambda limit=20000, scope=None: [],
    )
    monkeypatch.setattr(
        trading_api.risk_manager,
        "get_trade_history",
        lambda limit=30000, scope=None: [],
    )
    monkeypatch.setattr(trading_api.audit_logger, "list_logs", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        trading_api,
        "_fetch_binance_realized_pnl_income",
        AsyncMock(return_value=[]),
    )

    _recent2 = datetime.now(timezone.utc) - timedelta(days=2)
    orders = [
        SimpleNamespace(
            id="paper_1",
            timestamp=_recent2.replace(tzinfo=None),
            status=SimpleNamespace(value="closed"),
            filled=2.0,
            amount=2.0,
            price=10.0,
            symbol="ETH/USDT",
            side=SimpleNamespace(value="buy"),
            account_id="paper_acc",
        ),
        SimpleNamespace(
            id="live_1",
            timestamp=_recent2 + timedelta(hours=1),
            status=SimpleNamespace(value="filled"),
            filled=1.0,
            amount=1.0,
            price=20.0,
            symbol="SOL/USDT",
            side=SimpleNamespace(value="sell"),
            account_id="live_acc",
        ),
    ]
    monkeypatch.setattr(trading_api.order_manager, "get_recent_orders", lambda limit=5000: orders)
    monkeypatch.setattr(
        trading_api.order_manager,
        "get_order_metadata",
        lambda order_id: {"mode": "paper"} if order_id == "paper_1" else {"mode": "live"},
    )

    paper_payload = asyncio.run(
        trading_api.get_pnl_heatmap(days=30, bucket="day", mode="paper")
    )
    live_payload = asyncio.run(
        trading_api.get_pnl_heatmap(days=30, bucket="day", mode="live")
    )

    assert paper_payload["display_mode"] == "cashflow_proxy"
    assert paper_payload["symbols"] == ["ETH/USDT"]
    assert paper_payload["matrix"] == [[-20.0]]
    assert live_payload["display_mode"] == "cashflow_proxy"
    assert live_payload["symbols"] == ["SOL/USDT"]
    assert live_payload["matrix"] == [[20.0]]


def test_get_pnl_heatmap_uses_live_income_only_for_live_mode(monkeypatch):
    monkeypatch.setattr(trading_api.execution_engine, "get_trading_mode", lambda: "paper")
    monkeypatch.setattr(trading_api.execution_engine, "is_paper_mode", lambda: True)
    monkeypatch.setattr(
        trading_api.position_manager,
        "get_closed_positions",
        lambda limit=20000, scope=None: [],
    )
    monkeypatch.setattr(
        trading_api.risk_manager,
        "get_trade_history",
        lambda limit=30000, scope=None: [],
    )
    monkeypatch.setattr(trading_api.audit_logger, "list_logs", AsyncMock(return_value=[]))
    monkeypatch.setattr(trading_api.order_manager, "get_recent_orders", lambda limit=5000: [])

    live_income = AsyncMock(
        return_value=[
            {
                "symbol": "BTC/USDT",
                "timestamp": "2026-04-20T03:00:00Z",
                "pnl": 4.0,
            }
        ]
    )
    monkeypatch.setattr(trading_api, "_fetch_binance_realized_pnl_income", live_income)

    paper_payload = asyncio.run(
        trading_api.get_pnl_heatmap(days=30, bucket="day", mode="paper")
    )
    live_payload = asyncio.run(
        trading_api.get_pnl_heatmap(days=30, bucket="day", mode="live")
    )

    assert paper_payload["trade_count"] == 0
    assert live_payload["trade_count"] == 1
    assert live_payload["symbols"] == ["BTC/USDT"]
    assert live_income.await_count == 1
