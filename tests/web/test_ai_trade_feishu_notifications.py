from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock


def test_build_ai_trade_open_notification_includes_reason(monkeypatch):
    from web import main as web_main

    monkeypatch.setattr(web_main.execution_engine, "get_trading_mode", lambda: "live")

    payload = {
        "signal": {
            "symbol": "BTC/USDT",
            "signal_type": "buy",
            "price": 68123.4,
            "timestamp": "2026-04-06T13:40:00+08:00",
            "strategy_name": "AI_AutonomousAgent",
            "strength": 0.72,
            "metadata": {
                "source": "ai_autonomous_agent",
                "exchange": "binance",
                "account_id": "main",
                "timeframe": "15m",
                "agent_provider": "codex",
                "agent_model": "gpt-5.4",
                "agent_confidence": 0.83,
                "agent_reason": "突破后回踩企稳，趋势延续。",
            },
        },
        "order": {
            "id": "ord-open-1",
            "status": "closed",
            "price": 68123.4,
            "amount": 0.02,
            "filled": 0.02,
        },
        "timestamp": "2026-04-06T05:40:00+00:00",
    }

    notification = web_main._build_ai_trade_execution_notification("order_executed", payload)

    assert notification is not None
    assert notification["title"] == "AI自治代理开多提醒: BTC/USDT"
    assert "动作: 开多" in notification["message"]
    assert "模型: codex/gpt-5.4" in notification["message"]
    assert "理由: 突破后回踩企稳，趋势延续。" in notification["message"]
    assert "成交数量: 0.02" in notification["message"]


def test_on_execution_event_sends_ai_close_notification_to_feishu(monkeypatch):
    from web import main as web_main

    monkeypatch.setattr(web_main.execution_engine, "get_trading_mode", lambda: "live")
    publish_mock = AsyncMock(return_value=None)
    send_mock = AsyncMock(return_value={"feishu": True})
    monkeypatch.setattr(web_main.event_bus, "publish_nowait_safe", publish_mock)
    monkeypatch.setattr(web_main.notification_manager, "send_message", send_mock)

    payload = {
        "action": "close_position",
        "symbol": "ETH/USDT",
        "side": "long",
        "close_price": 2135.5,
        "quantity": 0.5,
        "pnl": 28.75,
        "exchange": "binance",
        "account_id": "main",
        "strategy": "AI_AutonomousAgent",
        "signal": {
            "symbol": "ETH/USDT",
            "signal_type": "close_long",
            "price": 2135.5,
            "timestamp": "2026-04-06T13:42:00+08:00",
            "strategy_name": "AI_AutonomousAgent",
            "strength": 0.41,
            "metadata": {
                "source": "ai_autonomous_agent",
                "exchange": "binance",
                "account_id": "main",
                "timeframe": "15m",
                "agent_provider": "codex",
                "agent_model": "gpt-5.4",
                "agent_confidence": 0.74,
                "agent_reason": "达到目标区间且上行动能减弱，先锁定利润。",
            },
        },
        "order": {
            "id": "ord-close-1",
            "status": "closed",
            "price": 2135.5,
            "amount": 0.5,
            "filled": 0.5,
        },
        "timestamp": "2026-04-06T05:42:00+00:00",
    }

    asyncio.run(web_main._on_execution_event("order_executed", payload))

    assert publish_mock.await_count == 1
    assert send_mock.await_count == 1
    kwargs = send_mock.await_args.kwargs
    assert kwargs["channels"] == ["feishu"]
    assert kwargs["title"] == "AI自治代理平多提醒: ETH/USDT"
    assert "理由: 达到目标区间且上行动能减弱，先锁定利润。" in kwargs["message"]
    assert "平仓盈亏: +28.75" in kwargs["message"]
