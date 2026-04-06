from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pandas as pd
import pytest
from pydantic import ValidationError


def test_runtime_config_contains_ai_autonomous_agent(monkeypatch):
    from web.api import ai_research as ai_module

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module.live_decision_router, "get_runtime_config", lambda: {"enabled": False})
    monkeypatch.setattr(
        ai_module.autonomous_trading_agent,
        "get_runtime_config",
        lambda: {"enabled": True, "mode": "shadow", "provider": "glm"},
    )

    result = asyncio.run(ai_module.get_ai_runtime_config(request))
    assert "ai_autonomous_agent" in result
    assert result["ai_autonomous_agent"]["provider"] == "glm"


def test_update_autonomous_agent_runtime_config_endpoint(monkeypatch):
    from web.api import ai_agent as ai_module

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))

    async def _fake_update(**kwargs):
        return {
            "enabled": bool(kwargs.get("enabled")),
            "mode": str(kwargs.get("mode") or "shadow"),
            "provider": str(kwargs.get("provider") or "glm"),
        }

    monkeypatch.setattr(
        ai_module.ai_research_module.autonomous_trading_agent,
        "update_runtime_config",
        _fake_update,
    )

    payload = ai_module.AIAutonomousAgentConfigUpdateRequest(
        enabled=True,
        mode="execute",
        provider="codex",
        symbol_mode="auto",
        universe_symbols=["BTC/USDT", "ETH/USDT"],
        selection_top_n=8,
    )
    result = asyncio.run(ai_module.update_ai_autonomous_agent_runtime_config(request, payload))
    assert result["updated"] is True
    assert result["config"]["enabled"] is True
    assert result["config"]["mode"] == "execute"
    assert result["config"]["provider"] == "codex"


def test_update_autonomous_agent_runtime_config_payload_rejects_non_one_leverage():
    from web.api import ai_agent as ai_module

    with pytest.raises(ValidationError):
        ai_module.AIAutonomousAgentConfigUpdateRequest(default_leverage=2.0)


def test_update_autonomous_agent_runtime_config_payload_rejects_auto_start():
    from web.api import ai_agent as ai_module

    with pytest.raises(ValidationError):
        ai_module.AIAutonomousAgentConfigUpdateRequest(auto_start=True)


def test_autonomous_agent_start_and_run_once_endpoints(monkeypatch):
    from web.api import ai_agent as ai_module

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))

    update_mock = AsyncMock(return_value={"enabled": True})
    start_mock = AsyncMock(return_value={"running": True})
    run_once_mock = AsyncMock(return_value={"decision": {"action": "hold"}})
    status_mock = lambda: {"running": True}

    monkeypatch.setattr(ai_module.ai_research_module.autonomous_trading_agent, "update_runtime_config", update_mock)
    monkeypatch.setattr(ai_module.ai_research_module.autonomous_trading_agent, "start", start_mock)
    monkeypatch.setattr(ai_module.ai_research_module.autonomous_trading_agent, "trigger_run_once", run_once_mock)
    monkeypatch.setattr(ai_module.ai_research_module.autonomous_trading_agent, "get_status", status_mock)
    monkeypatch.setattr(
        ai_module.ai_research_module.autonomous_trading_agent,
        "get_runtime_config",
        lambda: {"enabled": True},
    )

    start_result = asyncio.run(
        ai_module.start_ai_autonomous_agent(
            request,
            ai_module.AIAutonomousAgentStartRequest(enable=True),
        )
    )
    assert start_result["started"] is True
    assert update_mock.await_count == 1
    assert start_mock.await_count == 1

    once_result = asyncio.run(
        ai_module.run_ai_autonomous_agent_once(
            request,
            ai_module.AIAutonomousAgentRunOnceRequest(force=True),
        )
    )
    assert once_result["decision"]["action"] == "hold"
    assert run_once_mock.await_count == 1


def test_autonomous_agent_symbol_ranking_endpoint(monkeypatch):
    from web.api import ai_agent as ai_module

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    scan_mock = AsyncMock(
        return_value={
            "generated_at": "2026-01-01T00:00:00+00:00",
            "symbol_mode": "auto",
            "configured_symbol": "BTC/USDT",
            "selected_symbol": "ETH/USDT",
            "selection_reason": "top_ranked_tradable_symbol",
            "candidate_count": 2,
            "top_n": 10,
            "top_candidates": [
                {"rank": 1, "symbol": "ETH/USDT", "score": 0.88},
                {"rank": 2, "symbol": "BTC/USDT", "score": 0.51},
            ],
        }
    )
    monkeypatch.setattr(
        ai_module.ai_research_module.autonomous_trading_agent,
        "get_symbol_scan_preview",
        scan_mock,
    )

    result = asyncio.run(ai_module.get_ai_autonomous_agent_symbol_ranking(request, limit=10, refresh=True))
    assert result["selected_symbol"] == "ETH/USDT"
    assert scan_mock.await_count == 1


def test_autonomous_agent_status_endpoint_does_not_require_ai_research_runtime(monkeypatch):
    from web.api import ai_agent as ai_module

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))

    def _unexpected_ensure(app):
        raise AssertionError("autonomous-agent status should not require ai research runtime")

    monkeypatch.setattr(ai_module.ai_research_module, "ensure_ai_research_runtime_state", _unexpected_ensure)
    monkeypatch.setattr(
        ai_module.ai_research_module.autonomous_trading_agent,
        "get_runtime_config",
        lambda: {"symbol_mode": "manual"},
    )
    monkeypatch.setattr(ai_module.ai_research_module.autonomous_trading_agent, "get_status", lambda: {"running": True})

    result = asyncio.run(ai_module.get_ai_autonomous_agent_status(request))
    assert result["status"]["running"] is True


def test_autonomous_agent_live_signals_endpoint_does_not_require_ai_research_runtime(monkeypatch):
    from web.api import ai_agent as ai_module

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))

    def _unexpected_ensure(app):
        raise AssertionError("autonomous-agent live signals should not require ai research runtime")

    payload = {
        "sections": [],
        "candidate_items": [],
        "watchlist_items": [],
        "items": [],
        "candidate_count": 0,
        "watchlist_count": 0,
        "count": 0,
        "ml_model_loaded": False,
        "ts": "2026-04-02T00:00:00+00:00",
    }

    monkeypatch.setattr(ai_module.ai_research_module, "ensure_ai_research_runtime_state", _unexpected_ensure)
    monkeypatch.setattr(
        ai_module.ai_research_module,
        "_build_autonomous_watchlist_live_signals_payload",
        AsyncMock(return_value=payload),
    )

    result = asyncio.run(ai_module.get_autonomous_agent_live_signals(request))
    assert result["watchlist_count"] == 0
    assert result["count"] == 0


def test_autonomous_agent_review_endpoint_includes_learning_memory(monkeypatch):
    from web.api import ai_agent as ai_module

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    monkeypatch.setattr(
        ai_module.ai_research_module,
        "_build_autonomous_agent_review",
        lambda limit=12: {"summary": {"submitted_count": 0}, "insights": [], "items": []},
    )
    monkeypatch.setattr(
        ai_module.ai_research_module.autonomous_trading_agent,
        "get_learning_memory",
        lambda force=False: {
            "adaptive_risk": {"effective_min_confidence": 0.66},
            "lessons": ["近期样本偏弱，抬高开仓门槛。"],
        },
    )

    result = asyncio.run(ai_module.get_ai_autonomous_agent_review(request, limit=8))
    assert result["summary"]["submitted_count"] == 0
    assert result["learning_memory"]["adaptive_risk"]["effective_min_confidence"] == 0.66


def test_build_autonomous_agent_review_includes_profit_curve(monkeypatch, tmp_path):
    from web.api import ai_research as ai_module

    journal_rows = [
        {
            "timestamp": "2026-04-04T09:00:00+00:00",
            "config": {"symbol": "BTC/USDT"},
            "decision": {"action": "buy", "reason": "trend", "confidence": 0.71},
            "diagnostics": {"primary": {"label": "趋势共振", "detail": "", "tone": "good"}},
            "context": {
                "price": 68000.0,
                "position": {},
                "execution_cost": {},
                "aggregated_signal": {"direction": "LONG", "confidence": 0.71},
            },
            "execution": {
                "submitted": True,
                "signal": {"symbol": "BTC/USDT", "signal_type": "buy", "price": 68000.0},
            },
        },
        {
            "timestamp": "2026-04-04T09:15:00+00:00",
            "config": {"symbol": "BTC/USDT"},
            "diagnostics": {"primary": {"label": "管理中", "detail": "", "tone": "info"}},
            "context": {
                "price": 68120.0,
                "position": {
                    "side": "long",
                    "quantity": 0.1,
                    "entry_price": 68000.0,
                    "current_price": 68125.0,
                    "unrealized_pnl": 1.25,
                },
            },
            "execution": {"submitted": False},
        },
        {
            "timestamp": "2026-04-04T09:30:00+00:00",
            "config": {"symbol": "BTC/USDT"},
            "decision": {"action": "close_long", "reason": "take_profit", "confidence": 0.64},
            "diagnostics": {"primary": {"label": "止盈离场", "detail": "", "tone": "good"}},
            "context": {
                "price": 68080.0,
                "position": {
                    "side": "long",
                    "quantity": 0.1,
                    "entry_price": 68000.0,
                    "current_price": 68080.0,
                    "unrealized_pnl": 0.8,
                },
                "execution_cost": {},
                "aggregated_signal": {"direction": "LONG", "confidence": 0.64},
            },
            "execution": {
                "submitted": True,
                "signal": {"symbol": "BTC/USDT", "signal_type": "close_long", "price": 68080.0},
            },
        },
    ]
    journal_path = tmp_path / "autonomous_agent_journal.jsonl"
    journal_path.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in journal_rows),
        encoding="utf-8",
    )

    monkeypatch.setattr(ai_module.autonomous_trading_agent, "_journal_path", journal_path)
    monkeypatch.setattr(ai_module.autonomous_trading_agent, "read_journal", lambda limit=500: [])
    monkeypatch.setattr(ai_module.order_manager, "get_recent_orders", lambda limit=5000: [])
    monkeypatch.setattr(ai_module.position_manager, "get_all_positions", lambda: [])

    payload = ai_module._build_autonomous_agent_review(limit=12)

    exit_item = next(item for item in payload["items"] if item["phase"] == "exit")
    curve = exit_item["profit_curve"]
    assert curve["closed"] is True
    assert [point["kind"] for point in curve["points"]] == ["entry", "mark", "exit"]
    assert [point["pnl"] for point in curve["points"]] == [0.0, 1.25, 0.8]


def test_build_autonomous_agent_review_falls_back_to_journal_signal_order(monkeypatch, tmp_path):
    from web.api import ai_research as ai_module

    journal_rows = [
        {
            "timestamp": "2026-04-06T06:16:50.768697+00:00",
            "config": {"symbol": "XRP/USDT", "exchange": "binance", "allow_live": True},
            "decision": {
                "action": "buy",
                "reason": "trend",
                "confidence": 0.69,
            },
            "diagnostics": {"primary": {"label": "trend", "detail": "", "tone": "good"}},
            "context": {
                "price": 1.3462,
                "position": {},
                "execution_cost": {},
                "aggregated_signal": {"direction": "LONG", "confidence": 0.69},
            },
            "execution": {
                "mode": "execute",
                "submitted": True,
                "reason": "submitted",
                "signal": {
                    "symbol": "XRP/USDT",
                    "signal_type": "buy",
                    "price": 1.3462,
                    "timestamp": "2026-04-06T06:16:50.767697+00:00",
                    "strategy_name": "AI_AutonomousAgent",
                    "quantity": None,
                    "stop_loss": 1.3300456,
                    "take_profit": 1.3785088,
                    "metadata": {
                        "exchange": "binance",
                        "account_id": "main",
                        "source": "ai_autonomous_agent",
                    },
                },
            },
        }
    ]
    journal_path = tmp_path / "autonomous_agent_journal.jsonl"
    journal_path.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in journal_rows),
        encoding="utf-8",
    )

    monkeypatch.setattr(ai_module.autonomous_trading_agent, "_journal_path", journal_path)
    monkeypatch.setattr(ai_module.autonomous_trading_agent, "read_journal", lambda limit=500: [])
    monkeypatch.setattr(ai_module.order_manager, "get_recent_orders", lambda limit=5000: [])
    monkeypatch.setattr(ai_module.position_manager, "get_all_positions", lambda: [])

    payload = ai_module._build_autonomous_agent_review(limit=12)

    item = payload["items"][0]
    assert item["order"]["match_source"] == "journal_signal"
    assert item["order"]["match_label"] == "journal signal"
    assert item["order"]["side"] == "buy"
    assert item["order"]["price"] == 1.3462
    assert item["order"]["account_id"] == "main"
    assert item["order"]["reduce_only"] is False


def test_build_autonomous_agent_review_marks_binance_merged_position_fallback(monkeypatch, tmp_path):
    from web.api import ai_research as ai_module

    journal_rows = [
        {
            "timestamp": "2026-04-06T06:42:53.052434+00:00",
            "config": {"symbol": "ETH/USDT", "exchange": "binance", "allow_live": True},
            "decision": {
                "action": "close_long",
                "reason": "reduce risk",
                "confidence": 0.82,
            },
            "diagnostics": {"primary": {"label": "risk", "detail": "", "tone": "warn"}},
            "context": {
                "price": 2054.15,
                "position": {
                    "side": "long",
                    "quantity": 1.333,
                    "entry_price": 2131.406054014,
                    "current_price": 2123.5,
                    "unrealized_pnl": -10.53876999,
                    "source": "exchange_live",
                },
                "execution_cost": {},
                "aggregated_signal": {"direction": "SHORT", "confidence": 0.82},
            },
            "execution": {
                "mode": "execute",
                "submitted": True,
                "reason": "submitted",
                "signal": {
                    "symbol": "ETH/USDT",
                    "signal_type": "close_long",
                    "price": 2054.15,
                    "timestamp": "2026-04-06T06:42:53.052434+00:00",
                    "strategy_name": "AI_AutonomousAgent",
                    "quantity": None,
                    "stop_loss": None,
                    "take_profit": None,
                    "metadata": {
                        "exchange": "binance",
                        "account_id": "main",
                        "source": "ai_autonomous_agent",
                    },
                },
            },
        }
    ]
    journal_path = tmp_path / "autonomous_agent_journal.jsonl"
    journal_path.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in journal_rows),
        encoding="utf-8",
    )

    monkeypatch.setattr(ai_module.autonomous_trading_agent, "_journal_path", journal_path)
    monkeypatch.setattr(ai_module.autonomous_trading_agent, "read_journal", lambda limit=500: [])
    monkeypatch.setattr(ai_module.order_manager, "get_recent_orders", lambda limit=5000: [])
    monkeypatch.setattr(ai_module.position_manager, "get_all_positions", lambda: [])

    payload = ai_module._build_autonomous_agent_review(limit=12)

    item = payload["items"][0]
    assert item["order"]["match_source"] == "merged_position"
    assert item["order"]["match_label"] == "binance merged position"
    assert item["order"]["side"] == "sell"
    assert item["order"]["price"] == 2054.15
    assert item["order"]["amount"] == 1.333
    assert item["order"]["reduce_only"] is True


def test_live_signals_gracefully_degrades_when_symbol_scan_times_out(monkeypatch):
    from web.api import ai_research as ai_module

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_module, "list_candidates", lambda app, limit=200: [])
    monkeypatch.setattr(ai_module, "_build_live_signal_watchlist_symbols", lambda runtime_cfg, selection: [])
    monkeypatch.setattr(ai_module.autonomous_trading_agent, "get_runtime_config", lambda: {"exchange": "binance"})
    monkeypatch.setattr(
        ai_module.autonomous_trading_agent,
        "get_symbol_scan",
        AsyncMock(return_value={"selected_symbol": "BTC/USDT"}),
    )

    async def fake_wait_for(awaitable, timeout):
        close = getattr(awaitable, "close", None)
        if callable(close):
            close()
        raise asyncio.TimeoutError("scan timed out")

    monkeypatch.setattr(ai_module.asyncio, "wait_for", fake_wait_for)

    result = asyncio.run(ai_module.get_live_signals(request, symbol="BTC/USDT"))

    assert result["count"] == 0
    assert result["watchlist_count"] == 0


def test_live_signal_snapshot_exposes_aggregated_timestamp(monkeypatch):
    from web.api import ai_research as ai_module

    async def fake_load_signal_market_data(**kwargs):
        df = pd.DataFrame(
            {"close": [1.0, 1.1]},
            index=pd.to_datetime(["2026-04-06T00:00:00Z", "2026-04-06T00:15:00Z"], utc=True),
        )
        return df, {
            "market_data_exchange": "binance",
            "market_data_symbol": "BTC/USDT",
            "market_data_timeframe": "15m",
            "market_data_source": "test",
            "market_data_rows": 2,
            "market_data_last_bar_at": "2026-04-06T00:15:00+00:00",
            "market_data_age_sec": 12.0,
            "market_data_stale": False,
            "market_data_load_error": None,
        }

    class FakeSignal:
        def to_dict(self):
            return {
                "symbol": "BTC/USDT",
                "direction": "LONG",
                "confidence": 0.73,
                "components": {},
                "timestamp": "2026-04-06T00:16:00+00:00",
            }

    class FakeSignalAggregator:
        async def aggregate(self, symbol, df, include_llm=False, include_ml=False):
            return FakeSignal()

    monkeypatch.setattr(ai_module, "_load_signal_market_data", fake_load_signal_market_data)

    payload, error = asyncio.run(
        ai_module._load_live_signal_snapshot(
            symbol="BTC/USDT",
            exchange="binance",
            timeframe="15m",
            signal_aggregator=FakeSignalAggregator(),
            limit=120,
            timeout_sec=1.0,
            log_label="unit-test",
        )
    )

    assert error == ""
    assert payload["timestamp"] == "2026-04-06T00:16:00+00:00"
    assert payload["aggregated_at"] == "2026-04-06T00:16:00+00:00"
    assert payload["market_data_last_bar_at"] == "2026-04-06T00:15:00+00:00"


def test_load_signal_market_data_localizes_naive_bar_timestamp_to_shanghai(monkeypatch):
    from web.api import ai_research as ai_module

    frame = pd.DataFrame(
        {"close": [1.0, 1.1]},
        index=pd.to_datetime(["2026-04-06 11:00:00", "2026-04-06 11:15:00"]),
    )

    monkeypatch.setattr(
        "core.strategies.strategy_manager._load_market_data",
        AsyncMock(return_value=frame),
    )

    _, meta = asyncio.run(
        ai_module._load_signal_market_data(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe="15m",
            limit=120,
        )
    )

    assert meta["market_data_last_bar_at"] == "2026-04-06T11:15:00+08:00"
    assert meta["market_data_age_sec"] is not None
