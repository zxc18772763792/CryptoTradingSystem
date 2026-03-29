from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from config.settings import settings


@pytest.fixture(autouse=True)
def _isolate_agent_overlay(tmp_path, monkeypatch):
    """Redirect agent overlay path so tests don't pollute cache/ or each other."""
    import core.ai.autonomous_agent as _mod
    monkeypatch.setattr(
        _mod.autonomous_trading_agent, "_overlay_path",
        tmp_path / "agent_runtime_config.json",
    )
    monkeypatch.setattr(_mod.news_db, "get_recent_events", AsyncMock(return_value=[]))
    monkeypatch.setattr(_mod.execution_engine, "get_account_equity_snapshot", AsyncMock(return_value=0.0))
    monkeypatch.setattr(_mod.execution_engine, "get_strategy_position_cap_notional", lambda **kwargs: 0.0)
    monkeypatch.setattr(_mod.execution_engine, "get_live_trade_review", lambda **kwargs: {"items": []})
    monkeypatch.setattr(_mod.strategy_manager, "get_strategy_allocation", lambda name: 0.0)


def _sample_df() -> pd.DataFrame:
    idx = pd.date_range("2025-01-01", periods=120, freq="15min")
    close = [100.0 + i * 0.2 for i in range(len(idx))]
    return pd.DataFrame(
        {
            "open": close,
            "high": [v + 0.1 for v in close],
            "low": [v - 0.1 for v in close],
            "close": close,
            "volume": [10.0] * len(close),
        },
        index=idx,
    )


def test_autonomous_agent_run_once_submit_signal(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    class _Agg:
        def to_dict(self):
            return {"direction": "LONG", "confidence": 0.72}

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")
    submit_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(module.execution_engine, "submit_signal", submit_mock)
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(
            return_value={
                "action": "buy",
                "confidence": 0.83,
                "strength": 0.76,
                "leverage": 4,
                "stop_loss_pct": 0.02,
                "take_profit_pct": 0.05,
                "reason": "trend_following",
            }
        ),
    )

    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute", cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    assert result["decision"]["action"] == "buy"
    assert result["execution"]["submitted"] is True
    assert submit_mock.await_count == 1
    signal = submit_mock.await_args.args[0]
    assert signal.strategy_name == "AI_AutonomousAgent"
    assert result["decision"]["leverage"] == 1.0
    assert signal.metadata["leverage"] == 1.0
    assert signal.stop_loss is not None
    assert signal.take_profit is not None


def test_autonomous_agent_run_once_low_confidence_forces_hold(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    class _Agg:
        def to_dict(self):
            return {"direction": "LONG", "confidence": 0.60}

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")
    submit_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(module.execution_engine, "submit_signal", submit_mock)
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(
            return_value={
                "action": "buy",
                "confidence": 0.32,
                "strength": 0.7,
                "leverage": 3,
                "reason": "weak_conviction",
            }
        ),
    )

    asyncio.run(
        agent.update_runtime_config(
            enabled=True,
            mode="execute",
            min_confidence=0.7,
            cooldown_sec=0,
        )
    )
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    assert result["decision"]["action"] == "hold"
    assert result["decision"]["leverage"] == 1.0
    assert result["execution"]["submitted"] is False
    assert submit_mock.await_count == 0


def test_autonomous_agent_run_once_force_bypasses_disabled_guard(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    class _Agg:
        def to_dict(self):
            return {"direction": "LONG", "confidence": 0.79}

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")
    monkeypatch.setattr(module.execution_engine, "submit_signal", AsyncMock(return_value=False))
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(
            return_value={
                "action": "buy",
                "confidence": 0.82,
                "strength": 0.7,
                "leverage": 3,
                "reason": "force_run_smoke_test",
            }
        ),
    )

    asyncio.run(agent.update_runtime_config(enabled=False, mode="shadow", cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="manual_smoke", force=True))

    assert result.get("skipped") is not True
    assert result["decision"]["action"] == "buy"
    assert result["execution"]["reason"] == "shadow_mode"


def test_autonomous_agent_auto_start_is_env_controlled(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)
    monkeypatch.setattr(module.settings, "AI_AUTONOMOUS_AGENT_AUTO_START", True, raising=False)
    agent._override["AI_AUTONOMOUS_AGENT_AUTO_START"] = False

    initial = agent.get_runtime_config()
    asyncio.run(agent.update_runtime_config(auto_start=False))
    updated = agent.get_runtime_config()

    assert initial["auto_start"] is True
    assert updated["auto_start"] is True
    assert not agent._overlay_path.exists()


def test_autonomous_agent_exposes_raw_model_action_rewrite(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    class _Agg:
        def to_dict(self):
            return {"direction": "LONG", "confidence": 0.81}

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")
    submit_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(module.execution_engine, "submit_signal", submit_mock)
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(
            return_value={
                "action": "exit",
                "confidence": 0.81,
                "strength": 0.6,
                "reason": "take_profit_hit",
            }
        ),
    )

    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute", cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    model_output = result["diagnostics"]["model_output"]
    codes = {item.get("code") for item in (result["diagnostics"].get("items") or [])}
    assert result["decision"]["action"] == "hold"
    assert model_output["source"] == "provider"
    assert model_output["raw_action"] == "exit"
    assert model_output["normalized_action"] == "hold"
    assert model_output["action_changed"] is True
    assert "model_action_rewritten" in codes
    assert submit_mock.await_count == 0


def test_autonomous_agent_run_once_blocks_live_when_not_allowed(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    class _Agg:
        def to_dict(self):
            return {"direction": "SHORT", "confidence": 0.75}

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "live")
    submit_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(module.execution_engine, "submit_signal", submit_mock)
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(
            return_value={
                "action": "sell",
                "confidence": 0.86,
                "strength": 0.8,
                "leverage": 5,
                "reason": "live_block_guard",
            }
        ),
    )

    asyncio.run(
        agent.update_runtime_config(
            enabled=True,
            mode="execute",
            allow_live=False,
            cooldown_sec=0,
        )
    )
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    assert result["decision"]["action"] == "sell"
    assert result["decision"]["leverage"] == 1.0
    assert result["execution"]["submitted"] is False
    assert result["execution"]["reason"] == "live_mode_blocked"
    assert submit_mock.await_count == 0


def test_autonomous_agent_same_side_signal_allows_add_when_below_half_cap(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    class _Agg:
        def to_dict(self):
            return {"direction": "SHORT", "confidence": 0.78}

    fake_connector = SimpleNamespace(
        config=SimpleNamespace(default_type="future"),
        get_positions=AsyncMock(
            return_value=[
                {
                    "symbol": "BTCUSDT",
                    "side": "short",
                    "amount": -0.3,
                    "entry_price": 100.0,
                    "current_price": 100.0,
                    "unrealizedPnl": 0.03,
                    "leverage": 2.0,
                }
            ]
        ),
    )

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.exchange_manager, "get_exchange", lambda exchange: fake_connector)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "live")
    monkeypatch.setattr(module.execution_engine, "get_account_equity_snapshot", AsyncMock(return_value=1000.0))
    monkeypatch.setattr(module.execution_engine, "get_strategy_position_cap_notional", lambda **kwargs: 100.0)
    monkeypatch.setattr(module.strategy_manager, "get_strategy_allocation", lambda name: 0.0)
    submit_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(module.execution_engine, "submit_signal", submit_mock)
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(
            return_value={
                "action": "sell",
                "confidence": 0.84,
                "strength": 0.76,
                "leverage": 1,
                "reason": "stay_short",
            }
        ),
    )

    asyncio.run(
        agent.update_runtime_config(
            enabled=True,
            mode="execute",
            symbol="BTC/USDT",
            symbol_mode="manual",
            allow_live=True,
            cooldown_sec=0,
        )
    )
    context_payload, _ = asyncio.run(agent._build_context(agent.get_runtime_config()))
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    assert result["decision"]["action"] == "sell"
    assert context_payload["position"]["side"] == "short"
    assert context_payload["position"]["leverage"] == 2.0
    assert context_payload["position"]["position_notional"] == 30.0
    assert context_payload["position"]["position_cap_notional"] == 100.0
    assert context_payload["position"]["same_direction_exposure_ratio"] == 0.3
    assert result["execution"]["submitted"] is True
    assert submit_mock.await_count == 1
    signal = submit_mock.await_args.args[0]
    assert signal.metadata["same_direction_max_exposure_ratio"] == 0.5
    assert signal.metadata["same_direction_existing_notional"] == 30.0


def test_autonomous_agent_same_side_signal_holds_when_exposure_reaches_half_cap(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    class _Agg:
        def to_dict(self):
            return {"direction": "SHORT", "confidence": 0.78}

    fake_connector = SimpleNamespace(
        config=SimpleNamespace(default_type="future"),
        get_positions=AsyncMock(
            return_value=[
                {
                    "symbol": "BTCUSDT",
                    "side": "short",
                    "amount": -0.6,
                    "entry_price": 100.0,
                    "current_price": 100.0,
                    "unrealizedPnl": 0.06,
                    "leverage": 2.0,
                }
            ]
        ),
    )

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.exchange_manager, "get_exchange", lambda exchange: fake_connector)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "live")
    monkeypatch.setattr(module.execution_engine, "get_account_equity_snapshot", AsyncMock(return_value=1000.0))
    monkeypatch.setattr(module.execution_engine, "get_strategy_position_cap_notional", lambda **kwargs: 100.0)
    monkeypatch.setattr(module.strategy_manager, "get_strategy_allocation", lambda name: 0.0)
    submit_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(module.execution_engine, "submit_signal", submit_mock)
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(
            return_value={
                "action": "sell",
                "confidence": 0.84,
                "strength": 0.76,
                "leverage": 1,
                "reason": "stay_short",
            }
        ),
    )

    asyncio.run(
        agent.update_runtime_config(
            enabled=True,
            mode="execute",
            symbol="BTC/USDT",
            symbol_mode="manual",
            allow_live=True,
            cooldown_sec=0,
        )
    )
    context_payload, _ = asyncio.run(agent._build_context(agent.get_runtime_config()))
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    assert result["decision"]["action"] == "hold"
    assert result["decision"]["reason"].startswith("existing_short_position_limit_reached")
    assert context_payload["position"]["position_notional"] == 60.0
    assert context_payload["position"]["same_direction_exposure_ratio"] == 0.6
    assert result["execution"]["submitted"] is False
    assert submit_mock.await_count == 0


def test_agent_runtime_config_leverage_is_fixed_to_one(tmp_path):
    from core.ai.autonomous_agent import AutonomousTradingAgent

    agent = AutonomousTradingAgent(cache_root=tmp_path / "agent_fixed_leverage")
    cfg_before = agent.get_runtime_config()
    assert cfg_before["default_leverage"] == 1.0
    assert cfg_before["max_leverage"] == 1.0

    cfg_after = asyncio.run(
        agent.update_runtime_config(
            default_leverage=9.0,
            max_leverage=12.0,
        )
    )
    assert cfg_after["default_leverage"] == 1.0
    assert cfg_after["max_leverage"] == 1.0


def test_agent_model_feedback_outage_alerts_feishu_after_prolonged_429(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module
    from core.notifications import notification_manager

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    class _Agg:
        def to_dict(self):
            return {"direction": "LONG", "confidence": 0.72}

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")
    monkeypatch.setattr(module.execution_engine, "submit_signal", AsyncMock(return_value=True))
    send_mock = AsyncMock(return_value={"feishu": True})
    monkeypatch.setattr(notification_manager, "send_message", send_mock)
    agent._last_model_feedback_at = time.time() - (module._MODEL_FEEDBACK_OUTAGE_ALERT_SEC + 5)
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(side_effect=RuntimeError('codex_http_429:{"code":"USAGE_LIMIT_EXCEEDED"}')),
    )

    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute", cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    assert result["decision"]["action"] == "hold"
    assert "model_error:codex_http_429" in result["decision"]["reason"]
    assert send_mock.await_count == 1
    assert agent.get_status()["model_feedback_guard"]["last_failure_kind"] == "rate_limit"


def test_agent_model_feedback_classifies_503_service_unavailable(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    class _Agg:
        def to_dict(self):
            return {"direction": "SHORT", "confidence": 0.79}

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")
    monkeypatch.setattr(module.execution_engine, "submit_signal", AsyncMock(return_value=True))
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(side_effect=RuntimeError('codex_http_503:{"error":{"message":"Service temporarily unavailable","type":"api_error"}}')),
    )

    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute", cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    diagnostics = result["diagnostics"]
    assert result["decision"]["action"] == "hold"
    assert diagnostics["primary"]["code"] == "model_service_unavailable"
    assert "503" in diagnostics["primary"]["label"]
    assert diagnostics["model_feedback"]["kind"] == "service_unavailable"
    assert diagnostics["model_feedback"]["http_status"] == 503
    assert result["status"]["model_feedback_guard"]["last_failure_kind"] == "service_unavailable"


def test_agent_model_feedback_guard_hard_timeout_alerts_and_ends_round(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module
    from core.notifications import notification_manager

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    class _Agg:
        def to_dict(self):
            return {"direction": "SHORT", "confidence": 0.74}

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")
    monkeypatch.setattr(module.execution_engine, "submit_signal", AsyncMock(return_value=True))
    monkeypatch.setattr(module, "_MODEL_FEEDBACK_HARD_TIMEOUT_SEC", 0.01)
    monkeypatch.setattr(module, "_MODEL_FEEDBACK_OUTAGE_ALERT_SEC", 0.0)
    send_mock = AsyncMock(return_value={"feishu": True})
    monkeypatch.setattr(notification_manager, "send_message", send_mock)

    async def _slow_call_provider(**kwargs):
        await asyncio.sleep(0.05)
        return {
            "action": "sell",
            "confidence": 0.81,
            "strength": 0.72,
            "leverage": 1,
            "reason": "should_not_complete",
        }

    monkeypatch.setattr(agent, "_call_provider", _slow_call_provider)

    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute", cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    assert result["decision"]["action"] == "hold"
    assert "model_feedback_guard_timeout" in result["decision"]["reason"]
    assert send_mock.await_count == 1
    assert agent.get_status()["model_feedback_guard"]["last_failure_kind"] == "timeout"


# ── Overlay persistence ───────────────────────────────────────────────────────

def test_symbol_scan_reuses_recent_cached_snapshot(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_scan_cache")
    monkeypatch.setattr(
        agent,
        "get_runtime_config",
        lambda: {
            "exchange": "binance",
            "symbol": "BTC/USDT",
            "symbol_mode": "auto",
            "universe_symbols": ["BTC/USDT", "ETH/USDT"],
            "selection_top_n": 10,
            "timeframe": "15m",
            "lookback_bars": 240,
            "account_id": "main",
        },
    )
    monkeypatch.setattr(agent, "_cfg_with_learning_overlays", lambda cfg, force_learning_refresh=False: dict(cfg))
    tracked_mock = AsyncMock(return_value=[])
    build_context_mock = AsyncMock(side_effect=AssertionError("cached scan should not rebuild contexts"))
    monkeypatch.setattr(agent, "_tracked_position_symbols", tracked_mock)
    monkeypatch.setattr(agent, "_build_context", build_context_mock)

    agent._last_symbol_scan = {
        "generated_at": module._utc_now().isoformat(),
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

    result = asyncio.run(agent.get_symbol_scan(limit=10, force=False))

    assert result["selected_symbol"] == "ETH/USDT"
    assert tracked_mock.await_count == 1
    assert build_context_mock.await_count == 0


def test_update_runtime_config_clears_cached_symbol_scan(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_scan_reset")
    monkeypatch.setattr(agent, "_save_overlay", lambda: None)
    agent._last_symbol_scan = {
        "generated_at": module._utc_now().isoformat(),
        "symbol_mode": "manual",
        "configured_symbol": "BTC/USDT",
        "selected_symbol": "BTC/USDT",
        "selection_reason": "manual_symbol",
        "candidate_count": 1,
        "top_n": 10,
        "top_candidates": [{"rank": 1, "symbol": "BTC/USDT", "score": 0.5}],
    }

    asyncio.run(agent.update_runtime_config(symbol="ETH/USDT"))

    assert agent.get_status()["last_symbol_scan"] is None


def test_agent_config_persists_to_overlay(tmp_path):
    """update_runtime_config writes overlay that is reloaded by a new agent instance."""
    from core.ai.autonomous_agent import AutonomousTradingAgent

    agent = AutonomousTradingAgent(cache_root=tmp_path / "agent_a")
    asyncio.run(agent.update_runtime_config(enabled=True, allow_live=True, cooldown_sec=60))

    overlay_path = agent._overlay_path
    assert overlay_path.exists(), "overlay file should have been written"
    data = json.loads(overlay_path.read_text())
    assert data["AI_AUTONOMOUS_AGENT_ENABLED"] is True
    assert data["AI_AUTONOMOUS_AGENT_ALLOW_LIVE"] is True
    assert data["AI_AUTONOMOUS_AGENT_COOLDOWN_SEC"] == 60

    # New agent reading same overlay
    agent2 = AutonomousTradingAgent(cache_root=tmp_path / "agent_b")
    agent2._overlay_path = overlay_path
    agent2._load_overlay()
    cfg = agent2.get_runtime_config()
    assert cfg["enabled"] is True
    assert cfg["allow_live"] is True
    assert cfg["cooldown_sec"] == 60


def test_agent_corrupt_overlay_safe_start(tmp_path):
    """A corrupt overlay must not prevent agent startup."""
    from core.ai.autonomous_agent import AutonomousTradingAgent

    agent = AutonomousTradingAgent(cache_root=tmp_path / "agent_corrupt")
    agent._overlay_path.parent.mkdir(parents=True, exist_ok=True)
    agent._overlay_path.write_text("{ corrupt json", encoding="utf-8")
    agent._load_overlay()  # must not raise
    cfg = agent.get_runtime_config()
    assert isinstance(cfg, dict)
    assert "enabled" in cfg


def test_agent_runtime_config_falls_back_to_openai_when_glm_unavailable(tmp_path, monkeypatch):
    """Agent runtime should auto-switch off stale GLM config when only OpenAI is available."""
    from core.ai.autonomous_agent import AutonomousTradingAgent

    agent = AutonomousTradingAgent(cache_root=tmp_path / "agent_fallback")
    agent._overlay_path.parent.mkdir(parents=True, exist_ok=True)
    agent._overlay_path.write_text(
        json.dumps(
            {
                "AI_AUTONOMOUS_AGENT_ENABLED": True,
                "AI_AUTONOMOUS_AGENT_PROVIDER": "glm",
                "AI_AUTONOMOUS_AGENT_MODEL": "GLM-4.5-Air",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-openai", raising=False)
    monkeypatch.setattr(settings, "OPENAI_MODEL", "gpt-5.4", raising=False)
    monkeypatch.setattr(settings, "ANTHROPIC_API_KEY", "", raising=False)
    monkeypatch.setattr(settings, "ZHIPU_API_KEY", "", raising=False)

    agent._load_overlay()
    cfg = agent.get_runtime_config()

    assert cfg["provider"] == "codex"
    assert cfg["model"] == "gpt-5.4"
    assert cfg["provider_requested"] == "glm"
    assert cfg["provider_fallback"] is True


def test_agent_journal_contains_request_id(tmp_path, monkeypatch):
    """Journal rows must have request_id, execution_allowed, and rejection_reason fields."""
    from core.ai.autonomous_agent import AutonomousTradingAgent

    agent = AutonomousTradingAgent(cache_root=tmp_path / "agent_journal")
    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute"))

    # Inject a pre-built journal row to verify schema
    agent._append_journal({
        "request_id": "abc12345",
        "timestamp": "2026-01-01T00:00:00",
        "trigger": "test",
        "execution_allowed": False,
        "rejection_reason": "shadow_mode",
        "decision": {"action": "hold"},
        "execution": {"submitted": False, "reason": "shadow_mode"},
    })
    rows = agent.read_journal(limit=5)
    assert any(r.get("request_id") for r in rows)
    assert any("rejection_reason" in r for r in rows)


def test_agent_run_once_exposes_structured_diagnostics(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    class _Agg:
        def to_dict(self):
            return {
                "direction": "LONG",
                "confidence": 0.62,
                "blocked_by_risk": False,
                "risk_reason": "",
            }

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")
    monkeypatch.setattr(module.execution_engine, "submit_signal", AsyncMock(return_value=True))
    monkeypatch.setattr(
        agent,
        "get_symbol_scan",
        AsyncMock(
            return_value={
                "generated_at": "2026-01-01T00:00:00+00:00",
                "symbol_mode": "manual",
                "configured_symbol": "BTC/USDT",
                "selected_symbol": "BTC/USDT",
                "selection_reason": "manual_symbol",
                "candidate_count": 1,
                "top_n": 10,
                "top_candidates": [
                    {
                        "rank": 1,
                        "symbol": "BTC/USDT",
                        "direction": "LONG",
                        "confidence": 0.62,
                        "score": 0.71,
                        "tradable_now": False,
                        "blocked_by_risk": False,
                        "risk_reason": "",
                        "summary": "LONG 0.620; below threshold 0.620 < 0.700",
                        "research": {"status": "paper_running", "validation_reasons": []},
                    }
                ],
            }
        ),
    )
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(
            return_value={
                "action": "buy",
                "confidence": 0.32,
                "strength": 0.7,
                "leverage": 3,
                "reason": "weak_conviction",
            }
        ),
    )

    asyncio.run(
        agent.update_runtime_config(
            enabled=True,
            mode="execute",
            min_confidence=0.7,
            cooldown_sec=0,
        )
    )
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    codes = {item.get("code") for item in (result.get("diagnostics", {}).get("items") or [])}
    assert "below_min_confidence" in codes
    assert result["status"]["last_diagnostics"]["primary"]["code"] == "below_min_confidence"


def test_build_context_includes_market_event_and_account_risk_payloads(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)
    now = module._utc_now()

    class _Agg:
        def to_dict(self):
            return {
                "direction": "LONG",
                "confidence": 0.68,
                "blocked_by_risk": False,
                "risk_reason": "",
                "components": {
                    "llm": {"direction": "LONG", "confidence": 0.61, "weight": 0.4},
                    "ml": {"direction": "LONG", "confidence": 0.72, "weight": 0.35},
                    "factor": {"direction": "LONG", "confidence": 0.66, "weight": 0.25},
                },
            }

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")
    monkeypatch.setattr(module.execution_engine, "get_account_equity_snapshot", AsyncMock(return_value=1500.0))
    monkeypatch.setattr(module.execution_engine, "get_strategy_position_cap_notional", lambda **kwargs: 150.0)
    monkeypatch.setattr(module.strategy_manager, "get_strategy_allocation", lambda name: 0.12)
    monkeypatch.setattr(
        module.news_db,
        "get_recent_events",
        AsyncMock(
            return_value=[
                {
                    "event_id": "evt-1",
                    "ts": (now - module.timedelta(minutes=12)).isoformat(),
                    "symbol": "BTCUSDT",
                    "event_type": "etf",
                    "sentiment": 1,
                    "impact_score": 0.9,
                    "half_life_min": 180,
                    "evidence": {"title": "ETF inflow accelerates", "source": "coindesk"},
                },
                {
                    "event_id": "evt-2",
                    "ts": (now - module.timedelta(minutes=35)).isoformat(),
                    "symbol": "BTCUSDT",
                    "event_type": "macro",
                    "sentiment": -1,
                    "impact_score": 0.3,
                    "half_life_min": 120,
                    "evidence": {"title": "Macro headwind", "source": "reuters"},
                },
            ]
        ),
    )

    asyncio.run(agent.update_runtime_config(symbol="BTC/USDT", timeframe="15m"))
    context_payload, _ = asyncio.run(agent._build_context(agent.get_runtime_config()))

    assert context_payload["returns"]["r_4h"] > 0
    assert context_payload["market_structure"]["available"] is True
    assert context_payload["market_structure"]["trend"]["label"] == "uptrend"
    assert context_payload["market_structure"]["microstructure"]["atr_pct"] >= 0.0
    assert context_payload["event_summary"]["events_count"] == 2
    assert context_payload["event_summary"]["top_events"][0]["event_id"] == "evt-1"
    assert context_payload["event_summary"]["news_alpha_proxy"] > 0
    assert context_payload["account_risk"]["account_equity"] == 1500.0
    assert context_payload["account_risk"]["position_cap_notional"] == 150.0
    assert context_payload["account_risk"]["fixed_leverage"] == 1.0
    assert context_payload["execution_cost"]["fee_bps"] > 0
    assert context_payload["execution_cost"]["estimated_slippage_bps"] >= 2.0
    assert context_payload["execution_cost"]["estimated_round_trip_cost_bps"] >= (
        context_payload["execution_cost"]["estimated_one_way_cost_bps"] * 2.0 - 1e-9
    )
    assert "llm" in context_payload["aggregated_signal"]["components"]


def test_agent_run_once_journal_includes_structured_context(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)
    now = module._utc_now()

    class _Agg:
        def to_dict(self):
            return {
                "direction": "LONG",
                "confidence": 0.71,
                "blocked_by_risk": False,
                "risk_reason": "",
                "components": {
                    "llm": {"direction": "LONG", "confidence": 0.71, "weight": 0.4},
                    "ml": {"direction": "LONG", "confidence": 0.69, "weight": 0.35},
                    "factor": {"direction": "LONG", "confidence": 0.67, "weight": 0.25},
                },
            }

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")
    monkeypatch.setattr(module.execution_engine, "get_account_equity_snapshot", AsyncMock(return_value=1200.0))
    monkeypatch.setattr(module.execution_engine, "get_strategy_position_cap_notional", lambda **kwargs: 120.0)
    monkeypatch.setattr(module.strategy_manager, "get_strategy_allocation", lambda name: 0.1)
    monkeypatch.setattr(module.execution_engine, "submit_signal", AsyncMock(return_value=True))
    monkeypatch.setattr(
        module.news_db,
        "get_recent_events",
        AsyncMock(
            return_value=[
                {
                    "event_id": "evt-journal",
                    "ts": (now - module.timedelta(minutes=8)).isoformat(),
                    "symbol": "BTCUSDT",
                    "event_type": "institution",
                    "sentiment": 1,
                    "impact_score": 0.8,
                    "half_life_min": 180,
                    "evidence": {"title": "Institutional accumulation", "source": "bloomberg"},
                }
            ]
        ),
    )
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(
            return_value={
                "action": "buy",
                "confidence": 0.84,
                "strength": 0.73,
                "leverage": 5,
                "stop_loss_pct": 0.02,
                "take_profit_pct": 0.05,
                "reason": "aligned_context",
            }
        ),
    )

    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute", cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="test", force=True))
    rows = agent.read_journal(limit=1)

    assert result["execution"]["submitted"] is True
    assert rows, "journal should contain at least one row"
    context = rows[0].get("context") or {}
    assert "market_structure" in context
    assert context["event_summary"]["events_count"] == 1
    assert context["account_risk"]["account_equity"] == 1200.0
    assert context["execution_cost"]["fee_bps"] > 0
    assert context["execution_cost"]["estimated_one_way_cost_bps"] >= context["execution_cost"]["fee_bps"]
    assert context["market_structure"]["trend"]["label"] == "uptrend"


def test_execution_cost_payload_uses_live_defaults_and_liquidity_adjustment(tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)
    payload = agent._build_execution_cost_payload(
        cfg={"exchange": "binance"},
        market_structure={
            "microstructure": {
                "atr_pct": 0.0026,
                "realized_vol": 0.0019,
                "spread_proxy": 0.0013,
            },
            "volume": {
                "last": 150000.0,
                "avg_20": 3000000.0,
            },
        },
        account_risk={
            "trading_mode": "live",
            "position_cap_notional": 800.0,
            "same_direction_remaining_notional": 400.0,
            "current_position_notional": 0.0,
            "last_price": 1.33,
        },
    )

    assert payload["trading_mode"] == "live"
    assert payload["fee_source"] == "live_default_fee_rate"
    assert payload["fee_bps"] == pytest.approx(4.0)
    assert payload["dynamic_slippage_raw_bps"] > payload["dynamic_slippage_bps"]
    assert payload["liquidity_reference_notional"] > 0.0
    assert payload["liquidity_adjustment_factor"] < 1.0
    assert payload["estimated_slippage_bps"] >= 2.0
    assert payload["estimated_one_way_cost_bps"] < (payload["dynamic_slippage_raw_bps"] + 4.0)


def test_build_signal_embeds_profit_management_metadata(tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)
    signal = agent._build_signal(
        decision={
            "action": "buy",
            "confidence": 0.84,
            "strength": 0.73,
            "stop_loss_pct": 0.02,
            "take_profit_pct": 0.05,
            "reason": "trend_following",
        },
        cfg={
            "exchange": "binance",
            "account_id": "main",
            "timeframe": "15m",
            "provider": "codex",
            "model": "gpt-test",
            "strategy_name": "AI_AutonomousAgent",
        },
        context_payload={
            "price": 100.0,
            "execution_cost": {
                "estimated_one_way_cost_bps": 6.0,
                "estimated_round_trip_cost_bps": 12.0,
            },
            "market_structure": {
                "microstructure": {
                    "atr_pct": 0.0028,
                }
            },
        },
    )

    assert signal is not None
    metadata = signal.metadata
    assert metadata["profit_protect_enabled"] is True
    assert metadata["profit_protect_trigger_pct"] >= module._AI_PROFIT_PROTECT_TRIGGER_PCT_MIN
    assert metadata["profit_protect_lock_pct"] > 0.0
    assert metadata["partial_take_profit_enabled"] is True
    assert metadata["partial_take_profit_trigger_pct"] > metadata["profit_protect_trigger_pct"]
    assert metadata["partial_take_profit_fraction"] == pytest.approx(0.5)
    assert metadata["post_partial_trailing_stop_pct"] > 0.0
    assert metadata["outage_tight_trailing_stop_pct"] > 0.0
    assert metadata["outage_tight_trailing_stop_pct"] <= metadata["post_partial_trailing_stop_pct"]


def test_agent_model_outage_tightens_profitable_local_position(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    class _Agg:
        def to_dict(self):
            return {"direction": "LONG", "confidence": 0.76}

    local_position = SimpleNamespace(
        side=SimpleNamespace(value="long"),
        quantity=1.0,
        entry_price=100.0,
        current_price=101.2,
        unrealized_pnl=1.2,
        leverage=1.0,
    )

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: local_position)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")
    monkeypatch.setattr(module.execution_engine, "submit_signal", AsyncMock(return_value=True))
    tighten_mock = AsyncMock(return_value={"applied": True, "reason": "outage_protection_armed"})
    monkeypatch.setattr(module.execution_engine, "tighten_profitable_position_protection", tighten_mock)
    agent._last_model_feedback_at = time.time() - 3600.0
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(side_effect=RuntimeError('codex_http_429:{"code":"USAGE_LIMIT_EXCEEDED"}')),
    )

    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute", cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    assert result["decision"]["action"] == "hold"
    assert "profit_protection_armed" in result["decision"]["reason"]
    assert tighten_mock.await_count == 1


def test_agent_symbol_scan_prefers_trade_ready_symbol(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    async def _fake_build_context(cfg):
        symbol = str(cfg.get("symbol") or "BTC/USDT")
        if symbol == "ETH/USDT":
            confidence = 0.74
            direction = "LONG"
        else:
            confidence = 0.41
            direction = "SHORT"
        return (
            {
                "exchange": "binance",
                "symbol": symbol,
                "timeframe": "15m",
                "price": 100.0,
                "returns": {"r_1h": 0.0, "r_24h": 0.0},
                "realized_vol_annualized": 0.25,
                "bars": 240,
                "aggregated_signal": {
                    "direction": direction,
                    "confidence": confidence,
                    "blocked_by_risk": False,
                    "risk_reason": "",
                },
                "position": {},
                "research_context": {
                    "selected_candidate": {
                        "candidate_id": f"candidate-{symbol}",
                        "strategy": "MAStrategy",
                        "status": "paper_running",
                        "promotion_target": "paper",
                        "validation": {"reasons": []},
                    }
                },
                "profile": {},
                "trading_mode": "paper",
            },
            pd.DataFrame(),
        )

    monkeypatch.setattr(agent, "_build_context", _fake_build_context)
    asyncio.run(
        agent.update_runtime_config(
            enabled=True,
            symbol_mode="auto",
            universe_symbols=["BTC/USDT", "ETH/USDT"],
            min_confidence=0.58,
            selection_top_n=5,
        )
    )

    scan = asyncio.run(agent.get_symbol_scan(limit=5, force=True))

    assert scan["selected_symbol"] == "ETH/USDT"
    assert scan["top_candidates"][0]["symbol"] == "ETH/USDT"


def test_agent_symbol_scan_prioritizes_existing_positions(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    async def _fake_build_context(cfg):
        symbol = str(cfg.get("symbol") or "BTC/USDT")
        has_position = symbol == "BTC/USDT"
        if has_position:
            confidence = 0.18
            direction = "FLAT"
            position = {
                "side": "short",
                "quantity": 1.0,
                "entry_price": 100.0,
                "current_price": 98.0,
                "unrealized_pnl": 2.0,
                "source": "local",
            }
        else:
            confidence = 0.79
            direction = "LONG"
            position = {}
        return (
            {
                "exchange": "binance",
                "symbol": symbol,
                "timeframe": "15m",
                "price": 100.0,
                "returns": {"r_1h": 0.0, "r_24h": 0.0},
                "realized_vol_annualized": 0.25,
                "bars": 240,
                "aggregated_signal": {
                    "direction": direction,
                    "confidence": confidence,
                    "blocked_by_risk": False,
                    "risk_reason": "",
                },
                "position": position,
                "research_context": {
                    "selected_candidate": {
                        "candidate_id": f"candidate-{symbol}",
                        "strategy": "MAStrategy",
                        "status": "paper_running",
                        "promotion_target": "paper",
                        "validation": {"reasons": []},
                    }
                },
                "profile": {},
                "trading_mode": "paper",
            },
            pd.DataFrame(),
        )

    monkeypatch.setattr(agent, "_build_context", _fake_build_context)
    monkeypatch.setattr(
        module.position_manager,
        "get_all_positions",
        lambda: [SimpleNamespace(exchange="binance", account_id="main", quantity=1.0, symbol="BTC/USDT")],
    )
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")

    asyncio.run(
        agent.update_runtime_config(
            enabled=True,
            symbol="ETH/USDT",
            symbol_mode="auto",
            universe_symbols=["ETH/USDT"],
            min_confidence=0.58,
            selection_top_n=5,
        )
    )

    scan = asyncio.run(agent.get_symbol_scan(limit=5, force=True))

    assert scan["selected_symbol"] == "BTC/USDT"
    assert scan["selection_reason"] == "existing_position_priority"
    assert [row["symbol"] for row in scan["top_candidates"][:2]] == ["BTC/USDT", "ETH/USDT"]
    assert scan["top_candidates"][0]["has_position"] is True


def test_agent_no_price_closes_losing_position_when_learning_memory_requires(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)
    monkeypatch.setattr(
        agent,
        "_refresh_learning_memory",
        lambda cfg=None, force=False: {
            "adaptive_risk": {
                "effective_min_confidence": 0.62,
                "same_direction_max_exposure_ratio": 0.35,
                "entry_size_scale": 0.7,
                "force_close_on_data_outage_losing_position": True,
            },
            "guardrails": ["close losing positions when market data is unavailable"],
            "lessons": ["近期价格缺失时不再继续被动 hold。"],
            "blocked_symbol_sides": [],
        },
    )
    monkeypatch.setattr(
        agent,
        "get_symbol_scan",
        AsyncMock(
            return_value={
                "selected_symbol": "BTC/USDT",
                "selection_reason": "manual_symbol",
                "top_candidates": [],
            }
        ),
    )
    monkeypatch.setattr(
        agent,
        "_build_context",
        AsyncMock(
            return_value=(
                {
                    "exchange": "binance",
                    "symbol": "BTC/USDT",
                    "timeframe": "15m",
                    "price": 0.0,
                    "bars": 0,
                    "returns": {"r_1h": 0.0, "r_24h": 0.0},
                    "realized_vol_annualized": 0.0,
                    "market_structure": {"available": False},
                    "aggregated_signal": {"direction": "SHORT", "confidence": 0.61, "blocked_by_risk": False, "risk_reason": ""},
                    "event_summary": {"available": False},
                    "position": {
                        "side": "short",
                        "quantity": 1.0,
                        "entry_price": 100.0,
                        "current_price": 101.5,
                        "unrealized_pnl": -1.5,
                        "position_notional": 101.5,
                        "position_cap_notional": 400.0,
                        "same_direction_exposure_ratio": 0.25375,
                        "same_direction_exposure_limit_ratio": 0.35,
                        "same_direction_remaining_notional": 38.5,
                    },
                    "account_risk": {
                        "trading_mode": "paper",
                        "min_confidence": 0.58,
                        "position_cap_notional": 400.0,
                    },
                    "execution_cost": {"estimated_one_way_cost_bps": 5.0, "estimated_round_trip_cost_bps": 10.0},
                    "research_context": {"available": False},
                    "profile": {},
                    "learning_memory": {},
                    "trading_mode": "paper",
                },
                pd.DataFrame(),
            )
        ),
    )
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")
    submit_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(module.execution_engine, "submit_signal", submit_mock)

    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute", cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    assert result["decision"]["action"] == "close_short"
    assert result["execution"]["submitted"] is True
    assert submit_mock.await_count == 1


def test_agent_learning_guard_requires_research_for_fresh_entry(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)
    monkeypatch.setattr(
        agent,
        "_refresh_learning_memory",
        lambda cfg=None, force=False: {
            "adaptive_risk": {
                "effective_min_confidence": 0.64,
                "same_direction_max_exposure_ratio": 0.4,
                "entry_size_scale": 0.8,
                "require_research_for_new_entries": True,
            },
            "guardrails": ["require research context before fresh entries"],
            "lessons": ["近期无研究支撑的新单表现偏弱。"],
            "blocked_symbol_sides": [],
        },
    )
    monkeypatch.setattr(
        agent,
        "get_symbol_scan",
        AsyncMock(
            return_value={
                "selected_symbol": "ETH/USDT",
                "selection_reason": "top_ranked_tradable_symbol",
                "top_candidates": [],
            }
        ),
    )
    monkeypatch.setattr(
        agent,
        "_build_context",
        AsyncMock(
            return_value=(
                {
                    "exchange": "binance",
                    "symbol": "ETH/USDT",
                    "timeframe": "15m",
                    "price": 2000.0,
                    "bars": 240,
                    "returns": {"r_1h": 0.01, "r_24h": 0.03},
                    "realized_vol_annualized": 0.2,
                    "market_structure": {"available": True, "microstructure": {"atr_pct": 0.003}},
                    "aggregated_signal": {"direction": "LONG", "confidence": 0.74, "blocked_by_risk": False, "risk_reason": ""},
                    "event_summary": {"available": False},
                    "position": {},
                    "account_risk": {
                        "trading_mode": "paper",
                        "min_confidence": 0.58,
                        "position_cap_notional": 400.0,
                    },
                    "execution_cost": {"estimated_one_way_cost_bps": 4.0, "estimated_round_trip_cost_bps": 8.0},
                    "research_context": {"available": False},
                    "profile": {},
                    "learning_memory": {},
                    "trading_mode": "paper",
                },
                pd.DataFrame(),
            )
        ),
    )
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(
            return_value={
                "action": "buy",
                "confidence": 0.88,
                "strength": 0.72,
                "leverage": 1.0,
                "stop_loss_pct": 0.02,
                "take_profit_pct": 0.04,
                "reason": "trend_following",
            }
        ),
    )
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")
    submit_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(module.execution_engine, "submit_signal", submit_mock)

    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute", cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    assert result["decision"]["action"] == "hold"
    assert result["decision"]["reason"] == "review_requires_research"
    assert result["execution"]["submitted"] is False
    assert submit_mock.await_count == 0
