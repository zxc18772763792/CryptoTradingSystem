from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pandas as pd


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
    assert result["execution"]["submitted"] is False
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
    assert result["execution"]["submitted"] is False
    assert result["execution"]["reason"] == "live_mode_blocked"
    assert submit_mock.await_count == 0
