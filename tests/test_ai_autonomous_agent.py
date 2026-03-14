from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pandas as pd
import pytest


@pytest.fixture(autouse=True)
def _isolate_agent_overlay(tmp_path, monkeypatch):
    """Redirect agent overlay path so tests don't pollute cache/ or each other."""
    import core.ai.autonomous_agent as _mod
    monkeypatch.setattr(
        _mod.autonomous_trading_agent, "_overlay_path",
        tmp_path / "agent_runtime_config.json",
    )


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


# ── Overlay persistence ───────────────────────────────────────────────────────

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
    assert any("execution_allowed" in r for r in rows)
