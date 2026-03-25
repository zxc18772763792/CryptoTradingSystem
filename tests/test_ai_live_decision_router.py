from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from config.settings import settings
from core.ai.live_decision_router import LiveAIDecisionRouter


@pytest.fixture(autouse=True)
def _isolate_overlay(tmp_path, monkeypatch):
    """Redirect overlay path to a temp dir so tests don't pollute data/ or each other."""
    import core.ai.live_decision_router as _mod
    monkeypatch.setattr(_mod, "_OVERLAY_PATH", tmp_path / "ai_runtime_config.json")


def _evaluate(router: LiveAIDecisionRouter):
    return router.evaluate_signal(
        trading_mode="live",
        strategy="MAStrategy",
        symbol="BTC/USDT",
        signal_type="buy",
        signal_strength=0.82,
        price=65000.0,
        account_equity=10000.0,
        order_value=500.0,
        leverage=3.0,
        timeframe="1h",
        existing_position={"side": "long", "quantity": 0.02},
        trade_policy={"allow_long": True, "allow_short": True},
        metadata={"exchange": "binance", "account_id": "main"},
    )


def test_live_decision_shadow_block_not_applied(monkeypatch):
    router = LiveAIDecisionRouter()
    monkeypatch.setattr(settings, "AI_LIVE_DECISION_ENABLED", True, raising=False)
    monkeypatch.setattr(settings, "AI_LIVE_DECISION_MODE", "shadow", raising=False)
    monkeypatch.setattr(settings, "AI_LIVE_DECISION_PROVIDER", "glm", raising=False)
    monkeypatch.setattr(settings, "AI_LIVE_DECISION_MODEL", "GLM-4.5-Air", raising=False)

    async def _fake_call_provider(**kwargs):
        return {"action": "block", "reason": "unstable context", "confidence": 0.66}

    monkeypatch.setattr(router, "_call_provider", _fake_call_provider)

    result = asyncio.run(_evaluate(router))
    assert result["action"] == "block"
    assert result["applied"] is False
    assert result["allowed"] is True
    assert result["mode"] == "shadow"


def test_live_decision_enforce_block_applied(monkeypatch):
    router = LiveAIDecisionRouter()
    monkeypatch.setattr(settings, "AI_LIVE_DECISION_ENABLED", True, raising=False)
    monkeypatch.setattr(settings, "AI_LIVE_DECISION_MODE", "enforce", raising=False)
    monkeypatch.setattr(settings, "AI_LIVE_DECISION_PROVIDER", "codex", raising=False)
    monkeypatch.setattr(settings, "AI_LIVE_DECISION_MODEL", "gpt-5-mini", raising=False)

    async def _fake_call_provider(**kwargs):
        return {"action": "block", "reason": "risk skew", "confidence": 0.92}

    monkeypatch.setattr(router, "_call_provider", _fake_call_provider)

    result = asyncio.run(_evaluate(router))
    assert result["action"] == "block"
    assert result["applied"] is True
    assert result["allowed"] is False
    assert result["mode"] == "enforce"


def test_live_decision_enforce_reduce_only_applied(monkeypatch):
    router = LiveAIDecisionRouter()
    monkeypatch.setattr(settings, "AI_LIVE_DECISION_ENABLED", True, raising=False)
    monkeypatch.setattr(settings, "AI_LIVE_DECISION_MODE", "enforce", raising=False)
    monkeypatch.setattr(settings, "AI_LIVE_DECISION_PROVIDER", "codex", raising=False)
    monkeypatch.setattr(settings, "AI_LIVE_DECISION_MODEL", "gpt-5.4", raising=False)

    async def _fake_call_provider(**kwargs):
        return {"action": "reduce_only", "reason": "only de-risk here", "confidence": 0.71}

    monkeypatch.setattr(router, "_call_provider", _fake_call_provider)

    result = asyncio.run(_evaluate(router))
    assert result["action"] == "reduce_only"
    assert result["applied"] is True
    assert result["allowed"] is True
    assert result["mode"] == "enforce"


def test_live_decision_fail_open(monkeypatch):
    router = LiveAIDecisionRouter()
    monkeypatch.setattr(settings, "AI_LIVE_DECISION_ENABLED", True, raising=False)
    monkeypatch.setattr(settings, "AI_LIVE_DECISION_MODE", "enforce", raising=False)
    monkeypatch.setattr(settings, "AI_LIVE_DECISION_FAIL_OPEN", True, raising=False)

    async def _raise_call_provider(**kwargs):
        raise RuntimeError("timeout")

    monkeypatch.setattr(router, "_call_provider", _raise_call_provider)

    result = asyncio.run(_evaluate(router))
    assert result["allowed"] is True
    assert result["action"] == "allow"
    assert result["reason"] == "ai_error_fail_open"


def test_live_decision_fail_closed(monkeypatch):
    router = LiveAIDecisionRouter()
    monkeypatch.setattr(settings, "AI_LIVE_DECISION_ENABLED", True, raising=False)
    monkeypatch.setattr(settings, "AI_LIVE_DECISION_MODE", "enforce", raising=False)
    monkeypatch.setattr(settings, "AI_LIVE_DECISION_FAIL_OPEN", False, raising=False)

    async def _raise_call_provider(**kwargs):
        raise RuntimeError("provider_down")

    monkeypatch.setattr(router, "_call_provider", _raise_call_provider)

    result = asyncio.run(_evaluate(router))
    assert result["allowed"] is False
    assert result["action"] == "block"
    assert result["applied"] is True
    assert result["reason"] == "ai_error_fail_closed"


def test_update_runtime_config_roundtrip():
    router = LiveAIDecisionRouter()
    updated = asyncio.run(
        router.update_runtime_config(
            enabled=True,
            mode="enforce",
            provider="claude",
            model="claude-3-5-sonnet-latest",
            timeout_ms=9000,
            max_tokens=260,
            temperature=0.15,
            fail_open=False,
            apply_in_paper=True,
        )
    )
    assert updated["enabled"] is True
    assert updated["mode"] == "enforce"
    assert updated["provider"] == "claude"
    assert updated["model"] == "claude-3-5-sonnet-latest"
    assert updated["timeout_ms"] == 9000
    assert updated["max_tokens"] == 260
    assert updated["temperature"] == 0.15
    assert updated["fail_open"] is False
    assert updated["apply_in_paper"] is True


# ── Step 4 回归：运行时配置持久化 ────────────────────────────────────────────

def test_runtime_config_persists_to_overlay(tmp_path, monkeypatch):
    """update_runtime_config writes overlay JSON that is reloaded by a new router."""
    overlay_path = tmp_path / "ai_runtime_config.json"
    monkeypatch.setattr("core.ai.live_decision_router._OVERLAY_PATH", overlay_path)

    router = LiveAIDecisionRouter()
    asyncio.run(router.update_runtime_config(enabled=True, mode="enforce", provider="claude"))

    assert overlay_path.exists(), "overlay file should have been created"
    data = json.loads(overlay_path.read_text())
    assert data["AI_LIVE_DECISION_ENABLED"] is True
    assert data["AI_LIVE_DECISION_MODE"] == "enforce"
    assert data["AI_LIVE_DECISION_PROVIDER"] == "claude"

    # New router instance should pick up the persisted values
    router2 = LiveAIDecisionRouter()
    cfg = router2.get_runtime_config()
    assert cfg["enabled"] is True
    assert cfg["mode"] == "enforce"
    assert cfg["provider"] == "claude"


def test_runtime_config_corrupt_overlay_safe_start(tmp_path, monkeypatch):
    """A corrupt overlay file must not prevent the router from starting."""
    overlay_path = tmp_path / "ai_runtime_config.json"
    overlay_path.write_text("{ not valid json !!!", encoding="utf-8")
    monkeypatch.setattr("core.ai.live_decision_router._OVERLAY_PATH", overlay_path)

    router = LiveAIDecisionRouter()  # must not raise
    cfg = router.get_runtime_config()
    # Falls back to settings defaults (enabled=False is the default)
    assert isinstance(cfg, dict)
    assert "enabled" in cfg


def test_runtime_config_overlay_does_not_store_api_keys(tmp_path, monkeypatch):
    """API key fields must never be written to the overlay."""
    overlay_path = tmp_path / "ai_runtime_config.json"
    monkeypatch.setattr("core.ai.live_decision_router._OVERLAY_PATH", overlay_path)

    router = LiveAIDecisionRouter()
    # Force an internal override with a key-like entry to simulate accidental injection
    router._override["ZHIPU_API_KEY"] = "secret"
    router._save_overlay()

    data = json.loads(overlay_path.read_text())
    assert "ZHIPU_API_KEY" not in data, "API keys must not be persisted to overlay"
