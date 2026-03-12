from __future__ import annotations

import asyncio

from config.settings import settings
from core.ai.live_decision_router import LiveAIDecisionRouter


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
