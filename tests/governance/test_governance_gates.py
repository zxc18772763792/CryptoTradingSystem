from __future__ import annotations

import asyncio
import secrets
from datetime import datetime, timedelta, timezone

from config.settings import settings
from config.database import init_db
from core.governance.decision_engine import decision_engine
from core.governance.rbac import GovernanceIdentity
from core.governance.schemas import RiskConfigPayload
from core.governance.service import (
    ensure_risk_config_initialized,
    propose_strategy,
    request_risk_change,
    transition_strategy,
)


def test_decision_gate_blocks_kill_switch(monkeypatch):
    monkeypatch.setattr(settings, "GOVERNANCE_ENABLED", True)
    async def _mock_cfg():
        return {
            "kill_switch": True,
            "reduce_only": False,
            "max_leverage": 10.0,
            "max_position_notional_pct": 1.0,
            "max_trade_risk_pct": 1.0,
            "max_daily_drawdown_pct": 1.0,
            "spread_limit_bps": 1000.0,
            "data_staleness_limit_ms": 60_000,
            "allowed_symbols": [],
            "allowed_timeframes": [],
        }

    monkeypatch.setattr(decision_engine, "_load_active_risk_config", _mock_cfg)
    async def _run():
        return await decision_engine.evaluate_order_intent(
            symbol="BTC/USDT",
            side="buy",
            leverage=1.0,
            order_value=100.0,
            account_equity=10000.0,
            signal_ts=datetime.now(timezone.utc),
            allow_close=False,
            spread_bps=5.0,
            timeframe="1m",
        )

    out = asyncio.run(_run())
    assert out.allowed is False
    assert out.reason == "kill_switch_enabled"


def test_decision_gate_blocks_reduce_only_open(monkeypatch):
    monkeypatch.setattr(settings, "GOVERNANCE_ENABLED", True)
    async def _mock_cfg():
        return {
            "kill_switch": False,
            "reduce_only": True,
            "max_leverage": 10.0,
            "max_position_notional_pct": 1.0,
            "max_trade_risk_pct": 1.0,
            "max_daily_drawdown_pct": 1.0,
            "spread_limit_bps": 1000.0,
            "data_staleness_limit_ms": 60_000,
            "allowed_symbols": [],
            "allowed_timeframes": [],
        }

    monkeypatch.setattr(decision_engine, "_load_active_risk_config", _mock_cfg)
    async def _run():
        return await decision_engine.evaluate_order_intent(
            symbol="BTC/USDT",
            side="buy",
            leverage=1.0,
            order_value=100.0,
            account_equity=10000.0,
            signal_ts=datetime.now(timezone.utc),
            allow_close=False,
            spread_bps=5.0,
            timeframe="1m",
        )

    out = asyncio.run(_run())
    assert out.allowed is False
    assert out.reason == "reduce_only_enabled"


def test_decision_gate_blocks_spread_and_staleness(monkeypatch):
    monkeypatch.setattr(settings, "GOVERNANCE_ENABLED", True)
    async def _mock_cfg():
        return {
            "kill_switch": False,
            "reduce_only": False,
            "max_leverage": 10.0,
            "max_position_notional_pct": 1.0,
            "max_trade_risk_pct": 1.0,
            "max_daily_drawdown_pct": 1.0,
            "spread_limit_bps": 10.0,
            "data_staleness_limit_ms": 1_000,
            "allowed_symbols": [],
            "allowed_timeframes": [],
        }

    monkeypatch.setattr(decision_engine, "_load_active_risk_config", _mock_cfg)
    async def _run_spread():
        return await decision_engine.evaluate_order_intent(
            symbol="BTC/USDT",
            side="buy",
            leverage=1.0,
            order_value=100.0,
            account_equity=10000.0,
            signal_ts=datetime.now(timezone.utc),
            allow_close=False,
            spread_bps=25.0,
            timeframe="1m",
        )

    out_spread = asyncio.run(_run_spread())
    assert out_spread.allowed is False
    assert out_spread.reason == "spread_limit_exceeded"

    async def _run_stale():
        return await decision_engine.evaluate_order_intent(
            symbol="BTC/USDT",
            side="buy",
            leverage=1.0,
            order_value=100.0,
            account_equity=10000.0,
            signal_ts=datetime.now(timezone.utc) - timedelta(seconds=5),
            allow_close=False,
            spread_bps=2.0,
            timeframe="1m",
        )

    out_stale = asyncio.run(_run_stale())
    assert out_stale.allowed is False
    assert out_stale.reason == "signal_data_stale"


def test_increase_risk_change_requires_approval():
    async def _run():
        await init_db()
        await ensure_risk_config_initialized(actor="pytest")
        requester = GovernanceIdentity(actor="ops_user", role="OPERATOR")
        return await request_risk_change(
            identity=requester,
            proposed_config=RiskConfigPayload(
                max_leverage=10.0,
                max_position_notional_pct=0.5,
                max_trade_risk_pct=0.3,
                max_daily_drawdown_pct=0.2,
                spread_limit_bps=100.0,
                data_staleness_limit_ms=120_000,
                allowed_symbols=["BTC/USDT", "ETH/USDT"],
                allowed_timeframes=["1m", "5m"],
                reduce_only=False,
                kill_switch=False,
            ),
            reason="test increase risk change",
        )

    result = asyncio.run(_run())
    assert result["status"] == "pending"
    assert result["increase_risk"] is True
    assert result["proposed_version"] is None


def test_live_transition_requires_dual_approval():
    async def _run():
        await init_db()
        sid = f"gov_test_{secrets.token_hex(4)}"
        research_lead = GovernanceIdentity(actor="rl", role="RESEARCH_LEAD")
        risk_owner = GovernanceIdentity(actor="ro", role="RISK_OWNER")

        proposed = await propose_strategy(
            research_lead,
            strategy_id=sid,
            name="Dual Approval Test",
            strategy_class="MAStrategy",
            params={"fast_period": 5, "slow_period": 20},
        )
        version = int(proposed["version"])
        approved = await transition_strategy(
            research_lead,
            strategy_id=sid,
            version=version,
            target="approved",
            note="approve for test",
        )
        paper = await transition_strategy(
            research_lead,
            strategy_id=sid,
            version=version,
            target="paper",
            note="paper for test",
        )
        first_live = await transition_strategy(
            research_lead,
            strategy_id=sid,
            version=version,
            target="live",
            note="first signature",
        )
        second_live = await transition_strategy(
            risk_owner,
            strategy_id=sid,
            version=version,
            target="live",
            note="second signature",
        )
        return approved, paper, first_live, second_live

    approved, paper, first_live, second_live = asyncio.run(_run())
    assert approved["status"] == "approved"
    assert paper["status"] == "paper"
    assert first_live["status"] == "paper"
    assert "RISK_OWNER" in first_live.get("pending_approvals", [])
    assert second_live["status"] == "live"
