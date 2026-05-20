from datetime import datetime, timedelta, timezone

from core.structural.context import StructuralMarketContext
from core.structural.derivatives_crowding import LiquidationOICrowdingGate
from core.structural.onchain_flow import OnChainFlowConfig, evaluate_onchain_regime


def test_derivatives_gate_missing_data_degrades_to_neutral_with_reason_codes():
    ctx = StructuralMarketContext.from_row(
        {"symbol": "BTC/USDT", "close": 100.0},
        timestamp=datetime(2026, 5, 20, tzinfo=timezone.utc),
    )

    gate = LiquidationOICrowdingGate().evaluate(ctx)

    assert gate.allow_long is True
    assert gate.allow_short is True
    assert gate.gate_scalar == 1.0
    assert gate.reason_codes


def test_execution_risk_never_amplifies_position():
    ctx = StructuralMarketContext.from_row(
        {
            "symbol": "BTC/USDT",
            "crowded_long_score": 0.1,
            "funding_z": 0.1,
            "execution_risk_score": 0.9,
        },
        timestamp=datetime(2026, 5, 20, tzinfo=timezone.utc),
    )

    gate = LiquidationOICrowdingGate().evaluate(ctx)

    assert gate.block_new_longs is True
    assert gate.block_new_shorts is True
    assert gate.gate_scalar < 1.0
    assert "execution_cost_or_depth_risk_block_entries" in gate.reason_codes


def test_stale_onchain_data_returns_neutral_and_no_scalar_up():
    now = datetime(2026, 5, 20, tzinfo=timezone.utc)
    ctx = StructuralMarketContext.from_row(
        {
            "symbol": "BTC/USDT",
            "exchange_netflow_z": -3.0,
            "exchange_balance_change_z": -3.0,
            "stablecoin_balance_change_z": 3.0,
            "whale_outflow_score": 1.0,
            "onchain_as_of": now - timedelta(hours=30),
        },
        timestamp=now,
    )

    scalar = evaluate_onchain_regime(ctx, config=OnChainFlowConfig(stale_data_ttl_hours=24), now=now)

    assert scalar.long_scalar == 1.0
    assert scalar.short_scalar == 1.0
    assert scalar.metadata["onchain_regime"] == "neutral"
    assert "onchain_stale_no_amplification" in scalar.reason_codes
