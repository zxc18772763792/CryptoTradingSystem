from datetime import datetime, timezone

import pytest
from core.strategies.strategy_base import Signal, SignalType
from core.structural.risk_gate import StructuralRiskGate


def _signal(signal_type=SignalType.BUY, metadata=None):
    return Signal(
        symbol="BTC/USDT",
        signal_type=signal_type,
        price=100.0,
        timestamp=datetime(2026, 5, 20, tzinfo=timezone.utc),
        strategy_name="test",
        strength=0.8,
        metadata=metadata or {},
    )


def test_structural_risk_gate_blocks_buy_when_metadata_blocks_longs():
    gate = StructuralRiskGate()
    result = gate.evaluate_signal(
        _signal(
            metadata={
                "risk_gate": {
                    "symbol": "BTC/USDT",
                    "timestamp": "2026-05-20T00:00:00+00:00",
                    "block_new_longs": True,
                    "reason_codes": ["derivatives_crowded_long_block_new_longs"],
                }
            }
        )
    )

    assert result.allowed is False
    assert result.adjusted_strength == 0.0
    assert "structural_gate_blocked_buy" in result.reason_codes


def test_structural_risk_gate_no_context_bypasses_existing_strategies():
    gate = StructuralRiskGate()
    result = gate.evaluate_signal(_signal())

    assert result.allowed is True
    assert result.adjusted_strength == 0.8
    assert result.reason_codes == ["structural_gate_no_context_bypass"]


def test_structural_risk_gate_reduces_strength_with_scalar():
    gate = StructuralRiskGate()
    result = gate.evaluate_signal(
        _signal(
            signal_type=SignalType.SELL,
            metadata={
                "risk_gate": {
                    "symbol": "BTC/USDT",
                    "timestamp": "2026-05-20T00:00:00+00:00",
                    "reduce_short_position_scalar": 0.5,
                    "reason_codes": ["execution_cost_or_depth_risk_reduce"],
                },
                "position_scalar": {"short_scalar": 0.75},
            },
        )
    )

    assert result.allowed is True
    assert result.adjusted_strength == pytest.approx(0.3)
    assert "structural_gate_allowed" in result.reason_codes
