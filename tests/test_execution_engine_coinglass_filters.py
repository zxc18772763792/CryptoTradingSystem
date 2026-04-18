import asyncio
import importlib
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

from core.strategies import Signal, SignalType
from core.trading.execution_engine import ExecutionEngine
from core.trading.order_manager import OrderSide


execution_engine_module = importlib.import_module("core.trading.execution_engine")


def _make_signal(*, strategy_name: str, signal_type: SignalType = SignalType.BUY) -> Signal:
    return Signal(
        symbol="BTC/USDT",
        signal_type=signal_type,
        price=100.0,
        timestamp=datetime.now(timezone.utc),
        strategy_name=strategy_name,
        strength=0.9,
        metadata={"account_id": "main", "exchange": "binance"},
    )


def test_coinglass_strategy_filter_blocks_trend_long_in_paper(monkeypatch):
    engine = ExecutionEngine()
    signal = _make_signal(strategy_name="TrendFollowingStrategy", signal_type=SignalType.BUY)

    monkeypatch.setattr(execution_engine_module.settings, "COINGLASS_ENABLED", True, raising=False)
    monkeypatch.setattr(execution_engine_module.settings, "COINGLASS_INCLUDE_STRATEGIES", True, raising=False)
    monkeypatch.setattr(
        "core.data.coinglass_feature_builder.load_latest_derivatives_snapshot",
        AsyncMock(
            return_value={
                "timestamp": "2024-01-01T00:00:00Z",
                "crowding_score": 0.83,
                "squeeze_score": 0.42,
                "distribution_score": 0.66,
                "funding_rate": 0.0016,
                "taker_buy_sell_imbalance": -0.09,
                "basis_pct": 0.031,
                "payload": {"crowding_warning": True},
            }
        ),
    )

    result = asyncio.run(engine._evaluate_coinglass_strategy_filter(signal=signal, side=OrderSide.BUY))

    assert result["enabled"] is True
    assert result["available"] is True
    assert result["applied"] is True
    assert result["action"] == "block"
    assert result["reason"] == "coinglass_crowding_filter_long"
    assert "crowding_score" in result["trigger_flags"]
    assert result["snapshot"]["taker_buy_sell_imbalance"] == -0.09
    assert result["snapshot"]["basis_pct"] == 0.031


def test_coinglass_strategy_filter_blocks_reversal_during_squeeze_cooldown(monkeypatch):
    engine = ExecutionEngine()
    signal = _make_signal(strategy_name="RSIStrategy", signal_type=SignalType.SELL)

    monkeypatch.setattr(execution_engine_module.settings, "COINGLASS_ENABLED", True, raising=False)
    monkeypatch.setattr(execution_engine_module.settings, "COINGLASS_INCLUDE_STRATEGIES", True, raising=False)
    monkeypatch.setattr(
        "core.data.coinglass_feature_builder.load_latest_derivatives_snapshot",
        AsyncMock(
            return_value={
                "timestamp": "2024-01-01T00:00:00Z",
                "crowding_score": 0.51,
                "squeeze_score": 0.74,
                "distribution_score": 0.32,
                "funding_rate": 0.0002,
                "liquidation_short_usd": 31_000_000.0,
                "liquidation_long_usd": 4_000_000.0,
                "taker_buy_sell_imbalance": 0.12,
                "payload": {
                    "crowding_warning": False,
                    "history_ready": True,
                    "long_short_ratio_change_24h": 0.11,
                    "order_flow_confirmed": True,
                },
            }
        ),
    )

    result = asyncio.run(engine._evaluate_coinglass_strategy_filter(signal=signal, side=OrderSide.SELL))

    assert result["enabled"] is True
    assert result["available"] is True
    assert result["applied"] is True
    assert result["action"] == "block"
    assert result["reason"] == "coinglass_liquidation_squeeze_cooldown"
    assert result["largest_liquidation_usd"] == 31_000_000.0
    assert "liquidation_cooldown" in result["trigger_flags"]
    assert "order_flow_confirmed" in result["trigger_flags"]


def test_coinglass_strategy_filter_blocks_trend_short_when_short_side_is_crowded(monkeypatch):
    engine = ExecutionEngine()
    signal = _make_signal(strategy_name="TrendFollowingStrategy", signal_type=SignalType.SELL)

    monkeypatch.setattr(execution_engine_module.settings, "COINGLASS_ENABLED", True, raising=False)
    monkeypatch.setattr(execution_engine_module.settings, "COINGLASS_INCLUDE_STRATEGIES", True, raising=False)
    monkeypatch.setattr(
        "core.data.coinglass_feature_builder.load_latest_derivatives_snapshot",
        AsyncMock(
            return_value={
                "timestamp": "2024-01-01T00:00:00Z",
                "crowding_score": 0.44,
                "squeeze_score": 0.54,
                "distribution_score": 0.21,
                "funding_rate": -0.0012,
                "liquidation_short_usd": 19_000_000.0,
                "liquidation_long_usd": 3_000_000.0,
                "taker_buy_sell_imbalance": 0.14,
                "payload": {
                    "history_ready": True,
                    "crowded_short": True,
                    "squeeze_building": True,
                    "order_flow_confirmed": True,
                    "long_short_ratio_change_24h": -0.10,
                },
            }
        ),
    )

    result = asyncio.run(engine._evaluate_coinglass_strategy_filter(signal=signal, side=OrderSide.SELL))

    assert result["enabled"] is True
    assert result["available"] is True
    assert result["applied"] is True
    assert result["action"] == "block"
    assert result["reason"] == "coinglass_crowding_filter_short"
    assert "crowded_short" in result["trigger_flags"]
    assert "order_flow_confirmed" in result["trigger_flags"]
    assert result["snapshot"]["long_short_ratio_change_24h"] == -0.10


def test_coinglass_strategy_filter_blocks_reversal_long_on_flush_risk(monkeypatch):
    engine = ExecutionEngine()
    signal = _make_signal(strategy_name="RSIStrategy", signal_type=SignalType.BUY)

    monkeypatch.setattr(execution_engine_module.settings, "COINGLASS_ENABLED", True, raising=False)
    monkeypatch.setattr(execution_engine_module.settings, "COINGLASS_INCLUDE_STRATEGIES", True, raising=False)
    monkeypatch.setattr(
        "core.data.coinglass_feature_builder.load_latest_derivatives_snapshot",
        AsyncMock(
            return_value={
                "timestamp": "2024-01-01T00:00:00Z",
                "crowding_score": 0.36,
                "squeeze_score": 0.33,
                "distribution_score": 0.76,
                "funding_rate": 0.0001,
                "liquidation_short_usd": 2_500_000.0,
                "liquidation_long_usd": 28_000_000.0,
                "taker_buy_sell_imbalance": -0.16,
                "basis_pct": 0.012,
                "payload": {
                    "history_ready": True,
                    "flush_risk": True,
                    "liquidation_burst_score": 0.72,
                    "long_short_ratio_change_24h": -0.12,
                    "order_flow_confirmed": False,
                    "flow_divergence": True,
                },
            }
        ),
    )

    result = asyncio.run(engine._evaluate_coinglass_strategy_filter(signal=signal, side=OrderSide.BUY))

    assert result["enabled"] is True
    assert result["available"] is True
    assert result["applied"] is True
    assert result["action"] == "block"
    assert result["reason"] == "coinglass_liquidation_squeeze_cooldown"
    assert "flush_risk" in result["trigger_flags"]
    assert "ratio_flush" in result["trigger_flags"]
    assert result["snapshot"]["liquidation_burst_score"] == 0.72


def test_execute_signal_rejects_when_coinglass_strategy_filter_blocks(monkeypatch):
    engine = ExecutionEngine()
    signal = _make_signal(strategy_name="TrendFollowingStrategy", signal_type=SignalType.BUY)
    rejected_order = SimpleNamespace(id="rej-deriv-1")
    create_order_mock = AsyncMock(return_value=None)

    monkeypatch.setattr(execution_engine_module.account_manager, "resolve_exchange", lambda account_id, exchange: "binance")
    monkeypatch.setattr(
        engine,
        "_resolve_strategy_trade_policy",
        lambda strategy_name, exchange: {"allow_long": True, "allow_short": True},
    )
    monkeypatch.setattr(execution_engine_module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(engine, "_get_account_equity", AsyncMock(return_value=10_000.0))
    monkeypatch.setattr(execution_engine_module.strategy_manager, "get_strategy_allocation", lambda name: 0.1)
    monkeypatch.setattr(engine, "_calculate_quantity", AsyncMock(return_value=0.01))
    monkeypatch.setattr(engine, "_resolve_order_context", AsyncMock(return_value=(100.0, 1.0)))
    monkeypatch.setattr(engine, "_ensure_signal_protection_levels", lambda **kwargs: (99.0, 102.0))
    monkeypatch.setattr(
        engine,
        "_evaluate_coinglass_strategy_filter",
        AsyncMock(
            return_value={
                "enabled": True,
                "available": True,
                "applied": True,
                "action": "block",
                "reason": "coinglass_crowding_filter_long",
                "snapshot": {"crowding_score": 0.84},
            }
        ),
    )
    rejected_mock = AsyncMock(return_value=rejected_order)
    monkeypatch.setattr(execution_engine_module.order_manager, "record_rejected_order", rejected_mock)
    monkeypatch.setattr(execution_engine_module.order_manager, "create_order", create_order_mock)
    monkeypatch.setattr(engine, "_notify_callbacks", AsyncMock(return_value=None))

    result = asyncio.run(engine.execute_signal(signal))

    assert result is None
    assert create_order_mock.await_count == 0
    assert rejected_mock.await_count == 1
    assert int(engine.get_signal_diagnostics().get("derivatives_filtered") or 0) == 1
    assert engine.get_signal_diagnostics()["last_result"]["status"] == "derivatives_filtered"
