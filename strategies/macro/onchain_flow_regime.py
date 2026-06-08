"""On-chain exchange-flow regime strategy."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd
from loguru import logger

from core.strategies.strategy_base import Signal, SignalType, StrategyBase
from core.structural.context import StructuralMarketContext, clamp
from core.structural.onchain_flow import OnChainFlowConfig, evaluate_onchain_regime


class OnChainFlowRegimeStrategy(StrategyBase):
    mutates_input = False

    def __init__(self, name: str = "OnChain_Flow_Regime", params: Optional[Dict[str, Any]] = None):
        default_params = {
            "regime_mode": True,
            "trade_mode": False,
            "lookback_days": 180,
            "min_data_days": 90,
            "regime_refresh_hours": 6,
            "stale_data_ttl_hours": 24,
            "accumulation_enter": 0.65,
            "distribution_enter": 0.65,
            "max_scalar_up": 1.20,
            "max_scalar_down": 0.40,
            "emit_regime_hold_signal": True,
            "emit_regime_close_signal": True,
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)
        self._config = OnChainFlowConfig(
            accumulation_enter=float(self.params["accumulation_enter"]),
            distribution_enter=float(self.params["distribution_enter"]),
            max_scalar_up=float(self.params["max_scalar_up"]),
            max_scalar_down=float(self.params["max_scalar_down"]),
            stale_data_ttl_hours=float(self.params["stale_data_ttl_hours"]),
        )

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        # Guard against tiny frames so callers that haven't accumulated enough
        # history (e.g. cold-start replay) don't generate a regime decision
        # from a single bar. Matches the defensive pattern in other strategies.
        if data is None or data.empty or "close" not in data:
            return []
        if len(data) < 2:
            return []
        row = data.iloc[-1].to_dict()
        symbol = str(row.get("symbol", "UNKNOWN"))
        timestamp = self._bar_time(data)
        price = float(row["close"])
        ctx = StructuralMarketContext.from_row(row, symbol=symbol, timestamp=timestamp)
        scalar = evaluate_onchain_regime(ctx, config=self._config, now=timestamp)
        regime = str(scalar.metadata.get("onchain_regime", "neutral"))
        signals: List[Signal] = []

        if bool(self.params.get("regime_mode", True)) and bool(self.params.get("emit_regime_hold_signal", True)):
            if regime != "neutral" or scalar.long_scalar != 1.0 or scalar.short_scalar != 1.0:
                signals.append(
                    Signal(
                        symbol=ctx.symbol,
                        signal_type=SignalType.HOLD,
                        price=price,
                        timestamp=ctx.timestamp,
                        strategy_name=self.name,
                        strength=max(0.1, clamp(max(ctx.onchain.accumulation_score, ctx.onchain.distribution_score))),
                        metadata={
                            "structural_role": "position_scalar",
                            "position_scalar": scalar.to_dict(),
                            "reason_codes": list(scalar.reason_codes or []),
                        },
                    )
                )

        if not bool(self.params.get("trade_mode", False)):
            return signals

        if bool(self.params.get("emit_regime_close_signal", True)):
            if regime == "accumulation":
                strength = clamp(ctx.onchain.accumulation_score)
                signals.append(
                    Signal(
                        symbol=ctx.symbol,
                        signal_type=SignalType.CLOSE_SHORT,
                        price=price,
                        timestamp=ctx.timestamp,
                        strategy_name=self.name,
                        strength=strength,
                        metadata={
                            "structural_role": "slow_regime_exit",
                            "position_scalar": scalar.to_dict(),
                            "reason_codes": ["onchain_accumulation_close_short"],
                        },
                    )
                )
            elif regime == "distribution":
                strength = clamp(ctx.onchain.distribution_score)
                signals.append(
                    Signal(
                        symbol=ctx.symbol,
                        signal_type=SignalType.CLOSE_LONG,
                        price=price,
                        timestamp=ctx.timestamp,
                        strategy_name=self.name,
                        strength=strength,
                        metadata={
                            "structural_role": "slow_regime_exit",
                            "position_scalar": scalar.to_dict(),
                            "reason_codes": ["onchain_distribution_close_long"],
                        },
                    )
                )

        if regime == "accumulation":
            strength = clamp(ctx.onchain.accumulation_score)
            signals.append(
                Signal(
                    symbol=ctx.symbol,
                    signal_type=SignalType.BUY,
                    price=price,
                    timestamp=ctx.timestamp,
                    strategy_name=self.name,
                    strength=strength,
                    metadata={
                        "structural_role": "slow_regime_trade",
                        "position_scalar": scalar.to_dict(),
                        "reason_codes": ["onchain_accumulation_optional_long"],
                    },
                )
            )
            logger.info(f"{self.name} BUY {ctx.symbol} accumulation={strength:.3f}")
        elif regime == "distribution":
            strength = clamp(ctx.onchain.distribution_score)
            signals.append(
                Signal(
                    symbol=ctx.symbol,
                    signal_type=SignalType.SELL,
                    price=price,
                    timestamp=ctx.timestamp,
                    strategy_name=self.name,
                    strength=strength,
                    metadata={
                        "structural_role": "slow_regime_trade",
                        "position_scalar": scalar.to_dict(),
                        "reason_codes": ["onchain_distribution_optional_short"],
                    },
                )
            )
            logger.info(f"{self.name} SELL {ctx.symbol} distribution={strength:.3f}")
        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "onchain_context",
            "columns": [
                "exchange_netflow_z",
                "exchange_balance_change_z",
                "stablecoin_balance_change_z",
                "whale_inflow_score",
                "whale_outflow_score",
                "onchain_as_of",
                "lookahead_risk",
            ],
            "min_history_days": int(self.params.get("min_data_days", 90)),
        }
