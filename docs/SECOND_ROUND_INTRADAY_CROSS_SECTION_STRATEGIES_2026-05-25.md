# Second-Round Diverse Cross-Section Strategy Pack

This pack wires the 20 factors from `docs/second_round_top20_diverse_factors.md` into the existing Binance USD-M cross-sectional strategy framework.

## Common Rules

- Data: Binance OHLCV panels.
- Execution: factor is formed on completed bar `t`; the web backtest applies weights with `weights.shift(1)`, so exposure starts on the next bar.
- Portfolio: equal-weight long/short baskets unless the mined execution mode is long-only or short-only.
- Default selection: bottom/top `20%` by factor value.
- Cost model: one-way taker fee `5 bps` plus dynamic slippage `max(2 bps, 0.08 * high-low range bps)`, capped at `20 bps`.
- Timeframes: most strategies use `5m`; `ReturnEntropy4hStrategy`, `FalseBreakoutSupply24hStrategy`, `CrossSectionalStress4hStrategy`, and `SignImbalance4hStrategy` use `1h`; `TurnoverEntropy48hStrategy` uses `15m`.

## Strategy Map

| Class | strategy_id | Timeframe | Lookback | Rebalance | Execution |
| --- | --- | --- | --- | --- | --- |
| `ReturnEntropy4hStrategy` | `return_entropy_4h` | `1h` | `4` | `24` | `spread_low_minus_high` |
| `FalseBreakoutSupply24hStrategy` | `false_breakout_supply_24h` | `1h` | `24` | `24` | `short_low` |
| `RangeAsymmetry48hStrategy` | `range_asymmetry_48h` | `5m` | `576` | `288` | `spread_low_minus_high` |
| `SessionAsiaFlow24hStrategy` | `session_asia_flow_24h` | `5m` | `288` | `288` | `spread_low_minus_high` |
| `SessionFlowRotation24hStrategy` | `session_flow_rotation_24h` | `5m` | `288` | `288` | `spread_low_minus_high` |
| `VolumeWeightedReturn24hStrategy` | `volume_weighted_return_24h` | `5m` | `288` | `288` | `spread_low_minus_high` |
| `WickImbalance48hStrategy` | `wick_imbalance_48h` | `5m` | `576` | `288` | `short_low` |
| `TurnoverEntropy48hStrategy` | `turnover_entropy_48h` | `15m` | `192` | `96` | `short_high` |
| `BodyVolumeCorr24hStrategy` | `body_volume_corr_24h` | `5m` | `288` | `288` | `spread_low_minus_high` |
| `CorrBreakdown24h72hStrategy` | `corr_breakdown_24h_72h` | `5m` | `864` | `288` | `short_high` |
| `ExtremeRecency48hStrategy` | `extreme_recency_48h` | `5m` | `576` | `288` | `short_high` |
| `UpDownBetaSpread24h72hStrategy` | `up_down_beta_spread_24h_72h` | `5m` | `864` | `288` | `spread_low_minus_high` |
| `DirectionalRangeEfficiency48hStrategy` | `directional_range_efficiency_48h` | `5m` | `576` | `288` | `spread_low_minus_high` |
| `CrossSectionalStress4hStrategy` | `cross_sectional_stress_4h` | `1h` | `4` | `24` | `spread_low_minus_high` |
| `SignImbalance4hStrategy` | `sign_imbalance_4h` | `1h` | `4` | `24` | `long_high` |
| `VWAPSlope24hStrategy` | `vwap_slope_24h` | `5m` | `288` | `288` | `spread_low_minus_high` |
| `VWAPGap48hStrategy` | `vwap_gap_48h` | `5m` | `576` | `288` | `spread_low_minus_high` |
| `RelativeVolShock24hStrategy` | `relative_vol_shock_24h` | `5m` | `288` | `144` | `short_low` |
| `LeadMarketResponse24h72hStrategy` | `lead_market_response_24h_72h` | `5m` | `864` | `288` | `spread_low_minus_high` |
| `BreakCountBalance24hStrategy` | `break_count_balance_24h` | `5m` | `288` | `288` | `spread_low_minus_high` |

Execution modes:

- `spread_low_minus_high`: long bottom factor bucket, short top bucket.
- `spread_high_minus_low`: long top factor bucket, short bottom bucket.
- `short_low`: short bottom bucket only.
- `short_high`: short top bucket only.
- `long_high`: long top bucket only.

## Local Validation

```powershell
python scripts/check_second_round_intraday_strategies.py
pytest tests/test_intraday_cross_section_strategies.py
```

The strategies are registered in `config/strategy_registry.py`, exported through `strategies.ALL_STRATEGIES`, and listed in `config/strategy_intraday_cross_section.yaml`.
