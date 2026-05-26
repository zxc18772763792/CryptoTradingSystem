# Intraday Cross-Section Strategy Pack

This pack adds five Binance USD-M 5m cross-sectional long/short strategy components to the existing strategy library.

## Common Trading Rules

- Timeframe: `5m`
- Rebalance: every `288` bars, about 24 hours
- Rebalance anchor: `rebalance_offset_bars=0`, UTC day boundary by default
- Signal timestamp: factor is computed on the completed bar at `t`
- Execution assumption: backtest applies target weights with `weights.shift(1)`, so the return starts on the next bar
- Selection: default long/short quantile is `20%`
- Portfolio: equal weight within long basket and equal weight within short basket
- Default market: Binance USD-M futures via `exchange=binance`, `market_type=future`
- Cost model defaults: `fee_bps_per_side=5`, dynamic one-way slippage `max(2 bps, 0.08 * high-low range bps)`, capped at `20 bps`

## Strategy IDs

| Class | strategy_id | Formula | Long | Short |
| --- | --- | --- | --- | --- |
| `ResidualMom48hStrategy` | `residual_mom_48h` | `close_t / close_{t-576} - 1 - market_ret_48h` | Bottom 20% | Top 20% |
| `Ret24hReversalStrategy` | `ret_24h` | `close_t / close_{t-288} - 1` | Bottom 20% | Top 20% |
| `RelRet24hReversalStrategy` | `rel_ret_24h` | `ret_24h - market_ret_24h` | Bottom 20% | Top 20% |
| `ResidualMom24hStrategy` | `residual_mom_24h` | `ret_24h - market_ret_24h` | Bottom 20% | Top 20% |
| `CloseLocation48hStrategy` | `close_location_48h` | `mean_576((close - low) / (high - low))` | Top 20% | Bottom 20% |

For `close_location_48h`, zero-range bars use `0.5` to avoid infinity.

## Defaults

Each strategy is registered in `config/strategy_registry.py` and exported through `strategies.ALL_STRATEGIES`.

Important defaults:

- `lookback_bars`: `576` for 48h strategies, `288` for 24h strategies
- `rebalance_bars`: `288`
- `rebalance_offset_bars`: `0`
- `long_quantile`: `0.2`
- `short_quantile`: `0.2`
- `max_symbol_weight`: `0.10`
- `max_portfolio_leverage`: `1.0`
- `min_quote_volume`: `0.0`, intended to be raised for live use
- `funding_abs_threshold`: `0.0`, reserved for funding filters when a funding panel is available
- `max_abs_return` and `max_range_bps`: disabled by default, available as extreme-return and extreme-range filters

Shared config example: `config/strategy_intraday_cross_section.yaml`.

## Backtest Example

Use the existing backtest API/CLI path with `timeframe=5m`. Example strategy names:

```text
ResidualMom48hStrategy
Ret24hReversalStrategy
RelRet24hReversalStrategy
ResidualMom24hStrategy
CloseLocation48hStrategy
```

Local self-check:

```powershell
python scripts/check_top5_intraday_strategies.py
```

## Live Risk Notes

These are futures long/short components, not spot long-only systems. Before live use:

- Set a real `min_quote_volume` threshold for the traded universe.
- Keep `max_symbol_weight` and strategy allocation conservative.
- Confirm account mode, leverage, isolated account routing, and Binance USD-M permissions.
- Add or enable funding filters when funding data is available.
- Watch turnover and cost drag; the 24h rebalance reduces churn but basket replacement can still be expensive.
- Avoid treating these as independent alpha streams.

The first four strategies are highly related because they all express 24h/48h cross-sectional weakness reversal. `rel_ret_24h` and `residual_mom_24h` are currently identical by definition, but the separate strategy ID is intentionally retained for compatibility and for a future beta-adjusted residual implementation.

Suggested live blend:

- 24h/48h relative weakness reversal cluster: `residual_mom_48h`, `ret_24h`, `rel_ret_24h`, `residual_mom_24h`, total 60%-70%
- Close-location control/continuation sleeve: `close_location_48h`, total 30%-40%
