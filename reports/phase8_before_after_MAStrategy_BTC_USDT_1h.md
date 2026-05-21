# Phase 8 Before/After: `MAStrategy` on `BTC/USDT` (1h)

- Generated: 2026-05-21T04:54:52.854518+00:00
- Lookback: 90 days (2161 bars)
- **Before** = pre-2026-05-21 backtest (ignores SL/TP/trailing/time_stop)
- **After**  = current behavior (all exit channels enabled)

## Trade-level metrics

| Metric | Before | After | Delta |
|---|---:|---:|---:|
| Total closed trades | 30 | 30 | +0 |
| Win rate | +26.67% | +30.00% | +3.33% |
| Total net PnL | -260.1203 | -206.5093 | +53.6111 |
| Avg PnL per trade | -8.6707 | -6.8836 | +1.7870 |
| Sharpe | -1.033 | -0.968 | +0.065 |
| Max drawdown | +3.19% | +2.65% | -0.53% |
| Avg max-unrealized | +0.00% | +0.00% | n/a |

## Exit reason distribution

| Reason | Before (count) | Before % | After (count) | After % |
|---|---:|---:|---:|---:|
| `ma_diff_compression` | 0 | 0.0% | 17 | 56.7% |
| `signal_reversal` | 30 | 100.0% | 13 | 43.3% |

## Interpretation

