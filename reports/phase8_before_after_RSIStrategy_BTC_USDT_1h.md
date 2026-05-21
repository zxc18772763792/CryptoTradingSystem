# Phase 8 Before/After: `RSIStrategy` on `BTC/USDT` (1h)

- Generated: 2026-05-21T04:54:50.155418+00:00
- Lookback: 90 days (2161 bars)
- **Before** = pre-2026-05-21 backtest (ignores SL/TP/trailing/time_stop)
- **After**  = current behavior (all exit channels enabled)

## Trade-level metrics

| Metric | Before | After | Delta |
|---|---:|---:|---:|
| Total closed trades | 19 | 19 | +0 |
| Win rate | +42.11% | +42.11% | +0.00% |
| Total net PnL | -165.6525 | -151.9741 | +13.6784 |
| Avg PnL per trade | -8.7186 | -7.9986 | +0.7199 |
| Sharpe | -0.549 | -0.500 | +0.049 |
| Max drawdown | +2.97% | +2.89% | -0.08% |
| Avg max-unrealized | +0.00% | +0.00% | n/a |

## Exit reason distribution

| Reason | Before (count) | Before % | After (count) | After % |
|---|---:|---:|---:|---:|
| `rsi_long_exit` | 0 | 0.0% | 5 | 26.3% |
| `signal_reversal` | 19 | 100.0% | 14 | 73.7% |

## Interpretation

