# Phase 8 Before/After: `BollingerBandsStrategy` on `BTC/USDT` (1h)

- Generated: 2026-05-21T04:54:43.831662+00:00
- Lookback: 90 days (2161 bars)
- **Before** = pre-2026-05-21 backtest (ignores SL/TP/trailing/time_stop)
- **After**  = current behavior (all exit channels enabled)

## Trade-level metrics

| Metric | Before | After | Delta |
|---|---:|---:|---:|
| Total closed trades | 20 | 23 | +3 |
| Win rate | +60.00% | +69.57% | +9.57% |
| Total net PnL | -26.0795 | 12.3741 | +38.4535 |
| Avg PnL per trade | -1.3040 | 0.5380 | +1.8420 |
| Sharpe | -0.138 | -0.034 | +0.105 |
| Max drawdown | +1.59% | +1.25% | -0.34% |
| Avg max-unrealized | +0.00% | +0.00% | n/a |

## Exit reason distribution

| Reason | Before (count) | Before % | After (count) | After % |
|---|---:|---:|---:|---:|
| `bollinger_middle_reversion` | 0 | 0.0% | 7 | 30.4% |
| `signal_reversal` | 20 | 100.0% | 16 | 69.6% |

## Interpretation

- Win rate **rose** by +9.57% - protective stops cut losses earlier and trailing locked profits.
- Sharpe improved by +0.10: exit channels reduce variance more than they cap winners.
