# Post-Change Live Exit Gate (2026-05-21)

- Lookback days: `30` (`0` means all local history)
- Since: `2026-05-21T00:00:00+00:00`
- Minimum samples per gate: `10`
- Journal close rows: `0`
- Known close_order_mode rows: `0`
- Bollinger live close rows: `0`

| Gate | Value | Status | Note |
|---|---:|---|---|
| Journal close_reason coverage | 0.0% (0/0) | PENDING | minimum samples: 10 |
| VWAP journal close semantics | 0/0 reason rows, 0 SELL rows | PENDING | pending until VWAPReversion has post-change close rows |
| Active strategy close share | 0.0% (0/0) | PENDING | strategy-specific close_reason or signal_close-like rows |
| LIMIT close order share | 0.0% (0/0) | PENDING | unknown historical modes are excluded |
| Average close slippage | n/a (0 samples) | PENDING | target <= 3.00 bps |
| Bollinger live/backtest win-rate delta | live n=0, live=0.0%, backtest=75.0% | PENDING | target delta <= 10.0% |

## Notes

- `PENDING` means the code path is instrumented but the local logs do not yet contain enough post-change live close samples.
- `unknown` close order modes are excluded from LIMIT-share statistics because they predate mode persistence.
- Re-run this report after live trading has produced enough close rows.
