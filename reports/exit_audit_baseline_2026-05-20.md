# Exit Reason Audit Baseline (2026-05-20)

- Mode: `live`
- Lookback days: `30` (`0` means all local history)
- Generated at: `2026-05-20T14:59:36.243040+00:00`

## Journal Summary

| Metric | Value |
|---|---:|
| Journal rows | 16 |
| Opens | 14 |
| Closes | 2 |
| Losing closes | 1 (50.0%) |
| Close rows missing explicit reason | 2 (100.0%) |
| Risk history rows | 51 |
| Risk manual/close rows missing close_reason | 34 |

## Exit Reasons

| Reason | Trades | Share | Avg PnL |
|---|---:|---:|---:|
| `signal_close` | 2 | 100.0% | 0.625392 |

## Open Exit Template Coverage

| Exit Template | Opens | Share |
|---|---:|---:|
| `SignalPlusTimeStop` | 13 | 92.9% |
| `NONE` | 1 | 7.1% |

## Cost Anomalies

| Timestamp | Strategy | Symbol | Gross PnL | Net PnL | Slippage bps |
|---|---|---|---:|---:|---:|
| 2026-05-20T13:46:32.624341+00:00 | `bt_williamsr_sol_5m_104936_826` | `SOL/USDT` | 1.926000 | -0.336215 | 36.7168 |

## Notes

- `signal_close` means the journal only showed `close_long`/`close_short`; it did not preserve a more specific close reason.
- `unknown` means no usable close reason could be inferred.
- Risk history reason gaps should shrink after execution paths persist `close_reason`.
