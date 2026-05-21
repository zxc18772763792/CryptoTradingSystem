# Exit Reason Audit Baseline (2026-05-21)

- Mode: `live`
- Lookback days: `30` (`0` means all local history)
- Generated at: `2026-05-21T04:24:57.907153+00:00`

## Journal Summary

| Metric | Value |
|---|---:|
| Journal rows | 38 |
| Opens | 28 |
| Closes | 10 |
| Losing closes | 5 (50.0%) |
| Close rows missing explicit reason | 0 (0.0%) |
| Risk history rows | 83 |
| Risk manual/close rows missing close_reason | 35 |

## Exit Reasons

| Reason | Trades | Share | Avg PnL |
|---|---:|---:|---:|
| `signal_close_legacy` | 7 | 70.0% | 1.130660 |
| `runtime_limit_reached` | 3 | 30.0% | -0.322379 |

## Close Order Modes

| Mode | Journal Closes | Share | Risk Close Rows |
|---|---:|---:|---:|
| `unknown` | 10 | 100.0% | 55 |

## Open Exit Template Coverage

| Exit Template | Opens | Share |
|---|---:|---:|
| `SignalPlusTimeStop` | 23 | 82.1% |
| `NONE` | 5 | 17.9% |

## Cost Anomalies

| Timestamp | Strategy | Symbol | Gross PnL | Net PnL | Slippage bps |
|---|---|---|---:|---:|---:|
| 2026-05-20T13:46:32.624341+00:00 | `bt_williamsr_sol_5m_104936_826` | `SOL/USDT` | 1.926000 | -0.336215 | 36.7168 |
| 2026-05-20T16:34:30.427886+00:00 | `bt_williamsr_sol_5m_104936_826` | `SOL/USDT` | 0.294000 | -0.388595 | 5.8282 |
| 2026-05-20T22:39:55.349210+00:00 | `bt_williamsr_sol_5m_104936_826` | `SOL/USDT` | 0.064400 | -0.106083 | 1.1644 |

## Notes

- `signal_close` means the journal only showed `close_long`/`close_short`; it did not preserve a more specific close reason.
- `unknown` means no usable close reason could be inferred.
- `unknown` under Close Order Modes means the row predates mode persistence or came from a path that did not record execution mode.
- Risk history reason gaps should shrink after execution paths persist `close_reason`.
