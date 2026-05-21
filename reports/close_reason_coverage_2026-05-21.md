# Close Reason Coverage Audit (2026-05-21)

- Lookback days: `30` (`0` means all local history)
- Since: `2026-05-21T00:00:00+00:00`
- Coverage threshold: `95.0%`

| Gate | Value | Status |
|---|---:|---|
| Journal close_reason coverage | 0.0% (0/0) | PENDING |
| Risk close_reason coverage | 100.0% (9/9) | PASS |
| VWAP close reason rows | 0/0 | PASS |
| VWAP misclassified SELL close rows | 0 | PASS |
| Invalid LONG take_profit below entry | 0 | PASS |

## Notes

- `PENDING` means the local lookback window does not yet contain enough post-change close rows to prove the target.
- The detailed exit reason and close order mode distribution is available from `scripts/audit_exit_reasons.py`.
