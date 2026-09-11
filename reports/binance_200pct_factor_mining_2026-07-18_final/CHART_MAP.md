# Chart map

| Report segment | Analytical question | Family / type | Fields | Supported claim | Palette policy | Evidence source |
|---|---|---|---|---|---|---|
| Current 30-day events | Which symbols had the largest valid earlier-close-to-later-high path? | Comparison & ranking / bar | `symbol`, `close_to_high_pct`; audit table also retains wick return, dates, OI and funding | Five close-confirmed events exceed +200%; two wick-only rows remain below the close threshold | Single blue root; direct axis labels; no redundant legend | `current_30d_runups.csv` |
| Factor validation | How much does daily top-2% selection improve the +200% label rate in train and test? | Comparison & benchmark / bar | `label`, `top2_lift`; rows retain n, positives, base rate, precision and AUC | Simple score test lift is 14.21x versus 2.95x in training; full model is weaker in test | Hard two-root cap for method, period carried in direct labels | `analysis_summary.json` |
| Strategy sensitivity | Does predictive concentration translate into robust 14-day returns? | Comparison / signed bar | `strategy`, `median_phase_mean_return_pct`; rows retain downside, median trade, win rate and target-hit rate | Simple score is positive in the recent test but negative in training; other test baselines are negative | Dark/open blue with visible zero line and signed labels | `strategy_backtest_summary.csv`, `strategy_train_summary.csv` |

All three visuals use bars because the evidence consists of discrete symbol, method-period, or strategy-period comparisons rather than time trends. Exact audit values remain available in the adjacent native tables or retained snapshot fields.
