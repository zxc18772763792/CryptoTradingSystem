# Chart map

| Section | Analytical question | Family / type | Data | Palette | Artifact |
|---|---|---|---|---|---|
| Historical robustness | Does lift persist across walk-forward folds? | Comparison / bar with benchmark | walk_forward_fold_metrics.csv | blue + neutral | charts/walk_forward_lift.png |
| Threshold sensitivity | Does the frozen model generalize across horizons and returns? | Matrix / heatmap | threshold_horizon_surface.csv | blue-gold | charts/threshold_horizon_heatmap.png |
| OI increment | Does OI improve the same-row price baseline? | Comparison / grouped bar | oi_walk_forward_fold_metrics.csv | blue + gold | charts/oi_increment.png |
| Trade path | Are MFE and MAE consistent with tradable exits? | Relationship / scatter | paper_trades.csv.gz | blue + orange | charts/mfe_mae.png |
| Exploratory portfolio | What return and drawdown did the full-sample four-policy winner produce? | Trend / line | paper_equity_curves.csv.gz | blue + orange | charts/equity_drawdown.png |
| Fixed take-profit confirmation | How do +25% to +300% fixed exits change expectancy and profit factor? | Comparison / two-panel line | exit_policy_confirmation_sensitivity.csv | blue + orange | charts/exit_fixed_tp_confirmation.png |
| Frozen exit cost stress | Does the calibration-selected exit survive 5/15/30bps slippage? | Comparison / two-panel bar | exit_policy_frozen_confirmation_summary.csv | blue + gold | charts/exit_cost_stress_confirmation.png |

<!-- ENTRY_EXIT_TIMING_BEGIN -->
## Entry and exit timing charts

- `timing-entry-precision`: comparison/bar; actual post-entry +200% precision by pre-registered timing rule; source `entry_timing_calibration.csv`.
- `timing-threshold-precision`: comparison/bar; confirmation precision across frozen rank thresholds; source `rank_threshold_confirmation.csv`.
- `timing-path-mfe`: trend/line; median MFE by outcome cohort and horizon; source `path_timing_summary.csv`.
- `timing-stop-expectancy`: comparison/bar; risk-equal expectancy by hard-stop width; source `stop_width_confirmation.csv`.
- `timing-fold-pf`: comparison/bar; selected timing strategy PF by fold; source `entry_exit_confirmation_folds.csv`.
<!-- ENTRY_EXIT_TIMING_END -->

<!-- SEQUENTIAL_STRATEGY_BEGIN -->
## Sequential launch charts

- `seq-rule-precision`: grouped comparison/bar; all top-2% versus the calibrated 24h launch rule by fold; source `simple_launch_recognition_folds.csv`.
- `seq-model-ap`: grouped comparison/bar; frozen price score versus the 24h shallow sequence model; source `sequential_model_confirmation_folds.csv`.
- `seq-exit-pf`: comparison/bar; selected -25% / break-even-30 exit challenger PF by fold; source `launch_exit_state_folds.csv`.
- `seq-rule-table`: ranked calibration table for interpretable 24h rules; source `sequential_simple_rule_calibration.csv`.
- `seq-forward-table`: immutable true-forward 24h observations; source `data/research/binance_200pct_forward_monitor/sequence_24h/*.json`.
<!-- SEQUENTIAL_STRATEGY_END -->

<!-- POSTLAUNCH_ENTRY_BEGIN -->
## Post-launch entry-location outputs

- `postlaunch-recognition`: baseline versus the calibration-selected delayed entry on actual +200% precision.
- `postlaunch-calibration-table`: all nine time-safe post-launch entry rules and their calibration metrics.
- `charts/postlaunch_entry_comparison.png`: report figure comparing recognition precision and costed profit factor.
<!-- POSTLAUNCH_ENTRY_END -->

<!-- STRATEGY_FRONTIER_BEGIN -->
## Strategy-frontier outputs

- `frontier-staged-pf`: confirmation diagnostic PF across pre-confirmation probe fractions; selection remains calibration-only.
- `frontier-lifecycle-ap`: price-only versus price-plus-listing-age AP by fold.
- `frontier-hybrid-table`: winner adverse-excursion diagnostics before later targets.
- `charts/strategy_frontier_falsifications.png`: combined report figure for the three rejected hypotheses.
<!-- STRATEGY_FRONTIER_END -->
<!-- INTENSITY_EXIT_FRONTIER_BEGIN -->
## Intensity and exit frontier

- `charts/intensity_exit_frontier.png`: confirmation AP, precision-retention trade-off, and frozen-versus-right-tail exit point estimates.
<!-- INTENSITY_EXIT_FRONTIER_END -->

<!-- UTILITY_INTENSITY_ROUTER_BEGIN -->
## Utility/intensity router

- `charts/utility_intensity_router.png`: fold-level recognition instability and the strongly negative routed-exit result.
<!-- UTILITY_INTENSITY_ROUTER_END -->

<!-- UTILITY_EXHAUSTION_EXIT_BEGIN -->
## Profit exhaustion exit

- `charts/utility_exhaustion_exit.png`: fold PF, point estimates, and paired incremental uncertainty for the volume-backed upper-wick exit.
<!-- UTILITY_EXHAUSTION_EXIT_END -->

<!-- UTILITY_EXHAUSTION_ATTRIBUTION_BEGIN -->
## Exit-factor attribution

- `charts/utility_exhaustion_attribution.png`: row/event point attribution, pure-wick clustered uncertainty, and policy PF by confirmation fold.
<!-- UTILITY_EXHAUSTION_ATTRIBUTION_END -->

<!-- UTILITY_ENTRY_TIMING_BEGIN -->
## Utility entry timing

- `charts/utility_entry_timing.png`: calibration fill/precision frontier, confirmation row/event enrichment, and clustered precision-increment intervals.
<!-- UTILITY_ENTRY_TIMING_END -->

<!-- UTILITY_ENTRY_FRONTIER_BEGIN -->
## Full utility-entry frontier

- `charts/utility_entry_frontier.png`: confirmation fill/precision frontier, calibration-to-confirmation reversal, and matched chase-penalty intervals.
<!-- UTILITY_ENTRY_FRONTIER_END -->

<!-- UTILITY_BREAKOUT_HOLD_ROUTER_BEGIN -->
## Breakout-confirmed hold router

- `charts/utility_breakout_hold_router.png`: event-level +200% concentration, exit-route expectancy/PF, and paired all-row/event increment intervals.
<!-- UTILITY_BREAKOUT_HOLD_ROUTER_END -->

<!-- UTILITY_BREAKOUT_ADD_BEGIN -->
## Risk-neutral breakout add allocation

- `charts/utility_breakout_add.png`: calibration allocation increments, confirmation expectancy/PF, and independent-event uncertainty for reserved add fractions.
<!-- UTILITY_BREAKOUT_ADD_END -->

<!-- UTILITY_TAKER_FLOW_BEGIN -->
## 8h taker-buy-share diagnostic

- `charts/utility_taker_flow_filter.png`: calibration/confirmation precision, row/event clustered intervals, and frozen/breakout-router strategy point estimates.
<!-- UTILITY_TAKER_FLOW_END -->

<!-- UTILITY_STRATEGY_ITERATION_BEGIN -->
## Taker independence and full-runner audit

- `charts/utility_strategy_iteration.png`: nearby taker-share thresholds, price-path matched permutation p-values, and calibration/confirmation expectancy for the partial-exit and full-runner families.
- `charts/utility_breakout_partial_fraction.png`: calibration/confirmation expectancy for selling 25%, 50%, or 75% at the fixed +150% target.
- `charts/utility_regime_partial_router.png`: calibration/confirmation paired increments for fixed regime routers and market-state counts of fraction-sensitive signals.

<!-- UTILITY_STRATEGY_ITERATION_END -->
