# Model card: Binance extreme-run-up price ranker

- Version: `binance-runup-price-v1-frozen-2026-07-18`
- Scope: Binance USDⓈ-M USDT perpetual altcoins; research and read-only monitoring only
- Prediction clock: compute at the daily 08:20 Asia/Shanghai monitor; hypothetical entry at the next 4h open, UTC 04:00
- Primary label: maximum high return of at least +200% over the next 14 calendar days
- Sensitivity labels: +50%, +100%, +150%, +300%, +500%, and +900% over 3/7/14/30 days
- Fixed factors: 7-day realized volatility, one-day intraday range, 7-day ATR/close, upper wick, distance to prior 20-day high, drawdown from 30-day high, lower wick, and 7-day/30-day range compression
- Model family: winsorized, standardized ridge logistic regression; preprocessing and coefficients are fit independently inside each historical fold
- Historical validation: expanding window with at least 180 days of training, 14 calendar days of purge, and 30 days of testing rolled every 30 days
- Evaluation: row-level AUC/AP/top-2% precision and lift, de-duplicated event capture, 7-day block bootstrap, independent event bootstrap, threshold/horizon multiple-testing control, and market-state slices
- OI rule: OI never changes the frozen monitor rank unless the pre-registered incremental validation gate passes; otherwise it is only a supportive/neutral/crowded/missing annotation
- Trading rule: no order routing, no leverage, and no connection to `altcoin_radar` or the existing A/B/C strategy stack
- Exit research: 52 fixed, staged, and trailing policies with a fixed -20% stop; policy selection uses only trades exited before 2026-03-24 and confirmation uses folds 3-6
- Frozen paper exit: sell half at +100%, activate a 25% trailing stop for the remainder, and exit by 30 days; strict classification is `watchlist_only`

## Intended use

Prioritize a small read-only watchlist for forward observation. The score is a relative concentration signal, not a calibrated probability of a +200% move and not a trade instruction.

## Known limitations

- Historical walk-forward results have been reviewed and are not untouched OOS.
- Current ExchangeInfo does not restore all delisted contracts, leaving survivorship bias.
- Binance public long-history OI is unavailable; long-window OI conclusions depend on the existing cache and a much smaller intersection universe.
- Historical circulating market cap is lagged 24 hours when available; current-supply price backfills are excluded from the primary OI conclusion.
- Four-hour bars cannot resolve intrabar order; ambiguous stop/target bars are simulated with the stop first.
- Extreme pumps can have materially worse real fills than bar-based simulations.
- The frozen exit has positive confirmation point estimates but only two of four confirmation folds have profit factor above one; week-block and symbol-cluster bootstrap expectancy intervals include zero.
- Historical funding is matched for only about 33.9% of frozen-exit confirmation trades; unmatched trades use zero funding and therefore do not represent complete realized costs.

## Freeze and change control

The price model, top-2% threshold, candidate stages, labeling rules, and paper exit stay frozen for 90 days. Daily snapshots are immutable; outcomes are appended separately. Reviews are scheduled at 30/60/90 days without refitting before day 90.

<!-- MULTISOURCE_BEGIN -->
## Multisource extension

- News and on-chain families never modify the frozen price rank unless their same-row incremental gate passes. Neither passed.
- Strict news availability uses local `fetched_at`; publication timestamps cannot make a feature available earlier.
- The exploratory catalyst entry failed the paper-trading gate: expectancy -1.75%, PF 0.88, week-bootstrap lower bound -20.18%.
- Deployment status remains `watchlist_only`; no order-routing module is imported.
<!-- MULTISOURCE_END -->

<!-- ENTRY_EXIT_TIMING_BEGIN -->
## Entry and exit timing extension

- The price model and eight frozen factors are unchanged. Eleven timing rules, one fixed one-bar microstructure model, six score thresholds, eleven stateful exits, and four risk-equal stop widths were tested.
- Rule calibration reads only outcomes matured before fold 3. Folds 3-6 are historical confirmation and are not described as untouched OOS.
- No timing or exit rule passed the paper-trading gate. Deployment remains `watchlist_only`; 98.5% is an annotation, not a replacement ranking threshold.
- The reference paper path remains next 4h open, -20% hard stop, half at +100%, 25% trailing remainder, 30-day maximum hold. It is not connected to execution.
<!-- ENTRY_EXIT_TIMING_END -->

<!-- SEQUENTIAL_STRATEGY_BEGIN -->
## Sequential launch challenger

- The production ranking remains the frozen eight-factor price model. The 24h challenger cannot change rank or override `reject_chase` / cooldown stages.
- Secondary launch rule: after the reference 4h open, require 24h MFE >=20% and the completed 24h close >=5%. Historical confirmation row precision was 25.42%; independent-event precision was 28.57%.
- Paper exit challenger: risk-equal 25% hard stop, break-even protection after +30%, half at +100%, 25% trailing remainder, maximum 30 days.
- Historical PF was 1.84, but only 50% of folds exceeded PF 1 and both clustered-bootstrap expectancy lower bounds were negative.
- Deployment remains `watchlist_only`. The read-only forward observer writes immutable 24h records and has no order-routing dependency.
<!-- SEQUENTIAL_STRATEGY_END -->

<!-- POSTLAUNCH_ENTRY_BEGIN -->
## Post-launch entry-location falsification

- Nine time-safe post-launch entry rules were calibrated before fold 3; actual labels were recomputed from each entry price.
- Calibration selected a 5% dip-and-reclaim entry, but confirmation precision fell from 13.56% to 6.12% and positive-path retention was only 37.50%.
- With the same frozen exit, PF fell from 1.84 to 1.09; leave-largest-winner expectancy was negative.
- The delayed entry is rejected. The 24h confirmation open remains a paper reference only; deployment stays `watchlist_only` with no automatic trading.
<!-- POSTLAUNCH_ENTRY_END -->

<!-- STRATEGY_FRONTIER_BEGIN -->
## Strategy-frontier falsifications

- Staged allocation selected 0% probe and 100% after 24h launch confirmation; early probes diluted expectancy.
- Hybrid close/catastrophic stops did not replace the 25% hard stop. Only 12.50% of eventual +200% paths crossed -25% on a prior completed bar before +50%.
- Listing age was directionally negative in every fold, but adding it reduced top-2% precision from 9.40% to 8.28% and event capture from 39 to 29.
- All three remain rejected/annotation-only. Frozen ranking, entry, exit, stage vetoes, and `watchlist_only` deployment are unchanged.
<!-- STRATEGY_FRONTIER_END -->
<!-- FORWARD_MICROSTRUCTURE_BEGIN -->
## Prospective launch-time microstructure protocol

- At 12:05 Asia/Shanghai, a read-only collector observes only frozen 24h `launch_confirmed` signals; it cannot change ranks, stage vetoes, or paper eligibility.
- Captures are valid only from 0 to 15 minutes after the decision clock. Realtime book/premium/OI and past-only 5m OI/taker histories must pass timestamp and completeness checks; stale records fail closed and cannot be backfilled.
- Features include spread, 50/100bp quote depth, fixed-notional impact, basis/funding, current OI USD, 1/4/24h OI change, account/position ratios, and taker-buy share. These are annotation-only until truly prospective incremental tests pass.
- Immutable 30d labels use the complete 4h path and actual funding. The frozen hard-25/BE30/half100/trail25 policy is evaluated alongside one pre-registered close-stop counterfactual.
- The initial audit had two stale historical launch confirmations and zero valid microstructure captures. No inference is made from them; deployment remains `watchlist_only`.
<!-- FORWARD_MICROSTRUCTURE_END -->

<!-- CONTINUATION_UTILITY_BEGIN -->
## Secondary 8h continuation-utility paper gate

- Version: `continuation-utility-8h-v1-frozen-2026-07-19`; model hash `1485fd7e217aff48949d30666f3a6fff0ce5a5eb91f11b51de37735c318b6b25`.
- Role: secondary paper-entry challenger only. It never changes the primary price rank and cannot override `reject_chase` or `cooldown_after_200pct_runup`.
- Clock: observe exactly two completed 4h bars after P0 and score at P0+8h; forward capture must occur within 15 minutes.
- Frozen model: strongly regularized logistic regression, 542 matured training rows, 143 positive 14d utility labels, top-30% training threshold 0.5664177568.
- Historical confirmation: 65 constrained 30d trades, expectancy 16.50%, PF 2.50, max drawdown -4.28%; 3/4 folds and 5/6 market states have PF above one. Week/symbol bootstrap expectancy lower bounds are +1.98%/+2.60%.
- Concentration: largest positive symbol LAB accounts for 23.02% of positive PnL; expectancy excluding LAB is 10.69%.
- Direct +200% continuation remains unvalidated: selected precision 9.68% and retention 13.64%, with clustered confidence intervals crossing zero.
- Classification: `paper_candidate_pending_true_oos` for entry utility, `watchlist_only` for direct extreme-run-up identification. No automatic trading or leverage.
<!-- CONTINUATION_UTILITY_END -->

<!-- INTENSITY_EXIT_FRONTIER_BEGIN -->
## Extreme-intensity and right-tail exit frontier

- Ordinal +50/+100/+150/+200% intensity improves confirmation AP to 0.1137, but clustered incremental lower bounds remain negative; ranking stays unchanged.
- Failure-to-launch exits at P0+24/48/72h were rejected in calibration; the frozen exit remains primary.
- Half at +150% with a 30% trail improves historical point estimates to 19.28% expectancy and PF 2.81, but paired incremental bootstrap intervals cross zero.
- `hard25_be30_half150_trail30` is a prospective read-only counterfactual, not a promoted policy. Automatic trading remains disabled.
<!-- INTENSITY_EXIT_FRONTIER_END -->

<!-- UTILITY_INTENSITY_ROUTER_BEGIN -->
## Rejected 8h/20h sequential router

- Frozen 8h utility entry followed by a 20h ordinal-intensity keep/exit decision is strongly rejected.
- Routed expectancy is -1.06% and PF 0.84, versus 16.50% and PF 2.50 for the frozen exit.
- Paired mean increment is -12.04%; both clustered 95% upper bounds remain below zero.
- The router is not added to forward signals or labels. The frozen entry and exit remain unchanged.
<!-- UTILITY_INTENSITY_ROUTER_END -->

<!-- UTILITY_EXHAUSTION_EXIT_BEGIN -->
## Profit-side exhaustion exit challenger

- Calibration selected half at +150%, a 35% trail, and next-open exit after a volume-backed upper-wick reversal once the position has doubled.
- Confirmation expectancy is 17.88%, PF 2.63, with all four fold PF values above one.
- Paired mean increment versus the frozen exit is 2.14%; week and symbol incremental lower bounds remain negative.
- `hard25_be30_half150_trail35_wick` is recorded as a prospective counterfactual only. The primary exit and automatic-trading prohibition are unchanged.
<!-- UTILITY_EXHAUSTION_EXIT_END -->

<!-- UTILITY_EXHAUSTION_ATTRIBUTION_BEGIN -->
## Exit-factor attribution and prospective derived policy

- Fixed factorial attribution uses 88 paired signals and 67 first-signal independent events; no policy is re-selected.
- Moving the static trail from 30% to 35% changes 12 paths and harms all 12; both row-cluster upper bounds are negative.
- The pure upper-wick timing effect is +1.99% per row and +1.26% per event, but all clustered lower bounds cross zero.
- The combined wick-35% point improvement is mostly attributable to wick timing, not the wider trail.
- `hard25_be30_half150_trail30_wick_posthoc` is a post-confirmation hypothesis recorded prospectively only. It cannot support historical promotion, ranking changes, or automatic trading.
<!-- UTILITY_EXHAUSTION_ATTRIBUTION_END -->

<!-- UTILITY_ENTRY_TIMING_BEGIN -->
## Entry execution after the frozen 8h utility score

- Calibration selects a 5% discount followed by a positive higher-close reclaim, entered at the next 4h open within a 24h trigger window.
- Confirmation filters 88 score-open signals to 54; row precision rises from 11.36% to 14.81% with 80% positive-row retention.
- Independent-event precision rises from 7.46% to 11.11%, retaining all five positive events; event-cluster lower bounds are positive but row-cluster lower bounds cross zero.
- Matched +200% labels are identical, so enrichment is a fill/filter effect rather than entry-price label manufacture.
- Trade expectancy is 14.97%, PF 2.28; absolute and paired clustered lower bounds cross zero. Score-open remains primary.
- `discount5_reclaim_24h_next_open` is recorded in utility forward labels only, without rank, stage, or automatic-trading authority.
<!-- UTILITY_ENTRY_TIMING_END -->

<!-- UTILITY_ENTRY_FRONTIER_BEGIN -->
## Full utility-entry frontier diagnostic

- All nine fixed entry rules are inspected with BH control, but confirmation-only winners are ineligible for historical promotion.
- Six-bar breakout has 28.57% row precision, 27.78% event precision, and improves over score-open in all four folds; it had zero calibration positives, misses BH significance, and event intervals cross zero.
- Waiting for the six-bar breakout reduces matched return by 17.80%; both paired 95% upper bounds are negative. The prelaunch-high breakout confirms the same chase penalty.
- Breakout remains a possible after-entry hold-strength annotation, not an entry trigger. No forward buy rule, rank change, or automatic-trading authority is added.
<!-- UTILITY_ENTRY_FRONTIER_END -->

<!-- UTILITY_BREAKOUT_HOLD_ROUTER_BEGIN -->
## Prospective breakout-confirmed hold router

- The frozen 8h entry is unchanged. A completed `breakout6` bar can switch only profit-side management at the following 4h open; all prior execution state is preserved.
- The prospective route uses +150% half-sale, 30% trailing remainder, and the existing volume-backed upper-wick next-open exit after a double.
- Historical point estimates are 19.65% expectancy and PF 2.79; paired all-row and independent-event increments are 3.00% and 2.31%, with week/symbol lower bounds above zero.
- This is confirmation-mined and cannot be historically promoted. It is recorded only as a rank-neutral, non-trading forward counterfactual; the primary entry and exit remain frozen.
<!-- UTILITY_BREAKOUT_HOLD_ROUTER_END -->

<!-- UTILITY_BREAKOUT_ADD_BEGIN -->
## Risk-neutral breakout add allocation

- Planned risk is fixed. The catalog reserves 0%, 10%, 25%, or 50% of the 8h position for a next-open six-bar breakout add; untriggered reserve stays in cash.
- Calibration selects 0% reserve: every non-zero fraction has negative paired increment in all three earlier calibration windows.
- Confirmation also rejects add-ons. The 10% reserve lowers expectancy from 19.65% to 18.38%; larger reserves reduce it further. Higher PF and smaller drawdown are explained by lower average deployment.
- Full planned 8h entry remains primary. Breakout is a hold/exit annotation only; no forward add-on, primary-rule change, or automatic-trading authority is added.
<!-- UTILITY_BREAKOUT_ADD_END -->

<!-- UTILITY_TAKER_FLOW_BEGIN -->
## 8h taker-buy-share prospective annotation

- Fixed condition: `early_taker_buy_share >= 0.50` after two completed 4h bars; it adds no entry delay.
- Calibration retains all 3/3 +200% rows; confirmation retains 9/10 and raises row precision from 11.36% to 18.75%.
- Row-cluster lower bounds are positive, but independent-event lower bounds cross zero. Filtered strategy fold 6 and symbol-cluster absolute-return gates fail.
- The field is written to future immutable utility observations and labels as a rank-neutral annotation only. It cannot alter rank, original stage veto, paper eligibility, position size, primary exit, or trading authority.
<!-- UTILITY_TAKER_FLOW_END -->

<!-- UTILITY_STRATEGY_ITERATION_BEGIN -->
## Taker robustness and breakout full-runner decision

- Taker buy share 48%-51% is directionally robust, but return plus path-efficiency matched permutation p=0.069; it remains a prospective rank-neutral annotation.
- Six full-position runner exits fail calibration versus `breakout6_half150_trail30_wick`; no forward exit field or counterfactual is added.
- Calibration selects a 75% partial sale, but confirmation reverses and favors 25%; the stable 50% partial fraction is retained and no adaptive switch is allowed.
- Confirmation's best runner is posthoc-only. Primary entry, exit, stage veto, paper gate, position sizing and trading authority are unchanged.
- Regime-conditioned partial-sale routing is rejected: only 3 calibration signals are affected, below the minimum support gate, and directional preferences reverse in confirmation.
- Forward operations are incomplete: immutable snapshots for 2026-07-20 and 2026-07-21 are absent, and mature utility labels remain zero. The late live dry-run is diagnostic only and is not backfilled as OOS.

<!-- UTILITY_STRATEGY_ITERATION_END -->
