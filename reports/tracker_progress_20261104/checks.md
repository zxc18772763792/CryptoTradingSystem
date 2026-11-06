# Tracker progress check, 2026-11-04

- Read-only summaries and retained raw public numerical fields; no tracker tick, collector cycle, orders, restart, trading-mode changes or credential reads.
- Baseline retained unchanged: `../tracker_readiness_20261103/metrics.json`, captured 2026-11-03 23:09:57 Beijing time. Current sequential read ended 2026-11-04 22:46:09 Beijing time.
- All 27 registered rows remain collecting. Six row sample counts increased; the baseline/filtered reversal rows share the same additional day and must not be counted as independent new experiments.
- New fade v2 settlements: NOMUSDT +10.3416163082% and ACTUSDT +1.3669921565%, after the model's costs and recorded funding. Sample definition retained from the validated journal; v1 remains separate.
- New Upbit KRW settlement: NILUSDT +27.555%, using the legacy paper-ledger execution model.
- Legacy audit comparison: 128 prior terminal financial hashes unchanged; three new terminal records across the 15 audited legacy files. Pinned journal events are outside that terminal-hash list.
- Supply factor source verified: `core/research/supply_factor_tracker.py` uses Binance spot daily prices and a fixed 0.4% monthly cost; borrow inventory, limits and interest are not verified. Existing closed-month missing-member caveat remains.
- Managed web status observed ready in paper mode, autonomous AI agent stopped, allow_live=false. Public health responses HTTP 200; zero stale WS symbols at capture. The two original Telegram forward sources show collection_error after RuntimeError at 22:39, later than the successful 22:27 source batch. CryptoQuant Alert is a separate source and remains active.
- Subsequent source-only refresh at 22:50:04: scheduled runner completed successfully at 22:46:27; both original sources now healthy, complete_batch=true and difference_gap=false. BinanceChinese clean_since reset to 22:32:43. Preserve this recovery separately from the earlier snapshot; it does not establish uninterrupted daily coverage. See `health_refresh.json` and `refresh_health.py`.
- Standard Data chart payload validated; local IAB preview visibly showed 23 dates through Nov 3, both plotted series, labels and legend. Temporary tab closed. Prior release's unchanged source-inspector Windows-path failure remains; provenance is retained in progress.json and this script. No plugin runtime or manifest modified.
- No profitability certification, executable fills, margin/borrow feasibility, or account-level return/drawdown is implied by paper sample means.
