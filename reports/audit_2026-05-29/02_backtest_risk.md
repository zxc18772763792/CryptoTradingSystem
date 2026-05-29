# Backtest, Risk, Accounting & Execution Audit 鈥?2026-05-29

## Summary

| Severity | Count |
|---|---|
| Critical | 3 |
| High | 6 |
| Medium | 7 |
| Low | 6 |

**Overall health**: The backtest engine and risk manager are generally well-structured with multiple correct prior fixes documented in memory. However, three critical issues remain: a lookahead bias in the exit engine (entry fills at current-bar close while simultaneously checking exits), a Sharpe annualization mismatch in `PerformanceAnalyzer`, and a missing close-position open-side determination in `pnl_decomposer`. Several high-severity money-safety issues (slippage sign errors, fee double-counting in open-trade net_pnl, unrealized-PnL wrong sign in position sizing) add up to systematic mis-statement of performance in both backtests and live risk reporting.

---

## Findings

### [CRIT-01] ExitEngine lookahead: entry and exit fire on same bar (allow_same_bar_exit default False, but exit-engine used from research path without explicit guard)

- **Severity**: Critical
- **Category**: Correctness 鈥?lookahead bias
- **Location**: `core/backtest/exit_engine.py:304鈥?13`, `core/backtest/execution_arrays.py:233鈥?38`
- **What's wrong**: In `ExitEngine.run()` the protective stop check is gated by `not entry_bar or self.config.allow_same_bar_exit` (line 310). When `allow_same_bar_exit=False`, the stop cannot fire on the entry bar 鈥?correct. However, the **reversal/signal** exit path at line 419 is NOT gated by the same `entry_bar` condition. A trade opened at bar N can be immediately reversed by the signal changing on the **same bar** when the ATR/reversal branch executes. In `execute_arrays.py` the reversal condition (line 233) also applies at `active_entry_idx == idx` without any guard. The research pipeline commonly calls these directly; `allow_same_bar_exit=False` is the default and only governs the protective stop path, not the reversal path.
- **Impact**: Backtests report round-trip trades with 0 bars that are impossible in live trading. Win rates and short-term metrics (1m-5m timeframes) are inflated by spurious same-bar reversals. Over thousands of backtests this produces systematically optimistic Sharpe estimates.
- **Fix**: In both engines, add `if entry_bar and not self.config.allow_same_bar_exit: skip reversal/time_stop` check mirroring the protective stop guard.
- **Confidence**: High

---

### [CRIT-02] PerformanceAnalyzer Sharpe uses bar-count annualization of 365 (assumes daily bars) regardless of actual timeframe

- **Severity**: Critical
- **Category**: Correctness 鈥?formula error
- **Location**: `core/backtest/performance_analyzer.py:177鈥?81`, `_calculate_volatility` line 157鈥?58, `_annualize_return` line 143
- **What's wrong**: `_calculate_sharpe` computes `mean_return * 365` and `std * sqrt(365)`. This is correct for **daily** equity bars. However the backtest equity curve appends one point per **bar** (every 1m, 5m, 15m, 1h etc.), not one per calendar day. A 1-hour backtest over 1 year has ~8760 bars, but the code multiplies by 365, under-annualizing returns by a factor of ~24 and over-stating the Sharpe (denominator is also wrong but in the opposite direction by sqrt(24)). `_annualize_return` has the same problem: `years = periods / 365` where `periods` is bar count, not day count. The **engine's own** `_calculate_result` does use `_annualization_factor_from_index` correctly; but `PerformanceAnalyzer` (used by the report generator and the performance history API) ignores that and hard-codes 365.
- **Impact**: All metrics from `PerformanceAnalyzer` (Sharpe, Sortino, volatility, CAGR, Calmar) are wrong for any timeframe shorter than 1d. For 1h backtests Sharpe is overstated by ~鈭?4 鈮?4.9脳. These numbers feed the `PerformanceMetrics` dataclass, the JSON report, and strategy comparison tables.
- **Fix**: Compute `annualization_factor` from the equity index (same way `BacktestEngine._annualization_factor_from_index` does). Pass it into `PerformanceAnalyzer.analyze()` or infer it from `result.equity_curve` length vs `result.trades` timestamps.
- **Confidence**: High

---

### [CRIT-03] pnl_decomposer._apply_closing_fill: flip-trade opens position with wrong side

- **Severity**: Critical
- **Category**: Money-safety 鈥?FIFO accounting error
- **Location**: `core/accounting/pnl_decomposer.py:111鈥?13`
- **What's wrong**: When a closing fill has `qty_close > position_qty` (i.e. it also flips side), the residual `open_qty` is opened via `_open_position(symbol, side_lower, ...)` where `side_lower` is the **fill direction** (e.g. "sell" to close a long, then also open a short). However `_open_position` maps "buy" 鈫?"long" and "sell" 鈫?"short" (line 200). For a long-to-short flip ("sell" fill), `side_lower="sell"` 鈫?new position side="short". That is actually correct in isolation. BUT the residual `open_qty` comes from `on_fill(..., reduce_only=False)` where `reduce_only` was **originally False** for the entire fill. The opened short inherits the closing fee/slip fractions but these may be zero or misallocated because `open_fee` and `open_slippage` are computed from proportional ratios before `_apply_closing_fill` is called. Additionally, `_open_position` does NOT add realized fees for the open-leg to `pos.realized` 鈥?the `fee` argument is stored only in the lot, meaning it is never counted in `portfolio_breakdown()` until that lot is closed. Opening fees are therefore invisible to live portfolio P&L.
- **Impact**: Opening transaction costs disappear from `portfolio_breakdown()` until the next close. For high-frequency strategies the cumulative understatement of fee drag can be material. Flip trades (common on reversal strategies) silently lose the opening-leg fee.
- **Fix**: In `_open_position`, add opening fee/slippage directly to `pos.realized.fee` and `pos.realized.slippage_cost` (and subtract from `net_pnl`) at open time, mirroring the close path.
- **Confidence**: High

---

### [HIGH-01] BacktestEngine: open-trade net_pnl records -(fee + slippage_cost) but slippage_cost double-counts on close

- **Severity**: High
- **Category**: Money-safety 鈥?fee/slippage accounting
- **Location**: `core/backtest/backtest_engine.py:580` (`_execute_buy`), `core/backtest/backtest_engine.py:664` (`_execute_sell`), and `_close_position` line 703
- **What's wrong**: On entry, `net_pnl = -(fee + slippage_cost)`. The `slippage_cost` is `abs(exec_price - current_price) * quantity`, where `exec_price` is already the post-slippage execution price. On close, `gross_pnl = (exec_price - entry_price) * quantity`, where `entry_price = exec_price` from the entry (already includes slippage). So the entry slippage is already embedded in `gross_pnl` at close, AND it is also subtracted via the close `slippage_cost` term (line 703). Net result: entry-side slippage is counted twice in the close trade's `net_pnl`.
- **Impact**: Backtest net PnL understates performance by entry slippage 脳 quantity on every round trip. For dynamic slippage (ATR-based) on volatile markets this can be 0.1鈥?.3% per trade.
- **Fix**: Either (a) record entry slippage only via the `slippage_cost` field and compute `entry_price` as the raw (no-slippage) price in the position dict, or (b) on close, compute `gross_pnl` using the original reference price rather than the already-slipped exec_price, consistent with the open-leg accounting.
- **Confidence**: High

---

### [HIGH-02] PerformanceAnalyzer._analyze_trades includes funding-stage trades in win_rate / profit_factor

- **Severity**: High
- **Category**: Correctness 鈥?metric inflation/deflation
- **Location**: `core/backtest/performance_analyzer.py:215鈥?19`
- **What's wrong**: `close_like` includes trades where `trade_stage == "funding"`. Funding events can have non-zero `pnl` (positive or negative) and will appear in the wins/losses lists. This inflates or deflates win_rate and profit_factor depending on whether funding was net positive or negative, because funding isn't a trade 鈥?it's a cash flow event on an open position.
- **Impact**: Reported win_rate and profit_factor are meaningless for perpetual strategies that use funding. A long-biased strategy in a high-funding-rate environment will appear to have worse win_rate than it actually does.
- **Fix**: Filter to only `trade_stage == "close"` (same as `BacktestEngine._calculate_result` which correctly uses `close_trades` only).
- **Confidence**: High

---

### [HIGH-03] risk_manager.pre_trade_check: gross exposure check uses position.value (unrealized mark-to-market) but single-position cap uses order notional vs equity 鈥?inconsistent basis

- **Severity**: High
- **Category**: Money-safety 鈥?risk-limit bypass
- **Location**: `core/risk/risk_manager.py:877鈥?89`
- **What's wrong**: The gross exposure check sums `p.value` for all positions (mark-to-market including unrealized PnL) and compares to `equity * max_gross_exposure_ratio`. In a falling market, open longs shrink in value while equity also shrinks 鈥?the ratio may stay under the cap even as actual notional risk has increased. More critically, `current_gross` can be **negative** if short positions show unrealized gains (position.value = margin + unrealized_pnl; for a profitable short this is positive, but for a losing short `value < margin`). The check `current_gross + notional > gross_cap` could then pass even when actual notional exposure is very large.
- **Impact**: In adverse markets (positions losing), `p.value` falls so the gross-exposure check becomes looser exactly when risk is highest. A new order can slip through when notional exposure should block it.
- **Fix**: Use position notional (qty 脳 current_price) as the gross exposure measure, not the mark-to-market position.value.
- **Confidence**: Medium-High

---

### [HIGH-04] BacktestEngine: slippage on short close applied in wrong direction (adds to, not against, position)

- **Severity**: High
- **Category**: Money-safety 鈥?slippage direction error
- **Location**: `core/backtest/backtest_engine.py:696`
- **What's wrong**: For closing a short, `exec_price = current_price * (1 + slip_rate)` 鈥?the close price is higher (you buy back at a premium, realistic). However `gross_pnl = (entry_price - exec_price) * quantity` correctly captures the higher repurchase cost. Then `slippage_cost = abs(exec_price - current_price) * quantity` is also subtracted in `net_pnl` (line 703): `net_pnl = gross_pnl + accrued_funding - fee - slippage_cost`. Because `gross_pnl` already reflects `exec_price > current_price`, the slippage impact is already embedded in `gross_pnl`. Subtracting `slippage_cost` again double-counts closing slippage for short positions, exactly the same bug as HIGH-01 (same root cause, just on the close side). The symmetry between long open (HIGH-01) and short close is identical.
- **Impact**: Short strategies systematically overstate costs by 1脳 slippage per close. Long strategies are doubly penalised per round-trip (open + close slippage).
- **Fix**: As in HIGH-01: standardize on a single place to account for slippage; do not use both adjusted exec_price and a separate slippage_cost subtraction simultaneously.
- **Confidence**: High

---

### [HIGH-05] ExitEngine bar_return uses px_prev_close not entry_price for first-bar return

- **Severity**: High
- **Category**: Correctness 鈥?off-by-one / lookahead
- **Location**: `core/backtest/exit_engine.py:312`, `core/backtest/execution_arrays.py:239`
- **What's wrong**: When a protective stop or reversal triggers on the **entry bar** (only relevant when `allow_same_bar_exit=True`), the return is computed as `direction * (stop_price / px_prev_close - 1)` where `px_prev_close` is the close of the **previous bar**. But the entry was at `px_close` (current bar close); the correct first-bar return baseline is `entry_price` (= `px_close` from the entry tick). Using prev_close over-estimates the return by `(px_close / px_prev_close - 1)` for the whole hold period.
- **Impact**: When same-bar exits occur (momentum strategies), each trade's P&L is misstated by one bar of return. At 1h timeframe on BTC this can be 0.5鈥?% per trade.
- **Fix**: On the bar that opened the trade (`idx == active.entry_idx`), substitute `entry_price` for `px_prev_close` in the return calculation.
- **Confidence**: High

---

### [HIGH-06] risk_manager._check_new_day does NOT clear trading_halted for the non-active scope

- **Severity**: High
- **Category**: Correctness 鈥?risk state residue
- **Location**: `core/risk/risk_manager.py:434鈥?55` (`_check_new_day`), line 197 (`_check_new_day_for_state`)
- **What's wrong**: `_check_new_day_for_state` (used for non-active scopes) does reset `trading_halted` (line 200). However `_check_new_day` (active scope, line 434) also resets `trading_halted=False` at line 450 correctly. The problem is a different one: when the scope switches from `paper` to `live` (or vice versa) via `set_account_scope`, the **stored scope state** is snapshotted before saving, so a `trading_halted=True` from yesterday is preserved in `_scope_states[scope]`. If the process restarts mid-halt but a new day has started, the persisted trade history is loaded but there is **no path** that clears `trading_halted` on load. `_initial_scope_state` sets it False, but `_load_persisted_trade_history` only loads trade rows; it does not check if a new day has started. Result: after restart on a new day, the system can start with an unexpected halt or (worse) un-halted when the halt should persist.
- **Impact**: Post-restart behavior diverges from intent: either stale halts block trading all day, or a valid halt is cleared silently. In live trading this is a safety regression.
- **Fix**: In `_load_persisted_trade_history` (or in `__init__` after loading), check the date of the most recent persisted trade versus today; if it is a different calendar day, start fresh (clear daily counters and `trading_halted`). Alternatively persist the full runtime state (including halt flag + daily start) in the trade history file.
- **Confidence**: Medium

---

### [MED-01] PerformanceAnalyzer.total_trades counts ALL trades including open-leg trades

- **Severity**: Medium
- **Category**: Correctness 鈥?metric error
- **Location**: `core/backtest/performance_analyzer.py:121`
- **What's wrong**: `total_trades = len(trades)` where `trades = result.trades`. This includes open-stage and funding-stage trades in the count. `BacktestEngine._calculate_result` correctly uses `close_trades` for this metric. `PerformanceAnalyzer` inflates total_trades by ~2脳 (open+close per round trip) plus funding events.
- **Impact**: Reported trade count is 2鈥?脳 actual round trips. `avg_trades_per_day` is also wrong. Comparison tables in `compare_strategies` and JSON reports are misleading.
- **Fix**: Filter to `trade_stage == "close"` only: `close_trades = [t for t in trades if getattr(t,"trade_stage","") == "close"]`.
- **Confidence**: High

---

### [MED-02] Sortino denominator uses population std of downside returns, not sample std (ddof=0)

- **Severity**: Medium
- **Category**: Correctness 鈥?formula error
- **Location**: `core/backtest/performance_analyzer.py:190鈥?93`
- **What's wrong**: `downside_std = np.std(downside_returns) * np.sqrt(365)` uses `ddof=0` (population). For a sample of downside returns (typically N=50鈥?00 trades), this understates downside risk and overstates the Sortino ratio. The same issue exists at line 157 for volatility (`np.std(returns)`, `ddof=0`). Notably `cost_models.py` uses `ddof=1` explicitly (with a comment about this exact issue), but `performance_analyzer.py` does not.
- **Impact**: Sortino ratio is slightly inflated; for small samples (< 100 trades) the bias can be 1鈥?%.
- **Fix**: Use `np.std(downside_returns, ddof=1)` and `np.std(returns, ddof=1)`.
- **Confidence**: High

---

### [MED-03] BacktestEngine: `_capital` deducted on entry but `notional` used for fee, yet margin != notional for leveraged positions

- **Severity**: Medium
- **Category**: Money-safety 鈥?position sizing inconsistency
- **Location**: `core/backtest/backtest_engine.py:524鈥?41` and `608鈥?25`
- **What's wrong**: `notional = _capital * position_size_pct`, `margin = notional / leverage`, `quantity = notional / current_price`. Capital deducted is `margin + fee`. But `fee = notional * fee_rate` and `notional = quantity * current_price`. For leverage > 1, the actual capital required (margin) is less than notional, but fee is charged on full notional 鈥?that part is correct for perpetuals. However the position's `margin` field then only stores the reduced-margin amount, while `_close_position` returns `margin + net_pnl`. If fee was charged on `notional` at open but only `margin` is returned at close, the fee spent at open is effectively lost (never credited back to `_capital`). The accounting is: open deducts `margin + fee`, close returns `margin + net_pnl`. Since `net_pnl = gross_pnl - fee - slip + funding`, the close fee is from the **close leg** only. The open-leg fee paid at entry is never returned. This is only correct if `net_pnl` for the close trade also includes the open-leg fee, which it does NOT (gross_pnl = price difference only, fee = close notional 脳 fee_rate).
- **Impact**: Each round trip "loses" the open-leg fee twice (once deducted from capital at entry, once counted in the cost_breakdown). Overstates total cost by 50%.
- **Fix**: Ensure the open-leg fee is reflected either in the position's margin (capital basis) so `_capital` deduction is `margin + fee` and return on close is also `margin + fee + net_pnl_excluding_open_fee`, OR track it separately in `BacktestTrade` cost breakdown and do NOT also deduct from capital. Pick one consistent convention.
- **Confidence**: Medium

---

### [MED-04] ExitEngine.compute_atr uses EWM with alpha=1/period (RMA not EWM), but bar-0 min_periods=period means first 13 bars return NaN 鈥?atr_for_bar.shift(1) then gives wrong ATR for bar 14

- **Severity**: Medium
- **Category**: Correctness 鈥?indicator calculation
- **Location**: `core/backtest/exit_engine.py:152鈥?54`
- **What's wrong**: `atr = true_range.ewm(alpha=1/span, min_periods=span, adjust=False).mean()` followed by `atr_for_bar = atr_series.shift(1).ffill()`. For the first `span` bars atr is NaN; the `shift(1).ffill()` means that bar `span` (first valid ATR) uses the ATR from bar `span-1`, which is NaN and then ffill'd from nothing 鈫?still NaN. Bar `span+1` is the first bar with a non-NaN ATR, but this ATR was computed using the close of bar `span` 鈥?that close is already "known" on bar `span+1`, so using it as bar `span+1`'s stop doesn't introduce lookahead. The ffill propagates the last valid ATR forward, which means that after any gap (NaN close), the stop uses a stale ATR that could be many bars old.
- **Impact**: ATR stops are slightly stale after price data gaps. Generally a minor issue but can cause stops to fire at wrong prices after gaps in crypto (exchange maintenance etc.).
- **Fix**: After `ffill()`, optionally cap the staleness; or accept this minor risk. No critical money impact.
- **Confidence**: Medium

---

### [MED-05] risk_manager.pre_trade_check not thread-safe: reads and checks `_trading_halted` / `_daily_trades` without the `_state_lock`

- **Severity**: Medium
- **Category**: Concurrency
- **Location**: `core/risk/risk_manager.py:788鈥?09`
- **What's wrong**: `pre_trade_check` calls `_check_new_day()` and reads `self._trading_halted`, `self._daily_trades`, `self._max_open_positions` etc. without holding `self._state_lock`. Meanwhile `record_trade` increments `self._daily_trades` under `_state_lock`. A race between a signal check and a parallel trade record can allow `daily_trades >= max_daily_trades` to pass briefly if the check and increment are interleaved.
- **Impact**: Under high concurrency (multiple strategies firing simultaneously) the daily trade limit could be exceeded by 1鈥? trades. Not critical for a bot with strategy coordination, but a correctness issue.
- **Fix**: Acquire `_state_lock` at the start of `pre_trade_check`, or make the critical reads atomic with a copy-then-check pattern.
- **Confidence**: Medium

---

### [MED-06] CircuitBreaker._load_from_disk uses bare `**row.items()` deserialization 鈥?unknown keys in persisted JSON cause `TypeError`

- **Severity**: Medium
- **Category**: Security / Quality 鈥?unsafe deserialization
- **Location**: `core/risk/circuit_breaker.py:158`
- **What's wrong**: `_StrategyState(**{k: v for k, v in row.items() if k in _StrategyState.__dataclass_fields__})` 鈥?the dict comprehension filters by known fields. This is actually safe. However the `_PortfolioState` deserialization at line 161 does the same filter. If a future schema adds fields to the dataclass but old files lack them, the constructor will use dataclass defaults (fine). BUT if the persisted JSON has `tripped=True` with a `tripped_at` field pointing to an old timestamp, the circuit breaker boots into a tripped state with no validation that the trip is still relevant. There is an auto-clear mechanism (`clear_strategy_false_trip`) that is only called from `run_circuit_breaker_checks`, which is called by the monitor task 鈥?but the monitor task may not run for several minutes after startup.
- **Impact**: After a process restart, a stale circuit-breaker trip can block all new strategy entries for up to the monitor interval (typically 5 minutes). In fast-moving markets this is costly.
- **Fix**: In `_load_from_disk`, if `tripped=True` and `tripped_at` is older than 24h (or the configured strategy_daily_threshold window), automatically clear the trip. Alternatively, add a startup sweep in `__init__`.
- **Confidence**: Medium

---

### [MED-07] pnl_decomposer on_funding adds to `realized.net_pnl` but does NOT add to `realized.gross_pnl` 鈥?portfolio_breakdown net_pnl != gross - fee - slip + funding

- **Severity**: Medium
- **Category**: Money-safety 鈥?accounting invariant violation
- **Location**: `core/accounting/pnl_decomposer.py:131鈥?33`
- **What's wrong**: `on_funding` does `pos.realized.funding_pnl += amount` and `pos.realized.net_pnl += amount`. In `_apply_closing_fill` the final `net_pnl` is computed as `gross_pnl - fee - slippage_cost + funding_pnl`. This is correct for the close event. However in `portfolio_breakdown()`, the formula uses `net += p.realized.net_pnl` which already includes funding, while `funding += p.realized.funding_pnl` is also summed separately. The `portfolio_breakdown` return dict does NOT compute a synthetic total; the caller gets separate fields. This is fine if callers use the individual fields, but the comment-implied invariant `net_pnl == gross_pnl - fee - slippage_cost + funding_pnl` fails for open positions that have received funding before closing (because `realized.net_pnl` was incremented by funding twice: once by `on_funding` and once by `_apply_closing_fill` which adds `funding_pnl` to `net_pnl` again at close).
- **Impact**: `portfolio_breakdown()["net_pnl"]` double-counts funding for positions that have already received funding payments before closing. This is the same class of bug as the known funding double-count in `backtest_engine` (which was explicitly fixed previously).
- **Fix**: In `on_funding`, increment only `funding_pnl` (not `net_pnl`). Recompute `net_pnl` only in `_apply_closing_fill` from components.
- **Confidence**: High

---

### [LOW-01] report_generator.py uses `datetime.now()` (naive) instead of `datetime.now(timezone.utc)` for report timestamp

- **Severity**: Low
- **Category**: Quality
- **Location**: `core/backtest/report_generator.py:62`
- **What's wrong**: `datetime.now().strftime(...)` is timezone-naive. The system uses UTC everywhere else.
- **Fix**: `datetime.now(timezone.utc).strftime(...)`.
- **Confidence**: High

---

### [LOW-02] performance_analyzer.generate_monthly_returns uses ALL trades (open+close+funding) for monthly P&L

- **Severity**: Low
- **Category**: Correctness
- **Location**: `core/backtest/performance_analyzer.py:265鈥?78`
- **What's wrong**: `pd.DataFrame([{"date": t.timestamp, "pnl": t.pnl} for t in result.trades])` 鈥?includes open-stage trades where `pnl=0.0` (harmless for sum) and funding-stage trades where `pnl = funding_cash`. Monthly P&L will conflate trading P&L and funding.
- **Fix**: Filter to close-stage trades for monthly trading PnL; separately report funding if needed.
- **Confidence**: Medium

---

### [LOW-03] BacktestEngine: position max_unrealized_pct computed using raw unrealized / entry_notional (no direction sign)

- **Severity**: Low
- **Category**: Correctness
- **Location**: `core/backtest/backtest_engine.py:752鈥?55`
- **What's wrong**: `favorable_pct = unrealized / entry_notional`. For a long, positive unrealized is good; for a short, positive unrealized means the position is also favorable. But a short's unrealized can be positive only when it's winning (price fell). So the sign logic is correct incidentally. However `max_unrealized_pct` is never reset between partial closes, so it can accumulate inaccurately.
- **Fix**: Reset `max_unrealized_pct=0` when a position is partially reduced.
- **Confidence**: Low

---

### [LOW-04] rate_limit_and_reconnect.acquire(wait=True) uses time.sleep in a loop 鈥?GIL contention if many strategies use this path

- **Severity**: Low
- **Category**: Performance
- **Location**: `core/execution/rate_limit_and_reconnect.py:173鈥?88`
- **What's wrong**: Synchronous `time.sleep` inside `acquire(wait=True)` can block other threads. The code correctly raises `RuntimeError` if called from within a running event loop (line 163-165). But if used from a non-async context with many threads, repeated `time.sleep` calls cause GIL contention.
- **Fix**: Acceptable for current use; document that async paths should use `acquire_async`.
- **Confidence**: Low

---

### [LOW-05] circuit_breaker: `_fire_close_positions` can call `asyncio.run(result)` even if `_main_loop` is not None but not reachable 鈥?silently swallows failure

- **Severity**: Low
- **Category**: Quality 鈥?swallowed exception
- **Location**: `core/risk/circuit_breaker.py:459鈥?65`
- **What's wrong**: The `asyncio.run(result)` fallback (line 462) creates a new event loop. If the calling thread is inside `asyncio.to_thread`, the main loop is running but unreachable for `run_coroutine_threadsafe`. Creating a second event loop will conflict with any pending coroutines. The `except` clause only logs at DEBUG level.
- **Fix**: Add a warning log when the asyncio.run fallback fires; this should be a visible anomaly.
- **Confidence**: Medium

---

### [LOW-06] order_state_machine.apply_update: `filled_qty` in OrderEvent set to `snap.filled_qty` not passed `filled_qty` when `filled_qty is not None`

- **Severity**: Low
- **Category**: Quality 鈥?minor logic inconsistency
- **Location**: `core/execution/order_state_machine.py:291鈥?94`
- **What's wrong**: `filled_qty=snap.filled_qty if filled_qty is not None else filled_qty`. The intent appears to be: if caller supplied `filled_qty`, record the current cumulative `snap.filled_qty` in the event. But this means the event history records the post-update quantity, not the delta that arrived. For reconciliation the delta (original `filled_qty` argument) would be more useful.
- **Fix**: Change to `filled_qty=float(filled_qty) if filled_qty is not None else None` to record the incoming event value.
- **Confidence**: Medium
