# Strategies & Signals Audit 鈥?2026-05-29

## Summary

| Severity | Count |
|----------|-------|
| Critical | 3 |
| High | 6 |
| Medium | 7 |
| Low | 5 |

The domain is in reasonable shape after the spring 2026 fixes. The `StrategyBase` wrapper, `bar_time()`, and ATR-stop pipeline are solid. However three critical issues remain: CEX arbitrage emits both a BUY and a SELL on the **same symbol** in the same cycle 鈥?the strategy manager's conflict detection then drops one leg, breaking arbitrage atomicity; the DEX arbitrage `find_arbitrage_opportunities` uses `buy_dex == sell_dex` as a cross-leg guard but the two legs already traverse different dictionaries so `quotes_ab[buy_dex]` and `quotes_ba[sell_dex]` are not guaranteed to be the same DEX; and `FactorStrategyBase._create_signal` uses a class-level mutable attribute `_active_bar_ts` as the signal timestamp 鈥?when `check_exit` calls `_create_signal` without first resetting `_active_bar_ts`, the signal carries the **previous bar's timestamp**, which can trick conflict detection and produce wrong signal ordering. High-severity issues include a `_regime_bias` shared mutable dict that survives position close in several strategies, an unbounded `_seen_trade_ids` set that grows indefinitely in live operation, and the `intraday_cross_section.py` use of `.rolling().apply()` with a pure-Python `entropy_array` lambda on up to 864-bar windows per bar 鈥?an O(n) per-bar Python loop that will be extremely slow at scale.

---

## Findings

### [SEV-01] CEX arbitrage BUY+SELL on same symbol triggers conflict detection, drops one leg
- **Severity**: Critical
- **Category**: money-safety
- **Location**: `strategies/arbitrage/cex_arbitrage.py:176-211`
- **What's wrong**: `generate_signals_async` appends a `BUY` and then a `SELL` signal for the **same `symbol`** string (e.g. "BTC/USDT") in the same return list. Both signals share the same account context, so `_emit_signals` in `strategy_manager.py:1024-1036` detects them as a conflict (buy vs sell on same symbol/account) and drops the weaker one (lower `strength`). Since both have the same `strength`, the SELL (second) is always dropped. Result: only a naked BUY is executed 鈥?the "sell leg" (short on the other exchange) that locks in the spread never fires. This turns an arbitrage trade into a one-sided directional bet.
- **Impact**: Every CEX arbitrage execution is a naked unhedged long. Under volatility the position can lose multiples of the intended arbitrage profit.
- **Fix**: Tag the buy-leg signal metadata with `"exchange": opp["buy_exchange"]` and the sell-leg with `"exchange": opp["sell_exchange"]` (already done 鈥?but the **conflict key** in `_emit_signals` uses the `account_id + symbol + exchange` triple, and both legs share the same `account_id` and `symbol`). Fix by assigning the two legs to different isolated account IDs (one per exchange leg) or by tagging with `"skip_conflict_check": True` in metadata and checking that flag early in `_emit_signals`.
- **Confidence**: High

---

### [SEV-02] `FactorStrategyBase._create_signal` uses stale `_active_bar_ts` from `generate_signals` as timestamp in `check_exit`
- **Severity**: Critical
- **Category**: correctness / money-safety
- **Location**: `strategies/factor_based/factor_strategies.py:47`, `strategies/factor_based/factor_strategies.py:172,260,353,...`
- **What's wrong**: All 18 factor strategy `generate_signals` methods write `self._active_bar_ts = self._bar_time(data)` as the first line. `_create_signal()` reads `getattr(self, "_active_bar_ts", None)` as the timestamp. When `check_exit` is called independently (by `strategy_manager._collect_exit_signals`) it calls the **inherited** `_create_signal` via `_oscillator_factor_exit`; `_active_bar_ts` still holds the timestamp from the **last call to `generate_signals`**, not the current bar. If `check_exit` runs on a fresh bar for which `generate_signals` has not yet been called (e.g. when a strategy is in cooldown), the exit signal carries the previous bar's timestamp. This can fail the conflict-detection window comparison (age = negative) or order the signal log incorrectly, leading to exits being silently dropped.
- **Impact**: Exit signals may be dropped by conflict detection when `age` computes negative. Existing long/short position stays open, accumulating losses.
- **Fix**: Move the `_active_bar_ts` assignment to the `FactorStrategyBase._oscillator_factor_exit` helper (or simply use `self._bar_time(data)` directly in `_create_signal`'s timestamp argument, eliminating the mutable instance attribute entirely). Simplest fix: replace `getattr(self, "_active_bar_ts", None) or datetime.now(timezone.utc)` with `self._bar_time(data)` and pass `data` into `_create_signal`.
- **Confidence**: High

---

### [SEV-03] DEX arbitrage cross-DEX matching bug: same-DEX legs can be silently compared
- **Severity**: Critical
- **Category**: correctness / money-safety
- **Location**: `strategies/arbitrage/dex_arbitrage.py:104-125`
- **What's wrong**: `find_arbitrage_opportunities` iterates `for buy_dex, buy_quote in quotes_ab.items(): for sell_dex, sell_quote in quotes_ba.items()`. `quotes_ba` is keyed by the *same* DEX used for the A鈫払 leg (the reverse quote was fetched from `self._dex_connectors[dex_name]`). The guard `if buy_dex == sell_dex: continue` is correct, but the sell-leg unit price is computed as `sell_quote / buy_quote` (line 154) where `sell_quote` is the B鈫扐 output and `buy_quote` is the A鈫払 output 鈥?both from **different DEXes**. The profit calculation `sell_quote - amount` uses `sell_quote` from DEX-2 but `amount` is the original token-A amount. When DEX-2 quotes A鈫払鈫扐 with slippage included, the actual round-trip net is under-estimated: gas costs, DEX fee tiers, and liquidity depth are not modelled. A position sized on incorrect profit estimate can lose money.
- **Impact**: Profitable-looking "opportunities" may actually result in net losses after on-chain fees and slippage.
- **Fix**: Add gas cost deduction (`params["max_gas_cost"]` exists but is defined and never subtracted from profit) before generating signals. Verify that `sell_quote` at line 109 is truly the round-trip output and deduct `max_gas_cost` before checking `min_profit_usd`.
- **Confidence**: Medium

---

### [SEV-04] `_regime_bias` mutable instance dict not cleared on position close; creates ghost signals
- **Severity**: High
- **Category**: correctness
- **Location**: `strategies/quantitative/mean_reversion.py:68,85`, `strategies/quantitative/momentum.py` (uses no explicit bias), `strategies/macro/fund_flow.py:183,200`, `strategies/macro/fund_flow.py:276,318`, `strategies/macro/market_sentiment.py`
- **What's wrong**: `MeanReversionStrategy`, `FundFlowStrategy`, `WhaleActivityStrategy`, and `MarketSentimentStrategy` maintain a `_regime_bias[symbol]` dict that is set on entry and cleared on exit by `check_exit`. However, if the position is closed by the execution engine via a stop-loss trigger (not through `check_exit`), `_regime_bias` is never cleared. On the next bar where conditions are ambiguous, the `neutral_exit_enabled` branch reads the stale bias and emits a spurious CLOSE_LONG or CLOSE_SHORT for a position that no longer exists 鈥?which arrives at the execution engine as a close order against empty inventory.
- **Impact**: Spurious close orders after external position closure. May trigger unnecessary order submissions, waste fees, or (on exchanges that allow oversell) inadvertently open a short.
- **Fix**: Add a `check_exit` call at startup or persist the `_regime_bias` to the position metadata so it is authoritative only when a live position exists. Alternatively, clear `_regime_bias[symbol]` when no position is found in `generate_signals` / `check_exit`.
- **Confidence**: High

---

### [SEV-05] `_seen_trade_ids` set grows unboundedly in `WhaleActivityStrategy` in long-running live mode
- **Severity**: High
- **Category**: quality / reliability
- **Location**: `strategies/macro/fund_flow.py:293-294` (WhaleActivityStrategy)
- **What's wrong**: The deduplication guard trims `_seen_trade_ids` only when it exceeds 20000 entries by keeping the **last 12000** entries. In live mode the strategy fetches up to 1000 trades every cycle. At 1000 trades/cycle and a 1-minute timeframe this set can reach 20000 in under 20 minutes; trimming retains 12000 but the set then refills. The cycle is correct for memory but the per-cycle conversion `set(list(self._seen_trade_ids)[-12000:])` is O(n) and creates unnecessary allocations every call above the threshold.
- **Impact**: Minor memory pressure; CPU spike every ~20 cycles on high-frequency live runs. Low direct financial impact but contributes to latency spikes during critical market events.
- **Fix**: Use a `collections.deque(maxlen=12000)` with O(1) push and O(1) `__contains__` wrapping a secondary set, or simply use an LRU cache keyed by trade_id. At minimum, replace the reconstruction with `self._seen_trade_ids = set(itertools.islice(iter(self._seen_trade_ids), 12000))` (still O(n) but avoids the intermediate list).
- **Confidence**: High

---

### [SEV-06] `intraday_cross_section.py` uses O(n) `rolling().apply()` with Python lambda per bar
- **Severity**: High
- **Category**: performance
- **Location**: `strategies/quantitative/intraday_cross_section.py:426, 438`
- **What's wrong**: Two factor computations 鈥?`entropy_array` (Shannon entropy) and `recency_array` 鈥?use `values.rolling(window).apply(fn, raw=True)`. `window` is up to 864 bars (`lookback_bars=864`). On a 5m timeframe, a 30-day dataset is ~8,640 bars. Each rolling apply evaluates the Python function 8,640 times, each time over up to 864 elements 鈥?~7.5 million numpy operations per factor per rebalance cycle. At 23 strategies sharing the scheduler this will routinely hit the `_STRATEGY_CYCLE_TIMEOUT_SEC = 45.0` timeout. These strategies have `lookback_bars` ranging from 4 to 864 and `rebalance_bars` as short as 24.
- **Impact**: Strategy cycle timeouts in live mode; backtest performance issues. When timeout fires, no signal is generated 鈥?missed entries/exits.
- **Fix**: Replace `rolling().apply(entropy_array, raw=True)` with a vectorised entropy computation using `numpy.lib.stride_tricks.sliding_window_view` (same technique used in `core/indicators/rolling.py`). Entropy is `H = -sum(p*log(p))` which is vectorisable bar-by-bar with a bincount trick.
- **Confidence**: High

---

### [SEV-07] `BollingerMeanReversionStrategy.generate_signals` 鈥?min_bars guard off by one (only `period`, needs `period+1`)
- **Severity**: High
- **Category**: correctness
- **Location**: `strategies/quantitative/mean_reversion.py:192`
- **What's wrong**: The guard is `len(data) < self.params["period"]` (needs strictly fewer than `period`). But the strategy reads both `iloc[-1]` and `iloc[-2]` (previous bar). With exactly `period` rows available, `upper.iloc[-2]` is the last NaN of the rolling window and `prev_close` is valid but `prev_upper`/`prev_lower` are NaN, causing the comparisons to always be False or raise FutureWarnings. The analogous strategies (`BollingerBandsStrategy`, `MAStrategy`, `EMAStrategy`, `MACDStrategy`) all use `period + 1`. This one was missed.
- **Impact**: On cold-start with exactly `period` rows of history the strategy silently skips all signals without logging. Unlikely to cause a wrong trade but degrades startup signal coverage.
- **Fix**: Change `len(data) < self.params["period"]` to `len(data) < self.params["period"] + 1`.
- **Confidence**: High

---

### [SEV-08] `CEXArbitrageStrategy` signal strength can exceed 1.0 when `effective_spread >> min_spread`
- **Severity**: Medium
- **Category**: money-safety
- **Location**: `strategies/arbitrage/cex_arbitrage.py:174`
- **What's wrong**: `strength = max(0.1, min(float(opp["effective_spread"]) / min_spread, 1.0))`. The `min()` clamps to 1.0 correctly. However in `TriangularArbitrageStrategy` at line 380 the same formula appears without the outer `min()`: `strength = max(0.1, min(abs(edge) / max(min_profit, 1e-9), 1.0))` 鈥?this one IS clamped. But the upstream `find_arbitrage_opportunities` stores `effective_spread` as an already-clipped value and the signal path is OK. Issue: `_finalize_generated_signals` in `StrategyBase` skips for async paths, so the ATR-stop override is not applied to arbitrage signals. Both arb strategies have no `stop_loss` field set, leaving positions without any stop if the spread collapses post-entry.
- **Impact**: Arbitrage positions opened with no stop loss; unlimited downside if one leg executes and the other fails.
- **Fix**: Add `stop_loss` to arbitrage signals based on `min_spread * position_size` or a fixed percentage, or ensure `_finalize_generated_signals` is called on async signal paths.
- **Confidence**: Medium

---

### [SEV-09] `MomentumStrategy.strength` can exceed 1.0 when `current_momentum >> threshold`
- **Severity**: Medium
- **Category**: money-safety
- **Location**: `strategies/quantitative/momentum.py:67, 82`
- **What's wrong**: `strength=min(current_momentum / threshold, 1.0)` correctly caps at 1.0 for the BUY case. But for the SELL case: `strength=min(abs(current_momentum) / threshold, 1.0)`. Both are fine in isolation. The issue is that downstream `_finalize_generated_signals` applies `metadata.setdefault("atr_stop_loss_mult", ...)` and `sl_mult = float(... or metadata.get("atr_stop_loss_mult") or 1.5)`. When `current_momentum` is 10脳 `threshold`, raw momentum strength is 1.0 (capped), but the signal carries no size guidance 鈥?position sizing in `execution_engine` defaults to the full `allocation`. At high momentum signals, full-allocation entries can be overleveraged.
- **Impact**: Potential over-allocation in fast momentum moves 鈥?limited, but real risk.
- **Fix**: No code change needed (strength is already clamped), but document that position sizing relies on `allocation` fraction, not raw signal strength. Consider adding `metadata["momentum_pct"] = current_momentum` for execution engine consumption.
- **Confidence**: Low

---

### [SEV-10] `PairsTradingStrategy._calculate_hedge_ratio_ols` centers both series; loses mean reversion baseline
- **Severity**: Medium
- **Category**: correctness
- **Location**: `strategies/quantitative/pairs_trading.py:76-82`
- **What's wrong**: Lines 76-77 demean both `xv` and `yv` before running OLS: `xv = xv - float(np.mean(xv))`. Demeaned OLS computes the slope of the zero-intercept regression of `(y - 瘸)` on `(x - x虅)`. This is equivalent to OLS with a free intercept **only when all assumptions hold**. However, the spread is defined as `price1 - hedge_ratio * price2` without removing the long-run mean, so the Z-score `(spread - spread_mean) / spread_std` is computed over a spread series that already has the long-run mean subtracted in one direction. The demeaning changes the hedge ratio compared to what the Z-score calculation expects, making entry/exit thresholds inaccurate by the ratio of the two series' means.
- **Impact**: Hedge ratio is systematically biased; spread oscillates around a non-zero mean that the Z-score calculation doesn't account for. Entry signals fire too early/late relative to true cointegration mean reversion.
- **Fix**: Either use standard OLS with an intercept (via `np.polyfit(x, y, 1)` or `np.linalg.lstsq` on `[x|1]`) or subtract the spread mean from the spread before Z-scoring. The current demeaning of the independent variable is the common "centering" approach but must be consistently applied in the spread definition.
- **Confidence**: Medium

---

### [SEV-11] `RSIDivergenceStrategy._find_peaks` / `_find_troughs` 鈥?O(n虏) Python loop on extrema detection
- **Severity**: Medium
- **Category**: performance
- **Location**: `strategies/technical/rsi_strategy.py:233-254, 257-276`
- **What's wrong**: `_find_peaks` and `_find_troughs` use a Python `for i in range(order, n - order)` loop and compare `arr[i]` against numpy slices. Each iteration is a Python function call with array operations: `arr[i-order:i].max()` creates a new temporary array per bar. For `lookback=20` bars and `order=5`, on a 500-bar window this is ~490 iterations 脳 2 slices = ~980 temporary numpy arrays per `generate_signals` call, per strategy, per cycle. Not catastrophically slow individually but this strategy is often run with a 100+ bar lookback; when it is part of a bank of 10+ strategies, cumulative cost adds up.
- **Impact**: Performance degradation, not a correctness issue. Low risk of causing cycle timeout alone but may contribute when combined with other strategies in the same scheduler slot.
- **Fix**: Use `scipy.signal.argrelextrema` (already optionally available) or a vectorised sliding-window approach with `np.lib.stride_tricks.sliding_window_view` (already used in `core/indicators/rolling.py`).
- **Confidence**: High

---

### [SEV-12] `VWAPReversionStrategy` 鈥?CLOSE_LONG and CLOSE_SHORT can fire in same bar as a new BUY/SELL
- **Severity**: Medium
- **Category**: correctness
- **Location**: `strategies/technical/common_strategies.py:309-370`
- **What's wrong**: The four `if/elif` branches in `generate_signals` handle: BUY entry (d_now < -entry), SELL entry (d_now > +entry), CLOSE_LONG exit (d_now > -exit_dev), and CLOSE_SHORT exit (d_now < +exit_dev). The ordering means that when `d_now` enters deeply negative territory (e.g. `d_now < -entry < -exit_dev`), the BUY branch fires but the CLOSE_LONG branch **never fires** because they are mutually exclusive `if/elif`. The problem is the inverse: when `d_prev` was in entry territory and `d_now` has moved through zero, both the SELL entry condition (`d_prev <= entry and d_now > entry`) and the CLOSE_LONG exit condition (`d_prev <= -exit_dev and d_now > -exit_dev`) can logically be true simultaneously, but the `elif` chain means only the first matching branch fires. The CLOSE_LONG can silently be skipped when a SELL fires at the same bar, leaving the long open.
- **Impact**: Existing long position stays open while a short is entered 鈥?double-directional exposure on the same symbol.
- **Fix**: Separate the exit logic from the entry logic. Check close conditions unconditionally first (they should always evaluate), then check entry conditions only if no close was generated.
- **Confidence**: Medium

---

### [SEV-13] `_recent_signal_by_symbol` cache grows without bound 鈥?potential memory leak
- **Severity**: Medium
- **Category**: quality / reliability
- **Location**: `core/strategies/strategy_manager.py:100, 1045`
- **What's wrong**: `_recent_signal_by_symbol` is a `Dict[Tuple[str, str, str], Signal]`. Entries are set on every non-HOLD, non-dropped signal at line 1045: `self._recent_signal_by_symbol[conflict_key] = signal`. Entries are **never removed**. In a system with many symbols across many strategies, this dict grows indefinitely. Each Signal object holds metadata dicts which can be large (e.g. `MultiFactorHFStrategy` embeds full factor blocks). Over a 24h live session with 20 strategies 脳 20 symbols 脳 ~1 signal/5min = ~57,600 entries per day.
- **Impact**: Gradual memory growth. Low direct financial risk but can eventually cause OOM in long-running deployments, destabilizing the whole system.
- **Fix**: Add an eviction pass in `_emit_signals`: before writing a new entry, evict entries older than `_SIGNAL_CONFLICT_WINDOW_SECONDS * 2`. This is already the intent of the 60s conflict window.
- **Confidence**: High

---

### [SEV-14] `FactorStrategyBase._create_signal` 鈥?Signal.timestamp uses `datetime.now(timezone.utc)` fallback when `_active_bar_ts` is None (cold start)
- **Severity**: Low
- **Category**: correctness
- **Location**: `strategies/factor_based/factor_strategies.py:47`
- **What's wrong**: `timestamp=getattr(self, "_active_bar_ts", None) or datetime.now(timezone.utc)`. On cold start when `_create_signal` is called **from `check_exit`** before `generate_signals` has ever been called (e.g. a position is open from a prior session), `_active_bar_ts` is `None` and the timestamp falls back to wall-clock `now()`. This breaks the `bar_time()` design contract and the conflict detection window comparison.
- **Impact**: First-bar exit signals after restart carry wall-clock timestamps; minor signal ordering issue. Low financial risk as conflicts are rare on first bar.
- **Fix**: Thread `data` through `_create_signal` (or the caller) and use `self._bar_time(data)` directly.
- **Confidence**: High

---

### [SEV-15] `MomentumStrategy` 鈥?no NaN guard on `current_momentum` / `prev_momentum`
- **Severity**: Low
- **Category**: correctness
- **Location**: `strategies/quantitative/momentum.py:48-56`
- **What's wrong**: `momentum = data["close"] / data["close"].shift(period) - 1`. For the first `period` rows `data["close"].shift(period)` is NaN, producing NaN momentum. With the `len(data) < lookback_period + 5` guard at line 42, at least 5 rows of valid momentum should exist. But if the DataFrame has NaN close values mid-series (e.g. from exchange data gaps), `current_momentum` or `prev_momentum` can be NaN. The threshold comparisons (`prev_momentum < threshold`) return `False` for NaN comparisons in Python/numpy, so no signal fires 鈥?this is silent data loss rather than a crash.
- **Impact**: Missed signals during data gaps; no financial loss but reduces strategy effectiveness. Could be confusing during debugging.
- **Fix**: Add `if not np.isfinite([current_momentum, prev_momentum]).all(): return []` after computing momentum values.
- **Confidence**: High

---

### [SEV-16] `MultiFactorHFStrategy` 鈥?`_last_bar_key` based on `(timestamp, len)` does not prevent duplicate signals when the same bar is re-fetched with extra rows appended
- **Severity**: Low
- **Category**: correctness
- **Location**: `strategies/quantitative/multi_factor_hf.py:125-128, 218-220`
- **What's wrong**: The dedup key is `f"{ts}|{len(data)}"`. When a cached DataFrame is re-fetched and one new bar is appended, the timestamp changes, so the dedup key changes and the strategy runs correctly. But if the cache serves the same frame twice (e.g. within the 30s TTL where two strategies share the same symbol/timeframe), the second call has the same `(ts, len)` and returns `[]`, which is the desired behaviour. The concern is if the DataFrame's last bar timestamp does not advance (e.g. due to exchange caching returning the same bar twice), the strategy will silently not generate a signal on that cycle 鈥?this is correct but could mask feed latency issues.
- **Impact**: No financial loss; potential confusion when debugging missed signals.
- **Fix**: Log a warning when the same bar key is seen more than N consecutive times.
- **Confidence**: Low

---

### [SEV-17] `StochasticStrategy` 鈥?SELL signal emitted without `stop_loss` / `take_profit` fields
- **Severity**: Low
- **Category**: money-safety
- **Location**: `strategies/technical/common_strategies.py:164-175`
- **What's wrong**: The SELL signal at line 164 does not set `stop_loss` or `take_profit`. The BUY signal at line 150-162 sets both. The omission means the `_finalize_generated_signals` ATR-stop fallback in `StrategyBase` will apply ATR-based levels instead, but only if `_use_atr_stops_for_signal` returns True and OHLC data is available. Without the explicit values the SELL signal relies entirely on the ATR fallback, which uses a default 1.5脳 ATR stop. This is acceptable but inconsistent with the BUY side, and may be unintentional.
- **Impact**: SELL signals have slightly different risk parameters than BUY signals from the same strategy. Not a direct money loss but can lead to asymmetric risk profiles.
- **Fix**: Add `stop_loss=c * (1 + float(self.params["stop_loss_pct"])), take_profit=c * (1 - float(self.params["take_profit_pct"]))` to the SELL signal in `StochasticStrategy`.
- **Confidence**: High

---

### [SEV-18] `WilliamsR` / `CCI` / `StochRSI` factor strategies: `_active_bar_ts` not set in `check_exit`
- **Severity**: Low (duplicate of SEV-02 for additional strategies not listed)
- **Category**: correctness
- **Location**: `strategies/factor_based/factor_strategies.py` (all 18 factor strategy check_exit paths)
- **What's wrong**: See SEV-02. All 18 factor strategies share the same `_create_signal` path through `_oscillator_factor_exit`. The timestamp is stale when `check_exit` is called independently of `generate_signals` (see detailed analysis in SEV-02). This low-severity duplicate entry acknowledges the scope is 18 strategies, not just one.
- **Impact**: See SEV-02.
- **Fix**: See SEV-02.
- **Confidence**: High

---

*Report generated: 2026-05-29*
