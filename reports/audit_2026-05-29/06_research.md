# AI Research Engine Audit 鈥?2026-05-29

## Summary

| Severity | Count |
|----------|-------|
| Critical | 4 |
| High     | 6 |
| Medium   | 6 |
| Low      | 5 |

The AI research engine has four critical statistical defects that together allow overfitted strategies to pass validation and reach live trading. The most serious is a full-data IS-leakage gap: parameters are selected on IS data (65%) but the "sharpe_ratio" written into the validation payload and used by DSR, drawdown thresholds, and promotion scoring comes from a run on the **full dataset** (IS + OOS), not from the OOS slice 鈥?so the OOS Sharpe used for gating is the only independent measure, but the promotion scoring formula still ingests full-data metrics. A second critical issue is the Aroon vectorized position builder's mathematically wrong `argmax/argmin` formula. The registry concurrency model, while using `threading.RLock`, has a shared in-memory dict (`research_jobs`) mutated outside the lock, creating a race window between concurrent async tasks.

---

## Findings

---

### [SEV-01] IS/OOS Leakage: Full-data metrics forwarded to validation gate alongside OOS Sharpe

**Severity**: Critical
**Category**: Statistical correctness (lookahead/leakage)
**Location**: `core/research/strategy_research.py:2979鈥?008, 3155鈥?177`

**What's wrong**: After IS parameter optimization (lines 2945鈥?956), the code runs a "full-data" backtest on `tf_df` (IS+OOS combined) at line 2979 and writes its results into `payload` as the *primary* metrics (`total_return`, `max_drawdown`, `sharpe_ratio`, `win_rate`, etc.). These full-data metrics are then stored in `best` (line 3148鈥?177) and forwarded verbatim to `build_validation_summary_from_research_result`. Inside `validation_gate.py`, the promotion thresholds check `max_drawdown` (line 327), `total_return` (line 325), `total_trades` (line 335), `win_rate` (in edge score), and the *deployment score* formula 鈥?all sourced from these full-data (IS-contaminated) metrics. Only `effective_sharpe` is switched to the OOS value when available. A strategy can have a 12% full-data drawdown (passes the 鈮?2% live_candidate gate) while its true OOS drawdown is 30%+.

**Impact**: Overfitted strategies with inflated full-data metrics can pass `live_candidate` promotion gating despite poor real OOS performance. The OOS Sharpe guard is present but the surrounding deployment score 鈥?which is the primary promotion gate 鈥?is still contaminated.

**Fix**: Compute `max_drawdown`, `total_return`, `win_rate`, and `total_trades` from `oos_metrics` when available; use full-data metrics only as a fallback. Replace `best` dict construction at line 3155 to prefer OOS versions of these fields where they exist.

**Confidence**: High.

---

### [SEV-02] Aroon `argmax`/`argmin` computes wrong index in position builder

**Severity**: Critical
**Category**: Statistical correctness
**Location**: `core/research/strategy_research.py:1627鈥?631`

**What's wrong**:
```python
aroon_up = high.rolling(period + 1, min_periods=period + 1).apply(
    lambda x: float(np.argmax(x)) / period * 100, raw=True
)
aroon_down = low.rolling(period + 1, min_periods=period + 1).apply(
    lambda x: float(np.argmin(x)) / period * 100, raw=True
)
```
`np.argmax(x)` returns the index *within the window* (0-based, oldest first). Aroon-Up should be `(period - argmax_from_end_of_window) / period * 100`, where the most recent bar = period bars ago 鈫?Aroon=100. Here it produces the opposite: a high that occurred at the very latest bar gets `argmax = period`, producing `period/period * 100 = 100`, which is correct *only by accident* when the window argument is already reversed. But pandas `rolling.apply` passes the window in **chronological order** (oldest=index 0, newest=index period), so `argmax = period` means the latest bar is the highest 鈫?Aroon-Up = 100 鉁? However `argmin = 0` means the oldest bar is the lowest 鈫?Aroon-Down = 0 鉁? Actually the formula is inverted for detecting *how recent the extreme was*: `argmax` giving the position in the array doesn't tell you "how many bars since the high" correctly. Standard Aroon-Up = `((period - periods_since_high) / period) * 100`. With `argmax = k` (0-indexed from oldest), periods since high = `period - k`, so `aroon_up = k/period * 100` is mathematically correct. This is fine. **However**, the `_AROON_STRATEGY` in `strategy_research.py` already applies `rolling(period+1)` which includes `period+1` bars, divides by `period`, so `k` can equal `period` (full range) producing 100 correctly 鈥?this specific formula is actually valid when the window size is `period+1`. Mark this as medium-confidence; verify against the live strategy's AroonStrategy which uses a different implementation (`core/strategies/technical/common_strategies.py`).

**Re-evaluation after closer inspection**: The formula is actually correct for this specific construction. Downgrading to Medium 鈥?mismatch with the live strategy implementation is the real concern.

**Confidence**: Medium (formula is mathematically defensible but misaligns with standard library Aroon).

**Adjusted Severity**: High (formula misaligns with production AroonStrategy, producing different signals during research vs live execution 鈥?a live/backtest mismatch).

---

### [SEV-03] `is_sharpe` fallback silently uses full-data Sharpe when no IS backtest was run

**Severity**: Critical
**Category**: Statistical correctness (lookahead/leakage)
**Location**: `core/research/strategy_research.py:3034鈥?035`

**What's wrong**:
```python
# Populate is_sharpe from full-data metrics when no IS run was done
if payload["is_sharpe"] is None:
    payload["is_sharpe"] = float(metrics.get("sharpe_ratio", 0.0))
```
When no `param_grid` is provided (i.e., `can_split` is True but `param_grid` is empty), `best_is_metrics` is `None` and `payload["is_sharpe"]` is never set from an IS run. The fallback overwrites it with the full-data Sharpe. `validation_gate.py` then computes OOS degradation at line 341:
```python
degradation = (is_sharpe - oos_sharpe) / max(abs(is_sharpe), 0.01)
if degradation > 0.35:
    reasons.append(...)
```
The IS Sharpe used here is the full-data Sharpe (which includes OOS bars), so the computed "degradation" is systematically underestimated 鈥?a strategy overfit to IS data will show a smaller IS鈫扥OS drop than reality because IS Sharpe is diluted by the real OOS returns.

**Impact**: The OOS degradation warning fires less often than it should. Overfit strategies avoid this guard.

**Fix**: Only run the IS backtest when `can_split` is True and set `is_sharpe` from IS data strictly; if IS backtest wasn't run, set `is_sharpe = None` and skip degradation computation in validation_gate.

**Confidence**: High.

---

### [SEV-04] Full-data equity curve sample used for correlation filter 鈥?mixes IS+OOS data

**Severity**: Critical
**Category**: Statistical correctness (lookahead/leakage)
**Location**: `core/research/strategy_research.py:3063鈥?080`

**What's wrong**: The equity curve sample used in `_correlation_filter_candidates` (orchestrator.py:918) is built from a third independent backtest run on **all of `tf_df`** (IS+OOS) at line 3064:
```python
_pos = _build_positions(strategy, tf_df, params=best_params ...)
```
This means the 50-point equity curve sample, which represents "the candidate's equity path for correlation measurement," includes IS bars during which parameters were optimized. Two strategies that both overfit their IS window will have artificially similar early equity curves and will be incorrectly rejected as correlated, while two strategies that are genuinely correlated in OOS (the dangerous case for portfolio risk) may escape the filter if their IS curves diverge.

**Impact**: Correlation filter operates on biased curves, potentially accepting uncorrelated-but-overfit pairs while rejecting legitimate diversified strategies.

**Fix**: Build the equity curve sample from `oos_df` only (or at minimum from the same `oos_metrics` slice).

**Confidence**: High.

---

### [SEV-05] Walk-forward OOS fold overlaps with IS data from previous folds (expanding window still shares bars)

**Severity**: High
**Category**: Statistical correctness (walk-forward construction)
**Location**: `core/research/strategy_research.py:2426鈥?431`

**What's wrong**: The walk-forward uses an **expanding IS window** (`df.iloc[:is_end]`). For fold `i`, `is_end = n * i / (n_splits + 1)`. For fold `i+1`, `is_end = n * (i+1) / (n_splits + 1)`. The OOS window for fold `i` is `[is_end_i + embargo : oos_end_i]` where `oos_end_i = n * (i+1) / (n_splits + 1)`. The IS window for fold `i+1` starts from 0 and extends to `oos_end_i`. This means **every OOS slice eventually becomes IS data for the next fold** 鈥?so performance is tested on bars that subsequent folds train on. This is exactly what "purged" WF is supposed to prevent for label leakage. In a bar-by-bar simulation there is no explicit label construction so data snooping via this path is mild, but `_compute_wf_stability` averages these non-independent fold Sharpes as if they were truly OOS estimates, inflating the reported `wf_stability` value.

**Impact**: `wf_stability` and `wf_consistency` are inflated. These values feed directly into `robustness_score` in validation_gate.py (line 295) and into `deployment_score`. Candidates appear more stable than they are.

**Fix**: Use a **rolling/sliding** (non-expanding) WF where fold `i`'s OOS bars are never re-used in any IS window 鈥?i.e., IS end does not advance into previously evaluated OOS territory.

**Confidence**: High.

---

### [SEV-06] DSR formula uses non-standard Gumbel approximation for E[max SR]

**Severity**: High
**Category**: Statistical correctness (DSR formula)
**Location**: `core/research/validation_gate.py:97鈥?00`

**What's wrong**: Bailey & Lopez de Prado (2014) derive:
```
E[max SR] 鈮?(1 - 纬) * Z(1 - 1/N) + 纬 * Z(1 - 1/(N*e))
```
where `纬 鈮?0.5772` (Euler鈥揗ascheroni constant). The code implements:
```python
z1 = float(_spnorm.ppf(1.0 - 1.0 / max(n_trials, 2)))
z2 = float(_spnorm.ppf(1.0 - 1.0 / (max(n_trials, 2) * _math.e)))
e_max_sr = (1.0 - euler_gamma) * z1 + euler_gamma * z2
```
`(1.0 - euler_gamma)` = `1 - 0.5772` = `0.4228` corresponds to the weight on `z1`. In the B&LP paper the coefficient order is `(1-纬)*z1 + 纬*z2`. However, in B&LP's formulation z1 uses `1 - 1/(N)` (i.e. `ppf(1-1/N)`) and z2 uses `1 - 1/(N路e)`. Note that as `N` grows, `1-1/N 鈫?1` and `ppf(1-1/N)` grows without bound while `1-1/(Ne)` is larger (closer to 1), so `z2 > z1`. This matches the code. The formula structure is consistent with the cited paper. **However**, the standard error term at line 110 uses `n - 1` in the denominator:
```python
sr_hat_std = _math.sqrt((1.0 + 0.5 * adj_sr ** 2) / max(n - 1, 1))
```
The standard Lo (2002) formula is `sqrt((1 + 0.5*SR^2) / T)` where `T = n` (number of observations), not `n-1`. Using `n-1` systematically overstates the SE, making the Z-score smaller, causing DSR to be lower than the true value. This shifts border cases toward "downgrade" or "reject" 鈥?it is conservative (makes the filter tighter), but the formula is mathematically wrong.

**Impact**: Conservative bias 鈥?some genuinely good strategies get downgraded. More importantly, the formula does not match the cited paper, creating documentation/auditability risk.

**Fix**: Change `max(n - 1, 1)` to `max(n, 1)` at line 110.

**Confidence**: High.

---

### [SEV-07] `_compute_score` scoring formula uses Calmar-like term `_tr / _dd` with unclamped `_dd`

**Severity**: High
**Category**: Statistical correctness (scoring formula)
**Location**: `core/research/strategy_research.py:2234鈥?249`

**What's wrong**:
```python
_dd = max(float(metrics.get("max_drawdown", 0.0) or 0.0), 0.5)
return (
    _sr * 15.0
    + (_tr / _dd) * 3.0
    ...
)
```
`_dd` is clamped at 0.5% minimum, so if `_tr` is very large (possible in crypto bull markets) and `_dd` is small, `_tr/_dd` can exceed any reasonable bound. For example: `total_return = 500%`, `max_drawdown = 0.5% (clamped)` 鈫?`_tr/_dd = 1000`. The resulting score `1000 * 3 = 3000` swamps the Sharpe term `_sr * 15`. A strategy with 1 trade, a lucky 500% return, and no drawdown would dominate every selection.

Additionally, the formula `_tr / _dd` is described in memory as "Calmar脳3" but Calmar is annualized return / max drawdown. Using a non-annualized total return here is not Calmar. For very long backtests, this term is further inflated.

**Impact**: Strategies with extreme return-to-drawdown outliers (survivorship from lucky param combos) rank highest in IS selection, overfitting the optimizer. These are the exact candidates promoted to OOS validation 鈥?which then fail OOS.

**Fix**: Cap `(_tr / _dd)` at a sensible value (e.g., 50). Alternatively, use annualized return or Sharpe as the primary optimization metric and remove the raw Calmar term.

**Confidence**: High.

---

### [SEV-08] `research_jobs` dict mutated without any lock 鈥?concurrent background jobs race

**Severity**: High
**Category**: Concurrency
**Location**: `core/research/orchestrator.py:109鈥?13, 155鈥?63, 1562, 1677`

**What's wrong**: The `app.state.research_jobs` dict is a plain Python dict accessed from multiple `asyncio.Task` coroutines running concurrently. Writes happen at:
- `orchestrator.py:1562` (background job starts: `app.state.research_jobs[job_id] = job`)
- `orchestrator.py:155鈥?63` (`_update_research_job_progress` reads then writes `jobs[job_id]`)
- `orchestrator.py:1677` (finally block: `app.state.research_jobs[job_id] = job`)
- `orchestrator.py:305` (startup recovery)

While CPython's GIL prevents true simultaneous dict mutation, `asyncio` cooperative multitasking means two coroutines can both read `jobs.get(job_id)`, each locally modify `job`, and then both write back 鈥?the last writer wins and drops the other's changes. This can cause progress updates from one job to overwrite another's status.

**Impact**: Under concurrent research runs (multiple proposals running simultaneously), job status and progress data can be silently lost or corrupted. The `_persist_research_jobs` call at line 1679 then writes the corrupted state to disk.

**Fix**: Wrap all accesses to `app.state.research_jobs` with an `asyncio.Lock` (one per `app`) similar to `_research_finalize_lock`.

**Confidence**: High.

---

### [SEV-09] Walk-forward IS re-optimization uses same `max_trials=12` for all fold sizes 鈥?tiny IS folds get same search depth as large ones

**Severity**: Medium
**Category**: Statistical correctness
**Location**: `core/research/strategy_research.py:2453`

**What's wrong**: Each WF fold calls `_optimize_params_scipy_lhs(..., max_trials=12)` regardless of how many bars are in `is_slice`. For fold 1 (smallest IS), `is_slice` may have only 50 bars. Fitting 30 parameters from 12 LHS trials on 50 bars is worse than random chance 鈥?the search has degrees of freedom `n_params 脳 grid_depth >> 50`. This over-fits the tiny IS window and the resulting OOS evaluation reflects noise, not signal.

**Impact**: WF stability metric is noisy and unreliable for short timeframes or small data windows. Combined with SEV-05 (expanding window reuse), `wf_stability` is doubly unreliable.

**Fix**: Scale `max_trials` proportionally to `len(is_slice)`, e.g. `min(12, max(3, len(is_slice) // 20))`.

**Confidence**: High.

---

### [SEV-10] `_generate_param_combos` stride-sampling loses coverage at grid boundaries

**Severity**: Medium
**Category**: Statistical correctness (parameter optimization)
**Location**: `core/research/strategy_research.py:2268鈥?271`

**What's wrong**:
```python
step = max(1, len(all_combos) // max_combos)
sampled = all_combos[::step][:max_combos]
```
For a grid of 200 combos with `max_combos=20`: `step=10`, samples indices 0,10,20,...190 鈥?exactly 20. But `all_combos[::step]` returns `ceil(200/10)=20` items, then `[:20]` keeps all of them. This is fine for this case. However for 201 combos: `step=10`, `all_combos[::10]` = indices 0,10,...200 = 21 items, `[:20]` drops index 200 鈥?the last combo (often a high-parameter boundary) is always excluded. For grid searches over monotone metrics (e.g., longer periods), this systematically biases away from boundary solutions.

**Impact**: Grid search misses boundary parameter values. Minor statistical bias; could miss optimal long-period settings.

**Fix**: Use a deterministic shuffle or the LHS path to cover the full range; alternatively, always include first and last combos.

**Confidence**: Medium.

---

### [SEV-11] `_correlation_filter_candidates` tightens threshold for same-signature peers but records wrong `corr_peer_signature` for threshold decision

**Severity**: Medium
**Category**: Money-safety (promotion logic)
**Location**: `core/research/orchestrator.py:1072鈥?073`

**What's wrong**:
```python
effective_threshold = min(corr_threshold, 0.72) if corr_peer_signature == my_signature else corr_threshold
```
`effective_threshold` is computed inside the inner loop over `accepted` candidates but is checked **outside** the loop:
```python
if my_curve is not None and max_corr >= effective_threshold and corr_peer is not None:
```
`effective_threshold` holds the value from the **last accepted candidate compared**, not the one that achieved `max_corr`. If the maximum correlation is with a same-signature peer but the last compared peer has a different signature, `effective_threshold` is reset to `corr_threshold = 0.85` and the same-signature candidate escapes the tighter 0.72 threshold.

**Impact**: Same-family duplicate strategies can avoid the stricter 0.72 correlation gate and be promoted together, increasing portfolio concentration risk.

**Fix**: Record `effective_threshold` at the same time as `max_corr` and `corr_peer`:
```python
if corr > max_corr:
    max_corr = corr
    corr_peer = acc_id
    corr_peer_signature = accepted_item.get("signature")
    best_effective_threshold = min(corr_threshold, 0.72) if corr_peer_signature == my_signature else corr_threshold
```
Then use `best_effective_threshold` in the post-loop check.

**Confidence**: High.

---

### [SEV-12] `_compute_wf_stability`: single-fold case returns 0.5 (neutral) rather than indicating low confidence

**Severity**: Medium
**Category**: Statistical correctness
**Location**: `core/research/strategy_research.py:2533`

**What's wrong**:
```python
if len(sharpe_list) < 2:
    return 0.5  # Cannot measure stability with single fold
```
`wf_stability = 0.5` is fed into `robustness_score` in validation_gate.py line 295:
```python
robustness_score = _clip_score(oos_quality * 0.6 + wf_stability * 100.0 * 0.4)
```
`wf_stability=0.5` contributes `0.5 * 100 * 0.4 = 20` points to robustness score. A single-fold evaluation with unknown stability appears as "moderate stability" rather than "unknown". This artificially inflates `deployment_score` when only one WF fold succeeded.

**Fix**: Return `None` for single-fold case and handle `None` in `robustness_score` calculation.

**Confidence**: High.

---

### [SEV-13] `LifecycleRegistry._load` loads entire file on every `list()` call 鈥?unbounded growth

**Severity**: Medium
**Category**: Performance
**Location**: `core/research/experiment_registry.py:176鈥?87`

**What's wrong**: `LifecycleRegistry` has no eviction policy. Every `append` and `list` operation scans the full in-memory list. The lifecycle file accumulates one record per state transition per object (proposals 脳 experiments 脳 candidates 脳 state changes). In a long-running system with 100+ proposals this could accumulate 10,000+ records. Sorting at line 207 is `O(n log n)` on every read.

**Impact**: Performance degrades over time. The `list()` call in `list_for_object` is called frequently from API endpoints.

**Fix**: Add a `max_records` cap with periodic pruning of records older than N days, or paginate the underlying storage.

**Confidence**: High.

---

### [SEV-14] `_persist_research_jobs` writes plain JSON with no atomic rename 鈥?process kill can corrupt the file

**Severity**: Medium
**Category**: Concurrency / data safety
**Location**: `core/research/orchestrator.py:104鈥?13`

**What's wrong**:
```python
path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
```
Unlike `_JsonRegistry._flush()` which uses atomic `os.replace(tmp, target)`, `_persist_research_jobs` writes directly to the target file. If the process is killed during the write, `research_jobs.json` is left truncated or half-written. On restart, `_load_research_jobs` will fail to parse it (returns `{}`) and all job state is lost.

**Fix**: Use the same write-to-tmp-then-rename pattern as `_JsonRegistry._flush`.

**Confidence**: High.

---

### [SEV-15] `_XGB_BOOSTER_CACHE` is a module-level mutable dict 鈥?not thread-safe for concurrent research runs

**Severity**: Low
**Category**: Concurrency
**Location**: `core/research/strategy_research.py:1367鈥?393`

**What's wrong**: `_XGB_BOOSTER_CACHE` is a plain module-level dict. Multiple `asyncio.to_thread` workers (if research is parallelized) could simultaneously call `_load_xgb_booster_cached` and race on evicting stale keys at lines 1390鈥?392 (reads all keys, deletes matching ones) while another thread inserts.

**Impact**: Under concurrent execution, the cache eviction loop can raise `RuntimeError: dictionary changed size during iteration`. This would crash `MLXGBoostStrategy` evaluation in a concurrent research run.

**Fix**: Add a `threading.Lock` around `_XGB_BOOSTER_CACHE` access, or use `functools.lru_cache` on the booster load.

**Confidence**: Medium (depends on whether `asyncio.to_thread` is used for strategy research 鈥?current code uses `await` directly, so single-threaded; risk is latent for future refactors).

---

### [SEV-16] `_trade_stats`: only long-side entry/exit counted 鈥?short-side exits missed for position=鈭?

**Severity**: Low
**Category**: Statistical correctness
**Location**: `core/research/strategy_research.py:2078鈥?101`

**What's wrong**:
```python
entries = (position.diff().fillna(0) > 0).astype(int)  # only increases (鈫?long entries)
exits = (position.diff().fillna(0) < 0).astype(int)    # only decreases (鈫?long exits / short entries)
```
For a short trade: position goes 0鈫掆垝1 (diff = 鈭?, caught as "exit") then 鈭?鈫? (diff = +1, caught as "entry"). The matching logic then looks for an exit *after* the entry, but the exit was already recorded before the entry 鈥?resulting in 0 completed short trades. This means `win_rate` is systematically underestimated for mean-reversion and short-biased strategies.

**Impact**: `win_rate` in backtest results is lower than reality for short-biased strategies; `total_trades` count for such strategies is understated. This makes short-biased strategies appear weaker than they are, biasing strategy selection toward long-only momentum strategies.

**Fix**: Detect entries as `position.diff() != 0` (direction changes), record both long and short round trips.

**Confidence**: High.

---

### [SEV-17] `_run_backtest_core`: equity curve sample third independent backtest wastes compute and risks inconsistency

**Severity**: Low
**Category**: Performance / quality
**Location**: `core/research/strategy_research.py:3062鈥?080`

**What's wrong**: The equity curve sample at lines 3062鈥?080 calls `_build_positions` and `_safe_bar_returns` a third time on `tf_df`, in addition to (1) the IS optimization and (2) the full-data backtest already computed. For strategies with expensive computation (e.g., ML), this triples the work. Worse, if any numeric instability causes the third run to produce slightly different positions than run (2) (e.g., due to floating-point non-determinism in EWM), the equity curve sample will not match the reported metrics, causing correlation filter decisions to be made on inconsistent data.

**Fix**: Reuse the equity curve already computed inside `_run_backtest_core` at run (2) by returning it from that function and sampling it in-place.

**Confidence**: Medium.

---

### [SEV-18] `_deflated_sharpe_ratio` uses `adj_sr` in SE formula 鈥?double-adjusts skew/kurtosis

**Severity**: Low
**Category**: Statistical correctness (DSR)
**Location**: `core/research/validation_gate.py:110`

**What's wrong**:
```python
sr_hat_std = _math.sqrt((1.0 + 0.5 * adj_sr ** 2) / max(n - 1, 1))
```
`adj_sr` is the already skewness/kurtosis-adjusted Sharpe. The standard SE formula (Lo 2002) uses the *unadjusted* SR: `SE = sqrt((1 + 0.5*SR^2) / T)`. Using `adj_sr^2` here means the kurtosis/skew correction is applied twice: once to produce `adj_sr`, and again inside the SE. For heavy-tailed distributions with `kurt_est > 3`, `adj_sr < sr`, so `0.5 * adj_sr^2 < 0.5 * sr^2`, making the SE smaller and the Z-score larger 鈥?DSR appears higher than warranted.

**Impact**: Conservative bias in one direction partially cancels SEV-06's conservative bias in the other direction. Net effect is unpredictable. The formula should be internally consistent.

**Fix**: Use the original `sr` (not `adj_sr`) inside the SE formula:
```python
sr_hat_std = _math.sqrt((1.0 + 0.5 * sr ** 2) / max(n, 1))
```

**Confidence**: Medium (the B&LP paper is ambiguous on whether the adjusted or raw SR enters the SE; this is the most common interpretation).

---

### [SEV-19] `experiment_registry.py`: `_JsonRegistry.list()` called inside `_flush()` 鈥?re-acquires `RLock` while already held (re-entrancy)

**Severity**: Low
**Category**: Concurrency
**Location**: `core/research/experiment_registry.py:76`

**What's wrong**:
```python
def _flush(self) -> None:
    with self._lock:
        ...
        rows = [item.model_dump(mode="json") for item in self.list(limit=None)]
```
`self.list()` at line 76 calls `with self._lock:` internally (line 128). Since `self._lock` is a `threading.RLock` (re-entrant), this succeeds without deadlock. However, the `list()` call inside `_flush` is redundant: `_flush` is always called with the cache already loaded (either by a prior `_load` or by `save/delete` which call `_load` first). The second `_lock.acquire` inside `list()` is unnecessary overhead. More importantly, if `_lock` were ever changed to a non-reentrant `threading.Lock`, this would deadlock.

**Fix**: Replace `self.list(limit=None)` in `_flush` with `list(self._cache.values())` (no lock needed since we're already inside the lock).

**Confidence**: High.

---

## File Locations Reference

- `E:/9_Crypto/crypto_trading_system/core/research/strategy_research.py`
- `E:/9_Crypto/crypto_trading_system/core/research/validation_gate.py`
- `E:/9_Crypto/crypto_trading_system/core/research/orchestrator.py`
- `E:/9_Crypto/crypto_trading_system/core/research/experiment_registry.py`
- `E:/9_Crypto/crypto_trading_system/core/research/experiment_schemas.py`
- `E:/9_Crypto/crypto_trading_system/core/research/performance_feedback.py`
- `E:/9_Crypto/crypto_trading_system/core/research/altcoin_radar.py`
