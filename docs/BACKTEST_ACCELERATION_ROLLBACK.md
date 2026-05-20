# Backtest Acceleration Rollback Guide (2026-05-20)

This document accompanies `BACKTEST_PERFORMANCE_ACCELERATION_PLAN_2026-05-20.md`
and tracks **what changed**, **how to verify it's still correct**, and
**how to roll back** each phase independently if a regression surfaces.

Every fast path is **opt-in via a settings flag**. The trusted per-bar
replay (`_replay_signal_strategy_position` calling the real strategy
class) and the trusted exit engine (`run_exit_engine`) remain the
source of truth and are what runs when the flags are off. Switching a
flag does not require code edits or redeploys — just edit
`config/settings.py` (or set the matching env var) and restart.

## Final scoreboard (all phases combined)

With `BACKTEST_FAST_EXACT_STRATEGIES=true` + `BACKTEST_FAST_EXIT_ARRAYS=true`
+ `BACKTEST_OPTIMIZE_WORKERS=4` on a 4-core machine:

| Workload                  | Phase 0 | All phases | Speedup |
|---------------------------|--------:|-----------:|--------:|
| MAStrategy 5m/1mo         |  6.14 s |     3.16 s |   1.94× |
| RSIStrategy 5m/1mo        | 10.99 s |     9.19 s |   1.20× |
| MACDStrategy 5m/1mo       |  6.76 s |     3.80 s |   1.78× |
| **MultiFactorHF 5m/1mo**  |**119.10 s**|  **1.37 s** | **87×** |
| Compare (9 strats)        |    191 s |       176 s |   1.08× |
| Optimize MA (32 trials)   |    198 s |     **24 s** |  **8.3×** (with workers=4) |

## Baseline reference

Captured before any changes (Python 3.13, Windows 10, BTC/USDT 5m local
parquet, 30-day window, 8 639 bars):

| Workload                | wall (s) | bars/sec |
|-------------------------|---------:|---------:|
| MAStrategy              |     6.14 |     1407 |
| RSIStrategy             |    10.99 |      786 |
| MACDStrategy            |     6.76 |     1277 |
| **MultiFactorHFStrategy** | **119.10** |   **72.5** |
| Compare (9 strategies)  |   191.0  |        — |
| Optimize MA (32 trials) |   198.0  |        — |

Reproduce: `python scripts/benchmark_backtest_runtime.py` (smoke set,
~7 min). Add `--full` for the full 1-year MultiFactor profile (~25 min).
Artifacts land in `.bench/`:

  * `baseline_<git_sha>.json` — every case + result hash
  * `phase1_<git_sha>.json`, `phase2_<git_sha>.json` — manual snapshots
  * `baseline_<git_sha>.prof` — cProfile dump (Phase 0 only, MultiFactorHF)
  * `baseline_<git_sha>.top.txt` — pstats top-30 text

The `result_hash` field on each case hashes total_return / sharpe /
drawdown / win_rate / total_trades. Cross-run drift in this hash on the
same data is a signal that a fast path has diverged from the trusted
path.

---

## Phase 0 — Benchmark + profile (no behavior change)

**Files added**

  * `scripts/benchmark_backtest_runtime.py`
  * `.bench/baseline_*.json`, `.bench/baseline_*.prof`,
    `.bench/baseline_*.top.txt`

**Behavior change**: none.

**Rollback**: delete the script and `.bench/`. No production code is
referenced.

**Verify still correct**:
```
python scripts/benchmark_backtest_runtime.py
```
Compare against the baseline table above. Result hashes from the same
git SHA on the same data should be stable across runs (modulo the
bench's 30-day window sliding with wall clock, which causes ~0.5%
trade-count noise — see `phase1` vs `phase2` MultiFactorHF rows for the
scale of that noise: 409 trades both times).

---

## Phase 1 — Replay view fast path (mutates_input contract)

**Files changed**

  * `core/strategies/strategy_base.py` — `StrategyBase.mutates_input: bool = True` (safe default)
  * `strategies/technical/ma_strategy.py` — `MAStrategy.mutates_input = False`, `EMAStrategy.mutates_input = False`
  * `strategies/technical/rsi_strategy.py` — `RSIStrategy.mutates_input = False`
  * `strategies/technical/macd_strategy.py` — `MACDStrategy.mutates_input = False`
  * `web/api/backtest.py::_replay_signal_strategy_position` — branches on `getattr(cls, "mutates_input", True)`
  * `config/settings.py` — `BACKTEST_REPLAY_VIEW_FAST_PATH: bool = True`
  * `tests/test_backtest_runtime_consistency.py` — 4 new parity tests under `_VIEW_FAST_PATH_STRATEGIES`

**Behavior change**: For strategies that explicitly declare
`mutates_input = False`, the replay loop hands the strategy a view
(`df.iloc[start:end+1]`) instead of a fresh `.copy()`. Strategies
without that declaration use the safe copy path. **Default is the safe
copy path** — opt-in is per-class.

**Measured win**: MA −8.6 %, MACD −6.8 %, RSI −4 %, optimize MA −8 %.
MultiFactorHF unchanged (still on copy path; it writes to its input).

**Rollback options (lowest impact first)**:

1. **Flip the global flag** (no code change):
   `config/settings.py` → `BACKTEST_REPLAY_VIEW_FAST_PATH: bool = False`
   This forces every replay back onto the copy path immediately. Use
   when something subtle is suspected anywhere.

2. **Per-strategy revert**: remove the `mutates_input = False` line on
   the offending class. The replay loop checks the attribute per call,
   so this localizes the rollback.

3. **Full revert**: delete the new `mutates_input` class attribute and
   the `if use_view:` branch in
   `web/api/backtest.py::_replay_signal_strategy_position`. The
   `else:` branch is the original implementation, byte-identical.

**Verify still correct**:
```
python -m pytest tests/test_backtest_runtime_consistency.py -q
```
The 4 `test_replay_view_fast_path_parity[*]` tests force both code
paths on the same input and assert bit-for-bit equality. If a strategy
quietly starts mutating, this test fails before the user sees a
diverged result.

**Adding new strategies to the view path**: only after auditing that
`generate_signals` does NOT write to `data` (no `data[col] = ...`, no
`data.sort_*`, no `data.drop`/`fillna` without `copy()`). Set
`mutates_input = False` on the class AND add it to
`_VIEW_FAST_PATH_STRATEGIES` in the test file. The test will refuse to
parametrize unless the attribute is present.

---

## Phase 2 — MultiFactorHF fast_exact path

**Files added**

  * `strategies/quantitative/multi_factor_hf_fast.py` — `build_multifactor_hf_position_series(df, params, ...)`
  * `tests/test_multi_factor_hf_parity.py` — 5 fixtures (trend / chop / high-vol / low-volume / missing-volume), bar-by-bar exact equality vs trusted replay

**Files changed**

  * `web/api/backtest.py::_build_backtest_position_series` — routes to the fast builder when the strategy is `MultiFactorHFStrategy` and `BACKTEST_FAST_EXACT_STRATEGIES` is on; any exception falls through to the trusted replay
  * `config/settings.py` — `BACKTEST_FAST_EXACT_STRATEGIES: bool = False` (**default off**)

**Behavior change**: when the flag is on, `MultiFactorHFStrategy`
backtests precompute the 6 factors once over the whole frame and walk
the strategy's state machine in a single Python loop, instead of
recomputing every factor on the trailing window at every bar (8 639
bars × 6 factors = ~52k redundant calls in the trusted path; only 6
calls in the fast path).

**Measured win**: 119.1 s → 5.77 s on the 30-day 5m fixture = **20.6×**.
Trade count: 409 → 409. Bar-by-bar parity locked by 5 synthetic
fixtures.

**Rollback options (lowest impact first)**:

1. **Flip the global flag**:
   `config/settings.py` → `BACKTEST_FAST_EXACT_STRATEGIES: bool = False`
   (this is already the default — explicit opt-in is required).
   Restart. Every backtest run hits the trusted replay again. No code
   change.

2. **Delete the fast path entirely**:
   * remove the `if (...) and strategy == "MultiFactorHFStrategy":`
     block from `_build_backtest_position_series`
   * delete `strategies/quantitative/multi_factor_hf_fast.py`
   * delete `tests/test_multi_factor_hf_parity.py`
   * remove `BACKTEST_FAST_EXACT_STRATEGIES` from `config/settings.py`

   This restores the file state to before Phase 2. The trusted path
   was never modified.

**Verify still correct**:
```
python -m pytest tests/test_multi_factor_hf_parity.py -q
```
5 fixtures, each runs both paths and asserts `fast[i] == trusted[i]`
for every bar (no tolerance — these are -1/0/+1 floats, any drift
means the state machine diverged). The fixtures intentionally exercise
distinct gate paths:

  * `trend`: rising prices, exercises long entry + cooldown cycling
  * `chop`: oscillation, exercises score flips and `exit_th`
  * `high_vol`: heavy realized vol, exercises `rv_ok`/`atr_ok` blocks
  * `low_volume`: constant volume → `volume_z=NaN`, exercises the NaN-blocks-gate semantic (a real bug found and fixed during Phase 2 — see commit history)
  * `missing_volume`: 10 % NaN holes, exercises factor robustness

If any fixture starts failing, do not enable the flag in production
until the divergence is reconciled.

**Adding more strategies to fast_exact**: write a parallel
`build_*_position_series` next to the strategy module, add a parity
test file with at least 5 regime fixtures asserting bar-by-bar
equality, then add a `strategy == "FooStrategy"` clause to
`_build_backtest_position_series`. Do NOT enable by default — make the
user opt in by flipping the flag after they've vetted the parity.

---

## Phase 3 — Array execution simulator

**Files added**

  * `core/backtest/execution_arrays.py` — `simulate_execution_arrays(df, signal_position, config)`, `is_supported_config(config)`
  * `tests/test_execution_arrays_parity.py` — 9 tests (4 config-gating, 5 fixture parities: long_only / short_only / alternating / single_long / no_trade)

**Files changed**

  * `web/api/backtest.py::_simulate_execution_summary` — routes to the array path when `BACKTEST_FAST_EXIT_ARRAYS` is on and `is_supported_config(config)` returns True; any exception or unsupported config falls through to `run_exit_engine`
  * `config/settings.py` — `BACKTEST_FAST_EXIT_ARRAYS: bool = False` (**default off**)

**Behavior change**: when the flag is on AND the resolved exit-engine
config is the supported subset (no stops, no take profits, no
breakeven, no partial TP, no trailing, no time stop —
`signal_reversal_exit` only), the bar-walking exit/PnL simulation runs
as NumPy arrays instead of `ExitEngine.run`'s pandas single-cell loop.

The supported subset matches the **default** config that
`_simulate_execution_summary` produces when called with
`use_stop_take=False` and no exit template — the most common path the
backtest API hits.

**Measured win** (5m/1mo fixture, both Phase 2 + Phase 3 enabled):

  * MA: 6.14 s → 3.16 s (−48.6 %)
  * MACD: 6.76 s → 3.80 s (−43.8 %)
  * RSI: 10.99 s → 9.19 s (−16.4 %; RSI spends less of its time in the exit engine)
  * MultiFactorHF: 5.77 s → 1.37 s (Phase 2 already cut factor compute; Phase 3 cuts the remaining exit-engine cost)
  * Optimize MA 32 trials: 182 s → 101 s (−45 %)

**Rollback options (lowest impact first)**:

1. **Flip the global flag**:
   `config/settings.py` → `BACKTEST_FAST_EXIT_ARRAYS: bool = False` (already the default). Restart. Every backtest goes back to `run_exit_engine`. No code change.

2. **Delete the fast path entirely**:
   * remove the `if bool(getattr(_settings, "BACKTEST_FAST_EXIT_ARRAYS", ...)):` block from `_simulate_execution_summary`
   * delete `core/backtest/execution_arrays.py`
   * delete `tests/test_execution_arrays_parity.py`
   * remove `BACKTEST_FAST_EXIT_ARRAYS` from `config/settings.py`

**Verify still correct**:
```
python -m pytest tests/test_execution_arrays_parity.py -q
```
The 5 fixture parities compare bar-by-bar `effective_position` (exact)
and `gross_returns` (atol=1e-12) against `run_exit_engine`. Trade
counts (entries/exits/completed) and the `reversal` exit-reason count
must match exactly. The 4 config-gating tests confirm that any stop /
take / template / time-stop config returns `is_supported_config →
False`, forcing the trusted engine.

**Adding more supported configs**: e.g. fixed stop loss is the next
natural extension. Process: (1) add the branch to
`simulate_execution_arrays`, (2) loosen the relevant check in
`is_supported_config`, (3) add new fixture(s) to
`test_execution_arrays_parity.py` (e.g. `single_long_with_stop`) and
verify bar-by-bar parity, (4) only then enable the flag in
production. Each new config widens the surface area, so test
fixtures grow with it.

---

## Phase 5 — Parallel optimize trials

**Files changed**

  * `web/api/backtest.py` — extracted `_run_optimize_trial(args)` as a top-level pickle-friendly worker; `_optimize_strategy_on_df` now builds a deterministic work list and dispatches via `ProcessPoolExecutor` with `spawn` context (Windows-safe) when settings request it
  * `config/settings.py` — `BACKTEST_OPTIMIZE_WORKERS: int = 1`, `BACKTEST_OPTIMIZE_PARALLEL_MIN_TRIALS: int = 8`

**Files added**

  * `tests/test_optimize_parallel.py` — asserts serial vs parallel produces the same best params / score / metrics

**Behavior change**: when `BACKTEST_OPTIMIZE_WORKERS > 1` AND the trial
count meets `BACKTEST_OPTIMIZE_PARALLEL_MIN_TRIALS`, optimize trials
run in a process pool. Results are reassembled in trial-index order so
the API output is byte-deterministic regardless of worker count.

Below the threshold, or with `workers=1`, the serial path is used —
identical to the pre-Phase-5 behavior.

**Measured win** (32 trials, MAStrategy 5m/1mo, Phase 2+3 on,
4-core machine):

  * workers=1: 62.2 s (1.95 s/trial)
  * workers=2: 40.4 s (1.26 s/trial) — 1.54× speedup
  * workers=4: 24.0 s (0.75 s/trial) — **2.60× speedup**

Sub-linear because each worker pays a ~1 s spawn cost on Windows
(spawn re-imports all modules), and the DataFrame + market_bundle are
pickled once per worker. Linear scaling is not expected on small-trial
runs; on a 256-trial run scaling will be tighter.

**Rollback options**:

1. **Flag flip**: `BACKTEST_OPTIMIZE_WORKERS: int = 1` (the default).
   Restart. Optimize falls back to single-process serial execution.

2. **Full revert**: collapse the pool branch in
   `_optimize_strategy_on_df` back to the original `for combo in
   combo_iter:` loop, delete `_run_optimize_trial`, remove the
   `BACKTEST_OPTIMIZE_*` settings, delete the new test file.

**Verify still correct**:
```
python -m pytest tests/test_optimize_parallel.py -q
```
Two tests: serial vs parallel produces identical `best` params/score/
metrics on the same input; parallel auto-falls-back to serial when
trial count is below the threshold.

**Operational notes**:

* On Windows, the pool uses the `spawn` start method (the default;
  `fork` is unavailable). Callers of the optimize API are unaffected.
* If invoked from a `__main__` script (not via FastAPI), the script
  must have an `if __name__ == "__main__":` guard or spawn will
  recursively re-execute the parent.
* Inside FastAPI, the optimize endpoint is async — the pool runs
  inside the request handler. For very large pools the request may
  block longer than the FastAPI keepalive; consider moving heavy
  optimize calls to background research jobs.

---

## Phase 6 — Rust native acceleration (NOT IMPLEMENTED)

**Decision rationale**: the plan's Phase 6 go-criteria require all of:

  * Phase 0 profile shows a pure array loop as the remaining bottleneck
  * The bottleneck has stable inputs/outputs
  * Expected speedup ≥ 3× for a meaningful workload
  * Existing tests can compare native output against Python output

After Phases 1–3, MultiFactorHF is at 1.37 s (87× from baseline) and
MA/MACD are at 3.16–3.80 s. None of the remaining functions show as a
runaway hot path that's also a clean array kernel. The MA replay loop
still calls a Python strategy `generate_signals` per bar — not an
array loop. Rewriting it in Rust would require either embedding the
strategy logic (defeats the "backtest equals live" rule) or per-bar
Python-to-Rust calls (FFI overhead dominates).

**Trigger to revisit**: if a future workload type (e.g. 1-second tick
data, multi-strategy live simulation, very large parameter sweeps)
appears that has a top-N profile dominated by one pure-array function,
revisit. Until then, the Python build remains the simplest deployment
target.

**Documentation only — no code added**. `native_backtest/`,
`maturin`, `pyo3`, and `core/backtest/native.py` are intentionally
absent.

---

## Phase 7 — Strategy DSL (NOT IMPLEMENTED)

**Decision rationale**: the plan itself says:

> Do not start here. This is only useful after the current
> bottlenecks are measured and the batch factor layer exists.

The batch factor layer now exists (Phase 2). A DSL on top of it would
introduce a second source of truth for strategy logic — the YAML/DSL
text vs the Python class — and that duplication is the same risk the
Phase 1 `mutates_input` and Phase 2 parity tests exist to prevent.
Plus, the project already has `config/strategy_registry.py` driving
metadata and `core/ai/research_planner.py` doing parameter generation;
a DSL would overlap with both without obvious payoff.

**Trigger to revisit**: if strategy authoring becomes a bottleneck
(e.g. dozens of nearly-identical strategies being hand-coded), revisit
a config-driven approach scoped to that exact pattern. Until then,
adding strategies as Python classes remains cheaper than the
infrastructure to compile them from a DSL.

**Documentation only — no code added.**

---

## Phase 4-lite — Request-scoped cache (NOT IMPLEMENTED)

**Audit conclusion** (2026-05-20): the plan's premise — that compare
and optimize reload OHLCV per strategy/trial — is not true in this
codebase. `compare_backtests` (web/api/backtest.py:4109) loads
`common_df` once and iterates strategies on it.
`optimize_backtest` (line 4508) loads `df` once and passes it to
`_optimize_strategy_on_df`, which runs all trials on the same frame.
Factor caching would also be moot because optimize is on `MAStrategy`
(no `compute_factor` calls) and `MultiFactorHFStrategy` already
precomputes factors once per call via the Phase 2 fast path.

No code added. If a future regression introduces redundant loading,
add the cache then.

---

## Phases implemented summary

| Phase | What | Status | Default flag |
|-------|------|--------|--------------|
| 0 | Benchmark + cProfile | ✅ done | n/a (script-only) |
| 1 | Replay view fast path (`mutates_input`) | ✅ done | `BACKTEST_REPLAY_VIEW_FAST_PATH: bool = True` |
| 2 | MultiFactorHF batch factor precompute | ✅ done | `BACKTEST_FAST_EXACT_STRATEGIES: bool = False` (opt-in) |
| 3 | Array execution simulator | ✅ done | `BACKTEST_FAST_EXIT_ARRAYS: bool = False` (opt-in) |
| 4-lite | Request-scoped cache | ⚪ verified unnecessary | n/a |
| 5 | Parallel optimize trials | ✅ done | `BACKTEST_OPTIMIZE_WORKERS: int = 1` (opt-in to parallel) |
| 6 | Rust native | ⚪ skipped by design | n/a |
| 7 | Strategy DSL | ⚪ skipped per plan | n/a |

---

## Rollback fire drill

To validate the rollback path on a live deployment:

1. Note current `.bench/baseline_latest.json` result hashes.
2. Edit `config/settings.py` (or set the matching env vars):
   ```python
   BACKTEST_REPLAY_VIEW_FAST_PATH: bool = False    # Phase 1 off
   BACKTEST_FAST_EXACT_STRATEGIES: bool = False    # Phase 2 off (already default)
   BACKTEST_FAST_EXIT_ARRAYS: bool = False         # Phase 3 off (already default)
   BACKTEST_OPTIMIZE_WORKERS: int = 1              # Phase 5 off (already default)
   ```
3. Restart the FastAPI process.
4. Rerun `python scripts/benchmark_backtest_runtime.py`.
5. Compare result hashes against pre-fast-path baseline
   (`.bench/baseline_27c82a7.json` — the very first run before any
   phase landed). Trade counts and total_return should match within
   the bench-window noise (~0.5 %, caused by `datetime.now() - 30d`
   shifting the data window slightly between runs).
6. Restore the flags. All fast paths are back on.

Any flag flip is a runtime restart, never a code revert. To run all
the parity tests in one shot:
```
python -m pytest \
    tests/test_backtest_runtime_consistency.py \
    tests/test_multi_factor_hf_parity.py \
    tests/test_execution_arrays_parity.py \
    tests/test_optimize_parallel.py \
    -q
```
67 tests total (47 pre-existing + 4 Phase 1 + 5 Phase 2 + 9 Phase 3 +
2 Phase 5). Any failure means that phase's fast path must be disabled
until the divergence is reconciled.
