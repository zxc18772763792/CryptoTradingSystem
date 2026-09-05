# Backtest Performance Acceleration Plan (2026-05-20)

> Scope: this plan is about making local strategy backtests and research scans faster without breaking the "backtest equals live strategy logic" direction already recorded in `STABILITY_PROFITABILITY_IMPROVEMENT_PLAN_2026-05-20.md`.
>
> Core decision: do not rewrite the whole strategy system in Rust/C first. Keep Python as the orchestration and research layer, remove the current pandas-per-bar hot spots, then move only stable array kernels to Numba or Rust.

## 1. Why This Plan Exists

The local repository already has many strategies and the backtest page supports single backtest, compare, optimize, pairs, cross-sectional factor, stop/take, and trade diagnostics. The current speed problem is likely dominated by repeated dataframe slicing and repeated rolling-factor calculation, not by Python syntax alone.

The most important current hot paths are:

- `web/api/backtest.py::_replay_signal_strategy_position`
  - Loops over every bar.
  - Builds `df.iloc[: end_idx + 1].tail(live_window).copy()` on each step.
  - Calls the real strategy class every step.
- `core/backtest/backtest_engine.py::BacktestEngine.run_backtest`
  - Loops over every bar.
  - Builds `current_data = data.iloc[: i + 1]`.
  - Calls `strategy.generate_signals(data.iloc[:i].tail(live_window))`.
- `strategies/quantitative/multi_factor_hf.py::generate_signals`
  - Copies each input window.
  - Recomputes factor series inside each bar decision.
- `core/factors_ts/registry.py::compute_factor`
  - Instantiates and computes a factor series on demand, which is fine for batch mode but expensive when repeated per bar.
- `web/api/backtest.py::_simulate_execution_summary`
  - Delegates to `core/backtest/exit_engine.py`; this is a good candidate for an array kernel once the signal-position input is stable.

The local dependencies already include `numpy`, `pandas`, `polars`, `pyarrow`, and `ta-lib`. That means the first speedups should use the existing Python-native numeric stack before introducing a native build toolchain.

## 2. Design Principles

1. Preserve result trust before optimizing runtime.
   - The project already moved toward `BACKTEST_USE_REAL_STRATEGY=True`. Any fast path must prove parity against the trusted real-strategy replay path, or be explicitly labeled approximate.

2. Optimize at the array/batch boundary.
   - Good: one call receives full `close/high/low/volume/signal_position` arrays and returns full `position/turnover/returns/trade_log`.
   - Bad: Python calls Rust/C once per bar. That would keep interpreter overhead and add FFI overhead.

3. Keep strategy iteration in Python.
   - Strategy authors should still be able to add or tune strategies quickly in Python/YAML.
   - Rust/C should be used for stable kernels, not for rapidly changing research logic.

4. Separate three execution modes.
   - `trusted`: real strategy replay, slower, used for final validation and live parity.
   - `fast_exact`: vectorized or cached implementation with parity tests against `trusted`.
   - `fast_approx`: intentionally approximate screening path, allowed only when response metadata/UI says so.

5. Make every speedup measurable.
   - Each phase must record wall time, bars/sec, memory, and output parity on fixed benchmark cases.

## 3. Target Architecture

```text
Python orchestration layer
  FastAPI routes, strategy registry, data loading, parameter search, reports

Strategy compatibility layer
  real StrategyBase replay for trusted validation
  optional fast position builders for strategies with parity tests

Feature/factor batch layer
  pandas/Polars/TA-Lib/NumPy
  precompute complete factor columns once per dataframe and parameter set

Execution simulation layer
  NumPy first
  optional Numba or Rust after the API stabilizes
  inputs: arrays and scalar config
  outputs: arrays, trade stats, trade log

Native acceleration layer, optional
  Rust/PyO3/maturin only for stable array kernels
```

## 4. Phase 0 - Benchmark And Profiling Baseline

Goal: measure before changing behavior.

### Tasks

- Add a local benchmark script:
  - `scripts/benchmark_backtest_runtime.py`
  - It should run fixed cases and print machine-readable JSON.
- Benchmark cases:
  - Single strategy, 5m, 1 symbol, 3 months.
  - Single strategy, 5m, 1 symbol, 1 year.
  - MultiFactorHFStrategy, 5m, 1 symbol, 1 year.
  - Compare mode with 10 strategies.
  - Optimize mode with 32 trials.
  - One sub-minute case if local 1s/5s data exists.
- For each case record:
  - `strategy`
  - `timeframe`
  - `bars`
  - `trials`
  - `wall_seconds`
  - `bars_per_second`
  - `peak_memory_mb` if available
  - `total_return`
  - `sharpe_ratio`
  - `total_trades`
  - a small result hash from key numeric series when `include_series=True`
- Run profiling:
  - `python -m cProfile -o .bench/backtest_profile.prof scripts/benchmark_backtest_runtime.py`
  - Optional: `py-spy` or `scalene` if installed.

### Acceptance Criteria

- A repeatable baseline exists under `.bench/` or printed JSON.
- The top five runtime functions are known.
- No optimization work starts without one saved baseline result.

### Expected Findings

Likely top costs:

- dataframe slicing/copying in replay loops
- repeated rolling indicators
- repeated strategy object calls
- exit/trade simulation loops
- optimization repeating the same factor calculations for nearby parameters

## 5. Phase 1 - Low-Risk Python Hot Path Cleanup

Goal: remove obvious avoidable overhead while preserving behavior.

### Tasks

- In real-strategy replay, avoid full dataframe copies where possible.
  - Current pattern: `df.iloc[: end_idx + 1].tail(live_window).copy()`.
  - Safer first step: use `df.iloc[start:end_idx + 1]` without `.copy()` unless a strategy mutates the frame.
  - Detect mutating strategies. `MultiFactorHFStrategy` currently writes `symbol` into `work = data.copy()`, so this one is safe if it receives a view.
- Pre-add stable columns such as `symbol` before replay when missing.
  - This avoids strategy-local copy just to attach a constant symbol.
- Avoid repeated `pd.to_numeric` on columns inside tight loops.
  - Normalize OHLCV numeric columns once in `_load_backtest_inputs` or before `_run_backtest_core`.
- Add a small in-request cache for `_build_backtest_position_series`.
  - Key: strategy name, dataframe identity/hash, params hash, date range, timeframe.
  - This is especially useful in compare/optimize paths that request the same position series more than once.

### Files To Inspect/Edit

- `web/api/backtest.py`
- `core/backtest/backtest_engine.py`
- `strategies/quantitative/multi_factor_hf.py`
- `tests/test_backtest_runtime_consistency.py`
- `tests/test_backtest_bidirectional_positions.py`

### Acceptance Criteria

- Existing backtest tests pass.
- `test_backtest_page_uses_real_strategy_path` still passes.
- Benchmark improves by at least 1.5x on at least one real-strategy replay case, or the profile shows Phase 2 is the real bottleneck.
- Results are numerically unchanged except for harmless metadata ordering.

## 6. Phase 2 - Batch Factor Precomputation

Goal: stop recomputing the same rolling factors on every bar.

### Core Idea

Instead of this:

```text
for each bar:
  take trailing window
  compute ema_slope series
  compute zscore series
  compute realized_vol series
  compute atr_pct series
  compute volume_z series
  read last row
```

Use this:

```text
once per dataframe and parameter set:
  compute factor columns for the full dataframe

for each bar:
  read precomputed factor values at current index
  update state machine
```

### Tasks

- Add a factor batch API:
  - candidate module: `core/factors_ts/batch.py`
  - function: `compute_factor_frame(df, specs) -> pd.DataFrame`
  - output columns should include deterministic names matching strategy metadata needs.
- Add a MultiFactorHF fast path:
  - candidate function: `build_multifactor_hf_position_series(df, params) -> pd.Series`
  - It should implement the same score, gate, cooldown, virtual side, and signal-to-position state as `MultiFactorHFStrategy`.
- Keep `MultiFactorHFStrategy.generate_signals` unchanged for live use at first.
- Add parity tests:
  - compare `build_multifactor_hf_position_series` against `_replay_signal_strategy_position(MultiFactorHFStrategy, ...)`.
  - Use multiple synthetic datasets: trend, chop, high volatility, low volume, missing volume.
  - Assert exact sign agreement or explain tolerated differences.
- Wire fast path behind a flag:
  - `BACKTEST_FAST_EXACT_STRATEGIES=True`
  - Only strategies with parity tests can use it.
  - The response should include `backtest_engine_mode: fast_exact|trusted|fast_approx`.

### Files To Inspect/Edit

- `strategies/quantitative/multi_factor_hf.py`
- `core/factors_ts/registry.py`
- `core/factors_ts/impl.py`
- `core/factors_ts/extended_factors.py`
- `web/api/backtest.py`
- `config/settings.py`
- `tests/test_multi_factor_hf_strategy.py`
- `tests/test_backtest_runtime_consistency.py`

### Acceptance Criteria

- MultiFactorHF backtest is at least 5x faster on a 1-year 5m case.
- Parity against real-strategy replay is locked in tests.
- Fast path can be disabled by config to return to trusted replay.
- UI/API can expose which mode produced the result.

## 7. Phase 3 - Array Execution Simulator

Goal: make stop/take/trailing/PnL/turnover simulation faster and easier to native-accelerate later.

### Core Idea

Create a pure array execution API independent of pandas:

```python
simulate_execution_arrays(
    close: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    signal_position: np.ndarray,
    config: dict,
) -> ExecutionArrays
```

The pandas wrapper then converts inputs/outputs at the boundary.

### Tasks

- Add a new internal module:
  - candidate: `core/backtest/execution_arrays.py`
- Implement NumPy/Python version first.
- Keep existing `core/backtest/exit_engine.py` as the trusted implementation until parity is proven.
- Add parity tests:
  - no stops
  - fixed stop loss
  - fixed take profit
  - trailing stop
  - reversal
  - long and short
  - same-bar exit disabled
- After parity, route `_simulate_execution_summary` through the array implementation for supported configs.
- Keep fallback to `run_exit_engine` for unsupported configs.

### Acceptance Criteria

- Same effective position, turnover, gross returns, trade counts, and exit reason breakdown as the current exit engine on fixtures.
- At least 2x speedup on execution simulation after position series is precomputed.
- The array API has no pandas dependency.

## 8. Phase 4 - Research And Optimization Caching

Goal: avoid recomputing the same data and factors across trials.

### Tasks

- Add per-request dataframe normalization cache.
- Add per-request factor cache:
  - key by factor name, parameters, dataframe index hash, source columns hash/version.
- In optimize mode, group trials by shared expensive factor parameters where possible.
- In compare mode, load data once per symbol/timeframe/date range and reuse.
- Persist optional disk cache for expensive factor frames:
  - candidate path: `data/cache/backtest_features/`
  - key should include symbol, exchange, timeframe, start, end, params hash, code version.
  - Start with in-memory only unless profiling proves disk cache is needed.

### Files To Inspect/Edit

- `web/api/backtest.py`
- `core/research/strategy_research.py`
- `core/research/orchestrator.py`
- `core/data/data_storage.py`

### Acceptance Criteria

- Optimize mode with 32 trials improves by at least 3x for factor-heavy strategies.
- Compare mode does not reload the same OHLCV dataframe for each strategy.
- Cache is request-scoped by default, so stale strategy code cannot silently reuse old results.

## 9. Phase 5 - Parallelism With Guardrails

Goal: use CPU cores for independent trials without overloading the local machine or web server.

### Tasks

- Add controlled parallel execution for optimization trials.
- Prefer process pools for CPU-bound work if pandas/NumPy work does not release the GIL enough.
- Add settings:
  - `BACKTEST_MAX_WORKERS`
  - `BACKTEST_PARALLEL_MIN_TRIALS`
  - `BACKTEST_PARALLEL_MAX_BARS`
- Keep interactive web requests bounded.
  - Large jobs should move to background research jobs instead of blocking FastAPI workers.
- Preserve deterministic output ordering.

### Acceptance Criteria

- Parameter optimization speed scales on a 4-core or 8-core local machine.
- Web API remains responsive under compare/optimize load.
- Memory usage is bounded and does not duplicate large frames unnecessarily.

## 10. Phase 6 - Native Acceleration Decision Gate

Goal: only introduce Rust/C after the Python array API is stable and measured.

### Go Criteria

Move to native acceleration only if all are true:

- Phase 0 profiling shows the remaining bottleneck is a pure array loop.
- Phase 2 and Phase 3 have already removed dataframe slicing and repeated rolling bottlenecks.
- The target function has stable inputs and outputs.
- Existing tests can compare native output against Python output.
- Expected speedup is at least 3x for a meaningful workload.

### No-Go Criteria

Do not native-rewrite if:

- The bottleneck is data loading, API I/O, or pandas slicing that can be removed.
- The logic is still changing frequently.
- It would duplicate live strategy logic and risk backtest/live drift.
- It would require per-bar Python-to-native calls.

### Recommended Native Path

Prefer Rust over C/C++ for this repository:

- Use `maturin` and `pyo3`.
- Build a Python package such as `native_backtest`.
- Export only batch functions.
- Keep Python fallback as the reference implementation.

Candidate native functions:

```text
simulate_execution_arrays
stateful_directional_position
rolling_rank_last / rolling_zscore kernels if still hot
orderbook/tick-level microstructure factors
```

Candidate directory:

```text
native_backtest/
  Cargo.toml
  pyproject.toml
  src/lib.rs
```

Python wrapper:

```text
core/backtest/native.py
```

### Acceptance Criteria

- Native module is optional.
- If import fails, Python fallback works.
- Native and Python outputs match on parity tests.
- Benchmarks prove speedup on local data.

## 11. Phase 7 - Optional Strategy DSL

Goal: make future strategies easier to batch and optimize.

This is optional and should not block the earlier phases.

A future config-driven strategy representation could describe:

```yaml
factors:
  - name: ema_slope
    params: {fast: 8, slow: 21}
    weight: 0.65
    transform: zscore
  - name: zscore_price
    params: {lookback: 30}
    weight: -0.35
    transform: none
entry:
  long: score > enter_th and gates_all_ok
  short: score < -enter_th and gates_all_ok
exit:
  flat: abs(score) < exit_th or not gates_all_ok
state:
  cooldown_bars: 2
```

If adopted, this DSL can compile to:

- live Python strategy
- trusted replay
- fast vectorized backtest
- Rust/Numba kernels where appropriate

Do not start here. This is only useful after the current bottlenecks are measured and the batch factor layer exists.

## 12. Testing Strategy

Required test groups:

- Runtime consistency:
  - `tests/test_backtest_runtime_consistency.py`
- Bidirectional positions:
  - `tests/test_backtest_bidirectional_positions.py`
- MultiFactorHF:
  - `tests/test_multi_factor_hf_strategy.py`
- Exit engine parity:
  - new `tests/test_execution_arrays_parity.py`
- Cost model:
  - `tests/test_backtest_cost_models.py`
- API routes:
  - `tests/web/test_backtest_pairs_route.py`
  - `tests/web/test_backtest_compare_route.py`

Parity rules:

- `trusted` vs `fast_exact` should match position sign exactly unless a test documents the edge case.
- PnL numeric differences should be less than a strict tolerance such as `1e-9` for pure NumPy paths.
- Trade count and exit reason breakdown must match exactly for execution parity.
- `fast_approx` must never be silently promoted to trusted output.

## 13. Local Rollout Order

Recommended order:

1. Phase 0 benchmark/profiling.
2. Phase 1 low-risk cleanup.
3. Phase 2 MultiFactorHF fast exact path.
4. Phase 3 array execution simulator.
5. Phase 4 caching for optimize/compare.
6. Phase 5 controlled parallelism.
7. Phase 6 Rust only if profiling still justifies it.
8. Phase 7 DSL only if strategy growth keeps making parity expensive.

Do not start Rust before Phase 3. Native acceleration should be the last mile after the data and API shape is right.

## 14. Suggested Milestone Checklist

### Milestone A - Measured Baseline

- [x] `scripts/benchmark_backtest_runtime.py` exists. (2026-08-07 local scan: benchmark script is present.)
- [ ] Baseline JSON recorded for at least three workloads.
- [ ] cProfile output identifies top runtime costs.
- [ ] No behavior changes yet.

### Milestone B - Faster Trusted Replay

- [x] Replay loop avoids unnecessary dataframe copies. (2026-08-10 local scan: `_replay_signal_strategy_position` skips per-bar `.copy()` for strategies that opt in with `mutates_input = False`, guarded by `BACKTEST_REPLAY_VIEW_FAST_PATH`.)
- [ ] OHLCV numeric normalization happens once.
- [ ] Position series cache exists for one request.
- [x] Existing runtime consistency tests pass. (2026-08-10 local scan: `tests/test_backtest_runtime_consistency.py` passed.)
- [ ] Benchmark shows improvement or confirms next bottleneck.

### Milestone C - MultiFactorHF Fast Exact

- [ ] Batch factor frame API exists.
- [x] MultiFactorHF fast position builder exists. (2026-08-09 local scan: `strategies/quantitative/multi_factor_hf_fast.py` provides `build_multifactor_hf_position_series`, gated by `BACKTEST_FAST_EXACT_STRATEGIES` in `web/api/backtest.py`.)
- [x] Parity tests against real replay pass. (2026-08-09 local scan: `tests/test_multi_factor_hf_parity.py` covers five regime fixtures against `_replay_signal_strategy_position(MultiFactorHFStrategy, ...)`.)
- [ ] API result exposes `backtest_engine_mode`.
- [ ] 1-year 5m benchmark improves materially.

### Milestone D - Array Execution Engine

- [x] `core/backtest/execution_arrays.py` exists. (2026-08-07 local scan: array simulator module is present.)
- [ ] Parity tests cover stop/take/trailing/reversal.
- [x] `_simulate_execution_summary` can use array path. (2026-08-07 local scan: opt-in `BACKTEST_FAST_EXIT_ARRAYS` dispatches supported configs to `simulate_execution_arrays`.)
- [x] Python fallback remains available. (2026-08-07 local scan: unsupported configs and array-path exceptions still fall back to `run_exit_engine`.)

### Milestone E - Optimize/Compare Scale

- [ ] Per-request factor cache exists.
- [ ] Optimize mode reuses precomputed factors where possible.
- [ ] Compare mode avoids repeated data loads.
- [x] Optional parallel trial execution is bounded by settings. (2026-08-10 local scan: `_optimize_strategy_on_df` uses `BACKTEST_OPTIMIZE_WORKERS` and `BACKTEST_OPTIMIZE_PARALLEL_MIN_TRIALS`, with serial fallback/parity coverage in `tests/test_optimize_parallel.py`.)

### Milestone F - Native Gate

- [ ] Remaining bottleneck is a stable array function.
- [ ] Python reference implementation exists.
- [ ] Native module is optional and tested.
- [ ] Speedup justifies added build complexity.

## 15. Key Risks

1. Backtest/live drift.
   - Mitigation: every fast exact path must have parity tests against real strategy replay.

2. Approximate paths being mistaken for trusted validation.
   - Mitigation: return `backtest_engine_mode` and show it in UI/report metadata.

3. Cache staleness.
   - Mitigation: start with request-scoped cache. Add disk cache only with code-version keys.

4. Native build complexity.
   - Mitigation: Rust is optional, Python fallback remains reference.

5. Premature optimization of the wrong layer.
   - Mitigation: Phase 0 benchmark is mandatory.

## 16. Bottom Line

The best improvement path is:

```text
measure -> remove dataframe-copy/recompute hot spots -> precompute factors -> array execution simulator -> cache/parallelize -> native Rust only for stable kernels
```

This keeps the system flexible for research, protects live/backtest consistency, and still leaves a clean path to Rust/C-level performance where it is actually useful.
