# Code Review Report — 2026-05-26

Automated daily review. Test run: **1740 passed, 1 skipped**.

---

## Fixes Applied This Session

### 1. Stale test constant (FIXED)
**File:** `tests/test_ai_research_phase5_ui_assets.py:184`
**Severity:** Low (test-only, no production impact)

`TRADING_STATS_TIMEOUT_MS` was raised from 25 000 ms to 35 000 ms in a recent reliability fix (commit `ec7ca18`), but the assertion in the UI-assets test still checked for the old value. The test was trivially fixed by updating the constant.

```diff
-    assert "const TRADING_STATS_TIMEOUT_MS=25000;" in app_js
+    assert "const TRADING_STATS_TIMEOUT_MS=35000;" in app_js
```

---

### 2. Circuit breaker global state bleeding into tests (FIXED)
**File:** `tests/conftest.py` — autouse fixture `_isolate_runtime_side_effect_paths`
**Severity:** High (caused 11 test failures across two test files)

**Root cause:** `core/risk/circuit_breaker.py` instantiates a module-level singleton (`circuit_breaker = CircuitBreaker()`) that loads its state from disk at import time. The production circuit-breaker state file (`data/cache/runtime_state/circuit_breaker.json`) has `portfolio.tripped = true` (daily drawdown 4.27 % ≥ 3 % threshold from live trading). Because the conftest fixture patched `settings.CACHE_PATH` but did NOT reset the already-loaded singleton, every test that called `engine.execute_signal()` with `_paper_trading = False` hit the circuit breaker before the AI-decision or order logic, making the assertions on `ai_rejected`, `ai_reduce_only_rejected`, etc. fail.

**Fix:** Added a circuit-breaker reset block to the conftest autouse fixture, consistent with how `position_manager`, `risk_manager`, and `account_manager` are already isolated:

```python
cb_module = sys.modules.get("core.risk.circuit_breaker")
if cb_module is not None:
    cb = getattr(cb_module, "circuit_breaker", None)
    if cb is not None:
        cb._store_path = tmp_path / "cache" / "runtime_state" / "circuit_breaker.json"
        with cb._lock:
            cb._portfolio = cb_module._PortfolioState()
            cb._strategies = {}
```

Affected tests (all now passing):
- `tests/test_execution_engine_ai_live_decision.py` — 10 tests
- `tests/test_execution_engine_coinglass_filters.py` — 1 test

---

## Code Bugs Found (Not Yet Fixed)

### 3. Reduce-only failure counter cleared before close attempt
**File:** `core/trading/execution_engine.py:4797`
**Severity:** Medium

When the force-close branch triggers (failure count ≥ threshold), the failure counter is cleared at line 4797 **before** `position_manager.close_position()` is called at line 4802. If `close_position()` returns falsy (e.g. if the local position was already deleted by a concurrent reconcile), the counter is gone and the next set of reduce-only rejections will need to accumulate `threshold` failures all over again before another force-close is attempted. This can extend a retry storm.

**Suggested fix:** Move `self._reduce_only_failure_counts.pop(fail_key, None)` into the `if closed:` block at line 4810, so the counter is only erased on a confirmed local close.

---

### 4. Mode-pin detection uses falsy check instead of `is not None`
**File:** `core/strategies/strategy_manager.py:799–806`
**Severity:** Low (practical risk is near-zero — mode values are always non-empty strings)

`sync_runtime_mode_to_global` determines whether a strategy has a pinned runtime mode using:

```python
pinned = (
    params.get("runtime_mode")
    or params.get("trading_mode")
    or params.get("mode")
    or ...
)
```

If any mode key is explicitly set to `""` (empty string), the expression evaluates to falsy and the pin is silently ignored, causing the strategy to follow the global mode change. This would only matter if something wrote an empty string as a mode value — unlikely today, but fragile.

**Suggested fix:**
```python
pinned = (
    params.get("runtime_mode") is not None
    or params.get("trading_mode") is not None
    or params.get("mode") is not None
    or metadata.get("runtime_mode") is not None
    or metadata.get("trading_mode") is not None
    or metadata.get("mode") is not None
)
```

---

### 5. Stale equity guard does not cover negative `prev_equity`
**File:** `web/api/trading_balances.py:868–872`
**Severity:** Low

The `prev_equity_unreliable` guard (added in commit `0d07bef`) only activates when `prev_equity > 0`:

```python
prev_equity_unreliable = (
    prev_equity > 0
    and (
        prev_equity < 10.0
        or (sanity_baseline > 0 and prev_equity < sanity_baseline * 0.1)
    )
)
```

If `prev_equity` is negative (e.g. a bad read from an uninitialized equity store), the guard is skipped and the negative value is used downstream in risk-cap and delta calculations. The effect is benign in practice because further guards at lines 887 and 903 (`prev_equity_for_check > 0`) prevent acting on negative values, but it is still inconsistent with the intent.

**Suggested fix:** Add `or prev_equity < 0` to treat negative equity as unreliable:
```python
prev_equity_unreliable = (
    prev_equity < 0
    or (prev_equity > 0 and prev_equity < 10.0)
    or (prev_equity > 0 and sanity_baseline > 0 and prev_equity < sanity_baseline * 0.1)
)
```

---

### 6. Callback exceptions silently swallowed in DataCollector
**File:** `core/data/data_collector.py` (callback dispatch loop)
**Severity:** Low

Exceptions thrown by data callbacks are caught and logged but not surfaced to callers. If a downstream consumer (e.g. sentiment processor, storage layer) raises an exception, the data collection loop continues silently. This is consistent with the fire-and-forget pattern used elsewhere, but may mask data-loss scenarios.

**Suggested improvement:** At minimum, track a failed-callback count in diagnostics so the health endpoint can surface it.

---

## Architecture Observations

- **Circuit breaker isolation pattern** — the `circuit_breaker`, `position_manager`, `risk_manager`, and `account_manager` singletons all share the same "load from disk at import" pattern. Any new singleton that follows this pattern must be added to the conftest isolation fixture.

- **Execution path ordering** — the ordering of gates in `_execute_signal_in_active_mode` is: circuit-breaker → structural-risk-gate → AI-live-decision → order logic. Tests that target inner layers must mock all outer gates, or the tests become load-order-dependent on production state.

- **TRADING_STATS_TIMEOUT_MS = 35 000 ms** — bumped from 25 s in the Wave 1 reliability fix. Any documentation or API contracts that reference a 25 s timeout should be updated.
