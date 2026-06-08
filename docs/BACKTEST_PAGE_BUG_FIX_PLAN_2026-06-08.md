# Backtest Page Bug Fix Plan (2026-06-08)

Scope: diagnosis only. No business code was changed in this pass.

## Validation Snapshot

- Runtime checked with `web.bat start -NoNewsWorkers`; service came up healthy in paper mode, and guarded startup blocked persisted live-mode restore.
- Browser smoke on `http://127.0.0.1:8000` confirmed the Backtest tab is visible, strategy catalog loads, and the page currently renders 70 strategy options plus 70 compare-picker entries.
- Focused tests passed:
  - `python -m pytest tests/test_backtest_pairs_ui_assets.py tests/test_backtest_factor_modes_ui_assets.py tests/test_arbitrage_ui_assets.py -q`
  - `python -m pytest tests/web/test_backtest_compare_route.py tests/web/test_backtest_pairs_route.py -q`
  - `python -m pytest tests/test_strategy_library_and_factors.py::test_mlxgboost_backtest_support_reflects_runtime_dependencies tests/test_strategy_library_and_factors.py::test_fama_factor_strategy_is_supported_in_web_backtest tests/test_strategy_signal_regressions.py::test_backtest_optimization_grid_keys_are_declared_in_defaults -q`
- Broader `tests/test_strategy_library_and_factors.py` timed out in one combined run, so that broad run is not treated as pass/fail evidence.

## Bug 1 - Parameter Optimization Still Uses Fixed 90s Timeout

Evidence:
- `web/static/js/app.js:800` defines dynamic single-run timeout estimation.
- `web/static/js/app.js:863` defines dynamic multi-strategy compare timeout estimation.
- `web/static/js/app.js:8928-8935` still sends `/backtest/optimize` with `timeoutMs:90000`.
- Preview paths also still use fixed 90s guards at `web/static/js/app.js:6087` and `web/static/js/app.js:6150`.

Impact:
- Long optimization runs can fail at exactly 90 seconds even while the backend is still working.
- This is especially likely with high `max_trials`, larger windows, slower strategies, or custom parameter payloads.

Fix plan:
1. Add an `estimateBacktestOptimizeTimeoutMs(strategy, maxTrials, timeframe, windowDays, customParams)` helper that mirrors the compare timeout style but is scoped to one strategy.
2. Use the helper in `btn-backtest-optimize` instead of `timeoutMs:90000`.
3. Use the same helper or a shared preview timeout helper in optimize/compare preview flows.
4. Update UI asset tests to assert the optimize button no longer hard-codes `90000`.
5. Add a regression test for the timeout formula bounds, including large trial counts and Fama/custom-param cases.

Acceptance checks:
- Static test fails if optimize request uses `timeoutMs:90000`.
- Browser smoke shows the optimize loading text reports the computed timeout.
- Existing compare/pairs route tests still pass.

## Bug 2 - Custom Parameter Hint Contradicts Actual Optimize Behavior

Evidence:
- `web/static/js/app.js:1281` and `web/static/js/app.js:1368` say: "多策略对比 / 参数优化暂不读取这里的 JSON。"
- `web/static/js/app.js:8933` appends `params_json` for optimize when custom params exist.
- `web/api/backtest.py:5053-5073` parses `params_json` and merges it into `base_params`.
- Runtime browser check confirmed the stale hint text is visible on the Backtest tab.

Impact:
- Users may incorrectly believe parameter optimization ignores imported/custom strategy parameters.
- This is particularly misleading for Arbitrage -> Backtest and ML/custom strategies where the JSON is required.

Fix plan:
1. Change the hint to distinguish behavior accurately: single run and optimize read the JSON; multi-strategy compare does not read the free-form JSON except purpose-built compare/factor flows.
2. Keep the Arbitrage import note consistent with the default custom-param hint.
3. Add/update UI asset tests that assert the corrected hint and assert optimize still sends `params_json`.

Acceptance checks:
- Browser DOM no longer contains the stale "参数优化暂不读取" sentence.
- Existing `params_json` backend tests for compare/custom/optimize still pass.

## Bug 3 - Fixed Take-Profit UI Allows 1.0 But Frontend/Backend Reject It

Evidence:
- `web/templates/index.html:3194-3195` labels fixed take-profit as `0.01~1.0` and sets `max="1.0"`.
- `web/static/js/app.js:5374-5375` and `web/static/js/app.js:5386-5387` only accept values `< 1`.
- `web/api/backtest.py:859-866` rejects values `>= 1`.
- Runtime browser check confirmed the take-profit input has `max="1.0"`.

Impact:
- A user can enter/select `1.0`, but the value is silently omitted from requests.
- The result can show fixed protection enabled with missing take-profit, causing confusing output.

Fix plan:
1. Decide the intended contract. The lower-risk fix is to keep the backend rule `< 1` and change the UI label/max to `0.99`.
2. Add client-side validation/notification when fixed protection is enabled but either percentage is outside the accepted range.
3. Add UI asset assertions for `max="0.99"` or equivalent validation text.
4. Add a small frontend unit/static test around `getBacktestProtectionConfig` semantics if the repo keeps static JS assertions.

Acceptance checks:
- Browser DOM no longer exposes an accepted-looking `1.0` take-profit option.
- Invalid fixed protection values produce a visible warning instead of silent parameter loss.

## Recommended Fix Order

1. Fix Bug 1 first, because it causes failed user operations and already has historical evidence.
2. Fix Bug 2 with the same patch if touching optimize/custom-param UI text.
3. Fix Bug 3 last; it is smaller but should include a visible validation warning to avoid future silent drops.

