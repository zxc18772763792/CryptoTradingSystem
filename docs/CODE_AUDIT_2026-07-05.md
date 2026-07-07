# Code Audit - 2026-07-05

Scope: full local audit of `F:\9_Crypto\crypto_trading_system` on branch
`codex/operating-reality-bug-sweep`, HEAD `05851a5`.

This pass did not change business code. It added this report only.

## Validation Summary

- Full test suite: `2059 passed, 1 skipped in 365.70s`
  - Command: `F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe -m pytest -q --tb=short`
  - Output: `logs/audit_2026-07-05/pytest_full.out.txt`
- Focused safety tests: `93 passed in 107.36s`
  - Covered sensitive API auth, startup mode, protective levels, circuit breaker, order connectivity.
- Syntax import check: `python -m compileall -q config core web strategies prediction_markets` passed.
- `ruff check .` with default rules: 361 findings.
  - Mostly import-order and unused-import / unused-variable issues.
  - There is no repo-level Ruff config (`pyproject.toml`, `ruff.toml`, `.ruff.toml`) found, so this is not currently a meaningful CI-quality gate.
- Narrow tracked-secret scan: no real credential-shaped hits found.
  - One false positive in a remediation-plan doc matched `sk-...` shape.

## Critical / High Findings

### 1. Running service is currently live

Evidence:

- `.\web.bat status` reports `Web: running (PID=9408, state=healthy, mode=live)`.
- Status also reports `Startup mode: configured=live, persisted=paper, source=configured`.
- Local config contains `TRADING_MODE=live` and `ALLOW_PERSISTED_LIVE_MODE_START=true`.

Impact:

The repository documentation emphasizes managed startup defaulting to paper mode, but the active local runtime is live. Any audit, test, or manual operation that assumes paper mode can have real-money consequences.

Recommendation:

- Treat the current process as live until explicitly changed.
- Before any further operational testing, either confirm live mode is intentional or switch/stop via the protected control path.
- Keep `.env` defaulted to paper and use a separate explicitly named live profile for live-shadow or real trading.

### 2. Binance futures fast-path ambiguous submit can be downgraded to ordinary failure

Evidence:

- `core/trading/order_manager.py:693-700` submits Binance futures orders through the raw signed fast path.
- `core/trading/order_manager.py:745-748` catches any fast-path exception and immediately falls back to `exchange.create_order(...)`.
- `core/trading/order_manager.py:779-788` retains `clientOrderId` only when the final exception is classified ambiguous; otherwise it releases the id.

I reproduced offline with monkeypatches: fast path raises `asyncio.TimeoutError`, fallback raises duplicate client order id, and the final state is:

```text
result_is_none=True
active_after_duplicate_fallback=False
last_error='Duplicate client order id: -4111 duplicated clientOrderId'
```

Impact:

If the fast-path timeout happened after the exchange accepted the order, a duplicate-id response from fallback is evidence that exchange state may already exist. Releasing the local `clientOrderId` and recording the submit as ordinary failure can leave local state out of sync with the venue. A later fresh signal may submit a new order while the original order is open or filled.

Recommendation:

- If the fast-path exception is ambiguous, do not immediately fallback as a normal submit.
- Keep the `clientOrderId` reserved, mark the order state as `unknown/needs_reconcile`, and reconcile by `clientOrderId` before allowing another entry for the same strategy/account/symbol/side.
- Add a regression test beside `tests/test_order_manager_safety.py`.

### 3. API-key RBAC pepper is unset

Evidence:

- `config/settings.py:266-267` defaults `OPS_TOKEN` and `RBAC_SECRET` to empty strings.
- `core/governance/rbac.py:16-31` falls back to an empty HMAC pepper and only logs a warning.
- Current settings check: `settings_RBAC_SECRET_configured=False`, `env_RBAC_SECRET_configured=False`.

Impact:

API keys are hashed with HMAC, which is good, but an empty pepper weakens the protection if the `api_users` table is copied. This is acceptable only for local development with no API-key users; it is not acceptable for a live exposed control plane.

Recommendation:

- Require non-empty `RBAC_SECRET` when API-key auth is enabled or when `WEB_HOST` is not loopback.
- Surface this in `web.bat status` and pre-release checks.

## Medium Findings

### 4. Runtime data is tracked under an ignored directory

Evidence:

- `.gitignore:26-33` ignores `/data/`.
- `git ls-files data` still includes `data/ai_calibration/family_regime_priors.json`.
- The tracked file is currently modified by runtime calibration counters.

Impact:

Runtime state can drift into commits and create noisy or misleading diffs. In a trading system, calibration state should be intentionally versioned seed data or local runtime state, not both.

Recommendation:

- Move versioned seed priors to `config/` or `models/`.
- Keep live/runtime priors under ignored `data/`.
- If the current file is meant to be runtime-only, untrack it after preserving a template.

### 5. Local launcher edits are machine-specific

Evidence:

- `scripts/market_ws_live_shadow.ps1:15` and `scripts/market_ws_paper_shadow.ps1:11` now hard-code `F:\9_Crypto\.conda\...`.
- Git diff also shows both PowerShell files gained a UTF-8 BOM at `param(`.

Impact:

These scripts become non-portable across machines and can create line-ending / encoding churn in future commits.

Recommendation:

- Prefer `-EnvName`, `CONDA_PREFIX`, or repo-relative interpreter discovery.
- Avoid committing machine-specific absolute paths.

### 6. Default shell Python is wrong for this project

Evidence:

- `python --version` from the shell returns Python `3.13.7`.
- Project runtime script points to Python `3.11.15`.
- `pytest` is not available on the default shell PATH.

Impact:

Direct `python ...` or `pytest ...` commands can fail or use a different interpreter than the app. This can produce false audit results or silently create incompatible cache/runtime artifacts.

Recommendation:

- Keep using `F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe -m ...` for validation.
- Add a small `scripts/dev_python.ps1` or status check that prints the resolved interpreter and refuses unsupported versions.

## Low / Hygiene Findings

### 7. Ruff is not a calibrated quality gate

`ruff check .` found 361 default-rule issues. Because the repo has no Ruff config, these are not directly actionable as a current release blocker. Still, the import-order and unused-variable drift makes real findings harder to see.

Recommendation:

- Add a minimal Ruff config and enable a small stable rule set first.
- Start with `F`, `E7`, and targeted safety rules; avoid enabling broad style churn in the same pass.

## Positive Checks

- Full pytest is clean in this environment.
- Protected API endpoints reject unauthenticated requests (`401`) while `/health` remains public.
- CORS defaults are explicit loopback origins, not wildcard with credentials.
- Live/paper startup guard code exists and default managed startup still blocks persisted live restore unless explicitly allowed.
- Sensitive trading, order, strategy, AI, data-download, and notification mutation routes mostly use `require_sensitive_ops_permissions`.
- Circuit breaker and pre-trade risk paths are fail-closed for new entries while allowing reduce-only exits.

## Immediate Next Actions

1. Confirm whether current live runtime is intentional.
2. Fix the Binance futures ambiguous fast-path fallback before further unattended live runs.
3. Set `RBAC_SECRET` if API-key auth is used.
4. Decide whether `data/ai_calibration/family_regime_priors.json` is seed data or runtime data.
5. Convert hard-coded local Python paths in the shadow launchers to environment/repo-relative resolution.

## 2026-07-07 Follow-up: MA Strategy No-Entry Diagnosis

User-facing symptom: MA strategies appeared to run for a long time but did not open new orders.

Evidence before the web process warm reload:

- `execution_engine.signal_diagnostics.last_signal` showed `bt_ma_near_15m_032320_409` emitted a live `buy` signal for `NEAR/USDT` at `2026-07-07T07:45:00+00:00`, price `2.023`.
- The corresponding `last_result.status` was `circuit_breaker_blocked`, scope `portfolio`, reason `24h_dd 0.0342 >= 0.0300`, action `close_only`.
- The live risk report showed `risk_level=critical`, `trading_halted=true`, `fresh_entry_allowed=false`, `reduce_only=true`.
- The other visible MA instance, `MAStrategy_ai_paper_1783247448_5139`, was running in `runtime_mode=paper`, so it cannot place live orders.

Evidence after the warm reload:

- `.\web.bat status` reported Web PID `35396`, state `healthy`, mode `live`.
- Strategy restore reported `restored=2`, `started=2`.
- Risk status was back to `risk_level=low`, `trading_halted=false`, `fresh_entry_allowed=true`.
- The live execution diagnostic counters reset to zero after process restart.
- Monitor payloads still showed stale OHLCV: paper BTC latest bar `2026-07-07T03:00:00`, live NEAR latest bar `2026-07-06T10:45:00`.
- Logs still showed Binance live kline timeouts, now with escalating backoff: first `30s after 1 consecutive failure(s)`, then `60s after 2 consecutive failure(s)`.

Interpretation:

- The immediate reason the observed live MA signal did not become an order was the portfolio circuit breaker, not the MA strategy failing to signal.
- After restart/reload, the live MA strategy is allowed to enter again from a risk perspective, but it still needs a new MA crossing on a fresh completed 15m bar.
- The MA implementation is crossing-triggered, not state-triggered: it does not open simply because fast MA remains above slow MA.
- Binance kline freshness is the remaining operational blocker for both monitoring smoothness and new-signal confidence.

Implemented follow-up fixes:

- Strategy summary now has a short TTL cache with an explicit `fresh=1` bypass for write-after-refresh flows.
- Monitor payloads expose `entry_status` and `ohlcv_freshness`, so the UI can show paper-only, risk-blocked, and stale-data reasons directly.
- Per-strategy open-order monitor failures are negative-cached longer and live order checks use a shorter timeout.
- Strategy live kline fetch failures now use capped consecutive-failure backoff and clear the failure count after a healthy fetch.

Validation:

- `F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe -m pytest tests/test_strategy_runtime_mode_switch.py tests/test_strategy_manager_runtime_data.py tests/web/test_strategy_monitor_route.py tests/test_sensitive_api_auth.py -q`
- Result: `66 passed in 11.42s`.
- `node --check web/static/js/app.js` passed.
- `git diff --check` passed with only existing CRLF warnings.

## 2026-07-07 Follow-up: External Manual Position PnL Excluded From Strategy Halt

User-facing requirement: if a position was opened outside the strategy system, its floating loss must not put strategy execution into portfolio halt. Manual/external position risk should remain visible, but it should not block system strategy entries.

Implemented behavior:

- Live position snapshots now split total exchange unrealized PnL into `system_unrealized_pnl_usd` and `external_unrealized_pnl_usd`.
- The split is based on local system-owned strategy positions (`strategy` present, strategy name not prefixed with `manual`, and metadata source not `manual`, `external`, or `exchange_live`) matched to exchange positions by exchange, normalized symbol, side, and quantity. Partial matches are prorated.
- `risk_manager.update_equity(...)` receives only `system_unrealized_pnl_usd` in live mode, while total/external PnL remains visible in balance and risk payloads.
- `daily_stop_basis_usd` now uses `daily_realized_pnl_usd + current system floating loss`, not total account floating loss.
- Live portfolio circuit-breaker drawdown now uses system strategy trade history plus current system floating loss. Manual orders and external exchange-live positions are excluded from the portfolio breaker basis.
- The circuit-breaker monitor now passes `auto_clear_false_trips=True`, so persisted false trips can clear after recomputation under the corrected system-owned basis.

Validation:

- `F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe -m pytest tests/test_live_risk_pnl_accounting.py tests/test_circuit_breaker.py tests/test_circuit_breaker_pnl_trigger.py tests/test_trading_balance_routes.py tests/test_execution_circuit_breaker_integration.py -q`
- Result: `55 passed in 8.46s`.
- `F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe -m py_compile web/api/trading.py web/api/trading_balances.py core/risk/risk_manager.py core/risk/circuit_breaker.py web/main.py tests/test_live_risk_pnl_accounting.py tests/test_circuit_breaker.py` passed.
- `git diff --check` passed with only existing CRLF warnings.

Operational note:

- The live Web process was left running and was not manually restarted. The change takes effect after a controlled reload/restart of the live service.
