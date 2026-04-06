# Changelog (Governance + AI Decision Integration)

## 2026-04-07

### Changed

- Startup entrypoints:
  - `web.bat` is now the only user-facing startup/control script
  - running `.\web.bat` with no args now performs one-click startup with browser
  - the managed default startup profile now starts web + news worker + news LLM worker
  - removed the extra top-level wrappers `start_web_oneclick.bat` and `start_once.bat` to reduce operator confusion.
- `web/main.py` and `_once.ps1`:
  - news LLM startup now has an internal fallback when the service is started directly
  - managed startup explicitly marks external-news-LLM-only mode when it launches the dedicated external LLM worker.
- `STARTUP.md`, `README.md`, and `docs/REPOSITORY_OVERVIEW.md`:
  - refreshed the startup documentation around the single-script workflow and default news-engine boot behavior.

## 2026-04-01

### Added

- `docs/STARTUP_STABILIZATION_PLAN_2026-04-01.md`: startup recovery plan, work tracks, and acceptance criteria captured during the stabilization pass.

### Changed

- `web.bat`, `scripts/web.ps1`, `scripts/start_web_ps.ps1`, and `_once.ps1`:
  - default startup now uses a safe web-only profile
  - `.env` `START_*` worker toggles are no longer auto-applied during default startup
  - analytics-history is disabled by default unless `-EnableAnalyticsHistory` is passed
  - status output now reports observed worker processes and startup policy more clearly.
- `web/main.py` and `core/realtime/event_bus.py`:
  - runtime websocket fanout now skips heavy snapshot/ticker/news preview work when there are no subscribers.
- `STARTUP.md`, `README.md`, and `docs/REPOSITORY_OVERVIEW.md`:
  - refreshed startup guidance to match the current safe-start behavior and mode verification workflow.

## 2026-03-28

### Added

- `SECURITY.md`: repository-specific secret handling, pre-push checks, and leak response guidance.
- `docs/REPOSITORY_OVERVIEW.md`: source tree walkthrough and runtime-vs-tracked file boundaries.

### Changed

- `README.md`: refreshed project overview, quick start, documentation links, and GitHub publishing guidance.
- `.gitignore`: expanded local secret and certificate patterns while keeping `.env.example` tracked.

## 2026-03-06

### Added

- `docs/INTEGRATION.md`: integration survey, insertion points, rollback strategy.
- `docs/GOVERNANCE.md`: role model, strategy/risk gate flows, emergency operations.
- Governance package:
  - `core/governance/rbac.py`
  - `core/governance/schemas.py`
  - `core/governance/service.py`
  - `core/governance/audit.py`
  - `core/governance/decision_engine.py`
- New DB models in `config/database.py`:
  - `api_users`
  - `strategy_specs`
  - `strategy_approvals`
  - `risk_configs`
  - `risk_change_requests`
  - `audit_records`

### Changed

- `core/ops/service/auth.py`:
  - added `X-API-KEY` RBAC auth path
  - kept `X-OPS-TOKEN` compatibility.
- `core/ops/service/api.py`:
  - added governance endpoints:
    - strategy propose/approve/promote/request-live/approve-live/retire/list
    - risk current/request-change/approve-change/reduce-only/kill-switch/list
    - governance audit query
    - API user upsert/list
- `core/trading/execution_engine.py`:
  - inserted decision+risk governance checks before order creation.
  - emits governance audit for rejected and executed intents.
- `core/trading/order_manager.py`:
  - fallback governance check before real exchange order submission.
- `core/ai/research_planner.py`:
  - accepts `llm_research_output` with strict schema validation.
- `core/research/orchestrator.py`:
  - creates governance `StrategySpec` proposals from AI research candidates.
  - with `GOVERNANCE_ENABLED=true`, runtime promotion is held for human gate.

### New Config

- `GOVERNANCE_ENABLED=true`
- `DECISION_MODE=shadow|paper|live`
- `REQUIRE_DUAL_APPROVAL_FOR_LIVE=true`
- `AUDIT_LEVEL=full|minimal`

