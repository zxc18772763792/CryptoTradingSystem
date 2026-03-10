# Changelog (Governance + AI Decision Integration)

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

