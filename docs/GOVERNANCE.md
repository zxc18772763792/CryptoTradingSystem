# Governance Guide

## Overview

This project now supports governance-controlled AI research and execution with:

- RBAC roles (`RESEARCH_LEAD`, `RISK_OWNER`, `OPERATOR`, `AUDITOR`, `ENGINEER`)
- Strategy lifecycle gate (`proposed -> approved -> paper -> live -> retired`)
- Risk config gate (versioned config + change request + approval for risk increases)
- Hard runtime risk controls in two places:
  - pre-order intent check in execution engine
  - pre-exchange-submit fallback check in order manager
- End-to-end governance audit records (`audit_records`) with `trace_id`, input/output hashes.

## Roles and Permissions

- `RESEARCH_LEAD`: propose/approve strategy, promote to paper, request live, retire.
- `RISK_OWNER`: approve risk changes, approve live, set kill switch/reduce-only, leverage caps.
- `OPERATOR`: pause/resume engine, set reduce-only (risk-reducing path).
- `AUDITOR`: query/export audit.
- `ENGINEER`: migrations/config/data-source management.

Legacy `X-OPS-TOKEN` auth remains supported for compatibility and maps to `SYSTEM` role.

## Strategy Gate Flow

1. AI research produces candidate and creates governance `StrategySpec` in `proposed`.
2. `RESEARCH_LEAD` approves: `proposed -> approved`.
3. `RESEARCH_LEAD` promotes to paper: `approved -> paper`.
4. Live transition:
  - default `REQUIRE_DUAL_APPROVAL_FOR_LIVE=true`
  - requires both `RESEARCH_LEAD` and `RISK_OWNER` approvals (`paper -> live`).
5. `RESEARCH_LEAD` or `RISK_OWNER` can retire: `paper/live -> retired`.

## Risk Gate Flow

Risk config is versioned in `risk_configs`.

Change process:

1. Submit `risk_change_request` with proposed config and reason.
2. System computes `risk_delta_score`.
3. If change increases risk, request remains `pending` and must be approved by `RISK_OWNER`.
4. Non-increasing risk changes can be auto-applied (operator/system path).

Increasing-risk examples:

- raise `max_leverage`
- raise `max_position_notional_pct`
- raise `max_trade_risk_pct`
- relax `max_daily_drawdown_pct`
- relax `spread_limit_bps`
- relax `data_staleness_limit_ms`
- expand allowed symbols/timeframes.

## Emergency Operations

### Kill switch

- API: `POST /ops/governance/risk/kill_switch` with `{ "enabled": true }`
- Effect: new order intents are blocked by decision gate.

### Reduce-only

- API: `POST /ops/governance/risk/set_reduce_only` with `{ "enabled": true }`
- Effect: opening trades blocked; only reducing/closing actions pass.

### Hard stop fallback

Existing endpoint `POST /ops/trading/kill_switch` still available to stop engine and flatten positions.

## Rollback

To return to legacy behavior:

1. Set `GOVERNANCE_ENABLED=false`.
2. Keep `DECISION_MODE=shadow`.
3. Restart service.

This disables governance gate enforcement while keeping data intact for audit.

