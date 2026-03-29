# Review Improvement Plan

Last updated: 2026-03-29

## Scope

This plan addresses the concrete issues found during the repository review on 2026-03-29 and turns them into an execution checklist with clear ownership boundaries, implementation notes, and acceptance criteria.

## Goals

1. Remove the live-trading governance bypass introduced by the AI research live activation flow.
2. Make OpenAI relay failover deterministic, testable, and resistant to cross-request state pollution.
3. Repair mojibake and encoding damage in critical AI prompts and user-facing metadata.
4. Eliminate the current front-end asset version regression and reduce the chance of repeating it.
5. Re-run focused validation so the changes are grounded in passing tests, not only code inspection.

## Workstreams

### 1. Governance And Live Activation

Problem:
- `POST /api/ai/candidates/{candidate_id}/activate-live` can push a candidate into live execution after only checking that the global trading mode is already `live`.
- The endpoint currently bypasses the stricter governance and confirmation paths already used elsewhere in the project.

Plan:
- Rework the endpoint so it honors governance settings on the server side.
- Require the candidate to be in an approved state that is consistent with the existing human-approval workflow.
- Reuse existing runtime-mode confirmation and/or approval state where possible rather than inventing a parallel path.
- Preserve audit logging, but make the audit trail reflect the stricter server-side checks.

Acceptance criteria:
- Governance-enabled mode does not allow a direct bypass from the AI research page to live execution.
- Live activation only succeeds when the candidate is in a state that the project already considers human-approved.
- New or updated tests cover both the allowed and denied paths.

Suggested write scope:
- `web/api/ai_research.py`
- `tests/test_ai_research_runtime_and_phase_e.py`

### 2. OpenAI Relay Failover

Problem:
- The current relay-priority cache can persist a failover preference globally, causing later requests and tests to start from the backup relay instead of the primary relay.
- That makes behavior harder to reason about and has already produced failing tests.

Plan:
- Refactor relay preference handling so failover is deterministic and isolated.
- Avoid long-lived global stickiness unless it has an explicit expiration strategy and a reset path.
- Keep request-local failover behavior intact: primary first, then backup when retryable/failover-worthy failures occur.
- Add a small state-reset hook if shared state remains necessary.

Acceptance criteria:
- The migration tests expecting `primary -> backup` ordering pass consistently.
- Failover state from one request or test cannot silently poison later requests.
- The utility remains compatible with current OpenAI relay call sites.

Suggested write scope:
- `core/utils/openai_responses.py`
- `core/ai/research_context_generator.py`
- `core/news/eventizer/llm_glm5.py`
- Related tests only if needed for better coverage

### 3. Encoding Repair

Problem:
- Critical research prompts and some metadata contain mojibake, which silently degrades model outputs and user-facing quality.

Plan:
- Restore readable UTF-8 text in critical prompts and metadata.
- Start with high-impact files used in runtime AI calls or exposed in API metadata.
- Prefer explicit literals that are easy to review and test.
- Add a small guardrail test if feasible so common mojibake fragments do not creep back into core prompt files.

Acceptance criteria:
- Critical prompts in the AI research context generator are readable Chinese/English text again.
- FastAPI app metadata no longer exposes mojibake in obvious fields.
- If a guard test is added, it passes and checks for known mojibake markers in the repaired file(s).

Suggested write scope:
- `core/ai/research_context_generator.py`
- `web/main.py`
- Optional tests for prompt integrity

### 4. Front-End Asset Versioning

Problem:
- `index.html` references `app.js?v=120`, while a regression test still expects `v=119`.
- Manual asset version bumps are brittle and easy to desynchronize.

Plan:
- Fix the immediate regression so the current test suite is green again.
- Reduce future drift by centralizing or simplifying asset version management where practical within the current stack.
- Keep the change minimal unless a larger refactor is clearly safe.

Acceptance criteria:
- The current UI asset test passes.
- The approach for asset version updates is at least internally consistent.

Suggested write scope:
- `web/templates/index.html`
- `tests/test_backtest_pairs_ui_assets.py`

### 5. Validation

Plan:
- Re-run focused tests first, then the broader suite if time permits.
- Prioritize:
  - `tests/test_openai_responses_migration.py`
  - `tests/test_backtest_pairs_ui_assets.py`
  - `tests/test_ai_research_runtime_and_phase_e.py`
- Run the full `pytest -q` pass after targeted fixes are stable.

Acceptance criteria:
- Previously failing tests pass.
- No obvious regressions are introduced in adjacent behavior.

## Parallel Execution Plan

Worker A:
- Governance and live-activation hardening.
- Owns `web/api/ai_research.py` and matching tests.

Worker B:
- Relay failover isolation and deterministic ordering.
- Owns `core/utils/openai_responses.py` and directly related call sites/tests.

Worker C:
- Encoding repair and the front-end asset/test cleanup.
- Owns `core/ai/research_context_generator.py`, `web/main.py`, `web/templates/index.html`, and the affected UI test.

Main thread:
- Create this plan.
- Coordinate agents, review returned changes, resolve any overlap, and run validation.

## Notes

- The repository already has unrelated local modifications. Each workstream should preserve existing changes and avoid reverting user edits.
- Where possible, prefer tightening server-side invariants over adding more client-side guidance.
