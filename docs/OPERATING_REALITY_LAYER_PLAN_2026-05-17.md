# Operating Reality Layer Execution Plan

Date: 2026-05-17

This plan turns the cross-page design review into an additive operating layer
over AI Research, Altcoin Radar, the research workbench, and the autonomous
agent. The implementation goal is not to rewrite those pages. The goal is to
make the system explain what it is doing, expose degraded modes, connect the
natural workflow funnel, and create the first hooks for calibration and audit.

## Operating Principles

- All contracts are additive. Keep legacy fields such as `reasons[]`,
  existing `/api/ai/*`, `/api/research/*`, and `/api/altcoin/*` responses.
- Do not change default live behavior in M1-M3. Market-state risk posture may
  enforce in paper/shadow; live enforcement requires
  `AI_MARKET_STATE_RISK_POSTURE_LIVE_ENFORCE=true`.
- Optional source failures are degraded state, not silent no-op and not a hard
  crash unless the caller explicitly requests hard failure.
- Prefer metadata, JSONL, and companion fields over schema-breaking database
  migrations.
- Raw existing scores stay in place. New calibrated fields must label their
  source and can be absent with an explicit `uncalibrated` explanation.

## Problem Coverage Matrix

| Problem | Local execution target |
| --- | --- |
| Decision gates are not attributable | Add `DecisionTrace` / `DecisionGateTrace`; validation, aggregation, and autonomous diagnostics expose root blocker and gate ladder. |
| OOS silently replaces IS Sharpe | Validation summary exposes `effective_sharpe_source`; UI shows OOS as the active Sharpe when present. |
| Three disconnected market views | Add `MarketStateSnapshot`; research regime, planner hints, and aggregator context share one schema/resolver. |
| Pages are isolated dashboards | Add Altcoin Radar -> research proposal, candidate -> autonomy watch handoff, and `/api/ai/work-queue`. |
| Silent degraded mode | Add `OperatingModeSnapshot` and visible banners on AI Research, AI Agent, and Altcoin Radar. |
| Hard thresholds without uncertainty | Add hysteresis/borderline helpers and expose `uncertainty`, `classification_margin`, `hysteresis_state`. |
| Correlation reject loses semantics | Mark `outcome_type=redundant_correlated`, `reserve_eligible=true`, and keep correlation metadata. |
| Approval concepts are scattered | Surface approval/live blockers as `WorkQueueItem` types without merging underlying flows yet. |
| Feedback loop is open | Add `PerformanceDivergenceReport` and local advisory prior file. |
| Gates are not falsifiable | Add JSONL counterfactual audit and summary endpoint. |
| Regime is display-only | Wire `risk_posture` into paper/shadow autonomous risk posture, live advisory by default. |

## M1: Decision Attribution

Files:

- `core/observability/decision_trace.py`
- `core/ai/proposal_schemas.py`
- `core/research/validation_gate.py`
- `core/research/orchestrator.py`
- `core/ai/signal_aggregator.py`
- `core/ai/autonomous_agent.py`
- `web/static/js/ai_research.js`
- `web/static/js/ai_research_agent.js`

Tasks:

- Define `DecisionTrace`, `DecisionGateTrace`, `GateStatus`, `append_gate()`,
  and `build_root_blocker()`.
- Add `decision_trace`, `effective_sharpe_source`, `outcome_type`,
  `reserve_eligible`, `calibrated_confidence`, `expected_edge_bps`,
  `risk_adjusted_edge`, and `score_explanation` to
  `ProposalValidationSummary`.
- Validation gates must append gates for deployment tier, OOS/IS Sharpe source,
  no-OOS live cap, DSR, trade count, and correlation redundancy.
- Root blocker priority: block/reject gates before downgrade gates before
  warning/degraded gates.
- Signal aggregation must expose component gates for LLM, ML, factor,
  derivatives, and risk/approval. Derivatives shadow-only keeps configured
  weight but exposes effective weight zero.
- Autonomous diagnostics must map model error, no price, confidence, cooldown,
  review cooldown, risk halt, risk gate, shadow/live blockers, and submit
  rejection into the same gate ladder.
- UI must show root blocker and expandable gate ladder before legacy reasons.

Acceptance:

- OOS-present validation reports `effective_sharpe_source=oos`.
- No-OOS live candidate is capped and root blocker is `no_oos_live_cap`.
- DSR and trade count produce distinct root blockers.
- Correlation reject enters reserve semantics instead of generic reject only.

## M2: Market State, Freshness, Hysteresis

Files:

- `core/market_state/schema.py`
- `core/market_state/classifier.py`
- `core/market_state/hysteresis.py`
- `core/market_state/planner_adapter.py`
- `web/api/research.py`
- `core/ai/research_planner.py`
- `core/ai/signal_aggregator.py`

Tasks:

- Define `MarketStateSnapshot` with `snapshot_id`, `symbol`, `exchange`,
  `as_of`, `regime`, `bias`, `confidence`, `uncertainty`, `risk_posture`,
  `component_votes`, `conflicts`, and `data_manifest`.
- Define `DataManifestEntry` with `source`, `scope`, `as_of`, `age_seconds`,
  `ttl_seconds`, `freshness`, and `degraded_reason`.
- Move regime/calendar classification to reusable classifier helpers.
- Keep `/workbench/regime-calendar`, but include `uncertainty`,
  `classification_margin`, `hysteresis_state`, and `data_manifest`.
- Add `GET /api/research/market-state`.
- Planner consumes `market_state_snapshot` first. Legacy parser is fallback and
  records `legacy_market_context_fallback`.
- Aggregator accepts `market_state_snapshot=None` and includes the shared
  `snapshot_id`; factor/derivatives become component votes, not a third regime.
- Scope rules: `symbol` can directly boost/suppress the symbol; `benchmark`
  explains and affects direct boosts only with explicit beta/sector rule;
  `global` affects macro risk posture only.

Acceptance:

- Borderline regime values are surfaced as uncertain/borderline.
- Altcoins are not directly boosted/suppressed by BTC benchmark context unless
  metadata makes the linkage explicit.

## M3: Operating Mode Truth Source

Files:

- `core/runtime/operating_mode.py`
- `config/settings.py`
- `web/api/ai_research.py`
- `web/static/js/ai_research.js`
- `web/static/js/ai_research_agent.js`
- `web/static/js/altcoin_radar.js`

Tasks:

- Define `OperatingModeSnapshot` and `validate_operating_mode()`.
- Add `GET /api/ai/operating-mode` as the authoritative UI source.
- Aggregate trading mode, live-decision config, autonomous config, CoinGlass
  live gating, provider catalog, runtime state, source health, and market-state
  posture settings.
- Degradations must include provider fallback, missing provider key,
  fail-open, `allow_live=false`, live-mode/agent-live mismatch, derivatives
  shadow-only, stale/missing source, no-OOS live cap policy, and live advisory
  risk posture.
- Add visible operating banners to AI Research, AI Agent, and Altcoin Radar.
- Add defaults:
  - `AI_MARKET_STATE_RISK_POSTURE_ENABLED=true`
  - `AI_MARKET_STATE_RISK_POSTURE_LIVE_ENFORCE=false`

Acceptance:

- Provider fallback appears in operating-mode degradations.
- Derivatives shadow-only appears with an explicit degraded/info state.
- Paper/shadow applies defensive/halt posture; live remains advisory unless
  explicitly enabled.

## M4: Workflow Funnel and Work Queue

Files:

- `core/research/altcoin_radar.py`
- `web/api/altcoin.py`
- `web/api/ai_research.py`
- `web/static/js/altcoin_radar.js`
- `web/static/js/ai_research.js`

Tasks:

- Add Altcoin Radar `priority` lens with `priority_score` and
  `next_best_action`; keep older sort modes.
- Add `POST /api/altcoin/radar/{symbol}/research-proposal`.
- Add `POST /api/ai/candidates/{candidate_id}/autonomy-handoff`; first version
  only records watch/handoff metadata and returns next actions.
- Add `GET /api/ai/work-queue` with item types:
  `radar_opportunity`, `research_draft`, `validation_blocked`,
  `reserve_candidate`, `human_approval`, `live_activation`,
  `runtime_degradation`, and `agent_hold_root_blocker`.
- UI adds row-level "generate research proposal", candidate "send to autonomy
  watch", and a work queue panel sorted by severity, risk-adjusted edge, and
  creation time.

Acceptance:

- Altcoin Radar can create a hybrid research draft with origin metadata.
- Candidate detail can request autonomy watch without direct live execution.
- Work queue surfaces approval, runtime, reserve, and root-blocker work.

## M5: Feedback, Reserve, Calibration

Files:

- `core/research/performance_feedback.py`
- `core/observability/score_calibration.py`
- `web/api/ai_research.py`
- `core/monitoring/cusum_watcher.py` (follow-up integration)
- `data/ai_calibration/family_regime_priors.json`

Tasks:

- Define `PerformanceDivergenceReport` with statuses `aligned`,
  `underperforming`, `overfit_suspect`, `insufficient_sample`, and `decayed`.
- Add `GET /api/ai/candidates/{candidate_id}/performance-divergence`.
- Add `GET /api/ai/reserve-candidates`.
- Create local advisory prior file keyed by
  `strategy_family + regime + symbol_scope`; first version is read/display only.
- Add companion score helpers for calibrated confidence, expected edge bps, and
  risk-adjusted edge.

Acceptance:

- Paper/live Sharpe far below validation expectation yields `overfit_suspect`.
- Redundant-but-good candidates remain discoverable as reserve candidates.

## M6: Counterfactual Audit

Files:

- `core/audit/gate_counterfactuals.py`
- `web/api/ai_research.py`
- `data/audit/gate_counterfactuals.jsonl` (runtime output, ignored by git)

Tasks:

- Record each block/downgrade/hold as JSONL with `trace_id`, subject, gate,
  observed decision, counterfactual decision, input summary, as-of timestamp,
  mode, and later outcome ref.
- Add `GET /api/ai/gate-audit/summary` for gate hit rate, downgrade rate,
  missed-alpha proxy, and avoided-loss proxy.
- First version is statistical/advisory only. It must not auto-change
  thresholds.

Acceptance:

- Validation and autonomous hold paths can write counterfactual rows.
- Summary endpoint returns stable aggregate counters when the JSONL exists or
  an empty summary when it does not.

## Verification Commands

Run focused tests first:

```bash
pytest tests/test_operating_reality_layer.py tests/test_research_validation_gate.py tests/test_signal_aggregator_fear_greed.py tests/test_research_market_state.py tests/test_ai_research_autonomous_agent_api.py tests/web/test_altcoin_route.py -q
```

Then run UI asset and runtime coverage:

```bash
pytest tests/test_altcoin_radar_ui_assets.py tests/test_ai_research_runtime_and_phase_e.py tests/test_research_workbench_ui_assets.py tests/test_research_workbench_recommendations_api.py tests/test_ai_research_phase4_runtime.py tests/test_ai_research_phase5_ui_assets.py -q
```

Run autonomous and phase2 regression suites:

```bash
pytest tests/test_ai_autonomous_agent.py -q
pytest tests/test_ai_research_phase2.py -q
```

Run static checks:

```bash
python -m compileall core/observability core/market_state core/runtime/operating_mode.py core/audit/gate_counterfactuals.py core/research/performance_feedback.py core/research/validation_gate.py core/research/orchestrator.py core/research/altcoin_radar.py core/ai/signal_aggregator.py core/ai/autonomous_agent.py core/ai/live_decision_router.py web/api/research.py web/api/ai_research.py web/api/altcoin.py
node --check web/static/js/ai_research.js
node --check web/static/js/ai_research_agent.js
node --check web/static/js/altcoin_radar.js
```
