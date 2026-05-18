# Operating Reality Layer — Continuation: Loop Closure

Date: 2026-05-17 (continuation of `OPERATING_REALITY_LAYER_PLAN_2026-05-17.md`)

The M1-M6 layer is implemented and green (244 tests). It *instruments* the
feedback loop but does not *close* it:

- `data/ai_calibration/family_regime_priors.json` is permanently `{}` — there
  is no writer. The planner reads it as advisory, so the advisory is vacuous.
- `core/monitoring/cusum_watcher.py` has no integration with
  `performance_feedback`, `gate_counterfactuals`, or the prior file. The
  original plan explicitly deferred this as "follow-up integration".
- `PerformanceDivergenceReport` documents a `decayed` status that no code path
  can produce (only 4 of 5 statuses are reachable).
- `/api/ai/operating-mode` does not report that its own self-calibration is
  cold, violating the layer's own "no silent degradation" principle.

## Operating Principles (unchanged)

- Strictly additive. No schema-breaking DB migrations. Advisory only — priors
  never hard-gate promotion or live behavior.
- Every new integration is best-effort and must never break the CUSUM watcher
  or any request path.
- Do not commit; leave the in-progress public-derivatives fallback intact.

## CL1 — Prior writer (close the calibration loop)

Files: `core/observability/score_calibration.py`,
`tests/test_operating_reality_layer.py`.

- Add `update_family_regime_priors(updates, *, path=None)` where each update is
  `{strategy_family, regime, symbol_scope, divergence_score, status,
  realized_sharpe, expected_sharpe}`.
- Key = `f"{family}|{regime}|{scope}"`. Maintain per-key EWMA (alpha=0.3) of
  `sharpe_gap`, a `sample_size` counter, `status_counts`, and
  `recent_decay_rate = decayed_count / sample_size`.
- Atomic write (`.tmp` → `os.replace`), preserve `schema_version`, set
  `updated_at`. Bounded: clamp stored floats, cap `sample_size` history.
- Acceptance: after one update, `get_family_regime_prior(...)` returns
  `available=True` with `sample_size`, `recent_decay_rate`, `sharpe_gap_ewma`.

## CL2 — Make `decayed` reachable

Files: `core/research/performance_feedback.py`,
`tests/test_operating_reality_layer.py`.

- `build_performance_divergence_report(..., decay_state: dict | None = None)`.
- If `decay_state` is truthy and `decay_state.get("triggered")`, status =
  `decayed` (takes priority over underperforming/overfit), append a note with
  `decay_pct`.
- Backward compatible: default `None` keeps current behavior.
- Acceptance: triggered decay_state → `status == "decayed"`.

## CL3 — CUSUM watcher integration (the deferred follow-up)

Files: `core/monitoring/cusum_watcher.py`,
`tests/test_operating_reality_layer.py`.

- Add `_record_decay_feedback(app, candidate, decay_result, new_status)`:
  1. Build a `PerformanceDivergenceReport` with `decay_state=decay_result`
     (status `decayed`).
  2. `record_gate_counterfactual(trace=..., observed_decision=new_status,
     mode="paper_or_shadow")` with a minimal CUSUM trace
     (`gate_code="cusum_decay"`, `counterfactual_decision="hold"`).
  3. `update_family_regime_priors([...])` from the report + candidate metadata
     (`strategy_family`, regime, `symbol_scope_for_symbol(candidate.symbol)`).
- Call it from `run_cusum_checks_for_all_candidates` right after
  `_demote_on_decay`, fully wrapped in try/except (never fatal).
- Acceptance: a decayed candidate run produces (a) a JSONL counterfactual row,
  (b) a non-empty prior entry, (c) report status `decayed`.

## CL4 — Operating-mode honesty about calibration maturity

Files: `web/api/ai_research.py` (operating-mode builder),
`tests/test_operating_reality_layer.py`.

- Add degradations: `feedback_priors_cold` (priors empty or `updated_at` null)
  and `cusum_feedback_inactive` (no counterfactual rows / priors never written).
- Severity: `info` (advisory maturity), not `error`.
- Acceptance: empty prior file → `feedback_priors_cold` present in degradations.

## CL5 — Integration safety for in-progress public-derivatives fallback

Files: none (verification only).

- `web/api/trading.py` + `tests/web/test_trading_microstructure_payload.py`
  are uncommitted and green (9 passed). Verify they still pass alongside CL1-CL4
  and that operating-mode/data-manifest can see the `degraded_reason`. Do not
  commit.

## CL6 — Regression + doc

- Run the focused + regression sweeps from the parent plan plus
  `tests/test_operating_reality_layer.py`.
- Append a completion note to this file.

## Verification

```bash
pytest tests/test_operating_reality_layer.py tests/test_research_validation_gate.py \
  tests/test_signal_aggregator_fear_greed.py tests/test_research_market_state.py \
  tests/test_ai_research_autonomous_agent_api.py tests/web/test_altcoin_route.py \
  tests/test_ai_autonomous_agent.py tests/test_ai_research_phase2.py \
  tests/web/test_trading_microstructure_payload.py -q
```

## Completion Note (2026-05-17)

Status: COMPLETE. Loop closed.

- CL1 — `update_family_regime_priors()` added to
  `core/observability/score_calibration.py` (EWMA alpha=0.3, bounded sample,
  atomic temp-file replace). Planner advisory is now non-vacuous once decay
  feedback has been folded in.
- CL2 — `build_performance_divergence_report(..., decay_state=)` added; the
  `decayed` status (previously unreachable) is now produced on both the
  snapshot and no-snapshot paths and takes priority over divergence status.
- CL3 — `_record_decay_feedback()` added to `core/monitoring/cusum_watcher.py`
  and invoked after `_demote_on_decay`. On decay it now (a) builds a `decayed`
  divergence report, (b) appends a `cusum_decay` counterfactual JSONL row
  (observed=demote, counterfactual=hold), (c) folds the divergence into the
  advisory prior file. Fully best-effort; cannot break the watcher.
- CL4 — `validate_operating_mode()` now emits `feedback_priors_cold` and
  `cusum_feedback_inactive` (severity `info`) so the system is honest that its
  own self-calibration may be cold.
- CL5 — verified the uncommitted public-derivatives fallback in
  `web/api/trading.py` + its 9-test suite still pass alongside the loop-closure
  changes. Left uncommitted as instructed.
- CL6 — regression: 289 passed across the operating-reality, validation,
  autonomous, live-decision, microstructure, and phase suites; 0 failures.

New tests in `tests/test_operating_reality_layer.py`:
`test_update_family_regime_priors_closes_the_loop`,
`test_performance_divergence_decayed_status_is_reachable`,
`test_operating_mode_reports_cold_calibration`,
`test_cusum_record_decay_feedback_closes_loop`.

Not done by design (advisory-only principle preserved): priors still never
hard-gate promotion or live behavior; the counterfactual audit remains
statistical/advisory and does not auto-tune thresholds.
