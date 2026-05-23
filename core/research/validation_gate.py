"""Validation scoring and promotion recommendation for AI research runs."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

from scipy.stats import norm as _spnorm
import math as _math

from config.settings import settings
from core.ai.proposal_schemas import ProposalValidationSummary
from core.observability.decision_trace import DecisionTrace, append_gate
from core.observability.score_calibration import (
    get_family_regime_prior,
    symbol_scope_for_symbol,
    validation_companion_scores,
)
from core.research.experiment_schemas import PromotionDecision

_MIN_TRADES_FOR_SHADOW = 1
_MIN_TRADES_FOR_PAPER = 10
_MIN_TRADES_FOR_LIVE_CANDIDATE = 30

# Deflated Sharpe Ratio gating thresholds (Bailey & López de Prado, 2014).
# Below _DSR_REJECT_BELOW the Sharpe is almost certainly a multiple-testing
# artifact → reject. In [_DSR_REJECT_BELOW, _DSR_DOWNGRADE_OK) the edge is weak
# → downgrade one tier. At or above _DSR_DOWNGRADE_OK no DSR adjustment.
_DSR_REJECT_BELOW = 0.4
_DSR_DOWNGRADE_OK = 0.65


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _clip_score(value: float) -> float:
    return round(max(0.0, min(100.0, float(value))), 2)


def _score_ratio(value: float, good_at: float) -> float:
    good = max(float(good_at), 1e-9)
    return _clip_score(float(value) / good * 100.0)


def _inverse_score(value: float, bad_at: float) -> float:
    bad = max(float(bad_at), 1e-9)
    return _clip_score(100.0 - (float(value) / bad * 100.0))


def _deflated_sharpe_ratio(
    sharpe: float,
    n_trials: int,
    n_obs: int,
    skewness: float = 0.0,
    kurtosis: float = 3.0,
) -> float:
    """Bailey & Lopez de Prado (2014) Deflated Sharpe Ratio.

    Corrects Sharpe for multiple testing bias when N strategies were screened.
    Returns P(SR > 0 | data) adjusted for the expected max SR under H0.
    Range: [0, 1]. Below 0.5 → likely spurious.
    """
    if n_trials <= 1 or n_obs <= 1:
        return float(_spnorm.cdf(float(sharpe)))

    # Expected max of n_trials iid standard normals (Gumbel approximation)
    euler_gamma = 0.5772156649
    z1 = float(_spnorm.ppf(1.0 - 1.0 / max(n_trials, 2)))
    z2 = float(_spnorm.ppf(1.0 - 1.0 / (max(n_trials, 2) * _math.e)))
    e_max_sr = (1.0 - euler_gamma) * z1 + euler_gamma * z2

    # Sharpe adjusted for non-normality (skewness/kurtosis correction)
    sr = float(sharpe)
    n = max(int(n_obs), 2)
    adj = 1.0 - skewness * sr + (kurtosis - 3.0) / 4.0 * sr ** 2
    adj = max(adj, 0.01)  # guard against negative
    adj_sr = sr * _math.sqrt(n - 1) / _math.sqrt(n) * _math.sqrt(adj)

    # Standard error of the Sharpe estimate
    sr_hat_std = _math.sqrt((1.0 + 0.5 * adj_sr ** 2) / max(n - 1, 1))
    if sr_hat_std <= 0:
        return 1.0

    z = (adj_sr - e_max_sr) / sr_hat_std
    dsr = float(_spnorm.cdf(z))
    return max(0.0, min(1.0, dsr))


def build_validation_summary_from_research_result(result: Dict[str, Any]) -> ProposalValidationSummary:
    now = _now_utc()
    runs = max(0, int(result.get("runs", 0) or 0))
    valid_runs = max(0, int(result.get("valid_runs", 0) or 0))
    best = dict(result.get("best") or {})
    quality_counts = dict(result.get("quality_counts") or {})
    quality_ok = int(quality_counts.get("ok", 0) or 0)

    if not best or valid_runs <= 0:
        trace = DecisionTrace(
            subject_type="research_result",
            subject_id=str(result.get("candidate_id") or result.get("proposal_id") or ""),
            stage="validation_gate",
            final_decision="reject",
            effective_score=0.0,
            input_summary={"runs": runs, "valid_runs": valid_runs, "quality_counts": quality_counts},
        )
        append_gate(
            trace,
            code="no_valid_research_runs",
            label="No valid research runs",
            status="block",
            severity=5,
            input_value=valid_runs,
            threshold=">0",
            decision_before="reject",
            decision_after="reject",
            reason="no valid research runs",
            source="validation_gate",
        )
        return ProposalValidationSummary(
            computed_at=now,
            decision="reject",
            edge_score=0.0,
            risk_score=0.0,
            stability_score=0.0,
            efficiency_score=0.0,
            deployment_score=0.0,
            reasons=["no valid research runs"],
            metrics={
                "runs": runs,
                "valid_runs": valid_runs,
                "quality_counts": quality_counts,
            },
            decision_trace=trace.to_dict(),
            effective_sharpe_source="none",
            outcome_type="insufficient_sample",
            reserve_eligible=False,
            calibrated_confidence=0.0,
            expected_edge_bps=0.0,
            risk_adjusted_edge=0.0,
            score_explanation="validation rejected before scoring because no valid runs were available",
        )

    total_return = float(best.get("total_return", 0.0) or 0.0)
    gross_total_return = float(best.get("gross_total_return", total_return) or total_return)
    sharpe_ratio = float(best.get("sharpe_ratio", 0.0) or 0.0)
    max_drawdown = float(best.get("max_drawdown", 0.0) or 0.0)
    win_rate = float(best.get("win_rate", 0.0) or 0.0)
    total_trades = float(best.get("total_trades", 0.0) or 0.0)
    anomaly_ratio = float(best.get("anomaly_bar_ratio", 0.0) or 0.0)
    cost_drag = abs(float(best.get("cost_drag_return_pct", 0.0) or 0.0))
    valid_ratio = (valid_runs / max(runs, 1)) * 100.0
    ok_ratio = (quality_ok / max(runs, 1)) * 100.0 if runs else 0.0

    # C: Extract IS/OOS/WF metrics from best result
    raw_is_sharpe = best.get("is_sharpe")
    raw_oos_sharpe = best.get("oos_sharpe")
    raw_wf_stability = best.get("wf_stability")
    is_sharpe = float(raw_is_sharpe) if raw_is_sharpe is not None else None
    oos_sharpe = float(raw_oos_sharpe) if raw_oos_sharpe is not None else None
    wf_stability = float(raw_wf_stability) if raw_wf_stability is not None else None

    raw_wf_consistency = best.get("wf_consistency")
    wf_consistency = float(raw_wf_consistency) if raw_wf_consistency is not None else None

    # C: Use OOS Sharpe for edge scoring when available
    effective_sharpe = oos_sharpe if oos_sharpe is not None else sharpe_ratio
    effective_sharpe_source = "oos" if oos_sharpe is not None else "in_sample"
    trace = DecisionTrace(
        subject_type="research_result",
        subject_id=str(result.get("candidate_id") or result.get("proposal_id") or best.get("strategy") or ""),
        stage="validation_gate",
        input_summary={
            "runs": runs,
            "valid_runs": valid_runs,
            "effective_sharpe": effective_sharpe,
            "effective_sharpe_source": effective_sharpe_source,
            "total_trades": total_trades,
        },
    )
    append_gate(
        trace,
        code="effective_sharpe_source",
        label="Effective Sharpe source",
        status="pass" if oos_sharpe is not None else "warn",
        severity=1 if oos_sharpe is None else 0,
        input_value=effective_sharpe,
        threshold={"source": effective_sharpe_source, "oos_required_for_live": True},
        decision_before="score",
        decision_after="score",
        reason=(
            "OOS Sharpe is used for edge scoring; IS Sharpe is only context"
            if oos_sharpe is not None
            else "No OOS Sharpe is available; in-sample Sharpe is used and live promotion is capped"
        ),
        source="validation_gate",
        metadata={"is_sharpe": is_sharpe, "oos_sharpe": oos_sharpe, "raw_sharpe": sharpe_ratio},
    )

    # FIX (P1-DSR effective): DSR previously hard-coded skew=0/kurt=3 (Gaussian) and
    # ignored hyper-parameter search depth, making the deflation ineffective for
    # heavy-tailed crypto returns and parameter-tuned strategies. Now:
    #   - n_trials multiplies by `optimization_trials` (how many param combos were
    #     screened on IS) — this is the real multiple-testing surface.
    #   - skewness / excess-kurtosis are estimated from the equity_curve_sample
    #     (50-pt) when available; fall back to a small heavy-tail prior (skew=-0.2,
    #     kurt=5.0) typical for crypto bar returns.
    runs_count = max(1, runs)
    opt_trials = int(best.get("optimization_trials", 0) or 0)
    # Effective multiple-testing N = runs * trials_per_run (avoid 0; min 1)
    n_trials_for_dsr = max(1, runs_count * max(1, opt_trials))
    _n_bars = int(best.get("n_bars", 0) or 0)
    _n_trades = int(best.get("total_trades", 10) or 10)
    n_obs_for_dsr = max(50, _n_bars if _n_bars > 0 else _n_trades * 5)

    skew_est, kurt_est = -0.2, 5.0
    try:
        eqc = best.get("equity_curve_sample") or []
        if isinstance(eqc, (list, tuple)) and len(eqc) >= 5:
            import numpy as _np
            _eq = _np.asarray([float(v) for v in eqc], dtype=float)
            _eq = _eq[_np.isfinite(_eq) & (_eq > 0)]
            if _eq.size >= 3:
                _rets = _np.diff(_eq) / _eq[:-1]
                _rets = _rets[_np.isfinite(_rets)]
                if _rets.size >= 3 and float(_np.std(_rets)) > 0:
                    _m = float(_np.mean(_rets))
                    _s = float(_np.std(_rets))
                    if _s > 0:
                        z = (_rets - _m) / _s
                        skew_est = float(_np.mean(z ** 3))
                        kurt_est = float(_np.mean(z ** 4))  # raw kurt; DSR func uses raw
    except Exception:
        skew_est, kurt_est = -0.2, 5.0  # heavy-tail prior typical for crypto

    dsr = _deflated_sharpe_ratio(
        sharpe=effective_sharpe,
        n_trials=n_trials_for_dsr,
        n_obs=n_obs_for_dsr,
        skewness=skew_est,
        kurtosis=kurt_est,
    )

    return_score = _score_ratio(max(total_return, 0.0), 25.0)
    sharpe_score = _score_ratio(max(effective_sharpe, 0.0), 2.0)
    win_score = _clip_score(win_rate)
    # Short-term trading: Sharpe matters more than raw return
    edge_score = _clip_score(return_score * 0.25 + sharpe_score * 0.55 + win_score * 0.20)

    drawdown_score = _inverse_score(max(max_drawdown, 0.0), 25.0)
    anomaly_score = _inverse_score(max(anomaly_ratio, 0.0), 0.03)
    risk_score = _clip_score(drawdown_score * 0.8 + anomaly_score * 0.2)

    stability_score = _clip_score(valid_ratio * 0.6 + ok_ratio * 0.4)

    gross_abs = max(abs(gross_total_return), 1.0)
    cost_burden_pct = cost_drag / gross_abs * 100.0
    cost_score = _inverse_score(cost_burden_pct, 35.0)
    trade_score = _clip_score(100.0 if total_trades >= 20 else total_trades / 20.0 * 100.0)
    efficiency_score = _clip_score(cost_score * 0.7 + trade_score * 0.3)

    # C: robustness_score combines OOS Sharpe quality and WF stability
    if oos_sharpe is not None:
        oos_quality = _clip_score(max(oos_sharpe, 0.0) / 2.0 * 100.0)
        if wf_stability is not None:
            robustness_score = _clip_score(oos_quality * 0.6 + wf_stability * 100.0 * 0.4)
        else:
            robustness_score = _clip_score(oos_quality)
    elif wf_stability is not None:
        robustness_score = _clip_score(wf_stability * 100.0)
    else:
        robustness_score = None

    # C: Include robustness in deployment score when available
    if robustness_score is not None:
        deployment_score = _clip_score(
            edge_score * 0.30
            + risk_score * 0.20
            + stability_score * 0.15
            + efficiency_score * 0.15
            + robustness_score * 0.20
        )
    else:
        deployment_score = _clip_score(
            edge_score * 0.35
            + risk_score * 0.25
            + stability_score * 0.20
            + efficiency_score * 0.20
        )

    # DSR-based promotion gating — Bailey & López de Prado (2014).
    dsr_reject = dsr < _DSR_REJECT_BELOW
    dsr_downgrade = _DSR_REJECT_BELOW <= dsr < _DSR_DOWNGRADE_OK

    reasons: List[str] = []
    if total_return <= 0:
        reasons.append("best strategy net return is non-positive")
    if max_drawdown > 15:
        reasons.append(f"max drawdown too high ({max_drawdown:.2f}%)")
    if effective_sharpe < 1.0:
        sharpe_label = "oos_sharpe" if oos_sharpe is not None else "sharpe"
        reasons.append(f"{sharpe_label} too low ({effective_sharpe:.2f})")
    if cost_burden_pct > 25:
        reasons.append(f"cost drag too high ({cost_burden_pct:.2f}% of gross return)")
    if total_trades < 10:
        reasons.append(f"trade count too low ({int(total_trades)})")
    if valid_ratio < 50:
        reasons.append(f"valid run ratio too low ({valid_ratio:.1f}%)")
    # C: OOS degradation warning — 35% degradation is the industry warning threshold
    if oos_sharpe is not None and is_sharpe is not None and is_sharpe > 0:
        degradation = (is_sharpe - oos_sharpe) / max(abs(is_sharpe), 0.01)
        if degradation > 0.35:
            reasons.append(f"OOS degradation too high (IS={is_sharpe:.2f} OOS={oos_sharpe:.2f}, deg={degradation:.0%})")
    if wf_stability is not None and wf_stability < 0.5:
        reasons.append(f"walk-forward unstable (stability={wf_stability:.2f})")

    # Base promotion tier from deployment metrics only. OOS and DSR adjustments
    # are applied as explicit, ordered downgrades below so the final decision
    # always reflects every gate that fired (rather than silently rejecting).
    decision = "reject"
    if deployment_score >= 75 and effective_sharpe >= 1.2 and max_drawdown <= 12 and valid_ratio >= 60:
        decision = "live_candidate"
    elif deployment_score >= 60 and effective_sharpe >= 1.0 and max_drawdown <= 15:
        decision = "paper"
    elif deployment_score >= 45 and valid_runs > 0:
        decision = "shadow"
    append_gate(
        trace,
        code="deployment_tier",
        label="Deployment tier",
        status="block" if decision == "reject" else "pass",
        severity=3 if decision == "reject" else 0,
        input_value={
            "deployment_score": deployment_score,
            "effective_sharpe": effective_sharpe,
            "max_drawdown": max_drawdown,
            "valid_ratio_pct": valid_ratio,
        },
        threshold={
            "shadow": {"deployment_score": 45, "valid_runs": ">0"},
            "paper": {"deployment_score": 60, "effective_sharpe": 1.0, "max_drawdown": 15},
            "live_candidate": {
                "deployment_score": 75,
                "effective_sharpe": 1.2,
                "max_drawdown": 12,
                "valid_ratio_pct": 60,
            },
        },
        decision_before="score",
        decision_after=decision,
        reason=f"base validation tier resolved to {decision}",
        counterfactual_decision=decision,
        source="validation_gate",
    )

    # Out-of-sample gating.
    #
    # When OOS is present, ``effective_sharpe`` is already the OOS Sharpe (see
    # the effective_sharpe assignment above), so a failing OOS automatically
    # tanks edge_score / robustness / deployment_score and the tier gates land
    # it on shadow (or reject if deployment / DSR also fail). No explicit
    # failing-OOS cap is needed — and one would be dead code, since the
    # paper/live tiers require effective_sharpe ≥ 1.0, which a sub-threshold
    # OOS Sharpe (< 0.6) can never satisfy.
    #
    # The one case the score path cannot catch is *no OOS validation at all*:
    # there ``effective_sharpe`` falls back to the in-sample Sharpe, which can
    # be high enough for live_candidate. Such a candidate must not go to
    # live_candidate on in-sample evidence alone — cap it at paper.
    if oos_sharpe is None and decision == "live_candidate":
        before = decision
        decision = "paper"
        reasons.append(
            "downgraded live_candidate→paper: no out-of-sample validation available"
        )

        append_gate(
            trace,
            code="no_oos_live_cap",
            label="No OOS live cap",
            status="downgrade",
            severity=4,
            input_value=oos_sharpe,
            threshold="OOS Sharpe required for live_candidate",
            decision_before=before,
            decision_after=decision,
            reason="candidate cannot reach live_candidate without out-of-sample validation",
            counterfactual_decision=before,
            source="validation_gate",
        )
    else:
        append_gate(
            trace,
            code="no_oos_live_cap",
            label="No OOS live cap",
            status="pass" if oos_sharpe is not None else "skip",
            severity=0,
            input_value=oos_sharpe,
            threshold="OOS Sharpe required for live_candidate",
            decision_before=decision,
            decision_after=decision,
            reason="OOS validation present or candidate is not live-candidate tier",
            source="validation_gate",
        )

    # DSR gating: reject or downgrade one tier based on multiple-testing correction.
    if dsr_reject:
        before = decision
        decision = "reject"
        reasons.append(
            f"DSR too low ({dsr:.2f}<{_DSR_REJECT_BELOW:.2f}) — likely spurious edge from multiple testing"
        )
        append_gate(
            trace,
            code="dsr_reject",
            label="DSR reject",
            status="block",
            severity=5,
            input_value=round(dsr, 4),
            threshold=_DSR_REJECT_BELOW,
            margin=round(dsr - _DSR_REJECT_BELOW, 6),
            decision_before=before,
            decision_after=decision,
            reason="deflated Sharpe is below reject threshold",
            counterfactual_decision=before,
            source="validation_gate",
        )
    elif dsr_downgrade:
        if decision == "live_candidate":
            before = decision
            decision = "paper"
            reasons.append(f"downgraded live_candidate→paper: DSR={dsr:.2f}<{_DSR_DOWNGRADE_OK:.2f}")
            append_gate(
                trace,
                code="dsr_downgrade",
                label="DSR downgrade",
                status="downgrade",
                severity=4,
                input_value=round(dsr, 4),
                threshold=_DSR_DOWNGRADE_OK,
                margin=round(dsr - _DSR_DOWNGRADE_OK, 6),
                decision_before=before,
                decision_after=decision,
                reason="deflated Sharpe is below promotion threshold",
                counterfactual_decision=before,
                source="validation_gate",
            )
        elif decision == "paper":
            before = decision
            decision = "shadow"
            reasons.append(f"downgraded paper→shadow: DSR={dsr:.2f}<{_DSR_DOWNGRADE_OK:.2f}")

            append_gate(
                trace,
                code="dsr_downgrade",
                label="DSR downgrade",
                status="downgrade",
                severity=4,
                input_value=round(dsr, 4),
                threshold=_DSR_DOWNGRADE_OK,
                margin=round(dsr - _DSR_DOWNGRADE_OK, 6),
                decision_before=before,
                decision_after=decision,
                reason="deflated Sharpe is below promotion threshold",
                counterfactual_decision=before,
                source="validation_gate",
            )
        else:
            append_gate(
                trace,
                code="dsr_downgrade",
                label="DSR downgrade",
                status="warn",
                severity=2,
                input_value=round(dsr, 4),
                threshold=_DSR_DOWNGRADE_OK,
                decision_before=decision,
                decision_after=decision,
                reason="DSR is weak but current tier cannot be downgraded further",
                source="validation_gate",
            )
    else:
        append_gate(
            trace,
            code="dsr_gate",
            label="DSR gate",
            status="pass",
            severity=0,
            input_value=round(dsr, 4),
            threshold=_DSR_DOWNGRADE_OK,
            margin=round(dsr - _DSR_DOWNGRADE_OK, 6),
            decision_before=decision,
            decision_after=decision,
            reason="deflated Sharpe passed multiple-testing gate",
            source="validation_gate",
        )

    # Promotion must respect minimum realized sample size. Thin trading samples are
    # too noisy to treat as paper/live-ready even when return and Sharpe look good.
    trade_count_int = int(total_trades)
    if trade_count_int < _MIN_TRADES_FOR_SHADOW:
        before = decision
        decision = "reject"
        reasons.append(
            f"rejected: completed trades {trade_count_int} < {_MIN_TRADES_FOR_SHADOW}"
        )
        append_gate(
            trace,
            code="trade_count_min_shadow",
            label="Trade count minimum",
            status="block",
            severity=5,
            input_value=trade_count_int,
            threshold=_MIN_TRADES_FOR_SHADOW,
            margin=float(trade_count_int - _MIN_TRADES_FOR_SHADOW),
            decision_before=before,
            decision_after=decision,
            reason="completed trades are below the minimum for shadow",
            counterfactual_decision=before,
            source="validation_gate",
        )
    elif decision == "live_candidate" and trade_count_int < _MIN_TRADES_FOR_LIVE_CANDIDATE:
        before = decision
        decision = "paper" if trade_count_int >= _MIN_TRADES_FOR_PAPER else "shadow"
        reasons.append(
            f"downgraded live_candidate due to trade count ({trade_count_int} < {_MIN_TRADES_FOR_LIVE_CANDIDATE})"
        )
        append_gate(
            trace,
            code="trade_count_live_candidate",
            label="Live candidate trade count",
            status="downgrade",
            severity=4,
            input_value=trade_count_int,
            threshold=_MIN_TRADES_FOR_LIVE_CANDIDATE,
            margin=float(trade_count_int - _MIN_TRADES_FOR_LIVE_CANDIDATE),
            decision_before=before,
            decision_after=decision,
            reason="sample is too thin for live-candidate tier",
            counterfactual_decision=before,
            source="validation_gate",
        )
    elif decision == "paper" and trade_count_int < _MIN_TRADES_FOR_PAPER:
        before = decision
        decision = "shadow"
        reasons.append(
            f"downgraded paper due to trade count ({trade_count_int} < {_MIN_TRADES_FOR_PAPER})"
        )
        append_gate(
            trace,
            code="trade_count_paper",
            label="Paper trade count",
            status="downgrade",
            severity=4,
            input_value=trade_count_int,
            threshold=_MIN_TRADES_FOR_PAPER,
            margin=float(trade_count_int - _MIN_TRADES_FOR_PAPER),
            decision_before=before,
            decision_after=decision,
            reason="sample is too thin for paper tier",
            counterfactual_decision=before,
            source="validation_gate",
        )
    else:
        append_gate(
            trace,
            code="trade_count_gate",
            label="Trade count gate",
            status="pass",
            severity=0,
            input_value=trade_count_int,
            threshold={
                "shadow": _MIN_TRADES_FOR_SHADOW,
                "paper": _MIN_TRADES_FOR_PAPER,
                "live_candidate": _MIN_TRADES_FOR_LIVE_CANDIDATE,
            },
            decision_before=decision,
            decision_after=decision,
            reason="trade sample is sufficient for the resolved tier",
            source="validation_gate",
        )

    if decision != "reject":
        reasons.insert(0, f"recommended for {decision}")

    if trade_count_int < _MIN_TRADES_FOR_SHADOW:
        outcome_type = "insufficient_sample"
    elif dsr_reject:
        outcome_type = "overfit_suspect"
    elif decision == "reject" and effective_sharpe < 1.0:
        outcome_type = "poor_edge"
    elif decision == "reject":
        outcome_type = "borderline"
    else:
        outcome_type = "accepted"

    companion = validation_companion_scores(
        deployment_score=deployment_score,
        edge_score=edge_score,
        risk_score=risk_score,
    )
    calibration_prior = get_family_regime_prior(
        strategy_family=best.get("strategy") or result.get("strategy_family") or result.get("strategy") or "unknown",
        regime=result.get("market_regime") or result.get("regime") or "mixed",
        symbol_scope=result.get("symbol_scope") or symbol_scope_for_symbol(best.get("symbol") or result.get("symbol")),
    )
    trace.final_decision = decision
    trace.effective_score = float(deployment_score or 0.0)
    trace.refresh_root_blocker()
    if trace.root_blocker_code:
        try:
            from core.audit.gate_counterfactuals import record_gate_counterfactual  # noqa: PLC0415

            record_gate_counterfactual(
                trace=trace.to_dict(),
                observed_decision=decision,
                mode="validation_gate",
            )
        except Exception:
            pass

    return ProposalValidationSummary(
        computed_at=now,
        decision=decision,
        edge_score=edge_score,
        risk_score=risk_score,
        stability_score=stability_score,
        efficiency_score=efficiency_score,
        deployment_score=deployment_score,
        is_score=is_sharpe,
        oos_score=oos_sharpe,
        wf_stability=wf_stability,
        robustness_score=robustness_score,
        dsr_score=round(dsr, 4),
        wf_consistency=wf_consistency,
        reasons=reasons,
        decision_trace=trace.to_dict(),
        effective_sharpe_source=effective_sharpe_source,
        outcome_type=outcome_type,
        reserve_eligible=bool(outcome_type == "borderline" and deployment_score >= 40.0),
        calibrated_confidence=companion["calibrated_confidence"],
        expected_edge_bps=companion["expected_edge_bps"],
        risk_adjusted_edge=companion["risk_adjusted_edge"],
        score_explanation=companion["score_explanation"],
        metrics={
            "runs": runs,
            "valid_runs": valid_runs,
            "valid_ratio_pct": round(valid_ratio, 2),
            "quality_counts": quality_counts,
            "quality_ok_ratio_pct": round(ok_ratio, 2),
            "best": best,
            "cost_burden_pct": round(cost_burden_pct, 2),
            "is_sharpe": is_sharpe,
            "oos_sharpe": oos_sharpe,
            "wf_stability": wf_stability,
            "robustness_score": robustness_score,
            "dsr_score": round(dsr, 4),
            "wf_consistency": wf_consistency,
            "calibration_prior": calibration_prior,
        },
    )


def build_promotion_decision(candidate_id: str, summary: ProposalValidationSummary) -> PromotionDecision:
    decision = str(summary.decision or "reject")
    paper_allocation_cap = max(0.0, min(1.0, float(getattr(settings, "DEFAULT_STRATEGY_ALLOCATION", 0.15) or 0.15)))
    constraints = {
        "allocation_cap": paper_allocation_cap if decision == "paper" else 0.0,
        "runtime_mode": "paper" if decision == "paper" else ("shadow_virtual" if decision == "shadow" else "candidate_only"),
        "deployment_score": float(summary.deployment_score or 0.0),
    }
    if decision == "live_candidate":
        constraints["approval_required"] = True
    reason = "; ".join(summary.reasons[:3]) if summary.reasons else "no explanation"
    return PromotionDecision(
        candidate_id=str(candidate_id),
        decision=decision,
        reason=reason,
        constraints=constraints,
        created_at=_now_utc(),
    )
