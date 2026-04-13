"""Validation scoring and promotion recommendation for AI research runs."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

from scipy.stats import norm as _spnorm
import math as _math

from config.settings import settings
from core.ai.proposal_schemas import ProposalValidationSummary
from core.research.experiment_schemas import PromotionDecision

_MIN_TRADES_FOR_SHADOW = 1
_MIN_TRADES_FOR_PAPER = 10
_MIN_TRADES_FOR_LIVE_CANDIDATE = 30


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
    adj = 1.0 - skewness * sr + (kurtosis - 1.0) / 4.0 * sr ** 2
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

    # DSR: deflated for multiple testing across all runs tested
    n_trials_for_dsr = max(1, runs)
    # n_obs should be bar count, not trade count — use n_bars when available,
    # fall back to total_trades * 5 as a rough proxy (assuming ~20% trade rate)
    _n_bars = int(best.get("n_bars", 0) or 0)
    _n_trades = int(best.get("total_trades", 10) or 10)
    n_obs_for_dsr = max(50, _n_bars if _n_bars > 0 else _n_trades * 5)
    dsr = _deflated_sharpe_ratio(
        sharpe=effective_sharpe,
        n_trials=n_trials_for_dsr,
        n_obs=n_obs_for_dsr,
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

    # DSR-based promotion gating (add to reasons list built below)
    dsr_reject = dsr < 0.3
    dsr_downgrade = 0.3 <= dsr < 0.5

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
    # C: OOS degradation warning
    if oos_sharpe is not None and is_sharpe is not None and is_sharpe > 0:
        degradation = (is_sharpe - oos_sharpe) / max(abs(is_sharpe), 0.01)
        if degradation > 0.5:
            reasons.append(f"OOS degradation too high (IS={is_sharpe:.2f} OOS={oos_sharpe:.2f})")
    if wf_stability is not None and wf_stability < 0.3:
        reasons.append(f"walk-forward unstable (stability={wf_stability:.2f})")

    # C: Promotion decision — OOS takes priority over IS for gating
    oos_passes = oos_sharpe is None or oos_sharpe >= 0.8  # must not fail OOS gate
    decision = "reject"
    if deployment_score >= 75 and effective_sharpe >= 1.2 and max_drawdown <= 12 and valid_ratio >= 60 and oos_passes:
        decision = "live_candidate"
    elif deployment_score >= 60 and effective_sharpe >= 1.0 and max_drawdown <= 15 and oos_passes:
        decision = "paper"
    elif deployment_score >= 45 and valid_runs > 0 and oos_passes:
        decision = "shadow"
    # DSR gating: reject or downgrade based on multiple-testing correction
    if dsr_reject:
        decision = "reject"
        reasons.append(f"DSR too low ({dsr:.2f}) — likely spurious edge from multiple testing")
    elif dsr_downgrade:
        if decision == "live_candidate":
            decision = "paper"
            reasons.append(f"downgraded live_candidate→paper: DSR={dsr:.2f}<0.5")
        elif decision == "paper":
            decision = "shadow"
            reasons.append(f"downgraded paper→shadow: DSR={dsr:.2f}<0.5")

    # Explicit downgrade: OOS fails → cap at shadow
    if oos_sharpe is not None and oos_sharpe < 0.8:
        if decision == "live_candidate":
            decision = "shadow"
            reasons.append("downgraded live_candidate→shadow: OOS Sharpe below threshold")
        elif decision == "paper":
            decision = "shadow"
            reasons.append("downgraded paper→shadow: OOS Sharpe below threshold")

    # Promotion must respect minimum realized sample size. Thin trading samples are
    # too noisy to treat as paper/live-ready even when return and Sharpe look good.
    trade_count_int = int(total_trades)
    if trade_count_int < _MIN_TRADES_FOR_SHADOW:
        decision = "reject"
        reasons.append(
            f"rejected: completed trades {trade_count_int} < {_MIN_TRADES_FOR_SHADOW}"
        )
    elif decision == "live_candidate" and trade_count_int < _MIN_TRADES_FOR_LIVE_CANDIDATE:
        decision = "paper" if trade_count_int >= _MIN_TRADES_FOR_PAPER else "shadow"
        reasons.append(
            f"downgraded live_candidate due to trade count ({trade_count_int} < {_MIN_TRADES_FOR_LIVE_CANDIDATE})"
        )
    elif decision == "paper" and trade_count_int < _MIN_TRADES_FOR_PAPER:
        decision = "shadow"
        reasons.append(
            f"downgraded paper due to trade count ({trade_count_int} < {_MIN_TRADES_FOR_PAPER})"
        )

    if decision != "reject":
        reasons.insert(0, f"recommended for {decision}")

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
