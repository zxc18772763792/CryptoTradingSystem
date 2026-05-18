"""Small companion-score helpers that do not replace existing raw scores."""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable


DEFAULT_PRIOR_PATH = Path(__file__).resolve().parents[2] / "data" / "ai_calibration" / "family_regime_priors.json"
_QUOTE_SUFFIXES = ("USDT", "USDC", "FDUSD", "BUSD", "USD")


def clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value or 0.0)))


def _symbol_base(symbol: Any) -> str:
    text = str(symbol or "").strip().upper()
    if not text:
        return ""
    main = text.split(":", 1)[0].replace("_", "/").replace("-", "/")
    base = main.split("/", 1)[0] if "/" in main else main
    for suffix in _QUOTE_SUFFIXES:
        if base.endswith(suffix) and len(base) > len(suffix):
            return base[: -len(suffix)]
    return base


def symbol_scope_for_symbol(symbol: Any) -> str:
    base = _symbol_base(symbol)
    if base in {"BTC", "ETH"}:
        return "benchmark"
    if base:
        return "altcoin"
    return "global"


def load_family_regime_priors(path: str | Path | None = None) -> Dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_PRIOR_PATH
    try:
        if not target.exists():
            return {"schema_version": "family_regime_priors.v1", "priors": {}}
        data = json.loads(target.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            data.setdefault("priors", {})
            return data
    except Exception:
        return {"schema_version": "family_regime_priors.v1", "priors": {}, "degraded_reason": "prior_load_failed"}
    return {"schema_version": "family_regime_priors.v1", "priors": {}}


def get_family_regime_prior(
    *,
    strategy_family: Any,
    regime: Any,
    symbol_scope: Any,
    path: str | Path | None = None,
) -> Dict[str, Any]:
    family = str(strategy_family or "unknown").strip() or "unknown"
    regime_key = str(regime or "mixed").strip().lower() or "mixed"
    scope = str(symbol_scope or "global").strip().lower() or "global"
    data = load_family_regime_priors(path)
    priors = dict(data.get("priors") or {})
    candidates = [
        f"{family}|{regime_key}|{scope}",
        f"{family}|{regime_key}|*",
        f"{family}|*|{scope}",
        f"{family}|*|*",
    ]
    for key in candidates:
        value = priors.get(key)
        if isinstance(value, dict):
            payload = dict(value)
            payload.setdefault("graduation", prior_graduation_policy(payload))
            return {
                "available": True,
                "key": key,
                "strategy_family": family,
                "regime": regime_key,
                "symbol_scope": scope,
                **payload,
            }
    return {
        "available": False,
        "key": candidates[0],
        "strategy_family": family,
        "regime": regime_key,
        "symbol_scope": scope,
        "degraded_reason": data.get("degraded_reason", ""),
    }


_PRIOR_EWMA_ALPHA = 0.3
_PRIOR_MAX_SAMPLE = 500
_PRIOR_GRADUATION_MIN_SAMPLE = 30
_PRIOR_GRADUATION_MIN_COUNTERFACTUAL_OUTCOMES = 20
_PRIOR_MAX_BINDING_ADJUSTMENT = 0.10


def _clamp_float(value: Any, lo: float, hi: float, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    if out != out:  # NaN
        return default
    return max(lo, min(hi, out))


def prior_graduation_policy(entry: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Return the governance policy and current eligibility for a prior.

    The prior file remains advisory by default. A prior only becomes eligible
    for binding use after it has enough observations *and* enough linked
    counterfactual outcomes to be falsifiable.
    """
    current = dict(entry or {})
    sample = int(current.get("sample_size") or 0)
    cf_samples = int(current.get("counterfactual_outcome_sample_size") or 0)
    status_counts = dict(current.get("status_counts") or {})
    total = sum(int(v) for v in status_counts.values()) or sample or 0
    aligned = int(status_counts.get("aligned", 0)) + int(status_counts.get("outperforming", 0))
    negative = (
        int(status_counts.get("underperforming", 0))
        + int(status_counts.get("overfit_suspect", 0))
        + int(status_counts.get("decayed", 0))
    )
    confirmation_rate = float(current.get("confirmation_rate") or (aligned / total if total else 0.0))
    negative_rate = float(current.get("negative_evidence_rate") or (negative / total if total else 0.0))
    binding_eligible = bool(
        sample >= _PRIOR_GRADUATION_MIN_SAMPLE
        and cf_samples >= _PRIOR_GRADUATION_MIN_COUNTERFACTUAL_OUTCOMES
        and (confirmation_rate >= 0.65 or negative_rate >= 0.35)
    )
    if binding_eligible and negative_rate >= 0.35:
        status = "eligible_for_capped_downweight"
    elif binding_eligible and confirmation_rate >= 0.65:
        status = "eligible_for_capped_uplift"
    elif sample >= _PRIOR_GRADUATION_MIN_SAMPLE:
        status = "needs_counterfactual_outcomes"
    elif sample > 0:
        status = "advisory_collecting_samples"
    else:
        status = "cold_start"
    return {
        "status": status,
        "binding_eligible": binding_eligible,
        "min_sample_size": _PRIOR_GRADUATION_MIN_SAMPLE,
        "min_counterfactual_outcome_sample_size": _PRIOR_GRADUATION_MIN_COUNTERFACTUAL_OUTCOMES,
        "max_binding_adjustment": _PRIOR_MAX_BINDING_ADJUSTMENT,
        "sample_size": sample,
        "counterfactual_outcome_sample_size": cf_samples,
        "confirmation_rate": round(confirmation_rate, 6),
        "negative_evidence_rate": round(negative_rate, 6),
    }


def update_family_regime_priors(
    updates: Iterable[Dict[str, Any]],
    *,
    path: str | Path | None = None,
) -> Dict[str, Any]:
    """Fold realized-vs-expected divergence observations into the advisory
    prior file. Advisory only; consumed by the planner as a note, never as a
    hard gate. Atomic write; bounded EWMA so a single bad sample cannot blow
    the prior up.

    Each update item: ``strategy_family``, ``regime``, ``symbol_scope``,
    ``divergence_score`` (expected_sharpe - realized_sharpe), optional
    ``edge_delta`` (realized_sharpe - expected_sharpe), and ``status`` (one of
    the PerformanceDivergenceReport statuses). Positive and negative statuses
    are both retained so the prior can learn confirmations, not just decays.
    """
    target = Path(path) if path is not None else DEFAULT_PRIOR_PATH
    data = load_family_regime_priors(target)
    priors: Dict[str, Any] = dict(data.get("priors") or {})

    applied = 0
    for raw in updates or []:
        if not isinstance(raw, dict):
            continue
        family = str(raw.get("strategy_family") or "unknown").strip() or "unknown"
        regime = str(raw.get("regime") or "mixed").strip().lower() or "mixed"
        scope = str(raw.get("symbol_scope") or "global").strip().lower() or "global"
        key = f"{family}|{regime}|{scope}"
        gap = _clamp_float(raw.get("divergence_score"), -10.0, 10.0, 0.0)
        edge_delta = _clamp_float(raw.get("edge_delta"), -10.0, 10.0, default=-gap)
        status = str(raw.get("status") or "").strip().lower() or "unknown"

        entry = dict(priors.get(key) or {})
        sample = int(entry.get("sample_size") or 0)
        prev_ewma = entry.get("sharpe_gap_ewma")
        if prev_ewma is None or sample == 0:
            ewma = gap
        else:
            ewma = (1.0 - _PRIOR_EWMA_ALPHA) * float(prev_ewma) + _PRIOR_EWMA_ALPHA * gap
        prev_edge_delta = entry.get("edge_delta_ewma")
        if prev_edge_delta is None or sample == 0:
            edge_delta_ewma = edge_delta
        else:
            edge_delta_ewma = (1.0 - _PRIOR_EWMA_ALPHA) * float(prev_edge_delta) + _PRIOR_EWMA_ALPHA * edge_delta

        status_counts = dict(entry.get("status_counts") or {})
        status_counts[status] = int(status_counts.get(status, 0)) + 1
        new_sample = min(_PRIOR_MAX_SAMPLE, sample + 1)
        decayed_count = int(status_counts.get("decayed", 0))
        overfit_count = int(status_counts.get("overfit_suspect", 0))
        underperform_count = int(status_counts.get("underperforming", 0))
        aligned_count = int(status_counts.get("aligned", 0)) + int(status_counts.get("outperforming", 0))
        total_for_rate = sum(int(v) for v in status_counts.values()) or 1
        negative_count = decayed_count + overfit_count + underperform_count
        cf_increment = int(bool(raw.get("counterfactual_outcome_observed"))) + max(
            0,
            int(raw.get("counterfactual_outcome_count") or 0),
        )
        cf_sample = min(
            _PRIOR_MAX_SAMPLE,
            int(entry.get("counterfactual_outcome_sample_size") or 0) + cf_increment,
        )

        next_entry = {
            "sample_size": new_sample,
            "counterfactual_outcome_sample_size": cf_sample,
            "sharpe_gap_ewma": round(_clamp_float(ewma, -10.0, 10.0, 0.0), 6),
            "edge_delta_ewma": round(_clamp_float(edge_delta_ewma, -10.0, 10.0, 0.0), 6),
            "recent_decay_rate": round(decayed_count / total_for_rate, 6),
            "overfit_rate": round(overfit_count / total_for_rate, 6),
            "underperform_rate": round(underperform_count / total_for_rate, 6),
            "confirmation_rate": round(aligned_count / total_for_rate, 6),
            "negative_evidence_rate": round(negative_count / total_for_rate, 6),
            "status_counts": status_counts,
            "last_status": status,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        next_entry["graduation"] = prior_graduation_policy(next_entry)
        priors[key] = next_entry
        applied += 1

    payload = {
        "schema_version": str(data.get("schema_version") or "family_regime_priors.v1"),
        "updated_at": datetime.now(timezone.utc).isoformat() if applied else data.get("updated_at"),
        "priors": priors,
    }
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(target.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
        os.replace(tmp_name, target)
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise
    return {"applied": applied, "keys": sorted(priors.keys()), "path": str(target)}


def validation_companion_scores(*, deployment_score: float, edge_score: float, risk_score: float) -> Dict[str, Any]:
    calibrated = clamp01(float(deployment_score or 0.0) / 100.0)
    risk_adjusted = clamp01(
        (float(edge_score or 0.0) * 0.55 + float(deployment_score or 0.0) * 0.35 + float(risk_score or 0.0) * 0.10)
        / 100.0
    )
    return {
        "calibrated_confidence": round(calibrated, 6),
        "expected_edge_bps": round(max(0.0, float(edge_score or 0.0)) * 2.0, 6),
        "risk_adjusted_edge": round(risk_adjusted, 6),
        "score_explanation": "validation deployment_score calibrated to 0-1 companion fields",
    }


def radar_companion_scores(row: Dict[str, Any]) -> Dict[str, Any]:
    scores = dict(row.get("scores") or {})
    raw = float(scores.get("priority") or scores.get("layout") or scores.get("alert") or 0.0)
    return {
        "calibrated_confidence": round(clamp01(raw), 6),
        "expected_edge_bps": None,
        "risk_adjusted_edge": round(clamp01(raw), 6),
        "score_explanation": "altcoin radar score calibrated as a companion 0-1 field",
    }
