"""Small companion-score helpers that do not replace existing raw scores."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


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
            return {
                "available": True,
                "key": key,
                "strategy_family": family,
                "regime": regime_key,
                "symbol_scope": scope,
                **value,
            }
    return {
        "available": False,
        "key": candidates[0],
        "strategy_family": family,
        "regime": regime_key,
        "symbol_scope": scope,
        "degraded_reason": data.get("degraded_reason", ""),
    }


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
