"""Compare validation expectations against paper/live performance snapshots."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List

from pydantic import BaseModel, Field


class PerformanceDivergenceReport(BaseModel):
    candidate_id: str = ""
    strategy_name: str = ""
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    expected: Dict[str, Any] = Field(default_factory=dict)
    realized: Dict[str, Any] = Field(default_factory=dict)
    sample_size: int = 0
    divergence_score: float = 0.0
    status: str = "insufficient_sample"
    notes: List[str] = Field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump(mode="json")


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def build_performance_divergence_report(
    *,
    candidate: Any,
    snapshots: Iterable[Any],
    min_sample: int = 10,
) -> PerformanceDivergenceReport:
    validation = getattr(candidate, "validation_summary", None)
    metrics = dict(getattr(validation, "metrics", {}) or {})
    best = dict(metrics.get("best") or {})
    expected_sharpe = _safe_float(getattr(validation, "oos_score", None), _safe_float(getattr(validation, "is_score", None), _safe_float(best.get("sharpe_ratio"), 0.0)))
    expected_drawdown = _safe_float(best.get("max_drawdown"), 0.0)
    rows = list(snapshots or [])
    if not rows:
        return PerformanceDivergenceReport(
            candidate_id=str(getattr(candidate, "candidate_id", "") or ""),
            strategy_name=str(getattr(candidate, "strategy", "") or ""),
            expected={"sharpe": expected_sharpe, "max_drawdown": expected_drawdown},
            sample_size=0,
            status="insufficient_sample",
            notes=["no paper/live snapshots available"],
        )

    latest = rows[0]
    realized_sharpe = _safe_float(getattr(latest, "sharpe", None), _safe_float(getattr(latest, "sharpe_ratio", None), 0.0))
    realized_drawdown = _safe_float(getattr(latest, "max_drawdown", None), 0.0)
    trade_count = int(_safe_float(getattr(latest, "trade_count", None), _safe_float(getattr(latest, "trades", None), len(rows))))
    divergence = max(0.0, expected_sharpe - realized_sharpe)
    status = "aligned"
    notes: List[str] = []
    if trade_count < min_sample:
        status = "insufficient_sample"
        notes.append(f"trade_count {trade_count} < {min_sample}")
    elif expected_sharpe >= 1.0 and realized_sharpe < 0.5:
        status = "overfit_suspect"
        notes.append("realized Sharpe is far below validation expectation")
    elif divergence >= 0.75:
        status = "underperforming"
        notes.append("realized edge is materially below validation expectation")
    if realized_drawdown > max(expected_drawdown * 1.5, expected_drawdown + 5.0):
        notes.append("realized drawdown exceeds validation expectation")
        if status == "aligned":
            status = "underperforming"

    return PerformanceDivergenceReport(
        candidate_id=str(getattr(candidate, "candidate_id", "") or ""),
        strategy_name=str(getattr(candidate, "strategy", "") or ""),
        expected={"sharpe": expected_sharpe, "max_drawdown": expected_drawdown},
        realized={"sharpe": realized_sharpe, "max_drawdown": realized_drawdown, "trade_count": trade_count},
        sample_size=trade_count,
        divergence_score=round(divergence, 6),
        status=status,
        notes=notes,
    )

