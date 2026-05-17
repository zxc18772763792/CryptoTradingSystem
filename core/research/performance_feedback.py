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


def _get_value(source: Any, *names: str, default: Any = None) -> Any:
    if isinstance(source, dict):
        for name in names:
            if name in source:
                value = source.get(name)
                if value is not None:
                    return value
        return default
    for name in names:
        value = getattr(source, name, None)
        if value is not None:
            return value
    return default


def build_performance_divergence_report(
    *,
    candidate: Any,
    snapshots: Iterable[Any],
    min_sample: int = 10,
) -> PerformanceDivergenceReport:
    validation = _get_value(candidate, "validation_summary")
    metrics = dict(_get_value(validation, "metrics", default={}) or {})
    best = dict(metrics.get("best") or {})
    expected_sharpe = _safe_float(
        _get_value(validation, "oos_score"),
        _safe_float(
            _get_value(validation, "is_score"),
            _safe_float(best.get("sharpe_ratio"), 0.0),
        ),
    )
    expected_drawdown = _safe_float(best.get("max_drawdown"), 0.0)
    rows = list(snapshots or [])
    if not rows:
        return PerformanceDivergenceReport(
            candidate_id=str(_get_value(candidate, "candidate_id", default="") or ""),
            strategy_name=str(_get_value(candidate, "strategy", "strategy_name", default="") or ""),
            expected={"sharpe": expected_sharpe, "max_drawdown": expected_drawdown},
            sample_size=0,
            status="insufficient_sample",
            notes=["no paper/live snapshots available"],
        )

    latest = rows[0]
    realized_sharpe = _safe_float(
        _get_value(latest, "sharpe"),
        _safe_float(_get_value(latest, "sharpe_ratio"), 0.0),
    )
    realized_drawdown = _safe_float(_get_value(latest, "max_drawdown"), 0.0)
    trade_count = int(
        _safe_float(
            _get_value(latest, "trade_count"),
            _safe_float(_get_value(latest, "trades", "total_trades"), len(rows)),
        )
    )
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
        candidate_id=str(_get_value(candidate, "candidate_id", default="") or ""),
        strategy_name=str(_get_value(candidate, "strategy", "strategy_name", default="") or ""),
        expected={"sharpe": expected_sharpe, "max_drawdown": expected_drawdown},
        realized={"sharpe": realized_sharpe, "max_drawdown": realized_drawdown, "trade_count": trade_count},
        sample_size=trade_count,
        divergence_score=round(divergence, 6),
        status=status,
        notes=notes,
    )
