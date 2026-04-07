from __future__ import annotations

from core.research.validation_gate import build_validation_summary_from_research_result


def _result_with_best(**best_overrides):
    best = {
        "strategy": "MAStrategy",
        "timeframe": "15m",
        "total_return": 18.5,
        "gross_total_return": 19.2,
        "sharpe_ratio": 1.6,
        "max_drawdown": 7.2,
        "win_rate": 58.0,
        "total_trades": 48,
        "anomaly_bar_ratio": 0.0,
    }
    best.update(best_overrides)
    return {
        "runs": 6,
        "valid_runs": 4,
        "quality_counts": {"ok": 4},
        "best": best,
    }


def test_validation_gate_rejects_zero_trade_candidate() -> None:
    result = _result_with_best(total_trades=0)

    summary = build_validation_summary_from_research_result(result)

    assert summary.decision == "reject"
    assert any("completed trades 0 < 1" in reason for reason in summary.reasons)


def test_validation_gate_downgrades_live_candidate_on_thin_trade_sample() -> None:
    result = _result_with_best(total_trades=12)

    summary = build_validation_summary_from_research_result(result)

    assert summary.decision == "paper"
    assert any("downgraded live_candidate due to trade count" in reason for reason in summary.reasons)
