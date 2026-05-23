from __future__ import annotations

import os
from pathlib import Path

import pytest

from core.research.validation_gate import build_validation_summary_from_research_result


def _result_with_best(**best_overrides):
    best = {
        "strategy": "MAStrategy",
        "timeframe": "15m",
        "total_return": 18.5,
        "gross_total_return": 19.2,
        "sharpe_ratio": 1.6,
        "oos_sharpe": 2.0,
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


def test_validation_gate_counterfactual_audit_uses_test_runtime_path() -> None:
    from core.audit import gate_counterfactuals as audit_module

    result = _result_with_best(total_trades=0)

    build_validation_summary_from_research_result(result)

    audit_path = Path(os.environ[audit_module.AUDIT_PATH_ENV])
    assert audit_path.exists()
    assert "data/audit" not in audit_path.as_posix()
    summary = audit_module.summarize_gate_counterfactuals()
    assert summary["path"] == str(audit_path)
    assert summary["total"] >= 1


def test_validation_gate_downgrades_live_candidate_on_thin_trade_sample() -> None:
    result = _result_with_best(total_trades=12)

    summary = build_validation_summary_from_research_result(result)

    assert summary.decision == "paper"
    assert any("downgraded live_candidate due to trade count" in reason for reason in summary.reasons)


def test_validation_gate_no_oos_cannot_reach_live_candidate() -> None:
    # Strong metrics but no out-of-sample validation: must be capped at paper.
    result = _result_with_best(total_trades=60)
    result["best"].pop("oos_sharpe", None)

    summary = build_validation_summary_from_research_result(result)

    assert summary.decision != "live_candidate"
    assert summary.oos_score is None
    assert summary.effective_sharpe_source == "in_sample"
    assert summary.decision_trace["root_blocker_code"] == "no_oos_live_cap"
    assert any("no out-of-sample validation" in reason for reason in summary.reasons)


def test_validation_gate_failing_oos_never_reaches_paper_or_live() -> None:
    # When OOS is present it drives effective_sharpe, so a failing OOS can
    # never satisfy the paper/live tier gates. It must land on shadow or
    # reject (never paper/live_candidate), and OOS must be the effective Sharpe.
    result = _result_with_best(total_trades=60, oos_sharpe=0.2)

    summary = build_validation_summary_from_research_result(result)

    assert summary.decision in {"shadow", "reject"}
    assert summary.oos_score == 0.2
    assert summary.effective_sharpe_source == "oos"
    assert summary.decision_trace["gates"][0]["code"] == "effective_sharpe_source"


def test_validation_gate_passing_oos_allows_live_candidate() -> None:
    result = _result_with_best(total_trades=60, oos_sharpe=2.0)

    summary = build_validation_summary_from_research_result(result)

    assert summary.decision == "live_candidate"


def test_validation_gate_dsr_trials_include_optimization_trials(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = {}

    def fake_dsr(**kwargs):
        captured.update(kwargs)
        return 0.99

    monkeypatch.setattr("core.research.validation_gate._deflated_sharpe_ratio", fake_dsr)
    result = _result_with_best(total_trades=60, oos_sharpe=2.0, optimization_trials=32, n_bars=400)

    build_validation_summary_from_research_result(result)

    assert captured["n_trials"] == result["runs"] * 32
    assert captured["n_obs"] == 400


def test_validation_gate_dsr_uses_equity_curve_moments(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = {}

    def fake_dsr(**kwargs):
        captured.update(kwargs)
        return 0.99

    monkeypatch.setattr("core.research.validation_gate._deflated_sharpe_ratio", fake_dsr)
    result = _result_with_best(
        total_trades=60,
        oos_sharpe=2.0,
        equity_curve_sample=[10000.0, 10080.0, 9940.0, 10120.0, 10020.0, 10350.0],
    )

    build_validation_summary_from_research_result(result)

    assert captured["skewness"] != 0.0
    assert captured["kurtosis"] != 3.0


def test_validation_gate_dsr_uses_heavy_tail_prior_without_equity_sample(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = {}

    def fake_dsr(**kwargs):
        captured.update(kwargs)
        return 0.99

    monkeypatch.setattr("core.research.validation_gate._deflated_sharpe_ratio", fake_dsr)
    result = _result_with_best(total_trades=60, oos_sharpe=2.0, equity_curve_sample=[])

    build_validation_summary_from_research_result(result)

    assert captured["skewness"] == -0.2
    assert captured["kurtosis"] == 5.0
