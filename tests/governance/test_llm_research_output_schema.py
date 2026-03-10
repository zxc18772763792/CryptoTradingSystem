from __future__ import annotations

import pytest

from core.governance.schemas import LLMResearchOutput


def test_llm_research_output_accepts_structured_research_text():
    out = LLMResearchOutput.model_validate(
        {
            "hypothesis": "Momentum edge may decay in high spread regimes.",
            "experiment_plan": ["Run 90-day walk-forward on BTC/ETH.", "Compare against baseline MA."],
            "metrics_to_check": ["sharpe", "drawdown", "turnover"],
            "expected_failure_modes": ["overfit", "regime shift"],
            "proposed_strategy_changes": [{"strategy": "MAStrategy", "params": {"fast_period": [5, 8]}}],
            "uncertainty": "High uncertainty during macro event weeks.",
            "evidence_refs": ["news:macro_202603", "backtest:run_001"],
        }
    )
    assert "Momentum edge" in out.hypothesis


def test_llm_research_output_rejects_direct_trade_instruction():
    with pytest.raises(ValueError):
        LLMResearchOutput.model_validate(
            {
                "hypothesis": "立即买入 BTC 并开多 5 倍杠杆",
                "experiment_plan": ["下单做多"],
                "metrics_to_check": [],
                "expected_failure_modes": [],
                "proposed_strategy_changes": [],
                "uncertainty": "",
                "evidence_refs": [],
            }
        )

