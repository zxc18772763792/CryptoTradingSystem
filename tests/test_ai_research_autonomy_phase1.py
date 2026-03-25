from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

from core.ai.proposal_schemas import (
    ResearchLineage,
    ResearchProposal,
    ResearchSearchBudget,
    StrategyDraft,
)
from core.ai.research_planner import PlannerGenerateRequest, generate_research_proposal
from core.research.orchestrator import _build_experiment_spec, build_research_config_from_proposal
from core.research.strategy_research import ResearchConfig


def _now() -> datetime:
    return datetime.now(timezone.utc)


def test_generate_research_proposal_captures_autonomy_fields():
    req = PlannerGenerateRequest(
        goal="Research funding dislocations with sentiment confirmation on BTC/USDT",
        market_regime="mixed",
        symbols=["BTCUSDT"],
        timeframes=["1h", "4h"],
        constraints={
            "max_templates": 4,
            "exploration_bias": 0.6,
            "lineage": {
                "parent_candidate_id": "cand-parent",
                "generation": 2,
                "mutation_notes": ["tighten exits"],
            },
        },
        metadata={"mutation_notes": ["add funding regime filter"]},
        llm_research_output={
            "hypothesis": "Funding dislocations confirmed by sentiment may create near-term reversals.",
            "experiment_plan": ["Compare hybrid seed against the baseline template set."],
            "metrics_to_check": ["sharpe_ratio", "max_drawdown"],
            "expected_failure_modes": ["Funding spikes may fade before confirmation arrives."],
            "proposed_strategy_changes": [
                {
                    "strategy": "MarketSentimentStrategy",
                    "name": "Funding shock hybrid",
                    "thesis": "Combine funding extremes with sentiment confirmation.",
                    "features": ["funding_rate", "news_sentiment_score"],
                    "entry_logic": ["Activate when funding is extreme and sentiment confirms the move."],
                    "exit_logic": ["Deactivate after funding normalizes or sentiment fades."],
                    "risk_logic": ["Reduce exposure during volatility spikes."],
                    "params": {"funding_z_threshold": 2.0, "sentiment_gate": 0.15},
                    "confidence": 0.66,
                }
            ],
            "uncertainty": "Needs out-of-sample validation.",
            "evidence_refs": ["funding_cache", "news_summary"],
        },
    )

    out = generate_research_proposal(req, actor="pytest")
    proposal = out.proposal

    assert proposal.research_mode == "hybrid"
    assert len(proposal.strategy_drafts) == 1
    assert proposal.search_budget.max_templates == 4
    assert proposal.search_budget.max_strategy_drafts == 3
    assert proposal.search_budget.max_backtest_runs == 48
    assert proposal.search_budget.notes == ["llm_strategy_drafts_present"]
    assert proposal.lineage is not None
    assert proposal.lineage.parent_candidate_id == "cand-parent"
    assert proposal.lineage.generation == 2
    assert proposal.lineage.mutation_notes == ["tighten exits", "add funding regime filter"]
    assert proposal.metadata["autonomy_summary"]["research_mode"] == "hybrid"
    assert proposal.metadata["autonomy_summary"]["strategy_draft_count"] == 1

    draft = proposal.strategy_drafts[0]
    assert draft.mode == "hybrid_seed"
    assert draft.template_hint == "MarketSentimentStrategy"
    assert draft.features == ["funding_rate", "news_sentiment_score"]
    assert draft.params == {"funding_z_threshold": 2.0, "sentiment_gate": 0.15}


def test_build_research_config_uses_draft_template_hint_when_templates_missing():
    now = _now()
    proposal = ResearchProposal(
        proposal_id="proposal-draft-hint",
        created_at=now,
        updated_at=now,
        research_mode="hybrid",
        thesis="Use a draft hint as the executable seed.",
        target_symbols=["BTC/USDT"],
        target_timeframes=["1h"],
        strategy_templates=[],
        strategy_drafts=[
            StrategyDraft(
                draft_id="draft-01",
                name="MA seed",
                mode="hybrid_seed",
                template_hint="MAStrategy",
                thesis="Use the MA template as an executable seed.",
            )
        ],
    )

    calls: list[list[str]] = []

    def _fake_filter(strategies: list[str]) -> tuple[list[str], list[str]]:
        calls.append(list(strategies))
        return list(strategies), []

    with patch("core.research.orchestrator._filter_supported_research_strategies", side_effect=_fake_filter):
        config = build_research_config_from_proposal(
            proposal,
            exchange="binance",
            symbol=None,
            days=30,
            commission_rate=0.0004,
            slippage_bps=2.0,
            initial_capital=10000.0,
            timeframes=[],
            strategies=[],
        )

    assert calls == [["MAStrategy"]]
    assert config.symbol == "BTC/USDT"
    assert config.timeframes == ["1h"]
    assert config.strategies == ["MAStrategy"]


def test_build_experiment_spec_propagates_autonomy_fields():
    now = _now()
    draft = StrategyDraft(
        draft_id="draft-01",
        name="Autonomy draft",
        mode="dsl_seed",
        template_hint="MAStrategy",
        thesis="Mutate a seed into a new research path.",
        features=["funding_rate", "basis_pct"],
        entry_logic=["Activate after a funding dislocation and basis confirmation."],
        exit_logic=["Deactivate when the spread normalizes."],
        risk_logic=["Cut exposure when volatility expands."],
        params={"lookback": 12},
        confidence=0.71,
    )
    search_budget = ResearchSearchBudget(
        max_templates=4,
        max_strategy_drafts=5,
        max_backtest_runs=80,
        exploration_bias=0.55,
        notes=["llm_strategy_drafts_present", "phase1_autonomy"],
    )
    lineage = ResearchLineage(
        lineage_id="lineage-001",
        parent_proposal_id="proposal-parent",
        parent_candidate_id="cand-parent",
        generation=3,
        mutation_notes=["narrow risk window"],
    )
    proposal = ResearchProposal(
        proposal_id="proposal-autonomy",
        created_at=now,
        updated_at=now,
        research_mode="autonomous_draft",
        thesis="Promote autonomous draft research metadata into the experiment layer.",
        target_symbols=["BTC/USDT"],
        target_timeframes=["1h", "4h"],
        strategy_templates=["MAStrategy"],
        strategy_drafts=[draft],
        search_budget=search_budget,
        lineage=lineage,
    )
    config = ResearchConfig(
        exchange="binance",
        symbol="BTC/USDT",
        days=45,
        initial_capital=25000.0,
        timeframes=["1h", "4h"],
        strategies=["MAStrategy"],
        commission_rate=0.0004,
        slippage_bps=1.5,
        parameter_space={"MAStrategy": {"fast_period": [5, 10]}},
    )

    experiment = _build_experiment_spec(proposal, config, actor="pytest")

    assert experiment.research_mode == "autonomous_draft"
    assert len(experiment.strategy_drafts) == 1
    assert experiment.strategy_drafts[0].draft_id == "draft-01"
    assert experiment.search_budget.max_backtest_runs == 80
    assert experiment.search_budget.max_strategy_drafts == 5
    assert experiment.lineage is not None
    assert experiment.lineage.parent_candidate_id == "cand-parent"
    assert experiment.metadata["research_mode"] == "autonomous_draft"
    assert experiment.metadata["strategy_draft_count"] == 1
