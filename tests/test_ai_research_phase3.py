from __future__ import annotations

from datetime import datetime, timezone

from core.ai.proposal_schemas import (
    ResearchProposal,
    ResearchSearchBudget,
    ResearchSearchSummary,
    SearchDraftEvaluation,
    StrategyDraft,
)
from core.ai.research_planner import PlannerGenerateRequest, generate_research_proposal
from core.ai.research_search_loop import run_research_search_loop
from core.research.experiment_schemas import ExperimentSpec
from core.research.orchestrator import _build_experiment_spec, _create_candidates_from_result
from core.research.strategy_research import ResearchConfig


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _make_draft(
    draft_id: str,
    *,
    name: str,
    params: dict[str, object],
    generation: int = 0,
    parent_draft_id: str | None = None,
) -> StrategyDraft:
    return StrategyDraft(
        draft_id=draft_id,
        name=name,
        mode="hybrid_seed",
        template_hint="EMAStrategy",
        thesis="Search EMA crossover variations with explicit exits.",
        rationale="Phase 3 draft fixture.",
        features=["ema_fast", "ema_slow"],
        entry_logic=["cross_over(ema_fast, ema_slow)"],
        exit_logic=["cross_under(ema_fast, ema_slow)"],
        params=dict(params),
        confidence=0.67,
        generation=generation,
        parent_draft_id=parent_draft_id,
    )


def _make_search_summary() -> ResearchSearchSummary:
    return ResearchSearchSummary(
        loop_enabled=True,
        evaluated_drafts=4,
        accepted_drafts=2,
        rejected_drafts=2,
        champion_draft_id="draft-ema-01",
        challenger_draft_ids=["draft-ema-02"],
        rejected_reason_counts={"duplicate_signature": 1, "budget_trimmed": 1},
        draft_evaluations=[
            SearchDraftEvaluation(
                draft_id="draft-ema-01",
                name="EMA Draft 01",
                template_hint="EMAStrategy",
                generation=0,
                heuristic_score=83.0,
                novelty_score=1.0,
                selection_status="champion",
            ),
            SearchDraftEvaluation(
                draft_id="draft-ema-02",
                name="EMA Draft 02",
                template_hint="EMAStrategy",
                parent_draft_id="draft-ema-01",
                generation=1,
                heuristic_score=79.5,
                novelty_score=0.58,
                selection_status="challenger",
                mutation_notes=["fast_period: 8 -> 13"],
            ),
            SearchDraftEvaluation(
                draft_id="draft-ema-03",
                name="EMA Draft 03",
                template_hint="EMAStrategy",
                generation=0,
                heuristic_score=46.0,
                novelty_score=0.0,
                selection_status="rejected",
                rejection_reason="duplicate_signature",
                critique=["same signature as an earlier draft"],
            ),
            SearchDraftEvaluation(
                draft_id="draft-ema-04",
                name="EMA Draft 04",
                template_hint="EMAStrategy",
                generation=1,
                heuristic_score=42.0,
                novelty_score=0.58,
                selection_status="rejected",
                rejection_reason="budget_trimmed",
            ),
        ],
        notes=["accepted=2", "rejected=2", "champion=draft-ema-01"],
    )


def test_generate_research_proposal_populates_phase3_search_summary():
    req = PlannerGenerateRequest(
        goal="Research autonomous EMA crossover variations on BTC/USDT",
        market_regime="trend_up",
        symbols=["BTCUSDT"],
        timeframes=["1h"],
        constraints={
            "max_strategy_drafts": 3,
            "max_backtest_runs": 24,
            "exploration_bias": 0.55,
        },
        llm_research_output={
            "hypothesis": "EMA crossover families may capture BTC continuation with controlled exits.",
            "proposed_strategy_changes": [
                {
                    "strategy": "EMAStrategy",
                    "name": "EMA Search Seed",
                    "thesis": "Use EMA crossover as a Phase 3 seed.",
                    "features": ["ema_fast", "ema_slow"],
                    "entry_logic": ["cross_over(ema_fast, ema_slow)"],
                    "exit_logic": ["cross_under(ema_fast, ema_slow)"],
                    "params": {"fast_period": 8, "slow_period": 21},
                    "confidence": 0.68,
                }
            ],
        },
    )

    out = generate_research_proposal(req, actor="pytest")
    proposal = out.proposal

    assert proposal.research_mode == "hybrid"
    assert proposal.search_summary is not None
    assert proposal.search_summary.loop_enabled is True
    assert proposal.search_summary.evaluated_drafts >= proposal.search_summary.accepted_drafts >= 1
    assert proposal.search_summary.champion_draft_id
    assert proposal.metadata["autonomy_summary"]["search_loop_enabled"] is True
    assert proposal.metadata["autonomy_summary"]["accepted_drafts"] == proposal.search_summary.accepted_drafts
    assert proposal.strategy_drafts
    assert proposal.strategy_drafts[0].selection_status == "champion"


def test_run_research_search_loop_rejects_duplicate_signatures():
    budget = ResearchSearchBudget(
        max_templates=2,
        max_strategy_drafts=2,
        max_backtest_runs=16,
        exploration_bias=0.45,
    )
    base_drafts = [
        _make_draft("draft-ema-01", name="EMA Base A", params={"fast_period": 8, "slow_period": 21}),
        _make_draft("draft-ema-02", name="EMA Base B", params={"fast_period": 8, "slow_period": 21}),
    ]

    accepted, summary = run_research_search_loop(
        goal="Search duplicate rejection coverage",
        selected_templates=["EMAStrategy"],
        base_drafts=base_drafts,
        search_budget=budget,
        enabled=True,
    )

    assert accepted
    assert summary.rejected_reason_counts.get("duplicate_signature", 0) >= 1
    assert any(row.rejection_reason == "duplicate_signature" for row in summary.draft_evaluations)
    assert any(draft.selection_status == "champion" for draft in accepted)


def test_build_experiment_spec_propagates_phase3_search_summary():
    now = _now()
    search_summary = _make_search_summary()
    proposal = ResearchProposal(
        proposal_id="proposal-phase3-exp",
        created_at=now,
        updated_at=now,
        research_mode="hybrid",
        thesis="Propagate Phase 3 search loop metadata into experiment specs.",
        target_symbols=["BTC/USDT"],
        target_timeframes=["1h"],
        strategy_templates=["EMAStrategy"],
        strategy_drafts=[
            _make_draft("draft-ema-01", name="EMA Draft 01", params={"fast_period": 8, "slow_period": 21}),
            _make_draft(
                "draft-ema-02",
                name="EMA Draft 02",
                params={"fast_period": 13, "slow_period": 34},
                generation=1,
                parent_draft_id="draft-ema-01",
            ),
        ],
        search_budget=ResearchSearchBudget(
            max_templates=3,
            max_strategy_drafts=2,
            max_backtest_runs=24,
            exploration_bias=0.55,
        ),
        search_summary=search_summary,
    )
    config = ResearchConfig(
        exchange="binance",
        symbol="BTC/USDT",
        days=30,
        initial_capital=10000.0,
        timeframes=["1h"],
        strategies=["EMAStrategy"],
        commission_rate=0.0004,
        slippage_bps=2.0,
        parameter_space={"EMAStrategy": {"fast_period": [8, 13], "slow_period": [21, 34]}},
    )

    experiment = _build_experiment_spec(proposal, config, actor="pytest")

    assert experiment.search_summary is not None
    assert experiment.search_summary.loop_enabled is True
    assert experiment.search_summary.champion_draft_id == "draft-ema-01"
    assert experiment.search_summary.rejected_reason_counts["duplicate_signature"] == 1


def test_create_candidates_from_result_marks_champion_and_challenger_and_carries_search_summary():
    now = _now()
    search_summary = _make_search_summary()
    proposal = ResearchProposal(
        proposal_id="proposal-phase3-candidates",
        created_at=now,
        updated_at=now,
        research_mode="hybrid",
        thesis="Carry Phase 3 metadata into candidate ranking.",
        target_symbols=["BTC/USDT"],
        target_timeframes=["1h"],
        strategy_templates=["EMAStrategy", "MAStrategy"],
        strategy_drafts=[
            _make_draft("draft-ema-01", name="EMA Draft 01", params={"fast_period": 8, "slow_period": 21}),
            _make_draft(
                "draft-ema-02",
                name="EMA Draft 02",
                params={"fast_period": 13, "slow_period": 34},
                generation=1,
                parent_draft_id="draft-ema-01",
            ),
        ],
        search_summary=search_summary,
    )
    experiment = ExperimentSpec(
        experiment_id="exp-phase3-candidates",
        proposal_id=proposal.proposal_id,
        created_at=now,
        exchange="binance",
        symbol="BTC/USDT",
        timeframes=["1h"],
        strategies=["EMAStrategy", "MAStrategy"],
        search_summary=search_summary,
    )
    result = {
        "runs": 2,
        "valid_runs": 2,
        "quality_counts": {"ok": 2},
        "top_results": [
            {"strategy": "EMAStrategy", "timeframe": "1h", "score": 82.0},
            {"strategy": "MAStrategy", "timeframe": "1h", "score": 74.0},
        ],
        "best_per_strategy": {
            "EMAStrategy": {
                "strategy": "EMAStrategy",
                "timeframe": "1h",
                "total_return": 18.0,
                "gross_total_return": 19.5,
                "cost_drag_return_pct": 1.5,
                "sharpe_ratio": 1.8,
                "max_drawdown": 5.0,
                "win_rate": 58.0,
                "total_trades": 24,
                "anomaly_bar_ratio": 0.0,
                "quality_flag": "ok",
                "score": 82.0,
            },
            "MAStrategy": {
                "strategy": "MAStrategy",
                "timeframe": "1h",
                "total_return": 11.0,
                "gross_total_return": 12.2,
                "cost_drag_return_pct": 1.2,
                "sharpe_ratio": 1.1,
                "max_drawdown": 7.0,
                "win_rate": 52.0,
                "total_trades": 18,
                "anomaly_bar_ratio": 0.0,
                "quality_flag": "ok",
                "score": 74.0,
            },
        },
        "csv_path": "phase3.csv",
        "markdown_path": "phase3.md",
    }

    _, candidates, best_candidate = _create_candidates_from_result(proposal, experiment, result)

    assert len(candidates) == 2
    assert best_candidate is not None
    assert best_candidate.metadata["search_role"] == "champion"
    assert best_candidate.metadata["champion_candidate_id"] == best_candidate.candidate_id
    assert best_candidate.metadata["search_summary"]["champion_draft_id"] == "draft-ema-01"

    challenger = next(candidate for candidate in candidates if candidate.candidate_id != best_candidate.candidate_id)
    assert challenger.metadata["search_role"] == "challenger"
    assert challenger.metadata["champion_candidate_id"] == best_candidate.candidate_id
    assert challenger.metadata["champion_strategy"] == best_candidate.strategy
