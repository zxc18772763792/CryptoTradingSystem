import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from core.ai.proposal_schemas import ResearchProposal
from core.research.experiment_registry import (
    CandidateRegistry,
    ExperimentRegistry,
    ExperimentRunRegistry,
    LifecycleRegistry,
    ProposalRegistry,
)
from core.research.experiment_schemas import ExperimentRun, ExperimentSpec
from core.research.orchestrator import _finalize_research_run
from core.research.strategy_research import ResearchConfig


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_llm_rationale_timeout_cancels_inner_tasks(tmp_path, monkeypatch):
    import core.research.orchestrator as orchestrator
    import core.ai.promotion_narrator as promotion_narrator

    cancelled = asyncio.Event()

    async def _slow_rationale(*_args, **_kwargs):
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async def _research_result(_config, progress_callback=None):
        if progress_callback is not None:
            progress_callback({"completed": 1, "total": 1, "strategy": "MAStrategy", "timeframe": "1h"})
        return {
            "runs": 1,
            "valid_runs": 1,
            "quality_counts": {"ok": 1},
            "best": {
                "strategy": "MAStrategy",
                "timeframe": "1h",
                "total_return": 12.0,
                "gross_total_return": 13.0,
                "cost_drag_return_pct": 1.0,
                "sharpe_ratio": 2.2,
                "max_drawdown": 3.0,
                "win_rate": 60.0,
                "total_trades": 24,
                "score": 95.0,
                "quality_flag": "ok",
            },
            "best_per_strategy": {
                "MAStrategy": {
                    "strategy": "MAStrategy",
                    "timeframe": "1h",
                    "total_return": 12.0,
                    "gross_total_return": 13.0,
                    "cost_drag_return_pct": 1.0,
                    "sharpe_ratio": 2.2,
                    "max_drawdown": 3.0,
                    "win_rate": 60.0,
                    "total_trades": 24,
                    "score": 95.0,
                    "quality_flag": "ok",
                }
            },
        }

    real_wait_for = orchestrator.asyncio.wait_for

    async def _short_wait_for(awaitable, timeout):
        return await real_wait_for(awaitable, timeout=0.01 if timeout == 30.0 else timeout)

    monkeypatch.setattr(orchestrator, "run_strategy_research", _research_result)
    monkeypatch.setattr(orchestrator, "governance_propose_strategy", None)
    monkeypatch.setattr(orchestrator, "_refresh_runtime_eligibility_snapshot_safe", lambda *, reason: None)
    monkeypatch.setattr(orchestrator.asyncio, "wait_for", _short_wait_for)
    monkeypatch.setattr(promotion_narrator, "generate_promotion_rationale", _slow_rationale)

    now = datetime.now(timezone.utc)
    proposal = ResearchProposal(
        proposal_id="proposal-timeout",
        created_at=now,
        updated_at=now,
        status="research_running",
        source="ai",
        thesis="timeout test",
        target_symbols=["BTC/USDT"],
        target_timeframes=["1h"],
        strategy_templates=["MAStrategy"],
    )
    experiment = ExperimentSpec(
        experiment_id="experiment-timeout",
        proposal_id=proposal.proposal_id,
        created_at=now,
        exchange="binance",
        symbol="BTC/USDT",
        timeframes=["1h"],
        strategies=["MAStrategy"],
        status="queued",
    )
    run = ExperimentRun(run_id="run-timeout", experiment_id=experiment.experiment_id, status="queued")

    app = SimpleNamespace(
        state=SimpleNamespace(
            ai_proposal_registry=ProposalRegistry(tmp_path / "proposals.json"),
            ai_experiment_registry=ExperimentRegistry(tmp_path / "experiments.json"),
            ai_experiment_run_registry=ExperimentRunRegistry(tmp_path / "runs.json"),
            ai_candidate_registry=CandidateRegistry(tmp_path / "candidates.json"),
            ai_lifecycle_registry=LifecycleRegistry(tmp_path / "lifecycle.json"),
            research_jobs={},
        )
    )
    app.state.ai_proposal_registry.save(proposal)
    app.state.ai_experiment_registry.save(experiment)
    app.state.ai_experiment_run_registry.save(run)

    config = ResearchConfig(
        exchange="binance",
        symbol="BTC/USDT",
        timeframes=["1h"],
        strategies=["MAStrategy"],
        days=30,
        initial_capital=10000.0,
    )

    await _finalize_research_run(
        app,
        proposal_id=proposal.proposal_id,
        experiment_id=experiment.experiment_id,
        run_id=run.run_id,
        request_payload={},
        config=config,
        actor="test",
        job_id=None,
    )

    assert cancelled.is_set()
    assert not [task for task in asyncio.all_tasks() if task.get_name().startswith("llm_rationale_")]
