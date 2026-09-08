from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
import asyncio
import pytest

from fastapi import FastAPI

from config.settings import settings
from core.ai.proposal_schemas import ResearchProposal
from core.ai.research_planner import PlannerGenerateRequest, generate_research_proposal
from core.ai.research_scheduler import ResearchScheduler
from core.monitoring.cusum_watcher import _auto_draft_replacement
from core.research.orchestrator import ensure_ai_research_runtime_state
from scripts.quarantine_research_test_data import quarantine_plan


def test_unconfigured_app_research_is_isolated(tmp_path):
    app = FastAPI()
    ensure_ai_research_runtime_state(app)
    assert Path(app.state.ai_proposal_registry.path).is_relative_to(tmp_path)
    assert Path(settings.DATA_STORAGE_PATH).is_relative_to(tmp_path)


def test_replacement_is_deduplicated_queued_and_keeps_candidate_market():
    app = FastAPI()
    candidate = SimpleNamespace(candidate_id="real-parent", strategy="MAStrategy", symbol="ETH/USDT", timeframe="5m")
    _auto_draft_replacement(app, candidate, {"decay_pct": -8})
    _auto_draft_replacement(app, candidate, {"decay_pct": -9})
    proposals = app.state.ai_proposal_registry.list(limit=None)
    assert len(proposals) == 1
    proposal = proposals[0]
    assert proposal.status == "research_queued"
    assert proposal.target_symbols == ["ETH/USDT"]
    assert proposal.target_timeframes == ["5m"]
    assert proposal.source == "rule"
    assert proposal.metadata["llm_used"] is False
    assert len(proposal.strategy_templates) > 1
    assert any(name != "MAStrategy" for name in proposal.strategy_templates)
    restarted = FastAPI()
    ensure_ai_research_runtime_state(restarted)
    assert restarted.state.ai_proposal_registry.get(proposal.proposal_id).status == "research_queued"


def test_template_planner_does_not_claim_llm_use():
    output = generate_research_proposal(PlannerGenerateRequest(goal="compare strategies"))
    assert output.proposal.source == "rule"
    assert output.proposal.metadata["generation_method"] == "rule_template"
    assert output.proposal.metadata["llm_used"] is False


def test_mixed_research_preserves_both_preferred_families():
    from core.ai.research_planner import _ensure_family_diversity

    templates = ["MAStrategy", "EMAStrategy", "ADXTrendStrategy", "AroonStrategy", "MACDStrategy"]
    selected = _ensure_family_diversity(templates, "mixed", 5)
    assert len(selected) == len(set(selected)) == 5
    assert "MLXGBoostStrategy" in selected
    assert "MarketSentimentStrategy" in selected
    assert selected[:3] == templates[:3]
    assert len(_ensure_family_diversity(templates, "mixed", 1)) == 1
    assert _ensure_family_diversity(templates, "mixed", 0) == []


def test_orphan_research_strategy_cannot_restore():
    from core.strategies.persistence import _research_restore_issue

    payload = {"metadata": {"source": "ai_research", "candidate_id": "missing", "proposal_id": "p", "experiment_id": "e"}}
    assert _research_restore_issue(payload, {}) == "ai_research_candidate_missing"
    assert _research_restore_issue({"metadata": {"source": "manual"}}, {}) is None
    candidate = SimpleNamespace(status="paper_running", proposal_id="p", experiment_id="e")
    assert _research_restore_issue(payload, {"missing": candidate}) is None
    candidate.status = "retired"
    assert _research_restore_issue(payload, {"missing": candidate}) == "ai_research_candidate_not_running"


def test_scheduler_dispatches_old_queue_beyond_fifty_drafts(monkeypatch):
    app = FastAPI()
    ensure_ai_research_runtime_state(app)
    now = datetime.now(timezone.utc)
    rows = [ResearchProposal(proposal_id=f"draft-{i}", created_at=now, updated_at=now, thesis="draft") for i in range(60)]
    rows.append(ResearchProposal(proposal_id="old-queued", created_at=now.replace(year=2020), updated_at=now.replace(year=2020), thesis="queued", status="research_queued"))
    app.state.ai_proposal_registry.save_many(rows)
    runner = AsyncMock()
    monkeypatch.setattr("core.research.orchestrator.run_proposal", runner)
    scheduler = ResearchScheduler()
    scheduler.set_app(app)
    asyncio.run(scheduler._tick())
    runner.assert_awaited_once()
    assert runner.call_args.kwargs["proposal_id"] == "old-queued"


def test_quarantine_follows_fixture_links_but_preserves_real_ma():
    data = {
        "candidates.json": {"candidates": [
            {"candidate_id": "fixture", "proposal_id": "p-test", "experiment_id": "e-test", "metadata": {"csv_path": "research_ops.csv"}},
            {"candidate_id": "real", "proposal_id": "p-real", "experiment_id": "e-real", "strategy": "MAStrategy", "metadata": {"csv_path": "real.csv"}},
        ]},
        "proposals.json": {"proposals": [
            {"proposal_id": "p-test"}, {"proposal_id": "p-real"},
            {"proposal_id": "replacement", "metadata": {"parent_candidate_id": "fixture"}},
        ]},
        "experiment_runs.json": {"runs": [
            {"run_id": "r-test", "experiment_id": "e-test"},
            {"run_id": "r-real", "experiment_id": "e-real"},
        ]},
    }
    cleaned, report = quarantine_plan(data)
    assert [row["candidate_id"] for row in cleaned["candidates.json"]["candidates"]] == ["real"]
    assert [row["proposal_id"] for row in cleaned["proposals.json"]["proposals"]] == ["p-real"]
    assert report["experiment_runs.json"]["quarantined"] == 1
    repeated, second_report = quarantine_plan(cleaned)
    assert repeated == cleaned
    assert all(row["quarantined"] == 0 for row in second_report.values())


def test_funding_warm_keeps_event_loop_responsive(monkeypatch):
    import threading
    import pandas as pd
    from web.api import ai_research

    heartbeat = threading.Event()
    observed = []

    def slow_fetch(*args, **kwargs):
        observed.append(heartbeat.wait(timeout=1))
        return pd.Series(dtype=float)

    monkeypatch.setattr(ai_research, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_research, "FundingRateProvider", lambda config: SimpleNamespace(ensure_history=slow_fetch))
    monkeypatch.setattr(ai_research, "_serialize_funding_cache", lambda *args, **kwargs: {})

    async def run():
        async def tick():
            await asyncio.sleep(0.01)
            heartbeat.set()
        result, _ = await asyncio.gather(
            ai_research.warm_ai_funding_cache(SimpleNamespace(app=FastAPI()), ai_research.AIFundingWarmRequest()),
            tick(),
        )
        assert result["warmed"] is False

    asyncio.run(run())
    assert observed == [True]


@pytest.mark.parametrize("age_hours, expected_health, ready", [
    (None, "missing", False),
    (72, "stale", False),
    (8, "healthy", True),
])
def test_funding_warm_reports_data_freshness(monkeypatch, tmp_path, age_hours, expected_health, ready):
    import pandas as pd
    from web.api import ai_research

    series = pd.Series(dtype=float) if age_hours is None else pd.Series(
        [0.0001], index=[pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=age_hours)],
    )
    provider = SimpleNamespace(
        ensure_history=lambda *args, **kwargs: series,
        _cache_path=lambda *args, **kwargs: tmp_path / "funding.parquet",
    )
    monkeypatch.setattr(ai_research, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(ai_research, "FundingRateProvider", lambda config: provider)
    result = asyncio.run(ai_research.warm_ai_funding_cache(
        SimpleNamespace(app=FastAPI()), ai_research.AIFundingWarmRequest(),
    ))
    assert result["warmed"] is ready
    assert result["funding"]["ready"] is ready
    assert result["funding"]["health"] == expected_health
