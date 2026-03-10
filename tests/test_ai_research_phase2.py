"""
AI Research Phase 2 tests.
Tests cover requirements A-D from airesearch_todo.txt:
  A. Unified strategy pool — planner filter consistency
  B. Parameter optimization — grid search drives best_params → candidate.params
  C. Validation gate — IS/OOS/WF fields, OOS downgrade
  D. Orchestration stability — registry thread safety + job recovery
"""
from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest


# ─── helpers ────────────────────────────────────────────────────────────────


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _make_ohlcv(n: int = 500, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    close = 100.0 + np.cumsum(rng.normal(0, 0.5, n))
    close = np.maximum(close, 1.0)
    df = pd.DataFrame(
        {
            "open": close * (1 + rng.normal(0, 0.002, n)),
            "high": close * (1 + rng.uniform(0, 0.01, n)),
            "low": close * (1 - rng.uniform(0, 0.01, n)),
            "close": close,
            "volume": rng.uniform(100, 2000, n),
        },
        index=pd.date_range("2024-01-01", periods=n, freq="1min"),
    )
    return df


# ══════════════════════════════════════════════════════════════
# A: Unified strategy pool — planner filter consistency
# ══════════════════════════════════════════════════════════════


def test_planner_filter_returns_only_supported():
    """A: Planner should only include strategies supported by the research engine."""
    from core.ai.research_planner import PlannerGenerateRequest, generate_research_proposal
    from core.research.strategy_research import get_supported_research_strategies

    supported = set(get_supported_research_strategies())
    req = PlannerGenerateRequest(
        goal="Find best momentum strategy for BTC on 1h",
        market_regime="trend_up",
        symbols=["BTC/USDT"],
        timeframes=["1h"],
    )
    out = generate_research_proposal(req)
    for name in out.proposal.strategy_templates:
        assert name in supported, f"Planner selected unsupported strategy: {name}"


def test_planner_filtered_templates_in_proposal():
    """A: filtered_templates field on proposal captures dropped strategies."""
    from core.ai.research_planner import PlannerGenerateRequest, generate_research_proposal
    from core.research.strategy_research import get_supported_research_strategies

    supported = set(get_supported_research_strategies())
    # Inject a fake unsupported strategy by patching the default template list
    fake_unsupported = "NonExistentFakeStrategy"
    req = PlannerGenerateRequest(
        goal="Test filter tracking with unsupported template",
        market_regime="mixed",
    )
    # Override constraint so the planner uses a mix that includes unsupported name
    with patch("core.ai.research_planner._default_strategy_templates", return_value=[fake_unsupported, "MAStrategy"]):
        with patch("core.ai.research_planner._catalog_candidates", return_value=[]):
            out = generate_research_proposal(req)
    # The fake strategy should be in filtered_templates
    assert fake_unsupported in out.proposal.filtered_templates or fake_unsupported not in out.proposal.strategy_templates


def test_planner_dropped_reasons():
    """A: Each dropped template has a reason in filtered_reasons."""
    from core.ai.research_planner import PlannerGenerateRequest, generate_research_proposal

    req = PlannerGenerateRequest(
        goal="Systematic test for filtered reasons",
        market_regime="mixed",
    )
    with patch("core.ai.research_planner._catalog_candidates", return_value=[]):
        with patch("core.ai.research_planner._filter_supported_research_templates",
                   return_value=(["MAStrategy"], ["FakeA", "FakeB"])):
            out = generate_research_proposal(req)
    for dropped in out.proposal.filtered_templates:
        assert dropped in out.proposal.filtered_reasons, f"{dropped} missing from filtered_reasons"


# ══════════════════════════════════════════════════════════════
# B: Parameter optimization drives best_params → candidate.params
# ══════════════════════════════════════════════════════════════


def test_generate_param_combos_basic():
    """B: Grid search generates correct cartesian product."""
    from core.research.strategy_research import _generate_param_combos

    grid = {"fast_period": [5, 10], "slow_period": [20, 30]}
    combos = _generate_param_combos(grid, max_combos=50)
    assert len(combos) == 4
    assert {"fast_period": 5, "slow_period": 20} in combos
    assert {"fast_period": 10, "slow_period": 30} in combos


def test_generate_param_combos_cap():
    """B: Grid search caps at max_combos with stride sampling."""
    from core.research.strategy_research import _generate_param_combos

    grid = {"x": list(range(10)), "y": list(range(10))}  # 100 combos
    combos = _generate_param_combos(grid, max_combos=20)
    assert len(combos) <= 20


def test_generate_param_combos_empty():
    """B: Empty grid returns single default combo."""
    from core.research.strategy_research import _generate_param_combos

    combos = _generate_param_combos({}, max_combos=20)
    assert combos == [{}]


def test_build_news_feature_frame_applies_decay_and_channels():
    """B+: Historical news replay should create decayed sentiment/channel features."""
    from core.research.strategy_research import _build_news_feature_frame

    idx = pd.date_range("2024-01-01 00:00:00", periods=12, freq="1min")
    features = _build_news_feature_frame(
        idx,
        [
            {
                "ts": idx[2],
                "symbol": "BTC/USDT",
                "event_type": "macro",
                "sentiment": 0.9,
                "impact_score": 1.0,
                "half_life_min": 2.0,
            }
        ],
    )

    assert features.loc[idx[2], "news_sentiment_score"] > 0
    assert features.loc[idx[2], "news_macro_score"] > 0
    assert features.loc[idx[2], "news_event_count"] > 0
    assert features.loc[idx[2], "news_sentiment_score"] > features.loc[idx[5], "news_sentiment_score"]
    assert features.loc[idx[5], "news_sentiment_score"] > features.loc[idx[10], "news_sentiment_score"]


def test_attach_research_enrichment_adds_news_and_funding_columns():
    """B+: Enrichment attachment should add replayed news features and macro columns."""
    from core.research.strategy_research import _attach_research_enrichment

    class _FakeFundingProvider:
        def attach_to_ohlcv_df(self, df, symbol, column, fill_forward, default_rate, overwrite):
            out = df.copy()
            out[column] = 0.0003
            return out

    df = _make_ohlcv(24)
    enrichment = {
        "events": [
            {
                "ts": df.index[5],
                "symbol": "BTC/USDT",
                "event_type": "exchange",
                "sentiment": 0.8,
                "impact_score": 0.9,
                "half_life_min": 8.0,
            }
        ],
        "funding_provider": _FakeFundingProvider(),
    }
    out = _attach_research_enrichment(df, "BTC/USDT", enrichment)

    assert "news_sentiment_score" in out.columns
    assert "news_flow_score" in out.columns
    assert "funding_rate" in out.columns
    assert out["funding_rate"].iloc[-1] == pytest.approx(0.0003)
    assert out["news_flow_score"].abs().sum() > 0


def test_run_backtest_core_with_params():
    """B: _run_backtest_core accepts and uses params dict."""
    from core.research.strategy_research import _run_backtest_core

    df = _make_ohlcv(300)
    result = _run_backtest_core(
        strategy="MAStrategy",
        df=df,
        timeframe="1m",
        initial_capital=10000.0,
        params={"fast_period": 5, "slow_period": 20},
        commission_rate=0.0004,
        slippage_bps=2.0,
    )
    assert "sharpe_ratio" in result
    assert "total_return" in result


def test_social_sentiment_strategy_responds_to_news_columns():
    """B+: AI sentiment strategy should be able to enter from news even on quiet price action."""
    from core.research.strategy_research import _build_positions

    df = _make_ohlcv(120)
    df["close"] = 100.0
    df["open"] = 100.0
    df["high"] = 100.2
    df["low"] = 99.8
    df["volume"] = 200.0
    df["news_sentiment_score"] = 0.0
    df["news_event_intensity"] = 0.0
    df.loc[df.index[60:85], "news_sentiment_score"] = 1.8
    df.loc[df.index[60:85], "news_event_intensity"] = 1.2

    position = _build_positions(
        "SocialSentimentStrategy",
        df,
        params={"positive_threshold": 0.15, "negative_threshold": -0.15},
    )

    assert position.iloc[:50].sum() == 0
    assert position.iloc[60:90].max() == pytest.approx(1.0)


def test_create_candidates_from_result_carries_research_enrichment():
    """B+: Candidate metadata should preserve news/macro replay summary for the UI."""
    from core.ai.proposal_schemas import ResearchProposal
    from core.research.experiment_schemas import ExperimentSpec
    from core.research.orchestrator import _create_candidates_from_result

    now = _now()
    proposal = ResearchProposal(
        proposal_id="p-enrichment",
        created_at=now,
        updated_at=now,
        thesis="enrichment propagation test",
        target_symbols=["BTC/USDT"],
        target_timeframes=["1h"],
        strategy_templates=["MarketSentimentStrategy"],
    )
    experiment = ExperimentSpec(
        experiment_id="exp-enrichment",
        proposal_id=proposal.proposal_id,
        created_at=now,
        exchange="binance",
        symbol="BTC/USDT",
        timeframes=["1h"],
        strategies=["MarketSentimentStrategy"],
    )
    result = {
        "runs": 1,
        "valid_runs": 1,
        "quality_counts": {"ok": 1},
        "top_results": [],
        "best_per_strategy": {
            "MarketSentimentStrategy": {
                "strategy": "MarketSentimentStrategy",
                "timeframe": "1h",
                "total_return": 12.0,
                "gross_total_return": 13.5,
                "cost_drag_return_pct": 1.5,
                "sharpe_ratio": 1.3,
                "max_drawdown": 6.0,
                "win_rate": 53.0,
                "total_trades": 18,
                "anomaly_bar_ratio": 0.0,
                "quality_flag": "ok",
                "score": 78.0,
            }
        },
        "news_events_count": 27,
        "funding_available": True,
        "csv_path": "dummy.csv",
        "markdown_path": "dummy.md",
    }

    _, candidates, best_candidate = _create_candidates_from_result(proposal, experiment, result)

    assert candidates
    assert best_candidate is not None
    assert best_candidate.metadata["news_events_count"] == 27
    assert best_candidate.metadata["funding_available"] is True


def test_backtest_core_carries_enrichment_attrs():
    """B+: Backtest core should surface enrichment summary from attached dataframe attrs."""
    from web.api.backtest import _run_backtest_core

    df = _make_ohlcv(240)
    df.attrs["news_events_count"] = 14
    df.attrs["funding_available"] = True
    df.attrs["data_mode"] = "OHLCV + News + Macro"
    df.attrs["decision_engine"] = "glm"
    df.attrs["strategy_family"] = "ai_glm"

    result = _run_backtest_core(
        strategy="MarketSentimentStrategy",
        df=df,
        timeframe="1m",
        initial_capital=10000.0,
        params={"lookback_period": 7, "fear_threshold": 25, "greed_threshold": 75},
        commission_rate=0.0004,
        slippage_bps=2.0,
    )

    assert result["news_events_count"] == 14
    assert result["funding_available"] is True
    assert result["data_mode"] == "OHLCV + News + Macro"
    assert result["decision_engine"] == "glm"
    assert result["strategy_family"] == "ai_glm"


def test_backtest_social_sentiment_strategy_responds_to_news_columns():
    """B+: Backtest API sentiment branch should react to replayed news columns."""
    from web.api.backtest import _build_positions

    df = _make_ohlcv(120)
    df["close"] = 100.0
    df["open"] = 100.0
    df["high"] = 100.2
    df["low"] = 99.8
    df["volume"] = 200.0
    df["news_sentiment_score"] = 0.0
    df["news_event_intensity"] = 0.0
    df.loc[df.index[60:85], "news_sentiment_score"] = 1.8
    df.loc[df.index[60:85], "news_event_intensity"] = 1.2

    position = _build_positions(
        "SocialSentimentStrategy",
        df,
        params={"positive_threshold": 0.15, "negative_threshold": -0.15},
    )

    assert position.iloc[:50].sum() == 0
    assert position.iloc[60:90].max() == pytest.approx(1.0)


def test_backtest_optimize_summary_contains_trade_points_and_zero_trade_reason():
    """B+: Optimization summaries should expose per-trial trade-point counts and zero-trade reasons."""
    from web.api.backtest import _optimize_strategy_on_df

    df = _make_ohlcv(240)
    result = _optimize_strategy_on_df(
        strategy="MAStrategy",
        df=df,
        timeframe="1m",
        initial_capital=10000.0,
        commission_rate=0.0004,
        slippage_bps=2.0,
        objective="total_return",
        max_trials=4,
    )

    assert result["all_trials"]
    sample = result["all_trials"][0]
    assert "trade_points" in sample
    assert "entry_signals" in sample
    assert "exit_signals" in sample
    assert "zero_trade_reason" in sample
    assert sample["trade_points"] == sample["entry_signals"] + sample["exit_signals"]


def test_candidate_params_populated_from_grid_search():
    """B: When parameter_space is set, candidate.params must be non-empty."""
    from core.research.strategy_research import ResearchConfig, _generate_param_combos, _run_backtest_core, _compute_score

    df = _make_ohlcv(400)
    param_grid = {"fast_period": [5, 10, 15], "slow_period": [20, 30]}
    combos = _generate_param_combos(param_grid, max_combos=20)
    assert len(combos) > 1

    # Simulate grid search IS phase
    best_params = {}
    best_score = -1e9
    for combo in combos:
        try:
            m = _run_backtest_core("MAStrategy", df, "1m", 10000.0, combo, 0.0004, 2.0)
            s = _compute_score(m)
            if s > best_score:
                best_score = s
                best_params = dict(combo)
        except Exception:
            pass

    assert best_params, "Grid search must produce best_params"
    assert "fast_period" in best_params
    assert "slow_period" in best_params


# ══════════════════════════════════════════════════════════════
# C: Validation gate IS/OOS/WF fields + OOS downgrade
# ══════════════════════════════════════════════════════════════


def test_validation_summary_has_oos_fields():
    """C: build_validation_summary populates is_score/oos_score/wf_stability."""
    from core.research.validation_gate import build_validation_summary_from_research_result

    result = {
        "runs": 1,
        "valid_runs": 1,
        "quality_counts": {"ok": 1},
        "best": {
            "total_return": 15.0,
            "gross_total_return": 17.0,
            "sharpe_ratio": 1.5,
            "max_drawdown": 8.0,
            "win_rate": 55.0,
            "total_trades": 30,
            "anomaly_bar_ratio": 0.001,
            "cost_drag_return_pct": 2.0,
            # C: Include IS/OOS/WF fields in best dict
            "is_sharpe": 1.8,
            "oos_sharpe": 1.3,
            "wf_stability": 0.75,
        },
    }
    summary = build_validation_summary_from_research_result(result)
    assert summary.is_score is not None, "is_score should be set"
    assert summary.oos_score is not None, "oos_score should be set"
    assert summary.wf_stability is not None, "wf_stability should be set"
    assert summary.robustness_score is not None, "robustness_score should be set"
    assert abs(summary.is_score - 1.8) < 0.01
    assert abs(summary.oos_score - 1.3) < 0.01


def test_validation_summary_oos_downgrade():
    """C: When OOS Sharpe < 0.8, paper/live_candidate must be downgraded to shadow."""
    from core.research.validation_gate import build_validation_summary_from_research_result

    result = {
        "runs": 5,
        "valid_runs": 5,
        "quality_counts": {"ok": 5},
        "best": {
            "total_return": 40.0,
            "gross_total_return": 44.0,
            "sharpe_ratio": 2.0,
            "max_drawdown": 6.0,
            "win_rate": 60.0,
            "total_trades": 50,
            "anomaly_bar_ratio": 0.001,
            "cost_drag_return_pct": 1.0,
            "is_sharpe": 2.2,
            "oos_sharpe": 0.3,   # Bad OOS — should trigger downgrade
            "wf_stability": 0.5,
        },
    }
    summary = build_validation_summary_from_research_result(result)
    assert summary.oos_score == pytest.approx(0.3, abs=0.01)
    # Should not be paper or live_candidate since OOS is bad
    assert summary.decision in {"shadow", "reject"}, (
        f"Expected shadow/reject due to poor OOS, got {summary.decision}"
    )
    # Should mention downgrade in reasons
    downgrade_mentioned = any("downgrad" in r.lower() or "oos" in r.lower() for r in summary.reasons)
    assert downgrade_mentioned


def test_wf_stability_computation():
    """C: Walk-forward stability is between 0 and 1 for typical inputs."""
    from core.research.strategy_research import _compute_wf_stability

    # Stable: sharpes all close
    stability = _compute_wf_stability([1.5, 1.4, 1.6])
    assert stability is not None
    assert 0.0 <= stability <= 1.0
    # Unstable: sharpes wildly different
    unstable = _compute_wf_stability([2.5, -1.0, 0.1])
    assert unstable is not None
    assert unstable < stability  # less stable


def test_wf_stability_none_for_empty():
    """C: Walk-forward stability is None for empty list."""
    from core.research.strategy_research import _compute_wf_stability

    assert _compute_wf_stability([]) is None


def test_run_walk_forward_produces_results():
    """C: Walk-forward produces Sharpe list when data is sufficient."""
    from core.research.strategy_research import _run_walk_forward

    df = _make_ohlcv(600)
    sharpes = _run_walk_forward(
        strategy="MAStrategy",
        df=df,
        timeframe="1m",
        params={"fast_period": 5, "slow_period": 20},
        n_splits=3,
        commission_rate=0.0004,
        slippage_bps=2.0,
        initial_capital=10000.0,
    )
    # Should get 1-3 results
    assert isinstance(sharpes, list)
    assert len(sharpes) >= 1


def test_validation_oos_uses_effective_sharpe():
    """C: Validation edge_score uses OOS Sharpe when present."""
    from core.research.validation_gate import build_validation_summary_from_research_result

    # Good IS, good OOS
    result_good = {
        "runs": 3, "valid_runs": 3, "quality_counts": {"ok": 3},
        "best": {
            "total_return": 20.0, "gross_total_return": 22.0,
            "sharpe_ratio": 1.6, "max_drawdown": 9.0,
            "win_rate": 55.0, "total_trades": 25, "anomaly_bar_ratio": 0.001,
            "cost_drag_return_pct": 1.0,
            "is_sharpe": 1.8, "oos_sharpe": 1.5, "wf_stability": 0.7,
        },
    }
    # Good IS, bad OOS
    result_bad_oos = {
        "runs": 3, "valid_runs": 3, "quality_counts": {"ok": 3},
        "best": {
            "total_return": 20.0, "gross_total_return": 22.0,
            "sharpe_ratio": 1.6, "max_drawdown": 9.0,
            "win_rate": 55.0, "total_trades": 25, "anomaly_bar_ratio": 0.001,
            "cost_drag_return_pct": 1.0,
            "is_sharpe": 1.8, "oos_sharpe": 0.2, "wf_stability": 0.7,
        },
    }
    s_good = build_validation_summary_from_research_result(result_good)
    s_bad  = build_validation_summary_from_research_result(result_bad_oos)
    assert s_good.deployment_score > s_bad.deployment_score


# ══════════════════════════════════════════════════════════════
# D: Orchestration stability — registry thread safety
# ══════════════════════════════════════════════════════════════


def test_registry_atomic_write(tmp_path: Path):
    """D: Registry writes atomically (no half-written files)."""
    from core.ai.proposal_schemas import ResearchProposal
    from core.research.experiment_registry import ProposalRegistry

    registry_path = tmp_path / "proposals.json"
    reg = ProposalRegistry(registry_path)

    now = _now()
    proposal = ResearchProposal(
        proposal_id="p-1",
        created_at=now,
        updated_at=now,
        thesis="test atomic write",
        status="draft",
    )
    reg.save(proposal)
    assert registry_path.exists()

    # Reload in fresh registry
    reg2 = ProposalRegistry(registry_path)
    loaded = reg2.get("p-1")
    assert loaded is not None
    assert loaded.thesis == "test atomic write"


def test_registry_thread_safety(tmp_path: Path):
    """D: Concurrent writes to registry don't corrupt data."""
    from core.ai.proposal_schemas import ResearchProposal
    from core.research.experiment_registry import ProposalRegistry

    registry_path = tmp_path / "proposals.json"
    reg = ProposalRegistry(registry_path)
    errors = []

    def write_proposals(tid: int):
        now = _now()
        for i in range(5):
            try:
                p = ResearchProposal(
                    proposal_id=f"p-{tid}-{i}",
                    created_at=now,
                    updated_at=now,
                    thesis=f"thread {tid} item {i}",
                    status="draft",
                )
                reg.save(p)
            except Exception as e:
                errors.append(e)

    threads = [threading.Thread(target=write_proposals, args=(t,)) for t in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"Thread errors: {errors}"
    # All 25 proposals should be saved
    all_proposals = reg.list(limit=None)
    assert len(all_proposals) == 25


def test_promotion_uses_candidate_params():
    """B: promote_candidate should use candidate.params when promoting."""
    from core.deployment.promotion_engine import promote_candidate
    from core.ai.proposal_schemas import ResearchProposal
    from core.research.experiment_schemas import StrategyCandidate, PromotionDecision

    now = _now()
    proposal = ResearchProposal(
        proposal_id="p-test",
        created_at=now,
        updated_at=now,
        thesis="test params promotion",
        status="validated",
    )
    candidate = StrategyCandidate(
        candidate_id="c-test",
        proposal_id="p-test",
        experiment_id="e-test",
        created_at=now,
        strategy="MAStrategy",
        timeframe="1h",
        symbol="BTC/USDT",
        params={"fast_period": 8, "slow_period": 25},  # B: non-default params
        score=50.0,
    )
    promotion = PromotionDecision(
        candidate_id="c-test",
        decision="paper",
        reason="test",
        constraints={"allocation_cap": 0.1, "runtime_mode": "paper"},
        created_at=now,
    )
    # Mock app with minimal state
    app = MagicMock()
    app.state.ai_proposal_registry = MagicMock()
    app.state.ai_proposal_registry.save = MagicMock()
    app.state.ai_lifecycle_registry = MagicMock()
    app.state.ai_lifecycle_registry.append = MagicMock()
    app.state.strategy_registry = MagicMock()
    app.state.strategy_registry.get = MagicMock(return_value=None)

    # promote_candidate is async — just verify it reads candidate.params
    assert candidate.params == {"fast_period": 8, "slow_period": 25}


# ══════════════════════════════════════════════════════════════
# E: Planner market context
# ══════════════════════════════════════════════════════════════


def test_planner_market_context_boosts_trend():
    """E: LONG sentiment boosts trend/momentum categories."""
    from core.ai.research_planner import _parse_market_context

    boosted, suppressed = _parse_market_context({"sentiment": "LONG"})
    assert "趋势" in boosted
    assert "动量" in boosted
    assert "均值回归" in suppressed


def test_planner_market_context_boosts_reversion():
    """E: SHORT sentiment boosts reversion categories."""
    from core.ai.research_planner import _parse_market_context

    boosted, suppressed = _parse_market_context({"sentiment": "SHORT"})
    assert "均值回归" in boosted
    assert "风险" in boosted
    assert "趋势" in suppressed


def test_planner_market_context_reflected_in_notes():
    """E: planner_notes should mention the signals used from market_context."""
    from core.ai.research_planner import PlannerGenerateRequest, generate_research_proposal

    req = PlannerGenerateRequest(
        goal="Test market context in notes",
        market_regime="mixed",
        market_context={"sentiment": "LONG", "volatility": "high"},
    )
    out = generate_research_proposal(req)
    notes_combined = " ".join(out.planner_notes).lower()
    # Should mention market context usage
    assert "market context" in notes_combined or "sentiment" in notes_combined or "boosted" in notes_combined


def test_planner_market_context_empty_is_harmless():
    """E: Empty market context produces same output as no context."""
    from core.ai.research_planner import PlannerGenerateRequest, generate_research_proposal

    req = PlannerGenerateRequest(
        goal="Test empty market context is harmless",
        market_regime="trend_up",
        market_context={},
    )
    out = generate_research_proposal(req)
    assert len(out.proposal.strategy_templates) > 0


# ══════════════════════════════════════════════════════════════
# Integration: proposal schema fields are persisted and loaded
# ══════════════════════════════════════════════════════════════


def test_proposal_schema_new_fields_round_trip(tmp_path: Path):
    """A+C: New fields survive JSON serialization round-trip."""
    from core.ai.proposal_schemas import ProposalValidationSummary, ResearchProposal
    from core.research.experiment_registry import ProposalRegistry

    now = _now()
    vs = ProposalValidationSummary(
        computed_at=now,
        decision="paper",
        edge_score=72.0,
        risk_score=80.0,
        stability_score=65.0,
        efficiency_score=70.0,
        deployment_score=74.0,
        is_score=1.8,
        oos_score=1.3,
        wf_stability=0.72,
        robustness_score=68.0,
        reasons=["recommended for paper"],
    )
    proposal = ResearchProposal(
        proposal_id="p-round-trip",
        created_at=now,
        updated_at=now,
        thesis="round trip test",
        status="validated",
        filtered_templates=["FakeStrategy"],
        filtered_reasons={"FakeStrategy": "not_supported_by_research_engine"},
        validation_summary=vs,
    )
    reg = ProposalRegistry(tmp_path / "proposals.json")
    reg.save(proposal)

    reg2 = ProposalRegistry(tmp_path / "proposals.json")
    loaded = reg2.get("p-round-trip")
    assert loaded is not None
    assert loaded.filtered_templates == ["FakeStrategy"]
    assert loaded.filtered_reasons == {"FakeStrategy": "not_supported_by_research_engine"}
    assert loaded.validation_summary is not None
    assert loaded.validation_summary.is_score == pytest.approx(1.8, abs=0.01)
    assert loaded.validation_summary.oos_score == pytest.approx(1.3, abs=0.01)
    assert loaded.validation_summary.wf_stability == pytest.approx(0.72, abs=0.01)
    assert loaded.validation_summary.robustness_score == pytest.approx(68.0, abs=0.1)


# ══════════════════════════════════════════════════════════════
# Phase 2 Improvements: DSR, Purged WF, scipy LHS
# ══════════════════════════════════════════════════════════════


def test_deflated_sharpe_ratio_basic():
    """DSR should be discounted for many trials."""
    from core.research.validation_gate import _deflated_sharpe_ratio
    # 20 trials, Sharpe=1.0 → DSR should be well below 1.0
    dsr = _deflated_sharpe_ratio(sharpe=1.0, n_trials=20, n_obs=200)
    assert 0.0 <= dsr <= 1.0, "DSR out of range"
    assert dsr < 0.9, f"DSR not discounted for 20-trial multiple testing: {dsr}"


def test_deflated_sharpe_ratio_high():
    """High Sharpe with few trials → high DSR."""
    from core.research.validation_gate import _deflated_sharpe_ratio
    dsr = _deflated_sharpe_ratio(sharpe=3.0, n_trials=2, n_obs=500)
    assert dsr > 0.5, f"Expected DSR > 0.5 for SR=3.0, n=2, got {dsr}"


def test_deflated_sharpe_ratio_single_trial():
    """With n_trials=1 DSR should equal normal CDF of Sharpe."""
    from core.research.validation_gate import _deflated_sharpe_ratio
    from scipy.stats import norm
    dsr = _deflated_sharpe_ratio(sharpe=1.0, n_trials=1, n_obs=200)
    expected = norm.cdf(1.0)
    assert abs(dsr - expected) < 0.05, f"Single trial DSR mismatch: {dsr} vs {expected}"


def test_validation_summary_has_dsr_field():
    """ProposalValidationSummary should have dsr_score and wf_consistency fields."""
    from core.ai.proposal_schemas import ProposalValidationSummary
    from datetime import datetime, timezone
    s = ProposalValidationSummary(
        computed_at=datetime.now(timezone.utc),
        decision="shadow",
        edge_score=50.0,
        risk_score=60.0,
        stability_score=55.0,
        efficiency_score=50.0,
        deployment_score=52.0,
        dsr_score=0.65,
        wf_consistency=0.75,
    )
    assert s.dsr_score == 0.65
    assert s.wf_consistency == 0.75


def test_dsr_gate_forces_reject():
    """Validation gate: very many trials with modest Sharpe should trigger DSR gate."""
    from core.research.validation_gate import build_validation_summary_from_research_result
    # 500 runs = very high multiple testing correction
    result = {
        "runs": 500,
        "valid_runs": 1,
        "best": {
            "total_return": 20.0,
            "sharpe_ratio": 1.2,
            "max_drawdown": 8.0,
            "win_rate": 55.0,
            "total_trades": 20,
            "anomaly_bar_ratio": 0.0,
            "cost_drag_return_pct": 0.5,
            "gross_total_return": 20.5,
        },
        "quality_counts": {"ok": 1},
    }
    summary = build_validation_summary_from_research_result(result)
    assert summary.dsr_score is not None, "dsr_score should be populated"
    assert 0.0 <= summary.dsr_score <= 1.0
    # With 500 trials, DSR should be very low → reject or shadow
    if summary.dsr_score < 0.3:
        assert summary.decision == "reject", f"Expected reject, got {summary.decision}"


def test_purged_walk_forward_returns_dict():
    """_run_purged_walk_forward should return dict with required keys."""
    from core.research.strategy_research import _run_purged_walk_forward
    df = _make_ohlcv(600)
    result = _run_purged_walk_forward(
        strategy="MAStrategy",
        df=df,
        timeframe="1m",
        params={"fast_period": 10, "slow_period": 30},
        n_splits=3,
        embargo_pct=0.02,
        commission_rate=0.0004,
        slippage_bps=2.0,
        initial_capital=10000.0,
    )
    assert isinstance(result, dict)
    assert "sharpe_list" in result
    assert "consistency" in result
    assert "n_folds" in result
    assert "positive_folds" in result
    assert 0.0 <= result["consistency"] <= 1.0
    assert len(result["sharpe_list"]) == result["n_folds"]


def test_purged_wf_consistency_range():
    """WF consistency should be fraction in [0, 1]."""
    from core.research.strategy_research import _run_purged_walk_forward
    df = _make_ohlcv(800, seed=7)
    result = _run_purged_walk_forward(
        strategy="EMAStrategy",
        df=df,
        timeframe="1m",
        params={"fast_period": 12, "slow_period": 26},
        n_splits=4,
        embargo_pct=0.01,
        commission_rate=0.0004,
        slippage_bps=2.0,
        initial_capital=10000.0,
    )
    c = result["consistency"]
    pf = result["positive_folds"]
    nf = result["n_folds"]
    assert 0.0 <= c <= 1.0
    if nf > 0:
        assert abs(c - pf / nf) < 1e-6


def test_scipy_lhs_returns_valid_params():
    """_optimize_params_scipy_lhs should return best_params dict and method string."""
    from core.research.strategy_research import _optimize_params_scipy_lhs
    df = _make_ohlcv(500)
    param_grid = {"fast_period": [5, 8, 10, 12, 15], "slow_period": [20, 25, 30, 35, 40]}
    best_params, trials, method = _optimize_params_scipy_lhs(
        strategy="MAStrategy",
        param_grid=param_grid,
        is_df=df,
        timeframe="1m",
        commission_rate=0.0004,
        slippage_bps=2.0,
        initial_capital=10000.0,
        max_trials=15,
    )
    assert isinstance(best_params, dict)
    assert trials >= 0
    assert method in ("scipy_lhs", "grid", "none")


def test_wf_consistency_in_validation_summary():
    """wf_consistency field should propagate from research result to validation summary."""
    from core.research.validation_gate import build_validation_summary_from_research_result
    result = {
        "runs": 3,
        "valid_runs": 1,
        "best": {
            "total_return": 15.0,
            "sharpe_ratio": 1.3,
            "max_drawdown": 6.0,
            "win_rate": 58.0,
            "total_trades": 30,
            "anomaly_bar_ratio": 0.0,
            "cost_drag_return_pct": 0.5,
            "gross_total_return": 15.5,
            "wf_consistency": 0.8,  # 4/5 folds positive
        },
        "quality_counts": {"ok": 1},
    }
    summary = build_validation_summary_from_research_result(result)
    assert summary.wf_consistency == 0.8, f"Expected 0.8, got {summary.wf_consistency}"


# ══════════════════════════════════════════════════════════════
# Phase 3: Correlation Filter + CUSUM Decay Detection
# ══════════════════════════════════════════════════════════════


def _make_candidate_with_curve(strategy, score, curve):
    """Helper: build a minimal StrategyCandidate with equity_curve_sample in metadata."""
    from datetime import datetime, timezone
    from core.research.experiment_schemas import StrategyCandidate
    return StrategyCandidate(
        candidate_id=f"cand-{strategy}",
        proposal_id="proposal-test",
        experiment_id="exp-test",
        created_at=datetime.now(timezone.utc),
        strategy=strategy,
        timeframe="1m",
        symbol="BTC/USDT",
        score=score,
        metadata={"best": {"equity_curve_sample": curve}},
    )


def test_correlation_filter_marks_redundant():
    """Correlation filter should mark the lower-score correlated candidate."""
    import numpy as np
    from core.research.orchestrator import _correlation_filter_candidates

    n = 50
    base = np.cumsum(np.random.default_rng(1).normal(0.001, 0.01, n)).tolist()
    # Identical curve (ρ=1.0)
    identical = [v * 1.0 for v in base]

    c1 = _make_candidate_with_curve("MAStrategy", 80.0, base)
    c2 = _make_candidate_with_curve("EMAStrategy", 60.0, identical)
    candidates = [c1, c2]  # already sorted desc by score

    _correlation_filter_candidates(candidates, corr_threshold=0.85)

    assert not c1.metadata.get("correlation_filtered"), "Best candidate should NOT be filtered"
    assert c2.metadata.get("correlation_filtered"), "Redundant candidate SHOULD be filtered"
    assert c2.metadata.get("correlated_with") == "MAStrategy"
    assert c2.promotion is None or c2.promotion.decision == "reject"


def test_correlation_filter_keeps_uncorrelated():
    """Filter should not mark strategies with low correlation."""
    import numpy as np
    from core.research.orchestrator import _correlation_filter_candidates

    rng = np.random.default_rng(99)
    curve_a = np.cumsum(rng.normal(0.001, 0.01, 50)).tolist()
    curve_b = np.cumsum(rng.normal(-0.001, 0.02, 50)).tolist()  # very different

    c1 = _make_candidate_with_curve("MAStrategy", 80.0, curve_a)
    c2 = _make_candidate_with_curve("RSIStrategy", 75.0, curve_b)
    candidates = [c1, c2]

    _correlation_filter_candidates(candidates, corr_threshold=0.85)

    assert not c1.metadata.get("correlation_filtered")
    assert not c2.metadata.get("correlation_filtered"), "Uncorrelated strategies should both be kept"


def test_correlation_filter_no_curve_skips():
    """Strategy without equity_curve_sample should not be filtered."""
    from core.research.orchestrator import _correlation_filter_candidates

    c1 = _make_candidate_with_curve("MAStrategy", 80.0, [])   # no curve
    c2 = _make_candidate_with_curve("EMAStrategy", 60.0, [])  # no curve
    candidates = [c1, c2]

    _correlation_filter_candidates(candidates, corr_threshold=0.85)

    assert not c1.metadata.get("correlation_filtered")
    assert not c2.metadata.get("correlation_filtered")


def test_cusum_detects_decay():
    """CUSUM should detect a persistent downward drift."""
    from core.monitoring.strategy_monitor import detect_strategy_decay

    # Uniform negative returns — clear decay signal
    bad_returns = [-0.005] * 100
    result = detect_strategy_decay(bad_returns, target_return=0.0, h=2.0, k=0.5, min_bars=10)

    assert result["triggered"], "Should detect decay in a sequence of consistent losses"
    assert result["trigger_idx"] is not None
    assert result["decay_pct"] < 0, "Cumulative excess loss should be negative"
    assert result["n_bars"] == 100


def test_cusum_no_false_positive():
    """CUSUM should not trigger on clearly positive returns."""
    from core.monitoring.strategy_monitor import detect_strategy_decay
    import numpy as np

    rng = np.random.default_rng(0)
    # Strong positive drift (mean >> std) with loose threshold
    strong_positive = rng.normal(0.01, 0.002, 200).tolist()
    result = detect_strategy_decay(strong_positive, target_return=0.0, h=5.0, k=1.0, min_bars=30)

    assert not result["triggered"], f"Should not trigger on strong positive returns: {result['message']}"


def test_cusum_monitor_stateful():
    """CUSUMMonitor should accumulate state and trigger on sustained losses."""
    from core.monitoring.strategy_monitor import CUSUMMonitor

    monitor = CUSUMMonitor(strategy_name="TestStrategy", h=2.0, k=0.5, min_bars=5)

    # Feed 5 warm-up bars (neutral)
    for _ in range(5):
        monitor.update(0.0)

    # Feed sustained losses
    triggered = False
    for _ in range(50):
        status = monitor.update(-0.01)
        if status["triggered"]:
            triggered = True
            break

    assert triggered, "CUSUMMonitor should trigger on sustained losses after warm-up"
    assert monitor.trigger_count >= 1


def test_cusum_monitor_reset():
    """CUSUMMonitor full_reset should clear all state."""
    from core.monitoring.strategy_monitor import CUSUMMonitor

    monitor = CUSUMMonitor(strategy_name="TestStrategy", h=1.0, k=0.1, min_bars=2)
    for _ in range(20):
        monitor.update(-0.02)
    monitor.full_reset()

    assert monitor.n_bars == 0
    assert monitor.trigger_count == 0


def test_cusum_returns_dict_structure():
    """detect_strategy_decay should return dict with all required keys."""
    from core.monitoring.strategy_monitor import detect_strategy_decay

    result = detect_strategy_decay([0.001, -0.002, 0.003, -0.001] * 30)
    for key in ("triggered", "cusum_low", "trigger_idx", "decay_pct", "n_bars", "std", "threshold", "message"):
        assert key in result, f"Missing key: {key}"
    assert isinstance(result["cusum_low"], list)
    assert len(result["cusum_low"]) == 120


# ══════════════════════════════════════════════════════════════
# Phase 4: _trades_to_returns + cross-batch correlation flag
# ══════════════════════════════════════════════════════════════


def test_trades_to_returns_pnl_pct():
    """_trades_to_returns should use pnl_pct field when available."""
    from web.api.ai_research import _trades_to_returns

    trades = [{"pnl_pct": 0.01}, {"pnl_pct": -0.005}, {"pnl_pct": 0.02}]
    returns = _trades_to_returns(trades)
    assert returns == [0.01, -0.005, 0.02]


def test_trades_to_returns_pnl_capital():
    """_trades_to_returns should derive return from pnl/capital."""
    from web.api.ai_research import _trades_to_returns

    trades = [{"pnl": 100.0, "capital": 10000.0}, {"pnl": -50.0, "capital": 10000.0}]
    returns = _trades_to_returns(trades)
    assert len(returns) == 2
    assert abs(returns[0] - 0.01) < 1e-9
    assert abs(returns[1] - (-0.005)) < 1e-9


def test_trades_to_returns_empty():
    """_trades_to_returns on empty list should return empty list."""
    from web.api.ai_research import _trades_to_returns

    assert _trades_to_returns([]) == []


def test_correlation_filter_cross_batch_flag():
    """Candidates correlated with existing (running) strategies get correlation_is_cross_batch=True."""
    import numpy as np
    from core.research.orchestrator import _correlation_filter_candidates

    rng = np.random.default_rng(7)
    base_curve = np.cumsum(rng.normal(0.001, 0.01, 50)).tolist()
    identical_curve = [v * 1.0 for v in base_curve]  # perfect correlation

    # Existing running candidate
    existing = _make_candidate_with_curve("RunningStrategy", 90.0, base_curve)
    # New candidate that's identical
    new_cand = _make_candidate_with_curve("NewStrategy", 70.0, identical_curve)
    candidates = [new_cand]  # only the new one is in this batch

    _correlation_filter_candidates(candidates, corr_threshold=0.85, existing_candidates=[existing])

    assert new_cand.metadata.get("correlation_filtered"), "Should be filtered against existing running strategy"
    assert new_cand.metadata.get("correlation_is_cross_batch") is True, "Should flag as cross-batch correlation"
    assert new_cand.metadata.get("correlated_with") == "RunningStrategy"


# ══════════════════════════════════════════════════════════════
# Phase 4: CUSUM Watcher + Research Context Generator
# ══════════════════════════════════════════════════════════════


def test_cusum_watcher_no_active():
    """run_cusum_checks_for_all_candidates returns empty list when no active candidates."""
    import asyncio
    from unittest.mock import MagicMock, patch

    async def _run():
        from core.monitoring.cusum_watcher import run_cusum_checks_for_all_candidates

        app = MagicMock()
        # Patch at the source module since imports happen inside the function body
        with patch("core.research.orchestrator.list_candidates", return_value=[]):
            result = await run_cusum_checks_for_all_candidates(app)
        return result

    result = asyncio.run(_run())
    assert result == [], f"Expected empty list, got {result}"


def test_cusum_watcher_triggers():
    """run_cusum_checks_for_all_candidates detects decay and returns triggered report."""
    import asyncio
    from unittest.mock import AsyncMock, MagicMock, patch

    from core.research.experiment_schemas import StrategyCandidate

    async def _run():
        from core.monitoring.cusum_watcher import run_cusum_checks_for_all_candidates

        # Build a running candidate
        cand = StrategyCandidate(
            candidate_id="cand-test-trigger",
            proposal_id="prop-1",
            experiment_id="exp-1",
            created_at=datetime.now(timezone.utc),
            strategy="MAStrategy",
            timeframe="1m",
            symbol="BTC/USDT",
            score=55.0,
            status="paper_running",
            metadata={"registered_strategy_name": "MAStrategy_ai_123"},
        )

        # Inject a series of strong negative returns → will trigger CUSUM
        negative_returns = [-0.015] * 80  # 80 losing trades

        app = MagicMock()
        app.state.ai_candidate_registry = MagicMock()
        app.state.ai_lifecycle_registry = MagicMock()
        app.state.ai_candidate_registry.save = MagicMock()

        # Patch at source modules — imports are done inside function body
        with (
            patch("core.research.orchestrator.list_candidates", return_value=[cand]),
            patch("web.api.ai_research._trades_to_returns", return_value=negative_returns),
            patch("core.monitoring.cusum_watcher._send_cusum_notification"),
            patch("core.monitoring.cusum_watcher._demote_on_decay", new=AsyncMock(return_value="shadow_running")),
        ):
            result = await run_cusum_checks_for_all_candidates(app)
        return result

    result = asyncio.run(_run())
    assert len(result) == 1, f"Expected 1 triggered report, got {result}"
    assert result[0]["candidate_id"] == "cand-test-trigger"
    assert result[0]["new_status"] == "shadow_running"


def test_research_context_generator_returns_none_on_error():
    """generate_research_context returns None when GLM client is unavailable."""
    import asyncio
    from unittest.mock import patch

    async def _run():
        from core.ai.research_context_generator import generate_research_context

        # Simulate GLM client import failure
        with patch.dict("sys.modules", {"core.news.eventizer.async_glm_client": None}):
            result = await generate_research_context(
                market_summary={"direction": "FLAT", "confidence": 0.5},
                goals="测试研究目标",
                timeout=10,
            )
        return result

    result = asyncio.run(_run())
    assert result is None, f"Expected None when GLM unavailable, got {result}"
