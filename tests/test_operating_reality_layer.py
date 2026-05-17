from __future__ import annotations


def test_decision_trace_picks_block_before_downgrade():
    from core.observability.decision_trace import DecisionTrace, append_gate

    trace = DecisionTrace(subject_type="unit", subject_id="x", stage="test")
    append_gate(trace, code="weak", label="Weak", status="downgrade", severity=5)
    append_gate(trace, code="hard", label="Hard block", status="block", severity=1)

    assert trace.root_blocker_code == "hard"
    assert trace.to_dict()["root_blocker_label"] == "Hard block"


def test_market_state_borderline_and_halt_posture():
    from core.market_state.classifier import classify_market_regime

    borderline = classify_market_regime(spread_bps=2.0, imbalance=0.18, long_short_ratio=1.0)
    assert borderline["uncertainty"] in {"borderline", "confirmed"}
    halted = classify_market_regime(spread_bps=13.0, imbalance=0.0, long_short_ratio=1.0)
    assert halted["risk_posture"] == "halt_new_entries"
    assert halted["regime"] == "high_risk_chop"


def test_operating_mode_surfaces_provider_fallback_and_derivatives_shadow(monkeypatch):
    from core.runtime.operating_mode import validate_operating_mode
    from core.runtime import operating_mode as module

    monkeypatch.setattr(module.settings, "COINGLASS_LIVE_GATING_ENABLED", False, raising=False)
    snapshot = validate_operating_mode(
        live_decision_config={
            "enabled": True,
            "mode": "enforce",
            "provider": "codex",
            "provider_requested": "claude",
            "provider_fallback": True,
            "fail_open": True,
            "providers": {"codex": {"available": True}},
        },
        agent_config={"enabled": True, "mode": "execute", "allow_live": False},
        source_health={"categories": {}},
    ).to_dict()

    codes = {item["code"] for item in snapshot["degradations"]}
    assert "provider_fallback" in codes
    assert "derivatives_shadow_only" in codes
    assert "autonomous_allow_live_false" in codes


def test_operating_mode_reads_mapping_source_health():
    from core.runtime.operating_mode import validate_operating_mode

    snapshot = validate_operating_mode(
        agent_config={"allow_live": True},
        source_health={
            "sources": {
                "cache_ok": {
                    "status": "ok",
                    "ready": "true",
                    "stale": "false",
                },
                "macro_cache": {
                    "status": "failed",
                    "issues": ["timeout"],
                }
            }
        },
    ).to_dict()

    degradation = next(item for item in snapshot["degradations"] if item["code"] == "source_macro_cache")
    assert degradation["severity"] == "danger"
    assert "timeout" in degradation["detail"]
    assert "source_cache_ok" not in {item["code"] for item in snapshot["degradations"]}


def test_performance_divergence_marks_overfit_suspect():
    from datetime import datetime, timezone
    from types import SimpleNamespace

    from core.ai.proposal_schemas import ProposalValidationSummary
    from core.research.performance_feedback import build_performance_divergence_report

    candidate = SimpleNamespace(
        candidate_id="cand-1",
        strategy="MAStrategy",
        validation_summary=ProposalValidationSummary(
            computed_at=datetime.now(timezone.utc),
            decision="paper",
            oos_score=2.0,
            metrics={"best": {"max_drawdown": 8.0}},
        ),
    )
    snapshot = SimpleNamespace(sharpe=0.3, max_drawdown=9.0, trade_count=30)

    report = build_performance_divergence_report(candidate=candidate, snapshots=[snapshot])

    assert report.status == "overfit_suspect"
    assert report.divergence_score > 1.0


def test_performance_divergence_accepts_dict_payloads():
    from core.research.performance_feedback import build_performance_divergence_report

    candidate = {
        "candidate_id": "cand-dict",
        "strategy": "TrendStrategy",
        "validation_summary": {
            "oos_score": 1.8,
            "metrics": {"best": {"max_drawdown": 5.0}},
        },
    }
    latest = {"sharpe": 0.2, "max_drawdown": 6.0, "trade_count": 25}

    report = build_performance_divergence_report(candidate=candidate, snapshots=[latest])

    assert report.candidate_id == "cand-dict"
    assert report.strategy_name == "TrendStrategy"
    assert report.status == "overfit_suspect"
    assert report.realized["trade_count"] == 25


def test_compact_symbols_are_treated_as_benchmarks_for_reality_hints():
    from core.market_state.planner_adapter import market_state_to_planner_hints
    from core.observability.score_calibration import symbol_scope_for_symbol

    assert symbol_scope_for_symbol("BTCUSDT") == "benchmark"
    assert symbol_scope_for_symbol("BTCUSD_PERP") == "benchmark"
    assert symbol_scope_for_symbol("ETH-USDT-SWAP") == "benchmark"
    assert symbol_scope_for_symbol("SOLUSDT") == "altcoin"

    boosted, suppressed, notes = market_state_to_planner_hints(
        {
            "scope": "benchmark",
            "regime": "trend_bullish",
            "bias": "bullish",
            "risk_posture": "defensive",
            "uncertainty": "confirmed",
        },
        symbol="BTCUSDT",
        benchmark_beta=0.0,
    )

    assert "benchmark_context_advisory_only" not in notes
    assert len(boosted) >= 2
    assert suppressed


def test_family_regime_prior_loader_reads_advisory_file(tmp_path):
    import json

    from core.observability.score_calibration import get_family_regime_prior

    prior_path = tmp_path / "family_regime_priors.json"
    prior_path.write_text(
        json.dumps(
            {
                "schema_version": "family_regime_priors.v1",
                "priors": {
                    "MAStrategy|trend_bullish|benchmark": {
                        "sample_size": 12,
                        "recent_decay_rate": 0.25,
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    prior = get_family_regime_prior(
        strategy_family="MAStrategy",
        regime="trend_bullish",
        symbol_scope="benchmark",
        path=prior_path,
    )

    assert prior["available"] is True
    assert prior["sample_size"] == 12
