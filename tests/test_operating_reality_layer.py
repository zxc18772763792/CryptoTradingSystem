from __future__ import annotations

import pytest


def test_decision_trace_picks_block_before_downgrade():
    from core.observability.decision_trace import DecisionTrace, append_gate
    from core.observability.gate_codes import GateCode, is_registered_gate_code

    trace = DecisionTrace(subject_type="unit", subject_id="x", stage="test")
    append_gate(trace, code="weak", label="Weak", status="downgrade", severity=5)
    append_gate(trace, code=GateCode.RISK_GATE, label="Hard block", status="block", severity=1)

    assert trace.root_blocker_code == "risk_gate"
    assert trace.to_dict()["root_blocker_label"] == "Hard block"
    assert is_registered_gate_code(trace.root_blocker_code)
    assert trace.gates[0].metadata["unregistered_gate_code"] is True


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


def test_operating_mode_prefers_current_trading_mode_override(monkeypatch):
    from core.runtime.operating_mode import validate_operating_mode
    from core.runtime import operating_mode as module

    monkeypatch.setattr(module.settings, "TRADING_MODE", "paper", raising=False)

    snapshot = validate_operating_mode(
        trading_mode="live",
        agent_config={"enabled": True, "mode": "execute", "allow_live": False},
        source_health={"categories": {}},
    ).to_dict()

    assert snapshot["trading_mode"] == "live"
    assert "live_mode_agent_blocked" in {item["code"] for item in snapshot["degradations"]}


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
    assert prior["graduation"]["status"] in {"advisory_collecting_samples", "cold_start"}


def test_update_family_regime_priors_closes_the_loop(tmp_path):
    import json

    from core.observability.score_calibration import (
        get_family_regime_prior,
        update_family_regime_priors,
    )

    prior_path = tmp_path / "family_regime_priors.json"
    prior_path.write_text(
        json.dumps({"schema_version": "family_regime_priors.v1", "updated_at": None, "priors": {}}),
        encoding="utf-8",
    )

    res = update_family_regime_priors(
        [
            {
                "strategy_family": "MAStrategy",
                "regime": "trend_bullish",
                "symbol_scope": "benchmark",
                "divergence_score": 1.2,
                "status": "decayed",
            }
        ],
        path=prior_path,
    )
    assert res["applied"] == 1

    prior = get_family_regime_prior(
        strategy_family="MAStrategy",
        regime="trend_bullish",
        symbol_scope="benchmark",
        path=prior_path,
    )
    assert prior["available"] is True
    assert prior["sample_size"] == 1
    assert prior["recent_decay_rate"] == 1.0
    assert prior["sharpe_gap_ewma"] == 1.2

    # Second observation folds via EWMA and is idempotent in structure.
    update_family_regime_priors(
        [
            {
                "strategy_family": "MAStrategy",
                "regime": "trend_bullish",
                "symbol_scope": "benchmark",
                "divergence_score": 0.2,
                "status": "aligned",
            }
        ],
        path=prior_path,
    )
    prior2 = get_family_regime_prior(
        strategy_family="MAStrategy",
        regime="trend_bullish",
        symbol_scope="benchmark",
        path=prior_path,
    )
    assert prior2["sample_size"] == 2
    assert 0.2 < prior2["sharpe_gap_ewma"] < 1.2
    assert prior2["recent_decay_rate"] == 0.5
    assert prior2["confirmation_rate"] == 0.5
    assert prior2["graduation"]["binding_eligible"] is False


def test_family_regime_priors_learn_positive_confirmations(tmp_path):
    import json

    from core.observability.score_calibration import get_family_regime_prior, update_family_regime_priors

    prior_path = tmp_path / "family_regime_priors.json"
    prior_path.write_text(
        json.dumps({"schema_version": "family_regime_priors.v1", "updated_at": None, "priors": {}}),
        encoding="utf-8",
    )

    update_family_regime_priors(
        [
            {
                "strategy_family": "Breakout",
                "regime": "trend_bullish",
                "symbol_scope": "altcoin",
                "divergence_score": 0.0,
                "edge_delta": 0.35,
                "status": "aligned",
                "counterfactual_outcome_count": 1,
            }
        ],
        path=prior_path,
    )

    prior = get_family_regime_prior(
        strategy_family="Breakout",
        regime="trend_bullish",
        symbol_scope="altcoin",
        path=prior_path,
    )
    assert prior["confirmation_rate"] == 1.0
    assert prior["edge_delta_ewma"] == 0.35
    assert prior["counterfactual_outcome_sample_size"] == 1


def test_performance_divergence_decayed_status_is_reachable():
    from datetime import datetime, timezone
    from types import SimpleNamespace

    from core.ai.proposal_schemas import ProposalValidationSummary
    from core.research.performance_feedback import build_performance_divergence_report

    candidate = SimpleNamespace(
        candidate_id="cand-decay",
        strategy="MAStrategy",
        validation_summary=ProposalValidationSummary(
            computed_at=datetime.now(timezone.utc),
            decision="paper",
            oos_score=1.8,
            metrics={"best": {"max_drawdown": 7.0}},
        ),
    )
    # Even a perfectly-aligned snapshot must report decayed when CUSUM fired.
    snapshot = SimpleNamespace(sharpe=1.7, max_drawdown=7.0, trade_count=40)

    report = build_performance_divergence_report(
        candidate=candidate,
        snapshots=[snapshot],
        decay_state={"triggered": True, "decay_pct": 42.0},
    )
    assert report.status == "decayed"
    assert any("decay" in note.lower() for note in report.notes)

    # No-snapshot decayed path is also reachable.
    report2 = build_performance_divergence_report(
        candidate=candidate,
        snapshots=[],
        decay_state={"triggered": True, "decay_pct": 30.0},
    )
    assert report2.status == "decayed"


def test_operating_mode_reports_cold_calibration(tmp_path, monkeypatch):
    import json

    import core.observability.score_calibration as sc
    from core.runtime.operating_mode import validate_operating_mode

    cold = tmp_path / "family_regime_priors.json"
    cold.write_text(
        json.dumps({"schema_version": "family_regime_priors.v1", "updated_at": None, "priors": {}}),
        encoding="utf-8",
    )
    monkeypatch.setattr(sc, "DEFAULT_PRIOR_PATH", cold)

    snapshot = validate_operating_mode(
        live_decision_config={"enabled": False, "provider": "codex", "providers": {}},
        agent_config={"allow_live": False},
        runtime_state_snapshot={},
        source_health={},
    )
    codes = {d["code"] for d in snapshot.degradations}
    assert "feedback_priors_cold" in codes


def test_cusum_record_decay_feedback_closes_loop(tmp_path, monkeypatch):
    """End-to-end: a decayed candidate writes a counterfactual row AND folds
    a decayed observation into the advisory prior file."""
    import json
    from types import SimpleNamespace

    import core.observability.score_calibration as sc
    from core.monitoring.cusum_watcher import _record_decay_feedback

    # conftest sets GATE_COUNTERFACTUAL_AUDIT_PATH and the env path wins over
    # DEFAULT_AUDIT_PATH; point it at our tmp file and read back from there.
    audit_path = tmp_path / "gate_counterfactuals.jsonl"
    monkeypatch.setenv("GATE_COUNTERFACTUAL_AUDIT_PATH", str(audit_path))
    prior_path = tmp_path / "family_regime_priors.json"
    prior_path.write_text(
        json.dumps({"schema_version": "family_regime_priors.v1", "updated_at": None, "priors": {}}),
        encoding="utf-8",
    )
    monkeypatch.setattr(sc, "DEFAULT_PRIOR_PATH", prior_path)

    candidate = SimpleNamespace(
        candidate_id="cand-cusum",
        strategy="MAStrategy",
        symbol="BTC/USDT",
        status="paper_running",
        metadata={"strategy_family": "trend", "research_mode": "trend_bullish"},
        validation_summary=SimpleNamespace(
            oos_score=1.8, is_score=1.9, metrics={"best": {"max_drawdown": 6.0}}
        ),
    )
    decay_result = {"triggered": True, "decay_pct": 38.0, "threshold": -2.0, "message": "decay"}

    _record_decay_feedback(app=None, candidate=candidate, decay_result=decay_result, new_status="shadow_running")

    rows = [json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(rows) == 1
    assert rows[0]["gate_code"] == "cusum_decay"
    assert rows[0]["counterfactual_decision"] == "hold"
    assert rows[0]["observed_decision"] == "shadow_running"

    prior_blob = json.loads(prior_path.read_text(encoding="utf-8"))
    assert prior_blob["updated_at"] is not None
    assert prior_blob["priors"], "prior file must be non-empty after decay feedback"
    key = next(iter(prior_blob["priors"]))
    assert prior_blob["priors"][key]["last_status"] == "decayed"


def test_cusum_prefers_matching_reserve_candidate(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from core.monitoring.cusum_watcher import _activate_reserve_replacement

    class _Lifecycle:
        def __init__(self):
            self.items = []

        def append(self, item):
            self.items.append(item)
            return item

    decayed = SimpleNamespace(
        candidate_id="champion",
        status="paper_running",
        strategy="Breakout",
        symbol="SOL/USDT",
        timeframe="15m",
        metadata={"strategy_family": "breakout"},
    )
    reserve = SimpleNamespace(
        candidate_id="reserve-1",
        status="new",
        strategy="Breakout",
        symbol="SOL/USDT",
        timeframe="15m",
        metadata={"reserve_eligible": True, "strategy_family": "breakout"},
        validation_summary=SimpleNamespace(risk_adjusted_edge=0.72, reserve_eligible=True, outcome_type="redundant_correlated"),
    )
    registry = SimpleNamespace(save=MagicMock())
    lifecycle = _Lifecycle()
    app = SimpleNamespace(state=SimpleNamespace(ai_candidate_registry=registry, ai_lifecycle_registry=lifecycle))

    result = _activate_reserve_replacement(app, decayed, {"decay_pct": 30.0}, [decayed, reserve])

    assert result["activated"] is True
    assert result["candidate_id"] == "reserve-1"
    assert reserve.status == "shadow_running"
    assert reserve.metadata["replaces_candidate_id"] == "champion"
    registry.save.assert_called_once_with(reserve)


def test_gate_counterfactual_outcomes_are_backfilled(tmp_path):
    from core.audit.gate_counterfactuals import (
        record_gate_counterfactual,
        summarize_gate_counterfactuals,
        update_gate_counterfactual_outcomes,
    )

    audit_path = tmp_path / "gate_counterfactuals.jsonl"
    row = record_gate_counterfactual(
        trace={
            "trace_id": "trace-1",
            "subject_type": "candidate",
            "subject_id": "cand-1",
            "root_blocker_code": "risk_gate",
            "created_at": "2026-05-17T00:00:00+00:00",
            "gates": [{"code": "risk_gate", "counterfactual_decision": "long"}],
        },
        observed_decision="hold",
        mode="paper",
        path=audit_path,
    )
    assert row["later_outcome_ref"] == ""

    result = update_gate_counterfactual_outcomes(
        [{"subject_type": "candidate", "subject_id": "cand-1", "later_outcome_ref": "paper:+0.5", "edge_delta": 0.5}],
        path=audit_path,
    )
    assert result["updated"] == 1
    summary = summarize_gate_counterfactuals(path=audit_path)
    assert summary["outcome_linked_count"] == 1
    assert summary["missed_alpha_proxy"] == 0.5


def test_gate_counterfactual_summary_limit_uses_tail_rows(tmp_path):
    from core.audit.gate_counterfactuals import record_gate_counterfactual, summarize_gate_counterfactuals

    audit_path = tmp_path / "gate_counterfactuals.jsonl"
    for idx, gate_code in enumerate(["gate_a", "gate_b", "gate_c"], start=1):
        record_gate_counterfactual(
            trace={
                "trace_id": f"trace-{idx}",
                "subject_type": "candidate",
                "subject_id": f"cand-{idx}",
                "root_blocker_code": gate_code,
                "created_at": "2026-05-17T00:00:00+00:00",
                "gates": [{"code": gate_code, "counterfactual_decision": "paper"}],
            },
            observed_decision="hold",
            mode="paper",
            path=audit_path,
        )

    summary = summarize_gate_counterfactuals(path=audit_path, limit=2)

    assert summary["total"] == 2
    assert summary["gate_hit_counts"] == {"gate_b": 1, "gate_c": 1}
    assert [item["subject_id"] for item in summary["items"]] == ["cand-2", "cand-3"]


def test_gate_counterfactual_outcome_backfill_preserves_existing_unless_forced(tmp_path):
    from core.audit.gate_counterfactuals import (
        record_gate_counterfactual,
        summarize_gate_counterfactuals,
        update_gate_counterfactual_outcomes,
    )

    audit_path = tmp_path / "gate_counterfactuals.jsonl"
    record_gate_counterfactual(
        trace={
            "trace_id": "trace-force",
            "subject_type": "candidate",
            "subject_id": "cand-force",
            "root_blocker_code": "risk_gate",
            "created_at": "2026-05-17T00:00:00+00:00",
            "gates": [{"code": "risk_gate", "counterfactual_decision": "paper"}],
        },
        observed_decision="hold",
        mode="paper",
        path=audit_path,
    )

    first = update_gate_counterfactual_outcomes(
        [{"trace_id": "trace-force", "later_outcome_ref": "paper:+0.5", "edge_delta": 0.5}],
        path=audit_path,
    )
    skipped = update_gate_counterfactual_outcomes(
        [{"trace_id": "trace-force", "later_outcome_ref": "paper:-0.2", "edge_delta": -0.2}],
        path=audit_path,
    )
    forced = update_gate_counterfactual_outcomes(
        [
            {
                "subject_type": "candidate",
                "subject_id": "cand-force",
                "later_outcome_ref": "paper:-0.2",
                "edge_delta": -0.2,
                "force": True,
            }
        ],
        path=audit_path,
    )

    assert first["updated"] == 1
    assert skipped["updated"] == 0
    assert forced["updated"] == 1
    summary = summarize_gate_counterfactuals(path=audit_path)
    assert summary["items"][-1]["later_outcome_ref"] == "paper:-0.2"
    assert summary["missed_alpha_proxy"] == 0.0
    assert summary["avoided_loss_proxy"] == 0.2


def test_market_state_benchmark_scope_requires_beta_or_sector_rule():
    from core.market_state.planner_adapter import market_state_to_planner_hints

    snapshot = {
        "scope": "benchmark",
        "symbol": "BTC/USDT",
        "regime": "trend_bullish",
        "bias": "bullish",
        "risk_posture": "normal",
        "uncertainty": "confirmed",
    }

    boosted, suppressed, notes = market_state_to_planner_hints(snapshot, symbol="SOL/USDT", benchmark_beta=0.0)
    assert boosted == []
    assert suppressed == []
    assert "benchmark_context_risk_only:no_beta_or_sector_rule" in notes

    boosted2, suppressed2, notes2 = market_state_to_planner_hints(
        snapshot,
        symbol="SOL/USDT",
        benchmark_beta=0.7,
    )
    assert "trend" in boosted2
    assert "mean_reversion" in suppressed2
    assert "benchmark_context_linked_by_beta" in notes2


def test_market_state_infers_benchmark_scope_when_snapshot_scope_missing():
    from core.market_state.planner_adapter import market_state_to_planner_hints

    snapshot = {
        "symbol": "BTC/USDT",
        "regime": "trend_bullish",
        "bias": "bullish",
        "risk_posture": "normal",
        "uncertainty": "confirmed",
    }

    boosted, suppressed, notes = market_state_to_planner_hints(snapshot, symbol="SOL/USDT", benchmark_beta=0.0)

    assert boosted == []
    assert suppressed == []
    assert "benchmark_context_risk_only:no_beta_or_sector_rule" in notes


def test_planner_computes_rolling_benchmark_beta_when_missing(monkeypatch, tmp_path):
    import pandas as pd

    from config.settings import settings
    from core.ai.research_planner import _parse_market_context
    from core.data.path_utils import canonical_symbol_dir
    from core.market_state.benchmark_beta import clear_benchmark_beta_cache, resolve_benchmark_beta

    storage_root = tmp_path / "klines"
    monkeypatch.setattr(settings, "DATA_STORAGE_PATH", storage_root, raising=False)
    idx = pd.date_range("2026-01-01", periods=80, freq="1h")
    btc_returns = pd.Series([0.004, -0.002, 0.006, -0.003] * 20, index=idx)
    sol_returns = btc_returns * 0.8
    btc_close = 100.0 * (1.0 + btc_returns).cumprod()
    sol_close = 20.0 * (1.0 + sol_returns).cumprod()
    for symbol, close in {"BTC/USDT": btc_close, "SOL/USDT": sol_close}.items():
        folder = canonical_symbol_dir(storage_root, "binance", symbol)
        folder.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"close": close, "open": close, "high": close, "low": close, "volume": 1.0}, index=idx).to_parquet(
            folder / "1h.parquet"
        )
    clear_benchmark_beta_cache()

    beta = resolve_benchmark_beta(exchange="binance", symbol="SOL/USDT", benchmark_symbol="BTC/USDT", timeframe="1h")
    assert beta["available"] is True
    assert beta["beta"] == pytest.approx(0.8, abs=0.05)

    planner_notes = []
    boosted, suppressed = _parse_market_context(
        {
            "exchange": "binance",
            "symbol": "SOL/USDT",
            "timeframe": "1h",
            "market_state_snapshot": {
                "scope": "benchmark",
                "symbol": "BTC/USDT",
                "regime": "trend_bullish",
                "bias": "bullish",
                "risk_posture": "normal",
                "uncertainty": "confirmed",
            },
            "metadata": {"benchmark_symbol": "BTC/USDT"},
        },
        planner_notes=planner_notes,
    )

    assert "trend" in boosted
    assert "mean_reversion" in suppressed
    assert any("benchmark_beta=" in note and "local_rolling_returns" in note for note in planner_notes)
