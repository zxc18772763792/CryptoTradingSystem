from __future__ import annotations

import asyncio

from core.ai import signal_aggregator as module


def test_explicit_hold_skips_artifact_loading_and_has_zero_vote(monkeypatch):
    def unexpected(*args, **kwargs):
        raise AssertionError("A deliberately held component must not load a model")
    monkeypatch.setattr(module.MLSignalModel, "load_from_path", unexpected)
    aggregator = module.SignalAggregator(ml_enabled=False, ml_disabled_reason="quality_gate_rejected")
    assert aggregator.ml_runtime_status() == {
        "enabled": False, "loaded": False, "state": "disabled",
        "effective_weight": 0.0, "reason": "quality_gate_rejected"}
    signal = asyncio.run(aggregator.aggregate(
        "BTC/USDT", None, include_llm=False, include_factor=False, include_derivatives=False))
    assert signal.components["ml"]["status"] == "disabled"
    assert signal.components["ml"]["effective_weight"] == 0.0
    assert signal.components["ml"]["reason"] == "quality_gate_rejected"


def test_default_enabled_path_still_uses_manifest_and_quality_gates(monkeypatch):
    calls = []
    class Refused:
        load_error = "quality_gate_failed"
        def is_loaded(self):
            return False
    def load(path):
        calls.append(path)
        return Refused()
    monkeypatch.setattr(module.MLSignalModel, "load_from_path", load)
    aggregator = module.SignalAggregator(ml_model_path="candidate.json")
    assert calls == ["candidate.json"]
    assert aggregator.ml_runtime_status()["state"] == "blocked"
    assert aggregator.ml_runtime_status()["effective_weight"] == 0.0
