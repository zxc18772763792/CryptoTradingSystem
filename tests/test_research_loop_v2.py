from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import numpy as np
import pandas as pd

import core.ai.research_context_generator as generator
from core.ai import research_loop_v2 as v2
from core.research import xs_panel

from tests.test_xs_research import synthetic_panel

GOOD = {"name": "volume_level", "direction": "high", "thesis": "planted", "expr": {"col": "volume"}}
INVALID = {"name": "exec", "direction": "high", "expr": {"op": "eval", "args": [{"col": "close"}]}}


def _loop(tmp_path, monkeypatch, outputs):
    import core.research.pump_precursor as precursor
    monkeypatch.setattr(precursor, "load_model_weights", lambda: {
        "features": precursor.FEATURES, "weights": [1.0] * len(precursor.FEATURES), "bias": 0.0,
    })
    panel = synthetic_panel(signal=True)
    rng = np.random.default_rng(1)
    weekly = panel[panel["date"].dt.dayofweek == 0][["base", "date"]].copy()
    weekly["baseline_score"] = rng.random(len(weekly))  # an uninformative "existing model"
    loop = v2.CrossSectionalResearchLoop(SimpleNamespace(state=SimpleNamespace(ai_research_dir=str(tmp_path))))
    monkeypatch.setattr(loop, "_load_research_data", lambda: (panel, weekly))
    mock = AsyncMock(side_effect=outputs)
    monkeypatch.setattr(generator, "generate_json", mock)
    loop.state["config"].update(n_perm=50)
    return loop, mock


def test_round_evaluates_valid_formulas_freezes_passing_and_hides_holdout(tmp_path, monkeypatch):
    loop, mock = _loop(tmp_path, monkeypatch, [
        {"hypothesis": "h1", "formulas": [GOOD, INVALID, dict(GOOD, name="same_again")]},
        {"hypothesis": "h2", "formulas": [dict(GOOD, name="renamed_duplicate")]},
    ])

    asyncio.run(loop.tick(force=True))
    status = loop.status()
    first = loop.state["rounds"][-1]

    assert status["ledger_size"] == 1
    assert first["status"] == "completed"
    assert first["invalid"][0]["name"] == "exec"
    assert first["duplicates"] == ["same_again"]
    entry = next(iter(loop.state["ledger"].values()))
    assert entry["trial_number"] == 1 and entry["decision"] == "frozen", entry["reasons"]
    assert loop.state["frozen"][0]["status"] == "forward_observing"
    assert loop.state["frozen"][0]["manual_review_required"] is True

    # Second round: the LLM sees its development results, never holdout numbers or verdicts.
    asyncio.run(loop.tick(force=True))
    prompt = json.loads(mock.await_args_list[1].args[0])
    previous = prompt["your_previous_formulas_development_results"]
    assert previous[0]["dev"]["lift"] is not None
    serialized = json.dumps(prompt)
    for leaked in ("holdout", "frozen", "decision", "reasons", "forward"):
        assert leaked not in serialized
    # A renamed copy of an evaluated formula is not re-run and does not inflate the ledger.
    assert loop.state["rounds"][-1]["duplicates"] == ["renamed_duplicate"]
    assert loop.status()["ledger_size"] == 1
    assert (tmp_path / "cross_sectional_loop.json").exists()


def test_multiplicity_bar_rises_with_ledger_size(tmp_path, monkeypatch):
    loop, _ = _loop(tmp_path, monkeypatch, [])
    dev = {"status": "ok", "coverage": 1.0, "z": 3.0}
    holdout = {"status": "ok", "coverage": 1.0, "z": 3.0, "shortlist_z": 2.5, "shortlist_lift": 1.5, "lift_ex_top3_coins": 2.0}
    cfg = v2.CrossSectionalLoopConfig()
    assert loop._gate(dev, holdout, n_trials=1, cfg=cfg) == []
    assert loop._gate(dev, holdout, n_trials=50, cfg=cfg) == ["shortlist_below_multiplicity_bar"]
    assert "no_gain_over_existing_model" in loop._gate(dev, dict(holdout, shortlist_lift=1.05), 1, cfg)
    assert "carried_by_few_coins" in loop._gate(dev, dict(holdout, lift_ex_top3_coins=1.1), 1, cfg)


def test_disabled_loop_does_nothing_unless_forced_and_model_errors_are_recorded(tmp_path, monkeypatch):
    loop, mock = _loop(tmp_path, monkeypatch, [generator.ResearchGenerationError("provider_unavailable", "down")])
    asyncio.run(loop.tick())
    assert mock.await_count == 0 and loop.state["rounds"] == []

    asyncio.run(loop.tick(force=True))
    assert loop.state["rounds"][-1]["status"] == "failed"
    assert loop.state["last_error_code"] == "provider_unavailable"
    assert loop.state["status"] == "paused"  # disabled loops fall back to paused after a manual run


def test_forward_observation_waits_for_archive(tmp_path, monkeypatch):
    loop, _ = _loop(tmp_path, monkeypatch, [])
    loop.state["frozen"] = [{**GOOD, "fingerprint": "x", "frozen_at": "2026-09-26T00:00:00+00:00", "status": "forward_observing"}]
    monkeypatch.setattr(xs_panel, "WEEKLY_ARCHIVE_DIR", tmp_path / "archive_empty")
    asyncio.run(loop._observe_forward(v2.CrossSectionalLoopConfig()))
    assert loop.state["frozen"][0]["status"] == "forward_observing"
    assert "归档" in loop.state["frozen"][0]["forward_note"]


def test_forward_observation_ignores_archives_captured_before_freeze(tmp_path, monkeypatch):
    loop, _ = _loop(tmp_path, monkeypatch, [])
    loop.state["frozen"] = [{**GOOD, "fingerprint": "x", "frozen_at": "2026-08-24T08:00:00+00:00", "status": "forward_observing"}]
    archive = tmp_path / "archive"
    archive.mkdir()
    panel = synthetic_panel(signal=True)
    for capture in ("2026-08-24", "2026-08-31"):  # the first was captured before the freeze
        last = pd.Timestamp(capture) - pd.Timedelta(days=1)
        part = panel[(panel["date"] > last - pd.Timedelta(days=60)) & (panel["date"] <= last)]
        part.drop(columns=["fwd30_maxret"]).to_parquet(archive / f"{capture}.parquet")
    (archive / "bad-name.parquet").write_bytes(b"")
    monkeypatch.setattr(xs_panel, "WEEKLY_ARCHIVE_DIR", archive)
    asyncio.run(loop._observe_forward(v2.CrossSectionalLoopConfig()))
    item = loop.state["frozen"][0]
    assert item["status"] == "forward_observing"
    assert item["forward"]["method"] == "point_in_time_vintages" and item["forward"]["vintages"] == 1
    assert item["forward_archives_rejected"] == [{"file": "bad-name.parquet", "reason": "unparseable_name"}]
