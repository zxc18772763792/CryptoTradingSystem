from __future__ import annotations

from pathlib import Path

from core.research import retirement, tracker_registry as tr

ROOT = Path(__file__).resolve().parents[1]


def test_every_tracker_has_a_registered_retirement_rule_and_unique_id():
    ids = [spec["id"] for spec in tr.TRACKERS]
    assert len(ids) == len(set(ids))
    for spec in tr.TRACKERS:
        assert spec["rule_key"] in retirement.RULES, spec["id"]


def test_row_uses_the_trackers_own_verdict_and_isolates_failures():
    spec = {"id": "x", "name": "X", "group": "g", "unit": "笔", "rule": "r", "rule_key": "upbit_caution",
            "load": lambda: {"retirement": {"n": 4, "min_n": 20, "mean_pct": 2.5, "ci90_pct": [0.1, 4.9],
                                            "verdict": "collecting", "label": "积累样本", "reason": "不足"},
                             "forward_win_rate": 0.75, "forward_open": 1, "forward_waiting": 2, "forward_late": 1,
                             "updated_at": "2026-10-08T00:00:00+00:00"}}
    row = tr.tracker_row(spec)
    assert row["n"] == 4 and row["mean_pct"] == 2.5 and row["total_pct"] == 10.0
    assert row["ci90_pct"] == [0.1, 4.9] and row["win_rate"] == 0.75
    assert (row["open"], row["waiting"], row["late"]) == (1, 2, 1)
    assert row["backtest_mean_pct"] == retirement.RULES["upbit_caution"]["backtest_mean_pct"]

    def broken():
        raise FileNotFoundError("state missing")

    bad = tr.tracker_row({**spec, "load": broken})
    assert "error" in bad and "FileNotFoundError" in bad["error"]


def test_all_live_trackers_produce_a_row():
    rows = tr.tracker_rows()  # reads whatever state exists; must never raise
    assert [r["id"] for r in rows] == [s["id"] for s in tr.TRACKERS]


def test_strategies_tab_shows_the_tracker_table():
    html = (ROOT / "web" / "templates" / "index.html").read_text(encoding="utf-8")
    js = (ROOT / "web" / "static" / "js" / "app.js").read_text(encoding="utf-8")
    assert 'id="research-trackers-tbody"' in html
    assert "api('/ai/research-trackers'" in js
    assert "loadResearchTrackers()" in js.split("async function loadStrategiesTabData(){", 1)[1].split("}", 1)[0]
