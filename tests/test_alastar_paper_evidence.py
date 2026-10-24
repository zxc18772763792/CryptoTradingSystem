from datetime import datetime, timedelta, timezone
import json

import pytest

from scripts import observe_alastar_paper as cli
from scripts import archive_alastar_research as archive


def fake_provider(tmp_path, fetch):
    provider = object.__new__(cli.OfficialEvidence)
    provider.output = tmp_path
    provider._fetch = fetch
    return provider


def test_official_pagination_covers_inclusive30days_without_future_rows(tmp_path):
    calls = []
    def fetch(endpoint, params):
        calls.append(params)
        return [[t, "100", "100", "100", "100"] for t in range(params["startTime"], params["endTime"]+1, 300000)], {}
    provider = fake_provider(tmp_path, fetch)
    end = datetime(2020,2,1,tzinfo=timezone.utc)
    rows, _ = provider.klines("BTCUSDT", end-timedelta(days=30), end)
    assert len(calls) == 6 and len(rows) == 8641
    assert len({r[0] for r in rows}) == 8641
    assert rows[-1][0] == int(end.timestamp()*1000)
    assert all(p["endTime"] <= int(end.timestamp()*1000) for p in calls)


def test_official_empty_funding_query_is_known_zero_only_with_complete_receipt(tmp_path):
    provider = fake_provider(tmp_path, lambda *_: ([], {"file": "receipt", "sha256": "test"}))
    start = datetime(2020,1,1,tzinfo=timezone.utc)
    rows, receipt = provider.funding("BTCUSDT", start, start+timedelta(hours=1))
    assert rows == [] and receipt["complete_query_coverage"] is True
    assert receipt["start_ms"] == int(start.timestamp()*1000)


@pytest.mark.parametrize("fault", ("duplicate", "wrong_symbol", "outside_interval", "full_page"))
def test_invalid_or_unproven_funding_is_rejected(tmp_path, fault):
    start = datetime(2020,1,1,tzinfo=timezone.utc)
    row = {"symbol": "BTCUSDT", "fundingTime": int(start.timestamp()*1000)}
    rows = [row]
    if fault == "duplicate": rows = [row, row]
    if fault == "wrong_symbol": rows = [{**row, "symbol": "ETHUSDT"}]
    if fault == "outside_interval": rows = [{**row, "fundingTime": row["fundingTime"]-1}]
    if fault == "full_page": rows = [row]*1000
    provider = fake_provider(tmp_path, lambda *_: (rows, {}))
    with pytest.raises(ValueError):
        provider.funding("BTCUSDT", start, start+timedelta(hours=1))


@pytest.mark.parametrize(("offset", "rtt", "expected"), [(0., .2, True), (2.1, .2, False), (1.95, .2, False), (0., 11, False)])
def test_clock_gate_uses_entire_network_uncertainty_interval(tmp_path, offset, rtt, expected):
    requested = datetime(2020,1,1,tzinfo=timezone.utc)
    received = requested+timedelta(seconds=rtt)
    metadata = {"requested_at_utc": requested.isoformat(), "received_at_utc": received.isoformat()}
    path = tmp_path/"clock.json"
    path.write_text(json.dumps(metadata), encoding="utf-8")
    server_ms = int((requested.timestamp()+rtt/2+offset)*1000)
    provider = fake_provider(tmp_path, lambda *_: ({"serverTime": server_ms}, {"file": path.name}))
    assert provider.clock_check()["ok"] is expected


def test_curated_user_research_is_preserved_and_machine_handoff_is_versioned(tmp_path):
    doc = tmp_path/"research.md"
    original = "第四轮和第五轮由用户补充，不能丢失。\n"
    doc.write_text(original, encoding="utf-8")
    assert archive.preserve_curated_document(doc, "old three-day recommendation") is False
    assert doc.read_text(encoding="utf-8") == original
    target = tmp_path/"handoff.json"
    archive.save(target, {"round": 3})
    old = target.read_bytes()
    archive.save(target, {"round": 5})
    snapshots = list((tmp_path/"handoff_history").glob("*.json"))
    assert len(snapshots) == 1 and snapshots[0].read_bytes() == old
    assert json.loads(target.read_bytes())["round"] == 5


def test_archiver_recognises_horizon_round_without_reviving_three_day_primary(tmp_path,monkeypatch):
    monkeypatch.setattr(archive,'ROOT',tmp_path)
    monkeypatch.setattr(archive,'DATA',tmp_path/'data/research/alastar_response_20261021')
    for name in ('reports/alastar_selection_timing_20261021/RESEARCH_NOTES.md','reports/alastar_horizons_20261022/RESEARCH_NOTES.md',
                 'data/research/alastar_horizons_20261022/prospective_horizon_protocol_v3.json'):
        p=tmp_path/name; p.parent.mkdir(parents=True,exist_ok=True); p.write_text('{}')
    context=archive.latest_context()
    assert context['latest_round']==6 and context['v3'] and not context['v3_activation_record_present']
    assert '未指定优胜周期' in archive.current_decision('fresh_view',context)
    assert '停止作为前瞻主方案' in archive.current_decision('broad_directional',context)


def test_clock_gate_uses_the_tightest_of_several_reads_and_keeps_all(tmp_path):
    start = datetime(2020,1,1,tzinfo=timezone.utc)
    plan = [(3.0, 1.9), (.05, 0.), (.08, 0.), (.06, 0.), (.07, 0.)]  # (round trip, true offset): slow first read
    state = {"i": 0}

    def fetch(*_):
        rtt, offset = plan[state["i"]]
        requested = start + timedelta(seconds=10*state["i"])
        received = requested + timedelta(seconds=rtt)
        name = f"clock{state['i']}.json"
        (tmp_path/name).write_text(json.dumps({"requested_at_utc": requested.isoformat(),
                                               "received_at_utc": received.isoformat()}), encoding="utf-8")
        state["i"] += 1
        return {"serverTime": int((requested.timestamp()+rtt/2+offset)*1000)}, {"file": name}

    result = fake_provider(tmp_path, fetch).clock_check()
    assert result["ok"] is True and result["round_trip_seconds"] == pytest.approx(.05)
    assert len(result["reads"]) == cli.CLOCK_SAMPLES == 5
    assert max(abs(v) for v in result["reads"][0]["offset_interval_seconds"]) > 2  # the slow read alone would fail
