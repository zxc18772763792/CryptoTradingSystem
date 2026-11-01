import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest

from core.research import korea_frozen_skip_ledger as L

DAY0 = datetime(2026, 11, 3, tzinfo=timezone.utc)
ENTRY = DAY0 + timedelta(days=1)


def event(**kw):
    return {"source_id": "ABC|1", "ticker": "ABC", "day0": DAY0.isoformat(), "entry_at": ENTRY.isoformat(), **kw}


def obs(at, upbit=None, bithumb=None):
    return {"source_id": "ABC|1", "observed_at": at.isoformat(), "upbit": upbit or {"listed": [], "notices": []},
            "bithumb": bithumb or {"error": "x"}}


def bt(closed, valid=True):
    return {"valid": valid, "both_closed_share": 0.004, "coin": {"networks": 1, "both_closed": closed}}


@pytest.mark.parametrize(("body", "hit"), [
    ("*플로우(FLOW)의 입출금 서비스는 이미 중단된 상태로, 추후 재개 시점에", True),
    ("- ENJ 입출금은 중단되지만, ENJ 거래는 가능합니다.", True),
    ("- ENJ 입금 중단 시점 : 2023-09-12 - ENJ 출금 중단 시점 : 2023-09-18", True),
    ("## 입출금 일시 중단 안내 - 대상 디지털 자산 : 리졸브(RESOLV) - 입금 차단 시점 : 2026년 3월", False),
    ("블러(BLUR)가 거래 유의 종목으로 지정되었으며, 이에 따라 입금 서비스가 일시 중단됩니다.", False),
    ("<p>입출금&nbsp;서비스는<br>이미 중단</p>", True),
])
def test_wallet_rule_reads_both_direction_suspension_only(body, hit):
    assert (L.wallet_phrase(body) is not None) is hit


def test_caution_titles_match_designations_not_releases():
    assert L.caution_tickers("멀티버스엑스(EGLD) 거래 유의 종목 지정 안내") == ["EGLD"]
    assert L.caution_tickers("멀티버스엑스(EGLD) 거래 유의 종목 지정 기간 연장 안내") == ["EGLD"]
    assert L.caution_tickers("멀티버스엑스(EGLD) 거래 유의 종목 지정 해제 안내") == []


def test_bithumb_view_flags_maintenance_wide_closures_invalid():
    rows = [{"currency": f"C{i}", "deposit_status": 1, "withdrawal_status": 1} for i in range(100)]
    rows += [{"currency": "ABC", "deposit_status": 0, "withdrawal_status": 0},
             {"currency": "ABC", "deposit_status": "0", "withdrawal_status": "0"}]
    view = L.bithumb_view({"status": "0000", "data": rows}, "ABC")
    assert view["valid"] and view["coin"] == {"networks": 2, "both_closed": True, "deposit_open": False,
                                              "withdrawal_open": False}
    rows += [{"currency": f"M{i}", "deposit_status": 0, "withdrawal_status": 0} for i in range(5)]
    assert not L.bithumb_view({"status": "0000", "data": rows}, "ABC")["valid"]
    assert L.bithumb_view({"status": "0000", "data": rows}, "XYZ")["coin"] is None


def test_upbit_notice_hit_skips_and_later_evidence_is_ignored():
    hit = {"listed": ["9"], "notices": [{"id": "9", "title": "t", "first_listed_at": (DAY0 + timedelta(hours=6)).isoformat(),
                                         "phrase": "입출금 서비스는 이미 중단"}]}
    d = L.decide(event(), [obs(DAY0 + timedelta(hours=7), upbit=hit)])
    assert d["action"] == "skip" and d["reason"] == "upbit_notice" and d["upbit_hit"]["id"] == "9"
    late = L.decide(event(), [obs(DAY0 + timedelta(hours=7)), obs(ENTRY, upbit=hit)])
    assert late["action"] == "keep"
    assert L.decide(event(), [obs(ENTRY + timedelta(minutes=1), upbit=hit)])["action"] == "late"


def test_notice_outside_the_lookback_does_not_count():
    old = {"listed": ["8"], "notices": [{"id": "8", "title": "t", "first_listed_at": (DAY0 - timedelta(days=8)).isoformat(),
                                         "phrase": "입출금 서비스는 이미 중단"}]}
    assert L.decide(event(), [obs(DAY0 + timedelta(hours=7), upbit=old)])["action"] == "keep"


def test_bithumb_needs_a_sixty_minute_valid_run():
    t = DAY0 + timedelta(hours=8)
    short = [obs(t, bithumb=bt(True)), obs(t + timedelta(minutes=45), bithumb=bt(True))]
    assert L.decide(event(), short)["action"] == "keep"
    broken = short + [obs(t + timedelta(minutes=50), bithumb=bt(False)), obs(t + timedelta(minutes=70), bithumb=bt(True))]
    assert L.decide(event(), broken)["action"] == "keep"
    gap = short + [obs(t + timedelta(minutes=50), bithumb={"error": "HTTP 503"}),
                   obs(t + timedelta(minutes=55), bithumb=bt(False, valid=False)),
                   obs(t + timedelta(minutes=60), bithumb=bt(True))]
    d = L.decide(event(), gap)
    assert d["action"] == "skip" and d["reason"] == "bithumb_wallet" and d["bithumb_valid_snapshots"] == 3


def test_evaluation_is_descriptive_until_review_and_uses_episodes():
    def settled(entry, action, ret):
        return {"event": {"entry_at": entry}, "decision": {"action": action}, "outcome": {"hedged_return_pct": ret}}
    rows = [settled("d1", "skip", -40.0), settled("d1", "skip", -20.0), settled("d2", "keep", 5.0)]
    ev = L.evaluate(rows, DAY0.isoformat(), DAY0 + timedelta(days=30))
    assert ev["verdict"] == "collecting" and ev["skipped_episodes"] == 1 and ev["skipped_mean_pct"] == -30.0
    assert ev["p_value"] is None
    rows = [settled(f"s{i}", "skip", -10.0 - i) for i in range(10)] + [settled(f"k{i}", "keep", 5.0 + i) for i in range(10)]
    ev = L.evaluate(rows, DAY0.isoformat(), DAY0 + timedelta(days=30))
    assert ev["verdict"] == "supported" and ev["p_value"] < 0.01
    assert L.evaluate(rows[:3] + rows[10:], DAY0.isoformat(), DAY0 + timedelta(days=400))["verdict"] == "insufficient"


class _Resp:
    def __init__(self, payload, code=200):
        self.payload, self.status_code = payload, code

    def json(self):
        return self.payload


class FakeClient:
    def __init__(self, notices, bodies, wallet):
        self.notices, self.bodies, self.wallet, self.calls = notices, bodies, wallet, []

    async def get(self, url, params=None, headers=None):
        self.calls.append(url)
        if url == L.UPBIT:
            return _Resp({"data": {"notices": self.notices if params["page"] == 1 else []}})
        if url.startswith(L.UPBIT + "/"):
            return _Resp({"data": {"body": self.bodies[url.rsplit("/", 1)[1]]}})
        if url == L.BITHUMB_WALLET:
            return _Resp(self.wallet)
        return _Resp({}, 404)


@pytest.fixture
def setup(tmp_path, monkeypatch):
    protocol, module = tmp_path / "p.json", tmp_path / "m.py"
    protocol.write_text(json.dumps({"protocol_id": "korea_frozen_skip_v1"}), encoding="utf-8")
    module.write_text("x", encoding="utf-8")
    monkeypatch.setattr(L, "PROTOCOL", protocol)
    monkeypatch.setattr(L, "MODULE", module)
    source = tmp_path / "tracker.json"
    wallet = {"status": "0000", "data": [{"currency": "BTC", "deposit_status": 1, "withdrawal_status": 1}] * 1 +
              [{"currency": f"C{i}", "deposit_status": 1, "withdrawal_status": 1} for i in range(60)] +
              [{"currency": "ABC", "deposit_status": 0, "withdrawal_status": 0}]}
    return tmp_path, source, protocol, wallet


def _source(path, trades):
    path.write_text(json.dumps({"started_at": "2026-10-11T00:00:00+00:00", "trades": trades,
                                "collection_health": {"status": "healthy"}}), encoding="utf-8")


def test_full_cycle_skips_frozen_trade_and_books_the_baseline_return(setup, monkeypatch):
    tmp, source, protocol, wallet = setup
    _source(source, {"OLD|1": {"ticker": "OLD", "symbol": "OLDUSDT", "day0_ms": 0, "discovered_at": "2026-10-01T00:00:00+00:00"}})
    clock = {"now": DAY0 + timedelta(hours=5)}
    monkeypatch.setattr(L, "now_utc", lambda: clock["now"])
    notices = [{"id": 77, "title": "에이비씨(ABC) 거래 유의 종목 지정 안내", "first_listed_at": "2026-11-03T15:00:00+09:00"},
               {"id": 78, "title": "디이에프(DEF) 거래 유의 종목 지정 안내", "first_listed_at": "2026-11-03T15:00:00+09:00"}]
    client = FakeClient(notices, {"77": "<p>에이비씨(ABC)의 입출금 서비스는 이미 중단된 상태로</p>", "78": "입금 서비스가 일시 중단됩니다"}, wallet)
    ledger = L.Ledger(tmp / "out", source)
    asyncio.run(ledger.activate(client))
    clock["now"] = DAY0 + timedelta(hours=7)
    trades = {"OLD|1": {"ticker": "OLD", "symbol": "OLDUSDT", "day0_ms": 0, "discovered_at": "2026-10-01T00:00:00+00:00"},
              "ABC|77": {"ticker": "ABC", "symbol": "ABCUSDT", "notice_id": 77, "source": "upbit_notice",
                         "notice_at": "2026-11-03T06:00:00+00:00", "day0_ms": int(DAY0.timestamp() * 1000),
                         "discovered_at": clock["now"].isoformat(), "status": "waiting_entry"},
              "DEF|78": {"ticker": "DEF", "symbol": "DEFUSDT", "notice_id": 78, "source": "upbit_notice",
                         "notice_at": "2026-11-03T06:00:00+00:00", "day0_ms": int(DAY0.timestamp() * 1000),
                         "discovered_at": clock["now"].isoformat(), "status": "waiting_entry"}}
    _source(source, trades)
    r = asyncio.run(L.Ledger(tmp / "out", source).cycle(client))
    assert r["new_events"] == 2 and r["observed"] == 2 and r["decided"] == 0
    bodies_fetched = sum(c.startswith(L.UPBIT + "/") for c in client.calls)
    clock["now"] = DAY0 + timedelta(hours=8)
    asyncio.run(L.Ledger(tmp / "out", source).cycle(client))
    assert sum(c.startswith(L.UPBIT + "/") for c in client.calls) == bodies_fetched  # each notice read once
    clock["now"] = ENTRY + timedelta(minutes=10)
    r = asyncio.run(L.Ledger(tmp / "out", source).cycle(client))
    assert r["decided"] == 2 and r["observed"] == 0
    for key, ret in (("ABC|77", -41.0), ("DEF|78", 6.0)):
        trades[key].update(status="stopped" if ret < 0 else "closed", return_pct=ret, hedged_return_pct=ret)
    _source(source, trades)
    clock["now"] = ENTRY + timedelta(days=8)
    r = asyncio.run(L.Ledger(tmp / "out", source).cycle(client))
    assert r["settled"] == 2
    s = L.summary(tmp / "out", clock["now"])
    assert s["decisions"] == {"keep": 1, "skip": 1, "late": 0}
    assert s["skips_by_arm"] == {"upbit_notice": 1, "bithumb_wallet": 1}  # ABC is also closed on Bithumb for 60 min
    assert s["books"]["baseline"]["mean_pct"] == -17.5 and s["books"]["filtered"]["mean_pct"] == 3.0
    assert s["books"]["skipped"]["stops"] == 1 and s["evaluation"]["verdict"] == "collecting"
    protocol.write_text(json.dumps({"protocol_id": "changed"}), encoding="utf-8")
    assert asyncio.run(L.Ledger(tmp / "out", source).cycle(client))["outcome"] == "drift_stopped"


def test_trade_first_seen_after_entry_is_late(setup, monkeypatch):
    tmp, source, _, wallet = setup
    _source(source, {})
    clock = {"now": DAY0}
    monkeypatch.setattr(L, "now_utc", lambda: clock["now"])
    client = FakeClient([], {}, wallet)
    asyncio.run(L.Ledger(tmp / "out", source).activate(client))
    clock["now"] = ENTRY + timedelta(hours=2)
    _source(source, {"ABC|1": {"ticker": "ABC", "symbol": "ABCUSDT", "day0_ms": int(DAY0.timestamp() * 1000),
                               "discovered_at": (DAY0 + timedelta(hours=7)).isoformat(), "status": "open"}})
    r = asyncio.run(L.Ledger(tmp / "out", source).cycle(client))
    assert r["new_events"] == 1 and r["decided"] == 1
    assert L.summary(tmp / "out", clock["now"])["decisions"]["late"] == 1


def test_journal_tamper_is_detected(setup, monkeypatch):
    tmp, source, _, wallet = setup
    _source(source, {})
    asyncio.run(L.Ledger(tmp / "out", source).activate(FakeClient([], {}, wallet)))
    path = tmp / "out" / "journal.jsonl"
    path.write_text(path.read_text(encoding="utf-8").replace('"upbit_notices_listed":0', '"upbit_notices_listed":1'),
                    encoding="utf-8")
    assert L.summary(tmp / "out")["status"] == "journal_broken"


def test_second_writer_is_turned_away(setup):
    tmp, source, _, wallet = setup
    _source(source, {})
    with L.exclusive(tmp / "out") as first:
        assert first
        assert asyncio.run(L.Ledger(tmp / "out", source).cycle(FakeClient([], {}, wallet)))["outcome"] == "busy"


def test_registry_row_reports_avoided_return_per_skipped_episode(monkeypatch):
    from core.research import tracker_registry as registry

    spec = next(s for s in registry.TRACKERS if s["id"] == "korea_frozen_skip_v1")
    fake = {"status": "running", "activated_at": DAY0.isoformat(), "pending_decision": 1, "open": 2,
            "decisions": {"keep": 3, "skip": 2, "late": 0}, "books": {},
            "evaluation": {"skipped_episodes": 2, "skipped_mean_pct": -20.0, "verdict": "collecting"}}
    monkeypatch.setattr(L, "summary", lambda *a, **k: fake)
    row = registry.tracker_row(spec)
    assert row["metric_kind"] == "paired_increment" and row["gross_notional_multiplier"] == 2
    assert row["n"] == 2 and row["mean_pct"] == 20.0 and row["total_pct"] == 40.0 and row["min_n"] == 10
    assert row["waiting"] == 1 and row["open"] == 2 and row["open_marks"] == []


def test_reader_ignores_a_record_still_being_written(setup):
    tmp, source, _, wallet = setup
    _source(source, {})
    asyncio.run(L.Ledger(tmp / "out", source).activate(FakeClient([], {}, wallet)))
    with (tmp / "out" / "journal.jsonl").open("ab") as f:
        f.write(b'{"seq":1,"kind":"ev')
    assert L.summary(tmp / "out")["status"] == "running"
