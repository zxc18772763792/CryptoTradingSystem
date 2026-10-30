"""next10xgem1 24h fade paper ledger: market math and the journal life cycle on a fake clock and archive."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import math
import shutil

import pytest

from core.news import alastar_observation as archive
from core.research import next10xgem1_fade_tracker as fade

CHANNEL = fade.CHANNEL
STEP = fade.STEP_MS
HOUR = fade.HOUR_MS
CALL = "POLYXUSDT buy and hold big Move soon"


def row(i, step=STEP, spike=0.0):
    o = 100.0 + 5 * math.sin(i / 50.0) + spike
    c = 100.0 + 5 * math.sin((i + 1) / 50.0)
    return [i * step, o, max(o, c) * 1.0005 + spike, min(o, c) * 0.9995, c, 10.0, i * step + step - 1, 1000.0, 7]


def series(count, *, start=0, step=STEP, spikes=None, gaps=()):
    first = start // step
    return [row(first + i, step, (spikes or {}).get(i, 0.0)) for i in range(count) if i not in gaps]


# ----------------------------------------------------------------------------------------- market math
def test_plain_book_is_the_arithmetic_short_and_liquidates_at_twice_entry():
    entry = 10 * 86_400_000
    bars = fade.bars_by_time(series(300, start=entry))
    plain = fade.short_path(bars, entry, 288, STEP)
    want = (bars[entry][0] - bars[entry + 288 * STEP][0]) / bars[entry][0]
    assert plain["exit_reason"] == "time" and plain["net_price_only"] == pytest.approx(want - .002)
    assert plain["net_price_only_stress"] == pytest.approx(want - .008)
    spiked = fade.bars_by_time(series(300, start=entry, spikes={7: 120.0}))
    assert fade.short_path(spiked, entry, 288, STEP)["exit_reason"] == "liquidated"
    assert fade.short_path(fade.bars_by_time(series(300, start=entry, gaps={3})), entry, 288, STEP) is None


def test_the_catastrophe_stop_fills_at_its_level_or_a_gapped_open_plus_slip():
    entry = 10 * 86_400_000
    bars = fade.bars_by_time(series(300, start=entry))
    e = bars[entry][0]
    o, h, lo, c = bars[entry + 5 * STEP]
    bars[entry + 5 * STEP] = (o, e * 1.6, lo, c)                       # the high crosses 1.5x inside the bar
    stopped = fade.short_path(bars, entry, 288, STEP, .5)
    assert stopped["exit_reason"] == "stop" and stopped["exit_bar_ms"] == entry + 5 * STEP
    assert stopped["net_price_only"] == pytest.approx(-.5 * 1.005 - .005 - .002)
    bars[entry + 5 * STEP] = (e * 1.7, e * 1.8, e * 1.65, e * 1.7)      # opens beyond the level
    gapped = fade.short_path(bars, entry, 288, STEP, .5)
    assert gapped["exit_price"] == pytest.approx(e * 1.7 * 1.005)
    assert fade.books(bars, entry)["plain"]["exit_reason"] == "time"     # 1.8x never reaches 2x


def test_zero_trade_bars_are_kept_as_real_bars():
    rows = series(3)
    rows[1][5], rows[1][8] = 0.0, 0
    assert len(fade.bars_by_time(rows)) == 3


def test_null_profile_equals_a_brute_force_mean_over_every_hourly_start():
    entry = 50 * 86_400_000 + 25 * STEP                                    # a 5m entry inside an hour
    rows = series(32 * 24, start=entry - 31 * 86_400_000 // HOUR * HOUR, step=HOUR, gaps={40, 41, 300},
                  spikes={500: 120.0})
    rows = [r for r in rows if r[0] < entry]
    profile = fade.null_profile(rows, entry)
    bars = fade.bars_by_time(rows, HOUR)
    first = -(-(entry - 30 * 86_400_000) // HOUR) * HOUR
    for name, stop in (("plain", None), ("stop50", .5)):
        values = [o["net_price_only"] for s in range(first, entry - 24 * HOUR + 1, HOUR)
                  if (o := fade.short_path(bars, s, 24, HOUR, stop))]
        block = profile["books"][name]
        assert block["n"] == len(values) and block["status"] == "ok"
        assert block["mean"] == pytest.approx(sum(values) / len(values), abs=1e-12)
    assert profile["funding_in_null"] is False
    short = fade.null_profile([r for r in rows if r[0] >= entry - 5 * 86_400_000], entry)
    assert short["books"]["plain"]["status"] == "insufficient_preentry_history"


def test_funding_counts_inner_settlements_and_boundaries_only_when_adverse():
    entry = 10 * 86_400_000
    outcome = {"entry_price": 100.0, "net_price_only": .01, "exit_reason": "time", "exit_bar_ms": entry + 24 * HOUR}
    proof = {"complete_coverage": True}

    def pay(at, rate):
        return {"symbol": "XUSDT", "fundingTime": at, "fundingRate": str(rate), "markPrice": "100"}

    rows = [pay(entry + 3, .001), pay(entry + 8 * HOUR, .001), pay(entry + 16 * HOUR, -.002), pay(entry + 24 * HOUR + 4, .003),
            pay(entry - 8 * HOUR, .5)]
    assert fade.funding_net(rows, proof, "XUSDT", outcome, entry) == pytest.approx(.01 + .001 - .002)
    adverse = [pay(entry + 2, -.001)]
    assert fade.funding_net(adverse, proof, "XUSDT", outcome, entry) == pytest.approx(.01 - .001)
    assert fade.funding_net(rows, {"complete_coverage": False}, "XUSDT", outcome, entry) is None
    stopped = {**outcome, "exit_reason": "stop", "exit_bar_ms": entry + 8 * HOUR}
    assert fade.funding_net(rows, proof, "XUSDT", stopped, entry) == pytest.approx(.01)   # last-bar receipt not counted
    assert fade.funding_net(rows, proof, "XUSDT", {**outcome, "exit_reason": "liquidated"}, entry) == -1.0
    with pytest.raises(ValueError):
        fade.funding_net([pay(entry, .001), pay(entry, .001)], proof, "XUSDT", outcome, entry)


# ----------------------------------------------------------------------------------------- journal
class FakeMarket:
    """Public-data stand-in: deterministic paths per interval, a fixed universe, never any network."""

    def __init__(self):
        self.fail_klines = False
        self.universe_calls = 0

    def clock_check(self):
        return {"ok": True, "checked_at_utc": fade.now_utc().isoformat()}

    def universe(self):
        self.universe_calls += 1
        return {"fetched_at_utc": fade.now_utc().isoformat(), "symbols": ["1000PEPEUSDT", "BTCUSDT", "POLYXUSDT"]
                + [f"C{i:03d}USDT" for i in range(12)], "evidence": {}, "symbols_sha256": "u" * 64}

    def klines(self, symbol, interval, start_ms, end_ms):
        if self.fail_klines:
            raise RuntimeError("Official public data unavailable")
        step = {"5m": STEP, "1h": HOUR}[interval]
        return [row(i, step) for i in range(-(-start_ms // step), end_ms // step + 1)], {"file": "evidence/f.json"}

    def funding_since(self, symbol, entry_ms):
        return [], {"complete_coverage": True, "file": "evidence/fund.json"}


@pytest.fixture
def world(tmp_path, monkeypatch):
    start = datetime(2026, 11, 3, 8, 0, 0, tzinfo=timezone.utc)
    clock = {"now": start}
    monkeypatch.setattr(fade, "now_utc", lambda: clock["now"])
    protocol = tmp_path / "protocol.json"
    shutil.copyfile(fade.PROTOCOL, protocol)
    expected = json.loads(protocol.read_bytes())["labels"]["development_sample"]
    monkeypatch.setattr(fade, "development_profile", lambda path=None: {k: expected[k] for k in ("history_sha256", "posts", "label_counts")})
    arch = tmp_path / "archive"
    journal = fade.Journal(tmp_path / "ledger", arch, protocol)
    counter = {"id": 39000}

    def poll(at, *messages):
        records = []
        for text, published, extra in messages:
            counter["id"] += 1
            mid = extra.get("message_id", counter["id"])
            records.append({"source": "telegram_alastar", "source_id": f"telegram:{CHANNEL}:{mid}", "title": text[:50],
                            "url": f"https://t.me/{CHANNEL}/{mid}", "content": text, "published_at": published.isoformat(),
                            "payload": {"channel": CHANNEL, "message_id": mid, "original_text": text,
                                        "telegram_edited": extra.get("edited", False), "text_truncated": False,
                                        "media_only": False, "has_photo": False}})
        return archive.observe_records(records, output=arch, mode="live_public_preview", observed_at=at,
                                       raw_preview=b"<html></html>", channel=CHANNEL)

    class World:
        pass

    w = World()
    w.start, w.clock, w.journal, w.poll, w.tmp, w.protocol = start, clock, journal, poll, tmp_path, protocol
    w.state = {"attempts": {}, "errors": [], "seen": {}}
    w.market = FakeMarket()
    w.provider = lambda: w.market

    def advance(seconds):
        end = clock["now"] + timedelta(seconds=seconds)
        while clock["now"] < end:
            clock["now"] = min(end, clock["now"] + timedelta(seconds=450))
            poll(clock["now"])

    w.advance = advance
    for offset in (-300, -200, -100, -5):
        poll(start + timedelta(seconds=offset))
    return w


def activate(w):
    return w.journal.activate({"ok": True, "checked_at_utc": w.clock["now"].isoformat()})


def post(w, text, *, delay=10, edited=False):
    published = w.clock["now"]
    w.poll(published + timedelta(seconds=delay), (text, published, {"edited": edited}))
    w.clock["now"] = published + timedelta(seconds=delay + 5)
    return w.journal.sync(w.state, w.provider)


def run_until_quiet(w, limit=40):
    for _ in range(limit):
        if not w.journal.tick(w.provider, w.state):
            return
        w.clock["now"] += timedelta(seconds=61)


def test_activation_needs_health_clock_and_the_label_profile_and_never_resets(world):
    w = world
    with pytest.raises(RuntimeError, match="prerequisites"):
        w.journal.activate({"ok": False})
    with pytest.raises(RuntimeError, match="development sample"):
        w.journal.activate({"ok": True}, profile={"history_sha256": "x", "posts": 1, "label_counts": {}})
    record = activate(w)
    assert record["kind"] == "activation" and record["payload"]["live_orders"] is False
    with pytest.raises(RuntimeError, match="already exists"):
        activate(w)


def test_only_fresh_unconditional_long_calls_after_activation_are_frozen(world):
    w = world
    w.poll(w.start - timedelta(seconds=3), (CALL, w.start - timedelta(seconds=8), {}))
    activate(w)
    assert w.journal.sync(w.state, w.provider) == []                      # published before the registration
    assert post(w, "Bitcoin is consolidating, stay patient") == []
    assert post(w, "If #POLYX breaks the range, long #POLYX") == []        # conditional
    assert post(w, "#POLYX profit 75% done") == []                          # result post
    _, events = w.journal._state()
    assert events == {}
    assert sorted(w.state["seen"].values()) == ["direct_call:conditional", "other", "recap_or_promo"]


def test_a_call_is_frozen_entered_settled_on_both_books_and_compared_with_its_control(world):
    w = world
    activate(w)
    (freeze,) = post(w, CALL)
    event = freeze["payload"]
    assert event["status"] == "planned" and event["exclusion_reasons"] == [] and event["primary_sample"] is True
    assert event["symbol"] == "POLYXUSDT" and event["side"] == -1 and event["label"] == "direct_call"
    entry = fade._utc(event["planned_entry_at"])
    assert entry == fade.ceil5(entry) and entry >= fade._utc(event["first_received_at_utc"]) + timedelta(seconds=60)
    assert fade._utc(event["planned_exit_at"]) == entry + timedelta(hours=24)

    w.clock["now"] = entry + timedelta(seconds=20)
    run_until_quiet(w)
    _, events = w.journal._state()
    assert events[event["event_id"]]["status"] == "observing"

    w.clock["now"] = entry + timedelta(hours=24, minutes=6)
    run_until_quiet(w)
    _, events = w.journal._state()
    done = events[event["event_id"]]
    assert done["status"] == "completed"
    entry_ms = fade.ms(entry)
    bars = fade.bars_by_time(w.market.klines("POLYXUSDT", "5m", entry_ms, entry_ms + 24 * HOUR)[0])
    want = fade.books(bars, entry_ms)
    for name in ("plain", "stop50"):
        assert done["settlement"]["books"][name]["net_price_only"] == pytest.approx(want[name]["net_price_only"])
        assert done["settlement"]["books"][name]["net_with_funding"] == pytest.approx(want[name]["net_price_only"])
    assert done["null"]["books"]["plain"]["status"] == "ok"
    assert fade.excess(done) == pytest.approx(want["plain"]["net_price_only"] - done["null"]["books"]["plain"]["mean"])
    view = fade.summary(w.journal)
    assert view["retirement"]["n"] == 1 and view["retirement"]["mean_pct"] == pytest.approx(100 * fade.excess(done))
    assert view["books"]["plain"]["pnl_usdt_price_only"] == pytest.approx(round(1000 * want["plain"]["net_price_only"], 2))
    assert view["execution_authorized"] is False and view["review_gate"] is None


def test_universe_dedup_and_the_notional_cap(world):
    w = world
    activate(w)
    assert post(w, "PEPEUSDT buy and hold big Move soon")[0]["payload"]["symbol"] == "1000PEPEUSDT"
    missing = post(w, "ZZZQUSDT buy and hold big Move soon")[0]["payload"]
    assert missing["status"] == "skipped" and "no_usdt_perpetual" in missing["exclusion_reasons"]
    again = post(w, "#PEPE buy and hold big Move soon")[0]["payload"]
    assert again["exclusion_reasons"] == ["same_coin_position_open"] and again["primary_sample"] is False
    for i in range(7):
        assert post(w, f"C{i:03d}USDT buy and hold big Move soon")[0]["payload"]["primary_sample"] is True
    capped = post(w, "C010USDT buy and hold big Move soon")[0]["payload"]
    assert capped["exclusion_reasons"] == ["notional_cap_reached"]
    assert w.market.universe_calls == 1                                      # one snapshot serves six hours
    w.advance(25 * 3600)
    assert post(w, "#PEPE buy and hold big Move soon")[0]["payload"]["primary_sample"] is True


def test_late_first_receipt_is_excluded_but_an_early_visible_edit_is_allowed(world):
    w = world
    activate(w)
    late = post(w, CALL, delay=700)[0]["payload"]
    assert "first_observation_more_than_10_minutes_late" in late["exclusion_reasons"]
    w.advance(25 * 3600)
    edited = post(w, CALL, edited=True)[0]["payload"]
    assert edited["primary_sample"] is True


def test_unavailable_prices_are_retried_then_recorded_as_unavailable(world):
    w = world
    activate(w)
    event = post(w, CALL)[0]["payload"]
    entry = fade._utc(event["planned_entry_at"])
    w.clock["now"] = entry + timedelta(seconds=20)
    run_until_quiet(w)
    w.market.fail_klines = True
    w.clock["now"] = entry + timedelta(hours=24, minutes=6)
    run_until_quiet(w)
    records, events = w.journal._state()
    assert events[event["event_id"]]["status"] == "observing" and w.state["errors"]
    count = len(records)
    w.clock["now"] += timedelta(seconds=30)
    run_until_quiet(w)
    assert len(w.journal._state()[0]) == count                               # no journal flood while retrying
    w.clock["now"] = entry + timedelta(days=8, minutes=10)              # more than 7 days after the planned exit
    run_until_quiet(w)
    _, events = w.journal._state()
    assert events[event["event_id"]]["status"] == "unavailable"
    assert fade.summary(w.journal)["unavailable_count"] == 1


def test_the_gate_stops_new_events_after_the_predeclared_futility(world, monkeypatch):
    w = world
    activate(w)
    monkeypatch.setattr(fade, "FUTILITY_N", 1)
    monkeypatch.setattr(fade, "TARGET_N", 3)
    event = post(w, CALL)[0]["payload"]
    entry = fade._utc(event["planned_entry_at"])
    w.clock["now"] = entry + timedelta(seconds=20)
    run_until_quiet(w)
    w.clock["now"] = entry + timedelta(hours=24, minutes=6)
    run_until_quiet(w)
    records, events = w.journal._state()
    gate = w.journal.gate(records, events)
    if fade.excess(events[event["event_id"]]) <= 0:
        assert gate == "futility245_stop"
        assert "futility245_stop" in post(w, "#PEPE buy and hold big Move soon")[0]["payload"]["exclusion_reasons"]
    else:
        assert gate is None


def test_the_hash_chain_detects_edits_and_the_protocol_cannot_drift(world):
    w = world
    activate(w)
    post(w, CALL)
    first = w.journal.output / "journal" / f"{1:012d}.json"
    raw = first.read_bytes()
    first.write_bytes(raw.replace(b'"paper_only": true', b'"paper_only": false'))
    with pytest.raises(RuntimeError):
        w.journal.verify()
    first.write_bytes(raw)
    w.journal.verify()
    w.protocol.write_bytes(w.protocol.read_bytes() + b"\n")
    with pytest.raises(RuntimeError, match="drift"):
        w.journal.verify()


def test_protocol_must_match_the_implementation(tmp_path):
    protocol = json.loads(fade.PROTOCOL.read_bytes())
    protocol["book"]["max_open_positions"] = 9
    path = tmp_path / "p.json"
    path.write_text(json.dumps(protocol), encoding="utf-8")
    with pytest.raises(ValueError, match="differ"):
        fade.Journal(tmp_path / "ledger", tmp_path / "archive", path)


def test_summary_tick_and_worker_before_activation(world, monkeypatch):
    view = fade.summary(world.journal)
    assert view["tracker_status_label"] == "未激活" and view["retirement"]["mean_pct"] is None
    state = fade.Tracker(world.journal, world.provider).tick()
    assert state["live_orders"] is False and "last_actions" not in state

    import asyncio
    beats = []

    class Missing:
        def __init__(self, *args, **kwargs):
            raise FileNotFoundError("protocol missing")

    monkeypatch.setattr(fade, "Tracker", Missing)

    async def run():
        stop = asyncio.Event()
        task = asyncio.create_task(fade.worker(stop, heartbeat=beats.append))
        await asyncio.sleep(0.05)
        stop.set()
        await asyncio.wait_for(task, 2)

    asyncio.run(run())
    assert beats == [False]


def test_the_module_never_names_an_order_or_account_endpoint():
    source = fade.Path(fade.__file__).read_text(encoding="utf-8")
    for forbidden in ("/fapi/v1/order", "/fapi/v2/account", "/fapi/v1/leverage", "create_order", "api_key", "apiKey"):
        assert forbidden not in source


# ----------------------------------------------------------------------------------------- v2 operations
def test_a_failed_list_refresh_keeps_the_previous_list_instead_of_skipping_the_call(world):
    w = world
    activate(w)
    assert post(w, CALL)[0]["payload"]["primary_sample"] is True
    w.advance(25 * 3600)                                                     # list is now older than 6 hours

    def dropped():
        raise RuntimeError("Official public data unavailable (SSLError)")

    w.market.universe = dropped
    event = post(w, "#PEPE buy and hold big Move soon")[0]["payload"]
    assert event["symbol"] == "1000PEPEUSDT" and event["primary_sample"] is True
    assert any(e.get("stage") == "universe" for e in w.state["errors"])
    w.state["universe"]["fetched_at_utc"] = (w.clock["now"] - timedelta(days=8)).isoformat()
    stale = post(w, "BTCUSDT buy and hold big Move soon")[0]["payload"]
    assert stale["exclusion_reasons"] == ["universe_unavailable"]          # nothing usable within 7 days


def test_tick_keeps_the_list_and_clock_warm_and_a_freeze_reuses_a_recent_clock_reading(world):
    w = world
    activate(w)
    tracker = fade.Tracker(w.journal, w.provider)
    tracker.tick()
    state = fade.runtime(w.journal.output)
    assert state["universe"]["symbols"] and state["last_clock_check"]["ok"] is True
    calls = []

    def dropped():
        calls.append(1)
        raise RuntimeError("Official public data unavailable (SSLError)")

    w.market.clock_check = dropped
    w.state = state
    w.clock["now"] += timedelta(minutes=3)
    assert post(w, CALL)[0]["payload"]["primary_sample"] is True              # 3-minute-old reading still valid
    w.advance(25 * 3600)
    w.state["universe"]["fetched_at_utc"] = w.clock["now"].isoformat()
    late = post(w, "#PEPE buy and hold big Move soon")[0]["payload"]
    assert late["exclusion_reasons"] == ["clock_unreachable"] and calls
    w.market.clock_check = lambda: {"ok": False, "checked_at_utc": fade.now_utc().isoformat()}
    bad = post(w, "BTCUSDT buy and hold big Move soon")[0]["payload"]
    assert bad["exclusion_reasons"] == ["clock_gate_failed"]


def test_transport_drops_are_retried_but_refusals_are_not(tmp_path, monkeypatch):
    import requests

    class Response:
        def __init__(self, status, body=b'{"serverTime": 1}'):
            self.status_code, self.content = status, body

        def raise_for_status(self):
            if self.status_code >= 400:
                raise requests.exceptions.HTTPError(str(self.status_code))

        def json(self):
            return json.loads(self.content)

    market = fade.PublicMarketData(tmp_path)
    attempts = []

    def flaky(url, params=None, timeout=None):
        attempts.append(url)
        if len(attempts) < 3:
            raise requests.exceptions.SSLError("UNEXPECTED_EOF_WHILE_READING")
        return Response(200)

    monkeypatch.setattr(market.session, "get", flaky)
    rows, _, _, _ = market._fetch(fade.TIME_URL, {})
    assert rows == {"serverTime": 1} and len(attempts) == 3 and market.calls == 1
    refused = fade.PublicMarketData(tmp_path)
    attempts.clear()
    monkeypatch.setattr(refused.session, "get", lambda url, params=None, timeout=None: attempts.append(url) or Response(403))
    with pytest.raises(RuntimeError, match="unavailable"):
        refused._fetch(fade.TIME_URL, {})
    assert len(attempts) == 1


def test_the_retired_v1_copy_is_byte_identical_and_only_settles(tmp_path, monkeypatch):
    import asyncio
    from core.research import next10xgem1_fade_v1_frozen as v1
    assert fade.sha(fade.Path(v1.__file__).read_bytes()) == \
        "f17854a9cd7c1ada7c1e7328f8a64254881d5088e5252238e5ceef09b7dfcc17"   # hash pinned in the v1 journal
    assert v1.PROTOCOL_ID == "next10xgem1_fade_24h_v1" and fade.PROTOCOL_ID == "next10xgem1_fade_24h_v2"
    calls = []

    class Journal:
        output = tmp_path

        def tick(self, factory, state):
            calls.append("tick")
            return []

        def sync(self, state, factory):
            raise AssertionError("a retired journal must not freeze new calls")

    class Retired:
        def __init__(self, managed=False):
            self.journal, self.factory = Journal(), None

        def save(self, state):
            (tmp_path / "runtime.json").write_text(json.dumps(state), encoding="utf-8")

    monkeypatch.setattr(v1, "Tracker", Retired)
    beats = []

    async def run():
        stop = asyncio.Event()
        task = asyncio.create_task(fade.settle_retired_v1(stop, heartbeat=beats.append))
        while not beats:
            await asyncio.sleep(0.01)
        stop.set()
        await asyncio.wait_for(task, 2)

    asyncio.run(run())
    assert calls == ["tick"] and beats == [True]
    assert json.loads((tmp_path / "runtime.json").read_text(encoding="utf-8"))["retired_to"] == fade.PROTOCOL_ID
