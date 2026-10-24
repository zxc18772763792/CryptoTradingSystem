"""CryptoQuant Alert shadow ledger: parser, market math and the full journal life cycle on a fake clock."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path

import pytest

from core.news import alastar_observation as archive
from core.research import cryptoquant_alert_shadow as cq

CHANNEL = cq.CHANNEL
SIMPLE = "\U0001F433\U0001F433 2,000.64 #BTC ($162,442,109)\naggregated inflows to #Binance\n\nView Chart | @cryptoquant_alert"
MINERS = "\U0001F6A8 3,200.77 #BTC ($371,164,545)\naggregated flows from miners to exchanges\n\nView Chart | @cryptoquant_alert"
UNUSUAL_IN = ("\U0001F6A8 Unusual Exchange Inflows\n5,168.03 #BTC ($510,748,864)\naggregated inflows to spot exchanges in an hour\n\n"
              "1. Coinbase_advanced 2,872 BTC (55%)\n2. Coinbase_prime 2,048 BTC (39%)\n3. Gemini 158 BTC (3%)\n\n"
              "Please note that large deposits into exchanges that support a custody solution can be internal transfers.")
UNUSUAL_OUT = ("✅✅ Unusual Exchange Outflows\n3,100.5 #BTC ($250,000,000)\naggregated outflows from multiple exchanges in an hour\n\n"
               "1. Coinbase_prime 1,500 BTC (48%)\n2. Binance 900 BTC (29%)\n\nPlease note that large withdrawals can be custody moves.")


# --------------------------------------------------------------------------------------------- parser
def test_every_observed_template_parses_with_its_side():
    simple = cq.parse_alert(SIMPLE)
    assert (simple["template"], simple["kind"], simple["side"], simple["venue"], simple["tier"]) == ("simple_inflow", "exchange_inflow", -1, "Binance", 2)
    assert simple["btc"] == 2000.64 and simple["usd"] == 162442109.0 and simple["asset"] == "BTC"
    miners = cq.parse_alert(MINERS)
    assert (miners["kind"], miners["side"], miners["venue"]) == ("miner_to_exchange", -1, None)
    inflow = cq.parse_alert(UNUSUAL_IN)
    assert (inflow["template"], inflow["scope"], inflow["side"]) == ("unusual_inflow", "spot", -1)
    assert [r["venue"] for r in inflow["breakdown"]] == ["Coinbase_advanced", "Coinbase_prime", "Gemini"]
    outflow = cq.parse_alert(UNUSUAL_OUT)
    assert (outflow["template"], outflow["kind"], outflow["side"], outflow["scope"], outflow["tier"]) == ("unusual_outflow", "exchange_outflow", 1, "multiple", 2)


def test_multiword_venue_and_whitespace_variants_still_parse():
    text = "\U0001F6A8 1,958.34 #BTC ($85,014,599)\naggregated inflows to #Coinbase Pro\n\nView Chart | @cryptoquant_alert"
    assert cq.parse_alert(text)["venue"] == "Coinbase Pro"
    padded = "  " + SIMPLE.replace(" #BTC", "\xa0#BTC").replace("\n", "  \n") + "  "
    assert cq.parse_alert(padded)["btc"] == 2000.64


@pytest.mark.parametrize("text", [
    "It was an internal transfer according to Binance team.",
    "Join us today on X Spaces! As we dive into the ETF boom.",
    SIMPLE.replace("aggregated inflows to", "aggregated outflows from"),   # a template never seen: not traded
    SIMPLE + "\nextra line",
    SIMPLE.replace("View Chart | @cryptoquant_alert", "View Chart | @someone_else"),
    SIMPLE.replace("2,000.64 #BTC", "two thousand #BTC"),
    "", "\U0001F433",
])
def test_anything_else_is_not_an_alert(text):
    assert cq.parse_alert(text) is None


def test_non_btc_asset_parses_so_it_can_be_logged_as_ineligible():
    assert cq.parse_alert(SIMPLE.replace("#BTC", "#ETH"))["asset"] == "ETH"


def test_development_sample_profile_matches_the_pre_registered_one():
    if not cq.DEV_SAMPLE.exists():
        pytest.skip("the 10/20 offline sample is local data")
    profile = cq.development_sample_profile()
    assert profile["messages"] == 1994 and sum(profile["by_template"].values()) == 1994
    assert profile["by_template"]["unparsed"] == 13  # chatter and promotions, none of them flow alerts


# ----------------------------------------------------------------------------------------- market math
STEP = 300_000


def row(i, spike=0.0):
    o = 100.0 + 5 * math.sin(i / 50.0) + spike
    c = 100.0 + 5 * math.sin((i + 1) / 50.0)
    return [i * STEP, o, max(o, c) * 1.0005, min(o, c) * 0.9995, c, 10.0, i * STEP + STEP - 1, 1000.0, 7]


def series(count, *, start=0, spikes=None, gaps=()):
    """Deterministic 5m rows [ms, o, h, l, c, volume, closeTime, quote, trades], indexed by absolute time."""
    first = start // STEP
    return [row(first + i, (spikes or {}).get(i, 0.0)) for i in range(count) if i not in gaps]


@pytest.mark.parametrize("side", [-1, 1])
def test_null_profile_equals_a_brute_force_mean_over_every_valid_start(side):
    entry_ms = 40 * 86_400_000
    rows = series(8641 + 10, start=entry_ms - 30 * 86_400_000, gaps={500, 501, 4000, 6000}, spikes={1000: 300.0})
    rows = [r for r in rows if r[0] <= entry_ms]
    entry = datetime.fromtimestamp(entry_ms / 1000, timezone.utc)
    profile = cq.null_profile(rows, entry, side)
    bars = cq.bars_by_time(rows)
    for hours in cq.HOURS:
        starts = range(entry_ms - 30 * 86_400_000, entry_ms - hours * 3_600_000 + 1, STEP)
        values = [o["net_price_only"] for s in starts if (o := cq.price_outcome(bars, datetime.fromtimestamp(s / 1000, timezone.utc), hours, side))]
        block = profile["horizons"][str(hours)]
        assert block["n"] == len(values) and block["status"] == "ok"
        assert block["mean"] == pytest.approx(sum(values) / len(values), abs=1e-12)
    assert profile["funding_in_null"] is False and profile["cost_bps"] == 20


def test_liquidation_path_and_cost_and_missing_bar_rules():
    entry_ms = 10 * 86_400_000
    rows = series(400, start=entry_ms)
    bars = cq.bars_by_time(rows)
    entry = datetime.fromtimestamp(entry_ms / 1000, timezone.utc)
    short = cq.price_outcome(bars, entry, 1, -1)
    assert short["net_price_only"] == pytest.approx(-(bars[entry_ms + 12 * STEP][0] / bars[entry_ms][0] - 1) - 0.002)
    assert cq.price_outcome(cq.bars_by_time(series(400, start=entry_ms, spikes={5: 120.0})), entry, 1, -1)["liquidated"] is True
    assert cq.price_outcome(cq.bars_by_time(series(400, start=entry_ms, gaps={3})), entry, 1, -1) is None  # gaps are never interpolated
    with pytest.raises(ValueError):
        cq.price_outcome(bars, entry, 5, -1)


def test_funding_boundary_payments_count_only_when_adverse():
    outcome = {"net_price_only": 0.01, "liquidated": False}
    a = datetime(2026, 11, 1, tzinfo=timezone.utc)
    b = a + timedelta(hours=4)
    proof = {"complete_query_coverage": True}
    def row(at, rate):
        return {"symbol": "BTCUSDT", "fundingTime": int(at.timestamp() * 1000), "fundingRate": str(rate), "markPrice": "100"}
    short = cq.funding_net([row(a, 0.001), row(a + timedelta(hours=2), 0.001)], proof, 100.0, -1, a, b, outcome)
    assert short == pytest.approx(0.01 + 0.001)          # boundary payment received is not counted; the inner one is
    long_ = cq.funding_net([row(a, 0.001), row(a + timedelta(hours=2), 0.001)], proof, 100.0, 1, a, b, outcome)
    assert long_ == pytest.approx(0.01 - 0.002)          # a paid boundary counts
    assert cq.funding_net([], {"complete_query_coverage": False}, 100.0, 1, a, b, outcome) is None
    with pytest.raises(ValueError):
        cq.funding_net([row(a - timedelta(hours=1), 0.001)], proof, 100.0, 1, a, b, outcome)


# ----------------------------------------------------------------------------------------- journal
class FakeMarket:
    """Public-data stand-in: a deterministic BTC path anchored at the epoch, never any network."""

    def __init__(self, fail_klines=False):
        self.fail_klines = fail_klines

    def clock_check(self):
        return {"ok": True, "checked_at_utc": cq.now_utc().isoformat()}

    def klines(self, start, end):
        if self.fail_klines:
            raise RuntimeError("Official public data unavailable")
        first, last = int(cq._utc(start).timestamp() * 1000) // STEP, int(cq._utc(end).timestamp() * 1000) // STEP
        return [row(i) for i in range(first, last + 1)], [{"file": "evidence/fake.json", "sha256": "0" * 64}]

    def funding(self, start, end):
        return [], {"complete_query_coverage": True, "file": "evidence/fake-funding.json", "sha256": "1" * 64}


@pytest.fixture
def world(tmp_path, monkeypatch):
    start = datetime(2026, 11, 3, 8, 0, 0, tzinfo=timezone.utc)
    clock = {"now": start}
    monkeypatch.setattr(cq, "now_utc", lambda: clock["now"])
    arch = tmp_path / "archive"
    profile = {"file": "x", "sha256": "a" * 64, "messages": 3, "by_template": {"simple_inflow": 3}}
    monkeypatch.setattr(cq, "development_sample_profile", lambda path=None: profile)
    protocol = tmp_path / "protocol.json"
    protocol.write_text(json.dumps({
        "protocol_id": cq.PROTOCOL_ID, "execution_authorized": False, "promotion_allowed": False,
        "book": {"symbol": "BTCUSDT", "holding_hours": list(cq.HOURS), "primary_holding_hours": cq.PRIMARY_HOURS, "round_trip_cost_bps": 20},
        "control": {"minimum_null_starts": 2000}, "development_sample": profile}), encoding="utf-8")
    journal = cq.Journal(tmp_path / "shadow", arch, protocol)
    state = {"unparsed": {}, "attempts": {}, "errors": []}
    counter = {"id": 6200}

    def poll(at, *messages):
        """One live public-page poll at `at`; messages are (text, published_at, extra) tuples."""
        records = []
        for text, published, extra in messages:
            counter["id"] += 1
            mid = extra.get("message_id", counter["id"])
            records.append({"source": "telegram_alastar", "source_id": f"telegram:{CHANNEL}:{mid}", "title": text[:50],
                            "url": f"https://t.me/{CHANNEL}/{mid}", "content": text, "published_at": published.isoformat(),
                            "payload": {"channel": CHANNEL, "message_id": mid, "original_text": text, "telegram_edited": extra.get("edited", False),
                                        "text_truncated": False, "media_only": False, "has_photo": False}})
        return archive.observe_records(records, output=arch, mode="live_public_preview", observed_at=at, raw_preview=b"<html></html>", channel=CHANNEL)

    class World:
        pass

    w = World()
    w.start, w.clock, w.arch, w.journal, w.state, w.poll, w.tmp = start, clock, arch, journal, state, poll, tmp_path
    def advance(seconds):
        end = clock["now"] + timedelta(seconds=seconds)
        while clock["now"] < end:
            clock["now"] = min(end, clock["now"] + timedelta(seconds=450))
            poll(clock["now"])

    w.advance = advance
    w.market = FakeMarket()
    w.provider = lambda: w.market
    for offset in (-300, -200, -100, -5):
        poll(start + timedelta(seconds=offset))
    return w


def activate(w):
    return w.journal.activate({"ok": True, "checked_at_utc": w.clock["now"].isoformat()})


def announce(w, text, *, edited=False):
    """The alert is published now and the next live poll receives it 10 s later."""
    published = w.clock["now"]
    w.poll(published + timedelta(seconds=10), (text, published, {"edited": edited}))
    w.clock["now"] = published + timedelta(seconds=15)
    return w.journal.sync(w.state, w.provider)


def run_until_quiet(w, limit=40):
    for _ in range(limit):
        if not w.journal.tick(w.provider, w.state):
            return
        w.clock["now"] += timedelta(seconds=61)


def test_activation_needs_a_healthy_collector_and_never_resets(world):
    w = world
    with pytest.raises(RuntimeError, match="prerequisites"):
        w.journal.activate({"ok": False})
    w.clock["now"] += timedelta(hours=2)  # polls stop: the collector is stale
    with pytest.raises(RuntimeError, match="prerequisites"):
        activate(w)
    w.clock["now"] = w.start
    record = activate(w)
    assert record["kind"] == "activation" and record["payload"]["live_orders"] is False
    with pytest.raises(RuntimeError, match="already exists"):
        activate(w)


def test_alerts_before_activation_and_unparseable_text_are_never_frozen(world):
    w = world
    w.poll(w.start - timedelta(seconds=3), (SIMPLE, w.start - timedelta(seconds=8), {}))
    activate(w)
    assert w.journal.sync(w.state, w.provider) == []                   # published before the registration
    assert announce(w, "Join us today on X Spaces!") == []
    assert len(w.state["unparsed"]) == 1
    _, events = w.journal._state()
    assert events == {}


def test_a_fresh_alert_is_frozen_entered_settled_and_compared_with_its_control(world):
    w = world
    activate(w)
    (freeze,) = announce(w, SIMPLE)
    event = freeze["payload"]
    assert event["status"] == "planned" and event["exclusion_reasons"] == [] and event["primary_sample"] is True
    assert event["side"] == -1 and event["symbol"] == "BTCUSDT" and event["alert"]["btc"] == 2000.64
    entry = cq._utc(event["planned_entry_at"])
    assert entry == cq.ceil5(entry) and entry >= cq._utc(event["first_received_at_utc"]) + timedelta(seconds=60)
    assert cq._utc(event["planned_entry_at"]) <= cq._utc(event["published_at_utc"]) + timedelta(minutes=30)
    assert event["first_receipt_run"]["file"].startswith("runs/")

    w.clock["now"] = entry + timedelta(seconds=20)
    run_until_quiet(w)                                                  # entry, then nothing is due yet
    _, events = w.journal._state()
    assert events[event["event_id"]]["status"] == "observing"
    assert events[event["event_id"]]["entry"]["price"] == cq.bars_by_time(FakeMarket().klines(entry, entry)[0])[int(entry.timestamp() * 1000)][0]

    w.clock["now"] = entry + timedelta(hours=72, minutes=6)
    run_until_quiet(w)
    records, events = w.journal._state()
    done = events[event["event_id"]]
    assert done["status"] == "completed" and set(done["outcomes"]) == {"1", "4", "24", "72"}
    market = FakeMarket()
    bars = cq.bars_by_time(market.klines(entry, entry + timedelta(hours=72))[0])
    for hours in cq.HOURS:
        want = cq.price_outcome(bars, entry, hours, -1)
        assert done["outcomes"][str(hours)]["net_price_only"] == pytest.approx(want["net_price_only"])
        assert done["outcomes"][str(hours)]["net_with_funding"] == pytest.approx(want["net_price_only"])  # no funding rows in the fake
    assert done["null"]["horizons"]["24"]["status"] == "ok" and "return_prior_24h" in done["null"]["context"]
    assert cq.excess(done, 24) == pytest.approx(done["outcomes"]["24"]["net_price_only"] - done["null"]["horizons"]["24"]["mean"])
    view = cq.summary(w.journal)
    assert view["retirement"]["n"] == 1 and view["retirement"]["mean_pct"] == pytest.approx(100 * cq.excess(done, 24))
    assert view["paper_only"] is True and view["execution_authorized"] is False and view["review_gate"] is None
    assert [h["hours"] for h in view["horizon_comparison"]] == list(cq.HOURS)


def test_side_comes_from_the_template_and_overlapping_alerts_leave_the_primary_sample(world):
    w = world
    activate(w)
    first = announce(w, SIMPLE)[0]["payload"]
    w.advance(3 * 3600)
    second = announce(w, UNUSUAL_OUT)[0]["payload"]           # entry 3h after the first: inside the 24h window
    w.advance(25 * 3600)
    third = announce(w, MINERS)[0]["payload"]                 # more than 24h after every earlier entry
    w.advance(5 * 3600)
    fourth = announce(w, UNUSUAL_IN)[0]["payload"]            # 5h after the third
    assert [e["side"] for e in (first, second, third, fourth)] == [-1, 1, -1, -1]
    assert [e["primary_sample"] for e in (first, second, third, fourth)] == [True, False, True, False]
    assert second["overlaps_prior_entry_within_24h"] is True and second["status"] == "planned"
    assert third["overlaps_prior_entry_within_24h"] is False


@pytest.mark.parametrize("text,edited,reason", [
    (SIMPLE.replace("#BTC", "#ETH"), False, "non_btc_asset"),
    (SIMPLE, True, "visible_edit"),
])
def test_ineligible_alerts_are_recorded_with_their_reason_and_never_entered(world, text, edited, reason):
    w = world
    activate(w)
    (freeze,) = announce(w, text, edited=edited)
    event = freeze["payload"]
    assert event["status"] == "skipped" and reason in event["exclusion_reasons"] and event["primary_sample"] is False
    w.clock["now"] += timedelta(hours=1)
    run_until_quiet(w)
    _, events = w.journal._state()
    assert events[event["event_id"]].get("entry") is None


def test_a_late_first_receipt_is_excluded_not_backfilled(world):
    w = world
    activate(w)
    published = w.clock["now"] + timedelta(seconds=5)
    w.poll(published + timedelta(minutes=11), (SIMPLE, published, {}))     # first poll that saw it was 11 minutes late
    w.clock["now"] = published + timedelta(minutes=11, seconds=5)
    (freeze,) = w.journal.sync(w.state, w.provider)
    assert freeze["payload"]["status"] == "skipped"
    assert "first_observation_more_than_10_minutes_late" in freeze["payload"]["exclusion_reasons"]


def test_unavailable_market_data_is_retried_without_flooding_or_inventing_fills(world):
    w = world
    activate(w)
    (freeze,) = announce(w, SIMPLE)
    entry = cq._utc(freeze["payload"]["planned_entry_at"])
    w.market.fail_klines = True
    w.clock["now"] = entry + timedelta(seconds=20)
    for _ in range(5):
        w.journal.tick(w.provider, w.state)
    _, events = w.journal._state()
    assert events[freeze["payload"]["event_id"]]["status"] == "planned"       # no price, no fill
    w.clock["now"] = entry + timedelta(hours=1, minutes=1)
    w.journal.tick(w.provider, w.state)
    _, events = w.journal._state()
    assert events[freeze["payload"]["event_id"]]["status"] == "skipped"
    assert events[freeze["payload"]["event_id"]]["missed"]["reason"] == "entry_price_unavailable"


def test_the_hash_chain_detects_edits_and_the_protocol_cannot_drift(world, tmp_path):
    w = world
    activate(w)
    announce(w, SIMPLE)
    assert w.journal.verify()["records"] == 2
    second = cq.Journal(w.journal.output, w.arch, w.journal.protocol_path)       # a fresh process re-reads everything
    assert second.verify()["records"] == 2
    path = w.journal.output / "journal" / f"{2:012d}.json"
    record = json.loads(path.read_bytes())
    record["payload"]["side"] = 1
    path.write_bytes(json.dumps(record).encode("utf-8"))
    with pytest.raises(RuntimeError, match="hash|checkpoint"):
        cq.Journal(w.journal.output, w.arch, w.journal.protocol_path).verify()


def test_protocol_must_match_the_implementation(world, tmp_path):
    w = world
    for key, value in (("book", {"symbol": "ETHUSDT", "holding_hours": list(cq.HOURS), "primary_holding_hours": 24, "round_trip_cost_bps": 20}),
                       ("execution_authorized", True)):
        broken = json.loads(w.journal.protocol_path.read_text(encoding="utf-8"))
        broken[key] = value
        path = tmp_path / f"broken_{key}.json"
        path.write_text(json.dumps(broken), encoding="utf-8")
        with pytest.raises(ValueError, match="Protocol parameters"):
            cq.Journal(tmp_path / "other", w.arch, path)


def test_the_gate_stops_new_observations_after_the_predeclared_counts(world):
    w = world
    activate(w)
    records, events = w.journal._state()
    assert w.journal.gate(records, events) is None
    fake = {f"e{i}": {"primary_sample": True, "status": "observing"} for i in range(40)}
    assert w.journal.gate(records, fake) == "await40_maturity"
    w.clock["now"] = w.start + timedelta(days=800)
    assert w.journal.gate(records, {}) == "maximum24months_reached"


def test_summary_of_a_journal_that_is_not_activated_is_an_honest_empty_state(world):
    view = cq.summary(world.journal)
    assert view["tracker_status_label"] == "未激活" and view["retirement"]["n"] == 0 and view["retirement"]["mean_pct"] is None
    assert view["execution_authorized"] is False


def test_tracker_tick_idles_until_activation_and_then_runs_both_passes(world):
    w = world
    tracker = cq.Tracker(w.journal, w.provider)
    state = tracker.tick()
    assert state["live_orders"] is False and "last_actions" not in state
    activate(w)
    w.poll(w.clock["now"] + timedelta(seconds=10), (SIMPLE, w.clock["now"] + timedelta(seconds=2), {}))
    w.clock["now"] += timedelta(seconds=20)
    state = tracker.tick()
    assert [a["kind"] for a in state["last_actions"]] == ["freeze"]


def test_worker_waits_for_a_missing_protocol_instead_of_crashing(monkeypatch):
    import asyncio

    beats = []

    class Missing:
        def __init__(self, *args, **kwargs):
            raise FileNotFoundError("protocol is local data")

    monkeypatch.setattr(cq, "Tracker", Missing)

    async def run():
        stop = asyncio.Event()
        task = asyncio.create_task(cq.worker(stop, heartbeat=beats.append))
        await asyncio.sleep(0.05)
        stop.set()
        await asyncio.wait_for(task, 2)

    asyncio.run(run())
    assert beats == [False]
