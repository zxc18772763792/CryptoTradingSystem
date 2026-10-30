from datetime import datetime, timedelta, timezone

import pytest

from core.news.alastar_observation import observe_records
from core.research import alastartrader1_ledger as led_mod
from core.research import hub_runup_ledger as hub

T0 = datetime(2026, 11, 2, 12, 0, tzinfo=timezone.utc)
BEAMX = ("\U0001F53A $BEAMX/USDT — LONG | 50x\n\n\U0001F3AF ENTRY: 0.004040\n\n"
         "\U0001F680 TP: 0.004095 → 0.004140 → 0.004180 → 0.004230\n\n\U0001F6D1 SL: 0.003760\n\n"
         "⚠️ RISK MANAGEMENT: Manage risk smartly.")
ON = ("\U0001F53A $ON/USDT — LONG | 30x\n\n\U0001F3AF ENTRY: 0.2480 → 0.2400\n\n"
      "\U0001F680 TP: 0.2535 → 0.2700 → 0.3200 → 0.3600\n\n\U0001F6D1 SL: 0.2140")
RECAP = "\U0001F3C6 $BEAMX/USDT — TP3 HIT ✅\n\n\U0001F4B0 PROFIT: +201.32% \U0001F4C8"
ANALYSIS = "#BTC/USDT ANALYSIS\n\nBitcoin is consolidating within the descending channel. A rebound could follow."


class FakeMarket:
    """Flat market at ``price``; never any network."""

    def __init__(self, price=0.2324):
        self.price = price

    def klines(self, symbol, interval, start, end):
        step = hub.STEP[interval]
        rows, t = [], hub.ms(start) // step * step
        while t < min(hub.ms(end), hub.ms(hub.now_utc())):
            p = self.price
            rows.append([t, p, p * 1.001, p * 0.999, p, 0, t + step - 1])
            t += step
        return rows

    def funding(self, symbol, since):
        return {"rows": [], "complete": True}


@pytest.fixture
def world(tmp_path, monkeypatch):
    now = {"t": T0}
    monkeypatch.setattr(hub, "now_utc", lambda: now["t"])
    monkeypatch.setattr(led_mod, "now_utc", lambda: now["t"])
    archive = tmp_path / "archive"
    ledger = led_mod.Ledger(tmp_path / "ledger", archive)
    ledger.journal.append("activation", {"protocol": led_mod.PROTOCOL_ID})
    rt = {"clock": {"ok": True, "checked_at": T0.isoformat()},
          "universe": {"fetched_at": T0.isoformat(), "symbols": ["ONUSDT", "BEAMXUSDT", "BTCUSDT"]}}

    def post(number, text, published, delay=60):
        observe_records([{"source_id": f"telegram:alastartrader1:{number}", "content": text,
                          "published_at": published.isoformat(), "payload": {"message_id": number, "channel": "alastartrader1"}}],
                        archive, channel="alastartrader1", mode="live_public_preview",
                        observed_at=published + timedelta(seconds=delay), raw_preview=b"public preview")

    class World:
        pass

    w = World()
    w.now, w.ledger, w.rt, w.post = now, ledger, rt, post
    return w


def decisions(ledger):
    return {int(k): v["decision"] for k, v in led_mod.state(ledger.journal)["events"].items() if "decision" in v}


def test_structured_calls_parse_as_full_brackets_and_recaps_do_not():
    beamx = led_mod.parse(BEAMX)
    assert beamx["eligible"] and beamx["completeness"] == "full_bracket"
    assert (beamx["symbol"], beamx["side"], beamx["entry_type"], beamx["leverage"]) == ("BEAMX", 1, "limit", 50.0)
    assert beamx["legs"] == [0.00404] and beamx["targets"] == [0.004095, 0.00414, 0.00418, 0.00423]
    assert beamx["stop"] == 0.00376
    on = led_mod.parse(ON)
    assert on["legs"] == [0.248, 0.24] and on["stop"] == 0.214 and on["eligible"]
    assert led_mod.parse(RECAP) == {"signal": False} and led_mod.parse(ANALYSIS) == {"signal": False}


def test_a_fresh_call_is_entered_and_other_posts_are_recorded_not_traded(world):
    w = world
    w.post(1, ON, T0 - timedelta(minutes=5))                               # before activation: never counted
    w.post(2, ON, T0 + timedelta(minutes=1))
    w.post(3, RECAP, T0 + timedelta(minutes=1))
    w.post(4, BEAMX, T0 + timedelta(minutes=1), delay=700)                  # first seen > 10 min late
    w.now["t"] = T0 + timedelta(minutes=2, seconds=10)
    w.ledger.decide(w.rt, FakeMarket())
    d = decisions(w.ledger)
    assert 1 not in d
    assert d[2]["entered"] and d[2]["native"] == "ONUSDT"
    assert d[2]["entry_at"] == (T0 + timedelta(minutes=3)).isoformat()      # next full minute after the freeze
    assert d[3]["reason"] == "not_a_signal" and not d[3]["entered"]
    assert d[4]["reason"].startswith("collector_first_observation_more_than_10_minutes_late")
    w.post(5, ON, T0 + timedelta(minutes=3))
    w.now["t"] = T0 + timedelta(minutes=4, seconds=10)
    w.ledger.decide(w.rt, FakeMarket())
    assert decisions(w.ledger)[5]["reason"] == "skipped_overlap"           # ON already filled and open


def test_a_stale_clock_or_list_keeps_the_call_out(world):
    w = world
    w.post(2, ON, T0 + timedelta(minutes=20))
    w.now["t"] = T0 + timedelta(minutes=21, seconds=10)
    w.ledger.decide(w.rt, FakeMarket())
    assert decisions(w.ledger)[2]["reason"] == "clock_unverified"
    w.rt["clock"]["checked_at"] = w.now["t"].isoformat()
    w.rt["universe"]["fetched_at"] = (w.now["t"] - timedelta(days=8)).isoformat()
    w.post(3, BEAMX, T0 + timedelta(minutes=21))
    w.ledger.decide(w.rt, FakeMarket())
    assert decisions(w.ledger)[3]["reason"] == "universe_unavailable"


def test_a_call_posted_after_its_targets_traded_is_cancelled_not_traded(world):
    w = world
    w.post(2, BEAMX, T0 + timedelta(minutes=1))
    w.now["t"] = T0 + timedelta(minutes=2, seconds=10)
    market = FakeMarket(price=0.00463)                                      # above every target, as on 10/30
    w.ledger.decide(w.rt, market)
    w.now["t"] = T0 + timedelta(minutes=10)
    w.ledger.settle(market)
    out = led_mod.state(w.ledger.journal)["events"]["2"]["outcome"]
    assert out["status"] == "cancelled" and out["exit_reason"] == "tp1_before_entry" and out["ret_on_filled"] is None
    s = led_mod.summary(w.ledger.output)
    assert s["traded"] == 0 and s["not_filled"] == {"tp1_before_entry": 1}


def test_a_changed_pinned_file_stops_the_ledger(world, monkeypatch, tmp_path):
    w = world
    other = tmp_path / "rules_copy.py"
    other.write_bytes(led_mod.PINNED["rules"].read_bytes() + b"\n")
    monkeypatch.setitem(led_mod.PINNED, "rules", other)
    assert w.ledger.cycle(FakeMarket())["outcome"] == "drift_stopped"


def test_registry_summary_before_activation(tmp_path, monkeypatch):
    monkeypatch.setattr(led_mod, "OUTPUT", tmp_path / "none")
    monkeypatch.setattr(led_mod, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(led_mod, "summary", lambda output=None: {"status": "not_activated", "review_n": 60})
    row = led_mod.registry_summary()
    assert row["tracker_status_label"] == "未激活" and row["execution_authorized"] is False and row["paper_only"] is True


def test_the_module_never_names_an_order_or_account_endpoint():
    source = led_mod.Path(led_mod.__file__).read_text(encoding="utf-8")
    for forbidden in ("/fapi/v1/order", "/fapi/v2/account", "/fapi/v1/leverage", "create_order", "api_key", "apiKey"):
        assert forbidden not in source
