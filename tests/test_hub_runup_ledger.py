from datetime import datetime, timedelta, timezone
import json

import pytest

from core.research import hub_runup_ledger as hub

SIGNAL = "XRP/USDT\n\nLONG LONG LONG\n\nOPEN MARKET PRICE\n\nTP 1.3450\nTP 1.3900\n\nSL 1.2100\n\nLEV 50X"
T0 = datetime(2026, 11, 2, 12, 0, tzinfo=timezone.utc)


class FakeMarket:
    """Flat 1.30 market, or a 1.5% climb into publication when ``runup`` is set."""

    def __init__(self, runup=False):
        self.runup, self.calls = runup, 0

    def klines(self, symbol, interval, start, end):
        step = hub.STEP[interval]
        rows, t = [], hub.ms(start)
        while t < hub.ms(end):
            price = 1.30
            if self.runup and interval == "1m":
                price = 1.30 * (1 + 0.015 * min(1.0, max(0.0, (t - hub.ms(start)) / (30 * 60_000))))
            rows.append([t, price, price * 1.001, price * 0.999, price, 0, t + step - 1])
            t += step
        return rows

    def funding(self, symbol, since):
        return {"rows": [], "complete": True}


def ledger_with(tmp_path, monkeypatch, now=T0 + timedelta(seconds=70)):
    monkeypatch.setattr(hub, "PRIVATE", tmp_path / "private")
    monkeypatch.setattr(hub, "now_utc", lambda: now)
    led = hub.Ledger(tmp_path)
    led.journal.append("activation", {"baseline_message_id": 1})
    rt = {"last_seen_id": 1, "clock": {"ok": True, "checked_at": T0.isoformat()},
          "universe": {"symbols": ["XRPUSDT", "BTCUSDT"]}}
    return led, rt


def receive(led, rt, mid, text, published, received):
    led.receive({"received_at": received, "truncated": False,
                 "messages": [{"id": mid, "date": published, "edit_date": None, "text": text}]}, rt)


def decisions(led):
    return {int(k): v["decision"] for k, v in hub.state(led.journal)["events"].items() if "decision" in v}


def test_fresh_signal_enters_both_books(tmp_path, monkeypatch):
    led, rt = ledger_with(tmp_path, monkeypatch)
    receive(led, rt, 2, SIGNAL, T0, T0 + timedelta(seconds=40))
    led.decide(rt, FakeMarket())
    d = decisions(led)[2]
    assert d["eligible"] and d["books"] == {"filtered": "entered", "all": "entered"}
    assert d["native"] == "XRPUSDT" and abs(d["pre_move_30m"]) < 1e-12
    assert d["entry_at"] == (T0 + timedelta(seconds=120)).isoformat()  # first full minute after the freeze
    assert rt["last_seen_id"] == 2


def test_runup_signal_only_in_the_unfiltered_book(tmp_path, monkeypatch):
    led, rt = ledger_with(tmp_path, monkeypatch)
    receive(led, rt, 2, SIGNAL, T0, T0 + timedelta(seconds=40))
    led.decide(rt, FakeMarket(runup=True))
    d = decisions(led)[2]
    assert d["pre_move_30m"] >= 0.01 and not d["filter_pass"]
    assert d["books"] == {"filtered": "filtered_out", "all": "entered"}


@pytest.mark.parametrize("text,published,reason", [
    ("Join VIP now", T0, "not_a_signal"),
    (SIGNAL, T0 - timedelta(seconds=200), "late_receipt"),
])
def test_ineligible_receipts_are_recorded_not_traded(tmp_path, monkeypatch, text, published, reason):
    led, rt = ledger_with(tmp_path, monkeypatch)
    receive(led, rt, 2, text, published, T0 + timedelta(seconds=40))
    led.decide(rt, FakeMarket())
    d = decisions(led)[2]
    assert not d["eligible"] and d["reason"] == reason
    assert d["books"] == {"filtered": "not_applicable", "all": "not_applicable"}


def test_unverified_clock_keeps_the_signal_out(tmp_path, monkeypatch):
    led, rt = ledger_with(tmp_path, monkeypatch)
    rt["clock"] = {"ok": False, "checked_at": T0.isoformat()}
    receive(led, rt, 2, SIGNAL, T0, T0 + timedelta(seconds=40))
    led.decide(rt, FakeMarket())
    assert decisions(led)[2]["reason"] == "clock_unverified"


def test_second_signal_on_an_open_coin_is_skipped(tmp_path, monkeypatch):
    led, rt = ledger_with(tmp_path, monkeypatch)
    receive(led, rt, 2, SIGNAL, T0, T0 + timedelta(seconds=40))
    led.decide(rt, FakeMarket())
    monkeypatch.setattr(hub.Ledger, "open_in_book", lambda self, st, book, native, entry, cache: True)
    receive(led, rt, 3, SIGNAL, T0 + timedelta(seconds=30), T0 + timedelta(seconds=60))
    led.decide(rt, FakeMarket())
    assert decisions(led)[3]["books"] == {"filtered": "skipped_overlap", "all": "skipped_overlap"}


def test_journal_tampering_is_detected(tmp_path, monkeypatch):
    led, rt = ledger_with(tmp_path, monkeypatch)
    receive(led, rt, 2, SIGNAL, T0, T0 + timedelta(seconds=40))
    lines = led.journal.path.read_text(encoding="utf-8").splitlines()
    rec = json.loads(lines[1])
    rec["data"]["delay_s"] = 1.0
    lines[1] = json.dumps(rec)
    led.journal.path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="chain broken"):
        led.journal.records()


def test_changed_pinned_file_stops_the_ledger(tmp_path, monkeypatch):
    led, rt = ledger_with(tmp_path, monkeypatch)
    monkeypatch.setattr(hub, "pins", lambda: {"protocol": "x", "ledger": "y", "rules": "z"})
    st = hub.state(led.journal)
    assert st["drift"] is not None
    import asyncio
    assert asyncio.run(led.cycle())["outcome"] == "drift_stopped"


def test_not_activated_cycle_is_idle(tmp_path):
    import asyncio
    assert asyncio.run(hub.Ledger(tmp_path).cycle()) == {"outcome": "not_activated"}


def test_flat_market_settles_at_expiry_and_gets_a_flat_control(tmp_path, monkeypatch):
    led, rt = ledger_with(tmp_path, monkeypatch)
    receive(led, rt, 2, SIGNAL, T0, T0 + timedelta(seconds=40))
    market = FakeMarket()
    led.decide(rt, market)
    monkeypatch.setattr(hub, "now_utc", lambda: T0 + timedelta(days=15))
    assert led.settle(market) == {"outcomes": 1, "controls": 0, "errors": 0}
    out = hub.state(led.journal)["events"]["2"]["outcome"]
    assert out["exit_reason"] == "expiry" and out["ret_on_filled"] == pytest.approx(-2 * hub.rules.COST)
    assert out["ret_with_funding"] == pytest.approx(out["ret_on_filled"])
    assert out["lev"]["exit_reason"] == "expiry"
    monkeypatch.setattr(hub, "now_utc", lambda: T0 + timedelta(days=46))
    assert led.settle(market)["controls"] == 1
    ctrl = hub.state(led.journal)["events"]["2"]["control"]
    assert ctrl["n_post"] == 58 and ctrl["excess_post"] == pytest.approx(0.0, abs=1e-12)
    s = hub.summary(tmp_path)
    assert s["books"]["filtered"]["controlled"] == 1 and s["books"]["all"]["traded"] == 1
