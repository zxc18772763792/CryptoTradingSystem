from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from core.research import exchange_notices as en
from core.research import listing_short_tracker as lt

DAY = 86_400_000


# ── notices ──

@pytest.mark.parametrize("title, kind, tokens, date", [
    ("Binance Will Delist ICX, SCRT, STORJ on 2026-09-03", "delist", ["ICX", "SCRT", "STORJ"], "2026-09-03"),
    ("Binance Will Delist GFT, IRIS & KEY on 2024-02-20", "delist", ["GFT", "IRIS", "KEY"], "2024-02-20"),
    ("Binance Futures Will Delist USDⓈ-Margined IPUSDT and IPUSDC Perpetual Contracts (2026-06-28)", "futures_delist", ["IP"], "2026-06-28"),
    ("Binance Futures Will Delist USDⓈ-M 1000XUSDT Perpetual Contract (2026-01-02)", "futures_delist", ["X"], "2026-01-02"),
    ("Binance Will Extend the Monitoring Tag to Include ACT, BLUR, PIVX & QKC on 2026-06-18", "monitoring_tag", ["ACT", "BLUR", "PIVX", "QKC"], None),
])
def test_parse_notice(title, kind, tokens, date):
    notice = en.parse_notice(title)
    assert notice == {"kind": kind, "tokens": tokens, "effective_date": date}


@pytest.mark.parametrize("title", [
    "Binance Will List Hyperliquid (HYPE) with Seed Tag Applied",
    "Binance Will Remove the Monitoring Tag for ABC",
    "Notice of Removal of Spot Trading Pairs - 2026-09-25",
])
def test_parse_notice_ignores_other_titles(title):
    assert en.parse_notice(title) is None


def test_active_flags_window_and_severity():
    ms = lambda s: int(datetime.fromisoformat(s).replace(tzinfo=timezone.utc).timestamp() * 1000)  # noqa: E731
    history = {"161": [
        {"code": "a", "title": "Binance Will Delist AAA, BBB on 2026-09-30", "release_ms": ms("2026-09-20")},
        {"code": "b", "title": "Binance Will Delist OLD on 2026-01-10", "release_ms": ms("2026-01-01")},
        {"code": "c", "title": "Binance Will Extend the Monitoring Tag to Include BBB, CCC on 2026-09-01", "release_ms": ms("2026-09-01")},
    ], "48": []}
    flags = en.active_flags(history, now=datetime(2026, 9, 27, tzinfo=timezone.utc))
    assert set(flags) == {"AAA", "BBB", "CCC"}  # OLD expired a week after its delist date
    assert flags["BBB"]["kind"] == "delist"     # more severe notice wins
    assert flags["CCC"]["kind"] == "monitoring_tag"
    assert en.flagged(["AAA/USDT", "CCCUSDT", "ZZZ/USDT:USDT"], flags).keys() == {"AAA/USDT", "CCCUSDT"}


def test_load_flags_is_stale_safe(tmp_path):
    path = tmp_path / "flags.json"
    assert en.load_flags(path) == {}
    path.write_text(json.dumps({"generated_at": "2020-01-01T00:00:00+00:00", "flags": {"A": {}}}), encoding="utf-8")
    assert en.load_flags(path) == {}
    en.write_flags({"A": {"kind": "delist"}}, path)
    assert en.load_flags(path) == {"A": {"kind": "delist"}}


# ── listing tracker rules (must match scripts/exchange_event_studies.py) ──

def bars(closes, highs=None, start=0):
    highs = highs or closes
    return [[start + i * DAY, c, h, c, c] for i, (c, h) in enumerate(zip(closes, highs))]


def test_evaluate_trade_waits_for_d2_close():
    assert lt.evaluate_trade(bars([1.0, 1.0, 1.0]), [], now_ms=2 * DAY + 1)["status"] == "waiting_d2"


def test_evaluate_trade_closes_at_d14_with_funding():
    closes = [1.0, 1.0, 1.0] + [0.5] * 12
    funding = [{"fundingTime": 5 * DAY, "fundingRate": "0.001"}, {"fundingTime": DAY, "fundingRate": "0.5"}]  # 2nd is pre-entry
    result = lt.evaluate_trade(bars(closes), funding, now_ms=20 * DAY)
    entry = 1.0 * (1 - lt.ENTRY_SLIPPAGE)
    assert result["status"] == "closed"
    # linear short: a fall from entry to 0.5 earns (entry - 0.5) / entry, not entry / 0.5 - 1
    assert result["return_pct"] == pytest.approx(((entry - 0.5) / entry + 0.001 - lt.ROUND_TRIP_COST) * 100, abs=1e-3)


def test_evaluate_trade_stops_on_a_daily_high_and_marks_open_trades():
    closes = [1.0, 1.0, 1.0, 1.0, 1.0]
    highs = [1.0, 1.0, 1.0, 1.1, 1.5]  # 1.5 >= entry * 1.40
    stopped = lt.evaluate_trade(bars(closes, highs), [], now_ms=10 * DAY)
    entry = 1.0 * (1 - lt.ENTRY_SLIPPAGE)
    assert stopped["status"] == "stopped"
    assert stopped["exit_price"] == pytest.approx(entry * 1.40 * 1.02)
    open_trade = lt.evaluate_trade(bars([1.0, 1.0, 1.0, 0.8]), [], now_ms=4 * DAY + 1)
    assert open_trade["status"] == "open" and open_trade["return_pct"] > 0


def test_tokenomics_regex_and_llm_validation():
    text = ("Circulating Supply upon Listing on Binance: 190,000,000 OPG (19% of Total Token Supply) "
            "HODLer Airdrops Token Rewards: 50,000,000 OPG (5% of total token supply)")
    assert lt.regex_tokenomics(text) == {"circulating_supply_pct_at_listing": 19.0, "airdrop_pct_of_total_supply": 5.0}
    cleaned = lt.validate_llm_tokenomics({"circulating_supply_pct_at_listing": "19", "airdrop_pct_of_total_supply": 250,
                                          "program": "rm -rf", "seed_tag": "yes", "total_supply": float("nan")})
    assert cleaned["circulating_supply_pct_at_listing"] == 19.0
    assert cleaned["airdrop_pct_of_total_supply"] is None and cleaned["total_supply"] is None
    assert cleaned["program"] == "other" and cleaned["seed_tag"] is False
    assert lt.validate_llm_tokenomics("not a dict")["program"] == "other"


class _Resp:
    def __init__(self, payload, status=200):
        self._payload, self.status_code = payload, status

    def json(self):
        return self._payload

    def raise_for_status(self):
        pass


class _FakeClient:
    def __init__(self, now_ms):
        self.now_ms = now_ms

    async def get(self, url, params=None):
        if url.endswith("/exchangeInfo"):
            return _Resp({"symbols": [
                {"symbol": "NEWUSDT", "baseAsset": "NEW", "contractType": "PERPETUAL", "quoteAsset": "USDT", "underlyingType": "COIN", "onboardDate": self.now_ms - 20 * DAY},
                {"symbol": "GOLDUSDT", "baseAsset": "GOLD", "contractType": "PERPETUAL", "quoteAsset": "USDT", "underlyingType": "INDEX", "onboardDate": self.now_ms - 5 * DAY},
            ]})
        if url.endswith("/klines"):
            return _Resp([[params["startTime"] + (i + 1) * DAY, "1", "1", "1", "0.7" if i > 2 else "1"] for i in range(18)])
        if url.endswith("/fundingRate"):
            return _Resp([])
        if "article/detail" in url:
            return _Resp({"data": {"body": json.dumps({"node": "text", "text": "Circulating Supply upon Listing on Binance: 1 NEW (12% of Total Token Supply)"})}})
        raise AssertionError(url)


def test_tracker_tick_records_paper_trade_with_tokenomics(tmp_path):
    now_ms = datetime.now(timezone.utc).timestamp() * 1000
    history = {"161": [], "48": [{"code": "x", "title": "Introducing New (NEW) on Binance HODLer Airdrops!", "release_ms": now_ms - 22 * DAY}]}

    async def llm(text):
        return {"circulating_supply_pct_at_listing": 12, "program": "hodler_airdrop"}

    state_path = tmp_path / "tracker.json"
    summary = asyncio.run(lt.tick(_FakeClient(now_ms), llm_extract=llm, history=history, state_path=state_path))
    trade = json.loads(state_path.read_text(encoding="utf-8"))["trades"]["NEWUSDT"]
    assert "GOLDUSDT" not in json.loads(state_path.read_text(encoding="utf-8"))["trades"]  # index perps excluded
    assert trade["backfilled"] is True and trade["status"] == "closed" and trade["return_pct"] > 0
    assert trade["tokenomics"]["regex"]["circulating_supply_pct_at_listing"] == 12.0
    assert trade["tokenomics"]["llm"]["program"] == "hodler_airdrop"
    assert summary["forward_trades"] == 0  # backfilled trades never count as forward evidence


# ── agent guard ──

def test_agent_blocks_fresh_entries_on_delisting_coins(monkeypatch, tmp_path):
    import core.ai.autonomous_agent as module

    monkeypatch.setattr(en, "load_flags", lambda *a, **k: {"AAA": {"kind": "delist", "title": "Binance Will Delist AAA on 2026-10-01"},
                                                            "MMM": {"kind": "monitoring_tag", "title": "tag"}})
    agent = module.AutonomousTradingAgent(cache_root=tmp_path)
    cfg = {"min_confidence": 0.58, "learning_memory": {}}
    blocked = agent._apply_learning_score_adjustments(row={"symbol": "AAA/USDT", "direction": "LONG", "score": 1.0, "tradable_now": True}, cfg=cfg)
    assert blocked["tradable_now"] is False and "delist" in blocked["summary"]
    tagged = agent._apply_learning_score_adjustments(row={"symbol": "MMM/USDT", "direction": "LONG", "score": 1.0, "tradable_now": True}, cfg=cfg)
    assert tagged["tradable_now"] is True and "monitoring_tag" in tagged["summary"]
    held = agent._apply_learning_score_adjustments(row={"symbol": "AAA/USDT", "direction": "LONG", "score": 1.0, "tradable_now": True, "has_position": True}, cfg=cfg)
    assert held["tradable_now"] is True  # never blocks managing an existing position


def test_tokenomics_published_after_entry_is_marked_unusable(tmp_path):
    now_ms = datetime.now(timezone.utc).timestamp() * 1000
    # Perp launched 20 days ago; the spot-listing text only appeared 10 days after launch (after D2 entry).
    history = {"161": [], "48": [{"code": "x", "title": "Binance Will List New (NEW) with Seed Tag Applied", "release_ms": now_ms - 10 * DAY}]}
    state_path = tmp_path / "tracker.json"
    asyncio.run(lt.tick(_FakeClient(now_ms), llm_extract=None, history=history, state_path=state_path))
    tok = json.loads(state_path.read_text(encoding="utf-8"))["trades"]["NEWUSDT"]["tokenomics"]
    assert tok["regex"]["circulating_supply_pct_at_listing"] == 12.0
    assert tok["available_before_entry"] is False


def test_young_trade_keeps_waiting_for_its_announcement(tmp_path):
    now_ms = datetime.now(timezone.utc).timestamp() * 1000

    class Young(_FakeClient):
        async def get(self, url, params=None):
            resp = await super().get(url, params)
            if url.endswith("/exchangeInfo"):
                resp._payload["symbols"][0]["onboardDate"] = now_ms - 3 * DAY
            return resp

    state_path = tmp_path / "tracker.json"
    asyncio.run(lt.tick(Young(now_ms), llm_extract=None, history={"161": [], "48": []}, state_path=state_path))
    assert "tokenomics" not in json.loads(state_path.read_text(encoding="utf-8"))["trades"]["NEWUSDT"]


def test_prelaunch_and_listed_perps_are_never_marked_delisted(tmp_path):
    # 2026-10-01: CTUSDT was in exchangeInfo one minute before it opened; klines answered 400
    # and the trade was frozen as "delisted" although the contract then traded normally.
    now_ms = datetime.now(timezone.utc).timestamp() * 1000
    seen = []

    class Exchange(_FakeClient):
        def __init__(self, now_ms, onboard, status, gone=False):
            super().__init__(now_ms)
            self.onboard, self.status, self.gone = onboard, status, gone

        async def get(self, url, params=None):
            if url.endswith("/exchangeInfo"):
                rows = [] if self.gone else [{"symbol": "NEWUSDT", "baseAsset": "NEW", "contractType": "PERPETUAL",
                                              "quoteAsset": "USDT", "underlyingType": "COIN",
                                              "onboardDate": self.onboard, "status": self.status}]
                return _Resp({"symbols": rows})
            if url.endswith("/klines"):
                seen.append(params["symbol"])
                return _Resp({"code": -1121, "msg": "Invalid symbol."}, 400)
            return await super().get(url, params)

    started = datetime.fromtimestamp((now_ms - 10 * DAY) / 1000, timezone.utc).isoformat()

    def run(ex, name):
        path = tmp_path / f"{name}.json"
        if not path.exists():
            lt.save_state({"started_at": started, "trades": {}}, path)
        asyncio.run(lt.tick(ex, llm_extract=None, history=None, state_path=path))
        return json.loads(path.read_text(encoding="utf-8"))["trades"]["NEWUSDT"]["status"]

    assert run(Exchange(now_ms, onboard=now_ms + 60_000, status="PENDING_TRADING"), "prelaunch") == "waiting_d2"
    assert seen == []  # not polled before it opens
    assert run(Exchange(now_ms, onboard=now_ms - DAY, status="TRADING"), "live") == "waiting_d2"  # a 400 while listed is transient
    assert run(Exchange(now_ms, onboard=now_ms - DAY, status="SETTLING", gone=True), "live") == "delisted"  # really gone
