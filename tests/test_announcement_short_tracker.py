from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest

from core.research import announcement_short_tracker as ast

MIN = 60_000
START = datetime(2026, 10, 7, 0, 0, tzinfo=timezone.utc)
LISTED = int(datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)


class _Resp:
    def __init__(self, payload, status=200):
        self._payload, self.status_code = payload, status

    def json(self):
        return self._payload

    def raise_for_status(self):
        pass


class Binance:
    def __init__(self):
        self.now = START
        self.articles = {"49": [], "161": []}
        self.price = lambda symbol, t_ms: 1.0  # 1m close at bar open t_ms
        self.high = lambda symbol, t_ms: self.price(symbol, t_ms)
        self.funding = 0.0002

    async def get(self, url, params=None, headers=None):
        params = params or {}
        if "cms/article/list" in url:
            arts = self.articles[str(params["catalogId"])]
            return _Resp({"data": {"catalogs": [{"articles": arts}]}})
        if url.endswith("/exchangeInfo"):
            return _Resp({"symbols": [
                {"symbol": s, "contractType": "PERPETUAL", "quoteAsset": "USDT", "status": "TRADING", "onboardDate": LISTED}
                for s in ("AAAUSDT", "BTCUSDT", "ETHUSDT")]})
        if url.endswith("/ticker/price"):
            return _Resp({"symbol": params["symbol"], "price": "1.0"})
        if url.endswith("/ticker/24hr"):
            return _Resp([{"symbol": "BTCUSDT", "quoteVolume": "9e9"}, {"symbol": "ETHUSDT", "quoteVolume": "5e9"},
                          {"symbol": "AAAUSDT", "quoteVolume": "1e9"}])
        if url.endswith("/depth"):
            return _Resp({"T": 1, "bids": [["0.999", "5000"], ["0.99", "10000"]], "asks": [["1.001", "5000"]]})
        if url.endswith("/klines"):
            start = int(params["startTime"])
            end = int(params.get("endTime", start + params["limit"] * MIN - 1))
            now_ms = self.now.timestamp() * 1000
            bars = []
            for t in range(start, min(end + 1, start + params["limit"] * MIN), MIN):
                if t + MIN > now_ms:
                    break
                bars.append([t, "0", str(self.high(params["symbol"], t)), "0", str(self.price(params["symbol"], t))])
            return _Resp(bars)
        if url.endswith("/fundingRate"):
            lo, hi = int(params["startTime"]), int(params["endTime"])
            step = 8 * 60 * MIN  # Binance settles at 00:00 / 08:00 / 16:00 UTC
            first = -(-lo // step) * step
            return _Resp([{"fundingTime": t, "fundingRate": str(self.funding)} for t in range(first, hi + 1, step)])
        raise AssertionError(url)


def _article(code, title, published):
    return {"code": code, "title": title, "releaseDate": int(published.timestamp() * 1000)}


@pytest.fixture
def bn(monkeypatch, tmp_path):
    b = Binance()
    monkeypatch.setattr(ast, "_now", lambda: b.now)
    b.dir = tmp_path
    b.run = lambda: asyncio.run(ast.tick(b, state_dir=tmp_path))
    b.trades = lambda kind: json.loads((tmp_path / ast.KINDS[kind]["state_path"].name).read_text(encoding="utf-8"))["trades"]
    return b


def test_sell_impact_walks_the_bid_book():
    depth = {"bids": [["1.0", "4000"], ["0.98", "10000"]]}
    # 4,000 USDT at 1.00 and 6,000 at 0.98 -> average 0.98793...: 1.2% below the best bid
    assert ast.sell_impact_pct(depth, 10_000) == pytest.approx((1 - 10_000 / (4_000 + 6_000 / 0.98)) * 100, abs=1e-3)
    assert ast.sell_impact_pct({"bids": [["1.0", "100"]]}, 10_000) is None  # too thin to fill


def test_notices_before_start_are_only_marked_seen(bn):
    bn.articles["49"] = [_article("old", "Binance Will Extend the Monitoring Tag to Include AAA on 2026-10-01", START - timedelta(days=6))]
    bn.run()
    assert bn.trades("binance_monitor") == {}
    assert "old" in json.loads((bn.dir / "poll.json").read_text(encoding="utf-8"))["seen"]


def test_monitoring_notice_opens_at_detection_price_and_settles_after_24h(bn):
    bn.run()  # tracker starts
    published = START + timedelta(minutes=10)
    bn.articles["49"] = [_article("m1", "Binance Will Extend the Monitoring Tag to Include AAA & BBB on 2026-10-07", published)]
    bn.now = published + timedelta(seconds=75)
    bn.run()
    trades = bn.trades("binance_monitor")
    aaa, bbb = trades["AAA|m1"], trades["BBB|m1"]
    assert bbb["status"] == "no_perp"
    assert aaa["status"] == "open" and aaa["entry_price"] == 1.0 and aaa["latency_sec"] == 75.0 and not aaa["late"]
    assert aaa["evidence"]["book_at_entry"]["sell_10k_impact_pct"] is not None
    assert aaa["evidence"]["market_basket"]["symbols"] == ["BTCUSDT", "ETHUSDT"]

    entry_ms = aaa["entry_ms"]
    bn.price = lambda symbol, t: (0.9 if symbol == "AAAUSDT" else 1.05) if t >= entry_ms else 1.0  # coin -10%, market +5%
    bn.now = published + timedelta(hours=24, minutes=5)
    bn.run()
    aaa = bn.trades("binance_monitor")["AAA|m1"]
    funding = 3 * bn.funding
    assert aaa["status"] == "closed" and aaa["exit_price"] == 0.9
    assert aaa["return_pct"] == pytest.approx((0.10 - ast.COST + funding) * 100, abs=1e-3)
    assert aaa["evidence"]["market_control"]["hedged_return_pct"] == pytest.approx(aaa["return_pct"] + 5.0, abs=1e-3)
    summary = ast.load_summaries(bn.dir)["binance_monitor"]["summary"]
    assert summary["forward_completed"] == 1 and summary["median_latency_sec"] == 75.0
    assert summary["retirement"]["min_n"] == 30


def test_delisting_short_stops_and_late_detection_is_not_counted(bn):
    bn.run()
    published = START + timedelta(minutes=5)
    bn.articles["161"] = [_article("d1", "Binance Will Delist AAA on 2026-10-14", published)]
    bn.now = published + timedelta(minutes=1)
    bn.run()
    entry_ms = bn.trades("binance_delist")["AAA|d1"]["entry_ms"]
    bn.high = lambda symbol, t: 1.35 if t >= entry_ms + 30 * MIN else 1.0  # +35% within the 4h hold
    bn.now = published + timedelta(hours=4, minutes=10)
    bn.run()
    trade = bn.trades("binance_delist")["AAA|d1"]
    assert trade["status"] == "stopped" and trade["exit_price"] == pytest.approx(1.3 * 1.02)

    published2 = bn.now - timedelta(minutes=20)  # first seen 20 minutes after publication
    bn.articles["161"] = [_article("d2", "Binance Will Delist AAA on 2026-10-20", published2)]
    bn.run()
    late = bn.trades("binance_delist")["AAA|d2"]
    assert late["late"] is True
    summary = ast.load_summaries(bn.dir)["binance_delist"]["summary"]
    assert summary["late"] == 1 and summary["forward_completed"] == 1  # only the on-time trade counts
