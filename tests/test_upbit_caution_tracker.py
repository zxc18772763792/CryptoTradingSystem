from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest

from core.research import upbit_caution_tracker as uc

DAY = 86_400_000
NOTICE_AT = datetime(2026, 10, 5, 6, 0, tzinfo=timezone.utc)  # 15:00 KST
DAY0 = int(datetime(2026, 10, 5, tzinfo=timezone.utc).timestamp() * 1000)


class _Resp:
    def __init__(self, payload, status=200):
        self._payload, self.status_code = payload, status
        self.text = json.dumps(payload, ensure_ascii=False)

    def json(self):
        return self._payload

    def raise_for_status(self):
        pass


class World:
    def __init__(self):
        self.now = NOTICE_AT + timedelta(hours=2)
        self.notices = [
            {"id": 7001, "title": "에이에이(AAA) 거래 유의 종목 지정 안내", "first_listed_at": "2026-10-05T15:00:00+09:00"},
            {"id": 7000, "title": "비비(BBB) 거래 유의 종목 지정 해제 안내", "first_listed_at": "2026-10-05T14:00:00+09:00"},
            {"id": 6990, "title": "씨씨(CCC) 거래 유의 종목 지정 안내", "first_listed_at": "2026-09-30T10:00:00+09:00"},
            {"id": 6980, "title": "디디(DDD) 거래 유의 종목 지정 안내", "first_listed_at": "2026-10-05T12:00:00+09:00"},
            {"id": 6970, "title": "이이(EEE) 거래 유의 종목 지정 안내", "first_listed_at": "2026-10-05T11:00:00+09:00"},
        ]
        self.path = lambda k: (1.0, 1.0)  # day k -> (high, close)
        self.path5m = lambda t: (1.0, 1.0)  # 5m bar open ms -> (open, high)
        self.basket_close = lambda k: 1.0  # day k close of every basket perp
        self.no_spot_history = set()
        self.calls = []

    async def get(self, url, params=None, headers=None):
        params = params or {}
        self.calls.append((url.rsplit("/", 1)[-1], params.get("symbol"), params.get("interval")))
        if url.endswith("/exchangeInfo"):
            listed = int(datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
            return _Resp({"symbols": [
                {"symbol": "AAAUSDT", "contractType": "PERPETUAL", "quoteAsset": "USDT", "onboardDate": listed, "status": "TRADING"},
                {"symbol": "1000CCCUSDT", "contractType": "PERPETUAL", "quoteAsset": "USDT", "onboardDate": listed, "status": "TRADING"},
                {"symbol": "DDDUSDT", "contractType": "PERPETUAL", "quoteAsset": "USDT", "onboardDate": DAY0 + DAY, "status": "TRADING"},  # listed after
                {"symbol": "EEEUSDT", "contractType": "PERPETUAL", "quoteAsset": "USDT", "onboardDate": listed, "status": "SETTLING"},
                {"symbol": "BTCUSDT", "contractType": "PERPETUAL", "quoteAsset": "USDT", "onboardDate": listed, "status": "TRADING"},
                {"symbol": "ETHUSDT", "contractType": "PERPETUAL", "quoteAsset": "USDT", "onboardDate": listed, "status": "TRADING"},
            ]})
        if url.endswith("/depth"):
            return _Resp({"T": 1, "bids": [["0.999", "1000"], ["0.985", "5000"], ["0.9", "1"]],
                          "asks": [["1.001", "2000"], ["1.015", "1000"], ["1.1", "1"]]})
        if url.endswith("/ticker/24hr"):
            return _Resp([{"symbol": "BTCUSDT", "quoteVolume": "9e9"}, {"symbol": "AAAUSDT", "quoteVolume": "5e9"},
                          {"symbol": "EEEUSDT", "quoteVolume": "4e9"}, {"symbol": "ETHUSDT", "quoteVolume": "3e9"}])
        if url.endswith("/klines") and params.get("interval") == "5m":
            start, end = int(params["startTime"]), int(params["endTime"])
            bars = []
            for t in range(start, min(end + 1, start + params["limit"] * 300_000), 300_000):
                open_, high = self.path5m(t)
                bars.append([t, str(open_), str(high), "0.8", "1"])
            return _Resp(bars)
        if url.endswith("/klines") and params.get("symbol") in {"BTCUSDT", "ETHUSDT"}:
            start = int(params["startTime"])
            return _Resp([[start + k * DAY, "1", "1", "1", str(self.basket_close((start + k * DAY - DAY0) // DAY))]
                          for k in range(params["limit"])])
        if url == uc.UPBIT:
            return _Resp({"data": {"notices": self.notices if params.get("page") == 1 else []}})
        if url.startswith(uc.UPBIT + "/"):
            return _Resp({"data": {"body": "<p>유통량 계획 변경 공시 미흡</p>"}})
        if url.startswith(uc.SPOT) and params.get("symbol") in self.no_spot_history:
            return _Resp([])
        if url.endswith("/klines"):
            start, now_ms = int(params["startTime"]), self.now.timestamp() * 1000
            bars = []
            for k in range(params["limit"]):
                t = start + k * DAY
                if t > now_ms:
                    break
                high, close = self.path((t - DAY0) // DAY)
                bars.append([t, "1", str(high), "1", str(close)])
            return _Resp(bars)
        if url.endswith("/fundingRate"):
            return _Resp([{"fundingTime": DAY0 + DAY + 3_600_000, "fundingRate": "-0.002"}])
        raise AssertionError(url)


@pytest.fixture
def world(monkeypatch):
    w = World()
    monkeypatch.setattr(uc, "_now", lambda: w.now)
    return w


async def _reason(text):
    assert "유통량" in text
    return {"reason": "disclosure_or_supply", "summary_en": "supply plan changed without disclosure"}


def run(world, path, llm=_reason):
    return asyncio.run(uc.tick(world, llm_extract=llm, state_path=path))


def test_ticker_parsing():
    assert uc.caution_tickers("소폰(SOPH) 거래 유의 종목 지정 안내") == ["SOPH"]
    assert uc.caution_tickers("인젝티브(INJ) 거래 유의 종목 지정 해제 안내") == []
    assert uc.caution_tickers("에이(AAA), 비(BBB) 거래 유의 종목 지정 안내 (KRW, BTC 마켓)") == ["AAA", "BBB"]
    assert uc.caution_tickers("에이(AAA) 거래지원 종료 안내") == []


def test_lifecycle_backfill_and_no_perp(world, tmp_path):
    path = tmp_path / "state.json"
    uc.save_state({"started_at": "2026-10-01T00:00:00+00:00", "trades": {}}, path)
    world.path = lambda k: (1.0, 1.0 if k == 0 else 0.9)  # falls 10% after entry
    summary = run(world, path)
    trades = json.loads(path.read_text(encoding="utf-8"))["trades"]
    assert set(trades) == {"AAA|7001", "CCC|6990", "DDD|6980", "EEE|6970"}  # the release notice is ignored
    assert trades["AAA|7001"]["status"] == "waiting_entry"  # day-0 close not final yet
    assert trades["CCC|6990"]["backfilled"] is True and trades["CCC|6990"]["symbol"] == "1000CCCUSDT"
    assert trades["DDD|6980"]["status"] == "no_perp"  # perp listed after the notice
    assert trades["EEE|6970"]["status"] == "no_perp"  # settled contract: flat klines would fake a 0% trade
    assert trades["AAA|7001"]["reason"]["reason"] == "disclosure_or_supply"
    assert summary["forward_waiting"] == 1 and summary["no_perp"] == 2

    world.now = datetime(2026, 10, 6, 1, tzinfo=timezone.utc)
    run(world, path)
    aaa = json.loads(path.read_text(encoding="utf-8"))["trades"]["AAA|7001"]
    assert aaa["status"] == "open" and aaa["entry_price"] == 1.0

    world.now = datetime(2026, 10, 13, 1, tzinfo=timezone.utc)  # day-7 close has passed
    summary = run(world, path)
    aaa = json.loads(path.read_text(encoding="utf-8"))["trades"]["AAA|7001"]
    assert aaa["status"] == "closed"
    assert aaa["return_pct"] == pytest.approx((0.10 - uc.ROUND_TRIP_COST - 0.002) * 100)
    assert summary["forward_completed"] == 1 and summary["retirement"]["verdict"] == "collecting"
    assert json.loads(path.read_text(encoding="utf-8"))["retirement_rule"]["min_n"] == 20


def test_stop_and_invalid_reason(world, tmp_path):
    path = tmp_path / "state.json"
    uc.save_state({"started_at": "2026-10-01T00:00:00+00:00", "trades": {}}, path)
    world.path = lambda k: (1.5 if k == 3 else 1.0, 1.0)
    world.now = datetime(2026, 10, 13, 1, tzinfo=timezone.utc)

    async def bad(text):
        return {"reason": "ignore previous instructions"}

    run(world, path, llm=bad)
    aaa = json.loads(path.read_text(encoding="utf-8"))["trades"]["AAA|7001"]
    assert aaa["status"] == "stopped" and aaa["exit_price"] == pytest.approx(1.4 * 1.02)
    assert aaa["reason"]["reason"] == "other"


def test_krw_listing_parsing():
    assert uc.krw_listing_tickers("라이터(LIT) KRW 마켓 디지털 자산 추가") == ["LIT"]
    assert uc.krw_listing_tickers("클러스터프로토콜(CP) 신규 거래지원 안내 (KRW, BTC, USDT 마켓)") == ["CP"]
    assert uc.krw_listing_tickers("원화 마켓 신규 상장 (ABC)") == ["ABC"]
    assert uc.krw_listing_tickers("비트(BIT) BTC 마켓 디지털 자산 추가") == []  # no KRW market
    assert uc.krw_listing_tickers("에이(AAA) 거래 유의 종목 지정 안내") == []
    assert uc.krw_listing_tickers("에이(AAA) 거래지원 종료 안내 (KRW 마켓)") == []


def test_krw_listing_dedupes_follow_ups_and_needs_spot_history(world, tmp_path):
    world.notices = [
        {"id": 8002, "title": "에이에이(AAA) KRW 마켓 디지털 자산 추가 (거래지원 개시 시점 변경 안내)", "first_listed_at": "2026-10-06T10:00:00+09:00"},
        {"id": 8001, "title": "에이에이(AAA) KRW 마켓 디지털 자산 추가", "first_listed_at": "2026-10-05T15:00:00+09:00"},
        {"id": 8000, "title": "씨씨(CCC) 신규 거래지원 안내 (KRW, BTC 마켓)", "first_listed_at": "2026-10-05T12:00:00+09:00"},
    ]
    world.no_spot_history = {"CCCUSDT"}
    path = tmp_path / "listing.json"
    uc.save_state({"started_at": "2026-10-01T00:00:00+00:00", "trades": {}}, path)
    summary = asyncio.run(uc.tick(world, strategy="krw_listing", state_path=path))
    trades = json.loads(path.read_text(encoding="utf-8"))["trades"]
    assert set(trades) == {"AAA|8001", "CCC|8000"}  # the start-time follow-up is not a second event
    assert trades["CCC|8000"]["status"] == "no_perp"  # never traded on Binance spot before: outside the study
    assert summary["strategy"] == "krw_listing" and "70 perps" in summary["backtest_reference"]
    assert json.loads(path.read_text(encoding="utf-8"))["retirement_rule"]["min_n"] == 30


def test_caution_extensions_are_not_deduped(world, tmp_path):
    world.notices = [
        {"id": 7101, "title": "에이에이(AAA) 거래 유의 종목 지정 기간 연장 안내", "first_listed_at": "2026-10-05T15:00:00+09:00"},
        {"id": 7100, "title": "에이에이(AAA) 거래 유의 종목 지정 안내", "first_listed_at": "2026-09-20T15:00:00+09:00"},
    ]
    path = tmp_path / "caution.json"
    uc.save_state({"started_at": "2026-09-01T00:00:00+00:00", "trades": {}}, path)
    asyncio.run(uc.tick(world, state_path=path))
    assert set(json.loads(path.read_text(encoding="utf-8"))["trades"]) == {"AAA|7100", "AAA|7101"}


def test_forward_trade_records_point_in_time_evidence(world, tmp_path):
    path = tmp_path / "state.json"
    uc.save_state({"started_at": "2026-10-01T00:00:00+00:00", "trades": {}}, path)
    world.path = lambda k: (1.5 if k == 3 else 1.0, 1.0)  # stop on day 3
    stop_bar = DAY0 + 3 * DAY + 12 * 300_000
    world.path5m = lambda t: (1.5, 1.6) if t == stop_bar else (1.0, 1.0)  # opens beyond the stop: a gap
    world.basket_close = lambda k: 1.1 if k >= 1 else 1.0  # market +10% over the hold
    run(world, path)
    ev = json.loads(path.read_text(encoding="utf-8"))["trades"]["AAA|7001"]["evidence"]
    assert ev["classifier_version"] == uc.CLASSIFIER_VERSION
    assert ev["notice_first_seen"]["title"] == "에이에이(AAA) 거래 유의 종목 지정 안내"
    assert ev["discovery_lag_sec"] == 7200.0
    assert ev["contract_at_discovery"]["AAAUSDT"]["status"] == "TRADING"
    assert ev["contract_at_discovery"]["1000AAAUSDT"] == "absent"
    book = ev["book_at_discovery"]
    assert book["spread_bps"] == pytest.approx(20.0) and book["bid_usdt_2pct"] == pytest.approx(999 + 4925)
    assert ev["market_basket"]["symbols"] == ["BTCUSDT", "ETHUSDT"]  # live perps only, the coin itself excluded
    backfill_ev = json.loads(path.read_text(encoding="utf-8"))["trades"]["CCC|6990"]["evidence"]
    assert "book_at_discovery" not in backfill_ev  # no requests spent on backfills
    eee = json.loads(path.read_text(encoding="utf-8"))["trades"]["EEE|6970"]["evidence"]
    assert eee["contract_at_discovery"]["EEEUSDT"]["status"] == "SETTLING"  # why it had no tradable perp

    world.now = datetime(2026, 10, 6, 1, tzinfo=timezone.utc)  # one hour after the entry close
    run(world, path)
    ev = json.loads(path.read_text(encoding="utf-8"))["trades"]["AAA|7001"]["evidence"]
    assert ev["book_at_entry"]["lag_after_entry_close_sec"] == 3600.0 and "mid" in ev["book_at_entry"]
    assert ev["contract_trading_at_entry_check"] is True

    world.now = datetime(2026, 10, 13, 1, tzinfo=timezone.utc)
    summary = run(world, path)
    trade = json.loads(path.read_text(encoding="utf-8"))["trades"]["AAA|7001"]
    assert trade["status"] == "stopped"
    p5 = trade["evidence"]["path"]
    assert p5["stop_hit_at"] == stop_bar and p5["stop_gap_pct"] == pytest.approx((1.5 / 1.4 - 1) * 100, abs=1e-3)
    assert p5["mae_pct"] == pytest.approx(60.0) and p5["coverage"] == 1.0
    control = trade["evidence"]["market_control"]
    assert control["return_pct"] == pytest.approx(10.0) and control["legs"] == 2
    assert control["hedged_return_pct"] == pytest.approx(trade["return_pct"] + 10.0)
    assert summary["evidence_coverage"] == {"completed": 1, "book_at_entry": 1, "path": 1, "market_control": 1}
    assert summary["forward_hedged_mean_return_pct"] == pytest.approx(round(trade["return_pct"] + 10.0, 2))


def test_entry_book_marked_missed_when_the_tracker_was_down(world, tmp_path):
    path = tmp_path / "state.json"
    uc.save_state({"started_at": "2026-10-01T00:00:00+00:00", "trades": {}}, path)
    run(world, path)
    world.now = datetime(2026, 10, 6, 9, tzinfo=timezone.utc)  # 9h after the entry close
    run(world, path)
    book = json.loads(path.read_text(encoding="utf-8"))["trades"]["AAA|7001"]["evidence"]["book_at_entry"]
    assert book == {"missed": True, "lag_after_entry_close_sec": 32400.0}


def test_notice_first_seen_after_entry_close_is_late_and_excluded(world, tmp_path):
    path = tmp_path / "state.json"
    uc.save_state({"started_at": "2026-10-01T00:00:00+00:00", "trades": {}}, path)
    world.now = datetime(2026, 10, 13, 1, tzinfo=timezone.utc)  # first pass after the whole hold
    summary = run(world, path)
    aaa = json.loads(path.read_text(encoding="utf-8"))["trades"]["AAA|7001"]
    assert aaa["late"] is True and aaa["backfilled"] is False and aaa["status"] == "closed"
    assert summary["forward_late"] == 1 and summary["forward_completed"] == 0
    assert summary["retirement"]["verdict"] == "collecting"



def _hedged(world, path):
    return asyncio.run(uc.tick(world, state_path=path, strategy="caution_hedged"))


def test_hedged_variant_adds_the_frozen_basket_leg(world, tmp_path):
    path = tmp_path / "hedged.json"
    uc.save_state({"started_at": "2026-10-01T00:00:00+00:00", "trades": {}}, path)
    world.path = lambda k: (1.0, 1.0 if k == 0 else 0.9)  # coin -10% after entry
    world.basket_close = lambda k: 1.1 if k >= 1 else 1.0  # market +10% over the hold
    _hedged(world, path)
    trade = json.loads(path.read_text(encoding="utf-8"))["trades"]["AAA|7001"]
    assert trade["evidence"]["market_basket"]["symbols"] == ["BTCUSDT", "ETHUSDT"]
    assert "reason" not in trade  # no LLM labelling in the hedged copy

    world.now = datetime(2026, 10, 13, 1, tzinfo=timezone.utc)
    summary = _hedged(world, path)
    trade = json.loads(path.read_text(encoding="utf-8"))["trades"]["AAA|7001"]
    coin = (0.10 - uc.ROUND_TRIP_COST - 0.002) * 100
    basket = 10.0 + 0.2 - uc.BASKET_FEE * 100  # +10% price, longs RECEIVE 0.2% (funding -0.002), 0.1% fees
    assert trade["return_pct"] == pytest.approx(coin)
    assert trade["hedge"]["net_basket_pct"] == pytest.approx(basket)
    assert trade["hedged_return_pct"] == pytest.approx(coin + basket)
    assert summary["forward_completed"] == 1 and summary["forward_mean_return_pct"] == pytest.approx(round(coin + basket, 2))
    assert summary["forward_unhedged_mean_return_pct"] == pytest.approx(round(coin, 2))
    assert json.loads(path.read_text(encoding="utf-8"))["retirement_rule"]["min_n"] == 20


def test_hedged_basket_closes_on_the_coin_stop_day(world, tmp_path):
    path = tmp_path / "hedged.json"
    uc.save_state({"started_at": "2026-10-01T00:00:00+00:00", "trades": {}}, path)
    world.path = lambda k: (1.5 if k == 3 else 1.0, 1.0)  # coin stopped on day 3
    world.basket_close = lambda k: 1.0 + 0.01 * k        # basket +3% by the day-3 close, +7% by day 7
    _hedged(world, path)
    world.now = datetime(2026, 10, 13, 1, tzinfo=timezone.utc)
    _hedged(world, path)
    trade = json.loads(path.read_text(encoding="utf-8"))["trades"]["AAA|7001"]
    assert trade["status"] == "stopped"
    assert trade["hedge"]["return_pct"] == pytest.approx(3.0)  # closed with the coin, not held to day 7
    assert trade["hedged_return_pct"] == pytest.approx(trade["return_pct"] + 3.0 + 0.2 - 0.1)


def test_hedged_trade_waits_while_the_basket_cannot_settle(world, tmp_path):
    path = tmp_path / "hedged.json"
    uc.save_state({"started_at": "2026-10-01T00:00:00+00:00", "trades": {}}, path)
    _hedged(world, path)
    world.now = datetime(2026, 10, 13, 1, tzinfo=timezone.utc)
    real_get = world.get

    async def no_basket_klines(url, params=None, headers=None):
        if url.endswith("/klines") and (params or {}).get("symbol") in {"BTCUSDT", "ETHUSDT"}:
            return _Resp([], 500)
        return await real_get(url, params, headers)

    world.get = no_basket_klines
    summary = _hedged(world, path)
    trade = json.loads(path.read_text(encoding="utf-8"))["trades"]["AAA|7001"]
    assert trade["status"] == "closed" and "hedged_return_pct" not in trade
    assert summary["forward_completed"] == 0  # a trade without its hedge leg is not booked
