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
        self.no_spot_history = set()

    async def get(self, url, params=None, headers=None):
        params = params or {}
        if url.endswith("/exchangeInfo"):
            listed = int(datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
            return _Resp({"symbols": [
                {"symbol": "AAAUSDT", "contractType": "PERPETUAL", "quoteAsset": "USDT", "onboardDate": listed, "status": "TRADING"},
                {"symbol": "1000CCCUSDT", "contractType": "PERPETUAL", "quoteAsset": "USDT", "onboardDate": listed, "status": "TRADING"},
                {"symbol": "DDDUSDT", "contractType": "PERPETUAL", "quoteAsset": "USDT", "onboardDate": DAY0 + DAY, "status": "TRADING"},  # listed after
                {"symbol": "EEEUSDT", "contractType": "PERPETUAL", "quoteAsset": "USDT", "onboardDate": listed, "status": "SETTLING"},
            ]})
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
