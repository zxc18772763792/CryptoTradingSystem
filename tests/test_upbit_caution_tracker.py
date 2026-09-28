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
        ]
        self.path = lambda k: (1.0, 1.0)  # day k -> (high, close)

    async def get(self, url, params=None, headers=None):
        params = params or {}
        if url.endswith("/exchangeInfo"):
            listed = int(datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
            return _Resp({"symbols": [
                {"symbol": "AAAUSDT", "contractType": "PERPETUAL", "quoteAsset": "USDT", "onboardDate": listed},
                {"symbol": "1000CCCUSDT", "contractType": "PERPETUAL", "quoteAsset": "USDT", "onboardDate": listed},
                {"symbol": "DDDUSDT", "contractType": "PERPETUAL", "quoteAsset": "USDT", "onboardDate": DAY0 + DAY},  # listed after
            ]})
        if url == uc.UPBIT:
            return _Resp({"data": {"notices": self.notices if params.get("page") == 1 else []}})
        if url.startswith(uc.UPBIT + "/"):
            return _Resp({"data": {"body": "<p>유통량 계획 변경 공시 미흡</p>"}})
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
    assert set(trades) == {"AAA|7001", "CCC|6990", "DDD|6980"}  # the release notice is ignored
    assert trades["AAA|7001"]["status"] == "waiting_entry"  # day-0 close not final yet
    assert trades["CCC|6990"]["backfilled"] is True and trades["CCC|6990"]["symbol"] == "1000CCCUSDT"
    assert trades["DDD|6980"]["status"] == "no_perp"  # perp listed after the notice
    assert trades["AAA|7001"]["reason"]["reason"] == "disclosure_or_supply"
    assert summary["forward_waiting"] == 1 and summary["no_perp"] == 1

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
