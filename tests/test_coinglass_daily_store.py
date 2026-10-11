from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest

from core.research import coinglass_daily_store as store

DAY_MS = 86_400_000
D = datetime(2026, 10, 10, tzinfo=timezone.utc)  # the day whose bars closed at 10-11 00:00
D_MS = int(D.timestamp() * 1000)


def _row(dataset: str, t: int) -> dict:
    values = {"oi": {"close": "100"}, "funding": {"close": "0.01"},
              "liquidation": {"aggregated_long_liquidation_usd": 5.0, "aggregated_short_liquidation_usd": 3.0},
              "taker": {"aggregated_buy_volume_usd": 10.0, "aggregated_sell_volume_usd": 9.0},
              "global_ratio": {"global_account_long_percent": 60.0, "global_account_short_percent": 40.0,
                               "global_account_long_short_ratio": 1.5},
              "top_ratio": {"top_position_long_percent": 55.0, "top_position_short_percent": 45.0,
                            "top_position_long_short_ratio": 1.22}}[dataset]
    return {"time": t, **values}


@pytest.fixture
def env(monkeypatch, tmp_path):
    (tmp_path / "universe.json").write_text(json.dumps({"bases": ["AAA", "BBB", "NOPERP"]}), encoding="utf-8")
    (tmp_path / "perps.json").write_text(json.dumps({"AAA": "AAAUSDT", "BBB": "1000BBBUSDT"}), encoding="utf-8")
    monkeypatch.setattr(store, "UNIVERSE_PATH", tmp_path / "universe.json")
    monkeypatch.setattr(store, "PERP_SYMBOLS_PATH", tmp_path / "perps.json")
    monkeypatch.setattr(store, "STORE_DIR", tmp_path / "store")
    monkeypatch.setattr(store, "STATUS_PATH", tmp_path / "store" / "_status.json")
    monkeypatch.setattr(store, "SEED_DIR", tmp_path / "seed")
    monkeypatch.setattr(store, "REQUEST_SPACING_SEC", 0.0)
    (tmp_path / "seed").mkdir()
    return tmp_path


class Relay:
    """Serves daily bars for any stamp <= the asked end, and hourly snapshots; logs every request."""

    def __init__(self, refuse=()):
        self.calls, self.refuse = [], set(refuse)

    async def request_json(self, path, params=None, manual=False):
        self.calls.append((path.rsplit("/", 2)[-2], params["symbol"], params["interval"]))
        if params["symbol"] in self.refuse:
            return {"payload": {"code": "400", "msg": "unsupported"}}
        dataset = next(k for k, v in store.DATASETS.items() if v[0] == path)
        step = DAY_MS if params["interval"] == "1d" else 3_600_000
        first = (int(params["start_time"]) + step - 1) // step * step
        stamps = range(first, int(params["end_time"]) + 1, step)
        return {"payload": {"code": "0", "data": [_row(dataset, t) for t in stamps][: params["limit"]]}}


def test_a_normal_day_asks_one_daily_bar_and_one_snapshot_per_coin():
    coin = {dataset: [_row(dataset, D_MS - DAY_MS)] for dataset in ("oi", "funding", "liquidation", "taker")}
    coin.update({dataset: [_row(dataset, D_MS)] for dataset in store.SNAPSHOT_AT_STAMP})  # yesterday's 00:00 snapshot
    asks = {d: store.requests_for(coin, "AAA", "AAAUSDT", d, D) for d in store.DATASETS}
    assert [p["interval"] for p in (a[1] for a in asks["oi"])] == ["1d"]
    assert asks["oi"][0][1]["start_time"] == D_MS - DAY_MS + 1 and asks["oi"][0][1]["end_time"] == D_MS + DAY_MS - 1
    assert asks["liquidation"][0][1]["exchange_list"] == "Binance,OKX,Bybit"
    snapshot = asks["global_ratio"]
    assert len(snapshot) == 1 and snapshot[0][1]["interval"] == "1h"
    assert snapshot[0][1]["start_time"] == D_MS + DAY_MS  # the 10-11 00:00 snapshot closes day 10-10
    assert snapshot[0][1]["symbol"] == "AAAUSDT" and snapshot[0][1]["exchange"] == "Binance"


def test_seed_then_update_brings_every_coin_to_the_day(env):
    seed_rows = {d: [_row(d, D_MS - k * DAY_MS) for k in range(1, 80)] for d in store.DATASETS}
    (env / "seed" / "AAAUSDT.json").write_text(json.dumps({"base": "AAA", **seed_rows}), encoding="utf-8")
    assert store.seed() == 1
    seeded = store.load_coin("AAAUSDT")
    assert len(seeded["oi"]) == store.KEEP_DAYS + 1 and seeded["oi"][-1]["time"] == D_MS - DAY_MS
    assert set(seeded["oi"][0]) == {"time", "close"}  # only the kept fields

    relay = Relay()
    status = asyncio.run(store.update(D, client=relay))
    assert status["complete_coins"] == 2 and not status["errors"]
    aaa, bbb = store.load_coin("AAAUSDT"), store.load_coin("1000BBBUSDT")
    assert store.complete(aaa, D) and store.complete(bbb, D)
    assert aaa["global_ratio"][-1]["time"] == D_MS + DAY_MS
    # AAA was seeded through D-1: per dataset one daily bar, plus the snapshot for the two ratio series;
    # BBB had nothing: its whole window
    aaa_calls = [c for c in relay.calls if c[1] in {"AAA", "AAAUSDT"}]
    assert len(aaa_calls) == 6 + 2
    assert len(bbb["oi"]) == store.KEEP_DAYS + 1
    assert store.update.__name__ and json.loads((env / "store" / "_status.json").read_text(encoding="utf-8"))["day"] == "2026-10-10"


def test_a_refused_coin_is_recorded_and_the_rest_complete(env):
    status = asyncio.run(store.update(D, client=Relay(refuse={"BBB", "1000BBBUSDT"})))
    # an empty ratio series is asked twice (the daily window and the snapshot): 4 + 2 x 2
    assert status["complete_coins"] == 1 and len(status["refused"]) == 8


def test_ensure_current_waits_starts_and_reports(env, monkeypatch):
    started = []

    async def fake_update(day, **kwargs):
        started.append(day)
        store.save_status({"day": f"{day:%Y-%m-%d}", "attempt": 1, "completed_at": "x", "complete_coins": 2, "errors": []})

    monkeypatch.setattr(store, "update", fake_update)
    monkeypatch.setattr("core.data.coinglass_client.coinglass_enabled", lambda: True)

    async def run(now):
        out = store.ensure_current(now)
        await asyncio.sleep(0)
        if store._task is not None:
            await store._task
        return out

    opens = D + timedelta(days=1)
    assert asyncio.run(run(opens + timedelta(minutes=5)))["status"] == "waiting"
    assert asyncio.run(run(opens + timedelta(minutes=20)))["status"] == "started" and started == [D]
    assert asyncio.run(run(opens + timedelta(hours=1)))["status"] == "current"
    store._task = None


def test_a_timed_out_request_is_asked_again(env, monkeypatch):
    monkeypatch.setattr(store, "TIMEOUT_WAIT_SEC", 0.0)

    class Slow(Relay):
        def __init__(self):
            super().__init__()
            self.slow_left = 2

        async def request_json(self, path, params=None, manual=False):
            if self.slow_left:
                self.slow_left -= 1
                raise asyncio.TimeoutError()
            return await super().request_json(path, params, manual)

    status = asyncio.run(store.update(D, client=Slow()))
    assert status["complete_coins"] == 2 and not status["errors"]  # two timeouts, then answers
