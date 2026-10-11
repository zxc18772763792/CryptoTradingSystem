from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from core.research import coinglass_snapshot_archive as archive


def _at(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 10, 11, hour, minute, tzinfo=timezone.utc)


@pytest.mark.parametrize("now, expected", [
    (_at(0, 5), _at(0)), (_at(2, 59), _at(0)), (_at(3, 1), None),   # past the 3h grace: skipped
    (_at(6), _at(6)), (_at(13, 30), _at(12)), (_at(21), _at(18)), (_at(21, 1), None),
])
def test_due_slot(now, expected):
    assert archive.due_slot(now) == expected


class _Relay:
    def __init__(self, sizes):
        self.sizes, self.calls = sizes, []

    async def request_json(self, path, params=None, manual=False):
        self.calls.append(params["page"])
        n = self.sizes[params["page"] - 1]
        return {"payload": {"code": "0", "data": [{"symbol": f"C{params['page']}_{i}"} for i in range(n)]}}


def test_one_snapshot_per_slot_with_all_pages(tmp_path, monkeypatch):
    monkeypatch.setattr(archive, "PAGE_SPACING_SEC", 0.0)
    relay = _Relay([200, 200, 37])
    out = asyncio.run(archive.tick(_at(6, 10), client=relay, root=tmp_path))
    assert out["status"] == "captured" and out["coins"] == 437 and relay.calls == [1, 2, 3]
    again = asyncio.run(archive.tick(_at(8), client=relay, root=tmp_path))
    assert again["status"] == "done" and relay.calls == [1, 2, 3]  # the slot is taken: no request
    assert asyncio.run(archive.tick(_at(10), client=relay, root=tmp_path))["status"] == "idle"
    snaps = list(archive.iter_snapshots(tmp_path))
    assert len(snaps) == 1 and snaps[0][0] == _at(6).isoformat() and len(snaps[0][2]) == 437
    assert archive.snapshot_path(_at(6), tmp_path).name == "06_coins_markets.json.gz"


def test_a_failed_page_leaves_no_partial_file(tmp_path, monkeypatch):
    monkeypatch.setattr(archive, "PAGE_SPACING_SEC", 0.0)

    class Broken(_Relay):
        async def request_json(self, path, params=None, manual=False):
            if params["page"] == 2:
                raise ConnectionError("relay down")
            return await super().request_json(path, params, manual)

    with pytest.raises(ConnectionError):
        asyncio.run(archive.tick(_at(12, 1), client=Broken([200, 200]), root=tmp_path))
    assert not archive.snapshot_path(_at(12), tmp_path).exists()  # retried on the next pass


def test_a_full_minute_budget_waits_instead_of_dropping_pages(tmp_path, monkeypatch):
    from core.data.coinglass_client import CoinglassBudgetExceeded

    monkeypatch.setattr(archive, "PAGE_SPACING_SEC", 0.0)
    monkeypatch.setattr(archive, "BUDGET_WAIT_SEC", 0.0)

    class Busy(_Relay):
        refused = False

        async def request_json(self, path, params=None, manual=False):
            if params["page"] == 2 and not Busy.refused:
                Busy.refused = True
                raise CoinglassBudgetExceeded("minute_budget_exhausted")
            return await super().request_json(path, params, manual)

    relay = Busy([200, 200, 5])
    out = asyncio.run(archive.tick(_at(18, 1), client=relay, root=tmp_path))
    assert out["coins"] == 405 and relay.calls == [1, 2, 3]  # page 1 kept, page 2 asked again


class _MapRelay:
    def __init__(self, refuse=(), fail_on=None):
        self.refuse, self.fail_on, self.calls = set(refuse), fail_on, []

    async def request_json(self, path, params=None, manual=False):
        base = params["symbol"]
        if base == self.fail_on:
            self.fail_on = None  # a one-off network error
            raise ConnectionError("relay down")
        self.calls.append(base)
        if base in self.refuse:
            return {"payload": {"code": "40001", "msg": "unsupported symbol"}}
        return {"payload": {"code": "0", "data": {"data": [{"liqMapV2": {"1.0": [[1.0, 5.0, None, None]]}}], "last_price": 1.0}}}


def test_liquidation_maps_are_collected_in_batches_and_written_once(tmp_path, monkeypatch):
    monkeypatch.setattr(archive, "PAGE_SPACING_SEC", 0.0)
    monkeypatch.setattr(archive, "LIQ_MAP_BATCH", 10)
    bases = [f"C{i:02d}" for i in range(25)]
    relay = _MapRelay(refuse={"C03"})
    assert asyncio.run(archive.tick_liq_maps(_at(5, 59), client=relay, root=tmp_path, bases=bases))["status"] == "idle"
    first = asyncio.run(archive.tick_liq_maps(_at(6, 1), client=relay, root=tmp_path, bases=bases))
    assert first == {"status": "collecting", "day": "2026-10-11", "done": 10, "remaining": 15}
    asyncio.run(archive.tick_liq_maps(_at(6, 5), client=relay, root=tmp_path, bases=bases))
    done = asyncio.run(archive.tick_liq_maps(_at(6, 9), client=relay, root=tmp_path, bases=bases))
    assert done["status"] == "captured" and done["coins"] == 25 and done["refused"] == 1
    assert relay.calls == bases  # every coin asked once, the refused one included and not asked again
    path = archive.liq_map_path(_at(0), tmp_path)
    assert path.exists() and not (path.parent / "liq_map_7d.partial.json").exists()
    import gzip
    import json
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        saved = json.load(fh)
    assert saved["maps"]["C00"]["data"]["last_price"] == 1.0 and "error" in saved["maps"]["C03"]
    assert asyncio.run(archive.tick_liq_maps(_at(9), client=relay, root=tmp_path, bases=bases))["status"] == "done"


def test_a_network_error_keeps_the_maps_already_fetched(tmp_path, monkeypatch):
    monkeypatch.setattr(archive, "PAGE_SPACING_SEC", 0.0)
    relay = _MapRelay(fail_on="C02")
    bases = ["C00", "C01", "C02", "C03"]
    with pytest.raises(ConnectionError):
        asyncio.run(archive.tick_liq_maps(_at(7), client=relay, root=tmp_path, bases=bases))
    out = asyncio.run(archive.tick_liq_maps(_at(7, 5), client=relay, root=tmp_path, bases=bases))
    assert out["status"] == "captured" and relay.calls == ["C00", "C01", "C02", "C03"]  # C00, C01 not refetched
