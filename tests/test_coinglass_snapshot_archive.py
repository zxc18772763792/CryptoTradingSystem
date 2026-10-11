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
