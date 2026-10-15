from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import collect_binance_history as binance  # noqa: E402
from scripts import collect_coinglass_history as cg  # noqa: E402

DAY = 86_400_000


def test_the_relay_is_left_alone_while_the_daily_store_updates():
    at = lambda h, m: datetime(2026, 10, 13, h, m, tzinfo=timezone.utc)  # noqa: E731
    assert not cg._in_quiet_window(at(0, 9)) and cg._in_quiet_window(at(0, 10))
    assert cg._in_quiet_window(at(5, 54)) and not cg._in_quiet_window(at(5, 55))
    assert [cg.coin_of(b) for b in ("1000PEPE", "1MBABYDOGE", "1INCH", "2Z")] == ["PEPE", "BABYDOGE", "1INCH", "2Z"]


class _Relay:
    """Daily rows from a first day, cut at ``cap`` rows from the newest end like the relay can do."""

    def __init__(self, first_ms: int, cap: int):
        self.first, self.cap, self.calls = first_ms, cap, []

    async def ask(self, client, path, params, last_call):
        self.calls.append((params["start_time"], params["end_time"]))
        stamps = [t for t in range(max(self.first, params["start_time"]), params["end_time"] + 1, DAY)]
        return [{"time": t, "close": "1"} for t in stamps[-self.cap:]]


def test_a_page_cut_at_the_relay_cap_is_followed_by_the_older_part(monkeypatch):
    lo = int(datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    hi = lo + 2499 * DAY + DAY - 1
    relay = _Relay(lo, cap=1000)
    monkeypatch.setattr(cg, "_ask", relay.ask)
    rows = asyncio.run(cg._window(None, "oi", {}, lo, hi, [0.0]))
    assert len(rows) == 2500 and min(rows) == lo and len(relay.calls) == 3

    whole = _Relay(lo + 400 * DAY, cap=4500)  # listed later; the full answer in one page
    monkeypatch.setattr(cg, "_ask", whole.ask)
    rows = asyncio.run(cg._window(None, "oi", {}, lo, hi, [0.0]))
    assert len(rows) == 2100 and len(whole.calls) == 1  # no extra request for the days before listing


def test_spot_filters_and_timeframe_floors():
    assert binance.LEVERAGED.fullmatch("BTCUP") and binance.LEVERAGED.fullmatch("ETHBULL")
    assert not binance.LEVERAGED.fullmatch("JUP") and "USDC" in binance.NOT_COINS
    at = datetime(2025, 1, 31, 8, 37)
    assert binance._floor(at, "1h") == datetime(2025, 1, 31, 8, 0)
    assert binance._floor(at, "4h") == datetime(2025, 1, 31, 8, 0)
    assert binance._floor(at, "1d") == datetime(2025, 1, 31, 0, 0)


def test_the_weekly_update_tops_up_live_perps_and_leaves_complete_delisted_ones(monkeypatch, tmp_path):
    monkeypatch.setattr(cg, "RAW", tmp_path / "raw")
    monkeypatch.setattr(cg, "DATASETS", {k: cg.DATASETS[k] for k in ("oi", "funding")})
    calls = []

    async def fake_fetch(client, name, entry, start_ms, last_call):
        calls.append((entry["perp"], name, start_ms))
        return [{"time": start_ms, "close": "1"}]

    class _Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

    monkeypatch.setattr(cg, "fetch_dataset", fake_fetch)
    monkeypatch.setattr("core.data.coinglass_client.CoinglassClient", _Client)
    d0 = int(datetime(2026, 9, 1, tzinfo=timezone.utc).timestamp() * 1000)
    live = {"perp": "AAAUSDT", "coin": "AAA", "status": "TRADING", "listed": "2024-01-01", "delisted": None}
    gone = {"perp": "BBBUSDT", "coin": "BBB", "status": "SETTLING", "listed": "2023-01-01", "delisted": "2026-09-10"}
    for entry, oi_last, funding_last in ((live, d0, d0), (gone, d0 + 9 * DAY, d0 + 6 * DAY)):
        payload = cg.load_raw(entry)
        payload["datasets"] = {"oi": [{"time": oi_last, "close": "1"}], "funding": [{"time": funding_last, "close": "1"}]}
        cg.save_raw(payload)
    asyncio.run(cg.collect([live, gone], update=True))
    assert sorted(calls) == sorted([("AAAUSDT", "oi", d0 + DAY), ("AAAUSDT", "funding", d0 + DAY),
                                    ("BBBUSDT", "funding", d0 + 7 * DAY)])  # BBB's oi already reaches its delisting
    assert [r["time"] for r in cg.load_raw(live)["datasets"]["oi"]] == [d0, d0 + DAY]


def test_venues_map_hyperliquid_names_and_spot_exchanges():
    perps = [{"coin": "PEPE", "perp": "1000PEPEUSDT"}, {"coin": "BLZ", "perp": "BLZUSDT"}, {"coin": "XYZ", "perp": "XYZUSDT"}]
    spot = {"Gate": [{"base_asset": "BLZ"}, {"base_asset": "PEPE"}], "Binance": [{"base_asset": "PEPE"}]}
    hl = {"Hyperliquid": [{"base_asset": "kPEPE"}, {"base_asset": "BTC"}]}
    v = cg.venue_map(perps, spot, hl)
    assert v["hyperliquid"] == {"PEPE": "kPEPE", "BLZ": None, "XYZ": None}
    assert v["spot"] == {"PEPE": ["Binance", "Gate"], "BLZ": ["Gate"], "XYZ": []}
    blz = {"coin": "BLZ", "perp": "BLZUSDT", "spot_venues": ["Gate"], "hl_name": None}
    assert cg.not_listed("hl_ratio", blz) and cg.not_listed("spot_taker", blz)  # no Binance/OKX/Bybit spot
    assert not cg.not_listed("spot_taker_all", blz) and not cg.not_listed("oi", blz)
    assert not cg.not_listed("hl_ratio", {"coin": "BLZ", "perp": "BLZUSDT"})  # no catalog loaded: ask
