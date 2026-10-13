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
