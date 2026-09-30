from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from core.research import signal_evidence as se

NOW = datetime(2026, 10, 6, 1, tzinfo=timezone.utc)
DAY = 86_400_000


class _Resp:
    def __init__(self, payload, status=200):
        self._payload, self.status_code = payload, status

    def json(self):
        return self._payload


class _Client:
    def __init__(self, handler):
        self.handler, self.calls = handler, []

    async def get(self, url, params=None):
        self.calls.append(params)
        return self.handler(url, params or {})


def test_book_metrics_flags_a_book_that_ends_inside_the_band():
    shallow = se.book_metrics({"bids": [["0.999", "10"]], "asks": [["1.001", "10"]]}, NOW)
    assert shallow["band_2pct_complete"] is False  # depth sums are lower bounds only
    assert se.book_metrics({"bids": [], "asks": [["1", "1"]]}, NOW)["error"] == "empty_book"


def test_fetch_path_pages_past_the_1500_bar_limit_and_stops_on_failure():
    start, end = 0, 7 * DAY  # 2016 five-minute bars

    def handler(url, params):
        first = params["startTime"]
        return _Resp([[t, 1, 1, 1, 1] for t in range(first, min(params["endTime"] + 1, first + 1500 * 300_000), 300_000)])

    client = _Client(handler)
    bars = asyncio.run(se.fetch_path(client, "AAAUSDT", start, end))
    assert len(bars) == 2016 and len(client.calls) == 2

    failing = _Client(lambda url, params: _Resp({}, 500))
    try:
        asyncio.run(se.fetch_path(failing, "AAAUSDT", start, end))
    except RuntimeError as exc:
        assert "http_500" in str(exc)
    else:
        raise AssertionError("a failed page must raise so the pass retries")


def test_basket_return_needs_half_the_legs_and_exact_bars():
    entry, exit_ = DAY, 8 * DAY

    def handler(url, params):
        if params["symbol"] == "GOOD":
            return _Resp([[entry - DAY + k * DAY, 1, 1, 1, 1.0 + 0.1 * (k == 7)] for k in range(8)])
        return _Resp([[entry - DAY, 1, 1, 1, 1.0]])  # listed mid-window: incomplete, skipped

    assert asyncio.run(se.basket_return(_Client(handler), ["GOOD", "NEW"], entry, exit_))["return_pct"] == 10.0
    assert asyncio.run(se.basket_return(_Client(handler), ["GOOD", "NEW", "NEW2"], entry, exit_)) is None


def test_path_metrics_without_a_stop():
    bars = [[0, 1.0, 1.1, 0.7, 0.8], [300_000, 0.8, 0.9, 0.75, 0.8]]
    out = se.path_metrics(bars, 1.0, 0, 600_000, 0.40)
    assert out["stop_hit_at"] is None and out["stop_gap_pct"] is None
    assert round(out["mae_pct"], 3) == 10.0 and round(out["mfe_pct"], 3) == 30.0 and out["coverage"] == 1.0
