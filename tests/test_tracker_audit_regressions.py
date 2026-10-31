"""Counterexamples found in the 2026-10-29 tracker audit; no network or ledgers."""
import asyncio
from datetime import datetime, timezone

import pandas as pd
import pytest

from core.research import announcement_short_tracker as announcement
from core.research import listing_short_tracker as listing
from core.research import unlock_short_tracker as unlock
from core.research import upbit_caution_tracker as korea
from core.research import signal_evidence
from core.research.tracker_data_quality import funding_rate_sum

DAY = 86_400_000


def daily():
    return [[i * DAY, 1.0, 1.0, 1.0, 1.0] for i in range(17)]


@pytest.mark.parametrize("mutation", ["gap", "duplicate", "nan", "zero"])
def test_listing_cannot_settle_corrupt_daily_stop_path(mutation):
    bars = daily()
    if mutation == "gap":
        del bars[4]  # The missing day could have hit the stop.
    elif mutation == "duplicate":
        bars.insert(4, bars[3][:])
    else:
        bars[5][2] = float("nan") if mutation == "nan" else 0.0
    with pytest.raises(ValueError):
        listing.evaluate_trade(bars, [], 20 * DAY)


def test_korea_duplicate_bar_cannot_hide_stop_with_dict_overwrite():
    bars = daily()[:8]
    bars.insert(3, [2 * DAY, 1.0, 2.0, 1.0, 1.0])
    with pytest.raises(ValueError):
        korea.evaluate_trade(bars, [], 0, 10 * DAY)


class Response:
    def __init__(self, rows, status=200):
        self.rows, self.status_code = rows, status

    def json(self):
        return self.rows

    def raise_for_status(self):
        if self.status_code != 200:
            raise RuntimeError(f"HTTP {self.status_code}")


@pytest.mark.parametrize("rows,status", [({"code": -1121}, 400), ({"code": -1}, 200),
    ([{"fundingTime": DAY, "fundingRate": None}], 200),
    ([{"fundingTime": DAY, "fundingRate": "NaN"}], 200),
    ([{"fundingTime": DAY, "fundingRate": ".001"}] * 2, 200)])
def test_unlock_bad_funding_never_becomes_zero_or_double_income(rows, status):
    class Client:
        async def get(self, *args, **kwargs):
            return Response(rows, status)
    with pytest.raises((ValueError, TypeError, RuntimeError)):
        asyncio.run(unlock._funding(Client(), "AAAUSDT", pd.Timestamp(0, tz="UTC"),
                                    pd.Timestamp(2 * DAY, unit="ms", tz="UTC")))


def test_announcement_truncated_tail_stays_unresolved_after_grace():
    entry, due = DAY, DAY + 24 * 3_600_000
    trade = {"symbol": "AAAUSDT", "status": "open", "entry_price": 1.0,
             "entry_ms": entry, "exit_due_ms": due, "stop_pct": .5}
    class Client:
        async def get(self, url, params=None):
            if url.endswith("/klines"):
                # The path ends 12 hours early; the unseen tail may hit the stop.
                return Response([[t, 1, 1, 1, .8] for t in
                                 range(entry + 60_000, entry + 12 * 3_600_000, 60_000)])
            return Response([])
    asyncio.run(announcement._settle(Client(), trade,
        datetime.fromtimestamp((due + 4 * DAY) / 1000, timezone.utc)))
    assert trade["status"] == "unresolved"
    assert "return_pct" not in trade


def test_basket_price_and_funding_cannot_use_disjoint_survivors(monkeypatch):
    async def price(client, symbols, *args):
        s = symbols[0]
        return None if s == "D" else {"return_pct": 200.0 if s == "A" else 0.0}
    async def funding(client, symbols, *args):
        s = symbols[0]
        return None if s == "A" else .9 if s == "D" else .01
    monkeypatch.setattr(signal_evidence, "basket_return", price)
    monkeypatch.setattr(signal_evidence, "basket_funding", funding)
    leg = asyncio.run(signal_evidence.matched_basket_settlement(None, list("ABCD"), DAY, 8 * DAY))
    assert leg["resolved_symbols"] == ["B", "C"]
    assert leg["missing_symbols"] == ["A", "D"] and leg["partial"]
    assert leg["return_pct"] == 0 and leg["funding_rate_sum"] == pytest.approx(.01)
    # Independent old survivor averages would have invented +36% before fees.
    assert (200 / 3 - (.01 + .01 + .9) / 3 * 100) > 35


def test_basket_joint_coverage_needs_half_the_original_members(monkeypatch):
    async def price(client, symbols, *args):
        return None if symbols == ["C"] else {"return_pct": 0.0}
    async def funding(client, symbols, *args):
        return None if symbols == ["A"] else .01
    monkeypatch.setattr(signal_evidence, "basket_return", price)
    monkeypatch.setattr(signal_evidence, "basket_funding", funding)
    assert asyncio.run(signal_evidence.matched_basket_settlement(None, list("ABC"), DAY, 8 * DAY)) is None


def test_valid_funding_keeps_each_trackers_registered_boundary():
    rows = [{"fundingTime": t, "fundingRate": r} for t, r in
            [(DAY, ".01"), (2 * DAY, "-.02"), (3 * DAY, ".4")]]
    assert funding_rate_sum(rows, DAY, 2 * DAY) == pytest.approx(-.02)
    assert funding_rate_sum(rows, DAY, 2 * DAY, include_start=True) == pytest.approx(-.01)


def test_listing_rejects_missing_launch_day_even_with_fifteen_later_rows():
    with pytest.raises(ValueError, match="missing_first_daily_bar"):
        listing.evaluate_trade(daily()[1:], [], 20 * DAY, expected_start_ms=0)


def test_terminal_status_without_primary_return_is_not_a_measured_sample():
    t = {"symbol": "AAAUSDT", "status": "closed", "entry_at": DAY,
         "entry_day": "2026-10-01", "backfilled": False, "late": False}
    for view in (listing.summary({"trades": {"x": t}}),
                 unlock.summary({"trades": {"x": t}}),
                 korea.summary({"trades": {"x": t}}, "caution_hedged")):
        assert view["forward_completed"] == 0
        assert view["completed_without_primary"] == 1
        assert view["retirement"]["n"] == 0
