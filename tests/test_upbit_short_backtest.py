"""Historical Upbit short scoring must not use data after a simulated exit."""

import pytest

from scripts.upbit_short_backtest import DAY_MS, score_short_trade


def _bars():
    return [[i * DAY_MS, 100, 101, 99, 100] for i in range(8)]


def test_stopped_trade_excludes_later_funding():
    bars = _bars()
    bars[1][2] = 150  # The first holding day crosses the +40% stop.
    funding = [{"fundingTime": i * DAY_MS,
                "fundingRate": "0.01" if i == 1 else "0.20" if i == 3 else "0"}
               for i in range(1, 9)]
    ret, charged, stopped = score_short_trade(bars, funding, 0)
    assert stopped is True
    assert charged == pytest.approx(0.01)
    assert ret == pytest.approx((100 - 142.8) / 100 - 0.001 + 0.01)


def test_missing_funding_and_daily_gap_are_unusable():
    bars = _bars()
    with pytest.raises(ValueError, match="missing_funding"):
        score_short_trade(bars, [], 0)
    bars[3][0] += DAY_MS
    with pytest.raises(ValueError, match="incomplete_daily_bars"):
        score_short_trade(bars, [{"fundingTime": DAY_MS, "fundingRate": "0"}], 0)


def test_partial_funding_window_is_unusable():
    with pytest.raises(ValueError, match="incomplete_funding"):
        score_short_trade(_bars(), [{"fundingTime": DAY_MS, "fundingRate": "0"}], 0)
