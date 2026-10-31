"""Invalid market observations cannot become paper profits or selected legs."""
import asyncio
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from core.research import announcement_short_tracker as announcement
from core.research import coinglass_composite_tracker as composite
from core.research import listing_short_tracker as listing
from core.research import signal_evidence
from core.research.tracker_data_quality import validate_daily_path

DAY, MINUTE = 86_400_000, 60_000


class Response:
    status_code = 200

    def __init__(self, rows):
        self.rows = rows

    def json(self):
        return self.rows


@pytest.mark.parametrize("high,close", [(1, 2), (float('nan'), .5), (1, float('inf'))])
def test_invalid_daily_high_close_cannot_hide_stop(high, close):
    bars = [[i * DAY, 1, 1, 1, 1] for i in range(16)]
    bars[5][2], bars[5][4] = high, close
    with pytest.raises(ValueError):
        listing.evaluate_trade(bars, [], 20 * DAY)


@pytest.mark.parametrize("high,close", [(1, 2), (float('nan'), .5), (1, float('inf')), (1, 0)])
def test_invalid_minute_stop_path_cannot_settle(high, close):
    trade = dict(symbol='AAAUSDT', status='open', entry_price=1., entry_ms=DAY,
                 exit_due_ms=DAY + 3 * MINUTE, stop_pct=.5)

    class Client:
        async def get(self, url, params=None):
            return Response([[DAY + MINUTE, 1, high, 1, close],
                             [DAY + 2 * MINUTE, 1, 1, 1, .9]]) if url.endswith('/klines') else Response([])

    with pytest.raises(ValueError):
        asyncio.run(announcement._settle(Client(), trade,
            datetime.fromtimestamp((DAY + 10 * MINUTE) / 1000, timezone.utc)))
    assert trade['status'] == 'open' and 'return_pct' not in trade


@pytest.mark.parametrize("price", [float('nan'), float('inf'), -1., True])
def test_invalid_quote_cannot_form_composite_book(monkeypatch, price):
    day = datetime(2026, 10, 1, tzinfo=timezone.utc)
    scores = pd.Series({f'C{i}USDT': i for i in range(40)}, dtype=float)
    monkeypatch.setattr(composite, 'frames_from_store', lambda *a: {})
    monkeypatch.setattr(composite, 'signals', lambda *a: {})
    monkeypatch.setattr(composite, 'composite_row', lambda *a: scores)
    prices = {p: 10. for p in scores.index}
    prices['C39USDT'] = price
    with pytest.raises(ValueError):
        composite.form({'books': {}}, day, day, prices, {}, partial=False)


@pytest.mark.parametrize("entry,exit_price", [(float('nan'), 10.), (0., 10.), (10., float('inf'))])
def test_corrupt_pending_leg_stays_unknown(entry, exit_price):
    formed = datetime(2026, 10, 1, tzinfo=timezone.utc)
    bad = dict(entry=entry, exit=exit_price, funding_until_ms=int((formed + timedelta(days=7)).timestamp()*1000))
    record = dict(formed_at=formed.isoformat(), main=dict(status='open', turnover=1.,
        longs={'BADUSDT': bad}, shorts={'OKUSDT': {'entry': 10.}}))

    class Client:
        async def get(self, *a, **k):
            return Response([])

    asyncio.run(composite.settle(Client(), record, formed + timedelta(days=7), {'OKUSDT': 10.}))
    assert record['main']['status'] == 'open'
    assert 'return' not in bad
    assert bad['settlement_issue'] == 'invalid_position_price'


def test_valid_daily_prices_preserve_zero_volume_and_registered_path():
    # No trading-volume filter or stop/entry threshold is introduced.
    validate_daily_path([[DAY, 1, 1.5, .8, 1.2, 0]], start_ms=DAY)


@pytest.mark.parametrize("entry,exit_price", [(float('nan'), 1), (1, float('inf')), (-1, 1)])
def test_hedge_invalid_endpoint_never_becomes_a_return(entry, exit_price):
    class Client:
        async def get(self, *a, **k):
            return Response([[0, 1, 1, 1, entry], [DAY, 1, 1, 1, exit_price]])

    with pytest.raises(ValueError):
        asyncio.run(signal_evidence.basket_return(Client(), ['BADUSDT'], DAY, 2 * DAY))


@pytest.mark.parametrize("stamp", [DAY + MINUTE + .5, DAY + MINUTE + 1, DAY - MINUTE, DAY + 3 * MINUTE])
def test_minute_stamp_cannot_be_rounded_or_escape_requested_window(stamp):
    trade = dict(symbol='AAAUSDT', status='open', entry_price=1., entry_ms=DAY,
                 exit_due_ms=DAY + 3 * MINUTE, stop_pct=.5)

    class Client:
        async def get(self, *a, **k):
            return Response([[stamp, 1, 1, 1, 1]])

    with pytest.raises(ValueError):
        asyncio.run(announcement._settle(Client(), trade,
            datetime.fromtimestamp((DAY + 10 * MINUTE) / 1000, timezone.utc)))
    assert 'return_pct' not in trade


def test_completed_composite_book_is_untouched_even_if_old_prices_are_bad():
    record = dict(formed_at=datetime(2026, 10, 1, tzinfo=timezone.utc).isoformat(),
                  main=dict(status='closed', net_pct=1., longs={'BADUSDT': {'entry': -1}}))
    import copy
    before = copy.deepcopy(record)
    asyncio.run(composite.settle(None, record, datetime(2026, 10, 8, tzinfo=timezone.utc), {}))
    assert record == before
