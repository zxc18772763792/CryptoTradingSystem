from datetime import datetime, timezone

from core.events.supply_event_store import SupplyEventStore


def test_supply_event_store_hides_events_not_visible_as_of_backtest_time():
    store = SupplyEventStore(
        [
            {
                "event_id": "LATE_EVENT",
                "symbol": "ARB/USDT",
                "base_asset": "ARB",
                "event_type": "token_unlock",
                "event_time": "2026-06-16T00:00:00Z",
                "first_seen_at": "2026-06-12T00:00:00Z",
                "source": "manual",
            }
        ]
    )

    events = store.active_events(
        symbol="ARB/USDT",
        as_of=datetime(2026, 6, 10, tzinfo=timezone.utc),
        before_days=14,
        after_days=14,
        require_visible=True,
    )

    assert events == []
