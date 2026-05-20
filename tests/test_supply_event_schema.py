from datetime import datetime, timezone

import pytest

from core.events.supply_event_schema import SupplyEvent, validate_supply_event_payload
from core.events.supply_event_store import SupplyEventStore


def _event(**overrides):
    payload = {
        "event_id": "ARB_unlock_2026_06_16",
        "symbol": "ARB/USDT",
        "base_asset": "ARB",
        "event_type": "token_unlock",
        "event_time": "2026-06-16T00:00:00Z",
        "first_seen_at": "2026-05-20T00:00:00Z",
        "source": "manual",
        "unlock_pct_float": 0.12,
        "recipient_type": "investor",
        "confidence": 0.9,
    }
    payload.update(overrides)
    return payload


def test_supply_event_requires_first_seen_for_pit_backtests():
    payload = _event()
    payload.pop("first_seen_at")

    with pytest.raises(ValueError, match="first_seen_at"):
        SupplyEvent.from_dict(payload)

    assert validate_supply_event_payload(payload)


def test_supply_event_visible_as_of_uses_first_seen_not_event_time():
    event = SupplyEvent.from_dict(_event(first_seen_at="2026-06-01T00:00:00Z"))

    assert event.visible_as_of(datetime(2026, 5, 31, tzinfo=timezone.utc)) is False
    assert event.visible_as_of(datetime(2026, 6, 1, tzinfo=timezone.utc)) is True


def test_supply_event_store_filters_active_visible_window():
    store = SupplyEventStore([_event(first_seen_at="2026-05-20T00:00:00Z")])

    visible = store.active_events(
        symbol="ARB/USDT",
        as_of=datetime(2026, 6, 10, tzinfo=timezone.utc),
        before_days=14,
        after_days=14,
    )

    assert len(visible) == 1
    assert visible[0].event_id == "ARB_unlock_2026_06_16"
