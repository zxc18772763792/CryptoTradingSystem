"""Manual/CSV store for supply events."""
from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from core.events.supply_event_schema import SupplyEvent
from core.structural.context import parse_utc_datetime


def _decode_metadata(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        val = json.loads(text)
        return dict(val) if isinstance(val, dict) else {"value": val}
    except Exception:
        return {"raw": text}


class SupplyEventStore:
    def __init__(self, events: Optional[Iterable[SupplyEvent | Mapping[str, Any]]] = None):
        self._events: Dict[str, SupplyEvent] = {}
        for event in events or []:
            self.upsert(event)

    def upsert(self, event: SupplyEvent | Mapping[str, Any]) -> SupplyEvent:
        item = event if isinstance(event, SupplyEvent) else SupplyEvent.from_dict(event)
        self._events[item.event_id] = item
        return item

    def all_events(self) -> List[SupplyEvent]:
        return sorted(self._events.values(), key=lambda item: (item.event_time, item.event_id))

    def visible_events(self, *, symbol: Optional[str] = None, as_of: Any = None) -> List[SupplyEvent]:
        ts = parse_utc_datetime(as_of)
        canonical = str(symbol or "").strip().upper()
        out = []
        for event in self.all_events():
            if canonical and event.symbol != canonical:
                continue
            if event.visible_as_of(ts):
                out.append(event)
        return out

    def active_events(
        self,
        *,
        symbol: str,
        as_of: Any,
        before_days: int = 14,
        after_days: int = 14,
        require_visible: bool = True,
    ) -> List[SupplyEvent]:
        ts = parse_utc_datetime(as_of)
        canonical = str(symbol or "").strip().upper()
        out = []
        for event in self.all_events():
            if event.symbol != canonical:
                continue
            if require_visible and not event.visible_as_of(ts):
                continue
            hours = event.hours_to_event(ts)
            if -24.0 * after_days <= hours <= 24.0 * before_days:
                out.append(event)
        return out

    def load_csv(self, path: str | Path) -> List[SupplyEvent]:
        loaded: List[SupplyEvent] = []
        with Path(path).open("r", encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                payload: Dict[str, Any] = dict(row)
                payload["metadata"] = _decode_metadata(payload.get("metadata"))
                event = self.upsert(payload)
                loaded.append(event)
        return loaded

    @classmethod
    def from_csv(cls, path: str | Path) -> "SupplyEventStore":
        store = cls()
        store.load_csv(path)
        return store


def load_supply_events_csv(path: str | Path) -> SupplyEventStore:
    return SupplyEventStore.from_csv(path)
