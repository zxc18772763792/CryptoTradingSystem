"""Frequent paper-risk observations within the serial exchange research pass.

Baseline discovery intervals remain 30m/1h/6h. Known future signals and active
risk shadows are checked every scheduler pass (~5m), before slow research jobs.
The announcement tracker has its own 90s worker and is deliberately excluded
here so two owners never overwrite its parent ledger.
"""
from __future__ import annotations

from datetime import datetime, timezone
from functools import partial
from typing import Any, Dict

from core.research import event_risk_shadow


def _now():
    return datetime.now(timezone.utc)


def providers():
    from core.research import listing_short_tracker as listing, unlock_short_tracker as unlock, upbit_caution_tracker as korean
    yield ("listing_short", listing.STATE_PATH, listing.load_state, listing.save_state, listing.risk_shadow_sources)
    for key, spec in unlock.VARIANTS.items():
        yield (f"unlock_{key}", spec["state_path"], unlock.load_state, unlock.save_state, unlock.risk_shadow_sources)
    for key, spec in korean.STRATEGIES.items():
        yield (key, spec["state_path"], korean.load_state, korean.save_state, partial(korean.risk_shadow_sources, strategy=key))


async def poll(client) -> Dict[str, Any]:
    out = {}
    for name, path, load, save, sources_for in providers():
        if not path.exists():
            continue
        try:
            state = load(path)
            source_error = None
            try:
                sources = sources_for(state)
            except Exception as exc:
                sources = []  # previously entered shadows still have frozen legs
                source_error = f"{type(exc).__name__}: {str(exc)[:120]}"
            now = _now()
            if event_risk_shadow.STATE_KEY not in state:
                entered = [s["source_id"] for s in sources if s["source_entry_ms"] <= now.timestamp()*1000
                           or s.get("source_status") in {"open", "closed", "stopped", "schedule_changed", "delisted"}]
                event_risk_shadow.register(state, now, entered)
                save(state, path)
            try:
                out[name] = await event_risk_shadow.apply_to_tracker(client, state, sources, now, clock=_now)
                if source_error:
                    out[name]["source_adapter_error"] = source_error
            finally:
                # Preserve pending exits/decisions even if an optional request fails.
                # Do not move the baseline's updated_at timestamp.
                save(state, path)
        except Exception as exc:
            out[name] = {"status": "unavailable", "error": f"{type(exc).__name__}: {str(exc)[:150]}"}
    return {"checked_at": _now().isoformat(), "cadence": "each serial research pass (~5min), gaps unobserved", "trackers": out}
