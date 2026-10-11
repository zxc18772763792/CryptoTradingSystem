"""Forward archive of Coinglass's all-coin market snapshot (research only, since 2026-10-11).

Why: the relay's history endpoints cover open interest, funding, liquidations, taker volume and
Binance long/short ratios, and scripts/coinglass_positioning_study.py backtests those. The
coins-markets snapshot adds what no history endpoint returns - every futures coin at once with
cross-exchange windows from 5m to 24h (price, OI and its change, OI / market cap, liquidations by
side, taker long/short volume, OI- and volume-weighted funding) - and a point-in-time copy cannot
be revised afterwards. Kept for an out-of-sample check of whatever the backtest finds, and for
the fields it cannot test.

Four snapshots a day, at the first pass after 00, 06, 12 and 18 UTC (a slot missed by more than
SLOT_GRACE is skipped, so every file is within 3h of its slot). Each is one gzip JSON file,
append-only: data/research/coinglass_snapshots/<YYYY-MM-DD>/<HH>_coins_markets.json.gz with
``slot``, ``captured_at`` and the raw ``rows`` (about 1,500 coins, eight relay requests).

Also once a day from 03:00 UTC: the cross-exchange 7-day liquidation map (estimated liquidation
volume by price level) of each coin in the research universe. The relay has no history of it,
so the idea that price drifts toward large liquidation clusters can only be tested forward.
One request per coin, LIQ_MAP_BATCH coins per runner pass, progress kept in a partial file; the
day's maps end up in <YYYY-MM-DD>/liq_map_7d.json.gz. All requests go through the shared
Coinglass budget ledger. Nothing here trades.
"""
from __future__ import annotations

import asyncio
import gzip
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ROOT = PROJECT_ROOT / "data" / "research" / "coinglass_snapshots"
SLOTS_UTC = (0, 6, 12, 18)
SLOT_GRACE = timedelta(hours=3)
PER_PAGE = 200
MAX_PAGES = 15
PAGE_SPACING_SEC = 6.0  # the relay allows 11 requests per rolling minute, shared with other jobs
BUDGET_RETRIES, BUDGET_WAIT_SEC = 8, 10.0
UNIVERSE_PATH = PROJECT_ROOT / "data" / "research" / "xs_reversal" / "universe.json"
LIQ_MAP_HOUR_UTC = 3
LIQ_MAP_BATCH = 10  # coins per runner pass: about a minute of requests, never a long hold


def _now() -> datetime:
    return datetime.now(timezone.utc)


def due_slot(now: datetime) -> Optional[datetime]:
    """The latest slot at or before ``now`` if it is still within the grace period."""
    day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    slot = max(day + timedelta(hours=h) for h in SLOTS_UTC if day + timedelta(hours=h) <= now)
    return slot if now - slot <= SLOT_GRACE else None


def snapshot_path(slot: datetime, root: Path = ROOT) -> Path:
    return root / f"{slot:%Y-%m-%d}" / f"{slot:%H}_coins_markets.json.gz"


class RelayRefused(RuntimeError):
    """The relay answered with a business error code: asking again will not help."""


async def _request(client, path: str, params: Dict[str, Any]) -> Any:
    """One request; when the shared minute budget is full, wait for it rather than lose the work
    already done in this pass (a relay 429 still fails the pass, which is retried on the next one)."""
    from core.data.coinglass_client import CoinglassBudgetExceeded

    for _ in range(BUDGET_RETRIES):
        try:
            resp = await client.request_json(path, params=params)
            break
        except CoinglassBudgetExceeded:
            await asyncio.sleep(BUDGET_WAIT_SEC)
    else:
        raise RuntimeError("coinglass minute budget stayed full")
    payload = resp.get("payload") if isinstance(resp, dict) else None
    if isinstance(payload, dict) and str(payload.get("code")) not in {"0", "None"}:
        raise RelayRefused(f"{path} code {payload.get('code')}: {str(payload.get('msg'))[:80]}")
    return payload


async def _request_page(client, page: int) -> Any:
    return await _request(client, "/api/futures/coins-markets", {"per_page": PER_PAGE, "page": page})


async def _coins_markets(client) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for page in range(1, MAX_PAGES + 1):
        if page > 1:
            await asyncio.sleep(PAGE_SPACING_SEC)
        payload = await _request_page(client, page)
        data = payload.get("data") if isinstance(payload, dict) else None
        items = [r for r in data if isinstance(r, dict)] if isinstance(data, list) else []
        rows += items
        if len(items) < PER_PAGE:
            break
    return rows


async def tick(now: Optional[datetime] = None, *, client=None, root: Path = ROOT) -> Dict[str, Any]:
    """Take the due slot's snapshot once; any other pass costs no request."""
    now = now or _now()
    slot = due_slot(now)
    if slot is None:
        return {"status": "idle", "checked_at": now.isoformat()}
    path = snapshot_path(slot, root)
    if path.exists():
        return {"status": "done", "slot": slot.isoformat(), "path": str(path)}
    if client is None:
        from core.data.coinglass_client import CoinglassClient, coinglass_enabled

        if not coinglass_enabled():
            return {"status": "disabled", "slot": slot.isoformat()}
        async with CoinglassClient() as own:
            rows = await _coins_markets(own)
    else:
        rows = await _coins_markets(client)
    if not rows:
        raise RuntimeError("coins-markets returned no rows")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as fh:
        json.dump({"slot": slot.isoformat(), "captured_at": _now().isoformat(), "rows": rows}, fh, ensure_ascii=False)
    tmp.replace(path)
    logger.info(f"coinglass snapshot {slot:%Y-%m-%d %H}:00: {len(rows)} coins")
    return {"status": "captured", "slot": slot.isoformat(), "coins": len(rows), "path": str(path)}


def liq_map_path(day: datetime, root: Path = ROOT) -> Path:
    return root / f"{day:%Y-%m-%d}" / "liq_map_7d.json.gz"


def _universe_bases() -> List[str]:
    return list(json.loads(UNIVERSE_PATH.read_text(encoding="utf-8")).get("bases") or [])


async def _fetch_maps(client, todo: List[str], maps: Dict[str, Any]) -> None:
    for i, base in enumerate(todo):
        if i:
            await asyncio.sleep(PAGE_SPACING_SEC)
        try:
            payload = await _request(client, "/api/futures/liquidation/aggregated-map", {"symbol": base, "range": "7d"})
        except RelayRefused as exc:  # e.g. a coin the relay does not cover: recorded, not asked again
            maps[base] = {"captured_at": _now().isoformat(), "error": str(exc)[:160]}
            continue
        maps[base] = {"captured_at": _now().isoformat(), "data": (payload or {}).get("data")}


async def tick_liq_maps(now: Optional[datetime] = None, *, client=None, root: Path = ROOT,
                        bases: Optional[List[str]] = None) -> Dict[str, Any]:
    """Collect today's maps LIQ_MAP_BATCH coins at a time; write the day's file once all are in."""
    now = now or _now()
    if now.hour < LIQ_MAP_HOUR_UTC:
        return {"status": "idle", "checked_at": now.isoformat()}
    day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    label = f"{day:%Y-%m-%d}"
    final = liq_map_path(day, root)
    if final.exists():
        return {"status": "done", "day": label}
    bases = _universe_bases() if bases is None else bases
    partial = final.parent / "liq_map_7d.partial.json"
    state = json.loads(partial.read_text(encoding="utf-8")) if partial.exists() else {"day": label, "maps": {}}
    todo = [b for b in bases if b not in state["maps"]][:LIQ_MAP_BATCH]
    try:
        if todo and client is None:
            from core.data.coinglass_client import CoinglassClient, coinglass_enabled

            if not coinglass_enabled():
                return {"status": "disabled", "day": label}
            async with CoinglassClient() as own:
                await _fetch_maps(own, todo, state["maps"])
        elif todo:
            await _fetch_maps(client, todo, state["maps"])
    finally:  # keep what this pass fetched even when a 429 or a network error ends it early
        partial.parent.mkdir(parents=True, exist_ok=True)
        partial.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
    remaining = [b for b in bases if b not in state["maps"]]
    if remaining:
        return {"status": "collecting", "day": label, "done": len(state["maps"]), "remaining": len(remaining)}
    tmp = final.with_suffix(".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as fh:
        json.dump({**state, "completed_at": _now().isoformat()}, fh, ensure_ascii=False)
    tmp.replace(final)
    partial.unlink()
    refused = sum("error" in m for m in state["maps"].values())
    logger.info(f"coinglass liquidation maps {label}: {len(state['maps'])} coins, {refused} refused")
    return {"status": "captured", "day": label, "coins": len(state["maps"]), "refused": refused}


def iter_snapshots(root: Path = ROOT) -> Iterator[Tuple[str, str, List[Dict[str, Any]]]]:
    """(slot, captured_at, rows) for every archived snapshot, oldest first."""
    for path in sorted(root.glob("*/*_coins_markets.json.gz")):
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            snap = json.load(fh)
        yield snap.get("slot"), snap.get("captured_at"), snap.get("rows") or []
