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
``slot``, ``captured_at`` and the raw ``rows``. About five relay requests per snapshot, through the
shared Coinglass budget ledger. Nothing here trades.
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


def _now() -> datetime:
    return datetime.now(timezone.utc)


def due_slot(now: datetime) -> Optional[datetime]:
    """The latest slot at or before ``now`` if it is still within the grace period."""
    day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    slot = max(day + timedelta(hours=h) for h in SLOTS_UTC if day + timedelta(hours=h) <= now)
    return slot if now - slot <= SLOT_GRACE else None


def snapshot_path(slot: datetime, root: Path = ROOT) -> Path:
    return root / f"{slot:%Y-%m-%d}" / f"{slot:%H}_coins_markets.json.gz"


async def _coins_markets(client) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for page in range(1, MAX_PAGES + 1):
        if page > 1:
            await asyncio.sleep(PAGE_SPACING_SEC)
        resp = await client.request_json("/api/futures/coins-markets", params={"per_page": PER_PAGE, "page": page})
        payload = resp.get("payload") if isinstance(resp, dict) else None
        if isinstance(payload, dict) and str(payload.get("code")) not in {"0", "None"}:
            raise RuntimeError(f"coins-markets code {payload.get('code')}: {str(payload.get('msg'))[:80]}")
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


def iter_snapshots(root: Path = ROOT) -> Iterator[Tuple[str, str, List[Dict[str, Any]]]]:
    """(slot, captured_at, rows) for every archived snapshot, oldest first."""
    for path in sorted(root.glob("*/*_coins_markets.json.gz")):
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            snap = json.load(fh)
        yield snap.get("slot"), snap.get("captured_at"), snap.get("rows") or []
