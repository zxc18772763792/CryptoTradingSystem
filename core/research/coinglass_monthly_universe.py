"""Monthly, auditable local-history universe for an independent paper tracker.

Only membership changes: the composite's trained weights remain frozen. Successful
snapshots are immutable, never reconstructed for a missed month. A late refresh is
available only after it was actually observed, not backdated to the month's start.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd
from loguru import logger

ROOT = Path(__file__).resolve().parents[2] / "data/research/coinglass_composite_monthly_v1"
SNAPSHOT_DIR = ROOT / "universes"
MIN_BARS = 120 * 24
MIN_COINS = 30
RULE = "local closed 1h bars >= 2880, excluding stablecoins, and a TRADING Binance USDT perpetual; monthly UTC"
_task: Optional[asyncio.Task] = None
_last_error: Optional[str] = None


def _now() -> datetime:
    return datetime.now(timezone.utc)


def load_snapshot(now: Optional[datetime] = None) -> dict[str, Any]:
    now = (now or _now()).astimezone(timezone.utc)
    path = SNAPSHOT_DIR / f"{now:%Y-%m}.json"
    if not path.exists():
        return {}
    snapshot = json.loads(path.read_text(encoding="utf-8"))
    if datetime.fromisoformat(snapshot["selected_at"]) > now:
        return {}
    return snapshot


def members(now: Optional[datetime] = None) -> list[tuple[str, str]]:
    return sorted(load_snapshot(now).get("members", {}).items())


async def history_counts(perps: dict[str, str], as_of: datetime) -> dict[str, int]:
    from core.data import data_storage
    from scripts.train_ml_signal import _local_symbols

    def count(symbol: str) -> int:
        import pyarrow.parquet as pq
        from core.data.data_storage import _parquet_index_column
        from core.data.path_utils import candidate_symbol_dirs
        from core.data.kline_partitions import list_part_files, part_read_lock

        stamps_seen: set[int] = set()
        floor, ceiling = pd.Timestamp(as_of - timedelta(days=3000)), pd.Timestamp(as_of)
        for directory in candidate_symbol_dirs(data_storage.storage_path, "binance", symbol):
            with part_read_lock(directory / "1h_parts"):
                files = ([directory / "1h.parquet"] if (directory / "1h.parquet").exists() else [])
                files += list_part_files(directory / "1h_parts")
                for path in files:
                    index = _parquet_index_column(path)
                    if index is None:
                        raise ValueError(f"hourly history missing timestamp index: {path.name}")
                    with pq.ParquetFile(path) as parquet:
                        for batch in parquet.iter_batches(batch_size=4096, columns=[index, "close"], use_threads=False):
                            stamps = pd.DatetimeIndex(pd.to_datetime(batch.column(index).to_pandas(), utc=True))
                            close = pd.to_numeric(batch.column("close").to_pandas(), errors="coerce")
                            valid = ((stamps >= floor) & (stamps + pd.Timedelta(hours=1) <= ceiling)
                                     & (stamps == stamps.floor("h")) & np.isfinite(close) & (close > 0))
                            stamps_seen.update(stamps[valid].asi8.tolist())
        return len(stamps_seen)

    counts = {}
    symbols = _local_symbols("binance")
    for number, symbol in enumerate(symbols, 1):
        base = symbol.split("/")[0]
        if base not in perps:
            continue
        counts[base] = await asyncio.to_thread(count, symbol)
        if number % 50 == 0:
            logger.info(f"Coinglass monthly universe: scanned {number}/{len(symbols)} local symbols")
    return counts


async def refresh(client, now: Optional[datetime] = None) -> dict[str, Any]:
    from core.research.xs_reversal_tracker import FAPI, perp_symbols
    from core.utils.file_lock import advisory_file_lock

    now = (now or _now()).astimezone(timezone.utc)
    existing = load_snapshot(now)
    if existing:
        return existing
    # Bootstrap at the observed cutoff; subsequent months use closed history
    # before the monthly boundary. Exchange eligibility is observed at refresh.
    bootstrap = not any(SNAPSHOT_DIR.glob("*.json"))
    as_of = now.replace(minute=0, second=0, microsecond=0)
    if not bootstrap:
        as_of = as_of.replace(day=1, hour=0)
    response = await client.get(f"{FAPI}/exchangeInfo")
    response.raise_for_status()
    info = response.json()
    if not isinstance(info, dict) or not isinstance(info.get("symbols"), list) or not info["symbols"]:
        raise ValueError("invalid Binance exchangeInfo; monthly universe unchanged")
    perps = perp_symbols(info)
    counts = await history_counts(perps, as_of)
    selected = {base: perps[base] for base in sorted(counts) if counts[base] >= MIN_BARS}
    if len(selected) < MIN_COINS:
        raise ValueError(f"monthly universe has only {len(selected)} eligible coins; no snapshot published")
    observed = _now()
    if observed.strftime("%Y-%m") != now.strftime("%Y-%m"):
        raise ValueError("month changed during selection; retry for the new month")
    digest = hashlib.sha256(json.dumps(selected, sort_keys=True).encode()).hexdigest()
    previous_files = sorted(p for p in SNAPSHOT_DIR.glob("*.json") if p.stem < f"{now:%Y-%m}")
    previous = json.loads(previous_files[-1].read_text(encoding="utf-8"))["members"] if previous_files else {}
    snapshot = {"month": f"{now:%Y-%m}", "id": f"{now:%Y-%m}:{digest[:16]}",
                "selected_at": observed.isoformat(), "as_of": as_of.isoformat(), "rule": RULE,
                "bootstrap": bootstrap, "members": selected,
                "history_bars": {b: counts[b] for b in selected}, "exchange_info": info,
                "added": sorted(set(selected) - set(previous)), "removed": sorted(set(previous) - set(selected))}
    path = SNAPSHOT_DIR / f"{now:%Y-%m}.json"
    with advisory_file_lock(path):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(snapshot, ensure_ascii=False, indent=1), encoding="utf-8")
        tmp.replace(path)
    return snapshot


def ensure_current() -> dict[str, Any]:
    """Keep local scans and network refresh off the research scheduler's path."""
    global _task
    snapshot = load_snapshot()
    if snapshot:
        return {"status": "current", "month": snapshot["month"], "coins": len(snapshot["members"])}
    if _task is not None and not _task.done():
        return {"status": "updating"}

    async def run():
        global _last_error
        from core.research.exchange_research_runner import _client
        try:
            async with _client() as client:
                await refresh(client)
            _last_error = None
        except Exception as exc:
            _last_error = f"{type(exc).__name__}: {exc}"
            logger.warning(f"Coinglass monthly universe: {_last_error}")

    _task = asyncio.get_running_loop().create_task(run())
    return {"status": "started", "last_error": _last_error}
