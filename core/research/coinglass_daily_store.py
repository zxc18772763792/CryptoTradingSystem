"""Daily Coinglass bars for the research universe, kept current for the forward trackers.

The Coinglass composite tracker (core/research/coinglass_composite_tracker.py) and the reversal
tracker's liquidation-shock shadow need, every day, the same daily bars that
scripts/coinglass_positioning_study.py backtested: aggregated OI close, OI-weighted funding close,
aggregated long/short liquidations and taker buy/sell volume (Binance+OKX+Bybit), and Binance's
global-account and top-position long/short ratios. This module keeps the last KEEP_DAYS of them
per perp in data/research/coinglass_daily/<PERP>.json.

Seeded from the study's download (data/research/coinglass_positioning/raw/). From then on, once
the bar of day D has closed, one background task asks for whatever each coin is missing - one
request per coin and dataset, 131 x 6 = 786 a day, one every REQUEST_SPACING_SEC so the relay's
rolling 11-per-minute limit keeps room for everything else (about 2.6 hours). It runs beside the
research runner, never inside its pass. Long/short ratio bars are snapshots AT their stamp, so
day D's value is the snapshot at D+1 00:00: the closed daily bars up to D come from the daily
history and the D+1 00:00 snapshot from the hourly history (the daily bar stamped D+1 would still
be moving). Research only: nothing here trades.
"""
from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[2]
STORE_DIR = PROJECT_ROOT / "data" / "research" / "coinglass_daily"
STATUS_PATH = STORE_DIR / "_status.json"
SEED_DIR = PROJECT_ROOT / "data" / "research" / "coinglass_positioning" / "raw"
UNIVERSE_PATH = PROJECT_ROOT / "data" / "research" / "xs_reversal" / "universe.json"
PERP_SYMBOLS_PATH = PROJECT_ROOT / "data" / "research" / "lsr_positioning" / "perp_symbols.json"
KEEP_DAYS = 60
DAY_MS = 86_400_000
HOUR_MS = 3_600_000
START_AFTER = timedelta(minutes=15)  # after the 00:00 snapshot and the reversal formation
REQUEST_SPACING_SEC = 12.0
TIMEOUT_RETRIES, TIMEOUT_WAIT_SEC = 2, 10.0  # a timed-out request is asked again this many times
MAX_ATTEMPTS = 3  # passes a day: a coin still incomplete (error, or a bar Coinglass had not published yet) is asked again
RETRY_UNTIL = timedelta(hours=10)
RETRY_GAP = timedelta(minutes=20)
EXCHANGES = "Binance,OKX,Bybit"
# dataset -> (path, keyed by base or by Binance pair, fields kept)
DATASETS: Dict[str, Tuple[str, str, Tuple[str, ...]]] = {
    "oi": ("/api/futures/open-interest/aggregated-history", "base", ("close",)),
    "funding": ("/api/futures/funding-rate/oi-weight-history", "base", ("close",)),
    "liquidation": ("/api/futures/liquidation/aggregated-history", "base",
                    ("aggregated_long_liquidation_usd", "aggregated_short_liquidation_usd")),
    "taker": ("/api/futures/aggregated-taker-buy-sell-volume/history", "base",
              ("aggregated_buy_volume_usd", "aggregated_sell_volume_usd")),
    "global_ratio": ("/api/futures/global-long-short-account-ratio/history", "pair",
                     ("global_account_long_percent", "global_account_short_percent", "global_account_long_short_ratio")),
    "top_ratio": ("/api/futures/top-long-short-position-ratio/history", "pair",
                  ("top_position_long_percent", "top_position_short_percent", "top_position_long_short_ratio")),
}
SNAPSHOT_AT_STAMP = {"global_ratio", "top_ratio"}

_task: "Optional[asyncio.Task[Any]]" = None


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def last_closed_day(now: datetime) -> datetime:
    """Day D whose daily bar has closed: yesterday (UTC)."""
    return now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)


def needed_stamp(dataset: str, day: datetime) -> int:
    """The newest stamp a dataset needs for day D: D itself, or D+1 00:00 for a snapshot series."""
    return _ms(day + timedelta(days=1)) if dataset in SNAPSHOT_AT_STAMP else _ms(day)


def universe() -> List[Tuple[str, str]]:
    """(base, Binance perp) of the research universe."""
    bases = json.loads(UNIVERSE_PATH.read_text(encoding="utf-8"))["bases"]
    perps = json.loads(PERP_SYMBOLS_PATH.read_text(encoding="utf-8"))
    return [(b, perps[b]) for b in bases if perps.get(b)]


def coin_path(perp: str, root: Optional[Path] = None) -> Path:
    return (root or STORE_DIR) / f"{perp}.json"


def load_coin(perp: str, root: Optional[Path] = None) -> Dict[str, Any]:
    path = coin_path(perp, root)
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def save_coin(perp: str, coin: Dict[str, Any], root: Optional[Path] = None) -> None:
    (root or STORE_DIR).mkdir(parents=True, exist_ok=True)
    tmp = coin_path(perp, root).with_suffix(".tmp")
    tmp.write_text(json.dumps(coin), encoding="utf-8")
    tmp.replace(coin_path(perp, root))


def _keep(rows: List[Dict[str, Any]], fields: Tuple[str, ...]) -> List[Dict[str, Any]]:
    return [{"time": int(r["time"]), **{f: r.get(f) for f in fields}} for r in rows if r.get("time") is not None]


def merge(coin: Dict[str, Any], dataset: str, rows: List[Dict[str, Any]], newest_ms: int) -> None:
    """Add rows by stamp (newer answers win) and keep the last KEEP_DAYS days."""
    by_time = {int(r["time"]): r for r in coin.get(dataset) or []}
    by_time.update({int(r["time"]): r for r in _keep(rows, DATASETS[dataset][2])})
    floor = newest_ms - KEEP_DAYS * DAY_MS
    coin[dataset] = [by_time[t] for t in sorted(by_time) if floor <= t <= newest_ms]


def seed(root: Optional[Path] = None, seed_dir: Optional[Path] = None) -> int:
    """Start each coin from the study's download, once."""
    seeded = 0
    seed_dir = seed_dir or SEED_DIR
    for base, perp in universe():
        if coin_path(perp, root).exists() or not (seed_dir / f"{perp}.json").exists():
            continue
        raw = json.loads((seed_dir / f"{perp}.json").read_text(encoding="utf-8"))
        coin: Dict[str, Any] = {"base": raw.get("base") or base, "perp": perp}
        for dataset in DATASETS:
            rows = raw.get(dataset) or []
            if rows:
                merge(coin, dataset, rows, max(int(r["time"]) for r in rows))
        save_coin(perp, coin, root)
        seeded += 1
    return seeded


def missing(coin: Dict[str, Any], dataset: str, day: datetime) -> bool:
    rows = coin.get(dataset) or []
    return not rows or int(rows[-1]["time"]) < needed_stamp(dataset, day)


def requests_for(coin: Dict[str, Any], base: str, perp: str, dataset: str, day: datetime) -> List[Tuple[str, Dict[str, Any]]]:
    """What to ask for one coin and dataset: closed daily bars after the last stored one, plus
    (snapshot series) the hourly bar stamped D+1 00:00."""
    path, keyed, _ = DATASETS[dataset]
    params: Dict[str, Any] = {"symbol": base} if keyed == "base" else {"symbol": perp, "exchange": "Binance"}
    if dataset in {"liquidation", "taker"}:
        params["exchange_list"] = EXCHANGES
    rows = coin.get(dataset) or []
    last = int(rows[-1]["time"]) if rows else _ms(day) - (KEEP_DAYS + 1) * DAY_MS  # empty: the whole window
    out: List[Tuple[str, Dict[str, Any]]] = []
    if last < _ms(day):  # closed daily bars up to D
        out.append((path, {**params, "interval": "1d", "start_time": last + 1, "end_time": _ms(day) + DAY_MS - 1,
                           "limit": KEEP_DAYS + 5}))
    if dataset in SNAPSHOT_AT_STAMP and (not rows or int(rows[-1]["time"]) < needed_stamp(dataset, day)):
        stamp = needed_stamp(dataset, day)
        out.append((path, {**params, "interval": "1h", "start_time": stamp, "end_time": stamp + HOUR_MS - 1, "limit": 2}))
    return out


async def _ask(client, path: str, params: Dict[str, Any], last_call: List[float]) -> Optional[List[Dict[str, Any]]]:
    """One paced request through the shared budget ledger; None when the relay refuses it for good."""
    from core.data.coinglass_client import CoinglassBudgetExceeded, CoinglassError

    slow = 0
    for _ in range(10):
        wait = REQUEST_SPACING_SEC - (time.monotonic() - last_call[0])
        if wait > 0:
            await asyncio.sleep(wait)
        last_call[0] = time.monotonic()
        try:
            resp = await client.request_json(path, params=params)
        except CoinglassBudgetExceeded:
            await asyncio.sleep(15)
            continue
        except CoinglassError as exc:
            if "429" in str(exc):
                await asyncio.sleep(75)
                continue
            raise
        except (asyncio.TimeoutError, OSError) as exc:  # the relay is slow now and then (10/11: ~1 in 6)
            slow += 1
            if slow > TIMEOUT_RETRIES:
                raise
            logger.debug(f"coinglass daily store: {path} {params.get('symbol')} slow ({type(exc).__name__}), asking again")
            await asyncio.sleep(TIMEOUT_WAIT_SEC)
            continue
        payload = resp.get("payload") if isinstance(resp, dict) else None
        if isinstance(payload, dict) and str(payload.get("code")) not in {"0", "None"}:
            logger.debug(f"coinglass daily store: {path} {params.get('symbol')} refused: {payload.get('msg')}")
            return None
        data = payload.get("data") if isinstance(payload, dict) else None
        return [r for r in data if isinstance(r, dict)] if isinstance(data, list) else []
    raise RuntimeError("coinglass budget never freed")


def load_status(path: Optional[Path] = None) -> Dict[str, Any]:
    path = path or STATUS_PATH
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def save_status(status: Dict[str, Any], path: Optional[Path] = None) -> None:
    path = path or STATUS_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(status, ensure_ascii=False, indent=1), encoding="utf-8")


async def update(day: datetime, *, client=None, root: Optional[Path] = None, status_path: Optional[Path] = None) -> Dict[str, Any]:
    """Bring every coin up to day D. Resumable: each coin is saved as soon as it is done."""
    from core.data.coinglass_client import CoinglassClient

    previous = load_status(status_path)
    attempt = int(previous.get("attempt") or 0) + 1 if previous.get("day") == f"{day:%Y-%m-%d}" else 1
    status = {"day": f"{day:%Y-%m-%d}", "attempt": attempt, "started_at": _now().isoformat(), "requests": 0,
              "refused": [], "errors": []}
    save_status(status, status_path)
    last_call = [0.0]

    async def run(cg) -> None:
        for base, perp in universe():
            coin = load_coin(perp, root) or {"base": base, "perp": perp}
            changed = False
            for dataset in DATASETS:
                if not missing(coin, dataset, day):
                    continue
                for path, params in requests_for(coin, coin.get("base") or base, perp, dataset, day):
                    try:
                        rows = await _ask(cg, path, params, last_call)
                    except Exception as exc:  # noqa: BLE001 - one coin never stops the others
                        status["errors"].append(f"{perp} {dataset}: {type(exc).__name__}: {str(exc)[:80]}")
                        continue
                    status["requests"] += 1
                    if rows is None:
                        status["refused"].append(f"{perp} {dataset}")
                        continue
                    merge(coin, dataset, rows, needed_stamp(dataset, day))
                    changed = True
            if changed:
                save_coin(perp, coin, root)
            save_status(status, status_path)

    if client is None:
        async with CoinglassClient() as own:
            await run(own)
    else:
        await run(client)
    status["completed_at"] = _now().isoformat()
    status["complete_coins"] = sum(complete(load_coin(perp, root), day) for _, perp in universe())
    status["universe_coins"] = len(universe())
    # final: nothing more will be asked today, so the trackers can stop waiting
    status["final"] = (status["complete_coins"] >= status["universe_coins"] or attempt >= MAX_ATTEMPTS
                       or _now() >= day + timedelta(days=1) + RETRY_UNTIL)
    save_status(status, status_path)
    logger.info(f"coinglass daily store: day {status['day']} {status['complete_coins']} coins complete, "
                f"{status['requests']} requests, {len(status['refused'])} refused, {len(status['errors'])} errors")
    return status


def complete(coin: Dict[str, Any], day: datetime) -> bool:
    return bool(coin) and not any(missing(coin, dataset, day) for dataset in DATASETS)


def ensure_current(now: Optional[datetime] = None) -> Dict[str, Any]:
    """Called by the research runner every pass: start today's update in the background if due.
    Returns at once; the update itself paces its requests over a couple of hours."""
    global _task
    now = now or _now()
    day = last_closed_day(now)
    status = load_status()
    if _task is not None and not _task.done():
        return {"status": "updating", **{k: status.get(k) for k in ("day", "requests", "started_at")}}
    if status.get("day") == f"{day:%Y-%m-%d}" and status.get("completed_at"):
        if status.get("final"):
            return {"status": "current", **{k: status.get(k) for k in ("day", "complete_coins", "completed_at")}}
        if now >= day + timedelta(days=1) + RETRY_UNTIL:  # out of time for another pass
            status["final"] = True
            save_status(status)
            return {"status": "current", **{k: status.get(k) for k in ("day", "complete_coins", "completed_at")}}
        if now - datetime.fromisoformat(status["completed_at"]) < RETRY_GAP:
            return {"status": "waiting_retry", **{k: status.get(k) for k in ("day", "complete_coins", "completed_at")}}
    if now - (day + timedelta(days=1)) < START_AFTER:
        return {"status": "waiting", "day": f"{day:%Y-%m-%d}"}
    from core.data.coinglass_client import coinglass_enabled

    if not coinglass_enabled():
        return {"status": "disabled"}
    seeded = seed()
    _task = asyncio.get_running_loop().create_task(update(day))
    _task.add_done_callback(_log_failure)
    return {"status": "started", "day": f"{day:%Y-%m-%d}", "seeded": seeded}


def _log_failure(task: "asyncio.Task[Any]") -> None:
    if not task.cancelled() and task.exception() is not None:
        logger.warning(f"coinglass daily store update failed: {task.exception()}")
