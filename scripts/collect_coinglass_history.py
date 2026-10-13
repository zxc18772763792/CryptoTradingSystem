"""Collect the full daily history of Coinglass's per-coin derivatives data for every coin with a
Binance USDT perpetual, trading or delisted, into data/research/coinglass_history/ (research only).

Why (2026-10-13): the user asked to gather, as completely as possible, every kind of historical
data for all coins locally for later research. The studies so far downloaded 131 + 103 coins from
2024-01 only. Coinglass's daily history reaches back to about 2020 for alts (LINK: aggregated OI
from 2020-03, top-trader account ratio 2020-05, global account ratio 2020-10), and one request
returns all of it.

Coins: every Binance USD-M USDT perpetual with status TRADING or SETTLING (delisted: kept, so later
studies are not limited to survivors), named without a contract multiplier (1000PEPE -> PEPE) as in
the earlier studies; the Binance-pair series are asked with the perp itself.
Datasets (daily unless noted; the first day asked):
  oi            aggregated open interest OHLC (all exchanges)                  2020-01-01
  funding       OI-weighted funding OHLC                                       2020-01-01
  funding_vol   volume-weighted funding OHLC                                   2020-01-01
  liquidation   aggregated long / short liquidations, Binance+OKX+Bybit         2020-01-01
  taker         aggregated taker buy / sell volume, Binance+OKX+Bybit           2020-01-01
  spot_taker    spot taker buy / sell volume, Binance+OKX+Bybit                 2020-01-01
  global_ratio  Binance pair, global account long / short                       2020-01-01
  top_account   Binance pair, top-trader account long / short                   2020-01-01
  top_position  Binance pair, top-trader position long / short                  2020-01-01
  net_position  Binance pair, net long / net short change                       2020-01-01
  depth_4h      aggregated order-book depth within +/-1% of price, 4h bars      2024-10-21
  hl_ratio      Hyperliquid account long / short                                2025-07-02
The aggregated series use Binance+OKX+Bybit like the 10/11 and 10/12 studies; a coin trading as
1000X on Binance is X on Coinglass and covers only the exchanges that list it so
(docs/COINGLASS_DATA_CATALOG.md section 5).

One gzip JSON per perp, raw/<PERP>.json.gz: each dataset's rows as the relay returned them, plus
the errors and when each dataset was fetched. Resumable: a rerun asks only for datasets that are
missing or failed (a refusal is an answer and is not asked again). --update refreshes the perp list
(new listings get their whole history) and asks every dataset for the days after its newest stored
bar; a delisted perp is asked only while its data stops before the delisting. Run weekly since
2026-10-13 (scripts/run_weekly_coinglass_topup.ps1, about 5,800 requests). --panels writes one
long-format parquet per dataset to panels/ for research, streaming one perp at a time.

Requests go one every 12 s through the shared budget ledger and pause from 00:10 to 05:55 UTC while
the daily store updates (the relay allows 11 per rolling minute for everything together). About 12
requests per coin, ~8,000 in all: about a day and a half.

    python scripts/collect_coinglass_history.py             # collect (resumable)
    python scripts/collect_coinglass_history.py --update    # top up every dataset to the last closed day
    python scripts/collect_coinglass_history.py --panels    # build panels/<dataset>.parquet
"""
from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts import coinglass_positioning_study as single  # noqa: E402

ROOT = PROJECT_ROOT / "data" / "research" / "coinglass_history"
RAW = ROOT / "raw"
PANELS = ROOT / "panels"
UNIVERSE_PATH = ROOT / "universe.json"
EXCHANGES = "Binance,OKX,Bybit"
FIRST_DAY = datetime(2020, 1, 1, tzinfo=timezone.utc)
LIMIT = 4500
RELAY_PAGE_CAP = 1000  # some history endpoints cut a page at 1000 rows whatever the limit
QUIET_UTC = ((0, 10), (5, 55))  # the daily store's window: no requests from here
MULTIPLIER = re.compile(r"^(?:1000000|100000|10000|1000|1M)(?=[A-Z])")
DAY_MS = 86_400_000
# dataset -> (path, "coin" or "pair", extra params, first day, bar length in ms)
DATASETS: Dict[str, Tuple[str, str, Dict[str, Any], datetime, int]] = {
    "oi": ("/api/futures/open-interest/aggregated-history", "coin", {"interval": "1d"}, FIRST_DAY, DAY_MS),
    "funding": ("/api/futures/funding-rate/oi-weight-history", "coin", {"interval": "1d"}, FIRST_DAY, DAY_MS),
    "funding_vol": ("/api/futures/funding-rate/vol-weight-history", "coin", {"interval": "1d"}, FIRST_DAY, DAY_MS),
    "liquidation": ("/api/futures/liquidation/aggregated-history", "coin",
                    {"interval": "1d", "exchange_list": EXCHANGES}, FIRST_DAY, DAY_MS),
    "taker": ("/api/futures/aggregated-taker-buy-sell-volume/history", "coin",
              {"interval": "1d", "exchange_list": EXCHANGES}, FIRST_DAY, DAY_MS),
    "spot_taker": ("/api/spot/aggregated-taker-buy-sell-volume/history", "coin",
                   {"interval": "1d", "exchange_list": EXCHANGES}, FIRST_DAY, DAY_MS),
    "global_ratio": ("/api/futures/global-long-short-account-ratio/history", "pair", {"interval": "1d"}, FIRST_DAY, DAY_MS),
    "top_account": ("/api/futures/top-long-short-account-ratio/history", "pair", {"interval": "1d"}, FIRST_DAY, DAY_MS),
    "top_position": ("/api/futures/top-long-short-position-ratio/history", "pair", {"interval": "1d"}, FIRST_DAY, DAY_MS),
    "net_position": ("/api/futures/v2/net-position/history", "pair", {"interval": "1d"}, FIRST_DAY, DAY_MS),
    "depth_4h": ("/api/futures/orderbook/aggregated-ask-bids-history", "coin",
                 {"interval": "4h", "exchange_list": EXCHANGES, "range": "1"}, datetime(2024, 10, 21, tzinfo=timezone.utc),
                 4 * 3_600_000),
    "hl_ratio": ("/api/hyperliquid/global-long-short-account-ratio/history", "coin", {"interval": "1d"},
                 datetime(2025, 7, 2, tzinfo=timezone.utc), DAY_MS),
}
LIMITS = {"hl_ratio": 1000}  # the Hyperliquid endpoint refuses limits above 1000


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def coin_of(base_asset: str) -> str:
    return MULTIPLIER.sub("", str(base_asset))


# --------------------------------------------------------------------------------------- universe
def build_universe(refresh: bool = False) -> List[Dict[str, Any]]:
    """Every Binance USDT perpetual, trading first, then delisted; kept until --update refreshes it."""
    if UNIVERSE_PATH.exists() and not refresh:
        return json.loads(UNIVERSE_PATH.read_text(encoding="utf-8"))["perps"]
    import httpx

    from config.settings import settings
    from core.utils.proxy_env import ensure_proxy_env

    ensure_proxy_env(settings)
    info = None
    for attempt in range(6):
        try:
            info = httpx.get(f"{single.FAPI}/fapi/v1/exchangeInfo", timeout=30, trust_env=True).json()
            break
        except Exception:  # noqa: BLE001 - the proxy drops a handshake now and then
            time.sleep(3 * (attempt + 1))
    if info is None:
        raise RuntimeError("Binance exchangeInfo unavailable")
    perps = []
    for s in info["symbols"]:
        if (s.get("contractType") != "PERPETUAL" or s.get("quoteAsset") != "USDT" or s.get("underlyingType") != "COIN"
                or s.get("status") not in {"TRADING", "SETTLING"}):
            continue
        perps.append({"perp": s["symbol"], "coin": coin_of(s["baseAsset"]), "status": s["status"],
                      "listed": datetime.fromtimestamp(int(s.get("onboardDate") or 0) / 1000, tz=timezone.utc).date().isoformat(),
                      "delisted": (datetime.fromtimestamp(int(s["deliveryDate"]) / 1000, tz=timezone.utc).date().isoformat()
                                   if s["status"] == "SETTLING" and s.get("deliveryDate") else None)})
    perps.sort(key=lambda p: (p["status"] != "TRADING", p["perp"]))
    ROOT.mkdir(parents=True, exist_ok=True)
    UNIVERSE_PATH.write_text(json.dumps({"built_at": _now().isoformat(), "perps": perps}, ensure_ascii=False, indent=1),
                             encoding="utf-8")
    return perps


# ----------------------------------------------------------------------------------------- relay
def _in_quiet_window(now: datetime) -> bool:
    (h0, m0), (h1, m1) = QUIET_UTC
    minutes = now.hour * 60 + now.minute
    return h0 * 60 + m0 <= minutes < h1 * 60 + m1


async def _ask(client, path: str, params: Dict[str, Any], last_call: List[float]) -> List[Dict[str, Any]]:
    while _in_quiet_window(_now()):
        await asyncio.sleep(60)
    return await single._coinglass(client, path, params, last_call)


async def _window(client, name: str, params: Dict[str, Any], lo: int, hi: int, last_call: List[float]) -> Dict[int, Dict[str, Any]]:
    """Rows in [lo, hi]; a page cut at the relay's cap is followed by one for the older part."""
    limit = LIMITS.get(name, LIMIT)
    by_ts: Dict[int, Dict[str, Any]] = {}
    for _ in range(8):
        page = await _ask(client, DATASETS[name][0], {**params, "limit": limit, "start_time": lo, "end_time": hi}, last_call)
        stamps = [int(r["time"]) for r in page if isinstance(r, dict) and r.get("time") is not None]
        by_ts.update({int(r["time"]): r for r in page if isinstance(r, dict) and r.get("time") is not None})
        cut = len(stamps) >= limit or len(stamps) == RELAY_PAGE_CAP
        if not stamps or not cut or min(stamps) <= lo + 2 * DATASETS[name][4]:
            break
        hi = min(stamps) - 1
    return by_ts


async def fetch_dataset(client, name: str, entry: Dict[str, Any], start_ms: int, last_call: List[float]) -> List[Dict[str, Any]]:
    """Rows from start_ms to the last closed bar; after a timeout, asked again in yearly pieces."""
    _, kind, extra, _, _ = DATASETS[name]
    params = {**extra, **({"symbol": entry["perp"], "exchange": "Binance"} if kind == "pair" else {"symbol": entry["coin"]})}
    end = _ms(_now().replace(hour=0, minute=0, second=0, microsecond=0)) - 1  # bars that closed by today's 00:00
    try:
        rows = await _window(client, name, params, start_ms, end, last_call)
    except asyncio.TimeoutError:
        rows = {}
        for lo in range(start_ms, end + 1, 365 * DAY_MS):
            rows.update(await _window(client, name, params, lo, min(lo + 365 * DAY_MS - 1, end), last_call))
    return [rows[t] for t in sorted(rows) if t <= end]


# --------------------------------------------------------------------------------------- storage
def raw_path(perp: str) -> Path:
    return RAW / f"{perp}.json.gz"


def load_raw(entry: Dict[str, Any]) -> Dict[str, Any]:
    path = raw_path(entry["perp"])
    if path.exists():
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            return json.load(fh)
    return {"perp": entry["perp"], "coin": entry["coin"], "status": entry["status"], "listed": entry["listed"],
            "delisted": entry.get("delisted"), "datasets": {}, "fetched_at": {}, "errors": {}}


def save_raw(payload: Dict[str, Any]) -> None:
    RAW.mkdir(parents=True, exist_ok=True)
    path = raw_path(payload["perp"])
    tmp = path.with_suffix(".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False)
    tmp.replace(path)


# ----------------------------------------------------------------------------------------- collect
async def collect(perps: List[Dict[str, Any]], update: bool = False) -> None:
    from core.data.coinglass_client import CoinglassClient

    last_call = [0.0]
    started, asked = time.monotonic(), 0
    async with CoinglassClient() as client:
        for i, entry in enumerate(perps):
            payload = load_raw(entry)
            payload["status"], payload["delisted"] = entry["status"], entry.get("delisted")
            delisted_ms = (_ms(datetime.fromisoformat(entry["delisted"]).replace(tzinfo=timezone.utc))
                           if entry["status"] == "SETTLING" and entry.get("delisted") else None)
            for name, (_, _, _, first_day, bar_ms) in DATASETS.items():
                have = payload["datasets"].get(name)
                if have is not None and not update:
                    continue  # answered before (a refusal is stored as [] and not asked again)
                if have is not None and name in payload["errors"] and payload["errors"][name].startswith("refused"):
                    continue
                stamps = [int(r["time"]) for r in have or [] if r.get("time") is not None]
                if update and delisted_ms is not None and have is not None and (not stamps or max(stamps) >= delisted_ms - bar_ms):
                    continue  # a delisted perp's series is complete up to its delisting
                start = max(stamps) + bar_ms if stamps else _ms(first_day)
                try:
                    rows = await fetch_dataset(client, name, entry, start, last_call)
                    known = set(stamps)
                    payload["datasets"][name] = sorted((have or []) + [r for r in rows if int(r["time"]) not in known],
                                                       key=lambda r: int(r["time"]))
                    payload["errors"].pop(name, None)
                    payload["fetched_at"][name] = _now().isoformat()
                except RuntimeError as exc:
                    if "business code" in str(exc):  # the relay has no such series for this coin
                        payload["datasets"].setdefault(name, [])
                        payload["errors"][name] = f"refused: {str(exc)[:140]}"
                    else:
                        payload["errors"][name] = str(exc)[:160]
                except Exception as exc:  # noqa: BLE001 - a timeout or a dropped connection: asked again next run
                    payload["errors"][name] = f"{type(exc).__name__}: {str(exc)[:120]}"
                asked += 1
                save_raw(payload)
            rate = asked / max(1.0, (time.monotonic() - started) / 60)
            sizes = ", ".join(f"{n}={len(payload['datasets'].get(n) or [])}" for n in DATASETS)
            print(f"[{i + 1}/{len(perps)}] {entry['perp']} ({entry['status']}): {sizes} | {rate:.1f} req/min", flush=True)


# ------------------------------------------------------------------------------------------ panels
def build_panels() -> Dict[str, int]:
    """One long-format parquet per dataset (perp, coin, time UTC, the dataset's fields as float64),
    written one perp at a time so memory stays small; rows sorted by perp, then time."""
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    PANELS.mkdir(parents=True, exist_ok=True)
    writers: Dict[str, Any] = {}
    fields: Dict[str, List[str]] = {}
    counts: Dict[str, int] = {}
    try:
        for path in sorted(RAW.glob("*.json.gz")):
            with gzip.open(path, "rt", encoding="utf-8") as fh:
                payload = json.load(fh)
            for name, rows in sorted((payload.get("datasets") or {}).items()):
                if not rows or name not in DATASETS:
                    continue
                frame = pd.DataFrame(rows)
                if name not in fields:  # the first perp with rows fixes the dataset's columns
                    fields[name] = sorted(c for c in frame.columns if c != "time")
                out = pd.DataFrame({"perp": payload["perp"], "coin": payload["coin"],
                                    "time": pd.to_datetime(frame["time"].astype("int64"), unit="ms", utc=True)})
                for column in fields[name]:
                    out[column] = (pd.to_numeric(frame[column], errors="coerce").astype("float64")
                                   if column in frame else float("nan"))
                table = pa.Table.from_pandas(out.sort_values("time"), preserve_index=False)
                if name not in writers:
                    writers[name] = pq.ParquetWriter(str(PANELS / f"{name}.parquet.tmp"), table.schema, compression="zstd")
                writers[name].write_table(table.cast(writers[name].schema))
                counts[name] = counts.get(name, 0) + len(out)
    finally:
        for writer in writers.values():
            writer.close()
    for name in writers:
        (PANELS / f"{name}.parquet.tmp").replace(PANELS / f"{name}.parquet")
    return counts


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--update", action="store_true", help="top up every dataset to the last closed day")
    parser.add_argument("--panels", action="store_true", help="build panels/<dataset>.parquet from raw/")
    args = parser.parse_args(argv)
    if args.panels:
        print(json.dumps(build_panels(), indent=1))
        return 0
    perps = build_universe(refresh=args.update)
    print(f"{len(perps)} perps ({sum(p['status'] == 'TRADING' for p in perps)} trading), {len(DATASETS)} datasets", flush=True)
    asyncio.run(collect(perps, update=args.update))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
