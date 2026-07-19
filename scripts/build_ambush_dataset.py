"""Build the small-cap ambush research dataset (klines + funding + OI + mcap).

Downloads, for a small-cap Binance USDT-perp universe:
  - 1h klines           (Binance fapi, free)
  - funding rate history (Binance fapi, free)
  - OI history 1d + 4h   (Coinglass via project client, budget-managed)
  - daily market cap     (CoinGecko free tier; supply-approx fallback)

Layout: data/research/ambush_modes/{klines_1h,funding,oi_1d,oi_4h,mcap_1d}/<BASE>.parquet
plus manifest.json with per-symbol provenance. Re-runs skip files that already
exist unless --refresh is passed, so an interrupted run can simply be restarted.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from loguru import logger

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.utils.aiohttp_resolver_hardening import install_aiohttp_threaded_resolver  # noqa: E402

install_aiohttp_threaded_resolver()

from core.data.coinglass_client import (  # noqa: E402
    CoinglassBudgetExceeded,
    CoinglassClient,
    CoinglassError,
)
from core.data.coinglass_registry import COINGLASS_DATASET_MANIFESTS  # noqa: E402

FAPI = "https://fapi.binance.com"
CG_API = "https://api.coingecko.com/api/v3"
OUT_DIR = PROJECT_ROOT / "data" / "research" / "ambush_modes"
SESSION = requests.Session()
SESSION.headers["User-Agent"] = "crypto-trading-system-research/1.0"

MAJOR_BASES = {"BTC", "ETH", "BNB", "SOL", "XRP", "DOGE", "ADA", "TRX", "TON"}
NON_ALT_BASES = {"USDT", "USDC", "FDUSD", "BUSD", "TUSD", "USDE", "USDS", "DAI", "PAXG", "XAUT"}
# Known pump/dump archetypes worth having in-sample when still listed on fapi.
FORCE_INCLUDE = [
    "PNUT", "ACT", "MOODENG", "NEIRO", "RAVE", "GENIUS", "SIREN", "MYX", "COAI",
    "AIA", "TST", "CHILLGUY", "HIPPO", "BAN", "GOAT", "PONKE", "BOME", "MEW",
    "SLERF", "KOMA", "PIPPIN", "TURBO", "ORDI", "ALPACA",
]

CG_PACE_SEC = 6.5
COINGLASS_PACE_SEC = 6.8
BINANCE_PACE_SEC = 0.3


def _utc_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _naive_utc_index(ms_values: List[int]) -> pd.DatetimeIndex:
    return pd.DatetimeIndex(pd.to_datetime(ms_values, unit="ms", utc=True).tz_localize(None))


def _get_json(url: str, params: Optional[Dict[str, Any]] = None, *, retries: int = 4, timeout: float = 25.0) -> Any:
    last_exc: Optional[Exception] = None
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, params=params, timeout=timeout)
            if resp.status_code in (403, 429):
                # 403 shows up as transient WAF blocking on some fapi endpoints;
                # treat it like a rate limit and back off instead of failing.
                wait = 30.0 * (attempt + 1)
                logger.warning(f"{resp.status_code} from {url}, sleeping {wait:.0f}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            time.sleep(2.0 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed after {retries} tries: {last_exc}")


def load_universe(days: int, max_symbols: int) -> List[Dict[str, Any]]:
    info = _get_json(f"{FAPI}/fapi/v1/exchangeInfo")
    perps = [
        s
        for s in info.get("symbols", [])
        if s.get("contractType") == "PERPETUAL"
        and s.get("quoteAsset") == "USDT"
        and s.get("status") == "TRADING"
    ]
    tickers = {t["symbol"]: t for t in _get_json(f"{FAPI}/fapi/v1/ticker/24hr")}
    rows: List[Dict[str, Any]] = []
    for s in perps:
        symbol = s["symbol"]
        base = s["baseAsset"]
        if base in MAJOR_BASES or base in NON_ALT_BASES:
            continue
        ticker = tickers.get(symbol) or {}
        rows.append(
            {
                "symbol": symbol,
                "base": base,
                "onboard": int(s.get("onboardDate") or 0),
                "quote_volume_24h": float(ticker.get("quoteVolume") or 0.0),
            }
        )
    rows.sort(key=lambda r: r["quote_volume_24h"], reverse=True)
    forced = [r for r in rows if r["base"] in FORCE_INCLUDE]
    ranked = [r for r in rows if r["base"] not in FORCE_INCLUDE]
    universe = forced + ranked[: max(0, max_symbols - len(forced))]
    logger.info(
        f"universe: {len(universe)} symbols ({len(forced)} force-included, "
        f"{len(perps)} total USDT perps on fapi)"
    )
    return universe


def fetch_klines_1h(symbol: str, days: int, out_path: Path) -> int:
    start = _utc_ms(datetime.now(timezone.utc) - timedelta(days=days))
    frames: List[List[Any]] = []
    cursor = start
    while True:
        batch = _get_json(
            f"{FAPI}/fapi/v1/klines",
            {"symbol": symbol, "interval": "1h", "startTime": cursor, "limit": 1500},
        )
        if not batch:
            break
        frames.extend(batch)
        cursor = int(batch[-1][0]) + 3_600_000
        time.sleep(BINANCE_PACE_SEC)
        if len(batch) < 1500:
            break
    if not frames:
        return 0
    df = pd.DataFrame(
        frames,
        columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades", "taker_buy_base",
            "taker_buy_quote", "ignore",
        ],
    )
    df = df.drop_duplicates(subset="open_time")
    # int64 explicitly: Windows numpy maps astype(int) to int32, overflowing ms epochs.
    idx = _naive_utc_index(df["open_time"].astype("int64").tolist())
    out = pd.DataFrame(
        {
            "open": pd.to_numeric(df["open"], errors="coerce").values,
            "high": pd.to_numeric(df["high"], errors="coerce").values,
            "low": pd.to_numeric(df["low"], errors="coerce").values,
            "close": pd.to_numeric(df["close"], errors="coerce").values,
            "volume": pd.to_numeric(df["quote_volume"], errors="coerce").values,
            "taker_buy_quote": pd.to_numeric(df["taker_buy_quote"], errors="coerce").values,
        },
        index=idx,
    ).sort_index()
    out.to_parquet(out_path)
    return len(out)


def fetch_funding(symbol: str, days: int, out_path: Path) -> int:
    start = _utc_ms(datetime.now(timezone.utc) - timedelta(days=days))
    records: List[Dict[str, Any]] = []
    cursor = start
    while True:
        batch = _get_json(
            f"{FAPI}/fapi/v1/fundingRate",
            {"symbol": symbol, "startTime": cursor, "limit": 1000},
        )
        if not batch:
            break
        records.extend(batch)
        cursor = int(batch[-1]["fundingTime"]) + 1
        time.sleep(BINANCE_PACE_SEC)
        if len(batch) < 1000:
            break
    if not records:
        return 0
    idx = _naive_utc_index([int(r["fundingTime"]) for r in records])
    out = pd.DataFrame(
        {"funding_rate": [float(r.get("fundingRate") or 0.0) for r in records]},
        index=idx,
    )
    out = out[~out.index.duplicated(keep="last")].sort_index()
    out.to_parquet(out_path)
    return len(out)


async def fetch_funding_coinglass(client: CoinglassClient, base: str, out_path: Path) -> int:
    """Fallback funding source when fapi funding endpoint is WAF-blocked."""
    manifest = COINGLASS_DATASET_MANIFESTS["funding_rate_history"]
    for attempt in range(6):
        try:
            resp = await client.request_dataset(
                manifest, symbol=base, exchange="Binance", interval="8h", limit=1000, manual=True
            )
            payload = resp.get("payload") or {}
            data = payload.get("data") if isinstance(payload, dict) else payload
            if not isinstance(data, list) or not data:
                return 0
            idx = _naive_utc_index([int(r["time"]) for r in data])
            out = pd.DataFrame(
                {"funding_rate": [float(r.get("close") or 0.0) for r in data]},
                index=idx,
            )
            out = out[~out.index.duplicated(keep="last")].sort_index()
            out.to_parquet(out_path)
            return len(out)
        except CoinglassBudgetExceeded:
            await asyncio.sleep(20)
        except CoinglassError as exc:
            text = str(exc)
            if "budget_exhausted" in text:
                await asyncio.sleep(20)
            elif "429" in text or "backoff" in text:
                await asyncio.sleep(30)
            else:
                logger.warning(f"coinglass funding {base} failed: {text[:120]}")
                return -1
    return -1


async def fetch_oi(client: CoinglassClient, base: str, interval: str, out_path: Path) -> int:
    manifest = COINGLASS_DATASET_MANIFESTS["open_interest_history"]
    for attempt in range(6):
        try:
            resp = await client.request_dataset(
                manifest, symbol=base, exchange="Binance", interval=interval, limit=1000, manual=True
            )
            payload = resp.get("payload") or {}
            data = payload.get("data") if isinstance(payload, dict) else payload
            if not isinstance(data, list) or not data:
                return 0
            idx = _naive_utc_index([int(r["time"]) for r in data])
            out = pd.DataFrame(
                {
                    "oi_usd": [float(r.get("close") or 0.0) for r in data],
                    "oi_usd_high": [float(r.get("high") or 0.0) for r in data],
                    "oi_usd_low": [float(r.get("low") or 0.0) for r in data],
                },
                index=idx,
            )
            out = out[~out.index.duplicated(keep="last")].sort_index()
            out.to_parquet(out_path)
            return len(out)
        except CoinglassBudgetExceeded:
            logger.info("coinglass minute budget exhausted; sleeping 20s")
            await asyncio.sleep(20)
        except CoinglassError as exc:
            # request_dataset re-wraps budget/backoff failures into plain
            # CoinglassError strings, so match on text as well.
            text = str(exc)
            if "budget_exhausted" in text:
                logger.info("coinglass budget exhausted (wrapped); sleeping 20s")
                await asyncio.sleep(20)
            elif "429" in text or "backoff" in text:
                logger.info(f"coinglass backoff ({text[:60]}); sleeping 30s")
                await asyncio.sleep(30)
            else:
                logger.warning(f"coinglass OI {base} {interval} failed: {text[:120]}")
                return -1
    return -1


def _cg_base_symbol(base: str) -> str:
    out = base
    for prefix in ("1000000", "10000", "1000"):
        if out.startswith(prefix) and len(out) > len(prefix):
            out = out[len(prefix):]
            break
    return out.lower()


def build_cg_mapping(bases: List[str]) -> Dict[str, str]:
    coin_list = _get_json(f"{CG_API}/coins/list", {"include_platform": "false"})
    time.sleep(CG_PACE_SEC)
    wanted = {_cg_base_symbol(b): b for b in bases}
    candidates: Dict[str, List[str]] = {}
    for coin in coin_list:
        sym = str(coin.get("symbol") or "").lower()
        if sym in wanted:
            candidates.setdefault(sym, []).append(str(coin.get("id")))
    all_ids = [cid for ids in candidates.values() for cid in ids]
    mcap_by_id: Dict[str, float] = {}
    for i in range(0, len(all_ids), 150):
        chunk = all_ids[i : i + 150]
        markets = _get_json(
            f"{CG_API}/coins/markets",
            {"vs_currency": "usd", "ids": ",".join(chunk), "per_page": 250, "page": 1},
        )
        for m in markets:
            mcap_by_id[str(m.get("id"))] = float(m.get("market_cap") or 0.0)
        time.sleep(CG_PACE_SEC)
    mapping: Dict[str, str] = {}
    for sym, ids in candidates.items():
        best = max(ids, key=lambda cid: mcap_by_id.get(cid, 0.0))
        if mcap_by_id.get(best, 0.0) > 0:
            mapping[wanted[sym]] = best
    logger.info(f"coingecko mapping resolved {len(mapping)}/{len(bases)} bases")
    return mapping


def fetch_mcap(cg_id: str, days: int, out_path: Path) -> int:
    payload = _get_json(
        f"{CG_API}/coins/{cg_id}/market_chart",
        {"vs_currency": "usd", "days": str(min(days, 365)), "interval": "daily"},
        retries=5,
    )
    caps = payload.get("market_caps") or []
    prices = payload.get("prices") or []
    if not caps:
        return 0
    idx = _naive_utc_index([int(row[0]) for row in caps])
    out = pd.DataFrame({"mcap_usd": [float(row[1] or 0.0) for row in caps]}, index=idx)
    if prices and len(prices) == len(caps):
        out["cg_price"] = [float(row[1] or 0.0) for row in prices]
    out = out[~out.index.duplicated(keep="last")].sort_index()
    out.to_parquet(out_path)
    return len(out)


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=365)
    parser.add_argument("--max-symbols", type=int, default=110)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--skip-coinglass", action="store_true")
    parser.add_argument("--skip-coingecko", action="store_true")
    args = parser.parse_args()

    for sub in ("klines_1h", "funding", "oi_1d", "oi_4h", "mcap_1d"):
        (OUT_DIR / sub).mkdir(parents=True, exist_ok=True)

    universe = load_universe(args.days, args.max_symbols)
    manifest: Dict[str, Any] = {
        "built_at": datetime.now(timezone.utc).isoformat(),
        "days": args.days,
        "symbols": {},
    }

    for i, row in enumerate(universe):
        base, symbol = row["base"], row["symbol"]
        entry: Dict[str, Any] = dict(row)
        kl_path = OUT_DIR / "klines_1h" / f"{base}.parquet"
        fu_path = OUT_DIR / "funding" / f"{base}.parquet"
        try:
            if args.refresh or not kl_path.exists():
                entry["klines_1h"] = fetch_klines_1h(symbol, args.days, kl_path)
            else:
                entry["klines_1h"] = "cached"
            if args.refresh or not fu_path.exists():
                entry["funding"] = fetch_funding(symbol, args.days, fu_path)
            else:
                entry["funding"] = "cached"
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"binance fetch failed for {symbol}: {exc}")
            entry["binance_error"] = str(exc)[:200]
        manifest["symbols"][base] = entry
        if i % 10 == 0:
            logger.info(f"binance leg {i + 1}/{len(universe)} ({base})")

    if not args.skip_coinglass:
        async with CoinglassClient() as client:
            for i, row in enumerate(universe):
                base = row["base"]
                entry = manifest["symbols"][base]
                for interval, sub in (("1d", "oi_1d"), ("4h", "oi_4h")):
                    path = OUT_DIR / sub / f"{base}.parquet"
                    if not args.refresh and path.exists():
                        entry[sub] = "cached"
                        continue
                    entry[sub] = await fetch_oi(client, base, interval, path)
                    await asyncio.sleep(COINGLASS_PACE_SEC)
                if i % 10 == 0:
                    logger.info(f"coinglass leg {i + 1}/{len(universe)} ({base})")
            for i, row in enumerate(universe):
                base = row["base"]
                fu_path = OUT_DIR / "funding" / f"{base}.parquet"
                if fu_path.exists():
                    continue
                manifest["symbols"][base]["funding_coinglass"] = await fetch_funding_coinglass(
                    client, base, fu_path
                )
                logger.info(f"coinglass funding fallback ({base})")
                await asyncio.sleep(COINGLASS_PACE_SEC)

    if not args.skip_coingecko:
        pending = [
            row["base"]
            for row in universe
            if args.refresh or not (OUT_DIR / "mcap_1d" / f"{row['base']}.parquet").exists()
        ]
        mapping = build_cg_mapping(pending) if pending else {}
        manifest["coingecko_mapping"] = mapping
        for i, base in enumerate(pending):
            entry = manifest["symbols"][base]
            cg_id = mapping.get(base)
            if not cg_id:
                entry["mcap_1d"] = "unmapped"
                continue
            try:
                entry["mcap_1d"] = fetch_mcap(cg_id, args.days, OUT_DIR / "mcap_1d" / f"{base}.parquet")
                entry["cg_id"] = cg_id
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"coingecko mcap failed for {base} ({cg_id}): {exc}")
                entry["mcap_1d"] = "error"
            time.sleep(CG_PACE_SEC)
            if i % 10 == 0:
                logger.info(f"coingecko leg {i + 1}/{len(pending)} ({base})")

    (OUT_DIR / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    logger.info(f"dataset build complete -> {OUT_DIR / 'manifest.json'}")


if __name__ == "__main__":
    asyncio.run(main())
