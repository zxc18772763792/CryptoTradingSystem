"""Generate the weekly pump-precursor watchlist (model-ranked Top-N).

Fetches per-symbol daily history (Binance fapi, free) + daily OI (Coinglass,
budget-paced) + current mcap (coins-markets snapshot), builds the SHARED
feature set (core/research/pump_precursor.py), scores the universe
cross-sectionally and writes:

  data/research/pump_watchlist/watchlist_<date>.json
  data/research/pump_watchlist/latest.json   (atomic copy, served by the API)

观察名单性质：验证过的相对提升（命中率 ~2.7×），非交易信号；纪律见
docs/AMBUSH_MODES_BACKTEST_REPORT_2026-07-18.md 第 9 节。
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from datetime import datetime, timezone
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
from core.data.coinglass_altcoin import load_coinglass_market_snapshots  # noqa: E402
from core.research.pump_precursor import (  # noqa: E402
    latest_feature_row,
    load_model_weights,
    score_universe,
    top_feature_drivers,
)

FAPI = "https://fapi.binance.com"
OUT_DIR = PROJECT_ROOT / "data" / "research" / "pump_watchlist"
SESSION = requests.Session()
SESSION.headers["User-Agent"] = "crypto-trading-system-watchlist/1.0"

MAJOR_BASES = {"BTC", "ETH", "BNB", "SOL", "XRP", "DOGE", "ADA", "TRX", "TON"}
NON_ALT_BASES = {"USDT", "USDC", "FDUSD", "BUSD", "TUSD", "USDE", "USDS", "DAI", "PAXG", "XAUT"}
MCAP_MIN, MCAP_MAX = 2e6, 1.5e9
HISTORY_DAYS = 365
COINGLASS_PACE_SEC = 6.8
BINANCE_PACE_SEC = 0.25


_ONCHAIN_DIR = PROJECT_ROOT / "data" / "research" / "onchain"


def _fetch_top_onchain(
    bases: List[str],
    mcap_by_base: Dict[str, float],
    price_by_base: Dict[str, float],
) -> tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """Fetch holder concentration (GeckoTerminal) + unlock proximity (DefiLlama)
    for a specific list of bases (this run's top-N). Reuses the snapshot module's
    per-coin fetchers so the logic stays single-sourced. Best-effort: any coin
    without a resolvable DEX contract / holder data / unlock schedule is omitted.
    """
    holders: Dict[str, Dict[str, Any]] = {}
    unlocks: Dict[str, Dict[str, Any]] = {}
    try:
        from scripts.snapshot_onchain_features import (  # noqa: PLC0415
            CG_API,
            CHAIN_MAP,
            _get_json,
            fetch_holder_row,
            fetch_unlock_features,
            load_unlock_slugs,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"onchain enrichment unavailable: {exc}")
        return holders, unlocks

    want = {b.upper() for b in bases}
    # symbol -> platforms (pick an entry that carries a contract on a known chain)
    sym_platforms: Dict[str, Dict[str, str]] = {}
    try:
        listing = _get_json(f"{CG_API}/coins/list", {"include_platform": "true"})
        known = {cg for cg, _ in CHAIN_MAP}
        for coin in listing:
            sym = str(coin.get("symbol") or "").upper()
            if sym not in want or sym in sym_platforms:
                continue
            plats = {k: v for k, v in (coin.get("platforms") or {}).items() if v}
            if plats and (set(plats) & known):
                sym_platforms[sym] = plats
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"coingecko platform lookup failed: {exc}")

    slugs = load_unlock_slugs()
    for base in bases:
        plats = sym_platforms.get(base.upper())
        if plats:
            try:
                row = fetch_holder_row(plats)
                if row:
                    holders[base] = row
            except Exception:  # noqa: BLE001
                pass
        slug = slugs.get(base)
        if slug:
            try:
                row = fetch_unlock_features(slug, mcap_by_base.get(base), price_by_base.get(base))
                if row:
                    unlocks[base] = row
            except Exception:  # noqa: BLE001
                pass
    logger.info(f"onchain top-N enrichment: holders {len(holders)}/{len(bases)}, unlocks {len(unlocks)}/{len(bases)}")
    return holders, unlocks


def _load_onchain_snapshot(kind: str) -> Dict[str, Dict[str, Any]]:
    """Read the latest holder/unlock snapshot (context columns; may be absent)."""
    path = _ONCHAIN_DIR / kind / "latest.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {}
    return dict(payload.get("rows") or {})


def _get_json(url: str, params: Optional[Dict[str, Any]] = None, *, retries: int = 4) -> Any:
    last: Optional[Exception] = None
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, params=params, timeout=25)
            if resp.status_code in (403, 429):
                wait = 20.0 * (attempt + 1)
                logger.warning(f"{resp.status_code} from fapi, sleeping {wait:.0f}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(2.0 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last}")


def load_universe(max_symbols: int, mcap_by_base: Dict[str, float]) -> List[Dict[str, Any]]:
    info = _get_json(f"{FAPI}/fapi/v1/exchangeInfo")
    tickers = {t["symbol"]: t for t in _get_json(f"{FAPI}/fapi/v1/ticker/24hr")}
    rows = []
    for s in info.get("symbols", []):
        if s.get("contractType") != "PERPETUAL" or s.get("quoteAsset") != "USDT" or s.get("status") != "TRADING":
            continue
        base = s["baseAsset"]
        if base in MAJOR_BASES or base in NON_ALT_BASES:
            continue
        mcap = mcap_by_base.get(base)
        if mcap is not None and not (MCAP_MIN <= mcap <= MCAP_MAX):
            continue
        rows.append(
            {
                "base": base,
                "symbol": s["symbol"],
                "quote_volume_24h": float((tickers.get(s["symbol"]) or {}).get("quoteVolume") or 0.0),
            }
        )
    rows.sort(key=lambda r: r["quote_volume_24h"], reverse=True)
    return rows[:max_symbols]


def fetch_daily_klines(symbol: str) -> Optional[pd.DataFrame]:
    batch = _get_json(
        f"{FAPI}/fapi/v1/klines",
        {"symbol": symbol, "interval": "1d", "limit": min(HISTORY_DAYS + 10, 499)},
    )
    if not batch or len(batch) < 35:
        return None
    idx = pd.to_datetime([int(r[0]) for r in batch], unit="ms", utc=True).tz_localize(None)
    frame = pd.DataFrame(
        {
            "close": [float(r[4]) for r in batch],
            "volume": [float(r[7]) for r in batch],
        },
        index=idx,
    )
    return frame.iloc[:-1]  # drop the in-progress day


def fetch_funding_daily(symbol: str) -> Optional[pd.Series]:
    batch = _get_json(f"{FAPI}/fapi/v1/fundingRate", {"symbol": symbol, "limit": 60})
    if not batch:
        return None
    idx = pd.to_datetime([int(r["fundingTime"]) for r in batch], unit="ms", utc=True).tz_localize(None)
    series = pd.Series([float(r.get("fundingRate") or 0.0) for r in batch], index=idx)
    return series.resample("1D").mean()


async def fetch_oi_daily(client: CoinglassClient, base: str) -> Optional[pd.Series]:
    manifest = COINGLASS_DATASET_MANIFESTS["open_interest_history"]
    for _ in range(6):
        try:
            resp = await client.request_dataset(
                manifest, symbol=base, exchange="Binance", interval="1d", limit=400, manual=True
            )
            data = (resp.get("payload") or {}).get("data") or []
            if not data:
                return None
            idx = pd.to_datetime([int(r["time"]) for r in data], unit="ms", utc=True).tz_localize(None)
            return pd.Series([float(r.get("close") or 0.0) for r in data], index=idx)
        except CoinglassBudgetExceeded:
            await asyncio.sleep(20)
        except CoinglassError as exc:
            # request_dataset re-wraps per-route failures (including budget
            # exhaustion) into CoinglassError strings — match on text.
            text = str(exc)
            if "budget_exhausted" in text:
                await asyncio.sleep(20)
            elif "429" in text or "backoff" in text:
                await asyncio.sleep(30)
            else:
                logger.warning(f"oi fetch failed {base}: {text[:100]}")
                return None
    return None


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--top", type=int, default=15)
    parser.add_argument("--max-symbols", type=int, default=130)
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    model = load_model_weights()

    snapshots = await load_coinglass_market_snapshots("binance", manual=True)
    mcap_by_base: Dict[str, float] = {}
    price_by_base: Dict[str, float] = {}
    for sym, row in snapshots.items():
        base = sym.split("/")[0]
        try:
            mcap = float(row.get("market_cap_usd"))
            if mcap > 0:
                mcap_by_base[base] = mcap
        except Exception:
            pass
        try:
            price = float(row.get("current_price") or row.get("price") or 0.0)
            if price > 0:
                price_by_base[base] = price
        except Exception:
            pass

    universe = load_universe(args.max_symbols, mcap_by_base)
    logger.info(f"watchlist universe: {len(universe)} symbols")

    feature_rows: Dict[str, Dict[str, float]] = {}
    skipped: Dict[str, str] = {}
    async with CoinglassClient() as client:
        for i, row in enumerate(universe):
            base, symbol = row["base"], row["symbol"]
            mcap = mcap_by_base.get(base)
            if not mcap:
                skipped[base] = "no_mcap"
                continue
            try:
                klines = fetch_daily_klines(symbol)
                time.sleep(BINANCE_PACE_SEC)
                funding = fetch_funding_daily(symbol)
                time.sleep(BINANCE_PACE_SEC)
            except Exception as exc:  # noqa: BLE001
                skipped[base] = f"binance:{str(exc)[:60]}"
                continue
            if klines is None or len(klines) < 35:
                skipped[base] = "short_history"
                continue
            oi = await fetch_oi_daily(client, base)
            await asyncio.sleep(COINGLASS_PACE_SEC)
            if oi is None or len(oi) < 32:
                skipped[base] = "no_oi"
                continue
            daily = klines.tail(HISTORY_DAYS).copy()
            daily["oi"] = oi.reindex(daily.index, method="ffill")
            daily["mcap"] = float(mcap)
            daily["funding"] = funding.reindex(daily.index).ffill(limit=3) if funding is not None else float("nan")
            features = latest_feature_row(daily)
            if features is None:
                skipped[base] = "feature_nan"
                continue
            feature_rows[base] = features
            if i % 10 == 0:
                logger.info(f"features {i + 1}/{len(universe)} ({base})")

    scored = score_universe(feature_rows, model)
    logger.info(f"scored {len(scored)} symbols, skipped {len(skipped)}")

    # On-chain display columns (context only, not scored): fetch holder
    # concentration + unlock proximity for THIS run's own top-N coins directly,
    # rather than a separately-scoped weekly snapshot (which drifts off the live
    # watchlist universe and leaves the columns blank). Brand-new coins without a
    # DEX contract / GeckoTerminal holder data / unlock schedule stay blank —
    # that is a real coverage limit, not a bug.
    top_bases = [b for b, _ in scored.head(args.top).iterrows()]
    holder_rows, unlock_rows = _fetch_top_onchain(top_bases, mcap_by_base, price_by_base)

    entries = []
    for rank, (base, row) in enumerate(scored.head(args.top).iterrows(), start=1):
        holder = holder_rows.get(base) or {}
        unlock = unlock_rows.get(base) or {}
        entries.append(
            {
                "rank": rank,
                "base": base,
                "symbol": f"{base}/USDT",
                "score": round(float(row["score"]), 4),
                "drivers": top_feature_drivers(row, model),
                "mcap_usd": mcap_by_base.get(base),
                "oi_mcap": round(float(row["oi_mcap"]), 4),
                "vola_30d": round(float(row["vola_30d"]), 4),
                "ret_30d": round(float(row["ret_30d"]), 4),
                "funding_7d": round(float(row["funding_7d"]), 6),
                "dd_from_ath": round(float(row["dd_from_ath"]), 4),
                "pumped_before_120d": bool(row["pumped_before_120d"] > 0),
                "top10_holder_pct": holder.get("top10_pct"),
                "unlock_next_30d_pct": unlock.get("unlock_next_30d_pct_mcap"),
                "days_to_next_unlock": unlock.get("days_to_next_unlock"),
            }
        )

    generated_at = datetime.now(timezone.utc)
    payload = {
        "version": 1,
        "generated_at": generated_at.isoformat(),
        "model": {
            "trained_at": model.get("trained_at"),
            "train_end": model.get("train_end"),
            "label": model.get("label"),
        },
        "universe_size": len(universe),
        "scored": len(scored),
        "skipped": skipped,
        "top": entries,
        "full_ranking": [
            {"base": b, "score": round(float(r["score"]), 4)} for b, r in scored.iterrows()
        ],
        "disclaimer": "研究观察名单：验证的是相对命中率提升（~2.7×），非交易信号；现货/无杠杆/无止损/阶梯止盈纪律见回测报告第9节。",
    }

    dated = OUT_DIR / f"watchlist_{generated_at:%Y-%m-%d}.json"
    dated.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp = OUT_DIR / "latest.json.tmp"
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(OUT_DIR / "latest.json")
    logger.info(f"watchlist written -> {dated}")
    for e in entries:
        logger.info(f"  #{e['rank']:>2} {e['base']:<10} score={e['score']:.3f} drivers={','.join(e['drivers'])}")


if __name__ == "__main__":
    asyncio.run(main())
