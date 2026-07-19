"""Weekly on-chain feature snapshot: holder concentration + unlock calendar.

Zero-key data stack validated in docs/ONCHAIN_DATA_SOURCE_RESEARCH_2026-07-19.md:
  - GeckoTerminal token info  -> top10 holder share, holder count, dev share,
    mint/freeze authority, honeypot flag (multichain: sol/bsc/eth/base/...)
  - DefiLlama datasets bucket -> vesting schedules -> unlock_next_7d/30d as %
    of market cap, days to next cliff (full-float coins legitimately 0)
  - CoinGecko platforms list  -> base -> chain -> contract mapping (7d cache)

Persists data/research/onchain/{holder_snapshots,unlocks}/<date>.json and
latest.json. Holder history is NOT purchasable cheaply — this script
manufactures it going forward; run weekly alongside the pump watchlist.
"""
from __future__ import annotations

import argparse
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

OUT_DIR = PROJECT_ROOT / "data" / "research" / "onchain"
AMBUSH_DIR = PROJECT_ROOT / "data" / "research" / "ambush_modes"
CG_API = "https://api.coingecko.com/api/v3"
GT_API = "https://api.geckoterminal.com/api/v2"
LLAMA_BUCKET = "https://defillama-datasets.llama.fi"

SESSION = requests.Session()
SESSION.headers["User-Agent"] = "crypto-trading-system-onchain/1.0"
GT_PACE_SEC = 2.2
PLATFORMS_CACHE_TTL_DAYS = 7
UNLOCK_CACHE_TTL_DAYS = 7

# CoinGecko platform key -> GeckoTerminal network id
CHAIN_MAP = [
    ("solana", "solana"),
    ("binance-smart-chain", "bsc"),
    ("ethereum", "eth"),
    ("base", "base"),
    ("arbitrum-one", "arbitrum"),
    ("optimistic-ethereum", "optimism"),
    ("sui", "sui-network"),
]


def _get_json(url: str, params: Optional[Dict[str, Any]] = None, *, retries: int = 3) -> Any:
    last: Optional[Exception] = None
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, params=params, timeout=20)
            if resp.status_code == 429:
                time.sleep(20.0 * (attempt + 1))
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(2.0 * (attempt + 1))
    raise RuntimeError(f"GET {url}: {last}")


def _load_cached(path: Path, ttl_days: float) -> Optional[Any]:
    if not path.exists():
        return None
    age_days = (time.time() - path.stat().st_mtime) / 86400.0
    if age_days > ttl_days:
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return None


def universe_bases() -> List[str]:
    manifest = json.loads((AMBUSH_DIR / "manifest.json").read_text(encoding="utf-8"))
    return sorted((manifest.get("symbols") or {}).keys())


def cg_mapping() -> Dict[str, str]:
    manifest = json.loads((AMBUSH_DIR / "manifest.json").read_text(encoding="utf-8"))
    mapping = dict(manifest.get("coingecko_mapping") or {})
    for base, meta in (manifest.get("symbols") or {}).items():
        if meta.get("cg_id"):
            mapping.setdefault(base, meta["cg_id"])
    return mapping


def load_platforms(mapping: Dict[str, str]) -> Dict[str, Dict[str, str]]:
    cache_path = OUT_DIR / "platforms_cache.json"
    cached = _load_cached(cache_path, PLATFORMS_CACHE_TTL_DAYS)
    if cached:
        return cached
    listing = _get_json(f"{CG_API}/coins/list", {"include_platform": "true"})
    by_id = {c["id"]: (c.get("platforms") or {}) for c in listing}
    out = {
        base: {k: v for k, v in (by_id.get(cg) or {}).items() if v}
        for base, cg in mapping.items()
    }
    cache_path.write_text(json.dumps(out, indent=1), encoding="utf-8")
    return out


def fetch_holder_row(platforms: Dict[str, str]) -> Optional[Dict[str, Any]]:
    for cg_chain, gt_net in CHAIN_MAP:
        address = platforms.get(cg_chain)
        if not address:
            continue
        try:
            payload = _get_json(f"{GT_API}/networks/{gt_net}/tokens/{address}/info")
        except Exception:  # noqa: BLE001
            time.sleep(GT_PACE_SEC)
            continue
        finally:
            time.sleep(GT_PACE_SEC)
        attrs = (payload.get("data") or {}).get("attributes") or {}
        holders = attrs.get("holders") or {}
        dist = holders.get("distribution_percentage") or {}
        if dist.get("top_10") is None:
            continue
        def _f(value: Any) -> Optional[float]:
            try:
                return float(value)
            except Exception:  # noqa: BLE001
                return None
        return {
            "chain": gt_net,
            "address": address,
            "holders_count": holders.get("count"),
            "top10_pct": _f(dist.get("top_10")),
            "top20_pct": (_f(dist.get("top_10")) or 0.0) + (_f(dist.get("11_20")) or 0.0),
            "top40_pct": (_f(dist.get("top_10")) or 0.0)
            + (_f(dist.get("11_20")) or 0.0)
            + (_f(dist.get("21_40")) or 0.0),
            "dev_pct": _f(attrs.get("developer_holding_percentage")),
            "mint_authority": attrs.get("mint_authority"),
            "freeze_authority": attrs.get("freeze_authority"),
            "is_honeypot": attrs.get("is_honeypot"),
            "gt_score": _f(attrs.get("gt_score")),
            "as_of": holders.get("last_updated"),
        }
    return None


def load_unlock_slugs() -> Dict[str, str]:
    # Runtime copy first; versioned seed in config/ as fallback (data/ is
    # gitignored, and this table is hand-curated — e.g. GRAM->ton removed).
    for path in (OUT_DIR / "unlock_slugs.json", PROJECT_ROOT / "config" / "onchain_unlock_slugs.json"):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    return {}


def fetch_unlock_features(slug: str, mcap_usd: Optional[float], price_usd: Optional[float]) -> Optional[Dict[str, Any]]:
    cache_path = OUT_DIR / "unlocks_cache" / f"{slug}.json"
    payload = _load_cached(cache_path, UNLOCK_CACHE_TTL_DAYS)
    if payload is None:
        try:
            payload = _get_json(f"{LLAMA_BUCKET}/emissions/{slug}")
        except Exception:  # noqa: BLE001
            return None
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(payload), encoding="utf-8")
        time.sleep(0.3)

    series: Dict[int, float] = {}
    for section in ("documentedData", "realTimeData"):
        for cat in ((payload.get(section) or {}).get("data") or []):
            for point in cat.get("data") or []:
                ts = int(point.get("timestamp") or 0)
                unlocked = float(point.get("unlocked") or 0.0)
                if ts > 0:
                    series[ts] = series.get(ts, 0.0) + unlocked
        if series:
            break
    if not series:
        return None
    now_ts = time.time()
    ordered = sorted(series.items())
    # cumulative schedule -> per-step increments
    increments: List[tuple] = []
    prev = None
    for ts, cum in ordered:
        if prev is not None and cum > prev:
            increments.append((ts, cum - prev))
        prev = cum if prev is None or cum > prev else prev
    next_7d = sum(amt for ts, amt in increments if now_ts < ts <= now_ts + 7 * 86400)
    next_30d = sum(amt for ts, amt in increments if now_ts < ts <= now_ts + 30 * 86400)
    future = [ts for ts, amt in increments if ts > now_ts and amt > 0]
    days_to_next = round((min(future) - now_ts) / 86400.0, 1) if future else None
    out = {
        "slug": slug,
        "unlock_next_7d_tokens": next_7d,
        "unlock_next_30d_tokens": next_30d,
        "days_to_next_unlock": days_to_next,
    }
    if mcap_usd and price_usd and mcap_usd > 0:
        out["unlock_next_7d_pct_mcap"] = round(next_7d * price_usd / mcap_usd, 5)
        out["unlock_next_30d_pct_mcap"] = round(next_30d * price_usd / mcap_usd, 5)
    return out


def current_mcap_price() -> Dict[str, Dict[str, float]]:
    path = AMBUSH_DIR / "current_mcap.json"
    mcaps = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    prices: Dict[str, float] = {}
    kl_dir = AMBUSH_DIR / "klines_1h"
    for base in mcaps:
        p = kl_dir / f"{base}.parquet"
        if p.exists():
            try:
                closes = pd.read_parquet(p, columns=["close"])["close"].dropna()
                if len(closes):
                    prices[base] = float(closes.iloc[-1])
            except Exception:  # noqa: BLE001
                pass
    return {base: {"mcap": float(mcaps[base]), "price": prices.get(base)} for base in mcaps}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-holders", action="store_true")
    parser.add_argument("--skip-unlocks", action="store_true")
    args = parser.parse_args()

    for sub in ("holder_snapshots", "unlocks"):
        (OUT_DIR / sub).mkdir(parents=True, exist_ok=True)

    bases = universe_bases()
    mapping = cg_mapping()
    platforms = load_platforms(mapping)
    market = current_mcap_price()
    today = datetime.now(timezone.utc)

    if not args.skip_holders:
        holder_rows: Dict[str, Any] = {}
        missing: List[str] = []
        for i, base in enumerate(bases):
            row = fetch_holder_row(platforms.get(base) or {})
            if row:
                holder_rows[base] = row
            else:
                missing.append(base)
            if i % 20 == 0:
                logger.info(f"holders {i + 1}/{len(bases)} (hits={len(holder_rows)})")
        payload = {
            "generated_at": today.isoformat(),
            "coverage": f"{len(holder_rows)}/{len(bases)}",
            "missing": missing,
            "rows": holder_rows,
        }
        dated = OUT_DIR / "holder_snapshots" / f"holders_{today:%Y-%m-%d}.json"
        dated.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
        (OUT_DIR / "holder_snapshots" / "latest.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8"
        )
        logger.info(f"holder snapshot -> {dated} ({payload['coverage']})")

    if not args.skip_unlocks:
        slugs = load_unlock_slugs()
        unlock_rows: Dict[str, Any] = {}
        for base in bases:
            slug = slugs.get(base)
            if not slug:
                continue
            m = market.get(base) or {}
            row = fetch_unlock_features(slug, m.get("mcap"), m.get("price"))
            if row:
                unlock_rows[base] = row
        payload = {
            "generated_at": today.isoformat(),
            "coverage": f"{len(unlock_rows)}/{len(slugs)} mapped ({len(bases)} universe)",
            "rows": unlock_rows,
        }
        dated = OUT_DIR / "unlocks" / f"unlocks_{today:%Y-%m-%d}.json"
        dated.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
        (OUT_DIR / "unlocks" / "latest.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8"
        )
        logger.info(f"unlock snapshot -> {dated} ({payload['coverage']})")


if __name__ == "__main__":
    main()
