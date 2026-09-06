"""Durable weekly snapshot of Binance Alpha token fundamentals (point-in-time).

WHY THIS EXISTS (2026-09-06 feasibility finding):
A cross-sectional "which new Alpha coin will take off" model is the only honest
way to cover the new-coin blind spot the perp model cannot see. But it CANNOT be
backtested today: the Alpha collector keeps only the current catalog (the raw
token_snapshots.jsonl rotates within days), so there is ZERO historical
fundamental time series. Price-only post-listing prediction was tested and is
dead (early-momentum -> takeoff spearman 0.12, no tercile separation). The
fundamental features that MIGHT carry signal — holder concentration, liquidity,
FDV, listing recency, chain, directory score — have no history.

This writer starts the clock: once per run it appends a COMPACT one-row-per-token
point-in-time snapshot to data/research/alpha_fundamentals/<date>.parquet
(append-only, never rotated). After ~8-12 weekly snapshots there is a real
panel to run the same rigorous cross-sectional + adversarial-verification
protocol used on the perp model. Reads the on-disk catalog (no network) to avoid
the degraded live collector's timeouts.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from loguru import logger

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

CATALOG = PROJECT_ROOT / "data" / "research" / "binance_alpha" / "token_catalog.json"
OUT_DIR = PROJECT_ROOT / "data" / "research" / "alpha_fundamentals"


def _f(v: Any) -> Optional[float]:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if x == x else None  # drop NaN


def _i(v: Any) -> Optional[int]:
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def main() -> None:
    if not CATALOG.exists():
        logger.error(f"Alpha catalog not found: {CATALOG} (is the collector running?)")
        raise SystemExit(1)
    payload = json.loads(CATALOG.read_text(encoding="utf-8"))
    tokens = payload.get("tokens") or []
    if not tokens:
        logger.error("Alpha catalog has no tokens")
        raise SystemExit(1)

    now = datetime.now(timezone.utc)
    now_ms = now.timestamp() * 1000.0
    rows: List[Dict[str, Any]] = []
    for t in tokens:
        alpha_id = str(t.get("alphaId") or "").strip()
        if not alpha_id:
            continue
        listing_ms = _f(t.get("listingTime"))
        listing_age_days = round((now_ms - listing_ms) / 86_400_000.0, 2) if listing_ms else None
        mcap = _f(t.get("marketCap"))
        fdv = _f(t.get("fdv"))
        rows.append(
            {
                "snapshot_at": now.isoformat(),
                "alpha_id": alpha_id,
                "symbol": str(t.get("symbol") or ""),
                "name": str(t.get("name") or ""),
                "chain": str(t.get("chainName") or ""),
                "listing_time_ms": _i(listing_ms) if listing_ms else None,
                "listing_age_days": listing_age_days,
                "price": _f(t.get("price")),
                "market_cap": mcap,
                "fdv": fdv,
                "circ_over_fdv": round(mcap / fdv, 4) if (mcap and fdv and fdv > 0) else None,
                "liquidity": _f(t.get("liquidity")),
                "holders": _i(t.get("holders")),
                "circ_supply": _f(t.get("circulatingSupply")),
                "total_supply": _f(t.get("totalSupply")),
                "volume_24h": _f(t.get("volume24h")),
                "pct_change_24h": _f(t.get("percentChange24h")),
                "trade_count_24h": _i(t.get("count24h")),
                "directory_score": _f(t.get("score")),
                "hot_tag": bool(t.get("hotTag")),
                "online_tge": bool(t.get("onlineTge")),
                "online_airdrop": bool(t.get("onlineAirdrop")),
                "fully_delisted": bool(t.get("fullyDelisted")),
                "offline": bool(t.get("offline")),
            }
        )

    df = pd.DataFrame(rows)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    dated = OUT_DIR / f"{now:%Y-%m-%d}.parquet"
    # Idempotent per day: overwrite same-day file, never touch prior days.
    df.to_parquet(dated, index=False)

    history = sorted(OUT_DIR.glob("*.parquet"))
    live = df[~df["fully_delisted"]]
    logger.info(
        f"alpha fundamentals snapshot -> {dated.name}: {len(df)} tokens "
        f"({len(live)} live, {len(df) - len(live)} delisted) | history files: {len(history)}"
    )
    if len(history) < 8:
        logger.info(
            f"panel not yet evaluable ({len(history)}/~8-12 weekly snapshots); "
            "keep collecting before any cross-sectional takeoff study"
        )


if __name__ == "__main__":
    main()
