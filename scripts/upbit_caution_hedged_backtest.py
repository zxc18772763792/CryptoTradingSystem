"""Hedged Upbit caution short: short the coin, long a liquid-perp basket (2026-10-04).

Why: the plain 7-day short after an Upbit caution designation earned +7.5%
per trade over 31 trades, but 22 of them fell in down-market weeks. In
up-market weeks the short lost 0.9% even though the coin still lagged the
market by ~2 points, and the first live-era backfills (Sept 2026 alt rally)
lost 4 of 5. The robust part is the RELATIVE move: the coin beat the market
in only 6 of 31 events. This backtest isolates it.

Frozen rule (core/research/upbit_caution_tracker.py strategy
"caution_hedged" trades exactly this):
  * coin leg = the existing caution short (scripts/upbit_short_backtest.py):
    entry at the close of the notice's UTC day, exit at the close 7 days
    later, +40% intraday stop filled 2% worse, 0.1% fees, real funding
  * hedge leg = long, same notional, an equal-weight basket of the 30
    USDT perps with the highest quote volume on the notice day (listed
    before it, coin and stablecoins excluded); entry/exit at the same
    closes; when the coin leg stops, the basket closes at that day's close
  * basket costs: 0.1% round trip and the funding longs pay
  * hedged return = coin short return + basket return - basket costs

Historical basket ranking uses today's perp list (delisted perps cannot be
fetched): a mild survivorship in the hedge leg, none in the coin leg.

  python scripts/upbit_caution_hedged_backtest.py
"""
from __future__ import annotations

import json
import sys
import time
from math import comb
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.upbit_short_backtest import DAY_MS, FEE, HOLD, SLIP, STOP, score_short_trade  # noqa: E402

OUT = ROOT / "data" / "research" / "upbit"
FAPI = "https://fapi.binance.com/fapi/v1"
DAILY_CACHE = OUT / "perp_daily_cache.json"
FUNDING_CACHE = OUT / "basket_funding_cache.json"
BASKET_SIZE = 30
BASKET_FEE = 0.001
HISTORY_START_MS = int(pd.Timestamp("2022-09-01", tz="UTC").timestamp() * 1000)
STABLE = {"USDC", "FDUSD", "TUSD", "DAI", "USDP", "BUSD", "USDE", "USD1", "BFUSD", "EUR", "PYUSD", "RLUSD"}


def coin_exit_day(kl: List[list]) -> int:
    """Index (1..HOLD) of the bar on which the coin leg closes: the stop bar, else HOLD."""
    entry = float(kl[0][4])
    for k in range(1, HOLD + 1):
        if float(kl[k][2]) >= entry * (1 + STOP):
            return k
    return HOLD


def pick_basket(daily: Dict[str, dict], onboard: Dict[str, int], day0_ms: int, exclude: str) -> List[str]:
    ranked = []
    for symbol, bars in daily.items():
        if symbol == exclude or symbol[:-4] in STABLE or onboard.get(symbol, 0) >= day0_ms:
            continue
        bar = bars.get(str(day0_ms))
        if bar:
            ranked.append((bar[1], symbol))
    ranked.sort(reverse=True)
    return [s for _, s in ranked[:BASKET_SIZE]]


def basket_leg(daily: Dict[str, dict], funding: Dict[str, list], members: List[str], entry_ms: int,
               exit_ms: int) -> Optional[Tuple[float, float, int]]:
    """(mean price return, mean funding paid by longs, legs) from closes at entry_ms-1d and exit_ms-1d bars."""
    moves, fund = [], []
    for s in members:
        a, b = daily[s].get(str(entry_ms - DAY_MS)), daily[s].get(str(exit_ms - DAY_MS))
        if not a or not b:
            continue
        moves.append(b[0] / a[0] - 1.0)
        fund.append(sum(r for t, r in funding.get(s, []) if entry_ms < t <= exit_ms))
    if len(moves) * 2 < len(members):
        return None
    return float(np.mean(moves)), float(np.mean(fund)), len(moves)


def _fetch_daily(client, symbols: List[str]) -> Dict[str, dict]:
    cache = json.loads(DAILY_CACHE.read_text(encoding="utf-8")) if DAILY_CACHE.exists() else {}
    for i, symbol in enumerate(symbols):
        if symbol in cache:
            continue
        bars: Dict[str, list] = {}
        cursor = HISTORY_START_MS
        while True:
            resp = client.get(f"{FAPI}/klines", params={"symbol": symbol, "interval": "1d", "startTime": cursor, "limit": 1500})
            if resp.status_code != 200:
                break
            page = resp.json()
            for b in page:
                bars[str(int(b[0]))] = [float(b[4]), float(b[7])]  # close, quote volume
            if len(page) < 1500:
                break
            cursor = int(page[-1][0]) + DAY_MS
            time.sleep(0.25)
        cache[symbol] = bars
        time.sleep(0.25)
        if i % 50 == 0:
            print(f"  daily bars {i}/{len(symbols)}", flush=True)
    DAILY_CACHE.write_text(json.dumps(cache), encoding="utf-8")
    return cache


def _fetch_funding(client, symbols: List[str]) -> Dict[str, list]:
    cache = json.loads(FUNDING_CACHE.read_text(encoding="utf-8")) if FUNDING_CACHE.exists() else {}
    for symbol in symbols:
        if symbol in cache:
            continue
        rows: List[Tuple[int, float]] = []
        cursor = HISTORY_START_MS
        while cursor < time.time() * 1000:
            resp = client.get(f"{FAPI}/fundingRate", params={"symbol": symbol, "startTime": cursor, "limit": 1000})
            if resp.status_code != 200:
                break
            page = [(int(r["fundingTime"]), float(r["fundingRate"])) for r in resp.json()]
            if not page:
                break
            rows += page
            cursor = page[-1][0] + 1
            time.sleep(0.15)
        cache[symbol] = rows
    FUNDING_CACHE.write_text(json.dumps(cache), encoding="utf-8")
    return cache


def main() -> int:
    import httpx

    from config.settings import settings
    from core.utils.proxy_env import ensure_proxy_env

    ensure_proxy_env(settings)
    ev = pd.read_csv(OUT / "events.csv", parse_dates=["date"])
    ev = ev[ev.kind == "caution"].drop_duplicates(["token", "date"])
    store = json.loads((OUT / "perp_cache.json").read_text(encoding="utf-8"))
    with httpx.Client(timeout=30, trust_env=True) as client:
        info = client.get(f"{FAPI}/exchangeInfo").json()
        perps = {s["symbol"]: int(s.get("onboardDate") or 0) for s in info["symbols"]
                 if s.get("quoteAsset") == "USDT" and s.get("contractType") == "PERPETUAL"}
        daily = _fetch_daily(client, sorted(perps))
        rows, baskets = [], {}
        for _, e in ev.iterrows():
            d0 = e.date.tz_convert("UTC") if e.date.tzinfo else e.date.tz_localize("UTC")
            day0_ms = int(d0.timestamp() * 1000)
            sym = next((s for s in (f"{e.token}USDT", f"1000{e.token}USDT") if s in perps and perps[s] < day0_ms), None)
            key = f"v2|{sym}|{d0.date()}"
            if not sym or key not in store:
                continue
            kl, fr = store[key]["klines"], store[key]["funding"]
            try:
                coin_ret, _, stopped = score_short_trade(kl, fr, day0_ms)
            except ValueError:
                continue
            exit_ms = day0_ms + (coin_exit_day(kl) + 1) * DAY_MS
            baskets[(e.token, d0)] = (pick_basket(daily, perps, day0_ms, sym), day0_ms + DAY_MS, exit_ms, coin_ret, stopped)
        funding = _fetch_funding(client, sorted({s for b in baskets.values() for s in b[0]}))
    for (token, d0), (members, entry_ms, exit_ms, coin_ret, stopped) in baskets.items():
        leg = basket_leg(daily, funding, members, entry_ms, exit_ms)
        if leg is None:
            continue
        b_ret, b_fund, n = leg
        rows.append({"token": token, "date": d0, "coin_short_ret": coin_ret, "basket_ret": b_ret, "basket_funding": b_fund,
                     "legs": n, "stopped": stopped, "hedged_ret": coin_ret + b_ret - b_fund - BASKET_FEE})
    r = pd.DataFrame(rows).sort_values("date")
    r.to_csv(OUT / "caution_hedged_backtest.csv", index=False)

    def stats(x: pd.Series, label: str) -> None:
        g = [gg.to_numpy() for _, gg in x.groupby(r.loc[x.index, "date"].dt.to_period("M"))]
        rng = np.random.default_rng(0)
        bs = [np.concatenate([g[i] for i in rng.integers(0, len(g), len(g))]).mean() for _ in range(4000)]
        k, n = int((x > 0).sum()), len(x)
        p = sum(comb(n, i) for i in range(k, n + 1)) / 2 ** n
        print(f"{label:22s} n={n} mean {x.mean() * 100:+.2f}% median {x.median() * 100:+.2f}% "
              f"90% CI [{np.percentile(bs, 5) * 100:+.2f}, {np.percentile(bs, 95) * 100:+.2f}] wins {k}/{n} (sign p={p:.4f}) "
              f"worst {x.min() * 100:+.1f}% sd {x.std() * 100:.1f}%")

    print(f"caution events with a perp and a complete basket: {len(r)}  {r.date.min():%Y-%m-%d}..{r.date.max():%Y-%m-%d}")
    stats(r.coin_short_ret, "unhedged short")
    stats(r.hedged_ret, "hedged (rule)")
    up = r.basket_ret > 0
    for label, part in (("basket up", r[up]), ("basket down", r[~up])):
        print(f"  {label:12s} n={len(part)}: basket {part.basket_ret.mean() * 100:+.2f}%  unhedged {part.coin_short_ret.mean() * 100:+.2f}%  "
              f"hedged {part.hedged_ret.mean() * 100:+.2f}%")
    print(f"  basket funding paid by the long leg: {r.basket_funding.mean() * 100:+.3f}% per trade; legs median {r.legs.median():.0f}")
    for year, part in r.groupby(r.date.dt.year):
        print(f"  {year}: n={len(part)} unhedged {part.coin_short_ret.mean() * 100:+.2f}%  hedged {part.hedged_ret.mean() * 100:+.2f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
