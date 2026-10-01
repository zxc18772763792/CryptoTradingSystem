"""Backtest of the frozen daily cross-sectional reversal rule (paper tracker reference).

Origin (2026-10-01, scripts/ml_timeframe_study.py): pooled XGB models across
5m..1d bars found no edge after costs, except 1h bars ranked for a 24h hold,
where a single feature did as well as the model: coins trading BELOW their
slow EMA outperformed the cross-section over the next day (short-term
reversal). Walk-forward 2025-07..2026-09: decile spread +0.52%/day, 13/15
months positive - but picked as the best of 9 timeframe/horizon cells.

Frozen rule (core/research/xs_reversal_tracker.py trades exactly this):
  * universe: coins with a Binance USDT perpetual and >= 120 days of local 1h bars
  * at 00:00 UTC: signal = EMA(21) of 1h closes / last close - 1, using bars
    that closed by 00:00 (the 23:00 bar is the last one)
  * long the top decile (furthest BELOW their EMA), short the bottom decile
    (furthest ABOVE); equal weight; >= 30 coins needed, decile = n // 10
  * hold 24h: entry = close of the 23:00 bar, exit = close of the next 23:00 bar
  * costs: 0.15% round trip per position; perp funding paid by longs and
    received by shorts for settlements in (entry, exit]
  * reported unit: net return per position per day = (long leg - short leg)/2
    - 0.15% - funding, the same unit the tracker's retirement rule uses

Prices are the local 1h store (what the study used); funding comes from
Binance fapi and is cached in data/research/xs_reversal/funding_cache.json.

  python scripts/xs_reversal_backtest.py
"""
from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.research import xs_reversal_tracker as rule  # noqa: E402

OUT = ROOT / "data" / "research" / "xs_reversal"
FUNDING_CACHE = OUT / "funding_cache.json"
MIN_DAYS = 120


async def load_local_1h() -> dict:
    from scripts import train_ml_signal as trainer

    frames = {}
    for symbol in trainer._local_symbols("binance"):
        df = await trainer.load_ohlcv("binance", symbol, "1h", 3000)
        if df is None or len(df) < MIN_DAYS * 24:
            continue
        df = df[["close"]].astype(float)
        df.index = pd.to_datetime(df.index, utc=True)
        frames[symbol.split("/")[0]] = df[~df.index.duplicated(keep="last")].sort_index()["close"]
    return frames


def fetch_funding(perps: dict, start_ms: int) -> dict:
    import httpx

    cache = json.loads(FUNDING_CACHE.read_text(encoding="utf-8")) if FUNDING_CACHE.exists() else {}
    with httpx.Client(timeout=30, trust_env=True) as client:
        for base, symbol in perps.items():
            rows = cache.get(symbol) or []
            cursor = max([start_ms] + [int(r[0]) + 1 for r in rows])
            while cursor < time.time() * 1000 - 3_600_000:
                resp = client.get(f"{rule.FAPI}/fundingRate", params={"symbol": symbol, "startTime": cursor, "limit": 1000})
                if resp.status_code != 200:
                    break
                page = [(int(r["fundingTime"]), float(r["fundingRate"])) for r in resp.json()]
                if not page:
                    break
                rows += page
                cursor = page[-1][0] + 1
                time.sleep(0.15)
            cache[symbol] = rows
    OUT.mkdir(parents=True, exist_ok=True)
    FUNDING_CACHE.write_text(json.dumps(cache), encoding="utf-8")
    return cache


def main() -> int:
    import httpx

    from config.settings import settings
    from core.utils.proxy_env import ensure_proxy_env

    ensure_proxy_env(settings)
    closes = asyncio.run(load_local_1h())
    with httpx.Client(timeout=30, trust_env=True) as client:
        info = client.get(f"{rule.FAPI}/exchangeInfo").json()
    perp_of = rule.perp_symbols(info)
    closes = {b: s for b, s in closes.items() if b in perp_of}
    print(f"universe: {len(closes)} coins with a USDT perp and >= {MIN_DAYS}d of 1h bars")
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "universe.json").write_text(json.dumps({
        "bases": sorted(closes), "created_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "rule": f"local 1h history >= {MIN_DAYS}d and a TRADING USDT perp",
    }, indent=1), encoding="utf-8")
    panel = pd.DataFrame(closes).sort_index()
    funding = fetch_funding({b: perp_of[b] for b in closes}, int(panel.index.min().timestamp() * 1000))

    days = []
    for entry_ts in panel.index[(panel.index.hour == 23)]:
        exit_ts = entry_ts + pd.Timedelta(days=1)
        if exit_ts not in panel.index:
            continue
        history = panel.loc[:entry_ts].tail(rule.SIGNAL_BARS)
        signal = rule.reversal_signal(history)
        entry, exit_ = panel.loc[entry_ts], panel.loc[exit_ts]
        valid = signal.dropna().index.intersection(entry.dropna().index).intersection(exit_.dropna().index)
        legs = rule.pick_legs(signal.loc[valid])
        if legs is None:
            continue
        longs, shorts = legs
        lo_ms, hi_ms = int(entry_ts.timestamp() * 1000) + 3_600_000, int(exit_ts.timestamp() * 1000) + 3_600_000

        def fund(base):
            return sum(r for t, r in funding.get(perp_of[base], []) if lo_ms < t <= hi_ms)

        ret = exit_ / entry - 1.0
        long_leg = float(np.mean([ret[b] - fund(b) for b in longs]))
        short_leg = float(np.mean([-ret[b] + fund(b) for b in shorts]))
        days.append({"date": (entry_ts + pd.Timedelta(hours=1)).date(), "n": len(valid), "per_side": len(longs),
                     "long_leg": long_leg, "short_leg": short_leg,
                     "gross_spread": float(np.mean([ret[b] for b in longs]) - np.mean([ret[b] for b in shorts])),
                     "net_per_position": (long_leg + short_leg) / 2 - rule.ROUND_TRIP_COST})
    d = pd.DataFrame(days).set_index("date")
    d.index = pd.to_datetime(d.index)
    d.to_csv(OUT / "backtest_daily.csv")

    weeks = [g["net_per_position"].to_numpy() for _, g in d.groupby(d.index.to_period("W"))]
    rng = np.random.default_rng(0)
    boots = [np.concatenate([weeks[i] for i in rng.integers(0, len(weeks), len(weeks))]).mean() for _ in range(2000)]
    monthly = d["net_per_position"].groupby(d.index.to_period("M")).mean()
    print(f"days {len(d)}  {d.index.min():%Y-%m-%d}..{d.index.max():%Y-%m-%d}  coins/day median {d.n.median():.0f}, per side {d.per_side.median():.0f}")
    print(f"gross decile spread {d.gross_spread.mean() * 100:+.3f}%/day")
    print(f"net per position {d.net_per_position.mean() * 100:+.3f}%/day  90% CI (week bootstrap) "
          f"[{np.percentile(boots, 5) * 100:+.3f}, {np.percentile(boots, 95) * 100:+.3f}]  sd {d.net_per_position.std() * 100:.2f}%")
    print(f"long leg {d.long_leg.mean() * 100:+.3f}%  short leg {d.short_leg.mean() * 100:+.3f}% (each after funding, before cost)")
    print(f"months positive {int((monthly > 0).sum())}/{len(monthly)}")
    print((monthly * 100).round(3).to_string())
    for label, part in d.groupby(d.index.to_period("Q")):
        print(f"  {label}: {part.net_per_position.mean() * 100:+.3f}% over {len(part)} days")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
