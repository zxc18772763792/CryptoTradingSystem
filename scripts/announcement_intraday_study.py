"""Shorting announcements over minutes and hours instead of days (2026-10-06).

The daily studies enter at the close of the announcement's UTC day. For
Binance monitoring tags that close comes after a -13.8% announcement-day
move, so the obvious question is whether the move is capturable right after
publication. This measures the 1-minute perp path around the exact
publication time of:
  * upbit_caution     Upbit 거래 유의 종목 지정 (notices.json first_listed_at)
  * upbit_krw_listing Upbit KRW-market additions (same archive)
  * binance_monitor   Binance "Extend the Monitoring Tag to Include ..." (catalog 49)
  * binance_delist    Binance "Will Delist X on <date>" (catalog 161)
Only coins with a Binance USDT perp listed before the announcement.

For entry delays d (minutes after publication) and holds h:
  short return = (P[T+d] - P[T+d+h]) / P[T+d], P[x] = close of the 1m bar ending at x
  net = gross - 0.1% fees - 0.2% slippage (0.1% per side; these books are thin)
Also the move in the 15 minutes BEFORE publication (leak check). Statistics
are bootstrapped by announcement (one notice can tag many coins).

  python scripts/announcement_intraday_study.py
  python scripts/announcement_intraday_study.py --frozen   # the paper tracker's rules

Frozen rules (core/research/announcement_short_tracker.py): entry 2 minutes
after publication (a realistic fast-poll detection delay), monitoring tags
held 24h with a +50% catastrophe stop, delistings held 4h with a +30% stop,
stops on 1m highs filled 2% worse, net of 0.3% costs (funding excluded
here; the tracker books it). A +30% stop on monitoring tags was tested and
rejected: it cut the mean from +5.4% to +4.1% (spikes revert within the day).
"""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.research.exchange_notices import load_history, parse_monitoring_changes, parse_notice  # noqa: E402
from core.research.upbit_caution_tracker import caution_tickers, krw_listing_tickers  # noqa: E402

OUT = ROOT / "data" / "research" / "announcement_intraday"
CACHE = OUT / "klines_1m.json"
FAPI = "https://fapi.binance.com/fapi/v1"
MIN = 60_000
PRE = 15
DELAYS = (1, 2, 5, 15, 30, 60)
HOLDS = (5, 15, 60, 240, 1440)
COST = 0.001 + 0.002


def events() -> pd.DataFrame:
    rows = []
    for n in json.loads((ROOT / "data" / "research" / "upbit" / "notices.json").read_text(encoding="utf-8")):
        t = int(datetime.fromisoformat(n["first_listed_at"]).timestamp() * 1000)
        for kind, match in (("upbit_caution", caution_tickers), ("upbit_krw_listing", krw_listing_tickers)):
            for tok in match(str(n.get("title") or "")):
                rows.append({"kind": kind, "token": tok, "t_ms": t, "notice": f"upbit-{n['id']}"})
    cat49 = json.loads((ROOT / "data" / "research" / "binance_announcements" / "catalog49.json").read_text(encoding="utf-8"))
    history = load_history()
    seen = set()
    for a in cat49 + [x for cat in history.values() for x in cat]:
        if a.get("code") in seen:
            continue
        seen.add(a.get("code"))
        for tok in parse_monitoring_changes(a.get("title", ""))["added"]:
            rows.append({"kind": "binance_monitor", "token": tok, "t_ms": int(a["release_ms"]), "notice": a["code"]})
        n = parse_notice(a.get("title", ""))
        if n and n["kind"] == "delist":
            for tok in n["tokens"]:
                rows.append({"kind": "binance_delist", "token": tok, "t_ms": int(a["release_ms"]), "notice": a["code"]})
    return pd.DataFrame(rows).drop_duplicates(["kind", "token", "t_ms"])


def fetch(client, ev: pd.DataFrame, onboard: dict) -> dict:
    cache = json.loads(CACHE.read_text(encoding="utf-8")) if CACHE.exists() else {}
    for i, e in enumerate(ev.itertuples()):
        sym = next((s for s in (f"{e.token}USDT", f"1000{e.token}USDT") if 0 < onboard.get(s, 0) < e.t_ms - 86_400_000), None)
        if not sym:
            continue
        key = f"{sym}|{e.t_ms}"
        if key in cache:
            continue
        start = (e.t_ms // MIN) * MIN - PRE * MIN
        resp = client.get(f"{FAPI}/klines", params={"symbol": sym, "interval": "1m", "startTime": start, "limit": 1500})
        cache[key] = [[int(b[0]), float(b[4]), float(b[2])] for b in resp.json()] if resp.status_code == 200 else []
        time.sleep(0.3)
        if i % 50 == 0:
            CACHE.write_text(json.dumps(cache), encoding="utf-8")
    CACHE.write_text(json.dumps(cache), encoding="utf-8")
    return cache


def price_at(closes: dict, ms: int):
    """Close of the 1m bar that ends at ms (its open is ms - 1 min)."""
    return closes.get(ms - MIN)


def cluster_ci(values, clusters, reps=3000):
    frame = pd.DataFrame({"v": values, "c": clusters}).dropna()
    groups = [g["v"].to_numpy() for _, g in frame.groupby("c")]
    if len(groups) < 3:
        return np.nan, np.nan, len(groups)
    rng = np.random.default_rng(0)
    means = [np.concatenate([groups[i] for i in rng.integers(0, len(groups), len(groups))]).mean() for _ in range(reps)]
    return float(np.percentile(means, 5)), float(np.percentile(means, 95)), len(groups)


def main() -> int:
    import httpx

    from config.settings import settings
    from core.utils.proxy_env import ensure_proxy_env

    ensure_proxy_env(settings)
    OUT.mkdir(parents=True, exist_ok=True)
    ev = events()
    with httpx.Client(timeout=30, trust_env=True) as client:
        info = client.get(f"{FAPI}/exchangeInfo").json()
        onboard = {s["symbol"]: int(s.get("onboardDate") or 0) for s in info["symbols"]
                   if s.get("quoteAsset") == "USDT" and s.get("contractType") == "PERPETUAL"}
        cache = fetch(client, ev, onboard)

    rows = []
    for e in ev.itertuples():
        key = next((k for k in (f"{e.token}USDT|{e.t_ms}", f"1000{e.token}USDT|{e.t_ms}") if k in cache), None)
        if not key or not cache[key]:
            continue
        closes = {b[0]: b[1] for b in cache[key]}
        t0 = (e.t_ms // MIN) * MIN + MIN  # end of the minute in which the notice went out
        pre_a, pre_b = price_at(closes, t0 - PRE * MIN), price_at(closes, t0 - MIN)
        row = {"kind": e.kind, "token": e.token, "notice": e.notice, "t_ms": e.t_ms,
               "pre15": (pre_b / pre_a - 1) if pre_a and pre_b else np.nan}
        for d in DELAYS:
            entry = price_at(closes, t0 + (d - 1) * MIN)
            for h in HOLDS:
                exit_ = price_at(closes, t0 + (d - 1 + h) * MIN)
                if entry and exit_:
                    row[f"g_{d}_{h}"] = (entry - exit_) / entry
        rows.append(row)
    r = pd.DataFrame(rows)
    r.to_csv(OUT / "intraday_short.csv", index=False)

    for kind, part in r.groupby("kind"):
        print(f"\n### {kind}: {len(part)} events with a perp, {part.notice.nunique()} announcements, "
              f"{pd.to_datetime(part.t_ms.min(), unit='ms'):%Y-%m}..{pd.to_datetime(part.t_ms.max(), unit='ms'):%Y-%m}")
        print(f"  15 min BEFORE publication: mean {part.pre15.mean() * 100:+.2f}%  median {part.pre15.median() * 100:+.2f}%")
        print("  gross short return, mean (median) by entry delay x hold; [90% CI of NET mean] for the 1-min entry")
        header = "  delay " + "".join(f"{f'{h}m' if h < 60 else f'{h // 60}h':>18s}" for h in HOLDS)
        print(header)
        for d in DELAYS:
            cells = []
            for h in HOLDS:
                col = f"g_{d}_{h}"
                x = part[col].dropna() if col in part else pd.Series(dtype=float)
                cells.append(f"{x.mean() * 100:+6.2f} ({x.median() * 100:+5.2f})" if len(x) >= 5 else f"{'n<5':>13s}")
            print(f"  {d:>4}m " + "".join(f"{c:>18s}" for c in cells))
        for h in HOLDS:
            col = f"g_1_{h}"
            x = part[col].dropna()
            if len(x) >= 5:
                lo, hi, k = cluster_ci((x - COST).to_numpy(), part.loc[x.index, "notice"].to_numpy())
                print(f"    1m entry, hold {h:>4}m: net mean {(x.mean() - COST) * 100:+.2f}%  90% CI [{lo * 100:+.2f}, {hi * 100:+.2f}] "
                      f"({k} announcements)  win {((x - COST) > 0).mean():.0%}")
    return 0


FROZEN = {"binance_monitor": 1440, "binance_delist": 240}
FROZEN_DELAY, STOP_SLIP = 2, 0.02
FROZEN_STOP = {"binance_monitor": 0.50, "binance_delist": 0.30}


def frozen() -> int:
    ev = events()
    cache = json.loads(CACHE.read_text(encoding="utf-8"))
    rows = []
    for e in ev[ev.kind.isin(FROZEN)].itertuples():
        key = next((k for k in (f"{e.token}USDT|{e.t_ms}", f"1000{e.token}USDT|{e.t_ms}") if k in cache), None)
        if not key or not cache[key]:
            continue
        bars = {b[0]: b for b in cache[key]}
        t0 = (e.t_ms // MIN) * MIN + MIN
        start = t0 + (FROZEN_DELAY - 1) * MIN  # entry at the close of this bar's predecessor
        entry_bar = bars.get(start - MIN)
        end = start + FROZEN[e.kind] * MIN
        exit_bar = bars.get(end - MIN)
        if not entry_bar or not exit_bar:
            continue
        entry = entry_bar[1]
        plain = (entry - exit_bar[1]) / entry - COST
        stopped_ret, stopped = plain, False
        for t in range(start, end, MIN):
            b = bars.get(t)
            if b and b[2] >= entry * (1 + FROZEN_STOP[e.kind]):
                stopped_ret, stopped = (entry - entry * (1 + FROZEN_STOP[e.kind]) * (1 + STOP_SLIP)) / entry - COST, True
                break
        rows.append({"kind": e.kind, "token": e.token, "notice": e.notice, "t_ms": e.t_ms,
                     "no_stop": plain, "with_stop": stopped_ret, "stopped": stopped})
    r = pd.DataFrame(rows)
    r.to_csv(OUT / "frozen_rule_backtest.csv", index=False)
    for kind, part in r.groupby("kind"):
        print(f"\n### {kind}: hold {FROZEN[kind]}m, entry +{FROZEN_DELAY}m, {len(part)} trades, {part.notice.nunique()} announcements")
        for col in ("no_stop", "with_stop"):
            x = part[col]
            lo, hi, k = cluster_ci(x.to_numpy(), part["notice"].to_numpy())
            yrs = " ".join(f"{y}:{g.mean() * 100:+.1f}%(n{len(g)})" for y, g in x.groupby(pd.to_datetime(part.t_ms, unit="ms").dt.year))
            print(f"  {col:9s} net mean {x.mean() * 100:+.2f}% median {x.median() * 100:+.2f}% 90% CI [{lo * 100:+.2f}, {hi * 100:+.2f}] "
                  f"win {(x > 0).mean():.0%} worst {x.min() * 100:+.1f}% stops {int(part.stopped.sum()) if col == 'with_stop' else 0} | {yrs}")
    return 0


if __name__ == "__main__":
    raise SystemExit(frozen() if "--frozen" in sys.argv else main())
