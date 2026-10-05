"""Do coins added to Binance's Monitoring Tag underperform similar coins afterwards? (2026-10-06)

Events: every "Extend the Monitoring Tag to Include ..." notice (catalog 49,
fetched into data/research/binance_announcements/catalog49.json; 25 notices,
135 coin additions, 2023-10..2026-09), parsed with
core.research.exchange_notices.parse_monitoring_changes.

Prices: the delisting-risk spot panel (data/research/delist_risk/close.parquet,
quote_volume.parquet). It keeps coins that were later delisted, which matters
here: many tagged coins are delisted, and dropping them would flatter a short.

Method:
  * entry = close of the announcement's UTC day (notices go out 01:00-11:00
    UTC); returns over 7/14/30/60 days, plus the announcement-day move and
    the prior 30 days for context
  * controls = same-day coins in the same 30-day dollar-volume quintile and
    the same prior-30-day-return tercile, not tagged within 180 days and not
    under a delisting notice; excess = event return - control median
  * one notice tags several coins at once, so statistics are bootstrapped by
    NOTICE (25 clusters), not by coin
  * later delisting rate of tagged coins vs controls, for context

Tradability (--tradability): short the USDT perp (if one traded before the
notice) from the announcement-day close for 14 / 30 days, +40% stop on the
daily high filled 2% worse, 0.1% fees, real funding; a contract delisted
mid-trade exits at its last close. Hedged variant: long the 30 most-traded
perps of that day (scripts/upbit_caution_hedged_backtest.py), same notional.

  python scripts/monitoring_tag_study.py
  python scripts/monitoring_tag_study.py --tradability
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.research.exchange_notices import load_history, parse_monitoring_changes, parse_notice  # noqa: E402

PANEL = ROOT / "data" / "research" / "delist_risk"
CAT49 = ROOT / "data" / "research" / "binance_announcements" / "catalog49.json"
HORIZONS = (7, 14, 30, 60)


def events() -> pd.DataFrame:
    rows, seen = [], set()
    history = load_history()
    articles = json.loads(CAT49.read_text(encoding="utf-8")) + [a for cat in history.values() for a in cat]
    for a in articles:
        if a.get("code") in seen:
            continue
        seen.add(a.get("code"))
        for token in parse_monitoring_changes(a.get("title", ""))["added"]:
            rows.append({"token": token, "day0": pd.Timestamp(int(a["release_ms"]), unit="ms", tz="UTC").normalize(),
                         "notice": a.get("code")})
    return pd.DataFrame(rows).drop_duplicates(["token", "day0"]).sort_values("day0").reset_index(drop=True)


def delist_dates() -> dict:
    out: dict = {}
    for a in (x for cat in load_history().values() for x in cat):
        n = parse_notice(a.get("title", ""))
        if n and n["kind"] == "delist":
            for t in n["tokens"]:
                out.setdefault(t, []).append(pd.Timestamp(int(a["release_ms"]), unit="ms", tz="UTC").normalize())
    return out


def cluster_ci(values: pd.Series, clusters: pd.Series, reps: int = 4000):
    frame = pd.DataFrame({"v": values.to_numpy(), "c": clusters.to_numpy()}).dropna()
    groups = [g["v"].to_numpy() for _, g in frame.groupby("c")]
    rng = np.random.default_rng(0)
    means = [np.concatenate([groups[i] for i in rng.integers(0, len(groups), len(groups))]).mean() for _ in range(reps)]
    return float(np.percentile(means, 5)), float(np.percentile(means, 95)), len(groups)


def main() -> int:
    C = pd.read_parquet(PANEL / "close.parquet")
    V = pd.read_parquet(PANEL / "quote_volume.parquet")
    for f in (C, V):
        f.index = pd.to_datetime(f.index, utc=True)
    C = C.asfreq("1D")
    V = V.reindex(C.index)
    ev = events()
    tagged_days = ev.groupby("token")["day0"].apply(list).to_dict()
    delists = delist_dates()
    print(f"monitoring-tag additions: {len(ev)} coins in {ev.notice.nunique()} notices, "
          f"{ev.day0.min():%Y-%m-%d}..{ev.day0.max():%Y-%m-%d}; in price panel: {ev.token.isin(C.columns).sum()}")

    dollar_vol = V.rolling(30, min_periods=20).mean()
    prior = C / C.shift(30) - 1.0
    rows = []
    for e in ev.itertuples():
        if e.token not in C.columns or e.day0 not in C.index:
            continue
        i = C.index.get_loc(e.day0)
        if i < 31 or pd.isna(C.iloc[i][e.token]):
            continue
        vol_q = pd.qcut(dollar_vol.iloc[i].dropna().rank(method="first"), 5, labels=False)
        mom_t = pd.qcut(prior.iloc[i].dropna().rank(method="first"), 3, labels=False)
        if e.token not in vol_q.index or e.token not in mom_t.index:
            continue
        pool = [c for c in vol_q.index[(vol_q == vol_q[e.token])].intersection(mom_t.index[mom_t == mom_t[e.token]])
                if c != e.token
                and not any(abs((d - e.day0).days) <= 180 for d in tagged_days.get(c, []))
                and not any(0 <= (e.day0 - d).days <= 60 or 0 <= (d - e.day0).days <= 7 for d in delists.get(c, []))]
        row = {"token": e.token, "day0": e.day0, "notice": e.notice, "controls": len(pool),
               "prior30": float(prior.iloc[i][e.token]),
               "day0_move": float(C.iloc[i][e.token] / C.iloc[i - 1][e.token] - 1) if C.iloc[i - 1][e.token] > 0 else np.nan,
               "delisted_within_180d": any(0 <= (d - e.day0).days <= 180 for d in delists.get(e.token, []))}
        for h in HORIZONS:
            if i + h >= len(C.index):
                continue
            base, end = C.iloc[i], C.iloc[i + h]
            r = end / base - 1.0
            if pd.isna(r[e.token]):
                # gone from the panel before the horizon: a delisting, book the last price seen
                last = C[e.token].iloc[i:i + h + 1].dropna()
                ev_ret = float(last.iloc[-1] / base[e.token] - 1.0) if len(last) else np.nan
            else:
                ev_ret = float(r[e.token])
            ctrl = r[pool].dropna()
            if len(ctrl) >= 5 and np.isfinite(ev_ret):
                row[f"ret{h}"] = ev_ret
                row[f"ctrl{h}"] = float(ctrl.median())
                row[f"ex{h}"] = ev_ret - float(ctrl.median())
        rows.append(row)
    d = pd.DataFrame(rows)
    d.to_csv(ROOT / "data" / "research" / "binance_announcements" / "monitoring_tag_study.csv", index=False)
    print(f"events with prices and >= 5 matched controls: {len(d)}; median controls {d.controls.median():.0f}")
    print(f"context: prior 30d {d.prior30.median() * 100:+.1f}% (median), announcement-day move {d.day0_move.mean() * 100:+.2f}% mean, "
          f"delisted within 180d: {d.delisted_within_180d.mean():.0%}")
    for h in HORIZONS:
        col = f"ex{h}"
        if col not in d or d[col].notna().sum() < 10:
            continue
        x = d[col].dropna()
        lo, hi, k = cluster_ci(x, d.loc[x.index, "notice"])
        print(f"{h:>3}d: tagged {d[f'ret{h}'].mean() * 100:+6.2f}%  controls {d[f'ctrl{h}'].mean() * 100:+6.2f}%  "
              f"excess mean {x.mean() * 100:+6.2f}% median {x.median() * 100:+6.2f}%  90% CI [{lo * 100:+.2f}, {hi * 100:+.2f}] "
              f"({k} notices, {len(x)} coins)  underperformed {(x < 0).mean():.0%}")
    for year, part in d.groupby(d.day0.dt.year):
        if "ex30" in part:
            print(f"  {year}: n={len(part)} excess 14d {part['ex14'].mean() * 100:+.2f}%  30d {part['ex30'].mean() * 100:+.2f}%")
    return 0


def tradability() -> int:
    import time

    import httpx

    from config.settings import settings
    from core.utils.proxy_env import ensure_proxy_env
    from scripts.upbit_caution_hedged_backtest import BASKET_FEE, DAY_MS, _fetch_funding, basket_leg, pick_basket

    ensure_proxy_env(settings)
    STOP, SLIP, FEE = 0.40, 0.02, 0.001
    d = pd.read_csv(ROOT / "data" / "research" / "binance_announcements" / "monitoring_tag_study.csv", parse_dates=["day0"])
    daily = json.loads((ROOT / "data" / "research" / "upbit" / "perp_daily_cache.json").read_text(encoding="utf-8"))
    cache_path = ROOT / "data" / "research" / "binance_announcements" / "monitoring_perp_cache.json"
    cache = json.loads(cache_path.read_text(encoding="utf-8")) if cache_path.exists() else {}
    rows = []
    with httpx.Client(timeout=30, trust_env=True) as client:
        info = client.get("https://fapi.binance.com/fapi/v1/exchangeInfo").json()
        onboard = {x["symbol"]: int(x.get("onboardDate") or 0) for x in info["symbols"]
                   if x.get("quoteAsset") == "USDT" and x.get("contractType") == "PERPETUAL"}
        jobs = []
        for r in d.itertuples():
            day0_ms = int(r.day0.timestamp() * 1000)
            sym = next((s for s in (f"{r.token}USDT", f"1000{r.token}USDT")
                        if 0 < onboard.get(s, 0) < day0_ms - 3 * DAY_MS and str(day0_ms) in daily.get(s, {})), None)
            if not sym:
                continue
            key = f"{sym}|{day0_ms}"
            if key not in cache:
                kl = client.get("https://fapi.binance.com/fapi/v1/klines",
                                params={"symbol": sym, "interval": "1d", "startTime": day0_ms, "limit": 62}).json()
                fr = client.get("https://fapi.binance.com/fapi/v1/fundingRate",
                                params={"symbol": sym, "startTime": day0_ms, "limit": 1000}).json()
                cache[key] = {"klines": [[int(b[0]), float(b[2]), float(b[4])] for b in kl] if isinstance(kl, list) else [],
                              "funding": [[int(f["fundingTime"]), float(f["fundingRate"])] for f in fr] if isinstance(fr, list) else None}
                time.sleep(0.2)
            jobs.append((r, sym, day0_ms, key))
        cache_path.write_text(json.dumps(cache), encoding="utf-8")
        baskets = {(r.token, day0_ms): pick_basket(daily, onboard, day0_ms, sym) for r, sym, day0_ms, _ in jobs}
        funding = _fetch_funding(client, sorted({s for b in baskets.values() for s in b}))

    for r, sym, day0_ms, key in jobs:
        kl, fr = cache[key]["klines"], cache[key]["funding"]
        if not kl or kl[0][0] != day0_ms or fr is None:
            continue
        entry = kl[0][2]
        out = {"token": r.token, "day0": r.day0, "notice": r.notice}
        for h in (14, 30):
            bars = [b for b in kl[1:h + 1]]
            if not bars:
                continue
            exit_px, exit_ms, stopped = bars[-1][2], bars[-1][0] + DAY_MS, False
            for b in bars:
                if b[1] >= entry * (1 + STOP):
                    exit_px, exit_ms, stopped = entry * (1 + STOP) * (1 + SLIP), b[0] + DAY_MS, True
                    break
            if not stopped and len(bars) < h and kl[-1][0] + DAY_MS < time.time() * 1000 - DAY_MS:
                pass  # contract stopped trading before the horizon: the last close is the settlement
            elif not stopped and len(bars) < h:
                continue  # horizon not reached yet
            fund = sum(rate for t, rate in fr if day0_ms + DAY_MS < t <= exit_ms)
            short = (entry - exit_px) / entry - FEE + fund
            leg = basket_leg(daily, funding, baskets[(r.token, day0_ms)], day0_ms + DAY_MS, exit_ms)
            out[f"short{h}"] = short
            out[f"stopped{h}"] = stopped
            if leg is not None:
                out[f"hedged{h}"] = short + leg[0] - leg[1] - BASKET_FEE
        rows.append(out)
    t = pd.DataFrame(rows)
    t.to_csv(ROOT / "data" / "research" / "binance_announcements" / "monitoring_tag_tradability.csv", index=False)
    print(f"perp shorts: {len(t)} events in {t.notice.nunique()} notices")
    for h in (14, 30):
        for kind in ("short", "hedged"):
            col = f"{kind}{h}"
            if col not in t:
                continue
            x = t[col].dropna()
            lo, hi, k = cluster_ci(x, t.loc[x.index, "notice"])
            print(f"{h:>3}d {kind:6s}: mean {x.mean() * 100:+6.2f}%  median {x.median() * 100:+6.2f}%  90% CI [{lo * 100:+.2f}, {hi * 100:+.2f}] "
                  f"({k} notices, {len(x)} trades)  win {(x > 0).mean():.0%}  worst {x.min() * 100:+.1f}%  stops {int(t[f'stopped{h}'].sum())}")
    for year, part in t.groupby(t.day0.dt.year):
        print(f"  {year}: n={len(part)} short30 {part['short30'].mean() * 100:+.2f}%  hedged30 {part.get('hedged30', pd.Series(dtype=float)).mean() * 100:+.2f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(tradability() if "--tradability" in sys.argv else main())
