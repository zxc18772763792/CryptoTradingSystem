"""Protocol fee growth vs next-month token returns (round 7).

WHY: the equity anomaly that survives best is earnings momentum (firms whose
earnings just grew keep outperforming). Round 5 found buyback *level*
priced in; this tests the *change*. Fees come from DefiLlama (overview/fees
+ summary/fees/<slug>, dailyFees), children summed into their parent
protocol, mapped to Binance spot tokens by symbol; prices from the
survivorship-free panel data/research/delist_risk/close.parquet. No market
cap is needed, so no supply approximation.

Signal at month start D (only fees dated before D):
  fee_growth = fees(D-30..D-1) / fees(D-120..D-31) * 3 - 1  (last month vs the prior quarter's monthly average)
Long top tercile / short bottom tercile, 30-day hold, 0.4% cost, returns
relative to the sample's equal-weight mean. Controls: the same inside
30-day price-momentum terciles (fees can simply follow price), and the rank
IC sign count with a one-sided binomial p. Minimum $300k fees in the base
quarter to skip dust protocols.
"""
import json
import sys
import time
from math import comb
from pathlib import Path

import httpx
import numpy as np
import pandas as pd

sys.path.insert(0, ".")
from core.research.xs_evaluation import spearman

OUT = Path("data/research/fees")
MIN_BASE_FEES = 300_000
COST = 0.004


def get(client, url, params=None):
    for attempt in range(4):
        try:
            r = client.get(url, params=params)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 404):
                return None
        except Exception:  # noqa: BLE001
            pass
        time.sleep(3 * (attempt + 1))
    return None


def fetch():
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / "daily_fees.parquet"
    if path.exists():
        return pd.read_parquet(path), json.loads((OUT / "tokens.json").read_text(encoding="utf-8"))
    with httpx.Client(timeout=60) as c:
        ov = get(c, "https://api.llama.fi/overview/fees", {"excludeTotalDataChart": "true", "excludeTotalDataChartBreakdown": "true"})["protocols"]
        lite = get(c, "https://api.llama.fi/lite/protocols2")
        parents = {p["id"]: p for p in lite["parentProtocols"]}
        protos = {str(p.get("defillamaId")): p for p in lite["protocols"]}
        series, tokens = {}, {}
        rows = [p for p in ov if (p.get("total30d") or 0) > 0 or (p.get("totalAllTime") or 0) > 0]
        print("protocols with fees:", len(rows))
        for i, p in enumerate(rows):
            par = parents.get(p.get("parentProtocol") or "")
            own = protos.get(str(p.get("defillamaId")))
            sym = str((par or {}).get("symbol") or (own or {}).get("symbol") or "").upper()
            if not sym or sym == "-":
                continue
            j = get(c, f"https://api.llama.fi/summary/fees/{p['slug']}", {"dataType": "dailyFees"})
            chart = (j or {}).get("totalDataChart") or []
            if not chart:
                continue
            s = pd.Series({pd.Timestamp(int(t), unit="s", tz="UTC").normalize(): float(v or 0) for t, v in chart})
            key = (par or {}).get("id") or p["slug"]
            series[key] = series[key].add(s, fill_value=0) if key in series else s
            tokens[key] = sym
            if i % 50 == 0:
                print(f"  {i}/{len(rows)}", flush=True)
    fees = pd.DataFrame(series).sort_index()
    fees.to_parquet(path)
    (OUT / "tokens.json").write_text(json.dumps(tokens), encoding="utf-8")
    return fees, tokens


def main():
    fees, tokens = fetch()
    by_token = {}
    for key, sym in tokens.items():
        if key in fees.columns:
            by_token[sym] = by_token[sym].add(fees[key], fill_value=0) if sym in by_token else fees[key]
    fees = pd.DataFrame(by_token).asfreq("1D")
    close = pd.read_parquet("data/research/delist_risk/close.parquet")
    close.index = pd.to_datetime(close.index, utc=True)
    close = close.asfreq("1D")
    common = sorted(set(fees.columns) & set(close.columns))
    print(f"fee series mapped to tokens: {fees.shape[1]} | with Binance prices: {len(common)}")
    last = (close.index.max() - pd.Timedelta(days=31)).normalize().replace(day=1)
    rows = []
    for d in pd.date_range(pd.Timestamp("2023-01-01", tz="UTC"), last, freq="MS"):
        recent = fees.loc[d - pd.Timedelta(days=30): d - pd.Timedelta(days=1), common].sum(min_count=20)
        base = fees.loc[d - pd.Timedelta(days=120): d - pd.Timedelta(days=31), common].sum(min_count=60)
        end, back = d + pd.Timedelta(days=30), d - pd.Timedelta(days=30)
        for t in common:
            if not (base.get(t, 0) >= MIN_BASE_FEES and pd.notna(recent.get(t))):
                continue
            p0, p1, pb = close.at[d, t], close.at[end, t], close.at[back, t] if back in close.index else np.nan
            if pd.notna(p0) and pd.notna(p1) and pd.notna(pb) and p0 > 0 and pb > 0:
                rows.append({"date": d, "tok": t, "fee_growth": recent[t] / base[t] * 3 - 1,
                             "mom30": p0 / pb - 1, "ret30": p1 / p0 - 1})
    P = pd.DataFrame(rows)
    P = P[P.groupby("date").tok.transform("size") >= 12]
    P["ret_x"] = P.ret30 - P.groupby("date").ret30.transform("mean")
    print(f"panel: {len(P)} token-months, {P.date.nunique()} months, median {int(P.groupby('date').size().median())} tokens/month")
    print(f"  corr(fee growth, past 30d price move) median across months: {P.groupby('date').apply(lambda g: spearman(g.fee_growth, g.mom30)).median():.2f}")

    def spread(frame, col):
        q = frame.groupby("date")[col].rank(pct=True)
        f = frame.assign(q=q)
        return f.groupby("date").apply(lambda g: g[g.q > 2 / 3].ret_x.mean() - g[g.q <= 1 / 3].ret_x.mean() - COST).dropna()

    def report(label, col, frame=P):
        s = spread(frame, col)
        ic = frame.groupby("date").apply(lambda g: spearman(g[col], g.ret30)).dropna()
        k, n = int((ic > 0).sum()), len(ic)
        rng = np.random.default_rng(0)
        boot = [s.sample(len(s), replace=True, random_state=int(x)).mean() for x in rng.integers(0, 10 ** 9, 2000)]
        early, late = s[s.index < "2025-01-01"], s[s.index >= "2025-01-01"]
        print(f"== {label}: long high / short low, 30d, after costs {s.mean() * 100:+.2f}%/month 90% CI [{np.percentile(boot, 5) * 100:+.2f}, {np.percentile(boot, 95) * 100:+.2f}] "
              f"| IC>0 in {k}/{n} months (p={sum(comb(n, i) for i in range(k, n + 1)) / 2 ** n:.3f}) | 2023-24 {early.mean() * 100:+.2f}% 2025-26 {late.mean() * 100:+.2f}%")

    report("fee growth", "fee_growth")
    report("past 30d price move (control)", "mom30")
    # fee growth net of price momentum: rank residual within momentum terciles
    P["mom_t"] = P.groupby("date").mom30.transform(lambda x: np.ceil(x.rank(pct=True) * 3))
    P["fee_in_mom"] = P.groupby(["date", "mom_t"]).fee_growth.rank(pct=True)
    report("fee growth within price-momentum terciles", "fee_in_mom")


if __name__ == "__main__":
    main()
