"""Read-only robustness audit of the cached Upbit short backtest.

Uses the exact cached daily bars and funding settlements from
scripts/upbit_short_backtest.py. No API calls, order calls, or output files.
The one-day delay and 1% per-side price stress are diagnostics, not new rules.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.upbit_short_backtest import score_short_trade  # noqa: E402

DATA = ROOT / "data" / "research" / "upbit"
DAY_MS = 86_400_000
FEE, STOP, STOP_SLIP = 0.001, 0.40, 0.02


def _return(bars: list, funding: list, day0_ms: int, *, delay_days: int = 0,
            price_stress: float = 0.0, cap_funding: bool = True) -> float:
    """Short D0/D1 close to D7 close with the archived intraday-high stop."""
    entry = float(bars[delay_days][4]) * (1 - price_stress)
    exit_px = float(bars[7][4])
    exit_ms = day0_ms + 8 * DAY_MS
    for bar in bars[delay_days + 1:8]:
        if float(bar[2]) >= entry * (1 + STOP):
            exit_px = entry * (1 + STOP) * (1 + STOP_SLIP)
            exit_ms = int(bar[0]) + DAY_MS
            break
    exit_px *= 1 + price_stress
    entry_ms = day0_ms + (delay_days + 1) * DAY_MS
    fund = sum(float(row["fundingRate"]) for row in funding
               if entry_ms <= int(row["fundingTime"]) <= (exit_ms if cap_funding else day0_ms + 8 * DAY_MS))
    return (entry - exit_px) / entry - FEE + fund


def audit() -> pd.DataFrame:
    rows = pd.read_csv(DATA / "short_backtest.csv")
    cache = json.loads((DATA / "perp_cache.json").read_text(encoding="utf-8"))
    output = []
    for row in rows.itertuples(index=False):
        if row.status != "ok":
            continue
        day0 = pd.Timestamp(row.date)
        day0_ms = int(day0.timestamp() * 1000)
        key = f"v2|{row.symbol}|{day0.date()}"
        item = cache.get(key) or {}
        bars, funding = item.get("klines") or [], item.get("funding") or []
        reason = None
        if len(bars) < 8 or any(int(bars[i][0]) != day0_ms + i * DAY_MS for i in range(8)):
            reason = "incomplete_daily_bars"
        elif not funding:
            reason = "missing_funding"
        if reason:
            output.append({"kind": row.kind, "token": row.token, "date": day0,
                           "quality": reason})
            continue
        baseline = _return(bars, funding, day0_ms, cap_funding=False)
        if abs(baseline - float(row.short_ret)) > 1e-9:
            raise ValueError(f"cache/CSV return mismatch for {key}: {baseline} vs {row.short_ret}")
        try:
            corrected, corrected_funding, stopped = score_short_trade(bars, funding, day0_ms)
        except ValueError as exc:
            output.append({"kind": row.kind, "token": row.token, "date": day0,
                           "quality": str(exc)})
            continue
        if abs(corrected - _return(bars, funding, day0_ms)) > 1e-9:
            raise ValueError(f"research scorer mismatch for {key}")
        output.append({"kind": row.kind, "token": row.token, "date": day0,
                       "quality": "valid", "baseline": baseline, "corrected": corrected,
                       "delay_1d": _return(bars, funding, day0_ms, delay_days=1),
                       "stress_1pct_per_side": _return(bars, funding, day0_ms, price_stress=0.01),
                       "funding_n": len(funding), "funding_corrected": corrected_funding,
                       "stopped": stopped, "mkt7": row.mkt7})
    return pd.DataFrame(output)


def main() -> None:
    frame = audit()
    print("quality:", frame.groupby(["kind", "quality"]).size().to_dict())
    print("excluded:", frame.loc[frame.quality != "valid", ["kind", "token", "date", "quality"]]
          .assign(date=lambda x: x.date.dt.strftime("%Y-%m-%d")).to_dict("records"))
    valid = frame[frame.quality == "valid"].copy()
    for kind, group in valid.groupby("kind"):
        rng = np.random.default_rng(0)
        month_key = group.date.dt.strftime("%Y-%m")
        metrics = {}
        for name in ("baseline", "corrected", "delay_1d", "stress_1pct_per_side"):
            monthly = group.groupby(month_key)[name].agg(["sum", "count"]).to_numpy()
            draw = rng.integers(0, len(monthly), size=(5000, len(monthly)))
            samples = monthly[draw, 0].sum(axis=1) / monthly[draw, 1].sum(axis=1)
            metrics[name] = {"mean_pct": round(float(group[name].mean() * 100), 2),
                             "median_pct": round(float(group[name].median() * 100), 2),
                             "min_pct": round(float(group[name].min() * 100), 2),
                             "positive": int((group[name] > 0).sum()),
                             "month_bootstrap_90_pct": [round(float(x * 100), 2)
                                                        for x in np.quantile(samples, [0.05, 0.95])]}
        years = {int(year): round(float(part.corrected.mean() * 100), 2)
                 for year, part in group.groupby(group.date.dt.year)}
        regimes = {label: {"n": len(part), "mean_pct": round(float(part.corrected.mean() * 100), 2)}
                   for label, part in group.groupby(np.where(group.mkt7 >= 0, "market_up", "market_down"))}
        hedge_proxy = group.corrected + group.mkt7 - FEE
        print(json.dumps({"kind": kind, "n": len(group), "months": group.date.dt.strftime("%Y-%m").nunique(),
                          "funding_records_min": int(group.funding_n.min()), "stops": int(group.stopped.sum()),
                          "funding_correction_pct_mean": round(float((group.corrected - group.baseline).mean() * 100), 3),
                          "market_proxy_mean_pct": round(float(group.mkt7.mean() * 100), 2),
                          "hedge_proxy_mean_pct": round(float(hedge_proxy.mean() * 100), 2),
                          "by_year_mean_pct": years, "by_realized_market_regime": regimes,
                          "metrics": metrics}, ensure_ascii=False))


if __name__ == "__main__":
    main()
