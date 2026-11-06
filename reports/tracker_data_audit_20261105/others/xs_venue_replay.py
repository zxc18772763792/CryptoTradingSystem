"""Independent read-only venue comparison of the frozen reversal ranking."""
import json
from pathlib import Path
from statistics import correlation

import numpy as np
import pandas as pd

ROOT = Path("data/historical")
RESEARCH = Path("data/research")
OUT = Path("reports/tracker_data_audit_20261105/others")
FROZEN = RESEARCH / "xs_reversal"
registered = pd.read_csv(FROZEN / "backtest_daily.csv", parse_dates=["date"]).set_index("date")
tracker = json.loads((FROZEN / "tracker.json").read_text(encoding="utf-8"))
bases = sorted(json.loads((FROZEN / "universe.json").read_text(encoding="utf-8"))["bases"])
first_date = pd.Timestamp("2021-01-01")
last_date = pd.Timestamp("2026-11-04")


def panel_for(venue, include_parts):
    series = {}
    for base in bases:
        folder = ROOT / venue / f"{base}_USDT"
        if venue == "binance_futures" and not folder.exists():
            folder = ROOT / venue / f"1000{base}_USDT"
        files = [folder / "1h.parquet"]
        if include_parts:
            files += sorted((folder / "1h_parts").glob("*.parquet"))
        pieces = [pd.read_parquet(f, columns=["close"])["close"] for f in files if f.exists()]
        if pieces:
            close = pd.concat(pieces)
            close.index = pd.to_datetime(close.index, utc=True).tz_convert(None)
            close = close[~close.index.duplicated(keep="last")].sort_index()
            series[base] = close
    return pd.DataFrame(series).sort_index()


def replay(panel):
    rows = []
    for date in pd.date_range(first_date, last_date):
        entry = date - pd.Timedelta(hours=1)
        exit_ = entry + pd.Timedelta(days=1)
        if entry not in panel.index or exit_ not in panel.index:
            continue
        recent = panel.loc[:entry].tail(200)
        price = recent.iloc[-1]
        exit_price = panel.loc[exit_]
        signal = (recent.ewm(span=21, adjust=False, min_periods=10).mean().iloc[-1] / price - 1).dropna()
        valid = signal.index.intersection(exit_price.dropna().index)
        # Match the historical evaluator, including its ex-post exit coverage filter.
        if len(valid) < 30:
            continue
        signal = signal[valid].sort_values(kind="mergesort")
        k = len(signal) // 10
        longs = signal.index[-k:]
        shorts = signal.index[:k]
        ret = exit_price / price - 1
        rows.append({"date": date, "n": len(valid),
                     "gross_pct": (ret[longs].mean() - ret[shorts].mean()) * 50})
    return pd.DataFrame(rows).set_index("date")


def tracker_gross():
    out = {}
    for day, record in tracker["days"].items():
        if record.get("status") != "closed" or record.get("late") or record.get("missing_positions"):
            continue
        long = np.mean([p["exit"] / p["entry"] - 1 for p in record["longs"].values()])
        short = np.mean([p["exit"] / p["entry"] - 1 for p in record["shorts"].values()])
        out[pd.Timestamp(day)] = (long - short) * 50
    return pd.Series(out, name="tracker_gross_pct")


def comparison(a, b):
    joined = pd.concat([a, b], axis=1, join="inner").dropna()
    return {"n": len(joined), "means": list(joined.mean()), "pearson": correlation(joined.iloc[:, 0], joined.iloc[:, 1])}


results = {"registered": {"n": len(registered), "gross_pct": float(registered.gross_spread.mean() * 50),
                          "net_pct": float(registered.net_per_position.mean() * 100),
                          "start": str(registered.index.min().date()), "end": str(registered.index.max().date())}}
daily = {"registered_gross_pct": registered.gross_spread * 50, "tracker_gross_pct": tracker_gross()}
for venue in ("binance", "binance_futures"):
    panel = panel_for(venue, include_parts=True)
    replayed = replay(panel).gross_pct
    daily[venue + "_gross_pct"] = replayed
    development = replayed.loc[registered.index.min():registered.index.max()]
    long_period = replayed.loc[:registered.index.max()]
    results[venue] = {
        "symbols": len(panel.columns),
        "development_window": comparison(development, daily["registered_gross_pct"]),
        "tracker_window": comparison(replayed, daily["tracker_gross_pct"]),
        "full_to_oct10": {"n": len(long_period), "mean_pct": long_period.mean(),
                          "start": str(long_period.index.min().date()), "end": str(long_period.index.max().date())},
    }
    print(venue, results[venue], flush=True)
results["spot_vs_perpetual_development"] = comparison(
    daily["binance_gross_pct"].loc[registered.index.min():registered.index.max()],
    daily["binance_futures_gross_pct"].loc[registered.index.min():registered.index.max()],
)
(OUT / "xs_venue_replay.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
pd.concat(daily, axis=1).to_csv(OUT / "xs_venue_replay_daily.csv", index_label="date")
print(json.dumps(results, indent=2))
