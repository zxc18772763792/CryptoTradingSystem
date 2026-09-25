"""Does the news LLM's sentiment predict price? (offline, from data/news.db)

WHY THIS EXISTS (2026-09-25):
The news worker has labelled every headline with an LLM (sentiment -1/0/+1,
impact_score, event_type) since early 2026. That is a clean post-training-cutoff
test of "LLM reads the news -> forecast", with no look-ahead from the model.

First run (2026-04-21..09-25, 16.5k events -> ~2.6k coin-hours): rank-IC
+0.02..+0.08, signed edge +0.02..+0.08% at 15m-1h, ~0 at 4h, slightly negative
at 24h; roughly 10x below round-trip costs. Measuring from publish time vs
from LLM-finished time gives the same numbers: whatever the headline carries is
already priced before a retail pipeline can act. Sub-slices (event types) are
~40 tests; treat any single CI clearing zero as a hypothesis for the next
out-of-sample window, not a finding.

Method mirrors scripts/agent_call_edge.py: events on one coin in one hour are
netted into one score (sum sentiment*impact), entry = last fully closed 5m bar
before the clock time, edge = signed return minus same-coin random-timing
return within +/-3d, CI = bootstrap over days.

Usage:
  python scripts/news_llm_signal_edge.py
  python scripts/news_llm_signal_edge.py --since 2026-10-01 --by event_type
  python scripts/news_llm_signal_edge.py --clock published --min-impact 0.8
"""
from __future__ import annotations

import argparse
import importlib.util
import sqlite3
import sys
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

_spec = importlib.util.spec_from_file_location("agent_call_edge", SCRIPT_DIR / "agent_call_edge.py")
ace = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ace)

NEWS_DB = PROJECT_ROOT / "data" / "news.db"


def load_events(db: Path, since: str, until: Optional[str], max_lag_min: float) -> pd.DataFrame:
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        df = pd.read_sql(
            "select ts, created_at, symbol, event_type, sentiment, impact_score, model_source from news_events",
            con,
        )
    finally:
        con.close()
    for col in ("ts", "created_at"):
        df[col] = pd.to_datetime(df[col], utc=True, errors="coerce", format="mixed")
    df["lag_min"] = (df["created_at"] - df["ts"]).dt.total_seconds() / 60.0
    # Backfilled rows (labelled long after publication) were never tradable live.
    df = df[(df["lag_min"] >= 0) & (df["lag_min"] <= max_lag_min) & (df["sentiment"] != 0)]
    df = df[df["ts"] >= pd.Timestamp(since, tz="UTC")]
    if until:
        df = df[df["ts"] < pd.Timestamp(until, tz="UTC")]
    df = df.copy()
    df["pair"] = df["symbol"].astype(str).str.upper().str.replace(r"USDT$", "/USDT", regex=True)
    df["score"] = df["sentiment"] * df["impact_score"].fillna(0.5)
    return df


def score_coin_hours(events: pd.DataFrame, clock: str, min_abs_score: float, baseline_days: float) -> pd.DataFrame:
    col = "ts" if clock == "published" else "created_at"
    ev = events.assign(hour=events[col].dt.floor("1h"))
    grouped = (
        ev.groupby(["pair", "hour"])
        .agg(score=("score", "sum"), t=(col, "max"), n=("score", "size"))
        .reset_index()
    )
    grouped = grouped[grouped["score"].abs() >= min_abs_score]
    window = pd.Timedelta(days=baseline_days)
    recs = []
    for row in grouped.itertuples(index=False):
        closes = ace.load_closes("binance", row.pair)
        if closes is None:
            continue
        pos = int(closes.index.searchsorted(row.t - ace.BAR, side="right")) - 1
        if pos < 0:
            continue
        lo = int(closes.index.searchsorted(row.t - window))
        hi = int(closes.index.searchsorted(row.t + window))
        side = float(np.sign(row.score))
        rec = {"ts": row.t, "pair": row.pair, "score": row.score}
        for name, bars in ace.HORIZONS.items():
            if pos + bars >= len(closes):
                rec[f"edge_{name}"] = rec[f"raw_{name}"] = np.nan
                continue
            fwd = closes.iloc[pos + bars] / closes.iloc[pos] - 1.0
            seg = closes.iloc[lo:min(hi, len(closes) - bars) + bars].to_numpy()
            base = float(np.mean(seg[bars:] / seg[:-bars] - 1.0))
            rec[f"raw_{name}"] = fwd
            rec[f"edge_{name}"] = side * (fwd - base)
        recs.append(rec)
    return pd.DataFrame(recs)


def print_table(scored: pd.DataFrame, label: str) -> None:
    if scored.empty:
        print(f"\n== {label}: no matched coin-hours")
        return
    days = scored["ts"].dt.date
    print(f"\n== {label}: {len(scored)} coin-hours ({(scored['score'] > 0).mean():.0%} bullish)")
    for name in ace.HORIZONS:
        edge = scored[f"edge_{name}"]
        lo, hi = ace.day_bootstrap_ci(edge, days)
        ic = scored[["score", f"raw_{name}"]].corr(method="spearman").iloc[0, 1]
        print(
            f"  {name:4s} edge {edge.mean() * 100:+.3f}%  90%CI[{lo * 100:+.3f}%, {hi * 100:+.3f}%]"
            f"  hit {(edge > 0).mean():.0%}  rank-IC {ic:+.3f}"
        )


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--db", type=Path, default=NEWS_DB)
    parser.add_argument("--since", default="2026-04-21", help="UTC; local 5m klines start 2026-04-20")
    parser.add_argument("--until")
    parser.add_argument("--clock", choices=["llm_done", "published"], default="llm_done",
                        help="llm_done = when the label existed (tradable); published = upper bound")
    parser.add_argument("--max-lag-min", type=float, default=60.0, help="drop events labelled later than this")
    parser.add_argument("--min-impact", type=float, default=0.0)
    parser.add_argument("--min-abs-score", type=float, default=0.5)
    parser.add_argument("--baseline-days", type=float, default=3.0)
    parser.add_argument("--by", choices=["event_type", "pair"])
    args = parser.parse_args(argv)

    events = load_events(args.db, args.since, args.until, args.max_lag_min)
    events = events[events["impact_score"].fillna(0.5) >= args.min_impact]
    if events.empty:
        print("no events in window")
        return 1
    print(
        f"events: {len(events)} {events['ts'].min():%Y-%m-%d} -> {events['ts'].max():%Y-%m-%d} | "
        f"median LLM lag {events['lag_min'].median():.1f} min | clock={args.clock}"
    )
    print_table(score_coin_hours(events, args.clock, args.min_abs_score, args.baseline_days), "all")
    if args.by:
        top = events[args.by].value_counts()
        for key in top[top >= 200].index:
            sub = events[events[args.by] == key]
            print_table(score_coin_hours(sub, args.clock, args.min_abs_score, args.baseline_days), f"{args.by}={key}")
        print("\nnote: many slices = many tests; a single CI clearing zero is a hypothesis, not a finding.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
