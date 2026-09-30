"""Does the LLM's go/hold filter on the rule aggregator's signals add value?

WHY THIS EXISTS (2026-09-30):
When flat, the autonomous agent only asks the model once the rule aggregator
already has a direction; the model then either follows it or holds (it never
took the opposite side). So "the model agreed with the aggregator 100% of the
time" is by construction, and scripts/agent_call_edge.py (which scores only
buy/sell calls) cannot say whether the MODEL added anything. This script can:
it compares, for the same kind of prompt, what the model let through with what
it held back.

Groups (journal rows with no open position and an aggregator direction):
  passed       the model returned buy/sell
  judged_hold  the model returned hold with its own market reasoning
  constraint   the model held citing exposure caps / cooldowns / limits
  no_judgment  the model never judged (error, timeout, instability guard,
               stale data) - a natural control: the raw aggregator signal
Rows held by a rule gate before/around the model are dropped. Every row is
scored as if it traded WITH the aggregator, using agent_call_edge.py's
no-lookahead entry and same-coin random-timing baseline, de-duplicated per
coin within 4h and group, CIs by calendar-day bootstrap.

First run (2026-03-26..09-30, deepseek flash mostly, 15m): the raw aggregator
LONG signal lost vs random timing (4h -0.45%); passed minus judged_hold was
+0.16% at 4h, 90% CI [-0.27%, +0.67%] - no demonstrable filter value. That
aggregator was dominated by a broken ML component (constant LONG on alts, see
core/ml/pipeline.py v2 notes), so re-run on data after that fix:

  python scripts/agent_filter_value.py --since 2026-10-01
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Iterable, List, Optional

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent))

from scripts.agent_call_edge import HORIZONS, day_bootstrap_ci, dedupe, default_journals, score_calls  # noqa: E402

NO_JUDGMENT = re.compile(r"^(model_error|review_service_instability|stale_market_data|timeout|provider|llm_unavailable)", re.I)
RULE_GATE = re.compile(r"^(cooldown|circuit_breaker|below_min_confidence|aggregated_signal_flat|aggregated_risk_blocked|"
                       r"all_signal_components|market_state_halt|review_cooldown)", re.I)
CONSTRAINT = re.compile(r"exposure|cap reached|cooldown|limit|blocked by|max position|remaining 0", re.I)
GROUPS = ("passed", "judged_hold", "constraint", "no_judgment")
SCORED_HORIZONS = ("1h", "4h", "24h")


def classify(action: Optional[str], reason: str) -> Optional[str]:
    if action in {"buy", "sell"}:
        return "passed"
    if action != "hold":
        return None
    if NO_JUDGMENT.search(reason):
        return "no_judgment"
    if RULE_GATE.search(reason):
        return None  # a rule gate held it, not the model
    if CONSTRAINT.search(reason):
        return "constraint"
    return "judged_hold"


def extract_prompts(paths: Iterable[Path], since: Optional[str] = None) -> pd.DataFrame:
    import json

    rows: List[dict] = []
    for path in paths:
        with open(path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                ctx = row.get("context") or {}
                if str((ctx.get("position") or {}).get("side") or "").strip():
                    continue
                agg = str((ctx.get("aggregated_signal") or {}).get("direction") or "").upper()
                if agg not in {"LONG", "SHORT"}:
                    continue
                decision = row.get("decision") or {}
                group = classify(decision.get("action"), str(decision.get("reason") or ""))
                if group is None:
                    continue
                config, selection = row.get("config") or {}, row.get("selection") or {}
                symbol = selection.get("selected_symbol") or config.get("symbol")
                if not symbol or not row.get("timestamp"):
                    continue
                rows.append({
                    "ts": row["timestamp"], "symbol": str(symbol).split(":")[0].upper(),
                    "exchange": str(config.get("exchange") or "binance").lower(),
                    "side": 1 if agg == "LONG" else -1, "group": group, "aggregator": agg,
                    "model": config.get("model"),
                })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["ts"], utc=True, format="ISO8601")
    if since:
        df = df[df["ts"] >= pd.Timestamp(since, tz="UTC")]
    return df.sort_values("ts").reset_index(drop=True)


def score(prompts: pd.DataFrame) -> pd.DataFrame:
    parts = []
    for _, part in prompts.groupby(["aggregator", "group"]):
        independent = dedupe(part.reset_index(drop=True), 4.0)
        scored = score_calls(independent, {k: HORIZONS[k] for k in SCORED_HORIZONS}, 3.0)
        if len(scored):
            parts.append(scored)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def filter_value(scored: pd.DataFrame, horizon: str, reps: int = 2000, seed: int = 0):
    """mean edge(passed) - mean edge(judged_hold) with a calendar-day bootstrap."""
    a = scored[scored["group"] == "passed"]
    b = scored[scored["group"] == "judged_hold"]
    col = f"edge_{horizon}"
    if a[col].notna().sum() == 0 or b[col].notna().sum() == 0:
        return float("nan"), (float("nan"), float("nan"))
    days = sorted(set(a["ts"].dt.date) | set(b["ts"].dt.date))
    ga = {d: x[col].dropna().to_numpy() for d, x in a.groupby(a["ts"].dt.date)}
    gb = {d: x[col].dropna().to_numpy() for d, x in b.groupby(b["ts"].dt.date)}
    rng = np.random.default_rng(seed)
    diffs = []
    for _ in range(reps):
        pick = [days[i] for i in rng.integers(0, len(days), len(days))]
        xa = np.concatenate([ga.get(d, np.array([])) for d in pick])
        xb = np.concatenate([gb.get(d, np.array([])) for d in pick])
        if len(xa) and len(xb):
            diffs.append(xa.mean() - xb.mean())
    point = float(a[col].mean() - b[col].mean())
    return point, (float(np.percentile(diffs, 5)), float(np.percentile(diffs, 95)))


def _pct(x: float) -> str:
    return "   n/a " if x != x else f"{x * 100:+6.2f}%"


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--journal", action="append", type=Path, help="journal file(s); default live + archives")
    parser.add_argument("--since", help="UTC date/time, inclusive")
    args = parser.parse_args(argv)

    prompts = extract_prompts(args.journal or default_journals(), args.since)
    if prompts.empty:
        print("no aggregator-directed prompts in the selected window")
        return 1
    print(f"prompts {len(prompts)}  {prompts['ts'].min():%Y-%m-%d} .. {prompts['ts'].max():%Y-%m-%d}")
    print(prompts.groupby(["aggregator", "group"]).size().unstack(fill_value=0).to_string())
    scored = score(prompts)
    if scored.empty:
        print("no prompts could be matched to local 5m klines")
        return 1
    for agg in ("LONG", "SHORT"):
        print(f"\n== aggregator {agg}: edge vs same coin at random times, trading WITH the aggregator")
        for group in GROUPS:
            g = scored[(scored["aggregator"] == agg) & (scored["group"] == group)]
            if g.empty:
                continue
            cells = []
            for h in SCORED_HORIZONS:
                lo, hi = day_bootstrap_ci(g[f"edge_{h}"], g["ts"].dt.date)
                cells.append(f"{h} {_pct(g[f'edge_{h}'].mean())} [{_pct(lo)},{_pct(hi)}]")
            print(f"  {group:12s} n={len(g):5d}  " + "   ".join(cells))
    print("\n== filter value (LONG prompts): passed - judged_hold")
    long_rows = scored[scored["aggregator"] == "LONG"]
    for h in SCORED_HORIZONS:
        point, (lo, hi) = filter_value(long_rows, h)
        print(f"  {h:4s} {_pct(point)}  90% CI [{_pct(lo)}, {_pct(hi)}]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
