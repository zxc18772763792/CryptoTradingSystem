"""Where does the autonomous agent's paper P&L come from? Fees vs price, exits, fills, chasing, churn.

WHY THIS EXISTS (2026-10-09):
The agent lost ~200 USDT on paper over 2026-09-10..10-07 (130 closed trades). Decomposed:
  - price P&L about zero (no timing edge); fees ~0.23% per round trip were the whole loss;
  - 45% of entries re-bought the same coin a median 2 min after closing it (-0.46%/trade vs +0.16%);
  - in September most entries came after a 24h move in the trade's direction, and the >4% chasers
    did worst over the next 5h;
  - paper market orders filled at the agent's last 15m bar close instead of the market:
    33/130 entries were >0.5% off the real price.
Changed on 2026-10-09: paper market orders fill at the live price; partial take-profit off;
4h same-coin re-entry cooldown; no fresh entry after a >4% 24h move in the trade's direction.
This script measures the trades again so the changes can be judged on new data:

    python scripts/agent_trade_attribution.py --split 2026-10-09T00:20

Pre-registered verdict (docs/AGENT_LOSS_ANALYSIS_2026-10-09.md): once >= 40 trades opened after
--split have closed, their mean net return per trade (after fees) and its 90% day-bootstrap CI
decide. CI wholly below 0: the changes did not rescue it, stop the autonomous direction calls.
CI wholly above 0: working. Otherwise: no evidence either way, keep collecting.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.agent_call_edge import day_bootstrap_ci, default_journals  # noqa: E402

STATE_DIR = PROJECT_ROOT / "data" / "cache" / "runtime_state"
KLINE_DIR = PROJECT_ROOT / "data" / "historical" / "binance"
STRATEGY = "AI_AutonomousAgent"
REENTRY_WINDOW_H = 4.0
CHASE_LIMIT = 0.04
MIN_VERDICT_TRADES = 40
FILL_TOLERANCE = 0.001  # a fill this far outside its 5m bar's high-low range cannot have traded
MATCH_SLACK = pd.Timedelta(seconds=30)
BARS_5H = 60
JOURNAL_TS = re.compile(rb'"timestamp": "([0-9T:\-\.+]+)"')
GATES = ("reentry_cooldown", "chase_guard", "review_cooldown", "review_service_instability", "model_error",
         "below_min_confidence", "aggregated_signal_flat", "circuit_breaker", "stale_market_data")


def _utc(value: Any) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def load_trades(state_dir: Path = STATE_DIR, strategy: str = STRATEGY) -> pd.DataFrame:
    """Closed paper positions of `strategy` with their fills' quantities, fees and exit reasons."""
    positions = json.loads((state_dir / "positions_paper.json").read_text(encoding="utf-8")).get("closed_positions") or []
    history = json.loads((state_dir / "risk_trade_history_paper.json").read_text(encoding="utf-8")).get("trade_history") or []
    fills = pd.DataFrame([row for row in history if row.get("strategy") == strategy])
    if not fills.empty:
        fills["ts"] = pd.to_datetime(fills["timestamp"], utc=True, format="ISO8601")
    rows: List[Dict[str, Any]] = []
    for pos in positions:
        if pos.get("strategy") != strategy:
            continue
        opened, closed = _utc(pos["opened_at"]), _utc(pos["updated_at"])
        mine = fills[(fills["symbol"] == pos["symbol"]) & (fills["ts"] >= opened - MATCH_SLACK)
                     & (fills["ts"] <= closed + MATCH_SLACK)] if not fills.empty else fills
        # entries strictly before the close: a re-entry seconds after it belongs to the next trade
        entries = mine[(mine["action"] == "open_or_add") & (mine["ts"] < closed)] if not mine.empty else mine
        exits = mine[(mine["action"] != "open_or_add") & (mine["ts"] > opened)] if not mine.empty else mine
        entry = float(pos["entry_price"])
        qty0 = float(entries["quantity"].sum()) if len(entries) else float(pos.get("quantity") or 0.0)
        notional = entry * qty0
        gross = float(pos.get("realized_pnl") or 0.0)
        fees = float(pd.to_numeric(mine.loc[entries.index.union(exits.index), "fee_usd"], errors="coerce").sum()) if len(mine) else 0.0
        reasons = [str(r) for r in exits.sort_values("ts")["close_reason"].fillna("")] if len(exits) else []
        meta = pos.get("metadata") or {}
        rows.append({
            "symbol": pos["symbol"], "side": str(pos.get("side") or "").lower(), "opened": opened, "closed": closed,
            "entry": entry, "exit": float(pos.get("current_price") or 0.0), "qty": qty0, "notional": notional,
            "gross": gross, "fees": fees, "net": gross - fees,
            "ret_gross": gross / notional if notional > 0 else np.nan,
            "ret_net": (gross - fees) / notional if notional > 0 else np.nan,
            "exit_reason": (reasons[-1] or "unknown") if reasons else "unrecorded",
            "partial_tp": "partial_take_profit" in reasons,
            "confidence": pd.to_numeric(meta.get("agent_confidence"), errors="coerce"),
            "hold_h": (closed - opened).total_seconds() / 3600.0,
        })
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out = out.sort_values("opened").reset_index(drop=True)
    out["sign"] = np.where(out["side"] == "long", 1.0, -1.0)
    prev_close = out.groupby("symbol")["closed"].shift(1)
    out["reentry_gap_h"] = (out["opened"] - prev_close).dt.total_seconds() / 3600.0
    return out


_BARS: Dict[str, Optional[pd.DataFrame]] = {}


def load_bars(symbol: str, first_day: str, last_day: str, root: Path = KLINE_DIR) -> Optional[pd.DataFrame]:
    """5m OHLC from the local daily parquet parts (files named YYYY-MM-DD), indexed by bar open time."""
    if symbol not in _BARS:
        parts = [p for p in sorted((root / symbol.replace("/", "_") / "5m_parts").glob("*.parquet"))
                 if first_day <= p.stem <= last_day]
        frames = []
        for part in parts:
            try:
                frames.append(pd.read_parquet(part, columns=["open", "high", "low", "close"]))
            except Exception:
                continue
        if not frames:
            _BARS[symbol] = None
        else:
            frame = pd.concat(frames)
            frame.index = pd.to_datetime(frame.index, utc=True)
            _BARS[symbol] = frame[~frame.index.duplicated(keep="last")].sort_index().astype(float)
    return _BARS[symbol]


def add_market_context(trades: pd.DataFrame, root: Path = KLINE_DIR) -> pd.DataFrame:
    """Fill plausibility vs the 5m bar, side-adjusted prior moves, 5h hold and the coin's own 5h drift."""
    if trades.empty:
        return trades
    first_day = (trades["opened"].min() - pd.Timedelta(days=4)).strftime("%Y-%m-%d")
    last_day = (trades["closed"].max() + pd.Timedelta(days=4)).strftime("%Y-%m-%d")
    cols: Dict[str, List[float]] = {k: [] for k in ("entry_outside", "exit_outside", "entry_gap", "exit_gap",
                                                    "r1h", "r4h", "r24h", "flat5h", "timing")}
    for t in trades.itertuples():
        bars = load_bars(t.symbol, first_day, last_day, root)
        values = dict.fromkeys(cols, np.nan)
        if bars is not None and len(bars) > 300:
            for label, price, when, sign in (("entry", t.entry, t.opened, t.sign), ("exit", t.exit, t.closed, -t.sign)):
                key = when.floor("5min")
                if key in bars.index and price > 0:
                    bar = bars.loc[key]
                    values[f"{label}_outside"] = float(price > bar["high"] * (1 + FILL_TOLERANCE)
                                                       or price < bar["low"] * (1 - FILL_TOLERANCE))
                    values[f"{label}_gap"] = sign * (price / ((bar["open"] + bar["close"]) / 2.0) - 1.0)
            before = bars["close"][bars.index < t.opened.floor("5min")]
            if len(before) > 288:
                p0 = before.iloc[-1]
                for name, steps in (("r1h", 12), ("r4h", 48), ("r24h", 288)):
                    values[name] = t.sign * (p0 / before.iloc[-1 - steps] - 1.0)
                after = bars["close"][bars.index >= t.opened.floor("5min")]
                if len(after) > BARS_5H:
                    values["flat5h"] = t.sign * (after.iloc[BARS_5H] / p0 - 1.0)
                    window = bars["close"][(bars.index >= t.opened - pd.Timedelta(days=3))
                                           & (bars.index < t.opened + pd.Timedelta(days=3))]
                    drift = (window.shift(-BARS_5H) / window - 1.0).dropna().mean()
                    values["timing"] = values["flat5h"] - t.sign * drift
        for key in cols:
            cols[key].append(values[key])
    out = trades.copy()
    for key, series in cols.items():
        out[key] = series
    return out


def gate_counts(paths: Iterable[Path], since: pd.Timestamp) -> Dict[str, Any]:
    """Decision reasons and answering models in the agent journal after `since`."""
    reasons: Dict[str, int] = {}
    models: Dict[str, int] = {}
    total = 0
    for path in paths:
        with open(path, "rb") as fh:
            for line in fh:
                match = JOURNAL_TS.search(line[:400])
                if not match or _utc(match.group(1).decode()) < since:
                    continue
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                total += 1
                decision = row.get("decision") or {}
                action = str(decision.get("action") or "").lower()
                reason = str(decision.get("reason") or "")
                gate = next((g for g in GATES if reason.startswith(g)), None)
                key = gate or (f"model:{action}" if action in {"buy", "sell", "hold", "close_long", "close_short"} else "other")
                reasons[key] = reasons.get(key, 0) + 1
                answered = (row.get("config") or {}).get("answered_by")
                if isinstance(answered, dict) and answered.get("model"):
                    name = f"{answered['model']}{' (backup)' if answered.get('backup') else ''}"
                    models[name] = models.get(name, 0) + 1
    return {"rows": total, "reasons": reasons, "models": models}


def journals_since(since: pd.Timestamp) -> List[Path]:
    """Archives are rotated by time and named by rotation instant; skip those that ended before `since`."""
    keep = []
    for path in default_journals():
        stamp = re.search(r"\.(\d{8}T\d{6}Z)\.jsonl$", path.name)
        if stamp and _utc(stamp.group(1)) < since:
            continue
        keep.append(path)
    return keep


def _pct(x: float, digits: int = 2) -> str:
    return "  n/a" if x is None or x != x else f"{x * 100:+.{digits}f}%"


def _summary(g: pd.DataFrame) -> str:
    if g.empty:
        return "n=0"
    days = max(1.0, (g["closed"].max() - g["opened"].min()).total_seconds() / 86400.0)
    wins, losses = g[g["gross"] > 0], g[g["gross"] <= 0]
    payoff = abs(wins["ret_gross"].mean() / losses["ret_gross"].mean()) if len(wins) and len(losses) else np.nan
    lo, hi = day_bootstrap_ci(g["ret_net"], g["opened"].dt.date)
    return (f"n={len(g)} ({len(g) / days:.1f}/day)  price {g['gross'].sum():+.2f}  fees {-g['fees'].sum():+.2f}  "
            f"net {g['net'].sum():+.2f} USDT | per trade gross {_pct(g['ret_gross'].mean(), 3)} "
            f"net {_pct(g['ret_net'].mean(), 3)} [90% {_pct(lo, 3)}, {_pct(hi, 3)}] | win {(g['gross'] > 0).mean():.0%} "
            f"avg win {_pct(wins['ret_gross'].mean())} avg loss {_pct(losses['ret_gross'].mean())} payoff {payoff:.2f}")


def report(trades: pd.DataFrame, split: Optional[pd.Timestamp], gates: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    print(f"closed agent trades: {len(trades)}  {trades['opened'].min():%Y-%m-%d %H:%M} .. {trades['closed'].max():%Y-%m-%d %H:%M} UTC")
    print(f"  all : {_summary(trades)}")
    post = trades[trades["opened"] >= split] if split is not None else trades.iloc[0:0]
    if split is not None:
        pre = trades[trades["opened"] < split]
        print(f"  pre : {_summary(pre)}")
        print(f"  post: {_summary(post)}   (opened >= {split:%Y-%m-%d %H:%M} UTC)")

    print("\n== exits (gross return per trade)")
    by_exit = trades.groupby("exit_reason").agg(n=("gross", "size"), gross=("ret_gross", "mean"), net=("ret_net", "mean"),
                                                hold_h=("hold_h", "median"))
    for name, row in by_exit.sort_values("n", ascending=False).iterrows():
        print(f"  {name:28s} n={int(row.n):4d}  gross {_pct(row.gross)}  net {_pct(row.net)}  median hold {row.hold_h:.1f}h")
    print(f"  trades with a partial take-profit: {int(trades['partial_tp'].sum())}")

    checked = trades.dropna(subset=["entry_outside"])
    print(f"\n== fills vs the 5m bar they happened in ({len(checked)}/{len(trades)} trades with local bars)")
    if len(checked):
        bad_entry = checked[checked["entry_outside"] > 0]
        bad_exit = checked[checked["exit_outside"] > 0]
        print(f"  entries outside the bar's high-low: {len(bad_entry)}  exits: {len(bad_exit)}  "
              f"(+ = worse than the bar mid) entry {_pct(checked['entry_gap'].mean(), 3)} exit {_pct(checked['exit_gap'].mean(), 3)}")
        if len(bad_entry):
            print("  " + ", ".join(f"{r.symbol} {r.opened:%m-%d %H:%M} {_pct(r.entry_gap)}" for r in bad_entry.tail(8).itertuples()))

    ctx = trades.dropna(subset=["r24h"])
    print(f"\n== entry context (side-adjusted: + = price had already moved in the trade's direction; {len(ctx)} trades)")
    if len(ctx):
        print(f"  prior 1h {_pct(ctx['r1h'].mean())}  4h {_pct(ctx['r4h'].mean())}  24h {_pct(ctx['r24h'].mean())} | "
              f"entries after a >{CHASE_LIMIT:.0%} 24h move: {(ctx['r24h'] > CHASE_LIMIT).sum()} ({(ctx['r24h'] > CHASE_LIMIT).mean():.0%})")
        buckets = pd.cut(ctx["r24h"], [-np.inf, 0.0, CHASE_LIMIT, np.inf], labels=["faded/flat", f"0..{CHASE_LIMIT:.0%}", f">{CHASE_LIMIT:.0%}"])
        for name, g in ctx.groupby(buckets, observed=True):
            print(f"  24h {str(name):10s} n={len(g):4d}  actual gross {_pct(g['ret_gross'].mean())}  hold-5h {_pct(g['flat5h'].mean())}")

    gaps = trades["reentry_gap_h"]
    quick = trades[gaps < REENTRY_WINDOW_H]
    rest = trades[~(gaps < REENTRY_WINDOW_H)]
    print(f"\n== churn: re-entries on the same coin within {REENTRY_WINDOW_H:.0f}h of closing it")
    print(f"  {len(quick)} of {len(trades)} ({len(quick) / max(1, len(trades)):.0%}); median gap "
          f"{quick['reentry_gap_h'].median() * 60 if len(quick) else float('nan'):.0f} min | net per trade: those {_pct(quick['ret_net'].mean())} "
          f"vs others {_pct(rest['ret_net'].mean())}")

    timed = trades.dropna(subset=["timing"])
    print(f"\n== timing skill ({len(timed)} trades): hold-5h {_pct(timed['flat5h'].mean())}, minus the coin's own 5h drift "
          f"{_pct(timed['timing'].mean())}; {(timed['timing'] > 0).mean() if len(timed) else float('nan'):.0%} beat it")

    if gates is not None:
        print(f"\n== agent decisions since the split ({gates['rows']} journal rows)")
        for key, count in sorted(gates["reasons"].items(), key=lambda kv: -kv[1]):
            print(f"  {key:28s} {count}")
        if gates["models"]:
            print("  answered by: " + ", ".join(f"{k} {v}" for k, v in sorted(gates["models"].items(), key=lambda kv: -kv[1])))

    verdict = {"n": int(len(post)), "min_n": MIN_VERDICT_TRADES}
    if split is not None:
        lo, hi = day_bootstrap_ci(post["ret_net"], post["opened"].dt.date) if len(post) else (np.nan, np.nan)
        verdict.update(mean=float(post["ret_net"].mean()) if len(post) else np.nan, ci=(lo, hi))
        if len(post) < MIN_VERDICT_TRADES:
            label = f"collecting ({len(post)}/{MIN_VERDICT_TRADES} trades after the split)"
        elif hi < 0:
            label = "NOT rescued: net return per trade is below zero; stop the autonomous direction calls"
        elif lo > 0:
            label = "working: net return per trade is above zero"
        else:
            label = "no evidence either way: keep collecting"
        verdict["label"] = label
        print(f"\n== verdict (pre-registered): {label}")
        print(f"  post-split mean net per trade {_pct(verdict['mean'], 3)}  90% CI [{_pct(lo, 3)}, {_pct(hi, 3)}]")
    return verdict


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--split", help="UTC time the changes went live; trades opened after it form the 'post' group")
    parser.add_argument("--since", help="only trades opened at/after this UTC time")
    parser.add_argument("--csv", type=Path, help="write the per-trade table here")
    parser.add_argument("--no-journal", action="store_true", help="skip the journal scan")
    args = parser.parse_args(argv)

    trades = load_trades()
    if args.since:
        trades = trades[trades["opened"] >= _utc(args.since)] if not trades.empty else trades
    if trades.empty:
        print("no closed agent trades in the selected window")
        return 0
    trades = add_market_context(trades)
    split = _utc(args.split) if args.split else None
    gates = gate_counts(journals_since(split), split) if split is not None and not args.no_journal else None
    report(trades, split, gates)
    if args.csv:
        trades.to_csv(args.csv, index=False)
        print(f"\nper-trade table: {args.csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
