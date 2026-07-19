"""Pump-decay SHORT study: is the bleed after completed pumps tradeable?

Evidence motivating this: every post-pump long entry in the ambush backtest
lost; the equal-weight universe bled -28%/yr; Odaily's 221-coin study found
shorting the abandonment point was the only viable path. This script tests it
on our dataset with honest squeeze risk (gap-through stops on re-ignitions)
and funding carry (crowded post-pump longs pay shorts).

Entry: 30d runup >= +100% happened, then price first closes >= X% below the
30d peak (decay confirmation). Variants add OI/mcap and funding-carry filters.
Short replay on 1h bars: target -30%, squeeze stop +20% (fill at open when
gapped through), trailing 15% off the low-water mark, time stop 21d, costs
5bps fee + 10bps slip per side, funding settled from the raw 8h series.

Output: reports/ambush_modes_2026-07-18/decay_short_study.{json,md} + stdout.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from loguru import logger

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

import importlib.util

spec = importlib.util.spec_from_file_location("bt", SCRIPT_DIR / "backtest_ambush_modes.py")
bt = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bt)

OUT_DIR = PROJECT_ROOT / "reports" / "ambush_modes_2026-07-18"
OOS_SPLIT = pd.Timestamp("2026-03-18")

RUNUP_MIN = 1.00          # completed pump: 30d runup >= +100%
COOLDOWN_DAYS = 14
TARGET_PCT = 0.30         # cover at -30%
SQUEEZE_STOP_PCT = 0.20   # stop if price +20% against the short
TRAIL_PCT = 0.15          # trail off low-water
TIME_STOP_DAYS = 21
FEE, SLIP = 0.0005, 0.0010

VARIANTS = {
    "V1_retrace20": {"retrace": 0.20, "oi_mcap_min": None, "funding_min": None},
    "V2_oi_mcap": {"retrace": 0.20, "oi_mcap_min": 0.10, "funding_min": None},
    "V3_carry": {"retrace": 0.20, "oi_mcap_min": 0.10, "funding_min": 0.0},
    "V4_deep30": {"retrace": 0.30, "oi_mcap_min": 0.10, "funding_min": 0.0},
    # Round 2 diagnostics: the round-1 killer was the 15% trail being clipped
    # by dead-cat bounces (339/366 trail exits, 1.9d holds) while the random-
    # short control (7.7d holds) sat at PF 1.03 — so test bounce-tolerant exits.
    "V5_no_trail": {"retrace": 0.20, "oi_mcap_min": 0.10, "funding_min": 0.0, "trail": None},
    "V6_deep_no_trail": {"retrace": 0.30, "oi_mcap_min": 0.10, "funding_min": 0.0, "trail": None},
}


def load_funding_raw(base: str) -> Optional[pd.Series]:
    path = bt.DATA_DIR / "funding" / f"{base}.parquet"
    if not path.exists():
        return None
    df = pd.read_parquet(path)
    series = pd.to_numeric(df["funding_rate"], errors="coerce").dropna()
    return series if len(series) else None


def detect_decay_events(frame: pd.DataFrame, cfg: Dict[str, Any]) -> List[pd.Timestamp]:
    close = pd.to_numeric(frame["close"], errors="coerce")
    oi = pd.to_numeric(frame["oi_usd"], errors="coerce")
    mcap = pd.to_numeric(frame["mcap_usd"], errors="coerce")
    funding = pd.to_numeric(frame["funding_rate"], errors="coerce")

    peak_30d = close.rolling(24 * 30, min_periods=24 * 5).max()
    base_30d = close.rolling(24 * 30, min_periods=24 * 5).min()
    runup = peak_30d / base_30d - 1.0
    pumped = runup >= RUNUP_MIN
    retraced = close <= peak_30d * (1.0 - cfg["retrace"])

    mask = pumped & retraced
    if cfg.get("oi_mcap_min") is not None:
        mask &= (oi / mcap) >= cfg["oi_mcap_min"]
    if cfg.get("funding_min") is not None:
        funding_7d = funding.rolling(24 * 7, min_periods=24).mean()
        mask &= funding_7d >= cfg["funding_min"]
    mask = mask.fillna(False)

    events: List[pd.Timestamp] = []
    last: Optional[pd.Timestamp] = None
    prev = False
    for ts, hit in zip(frame.index, mask.to_numpy()):
        if hit and not prev:
            if last is None or (ts - last).total_seconds() >= COOLDOWN_DAYS * 86400:
                events.append(ts)
                last = ts
        prev = hit
    return events


def replay_short(
    frame: pd.DataFrame,
    funding_raw: Optional[pd.Series],
    signal_ts: pd.Timestamp,
    trail_pct: Optional[float] = TRAIL_PCT,
) -> Optional[Dict[str, Any]]:
    path = frame[frame.index > signal_ts]
    if len(path) < 4:
        return None
    entry_price = float(pd.to_numeric(path["close"], errors="coerce").iloc[0]) * (1 - SLIP)
    if not np.isfinite(entry_price) or entry_price <= 0:
        return None
    entry_ts = path.index[0]

    target = entry_price * (1 - TARGET_PCT)
    stop = entry_price * (1 + SQUEEZE_STOP_PCT)
    low_water = entry_price
    deadline = entry_ts + pd.Timedelta(days=TIME_STOP_DAYS)

    exit_price = exit_reason = exit_ts = None
    for ts, bar in path.iloc[1:].iterrows():
        high = float(bar["high"])
        low = float(bar["low"])
        open_ = float(bar["open"])
        close = float(bar["close"])
        if not all(np.isfinite(v) for v in (high, low, open_, close)):
            continue
        if ts > deadline:
            exit_price, exit_reason, exit_ts = open_, "time_stop", ts
            break
        low_water = min(low_water, low)
        # squeeze stop first (engine-style sl_first), gap-through at open
        if high >= stop:
            exit_price = max(open_, stop) if open_ > stop else stop
            exit_reason, exit_ts = "squeeze_stop", ts
            break
        if trail_pct is not None:
            trail = low_water * (1 + trail_pct)
            if high >= trail:
                exit_price = max(open_, trail) if open_ > trail else trail
                exit_reason, exit_ts = "trailing_stop", ts
                break
        if low <= target:
            exit_price, exit_reason, exit_ts = target, "target", ts
            break
    if exit_price is None:
        exit_price, exit_reason, exit_ts = float(path["close"].iloc[-1]), "data_end", path.index[-1]

    exit_price *= 1 + SLIP
    gross = (entry_price - exit_price) / entry_price

    funding_ret = 0.0
    if funding_raw is not None:
        window = funding_raw[(funding_raw.index > entry_ts) & (funding_raw.index <= exit_ts)]
        # positive funding: longs pay shorts -> short RECEIVES the rate
        funding_ret = float(window.sum())

    net = gross + funding_ret - 2 * FEE
    return {
        "signal": str(signal_ts),
        "entry_time": str(entry_ts),
        "exit_time": str(exit_ts),
        "hold_days": round((exit_ts - entry_ts).total_seconds() / 86400.0, 2),
        "ret_pct": round(net, 5),
        "gross_ret": round(gross, 5),
        "funding_ret": round(funding_ret, 5),
        "exit_reason": exit_reason,
    }


def summarize(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not trades:
        return {"n": 0}
    rets = np.array([t["ret_pct"] for t in trades])
    wins = rets[rets > 0]
    losses = rets[rets <= 0]
    reasons: Dict[str, int] = {}
    for t in trades:
        reasons[t["exit_reason"]] = reasons.get(t["exit_reason"], 0) + 1
    return {
        "n": int(len(rets)),
        "win_rate": round(float(len(wins) / len(rets)), 3),
        "avg_ret": round(float(rets.mean()), 4),
        "median_ret": round(float(np.median(rets)), 4),
        "avg_funding": round(float(np.mean([t["funding_ret"] for t in trades])), 4),
        "best": round(float(rets.max()), 4),
        "worst": round(float(rets.min()), 4),
        "profit_factor": round(float(wins.sum() / abs(losses.sum())), 3) if len(losses) and losses.sum() != 0 else None,
        "avg_hold_days": round(float(np.mean([t["hold_days"] for t in trades])), 1),
        "exit_reasons": reasons,
    }


def main() -> None:
    frames: Dict[str, pd.DataFrame] = {}
    fundings: Dict[str, Optional[pd.Series]] = {}
    for path in sorted((bt.DATA_DIR / "klines_1h").glob("*.parquet")):
        base = path.stem
        frame = bt.load_enriched_frame(base)
        if frame is None:
            continue
        if pd.to_numeric(frame["mcap_usd"], errors="coerce").notna().sum() < 24:
            continue
        frames[base] = frame
        fundings[base] = load_funding_raw(base)
    logger.info(f"usable symbols: {len(frames)}")

    results: Dict[str, Any] = {}
    all_trades: Dict[str, List[Dict[str, Any]]] = {}
    for name, cfg in VARIANTS.items():
        trades: List[Dict[str, Any]] = []
        variant_trail = cfg.get("trail", TRAIL_PCT)
        for base, frame in frames.items():
            for ts in detect_decay_events(frame, cfg):
                trade = replay_short(frame, fundings.get(base), ts, trail_pct=variant_trail)
                if trade:
                    trades.append({"base": base, **trade})
        trades.sort(key=lambda t: t["entry_time"])
        is_trades = [t for t in trades if pd.Timestamp(t["entry_time"]) < OOS_SPLIT]
        oos_trades = [t for t in trades if pd.Timestamp(t["entry_time"]) >= OOS_SPLIT]
        results[name] = {
            "config": cfg,
            "full": summarize(trades),
            "is": summarize(is_trades),
            "oos": summarize(oos_trades),
        }
        all_trades[name] = trades
        logger.info(f"{name}: n={len(trades)}")

    # no-skill control: short every symbol at random monthly points (same replay)
    rng = np.random.default_rng(7)
    control_trades: List[Dict[str, Any]] = []
    for base, frame in frames.items():
        if len(frame) < 24 * 40:
            continue
        candidates = frame.index[24 * 35 :: 24 * 21]
        for ts in candidates:
            if rng.random() < 0.5:
                trade = replay_short(frame, fundings.get(base), ts)
                if trade:
                    control_trades.append({"base": base, **trade})
    results["CONTROL_random_short"] = {"config": {}, "full": summarize(control_trades)}

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "oos_split": str(OOS_SPLIT.date()),
        "replay": {
            "target_pct": TARGET_PCT, "squeeze_stop_pct": SQUEEZE_STOP_PCT,
            "trail_pct": TRAIL_PCT, "time_stop_days": TIME_STOP_DAYS,
            "fee": FEE, "slip": SLIP, "runup_min": RUNUP_MIN,
        },
        "results": results,
    }
    (OUT_DIR / "decay_short_study.json").write_text(
        json.dumps({**payload, "trades": all_trades}, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    print(json.dumps(payload, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
