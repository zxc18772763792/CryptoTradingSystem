"""Fair retest of mode C (ignition fast-follow) at 5-minute execution granularity.

The 1h backtest structurally penalises C: the signal closes at T, the engine
fills at T+1h — one full hour late on coins that pump and dump in minutes.
This script reruns C's entries from the SAME vectorised signal definition and
compares two execution variants on identical 5m data:

  fast: enter at the first 5m close after T+5min   (what live execution can do)
  slow: enter at the first 5m close after T+60min  (what the 1h backtest did)

Exits replicate the engine rules on 5m bars: SL -8% (gap-through at open),
TP +25%, trailing 10% (5m high-water), time stop 48h. Costs: taker 5bps/side
+ 10bps slippage per side. Funding ignored (~±0.03% on 2-day holds; noted).

Output: reports/ambush_modes_2026-07-18/ignition_5m_retest.{json,md}
"""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import requests
from loguru import logger

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

import importlib.util

spec = importlib.util.spec_from_file_location("bt", SCRIPT_DIR / "backtest_ambush_modes.py")
bt = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bt)

FAPI = "https://fapi.binance.com"
CACHE_DIR = bt.DATA_DIR / "klines_5m_events"
OUT_DIR = PROJECT_ROOT / "reports" / "ambush_modes_2026-07-18"
SESSION = requests.Session()
SESSION.headers["User-Agent"] = "crypto-trading-system-research/1.0"

# Calibrated mode-C gate set (matches the calibrated backtest variant).
PARAMS = {
    "mcap_min_usd": 5e6,
    "mcap_max_usd": 300e6,
    "oi_mcap_min": 0.05,
    "volume_z_min": 4.0,
    "volume_z_window": 720,
    "ret_1h_min": 0.06,
    "breakout_bars": 168,
    "oi_jump_bars": 4,
    "oi_jump_min": 0.05,
    "max_runup_48h": 0.80,
    "cooldown_bars": 24,
}
STOP_PCT, TP_PCT, TRAIL_PCT = 0.08, 0.25, 0.10
HOLD_HOURS = 48
FEE, SLIP = 0.0005, 0.0010


def detect_signals(frame: pd.DataFrame) -> List[pd.Timestamp]:
    close = pd.to_numeric(frame["close"], errors="coerce")
    volume = pd.to_numeric(frame["volume"], errors="coerce")
    oi = pd.to_numeric(frame["oi_usd"], errors="coerce")
    mcap = pd.to_numeric(frame["mcap_usd"], errors="coerce")

    ret_1h = close.pct_change()
    vol_mean = volume.rolling(PARAMS["volume_z_window"], min_periods=60).mean()
    vol_std = volume.rolling(PARAMS["volume_z_window"], min_periods=60).std()
    vol_z = (volume - vol_mean) / vol_std
    breakout = close > close.rolling(PARAMS["breakout_bars"]).max().shift(1)
    runup = close / close.shift(48) - 1.0
    oi_jump = oi / oi.shift(PARAMS["oi_jump_bars"]) - 1.0
    structure = (
        (mcap >= PARAMS["mcap_min_usd"]) & (mcap <= PARAMS["mcap_max_usd"]) & ((oi / mcap) >= PARAMS["oi_mcap_min"])
    )
    mask = (
        structure
        & (ret_1h >= PARAMS["ret_1h_min"])
        & (vol_z >= PARAMS["volume_z_min"])
        & breakout
        & (runup < PARAMS["max_runup_48h"])
        & (oi_jump >= PARAMS["oi_jump_min"])
    ).fillna(False)

    signals: List[pd.Timestamp] = []
    last: Optional[pd.Timestamp] = None
    for ts in frame.index[mask.to_numpy()]:
        if last is not None and (ts - last).total_seconds() < PARAMS["cooldown_bars"] * 3600:
            continue
        signals.append(ts)
        last = ts
    return signals


def fetch_5m_window(symbol: str, signal_ts: pd.Timestamp) -> Optional[pd.DataFrame]:
    key = f"{symbol}_{signal_ts:%Y%m%d_%H%M}"
    cache_path = CACHE_DIR / f"{key}.parquet"
    if cache_path.exists():
        return pd.read_parquet(cache_path)
    start_ms = int((signal_ts - pd.Timedelta(minutes=30)).tz_localize(timezone.utc).timestamp() * 1000)
    end_ms = int((signal_ts + pd.Timedelta(hours=HOLD_HOURS + 2)).tz_localize(timezone.utc).timestamp() * 1000)
    for attempt in range(4):
        try:
            resp = SESSION.get(
                f"{FAPI}/fapi/v1/klines",
                params={"symbol": symbol, "interval": "5m", "startTime": start_ms, "endTime": end_ms, "limit": 1500},
                timeout=25,
            )
            if resp.status_code in (403, 429):
                time.sleep(15.0 * (attempt + 1))
                continue
            resp.raise_for_status()
            batch = resp.json()
            break
        except Exception:  # noqa: BLE001
            time.sleep(3.0 * (attempt + 1))
    else:
        return None
    if not batch:
        return None
    idx = pd.to_datetime([int(r[0]) for r in batch], unit="ms", utc=True).tz_localize(None)
    frame = pd.DataFrame(
        {
            "open": [float(r[1]) for r in batch],
            "high": [float(r[2]) for r in batch],
            "low": [float(r[3]) for r in batch],
            "close": [float(r[4]) for r in batch],
        },
        index=idx,
    )
    frame.to_parquet(cache_path)
    time.sleep(0.25)
    return frame


def replay(frame_5m: pd.DataFrame, signal_ts: pd.Timestamp, lag_minutes: int) -> Optional[Dict[str, Any]]:
    """Enter at first 5m close at/after signal close + lag; engine-style exits."""
    # signal_ts is the 1h bar OPEN time; the bar closes at signal_ts + 1h.
    signal_close_time = signal_ts + pd.Timedelta(hours=1)
    entry_time_min = signal_close_time + pd.Timedelta(minutes=lag_minutes)
    # a 5m bar indexed t covers [t, t+5m) and is complete at t+5m
    candidates = frame_5m[frame_5m.index + pd.Timedelta(minutes=5) >= entry_time_min]
    if candidates.empty:
        return None
    entry_bar_ts = candidates.index[0]
    entry_price = float(candidates["close"].iloc[0]) * (1 + SLIP)
    if not np.isfinite(entry_price) or entry_price <= 0:
        return None

    stop = entry_price * (1 - STOP_PCT)
    take = entry_price * (1 + TP_PCT)
    anchor = entry_price
    deadline = signal_close_time + pd.Timedelta(hours=HOLD_HOURS)
    path = frame_5m[frame_5m.index > entry_bar_ts]

    exit_price, exit_reason, exit_ts = None, None, None
    for ts, bar in path.iterrows():
        if ts > deadline:
            exit_price, exit_reason, exit_ts = float(bar["open"]), "time_stop", ts
            break
        high, low, open_ = float(bar["high"]), float(bar["low"]), float(bar["open"])
        anchor = max(anchor, high)
        trail = anchor * (1 - TRAIL_PCT)
        if low <= stop:
            exit_price, exit_reason, exit_ts = (min(open_, stop) if open_ < stop else stop), "stop_loss", ts
            break
        if low <= trail:
            exit_price, exit_reason, exit_ts = (min(open_, trail) if open_ < trail else trail), "trailing_stop", ts
            break
        if high >= take:
            exit_price, exit_reason, exit_ts = take, "take_profit", ts
            break
    if exit_price is None:
        exit_price, exit_reason, exit_ts = float(path["close"].iloc[-1]) if len(path) else entry_price, "data_end", path.index[-1] if len(path) else entry_bar_ts

    exit_price *= 1 - SLIP
    gross = exit_price / entry_price - 1.0
    net = gross - 2 * FEE
    return {
        "entry_time": str(entry_bar_ts),
        "exit_time": str(exit_ts),
        "ret_pct": net,
        "exit_reason": exit_reason,
        "hold_hours": round((exit_ts - entry_bar_ts).total_seconds() / 3600.0, 2),
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
        "win_rate": round(float(len(wins) / len(rets)), 4),
        "avg_ret": round(float(rets.mean()), 4),
        "median_ret": round(float(np.median(rets)), 4),
        "best": round(float(rets.max()), 4),
        "worst": round(float(rets.min()), 4),
        "profit_factor": round(float(wins.sum() / abs(losses.sum())), 3) if len(losses) and losses.sum() != 0 else None,
        "exit_reasons": reasons,
    }


def main() -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    bases = sorted(p.stem for p in (bt.DATA_DIR / "klines_1h").glob("*.parquet"))
    all_signals: List[Any] = []
    for base in bases:
        frame = bt.load_enriched_frame(base)
        if frame is None:
            continue
        if pd.to_numeric(frame["mcap_usd"], errors="coerce").notna().sum() < 24:
            continue
        if pd.to_numeric(frame["oi_usd"], errors="coerce").notna().sum() < 24:
            continue
        for ts in detect_signals(frame):
            all_signals.append((base, ts))
    logger.info(f"detected {len(all_signals)} C-mode signals across {len(bases)} symbols")

    fast_trades, slow_trades, missing = [], [], 0
    for i, (base, ts) in enumerate(all_signals):
        frame_5m = fetch_5m_window(f"{base}USDT", ts)
        if frame_5m is None or len(frame_5m) < 30:
            missing += 1
            continue
        fast = replay(frame_5m, ts, lag_minutes=5)
        slow = replay(frame_5m, ts, lag_minutes=60)
        if fast:
            fast_trades.append({"base": base, "signal": str(ts), **fast})
        if slow:
            slow_trades.append({"base": base, "signal": str(ts), **slow})
        if i % 20 == 0:
            logger.info(f"replayed {i + 1}/{len(all_signals)}")

    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "params": PARAMS,
        "signals": len(all_signals),
        "missing_5m_windows": missing,
        "fast_lag_5min": summarize(fast_trades),
        "slow_lag_60min": summarize(slow_trades),
        "note": "同一 5m 数据源对比执行滞后；成本 5bps 费用+10bps 滑点每边；未计资金费（±0.03%/2d 量级）。",
    }
    (OUT_DIR / "ignition_5m_retest.json").write_text(
        json.dumps({**result, "fast_trades": fast_trades, "slow_trades": slow_trades}, ensure_ascii=False, indent=1),
        encoding="utf-8",
    )
    print(json.dumps(result, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
