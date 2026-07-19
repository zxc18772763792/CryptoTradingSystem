"""15-minute event lab: ignition anatomy, listing anatomy, bounce-short test.

Round-3 exploration at intraday granularity. Three studies share one event
framework: detect events on the 1h enriched frames, fetch/cache a 15m window
per event from Binance fapi, analyse at 15m resolution.

Studies
  ignition : loosened ignition events (ret_1h>=5%, vol_z>=3, 3d-high break,
             small-cap structure). Forward decay curve by hour, session
             heatmap, and the FIRST-PULLBACK long (stop under pullback low —
             structural risk 3-8% instead of the 20% stops that killed A/B/C).
  listing  : first 21 days at 15m for every in-window perp listing. Where does
             the fade actually start (hour grid), short-entry timing grid.
  bounce   : after a dump leg (-30% off 48h high on a previously pumped coin),
             SHORT THE RIP (+12% bounce stall) instead of the breakdown —
             inverts the round-2 entry that got squeezed. Funding included.

Costs everywhere: 5bps fee + 10bps slip per side. IS/OOS split 2026-03-18.
Output: reports/ambush_modes_2026-07-18/intraday_15m_lab.json + stdout tables.
"""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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
CACHE_DIR = bt.DATA_DIR / "klines_15m_events"
OUT_DIR = PROJECT_ROOT / "reports" / "ambush_modes_2026-07-18"
OOS_SPLIT = pd.Timestamp("2026-03-18")
FEE, SLIP = 0.0005, 0.0010
COST_RT = 2 * (FEE + SLIP)
SESSION = requests.Session()
SESSION.headers["User-Agent"] = "crypto-trading-system-research/1.0"

MAX_EVENTS_PER_STUDY = 600


# ── shared: fetch/cache 15m windows ─────────────────────────────────────────

def fetch_15m(symbol: str, start: pd.Timestamp, end: pd.Timestamp, tag: str) -> Optional[pd.DataFrame]:
    key = f"{symbol}_{tag}_{start:%Y%m%d%H%M}"
    cache = CACHE_DIR / f"{key}.parquet"
    if cache.exists():
        return pd.read_parquet(cache)
    frames: List[pd.DataFrame] = []
    cursor = int(start.tz_localize(timezone.utc).timestamp() * 1000)
    end_ms = int(end.tz_localize(timezone.utc).timestamp() * 1000)
    while cursor < end_ms:
        for attempt in range(4):
            try:
                resp = SESSION.get(
                    f"{FAPI}/fapi/v1/klines",
                    params={"symbol": symbol, "interval": "15m", "startTime": cursor, "endTime": end_ms, "limit": 1500},
                    timeout=25,
                )
                if resp.status_code in (403, 429):
                    time.sleep(12.0 * (attempt + 1))
                    continue
                resp.raise_for_status()
                batch = resp.json()
                break
            except Exception:  # noqa: BLE001
                time.sleep(3.0 * (attempt + 1))
        else:
            return None
        if not batch:
            break
        idx = pd.to_datetime([int(r[0]) for r in batch], unit="ms", utc=True).tz_localize(None)
        frames.append(
            pd.DataFrame(
                {
                    "open": [float(r[1]) for r in batch],
                    "high": [float(r[2]) for r in batch],
                    "low": [float(r[3]) for r in batch],
                    "close": [float(r[4]) for r in batch],
                    "volume": [float(r[7]) for r in batch],
                },
                index=idx,
            )
        )
        cursor = int(batch[-1][0]) + 900_000
        time.sleep(0.2)
        if len(batch) < 1500:
            break
    if not frames:
        return None
    out = pd.concat(frames)
    out = out[~out.index.duplicated(keep="last")].sort_index()
    out.to_parquet(cache)
    return out


def load_funding_raw(base: str) -> Optional[pd.Series]:
    path = bt.DATA_DIR / "funding" / f"{base}.parquet"
    if not path.exists():
        return None
    df = pd.read_parquet(path)
    series = pd.to_numeric(df["funding_rate"], errors="coerce").dropna()
    return series if len(series) else None


def stat(vals: List[float]) -> Dict[str, Any]:
    if not vals:
        return {"n": 0}
    arr = np.array(vals, dtype=float)
    wins = arr[arr > 0]
    losses = arr[arr <= 0]
    return {
        "n": int(len(arr)),
        "win": round(float((arr > 0).mean()), 3),
        "avg": round(float(arr.mean()), 4),
        "median": round(float(np.median(arr)), 4),
        "best": round(float(arr.max()), 4),
        "worst": round(float(arr.min()), 4),
        "pf": round(float(wins.sum() / abs(losses.sum())), 3) if len(losses) and losses.sum() != 0 else None,
    }


def split_stats(trades: List[Dict[str, Any]], key: str = "ret") -> Dict[str, Any]:
    full = [t[key] for t in trades]
    is_ = [t[key] for t in trades if pd.Timestamp(t["ts"]) < OOS_SPLIT]
    oos = [t[key] for t in trades if pd.Timestamp(t["ts"]) >= OOS_SPLIT]
    return {"full": stat(full), "is": stat(is_), "oos": stat(oos)}


# ── event detection on 1h frames ────────────────────────────────────────────

def detect_ignitions(frame: pd.DataFrame) -> List[pd.Timestamp]:
    close = pd.to_numeric(frame["close"], errors="coerce")
    volume = pd.to_numeric(frame["volume"], errors="coerce")
    mcap = pd.to_numeric(frame["mcap_usd"], errors="coerce")
    oi = pd.to_numeric(frame["oi_usd"], errors="coerce")
    ret_1h = close.pct_change()
    vol_mean = volume.rolling(720, min_periods=60).mean()
    vol_std = volume.rolling(720, min_periods=60).std()
    vol_z = (volume - vol_mean) / vol_std
    high_3d = close.rolling(72).max().shift(1)
    mask = (
        (ret_1h >= 0.05)
        & (vol_z >= 3.0)
        & (close > high_3d)
        & (mcap >= 5e6) & (mcap <= 500e6)
        & ((oi / mcap) >= 0.03)
    ).fillna(False)
    events: List[pd.Timestamp] = []
    last: Optional[pd.Timestamp] = None
    for ts, hit in zip(frame.index, mask.to_numpy()):
        if hit and (last is None or (ts - last).total_seconds() >= 48 * 3600):
            events.append(ts)
            last = ts
    return events


def detect_dump_legs(frame: pd.DataFrame) -> List[pd.Timestamp]:
    close = pd.to_numeric(frame["close"], errors="coerce")
    high_48h = close.rolling(48).max().shift(1)
    runup_30d = close.rolling(720, min_periods=120).max() / close.rolling(720, min_periods=120).min() - 1.0
    mask = ((close <= high_48h * 0.70) & (runup_30d >= 0.50)).fillna(False)
    events: List[pd.Timestamp] = []
    last: Optional[pd.Timestamp] = None
    for ts, hit in zip(frame.index, mask.to_numpy()):
        if hit and (last is None or (ts - last).total_seconds() >= 96 * 3600):
            events.append(ts)
            last = ts
    return events


# ── study 1: ignition anatomy ───────────────────────────────────────────────

def study_ignition(frames: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    events: List[Tuple[str, pd.Timestamp]] = []
    for base, frame in frames.items():
        for ts in detect_ignitions(frame):
            events.append((base, ts))
    events.sort(key=lambda e: e[1])
    if len(events) > MAX_EVENTS_PER_STUDY:
        step = len(events) / MAX_EVENTS_PER_STUDY
        events = [events[int(i * step)] for i in range(MAX_EVENTS_PER_STUDY)]
    logger.info(f"ignition events: {len(events)}")

    horizon_trades: Dict[int, List[Dict[str, Any]]] = {1: [], 2: [], 4: [], 8: [], 24: [], 48: []}
    pullback_trades: List[Dict[str, Any]] = []
    hour_buckets: Dict[int, List[float]] = {h: [] for h in range(24)}

    for i, (base, ts) in enumerate(events):
        signal_close = ts + pd.Timedelta(hours=1)
        frame_15m = fetch_15m(f"{base}USDT", ts - pd.Timedelta(hours=24), ts + pd.Timedelta(hours=72), "ign")
        if frame_15m is None or len(frame_15m) < 40:
            continue
        path = frame_15m[frame_15m.index >= signal_close]
        if len(path) < 8:
            continue
        entry = float(path["close"].iloc[0]) * (1 + SLIP)
        closes = path["close"]

        for hours, bucket in horizon_trades.items():
            j = hours * 4
            if len(closes) > j:
                ret = float(closes.iloc[j]) * (1 - SLIP) / entry - 1.0 - 2 * FEE
                bucket.append({"ts": str(ts), "ret": ret})

        ret_4h_idx = min(16, len(closes) - 1)
        hour_buckets[signal_close.hour].append(float(closes.iloc[ret_4h_idx]) / entry - 1.0)

        # first-pullback long: retrace >=1/3 of the ignition leg within 8h,
        # then a 15m close back above the prior 15m high -> enter; stop under
        # pullback low; target 2R; time stop 24h from entry.
        ign_bar = frames[base].loc[ts]
        leg_low = float(pd.to_numeric(ign_bar["open"], errors="coerce"))
        leg_high = float(path["high"].iloc[0])
        leg = max(leg_high - leg_low, 1e-12)
        pull_zone = leg_high - leg / 3.0
        window = path.iloc[1 : 8 * 4]
        state, pull_low, prev_high = "wait_pull", None, None
        entry2 = stop2 = None
        entry2_idx = None
        for k in range(len(window)):
            bar = window.iloc[k]
            if state == "wait_pull":
                if float(bar["low"]) <= pull_zone:
                    state, pull_low, prev_high = "in_pull", float(bar["low"]), float(bar["high"])
            elif state == "in_pull":
                pull_low = min(pull_low, float(bar["low"]))
                if float(bar["close"]) > prev_high:
                    entry2 = float(bar["close"]) * (1 + SLIP)
                    stop2 = pull_low
                    entry2_idx = k
                    break
                prev_high = float(bar["high"])
        if entry2 is not None and stop2 is not None and entry2 > stop2:
            risk = (entry2 - stop2) / entry2
            if 0.01 <= risk <= 0.15:
                target = entry2 * (1 + 2 * risk)
                deadline_idx = entry2_idx + 24 * 4
                seg = window.iloc[entry2_idx + 1 :]
                exit_ret = None
                for k2 in range(len(seg)):
                    bar2 = seg.iloc[k2]
                    if float(bar2["low"]) <= stop2:
                        fill = min(float(bar2["open"]), stop2)
                        exit_ret = fill * (1 - SLIP) / entry2 - 1.0 - 2 * FEE
                        break
                    if float(bar2["high"]) >= target:
                        exit_ret = target * (1 - SLIP) / entry2 - 1.0 - 2 * FEE
                        break
                    if entry2_idx + 1 + k2 >= deadline_idx:
                        exit_ret = float(bar2["close"]) * (1 - SLIP) / entry2 - 1.0 - 2 * FEE
                        break
                if exit_ret is None and len(seg):
                    exit_ret = float(seg["close"].iloc[-1]) * (1 - SLIP) / entry2 - 1.0 - 2 * FEE
                if exit_ret is not None:
                    pullback_trades.append({"ts": str(ts), "base": base, "ret": exit_ret, "risk": round(risk, 4)})
        if i % 100 == 0:
            logger.info(f"ignition {i + 1}/{len(events)}")

    return {
        "events": len(events),
        "hold_by_hours": {str(h): split_stats(tr) for h, tr in horizon_trades.items()},
        "first_pullback_2R": split_stats(pullback_trades),
        "first_pullback_n_by_risk": stat([t["risk"] for t in pullback_trades]),
        "session_fwd4h_median": {
            str(h): round(float(np.median(v)), 4) for h, v in hour_buckets.items() if len(v) >= 5
        },
    }


# ── study 2: listing anatomy at 15m ────────────────────────────────────────

def study_listing(manifest: Dict[str, Any]) -> Dict[str, Any]:
    rows = manifest.get("symbols") or {}
    listings: List[Tuple[str, pd.Timestamp]] = []
    for base, meta in rows.items():
        onboard_ms = int(meta.get("onboard") or 0)
        if onboard_ms <= 0:
            continue
        onboard = pd.Timestamp(onboard_ms, unit="ms")
        kl_path = bt.DATA_DIR / "klines_1h" / f"{base}.parquet"
        if not kl_path.exists():
            continue
        kl_index = pd.read_parquet(kl_path, columns=["close"]).index
        if not len(kl_index) or abs((kl_index[0] - onboard).total_seconds()) > 3 * 86400:
            continue
        listings.append((base, onboard))
    logger.info(f"listings: {len(listings)}")

    rel_paths: List[Dict[str, Any]] = []
    entry_grid: Dict[int, List[Dict[str, Any]]] = {4: [], 8: [], 12: [], 24: [], 48: []}
    for base, onboard in listings:
        frame_15m = fetch_15m(f"{base}USDT", onboard, onboard + pd.Timedelta(days=15), "list")
        if frame_15m is None or len(frame_15m) < 96:
            continue
        closes = frame_15m["close"]
        base_price = float(closes.iloc[0])
        if base_price <= 0:
            continue
        rel = closes / base_price - 1.0
        hours_rel = {}
        for h in (1, 2, 4, 8, 12, 24, 48, 96, 168, 336):
            j = h * 4
            if len(rel) > j:
                hours_rel[str(h)] = round(float(rel.iloc[j]), 4)
        peak_idx = int(np.argmax(frame_15m["high"].iloc[: 4 * 96].to_numpy()))
        rel_paths.append(
            {
                "base": base,
                "onboard": str(onboard),
                "hours_rel": hours_rel,
                "peak_hour_within_4d": round(peak_idx / 4.0, 1),
            }
        )

        funding = load_funding_raw(base)
        for entry_h, bucket in entry_grid.items():
            j = entry_h * 4
            if len(closes) <= j + 8:
                continue
            entry = float(closes.iloc[j]) * (1 - SLIP)
            entry_ts = frame_15m.index[j]
            cover_idx = min(len(closes) - 1, (14 * 24) * 4)
            seg = frame_15m.iloc[j + 1 : cover_idx + 1]
            exit_price, exit_ts = None, None
            for ts2, bar in seg.iterrows():
                if float(bar["high"]) >= entry / (1 - SLIP) * 1.25:
                    exit_price = entry / (1 - SLIP) * 1.25
                    exit_ts = ts2
                    break
            if exit_price is None and len(seg):
                exit_price, exit_ts = float(seg["close"].iloc[-1]), seg.index[-1]
            if exit_price is None:
                continue
            gross = (entry - exit_price * (1 + SLIP)) / entry
            funding_ret = 0.0
            if funding is not None:
                fw = funding[(funding.index > entry_ts) & (funding.index <= exit_ts)]
                funding_ret = float(fw.sum())
            bucket.append({"ts": str(onboard), "ret": gross + funding_ret - 2 * FEE})

    return {
        "listings_with_15m": len(rel_paths),
        "median_rel_by_hour": {
            h: round(float(np.median([p["hours_rel"][h] for p in rel_paths if h in p["hours_rel"]])), 4)
            for h in ("1", "2", "4", "8", "12", "24", "48", "96", "168", "336")
        },
        "peak_hour_median": round(float(np.median([p["peak_hour_within_4d"] for p in rel_paths])), 1) if rel_paths else None,
        "short_entry_grid_cover14d": {f"H+{h}": split_stats(tr) for h, tr in entry_grid.items()},
    }


# ── study 3: bounce-short (sell the rip) ────────────────────────────────────

def study_bounce(frames: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    events: List[Tuple[str, pd.Timestamp]] = []
    for base, frame in frames.items():
        for ts in detect_dump_legs(frame):
            events.append((base, ts))
    events.sort(key=lambda e: e[1])
    if len(events) > MAX_EVENTS_PER_STUDY:
        step = len(events) / MAX_EVENTS_PER_STUDY
        events = [events[int(i * step)] for i in range(MAX_EVENTS_PER_STUDY)]
    logger.info(f"dump-leg events: {len(events)}")

    trades: List[Dict[str, Any]] = []
    no_setup = 0
    for i, (base, ts) in enumerate(events):
        frame_15m = fetch_15m(f"{base}USDT", ts, ts + pd.Timedelta(days=7), "dump")
        if frame_15m is None or len(frame_15m) < 60:
            continue
        funding = load_funding_raw(base)
        lows = frame_15m["low"]
        dump_low = float(lows.iloc[: 8 * 4].min())
        entered = False
        state, bounce_high, prev_low = "wait_bounce", None, None
        for k in range(8 * 4, len(frame_15m) - 1):
            bar = frame_15m.iloc[k]
            price = float(bar["close"])
            if state == "wait_bounce":
                if price >= dump_low * 1.12:
                    state, bounce_high, prev_low = "in_bounce", float(bar["high"]), float(bar["low"])
            elif state == "in_bounce":
                bounce_high = max(bounce_high, float(bar["high"]))
                if price < prev_low:
                    entry = price * (1 - SLIP)
                    entry_ts = frame_15m.index[k]
                    stop = bounce_high * 1.02
                    target = dump_low
                    seg = frame_15m.iloc[k + 1 :]
                    deadline = entry_ts + pd.Timedelta(days=5)
                    exit_price = exit_ts = None
                    reason = None
                    for ts2, bar2 in seg.iterrows():
                        if ts2 > deadline:
                            exit_price, exit_ts, reason = float(bar2["open"]), ts2, "time"
                            break
                        if float(bar2["high"]) >= stop:
                            exit_price = max(float(bar2["open"]), stop)
                            exit_ts, reason = ts2, "stop"
                            break
                        if float(bar2["low"]) <= target:
                            exit_price, exit_ts, reason = target, ts2, "target"
                            break
                    if exit_price is None and len(seg):
                        exit_price, exit_ts, reason = float(seg["close"].iloc[-1]), seg.index[-1], "data_end"
                    if exit_price is not None:
                        gross = (entry - exit_price * (1 + SLIP)) / entry
                        funding_ret = 0.0
                        if funding is not None:
                            fw = funding[(funding.index > entry_ts) & (funding.index <= exit_ts)]
                            funding_ret = float(fw.sum())
                        trades.append(
                            {"ts": str(ts), "base": base, "ret": gross + funding_ret - 2 * FEE, "reason": reason}
                        )
                        entered = True
                    break
                prev_low = float(bar["low"])
        if not entered:
            no_setup += 1
        if i % 100 == 0:
            logger.info(f"bounce {i + 1}/{len(events)}")

    reasons: Dict[str, int] = {}
    for t in trades:
        reasons[t["reason"]] = reasons.get(t["reason"], 0) + 1
    return {"events": len(events), "no_setup": no_setup, "trades": split_stats(trades), "exit_reasons": reasons}


def main() -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    frames: Dict[str, pd.DataFrame] = {}
    for path in sorted((bt.DATA_DIR / "klines_1h").glob("*.parquet")):
        base = path.stem
        frame = bt.load_enriched_frame(base)
        if frame is None:
            continue
        if pd.to_numeric(frame["mcap_usd"], errors="coerce").notna().sum() < 24:
            continue
        frames[base] = frame
    logger.info(f"usable symbols: {len(frames)}")
    manifest = json.loads((bt.DATA_DIR / "manifest.json").read_text(encoding="utf-8"))

    results = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "oos_split": str(OOS_SPLIT.date()),
        "ignition": study_ignition(frames),
        "listing": study_listing(manifest),
        "bounce_short": study_bounce(frames),
    }
    (OUT_DIR / "intraday_15m_lab.json").write_text(json.dumps(results, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(results, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
