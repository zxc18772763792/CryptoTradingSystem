"""Golden-case audit: did modes A/B/C fire before the biggest pumps, and if not, which gate blocked?

Finds the top pump episodes in the ambush dataset (30d forward return from local
base), then replays each mode's entry gates bar-by-bar over the 30 days leading
into the pump and reports, per gate, the fraction of bars it blocked. This
separates "thesis wrong" from "threshold wrong".
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

import importlib.util

spec = importlib.util.spec_from_file_location("bt", SCRIPT_DIR / "backtest_ambush_modes.py")
bt = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bt)

from strategies import (  # noqa: E402
    AccumulationAmbushStrategy,
    IgnitionFastFollowStrategy,
    SqueezeFuelStrategy,
)


def find_pump_episodes(frames, top_n=12):
    episodes = []
    for base, frame in frames.items():
        close = pd.to_numeric(frame["close"], errors="coerce").dropna()
        if len(close) < 24 * 40:
            continue
        daily = close.resample("1D").last().dropna()
        fwd30 = daily.shift(-30) / daily - 1.0
        best_start = fwd30.idxmax()
        best_ret = float(fwd30.max())
        if best_ret > 1.0:
            episodes.append({"base": base, "start": best_start, "fwd30_ret": best_ret})
    episodes.sort(key=lambda e: e["fwd30_ret"], reverse=True)
    return episodes[:top_n]


def audit_mode_A(frame, window):
    p = AccumulationAmbushStrategy.default_params()
    oi = pd.to_numeric(frame["oi_usd"], errors="coerce")
    mcap = pd.to_numeric(frame["mcap_usd"], errors="coerce")
    close = pd.to_numeric(frame["close"], errors="coerce")
    funding = pd.to_numeric(frame["funding_rate"], errors="coerce")
    rise = oi / oi.shift(int(p["oi_rise_bars"])) - 1.0
    move = (close / close.shift(int(p["oi_rise_bars"])) - 1.0).abs()
    favg = funding.rolling(int(p["funding_avg_bars"]), min_periods=24).mean()
    ratio = oi / mcap
    gates = {
        "mcap_band": (mcap >= p["mcap_min_usd"]) & (mcap <= p["mcap_max_usd"]),
        "oi_mcap>=0.15": ratio >= p["oi_mcap_min"],
        "oi_rise>=25%": rise >= p["oi_rise_min"],
        "price_flat<=10%": move <= p["price_flat_max"],
        "funding_avg<=0.005%": favg <= p["funding_avg_max"],
    }
    return _gate_pass_rates(gates, window)


def audit_mode_B(frame, window):
    p = SqueezeFuelStrategy.default_params()
    oi = pd.to_numeric(frame["oi_usd"], errors="coerce")
    mcap = pd.to_numeric(frame["mcap_usd"], errors="coerce")
    close = pd.to_numeric(frame["close"], errors="coerce")
    high = pd.to_numeric(frame["high"], errors="coerce")
    funding = pd.to_numeric(frame["funding_rate"], errors="coerce")
    rise = oi / oi.shift(int(p["oi_rise_bars"])) - 1.0
    prior_high = high.rolling(int(p["breakout_bars"])).max().shift(1)
    gates = {
        "mcap_band": (mcap >= p["mcap_min_usd"]) & (mcap <= p["mcap_max_usd"]),
        "oi_mcap>=0.30": (oi / mcap) >= p["oi_mcap_min"],
        "funding<=-0.05%": funding <= p["funding_max"],
        "oi_rise3d>=15%": rise >= p["oi_rise_min"],
        "breakout_24h": close > prior_high,
    }
    return _gate_pass_rates(gates, window)


def audit_mode_C(frame, window):
    p = IgnitionFastFollowStrategy.default_params()
    oi = pd.to_numeric(frame["oi_usd"], errors="coerce")
    mcap = pd.to_numeric(frame["mcap_usd"], errors="coerce")
    close = pd.to_numeric(frame["close"], errors="coerce")
    volume = pd.to_numeric(frame["volume"], errors="coerce")
    ret1 = close.pct_change()
    zwin = int(p["volume_z_window"])
    vz = (volume - volume.rolling(zwin).mean()) / volume.rolling(zwin).std()
    high7 = close.rolling(int(p["breakout_bars"])).max().shift(1)
    oi_jump = oi / oi.shift(int(p["oi_jump_bars"])) - 1.0
    runup = close / close.shift(48) - 1.0
    gates = {
        "mcap_band": (mcap >= p["mcap_min_usd"]) & (mcap <= p["mcap_max_usd"]),
        "oi_mcap>=0.15": (oi / mcap) >= p["oi_mcap_min"],
        "ret1h>=6%": ret1 >= p["ret_1h_min"],
        "vol_z>=4": vz >= p["volume_z_min"],
        "break_7d": close > high7,
        "oi_jump4h>=8%": oi_jump >= p["oi_jump_min"],
        "runup48h<80%": runup < p["max_runup_48h"],
    }
    return _gate_pass_rates(gates, window)


def _gate_pass_rates(gates, window):
    out = {}
    joint = None
    for name, series in gates.items():
        s = series.loc[window[0] : window[1]].fillna(False)
        out[name] = float(s.mean())
        joint = s if joint is None else (joint & s)
    out["ALL_GATES_JOINT"] = float(joint.mean()) if joint is not None else 0.0
    out["JOINT_BARS"] = int(joint.sum()) if joint is not None else 0
    return out


def main():
    bases = sorted(p.stem for p in (bt.DATA_DIR / "klines_1h").glob("*.parquet"))
    frames = {}
    for base in bases:
        frame = bt.load_enriched_frame(base)
        if frame is None:
            continue
        if pd.to_numeric(frame["mcap_usd"], errors="coerce").notna().sum() < 24:
            continue
        if pd.to_numeric(frame["oi_usd"], errors="coerce").notna().sum() < 24:
            continue
        frames[base] = frame

    episodes = find_pump_episodes(frames)
    print(f"top pump episodes (30d forward return > 100%): {len(episodes)}")
    for ep in episodes:
        base = ep["base"]
        frame = frames[base]
        start = ep["start"]
        window = (start - pd.Timedelta(days=30), start + pd.Timedelta(days=5))
        print(f"\n=== {base}: pump from {start.date()} (+{ep['fwd30_ret']*100:.0f}% in 30d) ===")
        for mode, fn in (("A", audit_mode_A), ("B", audit_mode_B), ("C", audit_mode_C)):
            rates = fn(frame, window)
            joint = rates.pop("ALL_GATES_JOINT")
            joint_bars = rates.pop("JOINT_BARS")
            blockers = sorted(rates.items(), key=lambda kv: kv[1])
            desc = ", ".join(f"{k}={v*100:.0f}%" for k, v in blockers)
            print(f"  {mode}: joint={joint*100:.1f}% ({joint_bars} bars)  | {desc}")


if __name__ == "__main__":
    main()
