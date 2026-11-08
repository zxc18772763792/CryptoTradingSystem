"""One-week risk reading for the altcoin radar. A risk reading only: it says nothing about direction.

Every coin gets the decile of its 14-day high-low (Parkinson) volatility inside the day's liquid universe, and each
decile gets the share of its coins that touched a level within the following 7 days, over the last 365 days of
finished windows. docs/RADAR_INDICATOR_USES_2026-11-08.md has the evidence: this volatility predicts next week's
extremes better than the weekly model score or any radar score, and adding them does not help. The 365-day window
was picked on 2023-2024 in scripts/radar_risk_meter_calibration.py; when the market heats up the top decile runs a
few points low (2025 on: 17% predicted vs 22% realized for +40%), and crash weeks hit every coin together.

Daily bars come straight from the local store, so this keeps working without the research panel.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from core.research.xs_daily_panel import EXCLUDED_COINS, MIN_HISTORY_DAYS, PEG_RATIO, UNIVERSE_SIZE

BINANCE_FUTURES = Path(__file__).resolve().parents[2] / "data" / "historical" / "binance_futures"
MAJOR_COINS = {"BTC", "ETH", "BNB", "SOL", "XRP", "DOGE", "ADA", "TRX", "TON"}
VOL_WINDOW_DAYS = 14
CAL_WINDOW_DAYS = 365
HOLD_DAYS = 7
TOUCH_UP, TOUCH_DOWN = 0.40, 0.30
LEVERAGES = (2, 3, 5, 10)
STALE_AFTER_DAYS = 3.0
MIN_COINS_FOR_A_DAY = 50
# Rows kept per coin: calibration + the forward window + what the universe rules look back on.
HISTORY_ROWS = CAL_WINDOW_DAYS + HOLD_DAYS + 120
GROUPS: Tuple[Tuple[str, str, Tuple[int, ...]], ...] = (
    ("calm", "平静", (1, 2, 3)),
    ("middle", "中等", (4, 5, 6, 7)),
    ("hot", "较疯", (8, 9)),
    ("hottest", "最疯", (10,)),
)


def strip_multiplier(base: str) -> str:
    """1000PEPE -> PEPE, 1000000MOG -> MOG, 1MBABYDOGE -> BABYDOGE; other names unchanged."""
    for prefix in ("1000000", "1000", "1M"):
        if base.startswith(prefix) and len(base) > len(prefix):
            return base[len(prefix):]
    return base


def load_daily_bars(folder: Path = BINANCE_FUTURES) -> Dict[str, pd.DataFrame]:
    """Wide high/low/close/volume (USD) frames from <coin>_USDT/1d.parquet, indexed by naive UTC day."""
    series: Dict[str, Dict[str, pd.Series]] = {"high": {}, "low": {}, "close": {}, "volume": {}}
    for path in sorted(Path(folder).glob("*_USDT/1d.parquet")):
        base = path.parent.name[: -len("_USDT")]
        if strip_multiplier(base).upper() in EXCLUDED_COINS:
            continue
        bars = pd.read_parquet(path, columns=["high", "low", "close", "volume"])
        index = pd.DatetimeIndex(bars.index)
        if index.tz is not None:
            index = index.tz_convert("UTC").tz_localize(None)
        bars.index = index.normalize()
        bars = bars[~bars.index.duplicated(keep="last")].sort_index().tail(HISTORY_ROWS).astype(float)
        typical = (bars["high"] + bars["low"] + bars["close"]) / 3.0
        series["high"][base] = bars["high"]
        series["low"][base] = bars["low"]
        series["close"][base] = bars["close"]
        series["volume"][base] = bars["volume"] * typical
    return {name: pd.DataFrame(columns) for name, columns in series.items()}


def parkinson_volatility(high: pd.DataFrame, low: pd.DataFrame, window: int = VOL_WINDOW_DAYS) -> pd.DataFrame:
    daily = np.sqrt((np.log(high.where(high > 0) / low.where(low > 0)) ** 2) / (4.0 * math.log(2.0)))
    return daily.rolling(window, min_periods=max(5, window // 2 + 1)).mean()


def liquid_universe(close: pd.DataFrame, volume: pd.DataFrame, size: int = UNIVERSE_SIZE) -> pd.DataFrame:
    """Same rule as the research panel: enough history, not pegged, top by 30-day quote volume, majors left out."""
    life = close.notna()
    present = life & (volume > 0)
    history = life.cumsum() >= MIN_HISTORY_DAYS
    hi, lo = close.rolling(90, min_periods=30).max(), close.rolling(90, min_periods=30).min()
    pegged = (hi / lo.where(lo > 0)) <= PEG_RATIO
    eligible = present & history & ~pegged.fillna(False)
    liquidity = volume.rolling(30, min_periods=20).mean().where(eligible)
    universe = liquidity.rank(axis=1, ascending=False, method="first") <= size
    majors = [c for c in universe.columns if strip_multiplier(str(c)).upper() in MAJOR_COINS]
    universe[majors] = False
    return universe


def _group_of(decile: int) -> Dict[str, str]:
    for key, label, deciles in GROUPS:
        if decile in deciles:
            return {"key": key, "label": label}
    return {"key": "middle", "label": "中等"}


def compute_risk_meter(frames: Dict[str, pd.DataFrame], now: Optional[datetime] = None) -> Dict[str, Any]:
    """The payload served at /api/altcoin/radar/risk-meter. Pure: no IO, so tests can feed it synthetic bars."""
    close, high, low, volume = frames["close"], frames["high"], frames["low"], frames["volume"]
    stamp = pd.Timestamp(now or datetime.now(timezone.utc))
    stamp = stamp.tz_convert("UTC").tz_localize(None) if stamp.tzinfo else stamp
    # Today's bar is still forming, and a day only a few coins have is not a day the market finished.
    counts = close.notna().sum(axis=1)
    full_days = counts.index[(counts >= MIN_COINS_FOR_A_DAY) & (counts.index < stamp.normalize())]
    if not len(full_days):
        return {"available": False, "reason": "not_enough_coins"}
    as_of = full_days[-1]
    close, high, low, volume = (f.loc[:as_of] for f in (close, high, low, volume))

    vol = parkinson_volatility(high, low)
    universe = liquid_universe(close, volume)
    pct = vol.where(universe).rank(axis=1, pct=True)
    decile = np.ceil(pct * 10).clip(1, 10)

    days = close.index
    last = len(days) - 1
    first_cal, last_cal = max(0, last - HOLD_DAYS - CAL_WINDOW_DAYS + 1), last - HOLD_DAYS
    if last_cal <= first_cal:
        return {"available": False, "reason": "not_enough_history", "as_of": str(as_of.date())}
    up = high.iloc[::-1].rolling(HOLD_DAYS, min_periods=1).max().iloc[::-1].shift(-1) / close - 1
    down = low.iloc[::-1].rolling(HOLD_DAYS, min_periods=1).min().iloc[::-1].shift(-1) / close - 1
    window = days[first_cal: last_cal + 1]

    def rates(hit: pd.DataFrame) -> Dict[int, float]:
        hits = hit.loc[window].to_numpy(dtype=bool)
        dec = decile.loc[window].to_numpy(dtype=float)
        return {d: float(hits[dec == d].mean()) if (dec == d).any() else 0.0 for d in range(1, 11)}

    up40, dn30 = rates(up >= TOUCH_UP), rates(down <= -TOUCH_DOWN)
    long_rates = {lev: rates(down <= -1.0 / lev) for lev in LEVERAGES}
    short_rates = {lev: rates(up >= 1.0 / lev) for lev in LEVERAGES}
    dec_window = decile.loc[window].to_numpy(dtype=float)
    calibration: List[Dict[str, Any]] = []
    for d in range(1, 11):
        calibration.append({
            "decile": d,
            "group": _group_of(d)["key"],
            "up40": round(up40[d], 4),
            "dn30": round(dn30[d], 4),
            "long": {str(lev): round(long_rates[lev][d], 4) for lev in LEVERAGES},
            "short": {str(lev): round(short_rates[lev][d], 4) for lev in LEVERAGES},
            "samples": int((dec_window == d).sum()),
        })

    # Coins outside today's universe (young or thin perps, majors) are placed on the universe's own scale.
    today_vol = vol.iloc[-1]
    today_universe = universe.iloc[-1]
    scale = np.sort(today_vol[today_universe].dropna().to_numpy(dtype=float))
    symbols: Dict[str, Dict[str, Any]] = {}
    if len(scale):
        recent = close.iloc[-1 - int(STALE_AFTER_DAYS):].notna().any()
        for coin, value in today_vol.items():
            if not np.isfinite(value) or not bool(recent.get(coin, False)):
                continue
            rank_pct = float(np.searchsorted(scale, value, side="right")) / len(scale)
            rank_pct = min(max(rank_pct, 1.0 / len(scale)), 1.0)
            symbols[str(coin).upper()] = {
                "perp": f"{coin}USDT",
                "vol14": round(float(value), 5),
                "pct": round(rank_pct, 4),
                "decile": int(min(10, max(1, math.ceil(rank_pct * 10)))),
                "in_universe": bool(today_universe.get(coin, False)),
            }
    for coin in list(symbols):
        plain = strip_multiplier(coin)
        if plain != coin and plain not in symbols:
            symbols[plain] = symbols[coin]

    age_days = (stamp - as_of).total_seconds() / 86400.0 - 1.0  # a daily bar closes one day after its label
    return {
        "available": True,
        "as_of": str(as_of.date()),
        "age_days": round(age_days, 2),
        "stale": age_days > STALE_AFTER_DAYS,
        "method": {
            "vol_window_days": VOL_WINDOW_DAYS,
            "calibration_days": len(window),
            "calibration_from": str(window[0].date()),
            "calibration_to": str(window[-1].date()),
            "hold_days": HOLD_DAYS,
            "universe_today": int(today_universe.sum()),
        },
        "groups": [{"key": key, "label": label, "deciles": list(deciles)} for key, label, deciles in GROUPS],
        "leverages": list(LEVERAGES),
        "calibration": calibration,
        "symbols": symbols,
    }


def build_risk_meter(folder: Path = BINANCE_FUTURES, now: Optional[datetime] = None) -> Dict[str, Any]:
    frames = load_daily_bars(folder)
    if frames["close"].empty:
        return {"available": False, "reason": "no_local_bars"}
    return compute_risk_meter(frames, now=now)
