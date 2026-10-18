"""Daily cross-sectional research panel from the local stores (research loop v3).

Every Binance USDT perpetual in data/research/coinglass_history/universe.json (654 on 2026-10-18,
delisted ones included, so no survivorship), one row per UTC day since 2020-01-01, built only from
local files kept current by the scheduled top-ups (docs/LOCAL_DATA_INVENTORY.md):

  data/historical/binance_futures/<COIN>_USDT/1d.parquet, funding_rate.parquet, premium_index_1d.parquet
  data/research/coinglass_history/panels/<dataset>.parquet

Timing: day D's value is what was known at D+1 00:00 UTC (day D's close). Daily bars stamped D
cover day D. Coinglass long/short ratios are snapshots taken at their stamp, so the one stamped
D+1 00:00 is day D's (as in the trackers, SNAPSHOT_AT_STAMP). Net position changes are kept at
their stamp (if they are snapshots too they are a day stale, never early). A funding settlement
at exactly 00:00 belongs to the day that ends there.

Universe (point in time): on day D, coins with a close and volume that day, >= 60 days of
history, not pegged (90-day max/min close > 1.10) and not excluded, ranked by 30-day mean
quote volume; the top UNIVERSE_SIZE are in.

Label: forward 7-day log return from day D's close to day D+7's close. A perp delisted within
the week exits at its last close (what a held position would get), so dying coins stay in.
fund7 = funding over days D+1..D+7 (longs pay, shorts earn).

Known factors (the information a new formula has to add to): short-term reversal, 30-day
momentum, 30-day volatility, liquidity, funding level, 7-day OI change and the eight-signal
Coinglass composite the forward trackers run (equal weights, training-IC signs).
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BINANCE_FUTURES = PROJECT_ROOT / "data" / "historical" / "binance_futures"
COINGLASS_PANELS = PROJECT_ROOT / "data" / "research" / "coinglass_history" / "panels"
PERPS_PATH = PROJECT_ROOT / "data" / "research" / "coinglass_history" / "universe.json"
CACHE_DIR = PROJECT_ROOT / "data" / "research" / "xs_daily_panel"

START = pd.Timestamp("2020-01-01")
UNIVERSE_SIZE = 150
MIN_HISTORY_DAYS = 60
PEG_RATIO = 1.10
HOLD_DAYS = 7
# stablecoins / fiat (scripts/collect_binance_history.py NOT_COINS), gold tokens, the BTC dominance index
EXCLUDED_COINS = {
    "USDC", "BUSD", "TUSD", "USDP", "PAX", "FDUSD", "DAI", "USDS", "USDSB", "SUSD", "EUR", "GBP", "AUD", "EURI",
    "AEUR", "XUSD", "USD1", "BFUSD", "PYUSD", "RLUSD", "USDE", "UST", "PAXG", "XAUT", "BTCDOM",
}

# What the research LLM is told about each column (one coin, one row per UTC day).
COLUMNS: Dict[str, str] = {
    "close": "perpetual daily close price",
    "open": "perpetual daily open price",
    "high": "perpetual daily high price",
    "low": "perpetual daily low price",
    "volume": "perpetual daily quote volume in USD (Binance)",
    "funding": "sum of the day's Binance funding rate settlements (longs pay when positive)",
    "premium": "Binance perpetual premium over the index price, daily close (a fraction)",
    "funding_all": "open-interest-weighted funding rate across exchanges, latest of the day",
    "oi": "open interest in USD across exchanges, daily close",
    "liq_long": "long liquidations in USD that day (Binance+OKX+Bybit)",
    "liq_short": "short liquidations in USD that day (Binance+OKX+Bybit)",
    "taker_buy": "futures taker buy volume in USD that day (Binance+OKX+Bybit)",
    "taker_sell": "futures taker sell volume in USD that day (Binance+OKX+Bybit)",
    "spot_buy": "spot taker buy volume in USD that day (10 spot exchanges; missing if the coin has no spot market)",
    "spot_sell": "spot taker sell volume in USD that day (10 spot exchanges)",
    "lsr_global": "Binance long/short ratio of all accounts at the day's close",
    "lsr_top_account": "Binance long/short ratio of top-trader accounts at the day's close",
    "lsr_top_position": "Binance long/short ratio of top-trader positions at the day's close",
    "net_long_usd": "Binance change in net long positions that day, in USD",
    "net_short_usd": "Binance change in net short positions that day, in USD",
    "btc_close": "BTC perpetual daily close (the same series for every coin)",
}
FACTORS: Dict[str, str] = {
    "rev21": "short-term reversal: close / 21-day EMA - 1",
    "mom30": "30-day price momentum",
    "vol30": "30-day volatility of daily returns",
    "size30": "liquidity: log of 30-day mean quote volume",
    "fund7": "7-day mean funding rate",
    "oi7": "7-day change in log open interest",
    "cg_composite": "the 8-signal Coinglass positioning composite (liquidation imbalance and shock, 3-day OI change, "
                    "funding z, retail and top-trader long/short z, top-vs-retail, 3-day taker imbalance)",
}
_CG_FIELDS = {  # dataset -> {value column in the panel file: panel column}
    "oi": {"close": "oi"},
    "funding": {"close": "funding_all"},
    "liquidation": {"aggregated_long_liquidation_usd": "liq_long", "aggregated_short_liquidation_usd": "liq_short"},
    "taker": {"aggregated_buy_volume_usd": "taker_buy", "aggregated_sell_volume_usd": "taker_sell"},
    "spot_taker_all": {"aggregated_buy_volume_usd": "spot_buy", "aggregated_sell_volume_usd": "spot_sell"},
    "net_position": {"net_long_change": "net_long_usd", "net_short_change": "net_short_usd"},
}
_CG_RATIOS = {"global_ratio": ("global_account", "lsr_global"), "top_account": ("top_account", "lsr_top_account"),
              "top_position": ("top_position", "lsr_top_position")}


@dataclass
class Panel:
    columns: Dict[str, pd.DataFrame]
    universe: pd.DataFrame
    fwd7: pd.DataFrame
    fund7: pd.DataFrame
    factors: Dict[str, pd.DataFrame]
    meta: Dict[str, Any] = field(default_factory=dict)

    @property
    def dates(self) -> pd.DatetimeIndex:
        return self.universe.index

    @property
    def coins(self) -> List[str]:
        return list(self.universe.columns)


def _perps() -> List[Dict[str, Any]]:
    return json.loads(PERPS_PATH.read_text(encoding="utf-8"))["perps"]


def _naive_days(index) -> pd.DatetimeIndex:
    index = pd.DatetimeIndex(index)
    if index.tz is not None:
        index = index.tz_convert("UTC").tz_localize(None)
    return index.normalize()


def _read_binance(perp: str) -> Dict[str, pd.Series]:
    folder = BINANCE_FUTURES / f"{perp[:-4]}_USDT"
    out: Dict[str, pd.Series] = {}
    path = folder / "1d.parquet"
    if not path.exists():
        return out
    bars = pd.read_parquet(path)
    bars.index = _naive_days(bars.index)
    bars = bars[~bars.index.duplicated(keep="last")].sort_index()
    for col in ("open", "high", "low", "close"):
        out[col] = bars[col].astype(float)
    typical = (bars["high"] + bars["low"] + bars["close"]) / 3.0
    out["volume"] = (bars["volume"] * typical).astype(float)
    path = folder / "funding_rate.parquet"
    if path.exists():
        funding = pd.read_parquet(path)
        stamps = pd.DatetimeIndex(funding.index)
        if stamps.tz is not None:
            stamps = stamps.tz_convert("UTC").tz_localize(None)
        day = (stamps - pd.Timedelta(milliseconds=1)).floor("D")  # a 00:00 settlement ends the day before
        out["funding"] = pd.Series(funding["funding_rate"].to_numpy(dtype=float), index=day).groupby(level=0).sum()
    path = folder / "premium_index_1d.parquet"
    if path.exists():
        premium = pd.read_parquet(path)
        premium.index = _naive_days(premium.index)
        out["premium"] = premium["close"][~premium.index.duplicated(keep="last")].astype(float)
    return out


def _read_coinglass(perps: List[str], log: Callable[[str], None]) -> Dict[str, pd.DataFrame]:
    out: Dict[str, pd.DataFrame] = {}
    wanted = set(perps)
    for dataset, fields in {**_CG_FIELDS, **{k: None for k in _CG_RATIOS}}.items():
        path = COINGLASS_PANELS / f"{dataset}.parquet"
        if not path.exists():
            log(f"coinglass panel missing: {dataset}")
            continue
        if fields is None:
            prefix, name = _CG_RATIOS[dataset]
            cols = [f"{prefix}_long_percent", f"{prefix}_short_percent", f"{prefix}_long_short_ratio"]
        else:
            cols = list(fields)
        frame = pd.read_parquet(path, columns=["perp", "time"] + cols)
        frame = frame[frame["perp"].isin(wanted)]
        frame = frame.assign(day=_naive_days(frame["time"]))
        if fields is None:
            long_pct, short_pct = frame[cols[0]].astype(float), frame[cols[1]].astype(float)
            ratio = (long_pct / short_pct.where(short_pct > 0)).fillna(frame[cols[2]].astype(float))
            frame = frame.assign(value=ratio, day=frame["day"] - pd.Timedelta(days=1))  # snapshot at D+1 00:00 = day D
            out[name] = frame.pivot_table(index="day", columns="perp", values="value", aggfunc="last")
        else:
            for src, name in fields.items():
                out[name] = frame.pivot_table(index="day", columns="perp", values=src, aggfunc="last").astype(float)
    return out


def _prior_z(frame: pd.DataFrame, window: int) -> pd.DataFrame:
    prior = frame.shift(1).rolling(window, min_periods=max(5, window * 2 // 3))
    std = prior.std()
    return (frame - prior.mean()) / std.where(std > 1e-12)


def _composite(columns: Dict[str, pd.DataFrame], universe: pd.DataFrame) -> pd.DataFrame:
    """The forward trackers' eight signals, equal weights with the training ICs' signs, ranked in the universe."""
    from core.research.coinglass_composite_tracker import SHADOW_WEIGHTS, signals

    frames = {"liq_long": columns["liq_long"], "liq_short": columns["liq_short"], "buy": columns["taker_buy"],
              "sell": columns["taker_sell"], "oi": columns["oi"], "funding": columns["funding_all"],
              "global": columns["lsr_global"], "top": columns["lsr_top_position"]}
    total = sum(abs(w) for w in SHADOW_WEIGHTS.values())
    num = pd.DataFrame(0.0, index=universe.index, columns=universe.columns)
    den = num.copy()
    for name, sig in signals(frames).items():
        score = sig.where(universe).rank(axis=1, pct=True) - 0.5
        num += (score * SHADOW_WEIGHTS[name]).fillna(0.0)
        den += score.notna() * abs(SHADOW_WEIGHTS[name])
    return (num / den.where(den > 0)).where(den >= 0.5 * total)


def build_panel(log: Callable[[str], None] = print) -> Panel:
    started = time.time()
    perps = [p for p in _perps() if p["coin"].upper() not in EXCLUDED_COINS and p["perp"].endswith("USDT")]
    series: Dict[str, Dict[str, pd.Series]] = {}
    for i, p in enumerate(perps):
        data = _read_binance(p["perp"])
        if "close" in data:
            series[p["perp"]] = data
        if (i + 1) % 100 == 0:
            log(f"binance {i + 1}/{len(perps)}")
    names = sorted(series)
    end = max(s["close"].index.max() for s in series.values())
    dates = pd.date_range(START, end, freq="1D")

    def wide(col: str) -> pd.DataFrame:
        return pd.DataFrame({perp: series[perp][col] for perp in names if col in series[perp]}).reindex(
            index=dates, columns=names).astype(float)

    columns: Dict[str, pd.DataFrame] = {col: wide(col) for col in ("open", "high", "low", "close", "volume", "funding", "premium")}
    for name, frame in _read_coinglass(names, log).items():
        columns[name] = frame.reindex(index=dates, columns=names).astype(float)
    for col in ("net_long_usd", "net_short_usd"):  # stored in coins
        if col in columns:
            columns[col] = columns[col] * columns["close"]
    btc = columns["close"]["BTCUSDT"]
    columns["btc_close"] = pd.DataFrame({c: btc for c in names}).reindex(index=dates)
    missing = [c for c in COLUMNS if c not in columns]
    if missing:
        raise RuntimeError(f"panel columns missing: {missing}")

    close, volume = columns["close"], columns["volume"]
    life = close.notna()
    last_day = life[::-1].idxmax()  # last date with a close, per coin
    present = life & (volume > 0)
    history = life.cumsum() >= MIN_HISTORY_DAYS
    hi, lo = close.rolling(90, min_periods=30).max(), close.rolling(90, min_periods=30).min()
    pegged = (hi / lo.where(lo > 0)) <= PEG_RATIO
    eligible = present & history & ~pegged.fillna(False)
    liquidity = volume.rolling(30, min_periods=20).mean().where(eligible)
    universe = liquidity.rank(axis=1, ascending=False, method="first") <= UNIVERSE_SIZE

    # label: exit at D+7's close, or at the last close of a perp delisted within the week
    status = {p["perp"]: p for p in perps}
    delisted = pd.Series({c: status.get(c, {}).get("status") == "SETTLING" for c in names})
    day_grid = dates.to_numpy()[:, None]
    close_life = close.ffill().where(pd.DataFrame(day_grid <= last_day.to_numpy()[None, :], index=dates, columns=names))
    exit_price = close_life.shift(-HOLD_DAYS)
    last_close = pd.Series({c: close.at[last_day[c], c] for c in names})
    before_end = pd.DataFrame(day_grid < last_day.to_numpy()[None, :], index=dates, columns=names)
    fill = pd.DataFrame(np.broadcast_to(last_close.to_numpy(), close.shape), index=dates, columns=names)
    exit_price = exit_price.where(exit_price.notna() | ~(before_end & delisted.to_numpy()[None, :]), fill)
    fwd7 = np.log(exit_price) - np.log(close)
    fund7 = columns["funding"].fillna(0.0).rolling(HOLD_DAYS, min_periods=1).sum().shift(-HOLD_DAYS)

    log_close = np.log(close.where(close > 0))
    oi = columns["oi"].where(columns["oi"] > 0)
    factors = {
        "rev21": close / close.ewm(span=21, adjust=False, min_periods=10).mean() - 1.0,
        "mom30": close / close.shift(30) - 1.0,
        "vol30": log_close.diff().rolling(30, min_periods=15).std(),
        "size30": np.log(volume.rolling(30, min_periods=15).mean().where(lambda v: v > 0)),
        "fund7": columns["funding"].rolling(7, min_periods=4).mean(),
        "oi7": np.log(oi) - np.log(oi.shift(7)),
        "cg_composite": _composite(columns, universe),
    }

    keep = [c for c in names if bool(universe[c].any())]  # coins never in the universe are never scored
    panel = Panel(
        columns={k: v[keep].astype("float32") for k, v in columns.items() if k in COLUMNS},
        universe=universe[keep],
        fwd7=fwd7[keep].astype("float32"),
        fund7=fund7[keep].astype("float32"),
        factors={k: v[keep].astype("float32") for k, v in factors.items()},
        meta={
            "built_at": datetime.now(timezone.utc).isoformat(), "start": str(dates[0].date()), "end": str(dates[-1].date()),
            "coins_total": len(names), "coins_kept": len(keep), "universe_size": UNIVERSE_SIZE,
            "build_seconds": round(time.time() - started, 1),
        },
    )
    log(f"panel built: {len(dates)} days x {len(keep)} coins (of {len(names)}) in {panel.meta['build_seconds']}s")
    return panel


def save_panel(panel: Panel, folder: Path = CACHE_DIR) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    frames = {f"col_{k}": v for k, v in panel.columns.items()}
    frames.update({f"factor_{k}": v for k, v in panel.factors.items()})
    frames.update(fwd7=panel.fwd7, fund7=panel.fund7, universe=panel.universe.astype("float32"))
    for name, frame in frames.items():
        frame.to_parquet(folder / f"{name}.parquet")
    (folder / "meta.json").write_text(json.dumps({**panel.meta, "files": sorted(frames)}, indent=1), encoding="utf-8")


def load_panel(*, max_age_hours: float = 20.0, folder: Path = CACHE_DIR, log: Callable[[str], None] = print) -> Panel:
    """The cached panel, rebuilt from the local stores when older than ``max_age_hours``."""
    meta_path = folder / "meta.json"
    if meta_path.exists():
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(meta["built_at"])).total_seconds() / 3600
        if age <= max_age_hours:
            read = lambda name: pd.read_parquet(folder / f"{name}.parquet")  # noqa: E731
            return Panel(
                columns={k: read(f"col_{k}") for k in COLUMNS},
                universe=read("universe") > 0.5,
                fwd7=read("fwd7"),
                fund7=read("fund7"),
                factors={k: read(f"factor_{k}") for k in FACTORS},
                meta=meta,
            )
    panel = build_panel(log)
    save_panel(panel, folder)
    return panel


def anonymized_summary(panel: Panel, end: Optional[pd.Timestamp] = None) -> Dict[str, Any]:
    """Panel facts for the LLM: sizes and typical magnitudes, no coin names or dates."""
    universe = panel.universe if end is None else panel.universe.loc[:end]
    fwd = panel.fwd7.loc[universe.index].where(universe)
    return {
        "days": int(len(universe)),
        "coins_per_day_median": int(universe.sum(axis=1).median()),
        "distinct_coins": int(universe.any().sum()),
        "forward_7d_return_cross_sectional_std": round(float(np.nanmedian(fwd.std(axis=1))), 4),
        "column_coverage_in_universe": {
            k: round(float(v.loc[universe.index].where(universe).notna().sum().sum() / max(1, universe.sum().sum())), 2)
            for k, v in panel.columns.items()
        },
    }
