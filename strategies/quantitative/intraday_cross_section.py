"""5m Binance USD-M cross-sectional long/short strategies."""
from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Set

import numpy as np
import pandas as pd
from loguru import logger

from core.data import data_storage
from core.strategies.strategy_base import Signal, SignalType, StrategyBase, bar_time


INTRADAY_CROSS_SECTION_STRATEGY_IDS = [
    "ResidualMom48hStrategy",
    "Ret24hReversalStrategy",
    "RelRet24hReversalStrategy",
    "ResidualMom24hStrategy",
    "CloseLocation48hStrategy",
]


DEFAULT_INTRADAY_CROSS_SECTION_UNIVERSE = [
    "BTC/USDT",
    "ETH/USDT",
    "BNB/USDT",
    "SOL/USDT",
    "XRP/USDT",
    "DOGE/USDT",
    "ADA/USDT",
    "AVAX/USDT",
    "LINK/USDT",
    "DOT/USDT",
    "TRX/USDT",
    "LTC/USDT",
    "BCH/USDT",
    "NEAR/USDT",
    "APT/USDT",
    "ARB/USDT",
    "OP/USDT",
    "INJ/USDT",
    "AAVE/USDT",
    "SUI/USDT",
]


@dataclass(frozen=True)
class IntradayCrossSectionSpec:
    strategy_id: str
    factor_name: str
    lookback_bars: int
    direction: str
    description: str


INTRADAY_CROSS_SECTION_SPECS: Dict[str, IntradayCrossSectionSpec] = {
    "ResidualMom48hStrategy": IntradayCrossSectionSpec(
        strategy_id="residual_mom_48h",
        factor_name="residual_return",
        lookback_bars=576,
        direction="low",
        description="48h residual reversal: long relative laggards, short relative leaders.",
    ),
    "Ret24hReversalStrategy": IntradayCrossSectionSpec(
        strategy_id="ret_24h",
        factor_name="return",
        lookback_bars=288,
        direction="low",
        description="24h cross-sectional short-term reversal.",
    ),
    "RelRet24hReversalStrategy": IntradayCrossSectionSpec(
        strategy_id="rel_ret_24h",
        factor_name="residual_return",
        lookback_bars=288,
        direction="low",
        description="24h relative-market return reversal.",
    ),
    "ResidualMom24hStrategy": IntradayCrossSectionSpec(
        strategy_id="residual_mom_24h",
        factor_name="residual_return",
        lookback_bars=288,
        direction="low",
        description="24h residual momentum placeholder, currently equal to relative-market return.",
    ),
    "CloseLocation48hStrategy": IntradayCrossSectionSpec(
        strategy_id="close_location_48h",
        factor_name="close_location_mean",
        lookback_bars=576,
        direction="high",
        description="48h rolling mean of close location within each candle.",
    ),
}


def normalize_symbol(value: Any) -> str:
    text = str(value or "").strip().upper().replace("_", "/")
    if not text:
        return ""
    if ":" in text:
        text = text.split(":", 1)[0]
    if "/" not in text and text.endswith("USDT") and len(text) > 4:
        text = f"{text[:-4]}/USDT"
    if "/" not in text:
        text = f"{text}/USDT"
    return text


def normalize_symbol_list(symbols: Iterable[Any]) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for item in symbols or []:
        symbol = normalize_symbol(item)
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
    return out


def calc_return(close: pd.DataFrame, lookback_bars: int) -> pd.DataFrame:
    close_num = close.apply(pd.to_numeric, errors="coerce")
    base = close_num.shift(int(lookback_bars))
    out = close_num / base - 1.0
    return out.replace([np.inf, -np.inf], np.nan)


def calc_market_equal_weight_return(ret_panel: pd.DataFrame) -> pd.Series:
    return ret_panel.replace([np.inf, -np.inf], np.nan).mean(axis=1, skipna=True)


def calc_residual_return(close: pd.DataFrame, lookback_bars: int) -> pd.DataFrame:
    returns = calc_return(close, lookback_bars)
    market = calc_market_equal_weight_return(returns)
    return returns.sub(market, axis=0).replace([np.inf, -np.inf], np.nan)


def calc_close_location(
    open_panel: Optional[pd.DataFrame],
    high_panel: pd.DataFrame,
    low_panel: pd.DataFrame,
    close_panel: pd.DataFrame,
) -> pd.DataFrame:
    del open_panel
    high = high_panel.apply(pd.to_numeric, errors="coerce")
    low = low_panel.apply(pd.to_numeric, errors="coerce")
    close = close_panel.apply(pd.to_numeric, errors="coerce")
    spread = high - low
    location = (close - low) / spread.replace(0.0, np.nan)
    location = location.mask(spread == 0.0, 0.5)
    return location.replace([np.inf, -np.inf], np.nan).clip(lower=0.0, upper=1.0)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    return out if math.isfinite(out) else float(default)


def _quote_volume_frame(close: pd.DataFrame, volume: Optional[pd.DataFrame]) -> pd.DataFrame:
    if volume is None or volume.empty:
        return pd.DataFrame(np.nan, index=close.index, columns=close.columns, dtype=float)
    aligned = volume.reindex(close.index).reindex(columns=close.columns)
    return (close * aligned.apply(pd.to_numeric, errors="coerce")).replace([np.inf, -np.inf], np.nan)


def filter_cross_section_universe(
    factor: pd.Series,
    close_row: pd.Series,
    *,
    lookback_valid: Optional[pd.Series] = None,
    high_row: Optional[pd.Series] = None,
    low_row: Optional[pd.Series] = None,
    quote_volume_row: Optional[pd.Series] = None,
    return_row: Optional[pd.Series] = None,
    funding_row: Optional[pd.Series] = None,
    blacklist: Optional[Iterable[str]] = None,
    min_quote_volume: float = 0.0,
    max_abs_return: float = 0.0,
    max_range_bps: float = 0.0,
    funding_abs_threshold: float = 0.0,
) -> pd.Series:
    values = pd.to_numeric(factor, errors="coerce").replace([np.inf, -np.inf], np.nan)
    mask = values.notna()

    close = pd.to_numeric(close_row.reindex(values.index), errors="coerce")
    mask &= close.notna() & (close > 0)

    if lookback_valid is not None:
        mask &= lookback_valid.reindex(values.index).fillna(False).astype(bool)

    if high_row is not None and low_row is not None:
        high = pd.to_numeric(high_row.reindex(values.index), errors="coerce")
        low = pd.to_numeric(low_row.reindex(values.index), errors="coerce")
        mask &= high.notna() & low.notna() & (high >= low) & (low >= 0)
        if max_range_bps > 0:
            range_bps = (high - low).abs() / close.replace(0.0, np.nan) * 10000.0
            mask &= range_bps.fillna(max_range_bps + 1.0) <= float(max_range_bps)

    if quote_volume_row is not None and min_quote_volume > 0:
        quote_volume = pd.to_numeric(quote_volume_row.reindex(values.index), errors="coerce")
        mask &= quote_volume.fillna(0.0) >= float(min_quote_volume)

    if return_row is not None and max_abs_return > 0:
        returns = pd.to_numeric(return_row.reindex(values.index), errors="coerce").abs()
        mask &= returns.fillna(max_abs_return + 1.0) <= float(max_abs_return)

    if funding_row is not None and funding_abs_threshold > 0:
        funding = pd.to_numeric(funding_row.reindex(values.index), errors="coerce").abs()
        mask &= funding.fillna(0.0) <= float(funding_abs_threshold)

    blocked = set(normalize_symbol_list(blacklist or []))
    if blocked:
        mask &= ~pd.Series([normalize_symbol(sym) in blocked for sym in values.index], index=values.index)

    return values[mask].dropna()


def cross_section_rank_select(
    factor: pd.Series,
    long_quantile: float = 0.2,
    short_quantile: float = 0.2,
    direction: str = "low",
    *,
    min_names_per_side: int = 1,
    max_names_per_side: Optional[int] = None,
) -> Dict[str, Any]:
    clean = pd.to_numeric(factor, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    clean = clean.sort_values(ascending=True)
    if clean.empty:
        return {
            "ranked": clean,
            "ranks": pd.Series(dtype=float),
            "long_symbols": [],
            "short_symbols": [],
        }

    n_assets = len(clean)
    long_n = max(int(min_names_per_side), int(math.floor(n_assets * max(0.0, float(long_quantile)))))
    short_n = max(int(min_names_per_side), int(math.floor(n_assets * max(0.0, float(short_quantile)))))
    half = max(1, n_assets // 2)
    long_n = min(long_n, half)
    short_n = min(short_n, half)
    if max_names_per_side is not None and int(max_names_per_side) > 0:
        long_n = min(long_n, int(max_names_per_side))
        short_n = min(short_n, int(max_names_per_side))

    direction_text = str(direction or "low").strip().lower()
    if direction_text == "high":
        long_symbols = list(clean.tail(long_n).sort_values(ascending=False).index)
        short_symbols = list(clean.head(short_n).index)
    else:
        long_symbols = list(clean.head(long_n).index)
        short_symbols = list(clean.tail(short_n).sort_values(ascending=False).index)

    ranks = clean.rank(method="first", ascending=True)
    return {
        "ranked": clean,
        "ranks": ranks,
        "long_symbols": long_symbols,
        "short_symbols": short_symbols,
    }


def build_equal_weight_long_short_positions(
    long_symbols: Iterable[str],
    short_symbols: Iterable[str],
    *,
    all_symbols: Iterable[str],
    allow_short: bool = True,
    allow_long: bool = True,
    max_symbol_weight: float = 0.10,
    max_portfolio_leverage: float = 1.0,
) -> pd.Series:
    symbols = list(all_symbols)
    weights = pd.Series(0.0, index=symbols, dtype=float)
    longs = [sym for sym in long_symbols if sym in weights.index]
    shorts = [sym for sym in short_symbols if sym in weights.index]

    allow_short = bool(allow_short)
    allow_long = bool(allow_long)
    max_weight = max(0.0, float(max_symbol_weight or 0.0))
    leverage = max(0.0, float(max_portfolio_leverage or 0.0))
    if leverage <= 0:
        return weights

    long_budget = 0.5 * leverage if allow_long and allow_short else leverage
    short_budget = 0.5 * leverage if allow_long and allow_short else leverage

    if allow_long and longs:
        per_long = long_budget / len(longs)
        if max_weight > 0:
            per_long = min(per_long, max_weight)
        weights.loc[longs] = per_long
    if allow_short and shorts:
        per_short = short_budget / len(shorts)
        if max_weight > 0:
            per_short = min(per_short, max_weight)
        weights.loc[shorts] = -per_short
    return weights


def estimate_slippage_bps(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    *,
    min_slippage_bps_per_side: float = 2.0,
    slippage_range_multiplier: float = 0.08,
    max_slippage_bps_per_side: float = 20.0,
) -> pd.Series:
    close_safe = pd.to_numeric(close, errors="coerce").replace(0.0, np.nan)
    range_bps = (
        (pd.to_numeric(high, errors="coerce") - pd.to_numeric(low, errors="coerce")).abs()
        / close_safe
        * 10000.0
    ).replace([np.inf, -np.inf], np.nan)
    slip = np.maximum(float(min_slippage_bps_per_side), float(slippage_range_multiplier) * range_bps)
    return pd.Series(slip, index=close.index).clip(
        lower=max(0.0, float(min_slippage_bps_per_side)),
        upper=max(0.0, float(max_slippage_bps_per_side)),
    )


def _is_rebalance_bar(
    timestamp: Any,
    rebalance_bars: int,
    *,
    rebalance_offset_bars: int = 0,
    fallback_index: Optional[int] = None,
) -> bool:
    bars = max(1, int(rebalance_bars or 1))
    try:
        ts = pd.Timestamp(timestamp)
        if pd.isna(ts):
            raise ValueError("invalid timestamp")
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")
        slot = int(ts.value // (5 * 60 * 1_000_000_000))
        return (slot - int(rebalance_offset_bars or 0)) % bars == 0
    except Exception:
        return fallback_index is not None and int(fallback_index) % bars == 0


def build_ohlcv_panels(frames: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    normalized: Dict[str, pd.DataFrame] = {}
    for raw_symbol, frame in (frames or {}).items():
        symbol = normalize_symbol(raw_symbol)
        if not symbol or frame is None or frame.empty:
            continue
        df = frame.copy()
        df.index = pd.to_datetime(df.index, errors="coerce")
        df = df[~df.index.isna()]
        df = df[~df.index.duplicated(keep="last")].sort_index()
        if not {"high", "low", "close"}.issubset(df.columns):
            continue
        normalized[symbol] = df

    common_index: Optional[pd.Index] = None
    for frame in normalized.values():
        idx = pd.Index(pd.to_datetime(frame.index))
        common_index = idx if common_index is None else common_index.intersection(idx)
    common_index = pd.Index(common_index).sort_values() if common_index is not None else pd.Index([])
    if len(common_index) == 0 or not normalized:
        return {}

    symbols = list(normalized.keys())
    panels = {
        "open": pd.DataFrame(index=common_index),
        "high": pd.DataFrame(index=common_index),
        "low": pd.DataFrame(index=common_index),
        "close": pd.DataFrame(index=common_index),
        "volume": pd.DataFrame(index=common_index),
    }
    for symbol in symbols:
        view = normalized[symbol].reindex(common_index)
        panels["open"][symbol] = pd.to_numeric(view.get("open", view["close"]), errors="coerce")
        panels["high"][symbol] = pd.to_numeric(view["high"], errors="coerce")
        panels["low"][symbol] = pd.to_numeric(view["low"], errors="coerce")
        panels["close"][symbol] = pd.to_numeric(view["close"], errors="coerce")
        panels["volume"][symbol] = pd.to_numeric(view.get("volume", 0.0), errors="coerce").fillna(0.0)

    panels["close"] = panels["close"].dropna(axis=1, how="all").ffill()
    symbols = list(panels["close"].columns)
    for key in ["open", "high", "low", "volume"]:
        panels[key] = panels[key].reindex(panels["close"].index).reindex(columns=symbols)
        if key in {"open", "high", "low"}:
            panels[key] = panels[key].ffill()
        else:
            panels[key] = panels[key].fillna(0.0)
    return panels


def compute_intraday_factor_panel(
    spec: IntradayCrossSectionSpec,
    panels: Dict[str, pd.DataFrame],
) -> pd.DataFrame:
    close = panels["close"]
    if spec.factor_name == "return":
        return calc_return(close, spec.lookback_bars)
    if spec.factor_name == "residual_return":
        return calc_residual_return(close, spec.lookback_bars)
    if spec.factor_name == "close_location_mean":
        location = calc_close_location(
            panels.get("open"),
            panels["high"],
            panels["low"],
            close,
        )
        return location.rolling(spec.lookback_bars, min_periods=spec.lookback_bars).mean()
    raise ValueError(f"Unknown intraday factor: {spec.factor_name}")


def build_intraday_cross_section_weights(
    spec: IntradayCrossSectionSpec,
    panels: Dict[str, pd.DataFrame],
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    cfg = dict(params or {})
    if not panels or "close" not in panels:
        raise ValueError("OHLCV panels are required")

    lookback_bars = max(1, int(cfg.get("lookback_bars", spec.lookback_bars) or spec.lookback_bars))
    effective_spec = IntradayCrossSectionSpec(
        strategy_id=spec.strategy_id,
        factor_name=spec.factor_name,
        lookback_bars=lookback_bars,
        direction=str(cfg.get("direction") or spec.direction),
        description=spec.description,
    )
    close = panels["close"].copy()
    high = panels["high"].reindex(close.index).reindex(columns=close.columns)
    low = panels["low"].reindex(close.index).reindex(columns=close.columns)
    volume = panels.get("volume", pd.DataFrame(index=close.index, columns=close.columns)).reindex(close.index).reindex(columns=close.columns)

    factor = compute_intraday_factor_panel(effective_spec, panels).reindex(close.index).reindex(columns=close.columns)
    returns_for_filter = calc_return(close, lookback_bars)
    valid_count = close.notna().rolling(lookback_bars + 1, min_periods=lookback_bars + 1).sum()
    lookback_valid = valid_count >= (lookback_bars + 1)
    quote_volume = _quote_volume_frame(close, volume)
    quote_volume_rolling = quote_volume.rolling(
        max(1, min(int(lookback_bars), int(cfg.get("volume_lookback_bars", 288) or 288))),
        min_periods=1,
    ).mean()

    long_q = max(0.01, min(0.49, float(cfg.get("long_quantile", 0.2) or 0.2)))
    short_q = max(0.01, min(0.49, float(cfg.get("short_quantile", 0.2) or 0.2)))
    rebalance_bars = max(1, int(cfg.get("rebalance_bars", 288) or 288))
    rebalance_offset_bars = int(cfg.get("rebalance_offset_bars", 0) or 0)
    max_symbol_weight = max(0.0, float(cfg.get("max_symbol_weight", 0.10) or 0.10))
    max_leverage = max(0.0, float(cfg.get("max_portfolio_leverage", 1.0) or 1.0))
    allow_short = bool(cfg.get("allow_short", True))
    allow_long = bool(cfg.get("allow_long", True))
    min_universe_size = max(2, int(cfg.get("min_universe_size", 5) or 5))
    min_names_per_side = max(1, int(cfg.get("min_names_per_side", 1) or 1))
    max_names_per_side_raw = cfg.get("max_names_per_side")
    max_names_per_side = int(max_names_per_side_raw) if max_names_per_side_raw not in {None, ""} else None

    weights = pd.DataFrame(0.0, index=close.index, columns=close.columns)
    current = pd.Series(0.0, index=close.columns, dtype=float)
    rebalance_rows: List[Dict[str, Any]] = []

    min_quote_volume = max(0.0, float(cfg.get("min_quote_volume", 0.0) or 0.0))
    max_abs_return = max(0.0, float(cfg.get("max_abs_return", 0.0) or 0.0))
    max_range_bps = max(0.0, float(cfg.get("max_range_bps", cfg.get("max_volatility_range_bps", 0.0)) or 0.0))
    funding_abs_threshold = max(0.0, float(cfg.get("funding_abs_threshold", 0.0) or 0.0))
    blacklist = cfg.get("blacklist_symbols") or cfg.get("blacklist") or []
    funding = cfg.get("funding_panel")

    for idx, ts in enumerate(close.index):
        if idx < lookback_bars:
            weights.iloc[idx] = current
            continue
        if not _is_rebalance_bar(
            ts,
            rebalance_bars,
            rebalance_offset_bars=rebalance_offset_bars,
            fallback_index=idx,
        ):
            weights.iloc[idx] = current
            continue

        funding_row = None
        if isinstance(funding, pd.DataFrame) and ts in funding.index:
            funding_row = funding.loc[ts]
        row = filter_cross_section_universe(
            factor.loc[ts],
            close.loc[ts],
            lookback_valid=lookback_valid.loc[ts],
            high_row=high.loc[ts],
            low_row=low.loc[ts],
            quote_volume_row=quote_volume_rolling.loc[ts],
            return_row=returns_for_filter.loc[ts],
            funding_row=funding_row,
            blacklist=blacklist,
            min_quote_volume=min_quote_volume,
            max_abs_return=max_abs_return,
            max_range_bps=max_range_bps,
            funding_abs_threshold=funding_abs_threshold,
        )
        current = pd.Series(0.0, index=close.columns, dtype=float)
        if len(row) >= min_universe_size:
            selection = cross_section_rank_select(
                row,
                long_quantile=long_q,
                short_quantile=short_q,
                direction=effective_spec.direction,
                min_names_per_side=min_names_per_side,
                max_names_per_side=max_names_per_side,
            )
            current = build_equal_weight_long_short_positions(
                selection["long_symbols"],
                selection["short_symbols"],
                all_symbols=close.columns,
                allow_short=allow_short,
                allow_long=allow_long,
                max_symbol_weight=max_symbol_weight,
                max_portfolio_leverage=max_leverage,
            )
            rebalance_rows.append(
                {
                    "timestamp": ts,
                    "strategy_id": spec.strategy_id,
                    "long_symbols": selection["long_symbols"],
                    "short_symbols": selection["short_symbols"],
                    "universe_size": int(len(row)),
                    "factor": row.to_dict(),
                    "rank": selection["ranks"].to_dict(),
                    "target_weights": current[current != 0].to_dict(),
                }
            )
        weights.iloc[idx] = current

    turnover = weights.diff().abs().sum(axis=1).fillna(weights.abs().sum(axis=1))
    return {
        "factor": factor,
        "weights": weights,
        "turnover": turnover,
        "rebalance_rows": rebalance_rows,
        "quote_volume": quote_volume_rolling,
        "lookback_bars": lookback_bars,
    }


def latest_intraday_cross_section_plan(
    spec: IntradayCrossSectionSpec,
    frames: Dict[str, pd.DataFrame],
    params: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    panels = build_ohlcv_panels(frames)
    if not panels or panels["close"].empty:
        return None
    components = build_intraday_cross_section_weights(spec, panels, params=params)
    rows = list(components.get("rebalance_rows") or [])
    if not rows:
        return None
    row = rows[-1]
    return {
        **row,
        "rebalance_timestamp": row["timestamp"],
        "price": panels["close"].loc[row["timestamp"]].to_dict(),
        "params": dict(params or {}),
        "lookback_bars": int(components.get("lookback_bars") or spec.lookback_bars),
        "rebalance_bars": int((params or {}).get("rebalance_bars", 288) or 288),
        "direction": str((params or {}).get("direction") or spec.direction),
    }


class IntradayCrossSectionStrategyBase(StrategyBase):
    """Base class for 5m cross-sectional Binance USD-M strategies."""

    mutates_input = False
    spec_key = "Ret24hReversalStrategy"

    def __init__(self, name: str, params: Optional[Dict[str, Any]] = None):
        spec = INTRADAY_CROSS_SECTION_SPECS[self.spec_key]
        default_params: Dict[str, Any] = {
            "strategy_id": spec.strategy_id,
            "timeframe": "5m",
            "exchange": "binance",
            "market_type": "future",
            "universe_symbols": list(DEFAULT_INTRADAY_CROSS_SECTION_UNIVERSE),
            "max_symbols": 100,
            "lookback_bars": spec.lookback_bars,
            "rebalance_bars": 288,
            "rebalance_offset_bars": 0,
            "long_quantile": 0.2,
            "short_quantile": 0.2,
            "direction": spec.direction,
            "max_symbol_weight": 0.10,
            "min_quote_volume": 0.0,
            "min_universe_size": 5,
            "min_names_per_side": 1,
            "max_names_per_side": None,
            "allow_long": True,
            "allow_short": True,
            "reverse_on_signal": True,
            "allow_pyramiding": False,
            "max_portfolio_leverage": 1.0,
            "fee_bps_per_side": 5.0,
            "min_slippage_bps_per_side": 2.0,
            "slippage_range_multiplier": 0.08,
            "max_slippage_bps_per_side": 20.0,
            "max_abs_return": 0.0,
            "max_range_bps": 0.0,
            "funding_abs_threshold": 0.0,
            "blacklist_symbols": [],
            "use_atr_stops": False,
        }
        if params:
            default_params.update(params)
        default_params["strategy_id"] = spec.strategy_id
        default_params["lookback_bars"] = int(default_params.get("lookback_bars") or spec.lookback_bars)
        default_params["direction"] = str(default_params.get("direction") or spec.direction)
        super().__init__(name=name, params=default_params)
        self.spec = IntradayCrossSectionSpec(
            strategy_id=spec.strategy_id,
            factor_name=spec.factor_name,
            lookback_bars=int(default_params["lookback_bars"]),
            direction=str(default_params["direction"]),
            description=spec.description,
        )
        self._last_rebalance_key: Optional[str] = None
        self._target_longs: Set[str] = set()
        self._target_shorts: Set[str] = set()

    def _universe(self) -> List[str]:
        return normalize_symbol_list(self.params.get("universe_symbols") or DEFAULT_INTRADAY_CROSS_SECTION_UNIVERSE)

    def _is_trigger_symbol(self, symbol: str, universe: List[str]) -> bool:
        trigger = normalize_symbol(self.params.get("trigger_symbol"))
        if not trigger and universe:
            trigger = universe[0]
        return normalize_symbol(symbol) == trigger

    def _rebalance_key(self, timestamp: Any) -> str:
        ts = pd.Timestamp(timestamp)
        if ts.tzinfo is not None:
            ts = ts.tz_convert("UTC").tz_localize(None)
        return ts.isoformat()

    async def _load_universe_frames(self, universe: List[str]) -> Dict[str, pd.DataFrame]:
        exchange = str(self.params.get("exchange", "binance") or "binance").strip().lower()
        timeframe = str(self.params.get("timeframe", "5m") or "5m").strip().lower()
        lookback = max(
            int(self.params.get("lookback_bars", self.spec.lookback_bars) or self.spec.lookback_bars)
            + int(self.params.get("rebalance_bars", 288) or 288)
            + 5,
            int(self.params.get("min_symbol_bars", 0) or 0),
        )
        max_symbols = max(2, int(self.params.get("max_symbols", 100) or 100))
        out: Dict[str, pd.DataFrame] = {}
        for symbol in universe[:max_symbols]:
            df = await data_storage.load_klines_from_parquet(
                exchange=exchange,
                symbol=symbol,
                timeframe=timeframe,
            )
            if df is None or df.empty:
                continue
            tail = df.tail(lookback).copy()
            if len(tail) < self.spec.lookback_bars + 1:
                continue
            out[symbol] = tail
        return out

    @staticmethod
    def _position_side(position: Any) -> str:
        side = getattr(position, "side", "")
        return str(getattr(side, "value", side) or "").strip().lower()

    @staticmethod
    def _position_symbol(position: Any) -> str:
        return normalize_symbol(getattr(position, "symbol", ""))

    def _active_sets_from_position_manager(self) -> tuple[Set[str], Set[str]]:
        try:
            from core.trading.position_manager import position_manager

            rows = position_manager.get_positions_by_strategy(self.name)
        except Exception:
            return set(self._target_longs), set(self._target_shorts)
        longs: Set[str] = set()
        shorts: Set[str] = set()
        for pos in rows:
            symbol = self._position_symbol(pos)
            side = self._position_side(pos)
            if side == "long":
                longs.add(symbol)
            elif side == "short":
                shorts.add(symbol)
        return longs, shorts

    def _signal_metadata(
        self,
        *,
        plan: Dict[str, Any],
        symbol: str,
        leg: str,
        target_weight: float,
    ) -> Dict[str, Any]:
        factor_map = dict(plan.get("factor") or {})
        rank_map = dict(plan.get("rank") or {})
        target_weights = dict(plan.get("target_weights") or {})
        return {
            "strategy_id": self.spec.strategy_id,
            "factor_name": self.spec.factor_name,
            "factor_value": _safe_float(factor_map.get(symbol), 0.0),
            "cross_section_rank": _safe_float(rank_map.get(symbol), 0.0),
            "long_symbols": list(plan.get("long_symbols") or []),
            "short_symbols": list(plan.get("short_symbols") or []),
            "target_weight": float(target_weight),
            "target_weights": {str(k): float(v) for k, v in target_weights.items()},
            "rebalance_timestamp": pd.Timestamp(plan["rebalance_timestamp"]).isoformat(),
            "lookback_bars": int(self.spec.lookback_bars),
            "rebalance_bars": int(plan.get("rebalance_bars") or self.params.get("rebalance_bars", 288)),
            "long_quantile": float(self.params.get("long_quantile", 0.2)),
            "short_quantile": float(self.params.get("short_quantile", 0.2)),
            "direction": str(self.params.get("direction") or self.spec.direction),
            "factor_leg": leg,
            "model": "intraday_cross_section_long_short",
            "market_type": str(self.params.get("market_type", "future")),
            "max_symbol_weight": float(self.params.get("max_symbol_weight", 0.10) or 0.10),
            "max_portfolio_leverage": float(self.params.get("max_portfolio_leverage", 1.0) or 1.0),
            "fee_bps_per_side": float(self.params.get("fee_bps_per_side", 5.0) or 5.0),
            "min_slippage_bps_per_side": float(self.params.get("min_slippage_bps_per_side", 2.0) or 2.0),
            "slippage_range_multiplier": float(self.params.get("slippage_range_multiplier", 0.08) or 0.08),
            "max_slippage_bps_per_side": float(self.params.get("max_slippage_bps_per_side", 20.0) or 20.0),
            "generic_check_exit_enabled": False,
            "use_atr_stops": False,
        }

    def _make_signal(
        self,
        *,
        plan: Dict[str, Any],
        symbol: str,
        signal_type: SignalType,
        leg: str,
        target_weight: float,
    ) -> Optional[Signal]:
        price = _safe_float(dict(plan.get("price") or {}).get(symbol), 0.0)
        if price <= 0:
            return None
        metadata = self._signal_metadata(plan=plan, symbol=symbol, leg=leg, target_weight=target_weight)
        return Signal(
            symbol=symbol,
            signal_type=signal_type,
            price=price,
            timestamp=bar_time(pd.DataFrame(index=[pd.Timestamp(plan["rebalance_timestamp"])])),
            strategy_name=self.name,
            strength=min(1.0, max(0.2, abs(float(target_weight or 0.0)) * 10.0)),
            metadata=metadata,
        )

    async def generate_signals_async(self, symbol: str) -> List[Signal]:
        universe = self._universe()
        if len(universe) < 2 or not self._is_trigger_symbol(symbol, universe):
            return []

        frames = await self._load_universe_frames(universe)
        plan = await asyncio.to_thread(
            latest_intraday_cross_section_plan,
            self.spec,
            frames,
            dict(self.params),
        )
        if not plan:
            return []

        rebalance_key = self._rebalance_key(plan["rebalance_timestamp"])
        if rebalance_key == self._last_rebalance_key:
            return []

        target_weights = {str(k): float(v) for k, v in dict(plan.get("target_weights") or {}).items()}
        target_longs = {normalize_symbol(sym) for sym in plan.get("long_symbols") or []}
        target_shorts = {normalize_symbol(sym) for sym in plan.get("short_symbols") or []}
        active_longs, active_shorts = self._active_sets_from_position_manager()
        signals: List[Signal] = []

        for sym in sorted(active_longs - target_longs):
            sig = self._make_signal(plan=plan, symbol=sym, signal_type=SignalType.CLOSE_LONG, leg="exit_long", target_weight=0.0)
            if sig:
                sig.metadata["close_only"] = True
                sig.metadata["close_reason"] = "intraday_cross_section_rebalance"
                signals.append(sig)
        for sym in sorted(active_shorts - target_shorts):
            sig = self._make_signal(plan=plan, symbol=sym, signal_type=SignalType.CLOSE_SHORT, leg="exit_short", target_weight=0.0)
            if sig:
                sig.metadata["close_only"] = True
                sig.metadata["close_reason"] = "intraday_cross_section_rebalance"
                signals.append(sig)
        for sym in sorted(target_longs - active_longs):
            sig = self._make_signal(
                plan=plan,
                symbol=sym,
                signal_type=SignalType.BUY,
                leg="long",
                target_weight=target_weights.get(sym, 0.0),
            )
            if sig:
                signals.append(sig)
        if bool(self.params.get("allow_short", True)):
            for sym in sorted(target_shorts - active_shorts):
                sig = self._make_signal(
                    plan=plan,
                    symbol=sym,
                    signal_type=SignalType.SELL,
                    leg="short",
                    target_weight=target_weights.get(sym, 0.0),
                )
                if sig:
                    signals.append(sig)

        self._target_longs = target_longs
        self._target_shorts = target_shorts
        self._last_rebalance_key = rebalance_key
        logger.info(
            f"{self.name} rebalance {rebalance_key} strategy_id={self.spec.strategy_id} "
            f"long={len(target_longs)} short={len(target_shorts)} signals={len(signals)}"
        )
        return signals

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        return []

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "cross_sectional_ohlcv",
            "columns": ["open", "high", "low", "close", "volume"],
            "timeframe": "5m",
            "min_length": int(self.spec.lookback_bars) + int(self.params.get("rebalance_bars", 288) or 288) + 5,
            "multi_symbol": True,
            "execution": "next_bar",
        }


class ResidualMom48hStrategy(IntradayCrossSectionStrategyBase):
    spec_key = "ResidualMom48hStrategy"

    def __init__(self, name: str = "ResidualMom48hStrategy", params: Optional[Dict[str, Any]] = None):
        super().__init__(name=name, params=params)


class Ret24hReversalStrategy(IntradayCrossSectionStrategyBase):
    spec_key = "Ret24hReversalStrategy"

    def __init__(self, name: str = "Ret24hReversalStrategy", params: Optional[Dict[str, Any]] = None):
        super().__init__(name=name, params=params)


class RelRet24hReversalStrategy(IntradayCrossSectionStrategyBase):
    spec_key = "RelRet24hReversalStrategy"

    def __init__(self, name: str = "RelRet24hReversalStrategy", params: Optional[Dict[str, Any]] = None):
        super().__init__(name=name, params=params)


class ResidualMom24hStrategy(IntradayCrossSectionStrategyBase):
    spec_key = "ResidualMom24hStrategy"

    def __init__(self, name: str = "ResidualMom24hStrategy", params: Optional[Dict[str, Any]] = None):
        super().__init__(name=name, params=params)


class CloseLocation48hStrategy(IntradayCrossSectionStrategyBase):
    spec_key = "CloseLocation48hStrategy"

    def __init__(self, name: str = "CloseLocation48hStrategy", params: Optional[Dict[str, Any]] = None):
        super().__init__(name=name, params=params)
