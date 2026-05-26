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
    "ReturnEntropy4hStrategy",
    "FalseBreakoutSupply24hStrategy",
    "RangeAsymmetry48hStrategy",
    "SessionAsiaFlow24hStrategy",
    "SessionFlowRotation24hStrategy",
    "VolumeWeightedReturn24hStrategy",
    "WickImbalance48hStrategy",
    "TurnoverEntropy48hStrategy",
    "BodyVolumeCorr24hStrategy",
    "CorrBreakdown24h72hStrategy",
    "ExtremeRecency48hStrategy",
    "UpDownBetaSpread24h72hStrategy",
    "DirectionalRangeEfficiency48hStrategy",
    "CrossSectionalStress4hStrategy",
    "SignImbalance4hStrategy",
    "VWAPSlope24hStrategy",
    "VWAPGap48hStrategy",
    "RelativeVolShock24hStrategy",
    "LeadMarketResponse24h72hStrategy",
    "BreakCountBalance24hStrategy",
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
    timeframe: str = "5m"
    rebalance_bars: int = 288
    horizon_bars: int = 288
    execution_mode: str = "spread_low_minus_high"
    family: str = "cross_section"
    live_verdict: str = "priority"


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
        execution_mode="spread_high_minus_low",
    ),
    "ReturnEntropy4hStrategy": IntradayCrossSectionSpec(
        strategy_id="return_entropy_4h",
        factor_name="return_entropy",
        lookback_bars=4,
        direction="low",
        description="4h binary return entropy: long low-entropy one-way tapes, short high-entropy chop.",
        timeframe="1h",
        rebalance_bars=24,
        horizon_bars=48,
        execution_mode="spread_low_minus_high",
        family="nonparametric_chop",
        live_verdict="priority",
    ),
    "FalseBreakoutSupply24hStrategy": IntradayCrossSectionSpec(
        strategy_id="false_breakout_supply_24h",
        factor_name="false_breakout_supply",
        lookback_bars=24,
        direction="low",
        description="24h failed new-high supply pressure; short the selected low factor bucket.",
        timeframe="1h",
        rebalance_bars=24,
        horizon_bars=72,
        execution_mode="short_low",
        family="liquidity_rejection",
        live_verdict="candidate",
    ),
    "RangeAsymmetry48hStrategy": IntradayCrossSectionSpec(
        strategy_id="range_asymmetry_48h",
        factor_name="range_asymmetry",
        lookback_bars=576,
        direction="low",
        description="48h intrabar upside-vs-downside range asymmetry.",
        execution_mode="spread_low_minus_high",
        family="intrabar_range_shape",
    ),
    "SessionAsiaFlow24hStrategy": IntradayCrossSectionSpec(
        strategy_id="session_asia_flow_24h",
        factor_name="session_asia_flow",
        lookback_bars=288,
        direction="low",
        description="24h UTC 00-08 volume-weighted directional flow.",
        execution_mode="spread_low_minus_high",
        family="session_flow",
    ),
    "SessionFlowRotation24hStrategy": IntradayCrossSectionSpec(
        strategy_id="session_flow_rotation_24h",
        factor_name="session_flow_rotation",
        lookback_bars=288,
        direction="low",
        description="24h Asia-session flow minus US-session flow rotation.",
        execution_mode="spread_low_minus_high",
        family="session_flow",
    ),
    "VolumeWeightedReturn24hStrategy": IntradayCrossSectionSpec(
        strategy_id="volume_weighted_return_24h",
        factor_name="volume_weighted_return",
        lookback_bars=288,
        direction="low",
        description="24h volume-weighted return pressure.",
        execution_mode="spread_low_minus_high",
        family="volume_confirmed_pressure",
    ),
    "WickImbalance48hStrategy": IntradayCrossSectionSpec(
        strategy_id="wick_imbalance_48h",
        factor_name="wick_imbalance",
        lookback_bars=576,
        direction="low",
        description="48h lower-wick minus upper-wick liquidity rejection imbalance.",
        execution_mode="short_low",
        family="liquidity_rejection",
    ),
    "TurnoverEntropy48hStrategy": IntradayCrossSectionSpec(
        strategy_id="turnover_entropy_48h",
        factor_name="turnover_entropy",
        lookback_bars=192,
        direction="high",
        description="48h turnover concentration entropy on 15m bars.",
        timeframe="15m",
        rebalance_bars=96,
        horizon_bars=192,
        execution_mode="short_high",
        family="attention_structure",
    ),
    "BodyVolumeCorr24hStrategy": IntradayCrossSectionSpec(
        strategy_id="body_volume_corr_24h",
        factor_name="body_volume_correlation",
        lookback_bars=288,
        direction="low",
        description="24h rolling correlation between candle body returns and log dollar volume.",
        execution_mode="spread_low_minus_high",
        family="activity_direction_confirmation",
    ),
    "CorrBreakdown24h72hStrategy": IntradayCrossSectionSpec(
        strategy_id="corr_breakdown_24h_72h",
        factor_name="correlation_breakdown",
        lookback_bars=864,
        direction="high",
        description="24h market-correlation minus 72h market-correlation breakdown.",
        execution_mode="short_high",
        family="market_relation_shift",
    ),
    "ExtremeRecency48hStrategy": IntradayCrossSectionSpec(
        strategy_id="extreme_recency_48h",
        factor_name="extreme_recency",
        lookback_bars=576,
        direction="high",
        description="48h recency of rolling high versus rolling low.",
        execution_mode="short_high",
        family="breakout_timing",
        live_verdict="watchlist",
    ),
    "UpDownBetaSpread24h72hStrategy": IntradayCrossSectionSpec(
        strategy_id="up_down_beta_spread_24h_72h",
        factor_name="up_down_beta_spread",
        lookback_bars=864,
        direction="low",
        description="72h up-market beta minus down-market beta asymmetry.",
        execution_mode="spread_low_minus_high",
        family="asymmetric_market_beta",
    ),
    "DirectionalRangeEfficiency48hStrategy": IntradayCrossSectionSpec(
        strategy_id="directional_range_efficiency_48h",
        factor_name="directional_range_efficiency",
        lookback_bars=576,
        direction="low",
        description="48h signed range expansion per dollar-volume liquidity.",
        execution_mode="spread_low_minus_high",
        family="liquidity_impact",
    ),
    "CrossSectionalStress4hStrategy": IntradayCrossSectionSpec(
        strategy_id="cross_sectional_stress_4h",
        factor_name="cross_sectional_stress",
        lookback_bars=4,
        direction="low",
        description="4h move extremity relative to same-timestamp cross-sectional dispersion.",
        timeframe="1h",
        rebalance_bars=24,
        horizon_bars=72,
        execution_mode="spread_low_minus_high",
        family="relative_shock",
        live_verdict="candidate",
    ),
    "SignImbalance4hStrategy": IntradayCrossSectionSpec(
        strategy_id="sign_imbalance_4h",
        factor_name="sign_imbalance",
        lookback_bars=4,
        direction="high",
        description="4h non-parametric up/down bar sign imbalance.",
        timeframe="1h",
        rebalance_bars=24,
        horizon_bars=48,
        execution_mode="long_high",
        family="nonparametric_trend",
        live_verdict="candidate",
    ),
    "VWAPSlope24hStrategy": IntradayCrossSectionSpec(
        strategy_id="vwap_slope_24h",
        factor_name="vwap_slope",
        lookback_bars=288,
        direction="low",
        description="24h short-VWAP versus long-VWAP slope.",
        execution_mode="spread_low_minus_high",
        family="volume_price_inventory",
        live_verdict="watchlist",
    ),
    "VWAPGap48hStrategy": IntradayCrossSectionSpec(
        strategy_id="vwap_gap_48h",
        factor_name="vwap_gap",
        lookback_bars=576,
        direction="low",
        description="48h close-to-volume-weighted-inventory gap.",
        execution_mode="spread_low_minus_high",
        family="volume_price_inventory",
        live_verdict="watchlist",
    ),
    "RelativeVolShock24hStrategy": IntradayCrossSectionSpec(
        strategy_id="relative_vol_shock_24h",
        factor_name="relative_vol_shock",
        lookback_bars=288,
        direction="low",
        description="24h realized-volatility shock versus cross-sectional peers.",
        rebalance_bars=144,
        horizon_bars=144,
        execution_mode="short_low",
        family="relative_volatility",
    ),
    "LeadMarketResponse24h72hStrategy": IntradayCrossSectionSpec(
        strategy_id="lead_market_response_24h_72h",
        factor_name="lead_market_response",
        lookback_bars=864,
        direction="low",
        description="72h rolling correlation of lagged asset returns with current market returns.",
        execution_mode="spread_low_minus_high",
        family="lead_lag",
        live_verdict="candidate",
    ),
    "BreakCountBalance24hStrategy": IntradayCrossSectionSpec(
        strategy_id="break_count_balance_24h",
        factor_name="break_count_balance",
        lookback_bars=288,
        direction="low",
        description="24h repeated high-break count minus low-break count balance.",
        execution_mode="spread_low_minus_high",
        family="breakout_frequency",
        live_verdict="candidate",
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


def _timeframe_to_seconds(timeframe: str) -> int:
    text = str(timeframe or "5m").strip().lower()
    if not text:
        return 300
    unit = text[-1]
    try:
        value = max(1, int(text[:-1] or 1))
    except Exception:
        return 300
    if unit == "s":
        return value
    if unit == "m":
        return value * 60
    if unit == "h":
        return value * 3600
    if unit == "d":
        return value * 86400
    return 300


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


def _rolling_min_periods(window: int) -> int:
    return max(2, min(int(window), max(2, int(window) // 2)))


def _binary_entropy(p_up: pd.DataFrame, p_down: pd.DataFrame) -> pd.DataFrame:
    up = p_up.where(p_up > 0.0)
    down = p_down.where(p_down > 0.0)
    entropy = -(up * np.log(up) + down * np.log(down))
    return entropy.replace([np.inf, -np.inf], np.nan).fillna(0.0)


def _rolling_entropy(values: pd.DataFrame, window: int) -> pd.DataFrame:
    def entropy_array(raw: np.ndarray) -> float:
        arr = np.asarray(raw, dtype=float)
        arr = arr[np.isfinite(arr) & (arr > 0.0)]
        if arr.size <= 1:
            return 0.0
        total = float(arr.sum())
        if total <= 0.0:
            return 0.0
        p = arr / total
        entropy = -float(np.sum(p * np.log(p)))
        return entropy / float(np.log(arr.size)) if arr.size > 1 else 0.0

    return values.rolling(int(window), min_periods=_rolling_min_periods(window)).apply(entropy_array, raw=True)


def _rolling_extreme_recency(values: pd.DataFrame, window: int, *, high: bool) -> pd.DataFrame:
    def recency_array(raw: np.ndarray) -> float:
        arr = np.asarray(raw, dtype=float)
        if arr.size <= 1 or not np.isfinite(arr).any():
            return 0.0
        filled = np.where(np.isfinite(arr), arr, -np.inf if high else np.inf)
        loc = int(np.argmax(filled) if high else np.argmin(filled))
        return float(loc) / float(max(1, arr.size - 1))

    return values.rolling(int(window), min_periods=_rolling_min_periods(window)).apply(recency_array, raw=True)


def _rolling_corr_with_market(values: pd.DataFrame, market: pd.Series, window: int) -> pd.DataFrame:
    minp = _rolling_min_periods(window)
    m = pd.to_numeric(market, errors="coerce").reindex(values.index)
    out = pd.DataFrame(index=values.index, columns=values.columns, dtype=float)
    for col in values.columns:
        out[col] = pd.to_numeric(values[col], errors="coerce").rolling(window, min_periods=minp).corr(m)
    return out.replace([np.inf, -np.inf], np.nan)


def _conditional_beta_panel(
    returns: pd.DataFrame,
    market: pd.Series,
    window: int,
    *,
    up_market: bool,
) -> pd.DataFrame:
    minp = max(2, min(int(window), max(2, int(window) // 6)))
    m = pd.to_numeric(market, errors="coerce").reindex(returns.index)
    mask = m.gt(0.0) if up_market else m.lt(0.0)
    count = mask.astype(float).rolling(window, min_periods=1).sum()
    valid = count >= minp
    m_sel = m.where(mask)
    m_sum = m_sel.rolling(window, min_periods=1).sum()
    m2_sum = (m_sel * m_sel).rolling(window, min_periods=1).sum()

    out = pd.DataFrame(index=returns.index, columns=returns.columns, dtype=float)
    for col in returns.columns:
        r = pd.to_numeric(returns[col], errors="coerce").where(mask)
        r_sum = r.rolling(window, min_periods=1).sum()
        rm_sum = (r * m_sel).rolling(window, min_periods=1).sum()
        mean_r = r_sum / count.replace(0.0, np.nan)
        mean_m = m_sum / count.replace(0.0, np.nan)
        cov = rm_sum / count.replace(0.0, np.nan) - mean_r * mean_m
        var = m2_sum / count.replace(0.0, np.nan) - mean_m * mean_m
        out[col] = (cov / var.replace(0.0, np.nan)).where(valid)
    return out.replace([np.inf, -np.inf], np.nan)


def _session_mask(index: pd.Index, start_hour: int, end_hour: int) -> pd.Series:
    idx = pd.DatetimeIndex(pd.to_datetime(index, errors="coerce"))
    if idx.tz is not None:
        idx = idx.tz_convert("UTC")
    hours = pd.Series(idx.hour, index=index)
    if int(start_hour) <= int(end_hour):
        active = (hours >= int(start_hour)) & (hours < int(end_hour))
    else:
        active = (hours >= int(start_hour)) | (hours < int(end_hour))
    return active.fillna(False).astype(bool)


def _session_volume_weighted_return(
    returns: pd.DataFrame,
    dollar_volume: pd.DataFrame,
    window: int,
    *,
    start_hour: int,
    end_hour: int,
) -> pd.DataFrame:
    mask = _session_mask(returns.index, start_hour, end_hour)
    weighted = (returns * dollar_volume).where(mask, 0.0)
    volume = dollar_volume.where(mask, 0.0)
    num = weighted.rolling(window, min_periods=_rolling_min_periods(window)).sum()
    den = volume.rolling(window, min_periods=_rolling_min_periods(window)).sum()
    return (num / den.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan)


def _rolling_vwap(
    close: pd.DataFrame,
    high: pd.DataFrame,
    low: pd.DataFrame,
    volume: pd.DataFrame,
    window: int,
) -> pd.DataFrame:
    typical = (high + low + close) / 3.0
    num = (typical * volume).rolling(window, min_periods=_rolling_min_periods(window)).sum()
    den = volume.rolling(window, min_periods=_rolling_min_periods(window)).sum()
    return (num / den.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan)


def _cross_sectional_zscore(values: pd.DataFrame) -> pd.DataFrame:
    mean = values.mean(axis=1, skipna=True)
    std = values.std(axis=1, skipna=True).replace(0.0, np.nan)
    return values.sub(mean, axis=0).div(std, axis=0).replace([np.inf, -np.inf], np.nan)


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
    execution_mode: Optional[str] = None,
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

    mode = str(execution_mode or "").strip().lower()
    direction_text = str(direction or "low").strip().lower()
    if not mode:
        mode = "spread_high_minus_low" if direction_text == "high" else "spread_low_minus_high"

    low_symbols = list(clean.head(max(long_n, short_n)).index)
    high_symbols = list(clean.tail(max(long_n, short_n)).sort_values(ascending=False).index)

    if mode == "spread_high_minus_low":
        long_symbols = high_symbols[:long_n]
        short_symbols = low_symbols[:short_n]
    elif mode == "short_low":
        long_symbols = []
        short_symbols = low_symbols[:short_n]
    elif mode == "short_high":
        long_symbols = []
        short_symbols = high_symbols[:short_n]
    elif mode == "long_high":
        long_symbols = high_symbols[:long_n]
        short_symbols = []
    elif mode == "long_low":
        long_symbols = low_symbols[:long_n]
        short_symbols = []
    else:
        long_symbols = low_symbols[:long_n]
        short_symbols = high_symbols[:short_n]

    ranks = clean.rank(method="first", ascending=True)
    return {
        "ranked": clean,
        "ranks": ranks,
        "long_symbols": long_symbols,
        "short_symbols": short_symbols,
    }


def execution_mode_permissions(execution_mode: str) -> tuple[bool, bool]:
    mode = str(execution_mode or "spread_low_minus_high").strip().lower()
    if mode.startswith("short_"):
        return False, True
    if mode.startswith("long_"):
        return True, False
    return True, True


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
    timeframe: str = "5m",
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
        tf_seconds = max(1, _timeframe_to_seconds(timeframe))
        slot = int(ts.value // (tf_seconds * 1_000_000_000))
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
    close = panels["close"].apply(pd.to_numeric, errors="coerce")
    high = panels["high"].reindex(close.index).reindex(columns=close.columns).apply(pd.to_numeric, errors="coerce")
    low = panels["low"].reindex(close.index).reindex(columns=close.columns).apply(pd.to_numeric, errors="coerce")
    open_ = panels.get("open", close).reindex(close.index).reindex(columns=close.columns).apply(pd.to_numeric, errors="coerce")
    volume = panels.get("volume", pd.DataFrame(index=close.index, columns=close.columns)).reindex(close.index).reindex(columns=close.columns)
    volume = volume.apply(pd.to_numeric, errors="coerce").fillna(0.0).clip(lower=0.0)
    returns = close.pct_change(fill_method=None).replace([np.inf, -np.inf], np.nan)
    dollar_volume = (close * volume).replace([np.inf, -np.inf], np.nan)
    window = max(1, int(spec.lookback_bars or 1))

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
    if spec.factor_name == "return_entropy":
        up_count = returns.gt(0.0).astype(float).rolling(window, min_periods=_rolling_min_periods(window)).sum()
        down_count = returns.lt(0.0).astype(float).rolling(window, min_periods=_rolling_min_periods(window)).sum()
        total = (up_count + down_count).replace(0.0, np.nan)
        return _binary_entropy(up_count / total, down_count / total)
    if spec.factor_name == "false_breakout_supply":
        prev_high = high.rolling(window, min_periods=window).max().shift(1)
        spread = (high - low).replace(0.0, np.nan)
        body_top = open_.where(open_ >= close, close)
        upper_rejection = ((high - body_top) / spread).clip(lower=0.0, upper=1.0)
        probe = high > prev_high
        return upper_rejection.where(probe).rolling(window, min_periods=1).mean()
    if spec.factor_name == "range_asymmetry":
        prev_close = close.shift(1).replace(0.0, np.nan)
        upside_probe = high / prev_close - 1.0
        downside_probe = prev_close / low.replace(0.0, np.nan) - 1.0
        metric = upside_probe - downside_probe
        return metric.rolling(window, min_periods=_rolling_min_periods(window)).mean()
    if spec.factor_name == "session_asia_flow":
        return _session_volume_weighted_return(
            returns,
            dollar_volume,
            window,
            start_hour=0,
            end_hour=8,
        )
    if spec.factor_name == "session_flow_rotation":
        asia = _session_volume_weighted_return(
            returns,
            dollar_volume,
            window,
            start_hour=0,
            end_hour=8,
        )
        us = _session_volume_weighted_return(
            returns,
            dollar_volume,
            window,
            start_hour=16,
            end_hour=24,
        )
        return (asia - us).replace([np.inf, -np.inf], np.nan)
    if spec.factor_name == "volume_weighted_return":
        num = (returns * dollar_volume).rolling(window, min_periods=_rolling_min_periods(window)).sum()
        den = dollar_volume.rolling(window, min_periods=_rolling_min_periods(window)).sum()
        return (num / den.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan)
    if spec.factor_name == "wick_imbalance":
        spread = (high - low).replace(0.0, np.nan)
        body_top = open_.where(open_ >= close, close)
        body_bottom = open_.where(open_ <= close, close)
        lower_wick = ((body_bottom - low) / spread).clip(lower=0.0, upper=1.0)
        upper_wick = ((high - body_top) / spread).clip(lower=0.0, upper=1.0)
        return (lower_wick - upper_wick).rolling(window, min_periods=_rolling_min_periods(window)).mean()
    if spec.factor_name == "turnover_entropy":
        return _rolling_entropy(dollar_volume.fillna(0.0), window)
    if spec.factor_name == "body_volume_correlation":
        body_return = (close / open_.replace(0.0, np.nan) - 1.0).replace([np.inf, -np.inf], np.nan)
        log_dv = np.log1p(dollar_volume.clip(lower=0.0))
        out = pd.DataFrame(index=close.index, columns=close.columns, dtype=float)
        for col in close.columns:
            out[col] = body_return[col].rolling(window, min_periods=_rolling_min_periods(window)).corr(log_dv[col])
        return out.replace([np.inf, -np.inf], np.nan)
    if spec.factor_name == "correlation_breakdown":
        short_window = max(2, window // 3)
        market = returns.mean(axis=1, skipna=True)
        short_corr = _rolling_corr_with_market(returns, market, short_window)
        long_corr = _rolling_corr_with_market(returns, market, window)
        return (short_corr - long_corr).replace([np.inf, -np.inf], np.nan)
    if spec.factor_name == "extreme_recency":
        high_recency = _rolling_extreme_recency(high, window, high=True)
        low_recency = _rolling_extreme_recency(low, window, high=False)
        return (high_recency - low_recency).replace([np.inf, -np.inf], np.nan)
    if spec.factor_name == "up_down_beta_spread":
        market = returns.mean(axis=1, skipna=True)
        beta_up = _conditional_beta_panel(returns, market, window, up_market=True)
        beta_down = _conditional_beta_panel(returns, market, window, up_market=False)
        metric = beta_up.sub(beta_down, fill_value=0.0)
        metric = metric.where(beta_up.notna() | beta_down.notna())
        return metric.replace([np.inf, -np.inf], np.nan)
    if spec.factor_name == "directional_range_efficiency":
        range_pct = (high - low).abs() / close.replace(0.0, np.nan)
        liquidity = np.log1p(dollar_volume.clip(lower=0.0)).replace(0.0, np.nan)
        metric = np.sign(returns.fillna(0.0)) * range_pct / liquidity
        return metric.rolling(window, min_periods=_rolling_min_periods(window)).mean()
    if spec.factor_name == "cross_sectional_stress":
        metric = _cross_sectional_zscore(returns)
        return metric.rolling(window, min_periods=_rolling_min_periods(window)).mean().replace([np.inf, -np.inf], np.nan)
    if spec.factor_name == "sign_imbalance":
        signed = returns.apply(np.sign).replace([np.inf, -np.inf], np.nan)
        return signed.rolling(window, min_periods=_rolling_min_periods(window)).mean()
    if spec.factor_name == "vwap_slope":
        long_vwap = _rolling_vwap(close, high, low, volume, window)
        short_window = max(2, window // 2)
        short_vwap = _rolling_vwap(close, high, low, volume, short_window)
        return (short_vwap / long_vwap.replace(0.0, np.nan) - 1.0).replace([np.inf, -np.inf], np.nan)
    if spec.factor_name == "vwap_gap":
        vwap = _rolling_vwap(close, high, low, volume, window)
        return (close / vwap.replace(0.0, np.nan) - 1.0).replace([np.inf, -np.inf], np.nan)
    if spec.factor_name == "relative_vol_shock":
        vol = returns.rolling(window, min_periods=_rolling_min_periods(window)).std()
        return _cross_sectional_zscore(vol).replace([np.inf, -np.inf], np.nan)
    if spec.factor_name == "lead_market_response":
        market = returns.mean(axis=1, skipna=True)
        return _rolling_corr_with_market(returns.shift(1), market, window)
    if spec.factor_name == "break_count_balance":
        prev_high = high.rolling(window, min_periods=window).max().shift(1)
        prev_low = low.rolling(window, min_periods=window).min().shift(1)
        new_high = (high > prev_high).astype(float)
        new_low = (low < prev_low).astype(float)
        return (new_high - new_low).rolling(window, min_periods=_rolling_min_periods(window)).mean()
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
    rebalance_bars = max(1, int(cfg.get("rebalance_bars", spec.rebalance_bars) or spec.rebalance_bars))
    timeframe = str(cfg.get("timeframe") or spec.timeframe or "5m").strip().lower()
    execution_mode = str(cfg.get("execution_mode") or spec.execution_mode or "spread_low_minus_high").strip().lower()
    effective_spec = IntradayCrossSectionSpec(
        strategy_id=spec.strategy_id,
        factor_name=spec.factor_name,
        lookback_bars=lookback_bars,
        direction=str(cfg.get("direction") or spec.direction),
        description=spec.description,
        timeframe=timeframe,
        rebalance_bars=rebalance_bars,
        horizon_bars=int(cfg.get("horizon_bars", spec.horizon_bars) or spec.horizon_bars),
        execution_mode=execution_mode,
        family=spec.family,
        live_verdict=spec.live_verdict,
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
    rebalance_offset_bars = int(cfg.get("rebalance_offset_bars", 0) or 0)
    max_symbol_weight = max(0.0, float(cfg.get("max_symbol_weight", 0.10) or 0.10))
    max_leverage = max(0.0, float(cfg.get("max_portfolio_leverage", 1.0) or 1.0))
    mode_allow_long, mode_allow_short = execution_mode_permissions(execution_mode)
    allow_short = bool(cfg.get("allow_short", mode_allow_short))
    allow_long = bool(cfg.get("allow_long", mode_allow_long))
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
            timeframe=effective_spec.timeframe,
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
                execution_mode=effective_spec.execution_mode,
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
                    "timeframe": effective_spec.timeframe,
                    "execution_mode": effective_spec.execution_mode,
                    "family": effective_spec.family,
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
        "rebalance_bars": rebalance_bars,
        "timeframe": effective_spec.timeframe,
        "execution_mode": effective_spec.execution_mode,
        "family": effective_spec.family,
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
        "rebalance_bars": int(components.get("rebalance_bars") or spec.rebalance_bars),
        "direction": str((params or {}).get("direction") or spec.direction),
        "timeframe": str(components.get("timeframe") or spec.timeframe),
        "execution_mode": str(components.get("execution_mode") or spec.execution_mode),
        "family": str(components.get("family") or spec.family),
    }


class IntradayCrossSectionStrategyBase(StrategyBase):
    """Base class for cross-sectional Binance USD-M long/short factor strategies."""

    mutates_input = False
    spec_key = "Ret24hReversalStrategy"

    def __init__(self, name: str, params: Optional[Dict[str, Any]] = None):
        spec = INTRADAY_CROSS_SECTION_SPECS[self.spec_key]
        mode_allow_long, mode_allow_short = execution_mode_permissions(spec.execution_mode)
        default_params: Dict[str, Any] = {
            "strategy_id": spec.strategy_id,
            "timeframe": spec.timeframe,
            "exchange": "binance",
            "market_type": "future",
            "universe_symbols": list(DEFAULT_INTRADAY_CROSS_SECTION_UNIVERSE),
            "max_symbols": 100,
            "lookback_bars": spec.lookback_bars,
            "rebalance_bars": spec.rebalance_bars,
            "rebalance_offset_bars": 0,
            "horizon_bars": spec.horizon_bars,
            "long_quantile": 0.2,
            "short_quantile": 0.2,
            "direction": spec.direction,
            "execution_mode": spec.execution_mode,
            "family": spec.family,
            "live_verdict": spec.live_verdict,
            "max_symbol_weight": 0.10,
            "min_quote_volume": 0.0,
            "min_universe_size": 5,
            "min_names_per_side": 1,
            "max_names_per_side": None,
            "allow_long": mode_allow_long,
            "allow_short": mode_allow_short,
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
        default_params["rebalance_bars"] = int(default_params.get("rebalance_bars") or spec.rebalance_bars)
        default_params["horizon_bars"] = int(default_params.get("horizon_bars") or spec.horizon_bars)
        default_params["timeframe"] = str(default_params.get("timeframe") or spec.timeframe).strip().lower()
        default_params["direction"] = str(default_params.get("direction") or spec.direction)
        default_params["execution_mode"] = str(default_params.get("execution_mode") or spec.execution_mode).strip().lower()
        override_allow_long, override_allow_short = execution_mode_permissions(default_params["execution_mode"])
        if not params or "allow_long" not in params:
            default_params["allow_long"] = override_allow_long
        if not params or "allow_short" not in params:
            default_params["allow_short"] = override_allow_short
        super().__init__(name=name, params=default_params)
        self.spec = IntradayCrossSectionSpec(
            strategy_id=spec.strategy_id,
            factor_name=spec.factor_name,
            lookback_bars=int(default_params["lookback_bars"]),
            direction=str(default_params["direction"]),
            description=spec.description,
            timeframe=str(default_params["timeframe"]),
            rebalance_bars=int(default_params["rebalance_bars"]),
            horizon_bars=int(default_params["horizon_bars"]),
            execution_mode=str(default_params["execution_mode"]),
            family=spec.family,
            live_verdict=spec.live_verdict,
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

    def _max_data_age_seconds(self) -> float:
        raw = self.params.get("max_data_age_seconds")
        if raw is not None:
            try:
                return max(0.0, float(raw))
            except Exception:
                pass
        timeframe_seconds = _timeframe_to_seconds(str(self.params.get("timeframe") or self.spec.timeframe))
        return float(max(3 * timeframe_seconds, 15 * 60))

    @staticmethod
    def _to_utc_timestamp(value: Any) -> Optional[pd.Timestamp]:
        try:
            ts = pd.Timestamp(value)
        except Exception:
            return None
        if pd.isna(ts):
            return None
        if ts.tzinfo is None:
            return ts.tz_localize("UTC")
        return ts.tz_convert("UTC")

    def _plan_is_fresh(self, plan: Dict[str, Any]) -> bool:
        max_age = self._max_data_age_seconds()
        if max_age <= 0:
            return True
        ts = self._to_utc_timestamp(plan.get("rebalance_timestamp"))
        if ts is None:
            return False
        now = pd.Timestamp.now(tz="UTC")
        age_seconds = (now - ts).total_seconds()
        return age_seconds <= max_age

    async def _load_universe_frames(self, universe: List[str]) -> Dict[str, pd.DataFrame]:
        exchange = str(self.params.get("exchange", "binance") or "binance").strip().lower()
        timeframe = str(self.params.get("timeframe", "5m") or "5m").strip().lower()
        lookback = max(
            int(self.params.get("lookback_bars", self.spec.lookback_bars) or self.spec.lookback_bars)
            + int(self.params.get("rebalance_bars", self.spec.rebalance_bars) or self.spec.rebalance_bars)
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

    def _active_positions_from_position_manager(self) -> tuple[Dict[str, Any], Dict[str, Any]]:
        try:
            from core.trading.position_manager import position_manager

            rows = position_manager.get_positions_by_strategy(self.name)
        except Exception:
            return {sym: None for sym in self._target_longs}, {sym: None for sym in self._target_shorts}
        longs: Dict[str, Any] = {}
        shorts: Dict[str, Any] = {}
        for pos in rows:
            symbol = self._position_symbol(pos)
            side = self._position_side(pos)
            if not symbol:
                continue
            if side == "long":
                longs[symbol] = pos
            elif side == "short":
                shorts[symbol] = pos
        return longs, shorts

    @staticmethod
    def _position_target_weight(position: Any) -> Optional[float]:
        metadata = getattr(position, "metadata", None) or {}
        if not isinstance(metadata, dict):
            return None
        if "target_weight" not in metadata:
            return None
        try:
            value = float(metadata.get("target_weight"))
        except Exception:
            return None
        if not math.isfinite(value):
            return None
        return value

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
            "rebalance_bars": int(plan.get("rebalance_bars") or self.params.get("rebalance_bars", self.spec.rebalance_bars)),
            "horizon_bars": int(self.params.get("horizon_bars", self.spec.horizon_bars) or self.spec.horizon_bars),
            "timeframe": str(self.params.get("timeframe") or self.spec.timeframe),
            "execution_mode": str(self.params.get("execution_mode") or self.spec.execution_mode),
            "family": str(self.spec.family),
            "live_verdict": str(self.spec.live_verdict),
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
        if not self._plan_is_fresh(plan):
            rebalance_ts = plan.get("rebalance_timestamp")
            logger.warning(
                f"{self.name} skipped stale cross-section plan ts={rebalance_ts} "
                f"max_age_seconds={self._max_data_age_seconds():.0f}"
            )
            return []

        rebalance_key = self._rebalance_key(plan["rebalance_timestamp"])
        if rebalance_key == self._last_rebalance_key:
            return []

        target_weights = {
            normalize_symbol(k): float(v)
            for k, v in dict(plan.get("target_weights") or {}).items()
            if normalize_symbol(k)
        }
        target_longs = {normalize_symbol(sym) for sym in plan.get("long_symbols") or [] if normalize_symbol(sym)}
        target_shorts = {normalize_symbol(sym) for sym in plan.get("short_symbols") or [] if normalize_symbol(sym)}
        active_long_positions, active_short_positions = self._active_positions_from_position_manager()
        active_longs = set(active_long_positions)
        active_shorts = set(active_short_positions)
        resize_tolerance = max(0.0, float(self.params.get("rebalance_weight_tolerance", 0.01) or 0.0))
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
        resized_longs: Set[str] = set()
        resized_shorts: Set[str] = set()
        for sym in sorted(active_longs & target_longs):
            current_weight = self._position_target_weight(active_long_positions.get(sym))
            target_weight = target_weights.get(sym, 0.0)
            if current_weight is None or abs(target_weight - current_weight) <= resize_tolerance:
                continue
            sig = self._make_signal(
                plan=plan,
                symbol=sym,
                signal_type=SignalType.CLOSE_LONG,
                leg="resize_exit_long",
                target_weight=0.0,
            )
            if sig:
                sig.metadata["close_only"] = True
                sig.metadata["close_reason"] = "intraday_cross_section_rebalance_resize"
                sig.metadata["rebalance_resize"] = True
                signals.append(sig)
                resized_longs.add(sym)
        if bool(self.params.get("allow_short", True)):
            for sym in sorted(active_shorts & target_shorts):
                current_weight = self._position_target_weight(active_short_positions.get(sym))
                target_weight = target_weights.get(sym, 0.0)
                if current_weight is None or abs(target_weight - current_weight) <= resize_tolerance:
                    continue
                sig = self._make_signal(
                    plan=plan,
                    symbol=sym,
                    signal_type=SignalType.CLOSE_SHORT,
                    leg="resize_exit_short",
                    target_weight=0.0,
                )
                if sig:
                    sig.metadata["close_only"] = True
                    sig.metadata["close_reason"] = "intraday_cross_section_rebalance_resize"
                    sig.metadata["rebalance_resize"] = True
                    signals.append(sig)
                    resized_shorts.add(sym)
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
        for sym in sorted(resized_longs):
            sig = self._make_signal(
                plan=plan,
                symbol=sym,
                signal_type=SignalType.BUY,
                leg="long",
                target_weight=target_weights.get(sym, 0.0),
            )
            if sig:
                sig.metadata["rebalance_resize"] = True
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
            for sym in sorted(resized_shorts):
                sig = self._make_signal(
                    plan=plan,
                    symbol=sym,
                    signal_type=SignalType.SELL,
                    leg="short",
                    target_weight=target_weights.get(sym, 0.0),
                )
                if sig:
                    sig.metadata["rebalance_resize"] = True
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
            "timeframe": str(self.params.get("timeframe") or self.spec.timeframe),
            "min_length": int(self.spec.lookback_bars) + int(self.params.get("rebalance_bars", self.spec.rebalance_bars) or self.spec.rebalance_bars) + 5,
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


class ReturnEntropy4hStrategy(IntradayCrossSectionStrategyBase):
    spec_key = "ReturnEntropy4hStrategy"

    def __init__(self, name: str = "ReturnEntropy4hStrategy", params: Optional[Dict[str, Any]] = None):
        super().__init__(name=name, params=params)


class FalseBreakoutSupply24hStrategy(IntradayCrossSectionStrategyBase):
    spec_key = "FalseBreakoutSupply24hStrategy"

    def __init__(self, name: str = "FalseBreakoutSupply24hStrategy", params: Optional[Dict[str, Any]] = None):
        super().__init__(name=name, params=params)


class RangeAsymmetry48hStrategy(IntradayCrossSectionStrategyBase):
    spec_key = "RangeAsymmetry48hStrategy"

    def __init__(self, name: str = "RangeAsymmetry48hStrategy", params: Optional[Dict[str, Any]] = None):
        super().__init__(name=name, params=params)


class SessionAsiaFlow24hStrategy(IntradayCrossSectionStrategyBase):
    spec_key = "SessionAsiaFlow24hStrategy"

    def __init__(self, name: str = "SessionAsiaFlow24hStrategy", params: Optional[Dict[str, Any]] = None):
        super().__init__(name=name, params=params)


class SessionFlowRotation24hStrategy(IntradayCrossSectionStrategyBase):
    spec_key = "SessionFlowRotation24hStrategy"

    def __init__(self, name: str = "SessionFlowRotation24hStrategy", params: Optional[Dict[str, Any]] = None):
        super().__init__(name=name, params=params)


class VolumeWeightedReturn24hStrategy(IntradayCrossSectionStrategyBase):
    spec_key = "VolumeWeightedReturn24hStrategy"

    def __init__(self, name: str = "VolumeWeightedReturn24hStrategy", params: Optional[Dict[str, Any]] = None):
        super().__init__(name=name, params=params)


class WickImbalance48hStrategy(IntradayCrossSectionStrategyBase):
    spec_key = "WickImbalance48hStrategy"

    def __init__(self, name: str = "WickImbalance48hStrategy", params: Optional[Dict[str, Any]] = None):
        super().__init__(name=name, params=params)


class TurnoverEntropy48hStrategy(IntradayCrossSectionStrategyBase):
    spec_key = "TurnoverEntropy48hStrategy"

    def __init__(self, name: str = "TurnoverEntropy48hStrategy", params: Optional[Dict[str, Any]] = None):
        super().__init__(name=name, params=params)


class BodyVolumeCorr24hStrategy(IntradayCrossSectionStrategyBase):
    spec_key = "BodyVolumeCorr24hStrategy"

    def __init__(self, name: str = "BodyVolumeCorr24hStrategy", params: Optional[Dict[str, Any]] = None):
        super().__init__(name=name, params=params)


class CorrBreakdown24h72hStrategy(IntradayCrossSectionStrategyBase):
    spec_key = "CorrBreakdown24h72hStrategy"

    def __init__(self, name: str = "CorrBreakdown24h72hStrategy", params: Optional[Dict[str, Any]] = None):
        super().__init__(name=name, params=params)


class ExtremeRecency48hStrategy(IntradayCrossSectionStrategyBase):
    spec_key = "ExtremeRecency48hStrategy"

    def __init__(self, name: str = "ExtremeRecency48hStrategy", params: Optional[Dict[str, Any]] = None):
        super().__init__(name=name, params=params)


class UpDownBetaSpread24h72hStrategy(IntradayCrossSectionStrategyBase):
    spec_key = "UpDownBetaSpread24h72hStrategy"

    def __init__(self, name: str = "UpDownBetaSpread24h72hStrategy", params: Optional[Dict[str, Any]] = None):
        super().__init__(name=name, params=params)


class DirectionalRangeEfficiency48hStrategy(IntradayCrossSectionStrategyBase):
    spec_key = "DirectionalRangeEfficiency48hStrategy"

    def __init__(self, name: str = "DirectionalRangeEfficiency48hStrategy", params: Optional[Dict[str, Any]] = None):
        super().__init__(name=name, params=params)


class CrossSectionalStress4hStrategy(IntradayCrossSectionStrategyBase):
    spec_key = "CrossSectionalStress4hStrategy"

    def __init__(self, name: str = "CrossSectionalStress4hStrategy", params: Optional[Dict[str, Any]] = None):
        super().__init__(name=name, params=params)


class SignImbalance4hStrategy(IntradayCrossSectionStrategyBase):
    spec_key = "SignImbalance4hStrategy"

    def __init__(self, name: str = "SignImbalance4hStrategy", params: Optional[Dict[str, Any]] = None):
        super().__init__(name=name, params=params)


class VWAPSlope24hStrategy(IntradayCrossSectionStrategyBase):
    spec_key = "VWAPSlope24hStrategy"

    def __init__(self, name: str = "VWAPSlope24hStrategy", params: Optional[Dict[str, Any]] = None):
        super().__init__(name=name, params=params)


class VWAPGap48hStrategy(IntradayCrossSectionStrategyBase):
    spec_key = "VWAPGap48hStrategy"

    def __init__(self, name: str = "VWAPGap48hStrategy", params: Optional[Dict[str, Any]] = None):
        super().__init__(name=name, params=params)


class RelativeVolShock24hStrategy(IntradayCrossSectionStrategyBase):
    spec_key = "RelativeVolShock24hStrategy"

    def __init__(self, name: str = "RelativeVolShock24hStrategy", params: Optional[Dict[str, Any]] = None):
        super().__init__(name=name, params=params)


class LeadMarketResponse24h72hStrategy(IntradayCrossSectionStrategyBase):
    spec_key = "LeadMarketResponse24h72hStrategy"

    def __init__(self, name: str = "LeadMarketResponse24h72hStrategy", params: Optional[Dict[str, Any]] = None):
        super().__init__(name=name, params=params)


class BreakCountBalance24hStrategy(IntradayCrossSectionStrategyBase):
    spec_key = "BreakCountBalance24hStrategy"

    def __init__(self, name: str = "BreakCountBalance24hStrategy", params: Optional[Dict[str, Any]] = None):
        super().__init__(name=name, params=params)
