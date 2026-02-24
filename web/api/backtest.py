"""Backtest API endpoints."""
import io
import itertools
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

import matplotlib
matplotlib.use("Agg")
import numpy as np
import pandas as pd
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from matplotlib import pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

from core.data import data_storage

router = APIRouter()

_SUB_MINUTE_TIMEFRAMES = {"1s", "5s", "10s", "30s"}
_RESAMPLE_RULES = {
    "1s": "1S",
    "5s": "5S",
    "10s": "10S",
    "30s": "30S",
    "1m": "1T",
    "5m": "5T",
    "15m": "15T",
    "30m": "30T",
    "1h": "1H",
    "4h": "4H",
    "1d": "1D",
    "1w": "1W",
    "1M": "1MS",
}

_RETURN_CLIP_BY_TIMEFRAME = {
    "1s": 0.03,
    "5s": 0.05,
    "10s": 0.07,
    "30s": 0.12,
    "1m": 0.20,
    "5m": 0.35,
    "15m": 0.50,
    "30m": 0.80,
    "1h": 1.20,
    "4h": 1.80,
    "1d": 3.00,
}


_BACKTEST_STRATEGY_CATALOG: List[Dict[str, Any]] = [
    {"name": "MAStrategy", "description": "双均线趋势策略（Long/Flat）", "backtest_supported": True},
    {"name": "EMAStrategy", "description": "EMA双均线趋势策略", "backtest_supported": True},
    {"name": "RSIStrategy", "description": "RSI超卖入场、超买离场", "backtest_supported": True},
    {"name": "RSIDivergenceStrategy", "description": "RSI背离策略", "backtest_supported": True},
    {"name": "MACDStrategy", "description": "MACD上穿持有、下穿离场", "backtest_supported": True},
    {"name": "MACDHistogramStrategy", "description": "MACD柱体拐点策略", "backtest_supported": True},
    {"name": "BollingerBandsStrategy", "description": "布林带均值回归策略", "backtest_supported": True},
    {"name": "BollingerSqueezeStrategy", "description": "布林挤压突破策略", "backtest_supported": True},
    {"name": "MeanReversionStrategy", "description": "Z-Score均值回归策略", "backtest_supported": True},
    {"name": "BollingerMeanReversionStrategy", "description": "布林均值回归策略", "backtest_supported": True},
    {"name": "MomentumStrategy", "description": "动量突破策略", "backtest_supported": True},
    {"name": "TrendFollowingStrategy", "description": "趋势跟随策略", "backtest_supported": True},
    {"name": "PairsTradingStrategy", "description": "配对交易策略（近似单腿回测）", "backtest_supported": True},
    {"name": "DonchianBreakoutStrategy", "description": "Donchian通道突破策略", "backtest_supported": True},
    {"name": "StochasticStrategy", "description": "随机指标策略", "backtest_supported": True},
    {"name": "ADXTrendStrategy", "description": "ADX趋势策略", "backtest_supported": True},
    {"name": "VWAPReversionStrategy", "description": "VWAP偏离回归策略", "backtest_supported": True},
    {"name": "MarketSentimentStrategy", "description": "宏观情绪策略", "backtest_supported": True},
    {"name": "SocialSentimentStrategy", "description": "社媒情绪策略", "backtest_supported": True},
    {"name": "FundFlowStrategy", "description": "资金流策略", "backtest_supported": True},
    {"name": "WhaleActivityStrategy", "description": "巨鲸活动策略", "backtest_supported": True},
    {
        "name": "CEXArbitrageStrategy",
        "description": "跨交易所套利策略",
        "backtest_supported": False,
        "reason": "依赖多交易所盘口/买卖价差，非单一OHLCV回测模型",
    },
    {
        "name": "TriangularArbitrageStrategy",
        "description": "三角套利策略",
        "backtest_supported": False,
        "reason": "依赖同交易所多交易对实时报价，不适用单一K线回测",
    },
    {
        "name": "DEXArbitrageStrategy",
        "description": "DEX套利策略",
        "backtest_supported": False,
        "reason": "依赖链上流动性池与实时路由报价",
    },
    {
        "name": "FlashLoanArbitrageStrategy",
        "description": "闪电贷套利策略",
        "backtest_supported": False,
        "reason": "依赖链上原子交易执行，K线回测无法刻画",
    },
]

_BACKTEST_STRATEGY_META: Dict[str, Dict[str, Any]] = {
    str(item["name"]): dict(item) for item in _BACKTEST_STRATEGY_CATALOG
}

_BACKTEST_OPTIMIZATION_GRIDS: Dict[str, Dict[str, List[Any]]] = {
    "MAStrategy": {
        "fast_period": [5, 8, 10, 12, 20],
        "slow_period": [20, 30, 40, 60],
    },
    "EMAStrategy": {
        "fast_period": [8, 12, 16],
        "slow_period": [21, 26, 34, 55],
    },
    "RSIStrategy": {
        "period": [10, 14, 21],
        "oversold": [20, 25, 30],
        "overbought": [65, 70, 75],
    },
    "RSIDivergenceStrategy": {
        "period": [10, 14, 21],
        "lookback": [12, 20, 30],
        "min_divergence": [0.01, 0.02, 0.03],
        "extrema_order": [3, 5, 7],
    },
    "MACDStrategy": {
        "fast_period": [8, 12, 16],
        "slow_period": [21, 26, 34],
        "signal_period": [7, 9, 12],
    },
    "MACDHistogramStrategy": {
        "fast_period": [8, 12, 16],
        "slow_period": [21, 26, 34],
        "signal_period": [7, 9, 12],
        "min_histogram": [0.00005, 0.0001, 0.0002],
    },
    "BollingerBandsStrategy": {
        "period": [14, 20, 26],
        "num_std": [1.8, 2.0, 2.2, 2.5],
    },
    "BollingerSqueezeStrategy": {
        "period": [14, 20, 26],
        "num_std": [1.8, 2.0, 2.2],
        "squeeze_threshold": [0.01, 0.02, 0.03],
        "breakout_threshold": [0.005, 0.01, 0.015],
    },
    "MeanReversionStrategy": {
        "lookback_period": [14, 20, 30],
        "entry_z_score": [1.5, 2.0, 2.5],
    },
    "BollingerMeanReversionStrategy": {
        "period": [14, 20, 26],
        "num_std": [1.8, 2.0, 2.2, 2.5],
    },
    "MomentumStrategy": {
        "lookback_period": [10, 14, 20, 30],
        "momentum_threshold": [0.01, 0.015, 0.02, 0.03],
    },
    "TrendFollowingStrategy": {
        "short_period": [10, 20, 30],
        "long_period": [40, 50, 80],
        "adx_threshold": [20, 25, 30],
    },
    "PairsTradingStrategy": {
        "lookback_period": [14, 20, 30, 40],
        "entry_z_score": [1.5, 2.0, 2.5],
        "exit_z_score": [0.3, 0.5, 0.8],
    },
    "DonchianBreakoutStrategy": {
        "lookback": [14, 20, 30],
        "exit_lookback": [7, 10, 14],
    },
    "StochasticStrategy": {
        "k_period": [9, 14, 21],
        "d_period": [3, 5],
        "oversold": [15, 20, 25],
        "overbought": [75, 80, 85],
    },
    "ADXTrendStrategy": {
        "period": [10, 14, 20],
        "adx_threshold": [20, 25, 30],
    },
    "VWAPReversionStrategy": {
        "window": [24, 48, 72],
        "entry_deviation_pct": [0.006, 0.01, 0.015],
        "exit_deviation_pct": [0.001, 0.002, 0.003],
    },
    "MarketSentimentStrategy": {
        "lookback_period": [12, 24, 36],
        "panic_threshold": [-0.06, -0.04, -0.03],
        "euphoria_threshold": [0.03, 0.04, 0.06],
    },
    "SocialSentimentStrategy": {
        "lookback_period": [6, 12, 24],
        "enter_momentum": [0.008, 0.015, 0.02],
        "exit_momentum": [-0.01, -0.008, -0.005],
    },
    "FundFlowStrategy": {
        "min_imbalance_ratio": [0.05, 0.08, 0.12],
        "inflow_threshold": [200000, 500000, 1000000],
        "outflow_threshold": [-200000, -500000, -1000000],
    },
    "WhaleActivityStrategy": {
        "min_whale_size": [100000, 150000, 300000],
        "accumulation_threshold": [3, 4, 5],
        "distribution_threshold": [3, 4, 5],
    },
}
_BACKTEST_OPT_OBJECTIVES = {"total_return", "sharpe_ratio", "win_rate"}


def get_backtest_strategy_catalog() -> List[Dict[str, Any]]:
    return [dict(item) for item in _BACKTEST_STRATEGY_CATALOG]


def get_backtest_strategy_info(strategy: str) -> Dict[str, Any]:
    return dict(_BACKTEST_STRATEGY_META.get(str(strategy), {}))


def is_strategy_backtest_supported(strategy: str) -> bool:
    info = _BACKTEST_STRATEGY_META.get(str(strategy), {})
    return bool(info.get("backtest_supported", False))


def _annual_factor(timeframe: str) -> int:
    tf = (timeframe or "1d")
    unit = tf[-1]
    value = int(tf[:-1] or 1)

    if unit == "s":
        return max(1, int((365 * 24 * 3600) / value))
    if unit == "m":
        return max(1, int((365 * 24 * 60) / value))
    if unit == "h":
        return max(1, int((365 * 24) / value))
    if unit == "d":
        return max(1, int(365 / value))
    if unit == "w":
        return max(1, int(52 / value))
    if unit == "M":
        return max(1, int(12 / value))
    return 252


def _return_clip_limit(timeframe: str) -> float:
    return float(_RETURN_CLIP_BY_TIMEFRAME.get(str(timeframe or "").strip(), 2.0))


def _is_date_only_input(value: str) -> bool:
    s = str(value or "").strip()
    return bool(s) and ("T" not in s) and (" " not in s) and len(s) <= 10


def _parse_backtest_bound(value: Optional[str], *, bound: str) -> Optional[pd.Timestamp]:
    if not value:
        return None
    try:
        ts = pd.to_datetime(value)
    except Exception:
        raise HTTPException(
            status_code=400,
            detail=f"{bound} 格式错误，应为 YYYY-MM-DD 或 YYYY-MM-DDTHH:MM",
        )
    if bound == "end_date" and _is_date_only_input(str(value)):
        ts = ts + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    return ts


def _safe_bar_returns(close: pd.Series, timeframe: str) -> tuple[pd.Series, float, float]:
    raw = pd.to_numeric(close, errors="coerce").pct_change().replace([np.inf, -np.inf], np.nan).fillna(0.0)
    clip_limit = _return_clip_limit(timeframe)
    clipped = raw.clip(lower=-clip_limit, upper=clip_limit)
    anomaly_ratio = float((raw.abs() > clip_limit).mean() or 0.0)
    return clipped, anomaly_ratio, clip_limit


def _safe_equity_curve(returns: pd.Series, initial_capital: float) -> pd.Series:
    safe = pd.to_numeric(returns, errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
    safe = safe.clip(lower=-0.95, upper=5.0)
    log_curve = np.log1p(safe).cumsum().clip(lower=-50.0, upper=20.0)
    return pd.Series(np.exp(log_curve) * float(initial_capital), index=returns.index)


def _resample_ohlcv(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    if df.empty:
        return df
    rule = _RESAMPLE_RULES.get(timeframe)
    if not rule:
        return pd.DataFrame()

    src = df.copy()
    src.index = pd.to_datetime(src.index)
    src = src.sort_index()

    ohlc = src[["open", "high", "low", "close"]].resample(rule).agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
        }
    )
    volume = src[["volume"]].resample(rule).sum()
    out = pd.concat([ohlc, volume], axis=1).dropna(subset=["open", "high", "low", "close"])
    return out


def _min_required_bars(timeframe: str) -> int:
    tf = str(timeframe or "").strip()
    if tf in _SUB_MINUTE_TIMEFRAMES:
        return 15
    return 30


def _candidate_base_timeframes(timeframe: str) -> List[str]:
    tf = str(timeframe or "").strip()
    mapping: Dict[str, List[str]] = {
        "5s": ["1s"],
        "10s": ["1s"],
        "30s": ["1s", "5s", "10s"],
        "1m": ["30s", "10s", "5s", "1s"],
        "5m": ["1m", "30s", "10s", "5s", "1s"],
        "15m": ["5m", "1m", "30s"],
        "30m": ["15m", "5m", "1m"],
        "1h": ["30m", "15m", "5m", "1m"],
        "4h": ["1h", "30m", "15m", "5m", "1m"],
        "1d": ["4h", "1h", "30m", "15m", "5m", "1m"],
        "1w": ["1d", "4h", "1h"],
        "1M": ["1d", "4h", "1h"],
    }
    return list(mapping.get(tf, []))


def _default_subminute_lookback_days(timeframe: str) -> int:
    tf = str(timeframe or "").strip()
    mapping = {
        "1s": 2,
        "5s": 5,
        "10s": 7,
        "30s": 14,
    }
    return int(mapping.get(tf, 14))


def _normalize_optimize_objective(objective: str) -> str:
    text = str(objective or "").strip()
    return text if text in _BACKTEST_OPT_OBJECTIVES else "total_return"


def _optimize_strategy_on_df(
    strategy: str,
    df: pd.DataFrame,
    timeframe: str,
    initial_capital: float,
    commission_rate: float,
    slippage_bps: float,
    objective: str = "total_return",
    max_trials: int = 64,
) -> Dict[str, Any]:
    if strategy not in _BACKTEST_OPTIMIZATION_GRIDS:
        raise ValueError(f"暂不支持 {strategy} 参数优化")

    grid = _BACKTEST_OPTIMIZATION_GRIDS[strategy]
    keys = list(grid.keys())
    values = [grid[k] for k in keys]
    all_combos = list(itertools.product(*values))

    limit = max(1, int(max_trials or 1))
    if len(all_combos) > limit:
        all_combos = all_combos[:limit]

    objective_key = _normalize_optimize_objective(objective)
    trials: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []

    for combo in all_combos:
        params = {keys[idx]: combo[idx] for idx in range(len(keys))}
        try:
            metrics = _run_backtest_core(
                strategy=strategy,
                df=df,
                timeframe=timeframe,
                initial_capital=initial_capital,
                params=params,
                include_series=False,
                commission_rate=max(0.0, float(commission_rate or 0.0)),
                slippage_bps=max(0.0, float(slippage_bps or 0.0)),
            )
            score = float(metrics.get(objective_key, 0))
            trials.append({"params": params, "metrics": metrics, "score": score})
        except Exception as exc:
            failures.append({"params": params, "error": str(exc)})

    trials.sort(key=lambda x: x["score"], reverse=True)
    best = trials[0] if trials else None
    all_trials_summary = [
        {
            "params": dict(item.get("params") or {}),
            "score": float(item.get("score") or 0.0),
            "total_return": float(item.get("metrics", {}).get("total_return") or 0.0),
            "sharpe_ratio": float(item.get("metrics", {}).get("sharpe_ratio") or 0.0),
            "max_drawdown": float(item.get("metrics", {}).get("max_drawdown") or 0.0),
            "win_rate": float(item.get("metrics", {}).get("win_rate") or 0.0),
            "total_trades": int(item.get("metrics", {}).get("total_trades") or 0),
        }
        for item in trials
    ]
    return {
        "strategy": strategy,
        "objective": objective_key,
        "trials": len(trials),
        "failed_trials": len(failures),
        "best": best,
        "top": trials[: min(10, len(trials))],
        "all_trials": all_trials_summary,
        "failures": failures[: min(5, len(failures))],
    }


def _build_positions(strategy: str, df: pd.DataFrame, params: Optional[Dict[str, Any]] = None) -> pd.Series:
    params = params or {}
    close = df["close"]
    position = pd.Series(0.0, index=df.index)

    if strategy == "MAStrategy":
        fast_n = int(params.get("fast_period", 10))
        slow_n = int(params.get("slow_period", 30))
        fast = close.rolling(fast_n, min_periods=fast_n).mean()
        slow = close.rolling(slow_n, min_periods=slow_n).mean()
        position = (fast > slow).astype(float)
    elif strategy == "EMAStrategy":
        ema_fast = close.ewm(span=int(params.get("fast_period", 12)), adjust=False).mean()
        ema_slow = close.ewm(span=int(params.get("slow_period", 26)), adjust=False).mean()
        position = (ema_fast > ema_slow).astype(float)
    elif strategy in {"RSIStrategy", "RSIDivergenceStrategy"}:
        period = int(params.get("period", 14))
        oversold = float(params.get("oversold", 30))
        overbought = float(params.get("overbought", 70))

        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = (-delta).clip(lower=0)
        avg_gain = gain.rolling(period, min_periods=period).mean()
        avg_loss = loss.rolling(period, min_periods=period).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))

        in_position = False
        values = []
        for val in rsi.fillna(50):
            if not in_position and val <= oversold:
                in_position = True
            elif in_position and val >= overbought:
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)
    elif strategy in {"MACDStrategy", "MACDHistogramStrategy"}:
        fast_n = int(params.get("fast_period", 12))
        slow_n = int(params.get("slow_period", 26))
        signal_n = int(params.get("signal_period", 9))

        ema_fast = close.ewm(span=fast_n, adjust=False).mean()
        ema_slow = close.ewm(span=slow_n, adjust=False).mean()
        macd = ema_fast - ema_slow
        signal = macd.ewm(span=signal_n, adjust=False).mean()
        position = (macd > signal).astype(float)
    elif strategy in {"BollingerBandsStrategy", "BollingerSqueezeStrategy"}:
        period = int(params.get("period", 20))
        num_std = float(params.get("num_std", 2.0))

        ma = close.rolling(period, min_periods=period).mean()
        std = close.rolling(period, min_periods=period).std()
        upper = ma + num_std * std
        lower = ma - num_std * std

        in_position = False
        values = []
        for c, up, lo in zip(close.ffill(), upper.fillna(float("inf")), lower.fillna(float("-inf"))):
            if not in_position and c <= lo:
                in_position = True
            elif in_position and c >= up:
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)
    elif strategy in {"MeanReversionStrategy", "BollingerMeanReversionStrategy", "PairsTradingStrategy"}:
        period = int(params.get("lookback_period", 20))
        z_entry = float(params.get("entry_z_score", 2.0))

        mean = close.rolling(period, min_periods=period).mean()
        std = close.rolling(period, min_periods=period).std()
        z = (close - mean) / std.replace(0, np.nan)

        in_position = False
        values = []
        for z_val in z.fillna(0):
            if not in_position and z_val <= -z_entry:
                in_position = True
            elif in_position and z_val >= 0:
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)
    elif strategy in {"MomentumStrategy", "TrendFollowingStrategy"}:
        lookback = int(params.get("lookback_period", 14))
        threshold = float(params.get("momentum_threshold", 0.02))

        momentum = close / close.shift(lookback) - 1
        in_position = False
        values = []
        for m in momentum.fillna(0):
            if not in_position and m >= threshold:
                in_position = True
            elif in_position and m <= -threshold * 0.5:
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)
    elif strategy == "DonchianBreakoutStrategy":
        high = df["high"]
        low = df["low"]
        lookback = int(params.get("lookback", 20))
        exit_lookback = int(params.get("exit_lookback", 10))
        upper = high.rolling(lookback, min_periods=lookback).max().shift(1)
        exit_low = low.rolling(exit_lookback, min_periods=exit_lookback).min().shift(1)
        in_position = False
        values = []
        for c, up, ex in zip(close.ffill(), upper.fillna(float("inf")), exit_low.fillna(float("-inf"))):
            if not in_position and c > up:
                in_position = True
            elif in_position and c < ex:
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)
    elif strategy == "StochasticStrategy":
        high = df["high"]
        low = df["low"]
        k_period = int(params.get("k_period", 14))
        d_period = int(params.get("d_period", 3))
        smooth_k = int(params.get("smooth_k", 3))
        oversold = float(params.get("oversold", 20))
        overbought = float(params.get("overbought", 80))
        lowest = low.rolling(k_period, min_periods=k_period).min()
        highest = high.rolling(k_period, min_periods=k_period).max()
        raw_k = (close - lowest) / (highest - lowest).replace(0, np.nan) * 100
        k_line = raw_k.rolling(smooth_k, min_periods=smooth_k).mean()
        d_line = k_line.rolling(d_period, min_periods=d_period).mean()
        in_position = False
        values = []
        k_prev = np.nan
        d_prev = np.nan
        for k, d in zip(k_line.fillna(50), d_line.fillna(50)):
            cross_up = pd.notna(k_prev) and pd.notna(d_prev) and k_prev <= d_prev and k > d
            cross_down = pd.notna(k_prev) and pd.notna(d_prev) and k_prev >= d_prev and k < d
            if not in_position and cross_up and k <= oversold:
                in_position = True
            elif in_position and cross_down and k >= overbought:
                in_position = False
            values.append(1.0 if in_position else 0.0)
            k_prev, d_prev = k, d
        position = pd.Series(values, index=df.index)
    elif strategy == "ADXTrendStrategy":
        high = df["high"]
        low = df["low"]
        period = int(params.get("period", 14))
        adx_threshold = float(params.get("adx_threshold", 25))
        up_move = high.diff()
        down_move = -low.diff()
        plus_dm = pd.Series(np.where((up_move > down_move) & (up_move > 0), up_move, 0.0), index=df.index)
        minus_dm = pd.Series(np.where((down_move > up_move) & (down_move > 0), down_move, 0.0), index=df.index)
        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.ewm(alpha=1 / period, adjust=False).mean()
        plus_di = 100 * (plus_dm.ewm(alpha=1 / period, adjust=False).mean() / atr.replace(0, np.nan))
        minus_di = 100 * (minus_dm.ewm(alpha=1 / period, adjust=False).mean() / atr.replace(0, np.nan))
        dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)) * 100
        adx = dx.ewm(alpha=1 / period, adjust=False).mean()
        in_position = False
        values = []
        p_prev = np.nan
        m_prev = np.nan
        for p, m, a in zip(plus_di.fillna(0), minus_di.fillna(0), adx.fillna(0)):
            cross_up = pd.notna(p_prev) and pd.notna(m_prev) and p_prev <= m_prev and p > m
            cross_down = pd.notna(p_prev) and pd.notna(m_prev) and p_prev >= m_prev and p < m
            if not in_position and cross_up and a >= adx_threshold:
                in_position = True
            elif in_position and cross_down:
                in_position = False
            values.append(1.0 if in_position else 0.0)
            p_prev, m_prev = p, m
        position = pd.Series(values, index=df.index)
    elif strategy == "VWAPReversionStrategy":
        window = int(params.get("window", 48))
        entry_dev = float(params.get("entry_deviation_pct", 0.01))
        exit_dev = float(params.get("exit_deviation_pct", 0.002))
        typical = (df["high"] + df["low"] + df["close"]) / 3.0
        vol = df["volume"].replace(0, np.nan)
        vwap = (typical * vol).rolling(window, min_periods=window).sum() / vol.rolling(window, min_periods=window).sum()
        dev = (close - vwap) / vwap
        in_position = False
        values = []
        for d in dev.fillna(0):
            if not in_position and d <= -entry_dev:
                in_position = True
            elif in_position and d >= -exit_dev:
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)
    elif strategy == "MarketSentimentStrategy":
        lookback = int(params.get("lookback_period", 24))
        panic_th = float(params.get("panic_threshold", -0.04))
        euphoria_th = float(params.get("euphoria_threshold", 0.04))
        mood = close.pct_change().rolling(lookback, min_periods=lookback).sum()
        in_position = False
        values = []
        for m in mood.fillna(0):
            if not in_position and m <= panic_th:
                in_position = True
            elif in_position and m >= euphoria_th:
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)
    elif strategy == "SocialSentimentStrategy":
        lookback = max(2, int(params.get("lookback_period", 12)))
        enter_th = float(params.get("enter_momentum", 0.015))
        exit_th = float(params.get("exit_momentum", -0.008))
        momentum = close / close.shift(lookback) - 1.0
        in_position = False
        values = []
        for m in momentum.fillna(0):
            if not in_position and m >= enter_th:
                in_position = True
            elif in_position and m <= exit_th:
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)
    elif strategy == "FundFlowStrategy":
        vol = df["volume"].fillna(0.0)
        vol_ma = vol.rolling(24, min_periods=24).mean()
        price_ma = close.rolling(24, min_periods=24).mean()
        flow_signal = (vol > (vol_ma * 1.2)) & (close > price_ma)
        exit_signal = close < price_ma
        in_position = False
        values = []
        for en, ex in zip(flow_signal.fillna(False), exit_signal.fillna(False)):
            if not in_position and bool(en):
                in_position = True
            elif in_position and bool(ex):
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)
    elif strategy == "WhaleActivityStrategy":
        vol = df["volume"].fillna(0.0)
        vol_mean = vol.rolling(48, min_periods=24).mean()
        vol_std = vol.rolling(48, min_periods=24).std().replace(0, np.nan)
        vol_z = (vol - vol_mean) / vol_std
        bar_ret = close.pct_change().fillna(0.0)
        entry = (vol_z >= 1.8) & (bar_ret > 0)
        exit_sig = (bar_ret < 0) | (vol_z < 0.2)
        in_position = False
        values = []
        for en, ex in zip(entry.fillna(False), exit_sig.fillna(False)):
            if not in_position and bool(en):
                in_position = True
            elif in_position and bool(ex):
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)
    else:
        raise ValueError(f"Unsupported strategy for OHLCV backtest: {strategy}")

    return position.fillna(0.0)


def _extract_trade_points(close: pd.Series, position: pd.Series) -> Dict[str, List[Dict[str, Any]]]:
    entries = (position.diff().fillna(0) > 0)
    exits = (position.diff().fillna(0) < 0)

    buy_points = [
        {"timestamp": ts.isoformat(), "price": float(px)}
        for ts, px in close[entries].items()
    ]
    sell_points = [
        {"timestamp": ts.isoformat(), "price": float(px)}
        for ts, px in close[exits].items()
    ]

    return {
        "buy_points": buy_points,
        "sell_points": sell_points,
        "entries": len(buy_points),
        "exits": len(sell_points),
    }


def _trade_stats(close: pd.Series, position: pd.Series) -> Dict[str, Any]:
    entries = (position.diff().fillna(0) > 0).astype(int)
    exits = (position.diff().fillna(0) < 0).astype(int)

    entry_points = list(close[entries == 1].items())
    exit_points = list(close[exits == 1].items())

    trade_returns = []
    exit_idx = 0
    for entry_time, entry_price in entry_points:
        while exit_idx < len(exit_points) and exit_points[exit_idx][0] <= entry_time:
            exit_idx += 1
        if exit_idx >= len(exit_points):
            break
        _, exit_price = exit_points[exit_idx]
        if entry_price > 0:
            trade_returns.append((exit_price - entry_price) / entry_price)
        exit_idx += 1

    completed = len(trade_returns)
    wins = sum(1 for r in trade_returns if r > 0)
    win_rate = (wins / completed * 100) if completed else 0.0

    return {
        "entries": int(entries.sum()),
        "exits": int(exits.sum()),
        "completed": completed,
        "win_rate": round(win_rate, 2),
    }


def _run_backtest_core(
    strategy: str,
    df: pd.DataFrame,
    timeframe: str,
    initial_capital: float,
    params: Optional[Dict[str, Any]] = None,
    include_series: bool = False,
    commission_rate: float = 0.0004,
    slippage_bps: float = 2.0,
) -> Dict[str, Any]:
    if not is_strategy_backtest_supported(strategy):
        info = get_backtest_strategy_info(strategy)
        reason = info.get("reason", "该策略不适用于单一OHLCV序列回测")
        raise HTTPException(
            status_code=400,
            detail=f"{strategy} 暂不支持当前回测引擎: {reason}",
        )

    min_bars = _min_required_bars(timeframe)
    if len(df) < min_bars:
        raise HTTPException(
            status_code=400,
            detail=f"数据量不足，无法回测（{timeframe} 至少需要 {min_bars} 根K线，当前 {len(df)} 根）",
        )

    position = _build_positions(strategy, df, params=params)
    returns, anomaly_ratio, clip_limit = _safe_bar_returns(df["close"], timeframe)
    gross_returns = position.shift(1).fillna(0.0) * returns

    turnover = position.diff().abs().fillna(0.0)
    if len(position) > 0:
        turnover.iloc[0] = abs(float(position.iloc[0] or 0.0))

    fee_rate = max(0.0, float(commission_rate or 0.0))
    slip_rate = max(0.0, float(slippage_bps or 0.0)) / 10000.0
    total_cost_rate = fee_rate + slip_rate

    trade_cost = turnover * total_cost_rate
    strategy_returns = (gross_returns - trade_cost).clip(lower=-0.95, upper=clip_limit)
    gross_returns = gross_returns.clip(lower=-0.95, upper=clip_limit)

    equity = _safe_equity_curve(strategy_returns, initial_capital)
    gross_equity = _safe_equity_curve(gross_returns, initial_capital)
    final_capital = float(equity.iloc[-1])
    total_return = (final_capital / initial_capital - 1) * 100
    gross_final_capital = float(gross_equity.iloc[-1])
    gross_total_return = (gross_final_capital / initial_capital - 1) * 100

    peak = equity.cummax()
    drawdown = (equity - peak) / peak.replace(0, np.nan)
    max_drawdown = abs(float(drawdown.min() or 0.0)) * 100

    ann = _annual_factor(timeframe)
    std = float(strategy_returns.std() or 0.0)
    sharpe = float(strategy_returns.mean() / std * np.sqrt(ann)) if std > 0 else 0.0

    trade_stats = _trade_stats(df["close"], position)
    quality_flag = "ok"
    if anomaly_ratio > 0.02:
        quality_flag = "warning_high_outlier"
    elif anomaly_ratio > 0.005:
        quality_flag = "watch_outlier"
    if (not np.isfinite(final_capital)) or final_capital <= 0:
        quality_flag = "invalid"

    result = {
        "final_capital": round(final_capital, 2),
        "total_return": round(total_return, 2),
        "total_trades": trade_stats["completed"],
        "win_rate": trade_stats["win_rate"],
        "max_drawdown": round(max_drawdown, 2),
        "sharpe_ratio": round(sharpe, 2),
        "entry_signals": trade_stats["entries"],
        "exit_signals": trade_stats["exits"],
        "gross_final_capital": round(gross_final_capital, 2),
        "gross_total_return": round(gross_total_return, 2),
        "cost_drag_return_pct": round(gross_total_return - total_return, 4),
        "estimated_trade_cost_pct": round(float(trade_cost.sum() * 100), 4),
        "estimated_trade_cost_usd": round(float((trade_cost * equity.shift(1).fillna(initial_capital)).sum()), 2),
        "commission_rate": fee_rate,
        "slippage_bps": float(slippage_bps or 0.0),
        "anomaly_bar_ratio": round(float(anomaly_ratio), 6),
        "return_clip_limit": round(float(clip_limit), 6),
        "quality_flag": quality_flag,
    }

    if include_series:
        points = _extract_trade_points(df["close"], position)

        # Downsample for frontend payload size.
        max_points = 1800
        series_df = pd.DataFrame(
            {
                "timestamp": df.index,
                "equity": equity.values,
                "gross_equity": gross_equity.values,
                "drawdown": (drawdown.fillna(0) * 100).values,
                "position": position.values,
                "close": df["close"].values,
                "cost": trade_cost.values,
            }
        )
        if len(series_df) > max_points:
            step = int(np.ceil(len(series_df) / max_points))
            series_df = series_df.iloc[::step]

        result["series"] = [
            {
                "timestamp": pd.Timestamp(row["timestamp"]).isoformat(),
                "equity": round(float(row["equity"]), 4),
                "drawdown": round(float(row["drawdown"]), 4),
                "position": float(row["position"]),
                "close": round(float(row["close"]), 8),
                "gross_equity": round(float(row["gross_equity"]), 4),
                "cost": round(float(row["cost"]), 8),
            }
            for _, row in series_df.iterrows()
        ]
        result["trade_points"] = points

    return result


async def _load_backtest_df(
    symbol: str,
    timeframe: str,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> pd.DataFrame:
    tf = str(timeframe or "1h")
    exchanges = ["binance", "gate", "okx"]
    best = pd.DataFrame()
    target_min = _min_required_bars(tf)
    start_hint = pd.to_datetime(start_time) if start_time is not None else None
    end_hint = pd.to_datetime(end_time) if end_time is not None else pd.Timestamp.utcnow()

    # Prevent sub-minute backtests from scanning huge 1s archives by default.
    if tf in _SUB_MINUTE_TIMEFRAMES and start_hint is None:
        lookback_days = _default_subminute_lookback_days(tf)
        start_hint = end_hint - pd.Timedelta(days=lookback_days)

    start_dt = start_hint.to_pydatetime() if start_hint is not None else None
    end_dt = end_hint.to_pydatetime() if end_hint is not None else None

    # 1) Exact timeframe search across exchanges.
    for exchange in exchanges:
        df = await data_storage.load_klines_from_parquet(
            exchange=exchange,
            symbol=symbol,
            timeframe=tf,
            start_time=start_dt,
            end_time=end_dt,
        )
        if len(df) > len(best):
            best = df
    if len(best) >= target_min:
        best.index = pd.to_datetime(best.index)
        return best.sort_index()

    # 2) Aggregate from finer base timeframe when exact file is missing or insufficient.
    for base_tf in _candidate_base_timeframes(tf):
        for exchange in exchanges:
            base = await data_storage.load_klines_from_parquet(
                exchange=exchange,
                symbol=symbol,
                timeframe=base_tf,
                start_time=start_dt,
                end_time=end_dt,
            )
            if base.empty:
                continue
            agg = _resample_ohlcv(base, tf)
            if len(agg) > len(best):
                best = agg
        if len(best) >= target_min:
            break

    if not best.empty:
        best.index = pd.to_datetime(best.index)
        best = best[~best.index.duplicated(keep="last")].sort_index()
    return best


@router.post("/run")
async def run_backtest(
    strategy: str = "MAStrategy",
    symbol: str = "BTC/USDT",
    timeframe: str = "1d",
    initial_capital: float = 10000,
    commission_rate: float = 0.0004,
    slippage_bps: float = 2.0,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    include_series: bool = True,
):
    parsed_start = _parse_backtest_bound(start_date, bound="start_date")
    parsed_end = _parse_backtest_bound(end_date, bound="end_date")

    df = await _load_backtest_df(
        symbol,
        timeframe,
        start_time=parsed_start.to_pydatetime() if parsed_start is not None else None,
        end_time=parsed_end.to_pydatetime() if parsed_end is not None else None,
    )
    if df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"未找到 {symbol} {timeframe} 数据，请先下载历史数据。",
        )
    full_df = df.copy()
    min_bars = _min_required_bars(timeframe)
    auto_expanded_range = False

    if parsed_start is not None:
        df = df[df.index >= parsed_start]
    if parsed_end is not None:
        df = df[df.index <= parsed_end]

    if df.empty:
        raise HTTPException(status_code=404, detail="该时间范围内无可用数据。")

    # If date filter is too narrow, fallback to full history to avoid frequent "insufficient data".
    if len(df) < min_bars and len(full_df) >= min_bars:
        df = full_df
        auto_expanded_range = True

    result = _run_backtest_core(
        strategy=strategy,
        df=df,
        timeframe=timeframe,
        initial_capital=initial_capital,
        include_series=include_series,
        commission_rate=max(0.0, float(commission_rate or 0.0)),
        slippage_bps=max(0.0, float(slippage_bps or 0.0)),
    )

    result.update(
        {
            "strategy": strategy,
            "symbol": symbol,
            "timeframe": timeframe,
            "initial_capital": initial_capital,
            "commission_rate": max(0.0, float(commission_rate or 0.0)),
            "slippage_bps": max(0.0, float(slippage_bps or 0.0)),
            "data_points": int(len(df)),
            "start_date": df.index[0].isoformat(),
            "end_date": df.index[-1].isoformat(),
            "auto_expanded_range": auto_expanded_range,
            "min_required_bars": min_bars,
        }
    )
    return result


@router.post("/compare")
async def compare_backtests(
    strategies: str = (
        "MAStrategy,EMAStrategy,RSIStrategy,RSIDivergenceStrategy,MACDStrategy,MACDHistogramStrategy,"
        "BollingerBandsStrategy,BollingerSqueezeStrategy,MeanReversionStrategy,BollingerMeanReversionStrategy,"
        "MomentumStrategy,TrendFollowingStrategy,PairsTradingStrategy,DonchianBreakoutStrategy,StochasticStrategy,"
        "ADXTrendStrategy,VWAPReversionStrategy,MarketSentimentStrategy,SocialSentimentStrategy,FundFlowStrategy,"
        "WhaleActivityStrategy"
    ),
    symbol: str = "BTC/USDT",
    timeframe: str = "1h",
    initial_capital: float = 10000,
    commission_rate: float = 0.0004,
    slippage_bps: float = 2.0,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    pre_optimize: bool = True,
    optimize_objective: str = "total_return",
    optimize_max_trials: int = 16,
):
    strategy_list = [s.strip() for s in strategies.split(",") if s.strip()]
    if not strategy_list:
        raise HTTPException(status_code=400, detail="至少需要一个策略")

    parsed_start = _parse_backtest_bound(start_date, bound="start_date")
    parsed_end = _parse_backtest_bound(end_date, bound="end_date")

    df = await _load_backtest_df(
        symbol,
        timeframe,
        start_time=parsed_start.to_pydatetime() if parsed_start is not None else None,
        end_time=parsed_end.to_pydatetime() if parsed_end is not None else None,
    )
    if df.empty:
        raise HTTPException(status_code=404, detail="缺少历史数据")
    if parsed_start is not None:
        df = df[df.index >= parsed_start]
    if parsed_end is not None:
        df = df[df.index <= parsed_end]
    if df.empty:
        raise HTTPException(status_code=404, detail="该时间范围内无可用数据")
    min_bars = _min_required_bars(timeframe)
    if len(df) < min_bars:
        raise HTTPException(
            status_code=400,
            detail=f"该时间范围内K线不足（{len(df)} 根），{timeframe} 至少需要 {min_bars} 根",
        )

    results = []
    for strategy in strategy_list:
        try:
            if pre_optimize and strategy in _BACKTEST_OPTIMIZATION_GRIDS:
                opt = _optimize_strategy_on_df(
                    strategy=strategy,
                    df=df,
                    timeframe=timeframe,
                    initial_capital=initial_capital,
                    commission_rate=max(0.0, float(commission_rate or 0.0)),
                    slippage_bps=max(0.0, float(slippage_bps or 0.0)),
                    objective=optimize_objective,
                    max_trials=max(1, min(int(optimize_max_trials or 16), 256)),
                )
                if opt.get("best"):
                    metrics = dict(opt["best"]["metrics"])
                    metrics.update(
                        {
                            "strategy": strategy,
                            "optimization_applied": True,
                            "optimized_params": dict(opt["best"].get("params") or {}),
                            "optimization_score": float(opt["best"].get("score") or 0.0),
                            "optimization_trials": int(opt.get("trials") or 0),
                            "optimization_failed_trials": int(opt.get("failed_trials") or 0),
                            "optimization_objective": opt.get("objective"),
                        }
                    )
                else:
                    metrics = _run_backtest_core(
                        strategy=strategy,
                        df=df,
                        timeframe=timeframe,
                        initial_capital=initial_capital,
                        include_series=False,
                        commission_rate=max(0.0, float(commission_rate or 0.0)),
                        slippage_bps=max(0.0, float(slippage_bps or 0.0)),
                    )
                    metrics.update(
                        {
                            "strategy": strategy,
                            "optimization_applied": False,
                            "optimization_reason": "优化无有效结果，已回退默认参数",
                            "optimization_trials": int(opt.get("trials") or 0),
                            "optimization_failed_trials": int(opt.get("failed_trials") or 0),
                            "optimization_objective": opt.get("objective"),
                        }
                    )
            else:
                metrics = _run_backtest_core(
                    strategy=strategy,
                    df=df,
                    timeframe=timeframe,
                    initial_capital=initial_capital,
                    include_series=False,
                    commission_rate=max(0.0, float(commission_rate or 0.0)),
                    slippage_bps=max(0.0, float(slippage_bps or 0.0)),
                )
                metrics.update(
                    {
                        "strategy": strategy,
                        "optimization_applied": False,
                        "optimization_reason": (
                            "未启用预优化" if not pre_optimize else "该策略暂不支持参数优化"
                        ),
                    }
                )
            results.append(metrics)
        except Exception as e:
            results.append({"strategy": strategy, "error": str(e)})

    ranked = sorted(
        [r for r in results if "error" not in r],
        key=lambda x: (x.get("total_return", -999999), x.get("sharpe_ratio", -999999)),
        reverse=True,
    )

    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "initial_capital": initial_capital,
        "commission_rate": max(0.0, float(commission_rate or 0.0)),
        "slippage_bps": max(0.0, float(slippage_bps or 0.0)),
        "requested_start_date": start_date,
        "requested_end_date": end_date,
        "data_points": int(len(df)),
        "start_date": df.index[0].isoformat(),
        "end_date": df.index[-1].isoformat(),
        "pre_optimize": bool(pre_optimize),
        "optimize_objective": _normalize_optimize_objective(optimize_objective),
        "optimize_max_trials": max(1, min(int(optimize_max_trials or 16), 256)),
        "results": results,
        "best": ranked[0] if ranked else None,
    }


@router.post("/run_custom")
async def run_backtest_custom(
    strategy: str = "MAStrategy",
    symbol: str = "BTC/USDT",
    timeframe: str = "1h",
    initial_capital: float = 10000,
    commission_rate: float = 0.0004,
    slippage_bps: float = 2.0,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    include_series: bool = True,
    params_json: Optional[str] = None,
):
    custom_params: Optional[Dict[str, Any]] = None
    if params_json:
        try:
            parsed = json.loads(params_json)
        except Exception:
            raise HTTPException(status_code=400, detail="params_json 不是合法JSON")
        if not isinstance(parsed, dict):
            raise HTTPException(status_code=400, detail="params_json 必须是对象JSON")
        custom_params = parsed

    parsed_start = _parse_backtest_bound(start_date, bound="start_date")
    parsed_end = _parse_backtest_bound(end_date, bound="end_date")

    df = await _load_backtest_df(
        symbol,
        timeframe,
        start_time=parsed_start.to_pydatetime() if parsed_start is not None else None,
        end_time=parsed_end.to_pydatetime() if parsed_end is not None else None,
    )
    if df.empty:
        raise HTTPException(status_code=404, detail="缺少历史数据")
    if parsed_start is not None:
        df = df[df.index >= parsed_start]
    if parsed_end is not None:
        df = df[df.index <= parsed_end]
    if df.empty:
        raise HTTPException(status_code=404, detail="该时间范围内无可用数据。")
    if len(df) < _min_required_bars(timeframe):
        raise HTTPException(status_code=400, detail="该时间范围K线不足，无法回测。")

    result = _run_backtest_core(
        strategy=strategy,
        df=df,
        timeframe=timeframe,
        initial_capital=initial_capital,
        params=custom_params,
        include_series=include_series,
        commission_rate=max(0.0, float(commission_rate or 0.0)),
        slippage_bps=max(0.0, float(slippage_bps or 0.0)),
    )
    result.update(
        {
            "strategy": strategy,
            "symbol": symbol,
            "timeframe": timeframe,
            "initial_capital": initial_capital,
            "commission_rate": max(0.0, float(commission_rate or 0.0)),
            "slippage_bps": max(0.0, float(slippage_bps or 0.0)),
            "data_points": int(len(df)),
            "start_date": df.index[0].isoformat(),
            "end_date": df.index[-1].isoformat(),
            "custom_params": custom_params or {},
        }
    )
    return result


@router.post("/optimize")
async def optimize_backtest(
    strategy: str = "MAStrategy",
    symbol: str = "BTC/USDT",
    timeframe: str = "1h",
    initial_capital: float = 10000,
    commission_rate: float = 0.0004,
    slippage_bps: float = 2.0,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    objective: str = "total_return",
    max_trials: int = 64,
    include_all_trials: bool = True,
):
    parsed_start = _parse_backtest_bound(start_date, bound="start_date")
    parsed_end = _parse_backtest_bound(end_date, bound="end_date")

    df = await _load_backtest_df(
        symbol,
        timeframe,
        start_time=parsed_start.to_pydatetime() if parsed_start is not None else None,
        end_time=parsed_end.to_pydatetime() if parsed_end is not None else None,
    )
    if df.empty:
        raise HTTPException(status_code=404, detail="缺少历史数据")
    if parsed_start is not None:
        df = df[df.index >= parsed_start]
    if parsed_end is not None:
        df = df[df.index <= parsed_end]
    if df.empty:
        raise HTTPException(status_code=404, detail="该时间范围内无可用数据")
    min_bars = _min_required_bars(timeframe)
    if len(df) < min_bars:
        raise HTTPException(
            status_code=400,
            detail=f"该时间范围内K线不足（{len(df)} 根），{timeframe} 至少需要 {min_bars} 根",
        )
    try:
        opt_result = _optimize_strategy_on_df(
            strategy=strategy,
            df=df,
            timeframe=timeframe,
            initial_capital=initial_capital,
            commission_rate=max(0.0, float(commission_rate or 0.0)),
            slippage_bps=max(0.0, float(slippage_bps or 0.0)),
            objective=objective,
            max_trials=max_trials,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return {
        "strategy": strategy,
        "symbol": symbol,
        "timeframe": timeframe,
        "requested_start_date": start_date,
        "requested_end_date": end_date,
        "data_points": int(len(df)),
        "start_date": df.index[0].isoformat(),
        "end_date": df.index[-1].isoformat(),
        "objective": opt_result.get("objective"),
        "commission_rate": max(0.0, float(commission_rate or 0.0)),
        "slippage_bps": max(0.0, float(slippage_bps or 0.0)),
        "trials": int(opt_result.get("trials") or 0),
        "failed_trials": int(opt_result.get("failed_trials") or 0),
        "best": opt_result.get("best"),
        "top": opt_result.get("top") or [],
        "all_trials": (opt_result.get("all_trials") or []) if include_all_trials else [],
        "failures": opt_result.get("failures") or [],
    }


@router.get("/export")
async def export_backtest_report(
    strategy: str = "MAStrategy",
    symbol: str = "BTC/USDT",
    timeframe: str = "1h",
    initial_capital: float = 10000,
    commission_rate: float = 0.0004,
    slippage_bps: float = 2.0,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    format: str = "xlsx",
):
    parsed_start = _parse_backtest_bound(start_date, bound="start_date")
    parsed_end = _parse_backtest_bound(end_date, bound="end_date")
    df = await _load_backtest_df(
        symbol,
        timeframe,
        start_time=parsed_start.to_pydatetime() if parsed_start is not None else None,
        end_time=parsed_end.to_pydatetime() if parsed_end is not None else None,
    )
    if df.empty:
        raise HTTPException(status_code=404, detail="缺少历史数据")
    if parsed_start is not None:
        df = df[df.index >= parsed_start]
    if parsed_end is not None:
        df = df[df.index <= parsed_end]
    if df.empty:
        raise HTTPException(status_code=404, detail="该时间范围内无可用数据")

    result = _run_backtest_core(
        strategy=strategy,
        df=df,
        timeframe=timeframe,
        initial_capital=initial_capital,
        include_series=True,
        commission_rate=max(0.0, float(commission_rate or 0.0)),
        slippage_bps=max(0.0, float(slippage_bps or 0.0)),
    )

    summary_df = pd.DataFrame(
        [
            {
                "strategy": strategy,
                "symbol": symbol,
                "timeframe": timeframe,
                "initial_capital": initial_capital,
                "commission_rate": max(0.0, float(commission_rate or 0.0)),
                "slippage_bps": max(0.0, float(slippage_bps or 0.0)),
                "requested_start_date": start_date,
                "requested_end_date": end_date,
                "start_date": df.index[0].isoformat() if len(df) else None,
                "end_date": df.index[-1].isoformat() if len(df) else None,
                "final_capital": result.get("final_capital"),
                "total_return": result.get("total_return"),
                "gross_total_return": result.get("gross_total_return"),
                "cost_drag_return_pct": result.get("cost_drag_return_pct"),
                "estimated_trade_cost_pct": result.get("estimated_trade_cost_pct"),
                "total_trades": result.get("total_trades"),
                "win_rate": result.get("win_rate"),
                "max_drawdown": result.get("max_drawdown"),
                "sharpe_ratio": result.get("sharpe_ratio"),
                "anomaly_bar_ratio": result.get("anomaly_bar_ratio"),
                "return_clip_limit": result.get("return_clip_limit"),
                "quality_flag": result.get("quality_flag"),
            }
        ]
    )

    series_df = pd.DataFrame(result.get("series", []))
    format_lower = (format or "xlsx").lower()

    if format_lower == "csv":
        content = summary_df.to_csv(index=False).encode("utf-8-sig")
        filename = f"backtest_{strategy}_{symbol.replace('/', '_')}_{timeframe}.csv"
        media_type = "text/csv"
    elif format_lower == "pdf":
        output = io.BytesIO()
        with PdfPages(output) as pdf:
            fig = plt.figure(figsize=(11.69, 8.27))
            fig.patch.set_facecolor("#ffffff")
            txt = (
                f"Backtest Report\\n\\n"
                f"Strategy: {strategy}\\n"
                f"Symbol: {symbol}\\n"
                f"Timeframe: {timeframe}\\n"
                f"Initial Capital: {initial_capital:.2f}\\n"
                f"Final Capital: {float(result.get('final_capital', 0.0)):.2f}\\n"
                f"Total Return: {float(result.get('total_return', 0.0)):.2f}%\\n"
                f"Gross Return: {float(result.get('gross_total_return', 0.0)):.2f}%\\n"
                f"Cost Drag: {float(result.get('cost_drag_return_pct', 0.0)):.2f}%\\n"
                f"Max Drawdown: {float(result.get('max_drawdown', 0.0)):.2f}%\\n"
                f"Sharpe: {float(result.get('sharpe_ratio', 0.0)):.2f}\\n"
                f"Quality: {result.get('quality_flag', 'ok')}\\n"
                f"Anomaly Ratio: {float(result.get('anomaly_bar_ratio', 0.0)):.4f}\\n"
                f"Trades: {int(result.get('total_trades', 0) or 0)}\\n"
                f"Win Rate: {float(result.get('win_rate', 0.0)):.2f}%\\n"
                f"Commission(one-way): {max(0.0, float(commission_rate or 0.0)) * 100:.4f}%\\n"
                f"Slippage(one-way): {max(0.0, float(slippage_bps or 0.0)):.2f} bps\\n"
            )
            fig.text(0.08, 0.92, txt, va="top", fontsize=12, family="monospace")
            plt.axis("off")
            pdf.savefig(fig, bbox_inches="tight")
            plt.close(fig)

            if not series_df.empty:
                fig, axes = plt.subplots(2, 1, figsize=(11.69, 8.27), sharex=True)
                x = pd.to_datetime(series_df["timestamp"])
                axes[0].plot(x, series_df["equity"], color="#1f77b4", linewidth=1.6, label="Net Equity")
                if "gross_equity" in series_df.columns:
                    axes[0].plot(x, series_df["gross_equity"], color="#2ca02c", linewidth=1.0, label="Gross Equity")
                axes[0].set_title("Equity Curve")
                axes[0].grid(True, alpha=0.25)
                axes[0].legend(loc="best")

                if "drawdown" in series_df.columns:
                    axes[1].plot(x, series_df["drawdown"], color="#d62728", linewidth=1.2, label="Drawdown %")
                axes[1].set_title("Drawdown")
                axes[1].grid(True, alpha=0.25)
                axes[1].legend(loc="best")
                plt.tight_layout()
                pdf.savefig(fig)
                plt.close(fig)

        content = output.getvalue()
        filename = f"backtest_{strategy}_{symbol.replace('/', '_')}_{timeframe}.pdf"
        media_type = "application/pdf"
    else:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            summary_df.to_excel(writer, index=False, sheet_name="summary")
            if not series_df.empty:
                series_df.to_excel(writer, index=False, sheet_name="series")
        content = output.getvalue()
        filename = f"backtest_{strategy}_{symbol.replace('/', '_')}_{timeframe}.xlsx"
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

    return StreamingResponse(
        io.BytesIO(content),
        media_type=media_type,
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@router.get("/strategies")
async def get_available_strategies():
    strategies = get_backtest_strategy_catalog()
    return {
        "strategies": strategies,
        "supported_count": sum(1 for x in strategies if bool(x.get("backtest_supported"))),
        "unsupported_count": sum(1 for x in strategies if not bool(x.get("backtest_supported"))),
    }
