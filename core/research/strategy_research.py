"""Run strategy research on second-level market data."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from loguru import logger

from config.settings import settings
from core.data import data_storage

SUPPORTED_RESEARCH_TIMEFRAMES = [
    "1s",
    "5s",
    "10s",
    "30s",
    "1m",
    "5m",
    "15m",
    "30m",
    "1h",
]

DEFAULT_STRATEGIES = [
    "MAStrategy",
    "EMAStrategy",
    "RSIStrategy",
    "RSIDivergenceStrategy",
    "MACDStrategy",
    "MACDHistogramStrategy",
    "BollingerBandsStrategy",
    "BollingerSqueezeStrategy",
    "MeanReversionStrategy",
    "BollingerMeanReversionStrategy",
    "MomentumStrategy",
    "TrendFollowingStrategy",
    "PairsTradingStrategy",
    "DonchianBreakoutStrategy",
    "StochasticStrategy",
    "ADXTrendStrategy",
    "VWAPReversionStrategy",
    "MarketSentimentStrategy",
    "SocialSentimentStrategy",
    "FundFlowStrategy",
    "WhaleActivityStrategy",
]

_RESAMPLE_RULES = {
    "1s": "1s",
    "5s": "5s",
    "10s": "10s",
    "30s": "30s",
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
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
    tf = str(timeframe or "").strip()
    return float(_RETURN_CLIP_BY_TIMEFRAME.get(tf, 2.0))


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
    else:
        raise ValueError(f"Unsupported strategy for research backtest: {strategy}")

    return position.fillna(0.0)


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
        "completed": int(completed),
        "win_rate": float(round(win_rate, 2)),
    }


def _run_backtest_core(
    strategy: str,
    df: pd.DataFrame,
    timeframe: str,
    initial_capital: float,
    params: Optional[Dict[str, Any]] = None,
    commission_rate: float = 0.0004,
    slippage_bps: float = 2.0,
) -> Dict[str, Any]:
    if len(df) < 50:
        raise ValueError("数据不足，至少需要 50 根 K 线")

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

    return {
        "final_capital": round(final_capital, 2),
        "total_return": round(total_return, 2),
        "gross_final_capital": round(gross_final_capital, 2),
        "gross_total_return": round(gross_total_return, 2),
        "cost_drag_return_pct": round(gross_total_return - total_return, 4),
        "estimated_trade_cost_pct": round(float(trade_cost.sum() * 100), 4),
        "estimated_trade_cost_usd": round(float((trade_cost * equity.shift(1).fillna(initial_capital)).sum()), 2),
        "commission_rate": fee_rate,
        "slippage_bps": float(slippage_bps or 0.0),
        "total_trades": trade_stats["completed"],
        "win_rate": trade_stats["win_rate"],
        "max_drawdown": round(max_drawdown, 2),
        "sharpe_ratio": round(sharpe, 2),
        "anomaly_bar_ratio": round(float(anomaly_ratio), 6),
        "return_clip_limit": round(float(clip_limit), 6),
        "quality_flag": quality_flag,
    }


@dataclass
class ResearchConfig:
    exchange: str = "binance"
    symbol: str = "BTC/USDT"
    days: int = 365
    initial_capital: float = 10000.0
    timeframes: List[str] = field(default_factory=lambda: list(SUPPORTED_RESEARCH_TIMEFRAMES))
    strategies: List[str] = field(default_factory=lambda: list(DEFAULT_STRATEGIES))
    min_rows_per_timeframe: int = 300
    commission_rate: float = 0.0004
    slippage_bps: float = 2.0
    output_dir: Path = field(default_factory=lambda: Path(settings.DATA_STORAGE_PATH) / ".." / "research")


def _normalize_symbol(symbol: str) -> str:
    raw = (symbol or "").strip().upper()
    if "/" in raw:
        return raw
    if "_" in raw:
        left, right = raw.split("_", 1)
        return f"{left}/{right}"
    if raw.endswith("USDT") and len(raw) > 4:
        return f"{raw[:-4]}/USDT"
    return raw


def _validate_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()
    out.index = pd.to_datetime(out.index)
    out = out.sort_index()
    out = out[~out.index.duplicated(keep="last")]

    missing_cols = [c for c in ["open", "high", "low", "close", "volume"] if c not in out.columns]
    if missing_cols:
        raise ValueError(f"数据缺失字段: {','.join(missing_cols)}")

    return out[["open", "high", "low", "close", "volume"]]


def _resample_ohlcv(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    if df.empty:
        return df

    rule = _RESAMPLE_RULES.get(timeframe)
    if not rule:
        return pd.DataFrame()

    if timeframe == "1s":
        return df.copy()

    src = df.copy()
    src.index = pd.to_datetime(src.index)
    src = src.sort_index()
    ohlc = src[["open", "high", "low", "close"]].resample(rule).agg(
        {"open": "first", "high": "max", "low": "min", "close": "last"}
    )
    volume = src[["volume"]].resample(rule).sum()
    merged = pd.concat([ohlc, volume], axis=1)
    merged = merged.dropna(subset=["open", "high", "low", "close"])
    return merged


async def _load_second_level_df(
    exchange: str,
    symbol: str,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> pd.DataFrame:
    df = await data_storage.load_klines_from_parquet(
        exchange=exchange,
        symbol=symbol,
        timeframe="1s",
        start_time=start_time,
        end_time=end_time,
    )
    if df.empty:
        return df
    return _validate_df(df)


def _build_markdown_report(
    config: ResearchConfig,
    full_df: pd.DataFrame,
    result_df: pd.DataFrame,
    saved_csv_path: Path,
) -> str:
    lines: List[str] = []
    lines.append("# 策略研究报告")
    lines.append("")
    lines.append(f"- 交易所: `{config.exchange}`")
    lines.append(f"- 交易对: `{config.symbol}`")
    lines.append(f"- 数据范围: `{full_df.index.min().isoformat()}` ~ `{full_df.index.max().isoformat()}`")
    lines.append(f"- 1秒K线条数: `{len(full_df)}`")
    lines.append(f"- 初始资金: `{config.initial_capital}`")
    lines.append(f"- 手续费(单边): `{config.commission_rate * 100:.4f}%`")
    lines.append(f"- 滑点(单边): `{config.slippage_bps:.2f} bps`")
    lines.append("- Bar收益裁剪: `已启用（抑制异常脏数据导致的爆炸收益）`")
    lines.append(f"- 结果CSV: `{saved_csv_path}`")
    lines.append("")

    valid_df = result_df[(result_df["error"] == "") & (result_df["quality_flag"] != "invalid")].copy()
    if valid_df.empty:
        lines.append("无可用结果。")
        return "\n".join(lines)

    quality = valid_df["quality_flag"].value_counts().to_dict()
    lines.append("## 质量分布")
    lines.append("")
    lines.append(f"- `ok`: {int(quality.get('ok', 0))}")
    lines.append(f"- `watch_outlier`: {int(quality.get('watch_outlier', 0))}")
    lines.append(f"- `warning_high_outlier`: {int(quality.get('warning_high_outlier', 0))}")
    lines.append("")

    top = valid_df.sort_values(["score", "total_return"], ascending=[False, False]).head(20)
    lines.append("## Top 20 结果")
    lines.append("")
    lines.append("| rank | strategy | timeframe | score | net_return(%) | gross_return(%) | cost_drag(%) | sharpe | max_drawdown(%) | win_rate(%) | trades | anomaly_ratio |")
    lines.append("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for i, (_, row) in enumerate(top.iterrows(), start=1):
        lines.append(
            f"| {i} | {row['strategy']} | {row['timeframe']} | {float(row.get('score', 0.0)):.2f} | "
            f"{row['total_return']:.2f} | {row.get('gross_total_return', 0.0):.2f} | "
            f"{row.get('cost_drag_return_pct', 0.0):.2f} | {row['sharpe_ratio']:.2f} | {row['max_drawdown']:.2f} | "
            f"{row['win_rate']:.2f} | {int(row['total_trades'])} | {float(row.get('anomaly_bar_ratio', 0.0)):.4f} |"
        )
    lines.append("")

    best_tf = (
        valid_df.groupby("timeframe")
        .agg(
            count=("strategy", "count"),
            avg_score=("score", "mean"),
            avg_return=("total_return", "mean"),
            avg_sharpe=("sharpe_ratio", "mean"),
        )
        .sort_values(["avg_score", "avg_return"], ascending=False)
        .head(8)
    )
    lines.append("## 周期聚合")
    lines.append("")
    lines.append("| timeframe | runs | avg_score | avg_return(%) | avg_sharpe |")
    lines.append("|---|---:|---:|---:|---:|")
    for tf, row in best_tf.iterrows():
        lines.append(
            f"| {tf} | {int(row['count'])} | {float(row['avg_score']):.2f} | "
            f"{float(row['avg_return']):.2f} | {float(row['avg_sharpe']):.2f} |"
        )
    lines.append("")

    best_st = (
        valid_df.groupby("strategy")
        .agg(
            count=("timeframe", "count"),
            avg_score=("score", "mean"),
            avg_return=("total_return", "mean"),
            avg_sharpe=("sharpe_ratio", "mean"),
        )
        .sort_values(["avg_score", "avg_return"], ascending=False)
        .head(10)
    )
    lines.append("## 策略聚合")
    lines.append("")
    lines.append("| strategy | runs | avg_score | avg_return(%) | avg_sharpe |")
    lines.append("|---|---:|---:|---:|---:|")
    for strategy, row in best_st.iterrows():
        lines.append(
            f"| {strategy} | {int(row['count'])} | {float(row['avg_score']):.2f} | "
            f"{float(row['avg_return']):.2f} | {float(row['avg_sharpe']):.2f} |"
        )

    return "\n".join(lines)


async def run_strategy_research(config: ResearchConfig) -> Dict[str, Any]:
    config.symbol = _normalize_symbol(config.symbol)
    config.exchange = (config.exchange or "binance").lower().strip()
    config.days = max(1, int(config.days))
    config.min_rows_per_timeframe = max(80, int(config.min_rows_per_timeframe))
    config.timeframes = [tf for tf in config.timeframes if tf in _RESAMPLE_RULES]
    if not config.timeframes:
        raise ValueError("timeframes 为空或不支持")

    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=config.days)

    base_df = await _load_second_level_df(
        exchange=config.exchange,
        symbol=config.symbol,
        start_time=start_time,
        end_time=end_time,
    )
    if base_df.empty:
        raise ValueError(
            f"未找到 {config.exchange} {config.symbol} 1s 数据，请先执行秒级回填。"
        )

    frames: Dict[str, pd.DataFrame] = {}
    for timeframe in config.timeframes:
        tf_df = _resample_ohlcv(base_df, timeframe)
        tf_df = _validate_df(tf_df)
        if len(tf_df) >= config.min_rows_per_timeframe:
            frames[timeframe] = tf_df
        else:
            logger.warning(
                f"Skip timeframe={timeframe} rows={len(tf_df)} < min_rows={config.min_rows_per_timeframe}"
            )

    if not frames:
        raise ValueError("没有足够数据可用于研究，请缩短周期或降低最小样本要求。")

    rows: List[Dict[str, Any]] = []
    for timeframe, tf_df in frames.items():
        for strategy in config.strategies:
            payload: Dict[str, Any] = {
                "exchange": config.exchange,
                "symbol": config.symbol,
                "timeframe": timeframe,
                "strategy": strategy,
                "rows": int(len(tf_df)),
                "error": "",
                "final_capital": 0.0,
                "total_return": 0.0,
                "gross_final_capital": 0.0,
                "gross_total_return": 0.0,
                "cost_drag_return_pct": 0.0,
                "estimated_trade_cost_pct": 0.0,
                "estimated_trade_cost_usd": 0.0,
                "commission_rate": float(config.commission_rate),
                "slippage_bps": float(config.slippage_bps),
                "total_trades": 0,
                "win_rate": 0.0,
                "max_drawdown": 0.0,
                "sharpe_ratio": 0.0,
                "anomaly_bar_ratio": 0.0,
                "return_clip_limit": 0.0,
                "quality_flag": "invalid",
                "score": -999999.0,
            }
            try:
                metrics = _run_backtest_core(
                    strategy=strategy,
                    df=tf_df,
                    timeframe=timeframe,
                    initial_capital=config.initial_capital,
                    params=None,
                    commission_rate=float(config.commission_rate),
                    slippage_bps=float(config.slippage_bps),
                )
                payload.update(
                    {
                        "final_capital": float(metrics.get("final_capital", 0.0)),
                        "total_return": float(metrics.get("total_return", 0.0)),
                        "gross_final_capital": float(metrics.get("gross_final_capital", 0.0)),
                        "gross_total_return": float(metrics.get("gross_total_return", 0.0)),
                        "cost_drag_return_pct": float(metrics.get("cost_drag_return_pct", 0.0)),
                        "estimated_trade_cost_pct": float(metrics.get("estimated_trade_cost_pct", 0.0)),
                        "estimated_trade_cost_usd": float(metrics.get("estimated_trade_cost_usd", 0.0)),
                        "commission_rate": float(metrics.get("commission_rate", config.commission_rate)),
                        "slippage_bps": float(metrics.get("slippage_bps", config.slippage_bps)),
                        "total_trades": int(metrics.get("total_trades", 0) or 0),
                        "win_rate": float(metrics.get("win_rate", 0.0)),
                        "max_drawdown": float(metrics.get("max_drawdown", 0.0)),
                        "sharpe_ratio": float(metrics.get("sharpe_ratio", 0.0)),
                        "anomaly_bar_ratio": float(metrics.get("anomaly_bar_ratio", 0.0)),
                        "return_clip_limit": float(metrics.get("return_clip_limit", 0.0)),
                        "quality_flag": str(metrics.get("quality_flag", "ok")),
                    }
                )
                payload["score"] = (
                    float(payload["total_return"])
                    + float(payload["sharpe_ratio"]) * 5.0
                    - float(payload["max_drawdown"]) * 0.6
                    - float(payload["anomaly_bar_ratio"]) * 300.0
                )
            except Exception as e:
                payload["error"] = str(e)
            rows.append(payload)

    result_df = pd.DataFrame(rows)
    valid_df = result_df[(result_df["error"] == "") & (result_df["quality_flag"] != "invalid")].copy()
    if not valid_df.empty:
        valid_df = valid_df.sort_values(
            by=["score", "total_return", "sharpe_ratio", "win_rate"],
            ascending=[False, False, False, False],
        )
        ranking_order = valid_df.index.tolist()
        result_df["rank"] = result_df.index.map(lambda idx: ranking_order.index(idx) + 1 if idx in ranking_order else 0)
    else:
        result_df["rank"] = 0

    run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_dir = config.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    symbol_safe = config.symbol.replace("/", "_")
    csv_path = output_dir / f"research_{config.exchange}_{symbol_safe}_{run_ts}.csv"
    md_path = output_dir / f"research_{config.exchange}_{symbol_safe}_{run_ts}.md"

    result_df.sort_values(by=["rank", "timeframe", "strategy"], ascending=[True, True, True]).to_csv(
        csv_path,
        index=False,
        encoding="utf-8-sig",
    )

    md_text = _build_markdown_report(
        config=config,
        full_df=base_df,
        result_df=result_df.sort_values(by=["rank", "total_return"], ascending=[True, False]),
        saved_csv_path=csv_path,
    )
    md_path.write_text(md_text, encoding="utf-8")

    best = None
    if not valid_df.empty:
        item = valid_df.iloc[0].to_dict()
        best = {
            "strategy": item.get("strategy"),
            "timeframe": item.get("timeframe"),
            "total_return": float(item.get("total_return", 0.0)),
            "gross_total_return": float(item.get("gross_total_return", 0.0)),
            "cost_drag_return_pct": float(item.get("cost_drag_return_pct", 0.0)),
            "sharpe_ratio": float(item.get("sharpe_ratio", 0.0)),
            "max_drawdown": float(item.get("max_drawdown", 0.0)),
            "win_rate": float(item.get("win_rate", 0.0)),
            "total_trades": int(item.get("total_trades", 0) or 0),
            "anomaly_bar_ratio": float(item.get("anomaly_bar_ratio", 0.0)),
            "quality_flag": str(item.get("quality_flag", "ok")),
            "score": float(item.get("score", 0.0)),
        }

    return {
        "exchange": config.exchange,
        "symbol": config.symbol,
        "data_start": base_df.index.min().isoformat(),
        "data_end": base_df.index.max().isoformat(),
        "base_rows": int(len(base_df)),
        "timeframes": list(frames.keys()),
        "strategies": list(config.strategies),
        "commission_rate": float(config.commission_rate),
        "slippage_bps": float(config.slippage_bps),
        "runs": int(len(result_df)),
        "valid_runs": int(len(valid_df)),
        "quality_counts": (
            result_df["quality_flag"].value_counts().to_dict() if "quality_flag" in result_df.columns else {}
        ),
        "best": best,
        "csv_path": str(csv_path),
        "markdown_path": str(md_path),
    }
