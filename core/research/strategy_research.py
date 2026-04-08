"""Run strategy research on second-level market data."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import numpy as np
import pandas as pd
from loguru import logger

from config.settings import settings
from core.ai.proposal_schemas import StrategyProgram
from core.backtest.funding_provider import FundingRateProvider
from core.data import data_storage
from core.news.storage import db as news_db
from core.research.strategy_program import build_program_positions

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

RESEARCH_SUPPORTED_STRATEGIES = [
    # ── 趋势 ──
    "MAStrategy",
    "EMAStrategy",
    "MACDStrategy",
    "MACDHistogramStrategy",
    "ADXTrendStrategy",
    "TrendFollowingStrategy",
    "AroonStrategy",
    # ── 震荡 ──
    "RSIStrategy",
    "RSIDivergenceStrategy",
    "StochasticStrategy",
    "BollingerBandsStrategy",
    "WilliamsRStrategy",
    "CCIStrategy",
    "StochRSIStrategy",
    # ── 动量 ──
    "MomentumStrategy",
    "ROCStrategy",
    "PriceAccelerationStrategy",
    # ── 均值回归 ──
    "MeanReversionStrategy",
    "BollingerMeanReversionStrategy",
    "VWAPReversionStrategy",
    "VWAPStrategy",
    "MeanReversionHalfLifeStrategy",
    # ── 突破 ──
    "BollingerSqueezeStrategy",
    "DonchianBreakoutStrategy",
    # ── 成交量 ──
    "MFIStrategy",
    "OBVStrategy",
    "TradeIntensityStrategy",
    # ── 波动率 / 风险 ──
    "ParkinsonVolStrategy",
    "UlcerIndexStrategy",
    "VaRBreakoutStrategy",
    "MaxDrawdownStrategy",
    "SortinoRatioStrategy",
    # ── 统计套利 ──
    "PairsTradingStrategy",
    "HurstExponentStrategy",
    # ── 量化 / 微观结构 ──
    "OrderFlowImbalanceStrategy",
    "MultiFactorHFStrategy",
    "MarketSentimentStrategy",
    "SocialSentimentStrategy",
    "FundFlowStrategy",
    "WhaleActivityStrategy",
    # ── ML ──
    "MLXGBoostStrategy",
]

DEFAULT_STRATEGIES = list(RESEARCH_SUPPORTED_STRATEGIES)

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


def _timeframe_seconds(timeframe: str) -> int:
    tf = str(timeframe or "").strip()
    if not tf:
        return 0
    unit = tf[-1]
    try:
        value = int(tf[:-1] or 1)
    except Exception:
        return 0
    if value <= 0:
        return 0
    if unit == "s":
        return value
    if unit == "m":
        return value * 60
    if unit == "h":
        return value * 3600
    if unit == "d":
        return value * 86400
    return 0


def _requires_second_level_data(timeframes: List[str]) -> bool:
    return any(0 < _timeframe_seconds(tf) < 60 for tf in list(timeframes or []))


def get_supported_research_strategies() -> List[str]:
    return list(RESEARCH_SUPPORTED_STRATEGIES)


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


def _rolling_minmax_scale(series: pd.Series, window: int, lower: float = 0.0, upper: float = 1.0) -> pd.Series:
    min_periods = max(3, window // 3)
    lo = series.rolling(window, min_periods=min_periods).min()
    hi = series.rolling(window, min_periods=min_periods).max()
    scaled = (series - lo) / (hi - lo).replace(0, np.nan)
    scaled = scaled.clip(0.0, 1.0).fillna(0.5)
    return scaled * (upper - lower) + lower


def _as_naive_utc_index(index_like: Any) -> pd.DatetimeIndex:
    idx = pd.to_datetime(index_like)
    if isinstance(idx, pd.DatetimeIndex) and idx.tz is not None:
        idx = idx.tz_convert("UTC").tz_localize(None)
    return pd.DatetimeIndex(idx)


def _to_naive_utc_timestamp(value: Any) -> Optional[pd.Timestamp]:
    if value is None:
        return None
    try:
        ts = pd.Timestamp(value)
    except Exception:
        return None
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts


def _clamp_series(series: pd.Series, lower: float = -1.0, upper: float = 1.0) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(lower, upper)


def _data_column_or_default(df: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if column in df.columns:
        return pd.to_numeric(df[column], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(default)
    return pd.Series(float(default), index=df.index, dtype=float)


def _news_symbol_variants(symbol: str) -> List[str]:
    raw = str(symbol or "").strip().upper()
    if not raw:
        return []
    compact = raw.replace("/", "").replace("_", "")
    slash = compact[:-4] + "/USDT" if compact.endswith("USDT") and len(compact) > 4 else raw
    return list(dict.fromkeys([raw, compact, slash]))


async def _load_news_events_for_symbol(
    symbol: str,
    start_time: Optional[datetime],
    end_time: Optional[datetime],
    limit: int = 5000,
) -> List[Dict[str, Any]]:
    variants = _news_symbol_variants(symbol)
    if not variants:
        return []

    rows: List[Dict[str, Any]] = []
    seen: set[str] = set()
    since = start_time
    end_ts = _to_naive_utc_timestamp(end_time)
    for key in variants:
        try:
            items = await news_db.list_events(symbol=key, since=since, limit=limit)
        except Exception:
            continue
        for item in items:
            ts = _to_naive_utc_timestamp(item.get("ts"))
            if ts is None:
                continue
            if end_ts is not None and ts > end_ts:
                continue
            event_key = str(
                item.get("event_id")
                or f"{ts.isoformat()}::{item.get('event_type') or ''}::{item.get('symbol') or ''}::{item.get('impact_score') or ''}"
            )
            if event_key in seen:
                continue
            seen.add(event_key)
            rows.append(item)
    rows.sort(key=lambda item: _to_naive_utc_timestamp(item.get("ts")) or pd.Timestamp.min)
    return rows


def _event_type_channel_weights(event_type: str) -> Dict[str, float]:
    et = str(event_type or "").strip().lower()
    mapping = {
        "macro": {"macro": 1.0},
        "etf": {"macro": 1.0, "flow": 0.6},
        "institution": {"macro": 0.8, "whale": 0.5},
        "exchange": {"flow": 1.0},
        "listing": {"flow": 0.8},
        "liquidation": {"flow": 1.0, "macro": 0.4},
        "hack": {"macro": 0.8, "whale": 0.4},
        "regulation": {"macro": 1.0},
        "tech": {"macro": 0.5},
    }
    return mapping.get(et, {"macro": 0.2})


def _build_news_feature_frame(index: pd.DatetimeIndex, events: List[Dict[str, Any]]) -> pd.DataFrame:
    idx = _as_naive_utc_index(index)
    out = pd.DataFrame(
        {
            "news_sentiment_score": np.zeros(len(idx), dtype=float),
            "news_event_intensity": np.zeros(len(idx), dtype=float),
            "news_macro_score": np.zeros(len(idx), dtype=float),
            "news_flow_score": np.zeros(len(idx), dtype=float),
            "news_whale_score": np.zeros(len(idx), dtype=float),
            "news_event_count": np.zeros(len(idx), dtype=float),
        },
        index=idx,
    )
    if len(idx) == 0 or not events:
        return out

    idx_ns = idx.view("int64")
    for item in events:
        try:
            event_ts = pd.Timestamp(item.get("ts"))
        except Exception:
            continue
        if event_ts.tzinfo is not None:
            event_ts = event_ts.tz_convert("UTC").tz_localize(None)
        event_ns = event_ts.value
        start_idx = int(np.searchsorted(idx_ns, event_ns, side="left"))
        if start_idx >= len(idx):
            continue
        impact = max(0.0, min(1.0, float(item.get("impact_score") or 0.0)))
        if impact <= 0:
            continue
        sentiment = float(item.get("sentiment") or 0.0)
        half_life = max(1.0, float(item.get("half_life_min") or 60.0))
        deltas_min = ((idx_ns[start_idx:] - event_ns) / 60_000_000_000.0).astype(float)
        decay = np.exp(-np.log(2.0) * np.maximum(deltas_min, 0.0) / half_life)
        signed = sentiment * impact * decay
        weights = _event_type_channel_weights(item.get("event_type"))

        out.iloc[start_idx:, out.columns.get_loc("news_sentiment_score")] += signed
        out.iloc[start_idx:, out.columns.get_loc("news_event_intensity")] += impact * decay
        out.iloc[start_idx:, out.columns.get_loc("news_event_count")] += decay
        out.iloc[start_idx:, out.columns.get_loc("news_macro_score")] += signed * float(weights.get("macro", 0.0))
        out.iloc[start_idx:, out.columns.get_loc("news_flow_score")] += signed * float(weights.get("flow", 0.0))
        out.iloc[start_idx:, out.columns.get_loc("news_whale_score")] += signed * float(weights.get("whale", 0.0))

    for col in out.columns:
        if col == "news_event_count":
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
        else:
            out[col] = _clamp_series(out[col], -3.0, 3.0)
    return out


async def _build_research_enrichment(
    symbol: str,
    start_time: Optional[datetime],
    end_time: Optional[datetime],
) -> Dict[str, Any]:
    events = await _load_news_events_for_symbol(symbol=symbol, start_time=start_time, end_time=end_time, limit=5000)

    funding_provider = FundingRateProvider()
    funding_available = False
    try:
        funding_provider.ensure_history(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            source="auto",
            save=True,
        )
        funding_available = not funding_provider.get_series(symbol, start_time=start_time, end_time=end_time).empty
    except Exception:
        funding_available = False

    return {
        "events": events,
        "events_count": len(events),
        "funding_provider": funding_provider if funding_available else None,
        "funding_available": funding_available,
    }


def _attach_research_enrichment(
    df: pd.DataFrame,
    symbol: str,
    enrichment: Dict[str, Any],
) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    idx = _as_naive_utc_index(out.index)
    out.index = idx

    news_features = _build_news_feature_frame(idx, list(enrichment.get("events") or []))
    for col in news_features.columns:
        out[col] = news_features[col].values

    funding_provider = enrichment.get("funding_provider")
    if funding_provider is not None:
        try:
            out = funding_provider.attach_to_ohlcv_df(
                out,
                symbol=symbol,
                column="funding_rate",
                fill_forward=True,
                default_rate=0.0,
                overwrite=True,
            )
        except Exception:
            out["funding_rate"] = 0.0
    else:
        out["funding_rate"] = 0.0

    out["funding_rate"] = pd.to_numeric(out.get("funding_rate"), errors="coerce").fillna(0.0)
    return out


def _build_positions(
    strategy: str,
    df: pd.DataFrame,
    params: Optional[Dict[str, Any]] = None,
    strategy_programs: Optional[Dict[str, StrategyProgram]] = None,
) -> pd.Series:
    params = params or {}
    program = dict(strategy_programs or {}).get(str(strategy))
    if program is not None:
        return build_program_positions(program, df, params=params).fillna(0.0)
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
    # ── 趋势新增 ──────────────────────────────────────────────
    elif strategy == "AroonStrategy":
        period = int(params.get("period", 25))
        threshold = float(params.get("threshold", 70.0))
        high = df["high"]
        low = df["low"]
        aroon_up = high.rolling(period + 1, min_periods=period + 1).apply(
            lambda x: float(np.argmax(x)) / period * 100, raw=True
        )
        aroon_down = low.rolling(period + 1, min_periods=period + 1).apply(
            lambda x: float(np.argmin(x)) / period * 100, raw=True
        )
        in_position = False
        values = []
        for up, dn in zip(aroon_up.fillna(50.0), aroon_down.fillna(50.0)):
            if not in_position and up >= threshold and up > dn:
                in_position = True
            elif in_position and dn > up:
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)

    # ── 震荡新增 ──────────────────────────────────────────────
    elif strategy == "WilliamsRStrategy":
        period = int(params.get("period", 14))
        oversold = float(params.get("oversold", -80.0))
        overbought = float(params.get("overbought", -20.0))
        hh = df["high"].rolling(period, min_periods=period).max()
        ll = df["low"].rolling(period, min_periods=period).min()
        wr = -100.0 * (hh - df["close"]) / (hh - ll).replace(0, np.nan)
        in_position = False
        values = []
        for w in wr.fillna(-50.0):
            if not in_position and w <= oversold:
                in_position = True
            elif in_position and w >= overbought:
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)

    elif strategy == "CCIStrategy":
        period = int(params.get("period", 20))
        overbought = float(params.get("overbought", 100.0))
        oversold = float(params.get("oversold", -100.0))
        typical = (df["high"] + df["low"] + df["close"]) / 3.0
        ma = typical.rolling(period, min_periods=period).mean()
        mad = typical.rolling(period, min_periods=period).apply(
            lambda x: np.abs(x - x.mean()).mean(), raw=True
        )
        cci = (typical - ma) / (0.015 * mad.replace(0, np.nan))
        in_position = False
        values = []
        for c in cci.fillna(0.0):
            if not in_position and c <= oversold:
                in_position = True
            elif in_position and c >= overbought:
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)

    elif strategy == "StochRSIStrategy":
        rsi_period = int(params.get("rsi_period", 14))
        stoch_period = int(params.get("stoch_period", 14))
        signal_period = int(params.get("signal_period", 3))
        oversold = float(params.get("oversold", 20.0))
        overbought = float(params.get("overbought", 80.0))
        delta = df["close"].diff()
        gain = delta.clip(lower=0).rolling(rsi_period, min_periods=rsi_period).mean()
        loss = (-delta).clip(lower=0).rolling(rsi_period, min_periods=rsi_period).mean()
        rsi_val = 100.0 - 100.0 / (1.0 + gain / loss.replace(0, np.nan))
        rsi_min = rsi_val.rolling(stoch_period, min_periods=stoch_period).min()
        rsi_max = rsi_val.rolling(stoch_period, min_periods=stoch_period).max()
        stoch_rsi = (rsi_val - rsi_min) / (rsi_max - rsi_min).replace(0, np.nan) * 100.0
        stoch_sig = stoch_rsi.rolling(signal_period, min_periods=signal_period).mean()
        in_position = False
        values = []
        prev_s, prev_sig = np.nan, np.nan
        for s, sig in zip(stoch_rsi.fillna(50.0), stoch_sig.fillna(50.0)):
            cross_up = pd.notna(prev_s) and prev_s <= prev_sig and s > sig
            cross_dn = pd.notna(prev_s) and prev_s >= prev_sig and s < sig
            if not in_position and cross_up and s <= oversold:
                in_position = True
            elif in_position and cross_dn and s >= overbought:
                in_position = False
            values.append(1.0 if in_position else 0.0)
            prev_s, prev_sig = s, sig
        position = pd.Series(values, index=df.index)

    # ── 动量新增 ──────────────────────────────────────────────
    elif strategy == "ROCStrategy":
        period = int(params.get("period", 12))
        threshold = float(params.get("threshold", 0.0))
        roc = (df["close"] / df["close"].shift(period).replace(0, np.nan) - 1.0) * 100.0
        position = (roc > threshold).astype(float)

    elif strategy == "PriceAccelerationStrategy":
        period = int(params.get("period", 14))
        velocity = df["close"].diff(period)
        acceleration = velocity.diff(period)
        in_position = False
        values = []
        prev_acc = np.nan
        for acc in acceleration.fillna(0.0):
            if not in_position and acc > 0 and pd.notna(prev_acc) and acc > prev_acc:
                in_position = True
            elif in_position and acc < 0:
                in_position = False
            values.append(1.0 if in_position else 0.0)
            prev_acc = acc
        position = pd.Series(values, index=df.index)

    # ── 均值回归新增 ───────────────────────────────────────────
    elif strategy == "VWAPStrategy":
        window = int(params.get("window", 24))
        typical = (df["high"] + df["low"] + df["close"]) / 3.0
        vol = df["volume"].replace(0, np.nan)
        vwap = (typical * vol).rolling(window, min_periods=window).sum() / vol.rolling(window, min_periods=window).sum()
        position = (df["close"] > vwap).astype(float)

    elif strategy == "MeanReversionHalfLifeStrategy":
        period = int(params.get("lookback_period", 30))
        z_entry = float(params.get("entry_z_score", 1.5))
        mean = df["close"].rolling(period, min_periods=period).mean()
        std = df["close"].rolling(period, min_periods=period).std()
        z = (df["close"] - mean) / std.replace(0, np.nan)
        in_position = False
        values = []
        for z_val in z.fillna(0.0):
            if not in_position and z_val <= -z_entry:
                in_position = True
            elif in_position and z_val >= 0.0:
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)

    # ── 成交量新增 ─────────────────────────────────────────────
    elif strategy == "MFIStrategy":
        period = int(params.get("period", 14))
        overbought = float(params.get("overbought", 80.0))
        oversold = float(params.get("oversold", 20.0))
        typical = (df["high"] + df["low"] + df["close"]) / 3.0
        mf = typical * df["volume"].replace(0, 0.0)
        prev_typical = typical.shift(1)
        pos_mf = mf.where(typical > prev_typical, 0.0)
        neg_mf = mf.where(typical <= prev_typical, 0.0)
        mfr = pos_mf.rolling(period, min_periods=period).sum() / neg_mf.rolling(period, min_periods=period).sum().replace(0, np.nan)
        mfi = 100.0 - 100.0 / (1.0 + mfr)
        in_position = False
        values = []
        for m in mfi.fillna(50.0):
            if not in_position and m <= oversold:
                in_position = True
            elif in_position and m >= overbought:
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)

    elif strategy == "OBVStrategy":
        period = int(params.get("period", 20))
        obv = (np.sign(df["close"].diff().fillna(0)) * df["volume"]).cumsum()
        obv_ma = obv.rolling(period, min_periods=period).mean()
        position = (obv > obv_ma).astype(float)

    elif strategy == "TradeIntensityStrategy":
        period = int(params.get("period", 20))
        threshold = float(params.get("threshold", 1.5))
        hold = int(params.get("hold_bars", 3))
        vol_ma = df["volume"].rolling(period, min_periods=period).mean()
        intensity = df["volume"] / vol_ma.replace(0, np.nan)
        price_up = (df["close"] > df["close"].shift(1)).astype(float)
        signal = ((intensity >= threshold) & (price_up == 1.0)).astype(float)
        position = signal.rolling(hold, min_periods=1).max()

    # ── 波动率 / 风险新增 ──────────────────────────────────────
    elif strategy == "ParkinsonVolStrategy":
        period = int(params.get("period", 20))
        log_hl = np.log((df["high"] / df["low"].replace(0, np.nan)).clip(lower=1e-9))
        park_vol = ((log_hl ** 2) / (4.0 * np.log(2))).rolling(period, min_periods=period).mean().apply(np.sqrt)
        vol_ma = park_vol.rolling(period * 2, min_periods=period).mean()
        price_above_mean = (df["close"] > df["close"].rolling(period, min_periods=period).mean()).astype(float)
        low_vol = (park_vol < vol_ma).astype(float)
        position = (low_vol * price_above_mean)

    elif strategy == "UlcerIndexStrategy":
        period = int(params.get("period", 14))
        threshold = float(params.get("ulcer_threshold", 5.0))
        peak = df["close"].rolling(period, min_periods=period).max()
        dd_pct = ((df["close"] - peak) / peak.replace(0, np.nan) * 100.0).clip(upper=0.0)
        ulcer = ((dd_pct ** 2).rolling(period, min_periods=period).mean()).apply(np.sqrt)
        position = (ulcer < threshold).astype(float)

    elif strategy == "VaRBreakoutStrategy":
        period = int(params.get("period", 20))
        var_lookback = int(params.get("var_lookback", 120))
        confidence = float(params.get("confidence", 0.95))
        rets = df["close"].pct_change()
        var = rets.rolling(var_lookback, min_periods=min(50, var_lookback)).quantile(1.0 - confidence)
        hh = df["high"].rolling(period, min_periods=period).max().shift(1)
        in_position = False
        values = []
        for c, h, v in zip(df["close"].ffill(), hh.fillna(float("inf")), var.fillna(-0.02)):
            if not in_position and c > h and abs(v) < 0.03:
                in_position = True
            elif in_position and c < h * 0.97:
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)

    elif strategy == "MaxDrawdownStrategy":
        period = int(params.get("period", 20))
        drawdown_entry = float(params.get("drawdown_entry", -0.05))
        peak = df["close"].rolling(period, min_periods=period).max()
        dd = (df["close"] - peak) / peak.replace(0, np.nan)
        in_position = False
        values = []
        for d in dd.fillna(0.0):
            if not in_position and d <= drawdown_entry:
                in_position = True
            elif in_position and d >= -0.005:
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)

    elif strategy == "SortinoRatioStrategy":
        period = int(params.get("period", 30))
        threshold = float(params.get("sortino_threshold", 1.0))
        rets = df["close"].pct_change()
        mean_r = rets.rolling(period, min_periods=period).mean()
        downside_std = rets.clip(upper=0.0).rolling(period, min_periods=period).std().replace(0, np.nan)
        sortino = (mean_r / downside_std) * np.sqrt(period)
        position = (sortino > threshold).astype(float).fillna(0.0)

    # ── 统计套利新增 ───────────────────────────────────────────
    elif strategy == "HurstExponentStrategy":
        period = int(params.get("period", 50))
        trend_thr = float(params.get("trend_threshold", 0.55))
        rev_thr = float(params.get("reversion_threshold", 0.45))
        k = max(4, period // 4)
        # Fast vectorized Hurst estimate via variance scaling
        var1 = df["close"].pct_change(1).rolling(period, min_periods=period).var().replace(0, np.nan)
        vark = df["close"].pct_change(k).rolling(period, min_periods=period).var().replace(0, np.nan)
        ratio = (vark / var1).clip(1e-4, 1e4)
        hurst = (np.log(ratio.apply(np.sqrt)) / np.log(float(k))).clip(0.1, 0.9).fillna(0.5)
        # Regime-switched signal
        fast_ma = df["close"].rolling(10, min_periods=10).mean()
        slow_ma = df["close"].rolling(30, min_periods=30).mean()
        trend_sig = (fast_ma > slow_ma).astype(float)
        mean_sig = (df["close"] < df["close"].rolling(period, min_periods=period).mean()).astype(float)
        position = pd.Series(
            np.where(hurst > trend_thr, trend_sig,
                     np.where(hurst < rev_thr, mean_sig, 0.0)),
            index=df.index,
        ).fillna(0.0)

    # ── 量化 / 微观结构新增 ───────────────────────────────────
    elif strategy == "OrderFlowImbalanceStrategy":
        period = int(params.get("period", 20))
        threshold = float(params.get("imbalance_threshold", 0.6))
        price_up = df["close"] > df["close"].shift(1)
        buy_vol = df["volume"].where(price_up, 0.0)
        sell_vol = df["volume"].where(~price_up, 0.0)
        total_vol = (buy_vol + sell_vol).rolling(period, min_periods=period).sum().replace(0, np.nan)
        buy_ratio = buy_vol.rolling(period, min_periods=period).sum() / total_vol
        position = (buy_ratio >= threshold).astype(float)

    elif strategy == "MultiFactorHFStrategy":
        period = int(params.get("period", 10))
        typical = (df["high"] + df["low"] + df["close"]) / 3.0
        vol = df["volume"].replace(0, np.nan)
        vwap = (typical * vol).rolling(period, min_periods=period).sum() / vol.rolling(period, min_periods=period).sum()
        vol_ma = df["volume"].rolling(period * 2, min_periods=period).mean().replace(0, np.nan)
        f_momentum = (df["close"].pct_change(period) > 0).astype(float)
        f_volume = (df["volume"] / vol_ma > 1.2).astype(float)
        f_vwap = (df["close"] > vwap).astype(float)
        position = ((f_momentum + f_volume + f_vwap) >= 2.0).astype(float)

    # ── ML XGBoost ─────────────────────────────────────────────
    elif strategy == "MarketSentimentStrategy":
        lookback = max(5, int(params.get("lookback_period", 7)))
        fear_th = float(params.get("fear_threshold", 25))
        greed_th = float(params.get("greed_threshold", 75))
        regime_window = max(lookback * 6, 24)
        regime_ret = close.pct_change(lookback).clip(-0.20, 0.20)
        fear_greed_proxy = _rolling_minmax_scale(regime_ret, regime_window, 0.0, 100.0)
        news_sentiment = _clamp_series(_data_column_or_default(df, "news_sentiment_score"), -3.0, 3.0)
        macro_score = _clamp_series(_data_column_or_default(df, "news_macro_score"), -3.0, 3.0)
        funding_rate = _clamp_series(_data_column_or_default(df, "funding_rate"), -0.02, 0.02)
        sentiment_component = (
            50.0 + 28.0 * np.tanh(news_sentiment * 0.65 + macro_score * 0.90 - funding_rate * 180.0)
        ).clip(0.0, 100.0)
        fear_greed_score = (fear_greed_proxy * 0.55 + sentiment_component * 0.45).clip(0.0, 100.0)
        in_position = False
        values = []
        for score in fear_greed_score.fillna(50.0):
            if not in_position and score <= fear_th:
                in_position = True
            elif in_position and score >= max(50.0, greed_th - 15.0):
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)

    elif strategy == "SocialSentimentStrategy":
        pos_th = float(params.get("positive_threshold", 0.2))
        neg_th = float(params.get("negative_threshold", -0.2))
        momentum = close.pct_change(6).clip(-0.15, 0.15)
        volume_ratio = df["volume"] / df["volume"].rolling(24, min_periods=12).mean().replace(0, np.nan)
        news_sentiment = _clamp_series(_data_column_or_default(df, "news_sentiment_score"), -3.0, 3.0)
        event_intensity = _clamp_series(_data_column_or_default(df, "news_event_intensity"), 0.0, 3.0)
        social_score = np.tanh(
            news_sentiment * 0.85
            + event_intensity * 0.30
            + momentum * 6.5
            + (volume_ratio.fillna(1.0) - 1.0) * 0.8
        )
        in_position = False
        values = []
        for score in social_score.fillna(0.0):
            if not in_position and score >= pos_th:
                in_position = True
            elif in_position and score <= max(0.0, neg_th + 0.1):
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)

    elif strategy == "FundFlowStrategy":
        flow_window = max(6, int(params.get("lookback_period", 7)) * 3)
        min_ratio = abs(float(params.get("min_imbalance_ratio", 0.03)))
        signed_flow = (close.pct_change().clip(-0.05, 0.05) * close * df["volume"]).fillna(0.0)
        flow_sum = signed_flow.rolling(flow_window, min_periods=max(4, flow_window // 3)).sum()
        flow_abs = signed_flow.abs().rolling(flow_window, min_periods=max(4, flow_window // 3)).sum().replace(0, np.nan)
        imbalance = (flow_sum / flow_abs).fillna(0.0)
        news_flow = _clamp_series(_data_column_or_default(df, "news_flow_score"), -3.0, 3.0)
        funding_rate = _clamp_series(_data_column_or_default(df, "funding_rate"), -0.02, 0.02)
        flow_score = (
            imbalance * 0.75
            + np.tanh(news_flow * 0.70) * 0.20
            - np.tanh(funding_rate * 160.0) * 0.10
        ).clip(-1.0, 1.0)
        neutral_ratio = max(min_ratio * 0.5, 0.01)
        in_position = False
        values = []
        for score in flow_score:
            if not in_position and score >= min_ratio:
                in_position = True
            elif in_position and score <= neutral_ratio:
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)

    elif strategy == "WhaleActivityStrategy":
        lookback = max(6, int(params.get("lookback_hours", 24)))
        accumulation = max(1, int(params.get("accumulation_threshold", 2)))
        distribution = max(1, int(params.get("distribution_threshold", 2)))
        notional = (close * df["volume"]).fillna(0.0)
        baseline = notional.rolling(lookback, min_periods=max(4, lookback // 3)).mean().replace(0, np.nan)
        whale_bar = (notional >= baseline * 1.8).fillna(False)
        buy_spikes = (whale_bar & (close.pct_change().fillna(0.0) > 0)).rolling(lookback, min_periods=1).sum()
        sell_spikes = (whale_bar & (close.pct_change().fillna(0.0) < 0)).rolling(lookback, min_periods=1).sum()
        news_whale = _clamp_series(_data_column_or_default(df, "news_whale_score"), -3.0, 3.0)
        event_intensity = _clamp_series(_data_column_or_default(df, "news_event_intensity"), 0.0, 3.0)
        whale_signal = np.tanh(news_whale * 0.90 + event_intensity * 0.15)
        buy_pressure = buy_spikes.fillna(0.0) + whale_signal.clip(lower=0.0) * float(accumulation)
        sell_pressure = sell_spikes.fillna(0.0) + (-whale_signal.clip(upper=0.0)) * float(distribution)
        in_position = False
        values = []
        for buy_count, sell_count in zip(buy_pressure, sell_pressure):
            if not in_position and buy_count >= accumulation and buy_count > sell_count:
                in_position = True
            elif in_position and sell_count >= distribution:
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)

    elif strategy == "MLXGBoostStrategy":
        try:
            import xgboost as xgb  # noqa: F401
        except ImportError:
            raise ValueError("MLXGBoostStrategy requires xgboost: pip install xgboost")
        from pathlib import Path as _Path
        from core.ai.ml_signal import build_feature_frame

        feat_df = build_feature_frame(df)
        # Locate model file
        model_path = str(params.get("model_path", ""))
        if not model_path or not _Path(model_path).exists():
            candidates = [
                _Path(model_path) if model_path else None,
                _Path("models/ml_signal_xgb.json"),
                _Path(__file__).parent.parent.parent / "models" / "ml_signal_xgb.json",
            ]
            model_path = next((str(p) for p in candidates if p and p.exists()), None)
        if not model_path:
            raise ValueError(
                "MLXGBoostStrategy: model file not found. "
                "Run: python scripts/train_ml_signal.py --symbol BTC/USDT --timeframe 1h"
            )
        booster = xgb.Booster()
        booster.load_model(model_path)
        threshold_ml = float(params.get("threshold", 0.55))
        dtest = xgb.DMatrix(feat_df.values, feature_names=list(feat_df.columns))
        long_probs = booster.predict(dtest)
        short_probs = 1.0 - long_probs
        # Only long signals (0/1); short = sit out
        signals = np.where(long_probs >= threshold_ml, 1.0, 0.0)
        position = pd.Series(signals, index=df.index)

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
    strategy_programs: Optional[Dict[str, StrategyProgram]] = None,
) -> Dict[str, Any]:
    if len(df) < 50:
        raise ValueError("数据不足，至少需要 50 根 K 线")

    position = _build_positions(strategy, df, params=params, strategy_programs=strategy_programs)
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
    # B: parameter space for grid search {strategy_name: {param: [val1, val2, ...]}}
    parameter_space: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    strategy_programs: Dict[str, StrategyProgram] = field(default_factory=dict)


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


def _compute_score(metrics: Dict[str, Any]) -> float:
    """Shared scoring formula (short-term oriented)."""
    _sr = float(metrics.get("sharpe_ratio", 0.0) or 0.0)
    _dd = max(float(metrics.get("max_drawdown", 0.0) or 0.0), 0.5)
    _tr = float(metrics.get("total_return", 0.0) or 0.0)
    _wr = float(metrics.get("win_rate", 0.0) or 0.0)
    _cost = abs(float(metrics.get("cost_drag_return_pct", 0.0) or 0.0))
    _anom = float(metrics.get("anomaly_bar_ratio", 0.0) or 0.0)
    return (
        _sr * 15.0
        + (_tr / _dd) * 3.0
        + _wr * 0.2
        - _dd * 1.5
        - _cost * 0.3
        - _anom * 300.0
    )


def _generate_param_combos(param_grid: Dict[str, Any], max_combos: int = 20) -> List[Dict[str, Any]]:
    """B: Generate cartesian product of parameter combinations, capped at max_combos."""
    if not param_grid:
        return [{}]
    import itertools
    keys = list(param_grid.keys())
    value_lists = []
    for k in keys:
        v = param_grid[k]
        if isinstance(v, (list, tuple)):
            value_lists.append(list(v))
        else:
            value_lists.append([v])
    all_combos = list(itertools.product(*value_lists))
    if len(all_combos) <= max_combos:
        return [dict(zip(keys, combo)) for combo in all_combos]
    # Stride-sample to cap while maintaining coverage
    step = max(1, len(all_combos) // max_combos)
    sampled = all_combos[::step][:max_combos]
    return [dict(zip(keys, combo)) for combo in sampled]


def _optimize_params_scipy_lhs(
    strategy: str,
    param_grid: Dict[str, Any],
    is_df: pd.DataFrame,
    timeframe: str,
    commission_rate: float,
    slippage_bps: float,
    initial_capital: float,
    max_trials: int = 30,
    strategy_programs: Optional[Dict[str, StrategyProgram]] = None,
) -> tuple:
    """Scipy Latin Hypercube Sampling optimization. Returns (best_params, n_trials, method)."""
    if not param_grid:
        return {}, 0, "none"

    keys = list(param_grid.keys())
    value_lists = []
    for k in keys:
        v = param_grid[k]
        value_lists.append(list(v) if isinstance(v, (list, tuple)) else [v])

    n_params = len(keys)
    n_samples = min(max_trials, 30)

    # Try scipy LHS
    try:
        from scipy.stats import qmc
        sampler = qmc.LatinHypercube(d=n_params, seed=42)
        raw_samples = sampler.random(n=n_samples)  # shape (n_samples, n_params)
        param_combos = []
        for row in raw_samples:
            combo = {}
            for i, k in enumerate(keys):
                vals = value_lists[i]
                idx = int(row[i] * len(vals))
                idx = min(idx, len(vals) - 1)
                combo[k] = vals[idx]
            param_combos.append(combo)
        method = "scipy_lhs"
    except Exception:
        # Fallback to grid
        param_combos = _generate_param_combos(param_grid, max_combos=max_trials)
        method = "grid"

    best_params: Dict[str, Any] = {}
    best_score = -999999.0
    n_trials = 0

    for combo in param_combos:
        try:
            m = _run_backtest_core(
                strategy=strategy,
                df=is_df,
                timeframe=timeframe,
                initial_capital=initial_capital,
                params=combo,
                commission_rate=commission_rate,
                slippage_bps=slippage_bps,
                strategy_programs=strategy_programs,
            )
            n_trials += 1
            if m.get("quality_flag") != "invalid":
                s = _compute_score(m)
                if s > best_score:
                    best_score = s
                    best_params = dict(combo)
        except Exception:
            pass

    # Local refinement: perturb best params by ±1 index and take better result
    if best_params and method == "scipy_lhs":
        for k, v in list(best_params.items()):
            vals = value_lists[keys.index(k)]
            if len(vals) > 1:
                idx = vals.index(v) if v in vals else 0
                for delta in [-1, 1]:
                    nidx = max(0, min(len(vals) - 1, idx + delta))
                    ncombo = dict(best_params)
                    ncombo[k] = vals[nidx]
                    try:
                        m = _run_backtest_core(
                            strategy=strategy,
                            df=is_df,
                            timeframe=timeframe,
                            initial_capital=initial_capital,
                            params=ncombo,
                            commission_rate=commission_rate,
                            slippage_bps=slippage_bps,
                            strategy_programs=strategy_programs,
                        )
                        n_trials += 1
                        if m.get("quality_flag") != "invalid":
                            s = _compute_score(m)
                            if s > best_score:
                                best_score = s
                                best_params = dict(ncombo)
                    except Exception:
                        pass

    return best_params, n_trials, method


def _run_walk_forward(
    strategy: str,
    df: pd.DataFrame,
    timeframe: str,
    params: Dict[str, Any],
    n_splits: int,
    commission_rate: float,
    slippage_bps: float,
    initial_capital: float,
    strategy_programs: Optional[Dict[str, StrategyProgram]] = None,
) -> List[float]:
    """C: Expanding-window walk-forward — return list of OOS Sharpe ratios per fold."""
    n = len(df)
    chunk_size = n // (n_splits + 1)
    if chunk_size < 50:
        return []
    sharpe_list: List[float] = []
    for i in range(1, n_splits + 1):
        is_end = i * chunk_size
        oos_end = (i + 1) * chunk_size
        oos_slice = df.iloc[is_end:oos_end]
        if len(oos_slice) < 50:
            continue
        try:
            m = _run_backtest_core(
                strategy=strategy,
                df=oos_slice,
                timeframe=timeframe,
                initial_capital=initial_capital,
                params=params,
                commission_rate=commission_rate,
                slippage_bps=slippage_bps,
                strategy_programs=strategy_programs,
            )
            if m.get("quality_flag") != "invalid":
                sharpe_list.append(float(m.get("sharpe_ratio", 0.0) or 0.0))
        except Exception:
            pass
    return sharpe_list


def _run_purged_walk_forward(
    strategy: str,
    df: pd.DataFrame,
    timeframe: str,
    params: Dict[str, Any],
    n_splits: int = 5,
    embargo_pct: float = 0.01,
    commission_rate: float = 0.0004,
    slippage_bps: float = 2.0,
    initial_capital: float = 10000.0,
    strategy_programs: Optional[Dict[str, StrategyProgram]] = None,
) -> Dict[str, Any]:
    """Purged expanding-window walk-forward with embargo gap to prevent data leakage."""
    n = len(df)
    min_required = max(50 * int(n_splits), 100)
    if n < min_required:
        logger.debug(
            f"walk-forward skipped: rows={n} < min_required={min_required}, "
            f"strategy={strategy}, timeframe={timeframe}, splits={n_splits}"
        )
        return {
            "sharpe_list": [],
            "consistency": None,
            "n_folds": 0,
            "positive_folds": 0,
        }
    embargo_bars = max(1, int(n * embargo_pct))
    min_is = max(50, n // (n_splits + 2))

    sharpe_list: List[float] = []
    positive_folds = 0

    for i in range(1, n_splits + 1):
        is_end = int(n * i / (n_splits + 1))
        if is_end < min_is:
            continue
        oos_start = is_end + embargo_bars
        oos_end = int(n * (i + 1) / (n_splits + 1))
        if oos_start >= oos_end or (oos_end - oos_start) < 50:
            continue
        oos_slice = df.iloc[oos_start:oos_end]
        try:
            m = _run_backtest_core(
                strategy=strategy,
                df=oos_slice,
                timeframe=timeframe,
                initial_capital=initial_capital,
                params=params,
                commission_rate=commission_rate,
                slippage_bps=slippage_bps,
                strategy_programs=strategy_programs,
            )
            if m.get("quality_flag") != "invalid":
                sr = float(m.get("sharpe_ratio", 0.0) or 0.0)
                sharpe_list.append(sr)
                if m.get("total_return", 0.0) > 0:
                    positive_folds += 1
        except Exception:
            pass

    n_folds = len(sharpe_list)
    consistency = (positive_folds / max(n_folds, 1)) if n_folds > 0 else None
    return {
        "sharpe_list": sharpe_list,
        "consistency": round(consistency, 4) if consistency is not None else None,
        "n_folds": n_folds,
        "positive_folds": positive_folds,
    }


def _compute_wf_stability(wf_result) -> Optional[float]:
    """C: Stability score [0, 1] — higher means more consistent OOS results."""
    if isinstance(wf_result, dict):
        sharpe_list = wf_result.get("sharpe_list", [])
    else:
        sharpe_list = list(wf_result) if wf_result else []
    if not sharpe_list:
        return None
    if len(sharpe_list) < 2:
        return 0.5  # Cannot measure stability with single fold
    mean_s = float(np.mean(sharpe_list))
    std_s = float(np.std(sharpe_list))
    abs_mean = max(abs(mean_s), 0.01)
    cv = std_s / abs_mean  # coefficient of variation
    stability = max(0.0, min(1.0, 1.0 - cv))
    return round(stability, 4)


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
    data_source_timeframe: str = "1s",
) -> str:
    lines: List[str] = []
    lines.append("# 策略研究报告")
    lines.append("")
    lines.append(f"- 交易所: `{config.exchange}`")
    lines.append(f"- 交易对: `{config.symbol}`")
    lines.append(f"- 数据范围: `{full_df.index.min().isoformat()}` ~ `{full_df.index.max().isoformat()}`")
    lines.append(f"- 基础K线周期: `{data_source_timeframe}`")
    lines.append(f"- 基础K线条数: `{len(full_df)}`")
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


async def run_strategy_research(
    config: ResearchConfig,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    config.symbol = _normalize_symbol(config.symbol)
    config.exchange = (config.exchange or "binance").lower().strip()
    config.days = max(1, int(config.days))
    config.min_rows_per_timeframe = max(80, int(config.min_rows_per_timeframe))
    config.timeframes = [tf for tf in config.timeframes if tf in _RESAMPLE_RULES]
    if not config.timeframes:
        raise ValueError("timeframes 为空或不支持")

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=config.days)
    try:
        enrichment = await _build_research_enrichment(
            symbol=config.symbol,
            start_time=start_time,
            end_time=end_time,
        )
    except Exception as e:
        logger.warning(f"research enrichment unavailable for {config.symbol}: {e}")
        enrichment = {
            "events": [],
            "events_count": 0,
            "funding_provider": None,
            "funding_available": False,
        }

    base_df = await _load_second_level_df(
        exchange=config.exchange,
        symbol=config.symbol,
        start_time=start_time,
        end_time=end_time,
    )
    data_source_timeframe = "1s"
    frames: Dict[str, pd.DataFrame] = {}
    if not base_df.empty:
        for timeframe in config.timeframes:
            tf_df = _resample_ohlcv(base_df, timeframe)
            tf_df = _validate_df(tf_df)
            tf_df = _attach_research_enrichment(tf_df, config.symbol, enrichment)
            if len(tf_df) >= config.min_rows_per_timeframe:
                frames[timeframe] = tf_df
            else:
                logger.warning(
                    f"Skip timeframe={timeframe} rows={len(tf_df)} < min_rows={config.min_rows_per_timeframe}"
                )
    else:
        if _requires_second_level_data(config.timeframes):
            raise ValueError(
                f"未找到 {config.exchange} {config.symbol} 的 1 秒级数据，"
                f"当前时间框架 {config.timeframes} 包含子分钟周期（<1m）。"
                "请先在“数据管理”页面回填 1s 数据，或改用 15m/1h 等标准周期。"
            )

        logger.warning(
            f"1s data missing for {config.exchange} {config.symbol}, fallback to native timeframe parquet loading"
        )
        for timeframe in config.timeframes:
            tf_df = await data_storage.load_klines_from_parquet(
                exchange=config.exchange,
                symbol=config.symbol,
                timeframe=timeframe,
                start_time=start_time,
                end_time=end_time,
            )
            if tf_df.empty:
                logger.warning(f"Skip timeframe={timeframe}, native parquet not found")
                continue
            tf_df = _validate_df(tf_df)
            tf_df = _attach_research_enrichment(tf_df, config.symbol, enrichment)
            if len(tf_df) >= config.min_rows_per_timeframe:
                frames[timeframe] = tf_df
            else:
                logger.warning(
                    f"Skip timeframe={timeframe} rows={len(tf_df)} < min_rows={config.min_rows_per_timeframe}"
                )

        if frames:
            ordered = sorted(frames.keys(), key=lambda tf: (_timeframe_seconds(tf) or 10**9, tf))
            data_source_timeframe = ordered[0]
            base_df = frames[data_source_timeframe]

    if not frames:
        timeframe_text = ", ".join(list(config.timeframes or [])) or "--"
        raise ValueError(
            f"找不到 {config.exchange} {config.symbol} 的历史数据（时间框架: {timeframe_text}，天数: {config.days}）。"
            "请先在“数据管理”页面回填 K 线数据，或切换到有数据的交易对。"
        )

    # B: IS split at 65% for parameter optimization
    is_split = 0.65
    rows: List[Dict[str, Any]] = []
    total_tasks = max(1, len(frames) * len(config.strategies))
    completed_tasks = 0
    for timeframe, tf_df in frames.items():
        is_end_idx = max(50, int(len(tf_df) * is_split))
        is_df = tf_df.iloc[:is_end_idx]
        oos_df = tf_df.iloc[is_end_idx:]
        can_split = len(is_df) >= 50 and len(oos_df) >= 50

        for strategy in config.strategies:
            if progress_callback is not None:
                try:
                    progress_callback(
                        {
                            "stage": "running",
                            "strategy": str(strategy),
                            "timeframe": str(timeframe),
                            "completed": completed_tasks,
                            "total": total_tasks,
                        }
                    )
                except Exception:
                    pass
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
                # B/C new fields
                "best_params": {},
                "optimization_trials": 0,
                "is_sharpe": None,
                "oos_sharpe": None,
                "wf_stability": None,
                "wf_consistency": None,
                "opt_method": "none",
                "equity_curve_sample": [],
            }
            try:
                # ── B: LHS optimization on IS data ─────────────────────
                param_grid = dict(config.parameter_space.get(strategy) or {})
                best_params: Dict[str, Any] = {}
                best_is_metrics: Optional[Dict[str, Any]] = None
                optimization_trials = 0
                opt_method = "none"

                if can_split and param_grid:
                    best_params, optimization_trials, opt_method = _optimize_params_scipy_lhs(
                        strategy=strategy,
                        param_grid=param_grid,
                        is_df=is_df,
                        timeframe=timeframe,
                        commission_rate=float(config.commission_rate),
                        slippage_bps=float(config.slippage_bps),
                        initial_capital=config.initial_capital,
                        max_trials=30,
                        strategy_programs=config.strategy_programs,
                    )
                    if best_params:
                        try:
                            best_is_metrics = _run_backtest_core(
                                strategy=strategy,
                                df=is_df,
                                timeframe=timeframe,
                                initial_capital=config.initial_capital,
                                params=best_params,
                                commission_rate=float(config.commission_rate),
                                slippage_bps=float(config.slippage_bps),
                                strategy_programs=config.strategy_programs,
                            )
                        except Exception:
                            pass

                payload["best_params"] = best_params
                payload["optimization_trials"] = optimization_trials
                payload["opt_method"] = opt_method
                if best_is_metrics is not None:
                    payload["is_sharpe"] = float(best_is_metrics.get("sharpe_ratio", 0.0))

                # ── Full-data run with best params ───────────────────────
                metrics = _run_backtest_core(
                    strategy=strategy,
                    df=tf_df,
                    timeframe=timeframe,
                    initial_capital=config.initial_capital,
                    params=best_params if best_params else None,
                    commission_rate=float(config.commission_rate),
                    slippage_bps=float(config.slippage_bps),
                    strategy_programs=config.strategy_programs,
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

                # ── C: OOS validation ────────────────────────────────────
                oos_metrics: Optional[Dict[str, Any]] = None
                if can_split:
                    try:
                        oos_metrics = _run_backtest_core(
                            strategy=strategy,
                            df=oos_df,
                            timeframe=timeframe,
                            initial_capital=config.initial_capital,
                            params=best_params if best_params else None,
                            commission_rate=float(config.commission_rate),
                            slippage_bps=float(config.slippage_bps),
                            strategy_programs=config.strategy_programs,
                        )
                        if oos_metrics.get("quality_flag") == "invalid":
                            oos_metrics = None
                    except Exception:
                        oos_metrics = None

                if oos_metrics is not None:
                    payload["oos_sharpe"] = float(oos_metrics.get("sharpe_ratio", 0.0))

                # Populate is_sharpe from full-data metrics when no IS run was done
                if payload["is_sharpe"] is None:
                    payload["is_sharpe"] = float(metrics.get("sharpe_ratio", 0.0))

                # ── C: Purged walk-forward stability ──────────────────────
                if can_split:
                    wf_result = _run_purged_walk_forward(
                        strategy=strategy,
                        df=tf_df,
                        timeframe=timeframe,
                        params=best_params if best_params else {},
                        n_splits=5,
                        embargo_pct=0.01,
                        commission_rate=float(config.commission_rate),
                        slippage_bps=float(config.slippage_bps),
                        initial_capital=config.initial_capital,
                        strategy_programs=config.strategy_programs,
                    )
                    payload["wf_stability"] = _compute_wf_stability(wf_result)
                    payload["wf_consistency"] = wf_result.get("consistency")
                else:
                    payload["wf_stability"] = None
                    payload["wf_consistency"] = None
                    logger.debug(
                        f"Skipped walk-forward for {strategy}/{timeframe}: "
                        f"insufficient split data (is={len(is_df)}, oos={len(oos_df)})"
                    )

                # ── Equity curve sample (50 points) ───────────────────
                try:
                    _pos = _build_positions(
                        strategy,
                        tf_df,
                        params=best_params if best_params else None,
                        strategy_programs=config.strategy_programs,
                    )
                    _rets, _, _ = _safe_bar_returns(tf_df["close"], timeframe)
                    _gross = _pos.shift(1).fillna(0.0) * _rets
                    _turnover = _pos.diff().abs().fillna(0.0)
                    _cost = _turnover * (float(config.commission_rate) + float(config.slippage_bps) / 10000.0)
                    _strat_rets = (_gross - _cost).clip(lower=-0.95, upper=5.0)
                    _equity = _safe_equity_curve(_strat_rets, config.initial_capital)
                    _n = len(_equity)
                    _idx = np.linspace(0, _n - 1, min(50, _n), dtype=int)
                    payload["equity_curve_sample"] = [round(float(_equity.iloc[i]), 4) for i in _idx]
                except Exception:
                    payload["equity_curve_sample"] = []

                # ── Score: OOS-weighted when available ───────────────────
                full_score = _compute_score(metrics)
                if oos_metrics is not None:
                    oos_score_raw = _compute_score(oos_metrics)
                    payload["score"] = oos_score_raw * 0.6 + full_score * 0.4
                else:
                    payload["score"] = full_score

            except Exception as e:
                payload["error"] = str(e)
            rows.append(payload)
            completed_tasks += 1
            if progress_callback is not None:
                try:
                    progress_callback(
                        {
                            "stage": "running",
                            "strategy": str(strategy),
                            "timeframe": str(timeframe),
                            "completed": completed_tasks,
                            "total": total_tasks,
                        }
                    )
                except Exception:
                    pass

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

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
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
        data_source_timeframe=data_source_timeframe,
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

    top_results: List[Dict[str, Any]] = []
    if not valid_df.empty:
        for _, row in valid_df.head(5).iterrows():
            top_results.append(
                {
                    "strategy": str(row.get("strategy") or ""),
                    "timeframe": str(row.get("timeframe") or ""),
                    "score": float(row.get("score", 0.0) or 0.0),
                    "total_return": float(row.get("total_return", 0.0) or 0.0),
                    "gross_total_return": float(row.get("gross_total_return", 0.0) or 0.0),
                    "sharpe_ratio": float(row.get("sharpe_ratio", 0.0) or 0.0),
                    "max_drawdown": float(row.get("max_drawdown", 0.0) or 0.0),
                    "win_rate": float(row.get("win_rate", 0.0) or 0.0),
                    "total_trades": int(row.get("total_trades", 0) or 0),
                    "cost_drag_return_pct": float(row.get("cost_drag_return_pct", 0.0) or 0.0),
                    "quality_flag": str(row.get("quality_flag") or "ok"),
                }
            )

    # Best result per strategy (sorted by score desc within each group)
    best_per_strategy: Dict[str, Dict[str, Any]] = {}
    if not valid_df.empty:
        for strat_name, grp in valid_df.groupby("strategy"):
            row = grp.iloc[0].to_dict()
            best_per_strategy[str(strat_name)] = {
                "strategy": str(row.get("strategy") or ""),
                "timeframe": str(row.get("timeframe") or ""),
                "total_return": float(row.get("total_return", 0.0) or 0.0),
                "gross_total_return": float(row.get("gross_total_return", 0.0) or 0.0),
                "cost_drag_return_pct": float(row.get("cost_drag_return_pct", 0.0) or 0.0),
                "sharpe_ratio": float(row.get("sharpe_ratio", 0.0) or 0.0),
                "max_drawdown": float(row.get("max_drawdown", 0.0) or 0.0),
                "win_rate": float(row.get("win_rate", 0.0) or 0.0),
                "total_trades": int(row.get("total_trades", 0) or 0),
                "anomaly_bar_ratio": float(row.get("anomaly_bar_ratio", 0.0) or 0.0),
                "quality_flag": str(row.get("quality_flag", "ok") or "ok"),
                "score": float(row.get("score", 0.0) or 0.0),
                # B: best params from grid search
                "best_params": dict(row.get("best_params") or {}),
                "optimization_trials": int(row.get("optimization_trials", 0) or 0),
                # C: IS/OOS/WF metrics
                "is_sharpe": row.get("is_sharpe"),
                "oos_sharpe": row.get("oos_sharpe"),
                "wf_stability": row.get("wf_stability"),
                "wf_consistency": row.get("wf_consistency"),
                "opt_method": str(row.get("opt_method") or "none"),
                "equity_curve_sample": list(row.get("equity_curve_sample") or []),
            }

    strategy_valid_counts: Dict[str, int] = {}
    if not valid_df.empty:
        strategy_valid_counts = {
            str(name): int(count)
            for name, count in valid_df.groupby("strategy").size().sort_values(ascending=False).items()
        }

    strategy_error_counts: Dict[str, int] = {}
    error_df = result_df[result_df["error"] != ""].copy()
    if not error_df.empty:
        strategy_error_counts = {
            str(name): int(count)
            for name, count in error_df.groupby("strategy").size().sort_values(ascending=False).items()
        }

    return {
        "exchange": config.exchange,
        "symbol": config.symbol,
        "data_start": base_df.index.min().isoformat(),
        "data_end": base_df.index.max().isoformat(),
        "base_rows": int(len(base_df)),
        "data_source_timeframe": data_source_timeframe,
        "timeframes": list(frames.keys()),
        "strategies": list(config.strategies),
        "commission_rate": float(config.commission_rate),
        "slippage_bps": float(config.slippage_bps),
        "news_events_count": int(enrichment.get("events_count", 0) or 0),
        "funding_available": bool(enrichment.get("funding_available", False)),
        "runs": int(len(result_df)),
        "valid_runs": int(len(valid_df)),
        "quality_counts": (
            result_df["quality_flag"].value_counts().to_dict() if "quality_flag" in result_df.columns else {}
        ),
        "top_results": top_results,
        "best_per_strategy": best_per_strategy,
        "strategy_valid_counts": strategy_valid_counts,
        "strategy_error_counts": strategy_error_counts,
        "best": best,
        "csv_path": str(csv_path),
        "markdown_path": str(md_path),
    }
