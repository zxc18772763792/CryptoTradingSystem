"""Backtest API endpoints."""
import asyncio
import io
import itertools
import json
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import matplotlib
matplotlib.use("Agg")
import numpy as np
import pandas as pd
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from matplotlib import pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

from config.strategy_registry import (
    get_backtest_optimization_grid as registry_backtest_optimization_grid,
    get_backtest_strategy_catalog as registry_backtest_strategy_catalog,
    get_backtest_strategy_info as registry_backtest_strategy_info,
    get_strategy_defaults,
    is_strategy_backtest_supported as registry_is_strategy_backtest_supported,
)
from core.backtest.common_pnl import build_common_pnl_summary
from core.backtest.cost_models import fee_rate as resolve_fee_rate
from core.backtest.cost_models import slippage_rate as resolve_slippage_rate
from core.ai.ml_signal import build_feature_frame
from core.data import data_storage
from core.research.strategy_research import (
    _attach_research_enrichment as attach_research_enrichment,
    _build_research_enrichment as build_research_enrichment,
)

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


_BACKTEST_STRATEGY_CATALOG: List[Dict[str, Any]] = registry_backtest_strategy_catalog()
_BACKTEST_STRATEGY_META: Dict[str, Dict[str, Any]] = {
    str(item["name"]): dict(item) for item in _BACKTEST_STRATEGY_CATALOG
}
_BACKTEST_OPTIMIZATION_GRIDS: Dict[str, Dict[str, List[Any]]] = {
    name: registry_backtest_optimization_grid(name)
    for name in _BACKTEST_STRATEGY_META.keys()
    if registry_backtest_optimization_grid(name)
}
_BACKTEST_OPT_OBJECTIVES = {"total_return", "sharpe_ratio", "win_rate"}
_BACKTEST_COMPARE_DEFAULT_TRIALS = 48
_BACKTEST_COMPARE_MAX_TRIALS = 512
_BACKTEST_OPTIMIZE_DEFAULT_TRIALS = 96
_BACKTEST_OPTIMIZE_MAX_TRIALS = 1024


def get_backtest_strategy_catalog() -> List[Dict[str, Any]]:
    return [dict(item) for item in _BACKTEST_STRATEGY_CATALOG]


def get_backtest_strategy_info(strategy: str) -> Dict[str, Any]:
    return dict(registry_backtest_strategy_info(strategy))


def is_strategy_backtest_supported(strategy: str) -> bool:
    return bool(registry_is_strategy_backtest_supported(strategy))


def _strategy_family(strategy: str) -> str:
    meta = _BACKTEST_STRATEGY_META.get(str(strategy or "").strip(), {})
    return str(meta.get("family") or "traditional")


def _strategy_decision_engine(strategy: str) -> str:
    meta = _BACKTEST_STRATEGY_META.get(str(strategy or "").strip(), {})
    return str(meta.get("decision_engine") or "rule")


def _strategy_data_mode(strategy: str, *, news_events_count: int = 0, funding_available: bool = False) -> str:
    family = _strategy_family(strategy)
    if family == "ai_glm":
        if news_events_count > 0 and funding_available:
            return "OHLCV + News + Macro"
        if news_events_count > 0:
            return "OHLCV + News"
        if funding_available:
            return "OHLCV + Macro"
        return "OHLCV only"
    if family == "ml":
        return "OHLCV only"
    return "OHLCV"


def _data_column_or_default(df: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if column in df.columns:
        return pd.to_numeric(df[column], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(default)
    return pd.Series(float(default), index=df.index, dtype=float)


def _clamp_series(series: pd.Series, lower: float = -1.0, upper: float = 1.0) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(lower, upper)


async def _attach_backtest_enrichment_if_needed(
    strategy: str,
    df: pd.DataFrame,
    symbol: str,
    start_time: Optional[datetime],
    end_time: Optional[datetime],
) -> pd.DataFrame:
    if df.empty:
        return df
    family = _strategy_family(strategy)
    news_events_count = 0
    funding_available = False
    out = df.copy()

    if family == "ai_glm":
        try:
            enrichment = await build_research_enrichment(
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
            )
            news_events_count = int(enrichment.get("events_count", 0) or 0)
            funding_available = bool(enrichment.get("funding_available", False))
            out = attach_research_enrichment(out, symbol, enrichment)
        except Exception:
            out = df.copy()

    out.attrs["news_events_count"] = int(news_events_count)
    out.attrs["funding_available"] = bool(funding_available)
    out.attrs["decision_engine"] = _strategy_decision_engine(strategy)
    out.attrs["strategy_family"] = family
    out.attrs["data_mode"] = _strategy_data_mode(
        strategy,
        news_events_count=int(news_events_count),
        funding_available=bool(funding_available),
    )
    return out


def _resolve_cost_rates(commission_rate: float, slippage_bps: float) -> tuple[float, float]:
    config = SimpleNamespace(
        fee_model="flat",
        commission_rate=max(0.0, float(commission_rate or 0.0)),
        slippage_model="flat",
        slippage=max(0.0, float(slippage_bps or 0.0)) / 10000.0,
    )
    return (
        float(resolve_fee_rate(config, role="taker")),
        float(resolve_slippage_rate(config)),
    )


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


def _drop_incomplete_last_bar(
    df: pd.DataFrame,
    timeframe: str,
    *,
    anchor_time: Optional[datetime] = None,
) -> pd.DataFrame:
    if df.empty:
        return df

    unit = str(timeframe or "1h")[-1]
    value = int(str(timeframe or "1h")[:-1] or 1)
    tf_seconds = {
        "s": value,
        "m": value * 60,
        "h": value * 3600,
        "d": value * 86400,
        "w": value * 7 * 86400,
        "M": value * 30 * 86400,
    }.get(unit, 3600)

    out = df.copy()
    out.index = pd.to_datetime(out.index)
    out = out[~out.index.duplicated(keep="last")].sort_index()
    if out.empty:
        return out

    anchor = pd.Timestamp(anchor_time or datetime.now())
    last_ts = pd.Timestamp(out.index[-1])
    if anchor.tzinfo is not None:
        anchor = anchor.tz_localize(None)
    if last_ts.tzinfo is not None:
        last_ts = last_ts.tz_localize(None)
    if anchor < last_ts + pd.Timedelta(seconds=max(1, int(tf_seconds))):
        return out.iloc[:-1].copy()
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


def _safe_positive_pct(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except Exception:
        return None
    if out <= 0:
        return None
    if out >= 1:
        return None
    return out


def _resolve_backtest_protective_settings(
    *,
    strategy: str,
    params: Optional[Dict[str, Any]] = None,
    use_stop_take: bool = False,
    stop_loss_pct: Optional[float] = None,
    take_profit_pct: Optional[float] = None,
) -> Dict[str, Any]:
    defaults = get_strategy_defaults(strategy)
    merged_params = dict(defaults or {})
    merged_params.update(params or {})

    # Explicit switch has top priority: when disabled, force no protective exits.
    if not bool(use_stop_take):
        merged_params.pop("stop_loss_pct", None)
        merged_params.pop("take_profit_pct", None)
        return {
            "enabled": False,
            "stop_loss_pct": None,
            "take_profit_pct": None,
            "params": merged_params,
        }

    sl = _safe_positive_pct(stop_loss_pct)
    tp = _safe_positive_pct(take_profit_pct)
    if sl is None:
        sl = _safe_positive_pct(merged_params.get("stop_loss_pct"))
    if tp is None:
        tp = _safe_positive_pct(merged_params.get("take_profit_pct"))
    if sl is None:
        sl = _safe_positive_pct(defaults.get("stop_loss_pct"))
    if tp is None:
        tp = _safe_positive_pct(defaults.get("take_profit_pct"))

    has_param_protection = (
        _safe_positive_pct(merged_params.get("stop_loss_pct")) is not None
        or _safe_positive_pct(merged_params.get("take_profit_pct")) is not None
    )
    enabled = bool(has_param_protection or sl is not None or tp is not None)
    if enabled:
        merged_params["stop_loss_pct"] = sl
        merged_params["take_profit_pct"] = tp
    return {
        "enabled": bool(enabled and (sl is not None or tp is not None)),
        "stop_loss_pct": sl,
        "take_profit_pct": tp,
        "params": merged_params,
    }
def _build_pct_candidates(center: Optional[float], *, floor: float, cap: float) -> List[float]:
    if center is None:
        center = max(floor, min(cap, (floor + cap) * 0.5))
    core = float(max(floor, min(cap, center)))
    coarse = [
        max(floor, min(cap, core * 0.8)),
        core,
        max(floor, min(cap, core * 1.6)),
    ]
    unique = sorted({round(float(v), 6) for v in coarse if float(v) > 0})
    # Keep stop/take candidate space intentionally coarse to avoid overfitting.
    if len(unique) > 2:
        return [unique[0], unique[-1]]
    return unique
def _augment_grid_with_stop_take(
    *,
    grid: Dict[str, List[Any]],
    strategy: str,
    use_stop_take: bool,
    stop_loss_pct: Optional[float],
    take_profit_pct: Optional[float],
) -> Dict[str, List[Any]]:
    out = {str(k): list(v) for k, v in dict(grid or {}).items()}
    if not use_stop_take:
        return out
    if strategy == "FamaFactorArbitrageStrategy":
        return out

    resolved = _resolve_backtest_protective_settings(
        strategy=strategy,
        params={},
        use_stop_take=True,
        stop_loss_pct=stop_loss_pct,
        take_profit_pct=take_profit_pct,
    )
    sl_center = _safe_positive_pct(resolved.get("stop_loss_pct")) or 0.03
    tp_center = _safe_positive_pct(resolved.get("take_profit_pct")) or 0.06
    out["stop_loss_pct"] = _build_pct_candidates(sl_center, floor=0.003, cap=0.20)
    out["take_profit_pct"] = _build_pct_candidates(tp_center, floor=0.005, cap=0.50)
    return out


def _timeframe_to_seconds(timeframe: str) -> int:
    text = str(timeframe or "").strip()
    if not text:
        return 3600
    try:
        value = max(1, int(text[:-1] or 1))
    except Exception:
        return 3600
    unit = text[-1]
    if unit == "s":
        return value
    if unit == "m":
        return value * 60
    if unit == "h":
        return value * 3600
    if unit == "d":
        return value * 86400
    if unit == "w":
        return value * 7 * 86400
    if unit == "M":
        return value * 30 * 86400
    return 3600


def _normalize_symbol(value: Any) -> str:
    text = str(value or "").strip().upper().replace("_", "/")
    if not text:
        return ""
    if "/" not in text:
        text = f"{text}/USDT"
    return text


def _default_fama_universe(anchor_symbol: str = "BTC/USDT") -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for item in [
        anchor_symbol,
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
        "MATIC/USDT",
        "LTC/USDT",
    ]:
        symbol = _normalize_symbol(item)
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
    return out


def _cross_sectional_zscore(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    mean = df.mean(axis=1)
    std = df.std(axis=1).replace(0, np.nan)
    out = df.sub(mean, axis=0).div(std, axis=0)
    return out.replace([np.inf, -np.inf], np.nan).fillna(0.0)


def _portfolio_trade_stats(returns: pd.Series, turnover: pd.Series) -> Dict[str, Any]:
    change_ts = [ts for ts, val in turnover.items() if float(val or 0.0) > 1e-12]
    if not change_ts:
        return {"entries": 0, "exits": 0, "completed": 0, "win_rate": 0.0}

    change_pos = [returns.index.get_loc(ts) for ts in change_ts]
    segment_returns: List[float] = []
    start_pos = 0
    for pos in change_pos[1:]:
        chunk = returns.iloc[start_pos : pos + 1]
        start_pos = pos + 1
        if not chunk.empty:
            segment_returns.append(float(np.expm1(np.log1p(chunk.clip(lower=-0.95)).sum())))

    tail = returns.iloc[start_pos:]
    if not tail.empty:
        segment_returns.append(float(np.expm1(np.log1p(tail.clip(lower=-0.95)).sum())))

    completed = len(segment_returns)
    wins = sum(1 for item in segment_returns if item > 0)
    win_rate = (wins / completed * 100.0) if completed else 0.0
    changes = len(change_ts)
    return {
        "entries": changes,
        "exits": changes,
        "completed": completed,
        "win_rate": round(win_rate, 2),
    }


def _build_fama_backtest_components(
    market_bundle: Dict[str, pd.DataFrame],
    timeframe: str,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    params = dict(params or {})
    if not market_bundle:
        raise HTTPException(status_code=400, detail="Fama 回测缺少横截面市场数据")

    normalized_bundle: Dict[str, pd.DataFrame] = {}
    for raw_symbol, frame in market_bundle.items():
        symbol = _normalize_symbol(raw_symbol)
        if not symbol or frame is None or frame.empty:
            continue
        df = frame.copy()
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        if not {"close", "volume"}.issubset(df.columns):
            continue
        normalized_bundle[symbol] = df

    if len(normalized_bundle) < 2:
        raise HTTPException(status_code=400, detail="Fama 回测至少需要 2 个可用标的")

    common_index = None
    for frame in normalized_bundle.values():
        idx = pd.Index(pd.to_datetime(frame.index))
        common_index = idx if common_index is None else common_index.intersection(idx)
    common_index = pd.Index(common_index) if common_index is not None else pd.Index([])
    if len(common_index) < _min_required_bars(timeframe):
        raise HTTPException(status_code=400, detail="Fama 回测有效公共样本不足")

    symbols = list(normalized_bundle.keys())
    close_df = pd.DataFrame({sym: normalized_bundle[sym].reindex(common_index)["close"] for sym in symbols}).ffill()
    volume_df = pd.DataFrame({sym: normalized_bundle[sym].reindex(common_index)["volume"] for sym in symbols}).fillna(0.0)
    high_df = pd.DataFrame({sym: normalized_bundle[sym].reindex(common_index)["high"] for sym in symbols}).ffill()
    low_df = pd.DataFrame({sym: normalized_bundle[sym].reindex(common_index)["low"] for sym in symbols}).ffill()

    close_df = close_df.dropna(axis=1, how="all").ffill().dropna(how="all")
    volume_df = volume_df.reindex(close_df.index).fillna(0.0)[close_df.columns]
    high_df = high_df.reindex(close_df.index).ffill()[close_df.columns]
    low_df = low_df.reindex(close_df.index).ffill()[close_df.columns]
    if close_df.shape[1] < 2 or close_df.empty:
        raise HTTPException(status_code=400, detail="Fama 回测有效标的不足")

    lookback_bars = max(60, int(params.get("lookback_bars", 240) or 240))
    quantile = max(0.05, min(0.45, float(params.get("quantile", 0.25) or 0.25)))
    min_abs_score = max(0.0, float(params.get("min_abs_score", params.get("alpha_threshold", 0.10)) or 0.0))
    top_n = max(1, int(params.get("top_n", 6) or 6))
    allow_long = bool(params.get("allow_long", True))
    allow_short = bool(params.get("allow_short", True))
    max_vol = max(0.0, float(params.get("max_vol", 0.0) or 0.0))
    max_spread = max(0.0, float(params.get("max_spread", 0.0) or 0.0))

    returns_df = close_df.pct_change().replace([np.inf, -np.inf], np.nan).fillna(0.0)
    momentum_window = max(4, min(lookback_bars // 4, max(6, len(close_df) // 3)))
    value_window = max(8, min(lookback_bars // 2, max(10, len(close_df) // 2)))
    quality_window = max(8, min(lookback_bars // 3, max(10, len(close_df) // 2)))
    vol_window = max(8, min(lookback_bars // 4, max(10, len(close_df) // 2)))
    liq_window = max(8, min(lookback_bars // 5, max(10, len(close_df) // 2)))
    ema_fast = max(3, momentum_window // 2)
    ema_slow = max(8, value_window // 2)

    momentum = close_df.pct_change(momentum_window)
    value = -(close_df / close_df.rolling(value_window, min_periods=max(4, value_window // 2)).mean() - 1.0)
    quality = returns_df.rolling(quality_window, min_periods=max(4, quality_window // 2)).mean() / (
        returns_df.rolling(quality_window, min_periods=max(4, quality_window // 2)).std().replace(0, np.nan)
    )
    low_vol = -returns_df.rolling(vol_window, min_periods=max(4, vol_window // 2)).std()
    trend = (
        close_df.ewm(span=ema_fast, adjust=False).mean()
        / close_df.ewm(span=ema_slow, adjust=False).mean().replace(0, np.nan)
    ) - 1.0
    liquidity = np.log((close_df * volume_df).rolling(liq_window, min_periods=max(4, liq_window // 2)).mean())

    score_df = (
        _cross_sectional_zscore(momentum) * 0.35
        + _cross_sectional_zscore(value) * 0.20
        + _cross_sectional_zscore(quality) * 0.20
        + _cross_sectional_zscore(low_vol) * 0.10
        + _cross_sectional_zscore(liquidity) * 0.05
        + _cross_sectional_zscore(trend) * 0.10
    ).replace([np.inf, -np.inf], np.nan)

    spread_proxy = ((high_df - low_df) / close_df.replace(0, np.nan)).rolling(
        max(4, vol_window // 2),
        min_periods=max(3, vol_window // 4),
    ).median()
    rolling_vol = returns_df.rolling(vol_window, min_periods=max(4, vol_window // 2)).std()

    interval_minutes = max(1, int(params.get("rebalance_interval_minutes", params.get("cooldown_min", 60)) or 60))
    rebalance_bars = max(1, int(round(interval_minutes * 60 / max(1, _timeframe_to_seconds(timeframe)))))
    eligible_min = max(2, int(np.ceil(close_df.shape[1] * quantile)))
    weights = pd.DataFrame(0.0, index=close_df.index, columns=close_df.columns)
    current = pd.Series(0.0, index=close_df.columns, dtype=float)
    warmup = max(momentum_window, value_window, quality_window, vol_window, liq_window)

    for idx, ts in enumerate(close_df.index):
        if idx < warmup:
            weights.iloc[idx] = current
            continue
        if idx % rebalance_bars != 0 and idx != len(close_df.index) - 1:
            weights.iloc[idx] = current
            continue

        score_row = score_df.loc[ts].dropna().sort_values(ascending=False)
        if score_row.empty:
            current = pd.Series(0.0, index=close_df.columns, dtype=float)
            weights.iloc[idx] = current
            continue

        if max_vol > 0:
            vol_row = rolling_vol.loc[ts].reindex(score_row.index)
            score_row = score_row[vol_row.fillna(max_vol + 1) <= max_vol]
        if max_spread > 0:
            spread_row = spread_proxy.loc[ts].reindex(score_row.index)
            score_row = score_row[spread_row.fillna(max_spread + 1) <= max_spread]
        if min_abs_score > 0:
            score_row = score_row[score_row.abs() >= min_abs_score]

        current = pd.Series(0.0, index=close_df.columns, dtype=float)
        if len(score_row) >= eligible_min:
            divisor = 2 if allow_long and allow_short else 1
            leg_n = min(top_n, max(1, len(score_row) // divisor))
            if allow_long:
                longs = list(score_row.head(leg_n).index)
                if longs:
                    current.loc[longs] = 0.5 / len(longs) if allow_short else 1.0 / len(longs)
            if allow_short:
                shorts = list(score_row.tail(leg_n).index)
                if shorts:
                    current.loc[shorts] = -(0.5 / len(shorts) if allow_long else 1.0 / len(shorts))
        weights.iloc[idx] = current

    turnover = weights.diff().abs().sum(axis=1).fillna(weights.abs().sum(axis=1))
    benchmark_symbol = _normalize_symbol(params.get("benchmark_symbol") or close_df.columns[0])
    if benchmark_symbol not in close_df.columns:
        benchmark_symbol = str(close_df.columns[0])

    return {
        "returns": returns_df,
        "weights": weights,
        "turnover": turnover,
        "benchmark_symbol": benchmark_symbol,
        "benchmark_close": close_df[benchmark_symbol].copy(),
        "universe_size": int(close_df.shape[1]),
        "quantile": quantile,
    }


def _pairs_hedge_ratio_bounds(params: Optional[Dict[str, Any]] = None) -> tuple[float, float]:
    cfg = dict(params or {})
    allow_negative = bool(cfg.get("allow_negative_hedge_ratio", True))
    min_hr = float(cfg.get("min_hedge_ratio", -5.0) or -5.0)
    max_hr = float(cfg.get("max_hedge_ratio", 5.0) or 5.0)
    if min_hr > max_hr:
        min_hr, max_hr = max_hr, min_hr
    if allow_negative:
        if min_hr >= 0 < max_hr:
            min_hr = -abs(max_hr)
    else:
        min_hr = max(0.0, min_hr)
        max_hr = max(max_hr, min_hr)
    return min_hr, max_hr


def _pairs_hedge_ratio_series(
    price1: pd.Series,
    price2: pd.Series,
    method: str = "ols",
) -> pd.Series:
    y = pd.to_numeric(price1, errors="coerce")
    x = pd.to_numeric(price2, errors="coerce")
    aligned = pd.DataFrame({"y": y, "x": x}).dropna()
    out = pd.Series(np.nan, index=price1.index, dtype=float)
    if aligned.empty:
        return out
    if str(method or "ols").strip().lower() != "ols":
        out.loc[aligned.index] = 1.0
        return out.ffill().fillna(1.0)

    sample_count = pd.Series(np.arange(1, len(aligned) + 1, dtype=float), index=aligned.index)
    cum_x = aligned["x"].cumsum()
    cum_y = aligned["y"].cumsum()
    cum_xy = (aligned["x"] * aligned["y"]).cumsum()
    cum_x2 = (aligned["x"] * aligned["x"]).cumsum()

    cov_num = cum_xy - (cum_x * cum_y) / sample_count
    var_num = cum_x2 - (cum_x * cum_x) / sample_count
    beta = cov_num / var_num.replace(0, np.nan)

    out.loc[aligned.index] = pd.to_numeric(beta, errors="coerce")
    return out.ffill().fillna(1.0)


def _pairs_signal_bias(current_z: float, entry_z: float, exit_z: float) -> str:
    if not np.isfinite(current_z):
        return "neutral"
    if current_z <= -abs(entry_z):
        return "long_spread_bias"
    if current_z >= abs(entry_z):
        return "short_spread_bias"
    if abs(current_z) <= abs(exit_z):
        return "exit_zone"
    return "neutral"


def _build_pairs_backtest_components(
    primary_df: pd.DataFrame,
    market_bundle: Dict[str, pd.DataFrame],
    timeframe: str,
    params: Optional[Dict[str, Any]] = None,
    *,
    use_stop_take: bool = False,
    stop_loss_pct: Optional[float] = None,
    take_profit_pct: Optional[float] = None,
) -> Dict[str, Any]:
    cfg = dict(params or {})
    normalized_bundle: Dict[str, pd.DataFrame] = {}
    for raw_symbol, frame in (market_bundle or {}).items():
        symbol = _normalize_symbol(raw_symbol)
        if not symbol or frame is None or frame.empty:
            continue
        item = frame.copy()
        item.index = pd.to_datetime(item.index)
        item = item[~item.index.duplicated(keep="last")].sort_index()
        if "close" not in item.columns:
            continue
        normalized_bundle[symbol] = item

    primary_symbol = _normalize_symbol(cfg.get("primary_symbol") or cfg.get("symbol"))
    pair_symbol = _normalize_symbol(cfg.get("pair_symbol"))
    if not primary_symbol:
        primary_candidates = [sym for sym in normalized_bundle.keys() if sym != pair_symbol]
        primary_symbol = primary_candidates[0] if primary_candidates else ""
    if not pair_symbol:
        pair_candidates = [sym for sym in normalized_bundle.keys() if sym != primary_symbol]
        pair_symbol = pair_candidates[0] if pair_candidates else ""

    if not pair_symbol:
        raise HTTPException(status_code=400, detail="PairsTradingStrategy 缺少 pair_symbol 参数")
    if not primary_symbol:
        primary_symbol = _normalize_symbol(primary_df.get("symbol").iloc[-1] if "symbol" in primary_df.columns and len(primary_df) else "")
    if not primary_symbol:
        raise HTTPException(status_code=400, detail="PairsTradingStrategy 缺少主腿历史数据")
    if primary_symbol == pair_symbol:
        raise HTTPException(status_code=400, detail="PairsTradingStrategy 的主腿和副腿不能相同")

    primary = normalized_bundle.get(primary_symbol)
    if primary is None or primary.empty:
        primary = primary_df.copy()
        primary.index = pd.to_datetime(primary.index)
        primary = primary[~primary.index.duplicated(keep="last")].sort_index()
    pair_df = normalized_bundle.get(pair_symbol)
    if pair_df is None or pair_df.empty:
        raise HTTPException(status_code=404, detail=f"PairsTradingStrategy 缺少副腿 {pair_symbol} 的历史数据")

    common_index = pd.Index(primary.index).intersection(pd.Index(pair_df.index))
    common_index = pd.Index(common_index).sort_values()
    min_bars = _min_required_bars(timeframe)
    if len(common_index) < min_bars:
        raise HTTPException(
            status_code=400,
            detail=f"PairsTradingStrategy 公共样本不足（主腿 {primary_symbol} / 副腿 {pair_symbol}，仅 {len(common_index)} 根）",
        )

    primary_close = pd.to_numeric(primary.reindex(common_index)["close"], errors="coerce")
    pair_close = pd.to_numeric(pair_df.reindex(common_index)["close"], errors="coerce")
    valid_mask = primary_close.notna() & pair_close.notna()
    primary_close = primary_close[valid_mask]
    pair_close = pair_close[valid_mask]
    common_index = primary_close.index
    if len(common_index) < min_bars:
        raise HTTPException(status_code=400, detail="PairsTradingStrategy 有效公共收盘价不足")

    lookback_period = max(10, int(cfg.get("lookback_period", 48) or 48))
    entry_z = abs(float(cfg.get("entry_z_score", 2.0) or 2.0))
    exit_z = abs(float(cfg.get("exit_z_score", 0.6) or 0.6))
    hedge_method = str(cfg.get("hedge_ratio_method") or "ols")
    min_hr, max_hr = _pairs_hedge_ratio_bounds(cfg)

    hedge_ratio = _pairs_hedge_ratio_series(primary_close, pair_close, method=hedge_method)
    hedge_ratio = pd.to_numeric(hedge_ratio, errors="coerce").clip(lower=min_hr, upper=max_hr).ffill().fillna(1.0)

    spread = primary_close - hedge_ratio * pair_close
    spread_mean = spread.rolling(lookback_period, min_periods=lookback_period).mean()
    spread_std = spread.rolling(lookback_period, min_periods=lookback_period).std().replace(0, np.nan)
    z_score = (spread - spread_mean) / spread_std

    spread_state = pd.Series(0.0, index=common_index, dtype=float)
    active_hr = pd.Series(0.0, index=common_index, dtype=float)
    trade_open_points: List[Dict[str, Any]] = []
    trade_close_points: List[Dict[str, Any]] = []
    buy_points: List[Dict[str, Any]] = []
    sell_points: List[Dict[str, Any]] = []

    sl_pct = _safe_positive_pct(stop_loss_pct) if bool(use_stop_take) else None
    tp_pct = _safe_positive_pct(take_profit_pct) if bool(use_stop_take) else None
    forced_stop = 0
    forced_take = 0
    entries = 0
    exits = 0
    completed = 0
    wins = 0

    state = 0.0
    trade_hr = 0.0
    entry_price1 = 0.0
    entry_price2 = 0.0
    entry_notional = 0.0

    for idx, ts in enumerate(common_index):
        px1 = float(primary_close.iloc[idx])
        px2 = float(pair_close.iloc[idx])
        current_hr = float(hedge_ratio.iloc[idx]) if np.isfinite(hedge_ratio.iloc[idx]) else trade_hr
        current_z = float(z_score.iloc[idx]) if np.isfinite(z_score.iloc[idx]) else float("nan")
        prev_z = float(z_score.iloc[idx - 1]) if idx > 0 and np.isfinite(z_score.iloc[idx - 1]) else float("nan")
        valid_signal = np.isfinite(current_z) and np.isfinite(prev_z)

        active_trade_return = None
        if state != 0 and np.isfinite(px1) and np.isfinite(px2) and entry_notional > 0:
            active_trade_return = (
                state * ((px1 - entry_price1) - trade_hr * (px2 - entry_price2)) / entry_notional
            )

        should_exit = False
        exit_reason = ""
        if state != 0:
            if sl_pct is not None and active_trade_return is not None and active_trade_return <= -sl_pct:
                should_exit = True
                exit_reason = "stop_loss"
                forced_stop += 1
            elif tp_pct is not None and active_trade_return is not None and active_trade_return >= tp_pct:
                should_exit = True
                exit_reason = "take_profit"
                forced_take += 1
            elif valid_signal and abs(current_z) <= exit_z < abs(prev_z):
                should_exit = True
                exit_reason = "mean_revert_exit"

        if should_exit:
            exits += 1
            completed += 1
            if active_trade_return is not None and active_trade_return > 0:
                wins += 1
            close_point = {
                "timestamp": pd.Timestamp(ts).isoformat(),
                "price": px1,
                "pair_price": px2,
                "spread": float(spread.loc[ts]) if np.isfinite(spread.loc[ts]) else None,
                "z_score": current_z if np.isfinite(current_z) else None,
                "direction": "long_spread" if state > 0 else "short_spread",
                "reason": exit_reason,
            }
            trade_close_points.append(close_point)
            if state > 0:
                sell_points.append({"timestamp": close_point["timestamp"], "price": px1})
            else:
                buy_points.append({"timestamp": close_point["timestamp"], "price": px1})
            state = 0.0
            trade_hr = 0.0
            entry_price1 = 0.0
            entry_price2 = 0.0
            entry_notional = 0.0

        elif state == 0 and valid_signal and prev_z > -entry_z >= current_z:
            proposed_notional = abs(px1) + abs(current_hr) * abs(px2)
            if np.isfinite(proposed_notional) and proposed_notional > 0:
                state = 1.0
                trade_hr = current_hr
                entry_price1 = px1
                entry_price2 = px2
                entry_notional = proposed_notional
                entries += 1
                open_point = {
                    "timestamp": pd.Timestamp(ts).isoformat(),
                    "price": px1,
                    "pair_price": px2,
                    "spread": float(spread.loc[ts]) if np.isfinite(spread.loc[ts]) else None,
                    "z_score": current_z if np.isfinite(current_z) else None,
                    "direction": "long_spread",
                }
                trade_open_points.append(open_point)
                buy_points.append({"timestamp": open_point["timestamp"], "price": px1})

        elif state == 0 and valid_signal and prev_z < entry_z <= current_z:
            proposed_notional = abs(px1) + abs(current_hr) * abs(px2)
            if np.isfinite(proposed_notional) and proposed_notional > 0:
                state = -1.0
                trade_hr = current_hr
                entry_price1 = px1
                entry_price2 = px2
                entry_notional = proposed_notional
                entries += 1
                open_point = {
                    "timestamp": pd.Timestamp(ts).isoformat(),
                    "price": px1,
                    "pair_price": px2,
                    "spread": float(spread.loc[ts]) if np.isfinite(spread.loc[ts]) else None,
                    "z_score": current_z if np.isfinite(current_z) else None,
                    "direction": "short_spread",
                }
                trade_open_points.append(open_point)
                sell_points.append({"timestamp": open_point["timestamp"], "price": px1})

        spread_state.iloc[idx] = state
        active_hr.iloc[idx] = trade_hr if state != 0 else 0.0

    prev_state = spread_state.shift(1).fillna(0.0)
    prev_hr = active_hr.shift(1).fillna(0.0)
    delta1 = primary_close.diff().fillna(0.0)
    delta2 = pair_close.diff().fillna(0.0)
    prev_gross_notional = (primary_close.shift(1).abs() + prev_hr.abs() * pair_close.shift(1).abs()).replace(0, np.nan)
    gross_returns = ((prev_state * delta1) - (prev_state * prev_hr * delta2)) / prev_gross_notional
    gross_returns = gross_returns.replace([np.inf, -np.inf], np.nan).fillna(0.0)

    current_gross_notional = (primary_close.abs() + active_hr.abs() * pair_close.abs()).replace(0, np.nan)
    weight1 = (spread_state * primary_close.abs() / current_gross_notional).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    weight2 = ((-spread_state * active_hr * pair_close.abs()) / current_gross_notional).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    turnover = weight1.diff().abs().fillna(weight1.abs()) + weight2.diff().abs().fillna(weight2.abs())

    clip_limit = _return_clip_limit(timeframe)
    anomaly_ratio = float((gross_returns.abs() > clip_limit).mean() or 0.0)
    win_rate = (wins / completed * 100.0) if completed else 0.0
    latest_hedge_ratio = float(hedge_ratio.iloc[-1]) if len(hedge_ratio) else 0.0
    latest_z = float(z_score.iloc[-1]) if len(z_score) and np.isfinite(z_score.iloc[-1]) else float("nan")

    return {
        "benchmark_close": primary_close.copy(),
        "primary_symbol": primary_symbol,
        "pair_symbol": pair_symbol,
        "primary_close": primary_close,
        "pair_close": pair_close,
        "spread": spread,
        "z_score": z_score,
        "hedge_ratio": hedge_ratio,
        "position": spread_state,
        "position_exposure": spread_state.abs(),
        "gross_returns": gross_returns,
        "turnover": turnover.fillna(0.0),
        "trade_stats": {
            "entries": int(entries),
            "exits": int(exits),
            "completed": int(completed),
            "win_rate": round(float(win_rate), 2),
        },
        "protective_stats": {
            "forced_stop_exits": int(forced_stop),
            "forced_take_exits": int(forced_take),
        },
        "trade_points": {
            "buy_points": buy_points,
            "sell_points": sell_points,
            "open_points": trade_open_points,
            "close_points": trade_close_points,
            "entries": int(entries),
            "exits": int(exits),
        },
        "effective_data_points": int(len(common_index)),
        "effective_start_date": pd.Timestamp(common_index[0]).isoformat(),
        "effective_end_date": pd.Timestamp(common_index[-1]).isoformat(),
        "pair_regime": "negative_corr" if latest_hedge_ratio < 0 else "positive_corr",
        "hedge_ratio_last": latest_hedge_ratio,
        "spread_last": float(spread.iloc[-1]) if len(spread) and np.isfinite(spread.iloc[-1]) else None,
        "z_score_last": latest_z if np.isfinite(latest_z) else None,
        "signal_bias": _pairs_signal_bias(latest_z, entry_z, exit_z),
        "return_clip_limit": float(clip_limit),
        "anomaly_ratio": round(float(anomaly_ratio), 6),
    }


def _optimize_strategy_on_df(
    strategy: str,
    df: pd.DataFrame,
    timeframe: str,
    initial_capital: float,
    commission_rate: float,
    slippage_bps: float,
    objective: str = "total_return",
    max_trials: int = 64,
    market_bundle: Optional[Dict[str, pd.DataFrame]] = None,
    use_stop_take: bool = False,
    stop_loss_pct: Optional[float] = None,
    take_profit_pct: Optional[float] = None,
) -> Dict[str, Any]:
    if strategy not in _BACKTEST_OPTIMIZATION_GRIDS:
        raise ValueError(f"暂不支持 {strategy} 参数优化")

    grid = _augment_grid_with_stop_take(
        grid=_BACKTEST_OPTIMIZATION_GRIDS[strategy],
        strategy=strategy,
        use_stop_take=bool(use_stop_take),
        stop_loss_pct=stop_loss_pct,
        take_profit_pct=take_profit_pct,
    )
    keys = list(grid.keys())
    values = [grid[k] for k in keys]
    limit = max(1, int(max_trials or 1))
    combo_iter = itertools.islice(itertools.product(*values), limit)

    objective_key = _normalize_optimize_objective(objective)
    trials: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []

    for combo in combo_iter:
        params = {keys[idx]: combo[idx] for idx in range(len(keys))}
        trial_stop_loss = _safe_positive_pct(params.get("stop_loss_pct"))
        trial_take_profit = _safe_positive_pct(params.get("take_profit_pct"))
        effective_stop_loss = trial_stop_loss if trial_stop_loss is not None else _safe_positive_pct(stop_loss_pct)
        effective_take_profit = trial_take_profit if trial_take_profit is not None else _safe_positive_pct(take_profit_pct)
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
                market_bundle=market_bundle,
                use_stop_take=bool(use_stop_take),
                stop_loss_pct=effective_stop_loss,
                take_profit_pct=effective_take_profit,
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
            "entry_signals": int(item.get("metrics", {}).get("entry_signals") or 0),
            "exit_signals": int(item.get("metrics", {}).get("exit_signals") or 0),
            "trade_points": int(item.get("metrics", {}).get("entry_signals") or 0)
            + int(item.get("metrics", {}).get("exit_signals") or 0),
            "zero_trade_reason": str(item.get("metrics", {}).get("zero_trade_reason") or ""),
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
        fast_n = int(params.get("fast_period", 20))
        slow_n = int(params.get("slow_period", 60))
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
    elif strategy in {"MeanReversionStrategy", "BollingerMeanReversionStrategy"}:
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
    elif strategy == "MomentumStrategy":
        lookback = int(params.get("lookback_period", 20))
        threshold = float(params.get("momentum_threshold", 0.015))

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
    elif strategy == "TrendFollowingStrategy":
        short_n = int(params.get("short_period", 20))
        long_n = int(params.get("long_period", 55))
        adx_threshold = float(params.get("adx_threshold", 23))

        short_ma = close.rolling(short_n, min_periods=short_n).mean()
        long_ma = close.rolling(long_n, min_periods=long_n).mean()

        # ADX calculation for trend strength filter
        high = df["high"]
        low = df["low"]
        adx_period = int(params.get("adx_period", 14))
        up_move = high.diff()
        down_move = -low.diff()
        plus_dm = pd.Series(np.where((up_move > down_move) & (up_move > 0), up_move, 0.0), index=df.index)
        minus_dm = pd.Series(np.where((down_move > up_move) & (down_move > 0), down_move, 0.0), index=df.index)
        tr = pd.concat([high - low, (high - close.shift(1)).abs(), (low - close.shift(1)).abs()], axis=1).max(axis=1)
        atr = tr.ewm(alpha=1 / adx_period, adjust=False).mean()
        plus_di = 100 * (plus_dm.ewm(alpha=1 / adx_period, adjust=False).mean() / atr.replace(0, np.nan))
        minus_di = 100 * (minus_dm.ewm(alpha=1 / adx_period, adjust=False).mean() / atr.replace(0, np.nan))
        dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)) * 100
        adx = dx.ewm(alpha=1 / adx_period, adjust=False).mean()

        in_position = False
        values = []
        for s, l, a in zip(short_ma.fillna(0), long_ma.fillna(0), adx.fillna(0)):
            if not in_position and s > l and a >= adx_threshold:
                in_position = True
            elif in_position and s < l:
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
        lookback = max(5, int(params.get("lookback_period", 7)))
        fear_th = float(params.get("fear_threshold", 25))
        greed_th = float(params.get("greed_threshold", 75))
        regime_window = max(lookback * 6, 24)
        regime_ret = close.pct_change(lookback).clip(-0.20, 0.20)
        mood = ((regime_ret - regime_ret.rolling(regime_window, min_periods=max(3, regime_window // 3)).min()) /
            (regime_ret.rolling(regime_window, min_periods=max(3, regime_window // 3)).max() -
             regime_ret.rolling(regime_window, min_periods=max(3, regime_window // 3)).min()).replace(0, np.nan) * 100.0
        ).clip(0.0, 100.0).fillna(50.0)
        news_sentiment = _clamp_series(_data_column_or_default(df, "news_sentiment_score"), -3.0, 3.0)
        macro_score = _clamp_series(_data_column_or_default(df, "news_macro_score"), -3.0, 3.0)
        funding_rate = _clamp_series(_data_column_or_default(df, "funding_rate"), -0.02, 0.02)
        sentiment_component = (
            50.0 + 28.0 * np.tanh(news_sentiment * 0.65 + macro_score * 0.90 - funding_rate * 180.0)
        ).clip(0.0, 100.0)
        fear_greed_score = (mood * 0.55 + sentiment_component * 0.45).clip(0.0, 100.0)
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
            raise HTTPException(status_code=400, detail="MLXGBoostStrategy 需要安装 xgboost")
        model_path = str(params.get("model_path", ""))
        if not model_path or not Path(model_path).exists():
            candidates = [
                Path(model_path) if model_path else None,
                Path("models/ml_signal_xgb.json"),
                Path(__file__).resolve().parents[2] / "models" / "ml_signal_xgb.json",
            ]
            model_path = next((str(p) for p in candidates if p and p.exists()), "")
        if not model_path:
            raise HTTPException(status_code=400, detail="MLXGBoostStrategy 模型文件不存在")
        model = xgb.Booster()
        model.load_model(model_path)
        feat_df = build_feature_frame(df)
        proba = model.predict(xgb.DMatrix(feat_df.values, feature_names=list(feat_df.columns)))
        proba = pd.Series(proba, index=df.index).clip(0.0, 1.0)
        threshold_ml = float(params.get("threshold", 0.55))
        position = (proba >= threshold_ml).astype(float)
    # ===== 新增因子策略回测逻辑 =====
    elif strategy == "AroonStrategy":
        period = int(params.get("period", 25))
        high = df["high"]
        low = df["low"]
        aroon_up = high.rolling(period + 1).apply(lambda x: (period - np.argmax(x)) / period * 100, raw=True)
        aroon_down = low.rolling(period + 1).apply(lambda x: (period - np.argmin(x)) / period * 100, raw=True)
        aroon = aroon_up - aroon_down
        buy_th = float(params.get("buy_threshold", 50))
        sell_th = float(params.get("sell_threshold", -50))
        in_position = False
        values = []
        for a in aroon.fillna(0):
            if not in_position and a >= buy_th:
                in_position = True
            elif in_position and a <= sell_th:
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)
    elif strategy == "ROCStrategy":
        period = int(params.get("period", 14))
        buy_th = float(params.get("buy_threshold", 5.0))
        sell_th = float(params.get("sell_threshold", -5.0))
        roc = (close / close.shift(period) - 1) * 100
        in_position = False
        values = []
        for r in roc.fillna(0):
            if not in_position and r >= buy_th:
                in_position = True
            elif in_position and r <= sell_th:
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)
    elif strategy == "PriceAccelerationStrategy":
        fast = int(params.get("fast", 5))
        slow = int(params.get("slow", 15))
        th = float(params.get("accel_threshold", 0.1))
        fast_mom = close.pct_change(fast)
        slow_mom = close.pct_change(slow)
        accel = (fast_mom - slow_mom) / slow_mom.abs().replace(0, np.nan)
        in_position = False
        values = []
        for a in accel.fillna(0):
            if not in_position and a >= th:
                in_position = True
            elif in_position and a <= -th:
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)
    elif strategy == "MFIStrategy":
        period = int(params.get("period", 14))
        oversold = float(params.get("oversold", 20))
        overbought = float(params.get("overbought", 80))
        high = df["high"]
        low = df["low"]
        vol = df["volume"]
        tp = (high + low + close) / 3
        mf = tp * vol
        pos_mf = mf.where(tp > tp.shift(1), 0)
        neg_mf = mf.where(tp < tp.shift(1), 0)
        pos_sum = pos_mf.rolling(period).sum()
        neg_sum = neg_mf.rolling(period).sum()
        mfi = 100 - (100 / (1 + pos_sum / neg_sum.replace(0, np.nan)))
        in_position = False
        values = []
        for m in mfi.fillna(50):
            if not in_position and m <= oversold:
                in_position = True
            elif in_position and m >= overbought:
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)
    elif strategy == "OBVStrategy":
        smooth = int(params.get("smooth", 20))
        div_th = float(params.get("divergence_threshold", 1.5))
        vol = df["volume"]
        direction = np.sign(close.diff())
        obv = (direction * vol).cumsum()
        obv_z = (obv - obv.rolling(smooth).mean()) / obv.rolling(smooth).std().replace(0, np.nan)
        in_position = False
        values = []
        price_falling = close < close.shift(5)
        price_rising = close > close.shift(5)
        for i, (oz, pf, pr) in enumerate(zip(obv_z.fillna(0), price_falling.fillna(False), price_rising.fillna(False))):
            if not in_position and pf and oz >= div_th:
                in_position = True
            elif in_position and pr and oz <= -div_th:
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)
    elif strategy == "VWAPStrategy":
        period = int(params.get("period", 20))
        buy_th = float(params.get("buy_threshold", -0.02))
        sell_th = float(params.get("sell_threshold", 0.02))
        high = df["high"]
        low = df["low"]
        vol = df["volume"]
        tp = (high + low + close) / 3
        vwap = (tp * vol).rolling(period).sum() / vol.rolling(period).sum().replace(0, np.nan)
        dev = (close - vwap) / vwap
        in_position = False
        values = []
        for d in dev.fillna(0):
            if not in_position and d <= buy_th:
                in_position = True
            elif in_position and d >= sell_th:
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)
    elif strategy == "TradeIntensityStrategy":
        fast = int(params.get("fast", 5))
        slow = int(params.get("slow", 20))
        th = float(params.get("intensity_threshold", 1.5)) - 1
        vol = df["volume"]
        fast_vol = vol.rolling(fast).mean()
        slow_vol = vol.rolling(slow).mean()
        intensity = fast_vol / slow_vol.replace(0, np.nan) - 1
        price_chg = close.pct_change(fast)
        in_position = False
        values = []
        for inten, pchg in zip(intensity.fillna(0), price_chg.fillna(0)):
            if not in_position and inten >= th and pchg > 0:
                in_position = True
            elif in_position and (inten >= th and pchg < 0):
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)
    elif strategy == "ParkinsonVolStrategy":
        period = int(params.get("period", 20))
        vol_low = float(params.get("vol_percentile_low", 20))
        vol_high = float(params.get("vol_percentile_high", 80))
        high = df["high"]
        low = df["low"]
        hl_log = np.log(high / low.replace(0, np.nan))
        variance = (hl_log ** 2) / (4 * np.log(2))
        park_vol = np.sqrt(variance.rolling(period).mean())
        vol_pct = park_vol.rolling(period * 2).rank(pct=True) * 100
        in_position = False
        values = []
        for vp in vol_pct.fillna(50):
            if not in_position and vp <= vol_low:
                in_position = True
            elif in_position and vp >= vol_high:
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)
    elif strategy == "UlcerIndexStrategy":
        period = int(params.get("period", 14))
        high_th = float(params.get("high_risk_threshold", 10))
        low_th = float(params.get("low_risk_threshold", 3))
        rolling_max = close.rolling(period).max()
        drawdown_pct = ((close - rolling_max) / rolling_max.replace(0, np.nan)) * 100
        ulcer = np.sqrt((drawdown_pct ** 2).rolling(period).mean())
        in_position = False
        values = []
        for u in ulcer.fillna(0):
            if not in_position and u <= low_th:
                in_position = True
            elif in_position and u >= high_th:
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)
    elif strategy == "VaRBreakoutStrategy":
        var_period = int(params.get("var_period", 20))
        conf = float(params.get("confidence", 0.95))
        mult = float(params.get("multiplier", 1.5))
        returns = close.pct_change()
        def calc_var(s):
            r = s.dropna()
            if len(r) < var_period // 2:
                return np.nan
            return np.percentile(r, (1 - conf) * 100)
        var = returns.rolling(var_period).apply(calc_var, raw=False)
        bar_ret = returns.fillna(0)
        in_position = False
        values = []
        for r, v in zip(bar_ret, var.fillna(0)):
            if not in_position and v != 0 and r < v * mult:
                in_position = True
            elif in_position and v != 0 and r > -v * mult:
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)
    elif strategy == "MaxDrawdownStrategy":
        lookback = int(params.get("lookback", 30))
        dd_th = float(params.get("dd_threshold", -0.10))
        recovery_th = float(params.get("recovery_threshold", 0.3))
        rolling_max = close.rolling(lookback).max()
        rolling_min = close.rolling(lookback).min()
        drawdown = (close - rolling_max) / rolling_max.replace(0, np.nan)
        recovery = (close - rolling_min) / (rolling_max - rolling_min).replace(0, np.nan)
        prev_dd = drawdown.shift(1)
        in_position = False
        values = []
        for dd, prev, rec, price in zip(drawdown.fillna(0), prev_dd.fillna(0), recovery.fillna(0), close):
            if not in_position and prev <= dd_th and rec > recovery_th and price > close.shift(1).fillna(price):
                in_position = True
            elif in_position and rec >= 0.8:
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)
    elif strategy == "SortinoRatioStrategy":
        period = int(params.get("period", 30))
        th = float(params.get("sortino_threshold", 1.0))
        returns = close.pct_change()
        def calc_sortino(s):
            r = s.dropna()
            if len(r) < period // 2:
                return np.nan
            mean_ret = r.mean()
            downside = r[r < 0]
            if len(downside) < 2:
                return np.nan
            downside_std = np.sqrt((downside ** 2).mean())
            return mean_ret / downside_std if downside_std > 0 else np.nan
        sortino = returns.rolling(period).apply(calc_sortino, raw=False)
        in_position = False
        values = []
        for s in sortino.fillna(0):
            if not in_position and s >= th:
                in_position = True
            elif in_position and s <= -th:
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)
    elif strategy == "HurstExponentStrategy":
        hurst_period = int(params.get("hurst_period", 100))
        zscore_period = int(params.get("zscore_period", 20))
        z_th = float(params.get("zscore_threshold", 1.5))
        returns = close.pct_change()
        # Simplified Hurst proxy using variance ratio
        def calc_vr(s):
            r = s.dropna()
            if len(r) < 20:
                return np.nan
            var_1 = np.var(r)
            lag_ret = r[::5]
            if len(lag_ret) < 5:
                return np.nan
            var_lag = np.var(lag_ret) * 5
            return var_lag / var_1 if var_1 > 0 else np.nan
        vr = returns.rolling(hurst_period).apply(calc_vr, raw=False)
        mean = close.rolling(zscore_period).mean()
        std = close.rolling(zscore_period).std().replace(0, np.nan)
        zscore = (close - mean) / std
        in_position = False
        values = []
        for v, z in zip(vr.fillna(1), zscore.fillna(0)):
            # VR > 1.1 = trending, use momentum
            if v > 1.1:
                if not in_position and z >= z_th:
                    in_position = True
                elif in_position and z <= -z_th:
                    in_position = False
            # VR < 0.9 = mean reverting
            elif v < 0.9:
                if not in_position and z <= -z_th:
                    in_position = True
                elif in_position and z >= z_th:
                    in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)
    elif strategy == "MeanReversionHalfLifeStrategy":
        lookback = int(params.get("lookback", 60))
        z_entry = float(params.get("zscore_entry", 2.0))
        mean = close.rolling(lookback).mean()
        std = close.rolling(lookback).std().replace(0, np.nan)
        zscore = (close - mean) / std
        in_position = False
        values = []
        for z in zscore.fillna(0):
            if not in_position and z <= -z_entry:
                in_position = True
            elif in_position and z >= z_entry:
                in_position = False
            values.append(1.0 if in_position else 0.0)
        position = pd.Series(values, index=df.index)
    elif strategy == "OrderFlowImbalanceStrategy":
        period = int(params.get("period", 10))
        imbal_th = float(params.get("imbalance_threshold", 1.0))
        high = df["high"]
        low = df["low"]
        vol = df["volume"]
        mid = (high + low) / 2
        rng = (high - low).replace(0, np.nan)
        imbalance = ((close - mid) / rng * vol).fillna(0)
        cum_imbal = imbalance.rolling(period).sum()
        ofi_z = (cum_imbal - cum_imbal.rolling(period).mean()) / cum_imbal.rolling(period).std().replace(0, np.nan)
        in_position = False
        values = []
        for oz in ofi_z.fillna(0):
            if not in_position and oz >= imbal_th:
                in_position = True
            elif in_position and oz <= -imbal_th:
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


def _apply_protective_position_rules(
    df: pd.DataFrame,
    position: pd.Series,
    *,
    stop_loss_pct: Optional[float],
    take_profit_pct: Optional[float],
) -> tuple[pd.Series, Dict[str, int]]:
    effective = position.fillna(0.0).astype(float).clip(lower=0.0, upper=1.0).copy()
    if effective.empty:
        return effective, {"forced_stop_exits": 0, "forced_take_exits": 0}

    sl_pct = _safe_positive_pct(stop_loss_pct)
    tp_pct = _safe_positive_pct(take_profit_pct)
    if sl_pct is None and tp_pct is None:
        return effective, {"forced_stop_exits": 0, "forced_take_exits": 0}

    close = pd.to_numeric(df.get("close"), errors="coerce")
    high = pd.to_numeric(df.get("high", close), errors="coerce")
    low = pd.to_numeric(df.get("low", close), errors="coerce")

    in_position = False
    entry_price = 0.0
    forced_stop = 0
    forced_take = 0

    for idx, ts in enumerate(effective.index):
        desired = bool(float(effective.iloc[idx]) > 0.0)
        px = float(close.loc[ts]) if pd.notna(close.loc[ts]) else float("nan")
        hi = float(high.loc[ts]) if pd.notna(high.loc[ts]) else px
        lo = float(low.loc[ts]) if pd.notna(low.loc[ts]) else px

        if (not np.isfinite(px)) or px <= 0:
            effective.iloc[idx] = 1.0 if in_position else 0.0
            continue

        if (not in_position) and desired:
            in_position = True
            entry_price = px
            effective.iloc[idx] = 1.0
            continue

        if not in_position:
            effective.iloc[idx] = 0.0
            continue

        hit_stop = False
        hit_take = False
        if sl_pct is not None:
            stop_price = entry_price * (1.0 - sl_pct)
            if np.isfinite(lo) and lo <= stop_price:
                hit_stop = True
        if tp_pct is not None:
            take_price = entry_price * (1.0 + tp_pct)
            if np.isfinite(hi) and hi >= take_price:
                hit_take = True

        if hit_stop or hit_take:
            if hit_stop:
                forced_stop += 1
            else:
                forced_take += 1
            in_position = False
            entry_price = 0.0
            effective.iloc[idx] = 0.0
            continue

        if not desired:
            in_position = False
            entry_price = 0.0
            effective.iloc[idx] = 0.0
            continue

        effective.iloc[idx] = 1.0

    return effective, {"forced_stop_exits": int(forced_stop), "forced_take_exits": int(forced_take)}


def _strategy_recommended_min_bars(
    strategy: str,
    timeframe: str,
    params: Optional[Dict[str, Any]] = None,
) -> int:
    base_min = _min_required_bars(timeframe)
    merged = dict(get_strategy_defaults(strategy) or {})
    merged.update(params or {})

    period_like_keys = (
        "period",
        "lookback",
        "window",
        "fast",
        "slow",
        "signal",
        "short",
        "long",
        "exit_lookback",
        "adx_period",
        "vol_window",
    )
    ignore_keys = (
        "threshold",
        "oversold",
        "overbought",
        "num_std",
        "zscore",
        "entry_z",
        "exit_z",
        "quantile",
        "top_n",
        "min_abs_score",
        "max_spread",
        "max_vol",
    )

    lookbacks: List[int] = []
    for key, raw_value in merged.items():
        key_text = str(key or "").strip().lower()
        if not key_text or any(token in key_text for token in ignore_keys):
            continue
        if not any(token in key_text for token in period_like_keys):
            continue
        try:
            value = int(float(raw_value))
        except Exception:
            continue
        if value > 1:
            lookbacks.append(value)

    max_lookback = max(lookbacks) if lookbacks else base_min
    return max(base_min, min(5000, max_lookback * 4))


def _diagnose_zero_trade_reason(
    strategy: str,
    df: pd.DataFrame,
    position: pd.Series,
    trade_stats: Dict[str, Any],
    timeframe: str,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    recommended_min_bars = _strategy_recommended_min_bars(strategy, timeframe, params=params)
    sample_size = int(len(df))
    if sample_size < recommended_min_bars:
        return f"当前样本仅 {sample_size} 根，低于建议的 {recommended_min_bars} 根"

    entries = int(trade_stats.get("entries") or 0)
    exits = int(trade_stats.get("exits") or 0)
    completed = int(trade_stats.get("completed") or 0)
    last_pos = float(position.iloc[-1]) if len(position) else 0.0
    active_ratio = float((position > 0).mean()) if len(position) else 0.0

    if entries == 0 and exits == 0:
        if active_ratio >= 0.98:
            return "策略几乎全程持仓，未出现完整平仓，无法形成闭环交易"
        return "当前区间未触发入场条件，阈值可能过严或行情不匹配"

    if completed == 0 and entries > 0:
        if last_pos > 0:
            return "触发了入场，但直到区间结束仍未满足平仓条件"
        return "出现零散入场/出场信号，但未配对成完整交易"

    return ""


def _run_backtest_core(
    strategy: str,
    df: pd.DataFrame,
    timeframe: str,
    initial_capital: float,
    params: Optional[Dict[str, Any]] = None,
    include_series: bool = False,
    commission_rate: float = 0.0004,
    slippage_bps: float = 2.0,
    market_bundle: Optional[Dict[str, pd.DataFrame]] = None,
    use_stop_take: bool = False,
    stop_loss_pct: Optional[float] = None,
    take_profit_pct: Optional[float] = None,
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

    resolved_protection = _resolve_backtest_protective_settings(
        strategy=strategy,
        params=params,
        use_stop_take=bool(use_stop_take),
        stop_loss_pct=stop_loss_pct,
        take_profit_pct=take_profit_pct,
    )
    merged_params = dict(resolved_protection.get("params") or {})
    protective_enabled = bool(resolved_protection.get("enabled"))
    resolved_stop_loss_pct = _safe_positive_pct(resolved_protection.get("stop_loss_pct"))
    resolved_take_profit_pct = _safe_positive_pct(resolved_protection.get("take_profit_pct"))

    benchmark_close = df["close"]
    protective_stats = {"forced_stop_exits": 0, "forced_take_exits": 0}
    position_for_diagnostics = pd.Series(0.0, index=df.index, dtype=float)
    pair_components: Optional[Dict[str, Any]] = None
    if strategy == "FamaFactorArbitrageStrategy":
        components = _build_fama_backtest_components(
            market_bundle=market_bundle or {},
            timeframe=timeframe,
            params=merged_params,
        )
        asset_returns = components["returns"]
        weights = components["weights"]
        turnover = components["turnover"]
        benchmark_close = components["benchmark_close"]
        returns, anomaly_ratio, clip_limit = _safe_bar_returns(benchmark_close, timeframe)
        gross_returns = (weights.shift(1).fillna(0.0) * asset_returns).sum(axis=1)
        gross_returns = gross_returns.reindex(benchmark_close.index).fillna(0.0)
        exposure = weights.abs().sum(axis=1).reindex(benchmark_close.index).fillna(0.0)
        position = exposure
        position_for_diagnostics = exposure
        trade_stats = _portfolio_trade_stats(gross_returns, turnover.reindex(benchmark_close.index).fillna(0.0))
        if protective_enabled:
            protective_enabled = False
    elif strategy == "PairsTradingStrategy":
        pair_components = _build_pairs_backtest_components(
            primary_df=df,
            market_bundle=market_bundle or {},
            timeframe=timeframe,
            params=merged_params,
            use_stop_take=bool(protective_enabled),
            stop_loss_pct=resolved_stop_loss_pct,
            take_profit_pct=resolved_take_profit_pct,
        )
        benchmark_close = pair_components["benchmark_close"]
        gross_returns = pd.to_numeric(pair_components["gross_returns"], errors="coerce").fillna(0.0)
        anomaly_ratio = float(pair_components.get("anomaly_ratio", 0.0) or 0.0)
        clip_limit = float(pair_components.get("return_clip_limit", _return_clip_limit(timeframe)) or _return_clip_limit(timeframe))
        position = pair_components["position"]
        position_for_diagnostics = pair_components["position_exposure"]
        turnover = pair_components["turnover"].reindex(benchmark_close.index).fillna(0.0)
        trade_stats = dict(pair_components.get("trade_stats") or {"entries": 0, "exits": 0, "completed": 0, "win_rate": 0.0})
        protective_stats = dict(pair_components.get("protective_stats") or protective_stats)
    else:
        raw_position = _build_positions(strategy, df, params=merged_params)
        if protective_enabled:
            position, protective_stats = _apply_protective_position_rules(
                df,
                raw_position,
                stop_loss_pct=resolved_stop_loss_pct,
                take_profit_pct=resolved_take_profit_pct,
            )
        else:
            position = raw_position
        returns, anomaly_ratio, clip_limit = _safe_bar_returns(df["close"], timeframe)
        gross_returns = position.shift(1).fillna(0.0) * returns

        turnover = position.diff().abs().fillna(0.0)
        if len(position) > 0:
            turnover.iloc[0] = abs(float(position.iloc[0] or 0.0))
        trade_stats = _trade_stats(df["close"], position)
        position_for_diagnostics = position.clip(lower=0.0, upper=1.0)

    if not protective_enabled:
        resolved_stop_loss_pct = None
        resolved_take_profit_pct = None

    fee_rate, slip_rate = _resolve_cost_rates(commission_rate=commission_rate, slippage_bps=slippage_bps)
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
        "recommended_min_bars": int(_strategy_recommended_min_bars(strategy, timeframe, params=merged_params)),
        "zero_trade_reason": "",
        "use_stop_take": bool(protective_enabled),
        "stop_loss_pct": resolved_stop_loss_pct,
        "take_profit_pct": resolved_take_profit_pct,
        "forced_stop_exits": int(protective_stats.get("forced_stop_exits") or 0),
        "forced_take_exits": int(protective_stats.get("forced_take_exits") or 0),
        "forced_protective_exits": int(
            int(protective_stats.get("forced_stop_exits") or 0)
            + int(protective_stats.get("forced_take_exits") or 0)
        ),
        "news_events_count": int(df.attrs.get("news_events_count", 0) or 0),
        "funding_available": bool(df.attrs.get("funding_available", False)),
        "data_mode": str(
            df.attrs.get("data_mode")
            or _strategy_data_mode(
                strategy,
                news_events_count=int(df.attrs.get("news_events_count", 0) or 0),
                funding_available=bool(df.attrs.get("funding_available", False)),
            )
        ),
        "decision_engine": str(df.attrs.get("decision_engine") or _strategy_decision_engine(strategy)),
        "strategy_family": str(df.attrs.get("strategy_family") or _strategy_family(strategy)),
    }
    result["common_pnl"] = build_common_pnl_summary(
        source="web_quick_backtest",
        unit="pct_return",
        gross_pnl=result["gross_total_return"],
        fee=result["estimated_trade_cost_pct"],
        slippage_cost=None,
        funding_pnl=0.0,
        net_pnl=result["total_return"],
        turnover=None,
        trade_count=result["total_trades"],
        win_rate=result["win_rate"],
        cost_model_version="web_api_backtest_v1",
        metadata={
            "timeframe": timeframe,
            "strategy": strategy,
            "decision_engine": result["decision_engine"],
            "funding_available": bool(result["funding_available"]),
            **({"pair_symbol": result.get("pair_symbol")} if result.get("pair_symbol") else {}),
        },
    )
    if int(trade_stats.get("completed") or 0) == 0:
        result["zero_trade_reason"] = _diagnose_zero_trade_reason(
            strategy=strategy,
            df=df,
            position=position_for_diagnostics,
            trade_stats=trade_stats,
            timeframe=timeframe,
            params=merged_params,
        )
    if strategy == "FamaFactorArbitrageStrategy":
        result["portfolio_mode"] = "cross_sectional_long_short"
        result["benchmark_symbol"] = components.get("benchmark_symbol")
        result["universe_size"] = int(components.get("universe_size") or 0)
        result["quantile"] = float(components.get("quantile") or 0.0)
    elif strategy == "PairsTradingStrategy" and pair_components:
        result.update(
            {
                "portfolio_mode": "pairs_spread_dual_leg",
                "pair_symbol": str(pair_components.get("pair_symbol") or ""),
                "pair_regime": str(pair_components.get("pair_regime") or "positive_corr"),
                "signal_bias": str(pair_components.get("signal_bias") or "neutral"),
                "pair_data_points": int(pair_components.get("effective_data_points") or 0),
                "effective_data_points": int(pair_components.get("effective_data_points") or 0),
                "effective_start_date": pair_components.get("effective_start_date"),
                "effective_end_date": pair_components.get("effective_end_date"),
                "hedge_ratio_last": round(float(pair_components.get("hedge_ratio_last") or 0.0), 6),
                "spread_last": (
                    round(float(pair_components.get("spread_last")), 8)
                    if pair_components.get("spread_last") is not None
                    else None
                ),
                "z_score_last": (
                    round(float(pair_components.get("z_score_last")), 4)
                    if pair_components.get("z_score_last") is not None
                    else None
                ),
            }
        )
        result["common_pnl"]["metadata"]["pair_symbol"] = str(pair_components.get("pair_symbol") or "")

    if include_series:
        points = (
            {"buy_points": [], "sell_points": [], "entries": trade_stats["entries"], "exits": trade_stats["exits"]}
            if strategy == "FamaFactorArbitrageStrategy"
            else (pair_components.get("trade_points") if strategy == "PairsTradingStrategy" and pair_components else _extract_trade_points(df["close"], position))
        )

        # Downsample for frontend payload size.
        max_points = 1800
        series_payload: Dict[str, Any] = {
            "timestamp": benchmark_close.index,
            "equity": equity.values,
            "gross_equity": gross_equity.values,
            "drawdown": (drawdown.fillna(0) * 100).values,
            "position": position.values,
            "close": benchmark_close.values,
            "cost": trade_cost.values,
        }
        if strategy == "PairsTradingStrategy" and pair_components:
            series_payload["pair_close"] = pair_components["pair_close"].reindex(benchmark_close.index).values
            series_payload["spread"] = pair_components["spread"].reindex(benchmark_close.index).values
            series_payload["z_score"] = pair_components["z_score"].reindex(benchmark_close.index).values
            series_payload["hedge_ratio"] = pair_components["hedge_ratio"].reindex(benchmark_close.index).values
        series_df = pd.DataFrame(series_payload)
        if len(series_df) > max_points:
            step = int(np.ceil(len(series_df) / max_points))
            series_df = series_df.iloc[::step]

        rendered_series = []
        for _, row in series_df.iterrows():
            item = {
                "timestamp": pd.Timestamp(row["timestamp"]).isoformat(),
                "equity": round(float(row["equity"]), 4),
                "drawdown": round(float(row["drawdown"]), 4),
                "position": float(row["position"]),
                "close": round(float(row["close"]), 8),
                "gross_equity": round(float(row["gross_equity"]), 4),
                "cost": round(float(row["cost"]), 8),
            }
            if strategy == "PairsTradingStrategy" and pair_components:
                pair_close = pd.to_numeric(pd.Series([row.get("pair_close")]), errors="coerce").iloc[0]
                spread_value = pd.to_numeric(pd.Series([row.get("spread")]), errors="coerce").iloc[0]
                z_value = pd.to_numeric(pd.Series([row.get("z_score")]), errors="coerce").iloc[0]
                hedge_value = pd.to_numeric(pd.Series([row.get("hedge_ratio")]), errors="coerce").iloc[0]
                item["pair_close"] = round(float(pair_close), 8) if np.isfinite(pair_close) else None
                item["spread"] = round(float(spread_value), 8) if np.isfinite(spread_value) else None
                item["z_score"] = round(float(z_value), 6) if np.isfinite(z_value) else None
                item["hedge_ratio"] = round(float(hedge_value), 6) if np.isfinite(hedge_value) else None
            rendered_series.append(item)

        result["series"] = rendered_series
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
        return _drop_incomplete_last_bar(best.sort_index(), tf, anchor_time=end_dt)

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
        best = _drop_incomplete_last_bar(best, tf, anchor_time=end_dt)
    return best


async def _load_fama_market_bundle(
    symbol: str,
    timeframe: str,
    params: Optional[Dict[str, Any]] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> Dict[str, pd.DataFrame]:
    params = dict(params or {})
    requested = _normalize_symbol(symbol) or "BTC/USDT"
    universe_raw = list(params.get("universe_symbols") or _default_fama_universe(requested))
    universe: List[str] = []
    seen: set[str] = set()
    max_symbols = max(4, int(params.get("max_symbols", 20) or 20))
    for item in [requested, *universe_raw]:
        sym = _normalize_symbol(item)
        if not sym or sym in seen:
            continue
        seen.add(sym)
        universe.append(sym)
    universe = universe[:max_symbols]

    bundle: Dict[str, pd.DataFrame] = {}
    min_rows = max(_min_required_bars(timeframe), min(300, max(60, int(params.get("min_symbol_bars", 120) or 120))))
    loaded_frames = await asyncio.gather(
        *[
            _load_backtest_df(sym, timeframe, start_time=start_time, end_time=end_time)
            for sym in universe
        ]
    )
    for sym, df in zip(universe, loaded_frames):
        if df.empty or len(df) < min_rows:
            continue
        bundle[sym] = df.copy()
    return bundle


async def _load_backtest_inputs(
    strategy: str,
    symbol: str,
    timeframe: str,
    params: Optional[Dict[str, Any]] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> tuple[pd.DataFrame, Optional[Dict[str, pd.DataFrame]], str]:
    merged_params = dict(get_strategy_defaults(strategy) or {})
    merged_params.update(params or {})
    if strategy == "FamaFactorArbitrageStrategy":
        bundle = await _load_fama_market_bundle(
            symbol=symbol,
            timeframe=timeframe,
            params=merged_params,
            start_time=start_time,
            end_time=end_time,
        )
        if not bundle:
            return pd.DataFrame(), None, _normalize_symbol(symbol) or "BTC/USDT"
        resolved_symbol = _normalize_symbol(symbol) or next(iter(bundle.keys()))
        if resolved_symbol not in bundle:
            resolved_symbol = next(iter(bundle.keys()))
        return bundle[resolved_symbol].copy(), bundle, resolved_symbol

    if strategy == "PairsTradingStrategy":
        resolved_symbol = _normalize_symbol(symbol) or "BTC/USDT"
        pair_symbol = _normalize_symbol(merged_params.get("pair_symbol"))
        if not pair_symbol:
            raise HTTPException(status_code=400, detail="PairsTradingStrategy 缺少 pair_symbol 参数")
        primary_df, pair_df = await asyncio.gather(
            _load_backtest_df(resolved_symbol, timeframe, start_time=start_time, end_time=end_time),
            _load_backtest_df(pair_symbol, timeframe, start_time=start_time, end_time=end_time),
        )
        bundle: Dict[str, pd.DataFrame] = {}
        if not primary_df.empty:
            bundle[resolved_symbol] = primary_df.copy()
        if not pair_df.empty:
            bundle[pair_symbol] = pair_df.copy()
        return primary_df, bundle or None, resolved_symbol

    df = await _load_backtest_df(symbol, timeframe, start_time=start_time, end_time=end_time)
    return df, None, _normalize_symbol(symbol) or symbol


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
    use_stop_take: bool = False,
    stop_loss_pct: Optional[float] = None,
    take_profit_pct: Optional[float] = None,
):
    parsed_start = _parse_backtest_bound(start_date, bound="start_date")
    parsed_end = _parse_backtest_bound(end_date, bound="end_date")

    df, market_bundle, resolved_symbol = await _load_backtest_inputs(
        strategy=strategy,
        symbol=symbol,
        timeframe=timeframe,
        start_time=parsed_start.to_pydatetime() if parsed_start is not None else None,
        end_time=parsed_end.to_pydatetime() if parsed_end is not None else None,
    )
    df = await _attach_backtest_enrichment_if_needed(
        strategy=strategy,
        df=df,
        symbol=resolved_symbol,
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
        market_bundle=market_bundle,
        params=get_strategy_defaults(strategy),
        use_stop_take=bool(use_stop_take),
        stop_loss_pct=stop_loss_pct,
        take_profit_pct=take_profit_pct,
    )

    result.update(
        {
            "strategy": strategy,
            "symbol": resolved_symbol,
            "timeframe": timeframe,
            "initial_capital": initial_capital,
            "commission_rate": max(0.0, float(commission_rate or 0.0)),
            "slippage_bps": max(0.0, float(slippage_bps or 0.0)),
            "data_points": int(result.get("effective_data_points") or len(df)),
            "start_date": str(result.get("effective_start_date") or df.index[0].isoformat()),
            "end_date": str(result.get("effective_end_date") or df.index[-1].isoformat()),
            "auto_expanded_range": auto_expanded_range,
            "min_required_bars": min_bars,
            "use_stop_take": bool(result.get("use_stop_take", False)),
            "stop_loss_pct": result.get("stop_loss_pct"),
            "take_profit_pct": result.get("take_profit_pct"),
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
        "WhaleActivityStrategy,FamaFactorArbitrageStrategy"
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
    optimize_max_trials: int = _BACKTEST_COMPARE_DEFAULT_TRIALS,
    use_stop_take: bool = False,
    stop_loss_pct: Optional[float] = None,
    take_profit_pct: Optional[float] = None,
):
    strategy_list = [s.strip() for s in strategies.split(",") if s.strip()]
    if not strategy_list:
        raise HTTPException(status_code=400, detail="至少需要一个策略")

    parsed_start = _parse_backtest_bound(start_date, bound="start_date")
    parsed_end = _parse_backtest_bound(end_date, bound="end_date")

    common_df = await _load_backtest_df(
        symbol,
        timeframe,
        start_time=parsed_start.to_pydatetime() if parsed_start is not None else None,
        end_time=parsed_end.to_pydatetime() if parsed_end is not None else None,
    )
    if common_df.empty:
        raise HTTPException(status_code=404, detail="缺少历史数据")
    if parsed_start is not None:
        common_df = common_df[common_df.index >= parsed_start]
    if parsed_end is not None:
        common_df = common_df[common_df.index <= parsed_end]
    if common_df.empty:
        raise HTTPException(status_code=404, detail="该时间范围内无可用数据")
    min_bars = _min_required_bars(timeframe)
    if len(common_df) < min_bars:
        raise HTTPException(
            status_code=400,
            detail=f"该时间范围内K线不足（{len(common_df)} 根），{timeframe} 至少需要 {min_bars} 根",
        )

    results = []
    for strategy in strategy_list:
        try:
            loop_df = common_df
            loop_bundle = None
            resolved_loop_symbol = _normalize_symbol(symbol) or symbol
            if strategy in {"FamaFactorArbitrageStrategy", "PairsTradingStrategy"}:
                loop_df, loop_bundle, resolved_loop_symbol = await _load_backtest_inputs(
                    strategy=strategy,
                    symbol=symbol,
                    timeframe=timeframe,
                    params=get_strategy_defaults(strategy),
                    start_time=parsed_start.to_pydatetime() if parsed_start is not None else None,
                    end_time=parsed_end.to_pydatetime() if parsed_end is not None else None,
                )
                if loop_df.empty and strategy == "FamaFactorArbitrageStrategy":
                    raise HTTPException(status_code=404, detail="Fama 回测缺少可用横截面数据")
            loop_df = await _attach_backtest_enrichment_if_needed(
                strategy=strategy,
                df=loop_df,
                symbol=resolved_loop_symbol,
                start_time=parsed_start.to_pydatetime() if parsed_start is not None else None,
                end_time=parsed_end.to_pydatetime() if parsed_end is not None else None,
            )
            if pre_optimize and strategy in _BACKTEST_OPTIMIZATION_GRIDS:
                opt = _optimize_strategy_on_df(
                    strategy=strategy,
                    df=loop_df,
                    timeframe=timeframe,
                    initial_capital=initial_capital,
                    commission_rate=max(0.0, float(commission_rate or 0.0)),
                    slippage_bps=max(0.0, float(slippage_bps or 0.0)),
                    objective=optimize_objective,
                    max_trials=max(
                        1,
                        min(
                            int(optimize_max_trials or _BACKTEST_COMPARE_DEFAULT_TRIALS),
                            _BACKTEST_COMPARE_MAX_TRIALS,
                        ),
                    ),
                    market_bundle=loop_bundle,
                    use_stop_take=bool(use_stop_take),
                    stop_loss_pct=stop_loss_pct,
                    take_profit_pct=take_profit_pct,
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
                            "news_events_count": int(loop_df.attrs.get("news_events_count", 0) or 0),
                            "funding_available": bool(loop_df.attrs.get("funding_available", False)),
                            "data_mode": str(loop_df.attrs.get("data_mode") or _strategy_data_mode(strategy)),
                            "decision_engine": str(loop_df.attrs.get("decision_engine") or _strategy_decision_engine(strategy)),
                            "strategy_family": str(loop_df.attrs.get("strategy_family") or _strategy_family(strategy)),
                        }
                    )
                else:
                    metrics = _run_backtest_core(
                        strategy=strategy,
                        df=loop_df,
                        timeframe=timeframe,
                        initial_capital=initial_capital,
                        include_series=False,
                        commission_rate=max(0.0, float(commission_rate or 0.0)),
                        slippage_bps=max(0.0, float(slippage_bps or 0.0)),
                        market_bundle=loop_bundle,
                        params=get_strategy_defaults(strategy),
                        use_stop_take=bool(use_stop_take),
                        stop_loss_pct=stop_loss_pct,
                        take_profit_pct=take_profit_pct,
                    )
                    metrics.update(
                        {
                            "strategy": strategy,
                            "optimization_applied": False,
                            "optimization_reason": "优化无有效结果，已回退默认参数",
                            "optimization_trials": int(opt.get("trials") or 0),
                            "optimization_failed_trials": int(opt.get("failed_trials") or 0),
                            "optimization_objective": opt.get("objective"),
                            "news_events_count": int(loop_df.attrs.get("news_events_count", 0) or 0),
                            "funding_available": bool(loop_df.attrs.get("funding_available", False)),
                            "data_mode": str(loop_df.attrs.get("data_mode") or _strategy_data_mode(strategy)),
                            "decision_engine": str(loop_df.attrs.get("decision_engine") or _strategy_decision_engine(strategy)),
                            "strategy_family": str(loop_df.attrs.get("strategy_family") or _strategy_family(strategy)),
                        }
                    )
            else:
                metrics = _run_backtest_core(
                    strategy=strategy,
                    df=loop_df,
                    timeframe=timeframe,
                    initial_capital=initial_capital,
                    include_series=False,
                    commission_rate=max(0.0, float(commission_rate or 0.0)),
                    slippage_bps=max(0.0, float(slippage_bps or 0.0)),
                    market_bundle=loop_bundle,
                    params=get_strategy_defaults(strategy),
                    use_stop_take=bool(use_stop_take),
                    stop_loss_pct=stop_loss_pct,
                    take_profit_pct=take_profit_pct,
                )
                metrics.update(
                    {
                        "strategy": strategy,
                        "optimization_applied": False,
                        "optimization_reason": (
                            "未启用预优化" if not pre_optimize else "该策略暂不支持参数优化"
                        ),
                        "news_events_count": int(loop_df.attrs.get("news_events_count", 0) or 0),
                        "funding_available": bool(loop_df.attrs.get("funding_available", False)),
                        "data_mode": str(loop_df.attrs.get("data_mode") or _strategy_data_mode(strategy)),
                        "decision_engine": str(loop_df.attrs.get("decision_engine") or _strategy_decision_engine(strategy)),
                        "strategy_family": str(loop_df.attrs.get("strategy_family") or _strategy_family(strategy)),
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
        "data_points": int(len(common_df)),
        "start_date": common_df.index[0].isoformat(),
        "end_date": common_df.index[-1].isoformat(),
        "pre_optimize": bool(pre_optimize),
        "optimize_objective": _normalize_optimize_objective(optimize_objective),
        "optimize_max_trials": max(
            1,
            min(
                int(optimize_max_trials or _BACKTEST_COMPARE_DEFAULT_TRIALS),
                _BACKTEST_COMPARE_MAX_TRIALS,
            ),
        ),
        "use_stop_take": bool(use_stop_take),
        "stop_loss_pct": _safe_positive_pct(stop_loss_pct),
        "take_profit_pct": _safe_positive_pct(take_profit_pct),
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
    use_stop_take: bool = False,
    stop_loss_pct: Optional[float] = None,
    take_profit_pct: Optional[float] = None,
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

    df, market_bundle, resolved_symbol = await _load_backtest_inputs(
        strategy=strategy,
        symbol=symbol,
        timeframe=timeframe,
        params=custom_params,
        start_time=parsed_start.to_pydatetime() if parsed_start is not None else None,
        end_time=parsed_end.to_pydatetime() if parsed_end is not None else None,
    )
    df = await _attach_backtest_enrichment_if_needed(
        strategy=strategy,
        df=df,
        symbol=resolved_symbol,
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
        market_bundle=market_bundle,
        use_stop_take=bool(use_stop_take),
        stop_loss_pct=stop_loss_pct,
        take_profit_pct=take_profit_pct,
    )
    result.update(
        {
            "strategy": strategy,
            "symbol": resolved_symbol,
            "timeframe": timeframe,
            "initial_capital": initial_capital,
            "commission_rate": max(0.0, float(commission_rate or 0.0)),
            "slippage_bps": max(0.0, float(slippage_bps or 0.0)),
            "data_points": int(result.get("effective_data_points") or len(df)),
            "start_date": str(result.get("effective_start_date") or df.index[0].isoformat()),
            "end_date": str(result.get("effective_end_date") or df.index[-1].isoformat()),
            "custom_params": custom_params or {},
            "use_stop_take": bool(result.get("use_stop_take", False)),
            "stop_loss_pct": result.get("stop_loss_pct"),
            "take_profit_pct": result.get("take_profit_pct"),
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
    max_trials: int = _BACKTEST_OPTIMIZE_DEFAULT_TRIALS,
    include_all_trials: bool = True,
    use_stop_take: bool = False,
    stop_loss_pct: Optional[float] = None,
    take_profit_pct: Optional[float] = None,
):
    parsed_start = _parse_backtest_bound(start_date, bound="start_date")
    parsed_end = _parse_backtest_bound(end_date, bound="end_date")

    df, market_bundle, resolved_symbol = await _load_backtest_inputs(
        strategy=strategy,
        symbol=symbol,
        timeframe=timeframe,
        start_time=parsed_start.to_pydatetime() if parsed_start is not None else None,
        end_time=parsed_end.to_pydatetime() if parsed_end is not None else None,
    )
    df = await _attach_backtest_enrichment_if_needed(
        strategy=strategy,
        df=df,
        symbol=resolved_symbol,
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
        resolved_max_trials = max(
            1,
            min(
                int(max_trials or _BACKTEST_OPTIMIZE_DEFAULT_TRIALS),
                _BACKTEST_OPTIMIZE_MAX_TRIALS,
            ),
        )
        opt_result = _optimize_strategy_on_df(
            strategy=strategy,
            df=df,
            timeframe=timeframe,
            initial_capital=initial_capital,
            commission_rate=max(0.0, float(commission_rate or 0.0)),
            slippage_bps=max(0.0, float(slippage_bps or 0.0)),
            objective=objective,
            max_trials=resolved_max_trials,
            market_bundle=market_bundle,
            use_stop_take=bool(use_stop_take),
            stop_loss_pct=stop_loss_pct,
            take_profit_pct=take_profit_pct,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return {
        "strategy": strategy,
        "symbol": resolved_symbol,
        "timeframe": timeframe,
        "requested_start_date": start_date,
        "requested_end_date": end_date,
        "data_points": int(len(df)),
        "start_date": df.index[0].isoformat(),
        "end_date": df.index[-1].isoformat(),
        "objective": opt_result.get("objective"),
        "commission_rate": max(0.0, float(commission_rate or 0.0)),
        "slippage_bps": max(0.0, float(slippage_bps or 0.0)),
        "use_stop_take": bool(use_stop_take),
        "stop_loss_pct": _safe_positive_pct(stop_loss_pct),
        "take_profit_pct": _safe_positive_pct(take_profit_pct),
        "news_events_count": int(df.attrs.get("news_events_count", 0) or 0),
        "funding_available": bool(df.attrs.get("funding_available", False)),
        "data_mode": str(df.attrs.get("data_mode") or _strategy_data_mode(strategy)),
        "decision_engine": str(df.attrs.get("decision_engine") or _strategy_decision_engine(strategy)),
        "strategy_family": str(df.attrs.get("strategy_family") or _strategy_family(strategy)),
        "trials": int(opt_result.get("trials") or 0),
        "max_trials": int(resolved_max_trials),
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
    use_stop_take: bool = False,
    stop_loss_pct: Optional[float] = None,
    take_profit_pct: Optional[float] = None,
):
    parsed_start = _parse_backtest_bound(start_date, bound="start_date")
    parsed_end = _parse_backtest_bound(end_date, bound="end_date")
    df, market_bundle, resolved_symbol = await _load_backtest_inputs(
        strategy=strategy,
        symbol=symbol,
        timeframe=timeframe,
        start_time=parsed_start.to_pydatetime() if parsed_start is not None else None,
        end_time=parsed_end.to_pydatetime() if parsed_end is not None else None,
    )
    df = await _attach_backtest_enrichment_if_needed(
        strategy=strategy,
        df=df,
        symbol=resolved_symbol,
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
        market_bundle=market_bundle,
        params=get_strategy_defaults(strategy),
        use_stop_take=bool(use_stop_take),
        stop_loss_pct=stop_loss_pct,
        take_profit_pct=take_profit_pct,
    )

    summary_df = pd.DataFrame(
        [
            {
                "strategy": strategy,
                "symbol": resolved_symbol,
                "pair_symbol": result.get("pair_symbol"),
                "portfolio_mode": result.get("portfolio_mode"),
                "timeframe": timeframe,
                "initial_capital": initial_capital,
                "commission_rate": max(0.0, float(commission_rate or 0.0)),
                "slippage_bps": max(0.0, float(slippage_bps or 0.0)),
                "requested_start_date": start_date,
                "requested_end_date": end_date,
                "start_date": result.get("effective_start_date") or (df.index[0].isoformat() if len(df) else None),
                "end_date": result.get("effective_end_date") or (df.index[-1].isoformat() if len(df) else None),
                "data_points": result.get("effective_data_points") or int(len(df)),
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
                "use_stop_take": bool(result.get("use_stop_take", False)),
                "stop_loss_pct": result.get("stop_loss_pct"),
                "take_profit_pct": result.get("take_profit_pct"),
                "forced_stop_exits": result.get("forced_stop_exits"),
                "forced_take_exits": result.get("forced_take_exits"),
            }
        ]
    )

    series_df = pd.DataFrame(result.get("series", []))
    format_lower = (format or "xlsx").lower()

    if format_lower == "csv":
        content = summary_df.to_csv(index=False).encode("utf-8-sig")
        filename = f"backtest_{strategy}_{resolved_symbol.replace('/', '_')}_{timeframe}.csv"
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
        filename = f"backtest_{strategy}_{resolved_symbol.replace('/', '_')}_{timeframe}.pdf"
        media_type = "application/pdf"
    else:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            summary_df.to_excel(writer, index=False, sheet_name="summary")
            if not series_df.empty:
                series_df.to_excel(writer, index=False, sheet_name="series")
        content = output.getvalue()
        filename = f"backtest_{strategy}_{resolved_symbol.replace('/', '_')}_{timeframe}.xlsx"
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

