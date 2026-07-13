"""Rolling benchmark beta helpers for symbol-scoped market context."""
from __future__ import annotations

from functools import lru_cache
import math
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from config.settings import settings
from core.data.path_utils import candidate_symbol_dirs


def _normalize_symbol(value: Any) -> str:
    text = str(value or "").strip().upper().replace("-", "/").replace(" ", "")
    if not text:
        return ""
    if "/" in text:
        return text
    for quote in ("USDT", "USDC", "FDUSD", "BUSD", "USD"):
        if text.endswith(quote) and len(text) > len(quote):
            return f"{text[:-len(quote)]}/{quote}"
    return text


def _normalize_frame_index(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    if "timestamp" in out.columns:
        out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce")
        out = out.set_index("timestamp")
    out.index = pd.to_datetime(out.index, errors="coerce")
    if getattr(out.index, "tz", None) is not None:
        out.index = out.index.tz_convert("UTC").tz_localize(None)
    out = out[~out.index.isna()]
    return out.sort_index()


def _read_parquet_tail(path: Path, *, tail_rows: int) -> pd.DataFrame:
    try:
        df = pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()
    df = _normalize_frame_index(df)
    if df.empty or "close" not in df.columns:
        return pd.DataFrame()
    return df[["close"]].tail(max(1, int(tail_rows)))


def _beta_and_corr(asset_returns: pd.Series, benchmark_returns: pd.Series) -> Optional[tuple[float, float]]:
    pairs: List[tuple[float, float]] = []
    for raw_asset, raw_benchmark in zip(asset_returns.tolist(), benchmark_returns.tolist()):
        try:
            asset = float(raw_asset)
            benchmark = float(raw_benchmark)
        except (TypeError, ValueError):
            continue
        if math.isfinite(asset) and math.isfinite(benchmark):
            pairs.append((asset, benchmark))
    if len(pairs) < 2:
        return None

    mean_asset = sum(asset for asset, _benchmark in pairs) / len(pairs)
    mean_benchmark = sum(benchmark for _asset, benchmark in pairs) / len(pairs)
    centered = [(asset - mean_asset, benchmark - mean_benchmark) for asset, benchmark in pairs]
    asset_ss = sum(asset * asset for asset, _benchmark in centered)
    benchmark_ss = sum(benchmark * benchmark for _asset, benchmark in centered)
    if benchmark_ss <= 1e-12:
        return None

    cross = sum(asset * benchmark for asset, benchmark in centered)
    beta = cross / benchmark_ss
    corr = 0.0 if asset_ss <= 1e-18 else cross / math.sqrt(asset_ss * benchmark_ss)
    return beta, corr


@lru_cache(maxsize=512)
def _load_close_series_cached(
    storage_root: str,
    exchange: str,
    symbol: str,
    timeframe: str,
    lookback: int,
) -> pd.Series:
    root = Path(storage_root)
    frames: List[pd.DataFrame] = []
    tail_rows = max(20, int(lookback) + 8)
    for symbol_root in candidate_symbol_dirs(root, exchange, symbol):
        single = symbol_root / f"{timeframe}.parquet"
        if single.exists():
            frame = _read_parquet_tail(single, tail_rows=tail_rows)
            if not frame.empty:
                frames.append(frame)
        parts_dir = symbol_root / f"{timeframe}_parts"
        if parts_dir.exists():
            for part_path in sorted(parts_dir.glob("*.parquet"))[-12:]:
                frame = _read_parquet_tail(part_path, tail_rows=tail_rows)
                if not frame.empty:
                    frames.append(frame)
    if not frames:
        return pd.Series(dtype=float)
    df = pd.concat(frames).sort_index()
    df = df[~df.index.duplicated(keep="last")]
    series = pd.to_numeric(df["close"], errors="coerce").dropna()
    return series.tail(tail_rows)


def resolve_benchmark_beta(
    *,
    exchange: str = "binance",
    symbol: str,
    benchmark_symbol: str = "BTC/USDT",
    timeframe: str = "1h",
    lookback: int = 240,
) -> Dict[str, Any]:
    """Compute rolling beta of *symbol* returns versus a benchmark from local klines."""
    symbol_text = _normalize_symbol(symbol)
    benchmark_text = _normalize_symbol(benchmark_symbol or "BTC/USDT")
    exchange_text = str(exchange or "binance").strip().lower() or "binance"
    timeframe_text = str(timeframe or "1h").strip() or "1h"
    if not symbol_text or not benchmark_text:
        return {"available": False, "beta": None, "reason": "missing_symbol"}
    if symbol_text == benchmark_text:
        return {
            "available": True,
            "beta": 1.0,
            "correlation": 1.0,
            "sample_size": int(max(1, min(int(lookback or 1), 240))),
            "source": "identity",
            "benchmark_symbol": benchmark_text,
        }

    storage_root = str(Path(settings.DATA_STORAGE_PATH))
    asset_close = _load_close_series_cached(storage_root, exchange_text, symbol_text, timeframe_text, int(lookback or 240))
    benchmark_close = _load_close_series_cached(
        storage_root,
        exchange_text,
        benchmark_text,
        timeframe_text,
        int(lookback or 240),
    )
    if asset_close.empty or benchmark_close.empty:
        return {
            "available": False,
            "beta": None,
            "reason": "missing_local_klines",
            "benchmark_symbol": benchmark_text,
        }

    joined = pd.concat(
        [
            asset_close.pct_change().rename("asset"),
            benchmark_close.pct_change().rename("benchmark"),
        ],
        axis=1,
        join="inner",
    ).dropna()
    joined = joined.tail(max(20, int(lookback or 240)))
    if len(joined) < 20:
        return {
            "available": False,
            "beta": None,
            "reason": "insufficient_overlap",
            "sample_size": int(len(joined)),
            "benchmark_symbol": benchmark_text,
        }

    stats = _beta_and_corr(joined["asset"], joined["benchmark"])
    if stats is None:
        return {
            "available": False,
            "beta": None,
            "reason": "benchmark_variance_too_low",
            "sample_size": int(len(joined)),
            "benchmark_symbol": benchmark_text,
        }
    beta, corr = stats
    return {
        "available": True,
        "beta": round(max(-3.0, min(3.0, beta)), 6),
        "correlation": round(max(-1.0, min(1.0, corr)), 6),
        "sample_size": int(len(joined)),
        "source": "local_rolling_returns",
        "benchmark_symbol": benchmark_text,
        "timeframe": timeframe_text,
    }


def clear_benchmark_beta_cache() -> None:
    _load_close_series_cached.cache_clear()
