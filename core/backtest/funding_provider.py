"""Funding rate provider for backtest/research (local cache + Binance HTTP fetch).

Incremental design:
- Does not alter live execution path
- Can preload from local parquet/csv
- Can fetch Binance USDT-M funding history via public REST and cache locally
- Can attach an aligned ``funding_rate`` column to a OHLCV DataFrame for backtests
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd
import requests
from loguru import logger


def _norm_symbol(symbol: str) -> str:
    s = str(symbol or "").strip().upper()
    if s.endswith("USDT") and "/" not in s and len(s) > 4:
        s = f"{s[:-4]}/USDT"
    if s in {"MATIC/USDT", "MATICUSDT"}:
        return "POL/USDT"
    return s


def _raw_binance_symbol(symbol: str) -> str:
    return _norm_symbol(symbol).replace("/", "")


def _to_naive_ts(v: datetime | pd.Timestamp | str) -> pd.Timestamp:
    t = pd.Timestamp(v)
    if t.tzinfo is not None:
        t = t.tz_convert("UTC").tz_localize(None)
    return t


@dataclass
class FundingProviderConfig:
    source: str = "local"  # local | binance_http | auto
    exchange: str = "binance"
    timeframe: str = "8h"
    market_type: str = "swap"
    cache_dir: str = "data/funding"
    http_timeout: float = 20.0
    default_rate: float = 0.0
    max_http_limit: int = 1000


class FundingRateProvider:
    """Local/remote funding history provider with per-symbol in-memory cache."""

    def __init__(self, config: Optional[FundingProviderConfig] = None):
        self.config = config or FundingProviderConfig()
        self._cache: Dict[str, pd.Series] = {}

    @property
    def cache_dir(self) -> Path:
        p = Path(self.config.cache_dir)
        p.mkdir(parents=True, exist_ok=True)
        return p

    def _cache_path(self, symbol: str, exchange: Optional[str] = None) -> Path:
        ex = str(exchange or self.config.exchange or "binance").lower()
        sym = _norm_symbol(symbol).replace("/", "_")
        return self.cache_dir / ex / f"{sym}_funding.parquet"

    def _set_series(self, symbol: str, s: pd.Series) -> None:
        if s is None:
            return
        out = pd.Series(pd.to_numeric(s, errors="coerce").values, index=pd.to_datetime(s.index))
        out = out[~out.index.duplicated(keep="last")].sort_index().dropna()
        # Project convention is naive local/UTC-ish timestamps in parquet. Keep naive UTC here.
        if isinstance(out.index, pd.DatetimeIndex) and out.index.tz is not None:
            out.index = out.index.tz_convert("UTC").tz_localize(None)
        self._cache[_norm_symbol(symbol)] = out.astype(float)

    def load_from_parquet(self, symbol: str, path: str | Path) -> None:
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(p)
        df = pd.read_parquet(p)
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df = df.set_index("timestamp")
        if "funding_rate" not in df.columns:
            raise ValueError(f"{p} missing funding_rate column")
        self._set_series(symbol, pd.to_numeric(df["funding_rate"], errors="coerce"))

    def load_from_csv(self, symbol: str, path: str | Path) -> None:
        p = Path(path)
        df = pd.read_csv(p)
        if "timestamp" not in df.columns or "funding_rate" not in df.columns:
            raise ValueError("CSV must contain timestamp,funding_rate")
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        self._set_series(symbol, pd.Series(pd.to_numeric(df["funding_rate"], errors="coerce").values, index=df["timestamp"]))

    def load_local_cache(self, symbol: str, exchange: Optional[str] = None, *, required: bool = False) -> pd.Series:
        p = self._cache_path(symbol, exchange=exchange)
        if not p.exists():
            if required:
                raise FileNotFoundError(p)
            return pd.Series(dtype=float)
        self.load_from_parquet(symbol, p)
        return self.get_series(symbol)

    def save_local_cache(self, symbol: str, exchange: Optional[str] = None) -> Path:
        sym = _norm_symbol(symbol)
        s = self._cache.get(sym)
        if s is None or s.empty:
            raise ValueError(f"No funding cache for {sym}")
        p = self._cache_path(sym, exchange=exchange)
        p.parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame({"timestamp": pd.to_datetime(s.index), "funding_rate": pd.to_numeric(s.values, errors="coerce")})
        df.to_parquet(p, index=False)
        return p

    def merge_series(self, symbol: str, s: pd.Series, exchange: Optional[str] = None, save: bool = True) -> pd.Series:
        sym = _norm_symbol(symbol)
        cur = self.get_series(sym)
        if cur.empty:
            merged = s.copy()
        else:
            merged = pd.concat([cur, s])
            merged = merged[~merged.index.duplicated(keep="last")].sort_index()
        self._set_series(sym, merged)
        if save:
            self.save_local_cache(sym, exchange=exchange)
        return self.get_series(sym)

    def fetch_binance_http_history(
        self,
        symbol: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        *,
        limit: Optional[int] = None,
    ) -> pd.Series:
        """Fetch funding history from Binance USDT-M public endpoint.

        Endpoint: /fapi/v1/fundingRate
        Returns sparse series indexed by funding settlement timestamp.
        """
        raw_symbol = _raw_binance_symbol(symbol)
        end_dt = end_time or datetime.now(timezone.utc)
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=timezone.utc)
        start_dt = start_time or (end_dt - timedelta(days=90))
        if start_dt.tzinfo is None:
            start_dt = start_dt.replace(tzinfo=timezone.utc)

        max_limit = max(1, min(int(limit or self.config.max_http_limit), 1000))
        url = "https://fapi.binance.com/fapi/v1/fundingRate"
        cursor_ms = int(start_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)
        rows: List[dict] = []
        sess = requests.Session()
        sess.headers.update({"User-Agent": "crypto_trading_system/funding-provider"})

        while cursor_ms <= end_ms:
            resp = sess.get(
                url,
                params={
                    "symbol": raw_symbol,
                    "startTime": cursor_ms,
                    "endTime": end_ms,
                    "limit": max_limit,
                },
                timeout=float(self.config.http_timeout),
            )
            resp.raise_for_status()
            batch = resp.json() or []
            if not isinstance(batch, list) or not batch:
                break
            rows.extend(batch)
            last_ms = int(batch[-1].get("fundingTime", 0) or 0)
            if last_ms <= cursor_ms:
                break
            cursor_ms = last_ms + 1
            if len(batch) < max_limit:
                break

        if not rows:
            return pd.Series(dtype=float)

        ts = []
        vals = []
        for r in rows:
            try:
                ft = pd.Timestamp(int(r["fundingTime"]), unit="ms", tz="UTC").tz_localize(None)
                fr = float(r["fundingRate"])
                ts.append(ft)
                vals.append(fr)
            except Exception:
                continue
        if not ts:
            return pd.Series(dtype=float)
        s = pd.Series(vals, index=pd.to_datetime(ts), dtype=float)
        s = s[~s.index.duplicated(keep="last")].sort_index()
        return s

    def ensure_history(
        self,
        symbol: str,
        *,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        source: Optional[str] = None,
        save: bool = True,
    ) -> pd.Series:
        """Ensure local/in-memory funding history covers requested window.

        For ``source in {'local', 'auto'}``, local cache is loaded if present.
        For ``source in {'binance_http', 'auto'}``, missing tail/head is fetched and merged.
        """
        sym = _norm_symbol(symbol)
        src = str(source or self.config.source or "local").lower()

        cur = self.get_series(sym)
        if cur.empty and src in {"local", "auto", "binance_http"}:
            try:
                cur = self.load_local_cache(sym)
            except Exception as e:
                logger.debug(f"load local funding cache failed {sym}: {e}")
                cur = pd.Series(dtype=float)

        if src == "local":
            return self.get_series(sym, start_time=start_time, end_time=end_time)

        if src not in {"auto", "binance_http"}:
            return self.get_series(sym, start_time=start_time, end_time=end_time)

        start_dt = start_time
        end_dt = end_time
        now_utc = datetime.now(timezone.utc)
        if end_dt is None:
            end_dt = now_utc
        if start_dt is None:
            start_dt = (pd.Timestamp(end_dt) - pd.Timedelta(days=90)).to_pydatetime()

        need_fetch = cur.empty
        if not need_fetch and start_dt is not None:
            need_fetch = _to_naive_ts(cur.index.min()) > _to_naive_ts(start_dt) + pd.Timedelta(hours=12)
        if not need_fetch and end_dt is not None:
            need_fetch = _to_naive_ts(cur.index.max()) < _to_naive_ts(end_dt) - pd.Timedelta(hours=12)

        if need_fetch:
            try:
                fetched = self.fetch_binance_http_history(sym, start_time=start_dt, end_time=end_dt)
                if not fetched.empty:
                    self.merge_series(sym, fetched, save=save)
            except Exception as e:
                logger.warning(f"fetch funding history failed {sym}: {e}")

        return self.get_series(sym, start_time=start_dt, end_time=end_dt)

    def get_rate(self, symbol: str, timestamp: datetime) -> float:
        s = self._cache.get(_norm_symbol(symbol))
        if s is None or s.empty:
            return float(self.config.default_rate)
        ts = _to_naive_ts(timestamp)
        idx = s.index[s.index <= ts]
        if len(idx) == 0:
            return float(self.config.default_rate)
        v = pd.to_numeric(pd.Series([s.loc[idx[-1]]]), errors="coerce").iloc[0]
        return float(0.0 if pd.isna(v) else v)

    def get_series(
        self,
        symbol: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> pd.Series:
        s = self._cache.get(_norm_symbol(symbol))
        if s is None:
            return pd.Series(dtype=float)
        out = s
        if start_time is not None:
            out = out[out.index >= _to_naive_ts(start_time)]
        if end_time is not None:
            out = out[out.index <= _to_naive_ts(end_time)]
        return out.copy()

    def align_to_index(
        self,
        symbol: str,
        index: Iterable,
        *,
        fill_forward: bool = True,
        default_rate: Optional[float] = None,
    ) -> pd.Series:
        idx = pd.to_datetime(pd.Index(index))
        if isinstance(idx, pd.DatetimeIndex) and idx.tz is not None:
            idx = idx.tz_convert("UTC").tz_localize(None)
        s = self._cache.get(_norm_symbol(symbol))
        if s is None or s.empty:
            rate = float(self.config.default_rate if default_rate is None else default_rate)
            return pd.Series(rate, index=idx, dtype=float)
        base = s.copy()
        base.index = pd.to_datetime(base.index)
        if isinstance(base.index, pd.DatetimeIndex) and base.index.tz is not None:
            base.index = base.index.tz_convert("UTC").tz_localize(None)

        if fill_forward:
            out = base.reindex(idx, method="ffill")
        else:
            out = base.reindex(idx)
        fill_val = float(self.config.default_rate if default_rate is None else default_rate)
        out = pd.to_numeric(out, errors="coerce").fillna(fill_val).astype(float)
        return out

    def attach_to_ohlcv_df(
        self,
        df: pd.DataFrame,
        *,
        symbol: str,
        column: str = "funding_rate",
        fill_forward: bool = True,
        default_rate: Optional[float] = None,
        overwrite: bool = False,
    ) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        out = df.copy()
        if column in out.columns and not overwrite:
            return out
        out[column] = self.align_to_index(symbol=symbol, index=out.index, fill_forward=fill_forward, default_rate=default_rate)
        return out

