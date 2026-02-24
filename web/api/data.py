"""Data API."""
import asyncio
import hashlib
import math
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import numpy as np
import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from loguru import logger

from config.settings import settings
from core.data import (
    data_collector,
    data_storage,
    historical_data_manager,
    second_level_backfill_manager,
    download_binance_1s_daily_archive,
)
from core.data.factor_library import FACTOR_CATALOG, build_factor_library
from core.exchanges import exchange_manager

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
_REPLAY_SESSIONS: Dict[str, Dict[str, Any]] = {}
_RESEARCH_COVERAGE_CACHE: Dict[str, Any] = {"path": None, "mtime": None, "df": None}


class KlineRequest(BaseModel):
    exchange: str
    symbol: str
    timeframe: str
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    limit: int = 500


class ReplayStartRequest(BaseModel):
    exchange: str = "binance"
    symbol: str = "BTC/USDT"
    timeframe: str = "1m"
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    window: int = 300
    speed: float = 1.0


def _timeframe_seconds(timeframe: str) -> int:
    tf = timeframe or "1m"
    unit = tf[-1]
    try:
        value = int(tf[:-1])
    except Exception:
        return 60

    if unit == "s":
        return max(1, value)
    if unit == "m":
        return max(1, value * 60)
    if unit == "h":
        return max(1, value * 3600)
    if unit == "d":
        return max(1, value * 86400)
    if unit == "w":
        return max(1, value * 7 * 86400)
    if unit == "M":
        return max(1, value * 30 * 86400)
    return 60


def _normalize_query_datetime(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt
    # Frontend passes ISO timestamps with timezone; convert to local naive
    # datetime to match parquet index convention in this project.
    return dt.astimezone().replace(tzinfo=None)


def _normalize_symbol_alias(symbol: str) -> str:
    s = str(symbol or "").strip().upper()
    if s in {"MATIC/USDT", "MATICUSDT"}:
        return "POL/USDT"
    return s


def _load_research_coverage_df() -> pd.DataFrame:
    try:
        path = Path(settings.DATA_STORAGE_PATH).parent / "research" / "universe30_coverage.csv"
        cache = _RESEARCH_COVERAGE_CACHE
        mtime = path.stat().st_mtime if path.exists() else None
        if cache.get("path") == str(path) and cache.get("mtime") == mtime and isinstance(cache.get("df"), pd.DataFrame):
            return cache["df"].copy()
        if not path.exists():
            cache.update({"path": str(path), "mtime": None, "df": pd.DataFrame()})
            return pd.DataFrame()
        df = pd.read_csv(path)
        for col in ("symbol", "timeframe"):
            if col in df.columns:
                df[col] = df[col].astype(str).str.upper()
        for col in ("is_stale", "retired_like"):
            if col in df.columns:
                df[col] = df[col].map(lambda x: str(x).strip().lower() in {"1", "true", "yes", "y"})
        cache.update({"path": str(path), "mtime": mtime, "df": df.copy()})
        return df
    except Exception as e:
        logger.warning(f"load research coverage failed: {e}")
        return pd.DataFrame()


def _research_retired_filter(
    exchange: str,
    timeframe: str,
    requested: List[str],
    exclude_retired: bool,
) -> tuple[List[str], List[str]]:
    norm_requested = [_normalize_symbol_alias(s) for s in requested]
    if not exclude_retired:
        return norm_requested, []
    if str(exchange or "").lower() != "binance":
        return norm_requested, []

    cov = _load_research_coverage_df()
    if cov.empty or "symbol" not in cov.columns or "timeframe" not in cov.columns or "retired_like" not in cov.columns:
        return norm_requested, []

    tf = str(timeframe or "").lower().upper()
    retired = cov[
        (cov["timeframe"].astype(str).str.upper() == tf) &
        (cov["retired_like"] == True)
    ]
    retired_set = set(retired["symbol"].astype(str).str.upper().tolist())
    filtered: List[str] = []
    excluded: List[str] = []
    seen = set()
    for sym in norm_requested:
        k = str(sym or "").strip().upper()
        if not k or k in seen:
            continue
        seen.add(k)
        if k in retired_set:
            excluded.append(k)
        else:
            filtered.append(k)
    return filtered, excluded


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
    merged = pd.concat([ohlc, volume], axis=1)
    merged = merged.dropna(subset=["open", "high", "low", "close"])
    return merged


def _parquet_path(exchange: str, symbol: str, timeframe: str) -> Path:
    base = Path(settings.DATA_STORAGE_PATH)
    return base / exchange / symbol.replace("/", "_") / f"{timeframe}.parquet"


async def _save_df_to_parquet(exchange: str, symbol: str, timeframe: str, df: pd.DataFrame) -> None:
    if df.empty:
        return

    target = _parquet_path(exchange, symbol, timeframe)
    target.parent.mkdir(parents=True, exist_ok=True)

    merged = df.copy()
    merged.index = pd.to_datetime(merged.index)
    merged = merged.sort_index()

    if target.exists():
        existing = pd.read_parquet(target)
        existing.index = pd.to_datetime(existing.index)
        merged = pd.concat([existing, merged])
        merged = merged[~merged.index.duplicated(keep="last")].sort_index()

    merged.to_parquet(target)


async def _safe_exchange_call(
    exchange: str,
    method_name: str,
    *args,
    retries: int = 2,
    **kwargs,
):
    last_error = None

    for attempt in range(retries + 1):
        connector = exchange_manager.get_exchange(exchange)
        if not connector:
            try:
                await exchange_manager.initialize([exchange])
                connector = exchange_manager.get_exchange(exchange)
            except Exception as e:
                last_error = e

        if not connector:
            await asyncio.sleep(0.2)
            continue

        method = getattr(connector, method_name, None)
        if not callable(method):
            raise RuntimeError(f"{exchange} connector has no method {method_name}")

        try:
            return await method(*args, **kwargs)
        except Exception as e:
            last_error = e
            logger.warning(
                f"[{exchange}] {method_name} failed (attempt {attempt + 1}/{retries + 1}): {e}"
            )

            # reconnect and retry
            try:
                await connector.disconnect()
            except Exception:
                pass
            try:
                await connector.connect()
            except Exception:
                pass
            await asyncio.sleep(0.4 * (attempt + 1))

    raise last_error or RuntimeError(f"{exchange}.{method_name} failed")


async def _fetch_public_trades(
    exchange: str,
    symbol: str,
    start_time: datetime,
    end_time: datetime,
    limit: int = 1000,
    max_batches: int = 80,
    max_trades: int = 120000,
) -> List[Dict[str, Any]]:
    connector = exchange_manager.get_exchange(exchange)
    if not connector:
        try:
            await exchange_manager.initialize([exchange])
            connector = exchange_manager.get_exchange(exchange)
        except Exception:
            connector = None

    if not connector:
        return []

    client = getattr(connector, "_client", None)
    fetch_trades = getattr(client, "fetch_trades", None)
    if not callable(fetch_trades):
        return []

    since_ms = int(start_time.timestamp() * 1000)
    end_ms = int(end_time.timestamp() * 1000)

    loops = 0
    all_trades: List[Dict[str, Any]] = []

    while since_ms < end_ms and loops < max(1, int(max_batches)):
        loops += 1
        batch = await fetch_trades(symbol, since=since_ms, limit=limit)
        if not batch:
            break

        valid = [t for t in batch if t.get("timestamp") and t["timestamp"] <= end_ms]
        if valid:
            all_trades.extend(valid)
            if len(all_trades) >= max(1, int(max_trades)):
                all_trades = all_trades[: max(1, int(max_trades))]
                break

        last_ts = batch[-1].get("timestamp")
        if not last_ts or last_ts <= since_ms:
            break

        since_ms = last_ts + 1
        if len(batch) < limit:
            break

    return all_trades


async def _fetch_binance_public_klines(symbol: str, timeframe: str, limit: int = 500) -> pd.DataFrame:
    tf_map = {
        "1m": "1m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "1h": "1h",
        "4h": "4h",
        "1d": "1d",
        "1w": "1w",
        "1M": "1M",
    }
    interval = tf_map.get(str(timeframe or "").strip())
    if not interval:
        return pd.DataFrame()

    clean_symbol = str(symbol or "").split(":")[0].replace("/", "").upper()
    if not clean_symbol:
        return pd.DataFrame()

    req_limit = max(10, min(int(limit or 500), 1000))
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": clean_symbol, "interval": interval, "limit": req_limit}
    async with httpx.AsyncClient(timeout=8.0) as client:
        res = await client.get(url, params=params)
        res.raise_for_status()
        payload = res.json()

    rows: List[Dict[str, Any]] = []
    for item in payload or []:
        if not isinstance(item, list) or len(item) < 6:
            continue
        ts = item[0]
        rows.append(
            {
                "timestamp": datetime.fromtimestamp(float(ts) / 1000.0),
                "open": float(item[1]),
                "high": float(item[2]),
                "low": float(item[3]),
                "close": float(item[4]),
                "volume": float(item[5]),
            }
        )

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).set_index("timestamp").sort_index()


def _trades_to_ohlcv(trades: List[Dict[str, Any]], timeframe: str) -> pd.DataFrame:
    if not trades:
        return pd.DataFrame()

    rule = _RESAMPLE_RULES.get(timeframe)
    if not rule:
        return pd.DataFrame()

    rows = []
    for t in trades:
        ts = t.get("timestamp")
        price = t.get("price")
        amount = t.get("amount")
        if ts is None or price is None or amount is None:
            continue
        rows.append(
            {
                "timestamp": datetime.fromtimestamp(float(ts) / 1000),
                "price": float(price),
                "amount": float(amount),
            }
        )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).set_index("timestamp").sort_index()
    ohlc = df["price"].resample(rule).ohlc()
    volume = df["amount"].resample(rule).sum().rename("volume")
    kdf = pd.concat([ohlc, volume], axis=1).dropna()
    kdf.columns = ["open", "high", "low", "close", "volume"]
    return kdf


async def _load_local_or_aggregate(
    exchange: str,
    symbol: str,
    timeframe: str,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> pd.DataFrame:
    df = await data_storage.load_klines_from_parquet(
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        start_time=start_time,
        end_time=end_time,
    )
    if not df.empty:
        return df

    if timeframe in {"1w", "1M"}:
        pad_start = (start_time - timedelta(days=35)) if start_time else None
        base = await data_storage.load_klines_from_parquet(
            exchange=exchange,
            symbol=symbol,
            timeframe="1d",
            start_time=pad_start,
            end_time=end_time,
        )
        if base.empty:
            for alt in ["gate", "binance"]:
                if alt == exchange:
                    continue
                base = await data_storage.load_klines_from_parquet(
                    exchange=alt,
                    symbol=symbol,
                    timeframe="1d",
                    start_time=pad_start,
                    end_time=end_time,
                )
                if not base.empty:
                    exchange = alt
                    break

        if not base.empty:
            agg = _resample_ohlcv(base, timeframe)
            if start_time:
                agg = agg[agg.index >= start_time]
            if end_time:
                agg = agg[agg.index <= end_time]
            if not agg.empty:
                await _save_df_to_parquet(exchange, symbol, timeframe, agg)
            return agg

    if timeframe in {"5s", "10s", "30s"}:
        pad_seconds = max(1, _timeframe_seconds(timeframe))
        base_start = (start_time - timedelta(seconds=pad_seconds)) if start_time else None
        base = await data_storage.load_klines_from_parquet(
            exchange=exchange,
            symbol=symbol,
            timeframe="1s",
            start_time=base_start,
            end_time=end_time,
        )
        if not base.empty:
            agg = _resample_ohlcv(base, timeframe)
            if start_time:
                agg = agg[agg.index >= start_time]
            if end_time:
                agg = agg[agg.index <= end_time]
            if not agg.empty:
                await _save_df_to_parquet(exchange, symbol, timeframe, agg)
            return agg

    return pd.DataFrame()


def _validate_ohlcv(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty:
        return {
            "rows": 0,
            "invalid_rows": 0,
            "duplicate_rows": 0,
            "invalid_ratio": 0.0,
        }

    check_df = df[["open", "high", "low", "close", "volume"]].copy()

    invalid = (
        (check_df[["open", "high", "low", "close"]] <= 0).any(axis=1)
        | (check_df["high"] < check_df[["open", "close", "low"]].max(axis=1))
        | (check_df["low"] > check_df[["open", "close", "high"]].min(axis=1))
        | (check_df["volume"] < 0)
    )

    duplicate_rows = int(check_df.index.duplicated().sum())
    invalid_rows = int(invalid.sum())
    rows = int(len(check_df))

    return {
        "rows": rows,
        "invalid_rows": invalid_rows,
        "duplicate_rows": duplicate_rows,
        "invalid_ratio": round((invalid_rows / rows), 6) if rows > 0 else 0.0,
    }


def _detect_missing_bars(df: pd.DataFrame, timeframe: str, max_preview: int = 2000) -> Dict[str, Any]:
    if df.empty:
        return {"missing_count": 0, "missing_preview": []}

    freq = _RESAMPLE_RULES.get(timeframe)
    if not freq:
        return {"missing_count": 0, "missing_preview": []}

    idx = pd.to_datetime(df.index).sort_values()
    full_range = pd.date_range(start=idx.min(), end=idx.max(), freq=freq)
    missing = full_range.difference(idx)

    preview = [ts.isoformat() for ts in missing[:max_preview]]
    return {
        "missing_count": int(len(missing)),
        "missing_preview": preview,
    }


def _fill_missing_bars(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    if df.empty:
        return df

    freq = _RESAMPLE_RULES.get(timeframe)
    if not freq:
        return df

    src = df.copy().sort_index()
    src.index = pd.to_datetime(src.index)

    full_index = pd.date_range(start=src.index.min(), end=src.index.max(), freq=freq)
    merged = src.reindex(full_index)

    close_ref = merged["close"].ffill()
    merged["open"] = merged["open"].fillna(close_ref)
    merged["high"] = merged["high"].fillna(merged[["open", "close"]].max(axis=1))
    merged["low"] = merged["low"].fillna(merged[["open", "close"]].min(axis=1))
    merged["close"] = merged["close"].fillna(close_ref)
    merged["volume"] = merged["volume"].fillna(0.0)

    merged = merged.dropna(subset=["open", "high", "low", "close"])
    return merged


def _new_replay_id(payload: Dict[str, Any]) -> str:
    raw = (
        f"{payload.get('exchange')}|{payload.get('symbol')}|{payload.get('timeframe')}|"
        f"{datetime.utcnow().isoformat()}|{len(_REPLAY_SESSIONS)}"
    )
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:12]


async def _load_symbol_df(
    exchange: str,
    symbol: str,
    timeframe: str,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> pd.DataFrame:
    df = await _load_local_or_aggregate(
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        start_time=start_time,
        end_time=end_time,
    )
    if start_time:
        df = df[df.index >= start_time]
    if end_time:
        df = df[df.index <= end_time]
    return df


async def _fetch_defillama_chain_tvl(chain: str = "Ethereum") -> Dict[str, Any]:
    url = f"https://api.llama.fi/v2/historicalChainTvl/{chain}"
    try:
        async with httpx.AsyncClient(timeout=12) as client:
            res = await client.get(url)
            res.raise_for_status()
            rows = res.json() or []
    except Exception as e:
        return {"chain": chain, "available": False, "error": str(e), "series": []}

    if not isinstance(rows, list) or not rows:
        return {"chain": chain, "available": False, "series": []}

    data = []
    for row in rows:
        ts = int(row.get("date") or 0)
        tvl = float(row.get("tvl") or 0.0)
        if ts <= 0:
            continue
        data.append({"timestamp": datetime.utcfromtimestamp(ts).isoformat(), "tvl": tvl})

    if not data:
        return {"chain": chain, "available": False, "series": []}

    latest = data[-1]["tvl"]
    prev_1d = data[-2]["tvl"] if len(data) >= 2 else latest
    prev_7d = data[-8]["tvl"] if len(data) >= 8 else prev_1d
    chg_1d = ((latest - prev_1d) / prev_1d * 100) if prev_1d > 0 else 0.0
    chg_7d = ((latest - prev_7d) / prev_7d * 100) if prev_7d > 0 else 0.0
    return {
        "chain": chain,
        "available": True,
        "latest_tvl": round(latest, 2),
        "change_1d_pct": round(chg_1d, 4),
        "change_7d_pct": round(chg_7d, 4),
        "series": data[-180:],
    }


async def _fetch_btc_whale_unconfirmed(min_btc: float = 100.0) -> Dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            tx_res = await client.get("https://blockchain.info/unconfirmed-transactions?format=json")
            px_res = await client.get("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT")
            tx_res.raise_for_status()
            px_res.raise_for_status()
            tx_json = tx_res.json() or {}
            px_json = px_res.json() or {}
    except Exception as e:
        return {"available": False, "error": str(e), "count": 0, "transactions": []}

    btc_price = float(px_json.get("price") or 0.0)
    txs = tx_json.get("txs") or []
    whales = []
    for tx in txs[:500]:
        out_value_satoshi = sum(float(v.get("value", 0.0) or 0.0) for v in (tx.get("out") or []))
        btc_amount = out_value_satoshi / 1e8
        if btc_amount < float(min_btc):
            continue
        whales.append(
            {
                "hash": tx.get("hash"),
                "btc": round(btc_amount, 6),
                "usd_estimate": round(btc_amount * btc_price, 2) if btc_price > 0 else None,
                "timestamp": datetime.utcfromtimestamp(int(tx.get("time") or 0)).isoformat()
                if tx.get("time")
                else None,
            }
        )

    whales.sort(key=lambda x: float(x.get("btc") or 0.0), reverse=True)
    return {
        "available": True,
        "btc_price": btc_price,
        "threshold_btc": float(min_btc),
        "count": len(whales),
        "transactions": whales[:50],
    }


def _calc_trade_imbalance_proxy(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not trades:
        return {"count": 0, "buy_volume": 0.0, "sell_volume": 0.0, "imbalance": 0.0}

    buy_volume = 0.0
    sell_volume = 0.0
    for t in trades:
        amount = float(t.get("amount") or 0.0)
        side = str(t.get("side") or "").lower()
        if side == "buy":
            buy_volume += amount
        elif side == "sell":
            sell_volume += amount
        else:
            if bool(t.get("takerOrMaker")):
                sell_volume += amount
            else:
                buy_volume += amount
    total = buy_volume + sell_volume
    imbalance = ((buy_volume - sell_volume) / total) if total > 0 else 0.0
    return {
        "count": len(trades),
        "buy_volume": round(buy_volume, 6),
        "sell_volume": round(sell_volume, 6),
        "imbalance": round(imbalance, 6),
    }


def _factor_series_from_returns(returns_df: pd.DataFrame) -> pd.DataFrame:
    if returns_df.empty:
        return pd.DataFrame()
    x = returns_df.dropna(how="all")
    if x.empty:
        return pd.DataFrame()

    mkt = x.mean(axis=1)
    vol = x.rolling(48, min_periods=12).std().mean(axis=1)
    mom = (1 + x).rolling(48, min_periods=12).apply(np.prod, raw=True) - 1
    mom_cross = mom.mean(axis=1)

    # Fama-like SMB proxy: low-liquidity basket - high-liquidity basket.
    avg_abs = x.abs().rolling(48, min_periods=12).mean().iloc[-1].sort_values()
    if len(avg_abs) >= 2:
        cut = max(1, len(avg_abs) // 3)
        low = list(avg_abs.head(cut).index)
        high = list(avg_abs.tail(cut).index)
        smb = x[low].mean(axis=1) - x[high].mean(axis=1)
    else:
        smb = pd.Series(0.0, index=x.index)

    factors = pd.DataFrame(
        {
            "MKT": mkt.fillna(0.0),
            "SMB": smb.fillna(0.0),
            "MOM": mom_cross.fillna(0.0),
            "VOL": vol.fillna(0.0),
        },
        index=x.index,
    )
    return factors


def _normalize_symbol_folder(folder_name: str) -> str:
    raw = str(folder_name or "").strip().upper()
    if not raw:
        return ""
    if "/" in raw:
        return raw
    if "_" in raw:
        parts = raw.split("_")
        if len(parts) >= 2:
            return f"{parts[0]}/{parts[1]}"
    if raw.endswith("USDT") and len(raw) > 4:
        return f"{raw[:-4]}/USDT"
    return raw


def _discover_local_symbols(exchange: str, timeframe: str, max_count: int = 200) -> List[str]:
    root = Path(settings.DATA_STORAGE_PATH) / str(exchange or "").lower()
    if not root.exists() or not root.is_dir():
        return []

    out: List[str] = []
    for sym_dir in root.iterdir():
        if not sym_dir.is_dir():
            continue
        tf_file = sym_dir / f"{timeframe}.parquet"
        tf_parts = sym_dir / f"{timeframe}_parts"
        if not tf_file.exists() and not tf_parts.exists():
            continue
        sym = _normalize_symbol_folder(sym_dir.name)
        if sym:
            out.append(sym)
        if len(out) >= max(1, int(max_count)):
            break
    return sorted(set(out))


def _expand_symbols_with_local(
    exchange: str,
    timeframe: str,
    requested: List[str],
    min_symbols: int,
    max_symbols: int,
    excluded_symbols: Optional[set[str]] = None,
) -> List[str]:
    out: List[str] = []
    seen = set()
    excluded = {str(s).strip().upper() for s in (excluded_symbols or set()) if str(s).strip()}
    for sym in requested:
        key = _normalize_symbol_alias(sym)
        if not key or key in seen:
            continue
        if key in excluded:
            continue
        out.append(key)
        seen.add(key)

    if len(out) < int(min_symbols):
        local = _discover_local_symbols(exchange=exchange, timeframe=timeframe, max_count=max_symbols * 2)
        for sym in local:
            key = _normalize_symbol_alias(sym)
            if not key or key in seen:
                continue
            if key in excluded:
                continue
            out.append(key)
            seen.add(key)
            if len(out) >= int(max_symbols):
                break

    return out[: max(1, int(max_symbols))]


def _latest_partition_end_time(exchange: str, symbol: str, timeframe: str) -> Optional[datetime]:
    sym_dir = Path(settings.DATA_STORAGE_PATH) / str(exchange or "").lower() / str(symbol).replace("/", "_")
    parts_dir = sym_dir / f"{timeframe}_parts"
    if not parts_dir.exists() or not parts_dir.is_dir():
        return None

    files = sorted(parts_dir.glob("*.parquet"), reverse=True)
    for path in files:
        try:
            day = pd.Timestamp(path.stem).to_pydatetime()
            return day + timedelta(days=1)
        except Exception:
            continue
    return None


async def _build_factor_input_frames(
    exchange: str,
    symbol_list: List[str],
    timeframe: str,
    lookback: int,
) -> tuple[pd.DataFrame, pd.DataFrame, List[str]]:
    approx_bars = max(120, int(lookback))
    tf_seconds = max(1, _timeframe_seconds(timeframe))
    # Bound disk scan cost for high-frequency data by querying a recent window first.
    window_seconds = max(3600, min(approx_bars * tf_seconds * 2, 366 * 24 * 3600))

    close_map: Dict[str, pd.Series] = {}
    volume_map: Dict[str, pd.Series] = {}

    for sym in symbol_list:
        end_hint = _latest_partition_end_time(exchange=exchange, symbol=sym, timeframe=timeframe)
        anchor = end_hint or datetime.utcnow()
        query_start = anchor - timedelta(seconds=window_seconds)

        df = await _load_symbol_df(
            exchange=exchange,
            symbol=sym,
            timeframe=timeframe,
            start_time=query_start,
            end_time=end_hint,
        )
        if df.empty and timeframe not in _SUB_MINUTE_TIMEFRAMES:
            # Fallback to full scan for symbols whose recent window has no local cache.
            df = await _load_symbol_df(exchange=exchange, symbol=sym, timeframe=timeframe)
        if df.empty:
            continue
        tail = df.tail(max(120, int(lookback)))
        close = pd.to_numeric(tail.get("close"), errors="coerce")
        volume = pd.to_numeric(tail.get("volume"), errors="coerce")
        if close is None or volume is None:
            continue
        close = close.dropna()
        volume = volume.reindex(close.index).fillna(0.0)
        if len(close) < 30:
            continue
        close_map[sym] = close
        volume_map[sym] = volume

    if not close_map:
        return pd.DataFrame(), pd.DataFrame(), []

    close_df = pd.DataFrame(close_map).sort_index()
    volume_df = pd.DataFrame(volume_map).reindex(close_df.index).sort_index().fillna(0.0)
    used = [c for c in close_df.columns if c in volume_df.columns]
    if not used:
        return pd.DataFrame(), pd.DataFrame(), []

    close_df = close_df[used].dropna(how="all")
    volume_df = volume_df[used].reindex(close_df.index).fillna(0.0)
    return close_df, volume_df, used


@router.get("/klines")
async def get_klines(
    exchange: str,
    symbol: str,
    timeframe: str = "1h",
    limit: int = 500,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    align: str = "tail",
):
    start_time = _normalize_query_datetime(start_time)
    end_time = _normalize_query_datetime(end_time)
    limit = max(10, min(limit, 5000))
    requested_exchange = str(exchange or "").lower() or "binance"
    candidates = [requested_exchange] + [
        ex for ex in ["binance", "gate", "okx"] if ex != requested_exchange
    ]
    actual_exchange = requested_exchange
    df = pd.DataFrame()

    load_start = start_time
    load_end = end_time
    if timeframe in _SUB_MINUTE_TIMEFRAMES and load_start is None:
        effective_end = load_end or datetime.now()
        seconds = _timeframe_seconds(timeframe)
        # Only load a bounded recent window for UI requests to avoid scanning full-year second-level partitions.
        lookback_seconds = max(900, min(limit * seconds * 4, 6 * 3600))
        load_start = effective_end - timedelta(seconds=lookback_seconds)

    async def _fetch_live_df(ex_name: str, live_limit: int) -> pd.DataFrame:
        if timeframe in _SUB_MINUTE_TIMEFRAMES:
            effective_limit = max(120, min(int(live_limit or 0), 480))
            seconds = _timeframe_seconds(timeframe)
            q_end_time = end_time or datetime.now()
            q_start_time = start_time
            if q_start_time is None:
                window_seconds = max(effective_limit * seconds, 300)
                window_seconds = min(window_seconds, 7200)
                q_start_time = q_end_time - timedelta(seconds=window_seconds)
            batch_cap = max(12, min(40, int(effective_limit // 15) + 8))
            trade_cap = max(6000, min(60000, int(effective_limit * 80)))
            try:
                trades = await asyncio.wait_for(
                    _fetch_public_trades(
                        ex_name,
                        symbol,
                        q_start_time,
                        q_end_time,
                        limit=1000,
                        max_batches=batch_cap,
                        max_trades=trade_cap,
                    ),
                    timeout=8.0,
                )
            except (asyncio.TimeoutError, asyncio.CancelledError) as e:
                raise TimeoutError(f"{ex_name} public trades fetch timeout/cancelled") from e
            return _trades_to_ohlcv(trades, timeframe)

        try:
            klines = await asyncio.wait_for(
                _safe_exchange_call(ex_name, "get_klines", symbol, timeframe, limit=live_limit),
                timeout=8.0,
            )
        except (asyncio.TimeoutError, asyncio.CancelledError) as e:
            raise TimeoutError(f"{ex_name} get_klines timeout/cancelled") from e
        if not klines:
            return pd.DataFrame()
        return pd.DataFrame(
            [
                {
                    "timestamp": k.timestamp,
                    "open": k.open,
                    "high": k.high,
                    "low": k.low,
                    "close": k.close,
                    "volume": k.volume,
                }
                for k in klines
            ]
        ).set_index("timestamp")

    # Prefer local data first, and fallback across exchanges.
    for ex in candidates:
        local_df = await _load_local_or_aggregate(
            exchange=ex,
            symbol=symbol,
            timeframe=timeframe,
            start_time=load_start,
            end_time=load_end,
        )
        if local_df.empty:
            continue
        df = local_df
        actual_exchange = ex
        break

    # If no local data, fetch live and persist.
    if df.empty:
        last_error: Optional[str] = None
        for ex in candidates:
            try:
                live_df = await _fetch_live_df(ex, live_limit=limit)
                if live_df.empty:
                    continue
                df = live_df
                actual_exchange = ex
                await _save_df_to_parquet(actual_exchange, symbol, timeframe, df)
                break
            except (asyncio.TimeoutError, asyncio.CancelledError) as e:
                last_error = f"{ex} live fetch timeout/cancelled: {e}"
            except Exception as e:
                last_error = str(e)
        if df.empty and last_error:
            return {
                "exchange": requested_exchange,
                "actual_exchange": requested_exchange,
                "symbol": symbol,
                "timeframe": timeframe,
                "data": [],
                "error": last_error,
            }
    else:
        # Merge latest live bars when requesting near-now window to keep chart realtime.
        if end_time is None:
            async def _refresh_cache_from_live(ex_name: str, live_limit: int) -> None:
                try:
                    fresh_df = await _fetch_live_df(ex_name, live_limit=live_limit)
                    if not fresh_df.empty:
                        await _save_df_to_parquet(ex_name, symbol, timeframe, fresh_df)
                except Exception as refresh_err:
                    logger.debug(f"background live refresh skipped: {refresh_err}")

            live_limit = max(120, min(limit, 240 if timeframe in _SUB_MINUTE_TIMEFRAMES else 1200))
            quick_timeout = 1.2 if timeframe in _SUB_MINUTE_TIMEFRAMES else 2.5
            stale_seconds = 0.0
            stale_threshold = max(90.0, _timeframe_seconds(timeframe) * 3.0)
            if not df.empty:
                last_local_ts = pd.to_datetime(df.index.max())
                stale_seconds = max(0.0, (datetime.now() - last_local_ts.to_pydatetime()).total_seconds())
                if stale_seconds > stale_threshold:
                    # Cache is stale: allow longer live pull to close chart gaps.
                    quick_timeout = max(quick_timeout, 8.0)
            used_public_fallback = False
            if actual_exchange == "binance" and timeframe not in _SUB_MINUTE_TIMEFRAMES:
                try:
                    public_df = await _fetch_binance_public_klines(
                        symbol=symbol,
                        timeframe=timeframe,
                        limit=live_limit,
                    )
                    if not public_df.empty:
                        await _save_df_to_parquet(actual_exchange, symbol, timeframe, public_df)
                        df = pd.concat([df, public_df])
                        df = df[~df.index.duplicated(keep="last")].sort_index()
                        used_public_fallback = True
                except Exception as public_err:
                    logger.debug(f"public kline fallback skipped: {public_err}")
            if not used_public_fallback:
                try:
                    live_df = await asyncio.wait_for(
                        _fetch_live_df(actual_exchange, live_limit=live_limit),
                        timeout=quick_timeout,
                    )
                    if not live_df.empty:
                        await _save_df_to_parquet(actual_exchange, symbol, timeframe, live_df)
                        df = pd.concat([df, live_df])
                        df = df[~df.index.duplicated(keep="last")].sort_index()
                except (asyncio.TimeoutError, asyncio.CancelledError) as live_err:
                    logger.debug(f"live refresh timeout/cancelled: {live_err}")
                    asyncio.create_task(_refresh_cache_from_live(actual_exchange, live_limit))
                except Exception as live_err:
                    # When cache is stale, force reconnect once and retry live pull.
                    if stale_seconds > stale_threshold:
                        try:
                            connector = exchange_manager.get_exchange(actual_exchange)
                            if connector:
                                try:
                                    await connector.disconnect()
                                except Exception:
                                    pass
                                await connector.connect()
                            retry_df = await asyncio.wait_for(
                                _fetch_live_df(actual_exchange, live_limit=live_limit),
                                timeout=max(10.0, quick_timeout),
                            )
                            if not retry_df.empty:
                                await _save_df_to_parquet(actual_exchange, symbol, timeframe, retry_df)
                                df = pd.concat([df, retry_df])
                                df = df[~df.index.duplicated(keep="last")].sort_index()
                        except (asyncio.TimeoutError, asyncio.CancelledError) as retry_err:
                            logger.debug(f"live refresh retry timeout/cancelled: {retry_err}")
                        except Exception as retry_err:
                            logger.debug(f"live refresh retry skipped: {retry_err}")
                        # Exchange connector may hang intermittently; use Binance public
                        # market data as fallback to keep UI candles near realtime.
                        if actual_exchange == "binance" and timeframe not in _SUB_MINUTE_TIMEFRAMES:
                            try:
                                public_df = await _fetch_binance_public_klines(
                                    symbol=symbol,
                                    timeframe=timeframe,
                                    limit=live_limit,
                                )
                                if not public_df.empty:
                                    await _save_df_to_parquet(actual_exchange, symbol, timeframe, public_df)
                                    df = pd.concat([df, public_df])
                                    df = df[~df.index.duplicated(keep="last")].sort_index()
                            except Exception as public_err:
                                logger.debug(f"public kline fallback skipped: {public_err}")
                    logger.debug(f"live refresh skipped: {live_err}")
                    # Do not block response on live fetch; refresh cache in background.
                    asyncio.create_task(_refresh_cache_from_live(actual_exchange, live_limit))

    if df.empty:
        return {
            "exchange": requested_exchange,
            "actual_exchange": actual_exchange,
            "symbol": symbol,
            "timeframe": timeframe,
            "data": [],
            "message": "无可用K线数据",
        }

    if start_time:
        df = df[df.index >= start_time]
    if end_time:
        df = df[df.index <= end_time]

    if df.empty:
        return {
            "exchange": requested_exchange,
            "actual_exchange": actual_exchange,
            "symbol": symbol,
            "timeframe": timeframe,
            "data": [],
            "message": "指定时间范围内无可用K线数据",
        }

    align_mode = str(align or "tail").lower()
    if align_mode == "head":
        df = df.head(limit)
    else:
        if start_time and not end_time:
            df = df.head(limit)
        else:
            df = df.tail(limit)

    return {
        "exchange": requested_exchange,
        "actual_exchange": actual_exchange,
        "symbol": symbol,
        "timeframe": timeframe,
        "data": [
            {
                "timestamp": idx.isoformat(),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row.get("volume", 0.0)),
            }
            for idx, row in df.iterrows()
        ],
    }


@router.get("/ticker")
async def get_ticker(exchange: str, symbol: str):
    try:
        ticker = await _safe_exchange_call(exchange, "get_ticker", symbol)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"交易所未连接或行情拉取失败: {e}")

    return {
        "exchange": exchange,
        "symbol": symbol,
        "last": ticker.last,
        "bid": ticker.bid,
        "ask": ticker.ask,
        "high_24h": ticker.high_24h,
        "low_24h": ticker.low_24h,
        "volume_24h": ticker.volume_24h,
        "timestamp": ticker.timestamp.isoformat(),
    }


@router.get("/tickers")
async def get_tickers(exchange: str):
    symbols = exchange_manager.get_supported_symbols(exchange)
    tickers = []

    for symbol in symbols[:20]:
        try:
            ticker = await _safe_exchange_call(exchange, "get_ticker", symbol)
            tickers.append(
                {
                    "symbol": symbol,
                    "last": ticker.last,
                    "change_24h": (ticker.last - ticker.low_24h) / ticker.low_24h if ticker.low_24h > 0 else 0,
                    "volume_24h": ticker.volume_24h,
                }
            )
        except Exception:
            continue

    return {"exchange": exchange, "tickers": tickers}


@router.post("/download")
async def download_historical_data(
    exchange: str,
    symbol: str,
    timeframe: str = "1h",
    days: int = 365,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
):
    start_time = _normalize_query_datetime(start_time)
    end_time = _normalize_query_datetime(end_time)
    if end_time is None:
        end_time = datetime.now()
    if start_time is None:
        start_time = end_time - timedelta(days=days)
    if start_time > end_time:
        start_time, end_time = end_time, start_time
    span_days = max(0.0, (end_time - start_time).total_seconds() / 86400.0)

    connector = exchange_manager.get_exchange(exchange)
    if not connector:
        for alt_exchange in ["gate", "binance"]:
            connector = exchange_manager.get_exchange(alt_exchange)
            if connector:
                exchange = alt_exchange
                break

    if not connector:
        return {
            "exchange": exchange,
            "symbol": symbol,
            "timeframe": timeframe,
            "count": 0,
            "error": "没有可用的交易所连接",
            "start": start_time.isoformat(),
            "end": end_time.isoformat(),
        }

    if timeframe in _SUB_MINUTE_TIMEFRAMES:
        if span_days > 7:
            if exchange == "binance" and timeframe == "1s":
                start_date = start_time.date()
                end_date = (end_time - timedelta(days=1)).date()
                if end_date < start_date:
                    end_date = start_date
                stats = await asyncio.to_thread(
                    download_binance_1s_daily_archive,
                    symbol,
                    start_date,
                    end_date,
                    True,
                )
                return {
                    "exchange": exchange,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "count": int(stats.total_rows),
                    "start": start_date.isoformat(),
                    "end": end_date.isoformat(),
                    "message": "已从 Binance 历史包下载秒级数据",
                    "archive_stats": stats.to_dict(),
                }

            task = second_level_backfill_manager.start_task(
                exchange=exchange,
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
                window_days=1,
            )
            return {
                "exchange": exchange,
                "symbol": symbol,
                "timeframe": timeframe,
                "count": 0,
                "async_task": task,
                "start": start_time.isoformat(),
                "end": end_time.isoformat(),
                "message": "秒级数据跨度超过7天，已切换为后台分片回填任务",
            }

        trades = await _fetch_public_trades(exchange, symbol, start_time, end_time)
        kdf = _trades_to_ohlcv(trades, timeframe)
        if kdf.empty:
            return {
                "exchange": exchange,
                "symbol": symbol,
                "timeframe": timeframe,
                "count": 0,
                "error": "未获取到逐笔成交，无法生成子分钟K线",
                "start": start_time.isoformat(),
                "end": end_time.isoformat(),
            }

        await _save_df_to_parquet(exchange, symbol, timeframe, kdf)
        return {
            "exchange": exchange,
            "symbol": symbol,
            "timeframe": timeframe,
            "count": int(len(kdf)),
            "start": kdf.index[0].isoformat(),
            "end": kdf.index[-1].isoformat(),
            "message": "子分钟K线已从逐笔成交聚合完成",
        }

    if timeframe in {"1w", "1M"}:
        await historical_data_manager.download_historical_klines(
            exchange=exchange,
            symbol=symbol,
            timeframe="1d",
            start_time=start_time,
            end_time=end_time,
        )
        day_df = await data_storage.load_klines_from_parquet(exchange=exchange, symbol=symbol, timeframe="1d")
        agg_df = _resample_ohlcv(day_df, timeframe)
        if agg_df.empty:
            return {
                "exchange": exchange,
                "symbol": symbol,
                "timeframe": timeframe,
                "count": 0,
                "error": "日线数据不足，无法聚合周/月线",
                "start": start_time.isoformat(),
                "end": end_time.isoformat(),
            }

        await _save_df_to_parquet(exchange, symbol, timeframe, agg_df)
        return {
            "exchange": exchange,
            "symbol": symbol,
            "timeframe": timeframe,
            "count": int(len(agg_df)),
            "start": agg_df.index[0].isoformat(),
            "end": agg_df.index[-1].isoformat(),
            "message": "周/月线已聚合生成",
        }

    klines = await historical_data_manager.download_historical_klines(
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        start_time=start_time,
        end_time=end_time,
    )

    return {
        "exchange": exchange,
        "symbol": symbol,
        "timeframe": timeframe,
        "count": len(klines),
        "start": start_time.isoformat(),
        "end": end_time.isoformat(),
    }


@router.get("/integrity/check")
async def check_data_integrity(
    exchange: str,
    symbol: str,
    timeframe: str = "1h",
):
    df = await _load_local_or_aggregate(exchange=exchange, symbol=symbol, timeframe=timeframe)
    if df.empty:
        return {
            "exchange": exchange,
            "symbol": symbol,
            "timeframe": timeframe,
            "ok": False,
            "message": "无本地数据",
            "quality": _validate_ohlcv(df),
            "missing": {"missing_count": 0, "missing_preview": []},
        }

    quality = _validate_ohlcv(df)
    missing = _detect_missing_bars(df, timeframe)
    ok = quality["invalid_rows"] == 0 and quality["duplicate_rows"] == 0

    return {
        "exchange": exchange,
        "symbol": symbol,
        "timeframe": timeframe,
        "ok": bool(ok),
        "quality": quality,
        "missing": missing,
        "rows": int(len(df)),
        "start": df.index.min().isoformat(),
        "end": df.index.max().isoformat(),
    }


@router.post("/integrity/repair")
async def repair_data_integrity(
    exchange: str,
    symbol: str,
    timeframe: str = "1h",
):
    df = await _load_local_or_aggregate(exchange=exchange, symbol=symbol, timeframe=timeframe)
    if df.empty:
        raise HTTPException(status_code=404, detail="无本地数据可修复")

    before_missing = _detect_missing_bars(df, timeframe)
    cleaned = df.copy()
    cleaned = cleaned[~cleaned.index.duplicated(keep="last")].sort_index()
    cleaned = _fill_missing_bars(cleaned, timeframe)

    await _save_df_to_parquet(exchange, symbol, timeframe, cleaned)

    after_missing = _detect_missing_bars(cleaned, timeframe)
    return {
        "success": True,
        "exchange": exchange,
        "symbol": symbol,
        "timeframe": timeframe,
        "before": {
            "rows": int(len(df)),
            "missing": before_missing,
            "quality": _validate_ohlcv(df),
        },
        "after": {
            "rows": int(len(cleaned)),
            "missing": after_missing,
            "quality": _validate_ohlcv(cleaned),
        },
    }


@router.get("/cross-validate")
async def cross_validate_data(
    symbol: str,
    timeframe: str = "1h",
    primary_exchange: str = "binance",
    secondary_exchange: str = "gate",
    limit: int = 500,
):
    primary = await _load_local_or_aggregate(primary_exchange, symbol, timeframe)
    secondary = await _load_local_or_aggregate(secondary_exchange, symbol, timeframe)

    if primary.empty or secondary.empty:
        raise HTTPException(status_code=404, detail="两路数据至少一路缺失")

    p = primary.tail(limit)[["close", "volume"]].copy()
    s = secondary.tail(limit)[["close", "volume"]].copy()

    merged = p.join(s, how="inner", lsuffix="_p", rsuffix="_s")
    if merged.empty:
        raise HTTPException(status_code=400, detail="两路数据无重合时间区间")

    close_diff_pct = (merged["close_p"] - merged["close_s"]).abs() / merged["close_p"].replace(0, pd.NA)
    volume_diff_pct = (merged["volume_p"] - merged["volume_s"]).abs() / merged["volume_p"].replace(0, pd.NA)

    close_diff_pct = close_diff_pct.fillna(0)
    volume_diff_pct = volume_diff_pct.fillna(0)

    result = {
        "symbol": symbol,
        "timeframe": timeframe,
        "primary_exchange": primary_exchange,
        "secondary_exchange": secondary_exchange,
        "overlap_bars": int(len(merged)),
        "close_diff": {
            "mean_pct": round(float(close_diff_pct.mean() * 100), 6),
            "max_pct": round(float(close_diff_pct.max() * 100), 6),
            "p95_pct": round(float(close_diff_pct.quantile(0.95) * 100), 6),
        },
        "volume_diff": {
            "mean_pct": round(float(volume_diff_pct.mean() * 100), 6),
            "max_pct": round(float(volume_diff_pct.max() * 100), 6),
            "p95_pct": round(float(volume_diff_pct.quantile(0.95) * 100), 6),
        },
    }

    result["is_consistent"] = result["close_diff"]["mean_pct"] < 1.0
    return result


@router.post("/reconnect")
async def reconnect_exchange(exchange: str):
    connector = exchange_manager.get_exchange(exchange)
    if not connector:
        ok = await exchange_manager.initialize([exchange])
        return {
            "exchange": exchange,
            "connected": bool(ok),
            "message": "交易所连接已初始化" if ok else "初始化失败",
        }

    try:
        await connector.disconnect()
    except Exception:
        pass

    ok = await connector.connect()
    return {
        "exchange": exchange,
        "connected": bool(ok),
        "message": "重连成功" if ok else "重连失败",
    }


@router.get("/coverage")
async def get_data_coverage(exchange: str, symbol: str, timeframe: str = "1h"):
    return await historical_data_manager.get_data_coverage(
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
    )


@router.get("/storage/stats")
async def get_storage_stats():
    return await data_storage.get_storage_stats()


@router.get("/available")
async def get_available_data():
    storage_path = Path(settings.DATA_STORAGE_PATH)
    available = []

    if storage_path.exists():
        for exchange_dir in storage_path.iterdir():
            if not exchange_dir.is_dir():
                continue
            for symbol_dir in exchange_dir.iterdir():
                if not symbol_dir.is_dir():
                    continue
                for file in symbol_dir.glob("*.parquet"):
                    available.append(
                        {
                            "exchange": exchange_dir.name,
                            "symbol": symbol_dir.name.replace("_", "/"),
                            "timeframe": file.stem,
                        }
                    )

    return {"available": available, "count": len(available)}


@router.get("/collector/tasks")
async def get_collector_tasks():
    return {
        "running": data_collector.is_running,
        "task_count": data_collector.task_count,
        "tasks": data_collector.list_tasks(),
    }


@router.post("/seconds/backfill/start")
async def start_second_level_backfill(
    exchange: str = "binance",
    symbol: str = "BTC/USDT",
    days: int = 365,
    window_days: int = 1,
):
    days = max(1, min(days, 1200))
    end_time = datetime.now()
    start_time = end_time - timedelta(days=days)
    task = second_level_backfill_manager.start_task(
        exchange=exchange,
        symbol=symbol,
        start_time=start_time,
        end_time=end_time,
        window_days=max(1, min(window_days, 7)),
    )
    return {
        "success": True,
        "message": "秒级回填任务已启动",
        "task": task,
    }


@router.get("/seconds/backfill/tasks")
async def list_second_level_backfill_tasks():
    tasks = second_level_backfill_manager.list_tasks()
    return {
        "count": len(tasks),
        "tasks": tasks,
    }


@router.get("/seconds/backfill/tasks/{task_id}")
async def get_second_level_backfill_task(task_id: str):
    task = second_level_backfill_manager.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


@router.post("/seconds/backfill/tasks/{task_id}/stop")
async def stop_second_level_backfill_task(task_id: str):
    ok = second_level_backfill_manager.stop_task(task_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Task not found")
    return {"success": True, "task_id": task_id}


@router.post("/replay/start")
async def start_replay(req: ReplayStartRequest):
    df = await _load_symbol_df(
        exchange=req.exchange,
        symbol=req.symbol,
        timeframe=req.timeframe,
        start_time=req.start_time,
        end_time=req.end_time,
    )
    if df.empty:
        raise HTTPException(status_code=404, detail="缺少回放数据")

    window = max(20, min(int(req.window or 300), 5000))
    replay_id = _new_replay_id(
        {
            "exchange": req.exchange,
            "symbol": req.symbol,
            "timeframe": req.timeframe,
        }
    )
    _REPLAY_SESSIONS[replay_id] = {
        "exchange": req.exchange,
        "symbol": req.symbol,
        "timeframe": req.timeframe,
        "window": window,
        "speed": max(0.1, min(float(req.speed or 1.0), 100.0)),
        "data": df,
        "cursor": 0,
        "started_at": datetime.utcnow().isoformat(),
    }
    return {
        "replay_id": replay_id,
        "exchange": req.exchange,
        "symbol": req.symbol,
        "timeframe": req.timeframe,
        "total": int(len(df)),
        "window": window,
        "started_at": _REPLAY_SESSIONS[replay_id]["started_at"],
    }


@router.get("/replay/{replay_id}")
async def get_replay_status(replay_id: str):
    session = _REPLAY_SESSIONS.get(replay_id)
    if not session:
        raise HTTPException(status_code=404, detail="Replay session not found")
    total = int(len(session["data"]))
    cursor = int(session["cursor"])
    return {
        "replay_id": replay_id,
        "exchange": session["exchange"],
        "symbol": session["symbol"],
        "timeframe": session["timeframe"],
        "cursor": cursor,
        "total": total,
        "progress": round(cursor / total, 6) if total > 0 else 0.0,
        "done": cursor >= total,
    }


@router.get("/replay/{replay_id}/next")
async def replay_next(replay_id: str, steps: int = 1):
    session = _REPLAY_SESSIONS.get(replay_id)
    if not session:
        raise HTTPException(status_code=404, detail="Replay session not found")

    df = session["data"]
    total = int(len(df))
    cursor = int(session["cursor"])
    steps = max(1, min(int(steps), 2000))
    next_cursor = min(total, cursor + steps)
    session["cursor"] = next_cursor

    window = int(session["window"])
    start = max(0, next_cursor - window)
    chunk = df.iloc[start:next_cursor]
    rows = [
        {
            "timestamp": idx.isoformat(),
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": float(row.get("volume", 0.0)),
        }
        for idx, row in chunk.iterrows()
    ]
    return {
        "replay_id": replay_id,
        "cursor": next_cursor,
        "total": total,
        "done": next_cursor >= total,
        "window": window,
        "data": rows,
    }


@router.post("/replay/{replay_id}/seek")
async def replay_seek(replay_id: str, timestamp: str):
    session = _REPLAY_SESSIONS.get(replay_id)
    if not session:
        raise HTTPException(status_code=404, detail="Replay session not found")
    df = session["data"]
    try:
        ts = pd.to_datetime(timestamp)
    except Exception:
        raise HTTPException(status_code=400, detail="timestamp 格式错误")
    idx = int(df.index.searchsorted(ts, side="left"))
    idx = max(0, min(idx, len(df)))
    session["cursor"] = idx
    return {"replay_id": replay_id, "cursor": idx, "total": int(len(df))}


@router.delete("/replay/{replay_id}")
async def stop_replay(replay_id: str):
    if replay_id in _REPLAY_SESSIONS:
        _REPLAY_SESSIONS.pop(replay_id, None)
        return {"success": True, "replay_id": replay_id}
    raise HTTPException(status_code=404, detail="Replay session not found")


@router.get("/onchain/overview")
async def get_onchain_overview(
    symbol: str = "BTC/USDT",
    exchange: str = "binance",
    whale_threshold_btc: float = 100.0,
    chain: str = "Ethereum",
):
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=4)
    trades = await _fetch_public_trades(exchange, symbol, start_time, end_time, limit=1000)
    imbalance = _calc_trade_imbalance_proxy(trades)

    tvl_task = asyncio.create_task(_fetch_defillama_chain_tvl(chain=chain))
    whale_task = asyncio.create_task(_fetch_btc_whale_unconfirmed(min_btc=max(1.0, whale_threshold_btc)))
    tvl, whales = await asyncio.gather(tvl_task, whale_task)

    return {
        "symbol": symbol,
        "exchange": exchange,
        "window_hours": 4,
        "exchange_flow_proxy": imbalance,
        "defi_tvl": tvl,
        "whale_activity": whales,
    }


@router.get("/multi-assets/overview")
async def get_multi_assets_overview(
    exchange: str = "binance",
    symbols: str = "BTC/USDT,ETH/USDT,SOL/USDT,BNB/USDT,XRP/USDT",
    timeframe: str = "1h",
    lookback: int = 500,
    exclude_retired: bool = True,
):
    requested = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    filtered_requested, excluded_retired = _research_retired_filter(
        exchange=exchange, timeframe=timeframe, requested=requested, exclude_retired=exclude_retired
    )
    symbol_list = filtered_requested
    symbol_list = symbol_list[:30]
    rows = []
    ret_map: Dict[str, pd.Series] = {}

    for sym in symbol_list:
        df = await _load_symbol_df(exchange=exchange, symbol=sym, timeframe=timeframe)
        if df.empty:
            continue
        sdf = df.tail(max(80, int(lookback)))
        close = sdf["close"].astype(float)
        ret = close.pct_change().dropna()
        if ret.empty:
            continue
        ret_map[sym] = ret
        rows.append(
            {
                "symbol": sym,
                "last": float(close.iloc[-1]),
                "return_pct": round(float((close.iloc[-1] / close.iloc[0] - 1) * 100), 4),
                "volatility_pct": round(float(ret.std() * math.sqrt(len(ret)) * 100), 4),
                "avg_volume": round(float(sdf["volume"].astype(float).mean()), 6),
                "max_drawdown_pct": round(float(((close / close.cummax()) - 1).min() * 100), 4),
            }
        )

    corr = {}
    if ret_map:
        corr_df = pd.DataFrame(ret_map).dropna(how="all")
        if len(corr_df.columns) >= 2:
            corr = corr_df.corr().round(4).fillna(0.0).to_dict()

    rows.sort(key=lambda x: x["return_pct"], reverse=True)
    return {
        "exchange": exchange,
        "timeframe": timeframe,
        "retired_filter": {
            "enabled": bool(exclude_retired),
            "excluded_symbols": excluded_retired,
            "requested_count": len(requested),
            "requested_after_filter_count": len(symbol_list),
        },
        "count": len(rows),
        "assets": rows,
        "correlation": corr,
    }


@router.get("/factors/fama")
async def get_fama_like_factors(
    exchange: str = "binance",
    symbols: str = "BTC/USDT,ETH/USDT,SOL/USDT,BNB/USDT,XRP/USDT,DOGE/USDT,ADA/USDT",
    timeframe: str = "1h",
    lookback: int = 1000,
    exclude_retired: bool = True,
):
    requested0 = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    requested, excluded_retired = _research_retired_filter(
        exchange=exchange, timeframe=timeframe, requested=requested0, exclude_retired=exclude_retired
    )
    symbol_list = _expand_symbols_with_local(
        exchange=exchange,
        timeframe=timeframe,
        requested=requested,
        min_symbols=2,
        max_symbols=40,
        excluded_symbols=set(excluded_retired),
    )
    close_df, volume_df, used = await _build_factor_input_frames(
        exchange=exchange,
        symbol_list=symbol_list,
        timeframe=timeframe,
        lookback=int(lookback),
    )
    if close_df.empty or len(used) < 2:
        raise HTTPException(status_code=404, detail="可用于因子计算的数据不足")

    result = build_factor_library(close_df=close_df, volume_df=volume_df, quantile=0.3, timeframe=timeframe)
    full = result.factors
    if full.empty:
        raise HTTPException(status_code=404, detail="因子计算失败")

    factors = full[[c for c in ["MKT", "SMB", "HML", "MOM", "RMW", "CMA", "VOL"] if c in full.columns]].copy()
    latest = factors.iloc[-1].to_dict()
    mean_24 = factors.tail(min(24, len(factors))).mean().to_dict()
    std_24 = factors.tail(min(24, len(factors))).std().fillna(0.0).to_dict()

    out_series = []
    for idx, row in factors.tail(400).iterrows():
        out_series.append(
            {
                "timestamp": idx.isoformat(),
                "MKT": round(float(row.get("MKT", 0.0)), 8),
                "SMB": round(float(row.get("SMB", 0.0)), 8),
                "HML": round(float(row.get("HML", 0.0)), 8),
                "MOM": round(float(row.get("MOM", 0.0)), 8),
                "RMW": round(float(row.get("RMW", 0.0)), 8),
                "CMA": round(float(row.get("CMA", 0.0)), 8),
                "VOL": round(float(row.get("VOL", 0.0)), 8),
            }
        )

    return {
        "exchange": exchange,
        "timeframe": timeframe,
        "symbols_requested": requested0,
        "retired_filter": {
            "enabled": bool(exclude_retired),
            "excluded_symbols": excluded_retired,
            "requested_after_filter": requested,
        },
        "symbols_used": used,
        "points": len(factors),
        "universe_size": len(used),
        "universe_quality": "low" if len(used) < 4 else "normal",
        "latest": {k: round(float(v), 8) for k, v in latest.items()},
        "mean_24": {k: round(float(v), 8) for k, v in mean_24.items()},
        "std_24": {k: round(float(v), 8) for k, v in std_24.items()},
        "series": out_series,
    }


@router.get("/factors/library")
async def get_factor_library(
    exchange: str = "binance",
    symbols: str = "BTC/USDT,ETH/USDT,SOL/USDT,BNB/USDT,XRP/USDT,DOGE/USDT,ADA/USDT",
    timeframe: str = "1h",
    lookback: int = 1200,
    quantile: float = 0.3,
    series_limit: int = 500,
    exclude_retired: bool = True,
):
    timeframe = str(timeframe or "1h").lower()
    lookback = int(lookback)
    if timeframe.endswith("s"):
        lookback = min(lookback, 300)
    elif timeframe == "1m":
        lookback = min(lookback, 480)
    elif timeframe == "5m":
        lookback = min(lookback, 900)
    elif timeframe == "15m":
        lookback = min(lookback, 1400)
    elif timeframe == "1h":
        lookback = min(lookback, 1800)
    else:
        lookback = min(lookback, 2400)
    lookback = max(120, lookback)

    requested0 = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    requested, excluded_retired = _research_retired_filter(
        exchange=exchange, timeframe=timeframe, requested=requested0, exclude_retired=exclude_retired
    )
    symbol_list = _expand_symbols_with_local(
        exchange=exchange,
        timeframe=timeframe,
        requested=requested,
        min_symbols=4,
        max_symbols=60,
        excluded_symbols=set(excluded_retired),
    )
    close_df, volume_df, used = await _build_factor_input_frames(
        exchange=exchange,
        symbol_list=symbol_list,
        timeframe=timeframe,
        lookback=lookback,
    )
    if close_df.empty or len(used) < 2:
        raise HTTPException(status_code=404, detail="可用于多因子计算的数据不足（至少2个币种）")

    result = build_factor_library(close_df=close_df, volume_df=volume_df, quantile=float(quantile), timeframe=timeframe)
    factors = result.factors
    if factors.empty:
        raise HTTPException(status_code=404, detail="多因子计算失败")

    latest = factors.iloc[-1].to_dict()
    mean_24 = factors.tail(min(24, len(factors))).mean().to_dict()
    std_24 = factors.tail(min(24, len(factors))).std().fillna(0.0).to_dict()
    corr = factors.corr().round(4).fillna(0.0).to_dict()

    series: List[Dict[str, Any]] = []
    tail = factors.tail(max(30, min(int(series_limit), 2000)))
    for idx, row in tail.iterrows():
        payload = {"timestamp": idx.isoformat()}
        for col in factors.columns:
            payload[col] = round(float(row.get(col, 0.0)), 10)
        series.append(payload)

    asset_scores = []
    if not result.asset_scores.empty:
        for _, row in result.asset_scores.head(30).iterrows():
            asset_scores.append(
                {
                    "symbol": str(row.get("symbol")),
                    "score": round(float(row.get("score", 0.0)), 6),
                    "momentum": round(float(row.get("momentum", 0.0)), 6),
                    "value": round(float(row.get("value", 0.0)), 6),
                    "value_hml": round(float(row.get("value_hml", 0.0)), 6),
                    "quality": round(float(row.get("quality", 0.0)), 6),
                    "profitability": round(float(row.get("profitability", 0.0)), 6),
                    "investment": round(float(row.get("investment", 0.0)), 6),
                    "low_vol": round(float(row.get("low_vol", 0.0)), 6),
                    "liquidity": round(float(row.get("liquidity", 0.0)), 6),
                    "low_beta": round(float(row.get("low_beta", 0.0)), 6),
                    "size": round(float(row.get("size", 0.0)), 6),
                }
            )

    warnings: List[str] = []
    if len(used) < 4:
        warnings.append("当前可用币种较少（<4），横截面因子稳定性有限，建议补充更多币种历史数据。")

    return {
        "exchange": exchange,
        "timeframe": timeframe,
        "lookback_effective": lookback,
        "symbols_requested": requested0,
        "retired_filter": {
            "enabled": bool(exclude_retired),
            "excluded_symbols": excluded_retired,
            "requested_after_filter": requested,
        },
        "symbols_used": used,
        "points": int(len(factors)),
        "factors": list(factors.columns),
        "catalog": FACTOR_CATALOG,
        "universe_size": len(used),
        "universe_quality": "low" if len(used) < 4 else "normal",
        "warnings": warnings,
        "latest": {k: round(float(v), 10) for k, v in latest.items()},
        "mean_24": {k: round(float(v), 10) for k, v in mean_24.items()},
        "std_24": {k: round(float(v), 10) for k, v in std_24.items()},
        "correlation": corr,
        "series": series,
        "asset_scores": asset_scores,
    }
