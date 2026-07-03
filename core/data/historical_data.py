from __future__ import annotations

"""
Historical data management helpers.
"""
import asyncio
import contextlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional

from loguru import logger

from config.settings import settings
from core.data.data_storage import data_storage
from core.exchanges import Kline, exchange_manager

try:
    import ccxt.async_support as ccxt

    _CCXT_ASYNC_AVAILABLE = True
except Exception:  # pragma: no cover - ccxt is an application dependency
    ccxt = None  # type: ignore
    _CCXT_ASYNC_AVAILABLE = False


_DOWNLOAD_REQUEST_TIMEOUT_SEC = 45.0
_DOWNLOAD_MAX_CONSECUTIVE_ERRORS = 6
_DOWNLOAD_RETRY_SLEEP_SEC = 2.0
_NON_RETRYABLE_DOWNLOAD_ERROR_MARKERS = (
    "does not have market symbol",
    "bad symbol",
    "symbol not found",
    "invalid symbol",
    "market not found",
)


def _normalize_market_type(value: str, fallback: str = "spot") -> str:
    raw = str(value or fallback or "spot").strip().lower()
    aliases = {
        "futures": "future",
        "perp": "swap",
        "perpetual": "swap",
    }
    normalized = aliases.get(raw, raw)
    return normalized if normalized in {"spot", "future", "swap", "margin"} else "spot"


def _public_default_type(exchange: str) -> str:
    attr = f"{str(exchange or '').strip().upper()}_DEFAULT_TYPE"
    return _normalize_market_type(str(getattr(settings, attr, "spot") or "spot"))


def _proxy_url() -> Optional[str]:
    proxy = str(settings.HTTP_PROXY or settings.HTTPS_PROXY or "").strip()
    return proxy or None


def _resolve_public_market_symbol(client: Any, symbol: str, default_type: str) -> str:
    raw_symbol = str(symbol or "").strip()
    markets = getattr(client, "markets", None) or {}
    if raw_symbol in markets:
        return raw_symbol

    if "/" not in raw_symbol:
        return raw_symbol

    base, quote = raw_symbol.split("/", 1)
    base = base.strip().upper()
    quote = quote.split(":", 1)[0].strip().upper()
    if not base or not quote:
        return raw_symbol

    contract_symbol = f"{base}/{quote}:{quote}"
    if contract_symbol in markets:
        return contract_symbol

    candidates = []
    for market in markets.values():
        if str(market.get("base") or "").upper() != base:
            continue
        if str(market.get("quote") or "").upper() != quote:
            continue
        market_symbol = str(market.get("symbol") or "").strip()
        if market_symbol:
            candidates.append(market)

    if not candidates:
        return raw_symbol

    prefer_derivatives = default_type in {"future", "swap"}

    def _score(market: Dict[str, Any]) -> int:
        score = 0
        if prefer_derivatives:
            if bool(market.get(default_type)):
                score += 100
            if bool(market.get("future")) or bool(market.get("swap")):
                score += 50
            if bool(market.get("linear")):
                score += 10
        elif bool(market.get("spot")):
            score += 100
        if market.get("active") is False:
            score -= 25
        return score

    best = max(candidates, key=_score)
    return str(best.get("symbol") or raw_symbol)


class _PublicCCXTKlineConnector:
    """Unauthenticated OHLCV source used when live trading connectors are down."""

    def __init__(self, exchange: str):
        self.name = str(exchange or "").strip().lower()
        self.default_type = _public_default_type(self.name)
        self._client: Any = None
        self._markets_loaded = False

    def _build_client(self) -> Any:
        if not _CCXT_ASYNC_AVAILABLE or ccxt is None:
            raise RuntimeError("ccxt async support is unavailable")
        factory = getattr(ccxt, self.name, None)
        if factory is None:
            raise RuntimeError(f"ccxt has no exchange '{self.name}'")

        client_config: Dict[str, Any] = {
            "enableRateLimit": True,
            "timeout": 15000,
            "options": {"defaultType": self.default_type},
        }
        proxy = _proxy_url()
        if proxy:
            client_config["aiohttp_proxy"] = proxy
            client_config["proxies"] = {
                "http": proxy,
                "https": str(settings.HTTPS_PROXY or proxy),
            }

        return factory(client_config)

    async def _ensure_client(self) -> Any:
        if self._client is None:
            self._client = self._build_client()
        if not self._markets_loaded:
            await self._client.load_markets()
            self._markets_loaded = True
        return self._client

    async def close(self) -> None:
        client = self._client
        self._client = None
        self._markets_loaded = False
        if client is None:
            return
        close_result = getattr(client, "close", lambda: None)()
        if asyncio.iscoroutine(close_result):
            with contextlib.suppress(Exception):
                await close_result

    async def get_klines(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> List[Kline]:
        client = await self._ensure_client()
        mapped_symbol = _resolve_public_market_symbol(client, symbol, self.default_type)
        since_ms = int(since.timestamp() * 1000) if since else None
        ohlcv = await client.fetch_ohlcv(
            mapped_symbol,
            timeframe,
            since=since_ms,
            limit=limit or 1000,
        )
        return [
            Kline(
                symbol=symbol,
                timeframe=timeframe,
                timestamp=datetime.fromtimestamp(candle[0] / 1000, tz=timezone.utc),
                open=float(candle[1]),
                high=float(candle[2]),
                low=float(candle[3]),
                close=float(candle[4]),
                volume=float(candle[5]),
                exchange=self.name,
            )
            for candle in ohlcv
        ]


def _is_non_retryable_download_error(error: Exception) -> bool:
    message = str(error or "").strip().lower()
    return any(marker in message for marker in _NON_RETRYABLE_DOWNLOAD_ERROR_MARKERS)


def _as_utc_naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _utc_now_naive() -> datetime:
    """Current UTC time as a naive datetime — matches `_as_utc_naive` output for stored timestamps."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


@dataclass
class DownloadProgress:
    """Tracks the state of one historical download."""

    exchange: str
    symbol: str
    timeframe: str
    estimated_total_candles: int
    total_candles: int
    downloaded_candles: int
    start_time: datetime
    end_time: datetime
    current_time: datetime
    is_complete: bool = False
    progress_pct: float = 0.0
    pages_fetched: int = 0
    retry_count: int = 0
    consecutive_errors: int = 0
    last_error: str = ""
    status: str = "pending"
    message: str = ""
    started_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    last_success_at: Optional[datetime] = None


class HistoricalDataManager:
    """Historical data manager."""

    def __init__(self):
        self._download_tasks: Dict[str, DownloadProgress] = {}

    @staticmethod
    def _estimate_total_candles(start_time: datetime, end_time: datetime, timeframe: str) -> int:
        tf = str(timeframe or "1m").strip()
        if len(tf) < 2:
            return 0

        unit = tf[-1]
        try:
            value = max(1, int(tf[:-1]))
        except Exception:
            return 0

        seconds_per_bar = 0
        if unit == "s":
            seconds_per_bar = value
        elif unit == "m":
            seconds_per_bar = value * 60
        elif unit == "h":
            seconds_per_bar = value * 3600
        elif unit == "d":
            seconds_per_bar = value * 86400
        elif unit == "w":
            seconds_per_bar = value * 7 * 86400
        elif unit == "M":
            seconds_per_bar = value * 30 * 86400

        if seconds_per_bar <= 0:
            return 0

        total_seconds = max(0.0, (end_time - start_time).total_seconds())
        if total_seconds <= 0:
            return 1
        return max(1, int(total_seconds // seconds_per_bar) + 1)

    @staticmethod
    async def _emit_progress(
        callback: Optional[Callable[[DownloadProgress], Awaitable[None] | None]],
        progress: DownloadProgress,
    ) -> None:
        if not callback:
            return
        result = callback(progress)
        if asyncio.iscoroutine(result):
            await result

    async def download_historical_klines(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        save_to_parquet: bool = True,
        progress_callback: Optional[Callable[[DownloadProgress], Awaitable[None] | None]] = None,
    ) -> List[Kline]:
        """
        Download historical klines.
        """
        connector = exchange_manager.get_exchange(exchange)
        public_connector: Optional[_PublicCCXTKlineConnector] = None
        if not connector:
            logger.warning(
                f"Exchange connector unavailable for {exchange}; using public ccxt OHLCV client"
            )
            public_connector = _PublicCCXTKlineConnector(exchange)
            connector = public_connector

        if end_time is None:
            end_time = _utc_now_naive()
        if start_time is None:
            start_time = end_time - timedelta(days=365)
        start_time = _as_utc_naive(start_time)
        end_time = _as_utc_naive(end_time)

        task_id = f"{exchange}_{symbol}_{timeframe}"
        started_at = _utc_now_naive()
        progress = DownloadProgress(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            estimated_total_candles=self._estimate_total_candles(start_time, end_time, timeframe),
            total_candles=0,
            downloaded_candles=0,
            start_time=start_time,
            end_time=end_time,
            current_time=start_time,
            progress_pct=0.0,
            status="running",
            message="下载已启动",
            started_at=started_at,
            updated_at=started_at,
        )
        self._download_tasks[task_id] = progress

        all_klines: List[Kline] = []
        current_time = start_time

        logger.info(f"Starting download: {task_id} from {start_time} to {end_time}")
        await self._emit_progress(progress_callback, progress)

        while current_time < end_time:
            try:
                klines = await asyncio.wait_for(
                    connector.get_klines(
                        symbol=symbol,
                        timeframe=timeframe,
                        since=current_time,
                        limit=settings.MAX_CANDLES_PER_REQUEST,
                    ),
                    timeout=_DOWNLOAD_REQUEST_TIMEOUT_SEC,
                )

                if not klines:
                    progress.current_time = min(current_time, end_time)
                    progress.updated_at = _utc_now_naive()
                    progress.last_error = ""
                    progress.message = "上游未返回更多K线，下载结束"
                    await self._emit_progress(progress_callback, progress)
                    break

                last_timestamp = _as_utc_naive(klines[-1].timestamp)
                next_time = last_timestamp + timedelta(milliseconds=1)
                if next_time <= current_time:
                    if all_klines:
                        progress.current_time = min(current_time, end_time)
                        progress.updated_at = _utc_now_naive()
                        progress.last_error = ""
                        progress.message = "Upstream repeated the latest candle; download is current"
                        await self._emit_progress(progress_callback, progress)
                        break
                    raise RuntimeError(
                        f"下载未向前推进，最后K线时间 {last_timestamp.isoformat()}，当前游标 {current_time.isoformat()}"
                    )

                filtered_klines = [
                    k for k in klines
                    if start_time <= _as_utc_naive(k.timestamp) <= end_time
                ]
                all_klines.extend(filtered_klines)

                now = _utc_now_naive()
                progress.downloaded_candles += len(filtered_klines)
                progress.current_time = min(last_timestamp, end_time)
                progress.pages_fetched += 1
                progress.consecutive_errors = 0
                progress.last_error = ""
                progress.status = "running"
                progress.last_success_at = now
                progress.updated_at = now
                progress.message = (
                    f"已抓取 {len(filtered_klines)} 根，本轮游标推进到 {progress.current_time.isoformat()}"
                )
                if progress.estimated_total_candles > 0:
                    progress.progress_pct = min(
                        99.5 if progress.current_time < end_time else 100.0,
                        (progress.downloaded_candles / progress.estimated_total_candles) * 100.0,
                    )

                current_time = next_time
                await self._emit_progress(progress_callback, progress)

                logger.debug(
                    f"Downloaded {len(klines)} candles, "
                    f"total: {len(all_klines)}, "
                    f"current: {last_timestamp}"
                )

                if last_timestamp >= end_time:
                    break

                await asyncio.sleep(0.5)

            except Exception as e:
                now = _utc_now_naive()
                if _is_non_retryable_download_error(e):
                    progress.last_error = str(e)
                    progress.status = "failed"
                    progress.message = f"Non-retryable download error: {progress.last_error}"
                    progress.finished_at = now
                    progress.updated_at = now
                    await self._emit_progress(progress_callback, progress)
                    logger.error(f"Download non-retryable error for {task_id}: {e}")
                    if public_connector is not None:
                        await public_connector.close()
                    raise RuntimeError(
                        f"{symbol} {timeframe} download failed without retry: {progress.last_error}"
                    ) from e

                progress.retry_count += 1
                progress.consecutive_errors += 1
                progress.last_error = str(e)
                progress.updated_at = now
                progress.status = "running"
                progress.message = (
                    f"下载异常，正在重试 {progress.consecutive_errors}/{_DOWNLOAD_MAX_CONSECUTIVE_ERRORS}: {progress.last_error}"
                )
                await self._emit_progress(progress_callback, progress)
                logger.error(f"Download error for {task_id}: {e}")

                if progress.consecutive_errors >= _DOWNLOAD_MAX_CONSECUTIVE_ERRORS:
                    progress.status = "failed"
                    progress.message = f"连续失败 {progress.consecutive_errors} 次，下载终止"
                    progress.finished_at = now
                    progress.updated_at = now
                    await self._emit_progress(progress_callback, progress)
                    if public_connector is not None:
                        await public_connector.close()
                    raise RuntimeError(
                        f"{symbol} {timeframe} 下载失败，连续重试 {progress.consecutive_errors} 次后仍未恢复：{progress.last_error}"
                    ) from e

                await asyncio.sleep(min(10.0, _DOWNLOAD_RETRY_SLEEP_SEC * progress.consecutive_errors))
                continue

        unique_klines = self._deduplicate_klines(all_klines)

        if save_to_parquet and unique_klines:
            await data_storage.save_klines_to_parquet(
                unique_klines,
                exchange,
                symbol,
                timeframe,
            )

        finished_at = _utc_now_naive()
        progress.is_complete = True
        progress.total_candles = len(unique_klines)
        progress.downloaded_candles = len(unique_klines)
        progress.current_time = end_time
        progress.progress_pct = 100.0
        progress.status = "completed"
        progress.message = f"下载完成，共 {len(unique_klines)} 根K线"
        progress.last_error = ""
        progress.finished_at = finished_at
        progress.updated_at = finished_at

        logger.info(
            f"Download complete: {task_id}, "
            f"total candles: {len(unique_klines)}"
        )
        await self._emit_progress(progress_callback, progress)
        if public_connector is not None:
            await public_connector.close()

        return unique_klines

    def _deduplicate_klines(self, klines: List[Kline]) -> List[Kline]:
        """Deduplicate kline data by timestamp."""
        seen = set()
        unique = []

        for kline in klines:
            key = kline.timestamp.isoformat()
            if key not in seen:
                seen.add(key)
                unique.append(kline)

        return sorted(unique, key=lambda x: x.timestamp)

    async def download_multiple_symbols(
        self,
        exchange: str,
        symbols: List[str],
        timeframe: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Dict[str, List[Kline]]:
        """Batch download historical data for multiple symbols."""
        results = {}

        for symbol in symbols:
            try:
                klines = await self.download_historical_klines(
                    exchange=exchange,
                    symbol=symbol,
                    timeframe=timeframe,
                    start_time=start_time,
                    end_time=end_time,
                )
                results[symbol] = klines

                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"Failed to download {symbol}: {e}")
                results[symbol] = []

        return results

    async def update_historical_data(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
    ) -> int:
        """
        Incrementally update historical data.
        """
        df = await data_storage.load_klines_from_parquet(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )

        if df.empty:
            start_time = None
        else:
            start_time = df.index.max() + timedelta(milliseconds=1)

        new_klines = await self.download_historical_klines(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            start_time=start_time,
            end_time=_utc_now_naive(),
        )

        return len(new_klines)

    async def update_all_data(
        self,
        exchanges: Optional[List[str]] = None,
    ) -> Dict[str, Dict[str, int]]:
        """
        Update all configured exchanges.
        """
        if exchanges is None:
            exchanges = exchange_manager.get_connected_exchanges()

        results = {}

        for exchange in exchanges:
            symbols = exchange_manager.get_supported_symbols(exchange)
            timeframes = ["1h", "4h", "1d"]
            results[exchange] = {}

            for symbol in symbols:
                for timeframe in timeframes:
                    try:
                        count = await self.update_historical_data(
                            exchange=exchange,
                            symbol=symbol,
                            timeframe=timeframe,
                        )
                        results[exchange][f"{symbol}_{timeframe}"] = count

                    except Exception as e:
                        logger.error(f"Update failed: {exchange} {symbol} {timeframe}: {e}")
                        results[exchange][f"{symbol}_{timeframe}"] = 0

        return results

    def get_download_progress(self, task_id: str) -> Optional[DownloadProgress]:
        """Get one download progress snapshot."""
        return self._download_tasks.get(task_id)

    def list_download_tasks(self) -> List[Dict[str, Any]]:
        """List tracked download tasks."""
        return [
            {
                "task_id": task_id,
                "exchange": task.exchange,
                "symbol": task.symbol,
                "timeframe": task.timeframe,
                "downloaded": task.downloaded_candles,
                "is_complete": task.is_complete,
                "status": task.status,
                "progress_pct": task.progress_pct,
                "message": task.message,
            }
            for task_id, task in self._download_tasks.items()
        ]

    async def get_data_coverage(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
    ) -> Dict[str, Any]:
        """Return basic local coverage stats."""
        df = await data_storage.load_klines_from_parquet(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )

        if df.empty:
            return {
                "has_data": False,
                "start": None,
                "end": None,
                "count": 0,
                "days": 0,
            }

        return {
            "has_data": True,
            "start": df.index.min().isoformat(),
            "end": df.index.max().isoformat(),
            "count": len(df),
            "days": (df.index.max() - df.index.min()).days,
        }


historical_data_manager = HistoricalDataManager()
