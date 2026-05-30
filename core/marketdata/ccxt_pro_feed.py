"""Real-time market-data feed backed by ccxt.pro WebSocket streams.

This replaces the REST polling fan-out in ``web.main._emit_market_ticks`` when
``MARKET_WS_ENABLED`` is set. Instead of asking each exchange for a ticker
every few seconds (which costs an HTTP round-trip and rate-limit budget per
call), we open one persistent WebSocket per exchange and let the venue *push*
ticker updates as prices move.

Design notes
------------
* **Public streams only.** Ticker/trade/ohlcv channels are unauthenticated, so
  we build the ccxt.pro client *without* API keys. This sidesteps signature /
  clock-skew failures and keeps the feed independent from the trading
  connectors managed by ``exchange_manager``.
* **One task per exchange.** Each runs its own ``watch_tickers`` loop with
  exponential-backoff reconnect, so a flaky venue cannot stall the others.
* **Same payload shape** as the old REST path
  (``{exchange: {symbol: {last, bid, ask, timestamp}}}``) so the websocket
  front-end (``applyMarketTick``) needs no changes.
* **Health beacon.** ``is_healthy()`` reports whether a fresh tick arrived
  recently; ``web.main`` uses it to decide whether REST backfill is still
  needed, giving an automatic fallback when the socket is down.
"""
from __future__ import annotations

import asyncio
import contextlib
import time
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional

from loguru import logger

try:  # ccxt.pro ships inside ccxt>=4 but guard anyway.
    import ccxt.pro as ccxtpro  # type: ignore

    CCXT_PRO_AVAILABLE = True
except Exception:  # pragma: no cover - environment without ws support
    ccxtpro = None  # type: ignore
    CCXT_PRO_AVAILABLE = False

from config.exchanges import get_exchange_config
from config.settings import settings


# Async callback: receives {exchange: {symbol: {last, bid, ask, timestamp}}}.
TickCallback = Callable[[Dict[str, Dict[str, Any]]], Awaitable[None]]
# Sync callable returning the current list of symbols to watch.
SymbolsProvider = Callable[[], List[str]]


async def _cancel_task(task: "asyncio.Future") -> None:
    """Cancel and await a task, swallowing the resulting CancelledError."""
    if task is not None and not task.done():
        task.cancel()
        with contextlib.suppress(BaseException):
            await task


def _consume_task_result(task: asyncio.Future) -> None:
    with contextlib.suppress(BaseException):
        task.result()


async def _await_close_result(result: Any) -> None:
    if result is None:
        return
    if asyncio.iscoroutine(result):
        task = asyncio.create_task(result)
    elif isinstance(result, asyncio.Future):
        task = result
    else:
        return
    task.add_done_callback(_consume_task_result)
    with contextlib.suppress(BaseException):
        await asyncio.shield(task)


async def _close_client_safely(client: Any) -> None:
    """Close ccxt/ccxt.pro clients and any leaked aiohttp session fallback."""
    close = getattr(client, "close", None)
    if callable(close):
        with contextlib.suppress(BaseException):
            await _await_close_result(close())

    for attr in ("session", "aiohttp_session", "client_session"):
        session = getattr(client, attr, None)
        if session is None or bool(getattr(session, "closed", False)):
            continue
        session_close = getattr(session, "close", None)
        if callable(session_close):
            with contextlib.suppress(BaseException):
                await _await_close_result(session_close())


def _proxy_url() -> Optional[str]:
    raw = str(
        getattr(settings, "HTTP_PROXY", "")
        or getattr(settings, "HTTPS_PROXY", "")
        or ""
    ).strip()
    return raw or None


class CcxtProMarketFeed:
    """Manage one ccxt.pro WebSocket per exchange and fan ticks to a callback."""

    def __init__(
        self,
        *,
        on_tick: TickCallback,
        symbols_provider: SymbolsProvider,
        exchanges: Optional[List[str]] = None,
        watch_timeout_sec: float = 25.0,
        reconnect_min_sec: float = 1.0,
        reconnect_max_sec: float = 30.0,
        health_max_age_sec: float = 15.0,
    ) -> None:
        self._on_tick = on_tick
        self._symbols_provider = symbols_provider
        self._exchanges = [str(e).strip().lower() for e in (exchanges or []) if str(e).strip()]
        self._watch_timeout_sec = max(5.0, float(watch_timeout_sec))
        self._reconnect_min_sec = max(0.5, float(reconnect_min_sec))
        self._reconnect_max_sec = max(self._reconnect_min_sec, float(reconnect_max_sec))
        self._health_max_age_sec = max(2.0, float(health_max_age_sec))
        # Per-exchange monotonic timestamp of the last successful push.
        self._last_push_monotonic: Dict[str, float] = {}
        self._clients: Dict[str, Any] = {}
        self._watch_attempt_count: Dict[str, int] = {}
        self._watch_timeout_count: Dict[str, int] = {}
        self._watch_error_count: Dict[str, int] = {}
        self._watch_empty_count: Dict[str, int] = {}
        self._last_error: Dict[str, str] = {}
        self._last_symbols: Dict[str, List[str]] = {}
        self._last_watch_started_at: Dict[str, str] = {}
        self._last_watch_completed_at: Dict[str, str] = {}
        self._last_watch_timeout_at: Dict[str, str] = {}
        self._last_watch_started_monotonic: Dict[str, float] = {}
        self._last_payload_symbol_count: Dict[str, int] = {}
        self._client_default_type: Dict[str, str] = {}
        self._client_rest_default_type: Dict[str, str] = {}

    # ── public API ────────────────────────────────────────────────────────
    def is_healthy(self, *, max_age_sec: Optional[float] = None) -> bool:
        """True if any exchange pushed a tick within the freshness window."""
        if not self._last_push_monotonic:
            return False
        horizon = float(max_age_sec if max_age_sec is not None else self._health_max_age_sec)
        now = time.monotonic()
        return any((now - ts) <= horizon for ts in self._last_push_monotonic.values())

    def healthy_exchanges(self, *, max_age_sec: Optional[float] = None) -> List[str]:
        horizon = float(max_age_sec if max_age_sec is not None else self._health_max_age_sec)
        now = time.monotonic()
        return [
            name
            for name, ts in self._last_push_monotonic.items()
            if (now - ts) <= horizon
        ]

    def status_snapshot(self) -> Dict[str, Any]:
        """Return feed-loop diagnostics for status APIs and smoke checks."""
        now = time.monotonic()
        exchanges: Dict[str, Dict[str, Any]] = {}
        for name in self._exchanges:
            last_push = self._last_push_monotonic.get(name)
            last_started = self._last_watch_started_monotonic.get(name)
            exchanges[name] = {
                "healthy": bool(last_push is not None and (now - last_push) <= self._health_max_age_sec),
                "last_push_age_ms": round((now - last_push) * 1000.0, 3) if last_push is not None else None,
                "watch_attempt_count": int(self._watch_attempt_count.get(name, 0)),
                "watch_timeout_count": int(self._watch_timeout_count.get(name, 0)),
                "watch_error_count": int(self._watch_error_count.get(name, 0)),
                "watch_empty_count": int(self._watch_empty_count.get(name, 0)),
                "last_error": self._last_error.get(name),
                "last_symbols": list(self._last_symbols.get(name, [])),
                "last_watch_started_at": self._last_watch_started_at.get(name),
                "last_watch_completed_at": self._last_watch_completed_at.get(name),
                "last_watch_timeout_at": self._last_watch_timeout_at.get(name),
                "active_watch_age_ms": (
                    round((now - last_started) * 1000.0, 3) if last_started is not None else None
                ),
                "last_payload_symbol_count": int(self._last_payload_symbol_count.get(name, 0)),
                "default_type": self._client_default_type.get(name),
                "rest_default_type": self._client_rest_default_type.get(name),
            }
        errors = [err for err in self._last_error.values() if err]
        return {
            "exchange_count": len(self._exchanges),
            "watch_attempt_count": sum(self._watch_attempt_count.values()),
            "watch_timeout_count": sum(self._watch_timeout_count.values()),
            "watch_error_count": sum(self._watch_error_count.values()),
            "watch_empty_count": sum(self._watch_empty_count.values()),
            "last_error": errors[-1] if errors else None,
            "exchanges": exchanges,
        }

    async def run(self, stop_event: asyncio.Event) -> None:
        """Run all per-exchange watch loops until ``stop_event`` is set."""
        if not CCXT_PRO_AVAILABLE:
            logger.warning(
                "ccxt_pro_feed: ccxt.pro unavailable — WS market feed disabled, "
                "REST polling remains the source of ticks"
            )
            return
        if not self._exchanges:
            logger.warning("ccxt_pro_feed: no exchanges configured, nothing to stream")
            return

        logger.info(
            "ccxt_pro_feed: starting WS market feed for {}",
            ", ".join(self._exchanges),
        )
        tasks = [
            asyncio.create_task(
                self._run_one_exchange(name, stop_event),
                name=f"ws_feed_{name}",
            )
            for name in self._exchanges
        ]
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
                    with contextlib.suppress(BaseException):
                        await task
            await self._close_all_clients()
            logger.info("ccxt_pro_feed: WS market feed stopped")

    # ── internals ─────────────────────────────────────────────────────────
    def _build_client(self, name: str) -> Optional[Any]:
        factory = getattr(ccxtpro, name, None)
        if factory is None:
            logger.warning(f"ccxt_pro_feed: ccxt.pro has no exchange '{name}'")
            return None

        cfg = get_exchange_config(name)
        rest_default_type = "spot"
        if cfg is not None:
            # Mirror the REST connector's market type so unified symbols resolve
            # to the same instrument (perp vs spot) the strategies trade.
            rest_default_type = str(
                getattr(settings, f"{name.upper()}_DEFAULT_TYPE", cfg.default_type)
                or cfg.default_type
                or "spot"
            )
        default_type = self._ws_default_type(name, rest_default_type)
        self._client_rest_default_type[name] = rest_default_type
        self._client_default_type[name] = default_type
        if default_type != rest_default_type:
            logger.info(
                "ccxt_pro_feed[{}]: using ccxt.pro defaultType={} for REST defaultType={}",
                name,
                default_type,
                rest_default_type,
            )

        options: Dict[str, Any] = {
            "enableRateLimit": True,
            "options": {"defaultType": default_type},
            # newUpdates=True makes watch_tickers return only the symbols that
            # actually changed on each wake, instead of the full snapshot.
            "newUpdates": True,
        }
        proxy = _proxy_url()
        client = factory(options)
        if proxy:
            with contextlib.suppress(Exception):
                client.aiohttp_proxy = proxy
            with contextlib.suppress(Exception):
                client.ws_proxy = proxy
        return client

    async def _run_one_exchange(self, name: str, stop_event: asyncio.Event) -> None:
        backoff = self._reconnect_min_sec
        while not stop_event.is_set():
            client = self._clients.get(name)
            if client is None:
                client = self._build_client(name)
                if client is None:
                    return  # unrecoverable: this exchange has no ws support
                self._clients[name] = client
                with contextlib.suppress(Exception):
                    await client.load_markets()

            symbols = self._current_symbols()
            if not symbols:
                # No symbols to watch right now — re-check shortly.
                await self._sleep_or_stop(stop_event, 2.0)
                continue

            # Race the watch against the stop signal so shutdown is immediate
            # rather than blocked for up to watch_timeout_sec inside the socket.
            self._watch_attempt_count[name] = self._watch_attempt_count.get(name, 0) + 1
            self._last_symbols[name] = list(symbols)
            self._last_watch_started_at[name] = self._now_iso()
            self._last_watch_started_monotonic[name] = time.monotonic()
            watch_task = asyncio.ensure_future(client.watch_tickers(symbols))
            stop_task = asyncio.ensure_future(stop_event.wait())
            done, _pending = await asyncio.wait(
                {watch_task, stop_task},
                timeout=self._watch_timeout_sec,
                return_when=asyncio.FIRST_COMPLETED,
            )

            if stop_event.is_set():
                await _cancel_task(watch_task)
                await _cancel_task(stop_task)
                break
            if watch_task not in done:
                # Timed out with no update — normal in quiet markets. Cancel the
                # in-flight watch and loop to re-evaluate symbols.
                self._watch_timeout_count[name] = self._watch_timeout_count.get(name, 0) + 1
                self._last_watch_timeout_at[name] = self._now_iso()
                logger.debug(
                    "ccxt_pro_feed[{}]: watch_tickers timeout after {:.1f}s for {}",
                    name,
                    self._watch_timeout_sec,
                    symbols,
                )
                await _cancel_task(watch_task)
                await _cancel_task(stop_task)
                continue

            await _cancel_task(stop_task)
            try:
                tickers = watch_task.result()
                self._last_watch_completed_at[name] = self._now_iso()
                backoff = self._reconnect_min_sec  # healthy → reset backoff
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._watch_error_count[name] = self._watch_error_count.get(name, 0) + 1
                self._last_error[name] = f"{type(exc).__name__}: {exc}"
                logger.debug(f"ccxt_pro_feed[{name}]: watch error: {exc}; reconnecting in {backoff:.1f}s")
                await self._reset_client(name)
                await self._sleep_or_stop(stop_event, backoff)
                backoff = min(self._reconnect_max_sec, backoff * 2.0)
                continue

            payload = self._normalize_tickers(name, tickers)
            self._last_payload_symbol_count[name] = len(payload)
            if not payload:
                self._watch_empty_count[name] = self._watch_empty_count.get(name, 0) + 1
                continue
            self._last_push_monotonic[name] = time.monotonic()
            self._last_error.pop(name, None)
            try:
                await self._on_tick({name: payload})
            except Exception as exc:  # never let a consumer error kill the loop
                logger.debug(f"ccxt_pro_feed[{name}]: on_tick callback failed: {exc}")

    def _current_symbols(self) -> List[str]:
        try:
            syms = self._symbols_provider() or []
        except Exception:
            return []
        # De-dup while preserving order; cap to a sane number of streams.
        seen: Dict[str, None] = {}
        for s in syms:
            key = str(s or "").strip()
            if key and key not in seen:
                seen[key] = None
        return list(seen.keys())[:16]

    @staticmethod
    def _ws_default_type(exchange: str, default_type: str) -> str:
        """Translate REST connector market type to ccxt.pro's WS type naming."""
        name = str(exchange or "").strip().lower()
        kind = str(default_type or "spot").strip().lower()
        if name == "binance" and kind in {"future", "futures", "perp", "perpetual"}:
            # ccxt REST configs often use "future" for Binance USD-M perpetuals,
            # while ccxt.pro public ticker streams return promptly under "swap".
            return "swap"
        return kind or "spot"

    @staticmethod
    def _spot_style_symbol(symbol: str) -> str:
        """Strip the ``:SETTLEMENT`` suffix so perp symbols match the UI keys.

        ccxt resolves "BTC/USDT" on a futures client to the unified perp symbol
        "BTC/USDT:USDT". The REST path and the front-end key ticks by the
        plain "BTC/USDT" form, so normalize back to that to keep them aligned.
        """
        s = str(symbol or "")
        return s.split(":", 1)[0] if ":" in s else s

    @classmethod
    def _normalize_tickers(cls, name: str, tickers: Any) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        if not isinstance(tickers, dict):
            return out
        for symbol, t in tickers.items():
            if not isinstance(t, dict):
                continue
            last = t.get("last") or t.get("close") or 0.0
            try:
                last_f = float(last or 0.0)
            except (TypeError, ValueError):
                continue
            if last_f <= 0:
                continue
            ts_ms = t.get("timestamp")
            if isinstance(ts_ms, (int, float)) and ts_ms > 0:
                ts_iso = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
            else:
                ts_iso = datetime.now(timezone.utc).isoformat()
            out[cls._spot_style_symbol(symbol)] = {
                "last": last_f,
                "bid": float(t.get("bid") or 0.0),
                "ask": float(t.get("ask") or 0.0),
                "timestamp": ts_iso,
            }
        return out

    async def _reset_client(self, name: str) -> None:
        client = self._clients.pop(name, None)
        if client is not None:
            await _close_client_safely(client)

    async def _close_all_clients(self) -> None:
        for name in list(self._clients.keys()):
            await self._reset_client(name)

    @staticmethod
    async def _sleep_or_stop(stop_event: asyncio.Event, seconds: float) -> None:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop_event.wait(), timeout=max(0.0, seconds))

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()
