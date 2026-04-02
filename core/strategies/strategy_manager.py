"""Strategy manager: registration, runtime orchestration, and signal dispatch."""
import asyncio
import contextlib
import inspect
import re
import statistics
import time
from dataclasses import dataclass, field
from decimal import Decimal
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Type

import pandas as pd
from loguru import logger

from config.settings import settings
from core.data.data_storage import data_storage
from core.exchanges import exchange_manager
from core.strategies.strategy_base import Signal, StrategyBase

_SUB_MINUTE_TIMEFRAMES = {"1s", "5s", "10s", "30s"}
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
    "1w": "1W",
    "1M": "1MS",
}


@dataclass
class StrategyConfig:
    name: str
    strategy_class: Type[StrategyBase]
    params: Dict[str, Any]
    symbols: List[str]
    timeframe: str
    enabled: bool = True
    exchange: str = "gate"
    allocation: float = settings.DEFAULT_STRATEGY_ALLOCATION
    runtime_limit_minutes: Optional[int] = None


@dataclass
class StrategyRuntimeStats:
    run_count: int = 0
    signal_count: int = 0
    error_count: int = 0
    last_run_at: Optional[datetime] = None
    last_signal_at: Optional[datetime] = None
    last_error_at: Optional[datetime] = None
    last_error: Optional[str] = None
    avg_cycle_ms: float = 0.0
    total_cycle_ms: float = 0.0


_SIGNAL_CONFLICT_WINDOW_SECONDS = 60


class StrategyManager:
    def __init__(self):
        self._strategies: Dict[str, StrategyBase] = {}
        self._configs: Dict[str, StrategyConfig] = {}
        self._signal_callbacks: List[callable] = []
        self._running: bool = False
        self._strategy_tasks: Dict[str, asyncio.Task] = {}
        self._last_run_at: Dict[str, datetime] = {}
        self._stats: Dict[str, StrategyRuntimeStats] = {}
        self._running_since: Dict[str, datetime] = {}
        self._runtime_deadlines: Dict[str, datetime] = {}
        # symbol -> most recent Signal, used for conflict detection
        self._recent_signal_by_symbol: Dict[str, Signal] = {}
        # Shared market data cache: (exchange, symbol, timeframe, limit) -> (df, timestamp)
        # TTL is dynamic per timeframe to keep sub-minute strategies responsive while
        # still avoiding redundant loads when multiple strategies share the same feed.
        self._market_data_cache: Dict[Tuple, Tuple[pd.DataFrame, float]] = {}
        self._market_data_cache_max_ttl: float = 30.0
        # Strategy runtime should behave like an event-driven backtest: process each
        # completed bar once instead of re-running on the same forming candle.
        self._last_processed_bar_at: Dict[Tuple[str, str, str], pd.Timestamp] = {}

    def _ensure_strategy_account(self, name: str, params: Dict[str, Any]) -> None:
        from core.trading.account_manager import account_manager

        account_id = str(params.get("account_id") or self._default_strategy_account_id(name)).strip()
        if not account_id:
            return
        exchange = str(params.get("exchange") or "binance").strip().lower() or "binance"
        main_account = account_manager.get_account("main") or {}
        mode = str(main_account.get("mode") or settings.TRADING_MODE or "paper").strip().lower()
        existing = account_manager.get_account(account_id)
        payload = {
            "name": f"策略账户 {name}",
            "exchange": exchange,
            "mode": mode if mode in {"paper", "live"} else "paper",
            "parent_account_id": "main",
            "enabled": True,
            "metadata": {"strategy_name": name, "auto_created": True, "isolated": account_id != "main"},
        }
        try:
            if existing:
                account_manager.update_account(account_id, payload)
            else:
                account_manager.create_account(account_id=account_id, **payload)
        except Exception as e:
            logger.warning(f"Failed to ensure strategy account for {name}: {e}")

    def _timeframe_to_seconds(self, timeframe: str) -> int:
        if not timeframe:
            return 60
        try:
            unit = timeframe[-1]
            value = int(timeframe[:-1])
            if unit == "s":
                return max(5, value)
            if unit == "m":
                return max(5, value * 60)
            if unit == "h":
                return max(5, value * 3600)
            if unit == "d":
                return max(5, value * 86400)
            if unit == "w":
                return max(5, value * 7 * 86400)
            if unit == "M":
                return max(5, value * 30 * 86400)
            return 60
        except Exception:
            return 60

    def _bar_timeframe_to_seconds(self, timeframe: str) -> int:
        if not timeframe:
            return 60
        try:
            unit = timeframe[-1]
            value = max(1, int(timeframe[:-1]))
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
            return 60
        except Exception:
            return 60

    def _market_data_cache_ttl_for_timeframe(self, timeframe: str) -> float:
        tf_seconds = max(1, int(self._bar_timeframe_to_seconds(timeframe)))
        # Use at most one-sixth of the bar size, capped at 30s and floored at 1s.
        return float(max(1.0, min(self._market_data_cache_max_ttl, tf_seconds / 6.0)))

    @staticmethod
    def _naive_timestamp(value: Any) -> pd.Timestamp:
        ts = pd.Timestamp(value)
        if ts.tzinfo is not None:
            ts = ts.tz_localize(None)
        return ts

    def _drop_incomplete_last_bar(
        self,
        df: pd.DataFrame,
        timeframe: str,
        *,
        now: Optional[datetime] = None,
    ) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame() if df is None else df

        out = df.copy()
        out.index = pd.to_datetime(out.index)
        out = out[~out.index.duplicated(keep="last")].sort_index()
        if out.empty:
            return out

        tf_seconds = max(1, int(self._bar_timeframe_to_seconds(timeframe)))
        anchor = self._naive_timestamp(now or datetime.now())
        last_ts = self._naive_timestamp(out.index[-1])
        bar_close_at = last_ts + pd.Timedelta(seconds=tf_seconds)
        if anchor < bar_close_at:
            return out.iloc[:-1].copy()
        return out

    def _bar_state_key(self, strategy_name: str, symbol: str, timeframe: str) -> Tuple[str, str, str]:
        return (
            str(strategy_name or "").strip(),
            str(symbol or "").strip().upper(),
            str(timeframe or "").strip(),
        )

    def _has_new_completed_bar(
        self,
        strategy_name: str,
        symbol: str,
        timeframe: str,
        bar_timestamp: Any,
    ) -> bool:
        key = self._bar_state_key(strategy_name, symbol, timeframe)
        current = self._naive_timestamp(bar_timestamp)
        previous = self._last_processed_bar_at.get(key)
        if previous is None:
            return True
        return bool(current > self._naive_timestamp(previous))

    def _mark_bar_processed(
        self,
        strategy_name: str,
        symbol: str,
        timeframe: str,
        bar_timestamp: Any,
    ) -> None:
        key = self._bar_state_key(strategy_name, symbol, timeframe)
        self._last_processed_bar_at[key] = self._naive_timestamp(bar_timestamp)

    def _clear_bar_runtime_state(self, strategy_name: str) -> None:
        prefix = str(strategy_name or "").strip()
        stale_keys = [key for key in self._last_processed_bar_at.keys() if key[0] == prefix]
        for key in stale_keys:
            self._last_processed_bar_at.pop(key, None)

    async def _load_market_data(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        limit: int = 300,
    ) -> pd.DataFrame:
        cache_key = (exchange, symbol, timeframe, limit)
        now = time.monotonic()
        cached = self._market_data_cache.get(cache_key)
        cache_ttl = self._market_data_cache_ttl_for_timeframe(timeframe)
        if cached is not None:
            df_cached, ts = cached
            if now - ts < cache_ttl:
                return df_cached.copy()

        local_df = await data_storage.load_klines_from_parquet(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )

        df = local_df.copy() if not local_df.empty else pd.DataFrame()
        connector = exchange_manager.get_exchange(exchange)

        # Always try pulling latest bars so live strategy sees recent market.
        if connector:
            try:
                live_df = await self._load_live_market_data(
                    connector=connector,
                    symbol=symbol,
                    timeframe=timeframe,
                    limit=limit,
                )
                if not live_df.empty:
                    if df.empty:
                        df = live_df
                    else:
                        df = pd.concat([df, live_df])
                        df = df[~df.index.duplicated(keep="last")].sort_index()
            except Exception as e:
                logger.debug(
                    f"Failed to fetch live klines for {exchange} {symbol} {timeframe}: {e}"
                )

        if df.empty:
            for fallback in ["gate", "binance"]:
                if fallback == exchange:
                    continue
                df = await data_storage.load_klines_from_parquet(
                    exchange=fallback,
                    symbol=symbol,
                    timeframe=timeframe,
                )
                if not df.empty:
                    break

        if df.empty:
            return pd.DataFrame()

        df = self._drop_incomplete_last_bar(df, timeframe)
        if df.empty:
            return pd.DataFrame()

        result = df.tail(limit).copy()
        result["symbol"] = symbol
        self._market_data_cache[cache_key] = (result, time.monotonic())
        # Evict cache entries older than 2x TTL to prevent unbounded growth
        if len(self._market_data_cache) > 200:
            cutoff = time.monotonic() - self._market_data_cache_max_ttl * 2
            stale = [k for k, (_, t) in self._market_data_cache.items() if t < cutoff]
            for k in stale:
                del self._market_data_cache[k]
        return result.copy()

    @staticmethod
    def _df_from_klines(klines: List[Any]) -> pd.DataFrame:
        if not klines:
            return pd.DataFrame()
        frame = pd.DataFrame(
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
        )
        if frame.empty:
            return pd.DataFrame()
        frame["timestamp"] = pd.to_datetime(frame["timestamp"])
        return frame.set_index("timestamp").sort_index()

    @staticmethod
    def _trades_to_ohlcv_df(trades: List[Dict[str, Any]], timeframe: str) -> pd.DataFrame:
        if not trades:
            return pd.DataFrame()
        rule = _RESAMPLE_RULES.get(str(timeframe or "").strip())
        if not rule:
            return pd.DataFrame()

        rows: List[Dict[str, float]] = []
        for trade in trades:
            ts = trade.get("timestamp")
            price = trade.get("price")
            amount = trade.get("amount")
            if ts is None or price is None or amount is None:
                continue
            rows.append(
                {
                    "timestamp": datetime.fromtimestamp(float(ts) / 1000.0),
                    "price": float(price),
                    "amount": float(amount),
                }
            )

        if not rows:
            return pd.DataFrame()

        src = pd.DataFrame(rows).set_index("timestamp").sort_index()
        ohlc = src["price"].resample(rule).ohlc()
        volume = src["amount"].resample(rule).sum().rename("volume")
        merged = pd.concat([ohlc, volume], axis=1).dropna()
        merged.columns = ["open", "high", "low", "close", "volume"]
        return merged

    @staticmethod
    def _resample_ohlcv(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        rule = _RESAMPLE_RULES.get(str(timeframe or "").strip())
        if not rule:
            return pd.DataFrame()
        src = df.copy()
        src.index = pd.to_datetime(src.index)
        src = src.sort_index()
        ohlc = src[["open", "high", "low", "close"]].resample(rule).agg(
            {"open": "first", "high": "max", "low": "min", "close": "last"}
        )
        volume = src[["volume"]].resample(rule).sum()
        out = pd.concat([ohlc, volume], axis=1)
        return out.dropna(subset=["open", "high", "low", "close"])

    async def _load_live_market_data(
        self,
        connector: Any,
        symbol: str,
        timeframe: str,
        limit: int,
    ) -> pd.DataFrame:
        tf = str(timeframe or "").strip()
        client = getattr(connector, "_client", None)
        fetch_trades = getattr(client, "fetch_trades", None)
        supported_raw = getattr(client, "timeframes", None)

        supports_tf: Optional[bool] = None
        if isinstance(supported_raw, dict) and supported_raw:
            supports_tf = tf in supported_raw
        elif isinstance(supported_raw, (list, tuple, set)) and supported_raw:
            supports_tf = tf in set(supported_raw)

        # Fast path: direct exchange OHLCV only when timeframe is supported or unknown.
        if supports_tf is not False:
            try:
                live_klines = await connector.get_klines(symbol, timeframe, limit=limit)
                return self._df_from_klines(live_klines).tail(limit)
            except Exception as live_error:
                if supports_tf:
                    raise live_error

        # Fallback for unsupported sub-minute intervals (e.g. Binance 10s/30s).
        if tf in _SUB_MINUTE_TIMEFRAMES and callable(fetch_trades):
            trade_limit = max(1000, min(8000, int(limit) * 30))
            trades = await fetch_trades(symbol, limit=trade_limit)
            agg = self._trades_to_ohlcv_df(trades or [], tf)
            if not agg.empty:
                return agg.tail(limit)

        # Fallback for weekly/monthly from daily.
        if tf in {"1w", "1M"}:
            day_klines = await connector.get_klines(symbol, "1d", limit=max(limit * 7, 180))
            day_df = self._df_from_klines(day_klines)
            if not day_df.empty:
                agg = self._resample_ohlcv(day_df, tf)
                if not agg.empty:
                    return agg.tail(limit)

        return pd.DataFrame()

    def _stats_for(self, name: str) -> StrategyRuntimeStats:
        if name not in self._stats:
            self._stats[name] = StrategyRuntimeStats()
        return self._stats[name]

    @staticmethod
    def _split_symbol(symbol: str) -> tuple[str, str]:
        raw = str(symbol or "").strip().upper()
        if "/" in raw:
            base, quote = raw.split("/", 1)
            return base, quote
        if raw.endswith("USDT") and len(raw) > 4:
            return raw[:-4], "USDT"
        return (raw or "BTC"), "USDT"

    @staticmethod
    def _sanitize_account_id(raw: str) -> str:
        text = str(raw or "").strip().lower()
        if not text:
            return "main"
        text = re.sub(r"[^a-z0-9_-]+", "_", text)
        text = re.sub(r"_+", "_", text).strip("_")
        if not text:
            return "main"
        if len(text) > 48:
            text = text[:48].rstrip("_")
        return text or "main"

    def _default_strategy_account_id(self, name: str) -> str:
        return self._sanitize_account_id(f"strategy_{name}")

    def _strategy_account_id(self, name: str) -> str:
        cfg = self._configs.get(name)
        if cfg:
            raw = cfg.params.get("account_id")
            if raw:
                return self._sanitize_account_id(str(raw))
        return self._default_strategy_account_id(name)

    async def _run_async_strategy(self, strategy: StrategyBase, symbol: str, config: StrategyConfig) -> List[Signal]:
        async_method = getattr(strategy, "generate_signals_async", None)
        if not callable(async_method):
            return []

        required_count = 1
        try:
            sig = inspect.signature(async_method)
            params = list(sig.parameters.values())
            required = [
                p for p in params
                if p.kind in (
                    inspect.Parameter.POSITIONAL_ONLY,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                )
                and p.default is inspect._empty
            ]
            required_count = len(required)
        except Exception:
            required_count = 1

        if required_count <= 1:
            result = await async_method(symbol)
            return result or []

        base, quote = self._split_symbol(symbol)
        amount = Decimal(str(config.params.get("test_amount", 1.0)))

        if required_count == 2:
            result = await async_method(base, quote)
            return result or []

        result = await async_method(base, quote, amount)
        return result or []

    async def _emit_signals(self, strategy_name: str, signals: List[Signal]) -> None:
        strategy = self._strategies.get(strategy_name)
        if not strategy:
            return

        config = self._configs.get(strategy_name)
        default_exchange = str((config.exchange if config else "binance") or "binance")
        account_id = self._strategy_account_id(strategy_name)
        stats = self._stats_for(strategy_name)
        if signals:
            stats.signal_count += len(signals)
            stats.last_signal_at = datetime.now(timezone.utc)

        for signal in signals:
            # Conflict detection: drop weaker conflicting signals within the window
            prior = self._recent_signal_by_symbol.get(signal.symbol)
            if prior is not None:
                age = (signal.timestamp - prior.timestamp).total_seconds()
                if age <= _SIGNAL_CONFLICT_WINDOW_SECONDS:
                    prior_side = prior.signal_type.value
                    new_side = signal.signal_type.value
                    buy_sides = {"buy", "close_short"}
                    sell_sides = {"sell", "close_long"}
                    is_conflict = (prior_side in buy_sides and new_side in sell_sides) or (
                        prior_side in sell_sides and new_side in buy_sides
                    )
                    if is_conflict:
                        if signal.strength <= prior.strength:
                            logger.warning(
                                f"Signal conflict for {signal.symbol}: dropping {new_side} "
                                f"(strength={signal.strength:.2f}) vs existing {prior_side} "
                                f"(strength={prior.strength:.2f})"
                            )
                            continue
                        else:
                            logger.warning(
                                f"Signal conflict for {signal.symbol}: replacing {prior_side} "
                                f"(strength={prior.strength:.2f}) with stronger {new_side} "
                                f"(strength={signal.strength:.2f})"
                            )
            self._recent_signal_by_symbol[signal.symbol] = signal

            execution_signal_dispatched = False
            meta = dict(signal.metadata or {})
            meta.setdefault("account_id", account_id)
            meta.setdefault("exchange", default_exchange)
            meta.setdefault("source", "strategy")
            meta.setdefault("is_strategy_isolated", True)
            if config:
                meta.setdefault("timeframe", str(config.timeframe or ""))
            signal.metadata = meta
            strategy.add_signal_to_history(signal)
            for callback in self._signal_callbacks:
                try:
                    await callback(signal)
                    cb_name = str(getattr(callback, "__name__", "") or "")
                    cb_self = getattr(callback, "__self__", None)
                    if cb_name == "submit_signal" or cb_self.__class__.__name__ == "ExecutionEngine":
                        execution_signal_dispatched = True
                except Exception as e:
                    logger.error(f"Signal callback error: {e}")

            if not execution_signal_dispatched:
                try:
                    from core.trading.execution_engine import execution_engine

                    await execution_engine.submit_signal(signal)
                    execution_signal_dispatched = True
                    logger.warning(
                        f"Execution signal callback missing, fallback routed directly: "
                        f"{strategy_name} {signal.signal_type.value} {signal.symbol}"
                    )
                except Exception as e:
                    logger.error(f"Fallback strategy signal dispatch failed: {e}")

    async def _close_positions_for_strategy_stop(self, name: str, reason: str = "strategy_stopped") -> Dict[str, Any]:
        from core.strategies import Signal, SignalType
        from core.trading.execution_engine import execution_engine
        from core.trading.position_manager import PositionSide, position_manager

        positions = list(position_manager.get_positions_by_strategy(name) or [])
        if not positions:
            return {"requested": 0, "closed": 0, "failed": 0}

        closed = 0
        failed = 0
        for pos in positions:
            close_signal = Signal(
                symbol=str(pos.symbol),
                signal_type=(SignalType.CLOSE_LONG if pos.side == PositionSide.LONG else SignalType.CLOSE_SHORT),
                price=float(pos.current_price or pos.entry_price or 0.0),
                timestamp=datetime.now(timezone.utc),
                strategy_name=name,
                strength=1.0,
                quantity=float(pos.quantity or 0.0),
                metadata={
                    "exchange": str(pos.exchange or "binance"),
                    "account_id": str(pos.account_id or "main"),
                    "source": "strategy_stop_close",
                    "close_reason": reason,
                },
            )
            try:
                result = await execution_engine.execute_signal(close_signal)
                if result:
                    closed += 1
                else:
                    failed += 1
            except Exception as exc:
                failed += 1
                logger.error(f"Auto-close on strategy stop failed: strategy={name} symbol={pos.symbol} error={exc}")
        return {"requested": len(positions), "closed": closed, "failed": failed}

    async def _run_strategy_once(self, name: str) -> None:
        strategy = self._strategies.get(name)
        config = self._configs.get(name)
        if not strategy or not config or not strategy.is_running:
            return

        stats = self._stats_for(name)
        cycle_start = datetime.now(timezone.utc)
        stats.run_count += 1
        stats.last_run_at = cycle_start
        self._last_run_at[name] = cycle_start

        symbols = config.symbols or ["BTC/USDT"]
        required = strategy.get_required_data()
        min_length = int(required.get("min_length", 100))
        data_limit = max(120, min_length + 20)
        requires_pair = bool(required.get("requires_pair", False))

        for symbol in symbols:
            try:
                if hasattr(strategy, "generate_signals_async"):
                    signals = await self._run_async_strategy(
                        strategy=strategy,
                        symbol=symbol,
                        config=config,
                    )
                    if signals:
                        await self._emit_signals(name, signals)
                    continue

                df = await self._load_market_data(
                    exchange=config.exchange,
                    symbol=symbol,
                    timeframe=config.timeframe,
                    limit=data_limit,
                )
                if df.empty:
                    continue
                latest_bar_at = pd.Timestamp(df.index[-1])
                if not self._has_new_completed_bar(name, symbol, config.timeframe, latest_bar_at):
                    continue

                if requires_pair:
                    pair_symbol = str(config.params.get("pair_symbol", "")).strip().upper()
                    if not pair_symbol:
                        pair_symbol = next(
                            (str(x).upper() for x in symbols if str(x).upper() != str(symbol).upper()),
                            "",
                        )
                    if not pair_symbol:
                        pair_symbol = "ETH/USDT" if str(symbol).upper() == "BTC/USDT" else "BTC/USDT"

                    pair_df = await self._load_market_data(
                        exchange=config.exchange,
                        symbol=pair_symbol,
                        timeframe=config.timeframe,
                        limit=data_limit,
                    )
                    if pair_df.empty:
                        continue

                    sig = inspect.signature(strategy.generate_signals)
                    params = list(sig.parameters.keys())
                    if len(params) >= 2:
                        signals = strategy.generate_signals(df, pair_df)  # type: ignore[arg-type]
                    else:
                        signals = strategy.generate_signals(df)

                    if signals:
                        await self._emit_signals(name, signals)
                        logger.info(
                            f"Strategy {name} generated {len(signals)} signal(s) for pair "
                            f"{symbol} vs {pair_symbol}"
                        )
                    self._mark_bar_processed(name, symbol, config.timeframe, latest_bar_at)
                    continue

                signals = await self.process_data(name, df)
                self._mark_bar_processed(name, symbol, config.timeframe, latest_bar_at)
                if signals:
                    logger.info(
                        f"Strategy {name} generated {len(signals)} signal(s) for {symbol}"
                    )
            except Exception as e:
                stats.error_count += 1
                stats.last_error_at = datetime.now(timezone.utc)
                stats.last_error = str(e)
                logger.error(f"Strategy {name} run error on {symbol}: {e}")

        cycle_ms = (datetime.now(timezone.utc) - cycle_start).total_seconds() * 1000
        stats.total_cycle_ms += cycle_ms
        stats.avg_cycle_ms = stats.total_cycle_ms / max(1, stats.run_count)

    async def _strategy_runner(self, name: str) -> None:
        while True:
            strategy = self._strategies.get(name)
            config = self._configs.get(name)
            if not strategy or not config or not strategy.is_running:
                break

            deadline = self._runtime_deadlines.get(name)
            if deadline and datetime.now(timezone.utc) >= deadline:
                logger.info(f"Strategy {name} reached runtime limit, stopping automatically")
                strategy.stop()
                self._running_since.pop(name, None)
                self._runtime_deadlines.pop(name, None)
                try:
                    close_summary = await self._close_positions_for_strategy_stop(name, reason="runtime_limit_reached")
                    logger.info(
                        f"Strategy {name} auto-closed positions on runtime stop: "
                        f"requested={close_summary.get('requested', 0)} "
                        f"closed={close_summary.get('closed', 0)} "
                        f"failed={close_summary.get('failed', 0)}"
                    )
                except Exception as exc:
                    logger.error(f"Strategy {name} runtime-limit auto-close failed: {exc}")
                break

            await self._run_strategy_once(name)
            timeframe_seconds = self._timeframe_to_seconds(config.timeframe)
            interval = max(5, min(max(5, timeframe_seconds // 3), 60))
            await asyncio.sleep(interval)

    def _start_task_for_strategy(self, name: str) -> None:
        existing = self._strategy_tasks.get(name)
        if existing and not existing.done():
            existing.cancel()
        self._strategy_tasks[name] = asyncio.create_task(
            self._strategy_runner(name),
            name=f"strategy_runner_{name}",
        )

    async def _stop_task_for_strategy(self, name: str) -> None:
        task = self._strategy_tasks.pop(name, None)
        if task and not task.done():
            task.cancel()
            # Wait at most 2 s for the task to honour cancellation.
            # strategy.stop() already sets state=STOPPED, so the runner
            # will exit on its next is_running check even if we don't wait.
            done, _ = await asyncio.wait({task}, timeout=2.0)
            if done:
                # Retrieve the result/exception so Python doesn't warn about it.
                with contextlib.suppress(BaseException):
                    task.result()

    def register_strategy(
        self,
        name: str,
        strategy_class: Type[StrategyBase],
        params: Optional[Dict[str, Any]] = None,
        symbols: Optional[List[str]] = None,
        timeframe: str = "1h",
        allocation: float = settings.DEFAULT_STRATEGY_ALLOCATION,
        runtime_limit_minutes: Optional[int] = None,
    ) -> bool:
        if name in self._strategies:
            logger.warning(f"Strategy {name} already registered")
            return False

        try:
            params = dict(params or {})
            params.setdefault("account_id", self._default_strategy_account_id(name))
            self._ensure_strategy_account(name, params)
            strategy = strategy_class(name=name, params=params)

            config = StrategyConfig(
                name=name,
                strategy_class=strategy_class,
                params=params,
                symbols=symbols or [],
                timeframe=timeframe,
                exchange=params.get("exchange", "gate"),
                allocation=max(0.0, min(float(allocation), 1.0)),
                runtime_limit_minutes=(
                    max(0, int(runtime_limit_minutes))
                    if runtime_limit_minutes is not None
                    else None
                ),
            )

            self._strategies[name] = strategy
            self._configs[name] = config
            self._stats[name] = StrategyRuntimeStats()
            logger.info(f"Strategy {name} registered")
            return True
        except Exception as e:
            logger.error(f"Failed to register strategy {name}: {e}")
            return False

    def unregister_strategy(self, name: str) -> bool:
        if name not in self._strategies:
            return False

        task = self._strategy_tasks.pop(name, None)
        if task and not task.done():
            task.cancel()

        strategy = self._strategies[name]
        if strategy.is_running:
            strategy.stop()

        del self._strategies[name]
        del self._configs[name]
        self._stats.pop(name, None)
        self._last_run_at.pop(name, None)
        self._running_since.pop(name, None)
        self._runtime_deadlines.pop(name, None)
        self._clear_bar_runtime_state(name)

        logger.info(f"Strategy {name} unregistered")
        return True

    def get_strategy(self, name: str) -> Optional[StrategyBase]:
        return self._strategies.get(name)

    def get_all_strategies(self) -> Dict[str, StrategyBase]:
        return self._strategies

    def get_running_strategies(self) -> List[StrategyBase]:
        return [s for s in self._strategies.values() if s.is_running]

    async def start_strategy(self, name: str) -> bool:
        strategy = self._strategies.get(name)
        if not strategy:
            logger.error(f"Strategy {name} not found")
            return False

        if strategy.is_running:
            logger.warning(f"Strategy {name} already running")
            return True

        self._clear_bar_runtime_state(name)
        strategy.initialize()
        strategy.start()
        self._running_since[name] = datetime.now(timezone.utc)
        cfg = self._configs.get(name)
        if cfg and cfg.runtime_limit_minutes and int(cfg.runtime_limit_minutes) > 0:
            self._runtime_deadlines[name] = datetime.now(timezone.utc) + pd.Timedelta(minutes=int(cfg.runtime_limit_minutes))
        else:
            self._runtime_deadlines.pop(name, None)
        self._start_task_for_strategy(name)

        logger.info(f"Strategy {name} started")
        return True

    async def stop_strategy(self, name: str) -> bool:
        strategy = self._strategies.get(name)
        if not strategy:
            return False

        strategy.stop()
        await self._stop_task_for_strategy(name)
        self._running_since.pop(name, None)
        self._runtime_deadlines.pop(name, None)
        self._clear_bar_runtime_state(name)
        logger.info(f"Strategy {name} stopped")
        return True

    async def pause_strategy(self, name: str) -> bool:
        strategy = self._strategies.get(name)
        if not strategy:
            return False

        strategy.pause()
        await self._stop_task_for_strategy(name)
        self._running_since.pop(name, None)
        self._runtime_deadlines.pop(name, None)
        logger.info(f"Strategy {name} paused")
        return True

    async def resume_strategy(self, name: str) -> bool:
        strategy = self._strategies.get(name)
        if not strategy:
            return False

        strategy.resume()
        self._running_since[name] = datetime.now(timezone.utc)
        cfg = self._configs.get(name)
        if cfg and cfg.runtime_limit_minutes and int(cfg.runtime_limit_minutes) > 0:
            self._runtime_deadlines[name] = datetime.now(timezone.utc) + pd.Timedelta(minutes=int(cfg.runtime_limit_minutes))
        else:
            self._runtime_deadlines.pop(name, None)
        self._start_task_for_strategy(name)
        logger.info(f"Strategy {name} resumed")
        return True

    async def start_all(self) -> None:
        for name in self._strategies:
            await self.start_strategy(name)

    async def stop_all(self) -> None:
        for name in list(self._strategies.keys()):
            await self.stop_strategy(name)

    def register_signal_callback(self, callback: callable) -> None:
        self._signal_callbacks.append(callback)

    async def process_data(self, strategy_name: str, data: Any) -> List[Signal]:
        strategy = self._strategies.get(strategy_name)
        if not strategy or not strategy.is_running:
            return []

        try:
            signals = strategy.generate_signals(data)
            if signals:
                await self._emit_signals(strategy_name, signals)
            return signals
        except Exception as e:
            stats = self._stats_for(strategy_name)
            stats.error_count += 1
            stats.last_error_at = datetime.now(timezone.utc)
            stats.last_error = str(e)
            logger.error(f"Strategy {strategy_name} error: {e}")
            return []

    async def run_strategies_on_data(self, symbol: str, df: Any) -> Dict[str, List[Signal]]:
        results = {}

        for name, strategy in self._strategies.items():
            config = self._configs.get(name)
            if not config:
                continue
            if not strategy.is_running:
                continue
            if symbol not in config.symbols and config.symbols:
                continue

            signals = await self.process_data(name, df)
            if signals:
                results[name] = signals

        return results

    def update_strategy_params(self, name: str, params: Dict[str, Any]) -> bool:
        strategy = self._strategies.get(name)
        if not strategy:
            return False

        for key, value in params.items():
            strategy.set_param(key, value)

        self._configs[name].params.update(params)
        if "exchange" in params:
            self._configs[name].exchange = str(params["exchange"])
        if "allocation" in params:
            self._configs[name].allocation = max(0.0, min(float(params["allocation"]), 1.0))

        logger.info(f"Strategy {name} params updated: {params}")
        return True

    def update_strategy_allocation(self, name: str, allocation: float) -> bool:
        if name not in self._configs:
            return False
        self._configs[name].allocation = max(0.0, min(float(allocation), 1.0))
        return True

    def update_strategy_runtime_config(
        self,
        name: str,
        *,
        timeframe: Optional[str] = None,
        symbols: Optional[List[str]] = None,
        runtime_limit_minutes: Optional[int] = None,
    ) -> bool:
        cfg = self._configs.get(name)
        if not cfg:
            return False
        if timeframe is not None:
            tf = str(timeframe or "").strip()
            if tf not in _RESAMPLE_RULES:
                return False
            cfg.timeframe = tf
        if symbols is not None:
            normalized_symbols = [
                str(symbol).strip()
                for symbol in list(symbols)
                if str(symbol).strip()
            ]
            if not normalized_symbols:
                normalized_symbols = ["BTC/USDT"]
            cfg.symbols = normalized_symbols
        if runtime_limit_minutes is not None:
            val = max(0, int(runtime_limit_minutes))
            cfg.runtime_limit_minutes = val or None
            strategy = self._strategies.get(name)
            if strategy and strategy.is_running and val > 0:
                self._runtime_deadlines[name] = datetime.now(timezone.utc) + pd.Timedelta(minutes=val)
            else:
                self._runtime_deadlines.pop(name, None)
        return True

    def restore_strategy_runtime_anchor(self, name: str, started_at: Optional[datetime]) -> bool:
        """Restore strategy running anchor from persisted state after process restart."""
        strategy = self._strategies.get(name)
        cfg = self._configs.get(name)
        if not strategy or not strategy.is_running or started_at is None:
            return False

        anchor = started_at
        if anchor.tzinfo is None:
            anchor = anchor.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        if anchor > now:
            anchor = now

        self._running_since[name] = anchor
        if cfg and cfg.runtime_limit_minutes and int(cfg.runtime_limit_minutes) > 0:
            self._runtime_deadlines[name] = anchor + pd.Timedelta(minutes=int(cfg.runtime_limit_minutes))
        return True

    def rebalance_allocations(self, allocations: Dict[str, float]) -> Dict[str, float]:
        normalized: Dict[str, float] = {}
        for name, value in allocations.items():
            if name in self._configs:
                normalized[name] = max(0.0, min(float(value), 1.0))

        total = sum(normalized.values())
        if total > 1.0 and total > 0:
            for name in normalized:
                normalized[name] = normalized[name] / total

        for name, value in normalized.items():
            self._configs[name].allocation = value

        return normalized

    def get_strategy_allocation(self, name: Optional[str]) -> float:
        if not name:
            return 1.0
        cfg = self._configs.get(name)
        if not cfg:
            return 1.0
        return float(cfg.allocation)

    @staticmethod
    def _is_scalar_json_value(value: Any) -> bool:
        return value is None or isinstance(value, (bool, int, float, str))

    def _sanitize_param_value(self, value: Any, depth: int = 0) -> Any:
        if depth > 2:
            return "<nested>"
        if self._is_scalar_json_value(value):
            return value
        if isinstance(value, Decimal):
            try:
                return float(value)
            except Exception:
                return str(value)
        if isinstance(value, dict):
            out: Dict[str, Any] = {}
            for i, (k, v) in enumerate(value.items()):
                if i >= 50:
                    out["__truncated__"] = True
                    break
                out[str(k)] = self._sanitize_param_value(v, depth + 1)
            return out
        if isinstance(value, (list, tuple, set)):
            seq = list(value)
            out = [self._sanitize_param_value(v, depth + 1) for v in seq[:50]]
            if len(seq) > 50:
                out.append("<truncated>")
            return out
        # Avoid expensive/verbose stringification of runtime objects (e.g. DataFrame, connector, cache)
        return f"<{type(value).__name__}>"

    def _sanitize_params_for_api(self, params: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for key, value in (params or {}).items():
            out[str(key)] = self._sanitize_param_value(value)
        return out

    def _infer_param_schema_from_params(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        schema: List[Dict[str, Any]] = []
        for key, value in params.items():
            if isinstance(value, bool):
                schema.append({"name": key, "type": "boolean", "default": value})
            elif isinstance(value, int) and not isinstance(value, bool):
                item: Dict[str, Any] = {
                    "name": key,
                    "type": "integer",
                    "default": value,
                    "min": 1,
                    "max": 5000,
                    "step": 1,
                }
                if "period" in key or "lookback" in key:
                    item["max"] = 1000
                schema.append(item)
            elif isinstance(value, float):
                item = {
                    "name": key,
                    "type": "number",
                    "default": value,
                    "step": 0.0001,
                }
                if "pct" in key or "threshold" in key:
                    item["min"] = 0.0
                    item["max"] = 1.0 if abs(value) <= 1 else max(abs(value) * 3, 10)
                schema.append(item)
            elif isinstance(value, Decimal):
                schema.append({"name": key, "type": "number", "default": float(value), "step": 0.0001})
            elif isinstance(value, (dict, list)):
                schema.append({"name": key, "type": "json", "default": self._sanitize_param_value(value)})
            elif value is None:
                schema.append({"name": key, "type": "string", "default": ""})
            elif isinstance(value, str):
                schema.append({"name": key, "type": "string", "default": value})
            else:
                schema.append(
                    {
                        "name": key,
                        "type": "string",
                        "default": f"<{type(value).__name__}>",
                        "readonly_hint": True,
                    }
                )
        return schema

    def get_strategy_param_schema(self, name: str) -> Optional[Dict[str, Any]]:
        strategy = self._strategies.get(name)
        config = self._configs.get(name)
        if not strategy or not config:
            return None

        base_params = dict(config.params or {})
        return {
            "name": name,
            "strategy_type": config.strategy_class.__name__,
            "params": self._infer_param_schema_from_params(base_params),
            "raw": self._sanitize_params_for_api(base_params),
        }

    def get_strategy_runtime(self, name: str) -> Dict[str, Any]:
        stats = self._stats_for(name)
        strategy = self._strategies.get(name)
        runner = self._strategy_tasks.get(name)
        started_at = self._running_since.get(name)
        cfg = self._configs.get(name)
        deadline = self._runtime_deadlines.get(name)
        is_running = bool(strategy and strategy.is_running)
        uptime_seconds = int((datetime.now(timezone.utc) - started_at).total_seconds()) if (is_running and started_at) else 0
        remaining_seconds = (
            max(0, int((deadline - datetime.now(timezone.utc)).total_seconds()))
            if (deadline and is_running)
            else None
        )
        account_id = self._strategy_account_id(name)
        return {
            "run_count": stats.run_count,
            "signal_count": stats.signal_count,
            "error_count": stats.error_count,
            "last_run_at": stats.last_run_at.isoformat() if stats.last_run_at else None,
            "last_signal_at": stats.last_signal_at.isoformat() if stats.last_signal_at else None,
            "last_error_at": stats.last_error_at.isoformat() if stats.last_error_at else None,
            "last_error": stats.last_error,
            "avg_cycle_ms": round(float(stats.avg_cycle_ms), 3),
            "started_at": started_at.isoformat() if started_at else None,
            "uptime_seconds": max(0, uptime_seconds),
            "runtime_limit_minutes": (int(cfg.runtime_limit_minutes) if (cfg and cfg.runtime_limit_minutes) else None),
            "stop_at": deadline.isoformat() if deadline else None,
            "remaining_seconds": remaining_seconds,
            "account_id": account_id,
            "isolated_account": account_id != "main",
            "runner_task": runner.get_name() if runner else None,
            "runner_alive": bool(runner and not runner.done()),
            "independent_runner": True,
        }

    def get_strategy_info(self, name: str) -> Optional[Dict[str, Any]]:
        strategy = self._strategies.get(name)
        config = self._configs.get(name)

        if not strategy or not config:
            return None

        base_info = dict(strategy.get_info() or {})
        base_info.pop("params", None)
        editable_params = self._sanitize_params_for_api(dict(config.params or {}))
        return {
            **base_info,
            "strategy_type": config.strategy_class.__name__,
            "symbols": config.symbols,
            "timeframe": config.timeframe,
            "exchange": config.exchange,
            "enabled": config.enabled,
            "allocation": config.allocation,
            "params": editable_params,
            "account_id": self._strategy_account_id(name),
            "last_run_at": self._last_run_at.get(name).isoformat() if self._last_run_at.get(name) else None,
            "runtime": self.get_strategy_runtime(name),
            "param_schema": self._infer_param_schema_from_params(dict(config.params or {})),
        }

    def list_strategies(self) -> List[Dict[str, Any]]:
        infos = []
        for name in self._strategies:
            info = self.get_strategy_info(name)
            if info:
                infos.append(info)
        return infos

    def get_aggregated_signals(self, symbol: str, min_strength: float = 0.5) -> Dict[str, Any]:
        buy_signals = []
        sell_signals = []

        for name, strategy in self._strategies.items():
            if not strategy.is_running:
                continue

            recent_signals = strategy.get_recent_signals(10)
            for signal in recent_signals:
                if signal.symbol != symbol:
                    continue
                if signal.strength < min_strength:
                    continue

                if signal.signal_type.value in ["buy", "close_short"]:
                    buy_signals.append(
                        {
                            "strategy": name,
                            "strength": signal.strength,
                            "price": signal.price,
                            "timestamp": signal.timestamp,
                        }
                    )
                elif signal.signal_type.value in ["sell", "close_long"]:
                    sell_signals.append(
                        {
                            "strategy": name,
                            "strength": signal.strength,
                            "price": signal.price,
                            "timestamp": signal.timestamp,
                        }
                    )

        return {
            "symbol": symbol,
            "buy_count": len(buy_signals),
            "sell_count": len(sell_signals),
            "avg_buy_strength": (
                sum(s["strength"] for s in buy_signals) / len(buy_signals)
                if buy_signals
                else 0
            ),
            "avg_sell_strength": (
                sum(s["strength"] for s in sell_signals) / len(sell_signals)
                if sell_signals
                else 0
            ),
            "buy_signals": buy_signals,
            "sell_signals": sell_signals,
        }

    def get_dashboard_summary(self, signal_limit: int = 20) -> Dict[str, Any]:
        running = []
        recent_signals = []
        runtime: Dict[str, Any] = {}
        stale_running: List[Dict[str, Any]] = []
        state_counter: Dict[str, int] = {
            "running": 0,
            "idle": 0,
            "paused": 0,
            "stopped": 0,
        }
        now_utc = datetime.now(timezone.utc)

        for name, strategy in self._strategies.items():
            config = self._configs.get(name)
            if not config:
                continue

            info = self.get_strategy_info(name)
            if info and strategy.is_running:
                running.append(info)
            state_name = str((info or {}).get("state") or strategy.state.value).lower()
            state_counter[state_name] = state_counter.get(state_name, 0) + 1

            run_stats = self.get_strategy_runtime(name)
            runtime[name] = run_stats

            if strategy.is_running:
                expected_cycle = max(10, min(self._timeframe_to_seconds(config.timeframe), 600))
                last_run_raw = run_stats.get("last_run_at")
                lag_seconds = None
                run_count = int(run_stats.get("run_count") or 0)
                avg_cycle_ms = float(run_stats.get("avg_cycle_ms") or 0.0)
                avg_cycle_seconds = max(0.0, avg_cycle_ms / 1000.0)
                effective_cycle = max(float(expected_cycle), avg_cycle_seconds)
                # Avoid false positives on fast (e.g. 10s) strategies under shared scheduler load.
                stale_threshold_seconds = int(max(60.0, effective_cycle * 4.0))
                if last_run_raw:
                    try:
                        lag_seconds = max(0, int((now_utc - datetime.fromisoformat(last_run_raw)).total_seconds()))
                    except Exception:
                        lag_seconds = None
                if run_count < 3:
                    continue
                if lag_seconds is None or lag_seconds > stale_threshold_seconds:
                    stale_running.append(
                        {
                            "strategy": name,
                            "timeframe": config.timeframe,
                            "expected_cycle_seconds": expected_cycle,
                            "stale_threshold_seconds": stale_threshold_seconds,
                            "lag_seconds": lag_seconds,
                            "last_run_at": last_run_raw,
                        }
                    )

            for signal in strategy.get_recent_signals(signal_limit):
                recent_signals.append(
                    {
                        "strategy": name,
                        "symbol": signal.symbol,
                        "signal_type": signal.signal_type.value,
                        "price": signal.price,
                        "strength": signal.strength,
                        "timestamp": signal.timestamp.isoformat(),
                    }
                )

        recent_signals.sort(key=lambda x: x["timestamp"], reverse=True)

        allocations = {
            name: cfg.allocation for name, cfg in self._configs.items()
        }
        strategy_performance = self._build_strategy_performance()

        return {
            "running": running,
            "running_count": len(running),
            "registered_count": len(self._strategies),
            "idle_count": int(state_counter.get("idle", 0)),
            "paused_count": int(state_counter.get("paused", 0)),
            "stopped_count": int(state_counter.get("stopped", 0)),
            "recent_signals": recent_signals[:signal_limit],
            "allocations": allocations,
            "runtime": runtime,
            "strategy_performance": strategy_performance,
            "stale_running": stale_running,
            "stale_running_count": len(stale_running),
            "refresh_hint_seconds": 5,
            "timestamp": datetime.now().isoformat(),
        }

    @staticmethod
    def _calc_max_drawdown_ratio(equity_curve: List[float]) -> float:
        if not equity_curve:
            return 0.0
        peak = float(equity_curve[0] or 0.0)
        if peak <= 0:
            return 0.0
        max_dd = 0.0
        for val in equity_curve:
            cur = float(val or 0.0)
            if cur > peak:
                peak = cur
            if peak > 0:
                dd = (peak - cur) / peak
                if dd > max_dd:
                    max_dd = dd
        return max(0.0, min(1.0, float(max_dd)))

    def _build_strategy_performance(self) -> Dict[str, Dict[str, Any]]:
        # Lazy imports avoid circular dependency during module initialization.
        from core.risk.risk_manager import risk_manager
        from core.trading.position_manager import position_manager

        risk_report = risk_manager.get_risk_report()
        current_equity = float(((risk_report.get("equity") or {}).get("current") or 0.0))
        min_notional = max(1.0, float(getattr(settings, "MIN_STRATEGY_ORDER_USD", 100.0) or 100.0))
        now_iso = datetime.now(timezone.utc).isoformat()

        grouped_trades: Dict[str, List[Dict[str, Any]]] = {}
        for row in risk_manager.get_trade_history(limit=5000):
            if not isinstance(row, dict):
                continue
            name = str(row.get("strategy") or "").strip()
            if not name:
                continue
            grouped_trades.setdefault(name, []).append(row)

        out: Dict[str, Dict[str, Any]] = {}
        for name, strategy in self._strategies.items():
            cfg = self._configs.get(name)
            if not cfg:
                continue

            trades = sorted(grouped_trades.get(name, []), key=lambda r: str(r.get("timestamp") or ""))
            realized_pnl = 0.0
            ret_samples: List[float] = []
            equity_base = current_equity * float(cfg.allocation or 0.0) if current_equity > 0 else 0.0
            equity_base = max(min_notional, float(equity_base))
            equity_curve = [equity_base]
            mark_equity = equity_base
            last_update = None

            for row in trades:
                pnl = float(row.get("pnl") or 0.0)
                notional = abs(float(row.get("notional") or 0.0))
                realized_pnl += pnl
                mark_equity += pnl
                equity_curve.append(mark_equity)
                if notional > 0:
                    ret_samples.append(pnl / notional)
                ts = str(row.get("timestamp") or "").strip()
                if ts:
                    last_update = ts

            unrealized_pnl = sum(float(p.unrealized_pnl or 0.0) for p in position_manager.get_positions_by_strategy(name))
            total_pnl = float(realized_pnl + unrealized_pnl)
            equity_curve.append(mark_equity + unrealized_pnl)

            return_ratio = (total_pnl / equity_base) if equity_base > 0 else 0.0
            max_dd_ratio = self._calc_max_drawdown_ratio(equity_curve)
            variance = statistics.pvariance(ret_samples) if len(ret_samples) >= 2 else 0.0

            out[name] = {
                "return_ratio": round(return_ratio, 8),
                "return_pct": round(return_ratio * 100.0, 4),
                "max_drawdown_ratio": round(max_dd_ratio, 8),
                "max_drawdown_pct": round(max_dd_ratio * 100.0, 4),
                "variance": round(float(variance), 10),
                "capital_base": round(equity_base, 4),
                "realized_pnl": round(realized_pnl, 4),
                "unrealized_pnl": round(unrealized_pnl, 4),
                "trade_count": len(trades),
                "last_update": last_update or now_iso,
                "running": bool(strategy.is_running),
            }
        return out


strategy_manager = StrategyManager()
