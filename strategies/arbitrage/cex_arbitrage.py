import asyncio
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from loguru import logger

from core.exchanges import exchange_manager
from core.marketdata.runtime_price_provider import get_realtime_price
from core.strategies.strategy_base import Signal, SignalType, StrategyBase


def _position_side_text(position: Any) -> str:
    side = getattr(position, "side", None)
    return str(getattr(side, "value", side) or "").strip().lower()


def _normalize_symbol_key(symbol: Any) -> str:
    raw = str(symbol or "").strip().upper()
    if not raw:
        return ""
    if ":" in raw:
        raw = raw.split(":", 1)[0]
    raw = raw.replace("_", "/")
    if "/" not in raw and raw.endswith("USDT") and len(raw) > 4:
        raw = f"{raw[:-4]}/USDT"
    return raw


def _resolve_exit_ratio(params: Dict[str, Any], default: float = 0.5) -> float:
    try:
        ratio = float(params.get("exit_spread_ratio", default))
    except (TypeError, ValueError):
        ratio = default
    if not math.isfinite(ratio):
        ratio = default
    return min(max(ratio, 0.0), 1.0)


def _close_signal_price(data: Any, position: Any) -> float:
    try:
        if data is not None and "close" in getattr(data, "columns", []) and len(data):
            px = float(data["close"].iloc[-1])
            if math.isfinite(px) and px > 0:
                return px
    except Exception:
        pass
    for attr in ("current_price", "entry_price"):
        try:
            px = float(getattr(position, attr, 0.0) or 0.0)
        except (TypeError, ValueError):
            continue
        if math.isfinite(px) and px > 0:
            return px
    return 0.0


def _observation_age_sec(observed_at: Any, now: datetime) -> Optional[float]:
    try:
        return max(0.0, (now - observed_at).total_seconds())
    except (TypeError, AttributeError):
        return None


class CEXArbitrageStrategy(StrategyBase):
    """Cross-exchange spot arbitrage strategy."""

    def __init__(self, name: str = "CEX_Arbitrage", params: Optional[Dict[str, Any]] = None):
        default_params = {
            "min_spread": 0.005,
            "alpha_threshold": 0.005,
            "min_volume": 10000,
            "exchanges": ["binance", "okx", "gate", "bybit"],
            "max_position_size": 1000,
            "consider_fees": True,
            "fee_rate": 0.001,
            "max_opportunities": 2,
            "cooldown_min": 1,
            "max_vol": 0.03,
            "max_spread": 0.05,
            # Close positions once the live spread falls below
            # exit_spread_ratio * min_spread (capital is otherwise locked
            # until global SL/TP fires).
            "exit_spread_ratio": 0.5,
            # Never act on a price book older than this (<=0 disables guard).
            "exit_price_max_age_sec": 300.0,
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)
        self._price_cache: Dict[str, Dict[str, Dict[str, float]]] = {}
        self._price_cache_at: Dict[str, datetime] = {}
        self._last_signal_at: Dict[str, datetime] = {}

    def _resolve_min_spread(self) -> float:
        raw = self.params.get("min_spread", self.params.get("alpha_threshold", 0.005))
        return max(float(raw), 0.0)

    @staticmethod
    def _estimate_cross_exchange_vol(prices: Dict[str, Dict[str, float]]) -> float:
        mids: List[float] = []
        for row in prices.values():
            bid = float(row.get("bid") or 0.0)
            ask = float(row.get("ask") or 0.0)
            if bid > 0 and ask > 0:
                mids.append((bid + ask) * 0.5)
        if len(mids) < 2:
            return 0.0
        series = pd.Series(mids, dtype=float)
        mean_px = float(series.mean() or 0.0)
        if mean_px <= 0:
            return 0.0
        return max(0.0, float(series.std(ddof=0) / mean_px))

    def _cooldown_ready(self, symbol: str, now: datetime) -> bool:
        cooldown_min = max(0, int(float(self.params.get("cooldown_min", 0) or 0)))
        if cooldown_min <= 0:
            return True
        last = self._last_signal_at.get(str(symbol).upper())
        if not last:
            return True
        return (now - last) >= timedelta(minutes=cooldown_min)

    async def update_prices(self, symbol: str) -> Dict[str, Dict[str, float]]:
        # PERF: fetch tickers from all configured exchanges concurrently rather
        # than sequentially. With 4 CEX connectors this reduces wall time from
        # ~4x to ~1x of the slowest connector. Per-connector failures are
        # isolated via return_exceptions=True so one bad exchange does not
        # impact the others.
        prices: Dict[str, Dict[str, float]] = {}
        ready: List[Tuple[str, Any]] = []
        for exchange_name in self.params.get("exchanges", []):
            connector = exchange_manager.get_exchange(exchange_name)
            if not connector or not connector.is_connected:
                continue
            ready.append((str(exchange_name), connector))
        if not ready:
            self._price_cache[symbol] = prices
            self._price_cache_at[symbol] = datetime.now(timezone.utc)
            return prices

        results = await asyncio.gather(
            *(
                get_realtime_price(
                    exchange_name,
                    symbol,
                    connector=connector,
                    allow_rest_fallback=True,
                )
                for exchange_name, connector in ready
            ),
            return_exceptions=True,
        )
        for (exchange_name, _), price_read in zip(ready, results):
            if isinstance(price_read, BaseException):
                logger.debug(
                    f"{self.name} ticker unavailable on {exchange_name}: {price_read}"
                )
                continue
            try:
                bid = float(price_read.bid or 0.0)
                ask = float(price_read.ask or 0.0)
                last = float(price_read.price or 0.0)
            except Exception as e:  # malformed ticker payload
                logger.debug(f"{self.name} malformed ticker from {exchange_name}: {e}")
                continue
            if bid > 0 and ask > 0:
                prices[exchange_name] = {"bid": bid, "ask": ask, "last": last}

        self._price_cache[symbol] = prices
        self._price_cache_at[symbol] = datetime.now(timezone.utc)
        return prices

    def find_arbitrage_opportunities(self, symbol: str, prices: Dict[str, Dict[str, float]]) -> List[Dict[str, Any]]:
        opportunities: List[Dict[str, Any]] = []
        exchanges = list(prices.keys())
        if len(exchanges) < 2:
            return opportunities

        fee_drag = 2 * float(self.params.get("fee_rate", 0.0)) if bool(self.params.get("consider_fees", True)) else 0.0
        min_spread = self._resolve_min_spread()
        max_spread = max(min_spread, float(self.params.get("max_spread", 0.05) or 0.05))
        max_vol = max(0.0, float(self.params.get("max_vol", 0.03) or 0.0))
        cross_vol = self._estimate_cross_exchange_vol(prices)
        if max_vol > 0 and cross_vol > max_vol:
            logger.debug(
                f"{self.name} {symbol} skipped: cross-exchange vol={cross_vol:.6f} > max_vol={max_vol:.6f}"
            )
            return opportunities

        for buy_exchange in exchanges:
            buy_ask = float(prices[buy_exchange].get("ask") or 0.0)
            if buy_ask <= 0:
                continue
            for sell_exchange in exchanges:
                if sell_exchange == buy_exchange:
                    continue
                sell_bid = float(prices[sell_exchange].get("bid") or 0.0)
                if sell_bid <= 0:
                    continue

                spread = (sell_bid - buy_ask) / buy_ask
                effective_spread = spread - fee_drag
                if effective_spread < min_spread:
                    continue
                if effective_spread > max_spread:
                    continue

                opportunities.append(
                    {
                        "symbol": symbol,
                        "buy_exchange": buy_exchange,
                        "sell_exchange": sell_exchange,
                        "buy_price": buy_ask,
                        "sell_price": sell_bid,
                        "spread": spread,
                        "effective_spread": effective_spread,
                        "cross_exchange_vol": cross_vol,
                        "timestamp": datetime.now(timezone.utc),
                    }
                )

        opportunities.sort(key=lambda x: float(x["effective_spread"]), reverse=True)
        max_n = max(1, int(self.params.get("max_opportunities", 2)))
        return opportunities[:max_n]

    def _lookup_price_book(
        self, symbol: str
    ) -> Tuple[Optional[Dict[str, Dict[str, float]]], Optional[datetime]]:
        target = _normalize_symbol_key(symbol)
        if not target:
            return None, None
        for key, book in self._price_cache.items():
            if _normalize_symbol_key(key) == target:
                return book, self._price_cache_at.get(key)
        return None, None

    def _current_effective_spread(
        self,
        book: Dict[str, Dict[str, float]],
        buy_exchange: Any = None,
        sell_exchange: Any = None,
    ) -> Optional[Dict[str, Any]]:
        """Current effective spread from the cached price book.

        Prefers the exact entry pair when both legs are still quoted;
        otherwise falls back to the best spread across all exchange pairs.
        Returns None when fewer than two usable quotes exist (never close
        blind on a one-sided book).
        """
        fee_drag = (
            2 * float(self.params.get("fee_rate", 0.0))
            if bool(self.params.get("consider_fees", True))
            else 0.0
        )

        def _pair_spread(buy_ex: str, sell_ex: str) -> Optional[Dict[str, Any]]:
            buy_ask = float((book.get(buy_ex) or {}).get("ask") or 0.0)
            sell_bid = float((book.get(sell_ex) or {}).get("bid") or 0.0)
            if buy_ask <= 0 or sell_bid <= 0:
                return None
            spread = (sell_bid - buy_ask) / buy_ask
            return {
                "spread": spread,
                "effective_spread": spread - fee_drag,
                "buy_exchange": buy_ex,
                "sell_exchange": sell_ex,
            }

        buy_key = str(buy_exchange or "").strip()
        sell_key = str(sell_exchange or "").strip()
        if buy_key and sell_key and buy_key != sell_key:
            pair = _pair_spread(buy_key, sell_key)
            if pair is not None:
                pair["spread_scope"] = "entry_pair"
                return pair

        best: Optional[Dict[str, Any]] = None
        for buy_ex in book:
            for sell_ex in book:
                if buy_ex == sell_ex:
                    continue
                row = _pair_spread(buy_ex, sell_ex)
                if row is None:
                    continue
                if best is None or row["effective_spread"] > best["effective_spread"]:
                    best = row
        if best is not None:
            best["spread_scope"] = "best_pair"
        return best

    def check_exit(self, data: pd.DataFrame, position: Any) -> Optional[Signal]:
        """Close an arbitrage leg once the cross-exchange spread converges.

        check_exit is synchronous, so it reads the price book cached by the
        latest ``update_prices`` run (runtime price provider) instead of
        awaiting fresh quotes; a stale book never triggers an exit.
        """
        side = _position_side_text(position)
        if side not in {"long", "short"}:
            return None
        symbol = str(getattr(position, "symbol", "") or "").strip()
        if not symbol:
            return None

        book, cached_at = self._lookup_price_book(symbol)
        if not book or cached_at is None:
            return None
        now = datetime.now(timezone.utc)
        age_sec = _observation_age_sec(cached_at, now)
        max_age = float(self.params.get("exit_price_max_age_sec", 300.0) or 0.0)
        if age_sec is None or (max_age > 0 and age_sec > max_age):
            return None

        metadata = dict(getattr(position, "metadata", {}) or {})
        spread_info = self._current_effective_spread(
            book,
            metadata.get("buy_exchange"),
            metadata.get("sell_exchange"),
        )
        if spread_info is None:
            return None

        min_spread = self._resolve_min_spread()
        ratio = _resolve_exit_ratio(self.params)
        threshold = min_spread * ratio
        if float(spread_info["effective_spread"]) > threshold:
            return None

        return Signal(
            symbol=symbol,
            signal_type=SignalType.CLOSE_LONG if side == "long" else SignalType.CLOSE_SHORT,
            price=_close_signal_price(data, position),
            timestamp=self._bar_time(data),
            strategy_name=self.name,
            strength=0.7,
            metadata={
                "close_reason": "arbitrage_spread_converged",
                "close_only": True,
                "current_spread": float(spread_info["spread"]),
                "current_effective_spread": float(spread_info["effective_spread"]),
                "exit_threshold": float(threshold),
                "entry_threshold": float(min_spread),
                "exit_spread_ratio": float(ratio),
                "spread_scope": str(spread_info["spread_scope"]),
                "buy_exchange": str(spread_info["buy_exchange"]),
                "sell_exchange": str(spread_info["sell_exchange"]),
                "price_book_age_sec": float(age_sec),
            },
        )

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        return []

    async def generate_signals_async(self, symbol: str) -> List[Signal]:
        now = datetime.now(timezone.utc)
        symbol_key = str(symbol or "").upper()
        if not self._cooldown_ready(symbol_key, now):
            return []

        prices = await self.update_prices(symbol)
        opportunities = self.find_arbitrage_opportunities(symbol, prices)
        if not opportunities:
            return []

        min_spread = max(self._resolve_min_spread(), 1e-9)
        signals: List[Signal] = []
        for opp in opportunities:
            strength = max(0.1, min(float(opp["effective_spread"]) / min_spread, 1.0))
            ts = opp["timestamp"]

            signals.append(
                Signal(
                    symbol=symbol,
                    signal_type=SignalType.BUY,
                    price=float(opp["buy_price"]),
                    timestamp=ts,
                    strategy_name=self.name,
                    strength=strength,
                    metadata={
                        "exchange": opp["buy_exchange"],
                        "arbitrage_type": "buy_side",
                        "buy_exchange": opp["buy_exchange"],
                        "sell_exchange": opp["sell_exchange"],
                        "spread": float(opp["spread"]),
                        "effective_spread": float(opp["effective_spread"]),
                        "cross_exchange_vol": float(opp.get("cross_exchange_vol", 0.0)),
                    },
                )
            )
            signals.append(
                Signal(
                    symbol=symbol,
                    signal_type=SignalType.SELL,
                    price=float(opp["sell_price"]),
                    timestamp=ts,
                    strategy_name=self.name,
                    strength=strength,
                    metadata={
                        "exchange": opp["sell_exchange"],
                        "arbitrage_type": "sell_side",
                        "buy_exchange": opp["buy_exchange"],
                        "sell_exchange": opp["sell_exchange"],
                        "spread": float(opp["spread"]),
                        "effective_spread": float(opp["effective_spread"]),
                        "cross_exchange_vol": float(opp.get("cross_exchange_vol", 0.0)),
                    },
                )
            )

            logger.info(
                f"{self.name} {symbol} buy@{opp['buy_exchange']}={opp['buy_price']:.4f} "
                f"sell@{opp['sell_exchange']}={opp['sell_price']:.4f} "
                f"edge={opp['effective_spread']*100:.2f}%"
            )

        if signals:
            self._last_signal_at[symbol_key] = now

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {"type": "realtime_ticker", "exchanges": self.params.get("exchanges", [])}


class TriangularArbitrageStrategy(StrategyBase):
    """Single-exchange triangular arbitrage strategy."""

    def __init__(self, name: str = "Triangular_Arbitrage", params: Optional[Dict[str, Any]] = None):
        default_params = {
            "exchange": "binance",
            "base_currency": "USDT",
            "min_profit": 0.003,
            "alpha_threshold": 0.003,
            "consider_fees": True,
            "fee_rate": 0.001,
            "bridge_assets": ["ETH", "BNB", "SOL"],
            "max_opportunities": 2,
            "cooldown_min": 1,
            "max_spread": 0.05,
            # Close positions once |edge| falls below
            # exit_spread_ratio * min_profit.
            "exit_spread_ratio": 0.5,
            # Never act on an edge observation older than this (<=0 disables).
            "exit_price_max_age_sec": 300.0,
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)
        self._triangles: List[List[str]] = []
        self._last_signal_at: Dict[str, datetime] = {}
        self._last_edge_obs: Dict[str, Dict[str, Any]] = {}

    def _resolve_min_profit(self) -> float:
        raw = self.params.get("min_profit", self.params.get("alpha_threshold", 0.003))
        return max(float(raw), 0.0)

    def _cooldown_ready(self, symbol: str, now: datetime) -> bool:
        cooldown_min = max(0, int(float(self.params.get("cooldown_min", 0) or 0)))
        if cooldown_min <= 0:
            return True
        last = self._last_signal_at.get(str(symbol).upper())
        if not last:
            return True
        return (now - last) >= timedelta(minutes=cooldown_min)

    def set_triangles(self, triangles: List[List[str]]) -> None:
        self._triangles = triangles

    @staticmethod
    def _split_symbol(symbol: str) -> Tuple[str, str]:
        raw = str(symbol or "").upper()
        if "/" in raw:
            base, quote = raw.split("/", 1)
            return base, quote
        if raw.endswith("USDT") and len(raw) > 4:
            return raw[:-4], "USDT"
        return raw or "BTC", "USDT"

    def _default_triangles(self, symbol: str) -> List[List[str]]:
        base, quote = self._split_symbol(symbol)
        out: List[List[str]] = []
        for mid in self.params.get("bridge_assets", []):
            m = str(mid).upper()
            if not m or m == base or m == quote:
                continue
            out.append([f"{base}/{quote}", f"{m}/{base}", f"{m}/{quote}"])
        return out

    @staticmethod
    def _edge_from_prices(direct: float, mid_base: float, mid_quote: float) -> Tuple[float, float]:
        if direct <= 0 or mid_base <= 0 or mid_quote <= 0:
            return 0.0, 0.0
        implied = mid_quote / mid_base
        edge = (implied - direct) / direct
        return edge, implied

    def calculate_profit(self, rates: Dict[str, float], triangle: List[str], amount: float = 1.0) -> float:
        current_amount = float(amount)
        for pair in triangle:
            rate = float(rates.get(pair, 0.0))
            if rate <= 0:
                return -float("inf")
            current_amount *= rate

        profit = (current_amount - float(amount)) / max(float(amount), 1e-9)
        if bool(self.params.get("consider_fees", True)):
            profit -= 3 * float(self.params.get("fee_rate", 0.001))
        return profit

    async def find_opportunities(self, rates: Dict[str, float]) -> List[Dict[str, Any]]:
        opportunities: List[Dict[str, Any]] = []
        min_profit = self._resolve_min_profit()
        for triangle in self._triangles:
            profit = self.calculate_profit(rates, triangle)
            if profit >= min_profit:
                opportunities.append({"triangle": triangle, "profit": profit, "timestamp": datetime.now(timezone.utc)})
        opportunities.sort(key=lambda x: float(x["profit"]), reverse=True)
        return opportunities

    def check_exit(self, data: pd.DataFrame, position: Any) -> Optional[Signal]:
        """Close a triangular position once the implied/direct edge fades.

        Reads the per-symbol edge recorded by the latest
        ``generate_signals_async`` run (check_exit is synchronous and cannot
        await fresh quotes); a stale observation never triggers an exit.
        """
        side = _position_side_text(position)
        if side not in {"long", "short"}:
            return None
        symbol = str(getattr(position, "symbol", "") or "").strip()
        key = _normalize_symbol_key(symbol)
        if not key:
            return None

        obs = self._last_edge_obs.get(key)
        if not isinstance(obs, dict):
            return None
        now = datetime.now(timezone.utc)
        age_sec = _observation_age_sec(obs.get("at"), now)
        max_age = float(self.params.get("exit_price_max_age_sec", 300.0) or 0.0)
        if age_sec is None or (max_age > 0 and age_sec > max_age):
            return None
        try:
            edge_abs = float(obs.get("edge_abs"))
        except (TypeError, ValueError):
            return None
        if not math.isfinite(edge_abs):
            return None

        min_profit = self._resolve_min_profit()
        ratio = _resolve_exit_ratio(self.params)
        threshold = min_profit * ratio
        if edge_abs > threshold:
            return None

        return Signal(
            symbol=symbol,
            signal_type=SignalType.CLOSE_LONG if side == "long" else SignalType.CLOSE_SHORT,
            price=_close_signal_price(data, position),
            timestamp=self._bar_time(data),
            strategy_name=self.name,
            strength=0.7,
            metadata={
                "close_reason": "triangular_edge_converged",
                "close_only": True,
                "current_edge_abs": float(edge_abs),
                "exit_threshold": float(threshold),
                "entry_threshold": float(min_profit),
                "exit_spread_ratio": float(ratio),
                "observation_age_sec": float(age_sec),
            },
        )

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        return []

    async def generate_signals_async(self, symbol: str) -> List[Signal]:
        exchange_name = str(self.params.get("exchange", "binance"))
        connector = exchange_manager.get_exchange(exchange_name)
        if not connector:
            return []

        now = datetime.now(timezone.utc)
        symbol_key = str(symbol or "").upper()
        if not self._cooldown_ready(symbol_key, now):
            return []

        triangles = self._triangles or self._default_triangles(symbol)
        if not triangles:
            return []

        min_profit = self._resolve_min_profit()
        max_spread = max(min_profit, float(self.params.get("max_spread", 0.05) or 0.05))
        fee_drag = 3 * float(self.params.get("fee_rate", 0.001)) if bool(self.params.get("consider_fees", True)) else 0.0
        opportunities: List[Dict[str, Any]] = []
        # Edge observations are recorded for every priced triangle (even below
        # min_profit) so check_exit can detect convergence later.
        observed_edges: Dict[str, float] = {}

        for tri in triangles:
            if len(tri) != 3:
                continue
            direct_pair, mid_base_pair, mid_quote_pair = tri
            try:
                t_direct = await get_realtime_price(
                    exchange_name,
                    direct_pair,
                    connector=connector,
                    allow_rest_fallback=True,
                )
                t_mid_base = await get_realtime_price(
                    exchange_name,
                    mid_base_pair,
                    connector=connector,
                    allow_rest_fallback=True,
                )
                t_mid_quote = await get_realtime_price(
                    exchange_name,
                    mid_quote_pair,
                    connector=connector,
                    allow_rest_fallback=True,
                )
            except Exception:
                continue

            direct = float(t_direct.price or 0.0)
            mid_base = float(t_mid_base.price or 0.0)
            mid_quote = float(t_mid_quote.price or 0.0)
            edge, implied = self._edge_from_prices(direct, mid_base, mid_quote)
            edge_after_fee = edge - fee_drag if edge > 0 else edge + fee_drag
            if direct > 0 and mid_base > 0 and mid_quote > 0:
                obs_key = _normalize_symbol_key(direct_pair)
                if obs_key:
                    # Convergence is measured on the raw edge: the fee-adjusted
                    # edge saturates at ±fee_drag when the dislocation closes
                    # and would never fall below the exit threshold.
                    current_abs = abs(edge)
                    if current_abs > observed_edges.get(obs_key, -1.0):
                        observed_edges[obs_key] = current_abs
            if abs(edge_after_fee) < min_profit:
                continue
            if abs(edge_after_fee) > max_spread:
                continue

            opportunities.append(
                {
                    "triangle": tri,
                    "direct": direct,
                    "implied": implied,
                    "edge": edge_after_fee,
                    "timestamp": datetime.now(timezone.utc),
                }
            )

        if observed_edges:
            obs_at = datetime.now(timezone.utc)
            for obs_key, edge_abs in observed_edges.items():
                self._last_edge_obs[obs_key] = {"edge_abs": float(edge_abs), "at": obs_at}

        if not opportunities:
            return []

        opportunities.sort(key=lambda x: abs(float(x["edge"])), reverse=True)
        opportunities = opportunities[: max(1, int(self.params.get("max_opportunities", 2)))]

        signals: List[Signal] = []
        for opp in opportunities:
            edge = float(opp["edge"])
            signal_type = SignalType.BUY if edge > 0 else SignalType.SELL
            strength = max(0.1, min(abs(edge) / max(min_profit, 1e-9), 1.0))
            tri = opp["triangle"]
            bridge = tri[1].split("/")[0] if "/" in tri[1] else tri[1]
            signals.append(
                Signal(
                    symbol=tri[0],
                    signal_type=signal_type,
                    price=float(opp["direct"]),
                    timestamp=opp["timestamp"],
                    strategy_name=self.name,
                    strength=strength,
                    metadata={
                        "exchange": exchange_name,
                        "triangle": tri,
                        "bridge_asset": bridge,
                        "direct_price": float(opp["direct"]),
                        "implied_price": float(opp["implied"]),
                        "edge": edge,
                    },
                )
            )

        if signals:
            self._last_signal_at[symbol_key] = now

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {"type": "realtime_ticker", "exchange": self.params.get("exchange", "binance")}
