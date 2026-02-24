from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from loguru import logger

from core.exchanges import exchange_manager
from core.strategies.strategy_base import Signal, SignalType, StrategyBase


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
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)
        self._price_cache: Dict[str, Dict[str, Dict[str, float]]] = {}
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
        prices: Dict[str, Dict[str, float]] = {}
        for exchange_name in self.params.get("exchanges", []):
            connector = exchange_manager.get_exchange(exchange_name)
            if not connector or not connector.is_connected:
                continue
            try:
                ticker = await connector.get_ticker(symbol)
                bid = float(ticker.bid or 0.0)
                ask = float(ticker.ask or 0.0)
                last = float(ticker.last or 0.0)
                if bid > 0 and ask > 0:
                    prices[exchange_name] = {"bid": bid, "ask": ask, "last": last}
            except Exception as e:
                logger.debug(f"{self.name} ticker unavailable on {exchange_name}: {e}")

        self._price_cache[symbol] = prices
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
                        "timestamp": datetime.now(),
                    }
                )

        opportunities.sort(key=lambda x: float(x["effective_spread"]), reverse=True)
        max_n = max(1, int(self.params.get("max_opportunities", 2)))
        return opportunities[:max_n]

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        return []

    async def generate_signals_async(self, symbol: str) -> List[Signal]:
        now = datetime.now()
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
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)
        self._triangles: List[List[str]] = []
        self._last_signal_at: Dict[str, datetime] = {}

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
                opportunities.append({"triangle": triangle, "profit": profit, "timestamp": datetime.now()})
        opportunities.sort(key=lambda x: float(x["profit"]), reverse=True)
        return opportunities

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        return []

    async def generate_signals_async(self, symbol: str) -> List[Signal]:
        exchange_name = str(self.params.get("exchange", "binance"))
        connector = exchange_manager.get_exchange(exchange_name)
        if not connector:
            return []

        now = datetime.now()
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

        for tri in triangles:
            if len(tri) != 3:
                continue
            direct_pair, mid_base_pair, mid_quote_pair = tri
            try:
                t_direct = await connector.get_ticker(direct_pair)
                t_mid_base = await connector.get_ticker(mid_base_pair)
                t_mid_quote = await connector.get_ticker(mid_quote_pair)
            except Exception:
                continue

            direct = float(t_direct.last or 0.0)
            mid_base = float(t_mid_base.last or 0.0)
            mid_quote = float(t_mid_quote.last or 0.0)
            edge, implied = self._edge_from_prices(direct, mid_base, mid_quote)
            edge_after_fee = edge - fee_drag if edge > 0 else edge + fee_drag
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
                    "timestamp": datetime.now(),
                }
            )

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
