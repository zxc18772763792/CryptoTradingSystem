"""Fund-flow and whale-activity macro strategies."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from loguru import logger

from core.exchanges import exchange_manager
from core.strategies.strategy_base import Signal, SignalType, StrategyBase


def _emit_neutral_close_signal(
    *,
    symbol: str,
    price: float,
    timestamp: datetime,
    strategy_name: str,
    prev_bias: int,
    metadata: Optional[Dict[str, Any]] = None,
) -> Optional[Signal]:
    if prev_bias > 0:
        sig_type = SignalType.CLOSE_LONG
    elif prev_bias < 0:
        sig_type = SignalType.CLOSE_SHORT
    else:
        return None
    return Signal(
        symbol=symbol,
        signal_type=sig_type,
        price=price,
        timestamp=timestamp,
        strategy_name=strategy_name,
        strength=0.5,
        metadata=dict(metadata or {}),
    )


def _safe_book_level(level: Any) -> Tuple[float, float]:
    if isinstance(level, (list, tuple)) and len(level) >= 2:
        try:
            return float(level[0]), float(level[1])
        except Exception:
            return 0.0, 0.0
    if isinstance(level, dict):
        try:
            return float(level.get("price", 0.0)), float(level.get("amount", 0.0))
        except Exception:
            return 0.0, 0.0
    return 0.0, 0.0


class FundFlowStrategy(StrategyBase):
    """Order-book flow proxy strategy."""

    def __init__(
        self,
        name: str = "Fund_Flow",
        params: Optional[Dict[str, Any]] = None,
    ):
        default_params = {
            "inflow_threshold": 150000.0,
            "outflow_threshold": -150000.0,
            "min_imbalance_ratio": 0.03,
            "neutral_exit_enabled": True,
            "neutral_exit_imbalance_ratio": 0.01,
            "lookback_period": 7,
            "book_depth": 80,
            "exchange": "binance",
            "stop_loss_pct": 0.05,
            "take_profit_pct": 0.10,
        }
        if params:
            default_params.update(params)

        super().__init__(name, default_params)
        self._flow_data: Dict[str, Any] = {}
        self._flow_history: List[Dict[str, Any]] = []
        self._regime_bias: Dict[str, int] = {}

    def update_flow_data(
        self,
        exchange_inflow: float,
        exchange_outflow: float,
        whale_activity: Optional[float] = None,
    ) -> None:
        net_flow = float(exchange_inflow) - float(exchange_outflow)
        self._flow_data = {
            "exchange_inflow": float(exchange_inflow),
            "exchange_outflow": float(exchange_outflow),
            "net_flow": net_flow,
            "whale_activity": float(whale_activity or 0.0),
            "timestamp": datetime.now(),
        }

        self._flow_history.append(self._flow_data)
        if len(self._flow_history) > 60:
            self._flow_history = self._flow_history[-60:]

    async def _fetch_orderbook_flow(self, symbol: str) -> Tuple[float, float, float]:
        exchange = str(self.params.get("exchange", "binance"))
        depth = max(10, min(int(self.params.get("book_depth", 80)), 200))
        connector = exchange_manager.get_exchange(exchange)
        if not connector:
            return 0.0, 0.0, 0.0

        try:
            book = await connector.get_order_book(symbol, limit=depth)
            bids = book.get("bids") or []
            asks = book.get("asks") or []
        except Exception as e:
            logger.debug(f"{self.name} order book unavailable for {symbol}: {e}")
            return 0.0, 0.0, 0.0

        bid_notional = 0.0
        ask_notional = 0.0
        best_bid = 0.0
        best_ask = 0.0

        for i, level in enumerate(bids):
            px, qty = _safe_book_level(level)
            if px <= 0 or qty <= 0:
                continue
            if i == 0:
                best_bid = px
            bid_notional += px * qty

        for i, level in enumerate(asks):
            px, qty = _safe_book_level(level)
            if px <= 0 or qty <= 0:
                continue
            if i == 0:
                best_ask = px
            ask_notional += px * qty

        mid = (best_bid + best_ask) / 2 if best_bid > 0 and best_ask > 0 else max(best_bid, best_ask, 0.0)
        return bid_notional, ask_notional, mid

    async def generate_signals_async(self, symbol: str) -> List[Signal]:
        bid_notional, ask_notional, mid_price = await self._fetch_orderbook_flow(symbol)
        if bid_notional <= 0 and ask_notional <= 0:
            return []

        total = bid_notional + ask_notional
        net = bid_notional - ask_notional
        imbalance_ratio = (net / total) if total > 0 else 0.0
        self.update_flow_data(
            exchange_inflow=bid_notional,
            exchange_outflow=ask_notional,
            whale_activity=abs(imbalance_ratio),
        )

        if mid_price <= 0:
            return []
        df = pd.DataFrame({"close": [mid_price], "symbol": [symbol]})
        return self.generate_signals(df)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        if not self._flow_data:
            return []

        net_flow = float(self._flow_data.get("net_flow", 0.0))
        inflow = float(self._flow_data.get("exchange_inflow", 0.0))
        outflow = float(self._flow_data.get("exchange_outflow", 0.0))
        timestamp = self._flow_data.get("timestamp", datetime.now())

        current_price = float(data["close"].iloc[-1]) if not data.empty else 0.0
        symbol = data.get("symbol", ["UNKNOWN"])[0] if "symbol" in data else "UNKNOWN"
        if current_price <= 0:
            return []

        total = inflow + outflow
        imbalance_ratio = (net_flow / total) if total > 0 else 0.0
        min_ratio = abs(float(self.params.get("min_imbalance_ratio", 0.08)))

        signals: List[Signal] = []
        if net_flow >= float(self.params["inflow_threshold"]) and imbalance_ratio >= min_ratio:
            strength = min(1.0, abs(imbalance_ratio) / max(min_ratio, 1e-6))
            self._regime_bias[symbol] = 1
            signals.append(
                Signal(
                    symbol=symbol,
                    signal_type=SignalType.BUY,
                    price=current_price,
                    timestamp=timestamp,
                    strategy_name=self.name,
                    strength=strength,
                    stop_loss=current_price * (1 - float(self.params["stop_loss_pct"])),
                    take_profit=current_price * (1 + float(self.params["take_profit_pct"])),
                    metadata={
                        "net_flow": round(net_flow, 4),
                        "imbalance_ratio": round(imbalance_ratio, 6),
                    },
                )
            )
            logger.info(f"{self.name} BUY {symbol} net={net_flow:.2f} ratio={imbalance_ratio:.4f}")
        elif net_flow <= float(self.params["outflow_threshold"]) and imbalance_ratio <= -min_ratio:
            strength = min(1.0, abs(imbalance_ratio) / max(min_ratio, 1e-6))
            self._regime_bias[symbol] = -1
            signals.append(
                Signal(
                    symbol=symbol,
                    signal_type=SignalType.SELL,
                    price=current_price,
                    timestamp=timestamp,
                    strategy_name=self.name,
                    strength=strength,
                    stop_loss=current_price * (1 + float(self.params["stop_loss_pct"])),
                    take_profit=current_price * (1 - float(self.params["take_profit_pct"])),
                    metadata={
                        "net_flow": round(net_flow, 4),
                        "imbalance_ratio": round(imbalance_ratio, 6),
                    },
                )
            )
            logger.info(f"{self.name} SELL {symbol} net={net_flow:.2f} ratio={imbalance_ratio:.4f}")
        elif bool(self.params.get("neutral_exit_enabled", True)):
            neutral_ratio = abs(float(self.params.get("neutral_exit_imbalance_ratio", min_ratio * 0.5)))
            if abs(imbalance_ratio) <= neutral_ratio:
                prev_bias = int(self._regime_bias.get(symbol, 0) or 0)
                close_signal = _emit_neutral_close_signal(
                    symbol=symbol,
                    price=current_price,
                    timestamp=timestamp,
                    strategy_name=self.name,
                    prev_bias=prev_bias,
                    metadata={
                        "net_flow": round(net_flow, 4),
                        "imbalance_ratio": round(imbalance_ratio, 6),
                        "macro_exit_reason": "fund_flow_back_to_neutral",
                        "neutral_exit_imbalance_ratio": round(neutral_ratio, 6),
                    },
                )
                if close_signal is not None:
                    signals.append(close_signal)
                    self._regime_bias.pop(symbol, None)
                    logger.info(f"{self.name} CLOSE {symbol} net={net_flow:.2f} ratio={imbalance_ratio:.4f}")

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "fund_flow",
            "sources": ["orderbook_imbalance", "whale_activity_proxy"],
        }


class WhaleActivityStrategy(StrategyBase):
    """Whale activity strategy from large public trades."""

    def __init__(
        self,
        name: str = "Whale_Activity",
        params: Optional[Dict[str, Any]] = None,
    ):
        default_params = {
            "min_whale_size": 50000.0,
            "accumulation_threshold": 2,
            "distribution_threshold": 2,
            "neutral_exit_enabled": True,
            "lookback_hours": 24,
            "exchange": "binance",
            "trade_limit": 600,
            "stop_loss_pct": 0.05,
            "take_profit_pct": 0.10,
        }
        if params:
            default_params.update(params)

        super().__init__(name, default_params)
        self._whale_transactions: List[Dict[str, Any]] = []
        self._seen_trade_ids: set[str] = set()
        self._regime_bias: Dict[str, int] = {}

    def add_whale_transaction(
        self,
        amount: float,
        direction: str,
        price: float,
        usd_value: float,
        trade_id: Optional[str] = None,
    ) -> None:
        if trade_id:
            tid = str(trade_id)
            if tid in self._seen_trade_ids:
                return
            self._seen_trade_ids.add(tid)
            if len(self._seen_trade_ids) > 20000:
                self._seen_trade_ids = set(list(self._seen_trade_ids)[-12000:])

        self._whale_transactions.append(
            {
                "amount": float(amount),
                "direction": str(direction).lower(),
                "price": float(price),
                "usd_value": float(usd_value),
                "timestamp": datetime.now(),
            }
        )

        cutoff = datetime.now() - pd.Timedelta(hours=float(self.params["lookback_hours"]))
        self._whale_transactions = [t for t in self._whale_transactions if t["timestamp"] > cutoff]

    @staticmethod
    def _infer_side(raw: Dict[str, Any]) -> str:
        side = str(raw.get("side") or "").lower()
        if side in {"buy", "sell"}:
            return side
        # ccxt unified trades: takerOrMaker may exist but side may be missing.
        if bool(raw.get("takerOrMaker")):
            return "sell"
        return "buy"

    async def _pull_whale_trades(self, symbol: str) -> Tuple[int, float]:
        exchange = str(self.params.get("exchange", "binance"))
        connector = exchange_manager.get_exchange(exchange)
        if not connector:
            return 0, 0.0

        client = getattr(connector, "_client", None)
        fetch_trades = getattr(client, "fetch_trades", None)
        if not callable(fetch_trades):
            return 0, 0.0

        try:
            limit = max(50, min(int(self.params.get("trade_limit", 600)), 1000))
            trades = await fetch_trades(symbol, limit=limit)
        except Exception as e:
            logger.debug(f"{self.name} trade fetch unavailable for {symbol}: {e}")
            return 0, 0.0

        min_size = float(self.params.get("min_whale_size", 150000.0))
        added = 0
        latest_px = 0.0
        for t in trades or []:
            price = float(t.get("price") or 0.0)
            amount = float(t.get("amount") or 0.0)
            usd_value = price * amount
            if price > 0:
                latest_px = price
            if usd_value < min_size:
                continue

            trade_id = t.get("id")
            if trade_id is None:
                trade_id = f"{t.get('timestamp')}|{price}|{amount}"

            before = len(self._whale_transactions)
            self.add_whale_transaction(
                amount=amount,
                direction=self._infer_side(t),
                price=price,
                usd_value=usd_value,
                trade_id=str(trade_id),
            )
            if len(self._whale_transactions) > before:
                added += 1

        return added, latest_px

    def analyze_whale_activity(self) -> Dict[str, Any]:
        if not self._whale_transactions:
            return {"status": "no_activity"}

        buys = [t for t in self._whale_transactions if t["direction"] == "buy"]
        sells = [t for t in self._whale_transactions if t["direction"] == "sell"]

        buy_volume = sum(float(t["usd_value"]) for t in buys)
        sell_volume = sum(float(t["usd_value"]) for t in sells)
        total = buy_volume + sell_volume

        return {
            "status": "active",
            "buy_count": len(buys),
            "sell_count": len(sells),
            "buy_volume": buy_volume,
            "sell_volume": sell_volume,
            "net_volume": buy_volume - sell_volume,
            "buy_ratio": buy_volume / total if total > 0 else 0.5,
            "total_volume": total,
        }

    async def generate_signals_async(self, symbol: str) -> List[Signal]:
        _, latest_px = await self._pull_whale_trades(symbol)
        if latest_px <= 0:
            exchange = str(self.params.get("exchange", "binance"))
            connector = exchange_manager.get_exchange(exchange)
            if connector:
                try:
                    ticker = await connector.get_ticker(symbol)
                    latest_px = float(ticker.last or 0.0)
                except Exception:
                    latest_px = 0.0
        if latest_px <= 0:
            return []

        df = pd.DataFrame({"close": [latest_px], "symbol": [symbol]})
        return self.generate_signals(df)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        analysis = self.analyze_whale_activity()
        if analysis.get("status") == "no_activity":
            return []

        current_price = float(data["close"].iloc[-1]) if not data.empty else 0.0
        symbol = data.get("symbol", ["UNKNOWN"])[0] if "symbol" in data else "UNKNOWN"
        if current_price <= 0:
            return []

        signals: List[Signal] = []
        if int(analysis.get("buy_count", 0)) >= int(self.params["accumulation_threshold"]):
            self._regime_bias[symbol] = 1
            signals.append(
                Signal(
                    symbol=symbol,
                    signal_type=SignalType.BUY,
                    price=current_price,
                    timestamp=datetime.now(),
                    strategy_name=self.name,
                    strength=min(1.0, float(analysis.get("buy_ratio", 0.5))),
                    stop_loss=current_price * (1 - float(self.params["stop_loss_pct"])),
                    take_profit=current_price * (1 + float(self.params["take_profit_pct"])),
                    metadata={"whale_analysis": analysis, "pattern": "accumulation"},
                )
            )
            logger.info(
                f"{self.name} BUY {symbol} whale_buy_count={analysis.get('buy_count', 0)} "
                f"ratio={analysis.get('buy_ratio', 0.0):.3f}"
            )
        elif int(analysis.get("sell_count", 0)) >= int(self.params["distribution_threshold"]):
            self._regime_bias[symbol] = -1
            signals.append(
                Signal(
                    symbol=symbol,
                    signal_type=SignalType.SELL,
                    price=current_price,
                    timestamp=datetime.now(),
                    strategy_name=self.name,
                    strength=min(1.0, 1 - float(analysis.get("buy_ratio", 0.5))),
                    stop_loss=current_price * (1 + float(self.params["stop_loss_pct"])),
                    take_profit=current_price * (1 - float(self.params["take_profit_pct"])),
                    metadata={"whale_analysis": analysis, "pattern": "distribution"},
                )
            )
            logger.info(
                f"{self.name} SELL {symbol} whale_sell_count={analysis.get('sell_count', 0)} "
                f"ratio={analysis.get('buy_ratio', 0.0):.3f}"
            )
        elif bool(self.params.get("neutral_exit_enabled", True)):
            prev_bias = int(self._regime_bias.get(symbol, 0) or 0)
            close_signal = _emit_neutral_close_signal(
                symbol=symbol,
                price=current_price,
                timestamp=datetime.now(),
                strategy_name=self.name,
                prev_bias=prev_bias,
                metadata={"whale_analysis": analysis, "macro_exit_reason": "whale_activity_back_to_neutral"},
            )
            if close_signal is not None:
                signals.append(close_signal)
                self._regime_bias.pop(symbol, None)
                logger.info(f"{self.name} CLOSE {symbol} whale activity neutralized")

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "whale_tracking",
            "min_size": float(self.params.get("min_whale_size", 150000.0)),
        }
