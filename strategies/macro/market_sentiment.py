"""Macro sentiment strategies with lightweight online data adapters."""
from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import httpx
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


class MarketSentimentStrategy(StrategyBase):
    """Global market sentiment strategy (fear/greed + momentum proxy)."""

    def __init__(
        self,
        name: str = "Market_Sentiment",
        params: Optional[Dict[str, Any]] = None,
    ):
        default_params = {
            "fear_threshold": 25,
            "greed_threshold": 75,
            "neutral_exit_enabled": True,
            "neutral_exit_buffer": 5,
            "lookback_period": 7,
            "stop_loss_pct": 0.05,
            "take_profit_pct": 0.10,
            "exchange": "binance",
            "timeout_sec": 6,
        }
        if params:
            default_params.update(params)

        super().__init__(name, default_params)
        self._sentiment_data: Dict[str, Any] = {}
        self._regime_bias: Dict[str, int] = {}

    def update_sentiment(
        self,
        fear_greed_index: int,
        social_sentiment: Optional[float] = None,
        news_sentiment: Optional[float] = None,
    ) -> None:
        self._sentiment_data = {
            "fear_greed_index": int(max(0, min(100, fear_greed_index))),
            "social_sentiment": float(social_sentiment or 0.0),
            "news_sentiment": float(news_sentiment or 0.0),
            "timestamp": datetime.now(),
        }

    async def _fetch_fear_greed_index(self) -> Optional[int]:
        url = "https://api.alternative.me/fng/?limit=1"
        try:
            async with httpx.AsyncClient(timeout=float(self.params.get("timeout_sec", 6))) as client:
                res = await client.get(url)
                res.raise_for_status()
                payload = res.json() or {}
            rows = payload.get("data") or []
            if not rows:
                return None
            return int(rows[0].get("value"))
        except Exception as e:
            logger.debug(f"{self.name} fear/greed unavailable: {e}")
            return None

    async def _fetch_symbol_momentum(self, symbol: str) -> Tuple[float, float]:
        exchange = str(self.params.get("exchange", "binance"))
        connector = exchange_manager.get_exchange(exchange)
        if not connector:
            return 0.0, 0.0

        try:
            ticker = await connector.get_ticker(symbol)
            last_px = float(ticker.last or 0.0)
        except Exception:
            last_px = 0.0

        try:
            klines = await connector.get_klines(symbol, "1h", limit=24)
            if not klines or len(klines) < 6:
                return last_px, 0.0
            closes = [float(k.close or 0.0) for k in klines if float(k.close or 0.0) > 0]
            if len(closes) < 6:
                return last_px, 0.0
            base = closes[-6]
            cur = closes[-1]
            if base <= 0:
                return (cur or last_px), 0.0
            change = (cur / base) - 1.0
            # Map roughly +/-20% to +/-1.
            score = max(-1.0, min(1.0, change / 0.20))
            return (cur or last_px), float(score)
        except Exception as e:
            logger.debug(f"{self.name} momentum unavailable for {symbol}: {e}")
            return last_px, 0.0

    async def generate_signals_async(self, symbol: str) -> List[Signal]:
        fgi = await self._fetch_fear_greed_index()
        last_px, momentum = await self._fetch_symbol_momentum(symbol)
        if fgi is None:
            # Fallback to momentum-derived pseudo sentiment index.
            fgi = int(max(0, min(100, round(50 + momentum * 35))))

        self.update_sentiment(
            fear_greed_index=fgi,
            social_sentiment=momentum,
            news_sentiment=0.0,
        )
        if last_px <= 0:
            return []

        df = pd.DataFrame({"close": [last_px], "symbol": [symbol]})
        return self.generate_signals(df)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        if not self._sentiment_data:
            return []

        signals: List[Signal] = []
        fgi = int(self._sentiment_data.get("fear_greed_index", 50))
        momentum = float(self._sentiment_data.get("social_sentiment", 0.0) or 0.0)
        timestamp = self._sentiment_data.get("timestamp", datetime.now())

        current_price = float(data["close"].iloc[-1]) if not data.empty else 0.0
        symbol = data.get("symbol", ["UNKNOWN"])[0] if "symbol" in data else "UNKNOWN"
        if current_price <= 0:
            return []

        fear_th = int(self.params["fear_threshold"])
        greed_th = int(self.params["greed_threshold"])
        neutral_buf = max(0, int(self.params.get("neutral_exit_buffer", 5)))
        neutral_low = min(greed_th, fear_th + neutral_buf)
        neutral_high = max(fear_th, greed_th - neutral_buf)

        if fgi <= int(self.params["fear_threshold"]):
            strength = 1 - (fgi / max(1.0, float(self.params["fear_threshold"])))
            strength = max(0.0, min(1.0, strength + max(0.0, momentum) * 0.2))
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
                        "fear_greed_index": fgi,
                        "sentiment": "extreme_fear",
                        "momentum_score": round(momentum, 4),
                    },
                )
            )
            logger.info(f"{self.name} BUY {symbol} FGI={fgi} momentum={momentum:.3f}")

        elif fgi >= int(self.params["greed_threshold"]):
            strength = (fgi - float(self.params["greed_threshold"])) / max(
                1.0, (100 - float(self.params["greed_threshold"]))
            )
            strength = max(0.0, min(1.0, strength + max(0.0, -momentum) * 0.2))
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
                        "fear_greed_index": fgi,
                        "sentiment": "extreme_greed",
                        "momentum_score": round(momentum, 4),
                    },
                )
            )
            logger.info(f"{self.name} SELL {symbol} FGI={fgi} momentum={momentum:.3f}")
        elif bool(self.params.get("neutral_exit_enabled", True)) and (neutral_low <= fgi <= neutral_high):
            prev_bias = int(self._regime_bias.get(symbol, 0) or 0)
            close_signal = _emit_neutral_close_signal(
                symbol=symbol,
                price=current_price,
                timestamp=timestamp,
                strategy_name=self.name,
                prev_bias=prev_bias,
                metadata={
                    "fear_greed_index": fgi,
                    "momentum_score": round(momentum, 4),
                    "macro_exit_reason": "sentiment_back_to_neutral",
                    "neutral_band": [neutral_low, neutral_high],
                },
            )
            if close_signal is not None:
                signals.append(close_signal)
                self._regime_bias.pop(symbol, None)
                logger.info(f"{self.name} CLOSE {symbol} FGI={fgi} neutral_band=[{neutral_low},{neutral_high}]")

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "sentiment",
            "sources": ["fear_greed_index", "market_momentum"],
        }


class SocialSentimentStrategy(StrategyBase):
    """Social sentiment strategy based on trending + price-action proxy."""

    def __init__(
        self,
        name: str = "Social_Sentiment",
        params: Optional[Dict[str, Any]] = None,
    ):
        default_params = {
            "positive_threshold": 0.22,
            "negative_threshold": -0.22,
            "neutral_exit_enabled": True,
            "neutral_exit_buffer": 0.08,
            "min_mentions": 25,
            "stop_loss_pct": 0.05,
            "take_profit_pct": 0.10,
            "timeout_sec": 6,
        }
        if params:
            default_params.update(params)

        super().__init__(name, default_params)
        self._social_data: Dict[str, Any] = {}
        self._regime_bias: Dict[str, int] = {}

    def update_social_data(
        self,
        mentions: int,
        sentiment_score: float,
        trending_score: float = 0.0,
    ) -> None:
        self._social_data = {
            "mentions": int(max(0, mentions)),
            "sentiment_score": float(max(-1.0, min(1.0, sentiment_score))),
            "trending_score": float(max(0.0, min(1.0, trending_score))),
            "timestamp": datetime.now(),
        }

    @staticmethod
    def _base_asset(symbol: str) -> str:
        return str(symbol or "").split("/")[0].upper() if symbol else "BTC"

    @staticmethod
    def _to_binance_symbol(symbol: str) -> str:
        return str(symbol or "").replace("/", "").upper()

    async def _fetch_trending_proxy(self, base_asset: str) -> Tuple[int, float]:
        url = "https://api.coingecko.com/api/v3/search/trending"
        try:
            async with httpx.AsyncClient(timeout=float(self.params.get("timeout_sec", 6))) as client:
                res = await client.get(url)
                res.raise_for_status()
                payload = res.json() or {}
            rows = payload.get("coins") or []
            mentions = 0
            rank_weight = 0.0
            for idx, row in enumerate(rows):
                item = row.get("item") or {}
                sym = str(item.get("symbol") or "").upper()
                if sym != base_asset:
                    continue
                mentions += 1
                rank_weight += max(0.0, 1.0 - idx * 0.1)
            # map to rough "mentions" scale and trending score [0,1]
            mention_count = int(20 + mentions * 80)
            trending_score = max(0.0, min(1.0, rank_weight))
            return mention_count, trending_score
        except Exception as e:
            logger.debug(f"{self.name} trending proxy unavailable: {e}")
            return 20, 0.0

    async def _fetch_price_proxy(self, symbol: str) -> Tuple[float, float]:
        pair = self._to_binance_symbol(symbol)
        url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={pair}"
        try:
            async with httpx.AsyncClient(timeout=float(self.params.get("timeout_sec", 6))) as client:
                res = await client.get(url)
                res.raise_for_status()
                payload = res.json() or {}
            change_pct = float(payload.get("priceChangePercent") or 0.0)
            last = float(payload.get("lastPrice") or 0.0)
            return change_pct, last
        except Exception as e:
            logger.debug(f"{self.name} 24h ticker proxy unavailable for {symbol}: {e}")
            return 0.0, 0.0

    @staticmethod
    def _score_sentiment(price_change_pct: float, trending_score: float) -> float:
        # tanh maps large pct changes to bounded range.
        price_component = math.tanh(float(price_change_pct) / 8.0)
        score = price_component * 0.75 + float(trending_score) * 0.25
        return max(-1.0, min(1.0, score))

    async def generate_signals_async(self, symbol: str) -> List[Signal]:
        base = self._base_asset(symbol)
        mentions, trending_score = await self._fetch_trending_proxy(base)
        price_change_pct, last_price = await self._fetch_price_proxy(symbol)
        sentiment = self._score_sentiment(price_change_pct, trending_score)

        mention_floor = int(self.params.get("min_mentions", 40))
        effective_mentions = max(mentions, mention_floor if abs(sentiment) >= 0.25 else mentions)
        self.update_social_data(
            mentions=effective_mentions,
            sentiment_score=sentiment,
            trending_score=trending_score,
        )
        if last_price <= 0:
            return []

        df = pd.DataFrame({"close": [last_price], "symbol": [symbol]})
        return self.generate_signals(df)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        if not self._social_data:
            return []

        mentions = int(self._social_data.get("mentions", 0))
        sentiment = float(self._social_data.get("sentiment_score", 0.0))
        trending = float(self._social_data.get("trending_score", 0.0))
        timestamp = self._social_data.get("timestamp", datetime.now())

        if mentions < int(self.params["min_mentions"]):
            return []

        current_price = float(data["close"].iloc[-1]) if not data.empty else 0.0
        symbol = data.get("symbol", ["UNKNOWN"])[0] if "symbol" in data else "UNKNOWN"
        if current_price <= 0:
            return []

        signals: List[Signal] = []
        pos_th = float(self.params["positive_threshold"])
        neg_th = float(self.params["negative_threshold"])
        neutral_buf = max(0.0, float(self.params.get("neutral_exit_buffer", 0.08)))
        neutral_low = min(pos_th, neg_th + neutral_buf)
        neutral_high = max(neg_th, pos_th - neutral_buf)
        if sentiment >= pos_th:
            self._regime_bias[symbol] = 1
            signals.append(
                Signal(
                    symbol=symbol,
                    signal_type=SignalType.BUY,
                    price=current_price,
                    timestamp=timestamp,
                    strategy_name=self.name,
                    strength=min(1.0, sentiment + trending * 0.2),
                    stop_loss=current_price * (1 - float(self.params["stop_loss_pct"])),
                    take_profit=current_price * (1 + float(self.params["take_profit_pct"])),
                    metadata={
                        "sentiment_score": round(sentiment, 4),
                        "mentions": mentions,
                        "trending_score": round(trending, 4),
                    },
                )
            )
            logger.info(f"{self.name} BUY {symbol} sentiment={sentiment:.3f} mentions={mentions}")
        elif sentiment <= neg_th:
            self._regime_bias[symbol] = -1
            signals.append(
                Signal(
                    symbol=symbol,
                    signal_type=SignalType.SELL,
                    price=current_price,
                    timestamp=timestamp,
                    strategy_name=self.name,
                    strength=min(1.0, abs(sentiment) + trending * 0.2),
                    stop_loss=current_price * (1 + float(self.params["stop_loss_pct"])),
                    take_profit=current_price * (1 - float(self.params["take_profit_pct"])),
                    metadata={
                        "sentiment_score": round(sentiment, 4),
                        "mentions": mentions,
                        "trending_score": round(trending, 4),
                    },
                )
            )
            logger.info(f"{self.name} SELL {symbol} sentiment={sentiment:.3f} mentions={mentions}")
        elif bool(self.params.get("neutral_exit_enabled", True)) and (neutral_low <= sentiment <= neutral_high):
            prev_bias = int(self._regime_bias.get(symbol, 0) or 0)
            close_signal = _emit_neutral_close_signal(
                symbol=symbol,
                price=current_price,
                timestamp=timestamp,
                strategy_name=self.name,
                prev_bias=prev_bias,
                metadata={
                    "sentiment_score": round(sentiment, 4),
                    "mentions": mentions,
                    "trending_score": round(trending, 4),
                    "macro_exit_reason": "social_sentiment_back_to_neutral",
                    "neutral_band": [round(neutral_low, 4), round(neutral_high, 4)],
                },
            )
            if close_signal is not None:
                signals.append(close_signal)
                self._regime_bias.pop(symbol, None)
                logger.info(f"{self.name} CLOSE {symbol} sentiment={sentiment:.3f} neutral")

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "social_sentiment",
            "sources": ["coingecko_trending", "binance_24h_ticker"],
        }
