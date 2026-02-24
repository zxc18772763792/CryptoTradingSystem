from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import pandas as pd
from loguru import logger

from core.data import data_storage
from core.data.factor_library import build_factor_library
from core.strategies.strategy_base import Signal, SignalType, StrategyBase


def _normalize_symbol_list(symbols: List[Any]) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for item in symbols or []:
        text = str(item or "").strip().upper().replace("_", "/")
        if not text:
            continue
        if "/" not in text:
            text = f"{text}/USDT"
        if text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


class FamaFactorArbitrageStrategy(StrategyBase):
    """
    Cross-sectional factor arbitrage strategy:
    - Build Fama-style factor scores on a multi-asset universe.
    - Long top-score basket, short bottom-score basket.
    - Rebalance on fixed interval.
    """

    def __init__(self, name: str = "Fama_Factor_Arbitrage", params: Optional[Dict[str, Any]] = None):
        default_params: Dict[str, Any] = {
            "exchange": "binance",
            "factor_timeframe": "1h",
            "universe_symbols": [
                "BTC/USDT",
                "ETH/USDT",
                "BNB/USDT",
                "SOL/USDT",
                "XRP/USDT",
                "DOGE/USDT",
                "ADA/USDT",
                "AVAX/USDT",
                "LINK/USDT",
                "DOT/USDT",
            ],
            "max_symbols": 100,
            "lookback_bars": 720,
            "min_symbol_bars": 300,
            "min_universe_size": 12,
            "quantile": 0.25,
            "top_n": 8,
            "min_abs_score": 0.15,
            "alpha_threshold": 0.15,
            "rebalance_interval_minutes": 60,
            "cooldown_min": 60,
            "max_vol": 0.20,
            "max_spread": 0.08,
            "stop_loss_pct": 0.03,
            "take_profit_pct": 0.06,
            "market_type": "future",
            "allow_long": True,
            "allow_short": True,
            "reverse_on_signal": True,
            "allow_pyramiding": False,
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)

        self._last_rebalance_at: Optional[datetime] = None
        self._active_longs: Set[str] = set()
        self._active_shorts: Set[str] = set()

    def _trigger_symbol(self, universe: List[str]) -> str:
        configured = str(self.params.get("trigger_symbol", "")).strip().upper().replace("_", "/")
        if configured:
            return configured
        return universe[0] if universe else "BTC/USDT"

    def _is_rebalance_due(self, now: datetime) -> bool:
        if self._last_rebalance_at is None:
            return True
        interval_raw = self.params.get(
            "rebalance_interval_minutes",
            self.params.get("cooldown_min", 60),
        )
        interval_minutes = max(1, int(float(interval_raw or 60)))
        elapsed = (now - self._last_rebalance_at).total_seconds()
        return elapsed >= interval_minutes * 60

    @staticmethod
    def _score_strength(score: float, floor: float = 0.15) -> float:
        raw = min(1.0, max(0.2, abs(float(score)) * 0.8))
        return max(raw, min(1.0, max(0.2, floor)))

    @staticmethod
    def _make_signal(
        strategy_name: str,
        symbol: str,
        signal_type: SignalType,
        price: float,
        score: float,
        leg: str,
        stop_loss_pct: float,
        take_profit_pct: float,
        universe_size: int,
        quantile: float,
    ) -> Signal:
        strength = FamaFactorArbitrageStrategy._score_strength(score)
        stop_loss = None
        take_profit = None
        if signal_type == SignalType.BUY:
            stop_loss = price * (1 - stop_loss_pct)
            take_profit = price * (1 + take_profit_pct)
        elif signal_type == SignalType.SELL:
            stop_loss = price * (1 + stop_loss_pct)
            take_profit = price * (1 - take_profit_pct)

        return Signal(
            symbol=symbol,
            signal_type=signal_type,
            price=price,
            timestamp=datetime.utcnow(),
            strategy_name=strategy_name,
            strength=strength,
            stop_loss=stop_loss,
            take_profit=take_profit,
            metadata={
                "factor_score": round(float(score), 6),
                "factor_leg": leg,
                "universe_size": int(universe_size),
                "quantile": float(quantile),
                "model": "fama_cross_sectional",
            },
        )

    async def _load_universe_frames(self, universe: List[str]) -> Dict[str, pd.DataFrame]:
        exchange = str(self.params.get("exchange", "binance")).strip().lower()
        timeframe = str(self.params.get("factor_timeframe", "1h")).strip().lower()
        lookback = max(120, int(self.params.get("lookback_bars", 720)))
        min_rows = max(80, int(self.params.get("min_symbol_bars", 300)))
        max_symbols = max(8, int(self.params.get("max_symbols", 100)))

        out: Dict[str, pd.DataFrame] = {}
        for symbol in universe[:max_symbols]:
            df = await data_storage.load_klines_from_parquet(
                exchange=exchange,
                symbol=symbol,
                timeframe=timeframe,
            )
            if df.empty:
                continue
            tail = df.tail(lookback).copy()
            if len(tail) < min_rows:
                continue
            tail.index = pd.to_datetime(tail.index)
            tail = tail.sort_index()
            if "close" not in tail.columns or "volume" not in tail.columns:
                continue
            out[symbol] = tail
        return out

    async def generate_signals_async(self, symbol: str) -> List[Signal]:
        universe = _normalize_symbol_list(list(self.params.get("universe_symbols") or []))
        if len(universe) < 2:
            return []

        trigger = self._trigger_symbol(universe)
        if str(symbol or "").strip().upper().replace("_", "/") != trigger:
            return []

        now = datetime.utcnow()
        if not self._is_rebalance_due(now):
            return []

        frames = await self._load_universe_frames(universe)
        max_vol = max(0.0, float(self.params.get("max_vol", 0.0) or 0.0))
        max_spread = max(0.0, float(self.params.get("max_spread", 0.0) or 0.0))
        if max_vol > 0 or max_spread > 0:
            filtered_frames: Dict[str, pd.DataFrame] = {}
            for sym, frame in frames.items():
                keep = True
                if max_vol > 0:
                    ret = frame["close"].astype(float).pct_change().replace([float("inf"), -float("inf")], float("nan")).dropna()
                    if len(ret) >= 20 and float(ret.std(ddof=0)) > max_vol:
                        keep = False
                if keep and max_spread > 0:
                    close_safe = frame["close"].astype(float).replace(0.0, float("nan"))
                    spread_series = (
                        (frame["high"].astype(float) - frame["low"].astype(float)) / close_safe
                    ).replace([float("inf"), -float("inf")], float("nan")).dropna()
                    if len(spread_series) >= 20 and float(spread_series.median()) > max_spread:
                        keep = False
                if keep:
                    filtered_frames[sym] = frame
            if len(filtered_frames) != len(frames):
                logger.info(
                    f"{self.name} universe filtered by max_vol/max_spread: {len(frames)} -> {len(filtered_frames)}"
                )
            frames = filtered_frames

        if len(frames) < max(2, int(self.params.get("min_universe_size", 12))):
            logger.warning(f"{self.name} skipped: insufficient universe data ({len(frames)})")
            return []

        close_df = pd.DataFrame({sym: df["close"] for sym, df in frames.items()}).sort_index().ffill()
        volume_df = pd.DataFrame({sym: df["volume"] for sym, df in frames.items()}).sort_index().fillna(0.0)
        common_idx = close_df.index.intersection(volume_df.index)
        close_df = close_df.reindex(common_idx).dropna(axis=1, how="all").ffill()
        volume_df = volume_df.reindex(common_idx).fillna(0.0)

        if close_df.empty or close_df.shape[1] < 2:
            return []

        quantile = max(0.05, min(0.45, float(self.params.get("quantile", 0.25))))
        factor_result = build_factor_library(close_df=close_df, volume_df=volume_df, quantile=quantile)
        scores_df = factor_result.asset_scores.copy()
        if scores_df.empty:
            return []

        scores_df["symbol"] = scores_df["symbol"].astype(str).str.upper()
        scores_df = scores_df.dropna(subset=["score"]).sort_values("score", ascending=False)
        min_abs_score = max(
            0.0,
            float(self.params.get("min_abs_score", self.params.get("alpha_threshold", 0.15)) or 0.0),
        )
        if min_abs_score > 0:
            scores_df = scores_df[scores_df["score"].abs() >= min_abs_score]
        if scores_df.empty:
            return []

        top_n = max(1, int(self.params.get("top_n", 8)))
        max_n = max(1, len(scores_df) // 2)
        n = min(top_n, max_n)
        if n < 1:
            return []

        long_rows = scores_df.head(n)
        short_rows = scores_df.tail(n).sort_values("score", ascending=True)

        target_longs = {str(x).upper() for x in long_rows["symbol"].tolist()}
        target_shorts = {str(x).upper() for x in short_rows["symbol"].tolist()}
        last_close = close_df.iloc[-1].to_dict()
        stop_loss_pct = max(0.0, float(self.params.get("stop_loss_pct", 0.03)))
        take_profit_pct = max(0.0, float(self.params.get("take_profit_pct", 0.06)))

        score_map = {str(r["symbol"]).upper(): float(r["score"]) for _, r in scores_df.iterrows()}
        universe_size = int(close_df.shape[1])
        signals: List[Signal] = []

        # Exit stale legs first.
        for sym in sorted(self._active_longs - target_longs):
            px = float(last_close.get(sym, 0.0) or 0.0)
            if px <= 0:
                continue
            signals.append(
                self._make_signal(
                    strategy_name=self.name,
                    symbol=sym,
                    signal_type=SignalType.CLOSE_LONG,
                    price=px,
                    score=score_map.get(sym, 0.0),
                    leg="exit_long",
                    stop_loss_pct=stop_loss_pct,
                    take_profit_pct=take_profit_pct,
                    universe_size=universe_size,
                    quantile=quantile,
                )
            )

        for sym in sorted(self._active_shorts - target_shorts):
            px = float(last_close.get(sym, 0.0) or 0.0)
            if px <= 0:
                continue
            signals.append(
                self._make_signal(
                    strategy_name=self.name,
                    symbol=sym,
                    signal_type=SignalType.CLOSE_SHORT,
                    price=px,
                    score=score_map.get(sym, 0.0),
                    leg="exit_short",
                    stop_loss_pct=stop_loss_pct,
                    take_profit_pct=take_profit_pct,
                    universe_size=universe_size,
                    quantile=quantile,
                )
            )

        # Open/hold target legs.
        for sym in sorted(target_longs - self._active_longs):
            px = float(last_close.get(sym, 0.0) or 0.0)
            if px <= 0:
                continue
            signals.append(
                self._make_signal(
                    strategy_name=self.name,
                    symbol=sym,
                    signal_type=SignalType.BUY,
                    price=px,
                    score=score_map.get(sym, 0.0),
                    leg="long",
                    stop_loss_pct=stop_loss_pct,
                    take_profit_pct=take_profit_pct,
                    universe_size=universe_size,
                    quantile=quantile,
                )
            )

        for sym in sorted(target_shorts - self._active_shorts):
            px = float(last_close.get(sym, 0.0) or 0.0)
            if px <= 0:
                continue
            signals.append(
                self._make_signal(
                    strategy_name=self.name,
                    symbol=sym,
                    signal_type=SignalType.SELL,
                    price=px,
                    score=score_map.get(sym, 0.0),
                    leg="short",
                    stop_loss_pct=stop_loss_pct,
                    take_profit_pct=take_profit_pct,
                    universe_size=universe_size,
                    quantile=quantile,
                )
            )

        self._active_longs = set(target_longs)
        self._active_shorts = set(target_shorts)
        self._last_rebalance_at = now

        logger.info(
            f"{self.name} rebalance at {now.isoformat()} | universe={universe_size} "
            f"long={len(target_longs)} short={len(target_shorts)} signals={len(signals)}"
        )
        return signals

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        # Runtime uses generate_signals_async.
        return []

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "cross_sectional_factor",
            "min_length": 120,
            "multi_symbol": True,
        }
