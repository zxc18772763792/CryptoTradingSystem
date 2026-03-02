"""Strategy API endpoints."""
import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
import numpy as np
import pandas as pd
from pydantic import BaseModel, Field

import strategies as strategy_module
from config.settings import settings
from core.audit import audit_logger
from core.data import data_storage
from core.exchanges import exchange_manager
from core.risk.risk_manager import risk_manager
from core.strategies import Signal, SignalType, strategy_manager
from core.strategies.persistence import (
    persist_strategy_snapshot,
    delete_strategy_snapshot,
)
from core.strategies.health_monitor import strategy_health_monitor
from core.trading.execution_engine import execution_engine
from core.trading.position_manager import PositionSide, position_manager
from strategies import ALL_STRATEGIES
from web.api.backtest import (
    _run_backtest_core,
    get_backtest_strategy_info,
    is_strategy_backtest_supported,
)

router = APIRouter()

DEFAULT_START_ALL_STRATEGIES: List[str] = [
    "MAStrategy",
    "EMAStrategy",
    "RSIStrategy",
    "MACDStrategy",
    "BollingerBandsStrategy",
    "MeanReversionStrategy",
    "MomentumStrategy",
    "DonchianBreakoutStrategy",
    "StochasticStrategy",
    "ADXTrendStrategy",
    "VWAPReversionStrategy",
    "MarketSentimentStrategy",
    "SocialSentimentStrategy",
    "FundFlowStrategy",
    "WhaleActivityStrategy",
]

_STRATEGY_LIBRARY_META: Dict[str, Dict[str, Any]] = {
    "MAStrategy": {"category": "趋势", "risk": "low", "usage": "双均线顺势"},
    "EMAStrategy": {"category": "趋势", "risk": "low", "usage": "快慢EMA趋势"},
    "RSIStrategy": {"category": "震荡", "risk": "medium", "usage": "超买超卖反转"},
    "RSIDivergenceStrategy": {"category": "反转", "risk": "medium", "usage": "背离反转"},
    "MACDStrategy": {"category": "趋势", "risk": "medium", "usage": "MACD趋势跟随"},
    "MACDHistogramStrategy": {"category": "趋势", "risk": "medium", "usage": "柱体拐点"},
    "BollingerBandsStrategy": {"category": "震荡", "risk": "medium", "usage": "布林带回归"},
    "BollingerSqueezeStrategy": {"category": "突破", "risk": "medium", "usage": "波动收敛后突破"},
    "DonchianBreakoutStrategy": {"category": "突破", "risk": "medium", "usage": "通道突破"},
    "StochasticStrategy": {"category": "震荡", "risk": "medium", "usage": "随机指标"},
    "ADXTrendStrategy": {"category": "趋势", "risk": "medium", "usage": "ADX强趋势"},
    "VWAPReversionStrategy": {"category": "均值回归", "risk": "medium", "usage": "VWAP偏离回归"},
    "MeanReversionStrategy": {"category": "均值回归", "risk": "medium", "usage": "Z-Score回归"},
    "BollingerMeanReversionStrategy": {"category": "均值回归", "risk": "medium", "usage": "布林均值回归"},
    "MomentumStrategy": {"category": "动量", "risk": "medium", "usage": "动量突破"},
    "TrendFollowingStrategy": {"category": "趋势", "risk": "medium", "usage": "趋势跟随"},
    "PairsTradingStrategy": {"category": "统计套利", "risk": "high", "usage": "价差回归（双腿）"},
    "FamaFactorArbitrageStrategy": {"category": "因子套利", "risk": "high", "usage": "多因子横截面多空"},
    "CEXArbitrageStrategy": {"category": "套利", "risk": "high", "usage": "跨交易所套利"},
    "TriangularArbitrageStrategy": {"category": "套利", "risk": "high", "usage": "三角路径套利"},
    "DEXArbitrageStrategy": {"category": "套利", "risk": "high", "usage": "链上价差套利"},
    "FlashLoanArbitrageStrategy": {"category": "套利", "risk": "high", "usage": "闪电贷套利"},
    "MarketSentimentStrategy": {"category": "宏观", "risk": "medium", "usage": "市场情绪因子"},
    "SocialSentimentStrategy": {"category": "宏观", "risk": "medium", "usage": "社媒情绪因子"},
    "FundFlowStrategy": {"category": "宏观", "risk": "medium", "usage": "资金流因子"},
    "WhaleActivityStrategy": {"category": "宏观", "risk": "high", "usage": "巨鲸行为跟踪"},
}

_CRYPTO_COMMON_DEFAULTS: Dict[str, Dict[str, Any]] = {
    "MAStrategy": {"fast_period": 20, "slow_period": 60, "signal_threshold": 0.0015, "stop_loss_pct": 0.03, "take_profit_pct": 0.08},
    "EMAStrategy": {"fast_period": 12, "slow_period": 26, "signal_threshold": 0.0012, "stop_loss_pct": 0.025, "take_profit_pct": 0.06},
    "RSIStrategy": {"period": 14, "oversold": 30, "overbought": 70, "exit_oversold": 42, "exit_overbought": 58, "stop_loss_pct": 0.025, "take_profit_pct": 0.055},
    "RSIDivergenceStrategy": {"period": 14, "lookback": 34, "min_divergence": 0.015, "extrema_order": 5, "stop_loss_pct": 0.03, "take_profit_pct": 0.08},
    "MACDStrategy": {"fast_period": 12, "slow_period": 26, "signal_period": 9, "stop_loss_pct": 0.025, "take_profit_pct": 0.06},
    "MACDHistogramStrategy": {"fast_period": 12, "slow_period": 26, "signal_period": 9, "min_histogram": 0.0002, "stop_loss_pct": 0.025, "take_profit_pct": 0.06},
    "BollingerBandsStrategy": {"period": 20, "num_std": 2.0, "stop_loss_pct": 0.025, "take_profit_pct": 0.05},
    "BollingerSqueezeStrategy": {"period": 20, "num_std": 2.0, "squeeze_threshold": 0.018, "breakout_threshold": 0.008, "stop_loss_pct": 0.03, "take_profit_pct": 0.08},
    "DonchianBreakoutStrategy": {"lookback": 20, "exit_lookback": 10, "breakout_buffer_pct": 0.001, "stop_loss_pct": 0.025, "take_profit_pct": 0.08},
    "StochasticStrategy": {"k_period": 14, "d_period": 3, "smooth_k": 3, "oversold": 20.0, "overbought": 80.0, "stop_loss_pct": 0.02, "take_profit_pct": 0.05},
    "ADXTrendStrategy": {"period": 14, "adx_threshold": 23.0, "stop_loss_pct": 0.025, "take_profit_pct": 0.07},
    "VWAPReversionStrategy": {"window": 48, "entry_deviation_pct": 0.012, "exit_deviation_pct": 0.003, "stop_loss_pct": 0.02, "take_profit_pct": 0.035},
    "MeanReversionStrategy": {"lookback_period": 24, "entry_z_score": 2.1, "exit_z_score": 0.6, "stop_loss_pct": 0.03, "take_profit_pct": 0.06},
    "BollingerMeanReversionStrategy": {"period": 20, "num_std": 2.2, "stop_loss_pct": 0.02, "take_profit_pct": 0.04},
    "MomentumStrategy": {"lookback_period": 20, "momentum_threshold": 0.015, "stop_loss_pct": 0.03, "take_profit_pct": 0.07},
    "TrendFollowingStrategy": {"short_period": 20, "long_period": 55, "adx_threshold": 23, "stop_loss_pct": 0.03, "take_profit_pct": 0.09},
    "PairsTradingStrategy": {"lookback_period": 48, "entry_z_score": 2.0, "exit_z_score": 0.6, "hedge_ratio_method": "ols", "min_hedge_ratio": 0.1, "max_hedge_ratio": 5.0, "stop_loss_pct": 0.04, "pair_symbol": "ETH/USDT"},
    "FamaFactorArbitrageStrategy": {
        "exchange": "binance",
        "factor_timeframe": "1h",
        "universe_symbols": [
            "BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT",
            "DOGE/USDT", "ADA/USDT", "AVAX/USDT", "LINK/USDT", "DOT/USDT",
            "MATIC/USDT", "LTC/USDT",
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
    },
    "CEXArbitrageStrategy": {
        "min_spread": 0.002,
        "alpha_threshold": 0.002,
        "min_volume": 50000,
        "exchanges": ["binance", "okx", "gate"],
        "max_position_size": 2000,
        "consider_fees": True,
        "fee_rate": 0.0008,
        "max_opportunities": 2,
        "cooldown_min": 1,
        "max_vol": 0.03,
        "max_spread": 0.03,
    },
    "TriangularArbitrageStrategy": {
        "base_currency": "USDT",
        "min_profit": 0.002,
        "alpha_threshold": 0.002,
        "consider_fees": True,
        "fee_rate": 0.0007,
        "bridge_assets": ["ETH", "BNB", "SOL"],
        "max_opportunities": 2,
        "cooldown_min": 1,
        "max_spread": 0.03,
    },
    "DEXArbitrageStrategy": {"min_spread": 0.008, "min_profit_usd": 30, "max_gas_cost": 20, "dex_list": ["uniswap", "sushiswap"], "chain": "ethereum"},
    "FlashLoanArbitrageStrategy": {"min_profit": 0.004, "loan_amount": 100000, "dex_list": ["uniswap", "sushiswap"]},
    "MarketSentimentStrategy": {"fear_threshold": 25, "greed_threshold": 75, "lookback_period": 7, "stop_loss_pct": 0.04, "take_profit_pct": 0.09, "timeout_sec": 6},
    "SocialSentimentStrategy": {"positive_threshold": 0.2, "negative_threshold": -0.2, "min_mentions": 30, "stop_loss_pct": 0.04, "take_profit_pct": 0.09, "timeout_sec": 6},
    "FundFlowStrategy": {"inflow_threshold": 150000.0, "outflow_threshold": -150000.0, "min_imbalance_ratio": 0.03, "lookback_period": 7, "book_depth": 80, "stop_loss_pct": 0.04, "take_profit_pct": 0.09},
    "WhaleActivityStrategy": {"min_whale_size": 100000.0, "accumulation_threshold": 2, "distribution_threshold": 2, "lookback_hours": 24, "trade_limit": 600, "stop_loss_pct": 0.04, "take_profit_pct": 0.09},
}

_RECOMMENDED_TIMEFRAMES: Dict[str, str] = {
    "MAStrategy": "15m",
    "EMAStrategy": "15m",
    "RSIStrategy": "15m",
    "RSIDivergenceStrategy": "15m",
    "MACDStrategy": "15m",
    "MACDHistogramStrategy": "15m",
    "BollingerBandsStrategy": "15m",
    "BollingerSqueezeStrategy": "15m",
    "DonchianBreakoutStrategy": "1h",
    "StochasticStrategy": "15m",
    "ADXTrendStrategy": "1h",
    "VWAPReversionStrategy": "15m",
    "MeanReversionStrategy": "1h",
    "BollingerMeanReversionStrategy": "1h",
    "MomentumStrategy": "1h",
    "TrendFollowingStrategy": "1h",
    "PairsTradingStrategy": "1h",
    "FamaFactorArbitrageStrategy": "1h",
    "CEXArbitrageStrategy": "5m",
    "TriangularArbitrageStrategy": "5m",
    "DEXArbitrageStrategy": "5m",
    "FlashLoanArbitrageStrategy": "5m",
    "MarketSentimentStrategy": "15m",
    "SocialSentimentStrategy": "15m",
    "FundFlowStrategy": "15m",
    "WhaleActivityStrategy": "15m",
}


def _recommended_symbols(strategy_type: str) -> List[str]:
    if strategy_type == "PairsTradingStrategy":
        return ["BTC/USDT", "ETH/USDT"]
    if strategy_type == "FamaFactorArbitrageStrategy":
        return ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "XRP/USDT", "DOGE/USDT"]
    return ["BTC/USDT"]


def _recommended_timeframe(strategy_type: str) -> str:
    return str(_RECOMMENDED_TIMEFRAMES.get(strategy_type, "1h"))


def _recommended_crypto_defaults(strategy_type: str, exchange: str) -> Dict[str, Any]:
    out = dict(_CRYPTO_COMMON_DEFAULTS.get(strategy_type, {}))
    if strategy_type in {
        "MarketSentimentStrategy",
        "FundFlowStrategy",
        "WhaleActivityStrategy",
        "TriangularArbitrageStrategy",
    }:
        out["exchange"] = str(exchange or out.get("exchange") or "binance").lower()
    return out


def _safe_float(value: Any, fallback: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(fallback)
    if np.isnan(out) or np.isinf(out):
        return float(fallback)
    return float(out)


def _normalize_strategy_specific_params(strategy_type: str, params: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(params or {})
    st = str(strategy_type or "").strip()

    if st == "FamaFactorArbitrageStrategy":
        if "alpha_threshold" in out and "min_abs_score" not in out:
            out["min_abs_score"] = max(0.0, _safe_float(out.get("alpha_threshold"), 0.15))
        if "min_abs_score" in out and "alpha_threshold" not in out:
            out["alpha_threshold"] = max(0.0, _safe_float(out.get("min_abs_score"), 0.15))

        if "cooldown_min" in out and "rebalance_interval_minutes" not in out:
            out["rebalance_interval_minutes"] = max(1, int(_safe_float(out.get("cooldown_min"), 60)))
        if "rebalance_interval_minutes" in out and "cooldown_min" not in out:
            out["cooldown_min"] = max(1, int(_safe_float(out.get("rebalance_interval_minutes"), 60)))

    if st == "CEXArbitrageStrategy":
        if "alpha_threshold" in out and "min_spread" not in out:
            out["min_spread"] = max(0.0, _safe_float(out.get("alpha_threshold"), 0.002))
        if "min_spread" in out and "alpha_threshold" not in out:
            out["alpha_threshold"] = max(0.0, _safe_float(out.get("min_spread"), 0.002))

    if st == "TriangularArbitrageStrategy":
        if "alpha_threshold" in out and "min_profit" not in out:
            out["min_profit"] = max(0.0, _safe_float(out.get("alpha_threshold"), 0.002))
        if "min_profit" in out and "alpha_threshold" not in out:
            out["alpha_threshold"] = max(0.0, _safe_float(out.get("min_profit"), 0.002))

    if "cooldown_min" in out:
        out["cooldown_min"] = max(0, int(_safe_float(out.get("cooldown_min"), 0)))

    if "max_vol" in out:
        out["max_vol"] = max(0.0, _safe_float(out.get("max_vol"), 0.0))

    if "max_spread" in out:
        out["max_spread"] = max(0.0, _safe_float(out.get("max_spread"), 0.0))

    return out


def _build_strategy_register_params(
    strategy_type: str,
    exchange: str,
    user_params: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    base = _recommended_crypto_defaults(strategy_type=strategy_type, exchange=exchange)
    normalized_user = _normalize_strategy_specific_params(
        strategy_type=strategy_type,
        params=dict(user_params or {}),
    )
    merged = dict(base)
    merged.update(normalized_user)
    normalized = _normalize_strategy_specific_params(strategy_type=strategy_type, params=merged)
    return _apply_trade_policy_defaults(normalized, exchange)


class StrategyRegisterRequest(BaseModel):
    name: str
    strategy_type: str
    params: Optional[Dict[str, Any]] = None
    symbols: Optional[List[str]] = None
    timeframe: str = "1h"
    exchange: str = "gate"
    allocation: float = Field(default=1.0, ge=0.0, le=1.0)
    runtime_limit_minutes: Optional[int] = Field(default=None, ge=0, le=10080)


class StrategyUpdateRequest(BaseModel):
    params: Dict[str, Any]


class StrategyConfigUpdateRequest(BaseModel):
    timeframe: Optional[str] = None
    symbols: Optional[List[str]] = None
    runtime_limit_minutes: Optional[int] = Field(default=None, ge=0, le=10080)


class StrategyAllocationRequest(BaseModel):
    allocation: float = Field(..., ge=0.0, le=1.0)


class AllocationRebalanceRequest(BaseModel):
    allocations: Dict[str, float]


class StrategyImportItem(BaseModel):
    name: str
    strategy_type: str
    params: Dict[str, Any] = Field(default_factory=dict)
    symbols: List[str] = Field(default_factory=lambda: ["BTC/USDT"])
    timeframe: str = "1h"
    exchange: str = "gate"
    allocation: float = Field(default=0.2, ge=0.0, le=1.0)
    state: str = "idle"


class StrategyImportRequest(BaseModel):
    strategies: List[StrategyImportItem]
    rename_prefix: Optional[str] = None
    auto_start: bool = False
    overwrite: bool = False


def _normalize_symbols_input(symbols: Optional[List[str]]) -> Optional[List[str]]:
    if symbols is None:
        return None
    normalized = []
    for item in symbols:
        text = str(item or "").strip()
        if not text:
            continue
        normalized.append(text.upper())
    if not normalized:
        return ["BTC/USDT"]
    deduped: List[str] = []
    seen = set()
    for item in normalized:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _ceil_to_decimals(value: float, decimals: int = 8) -> float:
    if decimals < 0:
        decimals = 0
    factor = 10 ** decimals
    return float(np.ceil(float(value) * factor) / factor)


async def _build_strategy_sizing_preview(name: str) -> Dict[str, Any]:
    info = strategy_manager.get_strategy_info(name)
    if not info:
        raise HTTPException(status_code=404, detail="Strategy not found")

    exchange = str(info.get("exchange") or "binance").strip().lower() or "binance"
    symbols = list(info.get("symbols") or ["BTC/USDT"])
    symbol = str(symbols[0] if symbols else "BTC/USDT")
    params = dict(info.get("params") or {})
    allocation = max(0.0, min(float(info.get("allocation") or 0.0), 1.0))
    market_type = str(params.get("market_type") or "").strip().lower()

    account_equity = 0.0
    try:
        account_equity = float(await asyncio.wait_for(execution_engine._get_account_equity(), timeout=8.0))
    except Exception:
        account_equity = float((risk_manager.get_risk_report().get("equity") or {}).get("current") or 0.0)

    connector = exchange_manager.get_exchange(exchange)
    last_price = 0.0
    price_source = "unavailable"
    if not connector:
        try:
            await exchange_manager.initialize([exchange])
            connector = exchange_manager.get_exchange(exchange)
        except Exception:
            connector = None
    if connector:
        try:
            ticker = await asyncio.wait_for(connector.get_ticker(symbol), timeout=5.0)
            last_price = float(getattr(ticker, "last", 0.0) or 0.0)
            if last_price > 0:
                price_source = "live"
        except Exception:
            last_price = 0.0
    if last_price <= 0:
        timeframe_candidates = [
            str(info.get("timeframe") or "").strip() or "1m",
            "1m",
            "5m",
            "15m",
            "1h",
        ]
        seen_tf: set[str] = set()
        for timeframe in timeframe_candidates:
            tf = str(timeframe or "").strip() or "1m"
            if tf in seen_tf:
                continue
            seen_tf.add(tf)
            try:
                df = await data_storage.load_klines_from_parquet(
                    exchange=exchange,
                    symbol=symbol,
                    timeframe=tf,
                )
                if df is not None and not df.empty:
                    px = _safe_float(df["close"].iloc[-1], 0.0)
                    if px > 0:
                        last_price = px
                        price_source = f"cache:{tf}"
                        break
            except Exception:
                continue

    min_amount, amount_decimals = await execution_engine._get_exchange_amount_rules(exchange, symbol)
    configured_min_notional = max(1.0, float(getattr(settings, "MIN_STRATEGY_ORDER_USD", 100.0) or 100.0))
    is_binance_futures = exchange == "binance" and market_type in {
        "future", "futures", "swap", "contract", "perp", "perpetual"
    }
    exchange_min_notional = 100.0 if is_binance_futures else 10.0
    effective_min_notional = max(exchange_min_notional, configured_min_notional)

    single_cap = max(0.0, account_equity * float(risk_manager.max_position_size or 0.1))
    alloc_cap = max(0.0, account_equity * allocation) if allocation > 0 else single_cap
    available_notional = min(single_cap, alloc_cap if allocation > 0 else single_cap)

    min_legal_qty = 0.0
    min_legal_notional = 0.0
    if last_price > 0:
        qty_by_notional = _ceil_to_decimals(effective_min_notional / last_price, amount_decimals)
        min_legal_qty = max(float(min_amount or 0.0), float(qty_by_notional or 0.0))
        min_legal_notional = float(min_legal_qty * last_price)

    has_price = last_price > 0
    can_estimate = bool(has_price and effective_min_notional > 0)
    executable_now = bool(
        can_estimate
        and available_notional > 0
        and min_legal_notional > 0
        and available_notional + max(0.05, available_notional * 0.01) >= min_legal_notional
    )
    preview_status = "ok" if executable_now else ("blocked" if can_estimate else "unknown")
    note = (
        "当前资金足够满足交易所最小下单门槛"
        if executable_now
        else (
            f"当前资金占比或单笔风控上限不足，最少需要 {min_legal_notional:.2f} USDT 名义金额"
            if can_estimate
            else "暂时无法获取实时价格或交易规则，当前预估结果不可用于判断是否可下单"
        )
    )

    return {
        "strategy": name,
        "exchange": exchange,
        "symbol": symbol,
        "market_type": market_type or None,
        "allocation": allocation,
        "account_equity": round(account_equity, 6),
        "risk_single_cap": round(single_cap, 6),
        "allocation_cap": round(alloc_cap, 6),
        "available_notional": round(available_notional, 6),
        "price": round(last_price, 8) if last_price > 0 else 0.0,
        "price_source": price_source,
        "exchange_min_notional": round(exchange_min_notional, 6),
        "configured_min_notional": round(configured_min_notional, 6),
        "effective_min_notional": round(effective_min_notional, 6),
        "min_amount": round(float(min_amount or 0.0), 12),
        "amount_decimals": int(amount_decimals),
        "min_legal_qty": round(min_legal_qty, 12),
        "min_legal_notional": round(min_legal_notional, 6),
        "executable_now": executable_now,
        "can_estimate": can_estimate,
        "status": preview_status,
        "note": note,
    }


async def _close_strategy_positions(name: str) -> Dict[str, Any]:
    positions = list(position_manager.get_positions_by_strategy(name) or [])
    if not positions:
        return {"requested": 0, "closed": 0, "failed": 0, "results": []}

    results: List[Dict[str, Any]] = []
    closed = 0
    failed = 0
    for pos in positions:
        close_signal = Signal(
            symbol=str(pos.symbol),
            signal_type=(SignalType.CLOSE_LONG if pos.side == PositionSide.LONG else SignalType.CLOSE_SHORT),
            price=float(pos.current_price or pos.entry_price or 0.0),
            timestamp=datetime.utcnow(),
            strategy_name=name,
            strength=1.0,
            quantity=float(pos.quantity or 0.0),
            metadata={
                "exchange": str(pos.exchange or "binance"),
                "account_id": str(pos.account_id or "main"),
                "source": "strategy_stop_close",
                "close_reason": "strategy_stopped",
            },
        )
        try:
            res = await execution_engine.execute_signal(close_signal)
            if res:
                closed += 1
                results.append(
                    {
                        "symbol": pos.symbol,
                        "exchange": pos.exchange,
                        "account_id": pos.account_id,
                        "status": "closed",
                        "result": res,
                    }
                )
            else:
                failed += 1
                results.append(
                    {
                        "symbol": pos.symbol,
                        "exchange": pos.exchange,
                        "account_id": pos.account_id,
                        "status": "failed",
                        "reason": "close_signal_rejected",
                    }
                )
        except Exception as exc:
            failed += 1
            results.append(
                {
                    "symbol": pos.symbol,
                    "exchange": pos.exchange,
                    "account_id": pos.account_id,
                    "status": "failed",
                    "reason": str(exc),
                }
            )
    return {"requested": len(positions), "closed": closed, "failed": failed, "results": results}


def _get_strategy_classes() -> Dict[str, Any]:
    classes: Dict[str, Any] = {}
    for class_name in ALL_STRATEGIES:
        klass = getattr(strategy_module, class_name, None)
        if klass is not None:
            classes[class_name] = klass
    return classes


def _audit_dataframe(symbol: str = "BTC/USDT", rows: int = 320) -> pd.DataFrame:
    index = pd.date_range(end=datetime.utcnow(), periods=max(120, int(rows)), freq="H")
    rng = np.random.default_rng(seed=42)
    close = pd.Series(50000 + np.cumsum(rng.normal(0, 80, len(index))), index=index).abs() + 1000
    open_ = close.shift(1).fillna(close.iloc[0])
    high = np.maximum(open_, close) + np.abs(rng.normal(0, 35, len(index)))
    low = np.minimum(open_, close) - np.abs(rng.normal(0, 35, len(index)))
    volume = np.abs(rng.normal(2000, 500, len(index))) + 200
    return pd.DataFrame(
        {
            "open": open_.values,
            "high": high,
            "low": low,
            "close": close.values,
            "volume": volume,
            "symbol": [symbol] * len(index),
        },
        index=index,
    )


def _audit_pair_dataframe(symbol: str = "ETH/USDT", rows: int = 320) -> pd.DataFrame:
    index = pd.date_range(end=datetime.utcnow(), periods=max(120, int(rows)), freq="H")
    rng = np.random.default_rng(seed=99)
    close = pd.Series(3000 + np.cumsum(rng.normal(0, 10, len(index))), index=index).abs() + 50
    open_ = close.shift(1).fillna(close.iloc[0])
    high = np.maximum(open_, close) + np.abs(rng.normal(0, 4, len(index)))
    low = np.minimum(open_, close) - np.abs(rng.normal(0, 4, len(index)))
    volume = np.abs(rng.normal(6000, 1200, len(index))) + 500
    return pd.DataFrame(
        {
            "open": open_.values,
            "high": high,
            "low": low,
            "close": close.values,
            "volume": volume,
            "symbol": [symbol] * len(index),
        },
        index=index,
    )


async def _persist_if_exists(name: str, state_override: Optional[str] = None) -> None:
    if not name:
        return
    try:
        await persist_strategy_snapshot(name, state_override=state_override)
    except Exception:
        pass


def _select_default_start_all_strategies(available: Dict[str, Any]) -> List[str]:
    return [name for name in DEFAULT_START_ALL_STRATEGIES if name in available]


def _default_market_type_for_exchange(exchange: str) -> str:
    ex = str(exchange or "").strip().lower()
    mapping = {
        "binance": str(getattr(settings, "BINANCE_DEFAULT_TYPE", "spot") or "spot"),
        "okx": str(getattr(settings, "OKX_DEFAULT_TYPE", "spot") or "spot"),
        "gate": str(getattr(settings, "GATE_DEFAULT_TYPE", "spot") or "spot"),
        "bybit": str(getattr(settings, "BYBIT_DEFAULT_TYPE", "spot") or "spot"),
    }
    market_type = str(mapping.get(ex, "spot") or "spot").strip().lower()
    aliases = {
        "futures": "future",
        "perp": "swap",
        "perpetual": "swap",
    }
    market_type = aliases.get(market_type, market_type)
    if market_type not in {"spot", "future", "swap", "margin"}:
        market_type = "spot"
    return market_type


def _apply_trade_policy_defaults(params: Dict[str, Any], exchange: str) -> Dict[str, Any]:
    out = dict(params or {})
    out["exchange"] = str(exchange or out.get("exchange") or "binance").lower()
    market_type = str(out.get("market_type") or "").strip().lower()
    if not market_type:
        market_type = _default_market_type_for_exchange(out["exchange"])
    aliases = {
        "futures": "future",
        "perp": "swap",
        "perpetual": "swap",
    }
    market_type = aliases.get(market_type, market_type)
    if market_type not in {"spot", "future", "swap", "margin"}:
        market_type = "spot"
    out["market_type"] = market_type

    is_derivatives = market_type in {"future", "swap"}
    out.setdefault("allow_long", True)
    out.setdefault("allow_short", bool(is_derivatives))
    out.setdefault("reverse_on_signal", True)
    out.setdefault("allow_pyramiding", False)
    return out


async def _auto_register_defaults_for_start_all() -> List[str]:
    """Auto-register missing defaults when start-all is requested."""
    existing = strategy_manager.list_strategies()
    existing_types = {str(item.get("strategy_type", "")) for item in existing}

    strategy_classes = _get_strategy_classes()
    selected = _select_default_start_all_strategies(strategy_classes)
    if not selected:
        return []

    created: List[str] = []
    allocation = round(1.0 / max(1, len(selected)), 4)
    suffix = datetime.now().strftime("%m%d%H%M")

    for strategy_type in selected:
        if strategy_type in existing_types:
            continue
        strategy_class = strategy_classes.get(strategy_type)
        if strategy_class is None:
            continue

        base_name = f"{strategy_type}_{suffix}"
        name = base_name
        i = 1
        while strategy_manager.get_strategy(name) is not None:
            i += 1
            name = f"{base_name}_{i}"

        ok = strategy_manager.register_strategy(
            name=name,
            strategy_class=strategy_class,
            params=_build_strategy_register_params(strategy_type, "binance", {}),
            symbols=_recommended_symbols(strategy_type),
            timeframe=_recommended_timeframe(strategy_type),
            allocation=allocation,
        )
        if not ok:
            continue

        await _persist_if_exists(name, state_override="idle")
        created.append(name)

    return created


@router.get("/list")
async def list_strategies():
    available_map = _get_strategy_classes()
    return {
        "strategies": list(available_map.keys()),
        "registered": strategy_manager.list_strategies(),
    }


@router.get("/library")
async def get_strategy_library():
    classes = _get_strategy_classes()
    registered = strategy_manager.list_strategies()
    reg_by_type: Dict[str, Dict[str, int]] = {}
    for item in registered:
        stype = str(item.get("strategy_type") or "")
        if not stype:
            continue
        row = reg_by_type.setdefault(stype, {"registered": 0, "running": 0})
        row["registered"] += 1
        if str(item.get("state") or "").lower() == "running":
            row["running"] += 1

    rows = []
    for name in sorted(classes.keys()):
        klass = classes[name]
        meta = dict(_STRATEGY_LIBRARY_META.get(name, {}))
        required_data: Dict[str, Any] = {}
        param_schema: List[Dict[str, Any]] = []
        sample_params: Dict[str, Any] = {}
        init_error: Optional[str] = None
        try:
            inst = klass(name=f"lib_{name}", params={})
            sample_params = dict(getattr(inst, "params", {}) or {})
            required_data = dict(inst.get_required_data() or {})
            param_schema = strategy_manager._infer_param_schema_from_params(sample_params)  # type: ignore[attr-defined]
        except Exception as e:
            init_error = str(e)

        bt_supported = is_strategy_backtest_supported(name)
        bt_info = get_backtest_strategy_info(name)
        counts = reg_by_type.get(name, {"registered": 0, "running": 0})
        rows.append(
            {
                "name": name,
                "category": meta.get("category", "其他"),
                "risk": meta.get("risk", "medium"),
                "usage": meta.get("usage", ""),
                "required_data": required_data,
                "param_schema": param_schema,
                "sample_params": sample_params,
                "backtest_supported": bt_supported,
                "backtest_reason": bt_info.get("reason"),
                "registered_count": counts["registered"],
                "running_count": counts["running"],
                "init_error": init_error,
            }
        )

    return {
        "total": len(rows),
        "registered_total": len(registered),
        "running_total": len([x for x in registered if str(x.get("state", "")).lower() == "running"]),
        "library": rows,
        "generated_at": datetime.utcnow().isoformat(),
    }


@router.get("/audit")
async def audit_strategy_library(
    symbol: str = "BTC/USDT",
    run_async_checks: bool = False,
    max_async_checks: int = 12,
):
    classes = _get_strategy_classes()
    base_df = _audit_dataframe(symbol=symbol, rows=320)
    pair_df = _audit_pair_dataframe(symbol="ETH/USDT", rows=320)

    details: List[Dict[str, Any]] = []
    async_used = 0

    for strategy_name in sorted(classes.keys()):
        klass = classes.get(strategy_name)
        item: Dict[str, Any] = {
            "strategy": strategy_name,
            "available": True,
            "init_ok": False,
            "sync_ok": False,
            "async_ok": None,
            "sync_signals": 0,
            "async_signals": None,
            "required_data": {},
            "issues": [],
        }

        try:
            strategy = klass(name=f"audit_{strategy_name}", params={})
            item["init_ok"] = True
        except Exception as e:
            item["issues"].append(f"init_failed: {e}")
            details.append(item)
            continue

        required = {}
        try:
            required = strategy.get_required_data() or {}
        except Exception as e:
            item["issues"].append(f"required_data_failed: {e}")
        item["required_data"] = required

        try:
            if bool(required.get("requires_pair", False)):
                signals = strategy.generate_signals(base_df, pair_df)
            else:
                signals = strategy.generate_signals(base_df)
            item["sync_ok"] = True
            item["sync_signals"] = int(len(signals or []))
        except Exception as e:
            item["issues"].append(f"sync_failed: {e}")

        if run_async_checks and hasattr(strategy, "generate_signals_async") and async_used < max(1, int(max_async_checks)):
            async_used += 1
            try:
                async_method = getattr(strategy, "generate_signals_async")
                try:
                    async_result = await asyncio.wait_for(async_method(symbol), timeout=8.0)
                except TypeError:
                    parts = str(symbol or "BTC/USDT").upper().split("/")
                    base = parts[0] if parts else "BTC"
                    quote = parts[1] if len(parts) > 1 else "USDT"
                    async_result = await asyncio.wait_for(async_method(base, quote, 1.0), timeout=8.0)
                item["async_ok"] = True
                item["async_signals"] = int(len(async_result or []))
            except Exception as e:
                item["async_ok"] = False
                item["issues"].append(f"async_failed: {e}")

        details.append(item)

    optional_missing = []
    for optional_name in ["DEXArbitrageStrategy", "FlashLoanArbitrageStrategy"]:
        if getattr(strategy_module, optional_name, None) is None:
            optional_missing.append(
                {
                    "strategy": optional_name,
                    "available": False,
                    "reason": "optional dependency missing (e.g. web3)",
                }
            )

    passed = [x for x in details if x.get("init_ok") and x.get("sync_ok")]
    failed = [x for x in details if not (x.get("init_ok") and x.get("sync_ok"))]

    return {
        "timestamp": datetime.utcnow().isoformat(),
        "symbol": symbol,
        "run_async_checks": bool(run_async_checks),
        "summary": {
            "total_available": len(details),
            "sync_passed": len(passed),
            "sync_failed": len(failed),
            "optional_missing": len(optional_missing),
        },
        "optional_missing": optional_missing,
        "details": details,
    }


@router.get("/summary")
async def get_strategy_summary(limit: int = 20):
    return strategy_manager.get_dashboard_summary(signal_limit=limit)


@router.get("/export/{name}")
async def export_strategy(name: str):
    info = strategy_manager.get_strategy_info(name)
    if not info:
        raise HTTPException(status_code=404, detail="Strategy not found")
    return {
        "strategy": {
            "name": info.get("name"),
            "strategy_type": info.get("strategy_type"),
            "params": info.get("params", {}),
            "symbols": info.get("symbols", []),
            "timeframe": info.get("timeframe", "1h"),
            "exchange": info.get("exchange", "gate"),
            "allocation": info.get("allocation", 1.0),
            "state": info.get("state", "idle"),
        },
        "exported_at": info.get("last_run_at"),
    }


@router.get("/export")
async def export_all_strategies():
    items = []
    for info in strategy_manager.list_strategies():
        items.append(
            {
                "name": info.get("name"),
                "strategy_type": info.get("strategy_type"),
                "params": info.get("params", {}),
                "symbols": info.get("symbols", []),
                "timeframe": info.get("timeframe", "1h"),
                "exchange": info.get("exchange", "gate"),
                "allocation": info.get("allocation", 1.0),
                "state": info.get("state", "idle"),
            }
        )
    return {"strategies": items, "count": len(items)}


@router.post("/import")
async def import_strategies(payload: StrategyImportRequest):
    strategy_classes = _get_strategy_classes()
    imported = []
    skipped = []

    for item in payload.strategies:
        strategy_class = strategy_classes.get(item.strategy_type)
        if not strategy_class:
            skipped.append({"name": item.name, "reason": "unknown_strategy_type"})
            continue

        name = item.name
        if payload.rename_prefix:
            name = f"{payload.rename_prefix}{name}"

        existing = strategy_manager.get_strategy(name)
        if existing and not payload.overwrite:
            skipped.append({"name": name, "reason": "already_exists"})
            continue
        if existing and payload.overwrite:
            strategy_manager.unregister_strategy(name)

        ok = strategy_manager.register_strategy(
            name=name,
            strategy_class=strategy_class,
            params=_apply_trade_policy_defaults(
                _normalize_strategy_specific_params(
                    strategy_type=item.strategy_type,
                    params=dict(item.params or {}),
                ),
                item.exchange,
            ),
            symbols=item.symbols,
            timeframe=item.timeframe,
            allocation=item.allocation,
        )
        if not ok:
            skipped.append({"name": name, "reason": "register_failed"})
            continue

        if payload.auto_start or str(item.state).lower() == "running":
            await strategy_manager.start_strategy(name)
            await _persist_if_exists(name, state_override="running")
        else:
            await _persist_if_exists(name, state_override="idle")

        imported.append({"name": name, "strategy_type": item.strategy_type})

    return {"success": True, "imported": imported, "skipped": skipped}


@router.get("/ranking")
async def get_strategy_ranking(
    symbol: str = "BTC/USDT",
    timeframe: str = "1h",
    initial_capital: float = 10000,
    top_n: int = 20,
):
    classes = _get_strategy_classes()
    if not classes:
        raise HTTPException(status_code=404, detail="No strategies available")

    df = await data_storage.load_klines_from_parquet(exchange="binance", symbol=symbol, timeframe=timeframe)
    if df.empty:
        for ex in ["gate", "okx", "binance"]:
            df = await data_storage.load_klines_from_parquet(exchange=ex, symbol=symbol, timeframe=timeframe)
            if not df.empty:
                break
    if df.empty:
        raise HTTPException(status_code=404, detail="缺少历史数据")

    rows: List[Dict[str, Any]] = []
    unsupported: List[Dict[str, Any]] = []
    for strategy_name in classes.keys():
        if not is_strategy_backtest_supported(strategy_name):
            info = get_backtest_strategy_info(strategy_name)
            unsupported.append(
                {
                    "strategy": strategy_name,
                    "backtest_supported": False,
                    "reason": info.get("reason", "当前策略不适用K线回测"),
                }
            )
            continue

        try:
            metrics = _run_backtest_core(
                strategy=strategy_name,
                df=df,
                timeframe=timeframe,
                initial_capital=initial_capital,
                include_series=False,
            )
            score = (
                float(metrics.get("total_return", 0.0)) * 0.5
                + float(metrics.get("sharpe_ratio", 0.0)) * 20.0
                - float(metrics.get("max_drawdown", 0.0)) * 0.4
                + float(metrics.get("win_rate", 0.0)) * 0.1
            )
            rows.append(
                {
                    "strategy": strategy_name,
                    "backtest_supported": True,
                    "score": round(score, 4),
                    "total_return": metrics.get("total_return", 0.0),
                    "sharpe_ratio": metrics.get("sharpe_ratio", 0.0),
                    "max_drawdown": metrics.get("max_drawdown", 0.0),
                    "win_rate": metrics.get("win_rate", 0.0),
                    "total_trades": metrics.get("total_trades", 0),
                }
            )
        except Exception as e:
            rows.append(
                {
                    "strategy": strategy_name,
                    "backtest_supported": True,
                    "error": str(e),
                    "score": -999999,
                }
            )

    rows.sort(key=lambda x: float(x.get("score", -999999)), reverse=True)
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "initial_capital": initial_capital,
        "unsupported": unsupported,
        "ranking": rows[: max(1, int(top_n))],
    }


@router.get("/runtime")
async def get_runtime_panel():
    summary = strategy_manager.get_dashboard_summary(signal_limit=10)
    return {
        "runtime": summary.get("runtime", {}),
        "allocations": summary.get("allocations", {}),
        "strategy_performance": summary.get("strategy_performance", {}),
        "running_count": summary.get("running_count", 0),
        "timestamp": summary.get("timestamp"),
    }


@router.get("/signals/aggregated")
async def get_aggregated_signals(symbol: str):
    return strategy_manager.get_aggregated_signals(symbol)


@router.post("/start-all")
async def start_all_strategies():
    auto_registered = await _auto_register_defaults_for_start_all()
    await strategy_manager.start_all()
    started: List[str] = []
    for item in strategy_manager.list_strategies():
        name = str(item.get("name", ""))
        if not name:
            continue
        if str(item.get("state", "")).lower() == "running":
            started.append(name)
        await _persist_if_exists(name, state_override="running")
    return {
        "success": True,
        "auto_registered": auto_registered,
        "started": started,
        "started_count": len(started),
        "total_registered": len(strategy_manager.list_strategies()),
    }


@router.post("/stop-all")
async def stop_all_strategies():
    stop_results: List[Dict[str, Any]] = []
    for item in list(strategy_manager.list_strategies()):
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        success = await strategy_manager.stop_strategy(name)
        close_summary = await _close_strategy_positions(name) if success else {"requested": 0, "closed": 0, "failed": 0, "results": []}
        await _persist_if_exists(item.get("name", ""), state_override="stopped")
        stop_results.append(
            {
                "name": name,
                "stopped": bool(success),
                "close_summary": close_summary,
            }
        )
    return {"success": True, "results": stop_results}


@router.post("/register")
async def register_strategy(request: StrategyRegisterRequest):
    strategy_classes = _get_strategy_classes()
    strategy_class = strategy_classes.get(request.strategy_type)
    if not strategy_class:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown strategy type: {request.strategy_type}",
        )

    params = _build_strategy_register_params(
        strategy_type=request.strategy_type,
        exchange=request.exchange,
        user_params=request.params,
    )

    success = strategy_manager.register_strategy(
        name=request.name,
        strategy_class=strategy_class,
        params=params,
        symbols=request.symbols,
        timeframe=request.timeframe,
        allocation=request.allocation,
        runtime_limit_minutes=request.runtime_limit_minutes,
    )

    if not success:
        await audit_logger.log(
            module="strategy",
            action="register",
            status="failed",
            message=request.name,
            details=request.model_dump(),
        )
        raise HTTPException(status_code=400, detail="Failed to register strategy")

    await audit_logger.log(
        module="strategy",
        action="register",
        status="success",
        message=request.name,
        details=request.model_dump(),
    )
    await _persist_if_exists(request.name, state_override="idle")

    return {
        "success": True,
        "name": request.name,
        "strategy_type": request.strategy_type,
        "allocation": request.allocation,
    }


@router.post("/allocations/rebalance")
async def rebalance_allocations(request: AllocationRebalanceRequest):
    normalized = strategy_manager.rebalance_allocations(request.allocations)
    for name in normalized.keys():
        await _persist_if_exists(name)
    return {
        "success": True,
        "allocations": normalized,
    }


@router.get("/health/monitor")
async def get_strategy_health_monitor():
    return strategy_health_monitor.get_status()


@router.get("/health")
async def get_strategy_health_status():
    return strategy_health_monitor.get_status()


@router.get("/health-monitor")
async def get_strategy_health_monitor_alias():
    return strategy_health_monitor.get_status()


@router.post("/health/check")
async def run_strategy_health_check():
    result = await strategy_health_monitor.check_once()
    return {
        "success": True,
        "result": result,
        "monitor": strategy_health_monitor.get_status(),
    }


@router.get("/{name}")
async def get_strategy(name: str):
    alias = str(name or "").strip().lower()
    # Defensive aliasing: avoid accidental dynamic-route fallback for known static paths.
    if alias in {"library", "library/"}:
        return await get_strategy_library()
    if alias in {"summary", "runtime"}:
        return strategy_manager.get_dashboard_summary(signal_limit=20)

    info = strategy_manager.get_strategy_info(name)
    if info:
        return info
    raise HTTPException(status_code=404, detail="Strategy not found")


@router.get("/{name}/params/schema")
async def get_strategy_params_schema(name: str):
    schema = strategy_manager.get_strategy_param_schema(name)
    if schema:
        return schema
    raise HTTPException(status_code=404, detail="Strategy not found")


@router.get("/{name}/sizing-preview")
async def get_strategy_sizing_preview(name: str):
    return await _build_strategy_sizing_preview(name)


@router.get("/{name}/live-vs-backtest")
async def get_live_vs_backtest(name: str, initial_capital: float = 10000):
    info = strategy_manager.get_strategy_info(name)
    if not info:
        raise HTTPException(status_code=404, detail="Strategy not found")

    symbols = info.get("symbols") or ["BTC/USDT"]
    symbol = symbols[0]
    timeframe = info.get("timeframe", "1h")
    exchange = info.get("exchange", "gate")

    df = await data_storage.load_klines_from_parquet(
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
    )

    if df.empty:
        for alt in ["gate", "binance"]:
            df = await data_storage.load_klines_from_parquet(
                exchange=alt,
                symbol=symbol,
                timeframe=timeframe,
            )
            if not df.empty:
                break

    if df.empty:
        raise HTTPException(status_code=404, detail="缺少历史K线，无法生成对比")

    backtest = _run_backtest_core(
        strategy=info.get("strategy_type", "MAStrategy"),
        df=df.tail(2000),
        timeframe=timeframe,
        initial_capital=initial_capital,
    )

    runtime = info.get("runtime", {})
    return {
        "strategy": name,
        "symbol": symbol,
        "timeframe": timeframe,
        "live": {
            "state": info.get("state"),
            "run_count": runtime.get("run_count", 0),
            "signal_count": runtime.get("signal_count", 0),
            "error_count": runtime.get("error_count", 0),
            "last_run_at": runtime.get("last_run_at"),
            "last_signal_at": runtime.get("last_signal_at"),
            "avg_cycle_ms": runtime.get("avg_cycle_ms", 0),
            "started_at": runtime.get("started_at"),
            "uptime_seconds": runtime.get("uptime_seconds", 0),
            "account_id": runtime.get("account_id") or info.get("account_id"),
            "isolated_account": bool(runtime.get("isolated_account", False)),
            "runner_alive": bool(runtime.get("runner_alive", False)),
            "allocation": info.get("allocation", 1.0),
        },
        "backtest": backtest,
    }


@router.post("/{name}/start")
async def start_strategy(name: str):
    success = await strategy_manager.start_strategy(name)
    if success:
        await _persist_if_exists(name, state_override="running")
        await audit_logger.log(module="strategy", action="start", status="success", message=name)
        return {"success": True, "name": name, "status": "running"}
    await audit_logger.log(module="strategy", action="start", status="failed", message=name)
    raise HTTPException(status_code=400, detail="Failed to start strategy")


@router.post("/{name}/stop")
async def stop_strategy(name: str):
    success = await strategy_manager.stop_strategy(name)
    if success:
        close_summary = await _close_strategy_positions(name)
        await _persist_if_exists(name, state_override="stopped")
        await audit_logger.log(module="strategy", action="stop", status="success", message=name)
        return {"success": True, "name": name, "status": "stopped", "close_summary": close_summary}
    await audit_logger.log(module="strategy", action="stop", status="failed", message=name)
    raise HTTPException(status_code=400, detail="Failed to stop strategy")


@router.post("/{name}/pause")
async def pause_strategy(name: str):
    success = await strategy_manager.pause_strategy(name)
    if success:
        await _persist_if_exists(name, state_override="paused")
        await audit_logger.log(module="strategy", action="pause", status="success", message=name)
        return {"success": True, "name": name, "status": "paused"}
    await audit_logger.log(module="strategy", action="pause", status="failed", message=name)
    raise HTTPException(status_code=400, detail="Failed to pause strategy")


@router.put("/{name}/params")
async def update_strategy_params(name: str, request: StrategyUpdateRequest):
    info = strategy_manager.get_strategy_info(name)
    if not info:
        raise HTTPException(status_code=404, detail="Strategy not found")
    strategy_type = str(info.get("strategy_type") or "")
    normalized_params = _normalize_strategy_specific_params(strategy_type, dict(request.params or {}))
    success = strategy_manager.update_strategy_params(name, normalized_params)
    if success:
        await _persist_if_exists(name)
        await audit_logger.log(
            module="strategy",
            action="update_params",
            status="success",
            message=name,
            details=normalized_params,
        )
        return {"success": True, "name": name}
    await audit_logger.log(
        module="strategy",
        action="update_params",
        status="failed",
        message=name,
        details=normalized_params,
    )
    raise HTTPException(status_code=400, detail="Failed to update params")


@router.put("/{name}/config")
async def update_strategy_config(name: str, request: StrategyConfigUpdateRequest):
    info = strategy_manager.get_strategy_info(name)
    if not info:
        raise HTTPException(status_code=404, detail="Strategy not found")

    normalized_symbols = _normalize_symbols_input(request.symbols)
    success = strategy_manager.update_strategy_runtime_config(
        name,
        timeframe=request.timeframe,
        symbols=normalized_symbols,
        runtime_limit_minutes=request.runtime_limit_minutes,
    )
    if not success:
        raise HTTPException(status_code=400, detail="Invalid strategy config (timeframe/symbols/runtime)")

    await _persist_if_exists(name)
    updated = strategy_manager.get_strategy_info(name) or {}
    return {
        "success": True,
        "name": name,
        "timeframe": updated.get("timeframe"),
        "symbols": updated.get("symbols") or [],
        "runtime": updated.get("runtime") or {},
    }


@router.put("/{name}/allocation")
async def update_strategy_allocation(name: str, request: StrategyAllocationRequest):
    success = strategy_manager.update_strategy_allocation(name, request.allocation)
    if success:
        await _persist_if_exists(name)
        await audit_logger.log(
            module="strategy",
            action="update_allocation",
            status="success",
            message=name,
            details={"allocation": request.allocation},
        )
        return {"success": True, "name": name, "allocation": request.allocation}
    await audit_logger.log(
        module="strategy",
        action="update_allocation",
        status="failed",
        message=name,
        details={"allocation": request.allocation},
    )
    raise HTTPException(status_code=400, detail="Failed to update allocation")


@router.delete("/{name}")
async def unregister_strategy(name: str):
    success = strategy_manager.unregister_strategy(name)
    if success:
        await delete_strategy_snapshot(name)
        await audit_logger.log(module="strategy", action="unregister", status="success", message=name)
        return {"success": True, "name": name}
    await audit_logger.log(module="strategy", action="unregister", status="failed", message=name)
    raise HTTPException(status_code=404, detail="Strategy not found")


@router.get("/{name}/signals")
async def get_strategy_signals(name: str, limit: int = 100):
    strategy = strategy_manager.get_strategy(name)
    if not strategy:
        raise HTTPException(status_code=404, detail="Strategy not found")

    signals = strategy.get_recent_signals(limit)
    return {
        "strategy": name,
        "signals": [s.to_dict() for s in signals],
    }
