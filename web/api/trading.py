"""Trading API endpoints."""
import asyncio
import json
import math
import statistics
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

import pandas as pd
import httpx
from fastapi import APIRouter, HTTPException
from loguru import logger
from pydantic import BaseModel, Field

from config.settings import settings
from core.audit import audit_logger
from core.data import data_storage
from core.exchanges import exchange_manager
from core.notifications import notification_manager
from core.realtime import event_bus
from core.risk.risk_manager import risk_manager
from core.strategies import strategy_manager
from core.trading import (
    account_manager,
    account_snapshot_manager,
    execution_engine,
    order_manager,
    position_manager,
)
from core.utils.asset_valuation import STABLE_COINS, build_currency_usd_quotes

router = APIRouter()

_BALANCE_FETCH_TIMEOUT_SEC = 14.0
_TICKER_FETCH_TIMEOUT_SEC = 1.6
_BALANCE_SNAPSHOT_CACHE_TTL_SEC = 300.0
_BALANCE_SNAPSHOT_FAST_AGE_SEC = 12.0
_BALANCE_SNAPSHOT_CACHE: Dict[str, Dict[str, Any]] = {}
_ANALYTICS_ROOT = Path("./data/cache/analytics")
_BEHAVIOR_JOURNAL_PATH = _ANALYTICS_ROOT / "behavior_journal.json"
_STOPLOSS_POLICY_PATH = _ANALYTICS_ROOT / "stoploss_policy.json"
_DEFAULT_STOPLOSS_POLICY: Dict[str, Any] = {
    "atr": {"enabled": True, "period": 14, "multiplier": 2.0},
    "time_stop": {"enabled": True, "max_hours": 24},
    "r_stop": {"enabled": True, "max_loss_r": 1.0},
    "trailing": {"enabled": True},
    "partial_stop": {"enabled": True, "r1_ratio": 0.5, "r2_ratio": 0.5},
}


class OrderRequest(BaseModel):
    exchange: str
    symbol: str
    side: str  # buy/sell
    order_type: str  # market/limit
    amount: float
    price: Optional[float] = None
    leverage: float = 1.0
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    trailing_stop_pct: Optional[float] = None
    trailing_stop_distance: Optional[float] = None
    trigger_price: Optional[float] = None
    order_mode: str = "normal"  # normal/iceberg/twap/vwap/conditional
    iceberg_parts: int = 1
    algo_slices: int = 1
    algo_interval_sec: int = 0
    account_id: str = "main"
    reduce_only: bool = False


class OrderResponse(BaseModel):
    order_id: str
    status: str
    symbol: str
    side: str
    price: float
    amount: float
    filled: float
    timestamp: str


class RiskUpdateRequest(BaseModel):
    max_position_size: Optional[float] = None
    max_daily_loss_ratio: Optional[float] = None
    max_daily_loss_usd: Optional[float] = None
    max_daily_trades: Optional[int] = None
    max_open_positions: Optional[int] = None
    max_leverage: Optional[float] = None
    balance_volatility_alert_pct: Optional[float] = None


class TradingModeRequest(BaseModel):
    target_mode: str = Field(..., pattern="^(paper|live)$")
    reason: Optional[str] = None


class TradingModeConfirmRequest(BaseModel):
    token: str
    confirm_text: str


class AccountCreateRequest(BaseModel):
    account_id: str
    name: str
    exchange: str
    mode: str = "paper"
    parent_account_id: Optional[str] = None
    enabled: bool = True
    metadata: Dict[str, Any] = Field(default_factory=dict)


class AccountUpdateRequest(BaseModel):
    name: Optional[str] = None
    exchange: Optional[str] = None
    mode: Optional[str] = None
    parent_account_id: Optional[str] = None
    enabled: Optional[bool] = None
    metadata: Optional[Dict[str, Any]] = None


class BehaviorJournalRequest(BaseModel):
    mood: str = Field(default="neutral")
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    plan_adherence: float = Field(default=0.5, ge=0.0, le=1.0)
    note: str = ""
    symbol: Optional[str] = None
    strategy: Optional[str] = None


class StoplossPolicyUpdateRequest(BaseModel):
    policy: Dict[str, Any] = Field(default_factory=dict)


_MODE_CONFIRM_TEXT = "CONFIRM LIVE TRADING"
_mode_switch_pending: Dict[str, Dict[str, Any]] = {}


def _serialize_order(order: Any) -> Dict[str, Any]:
    meta = order_manager.get_order_metadata(order.id)
    return {
        "id": order.id,
        "exchange": order.exchange,
        "symbol": order.symbol,
        "side": order.side.value,
        "type": order.type.value,
        "price": float(order.price or 0.0),
        "amount": float(order.amount or 0.0),
        "filled": float(order.filled or 0.0),
        "status": order.status.value,
        "timestamp": order.timestamp.isoformat() if order.timestamp else None,
        "strategy": meta.get("strategy"),
        "account_id": meta.get("account_id", "main"),
        "order_mode": meta.get("order_mode", "normal"),
        "stop_loss": meta.get("stop_loss"),
        "take_profit": meta.get("take_profit"),
        "trailing_stop_pct": meta.get("trailing_stop_pct"),
        "trailing_stop_distance": meta.get("trailing_stop_distance"),
        "trigger_price": meta.get("trigger_price"),
        "reduce_only": bool(meta.get("reduce_only", False)),
        "rejected": bool(meta.get("rejected", False)),
        "reject_reason": meta.get("reject_reason"),
        "paper_fee_rate": float(meta.get("paper_fee_rate") or 0.0),
        "paper_fee_usd": float(meta.get("paper_fee_usd") or 0.0),
        "paper_slippage_bps": float(meta.get("paper_slippage_bps") or 0.0),
        "paper_slippage_cost_usd": float(meta.get("paper_slippage_cost_usd") or 0.0),
        "paper_reference_price": float(meta.get("paper_reference_price") or 0.0),
        "paper_notional_usd": float(meta.get("paper_notional_usd") or 0.0),
    }


def _calc_usd_value(currency: str, total: float, last_price: Optional[float]) -> float:
    if total <= 0:
        return 0.0
    if currency in {"USDT", "USDC", "USD", "BUSD"}:
        return float(total)
    if last_price and last_price > 0:
        return float(total) * float(last_price)
    return 0.0


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return float(default)
        return out
    except Exception:
        return float(default)


def _safe_dt(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 1e12:
            ts = ts / 1000.0
        if ts > 0:
            try:
                return datetime.utcfromtimestamp(ts)
            except Exception:
                return None
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def _bucket_key(ts: datetime, mode: str) -> str:
    if mode == "hour":
        return ts.strftime("%Y-%m-%d %H:00")
    return ts.strftime("%Y-%m-%d")


def _init_analytics_paths() -> None:
    _ANALYTICS_ROOT.mkdir(parents=True, exist_ok=True)


def _read_json_file(path: Path, default: Any) -> Any:
    _init_analytics_paths()
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _write_json_file(path: Path, payload: Any) -> None:
    _init_analytics_paths()
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_behavior_journal() -> List[Dict[str, Any]]:
    rows = _read_json_file(_BEHAVIOR_JOURNAL_PATH, default=[])
    if not isinstance(rows, list):
        return []
    out: List[Dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            out.append(dict(row))
    return out


def _save_behavior_journal(rows: List[Dict[str, Any]]) -> None:
    _write_json_file(_BEHAVIOR_JOURNAL_PATH, rows[-5000:])


def _load_stoploss_policy() -> Dict[str, Any]:
    data = _read_json_file(_STOPLOSS_POLICY_PATH, default={})
    if not isinstance(data, dict):
        data = {}
    merged = dict(_DEFAULT_STOPLOSS_POLICY)
    for key, value in data.items():
        if isinstance(value, dict):
            merged[key] = dict(merged.get(key, {}), **value)
        else:
            merged[key] = value
    return merged


def _save_stoploss_policy(policy: Dict[str, Any]) -> Dict[str, Any]:
    merged = _load_stoploss_policy()
    for key, value in policy.items():
        if isinstance(value, dict):
            merged[key] = dict(merged.get(key, {}), **value)
        else:
            merged[key] = value
    _write_json_file(_STOPLOSS_POLICY_PATH, merged)
    return merged


def _iter_trade_records(days: int = 90) -> List[Dict[str, Any]]:
    cutoff_ts = datetime.utcnow().timestamp() - max(1, int(days)) * 86400
    out: List[Dict[str, Any]] = []
    signatures = set()

    for pos in position_manager.get_closed_positions(limit=20000):
        ts = getattr(pos, "updated_at", None) or getattr(pos, "opened_at", None)
        if not ts or ts.timestamp() < cutoff_ts:
            continue
        qty = _safe_float(getattr(pos, "quantity", 0.0))
        entry = _safe_float(getattr(pos, "entry_price", 0.0))
        close = _safe_float(getattr(pos, "current_price", 0.0))
        notional = abs(entry * qty)
        out.append(
            {
                "timestamp": ts,
                "symbol": str(getattr(pos, "symbol", "") or ""),
                "strategy": str(getattr(pos, "strategy", "") or "unknown"),
                "pnl": _safe_float(getattr(pos, "realized_pnl", 0.0)),
                "entry_price": entry,
                "close_price": close,
                "quantity": qty,
                "notional": notional,
                "source": "position",
            }
        )
        signatures.add((int(ts.timestamp()), str(getattr(pos, "symbol", "") or ""), round(_safe_float(getattr(pos, "realized_pnl", 0.0)), 6), str(getattr(pos, "strategy", "") or "unknown")))

    for row in risk_manager.get_trade_history(limit=30000):
        ts = _safe_dt(row.get("timestamp"))
        if not ts or ts.timestamp() < cutoff_ts:
            continue
        symbol = str(row.get("symbol") or "")
        strategy = str(row.get("strategy") or "unknown")
        pnl = _safe_float(row.get("pnl"))
        sig = (int(ts.timestamp()), symbol, round(pnl, 6), strategy)
        if sig in signatures:
            continue
        out.append(
            {
                "timestamp": ts,
                "symbol": symbol,
                "strategy": strategy,
                "pnl": pnl,
                "entry_price": 0.0,
                "close_price": 0.0,
                "quantity": 0.0,
                "notional": abs(_safe_float(row.get("notional"))),
                "source": "risk_trade",
            }
        )
        signatures.add(sig)

    out.sort(key=lambda x: x["timestamp"])
    return out


def _calc_max_streak(values: List[float], positive: bool = True) -> int:
    best = 0
    cur = 0
    for value in values:
        cond = value > 0 if positive else value < 0
        if cond:
            cur += 1
            if cur > best:
                best = cur
        else:
            cur = 0
    return best


def _drawdown_profile(equity: List[float]) -> Dict[str, Any]:
    if not equity:
        return {"max_drawdown_usd": 0.0, "max_drawdown_pct": 0.0, "duration": 0, "recovery": 0}
    peak = equity[0]
    peak_idx = 0
    max_dd = 0.0
    max_dd_pct = 0.0
    max_dd_start = 0
    max_dd_end = 0
    for idx, val in enumerate(equity):
        if val >= peak:
            peak = val
            peak_idx = idx
        dd = peak - val
        dd_pct = (dd / peak) if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd
            max_dd_pct = dd_pct
            max_dd_start = peak_idx
            max_dd_end = idx

    recovery_idx = max_dd_end
    for idx in range(max_dd_end + 1, len(equity)):
        if equity[idx] >= equity[max_dd_start]:
            recovery_idx = idx
            break

    return {
        "max_drawdown_usd": round(max_dd, 4),
        "max_drawdown_pct": round(max_dd_pct * 100, 4),
        "duration": max(0, max_dd_end - max_dd_start),
        "recovery": max(0, recovery_idx - max_dd_end),
    }


def _var_quantile(returns: List[float], confidence: float) -> float:
    if not returns:
        return 0.0
    series = sorted(float(x) for x in returns)
    q = max(0.0, min(1.0, 1.0 - confidence))
    idx = int(round((len(series) - 1) * q))
    return float(series[idx])


async def _load_symbol_returns(symbol: str, lookback: int = 240) -> pd.Series:
    frames = []
    for ex in ["binance", "gate", "okx"]:
        df = await data_storage.load_klines_from_parquet(exchange=ex, symbol=symbol, timeframe="1h")
        if df is not None and not df.empty:
            frames.append(df.tail(max(60, int(lookback)))[["close"]].rename(columns={"close": ex}))
    if not frames:
        return pd.Series(dtype=float)
    merged = pd.concat(frames, axis=1).ffill().bfill()
    close = merged.iloc[:, 0].astype(float)
    ret = close.pct_change().replace([math.inf, -math.inf], pd.NA).dropna()
    return ret.tail(max(30, int(lookback)))


async def _fetch_orderbook(exchange: str, symbol: str, limit: int = 80) -> Dict[str, Any]:
    connector = exchange_manager.get_exchange(exchange)
    if not connector:
        return {
            "available": False,
            "error": f"exchange_not_connected:{exchange}",
            "bids": [],
            "asks": [],
            "timestamp": None,
        }
    try:
        orderbook = await asyncio.wait_for(
            connector.get_order_book(symbol, limit=max(5, min(int(limit), 200))),
            timeout=2.8,
        )
    except (asyncio.TimeoutError, asyncio.CancelledError) as e:
        return {
            "available": False,
            "error": f"timeout_or_cancelled:{e}",
            "bids": [],
            "asks": [],
            "timestamp": None,
        }
    except (asyncio.TimeoutError, asyncio.CancelledError) as e:
        return {
            "available": False,
            "error": f"orderbook_timeout:{e}",
            "bids": [],
            "asks": [],
            "timestamp": None,
        }
    except Exception as e:
        return {
            "available": False,
            "error": str(e),
            "bids": [],
            "asks": [],
            "timestamp": None,
        }
    bids = orderbook.get("bids") or []
    asks = orderbook.get("asks") or []
    return {
        "available": True,
        "bids": bids,
        "asks": asks,
        "timestamp": orderbook.get("timestamp"),
    }


async def _fetch_trade_imbalance(exchange: str, symbol: str, limit: int = 600) -> Dict[str, Any]:
    connector = exchange_manager.get_exchange(exchange)
    if not connector:
        return {"count": 0, "buy_volume": 0.0, "sell_volume": 0.0, "imbalance": 0.0}
    client = getattr(connector, "_client", None)
    fetch_trades = getattr(client, "fetch_trades", None)
    if not callable(fetch_trades):
        return {"count": 0, "buy_volume": 0.0, "sell_volume": 0.0, "imbalance": 0.0}
    try:
        trades = await asyncio.wait_for(
            fetch_trades(symbol, limit=max(50, min(int(limit), 2000))),
            timeout=2.8,
        )
    except (asyncio.TimeoutError, asyncio.CancelledError):
        return {"count": 0, "buy_volume": 0.0, "sell_volume": 0.0, "imbalance": 0.0}
    except (asyncio.TimeoutError, asyncio.CancelledError):
        return {"count": 0, "buy_volume": 0.0, "sell_volume": 0.0, "imbalance": 0.0}
    except Exception:
        return {"count": 0, "buy_volume": 0.0, "sell_volume": 0.0, "imbalance": 0.0}
    buy_volume = 0.0
    sell_volume = 0.0
    for row in trades or []:
        qty = abs(_safe_float(row.get("amount")))
        side = str(row.get("side") or "").lower()
        if side == "buy":
            buy_volume += qty
        elif side == "sell":
            sell_volume += qty
        elif bool(row.get("takerOrMaker")):
            sell_volume += qty
        else:
            buy_volume += qty
    total = buy_volume + sell_volume
    return {
        "count": len(trades or []),
        "buy_volume": round(buy_volume, 6),
        "sell_volume": round(sell_volume, 6),
        "imbalance": round(((buy_volume - sell_volume) / total) if total > 0 else 0.0, 6),
    }


async def _load_rule_prices() -> Dict[str, float]:
    symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
    prices: Dict[str, float] = {}
    for exchange_name in ["gate", "binance", "okx"]:
        connector = exchange_manager.get_exchange(exchange_name)
        if not connector:
            continue
        for symbol in symbols:
            if symbol in prices:
                continue
            try:
                ticker = await connector.get_ticker(symbol)
                prices[symbol] = float(ticker.last or 0.0)
            except Exception:
                continue
    return prices


@router.post("/order", response_model=OrderResponse)
async def create_order(request: OrderRequest):
    result = await execution_engine.execute_manual_order(
        exchange=request.exchange,
        symbol=request.symbol,
        side=request.side,
        order_type=request.order_type,
        amount=request.amount,
        price=request.price,
        leverage=request.leverage,
        stop_loss=request.stop_loss,
        take_profit=request.take_profit,
        trailing_stop_pct=request.trailing_stop_pct,
        trailing_stop_distance=request.trailing_stop_distance,
        trigger_price=request.trigger_price,
        order_mode=request.order_mode,
        iceberg_parts=request.iceberg_parts,
        algo_slices=request.algo_slices,
        algo_interval_sec=request.algo_interval_sec,
        account_id=request.account_id,
        reduce_only=request.reduce_only,
        strategy="manual",
    )

    if not result:
        risk = risk_manager.get_risk_report()
        detail = risk.get("halt_reason") or "下单失败，可能触发风控限制"
        await audit_logger.log(
            module="trading",
            action="create_order",
            status="failed",
            message=detail,
            details=request.model_dump(),
        )
        raise HTTPException(status_code=400, detail=detail)

    await audit_logger.log(
        module="trading",
        action="create_order",
        status="success",
        message=f"{request.side} {request.symbol}",
        details={
            **request.model_dump(),
            "order_id": result.get("order_id") or result.get("conditional_id"),
            "filled": result.get("filled"),
        },
    )

    order_id = str(result.get("order_id") or result.get("conditional_id") or "")
    status = str(result.get("status") or "unknown")
    result_price = float(result.get("price") or 0.0)
    result_amount = float(result.get("amount") or request.amount or 0.0)
    result_filled = float(result.get("filled") or 0.0)

    return OrderResponse(
        order_id=order_id,
        status=status,
        symbol=request.symbol,
        side=request.side,
        price=result_price,
        amount=result_amount,
        filled=result_filled,
        timestamp=datetime.now().isoformat(),
    )


@router.get("/orders")
async def get_orders(
    symbol: Optional[str] = None,
    exchange: Optional[str] = None,
    include_history: bool = True,
    limit: int = 100,
):
    if include_history:
        orders = order_manager.get_recent_orders(
            symbol=symbol,
            exchange=exchange,
            limit=limit,
        )
    else:
        orders = await order_manager.get_open_orders(
            symbol=symbol,
            exchange=exchange,
        )

    return {"orders": [_serialize_order(o) for o in orders]}


@router.get("/orders/conditional")
async def get_conditional_orders():
    return {
        "orders": execution_engine.list_conditional_orders(),
        "count": len(execution_engine.list_conditional_orders()),
    }


@router.delete("/orders/conditional/{conditional_id}")
async def cancel_conditional_order(conditional_id: str):
    ok = execution_engine.cancel_conditional_order(conditional_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Conditional order not found")
    return {"success": True, "conditional_id": conditional_id}


@router.delete("/order/{order_id}")
async def cancel_order(
    order_id: str,
    symbol: str,
    exchange: str = "binance",
):
    success = await order_manager.cancel_order(order_id, symbol, exchange)
    if success:
        await audit_logger.log(
            module="trading",
            action="cancel_order",
            status="success",
            message=f"{exchange} {symbol} {order_id}",
            details={"order_id": order_id, "symbol": symbol, "exchange": exchange},
        )
        return {"success": True, "order_id": order_id}
    await audit_logger.log(
        module="trading",
        action="cancel_order",
        status="failed",
        message=f"{exchange} {symbol} {order_id}",
        details={"order_id": order_id, "symbol": symbol, "exchange": exchange},
    )
    raise HTTPException(status_code=400, detail="Failed to cancel order")


@router.delete("/orders")
async def cancel_all_orders(
    symbol: Optional[str] = None,
    exchange: str = "binance",
):
    count = await order_manager.cancel_all_orders(symbol, exchange)
    await audit_logger.log(
        module="trading",
        action="cancel_all_orders",
        status="success",
        message=f"{exchange} cancelled={count}",
        details={"symbol": symbol, "exchange": exchange, "cancelled": count},
    )
    return {"cancelled": count}


@router.get("/positions")
async def get_positions():
    positions = position_manager.get_all_positions()
    return {
        "positions": [p.to_dict() for p in positions],
        "stats": position_manager.get_stats(),
    }


@router.get("/balance")
async def get_balance(exchange: str = "gate"):
    connector = exchange_manager.get_exchange(exchange)
    if not connector:
        return {
            "exchange": exchange,
            "balances": [],
            "error": "Exchange not connected",
        }

    try:
        balances = await connector.get_balance()
        return {
            "exchange": exchange,
            "balances": [
                {
                    "currency": b.currency,
                    "free": b.free,
                    "used": b.used,
                    "total": b.total,
                }
                for b in balances
            ],
        }
    except Exception as e:
        return {
            "exchange": exchange,
            "balances": [],
            "error": str(e),
        }


@router.get("/balances")
async def get_all_balances():
    results: Dict[str, Dict[str, Any]] = {}
    total_usd = 0.0
    distribution_map: Dict[str, float] = {}
    total_unpriced_assets = 0
    mode_name = execution_engine.get_trading_mode()
    is_paper_mode = execution_engine.is_paper_mode()
    paper_account: Optional[Dict[str, Any]] = None

    async def _collect_exchange(exchange_name: str):
        now_ts = time.time()
        cached = _BALANCE_SNAPSHOT_CACHE.get(exchange_name)
        if cached and (now_ts - float(cached.get("ts", 0.0))) <= _BALANCE_SNAPSHOT_FAST_AGE_SEC:
            age = max(0.0, now_ts - float(cached.get("ts", 0.0)))
            cached_result = dict(cached.get("result") or {})
            cached_result["from_cache"] = True
            cached_result["cache_age_sec"] = round(age, 2)
            return (
                exchange_name,
                cached_result,
                float(cached.get("total_usd", 0.0) or 0.0),
                dict(cached.get("distribution") or {}),
            )

        connector = exchange_manager.get_exchange(exchange_name)
        if not connector:
            return (
                exchange_name,
                {
                    "connected": False,
                    "balances": [],
                    "total_usd": 0,
                    "from_cache": False,
                },
                0.0,
                {},
            )

        try:
            balances = await asyncio.wait_for(
                connector.get_balance(),
                timeout=_BALANCE_FETCH_TIMEOUT_SEC,
            )
            exchange_balances: List[Dict[str, Any]] = []
            exchange_total_usd = 0.0
            local_distribution: Dict[str, float] = {}

            quote_map: Dict[str, float] = {}
            price_candidates: List[str] = []
            last_unit_usd: Dict[str, float] = {}
            if cached:
                for row in (cached.get("result") or {}).get("balances", []):
                    if not isinstance(row, dict):
                        continue
                    currency = str(row.get("currency") or "").upper()
                    total_prev = float(row.get("total") or 0.0)
                    usd_prev = float(row.get("usd_value") or 0.0)
                    if currency and total_prev > 0 and usd_prev > 0:
                        last_unit_usd[currency] = usd_prev / total_prev
            for b in balances:
                ccy = str(b.currency or "").upper()
                total = float(b.total or 0.0)
                if total <= 0:
                    continue
                if ccy in STABLE_COINS:
                    continue
                if ccy not in price_candidates:
                    price_candidates.append(ccy)

            if price_candidates:
                quote_map = await build_currency_usd_quotes(
                    connector=connector,
                    currencies=price_candidates,
                    timeout_sec=_TICKER_FETCH_TIMEOUT_SEC,
                    max_parallel=2,
                )

            priced_assets = 0
            unpriced_assets = 0
            for b in balances:
                currency = str(b.currency or "").upper()
                total = float(b.total or 0.0)
                unit_usd = 1.0 if currency in STABLE_COINS else float(quote_map.get(currency, 0.0) or 0.0)
                valuation_source = "live" if unit_usd > 0 and currency not in STABLE_COINS else "stable"
                if unit_usd <= 0 and currency not in STABLE_COINS:
                    fallback_unit = float(last_unit_usd.get(currency, 0.0) or 0.0)
                    if fallback_unit > 0:
                        unit_usd = fallback_unit
                        valuation_source = "cache"
                usd_value = float(total) * float(unit_usd) if total > 0 and unit_usd > 0 else 0.0
                if total > 0:
                    if usd_value > 0:
                        priced_assets += 1
                    else:
                        unpriced_assets += 1
                exchange_total_usd += usd_value
                local_distribution[currency] = local_distribution.get(currency, 0.0) + usd_value
                exchange_balances.append(
                    {
                        "currency": currency,
                        "free": float(b.free or 0.0),
                        "used": float(b.used or 0.0),
                        "total": total,
                        "usd_value": round(usd_value, 4),
                        "unit_usd": round(float(unit_usd), 8) if unit_usd > 0 else 0.0,
                        "valuation_source": valuation_source,
                    }
                )

            exchange_balances.sort(key=lambda item: item["usd_value"], reverse=True)
            exchange_result = {
                "connected": True,
                "balances": exchange_balances,
                "total_usd": round(exchange_total_usd, 2),
                "valuation_coverage": {
                    "priced_assets": priced_assets,
                    "unpriced_assets": unpriced_assets,
                },
                "from_cache": False,
            }
            _BALANCE_SNAPSHOT_CACHE[exchange_name] = {
                "ts": time.time(),
                "result": {
                    "connected": exchange_result["connected"],
                    "balances": exchange_result["balances"],
                    "total_usd": exchange_result["total_usd"],
                },
                "total_usd": exchange_total_usd,
                "distribution": dict(local_distribution),
            }
            return (
                exchange_name,
                exchange_result,
                exchange_total_usd,
                local_distribution,
            )
        except Exception as e:
            err_msg = (
                f"balance request timeout after {_BALANCE_FETCH_TIMEOUT_SEC:.0f}s"
                if isinstance(e, asyncio.TimeoutError)
                else str(e)
            )
            logger.error(f"[{exchange_name}] Failed to get balances: {err_msg}")
            cached = _BALANCE_SNAPSHOT_CACHE.get(exchange_name)
            if cached and (time.time() - float(cached.get("ts", 0.0))) <= _BALANCE_SNAPSHOT_CACHE_TTL_SEC:
                age = max(0.0, time.time() - float(cached.get("ts", 0.0)))
                cached_result = dict(cached.get("result") or {})
                cached_result["from_cache"] = True
                cached_result["cache_age_sec"] = round(age, 2)
                cached_result["warning"] = err_msg
                return (
                    exchange_name,
                    cached_result,
                    float(cached.get("total_usd", 0.0) or 0.0),
                    dict(cached.get("distribution") or {}),
                )
            return (
                exchange_name,
                {
                    "connected": bool(getattr(connector, "is_connected", False)),
                    "error": err_msg,
                    "balances": [],
                    "total_usd": 0,
                    "from_cache": False,
                },
                0.0,
                {},
            )

    rows = await asyncio.gather(
        *[_collect_exchange(exchange_name) for exchange_name in ["gate", "binance", "okx"]],
        return_exceptions=False,
    )
    for exchange_name, exchange_result, exchange_total_usd, local_distribution in rows:
        results[exchange_name] = exchange_result
        total_usd += float(exchange_total_usd or 0.0)
        coverage = exchange_result.get("valuation_coverage") if isinstance(exchange_result, dict) else None
        total_unpriced_assets += int(((coverage or {}).get("unpriced_assets") or 0))
        for ccy, val in local_distribution.items():
            distribution_map[ccy] = distribution_map.get(ccy, 0.0) + float(val or 0.0)

    market_total_usd = float(total_usd or 0.0)
    risk_report_before = risk_manager.get_risk_report()
    prev_equity = float(((risk_report_before.get("equity") or {}).get("current") or 0.0))
    risk_equity_input = float(market_total_usd)
    paper_equity = 0.0

    if is_paper_mode:
        try:
            paper_equity = float(await execution_engine.get_account_equity_snapshot() or 0.0)
            if paper_equity > 0:
                risk_equity_input = paper_equity
        except Exception as e:
            logger.warning(f"Failed to refresh paper equity snapshot: {e}")

    if (
        (not is_paper_mode)
        and total_unpriced_assets > 0
        and prev_equity > 0
        and risk_equity_input > 0
        and risk_equity_input < prev_equity * 0.6
    ):
        logger.warning(
            f"Skip abnormal equity drop for risk update: prev={prev_equity:.4f}, "
            f"new={risk_equity_input:.4f}, unpriced_assets={total_unpriced_assets}"
        )
        risk_equity_input = prev_equity

    display_total_usd = risk_equity_input if (is_paper_mode and risk_equity_input > 0) else market_total_usd
    risk_manager.update_equity(risk_equity_input)

    if is_paper_mode:
        asset_map: Dict[str, Dict[str, float]] = {}
        long_value_sum = 0.0
        for pos in position_manager.get_all_positions():
            side_name = str(getattr(getattr(pos, "side", None), "value", getattr(pos, "side", "")) or "").lower()
            if side_name != "long":
                continue
            symbol = str(getattr(pos, "symbol", "") or "").upper()
            base = symbol.split("/")[0].strip() if "/" in symbol else symbol.strip()
            if not base:
                continue
            qty = abs(float(getattr(pos, "quantity", 0.0) or 0.0))
            px = float(getattr(pos, "current_price", 0.0) or 0.0)
            if px <= 0:
                px = float(getattr(pos, "entry_price", 0.0) or 0.0)
            if qty <= 0 or px <= 0:
                continue
            usd_val = qty * px
            long_value_sum += usd_val
            slot = asset_map.setdefault(base, {"total": 0.0, "usd_value": 0.0, "unit_usd": 0.0})
            slot["total"] += qty
            slot["usd_value"] += usd_val
            slot["unit_usd"] = px

        cash_usdt = max(0.0, float(display_total_usd) - float(long_value_sum))
        if cash_usdt > 0:
            slot = asset_map.setdefault("USDT", {"total": 0.0, "usd_value": 0.0, "unit_usd": 1.0})
            slot["total"] += cash_usdt
            slot["usd_value"] += cash_usdt
            slot["unit_usd"] = 1.0

        paper_balances: List[Dict[str, Any]] = []
        paper_distribution_map: Dict[str, float] = {}
        for ccy, row in asset_map.items():
            usd_val = float(row.get("usd_value", 0.0) or 0.0)
            if usd_val <= 0:
                continue
            total_val = float(row.get("total", 0.0) or 0.0)
            unit_usd = float(row.get("unit_usd", 0.0) or 0.0)
            paper_distribution_map[ccy] = paper_distribution_map.get(ccy, 0.0) + usd_val
            paper_balances.append(
                {
                    "currency": ccy,
                    "free": total_val,
                    "used": 0.0,
                    "total": total_val,
                    "usd_value": round(usd_val, 4),
                    "unit_usd": round(unit_usd, 8) if unit_usd > 0 else 0.0,
                    "valuation_source": "paper",
                }
            )
        paper_balances.sort(key=lambda item: item["usd_value"], reverse=True)
        paper_account = {
            "connected": True,
            "balances": paper_balances,
            "total_usd": round(float(display_total_usd), 2),
            "valuation_coverage": {
                "priced_assets": len([x for x in paper_balances if float(x.get("usd_value", 0.0) or 0.0) > 0]),
                "unpriced_assets": 0,
            },
        }
        distribution_map = paper_distribution_map

    await account_snapshot_manager.record_snapshot(
        total_usd=display_total_usd,
        exchanges=results,
        mode=mode_name,
    )

    distribution_total = float(display_total_usd if is_paper_mode else market_total_usd)
    distribution = [
        {
            "currency": ccy,
            "usd_value": round(val, 4),
            "weight": round((val / distribution_total), 6) if distribution_total > 0 else 0,
        }
        for ccy, val in sorted(distribution_map.items(), key=lambda x: x[1], reverse=True)
        if val > 0
    ]
    risk_report = risk_manager.get_risk_report()
    rule_prices = await _load_rule_prices()
    rule_eval = await notification_manager.evaluate_rules(
        {
            "total_usd": display_total_usd,
            "prices": rule_prices,
            "risk_report": risk_report,
            "position_count": position_manager.get_position_count(),
            "connected_exchanges": exchange_manager.get_connected_exchanges(),
            "strategy_summary": strategy_manager.get_dashboard_summary(signal_limit=10),
        }
    )

    return {
        "exchanges": results,
        "distribution": distribution,
        "total_usd_estimate": round(display_total_usd, 2),
        "market_total_usd_estimate": round(market_total_usd, 2),
        "paper_equity_estimate": round(paper_equity, 2) if is_paper_mode else None,
        "real_account_usd_estimate": round(market_total_usd, 2),
        "virtual_account_usd_estimate": round(paper_equity, 2) if is_paper_mode else None,
        "active_account_type": "paper" if is_paper_mode else "live",
        "active_account_usd_estimate": round(display_total_usd, 2),
        "inactive_account_usd_estimate": (
            round(market_total_usd, 2) if is_paper_mode else (round(paper_equity, 2) if paper_equity > 0 else None)
        ),
        "paper_account": paper_account,
        "risk_equity_input": round(risk_equity_input, 2),
        "unpriced_assets": total_unpriced_assets,
        "connected_exchanges": exchange_manager.get_connected_exchanges(),
        "mode": mode_name,
        "risk": {
            "trading_halted": risk_report.get("trading_halted", False),
            "risk_level": risk_report.get("risk_level", "low"),
        },
        "notifications": {
            "triggered_count": rule_eval.get("triggered_count", 0),
        },
    }


@router.get("/balances/history")
async def get_balance_history(
    hours: int = 24,
    exchange: str = "all",
    limit: int = 500,
):
    history = await account_snapshot_manager.get_history(
        hours=hours,
        exchange=exchange,
        limit=limit,
    )
    return {
        "exchange": exchange,
        "hours": hours,
        "points": len(history),
        "history": history,
    }


@router.get("/risk/report")
async def get_risk_report():
    return risk_manager.get_risk_report()


@router.post("/risk/params")
async def update_risk_params(request: RiskUpdateRequest):
    payload = request.model_dump(exclude_none=True)
    risk_manager.update_parameters(payload)
    await audit_logger.log(
        module="risk",
        action="update_params",
        status="success",
        message="Risk params updated",
        details=payload,
    )
    return {
        "success": True,
        "report": risk_manager.get_risk_report(),
    }


@router.post("/risk/reset")
async def reset_risk_halt():
    risk_manager.reset_halt()
    await audit_logger.log(
        module="risk",
        action="reset_halt",
        status="success",
        message="Risk halt reset",
    )
    return {
        "success": True,
        "report": risk_manager.get_risk_report(),
    }


@router.post("/paper/reset")
async def reset_paper_trading_state(clear_snapshots: bool = True):
    if not execution_engine.is_paper_mode():
        raise HTTPException(status_code=400, detail="当前为实盘模式，禁止执行模拟盘清零")

    runtime_reset = execution_engine.clear_paper_runtime()
    order_reset = order_manager.clear_paper_history()
    position_reset = position_manager.clear_all()
    risk_reset = risk_manager.clear_runtime_history()
    snapshots_deleted = await account_snapshot_manager.clear_history(mode="paper") if clear_snapshots else 0

    strategy_signal_cleared = 0
    strategy_position_cleared = 0
    for strategy in strategy_manager.get_all_strategies().values():
        try:
            strategy_signal_cleared += len(getattr(strategy, "signals_history", []) or [])
            strategy_position_cleared += len(getattr(strategy, "positions", {}) or {})
            strategy.signals_history.clear()
            strategy.positions.clear()
        except Exception:
            continue

    payload = {
        "runtime": runtime_reset,
        "orders": order_reset,
        "positions": position_reset,
        "risk": risk_reset,
        "snapshots_deleted": int(snapshots_deleted),
        "strategy_signal_cleared": strategy_signal_cleared,
        "strategy_position_cleared": strategy_position_cleared,
    }
    await audit_logger.log(
        module="trading",
        action="paper_reset",
        status="success",
        message="Paper trading state reset",
        details=payload,
    )
    return {"success": True, "result": payload}


@router.get("/pnl/heatmap")
async def get_pnl_heatmap(
    days: int = 30,
    bucket: str = "day",
):
    days = max(1, min(days, 365))
    mode = bucket if bucket in {"day", "hour"} else "day"
    cutoff = datetime.utcnow().timestamp() - days * 86400
    agg: Dict[tuple, float] = {}
    source_count = {"position": 0, "risk_trade": 0}

    for record in _iter_trade_records(days=days):
        ts = _safe_dt(record.get("timestamp"))
        if not ts or ts.timestamp() < cutoff:
            continue
        symbol = str(record.get("symbol") or "").strip().upper() or "UNKNOWN"
        pnl = _safe_float(record.get("pnl"))
        key = (_bucket_key(ts, mode), symbol)
        agg[key] = agg.get(key, 0.0) + pnl
        source_name = str(record.get("source") or "risk_trade")
        source_count[source_name] = source_count.get(source_name, 0) + 1

    if not agg:
        return {
            "bucket": mode,
            "days": days,
            "times": [],
            "symbols": [],
            "matrix": [],
            "points": [],
            "meta": {
                "source_count": source_count,
                "non_zero_points": 0,
            },
        }

    times = sorted({k[0] for k in agg.keys()})
    symbols = sorted({k[1] for k in agg.keys()})
    matrix: List[List[float]] = []
    points: List[Dict[str, Any]] = []

    for t in times:
        row: List[float] = []
        for s in symbols:
            pnl = round(float(agg.get((t, s), 0.0)), 6)
            row.append(pnl)
            if pnl != 0:
                points.append({"time": t, "symbol": s, "pnl": pnl})
        matrix.append(row)

    return {
        "bucket": mode,
        "days": days,
        "times": times,
        "symbols": symbols,
        "matrix": matrix,
        "points": points,
        "meta": {
            "source_count": source_count,
            "non_zero_points": len(points),
        },
    }


@router.get("/audit")
async def get_audit_logs(
    module: Optional[str] = None,
    action: Optional[str] = None,
    status: Optional[str] = None,
    hours: int = 72,
    limit: int = 200,
):
    logs = await audit_logger.list_logs(
        module=module,
        action=action,
        status=status,
        hours=hours,
        limit=limit,
    )
    return {
        "count": len(logs),
        "logs": logs,
    }


@router.get("/stats")
async def get_trading_stats():
    return {
        "orders": order_manager.get_stats(),
        "positions": position_manager.get_stats(),
        "risk": risk_manager.get_risk_report(),
        "trading_mode": execution_engine.get_trading_mode(),
    }


@router.get("/mode")
async def get_trading_mode():
    now = datetime.utcnow().isoformat()
    pending = []
    for token, item in list(_mode_switch_pending.items()):
        expires_at = item.get("expires_at")
        if expires_at and expires_at < datetime.utcnow():
            _mode_switch_pending.pop(token, None)
            continue
        pending.append(
            {
                "token": token,
                "target_mode": item.get("target_mode"),
                "reason": item.get("reason"),
                "created_at": item.get("created_at"),
                "expires_at": expires_at.isoformat() if expires_at else None,
            }
        )
    return {
        "mode": execution_engine.get_trading_mode(),
        "paper_trading": execution_engine.is_paper_mode(),
        "server_time": now,
        "pending_switches": pending,
        "confirm_hint": _MODE_CONFIRM_TEXT,
    }


@router.post("/mode/request")
async def request_trading_mode_switch(req: TradingModeRequest):
    target = req.target_mode.lower()
    if target == execution_engine.get_trading_mode():
        return {"success": True, "mode": target, "message": "当前已是目标模式"}

    token = uuid4().hex
    created_at = datetime.utcnow()
    expires_at = created_at + timedelta(minutes=5)
    _mode_switch_pending[token] = {
        "target_mode": target,
        "reason": req.reason or "",
        "created_at": created_at.isoformat(),
        "expires_at": expires_at,
    }
    return {
        "success": True,
        "token": token,
        "target_mode": target,
        "confirm_text": _MODE_CONFIRM_TEXT,
        "expires_at": expires_at.isoformat(),
        "warning": "切换实盘风险很高，请确认API权限和风控参数。",
    }


@router.post("/mode/confirm")
async def confirm_trading_mode_switch(req: TradingModeConfirmRequest):
    pending = _mode_switch_pending.get(req.token)
    if not pending:
        raise HTTPException(status_code=404, detail="切换令牌不存在或已过期")
    if pending.get("expires_at") and pending["expires_at"] < datetime.utcnow():
        _mode_switch_pending.pop(req.token, None)
        raise HTTPException(status_code=400, detail="切换令牌已过期")
    if req.confirm_text.strip() != _MODE_CONFIRM_TEXT:
        raise HTTPException(status_code=400, detail="确认文本不匹配")

    target_mode = str(pending.get("target_mode", "paper"))
    execution_engine.set_paper_trading(target_mode != "live")
    _mode_switch_pending.pop(req.token, None)

    await audit_logger.log(
        module="trading",
        action="switch_mode",
        status="success",
        message=f"mode={target_mode}",
        details={"target_mode": target_mode},
    )
    await event_bus.publish_nowait_safe(
        event="mode_changed",
        payload={"mode": execution_engine.get_trading_mode()},
    )
    return {
        "success": True,
        "mode": execution_engine.get_trading_mode(),
        "paper_trading": execution_engine.is_paper_mode(),
    }


@router.post("/mode/cancel")
async def cancel_trading_mode_switch(token: str):
    if token in _mode_switch_pending:
        _mode_switch_pending.pop(token, None)
        return {"success": True, "token": token}
    raise HTTPException(status_code=404, detail="切换令牌不存在")


@router.get("/accounts")
async def list_accounts():
    return {"accounts": account_manager.list_accounts()}


@router.post("/accounts")
async def create_account(req: AccountCreateRequest):
    try:
        item = account_manager.create_account(
            account_id=req.account_id,
            name=req.name,
            exchange=req.exchange,
            mode=req.mode,
            parent_account_id=req.parent_account_id,
            enabled=req.enabled,
            metadata=req.metadata,
        )
        return {"success": True, "account": item}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/accounts/{account_id}")
async def update_account(account_id: str, req: AccountUpdateRequest):
    payload = req.model_dump(exclude_none=True)
    try:
        item = account_manager.update_account(account_id, payload)
        return {"success": True, "account": item}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/accounts/{account_id}")
async def delete_account(account_id: str):
    try:
        ok = account_manager.delete_account(account_id)
        if not ok:
            raise HTTPException(status_code=404, detail="Account not found")
        return {"success": True, "account_id": account_id}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/accounts/summary")
async def account_summary():
    positions = [p.to_dict() for p in position_manager.get_all_positions()]
    orders = [_serialize_order(o) for o in order_manager.get_recent_orders(limit=1000)]
    agg: Dict[str, Dict[str, Any]] = {}

    for item in account_manager.list_accounts():
        aid = item["account_id"]
        agg[aid] = {
            "account": item,
            "positions": 0,
            "position_value": 0.0,
            "unrealized_pnl": 0.0,
            "orders": 0,
        }

    for p in positions:
        aid = p.get("account_id", "main")
        if aid not in agg:
            continue
        agg[aid]["positions"] += 1
        agg[aid]["position_value"] += float(p.get("value") or 0.0)
        agg[aid]["unrealized_pnl"] += float(p.get("unrealized_pnl") or 0.0)

    for o in orders:
        aid = o.get("account_id", "main")
        if aid in agg:
            agg[aid]["orders"] += 1

    return {"accounts": list(agg.values())}


def _session_name(ts: datetime) -> str:
    hour = int(ts.hour)
    if 0 <= hour < 8:
        return "亚盘"
    if 8 <= hour < 16:
        return "欧盘"
    return "美盘"


def _parse_target_allocations(raw: str) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for part in str(raw or "").split(","):
        item = part.strip()
        if not item or ":" not in item:
            continue
        k, v = item.split(":", 1)
        sym = k.strip().upper()
        if not sym:
            continue
        out[sym] = max(0.0, _safe_float(v))
    total = sum(out.values())
    if total <= 0:
        return {}
    return {k: v / total for k, v in out.items()}


async def _estimate_atr_for_symbol(symbol: str, period: int = 14) -> Optional[float]:
    period = max(3, min(int(period or 14), 200))
    for ex in ["binance", "gate", "okx"]:
        df = await data_storage.load_klines_from_parquet(exchange=ex, symbol=symbol, timeframe="1h")
        if df is None or df.empty or len(df) < (period + 5):
            continue
        src = df.tail(period * 4).copy()
        high = pd.to_numeric(src["high"], errors="coerce")
        low = pd.to_numeric(src["low"], errors="coerce")
        close = pd.to_numeric(src["close"], errors="coerce")
        tr = pd.concat(
            [
                (high - low),
                (high - close.shift(1)).abs(),
                (low - close.shift(1)).abs(),
            ],
            axis=1,
        ).max(axis=1)
        atr = tr.rolling(period, min_periods=period).mean().dropna()
        if not atr.empty:
            return _safe_float(atr.iloc[-1], default=0.0)
    return None


async def _fetch_whale_transfers(min_btc: float = 100.0) -> Dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            tx_res, px_res = await asyncio.gather(
                client.get("https://blockchain.info/unconfirmed-transactions?format=json"),
                client.get("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"),
            )
            tx_res.raise_for_status()
            px_res.raise_for_status()
            tx_json = tx_res.json() or {}
            px_json = px_res.json() or {}
    except (asyncio.TimeoutError, asyncio.CancelledError) as e:
        return {"available": False, "error": f"timeout_or_cancelled:{e}", "count": 0, "transactions": []}
    except (asyncio.TimeoutError, asyncio.CancelledError) as e:
        return {"available": False, "error": f"whale_timeout:{e}", "count": 0, "transactions": []}
    except Exception as e:
        return {"available": False, "error": str(e), "count": 0, "transactions": []}

    btc_price = _safe_float(px_json.get("price"), default=0.0)
    whales = []
    for tx in (tx_json.get("txs") or [])[:500]:
        out_value_satoshi = sum(_safe_float(v.get("value")) for v in (tx.get("out") or []))
        btc_amount = out_value_satoshi / 1e8
        if btc_amount < float(min_btc):
            continue
        ts = int(_safe_float(tx.get("time"), default=0))
        whales.append(
            {
                "hash": tx.get("hash"),
                "btc": round(btc_amount, 6),
                "usd_estimate": round(btc_amount * btc_price, 2) if btc_price > 0 else None,
                "timestamp": datetime.utcfromtimestamp(ts).isoformat() if ts > 0 else None,
            }
        )
    whales.sort(key=lambda x: _safe_float(x.get("btc")), reverse=True)
    return {
        "available": True,
        "threshold_btc": float(min_btc),
        "btc_price": btc_price,
        "count": len(whales),
        "transactions": whales[:30],
    }


async def _capture_analytics(task_name: str, coro: Any) -> Dict[str, Any]:
    started = time.perf_counter()
    try:
        data = await coro
        return {
            "task": task_name,
            "ok": True,
            "latency_ms": round((time.perf_counter() - started) * 1000, 3),
            "data": data,
        }
    except Exception as e:
        return {
            "task": task_name,
            "ok": False,
            "latency_ms": round((time.perf_counter() - started) * 1000, 3),
            "error": str(e),
        }


@router.get("/analytics/overview")
async def get_analytics_overview(
    days: int = 90,
    lookback: int = 240,
    calendar_days: int = 30,
    exchange: str = "binance",
    symbol: str = "BTC/USDT",
):
    module_jobs = {
        "performance": _capture_analytics(
            "performance",
            get_advanced_performance(days=max(1, min(int(days or 90), 720))),
        ),
        "risk_dashboard": _capture_analytics(
            "risk_dashboard",
            get_risk_dashboard(lookback=max(60, min(int(lookback or 240), 2000))),
        ),
        "calendar": _capture_analytics(
            "calendar",
            get_trading_calendar(days=max(1, min(int(calendar_days or 30), 180))),
        ),
        "microstructure": _capture_analytics(
            "microstructure",
            get_market_microstructure(exchange=exchange, symbol=symbol, depth_limit=80),
        ),
        "equity_rebalance": _capture_analytics(
            "equity_rebalance",
            get_equity_rebalance(hours=168, target_alloc="BTC:0.4,ETH:0.3,USDT:0.3"),
        ),
        "community": _capture_analytics(
            "community",
            get_community_overview(symbol=symbol, exchange=exchange),
        ),
        "behavior_report": _capture_analytics(
            "behavior_report",
            get_behavior_report(days=7),
        ),
        "stoploss_policy": _capture_analytics(
            "stoploss_policy",
            get_stoploss_policy(),
        ),
    }
    module_names = list(module_jobs.keys())
    module_results = await asyncio.gather(*module_jobs.values())
    modules = {name: result for name, result in zip(module_names, module_results)}
    ok_count = len([x for x in modules.values() if x.get("ok")])
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "all_ok": ok_count == len(modules),
        "ok_count": ok_count,
        "total": len(modules),
        "modules": modules,
    }


@router.get("/analytics/performance")
async def get_advanced_performance(days: int = 90):
    days = max(1, min(days, 720))
    records = _iter_trade_records(days=days)
    if not records:
        return {
            "days": days,
            "trade_count": 0,
            "risk_adjusted": {"sharpe": 0.0, "sortino": 0.0, "calmar": 0.0},
            "trade_quality": {"ev": 0.0, "avg_r_multiple": 0.0, "profit_factor": 0.0},
            "win_rate_breakdown": {"overall": 0.0, "by_strategy": [], "by_symbol": [], "by_session": []},
            "drawdown": {"max_drawdown_usd": 0.0, "max_drawdown_pct": 0.0, "duration": 0, "recovery": 0},
            "streaks": {"max_win_streak": 0, "max_loss_streak": 0},
        }

    pnls = [_safe_float(x.get("pnl")) for x in records]
    wins = [x for x in pnls if x > 0]
    losses = [x for x in pnls if x < 0]
    trade_count = len(pnls)
    win_rate = (len(wins) / trade_count) if trade_count > 0 else 0.0
    avg_win = statistics.fmean(wins) if wins else 0.0
    avg_loss = statistics.fmean(losses) if losses else 0.0
    ev = statistics.fmean(pnls) if pnls else 0.0
    profit_factor = (sum(wins) / abs(sum(losses))) if losses else 0.0

    trade_returns = []
    r_values = []
    for row in records:
        pnl = _safe_float(row.get("pnl"))
        notional = abs(_safe_float(row.get("notional")))
        if notional > 0:
            trade_returns.append(pnl / notional)
        risk_unit = max(1e-6, notional * 0.01) if notional > 0 else max(1.0, abs(pnl))
        r_values.append(pnl / risk_unit)

    ret_mean = statistics.fmean(trade_returns) if trade_returns else 0.0
    ret_std = statistics.pstdev(trade_returns) if len(trade_returns) > 1 else 0.0
    downside = [x for x in trade_returns if x < 0]
    downside_std = statistics.pstdev(downside) if len(downside) > 1 else 0.0
    annual_factor = math.sqrt(252.0)
    sharpe = (ret_mean / ret_std * annual_factor) if ret_std > 0 else 0.0
    sortino = (ret_mean / downside_std * annual_factor) if downside_std > 0 else 0.0

    equity_curve = [10000.0]
    for pnl in pnls:
        equity_curve.append(equity_curve[-1] + pnl)
    dd = _drawdown_profile(equity_curve)
    annual_return = ((equity_curve[-1] / equity_curve[0]) ** (365.0 / max(1.0, float(days))) - 1.0) if equity_curve[0] > 0 else 0.0
    calmar = (annual_return / max(1e-9, dd["max_drawdown_pct"] / 100.0)) if dd["max_drawdown_pct"] > 0 else 0.0

    def _breakdown(key: str) -> List[Dict[str, Any]]:
        rows: Dict[str, List[float]] = {}
        for rec in records:
            if key == "session":
                k = _session_name(rec["timestamp"])
            else:
                k = str(rec.get(key) or "unknown")
            rows.setdefault(k, []).append(_safe_float(rec.get("pnl")))
        out = []
        for k, values in rows.items():
            c = len(values)
            w = len([x for x in values if x > 0])
            out.append(
                {
                    "key": k,
                    "count": c,
                    "win_rate": round((w / c * 100) if c > 0 else 0.0, 4),
                    "avg_pnl": round(statistics.fmean(values) if values else 0.0, 6),
                    "net_pnl": round(sum(values), 6),
                }
            )
        out.sort(key=lambda x: x["net_pnl"], reverse=True)
        return out[:20]

    return {
        "days": days,
        "trade_count": trade_count,
        "risk_adjusted": {
            "sharpe": round(sharpe, 6),
            "sortino": round(sortino, 6),
            "calmar": round(calmar, 6),
        },
        "trade_quality": {
            "avg_win": round(avg_win, 6),
            "avg_loss": round(avg_loss, 6),
            "ev": round(ev, 6),
            "avg_r_multiple": round(statistics.fmean(r_values) if r_values else 0.0, 6),
            "median_r_multiple": round(statistics.median(r_values) if r_values else 0.0, 6),
            "profit_factor": round(profit_factor, 6),
        },
        "win_rate_breakdown": {
            "overall": round(win_rate * 100, 4),
            "by_strategy": _breakdown("strategy"),
            "by_symbol": _breakdown("symbol"),
            "by_session": _breakdown("session"),
        },
        "drawdown": dd,
        "streaks": {
            "max_win_streak": _calc_max_streak(pnls, positive=True),
            "max_loss_streak": _calc_max_streak(pnls, positive=False),
        },
    }


@router.get("/analytics/risk-dashboard")
async def get_risk_dashboard(lookback: int = 240):
    lookback = max(60, min(int(lookback or 240), 2000))
    report = risk_manager.get_risk_report()
    positions = position_manager.get_all_positions()
    equity = _safe_float((report.get("equity") or {}).get("current"))

    exposure_by_symbol: Dict[str, float] = {}
    weighted_lev = 0.0
    liq_rows = []
    total_exposure = 0.0
    for p in positions:
        symbol = str(getattr(p, "symbol", "") or "")
        value = abs(_safe_float(getattr(p, "value", 0.0)))
        total_exposure += value
        exposure_by_symbol[symbol] = exposure_by_symbol.get(symbol, 0.0) + value
        lev = max(1.0, _safe_float(getattr(p, "leverage", 1.0), default=1.0))
        weighted_lev += value * lev

        entry = _safe_float(getattr(p, "entry_price", 0.0))
        current = _safe_float(getattr(p, "current_price", 0.0))
        side = str(getattr(p, "side", "") or "")
        liq_price = _safe_float(getattr(p, "liquidation_price", 0.0))
        if liq_price <= 0 and entry > 0:
            liq_price = entry * (1.0 - (0.9 / lev)) if side == "long" else entry * (1.0 + (0.9 / lev))
        dist_pct = abs((current - liq_price) / current * 100) if current > 0 and liq_price > 0 else None
        liq_rows.append(
            {
                "symbol": symbol,
                "side": side,
                "current_price": round(current, 8),
                "liquidation_price": round(liq_price, 8) if liq_price > 0 else None,
                "distance_pct": round(dist_pct, 4) if dist_pct is not None else None,
            }
        )

    concentration = []
    for symbol, value in sorted(exposure_by_symbol.items(), key=lambda x: x[1], reverse=True):
        concentration.append(
            {
                "symbol": symbol,
                "exposure": round(value, 6),
                "weight": round((value / total_exposure) if total_exposure > 0 else 0.0, 6),
            }
        )

    corr_matrix: Dict[str, Dict[str, float]] = {}
    avg_abs_corr = 0.0
    symbols = [x["symbol"] for x in concentration[:8] if x["symbol"]]
    if len(symbols) >= 2:
        ret_map: Dict[str, pd.Series] = {}
        for symbol in symbols:
            ret = await _load_symbol_returns(symbol, lookback=lookback)
            if not ret.empty:
                ret_map[symbol] = ret
        if len(ret_map) >= 2:
            corr_df = pd.DataFrame(ret_map).dropna(how="any")
            if len(corr_df) >= 10:
                corr_df = corr_df.corr().fillna(0.0)
                corr_matrix = corr_df.round(4).to_dict()
                vals = []
                cols = list(corr_df.columns)
                for i in range(len(cols)):
                    for j in range(i + 1, len(cols)):
                        vals.append(abs(_safe_float(corr_df.iloc[i, j])))
                avg_abs_corr = statistics.fmean(vals) if vals else 0.0

    history = await account_snapshot_manager.get_history(hours=168, exchange="all", limit=1200)
    ret = []
    prev = None
    for row in history:
        total = _safe_float(row.get("total_usd"))
        if prev and prev > 0 and total > 0:
            ret.append((total - prev) / prev)
        prev = total
    var95 = abs(_var_quantile(ret, 0.95))
    var99 = abs(_var_quantile(ret, 0.99))

    implicit_lev = (total_exposure / equity) if equity > 0 else 0.0
    explicit_lev = (weighted_lev / total_exposure) if total_exposure > 0 else 0.0
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "risk_level": report.get("risk_level", "low"),
        "total_exposure": round(total_exposure, 6),
        "exposure_pct_of_equity": round((total_exposure / equity * 100) if equity > 0 else 0.0, 4),
        "concentration": concentration,
        "correlation_risk": {
            "avg_abs_correlation": round(avg_abs_corr, 6),
            "matrix": corr_matrix,
        },
        "leverage": {
            "implicit": round(implicit_lev, 6),
            "explicit_weighted": round(explicit_lev, 6),
        },
        "liquidation_distance": liq_rows,
        "var": {
            "var95_pct": round(var95 * 100, 6),
            "var99_pct": round(var99 * 100, 6),
            "sample_points": len(ret),
        },
    }


@router.get("/analytics/calendar")
async def get_trading_calendar(days: int = 30):
    days = max(1, min(int(days or 30), 180))
    now = datetime.utcnow()
    end = now + timedelta(days=days)
    events: List[Dict[str, Any]] = []

    month_cursor = datetime(now.year, now.month, 1)
    while month_cursor <= end:
        cpi_day = datetime(month_cursor.year, month_cursor.month, 12, 13, 30)
        while cpi_day.weekday() >= 5:
            cpi_day += timedelta(days=1)
        if now <= cpi_day <= end:
            events.append(
                {
                    "category": "economic",
                    "name": "美国CPI（预估）",
                    "time_utc": cpi_day.isoformat(),
                    "importance": "high",
                }
            )

        first_day = datetime(month_cursor.year, month_cursor.month, 1, 13, 30)
        offset = (4 - first_day.weekday()) % 7
        nfp_day = first_day + timedelta(days=offset)
        if now <= nfp_day <= end:
            events.append(
                {
                    "category": "economic",
                    "name": "美国非农就业（预估）",
                    "time_utc": nfp_day.isoformat(),
                    "importance": "high",
                }
            )
        if month_cursor.month == 12:
            month_cursor = datetime(month_cursor.year + 1, 1, 1)
        else:
            month_cursor = datetime(month_cursor.year, month_cursor.month + 1, 1)

    fomc_2026 = [
        "2026-03-18T18:00:00",
        "2026-04-29T18:00:00",
        "2026-06-17T18:00:00",
        "2026-07-29T18:00:00",
        "2026-09-16T18:00:00",
        "2026-10-28T18:00:00",
        "2026-12-09T18:00:00",
    ]
    for item in fomc_2026:
        dt = _safe_dt(item)
        if dt and now <= dt <= end:
            events.append(
                {
                    "category": "economic",
                    "name": "FOMC利率决议（预估）",
                    "time_utc": dt.isoformat(),
                    "importance": "high",
                }
            )

    unlock_templates = [
        ("APT", 20),
        ("SUI", 25),
        ("ARB", 28),
        ("OP", 21),
    ]
    for token, base_day in unlock_templates:
        dt = datetime(now.year, now.month, min(base_day, 28), 8, 0)
        for _ in range(4):
            if dt < now:
                dt = (dt + timedelta(days=32)).replace(day=min(base_day, 28))
                continue
            if dt > end:
                break
            events.append(
                {
                    "category": "unlock",
                    "name": f"{token} 代币解锁（估算）",
                    "time_utc": dt.isoformat(),
                    "importance": "medium",
                }
            )
            dt = (dt + timedelta(days=32)).replace(day=min(base_day, 28))

    expiry = now.replace(hour=8, minute=0, second=0, microsecond=0)
    for _ in range(20):
        while expiry.weekday() != 4:
            expiry += timedelta(days=1)
        if expiry > end:
            break
        if expiry >= now:
            events.append(
                {
                    "category": "expiry",
                    "name": "周五交割/到期提醒",
                    "time_utc": expiry.isoformat(),
                    "importance": "medium",
                }
            )
        expiry += timedelta(days=7)

    events.sort(key=lambda x: x["time_utc"])
    return {
        "source": "internal_estimate",
        "note": "经济与解锁事件为内置估算日历，建议与专业日历交叉确认。",
        "days": days,
        "events": events,
        "count": len(events),
    }


@router.get("/analytics/microstructure")
async def get_market_microstructure(
    exchange: str = "binance",
    symbol: str = "BTC/USDT",
    depth_limit: int = 80,
):
    ob = await _fetch_orderbook(exchange=exchange, symbol=symbol, limit=depth_limit)
    bids = [[_safe_float(x[0]), _safe_float(x[1])] for x in (ob.get("bids") or []) if len(x) >= 2]
    asks = [[_safe_float(x[0]), _safe_float(x[1])] for x in (ob.get("asks") or []) if len(x) >= 2]
    bids = [x for x in bids if x[0] > 0 and x[1] > 0]
    asks = [x for x in asks if x[0] > 0 and x[1] > 0]
    bids.sort(key=lambda x: x[0], reverse=True)
    asks.sort(key=lambda x: x[0])

    best_bid = bids[0][0] if bids else 0.0
    best_ask = asks[0][0] if asks else 0.0
    spread = best_ask - best_bid if best_bid > 0 and best_ask > 0 else 0.0
    mid = (best_bid + best_ask) / 2 if best_bid > 0 and best_ask > 0 else 0.0

    bid_depth = []
    ask_depth = []
    cumulative = 0.0
    for price, qty in bids[:100]:
        cumulative += qty
        bid_depth.append({"price": round(price, 8), "qty": round(qty, 8), "cum_qty": round(cumulative, 8)})
    cumulative = 0.0
    for price, qty in asks[:100]:
        cumulative += qty
        ask_depth.append({"price": round(price, 8), "qty": round(qty, 8), "cum_qty": round(cumulative, 8)})

    all_sizes = sorted([x[1] for x in bids + asks])
    size_threshold = all_sizes[int(len(all_sizes) * 0.95)] if all_sizes else 0.0
    large_orders = []
    for side, rows in [("bid", bids), ("ask", asks)]:
        for price, qty in rows[:200]:
            if qty >= size_threshold and size_threshold > 0:
                large_orders.append(
                    {
                        "side": side,
                        "price": round(price, 8),
                        "qty": round(qty, 8),
                        "notional": round(price * qty, 4),
                    }
                )
    large_orders = sorted(large_orders, key=lambda x: x["notional"], reverse=True)[:30]

    iceberg_candidates = 0
    for rows in [bids[:60], asks[:60]]:
        prev_qty = None
        repeat = 0
        for _, qty in rows:
            if prev_qty is not None and abs(qty - prev_qty) <= max(1e-9, prev_qty * 0.003):
                repeat += 1
            prev_qty = qty
        if repeat >= 3:
            iceberg_candidates += 1

    flow = await _fetch_trade_imbalance(exchange=exchange, symbol=symbol, limit=800)

    funding = {"available": False}
    basis = {"available": False}
    connector = exchange_manager.get_exchange(exchange)
    client = getattr(connector, "_client", None) if connector else None
    if client:
        fetch_funding_rate = getattr(client, "fetch_funding_rate", None)
        perp_symbol = symbol if ":" in symbol else f"{symbol}:USDT"
        if callable(fetch_funding_rate):
            try:
                fr = await asyncio.wait_for(fetch_funding_rate(perp_symbol), timeout=2.5)
                funding = {
                    "available": True,
                    "symbol": perp_symbol,
                    "funding_rate": _safe_float(fr.get("fundingRate")),
                    "next_funding_time": _safe_dt(fr.get("nextFundingTimestamp")).isoformat() if _safe_dt(fr.get("nextFundingTimestamp")) else None,
                }
            except Exception:
                pass
        fetch_ticker = getattr(client, "fetch_ticker", None)
        if callable(fetch_ticker):
            try:
                spot_ticker, perp_ticker = await asyncio.wait_for(
                    asyncio.gather(
                        fetch_ticker(symbol),
                        fetch_ticker(perp_symbol),
                    ),
                    timeout=3.0,
                )
                spot_px = _safe_float((spot_ticker or {}).get("last"))
                perp_px = _safe_float((perp_ticker or {}).get("last"))
                if spot_px > 0 and perp_px > 0:
                    basis_val = (perp_px - spot_px) / spot_px
                    basis = {
                        "available": True,
                        "spot_symbol": symbol,
                        "perp_symbol": perp_symbol,
                        "spot_price": spot_px,
                        "perp_price": perp_px,
                        "basis_pct": round(basis_val * 100, 6),
                    }
            except Exception:
                pass

    return {
        "exchange": exchange,
        "symbol": symbol,
        "timestamp": datetime.utcnow().isoformat(),
        "available": bool(ob.get("available", True)),
        "source_error": ob.get("error"),
        "orderbook": {
            "best_bid": best_bid,
            "best_ask": best_ask,
            "mid_price": round(mid, 8),
            "spread": round(spread, 8),
            "spread_bps": round((spread / mid * 10000) if mid > 0 else 0.0, 6),
            "bid_depth": bid_depth,
            "ask_depth": ask_depth,
        },
        "large_orders": large_orders,
        "iceberg_detection": {
            "candidate_count": iceberg_candidates,
            "note": "基于盘口重复量级的启发式检测",
        },
        "aggressor_flow": flow,
        "funding_rate": funding,
        "spot_futures_basis": basis,
    }


@router.post("/analytics/behavior/journal")
async def add_behavior_journal(request: BehaviorJournalRequest):
    rows = _load_behavior_journal()
    item = {
        "timestamp": datetime.utcnow().isoformat(),
        "mood": str(request.mood or "neutral").strip().lower(),
        "confidence": round(_safe_float(request.confidence), 6),
        "plan_adherence": round(_safe_float(request.plan_adherence), 6),
        "note": str(request.note or "").strip(),
        "symbol": str(request.symbol or "").strip().upper() or None,
        "strategy": str(request.strategy or "").strip() or None,
    }
    rows.append(item)
    _save_behavior_journal(rows)
    return {"success": True, "entry": item, "count": len(rows)}


@router.get("/analytics/behavior/report")
async def get_behavior_report(days: int = 7):
    days = max(1, min(int(days or 7), 90))
    cutoff = datetime.utcnow() - timedelta(days=days)
    rows = []
    for row in _load_behavior_journal():
        ts = _safe_dt(row.get("timestamp"))
        if not ts or ts < cutoff:
            continue
        rows.append(dict(row, _ts=ts))

    total = len(rows)
    impulsive = [x for x in rows if _safe_float(x.get("plan_adherence")) < 0.5 or _safe_float(x.get("confidence")) < 0.35]
    mood_count: Dict[str, int] = {}
    for row in rows:
        mood = str(row.get("mood") or "neutral")
        mood_count[mood] = mood_count.get(mood, 0) + 1

    risk = risk_manager.get_risk_report()
    trade_util = _safe_float((risk.get("utilization") or {}).get("daily_trade_utilization"))
    overtrade_warn = trade_util >= 0.8

    return {
        "days": days,
        "entries": total,
        "mood_distribution": mood_count,
        "impulsive_ratio": round((len(impulsive) / total) if total > 0 else 0.0, 6),
        "avg_confidence": round(statistics.fmean([_safe_float(x.get("confidence")) for x in rows]) if rows else 0.0, 6),
        "avg_plan_adherence": round(statistics.fmean([_safe_float(x.get("plan_adherence")) for x in rows]) if rows else 0.0, 6),
        "overtrading_warning": overtrade_warn,
        "daily_trade_utilization": round(trade_util, 6),
        "deviation_alert": bool(len(impulsive) >= 3 and total >= 5),
        "recent_notes": [
            {
                "timestamp": x.get("timestamp"),
                "mood": x.get("mood"),
                "note": x.get("note"),
                "symbol": x.get("symbol"),
                "strategy": x.get("strategy"),
            }
            for x in rows[-10:]
        ],
    }


@router.get("/analytics/stoploss/policy")
async def get_stoploss_policy():
    policy = _load_stoploss_policy()
    suggestions = []
    for pos in position_manager.get_all_positions()[:50]:
        symbol = str(getattr(pos, "symbol", "") or "")
        atr = await _estimate_atr_for_symbol(symbol, period=int(((policy.get("atr") or {}).get("period") or 14)))
        atr_mult = _safe_float((policy.get("atr") or {}).get("multiplier"), default=2.0)
        entry = _safe_float(getattr(pos, "entry_price", 0.0))
        current = _safe_float(getattr(pos, "current_price", 0.0))
        qty = abs(_safe_float(getattr(pos, "quantity", 0.0)))
        side = str(getattr(pos, "side", "") or "")
        opened_at = getattr(pos, "opened_at", None)
        hold_hours = ((datetime.utcnow() - opened_at).total_seconds() / 3600.0) if isinstance(opened_at, datetime) else 0.0

        atr_stop = None
        if atr and entry > 0:
            atr_stop = entry - atr * atr_mult if side == "long" else entry + atr * atr_mult
        risk_unit = max(1e-6, entry * qty * 0.01) if entry > 0 and qty > 0 else 1.0
        current_r = (_safe_float(getattr(pos, "unrealized_pnl", 0.0)) / risk_unit)
        suggestions.append(
            {
                "symbol": symbol,
                "side": side,
                "entry_price": round(entry, 8),
                "current_price": round(current, 8),
                "atr_estimate": round(atr, 8) if atr else None,
                "atr_dynamic_stop": round(atr_stop, 8) if atr_stop else None,
                "time_stop_triggered": hold_hours >= _safe_float((policy.get("time_stop") or {}).get("max_hours"), default=24),
                "r_value": round(current_r, 6),
                "r_stop_triggered": current_r <= -abs(_safe_float((policy.get("r_stop") or {}).get("max_loss_r"), default=1.0)),
                "trailing_stop_price": _safe_float(getattr(pos, "trailing_stop_price", 0.0)) or None,
                "partial_exit_plan": policy.get("partial_stop") or {},
            }
        )
    return {"policy": policy, "position_suggestions": suggestions}


@router.post("/analytics/stoploss/policy")
async def update_stoploss_policy(request: StoplossPolicyUpdateRequest):
    policy = _save_stoploss_policy(request.policy or {})
    return {"success": True, "policy": policy}


@router.get("/analytics/equity/rebalance")
async def get_equity_rebalance(
    hours: int = 168,
    target_alloc: str = "BTC:0.4,ETH:0.3,USDT:0.3",
    drift_threshold: float = 0.08,
    monthly_return: float = 0.03,
    months: int = 12,
):
    hours = max(24, min(int(hours or 168), 24 * 365))
    hist = await account_snapshot_manager.get_history(hours=hours, exchange="all", limit=2000)
    equity_series = [{"timestamp": x.get("timestamp"), "value": _safe_float(x.get("total_usd"))} for x in hist]
    equity_series = [x for x in equity_series if x["value"] > 0]

    benchmark = {}
    points = max(60, min(len(equity_series), 800))
    for sym in ["BTC/USDT", "ETH/USDT"]:
        bdf = await data_storage.load_klines_from_parquet(exchange="binance", symbol=sym, timeframe="1h")
        if bdf is None or bdf.empty:
            continue
        close = pd.to_numeric(bdf["close"], errors="coerce").dropna().tail(points)
        if close.empty:
            continue
        base = _safe_float(close.iloc[0], default=0.0)
        if base <= 0:
            continue
        benchmark[sym] = [
            {"timestamp": idx.isoformat(), "value": round(_safe_float(px) / base, 6)}
            for idx, px in close.items()
        ]

    dist_map: Dict[str, float] = {}
    for _, item in _BALANCE_SNAPSHOT_CACHE.items():
        for ccy, value in (item.get("distribution") or {}).items():
            dist_map[str(ccy).upper()] = dist_map.get(str(ccy).upper(), 0.0) + _safe_float(value)
    total_dist = sum(dist_map.values())
    current_alloc = {k: (v / total_dist) for k, v in dist_map.items()} if total_dist > 0 else {}

    target = _parse_target_allocations(target_alloc)
    drifts = []
    for sym, tar in target.items():
        cur = _safe_float(current_alloc.get(sym), default=0.0)
        drift = cur - tar
        drifts.append({"asset": sym, "target": round(tar, 6), "current": round(cur, 6), "drift": round(drift, 6)})
    suggestions = [x for x in drifts if abs(_safe_float(x["drift"])) >= abs(_safe_float(drift_threshold))]
    suggestions.sort(key=lambda x: abs(_safe_float(x["drift"])), reverse=True)

    latest_equity = equity_series[-1]["value"] if equity_series else 0.0
    months = max(1, min(int(months or 12), 120))
    mret = _safe_float(monthly_return, default=0.03)
    compound_end = latest_equity * ((1.0 + mret) ** months) if latest_equity > 0 else 0.0

    return {
        "hours": hours,
        "equity_curve": equity_series[-800:],
        "benchmark": benchmark,
        "rebalance": {
            "target": target,
            "current": {k: round(v, 6) for k, v in current_alloc.items()},
            "drifts": drifts,
            "drift_threshold": drift_threshold,
            "suggestions": suggestions,
        },
        "compounding": {
            "start_equity": round(latest_equity, 6),
            "monthly_return_assumption": mret,
            "months": months,
            "projected_equity": round(compound_end, 6),
        },
    }


@router.get("/analytics/community/overview")
async def get_community_overview(symbol: str = "BTC/USDT", exchange: str = "binance"):
    flow = await _fetch_trade_imbalance(exchange=exchange, symbol=symbol, limit=600)
    whales = await _fetch_whale_transfers(min_btc=100.0)

    announcements = []
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            resp = await client.get(
                "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query",
                params={"type": 1, "catalogId": 48, "pageNo": 1, "pageSize": 6},
            )
            if resp.status_code == 200:
                rows = (((resp.json() or {}).get("data") or {}).get("articles") or [])
                for row in rows[:6]:
                    announcements.append(
                        {
                            "title": row.get("title"),
                            "code": row.get("code"),
                            "release_date": row.get("releaseDate"),
                        }
                    )
    except Exception:
        pass

    return {
        "symbol": symbol,
        "exchange": exchange,
        "twitter_watchlist": [
            "elonmusk",
            "cz_binance",
            "VitalikButerin",
            "lookonchain",
            "WuBlockchain",
        ],
        "flow_proxy": flow,
        "whale_transfers": whales,
        "security_alerts": {
            "source": "internal_placeholder",
            "events": [
                {"level": "info", "message": "未检测到系统级合约安全事件（需外部安全源接入增强）。"}
            ],
        },
        "announcements": announcements,
    }
