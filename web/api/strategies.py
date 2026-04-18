"""Strategy API endpoints."""
import asyncio
import inspect
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from loguru import logger
import numpy as np
import pandas as pd
from pydantic import BaseModel, Field

import strategies as strategy_module
from config.settings import settings
from config.strategy_registry import (
    DEFAULT_START_ALL_STRATEGIES,
    get_strategy_defaults,
    get_strategy_library_meta,
    get_strategy_recommended_symbols,
    get_strategy_recommended_timeframe,
)
from core.ai.research_runtime_context import resolve_runtime_research_context
from core.audit import audit_logger
from core.data import data_storage
from core.exchanges import exchange_manager
from core.risk.risk_manager import risk_manager
from core.strategies import Signal, SignalType, strategy_manager
from core.strategies.persistence import (
    persist_strategy_snapshot,
    delete_strategy_snapshot,
)
from core.strategies.runtime_policy import build_runtime_limit_policy
from core.strategies.health_monitor import strategy_health_monitor
from core.trading.execution_engine import execution_engine
from core.trading.order_manager import order_manager
from core.trading.position_manager import PositionSide, position_manager
from strategies import ALL_STRATEGIES
from web.api.backtest import (
    _load_backtest_inputs,
    _pairs_hedge_ratio_bounds,
    _pairs_hedge_ratio_series,
    _pairs_signal_bias,
    _run_backtest_core,
    get_backtest_strategy_info,
    is_strategy_backtest_supported,
)

router = APIRouter()


_MONITOR_RESAMPLE_RULES: Dict[str, str] = {
    "1m": "1min",
    "3m": "3min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1h",
    "2h": "2h",
    "4h": "4h",
    "6h": "6h",
    "8h": "8h",
    "12h": "12h",
    "1d": "1D",
    "1w": "1W",
}

_MONITOR_TIMEFRAME_FALLBACKS: List[str] = [
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "6h",
    "12h",
    "1d",
]


def _recommended_symbols(strategy_type: str) -> List[str]:
    return list(get_strategy_recommended_symbols(strategy_type))


def _recommended_timeframe(strategy_type: str) -> str:
    return str(get_strategy_recommended_timeframe(strategy_type))


def _recommended_crypto_defaults(strategy_type: str, exchange: str) -> Dict[str, Any]:
    out = dict(get_strategy_defaults(strategy_type))
    if strategy_type in {
        "MarketSentimentStrategy",
        "FundFlowStrategy",
        "WhaleActivityStrategy",
        "TriangularArbitrageStrategy",
    }:
        out["exchange"] = str(exchange or out.get("exchange") or "binance").lower()
    return out


def _effective_strategy_defaults(strategy_type: str, exchange: str, klass: Any = None) -> Dict[str, Any]:
    recommended = deepcopy(_recommended_crypto_defaults(strategy_type, exchange))
    runtime_defaults: Dict[str, Any] = {}
    strategy_class = klass or _get_strategy_classes().get(str(strategy_type))
    if strategy_class is not None:
        try:
            inst = strategy_class(name=f"defaults_{strategy_type}", params={})
            runtime_defaults = deepcopy(dict(getattr(inst, "params", {}) or {}))
        except Exception:
            runtime_defaults = {}
    merged = dict(runtime_defaults)
    merged.update(recommended)
    return merged if merged else recommended


def _clean_strategy_text(value: Any) -> str:
    return str(value or "").strip()


def _strategy_runtime_mode(name: str, info: Optional[Dict[str, Any]] = None) -> str:
    payload = dict(info or {})
    runtime = dict(payload.get("runtime") or {}) if isinstance(payload.get("runtime"), dict) else {}
    raw = payload.get("runtime_mode") or runtime.get("runtime_mode")
    text = str(raw or "").strip().lower()
    return "live" if text == "live" else "paper"


def _positions_by_strategy(name: str, runtime_mode: Optional[str] = None) -> List[Any]:
    normalized_mode = "live" if str(runtime_mode or "").strip().lower() == "live" else "paper"
    get_positions = position_manager.get_positions_by_strategy
    try:
        signature = inspect.signature(get_positions)
        parameters = list(signature.parameters.values())
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in parameters):
            return list(get_positions(name, scope=normalized_mode) or [])
        for parameter in parameters:
            if parameter.name != "scope":
                continue
            if parameter.kind == inspect.Parameter.POSITIONAL_ONLY:
                return list(get_positions(name, normalized_mode) or [])
            return list(get_positions(name, scope=normalized_mode) or [])
        positional_params = [
            item for item in parameters
            if item.kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            )
        ]
        if len(positional_params) >= 2:
            return list(get_positions(name, normalized_mode) or [])
        return list(get_positions(name) or [])
    except (TypeError, ValueError):
        try:
            return list(get_positions(name, scope=normalized_mode) or [])
        except TypeError:
            return list(get_positions(name) or [])


def _strategy_metadata(info: Dict[str, Any]) -> Dict[str, Any]:
    raw = info.get("metadata")
    return dict(raw) if isinstance(raw, dict) else {}


def _strategy_ownership_label(source: str) -> str:
    mapping = {
        "ai_research": "AI研究",
        "ai_autonomous_agent": "AI自治代理",
        "backtest_import": "回测导入",
        "manual": "手动注册",
    }
    return mapping.get(str(source or "").strip().lower(), "手动注册")


def _strategy_ownership_tone(source: str) -> str:
    mapping = {
        "ai_research": "ai-research",
        "ai_autonomous_agent": "ai-agent",
        "backtest_import": "backtest",
        "manual": "manual",
    }
    return mapping.get(str(source or "").strip().lower(), "manual")


def _compose_strategy_ownership_detail(parts: List[str]) -> str:
    return " · ".join([str(item).strip() for item in parts if str(item or "").strip()])


def _make_strategy_ownership(
    *,
    source: str,
    detail: str = "",
    candidate_id: str = "",
    proposal_id: str = "",
    runtime_mode: str = "",
    search_role: str = "",
    promotion_target: str = "",
    inferred: bool = False,
    matched_from: str = "",
    label: str = "",
) -> Dict[str, Any]:
    resolved_source = str(source or "manual").strip().lower() or "manual"
    payload: Dict[str, Any] = {
        "source": resolved_source,
        "label": str(label or _strategy_ownership_label(resolved_source)).strip() or _strategy_ownership_label(resolved_source),
        "badge_tone": _strategy_ownership_tone(resolved_source),
        "detail": str(detail or "").strip(),
        "candidate_id": str(candidate_id or "").strip(),
        "proposal_id": str(proposal_id or "").strip(),
        "runtime_mode": str(runtime_mode or "").strip(),
        "search_role": str(search_role or "").strip(),
        "promotion_target": str(promotion_target or "").strip(),
        "inferred": bool(inferred),
        "matched_from": str(matched_from or "").strip(),
    }
    return {key: value for key, value in payload.items() if value not in ("", None)}


def _build_ai_research_context_ownership(context: Dict[str, Any], *, matched_from: str) -> Optional[Dict[str, Any]]:
    payload = dict(context or {})
    candidate = payload.get("selected_candidate")
    if not isinstance(candidate, dict) or not candidate:
        candidate = payload.get("selected_eligibility")
    candidate = dict(candidate or {}) if isinstance(candidate, dict) else {}
    if not candidate:
        return None

    detail = _compose_strategy_ownership_detail(
        [
            f"模式 {candidate.get('runtime_mode_cap') or candidate.get('runtime_mode')}" if candidate.get("runtime_mode_cap") or candidate.get("runtime_mode") else "",
            f"候选 {candidate.get('candidate_id')}" if candidate.get("candidate_id") else "",
            f"提案 {candidate.get('proposal_id')}" if candidate.get("proposal_id") else "",
            f"角色 {candidate.get('search_role')}" if candidate.get("search_role") else "",
            f"目标 {candidate.get('promotion_target')}" if candidate.get("promotion_target") else "",
        ]
    )
    return _make_strategy_ownership(
        source="ai_research",
        detail=detail,
        candidate_id=_clean_strategy_text(candidate.get("candidate_id")),
        proposal_id=_clean_strategy_text(candidate.get("proposal_id")),
        runtime_mode=_clean_strategy_text(candidate.get("runtime_mode_cap") or candidate.get("runtime_mode")),
        search_role=_clean_strategy_text(candidate.get("search_role")),
        promotion_target=_clean_strategy_text(candidate.get("promotion_target")),
        inferred=True,
        matched_from=matched_from,
    )


def _build_metadata_ownership(info: Dict[str, Any], metadata: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    source_hint = _clean_strategy_text(
        metadata.get("source")
        or metadata.get("owner_group")
        or metadata.get("live_activation_source")
    ).lower()
    candidate_id = _clean_strategy_text(metadata.get("candidate_id"))
    proposal_id = _clean_strategy_text(metadata.get("proposal_id"))
    runtime_mode = _clean_strategy_text(metadata.get("runtime_mode"))
    search_role = _clean_strategy_text(metadata.get("search_role"))
    promotion_target = _clean_strategy_text(metadata.get("promotion_target"))
    custom_label = _clean_strategy_text(metadata.get("source_label"))

    if source_hint in {"ai_research", "ai_research_live_activate"} or candidate_id or proposal_id:
        return _make_strategy_ownership(
            source="ai_research",
            label=custom_label,
            detail=_compose_strategy_ownership_detail(
                [
                    f"模式 {runtime_mode}" if runtime_mode else "",
                    f"候选 {candidate_id}" if candidate_id else "",
                    f"提案 {proposal_id}" if proposal_id else "",
                    f"角色 {search_role}" if search_role else "",
                    f"目标 {promotion_target}" if promotion_target else "",
                ]
            ),
            candidate_id=candidate_id,
            proposal_id=proposal_id,
            runtime_mode=runtime_mode,
            search_role=search_role,
            promotion_target=promotion_target,
            inferred=False,
            matched_from="metadata",
        )

    strategy_type = _clean_strategy_text(info.get("strategy_type"))
    name = _clean_strategy_text(info.get("name"))
    if source_hint in {"ai_autonomous_agent", "autonomous_agent"} or strategy_type == "AI_AutonomousAgent" or name == "AI_AutonomousAgent":
        return _make_strategy_ownership(
            source="ai_autonomous_agent",
            label=custom_label,
            detail="由 AI 自治代理运行与执行链路托管",
            inferred=False,
            matched_from="metadata",
        )

    if source_hint in {"backtest", "backtest_import"}:
        return _make_strategy_ownership(
            source="backtest_import",
            label=custom_label,
            detail="由回测/批量导入生成的策略实例",
            inferred=False,
            matched_from="metadata",
        )

    if source_hint in {"manual", "user"}:
        return _make_strategy_ownership(
            source="manual",
            label=custom_label,
            detail="页面手动注册或编辑生成的策略实例",
            inferred=False,
            matched_from="metadata",
        )
    return None


def _looks_like_ai_research_runtime(info: Dict[str, Any], metadata: Dict[str, Any]) -> bool:
    if _build_metadata_ownership(info, metadata):
        ownership = _build_metadata_ownership(info, metadata) or {}
        if ownership.get("source") == "ai_research":
            return True
    name = _clean_strategy_text(info.get("name")).lower()
    account_id = _clean_strategy_text(info.get("account_id") or (info.get("params") or {}).get("account_id")).lower()
    return any(
        (
            candidate
            for candidate in (
                "_ai_" in name,
                name.startswith("ai_"),
                "_ai_" in account_id,
                account_id.startswith("ai_"),
                bool(metadata.get("registered_from") == "candidate_runtime"),
            )
            if candidate
        )
    )


def _looks_like_backtest_runtime(info: Dict[str, Any]) -> bool:
    name = _clean_strategy_text(info.get("name")).lower()
    account_id = _clean_strategy_text(info.get("account_id") or (info.get("params") or {}).get("account_id")).lower()
    return (
        name.startswith("bt_")
        or name.startswith("backtest_")
        or account_id.startswith("strategy_bt_")
        or account_id.startswith("bt_")
    )


def _resolve_strategy_ownership(info: Dict[str, Any]) -> Dict[str, Any]:
    metadata = _strategy_metadata(info)
    explicit = _build_metadata_ownership(info, metadata)
    if explicit is not None:
        if explicit.get("source") == "ai_research" and (not explicit.get("candidate_id") or not explicit.get("proposal_id")):
            try:
                context = resolve_runtime_research_context(
                    exchange=_clean_strategy_text(info.get("exchange")),
                    symbol=str((info.get("symbols") or [""])[0] or ""),
                    timeframe=_clean_strategy_text(info.get("timeframe")),
                    strategy_name=_clean_strategy_text(info.get("strategy_type") or info.get("name")),
                )
            except Exception:
                context = {}
            context_ownership = _build_ai_research_context_ownership(context, matched_from="metadata+context")
            if context_ownership is not None:
                merged = dict(context_ownership)
                merged.update(
                    {
                        "label": explicit.get("label"),
                        "badge_tone": explicit.get("badge_tone"),
                        "source": explicit.get("source"),
                        "inferred": False,
                        "matched_from": "metadata+context",
                    }
                )
                merged["detail"] = explicit.get("detail") or context_ownership.get("detail") or ""
                if explicit.get("candidate_id"):
                    merged["candidate_id"] = explicit.get("candidate_id")
                if explicit.get("proposal_id"):
                    merged["proposal_id"] = explicit.get("proposal_id")
                if explicit.get("runtime_mode"):
                    merged["runtime_mode"] = explicit.get("runtime_mode")
                if explicit.get("search_role"):
                    merged["search_role"] = explicit.get("search_role")
                if explicit.get("promotion_target"):
                    merged["promotion_target"] = explicit.get("promotion_target")
                return merged
        return explicit

    strategy_type = _clean_strategy_text(info.get("strategy_type"))
    name = _clean_strategy_text(info.get("name"))
    if strategy_type == "AI_AutonomousAgent" or name == "AI_AutonomousAgent":
        return _make_strategy_ownership(
            source="ai_autonomous_agent",
            detail="由 AI 自治代理运行与执行链路托管",
            inferred=True,
            matched_from="heuristic_strategy_name",
        )

    if _looks_like_ai_research_runtime(info, metadata):
        try:
            context = resolve_runtime_research_context(
                exchange=_clean_strategy_text(info.get("exchange")),
                symbol=str((info.get("symbols") or [""])[0] or ""),
                timeframe=_clean_strategy_text(info.get("timeframe")),
                strategy_name=_clean_strategy_text(info.get("strategy_type") or info.get("name")),
            )
        except Exception:
            context = {}
        context_ownership = _build_ai_research_context_ownership(context, matched_from="heuristic+context")
        if context_ownership is not None:
            return context_ownership
        return _make_strategy_ownership(
            source="ai_research",
            detail="AI研究运行实例（按策略命名/账户自动识别）",
            inferred=True,
            matched_from="heuristic_ai_account",
        )

    if _looks_like_backtest_runtime(info):
        return _make_strategy_ownership(
            source="backtest_import",
            detail="由回测/批量导入生成的策略实例",
            inferred=True,
            matched_from="heuristic_backtest",
        )

    return _make_strategy_ownership(
        source="manual",
        detail="页面手动注册或编辑生成的策略实例",
        inferred=True,
        matched_from="default_manual",
    )


def _enrich_strategy_info(info: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(info or {})
    payload["metadata"] = _strategy_metadata(payload)
    payload["ownership"] = _resolve_strategy_ownership(payload)
    return payload


def _safe_float(value: Any, fallback: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(fallback)
    if np.isnan(out) or np.isinf(out):
        return float(fallback)
    return float(out)


def _safe_optional_float(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except Exception:
        return None
    if np.isnan(out) or np.isinf(out):
        return None
    return float(out)


def _json_safe_value(value: Any) -> Any:
    """Convert runtime payloads to JSON-safe primitives."""
    if value is None:
        return None
    if isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return None if (np.isnan(value) or np.isinf(value)) else float(value)
    if isinstance(value, np.floating):
        num = float(value)
        return None if (np.isnan(num) or np.isinf(num)) else num
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, Enum):
        return _json_safe_value(value.value)
    if isinstance(value, dict):
        return {str(k): _json_safe_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe_value(v) for v in value]
    if hasattr(value, "isoformat") and callable(getattr(value, "isoformat")):
        try:
            return value.isoformat()
        except Exception:
            pass
    return str(value)


def _monitor_timeframe_candidates(timeframe: str) -> List[str]:
    target = str(timeframe or "1h").strip().lower() or "1h"
    target_sec = max(1, _timeframe_to_seconds(target))
    finer: List[str] = []
    coarser: List[str] = []
    same: List[str] = []
    seen = {target}
    for tf in _MONITOR_TIMEFRAME_FALLBACKS:
        if tf in seen:
            continue
        seen.add(tf)
        cur_sec = max(1, _timeframe_to_seconds(tf))
        if cur_sec < target_sec:
            finer.append(tf)
        elif cur_sec > target_sec:
            coarser.append(tf)
        else:
            same.append(tf)
    finer.sort(key=_timeframe_to_seconds, reverse=True)
    coarser.sort(key=_timeframe_to_seconds)
    return [target, *same, *finer, *coarser]


def _monitor_resample_ohlcv(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    rule = _MONITOR_RESAMPLE_RULES.get(str(timeframe or "").strip().lower())
    if not rule:
        return pd.DataFrame()
    src = df.copy()
    src.index = pd.to_datetime(src.index)
    src = src.sort_index()
    out = pd.concat(
        [
            src[["open", "high", "low", "close"]].resample(rule).agg(
                {"open": "first", "high": "max", "low": "min", "close": "last"}
            ),
            src[["volume"]].resample(rule).sum(),
        ],
        axis=1,
    )
    return out.dropna(subset=["open", "high", "low", "close"])


async def _load_monitor_ohlcv_with_fallback(
    *,
    exchange: str,
    symbol: str,
    timeframe: str,
    end_time: datetime,
    bars: int,
) -> tuple[pd.DataFrame, str]:
    target_tf = str(timeframe or "1h").strip().lower() or "1h"
    target_sec = max(60, _timeframe_to_seconds(target_tf))
    start_time = end_time - timedelta(seconds=target_sec * (bars + 50))
    local_end = end_time.astimezone().replace(tzinfo=None)
    fresh_cutoff = local_end - timedelta(seconds=max(target_sec * 3, 1800))

    best_df = pd.DataFrame()
    best_source = target_tf
    best_latest: Optional[pd.Timestamp] = None

    for source_tf in _monitor_timeframe_candidates(target_tf):
        raw_df = await data_storage.load_klines_from_parquet(
            exchange=exchange,
            symbol=symbol,
            timeframe=source_tf,
            start_time=start_time,
            end_time=end_time,
        )
        if raw_df is None or raw_df.empty:
            continue

        candidate_df = raw_df.copy()
        candidate_df.index = pd.to_datetime(candidate_df.index)
        candidate_df = candidate_df.sort_index()

        if source_tf != target_tf:
            source_sec = max(60, _timeframe_to_seconds(source_tf))
            if source_sec < target_sec:
                candidate_df = _monitor_resample_ohlcv(candidate_df, target_tf)
        if candidate_df.empty:
            continue

        latest = pd.Timestamp(candidate_df.index.max())
        if best_latest is None or latest > best_latest:
            best_df = candidate_df
            best_source = source_tf
            best_latest = latest
        if latest >= fresh_cutoff:
            return candidate_df.tail(bars), source_tf

    if best_latest is not None:
        return best_df.tail(bars), best_source
    return pd.DataFrame(), target_tf


def _build_pairs_monitor_enrichment(
    primary_df: pd.DataFrame,
    pair_df: pd.DataFrame,
    params: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    if primary_df is None or primary_df.empty or pair_df is None or pair_df.empty:
        return None

    cfg = dict(params or {})
    lookback_period = max(10, int(_safe_float(cfg.get("lookback_period"), 48)))
    entry_z = abs(_safe_float(cfg.get("entry_z_score"), 2.0))
    exit_z = abs(_safe_float(cfg.get("exit_z_score"), 0.6))
    hedge_method = str(cfg.get("hedge_ratio_method") or "ols")
    min_hr, max_hr = _pairs_hedge_ratio_bounds(cfg)

    primary = primary_df.copy()
    primary.index = pd.to_datetime(primary.index)
    primary = primary[~primary.index.duplicated(keep="last")].sort_index()

    pair = pair_df.copy()
    pair.index = pd.to_datetime(pair.index)
    pair = pair[~pair.index.duplicated(keep="last")].sort_index()

    primary_close = pd.to_numeric(primary.get("close"), errors="coerce")
    pair_close = pd.to_numeric(pair.get("close"), errors="coerce")
    aligned = pd.concat(
        [primary_close.rename("close"), pair_close.rename("pair_close")],
        axis=1,
    ).sort_index()
    aligned["pair_close"] = aligned["pair_close"].ffill()
    aligned = aligned.dropna(subset=["close", "pair_close"])
    if aligned.empty:
        return None

    hedge_ratio = _pairs_hedge_ratio_series(
        aligned["close"],
        aligned["pair_close"],
        method=hedge_method,
    )
    hedge_ratio = (
        pd.to_numeric(hedge_ratio, errors="coerce")
        .clip(lower=min_hr, upper=max_hr)
        .ffill()
        .fillna(1.0)
    )
    spread = aligned["close"] - hedge_ratio * aligned["pair_close"]
    spread_mean = spread.rolling(lookback_period, min_periods=lookback_period).mean()
    spread_std = spread.rolling(lookback_period, min_periods=lookback_period).std().replace(0, np.nan)
    z_score = (spread - spread_mean) / spread_std

    enriched = primary.copy()
    enriched["pair_close"] = aligned["pair_close"].reindex(enriched.index).ffill()
    enriched["spread"] = spread.reindex(enriched.index)
    enriched["z_score"] = z_score.reindex(enriched.index)
    enriched["hedge_ratio"] = hedge_ratio.reindex(enriched.index).ffill().fillna(1.0)

    latest_hedge = pd.to_numeric(enriched["hedge_ratio"], errors="coerce")
    latest_z_series = pd.to_numeric(enriched["z_score"], errors="coerce")
    latest_spread_series = pd.to_numeric(enriched["spread"], errors="coerce")

    latest_hedge_value = latest_hedge.dropna().iloc[-1] if latest_hedge.notna().any() else np.nan
    latest_z_value = latest_z_series.dropna().iloc[-1] if latest_z_series.notna().any() else np.nan
    latest_spread_value = latest_spread_series.dropna().iloc[-1] if latest_spread_series.notna().any() else np.nan

    metrics = {
        "lookback_period": int(lookback_period),
        "entry_z_score": float(entry_z),
        "exit_z_score": float(exit_z),
        "hedge_ratio_last": _safe_optional_float(latest_hedge_value),
        "spread_last": _safe_optional_float(latest_spread_value),
        "z_score_last": _safe_optional_float(latest_z_value),
        "pair_regime": "negative_corr" if np.isfinite(latest_hedge_value) and latest_hedge_value < 0 else "positive_corr",
        "signal_bias": _pairs_signal_bias(
            float(latest_z_value) if np.isfinite(latest_z_value) else float("nan"),
            float(entry_z),
            float(exit_z),
        ),
    }

    return {
        "frame": enriched,
        "metrics": metrics,
        "portfolio_mode": "pairs_spread_dual_leg",
    }


def _strategy_monitor_trade_key(row: Dict[str, Any]) -> tuple:
    payload = dict(row or {})
    order_id = _clean_strategy_text(payload.get("order_id"))
    if order_id:
        return ("order_id", order_id)
    return (
        "composite",
        _clean_strategy_text(payload.get("timestamp")),
        _clean_strategy_text(payload.get("symbol")),
        _clean_strategy_text(payload.get("side") or payload.get("signal_type")),
        round(_safe_float(payload.get("fill_price") or payload.get("price") or 0.0), 8),
        round(_safe_float(payload.get("quantity") or 0.0), 8),
    )


def _merge_strategy_monitor_trades(
    live_review_items: List[Dict[str, Any]],
    history_trades: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    seen_keys: set[tuple] = set()

    for source_rows in (live_review_items, history_trades):
        for row in source_rows or []:
            if not isinstance(row, dict):
                continue
            key = _strategy_monitor_trade_key(row)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            merged.append(dict(row))

    merged.sort(key=lambda row: str(row.get("timestamp") or ""))
    return merged


def _normalize_order_enum_text(value: Any) -> str:
    return str(getattr(value, "value", value) or "").strip().lower()


def _strategy_monitor_order_payload(order: Any, metadata: Dict[str, Any]) -> Dict[str, Any]:
    timestamp = getattr(order, "timestamp", None)
    return {
        "id": str(getattr(order, "id", "") or ""),
        "symbol": str(getattr(order, "symbol", "") or ""),
        "exchange": str(getattr(order, "exchange", "") or ""),
        "side": _normalize_order_enum_text(getattr(order, "side", None)),
        "type": _normalize_order_enum_text(getattr(order, "type", None)),
        "status": _normalize_order_enum_text(getattr(order, "status", None)),
        "price": _safe_float(getattr(order, "price", 0.0), 0.0),
        "amount": _safe_float(getattr(order, "amount", 0.0), 0.0),
        "filled": _safe_float(getattr(order, "filled", 0.0), 0.0),
        "remaining": _safe_float(getattr(order, "remaining", 0.0), 0.0),
        "timestamp": timestamp.isoformat() if hasattr(timestamp, "isoformat") else str(timestamp or ""),
        "account_id": _clean_strategy_text(metadata.get("account_id")) or "main",
        "order_mode": _clean_strategy_text(metadata.get("order_mode")) or "normal",
        "reduce_only": bool(metadata.get("reduce_only", False)),
        "stop_loss": _safe_optional_float(metadata.get("stop_loss")),
        "take_profit": _safe_optional_float(metadata.get("take_profit")),
        "trailing_stop_pct": _safe_optional_float(metadata.get("trailing_stop_pct")),
        "trailing_stop_distance": _safe_optional_float(metadata.get("trailing_stop_distance")),
        "trigger_price": _safe_optional_float(metadata.get("trigger_price")),
    }


async def _load_strategy_open_orders(
    *,
    name: str,
    exchange: str,
    runtime_mode: str,
) -> List[Dict[str, Any]]:
    try:
        rows = await asyncio.wait_for(
            order_manager.get_open_orders(exchange=exchange),
            timeout=4.5,
        )
    except Exception as exc:
        logger.debug(f"monitor-data: open orders load failed for {name}: {exc}")
        return []

    items: List[Dict[str, Any]] = []
    for order in rows or []:
        order_id = str(getattr(order, "id", "") or "")
        metadata = order_manager.get_order_metadata(order_id)
        strategy_name = _clean_strategy_text(
            metadata.get("strategy") or getattr(order, "strategy", None)
        )
        if strategy_name != name:
            continue
        order_mode = str(metadata.get("mode") or "").strip().lower()
        if order_mode in {"paper", "live"} and order_mode != runtime_mode:
            continue
        items.append(_strategy_monitor_order_payload(order, metadata))

    items.sort(key=lambda row: str(row.get("timestamp") or ""), reverse=True)
    return items


def _shift_iso_timestamp(ts_raw: Optional[str], seconds: int) -> Optional[str]:
    text = str(ts_raw or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return text
    return (dt + timedelta(seconds=int(seconds or 0))).isoformat()


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
    allocation: float = Field(default=settings.DEFAULT_STRATEGY_ALLOCATION, ge=0.0, le=1.0)
    runtime_limit_minutes: Optional[int] = Field(default=None, ge=0, le=10080)
    metadata: Dict[str, Any] = Field(default_factory=dict)


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
    allocation: float = Field(default=settings.DEFAULT_STRATEGY_ALLOCATION, ge=0.0, le=1.0)
    state: str = "idle"
    metadata: Dict[str, Any] = Field(default_factory=dict)


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

    # Use cached equity from risk manager (avoids slow network calls in preview)
    account_equity = float((risk_manager.get_risk_report().get("equity") or {}).get("current") or 0.0)
    if account_equity <= 0:
        account_equity = float(execution_engine._cached_equity or 0.0)

    # Try parquet cache for price — no network calls in the fast path
    last_price = 0.0
    price_source = "unavailable"
    timeframe_candidates = [
        str(info.get("timeframe") or "").strip() or "1h",
        "1h", "15m", "5m", "1m",
    ]
    seen_tf: set[str] = set()
    for timeframe in timeframe_candidates:
        tf = str(timeframe or "").strip() or "1h"
        if tf in seen_tf:
            continue
        seen_tf.add(tf)
        try:
            df = await data_storage.load_klines_from_parquet(exchange=exchange, symbol=symbol, timeframe=tf)
            if df is not None and not df.empty:
                px = _safe_float(df["close"].iloc[-1], 0.0)
                if px > 0:
                    last_price = px
                    price_source = f"cache:{tf}"
                    break
        except Exception:
            continue

    # Only use live ticker if already connected (no new connection attempts)
    if last_price <= 0:
        connector = exchange_manager.get_exchange(exchange)
        if connector:
            try:
                ticker = await asyncio.wait_for(connector.get_ticker(symbol), timeout=3.0)
                last_price = float(getattr(ticker, "last", 0.0) or 0.0)
                if last_price > 0:
                    price_source = "live"
            except Exception:
                last_price = 0.0

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
    runtime_mode = _strategy_runtime_mode(name, strategy_manager.get_strategy_info(name) or {})
    positions = _positions_by_strategy(name, runtime_mode)
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
            timestamp=datetime.now(timezone.utc),
            strategy_name=name,
            strength=1.0,
            quantity=float(pos.quantity or 0.0),
            metadata={
                "exchange": str(pos.exchange or "binance"),
                "account_id": str(pos.account_id or "main"),
                "source": "strategy_stop_close",
                "close_reason": "strategy_stopped",
                "runtime_mode": runtime_mode,
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
    index = pd.date_range(end=datetime.now(timezone.utc), periods=max(120, int(rows)), freq="H")
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
    index = pd.date_range(end=datetime.now(timezone.utc), periods=max(120, int(rows)), freq="H")
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

        params = _build_strategy_register_params(strategy_type, "binance", {})
        runtime_policy = build_runtime_limit_policy(
            timeframe=_recommended_timeframe(strategy_type),
            params=params,
        )
        ok = strategy_manager.register_strategy(
            name=name,
            strategy_class=strategy_class,
            params=params,
            symbols=_recommended_symbols(strategy_type),
            timeframe=_recommended_timeframe(strategy_type),
            allocation=allocation,
            runtime_limit_minutes=runtime_policy["runtime_limit_minutes"],
        )
        if not ok:
            continue

        await _persist_if_exists(name, state_override="idle")
        created.append(name)

    return created


@router.get("/list")
async def list_strategies():
    available_map = _get_strategy_classes()
    registered = [_enrich_strategy_info(item) for item in strategy_manager.list_strategies()]
    return {
        "strategies": list(available_map.keys()),
        "registered": registered,
    }


@router.get("/catalog")
async def get_strategy_catalog():
    classes = _get_strategy_classes()
    rows: List[Dict[str, Any]] = []
    for name in sorted(classes.keys()):
        meta = get_strategy_library_meta(name)
        rows.append(
            {
                "name": name,
                "category": meta.get("category", "其他"),
                "risk": meta.get("risk", "medium"),
                "usage": meta.get("usage", ""),
                "family": meta.get("family", "traditional"),
                "decision_engine": meta.get("decision_engine", "rule"),
                "ai_driven": bool(meta.get("ai_driven", False)),
                "default_start": name in DEFAULT_START_ALL_STRATEGIES,
                "recommended_timeframe": _recommended_timeframe(name),
                "recommended_symbols": _recommended_symbols(name),
                "defaults": _effective_strategy_defaults(name, "binance", classes.get(name)),
                "backtest_supported": is_strategy_backtest_supported(name),
                "backtest_reason": get_backtest_strategy_info(name).get("reason"),
            }
        )
    return {"strategies": rows, "total": len(rows), "generated_at": datetime.now(timezone.utc).isoformat()}


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
        meta = get_strategy_library_meta(name)
        required_data: Dict[str, Any] = {}
        param_schema: List[Dict[str, Any]] = []
        sample_params: Dict[str, Any] = _effective_strategy_defaults(name, "binance", klass)
        init_error: Optional[str] = None
        try:
            inst = klass(name=f"lib_{name}", params=sample_params)
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
                "family": meta.get("family", "traditional"),
                "decision_engine": meta.get("decision_engine", "rule"),
                "ai_driven": bool(meta.get("ai_driven", False)),
                "default_timeframe": _recommended_timeframe(name),
                "default_symbols": _recommended_symbols(name),
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
        "generated_at": datetime.now(timezone.utc).isoformat(),
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
        "timestamp": datetime.now(timezone.utc).isoformat(),
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
    runtime_mode = _strategy_runtime_mode(name, info)
    return {
        "strategy": {
            "name": info.get("name"),
            "strategy_type": info.get("strategy_type"),
            "params": info.get("params", {}),
            "symbols": info.get("symbols", []),
            "timeframe": info.get("timeframe", "1h"),
            "exchange": info.get("exchange", "gate"),
            "allocation": info.get("allocation", settings.DEFAULT_STRATEGY_ALLOCATION),
            "state": info.get("state", "idle"),
            "metadata": info.get("metadata", {}),
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
                "allocation": info.get("allocation", settings.DEFAULT_STRATEGY_ALLOCATION),
                "state": info.get("state", "idle"),
                "metadata": info.get("metadata", {}),
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

        runtime_policy = build_runtime_limit_policy(
            timeframe=item.timeframe,
            params=item.params,
        )
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
            runtime_limit_minutes=runtime_policy["runtime_limit_minutes"],
            metadata=item.metadata,
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
            loop_df = df
            loop_bundle = None
            if strategy_name == "FamaFactorArbitrageStrategy":
                loop_df, loop_bundle, _ = await _load_backtest_inputs(
                    strategy=strategy_name,
                    symbol=symbol,
                    timeframe=timeframe,
                )
                if loop_df.empty:
                    raise HTTPException(status_code=404, detail="Fama 回测缺少可用横截面数据")
            metrics = _run_backtest_core(
                strategy=strategy_name,
                df=loop_df,
                timeframe=timeframe,
                initial_capital=initial_capital,
                include_series=False,
                market_bundle=loop_bundle,
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
        close_summary = (
            strategy_manager.pop_last_stop_close_summary(name)
            if success
            else {"requested": 0, "closed": 0, "failed": 0, "results": []}
        )
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

    runtime_limit_minutes = request.runtime_limit_minutes
    runtime_policy = None
    if runtime_limit_minutes is None:
        runtime_policy = build_runtime_limit_policy(
            timeframe=request.timeframe,
            params=params,
        )
        runtime_limit_minutes = int(runtime_policy["runtime_limit_minutes"])

    success = strategy_manager.register_strategy(
        name=request.name,
        strategy_class=strategy_class,
        params=params,
        symbols=request.symbols,
        timeframe=request.timeframe,
        allocation=request.allocation,
        runtime_limit_minutes=runtime_limit_minutes,
        metadata=request.metadata,
    )

    if not success:
        asyncio.create_task(audit_logger.log(
            module="strategy",
            action="register",
            status="failed",
            message=request.name,
            details=request.model_dump(),
        ))
        raise HTTPException(status_code=400, detail="Failed to register strategy")

    # Fire-and-forget DB writes so the response returns immediately
    # (avoids blocking on SQLite lock held by the news background worker)
    asyncio.create_task(audit_logger.log(
        module="strategy",
        action="register",
        status="success",
        message=request.name,
        details=request.model_dump(),
    ))
    asyncio.create_task(_persist_if_exists(request.name, state_override="idle"))

    return {
        "success": True,
        "name": request.name,
        "strategy_type": request.strategy_type,
        "allocation": request.allocation,
        "runtime_limit_minutes": runtime_limit_minutes,
        "runtime_policy": runtime_policy,
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
        return _enrich_strategy_info(info)
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

    params = dict(info.get("params") or {})
    df, market_bundle, resolved_symbol = await _load_backtest_inputs(
        strategy=str(info.get("strategy_type", "MAStrategy")),
        symbol=symbol,
        timeframe=timeframe,
        params=params,
    )

    if df.empty:
        raise HTTPException(status_code=404, detail="缺少历史K线，无法生成对比")

    backtest = _run_backtest_core(
        strategy=info.get("strategy_type", "MAStrategy"),
        df=df.tail(2000),
        timeframe=timeframe,
        initial_capital=initial_capital,
        params=params,
        market_bundle=market_bundle,
    )

    runtime = info.get("runtime", {})
    return {
        "strategy": name,
        "symbol": resolved_symbol,
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
            "allocation": info.get("allocation", settings.DEFAULT_STRATEGY_ALLOCATION),
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
        close_summary = strategy_manager.pop_last_stop_close_summary(name)
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
        asyncio.create_task(delete_strategy_snapshot(name))
        asyncio.create_task(audit_logger.log(module="strategy", action="unregister", status="success", message=name))
        return {"success": True, "name": name}
    asyncio.create_task(audit_logger.log(module="strategy", action="unregister", status="failed", message=name))
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


@router.get("/{name}/monitor-data")
async def get_strategy_monitor_data(name: str, bars: int = 200):
    """Return combined OHLCV + signals + equity curve for the strategy monitor panel.

    Data sources (all read-only, no side effects):
      - OHLCV: local parquet cache, with timeframe fallback when the target file is stale
      - Signals: executed live trade journal first, fallback to strategy.get_recent_signals(200)
      - Equity curve: executed live trade journal first, fallback to risk_manager history
      - Positions: position_manager.get_positions_by_strategy(name)
    """
    bars = max(50, min(int(bars), 500))

    # ── 1. Strategy info ─────────────────────────────────────────────────────
    strategy = strategy_manager.get_strategy(name)
    info = strategy_manager.get_strategy_info(name)
    if not info:
        raise HTTPException(status_code=404, detail="Strategy not found")

    symbol = (info.get("symbols") or ["BTC/USDT"])[0]
    timeframe = str(info.get("timeframe") or "1h")
    is_running = bool(info.get("state") == "running")
    exchange = str(info.get("exchange") or "binance").strip().lower() or "binance"
    strategy_type = str(info.get("strategy_type") or info.get("name") or "").strip()
    strategy_params = dict(info.get("params") or {})
    runtime_mode = _strategy_runtime_mode(name, info)

    live_review_items: list = []
    try:
        live_review = execution_engine.get_live_trade_review(
            limit=2000,
            strategy=name,
            hours=24 * 365,
        )
        live_review_items = [
            row for row in (live_review.get("items") or [])
            if isinstance(row, dict)
        ]
    except Exception as exc:
        logger.debug(f"monitor-data: live review load failed for {name}: {exc}")

    history_trades: list = []
    try:
        history_trades = sorted(
            [
                r for r in risk_manager.get_trade_history(limit=5000)
                if isinstance(r, dict) and str(r.get("strategy") or "").strip() == name
            ],
            key=lambda r: str(r.get("timestamp") or ""),
        )
    except Exception as exc:
        logger.debug(f"monitor-data: trade history load failed for {name}: {exc}")

    merged_trades = _merge_strategy_monitor_trades(live_review_items, history_trades)
    open_orders = await _load_strategy_open_orders(
        name=name,
        exchange=exchange,
        runtime_mode=runtime_mode,
    )

    # ── 2. OHLCV bars ────────────────────────────────────────────────────────
    ohlcv: list = []
    ohlcv_source_timeframe = timeframe
    pair_symbol = ""
    pair_ohlcv_source_timeframe: Optional[str] = None
    pair_monitor: Optional[Dict[str, Any]] = None
    monitor_df = pd.DataFrame()
    monitor_load_bars = bars
    if strategy_type == "PairsTradingStrategy":
        lookback_period = max(10, int(_safe_float(strategy_params.get("lookback_period"), 48)))
        monitor_load_bars = min(500, bars + max(50, lookback_period * 2))
    try:
        end_time = datetime.now(timezone.utc)
        df, ohlcv_source_timeframe = await _load_monitor_ohlcv_with_fallback(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            end_time=end_time,
            bars=monitor_load_bars,
        )
        if df is not None and not df.empty:
            monitor_df = df.copy()

            if strategy_type == "PairsTradingStrategy":
                pair_symbol = str(strategy_params.get("pair_symbol") or "").strip()
                if pair_symbol:
                    try:
                        pair_df, pair_ohlcv_source_timeframe = await _load_monitor_ohlcv_with_fallback(
                            exchange=exchange,
                            symbol=pair_symbol,
                            timeframe=timeframe,
                            end_time=end_time,
                            bars=monitor_load_bars,
                        )
                        pair_monitor = _build_pairs_monitor_enrichment(
                            primary_df=monitor_df,
                            pair_df=pair_df,
                            params=strategy_params,
                        )
                        if pair_monitor and isinstance(pair_monitor.get("frame"), pd.DataFrame):
                            monitor_df = pair_monitor["frame"]
                    except Exception as exc:
                        logger.debug(f"monitor-data: pairs enrichment failed for {name}: {exc}")

            display_df = monitor_df.tail(bars).copy()
            for row in display_df.itertuples():
                ts = row.Index
                ts_str = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
                item = {
                    "t": ts_str,
                    "o": _safe_optional_float(getattr(row, "open", None)),
                    "h": _safe_optional_float(getattr(row, "high", None)),
                    "l": _safe_optional_float(getattr(row, "low", None)),
                    "c": _safe_optional_float(getattr(row, "close", None)),
                    "v": _safe_optional_float(getattr(row, "volume", None)),
                }
                pair_close = _safe_optional_float(getattr(row, "pair_close", None))
                spread = _safe_optional_float(getattr(row, "spread", None))
                z_score = _safe_optional_float(getattr(row, "z_score", None))
                hedge_ratio = _safe_optional_float(getattr(row, "hedge_ratio", None))
                if pair_close is not None:
                    item["pair_close"] = pair_close
                if spread is not None:
                    item["spread"] = spread
                if z_score is not None:
                    item["z_score"] = z_score
                if hedge_ratio is not None:
                    item["hedge_ratio"] = hedge_ratio
                ohlcv.append(item)
    except Exception as exc:
        logger.debug(f"monitor-data: OHLCV load failed for {name}: {exc}")

    # ── 3. Signals ───────────────────────────────────────────────────────────
    recent_signals: list = []
    if strategy:
        try:
            for sig in (strategy.get_recent_signals(200) or []):
                sig_ts = getattr(sig, "timestamp", None)
                sig_type = getattr(sig, "signal_type", None)
                sig_type_value = sig_type.value if hasattr(sig_type, "value") else str(sig_type or "")
                recent_signals.append({
                    "t":           sig_ts.isoformat() if hasattr(sig_ts, "isoformat") else str(sig_ts),
                    "type":        sig_type_value,
                    "price":       _safe_float(getattr(sig, "price", 0.0), 0.0),
                    "strength":    _safe_float(getattr(sig, "strength", 0.0), 0.0),
                    "stop_loss":   _safe_optional_float(getattr(sig, "stop_loss", None)),
                    "take_profit": _safe_optional_float(getattr(sig, "take_profit", None)),
                })
        except Exception as exc:
            logger.debug(f"monitor-data: signals failed for {name}: {exc}")

    executed_signals: list = []
    for row in merged_trades:
        signal_payload = row.get("signal") if isinstance(row.get("signal"), dict) else {}
        executed_signals.append({
            "t":           str(row.get("timestamp") or "").strip(),
            "type":        str(row.get("signal_type") or row.get("side") or "").strip().lower(),
            "price":       _safe_float(row.get("fill_price") or signal_payload.get("price") or 0.0, 0.0),
            "strength":    _safe_float(signal_payload.get("strength", row.get("strength")), 1.0),
            "stop_loss":   _safe_optional_float(signal_payload.get("stop_loss", row.get("stop_loss"))),
            "take_profit": _safe_optional_float(signal_payload.get("take_profit", row.get("take_profit"))),
        })

    restored_signals: list = []
    for row in history_trades:
        restored_signals.append({
            "t":           str(row.get("timestamp") or "").strip(),
            "type":        str(row.get("signal_type") or row.get("side") or "").strip().lower(),
            "price":       _safe_float(row.get("fill_price") or row.get("price") or 0.0, 0.0),
            "strength":    _safe_float(row.get("strength"), 1.0),
            "stop_loss":   _safe_optional_float(row.get("stop_loss")),
            "take_profit": _safe_optional_float(row.get("take_profit")),
        })

    signals = executed_signals or restored_signals or recent_signals
    signal_mode = "executed_trade" if executed_signals else ("strategy_signal" if recent_signals else "none")
    signal_summary = {
        "mode": signal_mode,
        "executed_count": len(executed_signals),
        "strategy_signal_count": len(recent_signals),
        "open_order_count": len(open_orders),
    }

    # ── 4. Equity curve with timestamps ──────────────────────────────────────
    equity: list = []
    metrics: dict = {}
    try:
        min_notional = max(1.0, float(getattr(settings, "MIN_STRATEGY_ORDER_USD", 100.0) or 100.0))
        risk_report = risk_manager.get_risk_report()
        current_equity = float(((risk_report.get("equity") or {}).get("current") or 0.0))
        config = strategy_manager._configs.get(name)
        equity_base = max(
            min_notional,
            current_equity * float((config.allocation if config else 0) or 0),
        )

        trades = list(merged_trades)

        mark = equity_base
        realized = 0.0
        timeframe_sec = max(60, _timeframe_to_seconds(timeframe))
        now_ts = datetime.now(timezone.utc).isoformat()
        base_ts = ohlcv[0]["t"] if ohlcv else None
        end_ts = ohlcv[-1]["t"] if ohlcv else now_ts
        if not base_ts and trades:
            base_ts = _shift_iso_timestamp(str(trades[0].get("timestamp") or "").strip(), -timeframe_sec)
        if not base_ts:
            base_ts = _shift_iso_timestamp(end_ts, -timeframe_sec) or end_ts
        equity.append({"t": base_ts, "v": round(mark, 4)})
        for trade in trades:
            pnl = float(trade.get("pnl") or 0.0)
            ts_raw = str(trade.get("timestamp") or "").strip()
            mark += pnl
            realized += pnl
            equity.append({"t": ts_raw or end_ts, "v": round(mark, 4)})

        unrealized = sum(
            float(p.unrealized_pnl or 0.0)
            for p in _positions_by_strategy(name, runtime_mode)
        )
        final_value = round(mark + unrealized, 4)
        if equity:
            last_ts = str((equity[-1] or {}).get("t") or "").strip()
            last_val = _safe_float((equity[-1] or {}).get("v"), mark)
            if end_ts and (last_ts != str(end_ts) or abs(last_val - final_value) > 1e-9):
                equity.append({"t": end_ts, "v": final_value})
            else:
                equity[-1]["v"] = final_value

        win_count = sum(1 for t in trades if float(t.get("pnl") or 0) > 0)
        metrics = {
            "equity_base":    round(equity_base, 2),
            "realized_pnl":   round(realized, 4),
            "unrealized_pnl": round(unrealized, 4),
            "total_pnl":      round(realized + unrealized, 4),
            "return_pct":     round((realized + unrealized) / equity_base * 100, 3) if equity_base > 0 else 0,
            "trade_count":    len(trades),
            "win_count":      win_count,
            "win_rate":       round(win_count / len(trades) * 100, 1) if trades else None,
        }
    except Exception as exc:
        logger.debug(f"monitor-data: equity curve failed for {name}: {exc}")

    # ── 5. Current open positions ─────────────────────────────────────────────
    positions_data: list = []
    try:
        for pos in _positions_by_strategy(name, runtime_mode):
            side = getattr(pos, "side", None)
            side_value = side.value if hasattr(side, "value") else str(side or "")
            entry_time = getattr(pos, "entry_time", None) or getattr(pos, "opened_at", None)
            positions_data.append({
                "symbol":             str(getattr(pos, "symbol", "") or ""),
                "side":               side_value,
                "entry_price":        _safe_float(getattr(pos, "entry_price", 0.0), 0.0),
                "current_price":      _safe_float(getattr(pos, "current_price", 0.0), 0.0),
                "quantity":           _safe_float(getattr(pos, "quantity", 0.0), 0.0),
                "unrealized_pnl":     _safe_float(getattr(pos, "unrealized_pnl", 0.0), 0.0),
                "unrealized_pnl_pct": _safe_float(getattr(pos, "unrealized_pnl_pct", 0.0), 0.0),
                "entry_time":         entry_time.isoformat() if hasattr(entry_time, "isoformat") else str(entry_time),
            })
    except Exception as exc:
        logger.debug(f"monitor-data: positions failed for {name}: {exc}")

    payload = {
        "name":       name,
        "strategy_type": strategy_type,
        "symbol":     symbol,
        "timeframe":  timeframe,
        "runtime_mode": runtime_mode,
        "ohlcv_source_timeframe": ohlcv_source_timeframe,
        "portfolio_mode": pair_monitor.get("portfolio_mode") if pair_monitor else None,
        "pair_symbol": pair_symbol or None,
        "pair_ohlcv_source_timeframe": pair_ohlcv_source_timeframe,
        "pair_metrics": (pair_monitor.get("metrics") if pair_monitor else None),
        "is_running": is_running,
        "signal_mode": signal_mode,
        "signal_summary": signal_summary,
        "ohlcv":      ohlcv,
        "signals":    signals,
        "equity":     equity,
        "metrics":    metrics,
        "positions":  positions_data,
        "open_orders": open_orders,
        "ts":         datetime.now(timezone.utc).isoformat(),
    }
    return _json_safe_value(payload)


def _timeframe_to_seconds(tf: str) -> int:
    """Convert timeframe string like '15m', '1h', '4h' to seconds."""
    import re as _re
    tf = str(tf or "1h").strip().lower()
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
    m = _re.fullmatch(r"(\d+)([smhdw])", tf)
    if m:
        return int(m.group(1)) * units[m.group(2)]
    return 3600
