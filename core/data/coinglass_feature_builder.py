from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import pandas as pd
from sqlalchemy import select

from config.database import (
    AnalyticsDerivativesSnapshot,
    AnalyticsMarketStructureSnapshot,
    async_session_maker,
)
from config.settings import settings
from core.data.coinglass_client import (
    CoinglassBudgetExceeded,
    CoinglassClient,
    coinglass_enabled,
    get_coinglass_budget_state,
    load_coinglass_ingest_statuses,
    load_dataset_rows_for_symbol,
    persist_normalized_rows,
    persist_raw_snapshot,
    persist_symbol_registry,
    record_coinglass_ingest_status,
)
from core.data.coinglass_registry import (
    COINGLASS_DEFAULT_DATASETS,
    COINGLASS_DATASET_MANIFESTS,
    get_coinglass_manifest,
    normalize_coinglass_symbol,
)


_DEFAULT_WORKER_SYMBOL_LIMIT = 1
_DEFAULT_OVERVIEW_SYMBOL = "BTC/USDT"
_STRUCTURED_SOURCE = "coinglass_proxy"


@dataclass
class DerivativesSnapshot:
    timestamp: datetime
    symbol: str
    exchange: str
    source_key: str
    source_ts: datetime
    oi_usd: Optional[float] = None
    oi_change_5m: Optional[float] = None
    oi_change_15m: Optional[float] = None
    oi_change_1h: Optional[float] = None
    oi_change_4h: Optional[float] = None
    oi_change_24h: Optional[float] = None
    funding_rate: Optional[float] = None
    funding_rate_oi_weighted: Optional[float] = None
    funding_rate_vol_weighted: Optional[float] = None
    liquidation_long_usd: Optional[float] = None
    liquidation_short_usd: Optional[float] = None
    long_short_ratio: Optional[float] = None
    top_trader_ratio: Optional[float] = None
    basis_pct: Optional[float] = None
    taker_buy_sell_imbalance: Optional[float] = None
    futures_volume_usd: Optional[float] = None
    crowding_score: Optional[float] = None
    squeeze_score: Optional[float] = None
    distribution_score: Optional[float] = None
    orderbook_imbalance_score: Optional[float] = None
    depth_thinness_score: Optional[float] = None
    payload: Dict[str, Any] | None = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp.astimezone(timezone.utc).isoformat(),
            "symbol": self.symbol,
            "exchange": self.exchange,
            "source_key": self.source_key,
            "source_ts": self.source_ts.astimezone(timezone.utc).isoformat(),
            "oi_usd": self.oi_usd,
            "oi_change_5m": self.oi_change_5m,
            "oi_change_15m": self.oi_change_15m,
            "oi_change_1h": self.oi_change_1h,
            "oi_change_4h": self.oi_change_4h,
            "oi_change_24h": self.oi_change_24h,
            "funding_rate": self.funding_rate,
            "funding_rate_oi_weighted": self.funding_rate_oi_weighted,
            "funding_rate_vol_weighted": self.funding_rate_vol_weighted,
            "liquidation_long_usd": self.liquidation_long_usd,
            "liquidation_short_usd": self.liquidation_short_usd,
            "long_short_ratio": self.long_short_ratio,
            "top_trader_ratio": self.top_trader_ratio,
            "basis_pct": self.basis_pct,
            "taker_buy_sell_imbalance": self.taker_buy_sell_imbalance,
            "futures_volume_usd": self.futures_volume_usd,
            "crowding_score": self.crowding_score,
            "squeeze_score": self.squeeze_score,
            "distribution_score": self.distribution_score,
            "orderbook_imbalance_score": self.orderbook_imbalance_score,
            "depth_thinness_score": self.depth_thinness_score,
            "payload": dict(self.payload or {}),
        }


@dataclass
class CoinglassOverviewPayload:
    symbol: str
    available: bool
    freshness_sec: Optional[float]
    degraded_reason: Optional[str]
    quota_headroom: Dict[str, Any]
    active_datasets: List[str]
    status: List[Dict[str, Any]]
    snapshot: Optional[Dict[str, Any]]
    generated_at: str
    cached: bool
    refreshing: bool
    key_configured: bool

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "available": self.available,
            "freshness_sec": self.freshness_sec,
            "degraded_reason": self.degraded_reason,
            "quota_headroom": dict(self.quota_headroom or {}),
            "active_datasets": list(self.active_datasets or []),
            "status": list(self.status or []),
            "snapshot": dict(self.snapshot or {}) if isinstance(self.snapshot, dict) else self.snapshot,
            "generated_at": self.generated_at,
            "cached": self.cached,
            "refreshing": self.refreshing,
            "key_configured": self.key_configured,
        }


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_payload_json(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    try:
        parsed = json.loads(str(value or ""))
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _normalize_key(name: str) -> str:
    return "".join(ch for ch in str(name or "").lower() if ch.isalnum())


def _row_value(row: Mapping[str, Any], *candidates: str) -> Any:
    normalized = {_normalize_key(key): value for key, value in dict(row or {}).items()}
    for candidate in candidates:
        key = _normalize_key(candidate)
        if key in normalized:
            return normalized[key]
    return None


def _to_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except Exception:
        return None
    if pd.isna(parsed):
        return None
    return float(parsed)


def _coalesce_float(row: Mapping[str, Any], *candidates: str) -> Optional[float]:
    for candidate in candidates:
        value = _to_float(_row_value(row, candidate))
        if value is not None:
            return value
    return None


def _latest_payload_rows(dataset: str, symbol: str) -> List[Dict[str, Any]]:
    frame = load_dataset_rows_for_symbol(dataset, symbol)
    if frame.empty:
        return []
    latest_request_key = str(frame.iloc[-1].get("request_key") or "")
    if latest_request_key:
        frame = frame[frame["request_key"].astype(str) == latest_request_key]
    rows: List[Dict[str, Any]] = []
    for _, row in frame.iterrows():
        payload = _parse_payload_json(row.get("payload_json"))
        if not payload:
            continue
        payload["_source_ts"] = str(row.get("source_ts") or "")
        payload["_exchange"] = str(row.get("exchange") or "aggregate")
        rows.append(payload)
    return rows


def _dataset_payloads(symbol: str) -> Dict[str, Any]:
    return {
        dataset: _latest_payload_rows(dataset, symbol)
        for dataset in COINGLASS_DEFAULT_DATASETS
    }


def _mean(values: Iterable[Optional[float]]) -> Optional[float]:
    numeric = [float(value) for value in values if value is not None]
    if not numeric:
        return None
    return sum(numeric) / len(numeric)


def _sum(values: Iterable[Optional[float]]) -> Optional[float]:
    numeric = [float(value) for value in values if value is not None]
    if not numeric:
        return None
    return sum(numeric)


def _clamp01(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return max(0.0, min(1.0, float(value)))


def _scale_percent(value: Optional[float], divisor: float) -> float:
    if value is None or divisor <= 0:
        return 0.0
    return max(0.0, min(abs(float(value)) / divisor, 1.0))


def build_derivatives_snapshot(symbol: str) -> Optional[DerivativesSnapshot]:
    normalized_symbol = normalize_coinglass_symbol(symbol)
    if not normalized_symbol:
        return None
    payloads = _dataset_payloads(normalized_symbol)
    oi_rows = list(payloads.get("open_interest_exchange_list") or [])
    funding_rows = list(payloads.get("funding_rate_exchange_list") or [])
    taker_rows = list(payloads.get("taker_buy_sell_volume_exchange_list") or [])
    liquidation_rows = list(payloads.get("liquidation_history") or [])
    ratio_rows = list(payloads.get("global_long_short_account_ratio_history") or [])
    arbitrage_rows = list(payloads.get("funding_arbitrage") or [])
    oi_row = oi_rows[-1] if oi_rows else {}
    funding_row = funding_rows[-1] if funding_rows else {}
    taker_row = taker_rows[-1] if taker_rows else {}
    liquidation_row = liquidation_rows[-1] if liquidation_rows else {}
    ratio_row = ratio_rows[-1] if ratio_rows else {}
    arbitrage_row = arbitrage_rows[-1] if arbitrage_rows else {}
    if not any(payloads.values()):
        return None

    oi_usd = _sum(_coalesce_float(item, "open_interest_usd", "openInterestUsd", "oivalue") for item in oi_rows)
    oi_change_5m = _mean(_coalesce_float(item, "open_interest_change_5m", "change5m", "oiChange5m") for item in oi_rows)
    oi_change_15m = _mean(_coalesce_float(item, "open_interest_change_15m", "change15m", "oiChange15m") for item in oi_rows)
    oi_change_1h = _mean(_coalesce_float(item, "open_interest_change_1h", "change1h", "oiChange1h") for item in oi_rows)
    oi_change_4h = _mean(_coalesce_float(item, "open_interest_change_4h", "change4h", "oiChange4h") for item in oi_rows)
    oi_change_24h = _mean(_coalesce_float(item, "open_interest_change_24h", "change24h", "oiChange24h") for item in oi_rows)

    funding_rate = _mean(_coalesce_float(item, "funding_rate", "fundingRate") for item in funding_rows)
    funding_rate_oi_weighted = _mean(
        _coalesce_float(
            item,
            "funding_rate_oi_weighted",
            "oi_weighted_funding_rate",
            "weightedFundingRateByOi",
        )
        for item in funding_rows
    )
    funding_rate_vol_weighted = _mean(
        _coalesce_float(
            item,
            "funding_rate_vol_weighted",
            "volume_weighted_funding_rate",
            "weightedFundingRateByVolume",
        )
        for item in funding_rows
    )
    if funding_rate_oi_weighted is None:
        funding_rate_oi_weighted = _coalesce_float(arbitrage_row, "oi_weighted_funding_rate", "oiWeightedFundingRate")
    if funding_rate_vol_weighted is None:
        funding_rate_vol_weighted = _coalesce_float(arbitrage_row, "volume_weighted_funding_rate", "volWeightedFundingRate")

    liquidation_long_usd = _coalesce_float(liquidation_row, "long_liquidation_usd", "longLiquidationUsd", "longVolUsd")
    liquidation_short_usd = _coalesce_float(liquidation_row, "short_liquidation_usd", "shortLiquidationUsd", "shortVolUsd")
    long_short_ratio = _coalesce_float(ratio_row, "long_short_ratio", "longShortRatio", "ratio")
    top_trader_ratio = _coalesce_float(ratio_row, "top_trader_ratio", "topTraderRatio")

    taker_buy = _sum(_coalesce_float(item, "taker_buy_volume", "buyVolume", "buy") for item in taker_rows)
    taker_sell = _sum(_coalesce_float(item, "taker_sell_volume", "sellVolume", "sell") for item in taker_rows)
    taker_buy_sell_imbalance = None
    if taker_buy is not None and taker_sell is not None and (taker_buy + taker_sell) > 0:
        taker_buy_sell_imbalance = (taker_buy - taker_sell) / (taker_buy + taker_sell)

    basis_pct = _coalesce_float(arbitrage_row, "basis_pct", "basisPercent", "basis")
    futures_volume_usd = _sum(_coalesce_float(item, "volume_usd", "turnover_usd", "notionalUsd") for item in taker_rows)

    squeeze_score = _clamp01(
        (
            _scale_percent(oi_change_1h, 10.0) * 0.40
            + _scale_percent(taker_buy_sell_imbalance, 1.0) * 0.35
            + _scale_percent(liquidation_short_usd, 50_000_000.0) * 0.25
        )
    )
    crowding_score = _clamp01(
        (
            _scale_percent(funding_rate_oi_weighted or funding_rate, 0.0025) * 0.45
            + _scale_percent(long_short_ratio, 2.0) * 0.35
            + _scale_percent(oi_change_24h, 25.0) * 0.20
        )
    )
    distribution_score = _clamp01(
        (
            _scale_percent(liquidation_long_usd, 50_000_000.0) * 0.40
            + _scale_percent(funding_rate, 0.0035) * 0.35
            + _scale_percent(basis_pct, 0.08) * 0.25
        )
    )
    orderbook_imbalance_score = _clamp01(
        0.5 + (float(taker_buy_sell_imbalance or 0.0) * 0.5)
    )
    depth_thinness_score = _clamp01(
        1.0 - _scale_percent(futures_volume_usd, 250_000_000.0)
    )

    source_candidates = []
    for payload_group in payloads.values():
        for payload in payload_group or []:
            if not payload:
                continue
            source_ts = payload.get("_source_ts")
            if not source_ts:
                continue
            try:
                source_candidates.append(pd.Timestamp(source_ts).to_pydatetime().astimezone(timezone.utc))
            except Exception:
                continue
    source_ts = max(source_candidates) if source_candidates else _utc_now()
    source_key = f"{normalized_symbol}|{source_ts.isoformat()}"
    payload = {
        "market_regime": "trend_follow" if (squeeze_score or 0.0) >= 0.62 else "mixed",
        "funding_regime": "hot" if (funding_rate or 0.0) >= 0.001 else "balanced",
        "oi_regime": "expanding" if (oi_change_1h or 0.0) > 0 else "flat",
        "liquidation_state": "short_squeeze" if (liquidation_short_usd or 0.0) > (liquidation_long_usd or 0.0) else "long_flush",
        "orderbook_state": "buy_pressure" if (taker_buy_sell_imbalance or 0.0) > 0 else "sell_pressure",
        "crowding_warning": bool((crowding_score or 0.0) >= 0.70),
        "active_datasets": [dataset for dataset, items in payloads.items() if items],
        "payload_counts": {dataset: len(items or []) for dataset, items in payloads.items()},
    }
    return DerivativesSnapshot(
        timestamp=_utc_now(),
        symbol=normalized_symbol,
        exchange="aggregate",
        source_key=source_key,
        source_ts=source_ts,
        oi_usd=oi_usd,
        oi_change_5m=oi_change_5m,
        oi_change_15m=oi_change_15m,
        oi_change_1h=oi_change_1h,
        oi_change_4h=oi_change_4h,
        oi_change_24h=oi_change_24h,
        funding_rate=funding_rate,
        funding_rate_oi_weighted=funding_rate_oi_weighted,
        funding_rate_vol_weighted=funding_rate_vol_weighted,
        liquidation_long_usd=liquidation_long_usd,
        liquidation_short_usd=liquidation_short_usd,
        long_short_ratio=long_short_ratio,
        top_trader_ratio=top_trader_ratio,
        basis_pct=basis_pct,
        taker_buy_sell_imbalance=taker_buy_sell_imbalance,
        futures_volume_usd=futures_volume_usd,
        crowding_score=crowding_score,
        squeeze_score=squeeze_score,
        distribution_score=distribution_score,
        orderbook_imbalance_score=orderbook_imbalance_score,
        depth_thinness_score=depth_thinness_score,
        payload=payload,
    )


async def persist_derivatives_snapshot(snapshot: DerivativesSnapshot) -> Dict[str, Any]:
    async with async_session_maker() as session:
        existing = (
            await session.execute(
                select(AnalyticsDerivativesSnapshot).where(
                    AnalyticsDerivativesSnapshot.exchange == snapshot.exchange,
                    AnalyticsDerivativesSnapshot.symbol == snapshot.symbol,
                    AnalyticsDerivativesSnapshot.source_key == snapshot.source_key,
                )
            )
        ).scalars().first()
        row = existing or AnalyticsDerivativesSnapshot(
            exchange=snapshot.exchange,
            symbol=snapshot.symbol,
            source_key=snapshot.source_key,
        )
        if existing is None:
            session.add(row)
        row.timestamp = snapshot.timestamp.astimezone(timezone.utc).replace(tzinfo=None)
        row.source_ts = snapshot.source_ts.astimezone(timezone.utc).replace(tzinfo=None)
        row.capture_status = "ok"
        row.source_error = ""
        row.source_name = _STRUCTURED_SOURCE
        row.latency_ms = 0
        row.ingest_version = "v1"
        row.oi_usd = snapshot.oi_usd
        row.oi_change_5m = snapshot.oi_change_5m
        row.oi_change_15m = snapshot.oi_change_15m
        row.oi_change_1h = snapshot.oi_change_1h
        row.oi_change_4h = snapshot.oi_change_4h
        row.oi_change_24h = snapshot.oi_change_24h
        row.funding_rate = snapshot.funding_rate
        row.funding_rate_oi_weighted = snapshot.funding_rate_oi_weighted
        row.funding_rate_vol_weighted = snapshot.funding_rate_vol_weighted
        row.liquidation_long_usd = snapshot.liquidation_long_usd
        row.liquidation_short_usd = snapshot.liquidation_short_usd
        row.long_short_ratio = snapshot.long_short_ratio
        row.top_trader_ratio = snapshot.top_trader_ratio
        row.basis_pct = snapshot.basis_pct
        row.taker_buy_sell_imbalance = snapshot.taker_buy_sell_imbalance
        row.futures_volume_usd = snapshot.futures_volume_usd
        row.crowding_score = snapshot.crowding_score
        row.squeeze_score = snapshot.squeeze_score
        row.distribution_score = snapshot.distribution_score
        row.orderbook_imbalance_score = snapshot.orderbook_imbalance_score
        row.depth_thinness_score = snapshot.depth_thinness_score
        row.payload = dict(snapshot.payload or {})
        await session.commit()
    return snapshot.to_dict()


async def persist_market_structure_snapshot(snapshot: DerivativesSnapshot) -> None:
    async with async_session_maker() as session:
        existing = (
            await session.execute(
                select(AnalyticsMarketStructureSnapshot).where(
                    AnalyticsMarketStructureSnapshot.exchange == snapshot.exchange,
                    AnalyticsMarketStructureSnapshot.symbol == snapshot.symbol,
                    AnalyticsMarketStructureSnapshot.source_key == snapshot.source_key,
                )
            )
        ).scalars().first()
        row = existing or AnalyticsMarketStructureSnapshot(
            exchange=snapshot.exchange,
            symbol=snapshot.symbol,
            source_key=snapshot.source_key,
        )
        if existing is None:
            session.add(row)
        row.timestamp = snapshot.timestamp.astimezone(timezone.utc).replace(tzinfo=None)
        row.capture_status = "ok"
        row.source_error = ""
        row.source_name = _STRUCTURED_SOURCE
        row.latency_ms = 0
        row.ingest_version = "v1"
        row.orderbook_imbalance = snapshot.orderbook_imbalance_score
        row.heatmap_pressure_score = snapshot.squeeze_score
        row.liquidity_void_score = snapshot.depth_thinness_score
        row.trade_delta = snapshot.taker_buy_sell_imbalance
        row.payload = {
            "crowding_score": snapshot.crowding_score,
            "distribution_score": snapshot.distribution_score,
            **dict(snapshot.payload or {}),
        }
        await session.commit()


async def update_coinglass_cache(
    *,
    symbols: Optional[Sequence[str]] = None,
    datasets: Optional[Sequence[str]] = None,
    manual: bool = False,
    max_symbols_per_run: int = _DEFAULT_WORKER_SYMBOL_LIMIT,
) -> Dict[str, Any]:
    selected_symbols = [normalize_coinglass_symbol(item) for item in (symbols or []) if normalize_coinglass_symbol(item)]
    if not selected_symbols:
        selected_symbols = normalize_symbols_for_refresh(max_items=max_symbols_per_run)
    else:
        selected_symbols = selected_symbols[: max(1, int(max_symbols_per_run or len(selected_symbols)))]
    selected_datasets = [str(item or "").strip() for item in (datasets or COINGLASS_DEFAULT_DATASETS) if get_coinglass_manifest(item)]
    summary: Dict[str, Any] = {
        "enabled": coinglass_enabled(),
        "symbols": selected_symbols,
        "datasets": selected_datasets,
        "updated": [],
        "errors": [],
    }
    if not coinglass_enabled():
        return summary
    async with CoinglassClient() as client:
        for symbol in selected_symbols:
            for dataset in selected_datasets:
                manifest = get_coinglass_manifest(dataset)
                if manifest is None:
                    continue
                interval = "h4" if "history" in dataset else ""
                try:
                    result = await client.request_dataset(
                        manifest,
                        symbol=symbol,
                        exchange="Binance",
                        interval=interval or None,
                        manual=manual,
                    )
                    persist_raw_snapshot(
                        dataset=dataset,
                        route=result["route"],
                        request_meta={
                            "symbol": symbol,
                            "exchange": result["params"].get("exchange"),
                            "interval": result["params"].get("interval"),
                            "request_key": result["request_key"],
                            "latency_ms": result["latency_ms"],
                        },
                        response_payload=result["payload"],
                    )
                    frame = persist_normalized_rows(
                        dataset=dataset,
                        manifest=manifest,
                        route=result["route"],
                        request_meta={
                            "symbol": symbol,
                            "exchange": result["params"].get("exchange"),
                            "interval": result["params"].get("interval"),
                            "request_key": result["request_key"],
                            "latency_ms": result["latency_ms"],
                        },
                        response_payload=result["payload"],
                    )
                    await record_coinglass_ingest_status(
                        dataset=dataset,
                        symbol=symbol,
                        exchange=str(result["params"].get("exchange") or "aggregate"),
                        interval=str(result["params"].get("interval") or ""),
                        status="ok",
                        rows_written=len(frame.index),
                        latency_ms=int(result["latency_ms"] or 0),
                        details={
                            "request_key": result["request_key"],
                            "api_version": result["route"].api_version,
                            "path": result["route"].path,
                        },
                        manifest=manifest,
                    )
                    summary["updated"].append(
                        {
                            "dataset": dataset,
                            "symbol": symbol,
                            "rows_written": len(frame.index),
                            "latency_ms": int(result["latency_ms"] or 0),
                        }
                    )
                except CoinglassBudgetExceeded as exc:
                    await record_coinglass_ingest_status(
                        dataset=dataset,
                        symbol=symbol,
                        exchange="aggregate",
                        interval=interval,
                        status="degraded",
                        rows_written=0,
                        error=str(exc),
                        details={"reason": "budget_guard"},
                        manifest=manifest,
                    )
                    summary["errors"].append({"dataset": dataset, "symbol": symbol, "error": str(exc)})
                    break
                except Exception as exc:
                    await record_coinglass_ingest_status(
                        dataset=dataset,
                        symbol=symbol,
                        exchange="aggregate",
                        interval=interval,
                        status="failed",
                        rows_written=0,
                        error=str(exc),
                        details={"reason": "request_failed"},
                        manifest=manifest,
                    )
                    summary["errors"].append({"dataset": dataset, "symbol": symbol, "error": str(exc)})
            snapshot = build_derivatives_snapshot(symbol)
            if snapshot is not None:
                await persist_derivatives_snapshot(snapshot)
                await persist_market_structure_snapshot(snapshot)
                await record_coinglass_ingest_status(
                    dataset="derivatives",
                    symbol=symbol,
                    exchange=snapshot.exchange,
                    interval="h4",
                    status="ok",
                    rows_written=1,
                    details={"source_key": snapshot.source_key, "active_datasets": snapshot.payload.get("active_datasets", [])},
                )
    persist_symbol_registry(selected_symbols)
    summary["budget"] = (await get_coinglass_budget_state()).to_dict()
    return summary


def normalize_symbols_for_refresh(max_items: int = _DEFAULT_WORKER_SYMBOL_LIMIT) -> List[str]:
    items = []
    seen = set()
    defaults = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
    ai_universe = str(getattr(settings, "AI_AUTONOMOUS_AGENT_UNIVERSE_SYMBOLS", "") or "")
    for symbol in defaults + ai_universe.split(","):
        normalized = normalize_coinglass_symbol(symbol)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        items.append(normalized)
        if len(items) >= max(1, int(max_items or _DEFAULT_WORKER_SYMBOL_LIMIT)):
            break
    return items


async def load_latest_derivatives_snapshot(symbol: str) -> Optional[Dict[str, Any]]:
    normalized_symbol = normalize_coinglass_symbol(symbol)
    async with async_session_maker() as session:
        row = (
            await session.execute(
                select(AnalyticsDerivativesSnapshot)
                .where(AnalyticsDerivativesSnapshot.symbol == normalized_symbol)
                .order_by(AnalyticsDerivativesSnapshot.timestamp.desc())
            )
        ).scalars().first()
    if row is None:
        return None
    return {
        "timestamp": row.timestamp.replace(tzinfo=timezone.utc).isoformat() if row.timestamp else None,
        "symbol": row.symbol,
        "exchange": row.exchange,
        "source_key": row.source_key,
        "source_ts": row.source_ts.replace(tzinfo=timezone.utc).isoformat() if row.source_ts else None,
        "capture_status": row.capture_status,
        "source_error": row.source_error,
        "source_name": row.source_name,
        "latency_ms": row.latency_ms,
        "ingest_version": row.ingest_version,
        "oi_usd": row.oi_usd,
        "oi_change_5m": row.oi_change_5m,
        "oi_change_15m": row.oi_change_15m,
        "oi_change_1h": row.oi_change_1h,
        "oi_change_4h": row.oi_change_4h,
        "oi_change_24h": row.oi_change_24h,
        "funding_rate": row.funding_rate,
        "funding_rate_oi_weighted": row.funding_rate_oi_weighted,
        "funding_rate_vol_weighted": row.funding_rate_vol_weighted,
        "liquidation_long_usd": row.liquidation_long_usd,
        "liquidation_short_usd": row.liquidation_short_usd,
        "long_short_ratio": row.long_short_ratio,
        "top_trader_ratio": row.top_trader_ratio,
        "basis_pct": row.basis_pct,
        "taker_buy_sell_imbalance": row.taker_buy_sell_imbalance,
        "futures_volume_usd": row.futures_volume_usd,
        "crowding_score": row.crowding_score,
        "squeeze_score": row.squeeze_score,
        "distribution_score": row.distribution_score,
        "orderbook_imbalance_score": row.orderbook_imbalance_score,
        "depth_thinness_score": row.depth_thinness_score,
        "payload": dict(row.payload or {}),
    }


async def load_coinglass_status_snapshot() -> Dict[str, Any]:
    statuses = await load_coinglass_ingest_statuses()
    budget = await get_coinglass_budget_state()
    active = [row for row in statuses if str(row.get("status") or "") == "ok" and int(row.get("rows_written") or 0) > 0]
    latest_times = [
        pd.Timestamp(row.get("last_success_at")).to_pydatetime().astimezone(timezone.utc)
        for row in active
        if row.get("last_success_at")
    ]
    freshness_sec = None
    if latest_times:
        freshness_sec = max(0.0, (_utc_now() - max(latest_times)).total_seconds())
    return {
        "available": bool(active),
        "freshness_sec": freshness_sec,
        "active_datasets": sorted({str(row.get("dataset") or "") for row in active}),
        "status": statuses,
        "quota_headroom": {
            "minute_remaining": budget.minute_remaining,
            "daily_remaining": budget.daily_remaining,
            "monthly_remaining": budget.monthly_remaining,
        },
        "budget": budget.to_dict(),
        "key_configured": budget.key_configured,
    }


async def build_coinglass_overview_payload(
    symbol: str = _DEFAULT_OVERVIEW_SYMBOL,
    *,
    refresh: bool = False,
    manual: bool = False,
) -> Dict[str, Any]:
    normalized_symbol = normalize_coinglass_symbol(symbol or _DEFAULT_OVERVIEW_SYMBOL)
    if refresh and coinglass_enabled():
        await update_coinglass_cache(
            symbols=[normalized_symbol],
            manual=manual,
            max_symbols_per_run=1,
        )
    snapshot = await load_latest_derivatives_snapshot(normalized_symbol)
    statuses = await load_coinglass_ingest_statuses(symbol=normalized_symbol)
    budget = await get_coinglass_budget_state()
    active_datasets = [str(row.get("dataset") or "") for row in statuses if str(row.get("status") or "") == "ok"]
    freshness_sec = None
    degraded_reason = None
    if snapshot and snapshot.get("timestamp"):
        freshness_sec = max(0.0, (_utc_now() - pd.Timestamp(snapshot["timestamp"]).to_pydatetime().astimezone(timezone.utc)).total_seconds())
        if freshness_sec > 1800:
            degraded_reason = "stale_derivatives_snapshot"
    elif coinglass_enabled():
        degraded_reason = "coinglass_cache_empty"
    payload = CoinglassOverviewPayload(
        symbol=normalized_symbol,
        available=bool(snapshot),
        freshness_sec=freshness_sec,
        degraded_reason=degraded_reason,
        quota_headroom={
            "minute_remaining": budget.minute_remaining,
            "daily_remaining": budget.daily_remaining,
            "monthly_remaining": budget.monthly_remaining,
        },
        active_datasets=sorted(set(active_datasets)),
        status=statuses,
        snapshot=snapshot,
        generated_at=_utc_now().isoformat(),
        cached=bool(snapshot),
        refreshing=False,
        key_configured=budget.key_configured,
    )
    return payload.to_dict()


def build_coinglass_runtime_context(snapshot: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    payload = dict((snapshot or {}).get("payload") or {})
    return {
        "market_regime": payload.get("market_regime"),
        "funding_regime": payload.get("funding_regime"),
        "oi_regime": payload.get("oi_regime"),
        "liquidation_state": payload.get("liquidation_state"),
        "orderbook_state": payload.get("orderbook_state"),
        "crowding_warning": bool(payload.get("crowding_warning")),
        "crowding_score": snapshot.get("crowding_score") if isinstance(snapshot, Mapping) else None,
        "squeeze_score": snapshot.get("squeeze_score") if isinstance(snapshot, Mapping) else None,
        "distribution_score": snapshot.get("distribution_score") if isinstance(snapshot, Mapping) else None,
    }
