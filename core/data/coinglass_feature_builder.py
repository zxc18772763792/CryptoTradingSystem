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
    coinglass_minute_headroom,
    coinglass_enabled,
    get_coinglass_budget_state,
    load_coinglass_ingest_statuses,
    load_dataset_rows_for_symbol,
    normalize_dataset_response,
    persist_normalized_rows,
    persist_raw_snapshot,
    persist_symbol_registry,
    record_coinglass_ingest_status,
    should_pause_coinglass_requests,
)
from core.data.coinglass_registry import (
    COINGLASS_DEFAULT_DATASETS,
    COINGLASS_SUPPORTED_DATASETS,
    coinglass_symbol_matches,
    normalize_coinglass_exchange,
    get_coinglass_manifest,
    normalize_coinglass_symbol,
)


_DEFAULT_WORKER_SYMBOL_LIMIT = 3
_DEFAULT_OVERVIEW_SYMBOL = "BTC/USDT"
_STRUCTURED_SOURCE = "coinglass_proxy"
_NON_MANUAL_DATASET_RESERVE = 3
# Where the next budget-limited background sweep starts in COINGLASS_DEFAULT_DATASETS.
_NON_MANUAL_DATASET_CURSOR = 0


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
    latest_request_key = ""
    # Rank by upstream source_ts, falling back to a row's own ingest time when the
    # dataset does not carry one. Several datasets only populate source_ts on error
    # responses, and treating a missing source_ts as "oldest possible" let a
    # months-old error payload shadow the batch fetched minutes ago.
    ranked = None
    source_ts = (
        pd.to_datetime(frame["source_ts"], utc=True, errors="coerce")
        if "source_ts" in frame.columns
        else None
    )
    ingested = (
        pd.to_datetime(frame["ingested_at"], utc=True, errors="coerce")
        if "ingested_at" in frame.columns
        else None
    )
    if source_ts is not None and ingested is not None:
        ranked = source_ts.fillna(ingested)
    else:
        ranked = source_ts if source_ts is not None else ingested
    if ranked is not None and not ranked.dropna().empty:
        latest_idx = ranked.fillna(pd.Timestamp.min.tz_localize("UTC")).idxmax()
        latest_request_key = str(frame.loc[latest_idx].get("request_key") or "")
    if not latest_request_key:
        latest_request_key = str(frame.iloc[-1].get("request_key") or "")
    if latest_request_key and "request_key" in frame.columns:
        frame = frame[frame["request_key"].astype(str) == latest_request_key]
    rows: List[Dict[str, Any]] = []
    for _, row in frame.iterrows():
        payload = _parse_payload_json(row.get("payload_json"))
        if not payload:
            continue
        payload_symbol = payload.get("symbol")
        if payload_symbol and not coinglass_symbol_matches(symbol, payload_symbol):
            continue
        payload["_source_ts"] = str(row.get("source_ts") or "")
        payload["_exchange"] = str(row.get("exchange") or "aggregate")
        payload["_interval"] = str(row.get("interval") or "")
        rows.append(payload)
    return rows


def _dataset_payloads(symbol: str) -> Dict[str, Any]:
    return {
        dataset: _latest_payload_rows(dataset, symbol)
        for dataset in COINGLASS_SUPPORTED_DATASETS
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


def _weighted_mean(pairs: Iterable[tuple[Optional[float], Optional[float]]]) -> Optional[float]:
    weighted_total = 0.0
    weight_total = 0.0
    for value, weight in pairs:
        if value is None or weight is None or weight <= 0:
            continue
        weighted_total += float(value) * float(weight)
        weight_total += float(weight)
    if weight_total <= 0:
        return None
    return weighted_total / weight_total


def _aggregate_exchange_row(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    for row in rows:
        exchange = str(row.get("exchange") or row.get("_exchange") or "").strip().lower()
        if exchange in {"all", "aggregate"}:
            return dict(row)
    return dict(rows[-1]) if rows else {}


def _interval_seconds(interval: Any) -> int:
    normalized = str(interval or "").strip().lower()
    mapping = {
        "1m": 60,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "1h": 3600,
        "h1": 3600,
        "4h": 14400,
        "h4": 14400,
        "12h": 43200,
        "h12": 43200,
        "24h": 86400,
        "h24": 86400,
        "1d": 86400,
    }
    return int(mapping.get(normalized, 3600))


def _history_value_series(rows: Sequence[Mapping[str, Any]], *candidates: str) -> List[tuple[datetime, float]]:
    series: List[tuple[datetime, float]] = []
    for row in rows:
        source_ts = row.get("_source_ts") or row.get("source_ts") or row.get("time") or row.get("timestamp")
        if not source_ts:
            continue
        try:
            ts = pd.Timestamp(source_ts).to_pydatetime().astimezone(timezone.utc)
        except Exception:
            continue
        value = _coalesce_float(row, *candidates)
        if value is None:
            continue
        series.append((ts, value))
    series.sort(key=lambda item: item[0])
    return series


def _series_change_pct(series: Sequence[tuple[datetime, float]], lookback_sec: int) -> Optional[float]:
    if len(series) < 2 or lookback_sec <= 0:
        return None
    latest_ts, latest_value = series[-1]
    target_ts = latest_ts - pd.Timedelta(seconds=int(lookback_sec)).to_pytimedelta()
    baseline_value = None
    for ts, value in reversed(series[:-1]):
        if ts <= target_ts:
            baseline_value = value
            break
    if baseline_value in (None, 0):
        return None
    return (float(latest_value) / float(baseline_value)) - 1.0


def _tail_values(series: Sequence[tuple[datetime, float]], count: int) -> List[float]:
    if count <= 0:
        return []
    return [float(value) for _, value in list(series)[-count:]]


def _series_mean(series: Sequence[tuple[datetime, float]], count: int) -> Optional[float]:
    values = _tail_values(series, count)
    if not values:
        return None
    return float(sum(values) / len(values))


def _series_zscore(series: Sequence[tuple[datetime, float]], count: int) -> Optional[float]:
    values = _tail_values(series, count)
    if len(values) < 2:
        return None
    sample = pd.Series(values, dtype=float)
    std = float(sample.std(ddof=0) or 0.0)
    if std <= 0:
        return None
    latest = float(values[-1])
    mean = float(sample.mean())
    return (latest - mean) / std


def _series_reversion_speed(series: Sequence[tuple[datetime, float]]) -> Optional[float]:
    if len(series) < 2:
        return None
    latest = float(series[-1][1])
    previous = float(series[-2][1])
    denominator = max(abs(previous), 1e-9)
    return abs(latest - previous) / denominator


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
    oi_history_rows = list(payloads.get("open_interest_history") or [])
    oi_aggregated_history_rows = list(payloads.get("open_interest_aggregated_history") or [])
    oi_stablecoin_history_rows = list(payloads.get("open_interest_stablecoin_margin_history") or [])
    funding_history_rows = list(payloads.get("funding_rate_history") or [])
    taker_rows = list(payloads.get("taker_buy_sell_volume_exchange_list") or [])
    taker_history_rows = list(payloads.get("taker_buy_sell_volume_history") or [])
    liquidation_rows = list(payloads.get("liquidation_history") or [])
    liquidation_aggregated_rows = list(payloads.get("liquidation_aggregated_history") or [])
    liquidation_map_rows = list(payloads.get("liquidation_aggregated_map") or [])
    liquidation_heatmap_rows = list(payloads.get("liquidation_aggregated_heatmap_model1") or [])
    ratio_rows = list(payloads.get("global_long_short_account_ratio_history") or [])
    top_account_ratio_rows = list(payloads.get("top_long_short_account_ratio_history") or [])
    top_position_ratio_rows = list(payloads.get("top_long_short_position_ratio_history") or [])
    net_position_rows = list(payloads.get("net_position_history") or [])
    futures_orderbook_rows = list(payloads.get("futures_orderbook_aggregated_ask_bids_history") or [])
    spot_netflow_rows = list(payloads.get("spot_coin_netflow") or [])
    exchange_balance_rows = list(payloads.get("exchange_balance_list") or [])
    exchange_balance_chart_rows = list(payloads.get("exchange_balance_chart") or [])
    coinbase_premium_rows = list(payloads.get("coinbase_premium_index") or [])
    option_max_pain_rows = list(payloads.get("option_max_pain") or [])
    options_info_rows = list(payloads.get("options_info") or [])
    options_oi_rows = list(payloads.get("options_exchange_oi_history") or [])
    options_volume_rows = list(payloads.get("options_exchange_volume_history") or [])
    option_futures_ratio_rows = list(payloads.get("option_vs_futures_oi_ratio") or [])
    bitcoin_etf_flow_rows = list(payloads.get("bitcoin_etf_flow_history") or [])
    arbitrage_rows = list(payloads.get("funding_arbitrage") or [])
    liquidation_row = liquidation_rows[-1] if liquidation_rows else {}
    liquidation_aggregated_row = liquidation_aggregated_rows[-1] if liquidation_aggregated_rows else {}
    ratio_row = ratio_rows[-1] if ratio_rows else {}
    top_account_ratio_row = top_account_ratio_rows[-1] if top_account_ratio_rows else {}
    top_position_ratio_row = top_position_ratio_rows[-1] if top_position_ratio_rows else {}
    net_position_row = net_position_rows[-1] if net_position_rows else {}
    arbitrage_row = arbitrage_rows[-1] if arbitrage_rows else {}
    if not any(payloads.values()):
        return None

    aggregate_oi_row = _aggregate_exchange_row(oi_rows)
    non_aggregate_oi_rows = [
        item
        for item in oi_rows
        if str(item.get("exchange") or item.get("_exchange") or "").strip().lower() not in {"all", "aggregate"}
    ]
    oi_usd = _coalesce_float(aggregate_oi_row, "open_interest_usd", "openInterestUsd", "oivalue")
    if oi_usd is None:
        oi_usd = _sum(_coalesce_float(item, "open_interest_usd", "openInterestUsd", "oivalue") for item in non_aggregate_oi_rows)
    oi_change_5m = _coalesce_float(
        aggregate_oi_row,
        "open_interest_change_5m",
        "open_interest_change_percent_5m",
        "change5m",
        "oiChange5m",
    )
    if oi_change_5m is None:
        oi_change_5m = _mean(_coalesce_float(item, "open_interest_change_5m", "open_interest_change_percent_5m", "change5m", "oiChange5m") for item in non_aggregate_oi_rows)
    oi_change_15m = _coalesce_float(
        aggregate_oi_row,
        "open_interest_change_15m",
        "open_interest_change_percent_15m",
        "change15m",
        "oiChange15m",
    )
    if oi_change_15m is None:
        oi_change_15m = _mean(_coalesce_float(item, "open_interest_change_15m", "open_interest_change_percent_15m", "change15m", "oiChange15m") for item in non_aggregate_oi_rows)
    oi_change_1h = _coalesce_float(
        aggregate_oi_row,
        "open_interest_change_1h",
        "open_interest_change_percent_1h",
        "change1h",
        "oiChange1h",
    )
    if oi_change_1h is None:
        oi_change_1h = _mean(_coalesce_float(item, "open_interest_change_1h", "open_interest_change_percent_1h", "change1h", "oiChange1h") for item in non_aggregate_oi_rows)
    oi_change_4h = _coalesce_float(
        aggregate_oi_row,
        "open_interest_change_4h",
        "open_interest_change_percent_4h",
        "change4h",
        "oiChange4h",
    )
    if oi_change_4h is None:
        oi_change_4h = _mean(_coalesce_float(item, "open_interest_change_4h", "open_interest_change_percent_4h", "change4h", "oiChange4h") for item in non_aggregate_oi_rows)
    oi_change_24h = _coalesce_float(
        aggregate_oi_row,
        "open_interest_change_24h",
        "open_interest_change_percent_24h",
        "change24h",
        "oiChange24h",
    )
    if oi_change_24h is None:
        oi_change_24h = _mean(_coalesce_float(item, "open_interest_change_24h", "open_interest_change_percent_24h", "change24h", "oiChange24h") for item in non_aggregate_oi_rows)

    selected_oi_history_rows = oi_history_rows or oi_aggregated_history_rows or oi_stablecoin_history_rows
    oi_history_series = _history_value_series(
        selected_oi_history_rows,
        "open_interest_close",
        "open_interest_usd",
        "close",
        "c",
    )
    history_interval = (
        str((selected_oi_history_rows[-1] if selected_oi_history_rows else {}).get("_interval") or "")
        or str((funding_history_rows[-1] if funding_history_rows else {}).get("_interval") or "")
        or str((ratio_rows[-1] if ratio_rows else {}).get("_interval") or "")
        or "h1"
    )
    oi_change_1h_history = _series_change_pct(oi_history_series, 3600)
    oi_change_4h_history = _series_change_pct(oi_history_series, 4 * 3600)
    oi_change_24h_history = _series_change_pct(oi_history_series, 24 * 3600)
    if oi_change_1h_history is not None:
        oi_change_1h = oi_change_1h_history * 100.0
    if oi_change_4h_history is not None:
        oi_change_4h = oi_change_4h_history * 100.0
    if oi_change_24h_history is not None:
        oi_change_24h = oi_change_24h_history * 100.0

    stablecoin_funding_rows = [item for item in funding_rows if str(item.get("margin_type") or "").strip().lower() == "stablecoin"]
    token_funding_rows = [item for item in funding_rows if str(item.get("margin_type") or "").strip().lower() == "token"]
    primary_funding_rows = stablecoin_funding_rows or funding_rows
    funding_rate = _mean(_coalesce_float(item, "funding_rate", "fundingRate") for item in primary_funding_rows)
    funding_rate_oi_weighted = _weighted_mean(
        (
            _coalesce_float(item, "funding_rate", "fundingRate"),
            _coalesce_float(item, "open_interest_usd", "openInterestUsd", "oi_usd", "oiUsd"),
        )
        for item in primary_funding_rows
    )
    funding_rate_vol_weighted = _weighted_mean(
        (
            _coalesce_float(item, "funding_rate", "fundingRate"),
            _coalesce_float(item, "volume_usd", "turnover_usd", "notionalUsd"),
        )
        for item in primary_funding_rows
    )
    if funding_rate_oi_weighted is None:
        funding_rate_oi_weighted = _coalesce_float(
            arbitrage_row,
            "oi_weighted_funding_rate",
            "oiWeightedFundingRate",
        )
    if funding_rate_vol_weighted is None:
        funding_rate_vol_weighted = _coalesce_float(
            arbitrage_row,
            "volume_weighted_funding_rate",
            "volWeightedFundingRate",
        )
    if funding_rate_oi_weighted is None:
        funding_rate_oi_weighted = funding_rate
    if funding_rate_vol_weighted is None:
        funding_rate_vol_weighted = funding_rate

    funding_history_series = _history_value_series(
        funding_history_rows,
        "funding_rate_close",
        "funding_rate",
        "close",
        "c",
    )
    history_interval_sec = _interval_seconds(history_interval)
    bars_24h = max(2, int(round(86400 / max(history_interval_sec, 60))))
    funding_mean = _series_mean(funding_history_series, bars_24h)
    funding_zscore = _series_zscore(funding_history_series, max(bars_24h, bars_24h * 3))
    funding_reversion_speed = _series_reversion_speed(funding_history_series)
    if funding_mean is not None:
        funding_rate_vol_weighted = funding_rate_vol_weighted if funding_rate_vol_weighted is not None else funding_mean

    liquidation_long_usd = _coalesce_float(liquidation_row, "long_liquidation_usd", "longLiquidationUsd", "longVolUsd")
    liquidation_short_usd = _coalesce_float(liquidation_row, "short_liquidation_usd", "shortLiquidationUsd", "shortVolUsd")
    if liquidation_long_usd is None:
        liquidation_long_usd = _coalesce_float(liquidation_aggregated_row, "long_liquidation_usd", "longLiquidationUsd", "longVolUsd")
    if liquidation_short_usd is None:
        liquidation_short_usd = _coalesce_float(liquidation_aggregated_row, "short_liquidation_usd", "shortLiquidationUsd", "shortVolUsd")
    long_short_ratio = _coalesce_float(
        ratio_row,
        "long_short_ratio",
        "longShortRatio",
        "global_account_long_short_ratio",
        "globalAccountLongShortRatio",
        "ratio",
    )
    ratio_series = _history_value_series(
        ratio_rows,
        "long_short_ratio",
        "longShortRatio",
        "global_account_long_short_ratio",
        "globalAccountLongShortRatio",
        "ratio",
    )
    long_short_ratio_change = _series_change_pct(ratio_series, 24 * 3600)
    top_account_ratio = _coalesce_float(
        top_account_ratio_row,
        "long_short_ratio",
        "longShortRatio",
        "top_trader_ratio",
        "topTraderRatio",
        "ratio",
    )
    top_position_ratio = _coalesce_float(
        top_position_ratio_row,
        "long_short_ratio",
        "longShortRatio",
        "top_position_ratio",
        "topPositionRatio",
        "ratio",
    )
    top_trader_ratio = top_position_ratio or top_account_ratio or _coalesce_float(ratio_row, "top_trader_ratio", "topTraderRatio")
    net_position = _coalesce_float(
        net_position_row,
        "net_position",
        "netPosition",
        "net_long_short_position",
        "netLongShortPosition",
        "value",
        "close",
    )

    taker_buy = _sum(_coalesce_float(item, "taker_buy_volume", "buyVolume", "buy") for item in taker_rows)
    taker_sell = _sum(_coalesce_float(item, "taker_sell_volume", "sellVolume", "sell") for item in taker_rows)
    taker_buy_sell_imbalance = None
    if taker_buy is not None and taker_sell is not None and (taker_buy + taker_sell) > 0:
        taker_buy_sell_imbalance = (taker_buy - taker_sell) / (taker_buy + taker_sell)

    taker_history_series = _history_value_series(
        taker_history_rows,
        "taker_buy_sell_imbalance",
    )
    bars_1h = max(1, int(round(3600 / max(history_interval_sec, 60))))
    bars_4h = max(1, int(round(4 * 3600 / max(history_interval_sec, 60))))
    taker_imbalance_1h = _series_mean(taker_history_series, bars_1h)
    taker_imbalance_4h = _series_mean(taker_history_series, bars_4h)

    basis_pct = _coalesce_float(arbitrage_row, "basis_pct", "basisPercent", "basis")
    coinbase_premium = _coalesce_float(
        coinbase_premium_rows[-1] if coinbase_premium_rows else {},
        "premium",
        "premium_index",
        "premiumIndex",
        "coinbase_premium_index",
        "close",
        "c",
    )
    option_max_pain = _coalesce_float(
        option_max_pain_rows[-1] if option_max_pain_rows else {},
        "option_max_pain",
        "max_pain",
        "maxPain",
        "max_pain_price",
        "maxPainPrice",
        "price",
    )
    bitcoin_etf_net_flow = _coalesce_float(
        bitcoin_etf_flow_rows[-1] if bitcoin_etf_flow_rows else {},
        "net_flow",
        "netFlow",
        "flow",
        "total_net_inflow",
        "totalNetInflow",
        "value",
    )
    spot_exchange_inflow_usd = _sum(
        _coalesce_float(item, "spot_exchange_inflow_usd", "inflow_usd", "inflow")
        for item in spot_netflow_rows
    )
    spot_exchange_outflow_usd = _sum(
        _coalesce_float(item, "spot_exchange_outflow_usd", "outflow_usd", "outflow")
        for item in spot_netflow_rows
    )
    spot_exchange_netflow_usd = _sum(
        _coalesce_float(item, "spot_exchange_netflow_usd", "netflow_usd", "netflow")
        for item in spot_netflow_rows
    )
    if (
        spot_exchange_netflow_usd is None
        and spot_exchange_inflow_usd is not None
        and spot_exchange_outflow_usd is not None
    ):
        spot_exchange_netflow_usd = spot_exchange_inflow_usd - spot_exchange_outflow_usd
    spot_exchange_inflow_count = int(
        _sum(_coalesce_float(item, "spot_exchange_inflow_count", "inflow_count") for item in spot_netflow_rows)
        or 0
    )
    spot_exchange_outflow_count = int(
        _sum(_coalesce_float(item, "spot_exchange_outflow_count", "outflow_count") for item in spot_netflow_rows)
        or 0
    )
    spot_netflow_score = None
    flow_gross = (spot_exchange_inflow_usd or 0.0) + (spot_exchange_outflow_usd or 0.0)
    if flow_gross > 0 and spot_exchange_netflow_usd is not None:
        spot_netflow_score = max(-1.0, min(1.0, spot_exchange_netflow_usd / flow_gross))
    exchange_flow_pressure = (
        "inflow_sell_pressure"
        if (spot_exchange_netflow_usd or 0.0) > max(flow_gross * 0.1, 0.0)
        else "outflow_supply_tight"
        if (spot_exchange_netflow_usd or 0.0) < -max(flow_gross * 0.1, 0.0)
        else "balanced"
    )
    exchange_balance_source_rows = exchange_balance_rows or exchange_balance_chart_rows
    exchange_balance_btc = _sum(
        _coalesce_float(item, "exchange_balance_btc", "balance_btc", "balance")
        for item in exchange_balance_source_rows
    )
    exchange_balance_usd = _sum(
        _coalesce_float(item, "exchange_balance_usd", "balance_usd", "balanceUsd", "value_usd")
        for item in exchange_balance_source_rows
    )
    exchange_balance_change_24h = _mean(
        _coalesce_float(item, "exchange_balance_change_24h", "change_24h", "change24h")
        for item in exchange_balance_source_rows
    )
    exchange_balance_change_7d = _mean(
        _coalesce_float(item, "exchange_balance_change_7d", "change_7d", "change7d")
        for item in exchange_balance_source_rows
    )
    stablecoin_exchange_balance_usd = _sum(
        _coalesce_float(item, "stablecoin_exchange_balance_usd", "stablecoin_balance_usd", "stablecoinBalanceUsd")
        for item in exchange_balance_source_rows
    )
    stablecoin_exchange_balance_change_24h = _mean(
        _coalesce_float(item, "stablecoin_exchange_balance_change_24h", "stablecoin_change_24h", "stablecoinChange24h")
        for item in exchange_balance_source_rows
    )
    stablecoin_netflow_usd = _sum(
        _coalesce_float(item, "stablecoin_netflow_usd", "stablecoinNetflowUsd")
        for item in exchange_balance_source_rows
    )
    if stablecoin_netflow_usd is None:
        stablecoin_netflow_usd = stablecoin_exchange_balance_change_24h
    onchain_activity_score = _clamp01(
        max(
            _scale_percent(flow_gross, 250_000_000.0),
            _scale_percent(exchange_balance_usd, 25_000_000_000.0),
        )
    )
    exchange_reserve_pressure_score = _clamp01(
        max(
            _scale_percent(exchange_balance_change_24h, 5.0),
            _scale_percent(spot_exchange_netflow_usd, 250_000_000.0),
        )
    )
    option_info_row = options_info_rows[-1] if options_info_rows else {}
    option_oi_row = options_oi_rows[-1] if options_oi_rows else {}
    option_volume_row = options_volume_rows[-1] if options_volume_rows else {}
    option_ratio_row = option_futures_ratio_rows[-1] if option_futures_ratio_rows else {}
    option_put_call_ratio = _coalesce_float(
        option_info_row,
        "option_put_call_ratio",
        "put_call_ratio",
        "putCallRatio",
    )
    option_open_interest_usd = _coalesce_float(
        option_info_row, "option_open_interest_usd", "open_interest_usd", "openInterestUsd", "oiUsd"
    )
    if option_open_interest_usd is None:
        option_open_interest_usd = _coalesce_float(
            option_oi_row, "option_open_interest_usd", "open_interest_usd", "openInterestUsd", "oiUsd", "close", "c"
        )
    option_volume_usd = _coalesce_float(
        option_info_row, "option_volume_usd", "volume_usd", "volumeUsd", "volUsd"
    )
    if option_volume_usd is None:
        option_volume_usd = _coalesce_float(
            option_volume_row, "option_volume_usd", "volume_usd", "volumeUsd", "volUsd", "close", "c"
        )
    option_iv = _coalesce_float(option_info_row, "option_iv", "iv", "atm_iv", "implied_volatility")
    option_iv_skew = _coalesce_float(option_info_row, "option_iv_skew", "iv_skew", "skew", "skew_25d")
    gamma_exposure = _coalesce_float(option_info_row, "gamma_exposure", "gammaExposure", "gex")
    option_vs_futures_oi_ratio = _coalesce_float(
        option_ratio_row,
        "option_vs_futures_oi_ratio",
        "ratio",
        "value",
        "close",
        "c",
    )
    option_distance_to_max_pain_pct = None
    futures_volume_usd = _sum(_coalesce_float(item, "volume_usd", "turnover_usd", "notionalUsd") for item in taker_rows)
    if futures_volume_usd is None and taker_buy is not None and taker_sell is not None:
        futures_volume_usd = taker_buy + taker_sell

    funding_extreme_deviation = None
    funding_rates = [
        _coalesce_float(item, "funding_rate", "fundingRate")
        for item in primary_funding_rows
        if _coalesce_float(item, "funding_rate", "fundingRate") is not None
    ]
    if funding_rates and funding_rate is not None:
        funding_extreme_deviation = max(abs(rate - funding_rate) for rate in funding_rates)

    liquidation_burst_score = _coalesce_float(liquidation_row, "burst_score")
    if liquidation_burst_score is None:
        liquidation_burst_score = _coalesce_float(liquidation_aggregated_row, "burst_score")
    if liquidation_burst_score is None:
        liquidation_burst_score = _clamp01(
            _scale_percent((liquidation_long_usd or 0.0) + (liquidation_short_usd or 0.0), 50_000_000.0)
        )
    liquidation_map_total_usd = _sum(
        _coalesce_float(item, "liquidation_map_total_usd") for item in liquidation_map_rows
    )
    liquidation_map_above_usd = _sum(
        _coalesce_float(item, "liquidation_map_above_usd") for item in liquidation_map_rows
    )
    liquidation_map_below_usd = _sum(
        _coalesce_float(item, "liquidation_map_below_usd") for item in liquidation_map_rows
    )
    liquidation_map_pressure_score = _clamp01(
        _scale_percent(liquidation_map_total_usd, 750_000_000.0)
    )
    liquidation_map_largest_row = max(
        liquidation_map_rows,
        key=lambda item: _coalesce_float(item, "liquidation_map_largest_cluster_usd") or 0.0,
        default={},
    )
    liquidity_map_rows = [*liquidation_map_rows, *liquidation_heatmap_rows]
    liquidity_heatmap_total_usd = _sum(
        _coalesce_float(item, "liquidity_heatmap_total_usd", "liquidation_map_total_usd")
        for item in liquidity_map_rows
    )
    liquidity_heatmap_above_usd = _sum(
        _coalesce_float(item, "liquidity_heatmap_above_usd", "liquidation_map_above_usd")
        for item in liquidity_map_rows
    )
    liquidity_heatmap_below_usd = _sum(
        _coalesce_float(item, "liquidity_heatmap_below_usd", "liquidation_map_below_usd")
        for item in liquidity_map_rows
    )
    heatmap_pressure_score = _mean(
        _coalesce_float(item, "heatmap_pressure_score", "liquidation_map_pressure_score")
        for item in liquidity_map_rows
    )
    if heatmap_pressure_score is None:
        heatmap_pressure_score = _scale_percent(liquidity_heatmap_total_usd, 750_000_000.0)
    heatmap_pressure_score = _clamp01(heatmap_pressure_score)
    liquidity_above_row = max(
        liquidity_map_rows,
        key=lambda item: _coalesce_float(item, "liquidity_wall_nearest_above_usd", "liquidation_map_nearest_above_usd") or 0.0,
        default={},
    )
    liquidity_below_row = max(
        liquidity_map_rows,
        key=lambda item: _coalesce_float(item, "liquidity_wall_nearest_below_usd", "liquidation_map_nearest_below_usd") or 0.0,
        default={},
    )
    liquidity_wall_above_usd = _coalesce_float(
        liquidity_above_row, "liquidity_wall_nearest_above_usd", "liquidation_map_nearest_above_usd"
    )
    liquidity_wall_below_usd = _coalesce_float(
        liquidity_below_row, "liquidity_wall_nearest_below_usd", "liquidation_map_nearest_below_usd"
    )
    liquidity_void_score = _mean(
        _coalesce_float(item, "liquidity_void_score") for item in liquidity_map_rows
    )
    if liquidity_void_score is None and liquidity_heatmap_total_usd:
        nearby_total = (liquidity_wall_above_usd or 0.0) + (liquidity_wall_below_usd or 0.0)
        liquidity_void_score = 1.0 - (nearby_total / max(liquidity_heatmap_total_usd, 1.0))
    liquidity_void_score = _clamp01(liquidity_void_score)

    oi_funding_divergence_score = 0.0
    if oi_change_24h is not None and funding_zscore is not None and (oi_change_24h * funding_zscore) < 0:
        oi_funding_divergence_score = _clamp01(
            _scale_percent(oi_change_24h, 12.0) * 0.55
            + _scale_percent(funding_zscore, 2.5) * 0.45
        ) or 0.0

    basis_dislocation_score = _clamp01(
        max(
            _scale_percent(basis_pct, 0.03),
            _scale_percent(funding_extreme_deviation, 0.004),
            _scale_percent(funding_zscore, 2.5),
        )
    )
    flow_divergence_score = 0.0
    if taker_buy_sell_imbalance is not None and oi_change_1h is not None and (taker_buy_sell_imbalance * oi_change_1h) < 0:
        flow_divergence_score = _clamp01(
            _scale_percent(taker_buy_sell_imbalance, 0.20) * 0.55
            + _scale_percent(oi_change_1h, 5.0) * 0.45
        ) or 0.0

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
    futures_orderbook_row = futures_orderbook_rows[-1] if futures_orderbook_rows else {}
    orderbook_agg_bid_usd = _coalesce_float(
        futures_orderbook_row,
        "orderbook_agg_bid_usd",
        "bid_usd",
        "bidUsd",
    )
    orderbook_agg_ask_usd = _coalesce_float(
        futures_orderbook_row,
        "orderbook_agg_ask_usd",
        "ask_usd",
        "askUsd",
    )
    orderbook_agg_imbalance = _coalesce_float(
        futures_orderbook_row,
        "orderbook_agg_imbalance",
        "orderbook_imbalance",
        "bid_ask_imbalance",
        "imbalance",
        "ask_bid_ratio",
        "bidAskRatio",
    )
    if (
        orderbook_agg_imbalance is None
        and orderbook_agg_bid_usd is not None
        and orderbook_agg_ask_usd is not None
        and (orderbook_agg_bid_usd + orderbook_agg_ask_usd) > 0
    ):
        orderbook_agg_imbalance = (
            (orderbook_agg_bid_usd - orderbook_agg_ask_usd)
            / (orderbook_agg_bid_usd + orderbook_agg_ask_usd)
        )
    if orderbook_agg_imbalance is not None:
        orderbook_imbalance_score = _clamp01(
            0.5 + (float(orderbook_agg_imbalance) * 0.5)
            if -1.0 <= float(orderbook_agg_imbalance) <= 1.0
            else _scale_percent(orderbook_agg_imbalance, 2.0)
        )
    orderbook_wall_above_usd = _coalesce_float(
        futures_orderbook_row, "orderbook_wall_above_usd", "ask_wall_usd", "askWallUsd"
    )
    orderbook_wall_below_usd = _coalesce_float(
        futures_orderbook_row, "orderbook_wall_below_usd", "bid_wall_usd", "bidWallUsd"
    )
    orderbook_wall_above_price = _coalesce_float(
        futures_orderbook_row, "orderbook_wall_above_price", "ask_wall_price", "askWallPrice"
    )
    orderbook_wall_below_price = _coalesce_float(
        futures_orderbook_row, "orderbook_wall_below_price", "bid_wall_price", "bidWallPrice"
    )
    spot_reference_price = _coalesce_float(liquidation_map_largest_row, "last_price") or _coalesce_float(
        futures_orderbook_row, "last_price", "price"
    )
    if option_max_pain not in (None, 0) and spot_reference_price:
        option_distance_to_max_pain_pct = (
            (spot_reference_price - option_max_pain) / option_max_pain
        ) * 100.0
    depth_thinness_score = _clamp01(
        1.0 - _scale_percent(futures_volume_usd, 250_000_000.0)
    )
    if orderbook_agg_bid_usd is not None or orderbook_agg_ask_usd is not None:
        orderbook_total_usd = (orderbook_agg_bid_usd or 0.0) + (orderbook_agg_ask_usd or 0.0)
        depth_thinness_score = _clamp01(
            max(depth_thinness_score or 0.0, 1.0 - _scale_percent(orderbook_total_usd, 200_000_000.0))
        )
    if liquidity_void_score is not None:
        depth_thinness_score = _clamp01(max(depth_thinness_score or 0.0, liquidity_void_score * 0.65))
    derivatives_heat_score = _clamp01(
        max(
            crowding_score or 0.0,
            squeeze_score or 0.0,
            distribution_score or 0.0,
            heatmap_pressure_score or 0.0,
        )
    )

    crowded_long = bool((crowding_score or 0.0) >= 0.70 and (funding_rate or 0.0) > 0 and (long_short_ratio or 1.0) >= 1.05)
    crowded_short = bool((crowding_score or 0.0) >= 0.70 and (funding_rate or 0.0) < 0 and (long_short_ratio or 1.0) <= 0.95)
    squeeze_building = bool(
        (squeeze_score or 0.0) >= 0.62
        and (taker_buy_sell_imbalance or 0.0) > 0
        and (oi_change_1h or 0.0) > 0
        and (liquidation_short_usd or 0.0) >= (liquidation_long_usd or 0.0)
    )
    flush_risk = bool(
        (distribution_score or 0.0) >= 0.62
        and (taker_buy_sell_imbalance or 0.0) <= 0
        and (liquidation_long_usd or 0.0) >= (liquidation_short_usd or 0.0)
    )
    basis_dislocation = bool((basis_dislocation_score or 0.0) >= 0.65)
    flow_divergence = bool((flow_divergence_score or 0.0) >= 0.55)
    _flow_signal = taker_imbalance_1h if taker_imbalance_1h is not None else taker_buy_sell_imbalance
    order_flow_confirmed = bool((_flow_signal or 0.0) >= 0.08 and (oi_change_1h or 0.0) > 0)
    derivatives_labels = [
        label
        for label, enabled in (
            ("crowded_long", crowded_long),
            ("crowded_short", crowded_short),
            ("squeeze_building", squeeze_building),
            ("flush_risk", flush_risk),
            ("basis_dislocation", basis_dislocation),
            ("flow_divergence", flow_divergence),
            ("order_flow_confirmed", order_flow_confirmed),
        )
        if enabled
    ]

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
        "market_regime": (
            "distribution"
            if crowded_long and (distribution_score or 0.0) >= 0.65
            else "squeeze_building"
            if squeeze_building
            else "short_crowded"
            if crowded_short
            else "trend_follow"
            if (squeeze_score or 0.0) >= 0.62
            else "mixed"
        ),
        "funding_regime": (
            "crowded_long"
            if crowded_long
            else "crowded_short"
            if crowded_short
            else "hot"
            if (funding_rate or 0.0) >= 0.001
            else "discounted"
            if (funding_rate or 0.0) <= -0.001
            else "balanced"
        ),
        "oi_regime": "expanding" if (oi_change_1h or 0.0) > 0 else "contracting" if (oi_change_1h or 0.0) < 0 else "flat",
        "liquidation_state": "short_squeeze" if (liquidation_short_usd or 0.0) > (liquidation_long_usd or 0.0) else "long_flush" if (liquidation_long_usd or 0.0) > 0 else "calm",
        "orderbook_state": "buy_pressure" if (taker_buy_sell_imbalance or 0.0) > 0 else "sell_pressure" if (taker_buy_sell_imbalance or 0.0) < 0 else "balanced",
        "crowding_warning": bool((crowding_score or 0.0) >= 0.70),
        "history_ready": bool(oi_history_series and funding_history_series),
        "taker_history_ready": bool(taker_history_series),
        "taker_imbalance_1h": taker_imbalance_1h,
        "taker_imbalance_4h": taker_imbalance_4h,
        "active_datasets": [dataset for dataset, items in payloads.items() if items],
        "payload_counts": {dataset: len(items or []) for dataset, items in payloads.items()},
        "history_exchange": str((selected_oi_history_rows[-1] if selected_oi_history_rows else {}).get("_exchange") or (funding_history_rows[-1] if funding_history_rows else {}).get("_exchange") or "Binance"),
        "history_interval": history_interval,
        "oi_history_source_dataset": (
            "open_interest_history"
            if oi_history_rows
            else "open_interest_aggregated_history"
            if oi_aggregated_history_rows
            else "open_interest_stablecoin_margin_history"
            if oi_stablecoin_history_rows
            else None
        ),
        "funding_exchange_count": len(primary_funding_rows),
        "funding_token_exchange_count": len(token_funding_rows),
        "funding_mean": funding_mean,
        "funding_zscore": funding_zscore,
        "funding_reversion_speed": funding_reversion_speed,
        "funding_extreme_deviation": funding_extreme_deviation,
        "liquidation_burst_score": liquidation_burst_score,
        "liquidation_map_total_usd": liquidation_map_total_usd,
        "liquidation_map_above_usd": liquidation_map_above_usd,
        "liquidation_map_below_usd": liquidation_map_below_usd,
        "liquidation_map_pressure_score": liquidation_map_pressure_score,
        "liquidation_map_largest_cluster_price": _coalesce_float(
            liquidation_map_largest_row, "liquidation_map_largest_cluster_price"
        ),
        "liquidation_map_largest_cluster_usd": _coalesce_float(
            liquidation_map_largest_row, "liquidation_map_largest_cluster_usd"
        ),
        "liquidation_map_last_price": _coalesce_float(
            liquidation_map_largest_row, "last_price"
        ),
        "orderbook_agg_bid_usd": orderbook_agg_bid_usd,
        "orderbook_agg_ask_usd": orderbook_agg_ask_usd,
        "orderbook_agg_imbalance": orderbook_agg_imbalance,
        "orderbook_wall_above_usd": orderbook_wall_above_usd,
        "orderbook_wall_below_usd": orderbook_wall_below_usd,
        "orderbook_wall_above_price": orderbook_wall_above_price,
        "orderbook_wall_below_price": orderbook_wall_below_price,
        "liquidity_heatmap_total_usd": liquidity_heatmap_total_usd,
        "liquidity_heatmap_above_usd": liquidity_heatmap_above_usd,
        "liquidity_heatmap_below_usd": liquidity_heatmap_below_usd,
        "liquidity_wall_nearest_above_price": _coalesce_float(
            liquidity_above_row, "liquidity_wall_nearest_above_price", "liquidation_map_nearest_above_price"
        ),
        "liquidity_wall_nearest_below_price": _coalesce_float(
            liquidity_below_row, "liquidity_wall_nearest_below_price", "liquidation_map_nearest_below_price"
        ),
        "liquidity_wall_nearest_above_usd": liquidity_wall_above_usd,
        "liquidity_wall_nearest_below_usd": liquidity_wall_below_usd,
        "liquidity_void_score": liquidity_void_score,
        "heatmap_pressure_score": heatmap_pressure_score,
        "top_account_long_short_ratio": top_account_ratio,
        "top_position_long_short_ratio": top_position_ratio,
        "net_position": net_position,
        "coinbase_premium": coinbase_premium,
        "option_max_pain": option_max_pain,
        "option_put_call_ratio": option_put_call_ratio,
        "option_open_interest_usd": option_open_interest_usd,
        "option_volume_usd": option_volume_usd,
        "option_iv": option_iv,
        "option_iv_skew": option_iv_skew,
        "option_distance_to_max_pain_pct": option_distance_to_max_pain_pct,
        "option_vs_futures_oi_ratio": option_vs_futures_oi_ratio,
        "gamma_exposure": gamma_exposure,
        "bitcoin_etf_net_flow": bitcoin_etf_net_flow,
        "spot_exchange_inflow_usd": spot_exchange_inflow_usd,
        "spot_exchange_outflow_usd": spot_exchange_outflow_usd,
        "spot_exchange_netflow_usd": spot_exchange_netflow_usd,
        "spot_exchange_inflow_count": spot_exchange_inflow_count,
        "spot_exchange_outflow_count": spot_exchange_outflow_count,
        "spot_netflow_score": spot_netflow_score,
        "exchange_flow_pressure": exchange_flow_pressure,
        "exchange_balance_btc": exchange_balance_btc,
        "exchange_balance_usd": exchange_balance_usd,
        "exchange_balance_change_24h": exchange_balance_change_24h,
        "exchange_balance_change_7d": exchange_balance_change_7d,
        "stablecoin_exchange_balance_usd": stablecoin_exchange_balance_usd,
        "stablecoin_exchange_balance_change_24h": stablecoin_exchange_balance_change_24h,
        "stablecoin_netflow_usd": stablecoin_netflow_usd,
        "onchain_activity_score": onchain_activity_score,
        "exchange_reserve_pressure_score": exchange_reserve_pressure_score,
        "long_short_ratio_change_24h": long_short_ratio_change,
        "oi_change_1h_history": None if oi_change_1h_history is None else oi_change_1h_history * 100.0,
        "oi_change_4h_history": None if oi_change_4h_history is None else oi_change_4h_history * 100.0,
        "oi_change_24h_history": None if oi_change_24h_history is None else oi_change_24h_history * 100.0,
        "oi_funding_divergence_score": oi_funding_divergence_score,
        "oi_funding_divergence": bool(oi_funding_divergence_score >= 0.55),
        "basis_dislocation_score": basis_dislocation_score,
        "basis_dislocation": basis_dislocation,
        "flow_divergence_score": flow_divergence_score,
        "flow_divergence": flow_divergence,
        "derivatives_heat_score": derivatives_heat_score,
        "crowded_long": crowded_long,
        "crowded_short": crowded_short,
        "squeeze_building": squeeze_building,
        "flush_risk": flush_risk,
        "order_flow_confirmed": order_flow_confirmed,
        "derivatives_labels": derivatives_labels,
        "funding_arbitrage_symbol_matched": bool(arbitrage_rows),
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
        row.heatmap_pressure_score = _coalesce_float(
            snapshot.payload or {}, "heatmap_pressure_score"
        ) or snapshot.squeeze_score
        row.liquidity_void_score = _coalesce_float(
            snapshot.payload or {}, "liquidity_void_score"
        ) or snapshot.depth_thinness_score
        row.trade_delta = snapshot.taker_buy_sell_imbalance
        row.payload = {
            "crowding_score": snapshot.crowding_score,
            "distribution_score": snapshot.distribution_score,
            **dict(snapshot.payload or {}),
        }
        await session.commit()


def _rotate_non_manual_datasets(datasets: Sequence[str], window: int) -> List[str]:
    """Take `window` datasets, advancing the start position on every sweep.

    The 10 req/min plan only affords a couple of datasets per background round.
    Always slicing the head of COINGLASS_DEFAULT_DATASETS meant the open-interest
    entries were refreshed every round while everything after them (funding,
    taker, liquidation, long/short) was never reached. Rotating puts every
    dataset on a bounded refresh cycle instead.
    """
    global _NON_MANUAL_DATASET_CURSOR
    total = len(datasets)
    if total <= 0:
        return []
    window = max(1, min(int(window), total))
    start = _NON_MANUAL_DATASET_CURSOR % total
    picked = [datasets[(start + offset) % total] for offset in range(window)]
    _NON_MANUAL_DATASET_CURSOR = (start + window) % total
    return picked


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
        "stopped_early": False,
        "stop_reason": None,
    }
    if not coinglass_enabled():
        return summary
    minute_limit = max(1, int(getattr(settings, "COINGLASS_RATE_LIMIT_PER_MIN", 30) or 30))
    if manual:
        total_planned = len(selected_symbols) * len(selected_datasets)
        if total_planned > minute_limit:
            if minute_limit < len(selected_symbols):
                selected_symbols = selected_symbols[:minute_limit]
                selected_datasets = selected_datasets[:1]
            else:
                selected_datasets = selected_datasets[: max(1, minute_limit // max(1, len(selected_symbols)))]
            summary["symbols"] = selected_symbols
            summary["datasets"] = selected_datasets
            summary["stopped_early"] = True
            summary["stop_reason"] = f"manual_refresh_limited_by_{minute_limit}_per_min_budget"
    else:
        try:
            budget_state = await get_coinglass_budget_state()
            headroom = coinglass_minute_headroom(budget_state, manual=False)
        except Exception:
            headroom = max(1, int(getattr(settings, "COINGLASS_RATE_LIMIT_PER_MIN", 30) or 30) - 2)
        effective_capacity = max(0, int(headroom) - _NON_MANUAL_DATASET_RESERVE)
        if effective_capacity <= 0:
            summary["stopped_early"] = True
            summary["stop_reason"] = "minute_budget_headroom_low"
            summary["budget"] = (await get_coinglass_budget_state()).to_dict()
            return summary
        total_planned = len(selected_symbols) * len(selected_datasets)
        if total_planned > effective_capacity:
            if effective_capacity < len(selected_symbols):
                selected_symbols = selected_symbols[:effective_capacity]
                selected_datasets = _rotate_non_manual_datasets(selected_datasets, 1)
            else:
                selected_datasets = _rotate_non_manual_datasets(
                    selected_datasets,
                    max(1, effective_capacity // max(1, len(selected_symbols))),
                )
            summary["symbols"] = selected_symbols
            summary["datasets"] = selected_datasets
            summary["stopped_early"] = True
            summary["stop_reason"] = f"non_manual_refresh_limited_by_{minute_limit}_per_min_budget"
    stop_reason = ""
    async with CoinglassClient() as client:
        for symbol in selected_symbols:
            for dataset in selected_datasets:
                manifest = get_coinglass_manifest(dataset)
                if manifest is None:
                    continue
                primary_route = manifest.routes[0] if manifest.routes else None
                requested_exchange = (
                    normalize_coinglass_exchange(
                        (primary_route.default_params.get("exchange") if primary_route else None) or "Binance"
                    )
                    if any("exchange" in route.required_params for route in manifest.routes)
                    else "aggregate"
                )
                requested_interval = (
                    str((primary_route.default_params.get("interval") if primary_route else None) or "h4")
                    if any("interval" in route.required_params for route in manifest.routes)
                    else ""
                )
                try:
                    result = await client.request_dataset(
                        manifest,
                        symbol=symbol,
                        exchange=requested_exchange if requested_exchange != "aggregate" else None,
                        interval=requested_interval or None,
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
                    normalized_result = normalize_dataset_response(
                        dataset=dataset,
                        request_meta={
                            "symbol": symbol,
                            "exchange": result["params"].get("exchange"),
                            "interval": result["params"].get("interval"),
                        },
                        response_payload=result["payload"],
                    )
                    if str(normalized_result.get("status") or "") != "ok":
                        error_text = str(normalized_result.get("error") or normalized_result.get("status") or "request_failed")
                        degrade_due_to_budget = (
                            str(normalized_result.get("status") or "") == "degraded"
                            or should_pause_coinglass_requests(error_text)
                        )
                        await record_coinglass_ingest_status(
                            dataset=dataset,
                            symbol=symbol,
                            exchange=str(result["params"].get("exchange") or requested_exchange),
                            interval=str(result["params"].get("interval") or requested_interval),
                            status="degraded" if degrade_due_to_budget else str(normalized_result.get("status") or "failed"),
                            rows_written=0,
                            latency_ms=int(result["latency_ms"] or 0),
                            error=error_text,
                            details={
                                "request_key": result["request_key"],
                                "api_version": result["route"].api_version,
                                "path": result["route"].path,
                                **dict(normalized_result.get("details") or {}),
                            },
                            manifest=manifest,
                        )
                        summary["errors"].append({"dataset": dataset, "symbol": symbol, "error": error_text})
                        if degrade_due_to_budget:
                            stop_reason = error_text
                            break
                        continue
                    normalized_rows = list(normalized_result.get("rows") or [])
                    persist_normalized_rows(
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
                        response_payload=normalized_rows,
                    )
                    rows_written = len(normalized_rows)
                    await record_coinglass_ingest_status(
                        dataset=dataset,
                        symbol=symbol,
                        exchange=str(result["params"].get("exchange") or "aggregate"),
                        interval=str(result["params"].get("interval") or ""),
                        status="ok",
                        rows_written=rows_written,
                        latency_ms=int(result["latency_ms"] or 0),
                        details={
                            "request_key": result["request_key"],
                            "api_version": result["route"].api_version,
                            "path": result["route"].path,
                            **dict(normalized_result.get("details") or {}),
                        },
                        manifest=manifest,
                    )
                    summary["updated"].append(
                        {
                            "dataset": dataset,
                            "symbol": symbol,
                            "rows_written": rows_written,
                            "latency_ms": int(result["latency_ms"] or 0),
                        }
                    )
                except CoinglassBudgetExceeded as exc:
                    await record_coinglass_ingest_status(
                        dataset=dataset,
                        symbol=symbol,
                        exchange=requested_exchange,
                        interval=requested_interval,
                        status="degraded",
                        rows_written=0,
                        error=str(exc),
                        details={"reason": "budget_guard"},
                        manifest=manifest,
                    )
                    summary["errors"].append({"dataset": dataset, "symbol": symbol, "error": str(exc)})
                    stop_reason = str(exc)
                    break
                except Exception as exc:
                    error_text = str(exc)
                    degrade_due_to_budget = should_pause_coinglass_requests(error_text)
                    await record_coinglass_ingest_status(
                        dataset=dataset,
                        symbol=symbol,
                        exchange=requested_exchange,
                        interval=requested_interval,
                        status="degraded" if degrade_due_to_budget else "failed",
                        rows_written=0,
                        error=error_text,
                        details={"reason": "budget_guard" if degrade_due_to_budget else "request_failed"},
                        manifest=manifest,
                    )
                    summary["errors"].append({"dataset": dataset, "symbol": symbol, "error": error_text})
                    if degrade_due_to_budget:
                        stop_reason = error_text
                        break
            # Reads every supported dataset parquet (tens of MB); keep it off the loop.
            snapshot = await asyncio.to_thread(build_derivatives_snapshot, symbol)
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
            if stop_reason:
                break
    persist_symbol_registry(selected_symbols)
    summary["budget"] = (await get_coinglass_budget_state()).to_dict()
    if stop_reason:
        summary["stopped_early"] = True
        summary["stop_reason"] = stop_reason
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
                .limit(1)
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
    snapshot_active_datasets = list(((snapshot or {}).get("payload") or {}).get("active_datasets") or [])
    active_datasets = snapshot_active_datasets or [
        str(row.get("dataset") or "")
        for row in statuses
        if str(row.get("status") or "") == "ok" and int(row.get("rows_written") or 0) > 0
    ]
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
        "history_ready": bool(payload.get("history_ready")),
        "taker_history_ready": bool(payload.get("taker_history_ready")),
        "taker_imbalance_1h": payload.get("taker_imbalance_1h"),
        "taker_imbalance_4h": payload.get("taker_imbalance_4h"),
        "history_exchange": payload.get("history_exchange"),
        "history_interval": payload.get("history_interval"),
        "funding_mean": payload.get("funding_mean"),
        "funding_zscore": payload.get("funding_zscore"),
        "funding_reversion_speed": payload.get("funding_reversion_speed"),
        "funding_extreme_deviation": payload.get("funding_extreme_deviation"),
        "liquidation_burst_score": payload.get("liquidation_burst_score"),
        "liquidation_map_total_usd": payload.get("liquidation_map_total_usd"),
        "liquidation_map_above_usd": payload.get("liquidation_map_above_usd"),
        "liquidation_map_below_usd": payload.get("liquidation_map_below_usd"),
        "liquidation_map_pressure_score": payload.get("liquidation_map_pressure_score"),
        "liquidation_map_largest_cluster_price": payload.get("liquidation_map_largest_cluster_price"),
        "liquidation_map_largest_cluster_usd": payload.get("liquidation_map_largest_cluster_usd"),
        "orderbook_agg_bid_usd": payload.get("orderbook_agg_bid_usd"),
        "orderbook_agg_ask_usd": payload.get("orderbook_agg_ask_usd"),
        "orderbook_agg_imbalance": payload.get("orderbook_agg_imbalance"),
        "orderbook_wall_above_usd": payload.get("orderbook_wall_above_usd"),
        "orderbook_wall_below_usd": payload.get("orderbook_wall_below_usd"),
        "orderbook_wall_above_price": payload.get("orderbook_wall_above_price"),
        "orderbook_wall_below_price": payload.get("orderbook_wall_below_price"),
        "liquidity_heatmap_total_usd": payload.get("liquidity_heatmap_total_usd"),
        "liquidity_heatmap_above_usd": payload.get("liquidity_heatmap_above_usd"),
        "liquidity_heatmap_below_usd": payload.get("liquidity_heatmap_below_usd"),
        "liquidity_wall_nearest_above_price": payload.get("liquidity_wall_nearest_above_price"),
        "liquidity_wall_nearest_below_price": payload.get("liquidity_wall_nearest_below_price"),
        "liquidity_wall_nearest_above_usd": payload.get("liquidity_wall_nearest_above_usd"),
        "liquidity_wall_nearest_below_usd": payload.get("liquidity_wall_nearest_below_usd"),
        "liquidity_void_score": payload.get("liquidity_void_score"),
        "heatmap_pressure_score": payload.get("heatmap_pressure_score"),
        "top_account_long_short_ratio": payload.get("top_account_long_short_ratio"),
        "top_position_long_short_ratio": payload.get("top_position_long_short_ratio"),
        "net_position": payload.get("net_position"),
        "coinbase_premium": payload.get("coinbase_premium"),
        "option_max_pain": payload.get("option_max_pain"),
        "option_put_call_ratio": payload.get("option_put_call_ratio"),
        "option_open_interest_usd": payload.get("option_open_interest_usd"),
        "option_volume_usd": payload.get("option_volume_usd"),
        "option_iv": payload.get("option_iv"),
        "option_iv_skew": payload.get("option_iv_skew"),
        "option_distance_to_max_pain_pct": payload.get("option_distance_to_max_pain_pct"),
        "option_vs_futures_oi_ratio": payload.get("option_vs_futures_oi_ratio"),
        "gamma_exposure": payload.get("gamma_exposure"),
        "bitcoin_etf_net_flow": payload.get("bitcoin_etf_net_flow"),
        "spot_exchange_inflow_usd": payload.get("spot_exchange_inflow_usd"),
        "spot_exchange_outflow_usd": payload.get("spot_exchange_outflow_usd"),
        "spot_exchange_netflow_usd": payload.get("spot_exchange_netflow_usd"),
        "spot_exchange_inflow_count": payload.get("spot_exchange_inflow_count"),
        "spot_exchange_outflow_count": payload.get("spot_exchange_outflow_count"),
        "spot_netflow_score": payload.get("spot_netflow_score"),
        "exchange_flow_pressure": payload.get("exchange_flow_pressure"),
        "exchange_balance_btc": payload.get("exchange_balance_btc"),
        "exchange_balance_usd": payload.get("exchange_balance_usd"),
        "exchange_balance_change_24h": payload.get("exchange_balance_change_24h"),
        "exchange_balance_change_7d": payload.get("exchange_balance_change_7d"),
        "stablecoin_exchange_balance_usd": payload.get("stablecoin_exchange_balance_usd"),
        "stablecoin_exchange_balance_change_24h": payload.get("stablecoin_exchange_balance_change_24h"),
        "stablecoin_netflow_usd": payload.get("stablecoin_netflow_usd"),
        "onchain_activity_score": payload.get("onchain_activity_score"),
        "exchange_reserve_pressure_score": payload.get("exchange_reserve_pressure_score"),
        "long_short_ratio_change_24h": payload.get("long_short_ratio_change_24h"),
        "oi_change_1h_history": payload.get("oi_change_1h_history"),
        "oi_change_4h_history": payload.get("oi_change_4h_history"),
        "oi_change_24h_history": payload.get("oi_change_24h_history"),
        "oi_funding_divergence": bool(payload.get("oi_funding_divergence")),
        "oi_funding_divergence_score": payload.get("oi_funding_divergence_score"),
        "basis_dislocation": bool(payload.get("basis_dislocation")),
        "basis_dislocation_score": payload.get("basis_dislocation_score"),
        "flow_divergence": bool(payload.get("flow_divergence")),
        "flow_divergence_score": payload.get("flow_divergence_score"),
        "crowded_long": bool(payload.get("crowded_long")),
        "crowded_short": bool(payload.get("crowded_short")),
        "squeeze_building": bool(payload.get("squeeze_building")),
        "flush_risk": bool(payload.get("flush_risk")),
        "order_flow_confirmed": bool(payload.get("order_flow_confirmed")),
        "derivatives_labels": list(payload.get("derivatives_labels") or []),
        "derivatives_heat_score": payload.get("derivatives_heat_score"),
        "crowding_score": snapshot.get("crowding_score") if isinstance(snapshot, Mapping) else None,
        "squeeze_score": snapshot.get("squeeze_score") if isinstance(snapshot, Mapping) else None,
        "distribution_score": snapshot.get("distribution_score") if isinstance(snapshot, Mapping) else None,
    }
