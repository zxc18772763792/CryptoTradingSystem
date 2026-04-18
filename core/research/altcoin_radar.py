"""Altcoin radar scoring and detail helpers."""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import numpy as np
import pandas as pd


VALID_TIMEFRAMES = {"1h", "4h", "1d"}
TIMEFRAME_SECONDS = {"1h": 3600, "4h": 14400, "1d": 86400}
STATE_LAYOUT = "布局吸筹"
STATE_ANOMALY = "异动启动"
STATE_CONTROL_TRACK = "高控盘跟踪"
STATE_CONTROL_WARN = "高控盘警戒"
STATE_DISTRIBUTION = "派发风险"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except Exception:
        return float(default)
    if not math.isfinite(parsed):
        return float(default)
    return float(parsed)


def _clamp01(value: Any) -> float:
    try:
        parsed = float(value)
    except Exception:
        return 0.0
    if math.isnan(parsed):
        return 0.0
    return max(0.0, min(1.0, parsed))


def _round4(value: Any) -> float:
    return round(_to_float(value), 4)


def _safe_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series(dtype=float)
    return pd.to_numeric(df[column], errors="coerce").dropna()


def _pct_change(close: pd.Series, periods: int) -> float:
    if len(close) <= periods:
        return 0.0
    base = _to_float(close.iloc[-periods - 1], 0.0)
    last = _to_float(close.iloc[-1], 0.0)
    if base <= 0:
        return 0.0
    return (last / base) - 1.0


def _avg_true_range_ratio(df: pd.DataFrame, window: int = 20) -> float:
    high = _safe_series(df, "high")
    low = _safe_series(df, "low")
    close = _safe_series(df, "close")
    if high.empty or low.empty or close.empty:
        return 0.0
    prev_close = close.shift(1)
    true_range = pd.concat(
        [
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    current = _to_float(true_range.iloc[-1], 0.0)
    baseline = _to_float(true_range.tail(window).mean(), 0.0)
    if baseline <= 0:
        return 0.0
    return current / baseline


def _volume_ratio(volume: pd.Series, fast: int = 3, slow: int = 20) -> float:
    if volume.empty:
        return 0.0
    fast_mean = _to_float(volume.tail(fast).mean(), 0.0)
    slow_mean = _to_float(volume.tail(max(slow, fast + 1)).mean(), 0.0)
    if slow_mean <= 0:
        return 0.0
    return fast_mean / slow_mean


def _rolling_return_volatility(close: pd.Series, window: int = 20) -> float:
    ret = close.pct_change().dropna()
    if ret.empty:
        return 0.0
    return _to_float(ret.tail(window).std(), 0.0)


def _drift_stability(close: pd.Series, bars: int = 24) -> float:
    if len(close) <= bars:
        return 0.0
    ret = close.pct_change().dropna().tail(bars)
    if ret.empty:
        return 0.0
    total = _to_float(ret.sum(), 0.0)
    path = _to_float(ret.abs().sum(), 0.0)
    if path <= 0:
        return 0.0
    return max(total / path, 0.0)


def _absorption_proxy(df: pd.DataFrame, window: int = 12) -> float:
    high = _safe_series(df, "high")
    low = _safe_series(df, "low")
    close = _safe_series(df, "close")
    open_ = _safe_series(df, "open")
    if high.empty or low.empty or close.empty or open_.empty:
        return 0.0
    frame = pd.DataFrame({"open": open_, "high": high, "low": low, "close": close}).tail(window)
    if frame.empty:
        return 0.0
    spread = (frame["high"] - frame["low"]).replace(0.0, np.nan)
    lower_wick = (frame[["open", "close"]].min(axis=1) - frame["low"]).clip(lower=0.0)
    close_pos = ((frame["close"] - frame["low"]) / spread).clip(lower=0.0, upper=1.0).fillna(0.0)
    red_bias = (frame["close"] <= frame["open"]).astype(float) * 0.6 + 0.4
    signal = (lower_wick / spread).fillna(0.0) * close_pos * red_bias
    return _to_float(signal.mean(), 0.0)


def _breakout_proximity(close: pd.Series, window: int = 30) -> float:
    if len(close) < 5:
        return 0.0
    lookback = close.tail(window)
    ref_high = _to_float(lookback.max(), 0.0)
    last = _to_float(close.iloc[-1], 0.0)
    if ref_high <= 0 or last <= 0:
        return 0.0
    gap = max(ref_high - last, 0.0) / ref_high
    return 1.0 - gap


def _close_control(df: pd.DataFrame, window: int = 8) -> float:
    high = _safe_series(df, "high")
    low = _safe_series(df, "low")
    close = _safe_series(df, "close")
    if high.empty or low.empty or close.empty:
        return 0.0
    spread = (high - low).replace(0.0, np.nan)
    close_pos = ((close - low) / spread).clip(lower=0.0, upper=1.0).fillna(0.5)
    return _to_float(close_pos.tail(window).mean(), 0.0)


def _impulse_after_compression(close: pd.Series, bars: int = 3, base_window: int = 18) -> float:
    if len(close) <= bars + 2:
        return 0.0
    short_ret = max(_pct_change(close, bars), 0.0)
    ret = close.pct_change().dropna()
    base_vol = _to_float(ret.tail(base_window).std(), 0.0)
    if base_vol <= 0:
        return short_ret
    return short_ret / base_vol


def _spread_impact(micro_snapshot: Mapping[str, Any]) -> float:
    orderbook = micro_snapshot.get("orderbook") or {}
    spread_bps = _to_float(orderbook.get("spread_bps"), 0.0)
    large_orders = _to_float((micro_snapshot.get("payload") or {}).get("large_order_count"), 0.0)
    return spread_bps + (large_orders * 0.25)


def _one_sided_flow(micro_snapshot: Mapping[str, Any], community_snapshot: Mapping[str, Any]) -> float:
    micro_flow = micro_snapshot.get("aggressor_flow") or {}
    community_flow = community_snapshot.get("flow_proxy") or {}
    imbalance = abs(_to_float(micro_flow.get("imbalance"), 0.0))
    imbalance = max(imbalance, abs(_to_float(community_flow.get("imbalance"), 0.0)))
    buy_ratio = _to_float(community_flow.get("buy_ratio"), 0.5)
    return imbalance + abs(buy_ratio - 0.5)


def _community_flow_value(snapshot: Mapping[str, Any]) -> Optional[float]:
    if not snapshot:
        return None
    flow = snapshot.get("flow_proxy") or {}
    value = max(_to_float(flow.get("imbalance"), 0.0), 0.0)
    value += max(_to_float(flow.get("buy_ratio"), 0.5) - 0.5, 0.0)
    return value


def _announcement_value(snapshot: Mapping[str, Any]) -> Optional[float]:
    if not snapshot:
        return None
    count = len(snapshot.get("announcements") or [])
    if count <= 0:
        count = int(_to_float((snapshot.get("payload") or {}).get("announcement_count"), 0.0))
    return float(max(count, 0))


def _funding_basis_value(micro_snapshot: Mapping[str, Any]) -> Optional[float]:
    if not micro_snapshot:
        return None
    funding = micro_snapshot.get("funding_rate") or {}
    basis = micro_snapshot.get("spot_futures_basis") or {}
    funding_rate = max(_to_float(funding.get("funding_rate"), 0.0), 0.0)
    basis_pct = max(_to_float(basis.get("basis_pct"), 0.0), 0.0)
    return funding_rate + basis_pct


def _whale_context_value(snapshot: Mapping[str, Any]) -> Optional[float]:
    if not snapshot:
        return None
    txs = snapshot.get("transactions") or []
    total_btc = sum(max(_to_float(item.get("btc"), 0.0), 0.0) for item in txs[:8])
    base = _to_float(snapshot.get("count"), 0.0)
    return base + total_btc / 10.0


def _security_event_value(community_snapshot: Mapping[str, Any]) -> float:
    if not community_snapshot:
        return 0.0
    alerts = community_snapshot.get("security_alerts") or {}
    events = alerts.get("events") or []
    count = len(events)
    if count <= 0:
        count = int(_to_float((community_snapshot.get("payload") or {}).get("security_alert_count"), 0.0))
    return float(max(count, 0))


def _snapshot_payload(snapshot: Mapping[str, Any]) -> Dict[str, Any]:
    payload = snapshot.get("payload") or {}
    return payload if isinstance(payload, dict) else {}


def _freshness_score(age_sec: Optional[float], threshold_sec: float, hard_cap_multiple: float) -> float:
    if age_sec is None or age_sec < 0:
        return 0.0
    threshold = max(60.0, threshold_sec)
    over = max(age_sec - threshold, 0.0)
    denom = max(threshold * max(hard_cap_multiple, 1.0), 1.0)
    return _clamp01(1.0 - (over / denom))


def _ts_to_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = pd.to_datetime(value, utc=True)
    except Exception:
        return None
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime()


def _age_seconds(value: Any, now: Optional[datetime] = None) -> Optional[float]:
    ts = _ts_to_datetime(value)
    if ts is None:
        return None
    current = now or _utcnow()
    return max(0.0, (current - ts).total_seconds())


def _normalize_symbols(symbols: Sequence[str]) -> List[str]:
    normalized: List[str] = []
    seen = set()
    for symbol in symbols:
        text = str(symbol or "").strip().upper()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


def _series_percentiles(raw_map: Mapping[str, Optional[float]]) -> Dict[str, Optional[float]]:
    series = pd.Series({k: (_to_float(v) if v is not None else np.nan) for k, v in raw_map.items()}, dtype=float)
    valid = series.dropna()
    if valid.empty:
        return {k: None for k in raw_map}
    if len(valid) == 1:
        only_key = str(valid.index[0])
        return {k: (0.5 if str(k) == only_key else None) for k in raw_map}
    ranked = valid.rank(method="average", pct=True).clip(lower=0.0, upper=1.0)
    result: Dict[str, Optional[float]] = {}
    for key in raw_map:
        if key in ranked:
            result[key] = _clamp01(ranked[key])
        else:
            result[key] = None
    return result


def _weighted_score(values: Mapping[str, Optional[float]], weights: Mapping[str, float]) -> float:
    weighted = 0.0
    weight_sum = 0.0
    for key, weight in weights.items():
        value = values.get(key)
        if value is None:
            continue
        numeric = _clamp01(value)
        if weight <= 0:
            continue
        weighted += numeric * float(weight)
        weight_sum += float(weight)
    if weight_sum <= 0:
        return 0.0
    return weighted / weight_sum


def _sort_key_for_row(row: Mapping[str, Any], sort_by: str) -> float:
    normalized = str(sort_by or "layout").strip().lower()
    if normalized == "alert":
        return _to_float(row.get("alert_score"), 0.0)
    if normalized == "anomaly":
        return _to_float(row.get("anomaly_score"), 0.0)
    if normalized == "accumulation":
        return _to_float(row.get("accumulation_score"), 0.0)
    if normalized == "control":
        return _to_float(row.get("control_score"), 0.0)
    if normalized == "chain":
        return _to_float(row.get("chain_confirmation_score"), 0.0)
    if normalized == "heat":
        return (
            _to_float(row.get("layout_score"), 0.0) * 0.4
            + _to_float(row.get("alert_score"), 0.0) * 0.3
            + _to_float(row.get("control_score"), 0.0) * 0.2
            + _to_float(row.get("chain_confirmation_score"), 0.0) * 0.1
        )
    return _to_float(row.get("layout_score"), 0.0)


def sort_rows(rows: Sequence[Mapping[str, Any]], sort_by: str = "layout") -> List[Dict[str, Any]]:
    ordered = [dict(row) for row in rows]
    ordered.sort(
        key=lambda row: (
            _sort_key_for_row(row, sort_by),
            _to_float(row.get("layout_score"), 0.0),
            _to_float(row.get("alert_score"), 0.0),
            _to_float(row.get("control_score"), 0.0),
        ),
        reverse=True,
    )
    out: List[Dict[str, Any]] = []
    for index, row in enumerate(ordered, start=1):
        row["rank"] = index
        out.append(row)
    return out


def summarize_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    exchange: str,
    timeframe: str,
    sort_by: str,
    symbols_requested: Sequence[str],
    symbols_used: Sequence[str],
    excluded_retired: Sequence[str],
    cache_key: str,
    warnings: Sequence[str],
) -> Dict[str, Any]:
    ordered = sort_rows(rows, sort_by=sort_by)
    leader = ordered[0] if ordered else {}
    degraded_count = sum(1 for row in ordered if row.get("data_quality", {}).get("degraded_reason"))
    summary = {
        "exchange": exchange,
        "timeframe": timeframe,
        "sort_by": sort_by,
        "scanned_count": len(symbols_used),
        "anomaly_count": sum(1 for row in ordered if row.get("signal_state") == STATE_ANOMALY),
        "accumulation_count": sum(1 for row in ordered if row.get("signal_state") == STATE_LAYOUT),
        "control_count": sum(
            1
            for row in ordered
            if row.get("signal_state") in {STATE_CONTROL_TRACK, STATE_CONTROL_WARN}
        ),
        "degraded_count": degraded_count,
        "leader": {
            "symbol": leader.get("symbol"),
            "signal_state": leader.get("signal_state"),
            "layout_score": leader.get("layout_score"),
            "alert_score": leader.get("alert_score"),
        }
        if leader
        else None,
        "symbols_used": list(symbols_used),
    }
    return {
        "summary": summary,
        "rows": ordered,
        "scan_meta": {
            "exchange": exchange,
            "timeframe": timeframe,
            "sort_by": sort_by,
            "symbols_requested": list(symbols_requested),
            "symbols_used": list(symbols_used),
            "excluded_retired": list(excluded_retired),
            "cache_key": cache_key,
        },
        "warnings": list(warnings),
    }


def _signal_state_for_row(row: Mapping[str, Any]) -> str:
    anomaly = _to_float(row.get("anomaly_score"), 0.0)
    accumulation = _to_float(row.get("accumulation_score"), 0.0)
    control = _to_float(row.get("control_score"), 0.0)
    risk_penalty = _to_float(row.get("risk_penalty"), 0.0)
    if anomaly >= 0.70 and accumulation < 0.35 and control >= 0.65:
        return STATE_DISTRIBUTION
    if control >= 0.70 and risk_penalty >= 0.25:
        return STATE_CONTROL_WARN
    if accumulation >= 0.68 and control >= 0.55 and risk_penalty < 0.20:
        return STATE_LAYOUT
    if anomaly >= 0.72 and (accumulation >= 0.45 or control >= 0.45):
        return STATE_ANOMALY
    if control >= 0.70 and risk_penalty < 0.25:
        return STATE_CONTROL_TRACK
    return ""


def _state_tags(signal_state: str, *, degraded: bool, has_alert_rule: bool) -> List[str]:
    tags: List[str] = []
    if signal_state:
        tags.append(signal_state)
    if degraded:
        tags.append("数据降级")
    if has_alert_rule:
        tags.append("已建预警")
    return tags


def build_altcoin_rows(
    *,
    market_frames: Mapping[str, pd.DataFrame],
    timeframe: str,
    factor_library: Optional[Mapping[str, Any]] = None,
    multi_assets: Optional[Mapping[str, Any]] = None,
    micro_snapshots: Optional[Mapping[str, Mapping[str, Any]]] = None,
    community_snapshots: Optional[Mapping[str, Mapping[str, Any]]] = None,
    whale_snapshots: Optional[Mapping[str, Mapping[str, Any]]] = None,
    alerted_symbols: Optional[Iterable[str]] = None,
    now: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    current = now or _utcnow()
    tf = str(timeframe or "4h").lower()
    if tf not in VALID_TIMEFRAMES:
        tf = "4h"
    factor_payload = dict(factor_library or {})
    multi_payload = dict(multi_assets or {})
    alerted = {str(symbol or "").strip().upper() for symbol in (alerted_symbols or []) if str(symbol or "").strip()}
    micro_map = {str(k).upper(): dict(v or {}) for k, v in (micro_snapshots or {}).items()}
    community_map = {str(k).upper(): dict(v or {}) for k, v in (community_snapshots or {}).items()}
    whale_map = {str(k).upper(): dict(v or {}) for k, v in (whale_snapshots or {}).items()}
    factor_rows = {
        str(item.get("symbol") or "").strip().upper(): dict(item or {})
        for item in (factor_payload.get("asset_scores") or [])
        if str(item.get("symbol") or "").strip()
    }
    multi_rows = {
        str(item.get("symbol") or "").strip().upper(): dict(item or {})
        for item in (multi_payload.get("assets") or [])
        if str(item.get("symbol") or "").strip()
    }
    corr_map = multi_payload.get("correlation") or {}
    expected_bar_sec = float(TIMEFRAME_SECONDS.get(tf, TIMEFRAME_SECONDS["4h"]))

    raw_components: Dict[str, Dict[str, Optional[float]]] = {
        "return_shock": {},
        "volume_burst": {},
        "range_expansion": {},
        "compression_inverse": {},
        "drift_stability": {},
        "absorption_proxy": {},
        "breakout_proximity": {},
        "positive_flow": {},
        "close_control": {},
        "liquidity_thinness": {},
        "impulse_after_compression": {},
        "spread_impact": {},
        "one_sided_flow": {},
        "community_flow": {},
        "announcements": {},
        "funding_basis": {},
        "whale_context": {},
        "security_events": {},
        "stale_data": {},
        "liquidity_risk": {},
        "snapshot_missing": {},
    }

    interim: Dict[str, Dict[str, Any]] = {}
    for symbol, frame in market_frames.items():
        normalized_symbol = str(symbol or "").strip().upper()
        if not normalized_symbol:
            continue
        df = frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
        if df.empty or "close" not in df.columns:
            continue
        df = df.sort_index().tail(180)
        close = _safe_series(df, "close")
        volume = _safe_series(df, "volume")
        if close.empty:
            continue
        micro = dict(micro_map.get(normalized_symbol) or {})
        community = dict(community_map.get(normalized_symbol) or {})
        whale = dict(whale_map.get(normalized_symbol) or {})
        factor_row = dict(factor_rows.get(normalized_symbol) or {})
        multi_row = dict(multi_rows.get(normalized_symbol) or {})
        market_age_sec = _age_seconds(df.index[-1], current)
        snapshot_ages = [
            age
            for age in (
                _age_seconds(micro.get("timestamp"), current),
                _age_seconds(community.get("timestamp"), current),
                _age_seconds(whale.get("timestamp"), current),
            )
            if age is not None
        ]
        snapshot_age_sec = (sum(snapshot_ages) / len(snapshot_ages)) if snapshot_ages else None
        market_freshness = _freshness_score(market_age_sec, expected_bar_sec, hard_cap_multiple=4.0)
        snapshot_freshness = _freshness_score(snapshot_age_sec, expected_bar_sec * 2.0, hard_cap_multiple=6.0)
        available_snapshots = sum(1 for snapshot in (micro, community, whale) if snapshot)
        chain_quality = _clamp01((available_snapshots / 3.0) * 0.45 + snapshot_freshness * 0.55)

        recent_range_ratio = _avg_true_range_ratio(df)
        recent_vol = _rolling_return_volatility(close)
        recent_return_1 = _pct_change(close, 1)
        recent_return_3 = _pct_change(close, 3)
        recent_return_6 = _pct_change(close, 6)
        positive_return_burst = max(recent_return_1, recent_return_3, recent_return_6, 0.0)
        absolute_return_burst = max(abs(recent_return_1), abs(recent_return_3), abs(recent_return_6))
        volume_burst = _volume_ratio(volume)
        drift_stability = _drift_stability(close)
        absorption = _absorption_proxy(df)
        breakout_proximity = _breakout_proximity(close)
        close_control = _close_control(df)
        impulse = _impulse_after_compression(close)
        micro_payload = _snapshot_payload(micro)
        community_payload = _snapshot_payload(community)
        whale_payload = _snapshot_payload(whale)
        orderbook = micro.get("orderbook") or {}
        spread_bps = _to_float(orderbook.get("spread_bps"), 0.0)
        avg_dollar_volume = _to_float((close.tail(24) * volume.tail(24)).mean(), 0.0)
        factor_liquidity = _to_float(factor_row.get("liquidity"), 0.0)
        btc_corr = _to_float((corr_map.get(normalized_symbol) or {}).get("BTC/USDT"), 0.0)
        liquidity_thinness = (
            (1.0 / max(avg_dollar_volume, 1.0))
            + max(-factor_liquidity, 0.0)
            + max(abs(btc_corr) - 0.75, 0.0) * 0.1
        )
        spread_impact = _spread_impact(micro)
        one_sided_flow = _one_sided_flow(micro, community)
        positive_flow = _community_flow_value(community)
        community_flow = _community_flow_value(community)
        announcements = _announcement_value(community)
        funding_basis = _funding_basis_value(micro)
        whale_context = _whale_context_value(whale)

        security_events = _security_event_value(community)
        missing_count = 3 - available_snapshots
        stale_data = (1.0 - market_freshness) + (1.0 - snapshot_freshness)
        liquidity_risk = spread_bps + (1.0 / max(avg_dollar_volume, 1.0)) * 1_000_000.0

        degraded_reason: List[str] = []
        if market_freshness < 0.45:
            degraded_reason.append("market_data_stale")
        if snapshot_freshness < 0.45:
            degraded_reason.append("snapshot_stale")
        if missing_count > 0:
            degraded_reason.append("snapshot_missing")
        if spread_bps >= 30:
            degraded_reason.append("spread_too_wide")
        if avg_dollar_volume > 0 and avg_dollar_volume < 1_000_000:
            degraded_reason.append("liquidity_thin")
        if security_events > 0:
            degraded_reason.append("security_event")

        raw_components["return_shock"][normalized_symbol] = max(positive_return_burst, absolute_return_burst * 0.75)
        raw_components["volume_burst"][normalized_symbol] = volume_burst
        raw_components["range_expansion"][normalized_symbol] = recent_range_ratio
        raw_components["compression_inverse"][normalized_symbol] = -recent_vol
        raw_components["drift_stability"][normalized_symbol] = drift_stability
        raw_components["absorption_proxy"][normalized_symbol] = absorption
        raw_components["breakout_proximity"][normalized_symbol] = breakout_proximity
        raw_components["positive_flow"][normalized_symbol] = positive_flow
        raw_components["close_control"][normalized_symbol] = close_control
        raw_components["liquidity_thinness"][normalized_symbol] = liquidity_thinness
        raw_components["impulse_after_compression"][normalized_symbol] = impulse
        raw_components["spread_impact"][normalized_symbol] = spread_impact
        raw_components["one_sided_flow"][normalized_symbol] = one_sided_flow
        raw_components["community_flow"][normalized_symbol] = community_flow
        raw_components["announcements"][normalized_symbol] = announcements
        raw_components["funding_basis"][normalized_symbol] = funding_basis
        raw_components["whale_context"][normalized_symbol] = whale_context
        raw_components["security_events"][normalized_symbol] = security_events
        raw_components["stale_data"][normalized_symbol] = stale_data
        raw_components["liquidity_risk"][normalized_symbol] = liquidity_risk
        raw_components["snapshot_missing"][normalized_symbol] = float(max(missing_count, 0))

        interim[normalized_symbol] = {
            "symbol": normalized_symbol,
            "factor_row": factor_row,
            "multi_row": multi_row,
            "micro": micro,
            "community": community,
            "whale": whale,
            "metrics_raw": {
                "last_price": _to_float(close.iloc[-1], 0.0),
                "return_1_bar": recent_return_1,
                "return_3_bar": recent_return_3,
                "return_6_bar": recent_return_6,
                "volume_burst_ratio": volume_burst,
                "range_expansion_ratio": recent_range_ratio,
                "compression_volatility": recent_vol,
                "drift_stability": drift_stability,
                "absorption_proxy": absorption,
                "breakout_proximity": breakout_proximity,
                "close_control": close_control,
                "avg_dollar_volume": avg_dollar_volume,
                "spread_bps": spread_bps,
                "order_flow_imbalance": _to_float((micro.get("aggressor_flow") or {}).get("imbalance"), 0.0),
                "community_flow_imbalance": _to_float((community.get("flow_proxy") or {}).get("imbalance"), 0.0),
                "announcement_count": _to_float((community_payload.get("announcement_count") or 0), 0.0)
                or _to_float(len(community.get("announcements") or []), 0.0),
                "whale_count": _to_float(whale.get("count"), 0.0),
                "btc_correlation": btc_corr,
                "factor_liquidity": factor_liquidity,
                "factor_low_beta": _to_float(factor_row.get("low_beta"), 0.0),
                "factor_low_vol": _to_float(factor_row.get("low_vol"), 0.0),
            },
            "freshness": {
                "as_of": df.index[-1].isoformat() if hasattr(df.index[-1], "isoformat") else str(df.index[-1]),
                "market_data_age_sec": None if market_age_sec is None else round(market_age_sec, 2),
                "snapshot_age_sec": None if snapshot_age_sec is None else round(snapshot_age_sec, 2),
                "market_label": "fresh" if market_freshness >= 0.7 else "watch" if market_freshness >= 0.45 else "stale",
                "snapshot_label": "fresh"
                if snapshot_freshness >= 0.7
                else "watch"
                if snapshot_freshness >= 0.45
                else "stale",
            },
            "data_quality": {
                "market_data_freshness": _round4(market_freshness),
                "snapshot_freshness": _round4(snapshot_freshness),
                "chain_quality": _round4(chain_quality),
                "degraded_reason": degraded_reason,
            },
            "sparkline": close.tail(36).tolist(),
            "has_alert_rule": normalized_symbol in alerted,
        }

    percentile_map = {key: _series_percentiles(values) for key, values in raw_components.items()}
    rows: List[Dict[str, Any]] = []
    for symbol in _normalize_symbols(interim.keys()):
        item = interim[symbol]
        pct = {name: percentile_map[name].get(symbol) for name in percentile_map}

        anomaly_score = _weighted_score(
            {
                "return_shock": pct["return_shock"],
                "volume_burst": pct["volume_burst"],
                "range_expansion": pct["range_expansion"],
            },
            {"return_shock": 0.45, "volume_burst": 0.35, "range_expansion": 0.20},
        )
        accumulation_score = _weighted_score(
            {
                "compression_inverse": pct["compression_inverse"],
                "drift_stability": pct["drift_stability"],
                "absorption_proxy": pct["absorption_proxy"],
                "breakout_proximity": pct["breakout_proximity"],
                "positive_flow": pct["positive_flow"],
            },
            {
                "compression_inverse": 0.35,
                "drift_stability": 0.25,
                "absorption_proxy": 0.20,
                "breakout_proximity": 0.10,
                "positive_flow": 0.10,
            },
        )
        control_score = _weighted_score(
            {
                "close_control": pct["close_control"],
                "liquidity_thinness": pct["liquidity_thinness"],
                "impulse_after_compression": pct["impulse_after_compression"],
                "spread_impact": pct["spread_impact"],
                "one_sided_flow": pct["one_sided_flow"],
            },
            {
                "close_control": 0.30,
                "liquidity_thinness": 0.25,
                "impulse_after_compression": 0.20,
                "spread_impact": 0.15,
                "one_sided_flow": 0.10,
            },
        )
        chain_components = {
            "community_flow": pct["community_flow"],
            "announcements": pct["announcements"],
            "funding_basis": pct["funding_basis"],
            "whale_context": pct["whale_context"],
        }
        chain_base = _weighted_score(
            chain_components,
            {
                "community_flow": 0.40,
                "announcements": 0.25,
                "funding_basis": 0.20,
                "whale_context": 0.15,
            },
        )
        chain_quality_factor = _to_float(item["data_quality"].get("chain_quality"), 0.0)
        chain_confirmation_score = chain_base * chain_quality_factor
        risk_penalty = _weighted_score(
            {
                "security_events": pct["security_events"],
                "stale_data": pct["stale_data"],
                "liquidity_risk": pct["liquidity_risk"],
                "snapshot_missing": pct["snapshot_missing"],
            },
            {
                "security_events": 0.35,
                "stale_data": 0.25,
                "liquidity_risk": 0.25,
                "snapshot_missing": 0.15,
            },
        )
        layout_score = (
            accumulation_score * 0.45
            + control_score * 0.30
            + anomaly_score * 0.15
            + chain_confirmation_score * 0.10
            - risk_penalty
        )
        alert_score = (
            anomaly_score * 0.55
            + accumulation_score * 0.20
            + control_score * 0.15
            + chain_confirmation_score * 0.10
            - risk_penalty
        )
        row = {
            "symbol": symbol,
            "layout_score": _round4(_clamp01(layout_score)),
            "alert_score": _round4(_clamp01(alert_score)),
            "anomaly_score": _round4(_clamp01(anomaly_score)),
            "accumulation_score": _round4(_clamp01(accumulation_score)),
            "control_score": _round4(_clamp01(control_score)),
            "chain_confirmation_score": _round4(_clamp01(chain_confirmation_score)),
            "risk_penalty": _round4(_clamp01(risk_penalty)),
            "signal_state": "",
            "tags": [],
            "reasons_proxy": [],
            "reasons_chain": [],
            "data_quality": item["data_quality"],
            "freshness": item["freshness"],
            "metrics": {
                **{key: _round4(value) for key, value in item["metrics_raw"].items()},
                "percentiles": {key: (None if value is None else _round4(value)) for key, value in pct.items()},
            },
            "sparkline": item["sparkline"],
            "has_alert_rule": bool(item["has_alert_rule"]),
        }
        row["signal_state"] = _signal_state_for_row(row)
        degraded = bool(row["data_quality"].get("degraded_reason"))
        row["tags"] = _state_tags(
            row["signal_state"],
            degraded=degraded,
            has_alert_rule=bool(item["has_alert_rule"]),
        )
        proxy_reasons: List[str] = []
        chain_reasons: List[str] = []
        if pct["compression_inverse"] is not None and pct["compression_inverse"] >= 0.7:
            proxy_reasons.append(f"波动压缩位于币池前 {int(_to_float(pct['compression_inverse']) * 100)}%")
        if pct["drift_stability"] is not None and pct["drift_stability"] >= 0.68:
            proxy_reasons.append(f"抬升路径稳定，drift_stability={_to_float(item['metrics_raw']['drift_stability']):.2f}")
        if pct["absorption_proxy"] is not None and pct["absorption_proxy"] >= 0.68:
            proxy_reasons.append("回落后下影吸收明显，承接迹象增强")
        if pct["return_shock"] is not None and pct["return_shock"] >= 0.72:
            proxy_reasons.append("近 1/3/6 bar 收益冲击显著抬升")
        if pct["volume_burst"] is not None and pct["volume_burst"] >= 0.72:
            proxy_reasons.append(
                f"量能突增，volume burst={_to_float(item['metrics_raw']['volume_burst_ratio']):.2f}"
            )
        if pct["close_control"] is not None and pct["close_control"] >= 0.68:
            proxy_reasons.append("收盘位置持续贴近区间上沿，控盘痕迹偏强")
        if pct["impulse_after_compression"] is not None and pct["impulse_after_compression"] >= 0.68:
            proxy_reasons.append("压缩后存在定向冲击，疑似试盘/拉抬")
        if not proxy_reasons:
            proxy_reasons.append("代理行为证据一般，当前更多作为待跟踪候选")

        if pct["community_flow"] is not None and pct["community_flow"] >= 0.65:
            chain_reasons.append("community flow 快照偏正，确认分得到加成")
        if pct["announcements"] is not None and pct["announcements"] >= 0.65:
            chain_reasons.append("近期公告/外生事件较多，存在辅助确认")
        if pct["funding_basis"] is not None and pct["funding_basis"] >= 0.65:
            chain_reasons.append("资金费率/基差偏强，短期情绪支持启动")
        if pct["whale_context"] is not None and pct["whale_context"] >= 0.65:
            chain_reasons.append("巨鲸上下文活跃，提升候选确认度")
        if not chain_reasons:
            if row["data_quality"].get("chain_quality", 0.0) < 0.45:
                chain_reasons.append("链上/外生确认较弱，本次排序主要依赖量价代理行为")
            else:
                chain_reasons.append("链上/外生确认中性，没有把弱候选抬到榜首")

        if "security_event" in row["data_quality"].get("degraded_reason", []):
            proxy_reasons.append("存在安全事件惩罚，优先按警戒状态处理")
        if "spread_too_wide" in row["data_quality"].get("degraded_reason", []):
            proxy_reasons.append("盘口价差偏大，需警惕控盘与出货风险")
        if "snapshot_missing" in row["data_quality"].get("degraded_reason", []):
            chain_reasons.append("部分快照缺失，确认引擎已自动降权")

        row["reasons_proxy"] = proxy_reasons[:4]
        row["reasons_chain"] = chain_reasons[:4]
        rows.append(row)
    return rows


def _normalized_sparkline(values: Sequence[Any]) -> List[float]:
    numeric = [_to_float(value, 0.0) for value in values if value is not None]
    if not numeric:
        return []
    base = numeric[0] if abs(numeric[0]) > 1e-12 else 1.0
    return [round((value / base) * 100.0, 4) for value in numeric]


def build_detail_payload(
    *,
    rows: Sequence[Mapping[str, Any]],
    symbol: str,
    sort_by: str = "layout",
    onchain_context: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    normalized_symbol = str(symbol or "").strip().upper()
    ordered = sort_rows(rows, sort_by=sort_by)
    selected = next((dict(row) for row in ordered if str(row.get("symbol") or "").upper() == normalized_symbol), None)
    if selected is None:
        return {
            "selected_row": None,
            "proxy_breakdown": {},
            "chain_breakdown": {},
            "sparkline": [],
            "invalidate_conditions": [],
            "related_candidates": [],
        }
    metrics = selected.get("metrics") or {}
    percentiles = metrics.get("percentiles") or {}
    state = str(selected.get("signal_state") or "").strip()
    proxy_breakdown = {
        "engine": "代理行为引擎",
        "dominant_sort": sort_by,
        "scores": {
            "layout": selected.get("layout_score"),
            "alert": selected.get("alert_score"),
            "anomaly": selected.get("anomaly_score"),
            "accumulation": selected.get("accumulation_score"),
            "control": selected.get("control_score"),
        },
        "components": [
            {"label": "收益冲击", "pctile": percentiles.get("return_shock"), "weight": 0.45},
            {"label": "量能爆发", "pctile": percentiles.get("volume_burst"), "weight": 0.35},
            {"label": "真实波幅扩张", "pctile": percentiles.get("range_expansion"), "weight": 0.20},
            {"label": "波动压缩", "pctile": percentiles.get("compression_inverse"), "weight": 0.35},
            {"label": "路径稳定", "pctile": percentiles.get("drift_stability"), "weight": 0.25},
            {"label": "承接吸收", "pctile": percentiles.get("absorption_proxy"), "weight": 0.20},
            {"label": "收盘控制", "pctile": percentiles.get("close_control"), "weight": 0.30},
            {"label": "流动性稀薄", "pctile": percentiles.get("liquidity_thinness"), "weight": 0.25},
        ],
        "reasons": list(selected.get("reasons_proxy") or []),
    }
    chain_breakdown = {
        "engine": "链上/外生确认引擎",
        "score": selected.get("chain_confirmation_score"),
        "chain_quality": (selected.get("data_quality") or {}).get("chain_quality"),
        "components": [
            {"label": "community flow", "pctile": percentiles.get("community_flow"), "weight": 0.40},
            {"label": "announcements", "pctile": percentiles.get("announcements"), "weight": 0.25},
            {"label": "funding/basis", "pctile": percentiles.get("funding_basis"), "weight": 0.20},
            {"label": "whale context", "pctile": percentiles.get("whale_context"), "weight": 0.15},
        ],
        "reasons": list(selected.get("reasons_chain") or []),
        "onchain_context": dict(onchain_context or {}),
    }

    invalidate_conditions: List[str] = []
    if state == STATE_LAYOUT:
        invalidate_conditions.extend(
            [
                "4h 结构重新放量下破，且 accumulation_score 回落到 0.45 以下",
                "risk_penalty 抬升到 0.25 以上，布局优先级自动失效",
                "close_control 明显走弱，右侧承接不再成立",
            ]
        )
    elif state == STATE_ANOMALY:
        invalidate_conditions.extend(
            [
                "异动后无法站稳，下一轮回落吞没启动 K 线",
                "量能脉冲回落到币池中位以下，说明启动延续性不足",
                "链上/外生确认持续缺失，且 control_score 无法跟上",
            ]
        )
    elif state in {STATE_CONTROL_TRACK, STATE_CONTROL_WARN}:
        invalidate_conditions.extend(
            [
                "spread_bps 继续走阔，价差/流动性风险放大",
                "上影回落继续增加，派发迹象盖过拉抬迹象",
                "安全事件或快照过旧导致 risk_penalty 继续攀升",
            ]
        )
    elif state == STATE_DISTRIBUTION:
        invalidate_conditions.extend(
            [
                "若回踩后吸收重新建立，需重新评估是否由派发转回布局",
                "若异常量价无法延续，派发风险权重可下调",
                "若链上确认转正且 security risk 消退，可降级为高控盘跟踪",
            ]
        )
    else:
        invalidate_conditions.extend(
            [
                "当前候选未形成稳定主标签，等待下一次扫描确认",
                "若 accumulation/control 任一维度站上阈值，将进入正式预警视野",
            ]
        )

    related_candidates = []
    for row in ordered:
        if str(row.get("symbol") or "").upper() == normalized_symbol:
            continue
        related_state = str(row.get("signal_state") or "").strip()
        same_group = bool(state) and related_state == state
        if same_group or not related_candidates:
            related_candidates.append(
                {
                    "symbol": row.get("symbol"),
                    "signal_state": related_state,
                    "layout_score": row.get("layout_score"),
                    "alert_score": row.get("alert_score"),
                    "control_score": row.get("control_score"),
                    "rank": row.get("rank"),
                }
            )
        if len(related_candidates) >= 5:
            break

    return {
        "selected_row": selected,
        "proxy_breakdown": proxy_breakdown,
        "chain_breakdown": chain_breakdown,
        "sparkline": _normalized_sparkline(selected.get("sparkline") or []),
        "invalidate_conditions": invalidate_conditions,
        "related_candidates": related_candidates,
    }
