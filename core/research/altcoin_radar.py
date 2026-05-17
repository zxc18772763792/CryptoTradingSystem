"""Altcoin radar scoring and detail helpers."""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import numpy as np
import pandas as pd

from config.settings import settings
from core.data.coinglass_altcoin import is_alt_candidate_symbol
from core.research.altcoin_radar_perp import classify_signal_source, compute_perp_scores
from core.research.altcoin_radar_narrative import classify_narrative_source, compute_narrative_scores
from core.research.altcoin_radar_universe import get_sector, get_watchlist_symbols
from core.research.altcoin_radar_events import (
    bulk_update_ranks,
    compute_rank_jump_score,
    get_recent_symbol_events,
    get_rank_history,
    record_crowding_spike,
    record_ignition_cross_up,
    record_narrative_heat_spike,
    record_rank_jump_event,
)


VALID_TIMEFRAMES = {"15m", "1h", "4h", "1d"}
TIMEFRAME_SECONDS = {"15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}
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


def _funding_basis_value(
    micro_snapshot: Mapping[str, Any],
    derivatives_snapshot: Optional[Mapping[str, Any]] = None,
    market_snapshot: Optional[Mapping[str, Any]] = None,
) -> Optional[float]:
    funding_rate = 0.0
    basis_pct = 0.0

    if micro_snapshot:
        funding = micro_snapshot.get("funding_rate") or {}
        basis = micro_snapshot.get("spot_futures_basis") or {}
        funding_rate = max(_to_float(funding.get("funding_rate"), 0.0), funding_rate)
        basis_pct = max(_to_float(basis.get("basis_pct"), 0.0), basis_pct)

    if derivatives_snapshot:
        funding_rate = max(_to_float(derivatives_snapshot.get("funding_rate"), 0.0), funding_rate)
        basis_pct = max(_to_float(derivatives_snapshot.get("basis_pct"), 0.0), basis_pct)
        derivatives_payload = _snapshot_payload(derivatives_snapshot)
        funding_rate = max(_to_float(derivatives_payload.get("funding_mean"), 0.0), funding_rate)

    if market_snapshot:
        funding_rate = max(_to_float(market_snapshot.get("avg_funding_rate_by_oi"), 0.0), funding_rate)
        basis_pct = max(_to_float(market_snapshot.get("oi_vol_ratio_change_percent_4h"), 0.0) / 100.0, basis_pct)

    if funding_rate <= 0.0 and basis_pct <= 0.0:
        return None
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


def _pair_base(symbol: str) -> str:
    return str(symbol or "").strip().upper().split("/", 1)[0]


def _pct_fraction(value: Any) -> float:
    return _to_float(value, 0.0) / 100.0


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, float(value)))


def _timeframe_window_map(timeframe: str) -> Dict[str, str]:
    tf = str(timeframe or "4h").strip().lower()
    if tf == "15m":
        return {"1": "1h", "3": "4h", "6": "12h", "flow": "1h", "liq": "1h", "oi": "1h", "volume": "1h"}
    if tf == "1h":
        return {"1": "1h", "3": "4h", "6": "12h", "flow": "1h", "liq": "1h", "oi": "1h", "volume": "1h"}
    if tf == "1d":
        return {"1": "24h", "3": "24h", "6": "24h", "flow": "24h", "liq": "24h", "oi": "24h", "volume": "24h"}
    return {"1": "4h", "3": "12h", "6": "24h", "flow": "4h", "liq": "4h", "oi": "4h", "volume": "4h"}


def _market_snapshot_flow_imbalance(snapshot: Mapping[str, Any], window: str) -> float:
    long_volume = _to_float(snapshot.get(f"long_volume_usd_{window}"), 0.0)
    short_volume = _to_float(snapshot.get(f"short_volume_usd_{window}"), 0.0)
    total = long_volume + short_volume
    if total <= 0:
        return 0.0
    return (long_volume - short_volume) / total


def _synthetic_sparkline(snapshot: Mapping[str, Any], timeframe: str) -> List[float]:
    current_price = max(_to_float(snapshot.get("current_price"), 0.0), 1e-6)
    windows = _timeframe_window_map(timeframe)
    ret1 = _pct_fraction(snapshot.get(f"price_change_percent_{windows['1']}"))
    ret3 = _pct_fraction(snapshot.get(f"price_change_percent_{windows['3']}"))
    ret6 = _pct_fraction(snapshot.get(f"price_change_percent_{windows['6']}"))

    def _backsolve(price: float, change: float) -> float:
        return price / max(1.0 + change, 1e-6)

    p6 = _backsolve(current_price, ret6)
    p3 = _backsolve(current_price, ret3)
    p1 = _backsolve(current_price, ret1)
    mid_a = p6 + (p3 - p6) * 0.5
    mid_b = p3 + (p1 - p3) * 0.5
    return [round(value, 4) for value in [p6, mid_a, p3, mid_b, p1, current_price]]


def _market_snapshot_metrics(
    snapshot: Mapping[str, Any],
    *,
    timeframe: str,
    now: datetime,
) -> Dict[str, Any]:
    windows = _timeframe_window_map(timeframe)
    ret1 = _pct_fraction(snapshot.get(f"price_change_percent_{windows['1']}"))
    ret3 = _pct_fraction(snapshot.get(f"price_change_percent_{windows['3']}"))
    ret6 = _pct_fraction(snapshot.get(f"price_change_percent_{windows['6']}"))
    volume_change = _to_float(snapshot.get(f"volume_change_percent_{windows['volume']}"), 0.0)
    flow_window = windows["flow"]
    liq_window = windows["liq"]
    oi_window = windows["oi"]

    imbalance = _market_snapshot_flow_imbalance(snapshot, flow_window)
    long_liq = _to_float(snapshot.get(f"long_liquidation_usd_{liq_window}"), 0.0)
    short_liq = _to_float(snapshot.get(f"short_liquidation_usd_{liq_window}"), 0.0)
    liq_total = long_liq + short_liq
    short_liq_share = short_liq / max(liq_total, 1.0)
    liq_ratio = liq_total / max(
        _to_float(snapshot.get(f"long_volume_usd_{flow_window}"), 0.0)
        + _to_float(snapshot.get(f"short_volume_usd_{flow_window}"), 0.0),
        1.0,
    )
    oi_change = _to_float(snapshot.get(f"open_interest_change_percent_{oi_window}"), 0.0)
    ret_path = abs(ret1) + abs(ret3) + abs(ret6) + 1e-6
    positive_trend = max(ret6, 0.0)

    drift_stability = positive_trend / ret_path
    absorption = _clamp(max(imbalance, 0.0) * 0.6 + short_liq_share * 0.4, 0.0, 1.5)
    breakout_proximity = _clamp(0.5 + ret3 * 5.0 + max(ret1, 0.0) * 2.0, 0.0, 1.2)
    close_control = _clamp(0.5 + max(ret1, 0.0) * 6.0 + max(imbalance, 0.0) * 0.35 + max(oi_change, 0.0) / 25.0, 0.0, 1.2)
    range_expansion_ratio = 1.0 + min((abs(ret1) * 18.0) + (liq_ratio * 4.0), 3.0)
    recent_vol = abs(ret1 - (ret3 / 3.0)) + abs(ret3 - (ret6 / 2.0))
    impulse = max(ret1, 0.0) / max(abs(ret6), 0.01)
    avg_dollar_volume = (
        _to_float(snapshot.get(f"long_volume_usd_{flow_window}"), 0.0)
        + _to_float(snapshot.get(f"short_volume_usd_{flow_window}"), 0.0)
    )

    timestamp = snapshot.get("timestamp") or _utcnow().isoformat()
    age_sec = _age_seconds(timestamp, now) or 0.0
    freshness = _freshness_score(age_sec, TIMEFRAME_SECONDS.get(timeframe, TIMEFRAME_SECONDS["4h"]), 4.0)
    return {
        "last_price": _to_float(snapshot.get("current_price"), 0.0),
        "return_1_bar": ret1,
        "return_3_bar": ret3,
        "return_6_bar": ret6,
        "volume_burst_ratio": max(0.15, 1.0 + (volume_change / 100.0)),
        "range_expansion_ratio": range_expansion_ratio,
        "compression_volatility": recent_vol,
        "drift_stability": drift_stability,
        "absorption_proxy": absorption,
        "breakout_proximity": breakout_proximity,
        "close_control": close_control,
        "avg_dollar_volume": avg_dollar_volume,
        "spread_bps": 0.0,
        "order_flow_imbalance": imbalance,
        "market_age_sec": age_sec,
        "market_freshness": freshness,
        "market_as_of": timestamp,
        "sparkline": _synthetic_sparkline(snapshot, timeframe),
        "impulse_after_compression": impulse,
        "market_cap_usd": _to_float(snapshot.get("market_cap_usd"), 0.0),
        "source_name": snapshot.get("source_name"),
    }


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
    # Phase 1 new sort keys
    if normalized == "ignition":
        return _to_float(row.get("ignition_score"), 0.0)
    if normalized == "continuation":
        return _to_float(row.get("continuation_score"), 0.0)
    if normalized == "rank_jump":
        return _to_float(row.get("rank_jump_score"), 0.0)
    if normalized == "crowding":
        return _to_float(row.get("crowding_late_score"), 0.0)
    # Phase 2 new sort keys
    if normalized == "narrative":
        return _to_float(row.get("narrative_heat_score"), 0.0)
    if normalized == "meme_rotation":
        return _to_float(row.get("meme_rotation_score"), 0.0)
    if normalized == "chain":
        return _to_float(row.get("chain_confirmation_score"), 0.0)
    if normalized == "heat":
        return (
            _to_float(row.get("layout_score"), 0.0) * 0.25
            + _to_float(row.get("alert_score"), 0.0) * 0.20
            + _to_float(row.get("control_score"), 0.0) * 0.15
            + _to_float(row.get("chain_confirmation_score"), 0.0) * 0.10
            + _to_float(row.get("derivatives_heat_score"), 0.0) * 0.20
            + _to_float(row.get("flow_confirmation_score"), 0.0) * 0.10
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
    if not bool(row.get("alt_eligible", True)):
        return ""
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
    market_snapshots: Optional[Mapping[str, Mapping[str, Any]]] = None,
    micro_snapshots: Optional[Mapping[str, Mapping[str, Any]]] = None,
    community_snapshots: Optional[Mapping[str, Mapping[str, Any]]] = None,
    whale_snapshots: Optional[Mapping[str, Mapping[str, Any]]] = None,
    derivatives_snapshots: Optional[Mapping[str, Mapping[str, Any]]] = None,
    alerted_symbols: Optional[Iterable[str]] = None,
    now: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    current = now or _utcnow()
    tf = str(timeframe or "4h").lower()
    if tf not in VALID_TIMEFRAMES:
        tf = "4h"
    factor_payload = dict(factor_library or {})
    multi_payload = dict(multi_assets or {})
    # Phase 2: pre-compute watchlist set for O(1) membership test per symbol
    _watchlist_set = set(get_watchlist_symbols())
    alerted = {str(symbol or "").strip().upper() for symbol in (alerted_symbols or []) if str(symbol or "").strip()}
    market_snapshot_map = {str(k).upper(): dict(v or {}) for k, v in (market_snapshots or {}).items()}
    micro_map = {str(k).upper(): dict(v or {}) for k, v in (micro_snapshots or {}).items()}
    community_map = {str(k).upper(): dict(v or {}) for k, v in (community_snapshots or {}).items()}
    whale_map = {str(k).upper(): dict(v or {}) for k, v in (whale_snapshots or {}).items()}
    derivatives_map = {str(k).upper(): dict(v or {}) for k, v in (derivatives_snapshots or {}).items()}
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
        "derivatives_heat": {},
        "squeeze_signal": {},
        "crowding_risk": {},
        "liquidity_trap": {},
        "flow_confirmation": {},
    }

    interim: Dict[str, Dict[str, Any]] = {}
    all_symbols = _normalize_symbols(list(market_frames.keys()) + list(market_snapshot_map.keys()))
    for normalized_symbol in all_symbols:
        if not normalized_symbol:
            continue
        frame = market_frames.get(normalized_symbol)
        df = frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
        market_snapshot = dict(market_snapshot_map.get(normalized_symbol) or {})
        if (df.empty or "close" not in df.columns) and not market_snapshot:
            continue
        if not df.empty and "close" in df.columns:
            df = df.sort_index().tail(180)
        close = _safe_series(df, "close")
        volume = _safe_series(df, "volume")
        micro = dict(micro_map.get(normalized_symbol) or {})
        community = dict(community_map.get(normalized_symbol) or {})
        whale = dict(whale_map.get(normalized_symbol) or {})
        derivatives = dict(derivatives_map.get(normalized_symbol) or {})
        factor_row = dict(factor_rows.get(normalized_symbol) or {})
        multi_row = dict(multi_rows.get(normalized_symbol) or {})
        coinglass_market_metrics = _market_snapshot_metrics(market_snapshot, timeframe=tf, now=current) if market_snapshot else {}
        local_market_age_sec = _age_seconds(df.index[-1], current) if not df.empty else None
        local_market_freshness = _freshness_score(local_market_age_sec, expected_bar_sec, hard_cap_multiple=4.0) if local_market_age_sec is not None else 0.0
        use_market_snapshot = bool(
            market_snapshot and (close.empty or local_market_freshness < 0.45)
        )
        if close.empty and not use_market_snapshot:
            continue
        derivatives_age_sec = _age_seconds(derivatives.get("timestamp"), current)
        snapshot_ages = [
            age
            for age in (
                _age_seconds(micro.get("timestamp"), current),
                _age_seconds(community.get("timestamp"), current),
                _age_seconds(whale.get("timestamp"), current),
                derivatives_age_sec,
                _age_seconds(market_snapshot.get("timestamp"), current) if market_snapshot else None,
            )
            if age is not None
        ]
        snapshot_age_sec = (sum(snapshot_ages) / len(snapshot_ages)) if snapshot_ages else None
        market_age_sec = (
            coinglass_market_metrics.get("market_age_sec")
            if use_market_snapshot
            else local_market_age_sec
        )
        market_freshness = (
            _to_float(coinglass_market_metrics.get("market_freshness"), 0.0)
            if use_market_snapshot
            else local_market_freshness
        )
        snapshot_freshness = _freshness_score(snapshot_age_sec, expected_bar_sec * 2.0, hard_cap_multiple=6.0)
        derivatives_freshness = _freshness_score(
            derivatives_age_sec,
            expected_bar_sec * 2.0,
            hard_cap_multiple=6.0,
        )
        available_snapshots = sum(1 for snapshot in (micro, community, whale) if snapshot)
        chain_quality = _clamp01((available_snapshots / 3.0) * 0.45 + snapshot_freshness * 0.55)

        if use_market_snapshot:
            recent_range_ratio = _to_float(coinglass_market_metrics.get("range_expansion_ratio"), 0.0)
            recent_vol = _to_float(coinglass_market_metrics.get("compression_volatility"), 0.0)
            recent_return_1 = _to_float(coinglass_market_metrics.get("return_1_bar"), 0.0)
            recent_return_3 = _to_float(coinglass_market_metrics.get("return_3_bar"), 0.0)
            recent_return_6 = _to_float(coinglass_market_metrics.get("return_6_bar"), 0.0)
            volume_burst = _to_float(coinglass_market_metrics.get("volume_burst_ratio"), 0.0)
            drift_stability = _to_float(coinglass_market_metrics.get("drift_stability"), 0.0)
            absorption = _to_float(coinglass_market_metrics.get("absorption_proxy"), 0.0)
            breakout_proximity = _to_float(coinglass_market_metrics.get("breakout_proximity"), 0.0)
            close_control = _to_float(coinglass_market_metrics.get("close_control"), 0.0)
            impulse = _to_float(coinglass_market_metrics.get("impulse_after_compression"), 0.0)
            avg_dollar_volume = _to_float(coinglass_market_metrics.get("avg_dollar_volume"), 0.0)
            spread_bps = _to_float(coinglass_market_metrics.get("spread_bps"), 0.0)
            last_price = _to_float(coinglass_market_metrics.get("last_price"), 0.0)
            sparkline = list(coinglass_market_metrics.get("sparkline") or [])
        else:
            recent_range_ratio = _avg_true_range_ratio(df)
            recent_vol = _rolling_return_volatility(close)
            recent_return_1 = _pct_change(close, 1)
            recent_return_3 = _pct_change(close, 3)
            recent_return_6 = _pct_change(close, 6)
            volume_burst = _volume_ratio(volume)
            drift_stability = _drift_stability(close)
            absorption = _absorption_proxy(df)
            breakout_proximity = _breakout_proximity(close)
            close_control = _close_control(df)
            impulse = _impulse_after_compression(close)
            avg_dollar_volume = _to_float((close.tail(24) * volume.tail(24)).mean(), 0.0)
            spread_bps = _to_float((micro.get("orderbook") or {}).get("spread_bps"), 0.0)
            last_price = _to_float(close.iloc[-1], 0.0)
            sparkline = close.tail(36).tolist()
        positive_return_burst = max(recent_return_1, recent_return_3, recent_return_6, 0.0)
        absolute_return_burst = max(abs(recent_return_1), abs(recent_return_3), abs(recent_return_6))
        micro_payload = _snapshot_payload(micro)
        community_payload = _snapshot_payload(community)
        whale_payload = _snapshot_payload(whale)
        derivatives_payload = _snapshot_payload(derivatives)
        derivatives_labels = [
            str(item).strip()
            for item in list(derivatives_payload.get("derivatives_labels") or [])
            if str(item).strip()
        ]
        history_ready = bool(derivatives_payload.get("history_ready"))
        crowded_long = bool(derivatives_payload.get("crowded_long"))
        crowded_short = bool(derivatives_payload.get("crowded_short"))
        squeeze_building = bool(derivatives_payload.get("squeeze_building"))
        flush_risk = bool(derivatives_payload.get("flush_risk"))
        basis_dislocation = bool(derivatives_payload.get("basis_dislocation"))
        flow_divergence = bool(derivatives_payload.get("flow_divergence"))
        order_flow_confirmed = bool(derivatives_payload.get("order_flow_confirmed"))
        orderbook = micro.get("orderbook") or {}
        spread_bps = max(spread_bps, _to_float(orderbook.get("spread_bps"), 0.0))
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
        funding_basis = _funding_basis_value(
            micro,
            derivatives_snapshot=derivatives,
            market_snapshot=market_snapshot,
        )
        whale_context = _whale_context_value(whale)
        derivatives_heat = max(
            _to_float(derivatives.get("crowding_score"), 0.0),
            _to_float(derivatives.get("squeeze_score"), 0.0),
        )
        squeeze_signal = _to_float(derivatives.get("squeeze_score"), 0.0)
        crowding_risk = max(
            _to_float(derivatives.get("crowding_score"), 0.0),
            _to_float(derivatives.get("distribution_score"), 0.0),
        )
        liquidity_trap = max(
            _to_float(derivatives.get("depth_thinness_score"), 0.0),
            _to_float(derivatives.get("distribution_score"), 0.0) * 0.6,
            _to_float(derivatives_payload.get("liquidity_void_score"), 0.0),
            _to_float(derivatives_payload.get("heatmap_pressure_score"), 0.0) * 0.5,
        )
        flow_confirmation = _clamp01(
            max(_to_float(derivatives.get("taker_buy_sell_imbalance"), 0.0), 0.0) * 0.5
            + max(_to_float(derivatives.get("oi_change_1h"), 0.0), 0.0) / 20.0
            + _to_float(community_flow or 0.0) * 0.2
            + max(-_to_float(derivatives_payload.get("spot_netflow_score"), 0.0), 0.0) * 0.2
        )
        derivatives_heat = max(derivatives_heat, _to_float(derivatives_payload.get("derivatives_heat_score"), 0.0))
        if squeeze_building:
            squeeze_signal = max(squeeze_signal, 0.72)
        if order_flow_confirmed:
            flow_confirmation = max(flow_confirmation, 0.78)

        security_events = _security_event_value(community)
        missing_count = 3 - available_snapshots
        stale_data = (1.0 - market_freshness) + (1.0 - snapshot_freshness)
        liquidity_risk = spread_bps + (1.0 / max(avg_dollar_volume, 1.0)) * 1_000_000.0

        degraded_reason: List[str] = []
        if market_freshness < 0.45:
            degraded_reason.append("market_data_stale")
        if snapshot_freshness < 0.45 and not market_snapshot:
            degraded_reason.append("snapshot_stale")
        if missing_count > 0 and not market_snapshot:
            degraded_reason.append("snapshot_missing")
        if spread_bps >= 30:
            degraded_reason.append("spread_too_wide")
        if avg_dollar_volume > 0 and avg_dollar_volume < 1_000_000:
            degraded_reason.append("liquidity_thin")
        if security_events > 0:
            degraded_reason.append("security_event")
        if bool(getattr(settings, "COINGLASS_INCLUDE_RADAR", True)) and not derivatives:
            degraded_reason.append("derivatives_missing")

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
        raw_components["snapshot_missing"][normalized_symbol] = 0.0 if market_snapshot else float(max(missing_count, 0))
        raw_components["derivatives_heat"][normalized_symbol] = derivatives_heat
        raw_components["squeeze_signal"][normalized_symbol] = squeeze_signal
        raw_components["crowding_risk"][normalized_symbol] = crowding_risk
        raw_components["liquidity_trap"][normalized_symbol] = liquidity_trap
        raw_components["flow_confirmation"][normalized_symbol] = flow_confirmation

        interim[normalized_symbol] = {
            "symbol": normalized_symbol,
            "factor_row": factor_row,
            "multi_row": multi_row,
            "market_snapshot": market_snapshot,
            "micro": micro,
            "community": community,
            "whale": whale,
            "derivatives": derivatives,
            "metrics_raw": {
                "last_price": last_price,
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
                "order_flow_imbalance": _to_float(
                    (micro.get("aggressor_flow") or {}).get("imbalance"),
                    _to_float(coinglass_market_metrics.get("order_flow_imbalance"), 0.0),
                ),
                "community_flow_imbalance": _to_float((community.get("flow_proxy") or {}).get("imbalance"), 0.0),
                "announcement_count": _to_float((community_payload.get("announcement_count") or 0), 0.0)
                or _to_float(len(community.get("announcements") or []), 0.0),
                "whale_count": _to_float(whale.get("count"), 0.0),
                "derivatives_heat_score": derivatives_heat,
                "squeeze_score": squeeze_signal,
                "crowding_risk_score": crowding_risk,
                "liquidity_trap_score": liquidity_trap,
                "flow_confirmation_score": flow_confirmation,
                "oi_change_1h": _to_float(derivatives.get("oi_change_1h"), 0.0),
                "funding_rate": _to_float(derivatives.get("funding_rate"), 0.0),
                "funding_mean": _to_float(derivatives_payload.get("funding_mean"), 0.0),
                "funding_zscore": _to_float(derivatives_payload.get("funding_zscore"), 0.0),
                "funding_reversion_speed": _to_float(derivatives_payload.get("funding_reversion_speed"), 0.0),
                "basis_pct": _to_float(derivatives.get("basis_pct"), 0.0),
                "basis_dislocation_score": _to_float(derivatives_payload.get("basis_dislocation_score"), 0.0),
                "flow_divergence_score": _to_float(derivatives_payload.get("flow_divergence_score"), 0.0),
                "long_short_ratio": _to_float(derivatives.get("long_short_ratio"), 0.0),
                "long_short_ratio_change_24h": _to_float(derivatives_payload.get("long_short_ratio_change_24h"), 0.0),
                "taker_buy_sell_imbalance": _to_float(derivatives.get("taker_buy_sell_imbalance"), 0.0),
                "liquidation_burst_score": _to_float(derivatives_payload.get("liquidation_burst_score"), 0.0),
                "liquidation_map_pressure_score": _to_float(derivatives_payload.get("liquidation_map_pressure_score"), 0.0),
                "liquidation_map_total_usd": _to_float(derivatives_payload.get("liquidation_map_total_usd"), 0.0),
                "liquidation_map_above_usd": _to_float(derivatives_payload.get("liquidation_map_above_usd"), 0.0),
                "liquidation_map_below_usd": _to_float(derivatives_payload.get("liquidation_map_below_usd"), 0.0),
                "liquidation_map_largest_cluster_price": _to_float(derivatives_payload.get("liquidation_map_largest_cluster_price"), 0.0),
                "orderbook_agg_bid_usd": _to_float(derivatives_payload.get("orderbook_agg_bid_usd"), 0.0),
                "orderbook_agg_ask_usd": _to_float(derivatives_payload.get("orderbook_agg_ask_usd"), 0.0),
                "orderbook_agg_imbalance": _to_float(derivatives_payload.get("orderbook_agg_imbalance"), 0.0),
                "orderbook_wall_above_usd": _to_float(derivatives_payload.get("orderbook_wall_above_usd"), 0.0),
                "orderbook_wall_below_usd": _to_float(derivatives_payload.get("orderbook_wall_below_usd"), 0.0),
                "liquidity_heatmap_total_usd": _to_float(derivatives_payload.get("liquidity_heatmap_total_usd"), 0.0),
                "liquidity_heatmap_above_usd": _to_float(derivatives_payload.get("liquidity_heatmap_above_usd"), 0.0),
                "liquidity_heatmap_below_usd": _to_float(derivatives_payload.get("liquidity_heatmap_below_usd"), 0.0),
                "liquidity_void_score": _to_float(derivatives_payload.get("liquidity_void_score"), 0.0),
                "heatmap_pressure_score": _to_float(derivatives_payload.get("heatmap_pressure_score"), 0.0),
                "spot_exchange_netflow_usd": _to_float(derivatives_payload.get("spot_exchange_netflow_usd"), 0.0),
                "spot_netflow_score": _to_float(derivatives_payload.get("spot_netflow_score"), 0.0),
                "exchange_balance_change_24h": _to_float(derivatives_payload.get("exchange_balance_change_24h"), 0.0),
                "exchange_reserve_pressure_score": _to_float(derivatives_payload.get("exchange_reserve_pressure_score"), 0.0),
                "onchain_activity_score": _to_float(derivatives_payload.get("onchain_activity_score"), 0.0),
                "option_put_call_ratio": _to_float(derivatives_payload.get("option_put_call_ratio"), 0.0),
                "option_iv_skew": _to_float(derivatives_payload.get("option_iv_skew"), 0.0),
                "crowding_score": _to_float(derivatives.get("crowding_score"), 0.0),
                "distribution_score": _to_float(derivatives.get("distribution_score"), 0.0),
                "depth_thinness_score": _to_float(derivatives.get("depth_thinness_score"), 0.0),
                "orderbook_imbalance_score": _to_float(derivatives.get("orderbook_imbalance_score"), 0.0),
                "history_ready": 1.0 if history_ready else 0.0,
                "btc_correlation": btc_corr,
                "factor_liquidity": factor_liquidity,
                "factor_low_beta": _to_float(factor_row.get("low_beta"), 0.0),
                "factor_low_vol": _to_float(factor_row.get("low_vol"), 0.0),
                "market_cap_usd": _to_float(
                    (derivatives_payload.get("market_cap_usd"))
                    or coinglass_market_metrics.get("market_cap_usd")
                    or market_snapshot.get("market_cap_usd"),
                    0.0,
                ),
            },
            "freshness": {
                "as_of": (
                    coinglass_market_metrics.get("market_as_of")
                    if use_market_snapshot
                    else df.index[-1].isoformat() if not df.empty and hasattr(df.index[-1], "isoformat") else str(df.index[-1]) if not df.empty else market_snapshot.get("timestamp")
                ),
                "market_data_age_sec": None if market_age_sec is None else round(market_age_sec, 2),
                "snapshot_age_sec": None if snapshot_age_sec is None else round(snapshot_age_sec, 2),
                "derivatives_age_sec": None if derivatives_age_sec is None else round(derivatives_age_sec, 2),
                "market_label": "fresh" if market_freshness >= 0.7 else "watch" if market_freshness >= 0.45 else "stale",
                "snapshot_label": "fresh"
                if snapshot_freshness >= 0.7
                else "watch"
                if snapshot_freshness >= 0.45
                else "stale",
                "derivatives_label": "missing"
                if not derivatives
                else "fresh"
                if derivatives_freshness >= 0.7
                else "watch"
                if derivatives_freshness >= 0.45
                else "stale",
            },
            "data_quality": {
                "market_data_freshness": _round4(market_freshness),
                "snapshot_freshness": _round4(snapshot_freshness),
                "derivatives_data_freshness": _round4(derivatives_freshness),
                "chain_quality": _round4(chain_quality),
                "derivatives_present": bool(derivatives),
                "degraded_reason": degraded_reason,
            },
            "derivatives_context": {
                "available": bool(derivatives),
                "timestamp": derivatives.get("timestamp"),
                "age_sec": None if derivatives_age_sec is None else round(derivatives_age_sec, 2),
                "freshness_label": "missing"
                if not derivatives
                else "fresh"
                if derivatives_freshness >= 0.7
                else "watch"
                if derivatives_freshness >= 0.45
                else "stale",
                "source_name": derivatives.get("source_name"),
                "capture_status": derivatives.get("capture_status"),
                "source_error": derivatives.get("source_error"),
                "history_ready": history_ready,
                "history_exchange": derivatives_payload.get("history_exchange"),
                "history_interval": derivatives_payload.get("history_interval"),
                "funding_zscore": derivatives_payload.get("funding_zscore"),
                "funding_reversion_speed": derivatives_payload.get("funding_reversion_speed"),
                "long_short_ratio_change_24h": derivatives_payload.get("long_short_ratio_change_24h"),
                "liquidation_burst_score": derivatives_payload.get("liquidation_burst_score"),
                "liquidation_map_pressure_score": derivatives_payload.get("liquidation_map_pressure_score"),
                "liquidation_map_total_usd": derivatives_payload.get("liquidation_map_total_usd"),
                "liquidation_map_above_usd": derivatives_payload.get("liquidation_map_above_usd"),
                "liquidation_map_below_usd": derivatives_payload.get("liquidation_map_below_usd"),
                "liquidation_map_largest_cluster_price": derivatives_payload.get("liquidation_map_largest_cluster_price"),
                "liquidation_map_largest_cluster_usd": derivatives_payload.get("liquidation_map_largest_cluster_usd"),
                "orderbook_agg_bid_usd": derivatives_payload.get("orderbook_agg_bid_usd"),
                "orderbook_agg_ask_usd": derivatives_payload.get("orderbook_agg_ask_usd"),
                "orderbook_agg_imbalance": derivatives_payload.get("orderbook_agg_imbalance"),
                "orderbook_wall_above_usd": derivatives_payload.get("orderbook_wall_above_usd"),
                "orderbook_wall_below_usd": derivatives_payload.get("orderbook_wall_below_usd"),
                "orderbook_wall_above_price": derivatives_payload.get("orderbook_wall_above_price"),
                "orderbook_wall_below_price": derivatives_payload.get("orderbook_wall_below_price"),
                "liquidity_heatmap_total_usd": derivatives_payload.get("liquidity_heatmap_total_usd"),
                "liquidity_heatmap_above_usd": derivatives_payload.get("liquidity_heatmap_above_usd"),
                "liquidity_heatmap_below_usd": derivatives_payload.get("liquidity_heatmap_below_usd"),
                "liquidity_wall_nearest_above_price": derivatives_payload.get("liquidity_wall_nearest_above_price"),
                "liquidity_wall_nearest_below_price": derivatives_payload.get("liquidity_wall_nearest_below_price"),
                "liquidity_wall_nearest_above_usd": derivatives_payload.get("liquidity_wall_nearest_above_usd"),
                "liquidity_wall_nearest_below_usd": derivatives_payload.get("liquidity_wall_nearest_below_usd"),
                "liquidity_void_score": derivatives_payload.get("liquidity_void_score"),
                "heatmap_pressure_score": derivatives_payload.get("heatmap_pressure_score"),
                "spot_exchange_inflow_usd": derivatives_payload.get("spot_exchange_inflow_usd"),
                "spot_exchange_outflow_usd": derivatives_payload.get("spot_exchange_outflow_usd"),
                "spot_exchange_netflow_usd": derivatives_payload.get("spot_exchange_netflow_usd"),
                "spot_netflow_score": derivatives_payload.get("spot_netflow_score"),
                "exchange_flow_pressure": derivatives_payload.get("exchange_flow_pressure"),
                "exchange_balance_btc": derivatives_payload.get("exchange_balance_btc"),
                "exchange_balance_usd": derivatives_payload.get("exchange_balance_usd"),
                "exchange_balance_change_24h": derivatives_payload.get("exchange_balance_change_24h"),
                "exchange_balance_change_7d": derivatives_payload.get("exchange_balance_change_7d"),
                "stablecoin_exchange_balance_usd": derivatives_payload.get("stablecoin_exchange_balance_usd"),
                "stablecoin_netflow_usd": derivatives_payload.get("stablecoin_netflow_usd"),
                "exchange_reserve_pressure_score": derivatives_payload.get("exchange_reserve_pressure_score"),
                "onchain_activity_score": derivatives_payload.get("onchain_activity_score"),
                "option_max_pain": derivatives_payload.get("option_max_pain"),
                "option_put_call_ratio": derivatives_payload.get("option_put_call_ratio"),
                "option_open_interest_usd": derivatives_payload.get("option_open_interest_usd"),
                "option_volume_usd": derivatives_payload.get("option_volume_usd"),
                "option_iv": derivatives_payload.get("option_iv"),
                "option_iv_skew": derivatives_payload.get("option_iv_skew"),
                "option_distance_to_max_pain_pct": derivatives_payload.get("option_distance_to_max_pain_pct"),
                "derivatives_heat_score": round(derivatives_heat, 4),
                "crowded_long": crowded_long,
                "crowded_short": crowded_short,
                "squeeze_building": squeeze_building,
                "flush_risk": flush_risk,
                "basis_dislocation": basis_dislocation,
                "flow_divergence": flow_divergence,
                "order_flow_confirmed": order_flow_confirmed,
                "derivatives_labels": derivatives_labels,
            },
            "sparkline": sparkline,
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
        derivatives_heat_score = _weighted_score(
            {
                "derivatives_heat": pct["derivatives_heat"],
                "squeeze_signal": pct["squeeze_signal"],
            },
            {
                "derivatives_heat": 0.55,
                "squeeze_signal": 0.45,
            },
        )
        crowding_risk_score = _weighted_score(
            {
                "crowding_risk": pct["crowding_risk"],
            },
            {
                "crowding_risk": 1.0,
            },
        )
        liquidity_trap_score = _weighted_score(
            {
                "liquidity_trap": pct["liquidity_trap"],
                "liquidity_risk": pct["liquidity_risk"],
            },
            {
                "liquidity_trap": 0.65,
                "liquidity_risk": 0.35,
            },
        )
        flow_confirmation_score = _weighted_score(
            {
                "flow_confirmation": pct["flow_confirmation"],
                "community_flow": pct["community_flow"],
                "whale_context": pct["whale_context"],
            },
            {
                "flow_confirmation": 0.45,
                "community_flow": 0.35,
                "whale_context": 0.20,
            },
        )
        risk_penalty = _weighted_score(
            {
                "security_events": pct["security_events"],
                "stale_data": pct["stale_data"],
                "liquidity_risk": pct["liquidity_risk"],
                "snapshot_missing": pct["snapshot_missing"],
                "crowding_risk": pct["crowding_risk"],
                "liquidity_trap": pct["liquidity_trap"],
            },
            {
                "security_events": 0.25,
                "stale_data": 0.20,
                "liquidity_risk": 0.15,
                "snapshot_missing": 0.10,
                "crowding_risk": 0.20,
                "liquidity_trap": 0.10,
            },
        )
        market_cap_usd = _to_float(item["metrics_raw"].get("market_cap_usd"), 0.0)
        alt_eligible = is_alt_candidate_symbol(symbol, market_cap_usd if market_cap_usd > 0 else None)
        layout_score = (
            accumulation_score * 0.45
            + control_score * 0.30
            + anomaly_score * 0.15
            + chain_confirmation_score * 0.10
            + derivatives_heat_score * 0.12
            + flow_confirmation_score * 0.08
            - risk_penalty
        )
        alert_score = (
            anomaly_score * 0.55
            + accumulation_score * 0.20
            + control_score * 0.15
            + chain_confirmation_score * 0.10
            + squeeze_signal * 0.08
            - risk_penalty
        )
        if not alt_eligible:
            control_score = min(control_score, 0.35)
            accumulation_score = min(accumulation_score, 0.35)
            layout_score = min(layout_score, 0.30)
            alert_score = min(alert_score, 0.45)

        # Phase 1: perp-specific scores
        perp_scores = compute_perp_scores(
            metrics_raw=item["metrics_raw"],
            pct=pct,
            derivatives=item["derivatives"],
        )
        ignition_score = perp_scores["ignition_score"]
        continuation_score = perp_scores["continuation_score"]
        crowding_late_score = perp_scores["crowding_late_score"]

        # Phase 2: narrative-specific scores
        sym_sector = get_sector(symbol)
        in_watchlist = symbol in _watchlist_set
        narrative_sc = compute_narrative_scores(
            metrics_raw=item["metrics_raw"],
            pct=pct,
            sector=sym_sector,
            in_watchlist=in_watchlist,
        )
        narrative_heat_score = narrative_sc["narrative_heat_score"]
        meme_rotation_score = narrative_sc["meme_rotation_score"]

        row = {
            "symbol": symbol,
            "layout_score": _round4(_clamp01(layout_score)),
            "alert_score": _round4(_clamp01(alert_score)),
            "anomaly_score": _round4(_clamp01(anomaly_score)),
            "accumulation_score": _round4(_clamp01(accumulation_score)),
            "control_score": _round4(_clamp01(control_score)),
            "chain_confirmation_score": _round4(_clamp01(chain_confirmation_score)),
            "derivatives_heat_score": _round4(_clamp01(derivatives_heat_score)),
            "squeeze_score": _round4(_clamp01(squeeze_signal)),
            "crowding_risk_score": _round4(_clamp01(crowding_risk_score)),
            "liquidity_trap_score": _round4(_clamp01(liquidity_trap_score)),
            "flow_confirmation_score": _round4(_clamp01(flow_confirmation_score)),
            "risk_penalty": _round4(_clamp01(risk_penalty)),
            # Phase 1 new scores
            "ignition_score": ignition_score,
            "continuation_score": continuation_score,
            "crowding_late_score": crowding_late_score,
            "rank_jump_score": 0.0,  # populated after sort in post-process step
            "signal_source": "",     # populated below
            "event_flags": [],
            "recent_events": [],
            # Phase 2 new scores
            "narrative_heat_score": narrative_heat_score,
            "meme_rotation_score": meme_rotation_score,
            "sector": sym_sector,
            "in_watchlist": in_watchlist,
            "alt_eligible": bool(alt_eligible),
            "signal_state": "",
            "tags": [],
            "reasons_proxy": [],
            "reasons_chain": [],
            "data_quality": item["data_quality"],
            "freshness": item["freshness"],
            "derivatives_context": item["derivatives_context"],
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
        extra_tags: List[str] = []
        if _to_float(row.get("derivatives_heat_score"), 0.0) >= 0.65:
            extra_tags.append("Derivatives Heat")
        if _to_float(row.get("squeeze_score"), 0.0) >= 0.65:
            extra_tags.append("Squeeze Setup")
        if _to_float(row.get("crowding_risk_score"), 0.0) >= 0.70:
            extra_tags.append("Crowding Risk")
        if _to_float(row.get("liquidity_trap_score"), 0.0) >= 0.70:
            extra_tags.append("Liquidity Trap")
        if row.get("derivatives_context", {}).get("crowded_long"):
            extra_tags.append("Crowded Long")
        if row.get("derivatives_context", {}).get("squeeze_building"):
            extra_tags.append("Short Squeeze Risk")
        if row.get("derivatives_context", {}).get("order_flow_confirmed"):
            extra_tags.append("Order Flow Confirmed")
        if row.get("freshness", {}).get("derivatives_label") == "stale":
            extra_tags.append("Derivatives Stale")
        if "derivatives_missing" in row.get("data_quality", {}).get("degraded_reason", []):
            extra_tags.append("Derivatives Missing")
        if not alt_eligible:
            extra_tags.append("Benchmark Excluded")
        for tag in extra_tags:
            if tag not in row["tags"]:
                row["tags"].append(tag)
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
        if pct["squeeze_signal"] is not None and pct["squeeze_signal"] >= 0.65:
            proxy_reasons.append("Derivatives squeeze setup is confirming the tape instead of staying neutral.")
        if pct["crowding_risk"] is not None and pct["crowding_risk"] >= 0.70:
            proxy_reasons.append("Crowding risk is elevated, so any chase entry should stay size-aware.")
        if pct["liquidity_trap"] is not None and pct["liquidity_trap"] >= 0.70:
            proxy_reasons.append("Liquidity trap score is high, so failed breakouts can unwind quickly.")
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
        if pct["flow_confirmation"] is not None and pct["flow_confirmation"] >= 0.65:
            chain_reasons.append("Derivatives flow is aligned with community and whale confirmation.")
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

        if "derivatives_missing" in row["data_quality"].get("degraded_reason", []):
            chain_reasons.append("Derivatives cache is missing for this symbol, so heat and crowding stay conservative.")
        if not alt_eligible:
            proxy_reasons.insert(0, "This symbol is treated as a benchmark / major coin, so altcoin radar alerts stay suppressed.")

        row["reasons_proxy"] = proxy_reasons[:4]
        row["reasons_chain"] = chain_reasons[:4]

        # Phase 1 + Phase 2: classify combined signal_source
        perp_src = classify_signal_source(
            ignition_score=ignition_score,
            continuation_score=continuation_score,
            crowding_late_score=crowding_late_score,
            derivatives_present=bool(item["derivatives"]),
        )
        narrative_src = classify_narrative_source(
            narrative_heat_score=narrative_heat_score,
            meme_rotation_score=meme_rotation_score,
        )
        # Priority: crowded_late_stage > perp_ignition > perp_continuation > narrative
        if perp_src == "crowded_late_stage":
            row["signal_source"] = "crowded_late_stage"
        elif perp_src in ("perp_ignition", "perp_continuation"):
            row["signal_source"] = perp_src
        elif narrative_src:
            row["signal_source"] = narrative_src
        else:
            row["signal_source"] = ""

        # Phase 1: add perp tags
        if ignition_score >= 0.60 and row["signal_source"] == "perp_ignition":
            if "Perp Ignition" not in row["tags"]:
                row["tags"].append("Perp Ignition")
        if crowding_late_score >= 0.65 and row["signal_source"] == "crowded_late_stage":
            if "Late Stage" not in row["tags"]:
                row["tags"].append("Late Stage")
        # Phase 2: add narrative tags
        if narrative_heat_score >= 0.55 and row["signal_source"] in ("narrative_ignition", "narrative_confirmation"):
            if "Narrative" not in row["tags"]:
                row["tags"].append("Narrative")
        if in_watchlist and "Watchlist" not in row["tags"]:
            row["tags"].append("Watchlist")
        # Multi-engine: both perp and narrative signals present
        if perp_src and perp_src != "crowded_late_stage" and narrative_src:
            if "Multi-Engine" not in row["tags"]:
                row["tags"].append("Multi-Engine")

        rows.append(row)

    # Phase 1+2: compute rank_jump_score after sort order is known
    # Preliminary sort by layout_score for temp ranks; final rank assigned in sort_rows
    temp_sorted = sorted(rows, key=lambda r: _to_float(r.get("layout_score"), 0.0), reverse=True)
    temp_ranked_rows: List[Dict[str, Any]] = []
    for temp_rank, row in enumerate(temp_sorted, start=1):
        sym = str(row.get("symbol") or "").strip().upper()
        history = get_rank_history(sym)
        prev_snapshot = history[-1] if history else {}
        prev_rank = int(_to_float(prev_snapshot.get("rank"), 0.0)) if prev_snapshot else 0
        prev_ignition_score = _to_float(prev_snapshot.get("ignition_score"), 0.0) if prev_snapshot else 0.0

        row["rank"] = temp_rank
        rjs = compute_rank_jump_score(sym, temp_rank)
        row["rank_jump_score"] = round(rjs, 4)
        row["event_flags"] = []
        row["recent_events"] = []

        ignition_event = record_ignition_cross_up(
            sym,
            _to_float(row.get("ignition_score"), 0.0),
            prev_ignition_score,
        )
        if ignition_event:
            row["event_flags"].append(ignition_event["event_type"])
            row["recent_events"].append(ignition_event)

        rank_jump_event = record_rank_jump_event(
            sym,
            current_rank=temp_rank,
            prev_rank=prev_rank,
        ) if prev_rank > 0 else None
        if rank_jump_event:
            row["event_flags"].append(rank_jump_event["event_type"])
            row["recent_events"].append(rank_jump_event)

        # Phase 1 events: crowding spike (best-effort)
        if _to_float(row.get("crowding_late_score"), 0.0) >= 0.65:
            crowding_event = record_crowding_spike(sym, _to_float(row.get("crowding_late_score"), 0.0))
            if crowding_event:
                row["event_flags"].append(crowding_event["event_type"])
                row["recent_events"].append(crowding_event)
        # Phase 2 events: narrative heat spike (best-effort)
        if _to_float(row.get("narrative_heat_score"), 0.0) >= 0.55:
            narrative_event = record_narrative_heat_spike(
                sym,
                _to_float(row.get("narrative_heat_score"), 0.0),
                metadata={"sector": row.get("sector", ""), "in_watchlist": bool(row.get("in_watchlist"))},
            )
            if narrative_event:
                row["event_flags"].append(narrative_event["event_type"])
                row["recent_events"].append(narrative_event)

        temp_ranked_rows.append(row)

    # Update rank history cache after this scan
    bulk_update_ranks(temp_ranked_rows)

    return rows


def _normalized_sparkline(values: Sequence[Any]) -> List[float]:
    numeric = [_to_float(value, 0.0) for value in values if value is not None]
    if not numeric:
        return []
    base = numeric[0] if abs(numeric[0]) > 1e-12 else 1.0
    return [round((value / base) * 100.0, 4) for value in numeric]


def _detail_top_components(
    items: Sequence[tuple[str, float]],
    *,
    minimum: float = 0.0,
    limit: int = 3,
) -> List[str]:
    ranked = [
        (label, _to_float(score, 0.0))
        for label, score in items
        if _to_float(score, 0.0) > minimum
    ]
    ranked.sort(key=lambda item: item[1], reverse=True)
    return [label for label, _ in ranked[:limit]]


def _row_metrics(row: Mapping[str, Any]) -> Dict[str, Any]:
    metrics = row.get("metrics") or {}
    return metrics if isinstance(metrics, dict) else {}


def _build_detail_component_raw_maps(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Dict[str, Optional[float]]]:
    raw_maps: Dict[str, Dict[str, Optional[float]]] = {
        "community_flow": {},
        "announcements": {},
        "funding_basis": {},
        "whale_context": {},
    }
    for row in rows:
        symbol = str((row or {}).get("symbol") or "").strip().upper()
        if not symbol:
            continue
        metrics = _row_metrics(row)
        raw_maps["community_flow"][symbol] = max(_to_float(metrics.get("community_flow_imbalance"), 0.0), 0.0)
        raw_maps["announcements"][symbol] = max(_to_float(metrics.get("announcement_count"), 0.0), 0.0)
        raw_maps["funding_basis"][symbol] = (
            max(_to_float(metrics.get("funding_rate"), 0.0), 0.0) + max(_to_float(metrics.get("basis_pct"), 0.0), 0.0)
        )
        raw_maps["whale_context"][symbol] = max(_to_float(metrics.get("whale_count"), 0.0), 0.0)
    return raw_maps


def _backfill_detail_chain_percentiles(
    *,
    rows: Sequence[Mapping[str, Any]],
    symbol: str,
    percentiles: Mapping[str, Any],
    detail_community_snapshot: Optional[Mapping[str, Any]] = None,
    detail_whale_snapshot: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Optional[float]]:
    normalized_symbol = str(symbol or "").strip().upper()
    raw_maps = _build_detail_component_raw_maps(rows)

    community_snapshot = dict(detail_community_snapshot or {})
    whale_snapshot = dict(detail_whale_snapshot or {})

    community_flow = _community_flow_value(community_snapshot)
    if community_flow is not None:
        raw_maps["community_flow"][normalized_symbol] = community_flow

    announcements = _announcement_value(community_snapshot)
    if announcements is not None:
        raw_maps["announcements"][normalized_symbol] = announcements

    whale_context = _whale_context_value(whale_snapshot)
    if whale_context is not None:
        raw_maps["whale_context"][normalized_symbol] = whale_context

    backfilled: Dict[str, Optional[float]] = {}
    for key in ("community_flow", "announcements", "funding_basis", "whale_context"):
        if percentiles.get(key) is not None:
            continue
        pct_map = _series_percentiles(raw_maps.get(key) or {})
        if normalized_symbol in pct_map:
            backfilled[key] = pct_map.get(normalized_symbol)
    return backfilled


def _build_action_plan(
    selected: Mapping[str, Any],
    invalidate_conditions: Sequence[str],
) -> Dict[str, Any]:
    source = str(selected.get("signal_source") or "").strip()
    ignition_score = _to_float(selected.get("ignition_score"), 0.0)
    continuation_score = _to_float(selected.get("continuation_score"), 0.0)
    crowding_score = _to_float(selected.get("crowding_late_score"), 0.0)
    narrative_heat = _to_float(selected.get("narrative_heat_score"), 0.0)
    meme_rotation = _to_float(selected.get("meme_rotation_score"), 0.0)
    in_watchlist = bool(selected.get("in_watchlist"))
    data_quality = selected.get("data_quality") or {}
    market_freshness = _to_float(data_quality.get("market_data_freshness"), 0.0)

    tone = "muted"
    stance = "先观察，等待确认"
    summary = "当前证据还不够集中，先别把它当成明确埋伏位。"
    primary_action = "加入 Watchlist"
    secondary_action = "等下一轮扫描"
    actions = [
        "先留在观察池，不要急着把榜单名次当成交点。",
        "下一次扫描若信号来源切成点火/延续，再升级处理。",
    ]

    if crowding_score >= 0.65 or source == "crowded_late_stage":
        tone = "danger"
        stance = "高拥挤，别追"
        summary = "更像末端拥挤，不是舒服的埋伏位，优先防止追在情绪末端。"
        primary_action = "建拥挤预警"
        secondary_action = "等拥挤回落"
        actions = [
            "先不要把它当新点火，优先等拥挤分和资金过热回落。",
            "如果只是想跟踪，保留在 Watchlist 即可，不要因为榜单靠前就直接追。",
        ]
    elif ignition_score >= 0.60 or source == "perp_ignition":
        tone = "ignition"
        stance = "点火观察，先等确认"
        summary = "这类最接近“刚启动”，但更适合预警加确认，不适合把榜单本身当作追价理由。"
        primary_action = "建点火预警"
        secondary_action = "等首轮回踩确认"
        actions = [
            "重点盯 15m / 1h 是否继续放量、OI 继续抬升，而不是只看一根启动K。",
            "更稳的埋伏方式是等首轮回踩不破，再观察是否有二次发力。",
        ]
    elif continuation_score >= 0.55 or source == "perp_continuation":
        tone = "control"
        stance = "延续跟踪，别当首爆"
        summary = "这更像走势已经发动后的延续段，不是最早的点火位。"
        primary_action = "建跃升预警"
        secondary_action = "看回踩确认"
        actions = [
            "适合跟踪回踩后的承接，不适合把它当成“刚启动”的埋伏点。",
            "如果 funding 和 crowding 继续抬升，要及时降级成观察而不是硬追。",
        ]
    elif source.startswith("narrative_") or narrative_heat >= 0.55 or meme_rotation >= 0.55 or in_watchlist:
        tone = "narrative"
        stance = "叙事观察，等合约跟随"
        summary = "更偏题材轮动 / 板块升温，适合先收藏和跟踪，不够像纯 Perp 点火。"
        primary_action = "建叙事预警"
        secondary_action = "保留 Watchlist"
        actions = [
            "先看同板块是不是一起升温，再看合约侧点火分会不会补上来。",
            "如果只是单币热度抬头但没有合约跟随，优先当观察，不要急着追。",
        ]

    if market_freshness and market_freshness < 0.35:
        summary = f"当前数据新鲜度偏低，{summary}"
        actions.insert(0, "先等下一次刷新确认，避免拿旧快照直接下判断。")

    if invalidate_conditions:
        actions.append(f"失效先看：{invalidate_conditions[0]}")

    return {
        "tone": tone,
        "stance": stance,
        "summary": summary,
        "primary_action": primary_action,
        "secondary_action": secondary_action,
        "actions": actions[:4],
    }


def build_detail_payload(
    *,
    rows: Sequence[Mapping[str, Any]],
    symbol: str,
    sort_by: str = "layout",
    onchain_context: Optional[Mapping[str, Any]] = None,
    detail_community_snapshot: Optional[Mapping[str, Any]] = None,
    detail_whale_snapshot: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    normalized_symbol = str(symbol or "").strip().upper()
    ordered = sort_rows(rows, sort_by=sort_by)
    selected = next((dict(row) for row in ordered if str(row.get("symbol") or "").upper() == normalized_symbol), None)
    if selected is None:
        return {
            "selected_row": None,
            "derivatives_context": {},
            "proxy_breakdown": {},
            "chain_breakdown": {},
            "sparkline": [],
            "ignition_path": {},
            "narrative_linkage": {},
            "event_timeline": [],
            "invalidate_conditions": [],
            "action_plan": {},
            "related_candidates": [],
        }
    metrics = dict(selected.get("metrics") or {})
    percentiles = dict(metrics.get("percentiles") or {})
    percentiles.update(
        {
            key: value
            for key, value in _backfill_detail_chain_percentiles(
                rows=ordered,
                symbol=normalized_symbol,
                percentiles=percentiles,
                detail_community_snapshot=detail_community_snapshot,
                detail_whale_snapshot=detail_whale_snapshot,
            ).items()
            if value is not None
        }
    )
    if percentiles:
        metrics["percentiles"] = percentiles
        selected["metrics"] = metrics
    state = str(selected.get("signal_state") or "").strip()
    metrics_raw = _row_metrics(selected)
    proxy_breakdown = {
        "engine": "代理行为引擎",
        "dominant_sort": sort_by,
        "scores": {
            "layout": selected.get("layout_score"),
            "alert": selected.get("alert_score"),
            "anomaly": selected.get("anomaly_score"),
            "accumulation": selected.get("accumulation_score"),
            "control": selected.get("control_score"),
            "derivatives_heat": selected.get("derivatives_heat_score"),
            "squeeze": selected.get("squeeze_score"),
            "crowding_risk": selected.get("crowding_risk_score"),
            "liquidity_trap": selected.get("liquidity_trap_score"),
            "flow_confirmation": selected.get("flow_confirmation_score"),
            "risk_penalty": selected.get("risk_penalty"),
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
            {"label": "Derivatives Heat", "pctile": percentiles.get("derivatives_heat"), "weight": 0.55},
            {"label": "Squeeze Setup", "pctile": percentiles.get("squeeze_signal"), "weight": 0.45},
            {"label": "Crowding Risk", "pctile": percentiles.get("crowding_risk"), "weight": 1.00},
            {"label": "Liquidity Trap", "pctile": percentiles.get("liquidity_trap"), "weight": 0.65},
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
            {"label": "flow confirmation", "pctile": percentiles.get("flow_confirmation"), "weight": 0.45},
        ],
        "reasons": list(selected.get("reasons_chain") or []),
        "onchain_context": dict(onchain_context or {}),
    }

    ignition_components = _detail_top_components(
        [
            ("OI 异动", metrics_raw.get("oi_change_1h")),
            ("量能爆发", metrics_raw.get("volume_burst_ratio")),
            ("空头清算占比", metrics_raw.get("short_liq_share")),
            ("压缩后突破", metrics_raw.get("impulse_after_compression")),
            ("盘口/交易所扩散", metrics_raw.get("exchange_breadth")),
        ],
        minimum=0.0,
        limit=4,
    )
    ignition_path = {
        "source": str(selected.get("signal_source") or ""),
        "summary": (
            "当前更像合约驱动的点火候选。"
            if str(selected.get("signal_source") or "") == "perp_ignition"
            else "当前更像延续/确认阶段，点火优先级次于结构确认。"
            if str(selected.get("signal_source") or "") == "perp_continuation"
            else "当前不是典型的 Perp 点火路径，需结合叙事与风险面一起看。"
        ),
        "scores": {
            "ignition": selected.get("ignition_score"),
            "continuation": selected.get("continuation_score"),
            "crowding": selected.get("crowding_late_score"),
            "rank_jump": selected.get("rank_jump_score"),
        },
        "drivers": ignition_components,
        "event_flags": list(selected.get("event_flags") or []),
    }

    selected_sector = str(selected.get("sector") or "").strip()
    narrative_peers: List[Dict[str, Any]] = []
    for row in ordered:
        row_symbol = str(row.get("symbol") or "").strip().upper()
        if row_symbol == normalized_symbol:
            continue
        same_sector = selected_sector and str(row.get("sector") or "").strip() == selected_sector
        same_watchlist = bool(selected.get("in_watchlist")) and bool(row.get("in_watchlist"))
        if not same_sector and not same_watchlist:
            continue
        narrative_peers.append(
            {
                "symbol": row.get("symbol"),
                "sector": row.get("sector"),
                "in_watchlist": bool(row.get("in_watchlist")),
                "signal_source": row.get("signal_source"),
                "narrative_heat_score": row.get("narrative_heat_score"),
                "rank": row.get("rank"),
            }
        )
        if len(narrative_peers) >= 5:
            break

    narrative_drivers = _detail_top_components(
        [
            ("板块热度", percentiles.get("announcements")),
            ("社区流动性", percentiles.get("community_flow")),
            ("量能轮动", percentiles.get("volume_burst")),
            ("鲸鱼/活跃地址", percentiles.get("whale_context")),
            ("短线弹性", percentiles.get("return_shock")),
        ],
        minimum=0.0,
        limit=4,
    )
    narrative_linkage = {
        "sector": selected_sector,
        "in_watchlist": bool(selected.get("in_watchlist")),
        "source": str(selected.get("signal_source") or ""),
        "summary": (
            "当前候选具备叙事先行特征，即使 Perp 数据一般，也可以进入叙事雷达。"
            if str(selected.get("signal_source") or "").startswith("narrative_")
            else "当前更偏合约或结构驱动，叙事层更多是辅助确认。"
        ),
        "scores": {
            "narrative_heat": selected.get("narrative_heat_score"),
            "meme_rotation": selected.get("meme_rotation_score"),
        },
        "drivers": narrative_drivers,
        "board_peers": narrative_peers,
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

    action_plan = _build_action_plan(selected, invalidate_conditions)

    related_candidates = []
    for row in ordered:
        if str(row.get("symbol") or "").upper() == normalized_symbol:
            continue
        related_state = str(row.get("signal_state") or "").strip()
        same_group = bool(state) and related_state == state
        same_sector = selected_sector and str(row.get("sector") or "").strip() == selected_sector
        if same_group or not related_candidates:
            related_candidates.append(
                {
                    "symbol": row.get("symbol"),
                    "signal_state": related_state,
                    "layout_score": row.get("layout_score"),
                    "alert_score": row.get("alert_score"),
                    "control_score": row.get("control_score"),
                    "derivatives_heat_score": row.get("derivatives_heat_score"),
                    "narrative_heat_score": row.get("narrative_heat_score"),
                    "sector": row.get("sector"),
                    "rank": row.get("rank"),
                }
            )
        elif same_sector:
            related_candidates.append(
                {
                    "symbol": row.get("symbol"),
                    "signal_state": related_state,
                    "layout_score": row.get("layout_score"),
                    "alert_score": row.get("alert_score"),
                    "control_score": row.get("control_score"),
                    "derivatives_heat_score": row.get("derivatives_heat_score"),
                    "narrative_heat_score": row.get("narrative_heat_score"),
                    "sector": row.get("sector"),
                    "rank": row.get("rank"),
                }
            )
        if len(related_candidates) >= 5:
            break

    seen_events = set()
    event_timeline: List[Dict[str, Any]] = []
    for event in list(selected.get("recent_events") or []):
        event_key = (event.get("event_type"), event.get("ts_iso"), event.get("symbol"))
        if event_key in seen_events:
            continue
        seen_events.add(event_key)
        event_timeline.append(dict(event))
    for event in get_recent_symbol_events(normalized_symbol, limit=12, max_age_sec=6 * 3600.0):
        event_key = (event.get("event_type"), event.get("ts_iso"), event.get("symbol"))
        if event_key in seen_events:
            continue
        seen_events.add(event_key)
        event_timeline.append(dict(event))
    event_timeline.sort(key=lambda item: _to_float(item.get("ts"), 0.0), reverse=True)

    return {
        "selected_row": selected,
        "derivatives_context": dict(selected.get("derivatives_context") or {}),
        "proxy_breakdown": proxy_breakdown,
        "chain_breakdown": chain_breakdown,
        "sparkline": _normalized_sparkline(selected.get("sparkline") or []),
        "ignition_path": ignition_path,
        "narrative_linkage": narrative_linkage,
        "event_timeline": event_timeline[:12],
        "invalidate_conditions": invalidate_conditions,
        "action_plan": action_plan,
        "related_candidates": related_candidates,
    }
