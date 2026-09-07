"""Altcoin radar scoring primitives and market-snapshot metrics.

Pure numeric/series helpers, per-signal value extractors, and the
market-snapshot metric builders that feed the radar row builder. Split out of
the former monolithic core/research/altcoin_radar.py; the radar module and its
siblings import from here.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional

import numpy as np
import pandas as pd



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


def _round_metric(value: Any) -> float:
    numeric = _to_float(value)
    magnitude = abs(numeric)
    if magnitude == 0:
        return 0.0
    if magnitude < 0.0001:
        return round(numeric, 10)
    if magnitude < 0.01:
        return round(numeric, 8)
    if magnitude < 1:
        return round(numeric, 6)
    return round(numeric, 4)


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


def _optional_pct_fraction(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except Exception:
        return None
    if not math.isfinite(parsed):
        return None
    return float(parsed) / 100.0


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


def _window_seconds(window: str) -> int:
    text = str(window or "").strip().lower()
    mapping = {"1h": 3600, "4h": 14400, "12h": 43200, "24h": 86400}
    return int(mapping.get(text, 3600))


def _market_snapshot_return_path(snapshot: Mapping[str, Any], timeframe: str) -> Dict[str, float]:
    windows = _timeframe_window_map(timeframe)
    ret6 = _optional_pct_fraction(snapshot.get(f"price_change_percent_{windows['6']}"))
    if ret6 is None and windows["6"] != "24h":
        ret6 = _optional_pct_fraction(snapshot.get("price_change_percent_24h"))
    ret3 = _optional_pct_fraction(snapshot.get(f"price_change_percent_{windows['3']}"))
    if ret3 is None and ret6 is not None:
        ret3 = ret6 * 0.5
    ret1 = _optional_pct_fraction(snapshot.get(f"price_change_percent_{windows['1']}"))
    if ret1 is None and ret6 is not None:
        ret1 = ret6 / 6.0
    return {
        "ret1": float(ret1 or 0.0),
        "ret3": float(ret3 or 0.0),
        "ret6": float(ret6 or 0.0),
    }


def _market_snapshot_flow_imbalance(snapshot: Mapping[str, Any], window: str) -> float:
    long_volume = _to_float(snapshot.get(f"long_volume_usd_{window}"), 0.0)
    short_volume = _to_float(snapshot.get(f"short_volume_usd_{window}"), 0.0)
    total = long_volume + short_volume
    if total <= 0:
        return 0.0
    return (long_volume - short_volume) / total


def _synthetic_sparkline(snapshot: Mapping[str, Any], timeframe: str) -> List[float]:
    current_price = max(_to_float(snapshot.get("current_price"), 0.0), 1e-6)
    returns = _market_snapshot_return_path(snapshot, timeframe)
    ret1 = returns["ret1"]
    ret3 = returns["ret3"]
    ret6 = returns["ret6"]

    def _backsolve(price: float, change: float) -> float:
        return price / max(1.0 + change, 1e-6)

    p6 = _backsolve(current_price, ret6)
    p3 = _backsolve(current_price, ret3)
    p1 = _backsolve(current_price, ret1)
    mid_a = p6 + (p3 - p6) * 0.5
    mid_b = p3 + (p1 - p3) * 0.5
    return [_round_metric(value) for value in [p6, mid_a, p3, mid_b, p1, current_price]]


def _market_snapshot_metrics(
    snapshot: Mapping[str, Any],
    *,
    timeframe: str,
    now: datetime,
) -> Dict[str, Any]:
    windows = _timeframe_window_map(timeframe)
    returns = _market_snapshot_return_path(snapshot, timeframe)
    ret1 = returns["ret1"]
    ret3 = returns["ret3"]
    ret6 = returns["ret6"]
    volume_change = _to_float(snapshot.get(f"volume_change_percent_{windows['volume']}"), 0.0)
    flow_window = windows["flow"]
    liq_window = windows["liq"]
    oi_window = windows["oi"]

    imbalance = _market_snapshot_flow_imbalance(snapshot, flow_window)
    long_liq = _to_float(snapshot.get(f"long_liquidation_usd_{liq_window}"), 0.0)
    short_liq = _to_float(snapshot.get(f"short_liquidation_usd_{liq_window}"), 0.0)
    liq_total = long_liq + short_liq
    flow_volume_usd = (
        _to_float(snapshot.get(f"long_volume_usd_{flow_window}"), 0.0)
        + _to_float(snapshot.get(f"short_volume_usd_{flow_window}"), 0.0)
    )
    quote_volume_24h = _to_float(snapshot.get("quote_volume_24h"), 0.0)
    if flow_volume_usd <= 0 and quote_volume_24h > 0:
        flow_volume_usd = quote_volume_24h * (_window_seconds(flow_window) / 86400.0)
    short_liq_share = short_liq / max(liq_total, 1.0)
    liq_ratio = liq_total / max(flow_volume_usd, 1.0)
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
    avg_dollar_volume = flow_volume_usd

    timestamp = snapshot.get("timestamp") or _utcnow().isoformat()
    age_sec = _age_seconds(timestamp, now) or 0.0
    freshness = _freshness_score(age_sec, TIMEFRAME_SECONDS.get(timeframe, TIMEFRAME_SECONDS["4h"]), 4.0)
    spread_bps = _to_float(snapshot.get("spread_bps"), 0.0)
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
        "spread_bps": spread_bps,
        "order_flow_imbalance": imbalance,
        "market_age_sec": age_sec,
        "market_freshness": freshness,
        "market_as_of": timestamp,
        "sparkline": _synthetic_sparkline(snapshot, timeframe),
        "impulse_after_compression": impulse,
        "market_cap_usd": _to_float(snapshot.get("market_cap_usd"), 0.0),
        "source_name": snapshot.get("source_name"),
    }


def _alpha_quality_score(context: Mapping[str, Any]) -> float:
    """Build a bounded Alpha quality/optionality score from public metadata.

    This is deliberately not a prediction model.  It rewards usable liquidity,
    real holder/activity breadth, and moderate positive momentum while keeping
    the score below 1.0.  Risk controls still come from ``risk_penalty`` and
    the UI labels the result as a heuristic.
    """
    if not context:
        return 0.0
    market_cap = max(_to_float(context.get("market_cap_usd"), 0.0), 0.0)
    liquidity = max(_to_float(context.get("liquidity_usd"), 0.0), 0.0)
    holders = max(_to_float(context.get("holders"), 0.0), 0.0)
    count_24h = max(_to_float(context.get("count_24h"), 0.0), 0.0)
    momentum_pct = _to_float(context.get("percent_change_24h"), 0.0)
    liquidity_ratio = liquidity / max(market_cap, 1.0) if market_cap > 0 else 0.0
    liquidity_component = _clamp01(liquidity_ratio / 0.35)
    holder_component = _clamp01(math.log10(holders + 1.0) / 5.0)
    activity_component = _clamp01(math.log10(count_24h + 1.0) / 4.0)
    momentum_component = _clamp01((momentum_pct + 8.0) / 28.0)
    supply_ratio = context.get("circulating_ratio")
    supply_component = _clamp01(_to_float(supply_ratio, 0.5)) if supply_ratio is not None else 0.5
    hot_component = 1.0 if bool(context.get("hot_tag")) else 0.0
    return _clamp01(
        liquidity_component * 0.28
        + holder_component * 0.20
        + activity_component * 0.18
        + momentum_component * 0.16
        + supply_component * 0.10
        + hot_component * 0.08
    )


def _upside_score(row: Mapping[str, Any]) -> float:
    """Rank asymmetric-upside candidates without presenting certainty."""
    alpha_quality = _to_float(row.get("alpha_quality_score"), 0.0)
    signal = (
        _to_float(row.get("layout_score"), 0.0) * 0.22
        + _to_float(row.get("alert_score"), 0.0) * 0.16
        + _to_float(row.get("anomaly_score"), 0.0) * 0.12
        + _to_float(row.get("accumulation_score"), 0.0) * 0.14
        + _to_float(row.get("ignition_score"), 0.0) * 0.14
        + _to_float(row.get("continuation_score"), 0.0) * 0.10
        + _to_float(row.get("narrative_heat_score"), 0.0) * 0.07
        + _to_float(row.get("flow_confirmation_score"), 0.0) * 0.05
    )
    if bool(row.get("is_alpha")):
        signal += alpha_quality * 0.10
    risk = _to_float(row.get("risk_penalty"), 0.0)
    return _clamp01(signal - risk * 0.48)

