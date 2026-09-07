"""Altcoin radar scoring and detail helpers."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional

import pandas as pd

from config.settings import settings
from core.data.coinglass_altcoin import is_alt_candidate_symbol
from core.research.altcoin_radar_perp import classify_signal_source, compute_perp_scores
from core.research.altcoin_radar_narrative import classify_narrative_source, compute_narrative_scores
from core.research.altcoin_radar_universe import get_sector, get_watchlist_symbols, normalize_altcoin_pair
from core.research.altcoin_radar_events import (
    bulk_update_ranks,
    compute_rank_jump_score,
    get_rank_history,
    record_crowding_spike,
    record_ignition_cross_up,
    record_narrative_heat_spike,
    record_rank_jump_event,
)

from core.research.altcoin_radar_metrics import (  # noqa: F401  re-export
    STATE_ANOMALY as STATE_ANOMALY,
    STATE_CONTROL_TRACK as STATE_CONTROL_TRACK,
    STATE_CONTROL_WARN as STATE_CONTROL_WARN,
    STATE_DISTRIBUTION as STATE_DISTRIBUTION,
    STATE_LAYOUT as STATE_LAYOUT,
    TIMEFRAME_SECONDS as TIMEFRAME_SECONDS,
    VALID_TIMEFRAMES as VALID_TIMEFRAMES,
)
from core.research.altcoin_radar_ranking import (  # noqa: F401  re-export
    sort_rows as sort_rows,
    summarize_rows as summarize_rows,
)
from core.research.altcoin_radar_detail import build_detail_payload as build_detail_payload  # noqa: F401
from core.research.altcoin_radar_metrics import (
    _absorption_proxy,
    _age_seconds,
    _alpha_quality_score,
    _announcement_value,
    _avg_true_range_ratio,
    _breakout_proximity,
    _clamp01,
    _close_control,
    _community_flow_value,
    _drift_stability,
    _freshness_score,
    _funding_basis_value,
    _impulse_after_compression,
    _market_snapshot_metrics,
    _one_sided_flow,
    _pct_change,
    _rolling_return_volatility,
    _round4,
    _round_metric,
    _safe_series,
    _security_event_value,
    _snapshot_payload,
    _spread_impact,
    _to_float,
    _upside_score,
    _utcnow,
    _volume_ratio,
    _whale_context_value,
)
from core.research.altcoin_radar_ranking import (
    _normalize_symbols,
    _series_percentiles,
    _signal_state_for_row,
    _state_tags,
    _weighted_score,
)


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
    alerted = {normalize_altcoin_pair(symbol) for symbol in (alerted_symbols or []) if normalize_altcoin_pair(symbol)}
    market_frame_map = {normalize_altcoin_pair(k): v for k, v in market_frames.items() if normalize_altcoin_pair(k)}
    market_snapshot_map = {normalize_altcoin_pair(k): dict(v or {}) for k, v in (market_snapshots or {}).items() if normalize_altcoin_pair(k)}
    micro_map = {normalize_altcoin_pair(k): dict(v or {}) for k, v in (micro_snapshots or {}).items() if normalize_altcoin_pair(k)}
    community_map = {normalize_altcoin_pair(k): dict(v or {}) for k, v in (community_snapshots or {}).items() if normalize_altcoin_pair(k)}
    whale_map = {normalize_altcoin_pair(k): dict(v or {}) for k, v in (whale_snapshots or {}).items() if normalize_altcoin_pair(k)}
    derivatives_map = {normalize_altcoin_pair(k): dict(v or {}) for k, v in (derivatives_snapshots or {}).items() if normalize_altcoin_pair(k)}
    factor_rows = {
        normalize_altcoin_pair(item.get("symbol")): dict(item or {})
        for item in (factor_payload.get("asset_scores") or [])
        if str(item.get("symbol") or "").strip()
    }
    multi_rows = {
        normalize_altcoin_pair(item.get("symbol")): dict(item or {})
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
    all_symbols = _normalize_symbols(list(market_frame_map.keys()) + list(market_snapshot_map.keys()))
    for normalized_symbol in all_symbols:
        if not normalized_symbol:
            continue
        frame = market_frame_map.get(normalized_symbol)
        df = frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
        market_snapshot = dict(market_snapshot_map.get(normalized_symbol) or {})
        alpha_context = dict(market_snapshot.get("alpha_context") or {})
        is_alpha = bool(alpha_context) or str(market_snapshot.get("source_name") or "").strip() == "binance_alpha"
        alpha_quality_score = _alpha_quality_score(alpha_context) if is_alpha else 0.0
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
        community_payload = _snapshot_payload(community)
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
        market_snapshot_fresh = bool(market_snapshot and market_freshness >= 0.45)
        local_market_as_of = (
            df.index[-1].isoformat()
            if not df.empty and hasattr(df.index[-1], "isoformat")
            else str(df.index[-1])
            if not df.empty
            else None
        )
        if use_market_snapshot:
            market_source_name = str(
                coinglass_market_metrics.get("source_name")
                or market_snapshot.get("source_name")
                or "market_snapshot"
            ).strip()
            market_source_type = "live_snapshot"
        elif not df.empty:
            market_source_name = "local_kline"
            market_source_type = "local_kline"
        else:
            market_source_name = str(market_snapshot.get("source_name") or "market_snapshot").strip()
            market_source_type = "live_snapshot" if market_snapshot else "missing"
        market_as_of = (
            coinglass_market_metrics.get("market_as_of")
            if use_market_snapshot
            else local_market_as_of or market_snapshot.get("timestamp")
        )

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
        if bool(getattr(settings, "COINGLASS_INCLUDE_RADAR", True)) and not derivatives and not market_snapshot_fresh:
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
                "alpha_quality_score": alpha_quality_score,
                "alpha_percent_change_24h": _to_float(alpha_context.get("percent_change_24h"), 0.0),
                "alpha_liquidity_usd": _to_float(alpha_context.get("liquidity_usd"), 0.0),
                "alpha_holders": _to_float(alpha_context.get("holders"), 0.0),
                "alpha_volume_24h_usd": _to_float(alpha_context.get("volume_24h_usd"), 0.0),
            },
            "alpha_context": alpha_context,
            "is_alpha": is_alpha,
            "freshness": {
                "as_of": market_as_of,
                "market_data_age_sec": None if market_age_sec is None else round(market_age_sec, 2),
                "snapshot_age_sec": None if snapshot_age_sec is None else round(snapshot_age_sec, 2),
                "derivatives_age_sec": None if derivatives_age_sec is None else round(derivatives_age_sec, 2),
                "market_source": market_source_name,
                "market_source_type": market_source_type,
                "using_market_snapshot": bool(use_market_snapshot),
                "local_market_as_of": local_market_as_of,
                "market_snapshot_as_of": market_snapshot.get("timestamp"),
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
                "market_source": market_source_name,
                "market_source_type": market_source_type,
                "using_market_snapshot": bool(use_market_snapshot),
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
            "alpha_quality_score": _to_float(item["metrics_raw"].get("alpha_quality_score"), 0.0),
            "is_alpha": bool(item.get("is_alpha")),
            "alpha_context": dict(item.get("alpha_context") or {}),
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
                **{key: _round_metric(value) for key, value in item["metrics_raw"].items()},
                "percentiles": {key: (None if value is None else _round4(value)) for key, value in pct.items()},
            },
            "sparkline": item["sparkline"],
            "has_alert_rule": bool(item["has_alert_rule"]),
        }
        row["upside_score"] = _round4(_upside_score(row))
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
        if row.get("is_alpha"):
            extra_tags.append("Binance Alpha")
            if row.get("alpha_context", {}).get("hot_tag"):
                extra_tags.append("Alpha Hot")
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
        if row.get("is_alpha"):
            alpha_context = row.get("alpha_context") or {}
            alpha_label = alpha_context.get("display_symbol") or alpha_context.get("alpha_id") or row.get("symbol")
            proxy_reasons.insert(
                0,
                f"Binance Alpha 目录候选：{alpha_label}；Alpha 上行分仅作筛选线索",
            )
            if alpha_context.get("hot_tag"):
                proxy_reasons.append("Alpha Hot 标签存在，但仍需验证流动性与价格路径")
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
    # Rank/ignition history is score-regime specific: keep each timeframe's
    # snapshots separate so alternating 15m/4h scans don't manufacture phantom
    # ignition cross-ups or rank jumps (which feed real Feishu alert rules).
    history_context = str(timeframe or "").strip().lower()
    for temp_rank, row in enumerate(temp_sorted, start=1):
        sym = str(row.get("symbol") or "").strip().upper()
        history = get_rank_history(sym, context=history_context)
        prev_snapshot = history[-1] if history else {}
        prev_rank = int(_to_float(prev_snapshot.get("rank"), 0.0)) if prev_snapshot else 0
        prev_ignition_score = _to_float(prev_snapshot.get("ignition_score"), 0.0) if prev_snapshot else 0.0

        row["rank"] = temp_rank
        rjs = compute_rank_jump_score(sym, temp_rank, context=history_context)
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
    bulk_update_ranks(temp_ranked_rows, context=history_context)

    return rows

