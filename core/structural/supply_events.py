"""Supply-event scoring and gate decisions."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional

from core.structural.context import RiskGateDecision, StructuralMarketContext, TradeSignalDecision, clamp, parse_utc_datetime, safe_float


RECIPIENT_RISK = {
    "investor": 1.0,
    "team": 0.85,
    "treasury": 0.55,
    "ecosystem": 0.45,
    "community": 0.35,
    "unknown": 0.60,
}


@dataclass
class SupplyEventConfig:
    pre_event_window_days: int = 14
    post_event_window_days: int = 14
    supply_pressure_enter: float = 0.70
    short_pressure_enter: float = 0.75
    priced_in_threshold: float = 0.60
    absorption_enter: float = 0.70
    event_blackout_hours_before: float = 2.0
    event_blackout_hours_after: float = 6.0
    min_liquidity_score: float = 0.60
    base_position_pct: float = 0.03


def recipient_risk_score(recipient_type: Any) -> float:
    return clamp(RECIPIENT_RISK.get(str(recipient_type or "unknown").lower(), RECIPIENT_RISK["unknown"]))


def _rank_like(value: float, scale: float) -> float:
    return clamp(safe_float(value) / max(scale, 1e-9))


def score_supply_pressure(event: Mapping[str, Any], market: Optional[Mapping[str, Any]] = None) -> float:
    market = dict(market or {})
    if "supply_pressure_score" in event:
        return clamp(event.get("supply_pressure_score"))
    metadata = dict(event.get("metadata") or {})
    if "supply_pressure_score" in metadata:
        return clamp(metadata.get("supply_pressure_score"))

    unlock_pct_float = safe_float(event.get("unlock_pct_float", event.get("unlock_pct_circ", 0.0)))
    unlock_usd = safe_float(event.get("unlock_usd", 0.0))
    adv_30d = safe_float(
        market.get("avg_daily_volume_30d", event.get("avg_daily_volume_30d", metadata.get("avg_daily_volume_30d", 0.0)))
    )
    unlock_adv = unlock_usd / adv_30d if adv_30d > 0 else 0.0
    low_liquidity_score = clamp(market.get("low_liquidity_score", metadata.get("low_liquidity_score", 0.0)))
    weak_market_regime_score = clamp(market.get("weak_market_regime_score", metadata.get("weak_market_regime_score", 0.0)))
    return clamp(
        0.35 * _rank_like(unlock_pct_float, 0.20)
        + 0.25 * _rank_like(unlock_adv, 1.00)
        + 0.20 * recipient_risk_score(event.get("recipient_type", "unknown"))
        + 0.10 * low_liquidity_score
        + 0.10 * weak_market_regime_score
    )


def event_priced_in_score(event: Mapping[str, Any], market: Optional[Mapping[str, Any]] = None) -> float:
    market = dict(market or {})
    metadata = dict(event.get("metadata") or {})
    for source in (market, event, metadata):
        if "priced_in_score" in source:
            return clamp(source.get("priced_in_score"))
    return clamp(
        0.50 * abs(safe_float(market.get("pre_event_price_move_z", 0.0)))
        + 0.30 * abs(safe_float(market.get("pre_event_crowding", 0.0)))
        + 0.20 * safe_float(market.get("news_mentions_z", 0.0))
    )


def event_absorption_score(event: Mapping[str, Any], market: Optional[Mapping[str, Any]] = None) -> float:
    market = dict(market or {})
    metadata = dict(event.get("metadata") or {})
    for source in (market, event, metadata):
        if "absorption_score" in source:
            return clamp(source.get("absorption_score"))
    return clamp(
        0.35 * safe_float(market.get("price_holds_above_event_low", 0.0))
        + 0.25 * safe_float(market.get("volume_above_average_without_new_low", 0.0))
        + 0.20 * safe_float(market.get("funding_normalizes", 0.0))
        + 0.20 * safe_float(market.get("exchange_netflow_not_worsening", 0.0))
    )


def hours_to_event(event: Mapping[str, Any], as_of: datetime) -> float:
    event_time = parse_utc_datetime(event.get("event_time"))
    return (event_time - parse_utc_datetime(as_of)).total_seconds() / 3600.0


def event_is_visible(event: Mapping[str, Any], as_of: datetime) -> bool:
    first_seen = event.get("first_seen_at")
    if not first_seen:
        return False
    return parse_utc_datetime(first_seen) <= parse_utc_datetime(as_of)


def event_in_window(event: Mapping[str, Any], as_of: datetime, before_days: int, after_days: int) -> bool:
    hours = hours_to_event(event, as_of)
    return -24.0 * after_days <= hours <= 24.0 * before_days


def filter_active_supply_events(
    events: Iterable[Mapping[str, Any]],
    *,
    symbol: str,
    as_of: datetime,
    before_days: int = 14,
    after_days: int = 14,
    require_visible: bool = True,
) -> List[Dict[str, Any]]:
    canonical = str(symbol or "").upper()
    out: List[Dict[str, Any]] = []
    for raw in events or []:
        event = dict(raw)
        if str(event.get("symbol") or "").upper() != canonical:
            continue
        if require_visible and not event_is_visible(event, as_of):
            continue
        if event_in_window(event, as_of, before_days, after_days):
            out.append(event)
    return out


class SupplyEventGate:
    def __init__(self, config: Optional[SupplyEventConfig] = None):
        self.config = config or SupplyEventConfig()

    def evaluate(self, ctx: StructuralMarketContext, *, market: Optional[Mapping[str, Any]] = None) -> RiskGateDecision:
        cfg = self.config
        market = dict(market or {})
        active = list(ctx.events.active_events or [])
        if not active:
            return RiskGateDecision.neutral(ctx.symbol, ctx.timestamp, "supply_event_gate_neutral")

        block_longs = False
        reduce_long = 1.0
        severity = 0.0
        reasons: List[str] = []
        event_rows: List[Dict[str, Any]] = []

        for event in active:
            pressure = max(ctx.events.supply_pressure_score, score_supply_pressure(event, market))
            priced = max(ctx.events.priced_in_score, event_priced_in_score(event, market))
            hours = hours_to_event(event, ctx.timestamp)
            row = {
                "event_id": event.get("event_id"),
                "event_type": event.get("event_type"),
                "hours_to_event": round(hours, 6),
                "supply_pressure_score": round(pressure, 6),
                "priced_in_score": round(priced, 6),
            }
            event_rows.append(row)
            if 0.0 <= hours <= 24.0 * cfg.pre_event_window_days and pressure >= cfg.supply_pressure_enter and priced < cfg.priced_in_threshold:
                block_longs = True
                severity = max(severity, pressure)
                reduce_long = min(reduce_long, clamp(0.7 - (pressure - cfg.supply_pressure_enter), 0.3, 0.7))
                reasons.append("supply_event_pre_event_long_gate")

        if not reasons:
            reasons.append("supply_event_gate_neutral")

        return RiskGateDecision(
            symbol=ctx.symbol,
            timestamp=ctx.timestamp,
            block_new_longs=block_longs,
            reduce_long_position_scalar=reduce_long,
            reduce_short_position_scalar=1.0,
            gate_scalar=reduce_long,
            severity=severity,
            reason_codes=sorted(set(reasons)),
            metadata={"active_events": event_rows},
        )


def evaluate_supply_event_trade(
    ctx: StructuralMarketContext,
    *,
    market: Optional[Mapping[str, Any]] = None,
    config: Optional[SupplyEventConfig] = None,
) -> TradeSignalDecision:
    cfg = config or SupplyEventConfig()
    market = dict(market or {})
    active = list(ctx.events.active_events or [])
    if not active:
        return TradeSignalDecision.hold(ctx.symbol, ctx.timestamp, "supply_event_no_active_event")

    for event in active:
        pressure = max(ctx.events.supply_pressure_score, score_supply_pressure(event, market))
        priced = max(ctx.events.priced_in_score, event_priced_in_score(event, market))
        absorption = max(ctx.events.absorption_score, event_absorption_score(event, market))
        hours = hours_to_event(event, ctx.timestamp)
        blackout = -cfg.event_blackout_hours_after <= hours <= cfg.event_blackout_hours_before
        liquidity_score = clamp(market.get("liquidity_score", event.get("liquidity_score", 1.0)))
        borrow_or_perp = bool(market.get("borrow_or_perp_available", event.get("borrow_or_perp_available", False)))
        market_regime = str(market.get("market_regime", event.get("market_regime", "neutral"))).lower()
        short_crowding = clamp(market.get("short_crowding_score", event.get("short_crowding_score", 0.0)))

        if (
            not blackout
            and 0.0 < hours <= 24.0 * cfg.pre_event_window_days
            and pressure >= cfg.short_pressure_enter
            and priced < cfg.priced_in_threshold
            and liquidity_score >= cfg.min_liquidity_score
            and borrow_or_perp
            and market_regime != "bullish"
            and short_crowding < 0.75
        ):
            strength = clamp(pressure)
            return TradeSignalDecision(
                symbol=ctx.symbol,
                timestamp=ctx.timestamp,
                direction="sell",
                bias=-strength,
                strength=strength,
                position_pct=cfg.base_position_pct,
                reason_codes=["supply_event_pre_event_small_short"],
                metadata={"event_id": event.get("event_id"), "hours_to_event": round(hours, 6)},
            )

        if (
            -24.0 * cfg.post_event_window_days <= hours <= -24.0
            and absorption >= cfg.absorption_enter
            and bool(market.get("price_recovers_event_vwap", event.get("price_recovers_event_vwap", True)))
            and (
                bool(market.get("funding_reset", event.get("funding_reset", True)))
                or short_crowding >= 0.70
            )
        ):
            strength = clamp(absorption)
            return TradeSignalDecision(
                symbol=ctx.symbol,
                timestamp=ctx.timestamp,
                direction="buy",
                bias=strength,
                strength=strength,
                position_pct=cfg.base_position_pct,
                reason_codes=["supply_event_post_event_absorption_long"],
                metadata={"event_id": event.get("event_id"), "hours_since_event": round(abs(hours), 6)},
            )

    return TradeSignalDecision.hold(ctx.symbol, ctx.timestamp, "supply_event_trade_not_confirmed")
