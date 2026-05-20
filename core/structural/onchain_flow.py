"""On-chain exchange-flow regime decisions."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from core.structural.context import (
    OnChainContext,
    PositionScalarDecision,
    StructuralMarketContext,
    clamp,
    clip_z,
    parse_utc_datetime,
)


@dataclass
class OnChainFlowConfig:
    accumulation_enter: float = 0.65
    distribution_enter: float = 0.65
    conflict_ceiling: float = 0.50
    max_scalar_up: float = 1.20
    max_scalar_down: float = 0.40
    stale_data_ttl_hours: float = 24.0


def calculate_onchain_scores(ctx: OnChainContext) -> tuple[float, float]:
    if ctx.accumulation_score > 0 or ctx.distribution_score > 0:
        return clamp(ctx.accumulation_score), clamp(ctx.distribution_score)
    accumulation = (
        0.35 * clip_z(-ctx.exchange_netflow_z)
        + 0.25 * clip_z(-ctx.exchange_balance_change_z)
        + 0.25 * clip_z(ctx.stablecoin_balance_change_z)
        + 0.15 * clamp(ctx.whale_outflow_score)
    )
    distribution = (
        0.35 * clip_z(ctx.exchange_netflow_z)
        + 0.25 * clip_z(ctx.exchange_balance_change_z)
        + 0.20 * clip_z(-ctx.stablecoin_balance_change_z)
        + 0.20 * clamp(ctx.whale_inflow_score)
    )
    return clamp(accumulation), clamp(distribution)


def _freshness_hours(ctx: StructuralMarketContext, now: datetime) -> float:
    if ctx.onchain.freshness_hours is not None:
        return max(0.0, float(ctx.onchain.freshness_hours))
    as_of = ctx.onchain.as_of or ctx.timestamp
    return max(0.0, (parse_utc_datetime(now) - parse_utc_datetime(as_of)).total_seconds() / 3600.0)


def evaluate_onchain_regime(
    ctx: StructuralMarketContext,
    *,
    config: Optional[OnChainFlowConfig] = None,
    now: Optional[datetime] = None,
) -> PositionScalarDecision:
    cfg = config or OnChainFlowConfig()
    now_utc = parse_utc_datetime(now or datetime.now(timezone.utc))
    freshness = _freshness_hours(ctx, now_utc)
    accumulation, distribution = calculate_onchain_scores(ctx.onchain)
    ctx.onchain.accumulation_score = accumulation
    ctx.onchain.distribution_score = distribution

    metadata = {
        "accumulation_score": round(accumulation, 6),
        "distribution_score": round(distribution, 6),
        "freshness_hours": round(freshness, 6),
        "lookahead_risk": ctx.onchain.lookahead_risk,
    }

    if ctx.onchain.degraded:
        ctx.onchain.onchain_regime = "neutral"
        return PositionScalarDecision(
            symbol=ctx.symbol,
            timestamp=ctx.timestamp,
            long_scalar=1.0,
            short_scalar=1.0,
            reason_codes=["onchain_degraded_no_amplification"],
            metadata={**metadata, "onchain_regime": "neutral"},
        )

    if freshness > cfg.stale_data_ttl_hours:
        ctx.onchain.onchain_regime = "neutral"
        return PositionScalarDecision(
            symbol=ctx.symbol,
            timestamp=ctx.timestamp,
            long_scalar=1.0,
            short_scalar=1.0,
            reason_codes=["onchain_stale_no_amplification"],
            metadata={**metadata, "onchain_regime": "neutral"},
        )

    if accumulation >= cfg.accumulation_enter and distribution < cfg.conflict_ceiling:
        ctx.onchain.onchain_regime = "accumulation"
        return PositionScalarDecision(
            symbol=ctx.symbol,
            timestamp=ctx.timestamp,
            long_scalar=min(cfg.max_scalar_up, 1.15),
            short_scalar=max(cfg.max_scalar_down, 0.75),
            reason_codes=["onchain_accumulation_regime"],
            metadata={**metadata, "onchain_regime": "accumulation"},
        )

    if distribution >= cfg.distribution_enter and accumulation < cfg.conflict_ceiling:
        ctx.onchain.onchain_regime = "distribution"
        return PositionScalarDecision(
            symbol=ctx.symbol,
            timestamp=ctx.timestamp,
            long_scalar=max(cfg.max_scalar_down, 0.50),
            short_scalar=min(cfg.max_scalar_up, 1.10),
            reason_codes=["onchain_distribution_regime"],
            metadata={**metadata, "onchain_regime": "distribution"},
        )

    ctx.onchain.onchain_regime = "neutral"
    return PositionScalarDecision(
        symbol=ctx.symbol,
        timestamp=ctx.timestamp,
        long_scalar=1.0,
        short_scalar=1.0,
        reason_codes=["onchain_neutral_regime"],
        metadata={**metadata, "onchain_regime": "neutral"},
    )
