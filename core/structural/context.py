"""Shared structural market context.

The structural layer intentionally separates market evidence from trade
execution.  Strategies can emit entries, but the same context is also useful as
an upstream gate for existing strategies.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timezone
from math import isfinite
from typing import Any, Dict, Iterable, List, Mapping, Optional

import pandas as pd


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_utc_datetime(value: Any, *, default: Optional[datetime] = None) -> datetime:
    if value is None or value == "":
        return default or utc_now()
    if isinstance(value, pd.Timestamp):
        ts = value.to_pydatetime()
    elif isinstance(value, datetime):
        ts = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            ts = datetime.fromisoformat(text)
        except Exception:
            return default or utc_now()
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return float(default)
        out = float(value)
        if not isfinite(out):
            return float(default)
        return out
    except Exception:
        return float(default)


def clamp(value: Any, low: float = 0.0, high: float = 1.0) -> float:
    val = safe_float(value, low)
    return max(float(low), min(float(high), val))


def clip_z(value: Any, z_cap: float = 3.0) -> float:
    """Map a positive z-score to 0..1 and ignore negative values."""
    cap = max(float(z_cap), 1e-9)
    return clamp(max(0.0, safe_float(value, 0.0)) / cap, 0.0, 1.0)


def _mapping_get(row: Mapping[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in row:
            return row[key]
    return default


def _optional_float(row: Mapping[str, Any], *keys: str) -> Optional[float]:
    marker = object()
    value = _mapping_get(row, *keys, default=marker)
    if value is marker:
        return None
    return safe_float(value)


def _serialize(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, pd.Timestamp):
        return parse_utc_datetime(value).isoformat()
    if is_dataclass(value):
        return {k: _serialize(v) for k, v in asdict(value).items()}
    if isinstance(value, dict):
        return {str(k): _serialize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serialize(v) for v in value]
    return value


@dataclass
class DerivativesContext:
    oi_change_z: float = 0.0
    funding_z: float = 0.0
    basis_z: float = 0.0
    long_short_ratio_z: float = 0.0
    taker_imbalance_z: float = 0.0
    liquidation_burst_score: float = 0.0
    long_liquidation_burst_score: float = 0.0
    short_liquidation_burst_score: float = 0.0
    crowded_long_score: float = 0.0
    crowded_short_score: float = 0.0
    crowding_side: str = "neutral"
    source: str = "unknown"
    as_of: Optional[datetime] = None
    freshness_hours: Optional[float] = None
    degraded: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.liquidation_burst_score = clamp(self.liquidation_burst_score)
        self.long_liquidation_burst_score = clamp(
            self.long_liquidation_burst_score or self.liquidation_burst_score
        )
        self.short_liquidation_burst_score = clamp(
            self.short_liquidation_burst_score or self.liquidation_burst_score
        )
        self.crowded_long_score = clamp(self.crowded_long_score)
        self.crowded_short_score = clamp(self.crowded_short_score)
        side = str(self.crowding_side or "neutral").lower()
        if side not in {"long", "short", "neutral"}:
            side = "neutral"
        self.crowding_side = side


@dataclass
class EventContext:
    active_events: List[Dict[str, Any]] = field(default_factory=list)
    event_risk_score: float = 0.0
    supply_pressure_score: float = 0.0
    priced_in_score: float = 0.0
    absorption_score: float = 0.0
    source: str = "unknown"
    as_of: Optional[datetime] = None
    freshness_hours: Optional[float] = None
    degraded: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.event_risk_score = clamp(self.event_risk_score)
        self.supply_pressure_score = clamp(self.supply_pressure_score)
        self.priced_in_score = clamp(self.priced_in_score)
        self.absorption_score = clamp(self.absorption_score)


@dataclass
class OnChainContext:
    exchange_netflow_z: float = 0.0
    exchange_balance_change_z: float = 0.0
    stablecoin_balance_change_z: float = 0.0
    whale_flow_score: float = 0.0
    whale_inflow_score: float = 0.0
    whale_outflow_score: float = 0.0
    accumulation_score: float = 0.0
    distribution_score: float = 0.0
    onchain_regime: str = "neutral"
    source: str = "unknown"
    as_of: Optional[datetime] = None
    freshness_hours: Optional[float] = None
    lookahead_risk: str = "unknown"
    degraded: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.whale_flow_score = clamp(self.whale_flow_score)
        self.whale_inflow_score = clamp(self.whale_inflow_score or max(0.0, self.whale_flow_score))
        self.whale_outflow_score = clamp(self.whale_outflow_score or max(0.0, -self.whale_flow_score))
        self.accumulation_score = clamp(self.accumulation_score)
        self.distribution_score = clamp(self.distribution_score)
        regime = str(self.onchain_regime or "neutral").lower()
        if regime not in {"accumulation", "distribution", "neutral", "unknown"}:
            regime = "neutral"
        self.onchain_regime = regime


@dataclass
class ExecutionContext:
    spread_bps: float = 0.0
    spread_bps_percentile: float = 0.0
    depth_score: float = 1.0
    low_depth_percentile: float = 0.0
    depth_imbalance: float = 0.0
    fee_bps: float = 0.0
    execution_risk_score: float = 0.0
    source: str = "unknown"
    as_of: Optional[datetime] = None
    freshness_hours: Optional[float] = None
    degraded: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.spread_bps_percentile = clamp(self.spread_bps_percentile)
        self.depth_score = clamp(self.depth_score)
        self.low_depth_percentile = clamp(self.low_depth_percentile)
        self.depth_imbalance = clamp(abs(self.depth_imbalance), 0.0, 1.0)
        self.execution_risk_score = clamp(self.execution_risk_score)


@dataclass
class RiskGateDecision:
    symbol: str
    timestamp: datetime
    allow_long: bool = True
    allow_short: bool = True
    block_new_longs: bool = False
    block_new_shorts: bool = False
    reduce_long_position_scalar: float = 1.0
    reduce_short_position_scalar: float = 1.0
    gate_scalar: float = 1.0
    severity: float = 0.0
    reason_codes: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.timestamp = parse_utc_datetime(self.timestamp)
        self.reduce_long_position_scalar = clamp(self.reduce_long_position_scalar, 0.0, 1.0)
        self.reduce_short_position_scalar = clamp(self.reduce_short_position_scalar, 0.0, 1.0)
        self.gate_scalar = clamp(self.gate_scalar, 0.0, 1.0)
        self.severity = clamp(self.severity)
        if self.block_new_longs:
            self.allow_long = False
        if self.block_new_shorts:
            self.allow_short = False
        if not self.reason_codes:
            self.reason_codes = ["gate_neutral"]

    @classmethod
    def neutral(cls, symbol: str, timestamp: Optional[datetime] = None, reason: str = "gate_neutral") -> "RiskGateDecision":
        return cls(symbol=symbol, timestamp=timestamp or utc_now(), reason_codes=[reason])

    @classmethod
    def combine(cls, symbol: str, timestamp: datetime, decisions: Iterable["RiskGateDecision"]) -> "RiskGateDecision":
        items = list(decisions)
        if not items:
            return cls.neutral(symbol, timestamp)
        reason_codes: List[str] = []
        metadata: Dict[str, Any] = {"components": []}
        for item in items:
            reason_codes.extend(list(item.reason_codes or []))
            metadata["components"].append(item.to_dict())
        return cls(
            symbol=symbol,
            timestamp=timestamp,
            allow_long=all(item.allow_long for item in items),
            allow_short=all(item.allow_short for item in items),
            block_new_longs=any(item.block_new_longs for item in items),
            block_new_shorts=any(item.block_new_shorts for item in items),
            reduce_long_position_scalar=min(item.reduce_long_position_scalar for item in items),
            reduce_short_position_scalar=min(item.reduce_short_position_scalar for item in items),
            gate_scalar=min(item.gate_scalar for item in items),
            severity=max(item.severity for item in items),
            reason_codes=sorted(set(reason_codes)),
            metadata=metadata,
        )

    def to_dict(self) -> Dict[str, Any]:
        return _serialize(self)


@dataclass
class PositionScalarDecision:
    symbol: str
    timestamp: datetime
    long_scalar: float = 1.0
    short_scalar: float = 1.0
    reason_codes: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.timestamp = parse_utc_datetime(self.timestamp)
        self.long_scalar = clamp(self.long_scalar, 0.0, 1.5)
        self.short_scalar = clamp(self.short_scalar, 0.0, 1.5)
        if not self.reason_codes:
            self.reason_codes = ["position_scalar_neutral"]

    @classmethod
    def neutral(
        cls,
        symbol: str,
        timestamp: Optional[datetime] = None,
        reason: str = "position_scalar_neutral",
    ) -> "PositionScalarDecision":
        return cls(symbol=symbol, timestamp=timestamp or utc_now(), reason_codes=[reason])

    def to_dict(self) -> Dict[str, Any]:
        return _serialize(self)


@dataclass
class TradeSignalDecision:
    symbol: str
    timestamp: datetime
    direction: str = "hold"
    bias: float = 0.0
    strength: float = 0.0
    position_pct: float = 0.0
    reason_codes: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.timestamp = parse_utc_datetime(self.timestamp)
        direction = str(self.direction or "hold").lower()
        if direction not in {"buy", "sell", "hold", "close_long", "close_short"}:
            direction = "hold"
        self.direction = direction
        self.bias = clamp(self.bias, -1.0, 1.0)
        self.strength = clamp(self.strength)
        self.position_pct = clamp(self.position_pct, 0.0, 1.0)
        if not self.reason_codes:
            self.reason_codes = ["trade_signal_neutral"]

    @classmethod
    def hold(cls, symbol: str, timestamp: Optional[datetime] = None, reason: str = "trade_signal_neutral") -> "TradeSignalDecision":
        return cls(symbol=symbol, timestamp=timestamp or utc_now(), reason_codes=[reason])

    def to_dict(self) -> Dict[str, Any]:
        return _serialize(self)


@dataclass
class StructuralMarketContext:
    symbol: str
    timestamp: datetime
    derivatives: DerivativesContext = field(default_factory=DerivativesContext)
    events: EventContext = field(default_factory=EventContext)
    onchain: OnChainContext = field(default_factory=OnChainContext)
    execution: ExecutionContext = field(default_factory=ExecutionContext)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.timestamp = parse_utc_datetime(self.timestamp)

    @classmethod
    def from_row(
        cls,
        row: Mapping[str, Any],
        *,
        symbol: Optional[str] = None,
        timestamp: Optional[Any] = None,
    ) -> "StructuralMarketContext":
        sym = str(symbol or _mapping_get(row, "symbol", default="UNKNOWN") or "UNKNOWN")
        ts = parse_utc_datetime(timestamp or _mapping_get(row, "timestamp", "time", "date", default=None))

        liq_score = safe_float(
            _mapping_get(row, "liquidation_burst_score", "liquidation_score", default=0.0)
        )
        derivatives = DerivativesContext(
            oi_change_z=safe_float(_mapping_get(row, "oi_change_z", default=0.0)),
            funding_z=safe_float(_mapping_get(row, "funding_z", default=0.0)),
            basis_z=safe_float(_mapping_get(row, "basis_z", default=0.0)),
            long_short_ratio_z=safe_float(_mapping_get(row, "long_short_ratio_z", default=0.0)),
            taker_imbalance_z=safe_float(_mapping_get(row, "taker_imbalance_z", "taker_buy_imbalance_z", default=0.0)),
            liquidation_burst_score=liq_score,
            long_liquidation_burst_score=safe_float(
                _mapping_get(row, "long_liquidation_burst_score", "liquidation_long_burst_score", default=liq_score)
            ),
            short_liquidation_burst_score=safe_float(
                _mapping_get(row, "short_liquidation_burst_score", "liquidation_short_burst_score", default=liq_score)
            ),
            crowded_long_score=safe_float(_mapping_get(row, "crowded_long_score", default=0.0)),
            crowded_short_score=safe_float(_mapping_get(row, "crowded_short_score", default=0.0)),
            crowding_side=str(_mapping_get(row, "crowding_side", default="neutral") or "neutral"),
            source=str(_mapping_get(row, "derivatives_source", default="row") or "row"),
            as_of=parse_utc_datetime(_mapping_get(row, "derivatives_as_of", default=ts), default=ts),
            freshness_hours=_optional_float(row, "derivatives_freshness_hours"),
            degraded=bool(_mapping_get(row, "derivatives_degraded", default=False)),
        )
        events = EventContext(
            active_events=list(_mapping_get(row, "active_events", default=[]) or []),
            event_risk_score=safe_float(_mapping_get(row, "event_risk_score", default=0.0)),
            supply_pressure_score=safe_float(_mapping_get(row, "supply_pressure_score", default=0.0)),
            priced_in_score=safe_float(_mapping_get(row, "priced_in_score", default=0.0)),
            absorption_score=safe_float(_mapping_get(row, "absorption_score", default=0.0)),
            source=str(_mapping_get(row, "events_source", default="row") or "row"),
            as_of=parse_utc_datetime(_mapping_get(row, "events_as_of", default=ts), default=ts),
            freshness_hours=_optional_float(row, "events_freshness_hours"),
            degraded=bool(_mapping_get(row, "events_degraded", default=False)),
        )
        onchain = OnChainContext(
            exchange_netflow_z=safe_float(_mapping_get(row, "exchange_netflow_z", "exchange_netflow_score", default=0.0)),
            exchange_balance_change_z=safe_float(_mapping_get(row, "exchange_balance_change_z", "exchange_reserve_pressure", default=0.0)),
            stablecoin_balance_change_z=safe_float(_mapping_get(row, "stablecoin_balance_change_z", "stablecoin_buying_power", default=0.0)),
            whale_flow_score=safe_float(_mapping_get(row, "whale_flow_score", default=0.0)),
            whale_inflow_score=safe_float(_mapping_get(row, "whale_inflow_score", default=0.0)),
            whale_outflow_score=safe_float(_mapping_get(row, "whale_outflow_score", default=0.0)),
            accumulation_score=safe_float(_mapping_get(row, "accumulation_score", default=0.0)),
            distribution_score=safe_float(_mapping_get(row, "distribution_score", default=0.0)),
            onchain_regime=str(_mapping_get(row, "onchain_regime", default="neutral") or "neutral"),
            source=str(_mapping_get(row, "onchain_source", default="row") or "row"),
            as_of=parse_utc_datetime(_mapping_get(row, "onchain_as_of", default=ts), default=ts),
            freshness_hours=_optional_float(row, "onchain_freshness_hours"),
            lookahead_risk=str(_mapping_get(row, "lookahead_risk", "onchain_lookahead_risk", default="unknown") or "unknown"),
            degraded=bool(_mapping_get(row, "onchain_degraded", default=False)),
        )
        execution = ExecutionContext(
            spread_bps=safe_float(_mapping_get(row, "spread_bps", default=0.0)),
            spread_bps_percentile=safe_float(_mapping_get(row, "spread_bps_percentile", default=0.0)),
            depth_score=safe_float(_mapping_get(row, "depth_score", default=1.0), 1.0),
            low_depth_percentile=safe_float(_mapping_get(row, "low_depth_percentile", default=0.0)),
            depth_imbalance=safe_float(_mapping_get(row, "depth_imbalance", default=0.0)),
            fee_bps=safe_float(_mapping_get(row, "fee_bps", default=0.0)),
            execution_risk_score=safe_float(_mapping_get(row, "execution_risk_score", default=0.0)),
            source=str(_mapping_get(row, "execution_source", default="row") or "row"),
            as_of=parse_utc_datetime(_mapping_get(row, "execution_as_of", default=ts), default=ts),
            freshness_hours=_optional_float(row, "execution_freshness_hours"),
            degraded=bool(_mapping_get(row, "execution_degraded", default=False)),
        )
        return cls(
            symbol=sym,
            timestamp=ts,
            derivatives=derivatives,
            events=events,
            onchain=onchain,
            execution=execution,
        )

    def to_dict(self) -> Dict[str, Any]:
        return _serialize(self)


def build_structural_context_from_row(
    row: Mapping[str, Any],
    *,
    symbol: Optional[str] = None,
    timestamp: Optional[Any] = None,
) -> StructuralMarketContext:
    return StructuralMarketContext.from_row(row, symbol=symbol, timestamp=timestamp)
