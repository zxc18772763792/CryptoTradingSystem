"""Static constants and request models for the altcoin radar API.

Split out of the former monolithic web/api/altcoin.py. Pure data with no
intra-package dependencies; re-exported from the package __init__ so the public
surface (altcoin_api.DEFAULT_EXCHANGE, .AltcoinAlertPresetRequest, ...) is
unchanged.
"""
from __future__ import annotations

from typing import List

from pydantic import BaseModel, Field


DEFAULT_EXCHANGE = "binance"
DEFAULT_TIMEFRAME = "4h"
# Budgets for the radar-detail external sources (onchain overview / live chain
# context). Production incident: an unbounded chain call hung the whole detail
# endpoint past the frontend's 60s budget, so the drawer never opened.
DETAIL_ONCHAIN_TIMEOUT_SEC = 15.0
DETAIL_LIVE_CHAIN_TIMEOUT_SEC = 10.0
DEFAULT_LIMIT = 30
DEFAULT_SORT = "priority"
MAX_UNIVERSE_SIZE = 60
MAX_EXPANDED_SIZE = 400
TTL_BY_TIMEFRAME = {"1h": 120.0, "4h": 300.0, "1d": 900.0}
# Phase 1: shorter cache for faster radar views
TTL_BY_VIEW = {"15m": 30.0, "1h": 60.0, "4h": 300.0}
ALLOWED_SORTS = {
    "priority", "upside", "layout", "alert", "anomaly", "accumulation", "control", "chain", "heat",
    # Phase 1 new sorts
    "ignition", "continuation", "rank_jump", "crowding",
    # Phase 2 new sorts
    "narrative", "meme_rotation",
}
ALLOWED_MODES = {"perp", "narrative", "combined"}
ALLOWED_VIEWS = {"15m", "1h", "4h"}
ALLOWED_UNIVERSE_SCOPES = {"research", "expanded", "alpha", "watchlist"}
ALTCOIN_RULE_TYPES = {
    "altcoin_score_above",
    "altcoin_rank_top_n",
    "altcoin_ignition_cross_up",
    "altcoin_rank_jump_top_n",
    "altcoin_crowding_risk_spike",
    "altcoin_narrative_heat_spike",
}
_PUBLIC_MARKET_SNAPSHOT_TTL_SEC = 45.0


class AltcoinAlertPresetRequest(BaseModel):
    preset: str
    exchange: str = DEFAULT_EXCHANGE
    timeframe: str = DEFAULT_TIMEFRAME
    symbol: str
    universe_symbols: List[str] = Field(default_factory=list)
    channels: List[str] = Field(default_factory=lambda: ["feishu"])
    mode: str = "combined"
    view: str = ""
    universe_scope: str = "research"


class AltcoinWatchlistMutationRequest(BaseModel):
    symbol: str
