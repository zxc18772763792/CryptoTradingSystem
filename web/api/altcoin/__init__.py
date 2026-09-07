"""Altcoin radar API package.

Split from a single ~2.7k-line module into a cohesive package. This __init__ is
a thin assembler: it wires every sub-router into one ``router`` and re-exports
the public + monkeypatch surface (``altcoin_api.<name>``) that other modules and
the test suite depend on. The real logic lives in the submodules:

  constants / state / helpers / cache — shared primitives
  scan     — the cache-backed discovery engine (+ /radar/scan, /radar/events)
  detail   — /radar/detail + research-proposal
  alerts   — /alerts/preset create + delete
  universe — /radar/watchlist, /radar/universe, /radar/collector
  pump     — /radar/pump-watchlist
  signals  — /radar/lsr-crowding, /radar/kol-consensus

Tests monkeypatch names at their owning submodule (e.g.
``altcoin_api.scan._resolve_universe``, ``altcoin_api.detail.get_onchain_overview``).
"""
from __future__ import annotations

from fastapi import APIRouter

from core.notifications import notification_manager as notification_manager

# --- extracted submodules (re-exported to preserve the public + patch surface) ---
from .constants import (  # noqa: E402
    ALLOWED_MODES as ALLOWED_MODES,
    ALLOWED_SORTS as ALLOWED_SORTS,
    ALLOWED_UNIVERSE_SCOPES as ALLOWED_UNIVERSE_SCOPES,
    ALLOWED_VIEWS as ALLOWED_VIEWS,
    ALTCOIN_RULE_TYPES as ALTCOIN_RULE_TYPES,
    DEFAULT_EXCHANGE as DEFAULT_EXCHANGE,
    DEFAULT_LIMIT as DEFAULT_LIMIT,
    DEFAULT_SORT as DEFAULT_SORT,
    DEFAULT_TIMEFRAME as DEFAULT_TIMEFRAME,
    DETAIL_LIVE_CHAIN_TIMEOUT_SEC as DETAIL_LIVE_CHAIN_TIMEOUT_SEC,
    DETAIL_ONCHAIN_TIMEOUT_SEC as DETAIL_ONCHAIN_TIMEOUT_SEC,
    MAX_EXPANDED_SIZE as MAX_EXPANDED_SIZE,
    MAX_UNIVERSE_SIZE as MAX_UNIVERSE_SIZE,
    TTL_BY_TIMEFRAME as TTL_BY_TIMEFRAME,
    TTL_BY_VIEW as TTL_BY_VIEW,
    _PUBLIC_MARKET_SNAPSHOT_TTL_SEC as _PUBLIC_MARKET_SNAPSHOT_TTL_SEC,
    AltcoinAlertPresetRequest as AltcoinAlertPresetRequest,
    AltcoinWatchlistMutationRequest as AltcoinWatchlistMutationRequest,
)
from .state import (  # noqa: E402
    _ALTCOIN_SCAN_CACHE as _ALTCOIN_SCAN_CACHE,
    _ALTCOIN_SCAN_LOCKS as _ALTCOIN_SCAN_LOCKS,
    _ALTCOIN_SCAN_LOCKS_GUARD as _ALTCOIN_SCAN_LOCKS_GUARD,
    _ALTCOIN_SCAN_REFRESH_TASKS as _ALTCOIN_SCAN_REFRESH_TASKS,
    _PUBLIC_MARKET_SNAPSHOT_CACHE as _PUBLIC_MARKET_SNAPSHOT_CACHE,
)
from .helpers import (  # noqa: E402
    _age_seconds_at as _age_seconds_at,
    _binance_public_symbol_key as _binance_public_symbol_key,
    _binance_timestamp_from_ticker as _binance_timestamp_from_ticker,
    _build_binance_public_market_snapshot as _build_binance_public_market_snapshot,
    _cache_ttl as _cache_ttl,
    _clone_payload as _clone_payload,
    _filter_fresh_market_frames as _filter_fresh_market_frames,
    _hash_universe as _hash_universe,
    _is_alpha_symbol as _is_alpha_symbol,
    _is_fresh_market_snapshot as _is_fresh_market_snapshot,
    _market_data_stale_cutoff_seconds as _market_data_stale_cutoff_seconds,
    _market_frame_age_seconds as _market_frame_age_seconds,
    _match_binance_public_ticker as _match_binance_public_ticker,
    _needs_detail_chain_fallback as _needs_detail_chain_fallback,
    _normalize_exchange as _normalize_exchange,
    _normalize_mode as _normalize_mode,
    _normalize_sort as _normalize_sort,
    _normalize_symbols as _normalize_symbols,
    _normalize_timeframe as _normalize_timeframe,
    _normalize_universe_scope as _normalize_universe_scope,
    _normalize_view as _normalize_view,
    _pair_symbol_from_base as _pair_symbol_from_base,
    _parse_symbols_param as _parse_symbols_param,
    _preset_definition as _preset_definition,
    _preset_label_from_rule as _preset_label_from_rule,
    _alert_kind_from_rule as _alert_kind_from_rule,
    _resolve_requested_timeframe as _resolve_requested_timeframe,
    _safe_float as _safe_float,
    _serialize_community_snapshot as _serialize_community_snapshot,
    _serialize_derivatives_snapshot as _serialize_derivatives_snapshot,
    _serialize_micro_snapshot as _serialize_micro_snapshot,
    _serialize_whale_snapshot as _serialize_whale_snapshot,
    _should_overlay_coinglass_market_snapshot as _should_overlay_coinglass_market_snapshot,
    _snapshot_age_seconds as _snapshot_age_seconds,
    _utcnow as _utcnow,
)
from .cache import (  # noqa: E402
    _ALTCOIN_SCAN_CACHE_MAX_ENTRIES as _ALTCOIN_SCAN_CACHE_MAX_ENTRIES,
    _build_cached_scan_payload as _build_cached_scan_payload,
    _cache_age_sec as _cache_age_sec,
    _cache_key as _cache_key,
    _cache_lock as _cache_lock,
    _clear_altcoin_scan_cache as _clear_altcoin_scan_cache,
    _evict_altcoin_scan_cache as _evict_altcoin_scan_cache,
    _finalize_scan_payload as _finalize_scan_payload,
    _should_cache_scan_payload as _should_cache_scan_payload,
    build_altcoin_notification_config_key as build_altcoin_notification_config_key,
)
# --- end extracted submodules ---


router = APIRouter()


# --- scan pipeline (extracted engine; routes merged, entry points re-exported) ---
from . import scan as scan  # noqa: E402
from .scan import (  # noqa: E402
    _alert_rules_for_scan as _alert_rules_for_scan,
    _apply_alert_rules_to_rows as _apply_alert_rules_to_rows,
    _compute_scan_payload as _compute_scan_payload,
    _normalize_alert_rule as _normalize_alert_rule,
    _rule_matches_scan_context as _rule_matches_scan_context,
    _universe_matches as _universe_matches,
    _fetch_binance_public_tickers as _fetch_binance_public_tickers,
    _load_active_altcoin_rules as _load_active_altcoin_rules,
    _load_exchange_public_market_snapshots as _load_exchange_public_market_snapshots,
    _load_market_frames as _load_market_frames,
    _load_snapshot_maps as _load_snapshot_maps,
    _map_derivatives_rows_to_requested_symbols as _map_derivatives_rows_to_requested_symbols,
    _resolve_universe as _resolve_universe,
    build_altcoin_notification_context as build_altcoin_notification_context,
    get_altcoin_scan_snapshot as get_altcoin_scan_snapshot,
    warm_default_scan_cache as warm_default_scan_cache,
)
router.include_router(scan.router)

# --- route clusters (own sub-routers, merged into the package router) ---
from . import alerts as alerts  # noqa: E402
from . import detail as detail  # noqa: E402
from . import universe as universe  # noqa: E402
router.include_router(detail.router)
router.include_router(alerts.router)
router.include_router(universe.router)

# --- feature route clusters (own sub-routers, merged into the package router) ---
from . import pump as pump  # noqa: E402
from . import signals as signals  # noqa: E402
router.include_router(pump.router)
router.include_router(signals.router)
