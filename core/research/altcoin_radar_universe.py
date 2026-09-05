"""Universe scope management for the altcoin radar.

Three tiers
-----------
research   : current research symbol pool (≤30, default)
expanded   : up to ~100 symbols (research + CoinGlass top + active)
watchlist  : manually curated narrative / meme watchlist

The module never fetches data itself; callers supply the raw symbol lists
and this module merges / deduplicates them according to scope.
"""
from __future__ import annotations

import json
import re
import threading
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

from config.settings import settings


# ── hardcoded narrative / meme watchlist ───────────────────────────────────
# Cover BSC memes, Runes/Ordinals, exchange concepts, AI narratives, etc.
# Adjust over time without changing calling code.

ALTCOIN_WATCHLIST: List[str] = [
    # ── L1 majors (anchors so radar always has the high-volume references) ──
    "SOL/USDT",
    "AVAX/USDT",
    "NEAR/USDT",
    "APT/USDT",
    "SUI/USDT",
    "INJ/USDT",
    "SEI/USDT",
    "TIA/USDT",
    # ── Ordinals / BTC ecosystem ──
    "ORDI/USDT",
    "SATS/USDT",
    "RATS/USDT",
    "STX/USDT",
    # ── Meme / community ──
    "PEPE/USDT",
    "FLOKI/USDT",
    "BONK/USDT",
    "WIF/USDT",
    "BOME/USDT",
    "MEME/USDT",
    "NEIRO/USDT",
    "TURBO/USDT",
    "POPCAT/USDT",
    "MEW/USDT",
    "PNUT/USDT",
    "GOAT/USDT",
    # ── AI narrative ──
    "TAO/USDT",
    "FET/USDT",
    "AGIX/USDT",
    "RENDER/USDT",
    "AKT/USDT",
    "OCEAN/USDT",
    "WLD/USDT",
    "AI16Z/USDT",
    "VIRTUAL/USDT",
    "ARKM/USDT",
    # ── DePIN / RWA ──
    "ONDO/USDT",
    "HNT/USDT",
    "IOTX/USDT",
    "GRT/USDT",
    "POL/USDT",
    # ── DeFi / real yield ──
    "GMX/USDT",
    "GNS/USDT",
    "JOE/USDT",
    "AAVE/USDT",
    "UNI/USDT",
    "MKR/USDT",
    "CRV/USDT",
    "PENDLE/USDT",
    "DYDX/USDT",
    "ENA/USDT",
    "ETHFI/USDT",
    # ── Gaming / Metaverse ──
    "AXS/USDT",
    "SAND/USDT",
    "MANA/USDT",
    "GALA/USDT",
    "IMX/USDT",
    "BEAM/USDT",
    "PIXEL/USDT",
    # ── Solana ecosystem infra ──
    "PYTH/USDT",
    "JTO/USDT",
    "JUP/USDT",
    "W/USDT",
    "KMNO/USDT",
    "DRIFT/USDT",
    # ── Exchange / CEX tokens ──
    "BGB/USDT",
    "GT/USDT",
    "OKB/USDT",
    "BNB/USDT",
    # ── L2 / modular ──
    "ARB/USDT",
    "OP/USDT",
    "STRK/USDT",
    "MANTA/USDT",
    "ALT/USDT",
    "ZETA/USDT",
    "ZK/USDT",
    "BLAST/USDT",
    # ── Newer narrative / 2025 listings ──
    "PYUSD/USDT",
    "EIGEN/USDT",
    "REZ/USDT",
    "IO/USDT",
    "ZRO/USDT",
    "OMNI/USDT",
    "USUAL/USDT",
    "MOVE/USDT",
    "ME/USDT",
    "VANA/USDT",
]

# Reentrant: ``universe_meta`` re-enters ``get_watchlist_symbols`` while the API
# layer holds it in the same call chain. A plain Lock would deadlock per coroutine.
_WATCHLIST_LOCK = threading.RLock()
_WATCHLIST_STORAGE_PATH = settings.BASE_DIR / "data" / "config" / "altcoin_radar_watchlist.json"
_WATCHLIST_SYMBOL_ALIASES: Dict[str, str] = {
    # RNDR was migrated to RENDER on major venues. Keep old persisted entries usable.
    "RNDR/USDT": "RENDER/USDT",
    "MATIC/USDT": "POL/USDT",
}
_WATCHLIST_BASE_ALIASES: Dict[str, str] = {
    "RNDR": "RENDER",
    "MATIC": "POL",
}


def normalize_altcoin_pair(symbol: object) -> str:
    """Return one canonical ``BASE/USDT`` key for radar symbols.

    Inputs arrive as forms such as ``TAG/USDT``, ``TAGUSDT`` and
    ``TAG-USDT-SWAP``. Treating those strings as different keys creates
    duplicate candidates downstream.
    """
    text = str(symbol or "").strip().upper()
    if not text:
        return ""
    if "/" in text:
        text = text.split("/", 1)[0]
    else:
        suffixes = (
            "-USDT-SWAP", "-USD-SWAP", "_USDT", "_USD",
            "-USDT", "-USD", "USDT", "PERP",
        )
        for suffix in suffixes:
            if text.endswith(suffix) and len(text) > len(suffix):
                text = text[: -len(suffix)]
                break
    base = re.sub(r"[^A-Z0-9]", "", text)
    base = _WATCHLIST_BASE_ALIASES.get(base, base)
    return f"{base}/USDT" if base else ""

# Tiny in-memory cache so a burst of API calls (radar dashboard hits this on
# every tab focus) doesn't repeatedly stat + read the JSON file. TTL kept
# short so admin mutations propagate to listeners within one render tick.
_WATCHLIST_CACHE_TTL_SEC = 5.0
_WATCHLIST_CACHE: Dict[str, object] = {"value": None, "expires_at": 0.0}

# Boards / sectors used in narrative scoring (symbol → sector label).
# Keep keys sorted by sector for easier upkeep when narratives shift.
SECTOR_MAP: Dict[str, str] = {
    # ── L1 ──
    "SOL/USDT": "l1",
    "AVAX/USDT": "l1",
    "NEAR/USDT": "l1",
    "APT/USDT": "l1",
    "SUI/USDT": "l1",
    "INJ/USDT": "l1",
    "SEI/USDT": "l1",
    "TIA/USDT": "l1",
    "MOVE/USDT": "l1",
    # ── Ordinals / BTC eco ──
    "ORDI/USDT": "ordinals",
    "SATS/USDT": "ordinals",
    "RATS/USDT": "ordinals",
    "STX/USDT": "ordinals",
    # ── Meme ──
    "PEPE/USDT": "meme",
    "FLOKI/USDT": "meme",
    "BONK/USDT": "meme",
    "WIF/USDT": "meme",
    "BOME/USDT": "meme",
    "MEME/USDT": "meme",
    "NEIRO/USDT": "meme",
    "TURBO/USDT": "meme",
    "POPCAT/USDT": "meme",
    "MEW/USDT": "meme",
    "PNUT/USDT": "meme",
    "GOAT/USDT": "meme",
    # ── AI ──
    "TAO/USDT": "ai",
    "FET/USDT": "ai",
    "AGIX/USDT": "ai",
    "RNDR/USDT": "ai",
    "RENDER/USDT": "ai",
    "AKT/USDT": "ai",
    "OCEAN/USDT": "ai",
    "WLD/USDT": "ai",
    "AI16Z/USDT": "ai",
    "VIRTUAL/USDT": "ai",
    "ARKM/USDT": "ai",
    # ── DePIN / RWA ──
    "ONDO/USDT": "rwa",
    "HNT/USDT": "depin",
    "IOTX/USDT": "depin",
    "GRT/USDT": "depin",
    "POL/USDT": "infra",
    # ── DeFi ──
    "GMX/USDT": "defi",
    "GNS/USDT": "defi",
    "JOE/USDT": "defi",
    "AAVE/USDT": "defi",
    "UNI/USDT": "defi",
    "MKR/USDT": "defi",
    "CRV/USDT": "defi",
    "PENDLE/USDT": "defi",
    "DYDX/USDT": "defi",
    "ENA/USDT": "defi",
    "ETHFI/USDT": "defi",
    # ── Gaming ──
    "AXS/USDT": "gaming",
    "SAND/USDT": "gaming",
    "MANA/USDT": "gaming",
    "GALA/USDT": "gaming",
    "IMX/USDT": "gaming",
    "BEAM/USDT": "gaming",
    "PIXEL/USDT": "gaming",
    # ── Solana ecosystem infra ──
    "PYTH/USDT": "infra",
    "JTO/USDT": "infra",
    "W/USDT": "infra",
    "JUP/USDT": "infra",
    "KMNO/USDT": "infra",
    "DRIFT/USDT": "infra",
    "ME/USDT": "infra",
    # ── CEX tokens ──
    "BGB/USDT": "cex",
    "GT/USDT": "cex",
    "OKB/USDT": "cex",
    "BNB/USDT": "cex",
    # ── L2 / modular ──
    "ARB/USDT": "l2",
    "OP/USDT": "l2",
    "STRK/USDT": "l2",
    "MANTA/USDT": "l2",
    "ALT/USDT": "l2",
    "ZETA/USDT": "l2",
    "ZK/USDT": "l2",
    "BLAST/USDT": "l2",
    # ── Restaking / new narratives ──
    "EIGEN/USDT": "restaking",
    "REZ/USDT": "restaking",
    "IO/USDT": "depin",
    "ZRO/USDT": "infra",
    "OMNI/USDT": "l2",
    "USUAL/USDT": "rwa",
    "PYUSD/USDT": "rwa",
    "VANA/USDT": "ai",
}

MAX_EXPANDED = 100


# ── helpers ────────────────────────────────────────────────────────────────

def _normalize(symbols: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen: set = set()
    for s in symbols:
        text = normalize_altcoin_pair(s)
        text = _WATCHLIST_SYMBOL_ALIASES.get(text, text)
        if text and text not in seen:
            seen.add(text)
            out.append(text)
    return out


def _watchlist_storage_path() -> Path:
    path = Path(_WATCHLIST_STORAGE_PATH)
    return path if path.is_absolute() else (settings.BASE_DIR / path).resolve()


def _load_watchlist_from_disk() -> Optional[List[str]]:
    path = _watchlist_storage_path()
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    symbols = payload.get("symbols") if isinstance(payload, dict) else payload
    if not isinstance(symbols, list):
        return None
    return _normalize(symbols)


def _persist_watchlist(symbols: Sequence[str]) -> List[str]:
    normalized = _normalize(symbols)
    path = _watchlist_storage_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"symbols": normalized}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return normalized


# ── scope resolution ───────────────────────────────────────────────────────

def resolve_universe_scope(
    scope: str,
    *,
    research_symbols: Sequence[str],
    coinglass_symbols: Optional[Sequence[str]] = None,
    extra_active: Optional[Sequence[str]] = None,
) -> List[str]:
    """Return symbol list for the given universe scope.

    Parameters
    ----------
    scope            : "research" | "expanded" | "watchlist"
    research_symbols : symbols from the existing research pool
    coinglass_symbols: symbols returned by CoinGlass top-N list
    extra_active     : any extra symbols discovered as active (volume/volatility)
    """
    s = str(scope or "research").strip().lower()

    research = _normalize(research_symbols)
    cg = _normalize(coinglass_symbols or [])
    active = _normalize(extra_active or [])
    watch = get_watchlist_symbols()

    if s == "watchlist":
        # Watchlist only, but keep research overlap too
        combined = _normalize(watch + research)
        return combined[:MAX_EXPANDED]

    if s == "expanded":
        # Merge: research first (priority), then CoinGlass, then active, then watchlist
        combined = _normalize(research + cg + active + watch)
        return combined[:MAX_EXPANDED]

    # Default: "research"
    return research


def get_sector(symbol: str) -> str:
    """Return sector label for a symbol, or empty string if unknown."""
    key = str(symbol or "").strip().upper()
    return SECTOR_MAP.get(key, "")


def _invalidate_watchlist_cache() -> None:
    _WATCHLIST_CACHE["value"] = None
    _WATCHLIST_CACHE["expires_at"] = 0.0


def get_watchlist_symbols() -> List[str]:
    """Return the active watchlist (persisted overlay if present, else built-in).

    Cached for ``_WATCHLIST_CACHE_TTL_SEC`` to absorb radar dashboard bursts
    (every tab focus calls this) without re-reading the JSON file. Cache is
    invalidated on every mutation.
    """
    with _WATCHLIST_LOCK:
        now = time.monotonic()
        cached_value = _WATCHLIST_CACHE.get("value")
        if isinstance(cached_value, list) and float(_WATCHLIST_CACHE.get("expires_at") or 0.0) > now:
            # Return a copy so callers can't accidentally mutate the cached list.
            return list(cached_value)
        persisted = _load_watchlist_from_disk()
        resolved = persisted if persisted else _normalize(ALTCOIN_WATCHLIST)
        _WATCHLIST_CACHE["value"] = list(resolved)
        _WATCHLIST_CACHE["expires_at"] = now + _WATCHLIST_CACHE_TTL_SEC
        return list(resolved)


def add_watchlist_symbol(symbol: str) -> List[str]:
    normalized_symbol = _normalize([symbol])
    if not normalized_symbol:
        return get_watchlist_symbols()
    with _WATCHLIST_LOCK:
        current = _load_watchlist_from_disk()
        if current is None:
            current = _normalize(ALTCOIN_WATCHLIST)
        result = _persist_watchlist(current + normalized_symbol)
        _invalidate_watchlist_cache()
        return result


def remove_watchlist_symbol(symbol: str) -> List[str]:
    normalized_symbol = _normalize([symbol])
    if not normalized_symbol:
        return get_watchlist_symbols()
    target = normalized_symbol[0]
    with _WATCHLIST_LOCK:
        current = _load_watchlist_from_disk()
        if current is None:
            current = _normalize(ALTCOIN_WATCHLIST)
        current = [item for item in current if item != target]
        result = _persist_watchlist(current)
        _invalidate_watchlist_cache()
        return result


def universe_meta(
    symbols_used: Sequence[str],
    scope: str,
) -> Dict[str, object]:
    """Return metadata about the resolved universe for API responses."""
    used = _normalize(symbols_used)
    watchlist_set = {w.upper() for w in get_watchlist_symbols()}
    watchlist_hits = [s for s in used if s in watchlist_set]
    sectors: Dict[str, int] = {}
    board_membership: Dict[str, str] = {}
    for s in used:
        sec = get_sector(s)
        if sec:
            sectors[sec] = sectors.get(sec, 0) + 1
            board_membership[s] = sec
    return {
        "universe_scope": str(scope or "research"),
        "symbols_count": len(used),
        "watchlist_hits": watchlist_hits,
        "watchlist_hit_count": len(watchlist_hits),
        "sectors": sectors,
        "board_membership": board_membership,
        "watchlist_storage": str(_watchlist_storage_path()),
    }
