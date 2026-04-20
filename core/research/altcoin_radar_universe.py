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
import threading
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

from config.settings import settings


# ── hardcoded narrative / meme watchlist ───────────────────────────────────
# Cover BSC memes, Runes/Ordinals, exchange concepts, AI narratives, etc.
# Adjust over time without changing calling code.

ALTCOIN_WATCHLIST: List[str] = [
    # Ordinals / BTC ecosystem
    "ORDI/USDT",
    "SATS/USDT",
    "RATS/USDT",
    # Meme / community
    "PEPE/USDT",
    "FLOKI/USDT",
    "BONK/USDT",
    "WIF/USDT",
    "BOME/USDT",
    "MEME/USDT",
    "NEIRO/USDT",
    "TURBO/USDT",
    # AI narrative
    "TAO/USDT",
    "FET/USDT",
    "AGIX/USDT",
    "RNDR/USDT",
    "AKT/USDT",
    "OCEAN/USDT",
    # DeFi / real yield
    "GMX/USDT",
    "GNS/USDT",
    "JOE/USDT",
    # Gaming / Metaverse
    "AXS/USDT",
    "SAND/USDT",
    "MANA/USDT",
    "GALA/USDT",
    "IMX/USDT",
    # Infrastructure
    "PYTH/USDT",
    "JTO/USDT",
    "W/USDT",
    "JUP/USDT",
    # Exchange / CEX tokens
    "BGB/USDT",
    "GT/USDT",
    "OKB/USDT",
    # L2 / modular
    "STRK/USDT",
    "MANTA/USDT",
    "ALT/USDT",
    "ZETA/USDT",
]

_WATCHLIST_LOCK = threading.Lock()
_WATCHLIST_STORAGE_PATH = settings.BASE_DIR / "data" / "config" / "altcoin_radar_watchlist.json"

# Boards / sectors used in narrative scoring (symbol → sector label)
SECTOR_MAP: Dict[str, str] = {
    "ORDI/USDT": "ordinals",
    "SATS/USDT": "ordinals",
    "RATS/USDT": "ordinals",
    "PEPE/USDT": "meme",
    "FLOKI/USDT": "meme",
    "BONK/USDT": "meme",
    "WIF/USDT": "meme",
    "BOME/USDT": "meme",
    "MEME/USDT": "meme",
    "NEIRO/USDT": "meme",
    "TURBO/USDT": "meme",
    "TAO/USDT": "ai",
    "FET/USDT": "ai",
    "AGIX/USDT": "ai",
    "RNDR/USDT": "ai",
    "AKT/USDT": "ai",
    "OCEAN/USDT": "ai",
    "GMX/USDT": "defi",
    "GNS/USDT": "defi",
    "JOE/USDT": "defi",
    "AAVE/USDT": "defi",
    "UNI/USDT": "defi",
    "MKR/USDT": "defi",
    "AXS/USDT": "gaming",
    "SAND/USDT": "gaming",
    "MANA/USDT": "gaming",
    "GALA/USDT": "gaming",
    "IMX/USDT": "gaming",
    "PYTH/USDT": "infra",
    "JTO/USDT": "infra",
    "W/USDT": "infra",
    "JUP/USDT": "infra",
    "ARB/USDT": "l2",
    "OP/USDT": "l2",
    "STRK/USDT": "l2",
    "MANTA/USDT": "l2",
    "ALT/USDT": "l2",
    "ZETA/USDT": "l2",
    "SOL/USDT": "l1",
    "AVAX/USDT": "l1",
    "NEAR/USDT": "l1",
    "APT/USDT": "l1",
    "SUI/USDT": "l1",
    "INJ/USDT": "l1",
}

MAX_EXPANDED = 100


# ── helpers ────────────────────────────────────────────────────────────────

def _normalize(symbols: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen: set = set()
    for s in symbols:
        text = str(s or "").strip().upper()
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
    watch = _normalize(ALTCOIN_WATCHLIST)

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


def get_watchlist_symbols() -> List[str]:
    """Return current hardcoded watchlist."""
    with _WATCHLIST_LOCK:
        persisted = _load_watchlist_from_disk()
        if persisted is not None:
            return persisted
        return _normalize(ALTCOIN_WATCHLIST)


def add_watchlist_symbol(symbol: str) -> List[str]:
    normalized_symbol = _normalize([symbol])
    if not normalized_symbol:
        return get_watchlist_symbols()
    with _WATCHLIST_LOCK:
        current = _load_watchlist_from_disk()
        if current is None:
            current = _normalize(ALTCOIN_WATCHLIST)
        return _persist_watchlist(current + normalized_symbol)


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
        return _persist_watchlist(current)


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
