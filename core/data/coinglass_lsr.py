"""LSR (long/short ratio) + KOL-consensus data access via the vip2 relay.

Two distinct products under the relay's `lsr` bucket:
- LSR: smart-money / whale positioning per coin. Broad coverage. The `ranking`
  endpoint surfaces the most crowded coins — a real-time CROWDING dimension for
  the radar. Positioning, NOT a takeoff prediction.
- KOL consensus: distilled high-win-rate-KOL long/short calls, ONLY on 5 majors
  (BTC/ETH/SOL/DOGE/BNB). A macro directional / risk-regime read, not an altcoin
  discovery tool. Paths carry `/consensus/v1/` and MUST keep it.

All calls go through the shared CoinglassClient (relay URL, X-Api-Key, retries,
rate-limit pause). These are non-versioned paths, so COINGLASS_STRIP_API_VERSION
is a no-op for them. Results are TTL-cached in-memory so the radar panels and the
KOL strategy can read them cheaply (and synchronously, via the cache) without
hammering the 11/60s bucket.
"""
from __future__ import annotations

import time
from typing import Any, Dict, List, Mapping, Optional

from loguru import logger

from core.data.coinglass_client import CoinglassClient, CoinglassError

# KOL consensus is only computed for these majors (relay healthz).
KOL_SYMBOLS = ("BTC", "ETH", "SOL", "DOGE", "BNB")

_RANKING_TTL_SEC = 120.0     # crowding shifts intraday
_SIGNAL_TTL_SEC = 90.0
_KOL_TTL_SEC = 1800.0        # consensus is a daily snapshot

_cache: Dict[str, Dict[str, Any]] = {}


def _cache_get(key: str, ttl: float) -> Optional[Any]:
    entry = _cache.get(key)
    if entry and (time.time() - float(entry.get("at", 0.0))) <= ttl:
        return entry.get("value")
    return None


def _cache_put(key: str, value: Any) -> None:
    _cache[key] = {"at": time.time(), "value": value}


def _norm_symbol(symbol: str) -> str:
    return str(symbol or "").strip().upper().replace("/USDT", "").replace("USDT", "") or "BTC"


# ── LSR ──────────────────────────────────────────────────────────────────────

async def fetch_lsr_ranking(mode: str = "trader", limit: int = 50, *, refresh: bool = False) -> Dict[str, Any]:
    """Current LSR ranking (most crowded first by 2m ratio change). Cached."""
    mode = "whale" if str(mode).lower() == "whale" else "trader"
    key = f"ranking:{mode}:{limit}"
    if not refresh:
        cached = _cache_get(key, _RANKING_TTL_SEC)
        if cached is not None:
            return cached
    try:
        async with CoinglassClient() as client:
            resp = await client._request_json(
                "/api/lsr/ranking", params={"mode": mode, "limit": int(limit), "offset": 0}, manual=True
            )
        payload = resp.get("payload") or {}
        rows = payload.get("data") or []
        out = {"available": bool(payload.get("success")), "mode": mode, "rows": rows,
               "updated_at": payload.get("updated_at"), "total": payload.get("total"), "ts": time.time()}
        _cache_put(key, out)
        return out
    except CoinglassError as exc:
        return {"available": False, "error": str(exc)[:120], "mode": mode, "rows": []}


async def fetch_lsr_signals(symbols: List[str], mode: str = "trader") -> Dict[str, Dict[str, Any]]:
    """Batch LSR signals for a symbol list. Returns {SYMBOL: row} for found ones."""
    bases = [_norm_symbol(s) for s in symbols if s]
    if not bases:
        return {}
    mode = "whale" if str(mode).lower() == "whale" else "trader"
    try:
        async with CoinglassClient() as client:
            resp = await client._request_json(
                "/api/lsr/signals", params={"symbols": ",".join(bases[:100]), "mode": mode}, manual=True
            )
        payload = resp.get("payload") or {}
        out: Dict[str, Dict[str, Any]] = {}
        for row in payload.get("data") or []:
            if not row.get("found", True):
                continue
            sym = _norm_symbol(row.get("symbol") or "")
            if sym:
                out[sym] = row
        return out
    except CoinglassError as exc:
        logger.warning(f"fetch_lsr_signals failed: {str(exc)[:100]}")
        return {}


def crowding_label(ratio: Optional[float]) -> str:
    """Human read of a long/short ratio into a crowding tag."""
    try:
        r = float(ratio)
    except (TypeError, ValueError):
        return "unknown"
    if r <= 0:
        return "unknown"
    if r >= 3.0:
        return "极度拥挤多"
    if r >= 1.6:
        return "偏拥挤多"
    if r <= 0.33:
        return "极度拥挤空"
    if r <= 0.625:
        return "偏拥挤空"
    return "均衡"


# ── KOL consensus (5 majors only) ────────────────────────────────────────────

async def fetch_kol_consensus(*, refresh: bool = False) -> Dict[str, Any]:
    """All-symbol KOL consensus snapshot (BTC/ETH/SOL/DOGE/BNB). Cached."""
    key = "kol:symbols"
    if not refresh:
        cached = _cache_get(key, _KOL_TTL_SEC)
        if cached is not None:
            return cached
    try:
        async with CoinglassClient() as client:
            resp = await client._request_json("/api/lsr/consensus/v1/symbols", params={}, manual=True)
        payload = resp.get("payload") or {}
        rows = payload.get("symbols") or []
        by_symbol: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            sym = _norm_symbol(row.get("display_symbol") or row.get("symbol") or "")
            if sym:
                by_symbol[sym] = row
        out = {"available": bool(rows), "by_symbol": by_symbol, "rows": rows,
               "risk_tone": _aggregate_risk_tone(rows), "ts": time.time()}
        _cache_put(key, out)
        return out
    except CoinglassError as exc:
        return {"available": False, "error": str(exc)[:120], "by_symbol": {}, "rows": []}


def _aggregate_risk_tone(rows: List[Mapping[str, Any]]) -> Dict[str, Any]:
    """Roll the 5-major consensus into one risk-regime read: confidence-weighted
    mean of trust_adjusted_bias (>0 risk-on, <0 risk-off)."""
    num = den = 0.0
    longs = shorts = neutral = 0
    for r in rows:
        try:
            bias = float(r.get("trust_adjusted_bias") or 0.0)
            conf = float(r.get("confidence") or 0.0)
        except (TypeError, ValueError):
            continue
        num += bias * max(conf, 0.0)
        den += max(conf, 0.0)
        dec = str(r.get("decision") or "").lower()
        if dec == "long":
            longs += 1
        elif dec == "short":
            shorts += 1
        else:
            neutral += 1
    score = (num / den) if den > 0 else 0.0
    tone = "risk_on" if score > 0.05 else "risk_off" if score < -0.05 else "neutral"
    label = {"risk_on": "偏多/可冒险", "risk_off": "偏空/收着点", "neutral": "中性"}[tone]
    return {"score": round(score, 4), "tone": tone, "label": label,
            "long": longs, "short": shorts, "neutral": neutral}


def get_cached_kol_symbol(symbol: str) -> Optional[Dict[str, Any]]:
    """Sync read of one coin's cached KOL consensus (for the strategy). None if
    not one of the 5 majors or cache empty/stale."""
    snap = _cache_get("kol:symbols", _KOL_TTL_SEC)
    if not snap:
        return None
    return (snap.get("by_symbol") or {}).get(_norm_symbol(symbol))


def get_cached_risk_tone() -> Optional[Dict[str, Any]]:
    snap = _cache_get("kol:symbols", _KOL_TTL_SEC)
    return snap.get("risk_tone") if snap else None
