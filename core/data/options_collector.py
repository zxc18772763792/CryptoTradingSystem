"""Deribit public options market data collector.

Fetches ATM implied volatility, IV skew proxy, and Put/Call OI ratio.
No authentication required — uses Deribit public REST API.

The "skew_25d" here is a proxy computed as:
    (avg_put_IV - avg_call_IV) / atm_IV
A positive value means put IVs are elevated (fear/hedging premium).
A negative value means call IVs are elevated (FOMO/upside demand).

True 25-delta skew requires locating specific strikes; this proxy
uses portfolio-wide averages and is sufficient for directional signals.

Usage:
    snap = await options_collector.fetch_snapshot("BTC")
    # snap.atm_iv, snap.skew_25d, snap.put_call_ratio, snap.signal
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

import aiohttp
from loguru import logger


@dataclass
class OptionsSnapshot:
    currency: str           # "BTC" | "ETH"
    atm_iv: float           # average call IV (annualized, 0-1 range)
    skew_25d: float         # (avg_put_iv - avg_call_iv) / atm_iv; >0 = fear premium
    put_call_ratio: float   # put OI / call OI; >1 = bearish hedging dominates
    n_calls: int            # number of call instruments found
    n_puts: int             # number of put instruments found
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def signal(self) -> str:
        """Derived directional signal from options market structure."""
        if self.skew_25d > 0.05 or self.put_call_ratio > 1.3:
            return "fear"
        if self.skew_25d < -0.05 and self.put_call_ratio < 0.7:
            return "greed"
        return "neutral"

    def to_dict(self) -> Dict:
        return {
            "available": True,
            "currency": self.currency,
            "atm_iv": round(self.atm_iv, 4),
            "atm_iv_pct": round(self.atm_iv * 100, 2),
            "skew_25d": round(self.skew_25d, 4),
            "put_call_ratio": round(self.put_call_ratio, 4),
            "n_calls": self.n_calls,
            "n_puts": self.n_puts,
            "signal": self.signal,
            "timestamp": self.timestamp.isoformat(),
        }


class DeribitOptionsCollector:
    """Fetches Deribit public options book summary — no API key required."""

    _BASE = "https://www.deribit.com/api/v2/public"
    _CACHE_TTL_SEC = 300  # 5-minute cache to avoid hammering public API

    def __init__(self, timeout: int = 12) -> None:
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._cache: Dict[str, tuple[float, OptionsSnapshot]] = {}

    async def fetch_snapshot(self, currency: str = "BTC") -> Optional[OptionsSnapshot]:
        """Return current options snapshot. Uses in-memory cache (5 min TTL)."""
        key = currency.upper()
        now = asyncio.get_event_loop().time()
        cached_at, cached_snap = self._cache.get(key, (0.0, None))  # type: ignore[assignment]
        if cached_snap is not None and (now - cached_at) < self._CACHE_TTL_SEC:
            return cached_snap

        snap = await self._fetch_from_api(key)
        if snap is not None:
            self._cache[key] = (now, snap)
        return snap

    async def _fetch_from_api(self, currency: str) -> Optional[OptionsSnapshot]:
        url = f"{self._BASE}/get_book_summary_by_currency"
        params = {"currency": currency, "kind": "option"}
        try:
            async with aiohttp.ClientSession(timeout=self._timeout) as session:
                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        logger.debug(f"DeribitOptions: HTTP {resp.status} for {currency}")
                        return None
                    data = await resp.json()
                    instruments: List[Dict] = data.get("result") or []
                    if not instruments:
                        logger.debug(f"DeribitOptions: empty result for {currency}")
                        return None
                    return self._parse(currency, instruments)
        except asyncio.TimeoutError:
            logger.debug(f"DeribitOptions: timeout fetching {currency} options")
            return None
        except Exception as exc:
            logger.debug(f"DeribitOptions: {exc}")
            return None

    @staticmethod
    def _parse(currency: str, instruments: List[Dict]) -> Optional[OptionsSnapshot]:
        call_ivs: List[float] = []
        put_ivs: List[float] = []
        call_oi = 0.0
        put_oi = 0.0

        for inst in instruments:
            name = str(inst.get("instrument_name") or "")
            iv = inst.get("mark_iv")
            oi = float(inst.get("open_interest") or 0)
            if iv is None:
                continue
            iv_f = float(iv)
            if iv_f <= 0:
                continue

            if name.endswith("-C"):
                call_ivs.append(iv_f)
                call_oi += oi
            elif name.endswith("-P"):
                put_ivs.append(iv_f)
                put_oi += oi

        if not call_ivs:
            return None

        # Convert from percent (Deribit returns IV in %) to fraction
        avg_call_iv = sum(call_ivs) / len(call_ivs) / 100.0
        avg_put_iv  = (sum(put_ivs) / len(put_ivs) / 100.0) if put_ivs else avg_call_iv
        atm_iv = avg_call_iv

        skew = ((avg_put_iv - avg_call_iv) / atm_iv) if atm_iv > 0 else 0.0
        pc_ratio = (put_oi / call_oi) if call_oi > 0 else 1.0

        return OptionsSnapshot(
            currency=currency,
            atm_iv=round(atm_iv, 4),
            skew_25d=round(skew, 4),
            put_call_ratio=round(pc_ratio, 4),
            n_calls=len(call_ivs),
            n_puts=len(put_ivs),
        )


# Module-level singleton — shared across all callers
options_collector = DeribitOptionsCollector()
