"""Binance perpetual marketdata WS client skeleton."""
from __future__ import annotations

from typing import Dict, Iterable

from core.marketdata.ws_client import WSClient, WSClientConfig


class BinancePerpWSClient(WSClient):
    """Skeleton wrapper for Binance Futures websocket subscriptions."""

    def __init__(self, symbols: Iterable[str] | None = None, base_url: str = "wss://fstream.binance.com/stream"):
        super().__init__(WSClientConfig(url=base_url, name="binance_perp_ws"))
        self.symbols = [str(s).upper().replace("/", "") for s in (symbols or [])]

    async def subscribe_book_ticker(self, symbols: Iterable[str] | None = None) -> None:
        syms = [s.upper().replace("/", "") for s in (symbols or self.symbols)]
        await self.subscribe({"type": "book_ticker", "symbols": syms})

    async def subscribe_depth(self, symbols: Iterable[str] | None = None, level: int = 20) -> None:
        syms = [s.upper().replace("/", "") for s in (symbols or self.symbols)]
        await self.subscribe({"type": "depth", "symbols": syms, "level": int(level)})

    async def subscribe_agg_trade(self, symbols: Iterable[str] | None = None) -> None:
        syms = [s.upper().replace("/", "") for s in (symbols or self.symbols)]
        await self.subscribe({"type": "agg_trade", "symbols": syms})

    async def subscribe_kline(self, symbols: Iterable[str] | None = None, interval: str = "5m") -> None:
        syms = [s.upper().replace("/", "") for s in (symbols or self.symbols)]
        await self.subscribe({"type": "kline", "symbols": syms, "interval": interval})

    async def subscribe_mark_price(self, symbols: Iterable[str] | None = None) -> None:
        syms = [s.upper().replace("/", "") for s in (symbols or self.symbols)]
        await self.subscribe({"type": "mark_price", "symbols": syms})

    async def subscribe_funding(self, symbols: Iterable[str] | None = None) -> None:
        syms = [s.upper().replace("/", "") for s in (symbols or self.symbols)]
        await self.subscribe({"type": "funding", "symbols": syms})

    @staticmethod
    def normalize_event(message: Dict) -> Dict:
        """TODO: map Binance raw ws message -> internal normalized event."""
        return dict(message or {})

