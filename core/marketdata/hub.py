"""In-memory market-data hub for WS-first ticker migration.

The hub is deliberately small: it records the latest normalized tick per
exchange/symbol/channel, keeps source/freshness metadata, and exposes a status
snapshot for the web runtime. It is not a persistence layer and it is not an
execution authority; live trading must still use REST reconciliation where
required.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Literal, Optional


MarketTickSource = Literal["ws", "rest_fallback", "rest_snapshot"]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def normalize_market_symbol(symbol: Any) -> str:
    """Normalize exchange/ccxt symbols to the app's spot-style key."""
    text = str(symbol or "").strip().upper()
    if not text:
        return ""
    if ":" in text:
        text = text.split(":", 1)[0]
    if "/" in text:
        base, quote = text.split("/", 1)
        return f"{base}/{quote}"
    for quote in ("USDT", "USDC", "BUSD", "BTC", "ETH"):
        if text.endswith(quote) and len(text) > len(quote):
            return f"{text[:-len(quote)]}/{quote}"
    return text


def normalize_exchange_name(exchange: Any) -> str:
    return str(exchange or "").strip().lower()


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out


def _coerce_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        raw = float(value)
        if raw <= 0:
            return None
        if raw > 10_000_000_000:
            raw = raw / 1000.0
        dt = datetime.fromtimestamp(raw, tz=timezone.utc)
    else:
        text = str(value or "").strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


@dataclass(frozen=True)
class MarketTick:
    exchange: str
    symbol: str
    channel: str = "ticker"
    last: Optional[float] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    mark: Optional[float] = None
    volume: Optional[float] = None
    timestamp_exchange: Optional[datetime] = None
    timestamp_received: datetime = field(default_factory=_utc_now)
    source: MarketTickSource = "ws"
    sequence: Optional[int] = None
    raw_symbol: Optional[str] = None

    def age_ms(self, *, now: Optional[datetime] = None) -> int:
        ref = now or _utc_now()
        return max(0, int((ref - self.timestamp_received).total_seconds() * 1000))

    def mid(self) -> Optional[float]:
        if self.bid is None or self.ask is None:
            return None
        if self.bid <= 0 or self.ask <= 0:
            return None
        return (self.bid + self.ask) / 2.0

    def spread_bps(self) -> Optional[float]:
        mid = self.mid()
        if not mid:
            return None
        return ((self.ask or 0.0) - (self.bid or 0.0)) / mid * 10_000.0

    def to_payload(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "last": self.last,
            "bid": self.bid,
            "ask": self.ask,
            "timestamp": self.timestamp_exchange.isoformat() if self.timestamp_exchange else None,
            "source": self.source,
            "channel": self.channel,
            "received_at": self.timestamp_received.isoformat(),
        }
        if self.mark is not None:
            payload["mark"] = self.mark
        if self.volume is not None:
            payload["volume"] = self.volume
        if self.raw_symbol:
            payload["raw_symbol"] = self.raw_symbol
        return payload

    def to_status(self, *, max_age_sec: float, now: Optional[datetime] = None) -> Dict[str, Any]:
        age_ms = self.age_ms(now=now)
        stale = age_ms > max(0.0, float(max_age_sec)) * 1000.0
        quality = "stale" if stale else "healthy"
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "channel": self.channel,
            "last": self.last,
            "bid": self.bid,
            "ask": self.ask,
            "mark": self.mark,
            "volume": self.volume,
            "source": self.source,
            "age_ms": age_ms,
            "is_stale": stale,
            "quality": quality,
            "spread_bps": self.spread_bps(),
            "timestamp_exchange": self.timestamp_exchange.isoformat() if self.timestamp_exchange else None,
            "timestamp_received": self.timestamp_received.isoformat(),
            "raw_symbol": self.raw_symbol,
        }


class MarketDataHub:
    """Latest-tick cache with freshness and fallback counters."""

    def __init__(
        self,
        *,
        symbol_max_age_sec: float = 10.0,
        exchange_max_age_sec: float = 15.0,
        max_price_diff_bps: float = 20.0,
        shadow_compare_max_age_sec: Optional[float] = None,
        shadow_compare_enabled: bool = False,
    ) -> None:
        self.symbol_max_age_sec = max(0.5, float(symbol_max_age_sec))
        self.exchange_max_age_sec = max(0.5, float(exchange_max_age_sec))
        self.max_price_diff_bps = max(0.0, float(max_price_diff_bps))
        default_compare_age = max(self.symbol_max_age_sec, 60.0)
        self.shadow_compare_max_age_sec = max(
            0.5,
            float(shadow_compare_max_age_sec if shadow_compare_max_age_sec is not None else default_compare_age),
        )
        self.shadow_compare_enabled = bool(shadow_compare_enabled)
        self._ticks: Dict[tuple[str, str, str], MarketTick] = {}
        self._source_ticks: Dict[tuple[str, str, str, str], MarketTick] = {}
        self._last_exchange_received: Dict[str, datetime] = {}
        self._last_exchange_received_by_source: Dict[tuple[str, str], datetime] = {}
        self._ws_tick_count = 0
        self._rest_fallback_count = 0
        self._rest_snapshot_count = 0
        self._invalid_payload_count = 0
        self._timestamp_regression_count = 0
        self._fallback_reasons: Dict[str, int] = {}
        self._last_error: Optional[str] = None
        self._shadow_compare_count = 0
        self._shadow_compare_violation_count = 0
        self._shadow_missing_ws_count = 0
        self._shadow_compare_stale_skip_count = 0
        self._shadow_last_compare: Optional[Dict[str, Any]] = None
        self._shadow_max_abs_diff_bps = 0.0

    def clear(self) -> None:
        self._ticks.clear()
        self._source_ticks.clear()
        self._last_exchange_received.clear()
        self._last_exchange_received_by_source.clear()
        self._ws_tick_count = 0
        self._rest_fallback_count = 0
        self._rest_snapshot_count = 0
        self._invalid_payload_count = 0
        self._timestamp_regression_count = 0
        self._fallback_reasons.clear()
        self._last_error = None
        self._shadow_compare_count = 0
        self._shadow_compare_violation_count = 0
        self._shadow_missing_ws_count = 0
        self._shadow_compare_stale_skip_count = 0
        self._shadow_last_compare = None
        self._shadow_max_abs_diff_bps = 0.0

    def upsert_ws_tick(self, exchange: Any, symbol: Any, payload: Dict[str, Any], *, channel: str = "ticker") -> Optional[MarketTick]:
        return self.upsert_tick(exchange, symbol, payload, source="ws", channel=channel)

    def upsert_rest_tick(
        self,
        exchange: Any,
        symbol: Any,
        payload: Dict[str, Any],
        *,
        source: MarketTickSource = "rest_fallback",
        reason: str = "ws_unhealthy",
        channel: str = "ticker",
    ) -> Optional[MarketTick]:
        tick = self.upsert_tick(exchange, symbol, payload, source=source, channel=channel)
        if tick is not None:
            if source == "rest_fallback":
                self._rest_fallback_count += 1
                key = str(reason or "unknown")
                self._fallback_reasons[key] = self._fallback_reasons.get(key, 0) + 1
            elif source == "rest_snapshot":
                self._rest_snapshot_count += 1
        return tick

    def upsert_tick(
        self,
        exchange: Any,
        symbol: Any,
        payload: Dict[str, Any],
        *,
        source: MarketTickSource,
        channel: str = "ticker",
    ) -> Optional[MarketTick]:
        name = normalize_exchange_name(exchange)
        norm_symbol = normalize_market_symbol(symbol)
        if not name or not norm_symbol or not isinstance(payload, dict):
            self._invalid_payload_count += 1
            return None

        last = _coerce_float(payload.get("last") if payload.get("last") is not None else payload.get("close"))
        bid = _coerce_float(payload.get("bid"))
        ask = _coerce_float(payload.get("ask"))
        mark = _coerce_float(payload.get("mark") if payload.get("mark") is not None else payload.get("mark_price"))
        volume = _coerce_float(
            payload.get("volume")
            if payload.get("volume") is not None
            else payload.get("volume_24h")
        )
        if last is not None and last <= 0:
            self._invalid_payload_count += 1
            return None
        if bid is not None and bid < 0:
            self._invalid_payload_count += 1
            return None
        if ask is not None and ask < 0:
            self._invalid_payload_count += 1
            return None
        if bid is not None and ask is not None and bid > ask:
            self._invalid_payload_count += 1
            return None
        if last is None and mark is None and bid is None and ask is None:
            self._invalid_payload_count += 1
            return None

        timestamp_exchange = _coerce_datetime(
            payload.get("timestamp_exchange")
            if payload.get("timestamp_exchange") is not None
            else payload.get("timestamp")
        )
        now = _utc_now()
        key = (name, norm_symbol, str(channel or "ticker"))
        source_key = (source, name, norm_symbol, str(channel or "ticker"))
        previous = self._source_ticks.get(source_key)
        if (
            previous is not None
            and previous.timestamp_exchange is not None
            and timestamp_exchange is not None
            and timestamp_exchange < previous.timestamp_exchange
        ):
            self._timestamp_regression_count += 1
            self._last_error = f"timestamp_regression:{name}:{norm_symbol}"
            return previous

        sequence = None
        raw_sequence = payload.get("sequence") if payload.get("sequence") is not None else payload.get("last_update_id")
        if raw_sequence is not None:
            try:
                sequence = int(raw_sequence)
            except (TypeError, ValueError):
                sequence = None

        tick = MarketTick(
            exchange=name,
            symbol=norm_symbol,
            channel=str(channel or "ticker"),
            last=last,
            bid=bid,
            ask=ask,
            mark=mark,
            volume=volume,
            timestamp_exchange=timestamp_exchange,
            timestamp_received=now,
            source=source,
            sequence=sequence,
            raw_symbol=str(payload.get("raw_symbol") or symbol or ""),
        )
        self._ticks[key] = tick
        self._source_ticks[source_key] = tick
        self._last_exchange_received[name] = now
        self._last_exchange_received_by_source[(source, name)] = now
        if source == "ws":
            self._ws_tick_count += 1
        self._record_shadow_compare(tick)
        return tick

    def _latest_rest_tick(self, exchange: str, symbol: str, channel: str) -> Optional[MarketTick]:
        for source in ("rest_snapshot", "rest_fallback"):
            tick = self._source_ticks.get((source, exchange, symbol, channel))
            if tick is not None:
                return tick
        return None

    def _record_shadow_compare(self, tick: MarketTick) -> None:
        if not self.shadow_compare_enabled:
            return
        if tick.channel != "ticker":
            return
        # REST snapshots are the slower side of the shadow pair, so use them as
        # the compare trigger. Comparing every WS push against a deliberately
        # sparse REST baseline only measures baseline age, not price drift.
        if tick.source == "ws":
            return
        if tick.source in {"rest_snapshot", "rest_fallback"}:
            rest_tick = tick
            ws_tick = self._source_ticks.get(("ws", tick.exchange, tick.symbol, tick.channel))
            if ws_tick is None:
                self._shadow_missing_ws_count += 1
                return
        else:
            return

        now = _utc_now()
        ws_age_ms = ws_tick.age_ms(now=now)
        rest_age_ms = rest_tick.age_ms(now=now)
        compare_max_age_ms = int(max(0.5, float(self.shadow_compare_max_age_sec)) * 1000.0)
        if ws_age_ms > compare_max_age_ms or rest_age_ms > compare_max_age_ms:
            self._shadow_compare_stale_skip_count += 1
            self._shadow_last_compare = {
                "exchange": tick.exchange,
                "symbol": tick.symbol,
                "accepted": None,
                "skipped": True,
                "skip_reason": "stale_compare_input",
                "max_compare_age_ms": compare_max_age_ms,
                "ws_age_ms": ws_age_ms,
                "rest_age_ms": rest_age_ms,
                "ws_source": ws_tick.source,
                "rest_source": rest_tick.source,
                "trigger_source": tick.source,
                "compared_at": now.isoformat(),
            }
            return

        if ws_tick.last is None or rest_tick.last is None or rest_tick.last <= 0:
            return
        diff_bps = ((ws_tick.last - rest_tick.last) / rest_tick.last) * 10_000.0
        abs_diff_bps = abs(diff_bps)
        self._shadow_compare_count += 1
        self._shadow_max_abs_diff_bps = max(self._shadow_max_abs_diff_bps, abs_diff_bps)
        accepted = abs_diff_bps <= self.max_price_diff_bps
        if not accepted:
            self._shadow_compare_violation_count += 1
        exchange_lag_ms = None
        if ws_tick.timestamp_exchange is not None and rest_tick.timestamp_exchange is not None:
            exchange_lag_ms = int(
                (ws_tick.timestamp_exchange - rest_tick.timestamp_exchange).total_seconds() * 1000
            )
        self._shadow_last_compare = {
            "exchange": tick.exchange,
            "symbol": tick.symbol,
            "ws_last": ws_tick.last,
            "rest_last": rest_tick.last,
            "diff_bps": diff_bps,
            "abs_diff_bps": abs_diff_bps,
            "accepted": accepted,
            "max_price_diff_bps": self.max_price_diff_bps,
            "ws_age_ms": ws_age_ms,
            "rest_age_ms": rest_age_ms,
            "received_lag_ms": ws_age_ms - rest_age_ms,
            "exchange_lag_ms": exchange_lag_ms,
            "ws_source": ws_tick.source,
            "rest_source": rest_tick.source,
            "trigger_source": tick.source,
            "compared_at": now.isoformat(),
        }

    def get_tick(
        self,
        exchange: Any,
        symbol: Any,
        *,
        channel: str = "ticker",
        max_age_sec: Optional[float] = None,
        source: Optional[MarketTickSource] = None,
    ) -> Optional[Dict[str, Any]]:
        name = normalize_exchange_name(exchange)
        norm_symbol = normalize_market_symbol(symbol)
        channel_key = str(channel or "ticker")
        tick = (
            self._source_ticks.get((source, name, norm_symbol, channel_key))
            if source
            else self._ticks.get((name, norm_symbol, channel_key))
        )
        if tick is None:
            return None
        horizon = self.symbol_max_age_sec if max_age_sec is None else float(max_age_sec)
        status = tick.to_status(max_age_sec=horizon)
        return {
            "tick": tick.to_payload(),
            "meta": {
                "age_ms": status["age_ms"],
                "is_stale": status["is_stale"],
                "quality": status["quality"],
                "source": tick.source,
                "exchange": tick.exchange,
                "symbol": tick.symbol,
                "fallback_required": bool(status["is_stale"]),
            },
        }

    def has_fresh_tick(self, *, max_age_sec: Optional[float] = None, source: Optional[MarketTickSource] = None) -> bool:
        horizon = self.symbol_max_age_sec if max_age_sec is None else float(max_age_sec)
        now = _utc_now()
        ticks = (
            [tick for (src, _exchange, _symbol, _channel), tick in self._source_ticks.items() if src == source]
            if source
            else list(self._ticks.values())
        )
        return any(not tick.to_status(max_age_sec=horizon, now=now)["is_stale"] for tick in ticks)

    def healthy_exchanges(self, *, max_age_sec: Optional[float] = None, source: Optional[MarketTickSource] = None) -> list[str]:
        horizon = self.exchange_max_age_sec if max_age_sec is None else float(max_age_sec)
        now = _utc_now()
        if source:
            return sorted(
                exchange
                for (src, exchange), ts in self._last_exchange_received_by_source.items()
                if src == source and (now - ts).total_seconds() <= horizon
            )
        return sorted(
            exchange
            for exchange, ts in self._last_exchange_received.items()
            if (now - ts).total_seconds() <= horizon
        )

    def snapshot(self, *, include_symbols: bool = False) -> Dict[str, Any]:
        now = _utc_now()
        statuses = [
            tick.to_status(max_age_sec=self.symbol_max_age_sec, now=now)
            for tick in self._ticks.values()
        ]
        ages = [int(row["age_ms"]) for row in statuses]
        stale = [row for row in statuses if row["is_stale"]]
        ws_statuses = [row for row in statuses if row["source"] == "ws"]
        ws_stale = [row for row in ws_statuses if row["is_stale"]]
        healthy_exchanges = self.healthy_exchanges(max_age_sec=self.exchange_max_age_sec)
        ws_healthy_exchanges = self.healthy_exchanges(max_age_sec=self.exchange_max_age_sec, source="ws")
        latest_age = min(ages) if ages else None
        oldest_age = max(ages) if ages else None
        out: Dict[str, Any] = {
            "hub_healthy": bool(healthy_exchanges and statuses and len(stale) < len(statuses)),
            "ws_hub_healthy": self.has_fresh_tick(max_age_sec=self.symbol_max_age_sec, source="ws"),
            "healthy_exchanges": healthy_exchanges,
            "ws_healthy_exchanges": ws_healthy_exchanges,
            "symbol_count": len(statuses),
            "ws_symbol_count": sum(1 for key in self._source_ticks if key[0] == "ws"),
            "last_tick_age_ms": latest_age,
            "oldest_tick_age_ms": oldest_age,
            "stale_symbol_count": len(stale),
            "ws_stale_symbol_count": len(ws_stale),
            "ws_tick_count": self._ws_tick_count,
            "rest_fallback_count": self._rest_fallback_count,
            "rest_snapshot_count": self._rest_snapshot_count,
            "invalid_payload_count": self._invalid_payload_count,
            "timestamp_regression_count": self._timestamp_regression_count,
            "fallback_reasons": dict(sorted(self._fallback_reasons.items())),
            "shadow_compare_count": self._shadow_compare_count,
            "shadow_compare_violation_count": self._shadow_compare_violation_count,
            "shadow_missing_ws_count": self._shadow_missing_ws_count,
            "shadow_compare_stale_skip_count": self._shadow_compare_stale_skip_count,
            "shadow_max_abs_diff_bps": self._shadow_max_abs_diff_bps,
            "shadow_last_compare": dict(self._shadow_last_compare) if self._shadow_last_compare else None,
            "last_error": self._last_error,
        }
        if include_symbols:
            per_exchange: Dict[str, Dict[str, Any]] = {}
            for row in statuses:
                exchange = str(row["exchange"])
                symbol = str(row["symbol"])
                per_exchange.setdefault(exchange, {})[symbol] = row
            out["symbols"] = per_exchange
        return out


market_data_hub = MarketDataHub()
