"""Funding rate data models and symbol normalization helpers."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Optional


class FundingRateSource(str, Enum):
    """Supported funding-rate sources."""

    BINANCE = "binance"
    BYBIT = "bybit"
    OKX = "okx"
    GATE = "gate"


@dataclass
class FundingRate:
    """Single funding-rate snapshot."""

    exchange: str
    symbol: str
    funding_rate: float
    funding_time: datetime
    timestamp: datetime = field(default_factory=datetime.now)
    mark_price: Optional[float] = None
    index_price: Optional[float] = None
    estimated_rate: Optional[float] = None

    def __post_init__(self) -> None:
        self.exchange = str(self.exchange or "").lower()

    @property
    def funding_rate_pct(self) -> float:
        return self.funding_rate * 100

    @property
    def annualized_rate(self) -> float:
        # Perpetuals typically settle every 8 hours => 3 times/day.
        return self.funding_rate * 365 * 3 * 100

    @property
    def is_extreme_positive(self) -> bool:
        return self.funding_rate > 0.0001

    @property
    def is_extreme_negative(self) -> bool:
        return self.funding_rate < -0.0001

    @property
    def sentiment(self) -> str:
        if self.funding_rate > 0.0005:
            return "long_heavy"
        if self.funding_rate < -0.0005:
            return "short_heavy"
        return "neutral"

    def to_dict(self) -> dict:
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "funding_rate": self.funding_rate,
            "funding_rate_pct": self.funding_rate_pct,
            "funding_time": self.funding_time.isoformat(),
            "timestamp": self.timestamp.isoformat(),
            "mark_price": self.mark_price,
            "index_price": self.index_price,
            "estimated_rate": self.estimated_rate,
            "annualized_rate": self.annualized_rate,
            "sentiment": self.sentiment,
        }


@dataclass
class FundingRateHistory:
    """Funding-rate history helpers."""

    symbol: str
    exchange: str
    rates: List[FundingRate]

    def get_latest(self) -> Optional[FundingRate]:
        return self.rates[0] if self.rates else None

    def get_mean(self, periods: int = 30) -> float:
        if not self.rates:
            return 0.0
        recent = self.rates[:periods]
        return sum(r.funding_rate for r in recent) / len(recent)

    def get_std(self, periods: int = 30) -> float:
        if len(self.rates) < 2:
            return 0.0
        import statistics

        recent = self.rates[:periods]
        values = [r.funding_rate for r in recent]
        return statistics.stdev(values)

    def get_zscore(self, current_rate: float, periods: int = 30) -> float:
        mean = self.get_mean(periods)
        std = self.get_std(periods)
        if std == 0:
            return 0.0
        return (current_rate - mean) / std


SYMBOL_FORMAT_MAP = {
    "binance": {"format": "{base}{quote}", "example": "BTCUSDT"},
    "bybit": {"format": "{base}{quote}", "example": "BTCUSDT"},
    "okx": {"format": "{base}-{quote}-SWAP", "example": "BTC-USDT-SWAP"},
    "gate": {"format": "{base}_{quote}", "example": "BTC_USDT"},
}


def normalize_symbol(symbol: str, target_exchange: str) -> str:
    """Normalize symbol to exchange-specific format."""

    raw = str(symbol or "").strip().upper()
    if not raw:
        return raw

    # CCXT perpetual format like BTC/USDT:USDT -> BTC/USDT
    if ":" in raw:
        raw = raw.split(":", 1)[0]

    compact = raw.replace("-SWAP", "")
    base = ""
    quote = ""

    if "/" in compact:
        parts = [p for p in compact.split("/") if p]
        if len(parts) >= 2:
            base, quote = parts[0], parts[1]
    elif "_" in compact:
        parts = [p for p in compact.split("_") if p]
        if len(parts) >= 2:
            base, quote = parts[0], parts[1]
    elif "-" in compact:
        parts = [p for p in compact.split("-") if p]
        if len(parts) >= 2:
            base, quote = parts[0], parts[1]

    if not base or not quote:
        for q in ["USDT", "USDC", "FDUSD", "BUSD", "USD", "BTC", "ETH"]:
            if compact.endswith(q) and len(compact) > len(q):
                base = compact[: -len(q)]
                quote = q
                break
        else:
            base = compact[:-4]
            quote = compact[-4:]

    target_exchange = str(target_exchange or "").strip().lower()
    if target_exchange == "okx":
        return f"{base}-{quote}-SWAP"
    if target_exchange == "gate":
        return f"{base}_{quote}"
    return f"{base}{quote}"
