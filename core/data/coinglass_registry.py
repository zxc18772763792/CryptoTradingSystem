from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Dict, Mapping, Optional


@dataclass(frozen=True)
class CoinglassRouteSpec:
    api_version: str
    path: str
    required_params: tuple[str, ...] = ()
    default_params: Mapping[str, Any] = field(default_factory=dict)
    fallback: bool = False


@dataclass(frozen=True)
class CoinglassDatasetManifest:
    dataset: str
    label: str
    market_type: str
    storage_group: str
    ttl_sec: int
    freshness_sec: int
    include_in_ai: bool = True
    include_in_radar: bool = True
    include_in_strategies: bool = False
    routes: tuple[CoinglassRouteSpec, ...] = ()


_COINGLASS_EXCHANGE_ALIASES: Dict[str, str] = {
    "BINANCE": "Binance",
    "OKX": "OKX",
    "BYBIT": "Bybit",
    "BITGET": "Bitget",
    "DYDX": "dYdX",
    "HUOBI": "HTX",
    "HTX": "HTX",
    "GATE.IO": "Gate",
    "GATE": "Gate",
    "COINEX": "CoinEx",
    "BINGX": "BingX",
    "MEXC": "MEXC",
    "WHITEBIT": "WhiteBIT",
    "KUCOIN": "KuCoin",
    "LBANK": "LBank",
}

_COINGLASS_PAIR_TEMPLATES: Dict[str, str] = {
    "BINANCE": "{base}USDT",
    "BYBIT": "{base}USDT",
    "BITGET": "{base}USDT",
    "BINGX": "{base}USDT",
    "MEXC": "{base}USDT",
    "GATE": "{base}USDT",
    "COINEX": "{base}USDT",
    "HTX": "{base}USDT",
    "KUCOIN": "{base}USDT",
    "WHITEBIT": "{base}USDT",
    "OKX": "{base}-USDT-SWAP",
    "DYDX": "{base}-USD",
}

_COINGLASS_INTERVAL_ALIASES: Dict[str, str] = {
    "1m": "1m",
    "m1": "1m",
    "5m": "5m",
    "m5": "5m",
    "15m": "15m",
    "m15": "15m",
    "30m": "30m",
    "m30": "30m",
    "1h": "h1",
    "h1": "h1",
    "4h": "h4",
    "h4": "h4",
    "12h": "h12",
    "h12": "h12",
    "24h": "h24",
    "h24": "h24",
    "1d": "h24",
    "d1": "h24",
}

_COINGLASS_RANGE_BY_INTERVAL: Dict[str, str] = {
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "h1": "1h",
    "h4": "4h",
    "h12": "12h",
    "h24": "24h",
}


def normalize_coinglass_symbol(symbol: Any) -> str:
    text = str(symbol or "").strip().upper()
    if not text:
        return ""
    if "/" in text:
        text = text.split("/", 1)[0]
    for suffix in (
        "-USDT-SWAP",
        "-USD-SWAP",
        "_USDT",
        "_USD",
        "USDT",
        "USD",
        "PERP",
    ):
        if text.endswith(suffix):
            text = text[: -len(suffix)]
            break
    text = re.sub(r"[^A-Z0-9]", "", text)
    return text


def normalize_coinglass_exchange(exchange: Any, *, default: str = "Binance") -> str:
    text = str(exchange or "").strip()
    if not text:
        return default
    return _COINGLASS_EXCHANGE_ALIASES.get(text.upper(), text)


def normalize_coinglass_interval(interval: Any, *, default: str = "h4") -> str:
    text = str(interval or "").strip().lower()
    if not text:
        return default
    return _COINGLASS_INTERVAL_ALIASES.get(text, text)


def coinglass_range_for_interval(interval: Any, *, default: str = "4h") -> str:
    normalized = normalize_coinglass_interval(interval, default="h4")
    return _COINGLASS_RANGE_BY_INTERVAL.get(normalized, default)


def coinglass_pair_symbol(symbol: Any, exchange: Any) -> str:
    base_symbol = normalize_coinglass_symbol(symbol)
    if not base_symbol:
        return ""
    normalized_exchange = normalize_coinglass_exchange(exchange)
    template = _COINGLASS_PAIR_TEMPLATES.get(normalized_exchange.upper())
    if not template:
        return base_symbol
    return template.format(base=base_symbol)


def coinglass_symbol_matches(expected_symbol: Any, actual_symbol: Any) -> bool:
    expected = normalize_coinglass_symbol(expected_symbol)
    actual = normalize_coinglass_symbol(actual_symbol)
    return bool(expected and actual and expected == actual)


COINGLASS_DATASET_MANIFESTS: Dict[str, CoinglassDatasetManifest] = {
    "price_history": CoinglassDatasetManifest(
        dataset="price_history",
        label="Price History (OHLC)",
        market_type="futures",
        storage_group="futures",
        ttl_sec=600,
        freshness_sec=1800,
        include_in_ai=False,
        include_in_radar=False,
        include_in_strategies=False,
        routes=(
            CoinglassRouteSpec(
                api_version="v4",
                path="/v4/api/futures/price/history",
                required_params=("exchange", "symbol", "interval"),
                default_params={"exchange": "Binance", "interval": "1h"},
            ),
        ),
    ),
    "open_interest_exchange_list": CoinglassDatasetManifest(
        dataset="open_interest_exchange_list",
        label="Open Interest Exchange List",
        market_type="futures",
        storage_group="futures",
        ttl_sec=300,
        freshness_sec=900,
        include_in_ai=True,
        include_in_radar=True,
        routes=(
            CoinglassRouteSpec(
                api_version="v4",
                path="/v4/api/futures/open-interest/exchange-list",
                required_params=("symbol",),
            ),
            CoinglassRouteSpec(
                api_version="v3",
                path="/v3/api/futures/openInterest/exchange-list",
                required_params=("symbol",),
                fallback=True,
            ),
        ),
    ),
    "open_interest_history": CoinglassDatasetManifest(
        dataset="open_interest_history",
        label="Open Interest History",
        market_type="futures",
        storage_group="futures",
        ttl_sec=600,
        freshness_sec=1800,
        include_in_ai=True,
        include_in_radar=True,
        routes=(
            CoinglassRouteSpec(
                api_version="v4",
                path="/v4/api/futures/open-interest/history",
                required_params=("symbol", "exchange", "interval"),
                default_params={"exchange": "Binance", "interval": "h1", "unit": "usd"},
            ),
        ),
    ),
    "funding_rate_exchange_list": CoinglassDatasetManifest(
        dataset="funding_rate_exchange_list",
        label="Funding Rate Exchange List",
        market_type="futures",
        storage_group="futures",
        ttl_sec=300,
        freshness_sec=900,
        include_in_ai=True,
        include_in_radar=True,
        routes=(
            CoinglassRouteSpec(
                api_version="v4",
                path="/v4/api/futures/funding-rate/exchange-list",
                required_params=("symbol",),
            ),
            CoinglassRouteSpec(
                api_version="v3",
                path="/v3/api/futures/fundingRate/exchange-list",
                required_params=("symbol",),
                fallback=True,
            ),
        ),
    ),
    "funding_rate_history": CoinglassDatasetManifest(
        dataset="funding_rate_history",
        label="Funding Rate History",
        market_type="futures",
        storage_group="futures",
        ttl_sec=600,
        freshness_sec=1800,
        include_in_ai=True,
        include_in_radar=True,
        routes=(
            CoinglassRouteSpec(
                api_version="v4",
                path="/v4/api/futures/funding-rate/history",
                required_params=("symbol", "exchange", "interval"),
                default_params={"exchange": "Binance", "interval": "h1"},
            ),
        ),
    ),
    "taker_buy_sell_volume_exchange_list": CoinglassDatasetManifest(
        dataset="taker_buy_sell_volume_exchange_list",
        label="Taker Buy Sell Volume Exchange List",
        market_type="futures",
        storage_group="futures",
        ttl_sec=300,
        freshness_sec=900,
        include_in_ai=True,
        include_in_radar=True,
        routes=(
            CoinglassRouteSpec(
                api_version="v4",
                path="/v4/api/futures/taker-buy-sell-volume/exchange-list",
                required_params=("symbol", "range"),
                default_params={"range": "4h"},
            ),
            CoinglassRouteSpec(
                api_version="v3",
                path="/v3/api/futures/takerBuySellVolume/exchange-list",
                required_params=("symbol", "range"),
                default_params={"range": "4h"},
                fallback=True,
            ),
        ),
    ),
    "taker_buy_sell_volume_history": CoinglassDatasetManifest(
        dataset="taker_buy_sell_volume_history",
        label="Taker Buy Sell Volume History",
        market_type="futures",
        storage_group="futures",
        ttl_sec=600,
        freshness_sec=1800,
        include_in_ai=True,
        include_in_radar=True,
        routes=(
            CoinglassRouteSpec(
                api_version="v4",
                path="/v4/api/futures/taker-buy-sell-volume/history",
                required_params=("symbol", "exchange", "interval"),
                default_params={"exchange": "Binance", "interval": "h1"},
            ),
        ),
    ),
    "liquidation_history": CoinglassDatasetManifest(
        dataset="liquidation_history",
        label="Liquidation History",
        market_type="futures",
        storage_group="futures",
        ttl_sec=300,
        freshness_sec=1800,
        include_in_ai=True,
        include_in_radar=True,
        routes=(
            CoinglassRouteSpec(
                api_version="v4",
                path="/v4/api/futures/liquidation/history",
                required_params=("symbol", "exchange", "interval"),
                default_params={"exchange": "Binance", "interval": "h1"},
            ),
            CoinglassRouteSpec(
                api_version="v3",
                path="/v3/api/futures/liquidation/history",
                required_params=("symbol", "exchange", "interval"),
                default_params={"exchange": "Binance", "interval": "h1"},
                fallback=True,
            ),
        ),
    ),
    "global_long_short_account_ratio_history": CoinglassDatasetManifest(
        dataset="global_long_short_account_ratio_history",
        label="Global Long Short Account Ratio History",
        market_type="futures",
        storage_group="futures",
        ttl_sec=300,
        freshness_sec=1800,
        include_in_ai=True,
        include_in_radar=True,
        routes=(
            CoinglassRouteSpec(
                api_version="v4",
                path="/v4/api/futures/global-long-short-account-ratio/history",
                required_params=("symbol", "exchange", "interval"),
                default_params={"exchange": "Binance", "interval": "h4"},
            ),
        ),
    ),
    "funding_arbitrage": CoinglassDatasetManifest(
        dataset="funding_arbitrage",
        label="Funding Arbitrage",
        market_type="futures",
        storage_group="futures",
        ttl_sec=900,
        freshness_sec=3600,
        include_in_ai=True,
        include_in_radar=False,
        routes=(
            CoinglassRouteSpec(
                api_version="v4",
                path="/v4/api/futures/funding-rate/arbitrage",
            ),
        ),
    ),
}

COINGLASS_DEFAULT_DATASETS: tuple[str, ...] = tuple(
    dataset
    for dataset in COINGLASS_DATASET_MANIFESTS.keys()
    if dataset != "price_history"
)


def get_coinglass_manifest(dataset: str) -> Optional[CoinglassDatasetManifest]:
    return COINGLASS_DATASET_MANIFESTS.get(str(dataset or "").strip())
