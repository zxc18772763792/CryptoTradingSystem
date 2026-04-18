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


COINGLASS_DATASET_MANIFESTS: Dict[str, CoinglassDatasetManifest] = {
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
                required_params=("symbol",),
            ),
            CoinglassRouteSpec(
                api_version="v3",
                path="/v3/api/futures/takerBuySellVolume/exchange-list",
                required_params=("symbol",),
                fallback=True,
            ),
        ),
    ),
    "liquidation_history": CoinglassDatasetManifest(
        dataset="liquidation_history",
        label="Liquidation History",
        market_type="futures",
        storage_group="futures",
        ttl_sec=300,
        freshness_sec=900,
        include_in_ai=True,
        include_in_radar=True,
        routes=(
            CoinglassRouteSpec(
                api_version="v4",
                path="/v4/api/futures/liquidation/history",
                required_params=("symbol", "interval"),
                default_params={"interval": "h4"},
            ),
            CoinglassRouteSpec(
                api_version="v3",
                path="/v3/api/futures/liquidation/history",
                required_params=("symbol", "interval"),
                default_params={"interval": "h4"},
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
        freshness_sec=900,
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
        freshness_sec=1800,
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

COINGLASS_DEFAULT_DATASETS: tuple[str, ...] = tuple(COINGLASS_DATASET_MANIFESTS.keys())


def get_coinglass_manifest(dataset: str) -> Optional[CoinglassDatasetManifest]:
    return COINGLASS_DATASET_MANIFESTS.get(str(dataset or "").strip())
