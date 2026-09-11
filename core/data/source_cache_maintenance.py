"""Low-frequency refresh for source caches consumed by research and agents.

CoinGlass calls use the existing shared budget and capability checks. No health
endpoint performs network I/O or silently turns on an unconfigured provider.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from core.backtest.funding_provider import FundingProviderConfig, FundingRateProvider
from core.data.coinglass_client import coinglass_enabled, load_dataset_rows_for_symbol
from core.data.coinglass_feature_builder import update_coinglass_cache
from core.data.options_collector import options_collector


def _dataset_due(dataset: str, symbol: str, max_age_sec: int) -> bool:
    frame = load_dataset_rows_for_symbol(dataset, symbol)
    if frame.empty or "source_ts" not in frame:
        return True
    timestamps = pd.to_datetime(frame["source_ts"], errors="coerce", utc=True).dropna()
    if timestamps.empty:
        return True
    age = (pd.Timestamp.now(tz="UTC") - timestamps.max()).total_seconds()
    return age < 0 or age > max_age_sec


def _sync_funding_cache(symbol: str) -> dict[str, Any]:
    provider = FundingRateProvider(FundingProviderConfig(exchange="binance", source="coinglass"))
    provider.load_local_cache(symbol, exchange="binance")
    series = provider.load_coinglass_cache(symbol, save=True)
    return {"rows": len(series), "latest": series.index.max().isoformat() if not series.empty else None}


async def refresh_source_caches() -> dict[str, Any]:
    result: dict[str, Any] = {"checked_at": datetime.now(timezone.utc).isoformat(), "options": {}, "funding": {}, "errors": []}
    for symbol in ("BTC", "ETH"):
        try:
            snapshot = await options_collector.fetch_snapshot(symbol)
            result["options"][symbol] = snapshot.to_dict() if snapshot else {"available": False}
            if snapshot is None or not options_collector._snapshot_is_fresh(snapshot):
                result["errors"].append({"source": f"deribit.{symbol}", "error": "fresh snapshot unavailable"})
        except Exception as exc:
            result["errors"].append({"source": f"deribit.{symbol}", "error": str(exc)})

    if coinglass_enabled():
        # One representative feed per family. Expensive optional datasets are
        # kept out of the frequent core sweep, but must not remain unwarmed.
        datasets = ("funding_rate_history", "options_info", "spot_coin_netflow", "exchange_balance_list")
        due = [name for name in datasets if await asyncio.to_thread(_dataset_due, name, "BTC", 3600)]
        if due:
            try:
                result["coinglass"] = await update_coinglass_cache(symbols=["BTC"], datasets=due, manual=False, max_symbols_per_run=1)
                result["errors"].extend(result["coinglass"].get("errors") or [])
            except Exception as exc:
                result["errors"].append({"source": "coinglass", "error": str(exc)})
        for symbol in ("BTC/USDT", "ETH/USDT"):
            try:
                result["funding"][symbol] = await asyncio.to_thread(_sync_funding_cache, symbol)
            except Exception as exc:
                result["errors"].append({"source": f"funding.{symbol}", "error": str(exc)})
    return result
