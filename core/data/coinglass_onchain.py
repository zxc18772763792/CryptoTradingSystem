from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional

import aiohttp
import pandas as pd

from core.data.coinglass_client import CoinglassClient, coinglass_enabled
from core.data.coinglass_registry import normalize_coinglass_symbol

_BINANCE_PUBLIC_BASE = "https://api.binance.com"
_DEFAULT_EXCHANGE_LIST = "Binance,OKX,Bybit,Bitget,Gate"


def _to_float(value: Any, default: Optional[float] = 0.0) -> Optional[float]:
    if value is None or value == "":
        return default
    try:
        parsed = float(value)
    except Exception:
        return default
    if pd.isna(parsed):
        return default
    return float(parsed)


def _row_value(row: Mapping[str, Any], *candidates: str) -> Any:
    normalized = {
        "".join(ch for ch in str(key or "").lower() if ch.isalnum()): value
        for key, value in dict(row or {}).items()
    }
    for candidate in candidates:
        key = "".join(ch for ch in str(candidate or "").lower() if ch.isalnum())
        if key in normalized:
            return normalized[key]
    return None


def _coalesce_float(row: Mapping[str, Any], *candidates: str) -> Optional[float]:
    for candidate in candidates:
        value = _to_float(_row_value(row, candidate), None)
        if value is not None:
            return value
    return None


def _timestamp_to_iso(value: Any) -> Optional[str]:
    if value is None or value == "":
        return None
    try:
        if isinstance(value, (int, float)):
            numeric = float(value)
            if numeric > 1_000_000_000_000:
                numeric /= 1000.0
            return datetime.fromtimestamp(numeric, tz=timezone.utc).isoformat()
        ts = pd.Timestamp(value)
        if pd.isna(ts):
            return None
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        return ts.tz_convert("UTC").isoformat()
    except Exception:
        text = str(value or "").strip()
        return text or None


def _payload_rows(response: Mapping[str, Any]) -> List[Dict[str, Any]]:
    payload = dict((response or {}).get("payload") or {})
    rows = payload.get("data")
    if isinstance(rows, list):
        return [dict(item or {}) for item in rows if isinstance(item, Mapping)]
    if isinstance(rows, Mapping):
        nested = rows.get("data") or rows.get("list") or rows.get("rows")
        if isinstance(nested, list):
            return [dict(item or {}) for item in nested if isinstance(item, Mapping)]
        return [dict(rows)]
    return []


def _symbol_base(symbol: Any) -> str:
    return normalize_coinglass_symbol(symbol) or "BTC"


async def _fetch_btc_price(timeout_sec: float = 6.5) -> float:
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=timeout_sec), trust_env=True
        ) as session:
            async with session.get(
                f"{_BINANCE_PUBLIC_BASE}/api/v3/ticker/price",
                params={"symbol": "BTCUSDT"},
            ) as response:
                payload = await response.json(content_type=None)
                return float(payload.get("price") or 0.0)
    except Exception:
        return 0.0


def exchange_flow_direction(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return "other"
    if any(token in text for token in ("deposit", "inflow", "to_exchange")):
        return "inflow"
    if any(token in text for token in ("withdraw", "outflow", "from_exchange")):
        return "outflow"
    if text in {"in", "into", "inbound"}:
        return "inflow"
    if text in {"out", "outbound"}:
        return "outflow"
    return "other"


def normalize_whale_transfer(row: Mapping[str, Any], *, btc_price: float) -> Dict[str, Any]:
    amount_usd = _coalesce_float(row, "amount_usd", "amountUsd", "usd", "value") or 0.0
    asset_qty = _coalesce_float(row, "asset_quantity", "assetQuantity", "quantity", "amount") or 0.0
    btc_equiv = amount_usd / btc_price if btc_price > 0 and amount_usd > 0 else 0.0
    return {
        "hash": row.get("transaction_hash") or row.get("hash") or row.get("tx_hash"),
        "btc": round(btc_equiv, 6),
        "amount_usd": round(amount_usd, 2),
        "asset_quantity": round(asset_qty, 8),
        "asset_symbol": str(row.get("asset_symbol") or row.get("symbol") or "").upper(),
        "from": row.get("from") or row.get("from_address"),
        "to": row.get("to") or row.get("to_address"),
        "chain": str(row.get("blockchain_name") or row.get("chain") or "").lower(),
        "timestamp": _timestamp_to_iso(row.get("block_timestamp") or row.get("timestamp")),
        "provider": "coinglass_whale_transfer",
    }


def normalize_exchange_chain_transfer(row: Mapping[str, Any], *, btc_price: float) -> Dict[str, Any]:
    amount_usd = _coalesce_float(row, "amount_usd", "amountUsd", "usd", "value") or 0.0
    asset_qty = _coalesce_float(row, "asset_quantity", "assetQuantity", "quantity", "amount") or 0.0
    btc_equiv = amount_usd / btc_price if btc_price > 0 and amount_usd > 0 else 0.0
    transfer_type = str(row.get("transfer_type") or row.get("type") or "").strip()
    return {
        "hash": row.get("transaction_hash") or row.get("hash") or row.get("tx_hash"),
        "btc": round(btc_equiv, 6),
        "amount_usd": round(amount_usd, 2),
        "asset_quantity": round(asset_qty, 8),
        "asset_symbol": str(row.get("asset_symbol") or row.get("symbol") or "").upper(),
        "exchange_name": str(row.get("exchange_name") or row.get("exchange") or "").strip(),
        "transfer_type": transfer_type,
        "direction": exchange_flow_direction(transfer_type),
        "from": row.get("from_address") or row.get("from"),
        "to": row.get("to_address") or row.get("to"),
        "timestamp": _timestamp_to_iso(row.get("transaction_time") or row.get("timestamp")),
        "provider": "coinglass_exchange_chain_tx",
    }


def summarize_exchange_flows(transactions: List[Mapping[str, Any]]) -> Dict[str, Any]:
    inflow_usd = 0.0
    outflow_usd = 0.0
    whale_inflow_usd = 0.0
    whale_outflow_usd = 0.0
    inflow_count = 0
    outflow_count = 0
    other_count = 0
    exchange_names: List[str] = []
    asset_symbols: List[str] = []

    for raw in transactions:
        item = dict(raw or {})
        direction = exchange_flow_direction(item.get("direction") or item.get("transfer_type"))
        amount_usd = _to_float(item.get("amount_usd"), 0.0) or 0.0
        if direction == "inflow":
            inflow_usd += amount_usd
            whale_inflow_usd += amount_usd
            inflow_count += 1
        elif direction == "outflow":
            outflow_usd += amount_usd
            whale_outflow_usd += amount_usd
            outflow_count += 1
        else:
            other_count += 1
        exchange_name = str(item.get("exchange_name") or "").strip()
        if exchange_name:
            exchange_names.append(exchange_name)
        asset_symbol = str(item.get("asset_symbol") or "").strip().upper()
        if asset_symbol:
            asset_symbols.append(asset_symbol)

    netflow_usd = inflow_usd - outflow_usd
    gross_usd = inflow_usd + outflow_usd
    pressure = (netflow_usd / gross_usd) if gross_usd > 0 else 0.0
    return {
        "available": bool(inflow_count or outflow_count),
        "source": "coinglass_exchange_chain_tx",
        "spot_exchange_inflow_usd": round(inflow_usd, 2),
        "spot_exchange_outflow_usd": round(outflow_usd, 2),
        "spot_exchange_netflow_usd": round(netflow_usd, 2),
        "spot_exchange_inflow_count": inflow_count,
        "spot_exchange_outflow_count": outflow_count,
        "spot_exchange_other_count": other_count,
        "spot_netflow_score": round(max(-1.0, min(1.0, pressure)), 6),
        "exchange_flow_pressure": "inflow_sell_pressure" if pressure > 0.1 else "outflow_supply_tight" if pressure < -0.1 else "balanced",
        "whale_inflow_usd": round(whale_inflow_usd, 2),
        "whale_outflow_usd": round(whale_outflow_usd, 2),
        "exchange_names": list(dict.fromkeys(exchange_names))[:10],
        "asset_symbols": list(dict.fromkeys(asset_symbols))[:10],
    }


def normalize_spot_netflow_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    record = dict(row or {})
    inflow = _coalesce_float(record, "inflow_usd", "inflowUsd", "inflow", "deposit_usd")
    outflow = _coalesce_float(record, "outflow_usd", "outflowUsd", "outflow", "withdraw_usd")
    netflow = _coalesce_float(record, "netflow_usd", "netFlowUsd", "netflow", "netFlow")
    if netflow is None and inflow is not None and outflow is not None:
        netflow = inflow - outflow
    if inflow is not None:
        record["spot_exchange_inflow_usd"] = inflow
    if outflow is not None:
        record["spot_exchange_outflow_usd"] = outflow
    if netflow is not None:
        record["spot_exchange_netflow_usd"] = netflow
    gross = (inflow or 0.0) + (outflow or 0.0)
    if gross > 0 and netflow is not None:
        score = max(-1.0, min(1.0, netflow / gross))
        record["spot_netflow_score"] = score
        record["exchange_flow_pressure"] = (
            "inflow_sell_pressure" if score > 0.1 else "outflow_supply_tight" if score < -0.1 else "balanced"
        )
    return record


def normalize_exchange_balance_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    record = dict(row or {})
    balance_coin = _coalesce_float(record, "balance", "balance_btc", "exchange_balance_btc", "amount", "quantity")
    balance_usd = _coalesce_float(record, "balance_usd", "balanceUsd", "exchange_balance_usd", "value_usd", "valueUsd")
    change_24h = _coalesce_float(record, "change_24h", "change24h", "balance_change_24h", "change_24h_pct")
    change_7d = _coalesce_float(record, "change_7d", "change7d", "balance_change_7d", "change_7d_pct")
    stablecoin_balance = _coalesce_float(record, "stablecoin_balance_usd", "stablecoinBalanceUsd", "stablecoin_usd")
    stablecoin_change_24h = _coalesce_float(record, "stablecoin_change_24h", "stablecoinChange24h")
    if balance_coin is not None:
        record["exchange_balance_btc"] = balance_coin
    if balance_usd is not None:
        record["exchange_balance_usd"] = balance_usd
    if change_24h is not None:
        record["exchange_balance_change_24h"] = change_24h
    if change_7d is not None:
        record["exchange_balance_change_7d"] = change_7d
    if stablecoin_balance is not None:
        record["stablecoin_exchange_balance_usd"] = stablecoin_balance
    if stablecoin_change_24h is not None:
        record["stablecoin_exchange_balance_change_24h"] = stablecoin_change_24h
    return record


def normalize_options_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    record = dict(row or {})
    max_pain = _coalesce_float(record, "max_pain", "maxPain", "max_pain_price", "maxPainPrice")
    put_call = _coalesce_float(record, "put_call_ratio", "putCallRatio", "put_call", "putCall")
    oi_usd = _coalesce_float(record, "open_interest_usd", "openInterestUsd", "oi_usd", "oiUsd")
    volume_usd = _coalesce_float(record, "volume_usd", "volumeUsd", "vol_usd", "volUsd")
    iv = _coalesce_float(record, "iv", "atm_iv", "implied_volatility", "impliedVolatility")
    skew = _coalesce_float(record, "iv_skew", "skew", "skew_25d", "skew25d")
    gamma = _coalesce_float(record, "gamma_exposure", "gammaExposure", "gex")
    if max_pain is not None:
        record["option_max_pain"] = max_pain
    if put_call is not None:
        record["option_put_call_ratio"] = put_call
    if oi_usd is not None:
        record["option_open_interest_usd"] = oi_usd
    if volume_usd is not None:
        record["option_volume_usd"] = volume_usd
    if iv is not None:
        record["option_iv"] = iv
    if skew is not None:
        record["option_iv_skew"] = skew
    if gamma is not None:
        record["gamma_exposure"] = gamma
    return record


async def fetch_coinglass_whale_transfers(
    *, symbol: str, min_btc: float = 10.0, manual: bool = False
) -> Dict[str, Any]:
    if not coinglass_enabled():
        return {
            "available": False,
            "error": "coinglass_disabled",
            "source_name": "coinglass_whale_transfer",
            "threshold_btc": min_btc,
            "btc_price": 0.0,
            "count": 0,
            "transactions": [],
        }

    btc_price = await _fetch_btc_price()
    try:
        async with CoinglassClient(timeout_sec=8) as client:
            response = await client.request_json(
                "/v4/api/chain/v2/whale-transfer",
                params={"symbol": _symbol_base(symbol), "limit": 20},
                manual=manual,
            )
    except Exception as exc:
        return {
            "available": False,
            "error": str(exc),
            "source_name": "coinglass_whale_transfer",
            "threshold_btc": min_btc,
            "btc_price": btc_price,
            "count": 0,
            "transactions": [],
        }

    transactions = [normalize_whale_transfer(row, btc_price=btc_price) for row in _payload_rows(response)]
    threshold_usd = (float(min_btc) * btc_price) if btc_price > 0 else 0.0
    filtered = [
        item for item in transactions if threshold_usd <= 0 or (_to_float(item.get("amount_usd"), 0.0) or 0.0) >= threshold_usd
    ]
    return {
        "available": bool(filtered),
        "error": "",
        "source_name": "coinglass_whale_transfer",
        "threshold_btc": min_btc,
        "btc_price": btc_price,
        "count": len(filtered),
        "transactions": filtered[:20],
    }


async def fetch_coinglass_exchange_chain_transfers(
    *, symbol: str, min_btc: float = 10.0, manual: bool = False
) -> Dict[str, Any]:
    if not coinglass_enabled():
        return {
            "available": False,
            "error": "coinglass_disabled",
            "source_name": "coinglass_exchange_chain_tx",
            "threshold_btc": min_btc,
            "btc_price": 0.0,
            "count": 0,
            "transactions": [],
        }

    btc_price = await _fetch_btc_price()
    threshold_usd = (float(min_btc) * btc_price) if btc_price > 0 else 0.0
    try:
        async with CoinglassClient(timeout_sec=8) as client:
            response = await client.request_json(
                "/v4/api/exchange/chain/tx/list",
                params={
                    "symbol": _symbol_base(symbol),
                    "min_usd": round(threshold_usd, 2) if threshold_usd > 0 else None,
                    "per_page": 20,
                    "page": 1,
                },
                manual=manual,
            )
    except Exception as exc:
        return {
            "available": False,
            "error": str(exc),
            "source_name": "coinglass_exchange_chain_tx",
            "threshold_btc": min_btc,
            "btc_price": btc_price,
            "count": 0,
            "transactions": [],
        }

    transactions = [
        normalize_exchange_chain_transfer(row, btc_price=btc_price)
        for row in _payload_rows(response)
    ]
    filtered = [
        item for item in transactions if threshold_usd <= 0 or (_to_float(item.get("amount_usd"), 0.0) or 0.0) >= threshold_usd
    ]
    return {
        "available": bool(filtered),
        "error": "",
        "source_name": "coinglass_exchange_chain_tx",
        "threshold_btc": min_btc,
        "btc_price": btc_price,
        "count": len(filtered),
        "flow_summary": summarize_exchange_flows(filtered),
        "transactions": filtered[:20],
    }


async def fetch_spot_netflow_summary(
    *, symbol: str, exchange_list: str = _DEFAULT_EXCHANGE_LIST, manual: bool = False
) -> Dict[str, Any]:
    if not coinglass_enabled():
        return {"available": False, "error": "coinglass_disabled", "source": "coinglass_spot_netflow"}
    try:
        async with CoinglassClient(timeout_sec=8) as client:
            response = await client.request_json(
                "/v4/api/spot/coin/netflow",
                params={"symbol": _symbol_base(symbol), "exchange_list": exchange_list},
                manual=manual,
            )
    except Exception as exc:
        return {"available": False, "error": str(exc), "source": "coinglass_spot_netflow"}
    rows = [normalize_spot_netflow_row(row) for row in _payload_rows(response)]
    if not rows:
        return {"available": False, "error": "empty_spot_netflow", "source": "coinglass_spot_netflow"}
    inflow = sum(_to_float(row.get("spot_exchange_inflow_usd"), 0.0) or 0.0 for row in rows)
    outflow = sum(_to_float(row.get("spot_exchange_outflow_usd"), 0.0) or 0.0 for row in rows)
    netflow = sum(_to_float(row.get("spot_exchange_netflow_usd"), 0.0) or 0.0 for row in rows)
    gross = inflow + outflow
    score = (netflow / gross) if gross > 0 else 0.0
    return {
        "available": True,
        "source": "coinglass_spot_netflow",
        "symbol": _symbol_base(symbol),
        "spot_exchange_inflow_usd": round(inflow, 2),
        "spot_exchange_outflow_usd": round(outflow, 2),
        "spot_exchange_netflow_usd": round(netflow, 2),
        "spot_netflow_score": round(max(-1.0, min(1.0, score)), 6),
        "exchange_flow_pressure": "inflow_sell_pressure" if score > 0.1 else "outflow_supply_tight" if score < -0.1 else "balanced",
        "rows": rows[:20],
    }


async def fetch_exchange_balance_snapshot(
    *, symbol: str, manual: bool = False
) -> Dict[str, Any]:
    if not coinglass_enabled():
        return {"available": False, "error": "coinglass_disabled", "source": "coinglass_exchange_balance"}
    try:
        async with CoinglassClient(timeout_sec=8) as client:
            response = await client.request_json(
                "/v4/api/exchange/balance/list",
                params={"symbol": _symbol_base(symbol)},
                manual=manual,
            )
    except Exception as exc:
        return {"available": False, "error": str(exc), "source": "coinglass_exchange_balance"}
    rows = [normalize_exchange_balance_row(row) for row in _payload_rows(response)]
    if not rows:
        return {"available": False, "error": "empty_exchange_balance", "source": "coinglass_exchange_balance"}
    balance_btc = sum(_to_float(row.get("exchange_balance_btc"), 0.0) or 0.0 for row in rows)
    balance_usd = sum(_to_float(row.get("exchange_balance_usd"), 0.0) or 0.0 for row in rows)
    stablecoin_usd = sum(_to_float(row.get("stablecoin_exchange_balance_usd"), 0.0) or 0.0 for row in rows)
    change_24h_values = [
        _to_float(row.get("exchange_balance_change_24h"), None)
        for row in rows
        if _to_float(row.get("exchange_balance_change_24h"), None) is not None
    ]
    change_7d_values = [
        _to_float(row.get("exchange_balance_change_7d"), None)
        for row in rows
        if _to_float(row.get("exchange_balance_change_7d"), None) is not None
    ]
    change_24h = sum(change_24h_values) / len(change_24h_values) if change_24h_values else None
    change_7d = sum(change_7d_values) / len(change_7d_values) if change_7d_values else None
    stablecoin_netflow = sum(
        _to_float(row.get("stablecoin_exchange_balance_change_24h"), 0.0) or 0.0 for row in rows
    )
    pressure = 0.0
    if balance_usd > 0 and change_24h is not None:
        pressure = max(-1.0, min(1.0, float(change_24h) / 10.0))
    return {
        "available": True,
        "source": "coinglass_exchange_balance",
        "symbol": _symbol_base(symbol),
        "exchange_balance_btc": round(balance_btc, 8),
        "exchange_balance_usd": round(balance_usd, 2),
        "exchange_balance_change_24h": None if change_24h is None else round(change_24h, 6),
        "exchange_balance_change_7d": None if change_7d is None else round(change_7d, 6),
        "stablecoin_exchange_balance_usd": round(stablecoin_usd, 2),
        "stablecoin_netflow_usd": round(stablecoin_netflow, 2),
        "exchange_reserve_pressure_score": round(abs(pressure), 6),
        "onchain_activity_score": round(min(1.0, len(rows) / 20.0), 6),
        "rows": rows[:20],
    }
