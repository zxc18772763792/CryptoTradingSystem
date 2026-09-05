"""Pure helpers for immutable Binance launch-time microstructure snapshots."""

from __future__ import annotations

from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd


def utc(value: Any) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    return stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if np.isfinite(number) else None


def _market_impact(levels: list[tuple[float, float]], notional: float, mid: float, *, buy: bool) -> float | None:
    remaining = float(notional)
    base_filled = 0.0
    quote_filled = 0.0
    for price, quantity in levels:
        available_quote = price * quantity
        used_quote = min(remaining, available_quote)
        if used_quote <= 0:
            continue
        base_filled += used_quote / price
        quote_filled += used_quote
        remaining -= used_quote
        if remaining <= 1e-9:
            break
    if remaining > 1e-6 or base_filled <= 0 or mid <= 0:
        return None
    vwap = quote_filled / base_filled
    return float((vwap / mid - 1.0) * 10_000.0 if buy else (1.0 - vwap / mid) * 10_000.0)


def orderbook_metrics(
    payload: Mapping[str, Any],
    *,
    depth_bands: Iterable[float] = (0.005, 0.01),
    impact_notionals: Iterable[float] = (2_000.0, 10_000.0),
) -> dict[str, Any]:
    bids = [(_finite(row[0]), _finite(row[1])) for row in payload.get("bids", []) if len(row) >= 2]
    asks = [(_finite(row[0]), _finite(row[1])) for row in payload.get("asks", []) if len(row) >= 2]
    clean_bids = sorted([(p, q) for p, q in bids if p is not None and q is not None and p > 0 and q > 0], reverse=True)
    clean_asks = sorted([(p, q) for p, q in asks if p is not None and q is not None and p > 0 and q > 0])
    if not clean_bids or not clean_asks:
        raise ValueError("order book has no valid bid/ask levels")
    best_bid = clean_bids[0][0]
    best_ask = clean_asks[0][0]
    if best_ask < best_bid:
        raise ValueError("crossed order book")
    mid = 0.5 * (best_bid + best_ask)
    output: dict[str, Any] = {
        "last_update_id": payload.get("lastUpdateId"),
        "event_time_utc": None if payload.get("E") is None else pd.to_datetime(payload["E"], unit="ms", utc=True),
        "transaction_time_utc": None if payload.get("T") is None else pd.to_datetime(payload["T"], unit="ms", utc=True),
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid_price": mid,
        "spread_bps": float((best_ask / best_bid - 1.0) * 10_000.0),
        "levels_bid": len(clean_bids),
        "levels_ask": len(clean_asks),
    }
    for band in depth_bands:
        suffix = f"{int(round(10000 * float(band)))}bp"
        bid_quote = float(sum(price * qty for price, qty in clean_bids if price >= mid * (1.0 - float(band))))
        ask_quote = float(sum(price * qty for price, qty in clean_asks if price <= mid * (1.0 + float(band))))
        total = bid_quote + ask_quote
        output[f"bid_depth_usd_{suffix}"] = bid_quote
        output[f"ask_depth_usd_{suffix}"] = ask_quote
        output[f"total_depth_usd_{suffix}"] = total
        output[f"depth_imbalance_{suffix}"] = float((bid_quote - ask_quote) / total) if total > 0 else None
    for notional in impact_notionals:
        suffix = str(int(round(float(notional))))
        output[f"buy_impact_bps_{suffix}"] = _market_impact(clean_asks, float(notional), mid, buy=True)
        output[f"sell_impact_bps_{suffix}"] = _market_impact(clean_bids, float(notional), mid, buy=False)
    return output


def records_at_or_before(
    records: Iterable[Mapping[str, Any]],
    *,
    cutoff: pd.Timestamp,
    timestamp_key: str = "timestamp",
) -> list[dict[str, Any]]:
    limit = utc(cutoff)
    output: list[dict[str, Any]] = []
    for raw in records:
        if raw.get(timestamp_key) is None:
            continue
        when = pd.to_datetime(raw[timestamp_key], unit="ms", utc=True)
        if when <= limit:
            row = dict(raw)
            row["_time"] = when
            output.append(row)
    return sorted(output, key=lambda row: row["_time"])


def latest_and_changes(
    records: Iterable[Mapping[str, Any]],
    *,
    cutoff: pd.Timestamp,
    value_key: str,
    lookbacks_hours: Iterable[int] = (1, 4, 24),
) -> dict[str, Any]:
    rows = records_at_or_before(records, cutoff=cutoff)
    valid = [(row["_time"], _finite(row.get(value_key))) for row in rows]
    valid = [(when, value) for when, value in valid if value is not None]
    if not valid:
        return {"latest": None, "latest_time_utc": None, **{f"change_{hours}h": None for hours in lookbacks_hours}}
    latest_time, latest = valid[-1]
    output: dict[str, Any] = {"latest": latest, "latest_time_utc": latest_time}
    for hours in lookbacks_hours:
        threshold = latest_time - pd.Timedelta(hours=int(hours))
        prior = [(when, value) for when, value in valid if when <= threshold]
        old = prior[-1][1] if prior else None
        output[f"change_{hours}h"] = None if old in (None, 0) else float(latest / old - 1.0)
    return output


def taker_flow(
    records: Iterable[Mapping[str, Any]],
    *,
    cutoff: pd.Timestamp,
    windows_hours: Iterable[int] = (1, 4, 24),
) -> dict[str, Any]:
    rows = records_at_or_before(records, cutoff=cutoff)
    limit = utc(cutoff)
    output: dict[str, Any] = {}
    for hours in windows_hours:
        start = limit - pd.Timedelta(hours=int(hours))
        window = [row for row in rows if row["_time"] > start]
        buy = float(sum(_finite(row.get("buyVol")) or 0.0 for row in window))
        sell = float(sum(_finite(row.get("sellVol")) or 0.0 for row in window))
        total = buy + sell
        output[f"buy_volume_{hours}h"] = buy
        output[f"sell_volume_{hours}h"] = sell
        output[f"taker_buy_share_{hours}h"] = float(buy / total) if total > 0 else None
        output[f"buy_sell_ratio_{hours}h"] = float(buy / sell) if sell > 0 else None
        output[f"observations_{hours}h"] = len(window)
    output["latest_time_utc"] = rows[-1]["_time"] if rows else None
    return output


def premium_metrics(payload: Mapping[str, Any]) -> dict[str, Any]:
    mark = _finite(payload.get("markPrice"))
    index = _finite(payload.get("indexPrice"))
    return {
        "symbol": payload.get("symbol"),
        "source_time_utc": None if payload.get("time") is None else pd.to_datetime(payload["time"], unit="ms", utc=True),
        "mark_price": mark,
        "index_price": index,
        "basis_bps": None if mark is None or index in (None, 0) else float((mark / index - 1.0) * 10_000.0),
        "last_funding_rate": _finite(payload.get("lastFundingRate")),
        "next_funding_time_utc": None if payload.get("nextFundingTime") is None else pd.to_datetime(payload["nextFundingTime"], unit="ms", utc=True),
    }


def validate_capture_clock(
    *,
    decision_time: pd.Timestamp,
    capture_time: pd.Timestamp,
    maximum_lag_minutes: int = 15,
) -> dict[str, Any]:
    decision = utc(decision_time)
    capture = utc(capture_time)
    lag = float((capture - decision).total_seconds())
    return {
        "decision_time_utc": decision,
        "capture_time_utc": capture,
        "capture_lag_seconds": lag,
        "passed": bool(0 <= lag <= maximum_lag_minutes * 60),
        "maximum_lag_minutes": int(maximum_lag_minutes),
    }
