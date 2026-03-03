from __future__ import annotations

import math
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from prediction_markets.polymarket.utils import parse_ts_any

CATEGORY_ORDER = ["PRICE", "MACRO", "REG_ETF", "ELECTION_GEO"]


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _normalize_symbol(symbol: str) -> str:
    raw = str(symbol or "").strip().upper()
    if "/" in raw:
        return raw.replace("/", "")
    if raw.endswith(":USDT"):
        raw = raw.split(":", 1)[0]
    return raw.replace("_", "")


def _quotes_df(quotes: List[Dict[str, Any]]) -> pd.DataFrame:
    if not quotes:
        return pd.DataFrame()
    rows = []
    for item in quotes:
        ts = parse_ts_any(item.get("ts"))
        weights = item.get("symbol_weights") or {}
        price = item.get("midpoint") if item.get("midpoint") is not None else item.get("price")
        rows.append(
            {
                "ts": ts,
                "market_id": str(item.get("market_id") or ""),
                "token_id": str(item.get("token_id") or ""),
                "outcome": str(item.get("outcome") or "YES").upper(),
                "category": str(item.get("category") or "OTHER").upper(),
                "prob_level": _safe_float(price, 0.0),
                "bid": _safe_float(item.get("bid"), np.nan),
                "ask": _safe_float(item.get("ask"), np.nan),
                "midpoint": _safe_float(item.get("midpoint"), np.nan),
                "spread": _safe_float(item.get("spread"), np.nan),
                "depth1": _safe_float(item.get("depth1"), np.nan),
                "depth5": _safe_float(item.get("depth5"), np.nan),
                "relevance_score": max(0.0, _safe_float(item.get("relevance_score"), 0.0)),
                "symbol_weights": weights,
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.sort_values(["category", "market_id", "token_id", "ts"]).reset_index(drop=True)
    return df


def _resample_single_market(df: pd.DataFrame, timeframe: str = "1m", shock_abs_dprob_5m: float = 0.05) -> pd.DataFrame:
    if df.empty:
        return df
    rule = "1min" if timeframe == "1m" else "5min"
    out_rows = []
    for (category, market_id, token_id), g in df.groupby(["category", "market_id", "token_id"], sort=False):
        g = g.set_index("ts").sort_index()
        agg = g.resample("1min").agg(
            {
                "prob_level": "last",
                "spread": "last",
                "depth1": "last",
                "depth5": "last",
                "relevance_score": "last",
            }
        )
        agg = agg.dropna(subset=["prob_level"])
        if agg.empty:
            continue
        agg["dprob_1m"] = agg["prob_level"].diff()
        agg["dprob_5m"] = agg["prob_level"].diff(5)
        agg["prob_vol_30m"] = agg["prob_level"].rolling(30, min_periods=5).std().fillna(0.0)
        agg["shock_flag"] = agg["dprob_5m"].abs() > shock_abs_dprob_5m
        agg["shock_severity"] = (agg["dprob_5m"].abs() / max(shock_abs_dprob_5m, 1e-9)).fillna(0.0)
        agg["liquidity_score"] = (agg["depth5"].fillna(0.0) / agg["spread"].replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        agg["category"] = category
        agg["market_id"] = market_id
        agg["token_id"] = token_id
        agg["outcome"] = str(g["outcome"].iloc[-1]) if "outcome" in g.columns else "YES"
        out_rows.append(agg.reset_index())
    if not out_rows:
        return pd.DataFrame()
    result = pd.concat(out_rows, ignore_index=True)
    if timeframe == "5m":
        result = (
            result.set_index("ts")
            .groupby(["category", "market_id", "token_id", "outcome"], group_keys=False)
            .resample(rule)
            .agg(
                {
                    "prob_level": "last",
                    "dprob_1m": "last",
                    "dprob_5m": "last",
                    "prob_vol_30m": "last",
                    "shock_flag": "max",
                    "shock_severity": "max",
                    "liquidity_score": "mean",
                    "spread": "last",
                    "depth1": "last",
                    "depth5": "last",
                    "relevance_score": "last",
                }
            )
            .reset_index()
        )
    return result.sort_values(["category", "ts", "market_id", "token_id"]).reset_index(drop=True)


def build_feature_frame(quotes: List[Dict[str, Any]], timeframe: str = "1m", shock_abs_dprob_5m: float = 0.05) -> pd.DataFrame:
    raw = _quotes_df(quotes)
    if raw.empty:
        return pd.DataFrame()
    market = _resample_single_market(raw, timeframe=timeframe, shock_abs_dprob_5m=shock_abs_dprob_5m)
    if market.empty:
        return market
    market["weighted_prob"] = market["prob_level"] * market["relevance_score"].clip(lower=1.0)
    market["weighted_dprob_1m"] = market["dprob_1m"].fillna(0.0) * market["relevance_score"].clip(lower=1.0)
    market["weighted_dprob_5m"] = market["dprob_5m"].fillna(0.0) * market["relevance_score"].clip(lower=1.0)
    market["weighted_liquidity"] = market["liquidity_score"].fillna(0.0) * market["relevance_score"].clip(lower=1.0)

    groups = []
    for (category, ts), g in market.groupby(["category", "ts"], sort=False):
        weights = g["relevance_score"].clip(lower=1.0)
        top_idx = g["shock_severity"].fillna(0.0).astype(float).idxmax()
        top_row = g.loc[top_idx] if top_idx in g.index else g.iloc[0]
        total_w = float(weights.sum() or 1.0)
        groups.append(
            {
                "ts": ts,
                "category": category,
                "cat_prob": float((g["weighted_prob"].sum()) / total_w),
                "cat_dprob_1m": float((g["weighted_dprob_1m"].sum()) / total_w),
                "cat_dprob_5m": float((g["weighted_dprob_5m"].sum()) / total_w),
                "cat_shock_sev": float(g["shock_severity"].fillna(0.0).max()),
                "cat_liquidity": float((g["weighted_liquidity"].sum()) / total_w),
                "top_market_id": str(top_row.get("market_id") or ""),
                "top_token_id": str(top_row.get("token_id") or ""),
            }
        )
    return pd.DataFrame(groups).sort_values(["category", "ts"]).reset_index(drop=True)


def _symbol_weight_from_quotes(quotes: List[Dict[str, Any]], symbol: str) -> float:
    target = _normalize_symbol(symbol)
    best = 0.0
    for item in quotes:
        weights = item.get("symbol_weights") or {}
        for key, value in weights.items():
            if _normalize_symbol(str(key)) == target:
                best = max(best, _safe_float(value, 0.0))
    return best


def get_features_range_from_quotes(
    quotes: List[Dict[str, Any]],
    *,
    symbol: str,
    since: datetime,
    until: datetime,
    timeframe: str = "1m",
    shock_abs_dprob_5m: float = 0.05,
) -> List[Dict[str, Any]]:
    frame = build_feature_frame(quotes, timeframe=timeframe, shock_abs_dprob_5m=shock_abs_dprob_5m)
    if frame.empty:
        return []
    symbol_norm = _normalize_symbol(symbol)
    price_weight = _symbol_weight_from_quotes(quotes, symbol_norm) or 0.0
    pivot = frame.pivot(index="ts", columns="category", values=["cat_prob", "cat_dprob_1m", "cat_dprob_5m", "cat_shock_sev", "cat_liquidity"])
    pivot = pivot.sort_index().ffill()
    rows: List[Dict[str, Any]] = []
    for ts, row in pivot.iterrows():
        if ts < pd.Timestamp(parse_ts_any(since)) or ts > pd.Timestamp(parse_ts_any(until)):
            continue
        macro_shock = _safe_float(row.get(("cat_shock_sev", "MACRO")), 0.0)
        reg_shock = _safe_float(row.get(("cat_shock_sev", "REG_ETF")), 0.0)
        geo_shock = _safe_float(row.get(("cat_shock_sev", "ELECTION_GEO")), 0.0)
        price_shock = _safe_float(row.get(("cat_shock_sev", "PRICE")), 0.0)
        price_dprob = _safe_float(row.get(("cat_dprob_5m", "PRICE")), 0.0)
        pm_global_risk = 0.40 * macro_shock + 0.35 * reg_shock + 0.25 * geo_shock
        pm_price_signal = price_shock * price_weight * (1.0 if price_dprob >= 0 else -1.0)
        rows.append(
            {
                "ts": pd.Timestamp(ts).tz_convert("UTC").isoformat(),
                "symbol": symbol_norm,
                "timeframe": timeframe,
                "pm_global_risk": round(float(pm_global_risk), 6),
                "pm_price_signal": round(float(pm_price_signal), 6),
                "pm_macro_shock_sev": round(float(macro_shock), 6),
                "pm_reg_shock_sev": round(float(reg_shock), 6),
                "pm_geo_shock_sev": round(float(geo_shock), 6),
                "pm_price_liquidity": round(_safe_float(row.get(("cat_liquidity", "PRICE")), 0.0), 6),
                "pm_feature_quality": round(min(1.0, max(0.0, price_weight or 0.0)), 6),
            }
        )
    return rows


def get_features_asof_from_quotes(
    quotes: List[Dict[str, Any]],
    *,
    symbol: str,
    ts: datetime,
    timeframe: str = "1m",
    shock_abs_dprob_5m: float = 0.05,
) -> Dict[str, Any]:
    rows = get_features_range_from_quotes(
        quotes,
        symbol=symbol,
        since=parse_ts_any(ts) - pd.Timedelta(days=3),
        until=parse_ts_any(ts),
        timeframe=timeframe,
        shock_abs_dprob_5m=shock_abs_dprob_5m,
    )
    if not rows:
        return {}
    target_ts = pd.Timestamp(parse_ts_any(ts))
    chosen = None
    for row in rows:
        row_ts = pd.Timestamp(row["ts"])
        if row_ts <= target_ts:
            chosen = row
        else:
            break
    return chosen or rows[-1]
