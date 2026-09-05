"""Read-only forward monitor for the frozen Binance extreme-run-up model.

This entry point deliberately has no dependency on order, strategy-manager, or
execution modules.  It creates immutable daily research snapshots and appends
outcomes only after their observation horizon has matured.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_VALIDATION_PATH = ROOT / "core" / "research" / "binance_runup_validation.py"
_SPEC = importlib.util.spec_from_file_location("binance_runup_validation_standalone", _VALIDATION_PATH)
if _SPEC is None or _SPEC.loader is None:
    raise RuntimeError(f"Unable to load {_VALIDATION_PATH}")
_VALIDATION = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _VALIDATION
_SPEC.loader.exec_module(_VALIDATION)

FrozenLogisticModel = _VALIDATION.FrozenLogisticModel
PRICE_FACTORS = _VALIDATION.PRICE_FACTORS
json_ready = _VALIDATION.json_ready
scan_30d_runups = _VALIDATION.scan_30d_runups
stable_hash = _VALIDATION.stable_hash


DEFAULT_BASELINE = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures"
DEFAULT_OUTPUT = ROOT / "data" / "research" / "binance_200pct_forward_monitor"
FAPI = "https://fapi.binance.com"
USER_AGENT = "binance-runup-forward-monitor/1.0-read-only"
HORIZONS = (3, 7, 14, 30)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", help="Research cutoff (ISO-8601); defaults to now")
    parser.add_argument("--dry-run", action="store_true", help="Compute and print audit; write nothing")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--baseline-dir", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--no-external-refresh", action="store_true")
    return parser.parse_args()


def _utc_timestamp(value: str | datetime | pd.Timestamp | None) -> pd.Timestamp:
    if value is None:
        local_date = pd.Timestamp.now(tz="Asia/Shanghai").date()
    elif isinstance(value, str) and len(value.strip()) == 10:
        local_date = pd.Timestamp(value).date()
    else:
        timestamp = pd.Timestamp(value)
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize("Asia/Shanghai")
        local_date = timestamp.tz_convert("Asia/Shanghai").date()
    # A logical monitoring day has one canonical cutoff.  Retries therefore
    # serialize identically instead of producing a conflicting same-day file.
    scheduled = pd.Timestamp(f"{local_date.isoformat()} 08:20:00", tz="Asia/Shanghai")
    return scheduled.tz_convert("UTC")


def _request_json(path: str, params: dict[str, Any] | None = None) -> Any:
    response = requests.get(
        f"{FAPI}{path}",
        params=params,
        headers={"User-Agent": USER_AGENT},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def current_universe() -> tuple[pd.DataFrame, str]:
    payload = _request_json("/fapi/v1/exchangeInfo")
    records = []
    excluded = {"BTC", "ETH", "USDT", "USDC", "FDUSD", "TUSD", "USDP", "BUSD", "DAI"}
    for item in payload.get("symbols", []):
        if (
            item.get("status") == "TRADING"
            and item.get("quoteAsset") == "USDT"
            and item.get("contractType") == "PERPETUAL"
            and item.get("baseAsset") not in excluded
        ):
            records.append(
                {
                    "symbol": str(item["symbol"]),
                    "base_asset": str(item["baseAsset"]),
                    "onboard_date": pd.to_datetime(item.get("onboardDate"), unit="ms", utc=True),
                }
            )
    return pd.DataFrame(records).sort_values("symbol"), stable_hash(payload)


def _normalize_klines(raw: list[list[Any]], symbol: str, base_asset: str) -> pd.DataFrame:
    columns = [
        "open_time_ms", "open", "high", "low", "close", "base_volume",
        "close_time_ms", "quote_volume", "trades", "taker_buy_base",
        "taker_buy_quote", "ignore",
    ]
    frame = pd.DataFrame(raw, columns=columns)
    if frame.empty:
        return frame
    frame["symbol"] = symbol
    frame["base_asset"] = base_asset
    for column in ["open", "high", "low", "close", "quote_volume", "taker_buy_quote"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["trades"] = pd.to_numeric(frame["trades"], errors="coerce")
    frame["open_time"] = pd.to_datetime(frame["open_time_ms"], unit="ms", utc=True)
    return frame[
        ["symbol", "base_asset", "open_time", "open", "high", "low", "close",
         "quote_volume", "trades", "taker_buy_quote"]
    ].drop_duplicates(["symbol", "open_time"])


def _fetch_symbol_klines(row: pd.Series, end_ms: int) -> tuple[pd.DataFrame, str | None]:
    try:
        raw = _request_json(
            "/fapi/v1/klines",
            {"symbol": row["symbol"], "interval": "4h", "endTime": end_ms, "limit": 300},
        )
        return _normalize_klines(raw, str(row["symbol"]), str(row["base_asset"])), None
    except Exception as exc:  # noqa: BLE001 - per-symbol failures are audit data
        return pd.DataFrame(), f"{row['symbol']}: {type(exc).__name__}: {exc}"


def fetch_live_panel(universe: pd.DataFrame, feature_date: pd.Timestamp) -> tuple[pd.DataFrame, list[str]]:
    end_ms = int((feature_date + pd.Timedelta(days=1) - pd.Timedelta(milliseconds=1)).timestamp() * 1000)
    frames: list[pd.DataFrame] = []
    errors: list[str] = []
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = [pool.submit(_fetch_symbol_klines, row, end_ms) for _, row in universe.iterrows()]
        for future in as_completed(futures):
            frame, error = future.result()
            if not frame.empty:
                frames.append(frame)
            if error:
                errors.append(error)
    panel = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return panel.sort_values(["symbol", "open_time"]), sorted(errors)


def engineer_daily_features(panel_4h: pd.DataFrame) -> pd.DataFrame:
    rows = panel_4h.copy()
    rows["date"] = rows["open_time"].dt.floor("D")
    daily = (
        rows.sort_values(["symbol", "open_time"])
        .groupby(["symbol", "base_asset", "date"], as_index=False)
        .agg(
            open=("open", "first"), high=("high", "max"), low=("low", "min"),
            close=("close", "last"), quote_volume=("quote_volume", "sum"),
            trades=("trades", "sum"), taker_buy_quote=("taker_buy_quote", "sum"),
            bars=("open_time", "size"),
        )
    )
    output: list[pd.DataFrame] = []
    for _, group in daily.groupby("symbol", sort=False):
        group = group.sort_values("date").copy()
        close = group["close"]
        returns = close.pct_change(fill_method=None)
        group["return_1d"] = returns
        previous_close = close.shift(1)
        prior_high_20 = group["high"].shift(1).rolling(20, min_periods=5).max()
        rolling_high_30 = group["high"].rolling(30, min_periods=7).max()
        rolling_low_30 = group["low"].rolling(30, min_periods=7).min()
        true_range = pd.concat(
            [
                group["high"] - group["low"],
                (group["high"] - previous_close).abs(),
                (group["low"] - previous_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        range_7d = group["high"].rolling(7, min_periods=3).max() / group["low"].rolling(7, min_periods=3).min() - 1
        range_30d = rolling_high_30 / rolling_low_30 - 1
        group["realized_vol_7d"] = returns.rolling(7, min_periods=4).std() * math.sqrt(365)
        group["intraday_range_1d"] = group["high"] / group["low"] - 1.0
        group["atr_7d_pct"] = true_range.rolling(7, min_periods=3).mean() / close
        group["upper_wick_pct"] = (group["high"] - group[["open", "close"]].max(axis=1)) / close
        group["breakout_vs_prior_20d"] = close / prior_high_20 - 1
        group["drawdown_from_30d_high"] = close / rolling_high_30 - 1
        group["lower_wick_pct"] = (group[["open", "close"]].min(axis=1) - group["low"]) / close
        group["range_compression_7d_30d"] = range_7d / range_30d.replace(0, np.nan)
        output.append(group)
    return pd.concat(output, ignore_index=True) if output else pd.DataFrame()


def load_baseline_rows(baseline: Path, feature_date: pd.Timestamp) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    daily = pd.read_csv(baseline / "daily_feature_panel.csv.gz", compression="gzip")
    daily["date"] = pd.to_datetime(daily["date"], utc=True)
    latest = daily[daily["date"] <= feature_date].sort_values("date").groupby("symbol").tail(1)
    if "is_altcoin" in latest.columns:
        latest = latest[latest["is_altcoin"].fillna(False)].copy()
    panel = pd.read_csv(baseline / "futures_4h_panel.csv.gz", compression="gzip")
    panel["open_time"] = pd.to_datetime(panel["open_time"], utc=True)
    panel = panel[panel["open_time"] < feature_date + pd.Timedelta(days=1)].copy()
    universe_hash = stable_hash(sorted(latest["symbol"].astype(str).tolist()))
    return latest, panel, universe_hash


def score_cross_section(rows: pd.DataFrame, model: FrozenLogisticModel) -> pd.DataFrame:
    scored = rows.copy()
    scored["score"] = model.predict_score(scored)
    scored["score_pctile"] = scored["score"].rank(pct=True, method="first")
    return scored.sort_values("score", ascending=False)


def classify_stage(row: pd.Series, recent_runups: set[str]) -> str:
    if str(row["symbol"]) in recent_runups:
        return "cooldown_after_200pct_runup"
    chase = (
        float(row.get("return_1d", 0) or 0) >= 0.20
        or float(row.get("intraday_range_1d", 0) or 0) >= 0.35
        or float(row.get("upper_wick_pct", 0) or 0) >= 0.15
    )
    return "reject_chase" if chase else "price_watch"


def annotate_oi(symbol: str, reference_close: float) -> tuple[str, dict[str, Any]]:
    try:
        oi = _request_json("/fapi/v1/openInterest", {"symbol": symbol})
        funding = _request_json("/fapi/v1/fundingRate", {"symbol": symbol, "limit": 20})
        oi_units = float(oi["openInterest"])
        oi_usd = oi_units * reference_close
        funding_values = [float(item["fundingRate"]) for item in funding]
        latest_funding = funding_values[-1] if funding_values else np.nan
        mean_funding = float(np.mean(funding_values)) if funding_values else np.nan
        if np.isfinite(latest_funding) and (latest_funding >= 0.001 or abs(mean_funding) >= 0.0005):
            label = "crowded"
        elif oi_usd >= 1_000_000 and np.isfinite(mean_funding):
            label = "supportive"
        else:
            label = "neutral"
        return label, {
            "oi_usd": oi_usd,
            "funding_rate_latest": latest_funding,
            "funding_rate_mean_20": mean_funding,
            "oi_timestamp_utc": pd.to_datetime(oi.get("time"), unit="ms", utc=True, errors="coerce"),
            "oi_affects_rank": False,
        }
    except Exception as exc:  # noqa: BLE001
        return "missing", {"oi_error": f"{type(exc).__name__}: {exc}", "oi_affects_rank": False}


def previous_universe_count(output_root: Path, *, before_date: str | None = None) -> int | None:
    files = sorted((output_root / "snapshots").glob("*.json")) if (output_root / "snapshots").exists() else []
    if before_date is not None:
        files = [path for path in files if path.stem < before_date]
    if not files:
        return None
    payload = json.loads(files[-1].read_text(encoding="utf-8"))
    return int(payload.get("audit", {}).get("universe_count", 0)) or None


def assess_quality(
    *,
    rows: pd.DataFrame,
    universe_count: int,
    previous_count: int | None,
    feature_date: pd.Timestamp,
    latest_bar: pd.Timestamp | None,
    source_errors: list[str],
) -> dict[str, Any]:
    valid = rows.loc[:, list(PRICE_FACTORS)].notna().all(axis=1) if not rows.empty else pd.Series(dtype=bool)
    valid_rate = float(valid.sum() / universe_count) if universe_count else 0.0
    coverage = float(universe_count / previous_count) if previous_count else 1.0
    expected_latest = feature_date + pd.Timedelta(hours=20)
    stale = latest_bar is None or latest_bar < expected_latest
    passed = coverage >= 0.90 and valid_rate >= 0.95 and not stale
    reasons = []
    if coverage < 0.90:
        reasons.append("universe_coverage_below_90pct_of_previous")
    if valid_rate < 0.95:
        reasons.append("valid_price_features_below_95pct")
    if stale:
        reasons.append("key_market_data_stale")
    return {
        "passed": passed,
        "universe_count": universe_count,
        "previous_universe_count": previous_count,
        "universe_coverage_vs_previous": coverage,
        "valid_price_feature_count": int(valid.sum()),
        "valid_price_feature_rate": valid_rate,
        "feature_date_utc": feature_date,
        "latest_4h_bar_utc": latest_bar,
        "source_error_count": len(source_errors),
        "failure_reasons": reasons,
    }


def immutable_write_json(path: Path, payload: dict[str, Any]) -> str:
    encoded = json.dumps(json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if path.exists():
        current = path.read_text(encoding="utf-8")
        if current != encoded:
            raise RuntimeError(f"Immutable snapshot exists with different content: {path}")
        return "idempotent_existing"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(encoded, encoding="utf-8")
    return "created"


def append_outcomes(output_root: Path, panel_4h: pd.DataFrame, as_of: pd.Timestamp) -> int:
    snapshot_dir = output_root / "snapshots"
    outcome_path = output_root / "outcomes.csv"
    existing = pd.read_csv(outcome_path) if outcome_path.exists() else pd.DataFrame()
    existing_keys = set()
    if not existing.empty:
        existing_keys = set(zip(existing["signal_id"].astype(str), existing["horizon"].astype(int)))
    records: list[dict[str, Any]] = []
    for path in sorted(snapshot_dir.glob("*.json")) if snapshot_dir.exists() else []:
        payload = json.loads(path.read_text(encoding="utf-8"))
        for signal in payload.get("universe_rows", payload.get("candidates", [])):
            signal_time = pd.Timestamp(signal["data_cutoff_utc"])
            symbol_bars = panel_4h[panel_4h["symbol"] == signal["symbol"]].sort_values("open_time")
            entry_bars = symbol_bars[symbol_bars["open_time"] >= signal_time]
            if entry_bars.empty:
                continue
            entry = entry_bars.iloc[0]
            entry_time = pd.Timestamp(entry["open_time"])
            entry_price = float(entry["open"])
            for horizon in HORIZONS:
                key = (str(signal["signal_id"]), horizon)
                if key in existing_keys or as_of < entry_time + pd.Timedelta(days=horizon):
                    continue
                path_rows = symbol_bars[
                    symbol_bars["open_time"].between(entry_time, entry_time + pd.Timedelta(days=horizon), inclusive="left")
                ]
                if path_rows.empty:
                    continue
                hit = path_rows[path_rows["high"] >= entry_price * 3.0]
                records.append(
                    {
                        "signal_id": signal["signal_id"], "horizon": horizon,
                        "entry_time_utc": entry_time, "entry_price": entry_price,
                        "max_return": float(path_rows["high"].max() / entry_price - 1),
                        "min_return": float(path_rows["low"].min() / entry_price - 1),
                        "close_return": float(path_rows.iloc[-1]["close"] / entry_price - 1),
                        "first_200pct_hit_utc": hit.iloc[0]["open_time"] if not hit.empty else None,
                        "hit_200pct": bool(not hit.empty), "labeled_at_utc": as_of,
                    }
                )
    if not records:
        return 0
    combined = pd.concat([existing, pd.DataFrame(records)], ignore_index=True) if not existing.empty else pd.DataFrame(records)
    combined = combined.drop_duplicates(["signal_id", "horizon"], keep="first").sort_values(["signal_id", "horizon"])
    outcome_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(outcome_path, index=False)
    return len(records)


def build_snapshot(args: argparse.Namespace) -> tuple[dict[str, Any], pd.DataFrame]:
    as_of = _utc_timestamp(args.as_of)
    feature_date = as_of.floor("D") - pd.Timedelta(days=1)
    model_path = args.baseline_dir / "factor_model.json"
    model_payload = json.loads(model_path.read_text(encoding="utf-8"))
    model = FrozenLogisticModel.from_json(model_payload)
    model_hash = hashlib.sha256(model_path.read_bytes()).hexdigest()
    source_errors: list[str] = []

    if args.no_external_refresh:
        feature_rows, panel_4h, universe_hash = load_baseline_rows(args.baseline_dir, feature_date)
        universe_count = int(feature_rows["symbol"].nunique())
    else:
        universe, universe_hash = current_universe()
        panel_4h, source_errors = fetch_live_panel(universe, feature_date)
        daily = engineer_daily_features(panel_4h)
        feature_rows = daily[daily["date"] == feature_date].copy()
        universe_count = int(len(universe))

    latest_bar = panel_4h["open_time"].max() if not panel_4h.empty else None
    quality = assess_quality(
        rows=feature_rows,
        universe_count=universe_count,
        previous_count=previous_universe_count(
            args.output_root, before_date=as_of.date().isoformat()
        ),
        feature_date=feature_date,
        latest_bar=latest_bar,
        source_errors=source_errors,
    )
    candidates: list[dict[str, Any]] = []
    universe_rows: list[dict[str, Any]] = []
    if quality["passed"]:
        valid = feature_rows[feature_rows.loc[:, list(PRICE_FACTORS)].notna().all(axis=1)].copy()
        scored = score_cross_section(valid, model)
        runups = scan_30d_runups(panel_4h, end_time=feature_date + pd.Timedelta(hours=20))
        recent_runups = set(runups["symbol"].astype(str)) if not runups.empty else set()
        data_cutoff = as_of
        for _, row in scored.iterrows():
            signal_id = stable_hash(
                {"as_of_date": as_of.date().isoformat(), "symbol": row["symbol"], "model_hash": model_hash}
            )[:24]
            universe_rows.append(
                {
                    "signal_id": signal_id,
                    "data_cutoff_utc": data_cutoff,
                    "model_version": "binance-runup-price-v1-frozen-2026-07-18",
                    "model_hash": model_hash,
                    "universe_hash": universe_hash,
                    "symbol": str(row["symbol"]),
                    "score": float(row["score"]),
                    "score_pctile": float(row["score_pctile"]),
                    "price_factors": {factor: float(row[factor]) for factor in PRICE_FACTORS},
                    "oi_annotation": "missing",
                    "oi_detail": {"oi_affects_rank": False, "reason": "candidate_annotations_only"},
                    "stage": classify_stage(row, recent_runups),
                    "data_quality": "passed",
                    "source_timestamps": {
                        "feature_date_utc": feature_date,
                        "latest_4h_bar_utc": latest_bar,
                        "snapshot_as_of_utc": as_of,
                    },
                }
            )
        candidate_ids = [
            item["signal_id"]
            for item in sorted(universe_rows, key=lambda item: item["score"], reverse=True)
            if item["score_pctile"] >= 0.98
        ][:3]
        by_id = {item["signal_id"]: item for item in universe_rows}
        for item in sorted(
            (by_id[signal_id] for signal_id in candidate_ids),
            key=lambda record: record["score"],
            reverse=True,
        ):
            row = scored[scored["symbol"] == item["symbol"]].iloc[0]
            oi_label, oi_detail = (
                ("missing", {"oi_affects_rank": False, "reason": "external_refresh_disabled"})
                if args.no_external_refresh
                else (
                    annotate_oi(str(row["symbol"]), float(row["close"]))
                    if abs(pd.Timestamp.now(tz="UTC") - as_of) <= pd.Timedelta(hours=6)
                    else ("missing", {"oi_affects_rank": False, "reason": "historical_as_of_current_oi_unavailable"})
                )
            )
            item["oi_annotation"] = oi_label
            item["oi_detail"] = oi_detail
            candidates.append(dict(item))
    payload = {
        "schema_version": "1.0",
        "as_of_utc": as_of,
        "timezone_schedule": "Asia/Shanghai 08:20",
        "mode": "baseline_cache" if args.no_external_refresh else "live_public_data",
        "read_only_no_order_routing": True,
        "model": {"version": "binance-runup-price-v1-frozen-2026-07-18", "hash": model_hash, "factors": list(model.factors)},
        "universe_hash": universe_hash,
        "audit": quality,
        "universe_rows": universe_rows,
        "candidates": candidates,
    }
    return payload, panel_4h


def main() -> None:
    args = parse_args()
    payload, panel_4h = build_snapshot(args)
    path = args.output_root / "snapshots" / f"{pd.Timestamp(payload['as_of_utc']).date().isoformat()}.json"
    if args.dry_run:
        print(json.dumps(json_ready({"snapshot_path": str(path), **payload}), ensure_ascii=False, indent=2))
        return
    status = immutable_write_json(path, payload)
    appended = append_outcomes(args.output_root, panel_4h, pd.Timestamp(payload["as_of_utc"]))
    print(
        json.dumps(
            json_ready(
                {
                    "status": status,
                    "snapshot": str(path),
                    "quality_passed": payload["audit"]["passed"],
                    "candidates": len(payload["candidates"]),
                    "outcomes_appended": appended,
                }
            ),
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
