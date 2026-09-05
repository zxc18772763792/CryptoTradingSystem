"""Immutable, read-only launch-time microstructure collector for Binance signals.

The collector is deliberately rank-neutral: it observes only already-confirmed
24h launch signals, never changes the frozen price ranking, and has no order
routing.  A capture more than 15 minutes after the decision clock fails closed
and may never be backfilled as an on-time observation.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.research.binance_forward_microstructure import (
    latest_and_changes,
    orderbook_metrics,
    premium_metrics,
    taker_flow,
    utc,
    validate_capture_clock,
)


DEFAULT_OUTPUT = ROOT / "data" / "research" / "binance_200pct_forward_monitor"
RULE = {
    "version": "launch-microstructure-v1-frozen-2026-07-19",
    "capture_maximum_lag_minutes": 15,
    "required_orderbook_levels_each_side": 100,
    "maximum_realtime_source_age_seconds": 60,
    "maximum_history_source_age_minutes": 10,
    "ranking_effect": False,
    "annotation_only": True,
    "automatic_trading_allowed": False,
}


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


FORWARD = _load(
    "binance_forward_monitor_for_microstructure",
    ROOT / "scripts" / "binance_runup_forward_monitor.py",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", help="ISO-8601 capture time; a date means 12:05 Asia/Shanghai")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--maximum-lag-minutes", type=int, default=15)
    parser.add_argument("--no-external-refresh", action="store_true")
    return parser.parse_args()


def as_of_timestamp(value: str | None) -> pd.Timestamp:
    if value is None:
        return pd.Timestamp.now(tz="UTC")
    if len(value.strip()) == 10:
        return pd.Timestamp(f"{value.strip()} 12:05:00", tz="Asia/Shanghai").tz_convert("UTC")
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize("Asia/Shanghai")
    return stamp.tz_convert("UTC")


def load_launch_observations(output_root: Path) -> list[dict[str, Any]]:
    folder = output_root / "sequence_24h"
    observations: list[dict[str, Any]] = []
    for path in sorted(folder.glob("*.json")) if folder.exists() else []:
        item = json.loads(path.read_text(encoding="utf-8"))
        if item.get("launch_status") == "launch_confirmed":
            observations.append(item)
    return observations


def fetch_bundle(symbol: str, decision_time: pd.Timestamp) -> dict[str, Any]:
    end_time = int(utc(decision_time).timestamp() * 1000)
    five_minute_limit = 289
    requests = {
        "depth": ("/fapi/v1/depth", {"symbol": symbol, "limit": 500}),
        "premium": ("/fapi/v1/premiumIndex", {"symbol": symbol}),
        "current_oi": ("/fapi/v1/openInterest", {"symbol": symbol}),
        "oi_history": (
            "/futures/data/openInterestHist",
            {"symbol": symbol, "period": "5m", "endTime": end_time, "limit": five_minute_limit},
        ),
        "global_ratio": (
            "/futures/data/globalLongShortAccountRatio",
            {"symbol": symbol, "period": "5m", "endTime": end_time, "limit": five_minute_limit},
        ),
        "top_position_ratio": (
            "/futures/data/topLongShortPositionRatio",
            {"symbol": symbol, "period": "5m", "endTime": end_time, "limit": five_minute_limit},
        ),
        "taker": (
            "/futures/data/takerlongshortRatio",
            {"symbol": symbol, "period": "5m", "endTime": end_time, "limit": five_minute_limit},
        ),
    }
    bundle: dict[str, Any] = {}
    errors: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=len(requests)) as pool:
        futures = {
            pool.submit(FORWARD._request_json, path, params): name
            for name, (path, params) in requests.items()
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                bundle[name] = future.result()
            except Exception as exc:  # noqa: BLE001 - retained in the audit record
                errors[name] = f"{type(exc).__name__}: {exc}"
    bundle["source_errors"] = errors
    return bundle


def _age_seconds(source_time: Any, capture_time: pd.Timestamp) -> float | None:
    if source_time is None:
        return None
    return float((utc(capture_time) - utc(source_time)).total_seconds())


def assemble_snapshot(
    observation: dict[str, Any],
    bundle: dict[str, Any],
    *,
    capture_time: pd.Timestamp,
    maximum_lag_minutes: int = 15,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    decision_time = utc(observation["decision_time_utc"])
    capture = utc(capture_time)
    clock = validate_capture_clock(
        decision_time=decision_time,
        capture_time=capture,
        maximum_lag_minutes=maximum_lag_minutes,
    )
    failures: list[str] = []
    if not clock["passed"]:
        failures.append("capture_clock_outside_allowed_window")
    source_errors = dict(bundle.get("source_errors") or {})
    required = {"depth", "premium", "current_oi", "oi_history", "taker"}
    for name in sorted(required):
        if name not in bundle:
            failures.append(f"missing_required_source:{name}")

    features: dict[str, Any] = {}
    source_timestamps: dict[str, Any] = {}
    if "depth" in bundle:
        try:
            book = orderbook_metrics(bundle["depth"])
            features["orderbook"] = book
            book_time = book.get("transaction_time_utc") or book.get("event_time_utc")
            source_timestamps["orderbook_utc"] = book_time
            if min(int(book["levels_bid"]), int(book["levels_ask"])) < RULE["required_orderbook_levels_each_side"]:
                failures.append("insufficient_orderbook_levels")
            age = _age_seconds(book_time, capture)
            if age is None or not (-5 <= age <= RULE["maximum_realtime_source_age_seconds"]):
                failures.append("stale_or_future_orderbook")
        except Exception as exc:  # noqa: BLE001
            failures.append(f"invalid_orderbook:{type(exc).__name__}")
    if "premium" in bundle:
        premium = premium_metrics(bundle["premium"])
        features["premium"] = premium
        source_timestamps["premium_utc"] = premium.get("source_time_utc")
        age = _age_seconds(premium.get("source_time_utc"), capture)
        if age is None or not (-5 <= age <= RULE["maximum_realtime_source_age_seconds"]):
            failures.append("stale_or_future_premium")
    if "current_oi" in bundle:
        current_oi = bundle["current_oi"]
        oi_time = None if current_oi.get("time") is None else pd.to_datetime(current_oi["time"], unit="ms", utc=True)
        try:
            oi_units = float(current_oi["openInterest"])
        except (KeyError, TypeError, ValueError):
            oi_units = None
            failures.append("invalid_current_oi")
        mark = features.get("premium", {}).get("mark_price")
        features["current_open_interest"] = {
            "units": oi_units,
            "usd_at_mark": None if oi_units is None or mark is None else float(oi_units * mark),
            "source_time_utc": oi_time,
        }
        source_timestamps["current_open_interest_utc"] = oi_time
        age = _age_seconds(oi_time, capture)
        if age is None or not (-5 <= age <= RULE["maximum_realtime_source_age_seconds"]):
            failures.append("stale_or_future_current_oi")
    if "oi_history" in bundle:
        oi = latest_and_changes(
            bundle["oi_history"], cutoff=decision_time, value_key="sumOpenInterestValue"
        )
        features["open_interest_history"] = oi
        source_timestamps["open_interest_history_utc"] = oi.get("latest_time_utc")
        age = _age_seconds(oi.get("latest_time_utc"), decision_time)
        if age is None or not (0 <= age <= RULE["maximum_history_source_age_minutes"] * 60):
            failures.append("stale_or_future_oi_history")
    if "global_ratio" in bundle:
        features["global_long_short_accounts"] = latest_and_changes(
            bundle["global_ratio"], cutoff=decision_time, value_key="longShortRatio"
        )
        source_timestamps["global_long_short_accounts_utc"] = features["global_long_short_accounts"].get("latest_time_utc")
    if "top_position_ratio" in bundle:
        features["top_trader_position_ratio"] = latest_and_changes(
            bundle["top_position_ratio"], cutoff=decision_time, value_key="longShortRatio"
        )
        source_timestamps["top_trader_position_ratio_utc"] = features["top_trader_position_ratio"].get("latest_time_utc")
    if "taker" in bundle:
        taker = taker_flow(bundle["taker"], cutoff=decision_time)
        features["taker_flow"] = taker
        source_timestamps["taker_flow_utc"] = taker.get("latest_time_utc")
        age = _age_seconds(taker.get("latest_time_utc"), decision_time)
        if age is None or not (0 <= age <= RULE["maximum_history_source_age_minutes"] * 60):
            failures.append("stale_or_future_taker_flow")

    failures.extend(f"source_error:{name}" for name in sorted(source_errors))
    quality = {
        "passed": not failures,
        "fail_closed": True,
        "failures": failures,
        "source_errors": source_errors,
        "required_sources": sorted(required),
    }
    audit = {
        "signal_id": str(observation["signal_id"]),
        "symbol": str(observation["symbol"]),
        "decision_time_utc": decision_time,
        "capture_time_utc": capture,
        "capture_clock": clock,
        "data_quality": quality,
    }
    if failures:
        return None, audit
    rule = {**RULE, "capture_maximum_lag_minutes": int(maximum_lag_minutes)}
    rule_hash = hashlib.sha256(json.dumps(rule, sort_keys=True).encode("utf-8")).hexdigest()
    snapshot = {
        "schema_version": "1.0",
        "signal_id": str(observation["signal_id"]),
        "snapshot_date": str(observation["snapshot_date"]),
        "symbol": str(observation["symbol"]),
        "original_stage": str(observation["original_stage"]),
        "original_score_pctile": float(observation["original_score_pctile"]),
        "paper_eligible_after_original_stage": bool(observation["paper_eligible_after_original_stage"]),
        "decision_time_utc": decision_time,
        "capture_time_utc": capture,
        "capture_lag_seconds": clock["capture_lag_seconds"],
        "features": features,
        "source_timestamps": source_timestamps,
        "data_quality": quality,
        "rule": rule,
        "rule_hash": rule_hash,
        "ranking_changed": False,
        "annotation_only": True,
        "automatic_trading_allowed": False,
        "read_only_no_order_routing": True,
    }
    return snapshot, audit


def immutable_write(path: Path, payload: dict[str, Any]) -> str:
    encoded = json.dumps(FORWARD.json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != encoded:
            raise RuntimeError(f"Immutable record exists with different content: {path}")
        return "idempotent_existing"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(encoded, encoding="utf-8")
    return "created"


def main() -> None:
    args = parse_args()
    output = args.output_root.resolve()
    capture = as_of_timestamp(args.as_of)
    existing = {path.stem for path in (output / "launch_microstructure").glob("*.json")} if (output / "launch_microstructure").exists() else set()
    terminal_stale_ids: set[str] = set()
    failed_folder = output / "launch_microstructure_failed"
    for path in sorted(failed_folder.glob("*.json")) if failed_folder.exists() else []:
        try:
            failure = json.loads(path.read_text(encoding="utf-8"))
            if failure.get("capture_clock", {}).get("passed") is False:
                terminal_stale_ids.add(str(failure["signal_id"]))
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            continue
    observations = [
        item for item in load_launch_observations(output)
        if str(item["signal_id"]) not in existing and str(item["signal_id"]) not in terminal_stale_ids
    ]
    pending: list[dict[str, Any]] = []
    due: list[dict[str, Any]] = []
    stale: list[dict[str, Any]] = []
    for item in observations:
        clock = validate_capture_clock(
            decision_time=utc(item["decision_time_utc"]),
            capture_time=capture,
            maximum_lag_minutes=args.maximum_lag_minutes,
        )
        if clock["capture_lag_seconds"] < 0:
            pending.append(item)
        elif clock["passed"]:
            due.append(item)
        else:
            stale.append(item)

    bundles: dict[str, dict[str, Any]] = {}
    if not args.no_external_refresh:
        for item in due:
            bundles[str(item["signal_id"])] = fetch_bundle(str(item["symbol"]), utc(item["decision_time_utc"]))

    successes: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for item in stale:
        clock = validate_capture_clock(
            decision_time=utc(item["decision_time_utc"]),
            capture_time=capture,
            maximum_lag_minutes=args.maximum_lag_minutes,
        )
        failures.append(
            {
                "signal_id": str(item["signal_id"]),
                "symbol": str(item["symbol"]),
                "decision_time_utc": utc(item["decision_time_utc"]),
                "capture_time_utc": capture,
                "capture_clock": clock,
                "data_quality": {
                    "passed": False,
                    "fail_closed": True,
                    "failures": ["capture_clock_outside_allowed_window"],
                },
            }
        )
    for item in due:
        if args.no_external_refresh:
            failures.append(
                {
                    "signal_id": str(item["signal_id"]),
                    "symbol": str(item["symbol"]),
                    "decision_time_utc": utc(item["decision_time_utc"]),
                    "capture_time_utc": capture,
                    "data_quality": {
                        "passed": False,
                        "fail_closed": True,
                        "failures": ["external_refresh_disabled"],
                    },
                }
            )
            continue
        snapshot, audit = assemble_snapshot(
            item,
            bundles[str(item["signal_id"])],
            capture_time=capture,
            maximum_lag_minutes=args.maximum_lag_minutes,
        )
        if snapshot is None:
            failures.append(audit)
        else:
            successes.append(snapshot)

    run_audit = {
        "as_of_utc": capture,
        "launch_confirmations_without_snapshot": len(observations),
        "terminal_stale_previously_audited": len(terminal_stale_ids),
        "pending_count": len(pending),
        "due_count": len(due),
        "stale_count": len(stale),
        "successful_snapshots": len(successes),
        "failed_count": len(failures),
        "failures": failures,
        "ranking_changed": False,
        "read_only_no_order_routing": True,
    }
    if args.dry_run:
        run_audit["snapshots"] = successes
        print(json.dumps(FORWARD.json_ready(run_audit), ensure_ascii=False, indent=2))
        return

    statuses = []
    for item in successes:
        statuses.append(
            {
                "signal_id": item["signal_id"],
                "status": immutable_write(output / "launch_microstructure" / f"{item['signal_id']}.json", item),
            }
        )
    capture_slug = capture.strftime("%Y%m%dT%H%M%SZ")
    for item in failures:
        signal_id = str(item["signal_id"])
        statuses.append(
            {
                "signal_id": signal_id,
                "status": immutable_write(
                    output / "launch_microstructure_failed" / f"{signal_id}-{capture_slug}.json",
                    item,
                ),
            }
        )
    run_audit["statuses"] = statuses
    print(json.dumps(FORWARD.json_ready(run_audit), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
