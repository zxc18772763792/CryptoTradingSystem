"""Append immutable 30-day outcome and exit-policy labels to launch signals.

This research labeler runs only after a complete 30-day 4h path exists.  It
does not change signal ranks and cannot place orders.  The historical baseline
exit remains frozen; a close-confirmed stop is recorded only as a prospective
counterfactual.
"""

from __future__ import annotations

import argparse
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

from core.research.binance_entry_exit_validation import simulate_stateful_trade
from core.research.binance_hybrid_stop_validation import simulate_hybrid_stop_trade


DEFAULT_OUTPUT = ROOT / "data" / "research" / "binance_200pct_forward_monitor"
HORIZON_DAYS = 30
FROZEN_POLICY = {
    "name": "baseline_hard25_be30_half100_trail25",
    "family": "forward_frozen_historical_challenger",
    "hard_stop": 0.25,
    "time_days": 30,
    "partial_target": 1.00,
    "partial_fraction": 0.50,
    "trail_activation": 1.00,
    "trail_distance": 0.25,
    "breakeven_activation": 0.30,
    "launch_deadline_days": None,
    "launch_mfe_required": None,
    "momentum_activation": None,
    "momentum_drawdown": None,
}
PROSPECTIVE_COUNTERFACTUAL = {
    "name": "cat40_close25x1_be30_half100_trail25",
    "family": "prospective_counterfactual_not_selected_historically",
    "time_days": 30,
    "catastrophic_stop": 0.40,
    "close_stop": 0.25,
    "close_stop_bars": 1,
    "breakeven_activation": 0.30,
    "partial_target": 1.00,
    "partial_fraction": 0.50,
    "trail_activation": 1.00,
    "trail_distance": 0.25,
}
PROSPECTIVE_TAIL_COUNTERFACTUAL = {
    "name": "hard25_be30_half150_trail30",
    "family": "prospective_right_tail_counterfactual_not_promoted",
    "hard_stop": 0.25,
    "time_days": 30,
    "partial_target": 1.50,
    "partial_fraction": 0.50,
    "trail_activation": 1.50,
    "trail_distance": 0.30,
    "breakeven_activation": 0.30,
    "launch_deadline_days": None,
    "launch_mfe_required": None,
    "momentum_activation": None,
    "momentum_drawdown": None,
}
PROSPECTIVE_EXHAUSTION_COUNTERFACTUAL = {
    "name": "hard25_be30_half150_trail35_wick",
    "family": "prospective_profit_exhaustion_counterfactual_not_promoted",
    "hard_stop": 0.25,
    "time_days": 30,
    "partial_target": 1.50,
    "partial_fraction": 0.50,
    "trail_activation": 1.50,
    "trail_distance": 0.35,
    "breakeven_activation": 0.30,
    "launch_deadline_days": None,
    "launch_mfe_required": None,
    "momentum_activation": None,
    "momentum_drawdown": None,
    "exhaustion_activation": 1.00,
    "exhaustion_upper_wick_min": 0.35,
    "exhaustion_close_location_max": 0.35,
    "exhaustion_volume_ratio_min": 1.50,
}
PROSPECTIVE_EXHAUSTION_TAIL30_COUNTERFACTUAL = {
    "name": "hard25_be30_half150_trail30_wick_posthoc",
    "family": "prospective_posthoc_factorial_counterfactual_not_promoted",
    "hard_stop": 0.25,
    "time_days": 30,
    "partial_target": 1.50,
    "partial_fraction": 0.50,
    "trail_activation": 1.50,
    "trail_distance": 0.30,
    "breakeven_activation": 0.30,
    "launch_deadline_days": None,
    "launch_mfe_required": None,
    "momentum_activation": None,
    "momentum_drawdown": None,
    "exhaustion_activation": 1.00,
    "exhaustion_upper_wick_min": 0.35,
    "exhaustion_close_location_max": 0.35,
    "exhaustion_volume_ratio_min": 1.50,
}
UTILITY_ENTRY_TIMING_COUNTERFACTUAL = {
    "name": "discount5_reclaim_24h_next_open",
    "family": "prospective_utility_entry_filter_not_promoted",
    "rule": "discount5_reclaim",
    "trigger_window_hours": 24,
    "description": "after the 8h score open, touch -5%, then require a positive bar and higher close; enter next 4h open",
}
UTILITY_BREAKOUT_HOLD_COUNTERFACTUAL = {
    "name": "breakout6_half150_trail30_wick_24h_next_open",
    "family": "prospective_breakout_hold_router_not_promoted",
    "rule": "breakout6",
    "trigger_window_hours": 24,
    "description": "keep the frozen 8h entry; after a completed six-bar breakout, switch profit-side management at the next 4h open",
    "post_switch_policy": PROSPECTIVE_EXHAUSTION_TAIL30_COUNTERFACTUAL,
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
    "binance_forward_monitor_for_strategy_labeler",
    ROOT / "scripts" / "binance_runup_forward_monitor.py",
)
SEQUENCE = _load(
    "binance_sequence_monitor_for_strategy_labeler",
    ROOT / "scripts" / "binance_sequence_forward_monitor.py",
)
POST = _load(
    "binance_postlaunch_validation_for_forward_strategy_labeler",
    ROOT / "core" / "research" / "binance_postlaunch_validation.py",
)


def utc(value: Any) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    return stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", help="ISO-8601 cutoff; a date means 12:20 Asia/Shanghai")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--no-external-refresh", action="store_true")
    return parser.parse_args()


def as_of_timestamp(value: str | None) -> pd.Timestamp:
    if value is None:
        return pd.Timestamp.now(tz="UTC")
    if len(value.strip()) == 10:
        return pd.Timestamp(f"{value.strip()} 12:20:00", tz="Asia/Shanghai").tz_convert("UTC")
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize("Asia/Shanghai")
    return stamp.tz_convert("UTC")


def load_confirmed(output_root: Path) -> list[dict[str, Any]]:
    folder = output_root / "sequence_24h"
    rows = []
    for path in sorted(folder.glob("*.json")) if folder.exists() else []:
        item = json.loads(path.read_text(encoding="utf-8"))
        if item.get("launch_status") == "launch_confirmed":
            rows.append(item)
    return rows


def fetch_path(item: dict[str, Any]) -> dict[str, Any]:
    symbol = str(item["symbol"])
    decision = utc(item["decision_time_utc"])
    horizon = decision + pd.Timedelta(days=HORIZON_DAYS)
    try:
        raw_bars = FORWARD._request_json(
            "/fapi/v1/klines",
            {
                "symbol": symbol,
                "interval": "4h",
                "startTime": int(decision.timestamp() * 1000),
                "endTime": int(horizon.timestamp() * 1000),
                "limit": 300,
            },
        )
        bars = FORWARD._normalize_klines(raw_bars, symbol, symbol.removesuffix("USDT"))
        raw_funding = FORWARD._request_json(
            "/fapi/v1/fundingRate",
            {
                "symbol": symbol,
                "startTime": int(decision.timestamp() * 1000),
                "endTime": int((horizon + pd.Timedelta(hours=4)).timestamp() * 1000),
                "limit": 1000,
            },
        )
        return {"bars": bars, "funding": raw_funding, "error": None}
    except Exception as exc:  # noqa: BLE001 - preserved in immutable failure audit
        return {"bars": pd.DataFrame(), "funding": [], "error": f"{type(exc).__name__}: {exc}"}


def funding_series(records: list[dict[str, Any]]) -> pd.Series:
    if not records:
        return pd.Series(dtype=float)
    values = {
        pd.to_datetime(item["fundingTime"], unit="ms", utc=True): float(item["fundingRate"])
        for item in records
        if item.get("fundingTime") is not None and item.get("fundingRate") is not None
    }
    return pd.Series(values, dtype=float).sort_index()


def _first_hit(path: pd.DataFrame, entry: float, target_return: float) -> float | None:
    hits = path[pd.to_numeric(path["high"], errors="coerce") >= entry * (1.0 + target_return)]
    if hits.empty:
        return None
    return float((pd.Timestamp(hits.iloc[0]["open_time"]) - pd.Timestamp(path.iloc[0]["open_time"])).total_seconds() / 3600.0)


def build_label(
    item: dict[str, Any],
    bars: pd.DataFrame,
    funding_records: list[dict[str, Any]],
    *,
    labeled_at: pd.Timestamp,
    include_utility_entry_counterfactual: bool = False,
    include_utility_hold_counterfactual: bool = False,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    decision = utc(item["decision_time_utc"])
    horizon = decision + pd.Timedelta(days=HORIZON_DAYS)
    path = bars.copy()
    if not path.empty:
        path["open_time"] = pd.to_datetime(path["open_time"], utc=True)
        path = path[(path["open_time"] >= decision) & (path["open_time"] <= horizon)].sort_values("open_time").reset_index(drop=True)
    expected = pd.date_range(decision, horizon, freq="4h")
    failures: list[str] = []
    if path.empty or list(path["open_time"]) != list(expected):
        failures.append("incomplete_or_noncontiguous_30d_4h_path")
    funding = funding_series(funding_records)
    if funding.empty:
        failures.append("missing_funding_history")
    else:
        if funding.index.min() > decision + pd.Timedelta(hours=12):
            failures.append("funding_history_starts_late")
        if funding.index.max() < horizon - pd.Timedelta(hours=12):
            failures.append("funding_history_ends_early")
    audit = {
        "signal_id": str(item["signal_id"]),
        "symbol": str(item["symbol"]),
        "decision_time_utc": decision,
        "horizon_end_utc": horizon,
        "labeled_at_utc": utc(labeled_at),
        "data_quality": {"passed": not failures, "fail_closed": True, "failures": failures},
    }
    if failures:
        return None, audit

    entry = float(path.iloc[0]["open"])
    first14 = path[path["open_time"] < decision + pd.Timedelta(days=14)]
    first30 = path[path["open_time"] < horizon]
    frozen = simulate_stateful_trade(
        path,
        entry_time=decision,
        policy=FROZEN_POLICY,
        slippage_bps=5.0,
        funding=funding,
        fee_bps=5.0,
    )
    counterfactual = simulate_hybrid_stop_trade(
        path,
        entry_time=decision,
        policy=PROSPECTIVE_COUNTERFACTUAL,
        slippage_bps=5.0,
        funding=funding,
        fee_bps=5.0,
    )
    tail_counterfactual = simulate_stateful_trade(
        path,
        entry_time=decision,
        policy=PROSPECTIVE_TAIL_COUNTERFACTUAL,
        slippage_bps=5.0,
        funding=funding,
        fee_bps=5.0,
    )
    exhaustion_counterfactual = simulate_stateful_trade(
        path,
        entry_time=decision,
        policy=PROSPECTIVE_EXHAUSTION_COUNTERFACTUAL,
        slippage_bps=5.0,
        funding=funding,
        fee_bps=5.0,
    )
    exhaustion_tail30_counterfactual = simulate_stateful_trade(
        path,
        entry_time=decision,
        policy=PROSPECTIVE_EXHAUSTION_TAIL30_COUNTERFACTUAL,
        slippage_bps=5.0,
        funding=funding,
        fee_bps=5.0,
    )
    entry_counterfactuals: dict[str, Any] = {}
    if include_utility_entry_counterfactual:
        located = POST.locate_postlaunch_entry(
            {
                "decision_time": decision,
                "decision_open": entry,
                "baseline_entry_open": entry,
                "early_mfe": 0.0,
            },
            path,
            rule_name=str(UTILITY_ENTRY_TIMING_COUNTERFACTUAL["rule"]),
            trigger_window_hours=int(UTILITY_ENTRY_TIMING_COUNTERFACTUAL["trigger_window_hours"]),
        )
        entry_counterfactuals[UTILITY_ENTRY_TIMING_COUNTERFACTUAL["name"]] = {
            "family": UTILITY_ENTRY_TIMING_COUNTERFACTUAL["family"],
            "description": UTILITY_ENTRY_TIMING_COUNTERFACTUAL["description"],
            "triggered": located is not None,
            "trigger_time": None if located is None else located["trigger_time"],
            "entry_time": None if located is None else located["entry_time"],
            "entry_open": None if located is None else located["entry_open"],
            "entry_delay_hours": None if located is None else located["entry_delay_hours_after_launch"],
            "entry_return_vs_score_open": None if located is None else located["entry_return_vs_decision_open"],
            "future_max_return_14d_from_entry": None if located is None else located["future_max_return_14d_from_entry"],
            "target200_14d_from_entry": None if located is None else located["target200_14d_from_entry"],
            "exit_outcome": None if located is None else simulate_stateful_trade(
                path,
                entry_time=pd.Timestamp(located["entry_time"]),
                policy=FROZEN_POLICY,
                slippage_bps=5.0,
                funding=funding,
                fee_bps=5.0,
            ),
            "historically_promoted": False,
            "ranking_changed": False,
            "automatic_trading_allowed": False,
        }
    hold_counterfactuals: dict[str, Any] = {}
    if include_utility_hold_counterfactual:
        located = POST.locate_postlaunch_entry(
            {
                "decision_time": decision,
                "decision_open": entry,
                "baseline_entry_open": entry,
                "early_mfe": 0.0,
            },
            path,
            rule_name=str(UTILITY_BREAKOUT_HOLD_COUNTERFACTUAL["rule"]),
            trigger_window_hours=int(UTILITY_BREAKOUT_HOLD_COUNTERFACTUAL["trigger_window_hours"]),
        )
        routed_outcome = simulate_stateful_trade(
            path,
            entry_time=decision,
            policy=FROZEN_POLICY,
            slippage_bps=5.0,
            funding=funding,
            fee_bps=5.0,
            policy_switch_time=None if located is None else pd.Timestamp(located["entry_time"]),
            post_switch_policy=(
                None if located is None
                else UTILITY_BREAKOUT_HOLD_COUNTERFACTUAL["post_switch_policy"]
            ),
        )
        hold_counterfactuals[UTILITY_BREAKOUT_HOLD_COUNTERFACTUAL["name"]] = {
            "family": UTILITY_BREAKOUT_HOLD_COUNTERFACTUAL["family"],
            "description": UTILITY_BREAKOUT_HOLD_COUNTERFACTUAL["description"],
            "triggered": located is not None,
            "trigger_time": None if located is None else located["trigger_time"],
            "policy_switch_time": None if located is None else located["entry_time"],
            "trigger_close_return": None if located is None else located["trigger_close_return"],
            "outcome": routed_outcome,
            "historically_promoted": False,
            "primary_exit_changed": False,
            "ranking_changed": False,
            "automatic_trading_allowed": False,
        }
    label = {
        "schema_version": "1.0",
        "signal_id": str(item["signal_id"]),
        "horizon": "30d",
        "snapshot_date": str(item["snapshot_date"]),
        "symbol": str(item["symbol"]),
        "original_stage": str(item["original_stage"]),
        "paper_eligible_after_original_stage": bool(item["paper_eligible_after_original_stage"]),
        "decision_time_utc": decision,
        "entry_reference_open": entry,
        "horizon_end_utc": horizon,
        "labeled_at_utc": utc(labeled_at),
        "path_labels": {
            "maximum_return_14d": float(first14["high"].max() / entry - 1.0),
            "minimum_return_14d": float(first14["low"].min() / entry - 1.0),
            "close_return_14d": float(first14.iloc[-1]["close"] / entry - 1.0),
            "target200_14d_from_decision": bool(first14["high"].max() / entry - 1.0 >= 2.0),
            "maximum_return_30d": float(first30["high"].max() / entry - 1.0),
            "minimum_return_30d": float(first30["low"].min() / entry - 1.0),
            "close_return_30d": float(first30.iloc[-1]["close"] / entry - 1.0),
            "hours_to_50pct": _first_hit(first30, entry, 0.50),
            "hours_to_100pct": _first_hit(first30, entry, 1.00),
            "hours_to_200pct": _first_hit(first30, entry, 2.00),
        },
        "exit_outcomes": {
            FROZEN_POLICY["name"]: frozen,
            PROSPECTIVE_COUNTERFACTUAL["name"]: counterfactual,
            PROSPECTIVE_TAIL_COUNTERFACTUAL["name"]: tail_counterfactual,
            PROSPECTIVE_EXHAUSTION_COUNTERFACTUAL["name"]: exhaustion_counterfactual,
            PROSPECTIVE_EXHAUSTION_TAIL30_COUNTERFACTUAL["name"]: exhaustion_tail30_counterfactual,
        },
        "entry_counterfactuals": entry_counterfactuals,
        "hold_counterfactuals": hold_counterfactuals,
        "cost_assumptions": {"fee_bps_each_side": 5.0, "slippage_bps_each_side": 5.0, "actual_funding_used": True},
        "microstructure_snapshot_present": False,
        "data_quality": audit["data_quality"],
        "ranking_changed": False,
        "automatic_trading_allowed": False,
        "read_only_no_order_routing": True,
    }
    return label, audit


def immutable_write(path: Path, payload: dict[str, Any]) -> str:
    encoded = json.dumps(FORWARD.json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != encoded:
            raise RuntimeError(f"Immutable label exists with different content: {path}")
        return "idempotent_existing"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(encoded, encoding="utf-8")
    return "created"


def main() -> None:
    args = parse_args()
    output = args.output_root.resolve()
    as_of = as_of_timestamp(args.as_of)
    existing = {path.stem.removesuffix("-30d") for path in (output / "strategy_30d_labels").glob("*-30d.json")} if (output / "strategy_30d_labels").exists() else set()
    items = [item for item in load_confirmed(output) if str(item["signal_id"]) not in existing]
    due = [item for item in items if as_of >= utc(item["decision_time_utc"]) + pd.Timedelta(days=30, hours=4)]
    pending = [item for item in items if item not in due]
    sources: dict[str, dict[str, Any]] = {}
    if not args.no_external_refresh:
        with ThreadPoolExecutor(max_workers=min(6, max(1, len(due)))) as pool:
            futures = {pool.submit(fetch_path, item): str(item["signal_id"]) for item in due}
            for future in as_completed(futures):
                sources[futures[future]] = future.result()
    labels: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for item in due:
        signal_id = str(item["signal_id"])
        if args.no_external_refresh:
            failures.append({"signal_id": signal_id, "symbol": item["symbol"], "data_quality": {"passed": False, "fail_closed": True, "failures": ["external_refresh_disabled"]}})
            continue
        source = sources[signal_id]
        if source["error"]:
            failures.append({"signal_id": signal_id, "symbol": item["symbol"], "data_quality": {"passed": False, "fail_closed": True, "failures": [source["error"]]}})
            continue
        label, audit = build_label(item, source["bars"], source["funding"], labeled_at=as_of)
        if label is None:
            failures.append(audit)
        else:
            label["microstructure_snapshot_present"] = (output / "launch_microstructure" / f"{signal_id}.json").exists()
            labels.append(label)
    audit = {
        "as_of_utc": as_of,
        "unlabeled_confirmed_signals": len(items),
        "due_count": len(due),
        "pending_count": len(pending),
        "labels_ready": len(labels),
        "failed_count": len(failures),
        "failures": failures,
        "ranking_changed": False,
        "read_only_no_order_routing": True,
    }
    if args.dry_run:
        audit["labels"] = labels
        print(json.dumps(FORWARD.json_ready(audit), ensure_ascii=False, indent=2))
        return
    statuses = []
    for label in labels:
        statuses.append({"signal_id": label["signal_id"], "status": immutable_write(output / "strategy_30d_labels" / f"{label['signal_id']}-30d.json", label)})
    for failure in failures:
        slug = as_of.strftime("%Y%m%dT%H%M%SZ")
        statuses.append({"signal_id": failure["signal_id"], "status": immutable_write(output / "strategy_30d_label_failed" / f"{failure['signal_id']}-{slug}.json", failure)})
    audit["statuses"] = statuses
    print(json.dumps(FORWARD.json_ready(audit), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
