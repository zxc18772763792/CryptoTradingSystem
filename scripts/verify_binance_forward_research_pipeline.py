"""Independently verify the read-only prospective Binance research records."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "data" / "research" / "binance_200pct_forward_monitor"
SCRIPTS = [
    ROOT / "scripts" / "binance_sequence_forward_monitor.py",
    ROOT / "scripts" / "binance_launch_microstructure_forward_monitor.py",
    ROOT / "scripts" / "binance_forward_strategy_labeler.py",
    ROOT / "scripts" / "binance_continuation_utility_forward_monitor.py",
    ROOT / "scripts" / "binance_continuation_utility_labeler.py",
]
FORBIDDEN_IMPORT_TERMS = [
    "core.execution",
    "order_manager",
    "strategy_manager",
    "altcoin_radar",
    "create_order",
    "place_order",
]


def utc(value: Any) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    return stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")


def load_folder(name: str) -> list[dict[str, Any]]:
    folder = OUTPUT / name
    return [json.loads(path.read_text(encoding="utf-8")) for path in sorted(folder.glob("*.json"))] if folder.exists() else []


def main() -> None:
    checks: dict[str, bool] = {}
    errors: list[str] = []
    script_text = "\n".join(path.read_text(encoding="utf-8") for path in SCRIPTS)
    checks["all_collectors_exist"] = all(path.exists() for path in SCRIPTS)
    checks["no_execution_import_or_order_call"] = not any(term in script_text for term in FORBIDDEN_IMPORT_TERMS)
    checks["required_cli_controls"] = all(
        token in path.read_text(encoding="utf-8")
        for path in SCRIPTS
        for token in ("--as-of", "--dry-run", "--output-root", "--no-external-refresh")
    )

    sequence = load_folder("sequence_24h")
    sequence_ids = [str(item.get("signal_id")) for item in sequence]
    checks["sequence_signal_ids_unique"] = len(sequence_ids) == len(set(sequence_ids))
    checks["sequence_is_rank_neutral"] = all(
        item.get("ranking_changed") is False and item.get("automatic_trading_allowed") is False
        for item in sequence
    )
    checks["sequence_clock_consistent"] = all(
        utc(item["decision_time_utc"]) - utc(item["entry_time_utc"]) == pd.Timedelta(hours=24)
        for item in sequence
    )

    captures = load_folder("launch_microstructure")
    capture_ids = [str(item.get("signal_id")) for item in captures]
    checks["microstructure_signal_ids_unique"] = len(capture_ids) == len(set(capture_ids))
    checks["microstructure_quality_and_clock"] = all(
        item.get("data_quality", {}).get("passed") is True
        and 0 <= float(item["capture_lag_seconds"]) <= 15 * 60
        and item.get("ranking_changed") is False
        and item.get("automatic_trading_allowed") is False
        and item.get("entry_counterfactuals", {}) == {}
        and item.get("hold_counterfactuals", {}) == {}
        and item.get("read_only_no_order_routing") is True
        for item in captures
    )
    checks["microstructure_sources_are_past_only"] = all(
        all(
            value is None or utc(value) <= utc(item["capture_time_utc"]) + pd.Timedelta(seconds=5)
            for value in item.get("source_timestamps", {}).values()
        )
        for item in captures
    )

    failed = load_folder("launch_microstructure_failed")
    checks["failed_captures_are_fail_closed"] = all(
        item.get("data_quality", {}).get("passed") is False
        and item.get("data_quality", {}).get("fail_closed") is True
        for item in failed
    )
    terminal_ids = {
        str(item["signal_id"])
        for item in failed
        if item.get("capture_clock", {}).get("passed") is False
    }
    checks["terminal_stale_not_backfilled"] = not (terminal_ids & set(capture_ids))

    labels = load_folder("strategy_30d_labels")
    label_keys = [(str(item.get("signal_id")), str(item.get("horizon"))) for item in labels]
    checks["strategy_label_keys_unique"] = len(label_keys) == len(set(label_keys))
    checks["strategy_labels_complete_and_read_only"] = all(
        item.get("horizon") == "30d"
        and utc(item["horizon_end_utc"]) - utc(item["decision_time_utc"]) == pd.Timedelta(days=30)
        and item.get("data_quality", {}).get("passed") is True
        and item.get("ranking_changed") is False
        and item.get("automatic_trading_allowed") is False
        and item.get("entry_counterfactuals", {}) == {}
        and item.get("hold_counterfactuals", {}) == {}
        and set(item.get("exit_outcomes", {}))
        == {
            "baseline_hard25_be30_half100_trail25",
            "cat40_close25x1_be30_half100_trail25",
            "hard25_be30_half150_trail30",
            "hard25_be30_half150_trail35_wick",
            "hard25_be30_half150_trail30_wick_posthoc",
        }
        for item in labels
    )

    utility = load_folder("continuation_utility_8h")
    utility_ids = [str(item.get("signal_id")) for item in utility]
    checks["utility_signal_ids_unique"] = len(utility_ids) == len(set(utility_ids))
    checks["utility_observations_are_frozen_read_only"] = all(
        item.get("model_version") == "continuation-utility-8h-v1-frozen-2026-07-19"
        and 0 <= float(item["capture_lag_seconds"]) <= 15 * 60
        and item.get("primary_ranking_changed") is False
        and item.get("automatic_trading_allowed") is False
        and item.get("read_only_no_order_routing") is True
        and (not item.get("paper_eligible_after_original_stage") or item.get("original_stage") == "price_watch")
        for item in utility
    )
    utility_failed = load_folder("continuation_utility_8h_failed")
    utility_terminal = {str(item["signal_id"]) for item in utility_failed if item.get("terminal") is True}
    checks["utility_failures_are_terminal_and_not_backfilled"] = all(
        item.get("fail_closed") is True for item in utility_failed
    ) and not (utility_terminal & set(utility_ids))
    utility_labels = load_folder("continuation_utility_30d_labels")
    utility_label_keys = [(str(item.get("signal_id")), str(item.get("horizon"))) for item in utility_labels]
    checks["utility_label_keys_unique"] = len(utility_label_keys) == len(set(utility_label_keys))
    checks["utility_labels_preserve_model_and_rank"] = all(
        item.get("label_family") == "continuation_utility_8h"
        and item.get("primary_ranking_changed") is False
        and item.get("automatic_trading_allowed") is False
        and set(item.get("entry_counterfactuals", {})) == {"discount5_reclaim_24h_next_open"}
        and set(item.get("hold_counterfactuals", {})) == {"breakout6_half150_trail30_wick_24h_next_open"}
        and all(
            value.get("historically_promoted") is False
            and value.get("ranking_changed") is False
            and value.get("automatic_trading_allowed") is False
            for value in item.get("entry_counterfactuals", {}).values()
        )
        and all(
            value.get("historically_promoted") is False
            and value.get("primary_exit_changed") is False
            and value.get("ranking_changed") is False
            and value.get("automatic_trading_allowed") is False
            for value in item.get("hold_counterfactuals", {}).values()
        )
        and set(item.get("exit_outcomes", {}))
        == {
            "baseline_hard25_be30_half100_trail25",
            "cat40_close25x1_be30_half100_trail25",
            "hard25_be30_half150_trail30",
            "hard25_be30_half150_trail35_wick",
            "hard25_be30_half150_trail30_wick_posthoc",
        }
        for item in utility_labels
    )

    for name, passed in checks.items():
        if not passed:
            errors.append(name)
    result = {
        "passed": not errors,
        "errors": errors,
        "checks": checks,
        "counts": {
            "sequence_observations": len(sequence),
            "valid_microstructure_snapshots": len(captures),
            "failed_microstructure_audits": len(failed),
            "terminal_stale_signals": len(terminal_ids),
            "mature_30d_labels": len(labels),
            "utility_8h_observations": len(utility),
            "utility_8h_failed_audits": len(utility_failed),
            "utility_8h_terminal_stale": len(utility_terminal),
            "utility_mature_30d_labels": len(utility_labels),
        },
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
