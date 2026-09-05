"""Append immutable 30-day outcomes to frozen 8h utility observations."""

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
DEFAULT_OUTPUT = ROOT / "data" / "research" / "binance_200pct_forward_monitor"


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


STRATEGY = _load(
    "binance_forward_strategy_labeler_for_utility",
    ROOT / "scripts" / "binance_forward_strategy_labeler.py",
)
FORWARD = STRATEGY.FORWARD


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", help="ISO-8601 cutoff; a date means 20:20 Asia/Shanghai")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--no-external-refresh", action="store_true")
    return parser.parse_args()


def as_of_timestamp(value: str | None) -> pd.Timestamp:
    if value is None:
        return pd.Timestamp.now(tz="UTC")
    if len(value.strip()) == 10:
        return pd.Timestamp(f"{value.strip()} 20:20:00", tz="Asia/Shanghai").tz_convert("UTC")
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize("Asia/Shanghai")
    return stamp.tz_convert("UTC")


def load_observations(output_root: Path) -> list[dict[str, Any]]:
    folder = output_root / "continuation_utility_8h"
    return [json.loads(path.read_text(encoding="utf-8")) for path in sorted(folder.glob("*.json"))] if folder.exists() else []


def immutable_write(path: Path, payload: dict[str, Any]) -> str:
    encoded = json.dumps(FORWARD.json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != encoded:
            raise RuntimeError(f"Immutable utility label exists with different content: {path}")
        return "idempotent_existing"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(encoded, encoding="utf-8")
    return "created"


def main() -> None:
    args = parse_args()
    output = args.output_root.resolve()
    as_of = as_of_timestamp(args.as_of)
    existing = {path.stem.removesuffix("-30d") for path in (output / "continuation_utility_30d_labels").glob("*-30d.json")} if (output / "continuation_utility_30d_labels").exists() else set()
    observations = [item for item in load_observations(output) if str(item["signal_id"]) not in existing]
    due = [item for item in observations if as_of >= STRATEGY.utc(item["decision_time_utc"]) + pd.Timedelta(days=30, hours=4)]
    pending = [item for item in observations if item not in due]
    sources: dict[str, dict[str, Any]] = {}
    if not args.no_external_refresh:
        with ThreadPoolExecutor(max_workers=min(6, max(1, len(due)))) as pool:
            futures = {pool.submit(STRATEGY.fetch_path, item): str(item["signal_id"]) for item in due}
            for future in as_completed(futures):
                sources[futures[future]] = future.result()

    labels: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for item in due:
        signal_id = str(item["signal_id"])
        if args.no_external_refresh:
            failures.append({"signal_id": signal_id, "symbol": item["symbol"], "failure": "external_refresh_disabled", "fail_closed": True})
            continue
        source = sources[signal_id]
        if source["error"]:
            failures.append({"signal_id": signal_id, "symbol": item["symbol"], "failure": source["error"], "fail_closed": True})
            continue
        adapted = {
            "signal_id": signal_id,
            "snapshot_date": item["snapshot_date"],
            "symbol": item["symbol"],
            "original_stage": item["original_stage"],
            "paper_eligible_after_original_stage": item["paper_eligible_after_original_stage"],
            "decision_time_utc": item["decision_time_utc"],
        }
        label, audit = STRATEGY.build_label(
            adapted,
            source["bars"],
            source["funding"],
            labeled_at=as_of,
            include_utility_entry_counterfactual=True,
            include_utility_hold_counterfactual=True,
        )
        if label is None:
            failures.append(audit)
            continue
        label.update(
            {
                "utility_score": float(item["utility_score"]),
                "utility_threshold": float(item["utility_threshold"]),
                "utility_selected_raw": bool(item["utility_selected_raw"]),
                "utility_selected_daily_top3": bool(item["utility_selected_daily_top3"]),
                "taker_flow_annotation": item.get("taker_flow_annotation"),
                "taker_flow_supportive": bool(item.get("taker_flow_supportive", False)),
                "model_version": item["model_version"],
                "model_hash": item["model_hash"],
                "label_family": "continuation_utility_8h",
                "primary_ranking_changed": False,
            }
        )
        labels.append(label)

    audit = {
        "as_of_utc": as_of,
        "unlabeled_observations": len(observations),
        "due_count": len(due),
        "pending_count": len(pending),
        "labels_ready": len(labels),
        "failed_count": len(failures),
        "failures": failures,
        "read_only_no_order_routing": True,
    }
    if args.dry_run:
        audit["labels"] = labels
        print(json.dumps(FORWARD.json_ready(audit), ensure_ascii=False, indent=2))
        return
    statuses = []
    for label in labels:
        statuses.append({"signal_id": label["signal_id"], "status": immutable_write(output / "continuation_utility_30d_labels" / f"{label['signal_id']}-30d.json", label)})
    slug = as_of.strftime("%Y%m%dT%H%M%SZ")
    for failure in failures:
        statuses.append({"signal_id": failure["signal_id"], "status": immutable_write(output / "continuation_utility_30d_label_failed" / f"{failure['signal_id']}-{slug}.json", failure)})
    audit["statuses"] = statuses
    print(json.dumps(FORWARD.json_ready(audit), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
