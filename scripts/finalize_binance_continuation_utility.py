"""Freeze the calibration-selected 8h utility model for true forward OOS use."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.research.binance_continuation_utility import fit_frozen_utility_model
from core.research.binance_sequential_validation import SEQUENCE_FEATURES


DEFAULT_REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
VERSION = "continuation-utility-8h-v1-frozen-2026-07-19"
FEATURES = [*SEQUENCE_FEATURES, "entry_chase_vs_signal_close"]


def json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_ready(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    report = args.report_dir.resolve()
    decision = json.loads((report / "continuation_entry_decision.json").read_text(encoding="utf-8"))
    utility = decision["entry_utility"]
    if utility.get("paper_trade_gate_pass") is not True:
        raise RuntimeError("utility challenger has not passed the frozen historical paper gate")
    hours = int(utility["selected_checkpoint_hours"])
    quantile = float(utility["selected_quantile"])
    candidates = pd.read_csv(report / "continuation_entry_candidates.csv.gz", compression="gzip")
    candidates["decision_time"] = pd.to_datetime(candidates["decision_time"], utc=True)
    training = candidates[
        (candidates["checkpoint_hours"] == hours)
        & candidates["utility_return_14d"].notna()
    ].copy()
    model = fit_frozen_utility_model(
        training,
        features=FEATURES,
        label_column="utility_positive_14d",
        version=VERSION,
        checkpoint_hours=hours,
        selection_quantile=quantile,
        maturity_days=14,
    )
    payload = model.to_dict()
    model_hash = hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()
    output = {
        "schema_version": "1.0",
        "model": payload,
        "model_hash": model_hash,
        "source_decision": "continuation_entry_decision.json",
        "historical_classification": decision["entry_strategy_classification"],
        "forward_role": "secondary_paper_entry_challenger",
        "ranking_effect": False,
        "original_stage_vetoes_preserved": True,
        "daily_maximum_selected": 3,
        "automatic_trading_allowed": False,
        "read_only_no_order_routing": True,
        "freeze_rule": "no refit or threshold change before the scheduled 30/60/90d OOS review",
    }
    (report / "continuation_utility_forward_model.json").write_text(
        json.dumps(json_ready(output), ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8"
    )
    print(json.dumps(json_ready(output), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
