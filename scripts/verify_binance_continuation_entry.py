"""Independent consistency verifier for the 8h continuation-utility study."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.research.binance_continuation_utility import FrozenUtilityModel


REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"


def close(left: float, right: float, tolerance: float = 1e-10) -> bool:
    return bool(np.isclose(float(left), float(right), rtol=tolerance, atol=tolerance))


def profit_factor(values: pd.Series) -> float:
    returns = pd.to_numeric(values, errors="coerce").dropna()
    gains = float(returns[returns > 0].sum())
    losses = float(-returns[returns < 0].sum())
    return gains / losses if losses > 0 else (np.inf if gains > 0 else 0.0)


def main() -> None:
    decision = json.loads((REPORT / "continuation_entry_decision.json").read_text(encoding="utf-8"))
    calibration = pd.read_csv(REPORT / "continuation_utility_calibration.csv")
    scored = pd.read_csv(REPORT / "continuation_utility_confirmation_scored.csv.gz", compression="gzip")
    trades = pd.read_csv(REPORT / "continuation_utility_strategy_trades.csv.gz", compression="gzip")
    candidates = pd.read_csv(REPORT / "continuation_entry_candidates.csv.gz", compression="gzip")
    model_payload = json.loads((REPORT / "continuation_utility_forward_model.json").read_text(encoding="utf-8"))
    utility = decision["entry_utility"]
    selected_spec = calibration.iloc[0]
    default_trades = trades[trades["slippage_bps_each_side"] == 5.0].copy()
    summary = utility["strategy_default_summary"]

    checks: dict[str, bool] = {}
    checks["selected_only_from_calibration"] = bool(
        int(selected_spec["checkpoint_hours"]) == int(utility["selected_checkpoint_hours"])
        and close(selected_spec["selection_quantile"], utility["selected_quantile"])
        and bool(selected_spec["selection_eligible"])
    )
    checks["selected_spec_is_8h_q70"] = bool(
        int(utility["selected_checkpoint_hours"]) == 8
        and close(utility["selected_quantile"], 0.70)
    )
    checks["all_candidate_labels_present"] = bool(
        candidates["utility_return_14d"].notna().all()
        and set(candidates["checkpoint_hours"].unique()) == {4, 8, 12, 16, 20, 24}
    )
    checks["confirmation_daily_limit"] = bool(
        scored[scored["selected"]]
        .assign(day=pd.to_datetime(scored.loc[scored["selected"], "decision_time"], utc=True).dt.floor("D"))
        .groupby("day").size().max() <= 3
    )
    checks["trade_membership_selected_only"] = set(default_trades["signal_key"].astype(int)).issubset(
        set(scored.loc[scored["selected"], "signal_key"].astype(int))
    )
    checks["strategy_trade_count"] = int(len(default_trades)) == int(summary["trades"])
    checks["strategy_expectancy"] = close(default_trades["net_return"].mean(), summary["expectancy"])
    checks["strategy_profit_factor"] = close(profit_factor(default_trades["net_return"]), summary["profit_factor"])
    checks["time_fold_gate"] = bool(
        utility["fold_majority_profit_factor_gt_1"] > 0.50
        and sum(float(item["profit_factor"]) > 1 for item in utility["strategy_fold_metrics"]) == 3
    )
    checks["market_state_gate"] = bool(
        int(summary["market_states"]) >= 3
        and float(summary["market_state_majority_profit_factor_gt_1"]) > 0.50
    )
    checks["cluster_bootstrap_gate"] = bool(
        utility["week_bootstrap"]["expectancy_lower_95pct"] > 0
        and utility["symbol_bootstrap"]["expectancy_lower_95pct"] > 0
        and utility["week_bootstrap"]["profit_factor_lower_95pct"] > 1
        and utility["symbol_bootstrap"]["profit_factor_lower_95pct"] > 1
    )
    largest_symbol = str(utility["largest_positive_symbol"])
    checks["symbol_concentration_gate"] = bool(
        utility["largest_symbol_positive_pnl_share"] <= 0.25
        and utility["leave_largest_symbol_out_expectancy"] > 0
        and close(
            default_trades.loc[default_trades["symbol"] != largest_symbol, "net_return"].mean(),
            utility["leave_largest_symbol_out_expectancy"],
        )
    )
    checks["direct_200pct_not_promoted"] = bool(
        decision["direct_continuation"]["ranking_gate_pass"] is False
        and decision["identification_classification"] == "watchlist_only"
    )
    checks["utility_paper_candidate_only"] = bool(
        utility["paper_trade_gate_pass"] is True
        and decision["entry_strategy_classification"] == "paper_candidate_pending_true_oos"
        and decision["automatic_trading_allowed"] is False
    )

    raw_model = model_payload["model"]
    calculated_hash = hashlib.sha256(json.dumps(raw_model, sort_keys=True).encode("utf-8")).hexdigest()
    model = FrozenUtilityModel.from_dict(raw_model)
    training = candidates[candidates["checkpoint_hours"] == model.checkpoint_hours].copy()
    training_scores = model.predict_score(training)
    checks["forward_model_hash"] = calculated_hash == model_payload["model_hash"]
    checks["forward_model_training_scope"] = bool(
        model.training_rows == len(training)
        and model.training_positives == int(training["utility_positive_14d"].sum())
        and pd.Timestamp(model.maximum_label_maturity_utc) <= pd.Timestamp("2026-07-18", tz="UTC")
    )
    checks["forward_threshold_is_frozen_q70"] = close(
        np.quantile(training_scores, model.selection_quantile), model.selection_threshold
    )
    checks["forward_model_is_rank_neutral_read_only"] = bool(
        model_payload["ranking_effect"] is False
        and model_payload["original_stage_vetoes_preserved"] is True
        and model_payload["automatic_trading_allowed"] is False
        and model_payload["read_only_no_order_routing"] is True
    )

    errors = [name for name, passed in checks.items() if not passed]
    result = {
        "passed": not errors,
        "errors": errors,
        "checks": checks,
        "recomputed": {
            "trades": int(len(default_trades)),
            "expectancy": float(default_trades["net_return"].mean()),
            "profit_factor": float(profit_factor(default_trades["net_return"])),
            "selected_rows": int(scored["selected"].sum()),
            "late_target200_selected": int(scored.loc[scored["selected"], "late_target200"].sum()),
        },
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
