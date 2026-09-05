"""Independently verify the multisource Binance run-up evidence bundle."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    return parser.parse_args()


def top_lift(frame: pd.DataFrame, score: str) -> float:
    rank = frame[score].groupby(frame["date"]).rank(pct=True, method="first")
    selected = frame.loc[rank >= 0.98, "label_primary_int"]
    return float(selected.mean() / frame["label_primary_int"].mean())


def verify_family(report: Path, family: str) -> dict[str, Any]:
    scored = pd.read_csv(report / f"multisource_{family}_walk_forward_scored.csv.gz", compression="gzip")
    scored["date"] = pd.to_datetime(scored["date"], utc=True)
    metrics = pd.read_csv(report / f"multisource_{family}_fold_metrics.csv")
    checks: list[dict[str, Any]] = []
    for fold, group in scored.groupby("fold"):
        expected = metrics[metrics["fold"] == fold].iloc[0]
        price_ap = float(average_precision_score(group["label_primary_int"], group["price_same_score"]))
        combo_ap = float(
            average_precision_score(group["label_primary_int"], group[f"price_{family}_score"])
        )
        price_lift = top_lift(group, "price_same_score")
        combo_lift = top_lift(group, f"price_{family}_score")
        checks.append(
            {
                "fold": int(fold),
                "rows_match": int(len(group)) == int(expected["test_rows"]),
                "positives_match": int(group["label_primary_int"].sum()) == int(expected["test_positives"]),
                "price_ap_match": bool(np.isclose(price_ap, expected["price_average_precision"], atol=1e-12)),
                "combo_ap_match": bool(np.isclose(combo_ap, expected["combo_average_precision"], atol=1e-12)),
                "price_lift_match": bool(np.isclose(price_lift, expected["price_top_2pct_lift"], atol=1e-12)),
                "combo_lift_match": bool(np.isclose(combo_lift, expected["combo_top_2pct_lift"], atol=1e-12)),
            }
        )
    passed = bool(checks and all(all(value for key, value in row.items() if key != "fold") for row in checks))
    return {"passed": passed, "fold_checks": checks}


def verify_strategy(report: Path) -> dict[str, Any]:
    decision = json.loads((report / "multisource_catalyst_strategy_decision.json").read_text(encoding="utf-8"))
    trades = pd.read_csv(report / "multisource_catalyst_strategy_trades.csv.gz", compression="gzip")
    default = trades[trades["slippage_bps_each_side"] == 5.0].copy()
    returns = pd.to_numeric(default["net_return"], errors="coerce")
    gains = float(returns[returns > 0].sum())
    losses = float(-returns[returns < 0].sum())
    expected = decision["default_summary"]
    checks = {
        "trade_count": int(len(default)) == int(expected["trades"]),
        "expectancy": bool(np.isclose(returns.mean(), expected["expectancy"], atol=1e-12)),
        "profit_factor": bool(np.isclose(gains / losses, expected["profit_factor"], atol=1e-12)),
        "same_bar_stop_conservative": bool(
            ((default["exit_reason"] == "hard_stop") | (default["mfe"] < 1.0) | (default["partial_take_profit"])).all()
        ),
        "cost_scenarios": sorted(trades["slippage_bps_each_side"].unique().tolist()) == [5.0, 15.0, 30.0],
        "failed_gate_is_not_promoted": bool(
            not decision["paper_trade_gate_pass"] and decision["classification"] == "watchlist_only"
        ),
    }
    return {"passed": all(checks.values()), "checks": checks}


def main() -> None:
    args = parse_args()
    report = args.report_dir.resolve()
    summary = json.loads((report / "multisource_analysis_summary.json").read_text(encoding="utf-8"))
    identities = pd.read_csv(report / "multisource_token_identity.csv")
    mentions = pd.read_csv(report / "multisource_news_mentions.csv.gz", compression="gzip")
    mentions["available_at"] = pd.to_datetime(mentions["available_at"], utc=True)
    event_news = pd.read_csv(report / "multisource_current_event_news.csv")

    news = verify_family(report, "news")
    onchain = verify_family(report, "onchain")
    strategy = verify_strategy(report)
    checks = {
        "news_family": news["passed"],
        "onchain_family": onchain["passed"],
        "strategy": strategy["passed"],
        "identity_counts": int((identities["mapping_status"] == "unique").sum())
        == int(summary["onchain"]["coverage"]["unique_coingecko_identities"]),
        "news_counts": int(len(mentions)) == int(summary["news"]["coverage"]["strict_mapped_mentions"]),
        "news_uses_fetched_time": bool(mentions["available_at"].notna().all()),
        "ake_btw_event_regression": {"AKEUSDT", "BTWUSDT"}.issubset(set(event_news["symbol"])),
        "btw_early_catalyst_signal": "BTWUSDT" in set(
            pd.read_csv(report / "multisource_catalyst_signals.csv")["symbol"]
        ),
        "current_events_complete": int(len(event_news)) == 19,
        "no_unvalidated_family_promoted": bool(
            not summary["news"]["decision"]["ranking_eligible"]
            and not summary["onchain"]["decision"]["ranking_eligible"]
            and not summary["strict_strategy_decision"]["new_family_ranking_eligible"]
        ),
    }
    payload = {
        "passed": all(checks.values()),
        "checks": checks,
        "news": news,
        "onchain": onchain,
        "strategy": strategy,
    }
    (report / "multisource_verification_summary.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    if not payload["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
