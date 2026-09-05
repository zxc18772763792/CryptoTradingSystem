"""Patch the complete native Binance report artifact with exit-policy evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"


def records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    clean = frame.replace({np.nan: None, np.inf: None, -np.inf: None}).copy()
    for column in clean.columns:
        if pd.api.types.is_datetime64_any_dtype(clean[column]):
            clean[column] = clean[column].astype(str)
    return clean.to_dict(orient="records")


def profit_factor(values: pd.Series) -> float:
    returns = pd.to_numeric(values, errors="coerce").dropna()
    gains = float(returns[returns > 0].sum())
    losses = float(-returns[returns < 0].sum())
    return gains / losses if losses > 0 else float("inf")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    report = args.report_dir.resolve()
    artifact_path = report / "artifact.json"
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    decision = json.loads((report / "exit_strategy_decision.json").read_text(encoding="utf-8"))
    fixed_sensitivity = pd.read_csv(report / "exit_policy_confirmation_sensitivity.csv")
    fixed_summary = pd.read_csv(report / "exit_policy_frozen_confirmation_summary.csv")
    adaptive_summary = pd.read_csv(report / "exit_policy_time_forward_summary.csv")
    calibration = pd.read_csv(report / "exit_policy_calibration_ranking.csv")
    frozen_trades = pd.read_csv(report / "exit_policy_frozen_confirmation_trades.csv.gz")

    fixed_tp = fixed_sensitivity[
        fixed_sensitivity["family"] == "fixed_take_profit"
    ][
        [
            "policy", "time_days", "fixed_take_profit_pct", "trades", "expectancy",
            "profit_factor", "win_rate", "median_return", "max_drawdown",
        ]
    ].sort_values(["time_days", "fixed_take_profit_pct"])
    cost_rows: list[dict[str, Any]] = []
    for strategy, frame in [
        ("Frozen fixed policy", fixed_summary),
        ("Time-forward selector", adaptive_summary),
    ]:
        for _, row in frame.iterrows():
            cost_rows.append(
                {
                    "strategy": strategy,
                    "slippage_bps_each_side": float(row["slippage_bps_each_side"]),
                    "trades": int(row["trades"]),
                    "expectancy": float(row["expectancy"]),
                    "profit_factor": float(row["profit_factor"]),
                    "max_drawdown": float(row["max_drawdown"]),
                    "fold_majority_profit_factor_gt_1": float(
                        row["fold_majority_profit_factor_gt_1"]
                    ),
                }
            )
    cost_frame = pd.DataFrame(cost_rows)
    default_frozen = frozen_trades[frozen_trades["slippage_bps_each_side"] == 5.0]
    fold_rows: list[dict[str, Any]] = []
    for fold, group in default_frozen.groupby("fold"):
        values = pd.to_numeric(group["net_return"], errors="coerce").dropna()
        fold_rows.append(
            {
                "fold": int(fold),
                "trades": int(len(values)),
                "expectancy": float(values.mean()),
                "win_rate": float((values > 0).mean()),
                "profit_factor": profit_factor(values),
                "positive_expectancy": bool(values.mean() > 0),
            }
        )
    fold_frame = pd.DataFrame(fold_rows).sort_values("fold")
    calibration_top = calibration[
        [
            "policy", "family", "matured_trades", "expectancy", "profit_factor",
            "selection_score", "selection_eligible", "fixed_take_profit_pct",
            "partial_take_profit_pct", "trail_distance_pct", "time_days",
        ]
    ].head(10)

    source = {
        "id": "exit-validation",
        "label": "Time-isolated exit-policy validation",
        "path": "exit_policy_frozen_confirmation_summary.csv",
        "query": {
            "engine": "duckdb",
            "language": "sql",
            "sql": (
                "SELECT * FROM read_csv_auto('exit_policy_frozen_confirmation_summary.csv') "
                "ORDER BY slippage_bps_each_side"
            ),
            "description": (
                "Fifty-two fixed, staged, and trailing exits compared at default cost; "
                "the calibration-selected policy is frozen for folds 3-6 and stressed at 15/30 bps."
            ),
            "tables_used": [
                "exit_policy_catalog.csv",
                "exit_policy_calibration_ranking.csv",
                "exit_policy_confirmation_sensitivity.csv",
                "exit_policy_frozen_confirmation_summary.csv",
                "exit_policy_time_forward_summary.csv",
            ],
            "filters": [
                "unchanged frozen top-2% entry signal",
                "-20% hard stop for every exit policy",
                "calibration uses only trades exited before 2026-03-24",
                "frozen confirmation uses walk-forward folds 3-6",
                "same-bar stop is evaluated before any profit target",
            ],
            "metric_definitions": [
                "Expectancy is the arithmetic mean net return per accepted trade after funding, fees, and slippage.",
                "Profit factor is the sum of positive trade returns divided by the absolute sum of negative trade returns.",
                "The fixed exit is eligible only if point gates, stress costs, time-fold majority, and both uncertainty lower bounds pass.",
                "Funding coverage is the share of accepted trades whose symbol has matched historical funding observations; missing funding is treated as zero.",
            ],
        },
    }
    manifest = artifact["manifest"]
    manifest["sources"] = [item for item in manifest.get("sources", []) if item.get("id") != "exit-validation"] + [source]
    artifact["sources"] = manifest["sources"]
    manifest["generatedAt"] = decision["generated_at_utc"]
    artifact["snapshot"]["generatedAt"] = decision["generated_at_utc"]

    exit_chart_ids = {"exit-fixed-tp", "exit-fold-pf", "exit-cost-stress"}
    manifest["charts"] = [item for item in manifest.get("charts", []) if item.get("id") not in exit_chart_ids]
    manifest["charts"].extend(
        [
            {
                "id": "exit-fixed-tp",
                "title": "Fixed take-profit confirmation expectancy",
                "type": "line",
                "dataset": "exit_fixed_tp",
                "encodings": {
                    "x": {"field": "fixed_take_profit_pct", "type": "quantitative", "title": "Take-profit threshold (%)"},
                    "y": {"field": "expectancy", "type": "quantitative", "title": "Expectancy"},
                    "color": {"field": "time_days", "type": "nominal", "title": "Maximum holding days"},
                },
                "sourceId": "exit-validation",
            },
            {
                "id": "exit-fold-pf",
                "title": "Frozen exit profit factor by confirmation fold",
                "type": "bar",
                "dataset": "exit_fold_metrics",
                "encodings": {
                    "x": {"field": "fold", "type": "nominal", "title": "Fold"},
                    "y": {"field": "profit_factor", "type": "quantitative", "title": "Profit factor"},
                },
                "sourceId": "exit-validation",
            },
            {
                "id": "exit-cost-stress",
                "title": "Exit strategy expectancy under slippage stress",
                "type": "bar",
                "dataset": "exit_costs",
                "encodings": {
                    "x": {"field": "slippage_bps_each_side", "type": "nominal", "title": "Slippage each side (bps)"},
                    "y": {"field": "expectancy", "type": "quantitative", "title": "Expectancy"},
                    "color": {"field": "strategy", "type": "nominal", "title": "Exit selection"},
                },
                "options": {"grouping": "grouped"},
                "sourceId": "exit-validation",
            },
        ]
    )

    exit_table_ids = {"exit-cost-table", "exit-calibration-table"}
    manifest["tables"] = [item for item in manifest.get("tables", []) if item.get("id") not in exit_table_ids]
    manifest["tables"].extend(
        [
            {
                "id": "exit-cost-table",
                "title": "Frozen and time-forward exit results by cost",
                "dataset": "exit_costs",
                "columns": [
                    {"field": "strategy", "label": "Strategy", "type": "text"},
                    {"field": "slippage_bps_each_side", "label": "Slippage/side", "type": "number"},
                    {"field": "trades", "label": "Trades", "type": "number"},
                    {"field": "expectancy", "label": "Expectancy", "type": "percent"},
                    {"field": "profit_factor", "label": "Profit factor", "type": "number"},
                    {"field": "max_drawdown", "label": "Max drawdown", "type": "percent"},
                ],
                "defaultSort": {"field": "expectancy", "direction": "desc"},
                "sourceId": "exit-validation",
            },
            {
                "id": "exit-calibration-table",
                "title": "Top matured calibration policies",
                "dataset": "exit_calibration_top",
                "columns": [
                    {"field": "policy", "label": "Policy", "type": "text"},
                    {"field": "family", "label": "Family", "type": "text"},
                    {"field": "matured_trades", "label": "Matured trades", "type": "number"},
                    {"field": "expectancy", "label": "Expectancy", "type": "percent"},
                    {"field": "profit_factor", "label": "Profit factor", "type": "number"},
                    {"field": "selection_score", "label": "Selection score", "type": "number"},
                ],
                "defaultSort": {"field": "selection_score", "direction": "desc"},
                "sourceId": "exit-validation",
            },
        ]
    )

    exit_block_ids = {
        "exit-scope", "exit-fixed-chart-block", "exit-frozen", "exit-fold-chart-block",
        "exit-cost-heading", "exit-cost-chart-block", "exit-cost-table-block",
        "exit-uncertainty", "exit-method", "exit-calibration-table-block",
    }
    blocks = [item for item in manifest["blocks"] if item.get("id") not in exit_block_ids]
    for item in blocks:
        if item.get("id") == "paper-heading":
            item["body"] = (
                "## 四政策组合回测仅作探索\n\n"
                "原完整历史四政策比较会使用全部历史选择赢家，因此不能证明平仓规则具有时间外稳定性。"
                "下面的 52 政策研究按平仓完成时间隔离校准与确认，严格结论以新结果为准。"
            )
            item.pop("sourceId", None)
    insertion = next((index + 1 for index, item in enumerate(blocks) if item.get("id") == "paper-table-block"), len(blocks))
    exit_blocks = [
        {
            "id": "exit-scope", "type": "markdown", "sourceId": "exit-validation",
            "body": (
                "## 小止盈会截断暴涨币收益的右尾\n\n"
                "固定止盈 +25% 和 14 天 +50% 在确认期为负；较高阈值整体更有利。"
                "14 天 +300% 的描述性期望最高，但它是查看确认期后才知道的赢家，不能直接冻结使用。"
            ),
        },
        {"id": "exit-fixed-chart-block", "type": "chart", "chartId": "exit-fixed-tp"},
        {
            "id": "exit-frozen", "type": "markdown", "sourceId": "exit-validation",
            "body": (
                "## 冻结退出规则只有两个确认折盈利\n\n"
                "校准选择 -20% 止损、+100% 卖一半、剩余 25% 追踪、最长 30 天。"
                "后四折 121 笔交易的期望为 4.50%、利润因子 1.30，但仅 2/4 折利润因子大于 1。"
            ),
        },
        {"id": "exit-fold-chart-block", "type": "chart", "chartId": "exit-fold-pf"},
        {
            "id": "exit-cost-heading", "type": "markdown", "sourceId": "exit-validation",
            "body": (
                "## 成本不是主要失败原因\n\n"
                "冻结政策在每边 30bps 滑点下仍有 4.13% 期望和 1.27 利润因子；"
                "逐折自适应版本的点估计更高。主要风险来自时间状态不稳定和不确定性区间，而不是手续费；"
                "同时只有约 33.9% 的确认交易匹配到历史资金费率，缺失部分按 0 处理。"
            ),
        },
        {"id": "exit-cost-chart-block", "type": "chart", "chartId": "exit-cost-stress"},
        {"id": "exit-cost-table-block", "type": "table", "tableId": "exit-cost-table"},
        {
            "id": "exit-uncertainty", "type": "markdown", "sourceId": "exit-validation",
            "body": (
                "## 置信区间否决了可交易结论\n\n"
                "冻结政策的周块与币种 bootstrap 期望下界分别为 -5.80% 和 -4.79%；"
                "自适应版本也分别为 -1.57% 和 -1.48%。固定规则归类为 `watchlist_only`，"
                "自适应规则归类为 `adaptive_promising_but_unconfirmed`。"
            ),
        },
        {
            "id": "exit-method", "type": "markdown", "sourceId": "exit-validation",
            "body": (
                "## 平仓选择只读取当时已经完成的交易\n\n"
                "52 个政策在默认成本下比较；2026-03-24 前完成平仓的交易用于一次性校准，folds 3–6 用于冻结确认。"
                "逐折版本每折重新执行同一规则，但只能读取该折开始前已经退出的交易。"
            ),
        },
        {"id": "exit-calibration-table-block", "type": "table", "tableId": "exit-calibration-table"},
    ]
    manifest["blocks"] = blocks[:insertion] + exit_blocks + blocks[insertion:]
    artifact["snapshot"]["datasets"]["exit_fixed_tp"] = records(fixed_tp)
    artifact["snapshot"]["datasets"]["exit_costs"] = records(cost_frame)
    artifact["snapshot"]["datasets"]["exit_fold_metrics"] = records(fold_frame)
    artifact["snapshot"]["datasets"]["exit_calibration_top"] = records(calibration_top)
    artifact_path.write_text(json.dumps(artifact, ensure_ascii=True, indent=2), encoding="utf-8")
    print(artifact_path)


if __name__ == "__main__":
    main()
