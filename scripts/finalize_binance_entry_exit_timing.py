"""Merge verified entry/exit timing research into the existing Binance report."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import nbformat
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
BEGIN = "<!-- ENTRY_EXIT_TIMING_BEGIN -->"
END = "<!-- ENTRY_EXIT_TIMING_END -->"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    return parser.parse_args()


def replace_section(text: str, section: str) -> str:
    if BEGIN in text and END in text:
        before, rest = text.split(BEGIN, 1)
        _, after = rest.split(END, 1)
        return before.rstrip() + "\n\n" + section + "\n" + after.lstrip()
    return text.rstrip() + "\n\n" + section + "\n"


def clean_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    clean = frame.replace({np.nan: None, np.inf: None, -np.inf: None}).copy()
    for column in clean.columns:
        if pd.api.types.is_datetime64_any_dtype(clean[column]):
            clean[column] = clean[column].astype(str)
    return clean.to_dict(orient="records")


def make_charts(
    report: Path,
    entry_calibration: pd.DataFrame,
    thresholds: pd.DataFrame,
    stops: pd.DataFrame,
    path_summary: pd.DataFrame,
) -> None:
    charts = report / "charts"
    charts.mkdir(parents=True, exist_ok=True)
    blue = "#3B6FB6"
    gold = "#C7922B"
    orange = "#D97732"
    ink = "#20262E"
    grey = "#AAB2BD"

    ordered = entry_calibration.sort_values("actual_200pct_precision", ascending=True)
    fig, axis = plt.subplots(figsize=(9.2, 5.6))
    colors = [blue if value else grey for value in ordered["selection_eligible"]]
    axis.barh(ordered["entry_rule"], 100 * ordered["actual_200pct_precision"], color=colors)
    axis.axvline(100 * float(ordered.loc[ordered["entry_rule"] == "next_open", "actual_200pct_precision"].iloc[0]), color=ink, linestyle="--", linewidth=1)
    axis.set(title="Calibration precision by entry timing rule", xlabel="Actual +200% precision after entry (%)", ylabel="Entry rule")
    axis.grid(axis="x", color="#E5E7EB")
    axis.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(charts / "timing_entry_precision.png", dpi=180, bbox_inches="tight")
    plt.close(fig)

    fig, axes = plt.subplots(1, 2, figsize=(11.5, 4.6))
    axes[0].bar((100 * thresholds["rank_threshold"]).map(lambda value: f"{value:.1f}"), 100 * thresholds["actual_200pct_precision"], color=blue)
    axes[0].set(title="Confirmation precision by rank threshold", xlabel="Frozen score percentile (%)", ylabel="Actual +200% precision (%)")
    axes[1].bar((100 * stops["hard_stop_pct"]).map(lambda value: f"-{value:.0f}"), 100 * stops["expectancy"], color=gold)
    axes[1].axhline(0, color=ink, linewidth=1)
    axes[1].set(title="Risk-equal expectancy by hard-stop width", xlabel="Hard stop (%)", ylabel="Expectancy (%)")
    for axis in axes:
        axis.grid(axis="y", color="#E5E7EB")
        axis.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(charts / "timing_threshold_stop.png", dpi=180, bbox_inches="tight")
    plt.close(fig)

    horizons = [1, 3, 5, 7, 14]
    fig, axis = plt.subplots(figsize=(8.2, 4.8))
    palette = {"actual_200pct": blue, "not_200pct": orange}
    for _, row in path_summary.iterrows():
        values = [100 * float(row[f"median_mfe_{days}d"]) for days in horizons]
        axis.plot(horizons, values, marker="o", linewidth=2, label=row["cohort"], color=palette[row["cohort"]])
    axis.set(title="Median maximum favorable excursion after entry", xlabel="Days after entry", ylabel="Median MFE (%)", xticks=horizons)
    axis.grid(axis="y", color="#E5E7EB")
    axis.legend(frameon=False)
    axis.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(charts / "timing_path_mfe.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def update_notebook(report: Path) -> None:
    path = report / "binance_runup_research.ipynb"
    notebook = nbformat.read(path, as_version=4)
    notebook.cells = [
        cell for cell in notebook.cells
        if "entry-exit-timing" not in cell.get("metadata", {}).get("tags", [])
    ]
    notebook.cells.extend(
        [
            nbformat.v4.new_markdown_cell(
                """## Entry and exit timing extension

This section reads the time-isolated entry catalog, one-bar microstructure model, rank-threshold sensitivity, risk-equal stop study, and 4h path timing diagnostics. Rule selection uses only outcomes matured before fold 3; folds 3-6 remain historical confirmation, not true future OOS.""",
                metadata={"tags": ["entry-exit-timing"]},
            ),
            nbformat.v4.new_code_cell(
                """timing = json.loads((ROOT / 'entry_exit_timing_decision.json').read_text(encoding='utf-8'))
micro = json.loads((ROOT / 'microstructure_confirmation_decision.json').read_text(encoding='utf-8'))
rank = json.loads((ROOT / 'rank_threshold_decision.json').read_text(encoding='utf-8'))
path_timing = json.loads((ROOT / 'path_timing_decision.json').read_text(encoding='utf-8'))
stops = json.loads((ROOT / 'stop_width_decision.json').read_text(encoding='utf-8'))
entry_rules = pd.read_csv(ROOT / 'entry_timing_calibration.csv')
rank_confirmation = pd.read_csv(ROOT / 'rank_threshold_confirmation.csv')
stop_confirmation = pd.read_csv(ROOT / 'stop_width_confirmation.csv')
path_summary = pd.read_csv(ROOT / 'path_timing_summary.csv')
pd.Series({
    'selected entry': timing['selected_entry_rule'],
    'selected exit': timing['selected_exit_policy'],
    'confirmation trade expectancy': timing['default_cost_summary']['expectancy'],
    'confirmation PF': timing['default_cost_summary']['profit_factor'],
    'micro selected precision': micro['pooled_metrics']['selected_precision'],
    'rank threshold': rank['selected_threshold'],
    'stop width': stops['selected_hard_stop_pct'],
})""",
                metadata={"tags": ["entry-exit-timing"]},
            ),
            nbformat.v4.new_code_cell(
                """fig, axes = plt.subplots(1, 2, figsize=(12, 4))
axes[0].bar((100 * rank_confirmation['rank_threshold']).map(lambda x: f'{x:.1f}'), 100 * rank_confirmation['actual_200pct_precision'], color='#3B6FB6')
axes[0].set(title='Confirmation precision by rank threshold', xlabel='Percentile threshold', ylabel='Actual +200% precision (%)')
horizons = [1, 3, 5, 7, 14]
for _, row in path_summary.iterrows():
    axes[1].plot(horizons, [100 * row[f'median_mfe_{d}d'] for d in horizons], marker='o', label=row['cohort'])
axes[1].set(title='Median maximum favorable excursion', xlabel='Days after entry', ylabel='Median MFE (%)', xticks=horizons)
axes[1].legend(frameon=False)
for axis in axes: axis.grid(axis='y', alpha=.2)
plt.tight_layout(); plt.show()""",
                metadata={"tags": ["entry-exit-timing"]},
            ),
        ]
    )
    nbformat.write(notebook, path)


def main() -> None:
    args = parse_args()
    report = args.report_dir.resolve()
    verification = json.loads((report / "entry_exit_timing_verification.json").read_text(encoding="utf-8"))
    if not verification.get("passed"):
        raise RuntimeError("Entry/exit timing verifier did not pass")
    timing = json.loads((report / "entry_exit_timing_decision.json").read_text(encoding="utf-8"))
    micro = json.loads((report / "microstructure_confirmation_decision.json").read_text(encoding="utf-8"))
    rank = json.loads((report / "rank_threshold_decision.json").read_text(encoding="utf-8"))
    path_timing = json.loads((report / "path_timing_decision.json").read_text(encoding="utf-8"))
    stops = json.loads((report / "stop_width_decision.json").read_text(encoding="utf-8"))
    entry_calibration = pd.read_csv(report / "entry_timing_calibration.csv")
    comparison = pd.read_csv(report / "entry_exit_confirmation_comparison.csv")
    timing_folds = pd.read_csv(report / "entry_exit_confirmation_folds.csv")
    micro_folds = pd.read_csv(report / "microstructure_confirmation_folds.csv")
    thresholds = pd.read_csv(report / "rank_threshold_confirmation.csv")
    threshold_folds = pd.read_csv(report / "rank_threshold_confirmation_folds.csv")
    stop_frame = pd.read_csv(report / "stop_width_confirmation.csv")
    path_summary = pd.read_csv(report / "path_timing_summary.csv")
    make_charts(report, entry_calibration, thresholds, stop_frame, path_summary)

    base_rec = timing["confirmation_recognition"]["next_open"]
    selected_rec = timing["confirmation_recognition"]["selected_entry"]
    selected_trade = timing["default_cost_summary"]
    micro_metrics = micro["pooled_metrics"]
    rank_selected = rank["confirmation_selected"]
    rank_base = rank["confirmation_baseline_98pct"]
    launch = path_timing["winner_launch_speed"]
    stop_risk = path_timing["stop_path_risk"]
    stop_selected = stops["confirmation_selected"]
    section = f"""{BEGIN}
## 入场与退出时机：新增严格历史研究

### 结论先行

- **目前仍没有通过门槛的买入策略。** 校准期选择了“等待一根 4h 再进场”和“+50% 卖一半、其余 20% 追踪”，但带到第 3–6 折后，实际 +200% 命中率从立即入场的 {base_rec['actual_200pct_precision']:.2%} 降至 {selected_rec['actual_200pct_precision']:.2%}；{int(selected_trade['trades'])} 笔交易的期望为 {selected_trade['expectancy']:.2%}、利润因子 {selected_trade['profit_factor']:.2f}，仅 {timing['fold_majority_profit_factor_gt_1']:.0%} 的确认折利润因子大于 1。
- **当前最合理的“何时买”仍是信号后的下一根预定 4h 开盘，但只能用于纸面观察。** 等待回踩、突破、波动收缩或单根 4h 微观确认都没有稳定提高识别率；它们更常见的效果是延迟并抬高成本。
- **当前最合理的“何时卖”仍是 -20% 灾难止损、+100% 卖一半、剩余 25% 追踪、最长 30 天。** 更早的失败退出、保本止损、动量退出以及 -15%/-25%/-30% 风险等额止损均未在时间隔离确认中取代它。

### 等待确认没有提高暴涨币浓度

11 个入场规则在 2026-03-24 前完成校准。等待一根 4h 在校准期合格，但确认期漏掉 1 个实际 +200% 信号，命中率低于立即入场；等待数天的回踩与突破规则则要么没有提高精确率，要么把正例保留率压得过低。

![入场规则校准精确率](charts/timing_entry_precision.png)

单根 4h 模型进一步使用成交量、主动买入占比、短线涨幅、区间和 EMA 位置。它在四个确认折中没有一折提高筛选精确率：价格基线 AP 为 {micro_metrics['price_average_precision']:.4f}，价格加 4h 微观结构 AP 降至 {micro_metrics['micro_average_precision']:.4f}；选择后的命中率仅 {micro_metrics['selected_precision']:.2%}，低于全体一根等待样本的 {micro_metrics['base_precision']:.2%}。

### 98.5% 只能作为优先标签，不能缩小主观察池

校准期选出的 98.5% 阈值，在确认期把汇总命中率从 {rank_base['actual_200pct_precision']:.2%} 提到 {rank_selected['actual_200pct_precision']:.2%}，并保留相同的 {int(rank_selected['actual_200pct_entries'])} 个实际正例；但只在 {rank['confirmation_fold_precision_improvement_rate']:.0%} 的折中改善，而且交易期望从 {rank_base['expectancy']:.2%} 降至 {rank_selected['expectancy']:.2%}。因此 98%–98.5% 仍需保留为主观察层，≥98.5% 只标记为高优先级。

![阈值与止损敏感性](charts/timing_threshold_stop.png)

### 真正赢家通常在两到三天内启动，但仍有先跌后涨路径

26 个实际 +200% 信号的中位 MFE 在第 1 天已达 {launch['median_mfe_1d']:.1%}、第 3 天达 {launch['median_mfe_3d']:.1%}；中位数约 {launch['median_hours_to_50pct']:.0f} 小时达到 +50%、{launch['median_hours_to_100pct']:.0f} 小时达到 +100%。这说明“3 天仍未达到 +10%”是有意义的风险标签，但不能直接平仓：仍有 {launch['share_below_10pct_mfe_at_3d']:.1%} 的最终赢家符合这个慢启动条件。

![赢家与非赢家 MFE 路径](charts/timing_path_mfe.png)

同时，{stop_risk['loser_share_hit_stop20_within_14d']:.1%} 的非赢家会在 14 天内触及 -20%，但也有 {stop_risk['winner_share_stop20_before_50pct']:.1%} 的最终 +200% 路径先触及 -20% 再达到 +50%。在每笔风险固定为权益 0.5% 后，校准仍选择 -20%；确认期利润因子 {stop_selected['profit_factor']:.2f}，但只有 {stops['selected_fold_majority_profit_factor_gt_1']:.0%} 的折盈利，周块与币种 bootstrap 期望下界分别为 {stops['week_block_bootstrap']['expectancy_lower_95pct']:.2%} 和 {stops['symbol_bootstrap']['expectancy_lower_95pct']:.2%}。

### 当前可执行的研究协议

1. **观察池：** 保留冻结价格分位 ≥98%；≥98.5% 标记 `priority_watch`，但不删除 98%–98.5% 候选。
2. **入场时钟：** 记录信号后下一根 4h 开盘作为统一参考价；4h 成交量、主动买入或回踩确认只作注释，不能提高排名或触发加仓。
3. **追高拒绝：** 继续拒绝单日涨幅 ≥20%、日内振幅 ≥35%、上影 ≥15% 或已经完成 +200% 的候选。
4. **退出观察：** 纸面路径使用 -20% 止损、+100% 卖一半、剩余 25% 追踪、最长 30 天；3 天 MFE <10% 标记 `failure_to_launch`，但现阶段不自动平仓。
5. **升级条件：** 真正未来 OOS 必须同时出现精确率增量、超过一半时间折盈利、成本后利润因子 >1、周块/币种 bootstrap 期望下界 >0，且删掉最大赢家后仍为正。当前未满足。

### 已验证、方向性与尚未验证

- **已验证：** 多日形态确认和单根 4h 微观模型没有提高识别；风险等额后 -20% 仍是校准选择；没有规则通过纸面交易门槛。
- **方向性证据：** 98.5% 分位可作为优先标签；真正赢家通常在 44–64 小时内跨过 +50%/+100%；3 天未启动可作为风险标记。
- **尚未验证：** 逐档试仓、盘口深度恢复、链上净流入与独立买方确认。现有历史数据不足以对这些字段做无偏全市场回测，必须由前瞻快照积累。
{END}"""
    report_path = report / "REPORT.md"
    report_path.write_text(replace_section(report_path.read_text(encoding="utf-8"), section), encoding="utf-8")

    summary_path = report / "analysis_summary.json"
    main_summary = json.loads(summary_path.read_text(encoding="utf-8"))
    main_summary["entry_exit_timing"] = timing
    main_summary["microstructure_confirmation"] = micro
    main_summary["rank_threshold_validation"] = rank
    main_summary["path_timing"] = path_timing
    main_summary["stop_width_validation"] = stops
    summary_path.write_text(json.dumps(main_summary, ensure_ascii=False, indent=2), encoding="utf-8")

    model_card_path = report / "MODEL_CARD.md"
    model_section = f"""{BEGIN}
## Entry and exit timing extension

- The price model and eight frozen factors are unchanged. Eleven timing rules, one fixed one-bar microstructure model, six score thresholds, eleven stateful exits, and four risk-equal stop widths were tested.
- Rule calibration reads only outcomes matured before fold 3. Folds 3-6 are historical confirmation and are not described as untouched OOS.
- No timing or exit rule passed the paper-trading gate. Deployment remains `watchlist_only`; 98.5% is an annotation, not a replacement ranking threshold.
- The reference paper path remains next 4h open, -20% hard stop, half at +100%, 25% trailing remainder, 30-day maximum hold. It is not connected to execution.
{END}"""
    model_card_path.write_text(
        replace_section(model_card_path.read_text(encoding="utf-8"), model_section), encoding="utf-8"
    )

    path_rows: list[dict[str, Any]] = []
    for _, row in path_summary.iterrows():
        for days in (1, 3, 5, 7, 14):
            path_rows.append(
                {
                    "cohort": row["cohort"],
                    "horizon_days": days,
                    "median_mfe": float(row[f"median_mfe_{days}d"]),
                    "median_mae": float(row[f"median_mae_{days}d"]),
                    "signals": int(row["signals"]),
                }
            )
    path_frame = pd.DataFrame(path_rows)

    artifact_path = report / "artifact.json"
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    manifest = artifact["manifest"]
    snapshot = artifact["snapshot"]
    for key in ("sources", "charts", "tables", "blocks"):
        manifest[key] = [item for item in manifest.get(key, []) if not str(item.get("id", "")).startswith("timing-")]
    for key in list(snapshot["datasets"]):
        if key.startswith("timing_"):
            snapshot["datasets"].pop(key)
    source = {
        "id": "timing-validation",
        "label": "Time-isolated entry and exit timing validation",
        "path": "entry_exit_timing_decision.json",
        "query": {
            "engine": "duckdb",
            "language": "sql",
            "sql": "SELECT * FROM read_csv_auto('entry_exit_confirmation_comparison.csv') ORDER BY strategy",
            "description": "Frozen price candidates, 4h confirmation paths, stateful exits, rank thresholds, and risk-equal stops; selections mature before fold 3 and confirm on folds 3-6.",
            "tables_used": [
                "entry_timing_calibration.csv",
                "microstructure_confirmation_folds.csv",
                "rank_threshold_confirmation.csv",
                "path_timing_summary.csv",
                "stop_width_confirmation.csv",
                "entry_exit_confirmation_folds.csv",
            ],
            "filters": [
                "Binance USD-M perpetuals",
                "frozen price rank first crossing with maximum three signals per day",
                "calibration outcomes must mature before 2026-03-24",
                "confirmation uses historical folds 3-6",
                "fee 5 bps per side with 5/15/30 bps slippage stress",
            ],
            "metric_definitions": [
                "Actual +200% precision is the share of entries whose following 14-day high reaches three times the actual entry open.",
                "Risk-equal stop comparisons size notional as 0.5% of equity divided by stop width.",
                "Profit factor is gross positive trade returns divided by absolute gross negative trade returns.",
            ],
        },
    }
    manifest["sources"].append(source)
    artifact["sources"] = manifest["sources"]
    snapshot["datasets"]["timing_entry_rules"] = clean_records(entry_calibration)
    snapshot["datasets"]["timing_micro_folds"] = clean_records(micro_folds)
    snapshot["datasets"]["timing_thresholds"] = clean_records(thresholds)
    snapshot["datasets"]["timing_threshold_folds"] = clean_records(threshold_folds)
    snapshot["datasets"]["timing_stops"] = clean_records(stop_frame)
    snapshot["datasets"]["timing_path_mfe"] = clean_records(path_frame)
    snapshot["datasets"]["timing_strategy_folds"] = clean_records(timing_folds)
    snapshot["datasets"]["timing_comparison"] = clean_records(comparison)
    manifest["charts"].extend(
        [
            {
                "id": "timing-entry-precision",
                "title": "Calibration precision by entry timing rule",
                "type": "bar",
                "dataset": "timing_entry_rules",
                "encodings": {
                    "x": {"field": "entry_rule", "type": "nominal", "title": "Entry rule"},
                    "y": {"field": "actual_200pct_precision", "type": "quantitative", "title": "Actual +200% precision"},
                },
                "sourceId": "timing-validation",
            },
            {
                "id": "timing-threshold-precision",
                "title": "Confirmation precision by frozen score threshold",
                "type": "bar",
                "dataset": "timing_thresholds",
                "encodings": {
                    "x": {"field": "rank_threshold", "type": "nominal", "title": "Rank threshold"},
                    "y": {"field": "actual_200pct_precision", "type": "quantitative", "title": "Actual +200% precision"},
                },
                "sourceId": "timing-validation",
            },
            {
                "id": "timing-path-mfe",
                "title": "Median maximum favorable excursion after entry",
                "type": "line",
                "dataset": "timing_path_mfe",
                "encodings": {
                    "x": {"field": "horizon_days", "type": "quantitative", "title": "Days after entry"},
                    "y": {"field": "median_mfe", "type": "quantitative", "title": "Median MFE"},
                    "color": {"field": "cohort", "type": "nominal", "title": "Outcome cohort"},
                },
                "sourceId": "timing-validation",
            },
            {
                "id": "timing-stop-expectancy",
                "title": "Risk-equal expectancy by hard-stop width",
                "type": "bar",
                "dataset": "timing_stops",
                "encodings": {
                    "x": {"field": "hard_stop_pct", "type": "nominal", "title": "Hard-stop width"},
                    "y": {"field": "expectancy", "type": "quantitative", "title": "Expectancy"},
                },
                "sourceId": "timing-validation",
            },
            {
                "id": "timing-fold-pf",
                "title": "Calibration-selected timing strategy profit factor by fold",
                "type": "bar",
                "dataset": "timing_strategy_folds",
                "encodings": {
                    "x": {"field": "fold", "type": "nominal", "title": "Historical confirmation fold"},
                    "y": {"field": "profit_factor", "type": "quantitative", "title": "Profit factor"},
                },
                "sourceId": "timing-validation",
            },
        ]
    )
    manifest["tables"].append(
        {
            "id": "timing-comparison-table",
            "title": "Entry and exit confirmation comparison",
            "dataset": "timing_comparison",
            "columns": [
                {"field": "strategy", "label": "Strategy", "type": "text"},
                {"field": "trades", "label": "Trades", "type": "number"},
                {"field": "expectancy", "label": "Expectancy", "type": "percent"},
                {"field": "profit_factor", "label": "Profit factor", "type": "number"},
                {"field": "win_rate", "label": "Win rate", "type": "percent"},
                {"field": "hard_stop_exit_rate", "label": "Hard-stop rate", "type": "percent"},
                {"field": "max_drawdown", "label": "Max drawdown", "type": "percent"},
            ],
            "defaultSort": {"field": "expectancy", "direction": "desc"},
            "sourceId": "timing-validation",
        }
    )
    manifest["blocks"].extend(
        [
            {
                "id": "timing-summary",
                "type": "markdown",
                "sourceId": "timing-validation",
                "body": (
                    "## Timing confirmation did not create a tradable entry rule\n\n"
                    f"The calibration-selected one-bar wait retained {selected_rec['actual_positive_retention_vs_next_open']:.1%} of actual +200% entries but reduced precision from {base_rec['actual_200pct_precision']:.2%} to {selected_rec['actual_200pct_precision']:.2%}. "
                    f"Its selected exit produced {selected_trade['expectancy']:.2%} expectancy and PF {selected_trade['profit_factor']:.2f}, with only {timing['fold_majority_profit_factor_gt_1']:.0%} of folds above PF 1."
                ),
            },
            {"id": "timing-entry-chart-block", "type": "chart", "chartId": "timing-entry-precision"},
            {
                "id": "timing-microstructure",
                "type": "markdown",
                "sourceId": "timing-validation",
                "body": (
                    "## One-bar volume and taker demand reduced precision\n\n"
                    f"The expanding one-bar model reduced AP from {micro_metrics['price_average_precision']:.4f} to {micro_metrics['micro_average_precision']:.4f}. Its selected signals achieved only {micro_metrics['selected_precision']:.2%} actual +200% precision, so 4h microstructure remains annotation-only."
                ),
            },
            {
                "id": "timing-threshold",
                "type": "markdown",
                "sourceId": "timing-validation",
                "body": (
                    "## The 98.5th percentile is a priority tag, not a replacement threshold\n\n"
                    f"Pooled precision rose from {rank_base['actual_200pct_precision']:.2%} at 98% to {rank_selected['actual_200pct_precision']:.2%} at 98.5%, with the same positive count. Only {rank['confirmation_fold_precision_improvement_rate']:.0%} of folds improved and trading expectancy fell, so the main 98% watch pool remains intact."
                ),
            },
            {"id": "timing-threshold-chart-block", "type": "chart", "chartId": "timing-threshold-precision"},
            {
                "id": "timing-path",
                "type": "markdown",
                "sourceId": "timing-validation",
                "body": (
                    "## Actual winners usually launch within two to three days\n\n"
                    f"Median time to +50% was {launch['median_hours_to_50pct']:.0f} hours and to +100% was {launch['median_hours_to_100pct']:.0f} hours. Yet {stop_risk['winner_share_stop20_before_50pct']:.1%} of eventual +200% paths touched -20% first, so early failure exits trade faster loss removal against false exits."
                ),
            },
            {"id": "timing-path-chart-block", "type": "chart", "chartId": "timing-path-mfe"},
            {
                "id": "timing-exit",
                "type": "markdown",
                "sourceId": "timing-validation",
                "body": (
                    "## Risk-equal stop calibration kept the 20% hard stop\n\n"
                    f"After sizing every stop width to the same 0.5% equity risk, the 20% stop remained selected. Confirmation PF was {stop_selected['profit_factor']:.2f}, but only {stops['selected_fold_majority_profit_factor_gt_1']:.0%} of folds were profitable and both clustered-bootstrap expectancy lower bounds stayed below zero."
                ),
            },
            {"id": "timing-stop-chart-block", "type": "chart", "chartId": "timing-stop-expectancy"},
            {"id": "timing-fold-chart-block", "type": "chart", "chartId": "timing-fold-pf"},
            {"id": "timing-comparison-table-block", "type": "table", "tableId": "timing-comparison-table"},
            {
                "id": "timing-protocol",
                "type": "markdown",
                "sourceId": "timing-validation",
                "body": (
                    "## Frozen forward protocol\n\n"
                    "Keep price rank >=98% as the watch pool and mark >=98.5% as priority only. Use the next scheduled 4h open as the reference price. Do not let 4h volume, taker demand, retests, or failure-to-launch raise rank or trigger trading. The paper path remains -20% hard stop, half at +100%, 25% trailing remainder, and a 30-day maximum hold. All signals remain watchlist-only until true future OOS clears the unchanged uncertainty and concentration gates."
                ),
            },
        ]
    )
    generated = timing["generated_at_utc"]
    manifest["generatedAt"] = generated
    snapshot["generatedAt"] = generated
    artifact_path.write_text(json.dumps(artifact, ensure_ascii=True, indent=2), encoding="utf-8")

    chart_map_path = report / "CHART_MAP.md"
    chart_section = f"""{BEGIN}
## Entry and exit timing charts

- `timing-entry-precision`: comparison/bar; actual post-entry +200% precision by pre-registered timing rule; source `entry_timing_calibration.csv`.
- `timing-threshold-precision`: comparison/bar; confirmation precision across frozen rank thresholds; source `rank_threshold_confirmation.csv`.
- `timing-path-mfe`: trend/line; median MFE by outcome cohort and horizon; source `path_timing_summary.csv`.
- `timing-stop-expectancy`: comparison/bar; risk-equal expectancy by hard-stop width; source `stop_width_confirmation.csv`.
- `timing-fold-pf`: comparison/bar; selected timing strategy PF by fold; source `entry_exit_confirmation_folds.csv`.
{END}"""
    chart_map_path.write_text(
        replace_section(chart_map_path.read_text(encoding="utf-8"), chart_section), encoding="utf-8"
    )
    update_notebook(report)
    print(artifact_path)


if __name__ == "__main__":
    main()
