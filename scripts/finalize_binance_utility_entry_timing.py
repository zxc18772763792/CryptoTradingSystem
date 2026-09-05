"""Finalize the frozen-8h utility entry-execution timing study."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
import nbformat
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
BEGIN = "<!-- UTILITY_ENTRY_TIMING_BEGIN -->"
END = "<!-- UTILITY_ENTRY_TIMING_END -->"


def replace_section(text: str, section: str) -> str:
    if BEGIN in text and END in text:
        start = text.index(BEGIN)
        finish = text.index(END, start) + len(END)
        return text[:start].rstrip() + "\n\n" + section + "\n" + text[finish:].lstrip()
    return text.rstrip() + "\n\n" + section + "\n"


def make_chart(decision: dict, calibration: pd.DataFrame) -> None:
    selected_rule = decision["selected_entry_rule"]
    selected_rec = decision["confirmation_recognition_selected"]
    baseline_rec = decision["confirmation_recognition_baseline_all"]
    boot = decision["filter_precision_bootstraps"]
    figure, axes = plt.subplots(1, 3, figsize=(18, 5.2))

    colors = ["#f59e0b" if rule == selected_rule else "#94a3b8" for rule in calibration["entry_rule"]]
    sizes = 50 + 25 * calibration["actual_200pct_entries"].to_numpy(float)
    axes[0].scatter(
        calibration["fill_rate"],
        calibration["actual_200pct_precision"],
        s=sizes,
        c=colors,
        alpha=0.9,
        edgecolor="white",
        linewidth=0.8,
    )
    for _, row in calibration.iterrows():
        if row["entry_rule"] in {"launch_open", selected_rule, "discount5_touch", "breakout6"}:
            launch_open = row["entry_rule"] == "launch_open"
            axes[0].annotate(
                str(row["entry_rule"]),
                (row["fill_rate"], row["actual_200pct_precision"]),
                xytext=(-5 if launch_open else 5, 5),
                textcoords="offset points",
                ha="right" if launch_open else "left",
                fontsize=8,
            )
    axes[0].set_xlabel("Calibration fill rate")
    axes[0].set_ylabel("Calibration +200% precision")
    axes[0].set_title("Entry-rule calibration")

    labels = ["Signal rows", "Independent events"]
    baseline_values = [baseline_rec["actual_200pct_precision"], baseline_rec["event_precision"]]
    selected_values = [selected_rec["actual_200pct_precision"], selected_rec["event_precision"]]
    x = np.arange(2)
    width = 0.36
    axes[1].bar(x - width / 2, baseline_values, width, label="8h score open", color="#64748b")
    axes[1].bar(x + width / 2, selected_values, width, label="5% dip + reclaim", color="#f59e0b")
    axes[1].set_xticks(x, labels)
    axes[1].set_ylabel("+200% precision")
    axes[1].set_title("Confirmation recognition")
    axes[1].legend(frameon=False)
    for index, value in enumerate(selected_values):
        axes[1].text(index + width / 2, value + 0.003, f"{value:.1%}", ha="center", fontsize=9)

    interval_keys = ["row_week", "row_symbol", "event_week", "event_symbol"]
    interval_labels = ["Row/week", "Row/symbol", "Event/week", "Event/symbol"]
    centers = np.asarray([boot[key]["precision_delta_median"] for key in interval_keys])
    lower = np.asarray([boot[key]["precision_delta_lower_95pct"] for key in interval_keys])
    upper = np.asarray([boot[key]["precision_delta_upper_95pct"] for key in interval_keys])
    axes[2].errorbar(
        np.arange(4),
        centers,
        yerr=np.vstack([centers - lower, upper - centers]),
        fmt="o",
        color="#7c3aed",
        capsize=6,
    )
    axes[2].axhline(0, color="#0f172a", linewidth=0.9)
    axes[2].set_xticks(np.arange(4), interval_labels, rotation=15)
    axes[2].set_ylabel("Precision increment")
    axes[2].set_title("Event evidence passes; row evidence does not")

    figure.suptitle("Entry timing after the frozen P0+8h utility score", fontsize=14, fontweight="bold")
    figure.tight_layout()
    chart = REPORT / "charts" / "utility_entry_timing.png"
    chart.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(chart, dpi=180, bbox_inches="tight")
    plt.close(figure)


def update_notebook(decision: dict) -> None:
    path = REPORT / "binance_runup_research.ipynb"
    notebook = nbformat.read(path, as_version=4)
    notebook.cells = [
        cell for cell in notebook.cells
        if "utility-entry-timing" not in cell.get("metadata", {}).get("tags", [])
    ]
    baseline = decision["confirmation_recognition_baseline_all"]
    selected = decision["confirmation_recognition_selected"]
    summary = decision["selected_strategy_summary"]
    notebook.cells.append(
        nbformat.v4.new_markdown_cell(
            f"""## Entry execution after the frozen 8h utility score

Calibration selects a 5% discount followed by a positive higher-close reclaim, with entry at the next 4h open within a 24h trigger window. Confirmation filters 88 score-open signals to 54, raises row precision from {baseline['actual_200pct_precision']:.2%} to {selected['actual_200pct_precision']:.2%}, and retains all five positive independent events. Event-cluster precision intervals are positive, but row-cluster intervals cross zero. The selected path has {summary['expectancy']:.2%} expectancy and PF {summary['profit_factor']:.2f}; absolute and paired trade intervals cross zero. It is prospective-only and does not replace score-open entry.""",
            metadata={"tags": ["utility-entry-timing"]},
        )
    )
    nbformat.write(notebook, path)


def main() -> None:
    verification = json.loads((REPORT / "utility_entry_timing_verification.json").read_text(encoding="utf-8"))
    if not verification.get("passed"):
        raise RuntimeError("Utility entry-timing verifier did not pass")
    decision = json.loads((REPORT / "utility_entry_timing_decision.json").read_text(encoding="utf-8"))
    calibration = pd.read_csv(REPORT / "utility_entry_timing_calibration.csv")
    make_chart(decision, calibration)
    update_notebook(decision)

    baseline = decision["confirmation_recognition_baseline_all"]
    matched = decision["confirmation_recognition_baseline_matched"]
    selected = decision["confirmation_recognition_selected"]
    selected_summary = decision["selected_strategy_summary"]
    matched_summary = decision["baseline_strategy_summary_matched"]
    all_summary = decision["baseline_strategy_summary_all"]
    boot = decision["filter_precision_bootstraps"]
    section = f"""{BEGIN}
## 8小时评分后的买点：5%回撤再收盘回升，只能作为前瞻过滤器

这轮把“8小时分数达到 q70 后立即在决策开盘买入”与九种时间安全的执行方式分开比较。所有非立即规则最多观察后续24小时；凡依赖回撤、收盘、突破或回升的条件，都等触发 K 线完整收盘后，在下一根4小时开盘成交。规则只在三段校准窗口选择，确认折仍为第3–6折。

校准唯一合格的是 `discount5_reclaim`：从8小时决策开盘价至少回撤5%，随后出现阳线且收盘高于前一根收盘，再在下一根4小时开盘买入。确认期结果：

- 88条原始8小时信号中触发54条，触发率 {selected['fill_rate']:.2%}，中位等待 {selected['median_delay_hours']:.0f} 小时，中位买价比8小时开盘低 {abs(selected['median_entry_return_vs_score_open']):.2%}；
- 逐行 +200% 命中从 {baseline['actual_200pct_entries']}/{baseline['filled_entries']}={baseline['actual_200pct_precision']:.2%} 提高到 {selected['actual_200pct_entries']}/{selected['filled_entries']}={selected['actual_200pct_precision']:.2%}，保留 {selected['positive_retention']:.0%} 的正行；
- 合并同币14天重复信号后，从 {baseline['positive_independent_events']}/{baseline['independent_events']}={baseline['event_precision']:.2%} 提高到 {selected['positive_independent_events']}/{selected['independent_events']}={selected['event_precision']:.2%}，5个正事件全部保留；
- 事件口径按周/按币的精度增量95%下界分别为 {boot['event_week']['precision_delta_lower_95pct']:.2%}/{boot['event_symbol']['precision_delta_lower_95pct']:.2%}，均大于0；但逐行下界为 {boot['row_week']['precision_delta_lower_95pct']:.2%}/{boot['row_symbol']['precision_delta_lower_95pct']:.2%}，仍略低于0。

54个已触发信号与其8小时立即入场版本逐一配对后，未来 +200% 标签完全相同。这说明命中率提高来自“回撤后能够重新收回”的过滤作用，不是便宜约2%的买价人为制造了更多 +200% 标签。

交易层没有通过替换门槛：过滤后的44笔组合期望 {selected_summary['expectancy']:.2%}、PF {selected_summary['profit_factor']:.2f}，优于相同54条信号立即入场的 {matched_summary['expectancy']:.2%}/PF {matched_summary['profit_factor']:.2f}；配对单笔增量为 {decision['paired_trade_increment_mean']:.2%}。但按周/按币的配对下界仍为 {decision['week_trade_increment_bootstrap']['mean_delta_lower_95pct']:.2%}/{decision['symbol_trade_increment_bootstrap']['mean_delta_lower_95pct']:.2%}，绝对期望下界也小于0，第4折 PF 只有 {decision['strategy_fold_metrics'][1]['profit_factor']:.2f}。而全部信号立即入场的历史点估计仍更高：期望 {all_summary['expectancy']:.2%}、PF {all_summary['profit_factor']:.2f}。

因此主买点继续冻结为 **P0+8h 分数通过后的决策开盘**。`discount5_reclaim_24h_next_open` 已加入未来30日只读标签，只作为“如果等回撤确认会怎样”的前瞻对照，不改变排名、原阶段否决或自动交易权限。只有真正 OOS 同时通过逐行、独立事件和交易配对下界，才允许升级。

![8小时评分后的买点验证](charts/utility_entry_timing.png)
{END}"""
    report_path = REPORT / "REPORT.md"
    report_path.write_text(replace_section(report_path.read_text(encoding="utf-8"), section), encoding="utf-8")

    summary_path = REPORT / "analysis_summary.json"
    analysis_summary = json.loads(summary_path.read_text(encoding="utf-8"))
    analysis_summary["utility_entry_timing"] = decision
    summary_path.write_text(json.dumps(analysis_summary, ensure_ascii=False, indent=2), encoding="utf-8")

    model_section = f"""{BEGIN}
## Entry execution after the frozen 8h utility score

- Calibration selects a 5% discount followed by a positive higher-close reclaim, entered at the next 4h open within a 24h trigger window.
- Confirmation filters 88 score-open signals to 54; row precision rises from {baseline['actual_200pct_precision']:.2%} to {selected['actual_200pct_precision']:.2%} with 80% positive-row retention.
- Independent-event precision rises from {baseline['event_precision']:.2%} to {selected['event_precision']:.2%}, retaining all five positive events; event-cluster lower bounds are positive but row-cluster lower bounds cross zero.
- Matched +200% labels are identical, so enrichment is a fill/filter effect rather than entry-price label manufacture.
- Trade expectancy is {selected_summary['expectancy']:.2%}, PF {selected_summary['profit_factor']:.2f}; absolute and paired clustered lower bounds cross zero. Score-open remains primary.
- `discount5_reclaim_24h_next_open` is recorded in utility forward labels only, without rank, stage, or automatic-trading authority.
{END}"""
    model_path = REPORT / "MODEL_CARD.md"
    model_path.write_text(replace_section(model_path.read_text(encoding="utf-8"), model_section), encoding="utf-8")

    chart_section = f"""{BEGIN}
## Utility entry timing

- `charts/utility_entry_timing.png`: calibration fill/precision frontier, confirmation row/event enrichment, and clustered precision-increment intervals.
{END}"""
    chart_path = REPORT / "CHART_MAP.md"
    chart_path.write_text(replace_section(chart_path.read_text(encoding="utf-8"), chart_section), encoding="utf-8")
    print(
        json.dumps(
            {
                "updated": True,
                "selected_entry_rule": decision["selected_entry_rule"],
                "recognition_gate": decision["recognition_filter_gate_pass"],
                "trade_gate": decision["entry_timing_gate_pass"],
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
