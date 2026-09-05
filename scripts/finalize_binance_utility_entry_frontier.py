"""Finalize the full fixed entry-rule frontier diagnostic."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
import nbformat
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
BEGIN = "<!-- UTILITY_ENTRY_FRONTIER_BEGIN -->"
END = "<!-- UTILITY_ENTRY_FRONTIER_END -->"


def replace_section(text: str, section: str) -> str:
    if BEGIN in text and END in text:
        start = text.index(BEGIN)
        finish = text.index(END, start) + len(END)
        return text[:start].rstrip() + "\n\n" + section + "\n" + text[finish:].lstrip()
    return text.rstrip() + "\n\n" + section + "\n"


def make_chart(decision: dict, summary: pd.DataFrame, calibration: pd.DataFrame) -> None:
    indexed = summary.set_index("entry_rule")
    cal = calibration.set_index("entry_rule")
    figure, axes = plt.subplots(1, 3, figsize=(18, 5.2))

    colors = []
    for rule in summary["entry_rule"]:
        if rule == "breakout6":
            colors.append("#dc2626")
        elif rule == "discount5_reclaim":
            colors.append("#f59e0b")
        elif rule == "launch_open":
            colors.append("#2563eb")
        else:
            colors.append("#94a3b8")
    axes[0].scatter(summary["fill_rate"], summary["actual_200pct_precision"], s=95, c=colors, alpha=0.9)
    for rule in ["launch_open", "discount5_reclaim", "breakout6", "prelaunch_high_break"]:
        row = indexed.loc[rule]
        launch = rule == "launch_open"
        axes[0].annotate(
            rule,
            (row["fill_rate"], row["actual_200pct_precision"]),
            xytext=(-5 if launch else 5, 5),
            textcoords="offset points",
            ha="right" if launch else "left",
            fontsize=8,
        )
    axes[0].set_xlabel("Confirmation fill rate")
    axes[0].set_ylabel("+200% precision")
    axes[0].set_title("Confirmation recognition frontier")

    rules = ["discount5_reclaim", "breakout6", "prelaunch_high_break"]
    x = np.arange(len(rules))
    width = 0.36
    axes[1].bar(x - width / 2, [cal.loc[r, "actual_200pct_precision"] for r in rules], width, label="Calibration", color="#64748b")
    axes[1].bar(x + width / 2, [indexed.loc[r, "actual_200pct_precision"] for r in rules], width, label="Confirmation", color="#f59e0b")
    axes[1].set_xticks(x, ["5% dip + reclaim", "6-bar breakout", "Prelaunch high break"], rotation=10)
    axes[1].set_ylabel("+200% precision")
    axes[1].set_title("Breakout evidence reverses across periods")
    axes[1].legend(frameon=False)

    interval_labels: list[str] = []
    centers: list[float] = []
    lower: list[float] = []
    upper: list[float] = []
    for rule, short in [("breakout6", "6-bar"), ("prelaunch_high_break", "Prelaunch")]:
        detail = decision["rules"][rule]
        for cluster, label in [("week_trade_increment_bootstrap", "week"), ("symbol_trade_increment_bootstrap", "symbol")]:
            item = detail[cluster]
            interval_labels.append(f"{short}/{label}")
            centers.append(item["mean_delta_median"])
            lower.append(item["mean_delta_lower_95pct"])
            upper.append(item["mean_delta_upper_95pct"])
    centers_array = np.asarray(centers)
    lower_array = np.asarray(lower)
    upper_array = np.asarray(upper)
    axes[2].errorbar(
        np.arange(len(centers)), centers_array,
        yerr=np.vstack([centers_array - lower_array, upper_array - centers_array]),
        fmt="o", color="#7c3aed", capsize=6,
    )
    axes[2].axhline(0, color="#0f172a", linewidth=0.9)
    axes[2].set_xticks(np.arange(len(centers)), interval_labels, rotation=15)
    axes[2].set_ylabel("Matched trade-return increment")
    axes[2].set_title("Waiting for breakout is too late to buy")

    figure.suptitle("Full entry-rule frontier after the frozen P0+8h score", fontsize=14, fontweight="bold")
    figure.tight_layout()
    chart = REPORT / "charts" / "utility_entry_frontier.png"
    chart.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(chart, dpi=180, bbox_inches="tight")
    plt.close(figure)


def update_notebook(decision: dict, summary: pd.DataFrame) -> None:
    path = REPORT / "binance_runup_research.ipynb"
    notebook = nbformat.read(path, as_version=4)
    notebook.cells = [
        cell for cell in notebook.cells
        if "utility-entry-frontier" not in cell.get("metadata", {}).get("tags", [])
    ]
    breakout = summary.set_index("entry_rule").loc["breakout6"]
    notebook.cells.append(
        nbformat.v4.new_markdown_cell(
            f"""## Full entry-rule frontier diagnostic

The six-bar close breakout has {breakout['actual_200pct_precision']:.2%} row precision and improves over score-open in all four confirmation folds, but it had zero calibration positives, misses catalog-level BH significance, and its event-cluster intervals cross zero. More importantly, waiting for the breakout reduces matched trade return by {abs(breakout['paired_trade_increment_mean']):.2%}; both paired upper bounds are negative. Breakout is therefore a possible after-entry hold/recognition annotation, not a buy trigger. No additional forward buy rule is added.""",
            metadata={"tags": ["utility-entry-frontier"]},
        )
    )
    nbformat.write(notebook, path)


def main() -> None:
    verification = json.loads((REPORT / "utility_entry_frontier_verification.json").read_text(encoding="utf-8"))
    if not verification.get("passed"):
        raise RuntimeError("Utility entry-frontier verifier did not pass")
    decision = json.loads((REPORT / "utility_entry_frontier_decision.json").read_text(encoding="utf-8"))
    summary = pd.read_csv(REPORT / "utility_entry_frontier_summary.csv")
    calibration = pd.read_csv(REPORT / "utility_entry_timing_calibration.csv")
    make_chart(decision, summary, calibration)
    update_notebook(decision, summary)

    indexed = summary.set_index("entry_rule")
    breakout = indexed.loc["breakout6"]
    breakout_detail = decision["rules"]["breakout6"]
    prelaunch = indexed.loc["prelaunch_high_break"]
    prelaunch_detail = decision["rules"]["prelaunch_high_break"]
    section = f"""{BEGIN}
## 突破能确认暴涨倾向，但等突破再买已经太晚

为了确认5%回撤规则究竟是不是唯一有信息的路径，这轮把事先固定的九种入场规则在确认期全部展开，并对八个非基准候选同时做 BH 多重检验。该步骤只用于假设生成：校准期没有选中的规则，即使确认期表现突出，也不允许回写成历史通过。

最显眼的是 `breakout6`：完成的4小时K线收盘突破此前六根最高价、当根涨幅0–10%、相对8小时开盘追价不超过25%，下一根开盘进入。它在确认期触发 {int(breakout['filled_entries'])} 条，{int(breakout['actual_200pct_entries'])} 条达到 +200%，逐行精度 **{breakout['actual_200pct_precision']:.2%}**；{int(breakout['independent_events'])} 个独立事件中命中 {int(breakout['positive_independent_events'])} 个，事件精度 **{breakout['event_precision']:.2%}**，四折均高于立即入场。

但它不能升级为识别规则：校准期正例为0；八规则 BH 校正后的逐行/事件 p 值为 {breakout['row_fisher_p_bh']:.3f}/{breakout['event_fisher_p_bh']:.3f}；逐行按周/按币增量下界虽为正，但事件下界仍为 {breakout_detail['precision_bootstraps']['event_week']['precision_delta_lower_95pct']:.2%}/{breakout_detail['precision_bootstraps']['event_symbol']['precision_delta_lower_95pct']:.2%}。样本只有5个正事件，期间反转风险很高。

更重要的是，它回答了“何时买”的方向：**突破适合确认，不适合等待后再追入。** 17笔受组合约束的突破后交易表面期望 {breakout['expectancy']:.2%}、PF {breakout['profit_factor']:.2f}，但与同一批信号在8小时开盘买入逐笔配对后，平均少赚 **{abs(breakout['paired_trade_increment_mean']):.2%}**；按周和按币的配对95%上界分别为 {breakout_detail['week_trade_increment_bootstrap']['mean_delta_upper_95pct']:.2%}/{breakout_detail['symbol_trade_increment_bootstrap']['mean_delta_upper_95pct']:.2%}，都低于0。`prelaunch_high_break` 也在四折提高识别精度，但等待后的配对收益少 {abs(prelaunch['paired_trade_increment_mean']):.2%}，两套上界同样为负（{prelaunch_detail['week_trade_increment_bootstrap']['mean_delta_upper_95pct']:.2%}/{prelaunch_detail['symbol_trade_increment_bootstrap']['mean_delta_upper_95pct']:.2%}）。

因此不新增突破后买入规则。主买点仍是8小时评分通过后的决策开盘；5%回撤回升继续作为已登记的前瞻过滤器。突破结构只保留为“入场后持仓强度/是否给赢家更多空间”的研究线索，下一步只能研究如何管理已持有仓位，不能用它追涨。

![完整买点规则前沿](charts/utility_entry_frontier.png)
{END}"""
    report_path = REPORT / "REPORT.md"
    report_path.write_text(replace_section(report_path.read_text(encoding="utf-8"), section), encoding="utf-8")

    summary_path = REPORT / "analysis_summary.json"
    analysis_summary = json.loads(summary_path.read_text(encoding="utf-8"))
    analysis_summary["utility_entry_frontier"] = decision
    summary_path.write_text(json.dumps(analysis_summary, ensure_ascii=False, indent=2), encoding="utf-8")

    model_section = f"""{BEGIN}
## Full utility-entry frontier diagnostic

- All nine fixed entry rules are inspected with BH control, but confirmation-only winners are ineligible for historical promotion.
- Six-bar breakout has {breakout['actual_200pct_precision']:.2%} row precision, {breakout['event_precision']:.2%} event precision, and improves over score-open in all four folds; it had zero calibration positives, misses BH significance, and event intervals cross zero.
- Waiting for the six-bar breakout reduces matched return by {abs(breakout['paired_trade_increment_mean']):.2%}; both paired 95% upper bounds are negative. The prelaunch-high breakout confirms the same chase penalty.
- Breakout remains a possible after-entry hold-strength annotation, not an entry trigger. No forward buy rule, rank change, or automatic-trading authority is added.
{END}"""
    model_path = REPORT / "MODEL_CARD.md"
    model_path.write_text(replace_section(model_path.read_text(encoding="utf-8"), model_section), encoding="utf-8")

    chart_section = f"""{BEGIN}
## Full utility-entry frontier

- `charts/utility_entry_frontier.png`: confirmation fill/precision frontier, calibration-to-confirmation reversal, and matched chase-penalty intervals.
{END}"""
    chart_path = REPORT / "CHART_MAP.md"
    chart_path.write_text(replace_section(chart_path.read_text(encoding="utf-8"), chart_section), encoding="utf-8")
    print(json.dumps({"updated": True, "prospective_rule": decision["prospective_rule"], "primary_entry_changed": False}))


if __name__ == "__main__":
    main()
