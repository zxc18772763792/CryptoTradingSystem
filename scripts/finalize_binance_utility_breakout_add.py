"""Finalize the risk-neutral breakout add-on falsification."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
import nbformat
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
BEGIN = "<!-- UTILITY_BREAKOUT_ADD_BEGIN -->"
END = "<!-- UTILITY_BREAKOUT_ADD_END -->"


def replace_section(text: str, section: str) -> str:
    if BEGIN in text and END in text:
        start = text.index(BEGIN)
        finish = text.index(END, start) + len(END)
        return text[:start].rstrip() + "\n\n" + section + "\n" + text[finish:].lstrip()
    return text.rstrip() + "\n\n" + section + "\n"


def make_chart(decision: dict, calibration: pd.DataFrame, summary: pd.DataFrame) -> None:
    figure, axes = plt.subplots(1, 3, figsize=(18, 5.2))
    fractions = calibration["breakout_add_fraction"].to_numpy(float)
    labels = [f"{value:.0%}" for value in fractions]
    colors = ["#475569" if np.isclose(value, 0.0) else "#3b6fb6" for value in fractions]

    axes[0].bar(labels, calibration["paired_increment_mean"], color=colors, edgecolor="#334155", linewidth=0.6)
    axes[0].axhline(0, color="#0f172a", linewidth=0.9)
    axes[0].set_ylim(float(calibration["paired_increment_mean"].min()) * 1.18, 0.003)
    axes[0].set_xlabel("Planned fraction reserved for breakout add")
    axes[0].set_ylabel("Paired return increment")
    axes[0].set_title("Calibration paired return increment")
    for index, value in enumerate(calibration["paired_increment_mean"]):
        if not np.isclose(value, 0.0):
            axes[0].text(index, value - 0.0012, f"{value:.1%}", ha="center", va="top", fontsize=8)

    axes[1].bar(labels, summary["expectancy"], color=colors, edgecolor="#334155", linewidth=0.6)
    axes[1].set_ylim(0, float(summary["expectancy"].max()) * 1.25)
    axes[1].set_xlabel("Planned fraction reserved for breakout add")
    axes[1].set_ylabel("Portfolio trade expectancy")
    axes[1].set_title("Confirmation expectancy and profit factor")
    for index, row in summary.reset_index(drop=True).iterrows():
        axes[1].text(index, row["expectancy"] + 0.005, f"{row['expectancy']:.1%}\nPF {row['profit_factor']:.2f}", ha="center", fontsize=8)

    interval_labels: list[str] = []
    centers: list[float] = []
    lower: list[float] = []
    upper: list[float] = []
    for fraction in [0.1, 0.25, 0.5]:
        detail = decision["confirmation_details"][str(fraction)]["event_week_increment_bootstrap"]
        interval_labels.append(f"{fraction:.0%} reserve")
        centers.append(float(detail["mean_delta_median"]))
        lower.append(float(detail["mean_delta_lower_95pct"]))
        upper.append(float(detail["mean_delta_upper_95pct"]))
    centres = np.asarray(centers)
    lows = np.asarray(lower)
    highs = np.asarray(upper)
    x = np.arange(len(centres))
    axes[2].errorbar(
        x,
        centres,
        yerr=np.vstack([centres - lows, highs - centres]),
        fmt="o",
        color="#3b6fb6",
        ecolor="#64748b",
        capsize=6,
    )
    axes[2].axhline(0, color="#0f172a", linewidth=0.9)
    axes[2].set_xticks(x, interval_labels)
    axes[2].set_ylabel("Independent-event paired increment")
    axes[2].set_title("Confirmation event-week intervals")

    figure.suptitle("Risk-neutral breakout add allocation diagnostic", fontsize=14, fontweight="bold")
    figure.tight_layout()
    chart = REPORT / "charts" / "utility_breakout_add.png"
    chart.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(chart, dpi=180, bbox_inches="tight")
    plt.close(figure)


def update_notebook(decision: dict, summary: pd.DataFrame) -> None:
    path = REPORT / "binance_runup_research.ipynb"
    notebook = nbformat.read(path, as_version=4)
    notebook.cells = [
        cell for cell in notebook.cells
        if "utility-breakout-add" not in cell.get("metadata", {}).get("tags", [])
    ]
    indexed = summary.set_index("breakout_add_fraction")
    baseline = indexed.loc[0.0]
    reserve10 = indexed.loc[0.1]
    notebook.cells.append(
        nbformat.v4.new_markdown_cell(
            f"""## Risk-neutral breakout add allocation

The total planned position and maximum risk remain fixed. Calibration selects zero reserve: every 10%/25%/50% breakout-add allocation has a negative paired increment in all three calibration windows. Confirmation agrees. Reserving 10% lowers expectancy from {baseline['expectancy']:.2%} to {reserve10['expectancy']:.2%} even though PF increases from {baseline['profit_factor']:.2f} to {reserve10['profit_factor']:.2f}; the apparent risk improvement comes from lower average deployment, not additional alpha. The result retains the full planned 8h entry and rejects breakout pyramiding. Breakout remains a profit-side hold/exit router only.""",
            metadata={"tags": ["utility-breakout-add"]},
        )
    )
    nbformat.write(notebook, path)


def main() -> None:
    verification = json.loads((REPORT / "utility_breakout_add_verification.json").read_text(encoding="utf-8"))
    if not verification.get("passed"):
        raise RuntimeError("Breakout-add verifier did not pass")
    decision = json.loads((REPORT / "utility_breakout_add_decision.json").read_text(encoding="utf-8"))
    calibration = pd.read_csv(REPORT / "utility_breakout_add_calibration.csv")
    summary = pd.read_csv(REPORT / "utility_breakout_add_summary.csv")
    make_chart(decision, calibration, summary)
    update_notebook(decision, summary)

    indexed = summary.set_index("breakout_add_fraction")
    baseline = indexed.loc[0.0]
    reserve10 = indexed.loc[0.1]
    reserve25 = indexed.loc[0.25]
    reserve50 = indexed.loc[0.5]
    detail10 = decision["confirmation_details"]["0.1"]
    section = f"""{BEGIN}
## 突破可以管理赢家，但不值得为它预留加仓资金

突破确认组的收益明显高于未突破组，因此本轮检验了一个自然延伸：总计划风险保持不变，在8小时买点只投入50%/75%/90%，把剩余部分留到24小时内 `breakout6` 确认后的下一根4小时开盘再加；如果没有触发，预留部分保持现金。每个子仓独立计入手续费、资金费率、止损与盈利退出，组合仍使用最多10仓、总名义敞口50%的保守约束，不增加杠杆。

更早的42条校准信号包含11次突破机会。校准没有选择任何预留比例：10%/25%/50%加仓方案的逐信号增量分别为 **-0.86%/-2.15%/-4.30%**，三个校准窗口的平均增量全部为负；50%预留在删除最大赢家后期望转为负值。因此校准结论是 **8小时买点投入100%，突破加仓0%**。

后续88条确认信号给出相同方向：

- 全仓在8小时买入：65笔、期望 **{baseline['expectancy']:.2%}**、PF **{baseline['profit_factor']:.2f}**、组合收益 **{baseline['total_return_on_initial_equity']:.2%}**、最大回撤 {baseline['max_drawdown']:.2%}；
- 预留10%：期望 {reserve10['expectancy']:.2%}、PF {reserve10['profit_factor']:.2f}、组合收益 {reserve10['total_return_on_initial_equity']:.2%}，配对增量 **{reserve10['paired_increment_mean']:.2%}**；
- 预留25%：期望 {reserve25['expectancy']:.2%}、PF {reserve25['profit_factor']:.2f}、配对增量 **{reserve25['paired_increment_mean']:.2%}**；
- 预留50%：期望 {reserve50['expectancy']:.2%}、PF {reserve50['profit_factor']:.2f}、配对增量 **{reserve50['paired_increment_mean']:.2%}**。

加仓子仓本身平均仍赚 {detail10['mean_add_leg_return']:.2%}，但它买得更晚；对已突破信号，预留10%再加也使总体收益少 {abs(detail10['breakout_segment_increment']):.2%}。对未突破信号，长期保留10%现金又少 {abs(detail10['nonbreakout_segment_increment']):.2%}。所以PF从2.79逐步提高到3.08、回撤从-4.55%收窄到-2.39%，本质是**降低平均资金投入**，不是产生新的超额收益。

结论进一步收敛了买点：当8小时效用分数通过且阶段仍为 `price_watch` 时，不应为了等待突破而少买，也不应在突破后追加入场；计划纸面仓位在8小时决策开盘一次完成。突破只保留为后续盈利侧退出路由。该加仓方案未加入前瞻标签，不改变主买点、主退出、排名或交易权限。

![突破加仓分配验证](charts/utility_breakout_add.png)
{END}"""
    report_path = REPORT / "REPORT.md"
    report_path.write_text(replace_section(report_path.read_text(encoding="utf-8"), section), encoding="utf-8")

    analysis_path = REPORT / "analysis_summary.json"
    analysis = json.loads(analysis_path.read_text(encoding="utf-8"))
    analysis["utility_breakout_add"] = decision
    analysis_path.write_text(json.dumps(analysis, ensure_ascii=False, indent=2), encoding="utf-8")

    model_section = f"""{BEGIN}
## Risk-neutral breakout add allocation

- Planned risk is fixed. The catalog reserves 0%, 10%, 25%, or 50% of the 8h position for a next-open six-bar breakout add; untriggered reserve stays in cash.
- Calibration selects 0% reserve: every non-zero fraction has negative paired increment in all three earlier calibration windows.
- Confirmation also rejects add-ons. The 10% reserve lowers expectancy from {baseline['expectancy']:.2%} to {reserve10['expectancy']:.2%}; larger reserves reduce it further. Higher PF and smaller drawdown are explained by lower average deployment.
- Full planned 8h entry remains primary. Breakout is a hold/exit annotation only; no forward add-on, primary-rule change, or automatic-trading authority is added.
{END}"""
    model_path = REPORT / "MODEL_CARD.md"
    model_path.write_text(replace_section(model_path.read_text(encoding="utf-8"), model_section), encoding="utf-8")

    chart_section = f"""{BEGIN}
## Risk-neutral breakout add allocation

- `charts/utility_breakout_add.png`: calibration allocation increments, confirmation expectancy/PF, and independent-event uncertainty for reserved add fractions.
{END}"""
    chart_path = REPORT / "CHART_MAP.md"
    chart_path.write_text(replace_section(chart_path.read_text(encoding="utf-8"), chart_section), encoding="utf-8")
    print(json.dumps({
        "updated": True,
        "selected_breakout_add_fraction": decision["selected_breakout_add_fraction"],
        "classification": decision["classification"],
        "primary_entry_changed": False,
    }))


if __name__ == "__main__":
    main()
