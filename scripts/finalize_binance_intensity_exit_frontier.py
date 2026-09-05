"""Finalize the extreme-intensity and utility-exit frontier in durable artifacts."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
import nbformat
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
BEGIN = "<!-- INTENSITY_EXIT_FRONTIER_BEGIN -->"
END = "<!-- INTENSITY_EXIT_FRONTIER_END -->"


def replace_section(text: str, section: str) -> str:
    if BEGIN in text and END in text:
        start = text.index(BEGIN)
        finish = text.index(END, start) + len(END)
        return text[:start].rstrip() + "\n\n" + section + "\n" + text[finish:].lstrip()
    return text.rstrip() + "\n\n" + section + "\n"


def make_chart(intensity: dict, tail: dict) -> None:
    prior = intensity["prior_binary200_confirmation"]
    current = intensity["confirmation"]
    baseline = tail["baseline_confirmation_summary"]
    challenger = tail["selected_confirmation_summary"]
    figure, axes = plt.subplots(1, 3, figsize=(15.5, 4.8))

    labels = ["Price", "Binary +200", "Ordinal"]
    ap_values = [
        current["price_average_precision"],
        prior["continuation_average_precision"],
        current["intensity_average_precision"],
    ]
    axes[0].bar(labels, ap_values, color=["#94a3b8", "#60a5fa", "#2563eb"])
    axes[0].set_title("Confirmation average precision")
    axes[0].set_ylabel("AP")
    axes[0].set_ylim(0, max(ap_values) * 1.30)
    for index, value in enumerate(ap_values):
        axes[0].text(index, value, f"{value:.3f}", ha="center", va="bottom")

    event = intensity["event_metrics"]
    x = np.arange(2)
    axes[1].bar(x - 0.18, [prior.get("selected_precision", 0), current["selected_precision"]], 0.36, label="Row precision", color="#0ea5e9")
    axes[1].bar(x + 0.18, [prior.get("positive_retention", 0), current["positive_retention"]], 0.36, label="Positive retention", color="#22c55e")
    axes[1].set_xticks(x, ["Binary +200", "Ordinal"])
    axes[1].set_ylim(0, 0.75)
    axes[1].set_title("Precision-retention trade-off")
    axes[1].set_ylabel("Share")
    axes[1].legend(frameon=False, fontsize=9)
    axes[1].text(1, event["positive_event_retention"] + 0.03, f"event retention {event['positive_event_retention']:.0%}", ha="center", fontsize=9)

    names = ["Frozen exit", "+150% half / 30% trail"]
    expectancy = [baseline["expectancy"], challenger["expectancy"]]
    bars = axes[2].bar(names, expectancy, color=["#64748b", "#f59e0b"])
    axes[2].set_title("8h-entry exit point estimates")
    axes[2].set_ylabel("Mean net return")
    axes[2].set_ylim(0, max(expectancy) * 1.35)
    axes[2].tick_params(axis="x", rotation=8)
    for bar, value, pf in zip(bars, expectancy, [baseline["profit_factor"], challenger["profit_factor"]]):
        axes[2].text(bar.get_x() + bar.get_width() / 2, value, f"{value:.1%}\nPF {pf:.2f}", ha="center", va="bottom")

    figure.suptitle("Extreme-intensity recognition and right-tail exit frontier", fontsize=14, fontweight="bold")
    figure.tight_layout()
    chart = REPORT / "charts" / "intensity_exit_frontier.png"
    chart.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(chart, dpi=180, bbox_inches="tight")
    plt.close(figure)


def update_notebook() -> None:
    path = REPORT / "binance_runup_research.ipynb"
    notebook = nbformat.read(path, as_version=4)
    notebook.cells = [cell for cell in notebook.cells if "intensity-exit-frontier" not in cell.get("metadata", {}).get("tags", [])]
    notebook.cells.append(
        nbformat.v4.new_markdown_cell(
            """## Multi-threshold recognition and right-tail exit frontier

The ordinal +50/+100/+150/+200% target improves confirmation AP and event retention but fails clustered uncertainty bounds. Early failure-to-launch exits are rejected. A +150% half-take-profit with a 30% trail improves exit point estimates, but paired incremental bootstrap intervals cross zero, so it is recorded only as a prospective counterfactual.""",
            metadata={"tags": ["intensity-exit-frontier"]},
        )
    )
    nbformat.write(notebook, path)


def main() -> None:
    verification = json.loads((REPORT / "intensity_exit_frontier_verification.json").read_text(encoding="utf-8"))
    if not verification.get("passed"):
        raise RuntimeError("Intensity/exit frontier verifier did not pass")
    intensity = json.loads((REPORT / "extreme_intensity_decision.json").read_text(encoding="utf-8"))
    failure = json.loads((REPORT / "utility_failure_exit_decision.json").read_text(encoding="utf-8"))
    tail = json.loads((REPORT / "utility_tail_exit_decision.json").read_text(encoding="utf-8"))
    make_chart(intensity, tail)
    update_notebook()

    direct = intensity["confirmation"]
    prior = intensity["prior_binary200_confirmation"]
    events = intensity["event_metrics"]
    baseline = tail["baseline_confirmation_summary"]
    challenger = tail["selected_confirmation_summary"]
    section = f"""{BEGIN}
## 继续提高识别与退出：多阈值强度有效但未过门槛，右尾退出只作前瞻反事实

### 多阈值目标比单一 +200% 标签更有效，但不确定性仍不足

直接 +200% 标签过于稀疏，因此预注册了两类丰富目标：固定权重的 +50%/+100%/+150%/+200% 序数模型，以及对未来14日 MFE 做对数回归的连续强度模型。48 个候选只在三个校准窗口选择，校准选中 **20小时序数模型、训练分数前20%**。

确认期 AP 从价格分数的 **{direct['price_average_precision']:.4f}** 和旧二元模型的 **{prior['continuation_average_precision']:.4f}** 提高到 **{direct['intensity_average_precision']:.4f}**；62 个入选信号命中7个，精度 **{direct['selected_precision']:.2%}**，保留 {direct['selected_positives']}/{direct['positives']} 个仍有 +200% 空间的标签。事件去重后选择 {events['selected_events']} 个、命中 {events['selected_positive_events']} 个，精度 **{events['selected_event_precision']:.2%}**、正事件保留率 **{events['positive_event_retention']:.2%}**。但只有 {direct['fold_joint_improvement_share']:.0%} 的确认折同时改善 AP 与精度，按周和按币 bootstrap 的 AP/精度增量下界仍小于0，因此仍是 `watchlist_only`，不能加入排名。

### “没有迅速启动就退出”被否决

在冻结8小时效用入场后，比较了 P0+24/48/72小时的 MFE 失败退出。校准仍选择 **不使用失败启动退出**；所有四个提前退出版本在逐信号配对后平均增量都为负。原来的 -25%硬止损、+30%后保本、+100%减半、25%追踪、最长30天保持不变。

### 更晚减半、放宽追踪有正点估计，但尚不能替换主退出

校准选择 **+150%卖出一半、余仓30%追踪**。确认期组合从基线 {baseline['trades']} 笔、期望 {baseline['expectancy']:.2%}、PF {baseline['profit_factor']:.2f}、收益 {baseline['total_return_on_initial_equity']:.2%}，变为 {challenger['trades']} 笔、期望 **{challenger['expectancy']:.2%}**、PF **{challenger['profit_factor']:.2f}**、收益 **{challenger['total_return_on_initial_equity']:.2%}**；最大回撤从 {baseline['max_drawdown']:.2%} 扩大到 {challenger['max_drawdown']:.2%}。逐信号平均增量为 {tail['paired_increment_mean']:.2%}，但按周95%区间下界 {tail['week_increment_bootstrap']['mean_delta_lower_95pct']:.2%}、按币下界 {tail['symbol_increment_bootstrap']['mean_delta_lower_95pct']:.2%}，仍无法排除偶然性。

![多阈值识别与右尾退出](charts/intensity_exit_frontier.png)

因此主策略不变；`hard25_be30_half150_trail30` 已加入未来30日标签，作为不改变排名、不触发交易的冻结反事实。真正未来样本若能让配对增量下界大于0，才讨论替换主退出。
{END}"""
    report_path = REPORT / "REPORT.md"
    report_path.write_text(replace_section(report_path.read_text(encoding="utf-8"), section), encoding="utf-8")

    model_section = f"""{BEGIN}
## Extreme-intensity and right-tail exit frontier

- Ordinal +50/+100/+150/+200% intensity improves confirmation AP to {direct['intensity_average_precision']:.4f}, but clustered incremental lower bounds remain negative; ranking stays unchanged.
- Failure-to-launch exits at P0+24/48/72h were rejected in calibration; the frozen exit remains primary.
- Half at +150% with a 30% trail improves historical point estimates to {challenger['expectancy']:.2%} expectancy and PF {challenger['profit_factor']:.2f}, but paired incremental bootstrap intervals cross zero.
- `hard25_be30_half150_trail30` is a prospective read-only counterfactual, not a promoted policy. Automatic trading remains disabled.
{END}"""
    model_path = REPORT / "MODEL_CARD.md"
    model_path.write_text(replace_section(model_path.read_text(encoding="utf-8"), model_section), encoding="utf-8")

    chart_section = f"""{BEGIN}
## Intensity and exit frontier

- `charts/intensity_exit_frontier.png`: confirmation AP, precision-retention trade-off, and frozen-versus-right-tail exit point estimates.
{END}"""
    chart_path = REPORT / "CHART_MAP.md"
    chart_path.write_text(replace_section(chart_path.read_text(encoding="utf-8"), chart_section), encoding="utf-8")
    print(json.dumps({"updated": True, "intensity_gate": intensity["ranking_gate_pass"], "tail_gate": tail["incremental_exit_gate_pass"]}))


if __name__ == "__main__":
    main()
