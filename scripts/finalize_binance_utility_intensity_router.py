"""Finalize the rejected 8h-entry / 20h-intensity router study."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
import nbformat
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
BEGIN = "<!-- UTILITY_INTENSITY_ROUTER_BEGIN -->"
END = "<!-- UTILITY_INTENSITY_ROUTER_END -->"


def replace_section(text: str, section: str) -> str:
    if BEGIN in text and END in text:
        start = text.index(BEGIN)
        finish = text.index(END, start) + len(END)
        return text[:start].rstrip() + "\n\n" + section + "\n" + text[finish:].lstrip()
    return text.rstrip() + "\n\n" + section + "\n"


def make_chart(decision: dict, folds: pd.DataFrame) -> None:
    recognition = decision["confirmation_recognition"]
    baseline = decision["baseline_confirmation_summary"]
    routed = decision["routed_confirmation_summary"]
    figure, axes = plt.subplots(1, 2, figsize=(12.5, 4.8))

    x = np.arange(len(folds))
    width = 0.36
    axes[0].bar(x - width / 2, folds["base_precision"], width, label="All 8h entries", color="#64748b")
    axes[0].bar(x + width / 2, folds["high_precision"], width, label="20h high intensity", color="#2563eb")
    axes[0].set_xticks(x, folds["validation_split"])
    axes[0].set_ylim(0, max(0.45, float(folds["high_precision"].max()) * 1.2))
    axes[0].set_ylabel("Remaining +200% precision")
    axes[0].set_title("Recognition is unstable across folds")
    axes[0].legend(frameon=False)
    axes[0].text(0.02, 0.95, f"Retention {recognition['positive_retention_from_entry']:.0%}", transform=axes[0].transAxes, va="top")

    labels = ["Frozen", "20h reject exit"]
    expectancy = [baseline["expectancy"], routed["expectancy"]]
    colors = ["#22c55e", "#ef4444"]
    bars = axes[1].bar(labels, expectancy, color=colors)
    axes[1].axhline(0, color="#0f172a", linewidth=0.8)
    axes[1].set_ylim(-0.04, 0.22)
    axes[1].set_ylabel("Mean net return")
    axes[1].set_title("Model-routed early exit destroys the right tail")
    for bar, value, pf in zip(bars, expectancy, [baseline["profit_factor"], routed["profit_factor"]]):
        axes[1].text(bar.get_x() + bar.get_width() / 2, value + (0.005 if value >= 0 else -0.003), f"{value:.1%}\nPF {pf:.2f}", ha="center", va="bottom" if value >= 0 else "top")

    figure.suptitle("P0+8h entry / P0+20h intensity router falsification", fontsize=14, fontweight="bold")
    figure.tight_layout()
    chart = REPORT / "charts" / "utility_intensity_router.png"
    chart.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(chart, dpi=180, bbox_inches="tight")
    plt.close(figure)


def update_notebook() -> None:
    path = REPORT / "binance_runup_research.ipynb"
    notebook = nbformat.read(path, as_version=4)
    notebook.cells = [cell for cell in notebook.cells if "utility-intensity-router" not in cell.get("metadata", {}).get("tags", [])]
    notebook.cells.append(
        nbformat.v4.new_markdown_cell(
            """## 8h utility entry / 20h ordinal-intensity router

The pre-selected 20h ordinal score modestly increases pooled remaining +200% precision, but fails in two of four folds. Exiting low-intensity 8h entries at the 20h decision open changes confirmation expectancy from +16.50% to -1.06% and PF from 2.50 to 0.84. Paired week and symbol bootstrap upper bounds remain negative, so the router is strongly rejected.""",
            metadata={"tags": ["utility-intensity-router"]},
        )
    )
    nbformat.write(notebook, path)


def main() -> None:
    verification = json.loads((REPORT / "utility_intensity_router_verification.json").read_text(encoding="utf-8"))
    if not verification.get("passed"):
        raise RuntimeError("Utility/intensity router verifier did not pass")
    decision = json.loads((REPORT / "utility_intensity_router_decision.json").read_text(encoding="utf-8"))
    folds = pd.read_csv(REPORT / "utility_intensity_router_recognition_folds.csv")
    make_chart(decision, folds)
    update_notebook()
    recognition = decision["confirmation_recognition"]
    events = decision["event_metrics"]
    baseline = decision["baseline_confirmation_summary"]
    routed = decision["routed_confirmation_summary"]
    section = f"""{BEGIN}
## 8小时入场后再用20小时强度决定去留：明确失败

为了避免固定 MFE 规则过于粗糙，本轮把两个已经校准选出的模型串联：P0+8小时效用模型先决定纸面进入，P0+20小时序数强度模型再决定是否继续持有。没有重新选择模型、检查点或阈值；低强度信号只读取截至20小时已经收完的K线，并在该边界开盘退出。

识别层的总体点估计从 **{recognition['base_precision_from_entry']:.2%}** 提高到 **{recognition['high_precision_from_entry']:.2%}**，但只保留 {recognition['high_positives_from_entry']}/{recognition['positives_from_entry']} 个正标签；四折中只有两折命中，高强度组在 folds 3 和 6 的命中均为0。事件去重后选择 {events['selected_events']} 个、命中 {events['selected_positive_events']} 个，精度 {events['selected_event_precision']:.2%}。按周和按币的精度/AP增量下界仍小于0，识别门槛失败。

交易层出现更强反证：确认期从基线 {baseline['trades']} 笔、期望 **{baseline['expectancy']:.2%}**、PF **{baseline['profit_factor']:.2f}**，变为 {routed['trades']} 笔、期望 **{routed['expectancy']:.2%}**、PF **{routed['profit_factor']:.2f}**。逐信号平均增量为 **{decision['paired_increment_mean']:.2%}**；按周增量95%区间上界只有 {decision['week_increment_bootstrap']['mean_delta_upper_95pct']:.2%}，按币上界 {decision['symbol_increment_bootstrap']['mean_delta_upper_95pct']:.2%}，即使最乐观区间也仍为负。

![20小时强度路由反证](charts/utility_intensity_router.png)

结论不是“样本不足”，而是**20小时低强度不能作为卖出理由**。暴涨币的收益依赖慢启动和少数长右尾；8小时入场后继续使用冻结退出，不新增20小时强制离场。
{END}"""
    report_path = REPORT / "REPORT.md"
    report_path.write_text(replace_section(report_path.read_text(encoding="utf-8"), section), encoding="utf-8")

    model_section = f"""{BEGIN}
## Rejected 8h/20h sequential router

- Frozen 8h utility entry followed by a 20h ordinal-intensity keep/exit decision is strongly rejected.
- Routed expectancy is {routed['expectancy']:.2%} and PF {routed['profit_factor']:.2f}, versus {baseline['expectancy']:.2%} and PF {baseline['profit_factor']:.2f} for the frozen exit.
- Paired mean increment is {decision['paired_increment_mean']:.2%}; both clustered 95% upper bounds remain below zero.
- The router is not added to forward signals or labels. The frozen entry and exit remain unchanged.
{END}"""
    model_path = REPORT / "MODEL_CARD.md"
    model_path.write_text(replace_section(model_path.read_text(encoding="utf-8"), model_section), encoding="utf-8")

    chart_section = f"""{BEGIN}
## Utility/intensity router

- `charts/utility_intensity_router.png`: fold-level recognition instability and the strongly negative routed-exit result.
{END}"""
    chart_path = REPORT / "CHART_MAP.md"
    chart_path.write_text(replace_section(chart_path.read_text(encoding="utf-8"), chart_section), encoding="utf-8")
    print(json.dumps({"updated": True, "recognition_gate": decision["recognition_gate_pass"], "trade_gate": decision["trade_gate_pass"]}))


if __name__ == "__main__":
    main()
