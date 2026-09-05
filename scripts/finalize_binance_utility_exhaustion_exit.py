"""Finalize the profit-exhaustion exit challenger in durable artifacts."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
import nbformat
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
BEGIN = "<!-- UTILITY_EXHAUSTION_EXIT_BEGIN -->"
END = "<!-- UTILITY_EXHAUSTION_EXIT_END -->"


def replace_section(text: str, section: str) -> str:
    if BEGIN in text and END in text:
        start = text.index(BEGIN)
        finish = text.index(END, start) + len(END)
        return text[:start].rstrip() + "\n\n" + section + "\n" + text[finish:].lstrip()
    return text.rstrip() + "\n\n" + section + "\n"


def make_chart(decision: dict, baseline_folds: pd.DataFrame, exhaustion_folds: pd.DataFrame) -> None:
    baseline = decision["baseline_confirmation_summary"]
    selected = decision["selected_confirmation_summary"]
    figure, axes = plt.subplots(1, 3, figsize=(16, 4.8))

    x = np.arange(len(exhaustion_folds))
    width = 0.36
    axes[0].bar(x - width / 2, baseline_folds["profit_factor"], width, label="Frozen", color="#64748b")
    axes[0].bar(x + width / 2, exhaustion_folds["profit_factor"], width, label="Wick exhaustion", color="#f59e0b")
    axes[0].axhline(1.0, color="#0f172a", linestyle="--", linewidth=0.9)
    axes[0].set_xticks(x, [f"fold {int(value)}" for value in exhaustion_folds["fold"]])
    axes[0].set_ylabel("Profit factor")
    axes[0].set_title("Profit factor by confirmation fold")
    axes[0].legend(frameon=False)

    bars = axes[1].bar(
        ["Frozen", "Wick exhaustion"],
        [baseline["expectancy"], selected["expectancy"]],
        color=["#64748b", "#f59e0b"],
    )
    axes[1].set_ylim(0, 0.23)
    axes[1].set_ylabel("Mean net return")
    axes[1].set_title("Exit-policy point estimates")
    for bar, value, pf in zip(bars, [baseline["expectancy"], selected["expectancy"]], [baseline["profit_factor"], selected["profit_factor"]]):
        axes[1].text(bar.get_x() + bar.get_width() / 2, value + 0.005, f"{value:.1%}\nPF {pf:.2f}", ha="center", va="bottom")

    clusters = ["Week", "Symbol"]
    bootstrap = [decision["week_increment_bootstrap"], decision["symbol_increment_bootstrap"]]
    medians = np.asarray([item["mean_delta_median"] for item in bootstrap])
    lower = np.asarray([item["mean_delta_lower_95pct"] for item in bootstrap])
    upper = np.asarray([item["mean_delta_upper_95pct"] for item in bootstrap])
    axes[2].errorbar(clusters, medians, yerr=np.vstack([medians - lower, upper - medians]), fmt="o", color="#2563eb", capsize=6)
    axes[2].axhline(0, color="#0f172a", linewidth=0.9)
    axes[2].set_ylabel("Paired mean-return increment")
    axes[2].set_title("Incremental 95% intervals still cross zero")
    axes[2].set_ylim(min(lower) - 0.01, max(upper) + 0.01)

    figure.suptitle("Profit-side blow-off exhaustion challenger", fontsize=14, fontweight="bold")
    figure.tight_layout()
    chart = REPORT / "charts" / "utility_exhaustion_exit.png"
    chart.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(chart, dpi=180, bbox_inches="tight")
    plt.close(figure)


def update_notebook() -> None:
    path = REPORT / "binance_runup_research.ipynb"
    notebook = nbformat.read(path, as_version=4)
    notebook.cells = [cell for cell in notebook.cells if "utility-exhaustion-exit" not in cell.get("metadata", {}).get("tags", [])]
    notebook.cells.append(
        nbformat.v4.new_markdown_cell(
            """## Profit-side blow-off exhaustion exit

After the frozen 8h entry, the calibration-selected policy waits for +150% before selling half, uses a 35% trail, and exits at the next 4h open after a doubled position prints a volume-backed upper-wick reversal. All four confirmation folds have PF above one and point estimates improve, but paired incremental week/symbol intervals still cross zero. It is therefore frozen only as a forward counterfactual.""",
            metadata={"tags": ["utility-exhaustion-exit"]},
        )
    )
    nbformat.write(notebook, path)


def main() -> None:
    verification = json.loads((REPORT / "utility_exhaustion_exit_verification.json").read_text(encoding="utf-8"))
    if not verification.get("passed"):
        raise RuntimeError("Utility exhaustion exit verifier did not pass")
    decision = json.loads((REPORT / "utility_exhaustion_exit_decision.json").read_text(encoding="utf-8"))
    baseline_folds = pd.read_csv(REPORT / "continuation_utility_strategy_folds.csv")
    exhaustion_folds = pd.read_csv(REPORT / "utility_exhaustion_exit_folds.csv")
    make_chart(decision, baseline_folds, exhaustion_folds)
    update_notebook()
    baseline = decision["baseline_confirmation_summary"]
    selected = decision["selected_confirmation_summary"]
    section = f"""{BEGIN}
## 盈利后的放量长上影：目前最有希望的卖点挑战者

此前的失败启动和20小时低强度退出都会误杀慢启动赢家。本轮反过来只在盈利已经形成后寻找衰竭：校准比较主退出、+150%减半后的30%/35%追踪，以及放量长上影和买盘衰退两类4小时信号。所有衰竭信号必须等当前K线收完，下一根4小时开盘才允许退出。

校准选择 `half150_trail35_wick`：**盈利达到+150%卖出一半，余仓35%追踪；价格至少翻倍后，如果当前4小时K线的上影占振幅≥35%、收盘位于振幅下35%、成交量≥过去均值1.5倍，则下一根开盘退出。** 买盘衰退版本没有胜出。

确认期共有 {selected['trades']} 笔，其中10笔由放量长上影退出。相对主退出：

- 单笔期望从 {baseline['expectancy']:.2%} 提高到 **{selected['expectancy']:.2%}**；
- 利润因子从 {baseline['profit_factor']:.2f} 提高到 **{selected['profit_factor']:.2f}**；
- 组合收益从 {baseline['total_return_on_initial_equity']:.2%} 提高到 **{selected['total_return_on_initial_equity']:.2%}**；
- 最大回撤从 {baseline['max_drawdown']:.2%} 变为 {selected['max_drawdown']:.2%}；
- 四个确认折的PF分别为 {'/'.join(f"{value:.2f}" for value in exhaustion_folds['profit_factor'])}，全部大于1；
- 周/币聚类的策略绝对期望下界均大于0，删除最大赢家后期望仍为 {decision['leave_largest_winner_out_expectancy']:.2%}。

但相对主退出的逐信号平均增量只有 **{decision['paired_increment_mean']:.2%}**；按周增量95%下界 {decision['week_increment_bootstrap']['mean_delta_lower_95pct']:.2%}，按币下界 {decision['symbol_increment_bootstrap']['mean_delta_lower_95pct']:.2%}，仍略低于0。因此它是当前最强卖点挑战者，但尚不能替换主退出。

![盈利衰竭退出验证](charts/utility_exhaustion_exit.png)

该规则已作为 `hard25_be30_half150_trail35_wick` 加入未来30日只读标签。主退出继续冻结；只有真正OOS的配对增量下界大于0，才允许升级。
{END}"""
    report_path = REPORT / "REPORT.md"
    report_path.write_text(replace_section(report_path.read_text(encoding="utf-8"), section), encoding="utf-8")

    model_section = f"""{BEGIN}
## Profit-side exhaustion exit challenger

- Calibration selected half at +150%, a 35% trail, and next-open exit after a volume-backed upper-wick reversal once the position has doubled.
- Confirmation expectancy is {selected['expectancy']:.2%}, PF {selected['profit_factor']:.2f}, with all four fold PF values above one.
- Paired mean increment versus the frozen exit is {decision['paired_increment_mean']:.2%}; week and symbol incremental lower bounds remain negative.
- `hard25_be30_half150_trail35_wick` is recorded as a prospective counterfactual only. The primary exit and automatic-trading prohibition are unchanged.
{END}"""
    model_path = REPORT / "MODEL_CARD.md"
    model_path.write_text(replace_section(model_path.read_text(encoding="utf-8"), model_section), encoding="utf-8")

    chart_section = f"""{BEGIN}
## Profit exhaustion exit

- `charts/utility_exhaustion_exit.png`: fold PF, point estimates, and paired incremental uncertainty for the volume-backed upper-wick exit.
{END}"""
    chart_path = REPORT / "CHART_MAP.md"
    chart_path.write_text(replace_section(chart_path.read_text(encoding="utf-8"), chart_section), encoding="utf-8")
    print(json.dumps({"updated": True, "selected": decision["selected_policy"], "gate": decision["incremental_exit_gate_pass"]}))


if __name__ == "__main__":
    main()
