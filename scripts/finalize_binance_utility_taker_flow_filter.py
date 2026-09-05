"""Add the verified 8h taker-flow diagnostic to the durable research bundle."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
import nbformat
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
BEGIN = "<!-- UTILITY_TAKER_FLOW_BEGIN -->"
END = "<!-- UTILITY_TAKER_FLOW_END -->"


def replace_section(text: str, section: str) -> str:
    if BEGIN in text and END in text:
        start = text.index(BEGIN)
        finish = text.index(END, start) + len(END)
        return text[:start] + section + text[finish:]
    return text.rstrip() + "\n\n" + section + "\n"


def make_chart(decision: dict, summary: pd.DataFrame) -> None:
    calibration = decision["calibration"]
    confirmation = decision["confirmation"]
    figure, axes = plt.subplots(1, 3, figsize=(15, 4.8))
    figure.suptitle("8h taker-buy-share identification diagnostic", fontsize=15, fontweight="bold")

    labels = ["Calibration\nrows", "Confirmation\nrows", "Confirmation\nevents"]
    base = [
        calibration["base_precision"],
        confirmation["base_precision"],
        confirmation["base_event_precision"],
    ]
    filtered = [
        calibration["selected_precision"],
        confirmation["selected_precision"],
        confirmation["selected_event_precision"],
    ]
    x = np.arange(len(labels))
    width = 0.34
    axes[0].bar(x - width / 2, base, width, label="All 8h utility signals", color="#64748b")
    axes[0].bar(x + width / 2, filtered, width, label="Taker buy share ≥50%", color="#2563eb")
    axes[0].set_xticks(x, labels)
    axes[0].set_ylabel("+200% precision")
    axes[0].set_title("Recognition precision")
    axes[0].set_ylim(0, max(filtered) * 1.35)
    axes[0].legend(frameon=False, fontsize=8)
    for xpos, value in zip(x - width / 2, base, strict=True):
        axes[0].text(xpos, value + 0.006, f"{value:.1%}", ha="center", fontsize=8)
    for xpos, value in zip(x + width / 2, filtered, strict=True):
        axes[0].text(xpos, value + 0.006, f"{value:.1%}", ha="center", fontsize=8)

    intervals = [
        ("Rows/week", confirmation["week_bootstrap"]),
        ("Rows/symbol", confirmation["symbol_bootstrap"]),
        ("Events/week", confirmation["event_week_bootstrap"]),
        ("Events/symbol", confirmation["event_symbol_bootstrap"]),
    ]
    centers = np.array([item[1]["precision_delta_median"] for item in intervals])
    lower = np.array([item[1]["precision_delta_lower_95pct"] for item in intervals])
    upper = np.array([item[1]["precision_delta_upper_95pct"] for item in intervals])
    axes[1].errorbar(
        np.arange(len(intervals)),
        centers,
        yerr=np.vstack([centers - lower, upper - centers]),
        fmt="o",
        color="#2563eb",
        ecolor="#64748b",
        capsize=5,
    )
    axes[1].axhline(0, color="#0f172a", linewidth=1)
    axes[1].set_xticks(np.arange(len(intervals)), [item[0] for item in intervals], rotation=15)
    axes[1].set_ylabel("Precision increment")
    axes[1].set_title("Confirmation clustered intervals")

    default = summary[summary["slippage_bps_each_side"] == 5.0].set_index("variant")
    order = ["all_frozen", "taker50_frozen", "all_breakout_router", "taker50_breakout_router"]
    short = ["All /\nfrozen", "Taker50 /\nfrozen", "All /\nrouter", "Taker50 /\nrouter"]
    values = [default.loc[name, "expectancy"] for name in order]
    colors = ["#64748b", "#2563eb", "#94a3b8", "#f59e0b"]
    bars = axes[2].bar(short, values, color=colors)
    axes[2].set_ylabel("Portfolio trade expectancy")
    axes[2].set_title("Confirmation strategy point estimates")
    axes[2].set_ylim(0, max(values) * 1.30)
    for bar, name, value in zip(bars, order, values, strict=True):
        axes[2].text(
            bar.get_x() + bar.get_width() / 2,
            value + 0.006,
            f"{value:.1%}\nPF {default.loc[name, 'profit_factor']:.2f}",
            ha="center",
            fontsize=8,
        )

    for axis in axes:
        axis.grid(axis="y", alpha=0.18)
        axis.spines[["top", "right"]].set_visible(False)
    figure.tight_layout()
    chart = REPORT / "charts" / "utility_taker_flow_filter.png"
    chart.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(chart, dpi=180, bbox_inches="tight")
    plt.close(figure)


def update_notebook(decision: dict, summary: pd.DataFrame) -> None:
    path = REPORT / "binance_runup_research.ipynb"
    notebook = nbformat.read(path, as_version=4)
    notebook.cells = [
        cell for cell in notebook.cells
        if "utility-taker-flow" not in cell.get("metadata", {}).get("tags", [])
    ]
    confirmation = decision["confirmation"]
    default = summary[summary["slippage_bps_each_side"] == 5.0].set_index("variant")
    notebook.cells.append(
        nbformat.v4.new_markdown_cell(
            f"""## 8h taker-buy-share identification annotation

A semantic filter requiring the first two completed 4h bars to have taker buy quote share of at least 50% retains {confirmation['selected_positives']}/{confirmation['positives']} confirmation +200% rows and raises precision from {confirmation['base_precision']:.2%} to {confirmation['selected_precision']:.2%}. Row-level week and symbol lower bounds are positive, but independent-event lower bounds cross zero. The filtered frozen-exit portfolio has {default.loc['taker50_frozen', 'expectancy']:.2%} expectancy and PF {default.loc['taker50_frozen', 'profit_factor']:.2f}; combining it with the prospective breakout hold router reaches {default.loc['taker50_breakout_router', 'expectancy']:.2%} and PF {default.loc['taker50_breakout_router', 'profit_factor']:.2f}, but fold 6 is negative and the symbol-cluster absolute-return lower bound remains below zero. The field is therefore recorded prospectively without changing rank, stage veto, paper eligibility, or trading authority.""",
            metadata={"tags": ["utility-taker-flow"]},
        )
    )
    nbformat.write(notebook, path)


def main() -> None:
    verification = json.loads((REPORT / "utility_taker_flow_verification.json").read_text(encoding="utf-8"))
    if not verification.get("passed"):
        raise RuntimeError("Taker-flow verifier did not pass")
    decision = json.loads((REPORT / "utility_taker_flow_decision.json").read_text(encoding="utf-8"))
    summary = pd.read_csv(REPORT / "utility_taker_flow_strategy_summary.csv")
    make_chart(decision, summary)
    update_notebook(decision, summary)

    calibration = decision["calibration"]
    confirmation = decision["confirmation"]
    default = summary[summary["slippage_bps_each_side"] == 5.0].set_index("variant")
    filtered_intervals = decision["absolute_return_intervals"]["taker50_frozen"]
    combo_intervals = decision["absolute_return_intervals"]["taker50_breakout_router"]
    section = f"""{BEGIN}
## 8小时主动买入占比：无需等待的识别增强项，但尚不能成为买入门

在8小时效用模型已经入选的信号中，继续等待回撤或突破都会损失时机。本轮因此只检查决策时已经可得的路径字段，并冻结一个语义明确、无需拟合的条件：前两根完整4小时K线合计的主动买入成交额占比至少 **50%**。它不改变入场时钟，仍在P0后8小时决策开盘判断。

更早的42条校准信号中，该条件保留21条并保留全部3个 +200% 正例，精度从 {calibration['base_precision']:.2%} 提高到 {calibration['selected_precision']:.2%}，三个校准窗口的精度增量均为正。但筛选后的14天效用期望从 {calibration['base_utility_expectancy_14d']:.2%} 降到 {calibration['selected_utility_expectancy_14d']:.2%}，说明“更像暴涨币”与“当时买入更赚钱”在早期样本中并不一致。

后四折确认的识别点估计更强：

- 88条8小时信号中保留48条，+200% 正例保留 **{confirmation['selected_positives']}/{confirmation['positives']}**；
- 逐行精度从 **{confirmation['base_precision']:.2%}** 提高到 **{confirmation['selected_precision']:.2%}**，周块/币种增量95%下界分别为 {confirmation['week_bootstrap']['precision_delta_lower_95pct']:.2%}/{confirmation['symbol_bootstrap']['precision_delta_lower_95pct']:.2%}；
- 合并同币14天重复信号后，精度从 {confirmation['base_event_precision']:.2%} 提高到 {confirmation['selected_event_precision']:.2%}，保留4/5个正事件，但事件周块/币种下界为 {confirmation['event_week_bootstrap']['precision_delta_lower_95pct']:.2%}/{confirmation['event_symbol_bootstrap']['precision_delta_lower_95pct']:.2%}，仍低于0。

交易点估计也改善，但稳定性不够：冻结退出下35笔交易的期望为 **{default.loc['taker50_frozen', 'expectancy']:.2%}**、PF **{default.loc['taker50_frozen', 'profit_factor']:.2f}**，高于全部信号的 {default.loc['all_frozen', 'expectancy']:.2%}/PF {default.loc['all_frozen', 'profit_factor']:.2f}；与突破持仓路由合并后为 **{default.loc['taker50_breakout_router', 'expectancy']:.2%}**、PF **{default.loc['taker50_breakout_router', 'profit_factor']:.2f}**。然而第6折筛选后5笔的期望为 -14.74%、PF 0.03；冻结退出按币种bootstrap期望下界 {filtered_intervals['symbol']['expectancy_lower_95pct']:.2%}，组合路由下界 {combo_intervals['symbol']['expectancy_lower_95pct']:.2%}，均未过零。

因此它是目前最值得前瞻验证的**识别注释**，不是新的排名或买入否决条件。未来8小时只读快照已经追加 `taker_flow_supportive`，30日标签会原样携带；该字段不提高排名、不覆盖 `reject_chase`/暴涨后冷却、不改变现有纸面资格。主买点、主退出和自动交易禁令均不变。

![8小时主动买入占比识别验证](charts/utility_taker_flow_filter.png)
{END}"""
    report_path = REPORT / "REPORT.md"
    report_path.write_text(replace_section(report_path.read_text(encoding="utf-8"), section), encoding="utf-8")

    analysis_path = REPORT / "analysis_summary.json"
    analysis = json.loads(analysis_path.read_text(encoding="utf-8"))
    analysis["utility_taker_flow_filter"] = decision
    analysis_path.write_text(json.dumps(analysis, ensure_ascii=False, indent=2), encoding="utf-8")

    model_section = f"""{BEGIN}
## 8h taker-buy-share prospective annotation

- Fixed condition: `early_taker_buy_share >= 0.50` after two completed 4h bars; it adds no entry delay.
- Calibration retains all 3/3 +200% rows; confirmation retains 9/10 and raises row precision from {confirmation['base_precision']:.2%} to {confirmation['selected_precision']:.2%}.
- Row-cluster lower bounds are positive, but independent-event lower bounds cross zero. Filtered strategy fold 6 and symbol-cluster absolute-return gates fail.
- The field is written to future immutable utility observations and labels as a rank-neutral annotation only. It cannot alter rank, original stage veto, paper eligibility, position size, primary exit, or trading authority.
{END}"""
    model_path = REPORT / "MODEL_CARD.md"
    model_path.write_text(replace_section(model_path.read_text(encoding="utf-8"), model_section), encoding="utf-8")

    chart_section = f"""{BEGIN}
## 8h taker-buy-share diagnostic

- `charts/utility_taker_flow_filter.png`: calibration/confirmation precision, row/event clustered intervals, and frozen/breakout-router strategy point estimates.
{END}"""
    chart_path = REPORT / "CHART_MAP.md"
    chart_path.write_text(replace_section(chart_path.read_text(encoding="utf-8"), chart_section), encoding="utf-8")
    print(json.dumps({
        "updated": True,
        "selected_rows": confirmation["selected_rows"],
        "selected_positives": confirmation["selected_positives"],
        "confirmation_full_gate_pass": decision["confirmation_full_gate_pass"],
        "classification": decision["classification"],
    }))


if __name__ == "__main__":
    main()
