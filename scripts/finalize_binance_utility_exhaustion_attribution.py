"""Finalize the static-tail versus wick-exit attribution in durable artifacts."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
import nbformat
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
BEGIN = "<!-- UTILITY_EXHAUSTION_ATTRIBUTION_BEGIN -->"
END = "<!-- UTILITY_EXHAUSTION_ATTRIBUTION_END -->"


def replace_section(text: str, section: str) -> str:
    if BEGIN in text and END in text:
        start = text.index(BEGIN)
        finish = text.index(END, start) + len(END)
        return text[:start].rstrip() + "\n\n" + section + "\n" + text[finish:].lstrip()
    return text.rstrip() + "\n\n" + section + "\n"


def make_chart(decision: dict, folds: pd.DataFrame) -> None:
    contrasts = decision["contrasts"]
    keys = [
        "trail35_minus_trail30",
        "wick_minus_tail35",
        "wick_minus_baseline",
        "posthoc_wick30_minus_baseline",
    ]
    labels = ["35% vs 30%\ntrail", "Wick vs\nstatic 35%", "Wick 35% vs\nfrozen", "Post-hoc wick 30%\nvs frozen"]
    row_values = np.asarray([contrasts[key]["mean_delta"] for key in keys])
    event_values = np.asarray([contrasts[key]["event_mean_delta"] for key in keys])
    figure, axes = plt.subplots(1, 3, figsize=(18, 5.2))

    x = np.arange(len(keys))
    width = 0.36
    axes[0].bar(x - width / 2, row_values, width, label="Signal rows", color="#2563eb")
    axes[0].bar(x + width / 2, event_values, width, label="Independent events", color="#f59e0b")
    axes[0].axhline(0, color="#0f172a", linewidth=0.9)
    axes[0].set_xticks(x, labels)
    axes[0].set_ylabel("Paired mean-return increment")
    axes[0].set_title("Point attribution shrinks after event de-duplication")
    axes[0].legend(frameon=False)

    pure = contrasts["wick_minus_tail35"]
    interval_items = [
        ("Row/week", pure["week_bootstrap"]),
        ("Row/symbol", pure["symbol_bootstrap"]),
        ("Event/week", pure["event_week_bootstrap"]),
        ("Event/symbol", pure["event_symbol_bootstrap"]),
    ]
    centers = np.asarray([item[1]["mean_delta_median"] for item in interval_items])
    lower = np.asarray([item[1]["mean_delta_lower_95pct"] for item in interval_items])
    upper = np.asarray([item[1]["mean_delta_upper_95pct"] for item in interval_items])
    axes[1].errorbar(
        np.arange(len(interval_items)),
        centers,
        yerr=np.vstack([centers - lower, upper - centers]),
        fmt="o",
        color="#7c3aed",
        capsize=6,
    )
    axes[1].axhline(0, color="#0f172a", linewidth=0.9)
    axes[1].set_xticks(np.arange(len(interval_items)), [item[0] for item in interval_items], rotation=15)
    axes[1].set_ylabel("Pure wick timing increment")
    axes[1].set_title("All pure-wick 95% intervals cross zero")

    policy_labels = {
        "baseline_half100_trail25": "Frozen",
        "half150_trail30": "Static 30%",
        "half150_trail35_wick": "Wick 35%",
        "half150_trail30_wick_posthoc": "Post-hoc wick 30%",
    }
    for policy, label in policy_labels.items():
        frame = folds[folds["policy"] == policy].sort_values("fold")
        axes[2].plot(frame["fold"], frame["profit_factor"], marker="o", linewidth=2, label=label)
    axes[2].axhline(1.0, color="#0f172a", linestyle="--", linewidth=0.9)
    axes[2].set_xticks([3, 4, 5, 6])
    axes[2].set_xlabel("Confirmation fold")
    axes[2].set_ylabel("Profit factor")
    axes[2].set_title("Policy PF by confirmation fold")
    axes[2].legend(frameon=False, fontsize=9)

    figure.suptitle("Exit-factor attribution: static trail width versus blow-off timing", fontsize=14, fontweight="bold")
    figure.tight_layout()
    chart = REPORT / "charts" / "utility_exhaustion_attribution.png"
    chart.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(chart, dpi=180, bbox_inches="tight")
    plt.close(figure)


def update_notebook(decision: dict) -> None:
    path = REPORT / "binance_runup_research.ipynb"
    notebook = nbformat.read(path, as_version=4)
    notebook.cells = [
        cell
        for cell in notebook.cells
        if "utility-exhaustion-attribution" not in cell.get("metadata", {}).get("tags", [])
    ]
    pure = decision["contrasts"]["wick_minus_tail35"]
    posthoc = decision["posthoc_hypothesis"]
    notebook.cells.append(
        nbformat.v4.new_markdown_cell(
            f"""## Static-tail versus wick-exit attribution

No policy is re-selected in this diagnostic. The pure volume-backed upper-wick timing effect is {pure['mean_delta']:.2%} per signal row and {pure['event_mean_delta']:.2%} per independent event, but row- and event-cluster intervals all cross zero. Widening the trail from 30% to 35% is uniformly harmful in confirmation. A derived 30%-trail plus wick policy has {posthoc['default_cost_summary']['expectancy']:.2%} expectancy and PF {posthoc['default_cost_summary']['profit_factor']:.2f}, but it was generated after inspecting confirmation and is therefore prospective-label-only.""",
            metadata={"tags": ["utility-exhaustion-attribution"]},
        )
    )
    nbformat.write(notebook, path)


def main() -> None:
    verification = json.loads((REPORT / "utility_exhaustion_attribution_verification.json").read_text(encoding="utf-8"))
    if not verification.get("passed"):
        raise RuntimeError("Utility exhaustion attribution verifier did not pass")
    decision = json.loads((REPORT / "utility_exhaustion_attribution_decision.json").read_text(encoding="utf-8"))
    folds = pd.read_csv(REPORT / "utility_exhaustion_attribution_folds.csv")
    make_chart(decision, folds)
    update_notebook(decision)

    pure = decision["contrasts"]["wick_minus_tail35"]
    trail = decision["contrasts"]["trail35_minus_trail30"]
    combined = decision["contrasts"]["wick_minus_baseline"]
    posthoc = decision["posthoc_hypothesis"]
    posthoc_summary = posthoc["default_cost_summary"]
    posthoc_baseline = posthoc["increment_vs_baseline"]
    posthoc_current = posthoc["increment_vs_current_wick35"]
    section = f"""{BEGIN}
## 卖点析因：长上影有方向，35%追踪本身明确拖累

这次没有再选参数，而是把已经测试过的退出规则放在同一批 8 小时入场、同一组确认折、同一成本下逐笔配对。88 条信号进一步按“同一币 14 天只保留首次信号”压缩为 67 个独立事件，避免 ARIA 等同一轮行情被重复计算。

结论可以拆成三层：

- **把追踪距离从 30% 放宽到 35% 是负贡献。** 只改变 12 条路径，12 条全部变差，逐行平均增量 {trail['mean_delta']:.2%}；周块与币种 bootstrap 的 95% 上界都低于 0。若未来使用 +150% 减半的右尾方案，30% 追踪优于 35%。
- **放量长上影择时有正向点估计，但尚未独立识别。** 在相同 35% 静态尾部上加入长上影退出，改变 13 条路径，9 条改善、4 条恶化；逐行平均 +{pure['mean_delta']:.2%}，事件去重后 +{pure['event_mean_delta']:.2%}。但逐行和事件口径下，按周及按币的 95% 下界全部小于 0。
- **此前组合改善主要来自长上影，不是 35% 追踪。** `half150_trail35_wick` 相对冻结退出的逐行增量 {combined['mean_delta']:.2%} 中，{decision['attribution']['wick_share_of_combined_point_increment']:.1%} 由长上影择时贡献；事件去重后的组合增量仅 {combined['event_mean_delta']:.2%}。

基于这个析因结果，新增一个只允许真正前瞻验证的事后假设：**盈利 +150% 卖一半，余仓使用 30% 追踪；价格至少翻倍后，如完成的 4 小时 K 线上影占振幅≥35%、收盘位于振幅下 35%、成交量≥过去 18 根均值的 1.5 倍，则下一根 4 小时开盘退出。**

该 `half150_trail30_wick_posthoc` 在历史确认中的点估计为：{posthoc_summary['trades']} 笔、期望 {posthoc_summary['expectancy']:.2%}、PF {posthoc_summary['profit_factor']:.2f}、组合收益 {posthoc_summary['total_return_on_initial_equity']:.2%}，四个确认折 PF 均大于 1。相对现有 35% 长上影版本，6 条被改变的路径全部改善，逐行与事件聚类下界均大于 0；但它是看完确认结果后才组合出来的，**不能把这些数字当作新确认集表现**。相对冻结主退出，它的逐行增量 {posthoc_baseline['mean_delta']:.2%}、独立事件增量 {posthoc_baseline['event_mean_delta']:.2%}，区间仍跨 0，而且它也不全面支配单纯 30% 追踪方案。

因此主退出不变；未来标签同时保留冻结退出、单纯 30% 尾部、35% 长上影和新 30% 长上影四条路径。新规则只有在真正 OOS 中相对冻结退出和单纯 30% 尾部的事件级增量下界都大于 0，才有升级资格。

![退出规则析因](charts/utility_exhaustion_attribution.png)
{END}"""
    report_path = REPORT / "REPORT.md"
    report_path.write_text(replace_section(report_path.read_text(encoding="utf-8"), section), encoding="utf-8")

    summary_path = REPORT / "analysis_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["utility_exhaustion_attribution"] = decision
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    model_section = f"""{BEGIN}
## Exit-factor attribution and prospective derived policy

- Fixed factorial attribution uses 88 paired signals and 67 first-signal independent events; no policy is re-selected.
- Moving the static trail from 30% to 35% changes 12 paths and harms all 12; both row-cluster upper bounds are negative.
- The pure upper-wick timing effect is +{pure['mean_delta']:.2%} per row and +{pure['event_mean_delta']:.2%} per event, but all clustered lower bounds cross zero.
- The combined wick-35% point improvement is mostly attributable to wick timing, not the wider trail.
- `hard25_be30_half150_trail30_wick_posthoc` is a post-confirmation hypothesis recorded prospectively only. It cannot support historical promotion, ranking changes, or automatic trading.
{END}"""
    model_path = REPORT / "MODEL_CARD.md"
    model_path.write_text(replace_section(model_path.read_text(encoding="utf-8"), model_section), encoding="utf-8")

    chart_section = f"""{BEGIN}
## Exit-factor attribution

- `charts/utility_exhaustion_attribution.png`: row/event point attribution, pure-wick clustered uncertainty, and policy PF by confirmation fold.
{END}"""
    chart_path = REPORT / "CHART_MAP.md"
    chart_path.write_text(replace_section(chart_path.read_text(encoding="utf-8"), chart_section), encoding="utf-8")
    print(
        json.dumps(
            {
                "updated": True,
                "pure_wick_increment_gate": decision["pure_wick_increment_gate_pass"],
                "posthoc_policy": decision["posthoc_exploratory_policy"],
                "posthoc_vs_current_increment": posthoc_current["mean_delta"],
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
