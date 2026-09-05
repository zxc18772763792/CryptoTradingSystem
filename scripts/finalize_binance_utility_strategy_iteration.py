"""Add the taker robustness and full-runner rejection to the durable report."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import nbformat
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
BEGIN = "<!-- UTILITY_STRATEGY_ITERATION_BEGIN -->"
END = "<!-- UTILITY_STRATEGY_ITERATION_END -->"


def replace_section(text: str, section: str) -> str:
    if BEGIN in text and END in text:
        start = text.index(BEGIN)
        finish = text.index(END, start) + len(END)
        return text[:start] + section + text[finish:]
    return text.rstrip() + "\n\n" + section + "\n"


def make_chart(definitions: pd.DataFrame, matches: pd.DataFrame, cal: pd.DataFrame, conf: pd.DataFrame) -> None:
    figure, axes = plt.subplots(1, 3, figsize=(15.5, 4.8))
    figure.suptitle("Buy-flow independence and breakout full-runner audit", fontsize=15, fontweight="bold")
    thresholds = [48, 49, 50, 51]
    for period, color, marker in [("calibration", "#64748b", "o"), ("confirmation", "#2563eb", "s")]:
        subset = definitions[
            (definitions["period"] == period)
            & definitions["definition"].isin([f"mean_ge_{value}pct" for value in thresholds])
        ].copy()
        subset["threshold"] = subset["definition"].str.extract(r"(\d+)").astype(int)
        subset = subset.sort_values("threshold")
        axes[0].plot(
            subset["threshold"], 100 * subset["selected_precision"],
            marker=marker, color=color, label=period.title(), linewidth=2,
        )
    axes[0].set(title="Nearby taker-share thresholds", xlabel="Taker buy share threshold (%)", ylabel="+200% precision (%)")
    axes[0].legend(frameon=False)

    match_labels = ["Return", "Return +\npath efficiency", "Return +\nmodel score"]
    match_p = matches["one_sided_p"].astype(float).to_numpy()
    colors = ["#16a34a" if value < 0.05 else "#f59e0b" for value in match_p]
    axes[1].bar(match_labels, match_p, color=colors)
    axes[1].axhline(0.05, color="#0f172a", linestyle="--", linewidth=1, label="5% threshold")
    axes[1].set(title="Price-path matched permutation", ylabel="One-sided p-value", ylim=(0, max(0.10, match_p.max() * 1.25)))
    axes[1].legend(frameon=False, fontsize=8)
    for index, value in enumerate(match_p):
        axes[1].text(index, value + 0.003, f"{value:.3f}", ha="center", fontsize=8)

    order = [
        "breakout6_half150_trail30_wick",
        "breakout6_full_runner_a100_t25_wick",
        "breakout6_full_runner_a100_t30_wick",
        "breakout6_full_runner_a100_t35_wick",
        "breakout6_full_runner_a150_t25_wick",
        "breakout6_full_runner_a150_t30_wick",
        "breakout6_full_runner_a150_t35_wick",
    ]
    labels = ["Current", "100/25", "100/30", "100/35", "150/25", "150/30", "150/35"]
    cal_indexed = cal.set_index("policy")
    conf_indexed = conf[conf["slippage_bps_each_side"] == 5.0].set_index("policy")
    x = np.arange(len(order))
    width = 0.36
    axes[2].bar(x - width / 2, 100 * cal_indexed.loc[order, "expectancy"], width, color="#64748b", label="Calibration 14d")
    axes[2].bar(x + width / 2, 100 * conf_indexed.loc[order, "expectancy"], width, color="#2563eb", label="Confirmation 30d")
    axes[2].axhline(0, color="#0f172a", linewidth=1)
    axes[2].set_xticks(x, labels, rotation=30)
    axes[2].set(title="Full-runner exit family", ylabel="Trade expectancy (%)")
    axes[2].legend(frameon=False, fontsize=8)

    for axis in axes:
        axis.grid(axis="y", alpha=0.18)
        axis.spines[["top", "right"]].set_visible(False)
    figure.tight_layout()
    chart = REPORT / "charts" / "utility_strategy_iteration.png"
    chart.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(chart, dpi=180, bbox_inches="tight")
    plt.close(figure)


def make_partial_fraction_chart(calibration: pd.DataFrame, confirmation: pd.DataFrame) -> None:
    default = confirmation[confirmation["slippage_bps_each_side"] == 5.0].sort_values("partial_fraction")
    calibration = calibration.sort_values("partial_fraction")
    x = np.arange(3)
    width = 0.36
    figure, axis = plt.subplots(figsize=(8.2, 4.8))
    axis.bar(x - width / 2, 100 * calibration["expectancy"], width, color="#64748b", label="Calibration 14d")
    axis.bar(x + width / 2, 100 * default["expectancy"], width, color="#2563eb", label="Confirmation 30d")
    axis.set_xticks(x, ["Sell 25%", "Sell 50%", "Sell 75%"])
    axis.set(title="Partial-sale fraction reverses across time", ylabel="Trade expectancy (%)")
    axis.legend(frameon=False, loc="center left", bbox_to_anchor=(1.01, 0.5))
    axis.grid(axis="y", alpha=0.18)
    axis.spines[["top", "right"]].set_visible(False)
    for xpos, value in zip(x - width / 2, calibration["expectancy"], strict=True):
        axis.text(xpos, 100 * value + 0.15, f"{value:.1%}", ha="center", fontsize=8)
    for xpos, value in zip(x + width / 2, default["expectancy"], strict=True):
        axis.text(xpos, 100 * value + 0.15, f"{value:.1%}", ha="center", fontsize=8)
    figure.tight_layout()
    figure.savefig(REPORT / "charts" / "utility_breakout_partial_fraction.png", dpi=180, bbox_inches="tight")
    plt.close(figure)


def make_regime_router_chart(
    calibration: pd.DataFrame,
    confirmation: pd.DataFrame,
    segments: pd.DataFrame,
) -> None:
    routes = [
        "semantic_vol_high25_low75",
        "calibration_vol_high75_low50",
        "semantic_risk_bull25_bear_narrow75",
        "calibration_market_state_min5",
    ]
    labels = ["High vol 25 / low 75", "High vol 75 / low 50", "Risk-state rule", "Learned state min 5"]
    cal = calibration.set_index("route").loc[routes]
    conf = confirmation[confirmation["slippage_bps_each_side"] == 5.0].set_index("route").loc[routes]
    figure, axes = plt.subplots(1, 2, figsize=(14.5, 5.0))
    figure.suptitle("Regime routing is sparse and reverses across time", fontsize=15, fontweight="bold")
    x = np.arange(len(routes))
    width = 0.36
    axes[0].bar(x - width / 2, 100 * cal["paired_increment_mean"], width, color="#64748b", label="Calibration")
    axes[0].bar(x + width / 2, 100 * conf["paired_increment_mean"], width, color="#2563eb", label="Confirmation")
    axes[0].axhline(0, color="#0f172a", linewidth=1)
    axes[0].set_xticks(x, labels, rotation=22, ha="right")
    axes[0].set(title="Paired increment versus fixed 50% sale", ylabel="Increment (percentage points)")
    axes[0].legend(frameon=False)

    support = segments[
        (segments["dimension"] == "market_state") & (segments["fraction"] == 0.25)
    ].copy()
    pivot = support.pivot(index="segment", columns="period", values="affected_rows").fillna(0)
    pivot = pivot.reindex(sorted(pivot.index))
    y = np.arange(len(pivot))
    axes[1].barh(y - 0.18, pivot.get("calibration", 0), 0.36, color="#64748b", label="Calibration")
    axes[1].barh(y + 0.18, pivot.get("confirmation", 0), 0.36, color="#2563eb", label="Confirmation")
    axes[1].axvline(5, color="#dc2626", linestyle="--", linewidth=1, label="Minimum calibration support")
    axes[1].set_yticks(y, pivot.index)
    axes[1].set(title="Trades whose return changes with sale fraction", xlabel="Distinct affected signals")
    axes[1].legend(frameon=False, fontsize=8)

    for axis in axes:
        axis.grid(axis="y", alpha=0.18)
        axis.spines[["top", "right"]].set_visible(False)
    figure.tight_layout()
    figure.savefig(REPORT / "charts" / "utility_regime_partial_router.png", dpi=180, bbox_inches="tight")
    plt.close(figure)


def update_notebook(robust: dict, runner: dict, partial: dict, regime: dict, operational: dict) -> None:
    path = REPORT / "binance_runup_research.ipynb"
    notebook = nbformat.read(path, as_version=4)
    notebook.cells = [
        cell for cell in notebook.cells
        if "utility-strategy-iteration" not in cell.get("metadata", {}).get("tags", [])
    ]
    full_path = robust["matched_permutation"][1]
    notebook.cells.extend(
        [
            nbformat.v4.new_markdown_cell(
                f"""## Taker-flow independence and breakout full-runner audit

The fixed 50% taker-buy-share annotation is directionally robust from 48% to 51%, but matching on both early return and path efficiency gives a one-sided permutation p-value of {full_path['one_sided_p']:.3f}. It is not independently identified and remains rank-neutral. All six full-position runner exits fail the 14-day calibration gate. A narrower partial-sale test selects 75% in calibration but reverses in confirmation, where 25% is best; the stable 50% rule is retained. Confirmation-only winners are counterexamples, not promotions.""",
                metadata={"tags": ["utility-strategy-iteration"]},
            ),
            nbformat.v4.new_code_cell(
                """display(pd.read_csv(ROOT / 'utility_taker_flow_robustness_definitions.csv'))
display(pd.read_csv(ROOT / 'utility_taker_flow_price_matched_permutation.csv'))
display(pd.read_csv(ROOT / 'utility_breakout_runner_calibration.csv'))
display(pd.read_csv(ROOT / 'utility_breakout_runner_confirmation.csv'))
display(pd.read_csv(ROOT / 'utility_breakout_partial_fraction_calibration.csv'))
display(pd.read_csv(ROOT / 'utility_breakout_partial_fraction_confirmation.csv'))""",
                metadata={"tags": ["utility-strategy-iteration"]},
            ),
            nbformat.v4.new_markdown_cell(
                f"""### Regime-router and forward-monitor audit

Only {regime['affected_calibration_signals']} calibration signals and {regime['affected_confirmation_signals']} confirmation signals change outcome when the partial-sale fraction changes. The calibration-selected state router therefore falls back to the fixed 50% rule and no forward field changes. The immutable monitor contains snapshots for {', '.join(operational['immutable_snapshot_dates_present'])}, but is missing {', '.join(operational['immutable_snapshot_dates_missing'])}; the late live-data dry-run passed data quality but is explicitly excluded from true OOS performance.""",
                metadata={"tags": ["utility-strategy-iteration"]},
            ),
            nbformat.v4.new_code_cell(
                """display(pd.read_csv(ROOT / 'utility_regime_partial_router_calibration.csv'))
display(pd.read_csv(ROOT / 'utility_regime_partial_router_confirmation.csv'))
display(pd.read_csv(ROOT / 'utility_regime_partial_fraction_segments.csv'))
display(json.loads((ROOT / 'forward_monitor_operational_audit_2026-07-21.json').read_text(encoding='utf-8')))""",
                metadata={"tags": ["utility-strategy-iteration"]},
            ),
        ]
    )
    nbformat.write(notebook, path)


def main() -> None:
    verification = json.loads((REPORT / "utility_strategy_iteration_verification.json").read_text(encoding="utf-8"))
    if not verification.get("passed"):
        raise RuntimeError("Utility strategy iteration verifier did not pass")
    robust = json.loads((REPORT / "utility_taker_flow_robustness_decision.json").read_text(encoding="utf-8"))
    runner = json.loads((REPORT / "utility_breakout_runner_decision.json").read_text(encoding="utf-8"))
    partial = json.loads((REPORT / "utility_breakout_partial_fraction_decision.json").read_text(encoding="utf-8"))
    regime = json.loads((REPORT / "utility_regime_partial_router_decision.json").read_text(encoding="utf-8"))
    operational = json.loads(
        (REPORT / "forward_monitor_operational_audit_2026-07-21.json").read_text(encoding="utf-8")
    )
    definitions = pd.read_csv(REPORT / "utility_taker_flow_robustness_definitions.csv")
    matches = pd.read_csv(REPORT / "utility_taker_flow_price_matched_permutation.csv")
    cal = pd.read_csv(REPORT / "utility_breakout_runner_calibration.csv")
    conf = pd.read_csv(REPORT / "utility_breakout_runner_confirmation.csv")
    sensitivity = pd.read_csv(REPORT / "utility_breakout_runner_30d_calibration_sensitivity.csv")
    partial_cal = pd.read_csv(REPORT / "utility_breakout_partial_fraction_calibration.csv")
    partial_conf = pd.read_csv(REPORT / "utility_breakout_partial_fraction_confirmation.csv")
    regime_cal = pd.read_csv(REPORT / "utility_regime_partial_router_calibration.csv")
    regime_conf = pd.read_csv(REPORT / "utility_regime_partial_router_confirmation.csv")
    regime_segments = pd.read_csv(REPORT / "utility_regime_partial_fraction_segments.csv")
    make_chart(definitions, matches, cal, conf)
    make_partial_fraction_chart(partial_cal, partial_conf)
    make_regime_router_chart(regime_cal, regime_conf, regime_segments)
    update_notebook(robust, runner, partial, regime, operational)

    fixed = definitions[definitions["definition"] == "mean_ge_50pct"].set_index("period")
    full_path = robust["matched_permutation"][1]
    current = cal[cal["policy"] == runner["current_router"]].iloc[0]
    candidates = cal[cal["policy"] != runner["current_router"]]
    best_confirmation = conf[
        (conf["slippage_bps_each_side"] == 5.0)
        & (conf["policy"] == runner["confirmation_point_estimate_best_policy"])
    ].iloc[0]
    partial_default = partial_conf[partial_conf["slippage_bps_each_side"] == 5.0].set_index("partial_fraction")
    report_section = f"""{BEGIN}
## 主动买盘的独立性与“整仓跑”退出审计

主动买盘占比的方向性不是由单一阈值造成：48%–51% 四个邻近阈值在校准期和确认期都提高了 +200% 浓度。固定 50% 条件将校准精度从 {fixed.loc['calibration', 'base_precision']:.2%} 提至 {fixed.loc['calibration', 'selected_precision']:.2%}，确认精度从 {fixed.loc['confirmation', 'base_precision']:.2%} 提至 {fixed.loc['confirmation', 'selected_precision']:.2%}。但这还不能证明它是独立因子：只匹配早期涨幅时随机检验 p={robust['matched_permutation'][0]['one_sided_p']:.3f}；同时匹配涨幅和路径效率后 p={full_path['one_sided_p']:.3f}，没有越过 5% 门槛。结合校准期筛选后 14 天效用反而降至 {robust['fixed_calibration_utility_expectancy_14d']:.2%}，结论仍是“值得前瞻记录的识别注释”，不是买入开关。

卖出端测试了六个“突破确认后不卖一半、整仓追踪”的固定方案。当前规则在 14 天校准期的组合交易期望为 **{current['expectancy']:.2%}**、PF **{current['profit_factor']:.2f}**；六个整仓候选的期望仅为 {candidates['expectancy'].min():.2%}–{candidates['expectancy'].max():.2%}，相对当前规则的配对增量全部为负（{candidates['paired_increment_mean'].min():.2%} 至 {candidates['paired_increment_mean'].max():.2%}），没有任何候选通过校准门槛。可用的两个早期 30 天窗仅 18 笔，所有候选绝对期望都为负；其中最好的配对增量 {runner['early_30d_sensitivity_best_paired_increment']:.2%} 不足以改变结论。

确认期里，`{runner['confirmation_point_estimate_best_policy']}` 的点估计达到 {best_confirmation['expectancy']:.2%}/PF {best_confirmation['profit_factor']:.2f}，高于当前路由；但这正是未被校准期支持、查看后段样本才显得漂亮的规则。它被保留为过拟合反例，不进入前瞻监控。当前执行假设仍是：原 8 小时买点；突破只管理已有仓位；+150% 卖出一半，余仓 30% 追踪并保留量能长上影退出；不加仓、不自动交易。

![主动买盘独立性与整仓退出审计](charts/utility_strategy_iteration.png)

分批比例也不是稳定参数。只改变 +150% 时卖出的比例，校准期选择 75%：相对卖 50% 增加 {partial_cal.loc[partial_cal['partial_fraction'] == 0.75, 'paired_increment_mean'].iloc[0]:.2%}，三个校准窗为“两正一零”。但确认期方向反转：卖 25% 的期望为 {partial_default.loc[0.25, 'expectancy']:.2%}，卖 50% 为 {partial_default.loc[0.50, 'expectancy']:.2%}，卖 75% 为 {partial_default.loc[0.75, 'expectancy']:.2%}；校准选中的 75% 相对 50% 变为 {partial_default.loc[0.75, 'paired_increment_mean']:.2%}。这说明最优兑现比例依赖市场状态，当前保留较中性的 50%，不做自适应切换。

![突破后分批卖出比例的时间反转](charts/utility_breakout_partial_fraction.png)
{END}"""
    regime_cal_indexed = regime_cal.set_index("route")
    regime_conf_indexed = regime_conf[
        regime_conf["slippage_bps_each_side"] == 5.0
    ].set_index("route")
    regime_addendum = f"""

### 市场状态能否决定卖出比例

答案是否定的，至少现有样本还不能。卖出比例真正改变交易结果的信号，校准期只有 **{regime['affected_calibration_signals']} 笔**、确认期只有 **{regime['affected_confirmation_signals']} 笔**。语义上看似合理的“高波动卖 25%、低波动卖 75%”在校准期相对固定卖 50% 减少 {regime_cal_indexed.loc['semantic_vol_high25_low75', 'paired_increment_mean']:.2%}，确认期却增加 {regime_conf_indexed.loc['semantic_vol_high25_low75', 'paired_increment_mean']:.2%}；而校准期偏好的“高波动卖 75%”在确认期反向失效。按市场状态学习的规则因每个状态少于 5 笔有效支持，全部回退到 50%。因此，状态只保留为失效诊断，不进入卖出路由。

![市场状态路由的稀疏支持与时间反转](charts/utility_regime_partial_router.png)

### 前瞻监控的运营审计

不可覆盖主快照目前只有 {', '.join(operational['immutable_snapshot_dates_present'])}，缺失 {', '.join(operational['immutable_snapshot_dates_missing'])}；其中真正按 08:20 时钟准时落盘的只有 {', '.join(operational['punctual_snapshot_dates'])}。7 月 21 日事后实时公开数据 dry-run 的 527/527 个合约通过覆盖和时效审计，但运行晚于冻结决策时钟，没有写入快照，也不计入真正 OOS。当前成熟的 8 小时效用标签和启动微结构标签仍均为 **0**，所以历史上的纸面候选不能升级为可执行策略。
"""
    report_section = report_section.replace(f"\n{END}", f"{regime_addendum}\n{END}")
    report_path = REPORT / "REPORT.md"
    report_path.write_text(replace_section(report_path.read_text(encoding="utf-8"), report_section), encoding="utf-8")

    model_section = f"""{BEGIN}
## Taker robustness and breakout full-runner decision

- Taker buy share 48%-51% is directionally robust, but return plus path-efficiency matched permutation p={full_path['one_sided_p']:.3f}; it remains a prospective rank-neutral annotation.
- Six full-position runner exits fail calibration versus `{runner['current_router']}`; no forward exit field or counterfactual is added.
- Calibration selects a 75% partial sale, but confirmation reverses and favors 25%; the stable 50% partial fraction is retained and no adaptive switch is allowed.
- Confirmation's best runner is posthoc-only. Primary entry, exit, stage veto, paper gate, position sizing and trading authority are unchanged.
{END}"""
    model_addendum = f"""
- Regime-conditioned partial-sale routing is rejected: only {regime['affected_calibration_signals']} calibration signals are affected, below the minimum support gate, and directional preferences reverse in confirmation.
- Forward operations are incomplete: immutable snapshots for 2026-07-20 and 2026-07-21 are absent, and mature utility labels remain zero. The late live dry-run is diagnostic only and is not backfilled as OOS.
"""
    model_section = model_section.replace(f"\n{END}", f"{model_addendum}\n{END}")
    model_path = REPORT / "MODEL_CARD.md"
    model_path.write_text(replace_section(model_path.read_text(encoding="utf-8"), model_section), encoding="utf-8")

    chart_section = f"""{BEGIN}
## Taker independence and full-runner audit

- `charts/utility_strategy_iteration.png`: nearby taker-share thresholds, price-path matched permutation p-values, and calibration/confirmation expectancy for the partial-exit and full-runner families.
- `charts/utility_breakout_partial_fraction.png`: calibration/confirmation expectancy for selling 25%, 50%, or 75% at the fixed +150% target.
{END}"""
    chart_addendum = """
- `charts/utility_regime_partial_router.png`: calibration/confirmation paired increments for fixed regime routers and market-state counts of fraction-sensitive signals.
"""
    chart_section = chart_section.replace(f"\n{END}", f"{chart_addendum}\n{END}")
    chart_path = REPORT / "CHART_MAP.md"
    chart_path.write_text(replace_section(chart_path.read_text(encoding="utf-8"), chart_section), encoding="utf-8")

    summary_path = REPORT / "analysis_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["utility_taker_flow_robustness"] = robust
    summary["utility_breakout_full_runner"] = runner
    summary["utility_breakout_partial_fraction"] = partial
    summary["utility_regime_partial_router"] = regime
    summary["forward_monitor_operational_audit_2026_07_21"] = operational
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({
        "updated": True,
        "taker_classification": robust["classification"],
        "runner_classification": runner["classification"],
        "partial_fraction_classification": partial["classification"],
        "regime_router_classification": regime["classification"],
        "forward_monitor_classification": operational["classification"],
        "forward_change_allowed": runner["forward_change_allowed"],
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
