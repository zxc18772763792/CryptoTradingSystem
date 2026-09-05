"""Finalize the breakout-confirmed hold-router hypothesis and report section."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
import nbformat
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
BEGIN = "<!-- UTILITY_BREAKOUT_HOLD_ROUTER_BEGIN -->"
END = "<!-- UTILITY_BREAKOUT_HOLD_ROUTER_END -->"
ROUTE = "breakout6_half150_trail30_wick"


def replace_section(text: str, section: str) -> str:
    if BEGIN in text and END in text:
        start = text.index(BEGIN)
        finish = text.index(END, start) + len(END)
        return text[:start].rstrip() + "\n\n" + section + "\n" + text[finish:].lstrip()
    return text.rstrip() + "\n\n" + section + "\n"


def make_chart(decision: dict, summary: pd.DataFrame) -> None:
    indexed = summary.set_index("route")
    detail = decision["route_details"][ROUTE]
    static = decision["route_details"]["breakout6_half150_trail30"]
    figure, axes = plt.subplots(1, 3, figsize=(18, 5.2))

    axes[0].bar(
        ["Score-open\nevents", "Breakout-confirmed\nevents"],
        [5 / 67, 5 / 18],
        color=["#2563eb", "#f59e0b"],
    )
    axes[0].set_ylim(0, 0.33)
    axes[0].set_ylabel("Independent-event +200% precision")
    axes[0].set_title("Breakout confirms propensity")
    for index, value in enumerate([5 / 67, 5 / 18]):
        axes[0].text(index, value + 0.012, f"{value:.1%}", ha="center", fontweight="bold")

    routes = ["baseline_no_router", "breakout6_half150_trail30", ROUTE]
    labels = ["Frozen exit", "Breakout tail30", "Breakout tail30 + wick"]
    colors = ["#2563eb", "#94a3b8", "#16a34a"]
    expectancy = [float(indexed.loc[name, "expectancy"]) for name in routes]
    axes[1].bar(labels, expectancy, color=colors)
    axes[1].set_ylim(0, max(expectancy) * 1.28)
    axes[1].set_ylabel("Portfolio trade expectancy")
    axes[1].set_title("Hold routing, not breakout buying")
    axes[1].tick_params(axis="x", rotation=12)
    for index, name in enumerate(routes):
        axes[1].text(
            index,
            expectancy[index] + 0.006,
            f"{expectancy[index]:.1%}\nPF {indexed.loc[name, 'profit_factor']:.2f}",
            ha="center",
            fontsize=8,
        )

    interval_items = [
        ("all/week", detail["week_increment_bootstrap_all_signals"]),
        ("all/symbol", detail["symbol_increment_bootstrap_all_signals"]),
        ("events/week", detail["week_increment_bootstrap_independent_events"]),
        ("events/symbol", detail["symbol_increment_bootstrap_independent_events"]),
    ]
    centers = np.asarray([item[1]["mean_delta_median"] for item in interval_items])
    lower = np.asarray([item[1]["mean_delta_lower_95pct"] for item in interval_items])
    upper = np.asarray([item[1]["mean_delta_upper_95pct"] for item in interval_items])
    x = np.arange(len(interval_items))
    axes[2].errorbar(
        x,
        centers,
        yerr=np.vstack([centers - lower, upper - centers]),
        fmt="o",
        color="#16a34a",
        capsize=6,
    )
    axes[2].axhline(0, color="#0f172a", linewidth=0.9)
    axes[2].set_xticks(x, [item[0] for item in interval_items], rotation=14)
    axes[2].set_ylabel("Paired return increment")
    axes[2].set_title("Prospective router clears historical CIs")
    static_event_low = min(
        static["week_increment_bootstrap_independent_events"]["mean_delta_lower_95pct"],
        static["symbol_increment_bootstrap_independent_events"]["mean_delta_lower_95pct"],
    )
    axes[2].text(
        0.02,
        0.96,
        f"Static tail30 event lower bound: {static_event_low:.1%}",
        transform=axes[2].transAxes,
        va="top",
        fontsize=8,
        color="#64748b",
    )

    figure.suptitle("Breakout confirmation is useful after the frozen 8h entry", fontsize=14, fontweight="bold")
    figure.tight_layout()
    chart = REPORT / "charts" / "utility_breakout_hold_router.png"
    chart.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(chart, dpi=180, bbox_inches="tight")
    plt.close(figure)


def update_notebook(decision: dict, summary: pd.DataFrame) -> None:
    path = REPORT / "binance_runup_research.ipynb"
    notebook = nbformat.read(path, as_version=4)
    notebook.cells = [
        cell for cell in notebook.cells
        if "utility-breakout-hold-router" not in cell.get("metadata", {}).get("tags", [])
    ]
    indexed = summary.set_index("route")
    routed = indexed.loc[ROUTE]
    detail = decision["route_details"][ROUTE]
    notebook.cells.append(
        nbformat.v4.new_markdown_cell(
            f"""## Breakout-confirmed hold router

The frozen 8h entry remains unchanged. A completed six-bar breakout within 24 hours switches only profit-side management at the following 4h open: +150% half-sale, 30% trailing remainder, and a next-open volume-backed upper-wick exhaustion exit after the trade has doubled. Historical expectancy is {routed['expectancy']:.2%}, PF {routed['profit_factor']:.2f}; paired all-row and independent-event gains are {detail['paired_increment_mean_all_signals']:.2%} and {detail['independent_event_increment_mean']:.2%}. All four week/symbol lower bounds are positive, but the router was discovered after confirmation inspection and is frozen only as a prospective counterfactual. It does not change the primary exit or authorize trading.""",
            metadata={"tags": ["utility-breakout-hold-router"]},
        )
    )
    nbformat.write(notebook, path)


def main() -> None:
    verification = json.loads((REPORT / "utility_breakout_hold_router_verification.json").read_text(encoding="utf-8"))
    if not verification.get("passed"):
        raise RuntimeError("Breakout hold-router verifier did not pass")
    decision = json.loads((REPORT / "utility_breakout_hold_router_decision.json").read_text(encoding="utf-8"))
    summary = pd.read_csv(REPORT / "utility_breakout_hold_router_summary.csv")
    make_chart(decision, summary)
    update_notebook(decision, summary)

    indexed = summary.set_index("route")
    baseline = indexed.loc["baseline_no_router"]
    static = indexed.loc["breakout6_half150_trail30"]
    routed = indexed.loc[ROUTE]
    detail = decision["route_details"][ROUTE]
    static_detail = decision["route_details"]["breakout6_half150_trail30"]
    section = f"""{BEGIN}
## 突破不是追买点，但可以决定是否给已持有赢家更多空间

完整买点前沿已经证明：等 `breakout6` 确认后再买，同一批信号平均少赚17.80%。本轮因此不改变冻结的8小时买点，而是把突破改成持仓管理信号。只有在8小时入场后的24小时内，某根完整4小时K线收盘突破此前六根最高价、当根涨幅不超过10%、相对入场开盘涨幅不超过25%时，才允许在下一根4小时开盘切换盈利侧退出；-25%硬止损和+30%后的保本规则不变。

确认期88条信号中有21条出现突破，包含6条 +200% 行；合并同币14天重复信号后为18个事件、命中5个，事件精度从全部信号的5/67=7.46%提高到5/18=27.78%。21条都在完成K线后的下一根开盘完成政策切换，切换前没有任何一条已经减半或启动追踪，排除了用同一根K线回看改规则。

单纯切换为“+150%减半、余仓30%追踪”的点估计最高：期望 **{static['expectancy']:.2%}**、PF **{static['profit_factor']:.2f}**；但独立事件按周/按币的增量下界仍为 {static_detail['week_increment_bootstrap_independent_events']['mean_delta_lower_95pct']:.2%}/{static_detail['symbol_increment_bootstrap_independent_events']['mean_delta_lower_95pct']:.2%}，不能排除重复行情影响。

更稳健的前瞻版本是：**突破确认后把减半目标延后到+150%，余仓30%追踪；价格至少翻倍后，若完成的4小时K线上影占振幅≥35%、收盘位于振幅下35%、成交量≥过去18根均值1.5倍，则下一根开盘退出。** 与冻结退出相比：

- 65笔组合期望从 {baseline['expectancy']:.2%} 提高到 **{routed['expectancy']:.2%}**，PF从 {baseline['profit_factor']:.2f} 提高到 **{routed['profit_factor']:.2f}**，组合收益从 {baseline['total_return_on_initial_equity']:.2%} 提高到 **{routed['total_return_on_initial_equity']:.2%}**；
- 最大回撤为 {routed['max_drawdown']:.2%}，四折PF均大于1，六种市场状态中五种PF大于1；30bps单边滑点压力下期望仍为 {detail['cost_stress'][2]['expectancy']:.2%}、PF {detail['cost_stress'][2]['profit_factor']:.2f}；
- 逐信号配对增量为 **{detail['paired_increment_mean_all_signals']:.2%}**，独立事件增量为 **{detail['independent_event_increment_mean']:.2%}**；逐行按周/币95%下界为 {detail['week_increment_bootstrap_all_signals']['mean_delta_lower_95pct']:.2%}/{detail['symbol_increment_bootstrap_all_signals']['mean_delta_lower_95pct']:.2%}，事件口径为 {detail['week_increment_bootstrap_independent_events']['mean_delta_lower_95pct']:.2%}/{detail['symbol_increment_bootstrap_independent_events']['mean_delta_lower_95pct']:.2%}，四个下界均大于0；
- 实际只改变8条路径，7条改善、1条恶化；最大赢家占正利润 {detail['largest_positive_pnl_share']:.2%}，删除最大赢家后期望仍为 {detail['leave_largest_winner_out_expectancy']:.2%}。

这是目前最有意义的新发现，但证据等级仍是**方向性前瞻假设**：`breakout6` 是查看确认期后才选出的持仓路由，不能把同一批历史数据重新称为确认集。它已登记为 `breakout6_half150_trail30_wick_24h_next_open` 的30日只读反事实，不改变排名、主买点、主退出或自动交易权限。真正OOS样本目前仍为0；只有未来独立事件持续保持正增量，才允许升级纸面退出。

![突破确认后的持仓路由](charts/utility_breakout_hold_router.png)
{END}"""
    report_path = REPORT / "REPORT.md"
    report_path.write_text(replace_section(report_path.read_text(encoding="utf-8"), section), encoding="utf-8")

    analysis_path = REPORT / "analysis_summary.json"
    analysis = json.loads(analysis_path.read_text(encoding="utf-8"))
    analysis["utility_breakout_hold_router"] = decision
    analysis_path.write_text(json.dumps(analysis, ensure_ascii=False, indent=2), encoding="utf-8")

    model_section = f"""{BEGIN}
## Prospective breakout-confirmed hold router

- The frozen 8h entry is unchanged. A completed `breakout6` bar can switch only profit-side management at the following 4h open; all prior execution state is preserved.
- The prospective route uses +150% half-sale, 30% trailing remainder, and the existing volume-backed upper-wick next-open exit after a double.
- Historical point estimates are {routed['expectancy']:.2%} expectancy and PF {routed['profit_factor']:.2f}; paired all-row and independent-event increments are {detail['paired_increment_mean_all_signals']:.2%} and {detail['independent_event_increment_mean']:.2%}, with week/symbol lower bounds above zero.
- This is confirmation-mined and cannot be historically promoted. It is recorded only as a rank-neutral, non-trading forward counterfactual; the primary entry and exit remain frozen.
{END}"""
    model_path = REPORT / "MODEL_CARD.md"
    model_path.write_text(replace_section(model_path.read_text(encoding="utf-8"), model_section), encoding="utf-8")

    chart_section = f"""{BEGIN}
## Breakout-confirmed hold router

- `charts/utility_breakout_hold_router.png`: event-level +200% concentration, exit-route expectancy/PF, and paired all-row/event increment intervals.
{END}"""
    chart_path = REPORT / "CHART_MAP.md"
    chart_path.write_text(replace_section(chart_path.read_text(encoding="utf-8"), chart_section), encoding="utf-8")
    print(json.dumps({
        "updated": True,
        "prospective_router": decision["prospective_router"],
        "historical_promotion_allowed": False,
        "primary_entry_changed": False,
        "primary_exit_changed": False,
    }))


if __name__ == "__main__":
    main()
