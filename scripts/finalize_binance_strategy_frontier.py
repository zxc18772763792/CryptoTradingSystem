"""Merge staged-entry, hybrid-stop, and lifecycle falsifications into the report."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import nbformat
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
BEGIN = "<!-- STRATEGY_FRONTIER_BEGIN -->"
END = "<!-- STRATEGY_FRONTIER_END -->"


def replace_section(text: str, section: str) -> str:
    if BEGIN in text and END in text:
        before, rest = text.split(BEGIN, 1)
        _, after = rest.split(END, 1)
        return before.rstrip() + "\n\n" + section + "\n" + after.lstrip()
    return text.rstrip() + "\n\n" + section + "\n"


def clean_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return frame.replace({np.nan: None, np.inf: None, -np.inf: None}).to_dict(orient="records")


def make_charts(staged_catalog: pd.DataFrame, path: pd.DataFrame, lifecycle: dict[str, Any]) -> None:
    charts = REPORT / "charts"
    charts.mkdir(parents=True, exist_ok=True)
    blue, orange, grey = "#3B6FB6", "#D97732", "#AAB2BD"

    fig, axes = plt.subplots(1, 3, figsize=(15, 4.5))
    axes[0].plot(100 * staged_catalog["probe_fraction"], staged_catalog["profit_factor"], marker="o", color=blue)
    axes[0].axhline(1, color="#20262E", linestyle="--", linewidth=1)
    axes[0].set(title="Staged entry diagnostic", xlabel="Probe before confirmation (%)", ylabel="Profit factor")

    winner = path[(path["period"] == "confirmation") & (path["population"] == "eventual_200pct")]
    positions = np.arange(len(winner))
    width = 0.36
    axes[1].bar(positions - width / 2, 100 * winner["prior_intrabar_below_minus25_share"], width, label="Intrabar below -25%", color=orange)
    axes[1].bar(positions + width / 2, 100 * winner["prior_close_below_minus20_share"], width, label="4h close below -20%", color=blue)
    axes[1].set_xticks(positions, [f"+{int(100*x)}%" for x in winner["target_return"]])
    axes[1].set(title="Winner drawdown before target", xlabel="Later target", ylabel="Share of +200% winners (%)")
    axes[1].legend(frameon=False, fontsize=8)

    price = lifecycle["price_pooled"]
    combo = lifecycle["price_plus_age_pooled"]
    axes[2].bar(["Price", "Price + age"], [100 * price["top_2pct_precision"], 100 * combo["top_2pct_precision"]], color=[blue, grey])
    axes[2].set(title="Listing age ranking increment", ylabel="Top-2% precision (%)")
    for axis in axes:
        axis.grid(axis="y", color="#E5E7EB")
        axis.spines[["top", "right"]].set_visible(False)
    fig.suptitle("Additional buy/sell hypotheses did not replace the frozen challenger")
    fig.tight_layout()
    fig.savefig(charts / "strategy_frontier_falsifications.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def update_notebook() -> None:
    path = REPORT / "binance_runup_research.ipynb"
    notebook = nbformat.read(path, as_version=4)
    notebook.cells = [cell for cell in notebook.cells if "strategy-frontier" not in cell.get("metadata", {}).get("tags", [])]
    notebook.cells.extend(
        [
            nbformat.v4.new_markdown_cell(
                """## Strategy frontier falsifications

Three additional hypotheses were time-isolated: staged probe/add allocation, close-confirmed initial stops with a catastrophic floor, and contract listing age as an exchange-lifecycle ranking factor. Calibration retained zero probe, the 25% hard stop, and the original price ranking.""",
                metadata={"tags": ["strategy-frontier"]},
            ),
            nbformat.v4.new_code_cell(
                """staged = json.loads((ROOT / 'staged_entry_decision.json').read_text(encoding='utf-8'))
hybrid = json.loads((ROOT / 'hybrid_stop_decision.json').read_text(encoding='utf-8'))
lifecycle = json.loads((ROOT / 'lifecycle_factor_decision.json').read_text(encoding='utf-8'))
pd.Series({
    'selected probe fraction': staged['selected_probe_fraction'],
    'selected initial stop': hybrid['selected_policy'],
    'price top2 precision': lifecycle['price_pooled']['top_2pct_precision'],
    'price+age top2 precision': lifecycle['price_plus_age_pooled']['top_2pct_precision'],
    'price event capture': lifecycle['price_event_capture']['captured'],
    'price+age event capture': lifecycle['price_plus_age_event_capture']['captured'],
})""",
                metadata={"tags": ["strategy-frontier"]},
            ),
            nbformat.v4.new_code_cell(
                """display(pd.read_csv(ROOT / 'staged_entry_calibration.csv'))
display(pd.read_csv(ROOT / 'hybrid_stop_calibration.csv'))
display(pd.read_csv(ROOT / 'lifecycle_factor_fold_metrics.csv'))""",
                metadata={"tags": ["strategy-frontier"]},
            ),
        ]
    )
    nbformat.write(notebook, path)


def main() -> None:
    verification = json.loads((REPORT / "sequential_strategy_verification.json").read_text(encoding="utf-8"))
    required = ["hybrid_stop", "staged_entry", "lifecycle_factor"]
    if not all(verification.get("sections", {}).get(name, {}).get("passed") for name in required):
        raise RuntimeError("Strategy-frontier verifier sections did not pass")
    staged = json.loads((REPORT / "staged_entry_decision.json").read_text(encoding="utf-8"))
    hybrid = json.loads((REPORT / "hybrid_stop_decision.json").read_text(encoding="utf-8"))
    lifecycle = json.loads((REPORT / "lifecycle_factor_decision.json").read_text(encoding="utf-8"))
    staged_cal = pd.read_csv(REPORT / "staged_entry_calibration.csv")
    staged_catalog = pd.read_csv(REPORT / "staged_entry_confirmation_catalog_diagnostic.csv")
    hybrid_cal = pd.read_csv(REPORT / "hybrid_stop_calibration.csv")
    hybrid_path = pd.read_csv(REPORT / "hybrid_stop_path_summary.csv")
    lifecycle_folds = pd.read_csv(REPORT / "lifecycle_factor_fold_metrics.csv")
    lifecycle_segments = pd.read_csv(REPORT / "lifecycle_factor_age_segments.csv")
    make_charts(staged_catalog, hybrid_path, lifecycle)
    update_notebook()

    winner50 = hybrid_path[
        (hybrid_path["period"] == "confirmation")
        & (hybrid_path["population"] == "eventual_200pct")
        & np.isclose(hybrid_path["target_return"], 0.50)
    ].iloc[0]
    price = lifecycle["price_pooled"]
    combo = lifecycle["price_plus_age_pooled"]
    section = f"""{BEGIN}
## 继续寻找更好的买卖点：三项看似合理的方案均未取代当前基线

### 分阶段建仓没有解决“太晚买”

研究按校准期比较了在最初 `P0` 提前买入 0%/10%/25%/50%/75%/100%，并在 24 小时启动确认后补足剩余仓位。校准最终选择 **0% 提前试仓、确认后 100% 再进入**。确认期提前试仓会把大量未启动信号带入组合：确认后入场的单笔期望为 {staged['baseline_strategy_summary']['expectancy']:.2%}，而各提前试仓方案只剩约 1.7%–2.2%。因此“先买一点再说”被否决。

### 收盘止损不能证明硬止损是主要问题

8 个确认后仍有 +200% 空间的赢家中，在达到 +50% 前只有 {int(winner50['signals'] * winner50['prior_intrabar_below_minus25_share'])}/{int(winner50['signals'])} 曾在先前完整 K 线盘中跌破 -25%，而 4h 收盘跌破 -20% 的比例为 {winner50['prior_close_below_minus20_share']:.0%}。比较 -35%/-40%/-50% 灾难止损与 -15%/-20%/-25% 收盘确认止损后，校准仍选择 **-25% 盘中硬止损**。这说明部分插针确实存在，但不足以解释策略不稳定。

### 新上市合约更容易暴涨，但上市年龄不能加入排序

上市年龄单独使用的 top-2% lift 为 {lifecycle['age_only_pooled']['top_2pct_lift']:.2f} 倍，七折中年龄系数全部为负，确认了“越新越容易出现极端波动”的方向。但它与现有价格脆弱度高度重叠：加入价格模型后，top-2% 命中率从 **{price['top_2pct_precision']:.2%} 降至 {combo['top_2pct_precision']:.2%}**，独立事件覆盖从 **{lifecycle['price_event_capture']['captured']}/99 降至 {lifecycle['price_plus_age_event_capture']['captured']}/99**，AP 增量 bootstrap 上界仍为负。因此上市年龄只作风险/新币注释，不提高排名。

![新增买卖假设的反证](charts/strategy_frontier_falsifications.png)

这组三项反证进一步收敛了规则：**识别仍依赖冻结价格前 2% 加 24 小时 `MFE≥20% / 收盘≥5%`；买点仍是启动确认后的下一根 4h 开盘；退出仍是 -25% 硬止损、+30% 后保本、+100% 减半、余仓 25% 追踪、最长 30 天。** 历史收益仍未通过稳定性与置信下界门槛，继续保持 `watchlist_only`。
{END}"""
    report_path = REPORT / "REPORT.md"
    report_path.write_text(replace_section(report_path.read_text(encoding="utf-8"), section), encoding="utf-8")

    summary_path = REPORT / "analysis_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["staged_entry"] = staged
    summary["hybrid_stop"] = hybrid
    summary["exchange_lifecycle_factor"] = lifecycle
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    model_path = REPORT / "MODEL_CARD.md"
    model_section = f"""{BEGIN}
## Strategy-frontier falsifications

- Staged allocation selected 0% probe and 100% after 24h launch confirmation; early probes diluted expectancy.
- Hybrid close/catastrophic stops did not replace the 25% hard stop. Only {winner50['prior_intrabar_below_minus25_share']:.2%} of eventual +200% paths crossed -25% on a prior completed bar before +50%.
- Listing age was directionally negative in every fold, but adding it reduced top-2% precision from {price['top_2pct_precision']:.2%} to {combo['top_2pct_precision']:.2%} and event capture from {lifecycle['price_event_capture']['captured']} to {lifecycle['price_plus_age_event_capture']['captured']}.
- All three remain rejected/annotation-only. Frozen ranking, entry, exit, stage vetoes, and `watchlist_only` deployment are unchanged.
{END}"""
    model_path.write_text(replace_section(model_path.read_text(encoding="utf-8"), model_section), encoding="utf-8")

    artifact_path = REPORT / "artifact.json"
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    manifest = artifact["manifest"]
    snapshot = artifact["snapshot"]
    for key in ("sources", "charts", "tables", "blocks"):
        manifest[key] = [item for item in manifest.get(key, []) if not str(item.get("id", "")).startswith("frontier-")]
    for key in list(snapshot["datasets"]):
        if key.startswith("frontier_"):
            snapshot["datasets"].pop(key)
    manifest["sources"].append(
        {
            "id": "frontier-validation",
            "label": "Staged entry, hybrid stop, and exchange-lifecycle validation",
            "path": "staged_entry_decision.json",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "sql": "SELECT * FROM read_csv_auto('staged_entry_calibration.csv') ORDER BY selection_eligible DESC, selection_score DESC",
                "description": "Time-isolated falsification of probe/add allocation, close-confirmed stops, and one pre-registered listing-age ranking feature.",
                "tables_used": [
                    "staged_entry_calibration.csv", "hybrid_stop_calibration.csv", "hybrid_stop_path_summary.csv",
                    "lifecycle_factor_fold_metrics.csv", "lifecycle_factor_scored.csv.gz", "sequential_strategy_verification.json",
                ],
                "filters": ["calibration completed before fold 3", "confirmation folds 3-6", "next-open execution after completed 4h triggers", "same-row lifecycle model comparison"],
                "metric_definitions": ["Probe fraction is initial planned notional before launch confirmation.", "Hybrid close stops execute at the next 4h open; catastrophic, break-even and trailing stops remain intrabar.", "Listing age is days since the contract onboard timestamp at each historical signal date."],
            },
        }
    )
    artifact["sources"] = manifest["sources"]
    snapshot["datasets"]["frontier_staged"] = clean_records(staged_catalog)
    snapshot["datasets"]["frontier_hybrid_path"] = clean_records(hybrid_path)
    snapshot["datasets"]["frontier_lifecycle_folds"] = clean_records(lifecycle_folds)
    snapshot["datasets"]["frontier_lifecycle_segments"] = clean_records(lifecycle_segments)
    manifest["charts"].extend(
        [
            {
                "id": "frontier-staged-pf",
                "title": "Staged entry profit factor by probe fraction",
                "type": "line",
                "dataset": "frontier_staged",
                "encodings": {
                    "x": {"field": "probe_fraction", "type": "quantitative", "title": "Probe fraction"},
                    "y": {"field": "profit_factor", "type": "quantitative", "title": "Profit factor"},
                },
                "sourceId": "frontier-validation",
            },
            {
                "id": "frontier-lifecycle-ap",
                "title": "Price versus price-plus-age average precision by fold",
                "type": "line",
                "dataset": "frontier_lifecycle_folds",
                "encodings": {
                    "x": {"field": "fold", "type": "nominal", "title": "Walk-forward fold"},
                    "y": {"fields": ["price_average_precision", "combo_average_precision"], "type": "quantitative", "title": "Average precision"},
                },
                "sourceId": "frontier-validation",
            },
        ]
    )
    manifest["tables"].append(
        {
            "id": "frontier-hybrid-table",
            "title": "Hybrid initial-stop calibration",
            "dataset": "frontier_hybrid_path",
            "columns": [
                {"field": "period", "label": "Period", "type": "text"},
                {"field": "target_return", "label": "Target", "type": "percent"},
                {"field": "population", "label": "Population", "type": "text"},
                {"field": "signals", "label": "Signals", "type": "number"},
                {"field": "prior_intrabar_below_minus25_share", "label": "Prior low below -25%", "type": "percent"},
                {"field": "prior_close_below_minus20_share", "label": "Prior close below -20%", "type": "percent"},
            ],
            "defaultSort": {"field": "target_return", "direction": "asc"},
            "sourceId": "frontier-validation",
        }
    )
    manifest["blocks"].extend(
        [
            {
                "id": "frontier-summary",
                "type": "markdown",
                "sourceId": "frontier-validation",
                "body": "## Three additional hypotheses did not replace the frozen challenger\n\nCalibration retained zero early probe, the 25% hard stop, and the original price ranking. Listing age is directionally informative but redundant; early probes dilute returns; close-confirmed stops did not solve time instability.",
            },
            {"id": "frontier-staged-chart-block", "type": "chart", "chartId": "frontier-staged-pf"},
            {"id": "frontier-lifecycle-chart-block", "type": "chart", "chartId": "frontier-lifecycle-ap"},
            {"id": "frontier-hybrid-table-block", "type": "table", "tableId": "frontier-hybrid-table"},
        ]
    )
    generated = lifecycle["generated_at_utc"]
    manifest["generatedAt"] = generated
    snapshot["generatedAt"] = generated
    artifact_path.write_text(json.dumps(artifact, ensure_ascii=True, indent=2), encoding="utf-8")

    chart_map_path = REPORT / "CHART_MAP.md"
    chart_section = f"""{BEGIN}
## Strategy-frontier outputs

- `frontier-staged-pf`: confirmation diagnostic PF across pre-confirmation probe fractions; selection remains calibration-only.
- `frontier-lifecycle-ap`: price-only versus price-plus-listing-age AP by fold.
- `frontier-hybrid-table`: winner adverse-excursion diagnostics before later targets.
- `charts/strategy_frontier_falsifications.png`: combined report figure for the three rejected hypotheses.
{END}"""
    chart_map_path.write_text(replace_section(chart_map_path.read_text(encoding="utf-8"), chart_section), encoding="utf-8")
    print(json.dumps({"updated": True, "probe": staged["selected_probe_fraction"], "stop": hybrid["selected_policy"], "lifecycle": lifecycle["classification"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
