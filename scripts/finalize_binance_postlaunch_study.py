"""Merge the verified post-launch entry-location falsification into the report."""

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
BEGIN = "<!-- POSTLAUNCH_ENTRY_BEGIN -->"
END = "<!-- POSTLAUNCH_ENTRY_END -->"


def replace_section(text: str, section: str) -> str:
    if BEGIN in text and END in text:
        before, rest = text.split(BEGIN, 1)
        _, after = rest.split(END, 1)
        return before.rstrip() + "\n\n" + section + "\n" + after.lstrip()
    return text.rstrip() + "\n\n" + section + "\n"


def clean_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    clean = frame.replace({np.nan: None, np.inf: None, -np.inf: None}).copy()
    return clean.to_dict(orient="records")


def make_chart(post: dict[str, Any]) -> None:
    baseline = post["confirmation_recognition_baseline"]
    selected = post["confirmation_recognition_selected"]
    base_trade = post["baseline_strategy_summary"]
    selected_trade = post["strategy_default_summary"]
    labels = ["24h confirmation open", "5% dip + reclaim"]
    blue = "#3B6FB6"
    orange = "#D97732"
    fig, axes = plt.subplots(1, 2, figsize=(10.5, 4.5))
    axes[0].bar(labels, [100 * baseline["actual_200pct_precision"], 100 * selected["actual_200pct_precision"]], color=[blue, orange])
    axes[0].set(title="Actual +200% precision", ylabel="Precision (%)")
    axes[1].bar(labels, [base_trade["profit_factor"], selected_trade["profit_factor"]], color=[blue, orange])
    axes[1].axhline(1, color="#20262E", linestyle="--", linewidth=1)
    axes[1].set(title="Costed trade profit factor", ylabel="Profit factor")
    for axis in axes:
        axis.grid(axis="y", color="#E5E7EB")
        axis.spines[["top", "right"]].set_visible(False)
        axis.tick_params(axis="x", rotation=12)
    fig.suptitle("Post-launch waiting failed historical confirmation")
    fig.tight_layout()
    charts = REPORT / "charts"
    charts.mkdir(parents=True, exist_ok=True)
    fig.savefig(charts / "postlaunch_entry_comparison.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def update_notebook(post: dict[str, Any]) -> None:
    path = REPORT / "binance_runup_research.ipynb"
    notebook = nbformat.read(path, as_version=4)
    notebook.cells = [
        cell for cell in notebook.cells
        if "postlaunch-entry" not in cell.get("metadata", {}).get("tags", [])
    ]
    notebook.cells.extend(
        [
            nbformat.v4.new_markdown_cell(
                """## Post-launch entry-location falsification

Nine time-safe entry rules were calibrated before fold 3. Every delayed rule observes a completed 4h trigger and enters at the next open; its +200% label is recomputed from the actual entry. The calibration-selected 5% dip-and-reclaim rule failed folds 3-6, so the 24h confirmation open remains the paper observation baseline.""",
                metadata={"tags": ["postlaunch-entry"]},
            ),
            nbformat.v4.new_code_cell(
                """postlaunch = json.loads((ROOT / 'postlaunch_entry_decision.json').read_text(encoding='utf-8'))
postlaunch_cal = pd.read_csv(ROOT / 'postlaunch_entry_calibration.csv')
postlaunch_regimes = pd.read_csv(ROOT / 'postlaunch_regime_diagnostics.csv')
pd.DataFrame([
    {'entry': '24h confirmation open', **postlaunch['confirmation_recognition_baseline'], 'profit_factor': postlaunch['baseline_strategy_summary']['profit_factor']},
    {'entry': '5% dip + reclaim', **postlaunch['confirmation_recognition_selected'], 'profit_factor': postlaunch['strategy_default_summary']['profit_factor']},
])""",
                metadata={"tags": ["postlaunch-entry"]},
            ),
            nbformat.v4.new_code_cell(
                """display(postlaunch_cal[['entry_rule','confirmed_entries','actual_200pct_precision','positive_retention','expectancy','profit_factor','selection_eligible']])
display(postlaunch_regimes)""",
                metadata={"tags": ["postlaunch-entry"]},
            ),
        ]
    )
    nbformat.write(notebook, path)


def main() -> None:
    verification = json.loads((REPORT / "sequential_strategy_verification.json").read_text(encoding="utf-8"))
    if not verification.get("sections", {}).get("postlaunch_entry", {}).get("passed"):
        raise RuntimeError("Post-launch verifier did not pass")
    post = json.loads((REPORT / "postlaunch_entry_decision.json").read_text(encoding="utf-8"))
    calibration = pd.read_csv(REPORT / "postlaunch_entry_calibration.csv")
    regimes = pd.read_csv(REPORT / "postlaunch_regime_diagnostics.csv")
    baseline = post["confirmation_recognition_baseline"]
    selected = post["confirmation_recognition_selected"]
    base_trade = post["baseline_strategy_summary"]
    selected_trade = post["strategy_default_summary"]
    make_chart(post)
    update_notebook(post)

    section = f"""{BEGIN}
## 启动确认后的买点研究：等待回踩没有提高胜算

在 24 小时启动规则之后，研究又按时间隔离比较了 9 个可执行买点：确认边界立即记录、再等 4 小时、回踩 5%/10%、回踩后收复、受控回踩、六根 K 线突破和突破启动前高点。所有非即时规则都只读取已经收完的 4h K 线，并在下一根开盘成交；未来 +200% 标签按实际成交价重新计算。

校准期选中了 `discount5_reclaim`（相对启动确认价回踩至少 5%，随后阳线收复，再下一根 4h 开盘）。但它在确认期失效：候选从 {baseline['entries']} 个降至 {selected['entries']} 个，实际 +200% 精度从 **{baseline['actual_200pct_precision']:.2%} 降至 {selected['actual_200pct_precision']:.2%}**，只保留 **{selected['actual_200pct_entries']}/{baseline['actual_200pct_entries']}** 个仍有三倍空间的路径。中位买价虽然低了 {-selected['median_entry_return_vs_launch']:.2%}，但代价是漏掉多数真正赢家。

交易结果同样恶化：冻结相同的 -25% / +30% 后保本 / +100% 减半 / 25% 追踪退出后，利润因子从 **{base_trade['profit_factor']:.2f}** 降至 **{selected_trade['profit_factor']:.2f}**，期望从 **{base_trade['expectancy']:.2%}** 降至 **{selected_trade['expectancy']:.2%}**；删掉最大赢家后期望为 **{post['leave_largest_winner_out_expectancy']:.2%}**。因此回踩收复规则被否决，不能取代启动确认后的下一根 4h 开盘基线。

![启动后入场对比](charts/postlaunch_entry_comparison.png)

市场状态也不能补救这个问题：BTC 趋势和山寨宽度在校准期与确认期的方向发生反转，所以只作为失效诊断，不允许成为事后过滤器。当前可执行结论是：**先用 24 小时启动路径提高识别浓度；若做纸面跟踪，就在启动确认后的下一根 4h 开盘建立统一参考价，不等待回踩，也不自动交易。**
{END}"""
    report_path = REPORT / "REPORT.md"
    report_path.write_text(replace_section(report_path.read_text(encoding="utf-8"), section), encoding="utf-8")

    summary_path = REPORT / "analysis_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["postlaunch_entry_location"] = post
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    model_path = REPORT / "MODEL_CARD.md"
    model_section = f"""{BEGIN}
## Post-launch entry-location falsification

- Nine time-safe post-launch entry rules were calibrated before fold 3; actual labels were recomputed from each entry price.
- Calibration selected a 5% dip-and-reclaim entry, but confirmation precision fell from {baseline['actual_200pct_precision']:.2%} to {selected['actual_200pct_precision']:.2%} and positive-path retention was only {selected['positive_retention_vs_launch_open']:.2%}.
- With the same frozen exit, PF fell from {base_trade['profit_factor']:.2f} to {selected_trade['profit_factor']:.2f}; leave-largest-winner expectancy was negative.
- The delayed entry is rejected. The 24h confirmation open remains a paper reference only; deployment stays `watchlist_only` with no automatic trading.
{END}"""
    model_path.write_text(replace_section(model_path.read_text(encoding="utf-8"), model_section), encoding="utf-8")

    artifact_path = REPORT / "artifact.json"
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    manifest = artifact["manifest"]
    snapshot = artifact["snapshot"]
    for key in ("sources", "charts", "tables", "blocks"):
        manifest[key] = [item for item in manifest.get(key, []) if not str(item.get("id", "")).startswith("postlaunch-")]
    for key in list(snapshot["datasets"]):
        if key.startswith("postlaunch_"):
            snapshot["datasets"].pop(key)
    manifest["sources"].append(
        {
            "id": "postlaunch-validation",
            "label": "Post-launch entry-location validation",
            "path": "postlaunch_entry_decision.json",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "sql": "SELECT * FROM read_csv_auto('postlaunch_entry_calibration.csv') ORDER BY selection_eligible DESC, selection_score DESC",
                "description": "Time-isolated comparison of nine post-launch entry locations with labels recomputed from actual next-open entries.",
                "tables_used": [
                    "postlaunch_entry_candidates.csv.gz",
                    "postlaunch_entry_calibration.csv",
                    "postlaunch_entry_confirmation_summary.csv",
                    "postlaunch_regime_diagnostics.csv",
                    "sequential_strategy_verification.json",
                ],
                "filters": [
                    "24h MFE >=20% and completed close >=5%",
                    "trigger bars must be complete and entry occurs at the next 4h open",
                    "calibration outcomes mature before fold 3",
                    "confirmation folds 3-6",
                ],
                "metric_definitions": [
                    "Actual +200% precision is recomputed from the post-launch entry open.",
                    "Positive retention compares actual +200% paths with the immediate 24h confirmation-open baseline.",
                ],
            },
        }
    )
    artifact["sources"] = manifest["sources"]
    snapshot["datasets"]["postlaunch_recognition"] = [
        {"entry": "24h confirmation open", "precision": baseline["actual_200pct_precision"], "positive_paths": baseline["actual_200pct_entries"]},
        {"entry": "5% dip + reclaim", "precision": selected["actual_200pct_precision"], "positive_paths": selected["actual_200pct_entries"]},
    ]
    snapshot["datasets"]["postlaunch_calibration"] = clean_records(calibration)
    snapshot["datasets"]["postlaunch_regimes"] = clean_records(regimes)
    manifest["charts"].append(
        {
            "id": "postlaunch-recognition",
            "title": "Post-launch waiting reduced actual +200% precision",
            "type": "bar",
            "dataset": "postlaunch_recognition",
            "encodings": {
                "x": {"field": "entry", "type": "nominal", "title": "Entry rule"},
                "y": {"field": "precision", "type": "quantitative", "title": "Actual +200% precision"},
                "color": {"field": "entry", "type": "nominal", "title": "Entry rule"},
            },
            "sourceId": "postlaunch-validation",
        }
    )
    manifest["tables"].append(
        {
            "id": "postlaunch-calibration-table",
            "title": "Post-launch entry-rule calibration",
            "dataset": "postlaunch_calibration",
            "columns": [
                {"field": "entry_rule", "label": "Entry rule", "type": "text"},
                {"field": "confirmed_entries", "label": "Entries", "type": "number"},
                {"field": "actual_200pct_precision", "label": "+200% precision", "type": "percent"},
                {"field": "positive_retention", "label": "Retention", "type": "percent"},
                {"field": "expectancy", "label": "Expectancy", "type": "percent"},
                {"field": "profit_factor", "label": "Profit factor", "type": "number"},
                {"field": "selection_eligible", "label": "Eligible", "type": "text"},
            ],
            "defaultSort": {"field": "actual_200pct_precision", "direction": "desc"},
            "sourceId": "postlaunch-validation",
        }
    )
    manifest["blocks"].extend(
        [
            {
                "id": "postlaunch-summary",
                "type": "markdown",
                "sourceId": "postlaunch-validation",
                "body": (
                    "## Waiting for a dip did not improve the buy point\n\n"
                    f"The calibration-selected 5% dip-and-reclaim rule cut actual +200% precision from {baseline['actual_200pct_precision']:.2%} to {selected['actual_200pct_precision']:.2%} in confirmation and retained only {selected['positive_retention_vs_launch_open']:.2%} of positive paths. It is rejected; the 24h confirmation open stays a paper-only reference."
                ),
            },
            {"id": "postlaunch-chart-block", "type": "chart", "chartId": "postlaunch-recognition"},
            {"id": "postlaunch-table-block", "type": "table", "tableId": "postlaunch-calibration-table"},
        ]
    )
    generated = post["generated_at_utc"]
    manifest["generatedAt"] = generated
    snapshot["generatedAt"] = generated
    artifact_path.write_text(json.dumps(artifact, ensure_ascii=True, indent=2), encoding="utf-8")

    chart_map_path = REPORT / "CHART_MAP.md"
    chart_section = f"""{BEGIN}
## Post-launch entry-location outputs

- `postlaunch-recognition`: baseline versus the calibration-selected delayed entry on actual +200% precision.
- `postlaunch-calibration-table`: all nine time-safe post-launch entry rules and their calibration metrics.
- `charts/postlaunch_entry_comparison.png`: report figure comparing recognition precision and costed profit factor.
{END}"""
    chart_map_path.write_text(replace_section(chart_map_path.read_text(encoding="utf-8"), chart_section), encoding="utf-8")

    print(json.dumps({"updated": True, "selected_rule": post["selected_entry_rule"], "classification": post["classification"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
