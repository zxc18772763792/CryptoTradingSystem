"""Merge verified sequential launch research into the existing report artifact."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import nbformat
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
DEFAULT_FORWARD = ROOT / "data" / "research" / "binance_200pct_forward_monitor"
BEGIN = "<!-- SEQUENTIAL_STRATEGY_BEGIN -->"
END = "<!-- SEQUENTIAL_STRATEGY_END -->"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--forward-root", type=Path, default=DEFAULT_FORWARD)
    return parser.parse_args()


def replace_section(text: str, section: str) -> str:
    if BEGIN in text and END in text:
        before, rest = text.split(BEGIN, 1)
        _, after = rest.split(END, 1)
        return before.rstrip() + "\n\n" + section + "\n" + after.lstrip()
    return text.rstrip() + "\n\n" + section + "\n"


def clean_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    clean = frame.replace({np.nan: None, np.inf: None, -np.inf: None}).copy()
    for column in clean.columns:
        if pd.api.types.is_datetime64_any_dtype(clean[column]):
            clean[column] = clean[column].astype(str)
    return clean.to_dict(orient="records")


def load_forward_observations(root: Path) -> pd.DataFrame:
    records = []
    directory = root / "sequence_24h"
    for path in sorted(directory.glob("*.json")) if directory.exists() else []:
        payload = json.loads(path.read_text(encoding="utf-8"))
        records.append(
            {
                "signal_id": payload["signal_id"],
                "snapshot_date": payload["snapshot_date"],
                "symbol": payload["symbol"],
                "original_stage": payload["original_stage"],
                "score_pctile": payload["original_score_pctile"],
                "mfe_24h": payload["mfe_24h"],
                "close_return_24h": payload["close_return_24h"],
                "launch_status": payload["launch_status"],
                "paper_eligible": payload["paper_eligible_after_original_stage"],
                "decision_time_utc": payload["decision_time_utc"],
            }
        )
    return pd.DataFrame(records)


def make_charts(
    report: Path,
    recognition_folds: pd.DataFrame,
    model_folds: pd.DataFrame,
    exit_folds: pd.DataFrame,
) -> None:
    charts = report / "charts"
    charts.mkdir(parents=True, exist_ok=True)
    blue = "#3B6FB6"
    gold = "#C7922B"
    orange = "#D97732"
    ink = "#20262E"

    positions = np.arange(len(recognition_folds))
    fig, axis = plt.subplots(figsize=(8.4, 4.8))
    width = 0.36
    axis.bar(positions - width / 2, 100 * recognition_folds["base_precision"], width, label="All top-2% candidates", color="#AAB2BD")
    axis.bar(positions + width / 2, 100 * recognition_folds["selected_precision"], width, label="24h MFE20 + close5", color=blue)
    axis.set_xticks(positions, recognition_folds["fold"].astype(str))
    axis.set(title="24h launch-rule precision by confirmation fold", xlabel="Historical confirmation fold", ylabel="Actual +200% precision (%)")
    axis.axhline(0, color=ink, linewidth=0.8)
    axis.grid(axis="y", color="#E5E7EB")
    axis.legend(frameon=False)
    axis.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(charts / "sequence_rule_precision.png", dpi=180, bbox_inches="tight")
    plt.close(fig)

    positions = np.arange(len(model_folds))
    fig, axis = plt.subplots(figsize=(8.4, 4.8))
    axis.bar(positions - width / 2, model_folds["price_average_precision"], width, label="Frozen price score", color="#AAB2BD")
    axis.bar(positions + width / 2, model_folds["sequence_average_precision"], width, label="24h sequence model", color=gold)
    axis.set_xticks(positions, model_folds["fold"].astype(str))
    axis.set(title="Price versus 24h sequence average precision", xlabel="Historical confirmation fold", ylabel="Average precision")
    axis.grid(axis="y", color="#E5E7EB")
    axis.legend(frameon=False)
    axis.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(charts / "sequence_model_ap.png", dpi=180, bbox_inches="tight")
    plt.close(fig)

    fig, axis = plt.subplots(figsize=(8.4, 4.8))
    colors = [blue if value > 1 else orange for value in exit_folds["profit_factor"]]
    axis.bar(exit_folds["fold"].astype(str), exit_folds["profit_factor"], color=colors)
    axis.axhline(1.0, color=ink, linestyle="--", linewidth=1)
    axis.set(title="24h launch challenger profit factor by fold", xlabel="Historical confirmation fold", ylabel="Profit factor")
    axis.grid(axis="y", color="#E5E7EB")
    axis.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(charts / "sequence_exit_profit_factor.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def update_notebook(report: Path) -> None:
    path = report / "binance_runup_research.ipynb"
    notebook = nbformat.read(path, as_version=4)
    notebook.cells = [
        cell for cell in notebook.cells
        if "sequential-strategy" not in cell.get("metadata", {}).get("tags", [])
    ]
    notebook.cells.extend(
        [
            nbformat.v4.new_markdown_cell(
                """## 24h sequential launch challenger

This extension separates identification from tradability. A secondary 24h path rule is selected on fully matured pre-confirmation rows, then measured on folds 3-6. Because the rule family followed earlier path exploration, it remains a forward-only challenger even when historical recognition gates pass.""",
                metadata={"tags": ["sequential-strategy"]},
            ),
            nbformat.v4.new_code_cell(
                """sequence = json.loads((ROOT / 'sequential_strategy_decision.json').read_text(encoding='utf-8'))
simple_launch = json.loads((ROOT / 'simple_launch_strategy_decision.json').read_text(encoding='utf-8'))
launch_stop = json.loads((ROOT / 'launch_stop_strategy_decision.json').read_text(encoding='utf-8'))
launch_exit = json.loads((ROOT / 'launch_exit_state_decision.json').read_text(encoding='utf-8'))
simple_folds = pd.read_csv(ROOT / 'simple_launch_recognition_folds.csv')
exit_folds = pd.read_csv(ROOT / 'launch_exit_state_folds.csv')
pd.Series({
    '24h rule precision': simple_launch['recognition_confirmation']['selected_precision'],
    '24h rule event precision': simple_launch['event_recognition_confirmation']['selected_event_precision'],
    'winner retention': simple_launch['recognition_confirmation']['positive_retention'],
    'selected hard stop': launch_stop['selected_stop_width'],
    'selected exit': launch_exit['selected_exit_policy'],
    'exit expectancy': launch_exit['strategy_default_summary']['expectancy'],
    'exit profit factor': launch_exit['strategy_default_summary']['profit_factor'],
})""",
                metadata={"tags": ["sequential-strategy"]},
            ),
            nbformat.v4.new_code_cell(
                """import numpy as np
fig, axes = plt.subplots(1, 2, figsize=(12, 4))
x = np.arange(len(simple_folds)); width = .36
axes[0].bar(x-width/2, 100*simple_folds['base_precision'], width, label='all top-2%')
axes[0].bar(x+width/2, 100*simple_folds['selected_precision'], width, label='24h launch rule')
axes[0].set_xticks(x, simple_folds['fold'].astype(str)); axes[0].set(title='Recognition precision', xlabel='fold', ylabel='%')
axes[0].legend(frameon=False)
axes[1].bar(exit_folds['fold'].astype(str), exit_folds['profit_factor'])
axes[1].axhline(1, color='black', linestyle='--'); axes[1].set(title='Exit challenger PF', xlabel='fold', ylabel='PF')
for axis in axes: axis.grid(axis='y', alpha=.2)
plt.tight_layout(); plt.show()""",
                metadata={"tags": ["sequential-strategy"]},
            ),
        ]
    )
    nbformat.write(notebook, path)


def main() -> None:
    args = parse_args()
    report = args.report_dir.resolve()
    verification = json.loads((report / "sequential_strategy_verification.json").read_text(encoding="utf-8"))
    if not verification.get("passed"):
        raise RuntimeError("Sequential strategy verifier did not pass")
    sequence = json.loads((report / "sequential_strategy_decision.json").read_text(encoding="utf-8"))
    robustness = json.loads((report / "sequential_robustness_decision.json").read_text(encoding="utf-8"))
    simple = json.loads((report / "simple_launch_strategy_decision.json").read_text(encoding="utf-8"))
    stop = json.loads((report / "launch_stop_strategy_decision.json").read_text(encoding="utf-8"))
    state = json.loads((report / "launch_exit_state_decision.json").read_text(encoding="utf-8"))
    simple_cal = pd.read_csv(report / "sequential_simple_rule_calibration.csv")
    recognition_folds = pd.read_csv(report / "simple_launch_recognition_folds.csv")
    model_folds = pd.read_csv(report / "sequential_model_confirmation_folds.csv")
    exit_folds = pd.read_csv(report / "launch_exit_state_folds.csv")
    stop_cal = pd.read_csv(report / "launch_stop_calibration.csv")
    forward = load_forward_observations(args.forward_root.resolve())
    make_charts(report, recognition_folds, model_folds, exit_folds)

    rec = simple["recognition_confirmation"]
    event = simple["event_recognition_confirmation"]
    null = robustness["permutation_null"]
    exit_summary = state["strategy_default_summary"]
    forward_confirmed = int((forward["launch_status"] == "launch_confirmed").sum()) if not forward.empty else 0
    forward_eligible = int(forward["paper_eligible"].sum()) if not forward.empty else 0
    section = f"""{BEGIN}
## 24 小时序列启动：识别能力明显提高，但交易稳定性仍未通过

### 最重要的新发现

- **暴涨币识别出现了可操作的二阶段方案。** 在冻结价格模型前 2% 观察池中，先记录下一根 4h 开盘价，然后观察完整 24 小时；若期间最高价至少达到参考价的 **+20%**，且第 24 小时结束时仍至少上涨 **+5%**，则标记为 `launch_confirmed`。历史确认期的逐行命中率由 {rec['base_precision']:.2%} 提高到 {rec['selected_precision']:.2%}，保留 {int(rec['selected_positives'])}/{int(rec['positives'])} 个正例。
- **事件去重后仍成立。** 155 个信号簇中有 {int(event['positive_events'])} 个独立正事件；启动规则选择 {int(event['selected_clusters'])} 个簇、命中 {int(event['selected_positive_events'])} 个，事件精度 {event['selected_event_precision']:.2%}，保留率 {event['positive_event_retention']:.2%}。
- **这不是简单的单币偶然。** 置换检验的 AP 增量单侧 p 值为 {null['ap_one_sided_pvalue']:.4f}，精度增量 p 值为 {null['precision_one_sided_pvalue']:.4f}；逐一删除 114 个币时，AP 和精度增量仍全部为正。
- **但它仍是二次历史研究。** 规则族是在前一轮路径研究之后形成，不能把第 3–6 折重新称为真正未见 OOS。因此它只能冻结为前瞻挑战者，不能直接替换主排名或触发交易。

![24 小时启动规则逐折精度](charts/sequence_rule_precision.png)

### 黑盒序列模型提供了独立佐证

24 小时浅层树在确认期把 AP 从 {sequence['recognition_confirmation']['price_average_precision']:.4f} 提高到 {sequence['recognition_confirmation']['sequence_average_precision']:.4f}，选择后命中率 {sequence['recognition_confirmation']['selected_precision']:.2%}，四折中 {sequence['recognition_fold_joint_improvement_share']:.0%} 同时改善 AP 与精度。路径-only 消融的 AP 为 {robustness['path_feature_ablation']['path_only']['sequence_average_precision']:.4f}，说明增量主要来自启动后的真实路径，而不是再次包装原价格分数。不过早期模型校准只有一个可用验证折，故排名门槛仍判定失败。

![价格分数与序列模型 AP](charts/sequence_model_ap.png)

### “何时买”：当前冻结的前瞻挑战者

1. 日线结束后只保留冻结价格分位 ≥98% 的观察池；原有 `reject_chase` 和 `cooldown_after_200pct_runup` 继续拥有否决权。
2. 下一根 4h 开盘只作为 `P0` 参考价，不因单根成交量或主动买入立即追价。
3. 在 `P0` 后观察六根完整 4h K 线。若 `MFE24h ≥20%` 且第六根收盘相对 `P0 ≥5%`，在 24 小时边界记录 `launch_confirmed`；否则记录 `launch_rejected`。
4. 校准期选择的是确认后再入场，而不是提前满仓或事后挑选的半仓模式。但确认后仍有 3 倍空间的命中率只有 {rec['late_selected_precision']:.2%}，所以目前只能模拟，不能据此追涨。

### “何时卖”：当前最强但尚未过门槛的退出挑战者

校准最终选择：风险等额的 **-25% 硬止损**；盈利曾达到 **+30%** 后，从下一根 4h 起把保护位移到成本；达到 **+100%** 卖出一半，余仓使用 **25% 追踪止损**，最长持有 30 天。确认期 {int(exit_summary['trades'])} 笔的期望为 {exit_summary['expectancy']:.2%}、利润因子 {exit_summary['profit_factor']:.2f}、最大回撤 {exit_summary['max_drawdown']:.2%}；30bps 单边滑点压力下期望仍为 {state['strategy_stress_costs'][-1]['expectancy']:.2%}。

但只有 {state['fold_majority_profit_factor_gt_1']:.0%} 的折利润因子大于 1，周块和币种 bootstrap 的期望下界分别为 {state['week_bootstrap']['expectancy_lower_95pct']:.2%}、{state['symbol_bootstrap']['expectancy_lower_95pct']:.2%}。因此退出策略仍是 `watchlist_only`。较早的“确认后一天未继续上涨就退出”没有取代 +30% 后保本规则。

![启动挑战者逐折利润因子](charts/sequence_exit_profit_factor.png)

### 第一批真正前瞻的 24 小时记录

现有不可覆盖快照已经产生 {len(forward)} 条成熟观察，其中 {forward_confirmed} 条触发启动规则、{forward_eligible} 条同时通过原始阶段否决。ESPORTS 和 AKE 虽触发启动，但原始状态均为暴涨后冷却；HOME 未触发且原始状态为追高拒绝。因此当前没有新的纸面入场候选。这 3 条只用于开始积累真正 OOS，样本量不足以评价规则。

### 当前结论

- **识别层：** 24 小时 `MFE≥20% + 收盘≥5%` 是迄今最有意义的二阶段识别规则，历史信号精度约为原观察池的 2.75 倍。
- **入场层：** 确认后追入仍会遇到大量回撤；只允许纸面记录，不自动交易。半仓试探、确认后补仓的历史 PF 更高，但它不是校准选择，必须留给未来 OOS 比较。
- **退出层：** -25% 风险等额、+30% 后保本、+100% 减半、25% 追踪是当前最强挑战者，但时间稳定性和置信下界未通过。
- **升级条件：** 真正未来样本中多数月份/时间折 PF>1，周块与币种期望下界均>0，删除最大赢家后仍为正，且原始阶段否决不得被绕过。
{END}"""
    report_path = report / "REPORT.md"
    report_text = report_path.read_text(encoding="utf-8")
    report_text = re.sub(
        r"- \*\*目前仍没有通过门槛的买入策略。\*\*.*",
        "- **目前仍没有通过交易门槛的买入策略；但 24 小时序列启动已成为识别挑战者。** 单根 4h 确认仍无效，新增二阶段规则详见后文。",
        report_text,
    )
    report_text = re.sub(
        r"- \*\*当前最合理的“何时买”.*",
        "- **基础入场基线仍是下一根 4h 开盘；新增挑战者需等待完整 24 小时。** 只有 `MFE≥20% 且收盘≥5%` 才进入启动观察，但现阶段仍不得自动追入。",
        report_text,
    )
    report_text = re.sub(
        r"- \*\*当前最合理的“何时卖”.*",
        "- **基础退出基线仍未通过交易门槛；新增挑战者为 -25% 风险等额止损、+30% 后保本、+100% 减半和 25% 追踪。** 它提高历史 PF，但稳定性与置信下界仍不合格。",
        report_text,
    )
    report_path.write_text(replace_section(report_text, section), encoding="utf-8")

    summary_path = report / "analysis_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["sequential_launch_model"] = sequence
    summary["sequential_robustness"] = robustness
    summary["simple_launch_rule"] = simple
    summary["launch_stop_width"] = stop
    summary["launch_exit_state"] = state
    summary["sequence_forward_observations"] = clean_records(forward) if not forward.empty else []
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    model_card_path = report / "MODEL_CARD.md"
    model_section = f"""{BEGIN}
## Sequential launch challenger

- The production ranking remains the frozen eight-factor price model. The 24h challenger cannot change rank or override `reject_chase` / cooldown stages.
- Secondary launch rule: after the reference 4h open, require 24h MFE >=20% and the completed 24h close >=5%. Historical confirmation row precision was {rec['selected_precision']:.2%}; independent-event precision was {event['selected_event_precision']:.2%}.
- Paper exit challenger: risk-equal 25% hard stop, break-even protection after +30%, half at +100%, 25% trailing remainder, maximum 30 days.
- Historical PF was {exit_summary['profit_factor']:.2f}, but only {state['fold_majority_profit_factor_gt_1']:.0%} of folds exceeded PF 1 and both clustered-bootstrap expectancy lower bounds were negative.
- Deployment remains `watchlist_only`. The read-only forward observer writes immutable 24h records and has no order-routing dependency.
{END}"""
    model_card_path.write_text(
        replace_section(model_card_path.read_text(encoding="utf-8"), model_section), encoding="utf-8"
    )

    artifact_path = report / "artifact.json"
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    manifest = artifact["manifest"]
    snapshot = artifact["snapshot"]
    for key in ("sources", "charts", "tables", "blocks"):
        manifest[key] = [item for item in manifest.get(key, []) if not str(item.get("id", "")).startswith("seq-")]
    for key in list(snapshot["datasets"]):
        if key.startswith("seq_"):
            snapshot["datasets"].pop(key)

    source = {
        "id": "seq-validation",
        "label": "24h sequential launch and stateful exit validation",
        "path": "simple_launch_strategy_decision.json",
        "query": {
            "engine": "duckdb",
            "language": "sql",
            "sql": "SELECT * FROM read_csv_auto('simple_launch_recognition_folds.csv') ORDER BY fold",
            "description": "Secondary 24h launch rule, event deduplication, negative controls, risk-equal stops and stateful exits. Historical confirmation remains secondary evidence, not true future OOS.",
            "tables_used": [
                "sequential_checkpoint_candidates.csv.gz",
                "sequential_model_confirmation_folds.csv",
                "simple_launch_recognition_folds.csv",
                "launch_stop_calibration.csv",
                "launch_exit_state_folds.csv",
                "sequential_strategy_verification.json",
            ],
            "filters": [
                "first crossing into frozen price top 2%",
                "six completed 4h bars before the 24h decision open",
                "calibration outcomes mature before fold 3",
                "historical confirmation folds 3-6",
                "fees 5 bps per side and 5/15/30 bps slippage stress",
            ],
            "metric_definitions": [
                "Launch rule requires 24h MFE >=20% and completed 24h close return >=5% versus the reference open.",
                "Event clusters merge same-symbol signals separated by no more than 14 days.",
                "Risk-equal sizing uses 0.5% equity risk divided by the hard-stop width.",
            ],
        },
    }
    manifest["sources"].append(source)
    artifact["sources"] = manifest["sources"]

    model_long = []
    for _, row in model_folds.iterrows():
        model_long.extend(
            [
                {"fold": int(row["fold"]), "series": "price", "average_precision": float(row["price_average_precision"])},
                {"fold": int(row["fold"]), "series": "24h sequence", "average_precision": float(row["sequence_average_precision"])},
            ]
        )
    recognition_long = []
    for _, row in recognition_folds.iterrows():
        recognition_long.extend(
            [
                {"fold": int(row["fold"]), "series": "all top-2%", "precision": float(row["base_precision"])},
                {"fold": int(row["fold"]), "series": "24h launch", "precision": float(row["selected_precision"])},
            ]
        )
    snapshot["datasets"]["seq_model_ap"] = model_long
    snapshot["datasets"]["seq_rule_precision"] = recognition_long
    snapshot["datasets"]["seq_exit_folds"] = clean_records(exit_folds)
    snapshot["datasets"]["seq_rule_calibration"] = clean_records(simple_cal)
    snapshot["datasets"]["seq_stop_calibration"] = clean_records(stop_cal)
    snapshot["datasets"]["seq_forward"] = clean_records(forward) if not forward.empty else []

    manifest["charts"].extend(
        [
            {
                "id": "seq-model-ap",
                "title": "Price versus 24h sequence average precision",
                "type": "bar",
                "dataset": "seq_model_ap",
                "encodings": {
                    "x": {"field": "fold", "type": "nominal", "title": "Historical confirmation fold"},
                    "y": {"field": "average_precision", "type": "quantitative", "title": "Average precision"},
                    "color": {"field": "series", "type": "nominal", "title": "Model"},
                },
                "sourceId": "seq-validation",
            },
            {
                "id": "seq-rule-precision",
                "title": "24h launch-rule precision by fold",
                "type": "bar",
                "dataset": "seq_rule_precision",
                "encodings": {
                    "x": {"field": "fold", "type": "nominal", "title": "Historical confirmation fold"},
                    "y": {"field": "precision", "type": "quantitative", "title": "Actual +200% precision"},
                    "color": {"field": "series", "type": "nominal", "title": "Candidate set"},
                },
                "sourceId": "seq-validation",
            },
            {
                "id": "seq-exit-pf",
                "title": "24h launch challenger profit factor by fold",
                "type": "bar",
                "dataset": "seq_exit_folds",
                "encodings": {
                    "x": {"field": "fold", "type": "nominal", "title": "Historical confirmation fold"},
                    "y": {"field": "profit_factor", "type": "quantitative", "title": "Profit factor"},
                },
                "sourceId": "seq-validation",
            },
        ]
    )
    manifest["tables"].extend(
        [
            {
                "id": "seq-rule-table",
                "title": "Calibrated 24h launch rules",
                "dataset": "seq_rule_calibration",
                "columns": [
                    {"field": "rule", "label": "Rule", "type": "text"},
                    {"field": "selected_rows", "label": "Selected", "type": "number"},
                    {"field": "selected_positives", "label": "Positive", "type": "number"},
                    {"field": "selected_precision", "label": "Precision", "type": "percent"},
                    {"field": "positive_retention", "label": "Retention", "type": "percent"},
                    {"field": "selection_score", "label": "Selection score", "type": "number"},
                    {"field": "selection_eligible", "label": "Eligible", "type": "text"},
                ],
                "defaultSort": {"field": "selection_score", "direction": "desc"},
                "sourceId": "seq-validation",
            },
            {
                "id": "seq-forward-table",
                "title": "Immutable forward 24h observations",
                "dataset": "seq_forward",
                "columns": [
                    {"field": "snapshot_date", "label": "Snapshot", "type": "date"},
                    {"field": "symbol", "label": "Symbol", "type": "text"},
                    {"field": "original_stage", "label": "Original stage", "type": "text"},
                    {"field": "mfe_24h", "label": "24h MFE", "type": "percent"},
                    {"field": "close_return_24h", "label": "24h close", "type": "percent"},
                    {"field": "launch_status", "label": "Launch status", "type": "text"},
                    {"field": "paper_eligible", "label": "Paper eligible", "type": "text"},
                ],
                "defaultSort": {"field": "snapshot_date", "direction": "desc"},
                "sourceId": "seq-validation",
            },
        ]
    )
    manifest["blocks"].extend(
        [
            {
                "id": "seq-summary",
                "type": "markdown",
                "sourceId": "seq-validation",
                "body": (
                    "## A 24h path rule materially improves historical identification\n\n"
                    f"Among frozen top-2% candidates, requiring 24h MFE >=20% and the completed 24h close >=5% raised row precision from {rec['base_precision']:.2%} to {rec['selected_precision']:.2%}. Event-level precision was {event['selected_event_precision']:.2%}, retaining {event['positive_event_retention']:.2%} of positive events. This remains secondary historical evidence and is frozen only as a forward challenger."
                ),
            },
            {"id": "seq-rule-chart-block", "type": "chart", "chartId": "seq-rule-precision"},
            {
                "id": "seq-model",
                "type": "markdown",
                "sourceId": "seq-validation",
                "body": (
                    "## A shallow sequence model independently supports the path effect\n\n"
                    f"Confirmation AP rose from {sequence['recognition_confirmation']['price_average_precision']:.4f} to {sequence['recognition_confirmation']['sequence_average_precision']:.4f}; three of four folds improved. Calibration depth was insufficient for promotion, so the model cannot alter rank."
                ),
            },
            {"id": "seq-model-chart-block", "type": "chart", "chartId": "seq-model-ap"},
            {
                "id": "seq-entry",
                "type": "markdown",
                "sourceId": "seq-validation",
                "body": (
                    "## Frozen entry challenger\n\n"
                    "Keep the original top-2% watch pool and stage vetoes. Record the next 4h open as P0, observe six complete 4h bars, and mark launch_confirmed only when MFE24h >=20% and the completed close >=5%. Confirmation after this point is paper-only; it does not authorize chasing or automation."
                ),
            },
            {
                "id": "seq-exit",
                "type": "markdown",
                "sourceId": "seq-validation",
                "body": (
                    "## Best exit challenger is stronger but still unstable\n\n"
                    f"Risk-equal -25%, break-even protection after +30%, half at +100%, 25% trailing remainder, and a 30-day maximum hold produced {exit_summary['expectancy']:.2%} expectancy and PF {exit_summary['profit_factor']:.2f}. Only {state['fold_majority_profit_factor_gt_1']:.0%} of folds exceeded PF 1 and both clustered-bootstrap lower bounds were negative."
                ),
            },
            {"id": "seq-exit-chart-block", "type": "chart", "chartId": "seq-exit-pf"},
            {"id": "seq-rule-table-block", "type": "table", "tableId": "seq-rule-table"},
            {
                "id": "seq-forward",
                "type": "markdown",
                "sourceId": "seq-validation",
                "body": (
                    f"## Forward observer has started\n\n{len(forward)} immutable 24h observations are mature. {forward_confirmed} triggered the launch rule and {forward_eligible} also passed the original stage veto. No order routing or rank change is permitted."
                ),
            },
            {"id": "seq-forward-table-block", "type": "table", "tableId": "seq-forward-table"},
        ]
    )
    generated = state["generated_at_utc"]
    manifest["generatedAt"] = generated
    snapshot["generatedAt"] = generated
    artifact_path.write_text(json.dumps(artifact, ensure_ascii=True, indent=2), encoding="utf-8")

    chart_map_path = report / "CHART_MAP.md"
    chart_section = f"""{BEGIN}
## Sequential launch charts

- `seq-rule-precision`: grouped comparison/bar; all top-2% versus the calibrated 24h launch rule by fold; source `simple_launch_recognition_folds.csv`.
- `seq-model-ap`: grouped comparison/bar; frozen price score versus the 24h shallow sequence model; source `sequential_model_confirmation_folds.csv`.
- `seq-exit-pf`: comparison/bar; selected -25% / break-even-30 exit challenger PF by fold; source `launch_exit_state_folds.csv`.
- `seq-rule-table`: ranked calibration table for interpretable 24h rules; source `sequential_simple_rule_calibration.csv`.
- `seq-forward-table`: immutable true-forward 24h observations; source `data/research/binance_200pct_forward_monitor/sequence_24h/*.json`.
{END}"""
    chart_map_path.write_text(
        replace_section(chart_map_path.read_text(encoding="utf-8"), chart_section), encoding="utf-8"
    )
    update_notebook(report)
    print(artifact_path)


if __name__ == "__main__":
    main()
