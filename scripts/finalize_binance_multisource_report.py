"""Attach verified multisource findings to the existing Binance research report."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
BEGIN = "<!-- MULTISOURCE_BEGIN -->"
END = "<!-- MULTISOURCE_END -->"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    return parser.parse_args()


def clean_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    clean = frame.replace({np.nan: None, np.inf: None, -np.inf: None}).copy()
    for column in clean.columns:
        if pd.api.types.is_datetime64_any_dtype(clean[column]):
            clean[column] = clean[column].astype(str)
    return clean.to_dict(orient="records")


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def replace_section(text: str, section: str) -> str:
    if BEGIN in text and END in text:
        before, rest = text.split(BEGIN, 1)
        _, after = rest.split(END, 1)
        return before.rstrip() + "\n\n" + section + "\n" + after.lstrip()
    return text.rstrip() + "\n\n" + section + "\n"


def plot_charts(report: Path, family_rows: pd.DataFrame, folds: pd.DataFrame) -> None:
    charts = report / "charts"
    charts.mkdir(parents=True, exist_ok=True)
    colors = ["#3B6FB6", "#C7922B", "#D97732"]
    fig, axes = plt.subplots(1, 2, figsize=(10.5, 4.2))
    families = list(family_rows["family"].unique())
    models = ["price_same_rows", "family_only", "price_plus_family"]
    x = np.arange(len(families))
    width = 0.23
    for index, model in enumerate(models):
        group = family_rows[family_rows["model"] == model].set_index("family").reindex(families)
        axes[0].bar(x + (index - 1) * width, group["average_precision"], width, label=model, color=colors[index])
        axes[1].bar(x + (index - 1) * width, group["top_2pct_lift"], width, label=model, color=colors[index])
    for axis, title, label in [
        (axes[0], "Average precision on identical rows", "Average precision"),
        (axes[1], "Top-2% lift on identical rows", "Lift"),
    ]:
        axis.set_xticks(x, families)
        axis.set_title(title)
        axis.set_ylabel(label)
        axis.grid(axis="y", alpha=0.2)
    axes[0].legend(frameon=False, fontsize=8)
    fig.suptitle("Multisource families versus the price baseline")
    fig.tight_layout()
    fig.savefig(charts / "multisource_increment.png", dpi=180, bbox_inches="tight")
    plt.close(fig)

    if not folds.empty:
        fig, axis = plt.subplots(figsize=(7.2, 4.2))
        bar_colors = ["#D97732" if value <= 1 else "#3B6FB6" for value in folds["profit_factor"]]
        axis.bar(folds["fold"].astype(str), folds["profit_factor"], color=bar_colors)
        axis.axhline(1.0, color="#20262E", linestyle="--", linewidth=1)
        axis.set_title("Catalyst entry: frozen-exit profit factor by fold")
        axis.set_xlabel("Historical fold")
        axis.set_ylabel("Profit factor")
        axis.grid(axis="y", alpha=0.2)
        fig.tight_layout()
        fig.savefig(charts / "multisource_catalyst_fold_pf.png", dpi=180, bbox_inches="tight")
        plt.close(fig)


def main() -> None:
    args = parse_args()
    report = args.report_dir.resolve()
    verification = json.loads((report / "multisource_verification_summary.json").read_text(encoding="utf-8"))
    if not verification.get("passed"):
        raise RuntimeError("Multisource verifier did not pass")
    summary = json.loads((report / "multisource_analysis_summary.json").read_text(encoding="utf-8"))
    strategy = json.loads((report / "multisource_catalyst_strategy_decision.json").read_text(encoding="utf-8"))
    rules = pd.read_csv(report / "multisource_news_catalyst_rules.csv")
    strategy_costs = pd.read_csv(report / "multisource_catalyst_strategy_summary.csv")
    strategy_folds = pd.read_csv(report / "multisource_catalyst_strategy_folds.csv")
    recent_oi = pd.read_csv(report / "multisource_recent_oi_sensitivity.csv")
    event_news = pd.read_csv(report / "multisource_current_event_news.csv")

    family_records: list[dict[str, Any]] = []
    for family in ("news", "onchain"):
        for model, metrics in summary[family]["pooled_metrics"].items():
            family_records.append({"family": family, "model": model, **metrics})
    family_rows = pd.DataFrame(family_records)
    plot_charts(report, family_rows, strategy_folds)

    news = summary["news"]
    onchain = summary["onchain"]
    news_price = news["pooled_metrics"]["price_same_rows"]
    news_combo = news["pooled_metrics"]["price_plus_family"]
    chain_price = onchain["pooled_metrics"]["price_same_rows"]
    chain_combo = onchain["pooled_metrics"]["price_plus_family"]
    catalyst = rules[rules["rule"] == "price_top10_catalyst7d"].iloc[0]
    default = strategy["default_summary"]
    week = strategy["week_block_bootstrap"]
    supply_ake = event_news[event_news["symbol"] == "AKEUSDT"]
    btw_signal = pd.read_csv(report / "multisource_catalyst_signals.csv")
    btw_signal = btw_signal[btw_signal["symbol"] == "BTWUSDT"]

    report_section = f"""{BEGIN}
## 多源策略挖掘：新闻、链上与交易所数据

### 结论先行

**还没有通过门槛的自动交易策略。** 新数据澄清了真正的问题：价格因子能提高暴涨币浓度，催化新闻还能进一步缩小观察名单，但“找到可能会涨的币”与“现在就能买”不是同一件事。把催化门槛接到冻结的 4 小时退出路径后，默认成本下 {int(default['trades'])} 笔交易的期望为 {default['expectancy']:.2%}、利润因子 {default['profit_factor']:.2f}，{default['hard_stop_exit_rate']:.0%} 触发 -20% 止损；周块 bootstrap 的期望下界为 {week['expectancy_lower_95pct']:.2%}。因此它仍是观察名单，不是纸面可交易候选。

![多源增量比较](charts/multisource_increment.png)

### 已验证

- 冻结价格模型仍是最可靠的提前筛选器。新闻连续模型在同样 {news_price['n']:,} 行上的 AP 从 {news_price['average_precision']:.4f} 降到 {news_combo['average_precision']:.4f}，top-2% lift 从 {news_price['top_2pct_lift']:.2f}x 降到 {news_combo['top_2pct_lift']:.2f}x；3 折中没有一折同时改善 AP 与 lift，增量 bootstrap 下界均小于零。
- “绝对 OI 大”不是正向因子。长窗 OI 已被严格验证否决；近 17 天全市场样本中，绝对 OI 高十分位命中率仅 {recent_oi.loc[recent_oi.factor == 'oi_usd', 'high_decile_rate'].iloc[0]:.2%}，低于 {recent_oi.loc[recent_oi.factor == 'oi_usd', 'base_rate'].iloc[0]:.2%} 的基准。
- 立即交易催化信号不可行。合并行级样本中，“价格前 10% + 7 日明确催化”命中率为 {catalyst['precision']:.2%}、lift {catalyst['lift']:.2f}x，但这些命中集中在 LAB、CLO、BTW 等少数独立事件；去重并执行组合约束后收益转负，只有 {strategy['fold_majority_profit_factor_gt_1']:.0%} 的折利润因子大于 1，删除最大赢家后的期望为 {strategy['leave_largest_winner_out_expectancy']:.2%}。

![催化策略逐折利润因子](charts/multisource_catalyst_fold_pf.png)

### 方向性证据

- 链上 TVL 只在 53 个币形成可用历史交集。价格+链上在 4,330 行、69 个正例中，AP 从 {chain_price['average_precision']:.4f} 升至 {chain_combo['average_precision']:.4f}，lift 从 {chain_price['top_2pct_lift']:.2f}x 升至 {chain_combo['top_2pct_lift']:.2f}x；但仅一半折同时改善，AP 增量 95% 下界为 {onchain['decision']['bootstrap_7d_blocks']['delta_average_precision']['lower_95pct']:.4f}，尚不能入排序。
- 近 17 天中，OI/市值高十分位命中率为 {recent_oi.loc[recent_oi.factor == 'oi_to_cmc_mcap', 'high_decile_rate'].iloc[0]:.2%}，OI 3 日增长高十分位为 {recent_oi.loc[recent_oi.factor == 'oi_change_3d', 'high_decile_rate'].iloc[0]:.2%}。这与“小市值 + 相对 OI/增量 OI”方向一致，但时间太短，只能当支持性标签。
- BTW 是催化型案例：6 月 4 日合约上线公告当天就触发研究信号，未来 14 日达到 +200%，冻结退出取得约 +109%。AKE 则是供给/交易所流向型案例，暴涨前出现的是庄家向 Binance 转币并伴随下跌的风险新闻。两类机制方向相反，不能用一个新闻总分混合。

### 怎样才可能成为可行策略

下一版不能再把“催化出现”直接当买点，而应拆成三阶段：

1. **脆弱性观察池**：继续使用冻结价格分位，不让 OI 或新闻抬高排名。
2. **事件分型**：把上市/集成/主网等需求催化，与解锁、交易所流入、庄家转币后的供给冲击分开；前者等买方确认，后者只观察恐慌出清后的反转，绝不混成一个线性分数。
3. **入场触发**：当前没有通过门槛的确认条件。统一记录下一根 4h 开盘作为观察基准价，但这不代表下单；主动买入、盘口深度、突破回踩和链上交易所净流入只进入前瞻快照作注释，真正 OOS 验证通过前不得提高排名或触发加仓。

升级门槛保持不变：同排同折增量置信区间下界大于零；冻结退出成本后多数折利润因子大于 1；最大回撤不超过 30%；周块和币种 bootstrap 期望下界均大于零；删除最大盈利币后仍为正。任何一条没过，就只做观察名单。

### 数据覆盖与限制

- 新闻：{news['coverage']['raw_rows']:,} 条原始记录，严格映射 {news['coverage']['strict_mapped_mentions']:,} 条、377 个币；档案始于 2026-02-22，只有 3 个可用后期折。
- 链上：524 个合约中 289 个 CoinGecko 唯一身份，83 个 DefiLlama 映射，最终 53 个币形成连续 TVL 历史；当前身份映射存在幸存者偏差。
- 交易所：丰富的资金费率、多空比、清算和主动买卖缓存是“被关注币”样本，不能作为全市场主回测；全市场官方 OI 历史窗口又只有约一个月。
- 当前 19 个事件中，暴涨参考点前 7 天只有 2 个出现严格新闻匹配，说明公开新闻不是多数暴涨的必要条件。
{END}"""
    report_path = report / "REPORT.md"
    report_path.write_text(
        replace_section(report_path.read_text(encoding="utf-8"), report_section), encoding="utf-8"
    )

    main_summary_path = report / "analysis_summary.json"
    main_summary = json.loads(main_summary_path.read_text(encoding="utf-8"))
    main_summary["multisource_validation"] = summary
    main_summary["multisource_catalyst_strategy"] = strategy
    main_summary_path.write_text(json.dumps(main_summary, ensure_ascii=False, indent=2), encoding="utf-8")

    model_card_path = report / "MODEL_CARD.md"
    model_section = f"""{BEGIN}
## Multisource extension

- News and on-chain families never modify the frozen price rank unless their same-row incremental gate passes. Neither passed.
- Strict news availability uses local `fetched_at`; publication timestamps cannot make a feature available earlier.
- The exploratory catalyst entry failed the paper-trading gate: expectancy {default['expectancy']:.2%}, PF {default['profit_factor']:.2f}, week-bootstrap lower bound {week['expectancy_lower_95pct']:.2%}.
- Deployment status remains `watchlist_only`; no order-routing module is imported.
{END}"""
    model_card_path.write_text(
        replace_section(model_card_path.read_text(encoding="utf-8"), model_section), encoding="utf-8"
    )

    artifact_path = report / "artifact.json"
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    manifest = artifact["manifest"]
    snapshot = artifact["snapshot"]
    for key in ("sources", "charts", "tables", "blocks"):
        manifest[key] = [item for item in manifest[key] if not str(item.get("id", "")).startswith("multisource-")]
    sources = [
        {
            "id": "multisource-news",
            "label": "Strict fetched-time news validation",
            "path": "multisource_news_fold_metrics.csv",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "sql": "SELECT * FROM read_csv_auto('multisource_news_fold_metrics.csv') ORDER BY fold",
                "description": "Raw local news remapped with strict token identifiers and fetched-time availability; family models compared on identical rows.",
                "tables_used": ["news.db.news_raw", "multisource_news_fold_metrics.csv"],
                "metric_definitions": ["Catalyst gate = price rank >= 90th percentile and a strict listing/integration/mainnet-type mention in the prior 7 days."],
            },
        },
        {
            "id": "multisource-onchain",
            "label": "Lagged DefiLlama TVL validation",
            "path": "multisource_onchain_fold_metrics.csv",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "sql": "SELECT * FROM read_csv_auto('multisource_onchain_fold_metrics.csv') ORDER BY fold",
                "description": "Unique CoinGecko identities matched to DefiLlama gecko_id; TVL features lagged one day.",
                "tables_used": ["multisource_onchain_mapping.csv", "multisource_onchain_fold_metrics.csv"],
                "metric_definitions": ["Missing protocol history is excluded, never filled as zero."],
            },
        },
        {
            "id": "multisource-strategy",
            "label": "Catalyst gate with frozen 4h exit",
            "path": "multisource_catalyst_strategy_summary.csv",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "sql": "SELECT * FROM read_csv_auto('multisource_catalyst_strategy_summary.csv') ORDER BY slippage_bps_each_side",
                "description": "First catalyst-gated entry, portfolio constraints, frozen exit, fee/funding/slippage and bootstrap stress.",
                "tables_used": ["multisource_catalyst_signals.csv", "futures_4h_panel.csv.gz", "multisource_catalyst_strategy_trades.csv.gz"],
                "metric_definitions": ["A failed profitability or uncertainty gate remains watchlist_only."],
            },
        },
    ]
    manifest["sources"].extend(sources)
    artifact["sources"] = manifest["sources"]
    snapshot["datasets"]["multisource_family_metrics"] = clean_records(family_rows)
    snapshot["datasets"]["multisource_catalyst_rules"] = clean_records(rules)
    snapshot["datasets"]["multisource_strategy_costs"] = clean_records(strategy_costs)
    snapshot["datasets"]["multisource_strategy_folds"] = clean_records(strategy_folds)
    snapshot["datasets"]["multisource_recent_oi"] = clean_records(recent_oi)
    snapshot["datasets"]["multisource_event_news"] = clean_records(event_news)
    manifest["charts"].extend(
        [
            {
                "id": "multisource-family-lift",
                "title": "Price, family-only and combined top-2% lift",
                "type": "bar",
                "dataset": "multisource_family_metrics",
                "encodings": {
                    "x": {"field": "family", "type": "nominal", "title": "Information family"},
                    "y": {"field": "top_2pct_lift", "type": "quantitative", "title": "Top-2% lift"},
                    "color": {"field": "model", "type": "nominal", "title": "Model"},
                },
                "options": {"grouping": "grouped"},
                "sourceId": "multisource-news",
            },
            {
                "id": "multisource-rule-lift",
                "title": "Interpretable catalyst-rule lift",
                "type": "bar",
                "dataset": "multisource_catalyst_rules",
                "encodings": {
                    "x": {"field": "rule", "type": "nominal", "title": "Rule"},
                    "y": {"field": "lift", "type": "quantitative", "title": "Lift"},
                },
                "sourceId": "multisource-news",
            },
            {
                "id": "multisource-strategy-fold-pf",
                "title": "Catalyst strategy profit factor by fold",
                "type": "bar",
                "dataset": "multisource_strategy_folds",
                "encodings": {
                    "x": {"field": "fold", "type": "nominal", "title": "Fold"},
                    "y": {"field": "profit_factor", "type": "quantitative", "title": "Profit factor"},
                },
                "sourceId": "multisource-strategy",
            },
        ]
    )
    manifest["tables"].extend(
        [
            {
                "id": "multisource-rule-table",
                "title": "Catalyst rule evidence",
                "dataset": "multisource_catalyst_rules",
                "columns": [
                    {"field": "rule", "label": "Rule", "type": "text"},
                    {"field": "selected_rows", "label": "Rows", "type": "number"},
                    {"field": "selected_positives", "label": "Positive", "type": "number"},
                    {"field": "precision", "label": "Precision", "type": "percent"},
                    {"field": "lift", "label": "Lift", "type": "number"},
                    {"field": "p_value_bh", "label": "BH p-value", "type": "number"},
                ],
                "defaultSort": {"field": "lift", "direction": "desc"},
                "sourceId": "multisource-news",
            },
            {
                "id": "multisource-event-news-table",
                "title": "Current run-up events and strict prior news",
                "dataset": "multisource_event_news",
                "columns": [
                    {"field": "symbol", "label": "Symbol", "type": "text"},
                    {"field": "classification", "label": "Class", "type": "text"},
                    {"field": "close_return", "label": "Run-up", "type": "percent"},
                    {"field": "news_mentions_7d", "label": "News 7d", "type": "number"},
                    {"field": "catalyst_mentions_14d", "label": "Catalyst 14d", "type": "number"},
                    {"field": "supply_flow_mentions_7d", "label": "Supply flow 7d", "type": "number"},
                ],
                "sourceId": "multisource-news",
            },
        ]
    )
    manifest["blocks"].extend(
        [
            {"id": "multisource-heading", "type": "markdown", "body": "## 多源策略挖掘\n\n新闻连续模型未增加排序价值；稀疏催化门槛提升浓度，但冻结退出后仍亏损。"},
            {"id": "multisource-family-chart-block", "type": "chart", "chartId": "multisource-family-lift"},
            {"id": "multisource-rules-chart-block", "type": "chart", "chartId": "multisource-rule-lift"},
            {"id": "multisource-rule-table-block", "type": "table", "tableId": "multisource-rule-table"},
            {"id": "multisource-strategy-text", "type": "markdown", "sourceId": "multisource-strategy", "body": f"### 信息价值不等于可交易时点\n\n催化规则默认成本期望 {default['expectancy']:.2%}、PF {default['profit_factor']:.2f}，75% 硬止损，分类保持 `watchlist_only`。"},
            {"id": "multisource-strategy-chart-block", "type": "chart", "chartId": "multisource-strategy-fold-pf"},
            {"id": "multisource-event-news-table-block", "type": "table", "tableId": "multisource-event-news-table"},
            {"id": "multisource-next", "type": "markdown", "body": "### 下一步\n\n把需求催化与供给冲击分型，等待主动买入、盘口恢复或突破回踩的独立确认；这些字段先进入前瞻快照，90 天冻结后再判断。"},
        ]
    )
    generated = pd.Timestamp.now(tz="UTC").isoformat()
    manifest["generatedAt"] = generated
    snapshot["generatedAt"] = generated
    artifact_path.write_text(json.dumps(artifact, ensure_ascii=False, indent=2), encoding="utf-8")

    notebook_path = report / "binance_runup_research.ipynb"
    notebook = json.loads(notebook_path.read_text(encoding="utf-8"))
    notebook["cells"] = [
        cell for cell in notebook["cells"]
        if "multisource-extension" not in cell.get("metadata", {}).get("tags", [])
    ]
    notebook["cells"].extend(
        [
            {
                "cell_type": "markdown",
                "metadata": {"tags": ["multisource-extension"]},
                "source": [
                    "## Multisource extension: news, on-chain and exchange evidence\n",
                    "This section reads only the independently verified output files. News uses fetched-time availability; TVL is lagged one day.\n",
                ],
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {"tags": ["multisource-extension"]},
                "outputs": [],
                "source": [
                    "from pathlib import Path\n",
                    "import json, pandas as pd\n",
                    "report = Path('.')\n",
                    "multisource = json.loads((report / 'multisource_analysis_summary.json').read_text(encoding='utf-8'))\n",
                    "news_folds = pd.read_csv(report / 'multisource_news_fold_metrics.csv')\n",
                    "onchain_folds = pd.read_csv(report / 'multisource_onchain_fold_metrics.csv')\n",
                    "catalyst_trades = pd.read_csv(report / 'multisource_catalyst_strategy_trades.csv.gz')\n",
                    "multisource['strict_strategy_decision'], news_folds, onchain_folds, catalyst_trades.head()\n",
                ],
            },
        ]
    )
    notebook_path.write_text(json.dumps(notebook, ensure_ascii=False, indent=1), encoding="utf-8")

    primary_manifest_path = report / "evidence_manifest.json"
    primary_manifest = json.loads(primary_manifest_path.read_text(encoding="utf-8"))
    for relative in list(primary_manifest.get("analysis_files", {})):
        path = report / relative
        if path.exists():
            primary_manifest["analysis_files"][relative] = {
                "bytes": path.stat().st_size,
                "sha256": file_hash(path),
            }
    additional_files = [
        "multisource_analysis_summary.json", "multisource_data_quality.json",
        "multisource_news_fold_metrics.csv", "multisource_news_catalyst_rules.csv",
        "multisource_news_decision.json", "multisource_onchain_fold_metrics.csv",
        "multisource_onchain_decision.json", "multisource_onchain_mapping.csv",
        "multisource_recent_oi_sensitivity.csv", "multisource_current_event_news.csv",
        "multisource_catalyst_signals.csv", "multisource_catalyst_strategy_summary.csv",
        "multisource_catalyst_strategy_folds.csv", "multisource_catalyst_strategy_decision.json",
        "multisource_verification_summary.json", "charts/multisource_increment.png",
        "charts/multisource_catalyst_fold_pf.png",
    ]
    for relative in additional_files:
        path = report / relative
        primary_manifest.setdefault("analysis_files", {})[relative] = {
            "bytes": path.stat().st_size,
            "sha256": file_hash(path),
        }
    primary_manifest["generated_at_utc"] = generated
    primary_manifest["analysis_hash"] = hashlib.sha256(
        json.dumps(primary_manifest["analysis_files"], sort_keys=True).encode("utf-8")
    ).hexdigest()
    primary_manifest_path.write_text(
        json.dumps(primary_manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    evidence_files = [
        "multisource_analysis_summary.json", "multisource_news_fold_metrics.csv",
        "multisource_onchain_fold_metrics.csv", "multisource_catalyst_strategy_decision.json",
        "multisource_catalyst_strategy_trades.csv.gz", "multisource_verification_summary.json",
        "REPORT.md", "artifact.json", "binance_runup_research.ipynb",
    ]
    evidence = {
        "generated_at_utc": generated,
        "verification_passed": True,
        "files": {name: {"sha256": file_hash(report / name), "bytes": (report / name).stat().st_size} for name in evidence_files},
    }
    (report / "multisource_evidence_manifest.json").write_text(
        json.dumps(evidence, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(report)


if __name__ == "__main__":
    main()
