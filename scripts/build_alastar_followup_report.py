"""Build the bounded native follow-up report and supporting audit narrative."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data/research/alastar_followup_20261021"
REPORT = ROOT / "reports/alastar_followup_20261021"


def load(name):
    return json.loads((DATA / name).read_text(encoding="utf-8"))


def main():
    REPORT.mkdir(parents=True, exist_ok=True)
    r, expanded, audit = load("robustness.json"), load("expanded_results.json"), load("funding_boundary_audit.json")
    review, month = load("coverage_review.json"), load("explicit_horizon_case.json")
    now = datetime.now(timezone.utc).isoformat()
    phase_names = {"development": "历史前段", "holdout": "固定日期后的历史段", "all": "全期"}
    headline = []
    for pool, label, parts in (("original", "首轮26条观点", r["summaries"]),
                               ("expanded", "全文复核40条观点", expanded["expanded_40"]["phases"])):
        for phase in ("development", "holdout", "all"):
            m = parts[phase]
            headline.append({"pool": label, "phase": phase_names[phase], "phase_id": phase,
                             "n": m["n"], "mean_net": m["mean_trade_return"], "mean_percent": 100*m["mean_trade_return"],
                             "win_rate": m["win_rate"], "median_net": m["median_trade_return"],
                             "portfolio_return": m["fixed_initial_20pct_portfolio_return"],
                             "drawdown": m["max_hourly_drawdown"], "without_best": m["mean_without_best_trade"],
                             "holding_hours": 24, "round_trip_cost_bps": 20,
                             "fixed_cutoff_utc": r["fixed_historical_holdout_start"]})
    delays, stops = [], []
    for s in r["scenarios"]:
        row = {"phase": phase_names[s["phase"]], "n": s["n"],
               "mean_percent": 100*s["mean_trade_return"], "drawdown": s["max_hourly_drawdown"],
               "holding_hours": 24, "cost_bps": 20, "pool": "首轮26条观点"}
        if s["family"] == "delay" and s["phase"] in ("all", "holdout"):
            delays.append({**row, "delay": str(s["parameter"])+"分钟", "delay_minutes": s["parameter"]})
        if s["family"] == "stop" and s["phase"] in ("all", "holdout"):
            stops.append({**row, "stop": f'{s["parameter"]:.0%}', "stop_fraction": s["parameter"],
                          "triggers": s["stop_triggers"], "funding": "命中小时不确定结算取不利前缀界限"})
    for phase in ("all", "holdout"):
        m = r["summaries"][phase]
        stops.append({"phase": phase_names[phase], "n": m["n"], "mean_percent": 100*m["mean_trade_return"],
                      "drawdown": m["max_hourly_drawdown"], "holding_hours": 24, "cost_bps": 20,
                      "pool": "首轮26条观点", "stop": "无固定止损", "stop_fraction": 0., "triggers": 0,
                      "funding": "原始逐时结算口径"})
    eh = expanded["expanded_40"]["phases"]["holdout"]
    ea = expanded["expanded_40"]["phases"]["all"]
    ah = r["summaries"]["holdout"]
    funding = audit["metrics"]["full"]
    sections = [
        ("title", "# Alastar Signal Research: Follow-up"),
        ("summary", "## Executive Summary\n\n"
         f"- **继续作为研究候选，尚未达到独立策略上线标准。** 全文复核把方向观点从26条扩展到40条；剔除一笔同币重叠后为{ea['n']}笔，全期平均每笔净收益{ea['mean_trade_return']:.2%}，胜率{ea['win_rate']:.1%}。\n"
         f"- **后段正收益仍依赖极少数交易。** 沿用原固定时间边界，扩展池后段{eh['n']}笔平均{eh['mean_trade_return']:.2%}；去掉最大盈利交易后为{eh['mean_without_best_trade']:.2%}。\n"
         "- **完善了数据留档与风险核算。** 新增首次实际收到时间、原文版本与待复核队列；历史补采不计为前瞻样本。复核资金费率边界、逐仓亏损下限和止损后的结算资格，结论未转为稳定盈利。"),
        ("scope", "## 这次评估的是方向观点，尚未重建作者完整交易计划\n\n"
         "2026年10月21日重新采集22页公开预览，仍有422条文字／图片说明，最新可见文字发表于10月14日；与上一份归档相比，没有新增正文或正文变化。全文审阅发现14条首轮未纳入的方向观点，其中10条被关键词过滤漏掉，4条在候选中被排除。原26条标签保留，新增解释单列。\n\n"
         "主方案仍为消息发布至少一分钟后下一根完整小时线开盘入场、持有24小时、往返费用与滑点共0.2%，按实际标记价格和方向计算资金费。每笔名义本金为初始资金20%，同币重叠不叠加。历史前后段边界固定为7月17日北京时间14:29，未因扩样重新划分。所有单笔均值以名义本金为分母，组合收益另算。文中独立持仓指同币不重叠，仍可能共同受行情影响，并非统计独立观测。\n\n"
         "这些文字包含方向判断、当前状态持续条件、目标和复盘，通常缺少可执行的入场、止损与退出规则。因此24小时事件研究不能等同作者实际策略表现。新增标注及本轮敏感性均属于探索性研究。"),
        ("coverage", "## 补齐文字覆盖后，全期结果仍未转正\n\n"
         f"扩展池全期{ea['n']}笔平均{ea['mean_trade_return']:.2%}，20%固定初始本金组合收益{ea['fixed_initial_20pct_portfolio_return']:.2%}。下图比较同一固定日期边界下的两个解释池，新增观点并未制造一个新的未见样本。扩展池后段均值的周块重采样95%区间跨过零。\n\n"
         f"首轮后段{ah['n']}笔平均{ah['mean_trade_return']:.2%}，最大交易占净利润{ah['best_trade_share_of_net']:.1%}；扩样后后段去掉最大盈利交易便变为{eh['mean_without_best_trade']:.2%}。按本次解释把观点自动转成固定24小时多空单，仍缺乏足够证据。"),
        ("delay", "## 延迟入场没有带来跨阶段一致的改善\n\n"
         "下图保持原26条观点，分别在发布至少1、60、180、360分钟后的下一完整小时线入场，持有24小时。原后段平均单笔由6.08%变为5.47%、3.99%、5.83%，变化并不单调；全期四种延迟均为负。不能凭这组历史结果选择一个最佳等待时间。小时线本身也无法检验秒级抢跑或极短泵行情。"),
        ("stops", "## 固定止损减少部分尾部亏损，仍未建立稳定盈利\n\n"
         "对原26条观点单独测试5%、10%、20%固定止损，持有上限仍为24小时；三种情景全期平均净收益均为负。下图展示全期和原历史后段，不能把某个后段最高值当成已选出的策略。\n\n"
         "止损根据小时高低价判断触发，若开盘已越过止损则按更差开盘价成交。命中小时之后的资金结算排除，小时内资格不清的结算按可能前缀取不利界限。这是保守的时序情景；没有订单簿与逐笔成交数据，仍不能保证止损执行价或最大损失。"),
        ("horizon", "## 对齐文字期限，也必须检查中途持仓能否存活\n\n"
         "原26条中，只有9月4日的AKE观点给出明确数值期限：一个月内回到0.0003附近。将它单列按一个日历月检查，不能将24小时结果直接解释为该月度预测成败。\n\n"
         "该价格路径在9月15日触及本研究的近似1倍空单强平阈值，早于10月4日的期限。它说明：方向最终如何与账户能否撑过路径是两个需要分别验证的问题。本例只作文本期限与路径风险说明；小时交易价并非精确标记价格强平模型，未据此给作者月度预测统计胜率。"),
        ("audit", "## 独立复算确认数据一致，执行假设仍需披露\n\n"
         f"原26笔结果复算一致，原38份行情和资金费率文件哈希未变化；新旧共10组官方价格／资金费锚点均匹配。把整点附近一秒内的结算按整点判断资格，全期均值从{funding['canonical_mean_net']:.2%}变为{funding['alternative_mean_net']:.2%}，后段8笔不变。\n\n"
         f"原极端SIREN空单将本金损失后继续叠加费用和资金费，得到超过100%的净损失，这与1倍逐仓标签不一致。仅把最终净损失下限设为100%的情景，全期均值为{funding['canonical_cap_only_mean_net']:.2%}，仍未转正；这只是收益下限敏感性，并非动态维持保证金模拟。旧结果保留用于审计。\n\n"
         "同入场反向和始终做多基线、去掉单一币种、周块区间及成本压力均已保留。反向全期较好也由极端事件影响，不能据此部署反向策略。"),
        ("next", "## 接下来用首次收到的新消息验证，而不是继续挑历史最优组合\n\n"
         "1. **保留原26条和扩展40条的解释记录。** 每条新消息先记录原文、首次收到时间、编辑状态，再提取方向、时间范围、条件和失效规则。条件未触发或只有方向观点时，进入研究复核队列。\n"
         "2. **前瞻统计从实际收到的新消息开始。** 既有422条历史消息不能冒充新样本；首次收到超过10分钟、可见编辑、截断或时间异常的消息不进入前瞻队列。后续编辑保留原版，并撤销该消息当前的待复核资格。\n"
         "3. **冻结规则后再看后续行情。** 固定24小时／0.2%成本作为基准；前瞻模拟入场须晚于实际收件和解释冻结时间，不能回填到之前的开盘价。若要研究文本期限或条件策略，应另行先冻结解释与触发规则；最低积累30笔独立新持仓、至少3个月，再检查成本、回撤、集中度和对照。该数量只是复核门槛，并不保证统计显著。\n"
         "4. **当前只记录和研究。** 归档已接入现有新闻采集轮次；待复核队列不产生可执行买卖单，不启用外部AI API，也不改变交易执行模式。"),
        ("questions", "## 哪些新增证据可能改变判断\n\n"
         "作者的私有Bot是否提供与公开观点不同的完整入场、失效和退出规则？截图中的关键价格能否在消息当时可靠提取？这些规则在首次收到的新消息上，能否扣除实际成本后优于同时间匹配基线？这些证据比继续尝试更多历史参数更有价值。"),
        ("caveats", "## 当前结论的范围\n\n"
         "公开预览不能重建已删除消息、早期文字版本或私有Bot完整告警，图片未做OCR。当前模型可能知道相关历史，全文复核虽然不读取逐笔价格挑标签，也不能成为真正未见历史的验证。新增40条仍只有39笔独立持仓，模型解释信心不是盈利概率。\n\n"
         "费用、滑点、资金结算时序、逐仓强平和小时回撤均有近似；所有情景相关，未通过调参挑胜者。归档使用本机实际UTC接收时间，依赖本机时钟与公开预览可见性。本文仅评价所述方向文本事件规则，不评价作者全部产品或实际账户业绩。"),
    ]
    blocks = []
    for key, body in sections:
        blocks.append({"id": key, "type": "markdown", "body": body})
        if key == "coverage":
            blocks.extend([{"id": "comparison-plot", "type": "chart", "chartId": "pool-comparison"},
                           {"id": "headline-table-block", "type": "table", "tableId": "headline-table"}])
        if key == "delay": blocks.append({"id": "delay-plot", "type": "chart", "chartId": "latency"})
        if key == "stops": blocks.append({"id": "stop-plot", "type": "chart", "chartId": "risk-stops"})
    datasets = {"comparison": headline, "delays": delays, "stops": stops}
    sources = [{"id": "raw", "label": "Alastar 公开预览", "href": "https://t.me/s/alastartrades"},
               {"id": "review", "label": "全部422条文字逐条复核与原句", "path": "data/research/alastar_followup_20261021/coverage_review.json"},
               {"id": "audit", "label": "独立数据与资金边界审计", "path": "data/research/alastar_followup_20261021/funding_boundary_audit.json"},
               {"id": "previous", "label": "上一轮完整研究保留", "path": "reports/telegram_signal_study_20261020/artifact.json"},
               {"id": "observation", "label": "首次观测归档规范", "path": "docs/ALASTAR_OBSERVATION_ARCHIVE.md"}]
    with sqlite3.connect(DATA / "followup_evidence.sqlite") as connection:
        for key, rows in datasets.items():
            table = "report_" + key
            pd.DataFrame(rows).to_sql(table, connection, index=False, if_exists="replace")
            ordering = {"comparison": "pool, phase_id", "delays": "delay_minutes, phase", "stops": "stop_fraction, phase"}[key]
            query = f'SELECT * FROM "{table}" ORDER BY {ordering}'
            datasets[key] = pd.read_sql_query(query, connection).to_dict(orient="records")
            sources.append({"id": key, "label": "复核研究结果 / " + key,
                            "path": "data/research/alastar_followup_20261021/followup_evidence.sqlite",
                            "query": {"engine": "sqlite", "language": "sql", "sql": query,
                                      "tables_used": [table], "description": "读取逐条原文冻结后计算、并经过执行边界复核的研究结果。",
                                      "upstream_files": ["robustness.json", "expanded_results.json", "scenario_outcomes.jsonl", "funding_boundary_audit.json"],
                                      "upstream_script": "scripts/research_alastar_robustness.py",
                                      "metric_definitions": ["单笔净收益=方向价格变动-往返费用滑点+实际签名资金费；小时止损使用不利结算资格界限。",
                                                             "历史后段固定为原7月17日边界；扩样不重新分割。",
                                                             "组合每笔为初始本金20%，不复利；独立持仓排除同币重叠。"]}})
    charts = []
    for identity, title, dataset, x, xlabel, color in (
        ("pool-comparison", "两个解释池的24小时单笔平均净收益", "comparison", "phase", "固定时间分段", "pool"),
        ("latency", "原26条观点的入场延迟情景", "delays", "delay", "发布后最低等待时间", "phase"),
        ("risk-stops", "原26条观点的固定止损情景", "stops", "stop", "固定止损比例", "phase")):
        charts.append({"id": identity, "title": title, "type": "bar", "dataset": dataset, "sourceId": dataset,
                       "options": {"grouping": "grouped"},
                       "encodings": {"x": {"field": x, "type": "nominal", "title": xlabel},
                                     "y": {"field": "mean_percent", "type": "quantitative", "title": "平均每笔净收益（%）"},
                                     "color": {"field": color, "type": "nominal", "title": "解释池" if color == "pool" else "历史范围"}}})
    table = {"id": "headline-table", "title": "同一24小时规则的准确数值", "dataset": "comparison", "sourceId": "comparison",
             "defaultSort": {"field": "pool", "direction": "asc"},
             "columns": [{"field": "pool", "label": "解释池", "type": "text"},
                         {"field": "phase", "label": "历史段", "type": "text"},
                         {"field": "n", "label": "独立持仓", "type": "number"},
                         {"field": "mean_net", "label": "单笔均值", "type": "percent"},
                         {"field": "win_rate", "label": "胜率", "type": "percent"},
                         {"field": "portfolio_return", "label": "20%本金组合收益", "type": "percent"},
                         {"field": "without_best", "label": "去掉最大盈利后均值", "type": "percent"}]}
    artifact = {"surface": "report", "manifest": {"version": 1, "surface": "report", "title": "Alastar Signal Research: Follow-up",
                "generatedAt": now, "blocks": blocks, "charts": charts, "tables": [table], "sources": sources},
                "snapshot": {"version": 1, "status": "ready", "generatedAt": now, "datasets": datasets}, "sources": sources}
    (REPORT / "artifact.json").write_text(json.dumps(artifact, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    (REPORT / "RESEARCH_NOTES.md").write_text("\n\n".join(body for _, body in sections), encoding="utf-8")
    notes = {"audience": "product stakeholders", "delivery": "mcp-app", "previous_full_report_preserved": True,
             "required_structure": ["Title", "Executive Summary", "Key findings with visual evidence", "Recommended next steps", "Further questions", "Caveats and assumptions"],
             "chart_contracts": [{"section": c["id"], "family": "bar", "rows": len(datasets[c["dataset"]]),
                                  "question": c["title"], "units": "percent points of per-trade net return",
                                  "palette": "native categorical defaults, legend labels distinguish groups",
                                  "zero_baseline": True, "width": "full report row"} for c in charts],
             "repeated_bar_reason": "All three ask discrete scenario/group comparisons; no few-point trend interpolation.",
             "omissions": {"capacity": "No historical order book; fixed slippage scenarios only.",
                           "image_signals": "No OCR source-at-publication record.", "true_forward_returns": "No newly timely observed messages yet."}}
    (REPORT / "REPORT_QA_NOTES.json").write_text(json.dumps(notes, ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame(headline).to_csv(REPORT / "primary_metrics.csv", index=False)
    prospective = {"version": 1, "created_at": now, "execution_authorized": False,
                   "archive": "data/research/alastar_observation", "first_receipt_max_delay_seconds": 600,
                   "historical_imports_count_as_forward": False, "required_new_independent_trades": 30,
                   "required_active_months": 3, "primary_hold_hours": 24, "round_trip_cost_bps": 20,
                   "selection": "Unedited full text from first timely receipt; freeze direction, asset and rationale before future outcome join.",
                   "forward_entry_time": "ceil_to_next_hour(max(publication_at, actual_first_received_at, label_frozen_at) + 60 seconds); never backfill an earlier execution price",
                   "historical_entry_time_is_assumed_latency": True,
                   "semantic_schema": ["primary_asset", "view_or_executable_instruction", "direction", "entry_condition_quote", "trigger_observed_at", "explicit_horizon_quote", "invalidation_quote", "evidence_quote", "message_revision_sha256", "label_frozen_at"],
                   "decision_gate": "Positive costs/funding result with uncertainty, matched baseline checks and concentration robustness; sample-size gate alone is not significance.",
                   "secondary_hypotheses": "Any new horizon/condition/risk rule needs separate pre-outcome frozen specification; no best historical scenario selected."}
    (DATA / "prospective_protocol.json").write_text(json.dumps(prospective, ensure_ascii=False, indent=2), encoding="utf-8")
    print(str(REPORT / "artifact.json"))


if __name__ == "__main__":
    main()
