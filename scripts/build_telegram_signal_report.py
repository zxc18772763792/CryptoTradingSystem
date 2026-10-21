"""Build a source-backed bounded report from validated Telegram study outputs."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data/research/telegram_signal_study_20261020"
REPORT = ROOT / "reports/telegram_signal_study_20261020"


def main():
    REPORT.mkdir(parents=True, exist_ok=True)
    result = json.loads((DATA / "summary.json").read_text(encoding="utf-8"))
    audit = json.loads((DATA / "validation.json").read_text(encoding="utf-8"))
    inventory = json.loads((DATA / "source_inventory.json").read_text(encoding="utf-8"))
    events = [json.loads(l) for l in (DATA / "event_outcomes.jsonl").read_text(encoding="utf-8").splitlines()]
    now = datetime.now(timezone.utc).isoformat()
    a = result["alastartrades"]["horizons"]["24"]
    c = result["cryptoquant_alert"]["horizons"]["24"]
    ah, ch = a["holdout"]["funding_covered_matched"], c["holdout"]["funding_covered_matched"]
    share = audit["primary_recomputation"]["alastartrades"]["holdout"]["best_trade_share_of_total_net"]
    interval = ch["calendar_week_bootstrap_95pct_mean"]
    headline = []
    for source, label in (("alastartrades", "Alastar 离线 AI 判断"), ("cryptoquant_alert", "CryptoQuant 流向规则")):
        phases = result[source]["horizons"]["24"]
        for phase, phase_name in (("development", "前 70% 历史段"), ("holdout", "后 30% 历史留出")):
            row = phases[phase]
            metric = row["funding_covered_matched"]
            headline.append({
                "source":label,"phase":phase_name,"n":metric["n"],"mean_net":metric["mean_trade_return"],
                "median_net":metric["median_trade_return"],"win_rate":metric["win_rate"],"profit_factor":metric["profit_factor"],
                "portfolio_return":row["signal_portfolio"]["return"],"hourly_drawdown":row["signal_portfolio"]["max_hourly_drawdown"],
                "holdout_start":result[source]["historical_holdout_start"],"round_trip_bps":20,"position_fraction":.2,
            })
    trade_ids = set(a["holdout"]["trade_ids"])
    detail = []
    for row in events:
        if row["holding_hours"] == 24 and row["source_id"] in trade_ids and row["status"] == "ok":
            at = pd.Timestamp(row["entry_at"]).tz_convert("Asia/Shanghai")
            detail.append({"label":at.strftime("%m-%d")+" "+row["symbol"].removesuffix("USDT"),
                           "symbol":row["symbol"],"entry_at":row["entry_at"],"direction":"多" if row["direction"]>0 else "空",
                           "net_percent":100*row["net_return"],"net_return":row["net_return"],
                           "funding_return":row["funding_return"],"url":row["url"],
                           "evidence":" ".join(row["evidence"].split()[:18])})
    detail.sort(key=lambda row:row["entry_at"])
    equity = pd.read_csv(DATA / "hourly_equity.csv", parse_dates=["at"])
    crypto = equity[(equity["source"]=="cryptoquant_alert") & (equity["phase"]=="holdout")].copy()
    crypto["at"] = pd.to_datetime(crypto["at"],utc=True)
    curve = []
    names = {"signal":"流向规则","inverted":"相反方向","always_long":"始终做多（同入场）"}
    for variant, frame in crypto.groupby("variant"):
        daily=frame.set_index("at")["equity"].resample("1D").last().dropna()
        curve.extend({"date":str(at),"equity":float(v),"variant":names[variant],"source":"cryptoquant_alert",
                      "holding_hours":24,"position_fraction":.2} for at,v in daily.items())
    assert len(curve)<=2000
    sensitivity=[]
    for hours, phases in result["cryptoquant_alert"]["horizons"].items():
        for key,label in (("funding_covered_matched","总成本 20 bps"),("cost_stress_40bps","总成本 40 bps")):
            values=phases["holdout"][key]
            sensitivity.append({"hours":int(hours),"cost":label,"mean_net_percent":100*values["mean_trade_return"],
                                "n":values["n"],"win_rate":values["win_rate"],"profit_factor":values["profit_factor"],
                                "selection":"同一预设持有期内的同入场交易，未用持有期择优"})
    headline.sort(key=lambda x:(x["source"],x["phase"]))
    sources=[
        {"id":"study","label":"冻结信号与回测结果","path":"data/research/telegram_signal_study_20261020/summary.json",
         "query":{"engine":"Python / pandas","language":"python","sql":"from scripts.research_telegram_signal_strategy import main\nmain()",
                  "tables_used":["frozen_signals.json","event_outcomes.jsonl","hourly_equity.csv","data/historical/binance_futures"],
                  "filters":["已标注的前瞻判断 / BTC或ETH原始流向告警","下一根完整小时线 + 至少60秒延迟","同币同时间只保留一笔，最多五笔并发","时间前70%与后30%分开，跨界交易剔除"],
                  "metric_definitions":["单笔净收益 = 方向*(出场价/入场价-1) - 20bps + 按实际结算与持仓方向计算的资金费率收益。",
                                        "组合每笔名义本金为初始资金20%，无复利、最高1倍初始名义敞口；回撤按小时持仓市值计算。",
                                        "同入场反向与始终做多为匹配事件基线；不是各自择优后的策略。"]}},
        {"id":"archive","label":"公开频道采集清单与 Bot 来源核对","path":"data/research/telegram_signal_study_20261020/source_inventory.json",
         "query":{"description":"公开预览原始HTML、消息编号和发布时间分别保留；Alastar完成可见分页，CryptoQuant限定100页，研究频道限定5页。","tables_used":["alastar_20261020/collection_manifest.json","cryptoquant_alert_20261020/collection_manifest.json","cryptoquant_official_20261020/collection_manifest.json"]}},
        {"id":"annotation","label":"AI逐条判断与原句证据","path":"data/research/telegram_signal_study_20261020/ai_annotations.json"},
        {"id":"validation","label":"独立价格、资金费率和收益复算","path":"data/research/telegram_signal_study_20261020/validation.json",
         "query":{"engine":"Python / pandas","language":"python","sql":"from scripts.verify_telegram_signal_study import main\nmain()",
                  "tables_used":["official_price_anchors.json","official_funding_anchors.json","event_outcomes.jsonl","summary.json"],
                  "description":"从原始parquet重新计算主方案全部已入选交易，并核对官方接口锚点。"}},
        {"id":"pumpglass","label":"PumpGlass 官方 Bot 说明","href":"https://pumpglass.com/about"},
        {"id":"cq-alert","label":"CryptoQuant 公开告警频道","href":"https://t.me/cryptoquant_alert"},
    ]
    sections=[
        ("title", "# Telegram Signals: Strategy Feasibility"),
        ("summary", "## Executive Summary\n\n"
         "**当前证据不足以把这些消息直接上线为独立策略。**\n\n"
         f"- **Alastar：后段收益由少量样本支撑。** 26 条明确方向判断中，历史后段仅 {ah['n']} 笔独立交易，单笔平均净收益 {ah['mean_trade_return']:.2%}；最大一笔占后段净利润约 {share:.1%}。前段平均 {a['development']['funding_covered_matched']['mean_trade_return']:.2%}，跨阶段不稳定。\n"
         f"- **CryptoQuant：简单流向规则缺乏稳健优势。** 后段 {ch['n']} 笔，单笔平均净收益 {ch['mean_trade_return']:.3%}，周块重采样95%区间为 {interval[0]:.3%} 至 {interval[1]:.3%}；成本提高到40bps后均值 {c['holdout']['cost_stress_40bps']['mean_trade_return']:.3%}。\n"
         "- **PumpGlass Bot 尚不能判定。** 两个官方Bot已核对，但未取得完整原始告警历史；频道里展示的最高涨幅与宣传复盘未计作入场信号。"),
        ("population", "## 这次检验的是可执行规则，而不是宣传中的最高涨幅\n\n"
         "采集 Alastar 公开文字 **422 条**（2026年2月至10月），按冻结关键词筛出143条候选，由本次离线AI研究提取26条方向较明确的前瞻观点。条件未触发、只说不宜做空、已实现涨幅复盘、宣传及明显含糊的内容不自动当买卖指令。未做图片文字识别。\n\n"
         "CryptoQuant Alert 取得 **1,994 条**（2024年1月至2026年10月，限定100页），将明确BTC/ETH的交易所流入及矿工流入映射为做空假设、流出映射为做多假设。内部转账的通用免责声明保留为不确定性标签；不能把资金移动等同于实际买卖。官方研究频道另取69条作来源核对，未混进告警收益。\n\n"
         "主方案固定持有24小时，发布后至少等待60秒，从下一根完整小时线开盘成交；费用10bps加往返滑点10bps，并扣／收实际资金费率。同币持仓重叠时不叠加；每笔占初始资金20%，最多五笔并发。分段按信号先后划为前70%与后30%，跨边界持仓剔除。两个来源的日期窗口不同，不直接用收益排名。"),
        ("alastar", "## Alastar 的后段正收益需要更多独立样本确认\n\n"
         f"后段 {ah['n']} 笔胜率 {ah['win_rate']:.1%}、盈利因子PF {ah['profit_factor']:.2f}；去掉最大盈利交易后，剩余平均收益降至 {ah['mean_without_best_trade']:.3%}。样本未达到预设的30笔门槛，活跃周也不足以给出可靠的周块置信区间。\n\n"
         "下图每根柱是一笔独立持仓的净收益，不是账户收益。前段整体亏损且出现一次1倍逐仓近似清算，说明固定24小时复制文字方向在这些波动币上可能承担很大尾部风险。"),
        ("cryptoquant", "## CryptoQuant 的流向告警没有通过成本和不确定性检验\n\n"
         f"24小时后段胜率 {ch['win_rate']:.1%}、PF {ch['profit_factor']:.2f}，中位数单笔收益 {ch['median_trade_return']:.3%}。均值的95%区间跨过零，前段均值为 {c['development']['funding_covered_matched']['mean_trade_return']:.3%}。正收益不能解释为稳定、可重复的预测能力。\n\n"
         "净值曲线使用相同入场记录与20%固定初始本金，同时展示流向规则、反向、始终做多的匹配基线。它检验的是这条简单映射；不代表否定CryptoQuant其他指标或自定义告警。"),
        ("sensitivity", "## 改持有期并未建立稳定的盈利证据\n\n"
         "6、24、72、168小时是预设的敏感性场景，全部披露，未依据后段结果挑选最佳周期。图中20bps与40bps使用同一批对应持有期的入场记录，均包含资金费率。不同持有期的重叠过滤会改变独立交易数量。"),
        ("actions", "## 下一步先积累可验证的实时样本\n\n"
         "1. **Alastar 保留为研究候选。** 冻结当前26条标注的解释规则与成本假设，未来逐条保存首次收到的原文，至少再取得30笔独立交易后复核；不要仅凭这8笔后段样本上线。\n"
         "2. **CryptoQuant 暂不作为独立做多／做空策略推进。** 若要研究为已有策略的过滤条件，需另设匹配对照与前瞻样本；本研究尚未证明过滤能改善收益。\n"
         "3. **PumpGlass 先补原始告警。** 必须包含所有触发、时间戳、标的、筛选条件及失败案例，才能检验泵捕捉和巨鲸开仓两种Bot信号。"),
        ("questions", "## 仍需解决的证据缺口\n\n"
         "PumpGlass 的泵告警与巨鲸告警历史是否完整？作者原帖是否被编辑或删除？公开频道之外的个人自定义告警条件是否不同？这些问题会改变样本选择和结论，当前没有把它们假设为已解决。"),
        ("caveats", "## 历史回放仍受消息版本与成交模型限制\n\n"
         "- 采到的是当前可见文字；编辑标记、删除记录和早期版本无法完整重建。因此这里称为**历史后段留出检验**，不称真正未见数据的前瞻样本外验证。\n"
         "- AI使用当前模型离线解释消息，无法证明模型从未见过相关历史；仅使用消息内容作判断，标注在价格联结前冻结。143条以外的消息未交给语义模型逐条评分。\n"
         "- 作者给出的巨鲸仓位／链上数据没有被当作官方成交事实核验。多月目标统一用固定持有期检验，不等于逐条复制作者完整交易计划。\n"
         "- 小时线延迟可能错过快速泵行情；滑点是情景假设，缺少当时订单簿，1倍清算仅用成交价OHLC近似。组合回撤按小时标记，不保证覆盖每个盘中极值。\n"
         "- 数据只覆盖当前可见公开分页；CryptoQuant100页并非完整历史，PumpGlass Bot私有告警、个人自定义告警与图片内信号均未覆盖。\n"
         "- 这次未训练模型或寻找最优参数，也未改变现有交易执行或实盘配置。"),
        ("audit", "## 数据与计算已按原始记录复核\n\n"
         "原始HTML、消息SHA、标注原句、冻结信号与逐笔结果均保留。独立脚本从parquet重算主方案全部入选交易；4组官方K线锚点和2组资金费率锚点一致，时间采用UTC，显示说明采用北京时间。\n\n"
         "复现：先运行研究脚本，再运行独立验证脚本与本报告生成脚本。公开采集器与交易计算的边界用合成案例测试，覆盖延迟成交、缺K线、资金费率方向、免责声明、重叠持仓和极端空头损失。"),
    ]
    blocks=[]
    for identity, body in sections:
        blocks.append({"id":identity,"type":"markdown","body":body})
        if identity=="population":blocks.append({"id":"summary-table-block","type":"table","tableId":"summary-table"})
        if identity=="alastar":blocks.append({"id":"alastar-chart-block","type":"chart","chartId":"alastar-trades"})
        if identity=="cryptoquant":blocks.append({"id":"crypto-curve-block","type":"chart","chartId":"crypto-equity"})
        if identity=="sensitivity":blocks.append({"id":"cost-chart-block","type":"chart","chartId":"cost-sensitivity"})
    manifest={"version":1,"surface":"report","title":"Telegram Signals: Strategy Feasibility","generatedAt":now,
              "description":"Alastar离线AI判断、CryptoQuant原始流向告警与PumpGlass数据缺口的历史检验。",
              "sources":sources,"blocks":blocks,
              "charts":[
                  {"id":"alastar-trades","title":"Alastar 后段单笔24小时净收益","type":"bar","dataset":"alastar_trades","sourceId":"study",
                   "encodings":{"x":{"field":"label","type":"nominal","title":"北京时间入场日期 / 币种"},"y":{"field":"net_percent","type":"quantitative","title":"净收益（%）"}}},
                  {"id":"crypto-equity","title":"CryptoQuant 24小时后段匹配净值","type":"line","dataset":"crypto_equity","sourceId":"study",
                   "encodings":{"x":{"field":"date","type":"temporal","title":"日期"},"y":{"field":"equity","type":"quantitative","title":"初始净值=1"},"color":{"field":"variant","type":"nominal","title":"规则"}}},
                  {"id":"cost-sensitivity","title":"CryptoQuant 后段持有期与成本场景","type":"bar","dataset":"sensitivity","sourceId":"study",
                   "encodings":{"x":{"field":"hours","type":"nominal","title":"持有小时"},"y":{"field":"mean_net_percent","type":"quantitative","title":"平均单笔净收益（%）"},"color":{"field":"cost","type":"nominal","title":"成本假设"}}},
              ],
              "tables":[{"id":"summary-table","title":"主方案24小时：历史前后段","dataset":"headline","sourceId":"study",
                         "columns":[{"field":"source","label":"来源","type":"text"},{"field":"phase","label":"阶段","type":"text"},
                                    {"field":"n","label":"独立交易","type":"number"},{"field":"mean_net","label":"单笔平均净收益","type":"percent"},
                                    {"field":"win_rate","label":"胜率","type":"percent"},{"field":"portfolio_return","label":"组合收益","type":"percent"},
                                    {"field":"hourly_drawdown","label":"最大小时回撤","type":"percent"}],
                         "defaultSort":{"field":"source","direction":"asc"}}]}
    artifact={"surface":"report","manifest":manifest,
              "snapshot":{"version":1,"status":"partial","generatedAt":now,
                          "accessIssues":[{"sourceId":"pumpglass","message":"未取得PumpGlass两个官方Bot的完整原始告警历史，不能判断其策略收益。"}],
                          "datasets":{"headline":headline,"alastar_trades":detail,"crypto_equity":curve,"sensitivity":sensitivity}},
              "sources":sources}
    # Materialize the reviewed Python results, then run the actual SQL feeding
    # each widget. Preserve the upstream Python provenance in source metadata.
    database = DATA / "report_evidence.sqlite"
    with sqlite3.connect(database) as connection:
        for dataset, rows in list(artifact["snapshot"]["datasets"].items()):
            table = "report_" + dataset
            pd.DataFrame(rows).to_sql(table, connection, if_exists="replace", index=False)
            ordering = {"headline":"source, phase","alastar_trades":"entry_at",
                        "crypto_equity":"date, variant","sensitivity":"hours, cost"}[dataset]
            query = f'SELECT * FROM "{table}" ORDER BY {ordering}'
            reviewed = pd.read_sql_query(query, connection)
            artifact["snapshot"]["datasets"][dataset] = reviewed.to_dict(orient="records")
            source_id = "dataset-" + dataset
            sources.append({"id":source_id,"label":f"已验证研究结果 / {dataset}",
                            "path":"data/research/telegram_signal_study_20261020/report_evidence.sqlite",
                            "query":{"engine":"sqlite","language":"sql","sql":query,"tables_used":[table],
                                     "description":"SQL读取已经独立复算的Python研究结果；上游完整计算见scripts/research_telegram_signal_strategy.py与scripts/verify_telegram_signal_study.py。",
                                     "upstream_code":"from scripts.research_telegram_signal_strategy import main\nmain()",
                                     "upstream_files":["summary.json","event_outcomes.jsonl","hourly_equity.csv","validation.json"],
                                     "metric_definitions":["所有收益先按信号持仓方向扣除往返20bps和实际资金费率。","净值每笔使用初始资金20%，不是单笔全仓复利。"]}})
            for chart in manifest["charts"]:
                if chart["dataset"] == dataset:
                    chart["sourceId"] = source_id
            for table_spec in manifest["tables"]:
                if table_spec["dataset"] == dataset:
                    table_spec["sourceId"] = source_id
    (REPORT/"artifact.json").write_text(json.dumps(artifact,ensure_ascii=True,indent=2),encoding="utf-8")
    # Supporting audit note; the primary reader surface is the native report artifact.
    (REPORT/"RESEARCH_NOTES.md").write_text("\n\n".join(body for _,body in sections)+
        "\n\n原始结果：data/research/telegram_signal_study_20261020/summary.json\n"
        "\n复现脚本：scripts/research_telegram_signal_strategy.py → scripts/verify_telegram_signal_study.py → scripts/build_telegram_signal_report.py\n",encoding="utf-8")
    pd.DataFrame(headline).to_csv(REPORT/"primary_metrics.csv",index=False)
    print(str(REPORT/"artifact.json"))


if __name__=="__main__":
    main()
