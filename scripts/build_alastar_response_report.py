"""Native report of the broad signal inventory and all response-cycle studies."""
from datetime import datetime, timezone
import json
import sqlite3

import pandas as pd

from scripts.research_alastar_responses import OUT, ROOT, write

REPORT=ROOT/"reports/alastar_response_20261021"
NAMES={"fresh_view":"新方向观点","broad_directional":"广义方向观点","numeric_cross":"等待新突破",
       "numeric_state":"条件已满足也响应","until_opposite_update":"跟随后续观点变化","broad_plus_images":"广义观点加图片首段"}
PHASE={"all":"全期","earlier":"历史前段","later":"历史后段"}


def main():
    REPORT.mkdir(parents=True,exist_ok=True)
    r=json.loads((OUT/"response_results.json").read_text(encoding="utf-8"))
    d=json.loads((OUT/"response_diagnostics.json").read_text(encoding="utf-8"))
    t=json.loads((OUT/"taxonomy.json").read_text(encoding="utf-8"))["summary"]
    concentration={(x["scenario"],x["phase"]):x for x in d["concentration"]}
    flat=[]
    for s in r["summary"]:
        c=concentration.get((s["scenario"],s["phase"]),{})
        ci=s.get("week_block_95") or [None,None]
        flat.append({"scenario":s["scenario"],"family":NAMES[s["family"]],"family_id":s["family"],
                     "phase":PHASE[s["phase"]],"phase_id":s["phase"],"delay_minutes":s["delay"],
                     "hours":s["hours"],"holding":"30分钟" if s["hours"]==.5 else f'{s["hours"]:g}小时',
                     "n":s["n"],"mean_net":s.get("mean"),"mean_percent":100*s["mean"] if "mean" in s else None,
                     "win_rate":s.get("win_rate"),"drawdown":s.get("max_5m_open_drawdown"),
                     "without_best":s.get("without_best"),"without_best_coin":c.get("mean_without_best_coin"),
                     "best_coin":c.get("best_coin"),"best_coin_share":c.get("best_coin_share_of_total_net"),
                     "stress100":s["stress100"].get("mean"),"ci_low":ci[0],"ci_high":ci[1],
                     "position_fraction":.2,"round_trip_cost_bps":20})
    cycles=[x for x in flat if x["delay_minutes"]==15 and x["phase_id"]=="later" and x["family_id"] in
            ("fresh_view","broad_directional","until_opposite_update")]
    focus=[x for x in flat if x["delay_minutes"]==15 and x["family_id"]=="broad_directional" and x["phase_id"] in ("all","later")]
    conditions=[x for x in flat if x["delay_minutes"]==15 and x["phase_id"]=="later" and x["family_id"] in ("numeric_cross","numeric_state")]
    latency=[{**x,"delay":f'{x["delay_minutes"]}分钟',"resolution":"5分钟线" if x["price_resolution"]=="5min" else "小时线"}
             for x in d["timestamp_rounding"]]
    legacy=[{**x,"mean_difference":x["mean_5m"]-x["mean_1h"]} for x in d["legacy40_timing_comparison"]]
    selected=lambda h,p="later":next(x for x in focus if x["hours"]==h and x["phase_id"]==p)
    three,seven=selected(72),selected(168,"all")
    lag5=next(x for x in latency if x["delay_minutes"]==1 and x["price_resolution"]=="5min")
    lag1=next(x for x in latency if x["delay_minutes"]==1 and x["price_resolution"]=="h")
    legacy24=next(x for x in legacy if x["hours"]==24)
    sections=[
        ("title","# Alastar Signals: Timing and Response Cycles"),
        ("summary","## Executive Summary\n\n"
         "- **上轮把信号口径收得过窄。** 422条文字中，300条含市场／交易信息，69条属于新方向或条件观点；之前40条只是一个严格方向子集，不能解释为频道总共只有40条信号。\n"
         f"- **需要按周期看，三天响应值得继续验证。** 广义方向观点、最低延迟15分钟、持有72小时的历史后段有{three['n']}笔，平均单笔净收益{three['mean_net']:.2%}；1小时和4小时后段为负。这改变了只依据24小时样本得出的研究重点。\n"
         f"- **时刻与集中度仍决定可信度。** 小时线取整把最低1分钟延迟变成中位{lag1['median_actual_wait_minutes']:.1f}分钟实际等待；5分钟线为{lag5['median_actual_wait_minutes']:.1f}分钟。三天后段区间仍跨零，七天全期约{seven['best_coin_share']:.1%}净利润来自RAVE，尚不能据最高历史收益上线。"),
        ("inventory","## 300条信息消息，不等于300次独立新开仓\n\n"
         "这次按时间顺序复核全部422条文字，区分新方向、条件计划、持续更新、仓位与资金流、风险提醒、复盘／推广和图片依赖。一条消息可同时属于多个角色，所以角色计数不能相加当作消息总量。\n\n"
         "文字共形成329个结构化事件：77个即时方向、82个条件、94个风险提示、73个背景方向和3个更新类事件；随后单列纠正了‘旧观点失效’与‘新反向开仓’的混淆。7条可见编辑消息保留在信息统计中，并从收益测试排除。\n\n"
         "4张原图另作人工视觉核对：NOM、BLUAI、EDEN先画上行再画下行，TRADOOR为直接下行。图片补充池只测试第一段方向；转折时间和精确触发价未给出，不能声称还原完整两段交易。其余缺少图片／链上确认的情景保留为待研究，未强行生成订单。\n\n"
         f"收益研究包含{r['pools']['broad_directional']}个广义方向事件、{r['pools']['numeric_cross']}个可用数值价格代理检验的条件事件。持仓数随触发、周期和同币去重改变；信息数量与成交数量不应混用。"),
        ("definitions","## 先对齐信息可用时刻，再比较周期\n\n"
         "行情改为币安官方5分钟K线，资金费率也重新从官方接口完整分页取得；不再靠小时线插值，也不靠结算间隔猜测缺失资金费。发布时间统一用UTC，解释日期采用北京时间。\n\n"
         "比较6类响应：新方向、广义方向、等待新突破、条件已经成立也响应、随新反向观点／明确失效更新离场、广义方向加图片首段。每类最低处理延迟取1、15、60分钟，持有30分钟、1、4、24、72、168小时，共108个相关情景。时长上限不等于作者原计划。\n\n"
         "直接响应在发布时间加延迟之后的下一根完整5分钟线开盘入场。条件策略只使用已收盘K线；‘站稳／接受’用两根收盘确认作研究代理，确认后再等至少一分钟入场，条件有效期24小时。‘已经低于某价’与‘出现新的下穿’分别测试。未满足的鲸鱼卸仓、OI、清算等额外条件不能由价格代替。\n\n"
         "往返费用与滑点合计0.2%，另外计算资金费，并测试0.4%及1%总成本。每笔用初始本金20%，最多5笔并发、同币不叠加、检查保证金。方向冲突仅按原文明确的短期／高周期区分，否则剔除；短期桶用于不超过24小时，高周期桶用于至少72小时，是研究分桶，并非作者精确持仓指令。历史前后段仍固定在7月17日北京时间14:29，新增标签不重划边界。图表固定展示居中的15分钟延迟，并非选出的最佳延迟；全部108情景另表保留。"),
        ("cycles","## 历史后段的优势主要出现在较长持有期\n\n"
         f"下图固定最低延迟15分钟，比较三个方向响应模型的历史后段。广义观点持有72小时的{three['n']}笔均值{three['mean_net']:.2%}；同一模型持有1小时为{selected(1)['mean_net']:.2%}、4小时为{selected(4)['mean_net']:.2%}。周期改变了结果，不能把24小时固定持有的评价扩展到全部信号。\n\n"
         "更长持有会减少重复开仓，因此各柱的持仓数量不同。另做117个相同事件的周期对照，广义观点30分钟、1小时、4小时、24小时、3天、7天均值分别约0.23%、0.78%、−1.13%、1.56%、14.37%、13.10%；这些重复事件可能重叠，仅用于排除样本变化解释，不是另一条组合收益曲线。"),
        ("focus","## 三天可以列为候选，置信区间与回撤仍未过关\n\n"
         f"三天广义响应历史后段均值的周块重采样95%区间为{three['ci_low']:.2%}至{three['ci_high']:.2%}，仍跨过零。总往返成本提高到1%后均值约{three['stress100']:.2%}；去掉贡献最大的币后均值约{three['without_best_coin']:.2%}。这比小样本24小时结果更值得继续检验，但未构成稳定盈利证明。\n\n"
         "下表同时保留全期与历史后段。回撤按20%初始本金的组合、5分钟开盘价标记，不是单笔回撤，也不是盘中最坏点。三天全期仍有较大回撤与RAVE集中度；七天全期约95%净利润来自RAVE，最高均值不能作为挑选参数的依据。"),
        ("clock","## 小时线等待会遮住一部分早期响应\n\n"
         f"按全部422条消息计算，‘至少等1分钟，再取下一小时开盘’的实际等待中位数是{lag1['median_actual_wait_minutes']:.1f}分钟；改用5分钟开盘约{lag5['median_actual_wait_minutes']:.1f}分钟。下图展示不同最低延迟与取整分辨率；这是成交模型的等待，不是已经测得的Telegram传输延迟。\n\n"
         f"保留旧40条标注，在同一新价格／资金费／亏损下限模型里只改变入场取整：24小时事件均值由小时取整{legacy24['mean_1h']:.2%}变为5分钟取整{legacy24['mean_5m']:.2%}，仍为负。该对照用同40个事件，不做组合去重，因此不能直接与之前39笔组合数值相减归因。"),
        ("conditions","## 纯价格触发目前较弱，尚不能代表全部条件信号\n\n"
         "17个数值条件事件在最低延迟15分钟时，等待新穿越方案有11个触发并进入全期持仓、后段10个；其它条件未在24小时内触发。只按价格穿越或价格已满足就入场，当前后段多个周期为负。下图保留两种代理规则的结果。\n\n"
         "这并非证明作者全部条件策略无效。原文不少计划还要求鲸鱼减仓、OI变化或清算；我们没有当时相应时间序列，不能偷换为碰到支撑／阻力即成交。‘多头逻辑失效’也不自动等于新做空。"),
        ("actions","## 下一轮优先验证广义方向的三天响应\n\n"
         "1. **将三天响应列入前瞻候选。** 先冻结广义方向标签、时刻规则、成本与风险约束，再用首次实际收到的新消息验证；不把108个历史情景中的最高值当成已选出的实盘参数。\n"
         "2. **保留消息角色和路径。** 持续更新用于观点状态；风险提示用于风险控制候选；先涨后跌保留两段。后续需要为每种更新明确触发、失效与退出含义，避免每篇更新都叠一笔仓位。\n"
         "3. **继续积累真实收件记录。** 现有归档保留首次收件和修订。未来模拟入场必须晚于实际收件与标签冻结时间，不能回填历史开盘价；旧历史、迟到、编辑或未满足条件的消息分别记录。\n"
         "4. **暂以研究与模拟观察推进。** 当前没有启用自动下单、训练模型或调用外部AI API。后续重点是样本稳定性、币种集中度、延迟和可执行成本，而不是继续加参数找历史赢家。"),
        ("questions","## 什么证据会改变候选排序\n\n"
         "三天响应能否在首次实际收到的新消息上继续优于同时间对照？收益是否仍由RAVE等少数行情驱动？图片两段路径的转折有没有作者实时给出的确认？鲸鱼、OI与清算条件的原始时间序列是否可补齐？这些答案会决定它是可执行策略还是事后可解释的观点流。"),
        ("limits","## 结论仍属于历史探索\n\n"
         "本轮全量重新标注与108个情景均使用已知历史，部分模型可能知道相关历史；它们不是新的未见样本外验证。区间未经108次比较校正，单个区间或某个最高格子不能证明统计显著。\n\n"
         "当前可见文字／图片不能重建删除和原始版本，私有Bot未完整覆盖；图片只核对4个关键场景。价格、资金费时序、5分钟开盘成交、滑点与1倍逐仓强平都有近似；亏损按分配保证金封顶，不是精确标记价格强平模型。历史发布时间加延迟是可用时间假设。\n\n"
         "去重持仓仍可能受同一市场行情影响，不能称为统计独立样本；同事件对照可互相重叠。信息消息、结构化事件、触发次数和实际持仓是不同分母。本文不代表作者实际账户或全部产品的业绩。"),
    ]
    datasets={"cycles":cycles,"focus":focus,"latency":latency,"conditions":conditions,"legacy":legacy}
    sources=[{"id":"caption","label":"全部422条文字复核","path":"data/research/alastar_response_20261021/taxonomy.json"},
             {"id":"images","label":"4张图像与多段路径复核","path":"data/research/alastar_response_20261021/visual_review.json"},
             {"id":"protocol","label":"时刻与响应策略冻结协议","path":"data/research/alastar_response_20261021/response_protocol.json"},
             {"id":"official","label":"官方行情与资金费逐页原始证据","path":"data/research/alastar_response_20261021/response_results.json"},
             {"id":"channel","label":"Alastar公开频道","href":"https://t.me/alastartrades"}]
    with sqlite3.connect(OUT/"response_report_evidence.sqlite") as connection:
        for key,rows in datasets.items():
            tab="report_"+key
            pd.DataFrame(rows).to_sql(tab,connection,if_exists="replace",index=False)
            order={"cycles":"hours,family","focus":"hours,phase","latency":"delay_minutes,resolution",
                   "conditions":"hours,family","legacy":"hours","all_scenarios":"family,delay_minutes,hours,phase"}[key]
            query=f'SELECT * FROM "{tab}" ORDER BY {order}'
            datasets[key]=json.loads(pd.read_sql_query(query,connection).to_json(orient="records",force_ascii=False,double_precision=12))
            sources.append({"id":key,"label":"已复核响应结果 / "+key,
                            "path":"data/research/alastar_response_20261021/response_report_evidence.sqlite",
                            "query":{"engine":"sqlite","language":"sql","sql":query,"tables_used":[tab],
                                     "description":"SQL读取完整事件研究、同样本时刻对照与集中度结果，非重新择优筛选。",
                                     "upstream_scripts":["scripts/research_alastar_responses.py","scripts/analyze_alastar_response_results.py"],
                                     "upstream_files":["response_results.json","response_diagnostics.json","admitted_trades.jsonl"],
                                     "metric_definitions":["单笔净收益以名义本金为分母，扣往返20bps与实际资金费；边界不确定结算取不利界限。",
                                                            "组合每笔为初始本金20%；回撤按5分钟开盘标记。",
                                                            "历史边界固定7月17日，108情景均为相关探索；置信区间未经多重比较校正。"]}})
    blocks=[]
    for identity,body in sections:
        blocks.append({"id":identity,"type":"markdown","body":body})
        if identity in ("cycles","clock","conditions"):
            blocks.append({"id":identity+"-plot","type":"chart","chartId":identity})
        if identity=="focus": blocks.append({"id":"focus-table-block","type":"table","tableId":"focus-table"})
        if identity=="clock": blocks.append({"id":"legacy-table-block","type":"table","tableId":"legacy-table"})
    charts=[]
    for identity,title,dataset,x,y,color,xtitle,ytitle in (
        ("cycles","不同响应模型的历史后段单笔均值","cycles","holding","mean_percent","family","最长持有时间","净收益（%）"),
        ("clock","处理延迟与取整后的实际等待","latency","delay","median_actual_wait_minutes","resolution","最低处理延迟","中位等待（分钟）"),
        ("conditions","两种价格条件响应的历史后段单笔均值","conditions","holding","mean_percent","family","最长持有时间","净收益（%）")):
        charts.append({"id":identity,"title":title,"type":"bar","dataset":dataset,"sourceId":dataset,
                       "options":{"grouping":"grouped"},"encodings":{"x":{"field":x,"type":"nominal","title":xtitle},
                       "y":{"field":y,"type":"quantitative","title":ytitle},"color":{"field":color,"type":"nominal","title":"响应规则" if color=="family" else "价格分辨率"}}})
    tables=[{"id":"focus-table","title":"广义方向、延迟15分钟的周期对照","dataset":"focus","sourceId":"focus",
             "defaultSort":{"field":"hours","direction":"asc"},"columns":[
                 {"field":"hours","label":"小时","type":"number"},{"field":"phase","label":"阶段","type":"text"},
                 {"field":"n","label":"持仓数","type":"number"},{"field":"mean_net","label":"单笔均值","type":"percent"},
                 {"field":"drawdown","label":"组合回撤","type":"percent"},{"field":"without_best_coin","label":"去最佳币后均值","type":"percent"},
                 {"field":"stress100","label":"成本1%后均值","type":"percent"}]},
            {"id":"legacy-table","title":"同40个旧标注事件：只改变入场取整","dataset":"legacy","sourceId":"legacy",
             "defaultSort":{"field":"hours","direction":"asc"},"columns":[
                 {"field":"hours","label":"持有小时","type":"number"},{"field":"n_matched","label":"匹配事件数","type":"number"},
                 {"field":"mean_1h","label":"小时取整均值","type":"percent"},{"field":"mean_5m","label":"5分钟取整均值","type":"percent"}]}]
    now=datetime.now(timezone.utc).isoformat()
    artifact={"surface":"report","manifest":{"version":1,"title":"Alastar Signals: Timing and Response Cycles","surface":"report",
               "generatedAt":now,"blocks":blocks,"charts":charts,"tables":tables,"sources":sources},
               "snapshot":{"version":1,"status":"ready","generatedAt":now,"datasets":datasets},"sources":sources}
    write(REPORT/"artifact.json",artifact)
    (REPORT/"RESEARCH_NOTES.md").write_text("\n\n".join(x[1] for x in sections),encoding="utf-8")
    pd.DataFrame(flat).to_csv(REPORT/"ALL_SCENARIOS.csv",index=False)
    write(REPORT/"REPORT_QA_NOTES.json",{"audience":"product stakeholders","delivery":"mcp-app",
          "required_structure":["Title","Executive Summary","Key findings with visual evidence","Recommended next steps","Further questions","Caveats and assumptions"],
          "chart_contracts":[{"id":c["id"],"type":"grouped bar","rows":len(datasets[c["dataset"]]),"width":"full report row",
                              "palette":"native categorical defaults; visible legends","zero_baseline":True,"claim":c["title"]} for c in charts],
          "repeated_bar_reason":"Discrete holding periods, timing resolution and trigger policies; lines would imply unobserved intermediate responses.",
          "quantitative_prose_without_extra_chart":"Counts and concentration use direct values; exact clock control uses4rowtable; no decorative trends.",
          "prior_reports_preserved":True,"source_labels_are_not_profit_probabilities":True})
    print(str(REPORT/"artifact.json"))


if __name__=="__main__": main()
