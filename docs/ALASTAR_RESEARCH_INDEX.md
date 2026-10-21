# Alastar 本地研究总入口

整理日期：2026-10-21。对象为 Alastar Trades 公开频道的可见历史和当前首次收件归档。

**当前判断：信息覆盖此前被低估，响应周期会改变评价；广义方向的三天响应值得前瞻模拟，尚未证明稳定盈利。**

> 10/21 第四轮补充（见下表第四轮行）：三天响应在同币零分布和48小时反转规则对照下没有可证实的择时或方向价值。
> 若继续前瞻，对照应改为同币随机时点和反转规则而非零；唯一线索是首次喊单持有1小时，已起草 v2 协议（未运行）。

## 先读这三份

| 文件 | 用途 |
|---|---|
| [最新完整研究](../reports/alastar_response_20261021/RESEARCH_NOTES.md) | 结论、图表说明、时刻影响、集中度与限制 |
| [策略研究与尝试台账](ALASTAR_STRATEGY_EXPERIMENTS.md) | 六类响应、成功与失败尝试、修订理由、下一轮模拟协议 |
| [方法及复现](ALASTAR_RESPONSE_RESEARCH.md) | 数据位置、命令、价格与资金费规则、复核证据 |

## 研究是怎样走到当前结论的

| 轮次 | 做过的研究 | 应当如何理解 |
|---|---|---|
| 首轮：狭义26条 | 422条文字中先筛143条候选，再解释出26条明确方向；小时线、固定24小时响应；另研究CryptoQuant告警 | [原报告](../reports/telegram_signal_study_20261020/RESEARCH_NOTES.md)保存原始口径和原始结果，不代表全频道只有26条信息 |
| 第二轮：扩展40条 | 追加14条方向观点，39笔同币去重持仓；研究延迟、固定止损、文本期限和极端空头路径 | [跟进报告](../reports/alastar_followup_20261021/RESEARCH_NOTES.md)全期24小时均值−1.04%；仍是狭义方向研究，未覆盖所有信息角色 |
| 当前轮：全量角色与时刻 | 全422条逐条解释，300条含市场／交易信息、69条为新方向或条件观点；复核4张原图；改用官方5分钟价格及完整资金费分页 | 六类响应×三种延迟×六种周期，共108组相关探索；三天广义方向是新的研究候选 |
| 第四轮：选币／方向／时效复核 | [定时复核](../reports/telegram_signal_review_20261021/RESEARCH_NOTES.md)加同币随机时点零分布；[第四轮](../reports/alastar_selection_timing_20261021/RESEARCH_NOTES.md)对账三天候选、山寨指数与同量级币、48小时反转规则、整族校正、5分钟时效衰减 | 三天候选对发帖前30天同币零分布超额+0.35%（p=0.43），整族校正无一格显著；仅剩首次喊单持有1小时的薄线索（中位数约+0.1%～0.2%，主要来自前段、后段未复现）；无分钟级时效衰减 |

旧报告数值和标签保留供审计，不能用新版覆盖后声称旧结果从未存在。
300条是信息消息数，69条是新观点消息数，333是最终结构化事件数；121是广义文字方向研究池。
每条消息可有多个角色或事件；实际持仓还受编辑、确认条件、周期、同币重叠和资金约束影响。
这些分母分别回答不同问题。

## 结论与试验优先级

固定最低延迟15分钟，历史后段广义方向：30分钟−0.42%、1小时−0.30%、4小时−0.44%、
24小时+2.81%、72小时+6.66%、168小时+4.49%。均为扣20bps总往返成本和资金费后的单笔均值，非账户收益。

72小时后段35笔的按周95%区间为−1.14%至+15.23%，仍跨零；去掉贡献最大的币后均值约+5.10%。
该方案20%固定初始本金组合的5分钟开盘标记回撤约−19.25%。
七天全期约95.4%净利润集中于RAVE。所有结果来自已知历史，不能按最高收益挑参数后称为验证。

最低延迟1分钟配下一小时开盘的实际等待中位数为31.1分钟，5分钟开盘为3.5分钟。
这是成交模型造成的等待，尚不是Telegram到本机的实测传输延迟。

下一轮优先做广义文字方向72小时的前瞻模拟，保留24小时对照；条件触发、图片完整两段和风险过滤暂作独立研究。
具体规则已写入[前瞻模拟协议v1](../data/research/alastar_response_20261021/prospective_paper_protocol_v1.json)，
当前为`documented_not_running`：仍需前瞻标签冻结与模拟成交登记器及其验证，不能当作已经接入运行的策略。

10/21 另起草[前瞻模拟协议v2](../data/research/alastar_response_20261021/prospective_paper_protocol_v2.json)（`documented_not_running`，与v1独立）：
只检验“首次喊单、收件+60秒后下一5分钟开盘入场、持有1小时”一格，统计量是相对同币入场前30天随机起点的超额；
发帖后超过30分钟才能入场的记为跳过。单边检验检出+1.5%/笔约需66笔、+1.0%约需149笔（约1～2年）。
启动前要先让新闻worker的观测归档连续运行，并用`observe_alastar_signals.py --latency`确认收件延迟。

## 本地文件位置

| 内容 | 入口 |
|---|---|
| 原始消息及公开页面 | `data/research/alastar_20261021/messages.jsonl`、同目录`pages/` |
| 全文解释与语义修正 | `data/research/alastar_response_20261021/labels_*.json`、`taxonomy.json`、`*overrides*.json` |
| 原图及视觉解释 | 同目录`images/`、`visual_review.json` |
| 冻结方法和最终事件 | 同目录`response_protocol*.json`、`resolved_response_events.json` |
| 官方价格与资金费证据 | 同目录`official_5m/`、`official_funding/`，共172份／213,016行K线和68份／6,034条资金费记录 |
| 所有情景与逐笔结果 | 同目录`response_results.json`、`response_outcomes.jsonl`、`admitted_trades.jsonl` |
| 全108组人类可读表 | [ALL_SCENARIOS.csv](../reports/alastar_response_20261021/ALL_SCENARIOS.csv)，含前段／后段／全期324行 |
| 全108组结构化研究台账 | [strategy_experiment_ledger.json](../data/research/alastar_response_20261021/strategy_experiment_ledger.json)，每组含三阶段、成本压力、集中度、研究判断及来源SHA |
| 集中度与时刻对照 | 同目录`response_diagnostics.json` |
| 独立复算回执 | 同目录`response_validation.json`、`response_diagnostics_validation.json`，均ok、0项差异 |
| 本次整理交接 | [research_handoff.json](../data/research/alastar_response_20261021/research_handoff.json) |
| 现有首次收件归档 | [归档说明](ALASTAR_OBSERVATION_ARCHIVE.md)，`data/research/alastar_observation/` |
| 新闻接入 | [接入说明](TELEGRAM_ALASTAR_INGESTION.md) |

13,488条入选结果含情景和阶段重复，不能当作13,488次独立交易。官方行情行数也不是独立研究样本数。
报告原生组件验证与渲染已接受；界面运行时路径错误使截图视觉复核未完成，数值证据和限制见报告QA文件。

## 继续研究时先做什么

1. 先查本入口和尝试台账，保留当前历史结果及SHA；新解释、新参数、新数据分别追加版本。
2. 未来消息保存实际收件和标签冻结时刻；模拟成交必须晚于两者。不得把历史补采或迟到解释回填为前瞻入场。
3. 冻结后如消息被编辑，保存修订，不追溯删除原记录或亏损；只看当前队列会产生未来信息筛选。
4. 图片转折、鲸鱼减仓、OI、清算条件需要当时的原始时序，不能靠事后文字补齐。
5. 完整归档原始公开页、研究数据、报告和首次收件哈希链。`data/`及生成报告不依赖Git保存；复制备份时应覆盖这些目录。

本轮整理仅写入本地文档、研究台账和未激活的模拟协议；没有启动新的回测、定时任务或自动交易。

使用项目解释器重新生成尝试台账和结构化交接：

```powershell
$py = "F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe"
& $py -m scripts.archive_alastar_research
```

此命令只读取已有本地结果并核对来源，生成台账和交接文件；已存在的前瞻v1协议不会被覆盖。修改试验规则应新增协议版本。
