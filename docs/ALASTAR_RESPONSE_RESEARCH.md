# Alastar 广义信息与多周期响应研究

2026-10-21，针对“422条消息不应只剩40条信号”的复核。

本地导航：[研究总入口](ALASTAR_RESEARCH_INDEX.md) · [策略研究与尝试台账](ALASTAR_STRATEGY_EXPERIMENTS.md)。

先按发布时间逐条进行一次性离线语义解释，再联结新取得的官方5分钟K线。
422条文字均有内容SHA和原句证据；300条含市场信息，69条是新方向或条件观点。
重复更新、风险提醒、观点失效和依赖额外数据的条件不是无信息，但也不能都变成新订单。
原26/40条研究保留。本轮结果是探索性修订，不能改称新的未见样本。

本轮文件：

- `data/research/alastar_response_20261021/labels_*.json`：三个时间分区的完整文字标注。
- `semantic_overrides_late.json`、`root_semantic_overrides.json`：保留原版后的语义修正。
- `visual_review.json`：四张公开原图的首段／多段路径核对。
- `response_protocol*.json`：结果联结前的参数和修订记录。
- `official_5m/`、`official_funding/`：只读公共接口的原始分页、时刻、查询区间和SHA。
- `resolved_response_events.json`：最终333个事件，含风险／非入场事件。
- `response_results.json`、`response_outcomes.jsonl`、`admitted_trades.jsonl`：108情景及前后段／全期。
- `response_diagnostics.json`：集中度、同事件周期对照、旧40条的等模型时刻比较。
- `response_validation.json`、`response_diagnostics_validation.json`：独立复核回执。
- `reports/alastar_response_20261021/`：原生报告、文字稿、全部情景CSV。

使用项目解释器从仓库根目录复现：

```powershell
$py = "F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe"
& $py -m scripts.research_alastar_responses prepare
# 标注与视觉解释属于一次性研究成果，不在重跑时调用AI服务。
& $py -m scripts.research_alastar_responses merge
& $py -m scripts.research_alastar_responses fetch --max-requests 600
& $py -m scripts.research_alastar_responses fetch_funding
& $py -m scripts.research_alastar_responses evaluate
& $py -m scripts.analyze_alastar_response_results
& $py -m scripts.build_alastar_response_report
& $py -m pytest tests/test_alastar_responses.py -q
# 只读取现有结果，整理108组试验台账和未激活的前瞻协议；不会重新下载或回测。
& $py -m scripts.archive_alastar_research
```

价格下载按完整研究窗合并，可从缓存继续；不调用交易、账户、Telegram私聊或AI接口。
不得把小时线插值成分钟线。资金费必须有官方分页覆盖证据，缺失不能当零；
官方完整查询证明区间内无结算时，零才是已知值。新突破与已满足状态分别检验，
确认K线必须收盘，且不能用交易之后的无关缺口反向剔除已触发的持仓／损失。

止损、强平、开盘成交和资金结算边界仍有近似。本文使用1倍分配保证金损失下限，
并非动态标记价格强平模拟。单币不叠加不意味着统计独立；按周分块区间未做108次
比较校正。未来入场应晚于实际首次收到和标签冻结时间，不能回填历史开盘。

本轮没有修改新闻运行配置、开启自动交易或建立新的定时任务。

前瞻模拟的新版时刻规则见`prospective_paper_protocol_v1.json`：
下一5分钟开盘不早于`max(发布+15分钟, 实际首次收件+60秒, 标签冻结+60秒)`。
该协议仅写入本地，状态为`documented_not_running`，尚未接通模拟成交登记器。
旧报告的小时开盘规则继续作为历史研究保留；不能将两种成交口径混算。
