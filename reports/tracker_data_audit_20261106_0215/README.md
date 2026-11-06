# 跟踪器与 Claude 新发现审计 · 2026-11-06

北京时间 02:15 启动，线上接口和账本抽样集中在 02:17–02:27。差分基线是 11 月 5 日 14:52 的状态快照。Claude 的相关可见结论来自 11 月 5 日 18:23、20:06 和 11 月 6 日 00:05，账户、资金费、行情和排期均用对应数据复核。

## 运行与前向账本

Web 运行在纸面模式，`/readyz` 返回 200，币安和 Gate 在线。策略主行情仍有 2/6 个订阅时点陈旧，个别符号随检查时刻变化。Telegram 固定来源两张卡在 02:18 显示采集异常，最近一次失败为 02:16:46 的 `RuntimeError`。02:22:27 成功采集后，两张卡恢复“前向观察”，成熟样本仍为零。失败记录只保留错误类型，分类路由有匹配模型的成功回执，根因无法从现有诊断确定。[线上接口](F:/9_Crypto/crypto_trading_system/reports/tracker_data_audit_20261106_0215/live_api_compact.json) · [恢复复查](F:/9_Crypto/crypto_trading_system/reports/tracker_data_audit_20261106_0215/final_health_recheck.json)

注册表 28 条。15 份旧账本的 133 条终态财务记录逐条哈希一致，没有新增终态。单独审计的新版日志中，CryptoQuant Alert 主统计新增 1 笔至 4 笔，均值 **−0.889 个百分点/笔**。next10xgem1 v2 新增 1 笔至 11 笔，均值 **+5.305 个百分点/笔**。两者是各自价格收益相对预先定义对照的超额。两笔新增样本的超额分别为 −0.442 和 −2.646 个百分点。[状态差分](F:/9_Crypto/crypto_trading_system/reports/tracker_data_audit_20261106_0215/state_audit.json) · [新增结算日志复算](F:/9_Crypto/crypto_trading_system/reports/tracker_data_audit_20261106_0215/other_new_settlements.json)

| 跟踪器 | 线上与账本核对 |
|---|---|
| CoinGlass 原版 | 18 期结算、7 期在持，+0.572%/期（单腿名义金额），总名义口径 +0.286%。下一期最早 11 月 6 日 10:51 到期。历史参照 +0.10%/期。 |
| CoinGlass 月度版 | 新增 UTC 11 月 4 日信号一期，5 期在持、0 期结算。成仓时已有 321/354 个合约，两个账本的 256 个入选分数逐项一致，4 个入场价抽样落在币安对应分钟的高低区间。日数据后来补齐到 354 个合约，XEC 在成仓后入库。首期最早 11 月 8 日 15:40 到期。 |
| CoinGlass 扩池 | 已停用，10 期完整结算、3 期未能结算，历史参照 −0.20%/期。11 月 5 日 UTC 日线在 02:15 北京尚未收盘，下一次采集最早 08:15 开始。 |
| 反转与爆仓过滤 | 原版 33 天、+0.208%/天（多空总名义），过滤 24 个配对日、+0.196 个百分点/天。各 1 期在持。上一轮确认的历史基准问题仍适用，线上已撤下两个旧回测数。 |
| 解锁 T−7 | 原账本 5 笔，+8.22%/笔（对冲单腿名义）。独立 ≥10% 影子组登记 5 笔、4 个入场日，另有 5 笔 5–10% 对照，合计 0 笔结算。第一笔 SIGN 的入场日为 11 月 8 日 UTC，按原规则取该日收盘。该账复用原账的约 119–123 币对冲篮子，Claude 的账户研究另用前 30。 |
| 供给因子 | 0 个完整前向结算，10 月缺 ACX 的 +7.12% 保留为诊断。11 月冻结篮子也缺 ACX 入场价，仍在持有期。 |
| Upbit 韩元上币 | 6 笔结算、0 笔在持，+13.337%/笔（空头入场名义金额），90% 区间 −3.48% 至 +30.15%，5 个入场日组。 |

[CoinGlass 分期证据](F:/9_Crypto/crypto_trading_system/reports/tracker_data_audit_20261106_0215/coinglass/README.md) · [线上汇总](F:/9_Crypto/crypto_trading_system/reports/tracker_data_audit_20261106_0215/live_api_compact.json) · [T−7 影子日志](F:/9_Crypto/crypto_trading_system/data/research/unlock_t7_big_v1/journal.jsonl)

## Claude 新发现的核验

### 旧回测的两处数据问题

- **解锁资金费漏记成立。** 保存的 T−7 历史文件 273 笔里，150 笔资金费记为零，现存币安档案这 150 笔在重放窗口内均有非零资金费。按重放时点和标记价折算，旧资金费项平均高估 **0.226 个百分点/笔**。LAYER 的 2025 年 5 月 6–12 日窗口里，旧缓存结算条数为零，币安公开接口返回 138 条，按入场名义折算为 **−19.806%**，直接相加的费率为 −34.263%。旧回测把失败或空响应都落成空资金费，旧请求记录不足以区分每次归零的原因。
- **Upbit 历史存在不可成交样本。** 旧表把 ICX 警示和 AI、RAY 韩元上币三笔标作 `ok`。币安官方日线在三个事件各自的 8 天持有区间里，全是相同价格，成交量和成交笔数均为零。纸面收益各为 −0.10%，即仅扣费用。剔除会改变历史基准的样本组成；当前 6 笔前向的入场和退出价另有真实行情支持。

[逐笔资金费与三组原始 K 线](F:/9_Crypto/crypto_trading_system/reports/tracker_data_audit_20261106_0215/legacy_event_sources.json) · [币安资金费率历史接口](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Get-Funding-Rate-History) · [币安日线接口](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Kline-Candlestick-Data)

### 韩元上币入场价格

按 Claude 保存的事件名单从币安重查 74 个事件的小时价格。54 个留出事件在 UTC 日线收盘后首小时均值 **−2.170%**，41 个下跌，简单 t 值 −3.75。20 个开发事件同口径为 −1.755%，合计 74 个为 −2.058%。这一复算验证名单内的价格变化，历史事件名单的完整性还需单独评估。

6 笔前向的 Upbit 公告首次时间和币安进出价、资金费均逐笔匹配。账本用北京时间 08:00 收盘价入场，实际盘口快照晚约 3 分 23 秒至 27 分 43 秒，快照中间价平均低 **2.58%**，中位低 1.87%。保持出场和资金费不动，只替换入场价，纸面均值从 **+13.34% 降至 +11.24%/笔**。Claude 的提前 1 小时 **+6.18%** 保留了对冲篮子的原收盘入场，无法据此估计两腿一起提前成交的结果。

[逐笔公告核对](F:/9_Crypto/crypto_trading_system/reports/tracker_data_audit_20261106_0215/krw/official_upbit_notice_crosscheck.json) · [小时线、盘口与账本复算](F:/9_Crypto/crypto_trading_system/reports/tracker_data_audit_20261106_0215/krw/binance_krw_timing_replay.json)

### 账户收益与覆盖范围

按保存的逐日仓位路径，用独立模拟器复算事后选出的 **≥10% 解锁加韩元上币** 双腿组合。127 笔、672 个日历日，Sharpe **3.632**，年化 **+25.75%**，最大回撤 **−2.98%**。去掉最好 20 笔，Sharpe **1.262**，这 20 笔占逐笔收益总和的 **70.05%**。三种冻结组合均能重算，研究脚本在正式打开留出段前已输出过 2025、2026 的按年收益。最终 127 笔组合是在看过结果后选的。

叠加去掉 APR、限制篮子极端上涨、统一资金费边界和假定的 20 万美元冲击成本后，Sharpe **2.875**。再加入一小时延迟近似为 **2.048**。冲击系数未按成交校准，延迟仅将差额放入原持仓路径的第一天。币安 K 线确认证据中 RAVE 篮子腿在 2026 年 4 月 10–16 日从 1.60594 升至 16.98929，约 **+958%**，这解释了收益对篮子极端值的敏感性。压力结果属于同一段历史的情景值。

15 笔有入场前的事件记录，共 11 个入场日，持仓重叠形成 2 段。按研究组合事后重放，每笔 **+3.61%**，去掉最好两笔为 **−0.70%**。目标子集去掉 APR 后剩 7 笔，**+5.91%/笔**，按逐笔独立假设做符号翻转的 p 为 **0.227**。这些前向事件能验证时间戳，组合本身还没有按这套统一执行规则积累独立前向样本。

按过去 60 天篮子走势重算，≥10% 解锁在下跌、横盘、上涨状态的单笔均值为 **+5.33%（50）**、**+9.43%（39）**、**+1.48%（22）**。上涨组按币聚类的重采样尾概率约 0.254。比较 DefiLlama 两份缓存的 148 个共同币种，2025 年以来截至 9 月 27 日的 77 个 ≥10% 历史事件，日期和规模在两份缓存间一致。该比较无法证明各笔历史入场当时就能看到排期。

[账户独立复算](F:/9_Crypto/crypto_trading_system/reports/tracker_data_audit_20261106_0215/event_portfolio/independent_recalc.json) · [压力和行情分组](F:/9_Crypto/crypto_trading_system/reports/tracker_data_audit_20261106_0215/event_portfolio/stress_recalc.json) · [排期两版逐日对照](F:/9_Crypto/crypto_trading_system/reports/tracker_data_audit_20261106_0215/legacy_event_sources.json)

### 更早一轮的资金费及趋势研究

按保存的成交表，负溢价做多 140 笔均值 +9.57%，中位 −6.84%，去掉最大 5 笔后 −0.42%。负资金费缩短间隔做多 132 笔均值 +4.31%，中位 −9.06%，去掉最大 3 笔后 −1.26%。两项的正均值都依赖右尾。此次只重算了这两个成交表及相关留出统计，未对第一轮全部六个策略重复建立行情和模拟路径。[负溢价成交表](C:/Users/zxc/AppData/Local/Temp/claude/93bc013aacae6232/d7a802af-aec4-4746-bd5c-659cad3a9a92/scratchpad/team_mining/lead_check/neg_premium_mine.csv) · [负资金费成交表](C:/Users/zxc/AppData/Local/Temp/claude/93bc013aacae6232/d7a802af-aec4-4746-bd5c-659cad3a9a92/scratchpad/team_mining/events/c1_holdout_trades.csv)

本轮只读核验。可复现的账户结果支持继续作为纸面研究，现有样本和价格证据仍不足以确认实盘表现。
