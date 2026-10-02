# 全量审计分报告：AI、数据、研究、回测与预测市场

审计日期：2026-10-01（Asia/Shanghai）。审计对象：`F:\9_Crypto\crypto_trading_system`，HEAD `2fecd05dea8c5ee438b4071e50548b828b7411af`。本报告仅记录审计结果；没有修改业务代码、生产参数、研究原始数据或账户状态。

## 范围、方法和边界

已读取适用根目录 `AGENTS.md`、主工程 `README.md` 和 `prediction_markets/polymarket/README.md`；分配目录内未发现额外 `AGENTS.md`。主工程 Git 边界、关联工作树和整套测试由主审计统一核验，本分报告不重复计算 `_radar_refactor`。

分配的 15 个范围共 193 个 Python 文件、75,354 行，均完成完整文本读取、AST 解析、导入与顶层调用枚举，以及未来数据、填充、持久化、并发、网络、时效、安全敏感执行模式扫描。解析错误 0 个。逐文件清单及 138 个关联测试候选见 [data_research_coverage.md](data_research_coverage.md)，定义行号、SHA256、模式命中与导入见 [data_research_inventory.json](data_research_inventory.json)。结构扫描不等于每一函数都经过动态验证；重点人工审查与动态验证范围见后文。

重点追踪：行情 Hub→实时价格→执行链；AI 决策/研究/训练→验证→晋级/衰减；DSL 与因子→缓存→回放；新闻采集→游标→原始存储；Polymarket 市场→报价→纸盘风控/成交→账户持久化；行情文件锁与修复；premium、宏观、资金费率、OI、orderbook 数据边界；指标、观测、通知、watchdog 与部署生命周期。

未导入项目 settings，也没有读取 `.env`、`.env.local`、密钥文件；未启动服务、worker、实盘、通知发送器、外部 API 或交易账户操作。动态复现通过 AST 仅提取原文件函数/类定义，删去原导入和顶层执行，用标准库、项目 Python 内的 numpy/pandas 与 Mock DB/行情构造执行原始函数体；写入仅限本审计目录。使用指定解释器 `F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe -B`。

复现脚本 [data_research_repro.py](data_research_repro.py) 已运行成功，输出 [data_research_repro.json](data_research_repro.json)。复现证明函数契约问题，并不证明真实外部服务一定产生这些异常输入。整套测试的结果与环境依赖分类以主报告为准。

## 已确认问题

本分报告无确认的 P0；3 个 P1、12 个 P2。P1 表示影响实盘决策边界或常规数据完整性的高优先问题；P2 表示研究、纸盘、统计、缓存或生命周期可靠性问题。下列行号基于上述 HEAD。

### DR-01 / P1：旧交易所报价被本地接收时间重新认定为新鲜

- 文件/行号：`core/marketdata/hub.py:110-114`、`:376-406`、`:501-539`；`core/marketdata/runtime_price_provider.py:43-44`；执行关联 `core/trading/execution_engine.py:1392-1419`（执行子审计独立核验）。
- 触发：上游持续重发相同交易所时间戳的旧价，或首次投递很旧的报价；价格为有限正数。
- 证据：`age_ms` 使用 `timestamp_received`，`upsert_tick` 仅拒绝严格小于上一条的交易所时间戳，同一旧时间戳可重复写入当前接收时间。Mock 连续喂入 2020-01-01 的报价100，在 `fail_closed=True, allow_rest_fallback=False` 下仍得到 `ok=True, age_ms≈7`。执行 `_resolve_price` 仅检查 `provider.ok`，无交易所时间年龄复核。
- 影响：上游 replay/stall 可持续保持绿灯；下单量、止损参考或估值可继续使用过时价格。没有发送任何订单，不宣称复现真实成交。
- 建议：分别记录和检查接收活性、源行情事件时间；对有可信事件时间的 feed 设置明确源年龄、重复序列/时间语义，未知时间按来源策略降级；执行入口再检查有限性和价格时效。保留心跳时间作运行观测，勿用其替代行情新鲜度。

### DR-02 / P1：AI enforce 模式把无效 action 静默改为 allow

- 文件/行号：`core/ai/live_decision_router.py:103-125`、`:761-769`、`:787-810`。
- 触发：provider 返回语法合法 JSON，但缺少 action、为 `{}` 或 action 拼错，例如 `BLOCKED`；配置 `enabled=True, mode=enforce, fail_open=False`。
- 证据：`payload.get("action") or "allow"` 为缺失值选 allow，任何未在 enum 的值也改 allow。Mock provider 返回 `{"action":"BLOCKED"}` 后，结果 `allowed=True, action=allow`。因未抛出 schema 错误，fail-closed 异常分支不会执行。
- 影响：AI 审核契约失效时绕过配置的拒绝策略；其他风控仍可能挡单，但 AI enforce 这道边界已经放行。
- 建议：严格 schema、必填 enum、字段类型/数值验证；无效响应作为 provider 错误进入现有 fail_open/fail_closed 分支；添加空对象、大小写/拼写、未知动作与缺失字段验收。

### DR-03 / P1：新闻游标先推进，再截断/存储，常规批次即可永久漏数据

- 文件/行号：`core/news/collectors/manager.py:380`、`:428-437`、`:470`、`:506-533`；`core/news/collectors/common.py:217-255`；`core/news/service/worker.py:623`。
- 触发：某来源返回条数多于全局 max_records，或游标 commit 后原始新闻保存失败/进程中断。
- 证据：manager 先对全部拉取结果提交最大 timestamp 游标，再按时间降序取前 max_records 返回；worker 在返回后才 `save_news_raw`。common 仅接收 `ts > cursor`。原 manager/common 定义 + Mock DB，单来源16条、max_records10：首次仅返回10条，第二次0条，6条无法再进入持久化；游标提交发生在调用者能保存前。per_source=ceil(max_records*1.6) 正常配置即会产生超量。
- 影响：原始新闻、事件、情绪/宏观输入出现静默永久缺口；同 timestamp 后到文章也会被严格时间游标漏掉。保存失败可能漏整批。
- 建议：把全部来源事件先写入 durable inbox，和游标推进原子提交；处理/展示上限放在存储后。游标应只覆盖已可靠保存的事件，并用 timestamp+稳定ID/带重叠 lookback 去重处理迟到事件。

### DR-04 / P2：单标的 ML 训练切分没有 purge 标签未来区间

- 文件/行号：`core/ml/pipeline.py:272-279`、`:401-439`、`:795`；调用路径 `web/api/ml.py:678`。
- 触发：单标的 `run_signal_training_pipeline`，forward_bars>0，时序 holdout。
- 证据：标签由未来 close 构成，随后直接按 train_count 截断特征和标签。200个小时bar、horizon4、test_size.2，测试首点2025-01-07 16:00；训练末4点12:00—15:00的标签分别使用测试16:00—19:00的close。
- 影响：holdout 结果受训练期提前观察测试价格污染；模型选择与泛化质量被高估。默认 pooled 管线已有 time purge（`:377-398`），本问题限定单标的路径。
- 建议：依据每个样本 label_end_time 删除训练标签触及测试边界的样本，含缺bar场景；同一切分工具供 pooled/单标的使用，固定 chronology/dedup 规则。

### DR-05 / P2：因子缓存数据指纹忽略 high/low/volume 等输入

- 文件/行号：`core/factors_ts/cache.py:95-108`、`:210-266`；`core/factors_ts/impl.py` 的 `SpreadProxyFactor`。
- 触发：两个 replay frame 具有相同 close、长度和首尾index，但修正了 high/low/volume/资金费率/OI等因子依赖；首bar缓存校验结果相同。
- 证据：指纹只 hash close；核验仅初次或每500bar最后值。实际 SpreadProxyFactor：第一frame第二bar high101，修正frame high121；close100/low99不变，首bar相同。两frame指纹相同，第二frame返回缓存0.02，而原函数实际0.22。
- 影响：参数优化/研究在修订数据上得到旧因子，不可复现，行情修复无法可靠生效；index中间时间变化同样不进入指纹。
- 建议：按完整 immutable 数据集版本和各因子真实依赖列/完整index建键，或完整内容hash；修订行情、增量窗口、并行任务分别验证缓存隔离。

### DR-06 / P2：策略 DSL 向后填充使用未来值

- 文件/行号：`core/research/strategy_program.py:334-338`。
- 触发：一般/legacy strategy_program 路径 source 列有前导缺失，例如外部flow在第三bar才可用。
- 证据：`.ffill().bfill()` 使 [NaN,NaN,99] 的前两bar读99；仅用前两bar运行得[0,0]，完整数据运行前两bar得[99,99]，破坏前缀不变性。
- 影响：缺失起始数据会在历史回放中提前可见，研究信号与收益存在未来数据泄漏。
- 建议：移除 bfill，源值未可用时明确 not-ready；仅按当时已到达的数据 forward fill，限定有效期并验证 prefix invariance。
- 边界：较新的 `core/ai/research_loop_evaluation.py:165-183` 限制 source 为 OHLCV，clean_frame 严格拒绝无效数值，因此未把此问题泛化为新研究循环所有输入必定泄漏。

### DR-07 / P2：缺失的 DSL source 被静默替换为 close

- 文件/行号：`core/research/strategy_program.py:335-337`、`:366-375`。
- 触发：一般 DSL spec 指向缺少的数据列或变量拼错。
- 证据：spec source=flow，输入仅close=[100,100,100]，原函数直接输出close而非校验失败；未知 operand 也可退为0。
- 影响：生成的策略可以在没有所需数据时仍被回测/选中，实际所检验假设与提案不同；数据集变化静默改变策略语义。
- 建议：编译阶段检查所有source/operand依赖，缺失时拒绝程序或明确禁用条目，记录reason；不要用 unrelated source/0替代未知依赖。新OHLCV严格路径边界同DR-06。

### DR-08 / P2：回测成交胜负与净收益遗漏开仓手续费

- 文件/行号：`core/backtest/backtest_engine.py:553`、`:595`、`:724`、`:863-885`、`:899-913`。
- 触发：有非零entry fee的交易，尤其毛利润介于单边/双边fee之间。
- 证据：开仓时现金已扣fee且open记录 `net_pnl=-fee`，close.net_pnl只扣exit fee。胜负、profit_factor、avg_trade、cost_breakdown.net_pnl仅用close/funding。离线原函数初资1000、仓位10%、无slip、双边fee .1%，100买入100.15平：实际portfolio_pnl=-0.05015，但reported_net_pnl=+0.04985且winning_trades=1；fee总数0.20015。
- 影响：胜率、profit factor、每笔净收益和成本分解与权益不一致，亏损策略可能被标盈利；现金总收益本身已扣费。
- 建议：position保留entry fee，round-trip统计纳入entry+exit fee；组合realized sum纳入开仓fee并与权益调节表对账，避免再从现金重复扣一次。

### DR-09 / P2：PerformanceAnalyzer 把所有采样点当日线，Calmar单位不一致

- 文件/行号：`core/backtest/performance_analyzer.py:69-90`、`:138-158`、`:173-200`；`:122-125` trading_days。
- 触发：小时/分钟回测经 PerformanceAnalyzer 报告；或任意非零回撤。
- 证据：periods=len(equity) 后 years=periods/365；volatility/Sharpe/Sortino固定sqrt365。365小时上涨10%，报告年化10%，按365天小时频率应约884.97%（示例展示频率错误，不代表可实现预测）；该年度分数.1除 result.max_drawdown_pct=5 得Calmar .02，应除.05得2。
- 影响：跨timeframe性能不可比，报告可能和BacktestEngine自身基于index的Sharpe不同；风险调整指标和交易日数失真。
- 建议：把权益时间戳带入结果/分析器，按实际elapsed time、有效采样间隔或先规范日频计算；所有收益/回撤使用fraction，在展示层乘100。

### DR-10 / P2：行情数值边界允许正无穷成为 ok 报价

- 文件/行号：`core/marketdata/hub.py:45-50`、`:338-361`；`core/marketdata/runtime_price_provider.py:43-44`、`:76-88`。
- 触发：上游price/last="inf"或数值overflow到inf。
- 证据：float成功且inf>0，没有math.isfinite；`fail_closed=True` 的无网络离线读取得 `price=inf, ok=True`。
- 影响：被信任的数据可污染纸盘估值、止损与减仓参考；执行子审计确认动态sizing往往得到0或普通开仓risk cap挡inf，因此没有宣称必定发送真实订单；allow_close等豁免路径需要额外保证。
- 建议：入Hub与执行入口都验证finite，bid/ask/mark/volume/funding及时间戳同样规范；拒绝计数与source quality可观测。

### DR-11 / P2：Polymarket纸盘成交分三次提交，重试可重复扣款/加仓

- 文件/行号：`prediction_markets/polymarket/paper_trading.py:105-148`；`prediction_markets/polymarket/db.py:268-279`、`:840`、`:888-950`。
- 触发：fill和account已经commit，而order update失败/进程中断；或者两个任务同时读取同一OPEN订单。
- 证据：record_paper_fill→apply_paper_fill_to_account→update_paper_order各自独立session commit；fill使用随机ID而非剩余成交幂等键。Mock在第三步第一次抛错后重试：订单filled_size100，账户position_size200，fill_count2，cash900。
- 影响：纸盘账户、订单、fills永久不一致；重启和重试失去幂等性，后续研究收益/风控受污染。项目README与clob_trader确认实盘Polymarket未实现，本问题限定纸盘。
- 建议：事务内锁定订单并原子提交fill/position/account/order，按remaining数量CAS更新；稳定fill幂等键、防重复apply；启动恢复对账和崩溃注入验收。

### DR-12 / P2：Polymarket纸盘仓位上限不预留同token待成交BUY

- 文件/行号：`prediction_markets/polymarket/paper_trading.py:221-246`，关键`:231-239`。
- 触发：相同token在先前BUY还OPEN时再下BUY，资金足够。
- 证据：读取open_orders但position_notional只取已有持仓+本单，忽略已有BUY剩余量；max_order50/max_position60时两个notional50的订单均被接受，待买总额100。
- 影响：多单成交后超仓位上限；纸盘风控报告与设定不符。
- 建议：风险事务包括current exposure+pending BUY remaining reservations；回收取消/部分成交预留，账户/订单并发控制，处理open_orders分页上限。

### DR-13 / P2：Polymarket纸盘使用任何年龄的 latest quote 成交

- 文件/行号：`prediction_markets/polymarket/paper_trading.py:109-115`、`:286-302`；`prediction_markets/polymarket/paper_strategy.py` latest quote消费路径。
- 触发：上游停更、服务刚重启或数据库只有历史报价。
- 证据：获取latest quote后没有source/ts/max_age检查。Mock2020-01-01报价可将当前新单直接填成FILLED。
- 影响：运行中的纸盘可根据多年旧报价制造可成交收益，与当前市场无关；未知quote来源也被接收。
- 建议：实时纸盘明确最大source age、market active/close条件和token/source一致性；历史replay必须显式传sim_time，让历史成交与实时成交契约分离。

### DR-14 / P2：Gamma市场级fallback复制同一报价到YES与NO，并伪造当前quote时间

- 文件/行号：`prediction_markets/polymarket/worker.py:66-107`、`:194-209`；`prediction_markets/polymarket/market_resolver.py:120-129`、`:218-225`。
- 触发：CLOB拉不到报价，从存储市场snapshot fallback，市场已订阅YES/NO各自token。
- 证据：fallback读取market级bestBid/bestAsk/lastTradePrice，完全不按token/outcome映射；仅复制subscription身份。Mock同一market bid.79/ask.81：YES=.8、NO=.8，原market.updatedAt为2020而quote.ts/fetched_at为当前；并把clob_rest状态mark_success。resolver已经保留outcomePrices和各token映射，但此函数不使用。
- 影响：至少无法保证token价正确；二元互斥token被标同价，旧snapshot掩饰成新报价，纸盘选择/成交与市场来源失配。
- 建议：仅使用经验证的token对应outcome snapshot字段/正确orderbook，保留源snapshot时间；缺乏token级bid/ask时标degraded且禁用实时成交，不应mark真实CLOB成功。

### DR-15 / P2：CUSUM少于min_bars时反而允许触发，且纸盘降级转换不被状态机接受

- 文件/行号：`core/monitoring/strategy_monitor.py:122-128`；`core/monitoring/cusum_watcher.py:64-68`、`:170-182`；`core/deployment/promotion_engine.py:32`、`:40`。
- 触发：样本2—19笔且持续负收益；或任何paper_running候选触发衰减。
- 证据：`reliable_from = min_bars if n >= min_bars else 0` 在未warmup时从第0bar启用判定；默认h/k、12个-.1收益、min_bars20复现triggered=True。watcher无额外样本门槛，直接消费此结果。watcher目标paper→shadow，而proposal/candidate transition map均不包含该边，必然抛ValueError后强设retired。
- 影响：样本未够即可停止/退役候选；预期先降为shadow观察的阶段被跳过；强赋值retired也绕过规范lifecycle记录。live_running不在_demote实际分支，未宣称live自动停单。
- 建议：n<min_bars强制warming_up/triggered=False；明确20笔时是否第19索引可触发，统一stateful与batch；状态转换表与watcher规则一致，失败应有记录和重试而非无审计强赋状态。分别验证短历史、阈值与paper→shadow→retired。

## 待验证风险（不计入确认问题）

1. 研究历史快照：`core/research/strategy_research.py:185-198`、`:642-779`、`:1254-1259`、`:1351` 将当前macro/premium/cross-sectional snapshot作为constant_features回填全部历史index，缺少available_at/as-of。如果生成的legacy程序引用这些特征，会有点时错误；本次没有验证实际生成程序是否使用这些source，新严格OHLCV循环不受同样入口影响。需要带vintage的模拟提案和historical datasets验证。
2. premium读取：glassnode/cryptoquant/nansen/kaiko单文件快照及loader缺乏统一max_age/source可用性约束，更新失败后旧值可能继续标available；不同asset覆盖同metric文件的业务使用需要配置验证。本次未读取真实cache或联网。
3. `core/research/experiment_registry.py` JSON registry 的进程内/实例锁与缓存不保证多实例/多worker读改写一致；atomic replace只保证文件完整。需要确认实际部署worker数、共享目录与写入者，在隔离数据目录做并发丢更新验收。
4. `core/factors_ts/cache.py` `_store_status` 未见与有限 `_store` 同步淘汰，长期大量dataset/params可能增长；threading.local scope与同线程异步backtest任务隔离需负载验证。
5. `core/ai/signal_aggregator.py` risk_gate调用只传atr，未传gate支持的spread/volatility；异常分支放行。是否另有执行层严格约束必须按实际strategy配置验证；已撤销“risk_gate根本没有spread/vol检查”的候选，因为`core/ai/risk_gate.py:85-99`明确存在。
6. `core/research/strategy_research.py` Sharpe annualization cap与真实bar频率/交易周期不统一；`performance_analyzer.py` funding-stage与close.pnl在部分trade/monthly统计中可能重复，需要单独注入funding后把每条现金流与报告对账。本报告DR-08只确认entry fee遗漏。
7. 通知发送失败后的cooldown和altcoin条件边缘状态仍会推进（notification_manager.py:942-953），服务恢复后的重试/补发语义及通知敏感异常信息脱敏需要离线故障测试；没有向任何第三方发送消息。
8. 秒级回填、外部premium API、交易所/Polymarket schema及ws恢复，真实缓存corruption和长周期重启恢复只能做结构审查；未联网、未触达账户。性能峰值、OS跨进程Parquet锁、SQLite多writer、长期DB增长需专门压力环境。

## 覆盖与验证记录

| 范围 | Python文件 | 重点人工审查/已核验边界 | 动态证据 |
|---|---:|---|---|
| core/ai | 25 | autonomous入口/决策链、live router、signal aggregation/risk gate、ML、research_loop验证及provider policy | DR-02 |
| core/ml | 2 | 特征/标签、单/多symbol split、模型指标/manifest/schema | DR-04 |
| core/research | 39 | DSL/研究执行/enrichment、候选/实验状态、XS vintages/as-of、validation/radar排序与事件 | DR-06/07；XS模块已有point-in-time/vintage逻辑 |
| core/backtest | 10 | cash/position/fee/funding/exit、execution arrays、cost model、报告统计、paper accounting | DR-08/09 |
| core/data | 35 | storage/parquet锁与修复、历史补齐、archive/premium/macro/OI/funding/orderbook输入和错误降级 | 结构审查；未写入真实cache |
| core/marketdata | 7 | Hub元数据、ws/REST选择、quality guards、runtime provider及execution关联 | DR-01/10 |
| core/realtime | 2 | event_bus订阅、事件调度、异常/队列边界（交易语义见执行子审计） | 结构审查 |
| core/news | 31 | 所有collector导入与模式、manager/common、去重quality、eventizer限流/LLM、raw/events DB与worker交付顺序 | DR-03；worker无HTTP监听，Docker健康检查错配由主审计另记 |
| core/monitoring | 4 | watchdog、批量/stateful CUSUM、自动demotion/反馈/替代候选 | DR-15 |
| core/observability | 4 | gate codes、decision traces、score calibration/priors持久化与输入边界 | 结构审查 |
| prediction_markets | 16 | client→resolver→worker→paper→DB、quote provenance/replay/profile、README声明live未实现 | DR-11/12/13/14 |
| core/factors_ts | 10 | 因子registry与cache、base/impl、扩展及OI/funding/sentiment/orderbook依赖 | DR-05 |
| core/indicators | 4 | rolling RSI/ATR、oscillators、两种ADX组件与warmup | 结构/函数审查，未确认新增缺陷 |
| core/notifications | 2 | 持久rule、各渠道请求/SMTP配置、事件/cooldown、雷达条件edge状态 | 静态审查，未发送通知 |
| core/deployment | 2 | promotion eligibility、paper/live_candidate界线、runtime重复实例、lifecycle转换 | DR-15关联 |

已确认正向保护：新研究循环限制OHLCV且检查清洁数据；pooled ML有label horizon time purge；Polymarket实盘trader明确NotImplemented且README定位离线纸盘；DSL解释规则使用受控类型/操作映射（未发现通过程序文本exec任意代码）；ADX复用原始up/down movement避免覆盖错误；BacktestEngine自身Sharpe使用权益index采样间隔。本次静态源码解析193/193成功，offline原函数复现完成，完整测试由主审计结果汇总。

边界声明：全量覆盖清单完成代表所有分配第一方文件已被登记与静态检查；75k行代码无法仅靠这些离线复现证明全部正确。没有进行真实网络schema/来源故障、账户、实盘成交、全历史结果复算、长期并发/崩溃/性能压力验证。本报告明确保留这些范围为未动态验证，没有以测试通过替代安全性证明。
