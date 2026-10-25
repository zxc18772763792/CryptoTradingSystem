# Alastar 文字1小时v2：激活与统一跟踪器登记

2026-10-22。用户明确要求“激活吧，注册到跟踪器”。已完成本地模拟激活、统一表登记、持续任务接入、受管服务重载和真实运行核验。策略仍待验证，当前新增观察／冻结／持仓／完成交易均为0；422条历史没有回填。

## 实际状态与证据

- 激活UTC：`2026-10-22T05:09:53.285184+00:00`，北京时间13:09:53。账本唯一记录为`data/research/alastar_paper_v2/journal/000000000001.json`；完整副本在本目录`activation_receipt.json`。
- 统一跟踪器ID：`alastar_fresh_call_1h_v2`；显示名“Alastar 首次喊单 · 1小时（v2）”；策略页研究跟踪器表可读取。已通过真实`/api/ai/research-trackers`接口核验，总共17行，Alastar行无读取错误、`runner_active=true`、`n=0`、`min_n=66`，空收益／区间保持null。
- 真实`/api/ai/alastar-paper`接口返回运行汇总、观察和标签回执；`/api/status`核验`trading_mode=paper`。认证只在进程内复用本地OPS令牌，回执没有保存令牌或账户状态。证据在`runtime_verification.json`。
- 受管网页新PID=18168；新闻采集PID=60368、新闻LLM PID=53536前后保持不变。使用`web.bat stop`（仅网页）及`web.bat start -PaperMode`，没有手工启动uvicorn。
- 连续两次实际心跳为`05:14:52.345641Z`及`05:14:57.362790Z`，间隔约5.017秒，PID一致，无本轮错误；激活协议和三个实现来源SHA仍匹配。见`heartbeat_verification.json`。最终05:15:35核验仍在运行，账本仍只有激活记录，见`final_readiness.json`。

## 激活门槛与实时性

激活时钟检查保留5次公共时间响应，采用最短往返的一次；推断偏移约+0.2029秒，区间`[+0.1595,+0.2464]`秒，往返0.0869秒，原±2秒／10秒门槛未变。保留用户先前添加的多次采样修正，没有修改操作系统时间。

05:15:35最终归档核验：690轮（689轮live），最近三次间隔94.224、88.405、76.835秒；最新轮距核验68.58秒。最近6小时223轮，间隔中位93.59秒、p90为127.39秒、最大206.31秒，超过600秒的间隔为0，最近四轮健康门槛通过。真正新帖首次收件和及时首次收件仍为0，不能据此估计消息传输延迟分布。

5秒是模拟任务每轮完成后的等待配置，新闻源公开预览采集仍是另一条链路。它不承诺5秒收到Telegram，也不保证捕获开盘。服务负载、网络查询和历史结算可能使入场错过30秒窗口；错过时只追加跳过，不重建历史成交。公开预览历史断流证据和原始归档都保留。

独立官方零分布审计及校验回执仍对应相同SHA，原协议的7项不可变引用一致；`receipt_latency.json`继续作为会变化的投影保存新快照，旧协议SHA没有静默改写。这里只是运行激活，没有新增回测结论。

## 新增运行路径

`core/research/alastar_paper_tracker.py`提供独立短周期任务，由现有网页任务监督器管理。它先跟踪激活后发布、实时及时收件的候选消息，再读取复核标签，冻结后按未来计划开盘捕获。它不会按关键词自动猜方向，不会调用AI、私有账户或下单接口。

语义解释仍需人工／本会话完成。可用CLI冻结，或把合格JSON放入`data/research/alastar_paper_v2/labels_inbox/`。每轮最多处理10份不同内容，以SHA去重并先保存原始输入；接受与剔除都通过同一冻结校验器。未解释的新帖显示待复核，超过可入场时限记录未解释超时；历史数据、晚到及编辑后消息不能追认未来样本。任务没有自动解释器，因此仅注册并不代表已经完成无人值守信号生成。

有计划入场时重新检查公共时钟，失败不准新入场；已开模拟仓位仍可用公共历史资料结算。历史结算／缺失对照重试间隔至少60秒。手动单次执行不冒充持续任务；成功心跳过旧或本轮错误时不显示健康运行。文件锁防止多个任务同时拥有同一账本。

统一表主指标为同币前30天、同向、20bps价格收益零分布的超额，单位百分点／笔。实际含资金费收益另列，不能当成同一个分子。缺失对照的已完成交易不混进主检验n，也不隐藏已经实现的盈亏。40笔无效停止、66笔集中复核、最多24个月沿用冻结v2；未套用其他跟踪器通用90%区间判定，没有编造回测参照均值。

核心登记器、公共数据CLI、跟踪适配器三个源码SHA及原协议已固定在激活记录内。后续修改必须审查并做独立版本迁移，不能删除账本、篡改旧激活或重置亏损。原v1／v2 JSON保持原样；v2文件中的`documented_not_running`是历史文档字段，当前运行状态由实际激活日志和新鲜心跳决定。

## 验证与尝试记录

1. 相关Python回归211项通过（42.10秒），覆盖登记器、官方证据、时钟、收件归档、同币研究、统一注册／口径及网页任务生命周期。
2. 通用跟踪器停止规则另14项通过（4.26秒）；前端5项通过，覆盖超额百分点与实际收益分离、待复核／缺失对照及字符串转义。
3. 最后修正错误心跳不可冒充运行后，相关108项再次通过（16.45秒）；此为重复回归，不追加成新的独立测试数量。
4. 项目解释器环境核验全部必需及可选依赖齐全；`git diff --check`通过，仅有既有Git行尾提示。
5. 首次提炼激活输出时误从payload读取位于外层的协议SHA，输出处理报`KeyError`；激活在此之前已经成功落盘。随后直接核验唯一激活记录及来源SHA，未再次激活、未重建账本。这不影响已保存完整回执。
6. 多文件文档补丁首次因交接文件标题不匹配被原子拒绝；核实标题后重做成功，没有覆盖用户原始研究报告。

新增边界测试包括：空闲任务不发网络请求、未激活不自行启动、待解释超时不回填、标签重复去重不阻塞后续文件、错误JSON存证、成功／失败时钟分支、结算失败重试节流、实收收益与超额分离，以及手动／过期／报错心跳不能宣称健康运行。

复现命令（在仓库根目录）：

```powershell
$py = 'F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe'
& $py scripts/verify_env.py
& $py -m pytest tests/test_alastar_forward_paper.py tests/test_alastar_paper_evidence.py tests/test_alastar_observation.py tests/test_alastar_selection_timing.py tests/test_telegram_signal_review.py tests/test_tracker_registry.py tests/test_tracker_measurement.py tests/test_web_main_runtime_tasks.py -q
& $py -m pytest tests/test_tracker_retirement.py -q
node --test tests/research_tracker_diagnostics.test.cjs
& $py -m scripts.observe_alastar_signals --latency
& $py -m scripts.observe_alastar_paper status
```

激活命令只在用户授权时执行过一次，不列为日常重复命令。后续通过[当前操作说明](../../docs/ALASTAR_PAPER_V2.md)继续。没有启动三天旧方案、图片方案、每六小时自动续研或实盘；没有提交／推送代码。
