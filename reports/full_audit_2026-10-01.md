# F:\9_Crypto 全量审计报告 · 2026-10-01

本次确认 **41项代码/工程问题：P0 0项、P1 15项、P2 25项、P3 1项**。其中WEB-08是代码中已确认的条件启用权限缺口，当前实例是否开启未读取。资金与控制边界有多项高优先缺陷，不能仅凭测试多数通过认定实盘安全。报告没有声称真实损失、账户利用或生产订单已发生。

交付为本报告、[全量覆盖清单](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_coverage.md)及[逐文件机器清单](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/inventory.json)。所有新增产物位于带日期的reports路径；**没有自动修复业务代码**。适用AGENTS、README、安全/治理与启动文档、Git状态均已重新读取/核对。真实密钥文件、交易账户、原始研究数据与生产参数未读取或修改；审计未启动服务、worker或发送真实订单。

## 审计基线与工程边界

| 对象 | 当前证据 | 本次处理 |
|---|---|---|
| 主工程 | `F:\9_Crypto\crypto_trading_system`，HEAD `2fecd05dea8c5ee438b4071e50548b828b7411af`，分支`codex/operating-reality-bug-sweep` | 1239个Git跟踪文件；874个Python文件共300242行；运行非LIVE隔离测试 |
| `_radar_refactor` | HEAD `8a627bf597e1f67c37a15aedfbeb70bce60b9be3`，分支`refactor/radar-modularize` | Git已注册的关联工作树；984文件/774 Python/281583行；AST全量检查，独有7文件补丁深读，不重复旧测试 |
| 工作区父目录 | 独立Git根；仅跟踪`.gitignore`、主工程gitlink、`docs/PROJECT_AGENT_HANDOFF_2026-06-08.md` | 交接文档与边界审阅；不是第二套业务系统 |
| `.conda`、编辑器元数据 | 外部运行环境与`.claude/.vscode`配置 | 环境依赖验证，排除第三方包逐行审计 |
| ignored源码辅助 | 12个`.py/.cjs/.ps1`，位于data、logs、tmp | 只读源码/副作用审阅，逐项单列，不执行历史维护辅助 |
| ignored数据/账户状态 | 原始行情、研究快照、运行日志、秘密配置 | 不覆盖、不复算、不读取真实账户状态；其业务来源和消费代码在范围内 |

`git worktree list --porcelain`证明`_radar_refactor`为活动关联工作树，与AGENTS中的旧描述不一致，以当前Git状态为准。主工程和工作树有194个第一方路径差异（config3、core65、scripts43、tests60、web23），但大多数为主工程更近的演进；工作树独有1个提交，7文件补丁为+373/-3。补丁全文审阅未确认新增安全边界缺陷；旧journal全文件读取及前端版本更新属于待验证性能/发布风险，不能推导工作树整体无缺陷。证据见[radar_inventory_summary.json](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/radar_inventory_summary.json)、[radar_diff_paths.txt](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/radar_diff_paths.txt)及本报告Web附录。主工程开始时干净；最终只新增日期审计文件，工作树无修改。

## 优先处理顺序

1. 先收敛所有实盘入口授权（WEB-01/02/08），把反手关闭与新开风险检查拆开（T1），隔离真实账户订单身份（T3）和sandbox路由（T6）。这些边界应先在模拟交易所验证拒绝矩阵，再评估上线。
2. 修复聚合持仓归因、合约单位、每次成交PNL以及熔断独立状态（T2/4/5、WEB-03/04）；对账必须覆盖部分成交、重启恢复和多策略共享账户。
3. 修复源行情时效、AI schema和新闻可靠入库（DR-01/02/03），再修回测/模型泄漏、纸盘原子性及其他P2。修复后运行确定性反例，不应仅重跑现有绿灯测试。

上述是代码证据的优先级，不推断当前账户或服务的实际模式。P0代表立即且广泛的灾难性风险，本次没有足够证据归为P0；P1为资金/关键权限或常规数据完整性高影响；P2为有条件正确性、纸盘/研究/运行可靠性；P3为低影响或离线组件问题。没有在真实账户验证不意味着缺陷可忽略。

## 确认问题索引

下表去重后的41项均在后文给出触发、影响、证据和修复建议。跟踪代码行号对应上述HEAD；ignored脚本对应本次只读快照SHA。并列根因合并计数（DR-09频率/单位、DR-15预热/降级），不同DSL缺源与未来填充分列。A3未找到生产调用，按P3计算。待验证风险不进入计数。

| ID | 级别 | 问题 | 准确定位 | 触发及影响 |
|---|---|---|---|---|
| T1 | P1 | 反手新开敞口被整单平仓豁免风控 | [core/trading/execution_engine.py](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:5805) | 反向非reduce-only量超过旧仓；可越过组合敞口/杠杆检查 |
| T2 | P1 | 聚合持仓复制到每个策略 | [core/trading/execution_engine.py](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:3774) | 同账户同标的同侧多策略；本地持仓重复放大 |
| T3 | P1 | 订单ID跨账户覆盖 | [core/trading/order_manager.py](F:/9_Crypto/crypto_trading_system/core/trading/order_manager.py:802) | 两账户返回相同裸ID；取消/查询发往错误账户 |
| T4 | P1 | 基币量与合约张数混用 | [core/exchanges/okx_connector.py](F:/9_Crypto/crypto_trading_system/core/exchanges/okx_connector.py:232) | 衍生品contractSize非1；订单与对账数量错倍率 |
| T5 | P1 | 部分平仓后累计PNL重复入账 | [core/trading/execution_engine.py](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:5505) | 部分平仓后专用CLOSE；统计和日内风控错误 |
| T6 | P1 | Binance fast REST丢失sandbox | [core/trading/binance_rest.py](F:/9_Crypto/crypto_trading_system/core/trading/binance_rest.py:152) | sandbox账户走fast REST；请求指向生产域名 |
| T7 | P2 | 算法单把零成交子单计为已成交 | [core/trading/execution_engine.py](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:6311) | OPEN/filled=0；汇总filled与closed状态错误 |
| T8 | P2 | 零成交仍标记部分止盈完成 | [core/trading/execution_engine.py](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:2380) | 减仓单接受但未成交；止盈保护被提前消费 |
| A3 | P3 | 离线PNL分解遗漏开仓成本 | [core/accounting/pnl_decomposer.py](F:/9_Crypto/crypto_trading_system/core/accounting/pnl_decomposer.py:249) | 离线lot消费；净收益偏高，未找到生产调用 |
| WEB-01 | P1 | 策略实盘入口缺approve_live | [web/api/strategies.py](F:/9_Crypto/crypto_trading_system/web/api/strategies.py:2815) | 有manage_strategies但无实盘批准角色；可设置并启动live |
| WEB-02 | P1 | 只读角色可解除熔断 | [web/api/risk.py](F:/9_Crypto/crypto_trading_system/web/api/risk.py:63) | 合法AUDITOR/ENGINEER身份；无风险管理能力检查 |
| WEB-03 | P1 | 允许范围清空/替换不走增风险审批 | [core/governance/service.py](F:/9_Crypto/crypto_trading_system/core/governance/service.py:124) | 集合变化非严格超集；空集合实际变为不限制 |
| WEB-04 | P1 | 普通风险配置应用清除日内停机 | [core/governance/service.py](F:/9_Crypto/crypto_trading_system/core/governance/service.py:234) | kill_switch=false配置应用；重置亏损基线并进入保护宽限 |
| WEB-05 | P1 | 策略摘要未转义持久化字段 | [web/static/js/app.js](F:/9_Crypto/crypto_trading_system/web/static/js/app.js:3371) | 可写名称/符号进入innerHTML；交易页面脚本执行风险 |
| WEB-06 | P2 | profile输出可覆盖仓库业务文件 | [core/ops/service/polymarket_routes.py](F:/9_Crypto/crypto_trading_system/core/ops/service/polymarket_routes.py:170) | 有研究管理能力并指定output_path；仅限仓库根不够 |
| WEB-07 | P2 | Ops新闻变更/审计读取能力缺口 | [core/ops/service/news_routes.py](F:/9_Crypto/crypto_trading_system/core/ops/service/news_routes.py:13) | 有效身份但无manage_news/read_audit；能到达敏感操作 |
| WEB-08 | P1 | 可选手工信号接口缺订单授权 | [core/ops/service/api.py](F:/9_Crypto/crypto_trading_system/core/ops/service/api.py:1166) | 仅OPS_ALLOW_MANUAL_SIGNAL开启时；当前启用状态未读取 |
| WEB-09 | P2 | 熔断解除审计签名错误被吞 | [web/api/risk.py](F:/9_Crypto/crypto_trading_system/web/api/risk.py:77) | 成功reset后的log调用；OperationAudit记录缺失 |
| WEB-10 | P2 | WS初始发送失败遗留订阅 | [web/main.py](F:/9_Crypto/crypto_trading_system/web/main.py:2766) | hello异常发生在try/finally之前；队列与fanout增长 |
| WEB-11 | P2 | 审计文件重写与追加竞态丢记录 | [core/audit/gate_counterfactuals.py](F:/9_Crypto/crypto_trading_system/core/audit/gate_counterfactuals.py:190) | 读快照后并发append再replace；新trace丢失 |
| WEB-12 | P2 | 未认证GET推进共享回放游标 | [web/api/data.py](F:/9_Crypto/crypto_trading_system/web/api/data.py:6949) | 已知回放ID；改变其他使用者回放状态 |
| DR-01 | P1 | 源旧行情仍通过新鲜度检查 | [core/marketdata/hub.py](F:/9_Crypto/crypto_trading_system/core/marketdata/hub.py:376) | 旧源时间戳重发；接收时间不断刷新ok |
| DR-02 | P1 | AI enforce无效action默认放行 | [core/ai/live_decision_router.py](F:/9_Crypto/crypto_trading_system/core/ai/live_decision_router.py:761) | 缺字段/未知动作；fail_open=false也允许 |
| DR-03 | P1 | 新闻先提交游标再截断和保存 | [core/news/collectors/manager.py](F:/9_Crypto/crypto_trading_system/core/news/collectors/manager.py:428) | 批量超过max_records或后续保存失败；永久漏数据 |
| DR-04 | P2 | 单标的ML标签切分无purge | [core/ml/pipeline.py](F:/9_Crypto/crypto_trading_system/core/ml/pipeline.py:401) | forward标签触及测试期；holdout泄漏 |
| DR-05 | P2 | 因子缓存忽略非close输入 | [core/factors_ts/cache.py](F:/9_Crypto/crypto_trading_system/core/factors_ts/cache.py:95) | high/low/volume等修订；复用错误因子结果 |
| DR-06 | P2 | DSL前导缺失bfill读未来 | [core/research/strategy_program.py](F:/9_Crypto/crypto_trading_system/core/research/strategy_program.py:334) | legacy/general source前导NaN；历史信号提前可见 |
| DR-07 | P2 | DSL缺少source静默换成close | [core/research/strategy_program.py](F:/9_Crypto/crypto_trading_system/core/research/strategy_program.py:335) | 列缺失/变量拼错；策略语义发生变化 |
| DR-08 | P2 | 回测每笔净收益漏entry fee | [core/backtest/backtest_engine.py](F:/9_Crypto/crypto_trading_system/core/backtest/backtest_engine.py:724) | 非零开仓fee；权益已亏但交易被标盈利 |
| DR-09 | P2 | 分析器频率/Calmar单位错误 | [core/backtest/performance_analyzer.py](F:/9_Crypto/crypto_trading_system/core/backtest/performance_analyzer.py:138) | 非日频或非零回撤；风险调整指标不可比 |
| DR-10 | P2 | 正无穷报价被认定有效 | [core/marketdata/runtime_price_provider.py](F:/9_Crypto/crypto_trading_system/core/marketdata/runtime_price_provider.py:43) | price=inf；缺finite检查，未证明必定真实下单 |
| DR-11 | P2 | 预测市场纸盘成交不原子 | [prediction_markets/polymarket/paper_trading.py](F:/9_Crypto/crypto_trading_system/prediction_markets/polymarket/paper_trading.py:105) | 第三步失败后重试/并发；重复加仓扣款 |
| DR-12 | P2 | 纸盘持仓上限不含待买预留 | [prediction_markets/polymarket/paper_trading.py](F:/9_Crypto/crypto_trading_system/prediction_markets/polymarket/paper_trading.py:231) | 同token多张OPEN BUY；成交后超限 |
| DR-13 | P2 | 纸盘按任意年龄报价成交 | [prediction_markets/polymarket/paper_trading.py](F:/9_Crypto/crypto_trading_system/prediction_markets/polymarket/paper_trading.py:109) | latest为多年旧价；仍FILLED |
| DR-14 | P2 | Gamma fallback未按outcome映射 | [prediction_markets/polymarket/worker.py](F:/9_Crypto/crypto_trading_system/prediction_markets/polymarket/worker.py:66) | CLOB失败fallback；YES/NO同价且旧快照被标新 |
| DR-15 | P2 | CUSUM预热与降级状态机不一致 | [core/monitoring/strategy_monitor.py](F:/9_Crypto/crypto_trading_system/core/monitoring/strategy_monitor.py:122) | 不足min_bars可触发；paper→shadow被强制改retired |
| OPS-01 | P2 | 启停脚本未隔离工程/端口进程 | [scripts/web.ps1](F:/9_Crypto/crypto_trading_system/scripts/web.ps1:196) | 主工程与工作树/多端口共存；停止或去重波及另一实例 |
| OPS-02 | P2 | 新闻worker继承Web健康探针 | [Dockerfile](F:/9_Crypto/crypto_trading_system/Dockerfile:71) | compose news_service运行纯worker；8000/livez不存在 |
| QA-01 | P2 | 离线测试依赖未跟踪数据与隐式token | [tests/test_pump_precursor.py](F:/9_Crypto/crypto_trading_system/tests/test_pump_precursor.py:88) | 干净克隆/清洁环境；4个数据依赖失败及1个认证前提失败 |
| IS1 | P2 | 辅助验收失败仍退出成功 | [data/_level1_relaxed_run.py](F:/9_Crypto/crypto_trading_system/data/_level1_relaxed_run.py:34) | evaluator非零或watcher无输出；父shell仍见exit0 |
| IS2 | P2 | HTTP错误被计为代理行情健康 | [data/_ws_proxy_monitor.py](F:/9_Crypto/crypto_trading_system/data/_ws_proxy_monitor.py:55) | futures time返回401/429/500但stream可达；计为ok |

## 验证结果与安全隔离

使用AGENTS指定解释器`F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe`，Python3.11.16。`scripts/verify_env.py --strict`通过，`python -m pip check`无损坏依赖。未安装或更新任何包。

| 检查 | 结果 | 范围/证据 |
|---|---|---|
| Python AST | 主工程874/874、工作树774/774成功 | [inventory.json](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/inventory.json)、[radar_inventory_summary.json](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/radar_inventory_summary.json) |
| PowerShell解析 | 21/21通过 | [powershell_syntax.json](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/powershell_syntax.json) |
| JavaScript/CJS语法 | 19/19 `node --check`通过 | [javascript_syntax.json](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/javascript_syntax.json) |
| YAML | 10/10 safe_load通过 | [yaml_syntax.json](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/yaml_syntax.json) |
| 跟踪JSON | 130个，错误0 | [json_syntax.json](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/json_syntax.json)；只验语法，不等于业务schema验证 |
| 配置契约 | `scripts/check_config_contract.ps1`通过 | 对`.env.example`与源码检查，不读取真实env |
| Git空白/冲突检查 | `git diff --check`通过 | 没有业务代码改动 |
| 定向危险原语/密钥模式 | 跟踪UTF-8文本扫描无命中 | 非穷尽语义安全/CVE审计，不读取秘密文件 |
| 交易边界复现 | 8项关键断言均通过；另复现A3开仓成本漏计 | [trading_repro_root_verified.json](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/trading_repro_root_verified.json) |
| 数据/研究复现 | 13组离线原函数观测成功，覆盖15项问题；关键断言root复核通过 | [data_research_root_verified.json](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/data_research_root_verified.json) |
| Web/治理/运维复现 | mock权限、路径、队列、审计并发与进程选择证据成立 | [web_security_repro_results.json](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/web_security_repro_results.json)、[web_security_xss_results.json](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/web_security_xss_results.json)、[ops_process_repro.json](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/ops_process_repro.json) |

测试副作用先经源码审查；完整pytest在临时目录的**HEAD git archive快照**运行，未导入真实env。子进程只继承基础OS环境，显式paper和worker关闭；sitecustomize guard阻止打开原主工程/工作树路径、阻止外部DNS与连接，仅允许测试自行拥有的loopback服务和已审阅的Python/PowerShell子进程。文件/数据库写入发生在临时快照或pytest临时目录；账户、交易所和敏感操作均为模拟。快照目录为`C:\Users\zxc\AppData\Local\Temp\crypto_full_audit_20261001_01a0f2da\source`。

完整命令`pytest -q -m "not live" --basetemp=隔离临时目录 --junitxml=隔离临时目录/pytest_results.xml`：**2593通过、2跳过、9失败**，342.42秒。随后对6个疑点定向复测：**5通过、1失败**，14.96秒。完整套件没有宣称全绿，也未把两次计数简单相加。

| 原9个失败分类 | 数量 | 核对后结论 |
|---|---:|---|
| 审计guard对子进程事件参数误判 | 4 | Windows subprocess executable可能为None；修正审计guard从args识别后，乱码修复dry-run及3个PS Unicode模型测试均通过；不是项目业务缺陷 |
| 未显式配置测试OPS_TOKEN | 1 | 无token时保护返回503，原测试期望401；给隔离环境提供合成dummy token后通过，不是未认证放行 |
| 未跟踪研究权重 | 2 | pump precursor、research loop直接依赖`data/research/pump_watchlist/model_weights.json`；隔离干净快照缺失 |
| 未跟踪ambush manifest | 1 | holder universe测试依赖`data/research/ambush_modes/manifest.json`，期望基础universe>50 |
| 未跟踪历史BTC 1h行情 | 1 | exit_logic验证脚本缺`data/historical/binance/BTC_USDT/1h.parquet`，独立复测仍失败；无法验证历史退出占比门槛 |

证据：[pytest_full.log](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/pytest_full.log)、[pytest_results.xml](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/pytest_results.xml)、[pytest_focused.log](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/pytest_focused.log)、[pytest_focused_results.xml](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/pytest_focused_results.xml)、[test_run_summary.json](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/test_run_summary.json)、[focused_test_run_summary.json](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/focused_test_run_summary.json)、[test_isolation_guard.py](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/test_isolation_guard.py)。4个数据依赖属于QA-01的工程可重复性问题，未判断现有真实本地数据缺失或其研究结论错误。没有复制/覆盖原始研究数据来让测试绿灯。初次尝试coverage参数时因pytest_cov不存在而在执行前退出4，随后移除参数；不算上述9失败。pytest_cov/coverage/mypy/ruff不在项目环境，未安装，**本次没有行/分支覆盖率百分比、类型检查或lint全绿结论**。出现一项Starlette/httpx弃用警告；不把第三方兼容警告当项目回归。

## 主审计补充问题

### OPS-01 / P2：进程启停与去重没有工程/端口所有权边界

- 位置：[scripts/web.ps1](F:/9_Crypto/crypto_trading_system/scripts/web.ps1:196)、175-193、573-638；[scripts/supervise_web.ps1](F:/9_Crypto/crypto_trading_system/scripts/supervise_web.ps1:66)、174-183；[_once.ps1](F:/9_Crypto/crypto_trading_system/_once.ps1:48)、48-70。
- 触发：同机同时运行主工程与`_radar_refactor`，或两端口Web/同module的worker。Get-ManagedWebProcesses按模块/命令特征匹配，不约束当前repo和目标port；Get-ObservedWorkerProcesses与supervisor worker去重同样只按module。
- 影响：对8000实例执行stop可能停止8001实例；supervisor可能把另一工程worker判为重复并强杀；启动可因发现另一工程worker而错误跳过。属于可造成停机/任务中断的运行维护缺陷，未声称数据损坏已发生。
- 证据：AST提取PowerShell函数并mock全部OS进程操作，请求port8000仍选中8000和8001；同worker module选中main与radar；重复清理停掉模拟radar PID12002。[ops_process_repro.ps1](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/ops_process_repro.ps1)与[ops_process_repro.json](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/ops_process_repro.json)，未枚举或终止真实进程。
- 建议：启动记录instance/project/port、PID和start_time，操作前核对完整解释器、命令和真实工程身份；worker同样采用实例注册，不以module名全局去重。多工作树/多端口用mock矩阵验证隔离。

### OPS-02 / P2：news_service沿用不存在的Web健康端点

- 位置：[Dockerfile](F:/9_Crypto/crypto_trading_system/Dockerfile:71)-72；[docker-compose.yml](F:/9_Crypto/crypto_trading_system/docker-compose.yml:47)-65；[core/news/service/worker.py](F:/9_Crypto/crypto_trading_system/core/news/service/worker.py:730)-748、760-763。
- 触发：启用compose的news_service profile，其command仅运行`core.news.service.worker`，继承同镜像`curl localhost:8000/livez` HEALTHCHECK，service未覆盖。
- 影响：worker正常工作也会持续unhealthy，误导监控与部署门禁。Docker restart:unless-stopped并不会仅因unhealthy自动重启，本报告不声称必然重启循环。
- 证据：compose、Dockerfile及worker启动全文结构核对；worker只连接新闻DB并执行worker_loop，没有HTTP8000监听。未build/启动容器。
- 建议：为worker独立探测持久心跳/最后成功拉取/处理延迟，或在compose显式覆盖适用健康检查；分别验收web与worker容器状态。

### QA-01 / P2：所谓离线测试依赖本机研究产物与隐式认证环境

- 位置：[tests/test_pump_precursor.py](F:/9_Crypto/crypto_trading_system/tests/test_pump_precursor.py:88)、[tests/test_research_loop_v2.py](F:/9_Crypto/crypto_trading_system/tests/test_research_loop_v2.py:98)、[tests/test_pump_watchlist_resilience.py](F:/9_Crypto/crypto_trading_system/tests/test_pump_watchlist_resilience.py:45)、[tests/test_exit_logic_overhaul_verification.py](F:/9_Crypto/crypto_trading_system/tests/test_exit_logic_overhaul_verification.py:4)、[scripts/verify_exit_logic_overhaul.py](F:/9_Crypto/crypto_trading_system/scripts/verify_exit_logic_overhaul.py:190)、[tests/test_sensitive_api_auth.py](F:/9_Crypto/crypto_trading_system/tests/test_sensitive_api_auth.py:565)。research_loop相关权重加载堆栈见完整测试日志。
- 触发：干净克隆/CI、没有ignored研究权重/manifest/真实历史行情；或没有OPS_TOKEN。即使环境所有依赖齐全，4个数据案例仍不能独立执行，另1认证案例依赖未声明环境前提。
- 影响：离线验收无法从代码版本重现，失败混淆算法回归和缺数据；历史阈值测试无法提供稳定门禁。
- 证据：HEAD archive无真实env/ignored数据，完整9失败经隔离guard及dummy token定向排除后剩4个数据依赖；exit_logic单独复测仍失败。证据见验证结果表与日志。
- 建议：确定性合成小fixture、mock权重/manifest/行情loader、每个认证案例显式dummy token；必须依赖历史产物的测试标成清楚的integration并提供受控版本fixture/来源，不放宽生产认证或提交秘密数据。缺数据应明确缺前提，不能生成带PASS措辞的研究结论。

## 架构、覆盖与未验证边界

架构资金主链为策略/AI信号→执行引擎→风险/治理→OrderManager→交易所，账户/模式metadata在多个层级解析；持仓归因和单位语义是本次主要边界缺陷。数据主链为WS/REST→Hub→runtime provider→执行与估值，源时间与接收活性目前混用。研究链为DSL/因子/训练→回测指标→候选验证/晋级；缓存、标签未来区间与交易成本必须统一。新闻和预测市场链的多步持久化缺乏统一可靠交付/事务边界。大型`web/api/trading.py`9881行、`web/api/data.py`7358行、`execution_engine.py`6904行增加跨层语义漂移风险；模块尺寸本身未记为额外缺陷。

全量覆盖指所有已识别第一方文件完成登记、适用语法/模式/依赖结构检查，关键链人工深读并复现；**不等于300242行均逐行深审，也不等于所有函数动态验证**。独立覆盖清单把S（静态）、D（人工重点链）、V（隔离验证）区分；现有测试提供广泛行为证据，但不是实际覆盖率。文档/历史报告/model manifests被登记并核对当前入口和使用边界，未复算历史收益、重训全部模型或断言其统计有效。

以下范围保持待验证，不计入41项：真实交易所/Polymarket schema与订单部分成交/撤单；实盘重启恢复及跨账户完整对账；数据库migration/多worker审批事务及Parquet跨进程锁压力；真实源故障/限流/断连恢复；长期journal/DB/队列/缓存增长与延迟基准；历史研究原始数据、premium时效、模型外推与walk-forward全量复算；浏览器真实DOM/CSP与完整页面端到端；Docker build/容器运行/真实部署健康；外部依赖CVE和供应链来源审计。只有语法与依赖一致性检查，不能称依赖无已知漏洞。Playwright smoke依赖`@playwright/test`及运行Web，当前无项目node_modules且本次不启动服务，未运行；bundled Node用于语法和离线DOM mock。其余风险按模块附录的具体位置记录，避免把推测升级为确认缺陷。

## 逐项详细证据附录

下列直接并入独立模块报告的确认问题和待验证风险，完整触发/建议以对应条目为准。分报告原件也保留，方便单独审阅。


## 附录：交易、执行与风控

原件：[trading.md](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/trading.md)。

## 确认问题

### T1 · P1 · 反向非 reduce-only 手动订单把反手开仓部分也作为平仓豁免风控

定位：[core/trading/execution_engine.py:5805](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:5805)、[core/trading/execution_engine.py:5840](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:5840)、[core/trading/execution_engine.py:5855](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:5855)、[core/trading/execution_engine.py:5871](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:5871)；被绕过的检查在 [core/risk/risk_manager.py:910](F:/9_Crypto/crypto_trading_system/core/risk/risk_manager.py:910)、[core/risk/risk_manager.py:928](F:/9_Crypto/crypto_trading_system/core/risk/risk_manager.py:928)、[core/risk/risk_manager.py:958](F:/9_Crypto/crypto_trading_system/core/risk/risk_manager.py:958)、[core/risk/risk_manager.py:971](F:/9_Crypto/crypto_trading_system/core/risk/risk_manager.py:971)。

触发：存在 1 单位多仓，用户卖出 100 单位，`reduce_only=False`。代码仅按订单方向和已有仓位确定 `closes_existing=True`；只有 reduce-only 才按原仓量封顶。原请求的 100 单位、100 倍杠杆仍传给订单层，两个上游检查却均收到 `allow_close=True`。成交处理 [core/trading/execution_engine.py:6061](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:6061) 先关闭旧仓再建立 99 单位空仓。

影响：daily trade、max position count、max leverage、单笔 notional、总敞口和策略分配限额对反手新敞口失效；日内亏损熔断也允许平仓豁免。隔离复现使用 equity=1000、max leverage=3、同一卖单杠杆100，观测到完整 amount100/reduceOnlyFalse 到达提交边界；把同样请求标为正常入场，真实 RiskManager 方法返回 False。

边界：真实 OrderManager 在启用 governance 时对非 reduce-only 订单另有治理复核，可能挡住一部分请求；默认 `GOVERNANCE_ENABLED=False`（config/settings.py:175），该复核也不恢复这里跳过的 RiskManager 组合敞口检查。并非所有非日内熔断都能绕过。

建议：对可减少的旧仓量与反手新开量拆分；关闭部分使用真实 reduce-only，新增部分单独做完整入场检查。不能仅凭“方向相反”给整笔单 `allow_close`。

### T2 · P1 · 交易所同侧聚合持仓被完整复制到每个策略持仓

定位：[core/trading/execution_engine.py:3774](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:3774)–3805；实际覆写数量为 [core/trading/execution_engine.py:3639](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:3639)–3655。

触发：同一 exchange/account/symbol/side 下存在策略 A 的 1 单位和 B 的 2 单位，交易所聚合持仓为 3。reconcile 构建仅按 symbol/side 的 snapshot，然后遍历每个策略 local_pos，分别用相同 snapshot 覆写 local quantity、entry price、unrealized PNL。该问题不依赖缺单或数据过期。

隔离复现：调用实际 reconcile 与 sync 方法，A=3、B=3，本地合计6，而交易所实际3。

影响：本地策略仓位归因、风险总敞口与 PNL 被放大；后续任一策略按“自身3单位”平仓可消耗另一策略实际持仓，保护单也使用错误规模。共享父账户真实凭据的逻辑账户还需以实际交易账户身份分组，不能假设每个逻辑账户拥有独立交易所持仓。

建议：把交易所聚合仓位与策略分仓账本分开保存，reconcile 对同账户同侧只比较分仓总额；利用已确认 fill/订单归属分配差额，不得把总量广播到每个策略。

### T3 · P1 · 裸订单 ID 作缓存键导致跨账户元数据覆盖和错误账户取消

定位：[core/trading/order_manager.py:66](F:/9_Crypto/crypto_trading_system/core/trading/order_manager.py:66)、[core/trading/order_manager.py:770](F:/9_Crypto/crypto_trading_system/core/trading/order_manager.py:770)、[core/trading/order_manager.py:802](F:/9_Crypto/crypto_trading_system/core/trading/order_manager.py:802)、[core/trading/order_manager.py:901](F:/9_Crypto/crypto_trading_system/core/trading/order_manager.py:901)。

触发：两个账户或交易所返回相同订单 ID（订单 ID 不能作为全系统唯一身份）。`_orders` 与 `_order_meta` 均只用 `order.id` 存储，第二个订单覆盖第一个。cancel/get operations 再从这一份元数据取 account_id 选择 connector；cancel API 不能显式传 account_id 来纠正缓存身份。

隔离复现：实际 `_create_real_order` 方法通过两个 mock connector 分别给账户 A/B 返回同 symbol/id=42。缓存只剩1条，account_id=B；取消先前 A 的 id42实际调用 B 的 cancel_order。没有真实账户交互。

影响：订单丢失、账户元数据污染；取消/状态查询发往另一真实账户。mode conflict 检查也依赖被覆盖元数据。无需假定某个交易所的 ID 格式恒定；触发边界是任何不具全局唯一性的返回值。

建议：存储、查询、取消、状态回调和对账均以 `(mode, real_account, exchange, symbol, exchange_order_id)` 为键；接口显式携带账户身份，裸 ID 模糊匹配应拒绝。

### T4 · P1 · 衍生品 contractSize 未转换，基币数量与合约张数混用

定位：[core/exchanges/okx_connector.py:232](F:/9_Crypto/crypto_trading_system/core/exchanges/okx_connector.py:232)–239、[core/exchanges/okx_connector.py:284](F:/9_Crypto/crypto_trading_system/core/exchanges/okx_connector.py:284)–293；同类位置 [core/exchanges/gate_connector.py:304](F:/9_Crypto/crypto_trading_system/core/exchanges/gate_connector.py:304)、[core/exchanges/bybit_connector.py:283](F:/9_Crypto/crypto_trading_system/core/exchanges/bybit_connector.py:283)。上游计算基币量为 [core/trading/execution_engine.py:3212](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:3212)，持仓 valuation 也按 price × quantity。

触发：使用非1 contractSize的衍生品市场。引擎按 USD/price 得基币 quantity；connector 仅做 amount precision 后直接把它送入 CCXT derivatives amount（合约张数），positions 读取则直接把 contracts当基币 amount。生产资金路径未发现 contractSize转换。对于线性合约，基币量=contracts×contractSize；倒数合约还需额外合同单位/价格处理。[CCXT 官方说明](https://docs.ccxt.com/docs/faq)、[CCXT 统一 API 文档](https://docs.ccxt.com/docs/manual)。

隔离复现：实际 OKX create_order/get_positions方法，mock market contractSize=0.1。目标基币1发送1张，正确应10张；交易所10张持仓回读为基币10，正确应1。该证明针对转换逻辑，不依赖市场连接成功或真实品种可用性。

影响：订单规模、最小量/精度、风控名义值及重启对账错一个 contractSize系数；规模可能偏大或偏小，甚至保护平仓拒单。Binance常见线性contractSize=1市场不触发这个倍率问题。ccxt_adapter存在同类模式，但默认 supports_execution=false，不把该阶段适配器算成当前可执行路径。

建议：定义系统 quantity 的统一单位，对市场 metadata contract/linear/inverse/contractSize在connector边界双向规范化；不支持的合同类型 fail closed。precision 和 min amount也必须在对应单位应用。

### T5 · P1 · 部分平仓后专用 CLOSE 再次入账累计已实现 PNL

定位：[core/trading/position_manager.py:648](F:/9_Crypto/crypto_trading_system/core/trading/position_manager.py:648)–674、[core/trading/position_manager.py:676](F:/9_Crypto/crypto_trading_system/core/trading/position_manager.py:676)–696；[core/trading/execution_engine.py:5505](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:5505)–5535、[core/trading/execution_engine.py:5554](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:5554)。

触发：持仓经过一次部分平仓，再通过专用 CLOSE 信号关闭剩余量。PositionManager返回的 realized_pnl是持仓生命周期累计值，而 CLOSE将它完整当作这一笔 fill PNL送给 RiskManager与live strategy trade记录。普通反向买卖路径已正确扣除 prev_realized（execution_engine.py:4913、4972、6002、6061），故不是账本设计要求。

隔离复现：2单位多仓entry100，先1单位在110平仓实现10，后1单位在120平仓产生20；真实CLOSE方法记录30而非20，前后合计40而非30。

影响：成交收益、策略统计与累计daily realized被重复计算；daily stop使用 realized +负unrealized（risk_manager.py:708），因此影响止损阈值。纸面equity另从position history计算，此处不声称其必然直接多计10。盈亏均可重复，重复亏损可能提前熔断，重复盈利可能抵消亏损。

建议：close前保存prev_realized，日志/风控/策略记录都使用本次增量；或让PositionManager明确返回独立CloseFill结果，包含closed_qty和fill_realized。

### T6 · P1 · Binance fast REST忽略 sandbox，测试网账户请求发向生产域名

定位：[core/trading/binance_rest.py:105](F:/9_Crypto/crypto_trading_system/core/trading/binance_rest.py:105)–111、[core/trading/binance_rest.py:152](F:/9_Crypto/crypto_trading_system/core/trading/binance_rest.py:152)–156；调用在 [core/trading/order_manager.py:680](F:/9_Crypto/crypto_trading_system/core/trading/order_manager.py:680)–697、[core/trading/order_manager.py:729](F:/9_Crypto/crypto_trading_system/core/trading/order_manager.py:729)–735。

触发：账户 ExchangeConfig/credentials sandbox=True，live模式使用Binance futures market_type以及market/limit订单。CCXT connector config会带sandbox且当前安装CCXT构造器识别它并切换测试网；但是 fast path绕过connector，由 _binance_credentials只取key/secret/proxy，丢sandbox，signed_request固定生产api/fapi域名。非reduce-only的leverage同步也先走该REST路径，可能在订单之前失败。

隔离复现：真实credentials和signed_request函数配fake sandbox=True账户；mock HTTP捕获 `POST https://fapi.binance.com/fapi/v1/order`。所有“key”均是脚本自造 AUDIT_FAKE_*，未读取实际凭据，没有发HTTP请求。

影响边界：使用仅测试网有效的key时生产通常会拒绝，导致测试网下单/杠杆流程失效；若错误绑定了生产有效key并依赖sandbox配置保护，这条链会提交生产订单或调整生产杠杆。不能把所有connector都称为忽略sandbox，也不能据此断言已有真实生产成交。

建议：统一从账户/connector的环境配置解析REST endpoint，包括time sync、leverage、balances和orders；sandbox请求不得落到生产base URL。若未支持测试网fast REST，sandbox时禁用fast path并仅调用已切换环境的CCXT。

### T7 · P2 · algo订单合并把零成交子单当成交并宣告完成

定位：[execution_engine.py:6311](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:6311)–6319。触发：iceberg/TWAP/VWAP子单被接受但尚未成交，子结果有明确filled=0和正amount。汇总代码用 `filled or amount`，零被回退为amount；status无条件为closed。

隔离复现：实际iceberg方法mock两张OPEN/filled0各amount1，返回filled2/statusclosed，实际成交0。影响限于汇总返回状态、监控、回调及使用者后续决策；资金持仓本身仍由子单成交逻辑处理，因此不声称仅此导致持仓账本虚增。建议应用显式filled字段，汇总状态按children lifecycle计算，unknown不能作为filled，父单完成必须等确认终态。

### T8 · P2 · partial take profit零成交也消费完成标志并移除止盈目标

定位：[execution_engine.py:2365](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:2365)、[execution_engine.py:2380](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:2380)、[execution_engine.py:2387](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:2387)。触发：live减仓请求被交易所接受返回真值dict，但订单仍OPEN/filled0；helper只判断返回值真假。

隔离复现：实际helper接受filled0的OPEN订单，position.quantity仍2，却输出applied=True、partial_take_profit_done=True、take_profit=None。影响：未完成目标减仓时就停止该盈利保护步骤，并可能移除原止盈价；仅部分成交也没有按目标累计数量推进。建议基于确认fill数量推进状态，保存pending订单与目标进度，撤单/拒单后恢复可重试，只有达到已确认目标才清空原止盈。

## 附加已证实观察

**A3 · P3 · 离线PnLDecomposer丢开仓手续费/滑点。** pnl_decomposer.py:201保存lot fee/slippage，249–271消费lot时只累加平仓费用，未计lot开仓成本。买1@100fee2/slip3，卖1@110fee1/slip1得net8，正确3。全仓库引用扫描只找到该模块及test_pnl_decomposer_flip.py，未找到生产调用者，所以没有把它扩展为当前live会计损失；接入前应修复按消费量分摊entry lot costs。



## 附录：Web、安全、治理与审计

原件：[web_security.md](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/web_security.md)。

## 已确认问题

### WEB-01 · P1 · 策略实盘入口未执行 approve_live 授权

**位置：** `web/api/strategies.py:2547`、`:2587-2609`、`:2781-2787`、`:2815-2835`；权限定义 `core/governance/rbac.py:42-85`；模式同步 `core/strategies/strategy_manager.py:1094-1098`、`:1643-1657`、`:1751-1762`。

**前提及影响：** 已认证的 OPERATOR 或 RESEARCH_LEAD 具有 `manage_strategies`，但没有 `approve_live`。策略注册接受 `runtime_mode=live`；策略运行模式切换仅检查请求中的确认布尔值；启动接口也只检查策略管理权限。上述权限边界允许这些角色设置并启动实盘策略账户，绕过在交易账户、手工订单和全局模式切换路由中已经实施的实盘批准权限。`confirm_live` 是输入确认，不能代替角色授权。

**证据：** 安全 FastAPI mock 验证显示 OPERATOR 的 `approve_live=false`，运行模式路由仍返回 200，并调用管理器将策略模式设为 live。另一审阅分工核对了账户与订单路由实现：策略信号显式模式/账户模式可以进入 live 路由；不能依赖全局 paper 模式消除该影响。

**建议：** 将所有实盘注册、切换、启动及批量导入路径收敛到统一服务层；实盘目标必须检查 `approve_live`，并根据治理策略核对批准记录。禁止通过 params/metadata 中的模式字段绕过该服务。加入各角色、单个/批量入口、全局 paper 与 live 的拒绝断言。

### WEB-02 · P1 · 只读身份可以解除交易熔断

**位置：** `web/api/risk.py:41`、`:50`、`:63-74`；身份依赖 `web/api/auth.py:151-162`；角色定义 `core/governance/rbac.py:86-91`。

**前提及影响：** 持有有效 AUDITOR 或 ENGINEER API key 的调用者可以通过身份认证。熔断评估与 reset 路由仅要求 `require_sensitive_ops_auth`，不核对风险管理权限；`confirm=true` 不能提供授权。Reset 会清除策略或组合熔断。组合 reset 还设置手动覆盖锁，当前同等回撤条件不会立即重新触发，见 `core/risk/circuit_breaker.py:361-375`。

**证据：** 实际路由声明的安全 mock 请求使用 AUDITOR 身份，reset 返回 200，模拟 reset 操作被调用一次；该角色没有风险批准权限。

**建议：** 为风险评估/解除明确分配权限，解除至少要求风险所有者权限，在依赖或服务入口完成验证；对只读身份拒绝应发生在任何 reset 副作用之前。

### WEB-03 · P1 · 清空或替换允许交易范围会被误判为非增风险

**位置：** `core/governance/service.py:124-129`、`:150-158`、`:263-275`；执行语义 `core/governance/decision_engine.py:153-160`。

**前提及影响：** 已存在受限 allowed_symbols 或 allowed_timeframes，OPERATOR 提交只修改范围的风险配置。`_is_list_expanded` 仅判断严格超集；受限列表被清空或换成另一列表均返回 false。执行层把空列表理解为不限范围，因此范围放宽可以得到 score=0 并自动 `applied`，无需风险所有者批准。替换集合新增的元素也不一定构成严格超集。

**证据：** 对原始风险评分和 request 函数进行隔离验证，清空两个受限列表得到 `increase_risk=false`、`risk_delta_score=0.0`、`status=applied`，激活函数被调用一次；替换 symbol 列表评分同样为 0。

**建议：** 根据实际执行语义比较有效允许集合：空列表表示全集；只要新增允许元素或取消限制即为增风险，收缩才可自动批准。大小写和 symbol/timeframe 标准化应先于比较。覆盖空、替换、超集、子集及等价列表。

### WEB-04 · P1 · 任意已应用风险配置会清除独立日内熔断

**位置：** `core/governance/service.py:234-247`；自动应用入口 `:263-275`；实际 reset 副作用 `core/risk/risk_manager.py:816-833`。

**前提及影响：** RiskConfig 的 `kill_switch=false`，但运行时已经因日内损失等其他原因停机。应用一项与 kill switch 无关的降风险修改仍执行 `risk_manager.reset_halt()`。该方法清除 halt 原因，重设当日权益基线和 realized PnL，live 下还设置 120 秒的日内停止保护期。没有风险批准权限的 OPERATOR 因此能通过自动应用配置改变独立熔断状态，损失追踪的基线也被重置。

**证据：** AST 抽取原始 `_activate_risk_config`，模拟原始停机原因为日内损失、kill_switch=false，仅降低杠杆，仍调用 reset_halt 一次；上述基线与保护期影响从 reset_halt 实现确认。

**建议：** 分离治理 kill switch 与日内损失等停机原因；配置同步只更新对应控制源。清除独立熔断必须进入专门的已授权恢复流程，不能伴随无关配置更新。风险配置和运行时状态需要分别恢复和核对。

### WEB-05 · P1 · 策略摘要存在持久化 HTML 注入与脚本执行风险

**位置：** `web/static/js/app.js:3371-3374`；输入保存 `web/api/strategies.py:1589-1599`、`:2548-2572`、`:2601-2609`；导入路径 `:2348-2377`。

**前提及影响：** 策略名称、信号中的策略名/交易对或 stale strategy 字段包含 HTML 特殊内容。注册只检查编码完整性，import 也未限制 HTML；运行摘要把这些值直接拼接到 `innerHTML`，未使用同文件的 `esc`。保存的数据再次渲染时会被浏览器当作元素和事件属性处理。交易控制界面中的此类持久化注入具有高影响；本地 UI 会获得 SYSTEM 会话，因此需要严格保持数据与页面代码的边界。

**证据：** 离线 DOM sink mock 使用原始 `renderStrategySummary`，确认合成的标签数据原样进入 active-strategies.innerHTML。未执行任何敏感页面行为。主列表 `:3168` 等位置已经正确 escape，但摘要使用了另一条渲染路径，因此不能由主列表的转义推导安全。

**建议：** 所有策略名/符号/源字段使用 textContent 或统一的 HTML 转义；避免让用户数据进入事件属性。根据输入用途约束名称和 symbol 格式，并扫描所有同类 `innerHTML` 模板。加入纯数据渲染断言，必要时部署 CSP 作为补充防护。

### WEB-06 · P2 · Polymarket profile 输出路径只限制到整个仓库，能覆盖业务文件

**位置：** `core/ops/service/api.py:210-220`；`core/ops/service/polymarket_routes.py:170-181`、`:424-449`；`prediction_markets/polymarket/paper_strategy.py:94-98`。

**前提及影响：** 已认证并具有 `manage_ai_research` 的调用者提交 profile promotion，配置 output_path。路径校验只排除仓库外位置，允许仓库内源码、配置和原始数据位置，也没有 JSON 后缀限制。保存函数直接 write_text 覆盖已有文件。输出内容是结构化 profile，并非任意字节；但这仍能破坏原配置、源码或数据。无需将路径逃逸作为影响前提。

**证据：** 只在报告临时沙箱模拟仓库根目录；原始 path resolver 接受 config 下已存在的 .py 路径，原始 save 函数将合成文件覆盖为 profile JSON。真实业务文件未改动。

**建议：** 仅允许专门的 profile 输出根目录和固定 `.json` 类型；校验 resolved 路径后再写；默认拒绝覆盖，覆盖明确由对应配置管理权限控制。读 report_path/profile_path 也应分别使用类型和目录 allowlist。

### WEB-07 · P2 · Ops 新闻变更与审计读取接口缺少能力授权

**位置：** `core/ops/service/api.py:1097`、`:1157-1159`；`core/ops/service/news_routes.py:13-25`、`:29-48`；`core/ops/service/governance_routes.py:256-266`。

**前提及影响：** 有效 API key 已通过 `/ops` 父 router 认证，但其角色没有 `manage_news` 或 `read_audit`。新闻 bridge 直接执行拉取和 LLM 队列工作，可以改新闻持久数据并消耗外部 API 资源；audit/query 直接返回审计记录而未核对 read_audit。相邻研究、AI 和 Polymarket 变更接口已经显式使用能力依赖，表明这里只做身份认证不符合统一边界。

**证据：** 合成 AUDITOR 身份没有 manage_news，原始新闻 endpoint 仍调用模拟 ingestion 一次。审计 query 权限缺口由完整路由/服务调用静态核对确认，未读取真实审计记录。

**建议：** 新闻拉取/运行要求 manage_news，审计查询要求 read_audit；将权限依赖放在 endpoint 外层并为所有角色做矩阵验证。

### WEB-08 · P1（条件启用）· 可选手工信号接口未要求交易权限

**位置：** `core/ops/service/api.py:1166-1201`。

**前提及影响：** 仅在启动环境明确启用 `OPS_ALLOW_MANUAL_SIGNAL` 时注册。该路由继承 Ops 身份认证，但没有 manage_orders 或 approve_live 校验；任何有效 API 用户身份均可到达信号提交代码。风险检查校验交易风险，不能替代调用者授权；接口固定使用 main 账户，实际执行后果取决于账户模式、可用连接器和风险限制。

**证据：** 从条件注册到 `risk_manager.check_signal`、`execution_engine.submit_signal` 的完整函数未包含能力校验。本审计未读取真实启用环境，未注册或调用真实接口，不能确认当前运行实例启用了此功能。

**建议：** 即使功能显式开启，也必须在入口要求订单管理能力，对有效目标 live 模式要求实盘批准；限制角色和账户范围，保留默认关闭。

### WEB-09 · P2 · 熔断解除的数据库审计调用必然失败并被吞掉

**位置：** `web/api/risk.py:74-89`；实际签名 `core/audit/audit_logger.py:102-110`。

**前提及影响：** Reset 到达写审计部分。调用传入 `target=...`，但 AuditLogger.log 不接受 target，且还要求 module。Python 在进入日志函数前抛 TypeError，随后 `except Exception: pass` 吞掉。熔断状态已经改变，HTTP 仍返回成功，OperationAudit 缺失对应解除记录。CircuitBreaker 本身仍有日志与状态通知，不应描述为完全没有任何痕迹。

**证据：** 对原始方法签名验证得到 `AuditLogger.log() got an unexpected keyword argument 'target'`。WEB-02 的安全 route mock 同样成功返回，从未执行实际数据库日志。

**建议：** 按现有签名传 module/action，target 放入 details；审计写失败应可观测，并按恢复操作要求确定是拒绝还是明确返回失败。验证副作用后的审计记录实际存在。

### WEB-10 · P2 · WebSocket 初始发送异常遗留事件订阅

**位置：** `web/main.py:2766-2779`、`:2842`；事件总线持有订阅 `core/realtime/event_bus.py:40-44`、`:61-85`。

**前提及影响：** 已通过认证的 WebSocket 在初始 hello 发送时断开，或 send_json 抛异常。subscribe 已执行，但包围 unsubscribe 的 try/finally 尚未进入，队列永久留在事件总线。事件总线只丢弃满队列的旧消息，不自动移除这个正常队列。连接抖动重复发生会增加无消费者队列与 fanout 成本。

**证据：** 原始 endpoint 配模拟 hello 断连，subscribe=1、unsubscribe=0；没有启动实际 WS 服务。

**建议：** 从订阅获取成功起立即进入 try/finally，初始 hello、循环和所有退出路径都在同一资源清理作用域。

### WEB-11 · P2 · Counterfactual 回填/裁剪与 append 并发会丢失审计记录

**位置：** `core/audit/gate_counterfactuals.py:65-71`、`:111-113`、`:133-134`、`:190-197`；调用方 `web/api/ai_research.py:4061-4064`、`:4897-4899`。

**前提及影响：** 一个线程或进程回填 outcome/裁剪文件，在读取快照之后另一个执行路径追加新 gate 记录。回填用旧快照整文件 os.replace，没有 append 与 rewrite 共享锁。原子替换只保证替换步骤，不保证读改写事务；新增记录会丢失，影响实验评估与审计追踪。

**证据：** 报告沙箱模拟精确的读快照→并发 append→replace 时序；回填后只保留旧记录，新 trace 消失。此验证不写生产审计文件。

**建议：** 使用单写入者或具备事务的存储；若保留 JSONL，append/回填/裁剪必须共享进程与跨进程锁，并明确崩溃恢复策略。不要只为 replace 增加锁。

### WEB-12 · P2 · 数据回放推进接口没有认证并使用 GET 改变游标

**位置：** `web/api/data.py:6900`、`:6949-6955`、`:6968-6979`、`:7008`。

**前提及影响：** 有一个已有回放会话且调用者知道 replay_id。start 和 seek/stop 要求数据管理权限，但 GET next 没有任何身份/能力依赖，也不校验会话归属；每次读取推进共享游标。无认证请求或并行页面读取可改变别人正在观察的回放进度，使事件顺序和研究展示失去可重复性。这里的影响是回放状态，不是直接订单执行。

**证据：** 完整路由静态核对：GET next 的 session["cursor"] 赋值位于返回之前，data router 无继承身份依赖。

**建议：** 将推进定义为已认证 POST 操作；绑定创建者或明确共享权限，纯 GET 只读状态/指定游标的数据。为并行推进定义锁或版本语义。

## 待验证风险与范围限制

1. **风险数值与执行语义不一致：** `core/governance/schemas.py:27-37` 对配置数值没有正值/范围/有限数约束；`decision_engine.py:130-141`、`:158` 等使用 `value or default`，显式零值与风险评分中的零值含义不同。应核对这些边界值、NaN/Infinity 与最终数据库/风险管理器状态。本分报告未将其升级为额外已复现资金问题。
2. **审批并发与版本一致性：** `service.py:215-232`、`:322-350` 的风险版本读改写、`trading_routes.py:71-99` 的 approval 在 await 后标记使用，需要在隔离数据库下测试同时请求、旧 base_version、失败回滚与多进程部署。现有静态证据不足以确定每种部署下的最终后果。
3. **治理 runtime 显示状态：** `governance_routes.py:220-252` 在请求未必 applied 的情况下直接写 app.state 的 toggle 值；实际订单 gate 使用数据库 active config。需验证状态展示是否把待审批修改错误表现为生效；不能仅凭 app.state 写入断言实际订单限制被清除。
4. **AI live 审批的一致性：** `web/api/ai_research.py:4392-4466`、`:4520-4645` 只有 manage_ai_research 并将审计 actor/role 固定为 UI/HUMAN。应将其审批语义与 Governance 双角色流程对齐并测试角色边界；本分报告的主要实盘能力问题以 WEB-01 为准，避免重复计数。
5. **长连接身份撤销：** WS 在连接时认证，连接内未再次验证 token/key 撤销；本地 cookie 值没有服务端时间字段，浏览器 max-age 不是服务端过期策略。由于本地请求和同源限制存在，未据此断言远程未认证绕过；需按部署威胁模型决定是否加入撤销和过期。
6. **运行成本与限流：** 对所有公开研究、行情、雷达与健康接口完成入口扫描；不能在不运行完整 runtime 的前提下确定冷计算、缓存穿透和最大并发成本。未进行压力测试。news URL HTML escaping 并不等同于协议 allowlist，应验证外部源 URL 范围。
7. **未验证运行实例：** 不读取秘密配置，不推断实际监听地址、OPS token 值/强度、真实 API 用户、代理可信来源、真实 CORS 白名单、当前账户模式、手工信号启用状态。CORS 的明确白名单、WS Origin 校验、loopback host/client 校验和恒定时间 token 比较在源代码中存在；本次未发现可据此确认的无身份远程认证绕过。
8. **安全持久化限制：** 未用实际数据库重跑 migration、审批提交或交易恢复；所有文件覆盖与 audit race 验证仅发生在报告沙箱。Mock 不证明跨进程/数据库一致性已满足。



## 附录：数据、AI、模型、回测与预测市场

原件：[data_research.md](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/data_research.md)。

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



## 附录：工作树独有补丁审阅



只读检查 `git diff HEAD...8a627bf`，覆盖独有提交相对共同祖先的全部 7 个 Web 文件，373 行增加、3 行删除；未改另一工作树，未重复完整旧代码或测试。此补丁主要新增自治代理活动时间线，以及雷达扫描按钮、说明条与 sticky 样式。

| 独有补丁文件 | 差异检查结果 |
|---|---|
| `web/api/ai_agent.py`（+8） | 新 `/autonomous-agent/activity` 有 read_trading_state 依赖；不执行交易副作用 |
| `web/api/ai_research.py`（+59） | 新 activity 函数读取 journal、限制 limit 到 1..100、只返回紧凑活动字段；没有新增文件写入或执行入口 |
| `web/asset_versions.py`（+2/-2） | CSS 与雷达 JS 版本更新；新增 activity 的 AI JS 版本没有随之变动，见下方发布注意事项 |
| `web/static/css/style.css`（+189） | 时间线/雷达布局与响应式样式；未新增动态代码或外部资源 URL |
| `web/static/js/ai_research_agent.js`（+79） | 新活动渲染的 label/symbol/trigger/detail/tone 等使用 esc；时间/数值格式化有类型处理。新读取调用没有新增敏感控制操作 |
| `web/static/js/altcoin_radar.js`（+9/-1） | 按钮忙碌状态使用 textContent 与固定 aria-busy 值，未引入 HTML 数据 sink |
| `web/templates/index.html`（+27） | 新时间线筛选与固定按钮，以及雷达说明条；不存在新增用户数据插值 |

**结论：** 在该独有补丁的防御性安全边界检查中，未确认新增授权/路径/渲染转义缺陷。共享旧代码的已确认问题按主工程去重；不能从此差异结论推导旧工作树整体安全。

**发布/性能待验证：** 新时间线增加 journal 轮询；其调用的旧实现 `_radar_refactor/core/ai/autonomous_agent.py:5644-5663` 同步读取整个 journal 文件后截取行数，长期文件增长下存在额外事件循环阻塞成本。未读取真实 journal、未测规模。新 AI JS 未增加对应 asset version（`:15` 仍为 14），若发布代理或浏览器设置长期缓存，需要同步版本；该旧工作树 main 没有主工程当前的 immutable 静态缓存 middleware，因此本次不将其判为当前必然缓存故障。`execution_allowed` 在旧 journal 写入 `:6013` 等价于 submitted，因此 activity 中两者合并没有据此确认状态误报。


## 附录：ignored运行辅助脚本

# 被gitignore排除的运行辅助脚本：补充静态审计

日期：2026-10-01。范围严格限定下面5个文件，共301行；全部逐文件只读审阅，3个Python文件通过AST解析，2个PowerShell文件按源码/控制流静态检查。**没有执行这5个脚本、它们的函数或其子脚本**，没有访问网络、账户、计划任务、进程运行状态、日志内容或数据文件，也未读取任何secret配置。工具显示的源码字符串全部经过遮蔽；本报告不包含token值、私人账户数据或代理凭据。只新增本审计报告，原文件保留。

`git check-ignore`确认5个都被忽略。ignore仅说明版本追踪状态，不代表脚本不能运行。它们属于人工WS验收、切换和监控辅助；硬编码路径/日期/本地端口表明用途专一，但不能仅凭源码证明目前没有被调用。尤其两个PowerShell脚本注册持久计划任务，副作用可超过一次人工执行。

## 覆盖与副作用

| 文件 | 行数 | 用途与静态确认副作用 | 运行/历史边界 |
|---|---:|---|---|
| data/_level1_relaxed_run.py | 46 | 运行带放宽容忍参数的market WS shadow selfcheck，再调用报告evaluator；创建时间戳stdout/stderr文件，向本地服务传token CLI参数 | ROOT与WT固定且不同，selfcheck/evaluator来自另一固定目录；未审阅或运行该目录。本脚本本身不包含直接交易下单调用，但被调用工具行为未在此次五文件范围内证明 |
| data/_level1_watcher.py | 54 | 等待固定STAMP目标日志/JSON最多7.5h，再运行evaluator；读取指定历史输出和service error日志，打印评估文本 | 固定STAMP使其更像某次验收的收尾器，不会自动选最新run。未读取对应日志，不证明当前被调用 |
| data/_restart_live8000.ps1 | 61 | 写启动.cmd；设置WS shadow配置；检查8000监听进程命令行，强制停止旧web.main，注册无限运行时间计划任务，再轮询health | 完整可执行运维脚本，不仅是说明文本；可以产生持久计划任务。名字含live，但源码未显式设置全局trading_mode=live，不能仅从文件名断言它启动实盘 |
| data/_switch_strategy_primary.ps1 | 62 | 同上，WS mode设strategy_primary，fail_closed=true；强制切换8000服务并注册持久任务 | 可切到策略主行情模式；注释把fail_closed=true描述为不健康时回REST，语义容易误解，实际运行语义仍由项目provider决定 |
| data/_ws_proxy_monitor.py | 78 | 默认6h循环，经指定代理GET公共Binance futures time及stream endpoint；按周期append JSONL并打印统计 | 公共连通性监控，不访问私有交易账户、不创建订单；把代理字符串写入LOG首条，若代理URL携带认证信息会落日志，未读取或显示实际代理值 |

## 已确认的问题

### IS1 · P2 · selfcheck/evaluator失败被打印成FAIL，但脚本仍正常退出0

定位：[data/_level1_relaxed_run.py:34](F:/9_Crypto/crypto_trading_system/data/_level1_relaxed_run.py:34)–46；[data/_level1_watcher.py:46](F:/9_Crypto/crypto_trading_system/data/_level1_watcher.py:46)–54。

静态证明：relaxed_run保存selfcheck返回码，只打印后继续评估；evaluator返回码仅用于打印PASS/FAIL，没有 `sys.exit(r.returncode)` 或raise。watcher也只打印evaluator结果；没有目标输出时只打印说明。脚本主体最终正常结束。

触发：子评估返回非零，或watcher直到超时仍无输出；只要脚本自身没有另一个未捕获异常，父shell看到exit0。影响：把这些辅助脚本接入调度/CI/外部告警时，失败验收可被当任务成功。此缺陷限于这些helper的退出码，不能据此断言主工程evaluator/标准验收同样误判。

建议：selfcheck失败是否允许生成诊断可显式约定，但最终退出码必须反映selfcheck与evaluator结果；watcher超时/缺失输出返回独立非零码。不要只在文本中写FAIL。

### IS2 · P2 · 代理监控把futures time接口HTTP错误计为健康

定位：[data/_ws_proxy_monitor.py:34](F:/9_Crypto/crypto_trading_system/data/_ws_proxy_monitor.py:34)–40、[data/_ws_proxy_monitor.py:55](F:/9_Crypto/crypto_trading_system/data/_ws_proxy_monitor.py:55)–63。

静态证明：probe只要requests.get收到HTTP响应就返回 `(True, r.status_code, ...)`，没有raise_for_status或status筛选。stream结果有200/400/404选择，但总good表达式仅检查f_ok和s_reach，忽略f_code。因此time接口返回401、429或500，而stream可达时，会计入ok并清零失败连续计数。

影响：若拿该统计证明“行情WS/REST链路健康”，服务限频/错误可被隐藏；对单纯TCP/TLS连通性检查，收到HTTP错误确能表示连通，所以报告需明确定义指标，不能把这个脚本统计直接当市场数据正常。它也没有读取time payload、建立WebSocket或检查消息freshness；不能当WS健康验收。

建议：区分transport_reachable与endpoint_healthy，time接口healthy要求HTTP200及合法serverTime；若目标是WS健康，应做实际WS握手和消息/时间戳检查。监控整体失败也应给调用方可读的退出状态。

## 运行风险和敏感信息边界

### IS3 · 条件性运维风险：绕过管理启动器，强杀后由独立持久任务直接运行uvicorn

定位：[data/_restart_live8000.ps1:19](F:/9_Crypto/crypto_trading_system/data/_restart_live8000.ps1:19)、[data/_restart_live8000.ps1:40](F:/9_Crypto/crypto_trading_system/data/_restart_live8000.ps1:40)–48；[data/_switch_strategy_primary.ps1:20](F:/9_Crypto/crypto_trading_system/data/_switch_strategy_primary.ps1:20)、[data/_switch_strategy_primary.ps1:41](F:/9_Crypto/crypto_trading_system/data/_switch_strategy_primary.ps1:41)–49。

两者直接以项目Python运行 `uvicorn web.main:app --host 127.0.0.1 --port 8000`，不经过web.ps1/web.bat supervisor；Register-ScheduledTask -Force可覆盖同名任务，Stop-Process -Force强制停止旧服务。源码有命令行web.main校验，避免误杀明显不相关的8000进程，但不是受管服务生命周期校验或优雅停止。

这与AGENTS.md第4节“Do not start uvicorn by hand — the supervisor owns process lifecycle”相冲突，绕过启动器提供的生命周期/pid/worker管理及paper默认启动约束。源码注释说明其他配置来自.env；本审计未读取.env，**没有证明新服务实际mode或现存计划任务**，因此此项作为历史/人工辅助的条件风险，不增加已确认当前生产缺陷计数。

建议：若仍需使用，把WS配置交给受管启动流程；记录变更、优雅停止并检查旧进程退出。若已弃用，移入明确的archive并添加不可误运行的提示，不直接删除用户现有文件。

### IS4 · 固定token与代理日志：确认存在传播路径，但未验证任何凭据有效性

relaxed_run.py:12含固定TOKEN字面量，23将它放入 `--token` 参数；34由subprocess.run创建子进程。值未显示、未验证，因此不声明它是有效生产凭据，但若有效会出现在子进程命令行并留在被忽略源码中。优先从安全运行环境/受限凭据接口传入，避免argv含token。

ws_proxy_monitor.py:24–29从env/default取代理；50把完整PROXY写入JSONL。若用户配置含userinfo的认证代理URL，日志会记录它。这里确认的是代码传播路径，不是本环境存在真实泄露。日志应只保存host/port和必要脱敏标记。

## 结论边界

五文件覆盖完毕并停止扩展。补充发现为helper自身的两个P2静态逻辑缺陷，以及条件性启动/凭据传播风险。未执行历史自检、未操作服务、未审阅另一个目录或统计真实运行状态；没有把固定STAMP收尾器等同于当前受管生产启动入口。这一补充可并入主审计覆盖附录，不改变交易分报告的8个资金路径结论。


