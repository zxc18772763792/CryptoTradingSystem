# 全量审计覆盖清单 · 2026-10-01

基线：主工程HEAD `2fecd05dea8c5ee438b4071e50548b828b7411af`。总报告：[reports/full_audit_2026-10-01.md](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01.md)。

## 覆盖口径

- **S**：登记、文本/AST/语法/模式或配置依赖结构审查，按文件类型适用；不是逐函数正确性证明。
- **D**：模块关键链人工深读，准确范围见模块报告；目录标D不表示目录每一行深读。
- **V**：隔离现有测试或AST/Mock反例，精确案例和覆盖对象见JUnit/复现JSON；不是实盘验证。
- **I**：文档、历史报告、模型/数据产物元信息登记、当前引用/接口契约核对；未复算历史结论。
- **X**：外部依赖或禁止访问的秘密/账户/真实状态；范围理由列明。没有将X伪装成通过。

## 工作区项目边界

| 边界 | 结果 | 去重/验证 |
|---|---|---|
| 主工程 | 1239个Git跟踪文件；874 Python/300242行 | S全部；D重点链；V非LIVE套件与原函数mock |
| `_radar_refactor` | 关联工作树，非独立业务副本；984跟踪/774 Python | S全量AST，D独有7文件补丁；不重复旧套件 |
| 父Git工程 | 3个跟踪条目 | 主工程gitlink、忽略规则、交接文档；无其他主业务 |
| ignored源码 | 12个辅助文件 | S/D副作用审阅，不执行；明细在本文件末尾 |
| `.conda` | 第三方runtime | 环境和pip一致性验证；X包源码逐行/CVE |
| ignored真实数据/日志与密钥 | 不纳入业务验证输入 | X不读取秘密、不覆盖原始数据；消费代码S/D |

## 审计领域检查表

| 领域 | 完成范围 | 层级 | 对应证据/限制 |
|---|---|---|---|
| 架构/模块 | main、config、strategy→risk→execution→order→connector、data/AI/research/web边界 | S/D | 总报告资金/数据/研究链说明 |
| 交易执行/风控 | 账户模式、手动/策略订单、止损止盈、分单、聚合持仓、合约单位、PNL | S/D/V | [trading.md](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/trading.md)；88 Python/36227行；真实venue未验证 |
| 持久化/恢复/并发 | position/order state、新闻游标、纸盘事务、registry/audit文件、worker进程 | S/D/V | mock故障/竞态；真实migration/长期压力X |
| 数据时效/完整性 | Hub、runtime价格、历史存储/premium/news/Polymarket | S/D/V | [data_research.md](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/data_research.md)；193 Python/75354行 |
| 回测/模型验证 | 标签split、DSL因果性、因子缓存、fee统计、年化、晋级与CUSUM | S/D/V | 没有复算真实历史收益/重训全模型 |
| API/安全 | 身份、RBAC/治理、敏感路由、路径、HTML、WS、审计 | S/D/V | [web_security.md](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/web_security.md)；75源文件109860行/343静态路由 |
| 日志/可观测性 | event bus、decision trace、audit logger/counterfactual、news/worker健康 | S/D/V | 审计签名、队列泄漏与丢更新复现；真实日志内容X |
| 部署/运维 | Docker/compose、PowerShell/bat、supervisor、启动/清理/环境脚本 | S/D/V | PS mock进程所有权；容器/服务未运行 |
| 依赖/配置 | requirements/environment、AGENTS、settings/database/env.example、YAML/JSON | S/V | verify_env/pip check/config contract通过；CVE未查询 |
| 测试 | 全部309跟踪测试文件登记，收集运行not live | S/V | 2593pass/2skip/9fail；6疑点复测5pass/1fail；缺数据4未补 |
| 文档/模型/历史报告 | README/SECURITY/STARTUP/governance/handoff/目录文档与metadata | S/I | 不把历史报告视为本次测试结果 |
| 性能 | 大模块、同步文件读取、队列/cache增长、fanout/批量上限 | S/D/V局部 | 没有吞吐、延迟、长周期内存或多进程压力基准 |

所有跟踪文件在下表登记；分报告与[inventory.json](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/inventory.json)提供对应机器索引。文件数为去重路径数，子代理LOC可能重叠，不能相加声称总行数。没有报告行覆盖率百分比。

## 目录计数及处理

| 顶层目录/文件 | 跟踪数 | 已完成处理 |
|---|---:|---|
| `.dockerignore` | 1 | S；D重点链；适用V见领域表 |
| `.env.example` | 1 | S；D重点链；适用V见领域表 |
| `.gitignore` | 1 | S；D重点链；适用V见领域表 |
| `.openclaw` | 2 | S/I；元信息/引用/当前接口核对，历史结论不复算 |
| `AGENTS.md` | 1 | S；D重点链；适用V见领域表 |
| `Dockerfile` | 1 | S；D重点链；适用V见领域表 |
| `LICENSE` | 1 | S/I；元信息/引用/当前接口核对，历史结论不复算 |
| `README.md` | 1 | S；D重点链；适用V见领域表 |
| `SECURITY.md` | 1 | S；D重点链；适用V见领域表 |
| `STARTUP.md` | 1 | S；D重点链；适用V见领域表 |
| `_once.ps1` | 1 | S；D重点链；适用V见领域表 |
| `config` | 15 | S；D重点链；适用V见领域表 |
| `core` | 267 | S；D重点链；适用V见领域表 |
| `data` | 1 | S/I；元信息/引用/当前接口核对，历史结论不复算 |
| `docker-compose.yml` | 1 | S；D重点链；适用V见领域表 |
| `docs` | 89 | S/I；元信息/引用/当前接口核对，历史结论不复算 |
| `environment.yml` | 1 | S；D重点链；适用V见领域表 |
| `main.py` | 1 | S；D重点链；适用V见领域表 |
| `models` | 34 | S/I；元信息/引用/当前接口核对，历史结论不复算 |
| `playwright.config.cjs` | 1 | S；D重点链；适用V见领域表 |
| `prediction_markets` | 17 | S；D重点链；适用V见领域表 |
| `prometheus.yml` | 1 | S；D重点链；适用V见领域表 |
| `pytest.ini` | 1 | S；D重点链；适用V见领域表 |
| `refresh_research_universe.bat` | 1 | S；D重点链；适用V见领域表 |
| `reports` | 179 | S/I；元信息/引用/当前接口核对，历史结论不复算 |
| `requirements.txt` | 1 | S；D重点链；适用V见领域表 |
| `scripts` | 220 | S全量；D启动/清理/监督/环境/验证副作用，V安全mock |
| `strategies` | 32 | S；D重点链；适用V见领域表 |
| `tests` | 309 | S/V；not live隔离suite；浏览器/live范围未运行 |
| `web` | 55 | S；D重点链；适用V见领域表 |
| `web.bat` | 1 | S；D重点链；适用V见领域表 |

## 主工程逐文件登记

语法栏仅对相应格式验证；非源码产物显示I，避免将解析成功误标业务通过。

| 文件 | 类型 | 字节/行 | 完成登记/静态检查 |
|---|---|---:|---|
| [.dockerignore](F:/9_Crypto/crypto_trading_system/.dockerignore) | 无后缀 | 1108 | S：登记/类型与引用范围 |
| [.env.example](F:/9_Crypto/crypto_trading_system/.env.example) | .example | 9063 | S：登记/类型与引用范围 |
| [.gitignore](F:/9_Crypto/crypto_trading_system/.gitignore) | 无后缀 | 1451 | S：登记/类型与引用范围 |
| [.openclaw/extensions/tradingops/index.ts](F:/9_Crypto/crypto_trading_system/.openclaw/extensions/tradingops/index.ts) | .ts | 10181 | S：登记/类型与引用范围 |
| [.openclaw/extensions/tradingops/openclaw.plugin.json](F:/9_Crypto/crypto_trading_system/.openclaw/extensions/tradingops/openclaw.plugin.json) | .json | 535 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [AGENTS.md](F:/9_Crypto/crypto_trading_system/AGENTS.md) | .md | 7277 / 171 | S：登记/类型与引用范围 |
| [Dockerfile](F:/9_Crypto/crypto_trading_system/Dockerfile) | 无后缀 | 2987 | S：登记/类型与引用范围 |
| [LICENSE](F:/9_Crypto/crypto_trading_system/LICENSE) | 无后缀 | 1092 | S：登记/类型与引用范围 |
| [README.md](F:/9_Crypto/crypto_trading_system/README.md) | .md | 11479 / 278 | S：登记/类型与引用范围 |
| [SECURITY.md](F:/9_Crypto/crypto_trading_system/SECURITY.md) | .md | 2348 / 66 | S：登记/类型与引用范围 |
| [STARTUP.md](F:/9_Crypto/crypto_trading_system/STARTUP.md) | .md | 4424 / 115 | S：登记/类型与引用范围 |
| [_once.ps1](F:/9_Crypto/crypto_trading_system/_once.ps1) | .ps1 | 30724 / 721 | S：PS AST通过；D/V依脚本副作用范围 |
| [config/__init__.py](F:/9_Crypto/crypto_trading_system/config/__init__.py) | .py | 852 / 48 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [config/database.py](F:/9_Crypto/crypto_trading_system/config/database.py) | .py | 30167 / 675 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [config/env_utils.py](F:/9_Crypto/crypto_trading_system/config/env_utils.py) | .py | 2360 / 89 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [config/exchanges.py](F:/9_Crypto/crypto_trading_system/config/exchanges.py) | .py | 3175 / 106 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [config/news_rules.yaml](F:/9_Crypto/crypto_trading_system/config/news_rules.yaml) | .yaml | 9405 / 260 | S：YAML解析通过；配置结构审阅 |
| [config/onchain_full_float_bases.json](F:/9_Crypto/crypto_trading_system/config/onchain_full_float_bases.json) | .json | 361 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [config/onchain_unlock_slugs.json](F:/9_Crypto/crypto_trading_system/config/onchain_unlock_slugs.json) | .json | 801 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [config/polymarket.yaml](F:/9_Crypto/crypto_trading_system/config/polymarket.yaml) | .yaml | 2648 / 103 | S：YAML解析通过；配置结构审阅 |
| [config/settings.py](F:/9_Crypto/crypto_trading_system/config/settings.py) | .py | 21323 / 494 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [config/strategy_factor_based.yaml](F:/9_Crypto/crypto_trading_system/config/strategy_factor_based.yaml) | .yaml | 1574 / 70 | S：YAML解析通过；配置结构审阅 |
| [config/strategy_intraday_cross_section.yaml](F:/9_Crypto/crypto_trading_system/config/strategy_intraday_cross_section.yaml) | .yaml | 4913 / 186 | S：YAML解析通过；配置结构审阅 |
| [config/strategy_multi_factor_hf.yaml](F:/9_Crypto/crypto_trading_system/config/strategy_multi_factor_hf.yaml) | .yaml | 587 / 40 | S：YAML解析通过；配置结构审阅 |
| [config/strategy_registry.py](F:/9_Crypto/crypto_trading_system/config/strategy_registry.py) | .py | 58732 / 1119 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [config/structural_strategy_suite.yaml](F:/9_Crypto/crypto_trading_system/config/structural_strategy_suite.yaml) | .yaml | 1012 / 42 | S：YAML解析通过；配置结构审阅 |
| [config/symbols.yaml](F:/9_Crypto/crypto_trading_system/config/symbols.yaml) | .yaml | 1583 / 61 | S：YAML解析通过；配置结构审阅 |
| [core/__init__.py](F:/9_Crypto/crypto_trading_system/core/__init__.py) | .py | 19 / 3 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/accounting/__init__.py](F:/9_Crypto/crypto_trading_system/core/accounting/__init__.py) | .py | 70 / 2 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/accounting/pnl_decomposer.py](F:/9_Crypto/crypto_trading_system/core/accounting/pnl_decomposer.py) | .py | 10915 / 292 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ai/__init__.py](F:/9_Crypto/crypto_trading_system/core/ai/__init__.py) | .py | 0 / 0 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ai/autonomous_agent.py](F:/9_Crypto/crypto_trading_system/core/ai/autonomous_agent.py) | .py | 308310 / 6329 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ai/autonomous_learning.py](F:/9_Crypto/crypto_trading_system/core/ai/autonomous_learning.py) | .py | 25185 / 621 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ai/autonomous_research_loop.py](F:/9_Crypto/crypto_trading_system/core/ai/autonomous_research_loop.py) | .py | 256 / 4 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ai/coinglass_signal.py](F:/9_Crypto/crypto_trading_system/core/ai/coinglass_signal.py) | .py | 6200 / 146 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ai/live_decision_router.py](F:/9_Crypto/crypto_trading_system/core/ai/live_decision_router.py) | .py | 39828 / 830 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ai/ml_signal.py](F:/9_Crypto/crypto_trading_system/core/ai/ml_signal.py) | .py | 16375 / 371 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ai/model_endpoints.py](F:/9_Crypto/crypto_trading_system/core/ai/model_endpoints.py) | .py | 1315 / 25 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ai/model_feedback_errors.py](F:/9_Crypto/crypto_trading_system/core/ai/model_feedback_errors.py) | .py | 6541 / 164 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ai/promotion_narrator.py](F:/9_Crypto/crypto_trading_system/core/ai/promotion_narrator.py) | .py | 4379 / 114 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ai/proposal_schemas.py](F:/9_Crypto/crypto_trading_system/core/ai/proposal_schemas.py) | .py | 7403 / 190 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ai/provider_runtime_policy.py](F:/9_Crypto/crypto_trading_system/core/ai/provider_runtime_policy.py) | .py | 3241 / 90 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ai/research_context_generator.py](F:/9_Crypto/crypto_trading_system/core/ai/research_context_generator.py) | .py | 23314 / 514 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ai/research_loop_evaluation.py](F:/9_Crypto/crypto_trading_system/core/ai/research_loop_evaluation.py) | .py | 13477 / 249 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ai/research_loop_service.py](F:/9_Crypto/crypto_trading_system/core/ai/research_loop_service.py) | .py | 29034 / 419 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ai/research_loop_v2.py](F:/9_Crypto/crypto_trading_system/core/ai/research_loop_v2.py) | .py | 19222 / 352 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ai/research_planner.py](F:/9_Crypto/crypto_trading_system/core/ai/research_planner.py) | .py | 51591 / 1131 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ai/research_runtime_context.py](F:/9_Crypto/crypto_trading_system/core/ai/research_runtime_context.py) | .py | 13517 / 321 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ai/research_scheduler.py](F:/9_Crypto/crypto_trading_system/core/ai/research_scheduler.py) | .py | 6862 / 175 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ai/research_search_loop.py](F:/9_Crypto/crypto_trading_system/core/ai/research_search_loop.py) | .py | 23660 / 581 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ai/risk_gate.py](F:/9_Crypto/crypto_trading_system/core/ai/risk_gate.py) | .py | 7442 / 159 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ai/runtime_eligibility.py](F:/9_Crypto/crypto_trading_system/core/ai/runtime_eligibility.py) | .py | 25654 / 616 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ai/runtime_strategy_metadata.py](F:/9_Crypto/crypto_trading_system/core/ai/runtime_strategy_metadata.py) | .py | 7484 / 202 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ai/signal_aggregator.py](F:/9_Crypto/crypto_trading_system/core/ai/signal_aggregator.py) | .py | 28149 / 628 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ai/signal_engine.py](F:/9_Crypto/crypto_trading_system/core/ai/signal_engine.py) | .py | 7825 / 197 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/audit/__init__.py](F:/9_Crypto/crypto_trading_system/core/audit/__init__.py) | .py | 135 / 5 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/audit/audit_logger.py](F:/9_Crypto/crypto_trading_system/core/audit/audit_logger.py) | .py | 5421 / 170 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/audit/gate_counterfactuals.py](F:/9_Crypto/crypto_trading_system/core/audit/gate_counterfactuals.py) | .py | 9084 / 237 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/audit/ops_audit.py](F:/9_Crypto/crypto_trading_system/core/audit/ops_audit.py) | .py | 3398 / 106 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/backtest/__init__.py](F:/9_Crypto/crypto_trading_system/core/backtest/__init__.py) | .py | 882 / 42 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/backtest/backtest_engine.py](F:/9_Crypto/crypto_trading_system/core/backtest/backtest_engine.py) | .py | 42200 / 956 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/backtest/common_pnl.py](F:/9_Crypto/crypto_trading_system/core/backtest/common_pnl.py) | .py | 1216 / 43 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/backtest/cost_models.py](F:/9_Crypto/crypto_trading_system/core/backtest/cost_models.py) | .py | 3409 / 87 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/backtest/execution_arrays.py](F:/9_Crypto/crypto_trading_system/core/backtest/execution_arrays.py) | .py | 14995 / 378 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/backtest/exit_engine.py](F:/9_Crypto/crypto_trading_system/core/backtest/exit_engine.py) | .py | 32932 / 765 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/backtest/funding_provider.py](F:/9_Crypto/crypto_trading_system/core/backtest/funding_provider.py) | .py | 15859 / 399 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/backtest/paper_trading.py](F:/9_Crypto/crypto_trading_system/core/backtest/paper_trading.py) | .py | 10038 / 283 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/backtest/performance_analyzer.py](F:/9_Crypto/crypto_trading_system/core/backtest/performance_analyzer.py) | .py | 10646 / 313 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/backtest/report_generator.py](F:/9_Crypto/crypto_trading_system/core/backtest/report_generator.py) | .py | 8101 / 218 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/__init__.py](F:/9_Crypto/crypto_trading_system/core/data/__init__.py) | .py | 1434 / 62 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/alpha_market_data.py](F:/9_Crypto/crypto_trading_system/core/data/alpha_market_data.py) | .py | 21595 / 582 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/binance_alpha.py](F:/9_Crypto/crypto_trading_system/core/data/binance_alpha.py) | .py | 14769 / 404 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/binance_alpha_collector.py](F:/9_Crypto/crypto_trading_system/core/data/binance_alpha_collector.py) | .py | 56522 / 1428 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/binance_archive.py](F:/9_Crypto/crypto_trading_system/core/data/binance_archive.py) | .py | 5515 / 169 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/coinglass_altcoin.py](F:/9_Crypto/crypto_trading_system/core/data/coinglass_altcoin.py) | .py | 29096 / 845 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/coinglass_client.py](F:/9_Crypto/crypto_trading_system/core/data/coinglass_client.py) | .py | 72982 / 1988 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/coinglass_feature_builder.py](F:/9_Crypto/crypto_trading_system/core/data/coinglass_feature_builder.py) | .py | 80485 / 1759 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/coinglass_lsr.py](F:/9_Crypto/crypto_trading_system/core/data/coinglass_lsr.py) | .py | 7549 / 185 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/coinglass_onchain.py](F:/9_Crypto/crypto_trading_system/core/data/coinglass_onchain.py) | .py | 21192 / 501 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/coinglass_registry.py](F:/9_Crypto/crypto_trading_system/core/data/coinglass_registry.py) | .py | 25112 / 743 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/cryptoquant_collector.py](F:/9_Crypto/crypto_trading_system/core/data/cryptoquant_collector.py) | .py | 4842 / 130 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/data_collector.py](F:/9_Crypto/crypto_trading_system/core/data/data_collector.py) | .py | 14040 / 397 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/data_processor.py](F:/9_Crypto/crypto_trading_system/core/data/data_processor.py) | .py | 10067 / 367 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/data_storage.py](F:/9_Crypto/crypto_trading_system/core/data/data_storage.py) | .py | 31436 / 818 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/factor_library.py](F:/9_Crypto/crypto_trading_system/core/data/factor_library.py) | .py | 26146 / 500 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/funding_rate_collector.py](F:/9_Crypto/crypto_trading_system/core/data/funding_rate_collector.py) | .py | 22851 / 631 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/funding_rate_models.py](F:/9_Crypto/crypto_trading_system/core/data/funding_rate_models.py) | .py | 4865 / 160 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/glassnode_collector.py](F:/9_Crypto/crypto_trading_system/core/data/glassnode_collector.py) | .py | 4931 / 130 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/google_trends_collector.py](F:/9_Crypto/crypto_trading_system/core/data/google_trends_collector.py) | .py | 3653 / 94 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/historical_data.py](F:/9_Crypto/crypto_trading_system/core/data/historical_data.py) | .py | 22284 / 648 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/kaiko_collector.py](F:/9_Crypto/crypto_trading_system/core/data/kaiko_collector.py) | .py | 5220 / 149 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/macro_collector.py](F:/9_Crypto/crypto_trading_system/core/data/macro_collector.py) | .py | 35785 / 1011 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/nansen_collector.py](F:/9_Crypto/crypto_trading_system/core/data/nansen_collector.py) | .py | 5547 / 161 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/news_collector.py](F:/9_Crypto/crypto_trading_system/core/data/news_collector.py) | .py | 14998 / 394 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/oi_collector.py](F:/9_Crypto/crypto_trading_system/core/data/oi_collector.py) | .py | 12080 / 381 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/options_collector.py](F:/9_Crypto/crypto_trading_system/core/data/options_collector.py) | .py | 9572 / 229 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/orderbook/__init__.py](F:/9_Crypto/crypto_trading_system/core/data/orderbook/__init__.py) | .py | 251 / 11 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/orderbook/orderbook_collector.py](F:/9_Crypto/crypto_trading_system/core/data/orderbook/orderbook_collector.py) | .py | 14732 / 466 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/parquet_lock.py](F:/9_Crypto/crypto_trading_system/core/data/parquet_lock.py) | .py | 3662 / 107 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/path_utils.py](F:/9_Crypto/crypto_trading_system/core/data/path_utils.py) | .py | 2011 / 65 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/second_level_backfill.py](F:/9_Crypto/crypto_trading_system/core/data/second_level_backfill.py) | .py | 18483 / 513 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/sentiment/__init__.py](F:/9_Crypto/crypto_trading_system/core/data/sentiment/__init__.py) | .py | 255 / 11 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/sentiment/fear_greed_collector.py](F:/9_Crypto/crypto_trading_system/core/data/sentiment/fear_greed_collector.py) | .py | 8644 / 245 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/data/source_cache_maintenance.py](F:/9_Crypto/crypto_trading_system/core/data/source_cache_maintenance.py) | .py | 3291 / 65 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/deployment/__init__.py](F:/9_Crypto/crypto_trading_system/core/deployment/__init__.py) | .py | 73 / 1 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/deployment/promotion_engine.py](F:/9_Crypto/crypto_trading_system/core/deployment/promotion_engine.py) | .py | 14697 / 359 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/events/__init__.py](F:/9_Crypto/crypto_trading_system/core/events/__init__.py) | .py | 332 / 10 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/events/supply_event_schema.py](F:/9_Crypto/crypto_trading_system/core/events/supply_event_schema.py) | .py | 5169 / 123 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/events/supply_event_store.py](F:/9_Crypto/crypto_trading_system/core/events/supply_event_store.py) | .py | 3111 / 92 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/exchange_adapters/__init__.py](F:/9_Crypto/crypto_trading_system/core/exchange_adapters/__init__.py) | .py | 435 / 20 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/exchange_adapters/base.py](F:/9_Crypto/crypto_trading_system/core/exchange_adapters/base.py) | .py | 3381 / 125 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/exchange_adapters/ccxt_adapter.py](F:/9_Crypto/crypto_trading_system/core/exchange_adapters/ccxt_adapter.py) | .py | 16715 / 381 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/exchanges/__init__.py](F:/9_Crypto/crypto_trading_system/core/exchanges/__init__.py) | .py | 1293 / 55 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/exchanges/base_exchange.py](F:/9_Crypto/crypto_trading_system/core/exchanges/base_exchange.py) | .py | 12503 / 398 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/exchanges/binance_connector.py](F:/9_Crypto/crypto_trading_system/core/exchanges/binance_connector.py) | .py | 32933 / 764 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/exchanges/bybit_connector.py](F:/9_Crypto/crypto_trading_system/core/exchanges/bybit_connector.py) | .py | 13050 / 352 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/exchanges/dex_connectors.py](F:/9_Crypto/crypto_trading_system/core/exchanges/dex_connectors.py) | .py | 14349 / 358 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/exchanges/exchange_manager.py](F:/9_Crypto/crypto_trading_system/core/exchanges/exchange_manager.py) | .py | 16811 / 435 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/exchanges/gate_connector.py](F:/9_Crypto/crypto_trading_system/core/exchanges/gate_connector.py) | .py | 15263 / 370 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/exchanges/okx_connector.py](F:/9_Crypto/crypto_trading_system/core/exchanges/okx_connector.py) | .py | 13108 / 353 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/exchanges/order_parsing.py](F:/9_Crypto/crypto_trading_system/core/exchanges/order_parsing.py) | .py | 1705 / 50 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/execution/__init__.py](F:/9_Crypto/crypto_trading_system/core/execution/__init__.py) | .py | 542 / 23 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/execution/order_intent_router.py](F:/9_Crypto/crypto_trading_system/core/execution/order_intent_router.py) | .py | 4568 / 109 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/execution/order_state_machine.py](F:/9_Crypto/crypto_trading_system/core/execution/order_state_machine.py) | .py | 15811 / 408 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/execution/rate_limit_and_reconnect.py](F:/9_Crypto/crypto_trading_system/core/execution/rate_limit_and_reconnect.py) | .py | 9522 / 245 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/factors_ts/__init__.py](F:/9_Crypto/crypto_trading_system/core/factors_ts/__init__.py) | .py | 634 / 28 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/factors_ts/base.py](F:/9_Crypto/crypto_trading_system/core/factors_ts/base.py) | .py | 1238 / 41 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/factors_ts/cache.py](F:/9_Crypto/crypto_trading_system/core/factors_ts/cache.py) | .py | 11562 / 295 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/factors_ts/extended_factors.py](F:/9_Crypto/crypto_trading_system/core/factors_ts/extended_factors.py) | .py | 35816 / 1075 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/factors_ts/funding_rate_factors.py](F:/9_Crypto/crypto_trading_system/core/factors_ts/funding_rate_factors.py) | .py | 16939 / 550 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/factors_ts/impl.py](F:/9_Crypto/crypto_trading_system/core/factors_ts/impl.py) | .py | 6101 / 164 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/factors_ts/oi_factors.py](F:/9_Crypto/crypto_trading_system/core/factors_ts/oi_factors.py) | .py | 6637 / 217 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/factors_ts/orderbook_factors.py](F:/9_Crypto/crypto_trading_system/core/factors_ts/orderbook_factors.py) | .py | 7527 / 244 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/factors_ts/registry.py](F:/9_Crypto/crypto_trading_system/core/factors_ts/registry.py) | .py | 1500 / 42 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/factors_ts/sentiment_factors.py](F:/9_Crypto/crypto_trading_system/core/factors_ts/sentiment_factors.py) | .py | 5786 / 186 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/governance/__init__.py](F:/9_Crypto/crypto_trading_system/core/governance/__init__.py) | .py | 548 / 22 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/governance/audit.py](F:/9_Crypto/crypto_trading_system/core/governance/audit.py) | .py | 2202 / 74 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/governance/decision_engine.py](F:/9_Crypto/crypto_trading_system/core/governance/decision_engine.py) | .py | 9191 / 220 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/governance/rbac.py](F:/9_Crypto/crypto_trading_system/core/governance/rbac.py) | .py | 4840 / 161 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/governance/schemas.py](F:/9_Crypto/crypto_trading_system/core/governance/schemas.py) | .py | 3379 / 102 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/governance/service.py](F:/9_Crypto/crypto_trading_system/core/governance/service.py) | .py | 27073 / 721 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/indicators/__init__.py](F:/9_Crypto/crypto_trading_system/core/indicators/__init__.py) | .py | 884 / 28 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/indicators/adx.py](F:/9_Crypto/crypto_trading_system/core/indicators/adx.py) | .py | 3763 / 103 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/indicators/oscillators.py](F:/9_Crypto/crypto_trading_system/core/indicators/oscillators.py) | .py | 2605 / 63 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/indicators/rolling.py](F:/9_Crypto/crypto_trading_system/core/indicators/rolling.py) | .py | 4779 / 134 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/market_state/__init__.py](F:/9_Crypto/crypto_trading_system/core/market_state/__init__.py) | .py | 80 / 2 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/market_state/benchmark_beta.py](F:/9_Crypto/crypto_trading_system/core/market_state/benchmark_beta.py) | .py | 6895 / 192 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/market_state/classifier.py](F:/9_Crypto/crypto_trading_system/core/market_state/classifier.py) | .py | 6400 / 179 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/market_state/hysteresis.py](F:/9_Crypto/crypto_trading_system/core/market_state/hysteresis.py) | .py | 1227 / 43 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/market_state/planner_adapter.py](F:/9_Crypto/crypto_trading_system/core/market_state/planner_adapter.py) | .py | 4609 / 115 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/market_state/schema.py](F:/9_Crypto/crypto_trading_system/core/market_state/schema.py) | .py | 1291 / 42 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/marketdata/__init__.py](F:/9_Crypto/crypto_trading_system/core/marketdata/__init__.py) | .py | 507 / 20 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/marketdata/binance_perp_ws_client.py](F:/9_Crypto/crypto_trading_system/core/marketdata/binance_perp_ws_client.py) | .py | 7623 / 191 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/marketdata/ccxt_pro_feed.py](F:/9_Crypto/crypto_trading_system/core/marketdata/ccxt_pro_feed.py) | .py | 36284 / 794 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/marketdata/hub.py](F:/9_Crypto/crypto_trading_system/core/marketdata/hub.py) | .py | 27423 / 681 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/marketdata/runtime_price_provider.py](F:/9_Crypto/crypto_trading_system/core/marketdata/runtime_price_provider.py) | .py | 9783 / 310 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/marketdata/ws_client.py](F:/9_Crypto/crypto_trading_system/core/marketdata/ws_client.py) | .py | 3786 / 105 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/marketdata/ws_quality_guard.py](F:/9_Crypto/crypto_trading_system/core/marketdata/ws_quality_guard.py) | .py | 10505 / 241 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ml/__init__.py](F:/9_Crypto/crypto_trading_system/core/ml/__init__.py) | .py | 686 / 31 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ml/pipeline.py](F:/9_Crypto/crypto_trading_system/core/ml/pipeline.py) | .py | 37310 / 987 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/monitoring/__init__.py](F:/9_Crypto/crypto_trading_system/core/monitoring/__init__.py) | .py | 0 / 0 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/monitoring/cusum_watcher.py](F:/9_Crypto/crypto_trading_system/core/monitoring/cusum_watcher.py) | .py | 19798 / 471 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/monitoring/loop_stall_watchdog.py](F:/9_Crypto/crypto_trading_system/core/monitoring/loop_stall_watchdog.py) | .py | 4905 / 129 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/monitoring/strategy_monitor.py](F:/9_Crypto/crypto_trading_system/core/monitoring/strategy_monitor.py) | .py | 9710 / 268 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/news/__init__.py](F:/9_Crypto/crypto_trading_system/core/news/__init__.py) | .py | 0 / 0 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/news/collectors/__init__.py](F:/9_Crypto/crypto_trading_system/core/news/collectors/__init__.py) | .py | 2542 / 72 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/news/collectors/binance_announcements.py](F:/9_Crypto/crypto_trading_system/core/news/collectors/binance_announcements.py) | .py | 6355 / 145 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/news/collectors/bybit_announcements.py](F:/9_Crypto/crypto_trading_system/core/news/collectors/bybit_announcements.py) | .py | 5245 / 125 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/news/collectors/chaincatcher_flash.py](F:/9_Crypto/crypto_trading_system/core/news/collectors/chaincatcher_flash.py) | .py | 6824 / 151 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/news/collectors/coinglass_news.py](F:/9_Crypto/crypto_trading_system/core/news/collectors/coinglass_news.py) | .py | 16041 / 387 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/news/collectors/common.py](F:/9_Crypto/crypto_trading_system/core/news/collectors/common.py) | .py | 9891 / 268 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/news/collectors/cryptocompare_news.py](F:/9_Crypto/crypto_trading_system/core/news/collectors/cryptocompare_news.py) | .py | 3624 / 80 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/news/collectors/cryptopanic.py](F:/9_Crypto/crypto_trading_system/core/news/collectors/cryptopanic.py) | .py | 4131 / 109 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/news/collectors/exchange_events.py](F:/9_Crypto/crypto_trading_system/core/news/collectors/exchange_events.py) | .py | 1464 / 42 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/news/collectors/gdelt.py](F:/9_Crypto/crypto_trading_system/core/news/collectors/gdelt.py) | .py | 4371 / 115 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/news/collectors/jin10.py](F:/9_Crypto/crypto_trading_system/core/news/collectors/jin10.py) | .py | 5795 / 145 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/news/collectors/manager.py](F:/9_Crypto/crypto_trading_system/core/news/collectors/manager.py) | .py | 25597 / 548 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/news/collectors/newsapi.py](F:/9_Crypto/crypto_trading_system/core/news/collectors/newsapi.py) | .py | 5902 / 150 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/news/collectors/okx_announcements.py](F:/9_Crypto/crypto_trading_system/core/news/collectors/okx_announcements.py) | .py | 5375 / 114 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/news/collectors/opennews.py](F:/9_Crypto/crypto_trading_system/core/news/collectors/opennews.py) | .py | 10998 / 280 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/news/collectors/quality.py](F:/9_Crypto/crypto_trading_system/core/news/collectors/quality.py) | .py | 5298 / 115 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/news/collectors/rss.py](F:/9_Crypto/crypto_trading_system/core/news/collectors/rss.py) | .py | 8340 / 201 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/news/eventizer/__init__.py](F:/9_Crypto/crypto_trading_system/core/news/eventizer/__init__.py) | .py | 162 / 7 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/news/eventizer/async_glm_client.py](F:/9_Crypto/crypto_trading_system/core/news/eventizer/async_glm_client.py) | .py | 74282 / 1746 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/news/eventizer/llm_glm5.py](F:/9_Crypto/crypto_trading_system/core/news/eventizer/llm_glm5.py) | .py | 60542 / 1455 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/news/eventizer/rate_limiter.py](F:/9_Crypto/crypto_trading_system/core/news/eventizer/rate_limiter.py) | .py | 9539 / 267 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/news/eventizer/rules.py](F:/9_Crypto/crypto_trading_system/core/news/eventizer/rules.py) | .py | 8746 / 225 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/news/service/__init__.py](F:/9_Crypto/crypto_trading_system/core/news/service/__init__.py) | .py | 0 / 0 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/news/service/api.py](F:/9_Crypto/crypto_trading_system/core/news/service/api.py) | .py | 9477 / 230 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/news/service/llm_worker.py](F:/9_Crypto/crypto_trading_system/core/news/service/llm_worker.py) | .py | 681 / 25 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/news/service/worker.py](F:/9_Crypto/crypto_trading_system/core/news/service/worker.py) | .py | 29664 / 767 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/news/storage/__init__.py](F:/9_Crypto/crypto_trading_system/core/news/storage/__init__.py) | .py | 0 / 0 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/news/storage/db.py](F:/9_Crypto/crypto_trading_system/core/news/storage/db.py) | .py | 82810 / 1995 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/news/storage/models.py](F:/9_Crypto/crypto_trading_system/core/news/storage/models.py) | .py | 8899 / 225 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/news/text_normalizer.py](F:/9_Crypto/crypto_trading_system/core/news/text_normalizer.py) | .py | 1570 / 51 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/notifications/__init__.py](F:/9_Crypto/crypto_trading_system/core/notifications/__init__.py) | .py | 190 / 5 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/notifications/notification_manager.py](F:/9_Crypto/crypto_trading_system/core/notifications/notification_manager.py) | .py | 38867 / 974 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/observability/__init__.py](F:/9_Crypto/crypto_trading_system/core/observability/__init__.py) | .py | 75 / 2 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/observability/decision_trace.py](F:/9_Crypto/crypto_trading_system/core/observability/decision_trace.py) | .py | 4221 / 143 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/observability/gate_codes.py](F:/9_Crypto/crypto_trading_system/core/observability/gate_codes.py) | .py | 2471 / 78 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/observability/score_calibration.py](F:/9_Crypto/crypto_trading_system/core/observability/score_calibration.py) | .py | 11726 / 284 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ops/__init__.py](F:/9_Crypto/crypto_trading_system/core/ops/__init__.py) | .py | 201 / 3 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ops/service/__init__.py](F:/9_Crypto/crypto_trading_system/core/ops/service/__init__.py) | .py | 341 / 11 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ops/service/ai_routes.py](F:/9_Crypto/crypto_trading_system/core/ops/service/ai_routes.py) | .py | 21365 / 460 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ops/service/api.py](F:/9_Crypto/crypto_trading_system/core/ops/service/api.py) | .py | 49884 / 1216 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ops/service/auth.py](F:/9_Crypto/crypto_trading_system/core/ops/service/auth.py) | .py | 4807 / 145 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ops/service/governance_routes.py](F:/9_Crypto/crypto_trading_system/core/ops/service/governance_routes.py) | .py | 10091 / 266 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ops/service/news_routes.py](F:/9_Crypto/crypto_trading_system/core/ops/service/news_routes.py) | .py | 2291 / 48 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ops/service/polymarket_routes.py](F:/9_Crypto/crypto_trading_system/core/ops/service/polymarket_routes.py) | .py | 29217 / 630 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ops/service/research_routes.py](F:/9_Crypto/crypto_trading_system/core/ops/service/research_routes.py) | .py | 4345 / 96 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/ops/service/trading_routes.py](F:/9_Crypto/crypto_trading_system/core/ops/service/trading_routes.py) | .py | 8890 / 190 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/realtime/__init__.py](F:/9_Crypto/crypto_trading_system/core/realtime/__init__.py) | .py | 144 / 5 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/realtime/event_bus.py](F:/9_Crypto/crypto_trading_system/core/realtime/event_bus.py) | .py | 3506 / 102 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/__init__.py](F:/9_Crypto/crypto_trading_system/core/research/__init__.py) | .py | 268 / 14 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/altcoin_radar.py](F:/9_Crypto/crypto_trading_system/core/research/altcoin_radar.py) | .py | 54604 / 1068 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/altcoin_radar_detail.py](F:/9_Crypto/crypto_trading_system/core/research/altcoin_radar_detail.py) | .py | 21688 / 480 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/altcoin_radar_events.py](F:/9_Crypto/crypto_trading_system/core/research/altcoin_radar_events.py) | .py | 10513 / 329 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/altcoin_radar_metrics.py](F:/9_Crypto/crypto_trading_system/core/research/altcoin_radar_metrics.py) | .py | 19910 / 516 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/altcoin_radar_narrative.py](F:/9_Crypto/crypto_trading_system/core/research/altcoin_radar_narrative.py) | .py | 9123 / 258 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/altcoin_radar_perp.py](F:/9_Crypto/crypto_trading_system/core/research/altcoin_radar_perp.py) | .py | 9720 / 271 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/altcoin_radar_ranking.py](F:/9_Crypto/crypto_trading_system/core/research/altcoin_radar_ranking.py) | .py | 10711 / 278 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/altcoin_radar_universe.py](F:/9_Crypto/crypto_trading_system/core/research/altcoin_radar_universe.py) | .py | 13808 / 453 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/binance_continuation_utility.py](F:/9_Crypto/crypto_trading_system/core/research/binance_continuation_utility.py) | .py | 4264 / 108 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/binance_entry_exit_validation.py](F:/9_Crypto/crypto_trading_system/core/research/binance_entry_exit_validation.py) | .py | 25827 / 555 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/binance_forward_microstructure.py](F:/9_Crypto/crypto_trading_system/core/research/binance_forward_microstructure.py) | .py | 7492 / 182 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/binance_hybrid_stop_validation.py](F:/9_Crypto/crypto_trading_system/core/research/binance_hybrid_stop_validation.py) | .py | 9849 / 231 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/binance_multisource_validation.py](F:/9_Crypto/crypto_trading_system/core/research/binance_multisource_validation.py) | .py | 25647 / 603 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/binance_postlaunch_validation.py](F:/9_Crypto/crypto_trading_system/core/research/binance_postlaunch_validation.py) | .py | 6910 / 145 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/binance_runup_validation.py](F:/9_Crypto/crypto_trading_system/core/research/binance_runup_validation.py) | .py | 71298 / 1631 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/binance_sequential_validation.py](F:/9_Crypto/crypto_trading_system/core/research/binance_sequential_validation.py) | .py | 12704 / 288 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/delist_risk.py](F:/9_Crypto/crypto_trading_system/core/research/delist_risk.py) | .py | 7054 / 154 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/exchange_notices.py](F:/9_Crypto/crypto_trading_system/core/research/exchange_notices.py) | .py | 9204 / 202 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/exchange_research_runner.py](F:/9_Crypto/crypto_trading_system/core/research/exchange_research_runner.py) | .py | 11079 / 252 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/experiment_registry.py](F:/9_Crypto/crypto_trading_system/core/research/experiment_registry.py) | .py | 11461 / 280 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/experiment_schemas.py](F:/9_Crypto/crypto_trading_system/core/research/experiment_schemas.py) | .py | 3185 / 94 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/listing_short_tracker.py](F:/9_Crypto/crypto_trading_system/core/research/listing_short_tracker.py) | .py | 12949 / 245 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/orchestrator.py](F:/9_Crypto/crypto_trading_system/core/research/orchestrator.py) | .py | 92788 / 2130 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/performance_feedback.py](F:/9_Crypto/crypto_trading_system/core/research/performance_feedback.py) | .py | 6469 / 170 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/pump_precursor.py](F:/9_Crypto/crypto_trading_system/core/research/pump_precursor.py) | .py | 5846 / 138 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/retirement.py](F:/9_Crypto/crypto_trading_system/core/research/retirement.py) | .py | 4462 / 89 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/signal_evidence.py](F:/9_Crypto/crypto_trading_system/core/research/signal_evidence.py) | .py | 6926 / 130 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/strategy_program.py](F:/9_Crypto/crypto_trading_system/core/research/strategy_program.py) | .py | 17610 / 448 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/strategy_research.py](F:/9_Crypto/crypto_trading_system/core/research/strategy_research.py) | .py | 143553 / 3310 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/supply_factor_tracker.py](F:/9_Crypto/crypto_trading_system/core/research/supply_factor_tracker.py) | .py | 17898 / 353 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/unlock_events.py](F:/9_Crypto/crypto_trading_system/core/research/unlock_events.py) | .py | 2722 / 65 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/unlock_short_tracker.py](F:/9_Crypto/crypto_trading_system/core/research/unlock_short_tracker.py) | .py | 16081 / 315 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/upbit_caution_tracker.py](F:/9_Crypto/crypto_trading_system/core/research/upbit_caution_tracker.py) | .py | 20226 / 347 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/validation_gate.py](F:/9_Crypto/crypto_trading_system/core/research/validation_gate.py) | .py | 29737 / 714 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/vesting_spec.py](F:/9_Crypto/crypto_trading_system/core/research/vesting_spec.py) | .py | 3131 / 71 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/xs_evaluation.py](F:/9_Crypto/crypto_trading_system/core/research/xs_evaluation.py) | .py | 15184 / 314 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/xs_feature_dsl.py](F:/9_Crypto/crypto_trading_system/core/research/xs_feature_dsl.py) | .py | 8337 / 204 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/research/xs_panel.py](F:/9_Crypto/crypto_trading_system/core/research/xs_panel.py) | .py | 9796 / 200 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/risk/__init__.py](F:/9_Crypto/crypto_trading_system/core/risk/__init__.py) | .py | 810 / 42 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/risk/circuit_breaker.py](F:/9_Crypto/crypto_trading_system/core/risk/circuit_breaker.py) | .py | 48627 / 1231 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/risk/position_sizer.py](F:/9_Crypto/crypto_trading_system/core/risk/position_sizer.py) | .py | 9240 / 259 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/risk/risk_manager.py](F:/9_Crypto/crypto_trading_system/core/risk/risk_manager.py) | .py | 69867 / 1534 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/risk/stop_loss.py](F:/9_Crypto/crypto_trading_system/core/risk/stop_loss.py) | .py | 11874 / 322 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/runtime/__init__.py](F:/9_Crypto/crypto_trading_system/core/runtime/__init__.py) | .py | 322 / 11 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/runtime/bootstrap.py](F:/9_Crypto/crypto_trading_system/core/runtime/bootstrap.py) | .py | 2764 / 81 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/runtime/operating_mode.py](F:/9_Crypto/crypto_trading_system/core/runtime/operating_mode.py) | .py | 10534 / 242 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/runtime/state.py](F:/9_Crypto/crypto_trading_system/core/runtime/state.py) | .py | 11217 / 270 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/runtime/supervisor.py](F:/9_Crypto/crypto_trading_system/core/runtime/supervisor.py) | .py | 3411 / 106 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/strategies/__init__.py](F:/9_Crypto/crypto_trading_system/core/strategies/__init__.py) | .py | 1121 / 52 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/strategies/health_monitor.py](F:/9_Crypto/crypto_trading_system/core/strategies/health_monitor.py) | .py | 5719 / 141 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/strategies/persistence.py](F:/9_Crypto/crypto_trading_system/core/strategies/persistence.py) | .py | 13495 / 328 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/strategies/runtime_policy.py](F:/9_Crypto/crypto_trading_system/core/strategies/runtime_policy.py) | .py | 3171 / 88 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/strategies/signal_generator.py](F:/9_Crypto/crypto_trading_system/core/strategies/signal_generator.py) | .py | 8987 / 291 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/strategies/strategy_base.py](F:/9_Crypto/crypto_trading_system/core/strategies/strategy_base.py) | .py | 22382 / 565 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/strategies/strategy_manager.py](F:/9_Crypto/crypto_trading_system/core/strategies/strategy_manager.py) | .py | 107701 / 2472 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/structural/__init__.py](F:/9_Crypto/crypto_trading_system/core/structural/__init__.py) | .py | 857 / 34 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/structural/context.py](F:/9_Crypto/crypto_trading_system/core/structural/context.py) | .py | 18244 / 429 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/structural/derivatives_crowding.py](F:/9_Crypto/crypto_trading_system/core/structural/derivatives_crowding.py) | .py | 20074 / 427 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/structural/onchain_flow.py](F:/9_Crypto/crypto_trading_system/core/structural/onchain_flow.py) | .py | 4487 / 125 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/structural/risk_gate.py](F:/9_Crypto/crypto_trading_system/core/structural/risk_gate.py) | .py | 5957 / 126 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/structural/supply_events.py](F:/9_Crypto/crypto_trading_system/core/structural/supply_events.py) | .py | 10392 / 251 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/trading/__init__.py](F:/9_Crypto/crypto_trading_system/core/trading/__init__.py) | .py | 910 / 46 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/trading/account_manager.py](F:/9_Crypto/crypto_trading_system/core/trading/account_manager.py) | .py | 20494 / 524 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/trading/account_snapshot.py](F:/9_Crypto/crypto_trading_system/core/trading/account_snapshot.py) | .py | 5959 / 166 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/trading/binance_rest.py](F:/9_Crypto/crypto_trading_system/core/trading/binance_rest.py) | .py | 15798 / 411 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/trading/equity_attribution.py](F:/9_Crypto/crypto_trading_system/core/trading/equity_attribution.py) | .py | 1976 / 33 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/trading/execution_engine.py](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py) | .py | 326343 / 6904 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/trading/order_manager.py](F:/9_Crypto/crypto_trading_system/core/trading/order_manager.py) | .py | 50429 / 1144 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/trading/position_manager.py](F:/9_Crypto/crypto_trading_system/core/trading/position_manager.py) | .py | 36294 / 897 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/utils/__init__.py](F:/9_Crypto/crypto_trading_system/core/utils/__init__.py) | .py | 1152 / 50 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/utils/aiohttp_resolver_hardening.py](F:/9_Crypto/crypto_trading_system/core/utils/aiohttp_resolver_hardening.py) | .py | 2458 / 56 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/utils/asset_valuation.py](F:/9_Crypto/crypto_trading_system/core/utils/asset_valuation.py) | .py | 8344 / 261 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/utils/asyncio_compat.py](F:/9_Crypto/crypto_trading_system/core/utils/asyncio_compat.py) | .py | 1200 / 37 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/utils/dual_transport.py](F:/9_Crypto/crypto_trading_system/core/utils/dual_transport.py) | .py | 2858 / 71 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/utils/listener_watchdog.py](F:/9_Crypto/crypto_trading_system/core/utils/listener_watchdog.py) | .py | 2498 / 58 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/utils/openai_responses.py](F:/9_Crypto/crypto_trading_system/core/utils/openai_responses.py) | .py | 56828 / 1532 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/utils/proactor_accept_hardening.py](F:/9_Crypto/crypto_trading_system/core/utils/proactor_accept_hardening.py) | .py | 7675 / 177 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/utils/proxy_env.py](F:/9_Crypto/crypto_trading_system/core/utils/proxy_env.py) | .py | 1685 / 35 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/utils/shared_ssl.py](F:/9_Crypto/crypto_trading_system/core/utils/shared_ssl.py) | .py | 2811 / 74 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [core/utils/time_utils.py](F:/9_Crypto/crypto_trading_system/core/utils/time_utils.py) | .py | 826 / 23 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [data/ai_calibration/family_regime_priors.json](F:/9_Crypto/crypto_trading_system/data/ai_calibration/family_regime_priors.json) | .json | 1909 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [docker-compose.yml](F:/9_Crypto/crypto_trading_system/docker-compose.yml) | .yml | 4054 / 143 | S：YAML解析通过；配置结构审阅 |
| [docs/AGENT_ML_BIAS_2026-09-30.md](F:/9_Crypto/crypto_trading_system/docs/AGENT_ML_BIAS_2026-09-30.md) | .md | 5371 / 106 | I：登记/引用及元信息；历史结果未复算 |
| [docs/AI_RESEARCH_INTEGRITY_REPAIR_2026-09-08.md](F:/9_Crypto/crypto_trading_system/docs/AI_RESEARCH_INTEGRITY_REPAIR_2026-09-08.md) | .md | 3346 / 37 | I：登记/引用及元信息；历史结果未复算 |
| [docs/AI_RESEARCH_PAGE_REDESIGN_PLAN_2026-09-08.md](F:/9_Crypto/crypto_trading_system/docs/AI_RESEARCH_PAGE_REDESIGN_PLAN_2026-09-08.md) | .md | 22922 / 419 | I：登记/引用及元信息；历史结果未复算 |
| [docs/ALPHA_NEWCOIN_FEASIBILITY_2026-09-06.md](F:/9_Crypto/crypto_trading_system/docs/ALPHA_NEWCOIN_FEASIBILITY_2026-09-06.md) | .md | 3719 / 64 | I：登记/引用及元信息；历史结果未复算 |
| [docs/ALTCOIN_DOWNTREND_BOUNCE_SHORT_STRATEGY_2026-05-24.md](F:/9_Crypto/crypto_trading_system/docs/ALTCOIN_DOWNTREND_BOUNCE_SHORT_STRATEGY_2026-05-24.md) | .md | 13257 / 165 | I：登记/引用及元信息；历史结果未复算 |
| [docs/ALTCOIN_RADAR_ARCHITECTURE.md](F:/9_Crypto/crypto_trading_system/docs/ALTCOIN_RADAR_ARCHITECTURE.md) | .md | 11658 / 136 | I：登记/引用及元信息；历史结果未复算 |
| [docs/ALTCOIN_RADAR_AUDIT_2026-07-19.md](F:/9_Crypto/crypto_trading_system/docs/ALTCOIN_RADAR_AUDIT_2026-07-19.md) | .md | 6488 / 81 | I：登记/引用及元信息；历史结果未复算 |
| [docs/ALTCOIN_RADAR_DATA_INTEGRITY_FIX_2026-06-16.md](F:/9_Crypto/crypto_trading_system/docs/ALTCOIN_RADAR_DATA_INTEGRITY_FIX_2026-06-16.md) | .md | 3313 / 79 | I：登记/引用及元信息；历史结果未复算 |
| [docs/ALTCOIN_RADAR_ENHANCEMENT_EXECUTION_PLAN_2026-04-20.md](F:/9_Crypto/crypto_trading_system/docs/ALTCOIN_RADAR_ENHANCEMENT_EXECUTION_PLAN_2026-04-20.md) | .md | 15617 / 637 | I：登记/引用及元信息；历史结果未复算 |
| [docs/ALTCOIN_RADAR_IMPLEMENTATION_PLAN_2026-04-18.md](F:/9_Crypto/crypto_trading_system/docs/ALTCOIN_RADAR_IMPLEMENTATION_PLAN_2026-04-18.md) | .md | 12136 / 77 | I：登记/引用及元信息；历史结果未复算 |
| [docs/ALTCOIN_RADAR_STATUS_TAXONOMY_REFACTOR_PROPOSAL_2026-05-20.md](F:/9_Crypto/crypto_trading_system/docs/ALTCOIN_RADAR_STATUS_TAXONOMY_REFACTOR_PROPOSAL_2026-05-20.md) | .md | 15017 / 471 | I：登记/引用及元信息；历史结果未复算 |
| [docs/ALTCOIN_RADAR_UX_FIX_2026-05-18.md](F:/9_Crypto/crypto_trading_system/docs/ALTCOIN_RADAR_UX_FIX_2026-05-18.md) | .md | 7684 / 152 | I：登记/引用及元信息；历史结果未复算 |
| [docs/AMBUSH_MODES_BACKTEST_REPORT_2026-07-18.md](F:/9_Crypto/crypto_trading_system/docs/AMBUSH_MODES_BACKTEST_REPORT_2026-07-18.md) | .md | 14323 / 228 | I：登记/引用及元信息；历史结果未复算 |
| [docs/ARBITRAGE_RISK_CONSOLE_PLAN_2026-04-18.md](F:/9_Crypto/crypto_trading_system/docs/ARBITRAGE_RISK_CONSOLE_PLAN_2026-04-18.md) | .md | 4392 / 57 | I：登记/引用及元信息；历史结果未复算 |
| [docs/BACKTEST_ACCELERATION_ROLLBACK.md](F:/9_Crypto/crypto_trading_system/docs/BACKTEST_ACCELERATION_ROLLBACK.md) | .md | 20221 / 452 | I：登记/引用及元信息；历史结果未复算 |
| [docs/BACKTEST_PAGE_BUG_FIX_PLAN_2026-06-08.md](F:/9_Crypto/crypto_trading_system/docs/BACKTEST_PAGE_BUG_FIX_PLAN_2026-06-08.md) | .md | 5509 / 87 | I：登记/引用及元信息；历史结果未复算 |
| [docs/BACKTEST_PERFORMANCE_ACCELERATION_PLAN_2026-05-20.md](F:/9_Crypto/crypto_trading_system/docs/BACKTEST_PERFORMANCE_ACCELERATION_PLAN_2026-05-20.md) | .md | 20163 / 528 | I：登记/引用及元信息；历史结果未复算 |
| [docs/CHANGELOG.md](F:/9_Crypto/crypto_trading_system/docs/CHANGELOG.md) | .md | 3878 / 96 | I：登记/引用及元信息；历史结果未复算 |
| [docs/CIRCUIT_BREAKER_RUNBOOK_2026-05-20.md](F:/9_Crypto/crypto_trading_system/docs/CIRCUIT_BREAKER_RUNBOOK_2026-05-20.md) | .md | 7070 / 187 | I：登记/引用及元信息；历史结果未复算 |
| [docs/CODE_AUDIT_2026-05-18.md](F:/9_Crypto/crypto_trading_system/docs/CODE_AUDIT_2026-05-18.md) | .md | 18761 / 242 | I：登记/引用及元信息；历史结果未复算 |
| [docs/CODE_AUDIT_2026-05-19.md](F:/9_Crypto/crypto_trading_system/docs/CODE_AUDIT_2026-05-19.md) | .md | 5411 / 78 | I：登记/引用及元信息；历史结果未复算 |
| [docs/CODE_AUDIT_2026-05-21.md](F:/9_Crypto/crypto_trading_system/docs/CODE_AUDIT_2026-05-21.md) | .md | 8157 / 116 | I：登记/引用及元信息；历史结果未复算 |
| [docs/CODE_AUDIT_2026-05-27.md](F:/9_Crypto/crypto_trading_system/docs/CODE_AUDIT_2026-05-27.md) | .md | 9522 / 195 | I：登记/引用及元信息；历史结果未复算 |
| [docs/CODE_AUDIT_2026-05-29.md](F:/9_Crypto/crypto_trading_system/docs/CODE_AUDIT_2026-05-29.md) | .md | 11263 / 259 | I：登记/引用及元信息；历史结果未复算 |
| [docs/CODE_AUDIT_2026-06-07.md](F:/9_Crypto/crypto_trading_system/docs/CODE_AUDIT_2026-06-07.md) | .md | 12995 / 255 | I：登记/引用及元信息；历史结果未复算 |
| [docs/CODE_AUDIT_2026-06-08.md](F:/9_Crypto/crypto_trading_system/docs/CODE_AUDIT_2026-06-08.md) | .md | 10922 / 205 | I：登记/引用及元信息；历史结果未复算 |
| [docs/CODE_AUDIT_2026-06-09.md](F:/9_Crypto/crypto_trading_system/docs/CODE_AUDIT_2026-06-09.md) | .md | 11038 / 236 | I：登记/引用及元信息；历史结果未复算 |
| [docs/CODE_AUDIT_2026-06-10.md](F:/9_Crypto/crypto_trading_system/docs/CODE_AUDIT_2026-06-10.md) | .md | 15574 / 276 | I：登记/引用及元信息；历史结果未复算 |
| [docs/CODE_AUDIT_2026-06-11.md](F:/9_Crypto/crypto_trading_system/docs/CODE_AUDIT_2026-06-11.md) | .md | 12618 / 258 | I：登记/引用及元信息；历史结果未复算 |
| [docs/CODE_AUDIT_2026-07-05.md](F:/9_Crypto/crypto_trading_system/docs/CODE_AUDIT_2026-07-05.md) | .md | 14803 / 247 | I：登记/引用及元信息；历史结果未复算 |
| [docs/CODE_AUDIT_TEAM_2026-05-29.md](F:/9_Crypto/crypto_trading_system/docs/CODE_AUDIT_TEAM_2026-05-29.md) | .md | 82971 / 515 | I：登记/引用及元信息；历史结果未复算 |
| [docs/CODE_REVIEW_2026-05-19.md](F:/9_Crypto/crypto_trading_system/docs/CODE_REVIEW_2026-05-19.md) | .md | 13420 / 267 | I：登记/引用及元信息；历史结果未复算 |
| [docs/COINGLASS_KEYSTONE_EXECUTION_PLAN_2026-04-18.md](F:/9_Crypto/crypto_trading_system/docs/COINGLASS_KEYSTONE_EXECUTION_PLAN_2026-04-18.md) | .md | 16830 / 717 | I：登记/引用及元信息；历史结果未复算 |
| [docs/COINGLASS_OPTIONAL_MARKET_DATA_IMPLEMENTATION_GUIDE_2026-05-17.md](F:/9_Crypto/crypto_trading_system/docs/COINGLASS_OPTIONAL_MARKET_DATA_IMPLEMENTATION_GUIDE_2026-05-17.md) | .md | 10414 / 296 | I：登记/引用及元信息；历史结果未复算 |
| [docs/COINGLASS_SIGNAL_ENHANCEMENT_PLAN_2026-04-18.md](F:/9_Crypto/crypto_trading_system/docs/COINGLASS_SIGNAL_ENHANCEMENT_PLAN_2026-04-18.md) | .md | 11021 / 364 | I：登记/引用及元信息；历史结果未复算 |
| [docs/CONDA_ENV_RUNTIME_DIAGNOSIS_2026-05-23.md](F:/9_Crypto/crypto_trading_system/docs/CONDA_ENV_RUNTIME_DIAGNOSIS_2026-05-23.md) | .md | 15911 / 491 | I：登记/引用及元信息；历史结果未复算 |
| [docs/CRYPTO_STRATEGY_GAP_RESEARCH_2026-05-20.md](F:/9_Crypto/crypto_trading_system/docs/CRYPTO_STRATEGY_GAP_RESEARCH_2026-05-20.md) | .md | 20168 / 339 | I：登记/引用及元信息；历史结果未复算 |
| [docs/EXIT_LOGIC_OVERHAUL_PLAN_2026-05-20.md](F:/9_Crypto/crypto_trading_system/docs/EXIT_LOGIC_OVERHAUL_PLAN_2026-05-20.md) | .md | 69359 / 1350 | I：登记/引用及元信息；历史结果未复算 |
| [docs/FACTOR_STRATEGY_EXTENSION.md](F:/9_Crypto/crypto_trading_system/docs/FACTOR_STRATEGY_EXTENSION.md) | .md | 7017 / 185 | I：登记/引用及元信息；历史结果未复算 |
| [docs/FULL_CODE_AUDIT_2026-09-29.md](F:/9_Crypto/crypto_trading_system/docs/FULL_CODE_AUDIT_2026-09-29.md) | .md | 5416 / 29 | I：登记/引用及元信息；历史结果未复算 |
| [docs/GOVERNANCE.md](F:/9_Crypto/crypto_trading_system/docs/GOVERNANCE.md) | .md | 2869 / 81 | I：登记/引用及元信息；历史结果未复算 |
| [docs/IMPROVEMENT_PLAN_2026-05-23.md](F:/9_Crypto/crypto_trading_system/docs/IMPROVEMENT_PLAN_2026-05-23.md) | .md | 36518 / 207 | I：登记/引用及元信息；历史结果未复算 |
| [docs/INTEGRATION.md](F:/9_Crypto/crypto_trading_system/docs/INTEGRATION.md) | .md | 8562 / 189 | I：登记/引用及元信息；历史结果未复算 |
| [docs/INTRADAY_CROSS_SECTION_STRATEGIES_2026-05-24.md](F:/9_Crypto/crypto_trading_system/docs/INTRADAY_CROSS_SECTION_STRATEGIES_2026-05-24.md) | .md | 3876 / 80 | I：登记/引用及元信息；历史结果未复算 |
| [docs/LANGUAGE_MIX_AUDIT_2026-05-28.md](F:/9_Crypto/crypto_trading_system/docs/LANGUAGE_MIX_AUDIT_2026-05-28.md) | .md | 11184 / 226 | I：登记/引用及元信息；历史结果未复算 |
| [docs/LLM_FORECASTING_RESEARCH_2026-09-25.md](F:/9_Crypto/crypto_trading_system/docs/LLM_FORECASTING_RESEARCH_2026-09-25.md) | .md | 8180 / 97 | I：登记/引用及元信息；历史结果未复算 |
| [docs/LLM_TRADING_RESEARCH_ROUND2_2026-09-25.md](F:/9_Crypto/crypto_trading_system/docs/LLM_TRADING_RESEARCH_ROUND2_2026-09-25.md) | .md | 5838 / 92 | I：登记/引用及元信息；历史结果未复算 |
| [docs/LLM_TRADING_RESEARCH_ROUND3_2026-09-26.md](F:/9_Crypto/crypto_trading_system/docs/LLM_TRADING_RESEARCH_ROUND3_2026-09-26.md) | .md | 7660 / 108 | I：登记/引用及元信息；历史结果未复算 |
| [docs/LLM_TRADING_RESEARCH_ROUND4_2026-09-27.md](F:/9_Crypto/crypto_trading_system/docs/LLM_TRADING_RESEARCH_ROUND4_2026-09-27.md) | .md | 3012 / 45 | I：登记/引用及元信息；历史结果未复算 |
| [docs/LLM_TRADING_RESEARCH_ROUND5_2026-09-27.md](F:/9_Crypto/crypto_trading_system/docs/LLM_TRADING_RESEARCH_ROUND5_2026-09-27.md) | .md | 4094 / 63 | I：登记/引用及元信息；历史结果未复算 |
| [docs/LLM_TRADING_RESEARCH_ROUND6_2026-09-27.md](F:/9_Crypto/crypto_trading_system/docs/LLM_TRADING_RESEARCH_ROUND6_2026-09-27.md) | .md | 6859 / 86 | I：登记/引用及元信息；历史结果未复算 |
| [docs/LLM_TRADING_RESEARCH_ROUND7_2026-09-28.md](F:/9_Crypto/crypto_trading_system/docs/LLM_TRADING_RESEARCH_ROUND7_2026-09-28.md) | .md | 14956 / 185 | I：登记/引用及元信息；历史结果未复算 |
| [docs/LOCAL_WORKSPACE_MEMORY.md](F:/9_Crypto/crypto_trading_system/docs/LOCAL_WORKSPACE_MEMORY.md) | .md | 1167 / 14 | I：登记/引用及元信息；历史结果未复算 |
| [docs/LOW_LATENCY_SMALL_CAP_STRATEGY_DESIGN_2026-05-20.md](F:/9_Crypto/crypto_trading_system/docs/LOW_LATENCY_SMALL_CAP_STRATEGY_DESIGN_2026-05-20.md) | .md | 23987 / 868 | I：登记/引用及元信息；历史结果未复算 |
| [docs/MARKET_DATA_WS_UPGRADE_PLAN_2026-05-29.md](F:/9_Crypto/crypto_trading_system/docs/MARKET_DATA_WS_UPGRADE_PLAN_2026-05-29.md) | .md | 269036 / 5015 | I：登记/引用及元信息；历史结果未复算 |
| [docs/OI_MCAP_AMBUSH_STRATEGY_FAMILY_2026-07-18.md](F:/9_Crypto/crypto_trading_system/docs/OI_MCAP_AMBUSH_STRATEGY_FAMILY_2026-07-18.md) | .md | 4403 / 74 | I：登记/引用及元信息；历史结果未复算 |
| [docs/ONCHAIN_DATA_SOURCE_RESEARCH_2026-07-19.md](F:/9_Crypto/crypto_trading_system/docs/ONCHAIN_DATA_SOURCE_RESEARCH_2026-07-19.md) | .md | 9612 / 158 | I：登记/引用及元信息；历史结果未复算 |
| [docs/OPERATING_REALITY_LAYER_CONTINUATION_2026-05-17.md](F:/9_Crypto/crypto_trading_system/docs/OPERATING_REALITY_LAYER_CONTINUATION_2026-05-17.md) | .md | 6799 / 140 | I：登记/引用及元信息；历史结果未复算 |
| [docs/OPERATING_REALITY_LAYER_PLAN_2026-05-17.md](F:/9_Crypto/crypto_trading_system/docs/OPERATING_REALITY_LAYER_PLAN_2026-05-17.md) | .md | 11325 / 261 | I：登记/引用及元信息；历史结果未复算 |
| [docs/PAPER_LIVE_ISOLATION_FIX_PLAN_2026-05-24.md](F:/9_Crypto/crypto_trading_system/docs/PAPER_LIVE_ISOLATION_FIX_PLAN_2026-05-24.md) | .md | 38008 / 818 | I：登记/引用及元信息；历史结果未复算 |
| [docs/PAPER_LONGRUN_BURNIN_CHECKLIST.md](F:/9_Crypto/crypto_trading_system/docs/PAPER_LONGRUN_BURNIN_CHECKLIST.md) | .md | 2201 / 68 | I：登记/引用及元信息；历史结果未复算 |
| [docs/PAPER_LONGRUN_RUNBOOK.md](F:/9_Crypto/crypto_trading_system/docs/PAPER_LONGRUN_RUNBOOK.md) | .md | 3474 / 147 | I：登记/引用及元信息；历史结果未复算 |
| [docs/PHASE3_COST_REALISM_PLAN_2026-05-20.md](F:/9_Crypto/crypto_trading_system/docs/PHASE3_COST_REALISM_PLAN_2026-05-20.md) | .md | 3303 / 72 | I：登记/引用及元信息；历史结果未复算 |
| [docs/PHASE4_2_PORTFOLIO_DRAWDOWN_KILLSWITCH_PLAN_2026-05-20.md](F:/9_Crypto/crypto_trading_system/docs/PHASE4_2_PORTFOLIO_DRAWDOWN_KILLSWITCH_PLAN_2026-05-20.md) | .md | 6216 / 123 | I：登记/引用及元信息；历史结果未复算 |
| [docs/PHASE5_PAPER_LIVE_RECONCILIATION_PLAN_2026-05-20.md](F:/9_Crypto/crypto_trading_system/docs/PHASE5_PAPER_LIVE_RECONCILIATION_PLAN_2026-05-20.md) | .md | 4411 / 103 | I：登记/引用及元信息；历史结果未复算 |
| [docs/POLYMARKET_LIVE_CLOB_SETUP.md](F:/9_Crypto/crypto_trading_system/docs/POLYMARKET_LIVE_CLOB_SETUP.md) | .md | 4073 / 98 | I：登记/引用及元信息；历史结果未复算 |
| [docs/REPOSITORY_OVERVIEW.md](F:/9_Crypto/crypto_trading_system/docs/REPOSITORY_OVERVIEW.md) | .md | 3248 / 90 | I：登记/引用及元信息；历史结果未复算 |
| [docs/RESEARCH_DATA_AUDIT_2026-09-29.md](F:/9_Crypto/crypto_trading_system/docs/RESEARCH_DATA_AUDIT_2026-09-29.md) | .md | 3116 / 22 | I：登记/引用及元信息；历史结果未复算 |
| [docs/RESEARCH_LOOP_V2.md](F:/9_Crypto/crypto_trading_system/docs/RESEARCH_LOOP_V2.md) | .md | 6362 / 84 | I：登记/引用及元信息；历史结果未复算 |
| [docs/RESEARCH_WORKBENCH_UX_FIX_2026-05-18.md](F:/9_Crypto/crypto_trading_system/docs/RESEARCH_WORKBENCH_UX_FIX_2026-05-18.md) | .md | 6041 / 112 | I：登记/引用及元信息；历史结果未复算 |
| [docs/SECOND_ROUND_INTRADAY_CROSS_SECTION_STRATEGIES_2026-05-25.md](F:/9_Crypto/crypto_trading_system/docs/SECOND_ROUND_INTRADAY_CROSS_SECTION_STRATEGIES_2026-05-25.md) | .md | 3722 / 54 | I：登记/引用及元信息；历史结果未复算 |
| [docs/STABILITY_PROFITABILITY_IMPROVEMENT_PLAN_2026-05-20.md](F:/9_Crypto/crypto_trading_system/docs/STABILITY_PROFITABILITY_IMPROVEMENT_PLAN_2026-05-20.md) | .md | 6124 / 114 | I：登记/引用及元信息；历史结果未复算 |
| [docs/STRATEGY_EXPLORATION_2026-07-19.md](F:/9_Crypto/crypto_trading_system/docs/STRATEGY_EXPLORATION_2026-07-19.md) | .md | 12855 / 200 | I：登记/引用及元信息；历史结果未复算 |
| [docs/STRATEGY_EXPLORATION_2026-09-29.md](F:/9_Crypto/crypto_trading_system/docs/STRATEGY_EXPLORATION_2026-09-29.md) | .md | 6703 / 36 | I：登记/引用及元信息；历史结果未复算 |
| [docs/STRATEGY_LOGIC_AND_PERFORMANCE_REVIEW_2026-05-25.md](F:/9_Crypto/crypto_trading_system/docs/STRATEGY_LOGIC_AND_PERFORMANCE_REVIEW_2026-05-25.md) | .md | 13816 / 108 | I：登记/引用及元信息；历史结果未复算 |
| [docs/STRATEGY_SIGNAL_CLOCK_FIX_2026-05-18.md](F:/9_Crypto/crypto_trading_system/docs/STRATEGY_SIGNAL_CLOCK_FIX_2026-05-18.md) | .md | 6438 / 135 | I：登记/引用及元信息；历史结果未复算 |
| [docs/SYSTEM_HEALTH_AUDIT_2026-05-24.md](F:/9_Crypto/crypto_trading_system/docs/SYSTEM_HEALTH_AUDIT_2026-05-24.md) | .md | 21869 / 443 | I：登记/引用及元信息；历史结果未复算 |
| [docs/UNLOCK_EVENT_STUDY_2026-09-27.md](F:/9_Crypto/crypto_trading_system/docs/UNLOCK_EVENT_STUDY_2026-09-27.md) | .md | 6577 / 84 | I：登记/引用及元信息；历史结果未复算 |
| [docs/architecture_adapters.md](F:/9_Crypto/crypto_trading_system/docs/architecture_adapters.md) | .md | 8966 / 313 | I：登记/引用及元信息；历史结果未复算 |
| [docs/autonomous_research_loop.md](F:/9_Crypto/crypto_trading_system/docs/autonomous_research_loop.md) | .md | 6703 / 60 | I：登记/引用及元信息；历史结果未复算 |
| [docs/backtest_logic_current.md](F:/9_Crypto/crypto_trading_system/docs/backtest_logic_current.md) | .md | 8206 / 274 | I：登记/引用及元信息；历史结果未复算 |
| [docs/code_review_20260526.md](F:/9_Crypto/crypto_trading_system/docs/code_review_20260526.md) | .md | 7338 / 143 | I：登记/引用及元信息；历史结果未复算 |
| [docs/data_onchain.txt](F:/9_Crypto/crypto_trading_system/docs/data_onchain.txt) | .txt | 27022 | I：登记/引用及元信息；历史结果未复算 |
| [docs/factor_formulas.md](F:/9_Crypto/crypto_trading_system/docs/factor_formulas.md) | .md | 9733 / 434 | I：登记/引用及元信息；历史结果未复算 |
| [docs/legacy/report_exit_refactor.md](F:/9_Crypto/crypto_trading_system/docs/legacy/report_exit_refactor.md) | .md | 6260 / 85 | I：登记/引用及元信息；历史结果未复算 |
| [docs/open_source_reference.md](F:/9_Crypto/crypto_trading_system/docs/open_source_reference.md) | .md | 8278 / 221 | I：登记/引用及元信息；历史结果未复算 |
| [docs/plans/CLAUDE.md](F:/9_Crypto/crypto_trading_system/docs/plans/CLAUDE.md) | .md | 60814 / 1439 | I：登记/引用及元信息；历史结果未复算 |
| [docs/second_round_top20_diverse_factors.md](F:/9_Crypto/crypto_trading_system/docs/second_round_top20_diverse_factors.md) | .md | 31016 / 348 | I：登记/引用及元信息；历史结果未复算 |
| [docs/web-style-guide.md](F:/9_Crypto/crypto_trading_system/docs/web-style-guide.md) | .md | 5449 / 86 | I：登记/引用及元信息；历史结果未复算 |
| [environment.yml](F:/9_Crypto/crypto_trading_system/environment.yml) | .yml | 387 / 14 | S：YAML解析通过；配置结构审阅 |
| [main.py](F:/9_Crypto/crypto_trading_system/main.py) | .py | 5677 / 181 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [models/ml_signal_xgb.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb.json) | .json | 214758 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb.manifest.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb.manifest.json) | .json | 427 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260413T115302328384Z_6992c71b/feature_importances.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260413T115302328384Z_6992c71b/feature_importances.json) | .json | 399 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260413T115302328384Z_6992c71b/manifest.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260413T115302328384Z_6992c71b/manifest.json) | .json | 33405 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260413T115302328384Z_6992c71b/metrics.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260413T115302328384Z_6992c71b/metrics.json) | .json | 27632 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260413T115302328384Z_6992c71b/model.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260413T115302328384Z_6992c71b/model.json) | .json | 213858 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260413T115328807331Z_6992c71b/feature_importances.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260413T115328807331Z_6992c71b/feature_importances.json) | .json | 399 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260413T115328807331Z_6992c71b/manifest.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260413T115328807331Z_6992c71b/manifest.json) | .json | 33405 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260413T115328807331Z_6992c71b/metrics.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260413T115328807331Z_6992c71b/metrics.json) | .json | 27632 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260413T115328807331Z_6992c71b/model.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260413T115328807331Z_6992c71b/model.json) | .json | 213858 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260413T120518304880Z_6992c71b/feature_importances.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260413T120518304880Z_6992c71b/feature_importances.json) | .json | 402 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260413T120518304880Z_6992c71b/manifest.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260413T120518304880Z_6992c71b/manifest.json) | .json | 33414 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260413T120518304880Z_6992c71b/metrics.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260413T120518304880Z_6992c71b/metrics.json) | .json | 27641 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260413T120518304880Z_6992c71b/model.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260413T120518304880Z_6992c71b/model.json) | .json | 214758 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T112655436614Z_d05f1a3c/feature_importances.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T112655436614Z_d05f1a3c/feature_importances.json) | .json | 400 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T112655436614Z_d05f1a3c/manifest.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T112655436614Z_d05f1a3c/manifest.json) | .json | 18268 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T112655436614Z_d05f1a3c/metrics.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T112655436614Z_d05f1a3c/metrics.json) | .json | 14759 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T112655436614Z_d05f1a3c/model.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T112655436614Z_d05f1a3c/model.json) | .json | 1445554 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T112926603117Z_b43f754c/feature_importances.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T112926603117Z_b43f754c/feature_importances.json) | .json | 207 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T112926603117Z_b43f754c/manifest.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T112926603117Z_b43f754c/manifest.json) | .json | 18116 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T112926603117Z_b43f754c/metrics.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T112926603117Z_b43f754c/metrics.json) | .json | 14627 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T112926603117Z_b43f754c/model.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T112926603117Z_b43f754c/model.json) | .json | 60905 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T113336015584Z_b43f754c/feature_importances.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T113336015584Z_b43f754c/feature_importances.json) | .json | 207 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T113336015584Z_b43f754c/manifest.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T113336015584Z_b43f754c/manifest.json) | .json | 18116 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T113336015584Z_b43f754c/metrics.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T113336015584Z_b43f754c/metrics.json) | .json | 14627 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T113336015584Z_b43f754c/model.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T113336015584Z_b43f754c/model.json) | .json | 60905 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T113727038892Z_b43f754c/feature_importances.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T113727038892Z_b43f754c/feature_importances.json) | .json | 400 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T113727038892Z_b43f754c/manifest.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T113727038892Z_b43f754c/manifest.json) | .json | 18268 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T113727038892Z_b43f754c/metrics.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T113727038892Z_b43f754c/metrics.json) | .json | 14759 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T113727038892Z_b43f754c/model.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T113727038892Z_b43f754c/model.json) | .json | 1445554 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T115544948825Z_b43f754c/feature_importances.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T115544948825Z_b43f754c/feature_importances.json) | .json | 401 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T115544948825Z_b43f754c/manifest.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T115544948825Z_b43f754c/manifest.json) | .json | 96926 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T115544948825Z_b43f754c/metrics.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T115544948825Z_b43f754c/metrics.json) | .json | 81713 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T115544948825Z_b43f754c/model.json](F:/9_Crypto/crypto_trading_system/models/ml_signal_xgb/mlsig_btc_usdt_1h_20260418T115544948825Z_b43f754c/model.json) | .json | 938949 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [playwright.config.cjs](F:/9_Crypto/crypto_trading_system/playwright.config.cjs) | .cjs | 323 / 14 | S：node语法通过；D/V依前端边界范围 |
| [prediction_markets/__init__.py](F:/9_Crypto/crypto_trading_system/prediction_markets/__init__.py) | .py | 67 / 3 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [prediction_markets/polymarket/README.md](F:/9_Crypto/crypto_trading_system/prediction_markets/polymarket/README.md) | .md | 2342 / 56 | S：登记/类型与引用范围 |
| [prediction_markets/polymarket/__init__.py](F:/9_Crypto/crypto_trading_system/prediction_markets/polymarket/__init__.py) | .py | 877 / 25 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [prediction_markets/polymarket/clob_reader.py](F:/9_Crypto/crypto_trading_system/prediction_markets/polymarket/clob_reader.py) | .py | 15505 / 342 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [prediction_markets/polymarket/clob_trader.py](F:/9_Crypto/crypto_trading_system/prediction_markets/polymarket/clob_trader.py) | .py | 3363 / 75 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [prediction_markets/polymarket/config.py](F:/9_Crypto/crypto_trading_system/prediction_markets/polymarket/config.py) | .py | 3704 / 98 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [prediction_markets/polymarket/db.py](F:/9_Crypto/crypto_trading_system/prediction_markets/polymarket/db.py) | .py | 40063 / 950 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [prediction_markets/polymarket/features.py](F:/9_Crypto/crypto_trading_system/prediction_markets/polymarket/features.py) | .py | 9992 / 238 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [prediction_markets/polymarket/gamma_client.py](F:/9_Crypto/crypto_trading_system/prediction_markets/polymarket/gamma_client.py) | .py | 4752 / 125 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [prediction_markets/polymarket/market_resolver.py](F:/9_Crypto/crypto_trading_system/prediction_markets/polymarket/market_resolver.py) | .py | 11995 / 235 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [prediction_markets/polymarket/models.py](F:/9_Crypto/crypto_trading_system/prediction_markets/polymarket/models.py) | .py | 11070 / 195 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [prediction_markets/polymarket/paper_replay.py](F:/9_Crypto/crypto_trading_system/prediction_markets/polymarket/paper_replay.py) | .py | 8739 / 229 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [prediction_markets/polymarket/paper_strategy.py](F:/9_Crypto/crypto_trading_system/prediction_markets/polymarket/paper_strategy.py) | .py | 13571 / 335 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [prediction_markets/polymarket/paper_trading.py](F:/9_Crypto/crypto_trading_system/prediction_markets/polymarket/paper_trading.py) | .py | 12995 / 299 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [prediction_markets/polymarket/replay_report.py](F:/9_Crypto/crypto_trading_system/prediction_markets/polymarket/replay_report.py) | .py | 17470 / 409 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [prediction_markets/polymarket/utils.py](F:/9_Crypto/crypto_trading_system/prediction_markets/polymarket/utils.py) | .py | 3992 / 126 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [prediction_markets/polymarket/worker.py](F:/9_Crypto/crypto_trading_system/prediction_markets/polymarket/worker.py) | .py | 16691 / 383 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [prometheus.yml](F:/9_Crypto/crypto_trading_system/prometheus.yml) | .yml | 274 / 14 | S：YAML解析通过；配置结构审阅 |
| [pytest.ini](F:/9_Crypto/crypto_trading_system/pytest.ini) | .ini | 2548 / 61 | S：登记/类型与引用范围 |
| [refresh_research_universe.bat](F:/9_Crypto/crypto_trading_system/refresh_research_universe.bat) | .bat | 289 / 15 | S：登记/类型与引用范围 |
| [reports/ambush_modes_2026-07-18/benchmark_equal_weight.csv](F:/9_Crypto/crypto_trading_system/reports/ambush_modes_2026-07-18/benchmark_equal_weight.csv) | .csv | 11255 | I：登记/引用及元信息；历史结果未复算 |
| [reports/ambush_modes_2026-07-18/decay_short_study.json](F:/9_Crypto/crypto_trading_system/reports/ambush_modes_2026-07-18/decay_short_study.json) | .json | 430445 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/ambush_modes_2026-07-18/equity_A_accumulation.csv](F:/9_Crypto/crypto_trading_system/reports/ambush_modes_2026-07-18/equity_A_accumulation.csv) | .csv | 10219 | I：登记/引用及元信息；历史结果未复算 |
| [reports/ambush_modes_2026-07-18/equity_B_squeeze.csv](F:/9_Crypto/crypto_trading_system/reports/ambush_modes_2026-07-18/equity_B_squeeze.csv) | .csv | 10259 | I：登记/引用及元信息；历史结果未复算 |
| [reports/ambush_modes_2026-07-18/equity_C_ignition.csv](F:/9_Crypto/crypto_trading_system/reports/ambush_modes_2026-07-18/equity_C_ignition.csv) | .csv | 10293 | I：登记/引用及元信息；历史结果未复算 |
| [reports/ambush_modes_2026-07-18/ignition_5m_retest.json](F:/9_Crypto/crypto_trading_system/reports/ambush_modes_2026-07-18/ignition_5m_retest.json) | .json | 57203 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/ambush_modes_2026-07-18/intraday_15m_lab.json](F:/9_Crypto/crypto_trading_system/reports/ambush_modes_2026-07-18/intraday_15m_lab.json) | .json | 7973 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/ambush_modes_2026-07-18/listing_effect_study.json](F:/9_Crypto/crypto_trading_system/reports/ambush_modes_2026-07-18/listing_effect_study.json) | .json | 33098 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/ambush_modes_2026-07-18/model_robustness_audit.json](F:/9_Crypto/crypto_trading_system/reports/ambush_modes_2026-07-18/model_robustness_audit.json) | .json | 8076 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/ambush_modes_2026-07-18/regime_gate_study.json](F:/9_Crypto/crypto_trading_system/reports/ambush_modes_2026-07-18/regime_gate_study.json) | .json | 9953 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/ambush_modes_2026-07-18/sensitivity.csv](F:/9_Crypto/crypto_trading_system/reports/ambush_modes_2026-07-18/sensitivity.csv) | .csv | 1357 | I：登记/引用及元信息；历史结果未复算 |
| [reports/ambush_modes_2026-07-18/summary.json](F:/9_Crypto/crypto_trading_system/reports/ambush_modes_2026-07-18/summary.json) | .json | 8246 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/ambush_modes_2026-07-18/summary.md](F:/9_Crypto/crypto_trading_system/reports/ambush_modes_2026-07-18/summary.md) | .md | 1236 / 17 | I：登记/引用及元信息；历史结果未复算 |
| [reports/ambush_modes_2026-07-18/trades.csv](F:/9_Crypto/crypto_trading_system/reports/ambush_modes_2026-07-18/trades.csv) | .csv | 10589 | I：登记/引用及元信息；历史结果未复算 |
| [reports/ambush_modes_2026-07-18/unlock_candidate_findings.json](F:/9_Crypto/crypto_trading_system/reports/ambush_modes_2026-07-18/unlock_candidate_findings.json) | .json | 2832 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/ambush_modes_2026-07-18/unlock_feature_panel.parquet](F:/9_Crypto/crypto_trading_system/reports/ambush_modes_2026-07-18/unlock_feature_panel.parquet) | .parquet | 130660 | I：登记/引用及元信息；历史结果未复算 |
| [reports/audit_2026-05-21/00_SUMMARY.md](F:/9_Crypto/crypto_trading_system/reports/audit_2026-05-21/00_SUMMARY.md) | .md | 13262 / 189 | I：登记/引用及元信息；历史结果未复算 |
| [reports/audit_2026-05-21/01_strategies.md](F:/9_Crypto/crypto_trading_system/reports/audit_2026-05-21/01_strategies.md) | .md | 18079 / 134 | I：登记/引用及元信息；历史结果未复算 |
| [reports/audit_2026-05-21/02_backtest.md](F:/9_Crypto/crypto_trading_system/reports/audit_2026-05-21/02_backtest.md) | .md | 19297 / 351 | I：登记/引用及元信息；历史结果未复算 |
| [reports/audit_2026-05-21/03_news.md](F:/9_Crypto/crypto_trading_system/reports/audit_2026-05-21/03_news.md) | .md | 23041 / 338 | I：登记/引用及元信息；历史结果未复算 |
| [reports/audit_2026-05-21/04_research_ai.md](F:/9_Crypto/crypto_trading_system/reports/audit_2026-05-21/04_research_ai.md) | .md | 21624 / 265 | I：登记/引用及元信息；历史结果未复算 |
| [reports/audit_2026-05-21/05_exchanges_trading.md](F:/9_Crypto/crypto_trading_system/reports/audit_2026-05-21/05_exchanges_trading.md) | .md | 18506 / 172 | I：登记/引用及元信息；历史结果未复算 |
| [reports/audit_2026-05-21/06_web.md](F:/9_Crypto/crypto_trading_system/reports/audit_2026-05-21/06_web.md) | .md | 13449 / 117 | I：登记/引用及元信息；历史结果未复算 |
| [reports/audit_2026-05-21/TEAM_REMEDIATION_PLAN.md](F:/9_Crypto/crypto_trading_system/reports/audit_2026-05-21/TEAM_REMEDIATION_PLAN.md) | .md | 12488 / 194 | I：登记/引用及元信息；历史结果未复算 |
| [reports/audit_2026-05-29/01_strategies.md](F:/9_Crypto/crypto_trading_system/reports/audit_2026-05-29/01_strategies.md) | .md | 24901 / 216 | I：登记/引用及元信息；历史结果未复算 |
| [reports/audit_2026-05-29/02_backtest_risk.md](F:/9_Crypto/crypto_trading_system/reports/audit_2026-05-29/02_backtest_risk.md) | .md | 27823 / 272 | I：登记/引用及元信息；历史结果未复算 |
| [reports/audit_2026-05-29/04_news.md](F:/9_Crypto/crypto_trading_system/reports/audit_2026-05-29/04_news.md) | .md | 24733 / 302 | I：登记/引用及元信息；历史结果未复算 |
| [reports/audit_2026-05-29/06_research.md](F:/9_Crypto/crypto_trading_system/reports/audit_2026-05-29/06_research.md) | .md | 27522 / 433 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18/REPORT.md](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18/REPORT.md) | .md | 7381 / 145 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18/analysis_summary.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18/analysis_summary.json) | .json | 106687 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18/binance_200pct_factor_mining.ipynb](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18/binance_200pct_factor_mining.ipynb) | .ipynb | 5298 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18/binance_200pct_factor_mining.py](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18/binance_200pct_factor_mining.py) | .py | 71388 / 1725 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [reports/binance_200pct_factor_mining_2026-07-18/data_quality.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18/data_quality.json) | .json | 2058 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18/factor_model.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18/factor_model.json) | .json | 1685 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18/model_metrics.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18/model_metrics.json) | .json | 7033 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_final/CHART_MAP.md](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_final/CHART_MAP.md) | .md | 1668 / 9 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_final/REPORT.md](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_final/REPORT.md) | .md | 7832 / 80 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_final/analysis_summary.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_final/analysis_summary.json) | .json | 110146 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_final/binance_200pct_factor_mining.ipynb](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_final/binance_200pct_factor_mining.ipynb) | .ipynb | 4526 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_final/data_quality.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_final/data_quality.json) | .json | 2058 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_final/factor_model.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_final/factor_model.json) | .json | 1685 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_final/model_metrics.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_final/model_metrics.json) | .json | 7033 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_final/run_200pct_factor_mining.py](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_final/run_200pct_factor_mining.py) | .py | 634 / 27 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [reports/binance_200pct_factor_mining_2026-07-18_final/verification_summary.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_final/verification_summary.json) | .json | 1082 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_final/verify_200pct_results.py](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_final/verify_200pct_results.py) | .py | 7455 / 195 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures/CHART_MAP.md](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures/CHART_MAP.md) | .md | 1146 / 9 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures/REPORT.md](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures/REPORT.md) | .md | 7847 / 90 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures/VALIDATION_REPORT.md](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures/VALIDATION_REPORT.md) | .md | 2405 / 40 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures/analysis_summary.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures/analysis_summary.json) | .json | 165674 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures/analyze_oi_factor_30d.py](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures/analyze_oi_factor_30d.py) | .py | 10526 / 235 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures/data_quality.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures/data_quality.json) | .json | 2181 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures/evaluate_oi_price_combo.py](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures/evaluate_oi_price_combo.py) | .py | 4354 / 107 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures/factor_model.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures/factor_model.json) | .json | 1699 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures/final_qa.py](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures/final_qa.py) | .py | 4022 / 113 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures/final_qa_summary.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures/final_qa_summary.json) | .json | 673 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures/model_metrics.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures/model_metrics.json) | .json | 7325 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures/oi_factor_summary_30d.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures/oi_factor_summary_30d.json) | .json | 3893 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures/oi_price_combo_summary_30d.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures/oi_price_combo_summary_30d.json) | .json | 3528 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures/run_200pct_futures_factor_mining.py](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures/run_200pct_futures_factor_mining.py) | .py | 646 / 23 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures/verification_summary.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures/verification_summary.json) | .json | 861 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures/verify_futures_results.py](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures/verify_futures_results.py) | .py | 5052 / 120 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/CHART_MAP.md](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/CHART_MAP.md) | .md | 6981 / 110 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/EXIT_VALIDATION_REPORT.md](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/EXIT_VALIDATION_REPORT.md) | .md | 377 / 9 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/MODEL_CARD.md](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/MODEL_CARD.md) | .md | 17226 / 198 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/REPORT.md](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/REPORT.md) | .md | 53152 / 512 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/UNIVERSE_COVERAGE.md](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/UNIVERSE_COVERAGE.md) | .md | 1176 / 13 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/VALIDATION_REPORT.md](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/VALIDATION_REPORT.md) | .md | 343 / 9 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/artifact.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/artifact.json) | .json | 135502 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/binance_runup_research.ipynb](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/binance_runup_research.ipynb) | .ipynb | 368152 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/continuation_utility_results.png](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/continuation_utility_results.png) | .png | 207717 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/equity_drawdown.png](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/equity_drawdown.png) | .png | 157505 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/exit_cost_stress_confirmation.png](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/exit_cost_stress_confirmation.png) | .png | 61570 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/exit_fixed_tp_confirmation.png](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/exit_fixed_tp_confirmation.png) | .png | 163959 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/intensity_exit_frontier.png](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/intensity_exit_frontier.png) | .png | 108967 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/mfe_mae.png](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/mfe_mae.png) | .png | 128963 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/multisource_catalyst_fold_pf.png](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/multisource_catalyst_fold_pf.png) | .png | 32640 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/multisource_increment.png](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/multisource_increment.png) | .png | 57780 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/oi_increment.png](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/oi_increment.png) | .png | 35016 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/postlaunch_entry_comparison.png](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/postlaunch_entry_comparison.png) | .png | 72837 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/sequence_exit_profit_factor.png](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/sequence_exit_profit_factor.png) | .png | 27071 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/sequence_model_ap.png](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/sequence_model_ap.png) | .png | 41615 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/sequence_rule_precision.png](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/sequence_rule_precision.png) | .png | 44875 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/strategy_frontier_falsifications.png](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/strategy_frontier_falsifications.png) | .png | 109301 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/threshold_horizon_heatmap.png](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/threshold_horizon_heatmap.png) | .png | 88476 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/timing_entry_precision.png](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/timing_entry_precision.png) | .png | 72208 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/timing_path_mfe.png](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/timing_path_mfe.png) | .png | 59738 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/timing_threshold_stop.png](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/timing_threshold_stop.png) | .png | 66133 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/utility_breakout_add.png](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/utility_breakout_add.png) | .png | 133787 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/utility_breakout_hold_router.png](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/utility_breakout_hold_router.png) | .png | 152848 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/utility_breakout_partial_fraction.png](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/utility_breakout_partial_fraction.png) | .png | 53680 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/utility_entry_frontier.png](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/utility_entry_frontier.png) | .png | 158850 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/utility_entry_timing.png](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/utility_entry_timing.png) | .png | 139195 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/utility_exhaustion_attribution.png](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/utility_exhaustion_attribution.png) | .png | 222882 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/utility_exhaustion_exit.png](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/utility_exhaustion_exit.png) | .png | 97209 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/utility_intensity_router.png](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/utility_intensity_router.png) | .png | 81953 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/utility_regime_partial_router.png](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/utility_regime_partial_router.png) | .png | 124999 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/utility_strategy_iteration.png](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/utility_strategy_iteration.png) | .png | 160708 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/utility_taker_flow_filter.png](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/utility_taker_flow_filter.png) | .png | 142905 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/walk_forward_lift.png](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/charts/walk_forward_lift.png) | .png | 37598 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/continuation_entry_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/continuation_entry_decision.json) | .json | 9263 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/continuation_utility_forward_model.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/continuation_utility_forward_model.json) | .json | 4665 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/data_quality.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/data_quality.json) | .json | 1634 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/entry_exit_timing_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/entry_exit_timing_decision.json) | .json | 6287 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/entry_exit_timing_verification.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/entry_exit_timing_verification.json) | .json | 1846 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/evidence_manifest.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/evidence_manifest.json) | .json | 69045 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/exit_strategy_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/exit_strategy_decision.json) | .json | 6086 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/exit_verification_summary.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/exit_verification_summary.json) | .json | 451 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/extreme_intensity_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/extreme_intensity_decision.json) | .json | 3549 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/forward_monitor_operational_audit_2026-07-21.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/forward_monitor_operational_audit_2026-07-21.json) | .json | 1673 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/hybrid_stop_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/hybrid_stop_decision.json) | .json | 7708 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/intensity_exit_frontier_verification.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/intensity_exit_frontier_verification.json) | .json | 1024 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/launch_exit_state_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/launch_exit_state_decision.json) | .json | 4659 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/launch_stop_strategy_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/launch_stop_strategy_decision.json) | .json | 4730 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/lifecycle_factor_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/lifecycle_factor_decision.json) | .json | 2398 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/microstructure_confirmation_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/microstructure_confirmation_decision.json) | .json | 8234 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/multisource_analysis_summary.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/multisource_analysis_summary.json) | .json | 8127 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/multisource_catalyst_strategy_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/multisource_catalyst_strategy_decision.json) | .json | 3784 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/multisource_data_quality.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/multisource_data_quality.json) | .json | 1140 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/multisource_evidence_manifest.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/multisource_evidence_manifest.json) | .json | 1517 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/multisource_news_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/multisource_news_decision.json) | .json | 606 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/multisource_onchain_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/multisource_onchain_decision.json) | .json | 605 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/multisource_verification_summary.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/multisource_verification_summary.json) | .json | 2443 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/oi_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/oi_decision.json) | .json | 1515 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/paper_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/paper_decision.json) | .json | 298 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/path_timing_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/path_timing_decision.json) | .json | 850 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/postlaunch_entry_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/postlaunch_entry_decision.json) | .json | 6700 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/rank_threshold_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/rank_threshold_decision.json) | .json | 1595 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/sequential_robustness_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/sequential_robustness_decision.json) | .json | 6311 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/sequential_strategy_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/sequential_strategy_decision.json) | .json | 9150 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/sequential_strategy_verification.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/sequential_strategy_verification.json) | .json | 3767 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/simple_launch_strategy_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/simple_launch_strategy_decision.json) | .json | 8382 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/staged_entry_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/staged_entry_decision.json) | .json | 5966 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/stop_width_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/stop_width_decision.json) | .json | 4197 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_breakout_add_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_breakout_add_decision.json) | .json | 30375 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_breakout_add_verification.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_breakout_add_verification.json) | .json | 810 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_breakout_hold_router_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_breakout_hold_router_decision.json) | .json | 42334 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_breakout_hold_router_verification.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_breakout_hold_router_verification.json) | .json | 814 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_breakout_partial_fraction_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_breakout_partial_fraction_decision.json) | .json | 36208 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_breakout_runner_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_breakout_runner_decision.json) | .json | 84257 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_entry_frontier_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_entry_frontier_decision.json) | .json | 64634 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_entry_frontier_verification.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_entry_frontier_verification.json) | .json | 841 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_entry_timing_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_entry_timing_decision.json) | .json | 14734 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_entry_timing_verification.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_entry_timing_verification.json) | .json | 961 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_exhaustion_attribution_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_exhaustion_attribution_decision.json) | .json | 34193 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_exhaustion_attribution_verification.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_exhaustion_attribution_verification.json) | .json | 914 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_exhaustion_exit_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_exhaustion_exit_decision.json) | .json | 10028 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_exhaustion_exit_verification.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_exhaustion_exit_verification.json) | .json | 703 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_failure_exit_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_failure_exit_decision.json) | .json | 9410 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_intensity_router_calibration.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_intensity_router_calibration.json) | .json | 3888 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_intensity_router_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_intensity_router_decision.json) | .json | 16408 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_intensity_router_verification.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_intensity_router_verification.json) | .json | 680 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_regime_partial_router_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_regime_partial_router_decision.json) | .json | 58822 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_strategy_iteration_verification.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_strategy_iteration_verification.json) | .json | 2044 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_tail_exit_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_tail_exit_decision.json) | .json | 9477 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_taker_flow_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_taker_flow_decision.json) | .json | 17835 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_taker_flow_robustness_decision.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_taker_flow_robustness_decision.json) | .json | 1938 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_taker_flow_verification.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/utility_taker_flow_verification.json) | .json | 855 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_factor_mining_2026-07-18_futures_extended/verification_summary.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_factor_mining_2026-07-18_futures_extended/verification_summary.json) | .json | 801 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_futures_universe_audit_2026-07-18/futures_scan_summary.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_futures_universe_audit_2026-07-18/futures_scan_summary.json) | .json | 1659 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_futures_universe_audit_2026-07-18/futures_verification_summary.json](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_futures_universe_audit_2026-07-18/futures_verification_summary.json) | .json | 335 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_200pct_futures_universe_audit_2026-07-18/scan_futures_30d_200pct.py](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_futures_universe_audit_2026-07-18/scan_futures_30d_200pct.py) | .py | 11683 / 296 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [reports/binance_200pct_futures_universe_audit_2026-07-18/verify_futures_30d_200pct.py](F:/9_Crypto/crypto_trading_system/reports/binance_200pct_futures_universe_audit_2026-07-18/verify_futures_30d_200pct.py) | .py | 4161 / 111 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [reports/binance_300pct_factor_mining_2026-07-18/REPORT.md](F:/9_Crypto/crypto_trading_system/reports/binance_300pct_factor_mining_2026-07-18/REPORT.md) | .md | 6857 / 128 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_300pct_factor_mining_2026-07-18/analysis_summary.json](F:/9_Crypto/crypto_trading_system/reports/binance_300pct_factor_mining_2026-07-18/analysis_summary.json) | .json | 99780 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_300pct_factor_mining_2026-07-18/binance_300pct_factor_mining.ipynb](F:/9_Crypto/crypto_trading_system/reports/binance_300pct_factor_mining_2026-07-18/binance_300pct_factor_mining.ipynb) | .ipynb | 3074 | I：登记/引用及元信息；历史结果未复算 |
| [reports/binance_300pct_factor_mining_2026-07-18/data_quality.json](F:/9_Crypto/crypto_trading_system/reports/binance_300pct_factor_mining_2026-07-18/data_quality.json) | .json | 2058 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_300pct_factor_mining_2026-07-18/factor_model.json](F:/9_Crypto/crypto_trading_system/reports/binance_300pct_factor_mining_2026-07-18/factor_model.json) | .json | 1695 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_300pct_factor_mining_2026-07-18/model_metrics.json](F:/9_Crypto/crypto_trading_system/reports/binance_300pct_factor_mining_2026-07-18/model_metrics.json) | .json | 7823 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_300pct_factor_mining_2026-07-18/run_300pct_factor_mining.py](F:/9_Crypto/crypto_trading_system/reports/binance_300pct_factor_mining_2026-07-18/run_300pct_factor_mining.py) | .py | 699 / 24 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [reports/binance_tenbagger_oi_2026-07-18/analysis_summary.json](F:/9_Crypto/crypto_trading_system/reports/binance_tenbagger_oi_2026-07-18/analysis_summary.json) | .json | 11996 | S/I：JSON语法通过；业务/历史schema未全量验证 |
| [reports/binance_tenbagger_oi_2026-07-18/binance_oi_research.py](F:/9_Crypto/crypto_trading_system/reports/binance_tenbagger_oi_2026-07-18/binance_oi_research.py) | .py | 21120 / 541 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [reports/close_reason_coverage_2026-05-21.md](F:/9_Crypto/crypto_trading_system/reports/close_reason_coverage_2026-05-21.md) | .md | 714 / 18 | I：登记/引用及元信息；历史结果未复算 |
| [reports/exit_audit_after_phase5_2026-05-21.md](F:/9_Crypto/crypto_trading_system/reports/exit_audit_after_phase5_2026-05-21.md) | .md | 1815 / 52 | I：登记/引用及元信息；历史结果未复算 |
| [reports/exit_audit_baseline_2026-05-20.md](F:/9_Crypto/crypto_trading_system/reports/exit_audit_baseline_2026-05-20.md) | .md | 1252 / 42 | I：登记/引用及元信息；历史结果未复算 |
| [reports/exit_logic_verification_2026-05-21.md](F:/9_Crypto/crypto_trading_system/reports/exit_logic_verification_2026-05-21.md) | .md | 2402 / 84 | I：登记/引用及元信息；历史结果未复算 |
| [reports/phase8_before_after_BollingerBandsStrategy_BTC_USDT_1h.md](F:/9_Crypto/crypto_trading_system/reports/phase8_before_after_BollingerBandsStrategy_BTC_USDT_1h.md) | .md | 1133 / 30 | I：登记/引用及元信息；历史结果未复算 |
| [reports/phase8_before_after_MAStrategy_BTC_USDT_1h.md](F:/9_Crypto/crypto_trading_system/reports/phase8_before_after_MAStrategy_BTC_USDT_1h.md) | .md | 934 / 28 | I：登记/引用及元信息；历史结果未复算 |
| [reports/phase8_before_after_RSIStrategy_BTC_USDT_1h.md](F:/9_Crypto/crypto_trading_system/reports/phase8_before_after_RSIStrategy_BTC_USDT_1h.md) | .md | 928 / 28 | I：登记/引用及元信息；历史结果未复算 |
| [reports/post_change_live_exit_gate_2026-05-21.md](F:/9_Crypto/crypto_trading_system/reports/post_change_live_exit_gate_2026-05-21.md) | .md | 1259 / 23 | I：登记/引用及元信息；历史结果未复算 |
| [requirements.txt](F:/9_Crypto/crypto_trading_system/requirements.txt) | .txt | 2727 | S：登记/类型与引用范围 |
| [scripts/agent_call_edge.py](F:/9_Crypto/crypto_trading_system/scripts/agent_call_edge.py) | .py | 12472 / 268 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/agent_filter_value.py](F:/9_Crypto/crypto_trading_system/scripts/agent_filter_value.py) | .py | 8299 / 181 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/alpha_feasibility_probe.py](F:/9_Crypto/crypto_trading_system/scripts/alpha_feasibility_probe.py) | .py | 3655 / 77 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_catalyst_strategy.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_catalyst_strategy.py) | .py | 9527 / 221 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_continuation_entry.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_continuation_entry.py) | .py | 32960 / 657 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_entry_exit_timing.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_entry_exit_timing.py) | .py | 25662 / 560 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_exit_strategies.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_exit_strategies.py) | .py | 45128 / 1029 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_extreme_intensity.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_extreme_intensity.py) | .py | 17429 / 405 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_hybrid_stops.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_hybrid_stops.py) | .py | 18351 / 376 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_launch_exit_state.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_launch_exit_state.py) | .py | 11319 / 258 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_launch_stop_strategy.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_launch_stop_strategy.py) | .py | 11050 / 250 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_lifecycle_factor.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_lifecycle_factor.py) | .py | 11135 / 216 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_microstructure_confirmation.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_microstructure_confirmation.py) | .py | 14203 / 286 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_multisource_factors.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_multisource_factors.py) | .py | 22467 / 492 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_path_timing.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_path_timing.py) | .py | 7260 / 163 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_postlaunch_entries.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_postlaunch_entries.py) | .py | 18322 / 378 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_rank_thresholds.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_rank_thresholds.py) | .py | 13016 / 288 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_runup_extended.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_runup_extended.py) | .py | 38793 / 816 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_sequence_robustness.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_sequence_robustness.py) | .py | 13598 / 301 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_sequential_strategy.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_sequential_strategy.py) | .py | 25467 / 553 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_simple_launch_strategy.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_simple_launch_strategy.py) | .py | 12519 / 263 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_staged_entry.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_staged_entry.py) | .py | 14564 / 314 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_stop_widths.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_stop_widths.py) | .py | 9431 / 195 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_utility_breakout_add.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_utility_breakout_add.py) | .py | 24480 / 510 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_utility_breakout_hold_router.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_utility_breakout_hold_router.py) | .py | 19959 / 428 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_utility_breakout_partial_fraction.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_utility_breakout_partial_fraction.py) | .py | 10071 / 219 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_utility_breakout_runner.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_utility_breakout_runner.py) | .py | 13745 / 302 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_utility_entry_frontier.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_utility_entry_frontier.py) | .py | 17729 / 338 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_utility_entry_timing.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_utility_entry_timing.py) | .py | 27395 / 563 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_utility_exhaustion_attribution.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_utility_exhaustion_attribution.py) | .py | 16217 / 351 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_utility_exhaustion_exit.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_utility_exhaustion_exit.py) | .py | 14310 / 324 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_utility_failure_exit.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_utility_failure_exit.py) | .py | 17697 / 395 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_utility_intensity_router.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_utility_intensity_router.py) | .py | 21919 / 466 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_utility_regime_partial_router.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_utility_regime_partial_router.py) | .py | 16192 / 383 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_utility_tail_exit.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_utility_tail_exit.py) | .py | 13495 / 292 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_utility_taker_flow_filter.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_utility_taker_flow_filter.py) | .py | 21595 / 471 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/analyze_binance_utility_taker_flow_robustness.py](F:/9_Crypto/crypto_trading_system/scripts/analyze_binance_utility_taker_flow_robustness.py) | .py | 9516 / 229 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/apply_final_solution_setup.py](F:/9_Crypto/crypto_trading_system/scripts/apply_final_solution_setup.py) | .py | 4523 / 123 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/audit_ambush_golden_cases.py](F:/9_Crypto/crypto_trading_system/scripts/audit_ambush_golden_cases.py) | .py | 6160 / 155 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/audit_close_reason_coverage.py](F:/9_Crypto/crypto_trading_system/scripts/audit_close_reason_coverage.py) | .py | 9345 / 248 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/audit_exit_reasons.py](F:/9_Crypto/crypto_trading_system/scripts/audit_exit_reasons.py) | .py | 11839 / 322 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/audit_upbit_short_cache.py](F:/9_Crypto/crypto_trading_system/scripts/audit_upbit_short_cache.py) | .py | 6106 / 119 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/backfill_close_reasons.py](F:/9_Crypto/crypto_trading_system/scripts/backfill_close_reasons.py) | .py | 11976 / 318 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/backtest_ambush_modes.py](F:/9_Crypto/crypto_trading_system/scripts/backtest_ambush_modes.py) | .py | 21934 / 518 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/benchmark_backtest_runtime.py](F:/9_Crypto/crypto_trading_system/scripts/benchmark_backtest_runtime.py) | .py | 15900 / 447 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/binance_continuation_utility_forward_monitor.py](F:/9_Crypto/crypto_trading_system/scripts/binance_continuation_utility_forward_monitor.py) | .py | 15606 / 348 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/binance_continuation_utility_labeler.py](F:/9_Crypto/crypto_trading_system/scripts/binance_continuation_utility_labeler.py) | .py | 6636 / 159 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/binance_forward_strategy_labeler.py](F:/9_Crypto/crypto_trading_system/scripts/binance_forward_strategy_labeler.py) | .py | 20709 / 504 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/binance_launch_microstructure_forward_monitor.py](F:/9_Crypto/crypto_trading_system/scripts/binance_launch_microstructure_forward_monitor.py) | .py | 16900 / 419 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/binance_runup_forward_monitor.py](F:/9_Crypto/crypto_trading_system/scripts/binance_runup_forward_monitor.py) | .py | 22550 / 497 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/binance_sequence_forward_monitor.py](F:/9_Crypto/crypto_trading_system/scripts/binance_sequence_forward_monitor.py) | .py | 9967 / 258 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/build_ambush_dataset.py](F:/9_Crypto/crypto_trading_system/scripts/build_ambush_dataset.py) | .py | 17140 / 428 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/build_binance_runup_artifact.py](F:/9_Crypto/crypto_trading_system/scripts/build_binance_runup_artifact.py) | .py | 76216 / 1203 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/build_unlock_feature_panel.py](F:/9_Crypto/crypto_trading_system/scripts/build_unlock_feature_panel.py) | .py | 6937 / 162 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/build_xs_research_panel.py](F:/9_Crypto/crypto_trading_system/scripts/build_xs_research_panel.py) | .py | 2747 / 66 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/buyback_fetch.py](F:/9_Crypto/crypto_trading_system/scripts/buyback_fetch.py) | .py | 2719 / 45 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/buyback_yield_study.py](F:/9_Crypto/crypto_trading_system/scripts/buyback_yield_study.py) | .py | 4195 / 61 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/check_backtest_supported_strategies.py](F:/9_Crypto/crypto_trading_system/scripts/check_backtest_supported_strategies.py) | .py | 5366 / 145 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/check_config_contract.ps1](F:/9_Crypto/crypto_trading_system/scripts/check_config_contract.ps1) | .ps1 | 8018 / 136 | S：PS AST通过；D/V依脚本副作用范围 |
| [scripts/check_native_runtime.py](F:/9_Crypto/crypto_trading_system/scripts/check_native_runtime.py) | .py | 2009 / 63 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/check_second_round_intraday_strategies.py](F:/9_Crypto/crypto_trading_system/scripts/check_second_round_intraday_strategies.py) | .py | 4625 / 122 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/check_top5_intraday_strategies.py](F:/9_Crypto/crypto_trading_system/scripts/check_top5_intraday_strategies.py) | .py | 3056 / 88 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/clean_empty_logs.ps1](F:/9_Crypto/crypto_trading_system/scripts/clean_empty_logs.ps1) | .ps1 | 943 / 32 | S：PS AST通过；D/V依脚本副作用范围 |
| [scripts/cleanup_repo.ps1](F:/9_Crypto/crypto_trading_system/scripts/cleanup_repo.ps1) | .ps1 | 10124 / 310 | S：PS AST通过；D/V依脚本副作用范围 |
| [scripts/debug_gate_balances.py](F:/9_Crypto/crypto_trading_system/scripts/debug_gate_balances.py) | .py | 2054 / 75 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/decay_short_study.py](F:/9_Crypto/crypto_trading_system/scripts/decay_short_study.py) | .py | 10541 / 264 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/delist_risk_fetch.py](F:/9_Crypto/crypto_trading_system/scripts/delist_risk_fetch.py) | .py | 1974 / 35 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/delist_risk_study.py](F:/9_Crypto/crypto_trading_system/scripts/delist_risk_study.py) | .py | 5174 / 76 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/dev_web.ps1](F:/9_Crypto/crypto_trading_system/scripts/dev_web.ps1) | .ps1 | 3266 / 111 | S：PS AST通过；D/V依脚本副作用范围 |
| [scripts/discover_coinglass_capabilities.py](F:/9_Crypto/crypto_trading_system/scripts/discover_coinglass_capabilities.py) | .py | 526 / 21 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/download_data.py](F:/9_Crypto/crypto_trading_system/scripts/download_data.py) | .py | 7902 / 199 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/download_tick_data.py](F:/9_Crypto/crypto_trading_system/scripts/download_tick_data.py) | .py | 7698 / 223 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/ensure_research_universe_refresh_task.ps1](F:/9_Crypto/crypto_trading_system/scripts/ensure_research_universe_refresh_task.ps1) | .ps1 | 7473 / 191 | S：PS AST通过；D/V依脚本副作用范围 |
| [scripts/ensure_web_supervisor_task.ps1](F:/9_Crypto/crypto_trading_system/scripts/ensure_web_supervisor_task.ps1) | .ps1 | 6872 / 162 | S：PS AST通过；D/V依脚本副作用范围 |
| [scripts/evaluate_market_ws_shadow_report.py](F:/9_Crypto/crypto_trading_system/scripts/evaluate_market_ws_shadow_report.py) | .py | 22425 / 460 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/exchange_event_studies.py](F:/9_Crypto/crypto_trading_system/scripts/exchange_event_studies.py) | .py | 10698 / 226 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/extend_binance_runup_artifact_with_exits.py](F:/9_Crypto/crypto_trading_system/scripts/extend_binance_runup_artifact_with_exits.py) | .py | 14810 / 295 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/fee_momentum_factor.py](F:/9_Crypto/crypto_trading_system/scripts/fee_momentum_factor.py) | .py | 7004 / 141 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/finalize_binance_continuation_utility.py](F:/9_Crypto/crypto_trading_system/scripts/finalize_binance_continuation_utility.py) | .py | 3349 / 90 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/finalize_binance_entry_exit_timing.py](F:/9_Crypto/crypto_trading_system/scripts/finalize_binance_entry_exit_timing.py) | .py | 29068 / 486 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/finalize_binance_exit_strategies.py](F:/9_Crypto/crypto_trading_system/scripts/finalize_binance_exit_strategies.py) | .py | 1317 / 34 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/finalize_binance_intensity_exit_frontier.py](F:/9_Crypto/crypto_trading_system/scripts/finalize_binance_intensity_exit_frontier.py) | .py | 9478 / 153 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/finalize_binance_multisource_report.py](F:/9_Crypto/crypto_trading_system/scripts/finalize_binance_multisource_report.py) | .py | 24717 / 426 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/finalize_binance_postlaunch_study.py](F:/9_Crypto/crypto_trading_system/scripts/finalize_binance_postlaunch_study.py) | .py | 14565 / 256 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/finalize_binance_runup_extended.py](F:/9_Crypto/crypto_trading_system/scripts/finalize_binance_runup_extended.py) | .py | 5537 / 126 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/finalize_binance_sequential_strategy.py](F:/9_Crypto/crypto_trading_system/scripts/finalize_binance_sequential_strategy.py) | .py | 29904 / 507 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/finalize_binance_strategy_frontier.py](F:/9_Crypto/crypto_trading_system/scripts/finalize_binance_strategy_frontier.py) | .py | 16536 / 277 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/finalize_binance_utility_breakout_add.py](F:/9_Crypto/crypto_trading_system/scripts/finalize_binance_utility_breakout_add.py) | .py | 10455 / 180 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/finalize_binance_utility_breakout_hold_router.py](F:/9_Crypto/crypto_trading_system/scripts/finalize_binance_utility_breakout_hold_router.py) | .py | 12090 / 200 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/finalize_binance_utility_entry_frontier.py](F:/9_Crypto/crypto_trading_system/scripts/finalize_binance_utility_entry_frontier.py) | .py | 11029 / 181 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/finalize_binance_utility_entry_timing.py](F:/9_Crypto/crypto_trading_system/scripts/finalize_binance_utility_entry_timing.py) | .py | 12262 / 200 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/finalize_binance_utility_exhaustion_attribution.py](F:/9_Crypto/crypto_trading_system/scripts/finalize_binance_utility_exhaustion_attribution.py) | .py | 11062 / 195 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/finalize_binance_utility_exhaustion_exit.py](F:/9_Crypto/crypto_trading_system/scripts/finalize_binance_utility_exhaustion_exit.py) | .py | 8497 / 146 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/finalize_binance_utility_intensity_router.py](F:/9_Crypto/crypto_trading_system/scripts/finalize_binance_utility_intensity_router.py) | .py | 7608 / 129 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/finalize_binance_utility_strategy_iteration.py](F:/9_Crypto/crypto_trading_system/scripts/finalize_binance_utility_strategy_iteration.py) | .py | 21611 / 320 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/finalize_binance_utility_taker_flow_filter.py](F:/9_Crypto/crypto_trading_system/scripts/finalize_binance_utility_taker_flow_filter.py) | .py | 11986 / 199 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/generate_pump_watchlist.py](F:/9_Crypto/crypto_trading_system/scripts/generate_pump_watchlist.py) | .py | 22175 / 499 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/governance_event_study.py](F:/9_Crypto/crypto_trading_system/scripts/governance_event_study.py) | .py | 4432 / 72 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/hack_event_study.py](F:/9_Crypto/crypto_trading_system/scripts/hack_event_study.py) | .py | 5198 / 109 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/intraday_15m_lab.py](F:/9_Crypto/crypto_trading_system/scripts/intraday_15m_lab.py) | .py | 20477 / 471 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/legacy/fix_mojibake.py](F:/9_Crypto/crypto_trading_system/scripts/legacy/fix_mojibake.py) | .py | 10699 / 264 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/listing_effect_study.py](F:/9_Crypto/crypto_trading_system/scripts/listing_effect_study.py) | .py | 6849 / 176 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/listing_short_horizons.py](F:/9_Crypto/crypto_trading_system/scripts/listing_short_horizons.py) | .py | 2979 / 68 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/maintain_all_binance_market_data.py](F:/9_Crypto/crypto_trading_system/scripts/maintain_all_binance_market_data.py) | .py | 40318 / 1029 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/maintain_research_universe_data.py](F:/9_Crypto/crypto_trading_system/scripts/maintain_research_universe_data.py) | .py | 23631 / 640 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/maintain_top100_data.py](F:/9_Crypto/crypto_trading_system/scripts/maintain_top100_data.py) | .py | 12845 / 387 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/market_ws_live_shadow.ps1](F:/9_Crypto/crypto_trading_system/scripts/market_ws_live_shadow.ps1) | .ps1 | 17281 / 451 | S：PS AST通过；D/V依脚本副作用范围 |
| [scripts/market_ws_paper_shadow.ps1](F:/9_Crypto/crypto_trading_system/scripts/market_ws_paper_shadow.ps1) | .ps1 | 14990 / 422 | S：PS AST通过；D/V依脚本副作用范围 |
| [scripts/market_ws_strategy_primary.ps1](F:/9_Crypto/crypto_trading_system/scripts/market_ws_strategy_primary.ps1) | .ps1 | 29258 / 662 | S：PS AST通过；D/V依脚本副作用范围 |
| [scripts/market_ws_ui_primary.ps1](F:/9_Crypto/crypto_trading_system/scripts/market_ws_ui_primary.ps1) | .ps1 | 26034 / 614 | S：PS AST通过；D/V依脚本副作用范围 |
| [scripts/migrate_parquet_klines_to_utc.py](F:/9_Crypto/crypto_trading_system/scripts/migrate_parquet_klines_to_utc.py) | .py | 5982 / 169 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/model_robustness_addendum.py](F:/9_Crypto/crypto_trading_system/scripts/model_robustness_addendum.py) | .py | 5120 / 128 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/model_robustness_audit.py](F:/9_Crypto/crypto_trading_system/scripts/model_robustness_audit.py) | .py | 7728 / 171 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/news_llm_latency_curve.py](F:/9_Crypto/crypto_trading_system/scripts/news_llm_latency_curve.py) | .py | 3769 / 55 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/news_llm_signal_edge.py](F:/9_Crypto/crypto_trading_system/scripts/news_llm_signal_edge.py) | .py | 7049 / 157 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/news_llm_vol_forecast.py](F:/9_Crypto/crypto_trading_system/scripts/news_llm_vol_forecast.py) | .py | 4126 / 59 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/news_self_check.py](F:/9_Crypto/crypto_trading_system/scripts/news_self_check.py) | .py | 1569 / 44 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/oneclick_ai_research_deploy.py](F:/9_Crypto/crypto_trading_system/scripts/oneclick_ai_research_deploy.py) | .py | 8070 / 202 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/phase8_before_after.py](F:/9_Crypto/crypto_trading_system/scripts/phase8_before_after.py) | .py | 12119 / 298 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/plot_binance_continuation_utility.py](F:/9_Crypto/crypto_trading_system/scripts/plot_binance_continuation_utility.py) | .py | 3487 / 80 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/polymarket_clob_setup.py](F:/9_Crypto/crypto_trading_system/scripts/polymarket_clob_setup.py) | .py | 16207 / 399 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/pre_release.ps1](F:/9_Crypto/crypto_trading_system/scripts/pre_release.ps1) | .ps1 | 5312 / 162 | S：PS AST通过；D/V依脚本副作用范围 |
| [scripts/precheck_market_ws_live_shadow.py](F:/9_Crypto/crypto_trading_system/scripts/precheck_market_ws_live_shadow.py) | .py | 12966 / 283 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/prepare_second_level_data.py](F:/9_Crypto/crypto_trading_system/scripts/prepare_second_level_data.py) | .py | 4959 / 125 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/prepare_year_seconds_and_research.py](F:/9_Crypto/crypto_trading_system/scripts/prepare_year_seconds_and_research.py) | .py | 5421 / 137 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/prune_logs.ps1](F:/9_Crypto/crypto_trading_system/scripts/prune_logs.ps1) | .ps1 | 3148 / 95 | S：PS AST通过；D/V依脚本副作用范围 |
| [scripts/pump_precursor_panel.py](F:/9_Crypto/crypto_trading_system/scripts/pump_precursor_panel.py) | .py | 10712 / 255 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/quarantine_research_test_data.py](F:/9_Crypto/crypto_trading_system/scripts/quarantine_research_test_data.py) | .py | 6538 / 124 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/quick_test_binance.py](F:/9_Crypto/crypto_trading_system/scripts/quick_test_binance.py) | .py | 1331 / 34 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/refresh_binance_evidence_manifest.py](F:/9_Crypto/crypto_trading_system/scripts/refresh_binance_evidence_manifest.py) | .py | 2121 / 57 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/refresh_coinglass_incremental.py](F:/9_Crypto/crypto_trading_system/scripts/refresh_coinglass_incremental.py) | .py | 1555 / 39 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/regime_gate_study.py](F:/9_Crypto/crypto_trading_system/scripts/regime_gate_study.py) | .py | 6124 / 154 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/research/__init__.py](F:/9_Crypto/crypto_trading_system/scripts/research/__init__.py) | .py | 74 / 2 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/research/_common.py](F:/9_Crypto/crypto_trading_system/scripts/research/_common.py) | .py | 4437 / 111 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/research/all_reports.py](F:/9_Crypto/crypto_trading_system/scripts/research/all_reports.py) | .py | 1168 / 39 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/research/audit_universe30_local_data.py](F:/9_Crypto/crypto_trading_system/scripts/research/audit_universe30_local_data.py) | .py | 16924 / 397 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/research/cost_sensitivity.py](F:/9_Crypto/crypto_trading_system/scripts/research/cost_sensitivity.py) | .py | 3375 / 82 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/research/data_qa.py](F:/9_Crypto/crypto_trading_system/scripts/research/data_qa.py) | .py | 2146 / 54 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/research/event_study_polymarket.py](F:/9_Crypto/crypto_trading_system/scripts/research/event_study_polymarket.py) | .py | 4760 / 98 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/research/factor_study.py](F:/9_Crypto/crypto_trading_system/scripts/research/factor_study.py) | .py | 3012 / 73 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/research/feature_ablation_polymarket.py](F:/9_Crypto/crypto_trading_system/scripts/research/feature_ablation_polymarket.py) | .py | 4040 / 78 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/research/paper_replay_batch_polymarket.py](F:/9_Crypto/crypto_trading_system/scripts/research/paper_replay_batch_polymarket.py) | .py | 4063 / 83 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/research/paper_replay_grid_polymarket.py](F:/9_Crypto/crypto_trading_system/scripts/research/paper_replay_grid_polymarket.py) | .py | 4239 / 91 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/research/paper_replay_polymarket.py](F:/9_Crypto/crypto_trading_system/scripts/research/paper_replay_polymarket.py) | .py | 2994 / 75 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/research/paper_replay_walk_forward_polymarket.py](F:/9_Crypto/crypto_trading_system/scripts/research/paper_replay_walk_forward_polymarket.py) | .py | 4766 / 100 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/research/paper_strategy_once_polymarket.py](F:/9_Crypto/crypto_trading_system/scripts/research/paper_strategy_once_polymarket.py) | .py | 3495 / 73 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/research/promote_polymarket_profile.py](F:/9_Crypto/crypto_trading_system/scripts/research/promote_polymarket_profile.py) | .py | 2390 / 50 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/research/pull_funding_cache.py](F:/9_Crypto/crypto_trading_system/scripts/research/pull_funding_cache.py) | .py | 2534 / 70 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/research/robustness.py](F:/9_Crypto/crypto_trading_system/scripts/research/robustness.py) | .py | 3055 / 77 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/research/walk_forward.py](F:/9_Crypto/crypto_trading_system/scripts/research/walk_forward.py) | .py | 4241 / 94 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/retest_ignition_5m.py](F:/9_Crypto/crypto_trading_system/scripts/retest_ignition_5m.py) | .py | 10199 / 263 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/retest_llm_strategy_programs.py](F:/9_Crypto/crypto_trading_system/scripts/retest_llm_strategy_programs.py) | .py | 9102 / 199 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/review_binance_runup_forward_monitor.py](F:/9_Crypto/crypto_trading_system/scripts/review_binance_runup_forward_monitor.py) | .py | 7200 / 161 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/run_all_binance_data_refresh.ps1](F:/9_Crypto/crypto_trading_system/scripts/run_all_binance_data_refresh.ps1) | .ps1 | 989 / 32 | S：PS AST通过；D/V依脚本副作用范围 |
| [scripts/run_exit_refactor.py](F:/9_Crypto/crypto_trading_system/scripts/run_exit_refactor.py) | .py | 31170 / 730 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/run_final_solution_research.py](F:/9_Crypto/crypto_trading_system/scripts/run_final_solution_research.py) | .py | 10862 / 281 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/run_ops_api.py](F:/9_Crypto/crypto_trading_system/scripts/run_ops_api.py) | .py | 283 / 11 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/run_paper_session.py](F:/9_Crypto/crypto_trading_system/scripts/run_paper_session.py) | .py | 7277 / 188 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/run_polymarket_worker.py](F:/9_Crypto/crypto_trading_system/scripts/run_polymarket_worker.py) | .py | 327 / 15 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/run_pump_watchlist_weekly.bat](F:/9_Crypto/crypto_trading_system/scripts/run_pump_watchlist_weekly.bat) | .bat | 1208 / 18 | S：登记/类型与引用范围 |
| [scripts/run_research_universe_refresh.ps1](F:/9_Crypto/crypto_trading_system/scripts/run_research_universe_refresh.ps1) | .ps1 | 9542 / 255 | S：PS AST通过；D/V依脚本副作用范围 |
| [scripts/run_strategy_research.py](F:/9_Crypto/crypto_trading_system/scripts/run_strategy_research.py) | .py | 3591 / 93 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/run_top100_universe_research.py](F:/9_Crypto/crypto_trading_system/scripts/run_top100_universe_research.py) | .py | 25721 / 685 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/selfcheck_market_ws_shadow.py](F:/9_Crypto/crypto_trading_system/scripts/selfcheck_market_ws_shadow.py) | .py | 39097 / 748 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/selfcheck_openclaw_ops.py](F:/9_Crypto/crypto_trading_system/scripts/selfcheck_openclaw_ops.py) | .py | 2337 / 61 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/selfcheck_opennews.py](F:/9_Crypto/crypto_trading_system/scripts/selfcheck_opennews.py) | .py | 3686 / 115 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/selfcheck_paper_longrun.py](F:/9_Crypto/crypto_trading_system/scripts/selfcheck_paper_longrun.py) | .py | 12672 / 321 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/selfcheck_polymarket.py](F:/9_Crypto/crypto_trading_system/scripts/selfcheck_polymarket.py) | .py | 3875 / 84 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/setup_local_env.ps1](F:/9_Crypto/crypto_trading_system/scripts/setup_local_env.ps1) | .ps1 | 2381 / 52 | S：PS AST通过；D/V依脚本副作用范围 |
| [scripts/snapshot_alpha_fundamentals.py](F:/9_Crypto/crypto_trading_system/scripts/snapshot_alpha_fundamentals.py) | .py | 5504 / 132 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/snapshot_onchain_features.py](F:/9_Crypto/crypto_trading_system/scripts/snapshot_onchain_features.py) | .py | 12968 / 322 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/standalone_strategy_miner.py](F:/9_Crypto/crypto_trading_system/scripts/standalone_strategy_miner.py) | .py | 24006 / 645 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/start_live_shadow_news.ps1](F:/9_Crypto/crypto_trading_system/scripts/start_live_shadow_news.ps1) | .ps1 | 9618 / 273 | S：PS AST通过；D/V依脚本副作用范围 |
| [scripts/start_web.py](F:/9_Crypto/crypto_trading_system/scripts/start_web.py) | .py | 2031 / 65 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/start_web_ps.ps1](F:/9_Crypto/crypto_trading_system/scripts/start_web_ps.ps1) | .ps1 | 1576 / 51 | S：PS AST通过；D/V依脚本副作用范围 |
| [scripts/stop_loss_sweep.py](F:/9_Crypto/crypto_trading_system/scripts/stop_loss_sweep.py) | .py | 3428 / 68 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/supervise_web.ps1](F:/9_Crypto/crypto_trading_system/scripts/supervise_web.ps1) | .ps1 | 13852 / 315 | S：PS AST通过；D/V依脚本副作用范围 |
| [scripts/supply_inflation_factor.py](F:/9_Crypto/crypto_trading_system/scripts/supply_inflation_factor.py) | .py | 5657 / 79 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/test.ps1](F:/9_Crypto/crypto_trading_system/scripts/test.ps1) | .ps1 | 2168 / 75 | S：PS AST通过；D/V依脚本副作用范围 |
| [scripts/test_api_direct.py](F:/9_Crypto/crypto_trading_system/scripts/test_api_direct.py) | .py | 7115 / 193 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/test_binance_alternatives.py](F:/9_Crypto/crypto_trading_system/scripts/test_binance_alternatives.py) | .py | 8540 / 257 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/test_binance_trading.py](F:/9_Crypto/crypto_trading_system/scripts/test_binance_trading.py) | .py | 7552 / 182 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/test_data_sources.py](F:/9_Crypto/crypto_trading_system/scripts/test_data_sources.py) | .py | 5439 / 167 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/test_live_link_all_strategies.py](F:/9_Crypto/crypto_trading_system/scripts/test_live_link_all_strategies.py) | .py | 10596 / 272 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/test_system.py](F:/9_Crypto/crypto_trading_system/scripts/test_system.py) | .py | 6734 / 186 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/train_ml_signal.py](F:/9_Crypto/crypto_trading_system/scripts/train_ml_signal.py) | .py | 9787 / 262 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/unlock_candidate_findings.py](F:/9_Crypto/crypto_trading_system/scripts/unlock_candidate_findings.py) | .py | 4588 / 102 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/unlock_event_study.py](F:/9_Crypto/crypto_trading_system/scripts/unlock_event_study.py) | .py | 10144 / 212 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/unlock_matched_control.py](F:/9_Crypto/crypto_trading_system/scripts/unlock_matched_control.py) | .py | 5312 / 112 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/unlock_short_backtest.py](F:/9_Crypto/crypto_trading_system/scripts/unlock_short_backtest.py) | .py | 10274 / 218 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/upbit_event_study.py](F:/9_Crypto/crypto_trading_system/scripts/upbit_event_study.py) | .py | 6300 / 133 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/upbit_reason_split.py](F:/9_Crypto/crypto_trading_system/scripts/upbit_reason_split.py) | .py | 2848 / 62 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/upbit_short_backtest.py](F:/9_Crypto/crypto_trading_system/scripts/upbit_short_backtest.py) | .py | 6685 / 129 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/verify_binance_continuation_entry.py](F:/9_Crypto/crypto_trading_system/scripts/verify_binance_continuation_entry.py) | .py | 6621 / 144 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/verify_binance_entry_exit_timing.py](F:/9_Crypto/crypto_trading_system/scripts/verify_binance_entry_exit_timing.py) | .py | 11070 / 197 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/verify_binance_exit_strategies.py](F:/9_Crypto/crypto_trading_system/scripts/verify_binance_exit_strategies.py) | .py | 11839 / 247 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/verify_binance_forward_research_pipeline.py](F:/9_Crypto/crypto_trading_system/scripts/verify_binance_forward_research_pipeline.py) | .py | 8524 / 196 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/verify_binance_intensity_exit_frontier.py](F:/9_Crypto/crypto_trading_system/scripts/verify_binance_intensity_exit_frontier.py) | .py | 7363 / 145 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/verify_binance_multisource_factors.py](F:/9_Crypto/crypto_trading_system/scripts/verify_binance_multisource_factors.py) | .py | 6104 / 129 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/verify_binance_runup_extended.py](F:/9_Crypto/crypto_trading_system/scripts/verify_binance_runup_extended.py) | .py | 10890 / 221 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/verify_binance_sequential_strategy.py](F:/9_Crypto/crypto_trading_system/scripts/verify_binance_sequential_strategy.py) | .py | 20504 / 336 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/verify_binance_utility_breakout_add.py](F:/9_Crypto/crypto_trading_system/scripts/verify_binance_utility_breakout_add.py) | .py | 6686 / 131 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/verify_binance_utility_breakout_hold_router.py](F:/9_Crypto/crypto_trading_system/scripts/verify_binance_utility_breakout_hold_router.py) | .py | 7784 / 157 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/verify_binance_utility_entry_frontier.py](F:/9_Crypto/crypto_trading_system/scripts/verify_binance_utility_entry_frontier.py) | .py | 6452 / 129 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/verify_binance_utility_entry_timing.py](F:/9_Crypto/crypto_trading_system/scripts/verify_binance_utility_entry_timing.py) | .py | 7558 / 147 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/verify_binance_utility_exhaustion_attribution.py](F:/9_Crypto/crypto_trading_system/scripts/verify_binance_utility_exhaustion_attribution.py) | .py | 7867 / 159 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/verify_binance_utility_exhaustion_exit.py](F:/9_Crypto/crypto_trading_system/scripts/verify_binance_utility_exhaustion_exit.py) | .py | 5224 / 118 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/verify_binance_utility_intensity_router.py](F:/9_Crypto/crypto_trading_system/scripts/verify_binance_utility_intensity_router.py) | .py | 6084 / 128 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/verify_binance_utility_strategy_iteration.py](F:/9_Crypto/crypto_trading_system/scripts/verify_binance_utility_strategy_iteration.py) | .py | 16754 / 317 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/verify_binance_utility_taker_flow_filter.py](F:/9_Crypto/crypto_trading_system/scripts/verify_binance_utility_taker_flow_filter.py) | .py | 8987 / 183 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/verify_env.py](F:/9_Crypto/crypto_trading_system/scripts/verify_env.py) | .py | 3805 / 112 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/verify_exit_logic_overhaul.py](F:/9_Crypto/crypto_trading_system/scripts/verify_exit_logic_overhaul.py) | .py | 18294 / 446 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/verify_phase_f_acceptance.py](F:/9_Crypto/crypto_trading_system/scripts/verify_phase_f_acceptance.py) | .py | 5869 / 137 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/verify_post_change_live_exits.py](F:/9_Crypto/crypto_trading_system/scripts/verify_post_change_live_exits.py) | .py | 13878 / 384 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/vesting_llm_compare.py](F:/9_Crypto/crypto_trading_system/scripts/vesting_llm_compare.py) | .py | 4188 / 88 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/vesting_llm_discover.py](F:/9_Crypto/crypto_trading_system/scripts/vesting_llm_discover.py) | .py | 12560 / 249 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/vesting_llm_extract.py](F:/9_Crypto/crypto_trading_system/scripts/vesting_llm_extract.py) | .py | 4490 / 80 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/vesting_llm_factor_backtest.py](F:/9_Crypto/crypto_trading_system/scripts/vesting_llm_factor_backtest.py) | .py | 5479 / 117 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/vesting_llm_fetch.py](F:/9_Crypto/crypto_trading_system/scripts/vesting_llm_fetch.py) | .py | 5367 / 96 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [scripts/web.ps1](F:/9_Crypto/crypto_trading_system/scripts/web.ps1) | .ps1 | 29515 / 725 | S：PS AST通过；D/V依脚本副作用范围 |
| [strategies/__init__.py](F:/9_Crypto/crypto_trading_system/strategies/__init__.py) | .py | 8488 / 299 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [strategies/ai/__init__.py](F:/9_Crypto/crypto_trading_system/strategies/ai/__init__.py) | .py | 137 / 5 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [strategies/ai/ml_xgboost_strategy.py](F:/9_Crypto/crypto_trading_system/strategies/ai/ml_xgboost_strategy.py) | .py | 4830 / 130 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [strategies/arbitrage/__init__.py](F:/9_Crypto/crypto_trading_system/strategies/arbitrage/__init__.py) | .py | 549 / 22 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [strategies/arbitrage/cex_arbitrage.py](F:/9_Crypto/crypto_trading_system/strategies/arbitrage/cex_arbitrage.py) | .py | 28756 / 700 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [strategies/arbitrage/dex_arbitrage.py](F:/9_Crypto/crypto_trading_system/strategies/arbitrage/dex_arbitrage.py) | .py | 13661 / 376 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [strategies/event_driven/__init__.py](F:/9_Crypto/crypto_trading_system/strategies/event_driven/__init__.py) | .py | 144 / 4 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [strategies/event_driven/supply_event_strategy.py](F:/9_Crypto/crypto_trading_system/strategies/event_driven/supply_event_strategy.py) | .py | 8778 / 208 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [strategies/factor_based/__init__.py](F:/9_Crypto/crypto_trading_system/strategies/factor_based/__init__.py) | .py | 1507 / 72 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [strategies/factor_based/factor_strategies.py](F:/9_Crypto/crypto_trading_system/strategies/factor_based/factor_strategies.py) | .py | 79414 / 1968 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [strategies/macro/__init__.py](F:/9_Crypto/crypto_trading_system/strategies/macro/__init__.py) | .py | 557 / 22 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [strategies/macro/fund_flow.py](F:/9_Crypto/crypto_trading_system/strategies/macro/fund_flow.py) | .py | 19873 / 503 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [strategies/macro/kol_consensus.py](F:/9_Crypto/crypto_trading_system/strategies/macro/kol_consensus.py) | .py | 7062 / 164 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [strategies/macro/market_sentiment.py](F:/9_Crypto/crypto_trading_system/strategies/macro/market_sentiment.py) | .py | 19448 / 479 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [strategies/macro/onchain_flow_regime.py](F:/9_Crypto/crypto_trading_system/strategies/macro/onchain_flow_regime.py) | .py | 7279 / 168 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [strategies/quantitative/__init__.py](F:/9_Crypto/crypto_trading_system/strategies/quantitative/__init__.py) | .py | 2848 / 80 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [strategies/quantitative/altcoin_downtrend_bounce_short.py](F:/9_Crypto/crypto_trading_system/strategies/quantitative/altcoin_downtrend_bounce_short.py) | .py | 8287 / 218 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [strategies/quantitative/fama_factor_arbitrage.py](F:/9_Crypto/crypto_trading_system/strategies/quantitative/fama_factor_arbitrage.py) | .py | 16080 / 432 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [strategies/quantitative/intraday_cross_section.py](F:/9_Crypto/crypto_trading_system/strategies/quantitative/intraday_cross_section.py) | .py | 72651 / 1700 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [strategies/quantitative/liquidation_oi_crowding.py](F:/9_Crypto/crypto_trading_system/strategies/quantitative/liquidation_oi_crowding.py) | .py | 9011 / 204 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [strategies/quantitative/mean_reversion.py](F:/9_Crypto/crypto_trading_system/strategies/quantitative/mean_reversion.py) | .py | 9307 / 257 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [strategies/quantitative/momentum.py](F:/9_Crypto/crypto_trading_system/strategies/quantitative/momentum.py) | .py | 9286 / 243 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [strategies/quantitative/multi_factor_hf.py](F:/9_Crypto/crypto_trading_system/strategies/quantitative/multi_factor_hf.py) | .py | 14240 / 331 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [strategies/quantitative/multi_factor_hf_fast.py](F:/9_Crypto/crypto_trading_system/strategies/quantitative/multi_factor_hf_fast.py) | .py | 12370 / 312 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [strategies/quantitative/oi_mcap_ambush.py](F:/9_Crypto/crypto_trading_system/strategies/quantitative/oi_mcap_ambush.py) | .py | 19487 / 474 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [strategies/quantitative/pairs_trading.py](F:/9_Crypto/crypto_trading_system/strategies/quantitative/pairs_trading.py) | .py | 15311 / 351 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [strategies/technical/__init__.py](F:/9_Crypto/crypto_trading_system/strategies/technical/__init__.py) | .py | 847 / 28 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [strategies/technical/bollinger_strategy.py](F:/9_Crypto/crypto_trading_system/strategies/technical/bollinger_strategy.py) | .py | 15752 / 394 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [strategies/technical/common_strategies.py](F:/9_Crypto/crypto_trading_system/strategies/technical/common_strategies.py) | .py | 14934 / 383 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [strategies/technical/ma_strategy.py](F:/9_Crypto/crypto_trading_system/strategies/technical/ma_strategy.py) | .py | 11928 / 313 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [strategies/technical/macd_strategy.py](F:/9_Crypto/crypto_trading_system/strategies/technical/macd_strategy.py) | .py | 10294 / 275 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [strategies/technical/rsi_strategy.py](F:/9_Crypto/crypto_trading_system/strategies/technical/rsi_strategy.py) | .py | 15604 / 355 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/__init__.py](F:/9_Crypto/crypto_trading_system/tests/__init__.py) | .py | 21 / 3 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/agent_model_feedback.cjs](F:/9_Crypto/crypto_trading_system/tests/agent_model_feedback.cjs) | .cjs | 1401 / 21 | S：node语法通过；D/V依前端边界范围 |
| [tests/ai_agent_workspace_smoke.cjs](F:/9_Crypto/crypto_trading_system/tests/ai_agent_workspace_smoke.cjs) | .cjs | 10883 / 144 | S：node语法通过；D/V依前端边界范围 |
| [tests/conftest.py](F:/9_Crypto/crypto_trading_system/tests/conftest.py) | .py | 5612 / 127 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/core/test_asyncio_compat.py](F:/9_Crypto/crypto_trading_system/tests/core/test_asyncio_compat.py) | .py | 1521 / 52 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/core/test_env_utils.py](F:/9_Crypto/crypto_trading_system/tests/core/test_env_utils.py) | .py | 2120 / 61 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/core/test_paper_trading_callbacks.py](F:/9_Crypto/crypto_trading_system/tests/core/test_paper_trading_callbacks.py) | .py | 1641 / 54 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/core/test_runtime_persistence.py](F:/9_Crypto/crypto_trading_system/tests/core/test_runtime_persistence.py) | .py | 17973 / 512 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/core/test_strategy_summary_persistence.py](F:/9_Crypto/crypto_trading_system/tests/core/test_strategy_summary_persistence.py) | .py | 7573 / 244 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/dashboard_attribution_coverage.cjs](F:/9_Crypto/crypto_trading_system/tests/dashboard_attribution_coverage.cjs) | .cjs | 2769 / 40 | S：node语法通过；D/V依前端边界范围 |
| [tests/governance/test_api_user_security.py](F:/9_Crypto/crypto_trading_system/tests/governance/test_api_user_security.py) | .py | 2596 / 88 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/governance/test_governance_gates.py](F:/9_Crypto/crypto_trading_system/tests/governance/test_governance_gates.py) | .py | 13508 / 380 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/governance/test_llm_research_output_schema.py](F:/9_Crypto/crypto_trading_system/tests/governance/test_llm_research_output_schema.py) | .py | 2254 / 52 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/ops/conftest.py](F:/9_Crypto/crypto_trading_system/tests/ops/conftest.py) | .py | 1221 / 45 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/ops/test_ops_ai_proposals.py](F:/9_Crypto/crypto_trading_system/tests/ops/test_ops_ai_proposals.py) | .py | 10388 / 255 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/ops/test_ops_audit_jsonl.py](F:/9_Crypto/crypto_trading_system/tests/ops/test_ops_audit_jsonl.py) | .py | 877 / 28 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/ops/test_ops_auth.py](F:/9_Crypto/crypto_trading_system/tests/ops/test_ops_auth.py) | .py | 614 / 19 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/ops/test_ops_live_approval.py](F:/9_Crypto/crypto_trading_system/tests/ops/test_ops_live_approval.py) | .py | 1835 / 56 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/ops/test_ops_news_bridge.py](F:/9_Crypto/crypto_trading_system/tests/ops/test_ops_news_bridge.py) | .py | 2561 / 70 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/ops/test_ops_rbac_regressions.py](F:/9_Crypto/crypto_trading_system/tests/ops/test_ops_rbac_regressions.py) | .py | 1788 / 64 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/ops/test_ops_research_jobs.py](F:/9_Crypto/crypto_trading_system/tests/ops/test_ops_research_jobs.py) | .py | 2028 / 58 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/ops/test_ops_status_resilience.py](F:/9_Crypto/crypto_trading_system/tests/ops/test_ops_status_resilience.py) | .py | 988 / 25 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/playwright_smoke.spec.js](F:/9_Crypto/crypto_trading_system/tests/playwright_smoke.spec.js) | .js | 5334 / 129 | S：node语法通过；D/V依前端边界范围 |
| [tests/polymarket/test_clob_reader.py](F:/9_Crypto/crypto_trading_system/tests/polymarket/test_clob_reader.py) | .py | 2364 / 78 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/polymarket/test_db_config.py](F:/9_Crypto/crypto_trading_system/tests/polymarket/test_db_config.py) | .py | 1338 / 37 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/polymarket/test_event_study_polymarket.py](F:/9_Crypto/crypto_trading_system/tests/polymarket/test_event_study_polymarket.py) | .py | 1741 / 45 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/polymarket/test_features.py](F:/9_Crypto/crypto_trading_system/tests/polymarket/test_features.py) | .py | 1139 / 30 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/polymarket/test_gamma_client.py](F:/9_Crypto/crypto_trading_system/tests/polymarket/test_gamma_client.py) | .py | 1433 / 40 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/polymarket/test_market_resolver.py](F:/9_Crypto/crypto_trading_system/tests/polymarket/test_market_resolver.py) | .py | 1387 / 35 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/polymarket/test_ops_polymarket.py](F:/9_Crypto/crypto_trading_system/tests/polymarket/test_ops_polymarket.py) | .py | 18939 / 528 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/polymarket/test_paper_replay.py](F:/9_Crypto/crypto_trading_system/tests/polymarket/test_paper_replay.py) | .py | 6536 / 194 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/polymarket/test_paper_strategy.py](F:/9_Crypto/crypto_trading_system/tests/polymarket/test_paper_strategy.py) | .py | 8408 / 237 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/polymarket/test_paper_trading.py](F:/9_Crypto/crypto_trading_system/tests/polymarket/test_paper_trading.py) | .py | 9491 / 264 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/polymarket/test_polymarket_clob_setup_script.py](F:/9_Crypto/crypto_trading_system/tests/polymarket/test_polymarket_clob_setup_script.py) | .py | 1897 / 69 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/polymarket/test_replay_grid.py](F:/9_Crypto/crypto_trading_system/tests/polymarket/test_replay_grid.py) | .py | 3302 / 92 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/polymarket/test_replay_report.py](F:/9_Crypto/crypto_trading_system/tests/polymarket/test_replay_report.py) | .py | 2709 / 74 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/polymarket/test_replay_walk_forward.py](F:/9_Crypto/crypto_trading_system/tests/polymarket/test_replay_walk_forward.py) | .py | 3415 / 88 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/polymarket/test_risk_gate_pm.py](F:/9_Crypto/crypto_trading_system/tests/polymarket/test_risk_gate_pm.py) | .py | 460 / 12 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/polymarket/test_signal_engine_pm.py](F:/9_Crypto/crypto_trading_system/tests/polymarket/test_signal_engine_pm.py) | .py | 1321 / 36 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/polymarket/test_token_universe.py](F:/9_Crypto/crypto_trading_system/tests/polymarket/test_token_universe.py) | .py | 1878 / 60 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/polymarket/test_utils.py](F:/9_Crypto/crypto_trading_system/tests/polymarket/test_utils.py) | .py | 612 / 24 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/polymarket/test_worker_once_polymarket.py](F:/9_Crypto/crypto_trading_system/tests/polymarket/test_worker_once_polymarket.py) | .py | 1278 / 30 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_account_manager_concurrency.py](F:/9_Crypto/crypto_trading_system/tests/test_account_manager_concurrency.py) | .py | 2899 / 87 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_account_scoped_live_paths.py](F:/9_Crypto/crypto_trading_system/tests/test_account_scoped_live_paths.py) | .py | 4775 / 139 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_agent_call_edge_script.py](F:/9_Crypto/crypto_trading_system/tests/test_agent_call_edge_script.py) | .py | 2686 / 66 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_ai_autonomous_agent.py](F:/9_Crypto/crypto_trading_system/tests/test_ai_autonomous_agent.py) | .py | 188342 / 4622 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_ai_autonomous_learning.py](F:/9_Crypto/crypto_trading_system/tests/test_ai_autonomous_learning.py) | .py | 7264 / 198 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_ai_live_decision_router.py](F:/9_Crypto/crypto_trading_system/tests/test_ai_live_decision_router.py) | .py | 16511 / 412 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_ai_research_autonomous_agent_api.py](F:/9_Crypto/crypto_trading_system/tests/test_ai_research_autonomous_agent_api.py) | .py | 74147 / 1929 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_ai_research_autonomy_phase1.py](F:/9_Crypto/crypto_trading_system/tests/test_ai_research_autonomy_phase1.py) | .py | 12607 / 317 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_ai_research_cleanup_api.py](F:/9_Crypto/crypto_trading_system/tests/test_ai_research_cleanup_api.py) | .py | 15404 / 403 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_ai_research_macro_warm_api.py](F:/9_Crypto/crypto_trading_system/tests/test_ai_research_macro_warm_api.py) | .py | 1925 / 51 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_ai_research_oneclick_api.py](F:/9_Crypto/crypto_trading_system/tests/test_ai_research_oneclick_api.py) | .py | 16122 / 400 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_ai_research_phase2.py](F:/9_Crypto/crypto_trading_system/tests/test_ai_research_phase2.py) | .py | 71676 / 1779 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_ai_research_phase3.py](F:/9_Crypto/crypto_trading_system/tests/test_ai_research_phase3.py) | .py | 11476 / 309 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_ai_research_phase4_runtime.py](F:/9_Crypto/crypto_trading_system/tests/test_ai_research_phase4_runtime.py) | .py | 17972 / 440 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_ai_research_phase5_ui_assets.py](F:/9_Crypto/crypto_trading_system/tests/test_ai_research_phase5_ui_assets.py) | .py | 12726 / 246 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_ai_research_runtime_and_phase_e.py](F:/9_Crypto/crypto_trading_system/tests/test_ai_research_runtime_and_phase_e.py) | .py | 67034 / 1620 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_ai_research_scheduler.py](F:/9_Crypto/crypto_trading_system/tests/test_ai_research_scheduler.py) | .py | 1538 / 56 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_aiohttp_resolver_hardening.py](F:/9_Crypto/crypto_trading_system/tests/test_aiohttp_resolver_hardening.py) | .py | 3151 / 88 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_all_binance_market_data_maintainer.py](F:/9_Crypto/crypto_trading_system/tests/test_all_binance_market_data_maintainer.py) | .py | 6213 / 175 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_alpha_market_data_integration.py](F:/9_Crypto/crypto_trading_system/tests/test_alpha_market_data_integration.py) | .py | 6947 / 204 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_altcoin_downtrend_bounce_short_strategy.py](F:/9_Crypto/crypto_trading_system/tests/test_altcoin_downtrend_bounce_short_strategy.py) | .py | 6075 / 165 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_altcoin_notification_manager.py](F:/9_Crypto/crypto_trading_system/tests/test_altcoin_notification_manager.py) | .py | 8423 / 280 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_altcoin_radar_derivatives.py](F:/9_Crypto/crypto_trading_system/tests/test_altcoin_radar_derivatives.py) | .py | 14660 / 362 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_altcoin_radar_events.py](F:/9_Crypto/crypto_trading_system/tests/test_altcoin_radar_events.py) | .py | 6920 / 188 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_altcoin_radar_events_context.py](F:/9_Crypto/crypto_trading_system/tests/test_altcoin_radar_events_context.py) | .py | 1551 / 39 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_altcoin_radar_narrative.py](F:/9_Crypto/crypto_trading_system/tests/test_altcoin_radar_narrative.py) | .py | 9643 / 246 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_altcoin_radar_perp.py](F:/9_Crypto/crypto_trading_system/tests/test_altcoin_radar_perp.py) | .py | 6061 / 180 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_altcoin_radar_ui_assets.py](F:/9_Crypto/crypto_trading_system/tests/test_altcoin_radar_ui_assets.py) | .py | 8119 / 148 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_altcoin_radar_universe.py](F:/9_Crypto/crypto_trading_system/tests/test_altcoin_radar_universe.py) | .py | 7441 / 187 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_arbitrage_check_exit.py](F:/9_Crypto/crypto_trading_system/tests/test_arbitrage_check_exit.py) | .py | 20563 / 502 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_arbitrage_readiness_api.py](F:/9_Crypto/crypto_trading_system/tests/test_arbitrage_readiness_api.py) | .py | 14590 / 389 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_arbitrage_ui_assets.py](F:/9_Crypto/crypto_trading_system/tests/test_arbitrage_ui_assets.py) | .py | 5883 / 123 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_audit_2026_06_08_followups.py](F:/9_Crypto/crypto_trading_system/tests/test_audit_2026_06_08_followups.py) | .py | 7305 / 178 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_audit_exit_reasons.py](F:/9_Crypto/crypto_trading_system/tests/test_audit_exit_reasons.py) | .py | 8932 / 232 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_audit_fixes_and_shadow_trackers.py](F:/9_Crypto/crypto_trading_system/tests/test_audit_fixes_and_shadow_trackers.py) | .py | 8779 / 173 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_audit_logger_non_blocking.py](F:/9_Crypto/crypto_trading_system/tests/test_audit_logger_non_blocking.py) | .py | 4498 / 133 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_autonomous_research_loop.py](F:/9_Crypto/crypto_trading_system/tests/test_autonomous_research_loop.py) | .py | 17889 / 373 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_backfill_close_reasons.py](F:/9_Crypto/crypto_trading_system/tests/test_backfill_close_reasons.py) | .py | 8133 / 184 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_backtest_bidirectional_positions.py](F:/9_Crypto/crypto_trading_system/tests/test_backtest_bidirectional_positions.py) | .py | 10808 / 309 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_backtest_cost_models.py](F:/9_Crypto/crypto_trading_system/tests/test_backtest_cost_models.py) | .py | 9566 / 275 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_backtest_engine_protective_exits.py](F:/9_Crypto/crypto_trading_system/tests/test_backtest_engine_protective_exits.py) | .py | 8107 / 275 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_backtest_factor_modes_ui_assets.py](F:/9_Crypto/crypto_trading_system/tests/test_backtest_factor_modes_ui_assets.py) | .py | 2849 / 64 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_backtest_pairs_ui_assets.py](F:/9_Crypto/crypto_trading_system/tests/test_backtest_pairs_ui_assets.py) | .py | 8077 / 159 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_backtest_process_pool_lifecycle.py](F:/9_Crypto/crypto_trading_system/tests/test_backtest_process_pool_lifecycle.py) | .py | 2525 / 93 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_backtest_runtime_consistency.py](F:/9_Crypto/crypto_trading_system/tests/test_backtest_runtime_consistency.py) | .py | 10207 / 259 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_backtest_sharpe_annualization.py](F:/9_Crypto/crypto_trading_system/tests/test_backtest_sharpe_annualization.py) | .py | 2835 / 82 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_base_exchange_error_log_throttle.py](F:/9_Crypto/crypto_trading_system/tests/test_base_exchange_error_log_throttle.py) | .py | 3134 / 95 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_binance_alpha.py](F:/9_Crypto/crypto_trading_system/tests/test_binance_alpha.py) | .py | 5490 / 168 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_binance_alpha_collector.py](F:/9_Crypto/crypto_trading_system/tests/test_binance_alpha_collector.py) | .py | 4742 / 150 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_binance_entry_exit_validation.py](F:/9_Crypto/crypto_trading_system/tests/test_binance_entry_exit_validation.py) | .py | 15696 / 393 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_binance_multisource_validation.py](F:/9_Crypto/crypto_trading_system/tests/test_binance_multisource_validation.py) | .py | 3987 / 107 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_binance_perp_ws_client.py](F:/9_Crypto/crypto_trading_system/tests/test_binance_perp_ws_client.py) | .py | 3861 / 126 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_binance_rest_proxy.py](F:/9_Crypto/crypto_trading_system/tests/test_binance_rest_proxy.py) | .py | 4051 / 133 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_binance_runup_validation.py](F:/9_Crypto/crypto_trading_system/tests/test_binance_runup_validation.py) | .py | 14401 / 387 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_binance_sequential_validation.py](F:/9_Crypto/crypto_trading_system/tests/test_binance_sequential_validation.py) | .py | 29595 / 752 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_ccxt_adapter_execution.py](F:/9_Crypto/crypto_trading_system/tests/test_ccxt_adapter_execution.py) | .py | 6433 / 167 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_ccxt_adapter_timestamp_units.py](F:/9_Crypto/crypto_trading_system/tests/test_ccxt_adapter_timestamp_units.py) | .py | 796 / 25 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_ccxt_pro_feed.py](F:/9_Crypto/crypto_trading_system/tests/test_ccxt_pro_feed.py) | .py | 17138 / 485 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_ccxt_pro_feed_markprice.py](F:/9_Crypto/crypto_trading_system/tests/test_ccxt_pro_feed_markprice.py) | .py | 5713 / 145 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_circuit_breaker.py](F:/9_Crypto/crypto_trading_system/tests/test_circuit_breaker.py) | .py | 15883 / 448 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_circuit_breaker_pnl_trigger.py](F:/9_Crypto/crypto_trading_system/tests/test_circuit_breaker_pnl_trigger.py) | .py | 11708 / 302 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_coinglass_altcoin.py](F:/9_Crypto/crypto_trading_system/tests/test_coinglass_altcoin.py) | .py | 6851 / 176 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_coinglass_budget_short_circuit.py](F:/9_Crypto/crypto_trading_system/tests/test_coinglass_budget_short_circuit.py) | .py | 14015 / 354 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_coinglass_dataset_normalization.py](F:/9_Crypto/crypto_trading_system/tests/test_coinglass_dataset_normalization.py) | .py | 40256 / 962 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_coinglass_news_collectors.py](F:/9_Crypto/crypto_trading_system/tests/test_coinglass_news_collectors.py) | .py | 4927 / 132 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_coinglass_read_only_overview.py](F:/9_Crypto/crypto_trading_system/tests/test_coinglass_read_only_overview.py) | .py | 1741 / 49 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_coinglass_signal.py](F:/9_Crypto/crypto_trading_system/tests/test_coinglass_signal.py) | .py | 4234 / 96 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_content_quality_static.py](F:/9_Crypto/crypto_trading_system/tests/test_content_quality_static.py) | .py | 1700 / 68 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_core_indicators.py](F:/9_Crypto/crypto_trading_system/tests/test_core_indicators.py) | .py | 4535 / 124 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_core_indicators_rolling.py](F:/9_Crypto/crypto_trading_system/tests/test_core_indicators_rolling.py) | .py | 5195 / 151 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_correlation_filter_same_strategy_diff_params.py](F:/9_Crypto/crypto_trading_system/tests/test_correlation_filter_same_strategy_diff_params.py) | .py | 1860 / 46 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_data_collector_lifecycle.py](F:/9_Crypto/crypto_trading_system/tests/test_data_collector_lifecycle.py) | .py | 3915 / 116 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_data_download_api.py](F:/9_Crypto/crypto_trading_system/tests/test_data_download_api.py) | .py | 16826 / 474 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_data_download_ui_assets.py](F:/9_Crypto/crypto_trading_system/tests/test_data_download_ui_assets.py) | .py | 3060 / 64 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_data_kline_ui_assets.py](F:/9_Crypto/crypto_trading_system/tests/test_data_kline_ui_assets.py) | .py | 1202 / 31 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_data_replay_api.py](F:/9_Crypto/crypto_trading_system/tests/test_data_replay_api.py) | .py | 2271 / 72 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_data_research_refresh_api.py](F:/9_Crypto/crypto_trading_system/tests/test_data_research_refresh_api.py) | .py | 3777 / 106 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_data_runtime_cache.py](F:/9_Crypto/crypto_trading_system/tests/test_data_runtime_cache.py) | .py | 4958 / 118 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_data_storage.py](F:/9_Crypto/crypto_trading_system/tests/test_data_storage.py) | .py | 12472 / 377 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_delist_risk.py](F:/9_Crypto/crypto_trading_system/tests/test_delist_risk.py) | .py | 2556 / 55 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_derivatives_crowding_vectorized_parity.py](F:/9_Crypto/crypto_trading_system/tests/test_derivatives_crowding_vectorized_parity.py) | .py | 4176 / 111 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_docker_model_assets.py](F:/9_Crypto/crypto_trading_system/tests/test_docker_model_assets.py) | .py | 686 / 18 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_equity_attribution.py](F:/9_Crypto/crypto_trading_system/tests/test_equity_attribution.py) | .py | 2914 / 59 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_exchange_connector_precision.py](F:/9_Crypto/crypto_trading_system/tests/test_exchange_connector_precision.py) | .py | 9374 / 293 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_exchange_guard_and_listing_tracker.py](F:/9_Crypto/crypto_trading_system/tests/test_exchange_guard_and_listing_tracker.py) | .py | 10258 / 193 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_exchange_manager_account_isolation.py](F:/9_Crypto/crypto_trading_system/tests/test_exchange_manager_account_isolation.py) | .py | 2744 / 70 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_exchange_manager_missing_retry.py](F:/9_Crypto/crypto_trading_system/tests/test_exchange_manager_missing_retry.py) | .py | 1802 / 51 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_exchange_order_fill_price.py](F:/9_Crypto/crypto_trading_system/tests/test_exchange_order_fill_price.py) | .py | 1720 / 60 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_exchanges.py](F:/9_Crypto/crypto_trading_system/tests/test_exchanges.py) | .py | 20559 / 623 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_execution_arrays_parity.py](F:/9_Crypto/crypto_trading_system/tests/test_execution_arrays_parity.py) | .py | 5592 / 150 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_execution_circuit_breaker_integration.py](F:/9_Crypto/crypto_trading_system/tests/test_execution_circuit_breaker_integration.py) | .py | 6153 / 159 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_execution_engine_ai_live_decision.py](F:/9_Crypto/crypto_trading_system/tests/test_execution_engine_ai_live_decision.py) | .py | 45006 / 1032 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_execution_engine_coinglass_filters.py](F:/9_Crypto/crypto_trading_system/tests/test_execution_engine_coinglass_filters.py) | .py | 9886 / 228 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_execution_engine_fill_accounting.py](F:/9_Crypto/crypto_trading_system/tests/test_execution_engine_fill_accounting.py) | .py | 34630 / 904 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_execution_engine_live_trade_review.py](F:/9_Crypto/crypto_trading_system/tests/test_execution_engine_live_trade_review.py) | .py | 20464 / 570 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_execution_engine_mode_context.py](F:/9_Crypto/crypto_trading_system/tests/test_execution_engine_mode_context.py) | .py | 1869 / 59 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_execution_engine_protective_levels.py](F:/9_Crypto/crypto_trading_system/tests/test_execution_engine_protective_levels.py) | .py | 24843 / 705 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_execution_engine_reconcile_sync.py](F:/9_Crypto/crypto_trading_system/tests/test_execution_engine_reconcile_sync.py) | .py | 3435 / 111 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_execution_engine_stale_position_force_close.py](F:/9_Crypto/crypto_trading_system/tests/test_execution_engine_stale_position_force_close.py) | .py | 4399 / 99 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_execution_rate_limit_policy.py](F:/9_Crypto/crypto_trading_system/tests/test_execution_rate_limit_policy.py) | .py | 2252 / 71 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_exit_engine.py](F:/9_Crypto/crypto_trading_system/tests/test_exit_engine.py) | .py | 3846 / 110 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_exit_logic_overhaul_verification.py](F:/9_Crypto/crypto_trading_system/tests/test_exit_logic_overhaul_verification.py) | .py | 646 / 14 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_experiment_registry_windows_concurrent.py](F:/9_Crypto/crypto_trading_system/tests/test_experiment_registry_windows_concurrent.py) | .py | 4038 / 115 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_factor_cache.py](F:/9_Crypto/crypto_trading_system/tests/test_factor_cache.py) | .py | 8640 / 225 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_factor_library_pending_status.py](F:/9_Crypto/crypto_trading_system/tests/test_factor_library_pending_status.py) | .py | 4169 / 116 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_factor_strategy_check_exit.py](F:/9_Crypto/crypto_trading_system/tests/test_factor_strategy_check_exit.py) | .py | 13984 / 362 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_funding_provider.py](F:/9_Crypto/crypto_trading_system/tests/test_funding_provider.py) | .py | 3611 / 90 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_funding_rate.py](F:/9_Crypto/crypto_trading_system/tests/test_funding_rate.py) | .py | 9545 / 298 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_historical_data_manager.py](F:/9_Crypto/crypto_trading_system/tests/test_historical_data_manager.py) | .py | 6333 / 186 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_infra_fixes.py](F:/9_Crypto/crypto_trading_system/tests/test_infra_fixes.py) | .py | 8754 / 245 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_intraday_cross_section_strategies.py](F:/9_Crypto/crypto_trading_system/tests/test_intraday_cross_section_strategies.py) | .py | 16185 / 419 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_kol_lsr.py](F:/9_Crypto/crypto_trading_system/tests/test_kol_lsr.py) | .py | 3791 / 89 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_liquidation_oi_crowding_strategy.py](F:/9_Crypto/crypto_trading_system/tests/test_liquidation_oi_crowding_strategy.py) | .py | 4721 / 115 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_listener_watchdog.py](F:/9_Crypto/crypto_trading_system/tests/test_listener_watchdog.py) | .py | 2072 / 58 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_live_risk_pnl_accounting.py](F:/9_Crypto/crypto_trading_system/tests/test_live_risk_pnl_accounting.py) | .py | 13429 / 384 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_loop_stall_watchdog.py](F:/9_Crypto/crypto_trading_system/tests/test_loop_stall_watchdog.py) | .py | 983 / 32 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_macro_collector.py](F:/9_Crypto/crypto_trading_system/tests/test_macro_collector.py) | .py | 7753 / 187 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_macro_workbench_and_premium_status.py](F:/9_Crypto/crypto_trading_system/tests/test_macro_workbench_and_premium_status.py) | .py | 32181 / 852 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_main_runtime_config.py](F:/9_Crypto/crypto_trading_system/tests/test_main_runtime_config.py) | .py | 928 / 35 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_market_data_hub.py](F:/9_Crypto/crypto_trading_system/tests/test_market_data_hub.py) | .py | 11769 / 332 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_market_data_ws_ui_assets.py](F:/9_Crypto/crypto_trading_system/tests/test_market_data_ws_ui_assets.py) | .py | 1557 / 50 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_market_source_stability.py](F:/9_Crypto/crypto_trading_system/tests/test_market_source_stability.py) | .py | 27972 / 882 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_market_ws_api_price_paths.py](F:/9_Crypto/crypto_trading_system/tests/test_market_ws_api_price_paths.py) | .py | 10881 / 329 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_market_ws_authority_static.py](F:/9_Crypto/crypto_trading_system/tests/test_market_ws_authority_static.py) | .py | 2564 / 91 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_market_ws_live_shadow_precheck.py](F:/9_Crypto/crypto_trading_system/tests/test_market_ws_live_shadow_precheck.py) | .py | 9079 / 260 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_market_ws_shadow_launcher_assets.py](F:/9_Crypto/crypto_trading_system/tests/test_market_ws_shadow_launcher_assets.py) | .py | 9455 / 217 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_market_ws_shadow_report_eval.py](F:/9_Crypto/crypto_trading_system/tests/test_market_ws_shadow_report_eval.py) | .py | 15077 / 434 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_market_ws_shadow_selfcheck.py](F:/9_Crypto/crypto_trading_system/tests/test_market_ws_shadow_selfcheck.py) | .py | 38196 / 992 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_ml_api.py](F:/9_Crypto/crypto_trading_system/tests/test_ml_api.py) | .py | 23135 / 646 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_ml_canonical_model.py](F:/9_Crypto/crypto_trading_system/tests/test_ml_canonical_model.py) | .py | 2318 / 49 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_ml_pipeline.py](F:/9_Crypto/crypto_trading_system/tests/test_ml_pipeline.py) | .py | 4173 / 122 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_ml_signal_v2.py](F:/9_Crypto/crypto_trading_system/tests/test_ml_signal_v2.py) | .py | 7235 / 144 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_model_env_unicode.py](F:/9_Crypto/crypto_trading_system/tests/test_model_env_unicode.py) | .py | 1646 / 33 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_model_feedback_errors.py](F:/9_Crypto/crypto_trading_system/tests/test_model_feedback_errors.py) | .py | 1829 / 51 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_multi_factor_hf_parity.py](F:/9_Crypto/crypto_trading_system/tests/test_multi_factor_hf_parity.py) | .py | 4948 / 144 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_multi_factor_hf_strategy.py](F:/9_Crypto/crypto_trading_system/tests/test_multi_factor_hf_strategy.py) | .py | 2303 / 53 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_news_collector_legacy_async.py](F:/9_Crypto/crypto_trading_system/tests/test_news_collector_legacy_async.py) | .py | 4012 / 120 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_news_collectors_manager.py](F:/9_Crypto/crypto_trading_system/tests/test_news_collectors_manager.py) | .py | 2386 / 74 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_news_collectors_resilience.py](F:/9_Crypto/crypto_trading_system/tests/test_news_collectors_resilience.py) | .py | 4276 / 126 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_news_eventizer_rules.py](F:/9_Crypto/crypto_trading_system/tests/test_news_eventizer_rules.py) | .py | 2113 / 75 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_news_exchange_announcements.py](F:/9_Crypto/crypto_trading_system/tests/test_news_exchange_announcements.py) | .py | 14647 / 321 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_news_service_api.py](F:/9_Crypto/crypto_trading_system/tests/test_news_service_api.py) | .py | 2293 / 75 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_news_storage_db_bootstrap.py](F:/9_Crypto/crypto_trading_system/tests/test_news_storage_db_bootstrap.py) | .py | 5067 / 131 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_news_storage_db_locks.py](F:/9_Crypto/crypto_trading_system/tests/test_news_storage_db_locks.py) | .py | 1154 / 31 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_news_storage_llm_tasks.py](F:/9_Crypto/crypto_trading_system/tests/test_news_storage_llm_tasks.py) | .py | 7399 / 191 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_news_web_api_archive.py](F:/9_Crypto/crypto_trading_system/tests/test_news_web_api_archive.py) | .py | 14685 / 369 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_news_worker_config_hot_reload.py](F:/9_Crypto/crypto_trading_system/tests/test_news_worker_config_hot_reload.py) | .py | 3643 / 105 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_news_worker_intervals.py](F:/9_Crypto/crypto_trading_system/tests/test_news_worker_intervals.py) | .py | 6703 / 144 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_no_lookahead_ts_factors.py](F:/9_Crypto/crypto_trading_system/tests/test_no_lookahead_ts_factors.py) | .py | 1669 / 41 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_oi_mcap_ambush_strategies.py](F:/9_Crypto/crypto_trading_system/tests/test_oi_mcap_ambush_strategies.py) | .py | 5370 / 141 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_onchain_flow_regime.py](F:/9_Crypto/crypto_trading_system/tests/test_onchain_flow_regime.py) | .py | 1580 / 48 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_onchain_flow_regime_strategy.py](F:/9_Crypto/crypto_trading_system/tests/test_onchain_flow_regime_strategy.py) | .py | 2667 / 79 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_openai_failover_file_lock.py](F:/9_Crypto/crypto_trading_system/tests/test_openai_failover_file_lock.py) | .py | 414 / 15 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_openai_responses_migration.py](F:/9_Crypto/crypto_trading_system/tests/test_openai_responses_migration.py) | .py | 125230 / 3124 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_openai_target_helpers.py](F:/9_Crypto/crypto_trading_system/tests/test_openai_target_helpers.py) | .py | 20661 / 514 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_opennews_collector.py](F:/9_Crypto/crypto_trading_system/tests/test_opennews_collector.py) | .py | 8371 / 218 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_opennews_selfcheck.py](F:/9_Crypto/crypto_trading_system/tests/test_opennews_selfcheck.py) | .py | 1776 / 48 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_operating_reality_layer.py](F:/9_Crypto/crypto_trading_system/tests/test_operating_reality_layer.py) | .py | 24378 / 668 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_optimize_parallel.py](F:/9_Crypto/crypto_trading_system/tests/test_optimize_parallel.py) | .py | 4124 / 106 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_orchestrator_llm_rationale_timeout.py](F:/9_Crypto/crypto_trading_system/tests/test_orchestrator_llm_rationale_timeout.py) | .py | 5032 / 146 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_order_intent_router.py](F:/9_Crypto/crypto_trading_system/tests/test_order_intent_router.py) | .py | 6035 / 161 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_order_manager_mode_routing.py](F:/9_Crypto/crypto_trading_system/tests/test_order_manager_mode_routing.py) | .py | 2379 / 75 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_order_manager_safety.py](F:/9_Crypto/crypto_trading_system/tests/test_order_manager_safety.py) | .py | 8768 / 246 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_order_state_machine.py](F:/9_Crypto/crypto_trading_system/tests/test_order_state_machine.py) | .py | 3224 / 84 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_pairs_trading_negative_correlation.py](F:/9_Crypto/crypto_trading_system/tests/test_pairs_trading_negative_correlation.py) | .py | 4687 / 115 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_paper_equity_restart.py](F:/9_Crypto/crypto_trading_system/tests/test_paper_equity_restart.py) | .py | 2453 / 45 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_paper_longrun_selfcheck.py](F:/9_Crypto/crypto_trading_system/tests/test_paper_longrun_selfcheck.py) | .py | 7065 / 215 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_parquet_tz_integrity.py](F:/9_Crypto/crypto_trading_system/tests/test_parquet_tz_integrity.py) | .py | 3047 / 83 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_perf_fixes_round2.py](F:/9_Crypto/crypto_trading_system/tests/test_perf_fixes_round2.py) | .py | 6670 / 175 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_pnl_decomposer_flip.py](F:/9_Crypto/crypto_trading_system/tests/test_pnl_decomposer_flip.py) | .py | 3031 / 74 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_proactor_accept_hardening.py](F:/9_Crypto/crypto_trading_system/tests/test_proactor_accept_hardening.py) | .py | 6934 / 196 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_proxy_env_and_history_collectors.py](F:/9_Crypto/crypto_trading_system/tests/test_proxy_env_and_history_collectors.py) | .py | 2105 / 50 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_pump_precursor.py](F:/9_Crypto/crypto_trading_system/tests/test_pump_precursor.py) | .py | 2873 / 90 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_pump_watchlist_resilience.py](F:/9_Crypto/crypto_trading_system/tests/test_pump_watchlist_resilience.py) | .py | 2461 / 51 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_rate_limiter.py](F:/9_Crypto/crypto_trading_system/tests/test_rate_limiter.py) | .py | 5402 / 182 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_realtime_event_bus.py](F:/9_Crypto/crypto_trading_system/tests/test_realtime_event_bus.py) | .py | 800 / 29 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_release_safety_assets.py](F:/9_Crypto/crypto_trading_system/tests/test_release_safety_assets.py) | .py | 3645 / 86 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_research_data_maintainer.py](F:/9_Crypto/crypto_trading_system/tests/test_research_data_maintainer.py) | .py | 5888 / 173 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_research_data_maintainer_assets.py](F:/9_Crypto/crypto_trading_system/tests/test_research_data_maintainer_assets.py) | .py | 2002 / 47 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_research_integrity.py](F:/9_Crypto/crypto_trading_system/tests/test_research_integrity.py) | .py | 7794 / 171 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_research_loop_v2.py](F:/9_Crypto/crypto_trading_system/tests/test_research_loop_v2.py) | .py | 5840 / 114 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_research_market_state.py](F:/9_Crypto/crypto_trading_system/tests/test_research_market_state.py) | .py | 33669 / 1000 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_research_news_summary.py](F:/9_Crypto/crypto_trading_system/tests/test_research_news_summary.py) | .py | 4987 / 144 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_research_recovery_draft_status.py](F:/9_Crypto/crypto_trading_system/tests/test_research_recovery_draft_status.py) | .py | 4368 / 113 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_research_validation_gate.py](F:/9_Crypto/crypto_trading_system/tests/test_research_validation_gate.py) | .py | 5940 / 172 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_research_workbench_factors.py](F:/9_Crypto/crypto_trading_system/tests/test_research_workbench_factors.py) | .py | 3388 / 101 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_research_workbench_module_cache.py](F:/9_Crypto/crypto_trading_system/tests/test_research_workbench_module_cache.py) | .py | 1987 / 48 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_research_workbench_recommendations_api.py](F:/9_Crypto/crypto_trading_system/tests/test_research_workbench_recommendations_api.py) | .py | 9520 / 227 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_research_workbench_ui_assets.py](F:/9_Crypto/crypto_trading_system/tests/test_research_workbench_ui_assets.py) | .py | 11149 / 232 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_runtime_price_provider.py](F:/9_Crypto/crypto_trading_system/tests/test_runtime_price_provider.py) | .py | 5265 / 172 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_runtime_state.py](F:/9_Crypto/crypto_trading_system/tests/test_runtime_state.py) | .py | 4562 / 135 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_sensitive_api_auth.py](F:/9_Crypto/crypto_trading_system/tests/test_sensitive_api_auth.py) | .py | 31430 / 807 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_shared_ssl.py](F:/9_Crypto/crypto_trading_system/tests/test_shared_ssl.py) | .py | 1053 / 30 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_signal_aggregator_fear_greed.py](F:/9_Crypto/crypto_trading_system/tests/test_signal_aggregator_fear_greed.py) | .py | 13272 / 324 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_signal_evidence.py](F:/9_Crypto/crypto_trading_system/tests/test_signal_evidence.py) | .py | 2626 / 71 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_signal_generator_utc.py](F:/9_Crypto/crypto_trading_system/tests/test_signal_generator_utc.py) | .py | 1606 / 49 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_source_recovery.py](F:/9_Crypto/crypto_trading_system/tests/test_source_recovery.py) | .py | 7710 / 151 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_standalone_strategy_miner.py](F:/9_Crypto/crypto_trading_system/tests/test_standalone_strategy_miner.py) | .py | 5058 / 149 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_startup_mode.py](F:/9_Crypto/crypto_trading_system/tests/test_startup_mode.py) | .py | 1806 / 52 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_stop_loss_strategy_scoping.py](F:/9_Crypto/crypto_trading_system/tests/test_stop_loss_strategy_scoping.py) | .py | 2148 / 73 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_strategies.py](F:/9_Crypto/crypto_trading_system/tests/test_strategies.py) | .py | 14996 / 461 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_strategy_bug_fixes.py](F:/9_Crypto/crypto_trading_system/tests/test_strategy_bug_fixes.py) | .py | 23186 / 532 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_strategy_check_exit.py](F:/9_Crypto/crypto_trading_system/tests/test_strategy_check_exit.py) | .py | 12884 / 299 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_strategy_conflict_scoping.py](F:/9_Crypto/crypto_trading_system/tests/test_strategy_conflict_scoping.py) | .py | 3035 / 65 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_strategy_coverage_round2.py](F:/9_Crypto/crypto_trading_system/tests/test_strategy_coverage_round2.py) | .py | 7248 / 176 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_strategy_library_and_factors.py](F:/9_Crypto/crypto_trading_system/tests/test_strategy_library_and_factors.py) | .py | 20862 / 518 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_strategy_manager_hold_handling.py](F:/9_Crypto/crypto_trading_system/tests/test_strategy_manager_hold_handling.py) | .py | 7650 / 202 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_strategy_manager_runtime_data.py](F:/9_Crypto/crypto_trading_system/tests/test_strategy_manager_runtime_data.py) | .py | 10156 / 305 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_strategy_mode_isolation.py](F:/9_Crypto/crypto_trading_system/tests/test_strategy_mode_isolation.py) | .py | 16199 / 499 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_strategy_mode_sync.py](F:/9_Crypto/crypto_trading_system/tests/test_strategy_mode_sync.py) | .py | 6448 / 167 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_strategy_monitor_robust_std.py](F:/9_Crypto/crypto_trading_system/tests/test_strategy_monitor_robust_std.py) | .py | 1400 / 42 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_strategy_order_routing.py](F:/9_Crypto/crypto_trading_system/tests/test_strategy_order_routing.py) | .py | 5405 / 159 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_strategy_ownership_sync.py](F:/9_Crypto/crypto_trading_system/tests/test_strategy_ownership_sync.py) | .py | 15936 / 449 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_strategy_performance_isolation.py](F:/9_Crypto/crypto_trading_system/tests/test_strategy_performance_isolation.py) | .py | 4682 / 125 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_strategy_registration_persistence.py](F:/9_Crypto/crypto_trading_system/tests/test_strategy_registration_persistence.py) | .py | 2821 / 70 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_strategy_runtime_lifecycle.py](F:/9_Crypto/crypto_trading_system/tests/test_strategy_runtime_lifecycle.py) | .py | 4693 / 145 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_strategy_runtime_mode_switch.py](F:/9_Crypto/crypto_trading_system/tests/test_strategy_runtime_mode_switch.py) | .py | 4125 / 107 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_strategy_runtime_policy.py](F:/9_Crypto/crypto_trading_system/tests/test_strategy_runtime_policy.py) | .py | 4156 / 110 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_strategy_signal_regressions.py](F:/9_Crypto/crypto_trading_system/tests/test_strategy_signal_regressions.py) | .py | 25379 / 661 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_strategy_signal_time.py](F:/9_Crypto/crypto_trading_system/tests/test_strategy_signal_time.py) | .py | 724 / 26 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_structural_context_degraded.py](F:/9_Crypto/crypto_trading_system/tests/test_structural_context_degraded.py) | .py | 2063 / 60 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_structural_risk_gate.py](F:/9_Crypto/crypto_trading_system/tests/test_structural_risk_gate.py) | .py | 2162 / 68 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_supply_event_no_first_seen_lookahead.py](F:/9_Crypto/crypto_trading_system/tests/test_supply_event_no_first_seen_lookahead.py) | .py | 801 / 29 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_supply_event_schema.py](F:/9_Crypto/crypto_trading_system/tests/test_supply_event_schema.py) | .py | 1657 / 54 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_supply_event_strategy_windows.py](F:/9_Crypto/crypto_trading_system/tests/test_supply_event_strategy_windows.py) | .py | 2314 / 71 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_supply_factor_tracker.py](F:/9_Crypto/crypto_trading_system/tests/test_supply_factor_tracker.py) | .py | 7284 / 152 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_tracker_retirement.py](F:/9_Crypto/crypto_trading_system/tests/test_tracker_retirement.py) | .py | 2377 / 52 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_tracker_robustness.py](F:/9_Crypto/crypto_trading_system/tests/test_tracker_robustness.py) | .py | 9026 / 202 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_trade_risk_ml_fixes.py](F:/9_Crypto/crypto_trading_system/tests/test_trade_risk_ml_fixes.py) | .py | 6453 / 215 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_trading_balance_routes.py](F:/9_Crypto/crypto_trading_system/tests/test_trading_balance_routes.py) | .py | 6752 / 201 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_trading_balances_altcoin_notifications.py](F:/9_Crypto/crypto_trading_system/tests/test_trading_balances_altcoin_notifications.py) | .py | 7672 / 237 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_trading_balances_stale_prev_equity.py](F:/9_Crypto/crypto_trading_system/tests/test_trading_balances_stale_prev_equity.py) | .py | 2643 / 64 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_trading_heatmap_ui_assets.py](F:/9_Crypto/crypto_trading_system/tests/test_trading_heatmap_ui_assets.py) | .py | 527 / 16 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_trading_order_connectivity.py](F:/9_Crypto/crypto_trading_system/tests/test_trading_order_connectivity.py) | .py | 5929 / 163 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_trading_route_modules.py](F:/9_Crypto/crypto_trading_system/tests/test_trading_route_modules.py) | .py | 35417 / 1022 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_trading_runtime_cache.py](F:/9_Crypto/crypto_trading_system/tests/test_trading_runtime_cache.py) | .py | 6293 / 136 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_trading_runtime_routes.py](F:/9_Crypto/crypto_trading_system/tests/test_trading_runtime_routes.py) | .py | 2289 / 66 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_trading_runtime_service.py](F:/9_Crypto/crypto_trading_system/tests/test_trading_runtime_service.py) | .py | 7833 / 199 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_unlock_short_tracker.py](F:/9_Crypto/crypto_trading_system/tests/test_unlock_short_tracker.py) | .py | 6604 / 148 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_upbit_caution_tracker.py](F:/9_Crypto/crypto_trading_system/tests/test_upbit_caution_tracker.py) | .py | 15049 / 258 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_upbit_short_backtest.py](F:/9_Crypto/crypto_trading_system/tests/test_upbit_short_backtest.py) | .py | 1288 / 35 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_validation_dsr_kurtosis.py](F:/9_Crypto/crypto_trading_system/tests/test_validation_dsr_kurtosis.py) | .py | 756 / 32 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_vectorized_oscillator_positions.py](F:/9_Crypto/crypto_trading_system/tests/test_vectorized_oscillator_positions.py) | .py | 2335 / 75 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_vesting_spec.py](F:/9_Crypto/crypto_trading_system/tests/test_vesting_spec.py) | .py | 1795 / 32 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_web_main_runtime_tasks.py](F:/9_Crypto/crypto_trading_system/tests/test_web_main_runtime_tasks.py) | .py | 53876 / 1451 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_workbench_source_timeouts.py](F:/9_Crypto/crypto_trading_system/tests/test_workbench_source_timeouts.py) | .py | 3759 / 100 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_ws_client_hardening.py](F:/9_Crypto/crypto_trading_system/tests/test_ws_client_hardening.py) | .py | 2598 / 60 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_ws_quality_guard.py](F:/9_Crypto/crypto_trading_system/tests/test_ws_quality_guard.py) | .py | 8774 / 221 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/test_xs_research.py](F:/9_Crypto/crypto_trading_system/tests/test_xs_research.py) | .py | 14377 / 260 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/web/test_ai_trade_feishu_notifications.py](F:/9_Crypto/crypto_trading_system/tests/web/test_ai_trade_feishu_notifications.py) | .py | 3838 / 106 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/web/test_altcoin_route.py](F:/9_Crypto/crypto_trading_system/tests/web/test_altcoin_route.py) | .py | 76760 / 2037 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/web/test_backtest_compare_route.py](F:/9_Crypto/crypto_trading_system/tests/web/test_backtest_compare_route.py) | .py | 17529 / 456 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/web/test_backtest_pairs_route.py](F:/9_Crypto/crypto_trading_system/tests/web/test_backtest_pairs_route.py) | .py | 11144 / 336 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/web/test_data_coinglass_overview_route.py](F:/9_Crypto/crypto_trading_system/tests/web/test_data_coinglass_overview_route.py) | .py | 2776 / 74 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/web/test_data_download_batch_route.py](F:/9_Crypto/crypto_trading_system/tests/web/test_data_download_batch_route.py) | .py | 3495 / 95 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/web/test_data_klines_timezone.py](F:/9_Crypto/crypto_trading_system/tests/web/test_data_klines_timezone.py) | .py | 9887 / 245 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/web/test_data_onchain_premium_payload.py](F:/9_Crypto/crypto_trading_system/tests/web/test_data_onchain_premium_payload.py) | .py | 15139 / 370 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/web/test_data_pairs_ranking_route.py](F:/9_Crypto/crypto_trading_system/tests/web/test_data_pairs_ranking_route.py) | .py | 4193 / 122 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/web/test_data_research_symbols_route.py](F:/9_Crypto/crypto_trading_system/tests/web/test_data_research_symbols_route.py) | .py | 8658 / 229 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/web/test_multi_assets_overview.py](F:/9_Crypto/crypto_trading_system/tests/web/test_multi_assets_overview.py) | .py | 1253 / 43 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/web/test_notifications_altcoin_context.py](F:/9_Crypto/crypto_trading_system/tests/web/test_notifications_altcoin_context.py) | .py | 2248 / 59 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/web/test_pump_watchlist_route.py](F:/9_Crypto/crypto_trading_system/tests/web/test_pump_watchlist_route.py) | .py | 2095 / 57 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/web/test_strategy_monitor_route.py](F:/9_Crypto/crypto_trading_system/tests/web/test_strategy_monitor_route.py) | .py | 35432 / 1086 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/web/test_trading_analytics_route_alias.py](F:/9_Crypto/crypto_trading_system/tests/web/test_trading_analytics_route_alias.py) | .py | 845 / 29 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/web/test_trading_live_trade_review_route.py](F:/9_Crypto/crypto_trading_system/tests/web/test_trading_live_trade_review_route.py) | .py | 776 / 28 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/web/test_trading_microstructure_payload.py](F:/9_Crypto/crypto_trading_system/tests/web/test_trading_microstructure_payload.py) | .py | 28023 / 726 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [tests/web/test_trading_pnl_heatmap_route.py](F:/9_Crypto/crypto_trading_system/tests/web/test_trading_pnl_heatmap_route.py) | .py | 6889 / 198 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web.bat](F:/9_Crypto/crypto_trading_system/web.bat) | .bat | 866 / 28 | S：登记/类型与引用范围 |
| [web/__init__.py](F:/9_Crypto/crypto_trading_system/web/__init__.py) | .py | 31 / 1 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/api/__init__.py](F:/9_Crypto/crypto_trading_system/web/api/__init__.py) | .py | 128 / 3 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/api/ai_agent.py](F:/9_Crypto/crypto_trading_system/web/api/ai_agent.py) | .py | 5123 / 138 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/api/ai_research.py](F:/9_Crypto/crypto_trading_system/web/api/ai_research.py) | .py | 282956 / 6580 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/api/altcoin/__init__.py](F:/9_Crypto/crypto_trading_system/web/api/altcoin/__init__.py) | .py | 6810 / 142 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/api/altcoin/alerts.py](F:/9_Crypto/crypto_trading_system/web/api/altcoin/alerts.py) | .py | 8175 / 198 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/api/altcoin/cache.py](F:/9_Crypto/crypto_trading_system/web/api/altcoin/cache.py) | .py | 7561 / 222 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/api/altcoin/constants.py](F:/9_Crypto/crypto_trading_system/web/api/altcoin/constants.py) | .py | 2185 / 63 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/api/altcoin/detail.py](F:/9_Crypto/crypto_trading_system/web/api/altcoin/detail.py) | .py | 12355 / 296 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/api/altcoin/helpers.py](F:/9_Crypto/crypto_trading_system/web/api/altcoin/helpers.py) | .py | 18417 / 498 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/api/altcoin/pump.py](F:/9_Crypto/crypto_trading_system/web/api/altcoin/pump.py) | .py | 5728 / 137 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/api/altcoin/scan.py](F:/9_Crypto/crypto_trading_system/web/api/altcoin/scan.py) | .py | 54440 / 1405 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/api/altcoin/signals.py](F:/9_Crypto/crypto_trading_system/web/api/altcoin/signals.py) | .py | 3492 / 83 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/api/altcoin/state.py](F:/9_Crypto/crypto_trading_system/web/api/altcoin/state.py) | .py | 685 / 18 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/api/altcoin/universe.py](F:/9_Crypto/crypto_trading_system/web/api/altcoin/universe.py) | .py | 8342 / 221 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/api/auth.py](F:/9_Crypto/crypto_trading_system/web/api/auth.py) | .py | 6435 / 182 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/api/backtest.py](F:/9_Crypto/crypto_trading_system/web/api/backtest.py) | .py | 243466 / 5637 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/api/data.py](F:/9_Crypto/crypto_trading_system/web/api/data.py) | .py | 285793 / 7358 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/api/ml.py](F:/9_Crypto/crypto_trading_system/web/api/ml.py) | .py | 62275 / 1551 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/api/news.py](F:/9_Crypto/crypto_trading_system/web/api/news.py) | .py | 153552 / 3829 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/api/notifications.py](F:/9_Crypto/crypto_trading_system/web/api/notifications.py) | .py | 6073 / 161 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/api/research.py](F:/9_Crypto/crypto_trading_system/web/api/research.py) | .py | 159729 / 4228 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/api/risk.py](F:/9_Crypto/crypto_trading_system/web/api/risk.py) | .py | 3334 / 92 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/api/strategies.py](F:/9_Crypto/crypto_trading_system/web/api/strategies.py) | .py | 138847 / 3293 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/api/trading.py](F:/9_Crypto/crypto_trading_system/web/api/trading.py) | .py | 362649 / 9881 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/api/trading_accounts.py](F:/9_Crypto/crypto_trading_system/web/api/trading_accounts.py) | .py | 3558 / 93 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/api/trading_analytics.py](F:/9_Crypto/crypto_trading_system/web/api/trading_analytics.py) | .py | 5706 / 190 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/api/trading_balances.py](F:/9_Crypto/crypto_trading_system/web/api/trading_balances.py) | .py | 54793 / 1333 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/api/trading_orders.py](F:/9_Crypto/crypto_trading_system/web/api/trading_orders.py) | .py | 2352 / 65 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/api/trading_positions.py](F:/9_Crypto/crypto_trading_system/web/api/trading_positions.py) | .py | 691 / 21 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/api/trading_runtime.py](F:/9_Crypto/crypto_trading_system/web/api/trading_runtime.py) | .py | 10468 / 276 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/asset_versions.py](F:/9_Crypto/crypto_trading_system/web/asset_versions.py) | .py | 909 / 30 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/main.py](F:/9_Crypto/crypto_trading_system/web/main.py) | .py | 124377 / 3025 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/services/__init__.py](F:/9_Crypto/crypto_trading_system/web/services/__init__.py) | .py | 565 / 21 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/services/trading_runtime_service.py](F:/9_Crypto/crypto_trading_system/web/services/trading_runtime_service.py) | .py | 10330 / 256 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/startup_mode.py](F:/9_Crypto/crypto_trading_system/web/startup_mode.py) | .py | 2326 / 77 | S：AST通过、结构/敏感模式扫描；D/V依模块证据 |
| [web/static/css/ai_agent.css](F:/9_Crypto/crypto_trading_system/web/static/css/ai_agent.css) | .css | 12024 / 122 | S：登记/类型与引用范围 |
| [web/static/css/style.css](F:/9_Crypto/crypto_trading_system/web/static/css/style.css) | .css | 528007 / 19928 | S：登记/类型与引用范围 |
| [web/static/favicon.svg](F:/9_Crypto/crypto_trading_system/web/static/favicon.svg) | .svg | 236 | S：登记/类型与引用范围 |
| [web/static/js/ai_research.js](F:/9_Crypto/crypto_trading_system/web/static/js/ai_research.js) | .js | 366401 / 7141 | S：node语法通过；D/V依前端边界范围 |
| [web/static/js/ai_research_agent.js](F:/9_Crypto/crypto_trading_system/web/static/js/ai_research_agent.js) | .js | 136302 / 2872 | S：node语法通过；D/V依前端边界范围 |
| [web/static/js/ai_research_candidates.js](F:/9_Crypto/crypto_trading_system/web/static/js/ai_research_candidates.js) | .js | 12487 / 297 | S：node语法通过；D/V依前端边界范围 |
| [web/static/js/ai_research_diagnostics.js](F:/9_Crypto/crypto_trading_system/web/static/js/ai_research_diagnostics.js) | .js | 22184 / 492 | S：node语法通过；D/V依前端边界范围 |
| [web/static/js/ai_research_patch.js](F:/9_Crypto/crypto_trading_system/web/static/js/ai_research_patch.js) | .js | 12196 / 203 | S：node语法通过；D/V依前端边界范围 |
| [web/static/js/ai_research_runtime.js](F:/9_Crypto/crypto_trading_system/web/static/js/ai_research_runtime.js) | .js | 21268 / 458 | S：node语法通过；D/V依前端边界范围 |
| [web/static/js/altcoin_radar.js](F:/9_Crypto/crypto_trading_system/web/static/js/altcoin_radar.js) | .js | 114351 / 2608 | S：node语法通过；D/V依前端边界范围 |
| [web/static/js/app.js](F:/9_Crypto/crypto_trading_system/web/static/js/app.js) | .js | 650028 / 10192 | S：node语法通过；D/V依前端边界范围 |
| [web/static/js/dashboard_news.js](F:/9_Crypto/crypto_trading_system/web/static/js/dashboard_news.js) | .js | 10408 / 264 | S：node语法通过；D/V依前端边界范围 |
| [web/static/js/dashboard_unstructured_news.js](F:/9_Crypto/crypto_trading_system/web/static/js/dashboard_unstructured_news.js) | .js | 14817 / 357 | S：node语法通过；D/V依前端边界范围 |
| [web/static/js/news.js](F:/9_Crypto/crypto_trading_system/web/static/js/news.js) | .js | 25336 / 533 | S：node语法通过；D/V依前端边界范围 |
| [web/static/js/news_tab.js](F:/9_Crypto/crypto_trading_system/web/static/js/news_tab.js) | .js | 39780 / 758 | S：node语法通过；D/V依前端边界范围 |
| [web/static/js/news_tab_runtime.js](F:/9_Crypto/crypto_trading_system/web/static/js/news_tab_runtime.js) | .js | 64529 / 1258 | S：node语法通过；D/V依前端边界范围 |
| [web/static/js/research_workbench.js](F:/9_Crypto/crypto_trading_system/web/static/js/research_workbench.js) | .js | 100087 / 2143 | S：node语法通过；D/V依前端边界范围 |
| [web/templates/index.html](F:/9_Crypto/crypto_trading_system/web/templates/index.html) | .html | 260215 / 3569 | S：登记/类型与引用范围 |
| [web/templates/news.html](F:/9_Crypto/crypto_trading_system/web/templates/news.html) | .html | 9905 / 173 | S：登记/类型与引用范围 |

## ignored第一方辅助源码

以下12个文件不在git archive测试快照，已只读检查副作用和用途，未执行。logs/tmp的旧pytest包装、模板重写与浏览器读取辅助属于历史维护产物；data下运行/切换/监控辅助的审阅见附录。未把它们悄然排除为第三方。

| 文件 | 行数 | 证据/用途 |
|---|---:|---|
| [data/_level1_relaxed_run.py](F:/9_Crypto/crypto_trading_system/data/_level1_relaxed_run.py) | 46 | 运行/策略切换/监督辅助；只读副作用审阅 |
| [data/_level1_watcher.py](F:/9_Crypto/crypto_trading_system/data/_level1_watcher.py) | 54 | 运行/策略切换/监督辅助；只读副作用审阅 |
| [data/_restart_live8000.ps1](F:/9_Crypto/crypto_trading_system/data/_restart_live8000.ps1) | 61 | 运行/策略切换/监督辅助；只读副作用审阅 |
| [data/_switch_strategy_primary.ps1](F:/9_Crypto/crypto_trading_system/data/_switch_strategy_primary.ps1) | 62 | 运行/策略切换/监督辅助；只读副作用审阅 |
| [data/_ws_proxy_monitor.py](F:/9_Crypto/crypto_trading_system/data/_ws_proxy_monitor.py) | 78 | 运行/策略切换/监督辅助；只读副作用审阅 |
| [logs/automation_daily_scan/pytest_20260731_090633.ps1](F:/9_Crypto/crypto_trading_system/logs/automation_daily_scan/pytest_20260731_090633.ps1) | 3 | 历史pytest包装；会在原树运行/写日志，不执行 |
| [logs/automation_daily_scan/pytest_rerun_20260731_091921.ps1](F:/9_Crypto/crypto_trading_system/logs/automation_daily_scan/pytest_rerun_20260731_091921.ps1) | 3 | 历史pytest包装；会在原树运行/写日志，不执行 |
| [tmp/codex_automation/run_full_pytest_20260709_090721.ps1](F:/9_Crypto/crypto_trading_system/tmp/codex_automation/run_full_pytest_20260709_090721.ps1) | 10 | 历史pytest包装；会在原树运行/写日志，不执行 |
| [tmp/codex_maintenance_20260806/run_tests.ps1](F:/9_Crypto/crypto_trading_system/tmp/codex_maintenance_20260806/run_tests.ps1) | 6 | 历史pytest包装；会在原树运行/写日志，不执行 |
| [tmp/redesign_agent.py](F:/9_Crypto/crypto_trading_system/tmp/redesign_agent.py) | 194 | 历史模板重写器；可写真实template，不执行 |
| [tmp/verify_agent_sources.cjs](F:/9_Crypto/crypto_trading_system/tmp/verify_agent_sources.cjs) | 31 | 历史浏览器读取/截图辅助；访问真实本机服务，不执行 |
| [tmp/verify_dashboard_coverage.cjs](F:/9_Crypto/crypto_trading_system/tmp/verify_dashboard_coverage.cjs) | 21 | 历史浏览器读取/截图辅助；访问真实本机服务，不执行 |

文件SHA/行数记录：[ignored_source_inventory.json](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/ignored_source_inventory.json)。模块细化：[data_research_coverage.md](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/data_research_coverage.md)、[web_security_coverage.json](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/web_security_coverage.json)、[trading.md](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/trading.md)；全部机器主清单：[inventory.json](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/inventory.json)。本次新增审计生成器和复现脚本属于交付证据，未混入产品源码统计。
