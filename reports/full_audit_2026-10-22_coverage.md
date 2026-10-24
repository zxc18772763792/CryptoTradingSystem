# 审计覆盖与边界 · 2026-10-22

审计对象：`F:\9_Crypto\crypto_trading_system`，HEAD `7ad26b078abb603d411b93958b28a4cf033861a9` 加当前工作树。基线时间 `2026-10-22T13:51:07.476872+00:00`。不是另一个分支，也没有把用户未提交的修改排除在外。

## 全量与深度的定义

- I：文件盘点、大小/SHA/类型及角色登记，不代表逐行审阅。
- S：适用文件的 Python AST、JSON/YAML 或 JS/PS 语法检查；另有路由装饰器提取与脱敏模式扫描。语法通过不等于语义正确。
- D：人工追踪列明的关键函数/边界；大模块只表示指定段落，不能解读为整文件每一行已审计。
- V：隔离执行的测试文件/特定合成反例或只读证据；没有安装 coverage 工具，不能提供生产代码行覆盖率。
- X：有明确原因未执行的行为/数据。保留边界，不把未观察到问题当作安全认证。

逐文件记录：[file_coverage.json](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-22_parts/file_coverage.json)。原始清单：[inventory.json](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-22_parts/inventory.json)；路由装饰器 369 个：[routes_static.json](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-22_parts/routes_static.json)。

## 基线量化

共 1534 个可盘点文件，1021 个 Python、333497 行 Python（包含测试、脚本和 reports 下可追踪的研究代码）；JSON 202、YAML 10、JavaScript/CJS 24、PowerShell 26。上述类型的语法/解析检查均通过。审计自身新增目录未计入基线。Git 忽略的业务目录里没有额外第一方源码文件；忽略的缓存、秘密及真实运行数据未进入测试副本。

| 模块 | 文件 | Python | 本轮覆盖方式 |
|---|---:|---:|---|
| core | 287 | 287 | S；交易/风控/研究/认证关键段 D；相关回归和反例 V |
| web | 56 | 36 | S；权限/生命周期/状态关键段 D；接口反例和 fixture UI V |
| config | 15 | 6 | S；模式/环境/测试隔离选择性检查 |
| strategies | 32 | 32 | S；既有策略测试 V，未重跑全市场回测 |
| prediction_markets | 17 | 16 | S；纸面交易路径对照与现有原子性回归 V |
| scripts | 279 | 254 | S；受管启动、配置契约和审计工具相关段 D/V；未执行生产操作脚本 |
| tests | 362 | 356 | S；3313 个 pytest 用例含 2 跳过；另有 JS 和 fixture UI |
| docs | 117 | 0 | I；AGENTS/SECURITY/启动、旧审计及 Alastar 交接选择性阅读 |
| models | 34 | 0 | I；未重训/重放全部模型，未加载不可信模型制品 |
| reports | 313 | 33 | I/S（按文件类型）；旧审计与最新 Alastar 研究选择性阅读 |
| data | 1 | 0 | I；跟踪到的非秘密文件；忽略的大型历史数据不属于全量测试输入 |
| .openclaw | 2 | 0 | I/S（按文件类型），未操作外部智能体 |
| (root) | 19 | 1 | I/S；Docker/环境/启动/pytest 契约 D |

## 关键段深读清单

| 文件 | 实际检查内容 |
|---|---|
| [web/api/trading_accounts.py](F:/9_Crypto/crypto_trading_system/web/api/trading_accounts.py) | 账户输出与 live 更新审批 |
| [core/trading/account_manager.py](F:/9_Crypto/crypto_trading_system/core/trading/account_manager.py) | list/update/credentials 边界 |
| [web/api/auth.py](F:/9_Crypto/crypto_trading_system/web/api/auth.py) | 本地会话与 OR 权限语义 |
| [core/governance/rbac.py](F:/9_Crypto/crypto_trading_system/core/governance/rbac.py) | 角色能力表 |
| [web/api/trading_runtime.py](F:/9_Crypto/crypto_trading_system/web/api/trading_runtime.py) | 风险复位与配置接口 |
| [web/api/ai_agent.py](F:/9_Crypto/crypto_trading_system/web/api/ai_agent.py) | AI 配置/风险路由 |
| [web/api/ai_research.py](F:/9_Crypto/crypto_trading_system/web/api/ai_research.py) | AI 配置/共享风险更新、研究跟踪入口（选择性段落） |
| [core/ai/autonomous_agent.py](F:/9_Crypto/crypto_trading_system/core/ai/autonomous_agent.py) | allow_live 更新/持久化（选择性段落） |
| [core/risk/risk_manager.py](F:/9_Crypto/crypto_trading_system/core/risk/risk_manager.py) | pre_trade_check 与 reset_halt |
| [core/trading/order_manager.py](F:/9_Crypto/crypto_trading_system/core/trading/order_manager.py) | 模拟报价/成交、账户订单作用域（选择性段落） |
| [core/trading/execution_engine.py](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py) | 账户隔离、反向平仓/风险路径（选择性段落） |
| [web/main.py](F:/9_Crypto/crypto_trading_system/web/main.py) | 启动/任务/健康、会话、WebSocket（选择性段落） |
| [core/ops/service/trading_routes.py](F:/9_Crypto/crypto_trading_system/core/ops/service/trading_routes.py) | 风控复位旁路对照 |
| [core/research/alastar_forward_paper.py](F:/9_Crypto/crypto_trading_system/core/research/alastar_forward_paper.py) | 冻结、收件、准入、同币零分布/停止门槛 |
| [core/research/alastar_paper_tracker.py](F:/9_Crypto/crypto_trading_system/core/research/alastar_paper_tracker.py) | 语义队列、时效、工作任务与汇总 |
| [core/research/alastar_horizon_tracker.py](F:/9_Crypto/crypto_trading_system/core/research/alastar_horizon_tracker.py) | 共同成熟周期、损失路径、资金费与冻结 |
| [core/research/retirement.py](F:/9_Crypto/crypto_trading_system/core/research/retirement.py) | 预登记、聚类与独立规则 |
| [core/research/tracker_registry.py](F:/9_Crypto/crypto_trading_system/core/research/tracker_registry.py) | 独立研究账本注册 |
| [core/news/alastar_observation.py](F:/9_Crypto/crypto_trading_system/core/news/alastar_observation.py) | 收件证据链、只读时延复算 |
| [core/news/collectors/rss.py](F:/9_Crypto/crypto_trading_system/core/news/collectors/rss.py) | 来源 URL/解析候选检查 |
| [core/news/collectors/manager.py](F:/9_Crypto/crypto_trading_system/core/news/collectors/manager.py) | URL 归一化候选检查 |
| [core/news/storage/db.py](F:/9_Crypto/crypto_trading_system/core/news/storage/db.py) | 新闻 URL 保存与去重选择性检查 |
| [prediction_markets/polymarket/paper_trading.py](F:/9_Crypto/crypto_trading_system/prediction_markets/polymarket/paper_trading.py) | 有限数、原子成交回归对照 |
| [core/backtest/cost_models.py](F:/9_Crypto/crypto_trading_system/core/backtest/cost_models.py) | 成本和滑点辅助函数 |
| [core/backtest/common_pnl.py](F:/9_Crypto/crypto_trading_system/core/backtest/common_pnl.py) | 公共业绩口径 |
| [web/static/js/dashboard_news.js](F:/9_Crypto/crypto_trading_system/web/static/js/dashboard_news.js) | URL 输出候选检查，未证实可利用 XSS |
| [tests/conftest.py](F:/9_Crypto/crypto_trading_system/tests/conftest.py) | 测试隔离与临时路径 |
| [scripts/web.ps1](F:/9_Crypto/crypto_trading_system/scripts/web.ps1) | 受管启动保护和只读 status（选择性段落） |
| [scripts/check_config_contract.ps1](F:/9_Crypto/crypto_trading_system/scripts/check_config_contract.ps1) | 只读配置契约 |
| [Dockerfile](F:/9_Crypto/crypto_trading_system/Dockerfile) | 上下文、用户、入口/健康检查 |
| [.dockerignore](F:/9_Crypto/crypto_trading_system/.dockerignore) | 构建上下文秘密过滤 |
| [docker-compose.yml](F:/9_Crypto/crypto_trading_system/docker-compose.yml) | 发布地址、服务凭据要求/健康、默认模式 |
| [scripts/verify_env.py](F:/9_Crypto/crypto_trading_system/scripts/verify_env.py) | 启动模块核验与版本契约差异 |
| [requirements.txt](F:/9_Crypto/crypto_trading_system/requirements.txt) | 版本约束、开发工具和安全公告匹配 |

## 没有升级为确认问题的候选

- 新闻 RSS/URL 链路允许非 http(s) 的字符串，并被 HTML 转义后写入 href。没有完成带 target=_blank/noopener 等浏览器条件的同源可利用证明；本轮不将其升级为“已证实存储型 XSS”。后续应补协议白名单及浏览器反例，现阶段是未决检查项。
- ML 制品清理路径、协方差数据的前向/反向填充等搜索候选没有形成完整可达错误证据，不计入确认问题数量。
- 旧风险/交易回归、环境启动核验及语法检查通过，没有因此声称所有交易所行为或研究策略已验证。

## 本轮 X：明确没有执行

1. 真实账户连接、余额刷新、交易所下单/撤单/杠杆修改、账户资金或密钥操作；不验证实盘成交、保证金、爆仓或真实网络故障恢复。
2. Docker 镜像构建/启动、PostgreSQL 迁移与恢复演练、Redis/监控压力；部署配置仅静态审阅。
3. 全部行情仓库的完整性/时区/退市覆盖扫描、全量历史策略重算、模型训练/过拟合重新评估。既有对应测试可以回归，但不能替代对全部数据的重算。
4. 连接正在运行交易服务的全页面浏览器 E2E；仅执行完全拦截网络的 AI fixture UI。已有 playwright_smoke.spec.js 依赖 @playwright/test，仓库没有独立 Node 包清单；本轮不为跑它安装工具或接入生产服务。
5. 自动化定时任务长期运行、小时/天级断线恢复和负载/并发穷举。本机 GET 健康检查只是一个时间点的快照。
6. 所有 Git 历史内容、二进制、私有运行文件的完整秘密审计。只做当前可盘点源码的脱敏模式扫描和指定秘密文件名的历史路径核验。
7. 操作系统/conda 系统库/CUDA、Node/browser 包和实际镜像的供应链漏洞扫描。OSV 查询仅覆盖 129 个当前 Python 分发包；上游命中尚未逐条完成应用可达性证明。

本轮以全项目盘点和关键风险证据为交付边界。未给出“可安全实盘”“全部源码逐行证明”或“历史策略收益可信”的通行结论。
