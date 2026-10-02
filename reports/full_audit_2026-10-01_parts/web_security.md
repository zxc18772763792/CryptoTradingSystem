# 2026-10-01 全量审计分报告：Web、安全边界、Governance、Ops 与 Audit

本报告提供防御性代码审阅结果。未修改业务代码、生产参数或已有用户修改；未读取 `.env`、`.env.local`、`keys.txt`；未启动真实服务、worker 或代理，未访问交易账户，未发出订单，未调用真实敏感接口。审阅基线为主工程 HEAD `2fecd05dea8c5ee438b4071e50548b828b7411af`。日期使用 Asia/Shanghai。

本部分覆盖 `web/`、`core/governance/`、`core/ops/`、`core/audit/`；`core/integrations/` 不存在。读取了主工程 `AGENTS.md`、`README.md`、`SECURITY.md` 和 `docs/GOVERNANCE.md`。目录去重和工作区边界由总报告统一处理。

## 覆盖与验证口径

- 全部 75 个第一方源文件纳入目录、语法和安全边界扫描，共 109,860 行。Python 全部通过 AST 解析。
- 枚举 343 条 HTTP/WS 路由声明，包括别名和条件注册路由；这是静态声明数，不是已启动应用的有效路由数。
- 深读范围：Web 主入口的启动/关闭、CORS、Cookie、WS 和健康接口；所有认证/RBAC 实现；Governance 全部实现；Ops 的身份依赖、控制操作、新闻桥接、研究、AI、Polymarket 和文件路径；Audit 全部实现；Web 交易/账户/订单/策略/风险控制路由；AI 批准与激活入口；ML 删除路径；数据回放状态；前端 HTML 写入与 URL 插值。大型行情/指标/回测模块在本部分完成路由与安全边界扫描，其计算正确性由其他分报告审阅。
- 文件哈希、行数、路由位置、直接依赖、router 继承依赖已保存为 `web_security_coverage.json`。不能以“装饰器无 Depends”直接判断未认证：AI Research 和交易分析 router 已有统一读取权限依赖，Ops 路由继承 `/ops` 身份依赖。
- 安全验证使用项目 Python 3.11，通过 AST 抽取选定函数，避免导入项目启动和配置模块。身份、交易管理器、持久化、worker、日志均替换为 mocks；FastAPI TestClient 使用仅在内存构造的测试应用。文件验证只写报告目录下的独立临时沙箱。
- `web_security_repro.py` 最终执行成功，结果为 `web_security_repro_results.json`。Node 使用离线 DOM sink mock，渲染转义结果为 `web_security_xss_results.json`；没有运行浏览器页面或请求真实服务。
- 现有全套 pytest 和更广静态检查由总审计执行，避免并发运行共享持久化测试。本分报告不把 mocks 结果表述为实盘验证。

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

## 覆盖清单

| 路径 | 完成的审阅 | 已确认问题 | 动态限制 |
|---|---|---|---|
| `web/main.py`, `startup_mode.py`, `asset_versions.py`, init | 入口/继承依赖、启动关闭、Cookie/CORS/WS/health、资源清理 | WEB-10 | 不启动 lifespan 或监听服务 |
| `web/api/auth.py` | loopback 会话、Origin/Referer、token 与 RBAC 衔接 | WEB-02、05 的相关边界 | 不读取真实 token |
| 交易 orders/positions/accounts/balances/runtime/analytics 与 `trading.py` | 所有路由权限、实盘入口、转发、错误与敏感返回扫描；资金计算另分工 | 相关授权对照；未新增重复资金问题 | 不访问账户或下单 |
| `web/api/strategies.py` | 注册/导入/运行/模式/params/export，名称与模式输入、持久化入口 | WEB-01、05 | Manager 模拟，runner 未启动 |
| `web/api/risk.py` | 全部三个 endpoint 的身份、风险副作用、审计 | WEB-02、09 | CircuitBreaker 模拟 |
| `web/api/ai_agent.py`, `ai_research.py`, `ml.py` | 全部 route 依赖、审批/激活入口、删除路径边界、对外返回与文件行为 | WEB-11 相关入口；AI 授权待一致性核对 | 未训练模型、调用模型供应商 |
| `web/api/data.py`, `research.py`, `backtest.py` | 全部 route，下载/脚本/文件/SSE导出边界、公开计算与回放状态 | WEB-12 | 指标正确性另分工，未触发网络下载 |
| `web/api/altcoin/*`, `news.py`, `notifications.py` | 枚举全部 endpoint，控制操作依赖、缓存/worker/进程/外部输入边界 | 无新增确定问题 | 未运行外部 worker/通知发送 |
| `web/services/*` | 模式切换、pending token、工作任务恢复/异常处理和清理 | 并发待隔离验证 | 所有后台 worker 不运行 |
| `web/static/js/*` | 全部文件 sink/事件/API扫描；策略摘要深读与离线渲染断言 | WEB-05 | 不用真实服务页面 |
| `web/templates/*`, `web/static/css/*`, favicon | 资源/脚本载入、HTML/插值边界扫描 | 无新增确定问题 | 未完整视觉回归 |
| `core/governance/*` | 全部函数、角色/服务层授权、状态机、风险评分、持久化/审计 | WEB-03、04；权限一致性待补 | 真实 DB 未写入 |
| `core/ops/service/*` | 全部路由及 parent 身份、控制能力、审批、路径和副作用 | WEB-06、07、08 | 条件路由启用状态未知 |
| `core/audit/*` | 全部日志签名/脱敏/后台排空、JSONL读改写与错误路径 | WEB-09、11 | 报告沙箱复现 |
| `core/integrations/` | 路径检查 | 目录不存在 | 无待审文件 |

逐文件与逐路由覆盖见 `web_security_coverage.json`。本部分完成静态边界覆盖，不声称所有 109,860 行已逐行深审或所有运行行为已经动态验证。

## `_radar_refactor` 独有补丁差异审阅

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
