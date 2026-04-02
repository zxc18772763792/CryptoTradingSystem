# AI 研究 / AI 自治代理后续修复计划

日期：2026-04-02

## 目标

在已完成“AI 研究”和“AI 自治代理”运行信号与执行链路解耦的基础上，继续清理残留耦合、启动语义不一致、文档偏差和兼容壳字段，避免后续出现“页面看起来分开了，但运行链路仍然互相牵连”的问题。

## 当前结论

- 已完成：
  - AI 研究候选实时信号与自治代理 watchlist 实时信号分离
  - 自治代理不再消费研究候选/champion 作为实时执行前提
  - 新闻引擎已纳入 `.\web.bat start` 默认启动链路
- 仍有后续工作：
  - 启动链路上，自治代理 `auto_start` 语义与 API/运行时配置不一致
  - API 层面，自治代理控制接口仍挂在 `ai_research` 路由下，并依赖 `ensure_ai_research_runtime_state`
  - 代码和文档里还保留部分旧时代的研究耦合兼容壳和描述

## 优先级与待修项

### P0: 自治代理 auto_start 语义不一致

现状：
- `web/main.py` 只读取 `settings.AI_AUTONOMOUS_AGENT_AUTO_START`
- `core/ai/autonomous_agent.py` 返回的 `config.auto_start` 也是环境变量值
- `web/api/ai_research.py` 的 `AIAutonomousAgentConfigUpdateRequest` 却仍暴露 `auto_start`
- 但 `update_runtime_config()` 并不会持久化或应用 `auto_start`

风险：
- API/调用方会误以为 `POST /runtime-config/autonomous-agent` 可以改启动自启行为
- 实际重启后不生效，形成“保存成功但没有效果”的假象

建议：
- 二选一，必须统一：
  - 方案 A：明确 `auto_start` 仅由环境变量控制，从 API schema 中移除，并在文档中写清楚
  - 方案 B：允许 `auto_start` 进入 overlay 持久化，并让 `web/main.py` 启动时优先读取持久化值

建议文件：
- `E:\9_Crypto\crypto_trading_system\web\api\ai_research.py`
- `E:\9_Crypto\crypto_trading_system\core\ai\autonomous_agent.py`
- `E:\9_Crypto\crypto_trading_system\web\main.py`
- `E:\9_Crypto\crypto_trading_system\STARTUP.md`
- `E:\9_Crypto\crypto_trading_system\.env.example`

### P1: 自治代理控制接口仍依赖 AI 研究运行态初始化

现状：
- `web/api/ai_research.py` 中的自治代理接口（status/start/stop/run-once 等）都先调用 `ensure_ai_research_runtime_state(request.app)`
- 这说明自治代理控制 API 仍然挂在研究路由的初始化假设之下

风险：
- 研究 registry 初始化失败、路径异常、文件损坏时，自治代理控制接口也可能被连带影响
- 架构语义不清，后续维护者容易继续把“自治代理 = 研究子功能”理解成默认前提

建议：
- 将自治代理接口拆到独立 router，例如 `web/api/ai_agent.py`
- 或至少把自治代理接口对 `ensure_ai_research_runtime_state` 的依赖移除

建议文件：
- `E:\9_Crypto\crypto_trading_system\web\api\ai_research.py`
- `E:\9_Crypto\crypto_trading_system\web\main.py`
- `E:\9_Crypto\crypto_trading_system\core\research\orchestrator.py`

### P1: live decision router 仍保留研究上下文耦合

现状：
- `core/ai/live_decision_router.py` 仍显式调用 `resolve_runtime_research_context`
- 这条链路不属于自治代理，但仍属于 AI 运行时决策体系的一部分

风险：
- 如果最终目标是“AI 研究”和所有运行时 AI 决策链路彻底分层”，这里仍是残留耦合点
- 容易让后续改动再次把研究输出直接塞回实时决策

建议：
- 先明确边界：
  - 若 `live_decision_router` 被定义为“研究辅助 veto 层”，则文档明确，不急于改
  - 若目标是“运行时 AI 决策完全独立于研究 runtime”，则需要继续拆

建议文件：
- `E:\9_Crypto\crypto_trading_system\core\ai\live_decision_router.py`
- `E:\9_Crypto\crypto_trading_system\docs\AI_AUTONOMY_IMPLEMENTATION_PLAN.md`

### P2: 自治代理内部仍保留 research_context 兼容壳

现状：
- `core/ai/autonomous_agent.py` 仍在 context/journal/debug payload 中保留 `research_context`
- 当前内容已经是 decoupled stub，不再承载研究候选信息

风险：
- 新调用方会误以为这个字段仍然是有效业务输入
- 兼容壳长期保留会拖慢后续 schema 清理

建议：
- 先确认前端/API/审计面板是否仍需要该字段占位
- 若无必须依赖，逐步移除或重命名为更中性的 runtime annotation 字段

建议文件：
- `E:\9_Crypto\crypto_trading_system\core\ai\autonomous_agent.py`
- `E:\9_Crypto\crypto_trading_system\web\api\ai_research.py`
- `E:\9_Crypto\crypto_trading_system\tests\test_ai_autonomous_agent.py`

### P2: 启动文档没有覆盖自治代理随服务启动的真实规则

现状：
- `STARTUP.md` 已清楚写了新闻引擎默认启动
- 但没有写自治代理是否会随服务自动拉起、由谁控制、如何查看、如何手动启动

风险：
- 操作人员会默认认为“服务起来了，自治代理也起来了”
- 实际当前逻辑是：只有 `AI_AUTONOMOUS_AGENT_AUTO_START=true` 时才会在服务启动时自动拉起

建议：
- 在 `STARTUP.md` 单独增加“AI 自治代理启动规则”章节
- 明确区分：
  - 服务启动
  - 新闻引擎启动
  - 自治代理启动
  - 手动 API 启动

建议文件：
- `E:\9_Crypto\crypto_trading_system\STARTUP.md`
- `E:\9_Crypto\crypto_trading_system\.env.example`

### P2: 前端和文档还有少量残留/死代码

现状：
- `web/static/js/ai_research.js` 中存在重复定义的 `loadLiveSignals()`，前者是旧逻辑，后者是新逻辑
- `docs/AI_AUTONOMY_IMPLEMENTATION_PLAN.md` 仍保留“自治代理消费研究结果”的旧目标表述

风险：
- 前端存在阅读歧义和二次修改误用风险
- 文档继续强化旧架构，容易把后续开发带偏

建议：
- 删除死代码和旧注释
- 更新文档边界：AI 研究负责研究、候选、回测、注册；AI 自治代理负责 watchlist、聚合信号、执行决策

建议文件：
- `E:\9_Crypto\crypto_trading_system\web\static\js\ai_research.js`
- `E:\9_Crypto\crypto_trading_system\docs\AI_AUTONOMY_IMPLEMENTATION_PLAN.md`

## 推荐修复顺序

1. 统一自治代理 `auto_start` 语义
2. 补全 `STARTUP.md` 与 `.env.example`
3. 清理前端死代码与旧文档表述
4. 将自治代理接口从研究初始化依赖中拆出
5. 评估是否继续拆 `live_decision_router`
6. 收尾移除 `research_context` 兼容壳

## 团队并行分工建议

### Workstream A: 启动链路与配置语义

- 负责人方向：后端 / 运维
- 处理：
  - `auto_start` 语义统一
  - `STARTUP.md`、`.env.example`、启动命令说明

### Workstream B: API 与路由解耦

- 负责人方向：后端
- 处理：
  - 自治代理接口从研究初始化依赖中拆离
  - 保证 agent API 在研究 runtime 异常时仍可工作

### Workstream C: 前端与文档收尾

- 负责人方向：前端 / 文档
- 处理：
  - 删除重复 `loadLiveSignals()` 旧逻辑
  - 更新页面说明与架构文档

### Workstream D: 运行时 AI 边界复核

- 负责人方向：架构 / 策略
- 处理：
  - 评估 `live_decision_router` 是否继续依赖研究上下文
  - 决定是否进入下一轮彻底拆分

## 验收标准

- 服务重启后，自治代理启动行为与配置/文档完全一致
- API 层不存在“保存成功但启动行为不生效”的假配置
- 新闻引擎、自主代理、研究调度三条链路的启动说明清晰分离
- 自治代理相关接口在研究 runtime 初始化异常时不被无关阻塞
- 前端与文档不再出现“自治代理依赖研究候选运行”的旧叙述
