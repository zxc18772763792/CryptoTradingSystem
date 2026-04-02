# AI 研究 / AI 自治代理解耦收口说明

日期：2026-04-02

## 目的

这份文档用于给当前“AI 研究”和“AI 自治代理”解耦工作做一次阶段性收口，明确三件事：

1. 现在代码里哪些边界已经真正拆开了。
2. 还有哪些地方只是“语义上分开”，但实现上仍有残留耦合或兼容壳。
3. 下一轮如果继续清理，应该按什么顺序做，避免把已经拆开的边界重新混回去。

## 结论摘要

- 这轮解耦的核心目标已经达到：AI 自治代理不再以 AI 研究候选、champion 或研究 runtime 作为实时执行前提。
- 当前剩余问题主要属于“物理代码拆分不彻底、命名与文档仍有历史包袱、少量运行时上下文字段仍保留兼容壳”，而不是“执行链路仍然强耦合”。
- 因此，当前阶段应把这项工作定义为：
  - 逻辑边界：已完成
  - 运行链路解耦：已完成
  - 代码物理拆分与历史字段清理：未完全完成

## 当前真实边界

截至 2026-04-02，建议统一按下面的边界理解系统：

- AI 研究：
  - 负责 proposal、experiment、candidate、回测、评估、注册和研究态 live signals。
  - 负责“研究产物”的生成和管理，不直接代表自治代理的实时执行依据。
- AI 自治代理：
  - 负责 watchlist/universe 选择、聚合信号、运行时决策、journal 和执行提交。
  - 不再以研究候选或 champion 作为实时执行前置条件。
- live decision router：
  - 属于独立的运行时守门层。
  - 目前仍可读取 research context 作为 advisory/veto 输入。
  - 它不是自治代理本体，也不应被继续描述为“研究页的附属能力”。
- 启动链路：
  - `.\web.bat start` 负责服务和新闻引擎的默认启动。
  - AI 自治代理是否随服务启动，仍只由 `AI_AUTONOMOUS_AGENT_AUTO_START` 决定。
  - 运行时配置保存不会改变进程重启时的自启动规则。

## 本轮已完成项

### 1. 自治代理已从研究候选执行链路中拆出

- 自治代理的运行逻辑已经基于自身的 watchlist、聚合信号和运行时上下文工作。
- `core/ai/autonomous_agent.py` 中保留的 `research_context` 已经退化为 decoupled stub，而不是研究候选输入。
- 这意味着“研究结果可以影响观察与治理说明”，但不再是“自治代理必须消费研究候选后才能运行”。

### 2. 自治代理 API 路由已经独立挂载

- `web/api/ai_agent.py` 已存在，并在 `web/main.py` 中单独挂载。
- 对外语义上，自治代理已不再要求通过研究路由来暴露接口。
- `status/start/stop/run-once/journal/review/symbol-ranking/live-signals` 等接口均有独立 agent 路由入口。

结论：

- “自治代理控制接口仍必须依赖研究页面入口”这一判断已经过时。
- 现在的问题不是“没有独立路由”，而是“独立路由背后的处理函数仍复用旧模块实现”。

### 3. `auto_start` 的运行时配置语义已经基本对齐

- `AIAutonomousAgentConfigUpdateRequest` 中已经不再暴露 `auto_start`。
- `core/ai/autonomous_agent.py` 里返回的 `config.auto_start` 已明确只是环境层面的启动信息。
- `web/main.py` 仍只根据 `settings.AI_AUTONOMOUS_AGENT_AUTO_START` 决定服务启动时是否自动拉起自治代理。

结论：

- 当前代码实际上已经选择了“方案 A”：
  - `auto_start` 由环境变量控制；
  - runtime config 不负责修改进程级启动行为。
- 因此，之前“API 仍可改 auto_start”的担忧已不再成立。

### 4. 启动文档已覆盖自治代理真实启动规则

- `STARTUP.md` 已单独说明：
  - 新闻引擎是默认启动链路的一部分；
  - 自治代理不会因为 `.\web.bat start` 自动启动，除非环境变量 `AI_AUTONOMOUS_AGENT_AUTO_START=true`；
  - 若服务已启动但 agent 未运行，应单独检查并手动调用启动接口。

结论：

- “服务起来了等于自治代理也起来了”这种隐含假设，已经在文档层面被纠正。

## 仍未完全收尾的部分

### P1: 独立路由已存在，但物理实现仍未完全拆开

现状：

- `web/api/ai_agent.py` 目前本质上仍是一个薄包装层。
- 具体处理逻辑、请求模型和部分响应构造仍复用 `web/api/ai_research.py` 中的实现。

风险：

- 代码组织仍会给维护者造成“agent 还是 research 子模块”的心理暗示。
- 后续如果继续在 `ai_research.py` 中堆自治代理逻辑，容易让边界再次变模糊。

建议：

- 将自治代理相关的 request model、handler、helper 逐步迁移到独立模块。
- 可以保守地按下面的拆法推进：
  - `web/api/ai_agent.py` 只保留路由定义；
  - 新建独立 handler 模块承接 agent 逻辑；
  - 最后再把 `ai_research.py` 中对应实现删除或仅保留兼容别名。

### P1: `live_decision_router` 仍保留 research context 依赖

现状：

- `core/ai/live_decision_router.py` 仍显式依赖 `resolve_runtime_research_context`。
- prompt 中也仍把 `research_context` 作为 advisory source 输入。

风险：

- 如果项目长期目标是“运行时 AI 决策体系完全独立于研究 runtime”，这里仍然是剩余耦合点。
- 如果边界不先讲清楚，后续很容易再次把研究 candidate/champion 直接回灌到实时执行判断里。

建议：

- 先做边界决策，再决定是否继续拆：
  - 如果将 `live_decision_router` 定义为“研究辅助 veto 层”，则保留该依赖，并把角色写清楚；
  - 如果目标是“运行时 AI 守门完全独立”，则需要把这部分 research context 替换成更中性的 runtime evidence。

### P2: `research_context` 兼容壳仍在自治代理内部保留

现状：

- `core/ai/autonomous_agent.py` 仍在 context、journal、status payload 中保留 `research_context` 字段。
- 当前该字段已经不是研究候选载体，而是 decoupled stub / 兼容占位。

风险：

- 新调用方仍可能误判该字段是有效研究输入。
- 长期保留会拖慢 schema 和前端说明的进一步清理。

建议：

- 先确认前端、日志、诊断面板是否还依赖这个字段形状。
- 若没有强依赖，下一轮可考虑：
  - 改名为更中性的 `runtime_annotation` 或 `advisory_context`；
  - 或在 API 输出层直接移除，仅在内部调试保留。

### P2: 历史文档仍有旧叙述，需要统一口径

现状：

- 一些较早的设计文档仍会出现“自治代理消费研究结果”的旧表述。
- 虽然 `docs/AI_AUTONOMY_IMPLEMENTATION_PLAN.md` 已加状态说明，但正文仍保留历史方向文字。

风险：

- 后续阅读者可能把“历史计划”当成“当前实现目标”。
- 容易导致下一轮改动再次偏回旧架构。

建议：

- 统一给历史设计文档加明显的 status note。
- 在后续架构说明中固定采用下面的口径：
  - AI 研究负责研究产物；
  - AI 自治代理负责实时观察与执行；
  - live decision router 负责守门；
  - 三者允许共享市场数据和治理约束，但不应重新形成执行强依赖。

## 不应回退的边界

后续继续迭代时，建议把下面几条当成硬边界：

- 不要重新让自治代理以 research candidate/champion 作为执行前提。
- 不要把 agent API 再挂回“必须先初始化 research runtime 才能使用”的路径。
- 不要让 UI 的 runtime config 保存行为暗示“它会改变进程下次启动方式”。
- 不要把 live decision router 描述成“自治代理的一部分”或“研究页附属功能”，除非先重新定义架构边界。

## 推荐收尾顺序

1. 完成自治代理 handler 与 request model 的物理拆分。
2. 对 `live_decision_router` 做一次边界决策：保留 advisory research context，还是继续去研究化。
3. 清理自治代理中的 `research_context` 兼容壳。
4. 给所有历史文档统一补上“历史方向 / 当前真实边界”状态说明。

## 验收标准

- 新维护者只看路由和文档，就能清楚区分 AI 研究、AI 自治代理和 live decision router 三条链路。
- 自治代理接口的实现位置不再继续向 `ai_research.py` 回流。
- 服务重启后的自治代理启动行为，与 `AI_AUTONOMOUS_AGENT_AUTO_START` 和 `STARTUP.md` 说明完全一致。
- 自治代理对外 payload 中不再保留会误导调用方的历史研究字段，或这些字段已被明确重命名并注明用途。
- 历史文档不会再把“自治代理消费研究候选”描述成当前系统目标。

## 最终判断

截至 2026-04-02，这项工作最准确的描述应为：

- “AI 研究”和“AI 自治代理”的运行链路解耦已经完成。
- 剩余工作属于架构收尾，而不是核心逻辑尚未拆开。
- 下一轮重点不应再是证明两者能否分离，而应是把已经分离的边界在代码组织、命名、文档和诊断字段上彻底固化。
