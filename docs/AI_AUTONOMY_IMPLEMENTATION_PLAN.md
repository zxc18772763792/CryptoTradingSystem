> Status note (2026-04-02)
>
> This document contains historical design directions. It does not fully match the current runtime boundary anymore.
>
> Current boundary:
> - AI research: candidate generation, evaluation, and runtime observation
> - AI autonomous agent: independent aggregated-signal execution loop
> - live decision router: separate runtime guard
>
> Any older wording in this file about the autonomous agent consuming research results should be treated as historical context, not current behavior.

# AI 研究页与自主研究/决策改造计划

## 目标

把当前“AI 研究页 = 从固定策略库里选模板 + 调参数 + 回测”的链路，
逐步升级为“AI 能提出研究假设、生成策略草案、搜索候选、验证、部署，再把研究结果接入运行时决策”的闭环。

这份计划可以直接共享给 Claude / Codex 之类的协作模型，作为后续改造的统一上下文。

## 当前约束

1. `core/ai/research_planner.py` 目前只能从 `STRATEGY_REGISTRY` 挑模板。
2. `core/research/strategy_research.py` 目前通过 `_build_positions(strategy_name, ...)` 的硬编码分支执行回测。
3. `ExperimentSpec` / `ResearchProposal` 里缺少“策略草案 IR、谱系、搜索预算”这些字段。
4. `core/ai/autonomous_agent.py` 已经能自主做交易动作，但它与研究产物没有形成正式闭环。
5. `web/static/js/ai_research.js` 现在更像“候选查看器”，不是“研究流程与搜索过程面板”。

## 总体实施分期

### Phase 1: 基础骨架

目标：不破坏现有模板研究链路，先让系统能保存和展示“自主研究草案”。

改造点：

1. 在 `ResearchProposal` / `ExperimentSpec` 中加入：
   - `research_mode`
   - `strategy_drafts`
   - `search_budget`
   - `lineage`

2. 在 planner 中把 `llm_research_output.proposed_strategy_changes` 规范化为策略草案 IR。

3. 让 proposal / experiment / candidate metadata 传播这些信息，便于 UI 和后续执行器使用。

4. 在 AI 研究页展示：
   - 研究模式
   - AI 假设
   - 策略草案
   - 搜索预算
   - 谱系信息

交付标准：

1. 不影响现有 proposal 创建、研究运行、候选生成、注册审批。
2. 前端可见“模板研究 / 混合研究 / 自主草案”状态。
3. 自动化测试覆盖新 schema 和 planner 传播。

### Phase 2: DSL/IR 执行器

目标：允许 AI 提交不依赖固定类名的策略表达。

改造点：

1. 新增 `StrategyProgram` / `StrategyDSL` 执行层。
2. 把策略表达拆成：
   - 特征定义
   - 入场逻辑
   - 出场逻辑
   - 风控逻辑
3. 在 research engine 中增加 `template` 与 `dsl` 双执行路径。

交付标准：

1. 至少支持一小组可验证 DSL 原语。
2. 回测结果结构与现有模板研究结果兼容。

### Phase 3: 搜索循环

目标：让 AI 真正做“研究搜索”，而不是一次性生成一个方案。

改造点：

1. 新增 `search_loop`：
   - hypothesis -> draft -> evaluate -> critique -> mutate
2. 记录 lineage：
   - `lineage_id`
   - `parent_candidate_id`
   - `generation`
   - `mutation_notes`
3. 引入 novelty / redundancy 约束，避免只在同一模板附近反复调参。

交付标准：

1. 系统能保留 champion/challenger 谱系。
2. UI 可显示“本轮搜索评估了多少个草案，淘汰原因是什么”。

### Phase 4: 研究结果接入运行时决策

目标：让自治代理消费研究结果，而不是与研究链路割裂。

改造点：

1. 让 `autonomous_agent` 从“单模型直接出动作”升级为：
   - 聚合当前 champion 候选
   - 结合实时信号、相关性、PnL、风控状态做运行时决策
2. `live_decision_router` 保持 veto / reduce-only / allow 角色，不让 research agent 直接越权。

交付标准：

1. 运行时决策可回溯到对应 proposal / candidate。
2. 保持治理边界：研究输出不能直接变成强制下单指令。

### Phase 5: 页面工程化

目标：让 AI 研究页真正成为“研究控制台”。

改造点：

1. 拆分 `web/static/js/ai_research.js` 为多个模块：
   - `planner`
   - `runtime`
   - `candidates`
   - `agent`
   - `diagnostics`
2. 补“研究流程可视化”：
   - 假设
   - 搜索
   - 验证
   - 决策
   - 部署
3. 清理编码/乱码问题。

## 本次提交范围

本次只落地 Phase 1：

1. 新增自主研究相关 schema 字段。
2. planner 从 LLM 研究输出中生成策略草案。
3. experiment / candidate 传播这些信息。
4. AI 研究页展示研究模式、AI 假设、策略草案、搜索预算。
5. 补最小测试。

## 风险边界

1. 本次不改变现有实盘执行与交易权限边界。
2. 本次不让 LLM 直接生成并执行任意 Python 策略代码。
3. 本次仍以模板研究链路为主执行路径；自主草案先作为一等数据结构进入系统。
4. 真正的 DSL 执行与搜索循环留到 Phase 2/3，避免一次改动过大。

## 2026-03-25 Runtime Note

1. The default LLM runtime has been switched from GLM to an OpenAI-compatible `gpt-5.4` endpoint.
2. Base URL default: `https://vpsairobot.com/v1`.
3. The current codebase keeps the internal provider name `codex` as the OpenAI-compatible adapter to avoid breaking existing API/UI contracts.
4. OpenAI-compatible paths should use the Responses API (`/v1/responses`) instead of `chat/completions`.
5. Real-time trading loops keep short local timeouts; do not blindly expand them to the model vendor's 1800s server timeout.
