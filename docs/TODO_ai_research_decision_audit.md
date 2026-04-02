> Status note (2026-04-02)
>
> This handoff file contains historical audit items. Several items have already been resolved, including:
> - AI autonomous-agent frontend wiring
> - split live-signals panels for research vs autonomous-agent watchlist
> - removal of hard runtime dependency from autonomous-agent endpoints to AI research runtime bootstrap
>
> Treat unresolved items in this file as historical leads that require re-validation before action.

# AI研究与决策链路审计 TODO

## 目的

本文件用于交接当前交易系统在 `AI研究`、`AI决策`、`AI自治代理` 三条链路上的已知问题、复现方式、修复顺序与验收标准。

适用对象：

- 后续接手的 AI agent
- 人工排查与联调人员

约束：

- 先修稳定性和语义问题，再补 UI
- 每一块修复都必须补回归测试
- 禁止一上来做大范围重构

当前代码基线：

- 分支：`main`
- 快照提交：`32459d9`
- 提交说明：`chore(snapshot): save current ai research and trading ui state`

---

## 本轮只读审计结论

### 1. 已确认的确定性 bug

文件：

- `web/api/ai_research.py`

问题：

- `get_live_signals()` 直接访问 `signal_aggregator._ml_model.is_loaded()`
- 该实现依赖私有属性，导致测试替身对象或轻量实现不兼容

已复现证据：

- 测试失败：`tests/test_ai_research_runtime_and_phase_e.py::test_live_signals_works_with_symbol_field_candidates`
- 报错：`AttributeError: 'types.SimpleNamespace' object has no attribute '_ml_model'`

结论：

- 这是明确后端 bug，不是偶发现象

### 2. AI自治代理后端已存在，但前端未完整接线

后端相关文件：

- `core/ai/autonomous_agent.py`
- `web/api/ai_research.py`

前端相关文件：

- `web/static/js/ai_research.js`
- `web/templates/index.html`

现状：

- 前端主要围绕 `ai_live_decision`
- autonomous agent 的状态、控制、日志能力没有完整暴露在页面

结论：

- 用户会感知为“功能有，但不好用或不完整”

### 3. AI运行时配置很可能只在内存中生效

文件：

- `core/ai/live_decision_router.py`
- `core/ai/autonomous_agent.py`

现状：

- 配置更新主要通过 runtime override 生效
- 未看到可靠的落盘恢复机制

风险：

- 页面上保存的设置，服务重启后丢失
- 用户表现感知为“刚调好，过一会又不对”

### 4. AI研究到策略注册的语义可能冲突

文件：

- `core/deployment/promotion_engine.py`
- `web/api/strategies.py`

现状：

- 当系统不处于 `paper` 模式时，自动 paper promotion 会被拒绝

风险：

- 如果系统运行在 `live`，研究页里“注册/试运行/转策略”动作可能失败
- 前端未必能给出准确解释

### 5. live-signals 的上下文可能与真实交易上下文不一致

文件：

- `web/api/ai_research.py`

现状：

- 市场数据存在写死 `binance` 的风险
- 候选项 exchange/symbol 与研究接口使用的 exchange/symbol 可能不一致

风险：

- AI研究页看到的信号与实盘/回测上下文不一致

### 6. 服务当前未运行，在线联调尚未完成

现状：

- 审计时 `127.0.0.1:8000` 无监听
- `uvicorn` 进程未运行

结论：

- 当前只完成了静态检查与测试复现
- 真实接口链路需要在服务启动后补联调

---

## 已完成的检查

### Python 语法/编译检查

已通过：

- `web/api/ai_research.py`
- `core/ai/autonomous_agent.py`
- `core/deployment/promotion_engine.py`
- `web/api/strategies.py`

### 前端 JS 语法检查

已通过：

- `web/static/js/ai_research.js`
- `web/static/js/app.js`

### 关键测试结果

已执行：

- `tests/test_ai_live_decision_router.py`
- `tests/test_execution_engine_ai_live_decision.py`
- `tests/test_ai_autonomous_agent.py`
- `tests/test_ai_research_autonomous_agent_api.py`
- `tests/test_ai_research_runtime_and_phase_e.py`

当前已知失败：

- `tests/test_ai_research_runtime_and_phase_e.py::test_live_signals_works_with_symbol_field_candidates`

---

## 执行原则

1. 先修后端确定性 bug，再补 UI
2. 先做服务可启动和可观测，再做功能增强
3. 每一个修复块都必须有对应回归测试
4. 不要把研究、决策、注册三条链路混在同一个提交里
5. 不做无必要的目录重构和大面积抽象

---

## 详细执行步骤

## Step 0. 建立基线

目标：

- 确保所有后续修改都在可回退、可复现的基础上进行

操作：

1. 切到提交 `32459d9`
2. 记录 `git status`
3. 记录当前环境变量中与 AI、交易模式、provider 有关的关键项
4. 记录当前启动命令与日志输出位置

验收标准：

- 工作区干净
- 启动方式明确
- 关键环境参数已留痕

---

## Step 1. 修复服务启动与基础健康检查

目标：

- 让服务稳定运行，便于后续联调

操作：

1. 使用 PowerShell 启动服务
2. 确认端口监听
3. 检查以下接口：
   - `/api/status`
   - `/api/ai/runtime-config`
   - `/api/ai/live-signals`
   - `/api/ai/autonomous-agent/status`
4. 如果启动失败：
   - 先查启动日志
   - 再查依赖初始化失败点
   - 最后查配置缺失

需要补充的最小能力：

- 统一健康检查日志
- 接口错误时输出 request path、异常类型、耗时

验收标准：

- 服务稳定运行至少 5 分钟
- 上述 4 个接口都能返回结构化响应

---

## Step 2. 修复 live-signals 的确定性后端 bug

目标：

- 去掉对私有属性 `_ml_model` 的脆弱依赖

目标文件：

- `web/api/ai_research.py`

建议做法：

1. 给信号聚合器增加显式能力判断
2. 或在接口层用 `hasattr` 和安全降级处理
3. 缺模型时返回可解释的降级字段，不抛 500

必须补的测试：

1. 聚合器没有 `_ml_model`
2. 聚合器有模型但未加载
3. 聚合器模型已加载
4. 候选对象只提供部分 symbol 字段

验收标准：

- `tests/test_ai_research_runtime_and_phase_e.py` 全通过
- `/api/ai/live-signals` 在降级场景不报 500

---

## Step 3. 修复 live-signals 的 exchange/symbol 解析问题

目标：

- 研究页的实时信号上下文要与候选策略上下文一致

目标文件：

- `web/api/ai_research.py`

操作：

1. 审查当前 exchange 解析优先级
2. 审查当前 symbol 解析优先级
3. 若写死 `binance`，改为：
   - 候选 exchange
   - 请求参数 exchange
   - 默认值兜底
4. 统一支持以下 symbol 形式：
   - `BTC/USDT`
   - `BTCUSDT`
   - `base + quote`
   - 候选对象里的替代字段

必须补的测试：

1. 不同 exchange 候选
2. 不同 symbol 格式
3. 缺字段时的默认兜底

验收标准：

- 研究信号与策略交易上下文一致
- 不再出现错交易所、错 symbol 的隐性偏差

---

## Step 4. 修复 AI运行时配置不持久的问题

目标：

- 页面保存的 AI 配置在服务重启后仍然有效

目标文件：

- `core/ai/live_decision_router.py`
- `core/ai/autonomous_agent.py`
- `config/settings.py`
- 可能新增一个运行时配置持久化文件

建议方案：

1. 不直接回写 `.env`
2. 增加独立 JSON/YAML runtime config overlay
3. 启动时加载 overlay
4. 更新时先校验，再落盘，再刷新内存

必须补的测试：

1. 修改 live decision 配置后重建对象
2. 修改 autonomous agent 配置后重建对象
3. 配置文件损坏时安全降级

验收标准：

- UI 修改后重启服务，配置不丢
- 配置损坏时系统仍能启动并给出日志

---

## Step 5. 理顺 AI研究 -> 注册 -> 运行 的状态机

目标：

- 避免“研究页能点，但实际上因系统模式冲突而失败”

目标文件：

- `core/deployment/promotion_engine.py`
- `web/api/strategies.py`
- `web/api/ai_research.py`

需要明确的业务语义：

1. 系统 `live` 时，是否允许创建仅 paper 运行的策略
2. AI研究页里的“一键注册”到底是：
   - 仅保存配置
   - 注册 paper 策略
   - 注册 live 策略
3. 被拒绝时前端怎么解释

推荐处理方式：

- 后端与前端都显式区分：
  - `research_only`
  - `register_paper`
  - `register_live`

必须补的测试：

1. 系统 `live`，研究页注册 `paper`
2. 系统 `live`，研究页注册 `live`
3. 系统 `paper`，研究页注册 `paper`
4. 非法模式组合的错误文案

验收标准：

- 所有注册动作都是显式的、可预测的
- 不出现静默失败和语义不清

---

## Step 6. 补齐 autonomous agent 前端接线

目标：

- 把后端已有能力完整暴露到 AI研究页面

目标文件：

- `web/static/js/ai_research.js`
- `web/templates/index.html`

至少应具备的 UI 能力：

1. 查看 autonomous agent 当前状态
2. 查看当前 provider / model / allow_live / cooldown
3. 执行 `start`
4. 执行 `stop`
5. 执行 `run-once`
6. 查看最近 journal
7. 明确区分：
   - AI辅助决策
   - AI自治代理

要求：

- 不要把两套功能混在同一个卡片里
- 所有操作都要有 loading、success、error 状态

验收标准：

- 页面可完整操作 autonomous agent
- 用户能直观看到“为什么没运行/为什么没下单”

---

## Step 7. 增加 AI 决策可观测性

目标：

- 将“感觉不顺”转化为可追踪的日志和决策证据

建议输出字段：

1. `request_id`
2. `timestamp`
3. `symbol`
4. `exchange`
5. `provider`
6. `model`
7. `input_summary`
8. `decision`
9. `confidence`
10. `risk_checks`
11. `execution_allowed`
12. `execution_result`
13. `rejection_reason`

建议位置：

- 后端 journal
- AI研究页的最近决策面板
- 必要时策略详情页增加只读摘要

验收标准：

- 用户能定位“为什么是 hold”
- 用户能定位“为什么没有发单”
- 用户能定位“为什么发单后又被风控拦截”

---

## Step 8. 做真实页面联调

目标：

- 验证 UI 与 API 的整条链路

手工联调路径：

1. 打开 AI研究页
2. 加载候选列表
3. 点击实时信号加载
4. 修改 AI runtime config
5. 刷新页面验证配置是否保留
6. 启动 autonomous agent
7. 运行一次 `run-once`
8. 查看 journal
9. 从研究页触发策略注册
10. 到策略页确认注册结果

需要重点观察：

1. 页面卡死
2. 无提示 fallback
3. 接口超时
4. 状态文案与真实状态不一致
5. 按钮可点但后端不支持

验收标准：

- 无白屏
- 无明显 silent failure
- 所有核心按钮状态一致

---

## Step 9. 回归测试与提交策略

目标：

- 每块功能修复都可独立验证和回退

建议提交拆分：

1. `fix(ai-research): harden live-signals model availability handling`
2. `fix(ai-research): resolve exchange and symbol consistently`
3. `fix(ai-runtime): persist live decision and autonomous config`
4. `fix(promotion): clarify research to paper/live registration semantics`
5. `feat(ai-ui): wire autonomous agent controls and journal`
6. `feat(observability): add ai decision traces and failure reasons`

要求：

- 每个提交必须附带对应测试
- 每个提交在本地都能单独通过相关测试

---

## 建议测试命令

### 后端核心测试

```powershell
pytest -q `
  tests/test_ai_live_decision_router.py `
  tests/test_execution_engine_ai_live_decision.py `
  tests/test_ai_autonomous_agent.py `
  tests/test_ai_research_autonomous_agent_api.py `
  tests/test_ai_research_runtime_and_phase_e.py
```

### Python 编译检查

```powershell
python -m py_compile `
  web/api/ai_research.py `
  core/ai/autonomous_agent.py `
  core/deployment/promotion_engine.py `
  web/api/strategies.py
```

### 前端 JS 语法检查

```powershell
node --check web/static/js/ai_research.js
node --check web/static/js/app.js
```

---

## 执行优先级

推荐顺序：

1. 服务启动与健康检查
2. `live-signals` 后端确定性 bug
3. `exchange/symbol` 上下文一致性
4. AI运行时配置持久化
5. AI研究到注册的状态机语义
6. autonomous agent 前端接线
7. 决策日志与可观测性
8. 完整 UI 联调

---

## 不建议先做的事

1. 不要先大改页面视觉样式
2. 不要先重构整个 AI 模块目录
3. 不要把多个链路问题揉成一次提交
4. 不要在没有测试和复现的情况下凭感觉修

---

## 交付物要求

后续 AI 完成任务后，至少应交付：

1. 修复后的代码提交记录
2. 通过的测试清单
3. 页面联调结果
4. 仍未解决的问题列表
5. 若有权衡，说明为什么这样做

---

## 结束条件

满足以下条件才算本轮完成：

1. 服务能稳定启动
2. AI研究页核心接口不再报错
3. AI决策配置可持久化
4. AI自治代理可从前端可视化操作
5. 研究到注册的行为语义清晰
6. 用户能看到决策与拒绝原因
7. 关键测试全部通过

