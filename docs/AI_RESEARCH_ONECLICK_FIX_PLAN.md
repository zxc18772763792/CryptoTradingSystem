# AI研究一键研究改进与执行计划

> 修订时间: 2026-04-08  
> 参考实现: `web/templates/index.html`, `web/static/js/ai_research.js`, `web/static/js/ai_research_agent.js`, `web/api/ai_research.py`, `core/research/orchestrator.py`, `core/research/strategy_research.py`

---

## 1. 当前页面现状结论

结合今天的计划文档、当前 AI 研究页和 AI 自治代理页实现，现状可以归纳为：

1. **页面职责已经基本分开，方向是对的**
   - AI 研究页负责“研究目标 → 提案 → 候选 → 验证 → 注册/部署建议”。
   - AI 自治代理页负责“实时盯盘 → 执行动作 → 风险纪律 → 日志 → 复盘”。
   - 两页都已经在文案和信号展示上明确强调“研究候选运行信号”和“自治代理 watchlist / 聚合信号”不能混看。

2. **真正卡住 one-click 可用性的不是布局，而是执行链路**
   - 一键研究原先仍是同步等待 `generate -> run -> deploy`，HTTP 必然超时。
   - 研究页虽然已有任务队列、候选卡片和反馈盒子，但 one-click 并没有充分利用现成的后台 job 模式。

3. **AI 自治代理页当前能力已经比较完整，不应被这次需求误伤**
   - 代理状态、启动/停止、单次试跑、自动选币、风险阈值、日志、复盘、记分卡、聚合信号都已具备。
   - 本次应坚持“研究链路修复优先，不把研究候选自动混入自治代理执行链路”。

4. **真正需要补的是“研究页完成后的交接说明”，而不是链路强耦合**
   - 研究候选部署成功后，仍优先在 AI 研究页观察候选和候选运行信号。
   - AI 自治代理页继续只服务自治代理，不自动接管研究候选。

---

## 2. 本次改进目标

本轮改进目标不是重做页面，而是把现有页面真正跑通并可运维：

1. one-click 必须在 30 秒内返回任务受理结果，而不是同步阻塞到研究结束。
2. 研究任务必须有后台 job、进度反馈、失败原因和候选产出信息。
3. 研究默认样本窗口要足够合理，不能再用 3 天默认值误导回测。
4. 数据缺失、策略不可执行、超时等错误必须给出可操作提示。
5. AI 研究页与 AI 自治代理页保持边界清晰，但要补齐“去哪里继续看”的提示。
6. 修复后要能通过核心测试，并完成服务重启。

---

## 3. 问题矩阵与改进项

### P0: one-click 同步阻塞 HTTP，导致必然超时

**问题**
- 原先 `oneclick_ai_research_deploy()` 直接同步执行完整研究。
- 研究过程包含参数搜索、回测、OOS、walk-forward、候选生成与 LLM 文案，天然是分钟级流程。

**改进**
- one-click 拆为两阶段：
  - 阶段 1：生成提案并提交后台研究任务，立即返回 `proposal_id/job_id`
  - 阶段 2：研究完成后，再单独执行候选部署
- 前端使用轮询 job 状态，不再用长 HTTP 超时硬扛。

**结果**
- one-click 入口从“同步串行流程”改成“后台任务 + 前端轮询 + 二段部署”。

---

### P0: `days` 默认值过低，导致样本明显不足

**问题**
- 手动运行默认 3 天。
- one-click 也复用了这个输入，导致研究常因样本不足而失败或统计不稳定。

**改进**
- 手动运行默认值改为 30 天。
- 新增 one-click 专用 `#ai-oneclick-days`，默认 30 天，与手动运行解耦。

**结果**
- 研究页的默认行为从“容易出错”改成“更接近可用区间”。

---

### P1: `strategies` 被过滤为空时，研究直接报错

**问题**
- 当规划器输出的策略模板全部不可执行时，原逻辑会直接抛 `ValueError`。

**改进**
- 后端增加最终安全网：
  - `BollingerBandsStrategy`
  - `RSIStrategy`
  - `MACDStrategy`
- 并在 proposal metadata 中写入 `emergency_fallback_strategies` 便于排查。

**结果**
- 策略模板质量不稳定时，不再把整条研究链路直接打断。

---

### P1: 数据缺失错误不友好，用户不知道下一步做什么

**问题**
- 1 秒级数据缺失、历史 K 线缺失等错误信息可读性差，前端也没有针对性建议。

**改进**
- 后端报错统一改为明确中文提示：
  - 哪个交易所
  - 哪个交易对
  - 哪些时间框架
  - 当前天数
  - 建议去数据管理回填，或切换时间框架/标的
- 前端 `buildOneClickFailureFeedback()` 增加数据错误、超时、策略不可执行等分类提示。

**结果**
- 用户看到的是“下一步怎么处理”，而不是模糊异常。

---

### P1: 缺少足够清晰的研究进度反馈

**问题**
- 原先后台 job 仅有状态，没有足够细的进度描述。

**改进**
- 后端 job 新增 `progress` 字段，并在这些阶段更新：
  - `queued`
  - `research_running`
  - `llm_rationale`
  - `finalizing`
  - `completed / cancelled / failed`
- `run_strategy_research()` 增加 `progress_callback`，回传当前策略/周期组合与完成度。
- API 透传 `job.progress` 与已完成结果摘要。
- 前端轮询时展示耗时、当前进度、策略/周期信息。

**结果**
- one-click 不再是“按下按钮后长时间没消息”的黑盒。

---

### P1: walk-forward 无门控，数据不够时还硬跑

**问题**
- 当 IS/OOS 样本不够分割时，walk-forward 仍执行，容易产生无意义结果并浪费时间。

**改进**
- `can_split=False` 时直接跳过 walk-forward。
- `_run_purged_walk_forward()` 增加最小样本数保护。

**结果**
- 研究稳定性更高，耗时也更合理。

---

### P2: LLM 候选解释生成会额外拖慢研究完成时间

**问题**
- 候选解释原先 best-effort 但没有总超时，异常也可能被静默吞掉。

**改进**
- LLM rationale 增加 30 秒总超时。
- 单个候选失败写 debug 日志。

**结果**
- 解释文案不会再拖垮整条研究链路，排障也更容易。

---

### P2: AI 研究页与 AI 自治代理页需要更清晰的交接说明

**问题**
- 页面职责虽然已分开，但 one-click 完成后用户仍可能误以为研究候选会自动进入自治代理执行链路。

**改进**
- 在 AI 研究页 one-click 区域补充轻量边界提示：
  - 这里负责研究与候选
  - 实时盯盘与执行属于 AI 自治代理页
- 继续保持：
  - 研究页只看研究候选运行信号
  - 自治代理页只看自治代理 watchlist / 聚合信号

**结果**
- 用户更容易理解“研究”和“自治执行”是两条并行但分离的链路。

---

## 4. 团队执行编排

本次按四条工作流并行执行：

### A. 研究内核后端组

**职责**
- `core/research/orchestrator.py`
- `core/research/strategy_research.py`

**负责项**
- 策略回退安全网
- walk-forward 门控
- 数据缺失友好报错
- LLM rationale 超时与日志
- 后台 job progress 写入

**完成状态**
- 已完成

---

### B. 编排与 API 组

**职责**
- `web/api/ai_research.py`

**负责项**
- one-click 改成“排队研究”
- 新增二段部署端点
- job-status 透传 progress 与结果摘要
- one-click 返回结构兼容前端轮询

**完成状态**
- 已完成

---

### C. 研究页前端组

**职责**
- `web/static/js/ai_research.js`
- `web/templates/index.html`

**负责项**
- one-click 三阶段交互
- one-click 专用 days
- 默认 days 调整
- 更友好的错误与进度反馈
- 研究页/自治代理页边界提示补强

**完成状态**
- 已完成

---

### D. 联调、验证与启停组

**职责**
- 核心测试
- 静态校验
- 服务重启

**负责项**
- py_compile
- one-click/API/页面资产相关测试
- 服务状态检查与重启

**完成状态**
- 已完成

---

## 5. 本次实际改动摘要

### 后端
- `web/api/ai_research.py`
  - one-click 入口改为返回后台任务
  - 新增 `/oneclick/deploy-candidate`
  - job-status 返回嵌套 `job.progress` 与研究结果摘要

- `core/research/orchestrator.py`
  - 增加研究 job progress 更新器
  - 增加策略回退安全网
  - 增加 LLM rationale 超时与失败日志

- `core/research/strategy_research.py`
  - 增加 progress callback
  - 增加 walk-forward 门控与最小样本保护
  - 优化数据缺失错误文案

### 前端
- `web/static/js/ai_research.js`
  - one-click 改为三阶段流程
  - 增加 job 轮询解析和进度展示
  - 增强错误分类
  - 手动运行默认 days 调整为 30

- `web/templates/index.html`
  - 新增 `#ai-oneclick-days`
  - `#run-days` 默认值改为 30
  - 补充研究页与自治代理页的职责边界提示

### 测试
- `tests/test_ai_research_oneclick_api.py`
  - 更新为异步 one-click + 二段部署模型

- `tests/test_ai_research_phase5_ui_assets.py`
  - 去掉 `news_tab_runtime.js` 版本号硬编码，改为版本无关断言

---

## 6. 验收标准

本轮以以下结果作为验收：

1. 点击 one-click 后，30 秒内必须返回任务已受理，而不是前端超时。
2. one-click 执行期间，页面能看到明确的阶段反馈与任务进度。
3. 没有候选时，页面展示“研究完成但未产出可部署候选”，不是模糊失败。
4. 数据缺失时，错误信息要能指导用户去数据管理回填。
5. 研究页和自治代理页继续保持边界，不把候选运行信号与自治代理信号混在一起。
6. 核心测试通过，服务可正常重启。

---

## 7. 本次验证记录

已通过：

- `node --check web/static/js/ai_research.js`
- `pytest -q tests/test_ai_research_oneclick_api.py`
- `pytest -q tests/test_ai_research_phase5_ui_assets.py`
- `pytest -q tests/test_ai_research_phase4_runtime.py tests/test_ai_research_autonomous_agent_api.py`
- 合并回归：`pytest -q tests/test_ai_research_oneclick_api.py tests/test_ai_research_phase4_runtime.py tests/test_ai_research_phase5_ui_assets.py tests/test_ai_research_autonomous_agent_api.py`
- `py_compile`:
  - `web/api/ai_research.py`
  - `core/research/orchestrator.py`
  - `core/research/strategy_research.py`

---

## 8. 重启与上线动作

建议使用统一入口：

```bat
.\web.bat stop -IncludeWorkers
.\web.bat start
.\web.bat status
```

说明：
- 默认启动不会自动拉起 AI 自治代理，这符合当前“研究链路”和“自治执行链路”分开的产品意图。
- 若需要连自治代理一起启动，再显式使用 `-StartAutonomousAgent`。

---

## 9. 后续建议

本轮已经解决 one-click 的可用性问题。后续可以继续做两类增强：

1. 在研究页增加更细粒度的“当前策略 / 当前周期 / 已完成组合数”可视化进度条。
2. 为 AI 自治代理页增加一个只读的“最近研究产出摘要入口”，仅作为导航，不做链路混用。

