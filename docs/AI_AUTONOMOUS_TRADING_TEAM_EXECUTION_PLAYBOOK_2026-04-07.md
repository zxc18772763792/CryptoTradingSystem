# AI 自治交易代理团队执行手册

更新时间：2026-04-07
主文档：`docs/AI_AUTONOMOUS_TRADING_EXECUTION_PLAN_2026-04-07.md`

## 1. 文档目的

这份文档用于把主方案转换成可直接执行的团队开发计划。

主文档负责回答：

1. 为什么系统当前不能稳定盈利
2. 目标架构是什么
3. 分阶段要做什么

这份执行手册负责回答：

1. 具体由谁做
2. 每个 Sprint 做什么
3. 哪些任务可以并行
4. 哪些任务必须串行
5. 每个阶段的完成定义是什么
6. 团队按什么节奏推进

## 2. 执行假设

本执行手册基于以下假设：

1. 当前工作区为 `E:\9_Crypto\crypto_trading_system`
2. 主方案文档已冻结为第一版蓝图
3. 本轮开发先聚焦“研究-准入-运行时-执行-风控-回顾”主链
4. 不在本轮优先扩展 symbol universe
5. 不在本轮优先扩展更多模型提供方
6. 不在本轮优先上新策略模板

如果以上假设变化，需要由 `架构 owner` 重新冻结边界后再重排 Sprint。

## 3. 团队编制

建议按 10 个角色组织：

1. `架构 owner / 后端总负责人`
2. `后端研究平台负责人`
3. `后端准入 / 发布负责人`
4. `后端自治运行时负责人`
5. `后端执行与账务负责人`
6. `后端风控负责人`
7. `后端平台 / API 负责人`
8. `前端负责人`
9. `QA 负责人`
10. `Ops / 治理负责人`

如果人员不足，可以压缩成 6 人，但仍必须保留：

1. `架构 owner`
2. `研究 + 准入 owner`
3. `运行时 + 执行 owner`
4. `风控 owner`
5. `前端 owner`
6. `QA + Ops owner`

## 4. 角色责任矩阵

### 4.1 架构 owner

负责：

1. 冻结跨域 schema
2. 冻结状态机
3. 处理边界争议
4. 排 Sprint 顺序
5. 主持 Go / No-Go 评审

不负责：

1. 接管所有具体编码
2. 替代各 owner 写实现细节

### 4.2 后端研究平台负责人

负责：

1. `core/research/*`
2. 研究结果可复现
3. validation gate 硬门
4. candidate 生成与生命周期一致性

### 4.3 后端准入 / 发布负责人

负责：

1. `core/deployment/*`
2. runtime eligibility provider
3. eligibility snapshot
4. promotion 与 runtime contract 映射

### 4.4 后端自治运行时负责人

负责：

1. `core/ai/autonomous_agent.py`
2. `core/ai/autonomous_learning.py`
3. `core/ai/live_decision_router.py`
4. 运行时模式与 learning memory 治理

### 4.5 后端执行与账务负责人

负责：

1. `core/trading/execution_engine.py`
2. `core/accounting/*`
3. `core/backtest/*`
4. live / paper 净 PnL 对齐

### 4.6 后端风控负责人

负责：

1. `core/risk/*`
2. `core/runtime/*`
3. `core/governance/*`
4. 组合预算、熔断、降档

### 4.7 后端平台 / API 负责人

负责：

1. `web/api/*`
2. 稳定 API 契约
3. 控制面入口
4. 状态查询与审批接口

### 4.8 前端负责人

负责：

1. 研究工作台
2. 自治代理面板
3. 候选审批与回顾页面
4. 错误态、空态、降级态一致性

### 4.9 QA 负责人

负责：

1. 测试矩阵
2. 回归计划
3. 场景测试
4. 发布质量门

### 4.10 Ops / 治理负责人

负责：

1. 环境配置
2. 运行模式
3. 审批链
4. 告警
5. 回滚
6. 发布手册

## 5. 轨道划分

本轮开发按 5 条执行轨道组织：

1. `研究 / 准入 / 发布治理轨道`
2. `运行时 / 执行 / 风控轨道`
3. `API / 控制面轨道`
4. `前端 / 运营可见性轨道`
5. `QA / Ops / 发布治理轨道`

其中：

1. 轨道 1 和轨道 2 是主干开发轨道
2. 轨道 3 负责控制面与对外契约
3. 轨道 4 负责让系统可运营
4. 轨道 5 负责让系统可验证、可上线、可回滚

## 6. Board 结构

建议看板使用以下列：

1. `Ready`
2. `In Progress`
3. `Blocked`
4. `In Review`
5. `In QA`
6. `Ready for Demo`
7. `Done`

每张任务卡必须包含：

1. `owner`
2. `workstream`
3. `related files`
4. `dependency`
5. `acceptance`
6. `risk`

## 7. Sprint 节奏

建议先执行 4 个 Sprint：

1. `Sprint 0`
2. `Sprint 1`
3. `Sprint 2`
4. `Sprint 3`

建议节奏：

1. 每个 Sprint 1 周
2. Sprint 0 可压缩为 3-4 天
3. 每周固定一次 demo
4. 每周固定一次 Go / No-Go

## 8. Sprint 0：冻结边界与建立基线

### 目标

在开始改代码前，先冻结契约、样例、测试口径和工作方式，避免并行开发后返工。

### 主责 owner

1. 架构 owner
2. 后端平台 / API 负责人
3. QA 负责人

### 主要任务

#### 研究 / 准入 / 发布治理轨道

1. 冻结 `validation gate` 的输入输出字段
2. 冻结 `promotion decision` 的 reason code
3. 定义 `eligibility snapshot` 的最小 schema
4. 定义 `candidate -> eligibility` 映射表

#### 运行时 / 执行 / 风控轨道

1. 冻结 `runtime_mode` 集合
2. 冻结 `block / allow / reduce_only` 执行语义
3. 冻结 learning memory 的最小字段集合
4. 冻结净 PnL 字段口径

#### API / 控制面轨道

1. 输出 `proposal`、`candidate`、`eligibility`、`runtime status` API 样例
2. 输出错误码与状态码草案

#### 前端 / 运营可见性轨道

1. 产出研究工作台、候选审批、运行时状态、回顾面板的状态矩阵
2. 明确空态、失败态、降级态

#### QA / Ops / 发布治理轨道

1. 建立测试矩阵
2. 建立环境清单
3. 建立回滚模板
4. 建立 demo 数据样本

### 依赖

1. 主文档必须冻结
2. 架构 owner 必须确认最小 contract

### 完成定义

1. 所有跨域 schema 有文档版定义
2. QA 拿到样例后可以开始写测试
3. 前端拿到状态矩阵后可以开始画控制面
4. 后端各轨道不再争议字段归属

## 9. Sprint 1：止血与研究准入硬化

### 目标

优先完成高风险止血项，并让研究准入正式变成硬门。

### 主责 owner

1. 后端自治运行时负责人
2. 后端研究平台负责人

### 主要任务

#### 研究 / 准入 / 发布治理轨道

1. 强化 `core/research/validation_gate.py`
2. 明确 `reject / shadow / paper / live_candidate` 具体门槛
3. 增加低样本、低 OOS、低 DSR、成本拖累过高的 reason code
4. 为 promotion decision 输出稳定理由

涉及模块：

1. `core/research/validation_gate.py`
2. `core/research/orchestrator.py`
3. `core/research/experiment_schemas.py`

#### 运行时 / 执行 / 风控轨道

1. 收口自治代理执行旁路
2. 强制 fresh entry 经过 live decision gate
3. 强化服务不稳定时的开仓阻断
4. 区分 `fresh entry` 与 `add position`

涉及模块：

1. `core/ai/autonomous_agent.py`
2. `core/ai/live_decision_router.py`
3. `core/trading/execution_engine.py`

#### API / 控制面轨道

1. 补 proposal / candidate / promotion 查询接口需要的固定字段
2. 固定拒绝原因和状态展示字段

#### 前端 / 运营可见性轨道

1. 先做最小版候选状态与审批展示
2. 先做最小版自治代理状态展示

#### QA / Ops / 发布治理轨道

1. 加入止血回归场景
2. 加入低样本降级场景
3. 加入服务不稳定阻断场景

### 依赖

1. Sprint 0 schema 冻结完成
2. API 字段最小集已确认

### 完成定义

1. 自治代理不再无条件绕过执行审查
2. 低样本候选无法越级到高状态
3. 关键拒绝原因有稳定结构化输出
4. QA 通过止血场景测试

## 10. Sprint 2：落地 Runtime Eligibility Provider 并迁移 consumer

### 目标

正式切断运行时对研究内部 registry 的直接依赖。

### 当前进展（2026-04-07）

这一阶段当前已经完成的关键闭环包括：

1. 已新增 `core/ai/runtime_eligibility.py`，并生成 `data/research/runtime/eligibility_snapshot.json`。
2. `resolve_runtime_research_context()` 已优先消费 eligibility snapshot，仅在 snapshot 缺失/解析失败/刷新失败时才 fallback。
3. 自治代理控制面已能稳定展示 eligibility 的 `data_source`、`reason_codes`、`generated_at`、`refresh_age_sec`。
4. 这套可见性接入的是自治代理专用 API，而不是让研究页面本身参与运行时决策。

### 主责 owner

1. 后端准入 / 发布负责人
2. 后端平台 / API 负责人

### 主要任务

#### 研究 / 准入 / 发布治理轨道

1. 新增 `core/ai/runtime_eligibility.py`
2. 生成 `data/research/runtime/eligibility_snapshot.json`
3. 建立 `candidate -> eligibility` 映射逻辑
4. 增加 snapshot 刷新触发点

涉及模块：

1. 新增 `core/ai/runtime_eligibility.py`
2. `core/research/orchestrator.py`
3. `core/deployment/promotion_engine.py`
4. `data/research/runtime/*`

#### 运行时 / 执行 / 风控轨道

1. `live_decision_router` 切换到 eligibility snapshot
2. 需要研究资格的运行时 consumer 改为读取 eligibility contract
3. 保留兼容层，但不再让新逻辑直接依赖 `research_runtime_context`

涉及模块：

1. `core/ai/live_decision_router.py`
2. `core/ai/autonomous_agent.py`
3. `core/ai/research_runtime_context.py`

#### API / 控制面轨道

1. 提供 eligibility 查询接口
2. 提供 refresh age 与 reason code 查询字段
3. 固定 eligibility 失败时的 API 返回模式

#### 前端 / 运营可见性轨道

1. 候选页增加 eligibility 状态
2. 运行时页展示 eligibility age、reason codes、runtime mode cap

#### QA / Ops / 发布治理轨道

1. 加入 eligibility 缺失、过期、刷新失败的场景测试
2. 准备 snapshot 演练样本
3. 准备兼容层回滚方案

### 依赖

1. Sprint 1 完成后状态机稳定
2. eligibility schema 已由架构 owner 冻结

### 完成定义

1. 新 consumer 默认读取 eligibility snapshot
2. 运行时不再依赖研究 registry 内部字段做正式决策
3. eligibility 缺失或过期会稳定降档
4. 前端与 QA 能稳定观察 eligibility 状态

## 11. Sprint 3：学习硬治理、成本归因、组合风控闭环

### 目标

让运行时真正进入“纪律化自治”状态，而不是只有一层研究准入。

### 当前进展（2026-04-07）

这一阶段当前已经完成的关键闭环包括：

1. 自治代理的服务不稳定阻断、连亏阻断、冷静期阻断、风险熔断阻断均已进主链。
2. 执行回顾已统一 `gross / fee / slippage / net` 成本归因口径。
3. `risk_manager` 已输出自治纪律契约与滚动回撤观测，并支持自治纪律阈值持久化配置。
4. 自治代理 scorecard API 与风险状态 API 已经落地，前端控制面可直接观测纪律、净收益、成本拖累与 eligibility 刷新状态。

### 主责 owner

1. 后端自治运行时负责人
2. 后端执行与账务负责人
3. 后端风控负责人

### 主要任务

#### 研究 / 准入 / 发布治理轨道

1. 让 cost drag 回流到 research 评估
2. 让 eligibility 可消费真实净值相关信号

#### 运行时 / 执行 / 风控轨道

1. 将 learning memory 分为 observation / rule / decision 三层
2. 增加 symbol-side 冷静期
3. 增加连续亏损自动收紧
4. 增加同向暴露限制
5. 增加回撤熔断与降档状态机

涉及模块：

1. `core/ai/autonomous_learning.py`
2. `core/ai/autonomous_agent.py`
3. `core/risk/risk_manager.py`
4. `core/runtime/runtime_state.py`

#### 执行 / 账务轨道

1. 修复 `fee_usd`
2. 修复 `slippage_cost_usd`
3. 统一 gross / net PnL 定义
4. 统一 paper / live 回顾字段

涉及模块：

1. `core/trading/execution_engine.py`
2. `core/accounting/*`
3. `data/cache/live_review/*`

#### API / 控制面轨道

1. 提供 scorecard API
2. 提供 learning memory explainability API
3. 提供组合风险与回撤状态 API

#### 前端 / 运营可见性轨道

1. 实盘回顾页
2. scorecard 面板
3. learning memory 原因展示
4. 风险降档与熔断显示

#### QA / Ops / 发布治理轨道

1. 加入 journal 成本非零校验
2. 加入连续亏损降档场景
3. 加入 eligibility + learning memory + risk 多重阻断场景
4. 准备上线回滚演练

### 依赖

1. Sprint 2 的 eligibility provider 已稳定
2. execution 旁路已经收口

### 完成定义

1. learning memory 可以直接解释当前禁止开仓的原因
2. fee/slippage 不再长期为零
3. scorecard 与 journal 净收益口径一致
4. 连续亏损和回撤会真实触发降档或 reduce_only

## 12. 各轨道首轮 backlog

### 12.1 研究 / 准入 / 发布治理轨道 backlog

优先级从高到低：

1. 固定 validation gate reason code
2. 固定 `paper` 与 `live_candidate` 硬门
3. 新建 runtime eligibility schema
4. 新建 provider builder
5. 将 promotion 映射到 eligibility contract
6. 建立 refresh hook

### 12.2 运行时 / 执行 / 风控轨道 backlog

优先级从高到低：

1. 去掉自治代理执行旁路
2. 将 instability fresh entry 阻断做成统一 gate
3. 将 `live_decision_router` 接入 eligibility
4. 重构 learning memory schema
5. 增加 symbol-side cooldown
6. 增加组合暴露与回撤熔断

### 12.3 API / 控制面轨道 backlog

优先级从高到低：

1. proposal / candidate / promotion 最小稳定查询接口
2. eligibility 查询接口
3. runtime status 查询接口
4. scorecard API
5. review API

### 12.4 前端 / 运营可见性轨道 backlog

优先级从高到低：

1. 候选状态与审批面板
2. 运行时状态面板
3. eligibility 状态展示
4. review / scorecard 面板
5. 风险降档展示

### 12.5 QA / Ops / 发布治理轨道 backlog

优先级从高到低：

1. 测试矩阵与样例冻结
2. 止血场景回归
3. eligibility 过期 / 缺失场景回归
4. 成本字段非零校验
5. Go / No-Go 模板
6. 回滚演练脚本

## 13. 会议机制

### 13.1 每日站会

参与人：

1. 全部 owner

时长：

1. 15 分钟

只回答三件事：

1. 昨天完成了什么
2. 今天要完成什么
3. 当前 blocker 是什么

### 13.2 每周 schema / state review

参与人：

1. 架构 owner
2. 全部后端 owner
3. QA 负责人
4. 前端负责人

输出：

1. schema 冻结项
2. 状态机变更记录
3. 兼容性决策

### 13.3 每周 demo

参与人：

1. 全部 owner
2. Ops / 治理负责人

demo 只接受真实链路，不接受口头说明。

最小 demo 路径：

1. proposal
2. candidate
3. eligibility
4. runtime
5. review

## 14. 开发规范

### 14.1 分支规则

建议：

1. 一个 Sprint 一个主分支
2. 一个轨道一个子分支
3. 涉及跨域 schema 的改动必须先过 schema review

### 14.2 PR 规则

每个 PR 必须包含：

1. 改动目标
2. 影响模块
3. 依赖关系
4. 风险点
5. 测试结果
6. 回滚说明

### 14.3 Definition of Done

一项任务只有同时满足以下条件才算完成：

1. 代码已合并
2. 测试已补齐
3. API 或状态机已稳定
4. 前端或调用方已验证
5. 日志与可观测性已补齐
6. 文档已更新

## 15. 质量门

以下情况不得进入下一阶段：

1. 没有稳定 API 样例
2. 没有固定状态矩阵
3. 没有拒绝原因结构化输出
4. eligibility 缺少 `reason_codes`
5. eligibility 缺少 `generated_at` / `expires_at`
6. 执行层仍存在自治代理旁路
7. `fee_usd` 和 `slippage_cost_usd` 仍长期为 `0`
8. 连续亏损和服务不稳定场景无法稳定复现阻断行为

## 16. 首个启动顺序

如果今天就要启动，建议按下面顺序开工：

1. 架构 owner 召开 45 分钟 kickoff，冻结 Sprint 0 范围
2. 后端研究平台负责人和后端准入 / 发布负责人先冻结 eligibility schema
3. 后端自治运行时负责人和后端执行与账务负责人一起确认 execution gate 边界
4. QA 负责人输出测试矩阵初版
5. 前端负责人输出状态矩阵初版
6. Ops / 治理负责人输出环境与回滚清单初版
7. 当天结束前，把 Sprint 0 任务卡全部进板并分配 owner

## 17. 本手册的使用方式

这份文档不替代主文档。

使用方式应为：

1. 主文档决定方向和边界
2. 本文档决定人、节奏、优先级和执行方式
3. 每个 Sprint 结束后，只更新本文档中的执行状态，不反复改主文档的核心架构结论
