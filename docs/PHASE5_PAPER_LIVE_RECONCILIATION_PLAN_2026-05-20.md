# Phase 5 · 纸盘→实盘对账闸门(独立任务)

> 上游计划:`docs/STABILITY_PROFITABILITY_IMPROVEMENT_PLAN_2026-05-20.md`
> 前置条件:Phase 1/2 + P4.1/P4.3 已落地;Phase 4.2 强烈推荐(熔断兜底)。

## 为什么单独做

这是计划里最大的一块:**持续对账闸门**——每个策略持续记录"实现 PnL vs
回测预期",偏差超阈值自动降级 shadow,只有持续吻合的策略才升真实资金。
这要求:① 长期采样(数周到数月),② 设计降级策略,③ 安全升级路径,
④ 完整的 UI。整个流程是个准独立项目。

## 目标

把"策略晋升真金"从**一次性 validation_gate**(目前)变为**持续监督**:
- 在 paper 持续运行,每日记录策略**模拟实现 PnL**(用真实成交价模拟)。
- 与 **回测预期 PnL**(在同周期、同价格序列下)逐日对比。
- 维护**滚动一致性分数**;低于阈值 → 自动降级。
- 仅滚动分数 ≥ 阈值 ≥ 14 天的策略可申请晋升 live;升 live 后小额测试,
  小额 PnL 仍持续对账。

## 设计

### 5.1 数据模型

新增表 `core/research/reconciliation_snapshots`(或扩展现有
`StrategyPerformanceSnapshot`):
- `strategy_name, symbol, day`
- `realized_pnl_pct`(paper 模拟实现)
- `expected_pnl_pct`(同日同 K线在策略回测下应得)
- `tracking_error_bps`(差额标准化)
- `rolling_consistency_30d`(过去 30 天 1 - mean(|err|/threshold))

### 5.2 计算管线

- 在 paper 运行链路里:每根 bar 完成时,记录:实际成交价、实际成交量、实际
  fee/funding;同时通过 backtest 引擎(已等于实盘的真实路径)在当日同段
  数据上跑一遍,得 expected。两者比较。
- 频率:逐日聚合,worker 类似 cusum_watcher。

### 5.3 闸门规则

- `tracking_error_bps_max`(单日)默认 30bp;超过 → 当日打"divergence"标记。
- 滚动一致性 = 1 - (过去 30 天 divergence_days / 30)。
- 状态机:
  - `paper_running` → `shadow`:连续 5 天 divergence 或滚动一致性 < 0.7。
  - `paper_running` → `live_candidate`:滚动一致性 ≥ 0.85 且 30 天稳定。
  - `live` → `live_warning`:连续 3 天 divergence。
  - `live_warning` → `shadow`:再连续 3 天 divergence。

阈值全部走 `settings.RECON_*` 旋钮,可灰度。

### 5.4 升级路径

- 候选 `live_candidate` 状态后,operator 显式批准 + 设置小额上限
  (`live_capital_cap_usdt`)才升 live。
- live 状态自动持续监控,降级是自动的,升级永远人工。

### 5.5 API & UI

- `GET /api/recon/snapshots?strategy=...&days=30` 时间序列。
- `GET /api/recon/state` 全策略当前状态 + 滚动一致性。
- `POST /api/recon/{id}/approve-live` (二次确认 + OPS_TOKEN)。
- UI:策略卡片显示滚动一致性条 + 状态徽章;详情面板有 expected vs realized
  时序图。

### 5.6 与既有组件衔接

- 复用 `core/monitoring/cusum_watcher.py` 调度器架构。
- 复用 `core/research/validation_gate.py` 的晋升管线,但把"一次性 OOS 检查"
  升级为"持续闸门"。
- 与 Phase 4.2 熔断协同:熔断 trip 时,recon 状态直接打 `frozen`,解除后
  必须重走 30 天观察期。

## 风险/回滚

- 计算开销:每日为每策略跑一段回测 → 时间消耗。控制每策略采样 200~500 bar
  即可(同 live_window)。
- 阈值放太松 → 闸门形同虚设;放太紧 → 永远没有策略能升 live。**用历史 paper
  数据先离线模拟一遍调阈值**,再上线。
- 回滚:`settings.RECON_GATE_ENABLED=False`,关闭自动降级,回到现状。

## 测试

- `tests/test_recon_snapshot_compute.py`:同一段 OHLCV 喂 paper 模拟 + 回测,
  断言 expected ≈ realized(扣除真实滑点/费)。
- `tests/test_recon_state_machine.py`:状态机各转移路径。
- `tests/test_recon_gate_blocks_live_promotion.py`:滚动一致性低的策略 promote
  请求被拒绝。

## 验收清单

- [ ] 数据模型 + worker 落地,每日有快照。
- [ ] 状态机正确转移,降级自动、升级人工。
- [ ] UI 显示一致性 + 时序图。
- [ ] settings 旋钮齐全,有合理默认。
- [ ] 完整测试 + 至少一份"历史 paper 数据模拟阈值"调参报告。
- [ ] 与 Phase 4.2 熔断的协同行为有测试覆盖。

## 估算

中等到大:核心约 ~800 行(状态机 + worker + API + UI),~6 个测试,
~2-3 个 commit。建议作为一个跨数天的独立项目执行,而非单次会话。
