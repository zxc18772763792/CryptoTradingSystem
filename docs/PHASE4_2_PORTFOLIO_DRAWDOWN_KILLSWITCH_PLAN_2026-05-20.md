# Phase 4.2 · 组合/单策略回撤熔断接入执行链路(独立任务)

> 上游计划:`docs/STABILITY_PROFITABILITY_IMPROVEMENT_PLAN_2026-05-20.md`
> 前置条件:Phase 1/2 完成;P4.1 冲突按账户隔离已落地。

## 为什么单独做

"稳定盈利"的工程定义 ≠ 每天赚,而是**最坏一段时间不会让你出局**。回撤熔断
是兜底机制——超过阈值时强制平仓、暂停下单。系统已有 CUSUM 衰减监控和相关性
过滤(`core/monitoring/strategy_monitor.py`, `cusum_watcher.py`),但目前只在
研究/晋升端使用,**没有接入实盘执行链路**——也就是真亏起来时熔断不会触发。
这是独立的中等工作量,需要小心不要在熔断期吃掉本应继续的策略。

## 目标

1. **单策略熔断**:24h / 7d 回撤超阈值 → 该策略 stop + 平仓 + 标记冻结,不再
   被 strategy_runner 重启。
2. **组合熔断**:全部账户合计 24h 回撤超阈值 → 全局停所有策略,只允许
   close-only 订单进入执行引擎。
3. 熔断状态可在 UI 看见,可手工解除(明确二次确认)。

## 现状盘点

- 监控可用:
  - `core/monitoring/strategy_monitor.py` `detect_strategy_decay` / `CUSUMMonitor`
  - `core/monitoring/cusum_watcher.py` `run_cusum_checks_for_all_candidates`
    已在 `web/main.py` lifespan 启动(5 分钟间隔)。
- 执行链路:`core/trading/execution_engine.py` `submit_signal`;熔断需要在这里
  插入"是否允许下单"的门。
- 权益/PnL 跟踪:`risk_manager._trade_history`、`config/database.py`
  `StrategyPerformanceSnapshot`。

## 设计

### 4.2.1 熔断状态机

新文件 `core/risk/circuit_breaker.py`:

- `CircuitBreaker` 单例,持有
  - `strategy_states: Dict[str, {tripped: bool, tripped_at, reason, manual_reset_at}]`
  - `portfolio_state: {tripped, tripped_at, reason}`
- API:`check_strategy(name) -> Decision(allow|close_only|block)`,
  `check_portfolio() -> Decision`, `trip_strategy(name, reason)`,
  `trip_portfolio(reason)`, `reset_strategy(name, operator)`,
  `reset_portfolio(operator)`。
- 阈值从 `settings`:
  - `CB_STRATEGY_DAILY_DD_PCT: float = 0.05`(5%)
  - `CB_STRATEGY_WEEKLY_DD_PCT: float = 0.10`
  - `CB_PORTFOLIO_DAILY_DD_PCT: float = 0.03`
  - `CB_PORTFOLIO_WEEKLY_DD_PCT: float = 0.06`

### 4.2.2 数据源

定期任务(随 cusum_watcher 同一调度,1 分钟间隔):
- 读取 `StrategyPerformanceSnapshot`(已存在,见 memory)或聚合
  `_trade_history` 计算每策略 24h/7d 回撤。
- 全局 = 所有策略加权(或简单累加 PnL %)。
- 任何阈值命中 → `trip_*` + 通知(`asyncio.create_task` 发邮件/Slack)。

### 4.2.3 执行链路接入

`execution_engine.submit_signal(signal)`:在 dispatch 前调用
`CircuitBreaker.check_strategy(signal.strategy_name)` 与
`check_portfolio()`:
- `allow` → 正常执行。
- `close_only` → 只允许 `close_long`/`close_short`/减仓单。其它拒绝并记日志。
- `block` → 全部拒绝。

注意:**减仓订单必须始终允许**——熔断的目的是停损,不是把已有持仓锁死。

### 4.2.4 自动平仓

熔断触发同时调用 `StrategyManager._close_positions_for_strategy_stop(name,
reason="circuit_breaker")`,复用既有平仓逻辑(P4.3 已存在)。

### 4.2.5 UI

- `web/static/js/app.js` / `web/templates/index.html`:Header 加红色熔断状态条;
  单策略状态面板加"已熔断/原因/触发时间/手动解除"按钮。
- API:`GET /api/risk/circuit-breaker`, `POST /api/risk/circuit-breaker/reset`
  (需要 OPS_TOKEN 或确认)。

## 风险/回滚

- 阈值默认值可能太紧 → 频繁误熔断 → 用 settings 旋钮调,默认值放在文档显眼处。
- 回滚:`settings.CIRCUIT_BREAKER_ENABLED: bool = True`,False 关闭整套机制
  (回到现状)。

## 测试

- `tests/test_circuit_breaker.py`:状态机单元测试,trip→close_only→manual_reset
  →allow 流程。
- `tests/test_execution_circuit_breaker_integration.py`:模拟 submit_signal
  在三种状态下的行为(allow/close_only/block);确认减仓单永远放行。
- `tests/test_circuit_breaker_pnl_trigger.py`:喂入 trade history 触达阈值,
  断言 trip 并触发自动平仓。

## 验收清单

- [x] settings 阈值与默认值合理(默认从 P0 期保守)。
- [x] execution_engine 在 dispatch 前完成状态检查,close-only 路径全开。
- [x] cusum_watcher 间隔内能稳定检测并 trip。(独立的 circuit_breaker_monitor,60s 间隔)
- [x] UI 能看见熔断状态、能手动解除。
- [x] 3 套测试齐全无回归。(26 passed,2026-05-20)
- [x] 文档示例:阈值触发的 PnL 模拟,记一份在 docs/ 下作运行手册。
  → [docs/CIRCUIT_BREAKER_RUNBOOK_2026-05-20.md](CIRCUIT_BREAKER_RUNBOOK_2026-05-20.md)

## 实现摘要 (2026-05-20)

| 改动 | 文件 |
|---|---|
| 状态机 / 阈值评估 / 持久化 | [core/risk/circuit_breaker.py](../core/risk/circuit_breaker.py) (新) |
| `__init__` 导出 | [core/risk/__init__.py](../core/risk/__init__.py) |
| settings 阈值 (5 个 + 总开关) | [config/settings.py](../config/settings.py) |
| execution_engine dispatch 前的 CB 门 | [core/trading/execution_engine.py](../core/trading/execution_engine.py) `_execute_signal_in_active_mode` 开头 |
| 60s 后台监控任务 + 通知监听 + close-positions hook | [web/main.py](../web/main.py) `_circuit_breaker_monitor_worker` |
| `/api/risk/circuit-breaker` 端点 (get/evaluate/reset) | [web/api/risk.py](../web/api/risk.py) (新) |
| router 注册 | [web/main.py](../web/main.py) |
| 顶部红色横幅 + 二次确认解除 | [web/templates/index.html](../web/templates/index.html), [web/static/js/app.js](../web/static/js/app.js) (尾部), [web/static/css/style.css](../web/static/css/style.css) (尾部) |
| 单元测试 (13 cases) | [tests/test_circuit_breaker.py](../tests/test_circuit_breaker.py) |
| 执行链路集成测试 (6 cases) | [tests/test_execution_circuit_breaker_integration.py](../tests/test_execution_circuit_breaker_integration.py) |
| PnL 触发端到端测试 (7 cases) | [tests/test_circuit_breaker_pnl_trigger.py](../tests/test_circuit_breaker_pnl_trigger.py) |
| 运行手册 | [docs/CIRCUIT_BREAKER_RUNBOOK_2026-05-20.md](CIRCUIT_BREAKER_RUNBOOK_2026-05-20.md) |
