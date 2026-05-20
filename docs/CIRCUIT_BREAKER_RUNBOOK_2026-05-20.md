# 组合 / 单策略熔断运行手册 (Phase 4.2)

> 上游计划: `docs/PHASE4_2_PORTFOLIO_DRAWDOWN_KILLSWITCH_PLAN_2026-05-20.md`
> 实现日期: 2026-05-20

## 1. 设计目标

熔断只做一件事:**让最坏的一段时间不会让你出局**。
当 24h / 7d 回撤超阈值时:

- 立即将该策略(或整个组合)切到 `close_only` 状态;
- 所有新开仓信号被执行引擎拒绝;
- **减仓 / 平仓订单永远放行** —— 这是兜底,不是把仓位锁死;
- UI 顶部红色横幅可见,可在二次确认后手动解除。

## 2. 阈值默认值

在 `config/settings.py` 中定义,可通过 env / `.env` 覆盖:

| Setting | 默认值 | 含义 |
|---|---|---|
| `CIRCUIT_BREAKER_ENABLED` | `True` | 总开关 |
| `CB_STRATEGY_DAILY_DD_PCT` | `0.05` (5%) | 单策略 24h 回撤阈值 |
| `CB_STRATEGY_WEEKLY_DD_PCT` | `0.10` (10%) | 单策略 7d 回撤阈值 |
| `CB_PORTFOLIO_DAILY_DD_PCT` | `0.03` (3%) | 组合 24h 回撤阈值 |
| `CB_PORTFOLIO_WEEKLY_DD_PCT` | `0.06` (6%) | 组合 7d 回撤阈值 |
| `CB_MONITOR_INTERVAL_SEC` | `60` | 后台扫描间隔(秒) |

> 阈值偏紧 → 频繁误熔断;偏松 → 兜底失效。**P0 期保守用默认值**,稳定后再放宽。

## 3. 关键文件

| 文件 | 说明 |
|---|---|
| `core/risk/circuit_breaker.py` | `CircuitBreaker` 状态机 + 阈值评估器 |
| `core/risk/__init__.py` | 重新导出 `circuit_breaker` 单例 |
| `core/trading/execution_engine.py:_execute_signal_in_active_mode` | 信号 dispatch 前的 CB 门 |
| `web/main.py:_circuit_breaker_monitor_worker` | 1 分钟扫描 + 通知 + 自动平仓 hook |
| `web/api/risk.py` | `GET /api/risk/circuit-breaker` + `POST /api/risk/circuit-breaker/reset` |
| `web/static/js/app.js` (尾部) | 顶部横幅轮询渲染 + 手动解除 |
| `web/templates/index.html` | `#circuit-breaker-banner` |

## 4. 状态机

```
   trip_strategy(name, reason)
        │
        ▼
  ┌────────────┐           reset_strategy(name, operator)
  │   close_   │ ◄────────────────────────────────────┐
  │   only     │                                       │
  └─────┬──────┘                                       │
        │                                              │
        ▼                                              │
  signal blocked (allow CLOSE_LONG/SHORT/reduce_only) ─┘

evaluate(strategy_name, is_reduce_only)
        │
        ├── is_reduce_only=True → DECISION_ALLOW
        ├── check_portfolio().is_close_only → DECISION_CLOSE_ONLY
        └── check_strategy(name).is_close_only → DECISION_CLOSE_ONLY
```

## 5. PnL 模拟示例

以 $10k 账户、默认阈值 5%(单策略 24h) 为例:

| 时刻 | 策略 PnL | 累计盈亏 | 触发? |
|---|---|---|---|
| T-3h | -$200 | -$200 (−2.0%) | 否 |
| T-2h | -$300 | -$500 (−5.0%) | **刚好触线** |
| T-1h | -$200 | -$700 (−7.0%) | 是 → trip |
| 之后 | 任何开仓信号 | — | 被拒,日志 `circuit_breaker_blocked` |
| 之后 | `CLOSE_LONG` / `close_only=True` | — | 正常执行 |

测试用例: `tests/test_circuit_breaker_pnl_trigger.py::test_pnl_trigger_breaches_daily_threshold`

## 6. 操作流程

### 6.1 实时查看状态

```bash
curl http://localhost:8000/api/risk/circuit-breaker | jq
```

返回:

```json
{
  "enabled": true,
  "portfolio": { "tripped": false, "tripped_at": null, ... },
  "strategies": {
    "BollingerBands_BTC": {
      "tripped": true,
      "tripped_at": "2026-05-20T10:23:14+00:00",
      "reason": "24h_dd 0.0612 >= 0.0500",
      "daily_dd": 0.0612,
      "weekly_dd": 0.0734
    }
  },
  "thresholds": { ... }
}
```

UI: 控制台顶部红/橙色横幅。

### 6.2 强制立刻评估(无需等 1 分钟)

```bash
curl -X POST http://localhost:8000/api/risk/circuit-breaker/evaluate \
  -H "X-OPS-TOKEN: $OPS_TOKEN"
```

### 6.3 手动解除

**组合熔断**:

```bash
curl -X POST http://localhost:8000/api/risk/circuit-breaker/reset \
  -H "Content-Type: application/json" \
  -H "X-OPS-TOKEN: $OPS_TOKEN" \
  -d '{"scope":"portfolio","confirm":true,"note":"已核实回撤来源,允许恢复"}'
```

**单策略熔断**:

```bash
curl -X POST http://localhost:8000/api/risk/circuit-breaker/reset \
  -H "Content-Type: application/json" \
  -H "X-OPS-TOKEN: $OPS_TOKEN" \
  -d '{"scope":"strategy","strategy_name":"BollingerBands_BTC","confirm":true,"note":"参数已调整"}'
```

> `confirm: true` 不能省略;UI 上点击"手动解除…"按钮会弹出二次确认对话框。

### 6.4 关闭整套机制(紧急回滚)

```env
# .env
CIRCUIT_BREAKER_ENABLED=false
```

重启后,所有 `check_*` 返回 `allow`,完全回到 P4.2 前的行为。

## 7. 监控 / 告警

- 监控任务名: `circuit_breaker_monitor` (在 `_build_runtime_task_factories` 中注册)
- 触发时通过 `notification_manager` 发送 Feishu + Telegram(若已配置)
- 自动平仓:`circuit_breaker` 在 trip 时调用注册的 close-positions hook,默认指向
  `strategy_manager._close_positions_for_strategy_stop(name, reason="circuit_breaker:<原因>")`
- 持久化: `data/cache/runtime_state/circuit_breaker.json` — 进程重启后熔断状态保留
- 审计: `reset` 端点会写入 `audit_logger` (`action=circuit_breaker.{portfolio_reset|strategy_reset}`)

## 8. 测试覆盖

| 文件 | 覆盖 |
|---|---|
| `tests/test_circuit_breaker.py` | 状态机单元(trip / reset / 持久化 / 监听器 / 禁用短路) |
| `tests/test_execution_circuit_breaker_integration.py` | execution_engine 在 3 种状态下的 dispatch 行为 + 减仓单永远放行 |
| `tests/test_circuit_breaker_pnl_trigger.py` | 喂入 trade history → 触发 trip + 自动平仓 hook |

```bash
python -m pytest tests/test_circuit_breaker.py \
                 tests/test_execution_circuit_breaker_integration.py \
                 tests/test_circuit_breaker_pnl_trigger.py -v
```

最近一次运行: **26 passed**(2026-05-20)。

## 9. 常见问题

**Q: 熔断后我的限价单还在挂着,会被取消吗?**
A: 不会。熔断只拦截 `execution_engine.submit_signal` 的新单分发,已挂订单照常运行。
   `close-positions hook` 会调用 `_close_positions_for_strategy_stop`,该函数本身会平
   仓 + 取消该策略的挂单(参见 `strategy_manager.py:861`)。

**Q: 重启服务后,熔断状态还在吗?**
A: 在。状态持久化到 `data/cache/runtime_state/circuit_breaker.json`,启动时自动恢复。
   如要"重启清除",删除该文件即可。

**Q: paper 模式 / live 模式行为有差异吗?**
A: 没有。熔断逻辑工作在 `execute_signal` 的最外层,paper / live 路径都被同一个门拦住。

**Q: 多账户场景下,某个子账户单独亏损会触发组合熔断吗?**
A: 当前实现使用 `risk_manager.get_rolling_drawdown_snapshot` 的全账户聚合权益时间线。
   后续若需要 per-account 隔离熔断,扩展 `circuit_breaker._strategies` key 为
   `f"{account_id}:{strategy_name}"` 即可。
