# 每日代码审计报告 — 2026-05-27

> 自动化运行 · HEAD: `1b0eb9f`（Audit strategy and runtime safety fixes）  
> 前次基线：`code_review_20260526.md`（1740 passed）  
> 本次测试结果：**1754 passed, 1 skipped**（+14 tests）

---

## 一、前次审计修复验证

以下 4 个 2026-05-26 报告中修复项均通过对应专项测试：

| 修复项 | 测试文件 | 结果 |
|---|---|---|
| reduce-only 失败计数器提前清除 | `test_execution_engine_stale_position_force_close.py` (5项) | ✅ 通过 |
| mode-pin 检测改用 `is not None` | `test_strategy_mode_sync.py` (5项) | ✅ 通过 |
| negative prev_equity 被视为不可信 | `test_trading_balances_stale_prev_equity.py` (5项) | ✅ 通过 |
| DataCollector 回调异常计数上报 | `test_data_collector_lifecycle.py` | ✅ 通过 |

之前标记的 `NotImplementedError` 问题（`ccxt_adapter` create_order / cancel_order / fetch_order）也已全部实现；`OrderIntentRouter.submit_intent` 的限流 TODO 已完成。

---

## 二、本轮发现的新 Bug

### Bug 1 — 陈旧注释：`_BALANCE_RESPONSE_TIMEOUT_SEC` 从 18s 改为 5s 后注释未更新
**文件：** `web/api/trading_balances.py:317`  
**严重性：** 低（仅影响可读性）

注释写道：
```python
# Notification eval can fan out into altcoin scans … 
# The outer endpoint already took the 18s `_BALANCE_RESPONSE_TIMEOUT_SEC` hit …
```

但常量已在近期提交中从 18.0s 改为 5.0s：
```python
_BALANCE_RESPONSE_TIMEOUT_SEC = 5.0
```

调试时依据此注释会误判超时预算（18s vs 5s）。

**建议修复：**
```python
# The outer endpoint already took the 5s `_BALANCE_RESPONSE_TIMEOUT_SEC` hit …
```

---

### Bug 2 — 死代码：`risk_manager.py:869` 的中文标注永不出现
**文件：** `core/risk/risk_manager.py:869`  
**严重性：** 低

```python
if not allow_close and equity > 0 and notional > 0:   # line 861
    single_limit = equity * self.max_position_size
    if notional > single_limit + epsilon:
        self._add_alert(
            ...
            + ("（平仓单）" if allow_close else "")   # line 869 — 永远为 ""
        )
```

整个 `if not allow_close:` 块在 `allow_close=True` 时不会执行，所以 `("（平仓单）" if allow_close else "")` 只能是 `""` 。这个三元表达式是无效的残留代码，且稍显混乱。

**建议修复：** 删除 `+ ("（平仓单）" if allow_close else "")` 这一行。

---

### Bug 3 — `ResidualMom24hStrategy` 仍在代码库中（文档称已删除）
**文件：** `strategies/quantitative/intraday_cross_section.py:22,108,1564`；`config/strategy_registry.py:500`  
**严重性：** 中（浪费一个注册槽，并与 `RelRet24hReversalStrategy` 完全重复）

2026-05-25 性能审查报告明确说明：

> *"`ResidualMom24hStrategy` was removed because it was identical to `RelRet24hReversalStrategy` in factor, lookback, direction, and execution behavior."*

但策略仍在代码中，且运行时验证证实两者完全一致：
```
factor_name: residual_return  ← 相同
lookback_bars: 288             ← 相同
direction: low                 ← 相同
execution_mode: spread_low_minus_high  ← 相同
timeframe: 5m                  ← 相同
rebalance_bars: 288            ← 相同
```

Spec 描述也承认这一问题："currently equal to relative-market return."

**建议：** 从 `strategies/quantitative/intraday_cross_section.py`、`strategies/quantitative/__init__.py`、`strategies/__init__.py` 和 `config/strategy_registry.py` 中删除 `ResidualMom24hStrategy` 相关条目，并更新 `INTRADAY_CROSS_SECTION_STRATEGY_IDS` 列表。注意先确认没有已注册的运行实例，防止误删活跃策略。

---

## 三、生产状态告警

### ⚠️ 组合熔断器仍处于触发状态（自 2026-05-25 起）

```json
{
  "tripped": true,
  "tripped_at": "2026-05-25T15:01:21+00:00",
  "reason": "24h_dd 0.0511 >= 0.0300",
  "daily_dd": 0.051141,
  "weekly_dd": 0.051141,
  "last_reset_at": "2026-05-25T09:56:19+00:00",
  "last_reset_by": "auto_false_trip_recalc"
}
```

- 所有新实盘入场单均被阻止（仅允许 close_only）。
- 自动清除逻辑不会介入：`stored_dd (0.0511) < _FALSE_TRIP_AUTO_CLEAR_MIN_RECORDED_DD (0.20)`，只有由极小权益分母引发的"大虚假 DD"才触发自动清除。
- **操作建议**：若认为亏损已实现且系统稳定，需人工调用 `POST /ops/risk/circuit-breaker/reset` 或直接修改 `data/cache/runtime_state/circuit_breaker.json` 后重启。重置前应先确认当前账户净值并核对当日亏损是否在可接受范围内。

---

## 四、架构审查（近期提交）

### 4.1 `core/risk/risk_manager.py` — `allow_close` 旁路扩展

| 路径 | 旁路意图 | 评估 |
|---|---|---|
| `daily_trades >= max_daily_trades` | 平仓不受日交易次数限制 | ✅ 合理 |
| `position_count >= max_open_positions` | 已有持仓允许平仓 | ✅ 合理 |
| `leverage > max_leverage` | 平仓订单杠杆可超限 | ✅ 合理 |
| `equity ≤ 0 or notional ≤ 0` | 数据不足时不阻止平仓 | ✅ 合理 |
| `notional > single_limit` | 平仓单不受单笔规模上限 | ✅ 合理 |
| `paper_ 前缀 order_id 进入 live trade_history` | 过滤误写入 | ✅ 新增防护 |

整体扩展方向正确，降低了熔断状态下无法平仓的风险。

### 4.2 `web/api/trading_balances.py` — 响应超时从 18s 压缩至 5s

积极影响：前端不再因单次 `/balances` 请求等待超 18s 才切回 stale cache；5s 内拿不到响应就立即返回缓存，并在后台继续刷新。

潜在风险：如果网络延迟普遍 > 5s（如交易所维护窗口），则每次请求都会触发 fallback，前端会持续显示陈旧数据；cache TTL 已相应从 10s 延长至 45s 以缓解这一问题。

### 4.3 `core/risk/circuit_breaker.py` — auto_false_trip 机制

新增 `_should_auto_clear_false_trip` 保守自动清除逻辑：
- 仅在 `stored_dd >= 0.20`（即权益分母极小时产生的虚假 20%+ DD）情况下才自动清除
- `account_equity < _credible_equity_floor()` 时也不清除（防止在权益仍不可信时误清除）

设计保守、合理。生产中的 5.11% DD 触发不属于此场景（无自动清除）。

### 4.4 `strategies/quantitative/intraday_cross_section.py` — 方向性执行模式

确认 `cross_section_rank_select` 已在函数内部处理 `execution_mode`：
- `short_low`/`short_high` → `long_symbols=[]`，不会生成 BUY 信号
- `long_high`/`long_low` → `short_symbols=[]`，不会生成 SELL 信号
- `generate_signals_async` 没有额外的 `allow_long` guard 是安全的（通过 plan 的 `long_symbols=[]` 保证）

---

## 五、持续积压（高优先级，来自前次审计，仍未落地）

| 编号 | 问题 | 状态 | 紧迫度 |
|---|---|---|---|
| P1 | **退出信号缺口**：27/37 经典策略无 CLOSE_LONG/CLOSE_SHORT；factor_based 策略仅部分覆盖；event_driven 的 `supply_event_strategy.py` 完全无退出信号 | 未处理 | 极高 |
| P2 | **回测引擎忽略 SL/TP**：`backtest_engine._execute_buy` 不触发 `signal.stop_loss` / `signal.take_profit` | 未处理 | 极高 |
| P3 | **VWAPReversionStrategy 退出信号修复**：均值回归完成后发 SELL 而非 CLOSE_LONG | 未处理 | 高 |
| P4 | **存量 Parquet UTC 迁移**：`scripts/migrate_parquet_klines_to_utc.py` 尚未执行 | 未处理 | 高 |
| P5 | **F401 未使用 import 清理** | 未处理 | 中 |

---

## 六、建议行动项（按优先级）

1. **立即（生产告警）**：确认 2026-05-25 的 5.11% 回撤是否属于正常交易亏损。若是，人工重置熔断器。若是数据问题，检查权益来源后再重置。
2. **本周**：删除 `ResidualMom24hStrategy`（Bug 3），其 spec 描述已承认这是临时占位符。
3. **本周**：修复两处低风险 bug（Bug 1 注释 + Bug 2 死代码），5 分钟内完成。
4. **下周**：开始 P1 退出信号补全（先从 `supply_event_strategy.py` 和 VWAPReversionStrategy 入手）。
5. **持续**：每次新建遵循"从磁盘加载状态"模式的 singleton 时，同步更新 `conftest.py` 隔离块。

---

## 七、测试基线

```
2026-05-26: 1740 passed, 1 skipped
2026-05-27: 1754 passed, 1 skipped (+14 tests)
```

新增测试分布（+14）：
- 4 tests: `test_account_scoped_live_paths.py`（account-scoped live execution paths）
- 5 tests: `test_circuit_breaker.py`（auto_false_trip logic）
- 3 tests: `test_coinglass_budget_short_circuit.py` / `test_coinglass_dataset_normalization.py`
- 2 tests: `test_backtest_factor_modes_ui_assets.py`

代码库整体健康：0 失败，0 错误，1 个 skip（已知跳过）。
