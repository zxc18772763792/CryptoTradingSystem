# 每日代码审计报告 — 2026-06-08

> 自动化运行 · HEAD: `a757303`（Fix WS shadow gate worst-age false-fire）
> 前次基线：`CODE_AUDIT_2026-06-07.md`（1926 passed, 1 skipped）
> 本次测试结果：**1938 passed, 1 skipped**（+12 tests）
> 运行时长：461s（~7 分 41 秒）

---

## 一、前次审计进度验证

| 项目 | 状态 | 说明 |
|---|---|---|
| `StochasticStrategy` `k_prev <= oversold` 修正 | ✅ 已修复 | `common_strategies.py:149` 使用 `k_prev`，且有行内注释说明原因 |
| `MeanReversionHalfLifeStrategy` 止盈方向错误 | ✅ 已修复 | 有 `if mean_value > current_price` 守卫，fallback 到百分比 |
| `HurstExponentStrategy` VR 阈值错位（0.55/0.45） | ✅ 已修复 | 现用 VR 尺度阈值 1.20/0.80，注释说明旧版 bug |
| `MomentumStrategy` 缺 CLOSE 信号 | ✅ 已修复 | `momentum.py:59-94` 有 CLOSE_LONG/CLOSE_SHORT |
| `pairs_trading` 链式比较隐式逻辑 | ✅ 已修复 | `pairs_trading.py:91-94` 改为显式 `and`，有注释 |
| `AmbiguousPositionError` 静默返回 None | ✅ 已修复 | `position_manager.py:624` 改为 `raise AmbiguousPositionError` |
| CUSUM `reset_on_trigger` 后立即再触发 | ✅ 已修复 | `strategy_monitor.py` 加 `cooldown_bars=5`，`_cooldown_until_n` 字段 |
| `bullish_keywords` 含前导空格 `" adoption"` | ✅ 已修复 | `news_collector.py:78` 现为 `"adoption"` |
| `market_sentiment` 信号时间戳偏移 | ✅ 已修复 | `market_sentiment.py:162` 用 `_bar_time(data, fallback=sample_ts)` |
| `VaRBreakoutStrategy` `var.iloc[-2]` 不必要推前 | ✅ 已修复 | 现用 `var.iloc[-1]` + 向量化 `rolling_var_quantile` |
| `orchestrator` `asyncio.TimeoutError` 被 `except Exception` 吞 | ✅ 已修复 | `orchestrator.py:1380,1391` 显式捕获 |
| `BinancePerpWsClient.normalize_event` TODO 存根 | ✅ 已修复（上次审计） | 已实现 bookTicker/aggTrade/kline/depthUpdate/markPriceUpdate 标准化 |
| P4-Parquet UTC 迁移 | ⚠️ 未执行 | 脚本已就绪，仍需 `--apply` |
| aiosqlite `RuntimeError: Event loop is closed` | ⚠️ 未处理 | 测试套件结束时仍有 2 次 warning |

---

## 二、本次新发现的 Bug 与问题

### Bug 1 — `HurstExponentStrategy` 趋势模式仅有 BUY，缺少 SELL（**已在本轮修复**）
**文件：** `strategies/factor_based/factor_strategies.py:1323-1333`
**严重性：** 高 → ✅ 已修复

**原始问题：**
趋势市场（VR > 1.20）中，策略仅在 z-score 向上穿越阈值时发出 BUY，对向下趋势（z-score 向下穿越 -threshold）完全没有 SELL 信号。在熊市趋势中策略永远沉默，占潜在机会的 50%。

**修复内容：**
在趋势模式中新增对称 SELL 分支：
```python
elif prev_z > -zt and current_z <= -zt:
    signal = self._create_signal(symbol, SignalType.SELL, ...)
    signal.stop_loss = current_price * (1 + stop_loss_pct)
    signal.take_profit = current_price * (1 - take_profit_pct)
```

---

### Bug 2 — `HurstExponentStrategy` 均值回归模式 `take_profit = mean.iloc[-1]` 无 NaN 守卫（**已在本轮修复**）
**文件：** `strategies/factor_based/factor_strategies.py:1344, 1354`
**严重性：** 中 → ✅ 已修复

**原始问题：**
在均值回归 SELL/BUY 路径上，`signal.take_profit = mean.iloc[-1]` 直接赋值，若 `zscore_period` 滚动窗口未满导致 `mean.iloc[-1]` 为 NaN，止盈价格将为 NaN。NaN take_profit 可能导致下游 `_check_position_exits` 误判或除零。

另外，对 SELL 信号未验证 `mean < current_price`（均值应低于当前价才是正确的 TP 目标），BUY 信号同理。

**修复内容：**
```python
mean_value = float(mean.iloc[-1]) if pd.notna(mean.iloc[-1]) else None
# SELL: mean 应 < current_price
if mean_value is not None and mean_value < current_price:
    signal.take_profit = mean_value
else:
    signal.take_profit = current_price * (1 - tp_pct)
```

---

### Bug 3 — `SupplyEventStrategy` 无 `check_exit`，事件窗口到期无平仓（**已在本轮修复**）
**文件：** `strategies/event_driven/supply_event_strategy.py`
**严重性：** 高 → ✅ 已修复

**原始问题：**
`SupplyEventStrategy.generate_signals` 只在有活跃事件时才产生 BUY/SELL 信号。当供给事件窗口过期（`post_event_window_days` 后 `active_events` 为空），策略不再产生任何信号，但已开仓的持仓没有任何主动平仓逻辑，只能依赖止盈止损价触发。若价格在止盈止损区间内震荡，持仓将无限期持有至系统级止损。

**修复内容：**
新增 `check_exit` 方法，当 `active_events` 为空时发出 CLOSE_LONG 或 CLOSE_SHORT：
```python
def check_exit(self, data, position):
    ctx, _ = self._context(data)
    if ctx.events.active_events:
        return None
    # 事件窗口到期 → 平仓
    sig_type = CLOSE_LONG if side == "long" else CLOSE_SHORT
    return Signal(..., metadata={"exit_reason": "event_window_expired"})
```

---

### Bug 4 — `PaperTradingEngine._run_strategies` 是空 stub，但无设计说明（**已在本轮修复**）
**文件：** `core/backtest/paper_trading.py:147-158`
**严重性：** 低 → ✅ 已修复（文档化）

**原始问题：**
`_run_strategies` 在每个 tick 被 `_main_loop` 调用，但方法体只有 `pass`。策略的 `generate_signals()` 从不被调用。代码中没有任何注释说明这是设计意图还是遗漏，造成维护混淆。

**修复内容：**
添加 docstring 明确说明：信号由 `strategy_manager` 生成，`PaperTradingEngine` 通过 `process_signal()` 接收预生成信号，此循环为调度钩子而非实际信号生成路径。

---

## 三、持续积压（更新状态）

| 编号 | 问题 | 状态 | 紧迫度 |
|---|---|---|---|
| P1-退出-arb | `cex_arbitrage` / `dex_arbitrage` 无显式 CLOSE 信号 | ⚠️ 持续 | 中（套利策略同时 BUY+SELL，平仓靠对腿，但价差消失无信号） |
| P4-Parquet | `scripts/migrate_parquet_klines_to_utc.py --apply` 仍未执行 | ⚠️ 未处理 | 高 |
| C3-aiosqlite | 全量测试结束时 `RuntimeError: Event loop is closed` warning（×2） | ⚠️ 未处理 | 低 |
| P2-WS mark 日志 | 不支持 `watch_mark_prices` 的交易所在 `_run_one_exchange_mark` 静默吞错，未暴露告警 | ⚠️ 中性 | 低（现无日志噪声，但诊断时无可见迹象） |
| P2-并发锁 | `experiment_registry.py` `threading.RLock` 在 async 单线程中不互斥 | ⚠️ 未处理 | 中 |
| P0-clientOrderId | 实盘下单无 `newClientOrderId`，网络重试可重复成交 | ⚠️ 未处理 | 极高 |
| P0-allow_close | `allow_close=True` 跳过杠杆/单笔比例等检查 | ⚠️ 未处理 | 高 |
| P1-Funding双计 | `backtest_engine` cost_decomposition funding_pnl 2× | ⚠️ 已标 per 注释，需复核 | 高 |
| Hurst-无-SELL趋势 | 已在本轮修复 | ✅ | — |
| Hurst-NaN-TP | 已在本轮修复 | ✅ | — |
| SupplyEvent-checkExit | 已在本轮修复 | ✅ | — |
| PaperTrading-stub | 已在本轮修复（文档化） | ✅ | — |

---

## 四、架构健康摘要

### 4.1 测试覆盖趋势

```
2026-05-29: 1768 passed, 1 skipped
2026-06-07: 1926 passed, 1 skipped  (+158, 主要 WS 相关)
2026-06-08: 1938 passed, 1 skipped  (+12)
```

策略测试文件数：28 个专项测试文件。核心因子策略 `factor_strategies.py` 的 `HurstExponentStrategy` 趋势 SELL 路径现在有新 Bug 修复，但该路径尚无专项单元测试。

### 4.2 信号完整性现状

| 策略 | BUY | SELL | CLOSE_LONG | CLOSE_SHORT | check_exit |
|---|---|---|---|---|---|
| MomentumStrategy | ✅ | ✅ | ✅ | ✅ | — |
| HurstExponentStrategy | ✅ | ✅（本轮新增） | — | — | — |
| MeanReversionHalfLife | ✅ | ✅ | ✅ | ✅ | ✅ |
| SupplyEventStrategy | ✅ | ✅ | — | — | ✅（本轮新增） |
| CEXArbitrageStrategy | ✅（buy_side） | ✅（sell_side） | — | — | — |
| DEXArbitrageStrategy | ✅ | ✅ | — | — | — |
| VaRBreakoutStrategy | ✅ | ✅ | — | — | — |
| MaxDrawdownStrategy | ✅ | — | — | — | — |
| SortinoRatioStrategy | ✅ | ✅ | — | — | — |

### 4.3 WS Feed 健康

- `CcxtProMarketFeed._run_one_exchange`：`_build_client` 返回 None 时直接 `return`（设计意图：无该 exchange 的 WS 支持时不重试）。这对配置错误时的诊断不友好，但 `logger.warning` 已在 `_build_client` 处打印一次。
- mark-price 流：`_run_one_exchange_mark` 的异常仅存入 `_mark_last_error[name]`，未打印 WARNING。需通过 `/api/data/ws-feed/status` 接口查看错误状态。

---

## 五、建议行动项（按优先级）

### 本轮已完成
1. ✅ `HurstExponentStrategy` 趋势 SELL 对称性修复
2. ✅ `HurstExponentStrategy` mean-revert TP NaN 守卫
3. ✅ `SupplyEventStrategy.check_exit` — 事件窗口到期自动平仓
4. ✅ `PaperTradingEngine._run_strategies` 设计意图文档化

### 本周
5. **执行 Parquet UTC 迁移**（P4）：
   ```bash
   python scripts/migrate_parquet_klines_to_utc.py      # dry-run 先确认
   python scripts/migrate_parquet_klines_to_utc.py --apply
   ```
6. **`clientOrderId` 注入**（P0）：`order_manager.py:402-595` 实盘下单加 `newClientOrderId = f"{strategy[:8]}-{ts_ms}-{seq}"` + in-memory dedup。
7. **为 `HurstExponentStrategy` 新增趋势 SELL 单元测试**：覆盖 `current_vr > trending_threshold` + `current_z <= -zscore_threshold` 场景。

### 下周
8. **`experiment_registry.py` 改 `asyncio.Lock`**：现 `threading.RLock` 在 async 单线程下无互斥语义。
9. **收紧 `allow_close=True` 风控豁免**：仅豁免 daily-loss-halt，杠杆/单笔比例仍校验。
10. **`MaxDrawdownStrategy` 补 SELL 分支**：当前只有 BUY（价格从回撤中恢复），无做空路径。

### 持续
11. **生产熔断器**：2026-05-25 熔断器（5.11% 回撤）仍处触发状态，需人工确认后 `POST /ops/risk/circuit-breaker/reset`。
12. **WS mark-price 流诊断**：在 `_run_one_exchange_mark` 异常处添加 `logger.warning`（首次失败时）或在 `/api/data/ws-feed/status` 暴露 `mark_last_error` 字段。

---

## 六、测试覆盖摘要

```
全量测试（本轮）:
  1938 passed, 1 skipped, 2 warnings
  时长：461s（~7 分 41 秒）

本轮修复相关测试（快速验证）:
  test_strategy_bug_fixes.py + test_strategy_signal_regressions.py
  + test_factor_strategy_check_exit.py + test_supply_event_strategy_windows.py
  → 103 passed, 1 skipped
```

**仍缺的测试**（建议优先补充）：
- `HurstExponentStrategy` 趋势模式 SELL 信号（`current_vr > 1.20` + z 向下穿越）
- `SupplyEventStrategy.check_exit` 事件窗口到期路径
- `VaRBreakoutStrategy` / `MaxDrawdownStrategy` / `SortinoRatioStrategy` — 无 `check_exit`，缺专项测试

---

*报告由自动化审计任务生成 — 2026-06-08*
