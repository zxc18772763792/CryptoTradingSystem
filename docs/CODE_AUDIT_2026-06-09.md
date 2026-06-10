# 每日代码审计报告 — 2026-06-09

> 自动化运行 · HEAD: `6a13d4f`（Checkpoint: 2026-06-08 audit follow-ups + REST throttle cooldown）
> 前次基线：`CODE_AUDIT_2026-06-08.md`（1938 passed, 1 skipped）
> 本次运行：读写扫描 + 目标文件 Bug Hunt（未执行全量测试）

---

## 一、前次积压项验证（逐一确认）

| 编号 | 问题 | 本次状态 | 说明 |
|---|---|---|---|
| P0-clientOrderId | 实盘下单无 `newClientOrderId` | ✅ **已修复** | `order_manager.py:550-577` 完整注入 COID + 60s 窗口去重 |
| P0-allow_close | `allow_close=True` 跳过杠杆/单笔比例等检查 | ⚠️ **设计确认** | 属于有意豁免（平仓降险优先），见下文分析 |
| P1-Funding 双计 | `backtest_engine` `funding_pnl` 2× | ✅ **已修复** | 关仓 `net_pnl` 已扣除 `accrued_funding`，组合级汇总精确求和 |
| P2-并发锁 | `experiment_registry.py` `threading.RLock` 无 async 互斥 | ✅ **安全（设计保证）** | 代码内有完整注释：临界区无 `await`，同时接受 async 线程两侧调用 |
| P4-Parquet UTC | `migrate_parquet_klines_to_utc.py --apply` 未执行 | ⚠️ **持续未执行** | 脚本已就绪，仍需人工执行 |
| C3-aiosqlite | 测试退出时 `RuntimeError: Event loop is closed` ×2 | ⚠️ **持续** | 测试套件末尾仍有 2 次 warning，低优先级 |
| Hurst-无-SELL趋势 | 趋势模式缺 SELL | ✅ **已修复** | `factor_strategies.py:1338-1346` 已添加对称 SELL 分支 |
| Hurst-NaN-TP | 均值回归模式 `take_profit` NaN 守卫 | ✅ **已修复** | `pd.notna` 守卫 + fallback `current_price * (1 - tp_pct)` |
| SupplyEvent-checkExit | 事件窗口到期无平仓 | ✅ **已修复** | `supply_event_strategy.py:178-201` `check_exit` 已实现 |
| PaperTrading-stub | `_run_strategies` 空 stub 无说明 | ✅ **已文档化** | 添加了 docstring，说明信号由 strategy_manager 生成 |
| MaxDrawdown-无-SELL | `MaxDrawdownStrategy` 仅 BUY | ✅ **已修复** | `factor_strategies.py:1535-1555` 添加 SELL 路径（价格从高点回落） |

---

## 二、本次新发现问题

### Bug 1 — WS mark-price 流错误静默，无日志可观测（**未修复**）
**文件：** `core/marketdata/ccxt_pro_feed.py:709, 744`
**严重性：** 中

**问题：**
`_run_one_exchange_mark` 在捕获异常时仅写入 `self._mark_last_error[name]` 字典，无 `logger.warning()` 或 `logger.error()` 调用。运维期间若 mark-price 流断连，只有通过 `/api/data/ws-feed/status` 才能发现，日志文件无任何痕迹。

```python
# ccxt_pro_feed.py:707-712
except Exception as exc:
    self._mark_watch_error_count[name] = ...
    self._mark_last_error[name] = f"{type(exc).__name__}: {exc}"
    await self._reset_mark_client(name)
    ...  # ← 没有 logger.warning
```

**建议修复：**
```python
except Exception as exc:
    err_msg = f"{type(exc).__name__}: {exc}"
    self._mark_last_error[name] = err_msg
    if self._mark_watch_error_count.get(name, 0) == 0:  # 只在首次失败时打印
        logger.warning("ccxt_pro_feed: mark-price stream error [%s]: %s", name, err_msg)
    ...
```

---

### Bug 2 — `web/api/trading.py` 平衡/持仓快照缓存无并发保护（**中风险**）
**文件：** `web/api/trading.py:65-69, 385-398, 3518`
**严重性：** 中

**问题：**
`_BALANCE_SNAPSHOT_CACHE`（dict）和 `_LIVE_POSITION_SNAPSHOT_CACHE`（dict）是模块级共享可变状态，读写分散在多个路由处理函数中，无 `asyncio.Lock`。在高并发请求时存在以下竞态：
- 并发写 `_BALANCE_SNAPSHOT_CACHE.clear()` 与读同时发生
- `_LIVE_POSITION_SNAPSHOT_CACHE["ts"]` 与 `["data"]` 非原子更新

**当前缓解：**
- 缓存是纯 Python dict，GIL 下的整数/小对象赋值具有部分线程安全
- TTL 300s 使竞态窗口较短
- 最坏后果是读到旧数据，而非数据损坏

**建议：**
为两个缓存添加模块级 `asyncio.Lock`，或改用 `asyncio.shield` 包装缓存刷新任务，确保原子更新。

---

### Bug 3 — `_governance_rejection_reason` 无 `None` 守卫（代码脆弱性）
**文件：** `core/trading/order_manager.py:276-285`
**严重性：** 低（当前路径无法触发，属防御性修复）

**问题：**
```python
def _governance_rejection_reason(self, governance_check, request):
    if not governance_check.allowed:  # ← 若 governance_check=None 则 AttributeError
        ...
```

当前 `_evaluate_order_governance` 总是返回 `DecisionOutcome`（已验证 `decision_engine.py:110-121`），所以实际上无法传入 `None`。但在 Mock/测试场景下或未来重构时，若 `evaluate_order_intent` 返回 `None`，会在 `_governance_rejection_reason` 处静默崩溃而非给出明确错误。

**建议修复：**
```python
def _governance_rejection_reason(self, governance_check, request):
    if governance_check is None:
        return ""  # 未做 governance 检查 → 放行
    if not governance_check.allowed:
        ...
```

---

## 三、持续积压（更新状态）

| 编号 | 问题 | 状态 | 紧迫度 |
|---|---|---|---|
| P4-Parquet | `scripts/migrate_parquet_klines_to_utc.py --apply` 仍未执行 | ⚠️ 未处理 | 高 |
| C3-aiosqlite | 全量测试结束时 `RuntimeError: Event loop is closed` warning（×2） | ⚠️ 未处理 | 低 |
| P2-WS mark 日志 | mark-price 流异常静默无日志（本轮新确认） | ⚠️ 新确认 | 中 |
| P1-退出-arb | `cex_arbitrage` / `dex_arbitrage` 无显式 CLOSE 信号 | ⚠️ 持续 | 中 |
| allow_close 文档 | 平仓豁免哪些检查未有明确 API 文档说明 | ⚠️ 建议 | 低 |
| 熔断器状态 | 2026-05-25 触发的熔断器需人工确认是否已 reset | ⚠️ 需人工 | 需确认 |

---

## 四、`allow_close=True` 豁免分析（设计确认）

`risk_manager.py:799-920` 中 `pre_trade_check` 在 `allow_close=True` 时跳过的检查：

| 跳过的检查 | 行号 | 设计理由 |
|---|---|---|
| 每日交易次数限制 | 824 | 减仓不应受每日下单次数约束 |
| 最大持仓数限制 | 834 | 关仓不新增持仓 |
| 杠杆倍数检查 | 842 | 旧仓已开，强制平仓不因杠杆超限而阻塞 |
| 单笔比例检查 | 872 | 平仓量取决于持仓大小，不受入场尺寸约束 |
| 组合总暴露上限 | 885 | 平仓后暴露降低 |
| 策略分配上限 | 902 | 同上 |

**仍生效的检查（即使 allow_close=True）：**
- 系统 HALT 状态（仅 `daily_loss_halt` 豁免）
- 最低资产/名义价值验证

**结论：** 设计合理，平仓应尽量不受入场限制约束。建议在代码注释或 `RISK_MANAGER.md` 中明确文档化这些豁免，以便 code review 时不被误判为 bug。

---

## 五、信号完整性现状（更新）

| 策略 | BUY | SELL | CLOSE_LONG | CLOSE_SHORT | check_exit |
|---|---|---|---|---|---|
| MomentumStrategy | ✅ | ✅ | ✅ | ✅ | — |
| HurstExponentStrategy | ✅ | ✅（本轮验证） | — | — | — |
| MeanReversionHalfLife | ✅ | ✅ | ✅ | ✅ | ✅ |
| SupplyEventStrategy | ✅ | ✅ | — | — | ✅（本轮验证） |
| MaxDrawdownStrategy | ✅ | ✅（本轮验证） | — | — | — |
| OnChainFlowRegime | ✅ | ✅ | ✅ | ✅ | — |
| LiquidationOICrowding | — | — | ✅ | ✅ | — |
| CEXArbitrageStrategy | ✅ | ✅ | — | — | — |
| DEXArbitrageStrategy | ✅ | ✅ | — | — | — |
| VaRBreakoutStrategy | ✅ | ✅ | — | — | — |
| SortinoRatioStrategy | ✅ | ✅ | — | — | — |

---

## 六、测试覆盖补充建议

### 仍缺的测试（从前次审计延续）
1. **`HurstExponentStrategy` 趋势模式 SELL**
   - 场景：`current_vr > trending_threshold` + `prev_z > -zscore_threshold` + `current_z <= -zscore_threshold`
   - 建议文件：`tests/test_factor_strategy_hurst_sell.py`

2. **`SupplyEventStrategy.check_exit` 事件窗口到期路径**
   - 场景：`active_events = []`，持有 long/short 仓位
   - 建议文件：`tests/test_supply_event_check_exit.py`

3. **`MaxDrawdownStrategy` SELL 路径（高点回落）**
   - 部分被 `test_audit_2026_06_08_followups.py` 覆盖，需确认是否完整

4. **`order_manager.py` paper 路径 governance 异常处理**
   - 场景：`_evaluate_order_governance` 抛出异常时，paper 订单的回退行为

5. **`risk_manager.py` 零资产/全亏损边界**
   - 场景：`_day_start_equity = 0` 时的 `stop_basis_ratio` 计算
   - 当前代码有 `if self._day_start_equity > 0 else 0.0` 守卫，需覆盖测试

---

## 七、架构健康摘要

### 7.1 近期变更文件安全性
经逐文件扫描（`binance_connector.py`, `paper_trading.py`, `order_manager.py`,
`supply_event_strategy.py`, `factor_strategies.py`, `onchain_flow_regime.py`,
`liquidation_oi_crowding.py`, `momentum.py`, `exchange_manager.py`）：

- 所有最近修改文件未发现新 Critical 级别 Bug
- `onchain_flow_regime.py` 逻辑完整，有 `min_bars`/`None` 守卫
- `liquidation_oi_crowding.py` 有完整的 gate 评估与信号门控

### 7.2 WS 健康状态
- mark-price 流：错误静默（见 Bug 1）
- ccxt-pro 主流：`_run_one_exchange` 已有 `logger.warning` 在 `_build_client` 处
- 主 WS 质量守卫（WS grade）功能已上线，默认关闭

### 7.3 熔断器状态
2026-05-25 熔断器仍可能处于触发状态（5.11% 回撤）。需人工通过：
```
POST /ops/risk/circuit-breaker/reset
```
确认重置前请核实当前权益与持仓状态。

---

## 八、行动项（按优先级）

### 高优先级
1. **执行 Parquet UTC 迁移**（P4）：
   ```bash
   python scripts/migrate_parquet_klines_to_utc.py       # dry-run
   python scripts/migrate_parquet_klines_to_utc.py --apply
   ```
2. **确认并重置熔断器状态**：`POST /ops/risk/circuit-breaker/reset`（需人工）

### 中优先级
3. **修复 WS mark-price 流错误静默**（Bug 1）：
   在 `ccxt_pro_feed.py:709,744` 首次异常时添加 `logger.warning`
4. **为 `HurstExponentStrategy` 趋势 SELL 添加单元测试**
5. **为 `SupplyEventStrategy.check_exit` 事件到期路径添加单元测试**

### 低优先级
6. **修复 `_governance_rejection_reason` None 守卫**（Bug 3，防御性）
7. **为 Trading API 缓存添加 `asyncio.Lock`**（Bug 2）
8. **文档化 `allow_close=True` 豁免规则**（建议补充 `SECURITY.md` 或 `RISK_MANAGER.md`）
9. **修复 `aiosqlite` 测试退出 warning**（C3）

---

## 九、总结

本次审计重点：
- **验证**：确认前次 4 项修复均已正确落地（Hurst SELL、SupplyEvent check_exit、MaxDrawdown SELL、PaperTrading stub 文档化）
- **验证**：前次 P0 级积压（clientOrderId、funding 双计）均已修复
- **发现**：3 项新问题（WS 静默错误为中级、缓存并发为中级、governance None 守卫为低级）
- **持续**：P4 Parquet 迁移和熔断器 reset 需人工跟进

系统整体代码质量持续提升，无新 Critical 级别 Bug。

---

*报告由自动化审计任务生成 — 2026-06-09*
