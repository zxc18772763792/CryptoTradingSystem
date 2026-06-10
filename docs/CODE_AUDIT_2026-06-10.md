# 每日代码审计报告 — 2026-06-10

> 自动化运行 · HEAD: `6a13d4f`（Checkpoint: 2026-06-08 audit follow-ups + REST throttle cooldown）
> 前次基线：`CODE_AUDIT_2026-06-09.md`
> 本次运行：未提交变更扫描 + 积压项验证 + 新 Bug Hunt

---

## 一、未提交变更检查（工作树 dirty 文件）

当前工作树中有 4 个已修改但未提交的文件：

| 文件 | 状态 | 内容摘要 |
|---|---|---|
| `web/static/js/app.js` | M（修改） | 新增 `estimateBacktestOptimizeTimeoutMs()` 函数；修复参数提示文案 |
| `web/templates/index.html` | M（修改） | 止盈比例 max=1.0 → max=0.99；参数提示文案更新 |
| `tests/test_backtest_pairs_ui_assets.py` | M（修改） | 新增 3 条回归测试（动态超时、参数提示、止盈范围） |
| `data/ai_calibration/family_regime_priors.json` | M（修改） | AI 校准数据，非代码变更 |

**新增测试已验证通过（8/8 passed）**，但上述变更尚未提交。

### 已修复但未提交的回测页 Bug（来自 BACKTEST_PAGE_BUG_FIX_PLAN_2026-06-08.md）

#### ✅ Bug 1：参数优化固定使用 90s 超时
**文件：** `web/static/js/app.js`
**已修复：** 新增 `estimateBacktestOptimizeTimeoutMs(strategyName, maxTrials, timeframe, windowDaysOverride, customParams)` 函数，动态计算超时，下限 90s、上限 20min。计算考虑：
- 策略类型（Fama × universeCount、双腿、ML 各有惩罚项）
- `maxTrials`（每 trial 1000-1500ms）
- `windowDays`（每段 18s）

#### ✅ Bug 2：自定义参数提示文案过期（"参数优化暂不读取这里的 JSON"）
**文件：** `web/static/js/app.js`、`web/templates/index.html`
**已修复：** 旧文案删除，新文案 `BACKTEST_CUSTOM_PARAMS_HINT` 明确区分：运行回测与优化会读取 JSON；多策略对比不读取。

#### ✅ Bug 3：止盈比例 UI 允许 1.0 但后端拒绝
**文件：** `web/templates/index.html`
**已修复：** `max="1.0"` → `max="0.99"`；标签同步更新 `0.01~0.99`；新增客户端 `validateBacktestProtectionConfig()` 提供明确错误提示。

**建议：** 尽快将这 3 个文件提交，避免工作树内容被意外覆盖。

---

## 二、积压项状态更新（承接 CODE_AUDIT_2026-06-09.md）

| 编号 | 问题 | 本次状态 | 紧迫度 |
|---|---|---|---|
| P4-Parquet | `scripts/migrate_parquet_klines_to_utc.py --apply` 未执行 | ⚠️ **持续未执行** | 高 |
| C3-aiosqlite | 全量测试结束时 `RuntimeError: Event loop is closed` warning | ⚠️ 未处理 | 低 |
| WS-mark 静默 | mark-price 流异常无日志（`ccxt_pro_feed.py:708,743`） | ⚠️ **未修复** | 中 |
| P1-退出-arb | `cex_arbitrage`/`dex_arbitrage` 无 CLOSE 信号 | ⚠️ 持续 | 中 |
| governance-None | `_governance_rejection_reason` 无 None 守卫（`order_manager.py:281`） | ⚠️ 未修复 | 低 |
| cache-lock | `trading.py` `_BALANCE_SNAPSHOT_CACHE` 无 asyncio.Lock | ⚠️ 未修复 | 中 |
| 熔断器状态 | 2026-05-25 触发熔断器需确认是否已 reset | ⚠️ 需人工确认 | 需确认 |
| allow_close 文档 | 平仓豁免规则无明确 API 文档 | ⚠️ 建议 | 低 |

---

## 三、本次新发现问题

### Bug A — `order_manager.py` `get_order()` 可能存储 None（**低风险，防御性修复**）
**文件：** `core/trading/order_manager.py:882-883`
**严重性：** 低（当前路径几乎不触发）

**现象：**
```python
order = await connector.get_order(order_id, symbol)
self._orders[order_id] = order  # ← 若 connector.get_order 返回 None，覆盖缓存
```
`binance_connector.get_order()` 本身总是 raise 或 return Order（因 `_handle_error` 总是 re-raise），所以 `order = None` 几乎不会发生。但**其他 exchange connector** 若未遵循此约定（如 DEX 连接器），可能在 try/except 内部吞掉异常并隐式返回 None，导致有效的 Order 缓存被 None 覆盖。

**建议修复：**
```python
order = await connector.get_order(order_id, symbol)
if order is not None:
    self._orders[order_id] = order
return order
```

---

### Bug B — `ccxt_pro_feed.py` mark-price 流错误首次发生时无日志（**已知，未修复**）
**文件：** `core/marketdata/ccxt_pro_feed.py:707-712, 742-748`
**严重性：** 中

**现象：** 异常仅写入 `self._mark_last_error[name]`，无 `logger.warning()` 或 `logger.error()`。mark-price 流断连时只能通过 `/api/data/ws-feed/status` 发现，日志文件无痕迹。

**建议修复（两处 except 块）：**
```python
except Exception as exc:
    err_count = self._mark_watch_error_count.get(name, 0)
    self._mark_watch_error_count[name] = err_count + 1
    err_msg = f"{type(exc).__name__}: {exc}"
    self._mark_last_error[name] = err_msg
    if err_count == 0:  # 首次错误打印，避免日志洪泛
        logger.warning("ccxt_pro_feed: mark-price stream error [%s]: %s", name, err_msg)
    await self._reset_mark_client(name)
    await self._sleep_or_stop(stop_event, backoff)
    backoff = min(self._reconnect_max_sec, backoff * 2.0)
    continue
```

---

### Bug C — CEX/DEX 套利策略缺少主动平仓信号（**持续，影响资金占用**）
**文件：** `strategies/arbitrage/cex_arbitrage.py`、`strategies/arbitrage/dex_arbitrage.py`
**严重性：** 中

**现象：** 两个套利策略仅产生 BUY/SELL 信号，无 CLOSE_LONG/CLOSE_SHORT 信号，也无 `check_exit()` 实现。当套利价差回归后，持仓依赖全局止盈止损触发，不主动平仓。在价差持续但名义利润已消失时，资金被长期锁定。

**建议：** 在两策略中新增 `check_exit()` 方法，当价差低于入场阈值时发出 CLOSE 信号。

---

### Bug D — `trading.py` 持仓/余额快照缓存无并发保护（**已知，低风险**）
**文件：** `web/api/trading.py:385-398, 3541-3542, 3762-3763`
**严重性：** 中（Python GIL 部分缓解，最坏后果为读到过期数据）

**现象：** 两个模块级 dict `_BALANCE_SNAPSHOT_CACHE` 和 `_LIVE_POSITION_SNAPSHOT_CACHE` 在多个 async handler 中并发读写，无 `asyncio.Lock`。在高并发时 `["ts"]` 和 `["data"]` 非原子更新，理论上可能读到已清 ts 但旧 data 的状态。

**建议：** 模块顶部添加：
```python
_BALANCE_SNAPSHOT_LOCK = asyncio.Lock()
_LIVE_POSITION_SNAPSHOT_LOCK = asyncio.Lock()
```
并在相应读写处使用 `async with _BALANCE_SNAPSHOT_LOCK:`。

---

### Bug E — `_governance_rejection_reason` 无 None 守卫（**已知，防御性**）
**文件：** `core/trading/order_manager.py:281`
**严重性：** 低

**现象：** 当前 `evaluate_order_intent` 总返回 `DecisionOutcome`，但若未来重构或 Mock 测试传入 None，将在 `governance_check.allowed` 处抛 `AttributeError`。

**建议修复：**
```python
def _governance_rejection_reason(self, governance_check, request: OrderRequest) -> str:
    if governance_check is None:
        return ""
    if not governance_check.allowed:
        return f"governance blocked: {governance_check.reason}"
    ...
```

---

## 四、测试覆盖建议

以下测试场景仍未覆盖（延续上次审计建议）：

| 场景 | 严重性 | 建议测试文件 |
|---|---|---|
| `HurstExponentStrategy` 趋势 SELL（VR>1.20，z-score 向下穿越） | 高 | `tests/test_factor_strategy_hurst.py` |
| `SupplyEventStrategy.check_exit` 事件窗口到期路径 | 中 | `tests/test_supply_event_check_exit.py` |
| `order_manager.get_order()` connector 返回 None 时缓存行为 | 低 | `tests/test_order_manager_get_order.py` |
| `risk_manager` `_day_start_equity=0` 零权益边界 | 中 | `tests/test_risk_manager_zero_equity.py` |
| 套利策略 `check_exit()` 价差回归平仓 | 中 | `tests/test_arbitrage_check_exit.py` |

---

## 五、架构健康摘要

### 5.1 代码质量趋势
- 持续无新 Critical 级别 Bug
- 各轮 P0 积压（clientOrderId、funding 双计、Hurst SELL 缺失、SupplyEvent check_exit）均已修复
- 回测页 3 个 UI Bug 已在工作树中修复，待提交

### 5.2 信号完整性（更新至本轮）
| 策略 | BUY | SELL | CLOSE_LONG | CLOSE_SHORT | check_exit |
|---|---|---|---|---|---|
| MomentumStrategy | ✅ | ✅ | ✅ | ✅ | — |
| HurstExponentStrategy | ✅ | ✅ | — | — | — |
| SupplyEventStrategy | ✅ | ✅ | — | — | ✅ |
| MaxDrawdownStrategy | ✅ | ✅ | — | — | — |
| CEXArbitrageStrategy | ✅ | ✅ | ❌ 缺 | ❌ 缺 | ❌ 缺 |
| DEXArbitrageStrategy | ✅ | ✅ | ❌ 缺 | ❌ 缺 | ❌ 缺 |
| MeanReversionHalfLife | ✅ | ✅ | ✅ | ✅ | ✅ |

### 5.3 运行时 WS 状态
- mark-price 流：错误静默（Bug B，未修复）
- 主 WS 质量守卫已上线（默认关闭）
- 熔断器状态：**待人工确认**（2026-05-25 触发的 5.11% 回撤阈值）

---

## 六、行动项（按优先级）

### 立即（已就绪，只需 commit）
1. **提交工作树中的回测页修复**（`app.js`、`index.html`、`test_backtest_pairs_ui_assets.py`）：
   ```bash
   git add web/static/js/app.js web/templates/index.html tests/test_backtest_pairs_ui_assets.py
   git commit -m "Fix backtest optimize timeout, custom-params hint, and take-profit max"
   ```

### 高优先级
2. **执行 Parquet UTC 迁移**（持续积压 P4）：
   ```bash
   python scripts/migrate_parquet_klines_to_utc.py          # dry-run 确认
   python scripts/migrate_parquet_klines_to_utc.py --apply  # 执行
   ```
3. **人工确认熔断器状态**（如服务运行中）：
   ```
   POST /ops/risk/circuit-breaker/reset
   ```

### 中优先级
4. **修复 WS mark-price 流错误静默**（Bug B）：在 `ccxt_pro_feed.py:707-712, 742-748` 首次异常时添加 `logger.warning`
5. **为 CEX/DEX 套利策略添加 `check_exit()`**（Bug C）：价差回归时主动平仓
6. **为 `trading.py` 缓存添加 `asyncio.Lock`**（Bug D）
7. **添加 `HurstExponentStrategy` 趋势 SELL 单元测试**

### 低优先级
8. **修复 `_governance_rejection_reason` None 守卫**（Bug E，防御性）
9. **在 `order_manager.get_order()` 添加 None 守卫**（Bug A，防御性）
10. **修复 `aiosqlite` 测试退出 warning**（C3）
11. **文档化 `allow_close=True` 豁免规则**（补充注释或 RISK_MANAGER.md）

---

## 七、总结

本次审计重点：
- **发现**：4 个工作树未提交文件，其中 3 个已修复回测页 Bug（优化超时、参数提示、止盈范围），测试全部通过，仅待 commit
- **验证**：前次 3 项新 Bug（WS 静默、缓存无锁、governance None 守卫）均未修复，维持上次状态
- **新确认**：CEX/DEX 套利策略缺主动平仓逻辑（持续积压）
- **无新 Critical 级别 Bug**

系统整体稳定，最重要的即时动作是 **commit 工作树中已修复的回测页内容**，其余问题按优先级排期处理。

---

*报告由自动化审计任务生成 — 2026-06-10*

---

## 八、补充审计（同日第二轮 — WS live 升级专项）

> 本节为 2026-06-10 当天针对 "完成到 WS live 升级" 的专项审计与修复记录。代码修复已提交：`d95f2c4`（审计修复）及后续 evaluator/launcher 提交。

### 8.1 已修复（原积压项）
| 编号 | 问题 | 修复 |
|---|---|---|
| Bug B | mark-price 流错误静默 | `ccxt_pro_feed.py` 两处 except 增加 episode 级日志（首次 warning / 重复 debug） |
| Bug A | `get_order()` 可能缓存 None | None 守卫已加 |
| Bug E | `_governance_rejection_reason` 无 None 守卫 | None 守卫已加 |
| Bug D | 快照缓存并发 | 审计结论修正：单事件循环下两语句写之间无 await，不存在撕裂；实际改进为 `_collect_live_position_snapshot` 单飞锁（避免并发重复全量 REST 扫描，降低事件循环阻塞） |

### 8.2 本轮新发现并修复
1. **evaluator live 日志门禁误判**（高，阻塞 Level 2 验收）：`Paper trading mode: False`(8098)/`scope switched`(16194)/`exchange_watchdog`(25) 在 live 服务日志中合法存在，但被当作硬污染 → 任何带 err-log 的 live 评估必失败。已修复为 live 模式自动降级 diagnostic。
2. **quality guard 双缺口**（中，Level 4 前置）：guard 只在有浏览器订阅者时被喂样本；降级只影响 UI fan-out 不影响策略读价。已修复（headless 采样 + hub `set_ws_trust` 联动 provider）。

### 8.3 仍未处理（按原优先级保留）
- Bug C：CEX/DEX 套利策略缺 `check_exit()`（与 WS 升级无关，单独排期）
- P4-Parquet UTC 迁移未执行
- C3-aiosqlite 测试退出 warning

### 8.4 新增工具
- `scripts/market_ws_ui_primary.ps1`：Level-3 launcher（evaluator 硬门禁 + quality guard 强制开启 + ui_primary 观察 selfcheck）
- 测试：launcher 资产测试 + evaluator live 降级测试 + hub 信任 3 项测试（共 +6 测试）
