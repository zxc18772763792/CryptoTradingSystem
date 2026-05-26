# Paper / Live 串号修复执行计划

**日期**: 2026-05-24
**作者**: 系统审计（三个并行 Explore agent + 主审计员深入复查）
**问题摘要**: 模拟盘（paper）和实盘（live）的策略、历史交易、资产评估在 UI 上互相污染；后端日志显示 paper↔live 在 50ms 内高频切换
**修复目标**: 让 paper 与 live 在 UI、策略、数据库三层完全隔离，消除高频切换日志洪水

---

## 0. TL;DR

**根因不是一处，而是三层全失守 + 五个机制级问题，叠加成完美风暴。**

| 层 | 失守点 | 直接后果 |
|---|---|---|
| **UI / 端点层** | `trading_balances.py:234` 每次请求强制改全局 scope；`order_manager.set_paper_trading` 无条件 log；多处 `_mode_guard` 触发 restore-flap | 日志里 50ms 内 paper↔live 反复闪烁，scope 在并发请求间互相覆盖 |
| **策略层** | 策略实例没有不可变 `runtime_mode` 字段；每个 cycle 现读全局 `settings.TRADING_MODE`；AI 注册路径只把 mode 写进 metadata，不写进 params | 策略持仓和订单跑错模式 |
| **数据层** | `trades`/`positions` 表完全没有 `mode` 列；查询路径无 `WHERE mode=?` 过滤；`position_manager.set_scope` 每次切换做同步磁盘 I/O | 历史交易/持仓物理混存，UI 看到的视图必然混读 |

**修复分三阶段**：
- **Stage 1（4-6h）**：止血 — 让日志能用、停掉来回闪烁
- **Stage 2（5-7h）**：物理隔离 — DB 加 mode 列、策略绑定模式、端点强制 mode 参数
- **Stage 3（2-3 天）**：架构治本 — contextvars 请求级隔离 + 重写 `_mode_guard` 设计 + 异步持久化 + 并发测试

---

## 0.5. 执行进度（2026-05-25 更新）

实际盘点（git diff 比对 HEAD）发现大量未提交工作已经覆盖 Stage 1/2 的多数条目。状态汇总：

| 计划项 | 状态 | 备注 |
|---|---|---|
| **S1-1** order_manager if-changed | ✅ Done | 2 行加锁式判断 |
| **S1-2** balances 不写 scope | ✅ Done | 不仅删了 `set_account_scope`，还给所有 `get_risk_report` / `update_equity` 加了 `scope=` 透传 |
| **S1-3** _mode_guard 重构 | ⏸️ Defer to Stage 3 | 思路 A 会让 16 个非守卫 reader 看到漂移值；正确做法是配合 contextvars (S3-3) 一起重写 |
| **S1-4** _background_tick 去 guard | ⏸️ Not needed | S1-1 后 bg_tick 同模式调用已不 log，闪烁实际消失 |
| **S2-1** trades/positions 加 mode 列 | ⚠️ Schema added, tables unused | `config/database.py` 的 `Trade`/`Position` SA 模型全仓无人 import；实际数据走 `risk_manager._trade_history` (JSON, 已按 scope) + `position_manager._positions` (JSON 快照, 已按 scope)。加列**当前无效，未来启用 SA 时可直接用** |
| **S2-2** Trade/Position 写入补 mode | N/A | 同上，目标表无人写入；event payload 已加 mode 字段 |
| **S2-3** 历史/持仓端点带 mode | ⚠️ 部分完成 | `_iter_trade_records(mode=)` 已支持；但 [trading.py:7705](../web/api/trading.py#L7705) 调用没传；`get_positions()` 没 mode 参数；其它 5 处 `get_all_positions()` 调用未带 scope |
| **S2-4** StrategyBase runtime_mode | ✅ Done | 不可变字段 + property + 注册路径接受 kwarg + TypeError fallback |
| **S2-5** AI 注册写 params.runtime_mode | ✅ Done | `ai_research.py:1203` + `promotion_engine.py:257` 双路径都补了 |
| **S2-6** 策略命名加 mode 后缀 | ✅ Done | `_build_candidate_strategy_name(target_mode)` + promotion_engine 同步 |

### 额外收获：risk_manager 已大规模重构（+303 行）

[core/risk/risk_manager.py](../core/risk/risk_manager.py) 添加了：
- `get_account_scope()` getter
- `_scope_state_copy(scope)` / `_save_scope_state_copy(scope, state)` —— scope 状态读写分离
- `record_trade(trade, scope=)`、`get_risk_metrics(scope=)`、`get_risk_report(scope=)`、`update_equity(scope=)` —— **大多数公共 API 接受显式 scope 参数**

这是 S3-3 contextvars 隔离的**地基已经铺好**——下一步把读路径全部接到 contextvar 就能完成请求级隔离，不必再全局开关 scope。

### 测试覆盖
- `tests/test_strategy_mode_isolation.py:91-110` — `test_strategy_base_runtime_mode_is_bound_at_construction`
- `tests/core/test_runtime_persistence.py:78-122` — `test_risk_manager_record_trade_accepts_explicit_scope` + `test_risk_manager_update_equity_accepts_explicit_scope`
- `tests/test_trading_balance_routes.py:35-118` — `test_all_balances_payload_does_not_switch_risk_scope` + `test_balance_history_route_uses_resolved_mode`

### Stage 1 / Stage 2 实质上接近完成

剩余真正需要补的：

1. **S2-3 残留**（约 1 小时）：trading.py 里几个 `_iter_trade_records` / `get_all_positions` 调用补 scope 参数。2026-05-26 已补 `/api/trading/balances` paper 分支的持仓读取、浮盈和数量统计 scope 过滤，并用 `tests/test_trading_balance_routes.py::test_paper_balances_use_scoped_positions` 覆盖。
2. **跑测试验证** Stage 1+2 改动没回归
3. **commit** 当前所有改动

Stage 3 仍按原计划择期推进。

---

## 1. 关键证据

### 1.1 日志洪水（50ms 闪烁）

[logs/uvicorn_web_20260523_225920.err.log](../logs/uvicorn_web_20260523_225920.err.log)：

```
23:17:13.720 INFO order_manager:set_paper_trading - Paper trading mode: False
23:17:13.748 INFO risk_manager:set_account_scope - Risk manager scope switched: paper -> live
23:17:13.749 INFO order_manager:set_paper_trading - Paper trading mode: True
23:17:13.770 INFO risk_manager:set_account_scope - Risk manager scope switched: live -> paper
23:17:17.682 INFO order_manager:set_paper_trading - Paper trading mode: False
23:17:17.706 INFO risk_manager:set_account_scope - Risk manager scope switched: paper -> live
23:17:17.707 INFO order_manager:set_paper_trading - Paper trading mode: True
23:17:17.739 INFO risk_manager:set_account_scope - Risk manager scope switched: live -> paper
```

四次切换/秒，持续整晚。**两条 log 之间的 21-28ms gap 是 `position_manager.set_scope` 同步磁盘写+读耗时**——切换不仅是逻辑混乱，还是磁盘 hot loop。

### 1.2 切换源头映射

七个独立机制都在写全局 scope/mode 状态：

| # | 调用位置 | 触发频率 | 是否走 `_mode_guard` 锁 |
|---|---|---|---|
| ① | [execution_engine.py:3496](../core/trading/execution_engine.py#L3496) `execute_signal` | 每个进入信号 | ✅ |
| ② | [execution_engine.py:4545](../core/trading/execution_engine.py#L4545) `_close_position` | 每次平仓 | ✅ |
| ③ | [execution_engine.py:5005](../core/trading/execution_engine.py#L5005) `_execute_manual_order_single` | 每个手动单 | ✅ |
| ④ | [execution_engine.py:5636](../core/trading/execution_engine.py#L5636) `tighten_profitable_position_protection` | 周期保护检查 | ✅ |
| ⑤ | [execution_engine.py:5893](../core/trading/execution_engine.py#L5893) `_background_tick` | **每 2 秒固定** | ✅ |
| ⑥ | [trading_balances.py:234](../web/api/trading_balances.py#L234) `_build_all_balances_payload` | **每次 UI 刷新** | ❌（绕过锁） |
| ⑦ | [trading_runtime_service.py:99,102](../web/services/trading_runtime_service.py#L99) `clear_local_trading_runtime` | 用户手动清理 | ❌（绕过锁） |

`_mode_guard` 用 `_acquire_mode_lock` 互斥（[execution_engine.py:341](../core/trading/execution_engine.py#L341)），但锁**只挡其它 guard 调用**——⑥⑦ 直接打 `set_account_scope` / `set_paper_trading`，guard 内部代码看到的 scope 可能被外部偷换。

### 1.3 表 schema 缺陷

| 表 | mode 列 | 后果 |
|---|---|---|
| `trades` ([database.py:52-93](../config/database.py#L52)) | ❌ | paper/live 物理混存 |
| `positions` | ❌ | 同上 |
| `account_snapshots` ([database.py:147](../config/database.py#L147)) | ✅ | 单这张做对了 |
| `strategy_performance_snapshots` ([database.py:362](../config/database.py#L362)) | ✅ | 单这张做对了 |

### 1.4 策略实例无模式绑定

[strategy_manager.py:770-782, 890](../core/strategies/strategy_manager.py#L770) 的 `get_strategy_runtime_mode(name)`：
1. 先查 account_manager 的账户模式
2. 查不到 → 读策略 metadata.runtime_mode
3. 还查不到 → fallback 到全局 `settings.TRADING_MODE`

[ai_research.py:1199-1202](../web/api/ai_research.py#L1199) 注册 AI 候选时把 `runtime_mode` 写进 metadata 但**没写进 params**，导致后续模式查询根本读不到。

---

## 2. 修复优先级总览

| 阶段 | ID | 任务 | 文件 | 工作量 | 风险 |
|---|---|---|---|---|---|
| **Stage 1: 止血** | | | | | |
| 1 | S1-1 | `order_manager.set_paper_trading` 加 if-changed 判断 | [order_manager.py:226](../core/trading/order_manager.py#L226) | 15 分钟 | 极低 |
| 1 | S1-2 | 删除 `trading_balances` 端点的强制 scope 改写 | [trading_balances.py:234](../web/api/trading_balances.py#L234) | 30 分钟 | 低 |
| 1 | S1-3 | `_mode_guard` 重构：消除 finally 的 restore-flap | [execution_engine.py:339](../core/trading/execution_engine.py#L339) | 2 小时 | 中 |
| 1 | S1-4 | `_background_tick` 不再用 `_mode_guard` 包裹 | [execution_engine.py:5886](../core/trading/execution_engine.py#L5886) | 1 小时 | 低 |
| **Stage 2: 物理隔离** | | | | | |
| 2 | S2-1 | `trades` / `positions` 表加 `mode` 列 + 迁移 | [database.py:52-93](../config/database.py#L52) | 1.5 小时 | 中 |
| 2 | S2-2 | 所有 Trade/Position 写入路径补 `mode` 字段 | 多处 | 1.5 小时 | 中 |
| 2 | S2-3 | 历史交易 / 持仓端点强制 `mode` 查询参数 | [trading.py:4703, 5456](../web/api/trading.py#L4703) | 1 小时 | 低 |
| 2 | S2-4 | `StrategyBase` 加不可变 `runtime_mode` 字段 | [strategy_base.py](../core/strategies/strategy_base.py), [strategy_manager.py:770](../core/strategies/strategy_manager.py#L770) | 2 小时 | 中 |
| 2 | S2-5 | AI 注册把 `runtime_mode` 写进 params | [ai_research.py:1199](../web/api/ai_research.py#L1199) | 30 分钟 | 低 |
| 2 | S2-6 | 策略命名强制带 `_paper_` / `_live_` 后缀 | [ai_research.py:1158](../web/api/ai_research.py#L1158) | 1 小时 | 低 |
| **Stage 3: 架构治本** | | | | | |
| 3 | S3-1 | `position_manager.set_scope` 持久化改异步/批量 | [position_manager.py:408](../core/trading/position_manager.py#L408) | 3 小时 | 中 |
| 3 | S3-2 | 强制所有外部 scope 改动走 `_mode_guard` 锁 | 多处 | 2 小时 | 中 |
| 3 | S3-3 | contextvars 请求级 scope 隔离 | 跨多个 web 文件 | 1-2 天 | 高 |
| 3 | S3-4 | 并发测试覆盖 | `tests/test_concurrent_scope_switches.py` | 半天 | 低 |
| 3 | S3-5 | 诊断日志增强（scope 解析链） | strategy_manager + position_manager | 1 小时 | 低 |

**预算**：Stage 1 约 4 小时、Stage 2 约 7-8 小时、Stage 3 约 2-3 天。Stage 1+2 跑完即可显著改善，Stage 3 是中长期治本。

---

## 3. Stage 1 — 止血（先让日志能用、停掉来回闪烁）

> **目的**：清掉日志噪音和高频切换源。不动数据层，做完后立刻能在干净日志中验证后续修复是否有效。

### S1-1 — `order_manager.set_paper_trading` 加 if-changed 判断

**为什么**：当前代码无条件 log（[order_manager.py:226-228](../core/trading/order_manager.py#L226)），导致日志里 `Paper trading mode:` 行数远多于 `Risk manager scope switched:` 行数，掩盖真实切换频次。

**当前**：
```python
def set_paper_trading(self, enabled: bool) -> None:
    self._paper_trading = enabled
    logger.info(f"Paper trading mode: {enabled}")
```

**改为**：
```python
def set_paper_trading(self, enabled: bool) -> None:
    if self._paper_trading == enabled:
        return  # 无变化，不写、不 log
    self._paper_trading = enabled
    logger.info(f"Paper trading mode: {enabled}")
```

**验证**：
1. 启动 web，跑 5 分钟，扫 `logs/uvicorn_web_*.err.log` 里 `Paper trading mode:` 行数应明显减少
2. 数量应与 `Risk manager scope switched:` 接近（说明只在真切换时记录）

**回退**：直接 revert，无副作用。

---

### S1-2 — 删除 `trading_balances` 端点的强制 scope 改写

**为什么**：[trading_balances.py:234-236](../web/api/trading_balances.py#L234) 在 UI 每次刷新 balance 时调用 `set_account_scope`，并发请求互相覆盖，且**绕过 `_mode_guard` 的锁**，是 ⑥ 号污染源。

**当前**：
```python
async def _build_all_balances_payload():
    mode_name = trading_api.execution_engine.get_trading_mode()
    is_paper_mode = trading_api.execution_engine.is_paper_mode()
    trading_api.risk_manager.set_account_scope(
        "paper" if is_paper_mode else "live", reset_baseline=False
    )
    # ... 4-18 秒的 I/O，全程暴露脏 scope
```

**改为**：
```python
async def _build_all_balances_payload():
    mode_name = trading_api.execution_engine.get_trading_mode()
    is_paper_mode = trading_api.execution_engine.is_paper_mode()
    current_scope = trading_api.risk_manager.get_account_scope()
    # 不再写全局 scope；后续 I/O 用 current_scope 作为标签返回，让前端自行区分
```

**前置条件**：确认 [risk_manager.py](../core/risk/risk_manager.py) 有 `get_account_scope()` getter（如无则补 5 行）。

**验证**：
1. 同时打开 paper tab 和 live tab，各自刷新 5 次
2. 扫日志，`Risk manager scope switched:` 行应只在用户**显式**切模式时出现
3. 两个 tab 的数字仍正确

**回退**：直接 revert（仅删了几行写操作）。

---

### S1-3 — `_mode_guard` 重构：消除 finally 的 restore-flap

**为什么**：[execution_engine.py:339-352](../core/trading/execution_engine.py#L339) 的 try/finally 反模式——切过去做事、完了切回来——是 ①②③④⑤ 五个调用点闪烁的根源。

**两种重构思路（选一）**：

#### 思路 A（最小改动）：guard 内只切一次，不再 finally restore

```python
@contextlib.asynccontextmanager
async def _mode_guard(self, mode: str, *, reset_baseline: bool = False):
    await self._acquire_mode_lock()
    try:
        target = self._normalize_trading_mode(mode)
        if target != self._current_trading_mode():
            self._activate_runtime_mode(target, reset_baseline=reset_baseline)
        yield
        # 不再 restore previous_mode；让下一个 guard 自然切到它需要的 mode
    finally:
        self._release_mode_lock()
```

**好处**：每个 cycle 只切换一次而非两次，闪烁直接砍半，逻辑也更符合直觉（"现在跑什么 mode 就保持，不要替别人擦屁股"）。

**风险**：依赖"全局默认 mode"的外部代码（特别是没有走 guard 的端点）会看到 mode 持续漂在最后一次 guard 设置的值上。需要 ⑥⑦ 这种外部直调先消除（S1-2 / S3-2）。

#### 思路 B（更彻底）：guard 接受 scope 参数返回上下文对象，guard 内代码不再依赖全局

```python
@contextlib.asynccontextmanager
async def _mode_guard(self, mode):
    target = self._normalize_trading_mode(mode)
    ctx = ScopedRuntimeContext(target, order_manager, risk_manager, position_manager)
    yield ctx  # 调用方通过 ctx 访问 scoped 的对象
    # 全局状态完全不动
```

调用方原来用 `order_manager.create_order(...)` 改成 `ctx.order_manager.create_order(...)`。

**好处**：根治"动全局"。
**坏处**：改动面很大，建议 Stage 3 配合 contextvars 一起做。

**Stage 1 推荐**：选思路 A，作为过渡。

**验证**：
1. 复现高频信号场景（启 5 个 AI 候选、混 paper + live），跑 5 分钟
2. `Risk manager scope switched:` 行数应减半
3. 没有任何信号执行报错（确认逻辑等价）

**回退**：保留原 finally 分支的代码于 git history，可单 commit revert。

---

### S1-4 — `_background_tick` 不再用 `_mode_guard` 包裹

**为什么**：[execution_engine.py:5886-5897](../core/trading/execution_engine.py#L5886) 每 2 秒（`_bg_check_interval_seconds = 2.0`）跑一次背景检查，无论 mode 有没有变都进 `_mode_guard`。在 S1-3 改造之前，这是稳定的 2 秒一次"心跳式闪烁"。

**当前**：
```python
async def _background_tick(self):
    modes = ("paper",) if self._default_paper_trading else ("live",)
    for mode in modes:
        async with self._mode_guard(mode):
            if mode == "live":
                await self._reconcile_local_positions_with_exchange()
            await self._check_conditional_orders()
            await self._check_protective_orders()
```

**改为**：
```python
async def _background_tick(self):
    # 背景 tick 不应该改全局 mode；conditional/protective 检查应自己按 cond.account_id 解析
    if not self._default_paper_trading:
        await self._reconcile_local_positions_with_exchange()
    await self._check_conditional_orders_scoped()      # 内部按每个 cond 独立解析 mode
    await self._check_protective_orders_scoped()
```

`_check_conditional_orders_scoped` 把原 `_check_conditional_orders` 改成"先按 mode 分组 cond，再各自处理"（已有 [execution_engine.py:5838](../core/trading/execution_engine.py#L5838) 的过滤逻辑可复用）。

**验证**：
1. 不启动任何信号，纯静默 web 跑 5 分钟
2. 日志里 `scope switched:` 应**完全为 0**
3. conditional/protective 检查日志仍然出现（确认功能没丢）

**回退**：恢复 `_mode_guard` 包装即可。

---

## 4. Stage 2 — 物理隔离（数据库 + 策略绑定）

> **目的**：让 paper 和 live 在 DB schema、策略实例、API 端点三个维度上**物理不可串号**。

### S2-1 — `trades` / `positions` 表加 mode 列 + 迁移

**为什么**：[database.py:52-93](../config/database.py#L52) 缺 `mode` 列。`AccountSnapshot` 已有正确示范（[database.py:147](../config/database.py#L147)）。

**步骤 1 — 修改 ORM**：

在 `Trade` 和 `Position` 类各加：
```python
mode = Column(String(20), default="paper", index=True, nullable=False)
```

**步骤 2 — 迁移脚本**：

在 [database.py:599](../config/database.py#L599) 附近的迁移函数加：
```python
for table_name in ("trades", "positions"):
    cursor.execute(f"PRAGMA table_info({table_name})")
    cols = {row[1] for row in cursor.fetchall()}
    if "mode" not in cols:
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN mode TEXT DEFAULT 'paper'")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_mode ON {table_name}(mode)")
```

**步骤 3 — 存量数据回填策略**（三选一）：
- **(推荐) 保守**：默认全标 `paper`（旧数据多半是 paper，误标无伤）
- **激进**：反查 `risk_trade_history_{scope}.json` 做匹配
- **干净**：清表重来（如果旧数据已认为不可信）

**前置 — DB 必须备份**：
```bash
cp data/trading.db data/trading.db.bak_2026-05-24
```

**验证**：
1. 跑迁移，`PRAGMA table_info(trades)` 出现 `mode` 列
2. 旧数据 `SELECT count(*)` 不变
3. 写一笔新 paper / live 单，`SELECT mode FROM trades ORDER BY id DESC LIMIT 2` 正确

**回退**：SQLite 不支持 DROP COLUMN，但加列向后兼容。如果非要回退，从 `.bak` 恢复。

---

### S2-2 — 所有 Trade/Position 写入路径补 mode 字段

**为什么**：表加了列，但如果写入代码不显式设 mode，会全部 default 成 `paper`，paper/live 仍然混存。

**排查命令**：
```bash
cd /e/9_Crypto/crypto_trading_system
grep -rn "session.add(Trade" core/ web/
grep -rn "session.add(Position" core/ web/
grep -rn "Trade(" core/trading/ core/accounting/
grep -rn "Position(" core/trading/ core/accounting/
```

**修改模板**：
```python
trade = Trade(
    # ... 原字段 ...
    mode=execution_engine.get_trading_mode(),  # 或从 signal.metadata.runtime_mode
)
```

**mode 取值优先级**：
1. 上层显式传入（signal metadata、API body）
2. `execution_engine.get_trading_mode()`
3. `"paper"`（永远不要不写 mode）

**关联 — SELECT 路径全部加 `WHERE mode=?`**：
```bash
grep -rn "FROM trades" core/ web/
grep -rn "FROM positions" core/ web/
grep -rn ".query(Trade)" core/ web/
grep -rn ".query(Position)" core/ web/
```

**验证**：
1. paper 模式开仓 → 平仓，SQL `SELECT mode FROM trades ORDER BY id DESC LIMIT 1` = `paper`
2. 切 live 重复 → `live`
3. 历史查询 SQL log 出现 `WHERE mode=?`

**回退**：写入逻辑保留兜底 default 不变，可单 commit revert。

---

### S2-3 — 历史交易 / 持仓端点强制 mode 查询参数

**为什么**：[trading.py:4703](../web/api/trading.py#L4703) `_iter_trade_records()` 内部已按 scope 过滤，但调用它的端点没强制要求前端传 mode。[trading.py:5456](../web/api/trading.py#L5456) `get_positions()` 同样问题。

**改动模板**：
```python
@router.get("/trades")
async def get_trades(
    mode: Optional[str] = Query(None, description="paper|live, default current"),
    ...
):
    target_mode = mode or execution_engine.get_trading_mode()
    return _iter_trade_records(mode=target_mode, ...)


@router.get("/positions")
async def get_positions(
    mode: Optional[str] = Query(None),
    ...
):
    target_mode = mode or execution_engine.get_trading_mode()
    return position_manager.get_all_positions(scope=target_mode)
```

**前端调整**：
```javascript
fetch(`/api/trading/trades?mode=${state.currentMode}`)
fetch(`/api/trading/positions?mode=${state.currentMode}`)
```

**验证**：
1. paper tab 调用 → 后端 SQL 含 `WHERE mode='paper'`
2. live tab 调用 → 含 `WHERE mode='live'`
3. 两个 tab 并行刷新 → 各自正确，不污染

---

### S2-4 — `StrategyBase` 加不可变 runtime_mode 字段

**为什么**：[strategy_manager.py:770-782](../core/strategies/strategy_manager.py#L770) 的"现读全局 settings.TRADING_MODE"是 ② 号污染路径。策略实例必须自己带模式，不能依赖外部。

**改动**：

[core/strategies/strategy_base.py](../core/strategies/strategy_base.py)：
```python
class StrategyBase:
    def __init__(self, name, params, runtime_mode: str = None):
        self.name = name
        self.params = params
        # 注册时一次性绑定，运行时不可变
        self._runtime_mode = runtime_mode or params.get("runtime_mode") or "paper"

    @property
    def runtime_mode(self) -> str:
        return self._runtime_mode  # 只读，没有 setter
```

[strategy_manager.py:770](../core/strategies/strategy_manager.py#L770) `get_strategy_runtime_mode`：
```python
def get_strategy_runtime_mode(self, name: str) -> str:
    strategy = self._strategies.get(name)
    if strategy and hasattr(strategy, "runtime_mode"):
        return strategy.runtime_mode  # 优先用实例绑定
    # 老 fallback 链保留（兼容老策略）
    ...
```

**注意**：
- 所有具体策略类（约 45 个，含 factor_strategies.py 18 个）继承 StrategyBase，**确认它们的 `__init__` 都 `super().__init__()` 透传 runtime_mode**
- MEMORY 里 fix #19 已经统一了 factor_strategies 接受 `name` kwarg，runtime_mode 参考它的做法

**验证**：
1. 单元测试：`MAStrategy(name="t", params={}, runtime_mode="paper").runtime_mode == "paper"`
2. 集成测试：注册到 manager → 调 `execution_engine.set_paper_trading(False)` → 策略仍 paper
3. AI 路径：`/api/ai/candidates/{id}/register?mode=paper` 注册后实例 runtime_mode = paper

---

### S2-5 — AI 注册把 runtime_mode 写进 params

**为什么**：[ai_research.py:1199-1202](../web/api/ai_research.py#L1199) 当前只把 `runtime_mode` 写进 metadata，但 S2-4 的 StrategyBase 从 params 读，没改的话 S2-4 对 AI 路径无效。

**当前**：
```python
metadata = build_ai_research_strategy_metadata(target_mode=resolved_mode, ...)
strategy_manager.register_strategy(
    name=...,
    params=...,  # 没有 runtime_mode
    metadata=metadata,
)
```

**改为**：
```python
metadata = build_ai_research_strategy_metadata(target_mode=resolved_mode, ...)
params_with_mode = {**original_params, "runtime_mode": resolved_mode}
strategy_manager.register_strategy(
    name=...,
    params=params_with_mode,
    metadata=metadata,
)
```

**验证**：通过 AI 候选 register?mode=paper → `strategy.runtime_mode == "paper"`

---

### S2-6 — 策略命名带 mode 后缀

**为什么**：同算法的 paper 实例和 live 实例当前命名相同（如 `MAStrategy_ai_1779548203_53f4`），会互相覆盖；account_id 自动 fallback `strategy_{name}` 也会冲突。

**改动**：[ai_research.py:1158](../web/api/ai_research.py#L1158) 附近命名逻辑加 mode 后缀：
```
旧：MAStrategy_ai_1779548203_53f4
新：MAStrategy_ai_paper_1779548203_53f4
    MAStrategy_ai_live_1779548203_53f4
```

**注意**：
- 存量策略名保持原样（不强制重命名）
- account_manager 不会因此找不到老账户（兼容旧名）

---

## 5. Stage 3 — 架构治本

> **目的**：消除"全局开关"反模式本身，让 paper/live 在架构层面不可能再串。Stage 1+2 稳定运行 1-2 周后启动。

### S3-1 — `position_manager.set_scope` 持久化改异步/批量

**为什么**：[position_manager.py:408-418](../core/trading/position_manager.py#L408) 每次切换做同步磁盘写+读（解释了日志里 21-28ms 的 gap）。高频信号下是真实瓶颈。

**改为**：
- 持久化操作 schedule 到后台 worker（asyncio.create_task 或专用 thread）
- `set_scope` 只动内存 `self._scope`，磁盘 flush 每 5 秒/每 N 次操作一次

```python
def set_scope(self, scope):
    target = self._normalize_scope(scope)
    if target == self._scope:
        return
    self._scope = target
    self._mark_persist_pending()  # 异步排队，不阻塞
    self._restore_scope_state_from_cache(target)  # 优先内存缓存
```

**风险**：进程崩溃时未 flush 的状态丢失。补救：进程退出 hook 强制 flush。

**验证**：profile 高频信号场景，`set_scope` 耗时应从 20-30ms 降到 <1ms。

---

### S3-2 — 强制所有外部 scope 改动走 `_mode_guard` 锁

**为什么**：`_mode_guard` 的锁只挡其它 guard 调用，⑥ `trading_balances.py:234`、⑦ `clear_local_trading_runtime` 等直接打 setter 的代码完全不受锁约束。

**做法**：把 `risk_manager.set_account_scope` 和 `order_manager.set_paper_trading` 改为 **私有 + 加锁**：

```python
# risk_manager
def _set_account_scope_unlocked(self, scope, reset_baseline=False):
    """Internal use only — caller must hold execution_engine._mode_lock."""
    ...

def set_account_scope(self, scope, reset_baseline=False):
    raise RuntimeError("Use execution_engine._mode_guard() instead")
```

外部所有调用点改成：
```python
async with execution_engine._mode_guard(target):
    # do work that needs that scope
```

**注意**：测试代码（如 [tests/test_account_scoped_live_paths.py](../tests/test_account_scoped_live_paths.py)、[tests/test_strategy_mode_isolation.py](../tests/test_strategy_mode_isolation.py)）也要跟着改。

---

### S3-3 — contextvars 请求级 scope 隔离

**目的**：彻底告别"全局开关"——每个 HTTP 请求有自己独立的 scope 视图，互不污染。

**设计**：
```python
# core/risk/scope_context.py (新文件)
import contextvars

current_scope: contextvars.ContextVar[str] = contextvars.ContextVar(
    "current_scope", default="paper"
)

# 中间件：每个请求开始时设置
@app.middleware("http")
async def scope_middleware(request, call_next):
    mode = request.query_params.get("mode") \
        or request.headers.get("X-Trading-Mode") \
        or default_mode()
    token = current_scope.set(mode)
    try:
        return await call_next(request)
    finally:
        current_scope.reset(token)

# risk_manager / order_manager / position_manager 读取时
def get_balance(self):
    scope = current_scope.get()  # 不再读 self._scope
    return self._balances[scope]
```

**影响范围**：所有 risk_manager、order_manager、position_manager 的读/写路径都要改成读 contextvar。

**改造前提**：S3-2 必须先完成（消除外部直调），否则 contextvars 和全局 `_scope` 会两套并存更乱。

**工作量**：1-2 天，改动面广。

---

### S3-4 — 并发测试覆盖

**新增文件**：`tests/test_concurrent_scope_switches.py`

```python
import asyncio
import pytest

@pytest.mark.asyncio
async def test_concurrent_balance_requests_dont_mix_scopes():
    """两个并发 balance 请求不应该互相污染 scope"""
    results = await asyncio.gather(
        fetch_balances(mode="paper"),
        fetch_balances(mode="live"),
        fetch_balances(mode="paper"),
        fetch_balances(mode="live"),
    )
    for r, expected in zip(results, ["paper", "live", "paper", "live"]):
        assert r["mode"] == expected

@pytest.mark.asyncio
async def test_strategy_runtime_mode_immutable_under_global_switch():
    """注册到 paper 的策略，全局切到 live 后仍然 paper"""
    strategy = register_strategy(mode="paper")
    set_global_mode("live")
    assert strategy.runtime_mode == "paper"

@pytest.mark.asyncio
async def test_mode_guard_no_flap_when_modes_match():
    """连续多次 _mode_guard 同 mode 不应产生 set_paper_trading log"""
    with capture_logs() as logs:
        for _ in range(10):
            async with execution_engine._mode_guard("paper"):
                pass
    assert sum(1 for l in logs if "Paper trading mode:" in l) == 0
```

---

### S3-5 — 诊断日志增强

为以后再出串号能快速定位，在 [strategy_manager.py](../core/strategies/strategy_manager.py) 和 [position_manager.py](../core/trading/position_manager.py) 的关键查询点加 DEBUG 日志：

```python
logger.debug(
    f"[scope_resolve] strategy={name} account_id={account_id} "
    f"resolved_scope={scope} source={source}"  # source: instance|metadata|account|global_fallback
)
```

平时关闭，怀疑串号时 ENV 开关启用。

---

## 6. 执行检查清单

### 准备
- [ ] **DB 备份**：`cp data/trading.db data/trading.db.bak_2026-05-24`
- [ ] 新分支：`git checkout -b fix/paper-live-isolation`
- [ ] 通读本文档

### Stage 1（4 小时）
- [ ] S1-1 `order_manager.set_paper_trading` 加 if-changed
- [ ] S1-1 验证：5 分钟日志里 `Paper trading mode:` 行数 ≈ `scope switched:` 行数
- [ ] S1-2 删 `trading_balances.py:234` 强制改 scope
- [ ] S1-2 验证：两 tab 并行刷新不污染
- [ ] S1-3 `_mode_guard` 重构（思路 A）
- [ ] S1-3 验证：高频信号下 `scope switched:` 行数减半
- [ ] S1-4 `_background_tick` 去掉 guard 包裹
- [ ] S1-4 验证：静默 5 分钟无任何 scope switched 日志
- [ ] git commit

### Stage 2（7-8 小时）
- [ ] S2-1 加 mode 列 + 迁移
- [ ] S2-1 验证：迁移成功，count 不变
- [ ] S2-2 写入路径补 mode
- [ ] S2-2 验证：新写记录 mode 正确
- [ ] S2-3 历史/持仓端点接受 mode 参数 + 前端透传
- [ ] S2-3 验证：SQL log 出现 WHERE mode=?
- [ ] S2-4 StrategyBase 加 runtime_mode 字段
- [ ] S2-5 AI 注册写 runtime_mode 进 params
- [ ] S2-6 策略命名加 mode 后缀
- [ ] Stage 2 集成验证：注册 paper 策略 → 全局切 live → 策略仍 paper
- [ ] git commit + push 备份

### Stage 3（择期，2-3 天）
- [ ] S3-1 position_manager 持久化异步化
- [ ] S3-2 setter 私有化 + 强制走 guard
- [ ] S3-3 contextvars 请求级隔离
- [ ] S3-4 并发测试
- [ ] S3-5 诊断日志增强

### 收尾
- [ ] 全测试套件跑通（`pytest tests/`）
- [ ] UI 端到端验证：两 tab 同时切换 + 刷新各 5 次，数据无串号
- [ ] 24 小时观察期：日志里 `scope switched:` < 10/小时
- [ ] 合并 master + 标签 `v-paper-live-isolation-fix`

---

## 7. 风险与回滚

### DB 迁移风险
- SQLite ALTER ADD COLUMN 非破坏性，旧代码读新表会忽略 mode
- **必须先备份 DB 文件**
- 失败回退：从 `.bak` 恢复

### S1-3 `_mode_guard` 思路 A 的隐性风险
- 不再 restore previous_mode 意味着"最后一个 guard 设置的 mode 会留到下一个 guard"
- 如果有代码隐式依赖"操作完成后 mode 自动恢复默认"，会出问题
- 缓解：S1-3 上线前 grep 所有 `_resolve_signal_trading_mode` 调用点，确认每个调用都显式传 mode

### S2-4 StrategyBase 改动的隐性风险
- 老策略实例如果没传 runtime_mode，会 default `paper`
- 原本跑 live 的老策略可能变 paper
- 缓解：所有注册路径在 S2-5/S2-6 之前必须显式传 mode；上线前 grep `register_strategy(` 确认每个调用都传

### S3-3 contextvars 改动的隐性风险
- 涉及面广，回归风险高
- 缓解：feature flag 渐进上线（先只对部分端点开启 middleware）

### 上线后监控
24 小时密切观察：
- `logs/uvicorn_web_*.err.log` 里 `scope switched` 频率（应 < 10/小时）
- 用户报告的串号案例（应清零）
- 新出现的 `runtime_mode` 相关 KeyError / AttributeError

---

## 8. 参考资料

### 关键代码位置（带行号）

#### 切换源（7 处）
- [execution_engine.py:3496](../core/trading/execution_engine.py#L3496) — execute_signal
- [execution_engine.py:4545](../core/trading/execution_engine.py#L4545) — _close_position
- [execution_engine.py:5005](../core/trading/execution_engine.py#L5005) — _execute_manual_order_single
- [execution_engine.py:5636](../core/trading/execution_engine.py#L5636) — tighten_profitable_position_protection
- [execution_engine.py:5886-5897](../core/trading/execution_engine.py#L5886) — _background_tick (2s 周期)
- [trading_balances.py:234-236](../web/api/trading_balances.py#L234) — _build_all_balances_payload (绕过锁)
- [trading_runtime_service.py:99-102](../web/services/trading_runtime_service.py#L99) — clear_local_trading_runtime (绕过锁)

#### 核心机制
- [execution_engine.py:328-352](../core/trading/execution_engine.py#L328) — `_activate_runtime_mode` + `_mode_guard`
- [execution_engine.py:182](../core/trading/execution_engine.py#L182) — `_bg_check_interval_seconds = 2.0`
- [order_manager.py:226-228](../core/trading/order_manager.py#L226) — set_paper_trading（无条件 log）
- [risk_manager.py:329-342](../core/risk/risk_manager.py#L329) — set_account_scope（有 if-changed）
- [position_manager.py:408-418](../core/trading/position_manager.py#L408) — set_scope（同步 I/O）

#### 策略层
- [strategy_manager.py:770-782, 890](../core/strategies/strategy_manager.py#L770) — get_strategy_runtime_mode
- [ai_research.py:1199-1202](../web/api/ai_research.py#L1199) — AI 注册（metadata only）
- [ai_research.py:1158](../web/api/ai_research.py#L1158) — 候选命名
- [ai_research.py:4317-4322](../web/api/ai_research.py#L4317) — register_ai_candidate

#### 数据层
- [database.py:52-93](../config/database.py#L52) — Trade / Position schema（缺 mode）
- [database.py:147](../config/database.py#L147) — AccountSnapshot.mode（正确示范）
- [database.py:362](../config/database.py#L362) — StrategyPerformanceSnapshot.mode（正确示范）
- [database.py:599](../config/database.py#L599) — 迁移函数入口
- [trading.py:4703](../web/api/trading.py#L4703) — _iter_trade_records（已按 scope）
- [trading.py:5456](../web/api/trading.py#L5456) — get_positions（未按 scope 过滤）

### 日志样本
- [logs/uvicorn_web_20260523_225920.err.log](../logs/uvicorn_web_20260523_225920.err.log) — 50ms 闪烁现场

### 相关 MEMORY 条目
- fix #19：factor_strategies 接受 name kwarg（S2-4 改造参考其做法）
- fix #21：market_data cache（S3-1 异步化参考）
- fix #29-31：trading API 性能修复（S1-2 的 fast-path 不要冲掉）

### 相关历史文档
- [docs/CODE_AUDIT_2026-05-21.md](CODE_AUDIT_2026-05-21.md) — 上次审计
- [docs/EXIT_LOGIC_OVERHAUL_PLAN_2026-05-20.md](EXIT_LOGIC_OVERHAUL_PLAN_2026-05-20.md) — `_background_tick` 历史改造

---

## 9. 总结

```
┌─ Stage 1 (4h) ─────────────────────────────────────┐
│  止血：日志干净 + 高频源关掉                        │
│  S1-1  set_paper_trading if-changed                │
│  S1-2  trading_balances 不改 scope                 │
│  S1-3  _mode_guard 不 restore                      │
│  S1-4  _background_tick 不进 guard                 │
└──────────────────┬──────────────────────────────────┘
                   ▼ 日志清爽，能看清剩下的真切换
┌─ Stage 2 (7-8h) ───────────────────────────────────┐
│  物理隔离：DB schema + 策略实例 + 端点参数          │
│  S2-1/2/3  trades/positions 加 mode 列 + 写入 + 查询│
│  S2-4/5/6  Strategy 绑定 runtime_mode + AI + 命名   │
└──────────────────┬──────────────────────────────────┘
                   ▼ UI 看到的数据永远分得开
┌─ Stage 3 (2-3 天) ─────────────────────────────────┐
│  架构治本：消除全局开关本身                         │
│  S3-1  position_manager 异步持久化                  │
│  S3-2  setter 强制走锁                              │
│  S3-3  contextvars 请求级隔离                       │
│  S3-4/5  并发测试 + 诊断日志                        │
└─────────────────────────────────────────────────────┘
```

**第一刀切哪里**：建议从 **S1-1** 开始（15 分钟，0 风险），让后续每一步改完都能在干净日志里立刻验证效果。
