# Paper / Live 串号修复执行计划

**日期**: 2026-05-24
**问题摘要**: 模拟盘（paper）和实盘（live）的策略、历史交易、资产评估在 UI 上互相污染
**审计来源**: 三个并行 Explore agent 的审计结论（模式隔离 / 策略归属 / 数据层）

---

## 0. 根因（一句话）

**三层都不隔离，叠成完美风暴**：
- **UI 层**：每次 `/api/trading/balances` 请求都强制改全局 `risk_manager` scope，并发请求把 scope 在 paper/live 之间高频翻转
- **策略层**：策略实例没有绑定模式字段，每个 cycle 现读全局 `settings.TRADING_MODE`，刚好读到被 UI 弄脏的值
- **DB 层**：`trades` 和 `positions` 表根本没有 `mode` 列，paper 和 live 物理上同一张表

**日志证据**（[uvicorn_web_20260523_225920.err.log](../logs/uvicorn_web_20260523_225920.err.log)）：
```
23:17:13.720 INFO Paper trading mode: False
23:17:13.748 INFO Risk manager scope switched: paper -> live
23:17:13.749 INFO Paper trading mode: True
23:17:13.770 INFO Risk manager scope switched: live -> paper
```
50ms 内三次切换，每隔几秒重复。

---

## 1. 修复优先级总览

| 优先级 | 任务 | 文件 | 工作量 | 风险 |
|---|---|---|---|---|
| P0-A | 删除 balances 端点的强制 scope 改写 | `web/api/trading_balances.py` | 30 分钟 | 低（向回退方向改） |
| P0-B | `trades` / `positions` 表加 `mode` 列 + 迁移脚本 | `config/database.py` + 新迁移 | 2-3 小时 | 中（涉及 DB schema 变更） |
| P0-C | 所有 trades/positions 写入路径补 `mode` 字段 | grep `session.add(Trade)` / `session.add(Position)` | 1-2 小时 | 中 |
| P1-A | `StrategyBase` 加 `runtime_mode` 不可变字段 | `core/strategies/strategy_base.py` + manager | 2-3 小时 | 中 |
| P1-B | AI 注册路径把 runtime_mode 写进 params | `web/api/ai_research.py:1199` | 30 分钟 | 低 |
| P1-C | 历史交易端点强制传 scope 参数 | `web/api/trading.py:4703` | 1 小时 | 低 |
| P1-D | 策略命名加 `_paper_` / `_live_` 后缀 | `web/api/ai_research.py:1158` 等 | 1 小时 | 低 |
| P2-A | 用 contextvars 做请求级 scope 隔离 | 跨多个 web 文件 | 1-2 天 | 高（架构性改动） |
| P2-B | 并发测试覆盖 | 新增测试文件 | 半天 | 低 |
| P3 | 持仓查询日志记 (strategy, account_id, resolved_scope) | strategy_manager + position_manager | 1 小时 | 低 |

**总评**：P0 全做完（约 4-6 小时）能止住绝大部分串号，UI 看到的历史/持仓/资产数据立刻干净。P1 做完（再 4-5 小时）能切断"现读全局"的漂移链，根治策略级问题。P2/P3 是中长期治本。

---

## 2. P0-A — 删除 balances 端点的强制 scope 改写

### 目的
止住日志里 50ms paper↔live 来回切的洪水。

### 文件
[web/api/trading_balances.py:234-236](../web/api/trading_balances.py#L234)

### 当前代码（要改的）
```python
async def _build_all_balances_payload():
    mode_name = trading_api.execution_engine.get_trading_mode()
    is_paper_mode = trading_api.execution_engine.is_paper_mode()
    trading_api.risk_manager.set_account_scope(
        "paper" if is_paper_mode else "live", reset_baseline=False
    )
    # ... 后面是 4-18 秒的 I/O，全程暴露脏 scope 给其他请求
```

### 改成
```python
async def _build_all_balances_payload():
    mode_name = trading_api.execution_engine.get_trading_mode()
    is_paper_mode = trading_api.execution_engine.is_paper_mode()
    # 不再改 scope，只读取当前 scope；如需区分让前端拿到 mode_name 自己分辨
    current_scope = trading_api.risk_manager.get_account_scope()
    # ... 后续 I/O 使用 current_scope 作为标签返回，不修改全局状态
```

### 验证步骤
1. 启动 web，连续 5 分钟，扫 `logs/uvicorn_web_*.err.log` 验证：
   - `Risk manager scope switched` 出现频率 ≤ 1 次/分钟（仅在用户手动切模式时）
   - `Paper trading mode:` 同上
2. UI 切换 paper/live tab，验证 balance 数字仍然正确
3. 同时打开 paper 和 live 两个 tab 并行刷新，看是否还有错位

### 回退方案
直接 git revert，因为只是删除几行写操作。

### 注意事项
- 需要先验证 `risk_manager` 暴露了 `get_account_scope()` 方法；如果没有，先在 [core/risk/risk_manager.py](../core/risk/risk_manager.py) 加一个 getter
- 检查 `_build_all_balances_payload` 后续代码有没有依赖被改后的 scope，如果有，需要把依赖改成显式传 `target_scope` 参数

---

## 3. P0-B — trades / positions 表加 mode 列

### 目的
让 paper 和 live 的历史数据在 DB 层物理分离，从根本上杜绝混读。

### 文件
- [config/database.py:52-93](../config/database.py#L52)（schema 定义）
- [config/database.py:599](../config/database.py#L599)（迁移函数）

### 改动步骤

**步骤 1：修改 ORM 定义**

在 `Trade` 和 `Position` 类中加：
```python
mode = Column(String(20), default="paper", index=True, nullable=False)
```

参考 [config/database.py:147](../config/database.py#L147) 的 `AccountSnapshot.mode` 写法（已经存在并工作正常）。

**步骤 2：写迁移脚本**

在现有迁移函数（约 [database.py:599](../config/database.py#L599)）里加：
```python
# Migration: add mode column to trades and positions
for table_name in ("trades", "positions"):
    cursor.execute(f"PRAGMA table_info({table_name})")
    cols = {row[1] for row in cursor.fetchall()}
    if "mode" not in cols:
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN mode TEXT DEFAULT 'paper'")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_mode ON {table_name}(mode)")
```

**步骤 3：历史数据回填策略**

存量数据没法 100% 准确回填（不知道当时是 paper 还是 live）。三种处理方式选一：
- **(推荐) 保守**：默认全标 `paper`，反正实盘历史用户会重视，paper 误标无伤大雅
- **激进**：按 risk_manager 历史快照（`risk_trade_history_live.json` vs `risk_trade_history_paper.json`）做反向匹配
- **干净**：直接清表重来（如果用户认为旧数据已经污染、不可信）

### 验证步骤
1. 运行迁移，`PRAGMA table_info(trades)` 验证有 `mode` 列且 NOT NULL
2. 写一笔新 paper 单和一笔新 live 单，SELECT 检查 `mode` 列正确
3. 旧数据 SELECT count 不变（确认迁移没丢数据）

### 回退方案
SQLite ALTER 不支持 DROP COLUMN，但加列向后兼容，旧代码忽略新列即可。如果非要回退，按"删表-从备份恢复"流程。**做迁移前必须备份 DB**：
```bash
cp data/trading.db data/trading.db.bak_2026-05-24
```

---

## 4. P0-C — 写入路径补 mode 字段

### 目的
让所有新写入的 Trade / Position 记录都带正确的 mode 标签。

### 排查命令
```bash
# 在仓库根目录
grep -rn "session.add(Trade" core/ web/
grep -rn "session.add(Position" core/ web/
grep -rn "Trade(" core/trading/ core/accounting/
grep -rn "Position(" core/trading/ core/accounting/
```

### 修改模板
每个写入处统一改成：
```python
trade = Trade(
    # ... 原字段 ...
    mode=execution_engine.get_trading_mode(),  # 或者从信号 metadata.runtime_mode 取
)
```

### 选 mode 值的优先级
1. 如果有上层传入的明确 mode（信号 metadata、API 请求体），优先用
2. 否则用 `execution_engine.get_trading_mode()`
3. 都没有就 fallback "paper"（永远不要不写 mode）

### 关联读取路径
[web/api/trading.py:4703](../web/api/trading.py#L4703) `_iter_trade_records()` 现在已经混合两个数据源：
- `position_manager.get_closed_positions(scope=target_mode)` ✓ 已按 scope
- `risk_manager.get_trade_history(scope=target_mode)` ✓ 已按 scope

但如果直接 SELECT trades 表的路径，必须加 `WHERE mode = ?`。grep 一遍：
```bash
grep -rn "FROM trades" core/ web/
grep -rn "FROM positions" core/ web/
grep -rn ".query(Trade)" core/ web/
grep -rn ".query(Position)" core/ web/
```

### 验证步骤
1. paper 模式下下一笔单 → SQL `SELECT mode FROM trades ORDER BY id DESC LIMIT 1` 应该是 `paper`
2. 切到 live 下一笔单 → 应该是 `live`
3. UI 在 paper tab 看历史，SQL log 里应该有 `WHERE mode = 'paper'`

---

## 5. P1-A — StrategyBase 加 runtime_mode 不可变字段

### 目的
切断"策略每个 cycle 现读全局 settings.TRADING_MODE"的漂移链。

### 文件
- [core/strategies/strategy_base.py](../core/strategies/strategy_base.py)
- [core/strategies/strategy_manager.py:770-782, 890](../core/strategies/strategy_manager.py#L770)

### 改动

**StrategyBase**：
```python
class StrategyBase:
    def __init__(self, name: str, params: dict, runtime_mode: str = None):
        self.name = name
        self.params = params
        # 注册时一次性绑定，运行时不可变
        self._runtime_mode = runtime_mode or params.get("runtime_mode") or "paper"

    @property
    def runtime_mode(self) -> str:
        return self._runtime_mode  # 只读，没有 setter
```

**strategy_manager.get_strategy_runtime_mode()** 改为：
```python
def get_strategy_runtime_mode(self, name: str) -> str:
    strategy = self._strategies.get(name)
    if strategy and hasattr(strategy, "runtime_mode"):
        return strategy.runtime_mode  # 优先用实例绑定
    # fallback 链保持不变（向后兼容老策略）
    ...
```

### 注意事项
- 所有具体策略类（约 45 个）继承 StrategyBase，必须确认它们的 `__init__` 调用 `super().__init__()` 时传递 `runtime_mode`
- 如果某些策略类自己重载了 `__init__` 但不调 super，需要单独处理
- factor_strategies.py 的 18 个策略（看 MEMORY 19 条修复）已经接受 `name` kwarg，应该已经统一了

### 验证步骤
1. 单元测试：创建一个 `MAStrategy(name="test", params={}, runtime_mode="paper")`，断言 `strategy.runtime_mode == "paper"`
2. 集成测试：注册到 manager → 切全局 mode → 策略仍然返回原 mode
3. AI 路径：通过 `/api/ai/candidates/{id}/register?mode=paper` 注册，验证策略实例的 runtime_mode 是 paper

---

## 6. P1-B — AI 注册路径把 runtime_mode 写进 params

### 文件
[web/api/ai_research.py:1199-1202](../web/api/ai_research.py#L1199)

### 当前
```python
metadata = build_ai_research_strategy_metadata(target_mode=resolved_mode, ...)
strategy_manager.register_strategy(
    name=...,
    params=...,  # 没有 runtime_mode
    metadata=metadata,  # metadata 里有 runtime_mode 但 strategy 不读
)
```

### 改成
```python
metadata = build_ai_research_strategy_metadata(target_mode=resolved_mode, ...)
params_with_mode = {**original_params, "runtime_mode": resolved_mode}
strategy_manager.register_strategy(
    name=...,
    params=params_with_mode,
    metadata=metadata,
)
```

### 验证
通过 AI 候选注册一个 paper 策略 → 在 strategy_manager 里查 `params["runtime_mode"]` 应该是 `paper`。

---

## 7. P1-C — 历史交易端点强制传 scope 参数

### 文件
[web/api/trading.py:4703](../web/api/trading.py#L4703) 的 `_iter_trade_records()` 调用方

### 改动
确保所有 `/api/trading/trades` 类端点都接收 `mode` 查询参数：
```python
@router.get("/trades")
async def get_trades(
    mode: Optional[str] = Query(None, description="paper|live, default current"),
    ...
):
    target_mode = mode or execution_engine.get_trading_mode()
    return _iter_trade_records(mode=target_mode, ...)
```

前端 JS 调用时显式传：
```javascript
fetch(`/api/trading/trades?mode=${state.currentMode}`)
```

### 验证
- 前端 paper tab 调用 → 后端 SQL `WHERE mode='paper'`
- 前端 live tab 调用 → 后端 SQL `WHERE mode='live'`
- 两个 tab 并行刷新 → 各自看到的数据不污染

---

## 8. P1-D — 策略命名加 mode 后缀

### 目的
防止同算法的 paper 和 live 实例互相覆盖。

### 文件
[web/api/ai_research.py:1158](../web/api/ai_research.py#L1158) 附近的命名逻辑

### 改动
当前命名：`MAStrategy_ai_1779548203_53f4`
改成：`MAStrategy_ai_paper_1779548203_53f4` 或 `MAStrategy_ai_live_1779548203_53f4`

### 注意事项
- 现存策略名要做兼容（不强制重命名旧的）
- account_id 自动 fallback `strategy_{name}` 会跟着变，需检查 account_manager 不会因此找不到老账户

---

## 9. P2-A — contextvars 请求级 scope 隔离（中长期）

### 目的
彻底告别"全局开关"反模式，让每个 HTTP 请求有自己独立的 scope 视图。

### 设计
```python
# core/risk/scope_context.py (新文件)
import contextvars

current_scope: contextvars.ContextVar[str] = contextvars.ContextVar("current_scope", default="paper")

# 中间件：每个请求开始时设置
@app.middleware("http")
async def scope_middleware(request, call_next):
    mode = request.query_params.get("mode") or request.headers.get("X-Trading-Mode") or "paper"
    token = current_scope.set(mode)
    try:
        return await call_next(request)
    finally:
        current_scope.reset(token)

# risk_manager / order_manager 读取时
def get_balance(self):
    scope = current_scope.get()  # 不再读 self._scope
    return self._balances[scope]
```

### 影响范围
- 所有 risk_manager、order_manager、position_manager 的读路径都要改成读 contextvar
- 写路径同理
- 全局 `set_account_scope` 保留但仅用于用户**显式**切模式（不再被 HTTP handler 隐式调用）

### 工作量
1-2 天，改动面广。**P0/P1 稳定 1-2 周后再启动**。

---

## 10. P2-B — 并发测试

### 新增文件
`tests/test_concurrent_scope_switches.py`

### 用例
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
    assert results[0]["mode"] == "paper"
    assert results[1]["mode"] == "live"
    # 检查没有 scope 漂移日志

@pytest.mark.asyncio
async def test_strategy_runtime_mode_immutable_under_global_switch():
    """注册到 paper 的策略，全局切到 live 后仍然 paper"""
    strategy = register_strategy(mode="paper")
    set_global_mode("live")
    assert strategy.runtime_mode == "paper"
```

---

## 11. P3 — 诊断日志增强

### 目的
未来再出串号能快速定位。

### 改动
在 [strategy_manager.py](../core/strategies/strategy_manager.py) 和 [position_manager.py](../core/trading/position_manager.py) 的关键查询点加 DEBUG 日志：
```python
logger.debug(
    f"[scope_resolve] strategy={name} account_id={account_id} "
    f"resolved_scope={scope} source={source}"  # source: instance|metadata|account|global_fallback
)
```

平时关闭，怀疑串号时开启。

---

## 12. 执行检查清单（按顺序勾）

### 准备
- [ ] 备份 DB：`cp data/trading.db data/trading.db.bak_2026-05-24`
- [ ] git 新建分支：`git checkout -b fix/paper-live-isolation`
- [ ] 通读本文档

### P0（4-6 小时，必做）
- [ ] **P0-A**: 删 [trading_balances.py:234-236](../web/api/trading_balances.py#L234) 强制 scope 改写
- [ ] P0-A 验证：5 分钟日志无 scope flip
- [ ] **P0-B**: trades/positions 加 mode 列 + 迁移
- [ ] P0-B 验证：迁移成功，count 不变
- [ ] **P0-C**: 所有 Trade/Position 写入路径补 mode 字段
- [ ] P0-C 验证：新写记录 mode 正确
- [ ] git commit + push 备份

### P1（4-5 小时，强烈建议）
- [ ] **P1-A**: StrategyBase 加 runtime_mode 不可变字段
- [ ] **P1-B**: AI 注册写 runtime_mode 进 params
- [ ] **P1-C**: 历史交易端点强制 mode 参数
- [ ] **P1-D**: 策略命名加 mode 后缀
- [ ] P1 集成验证：注册 paper 策略 → 切全局 → 策略仍 paper

### P2（择期，根治）
- [ ] **P2-A**: contextvars 请求级隔离
- [ ] **P2-B**: 并发测试用例

### P3
- [ ] 诊断日志增强

### 收尾
- [ ] 跑完整测试套件
- [ ] UI 端到端：两个 tab 同时切换 + 刷新各 5 次，对比数据无串号
- [ ] 合并到 master + 标签 `v-paper-live-isolation-fix`

---

## 13. 风险与回滚

### DB 迁移风险
- ALTER TABLE 加列对 SQLite 是非破坏性的，旧代码读新表会忽略 mode 列
- **必须先备份 DB 文件**
- 如果迁移过程崩了，从 `.bak` 恢复

### 行为变更风险
- P0-A 之后，如果某个端点之前隐式依赖"被 balance 端点改过 scope"才能工作，会出问题——但这恰恰是我们要消除的反模式，发现一个修一个
- P1-A 之后，老策略实例如果没传 runtime_mode，会 default `paper`，可能导致原本跑 live 的策略变成 paper——所以**所有注册路径都必须显式传 mode**

### 监控建议
修复上线后 24 小时密切看：
- `logs/uvicorn_web_*.err.log` 里 `scope switched` 行数（应该接近 0）
- 用户报告的串号案例（应该清零）
- 是否有新出现的 `runtime_mode` 相关 KeyError / AttributeError

---

## 14. 相关文档与代码引用

- **审计原始报告**：本文档前半部分（根因总览）
- **关键代码位置**：
  - [web/api/trading_balances.py:234](../web/api/trading_balances.py#L234) — 主反模式
  - [core/strategies/strategy_manager.py:770-782, 890](../core/strategies/strategy_manager.py#L770) — 现读全局
  - [core/risk/risk_manager.py:59, 122-123](../core/risk/risk_manager.py#L59) — RLock 不够
  - [config/database.py:52-93](../config/database.py#L52) — trades/positions 缺 mode
  - [config/database.py:147](../config/database.py#L147) — AccountSnapshot 正确示范
  - [web/api/ai_research.py:1199-1202, 4317-4322](../web/api/ai_research.py#L1199) — AI 注册路径
  - [web/api/trading.py:4703](../web/api/trading.py#L4703) — 历史交易聚合
- **日志样本**：[logs/uvicorn_web_20260523_225920.err.log](../logs/uvicorn_web_20260523_225920.err.log)
- **MEMORY 相关条目**：`MEMORY.md` 里的 fix #29-31（trading API 性能修复，可能与本次冲突需注意）
