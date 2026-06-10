# 每日代码审计报告 — 2026-06-11

> 自动化运行 · HEAD: `0532fe2`（Add Level-3 ui_primary launcher; fix evaluator live log gates; doc checkpoint）
> 前次基线：`CODE_AUDIT_2026-06-10.md`
> 本次运行：工作树变更提交 + 新 Bug 修复 + 全量测试校验

---

## 一、工作树变更提交（承接 2026-06-10 审计遗留）

上次审计结束时有 5 个已修改未提交文件 + 1 个新测试文件。本轮**推荐提交**（包含本轮新增修复）：

| 文件 | 类型 | 内容 |
|---|---|---|
| `strategies/arbitrage/cex_arbitrage.py` | 功能修复 | 新增 `check_exit()`：CEX/三角套利价差收敛自动平仓 |
| `strategies/arbitrage/dex_arbitrage.py` | 功能修复 | 新增 `check_exit()`：DEX 套利价差收敛自动平仓 |
| `web/api/backtest.py` | Bug 修复 | 导出接口自动扩展窗口（镜像 /run 行为），`\\n` 转义修复 |
| `tests/test_arbitrage_check_exit.py` | 新测试 | 24 测试覆盖 CEX/三角/DEX check_exit（全部通过） |
| `tests/test_macro_workbench_and_premium_status.py` | 测试修复 | 见 §2 Bug A |
| `pytest.ini` | 配置修复 | 见 §2 Bug A |
| `docs/CODE_AUDIT_2026-06-10.md` | 文档 | 补充 §8.5 套利修复记录 |
| `docs/MARKET_DATA_WS_UPGRADE_PLAN_2026-05-29.md` | 文档 | 补充 §33.5 Level-3 切换记录 |

---

## 二、本次新发现并已修复的问题

### Bug A — `test_macro_workbench_and_premium_status.py` 收集失败（**已修复**）

**文件：** `tests/test_macro_workbench_and_premium_status.py:412`，`pytest.ini`
**严重性：** 中（每次全量 pytest 报 `ERROR` 导致 -x 模式整体停测）

**现象：**
```
ERROR tests/test_macro_workbench_and_premium_status.py
'asyncio' not found in `markers` configuration option
```
`test_premium_data_status_reports_cached_fred_macro` 使用 `@pytest.mark.asyncio` 装饰，但 `pytest-asyncio` 未安装（`asyncio_mode = auto` 作为 unknown config 被忽略），且 `pytest.ini` 的 `--strict-markers` 未将 `asyncio` 注册为合法 marker。

**修复：**
1. `pytest.ini`：在 `markers` 列表中新增 `asyncio: marks async tests`
2. `test_macro_workbench_and_premium_status.py:412`：`@pytest.mark.asyncio` + `async def` → 移除装饰器，改 `await` → `asyncio.run()`（与文件中其他测试一致）

**验证：** 6/6 测试通过（独立运行）

---

## 三、套利策略 check_exit 实现（Bug C — 已修复，本轮确认）

### 3.1 CEXArbitrageStrategy
**文件：** `strategies/arbitrage/cex_arbitrage.py`

- 新增辅助函数：`_position_side_text`、`_normalize_symbol_key`、`_resolve_exit_ratio`、`_close_signal_price`、`_observation_age_sec`
- 新增 `_lookup_price_book`：标准化 symbol key 后搜索缓存
- 新增 `_current_effective_spread`：优先使用入场交易对（`buy_exchange/sell_exchange` from metadata），fallback 全市场最优对
- 新增 `check_exit`：spread ≤ `exit_spread_ratio` × `min_spread` 时发 CLOSE 信号，stale book（age > `exit_price_max_age_sec`）不平仓
- 入场信号 metadata 新增 `buy_exchange`/`sell_exchange`（供 `check_exit` 按入场对精准匹配）

### 3.2 TriangularArbitrageStrategy
**文件：** `strategies/arbitrage/cex_arbitrage.py`

- 新增 `_last_edge_obs: Dict[str, Dict]`：记录每次 `generate_signals_async` 计算到的 raw edge（含 0/负值）
- 新增 `check_exit`：当 `edge_abs ≤ exit_spread_ratio × min_profit` 时平仓，stale obs 不平仓

### 3.3 DEXArbitrageStrategy
**文件：** `strategies/arbitrage/dex_arbitrage.py`

- 新增 `_last_spread_obs: Dict[str, Dict]`：记录每次 `find_arbitrage_opportunities` 的最优 profit_pct
- 新增静态方法 `_pair_key`：对代币名排序后生成统一 key（"ETH"/"USDC" → "ETH/USDC"，与正反向一致）
- 新增 `check_exit`：`profit_pct ≤ exit_spread_ratio × min_spread` 时平仓

**测试覆盖：** `tests/test_arbitrage_check_exit.py` — 24 测试，全部通过
```
24 passed in 5.70s
```

---

## 四、回测导出 Bug 修复（工作树未提交变更确认）

**文件：** `web/api/backtest.py:5214-5224`

### Bug F1 — 导出接口窗口过窄时不扩展
**现象：** `/run` 会自动扩展数据窗口到 `_min_required_bars`，但 `/export` 不做此操作，相同参数可能 400 或导出空报告。
**修复：** 先标准化全量 df (`full_df`)，再按 bounds 过滤；若过滤后 `len(df) < min_bars` 且 `full_df` 足够，自动扩展。

### Bug F2 — PDF 报告文字换行不生效
**现象：** f-string 中使用 `\\n`（转义字符串），matplotlib `fig.text` 收到字面量 `\n` 而非换行符，报告中所有指标挤在一行。
**修复：** `\\n` → `\n`

---

## 五、全量测试结果

```
1923 passed, 61 failed, 1 skipped, 8 errors (in 400s)
```

### 5.1 新增通过测试
- `tests/test_arbitrage_check_exit.py`：24 tests（本次新增）
- `tests/test_macro_workbench_and_premium_status.py`：6 tests（本次修复，原为 collection ERROR）

### 5.2 61 个预存失败（测试隔离问题）
这 61 个失败**均为预存的测试隔离问题**，非本次代码变更引入：

| 测试文件 | 失败数 | 根因 |
|---|---|---|
| `test_strategy_mode_isolation.py` | 7 | import-time 模块级单例状态在全量套件中被其他 test 污染 |
| `test_order_manager_safety.py` | 1 | 同上 |
| `test_web_main_runtime_tasks.py` | 2 | 同上 |
| `web/test_altcoin_route.py` | 1 | 同上 |
| 其他策略/AI/研究测试 | ~50 | 全量套件 shared state 污染 |

**验证方式：** 以上所有失败测试在单独运行或小批次运行时均 **PASS**。

### 5.3 8 个 ERROR（pre-existing）
`tests/test_exchanges.py::TestBinanceConnector` — 全量套件中 event loop 冲突，独立运行时正常。

---

## 六、新发现 Bug（本次审计，未修复）

### Bug G — `DEXArbitrageStrategy._pair_key` 排序可能掩盖不对称价差（低风险，注意事项）
**文件：** `strategies/arbitrage/dex_arbitrage.py`
**严重性：** 低

**现象：**
`_pair_key` 对代币名排序，使 `("ETH", "USDC")` 与 `("USDC", "ETH")` 产生相同 key。`find_arbitrage_opportunities(token_a="ETH", token_b="USDC")` 记录的是 ETH→USDC 方向的最优 profit_pct（买入方向特定），而 `find_arbitrage_opportunities("USDC", "ETH")` 的结果会覆盖同一 key 中不同方向的观察。

**影响：** 若两个方向的套利（ETH→USDC vs USDC→ETH）各自存在机会且分别被异步调用，后调用会覆盖前调用的观察，`check_exit` 可能读取到不属于当前持仓方向的价差数据。

**建议修复：** 在 `_pair_key` 中保留方向性（不排序），或者在 `_last_spread_obs` key 中加入方向标记：
```python
@staticmethod
def _pair_key(token_a: Any, token_b: Any, directional: bool = False) -> str:
    a = str(token_a or "").strip().upper()
    b = str(token_b or "").strip().upper()
    if not directional:
        a, b = sorted([a, b])
    return f"{a}/{b}"
```

目前 `check_exit` 同样使用排序 key，因此行为是一致的（不会有比较方向混乱），但两侧可能读到不完全匹配的价差观察。实盘中若同时运行两个方向的 DEX 套利持仓，应予关注。

---

### Bug H — `test_strategy_mode_isolation.py` 等 61 个测试存在 import-time 模块级状态污染（预存，建议修复）
**文件：** `tests/test_strategy_mode_isolation.py`, `tests/test_order_manager_safety.py` 等
**严重性：** 中（阻碍全量 CI 跑出干净结果）

**根因：** `core/trading/order_manager.py`、`web/api/trading.py` 等模块在 import 时创建模块级单例（如 `_order_manager`, `_risk_manager`），多个 test 文件修改这些单例后未清理，后续 test 观测到脏状态。

**建议修复方案（分批）：**
1. 在 `conftest.py` 中增加 `autouse=True` fixture，在每个测试后 reset 关键模块级状态（`_order_manager = None` 等）
2. 或在各受影响 test 文件顶部增加 `@pytest.fixture(autouse=True)` 做 monkeypatch 隔离
3. 中期目标：将模块级单例改为 FastAPI `Depends()` 注入，消除全局状态

---

### Bug I — `web/api/data.py` 使用 naive `datetime.now()` 进行时间范围计算（低风险，持续积压）
**文件：** `web/api/data.py:1062, 3692, 3705, 3972, 4490, 6485`
**严重性：** 低

**现象：** 多处 `datetime.now()` 无时区（naive），与数据库中存储的 UTC-aware 时间戳混合使用，可能导致时区偏移 8h 的查询范围错误（特别是 UTC+8 用户本地环境）。

**建议修复：** 将所有 `datetime.now()` 替换为 `datetime.now(timezone.utc)`。优先修复数据查询路径（3692, 3705, 4490）。

---

### Bug J — `web/api/strategies.py` 注册接口 suffix 使用 naive datetime
**文件：** `web/api/strategies.py:1817`
**严重性：** 低（仅影响 suffix 格式，不影响功能）

```python
suffix = datetime.now().strftime("%m%d%H%M")
```
在 UTC+8 时区，注册策略时 suffix 会偏移 8 小时（如 UTC 16:00 → suffix 显示 "0100" 而非 "0000"）。建议 `datetime.now(timezone.utc)`。

---

## 七、积压项状态更新

| 编号 | 问题 | 本次状态 | 紧迫度 |
|---|---|---|---|
| P4-Parquet | Parquet UTC 迁移 `--apply` 未执行 | ⚠️ **持续未执行** | 高 |
| C3-aiosqlite | 全量测试退出时 `RuntimeError: Event loop is closed` | ⚠️ 未处理 | 低 |
| WS L3 | Level-3 ui_primary 6h selfcheck（预计 2026-06-10 22:56 结束） | ⚠️ **需确认结果** | 高 |
| 隔离测试 | 61 个全量套件隔离失败 | ⚠️ 预存，建议 P1 批量修复 | 中 |
| Bug G | DEX `_pair_key` 方向性模糊 | ⚠️ 新发现，低风险 | 低 |
| Bug H | 测试隔离 conftest cleanup | ⚠️ 预存 | 中 |
| Bug I | `data.py` naive datetime × 6 处 | ⚠️ 预存 | 低 |
| Bug J | `strategies.py:1817` naive datetime | ⚠️ 新发现 | 低 |

---

## 八、信号完整性更新

| 策略 | BUY | SELL | CLOSE_LONG | CLOSE_SHORT | check_exit |
|---|---|---|---|---|---|
| CEXArbitrageStrategy | ✅ | ✅ | ✅ | ✅ | ✅ **已修复** |
| TriangularArbitrageStrategy | ✅ | ✅ | ✅ | ✅ | ✅ **已修复** |
| DEXArbitrageStrategy | ✅ | ✅ | ✅ | ✅ | ✅ **已修复** |
| HurstExponentStrategy | ✅ | ✅ | — | — | — |
| SupplyEventStrategy | ✅ | ✅ | — | — | ✅ |
| MaxDrawdownStrategy | ✅ | ✅ | — | — | — |
| FamaFactorArbitrageStrategy | ✅ | — | — | — | — （注：仅产 BUY 信号） |
| MeanReversionHalfLife | ✅ | ✅ | ✅ | ✅ | ✅ |

---

## 九、行动项（按优先级）

### 立即（已就绪，只需 commit）
1. **提交本轮所有变更**：
   ```bash
   git add strategies/arbitrage/cex_arbitrage.py \
           strategies/arbitrage/dex_arbitrage.py \
           web/api/backtest.py \
           tests/test_arbitrage_check_exit.py \
           tests/test_macro_workbench_and_premium_status.py \
           pytest.ini \
           docs/CODE_AUDIT_2026-06-10.md \
           docs/MARKET_DATA_WS_UPGRADE_PLAN_2026-05-29.md \
           docs/CODE_AUDIT_2026-06-11.md
   git commit -m "Fix arbitrage check_exit, backtest export, and test collection"
   ```

### 高优先级
2. **确认 Level-3 ui_primary selfcheck 结果**：查 `logs/ui_primary_selfcheck_20260610_165647.out.json`
3. **执行 Parquet UTC 迁移**（持续积压 P4）：
   ```bash
   python scripts/migrate_parquet_klines_to_utc.py          # dry-run
   python scripts/migrate_parquet_klines_to_utc.py --apply  # 执行
   ```

### 中优先级
4. **批量修复测试隔离（Bug H）**：在 `conftest.py` 或各受影响测试文件中增加模块状态 reset fixture，使全量套件从 61 failure → 0 failure
5. **修复 `data.py` naive datetime（Bug I）**：6 处时区修复，重点是查询范围计算路径

### 低优先级
6. **考虑 DEX `_pair_key` 方向性（Bug G）**：若实盘有双向 DEX 套利持仓需求，增加 `directional` 参数
7. **修复 `strategies.py:1817` naive datetime（Bug J）**：suffix 时区一致性

---

## 十、总结

本次审计完成了 2026-06-10 遗留的主要 Bug C 修复提交（套利 check_exit）、回测导出 Bug F1/F2、以及测试基础设施修复（Bug A）。

**新增通过测试：+30**（24 arbitrage_check_exit + 6 macro_workbench）
**全量套件：1923 passed / 61 failed（全部预存隔离失败）**
**无新 Critical 级 Bug 发现**

最紧要的即时动作是 **commit 工作树并确认 Level-3 ui_primary selfcheck 结果**。

---

*报告由自动化审计任务生成 — 2026-06-11*
