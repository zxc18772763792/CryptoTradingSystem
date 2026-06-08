# 每日代码审计报告 — 2026-06-07

> 自动化运行 · HEAD: `a757303`（Fix WS shadow gate worst-age false-fire; relax Level-2 launcher gate）
> 前次基线：`CODE_AUDIT_2026-05-29.md`（1768 passed, 1 skipped）
> 本次测试结果：**1926 passed, 1 skipped**（+158 tests，主要来自 WS 相关新测试）
> 测试运行说明：全量测试成功，含 `test_macro_workbench_and_premium_status.py`（本轮已修复超时问题）

---

## 一、前次审计进度验证

| 项目 | 状态 | 说明 |
|---|---|---|
| `code_audit_2026-05-29` Bug 1-11（孤立导入、死变量、计时断言）| ✅ 已修复 | 代码中已无对应问题 |
| P1-DSR 高阶矩校正 | ✅ 已修复 | `validation_gate.py` 现在从 equity_curve_sample 估算 skew/kurt，n_trials 乘以优化轮次 |
| P1-Funding 双计 | ✅ 已修复 | `backtest_engine.py:877` 注释明确说明修复逻辑，close 阶段净 PnL 已剔除 funding |
| ADX +DM 就地修改 bug | ✅ 已修复 | `momentum.py` 委托给 `core.indicators.sma_adx`，中心化实现无 self-overwrite |
| RSI peak/trough 未来数据泄露 | ✅ 已修复 | `rsi_strategy.py:233` 使用非中心化算法，确认无 `rolling(center=True)` |
| VWAPReversionStrategy 缺 SHORT 分支 | ✅ 已修复 | `common_strategies.py:345-362` 已有 CLOSE_LONG + CLOSE_SHORT 分支 |
| P4 Parquet UTC 迁移 | ⚠️ 未执行 | 脚本已就绪，但仍未 `--apply` 执行 |
| P1-6 个策略缺退出信号 | ⚠️ 部分未修复 | 见下方详述 |

---

## 二、新发现的 Bug 与问题

### Bug 1 — `test_premium_data_status_reports_cached_fred_macro` 在全量测试时超时（已修复）
**文件：** `tests/test_macro_workbench_and_premium_status.py`
**严重性：** 中 → ✅ 已修复

**根因：**
测试使用 `asyncio.run()` 在同步函数中，在全量套件运行后 Windows IOCP event loop 存在残留 I/O，导致死锁。另外 `_build_sources_health_payload()` 在 news_db 层（`summarize_news_raw_coverage`, `list_source_states`, `get_llm_queue_stats`）未被 monkeypatch，实际尝试 aiosqlite 操作导致额外阻塞。

**修复内容：**
1. 将 `def test_...` 改为 `async def test_...`（利用 `asyncio_mode = auto`，由 pytest-asyncio 管理事件循环）
2. `asyncio.run(...)` 改为 `await ...`
3. 新增 `news_db.summarize_news_raw_coverage`, `news_db.list_source_states`, `news_db.get_llm_queue_stats` 的 `AsyncMock` 补丁

同时发现 `tests/test_historical_data_manager.py` 同样模式（3 个测试用 `asyncio.run()` 在 Windows IOCP 下有风险），一并迁移为 `async def`。

---

### Bug 2 — `BinancePerpWsClient.normalize_event` 仍为 TODO 存根（已修复）
**文件：** `core/marketdata/binance_perp_ws_client.py:42`
**严重性：** 高 → ✅ 已修复

```python
@staticmethod
def normalize_event(message: Dict) -> Dict:
    """Normalize Binance Futures raw WS events into the app's market shape."""
```

**修复内容：**
- 实现 combined-stream 解包与 `bookTicker`、`aggTrade`、`kline`、`depthUpdate`、`markPriceUpdate` 标准化。
- 输出统一包含 `exchange/type/event_type/symbol/raw_symbol/timestamp/timestamp_ms/raw`，并映射 `last/bid/ask/mark/index/funding_rate/next_funding_time` 等字段。
- 未识别事件返回 `type="unknown"` 的结构化载荷，不再裸透传原始消息。
- 新增 `tests/test_binance_perp_ws_client.py` 覆盖常见事件与 unknown fallback。

---

### Bug 3 — 部分策略仍缺退出信号（本轮已修复 3 个）
**文件：** `strategies/quantitative/momentum.py:59-88`, `strategies/macro/onchain_flow_regime.py`
**严重性：** 极高 → ⚠️ 部分已修复

`MomentumStrategy.generate_signals` 已补充动量从 ±threshold 回退时的 `CLOSE_LONG/CLOSE_SHORT`：

```python
# momentum.py:59 — 正动量突破 → BUY（新仓）
# momentum.py:75 — 负动量突破 → SELL（新仓）
# 缺失：
# elif prev_momentum >= threshold and current_momentum < threshold:
#     → CLOSE_LONG（持多头时动量减退应平仓）
# elif prev_momentum <= -threshold and current_momentum > -threshold:
#     → CLOSE_SHORT（持空头时动量回归应平仓）
```

`OnChainFlowRegimeStrategy` 已在 `trade_mode=true` 时补充 regime 反向 close：accumulation 先 `CLOSE_SHORT`，distribution 先 `CLOSE_LONG`，再保留原 optional BUY/SELL。

`LiquidationOICrowdingStrategy` 已在 crowded gate 明确阻断同侧新仓时发出风险降低 close：`block_new_longs` → `CLOSE_LONG`，`block_new_shorts` → `CLOSE_SHORT`。

**影响范围：**
- `LiquidationOICrowdingStrategy` — ✅ 已补 crowded gate close
- `MomentumStrategy` — ✅ 已补阈值回退 close
- `OnChainFlowRegimeStrategy` — ✅ 已补 regime 反向 close
- `cex_arbitrage` / `dex_arbitrage` / `supply_event_strategy` — 仍需逐项验证

---

### Bug 4 — `StochasticStrategy` 穿越条件缺少历史值判断
**文件：** `strategies/technical/common_strategies.py:144-145`（推测行号，需验证）
**严重性：** 中（绝大多数有效穿越被过滤掉）

原始问题（上次审计记录）：
```python
if cross_up and k_now <= oversold:  # 当 k_now 刚刚穿越，k_now 通常 > oversold
```

正确写法：
```python
if k_prev <= oversold and cross_up:  # 前一棒在超卖区，当棒向上穿越均线
```

---

### Bug 5 — WS feed 配置错误时 `_run_one_exchange` 静默退出（无重启）
**文件：** `web/main.py`（WS feed worker 新逻辑）
**严重性：** 中（如 `MARKET_WS_EXCHANGES` 配置了 ccxt.pro 不支持的交易所名，worker 直接退出，无法被 supervisor 重启）

**现象：** 新增的 `while not exchanges and not stop_event.is_set()` 循环在 exchange 上线后继续，但若 `_build_client` 返回 `None`（无效交易所），`_run_one_exchange` 退出后整个 feed 任务结束，不触发重启。

**建议修复：** 在 `_run_one_exchange` 顶部对 None 客户端抛异常或记录 critical 日志，触发外层 supervisor 重启。

---

### Bug 6 — `MeanReversionHalfLifeStrategy` 止盈价格方向错误
**文件：** `strategies/factor_based/factor_strategies.py:1180`
**严重性：** 高（买入信号发出后若 mean < current_price，止盈立即被触发）

上次审计记录的 bug，验证尚未修复：
```python
# 当 mean < current_price（即价格处于高位回归），BUY 做多
# take_profit=mean（mean 低于 current_price）→ 创建时即低于开仓价，会立即被触发为"已盈利"
```

正确做法：BUY 的 take_profit 应在 mean 之外（如 `current_price + (current_price - mean) * 1.5`），或改用 ATR 倍数。

---

### Bug 7 — `HurstExponentStrategy` 阈值与 VR 中性值错位
**文件：** `strategies/factor_based/factor_strategies.py:1305`
**严重性：** 中（mean-revert 分支永远不进入）

上次审计记录的 bug：
```python
# Hurst: 0.55 = 趋势；0.45 = 均值回归
# 但 VR (Variance Ratio) 中性 = 1.0：>1 趋势, <1 均值回归
# 当代码直接将 0.55/0.45 与 VR 值对比时，VR 通常 > 0.55，mean-revert 条件永远为 False
```

---

## 三、架构审查（最近 WS 相关 commits）

### 3.1 WS shadow gate 最差龄修复（HEAD commit）

**变更：** `selfcheck_market_ws_shadow.py:326-330` — `worst_degraded_oldest` 改用 `last_tick_age_ms`（被监视 feed 的新鲜度）而非 `hub.oldest_tick_age_ms`（全局最旧，可能包含未监视的陈旧 symbol）。

**评估：** 正确修复。旧逻辑在 24h 运行时对未监视的 ~21.8h 陈旧 symbol 误报，现在只检查被监视的活跃 feed 龄。

### 3.2 Level-2 live-shadow 启动门（`market_ws_live_shadow.ps1`）

新增 24h selfcheck 使用宽松参数（`--tolerate-transient`, `--max-degraded-samples 24` 等），完整性门保持严格。

**评估：** 合理。长时间运行中的瞬态降级不代表真正问题，放宽周期性健康检查容忍度，完整性（价格差值）门保持原严格度。

**潜在风险：** `--max-shadow-violation-delta 30` 允许 30 个违规样本，在高波动时期可能掩盖真实的价格差异问题。建议在 prod 日志中额外记录"达到最大容忍"事件。

### 3.3 `ccxt_pro_feed.py` — mark-price/funding 可选流

新增 `_mark_stream_worker` 订阅 ccxt.pro `watch_mark_prices`。

**评估：** 实现合理。使用 `CCXT_PRO_AVAILABLE` 守卫且带完整重连逻辑，与 ticker 流解耦。

**潜在风险：** 若交易所不支持 `watch_mark_prices`，会在每个重连周期打印 WARNING，可能淹没日志。建议在首次失败后降至 DEBUG 级别或完全禁用该交易所的 mark 流。

### 3.4 `core/backtest/paper_trading.py` — PaperTradingEngine._run_strategies 空实现

```python
async def _run_strategies(self) -> None:
    for strategy in self._strategies:
        if not strategy.is_running:
            continue
        try:
            pass  # ← 空实现，策略实际上从未被调用
        except Exception as e:
            logger.error(f"Strategy {strategy.name} error: {e}")
```

`add_strategy`/`start` 存在，但策略从不生成信号。PaperTradingEngine 似乎是历史遗留实现，当前系统通过 strategy_manager 驱动信号，paper 执行走 execution_engine。但这个空实现容易引起混淆——建议加注释说明此路径已废弃，或者抛出 `NotImplementedError`。

---

## 四、持续积压（继承自前次，更新状态）

| 编号 | 问题 | 状态 | 紧迫度 |
|---|---|---|---|
| P1-退出 | 6 个策略缺 CLOSE_LONG/CLOSE_SHORT（momentum, onchain_flow_regime, cex_arbitrage, dex_arbitrage, liquidation_oi_crowding，`supply_event_strategy` 需验证）| ⚠️ 部分遗留 | 极高 |
| P4-Parquet | `scripts/migrate_parquet_klines_to_utc.py --apply` 尚未执行 | ⚠️ 未处理 | 高 |
| C2-测试隔离 | `test_premium_data_status_reports_cached_fred_macro` 全量时超时 | ✅ 已修复 | 中 |
| C3-aiosqlite | 全量测试结束时 `RuntimeError: Event loop is closed` 警告 | ⚠️ 未处理 | 低 |
| C1-模块属性导出 | `trading_balances.py` 通过 `trading_api.strategy_manager` 访问 | ⚠️ 未处理 | 低 |
| TODO存根 | `BinancePerpWsClient.normalize_event` 仍为 pass-through | ⚠️ 新发现 | 高（若在用）|

---

## 五、建议行动项（按优先级）

### 已在本轮完成
- **修复 `test_premium_data_status_reports_cached_fred_macro` 测试超时**（`tests/test_macro_workbench_and_premium_status.py`）：改为 `async def` + `await`，补全 news_db 的 AsyncMock
- **修复 `test_historical_data_manager.py` asyncio.run() 风险**：3 个测试迁移为 `async def`
- **修复 `BinancePerpWSClient.normalize_event` TODO 存根**：新增 Binance raw WS 标准化与回归测试
- **补全 3 个策略退出信号**：`MomentumStrategy`、`OnChainFlowRegimeStrategy`、`LiquidationOICrowdingStrategy`

### 本周
1. **继续验证剩余退出信号（P1）**：
   - `cex_arbitrage` / `dex_arbitrage`：确认价差消失或成交失败时是否有 CLOSE_* 或等价撤退路径
   - `supply_event_strategy`：确认事件风险消退、priced-in 或窗口结束时是否有 CLOSE_* 或等价退出路径

2. **执行 Parquet UTC 迁移**（P4）：
   ```bash
   python scripts/migrate_parquet_klines_to_utc.py  # 先 dry-run 确认
   python scripts/migrate_parquet_klines_to_utc.py --apply
   ```

3. **修复 `test_premium_data_status_reports_cached_fred_macro`**：
   将 `asyncio.run(...)` 改为 `@pytest.mark.asyncio` + `await`，或在该测试前 reset 事件循环。

4. **已处理 `BinancePerpWsClient.normalize_event`**：
   本轮已实现映射逻辑并补测试；后续只需确认该 skeleton 是否仍应保留。

### 下周
5. **修复 `MeanReversionHalfLifeStrategy` 止盈方向错误**
6. **修复 `HurstExponentStrategy` VR 阈值错位**
7. **修复 `StochasticStrategy` 穿越条件**（`k_prev <= oversold and cross_up`）

### 持续
8. **生产告警**：2026-05-25 熔断器（5.11% 回撤）仍处触发状态，需人工确认后 `POST /ops/risk/circuit-breaker/reset`
9. **WS mark-price 流日志噪声**：若交易所不支持 `watch_mark_prices`，首次失败后降至 DEBUG

---

## 六、测试覆盖摘要

```
全量测试（含 test_macro_workbench_and_premium_status.py，本轮修复后）:
  1926 passed, 1 skipped
  时长：~6分钟（359s）

WS 新增测试（本轮):
  test_ccxt_pro_feed_markprice.py: 9 passed
  test_market_data_hub.py: 6 passed
  test_market_ws_shadow_selfcheck.py: 13 passed
  test_market_ws_live_shadow_precheck.py: 5 passed
  test_market_ws_shadow_report_eval.py: 11 passed
  test_market_ws_shadow_launcher_assets.py: 7 passed
  test_market_ws_api_price_paths.py: 9 passed
  test_market_data_ws_ui_assets.py: 8 passed
  test_perf_fixes_round2.py: 12 passed
  test_web_main_runtime_tasks.py: 6 passed
  test_openai_responses_migration.py: 3 passed
```

---

*报告由自动化审计任务生成 — 2026-06-07*
