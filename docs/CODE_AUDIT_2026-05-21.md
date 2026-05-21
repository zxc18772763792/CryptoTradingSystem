# 每日代码审计报告 — 2026-05-21

> 自动化运行 · 覆盖提交 `3f8cdeb`（HEAD）至 `589e897`（5 次提交，约 2026-05-20 当天所有改动）
> 测试基线：修复前 **4 失败**，修复后 **0 失败**（全量通过）

---

## 一、已完成修复（本轮）

### Fix 1 — 4 个测试因行为变更 / 日期过期而失败

#### 1a. `tests/test_openai_target_helpers.py` — source 格式变更

**问题**：`_summary_source_with_model`（`llm_glm5.py:350`）会在 source 后追加模型名称（如 `openai_responses:gpt-5.5-mini`），但以下两个测试仍断言旧格式 `"openai_responses"`：
- `test_news_failover_uses_per_source_models` 第 139 行
- `test_news_failover_supports_anthropic_style_backup` 第 193 行

**修复**：更新断言为：
```python
assert result["source"] == "openai_responses:gpt-5.5-mini"
assert result["source"] == "openai_responses:claude-compatible-model"
```

#### 1b. `tests/web/test_trading_pnl_heatmap_route.py` — 硬编码日期已过 30 天

**问题**：`test_get_pnl_heatmap_normalizes_mixed_timestamp_awareness` 和 `test_get_pnl_heatmap_separates_paper_and_live_fallback_orders` 使用 `datetime(2026, 4, 20, ...)` 作为测试数据。`_iter_trade_records` 有 30 天过滤（`ts.timestamp() < cutoff_ts`），2026-04-20 现在超出窗口 → `trade_count=0`、`display_mode='empty'`。

**修复**：改用 `datetime.now(timezone.utc) - timedelta(days=N)` 动态时间，确保与当前系统时间保持相对偏移，避免日期腐烂。

### Fix 2 — `pytest.ini` Windows 临时目录积累阻断测试

**问题**：`pytest.ini` 使用固定 `--basetemp` 目录，且缺少保留策略配置。在 Windows 上，若某测试（`test_premium_data_status_reports_cached_fred_macro` 使用 `monkeypatch.chdir`）的 SQLite/日志文件句柄未在测试结束后关闭，下次运行 pytest 时清理该目录会遭 `PermissionError [WinError 32]`，导致所有使用 `tmp_path` 的测试报 ERROR（1148 个）。

**修复**：在 `pytest.ini` `[pytest]` 节下添加：
```ini
tmp_path_retention_count = 0
tmp_path_retention_policy = none
```
每次测试结束立即清理，不积累跨 session 的残留目录。

---

## 二、新增代码审查（近 5 次提交）

### 2.1 `core/backtest/execution_arrays.py` — NumPy 快速路径

- **设计合理**：`is_supported_config` 守卫保守，只支持 signal_reversal_exit 纯信号跟随场景；任何止损/止盈/trailing 均降级到可信引擎。
- **逻辑正确**：precompute `long_entry_signal`/`short_entry_signal` 再在循环内使用（`le`/`se`），同 bar 反转→新开不会引入多余收益。
- **潜在问题（轻微）**：当 `active_direction = 0` 且 `long_entry_signal[idx]` 和 `short_entry_signal[idx]` 都为 True（理论上不可能，因为 `raw_signed` 不能同时 > 0 且 < 0），代码会保持 flat。已有 parity 测试覆盖（20 个通过）。

### 2.2 `strategies/quantitative/multi_factor_hf_fast.py` — MFH 批量快速路径

- **已通过完整 parity 测试**（`tests/test_multi_factor_hf_parity.py`）。
- **关注点**：`_batch_zscore` 对稀疏 NaN 序列做 `dropna` + `rolling` 后 `reindex` + `ffill`，会将最近的 non-NaN z 值向前填充，与 per-bar 的"取 dropna().tail()[-1]"语义等价——但若数据源频繁产生 NaN，ffill 可能使策略用陈旧信号，需监控 NaN 率。

### 2.3 `core/structural/` — 衍生品拥挤 + 供应事件结构层

- **设计健壮**：`clamp` / `clip_z` / `safe_float` 广泛使用，无 division-by-zero 暴露。
- **待验证逻辑**：`calculate_crowding_scores` 中 `oi_change_z` 同等贡献于 `crowded_long` 和 `crowded_short`（两者均用 `clip_z(oi_change_z)`，而非取正负方向）。OI 增加无方向信息时这可能合理，但若数据源已提供方向性 OI，则信号会被稀释——建议在上线实盘前通过历史数据验证效果。
- **降级覆盖完善**：`DerivativesContext.degraded` / `OnChainContext.freshness_hours` 在数据缺失时返回中性决策，无 fail-open 风险。

### 2.4 `core/execution/order_intent_router.py` — 新订单路由骨架

- **当前状态**：`submit_intent` 存在 `# TODO: add rate-limit policy + state machine hooks`（第 60 行）。
- **风险**：若在 state-machine 集成完成前就将该 router 接入实盘路径，限流和状态追踪缺失，可能造成重复下单或无限速下单。
- **建议**：在 `router_enabled = False` 特性开关就绪前不应在生产路径中调用 `submit_intent`；已有 `supports_execution` 守卫，但需在更高层确认该 flag 不会误为 True。

### 2.5 `core/exchange_adapters/ccxt_adapter.py` — 执行方法未实现

- **三个 `NotImplementedError`**（第 219/222/225 行）：`create_order`、`cancel_order`、`fetch_order`。
- **当前无影响**：测试通过，主路径仍走老 execution_engine；但若与 `OrderIntentRouter` 联动前未补充实现，会在运行时报错。
- **建议**：在 adapter 文档 / CLAUDE.md 明确标注，避免误用。

### 2.6 `core/trading/execution_engine.py` — 实盘成本默认填充

- **新增 `_with_live_trade_review_cost_defaults`**（第 1246 行）：只对 `mode = "live"` 的记录按 `LIVE_FEE_RATE`/`LIVE_SLIPPAGE_BPS` 推断缺失成本。逻辑清晰，有测试覆盖（`tests/test_execution_engine_fill_accounting.py`，19 个通过）。
- **潜在问题**：`_live_fee_backfill_cache` 用 `len > 500` 驱逐前 100 个插入键（FIFO），并非 LRU。如果某些键频繁访问，被驱逐后会重新拉取，导致额外网络调用。低频场景不影响正确性，但高频场景可优化为 `functools.lru_cache` 或 `OrderedDict` LRU。

---

## 三、已知未实施计划（高优先级积压）

以下为文档已列明、代码中尚未落地的结构性改进，**不影响当前系统运行，但会持续削弱盈利可靠性**：

| 编号 | 计划 | 文档 | 紧迫度 |
|---|---|---|---|
| P1 | **退出信号补全**：27/37 策略只发 BUY/SELL，无 CLOSE_LONG/CLOSE_SHORT；因子策略全部无退出信号 | `EXIT_LOGIC_OVERHAUL_PLAN_2026-05-20.md` A1-A3 | 极高 |
| P2 | **回测引擎补 SL/TP 触发**：`backtest_engine._execute_buy` 完全无视 `signal.stop_loss` / `signal.take_profit` | `EXIT_LOGIC_OVERHAUL_PLAN_2026-05-20.md` D1-D3 | 极高 |
| P3 | **VWAPReversionStrategy 退出信号修复**：均值回归完成发 `SELL` 而非 `CLOSE_LONG`，可能被解释为做空 | `EXIT_LOGIC_OVERHAUL_PLAN_2026-05-20.md` A7 | 高 |
| P4 | **数据 UTC 迁移脚本**（存量 parquet）：`scripts/migrate_parquet_klines_to_utc.py` 未执行 | `STABILITY_PROFITABILITY_IMPROVEMENT_PLAN_2026-05-20.md` Phase 1.2 | 高 |
| P5 | **F401 未使用 import 清理**：269 处，自动删除有回归风险，需人工排期 | `CODE_AUDIT_2026-05-19.md` | 中 |
| P6 | **AI 研究 Phase A-F**（实时信号、快速注册、CUSUM 自动草案等）| `CLAUDE.md` | 中 |

---

## 四、测试基线

```
修复前: 4 failed, 1148 errors (Windows tmp dir lock)
修复后: 0 failed, 0 errors
```

**关键文件修改摘要**：
- `tests/test_openai_target_helpers.py`: 2 处断言更新（source 格式）
- `tests/web/test_trading_pnl_heatmap_route.py`: 2 处动态日期（`timedelta` 替换硬编码）+ `timedelta` import
- `pytest.ini`: 添加 `tmp_path_retention_count = 0` / `tmp_path_retention_policy = none`

---

## 五、建议行动项（按优先级）

1. **立即**：执行 `VWAPReversionStrategy` 退出信号修复（P3），现已有日志证据"均值回归触发却开了空仓"，实盘风险。
2. **本周**：开始 Phase 0（退出原因基线脚本）+ Phase 1（回测补 SL/TP）——不做这两步，回测参数优化无意义。
3. **本周**：补充 `OrderIntentRouter.submit_intent` 限流 + 状态钩子，然后才能进行下游 adapter 集成测试。
4. **下周**：运行 parquet UTC 迁移脚本（确保 `--dry-run` 先检查），然后重跑回测一致性测试。
5. **排期中**：F401 未使用 import 逐模块人工审查，减少代码异味并降低未来静态分析噪声。
