# 全代码库审计汇总 (2026-05-21)

> 6 个并行 agent 深度审计，覆盖 13 个核心模块 (~18,686 行 / 150+ 文件)。
> 重点维度：Bug / 性能并发 / 死代码一致性。
> 安全维度未单独立项，但相关问题（如临时修改共享 client.options 的并发污染）已并入 Bug 项。

---

## 总览

| # | 模块 | 报告 | H / M / L |
|---|---|---|---|
| 1 | 策略 (`core/strategies/`, `strategies/*`) | [01_strategies.md](01_strategies.md) | 8 / 5 / 8 |
| 2 | 回测 + 会计 (`core/backtest/`, `core/accounting/`, validation_gate) | [02_backtest.md](02_backtest.md) | 7 / 5 / 6 |
| 3 | 新闻管道 (`core/news/`, `core/data/news_collector.py`) | [03_news.md](03_news.md) | 10 / 7 / 8 |
| 4 | AI 研究 + 监控 (`core/research/`, `core/ai/`, `core/monitoring/`) | [04_research_ai.md](04_research_ai.md) | 8 / 6 / 8 |
| 5 | 交易所/执行/风控 (`core/exchanges/`, `execution/`, `trading/`, `risk/`) | [05_exchanges_trading.md](05_exchanges_trading.md) | 7 / 6 / 8 |
| 6 | Web (`web/api/`, `web/static/`, `web/main.py`) | [06_web.md](06_web.md) | 7 / 6 / 9 |
| **合计** | — | — | **47 / 35 / 47 = 129** |

---

## 🔴 必须立即修复 (P0) — 跨模块全局排序

按"对资金安全/交易系统正确性的影响"排序，与单个模块的严重程度无关。

### 资金 / 订单 / 持仓正确性（最高风险）

1. **PnL 多空翻转时超额数量被静默丢弃** — [pnl_decomposer.py:113-118,204-247](crypto_trading_system/core/accounting/pnl_decomposer.py) (报告 02 #B-002)
   - 一笔 SELL > 现有多头数量时，仅平多，**反向空头从未开**；`positions` 既无多也无空，净敞口低估。
2. **OKX/Bybit `create_order` 不做 lot/tick 精度对齐** — `core/exchanges/okx_connector.py:180`、bybit_connector 同位置 (报告 05 #H2)
   - 仅 Binance 快速路径做了精度格式化；OKX/Bybit 收到 -4014/-1111 直接抛出，订单丢失，持仓记录与交易所不符。
3. **OKX/Bybit 多个方法绕过 `_ensure_client()`** — `core/exchanges/okx_connector.py:133,198,208,216,224,254` (报告 05 #H3)
   - 连接断开后 `cancel_order`/`get_positions` 等直接 `AttributeError: NoneType`，**自动重连失效**。
4. **`risk_manager.record_trade()` 无并发锁** — `core/risk/risk_manager.py:723-738` (报告 05 #H5)
   - `_daily_trades += 1` 非原子，可能少计 → `max_daily_trades` 熔断失效。
5. **Binance `get_positions` 临时改 `client.options["defaultType"]` 无锁** — `core/exchanges/binance_connector.py:620-633` (报告 05 #H4)
   - 高并发下 `get_balance` 可能带错误 type 发出，返回错误余额 → 风控 equity 失真。
6. **PnL closed_trades 快照 qty=0 / avg_entry_price=0** — `core/accounting/pnl_decomposer.py:238-243` (报告 02 #B-003)
   - while 循环已清空 lots 后才取快照，关仓档案丢失仓位规模数据。
7. **`PositionManager._position_history` 无界增长 + 每次开/平仓强制 `persist(force=True)` 同步写盘** — `core/trading/position_manager.py:475,556,570` (报告 05 #M6)
   - 高频策略下磁盘 I/O 热点 + 内存无界。

### 验证 / 评分系统正确性（直接影响策略上线决策）

8. **DSR 峰度修正公式错误：`(kurtosis-1)/4` 应为 `(kurtosis-3)/4`** — `core/research/validation_gate.py:75` (报告 02 #B-001)
   - 正态返回下 adj 由 1.0 被虚高到 2.125，DSR 评分系统性高估，过拟合策略易过门控。Bailey & López de Prado 原公式用超额峰度。
9. **`BacktestEngine` Sharpe 硬编码 `sqrt(365)`** — `core/backtest/backtest_engine.py:788` (报告 02 #B-006)
   - 1h bar Sharpe 被低估约 4.9 倍；与 `strategy_research` 的 `_annual_factor()` 不一致，两个引擎结果无法对比。
10. **`strategy_monitor.py` 的 scipy IQR 分支是死代码，CUSUM 永远走 sample std** — `core/monitoring/strategy_monitor.py:83-91` (报告 04 #B-4)
    - 意图鲁棒估计，实际两次 `_iqr` 调用都没用结果且无条件 `raise ValueError`，fat-tail 序列下漏报真实衰减。
11. **相关性过滤用 `strategy` 名作 key 而非 `candidate_id`** — `core/research/orchestrator.py:964` (报告 04 #B-7)
    - 同策略不同参数候选互相覆盖 equity curve，相关性计算用错数据。
12. **`check_exit` 接收含当前 bar 的数据，潜在前视偏差** — `core/backtest/backtest_engine.py:187,419` (报告 02 #B-004)
    - 与 `generate_signals` (用 `iloc[:i]`) 不一致，回测 Sharpe 略高于实盘。

### 配置 / 热更新 / 状态机

13. **新闻 LLM timeout 被 `min(yaml_90, env_16)` 强制截断到 16s** — `core/news/service/worker.py:378-383` (报告 03 #H5)
    - 已修复的 90s YAML 配置完全失效，频繁超时/重试/状态混乱。
14. **`_event_processor_loop` 配置静态固化，运行时改 YAML 无效** — `core/news/service/worker.py:501` (报告 03 #H1)
15. **事件驱动 LLM 路径绕过 provider backoff 和任务表去重** — `core/news/service/worker.py:546-555` (报告 03 #H3, H10)
    - 同一新闻被轮询路径和事件路径双重处理；rate-limit 期间仍发请求加速封禁。
16. **服务重启后 `research_running/queued` 提案被强制改为 `rejected`** — `core/research/orchestrator.py:241` (报告 04 #B-6)
    - 应改为 `draft` 允许重试，否则用户提交的提案在重启时全军覆没。

### 并发 / 异步泄漏

17. **OKX/Bybit `connect()` 无 `asyncio.Lock`** — `core/exchanges/okx_connector.py:30`、bybit 同 (报告 05 #H1)
    - 多策略并发启动会泄漏 ccxt client 实例 + 文件描述符。
18. **`_send_cusum_notification` 在同步函数中 `asyncio.create_task()` 且不保存引用** — `core/monitoring/cusum_watcher.py:128-133` (报告 04 #B-8 + L-3)
    - GC 可能取消通知任务；某些路径下抛 `RuntimeError: no running event loop`。
19. **`asyncio.wait_for(gather(...))` 超时后内部 task 泄漏** — `core/research/orchestrator.py:1293-1296` (报告 04 #M-3)
    - GLM rationale 30s 超时后 3 个调用继续后台运行，占用连接配额。
20. **`_event_processor_running` 用裸 bool 标志，crash + 双重处理器竞态** — `core/news/service/worker.py:487-494` (报告 03 #H2)
21. **`_JsonRegistry.list()` / `_flush()` 锁外读写 `_cache`** — `core/research/experiment_registry.py:54,90` (报告 04 #B-1, B-2)
    - 高并发下序列化可能不一致 + Windows `os.replace` 在文件被 UI 持有时抛 `PermissionError` (B-3)。

### 阻塞调用污染事件循环

22. **`core/data/news_collector.py` 用同步 `feedparser.parse` 在 async 函数中** — `core/data/news_collector.py:99` (报告 03 #H7)
    - `asyncio.gather` 内每个协程实际串行阻塞，且与生产管道平行无集成 (L1)。
23. **`audit_logger.log` 阻塞 5 个 strategies 端点 + 4 个 trading_runtime 端点** — `web/api/strategies.py:2432,2434,2444...`、`web/api/trading_runtime.py:91,108,128,199` (报告 06 #1, #2)
    - 已修复 register/delete，但 start/stop/pause/update/risk_param/reset/confirm 还在直 await，可阻塞 5–30s。

### 前端信号 / 状态丢失

24. **`pendingLlmContext` 在 API 调用前清空** — `web/static/js/ai_research.js:4949` (报告 06 #3)
    - 网络失败时 AI 研究上下文丢失，用户需重新生成。
25. **轮询定时器在 404 后永不停止** — `web/static/js/ai_research.js:5431-5432` (报告 06 #5)
    - 提案删除后每 3s 无效请求累积。

### 工具/类库错误

26. **`ccxt_adapter._to_dt_ms` 阈值错误：`> 10^12` 把所有正常毫秒时间戳除以 1000** — `core/exchange_adapters/ccxt_adapter.py:22-31` (报告 05 #L8)
    - 当前正常毫秒戳约 1.7×10¹² 全被误判，所有时间记录倒退 ~2000 年。**虽列为 L 实际优先级很高，应尽快修。**

---

## 🟠 重点 P1（修复后系统稳态显著改善）

| 主题 | 涉及 | 报告 |
|---|---|---|
| O(n²) 因子策略 (`HurstExponent`/`VaRBreakout`/`SortinoRatio`/`CCI`) 用 `rolling.apply(lambda)` | `factor_strategies.py:1292,1390,1549,1724,1765` | 01 #3,4,5 |
| `_update_positions` 每 bar 调用 3 次；`microstructure_proxies` 每 trade 重算 | `backtest_engine.py:184-203`、`cost_models.py:37-66` | 02 #P-003, P-004 |
| OKX/Bybit/Gate 无余额缓存 + execution_engine 多交易所串行 equity 刷新 | `okx_connector.py:142`、`execution_engine.py:921` | 05 #M3, M4 |
| `get_risk_report()` 每信号被调 2-3 次，含 72h/168h 全量 equity 遍历 | `risk_manager.py:965-971` | 05 #M5 |
| `_data_maintenance_worker` 83 个 sync 任务全部顺序 await | `web/main.py:762-769` | 06 #9 |
| `CCXTExchangeAdapter` 用同步 ccxt + `to_thread`（应改用 `ccxt.async_support`） | `ccxt_adapter.py:71-95` | 05 #M1 |
| Signal/timestamp naive datetime 残留 (`SignalFilter`, `SignalCombiner`, `strategy_manager.get_status`) | `signal_generator.py:48-168`、`strategy_manager.py:1967` | 01 #1, #2 |
| `non-SQLite` 部署下 `save_news_raw` IntegrityError 触发 session rollback，后续行全丢 | `db.py:1170-1178` | 03 #H4 |
| `CUSUMMonitor.update()` 每 bar O(n) 重算 std（应改 Welford 在线算法） | `strategy_monitor.py:180-182` | 04 #B-5 |
| commission_rate=0.0004 单边过低，建议提升到 0.001 以贴近 Binance taker | `strategy_research.py:2086` | 02 #B-005 |

---

## 🟡 死代码 / 清理 / 一致性 (P2)

仅列影响较大的几条，详见各分模块报告：

- **孤儿前端文件**：`web/static/js/ai_research_candidates.js` (297 行)、`ai_research_patch.js` 均未被 `index.html` 引用 — 06 #14, #15
- **`core/data/news_collector.py` 整个文件**与 `core/news/` 管道完全平行未集成，两套情感分析/RSS/去重 — 03 #L1
- **`core/news/eventizer/llm_glm5.py` 与 `async_glm_client.py`** 大量代码重复（hints/prompt/validate/summary_cache），已出现"ts[:16] 只改一处"的 bug — 03 #L2
- **`stop_loss.py`** 的 `StopLossManager`/`TakeProfitManager` 与 `execution_engine` 双轨并存，文件无 deprecation 警告，可被误调用触发双重平仓 — 05 #L6
- **`signal_generator.py` 的 `SignalCombiner` 全模块无任何调用方**（孤儿） — 01 #16
- **`_run_walk_forward` 已被 `_run_purged_walk_forward` 取代**但保留 wrapper 且测试仍直接调用 — 02 #L-001
- **`emit_counter` 死逻辑**（`should_emit` 硬编码 True，计数无意义） — 06 #21
- **`_llm_provider()` 所有分支都返回 `"openai"`** — 03 #L3
- **`HurstExponentStrategy` 阈值参数化错误**（用 0.5 阈值但实际计算的是 variance ratio 该用 ~1.0） — 01 #B

---

## 已确认无回归的过往修复

✅ Funding 双重计算（backtest）— `_apply_funding_for_bar` 不再直接改 `_capital`
✅ `_funding_boundary` try/except
✅ `generate_signals` 用 `iloc[:i]` 排除当前 bar
✅ 30-min news dedup（秒级精度、impact_score+sentiment 联合判断）
✅ `get_llm_queue_stats` SQL GROUP BY
✅ per-provider backoff 实现
✅ `save_news_raw` `OR IGNORE`（仅 SQLite 路径）
✅ Symbol 提取限制 8
✅ Sentiment position-weighting 1.5x
✅ ai_research.js 9 组重复定义清理（当前 ~6603 行无新重复）
✅ Bollinger / RSI 等已修信号方向 + min_bars
✅ AroonStrategy 向量化、market_data 30s TTL 缓存
✅ `experiment_registry` 加 `threading.Lock` + 原子写
✅ HMAC 签名（由 ccxt 维护）、未启用 Binance hedging mode

---

## 推荐修复路线

**Week 1 — 数据/订单完整性**
- P0 #1-7: PnL FIFO 翻仓 + OKX/Bybit 精度对齐 + `_ensure_client` 修复 + `record_trade` 加锁 + `defaultType` 并发污染

**Week 2 — 评分系统与状态机**
- P0 #8-12: DSR 公式 / Sharpe 年化 / CUSUM IQR 死代码 / 相关性 key / check_exit 前视
- P0 #13-16: News 配置热更新 + LLM timeout / 重启状态恢复

**Week 3 — 并发与阻塞**
- P0 #17-23: OKX/Bybit 锁 / CUSUM `create_task` / `wait_for` 取消 / feedparser 同步 / audit_logger 阻塞 9 端点
- P0 #26: `ccxt_adapter` 时间戳阈值（虽列 L 实际 P0）

**Week 4 — 性能调优**
- P1 全部 10 项（向量化、缓存、并发刷新）

**Cleanup (滚动)**
- P2 死代码清理可与 PR 同步进行（不阻塞）

---

## 测试建议

新增以下测试覆盖最易回归点：
1. `tests/test_pnl_decomposer_flip.py` — 单笔翻仓覆盖反向开仓
2. `tests/test_validation_dsr_kurtosis.py` — 正态返回下 DSR 应回到 1.0
3. `tests/test_backtest_sharpe_annualization.py` — 1h/4h/1d 三种 bar 年化因子对比
4. `tests/test_okx_connector_reconnect.py` — 断开后 `cancel_order`/`get_positions` 应自动重连而非 AttributeError
5. `tests/test_strategy_monitor_robust_std.py` — fat-tail 序列的鲁棒 std 估计
6. `tests/test_correlation_filter_same_strategy_diff_params.py` — 同策略两套参数候选互不覆盖
7. `tests/test_experiment_registry_windows_concurrent.py` — Windows 上 `os.replace` PermissionError 重试
8. `tests/test_news_worker_config_hot_reload.py` — YAML LLM timeout 修改后下一轮生效
9. `tests/test_audit_logger_non_blocking.py` — strategies/trading 高频端点响应 < 50ms

---

*汇总人：Claude Opus 4.7 — 2026-05-21*
*原始审计由 6 个 Sonnet 4.6 agent 并行完成，总耗时约 9 分钟，覆盖 ~18,686 行代码。*
