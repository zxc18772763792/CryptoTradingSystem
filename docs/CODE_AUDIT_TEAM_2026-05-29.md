# 团队深度代码审计报告 — 2026-05-29（运行时稳定性 / 流畅度 / 实时性）

> 方法：7 个专项 agent 并行只读审计全部 ~22 万行生产代码（`core/`、`web/`、`strategies/`、`config/`、`prediction_markets/`、`scripts/`）。
> 重点：**不重复**每日 lint 审计（无用导入/死代码，见 `CODE_AUDIT_2026-05-29.md`），专攻影响**稳定性、流畅度、实时运行**的运行时缺陷。
> HEAD: `40d5ceb`。本报告为只读审计结论，未改动任何代码。

## 分工与各域健康度

| Agent | 范围 | 结论 |
|---|---|---|
| 运行时与执行核心 | `core/trading,execution,realtime,runtime,marketdata` | 热路径工程质量高；风险在单 worker 串行化 + 无界增长 |
| Web API 与生命周期 | `web/**` | 异步层工程质量**极高**；残留 `/readyz` 故障 + 同步 parquet |
| 策略与信号 | `core/strategies`,`strategies/**`,`indicators`,`factors_ts` | 实时正确性**良好**，无系统性前视；残留墙钟再平衡/未门控异步评估 |
| 数据与交易所 | `core/data,exchanges,exchange_adapters` | CEX 层稳健；**2 个 P0**（parquet）+ naive 时间戳簇 |
| 新闻管线 | `core/news/**` | worker 循环隔离良好；残留重试无上限/饿死 |
| AI 研究 | `core/ai,research,ml` | 崩溃安全/数值稳健；**1 个 P0**（重计算阻塞循环） |
| 风控/会计/运维 | `core/risk,accounting,monitoring,ops,backtest,governance,...` | 会计/回测正确；残留**风控静默放松**（最值得关注） |

**总体判断**：这是一套成熟、防御性良好的系统。没有发现"会丢钱的会计错误"或"误启 live"。最严重的问题集中在三条主线：**(1) 重 IO/重计算在事件循环上同步执行 → 冻结整个服务（含实盘）**；**(2) 多处无界增长 → 长跑内存/磁盘泄漏**；**(3) 风控阈值语义偏松 / 异常时 fail-open**。

---

## 修复状态（2026-05-29，已实施 + 测试）

> 分 4 批实施，每批 `py_compile` + 跑相关测试；最终广覆盖回归 **287 passed, 0 regressions**。

**已修复（3 P0 + 18 P1 + 若干 P2）：**
- **P0**：P0-1 研究计算 offload `to_thread`（6 处重调用）✅ · P0-2 二级回填 `to_thread`+原子写 ✅ · P0-3 `data_storage` 原子 parquet 写 ✅
- **P1-A**：A1 web `_save_df_to_parquet` `to_thread`+原子写 ✅ · A2 DEX web3 4 方法 `to_thread` ✅
- **P1-B**：B1 实盘订单终态淘汰（cap 5000）✅ · B2 信号队列有界(2000)+丢最旧 ✅ · B3 `DataCollector` deque(maxlen) ✅ · B4 `research_jobs` 有界(200 finished) ✅
- **P1-C**：C1 **已由 `get_realtime_price` 重构修复**（验证，无需改）✅ · C2 采集器 5 处 `wait_for` 超时 ✅ · C3 新闻限流重试封顶(attempt≥8→failed) ✅ · C4 claim 前排除退避 provider（`claim_llm_tasks` SQL + worker）✅
- **P1-D**：D1 OKX/Bybit ticker · D2 4 CEX 订单 · D3 资金费率 ×4 + 二级回填 klines —— 共 11 处 naive→tz-aware ✅
- **P1-E**：E1 每策略熔断改用分配资金分母 ✅ · E2 日停豁免加"无开仓"条件 ✅ · E3 熔断异常 fail-closed（新开仓）✅
- **P1-F**：F1 registry 唯一临时文件名(`pid+uuid`，防跨进程撕裂 JSON) ✅ · F2 `_replace_with_retry` 捕 `OSError` ✅ · F3 研究调度并发上限(2) ✅
- **P1-G**：G1 Fama 加载后丢未收盘末 bar（因子只用完成 K 线）✅
- **P1-H**：H1 `/readyz` 改 `async_session_maker`+`text` ✅
- **P2 附带**：`/api/status`+`/health` UTC 时间戳 · `_prime_live_equity` 任务保引用 · `get_collected_data` 返回 list 拷贝
- **P2（批次6，clean 文件，codex 暂停后补）**：Bollinger 带宽除零守卫(`middle.replace(0,nan)`) · MeanReversion `rolling_std` 除零守卫 + min_bars `+1` · OPS token `secrets.compare_digest` 常数时间比较 · `gate_counterfactuals.jsonl` 超 16MB 自动裁剪到末 1万行

**已修复合计：3 P0 + 21 P1（含 C1 验证免改）+ 若干 P2。**

**待修复（1 P1 + 多数 P2）：**
- **P1-G2** 异步策略未受完成 bar 门控 → 重复盘中评估 —— **暂缓**：正确修法需给 `fund_flow`/`market_sentiment`/`cex_arbitrage` 等加"连续/逐 bar"标记，而这些文件正处于 codex 的 WS 改造工作区中；在 `strategy_manager` 做一刀切门控会误伤需连续评估的套利策略。待 WS 改造合并后再做。
- 其余 P2（~35）见下文清单，多为健壮性/一致性，非稳定性阻塞项。

---

## P0 — 致命（会冻结服务或静默丢数据，应优先修）

### P0-1 研究计算在事件循环上同步运行，冻结整个服务（含实盘交易循环）
`core/research/strategy_research.py:2911-3140`（入口 `run_strategy_research`@2759）
`run_strategy_research` 声明为 `async def`，但核心 `frames × strategies` 嵌套循环全程**零 `await`**（唯一的 await 在循环前的数据加载）。默认参数（60 天、4 个 timeframe、~37 策略）下，经 LHS 优化 + 全量 + OOS + 5 折 purged WF，约执行 **1 万+ 次同步回测**（`_run_backtest_core` 是纯同步 `def`@2132）。
**影响**：FastAPI 单事件循环被占用数分钟；`/health`、WS、自治 agent、CUSUM/熔断 worker、订单处理全部停摆——实盘下会延迟风控反应。`web/api/backtest.py`、`web/api/ml.py` 已用 `asyncio.to_thread`，研究模块是唯一漏网者。
**修复**：将同步 sweep 主体（或每个 cell 的 `_run_backtest_core`/`_optimize_*`/`_run_purged_walk_forward` 块）包入 `await asyncio.to_thread(...)`；`progress_callback` 仅改 dict，线程安全。

### P0-2 二级回填在协程内同步读写 parquet，阻塞事件循环
`core/data/second_level_backfill.py:292-316`（`_save_parts`，由 `async def _run`@353 直接调用）
`pd.read_parquet(fp)` + `merged.to_parquet(fp)` 同步执行，无 `to_thread`。每个窗口的合并/写盘按完整磁盘 IO + zstd 时长阻塞整个事件循环（实盘下单、WS、健康看门狗）。
**修复**：`await asyncio.to_thread(_save_parts, ...)`，与 `data_storage.save_klines_to_parquet` 现有写法一致。

### P0-3 parquet 直写最终路径，读者可能读到撕裂文件 → 当日 K 线静默丢失
`core/data/data_storage.py:262`（`pq.write_table(table, str(part_path), ...)`）
直接写最终路径；`load_klines_from_parquet`@329 与 `second_level_backfill._save_parts` 并发读同一文件。写入中途崩溃或并发读到半截文件 → 该日 bar 消失（仅被 `_quarantine_corrupted_parquet` 反应式重命名隔离 = 静默丢数据）。
**修复**：写入 `part_path.with_suffix('.tmp')` 后 `os.replace()` 原子发布。

---

## P1 — 高（负载/长跑下的稳定性、延迟、风控风险）

### A. 事件循环阻塞（除上述 P0 外）
- **P1-A1** Web 端 K 线服务路径 `_save_df_to_parquet` 同步读写 parquet — `web/api/data.py:1298,1303`（热路径 3717/3741/3782/3796/1537/1557）。大历史文件合并时阻塞所有客户端。→ `to_thread`。
- **P1-A2** DEX 方法内同步 web3 RPC — `core/exchanges/dex_connectors.py:104,106,133-190`。`async def connect/get_token_info/get_quote` 内 `Web3(HTTPProvider).is_connected()` 与 `contract.functions.x().call()` 均阻塞；`connect()` 在启动 `gather` 期间冻结循环。→ `asyncio.to_thread` 或 `AsyncWeb3`。

### B. 无界增长 / 内存泄漏
- **P1-B1** 实盘订单永不淘汰 — `core/trading/order_manager.py:449,672,695,753` 入库，仅 `clear_paper_history`@938 按 `mode=="paper"` 清理 → live 订单永久累积 → 长跑 OOM。→ 终态订单上限淘汰 / 定期按时长剪枝。
- **P1-B2** 信号队列无界 — `core/trading/execution_engine.py:855`（`asyncio.Queue()` 无 `maxsize`）。信号风暴下内存增长 + 执行陈旧信号。→ `maxsize` + 溢出丢最旧并计数。
- **P1-B3** `DataCollector._collected_data` 无界增长 — `core/data/data_collector.py:257-259` 每轮 append；`clear_collected_data`/`get_collected_data`@326-333 **全代码库无调用**。→ `deque(maxlen=N)` 或直接移除该缓冲。
- **P1-B4** `research_jobs.json` 无界增长 — `core/research/orchestrator.py:105`（`_persist_research_jobs`），仅 `delete_proposal`@816 剪枝；完成/失败任务永久累积，每次状态变更 O(n) 全文件重写。→ 仅保留近 N 条 / N 天。

### C. 缺超时 / 重试风暴 / 饿死
- **P1-C1** 信号 worker 在 `get_ticker` 无超时时阻塞 → 止损/移动止损对**所有持仓**延迟触发 — `core/trading/execution_engine.py:1168`（`_resolve_price`，被 `_check_protective_orders`@6060、`_check_conditional_orders`@6125 复用，与 `execute_signal` 同一单 worker）。慢交易所 = 全仓保护单延迟到 ccxt 超时（10–30s）。→ `asyncio.wait_for(..., 3-5s)`，并考虑保护单单独任务。
- **P1-C2** 实时采集无 per-call 超时 — `core/data/data_collector.py:134,157,172`、`core/data/second_level_backfill.py:142`。半开 TCP 会挂起任务；backfill 内层循环只有 `max_loops` 无墙钟上限。→ `asyncio.wait_for(..., 20-45s)`。
- **P1-C3** 限流重试无最大次数上限 — `core/news/storage/db.py:1428-1433`。timeout 分支 `attempt>=5→failed`、other `attempt>=3→failed`，唯独 rate-limit 分支无终态 → 持续 429 的任务永久 `retry`、永占优先队列头。→ 加 `attempt>=8→failed`。
- **P1-C4** provider 退避项每轮被领取后退回，饿死健康 provider — `core/news/service/worker.py:437-465`。`claim_llm_tasks` 按 `priority DESC` 排序且**不感知** provider 退避，每轮先领高优先退避项、`attempt++`、再退回，耗尽批次预算。→ 在 `claim_llm_tasks` SQL 内排除退避中的 provider。

### D. naive（非 UTC）时间戳 —— 与 tz-aware 比较会 `TypeError`/偏移
- **P1-D1** OKX/Bybit ticker：`datetime.fromtimestamp(ts/1000)` 无 `tz`，且 `ts=0` 时回退到 1970 — `okx_connector.py:129`、`bybit_connector.py:128`。Binance/Gate 已正确。
- **P1-D2** 四个 CEX `_parse_order` 填充时间戳 naive — `binance_connector.py:704`、`gate_connector.py:364`、`okx_connector.py:346`、`bybit_connector.py:345`。下游 PnL/会计比较出错。
- **P1-D3** 资金费率事件时间戳 naive — `core/data/funding_rate_collector.py:112,155,195,417`（Binance/Gate 路径漏 `tz`，Bybit/OKX 正确）。喂入资金成本模型。
> 统一修复：`datetime.fromtimestamp(ts/1000, tz=timezone.utc) if ts else datetime.now(timezone.utc)`。

### E. 风控静默放松 / fail-open（**最值得关注 —— 涉及风控语义**）
- **P1-E1** 每策略熔断用**全账户权益**作分母 — `core/risk/circuit_breaker.py:846-851`（`_drawdown_from_pnl`@742）。`base_capital=_resolve_account_equity()`（全组合）→ 占 15% 资金的策略需亏掉**自身 ~33%** 才触发配置的 5% 阈值，实际松约 6 倍。→ 用该策略分配资金作分母，或按分配比例缩放阈值。
- **P1-E2** 实盘日停豁免**隔夜浮亏**（fail-open 至 2×）— `core/risk/risk_manager.py:644-670`。豁免条件 `live AND _daily_trades<=0 AND |daily_realized|<1e-9`；二者午夜归零但 `_current_unrealized_pnl` 不归零 → 昨开仓今放血被当作"外部权益变动"忽略，直到 2× 灾难兜底。→ 豁免再加 `position_manager.get_position_count()==0`。
- **P1-E3** 熔断闸门在评估异常时 fail-open — `core/trading/execution_engine.py:3674-3683`。`cb_decision=_cb.evaluate(...)` 异常时置 `None`，守卫 `if cb_decision is not None and not is_allow` → 异常即放行下单。→ 异常时对非 reduce-only 订单按 `close_only`/阻止处理。

### F. registry / 并发
- **P1-F1** 跨进程 registry 损坏 — `core/research/experiment_registry.py:54,72-93` + `core/ops/service/api.py:464`。每进程 `threading.RLock` + 共享 `proposals.tmp`→`os.replace`，无跨进程锁。若 ops 以独立进程运行（`standalone=True`@1061），两进程写同一 tmp/目标 → 交错写/丢更新/半截 JSON 启动失败。**进程内挂载（默认）安全**。→ tmp 名按 `pid+token` 唯一化 + `.lock` 跨进程锁。
- **P1-F2** `_replace_with_retry` 仅捕获 `PermissionError` — `core/research/experiment_registry.py:23-33`。Windows 上杀软/索引器瞬时锁可能是其他 `OSError` 子类，直接抛出中止 flush，可使 `_finalize_research_run` 失败、proposal 卡半态。→ 改捕 `OSError`。
- **P1-F3** 研究调度无并发上限 — `core/ai/research_scheduler.py:111-133`。`_tick` 对每个 `research_queued` 都 `run_proposal(background=True)`，无信号量；与 P0-1 叠加（每个研究冻结循环）使队列膨胀。→ 每 tick 上限 + `asyncio.Semaphore(1-2)`。

### G. 实时正确性
- **P1-G1** Fama 因子套利按**墙钟**而非 bar 时间再平衡，读未收盘 K 线 — `strategies/quantitative/fama_factor_arbitrage.py:281-290,348,374`。`_load_universe_frames` 不丢未收盘末 bar，因子分/多空篮按半成型 K 线计算。→ 丢未收盘末 bar + 按末**完成** bar 时间触发再平衡。
- **P1-G2** 异步策略未受"新完成 bar"门控 → 每 5–60s 重复评估/重复同向信号 — `core/strategies/strategy_manager.py:1170-1192`（`generate_signals_async` 分支无 sync 分支@1203 的 `_has_new_completed_bar` 守卫）。`fund_flow`/`market_sentiment` 等无自去重者反复触发 + 冗余网络抓取。→ 异步分支同样加完成 bar 门控。

### H. 健康探针
- **P1-H1** `/readyz` 永久返回 503 — `web/main.py:1958`。`from config.database import get_session` 名称错误（实际导出 `get_db_session`），`python -c` 已确认 `ImportError`，被捕获 → `overall_ready=False`。容器/编排器视服务永久 NotReady，可能重启循环 / 摘出轮转。→ 用 `get_db_session` 异步上下文 + `await session.execute(text("SELECT 1"))`。

---

## P2 — 中（正确性 / 健壮性，建议修）

**运行时/执行**
- `_prime_live_equity` fire-and-forget 丢引用（可被 GC 取消）— `execution_engine.py:6221`。
- 供给的 `client_order_id` 锁外 check-then-act 竞态 → 罕见重复下单 — `order_manager.py:514-522`。
- 实盘下单热路径 2 次同步写盘（journal + counts）— `execution_engine.py:735-736,415`。
- reduce-only 卡仓在 live 不可验证时无限重试、无告警升级 — `execution_engine.py:4965-4972`。
- 条件单无过期 → 累积 + 每 tick 定价 — `execution_engine.py:5217`。

**数据/交易所**
- 共享采集器 aiohttp `_get_session` check-then-create 竞态 → session 泄漏 — `orderbook_collector.py:170`、`funding_rate_collector.py:56`。
- 全局采集器单例（orderbook/funding/oi）`.close()` 无任何调用 → 关停时 session 泄漏 — `orderbook_collector.py:440` 等。
- `save_klines_to_db` 逐行 `flush()` → 1000 bar = 1000 次往返 — `data_storage.py:202-208`。
- 二级回填连续失败后跳整窗、仍报 `completed`（静默缺口）— `second_level_backfill.py:378-382`。

**策略/信号**
- 电平触发策略每周期重发同向信号（无边沿检测）— `ml_xgboost_strategy.py:79`、`market_sentiment.py:169`、`fund_flow.py:181`、`factor_strategies.py:1413`(VaRBreakout)。依赖执行层按持仓去重。
- parquet 直读的跨截面策略未丢未收盘末 bar — `intraday_cross_section.py:1242,752`（被 `_plan_is_fresh` + 粗再平衡缓解）。
- `MultiFactorHFStrategy` 信号双记入 history — `multi_factor_hf.py:320` + `strategy_manager.py:1048`。
- `AltcoinDowntrendBounceShort` 持仓登记前可重复入场 — `altcoin_downtrend_bounce_short.py:109,171`。
- `BollingerSqueezeStrategy` 带宽除以中轨无零守卫 — `bollinger_strategy.py:238`（fail-safe 但不一致）。
- `MeanReversionStrategy` min_bars 差一 + `rolling_std` 未 `replace(0,nan)` — `mean_reversion.py:47,104`。
- `SocialSentimentStrategy` 用采样钟而非 bar 时间戳 — `market_sentiment.py:362`。

**新闻**
- provider/global 退避仅进程内（重启/多进程丢失）→ 雷鸣群 — `db.py:104,1819 / 99,1770`。
- `OPENAI_FAILOVER_STATE_PATH` 开启时同步文件锁 + 读写在事件循环上 — `async_glm_client.py:616`→`core/utils/openai_responses.py:791`（默认关，潜在）。
- 30 分钟去重在 `no_anchor` 时可误删不同事件 — `db.py:1503-1512`。
- 30 分钟分桶使跨边界事件分裂（10:29/10:31 不去重）— `db.py:235`、`async_glm_client.py:1395`。
- 采集层与 DB 层两套 `_canonical_title` 可不一致 — `collectors/manager.py:86` vs `db.py:212`。
- `_persist_llm_title_summaries` 外层超时对大批次可达数千秒 — `worker.py:125-128`。

**AI 研究**
- 候选 `metadata` 在 finalize 锁外被别名突变（narrator/cusum 改同一缓存对象）— `orchestrator.py:1339,1448`、`cusum_watcher.py:79,112`（当前单线程安全，若 `save` 移入 `to_thread` 则可撕裂）。
- `ensure_ai_research_runtime_state` 首次初始化/恢复无锁（依赖单线程时序）— `orchestrator.py:441-464`。
- `CUSUMMonitor.update` 每 bar O(n log n) + 历史无界（**当前仅测试用**，潜在）— `strategy_monitor.py:199-203`。
- 储备激活先 transition 后 save，save 失败致 registry/生命周期分歧 — `cusum_watcher.py:389-400`。

**风控/运维/Web**
- live 切换单操作员确认，`REQUIRE_DUAL_APPROVAL_FOR_LIVE`(默认 True) 运行期未校验 — `web/services/trading_runtime_service.py:52-74,157`。
- `gate_counterfactuals.jsonl` 无界增长 + 全文件重写 — `core/audit/gate_counterfactuals.py:81,102`。
- OPS token 非常数时间比较 — `core/ops/service/auth.py:62`（localhost 绑定，低危）。→ `secrets.compare_digest`。
- CUSUM watcher 降级异常仅 `logger.debug` 吞掉；`_trade_history` 仅按名过滤无 mode 标签（paper/live 混淆）— `cusum_watcher.py:114,51`。
- AI-agent review/scorecard 端点每轮轮询全量同步读无限增长 JSONL — `web/api/ai_research.py:1884`、`execution_engine.py:764`。
- AI-agent GET 端点缺 `read_trading_state` 鉴权（暴露交易状态）— `web/api/ai_agent.py:21-109`。
- `/api/status`、`/health` 用 naive 时间戳（CN 主机 8h 偏移）— `web/main.py:1750,1803,2004`。

---

## 重复主线（修复时一并处理收益最大）

1. **重 IO/计算 → `asyncio.to_thread`**：P0-1、P0-2、P1-A1、P1-A2、以及 P2 的 journal 全量读。
2. **原子写**：P0-3（parquet）、P1-F1/F2（registry）。
3. **无界增长加上限**：P1-B1/B2/B3/B4、P2 gate_counterfactuals、journal 轮转、CUSUMMonitor。
4. **缺超时/重试封顶**：P1-C1/C2/C3/C4。
5. **naive → tz-aware UTC**：P1-D1/D2/D3、P2 `/api/status`、Social 时间戳。
6. **风控收紧/fail-closed**（需确认语义）：P1-E1/E2/E3、P2 dual-approval。

---

## 已验证健康（无需改动，节选）

- 任务生命周期：所有长循环经 `RuntimeTaskSupervisor` 强引用 + 指数退避 + `CancelledError` 正确处理；执行队列 worker、各监控 worker 均无 GC 静默死亡。
- WS `/ws`：accept 前鉴权、子任务 `finally` 必取消、event_bus 满队列丢最旧不丢订阅者。
- 轮询端点：普遍 TTL 缓存 + single-flight + stale-while-revalidate；回测/因子重计算已 `to_thread`；DB 访问统一 async。
- 会计/回测：funding 不再双计、SL/TP 按悲观 gap 成交、FIFO 撮合与 close-and-flip 正确、无前视。
- 数值安全：sizing `price<=0`/NaN/inf 守卫、杠杆 1–125 钳制；DSR/score/walk-forward/相关性过滤均有除零/空集守卫。
- 新闻：worker 循环每项/每批 try-except 隔离、卡任务 reclaim、原子 claim 防双取、aiohttp `async with` 无泄漏、采集器单源故障隔离。
- 安全：governance/ops 鉴权 fail-closed（未设 token→503）；managed 启动默认 paper 且阻止持久化 live 恢复；无明文 secret 日志；CORS 显式白名单。
- 时区：策略层 `bar_time` 归一、`_emit_signals` 冲突去重（age 钳 0–60s、naive/aware 混用守卫、退出单豁免、HOLD 排除）稳健。

---

# 附录 A — 各专项 Agent 完整审计原文（逐域，未删减）

> 以下为 7 个审计 agent 各自返回的完整报告原文（含代码引用、影响、修复建议与"已验证健康"清单）。汇总段（上文）是对其去重/合并后的版本；本附录保留全部细节。

## A1. 运行时与执行核心（audit-runtime-exec）

**范围**：`core/trading/`(execution_engine 6316 行、order_manager、position_manager、account_manager、account_snapshot、binance_rest)、`core/execution/`、`core/realtime/`、`core/runtime/`、`core/marketdata/`

### Health Summary
实盘交易热路径（`execution_engine.py`）整体工程质量高：多数交易所调用有超时、可重入 mode lock、有界的对账（含 absence-counting）、确认有界的 reduce-only 重试。任务生命周期稳固——所有长循环经 `RuntimeTaskSupervisor` 持强引用 + 退避，WS feed/event-bus 有正确背压。主要风险是**单 worker 串行化**（一个慢交易所调用同时拖住信号与保护性止损）、**无界的实盘订单内存泄漏**、**无界的信号队列**。

### Findings（best-first）

**[P1] 实盘模式下 `order_manager._orders` / `_order_meta` 无界增长** — `core/trading/order_manager.py:449,672,695,753`（插入）vs `:949-950`（唯一剪枝）。每个实盘订单永久存储；`clear_paper_history` 按 `mode=="paper"` 过滤（`:938`），故**实盘订单永不淘汰**。数周实盘后无界增长（每条含 `Order` + 完整 meta dict 含 `params`）。运行时影响：内存稳步攀升 → 长跑 OOM。修复：增加终态订单淘汰（上限 + 丢最旧 CLOSED/CANCELED/REJECTED，镜像 `OrderStateMachine.drop_terminal`），或不论模式按时长定期剪枝。

**[P1] 单信号队列 worker 在保护/条件检查里阻塞于无超时的 `get_ticker`** — `core/trading/execution_engine.py:1168`（`await connector.get_ticker(symbol)` 无 `asyncio.wait_for`）。由 `_check_protective_orders`(`:6060`) 与 `_check_conditional_orders`(`:6125`) 按符号调用，二者与 `execute_signal` 跑在**同一**单 worker（经 `_background_tick`）。`fetch_ticker` 仅受 ccxt client `timeout`(~10–30s) 约束。慢交易所因此会阻塞信号执行**以及所有持仓的**止损/移动止损触发，最长达该超时。运行时影响：venue 降级时保护性退出错过/延迟 + 信号延迟尖峰。修复：将 `_resolve_price` 的 ticker 抓取包入 `asyncio.wait_for(..., 3-5s)`；考虑用独立任务服务保护检查。

**[P1] 信号队列无界** — `core/trading/execution_engine.py:855`（`asyncio.Queue()` 无 `maxsize`）。单 worker 串行排空，每个 `execute_signal` 多次 await（治理、AI review、对账、网络）。信号风暴下（多策略×多符号）队列无界增长，信号被任意陈旧地执行。运行时影响：内存增长 + 按过期信号下单。修复：限制队列（如 `maxsize=1000`），溢出丢/拒最旧并计数，或按 (strategy,symbol) 合并。

**[P2] 启动期 fire-and-forget 任务丢引用** — `core/trading/execution_engine.py:6221` `asyncio.create_task(self._prime_live_equity(), ...)`——返回值未存，故可被 GC 取消（正是此前 DataCollector 修复处理的反模式）。它是一次性 30s 预热，影响低（被 GC 则权益可能未预热），但与代码库自身"pin task"约定不一致。修复：存到 `self._prime_task`（并可选在 `stop()` 中 await/取消）。

**[P2] 供给的 `client_order_id` 锁外 check-then-act 竞态** — `core/trading/order_manager.py:514-522`。`_is_client_order_id_active(...)` 然后 `self._client_order_ids[client_order_id] = time.monotonic()` 在**无** `self._client_order_lock` 下运行（仅 `_allocate_client_order_id` 持锁）。两个并发实盘下单复用同一供给 `clientOrderId` 可能都通过重复检查并都提交。运行时影响：caller 供给 id 重试时罕见双成交。修复：供给 ID 的 check+reserve 也在 `_client_order_lock` 下进行。

**[P2] 实盘下单热路径上的同步磁盘 I/O** — `core/trading/execution_engine.py:735-736`（journal append `self._live_trade_journal_path.open("a")…write`）与 `:415`（`_persist_live_trade_counts` `write_text`），二者都在 `_record_live_strategy_trade` 内、`_live_review_lock` 下。每笔实盘交易在事件循环上做 2 次阻塞文件写。小/原子，通常亚毫秒，但磁盘压力下会在下单中途阻塞循环。修复：经 `asyncio.to_thread` 卸载，或把 journal 写批量/移出热路径。

**[P2] `readyz` 探针在事件循环上发同步 DB 调用** — `web/main.py:1959-1960` `with get_session() as session: session.execute("SELECT 1")`。若 `get_session` 产出同步 SQLAlchemy session，则阻塞循环。频率低（健康探针）故较小，但繁忙 LB 猛打 `/readyz` 会增加循环停顿。修复：用异步 session/`text("SELECT 1")`，如 `account_snapshot.py` 所做。（注：与 web-api agent 的 P1-H1 合并——根因是 `get_session` 名称错误致 ImportError，更严重。）

**[P2] 实盘 reduce-only 失败留下自我延续的卡仓路径（设计如此，但值得加护栏）** — `core/trading/execution_engine.py:4965-4972`。重试**确实有界**（按 `fail_key` 计数，无内层循环），但**实盘**模式下当交易所验证不可用时，代码有意保持本地持仓打开，并在该符号每个未来平仓信号上重试 reduce-only——无限期，除日志外无告警升级。运行时影响：永久背离的本地持仓持续发 `-2022` 拒单且永不对账，而 venue 的 `get_positions` 处于降级。修复：K 次实盘失败后置健康/告警标志（或强制一次权威 `get_positions` 对账），而非静默无限重试。

**[P2] 条件手动单无过期** — `core/trading/execution_engine.py:5217`（`_conditional_orders[cid] = …`）；仅在触发(`:6166`)或手动取消时移除。永不触发的条件单永久存活，且每 tick 经 `_resolve_price` 定价（worker 上串行 ticker 抓取——`:6125`）。运行时影响：缓慢累积 + 若大量排队则每 tick ticker 扇出成本。修复：加 created-at TTL / 最大数量淘汰，并跳过离触发价很远的定价。

### Verified OK（已检查且稳健）
- **任务 pinning / GC 死亡**：每个长循环（`runtime`、`market_ws_feed`、`exchange_watchdog`、news、analytics、monitors）经 `RuntimeTaskSupervisor.start_task` 创建并同时存于 `supervisor._tasks` 与 `app.state.<name>_task`——强引用（`web/main.py:1571-1582`）。执行队列 worker 存于 `self._queue_task`。持久循环无静默 GC 取消。
- **Supervisor 重启/退避**：指数退避 1→30s，`CancelledError` 正确重抛，仅在 `restart_on_failure` 且非停止时重启（`supervisor.py:60-76`）。
- **reduce-only 重试风暴**：按 `fail_key` 计数 + 阈值有界；无紧重试循环（`execution_engine.py:4942-5000`）。
- **持仓对账**：absence-counting、宽限窗、stale-key 清理防止过早/循环平仓；`get_positions` 经 `wait_for` 限于 7.5s（`:3424,3574-3580`）。
- **WS 行情 feed**：每交易所循环含指数退避重连、watch 与 stop_event 竞速、关停时关闭 clients、`_last_push_monotonic` 受交易所数有界（`ccxt_pro_feed.py`）。`ws_client.py`/`binance_perp_ws_client.py` 是显式 NotImplementedError 骨架——非实时。
- **event_bus**：有界的每订阅者队列(maxsize 300)，满则丢最旧，从不阻塞 publisher，保持订阅者存活；WS 端点在 `finally` 取消子任务+退订（`event_bus.py`、`main.py:1933-1941`）。
- **sizing 数值安全**：`_calculate_quantity` 守卫 `price<=0`(返 0)，`_safe_ratio`/`_safe_nonnegative_float` 拒 NaN/inf，杠杆钳 1–125（`execution_engine.py:2724`、`order_manager.py:130-157`）。
- **TWAP `asyncio.sleep`**：仅在 `execute_manual_order`（API 请求协程，5789 行），**非**队列 worker 路径；条件/worker 路径传 `algo_interval_sec=0` 并用非切片 `_execute_manual_order_single`。
- **权益刷新**：`wait_for` 超时（8.5s 钱包快照、1.6s 报价），45s 缓存 TTL，`_asset_unit_usd_cache` 受资产数有界（`:957-1066`）。
- **REST 签名请求**：每调用 `httpx.AsyncClient` 经 `async with`（无泄漏），显式超时，`-1021` 时钟偏移重试，错误体上抛（`binance_rest.py`）。
- **`OrderStateMachine`**：幂等、终态不可回退、单调 fills、`drop_terminal()` 可用、重放去重（`order_state_machine.py`）。
- **Mode lock**：按任务可重入、正确释放；mode 切换在单 worker 上串行（`execution_engine.py:226-388`）。

---

## A2. Web API 与生命周期（audit-web-api）

**范围**：`web/main.py`、`web/api/*.py`、`web/services/`

### Domain Health Summary
`web/` 异步/API 层在稳定性与流畅度上**工程质量极高**：lifespan 后台任务跑在 `RuntimeTaskSupervisor` 下，含 stop-event、restart-on-failure、有界关停；WS 端点细致取消子任务；几乎每个被轮询端点都有 TTL 缓存 + single-flight + stale-while-revalidate + 后台刷新；几乎所有外部/分析调用都有超时 + 取消；CPU 重的回测/因子计算经 `asyncio.to_thread` 卸载；DB 访问统一 async；敏感交易/模式切换/订单端点有强 RBAC。以下是越过该高标准后仍存的真实运行时缺口——尤以坏掉的就绪探针与事件循环上的同步 parquet/journal IO 为最。

### Findings（best-first）

**[P1] `/readyz` 永远返回 503——就绪探针坏掉** — `web/main.py:1958` — `from config.database import get_session` 在 `readyz_check` 内导入，但 `config.database` 导出的是 `get_db_session`(async)，没有 `get_session`。该导入抛 `ImportError`，于 1962 行被捕 → `checks["db"]="error: ..."`，`overall_ready=False`。已经 `python -c "from config.database import get_session"` 验证 → ImportError。**运行时影响**：任何容器/编排器/uptime 监控探测 `/readyz` 都见服务永久 NotReady，可能重启循环或摘出轮转。**修复**：用 `get_db_session` 作异步上下文 + `await session.execute(text("SELECT 1"))`（并导入 `text`），或经 `async_session_maker` ping。

**[P1] K线服务路径在事件循环上同步读+写 parquet** — `web/api/data.py:1298,1303`（`_save_df_to_parquet`）— `async def` 但对每个候选目录调用同步 `pd.read_parquet(existing_path)` 然后直接 `merged.to_parquet(target)`，无 `to_thread`。在热图表路径被调用 `data.py:3717,3741,3782,3796,1537,1557`，凡实时刷新合并新 bar 即触发。对比 `data_storage.load_klines_from_parquet` 正确用了 `asyncio.to_thread`(`core/data/data_storage.py:350`)。**运行时影响**：对大历史文件(1m/1s、365+ 天)，整文件读+concat+写阻塞单事件循环，每次图表端点持久化时停顿**所有**客户端的 API/WS 流量。**修复**：将 read/concat/write 体包入 `await asyncio.to_thread(...)`，如 data_storage 的 `_save_sync`。

**[P2] AI-agent review/scorecard 端点在循环上同步全量扫描无界 JSONL journal** — `web/api/ai_research.py:1884`(`read_text().splitlines()`) 与 `core/trading/execution_engine.py:764`(逐行全读)——二者均由被轮询的 GET `/api/ai/autonomous-agent/review` 与 `/scorecard` 可达（`web/api/ai_agent.py:81,86`；由 `ai_research_agent.js` 调用）。自治 agent journal(`autonomous_agent.py:5603`) 与实盘交易 journal(`execution_engine.py:735`) 是 append-only 无轮转。**运行时影响**：每次轮询在事件循环上读整个（持续增长）文件；尽管只需末 N 行，延迟与循环阻塞时间随实盘会话增长。**修复**：只读尾部（seek-from-end 或 `deque(maxlen=N)`）并经 `asyncio.to_thread` 卸载；轮转/限制 journal。

**[P2] AI-agent 读端点缺同类交易端点强制的 `read_trading_state` 闸门** — `web/api/ai_agent.py:21,34,47,76,81,86,95,100,109` — GET status/review/scorecard/journal/risk-status/runtime-config/symbol-ranking/live-signals 路由**无**鉴权依赖，而 `/api/status`(`web/main.py:1728`)、`/api/trading/stats`、`/api/trading/orders` 等都要求 `require_sensitive_ops_permissions("read_trading_state")`。这些 GET 暴露 agent 的完整交易 journal、盈亏、持仓、风险状态与运行配置。**运行时影响**：任何非 loopback 暴露下，这是与 API 其余部分不一致的交易状态信息泄露（loopback 仍由本地 UI 会话模型覆盖）。**修复**：给 agent GET 路由加 `dependencies=[Depends(require_sensitive_ops_permissions("read_trading_state"))]`。

**[P2] `/api/status` 与 `/health` 发出 naive(非 UTC) 时间戳** — `web/main.py:1750,1803,2004` — `"timestamp": datetime.now().isoformat()`（及 1803 回退、`/health` 2004），而项目标准是 `datetime.now(timezone.utc)`（其余处处用，如 `/livez`@1947、`/readyz`@1993）。**运行时影响**：将其与别处 UTC 时间戳混用的客户端/日志关联器得到等于主机 TZ 的静默偏移；本 CN 主机即仪表盘状态徽章时间 8h 偏移。**修复**：`datetime.now(timezone.utc)`。

### Verified OK
- **Lifespan 任务卫生**——所有后台 worker 经 `RuntimeTaskSupervisor`(`core/runtime/supervisor.py`)：每任务 stop-event、`restart_on_failure` 含上限退避、`CancelledError` 处理、`stop_all` 关停时 `wait_for` 有界(`web/main.py:1605-1633`)。无泄漏/未跟踪 lifespan 任务。
- **WebSocket `/ws`**(`web/main.py:1859-1941`)——accept 前鉴权；recv/send 子任务总在 `finally` 取消+await；pending 与 done 任务异常排空；`unsubscribe` 总运行。event bus 满队列丢最旧而不丢订阅者(`core/realtime/event_bus.py`)。
- **被轮询端点缓存/single-flight**——trading stats(`trading_runtime.py:195`)、microstructure/community/risk-dashboard/calendar(`trading.py`)、news health/DB snapshot(`news.py:596`)、factor-library/onchain/fama(`data.py`) 都用 TTL 缓存 + `asyncio.Lock`/single-flight task map + stale-while-revalidate；`_status` 缓存 1.5s。
- **超时纪律**——`_bounded_analytics_call`/`_await_optional_analytics_task`(`trading.py:1726-1758`) 将每个外部调用包入 `wait_for` 并**超时即取消**；市场 ticker 扇出每调用 `wait_for`(`main.py:442`)。
- **后台任务跟踪**——`_schedule_live_cache_refresh`/`_schedule_cache_refresh`/下载任务都 single-flight + `add_done_callback` 清理；下载信号量在 loop 变化时重绑(`data.py:1948`) 并限并发。
- **CPU 重活卸载**——backtest run/compare/optimize/export 与 `build_factor_library` 用 `asyncio.to_thread`；DB 访问统一 async(`async_session_maker`、async news DB helpers)。
- **敏感变更鉴权**——order create/cancel(`manage_orders` + live 需 `approve_live`)、mode switch/confirm(`require_sensitive_ops_auth` + `request_live`/`approve_live`)、risk params、backtest、AI-agent control 都有闸门；裸 `create_task` fire-and-forget(news.py:2011/2732/3232) 体内异常安全。

---

## A3. 策略与信号（audit-strategies）

**范围**：`core/strategies/`、`strategies/**`、`core/indicators/`、`core/factors_ts/`

### Domain Health Summary
策略域在**实时正确性上状态稳固**。`strategy_manager._emit_signals` 的核心冲突检测/去重逻辑稳健（age 钳 0–60s、naive/aware 混用守卫、退出方向豁免、HOLD 排除于冲突窗外），指标向量化良好且 NaN/零守卫一致，technical/factor 策略几乎普遍用因果 prev-vs-current 交叉检测 + `min_bars = period+1` 守卫。bar 驱动策略**无系统性前视**。主要残留风险是 (a) **parquet 直读的异步策略跳过未收盘末 bar 丢弃**并用墙钟再平衡门控，(b) **电平触发(非边沿)入场策略每周期重发同向信号**，依赖执行层按持仓去重。

### Findings（best-first）

**[P1] Fama 因子套利按墙钟而非 bar 时间再平衡 → 读未收盘 K 线** — `strategies/quantitative/fama_factor_arbitrage.py:281-290, 374-378` — `_is_rebalance_due` 按 `datetime.now(timezone.utc)` 相对 `rebalance_interval_minutes` 门控，而 `_load_universe_frames`(348 行) 经 `data_storage.load_klines_from_parquet` 加载且**不丢未收盘末 bar**，信号被打 `bar_ts = bar_time(close_df)` = 末(可能正在形成)bar。60 分钟间隔/1h timeframe 下，因子分与截面多空篮可能基于半成型 K 线计算。运行时影响：因子分/排名基于不完整数据 → 错误篮选与入场价(每次再平衡附近)。修复：加载后丢未收盘末 bar(镜像 `strategy_manager._drop_incomplete_last_bar`)，并按末**完成** bar 时间戳而非墙钟门控再平衡。

**[P1] 异步策略未受 `_has_new_completed_bar` 门控 → 重复盘中评估** — `core/strategies/strategy_manager.py:1170-1192` — `generate_signals_async` 分支每个调度 tick(`interval = max(5, min(tf//3, 60))`，即 5–60s) 运行，无新完成 bar 守卫，不同于 sync 分支(1203 行)。无自去重的策略(`fund_flow`、`market_sentiment`，及经合成 1 行 df 路由的 whale/social 变体)每 bar 重抓重评多次。运行时影响：重复同向信号 + 每 bar 冗余网络/orderbook 抓取；仅 `intraday_cross_section`(`_last_rebalance_key`) 与 `cex_arbitrage`/`fama`(cooldown/interval) 自去重。修复：给异步分支加同样的完成 bar/上次评估守卫，或要求每个异步策略带显式每 bar 去重 key。

**[P2] 电平触发入场每周期重发同向信号(无边沿检测)** — `strategies/ai/ml_xgboost_strategy.py:79-109`、`strategies/macro/market_sentiment.py:169-215`、`strategies/macro/fund_flow.py:181-220`、`strategies/factor_based/factor_strategies.py:1413-1444`(VaRBreakout) — 这些在*电平*条件成立时即发 `BUY`/`SELL`(`result.direction == "LONG"`、`fgi <= fear_threshold`、`current_ret >= var_threshold`)，而非转变时。60s 冲突窗只压制*反向*信号，故连续同向入场通过。运行时影响：条件持续时每 bar/周期重复入场信号——完全依赖执行引擎按现有持仓去重；若曾启用金字塔则超额加仓。修复：按符号跟踪上次发出方向(它们已存 `_regime_bias`/`_last_bias`)，仅状态变化时发出，或在策略内加"同向持仓存在时不新入场"守卫。

**[P2] parquet 直读的截面策略不丢未收盘末 bar** — `strategies/quantitative/intraday_cross_section.py:1242-1265, 752-798` — `_load_universe_frames` + `build_ohlcv_panels` 用原始 parquet 尾部，无未收盘 bar 丢弃。被 `_plan_is_fresh`(max-age) 与粗再平衡边界(24h/48h via `_is_rebalance_bar`)缓解，故影响远低于 Fama，但落在成型 bar 上的再平衡仍含该 bar 算因子。修复：算因子面板前，对面板 timeframe 丢未收盘末 bar。

**[P2] `MultiFactorHFStrategy` 每个信号双记入 history** — `strategies/quantitative/multi_factor_hf.py:320-321` 加 `core/strategies/strategy_manager.py:1048` — `generate_signals` 对每个发出信号调 `self.add_signal_to_history(s)`，然后 `_emit_signals` 再调 `strategy.add_signal_to_history(signal)`。运行时影响：该策略 `signals_history`/仪表盘信号数虚高且双倍 history-trim 压力(无交易重复，因执行分发仅在 manager)。修复：移除策略内 `add_signal_to_history` 循环，让 manager 拥有 history 插入(同其他策略)。

**[P2] `AltcoinDowntrendBounceShort` 可在持仓打开前发重复入场** — `strategies/quantitative/altcoin_downtrend_bounce_short.py:109-128, 171-177` — 入场是*电平*检查(`trend_down and bounce_extended and momentum_repaired`)由 `if symbol in self._active_entry_at` 守卫，但 `_active_entry_at[symbol]` 仅在 `check_exit` 内填充(175/177 行)，而后者仅在持仓存在时运行。在发 SELL 与异步执行实际开仓之间，下一 bar 的 `generate_signals` 可重发同一 SELL，因 `_active_entry_at` 仍空。运行时影响：在首笔成交登记前的 bar 上第二次做空入场。修复：在 `generate_signals` 发出时即 `_active_entry_at[symbol] = entry_at`，而非仅在 `check_exit`。

**[P2] `BollingerSqueezeStrategy` 带宽除以中轨无零守卫** — `strategies/technical/bollinger_strategy.py:238` — `bandwidth = (upper - lower) / middle`；若 `middle`(滚动均值) 为 0 则带宽 inf/NaN。下游 `was_squeezed = prev_bandwidth <= squeeze_threshold and ...` 对 inf 为 False，且突破数学另守 `current_middle != 0.0`(269 行)，故 fail safe(无信号)而非误发——但 `check_exit`(335-349 行)也读带宽，inf/NaN 仅事后过 `np.isfinite`。运行时影响：低(退化为无信号)，但与套件其余不一致。修复：除前 `middle.replace(0, np.nan)`，匹配 `BollingerMeanReversion`/factor 模式。

**[P2] `MeanReversionStrategy` min-bars 守卫比 prev-bar 访问少一根** — `strategies/quantitative/mean_reversion.py:47, 104` — `generate_signals`/`check_exit` 守卫 `len(data) < lookback_period` 但随后读 `z_score.iloc[-2]`；恰好 `lookback_period` 行时 z-score 仅末值非 NaN，故 `prev_z` 为 NaN。NaN 比较 fail safe(无假信号、无崩溃)，但别处约定是 `period+1`。且 `_calculate_z_score` 除以 `rolling_std` 无 `.replace(0, np.nan)`，故平窗产生 inf `current_z`。修复：守卫 `< lookback_period + 1` 并加 `rolling_std.replace(0, np.nan)`(姊妹 `MeanReversionHalfLifeStrategy` 已两者皆有)。

**[P2] `SocialSentimentStrategy` 用采样钟而非 bar 时间打信号时间戳** — `strategies/macro/market_sentiment.py:362` — 用 `timestamp = self._social_data.get("timestamp", ...)`，而 `FundFlowStrategy` 与 `MarketSentimentStrategy` 用 `self._bar_time(data, fallback=sample_ts)`。今功能等价(合成 1 行 df 无真实 bar 索引，`bar_time` 回退到墙钟)，但若曾喂真实 bar 帧会有别，且略微错位信号与冲突窗。修复：用 `self._bar_time(data, fallback=timestamp)` 与其他宏观策略一致。

### Verified OK（无需处理）
- **`_emit_signals` 冲突/去重逻辑**(`strategy_manager.py:1000-1046`)：age 钳 `0 <= age <= 60`，naive/aware 混用 `TypeError` 当作无关，`close_long`/`close_short` 退出豁免压制，HOLD 完全排除于冲突窗(988-994 行)。`_evict_stale_signal_conflicts`(1064-1093) 正确归一到 aware-UTC 并清除超 2× 窗的条目。
- **行情缓存 & 未收盘 bar 处理**(`strategy_manager.py:440-624`)：TTL 动态(`tf_seconds/6`，上限 30s，下限 1s)；`_drop_incomplete_last_bar`(452-474) 对 sync 路径正确移除正在形成的 K 线；缓存淘汰有界(超 2×TTL stale + LRU size cap 100)。
- **技术策略**(bollinger entry、rsi、macd、ma/ema、donchian、stochastic、adx、vwap-reversion)：均用因果 prev-vs-current 交叉、`min_bars = period+1`、`band_width<=0`/`replace(0,np.nan)` 守卫，信号时间戳用 `_bar_time(data)`。Donchian 正确 `.shift(1)` 其突破/退出位——无前视。
- **ADX helpers**(`core/indicators/adx.py`)：`_directional_components` 在掩码前缓存 `up_move`/`down_move`(历史自覆盖 bug 已修)；`wilder_adx` 与 `sma_adx` 都守卫 ATR/DI-sum 用 `replace(0, np.nan)`。
- **向量化滚动 helpers**(`core/indicators/rolling.py`)：`rolling_mad`/`rolling_var_quantile`/`rolling_sortino` 是正确 O(n) 替代，含 NaN/除零处理。
- **RSIDivergence 峰谷检测**(`rsi_strategy.py:233-276`)：显式非中心(仅在 `order` 根后续 bar 后确认)——因果、无未来泄漏。
- **截面因子面板**(`intraday_cross_section.py:801-937`)：`false_breakout_supply`、`break_count_balance` 等都 `.shift(1)` 其滚动高/低位；plan 新鲜度 + `_last_rebalance_key` 去重；next_bar 执行声明。
- **时间序列因子**(`core/factors_ts/impl.py`、`base.py`、`registry.py`)：均因果(`.shift()`、合理 `min_periods`、inf/0→NaN)；base 契约强制 point-in-time；`compute_factor` 每调用重建(无跨 bar 状态泄漏)。
- **结构策略**(`liquidation_oi_crowding.py`、`onchain_flow_regime.py`、`supply_event_strategy.py`)：对风险闸门发 `HOLD`(正确排除于冲突/执行)，`SupplyEventStrategy` 用 point-in-time `require_visible=True, as_of=timestamp` 事件可见性(无事件前视)。
- **`bar_time` helper**(`strategy_base.py:16-68`)：tz-aware UTC 归一含守卫的 CST→UTC 启发；`_finalize_generated_signals` ATR-止损注入对 BUY/SELL 方向正确。

---

## A4. 数据与交易所连接器（audit-data-exchanges）

**范围**：`core/data/**`、`core/exchanges/`、`core/exchange_adapters/`

### Domain Health Summary
CEX 连接层(binance/okx/gate/bybit) 与 `exchange_manager` 大体稳固——连接锁、重连时候选 client 交换、time-sync、有界余额/资金缓存都做得好，重的历史下载循环正确地用超时+退避有界。主要运行时风险集中在别处：二级回填与 DEX 路径里**同步 IO/web3 阻塞事件循环**、实时 `DataCollector` 里**无界内存缓冲**、实时采集循环里**无超时的 kline/trade 抓取**(超时/退避修复落在 `web/api/data.py` 但未落到采集器)、**parquet 撕裂写竞态**、以及一簇 **naive-datetime / epoch-1970 ticker 时间戳 bug**。

### Findings（best-first）

**[P0] 实时回填 runner 在事件循环上同步读写 parquet** — `core/data/second_level_backfill.py:292-316`(`_save_parts`) 由 `async def _run`(353 行) 直接调用——`pd.read_parquet(fp)` + `merged.to_parquet(fp)` 在协程内同步运行，无 `asyncio.to_thread`。每个窗口合并/写阻塞整个事件循环(实时订单路由、WS 处理、健康看门狗)，按完整磁盘 IO + zstd 时长(对可能很大的 1s 分区文件)。修复：将 `_save_parts` 包入 `await asyncio.to_thread(...)`，正如 `data_storage.save_klines_to_parquet` 已做。

**[P0] parquet 撕裂写——读者可在写入中看到截断分区** — `core/data/data_storage.py:262` 直接将 `pq.write_table(table, str(part_path), ...)` 写最终路径；`load_klines_from_parquet`(329 行，经 `asyncio.to_thread`) 与 `second_level_backfill._save_parts` 并发读同文件。写入中途崩溃或写入中并发读产出截断文件 → 当日 bar 消失(仅被 `_quarantine_corrupted_parquet` 反应式缓解，即*把分区重命名移走*=该日静默数据丢失)。修复：写到 `part_path.with_suffix('.tmp')` 然后 `os.replace()` 原子发布。

**[P1] `DataCollector._collected_data` 永久无界增长** — `core/data/data_collector.py:257-259` 每个循环迭代把每个采集的 kline/ticker/orderbook 加入 `self._collected_data[task_id]`；`clear_collected_data`/`get_collected_data`(326-333 行) 在 `web/` 或 `core/` 中**从未被调用**(grep 验证)。长跑进程上这是与 uptime × task 数 × 轮询率成正比的稳步内存泄漏。修复：限制每个缓冲(如 `deque(maxlen=N)`)或直接丢弃缓冲，因 callback 已消费数据。

**[P1] 实时 kline/ticker/trade 采集无 per-call 超时** — `core/data/data_collector.py:134`(`_collect_kline` 中 `exchange.get_klines(...)`)、`:157`(`get_ticker`)、`:172`(`get_order_book`)；及 `core/data/second_level_backfill.py:142`(`await fetch_trades(...)`)。不同于已加固的 `web/api/data.py` 路径(每个抓取包 `asyncio.wait_for`)，这些 await 仅靠 ccxt 内部 socket 超时。停滞 TCP 连接(代理打嗝、半开 socket)挂起任务——且 `second_level_backfill` 内层 `while since_ms < end_ms` 循环(140 行)无整体墙钟界，仅 `max_loops=20000`。修复：将每个连接器调用包入 `asyncio.wait_for(..., timeout=~20-45s)`。

**[P1] DEX `async def` 方法内同步 web3 RPC** — `core/exchanges/dex_connectors.py:104` `Web3(Web3.HTTPProvider(rpc_url))` 与 `.is_connected()`(106 行)，加 `contract.functions.<x>().call()` 在 133-135、149-153、172-190 行——全阻塞 HTTP，全在 `async def connect/get_token_info/get_quote` 内。任何 DEX 报价/余额调用按完整网络往返冻结事件循环(且 `connect()` 在启动 `gather` 期间做)。修复：经 `asyncio.to_thread` 路由，或用 `AsyncWeb3`/`AsyncHTTPProvider`。

**[P1] OKX & Bybit ticker 时间戳：naive + epoch-1970 回退** — `core/exchanges/okx_connector.py:129` 与 `core/exchanges/bybit_connector.py:128`：`timestamp=datetime.fromtimestamp(ticker.get("timestamp", 0) / 1000)`。一行两 bug：(a) 无 `tz=timezone.utc` → naive(违反项目标准；与 tz-aware Kline/Signal 时间戳混用在比较时抛 `TypeError`——正是 5-21 修复追的那类 bug)，(b) 当 ccxt 返回 `timestamp=None/0` 时产出 `1970-01-01` 而非 now。Binance/Gate 已正确(`...fromtimestamp(ts/1000, tz=timezone.utc) if ts else datetime.now(timezone.utc)`)。修复：照搬 Binance/Gate 守卫模式。

**[P1] 四个 CEX 连接器 `_parse_order` 填充时间戳 naive** — `binance_connector.py:704`、`gate_connector.py:364`、`okx_connector.py:346`、`bybit_connector.py:345`：`datetime.fromtimestamp(ccxt_order.get("timestamp", 0) / 1000)` 无 tz。订单/成交时间是 naive UTC-local；与 tz-aware `datetime.now(timezone.utc)` 比较的下游 PnL/会计会失配或抛错。修复：加 `tz=timezone.utc`。

**[P1] 资金费率事件时间戳 naive** — `core/data/funding_rate_collector.py:112`(`funding_time=datetime.fromtimestamp(latest["fundingTime"]/1000)`)、`:155`、`:195`(`next_funding_time`)、`:417`(Gate `funding_time`)。同文件 Bybit/OKX 路径正确传 `tz=timezone.utc`；Binance/Gate 路径没有——不一致，且 naive 值喂入资金成本模型。修复：加 `tz=timezone.utc`。

**[P2] 共享采集器里 aiohttp session check-then-create 竞态** — `core/data/orderbook/orderbook_collector.py:170-173`(`_get_session`) 与 `funding_rate_collector.py:56-60`：`if self._session is None or self._session.closed: self._session = aiohttp.ClientSession(...)`。全局单例(`orderbook_collector`、`funding_rate_collector`、`oi_collector`) 共享；并发 `fetch_all`/并行 `gather` 下，两协程可都过检查并各建一个 session——第一个被覆盖且**永不关闭**(handle/connector 泄漏)。修复：用 `asyncio.Lock` 守卫创建。

**[P2] 全局采集器单例永不关闭 → 关停时 session 泄漏** — `orderbook_collector`(`orderbook_collector.py:440`)、`funding_rate_collector`(`:583`)、`oi_collector`，加 `DataCollector` 惰性建的 `FundingRateCollector()`(`data_collector.py:208`)——无一在任何 lifespan/关停路径被 `.close()`(grep 仅在类内找到 `.close()`)。各泄漏其 aiohttp `ClientSession`(并发"Unclosed client session"警告)。修复：在 app `lifespan` 关停中关闭它们。

**[P2] `save_klines_to_db` 逐行 flush** — `core/data/data_storage.py:202-208`：`await session.flush()` 在每 kline 循环内以单独捕 `IntegrityError`。对 1000-bar 批是 1000 次 SQLite(WAL)往返——慢，且持写事务足够久以加剧与并发读者的争用。(parquet 是实时热路径，故影响有界，但批量 DB 回填会爬。)修复：用 `INSERT OR IGNORE` 批量插入(news 模块已用此模式)而非逐行 flush。

**[P2] 二级回填在持续错误时静默跳过整窗** — `core/data/second_level_backfill.py:378-382`：连续 3 次失败后推进 `cursor = window_end`、重置 `error_count = 0`、继续；任务最终报 `status="completed"` 尽管有缺口。无挂起(好)，但抖动的交易所/代理产出静默不完整的 1s 数据集且不向操作者暴露失败。修复：在任务状态记录跳过的范围 和/或 标 `status="completed_with_gaps"`。

### Verified OK
- `exchange_manager.initialize`——经 `gather(..., return_exceptions=True)` 并行连接器初始化，含每连接器 `EXCHANGE_STARTUP_CONNECT_TIMEOUT_SEC`(`exchange_manager.py:175-206`)；`_create_connector` 清理失败的 client(`:259-266`)；`reconnect_exchange` 受 `wait_for(timeout=20s)` 有界。
- `BinanceConnector.connect/disconnect`——`_connection_lock` 串行化，候选 client 模式避免失败重连时丢活 client，交换时关旧 client(`binance_connector.py:217-255`)；OKX/Bybit 正确镜像。(Gate 的 `connect` 缺锁+候选交换但单共享实例，低风险。)
- `historical_data.download_historical_klines`——`wait_for(get_klines, 45s)`、连续错误上限(6)、近指数退避、不可重试符号快速失败、cursor 推进守卫防非进展(`historical_data.py:175-276`)。这是实时采集器应匹配的参考实现。
- `base_exchange.health_check`——用轻量 `fetch_time` 含 `wait_for(6s)` 而非 24hr ticker；`_is_transient_connection_error` 仅在瞬时错误时翻 `_connected=False`，故看门狗重连。
- `data_storage` 时区归一(`_normalize_parquet_frame_index`、`_heal_local_existing_against_utc`)——legacy +8h healing 是 OHLC-anchored 且自验证；future-bar `-8h` 回退守卫(`now + 2min`)不会在真实实时 bar 上触发(实时 bar 时间戳 at-or-before now)。
- `ccxt_adapter`——默认只读，执行由 `supports_execution` 闸门，所有 REST 经 `asyncio.to_thread`(它有意包装*同步* ccxt 模块)，`_to_dt_ms` 正确处理 s/ms/us/ns。
- `macro_collector` 同步 `requests`/`yfinance`——正确隔离在 `_sync()` 闭包中经 `run_in_executor`/`to_thread` 分发(`macro_collector.py:555,612,706`)，不阻塞循环。
- `DataCollector` 循环任务生命周期——引用保留防 GC，`stop()` 中干净 cancel-and-await(`data_collector.py:74,306,315-323`)。
- `path_utils.normalize_symbol`——一致的 `/`-分隔规范化，含 `candidate_symbol_dirs` 对 legacy `_`/`-`/拼接目录名的回退；存储路径无 key 碰撞风险。

---

## A5. 新闻管线（audit-news）

**范围**：`core/news/collectors/*`、`core/news/eventizer/*`、`core/news/service/worker.py`、`core/news/storage/db.py`

### Domain Health Summary
**整体稳固。** worker 循环隔离良好(每源 try/except、每批 try/except、stale-running reclaim、CancelledError 传播、有界队列、有界扫描、原子任务领取)。采集器阻塞 I/O 正确经 `asyncio.to_thread` 卸载。下列问题多为 retry-storm / starvation / dedup-correctness 边缘，在持续 uptime 或 provider 中断下咬人，非即时崩溃。

### Findings（best-first）

**[P1] 限流重试无最大尝试上限——provider 卡死时任务永久 churn** — `core/news/storage/db.py:1428-1433`。非成功分支对 timeout(`attempt >= 5 → failed`)与 other(`attempt >= 3 → failed`)封顶，但 rate-limit 分支无条件置 `status="retry"` 无终态：
```python
if is_rate_limited or error_type == "rate_limit":
    backoff_seconds = min(300, 30 * (2 ** (attempt - 1)))
    row.status = "retry"   # 无 attempt >= N → failed 守卫
```
运行时影响：provider 持续返回 429(或永久处于 provider-backoff，见下条)的任务被反复 re-claim → re-requeue，每周期烧一个 claim 槽 + `attempt_count++`。永不流向 `failed`，故永久占据优先队列头。唯一逃逸是 stale-running `attempt >= 6 → failed` 路径(1309 行)，仅 worker 中途死亡时触发。修复：在 retry 赋值前加 `elif (is_rate_limited or error_type=="rate_limit") and attempt >= 8: row.status="failed"`。

**[P1] provider-backoff 项被领取后每轮立即 requeue——饿死健康 provider** — `core/news/service/worker.py:437-465`。`claim_llm_tasks(limit=8)` 按 `priority DESC` 排序且**不感知** provider backoff；provider 过滤仅在领取*之后*。故当高重要性 provider(如 `binance_announcements`，priority 48) 处于 backoff 时，每轮先领那些行，调 `finish_llm_tasks(is_rate_limited=True)`(增 `attempt_count`，见上条)，并 requeue——在到达更低优先级健康 provider 任务前耗尽批预算。
```python
batch = await news_db.claim_llm_tasks(limit=limit)   # 领取 backed-off provider 的行
...
provider_backoff = await news_db.get_provider_backoff(provider)
if provider_backoff: backoff_ids.append(...)          # 然后扔回去
```
运行时影响：某 provider 中断期间，其他 provider 的事件停滞(处理延迟尖峰)且 attempt 计数膨胀。指数 `next_retry_at` 推进防止*紧*循环，故是吞吐饿死问题，非忙循环。修复：在 `claim_llm_tasks` SQL 内过滤 provider-backed-off 行(或传 exclude-provider 集)，使其根本不被领取。

**[P2] provider backoff 仅进程内——多 worker/重启丢失；global backoff 同理** — `core/news/storage/db.py:104, 1819-1834` 与 `99, 1770-1803`。`_provider_rate_limit_backoff` 与 `_global_rate_limit_backoff` 是模块级 dict/var(global 那个甚至在 1773 行文档化为进程本地)。运行时影响：若系统跑 >1 worker 进程，或 worker 重启，所有 backoff 状态丢失，每进程重锤被限流 provider 直到拿到新 429——对 relay 的雷鸣群。DB 持久的每任务 `next_retry_at` 部分缓解，但 provider 级闸门没了。修复：把 provider backoff 持久到 `news_source_state.paused_until`(已存在且被采集器路径遵守)或小表，而非内存 dict。

**[P2] 若设了 `OPENAI_FAILOVER_STATE_PATH` 则事件循环停滞风险——async `_request` 内同步文件锁 + 读/写** — `core/news/eventizer/async_glm_client.py:616` → `core/utils/openai_responses.py:791,457-...`。`AsyncGLMClient._request` 同步调用 `prioritize_openai_targets(...)` / `remember_openai_target_*`。设了该 env var 时，`_scoped_failover_enabled()` 变 True，那些 helper 获取阻塞 OS 文件锁(`msvcrt.locking`/`fcntl.flock`)并做同步 JSON 读 + `os.replace`(350-404 行)**在事件循环上**，每请求一次、每成功/失败再一次。运行时影响：争用下(多个 async LLM 调用 + 锁被另一进程持有)整个 news 事件循环阻塞于文件锁——停滞所有其他 async news 任务。默认配置安全(`_ENABLE_CROSS_REQUEST_FAILOVER_CACHE=False`，env 未设 → 纯 pass-through)，故潜在。修复：将 scoped-failover 持久化包入 `asyncio.to_thread`，或在 async 路径跳过。

**[P2] 30 分钟去重窗在 impact_score 仅小幅变动时可误删不同事件** — `core/news/storage/db.py:1503-1512`。语义 key 按 `(symbol, event_type, sentiment, 30 分钟桶, title/url anchor)` 分桶。key 碰撞时，若 `impact_diff <= 5% AND sentiment unchanged` 则丢行：
```python
impact_diff = abs(new_impact - prev_impact) / max(abs(prev_impact), 0.01)
if impact_diff <= 0.05 and new_sentiment == prev_sentiment:
    deduped_count += 1; continue
```
运行时影响：30 分钟内映射到同 symbol/event_type/sentiment 且**无共同 title/url anchor**(anchor 回退到 `"no_anchor"`)的两条真正不同的新闻塌成一条——真实事件静默丢失(如同半小时内同一币的两次不同交易所上币)。`no_anchor` 回退是弱点：同粗 key 的不同无 anchor 事件被当作 dup。修复：当 `anchor == "no_anchor"` 时跳过 impact/sentiment 去重(去重要求真实 title 或 url)。

**[P2] 30 分钟时间桶去重分裂跨边界事件** — `core/news/storage/db.py:235`、`core/news/eventizer/async_glm_client.py:1395`。`bucket = int(dt.timestamp() // 1800)` 意味着同一事件在 10:29 与 10:31 落入不同桶且不去重，而 10:01 与 10:29 去重。这是已知 floor-bucket 伪影。运行时影响：半小时边界附近偶发重复事件虚增计数且可能双计新闻情绪。低严重(外观/计数漂移)。修复(如需)：对当前与前一桶都去重，或用基于最近事件查找的滑窗。

**[P2] 两个略有差异的去重实现(raw 采集器 vs DB)可不一致** — `core/news/collectors/manager.py:86-93` vs `core/news/storage/db.py:212-218`。`manager._canonical_title` 保留原始文本仅剥 HTML/标点；`db._canonical_title` 额外跑 `clean_news_text`(mojibake 修复)。运行时影响：在两阶段间归一不同的标题可能过采集器去重但是 DB dup(反之亦然)——一般无害因 DB 层权威，但意味着采集器阶段 `kept_total` 计数会误导且少量 dup 溜进 LLM 队列。修复：共享单一 `_canonical_title` 实现。

**[P2] `process_llm_batch` global-backoff 检查是每进程；周期 poll + event-driven 路径都跑** — `core/news/service/worker.py:583-585, 620-624`。每个新拉批在 `pull_source_once`(620 行) 入队 **并** 在 `_process_event_batch`(583 行) 再入队+处理，而 `worker_loop` 也每 ~20s poll `process_llm_batch`。入队幂等且 `claim_llm_tasks` 原子(UPDATE...WHERE status IN(pending,retry) 含 rowcount 检查@1338)，故**无双 LLM 调用**——已验证安全。唯一成本是每拉冗余 `enqueue_llm_tasks` 往返。记为已验证安全但浪费，非 bug。

**[P2] `_persist_llm_title_summaries` 外层超时对大批次可极大** — `core/news/service/worker.py:125-128`：
```python
timeout=max(timeout_sec + 2, timeout_sec * ceil(len(raw_targets)/batch_size) + 2)
```
有 local-gemma 备份时 `timeout_sec` 被强制 ≥120 且 `batch_size` 为 1，故 50-title 批的外层 `asyncio.wait_for` 变 `120*50+2 ≈ 6002s`。运行时影响：summary-persist 步(best-effort，包 try/except → 失败返零)在病态 local-LLM 配置下可阻塞单批完成达 ~100 分钟。它不卡死循环(在 `_execute_llm_batch` 内联 await，且 per-call 超时仍触发)，但延迟 task-done 标记。修复：封顶外层超时(如 `min(..., 300)`)；靠内层 per-call `summarize_timeout` 真正强制。

### Verified OK
- **worker 循环韧性**：`worker_loop` 把每个源拉与 LLM poll 包 try/except(worker.py:690-708)；一个坏源/异常杀不掉循环。`_event_processor_loop` 重抛 `CancelledError`(561-563 行)且其他异常退避 5s(566 行)——无吞、无紧崩溃循环。
- **卡任务恢复**：`claim_llm_tasks` 把超过 `NEWS_LLM_RUNNING_TIMEOUT_SEC`(默认 900s)的 `running` 任务 reclaim → retry，或 6 次后 → failed(db.py:1293-1316)。崩溃的 worker 不会永久卡死任务。
- **并发下原子领取**：claim 用 `UPDATE ... WHERE id=? AND status IN(...)` + rowcount 检查(db.py:1338-1359)——两 worker 不能都领同一任务。
- **批处理每项隔离**：`_process_llm_task_batches` 把每子批包 try/except continue(worker.py:279-291)；`_execute_llm_batch` 异常时标任务 failed 再重抛(worker.py:245-253)，故状态从不停在 `running`。
- **SQLite WAL**：连接时设 WAL + `synchronous=NORMAL` + `busy_timeout` PRAGMA(db.py:88-96)；NullPool 避免陈旧连接；去重扫描有界 `_EXISTING_RECENT_SCAN_LIMIT=5000`。`OR IGNORE` 插入 + `IntegrityError`/rollback 路径正确(db.py:1198-1222)。
- **LLM 超时强制**：`aiohttp.ClientTimeout(total=..., connect=...)` 真实且 per-target(async_glm_client.py:566-573, 631)；`TimeoutError`/`ClientError` 捕获 → `"timeout"`/`"other"` 含 failover-to-next-target 后返回(无挂起)。`llm_glm5.py` 同步路径加 wall-clock 预算上限(416 行)，故 N-target failover 不会变 N×timeout。
- **采集器阻塞 I/O 离循环**：RSS/`requests` 采集器经 `_run_incremental_source_pull` 的 `asyncio.to_thread`(manager.py:447-462) 与同步 `pull_latest` 的 `ThreadPoolExecutor` 运行——同步 `requests.get` 在 worker 路径不阻塞事件循环。
- **aiohttp session 生命周期**：每 `_request` 用 `async with aiohttp.ClientSession(...)`(async_glm_client.py:631)——退出时关闭，无泄漏。`requests.Session` 采集器在 `finally` 经 `_close_collectors` 关闭(manager.py:326-327, 434-435)。
- **采集器错误隔离**：每 feed try/except 含 `logger.warning(... source URL ...)` continue(rss.py:176-178)；每源 future 异常被记录并录入 `source_stats[...]['errors']`(manager.py:308-313, 402-420)——一个死源从不停其他源；429 经 `paused_until` 触发每源 cooldown。
- **有界内存增长**：事件队列 `maxsize=1000` 含 `put_nowait`/QueueFull 丢+记(worker.py:485, 505-507)；summary 缓存 4000 LRU 淘汰(async_glm_client.py:1679-1683, llm_glm5.py:840-844)；去重集每调用(不保留)。
- **时区正确性**：全 `datetime.now(timezone.utc)`；`parse_any_datetime` 归一 naive→UTC(models.py:138-140)。范围内无 naive-datetime bug。
- **async-lock 循环亲和**：global/provider backoff 锁在运行循环变化/关闭时重建(db.py:164-189)——避免跨重启"锁绑到不同循环"错误。

---

## A6. AI 研究（audit-ai-research）

**范围**：`core/ai/*`、`core/research/*`、`core/ml/*`

### Domain Health Summary
agent 循环与 registry 在崩溃安全上**架构稳健**：agent 的 `_loop` 捕获每周期异常且永不死，`run_once` 由锁串行化，model-provider 调用硬超时有界(45s)，后台研究任务保留任务引用(无 GC 死亡)，best-effort LLM 调用(narrator/context)完全隔离。数值代码(DSR、`_compute_score`、walk-forward、相关性 Pearson)对除零/NaN/空有良好守卫。**主导问题是流畅度/稳定性：整个重研究计算同步跑在事件循环上**，每次运行冻结整个服务(含实盘交易循环与健康检查)数分钟。次要问题是无界持久 job 增长与一个仅在 ops 跑独立进程时咬人的跨进程 registry 损坏洞。

### [P0] 重研究计算阻塞事件循环 — `core/research/strategy_research.py:2911-3140`（入口 `run_strategy_research`@2759；由 `orchestrator.py:1312`、`ops/service/api.py:665,721`、`ai_routes.py:47,313` 以 `await` 方式调用）
`run_strategy_research` 是 `async def`，但其核心是 `frames × strategies` 上的同步嵌套循环，每个调 `_optimize_params_scipy_lhs`(30 回测)、full + OOS 回测、`_run_purged_walk_forward`(5 折 × 每折至多 13 回测)、equity-curve 构建——全经纯同步 `def _run_backtest_core`(2132)。**2911 到 3140 行间无一个 `await`**(唯一 await 是循环*前*的数据加载)。默认下(60d、4 timeframe、~37 策略)，约 1 万+次同步回测持有循环。
- **运行时影响**：FastAPI 事件循环整个运行期(数分钟)卡死。`/health`、websocket、自治 agent `run_once`、CUSUM/熔断 worker、订单管理 handler 全停滞——实盘系统上延迟风险反应。注意 `web/api/backtest.py` 与 `web/api/ml.py` 对同类工作已做 `await asyncio.to_thread(...)`——研究是异类。
- **修复**：将计算体卸载到 worker 线程：把同步 sweep(或每个 `_run_backtest_core`/`_optimize_*`/`_run_purged_walk_forward` 块)包入 `await asyncio.to_thread(...)`，保持 `progress_callback` 线程安全(它只改 dict)。最简：把每 cell 工作抽成同步 helper 并 `to_thread` 它。

### [P1] 跨进程 registry 损坏 — `core/research/experiment_registry.py:54,72-93` + `core/ops/service/api.py:464`
registry 用**每进程** `threading.RLock` 与共享 tmp 路径 `self.path.with_suffix(".tmp")` → `os.replace` 守卫写。ops 服务有独立 lifespan(`api.py:1061-1067`，`initialize_ops_runtime(..., standalone=True)`)，对**相同** `DATA_STORAGE_PATH/../research/ai/` 文件调 `ensure_ai_research_runtime_state(app)`(`orchestrator.py:432`)。若 ops 在 web app 旁作独立进程运行，两进程写**同一** `proposals.tmp` 并 `os.replace` 同一目标，无跨进程文件锁。
- **运行时影响**：交错 tmp 写/丢更新/下次启动 `json.loads` 失败的半写 JSON → registry 加载失败或静默数据丢失。正常进程内挂载(`web/main.py:1502,1706`，`standalone=False`)安全——严重性取决于部署。
- **修复**：tmp 名按写者唯一(如 `.tmp.{os.getpid()}.{token}`) 并在 read-modify-write 周围加跨进程锁(`.lock` 边车 via `msvcrt.locking`/`portalocker`)，或文档化 registry 目录须恰由一个进程拥有。

### [P1] `_replace_with_retry` 仅捕 `PermissionError` — `core/research/experiment_registry.py:23-33`
Windows 上，`os.replace` 覆盖被另一句柄打开的文件最常抛 `PermissionError`(WinError 5)——已覆盖——但瞬时杀软/索引器锁可能表现为其他 `OSError` 子类。仅 `PermissionError` 被重试；其他立即传播并中止 flush。
- **运行时影响**：命中非 `PermissionError` OS 锁的 flush 抛出 `save()/save_many()`，在 `_finalize_research_run`(orchestrator) 中可使整个研究 finalize 失败并留 proposal 半态。罕见，但正是此重试存在要吸收的 Windows 故障模式。
- **修复**：把重试循环的 `except` 放宽到 `OSError`(或 `(PermissionError, OSError)`)。

### [P1] 持久 `research_jobs` 无界增长 — `core/research/orchestrator.py`（`_persist_research_jobs`@105；仅 `delete_proposal`@816 剪枝）
`_prune_finished_research_job_tasks`(314-328) 仅剪枝内存 `research_job_tasks`(asyncio.Task 引用)，**非**序列化到 `research_jobs.json` 的 `research_jobs` dict。完成/失败/取消的 job 永久累积除非其 proposal 被删。每次 dispatch 重写整个文件(`_persist_research_jobs` dump 整个 dict)。
- **运行时影响**：长 uptime 下 JSON 与内存 dict 无界增长，每次 job 状态变更成 O(n) 全文件重写，启动 `_load_research_jobs` 读越来越大的文件。慢但真实的内存 + I/O 蠕变。
- **修复**：在 `_persist_research_jobs`/启动加载时封顶保留的完成 job(如保留末 N 或 X 天内)。

### [P1] 研究调度器无并发界 — `core/ai/research_scheduler.py:111-133`
`_tick` 遍历每个 `research_queued` proposal 并对每个未在跑的 `await run_proposal(..., background=True)`，无信号量/队列上限。tick 内去重(`running_ids`，106-109)正确，但若一次排队很多 proposal(如 CUSUM auto-draft + 手动)，每个生成后台任务。
- **运行时影响**：与 P0 叠加(每个后台研究阻塞循环)，堆积的 job 在冻结循环上串行化且队列可膨胀；许多并发 `ResearchConfig`/result 对象的内存。仅受 300s 间隔有界。
- **修复**：每 tick 封顶 dispatch 和/或用 `asyncio.Semaphore`(如 max 1-2 并发)闸门活跃后台研究。

### [P2] 候选 metadata 在 finalize 锁外被突变(别名) — `core/research/orchestrator.py:1322 vs 1330-1339,1448`
在 `_research_finalize_lock` 下，`save_many(candidates)`(1322) 把**活对象引用**存入 registry 缓存(`experiment_registry.py:108` `items[key]=item`，无拷贝)。然后*在锁外*，`_add_rationale` 突变 `cand.metadata["llm_rationale"]`(1339)，且 cusum_watcher(`cusum_watcher.py:79,112`) 突变 `cand.metadata["cusum_status"]` 并保存——全是同一缓存对象。
- **运行时影响**：今所有 caller 在同一事件循环线程，故无活损坏；但若任何 registry `save`/`_flush` 曾从 `to_thread` worker 运行(锁定注释明确预期此)而另一路径突变同一 `metadata` dict，`model_dump()` 可抛 `RuntimeError: dictionary changed size during iteration` 或序列化撕裂状态。
- **修复**：保存时存拷贝(`model_copy(deep=True)`)，或仅在 registry 锁下 mutate-then-save；从不交出再突变缓存引用。

### [P2] `ensure_ai_research_runtime_state` 首次初始化/恢复非并发安全 — `core/research/orchestrator.py:441-464`
`first_init` 由读(`isinstance(getattr(...), ProposalRegistry)`)算出，然后赋 registry 并仅在 `first_init` 时跑 `_recover_stale_jobs_on_startup` + `_recover_missing_proposals_from_candidates`。check→assign→recover 序列无守卫。单事件循环线程上安全(check 与 set 间无 await)，但可从请求 handler、调度 worker、`asyncio.to_thread` worker 线程到达。
- **运行时影响**：`to_thread` caller 在首次调用与循环线程竞速可双初始化 registry 或双跑恢复(恢复幂等-ish——仅翻 `research_running/queued`→`draft`，故影响低)。"恰一次"保证靠单线程时序，非锁。
- **修复**：用 `threading.Lock` 与 `app.state` 上显式 `_ai_research_initialized` 标志守卫 init/recovery 块。

### [P2] `CUSUMMonitor.update` 每 bar O(n)、历史无界 — `core/monitoring/strategy_monitor.py:199-203`
`update()` 加入 `self._returns`(从不自动剪枝；仅 `full_reset()` 清)并每次调用对**整个**历史重算 `_robust_std`(排序 → O(n log n))。累积 O(n² log n)，无界内存。
- **运行时影响**：长实时流下会退化并泄漏——**但**该类目前仅测试用；生产 CUSUM watcher 用对 5000-capped `risk_manager._trade_history` 的一次性 `detect_strategy_decay`，故今无实时影响。若接入真实 per-bar 循环则是潜在地雷。
- **修复**：维护有界窗(deque maxlen)与增量/Welford std，或文档化为仅离线。

### [P2] 储备激活在保存失败时可留不一致状态 — `core/monitoring/cusum_watcher.py:389-400`
`_activate_reserve_replacement` 调 `transition_candidate(reserve, to_state="shadow_running", ...)`(它 append 一条 lifecycle 记录)然后才 `registry.save(reserve)`(399)。若 `save` 抛，lifecycle 日志记录了 candidate registry 从未持久的提升。
- **运行时影响**：registry vs lifecycle 分歧(一个 "shadow_running" 事件而 candidate 在盘上仍是先前状态)。低频、可恢复，但内存 `reserve.status` 无论如何被突变。
- **修复**：先保存 candidate(或把 transition+save 包起来使 save 失败回滚/重记)，与主 finalize 路径排序保存一致。

### Verified OK
- **agent 循环崩溃安全**(`autonomous_agent.py:1658-1684`)：每周期 `run_once` 异常捕获、循环继续；固定速率 sleep 含 1s stop-event 响应；`stop()` 干净取消 main 与 manual 任务。
- **model-provider 硬超时**(`autonomous_agent.py:5829-5848`)：包在 `asyncio.wait_for(45s)` 外、aiohttp 调用本身有 `ClientTimeout`；超时/错误处理，循环不会卡在挂起 provider 上。
- **`run_once` 重入**(`5638`)：`_run_once_lock` 串行化 loop vs manual 运行；manual 任务引用跟踪 + `add_done_callback` 清理。
- **后台研究 job 生命周期**(`orchestrator.py:1542-1683`)：任务引用存于 `research_job_tasks`；`try/except CancelledError/Exception/finally` 总置终态 job 状态、转 proposal/experiment/run 到 failed/rejected 并重持久——job 崩溃无孤儿 "running" 态。
- **stale-job 恢复**(`_recover_stale_jobs_on_startup`@266)：仅在 `first_init` 跑、全包 try/except、仅重置 `research_running/research_queued`→`draft` 与 pending/running runs→failed(不会杀真正在跑的工作，因在跑工作仅在活任务后存在，新进程没有)。
- **best-effort LLM 隔离**：`promotion_narrator.generate_promotion_rationale` 与 `research_context_generator` 在每个失败路径返 `None`；narrator 在 `asyncio.wait_for(30s)` 下调用含任务取消(`orchestrator.py:1352-1372`)且从不阻塞提升。
- **数值健壮**：`_deflated_sharpe_ratio`(`validation_gate.py:80-116`)守卫 `adj`≥0.01、`sr_hat_std<=0`、钳 [0,1]；`_compute_score` 把 `_dd` 下限 0.5；`_compute_wf_stability` 处理空/单折；`_correlation_filter_candidates` 在 `np.corrcoef` 前跳过 `std<1e-9`；edge/risk/efficiency 分用 `max(...,1.0)` 分母与 clip helper。
- **CUSUM watcher**(`cusum_watcher.py`)：用有界(5000)`risk_manager._trade_history`；通知任务在 `_NOTIFICATION_TASKS` 集含 discard callback(无 GC 死亡)；每个反馈子步独立守卫。
- **registry 临界区**：任何 `with self._lock:` 块内无 `await`——文档化的 async-safety 不变量成立。

---

## A7. 风控 / 会计 / 监控 / 运维（audit-risk-ops）

**范围**：`core/risk/*`、`core/accounting/*`、`core/monitoring/*`、`core/ops/*`、`core/governance/*`、`core/audit/*`、`core/observability/*`、`core/notifications/*`、`core/backtest/*`、`core/market_state/*`、`core/structural/*`、`core/events/*`、`config/*`；次要：`prediction_markets/*`、`scripts/*`

### Domain Health Summary
风控、会计、监控与回测层成熟且防御性编码：熔断器、daily-stop 与 PnL decomposer 带有此前 bug(funding 双计、SL/TP gap、close-and-flip)的正确修复，monitor worker 跑在受监督的 restart-with-backoff 框架下，通知正确卸载阻塞 I/O，managed startup 正确阻止持久化 live-mode 恢复。最实质风险不是崩溃而是**风控静默削弱**：每策略回撤熔断按总组合权益度量，以及一个 live daily-stop 豁免使隔夜浮亏逃过 1× 限。未发现意外 live 启用或丢钱会计 bug。

### Findings（best-first）

**[P1] 每策略熔断用总账户权益作分母** — `core/risk/circuit_breaker.py:846-851`(+ `_drawdown_from_pnl:742`) — `run_circuit_breaker_checks` 把 `base_capital=_resolve_account_equity()`(全组合权益)传入 `evaluate_strategy_drawdowns`，故每策略回撤是 `cumulative_strategy_pnl / total_equity`。分配 ~15% 资金的策略须亏其自身账面 ~33% 才登记配置的 5% `CB_STRATEGY_DAILY_DD_PCT`。**影响**：每策略熔断实际松约 6×；单个衰减策略可在触发前远超其预期 kill 阈值地放血。**修复**：除以每策略分配资金(equity × allocation)而非总权益，或按 allocation 缩放阈值。

**[P1] live daily-stop 豁免隔夜浮亏(fail-open 至 2×)** — `core/risk/risk_manager.py:644-670` — 当 `live AND _daily_trades <= 0 AND |daily_realized| < 1e-9` 时豁免触发。`_daily_trades` 与 `_daily_realized_pnl` 都在午夜重置为 0(`_check_new_day:440-441`)，但 `_current_unrealized_pnl` 持续。昨开今放血的持仓 `stop_basis_usd = 0 + min(0, unrealized) < 0` → 违规，却被当作"未跟踪外部权益变动"并忽略，直到 2× 灾难兜底。**影响**：持有的持仓可在 halt 接管前亏至 2× 日限。**修复**：仅在也无系统开仓时(`position_manager.get_position_count() == 0`)应用豁免，而非仅今日无交易。

**[P1] 熔断闸门在评估错误时 fail-open** — `core/trading/execution_engine.py:3674-3683` — `cb_decision = _cb.evaluate(...)` 被包成任何异常都置 `cb_decision = None`，守卫 `if cb_decision is not None and not cb_decision.is_allow` 即完全跳过熔断器且订单继续。**影响**：若熔断器导入或状态读曾抛(模块 reload、状态损坏)，被触发的策略/组合会被静默绕过且放置新入场。低概率(锁下纯内存 dict)但确是风控绕过。**修复**：异常时对非 reduce-only 订单按 `close_only`/阻止处理而非放行。

**[P2] live 切换单操作员确认闸门；dual-approval 设置未在运行期强制** — `web/services/trading_runtime_service.py:52-74,157-172` — `request_mode_switch`/`switch_trading_mode` 把 live 进入闸门设为 token + 精确文本 "CONFIRM LIVE TRADING" 但从不查 `settings.REQUIRE_DUAL_APPROVAL_FOR_LIVE`(默认 True)；该标志仅在 `core/governance/service.py:456` 为策略生命周期遵守。**影响**：运行期 paper→live 翻转是单操作员，即使操作员相信需要 dual approval。由 managed startup 默认 paper 缓解。**修复**：在 `switch_trading_mode` 检查 `REQUIRE_DUAL_APPROVAL_FOR_LIVE` 并在设置时要求第二 approval token。

**[P2] `gate_counterfactuals.jsonl` 无界增长 + 全文件重写** — `core/audit/gate_counterfactuals.py:81-83, 102-166` — `record_gate_counterfactual` 无轮转/上限地 append，且 `update_gate_counterfactual_outcomes` 把整个文件载入内存(`list(iter_gate_counterfactuals(...))`)并原子重写(每次更新 O(n))。由 `cusum_watcher._record_decay_feedback` 写。**影响**：缓慢无界磁盘增长；回填成本随系统寿命线性增长。**修复**：封顶/轮转 jsonl(如保留末 N 或按日 roll)。

**[P2] OPS token 非常数时间比较** — `core/ops/service/auth.py:62` — `received != expected` 是普通字符串比较。**影响**：ops token 的理论时序侧信道；低严重因 API 绑 localhost 且 token 未设时 fail closed(503，非绕过)。**修复**：用 `secrets.compare_digest`。

**[P2] CUSUM watcher 降级副作用被吞；储备激活可跨模式误触发** — `core/monitoring/cusum_watcher.py:114-115, 51-67` — 每候选循环在 `logger.debug` 级捕获所有异常，故失败的降级/通知在 INFO/WARNING 不可见。decay 由活范围 `_trade_history` 仅按策略名过滤(无 mode 标签)算出，故跨 paper/live 复用的名字可能混合 returns。**影响**：健康策略可能被降级(或衰减的被留下运行)而仅 debug 级痕迹；低可能。**修复**：降级失败记 warning 级；CUSUM 前按 mode 标记 returns。

### Verified OK
- **回测 funding 双计**——正确修复：funding 累入 `pos["funding_pnl"]`，funding 时**不**credit `_capital`，平仓时实现一次；`_calculate_result:874-893` 从 close-net 剥 funding 并恰一次重加 funding-stage 总额。
- **回测 SL/TP gap-through**——`_check_position_exits:427-446` 在 `min/max(stop, open)`(悲观)成交止损并拒止盈的有利 gap 改善。
- **回测前视**——`generate_signals` 与 `check_exit` 见 `data.iloc[:i]`(排除执行 bar)；执行用当前 bar 价。slippage 用同期 bar 波动但仅作成本(保守)，非收益预测器。
- **PnL FIFO 撮合**——`_apply_closing_fill` lot 消耗、部分平仓按比例 fee/slip、close-and-flip(新持仓 funding-clean 起步)全正确。
- **monitor worker**——`cusum_monitor` & `circuit_breaker_monitor` 跑在 `RuntimeTaskSupervisor` 下含 `restart_on_failure=True` + 1→30s 指数退避与每迭代 try/except；`circuit_breaker` 跑在 `asyncio.to_thread`(离事件循环)。
- **通知 best-effort**——telegram/feishu/wechat 用 async `httpx`(12s 超时)；email 经 `asyncio.to_thread` 卸载；CUSUM 通知任务持于模块集含 `add_done_callback(discard)`(无泄漏/无 GC 丢)。
- **managed live-restore 守卫**——`web/startup_mode.py` 阻止持久化-live 恢复除非 `allow_persisted_live_mode_start=True`；默认 `paper`。
- **governance/ops 鉴权**——fail CLOSED：未设 `OPS_TOKEN` → 503；无效 key/token → 401；`evaluate_order_intent` DB 错误传播并被执行引擎外层 handler 捕获返 `None`(订单不下)。
- **`pre_trade_check` fail-closed**——权益/名义不可验证的新入场被拒(`risk_manager.py:845-855`)；平仓总允许(降风险)。
- **设置/secrets**——无明文 secret 日志；所有凭据字段默认空；`WEB_SECRET_KEY` 占位是非 secret 且 CORS 是显式白名单(无 `"*"`)。
- **预测市场(次要)**——live CLOB 交易抛 `NotImplementedError`；默认禁用 + paper 模式含名义上限。无 money 路径。

— 审计结束 —
