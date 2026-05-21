# 审计整改团队分工方案

审计输入：`reports/audit_2026-05-21/00_SUMMARY.md` 及 6 份分模块报告。  
目标：先消除资金、订单、回测评分和状态机正确性风险，再处理并发阻塞、性能和清理项。

## 总体原则

1. P0 项按审计汇总编号建 issue，issue 标题保留 `P0-xx`，避免跨组沟通时丢失上下文。
2. 每个 P0 修复必须包含回归测试；无法自动化的实盘路径至少提供 mock/假 connector 测试。
3. 资金、订单、持仓、风控相关 PR 必须两人 review，并先在 paper/shadow 环境跑冒烟验证。
4. 不在 P0 PR 中做大规模重构；需要重构的内容拆成后续 P1/P2 PR。
5. 每个小组每天同步一次状态：已合并、待 review、阻塞项、是否影响其他组。

## 分组与职责

| 小组 | 负责人角色 | 范围 | 首要目标 |
|---|---|---|---|
| A 资金与交易执行组 | 交易后端负责人 | `core/accounting/`, `core/exchanges/`, `core/trading/`, `core/risk/`, `core/exchange_adapters/` | 保证订单、持仓、PnL、风控计数正确 |
| B 回测与策略验证组 | 量化引擎负责人 | `core/backtest/`, `core/research/validation_gate.py`, `strategies/`, `core/strategies/` | 修复回测前视、评分公式和策略信号一致性 |
| C 新闻管道组 | 数据管道负责人 | `core/news/`, `core/data/news_collector.py` | 修复 LLM 任务状态机、热更新、去重和阻塞采集 |
| D AI 研究与监控组 | AI 平台负责人 | `core/research/`, `core/ai/`, `core/monitoring/` | 修复候选存储并发、研究状态恢复、CUSUM 与相关性过滤 |
| E Web 与前端组 | Web 负责人 | `web/api/`, `web/static/`, `web/main.py` | 消除 API 阻塞、前端状态丢失和无效轮询 |
| F 测试与发布组 | QA/Release 负责人 | `tests/`, CI, release checklist | 统一测试门槛、回归矩阵、上线验收 |

## 阶段安排

### Week 1：资金、订单、持仓完整性

主责：A 组。  
协作：F 组补测试，B 组确认回测会计语义。

| 优先级 | 审计项 | 交付物 | 验收 |
|---|---|---|---|
| P0 | #1 PnL 翻仓超额数量丢弃 | 修复 `PnLDecomposer` 单笔 long-to-short / short-to-long 翻仓 | 新增 `tests/test_pnl_decomposer_flip.py`，覆盖反向开仓和 closed snapshot |
| P0 | #6 closed_trades 快照 qty/avg 为 0 | 平仓前保存原始 qty/avg_entry_price | closed record 中 qty、entry_price、avg_entry_price 非 0 且与原仓一致 |
| P0 | #2 OKX/Bybit lot/tick 精度 | connector 层统一 `amount_to_precision` / `price_to_precision` | mock ccxt 验证下单参数已格式化 |
| P0 | #3 OKX/Bybit 绕过 `_ensure_client()` | 所有查询/撤单/持仓方法先 `await _ensure_client()` | 断线后调用 `cancel_order` / `get_positions` 不抛 `NoneType` |
| P0 | #17 OKX/Bybit connect 无锁 | 增加 `_connection_lock`，仿照 Binance | 并发 `connect()` 只创建一个有效 client |
| P0 | #4 `risk_manager.record_trade()` 无锁 | 加并发锁或串行化更新 | 并发记录 100 笔交易，`_daily_trades` 精确等于 100 |
| P0 | #5 Binance defaultType 污染 | 用锁保护临时 options 或改用参数化调用 | 并发 `get_positions` + `get_balance` 不交叉污染 |
| P0 | #26 `ccxt_adapter._to_dt_ms` 阈值 | 修正毫秒/微秒/纳秒判断 | 正常毫秒时间戳保持 2020+ 年份，不倒退到 1970 |

建议 PR 切分：

- `PR-A1-pnl-flip-and-closed-snapshot`
- `PR-A2-okx-bybit-client-precision-reconnect`
- `PR-A3-risk-binance-concurrency`
- `PR-A4-ccxt-adapter-timestamps`

### Week 2：评分、回测、状态机正确性

主责：B 组、C 组、D 组。  
协作：F 组建立评分回归基线。

| 小组 | 审计项 | 交付物 | 验收 |
|---|---|---|---|
| B | #8 DSR 峰度公式 | `(kurtosis - 1)` 改为 `(kurtosis - 3)`，必要时记录阈值影响 | 正态返回下 adjustment 为 1.0 |
| B | #9 Sharpe 年化硬编码 | `BacktestEngine` 基于 bar 间隔或 timeframe 年化 | 1h/4h/1d 年化因子测试通过 |
| B | #12 `check_exit` 前视 | exit data 与 generate_signals 一致，排除当前 bar | 构造当前 bar 才触发退出的用例，应不提前退出 |
| B | P1 timestamp naive 残留 | `SignalFilter`、`SignalCombiner`、`get_status` 改 UTC aware | naive/aware 混用测试不再 TypeError |
| C | #13 LLM timeout 被截断 | YAML/env timeout 语义统一，默认不压到 16s | YAML 90s 配置在下一轮 worker 生效 |
| C | #14 配置热更新无效 | `_event_processor_loop` 定期或每批 reload config | 修改配置后无需进程重启 |
| C | #15 事件 LLM 绕过 backoff/任务去重 | 事件路径复用 backoff 与任务表状态 | 同 raw_id 不被事件和轮询双处理 |
| C | #20 `_event_processor_running` bool 竞态 | 改为保存 `asyncio.Task` 并检查 `done()` | crash 后只恢复一个 processor |
| D | #10 CUSUM IQR 死代码 | 删除坏分支或改 MAD 鲁棒 std | fat-tail 序列测试能触发预期衰减 |
| D | #11 相关性 key 用 strategy | 改为 `candidate_id` | 同策略不同参数曲线不互相覆盖 |
| D | #16 重启后 queued/running 变 rejected | 改为恢复到 `draft` 并记录原因 | 重启恢复后用户可重新运行 |

建议 PR 切分：

- `PR-B1-validation-and-sharpe`
- `PR-B2-backtest-exit-lookahead`
- `PR-C1-news-worker-config-and-timeout`
- `PR-C2-news-llm-dedup-backoff`
- `PR-D1-research-state-and-correlation`
- `PR-D2-cusum-robust-std`

### Week 3：并发泄漏与事件循环阻塞

主责：C 组、D 组、E 组。  
协作：A 组确认交易 API 操作路径不被 audit 写入阻塞。

| 小组 | 审计项 | 交付物 | 验收 |
|---|---|---|---|
| D | #18 `_send_cusum_notification` 同步函数 create_task | 改 async 并 await，或集中管理 task 引用 | 无 running loop 时不崩，通知不被 GC 取消 |
| D | #19 `wait_for(gather)` 超时泄漏 | 显式创建 gather task，超时后 cancel | GLM rationale 超时后无后台残留任务 |
| D | #21 `_JsonRegistry` 锁外读写 | 改 `RLock`，`list/_flush/_load` 锁语义统一；Windows replace 重试 | 并发 save/list 测试稳定，PermissionError 可重试 |
| C | #22 `feedparser.parse` 阻塞 async | 改 `asyncio.to_thread` 或标记 legacy 不走生产路径 | `collect_all_news` 不阻塞事件循环 |
| E | #23 API 端点 await audit_logger | strategies/trading_runtime/trading 错误路径全部后台写 audit | mock 慢 audit 时 HTTP 响应仍 < 50ms |
| E | #24 `pendingLlmContext` 失败丢失 | API 成功后再清空上下文 | 网络失败后 UI 保留研究上下文 |
| E | #25 轮询 404 不停止 | 404 或提案终态时停止 polling | 删除提案后不再每 3s 请求 job-status |

建议 PR 切分：

- `PR-D3-async-task-cancellation-and-registry-locks`
- `PR-C3-news-legacy-collector-nonblocking`
- `PR-E1-audit-logger-nonblocking`
- `PR-E2-ai-research-ui-state-polling`

### Week 4：性能与稳态优化

主责：A、B、C、E 组按模块推进。  
F 组做性能对比报告。

| 小组 | 主题 | 交付物 | 验收 |
|---|---|---|---|
| B | 因子策略 O(n^2) | Hurst/VaR/Sortino/CCI 替换高成本 `rolling.apply(lambda)` | 典型 10k bars 运行时间下降，有结果一致性说明 |
| B | 回测热路径 | `_update_positions` 每 bar 减少重复调用，microstructure proxy 缓存 | 回测耗时下降且交易结果不变 |
| A | 余额/equity 刷新 | OKX/Bybit/Gate 余额 TTL 缓存；多交易所 `gather` 并发刷新 | 多交易所 equity 刷新耗时接近最慢单交易所 |
| A | 风控报告缓存 | `get_risk_report()` 内 drawdown snapshot 复用 | 高频信号下 CPU 降低，无风控语义变化 |
| A | position persistence | 开/平仓写盘节流，`_position_history` 截断 | 高频开平仓无明显 I/O 抖动 |
| E | 数据维护 worker | 83 个 market sync 控制并发执行 | 维护周期耗时下降且不触发交易所限速 |
| C | non-SQLite `save_news_raw` | 改 ON CONFLICT 或 savepoint，避免 rollback 丢后续行 | 重复新闻不影响同批后续新新闻 |

## P2 清理滚动安排

清理项不阻塞 P0/P1，但每个功能 PR 可顺手处理同文件内的死代码。建议单独建 `cleanup` 看板：

| 事项 | 建议归属 | 处理方式 |
|---|---|---|
| 孤儿前端文件 `ai_research_candidates.js` / `ai_research_patch.js` | E 组 | 确认是否引用；无用则删除并修测试 |
| `core/data/news_collector.py` 与生产新闻管道平行 | C 组 | 标记 legacy 或迁移调用方 |
| `llm_glm5.py` 与 `async_glm_client.py` 重复 | C 组 | 提取共享 prompt/validate/cache 基础模块 |
| `stop_loss.py` 双轨风险 | A 组 | 加 deprecation warning 或移入 legacy |
| `SignalCombiner` 无调用方 | B 组 | 确认后废弃或补接入计划 |
| `_llm_provider()` 永远返回 openai | C 组 | 删除或明确兼容 API 语义 |
| `_run_walk_forward` legacy wrapper | B 组 | 标记 deprecated，测试迁移到 `_run_purged_walk_forward` |

## 测试与验收基线

F 组负责把以下测试变成整改 gate。P0 对应测试未合并前，相关 PR 不进入发布分支。

1. `tests/test_pnl_decomposer_flip.py`
2. `tests/test_validation_dsr_kurtosis.py`
3. `tests/test_backtest_sharpe_annualization.py`
4. `tests/test_okx_connector_reconnect.py`
5. `tests/test_strategy_monitor_robust_std.py`
6. `tests/test_correlation_filter_same_strategy_diff_params.py`
7. `tests/test_experiment_registry_windows_concurrent.py`
8. `tests/test_news_worker_config_hot_reload.py`
9. `tests/test_audit_logger_non_blocking.py`
10. `tests/test_ccxt_adapter_timestamp_units.py`

最低验收命令：

```powershell
python -m pytest tests/test_pnl_decomposer_flip.py tests/test_validation_dsr_kurtosis.py tests/test_backtest_sharpe_annualization.py
python -m pytest tests/test_okx_connector_reconnect.py tests/test_strategy_monitor_robust_std.py tests/test_correlation_filter_same_strategy_diff_params.py
python -m pytest tests/test_experiment_registry_windows_concurrent.py tests/test_news_worker_config_hot_reload.py tests/test_audit_logger_non_blocking.py tests/test_ccxt_adapter_timestamp_units.py
```

发布前增加一次冒烟：

```powershell
python -m pytest
python -m pytest tests/test_account_scoped_live_paths.py tests/test_strategy_mode_isolation.py tests/core/test_runtime_persistence.py
```

## Review 规则

| PR 类型 | 必须 reviewer | 额外要求 |
|---|---|---|
| 资金/订单/持仓/风控 | A 组负责人 + F 组 | 提供 paper/shadow 冒烟记录 |
| 回测/评分/验证 | B 组负责人 + 量化 reviewer | 提供 before/after 指标差异说明 |
| 新闻/LLM 状态机 | C 组负责人 + D 组 | 提供重复处理和 backoff 测试 |
| AI 研究/监控 | D 组负责人 + F 组 | 提供并发或超时测试 |
| Web/API/前端 | E 组负责人 + F 组 | 提供慢 audit、404 polling、失败保留状态测试 |

## 每日看板字段

每个 issue 使用同一套字段，便于汇总：

- `Owner`：负责小组和具体开发者
- `Audit Ref`：例如 `00_SUMMARY P0 #13`、`03_news H5`
- `Risk`：资金/订单、评分、状态机、并发、性能、清理
- `Status`：todo / in-progress / review / merged / blocked
- `Tests`：新增或修改的测试文件
- `Deploy Note`：是否需要配置变更、数据迁移、重启、paper 验证

## 关键依赖

1. A 组的 PnL、connector、risk 修复应先于任何实盘路径改动合并。
2. B 组 DSR/Sharpe 修复会影响历史评分阈值，合并后需要重新跑一批代表性策略结果，避免 UI 中旧分数与新分数混用。
3. C 组 LLM timeout 和 backoff 修复会改变任务吞吐，E 组轮询逻辑应兼容任务更长时间处于 running/retry。
4. D 组 registry 锁修复可能影响 AI research API 写入路径，E 组需要在 UI 侧做一次候选列表和详情刷新冒烟。
5. Week 4 性能项不应改变策略语义；如必须改变数学定义，需要单独写 migration note。

## 发布检查清单

- 所有 P0 issue 已关闭，且对应测试进入主测试集。
- paper/shadow 环境完成一轮：策略启动、下单、撤单、持仓刷新、风险报告、研究提案生成、新闻 LLM 处理、Web 操作。
- 审计目录新增 `P0_FIX_STATUS.md` 或 issue 链接索引，记录每个 P0 的 PR、测试、部署说明。
- 若 DSR/Sharpe 修复改变历史指标，明确标注旧结果不可与新结果直接比较。
- 若配置项语义变化，例如 `NEWS_LLM_WORKER_TIMEOUT_SEC`，同步更新 `.env.example` 和运维说明。
