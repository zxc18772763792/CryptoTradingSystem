# 代码全面审查改进文档

> 审查日期：2026-05-23
> 审查方式：6 个并行专项审查代理覆盖策略 / 回测研究 / 新闻ML / 风控执行 / Web 层 / 基础设施
> 发现总数：约 160 项（每项均有真实 `file:line` 引用）
> 受审范围：`E:/9_Crypto/crypto_trading_system`（约 18,686 行，150+ 文件）

---

## 1. 执行摘要 — Top 10 必须立即处理

| # | 类别 | 问题 | 位置 | 风险 |
|---|------|------|------|------|
| 1 | **机密泄漏** | `keys.txt` / `.env` / `.env.local` / `config/coinglass_api_key.txt` / `config/fred_api_key.txt` 含真实生产 API 密钥（Binance / Gate / GLM / CoinGlass / NVIDIA / OpenAI sk-*）以明文形式存在于工作目录 | 仓库根 | **必须立即轮换所有密钥** |
| 2 | **WebSocket 无认证** | `/ws` 端点直接 `accept()` 并广播实时仓位/订单/交易模式事件流 | [web/main.py:1576-1623](web/main.py) | 任意网络可达者可订阅交易数据 |
| 3 | **CORS 配置错误** | `allow_origins=["*"]` + `allow_credentials=True` 同时启用 | [web/main.py:1421-1427](web/main.py) | Cookie 会话凭证可被跨域劫持 |
| 4 | **回测端点无认证** | `/api/backtest/run`、`/optimize`、`/export` 等全部匿名可调，CPU 密集且未 `to_thread`，可被远程 DoS | [web/api/backtest.py:4200, 4296, 4597](web/api/backtest.py) | 资源耗尽 + 阻塞事件循环 |
| 5 | **实盘订单无 clientOrderId** | 网络抖动 + 上游重试可造成**重复成交** | [core/trading/order_manager.py:402-595](core/trading/order_manager.py) | 资金损失 |
| 6 | **`allow_close=True` 绕过风控** | 任意 close 类型信号跳过单笔比例、杠杆、持仓数检查 | [core/risk/risk_manager.py:666-676](core/risk/risk_manager.py), [core/governance/decision_engine.py:130](core/governance/decision_engine.py) | 风控形同虚设 |
| 7 | **Funding PnL 双计** | `cost_decomposition` 同时统计 `funding 阶段 PnL` 与 `close 阶段 accrued_funding`，对外报告值 = 真实值 × 2 | [core/backtest/backtest_engine.py:837-841](core/backtest/backtest_engine.py) | 回测结果系统性失真 |
| 8 | **Walk-Forward 名不副实** | 所谓 WF 实际上是把数据等分 6 份各跑独立回测，**完全没有 IS→OOS 训练流程**；embargo 也没意义 | [core/research/strategy_research.py:2347-2363](core/research/strategy_research.py) | 过拟合防护失效 |
| 9 | **DSR 高阶矩校正失效** | `validation_gate._deflated_sharpe_ratio` 中 skew/kurt 始终用默认 0/3，调用方从不传入实际样本统计量 | [core/research/validation_gate.py:75-77, 200-210](core/research/validation_gate.py) | 多重检验校正功能假装存在 |
| 10 | **ML 推理特征 0 填充** | `_align_features` 对缺失列填 0.0 → 训练时 dropna、推理时 RSI=0 等极端值，**静默 train/serve skew** | [core/ai/ml_signal.py:178-184](core/ai/ml_signal.py) | XGBoost 信号错误 |

---

## 2. 改进路线图（按优先级）

### P0 — 安全与资金（72 小时内）

#### 2.1 密钥轮换与机密管理
- [ ] 立即轮换：Binance / Gate / CoinGlass / GLM / NVIDIA / OpenAI / Polymarket 全部 key
- [ ] 删除 `keys.txt`、`config/coinglass_api_key.txt`、`config/fred_api_key.txt`，统一从 `.env` 读取
- [ ] `docker-compose.yml:15` 改用 Docker secrets 或 `env_file` + `environment:` 白名单注入，禁止全文挂载
- [ ] [docker-compose.yml:41-43](docker-compose.yml) Postgres / Redis / Grafana 默认弱密码改为环境变量随机生成；移除 `ports` 仅用内网
- [ ] [core/governance/rbac.py:85-86](core/governance/rbac.py) `hash_api_key` 从裸 SHA-256 改为 HMAC-SHA-256 或 argon2

#### 2.2 Web 暴露面收敛
- [ ] [web/main.py:1421-1427](web/main.py) CORS 改为显式白名单源列表，credentials 模式禁止使用 `*`
- [ ] [web/main.py:1576-1623](web/main.py) `/ws` 升级前校验 cookie/Ops token，非回环来源拒绝
- [ ] `/api/backtest/*`、`/api/ai/candidates/{id}/param-sensitivity` 全部加 `require_sensitive_ops_permissions` 依赖 + `asyncio.to_thread` 包裹同步回测
- [ ] [web/api/ai_research.py:4478-4524](web/api/ai_research.py) `decay-check` 从 GET 改为 POST（当前 GET 写入 metadata，违反 HTTP 语义+CSRF）
- [ ] [web/api/strategies.py:2010-2014](web/api/strategies.py) `/export/{name}` 加认证 + 过滤敏感字段
- [ ] [web/main.py:1626-1628](web/main.py) `/health` 重写为 `/livez`（静态 200）+ `/readyz`（检查 DB/Exchange/Strategy Manager）

#### 2.3 实盘订单安全
- [ ] [core/trading/order_manager.py:402-595](core/trading/order_manager.py) 实盘下单注入 `newClientOrderId = f"{strategy[:8]}-{ts_ms}-{seq}"`，并基于此做 in-memory dedup
- [ ] [core/risk/risk_manager.py:666-676](core/risk/risk_manager.py) `allow_close=True` 仅豁免 daily-loss-halt，杠杆/单笔比例仍校验
- [ ] [core/governance/decision_engine.py:130](core/governance/decision_engine.py) 杠杆检查补 `and not allow_close`，避免无法平仓
- [ ] [core/marketdata/ws_client.py:25-68](core/marketdata/ws_client.py) WS 客户端目前是 skeleton（`await sleep(1)` 死循环），在 live 路径上启用前必须实现或 `raise NotImplementedError`

---

### P1 — 正确性 Bug（2 周内）

#### 3.1 策略指标计算错误
- [ ] [strategies/quantitative/momentum.py:133-134](strategies/quantitative/momentum.py) `_calculate_adx` 中 `plus_dm` 就地修改后被 `(minus_dm > plus_dm)` 引用，ADX/+DI/-DI 系统性偏差。修法：先缓存原始 `up_move`/`down_move` 到新变量
- [ ] [strategies/technical/rsi_strategy.py:236-254](strategies/technical/rsi_strategy.py) `_find_peaks/_find_troughs` 使用 `rolling(center=True)` 含未来数据，回测/生产口径不一致
- [ ] [strategies/factor_based/factor_strategies.py:1305](strategies/factor_based/factor_strategies.py) `HurstExponentStrategy` 阈值 0.55/0.45 与 VR 实际中性值 1.0 错位，mean-revert 分支永远不进入
- [ ] [strategies/technical/common_strategies.py:296-350](strategies/technical/common_strategies.py) `VWAPReversionStrategy` 缺 SHORT 入场和上行偏离 SELL/CLOSE_SHORT 对称分支
- [ ] [strategies/factor_based/factor_strategies.py:1393](strategies/factor_based/factor_strategies.py) `VaRBreakoutStrategy` 使用 `var.iloc[-2]` 不必要再向前推 1 bar
- [ ] [strategies/quantitative/pairs_trading.py:92](strategies/quantitative/pairs_trading.py) `if min_hr >= 0 < max_hr` 链式比较+隐式覆写用户配置，改为显式 and
- [ ] [strategies/technical/common_strategies.py:144-145](strategies/technical/common_strategies.py) `StochasticStrategy` 入场条件 `cross_up and k_now <= oversold` 丢失绝大多数实际穿越，改为 `k_prev <= oversold and cross_up`
- [ ] [strategies/factor_based/factor_strategies.py:1180](strategies/factor_based/factor_strategies.py) `MeanReversionHalfLifeStrategy` 把 `mean` 当 take_profit 绝对价，BUY 时若 mean<current_price 会立即触发"止盈"
- [ ] [strategies/macro/market_sentiment.py:152-155](strategies/macro/market_sentiment.py), [fund_flow.py:166](strategies/macro/fund_flow.py) Signal 时间戳用采样时间而非 bar 时间，与冲突检测窗口错位

#### 3.2 回测与研究统计错误
- [ ] [core/backtest/backtest_engine.py:837-841](core/backtest/backtest_engine.py) `funding_pnl` 双计：funding 阶段和 close 阶段统计同笔费率，cost_decomposition 实际是 2× 真实值
- [ ] [core/backtest/backtest_engine.py:393-426](core/backtest/backtest_engine.py) `_check_position_exits` 当 gap-through 时 fill price 应该是 `min(stop_loss, open_price)` 而非 stop_loss 自身
- [ ] [core/backtest/backtest_engine.py:484-490](core/backtest/backtest_engine.py) `_execute_buy` 反向时只平不开，与策略 reversal 语义不一致
- [ ] [core/research/strategy_research.py:2347-2363](core/research/strategy_research.py) Walk-Forward 改为真正的 IS 训练 + OOS 评估流程
- [ ] [core/research/strategy_research.py:2841-2884](core/research/strategy_research.py) 评分用 `OOS×0.6 + Full×0.4`，Full data 包含 IS 段，污染最终决策；改为纯 OOS
- [ ] [core/research/strategy_research.py:2082-2089](core/research/strategy_research.py) `trade_cost = turnover * (fee + slip)` 在双边 turnover=2 时实际承担 4× 单边成本，需 `/2` 或文档化双边定义
- [ ] [core/research/strategy_research.py:2107](core/research/strategy_research.py) Sharpe 在 1s/5s 周期 `sqrt(31.5M)≈5615` 严重放大；限制最大 ann factor
- [ ] [core/research/validation_gate.py:75-77, 200-210](core/research/validation_gate.py) DSR 公式 skew/kurt 永远使用默认值，要么从 equity_curve_sample 估算后传入，要么删除假装存在的高阶矩校正
- [ ] [core/research/validation_gate.py:200](core/research/validation_gate.py) `n_trials_for_dsr` 应乘以平均参数试次数（实际 ~32），当前严重低估 multiple-testing 惩罚

#### 3.3 风控/会计正确性
- [ ] [core/accounting/pnl_decomposer.py:268, 90](core/accounting/pnl_decomposer.py) 反手平仓后 funding_pnl 未归档，老 funding 被计入新仓；统一 `datetime.now(timezone.utc)` 替换所有 naive 时间
- [ ] [core/trading/position_manager.py:537-549](core/trading/position_manager.py) `Ambiguous position close` 返回 None 静默失败，应抛 `AmbiguousPositionError`
- [ ] [core/trading/position_manager.py:584-586](core/trading/position_manager.py) `realized_pnl += unrealized_pnl` 是 gross 而非 net，与 risk_manager 的 net 口径不一致，导致 UI 显示混乱
- [ ] [core/risk/risk_manager.py:485-509](core/risk/risk_manager.py) 当 `_daily_trades=0` 时熔断豁免可被滥用（手动开仓爆仓不熔断），加灾难性后备阈值
- [x] [core/risk/circuit_breaker.py:481-526](core/risk/circuit_breaker.py) `_drawdown_from_pnl` fallback 到 `max_notional` 会让单笔大额亏损被低估（2026-05-24 已修：无 equity/capital 时不再用 raw notional 稀释大额亏损，小额无基准成本行仍保持 0）
- [ ] [core/monitoring/strategy_monitor.py:215-217](core/monitoring/strategy_monitor.py) CUSUM `reset_on_trigger=True` 后会立即再次触发，加 cooldown_bars

#### 3.4 ML / 新闻管线
- [ ] [core/ai/ml_signal.py:178-184](core/ai/ml_signal.py) 缺失特征列严格校验，缺失即返回 FLAT，不要填 0
- [ ] [core/ai/ml_signal.py:101-103](core/ai/ml_signal.py) `xgb.XGBClassifier.load_model` 前校验 manifest 中 `feature_set_version`/`feature_columns`
- [ ] [core/news/eventizer/llm_glm5.py:687-694](core/news/eventizer/llm_glm5.py) `_extract_json_block` 用 `re.search(r"```(?:json)?\s*([\s\S]+?)```", raw)` 替代当前易碎的截取
- [ ] [core/news/eventizer/llm_glm5.py:1019-1020, 985](core/news/eventizer/llm_glm5.py) `choices[0]` 可能 IndexError；timeout 在 failover 链上实际墙钟为 3× 单倍 timeout，外层 `wait_for` 需匹配
- [ ] [core/news/storage/db.py:1127-1142, 1416-1422](core/news/storage/db.py) `existing_recent_rows` 全表扫无 LIMIT，11000+ 行已可观察延迟，加 LIMIT + published_at 索引
- [ ] [core/news/storage/db.py:1149](core/news/storage/db.py) URL 去重未做 normalize，相同 URL 不同 utm 参数绕过去重
- [ ] [core/news/service/worker.py:519-527](core/news/service/worker.py) `CancelledError` 被外层 `except Exception` 当普通错误，应穿透传播
- [ ] [core/news/collectors/manager.py:298](core/news/collectors/manager.py) ThreadPoolExecutor 内每次新建/销毁 event loop + aiohttp Session，浪费连接

---

### P2 — 统计严谨性与性能（1 个月内）

#### 4.1 性能优化
- [ ] [strategies/factor_based/factor_strategies.py:1042, 1390, 1549, 1724, 1765](strategies/factor_based/factor_strategies.py) 多处 `rolling.apply(lambda)` 向量化：VaR 用 `quantile`、MAD 用复合滚动均值、Sortino 改 numpy
- [ ] [core/strategies/strategy_base.py:166-176](core/strategies/strategy_base.py) `_wrapped_generate` 中无条件计算 ATR，对 32+ 策略累计开销大，仅在产出 entry 信号时再算
- [ ] [strategies/factor_based/factor_strategies.py:660-685, 762-790](strategies/factor_based/factor_strategies.py) MFI/VWAP/OBV `check_exit` 重算指标，加缓存 `self._last_indicators[symbol]`
- [ ] [core/research/strategy_research.py:1999-2030](core/research/strategy_research.py) MLXGBoost Booster 每 fold 重新 load_model，加 `lru_cache`
- [ ] [strategies/arbitrage/cex_arbitrage.py:65-77](strategies/arbitrage/cex_arbitrage.py) 串行 `await get_ticker` 改 `asyncio.gather`
- [ ] [strategies/macro/market_sentiment.py:81-94](strategies/macro/market_sentiment.py) Fear&Greed / Trending 加 5 分钟 TTL 缓存
- [ ] [core/ai/research_planner.py:397-527](core/ai/research_planner.py) Google Trends / Macro / Glassnode 等同步数据源包 `asyncio.to_thread`

#### 4.2 并发与状态
- [ ] [core/research/orchestrator.py:1306, 1323](core/research/orchestrator.py) `asyncio.TimeoutError` 不是 `Exception` 子类，会跨过 `except Exception` 让整个 job 标 failed；加 `except (Exception, asyncio.TimeoutError)`
- [ ] [core/research/orchestrator.py:1402](core/research/orchestrator.py) `save_batch` 替代逐 candidate 落盘，避免 N 次完整 JSON 重写
- [ ] [core/research/experiment_registry.py:44](core/research/experiment_registry.py) `threading.RLock` 在 async 单线程下不互斥，改 `asyncio.Lock` 或 `run_in_executor`
- [ ] [core/research/orchestrator.py:1246-1251](core/research/orchestrator.py) 并发研究 job finalize 阶段无锁，可能让相关性高的策略都通过过滤
- [ ] [core/news/eventizer/rate_limiter.py:29-35, 101-104](core/news/eventizer/rate_limiter.py) singleton `__init__` 静默丢弃新参数；asyncio.Lock 懒创建并发不安全
- [ ] [core/news/storage/db.py:1709-1721](core/news/storage/db.py) `_global_rate_limit_backoff` 进程级，多 worker 部署无效；持久化到 DB
- [ ] [core/trading/binance_rest.py:137](core/trading/binance_rest.py) `_BINANCE_TIME_OFFSET_MS` 全局可变 dict 无锁，加 `asyncio.Lock`
- [ ] [core/execution/rate_limit_and_reconnect.py:152-181](core/execution/rate_limit_and_reconnect.py) sync `acquire` 用 `time.sleep` 会阻塞事件循环，加断言或禁用

#### 4.3 后台 worker / Lifespan
- [ ] [web/main.py:1140-1166](web/main.py) `_data_maintenance_worker` 加 per-task `asyncio.wait_for`
- [ ] [web/main.py:846-883](web/main.py) `_google_trends_worker` 等数据 worker 在 ImportError 后停止重试
- [ ] [web/main.py:1287-1295](web/main.py) lifespan shutdown 串行 await 加总超时，避免 hang
- [ ] [core/research/orchestrator.py:267-275](core/research/orchestrator.py) `_recover_stale_jobs_on_startup` 需要先清理 `research_job_tasks` 字典

#### 4.4 数据采集与 DEX/CEX
- [ ] [core/news/collectors/jin10.py:82](core/news/collectors/jin10.py), [newsapi.py:90-91](core/news/collectors/newsapi.py) 缺重试/backoff；NewsAPI 不读 Retry-After
- [ ] [core/news/collectors/rss.py:94](core/news/collectors/rss.py) XML 解析考虑使用 `defusedxml` 显式禁用外部实体
- [ ] [core/data/news_collector.py:78-79](core/data/news_collector.py) `bullish_keywords` 含前导空格的 `" adoption"` 永远不命中
- [ ] [core/realtime/event_bus.py:62-66](core/realtime/event_bus.py) full 队列 `get_nowait` 异常误判 stale 驱逐 active subscriber

---

### P3 — 代码质量与测试覆盖（持续）

#### 5.1 测试
- [ ] [tests/test_strategies.py](tests/test_strategies.py) 仅 20 个测试覆盖核心策略，多为浅断言（`isinstance(list)`）；至少补：方向对称性、min_bars 边界、NaN/Inf 输入、check_exit 持仓方向
- [ ] 缺失测试的策略：`BollingerSqueezeStrategy`、`MACDHistogramStrategy`、`RSIDivergenceStrategy`、`HurstExponentStrategy`、`SortinoRatioStrategy`、`VaRBreakoutStrategy`、`MaxDrawdownStrategy`、`OrderFlowImbalanceStrategy`、`TradeIntensityStrategy`、`SocialSentimentStrategy`、`MLXGBoostStrategy`、DEX/CEX Arbitrage、`SupplyEventStrategy`
- [ ] [pytest.ini](pytest.ini) 加 `asyncio_mode = auto`、`--strict-markers`、`--tb=short`、`filterwarnings`、timeout 插件
- [ ] [tests/conftest.py:43-44](tests/conftest.py) 不要直接写 `_trade_history_store_root` 私有属性，提供 `RiskManager.configure_storage(path)`
- [ ] [tests/test_funding_rate.py:33,40,52](tests/test_funding_rate.py) 用固定 `datetime(2024,1,1)` 替代 `datetime.now()`，避免午夜跨边界 flaky

#### 5.2 基础设施
- [ ] [Dockerfile](Dockerfile) 加 `USER nonroot`、`HEALTHCHECK`、多阶段构建、扩展 `.dockerignore`
- [ ] [requirements.txt](requirements.txt) 引入 lockfile（pip-tools / uv）；解锁 numpy/pyarrow 上限；DEX 依赖移到 extras
- [ ] [config/database.py](config/database.py) 29 处 `default=datetime.utcnow` 改 `default=lambda: datetime.now(timezone.utc)`（Py3.12+ deprecated）
- [ ] Python 版本对齐：本机 3.13.7 / Dockerfile 3.11 / README 3.11 — 统一一份声明
- [ ] [logs/](logs/) 日志切割：ops_audit.jsonl 按大小切片 + 压缩；uvicorn stdout/stderr 纳入 Loguru rotation
- [ ] [scripts/cleanup_repo.ps1](scripts/cleanup_repo.ps1) `LogRetentionDays=7` 与 `main.py` 的 `LOG_RETENTION=30 days` 不一致

#### 5.3 前端
- [ ] [web/static/js/ai_research.js:288-360](web/static/js/ai_research.js) `normalizeUiText` 内嵌乱码→中文映射是 UTF-8 mojibake 修复 hack，找上游编码 bug 删除
- [ ] [web/static/js/ai_research.js:5460-5470, 6080-6128](web/static/js/ai_research.js) `state.jobPollingTimers` 在 stopPolling 中未清理；加 maxAttempts 上限
- [ ] [web/static/js/research_workbench.js:1664-1674](web/static/js/research_workbench.js) `startWorkbenchAutoRefresh` 隐藏 tab 时清理 timer
- [ ] [web/templates/index.html:24-25](web/templates/index.html) 第三方 CDN 脚本加 SRI integrity 校验或本地化

#### 5.4 清洁与文档
- [ ] 仓库根清理：`MagicMock/`、`.pytest_tmp_broken/`、`fix_mojibake.py`、`_once.ps1`、`report_exit_refactor.md`、`refresh_research_universe.bat` 移入 `scripts/legacy/`
- [ ] `CLAUDE.md` 60KB 是"任务清单"非规范文档，移到 `docs/plans/`
- [ ] [docker-compose.yml:58-65](docker-compose.yml) Prometheus 引用不存在的 `./prometheus.yml`，profile 启动即失败
- [ ] 抽公共工具：`oscillator_entry_strength`、`position_field_extractor`、`adx_indicator`（多策略重复 ~50 行）
- [ ] 多文件未使用 import 清理（`from datetime import datetime, timezone` 但被 `bar_time` 替代后忘删）

---

## 3. 各模块审查总评（来自 6 个代理）

### 策略模块
> 结构清晰、`StrategyBase` 接口统一，但具体策略实现质量参差不齐：振荡器/因子类大量使用 `rolling.apply` lambda；多处"看似双向但仅单向"（VWAPReversion、MaxDrawdown、BollingerMeanReversion close-only）；TrendFollowing ADX 自引用、RSIDivergence 未来泄漏、HurstExponent 阈值错位是 3 个真实统计 bug。**最关键改进**：① 修这 3 个 bug ② 抽公共工具减少 ~30% 重复 ③ 单元测试覆盖率从 ~25% 提到接近 100%。

### 回测与研究
> 覆盖了 DSR、purged WF、相关性过滤、IS/OOS、人工审批等行业标准元素，但**实现层有关键正确性漏洞**：funding 双计、所谓 WF 实际无 IS 训练、DSR skew/kurt 永远失效、Sharpe 秒级周期年化爆表、回测填单无 gap 处理、含 IS 的 Full Sharpe 占 40% 评分污染。`threading.RLock` 在 async 框架下不互斥。**优先级最高**：funding 双计 + walk-forward 实现 + Sharpe annualization。

### 新闻/ML/AI 管线
> LLM failover、provider backoff、持久化方面工程化程度高，但仍有两类系统性问题：① **ML 训练-推理特征对齐缺乏强校验**（0 填充、版本字符串硬编码、无 manifest 校验）；② 新闻去重和 DB 扫描存在全表 O(n) 模式（11000+ 条已可观察延迟）。建议优先处理 ML feature alignment + DB 全表扫描。

### 风控/执行/会计
> 设计良好（熔断 + scope 隔离 + governance 双层 + FIFO PnL），但存在多处"豁免路径绕过检查"：`allow_close=True` 跳过过多检查、实盘订单缺 clientOrderId、PnL 反手 funding 串账、CUSUM 重置后报警风暴。`ws_client.py` 是 skeleton 不应在 live 启用。**优先级**：① clientOrderId ② 收紧 allow_close ③ 修 PnL funding 反手归档与 naive datetime。

### Web 层
> 架构良好（lifespan 编排、RuntimeTaskSupervisor、shared polling），但**暴露面安全控制不均衡**：高 CPU 的 `/api/backtest/*` 未加权限最严重；WS 完全裸奔次之；CORS `*` + credentials 配置错误；若干同步 CPU 调用未 `to_thread`、GET 带副作用、metadata 大小未校验。前端 JS 单文件 6656 行 + 大量内嵌 mojibake 映射表显示重构债务沉重。

### 基础设施/测试/配置
> 当前最严峻：**生产 API 密钥实际存在于 keys.txt/.env/.env.local 三份明文文件**，虽 `.gitignore` 阻止入库但本机磁盘/备份风险已现实存在，必须立即轮换。其次 Dockerfile root + 无 healthcheck + docker-compose 弱密码 + `/health` 形同虚设。核心策略只有 20 条测试且多浅断言，与系统 ~18686 行规模严重失衡。

---

## 4. 建议执行节奏

| 周次 | 任务 |
|------|------|
| 第 1 周 | P0 全部（密钥轮换 / Web 暴露面 / clientOrderId / allow_close） |
| 第 2-3 周 | P1 策略指标 Bug + 回测 funding 双计 + WF 重写 + ML 特征校验 |
| 第 4-6 周 | P1 风控/会计修正 + DSR / Sharpe 统计修复 + 性能优化第一批 |
| 第 7-8 周 | P2 并发与状态 + worker 健壮性 |
| 持续 | P3 测试补全（每周新增 10 测试）+ 基础设施加固 + 前端 mojibake 清理 |

---

## 5. 验收建议

每项修复完成后需要：
1. 加单元/集成测试覆盖触发场景
2. 对回测/统计类修复，跑一次对比报告：旧版 vs 新版的 Sharpe / DSR / cost_breakdown 数值
3. 安全类修复加渗透测试用例（curl 验证 401/403）
4. 性能优化记录修复前后基准（pytest-benchmark 或 timeit）

完整 ~160 项原始发现（含全部 file:line）保存在审查代理输出 transcripts 中，如需细化某一项可单独展开。
