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
- [x] [core/governance/rbac.py:85-86](core/governance/rbac.py) `hash_api_key` 从裸 SHA-256 改为 HMAC-SHA-256 或 argon2（2026-07-16 已复核：`hash_api_key` 使用 HMAC-SHA256 + `RBAC_SECRET` pepper，API user role/key 校验覆盖 `tests/governance/test_api_user_security.py`）

#### 2.2 Web 暴露面收敛
- [x] [web/main.py:1421-1427](web/main.py) CORS 改为显式白名单源列表，credentials 模式禁止使用 `*`（2026-07-16 已复核：`_allowed_origins` 过滤 `*` 并提供本地默认白名单，回归覆盖 `tests/test_web_main_runtime_tasks.py::test_cors_does_not_allow_wildcard_with_credentials`）
- [x] [web/main.py:1576-1623](web/main.py) `/ws` 升级前校验 cookie/Ops token，非回环来源拒绝（2026-07-16 已复核：WS 需要白名单 Origin + 有效本地会话或 Ops token，无凭证 loopback 也拒绝，回归覆盖 `tests/test_web_main_runtime_tasks.py::test_websocket_rejects_loopback_without_credentials` 等）
- [x] `/api/backtest/*`、`/api/ai/candidates/{id}/param-sensitivity` 全部加 `require_sensitive_ops_permissions` 依赖 + `asyncio.to_thread` 包裹同步回测（2026-07-18 已复核：backtest router 敏感路由均声明权限依赖，param-sensitivity 使用 `manage_ai_research` 并通过 `asyncio.to_thread` 运行同步回测，回归覆盖 `tests/test_sensitive_api_auth.py::test_backtest_sensitive_routes_are_dependency_gated` 与 `tests/test_sensitive_api_auth.py::test_ai_candidate_mutation_routes_are_dependency_gated_and_decay_get_is_read_only`）
- [x] [web/api/ai_research.py:4478-4524](web/api/ai_research.py) `decay-check` 从 GET 改为 POST（当前 GET 写入 metadata，违反 HTTP 语义+CSRF）（2026-07-18 已复核：POST 持久化检查结果，GET 保留为只读兼容路径且不写 metadata，UI 使用 POST；回归覆盖 `tests/test_ai_research_autonomous_agent_api.py::test_candidate_decay_check_get_is_read_only_and_post_persists` 与 `tests/test_ai_research_runtime_and_phase_e.py::test_candidate_decay_check_button_uses_post_route`）
- [x] [web/api/strategies.py:2010-2014](web/api/strategies.py) `/export/{name}` 加认证 + 过滤敏感字段（2026-07-18 已复核：`/export/{name}` 与 `/export` 均需要 `manage_strategies`，导出 payload 递归脱敏密钥字段，回归覆盖 `tests/test_sensitive_api_auth.py::test_strategy_export_routes_are_dependency_gated_and_sanitized`）
- [x] [web/main.py:1626-1628](web/main.py) `/health` 重写为 `/livez`（静态 200）+ `/readyz`（检查 DB/Exchange/Strategy Manager）（2026-07-16 已复核：`/livez` 为轻量 liveness，`/readyz` 检查 DB、Exchange、Strategy Manager 和 runtime freshness；`/health` 保留为兼容别名，回归覆盖 `tests/test_web_main_runtime_tasks.py::test_livez_and_health_are_lightweight_liveness_aliases`）

#### 2.3 实盘订单安全
- [x] [core/trading/order_manager.py:402-595](core/trading/order_manager.py) 实盘下单注入 `newClientOrderId = f"{strategy[:8]}-{ts_ms}-{seq}"`，并基于此做 in-memory dedup（2026-07-22 已复核：`OrderManager._allocate_client_order_id` 为实盘提交注入 `newClientOrderId`/`clientOrderId`，60s TTL 内存去重，提交超时等 ambiguous 错误保留 ID 防盲重试；回归覆盖 `tests/test_order_manager_safety.py::test_real_order_reuses_generated_client_order_id_for_request_retry`、`test_ambiguous_timeout_holds_client_order_id_and_blocks_blind_retry`、`test_binance_fast_path_ambiguous_error_skips_ccxt_fallback`）
- [x] [core/risk/risk_manager.py:899-971](core/risk/risk_manager.py) `allow_close=True` 只豁免 daily-loss-halt 类熔断；杠杆/单笔/总敞口等 entry-only cap 不阻断减仓退出（2026-08-05 已复核：非 daily-loss halt 仍阻断，daily-loss halt 下 close 可通过；回归覆盖 `tests/test_trade_risk_ml_fixes.py::test_allow_close_bypasses_entry_only_caps_during_daily_loss_halt` 与 `::test_allow_close_does_not_bypass_non_daily_halt_but_bypasses_position_count`）
- [x] [core/governance/decision_engine.py:130](core/governance/decision_engine.py) 杠杆检查补 `and not allow_close`，避免无法平仓（2026-07-17 已复核：`DecisionEngine.evaluate_order_intent` 对 leverage / notional / trade-risk / spread / stale / universe gates 均保留 `not allow_close`，回归覆盖 `tests/governance/test_governance_gates.py::test_decision_gate_allows_close_despite_leverage_cap`）
- [x] [core/marketdata/ws_client.py:25-68](core/marketdata/ws_client.py) WS 客户端目前是 skeleton（`await sleep(1)` 死循环），在 live 路径上启用前必须实现或 `raise NotImplementedError`（2026-06-29 已改为显式 skeleton/`NotImplementedError` 防护）

---

### P1 — 正确性 Bug（2 周内）

#### 3.1 策略指标计算错误
- [x] [strategies/quantitative/momentum.py:133-134](strategies/quantitative/momentum.py) `_calculate_adx` 中 `plus_dm` 就地修改后被 `(minus_dm > plus_dm)` 引用，ADX/+DI/-DI 系统性偏差。修法：先缓存原始 `up_move`/`down_move` 到新变量（2026-07-02 复核：已委托 `core.indicators.sma_adx`，由 `tests/test_strategy_bug_fixes.py::TestADXSelfReference` 覆盖）
- [x] [strategies/technical/rsi_strategy.py:236-254](strategies/technical/rsi_strategy.py) `_find_peaks/_find_troughs` 使用 `rolling(center=True)` 含未来数据，回测/生产口径不一致（2026-07-02 复核：已改为因果 extrema，`TestRSIDivergenceCausalExtrema` 覆盖）
- [x] [strategies/factor_based/factor_strategies.py:1305](strategies/factor_based/factor_strategies.py) `HurstExponentStrategy` 阈值 0.55/0.45 与 VR 实际中性值 1.0 错位，mean-revert 分支永远不进入（2026-07-02 复核：默认阈值已回到 VR=1.0 尺度，`TestHurstThresholds` 覆盖）
- [x] [strategies/technical/common_strategies.py:296-350](strategies/technical/common_strategies.py) `VWAPReversionStrategy` 缺 SHORT 入场和上行偏离 SELL/CLOSE_SHORT 对称分支（2026-07-02 复核：SELL/CLOSE_SHORT 对称分支已落地，`TestVWAPReversionSymmetry` 覆盖）
- [x] [strategies/factor_based/factor_strategies.py:1393](strategies/factor_based/factor_strategies.py) `VaRBreakoutStrategy` 使用 `var.iloc[-2]` 不必要再向前推 1 bar（2026-07-02 复核：信号使用最新 VaR，`TestVaRBreakoutIndex` 覆盖）
- [x] [strategies/quantitative/pairs_trading.py:92](strategies/quantitative/pairs_trading.py), [web/api/backtest.py:1353](web/api/backtest.py) hedge-ratio 下界不再因链式比较或 `0.0` 默认值回退而隐式改写用户配置（2026-06-19 已修复并补回归测试）
- [x] [strategies/technical/common_strategies.py:144-145](strategies/technical/common_strategies.py) `StochasticStrategy` 入场条件 `cross_up and k_now <= oversold` 丢失绝大多数实际穿越，改为 `k_prev <= oversold and cross_up`（2026-07-02 复核：BUY/SELL 均使用前一根 K 线阈值窗口，`TestStochasticEntryWindow` 覆盖）
- [x] [strategies/factor_based/factor_strategies.py:1180](strategies/factor_based/factor_strategies.py) `MeanReversionHalfLifeStrategy` 把 `mean` 当 take_profit 绝对价，BUY 时若 mean<current_price 会立即触发"止盈"（2026-07-02 复核：BUY take_profit 已保证高于入场价，`TestMeanReversionHalfLifeTPGuard` 覆盖）
- [x] [strategies/macro/market_sentiment.py:152-155](strategies/macro/market_sentiment.py), [fund_flow.py:166](strategies/macro/fund_flow.py) Signal 时间戳用采样时间而非 bar 时间，与冲突检测窗口错位（2026-07-02 复核：信号时间使用 bar timestamp，`TestMacroSignalTimestamps` 覆盖）

#### 3.2 回测与研究统计错误
- [x] [core/backtest/backtest_engine.py:837-841](core/backtest/backtest_engine.py) `funding_pnl` 双计：funding 阶段和 close 阶段统计同笔费率，cost_decomposition 实际是 2× 真实值（2026-07-16 已复核：funding 阶段作为 attribution，close 阶段扣除重复 funding，回归覆盖 `tests/test_backtest_cost_models.py::test_backtest_cost_breakdown_does_not_double_count_funding`）
- [x] [core/backtest/backtest_engine.py:393-426](core/backtest/backtest_engine.py) `_check_position_exits` 当 gap-through 时 fill price 应该是 `min(stop_loss, open_price)` 而非 stop_loss 自身（2026-07-16 已复核：stop gap-through 使用开盘价保守成交，回归覆盖 `tests/test_backtest_engine_protective_exits.py::test_backtest_stop_loss_gap_through_fills_at_open`）
- [x] [core/backtest/backtest_engine.py:484-490](core/backtest/backtest_engine.py) `_execute_buy` 反向时只平不开，与策略 reversal 语义不一致（2026-07-16 已复核：反向信号 close 后继续 open 新方向，回归覆盖 `tests/test_backtest_engine_protective_exits.py::test_backtest_opposite_buy_closes_short_and_opens_long`）
- [x] [core/research/strategy_research.py:2347-2363](core/research/strategy_research.py) Walk-Forward 改为真正的 IS 训练 + OOS 评估流程（2026-08-04 复核：`_run_purged_walk_forward` 在有 `param_grid` 时按 fold 做 IS 参数优化后仅在 OOS slice 评估，并保留 embargo；回归覆盖 `tests/test_ai_research_phase2.py::test_purged_walk_forward_returns_dict` 与 `::test_purged_wf_consistency_range`）
- [x] [core/research/strategy_research.py:2841-2884](core/research/strategy_research.py) 评分用 `OOS×0.6 + Full×0.4`，Full data 包含 IS 段，污染最终决策；改为纯 OOS（2026-08-04 复核：候选 score 在 `oos_metrics` 可用时只用 OOS `_compute_score`，无 OOS 时对 full-data score 施加 0.7 惩罚；验证门也以 OOS 作为 effective Sharpe，回归覆盖 `tests/test_research_validation_gate.py::test_validation_gate_failing_oos_never_reaches_paper_or_live`）
- [x] [core/research/strategy_research.py:2082-2089](core/research/strategy_research.py) `trade_cost = turnover * (fee + slip)` 在双边 turnover=2 时实际承担 4× 单边成本，需 `/2` 或文档化双边定义（2026-08-04 复核：代码文档化 `commission_rate` / `slippage_bps` 为 per-side 费率，`turnover=2` 表示 entry+exit 两次成交，`turnover * total_cost_rate` 对应完整往返成本）
- [x] [core/research/strategy_research.py:2107](core/research/strategy_research.py) Sharpe 在 1s/5s 周期 `sqrt(31.5M)≈5615` 严重放大；限制最大 ann factor（2026-08-04 复核：研究回测 Sharpe 使用 `_SHARPE_ANN_CAP = 252 * 24` 上限，避免秒级周期噪声被过度年化）
- [x] [core/research/validation_gate.py:75-77, 200-210](core/research/validation_gate.py) DSR 公式 skew/kurt 永远使用默认值，要么从 equity_curve_sample 估算后传入，要么删除假装存在的高阶矩校正（2026-08-04 复核：`build_validation_summary_from_research_result` 从 `equity_curve_sample` 估算 skew/kurt，缺样本时使用 crypto heavy-tail prior；回归覆盖 `tests/test_research_validation_gate.py::test_validation_gate_dsr_uses_equity_curve_moments` 与 `tests/test_validation_dsr_kurtosis.py`）
- [x] [core/research/validation_gate.py:200](core/research/validation_gate.py) `n_trials_for_dsr` 应乘以平均参数试次数（实际 ~32），当前严重低估 multiple-testing 惩罚（2026-08-04 复核：`n_trials_for_dsr = runs * optimization_trials`，并从 batch results 估算平均优化次数；回归覆盖 `tests/test_research_validation_gate.py::test_validation_gate_dsr_trials_include_optimization_trials` 与 `::test_validation_gate_dsr_trials_use_average_batch_optimization_trials`）

#### 3.3 风控/会计正确性
- [ ] [core/accounting/pnl_decomposer.py:268, 90](core/accounting/pnl_decomposer.py) 反手平仓后 funding_pnl 未归档，老 funding 被计入新仓；统一 `datetime.now(timezone.utc)` 替换所有 naive 时间
- [x] [core/trading/position_manager.py:537-549](core/trading/position_manager.py) `Ambiguous position close` 返回 None 静默失败，应抛 `AmbiguousPositionError`（2026-07-15 已复核：`PositionManager.close_position` 抛出 `AmbiguousPositionError`，回归覆盖 `tests/core/test_runtime_persistence.py`）
- [ ] [core/trading/position_manager.py:584-586](core/trading/position_manager.py) `realized_pnl += unrealized_pnl` 是 gross 而非 net，与 risk_manager 的 net 口径不一致，导致 UI 显示混乱
- [x] [core/risk/risk_manager.py:485-509](core/risk/risk_manager.py) 当 `_daily_trades=0` 时熔断豁免可被滥用（手动开仓爆仓不熔断），加灾难性后备阈值（2026-07-22 已复核：`_evaluate_daily_stop` 使用 system stop basis，只有 live/零成交/零已实现/无系统持仓且未达 catastrophic 时才豁免，达到 2× 日损阈值仍强制计入熔断；回归覆盖 `tests/test_live_risk_pnl_accounting.py::test_risk_manager_does_not_halt_on_external_equity_drop_when_system_pnl_flat` 与 `test_risk_manager_catastrophic_backstop_halts_zero_trade_live_loss`）
- [x] [core/risk/circuit_breaker.py:481-526](core/risk/circuit_breaker.py) `_drawdown_from_pnl` fallback 到 `max_notional` 会让单笔大额亏损被低估（2026-05-24 已修：无 equity/capital 时不再用 raw notional 稀释大额亏损，小额无基准成本行仍保持 0）
- [x] [core/monitoring/strategy_monitor.py:215-217](core/monitoring/strategy_monitor.py) CUSUM `reset_on_trigger=True` 后会立即再次触发，加 cooldown_bars（2026-07-22 已复核：`CUSUMMonitor` 暴露 `cooldown_bars` 并维护 `_cooldown_until_n`/`cooldown_bars_remaining`，reset 后不会在下一根 bar 立即重复触发；回归覆盖 `tests/test_strategy_monitor_robust_std.py::test_stateful_cusum_monitor_suppresses_immediate_retrigger_during_cooldown`）

#### 3.4 ML / 新闻管线
- [x] [core/ai/ml_signal.py:178-184](core/ai/ml_signal.py) 缺失特征列严格校验，缺失即返回 FLAT，不要填 0（2026-07-16 已复核：`_align_features` 对缺失/NaN 特征抛错并由 `predict` fail-closed 为 FLAT，回归覆盖 `tests/test_trade_risk_ml_fixes.py::test_ml_signal_missing_feature_returns_flat_without_zero_fill`）
- [x] [core/ai/ml_signal.py:101-103](core/ai/ml_signal.py) `xgb.XGBClassifier.load_model` 前校验 manifest 中 `feature_set_version`/`feature_columns`（2026-07-16 已复核：加载前强制校验 manifest 版本和列集合，回归覆盖 `tests/test_trade_risk_ml_fixes.py::test_ml_signal_load_rejects_manifest_feature_mismatch`）
- [x] [core/news/eventizer/llm_glm5.py:687-694](core/news/eventizer/llm_glm5.py) `_extract_json_block` 用 `re.search(r"```(?:json)?\s*([\s\S]+?)```", raw)` 替代当前易碎的截取（2026-06-29 已落地并由 `tests/test_openai_responses_migration.py` 覆盖）
- [x] [core/news/eventizer/llm_glm5.py:1019-1020, 985](core/news/eventizer/llm_glm5.py) `choices[0]` 可能 IndexError；timeout 在 failover 链上实际墙钟为 3× 单倍 timeout，外层 `wait_for` 需匹配（2026-06-29 已补空 choices 防护并加 failover 墙钟预算）
- [x] [core/news/storage/db.py:1127-1142, 1416-1422](core/news/storage/db.py) `existing_recent_rows` 全表扫无 LIMIT，11000+ 行已可观察延迟，加 LIMIT + published_at 索引（2026-07-01 复核：`_EXISTING_RECENT_SCAN_LIMIT` 已落地，查询按 `published_at DESC` 限流，`NewsRaw.published_at` 与 `ix_news_raw_source_published` 已建索引）
- [x] [core/news/storage/db.py:1149](core/news/storage/db.py) URL 去重未做 normalize，相同 URL 不同 utm 参数绕过去重（2026-07-01 复核：`_normalize_url_for_dedup()` 已统一去掉 `utm_*`/`fbclid`/`gclid`/`spm`，并由 `tests/test_news_storage_llm_tasks.py` 覆盖 tracking-param 变体）
- [x] [core/news/service/worker.py:519-527](core/news/service/worker.py) `CancelledError` 被外层 `except Exception` 当普通错误，应穿透传播（2026-06-29 已显式 `except asyncio.CancelledError: raise`）
- [ ] [core/news/collectors/manager.py:298](core/news/collectors/manager.py) ThreadPoolExecutor 内每次新建/销毁 event loop + aiohttp Session，浪费连接

---

### P2 — 统计严谨性与性能（1 个月内）

#### 4.1 性能优化
- [ ] [strategies/factor_based/factor_strategies.py:1042, 1390, 1549, 1724, 1765](strategies/factor_based/factor_strategies.py) 多处 `rolling.apply(lambda)` 向量化：VaR 用 `quantile`、MAD 用复合滚动均值、Sortino 改 numpy
- [ ] [core/strategies/strategy_base.py:166-176](core/strategies/strategy_base.py) `_wrapped_generate` 中无条件计算 ATR，对 32+ 策略累计开销大，仅在产出 entry 信号时再算
- [ ] [strategies/factor_based/factor_strategies.py:660-685, 762-790](strategies/factor_based/factor_strategies.py) MFI/VWAP/OBV `check_exit` 重算指标，加缓存 `self._last_indicators[symbol]`
- [x] [core/research/strategy_research.py:1999-2030](core/research/strategy_research.py) MLXGBoost Booster 每 fold 重新 load_model，加 `lru_cache`（2026-07-28 复核：`_load_xgb_booster_cached` 按 resolved path + mtime 缓存并发安全复用 Booster；回归覆盖 `tests/test_perf_fixes_round2.py::test_xgb_booster_cache_returns_same_instance`、`test_xgb_booster_cache_invalidates_on_mtime_change`、`test_xgb_booster_cache_concurrent_miss_returns_one_cached_instance`）
- [x] [strategies/arbitrage/cex_arbitrage.py:65-77](strategies/arbitrage/cex_arbitrage.py) 串行 `await get_ticker` 改 `asyncio.gather`（2026-07-28 复核：`update_prices` 对已连接交易所并发调用 `get_realtime_price(..., allow_rest_fallback=True)` 并用 `return_exceptions=True` 隔离单交易所失败；回归覆盖 `tests/test_perf_fixes_round2.py::test_cex_arbitrage_update_prices_runs_concurrent`、`test_cex_arbitrage_isolates_single_exchange_failure`）
- [x] [strategies/macro/market_sentiment.py:81-94](strategies/macro/market_sentiment.py) Fear&Greed / Trending 加 5 分钟 TTL 缓存（2026-07-29 已修复：`MarketSentimentStrategy._fetch_fear_greed_index` 和 `SocialSentimentStrategy._fetch_trending_proxy` 默认使用 300s TTL 缓存，TTL=0 可禁用；回归覆盖 `tests/test_strategy_signal_regressions.py::test_market_sentiment_fear_greed_fetch_uses_ttl_cache` 与 `test_social_sentiment_trending_fetch_uses_ttl_cache`）
- [ ] [core/ai/research_planner.py:397-527](core/ai/research_planner.py) Google Trends / Macro / Glassnode 等同步数据源包 `asyncio.to_thread`

#### 4.2 并发与状态
- [x] [core/research/orchestrator.py:1306, 1323](core/research/orchestrator.py) `asyncio.TimeoutError` 不是 `Exception` 子类，会跨过 `except Exception` 让整个 job 标 failed；加 `except (Exception, asyncio.TimeoutError)`（2026-08-06 复核：当前 Python 中 `asyncio.TimeoutError is TimeoutError` 且是 `Exception` 子类；LLM rationale timeout 已显式捕获并取消子任务，回归覆盖 `tests/test_orchestrator_llm_rationale_timeout.py`）
- [x] [core/research/orchestrator.py:1402](core/research/orchestrator.py) `save_batch` 替代逐 candidate 落盘，避免 N 次完整 JSON 重写（2026-08-06 复核：`CandidateRegistry.save_many()` 单次 flush 批量写入候选，orchestrator 在候选初始可见和 rationale metadata 更新后均批量保存；批量 flush 覆盖 `tests/test_experiment_registry_windows_concurrent.py::test_registry_save_many_flushes_once`）
- [x] [core/research/experiment_registry.py:44](core/research/experiment_registry.py) `threading.RLock` 在 async 单线程下不互斥，改 `asyncio.Lock` 或 `run_in_executor`（2026-08-06 复核：registry 临界区是同步代码且无 `await`，并会从 FastAPI loop 与 `asyncio.to_thread` worker 两类上下文调用；代码注释已记录继续使用 `threading.RLock` 的线程安全边界）
- [x] [core/research/orchestrator.py:1246-1251](core/research/orchestrator.py) 并发研究 job finalize 阶段无锁，可能让相关性高的策略都通过过滤（2026-08-06 复核：`_research_finalize_lock(app)` 包裹 existing active candidate 读取、相关性过滤与初始候选批量落盘，避免并发 finalize 同时通过过滤）
- [x] [core/news/eventizer/rate_limiter.py:29-35, 101-104](core/news/eventizer/rate_limiter.py) singleton `__init__` 静默丢弃新参数；asyncio.Lock 懒创建并发不安全（2026-07-19 已复核：singleton conflicting re-init 会显式 warning 并保留原配置，token lock 初始化路径已有保护；回归覆盖 `tests/test_rate_limiter.py::test_conflicting_singleton_reinit_warns_and_keeps_original` 与既有 async acquire 测试）
- [ ] [core/news/storage/db.py:1709-1721](core/news/storage/db.py) `_global_rate_limit_backoff` 进程级，多 worker 部署无效；持久化到 DB
- [x] [core/trading/binance_rest.py:137](core/trading/binance_rest.py) `_BINANCE_TIME_OFFSET_MS` 全局可变 dict 无锁，加 `asyncio.Lock`（2026-07-15 已复核：`_refresh_binance_time_offset` 使用共享 async lock，回归覆盖 `tests/test_binance_rest_proxy.py::test_binance_signed_request_serializes_time_offset_refresh`）
- [x] [core/execution/rate_limit_and_reconnect.py:152-181](core/execution/rate_limit_and_reconnect.py) sync `acquire` 用 `time.sleep` 会阻塞事件循环，加断言或禁用（2026-07-19 已复核：`acquire(wait=True)` 在 running event loop 内抛 `RuntimeError` 并提示使用 `acquire_async`；回归覆盖 `tests/test_execution_rate_limit_policy.py::test_waiting_sync_acquire_is_forbidden_inside_event_loop`）

#### 4.3 后台 worker / Lifespan
- [x] [web/main.py:1140-1166](web/main.py) `_data_maintenance_worker` 加 per-task `asyncio.wait_for`（2026-08-01 已修复：数据维护周期新增 `DATA_MAINTENANCE_TOTAL_TIMEOUT_SEC`，单个 market sync / analytics / community / factor / onchain / multi-assets / news snapshot 步骤通过 `_maintenance_wait_for` / `_maintenance_safe_call` 设置独立超时，超时记录到快照而不是卡死 worker；回归覆盖 `tests/test_web_main_runtime_tasks.py::test_maintenance_safe_call_reports_timeout` 与 `::test_sync_market_dataset_reports_download_timeout`）
- [x] [web/main.py:1596-1691](web/main.py) `_google_trends_worker` 等数据 worker 在 ImportError 后停止重试（2026-08-02 已修复：Google Trends / Macro / Glassnode / CryptoQuant worker 的 `ImportError` 分支改为记录失败 heartbeat 后按 `EXTERNAL_DATA_IMPORT_RETRY_SEC` 重试，不再永久退出；回归覆盖 `tests/test_web_main_runtime_tasks.py::test_optional_external_worker_retries_after_import_error`）
- [x] [web/main.py:2227-2287](web/main.py) lifespan shutdown 串行 await 加总超时，避免 hang（2026-07-30 已修复：shutdown await 步骤通过 `_run_shutdown_step` 共享 `_SHUTDOWN_TOTAL_TIMEOUT_SEC` 总预算；回归覆盖 `tests/test_web_main_runtime_tasks.py::test_shutdown_step_respects_shared_deadline` 和 `::test_shutdown_step_skips_when_shared_deadline_is_exhausted`）
- [x] [core/research/orchestrator.py:372-415](core/research/orchestrator.py) `_recover_stale_jobs_on_startup` 需要先清理 `research_job_tasks` 字典（2026-08-03 已修复：startup recovery 先重置内存 task registry，再把 stale proposals/runs/persisted jobs 标为 failed；回归覆盖 `tests/test_research_recovery_draft_status.py::test_stale_research_proposal_recovers_to_draft`）

#### 4.4 数据采集与 DEX/CEX
- [x] [core/news/collectors/jin10.py:82](core/news/collectors/jin10.py), [newsapi.py:90-91](core/news/collectors/newsapi.py) 缺重试/backoff；NewsAPI 不读 Retry-After（2026-07-13 复核：Jin10/NewsAPI 已有 transient status retry、Retry-After 支持，并由 `tests/test_news_collectors_resilience.py` 覆盖）
- [x] [core/news/collectors/rss.py:94](core/news/collectors/rss.py) XML 解析考虑使用 `defusedxml` 显式禁用外部实体（2026-07-13 复核：RSS collectors 已使用 `defusedxml.ElementTree`，外部实体拒绝由 `tests/test_news_collectors_resilience.py` 覆盖）
- [x] [core/data/news_collector.py:78-79](core/data/news_collector.py) `bullish_keywords` 含前导空格的 `" adoption"` 永远不命中（2026-06-29 已修正为 `adoption`，并由 `tests/test_news_collector_legacy_async.py` 回归覆盖）
- [x] [core/realtime/event_bus.py:62-66](core/realtime/event_bus.py) full 队列 `get_nowait` 异常误判 stale 驱逐 active subscriber（2026-06-29 已保留活跃订阅者，并由 `tests/test_realtime_event_bus.py` 覆盖）

---

### P3 — 代码质量与测试覆盖（持续）

#### 5.1 测试
- [ ] 核心策略覆盖仍不完整：`tests/test_strategy_bug_fixes.py` 已补多项回归，但仍需系统覆盖方向对称性、min_bars 边界、NaN/Inf 输入、check_exit 持仓方向
- [ ] 仍缺完整策略测试的重点：`BollingerSqueezeStrategy`、`MACDHistogramStrategy`、`MaxDrawdownStrategy`、`OrderFlowImbalanceStrategy`、`TradeIntensityStrategy`、`SocialSentimentStrategy`、`MLXGBoostStrategy`、DEX/CEX Arbitrage、`SupplyEventStrategy`；`RSIDivergenceStrategy`、`HurstExponentStrategy`、`SortinoRatioStrategy`、`VaRBreakoutStrategy` 已有 targeted regression，仍可补全矩阵覆盖
- [x] [pytest.ini](pytest.ini) 加 `asyncio_mode = auto`、`--strict-markers`、`--tb=short`、`filterwarnings`、timeout 插件（2026-06-24 已完成，timeout 提升到 120s 以降低重载下误报）
- [x] [tests/conftest.py:43-44](tests/conftest.py) 不要直接写 `_trade_history_store_root` 私有属性，提供 `RiskManager.configure_storage(path)`（2026-06-24 已完成）
- [x] [tests/test_funding_rate.py:33,40,52](tests/test_funding_rate.py) 用固定 `datetime(2024,1,1)` 替代 `datetime.now()`，避免午夜跨边界 flaky（2026-07-02 复核：已使用 `FIXED_FUNDING_TIME`，`tests/test_funding_rate.py` 通过）

#### 5.2 基础设施
- [x] [Dockerfile](Dockerfile) 加 `USER nonroot`、`HEALTHCHECK`、多阶段构建、扩展 `.dockerignore`（2026-07-18 已复核：Dockerfile 使用 builder/runtime 多阶段、`USER cts`、`/livez` healthcheck，`.dockerignore` 覆盖 secrets/runtime/logs，回归覆盖 `tests/test_infra_fixes.py::test_dockerfile_contains_nonroot_user_and_healthcheck` 与 `tests/test_infra_fixes.py::test_dockerignore_blocks_secrets_and_runtime_dirs`）
- [ ] [requirements.txt](requirements.txt) 引入 lockfile（pip-tools / uv）；解锁 numpy/pyarrow 上限；DEX 依赖移到 extras
- [x] [config/database.py](config/database.py) 29 处 `default=datetime.utcnow` 改 `default=lambda: datetime.now(timezone.utc)`（Py3.12+ deprecated）（2026-07-18 已复核：`config.database._utcnow` 返回 timezone-aware UTC，模块不再包含 `datetime.utcnow`，回归覆盖 `tests/test_infra_fixes.py::test_database_utcnow_returns_tz_aware_utc` 与 `tests/test_infra_fixes.py::test_database_module_has_no_utcnow_call_sites`）
- [x] Python 版本对齐：项目运行口径统一为 Python 3.11（2026-08-05 已复核：`scripts/test.ps1` 自动选择 repo-local `crypto_trading` Python 3.11.15，`README.md`、`Dockerfile`、`environment.yml`、`pytest.ini` 均声明 3.11；系统级 `python` 可能仍是 3.13.7，但不是项目测试运行时）
- [ ] [logs/](logs/) 日志切割：ops_audit.jsonl 按大小切片 + 压缩；uvicorn stdout/stderr 纳入 Loguru rotation
- [x] [scripts/cleanup_repo.ps1](scripts/cleanup_repo.ps1) `LogRetentionDays=7` 与 `main.py` 的 `LOG_RETENTION=30 days` 不一致（2026-07-18 已复核：cleanup script default is now 30 days，回归覆盖 `tests/test_infra_fixes.py::test_cleanup_script_log_retention_default_is_30`）

#### 5.3 前端
- [ ] [web/static/js/ai_research.js:288-360](web/static/js/ai_research.js) `normalizeUiText` 内嵌乱码→中文映射是 UTF-8 mojibake 修复 hack，找上游编码 bug 删除
- [x] [web/static/js/ai_research.js:5460-5470, 6080-6128](web/static/js/ai_research.js) `state.jobPollingTimers` 在 stopPolling 中未清理；加 maxAttempts 上限（2026-07-23：`stopPolling()` 统一调用 `stopAllJobPolling()`，新增 `JOB_POLL_MAX_ATTEMPTS=1200` 与 `tests/test_ai_research_phase5_ui_assets.py` 静态回归）
- [x] [web/static/js/research_workbench.js:1664-1674](web/static/js/research_workbench.js) `startWorkbenchAutoRefresh` 隐藏 tab 时清理 timer（2026-07-27 已复核：新增 `stopWorkbenchAutoRefresh()` 统一清理 overview/countdown timers，隐藏 tab 与重启 auto-refresh 均走同一路径；静态回归覆盖 `tests/test_research_workbench_ui_assets.py::test_research_workbench_auto_refresh_stops_timers_when_tab_hidden`）
- [ ] [web/templates/index.html:24-25](web/templates/index.html) 第三方 CDN 脚本加 SRI integrity 校验或本地化

#### 5.4 清洁与文档
- [ ] 仓库根清理：`MagicMock/`、`.pytest_tmp_broken/`、`fix_mojibake.py`、`_once.ps1`、`report_exit_refactor.md`、`refresh_research_universe.bat` 移入 `scripts/legacy/`
- [x] `CLAUDE.md` 60KB 是"任务清单"非规范文档，移到 `docs/plans/`（2026-08-08 复核：仓库根无 `CLAUDE.md`，任务清单文件位于 `docs/plans/CLAUDE.md`）
- [x] [docker-compose.yml:58-65](docker-compose.yml) Prometheus 引用不存在的 `./prometheus.yml`，profile 启动即失败（2026-06-24 已补齐 `prometheus.yml` 并恢复 monitoring profile）
- [ ] 抽公共工具：`oscillator_entry_strength`、`position_field_extractor`、`adx_indicator`（多策略重复 ~50 行）
- [ ] 多文件未使用 import 清理（`from datetime import datetime, timezone` 但被 `bar_time` 替代后忘删）

---

## 3. 各模块审查总评（来自 6 个代理）

### 策略模块
> 结构清晰、`StrategyBase` 接口统一，但具体策略实现质量参差不齐：振荡器/因子类大量使用 `rolling.apply` lambda；部分"看似双向但仅单向"问题仍需继续排查（例如 MaxDrawdown、BollingerMeanReversion close-only）。TrendFollowing ADX 自引用、RSIDivergence 未来泄漏、HurstExponent 阈值错位、VWAPReversion SHORT 对称性等已完成回归覆盖。**最关键改进**：① 继续处理剩余策略统计 bug ② 抽公共工具减少 ~30% 重复 ③ 单元测试覆盖率从 ~25% 提到接近 100%。

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
