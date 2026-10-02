# 数据、AI、研究子审计逐文件覆盖清单

下列全部 Python 文件均完成完整文本读取、AST 结构解析、导入/顶层调用与未来数据/持久化/并发/网络/时效/执行危险模式扫描。
此清单区分结构扫描与重点人工审查；并不代表每一函数都经过动态复现。完整定义、行号、SHA256 与匹配位置见 data_research_inventory.json。

| 文件 | 行数 | AST | 定义数 | 静态关注面 |
|---|---:|---|---:|---|
| `core/ai/__init__.py` | 0 | 通过 | 0 | 纯定义/初始化 |
| `core/ai/autonomous_agent.py` | 6329 | 通过 | 161 | future_or_fill、persistence、concurrency、network、freshness |
| `core/ai/autonomous_learning.py` | 621 | 通过 | 15 | future_or_fill |
| `core/ai/autonomous_research_loop.py` | 4 | 通过 | 0 | 纯定义/初始化 |
| `core/ai/coinglass_signal.py` | 146 | 通过 | 2 | future_or_fill |
| `core/ai/live_decision_router.py` | 830 | 通过 | 23 | future_or_fill、persistence、concurrency、network |
| `core/ai/ml_signal.py` | 371 | 通过 | 18 | future_or_fill |
| `core/ai/model_endpoints.py` | 25 | 通过 | 1 | 纯定义/初始化 |
| `core/ai/model_feedback_errors.py` | 164 | 通过 | 3 | future_or_fill |
| `core/ai/promotion_narrator.py` | 114 | 通过 | 3 | future_or_fill |
| `core/ai/proposal_schemas.py` | 190 | 通过 | 10 | future_or_fill |
| `core/ai/provider_runtime_policy.py` | 90 | 通过 | 3 | future_or_fill |
| `core/ai/research_context_generator.py` | 514 | 通过 | 12 | future_or_fill、network |
| `core/ai/research_loop_evaluation.py` | 249 | 通过 | 15 | future_or_fill、persistence、network、freshness |
| `core/ai/research_loop_service.py` | 419 | 通过 | 17 | future_or_fill、persistence、concurrency、freshness |
| `core/ai/research_loop_v2.py` | 352 | 通过 | 19 | future_or_fill、persistence、concurrency |
| `core/ai/research_planner.py` | 1131 | 通过 | 26 | future_or_fill |
| `core/ai/research_runtime_context.py` | 321 | 通过 | 17 | future_or_fill |
| `core/ai/research_scheduler.py` | 175 | 通过 | 9 | future_or_fill、concurrency |
| `core/ai/research_search_loop.py` | 581 | 通过 | 9 | future_or_fill |
| `core/ai/risk_gate.py` | 159 | 通过 | 6 | future_or_fill |
| `core/ai/runtime_eligibility.py` | 616 | 通过 | 29 | future_or_fill、persistence、freshness |
| `core/ai/runtime_strategy_metadata.py` | 202 | 通过 | 8 | future_or_fill |
| `core/ai/signal_aggregator.py` | 628 | 通过 | 13 | future_or_fill |
| `core/ai/signal_engine.py` | 197 | 通过 | 6 | future_or_fill、freshness |
| `core/ml/__init__.py` | 31 | 通过 | 0 | 纯定义/初始化 |
| `core/ml/pipeline.py` | 987 | 通过 | 37 | future_or_fill、persistence、security |
| `core/research/__init__.py` | 14 | 通过 | 0 | 纯定义/初始化 |
| `core/research/altcoin_radar.py` | 1068 | 通过 | 3 | future_or_fill、freshness |
| `core/research/altcoin_radar_detail.py` | 480 | 通过 | 7 | future_or_fill、freshness |
| `core/research/altcoin_radar_events.py` | 329 | 通过 | 15 | future_or_fill、concurrency、freshness |
| `core/research/altcoin_radar_metrics.py` | 516 | 通过 | 39 | future_or_fill |
| `core/research/altcoin_radar_narrative.py` | 258 | 通过 | 8 | future_or_fill |
| `core/research/altcoin_radar_perp.py` | 271 | 通过 | 10 | future_or_fill |
| `core/research/altcoin_radar_ranking.py` | 278 | 通过 | 10 | future_or_fill |
| `core/research/altcoin_radar_universe.py` | 453 | 通过 | 12 | future_or_fill、persistence、concurrency |
| `core/research/binance_continuation_utility.py` | 108 | 通过 | 5 | future_or_fill |
| `core/research/binance_entry_exit_validation.py` | 555 | 通过 | 8 | future_or_fill |
| `core/research/binance_forward_microstructure.py` | 182 | 通过 | 9 | future_or_fill、concurrency |
| `core/research/binance_hybrid_stop_validation.py` | 231 | 通过 | 4 | future_or_fill |
| `core/research/binance_multisource_validation.py` | 603 | 通过 | 19 | future_or_fill、concurrency、freshness |
| `core/research/binance_postlaunch_validation.py` | 145 | 通过 | 3 | future_or_fill |
| `core/research/binance_runup_validation.py` | 1631 | 通过 | 36 | future_or_fill |
| `core/research/binance_sequential_validation.py` | 288 | 通过 | 7 | future_or_fill |
| `core/research/delist_risk.py` | 154 | 通过 | 9 | future_or_fill、persistence、concurrency、freshness |
| `core/research/exchange_notices.py` | 202 | 通过 | 12 | future_or_fill、persistence、freshness |
| `core/research/exchange_research_runner.py` | 252 | 通过 | 20 | future_or_fill、persistence、concurrency、network、freshness |
| `core/research/experiment_registry.py` | 280 | 通过 | 29 | future_or_fill、persistence、concurrency |
| `core/research/experiment_schemas.py` | 94 | 通过 | 5 | future_or_fill |
| `core/research/listing_short_tracker.py` | 245 | 通过 | 13 | future_or_fill、persistence、security |
| `core/research/orchestrator.py` | 2130 | 通过 | 58 | future_or_fill、persistence、concurrency、freshness |
| `core/research/performance_feedback.py` | 170 | 通过 | 6 | future_or_fill |
| `core/research/pump_precursor.py` | 138 | 通过 | 5 | future_or_fill |
| `core/research/retirement.py` | 89 | 通过 | 2 | future_or_fill |
| `core/research/signal_evidence.py` | 130 | 通过 | 7 | future_or_fill |
| `core/research/strategy_program.py` | 448 | 通过 | 18 | future_or_fill |
| `core/research/strategy_research.py` | 3310 | 通过 | 60 | future_or_fill、persistence、concurrency、freshness |
| `core/research/supply_factor_tracker.py` | 353 | 通过 | 17 | future_or_fill、persistence、network、security |
| `core/research/unlock_events.py` | 65 | 通过 | 4 | future_or_fill |
| `core/research/unlock_short_tracker.py` | 315 | 通过 | 19 | future_or_fill、persistence、concurrency、freshness |
| `core/research/upbit_caution_tracker.py` | 347 | 通过 | 13 | future_or_fill、persistence |
| `core/research/validation_gate.py` | 714 | 通过 | 9 | future_or_fill |
| `core/research/vesting_spec.py` | 71 | 通过 | 3 | future_or_fill |
| `core/research/xs_evaluation.py` | 314 | 通过 | 15 | future_or_fill |
| `core/research/xs_feature_dsl.py` | 204 | 通过 | 8 | future_or_fill、security |
| `core/research/xs_panel.py` | 200 | 通过 | 8 | future_or_fill |
| `core/backtest/__init__.py` | 42 | 通过 | 0 | 纯定义/初始化 |
| `core/backtest/backtest_engine.py` | 956 | 通过 | 30 | future_or_fill |
| `core/backtest/common_pnl.py` | 43 | 通过 | 2 | future_or_fill |
| `core/backtest/cost_models.py` | 87 | 通过 | 4 | future_or_fill |
| `core/backtest/execution_arrays.py` | 378 | 通过 | 3 | future_or_fill |
| `core/backtest/exit_engine.py` | 765 | 通过 | 28 | future_or_fill |
| `core/backtest/funding_provider.py` | 399 | 通过 | 21 | future_or_fill、persistence、network |
| `core/backtest/paper_trading.py` | 283 | 通过 | 27 | concurrency、freshness |
| `core/backtest/performance_analyzer.py` | 313 | 通过 | 16 | 纯定义/初始化 |
| `core/backtest/report_generator.py` | 218 | 通过 | 6 | 纯定义/初始化 |
| `core/data/__init__.py` | 62 | 通过 | 0 | 纯定义/初始化 |
| `core/data/alpha_market_data.py` | 582 | 通过 | 18 | future_or_fill、concurrency、freshness |
| `core/data/binance_alpha.py` | 404 | 通过 | 22 | future_or_fill、persistence、concurrency、network、freshness |
| `core/data/binance_alpha_collector.py` | 1428 | 通过 | 53 | future_or_fill、persistence、concurrency、network |
| `core/data/binance_archive.py` | 169 | 通过 | 8 | future_or_fill、persistence、network |
| `core/data/coinglass_altcoin.py` | 845 | 通过 | 36 | future_or_fill、persistence、concurrency、network、freshness |
| `core/data/coinglass_client.py` | 1988 | 通过 | 83 | future_or_fill、persistence、concurrency、network |
| `core/data/coinglass_feature_builder.py` | 1759 | 通过 | 35 | future_or_fill、persistence、concurrency、freshness |
| `core/data/coinglass_lsr.py` | 185 | 通过 | 10 | future_or_fill、freshness |
| `core/data/coinglass_onchain.py` | 501 | 通过 | 21 | future_or_fill |
| `core/data/coinglass_registry.py` | 743 | 通过 | 9 | future_or_fill |
| `core/data/cryptoquant_collector.py` | 130 | 通过 | 4 | future_or_fill、persistence、network |
| `core/data/data_collector.py` | 397 | 通过 | 24 | concurrency、network |
| `core/data/data_processor.py` | 367 | 通过 | 17 | 纯定义/初始化 |
| `core/data/data_storage.py` | 818 | 通过 | 36 | future_or_fill、persistence、concurrency |
| `core/data/factor_library.py` | 500 | 通过 | 12 | future_or_fill |
| `core/data/funding_rate_collector.py` | 631 | 通过 | 22 | future_or_fill、concurrency、network |
| `core/data/funding_rate_models.py` | 160 | 通过 | 15 | network |
| `core/data/glassnode_collector.py` | 130 | 通过 | 4 | future_or_fill、persistence、network |
| `core/data/google_trends_collector.py` | 94 | 通过 | 5 | future_or_fill、persistence |
| `core/data/historical_data.py` | 648 | 通过 | 27 | future_or_fill、persistence、network |
| `core/data/kaiko_collector.py` | 149 | 通过 | 5 | future_or_fill、persistence、network |
| `core/data/macro_collector.py` | 1011 | 通过 | 36 | future_or_fill、persistence、concurrency、network |
| `core/data/nansen_collector.py` | 161 | 通过 | 6 | future_or_fill、persistence、concurrency、network |
| `core/data/news_collector.py` | 394 | 通过 | 16 | concurrency、network、freshness |
| `core/data/oi_collector.py` | 381 | 通过 | 22 | future_or_fill、network |
| `core/data/options_collector.py` | 229 | 通过 | 13 | future_or_fill、persistence、network |
| `core/data/orderbook/__init__.py` | 11 | 通过 | 0 | 纯定义/初始化 |
| `core/data/orderbook/orderbook_collector.py` | 466 | 通过 | 29 | future_or_fill、network |
| `core/data/parquet_lock.py` | 107 | 通过 | 3 | future_or_fill、concurrency |
| `core/data/path_utils.py` | 65 | 通过 | 7 | future_or_fill |
| `core/data/second_level_backfill.py` | 513 | 通过 | 23 | future_or_fill、persistence、concurrency |
| `core/data/sentiment/__init__.py` | 11 | 通过 | 0 | 纯定义/初始化 |
| `core/data/sentiment/fear_greed_collector.py` | 245 | 通过 | 25 | network |
| `core/data/source_cache_maintenance.py` | 65 | 通过 | 3 | future_or_fill、concurrency、freshness |
| `core/marketdata/__init__.py` | 20 | 通过 | 0 | 纯定义/初始化 |
| `core/marketdata/binance_perp_ws_client.py` | 191 | 通过 | 13 | future_or_fill、network |
| `core/marketdata/ccxt_pro_feed.py` | 794 | 通过 | 34 | future_or_fill、persistence、concurrency、network、freshness |
| `core/marketdata/hub.py` | 681 | 通过 | 27 | future_or_fill、network、freshness |
| `core/marketdata/runtime_price_provider.py` | 310 | 通过 | 13 | future_or_fill、freshness |
| `core/marketdata/ws_client.py` | 105 | 通过 | 9 | future_or_fill、network |
| `core/marketdata/ws_quality_guard.py` | 241 | 通过 | 12 | future_or_fill、freshness |
| `core/realtime/__init__.py` | 5 | 通过 | 0 | 纯定义/初始化 |
| `core/realtime/event_bus.py` | 102 | 通过 | 11 | future_or_fill、concurrency、network、freshness |
| `core/news/__init__.py` | 0 | 通过 | 0 | 纯定义/初始化 |
| `core/news/collectors/__init__.py` | 72 | 通过 | 1 | future_or_fill |
| `core/news/collectors/binance_announcements.py` | 145 | 通过 | 5 | future_or_fill、freshness |
| `core/news/collectors/bybit_announcements.py` | 125 | 通过 | 6 | future_or_fill、freshness |
| `core/news/collectors/chaincatcher_flash.py` | 151 | 通过 | 8 | future_or_fill、freshness |
| `core/news/collectors/coinglass_news.py` | 387 | 通过 | 26 | future_or_fill、freshness |
| `core/news/collectors/common.py` | 268 | 通过 | 18 | future_or_fill、network、freshness |
| `core/news/collectors/cryptocompare_news.py` | 80 | 通过 | 4 | future_or_fill、freshness |
| `core/news/collectors/cryptopanic.py` | 109 | 通过 | 5 | future_or_fill、network、freshness |
| `core/news/collectors/exchange_events.py` | 42 | 通过 | 1 | future_or_fill |
| `core/news/collectors/gdelt.py` | 115 | 通过 | 5 | future_or_fill、network、freshness |
| `core/news/collectors/jin10.py` | 145 | 通过 | 7 | future_or_fill、network、freshness |
| `core/news/collectors/manager.py` | 548 | 通过 | 15 | future_or_fill、concurrency、freshness |
| `core/news/collectors/newsapi.py` | 150 | 通过 | 7 | future_or_fill、network、freshness |
| `core/news/collectors/okx_announcements.py` | 114 | 通过 | 5 | future_or_fill、freshness |
| `core/news/collectors/opennews.py` | 280 | 通过 | 16 | future_or_fill、freshness |
| `core/news/collectors/quality.py` | 115 | 通过 | 5 | future_or_fill |
| `core/news/collectors/rss.py` | 201 | 通过 | 9 | future_or_fill、network、freshness |
| `core/news/eventizer/__init__.py` | 7 | 通过 | 0 | 纯定义/初始化 |
| `core/news/eventizer/async_glm_client.py` | 1746 | 通过 | 63 | future_or_fill、concurrency、network、freshness |
| `core/news/eventizer/llm_glm5.py` | 1455 | 通过 | 59 | future_or_fill、concurrency、network、freshness |
| `core/news/eventizer/rate_limiter.py` | 267 | 通过 | 17 | future_or_fill、concurrency |
| `core/news/eventizer/rules.py` | 225 | 通过 | 13 | future_or_fill、freshness |
| `core/news/service/__init__.py` | 0 | 通过 | 0 | 纯定义/初始化 |
| `core/news/service/api.py` | 230 | 通过 | 18 | future_or_fill |
| `core/news/service/llm_worker.py` | 25 | 通过 | 2 | future_or_fill |
| `core/news/service/worker.py` | 767 | 通过 | 27 | future_or_fill、concurrency |
| `core/news/storage/__init__.py` | 0 | 通过 | 0 | 纯定义/初始化 |
| `core/news/storage/db.py` | 1995 | 通过 | 70 | future_or_fill、persistence、concurrency、freshness |
| `core/news/storage/models.py` | 225 | 通过 | 17 | future_or_fill、freshness |
| `core/news/text_normalizer.py` | 51 | 通过 | 3 | future_or_fill |
| `core/monitoring/__init__.py` | 0 | 通过 | 0 | 纯定义/初始化 |
| `core/monitoring/cusum_watcher.py` | 471 | 通过 | 11 | future_or_fill、concurrency |
| `core/monitoring/loop_stall_watchdog.py` | 129 | 通过 | 8 | future_or_fill、concurrency |
| `core/monitoring/strategy_monitor.py` | 268 | 通过 | 11 | future_or_fill |
| `core/observability/__init__.py` | 2 | 通过 | 0 | 纯定义/初始化 |
| `core/observability/decision_trace.py` | 143 | 通过 | 9 | future_or_fill |
| `core/observability/gate_codes.py` | 78 | 通过 | 5 | future_or_fill |
| `core/observability/score_calibration.py` | 284 | 通过 | 10 | future_or_fill、persistence |
| `prediction_markets/__init__.py` | 3 | 通过 | 0 | 纯定义/初始化 |
| `prediction_markets/polymarket/__init__.py` | 25 | 通过 | 0 | 纯定义/初始化 |
| `prediction_markets/polymarket/clob_reader.py` | 342 | 通过 | 20 | future_or_fill、concurrency、network |
| `prediction_markets/polymarket/clob_trader.py` | 75 | 通过 | 10 | future_or_fill |
| `prediction_markets/polymarket/config.py` | 98 | 通过 | 5 | future_or_fill |
| `prediction_markets/polymarket/db.py` | 950 | 通过 | 51 | future_or_fill、persistence、freshness |
| `prediction_markets/polymarket/features.py` | 238 | 通过 | 8 | future_or_fill、freshness |
| `prediction_markets/polymarket/gamma_client.py` | 125 | 通过 | 9 | future_or_fill、concurrency、network |
| `prediction_markets/polymarket/market_resolver.py` | 235 | 通过 | 12 | future_or_fill |
| `prediction_markets/polymarket/models.py` | 195 | 通过 | 10 | future_or_fill |
| `prediction_markets/polymarket/paper_replay.py` | 229 | 通过 | 21 | future_or_fill |
| `prediction_markets/polymarket/paper_strategy.py` | 335 | 通过 | 12 | future_or_fill、persistence |
| `prediction_markets/polymarket/paper_trading.py` | 299 | 通过 | 22 | future_or_fill |
| `prediction_markets/polymarket/replay_report.py` | 409 | 通过 | 18 | future_or_fill、persistence |
| `prediction_markets/polymarket/utils.py` | 126 | 通过 | 12 | future_or_fill、concurrency |
| `prediction_markets/polymarket/worker.py` | 383 | 通过 | 18 | future_or_fill、concurrency |
| `core/factors_ts/__init__.py` | 28 | 通过 | 0 | 纯定义/初始化 |
| `core/factors_ts/base.py` | 41 | 通过 | 4 | future_or_fill |
| `core/factors_ts/cache.py` | 295 | 通过 | 12 | future_or_fill、concurrency、security |
| `core/factors_ts/extended_factors.py` | 1075 | 通过 | 89 | future_or_fill |
| `core/factors_ts/funding_rate_factors.py` | 550 | 通过 | 48 | 纯定义/初始化 |
| `core/factors_ts/impl.py` | 164 | 通过 | 22 | future_or_fill |
| `core/factors_ts/oi_factors.py` | 217 | 通过 | 23 | 纯定义/初始化 |
| `core/factors_ts/orderbook_factors.py` | 244 | 通过 | 29 | 纯定义/初始化 |
| `core/factors_ts/registry.py` | 42 | 通过 | 4 | future_or_fill |
| `core/factors_ts/sentiment_factors.py` | 186 | 通过 | 20 | 纯定义/初始化 |
| `core/indicators/__init__.py` | 28 | 通过 | 0 | 纯定义/初始化 |
| `core/indicators/adx.py` | 103 | 通过 | 3 | future_or_fill |
| `core/indicators/oscillators.py` | 63 | 通过 | 1 | future_or_fill |
| `core/indicators/rolling.py` | 134 | 通过 | 3 | future_or_fill |
| `core/notifications/__init__.py` | 5 | 通过 | 0 | 纯定义/初始化 |
| `core/notifications/notification_manager.py` | 974 | 通过 | 44 | future_or_fill、persistence、concurrency、network、freshness |
| `core/deployment/__init__.py` | 1 | 通过 | 0 | 纯定义/初始化 |
| `core/deployment/promotion_engine.py` | 359 | 通过 | 9 | future_or_fill |

## 关联测试文件（按源码模块字符串识别，实际执行由主审计汇总）

- `tests/conftest.py`：core/ai、core/observability
- `tests/core/test_paper_trading_callbacks.py`：core/backtest
- `tests/ops/test_ops_ai_proposals.py`：core/research
- `tests/polymarket/test_clob_reader.py`：prediction_markets
- `tests/polymarket/test_db_config.py`：prediction_markets
- `tests/polymarket/test_features.py`：prediction_markets
- `tests/polymarket/test_gamma_client.py`：prediction_markets
- `tests/polymarket/test_market_resolver.py`：prediction_markets
- `tests/polymarket/test_ops_polymarket.py`：prediction_markets
- `tests/polymarket/test_paper_replay.py`：prediction_markets
- `tests/polymarket/test_paper_strategy.py`：prediction_markets
- `tests/polymarket/test_paper_trading.py`：prediction_markets
- `tests/polymarket/test_replay_grid.py`：prediction_markets
- `tests/polymarket/test_replay_report.py`：prediction_markets
- `tests/polymarket/test_replay_walk_forward.py`：prediction_markets
- `tests/polymarket/test_risk_gate_pm.py`：core/ai
- `tests/polymarket/test_signal_engine_pm.py`：core/ai
- `tests/polymarket/test_token_universe.py`：prediction_markets
- `tests/polymarket/test_utils.py`：prediction_markets
- `tests/polymarket/test_worker_once_polymarket.py`：prediction_markets
- `tests/test_ai_autonomous_agent.py`：core/ai、core/research、core/notifications
- `tests/test_ai_autonomous_learning.py`：core/ai
- `tests/test_ai_live_decision_router.py`：core/ai
- `tests/test_ai_research_autonomous_agent_api.py`：core/ai、core/research、core/monitoring、core/observability
- `tests/test_ai_research_autonomy_phase1.py`：core/ai、core/research
- `tests/test_ai_research_cleanup_api.py`：core/ai、core/research
- `tests/test_ai_research_macro_warm_api.py`：core/data
- `tests/test_ai_research_phase2.py`：core/ai、core/research、core/data、core/monitoring、core/observability
- `tests/test_ai_research_phase3.py`：core/ai、core/research
- `tests/test_ai_research_phase4_runtime.py`：core/ai、core/research
- `tests/test_ai_research_runtime_and_phase_e.py`：core/ai、core/research、core/data、core/news、core/monitoring、core/deployment
- `tests/test_ai_research_scheduler.py`：core/ai
- `tests/test_alpha_market_data_integration.py`：core/data
- `tests/test_altcoin_notification_manager.py`：core/notifications
- `tests/test_altcoin_radar_derivatives.py`：core/research、core/data
- `tests/test_altcoin_radar_events.py`：core/research
- `tests/test_altcoin_radar_events_context.py`：core/research
- `tests/test_altcoin_radar_narrative.py`：core/research
- `tests/test_altcoin_radar_perp.py`：core/research
- `tests/test_altcoin_radar_universe.py`：core/research
- `tests/test_audit_fixes_and_shadow_trackers.py`：core/research
- `tests/test_autonomous_research_loop.py`：core/ai、core/research
- `tests/test_backtest_cost_models.py`：core/backtest
- `tests/test_backtest_engine_protective_exits.py`：core/backtest
- `tests/test_backtest_runtime_consistency.py`：core/research
- `tests/test_backtest_sharpe_annualization.py`：core/backtest
- `tests/test_binance_alpha.py`：core/data
- `tests/test_binance_alpha_collector.py`：core/data
- `tests/test_binance_perp_ws_client.py`：core/marketdata
- `tests/test_binance_sequential_validation.py`：core/research
- `tests/test_ccxt_pro_feed.py`：core/marketdata
- `tests/test_ccxt_pro_feed_markprice.py`：core/marketdata
- `tests/test_coinglass_altcoin.py`：core/data
- `tests/test_coinglass_budget_short_circuit.py`：core/data
- `tests/test_coinglass_dataset_normalization.py`：core/data
- `tests/test_coinglass_news_collectors.py`：core/news
- `tests/test_coinglass_read_only_overview.py`：core/data
- `tests/test_coinglass_signal.py`：core/ai
- `tests/test_content_quality_static.py`：core/data
- `tests/test_core_indicators.py`：core/indicators
- `tests/test_core_indicators_rolling.py`：core/indicators
- `tests/test_correlation_filter_same_strategy_diff_params.py`：core/research
- `tests/test_data_collector_lifecycle.py`：core/data
- `tests/test_data_storage.py`：core/data
- `tests/test_delist_risk.py`：core/research
- `tests/test_exchange_guard_and_listing_tracker.py`：core/ai、core/research
- `tests/test_execution_arrays_parity.py`：core/backtest
- `tests/test_execution_engine_coinglass_filters.py`：core/data
- `tests/test_execution_engine_protective_levels.py`：core/backtest
- `tests/test_exit_engine.py`：core/backtest
- `tests/test_experiment_registry_windows_concurrent.py`：core/ai、core/research
- `tests/test_factor_cache.py`：core/factors_ts
- `tests/test_funding_provider.py`：core/backtest、core/data
- `tests/test_funding_rate.py`：core/data、core/factors_ts
- `tests/test_historical_data_manager.py`：core/data
- `tests/test_kol_lsr.py`：core/data
- `tests/test_loop_stall_watchdog.py`：core/monitoring
- `tests/test_macro_collector.py`：core/data
- `tests/test_macro_workbench_and_premium_status.py`：core/data、core/news
- `tests/test_market_data_hub.py`：core/marketdata
- `tests/test_market_source_stability.py`：core/data
- `tests/test_market_ws_api_price_paths.py`：core/ai、core/backtest、core/marketdata
- `tests/test_market_ws_authority_static.py`：core/marketdata
- `tests/test_ml_canonical_model.py`：core/ai、core/ml
- `tests/test_ml_pipeline.py`：core/ai、core/ml
- `tests/test_ml_signal_v2.py`：core/ai、core/ml
- `tests/test_model_feedback_errors.py`：core/ai
- `tests/test_news_collector_legacy_async.py`：core/data
- `tests/test_news_collectors_manager.py`：core/news
- `tests/test_news_collectors_resilience.py`：core/news
- `tests/test_news_eventizer_rules.py`：core/news
- `tests/test_news_exchange_announcements.py`：core/news
- `tests/test_news_service_api.py`：core/news
- `tests/test_news_storage_db_bootstrap.py`：core/news
- `tests/test_news_storage_db_locks.py`：core/news
- `tests/test_news_storage_llm_tasks.py`：core/news
- `tests/test_news_web_api_archive.py`：core/news
- `tests/test_news_worker_config_hot_reload.py`：core/news
- `tests/test_news_worker_intervals.py`：core/news
- `tests/test_no_lookahead_ts_factors.py`：core/factors_ts
- `tests/test_openai_responses_migration.py`：core/ai、core/news
- `tests/test_openai_target_helpers.py`：core/news
- `tests/test_opennews_collector.py`：core/news
- `tests/test_operating_reality_layer.py`：core/ai、core/research、core/data、core/monitoring、core/observability
- `tests/test_orchestrator_llm_rationale_timeout.py`：core/ai、core/research
- `tests/test_order_manager_safety.py`：core/marketdata
- `tests/test_parquet_tz_integrity.py`：core/data
- `tests/test_perf_fixes_round2.py`：core/research、core/marketdata
- `tests/test_proxy_env_and_history_collectors.py`：core/monitoring
- `tests/test_pump_precursor.py`：core/research
- `tests/test_rate_limiter.py`：core/news
- `tests/test_realtime_event_bus.py`：core/realtime
- `tests/test_research_integrity.py`：core/ai、core/research、core/monitoring
- `tests/test_research_loop_v2.py`：core/ai、core/research
- `tests/test_research_recovery_draft_status.py`：core/ai、core/research
- `tests/test_research_validation_gate.py`：core/research
- `tests/test_runtime_price_provider.py`：core/marketdata
- `tests/test_signal_aggregator_fear_greed.py`：core/ai、core/data
- `tests/test_signal_evidence.py`：core/research
- `tests/test_source_recovery.py`：core/data、core/news
- `tests/test_strategy_library_and_factors.py`：core/ai、core/ml、core/backtest、core/data
- `tests/test_strategy_monitor_robust_std.py`：core/monitoring
- `tests/test_strategy_ownership_sync.py`：core/research、core/deployment
- `tests/test_strategy_signal_regressions.py`：core/ai
- `tests/test_supply_factor_tracker.py`：core/research
- `tests/test_tracker_retirement.py`：core/research、core/notifications
- `tests/test_tracker_robustness.py`：core/research
- `tests/test_trade_risk_ml_fixes.py`：core/ai、core/ml
- `tests/test_trading_route_modules.py`：core/data
- `tests/test_unlock_short_tracker.py`：core/research
- `tests/test_upbit_caution_tracker.py`：core/research
- `tests/test_validation_dsr_kurtosis.py`：core/research
- `tests/test_vesting_spec.py`：core/research
- `tests/test_web_main_runtime_tasks.py`：core/data、core/marketdata
- `tests/test_ws_quality_guard.py`：core/marketdata
- `tests/test_xs_research.py`：core/research
- `tests/web/test_altcoin_route.py`：core/ai、core/research
- `tests/web/test_data_onchain_premium_payload.py`：core/data
