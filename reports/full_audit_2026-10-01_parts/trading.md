# 全量审计分报告：交易、执行、风控与策略资金路径

审计日期：2026-10-01；基线 HEAD：`2fecd05dea8c5ee438b4071e50548b828b7411af`。本分报告确认 **6 个 P1、2 个 P2 主问题**，另有 1 个 P3 离线会计问题。P1 表示足以导致风控突破、错误账户操作、持仓归因损坏或测试网订单误向生产；P2 表示有条件的执行状态错误。本次证据不足以确认 P0 级别立即且广泛的灾难性风险；未触达真实账户不作为降低已确认问题级别的依据。

读取业务代码，没有读取 `.env`、`.env.local`、`keys.txt`，没有启动服务、worker、访问账户、提交真实订单、写持仓/账户持久状态或修改业务代码。完整测试由主审计线程负责。本次复现只从 AST 提取项目函数到内存；所有网络、交易所、治理、账户、持久化和回调边界均 mock。只新增本目录审计报告、复现脚本及 JSON。

## 确认问题

### T1 · P1 · 反向非 reduce-only 手动订单把反手开仓部分也作为平仓豁免风控

定位：[core/trading/execution_engine.py:5805](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:5805)、[core/trading/execution_engine.py:5840](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:5840)、[core/trading/execution_engine.py:5855](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:5855)、[core/trading/execution_engine.py:5871](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:5871)；被绕过的检查在 [core/risk/risk_manager.py:910](F:/9_Crypto/crypto_trading_system/core/risk/risk_manager.py:910)、[core/risk/risk_manager.py:928](F:/9_Crypto/crypto_trading_system/core/risk/risk_manager.py:928)、[core/risk/risk_manager.py:958](F:/9_Crypto/crypto_trading_system/core/risk/risk_manager.py:958)、[core/risk/risk_manager.py:971](F:/9_Crypto/crypto_trading_system/core/risk/risk_manager.py:971)。

触发：存在 1 单位多仓，用户卖出 100 单位，`reduce_only=False`。代码仅按订单方向和已有仓位确定 `closes_existing=True`；只有 reduce-only 才按原仓量封顶。原请求的 100 单位、100 倍杠杆仍传给订单层，两个上游检查却均收到 `allow_close=True`。成交处理 [core/trading/execution_engine.py:6061](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:6061) 先关闭旧仓再建立 99 单位空仓。

影响：daily trade、max position count、max leverage、单笔 notional、总敞口和策略分配限额对反手新敞口失效；日内亏损熔断也允许平仓豁免。隔离复现使用 equity=1000、max leverage=3、同一卖单杠杆100，观测到完整 amount100/reduceOnlyFalse 到达提交边界；把同样请求标为正常入场，真实 RiskManager 方法返回 False。

边界：真实 OrderManager 在启用 governance 时对非 reduce-only 订单另有治理复核，可能挡住一部分请求；默认 `GOVERNANCE_ENABLED=False`（config/settings.py:175），该复核也不恢复这里跳过的 RiskManager 组合敞口检查。并非所有非日内熔断都能绕过。

建议：对可减少的旧仓量与反手新开量拆分；关闭部分使用真实 reduce-only，新增部分单独做完整入场检查。不能仅凭“方向相反”给整笔单 `allow_close`。

### T2 · P1 · 交易所同侧聚合持仓被完整复制到每个策略持仓

定位：[core/trading/execution_engine.py:3774](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:3774)–3805；实际覆写数量为 [core/trading/execution_engine.py:3639](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:3639)–3655。

触发：同一 exchange/account/symbol/side 下存在策略 A 的 1 单位和 B 的 2 单位，交易所聚合持仓为 3。reconcile 构建仅按 symbol/side 的 snapshot，然后遍历每个策略 local_pos，分别用相同 snapshot 覆写 local quantity、entry price、unrealized PNL。该问题不依赖缺单或数据过期。

隔离复现：调用实际 reconcile 与 sync 方法，A=3、B=3，本地合计6，而交易所实际3。

影响：本地策略仓位归因、风险总敞口与 PNL 被放大；后续任一策略按“自身3单位”平仓可消耗另一策略实际持仓，保护单也使用错误规模。共享父账户真实凭据的逻辑账户还需以实际交易账户身份分组，不能假设每个逻辑账户拥有独立交易所持仓。

建议：把交易所聚合仓位与策略分仓账本分开保存，reconcile 对同账户同侧只比较分仓总额；利用已确认 fill/订单归属分配差额，不得把总量广播到每个策略。

### T3 · P1 · 裸订单 ID 作缓存键导致跨账户元数据覆盖和错误账户取消

定位：[core/trading/order_manager.py:66](F:/9_Crypto/crypto_trading_system/core/trading/order_manager.py:66)、[core/trading/order_manager.py:770](F:/9_Crypto/crypto_trading_system/core/trading/order_manager.py:770)、[core/trading/order_manager.py:802](F:/9_Crypto/crypto_trading_system/core/trading/order_manager.py:802)、[core/trading/order_manager.py:901](F:/9_Crypto/crypto_trading_system/core/trading/order_manager.py:901)。

触发：两个账户或交易所返回相同订单 ID（订单 ID 不能作为全系统唯一身份）。`_orders` 与 `_order_meta` 均只用 `order.id` 存储，第二个订单覆盖第一个。cancel/get operations 再从这一份元数据取 account_id 选择 connector；cancel API 不能显式传 account_id 来纠正缓存身份。

隔离复现：实际 `_create_real_order` 方法通过两个 mock connector 分别给账户 A/B 返回同 symbol/id=42。缓存只剩1条，account_id=B；取消先前 A 的 id42实际调用 B 的 cancel_order。没有真实账户交互。

影响：订单丢失、账户元数据污染；取消/状态查询发往另一真实账户。mode conflict 检查也依赖被覆盖元数据。无需假定某个交易所的 ID 格式恒定；触发边界是任何不具全局唯一性的返回值。

建议：存储、查询、取消、状态回调和对账均以 `(mode, real_account, exchange, symbol, exchange_order_id)` 为键；接口显式携带账户身份，裸 ID 模糊匹配应拒绝。

### T4 · P1 · 衍生品 contractSize 未转换，基币数量与合约张数混用

定位：[core/exchanges/okx_connector.py:232](F:/9_Crypto/crypto_trading_system/core/exchanges/okx_connector.py:232)–239、[core/exchanges/okx_connector.py:284](F:/9_Crypto/crypto_trading_system/core/exchanges/okx_connector.py:284)–293；同类位置 [core/exchanges/gate_connector.py:304](F:/9_Crypto/crypto_trading_system/core/exchanges/gate_connector.py:304)、[core/exchanges/bybit_connector.py:283](F:/9_Crypto/crypto_trading_system/core/exchanges/bybit_connector.py:283)。上游计算基币量为 [core/trading/execution_engine.py:3212](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:3212)，持仓 valuation 也按 price × quantity。

触发：使用非1 contractSize的衍生品市场。引擎按 USD/price 得基币 quantity；connector 仅做 amount precision 后直接把它送入 CCXT derivatives amount（合约张数），positions 读取则直接把 contracts当基币 amount。生产资金路径未发现 contractSize转换。对于线性合约，基币量=contracts×contractSize；倒数合约还需额外合同单位/价格处理。[CCXT 官方说明](https://docs.ccxt.com/docs/faq)、[CCXT 统一 API 文档](https://docs.ccxt.com/docs/manual)。

隔离复现：实际 OKX create_order/get_positions方法，mock market contractSize=0.1。目标基币1发送1张，正确应10张；交易所10张持仓回读为基币10，正确应1。该证明针对转换逻辑，不依赖市场连接成功或真实品种可用性。

影响：订单规模、最小量/精度、风控名义值及重启对账错一个 contractSize系数；规模可能偏大或偏小，甚至保护平仓拒单。Binance常见线性contractSize=1市场不触发这个倍率问题。ccxt_adapter存在同类模式，但默认 supports_execution=false，不把该阶段适配器算成当前可执行路径。

建议：定义系统 quantity 的统一单位，对市场 metadata contract/linear/inverse/contractSize在connector边界双向规范化；不支持的合同类型 fail closed。precision 和 min amount也必须在对应单位应用。

### T5 · P1 · 部分平仓后专用 CLOSE 再次入账累计已实现 PNL

定位：[core/trading/position_manager.py:648](F:/9_Crypto/crypto_trading_system/core/trading/position_manager.py:648)–674、[core/trading/position_manager.py:676](F:/9_Crypto/crypto_trading_system/core/trading/position_manager.py:676)–696；[core/trading/execution_engine.py:5505](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:5505)–5535、[core/trading/execution_engine.py:5554](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:5554)。

触发：持仓经过一次部分平仓，再通过专用 CLOSE 信号关闭剩余量。PositionManager返回的 realized_pnl是持仓生命周期累计值，而 CLOSE将它完整当作这一笔 fill PNL送给 RiskManager与live strategy trade记录。普通反向买卖路径已正确扣除 prev_realized（execution_engine.py:4913、4972、6002、6061），故不是账本设计要求。

隔离复现：2单位多仓entry100，先1单位在110平仓实现10，后1单位在120平仓产生20；真实CLOSE方法记录30而非20，前后合计40而非30。

影响：成交收益、策略统计与累计daily realized被重复计算；daily stop使用 realized +负unrealized（risk_manager.py:708），因此影响止损阈值。纸面equity另从position history计算，此处不声称其必然直接多计10。盈亏均可重复，重复亏损可能提前熔断，重复盈利可能抵消亏损。

建议：close前保存prev_realized，日志/风控/策略记录都使用本次增量；或让PositionManager明确返回独立CloseFill结果，包含closed_qty和fill_realized。

### T6 · P1 · Binance fast REST忽略 sandbox，测试网账户请求发向生产域名

定位：[core/trading/binance_rest.py:105](F:/9_Crypto/crypto_trading_system/core/trading/binance_rest.py:105)–111、[core/trading/binance_rest.py:152](F:/9_Crypto/crypto_trading_system/core/trading/binance_rest.py:152)–156；调用在 [core/trading/order_manager.py:680](F:/9_Crypto/crypto_trading_system/core/trading/order_manager.py:680)–697、[core/trading/order_manager.py:729](F:/9_Crypto/crypto_trading_system/core/trading/order_manager.py:729)–735。

触发：账户 ExchangeConfig/credentials sandbox=True，live模式使用Binance futures market_type以及market/limit订单。CCXT connector config会带sandbox且当前安装CCXT构造器识别它并切换测试网；但是 fast path绕过connector，由 _binance_credentials只取key/secret/proxy，丢sandbox，signed_request固定生产api/fapi域名。非reduce-only的leverage同步也先走该REST路径，可能在订单之前失败。

隔离复现：真实credentials和signed_request函数配fake sandbox=True账户；mock HTTP捕获 `POST https://fapi.binance.com/fapi/v1/order`。所有“key”均是脚本自造 AUDIT_FAKE_*，未读取实际凭据，没有发HTTP请求。

影响边界：使用仅测试网有效的key时生产通常会拒绝，导致测试网下单/杠杆流程失效；若错误绑定了生产有效key并依赖sandbox配置保护，这条链会提交生产订单或调整生产杠杆。不能把所有connector都称为忽略sandbox，也不能据此断言已有真实生产成交。

建议：统一从账户/connector的环境配置解析REST endpoint，包括time sync、leverage、balances和orders；sandbox请求不得落到生产base URL。若未支持测试网fast REST，sandbox时禁用fast path并仅调用已切换环境的CCXT。

### T7 · P2 · algo订单合并把零成交子单当成交并宣告完成

定位：[execution_engine.py:6311](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:6311)–6319。触发：iceberg/TWAP/VWAP子单被接受但尚未成交，子结果有明确filled=0和正amount。汇总代码用 `filled or amount`，零被回退为amount；status无条件为closed。

隔离复现：实际iceberg方法mock两张OPEN/filled0各amount1，返回filled2/statusclosed，实际成交0。影响限于汇总返回状态、监控、回调及使用者后续决策；资金持仓本身仍由子单成交逻辑处理，因此不声称仅此导致持仓账本虚增。建议应用显式filled字段，汇总状态按children lifecycle计算，unknown不能作为filled，父单完成必须等确认终态。

### T8 · P2 · partial take profit零成交也消费完成标志并移除止盈目标

定位：[execution_engine.py:2365](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:2365)、[execution_engine.py:2380](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:2380)、[execution_engine.py:2387](F:/9_Crypto/crypto_trading_system/core/trading/execution_engine.py:2387)。触发：live减仓请求被交易所接受返回真值dict，但订单仍OPEN/filled0；helper只判断返回值真假。

隔离复现：实际helper接受filled0的OPEN订单，position.quantity仍2，却输出applied=True、partial_take_profit_done=True、take_profit=None。影响：未完成目标减仓时就停止该盈利保护步骤，并可能移除原止盈价；仅部分成交也没有按目标累计数量推进。建议基于确认fill数量推进状态，保存pending订单与目标进度，撤单/拒单后恢复可重试，只有达到已确认目标才清空原止盈。

## 附加已证实观察

**A3 · P3 · 离线PnLDecomposer丢开仓手续费/滑点。** pnl_decomposer.py:201保存lot fee/slippage，249–271消费lot时只累加平仓费用，未计lot开仓成本。买1@100fee2/slip3，卖1@110fee1/slip1得net8，正确3。全仓库引用扫描只找到该模块及test_pnl_decomposer_flip.py，未找到生产调用者，所以没有把它扩展为当前live会计损失；接入前应修复按消费量分摊entry lot costs。

## 与其他分报告合并边界

- **策略LIVE权限问题归web安全分报告。** 本线程确认其可执行资金链：strategy_manager.py:1228把runtime_mode=live写metadata；execution_engine.py:357–368由signal metadata优先取mode，4015–4018进入live _mode_guard；OrderRequest传mode；OrderManager.py:89–93优先取显式live并走真实提交。全局paper不提供阻断。没有重复列为T7。
- **行情旧时间戳/非有限价格归data分报告。** _resolve_price1392–1419仅provider.ok，未独立验证exchange timestamp/source/finite。有限但旧的价格可继续sizing；inf动态quantity通常归零或entry风险名义值超限，不能笼统声称inf必定提交真实订单。保护与explicit quantity/allow_close边界仍需防finite。
- **order_intent_router非法action问题归data分报告。** 本线程扫描该路径，避免重复认定。
- stop_loss.py源码注明NOT ON LIVE PATH；不把其遗留行为当当前实盘防护。DEX执行、交易所权限与逆向合约真实性没有联网验证。

## 覆盖和限制

分配目录共 **88个Python文件、36227行、1388个函数/方法**，全部清单扫描、AST解析与关键交易符号/风险模式扫描。此数字包括__init__，不代表每行均完整语义验证。下表S表示清单/AST/风险符号扫描；D表示在S基础上深读关键函数和上下游；V表示在D基础上执行隔离函数复现。D也不是整文件逐行证明。大型factor/intraday和其余策略以S覆盖，没有完整数学/回测正确性证明。

深读侧重：account凭据继承/策略账户/模式优先级；任务mode锁；订单幂等/缓存/查询取消/重试；持久化与恢复/条件单；持仓匹配/部分平仓/同账户reconcile；profit management；risk预检/日内停止/统计；connector精度/张数/订单响应；状态机/速率限制；runner恢复和退出信号；structural/market_state上下文与risk gate。整套测试结果由主审计报告统一公布，本分报告不借静态覆盖声明交易所端到端已安全。

| 文件 | 行数 | 函数 | 深度 |
|---|---:|---:|---|
| core/accounting/__init__.py | 2 | 0 | S |
| core/accounting/pnl_decomposer.py | 292 | 12 | V |
| core/events/__init__.py | 10 | 0 | S |
| core/events/supply_event_schema.py | 123 | 6 | D |
| core/events/supply_event_store.py | 92 | 9 | D |
| core/exchange_adapters/__init__.py | 20 | 0 | S |
| core/exchange_adapters/base.py | 125 | 12 | D |
| core/exchange_adapters/ccxt_adapter.py | 381 | 18 | D |
| core/exchanges/__init__.py | 55 | 0 | S |
| core/exchanges/base_exchange.py | 398 | 25 | D |
| core/exchanges/binance_connector.py | 764 | 29 | D |
| core/exchanges/bybit_connector.py | 352 | 17 | D |
| core/exchanges/dex_connectors.py | 358 | 29 | D |
| core/exchanges/exchange_manager.py | 435 | 22 | D |
| core/exchanges/gate_connector.py | 370 | 17 | D |
| core/exchanges/okx_connector.py | 353 | 17 | V |
| core/exchanges/order_parsing.py | 50 | 2 | D |
| core/execution/__init__.py | 23 | 0 | S |
| core/execution/order_intent_router.py | 109 | 4 | D |
| core/execution/order_state_machine.py | 408 | 21 | D |
| core/execution/rate_limit_and_reconnect.py | 245 | 20 | D |
| core/market_state/__init__.py | 2 | 0 | S |
| core/market_state/benchmark_beta.py | 192 | 7 | D |
| core/market_state/classifier.py | 179 | 4 | D |
| core/market_state/hysteresis.py | 43 | 2 | D |
| core/market_state/planner_adapter.py | 115 | 5 | D |
| core/market_state/schema.py | 42 | 2 | D |
| core/realtime/__init__.py | 5 | 0 | S |
| core/realtime/event_bus.py | 102 | 9 | D |
| core/risk/__init__.py | 42 | 0 | S |
| core/risk/circuit_breaker.py | 1231 | 50 | D |
| core/risk/position_sizer.py | 259 | 10 | D |
| core/risk/risk_manager.py | 1534 | 51 | V |
| core/risk/stop_loss.py | 322 | 19 | D |
| core/runtime/state.py | 270 | 21 | D |
| core/strategies/__init__.py | 52 | 0 | S |
| core/strategies/health_monitor.py | 141 | 8 | D |
| core/strategies/persistence.py | 328 | 11 | D |
| core/strategies/runtime_policy.py | 88 | 4 | D |
| core/strategies/signal_generator.py | 291 | 14 | D |
| core/strategies/strategy_base.py | 565 | 39 | D |
| core/strategies/strategy_manager.py | 2472 | 91 | D |
| core/structural/__init__.py | 34 | 0 | S |
| core/structural/context.py | 429 | 26 | D |
| core/structural/derivatives_crowding.py | 427 | 13 | D |
| core/structural/onchain_flow.py | 125 | 3 | D |
| core/structural/risk_gate.py | 126 | 5 | D |
| core/structural/supply_events.py | 251 | 12 | D |
| core/trading/__init__.py | 46 | 0 | S |
| core/trading/account_manager.py | 524 | 37 | D |
| core/trading/account_snapshot.py | 166 | 5 | D |
| core/trading/binance_rest.py | 411 | 18 | V |
| core/trading/equity_attribution.py | 33 | 1 | D |
| core/trading/execution_engine.py | 6904 | 158 | V |
| core/trading/order_manager.py | 1144 | 49 | V |
| core/trading/position_manager.py | 897 | 55 | V |
| strategies/__init__.py | 299 | 0 | S |
| strategies/ai/__init__.py | 5 | 0 | S |
| strategies/ai/ml_xgboost_strategy.py | 130 | 4 | S |
| strategies/arbitrage/__init__.py | 22 | 0 | S |
| strategies/arbitrage/cex_arbitrage.py | 700 | 31 | D |
| strategies/arbitrage/dex_arbitrage.py | 376 | 16 | D |
| strategies/event_driven/__init__.py | 4 | 0 | S |
| strategies/event_driven/supply_event_strategy.py | 208 | 7 | D |
| strategies/factor_based/__init__.py | 72 | 0 | S |
| strategies/factor_based/factor_strategies.py | 1968 | 71 | S |
| strategies/macro/__init__.py | 22 | 0 | S |
| strategies/macro/fund_flow.py | 503 | 17 | S |
| strategies/macro/kol_consensus.py | 164 | 6 | S |
| strategies/macro/market_sentiment.py | 479 | 18 | S |
| strategies/macro/onchain_flow_regime.py | 168 | 3 | D |
| strategies/quantitative/__init__.py | 80 | 0 | S |
| strategies/quantitative/altcoin_downtrend_bounce_short.py | 218 | 8 | S |
| strategies/quantitative/fama_factor_arbitrage.py | 432 | 12 | S |
| strategies/quantitative/intraday_cross_section.py | 1700 | 72 | S |
| strategies/quantitative/liquidation_oi_crowding.py | 204 | 5 | S |
| strategies/quantitative/mean_reversion.py | 257 | 8 | S |
| strategies/quantitative/momentum.py | 243 | 8 | S |
| strategies/quantitative/multi_factor_hf.py | 331 | 17 | S |
| strategies/quantitative/multi_factor_hf_fast.py | 312 | 7 | S |
| strategies/quantitative/oi_mcap_ambush.py | 474 | 21 | S |
| strategies/quantitative/pairs_trading.py | 351 | 12 | D |
| strategies/technical/__init__.py | 28 | 0 | S |
| strategies/technical/bollinger_strategy.py | 394 | 10 | S |
| strategies/technical/common_strategies.py | 383 | 14 | S |
| strategies/technical/ma_strategy.py | 313 | 10 | S |
| strategies/technical/macd_strategy.py | 275 | 9 | S |
| strategies/technical/rsi_strategy.py | 355 | 13 | S |

## 可复现审计产物

- [trading_repro.py](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/trading_repro.py)：AST提取函数，不import项目，不连接账户；涵盖T1–T8、A3。八个主断言均通过；A3有显式观测但未单独新增主断言。
- [trading_repro_results.json](F:/9_Crypto/crypto_trading_system/reports/full_audit_2026-10-01_parts/trading_repro_results.json)：观测值、revision与8项true断言。

在工程根目录运行：
```powershell
& 'F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe' -B reports\full_audit_2026-10-01_parts\trading_repro.py
```

复现未使用真实secret、数据库或持仓state；真实venue错误处理、tick size资料与合同类型需要部署前在隔离测试网补充验证。本报告描述当前基线的可证实缺陷，没有改代码或替用户决定修复策略。

