# 模块化适配层设计文档（适配当前 `crypto_trading_system`）

更新时间：2026-02-24

目标：在不推翻现有 live 下单接口的前提下，为 Binance 永续（5m、多空、高换手）补齐工程化基础设施，包括适配层、WS、订单状态一致性、记账分解、资金费率数据接入。

## 1. 设计原则（当前仓库约束）

1. 不改现有 `core/trading/*` 的主执行接口（增量改造）
2. 先做“旁路组件”，再逐步接线
3. 回测 / 模拟盘 / 实盘逐步统一字段口径（尤其 PnL 分解）
4. 优先解决稳定性问题：
- 限流
- 重连
- 订单状态乱序
- funding 处理

## 2. 当前状态（已落地 vs 规划中）

## 2.1 已有/已落地（本仓库已有文件）

已新增并可用（至少 skeleton + 部分实现）：

- `core/exchange_adapters/base.py`
- `core/exchange_adapters/ccxt_adapter.py`（只读能力优先）
- `core/marketdata/ws_client.py`
- `core/marketdata/binance_perp_ws_client.py`（骨架）
- `core/execution/order_state_machine.py`（已实现核心状态机）
- `core/execution/rate_limit_and_reconnect.py`（已实现 token bucket/退避）
- `core/execution/order_intent_router.py`（桥接骨架）
- `core/accounting/pnl_decomposer.py`（统一 PnL 分解方向）
- `core/backtest/funding_provider.py`（已实现本地缓存 + Binance funding 拉取）
- `core/backtest/cost_models.py`（成本模型抽离方向）

## 2.2 仍以规划为主（可继续接线）

- Binance 永续 WS 行情/用户流完整接入现有执行链路
- 订单状态机与真实下单执行链路联动
- 实盘与回测统一 accounting ledger
- 已注册实例参数逐个回测 compare 接口（当前 compare 仍主要按策略类型）

## 3. 推荐目录结构（当前已采用）

```text
core/
  exchange_adapters/
    __init__.py
    base.py
    ccxt_adapter.py
  marketdata/
    __init__.py
    ws_client.py
    binance_perp_ws_client.py
  execution/
    __init__.py
    order_state_machine.py
    order_intent_router.py
    rate_limit_and_reconnect.py
  accounting/
    __init__.py
    pnl_decomposer.py
  backtest/
    funding_provider.py
    cost_models.py
```

## 4. 各模块职责与建议接口

## 4.1 `core/exchange_adapters/base.py`

职责：

- 定义统一交易所适配接口（REST 为主）
- 隔离 `ccxt` / 原生 SDK / 交易所差异
- 统一市场信息、订单快照、持仓快照、funding 数据结构

建议接口（抽象层）：

- `initialize() -> None`
- `close() -> None`
- `fetch_markets(reload=False) -> list[...]`
- `fetch_ticker(symbol) -> dict`
- `fetch_balances() -> dict`
- `fetch_positions(symbols=None) -> list[...]`
- `create_order(...)`
- `cancel_order(...)`
- `fetch_order(...)`
- `fetch_open_orders(...)`
- `fetch_funding_rate(symbol)`
- `fetch_funding_history(symbol, start_time, end_time, limit=...)`

## 4.2 `core/exchange_adapters/ccxt_adapter.py`

职责：

- 使用 `ccxt` 实现 Binance/其他交易所的统一 REST 适配
- 先做只读（市场、资金费率、持仓、余额），再逐步做写操作
- 做 symbol、market type、异常、精度规则归一化

当前建议（与你仓库一致）：

- 研究/回测优先使用只读能力（特别是 funding）
- 实盘下单暂不强制切换到 adapter（降低改动风险）

## 4.3 `core/marketdata/ws_client.py`

职责：

- 通用 WebSocket 客户端框架
- 统一连接、断线重连、订阅恢复、消息分发

关键点：

- handler 注册机制
- 心跳/超时检测
- 重连 backoff（建议与 `rate_limit_and_reconnect.py` 共用策略）

## 4.4 `core/marketdata/binance_perp_ws_client.py`

职责：

- Binance 永续 WS 订阅封装与标准化输出

建议先接入的频道：

- `bookTicker`
- `markPrice`
- `kline_5m`
- `aggTrade`

后续可接：

- 用户流（订单回报、账户更新、持仓更新）

## 4.5 `core/execution/order_state_machine.py`

职责：

- 统一管理订单生命周期
- 处理 REST/WS 回报乱序、重复消息、终态回退问题

当前已实现能力（本仓库）：

- `submit -> ack -> partial -> filled/canceled/rejected`
- 多键索引（`order_id/client_order_id/exchange_order_id`）
- 终态不回退
- 重复事件基础去重
- `snapshot/export/restore`

建议接线点：

- 先作为“订单状态镜像”（不替换现有执行逻辑）
- 用于对账与 UI 展示一致性增强

## 4.6 `core/execution/order_intent_router.py`

职责：

- 将策略 `Signal` 转换为标准化 `OrderIntent`
- 为未来多交易所/多账户/多执行通道打桥接层

当前策略：

- 先保留为薄层，不替换现有 `execution_engine`
- 用于研究“同信号在不同执行通道”的兼容性

## 4.7 `core/execution/rate_limit_and_reconnect.py`

职责：

- REST 限流（token bucket）
- 重试/退避策略（含 jitter）
- 连接异常后的冷却与降级模式

当前已实现能力（本仓库）：

- 多 bucket 限流
- penalty/cooldown
- `reduce-only` 冷却模式（禁开仓、允许减仓）
- 同步/异步获取令牌
- 重连退避与统计输出

建议用法：

- 对交易所写操作、查询、WS 控制消息分 bucket 管理
- 将“超限/风控返回”统一映射到 penalty 逻辑

## 4.8 `core/accounting/pnl_decomposer.py`

职责：

- 统一实盘/模拟盘/回测的 PnL 分解字段
- 输出一致口径：
  - `gross_pnl`
  - `fee`
  - `slippage_cost`
  - `funding_pnl`
  - `net_pnl`

价值：

- 前端展示口径统一（避免“日内盈亏”和“浮盈亏”混淆）
- 研究报告更容易做成本归因

## 4.9 `core/backtest/funding_provider.py`

职责：

- 资金费率历史缓存与对齐
- 为回测引擎补齐 `funding_rate` 列

当前已实现（本仓库）：

- 本地缓存读写（parquet/csv）
- Binance 公共 funding 历史拉取
- 对齐到 OHLCV DataFrame 索引
- `scripts/research/pull_funding_cache.py` 脚本支持预拉取

## 4.10 `core/backtest/cost_models.py`

职责：

- 抽离并统一成本模型公式
- 让回测引擎与研究脚本共享同一成本逻辑

建议覆盖：

- flat / maker-taker fee
- dynamic slippage（`atr_pct/rv/spread_proxy`）
- funding 成本辅助计算

## 5. 分阶段落地计划（不破坏现有 live 逻辑）

## Phase A：旁路镜像（低风险）

目标：不动下单主链路，先建立可观测性与一致性基础。

任务：

1. `ccxt_adapter` 只读接入（市场、余额、持仓、funding）
2. `binance_perp_ws_client` 行情订阅（bookTicker/markPrice/kline）
3. `order_state_machine` 作为镜像状态机接收订单事件
4. `pnl_decomposer` 做镜像记账对比

产出：

- 更稳定的状态/订单展示
- 研究与实盘口径差异可审计

## Phase B：状态一致性与风控增强

目标：降低高换手场景下的拒单、乱序、重连波动。

任务：

1. 接入 `rate_limit_and_reconnect` 到交易所调用路径
2. 将交易所错误映射为统一 penalty/cooldown
3. 用 `order_state_machine` 驱动订单状态视图与对账

产出：

- 订单生命周期可追踪
- 重连/限流行为可控

## Phase C：回测/实盘口径统一深化

目标：研究结果更接近交易执行与账户表现。

任务：

1. 回测使用统一 `cost_models`
2. funding provider 与研究脚本、回测引擎统一
3. `pnl_decomposer` 字段逐步接入模拟盘/实盘统计与前端展示

产出：

- 成本与资金费率归因统一
- 页面展示与回测结果更一致

## 6. 与当前仓库的接线建议（具体）

## 6.1 不要做的事（当前阶段）

- 不要直接替换 `core/trading/execution_engine.py` 下单逻辑
- 不要一次性引入大型框架并替换现有策略系统

## 6.2 优先做的事（收益最高）

1. 用 `funding_provider` 强化永续研究口径（已部分完成）
2. 用 `rate_limit_and_reconnect` 管理交易所请求节奏（已具备基础实现）
3. 用 `order_state_machine` 做订单状态镜像与 UI 对账
4. 用 `ccxt_adapter` 统一只读数据来源

## 7. 验收建议（工程化）

1. 稳定性
- 状态接口不因某个慢依赖长期失败（已加缓存与兜底）
- WS 断线后能恢复订阅

2. 一致性
- 同一订单在 REST/WS/页面中的状态不回退
- PnL 分解字段在回测/模拟盘/实盘命名一致

3. 侵入性控制
- 未切换前，现有策略注册、下单、风控、页面功能不受影响

## 8. 当前文档对应文件（便于继续开发）

- 适配器设计：`docs/architecture_adapters.md`
- 开源选型：`docs/open_source_reference.md`
- 回测逻辑：`docs/backtest_logic_current.md`
- 因子公式：`docs/factor_formulas.md`

