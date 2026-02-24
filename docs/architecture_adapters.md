# 模块化适配层设计文档（适配 `crypto_trading_system`）

目标：增强现有仓库的 Binance 永续（5m、多空）稳定性与工程化，不推翻现有交易执行逻辑。

## 1. 设计目标

1. 保持现有 `core/trading/*` live 下单接口与业务逻辑不变
2. 新增“适配层 + 状态机 + WS 客户端 + 记账组件”，先旁路运行
3. 统一口径：订单状态、持仓、手续费、滑点、funding、PnL 分解
4. 让回测/模拟盘/实盘逐步共享同一套 accounting 字段

---

## 2. 最小可行 Patch 计划（MVP）

新增目录（不改现有业务逻辑）：

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

实施顺序：

### Phase A（旁路，不碰下单）
- `ccxt_adapter.py`：只读接口（market、position、funding）
- `binance_perp_ws_client.py`：只订阅行情，输出标准化事件
- `pnl_decomposer.py`：镜像记账（不替换现有统计）

### Phase B（小范围接入执行状态）
- `order_state_machine.py`：合并 REST/WS 订单状态
- `rate_limit_and_reconnect.py`：统一重试/退避策略

### Phase C（统一回测/实盘口径）
- `funding_provider.py`：历史 funding 数据对齐
- `cost_models.py`：回测与研究脚本复用成本公式

---

## 3. 模块职责与建议函数签名

## 3.1 `core/exchange_adapters/base.py`

职责：
- 定义交易所适配器抽象接口（REST）
- 统一返回对象：市场信息、订单快照、持仓快照、funding 数据点

建议接口：
- `async initialize() -> None`
- `async close() -> None`
- `async fetch_markets(reload: bool = False) -> list[MarketInfo]`
- `async fetch_ticker(symbol: str) -> dict`
- `async fetch_balances() -> dict`
- `async fetch_positions(symbols: list[str] | None = None) -> list[ExchangePositionSnapshot]`
- `async create_order(request: ExchangeOrderRequest) -> ExchangeOrderSnapshot`
- `async cancel_order(symbol: str, order_id: str, params: dict | None = None) -> ExchangeOrderSnapshot`
- `async fetch_order(symbol: str, order_id: str) -> ExchangeOrderSnapshot`
- `async fetch_open_orders(symbol: str | None = None) -> list[ExchangeOrderSnapshot]`
- `async fetch_funding_rate(symbol: str) -> FundingRatePoint | None`
- `async fetch_funding_history(symbol: str, start_time: datetime | None, end_time: datetime | None, limit: int = 200) -> list[FundingRatePoint]`

## 3.2 `core/exchange_adapters/ccxt_adapter.py`

职责：
- 使用 `ccxt` 实现 Binance 永续/合约 REST 适配器
- 做 symbol、market type、异常、精度/规则归一化

关键点：
- 支持 `swap/future`
- 统一 `BTC/USDT` vs `BTCUSDT` 转换
- 支持 `fetch_funding_rate / funding_history`

## 3.3 `core/marketdata/ws_client.py`

职责：
- 通用 WS 客户端框架：连接、断线重连、订阅恢复、handler 注册

建议接口：
- `register_handler(channel: str, handler: Callable[..., Awaitable[None]])`
- `async connect()`
- `async disconnect()`
- `async subscribe(payload: dict)`
- `async run_forever()`
- `async _dispatch(channel: str, message: dict)`

## 3.4 `core/marketdata/binance_perp_ws_client.py`

职责：
- Binance 永续 WS 订阅封装与事件标准化

建议接口：
- `async subscribe_book_ticker(symbols)`
- `async subscribe_depth(symbols, level=20)`
- `async subscribe_agg_trade(symbols)`
- `async subscribe_kline(symbols, interval="5m")`
- `async subscribe_mark_price(symbols)`
- `async subscribe_funding(symbols)`
- `normalize_event(message: dict) -> dict`

## 3.5 `core/execution/order_state_machine.py`

职责：
- 管理订单生命周期状态流转，解决重复/乱序回报

状态建议：
- `NEW`
- `SUBMITTED`
- `ACKED`
- `PARTIALLY_FILLED`
- `FILLED`
- `CANCELED`
- `REJECTED`
- `EXPIRED`

建议接口：
- `on_submit(...) -> OrderStateSnapshot`
- `apply_update(order_id, status, filled_qty=None, avg_price=None, raw=None) -> OrderStateSnapshot | None`
- `snapshot(order_id) -> OrderStateSnapshot | None`
- `all_open() -> list[OrderStateSnapshot]`

## 3.6 `core/execution/order_intent_router.py`

职责：
- 将现有策略 `Signal` 转换为 adapter 侧 `OrderIntent/OrderRequest`
- 初期仅做桥接层，不替换执行引擎

建议接口：
- `build_order_intent(signal, context=None) -> OrderIntent`
- `async submit_intent(intent) -> ExchangeOrderSnapshot`

## 3.7 `core/execution/rate_limit_and_reconnect.py`

职责：
- REST 限速（token bucket）
- WS 重连退避（指数退避 + 抖动）

建议接口：
- `configure_bucket(key, capacity, refill_per_sec)`
- `acquire(key, cost=1.0) -> bool`
- `record_success(key)`
- `record_failure(key)`
- `next_retry_delay(key, base=1.0, cap=30.0) -> float`

## 3.8 `core/accounting/pnl_decomposer.py`

职责：
- 统一 PnL 分解字段（回测/模拟盘/实盘）
- 统一输出：`gross_pnl / fee / slippage_cost / funding_pnl / net_pnl`

建议接口：
- `on_fill(...)`
- `on_funding(symbol, amount, ...)`
- `mark_to_market(marks: dict[str, float])`
- `position_snapshot(symbol) -> dict | None`
- `portfolio_breakdown() -> dict[str, float]`

## 3.9 `core/backtest/funding_provider.py`

职责：
- 提供回测资金费率历史读取与时间对齐

建议接口：
- `load_from_parquet(symbol, path)`
- `load_from_csv(symbol, path)`
- `get_rate(symbol, timestamp) -> float`
- `get_series(symbol, start_time=None, end_time=None) -> pd.Series`

## 3.10 `core/backtest/cost_models.py`

职责：
- 抽离回测/研究脚本成本公式，避免散落在多个模块

建议接口：
- `fee_rate(config, role="taker") -> float`
- `dynamic_slippage_rate(atr_pct, realized_vol, spread_proxy, params) -> float`

---

## 4. 与现有系统的接线策略（不改 live 下单接口）

### 4.1 旁路镜像阶段（推荐）
- 保留现有 `core/trading/execution_engine.py`
- 新模块仅做：
  - 行情订阅镜像（WS）
  - 订单状态镜像（state machine）
  - PnL 镜像记账（decomposer）

优点：
- 风险低
- 可对比现有口径与新口径差异

### 4.2 渐进替换阶段
- 先替换“读路径”：
  - 市场规则、funding 查询、持仓查询
- 再替换“状态归并路径”：
  - 订单状态由 `order_state_machine` 输出统一视图
- 最后才考虑替换“下单提交路径”

---

## 5. 验收建议（工程化）

1. 稳定性
- Binance WS 连续运行 4h+，自动重连后订阅恢复

2. 状态一致性
- 同一订单的 REST 与 WS 回报不会出现状态回退

3. 口径一致性
- `gross/fee/slippage/funding/net` 在回测、模拟盘、展示层字段一致

4. 侵入性控制
- 未接线前不影响现有策略注册、下单、风控逻辑

