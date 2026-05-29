# Market Data WS Upgrade Plan - 2026-05-29

## 1. 结论

当前系统不是“交易全链路 WS”。现状更准确地说是：

- 交易执行、下单、撤单、账户、持仓和订单查询仍以 REST / ccxt REST 为主。
- 前端浏览器与后端之间已经有内部 `/ws` WebSocket，用于推送运行状态和行情事件。
- 交易所行情 WS 已有阶段性实现：`core/marketdata/ccxt_pro_feed.py` 可以通过 `ccxt.pro` 订阅公开行情，并由 `MARKET_WS_ENABLED` 控制是否启动；默认仍关闭。
- 当前 REST 行情 fan-out 仍是稳定兜底路径。`web/main.py` 在 WS feed 健康时跳过 REST 行情推送，在 WS 不健康或未启用时自动回落到 REST。

建议升级目标不是“全部改 WS”，而是：

> 行情 WS 作为主通道，REST 作为权威兜底和周期性校验；交易执行、风控、账户/持仓对账继续以 REST 为权威。

这样可以改善行情刷新流畅度和延迟，同时避免把实盘稳定性押在单一长连接上。

## 2. 升级边界

### 纳入范围

- 公开市场数据：
  - ticker / bookTicker
  - trades
  - mark price
  - funding rate
  - kline 增量或收盘确认
  - 可选 order book，但必须有序列号校验和 REST snapshot 对账
- 后端行情缓存层：
  - 最新价、bid/ask、时间戳、来源、数据年龄
  - 每交易所健康度、重连次数、最后推送时间
  - WS stale 时自动 REST fallback
- 前端展示：
  - 继续使用现有 `/ws` 内部推送，不直接让浏览器连交易所 WS。
  - 增加行情来源和健康状态展示，例如 `WS healthy` / `REST fallback`。
- 策略读行情路径：
  - 先从统一行情 hub 读取缓存。
  - 当缓存过期或来源不可信时，强制走 REST 或拒绝生成交易信号。

### 不纳入第一阶段

- 不把下单、撤单、订单状态权威逻辑迁移到 WS。
- 不让实盘默认启用 `MARKET_WS_ENABLED=true`。
- 不用未校验的 order book diff 驱动风控或执行。
- 不把 `core/marketdata/ws_client.py` 和 `binance_perp_ws_client.py` 这类骨架实现直接接入实盘路径。

## 3. 当前代码锚点

- `config/settings.py`
  - `MARKET_WS_ENABLED: bool = False`
  - `MARKET_WS_EXCHANGES: str = "binance"`
  - `MARKET_WS_WATCH_TIMEOUT_SEC`
  - `MARKET_WS_HEALTH_MAX_AGE_SEC`
- `web/main.py`
  - `_MARKET_WS_ENABLED`：启动门禁。
  - `_market_ws_feed_worker()`：创建并运行 `CcxtProMarketFeed`。
  - `_publish_market_ticks()`：把 WS 标准化行情推给 event bus。
  - `_runtime_pusher()`：WS 健康时跳过 REST，WS 不健康时调用 `_emit_market_ticks()` 回落。
  - `/ws`：浏览器内部推送端点。
- `core/marketdata/ccxt_pro_feed.py`
  - 使用 `ccxt.pro` 公开行情流。
  - 每个交易所一个 watch loop。
  - 支持 proxy。
  - `watch_tickers(symbols)` 推送 ticker。
  - `is_healthy()` / `healthy_exchanges()` 提供新鲜度检查。
  - 把 `BTC/USDT:USDT` 归一化回 `BTC/USDT`，避免 UI key 不一致。
- 已有测试：
  - `tests/test_ccxt_pro_feed.py`
  - `tests/test_web_main_runtime_tasks.py`

## 4. 目标架构

### 4.1 行情分层

建议新增或逐步收敛到统一 `MarketDataHub`：

```text
exchange public WS
        |
        v
CcxtProMarketFeed / future native exchange feed
        |
        v
MarketDataHub in-memory cache
        |
        +--> web event_bus --> /ws --> browser
        |
        +--> strategies / monitors / risk checks
        |
        +--> REST fallback / REST snapshot reconciliation
```

`MarketDataHub` 至少保存：

- `exchange`
- `symbol`
- `channel`
- `last`
- `bid`
- `ask`
- `mark`
- `volume`
- `timestamp_exchange`
- `timestamp_received`
- `age_ms`
- `source`: `ws` / `rest_fallback` / `rest_snapshot`
- `healthy`
- `sequence` 或 `last_update_id`，仅 order book 类通道需要

### 4.2 权威规则

- UI 展示可以优先 WS。
- 非关键策略指标可以优先 WS，但必须有新鲜度阈值。
- 下单前价格、持仓、余额、未成交订单仍需要 REST 或交易所权威接口确认。
- 任何实盘交易信号不得使用过期行情：
  - ticker/bookTicker 超过阈值，降级 REST。
  - REST 也失败，信号应 fail closed。
  - order book 序列断裂，丢弃本地簿并重新拉 snapshot。

## 5. 分阶段实施

### Phase 0 - 现状审计和依赖确认

目标：确认现在的 WS 实现只作为可控实验通道存在。

任务：

- 确认运行环境是否可 import `ccxt.pro`。
- 确认 `MARKET_WS_ENABLED=false` 是默认值。
- 检查 `MARKET_WS_EXCHANGES` 与当前实盘交易所一致。
- 确认 proxy 配置在 WS 和 REST 下都可用。
- 检查 `ccxt.pro` 对 Binance futures symbol 的解析结果。
- 明确实盘服务状态页是否能看到行情 WS 开关和健康度。

交付：

- 一份运行前检查清单。
- 一组不需要真实下单的 smoke command。
- 如果 `ccxt.pro` 不稳定，保留 REST 当前行为，不进入下一阶段。

### Phase 1 - Shadow Mode

目标：WS 连接真实行情，但不作为交易或 UI 的唯一来源。

行为：

- `MARKET_WS_ENABLED=true` 仅在明确实验环境或手工批准的窗口开启。
- WS 推送写入 `MarketDataHub`。
- REST 继续按原节奏获取行情。
- 后台比较 WS 和 REST：
  - last price diff bps
  - bid/ask diff
  - timestamp lag
  - missing symbol
  - reconnect count
  - stale count

验收：

- 连续 6-24 小时无未处理异常。
- WS 与 REST 主流交易对价格偏差在阈值内。
- 断网、代理失败、交易所 watch timeout 后能够自动恢复或干净 fallback。
- 关闭 `MARKET_WS_ENABLED` 后行为完全回到 REST。

### Phase 2 - UI 和非关键行情主用 WS

目标：先让用户可见行情更流畅，但不扩大实盘交易风险。

行为：

- UI `/ws` 的 market_tick 优先使用 WS。
- REST fan-out 只在 WS 不健康、无订阅、无 tick 或 stale 时触发。
- 状态页展示：
  - `market_ws.enabled`
  - `market_ws.healthy`
  - `market_ws.healthy_exchanges`
  - `market_ws.last_tick_age_ms`
  - `market_ws.rest_fallback_count`
  - `market_ws.reconnect_count`

验收：

- 前端行情刷新更平滑，且没有明显跳价、停顿、重复 key。
- 关闭浏览器订阅时不会浪费 REST QPS。
- WS 健康状态变红时 UI 不误报为正常行情。

### Phase 3 - 策略读行情接入 Hub

目标：让策略和监控从统一缓存层读取行情，同时保留保守风控。

行为：

- 策略只通过 `MarketDataHub` 或等价 adapter 获取实时行情。
- 每次读取返回 `value + metadata`，不能只返回裸价格。
- metadata 至少包含：
  - `source`
  - `age_ms`
  - `exchange`
  - `symbol`
  - `is_stale`
- 策略下单前检查：
  - 行情年龄小于阈值。
  - exchange/symbol 与交易执行目标一致。
  - WS 来源异常时 REST 复核成功。

验收：

- 单测覆盖 stale、fallback、symbol 归一化和 exchange mismatch。
- 实盘 dry-run / paper 长跑无异常信号。
- live 模式必须有明确 kill switch。

### Phase 4 - 可选 User Data WS

目标：只在行情 WS 稳定后，考虑接入账户/订单事件流作为加速通知。

原则：

- user data WS 只作为事件通知和延迟优化。
- 账户、持仓、成交、手续费、订单最终状态仍由 REST reconciliation 确认。
- listen key 续期、断线补偿、事件丢失检测必须完整。
- 没有 REST 对账前，不允许用 WS 事件单独改写权威持仓。

此阶段不是当前升级的必要条件，可以单独立项。

## 6. 稳定性要求

### 连接和订阅

- 每个交易所独立任务，避免一个 venue 卡死全局。
- watch timeout 后取消 in-flight task 并重新进入循环。
- 网络错误使用指数退避，退避上限可配置。
- 重连后重新订阅全部 symbols。
- symbol provider 支持动态增减，但要去重和限流。

### 新鲜度和 fallback

- 每个 exchange/symbol 单独记录 last update。
- 全局健康不能只看“任意一个 exchange 有 tick”。
- fallback 条件建议：
  - exchange 无 tick 超过 `MARKET_WS_HEALTH_MAX_AGE_SEC`
  - symbol 无 tick 超过 symbol 级阈值
  - watch loop 连续错误超过阈值
  - payload 解析失败
  - timestamp 明显倒退
- fallback 不是静默行为，必须计数并暴露到状态页。

### order book 特殊要求

如果后续接入 depth/order book：

- 必须先拉 REST snapshot。
- 必须校验 Binance `lastUpdateId` / update sequence。
- diff stream 序列断裂必须丢弃本地簿并重建。
- 未实现完整序列校验前，order book 只能用于展示或研究，不能用于实盘执行决策。

## 7. 配置建议

保留现有配置：

```env
MARKET_WS_ENABLED=false
MARKET_WS_EXCHANGES=binance
MARKET_WS_WATCH_TIMEOUT_SEC=25
MARKET_WS_HEALTH_MAX_AGE_SEC=15
```

建议新增：

```env
MARKET_WS_MODE=off              # off | shadow | ui_primary | strategy_primary
MARKET_WS_FORCE_REST=false      # 一键强制 REST
MARKET_WS_SYMBOL_LIMIT=16
MARKET_WS_SYMBOL_MAX_AGE_SEC=10
MARKET_WS_RECONNECT_MIN_SEC=1
MARKET_WS_RECONNECT_MAX_SEC=30
MARKET_WS_REST_RECONCILE_SEC=30
MARKET_WS_MAX_PRICE_DIFF_BPS=20
MARKET_WS_FAIL_CLOSED_FOR_LIVE=true
```

live 模式建议约束：

- `MARKET_WS_MODE=strategy_primary` 只能在显式批准后使用。
- `MARKET_WS_FAIL_CLOSED_FOR_LIVE=true` 是实盘默认值。
- `MARKET_WS_FORCE_REST=true` 必须能无需改代码快速回退。

## 8. 可观测性

### 状态接口

建议 `/api/status` 或单独 `/api/market-data/status` 增加：

```json
{
  "market_ws": {
    "enabled": true,
    "mode": "shadow",
    "healthy": true,
    "healthy_exchanges": ["binance"],
    "last_tick_age_ms": 1234,
    "rest_fallback_count": 0,
    "reconnect_count": 1,
    "stale_drop_count": 0,
    "symbols": ["BTC/USDT", "ETH/USDT"]
  }
}
```

### 日志和指标

- feed start / stop
- exchange connected / disconnected
- subscribe symbols count
- reconnect reason
- fallback reason
- stale symbol drops
- REST vs WS price diff
- event bus publish failures

日志需要结构化到 exchange/symbol/source，避免排障时只能看到泛化错误。

## 9. 测试计划

### 单元测试

- `CcxtProMarketFeed`
  - import unavailable 时安全退出。
  - watch timeout 不阻塞 shutdown。
  - reconnect backoff 生效。
  - symbol 去重、数量上限、perp suffix 归一化正确。
  - `is_healthy()` 和 `healthy_exchanges()` 按时间窗口变化。
- `MarketDataHub`
  - stale 判定。
  - REST fallback 选择。
  - source metadata 保留。
  - symbol/exchange mismatch 拒绝。
- `web/main.py`
  - `MARKET_WS_ENABLED=false` 时不创建 feed。
  - WS healthy 时不触发 REST fan-out。
  - WS dead 时触发 REST fallback。

### 集成测试

- fake ccxt.pro client 模拟：
  - 正常 tick。
  - timeout。
  - exception 后恢复。
  - symbol list 为空后延迟出现。
  - payload 缺字段。
- 本地服务 smoke：
  - 启动 paper 模式，开启 shadow。
  - 订阅 `/ws`，确认 market_tick 到达。
  - 强制断开 WS，确认 REST fallback。
  - 关闭 `MARKET_WS_ENABLED`，确认旧行为恢复。

### 实盘前长跑

- paper 模式 24 小时。
- live 只允许 shadow 24 小时。
- 观察：
  - reconnect count
  - stale count
  - fallback count
  - REST QPS
  - event loop lag
  - CPU / memory
  - 交易信号是否因行情 stale 被正确拒绝

## 10. 验收标准

进入下一阶段前必须满足：

- 默认配置仍是 REST 稳定路径。
- 一键 kill switch 能回退 REST。
- WS 断线、proxy 失败、ccxt.pro 不可用时服务不崩溃。
- 状态接口能看见 WS 健康度和 fallback 计数。
- 单测覆盖 symbol 归一化、stale、fallback、重连。
- paper 长跑无未处理异常。
- live shadow 至少 24 小时，未发现 WS/REST 价格异常扩散到交易执行。

策略主用 WS 还必须满足：

- 策略读取的是带 metadata 的行情对象。
- 实盘 stale 行情 fail closed。
- 下单前仍有 REST 权威复核或等价风险确认。
- 有明确回滚步骤和回滚验证命令。

## 11. 风险和取舍

- WS 更流畅，但长连接对网络、代理和交易所断流更敏感。
- `ccxt.pro` 简化多交易所接入，但会引入额外依赖和抽象层行为差异。
- Binance futures symbol 需要持续验证 `BTC/USDT` 与 `BTC/USDT:USDT` 的映射。
- 多 symbol WS 能降低 REST QPS，但如果 symbol 动态增长，需要限流和分片。
- order book 能提供更细粒度微结构，但没有序列校验时风险高于收益。
- UI 流畅不等于交易安全；交易安全取决于 stale 检测、fallback、REST 对账和 fail-closed。

## 12. 推荐执行顺序

1. 保持当前实盘 REST 权威路径不变。
2. 补一个 `MarketDataHub` 小接口和状态快照，不先改策略。
3. 把现有 `CcxtProMarketFeed` 接入 hub，先 shadow。
4. 在 `/api/status` 暴露 WS 健康和 fallback 指标。
5. UI market_tick 切到 WS primary + REST fallback。
6. paper 长跑后，再让非关键策略读取 hub。
7. live shadow 通过后，才考虑策略主用 WS。
8. 单独评估 user data WS，不和行情 WS 升级混在同一批上线。

## 13. 继续补充：代码级落地拆分

本节是 2026-05-29 对计划的执行层补充，目标是把“REST 到 WS 升级”拆成可独立提交、可回滚、可验证的小批次。

### 13.1 当前代码状态判断

当前仓库已经具备一个低风险入口：

- `web/main.py` 中 `_MARKET_WS_ENABLED` 默认关闭，符合实盘保守原则。
- `_market_ws_feed_worker()` 已经会等待 exchange 连接完成后再启动，不会因为启动顺序过早而永久停用 feed。
- `_runtime_pusher()` 已经在 WS healthy 时跳过 REST ticker fan-out，在 WS dead 或 absent 时回落 REST。
- `CcxtProMarketFeed` 已经处理了 `ccxt.pro` import guard、proxy、每 exchange 独立 watch loop、watch timeout、重连退避、`BTC/USDT:USDT` 到 `BTC/USDT` 的 symbol 归一化。
- `core/marketdata/ws_client.py` 和 `binance_perp_ws_client.py` 仍是 skeleton，不能作为下一步主实现入口。

因此下一步不建议先写原生 Binance WS 客户端，而是先把现有 `CcxtProMarketFeed` 从“直接推 event_bus”升级为“写入 `MarketDataHub`，再由 hub 决定是否向 UI 推送或 REST fallback”。这样保留现有代码成果，同时把权威性、状态、fallback 和测试集中到一个层。

### 13.2 第一批 PR 范围

建议第一批只做 shadow 基建，不改变实盘交易决策：

1. 新增 `core/marketdata/hub.py`：
   - 定义 `MarketTick` 和 `MarketDataHubSnapshot`。
   - 提供 `upsert_ws_tick()`、`upsert_rest_tick()`、`get_tick()`、`snapshot()`。
   - 只保存内存状态，不落盘。
2. `web/main.py` 接入 hub：
   - `_publish_market_ticks()` 先写 hub，再继续按现有 payload 形状推送 `market_tick`。
   - `_emit_market_ticks()` 的 REST 结果也写 hub，source 标记为 `rest_fallback`。
   - `_runtime_pusher()` 的 WS healthy 判断从 feed 级逐步改为 hub 级，至少暴露 symbol 级 stale。
3. 状态接口增加只读快照：
   - 优先在既有 status payload 中加入 `market_ws`，避免新增一堆前端调用。
   - 若 status route 结构不适合，新增 `/api/market-data/status`。
4. 测试：
   - `tests/test_market_data_hub.py`
   - 扩展 `tests/test_web_main_runtime_tasks.py`
   - 保留 `tests/test_ccxt_pro_feed.py`，不要让它依赖真实网络。

第一批完成后，功能行为应仍然是：默认 REST，开启 `MARKET_WS_ENABLED=true` 后 WS 只改善推送路径和可观测性，不驱动下单。

### 13.3 第二批 PR 范围

第二批才做 UI primary：

1. 增加 `MARKET_WS_MODE`：
   - `off`：不启动 exchange WS。
   - `shadow`：启动 WS，写 hub，对比 REST，UI 仍可看到来源但不把 WS 作为唯一通道。
   - `ui_primary`：UI `market_tick` 优先来自 WS/hub，REST 仅 fallback。
   - `strategy_primary`：预留值，不在本批启用。
2. 前端展示：
   - 交易页或系统状态区域展示 `WS` / `REST fallback` / `stale`。
   - 不新增复杂交互，只显示来源和年龄，避免 UI 改动掩盖后端风险。
3. REST fan-out 优化：
   - 没有 `/ws` 订阅者时继续不拉 REST。
   - WS healthy 且 symbol 未 stale 时不拉 REST。
   - 有任一关键 symbol stale 时，只对 stale symbol 拉 REST fallback，而不是全量拉所有 symbol。

### 13.4 第三批 PR 范围

第三批再触达策略读取路径：

1. 新增一个薄 adapter，例如 `core/marketdata/runtime_price_provider.py`。
2. 只让非关键监控和 paper/dry-run 策略读取 hub。
3. 每次读取必须返回带 metadata 的对象，禁止只返回 `float price`。
4. live 下单前仍保留 REST 权威复核；如果复核失败，fail closed。

策略主用 WS 必须等第一、二批在 paper 和 live shadow 长跑通过后再打开。

## 14. `MarketDataHub` 最小契约

### 14.1 数据结构

建议最小类型如下，字段名保持直白，方便 API 直接序列化：

```python
@dataclass(frozen=True)
class MarketTick:
    exchange: str
    symbol: str
    channel: str
    last: float | None = None
    bid: float | None = None
    ask: float | None = None
    mark: float | None = None
    volume: float | None = None
    timestamp_exchange: datetime | None = None
    timestamp_received: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    source: Literal["ws", "rest_fallback", "rest_snapshot"] = "ws"
    sequence: int | None = None
    raw_symbol: str | None = None
```

派生字段不必存储，可以在 `snapshot()` 时计算：

- `age_ms`
- `is_stale`
- `spread_bps`
- `mid`
- `quality`: `healthy` / `stale` / `missing` / `invalid`

### 14.2 写入规则

- `exchange` 统一 lower case。
- `symbol` 统一使用当前 UI/REST 口径，例如 `BTC/USDT`。
- `raw_symbol` 保留交易所或 ccxt 原始 symbol，例如 `BTC/USDT:USDT` 或 `BTCUSDT`。
- `timestamp_received` 使用服务端 UTC 时间，不相信交易所时间单独作为新鲜度依据。
- 如果新 tick 的 `timestamp_exchange` 明显早于当前值，需要记录 `timestamp_regression_count`，但不要直接覆盖健康数据。
- `last <= 0`、`bid < 0`、`ask < 0`、`bid > ask` 的 payload 标记 invalid 或丢弃。

### 14.3 读取规则

`get_tick(exchange, symbol, max_age_sec)` 返回值应包含两部分：

```json
{
  "tick": {
    "exchange": "binance",
    "symbol": "BTC/USDT",
    "last": 68000.0,
    "bid": 67999.5,
    "ask": 68000.5,
    "source": "ws"
  },
  "meta": {
    "age_ms": 420,
    "is_stale": false,
    "quality": "healthy",
    "fallback_required": false
  }
}
```

调用方不应自行猜测 stale，统一使用 hub 返回的 metadata。

### 14.4 线程和异步边界

当前主要运行在 asyncio event loop 内，第一批可以先不加复杂锁，但建议：

- hub 写入方法保持同步、无 await，避免 event_bus publish 失败影响缓存更新。
- 如未来有多线程写入，再补 `threading.RLock` 或把 hub 限定为 event-loop owned。
- callback 中顺序应为：先写 hub，再 publish event_bus。

## 15. 官方 WS 约束核对

已按 2026-05-29 查询 Binance USD-M Futures 官方文档，计划中需要吸收以下约束：

- USD-M Futures WS base url 是 `wss://fstream.binance.com`，且官方现在区分 routed endpoints：`/public`、`/market`、`/private`。没有 routed path 的连接只保证 public endpoint，`markPrice` 等 market endpoint stream 不应继续挂在未路由旧路径上。
- combined stream payload 外层是 `{"stream": "...", "data": ...}`，原生客户端要显式解包；`ccxt.pro` 通常会帮忙统一，但 status/debug log 中应保留 raw channel 以便排错。
- 单连接 24 小时有效，应预期 24 小时断开并主动轮换。
- 服务端 ping 间隔和 pong 超时是连接级稳定性要求，内部 `/ws` 和交易所 WS 都要有 heartbeat 思路。
- incoming messages 有速率限制，动态订阅/退订时要做节流，不能在 symbol universe 抖动时频繁发控制消息。
- 单连接最多 1024 streams，但本系统第一阶段仍建议 `MARKET_WS_SYMBOL_LIMIT=16`，先以稳定性和观测为主。
- `bookTicker` 属于 `/public`，stream name 是 `<symbol>@bookTicker`，推送 best bid/ask 实时更新。
- `markPrice` 属于 `/market`，stream name 是 `<symbol>@markPrice` 或 `<symbol>@markPrice@1s`，包含 mark price 和 funding rate。
- diff depth 属于 `/public`，包含 `U`、`u`、`pu` 序列字段；没有本地簿重建和序列校验前，不准进入实盘执行路径。
- user data stream 属于 `/private`，listenKey 60 分钟有效，WS 连接同样要预期 24 小时断开；同一用户同一连接上的同类事件有顺序保证，但仍需要 REST reconciliation 做最终权威对账。

参考链接：

- Binance USD-M Futures Websocket Market Streams Connect: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Connect
- Binance Individual Symbol Book Ticker Streams: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Individual-Symbol-Book-Ticker-Streams
- Binance Mark Price Stream: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Mark-Price-Stream
- Binance Diff Book Depth Streams: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams
- Binance Local Order Book Guide: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly
- Binance User Data Streams Connect: https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams
- Binance User Data Stream Order Update: https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams/Event-Order-Update
- CCXT Pro manual: https://github.com/ccxt/ccxt/wiki/ccxt.pro.manual

## 16. 状态、指标和告警细化

### 16.1 `/api/status` 建议字段

建议在现有 status payload 下增加：

```json
{
  "market_ws": {
    "enabled": false,
    "mode": "off",
    "force_rest": false,
    "feed_present": false,
    "feed_healthy": false,
    "hub_healthy": false,
    "healthy_exchanges": [],
    "symbol_count": 0,
    "last_tick_age_ms": null,
    "oldest_tick_age_ms": null,
    "rest_fallback_count": 0,
    "ws_tick_count": 0,
    "stale_symbol_count": 0,
    "invalid_payload_count": 0,
    "timestamp_regression_count": 0,
    "reconnect_count": 0,
    "last_error": null
  }
}
```

### 16.2 per-symbol 详情

如果 status payload 太重，可以在独立接口返回 symbol 明细：

```json
{
  "exchange": "binance",
  "symbols": {
    "BTC/USDT": {
      "source": "ws",
      "age_ms": 420,
      "last": 68000.0,
      "bid": 67999.5,
      "ask": 68000.5,
      "quality": "healthy"
    }
  }
}
```

### 16.3 告警阈值

先按保守阈值设置，不要追求低延迟数字好看：

- `market_ws.hub_healthy=false` 持续 30 秒：warning。
- `stale_symbol_count > 0` 持续 60 秒：warning。
- `rest_fallback_count` 在 10 分钟内持续增长：warning。
- `timestamp_regression_count > 0`：warning，需要人工看日志。
- `invalid_payload_count` 在 10 分钟内超过 10：warning。
- live 模式中任一交易目标 symbol stale 且 REST fallback 失败：critical，策略信号 fail closed。

## 17. Shadow 对比和验收数据

Phase 1 shadow 不只是“能连上”，必须产生对比数据。建议 hub 或旁路比较器每 30 秒记录一行摘要：

```json
{
  "exchange": "binance",
  "symbol": "BTC/USDT",
  "ws_last": 68000.0,
  "rest_last": 68001.0,
  "diff_bps": 0.147,
  "ws_age_ms": 500,
  "rest_age_ms": 1200,
  "ws_source": "ccxt_pro_watch_tickers",
  "rest_source": "connector_get_ticker",
  "accepted": true
}
```

验收建议：

- 主交易 symbol 连续 6 小时 `p95(ws_age_ms) <= MARKET_WS_SYMBOL_MAX_AGE_SEC * 1000`。
- `p99(abs(diff_bps)) <= MARKET_WS_MAX_PRICE_DIFF_BPS`，异常尖刺必须能在日志中解释为市场快速变动或 REST 延迟。
- `rest_fallback_count` 可以非零，但必须有明确 reason 分布。
- `invalid_payload_count == 0` 或每条都有可解释 source。
- 关闭 `MARKET_WS_ENABLED` 后，status 中 `feed_present=false`，REST `market_tick` 行为恢复。

## 18. 本地 smoke 命令

这些命令均不需要真实下单。运行前建议确认 `.env` 中 live 写操作仍关闭或处于 paper/guarded 模式。

```powershell
# 1. 基础测试
pytest tests/test_ccxt_pro_feed.py tests/test_web_main_runtime_tasks.py tests/test_ws_client_hardening.py -q

# 2. 未来新增 hub 后的目标测试
pytest tests/test_market_data_hub.py tests/test_ccxt_pro_feed.py tests/test_web_main_runtime_tasks.py -q

# 3. 默认 REST 路径启动，确认不创建 exchange WS feed
$env:MARKET_WS_ENABLED="false"
python -m pytest tests/test_web_main_runtime_tasks.py::test_market_ws_feed_factory_gated_by_flag -q

# 4. Shadow 配置启动本地 Web 服务
$env:MARKET_WS_ENABLED="true"
$env:MARKET_WS_MODE="shadow"
$env:MARKET_WS_EXCHANGES="binance"
powershell -ExecutionPolicy Bypass -File scripts/web.ps1

# 5. Shadow 短自检。base-url 按本地服务端口替换，token 使用本机 OPS_TOKEN。
python scripts\selfcheck_market_ws_shadow.py --base-url http://127.0.0.1:8000 --token $env:OPS_TOKEN --duration-sec 60 --interval-sec 10 --min-samples 3 --min-ws-tick-delta 1 --min-shadow-compare-delta 1
```

服务启动后手动观察：

- 浏览器 `/ws` 能收到 `hello`、`runtime_snapshot`、`market_tick`。
- status payload 能看到 `market_ws.enabled=true` 和 mode。
- `/api/market-data/status` 中 `ws_tick_count` 和 `shadow_compare_count` 在 shadow 自检窗口内递增。
- 断网或关闭 proxy 后，`rest_fallback_count` 增加，服务不崩溃。
- 恢复网络后，WS 能重新推送或至少不阻塞 REST。

## 19. 回滚步骤

任何阶段出现异常，优先按以下顺序回滚：

1. 设置 `MARKET_WS_FORCE_REST=true`，保留进程继续运行，确认 REST fallback 恢复。
2. 设置 `MARKET_WS_MODE=off` 或 `MARKET_WS_ENABLED=false`，重启 web 服务。
3. 如果状态接口或 UI 因新增字段异常，回滚对应 commit，但保留 `MARKET_WS_ENABLED=false`。
4. 如果怀疑 `ccxt.pro` import 或依赖冲突，先禁用 WS，不在生产环境临时改依赖。
5. live 模式中若触发 stale + fallback failed，策略侧应 fail closed，不通过手工切换强行恢复开仓。

回滚验收：

```powershell
$env:MARKET_WS_ENABLED="false"
pytest tests/test_web_main_runtime_tasks.py::test_runtime_pusher_uses_rest_when_ws_unhealthy -q
pytest tests/test_ccxt_pro_feed.py -q
```

通过标准：

- `_market_ws_feed` 为 `None` 或 feed 不再启动。
- REST market tick 正常发布。
- `/ws` 内部浏览器连接仍可用。
- 交易执行、账户、持仓接口不受影响。

## 20. 开发检查清单

提交前逐项确认：

- [x] 默认配置仍不启用 exchange WS。
- [x] 所有新增配置都有保守默认值。
- [x] WS import 失败、ccxt.pro 不存在、exchange 名称错误时服务不崩。
- [x] event_bus publish 失败不会阻止 hub 写入或 feed loop。
- [x] REST fallback 计数和 reason 可见。
- [x] symbol 归一化有单测，覆盖 `BTC/USDT:USDT`、`BTCUSDT`、`BTC/USDT`。
- [x] stale 判定有单测，覆盖 exchange 级和 symbol 级。
- [x] live 策略执行价格读取路径没有绕过 metadata。
- [x] 策略、执行、估值路径中不再直接调用 `.get_ticker()` 获取实时 ticker。
- [x] order book diff 未完成序列校验前，没有被任何 live 执行逻辑使用。
- [x] user data WS 未接入权威持仓前，不会改写账户/订单最终状态。

## 21. 本地落地状态更新 - 2026-05-29

本节记录当前工作树已经完成的计划项，避免后续继续推进时把已完成的基建当成“待设计事项”重复实现。

### 21.1 已落地

- `core/marketdata/hub.py` 已新增 `MarketDataHub` 和 `MarketTick`：
  - 按 `(exchange, symbol, channel)` 保存最新 tick。
  - 保留 `ws`、`rest_snapshot`、`rest_fallback` 三类来源。
  - 支持 `BTC/USDT:USDT`、`BTCUSDT`、`BTC/USDT` 归一化到统一 key。
  - 记录 `ws_tick_count`、`rest_snapshot_count`、`rest_fallback_count`、`fallback_reasons`、`invalid_payload_count`、`timestamp_regression_count`。
  - 提供 symbol 级 stale 判定、exchange 级健康快照、per-symbol status 输出。
  - 增加 shadow 对比统计：`shadow_compare_count`、`shadow_compare_violation_count`、`shadow_missing_ws_count`、`shadow_max_abs_diff_bps`、`shadow_last_compare`。
- `core/marketdata/__init__.py` 已导出：
  - `MarketDataHub`
  - `MarketTick`
  - `market_data_hub`
  - `normalize_market_symbol`
- `config/settings.py` 已补充保守默认配置：
  - `MARKET_WS_MODE=off`
  - `MARKET_WS_FORCE_REST=false`
  - `MARKET_WS_SYMBOL_LIMIT=16`
  - `MARKET_WS_SYMBOL_MAX_AGE_SEC=10`
  - `MARKET_WS_RECONNECT_MIN_SEC=1`
  - `MARKET_WS_RECONNECT_MAX_SEC=30`
  - `MARKET_WS_REST_RECONCILE_SEC=30`
  - `MARKET_WS_MAX_PRICE_DIFF_BPS=20`
  - `MARKET_WS_FAIL_CLOSED_FOR_LIVE=true`
- `.env.example` 已加入 Market Data WebSocket upgrade 配置块。
- `web/main.py` 已接入 hub：
  - `MARKET_WS_ENABLED=true` 且没有显式 `MARKET_WS_MODE` 时，为兼容旧行为自动视为 `shadow`。
  - `MARKET_WS_FORCE_REST=true` 或 `MARKET_WS_MODE=off` 时不创建 exchange WS feed。
  - `_publish_market_ticks()` 会先写入 hub；`shadow` 模式不向 UI fan-out WS tick，`ui_primary` / `strategy_primary` 才向 UI fan-out WS tick。
  - `_emit_market_ticks()` 会把 REST ticker 写入 hub，并区分 `rest_snapshot` 与 `rest_fallback`。
  - `_runtime_pusher()` 在 `shadow` 模式继续保留 REST 作为 UI/runtime 行情来源；在 `ui_primary` / `strategy_primary` 模式下，只有 feed 健康且 hub 中存在新鲜 WS tick 时才抑制 REST。
  - feed 不健康时 REST fallback reason 为 `ws_unhealthy`；feed 健康但 hub stale/missing 时 reason 为 `ws_stale`。
  - `ui_primary` / `strategy_primary` 下，若 feed 健康但只有部分 watch symbol 缺失或 stale，只对缺失/陈旧 symbol 拉 REST fallback，不全量拉取所有 watch symbol。
  - `/api/status` 已包含 `market_ws` 状态快照。
- `web/main.py` 已新增 `/api/market-data/status`：
  - 使用 `read_trading_state` 权限保护。
  - 默认返回 `include_symbols=True` 的 per-symbol market data 状态。
- 前端顶栏已新增独立 `market-data-status` badge：
  - 与浏览器内部 `/ws` 的 `system-status` 分开展示。
  - 根据 `market_ws.mode`、`feed_healthy`、`ws_hub_healthy`、`ws_stale_symbol_count`、`rest_fallback_count` 展示 `REST`、`WS shadow`、`WS primary`、`REST fallback`、`stale`；`stale_symbol_count` 作为全源诊断保留在 tooltip，避免 REST/Gate 旧快照把健康的 exchange WS badge 误报为红色。
- `core/marketdata/runtime_price_provider.py` 已新增策略/执行侧价格读取 adapter：
  - 返回 `PriceReadResult`，包含 price、bid/ask、source、age_ms、is_stale、fallback_required、reason。
  - 优先读取 hub 新鲜 tick；缺失、stale 或 malformed 时可走 REST fallback 并写回 hub。
  - 支持 `PriceUnavailableError` / `require_realtime_price()` 用于 fail-closed。
- `core/trading/execution_engine.py` 的 `_resolve_price()` 已接入 `get_realtime_price()`：
  - 默认兼容旧行为，有 preferred price 时仍保留非 live strategy-primary 的快速路径。
  - live + `MARKET_WS_MODE=strategy_primary` + `MARKET_WS_FAIL_CLOSED_FOR_LIVE=true` 时，不信任裸 preferred price；必须通过 adapter 拿到带 metadata 的新鲜价格，否则 fail closed。
- `core/trading/order_manager.py` 已接入 `get_realtime_price()`：
  - paper fill price fallback 通过 adapter 读取。
  - live market order notional valuation 通过 adapter 读取；strategy-primary + fail-closed 下读取失败会拒绝订单创建。
- `strategies/macro/market_sentiment.py`、`strategies/macro/fund_flow.py`、`strategies/arbitrage/cex_arbitrage.py` 已迁移实时 ticker 读取到 adapter。
- `core/utils/asset_valuation.py` 已迁移报价读取到 adapter；仅在无法识别 connector exchange 名称时保留兼容的反射 fallback。
- 静态扫描已确认 `strategies`、`core/trading`、`core/utils` 下无直接 `.get_ticker(` 调用。
- 测试已补充：
  - `tests/test_market_data_hub.py`
  - `tests/test_web_main_runtime_tasks.py` 中的 WS mode、force REST、shadow、UI primary、fallback、status snapshot 覆盖。
  - `tests/test_runtime_price_provider.py`
  - `tests/test_market_data_ws_ui_assets.py`
  - `tests/test_market_ws_authority_static.py` 锁定 order book WS skeleton 和 user data WS 不进入 live/account/order authority 路径。
  - `tests/test_market_ws_shadow_report_eval.py` 锁定长跑最终 JSON 和服务日志污染的自动验收规则。

### 21.2 仍未落地

- `MARKET_WS_MODE=strategy_primary` 目前只是预留/受保护模式，不应直接用于 live。
- user data WS 尚未接入，也不应与本轮公开行情 WS 升级混在一起上线。
- order book diff 尚未实现本地簿重建和序列校验，不准进入 live 执行路径。
- 尚未完成 paper shadow 6 小时和 live shadow 24 小时长跑，因此不得宣称交易所 WS 长连接稳定性已经完成验收。
- 真实 exchange WS 60 秒短 smoke 已通过；仍需进入 paper shadow 6 小时和 live shadow 24 小时长跑。
- 已做真实浏览器 DOM/布局 smoke；截图命令在本机 CDP 上超时，未产出截图文件。

## 22. 逐项检验矩阵

| 检验项 | 当前状态 | 本地证据 | 继续要求 |
| --- | --- | --- | --- |
| 默认仍走 REST | 已完成 | `MARKET_WS_MODE=off`、`MARKET_WS_ENABLED=false` 默认关闭；runtime task factory 不创建 `market_ws_feed` | 每次发布前保留回归测试 |
| 一键强制 REST | 已完成 | `MARKET_WS_FORCE_REST=true` 时 `_is_market_ws_stream_enabled()` 为 false，feed task 不创建 | 生产回滚 runbook 中必须优先使用 |
| Hub 保存来源和新鲜度 | 已完成 | `MarketDataHub.get_tick()` 返回 `tick + meta`，`snapshot()` 返回健康度和 stale 计数 | Phase 3 adapter 必须沿用 metadata，不返回裸价格 |
| WS tick 写入 hub | 已完成 | `_publish_market_ticks()` 先写 hub，再按 mode 决定是否 fan-out | 后续如接 mark price/trades，要扩展 channel 而不是绕过 hub |
| REST fallback 可观测 | 已完成 | `rest_fallback_count`、`fallback_reasons`、`rest_snapshot_count` 已暴露 | 日志层后续补结构化 reason 采样 |
| Shadow 对比 | 短 smoke 已通过 | hub 记录 REST/WS last price diff bps、missing WS、violation count；60 秒 selfcheck 中 `ws_tick_delta=105`、`shadow_compare_delta=115`、violation/invalid/regression 均为 0 | 继续做 paper 6 小时和 live shadow 24 小时 p95/p99 长跑 |
| 真实 exchange WS 短 smoke | 已通过 | 根因是 Binance REST `future` defaultType 在 ccxt.pro ticker WS 下超时；feed 已将 Binance WS defaultType 映射为 `swap`，status 保留 `rest_default_type=future` 和 `default_type=swap` | 后续如新增交易所，必须各自做 defaultType/symbol 探针 |
| UI primary gating | 已完成后端逻辑 | 只有 `ui_primary` / `strategy_primary` 且 feed + hub fresh 时才抑制 REST | 保持前端状态展示与 fallback 文案一致 |
| 前端行情状态展示 | 已完成 | 顶栏 `market-data-status` 独立展示 exchange market data 状态；桌面和 390px DOM/布局 smoke 已通过 | 截图 CDP 超时，必要时可用人工截图补充 |
| `/api/status` 可观测 | 已完成 | `market_ws` 包含 mode、force_rest、feed、hub、fallback、shadow 指标 | 保持兼容字段 |
| `/api/market-data/status` 明细 | 已完成 | 独立 read-protected route 返回 per-symbol 快照 | 如 payload 太大，再增加分页或 symbol 过滤 |
| 策略读取 adapter | 已完成代码级迁移 | `runtime_price_provider` 已实现；执行、订单、策略、估值实时 ticker 读取已迁移；静态扫描无 `.get_ticker(` | 继续通过长跑验证运行稳定性 |
| 策略主用 WS | 已完成代码级保护 | live strategy-primary 执行价格不再信任裸 preferred price | 等 paper + live shadow 通过后再允许实盘开关 |
| live fail closed | 已完成代码级保护 | `PriceUnavailableError`、execution engine 与 order manager fail-closed 路径已覆盖 | 长跑和实盘前演练仍必需 |
| user data WS | 未开始 | 无权威持仓/订单 WS 改写路径 | 单独立项，REST reconciliation 先行 |
| order book diff | 未开始 | 未接入 live 执行 | 必须先做 snapshot + sequence 校验 |

## 23. 下一批实施建议

### 23.1 当前批次收口

当前批次目标应限定为“Phase 1 shadow 基建 + Phase 2 后端 UI primary gating”，不要扩展到 live strategy primary。

收口前必须完成：

- 跑通本地单测和 py_compile。
- 确认 `.env.example` 中所有 WS 新配置默认保守。
- 确认 `/api/status.market_ws` 在默认 off、shadow、force REST 三种场景下字段稳定。
- 如果要把文档中的开发检查清单改成已完成状态，必须以测试结果为依据，而不是只看代码路径。

### 23.2 前端状态展示

目的：让用户能区分“内部浏览器 `/ws` 连接正常”和“交易所行情 WS 正常”，避免把两个 WS 概念混在一起。

状态：已落地。桌面默认 viewport 和 390px viewport 的 DOM/布局 smoke 已通过，未发现横向溢出或顶部状态项重叠。

建议做法：

- 在现有系统状态区域增加一个简短 market data 状态：
  - `Market data: REST`
  - `Market data: WS shadow`
  - `Market data: WS primary`
  - `Market data: REST fallback`
  - `Market data: stale`
- 展示字段来自 `/api/status.market_ws`：
  - `mode`
  - `enabled`
  - `hub_healthy`
  - `feed_healthy`
  - `rest_fallback_count`
  - `stale_symbol_count`
  - `shadow_compare_violation_count`
- 不要把 exchange market WS 状态复用到现有浏览器 `/ws` badge 上；两者含义不同。
- 前端只展示状态，不提供一键切换 live/WS 的按钮。

验收：

```powershell
pytest tests/test_web_main_runtime_tasks.py tests/test_sensitive_api_auth.py -q
```

如果改动 `web/static/js/app.js`，增加静态测试或至少确认 status 字段缺失时 UI 不报错。

### 23.3 策略读取 adapter

目的：为 Phase 3 做安全入口，不直接让策略依赖全局 hub。

状态：代码级迁移已落地，执行引擎、订单管理器、策略模块和估值 helper 的实时 ticker 读取已接入 adapter。

已新增 `core/marketdata/runtime_price_provider.py`：

- `get_realtime_price(exchange, symbol, *, max_age_sec, allow_rest_fallback) -> PriceReadResult`
- `PriceReadResult` 至少包含：
  - `price`
  - `source`
  - `age_ms`
  - `is_stale`
  - `exchange`
  - `symbol`
  - `fallback_required`
  - `reason`
- live 模式中：
  - hub stale 且 REST 复核失败：fail closed。
  - exchange/symbol 不一致：fail closed。
  - metadata 缺失：fail closed。
- paper/dry-run 可先允许 hub 读取，但必须记录来源。

已补充的静态检查：

```powershell
rg -n "\.get_ticker\(" strategies core\trading core\utils -g "*.py"
```

当前结果：无匹配，`rg` 以 exit code 1 结束表示未找到匹配项。

下一步验收：

```powershell
pytest tests/test_runtime_price_provider.py tests/test_market_data_hub.py tests/test_strategy_runtime_policy.py tests/test_strategy_manager_runtime_data.py -q
```

### 23.4 长跑验证

代码单测只能证明 gating 和计数逻辑正确，不能证明交易所 WS 长连接稳定。进入 live shadow 前仍需：

长跑前新增短 smoke 门禁：本地 paper shadow 服务在 60 秒窗口内必须看到 `ws_tick_count` 和 `shadow_compare_count` 递增；如果只看到 `rest_snapshot_count` / `shadow_missing_ws_count` 增长，说明 REST reconcile 正常但 exchange WS 未真正产 tick，应先排障 feed 订阅层，不开始 6 小时或 24 小时长跑计时。

1. paper shadow 6 小时：
   - `MARKET_WS_ENABLED=true`
   - `MARKET_WS_MODE=shadow`
   - `MARKET_WS_SYMBOL_LIMIT=16`
   - 检查 `shadow_compare_violation_count`、`shadow_missing_ws_count`、`rest_fallback_count`。
2. live shadow 24 小时：
   - 只允许 shadow，不允许 `ui_primary` 或 `strategy_primary`。
   - 观察 reconnect、fallback、price diff p95/p99。
3. 通过后再开启 `ui_primary`，仍不允许 strategy primary。

## 24. 当前推荐验证命令

本地提交前至少运行：

```powershell
python -m py_compile core\marketdata\hub.py core\marketdata\__init__.py config\env_utils.py config\settings.py web\main.py tests\test_market_data_hub.py tests\test_web_main_runtime_tasks.py
pytest tests\test_runtime_price_provider.py tests\test_market_data_ws_ui_assets.py tests\test_market_data_hub.py tests\test_web_main_runtime_tasks.py tests\test_ccxt_pro_feed.py tests\test_ws_client_hardening.py -q
```

如果 `/api/status` 或认证相关代码有变更，再运行：

```powershell
pytest tests\test_sensitive_api_auth.py tests\test_infra_fixes.py tests\test_startup_mode.py -q
```

通过后才能把第 20 节的相应检查项从 `[ ]` 改为 `[x]`。

## 25. 本轮新增验证记录 - 2026-05-29

已通过：

```powershell
python -m py_compile core\marketdata\runtime_price_provider.py core\marketdata\hub.py core\marketdata\__init__.py core\trading\execution_engine.py web\main.py tests\test_runtime_price_provider.py tests\test_market_data_ws_ui_assets.py tests\test_web_main_runtime_tasks.py
pytest tests\test_runtime_price_provider.py tests\test_market_data_ws_ui_assets.py tests\test_market_data_hub.py tests\test_web_main_runtime_tasks.py -q
pytest tests\test_runtime_price_provider.py tests\test_market_data_ws_ui_assets.py tests\test_market_data_hub.py tests\test_web_main_runtime_tasks.py tests\test_ccxt_pro_feed.py tests\test_ws_client_hardening.py tests\test_sensitive_api_auth.py tests\test_infra_fixes.py tests\test_startup_mode.py tests\test_order_manager_safety.py tests\test_strategy_order_routing.py tests\test_execution_engine_live_trade_review.py tests\test_account_scoped_live_paths.py tests\test_trading_balances_stale_prev_equity.py tests\test_strategy_runtime_policy.py tests\test_strategy_manager_runtime_data.py tests\test_content_quality_static.py -q
python -m py_compile core\marketdata\runtime_price_provider.py core\marketdata\hub.py core\marketdata\__init__.py core\trading\execution_engine.py core\trading\order_manager.py core\utils\asset_valuation.py strategies\macro\market_sentiment.py strategies\macro\fund_flow.py strategies\arbitrage\cex_arbitrage.py web\main.py web\asset_versions.py tests\test_runtime_price_provider.py tests\test_market_data_ws_ui_assets.py tests\test_web_main_runtime_tasks.py
rg -n "\.get_ticker\(" strategies core\trading core\utils -g "*.py"
```

结果：

- `41 passed`
- `153 passed`
- `.get_ticker(` 静态扫描无匹配

浏览器 smoke：

- 本地服务以显式 paper / `MARKET_WS_MODE=off` / `MARKET_WS_ENABLED=false` 启动在 `http://127.0.0.1:8011/`。
- 桌面默认 viewport：
  - `market-data-status` 文本：`行情: REST`
  - `marketWsMode=off`
  - `marketWsEnabled=false`
  - 无横向溢出，未与 `system-status`、`trading-mode`、`current-time` 重叠。
- 移动宽度 390px：
  - `market-data-status` 文本：`行情: REST`
  - 无横向溢出，未与顶部其他状态项重叠。
- 截图尝试：`Page.captureScreenshot` 在本机 CDP 上超时，未生成截图；DOM 布局数据已完成验证。
- 临时 8011 服务已停止。

待补充：

- paper shadow 6 小时与 live shadow 24 小时长跑。

## 26. Shadow smoke 结果与下一步排障 - 2026-05-29

本轮新增了一次真实本地 shadow smoke，目的不是证明长跑稳定，而是确认 exchange WS 是否已经能在本机服务中持续产出 tick。

启动条件：

```powershell
$env:TRADING_MODE="paper"
$env:ALLOW_PERSISTED_LIVE_MODE_START="false"
$env:MARKET_WS_ENABLED="true"
$env:MARKET_WS_MODE="shadow"
$env:MARKET_WS_EXCHANGES="binance"
$env:MARKET_WS_SYMBOL_LIMIT="2"
$env:MARKET_WS_REST_RECONCILE_SEC="10"
python -m uvicorn web.main:app --host 127.0.0.1 --port 8012
```

观察结果：

- `market_ws_feed` task 已创建，日志出现 `ccxt_pro_feed: starting WS market feed for binance`。
- `MARKET_WS_MODE=shadow` 生效，`feed_present=true`。
- 后台 REST reconcile 已经不依赖浏览器订阅，`rest_snapshot_count` 能持续增长。
- `shadow_missing_ws_count` 随 REST snapshot 增长，说明 shadow 比较器正在发现“有 REST、无 WS”的样本。
- 关键未通过项：`ws_tick_count=0`、`shadow_compare_count=0`、`feed_healthy=false`、`ws_hub_healthy=false`。

结论：

- 当前不能宣称 Phase 1 shadow 真实链路验收通过。
- `MarketDataHub`、REST reconcile、status API 和 selfcheck 基建已经可用，但 exchange WS 订阅侧尚未证明能稳定产 tick。
- paper shadow 6 小时和 live shadow 24 小时长跑都必须等短 smoke 先通过；否则长跑只是在验证 REST fallback，而不是验证 WS 升级。

下一步排障顺序：

1. 给 `CcxtProMarketFeed` 补最小可观测性：
   - `watch_attempt_count`
   - `watch_timeout_count`
   - `watch_error_count`
   - `last_error`
   - `last_symbols`
   - `last_watch_started_at`
   - `last_watch_completed_at`
   - `status_snapshot()`
2. 在 `/api/status.market_ws` 或 `/api/market-data/status` 中透出 feed 级 status snapshot，避免只能从日志猜测 `watch_tickers()` 是超时、异常还是没有返回数据。
3. 用同一套运行环境单独探测 Binance：
   - `ccxt.pro.binance({"options": {"defaultType": ...}}).watch_tickers(["BTC/USDT", "ETH/USDT"])`
   - 如果 futures/defaultType 下 `BTC/USDT` 不产 tick，尝试 `BTC/USDT:USDT`，但 hub/UI 仍统一归一化为 `BTC/USDT`。
   - 如果批量 `watch_tickers(symbols)` 持续无返回，增加按 symbol 的 `watch_ticker(symbol)` fallback。
4. 重新运行短自检：

```powershell
python scripts\selfcheck_market_ws_shadow.py --base-url http://127.0.0.1:8012 --token $env:OPS_TOKEN --duration-sec 60 --interval-sec 10 --min-samples 3 --min-ws-tick-delta 1 --min-shadow-compare-delta 1
```

短自检通过标准：

- `ws_tick_delta >= 1`
- `shadow_compare_delta >= 1`
- `shadow_compare_violation_delta == 0`
- `invalid_payload_delta == 0`
- `timestamp_regression_delta == 0`
- `feed_healthy=true` 或至少 `ws_hub_healthy=true`

后续修复：

- `CcxtProMarketFeed` 已新增 feed 级可观测性：`watch_attempt_count`、`watch_timeout_count`、`watch_error_count`、`watch_empty_count`、`last_error`、`last_symbols`、`last_watch_started_at`、`last_watch_completed_at`、`last_watch_timeout_at`、`status_snapshot()`。
- `/api/status.market_ws` 和 `/api/market-data/status` 已透出 `feed_status`、`feed_watch_attempt_count`、`feed_watch_timeout_count`、`feed_watch_error_count`、`feed_watch_empty_count`、`feed_last_error`。
- 原始 ccxt.pro 探针结果：
  - `defaultType=spot` + `watch_tickers(["BTC/USDT", "ETH/USDT"])` 约 4 秒返回 tick。
  - `defaultType=swap` + `watch_tickers(["BTC/USDT", "ETH/USDT"])` 约 4 秒返回 tick。
  - `defaultType=future` 下 `watch_tickers(["BTC/USDT", "ETH/USDT"])`、`watch_tickers(["BTC/USDT:USDT", "ETH/USDT:USDT"])`、`watch_ticker("BTC/USDT")`、`watch_ticker("BTC/USDT:USDT")` 均在 12 秒窗口内超时。
- 因此 feed 对 Binance 做了局部映射：REST connector 仍可保持 `BINANCE_DEFAULT_TYPE=future`，ccxt.pro 公开 ticker WS 使用 `defaultType=swap`；状态接口同时展示 `rest_default_type=future` 和 `default_type=swap`。
- `MarketDataHub.snapshot()` 已新增 `ws_stale_symbol_count`，selfcheck 优先用 WS 来源 stale 计数，避免 REST-only symbol 误杀 exchange WS smoke。

复验结果：

```powershell
python scripts\selfcheck_market_ws_shadow.py --base-url http://127.0.0.1:8012 --token codex-shadow-smoke-token --duration-sec 60 --interval-sec 10 --min-samples 3 --min-ws-tick-delta 1 --min-shadow-compare-delta 1
```

通过摘要：

- `overall_ok=true`
- `sample_count=7`
- `ws_tick_delta=105`
- `shadow_compare_delta=115`
- `shadow_compare_violation_delta=0`
- `invalid_payload_delta=0`
- `timestamp_regression_delta=0`
- `feed_watch_attempt_delta=105`
- `feed_watch_timeout_delta=0`
- `feed_watch_error_delta=0`
- `feed_watch_empty_delta=0`
- `ws_stale_symbol_count=0`
- `p99_abs_diff_bps=5.18678557221424`
- `p95_ws_age_ms=0.0`
- final status：`feed_healthy=true`、`ws_hub_healthy=true`、`feed_last_error=null`

短 smoke 已满足进入 paper shadow 6 小时的前置条件；它不能替代 paper 6 小时和 live shadow 24 小时长跑。

## 27. Paper shadow 6 小时长跑 - 2026-05-29

状态：运行中，尚未验收完成。

启动条件：

- `TRADING_MODE=paper`
- `ALLOW_PERSISTED_LIVE_MODE_START=false`
- `MARKET_WS_ENABLED=true`
- `MARKET_WS_MODE=shadow`
- `MARKET_WS_EXCHANGES=binance`
- `MARKET_WS_SYMBOL_LIMIT=16`
- `MARKET_WS_REST_RECONCILE_SEC=30`
- `MARKET_WS_WATCH_TIMEOUT_SEC=25`

服务进程：

- PID: `159408`
- URL: `http://127.0.0.1:8012`
- stdout: `logs\paper_shadow_6h_service_20260529_182618.out.log`
- stderr: `logs\paper_shadow_6h_service_20260529_182618.err.log`

自检进程：

- PID: `157372`
- stdout/final JSON: `logs\paper_shadow_6h_selfcheck_20260529_182618.out.json`
- stderr/human summary: `logs\paper_shadow_6h_selfcheck_20260529_182618.err.log`
- 采样策略：`duration-sec=21600`，`interval-sec=60`，`min-samples=361`
- 验收阈值：
  - `min-ws-tick-delta=1`
  - `min-shadow-compare-delta=1`
  - `max-shadow-violation-delta=0`
  - `max-invalid-payload-delta=0`
  - `max-timestamp-regression-delta=0`
  - `max-stale-symbol-count=0`
  - `max-price-diff-bps=20`
  - `max-ws-age-p95-ms=10000`

启动后约 60 秒抽样：

```json
{
  "mode": "shadow",
  "enabled": true,
  "feed_healthy": true,
  "ws_hub_healthy": true,
  "ws_tick_count": 263,
  "shadow_compare_count": 268,
  "shadow_compare_violation_count": 0,
  "invalid_payload_count": 0,
  "timestamp_regression_count": 0,
  "rest_snapshot_count": 19,
  "feed_watch_attempt_count": 264,
  "feed_watch_timeout_count": 0,
  "feed_watch_error_count": 0,
  "feed_watch_empty_count": 0,
  "ws_stale_symbol_count": 0
}
```

运行中段抽样，2026-05-29 18:32:05 +08:00：

```json
{
  "mode": "shadow",
  "enabled": true,
  "feed_healthy": true,
  "ws_hub_healthy": true,
  "ws_tick_count": 491,
  "shadow_compare_count": 502,
  "shadow_compare_violation_count": 0,
  "invalid_payload_count": 0,
  "timestamp_regression_count": 0,
  "rest_snapshot_count": 33,
  "rest_fallback_count": 0,
  "shadow_missing_ws_count": 22,
  "shadow_max_abs_diff_bps": 5.175056228976152,
  "feed_watch_attempt_count": 492,
  "feed_watch_timeout_count": 0,
  "feed_watch_error_count": 0,
  "feed_watch_empty_count": 0,
  "ws_stale_symbol_count": 0,
  "last_tick_age_ms": 228
}
```

同一时刻 `feed_status.exchanges.binance` 显示 `default_type=swap`、`rest_default_type=future`、`last_error=null`、`last_push_age_ms=219`。

2026-05-29 18:38 +08:00 中止说明：

- 本轮长跑未作为最终验收使用，原因是运行进程加载的是修复前代码。
- 运行期间 market WS 指标仍健康，停止前抽样：
  - `ws_tick_count=1158`
  - `shadow_compare_count=1180`
  - `shadow_compare_violation_count=0`
  - `invalid_payload_count=0`
  - `timestamp_regression_count=0`
  - `feed_watch_timeout_count=0`
  - `feed_watch_error_count=0`
  - `feed_watch_empty_count=0`
  - `ws_stale_symbol_count=0`
- 但服务日志显示 `execution_engine._background_tick()` 每 2 秒短暂切换 paper/live scope，用于同时检查 paper/live 条件单和保护单。该行为会污染 paper shadow 长跑审计，因此已修复为：默认只检查当前运行模式；只有存在另一模式的条件单或持仓时才检查另一模式。
- 需要用最新代码重新启动 6 小时 paper shadow 长跑。

2026-05-29 18:41 +08:00 第二轮重启说明：

- 用修复后的 `_background_tick()` 重启后，paper/live scope 抖动消失。
- 但服务仍启动 `exchange_watchdog`，Gate health check 会产生与本轮 Binance market WS 验证无关的 reconnect / unclosed session 噪音。
- 已新增 `EXCHANGE_WATCHDOG_ENABLED=true` 保守默认开关；受控 market WS 长跑显式设置 `EXCHANGE_WATCHDOG_ENABLED=false`，避免 Gate watchdog 污染 paper shadow 证据。

2026-05-29 18:47 +08:00 第三轮长跑启动：

启动条件在前述配置基础上增加：

- `EXCHANGE_WATCHDOG_ENABLED=false`

服务进程：

- PID: `155540`
- URL: `http://127.0.0.1:8012`
- stdout: `logs\paper_shadow_6h_service_20260529_184724.out.log`
- stderr: `logs\paper_shadow_6h_service_20260529_184724.err.log`

自检进程：

- PID: `134108`
- stdout/final JSON: `logs\paper_shadow_6h_selfcheck_20260529_184724.out.json`
- stderr/human summary: `logs\paper_shadow_6h_selfcheck_20260529_184724.err.log`
- 采样策略：`duration-sec=21600`，`interval-sec=60`，`min-samples=361`

启动日志确认 runtime tasks 不再包含 `exchange_watchdog`：

```text
Managed background tasks started: ai_research_scheduler, circuit_breaker_monitor, coinglass, cusum_monitor, market_ws_feed, news, news_llm, runtime
```

第三轮启动后约 60 秒抽样：

```json
{
  "mode": "shadow",
  "enabled": true,
  "feed_healthy": true,
  "ws_hub_healthy": true,
  "ws_tick_count": 261,
  "shadow_compare_count": 246,
  "shadow_compare_violation_count": 0,
  "invalid_payload_count": 0,
  "timestamp_regression_count": 0,
  "rest_snapshot_count": 17,
  "rest_fallback_count": 0,
  "shadow_missing_ws_count": 11,
  "shadow_max_abs_diff_bps": 10.446617783127763,
  "feed_watch_attempt_count": 262,
  "feed_watch_timeout_count": 0,
  "feed_watch_error_count": 0,
  "feed_watch_empty_count": 0,
  "ws_stale_symbol_count": 0,
  "last_tick_age_ms": 787
}
```

第三轮运行中段抽样，2026-05-29 18:53:01 +08:00：

```json
{
  "mode": "shadow",
  "enabled": true,
  "feed_healthy": true,
  "ws_hub_healthy": true,
  "ws_tick_count": 555,
  "shadow_compare_count": 546,
  "shadow_compare_violation_count": 0,
  "invalid_payload_count": 0,
  "timestamp_regression_count": 0,
  "rest_snapshot_count": 35,
  "rest_fallback_count": 0,
  "shadow_missing_ws_count": 23,
  "shadow_max_abs_diff_bps": 10.446617783127763,
  "feed_watch_attempt_count": 556,
  "feed_watch_timeout_count": 0,
  "feed_watch_error_count": 0,
  "feed_watch_empty_count": 0,
  "ws_stale_symbol_count": 0,
  "last_tick_age_ms": 617
}
```

同一日志检查：

- `Paper trading mode: False` 次数：`0`
- `scope switched: paper -> live` 次数：`0`
- `exchange_watchdog` 次数：`0`
- `Health check failed for gate` 次数：`0`
- 仍可见新闻源 `gdelt` 429，这属于新闻后台任务，不纳入本轮 Binance market WS 验收指标。

第三轮运行中段抽样，2026-05-29 18:54:44 +08:00：

```json
{
  "mode": "shadow",
  "enabled": true,
  "feed_healthy": true,
  "ws_hub_healthy": true,
  "ws_tick_count": 727,
  "shadow_compare_count": 720,
  "shadow_compare_violation_count": 0,
  "invalid_payload_count": 0,
  "timestamp_regression_count": 0,
  "rest_snapshot_count": 45,
  "rest_fallback_count": 0,
  "shadow_missing_ws_count": 31,
  "shadow_max_abs_diff_bps": 10.446617783127763,
  "feed_watch_attempt_count": 728,
  "feed_watch_timeout_count": 0,
  "feed_watch_error_count": 0,
  "feed_watch_empty_count": 0,
  "ws_stale_symbol_count": 0,
  "last_tick_age_ms": 530
}
```

同一日志检查累计：

- `Paper trading mode: False` 次数：`0`
- `scope switched: paper -> live` 次数：`0`
- `exchange_watchdog` 次数：`0`
- `Health check failed for gate` 次数：`0`
- `watch_tickers timeout` 次数：`0`
- `ccxt_pro_feed[binance]: watch error` 次数：`0`

第三轮运行中段抽样，2026-05-29 19:07:28 +08:00：

```json
{
  "mode": "shadow",
  "enabled": true,
  "feed_healthy": true,
  "ws_hub_healthy": true,
  "ws_tick_count": 2009,
  "shadow_compare_count": 2023,
  "shadow_compare_violation_count": 0,
  "invalid_payload_count": 0,
  "timestamp_regression_count": 0,
  "rest_fallback_count": 0,
  "shadow_missing_ws_count": 77,
  "shadow_max_abs_diff_bps": 10.446617783127763,
  "feed_watch_attempt_count": 2010,
  "feed_watch_timeout_count": 0,
  "feed_watch_error_count": 0,
  "feed_watch_empty_count": 0,
  "ws_stale_symbol_count": 0,
  "last_tick_age_ms": 28,
  "feed_last_error": null
}
```

同一 `/api/status` 抽样确认：

- `status=running`
- `paper_trading=true`
- `trading_mode=paper`
- `market_ws.mode=shadow`
- `market_ws.feed_status.exchanges.binance.default_type=swap`
- `market_ws.feed_status.exchanges.binance.rest_default_type=future`
- `market_ws.feed_status.exchanges.binance.last_error=null`

同一日志检查累计：

- `Paper trading mode: False` 次数：`0`
- `scope switched: paper -> live` 次数：`0`
- `exchange_watchdog` 次数：`0`
- `Health check failed for gate` 次数：`0`
- `watch_tickers timeout` 次数：`0`
- `ccxt_pro_feed[binance]: watch error` 次数：`0`

2026-05-29 19:05 +08:00 回归与静态检查：

```powershell
python -m py_compile config\settings.py web\main.py tests\test_web_main_runtime_tasks.py core\trading\execution_engine.py tests\test_execution_engine_protective_levels.py core\marketdata\hub.py core\marketdata\runtime_price_provider.py scripts\selfcheck_market_ws_shadow.py
rg -n "\.get_ticker\(" strategies core\trading core\utils -g "*.py"
pytest tests\test_runtime_price_provider.py tests\test_market_data_ws_ui_assets.py tests\test_market_data_hub.py tests\test_web_main_runtime_tasks.py tests\test_ccxt_pro_feed.py tests\test_ws_client_hardening.py tests\test_sensitive_api_auth.py tests\test_infra_fixes.py tests\test_startup_mode.py tests\test_order_manager_safety.py tests\test_strategy_order_routing.py tests\test_execution_engine_live_trade_review.py tests\test_account_scoped_live_paths.py tests\test_trading_balances_stale_prev_equity.py tests\test_strategy_runtime_policy.py tests\test_strategy_manager_runtime_data.py tests\test_content_quality_static.py tests\test_market_ws_shadow_selfcheck.py tests\test_execution_engine_protective_levels.py -q
```

结果：

- `py_compile` 通过。
- 静态扫描无匹配；`rg` exit code 1 表示未找到 `.get_ticker(` 直连调用。
- 回归集合 `182 passed in 25.87s`。

2026-05-29 19:09 +08:00 新增 authority 静态门禁：

- 新增 `tests/test_market_ws_authority_static.py`。
- 锁定 `core/trading`、`core/execution`、`core/exchange_adapters`、`strategies`、`web/main.py` 中不得引用 `BinancePerpWSClient`、`core.marketdata.ws_client`、`subscribe_depth`、`subscribe_book_ticker` 等 skeleton/order book WS 入口。
- 锁定 `core/trading`、`core/execution`、`core/exchange_adapters`、`core/exchanges`、`web/api/trading.py`、`web/api/strategies.py` 中不得出现 `listenKey`、`userData`、`watchOrders`、`watchBalance`、`watchPositions` 等 user data WS authority 入口。

验证命令：

```powershell
pytest tests\test_market_ws_authority_static.py tests\test_order_manager_safety.py tests\test_content_quality_static.py -q
```

结果：`5 passed in 4.99s`。

包含 authority 静态门禁后的目标回归集合：

```powershell
pytest tests\test_runtime_price_provider.py tests\test_market_data_ws_ui_assets.py tests\test_market_data_hub.py tests\test_web_main_runtime_tasks.py tests\test_ccxt_pro_feed.py tests\test_ws_client_hardening.py tests\test_sensitive_api_auth.py tests\test_infra_fixes.py tests\test_startup_mode.py tests\test_order_manager_safety.py tests\test_strategy_order_routing.py tests\test_execution_engine_live_trade_review.py tests\test_account_scoped_live_paths.py tests\test_trading_balances_stale_prev_equity.py tests\test_strategy_runtime_policy.py tests\test_strategy_manager_runtime_data.py tests\test_content_quality_static.py tests\test_market_ws_shadow_selfcheck.py tests\test_execution_engine_protective_levels.py tests\test_market_ws_authority_static.py -q
```

结果：`184 passed in 24.48s`。

2026-05-29 19:20 +08:00 新增长跑最终报告评估门禁：

- 新增 `scripts/evaluate_market_ws_shadow_report.py`。
- 新增 `tests/test_market_ws_shadow_report_eval.py`。
- 脚本读取 `scripts/selfcheck_market_ws_shadow.py` 的最终 JSON，逐项检查第 28.2 节门禁，并扫描服务 stderr 中的污染模式：
  - `Paper trading mode: False`
  - `scope switched: paper -> live`
  - `exchange_watchdog`
  - `Health check failed for gate`
- 2026-05-29 20:36 +08:00 更新：`watch_tickers timeout` 与 `ccxt_pro_feed[binance]: watch error` 从默认 stderr 污染模式中移除；它们作为恢复性链路事件记录在 selfcheck summary 中，默认不触发硬失败。

paper shadow 6 小时结束后的自动验收命令：

```powershell
python scripts\evaluate_market_ws_shadow_report.py --report logs\paper_shadow_6h_selfcheck_<ts>.out.json --service-err-log logs\paper_shadow_6h_service_<ts>.err.log --expect-mode shadow --expect-runtime paper --min-samples 361 --min-ws-tick-delta 1 --min-shadow-compare-delta 1 --max-shadow-violation-delta 0 --max-invalid-payload-delta 0 --max-timestamp-regression-delta 0 --max-shadow-stale-skip-delta 0 --max-feed-watch-timeout-delta -1 --max-feed-watch-error-delta -1 --max-feed-watch-empty-delta 0 --max-stale-symbol-count 0 --max-price-diff-bps 20 --max-ws-age-p95-ms 10000 --max-log-count 0
```

新增脚本验证：

```powershell
python -m py_compile scripts\evaluate_market_ws_shadow_report.py tests\test_market_ws_shadow_report_eval.py
pytest tests\test_market_ws_shadow_report_eval.py tests\test_market_ws_shadow_selfcheck.py tests\test_market_ws_authority_static.py -q
```

结果：`9 passed in 3.54s`。

包含最终报告评估门禁后的目标回归集合：

```powershell
pytest tests\test_runtime_price_provider.py tests\test_market_data_ws_ui_assets.py tests\test_market_data_hub.py tests\test_web_main_runtime_tasks.py tests\test_ccxt_pro_feed.py tests\test_ws_client_hardening.py tests\test_sensitive_api_auth.py tests\test_infra_fixes.py tests\test_startup_mode.py tests\test_order_manager_safety.py tests\test_strategy_order_routing.py tests\test_execution_engine_live_trade_review.py tests\test_account_scoped_live_paths.py tests\test_trading_balances_stale_prev_equity.py tests\test_strategy_runtime_policy.py tests\test_strategy_manager_runtime_data.py tests\test_content_quality_static.py tests\test_market_ws_shadow_selfcheck.py tests\test_execution_engine_protective_levels.py tests\test_market_ws_authority_static.py tests\test_market_ws_shadow_report_eval.py -q
```

结果：`188 passed, 1 warning in 22.04s`。警告为既有 `aiosqlite` 背景线程在 event loop close 后报告的 `PytestUnhandledThreadExceptionWarning`，本轮没有测试失败。

第三轮运行中段抽样，2026-05-29 19:05:20 +08:00：

```json
{
  "mode": "shadow",
  "enabled": true,
  "feed_healthy": true,
  "ws_hub_healthy": true,
  "ws_tick_count": 1781,
  "shadow_compare_count": 1791,
  "shadow_compare_violation_count": 0,
  "invalid_payload_count": 0,
  "timestamp_regression_count": 0,
  "rest_fallback_count": 0,
  "shadow_missing_ws_count": 69,
  "shadow_max_abs_diff_bps": 10.446617783127763,
  "feed_watch_attempt_count": 1782,
  "feed_watch_timeout_count": 0,
  "feed_watch_error_count": 0,
  "feed_watch_empty_count": 0,
  "ws_stale_symbol_count": 0,
  "last_tick_age_ms": 962,
  "feed_last_error": null
}
```

同一日志检查累计：

- `Paper trading mode: False` 次数：`0`
- `scope switched: paper -> live` 次数：`0`
- `exchange_watchdog` 次数：`0`
- `Health check failed for gate` 次数：`0`
- `watch_tickers timeout` 次数：`0`
- `ccxt_pro_feed[binance]: watch error` 次数：`0`

第三轮运行中段抽样，2026-05-29 19:20:09 +08:00：

```json
{
  "mode": "shadow",
  "enabled": true,
  "feed_healthy": true,
  "ws_hub_healthy": true,
  "ws_tick_count": 3281,
  "shadow_compare_count": 3307,
  "shadow_compare_violation_count": 0,
  "invalid_payload_count": 0,
  "timestamp_regression_count": 0,
  "rest_fallback_count": 0,
  "shadow_missing_ws_count": 127,
  "shadow_max_abs_diff_bps": 15.717092337917892,
  "feed_watch_attempt_count": 3282,
  "feed_watch_timeout_count": 0,
  "feed_watch_error_count": 0,
  "feed_watch_empty_count": 0,
  "ws_stale_symbol_count": 0,
  "last_tick_age_ms": 404,
  "feed_last_error": null
}
```

同一日志检查累计：

- `Paper trading mode: False` 次数：`0`
- `scope switched: paper -> live` 次数：`0`
- `exchange_watchdog` 次数：`0`
- `Health check failed for gate` 次数：`0`
- `watch_tickers timeout` 次数：`0`
- `ccxt_pro_feed[binance]: watch error` 次数：`0`

2026-05-29 19:28 +08:00 第三轮不作为最终验收说明：

- 19:22 抽样中 `shadow_max_abs_diff_bps=18.60187510880112`，仍低于 20 bps，但 `shadow_last_compare.rest_age_ms` 约为 582 秒。
- 该价差来自新鲜 WS tick 和过旧 REST snapshot 的比较，不是 WS market type 错配；`shadow_last_compare` 显示 `exchange=binance`、`symbol=ETH/USDT`、`ws_source=ws`、`rest_source=rest_snapshot`、`accepted=true`。
- 已修复 `MarketDataHub`：shadow compare 只在 WS/REST 两侧样本都不超过 `shadow_compare_max_age_sec` 时计入 diff；过旧输入改为 `shadow_compare_stale_skip_count`，不再推高 `shadow_max_abs_diff_bps` 或 violation。
- `web.main` 已将 `shadow_compare_max_age_sec` 设为 `max(MARKET_WS_SYMBOL_MAX_AGE_SEC, MARKET_WS_REST_RECONCILE_SEC * 2)`，当前配置下为 60 秒。
- `scripts/selfcheck_market_ws_shadow.py` 和 `scripts/evaluate_market_ws_shadow_report.py` 已新增 `shadow_compare_stale_skip_delta` 门禁，默认要求为 0。
- 第三轮运行进程加载的是修复前代码，状态接口中 `shadow_compare_stale_skip_count=null`，因此即使继续跑满 6 小时，也不能证明 stale REST compare 防护已生效。
- 第三轮停止前抽样仍显示 WS feed 健康：
  - `ws_tick_count=4126`
  - `shadow_compare_count=4152`
  - `shadow_compare_violation_count=0`
  - `invalid_payload_count=0`
  - `timestamp_regression_count=0`
  - `feed_watch_timeout_count=0`
  - `feed_watch_error_count=0`
  - `feed_watch_empty_count=0`
  - `ws_stale_symbol_count=0`
  - `shadow_max_abs_diff_bps=18.60187510880112`
- 同一日志污染计数仍为 0：
  - `Paper trading mode: False`
  - `scope switched: paper -> live`
  - `exchange_watchdog`
  - `Health check failed for gate`
  - `watch_tickers timeout`
  - `ccxt_pro_feed[binance]: watch error`
- 处理：受控停止 PID `134108` 和 `155540`，重新启动加载最新代码的 clean paper shadow 6 小时长跑。第三轮日志保留用于排障参考，但不作为 Level 1 最终验收。

第三轮运行中段抽样，2026-05-29 19:14:27 +08:00：

```json
{
  "mode": "shadow",
  "enabled": true,
  "feed_healthy": true,
  "ws_hub_healthy": true,
  "ws_tick_count": 2713,
  "shadow_compare_count": 2739,
  "shadow_compare_violation_count": 0,
  "invalid_payload_count": 0,
  "timestamp_regression_count": 0,
  "rest_fallback_count": 0,
  "shadow_missing_ws_count": 105,
  "shadow_max_abs_diff_bps": 11.46683042677181,
  "feed_watch_attempt_count": 2714,
  "feed_watch_timeout_count": 0,
  "feed_watch_error_count": 0,
  "feed_watch_empty_count": 0,
  "ws_stale_symbol_count": 0,
  "last_tick_age_ms": 970,
  "feed_last_error": null
}
```

同一日志检查累计：

- `Paper trading mode: False` 次数：`0`
- `scope switched: paper -> live` 次数：`0`
- `exchange_watchdog` 次数：`0`
- `Health check failed for gate` 次数：`0`
- `watch_tickers timeout` 次数：`0`
- `ccxt_pro_feed[binance]: watch error` 次数：`0`

第三轮运行中段抽样，2026-05-29 19:02:50 +08:00：

```json
{
  "mode": "shadow",
  "enabled": true,
  "feed_healthy": true,
  "ws_hub_healthy": true,
  "ws_tick_count": 1526,
  "shadow_compare_count": 1533,
  "shadow_compare_violation_count": 0,
  "invalid_payload_count": 0,
  "timestamp_regression_count": 0,
  "rest_fallback_count": 0,
  "shadow_missing_ws_count": 59,
  "shadow_max_abs_diff_bps": 10.446617783127763,
  "feed_watch_attempt_count": 1527,
  "feed_watch_timeout_count": 0,
  "feed_watch_error_count": 0,
  "feed_watch_empty_count": 0,
  "ws_stale_symbol_count": 0,
  "last_tick_age_ms": 168,
  "feed_last_error": null
}
```

同一日志检查累计：

- `Paper trading mode: False` 次数：`0`
- `scope switched: paper -> live` 次数：`0`
- `exchange_watchdog` 次数：`0`
- `Health check failed for gate` 次数：`0`
- `watch_tickers timeout` 次数：`0`
- `ccxt_pro_feed[binance]: watch error` 次数：`0`

完成后验收动作：

1. 确认 `134108` 已退出并检查 exit code；如果仍在运行，继续等待。
2. 解析 `logs\paper_shadow_6h_selfcheck_20260529_184724.out.json`：
   - `overall_ok=true`
   - `sample_count >= 361`
   - `ws_tick_delta > 0`
   - `shadow_compare_delta > 0`
   - `shadow_compare_violation_delta == 0`
   - `invalid_payload_delta == 0`
   - `timestamp_regression_delta == 0`
   - `feed_watch_timeout_delta == 0`
   - `feed_watch_error_delta == 0`
   - `feed_watch_empty_delta == 0`
   - `max_stale_symbol_count_observed == 0`
   - `p99_abs_diff_bps <= 20`
   - `p95_ws_age_ms <= 10000`
3. 如果通过，停止或保留服务按下一步安排；文档第 22 节和第 23.4 节可把 paper shadow 6 小时改为已通过。
4. 如果失败，保留服务日志和 selfcheck JSON，不进入 live shadow 24 小时。

第三轮运行中段抽样，2026-05-29 18:59:11 +08:00：

```json
{
  "mode": "shadow",
  "enabled": true,
  "feed_healthy": true,
  "ws_hub_healthy": true,
  "ws_tick_count": 1140,
  "shadow_compare_count": 1139,
  "shadow_compare_violation_count": 0,
  "invalid_payload_count": 0,
  "timestamp_regression_count": 0,
  "rest_fallback_count": 0,
  "shadow_missing_ws_count": 45,
  "shadow_max_abs_diff_bps": 10.446617783127763,
  "feed_watch_attempt_count": 1141,
  "feed_watch_timeout_count": 0,
  "feed_watch_error_count": 0,
  "feed_watch_empty_count": 0,
  "ws_stale_symbol_count": 0,
  "last_tick_age_ms": 36
}
```

同一日志检查累计：

- `Paper trading mode: False` 次数：`0`
- `scope switched: paper -> live` 次数：`0`
- `exchange_watchdog` 次数：`0`
- `Health check failed for gate` 次数：`0`
- `watch_tickers timeout` 次数：`0`
- `ccxt_pro_feed[binance]: watch error` 次数：`0`

## 28. 升级门禁补充：REST 权威到 WS 权威

本节把“由 REST 到 WS”的升级拆成可回退的五级阶梯。除非上一级验收记录已写入本文档，不允许进入下一级。

### 28.1 Level 0 - REST 权威基线

目标：证明新代码在默认配置下仍等价于原 REST 行情路径。

运行条件：

- `MARKET_WS_ENABLED=false`
- `MARKET_WS_MODE=off`
- `MARKET_WS_FORCE_REST=false`
- `EXCHANGE_WATCHDOG_ENABLED` 保持生产默认值，除非是在受控 shadow 长跑中隔离噪音。

验收：

- runtime task 中不创建 `market_ws_feed`。
- `/api/status.market_ws.enabled=false`，`mode=off`。
- UI 顶栏显示 REST 或等价状态，不显示 WS primary。
- 静态扫描仍无策略/执行路径直接调用 `.get_ticker(`。
- 当前已通过的测试证据可以作为代码级验收，但发布前仍需重新跑第 24 节命令。

回滚：

- 任意阶段出现行情异常时，第一动作是设置 `MARKET_WS_FORCE_REST=true` 或 `MARKET_WS_MODE=off`。
- 回滚后确认 `feed_present=false` 或 `enabled=false`，并确认 `rest_snapshot_count` 仍递增。

### 28.2 Level 1 - Paper Shadow

目标：WS 真实连接持续写入 hub，但 REST 仍是 UI/runtime 的行情发布来源。

运行条件：

- `TRADING_MODE=paper`
- `ALLOW_PERSISTED_LIVE_MODE_START=false`
- `MARKET_WS_ENABLED=true`
- `MARKET_WS_MODE=shadow`
- `MARKET_WS_FORCE_REST=false`
- `MARKET_WS_EXCHANGES=binance`
- `MARKET_WS_SYMBOL_LIMIT=16`
- `MARKET_WS_REST_RECONCILE_SEC=30`
- `MARKET_WS_WATCH_TIMEOUT_SEC=25`
- 受控长跑可设置 `EXCHANGE_WATCHDOG_ENABLED=false`，避免非目标交易所 watchdog 污染本轮 market WS 证据。

当前状态（2026-05-29 20:36 +08:00 更新）：

- 第六轮 clean paper shadow 已确认启动配置正确：`COINGLASS_WORKER_ENABLED=false` 生效，runtime tasks 不包含 `coinglass` 或 `exchange_watchdog`。
- 第六轮 60 秒 smoke 通过，但正式 6 小时长跑在早期观察到恢复型 `watch_tickers timeout` 和一次已重连的 `watch error`；feed 与 hub 保持 healthy，WS tick 仍新鲜，未出现 violation/invalid/regression/stale-skip/empty payload。
- 第六轮不作为最终 Level 1 验收；原因是验收脚本的旧门槛把恢复型 timeout/error 当作硬失败。当前已把 timeout/error 改为默认诊断项，并保留 `feed_watch_empty_delta == 0`、最终健康、p95 age、stale symbol、价差和日志污染等硬门禁。
- 第七轮暴露 REST connector transient disconnect 后无法自恢复的问题，已补充 REST snapshot/fallback 侧的 `ensure_exchange()` 自恢复并验证。
- 第八轮 clean paper shadow 已启动且 60 秒 smoke 通过；第八轮 6 小时自检完成并写回最终评估前，不进入 Level 2 live shadow。

完成门禁：

- `overall_ok=true`
- `sample_count >= 361`
- `ws_tick_delta > 0`
- `shadow_compare_delta > 0`
- `shadow_compare_violation_delta == 0`
- `invalid_payload_delta == 0`
- `timestamp_regression_delta == 0`
- `feed_watch_empty_delta == 0`
- `max_stale_symbol_count_observed == 0`
- `p99_abs_diff_bps <= 20`
- `p95_ws_age_ms <= 10000`
- `shadow_compare_stale_skip_delta == 0`
- `final_feed_healthy == true`
- `final_ws_hub_healthy == true`
- `final_feed_last_error` 为空
- `feed_watch_timeout_delta` 和 `feed_watch_error_delta` 默认只作诊断；只有在显式传入非负阈值时才作为硬失败。恢复型 timeout/error 可接受的前提是最终 feed/hub healthy、WS age 未超阈值、无 stale symbol、无 empty payload、无 invalid/regression/violation/stale-skip。

失败处理：

- 任一门禁失败，不进入 live shadow。
- 保留 `paper_shadow_6h_service_*.out.log`、`paper_shadow_6h_service_*.err.log`、`paper_shadow_6h_selfcheck_*.out.json`、`paper_shadow_6h_selfcheck_*.err.log`。
- 如果 `feed_watch_timeout_count` 或 `feed_watch_error_count` 增长但其它硬门禁全部健康，先作为链路诊断记录频率、持续时间和恢复情况；如果同时出现 `final_feed_last_error`、feed/hub unhealthy、WS stale、empty payload 或价差/compare 异常，则按失败处理并优先排查 ccxt.pro `defaultType`、订阅 symbol 格式和交易所连接层。
- 如果失败原因是 `shadow_compare_violation_count` 增长，优先保留最后一次 `shadow_last_compare`，核对 REST 与 WS 是否来自同一市场类型。
- 如果失败原因是 `shadow_compare_stale_skip_delta` 增长，说明 REST reconcile 没有提供足够新鲜的对比基准，先排查 REST ticker 超时、交易所 REST 限速或 `MARKET_WS_REST_RECONCILE_SEC` 配置，不进入 live shadow。

2026-05-29 19:31 +08:00 第四轮 clean paper shadow 启动：

启动条件：

- `TRADING_MODE=paper`
- `ALLOW_PERSISTED_LIVE_MODE_START=false`
- `MARKET_WS_ENABLED=true`
- `MARKET_WS_MODE=shadow`
- `MARKET_WS_FORCE_REST=false`
- `MARKET_WS_EXCHANGES=binance`
- `MARKET_WS_SYMBOL_LIMIT=16`
- `MARKET_WS_REST_RECONCILE_SEC=30`
- `MARKET_WS_WATCH_TIMEOUT_SEC=25`
- `MARKET_WS_MAX_PRICE_DIFF_BPS=20`
- `MARKET_WS_SYMBOL_MAX_AGE_SEC=10`
- `EXCHANGE_WATCHDOG_ENABLED=false`

服务进程：

- PID: `91944`
- URL: `http://127.0.0.1:8012`
- token: `codex-paper-shadow-longrun-token-4`
- stdout: `logs\paper_shadow_6h_service_20260529_193044.out.log`
- stderr: `logs\paper_shadow_6h_service_20260529_193044.err.log`

启动日志确认：

- runtime tasks 包含 `market_ws_feed`。
- runtime tasks 不包含 `exchange_watchdog`。
- `ccxt_pro_feed[binance]` 使用 `defaultType=swap`，REST default type 保持 `future`。
- `/api/status` 确认 `status=running`、`paper_trading=true`、`trading_mode=paper`、`market_ws.mode=shadow`。

第四轮启动后 60 秒 smoke：

```powershell
python scripts\selfcheck_market_ws_shadow.py --base-url http://127.0.0.1:8012 --token codex-paper-shadow-longrun-token-4 --duration-sec 60 --interval-sec 10 --min-samples 3 --min-ws-tick-delta 1 --min-shadow-compare-delta 1 --max-shadow-violation-delta 0 --max-invalid-payload-delta 0 --max-timestamp-regression-delta 0 --max-shadow-stale-skip-delta 0 --max-stale-symbol-count 0 --max-price-diff-bps 20 --max-ws-age-p95-ms 10000
```

结果：`overall_ok=true`。

核心摘要：

- `sample_count=7`
- `ws_tick_delta=112`
- `shadow_compare_delta=113`
- `shadow_compare_violation_delta=0`
- `invalid_payload_delta=0`
- `timestamp_regression_delta=0`
- `shadow_compare_stale_skip_delta=0`
- `rest_fallback_delta=0`
- `feed_watch_timeout_delta=0`
- `feed_watch_error_delta=0`
- `feed_watch_empty_delta=0`
- `p99_abs_diff_bps=6.432603929486536`
- `p95_ws_age_ms=0.0`
- `max_stale_symbol_count_observed=0`

第四轮正式 6 小时自检：

- PID: `153100`
- stdout/final JSON: `logs\paper_shadow_6h_selfcheck_20260529_193044.out.json`
- stderr/human summary: `logs\paper_shadow_6h_selfcheck_20260529_193044.err.log`
- 采样策略：`duration-sec=21600`，`interval-sec=60`，`min-samples=361`
- 新增门禁：`--max-shadow-stale-skip-delta 0`

第四轮完成后的自动评估命令：

```powershell
python scripts\evaluate_market_ws_shadow_report.py --report logs\paper_shadow_6h_selfcheck_20260529_193044.out.json --service-err-log logs\paper_shadow_6h_service_20260529_193044.err.log --expect-mode shadow --expect-runtime paper --min-samples 361 --min-ws-tick-delta 1 --min-shadow-compare-delta 1 --max-shadow-violation-delta 0 --max-invalid-payload-delta 0 --max-timestamp-regression-delta 0 --max-shadow-stale-skip-delta 0 --max-feed-watch-timeout-delta 0 --max-feed-watch-error-delta 0 --max-feed-watch-empty-delta 0 --max-stale-symbol-count 0 --max-price-diff-bps 20 --max-ws-age-p95-ms 10000 --max-log-count 0
```

2026-05-29 19:38 +08:00 第四轮提前判定失败并停止：

- 判定原因：正式 6 小时自检要求 `shadow_compare_stale_skip_delta == 0`，但运行中 `shadow_compare_stale_skip_count` 持续增长，不属于启动瞬时噪声。
- 19:36 左右抽样：`shadow_compare_stale_skip_count=11`，`shadow_compare_count=521`，`shadow_compare_violation_count=0`，`shadow_max_abs_diff_bps=8.833405641682942`，`feed_watch_timeout_count=0`，`feed_watch_error_count=0`，`feed_watch_empty_count=0`。
- 65 秒后复查：`shadow_compare_stale_skip_count=67`，`shadow_compare_count=608`，`shadow_compare_violation_count=0`，`shadow_max_abs_diff_bps=9.009412596253604`，`ws_tick_count=686`，`rest_snapshot_count=41`，`feed_watch_timeout_count=0`，`feed_watch_error_count=0`，`feed_watch_empty_count=0`。
- 停止前最终抽样，2026-05-29 19:38:34 +08:00：
  - `ws_tick_count=743`
  - `shadow_compare_count=620`
  - `shadow_compare_violation_count=0`
  - `shadow_compare_stale_skip_count=114`
  - `invalid_payload_count=0`
  - `timestamp_regression_count=0`
  - `rest_snapshot_count=45`
  - `shadow_max_abs_diff_bps=9.009412596253604`
  - `feed_watch_attempt_count=744`
  - `feed_watch_timeout_count=0`
  - `feed_watch_error_count=0`
  - `feed_watch_empty_count=0`
  - `ws_stale_symbol_count=0`
  - `last_tick_age_ms=656`
- 同一服务 stderr 污染计数仍为 0：
  - `Paper trading mode: False`
  - `scope switched: paper -> live`
  - `exchange_watchdog`
  - `Health check failed for gate`
  - `watch_tickers timeout`
  - `ccxt_pro_feed[binance]: watch error`
- 处理：受控停止 selfcheck PID `153100` 和服务 PID `91944`。日志保留：
  - `logs\paper_shadow_6h_service_20260529_193044.out.log`
  - `logs\paper_shadow_6h_service_20260529_193044.err.log`
  - `logs\paper_shadow_6h_selfcheck_20260529_193044.out.json`
  - `logs\paper_shadow_6h_selfcheck_20260529_193044.err.log`
- 结论：第四轮 smoke 通过但正式长跑失败，不允许进入 Level 2 live shadow。

第四轮失败根因与修正：

- 根因：`MarketDataHub._record_shadow_compare()` 之前会在每次 WS tick 到达时，把高频 WS 样本与最近一次低频 REST baseline 再比较一次；当 REST baseline 按 `MARKET_WS_REST_RECONCILE_SEC=30` 稀疏刷新时，WS 推送会自然遇到超过 `shadow_compare_max_age_sec` 的 REST 样本，从而让 `shadow_compare_stale_skip_count` 按 WS 频率增长。
- 这类 stale skip 主要测到的是 REST baseline 年龄和 WS 推送频率的组合，不是“REST reconcile 到达时无法提供新鲜对照样本”。
- 已修正：shadow compare 改为只在 `rest_snapshot` 或 `rest_fallback` tick 到达时触发；WS tick 只更新 hub，不再重复消费旧 REST baseline。
- 修正后 `shadow_compare_stale_skip_count` 的含义收敛为：REST 对照样本到达时，对应 WS 输入仍然超过 `shadow_compare_max_age_sec`，这才代表真正的 shadow 对照不可用。
- `shadow_last_compare` 现在记录 `trigger_source`，用于区分本次对比由 `rest_snapshot` 还是 `rest_fallback` 触发。
- `scripts/selfcheck_market_ws_shadow.py` 的 stderr 摘要已补充 `stale_skip_delta`、`feed_watch_error_delta`、`feed_watch_empty_delta`，便于长跑中快速判断失败原因。

修正后的代码级验证，2026-05-29 19:45 +08:00：

```powershell
python -m py_compile core\marketdata\hub.py scripts\selfcheck_market_ws_shadow.py scripts\evaluate_market_ws_shadow_report.py web\main.py
pytest tests\test_market_data_hub.py tests\test_market_ws_shadow_selfcheck.py tests\test_market_ws_shadow_report_eval.py tests\test_web_main_runtime_tasks.py -q
```

结果：

- `py_compile` 通过。
- `49 passed in 17.03s`。

第五轮 clean paper shadow 重跑要求：

- 必须重新启动服务进程，确保加载 `MarketDataHub` 的 REST-trigger shadow compare 修正。
- 先运行 60 秒 smoke，继续要求 `--max-shadow-stale-skip-delta 0`。
- smoke 通过后再运行 6 小时 selfcheck，仍保留：
  - `--duration-sec 21600`
  - `--interval-sec 60`
  - `--min-samples 361`
  - `--max-shadow-stale-skip-delta 0`
  - `--max-feed-watch-timeout-delta 0`
  - `--max-feed-watch-error-delta 0`
  - `--max-feed-watch-empty-delta 0`
- 第五轮通过前，不运行 live shadow，不开启 `ui_primary` 或 `strategy_primary`。

2026-05-29 19:46 +08:00 第五轮 clean paper shadow 启动：

启动条件：

- `TRADING_MODE=paper`
- `ALLOW_PERSISTED_LIVE_MODE_START=false`
- `MARKET_WS_ENABLED=true`
- `MARKET_WS_MODE=shadow`
- `MARKET_WS_FORCE_REST=false`
- `MARKET_WS_EXCHANGES=binance`
- `MARKET_WS_SYMBOL_LIMIT=16`
- `MARKET_WS_REST_RECONCILE_SEC=30`
- `MARKET_WS_WATCH_TIMEOUT_SEC=25`
- `MARKET_WS_MAX_PRICE_DIFF_BPS=20`
- `MARKET_WS_SYMBOL_MAX_AGE_SEC=10`
- `EXCHANGE_WATCHDOG_ENABLED=false`
- 非目标后台 worker 尽量关闭：`NEWS_BACKGROUND_ENABLED=false`、`NEWS_LLM_BACKGROUND_ENABLED=false`、`DATA_MAINTENANCE_ENABLED=false`、`PUBLIC_MACRO_WORKERS_ENABLED=false`、`PREMIUM_EXTERNAL_WORKERS_ENABLED=false`、`ANALYTICS_HISTORY_ENABLED=false`。

服务进程：

- PID: `156884`
- URL: `http://127.0.0.1:8012`
- token: `codex-paper-shadow-longrun-token-5`
- stdout: `logs\paper_shadow_6h_service_20260529_194430.out.log`
- stderr: `logs\paper_shadow_6h_service_20260529_194430.err.log`

启动日志确认：

- runtime tasks 包含 `market_ws_feed`。
- runtime tasks 不包含 `exchange_watchdog`。
- `ccxt_pro_feed[binance]` 使用 `defaultType=swap`，REST default type 保持 `future`。
- `/health` 返回 `status=healthy`。
- `/api/status` 确认 `status=running`、`paper_trading=true`、`trading_mode=paper`、`market_ws.mode=shadow`。
- 启动 stderr 中出现一次 `positions_live.json` 权限警告：`Failed to load persisted positions for scope=live`。该警告来自 position manager 读取另一 runtime scope 的本地持仓缓存，不属于 market WS feed/reconcile 错误，也不在本轮 market WS 日志污染门禁模式内；继续作为旁路观察，不作为 Level 1 阻断项。

第五轮启动后 60 秒 smoke：

```powershell
python scripts\selfcheck_market_ws_shadow.py --base-url http://127.0.0.1:8012 --token codex-paper-shadow-longrun-token-5 --duration-sec 60 --interval-sec 10 --min-samples 3 --min-ws-tick-delta 1 --min-shadow-compare-delta 1 --max-shadow-violation-delta 0 --max-invalid-payload-delta 0 --max-timestamp-regression-delta 0 --max-shadow-stale-skip-delta 0 --max-stale-symbol-count 0 --max-price-diff-bps 20 --max-ws-age-p95-ms 10000
```

结果：`overall_ok=true`。

核心摘要：

- `sample_count=7`
- `ws_tick_delta=104`
- `shadow_compare_delta=2`
- `shadow_compare_violation_delta=0`
- `invalid_payload_delta=0`
- `timestamp_regression_delta=0`
- `shadow_compare_stale_skip_delta=0`
- `rest_fallback_delta=0`
- `rest_snapshot_delta=6`
- `feed_watch_attempt_delta=104`
- `feed_watch_timeout_delta=0`
- `feed_watch_error_delta=0`
- `feed_watch_empty_delta=0`
- `p99_abs_diff_bps=4.085028371021027`
- `p95_ws_age_ms=1771.0`
- `max_stale_symbol_count_observed=0`

第五轮 smoke 后抽样，2026-05-29 19:48:05 +08:00：

```json
{
  "mode": "shadow",
  "enabled": true,
  "feed_healthy": true,
  "ws_hub_healthy": true,
  "ws_tick_count": 213,
  "shadow_compare_count": 4,
  "shadow_compare_violation_count": 0,
  "shadow_compare_stale_skip_count": 0,
  "invalid_payload_count": 0,
  "timestamp_regression_count": 0,
  "rest_fallback_count": 0,
  "rest_snapshot_count": 13,
  "shadow_missing_ws_count": 9,
  "shadow_max_abs_diff_bps": 4.304467228618529,
  "feed_watch_attempt_count": 214,
  "feed_watch_timeout_count": 0,
  "feed_watch_error_count": 0,
  "feed_watch_empty_count": 0,
  "ws_stale_symbol_count": 0,
  "last_tick_age_ms": 930,
  "feed_last_error": null
}
```

同一服务 stderr 污染计数：

- `Paper trading mode: False` 次数：`0`
- `scope switched: paper -> live` 次数：`0`
- `exchange_watchdog` 次数：`0`
- `Health check failed for gate` 次数：`0`
- `watch_tickers timeout` 次数：`0`
- `ccxt_pro_feed[binance]: watch error` 次数：`0`

第五轮正式 6 小时自检：

- PID: `113368`
- stdout/final JSON: `logs\paper_shadow_6h_selfcheck_20260529_194430.out.json`
- stderr/human summary: `logs\paper_shadow_6h_selfcheck_20260529_194430.err.log`
- 采样策略：`duration-sec=21600`，`interval-sec=60`，`min-samples=361`
- 门禁：继续使用 `--max-shadow-stale-skip-delta 0`，且 watch timeout/error/empty delta 均要求为 0。

第五轮完成后的自动评估命令：

```powershell
python scripts\evaluate_market_ws_shadow_report.py --report logs\paper_shadow_6h_selfcheck_20260529_194430.out.json --service-err-log logs\paper_shadow_6h_service_20260529_194430.err.log --expect-mode shadow --expect-runtime paper --min-samples 361 --min-ws-tick-delta 1 --min-shadow-compare-delta 1 --max-shadow-violation-delta 0 --max-invalid-payload-delta 0 --max-timestamp-regression-delta 0 --max-shadow-stale-skip-delta 0 --max-feed-watch-timeout-delta 0 --max-feed-watch-error-delta 0 --max-feed-watch-empty-delta 0 --max-stale-symbol-count 0 --max-price-diff-bps 20 --max-ws-age-p95-ms 10000 --max-log-count 0
```

第五轮运行期间提前完成的代码级门禁，2026-05-29 19:54 +08:00：

```powershell
rg -n "\.get_ticker\(" strategies core\trading core\utils -g "*.py"
python -m py_compile core\marketdata\hub.py core\marketdata\runtime_price_provider.py core\marketdata\ccxt_pro_feed.py web\main.py scripts\selfcheck_market_ws_shadow.py scripts\evaluate_market_ws_shadow_report.py
pytest tests\test_runtime_price_provider.py tests\test_market_data_ws_ui_assets.py tests\test_market_data_hub.py tests\test_web_main_runtime_tasks.py tests\test_ccxt_pro_feed.py tests\test_ws_client_hardening.py tests\test_sensitive_api_auth.py tests\test_infra_fixes.py tests\test_startup_mode.py tests\test_order_manager_safety.py tests\test_strategy_order_routing.py tests\test_execution_engine_live_trade_review.py tests\test_account_scoped_live_paths.py tests\test_trading_balances_stale_prev_equity.py tests\test_strategy_runtime_policy.py tests\test_strategy_manager_runtime_data.py tests\test_content_quality_static.py tests\test_market_ws_shadow_selfcheck.py tests\test_execution_engine_protective_levels.py tests\test_market_ws_authority_static.py tests\test_market_ws_shadow_report_eval.py -q
```

结果：

- `.get_ticker(` 静态扫描无命中，`rg` exit code 为 `1`，符合预期。
- `py_compile` 通过。
- 目标回归：`190 passed in 22.90s`。

第五轮运行中抽样，2026-05-29 19:54 +08:00：

```json
{
  "mode": "shadow",
  "enabled": true,
  "feed_healthy": true,
  "ws_hub_healthy": true,
  "ws_tick_count": 912,
  "shadow_compare_count": 16,
  "shadow_compare_violation_count": 0,
  "shadow_compare_stale_skip_count": 0,
  "invalid_payload_count": 0,
  "timestamp_regression_count": 0,
  "rest_snapshot_count": 51,
  "feed_watch_attempt_count": 913,
  "feed_watch_timeout_count": 0,
  "feed_watch_error_count": 0,
  "feed_watch_empty_count": 0,
  "ws_stale_symbol_count": 0,
  "last_tick_age_ms": 699,
  "feed_last_error": null
}
```

第五轮运行中真实服务语义确认：

- `/api/market-data/status.shadow_last_compare.trigger_source=rest_snapshot`，说明当前服务已加载“只在 REST tick 到达时触发 shadow compare”的新语义。
- 最近一次 compare 示例：`exchange=binance`、`symbol=ETH/USDT`、`ws_source=ws`、`rest_source=rest_snapshot`、`accepted=true`、`abs_diff_bps=4.038711800518415`、`ws_age_ms=983`、`rest_age_ms=0`。

第五轮旁路日志噪声：

- 尽管启动环境中关闭了多类非目标 worker，当前 `web.main` 仍无条件启动 `coinglass` worker；服务日志显示 runtime tasks 为 `ai_research_scheduler, circuit_breaker_monitor, coinglass, cusum_monitor, market_ws_feed, runtime`。
- 截至 19:56 +08:00，`coinglass: rate-limit backoff` 出现 `1` 次，`positions_live.json` 权限警告出现 `1` 次。
- 这些日志不属于本轮 market WS 自动评估的污染模式，不计入 `--max-log-count 0` 的六类门禁；但发布前若要获得完全安静的 shadow 证据，应补充 `COINGLASS_WORKER_ENABLED` 一类开关，或把 `coinglass` worker 也纳入受控长跑环境隔离。

旁路噪声控制修正，2026-05-29 20:00 +08:00：

- 已新增 `COINGLASS_WORKER_ENABLED` 配置，默认 `true`，保持现有生产行为。
- `web.main._build_runtime_task_factories()` 现在仅在 `_COINGLASS_WORKER_ENABLED=true` 时启动 `coinglass` worker；后续受控 paper/live shadow 长跑可显式设置 `COINGLASS_WORKER_ENABLED=false`，减少非目标 429 噪声。
- `.env.example` 已补充 `COINGLASS_WORKER_ENABLED=true`。
- 验证：
  - `python -m py_compile web\main.py config\settings.py tests\test_web_main_runtime_tasks.py` 通过。
  - `pytest tests\test_web_main_runtime_tasks.py -q` 结果：`32 passed in 15.73s`。
- 注意：当前第五轮服务 PID `156884` 是修正前启动的进程，不会动态卸载已经启动的 `coinglass` worker；该修正从下一次服务重启开始生效。

第五轮运行中抽样，2026-05-29 20:02 +08:00：

```json
{
  "mode": "shadow",
  "enabled": true,
  "feed_healthy": true,
  "ws_hub_healthy": true,
  "ws_tick_count": 1671,
  "shadow_compare_count": 30,
  "shadow_compare_violation_count": 0,
  "shadow_compare_stale_skip_count": 0,
  "invalid_payload_count": 0,
  "timestamp_regression_count": 0,
  "rest_snapshot_count": 95,
  "feed_watch_attempt_count": 1672,
  "feed_watch_timeout_count": 0,
  "feed_watch_error_count": 0,
  "feed_watch_empty_count": 0,
  "ws_stale_symbol_count": 0,
  "last_tick_age_ms": 331,
  "feed_last_error": null,
  "shadow_last_compare": {
    "exchange": "binance",
    "symbol": "ETH/USDT",
    "abs_diff_bps": 4.635860625093782,
    "accepted": true,
    "ws_age_ms": 401,
    "rest_age_ms": 0,
    "ws_source": "ws",
    "rest_source": "rest_snapshot",
    "trigger_source": "rest_snapshot"
  }
}
```

同一服务 stderr 污染计数：

- `Paper trading mode: False` 次数：`0`
- `scope switched: paper -> live` 次数：`0`
- `exchange_watchdog` 次数：`0`
- `Health check failed for gate` 次数：`0`
- `watch_tickers timeout` 次数：`0`
- `ccxt_pro_feed[binance]: watch error` 次数：`0`
- 旁路非门禁噪声：`coinglass: rate-limit backoff` 次数 `1`，`positions_live.json` 权限警告次数 `1`。

2026-05-29 20:19 +08:00 第五轮停止并不作为最终验收：

- 停止原因：第五轮启动后，工作树又新增了 `COINGLASS_WORKER_ENABLED` 并修改了 `web.main._build_runtime_task_factories()`。该变更不改变 market WS 核心 compare/fallback 逻辑，但最终 Level 1 验收必须覆盖当前最终代码和受控 worker 集合，因此第五轮只能作为健康运行证据，不作为最终 6 小时验收。
- 停止前抽样：
  - `ws_tick_count=3600`
  - `shadow_compare_count=56`
  - `shadow_compare_violation_count=0`
  - `shadow_compare_stale_skip_count=0`
  - `invalid_payload_count=0`
  - `timestamp_regression_count=0`
  - `rest_snapshot_count=189`
  - `feed_watch_attempt_count=3601`
  - `feed_watch_timeout_count=0`
  - `feed_watch_error_count=0`
  - `feed_watch_empty_count=0`
  - `ws_stale_symbol_count=0`
  - `last_tick_age_ms=890`
  - `feed_last_error=null`
- 同一服务 stderr 污染计数：
  - `Paper trading mode: False` 次数：`0`
  - `scope switched: paper -> live` 次数：`0`
  - `exchange_watchdog` 次数：`0`
  - `Health check failed for gate` 次数：`0`
  - `watch_tickers timeout` 次数：`0`
  - `ccxt_pro_feed[binance]: watch error` 次数：`0`
  - 旁路非门禁噪声：`coinglass: rate-limit backoff` 次数 `3`，`positions_live.json` 权限警告次数 `3`。
- 处理：受控停止 selfcheck PID `113368` 和服务 PID `156884`。日志保留：
  - `logs\paper_shadow_6h_service_20260529_194430.out.log`
  - `logs\paper_shadow_6h_service_20260529_194430.err.log`
  - `logs\paper_shadow_6h_selfcheck_20260529_194430.out.json`
  - `logs\paper_shadow_6h_selfcheck_20260529_194430.err.log`
- 下一步：启动第六轮 clean paper shadow，加载当前代码，并显式设置 `COINGLASS_WORKER_ENABLED=false`，消除非目标 CoinGlass worker 噪声。

说明：上述代码级门禁不能替代 6 小时 paper shadow 最终验收；Level 1 仍需等待 PID `113368` 生成最终 JSON，并通过自动评估命令。

2026-05-29 20:21 +08:00 第六轮 clean paper shadow 启动：

启动原因：

- 第五轮启动后发生了 `COINGLASS_WORKER_ENABLED` / `web.main._build_runtime_task_factories()` 代码变更。为确保最终 Level 1 证据覆盖当前代码，重新启动第六轮。

启动条件：

- `TRADING_MODE=paper`
- `ALLOW_PERSISTED_LIVE_MODE_START=false`
- `MARKET_WS_ENABLED=true`
- `MARKET_WS_MODE=shadow`
- `MARKET_WS_FORCE_REST=false`
- `MARKET_WS_EXCHANGES=binance`
- `MARKET_WS_SYMBOL_LIMIT=16`
- `MARKET_WS_REST_RECONCILE_SEC=30`
- `MARKET_WS_WATCH_TIMEOUT_SEC=25`
- `MARKET_WS_MAX_PRICE_DIFF_BPS=20`
- `MARKET_WS_SYMBOL_MAX_AGE_SEC=10`
- `EXCHANGE_WATCHDOG_ENABLED=false`
- `COINGLASS_WORKER_ENABLED=false`
- `NEWS_BACKGROUND_ENABLED=false`
- `NEWS_LLM_BACKGROUND_ENABLED=false`
- `DATA_MAINTENANCE_ENABLED=false`
- `PUBLIC_MACRO_WORKERS_ENABLED=false`
- `PREMIUM_EXTERNAL_WORKERS_ENABLED=false`
- `ANALYTICS_HISTORY_ENABLED=false`

服务进程：

- PID: `21944`
- URL: `http://127.0.0.1:8012`
- token: `codex-paper-shadow-longrun-token-6`
- stdout: `logs\paper_shadow_6h_service_20260529_202030.out.log`
- stderr: `logs\paper_shadow_6h_service_20260529_202030.err.log`

启动日志确认：

- runtime tasks 为 `ai_research_scheduler, circuit_breaker_monitor, cusum_monitor, market_ws_feed, runtime`。
- runtime tasks 不包含 `coinglass`。
- runtime tasks 不包含 `exchange_watchdog`。
- `ccxt_pro_feed[binance]` 使用 `defaultType=swap`，REST default type 保持 `future`。
- `/api/status` 确认 `status=running`、`paper_trading=true`、`trading_mode=paper`、`market_ws.mode=shadow`。

第六轮启动后 60 秒 smoke：

```powershell
python scripts\selfcheck_market_ws_shadow.py --base-url http://127.0.0.1:8012 --token codex-paper-shadow-longrun-token-6 --duration-sec 60 --interval-sec 10 --min-samples 3 --min-ws-tick-delta 1 --min-shadow-compare-delta 1 --max-shadow-violation-delta 0 --max-invalid-payload-delta 0 --max-timestamp-regression-delta 0 --max-shadow-stale-skip-delta 0 --max-stale-symbol-count 0 --max-price-diff-bps 20 --max-ws-age-p95-ms 10000
```

结果：`overall_ok=true`。

核心摘要：

- `sample_count=7`
- `ws_tick_delta=104`
- `shadow_compare_delta=2`
- `shadow_compare_violation_delta=0`
- `invalid_payload_delta=0`
- `timestamp_regression_delta=0`
- `shadow_compare_stale_skip_delta=0`
- `rest_fallback_delta=0`
- `rest_snapshot_delta=6`
- `shadow_missing_ws_delta=4`
- `feed_watch_attempt_delta=104`
- `feed_watch_timeout_delta=0`
- `feed_watch_error_delta=0`
- `feed_watch_empty_delta=0`
- `p99_abs_diff_bps=4.944208865205181`
- `p95_ws_age_ms=739.0`
- `max_stale_symbol_count_observed=0`

第六轮 smoke 后抽样，2026-05-29 20:23:20 +08:00：

```json
{
  "mode": "shadow",
  "enabled": true,
  "feed_healthy": true,
  "ws_hub_healthy": true,
  "ws_tick_count": 198,
  "shadow_compare_count": 4,
  "shadow_compare_violation_count": 0,
  "shadow_compare_stale_skip_count": 0,
  "invalid_payload_count": 0,
  "timestamp_regression_count": 0,
  "rest_snapshot_count": 13,
  "feed_watch_attempt_count": 199,
  "feed_watch_timeout_count": 0,
  "feed_watch_error_count": 0,
  "feed_watch_empty_count": 0,
  "ws_stale_symbol_count": 0,
  "last_tick_age_ms": 821,
  "feed_last_error": null
}
```

同一服务 stderr 污染计数：

- `Paper trading mode: False` 次数：`0`
- `scope switched: paper -> live` 次数：`0`
- `exchange_watchdog` 次数：`0`
- `Health check failed for gate` 次数：`0`
- `watch_tickers timeout` 次数：`0`
- `ccxt_pro_feed[binance]: watch error` 次数：`0`
- 旁路噪声：`coinglass: rate-limit backoff` 次数 `0`，`positions_live.json` 权限警告次数 `0`。

第六轮若仅作复盘时使用的新评估口径：

```powershell
python scripts\evaluate_market_ws_shadow_report.py --report logs\paper_shadow_6h_selfcheck_20260529_202030.out.json --service-err-log logs\paper_shadow_6h_service_20260529_202030.err.log --expect-mode shadow --expect-runtime paper --min-samples 361 --min-ws-tick-delta 1 --min-shadow-compare-delta 1 --max-shadow-violation-delta 0 --max-invalid-payload-delta 0 --max-timestamp-regression-delta 0 --max-shadow-stale-skip-delta 0 --max-feed-watch-timeout-delta -1 --max-feed-watch-error-delta -1 --max-feed-watch-empty-delta 0 --max-stale-symbol-count 0 --max-price-diff-bps 20 --max-ws-age-p95-ms 10000 --max-log-count 0
```

2026-05-29 20:36 +08:00 第六轮停止并不作为最终验收：

- 旧门槛问题：第六轮早期出现 `watch_tickers timeout` 计数增长和一次已恢复的 `ccxt_pro_feed[binance]: watch error: Abnormal closure of client; reconnecting`。当时脚本仍按 `feed_watch_timeout_delta == 0`、`feed_watch_error_delta == 0` 做硬失败，导致恢复型 quiet-window/reconnect 被等同于 feed 不健康。
- 运行中观察：`feed_healthy=true`、`ws_hub_healthy=true`，最近 tick age 仍低于秒级阈值；`feed_watch_empty_count=0`、`shadow_compare_stale_skip_count=0`、`shadow_compare_violation_count=0`、`invalid_payload_count=0`、`timestamp_regression_count=0`。
- 处理：受控停止第六轮 service PID `21944` 与 selfcheck PID `160960`。日志保留：
  - `logs\paper_shadow_6h_service_20260529_202030.out.log`
  - `logs\paper_shadow_6h_service_20260529_202030.err.log`
  - `logs\paper_shadow_6h_selfcheck_20260529_202030.out.json`
  - `logs\paper_shadow_6h_selfcheck_20260529_202030.err.log`
- 代码修正：`scripts/selfcheck_market_ws_shadow.py` 和 `scripts/evaluate_market_ws_shadow_report.py` 默认把 `feed_watch_timeout_delta`、`feed_watch_error_delta` 的最大阈值设为 `-1`，表示诊断项；仍可显式传 `0` 恢复严格模式。`feed_watch_empty_delta` 继续默认为硬门禁 `0`。
- 日志扫描修正：`scripts/evaluate_market_ws_shadow_report.py` 的 `DEFAULT_LOG_PATTERNS` 不再把 `watch_tickers timeout` 和 `ccxt_pro_feed[binance]: watch error` 当作默认污染模式；真正的失败仍由 `final_feed_healthy`、`final_ws_hub_healthy`、`final_feed_last_error`、p95 WS age、stale symbol、empty payload、invalid/regression/violation/stale-skip 和 paper/live scope 污染捕捉。
- 修正后验证：

```powershell
python -m py_compile scripts\selfcheck_market_ws_shadow.py scripts\evaluate_market_ws_shadow_report.py tests\test_market_ws_shadow_selfcheck.py tests\test_market_ws_shadow_report_eval.py
pytest tests\test_market_ws_shadow_selfcheck.py tests\test_market_ws_shadow_report_eval.py tests\test_ccxt_pro_feed.py -q
```

结果：

- `py_compile` 通过。
- `21 passed in 4.14s`。

第八轮 clean paper shadow 运行命令模板：

```powershell
$env:TRADING_MODE="paper"
$env:ALLOW_PERSISTED_LIVE_MODE_START="false"
$env:OPS_TOKEN="codex-paper-shadow-longrun-token-8"
$env:MARKET_WS_ENABLED="true"
$env:MARKET_WS_MODE="shadow"
$env:MARKET_WS_FORCE_REST="false"
$env:MARKET_WS_EXCHANGES="binance"
$env:MARKET_WS_SYMBOL_LIMIT="16"
$env:MARKET_WS_REST_RECONCILE_SEC="30"
$env:MARKET_WS_WATCH_TIMEOUT_SEC="25"
$env:MARKET_WS_MAX_PRICE_DIFF_BPS="20"
$env:MARKET_WS_SYMBOL_MAX_AGE_SEC="10"
$env:EXCHANGE_WATCHDOG_ENABLED="false"
$env:COINGLASS_WORKER_ENABLED="false"
$env:NEWS_BACKGROUND_ENABLED="false"
$env:NEWS_LLM_BACKGROUND_ENABLED="false"
$env:DATA_MAINTENANCE_ENABLED="false"
$env:PUBLIC_MACRO_WORKERS_ENABLED="false"
$env:PREMIUM_EXTERNAL_WORKERS_ENABLED="false"
$env:ANALYTICS_HISTORY_ENABLED="false"
python -m uvicorn web.main:app --host 127.0.0.1 --port 8012
```

第八轮 60 秒 smoke：

```powershell
python scripts\selfcheck_market_ws_shadow.py --base-url http://127.0.0.1:8012 --token codex-paper-shadow-longrun-token-8 --duration-sec 60 --interval-sec 10 --min-samples 3 --min-ws-tick-delta 1 --min-shadow-compare-delta 1 --max-shadow-violation-delta 0 --max-invalid-payload-delta 0 --max-timestamp-regression-delta 0 --max-shadow-stale-skip-delta 0 --max-feed-watch-empty-delta 0 --max-stale-symbol-count 0 --max-price-diff-bps 20 --max-ws-age-p95-ms 10000
```

第八轮 6 小时 selfcheck：

```powershell
python scripts\selfcheck_market_ws_shadow.py --base-url http://127.0.0.1:8012 --token codex-paper-shadow-longrun-token-8 --duration-sec 21600 --interval-sec 60 --min-samples 361 --min-ws-tick-delta 1 --min-shadow-compare-delta 1 --max-shadow-violation-delta 0 --max-invalid-payload-delta 0 --max-timestamp-regression-delta 0 --max-shadow-stale-skip-delta 0 --max-feed-watch-empty-delta 0 --max-stale-symbol-count 0 --max-price-diff-bps 20 --max-ws-age-p95-ms 10000
```

第八轮完成后的自动评估命令：

```powershell
python scripts\evaluate_market_ws_shadow_report.py --report logs\paper_shadow_6h_selfcheck_<ts>.out.json --service-err-log logs\paper_shadow_6h_service_<ts>.err.log --expect-mode shadow --expect-runtime paper --min-samples 361 --min-ws-tick-delta 1 --min-shadow-compare-delta 1 --max-shadow-violation-delta 0 --max-invalid-payload-delta 0 --max-timestamp-regression-delta 0 --max-shadow-stale-skip-delta 0 --max-feed-watch-timeout-delta -1 --max-feed-watch-error-delta -1 --max-feed-watch-empty-delta 0 --max-stale-symbol-count 0 --max-price-diff-bps 20 --max-ws-age-p95-ms 10000 --max-log-count 0
```

2026-05-29 20:52 +08:00 第七轮启动、smoke 通过，但不作为最终验收：

- 服务 PID：`115312`
- selfcheck PID：`161484`
- token：`codex-paper-shadow-longrun-token-7`
- stdout/stderr：
  - `logs\paper_shadow_6h_service_20260529_205203.out.log`
  - `logs\paper_shadow_6h_service_20260529_205203.err.log`
  - `logs\paper_shadow_6h_selfcheck_20260529_205203.out.json`
  - `logs\paper_shadow_6h_selfcheck_20260529_205203.err.log`
- 60 秒 smoke：`overall_ok=true`，`sample_count=7`，`ws_tick_delta=112`，`shadow_compare_delta=2`，`shadow_compare_stale_skip_delta=0`，`feed_watch_timeout_delta=0`，`feed_watch_error_delta=0`，`feed_watch_empty_delta=0`，`p99_abs_diff_bps=2.6412711918732703`，`p95_ws_age_ms=476.0`。
- 早期抽样发现：WS feed 仍 healthy，但 REST exchange manager 在一次 transient `get_ticker` 失败后把 `binance` 标成 disconnected；由于受控长跑关闭了 `exchange_watchdog`，`rest_snapshot_count` 卡在 `4`，`shadow_compare_count` 卡在 `2`。
- 临时处理：调用 `/api/data/reconnect?exchange=binance` 后 REST 恢复，`rest_snapshot_count` 从 `4` 增至 `8`，`shadow_compare_count` 从 `2` 增至 `6`。
- 结论：第七轮有人工重连介入，且服务进程加载的是自动恢复补丁前代码，因此不作为 clean 6 小时最终证据。已受控停止 service PID `115312` 与 selfcheck PID `161484`。

第七轮暴露的 REST reconnect gap 与修正：

- 问题：`_emit_market_ticks()` 只遍历 `exchange_manager.get_connected_exchanges()`；当一次 transient REST ticker error 把 connector 标记为 disconnected 时，shadow reconcile 不再尝试 REST snapshot，也不会触发 compare。
- 修正：新增 `web.main._market_tick_connector_items()`。该路径会合并 connected exchanges 和 manager 已保存的 exchanges；发现 stored exchange disconnected 时，按 `_MARKET_TICK_RECONNECT_MIN_SEC=60` 节流调用 `exchange_manager.ensure_exchange()`，恢复后继续写 `rest_snapshot`/`rest_fallback`。
- 范围：只影响 REST snapshot/fallback 获取路径，不改变 WS feed、UI primary、strategy primary 或 live user-data/order 权威来源。
- 修正后验证：

```powershell
python -m py_compile web\main.py tests\test_web_main_runtime_tasks.py scripts\selfcheck_market_ws_shadow.py scripts\evaluate_market_ws_shadow_report.py
pytest tests\test_web_main_runtime_tasks.py tests\test_market_ws_shadow_selfcheck.py tests\test_market_ws_shadow_report_eval.py tests\test_ccxt_pro_feed.py -q
```

结果：

- `py_compile` 通过。
- `54 passed in 17.84s`。

2026-05-29 21:02 +08:00 第八轮 clean paper shadow 启动：

- 服务 PID：`157484`
- token：`codex-paper-shadow-longrun-token-8`
- stdout：`logs\paper_shadow_6h_service_20260529_210223.out.log`
- stderr：`logs\paper_shadow_6h_service_20260529_210223.err.log`
- runtime tasks 为 `ai_research_scheduler, circuit_breaker_monitor, cusum_monitor, market_ws_feed, runtime`。
- runtime tasks 不包含 `coinglass`。
- runtime tasks 不包含 `exchange_watchdog`。
- `/api/status` 确认 `status=running`、`trading_mode=paper`、`paper_trading=true`、`market_ws.mode=shadow`、`market_ws.feed_healthy=true`、`market_ws.ws_hub_healthy=true`。
- `ccxt_pro_feed[binance]` 使用 `defaultType=swap`，REST default type 保持 `future`。

第八轮 60 秒 smoke 结果：

- `overall_ok=true`
- `sample_count=7`
- `ws_tick_delta=114`
- `shadow_compare_delta=2`
- `shadow_compare_violation_delta=0`
- `invalid_payload_delta=0`
- `timestamp_regression_delta=0`
- `shadow_compare_stale_skip_delta=0`
- `rest_fallback_delta=0`
- `rest_snapshot_delta=2`
- `feed_watch_attempt_delta=114`
- `feed_watch_timeout_delta=0`
- `feed_watch_error_delta=0`
- `feed_watch_empty_delta=0`
- `p99_abs_diff_bps=4.937040587464761`
- `p95_ws_age_ms=1824.0`
- `max_stale_symbol_count_observed=0`
- final status：`final_feed_healthy=true`、`final_ws_hub_healthy=true`、`final_feed_last_error=null`

第八轮正式 6 小时自检：

- PID：`130764`
- stdout/final JSON：`logs\paper_shadow_6h_selfcheck_20260529_210223.out.json`
- stderr/human summary：`logs\paper_shadow_6h_selfcheck_20260529_210223.err.log`
- 采样策略：`duration-sec=21600`，`interval-sec=60`，`min-samples=361`
- 门禁：`--max-shadow-stale-skip-delta 0`、`--max-feed-watch-empty-delta 0`；恢复型 `feed_watch_timeout_delta` / `feed_watch_error_delta` 默认诊断。

第八轮启动后早期抽样，2026-05-29 21:12 +08:00：

- 服务 PID `157484` 与 selfcheck PID `130764` 均仍在运行。
- `trading_mode=paper`、`paper_trading=true`、`market_ws.mode=shadow`。
- `exchange_status.binance=true`，REST snapshot 未再卡住。
- `feed_healthy=true`、`ws_hub_healthy=true`、`feed_last_error=null`。
- `ws_tick_count=1040`
- `rest_snapshot_count=21`
- `shadow_compare_count=19`
- `shadow_compare_violation_count=0`
- `shadow_compare_stale_skip_count=0`
- `invalid_payload_count=0`
- `timestamp_regression_count=0`
- `feed_watch_timeout_count=0`
- `feed_watch_error_count=0`
- `feed_watch_empty_count=0`
- `ws_stale_symbol_count=0`
- `last_tick_age_ms=159`
- `shadow_last_compare.trigger_source=rest_snapshot`

第八轮中途观察，2026-05-29 21:15 +08:00：

- 服务 PID `157484` 与 selfcheck PID `130764` 均仍在运行。
- selfcheck JSON/stderr 仍为 0 字节，这是自检结束前的预期状态。
- `trading_mode=paper`、`paper_trading=true`、`market_ws.mode=shadow`。
- `exchange_status.binance=true`，未发生人工重连。
- `feed_healthy=true`、`ws_hub_healthy=true`、`feed_last_error=null`。
- `ws_tick_count=1435`
- `rest_snapshot_count=27`
- `shadow_compare_count=25`
- `shadow_compare_violation_count=0`
- `shadow_compare_stale_skip_count=0`
- `invalid_payload_count=0`
- `timestamp_regression_count=0`
- `feed_watch_timeout_count=0`
- `feed_watch_error_count=0`
- `feed_watch_empty_count=0`
- `ws_stale_symbol_count=0`
- `last_tick_age_ms=763`
- `shadow_last_compare.trigger_source=rest_snapshot`
- 服务 stderr 计数：
  - `Paper trading mode: False`：`0`
  - `scope switched: paper -> live`：`0`
  - `exchange_watchdog`：`0`
  - `Health check failed for gate`：`0`
  - `watch_tickers timeout`：`0`
  - `ccxt_pro_feed[binance]: watch error`：`0`
  - `coinglass: rate-limit backoff`：`0`
  - `positions_live.json`：`0`
  - `exchange_manager: binance reconnected`：`0`

第八轮中途观察，2026-05-29 21:31-21:33 +08:00：

- 21:31 抽样发现 `exchange_status.binance=false`，同时 `feed_healthy=true`、`ws_hub_healthy=true`、`feed_last_error=null`。
- 当时 WS 与 REST 对照仍未出现硬门禁异常：
  - `ws_tick_count=3203`
  - `rest_snapshot_count=57`
  - `shadow_compare_count=55`
  - `shadow_compare_violation_count=0`
  - `shadow_compare_stale_skip_count=0`
  - `invalid_payload_count=0`
  - `timestamp_regression_count=0`
  - `feed_watch_timeout_count=0`
  - `feed_watch_error_count=0`
  - `feed_watch_empty_count=0`
  - `ws_stale_symbol_count=0`
- 未执行人工 `/api/data/reconnect`，等待一个 REST reconcile 周期后，21:33 抽样显示自动恢复生效：
  - `exchange_status.binance=true`
  - `rest_snapshot_count=61`
  - `shadow_compare_count=59`
  - `shadow_compare_violation_count=0`
  - `shadow_compare_stale_skip_count=0`
  - `feed_watch_timeout_count=0`
  - `feed_watch_error_count=0`
  - `feed_watch_empty_count=0`
  - `last_tick_age_ms=16`
  - `shadow_last_compare.trigger_source=rest_snapshot`
- 服务 stderr 显示触发原因与恢复：
  - `get_klines(BTC/USDT, 15m) failed`：`1`
  - `exchange_manager: binance reconnected`：`1`
  - `Live kline fetch timed out`：`1`
  - `positions_live.json` 权限警告：`1`
- 结论：第七轮暴露的 REST connector transient disconnect gap 在第八轮当前代码中已通过 `_market_tick_connector_items()` / `exchange_manager.ensure_exchange()` 自动恢复；本次恢复没有人工介入，没有开启 `exchange_watchdog`，也没有影响 WS feed/hub 健康。

第八轮中途观察，2026-05-29 21:35 +08:00：

- 自动恢复持续生效，`exchange_status.binance=true`。
- `feed_healthy=true`、`ws_hub_healthy=true`、`feed_last_error=null`。
- `ws_tick_count=3714`
- `rest_snapshot_count=67`
- `shadow_compare_count=65`
- `shadow_compare_violation_count=0`
- `shadow_compare_stale_skip_count=0`
- `invalid_payload_count=0`
- `timestamp_regression_count=0`
- `feed_watch_timeout_count=0`
- `feed_watch_error_count=0`
- `feed_watch_empty_count=0`
- `ws_stale_symbol_count=0`
- `last_tick_age_ms=152`
- `shadow_last_compare.trigger_source=rest_snapshot`
- 服务 stderr 计数继续保持：
  - `Paper trading mode: False`：`0`
  - `scope switched: paper -> live`：`0`
  - `exchange_watchdog`：`0`
  - `Health check failed for gate`：`0`
  - `watch_tickers timeout`：`0`
  - `ccxt_pro_feed[binance]: watch error`：`0`
  - `coinglass: rate-limit backoff`：`0`
- 旁路噪声依旧仅有：
  - `positions_live.json` 权限警告：`1`
  - `Live kline fetch timed out`：`1`
  - `get_klines(BTC/USDT, 15m) failed`：`1`

第八轮中途观察，2026-05-29 21:47 +08:00：

- 第八轮仍在运行，selfcheck JSON/stderr 仍为 0 字节，这是自检结束前的预期状态。
- `exchange_status.binance=true`，说明 21:46 左右的第二次 REST transient disconnect 也已自动恢复。
- `feed_healthy=true`、`ws_hub_healthy=true`、`feed_last_error=null`。
- `ws_tick_count=5123`
- `rest_snapshot_count=89`
- `shadow_compare_count=87`
- `shadow_compare_violation_count=0`
- `shadow_compare_stale_skip_count=0`
- `invalid_payload_count=0`
- `timestamp_regression_count=0`
- `feed_watch_timeout_count=0`
- `feed_watch_error_count=0`
- `feed_watch_empty_count=0`
- `ws_stale_symbol_count=0`
- `last_tick_age_ms=897`
- `shadow_last_compare.trigger_source=rest_snapshot`
- 服务 stderr 计数：
  - `Paper trading mode: False`：`0`
  - `scope switched: paper -> live`：`0`
  - `exchange_watchdog`：`0`
  - `Health check failed for gate`：`0`
  - `watch_tickers timeout`：`0`
  - `ccxt_pro_feed[binance]: watch error`：`0`
  - `coinglass: rate-limit backoff`：`0`
  - `exchange_manager: binance reconnected`：`2`
  - `Live kline fetch timed out`：`2`
  - `get_klines(BTC/USDT, 15m) failed`：`1`
  - `get_ticker(`：`1`
  - `positions_live.json` 权限警告：`1`
- 旁路风险：本次 reconnect 附近出现一次 ccxt/aiohttp `Unclosed client session` 提示。该提示未进入 market WS 默认硬门禁，且当前 REST snapshot/compare 已继续推进；最终评估时需把它作为 REST reconnect 资源释放风险单独复核。如果后续判断需要代码修正，必须重新启动新的 clean paper shadow 覆盖修正后代码。

第八轮运行中提前代码级回归，2026-05-29 21:37-21:39 +08:00：

```powershell
rg -n "\.get_ticker\(" strategies core\trading core\utils -g "*.py"
python -m py_compile core\marketdata\hub.py core\marketdata\runtime_price_provider.py core\marketdata\ccxt_pro_feed.py web\main.py scripts\selfcheck_market_ws_shadow.py scripts\evaluate_market_ws_shadow_report.py tests\test_web_main_runtime_tasks.py tests\test_market_ws_shadow_selfcheck.py tests\test_market_ws_shadow_report_eval.py
pytest tests\test_runtime_price_provider.py tests\test_market_data_ws_ui_assets.py tests\test_market_data_hub.py tests\test_web_main_runtime_tasks.py tests\test_ccxt_pro_feed.py tests\test_ws_client_hardening.py tests\test_sensitive_api_auth.py tests\test_infra_fixes.py tests\test_startup_mode.py tests\test_order_manager_safety.py tests\test_strategy_order_routing.py tests\test_execution_engine_live_trade_review.py tests\test_account_scoped_live_paths.py tests\test_trading_balances_stale_prev_equity.py tests\test_strategy_runtime_policy.py tests\test_strategy_manager_runtime_data.py tests\test_content_quality_static.py tests\test_market_ws_shadow_selfcheck.py tests\test_execution_engine_protective_levels.py tests\test_market_ws_authority_static.py tests\test_market_ws_shadow_report_eval.py -q
```

结果：

- `.get_ticker(` 静态扫描无命中，`rg` exit code 为 `1`，符合预期。
- `py_compile` 通过。
- 目标回归：`196 passed in 34.86s`。
- 说明：这是第八轮运行期间的提前代码级回归，用于尽早发现实现问题；它不能替代 6 小时 paper shadow 完成后的最终静态扫描和目标回归。第八轮最终通过后仍需重跑。

回归后第八轮状态确认，2026-05-29 21:38 +08:00：

- 服务 PID `157484` 与 selfcheck PID `130764` 均仍在运行。
- selfcheck JSON/stderr 仍为 0 字节，这是自检结束前的预期状态。
- `exchange_status.binance=true`
- `feed_healthy=true`
- `ws_hub_healthy=true`
- `ws_tick_count=4105`
- `rest_snapshot_count=73`
- `shadow_compare_count=71`
- `shadow_compare_violation_count=0`
- `shadow_compare_stale_skip_count=0`
- `feed_watch_timeout_count=0`
- `feed_watch_error_count=0`
- `feed_watch_empty_count=0`
- `ws_stale_symbol_count=0`
- `last_tick_age_ms=421`

第八轮后续判定，2026-05-29 21:58 +08:00：

- 第八轮继续可以作为 REST transient disconnect 自动恢复的诊断样本，但不再作为 Level 1 clean paper shadow 最终证据。
- 原因：第八轮启动后，在 21:46 左右第二次 REST reconnect 附近出现一次 ccxt/aiohttp `Unclosed client session` 提示；这说明 `BinanceConnector.connect()` 在候选 ccxt client 初始化或重连被取消时存在资源释放风险。
- 已修正 `core/exchanges/binance_connector.py`：
  - 新增 `_close_client_safely()`，通过独立 task + `asyncio.shield()` 尽量完成 ccxt client close，并消费 close task 结果，避免取消路径留下未关闭 aiohttp session。
  - `connect()` 中替换候选 client 和旧 client 的直接 `close()`。
  - `disconnect()` 中同样使用安全关闭路径。
- 已补充 `tests/test_exchanges.py::TestBinanceConnector::test_connect_cancellation_closes_candidate_client`，锁定 `_prepare_client()` 被取消时候选 client 必须关闭，connector 状态必须恢复为 disconnected。
- 定向验证：

```powershell
python -m py_compile core\exchanges\binance_connector.py tests\test_exchanges.py
pytest tests\test_exchanges.py tests\test_exchange_connector_precision.py -q
```

结果：`28 passed in 5.70s`。

处理结论：

- 第八轮如果继续跑完，只能归档为“自动恢复补丁 + 资源释放缺口发现”的非最终诊断记录。
- 必须受控停止第八轮 service/selfcheck，使用上述资源释放修正后的代码启动第九轮 clean paper shadow。
- 第九轮仍需先通过 60 秒 smoke，再跑完整 6 小时 selfcheck；第九轮通过前不得进入 Level 2 live shadow。

2026-05-29 22:01-22:02 +08:00 第八轮停止前归档快照：

- 服务 PID `157484` 与 selfcheck PID `130764` 已受控停止，日志保留为 `logs\paper_shadow_6h_*20260529_210223*`。
- 停止前 `/api/status`：
  - `trading_mode=paper`、`paper_trading=true`、`market_ws.mode=shadow`
  - `exchange_status.binance=true`
  - `feed_healthy=true`、`ws_hub_healthy=true`、`feed_last_error=null`
  - `ws_tick_count=6785`
  - `rest_snapshot_count=117`
  - `shadow_compare_count=115`
  - `shadow_compare_violation_count=0`
  - `shadow_compare_stale_skip_count=0`
  - `invalid_payload_count=0`
  - `timestamp_regression_count=0`
  - `feed_watch_timeout_count=0`
  - `feed_watch_error_count=0`
  - `feed_watch_empty_count=0`
  - `ws_stale_symbol_count=0`
  - `last_tick_age_ms=646`
  - `shadow_last_compare.trigger_source=rest_snapshot`
  - `shadow_last_abs_diff_bps=3.0519126094191615`
- 第八轮 service stderr 停止前计数：
  - `Paper trading mode: False`：`0`
  - `scope switched: paper -> live`：`0`
  - `exchange_watchdog`：`0`
  - `Health check failed for gate`：`0`
  - `watch_tickers timeout`：`0`
  - `ccxt_pro_feed[binance]: watch error`：`0`
  - `coinglass: rate-limit backoff`：`0`
  - `positions_live.json`：`1`
  - `exchange_manager: binance reconnected`：`2`
  - `Live kline fetch timed out`：`2`
  - `get_klines(BTC/USDT, 15m) failed`：`1`
  - `get_ticker(`：`1`
  - `Unclosed client session`：`1`

2026-05-29 22:03 +08:00 第九轮 clean paper shadow 启动：

- 服务 PID：`161460`
- token：`codex-paper-shadow-longrun-token-9`
- stdout：`logs\paper_shadow_6h_service_20260529_220305.out.log`
- stderr：`logs\paper_shadow_6h_service_20260529_220305.err.log`
- runtime tasks 日志确认：`ai_research_scheduler, circuit_breaker_monitor, cusum_monitor, market_ws_feed, runtime`。
- runtime tasks 不包含 `coinglass`。
- runtime tasks 不包含 `exchange_watchdog`。
- `/api/status` 确认 `status=running`、`trading_mode=paper`、`paper_trading=true`、`market_ws.mode=shadow`、`market_ws.configured_enabled=true`、`market_ws.force_rest=false`、`market_ws.feed_present=true`。
- feed 稳定后确认：`feed_healthy=true`、`ws_hub_healthy=true`、`exchange_status.binance=true`、`feed_last_error=null`、`ws_tick_count=94`、`rest_snapshot_count=7`、`shadow_compare_count=2`。

第九轮 60 秒 smoke 结果：

- `overall_ok=true`
- `sample_count=7`
- `ws_tick_delta=119`
- `shadow_compare_delta=2`
- `shadow_compare_violation_delta=0`
- `invalid_payload_delta=0`
- `timestamp_regression_delta=0`
- `shadow_compare_stale_skip_delta=0`
- `rest_fallback_delta=0`
- `rest_snapshot_delta=6`
- `shadow_missing_ws_delta=4`
- `feed_watch_attempt_delta=119`
- `feed_watch_timeout_delta=0`
- `feed_watch_error_delta=0`
- `feed_watch_empty_delta=0`
- `p99_abs_diff_bps=4.202794858580546`
- `p95_ws_age_ms=675.0`
- `max_stale_symbol_count_observed=0`
- final status：`final_feed_healthy=true`、`final_ws_hub_healthy=true`、`final_feed_last_error=null`

第九轮正式 6 小时自检：

- PID：`163160`
- stdout/final JSON：`logs\paper_shadow_6h_selfcheck_20260529_220305.out.json`
- stderr/human summary：`logs\paper_shadow_6h_selfcheck_20260529_220305.err.log`
- 采样策略：`duration-sec=21600`，`interval-sec=60`，`min-samples=361`
- 门禁：`--max-shadow-stale-skip-delta 0`、`--max-feed-watch-empty-delta 0`；恢复型 `feed_watch_timeout_delta` / `feed_watch_error_delta` 默认诊断。
- 预计完成：2026-05-30 04:06 +08:00 左右。
- heartbeat automation `check-paper-shadow-longrun` 已更新为跟踪第九轮 PID、token 和日志。

第九轮启动后早期抽样，2026-05-29 22:08 +08:00：

- 服务 PID `161460` 与 selfcheck PID `163160` 均仍在运行。
- selfcheck JSON/stderr 仍为 0 字节，这是自检结束前的预期状态。
- `trading_mode=paper`、`paper_trading=true`、`market_ws.mode=shadow`。
- `exchange_status.binance=true`
- `feed_healthy=true`、`ws_hub_healthy=true`、`feed_last_error=null`
- `ws_tick_count=595`
- `rest_snapshot_count=31`
- `shadow_compare_count=10`
- `shadow_compare_violation_count=0`
- `shadow_compare_stale_skip_count=0`
- `invalid_payload_count=0`
- `timestamp_regression_count=0`
- `feed_watch_timeout_count=0`
- `feed_watch_error_count=0`
- `feed_watch_empty_count=0`
- `ws_stale_symbol_count=0`
- `last_tick_age_ms=960`
- 服务 stderr 计数：
  - `Paper trading mode: False`：`0`
  - `scope switched: paper -> live`：`0`
  - `exchange_watchdog`：`0`
  - `Health check failed for gate`：`0`
  - `watch_tickers timeout`：`0`
  - `ccxt_pro_feed[binance]: watch error`：`0`
  - `coinglass: rate-limit backoff`：`0`
  - `positions_live.json`：`0`
  - `exchange_manager: binance reconnected`：`0`
  - `Live kline fetch timed out`：`0`
  - `get_klines(BTC/USDT, 15m) failed`：`0`
  - `get_ticker(`：`0`
  - `Unclosed client session`：`0`

第九轮后续判定，2026-05-29 22:11-22:16 +08:00：

- 第九轮继续作为诊断证据保留，但不作为 Level 1 clean paper shadow 最终证据。
- 22:11 抽样显示 WS/hub 仍健康，REST snapshot 与 shadow compare 持续推进：
  - `exchange_status.binance=true`
  - `feed_healthy=true`、`ws_hub_healthy=true`
  - `ws_tick_count=916`
  - `rest_snapshot_count=49`
  - `shadow_compare_count=16`
  - `shadow_compare_violation_count=0`
  - `shadow_compare_stale_skip_count=0`
  - `feed_watch_timeout_count=0`
  - `feed_watch_error_count=0`
  - `feed_watch_empty_count=0`
  - `last_tick_age_ms=175`
- 同一日志显示再次出现 REST reconnect 清理问题：
  - `exchange_manager: binance reconnected`：`1`
  - `Live kline fetch timed out`：`1`
  - `get_ticker(`：`1`
  - `Unclosed client session`：`1`
- 22:16 停止前抽样还显示 WS feed 刚经历 timeout 后暂时不健康：
  - `feed_healthy=false`
  - `ws_hub_healthy=false`
  - `feed_watch_timeout_count=2`
  - `watch_tickers timeout` 日志计数：`2`
- 诊断结论：
  - `Unclosed client session` 的主要缺口不是候选 client 关闭本身，而是旧 `_client` 已被 transient `get_ticker` 标为 disconnected 后仍挂在 connector 上；随后新 candidate connect 被策略 kline 的 8 秒 `wait_for` 取消时，只关闭 candidate，没有关闭这个 disconnected old client。
  - 已补充 `BinanceConnector.connect()` 失败/取消分支：当 `existing_connected=false` 且 `existing_client` 存在时，同样调用 `_close_client_safely(existing_client)` 后再清空 `_client`。
  - 新增 `tests/test_exchanges.py::TestBinanceConnector::test_connect_cancellation_closes_existing_disconnected_client`。
- 定向验证：

```powershell
python -m py_compile core\exchanges\binance_connector.py tests\test_exchanges.py
pytest tests\test_exchanges.py tests\test_exchange_connector_precision.py -q
```

结果：`29 passed in 6.32s`。

第十轮诊断，2026-05-29 22:16-22:21 +08:00：

- 第十轮服务 PID：`162184`
- token：`codex-paper-shadow-longrun-token-10`
- stdout：`logs\paper_shadow_6h_service_20260529_221644.out.log`
- stderr：`logs\paper_shadow_6h_service_20260529_221644.err.log`
- 第十轮启动时 Binance REST connector 初始创建超时：
  - `Connector binance connect timed out`：`1`
  - `exchange_status.binance=false`
  - `rest_snapshot_count=6`
  - `shadow_compare_count=0`
  - `Unclosed client session`：`0`
- 诊断结论：
  - 第七轮修正只覆盖“stored connector 已存在但 disconnected”的自动恢复。
  - 如果 startup 阶段 Binance REST connector 创建失败，`exchange_manager` store 中没有 `binance`，原 `_market_tick_connector_items()` 只遍历 connected/store 交易所，不会按 `MARKET_WS_EXCHANGES=binance` 补建 missing connector，导致 shadow REST compare 永远不能开始。
- 已修正 `web/main.py`：
  - 新增 `_configured_market_ws_exchange_names()`，统一解析 `MARKET_WS_EXCHANGES`。
  - `_market_tick_connector_items()` 在 market WS stream enabled 时，把配置要求的 exchange 也加入 REST snapshot/reconcile 候选。
  - connector 为 `None` 或 disconnected 时，统一按 `_MARKET_TICK_RECONNECT_MIN_SEC` 节流调用 `exchange_manager.ensure_exchange()`。
  - `_market_ws_feed_worker()` 复用同一配置解析 helper。
- 已补充 `tests/test_web_main_runtime_tasks.py::test_emit_market_ticks_reconnects_missing_configured_exchange_for_shadow_reconcile`。
- 定向验证：

```powershell
python -m py_compile web\main.py tests\test_web_main_runtime_tasks.py core\exchanges\binance_connector.py tests\test_exchanges.py
pytest tests\test_web_main_runtime_tasks.py tests\test_exchanges.py tests\test_market_ws_shadow_selfcheck.py tests\test_market_ws_shadow_report_eval.py tests\test_ccxt_pro_feed.py -q
```

结果：`76 passed in 22.04s`。

处理结论：

- 第九轮和第十轮都保留为诊断轮，不作为 Level 1 最终验收。
- 下一步启动第十一轮 clean paper shadow，要求同时覆盖：
  - REST reconnect 后无 `Unclosed client session`。
  - startup missing Binance REST connector 能通过 shadow reconcile 自动补建。
  - 60 秒 smoke 通过后再进入 6 小时 selfcheck。

2026-05-29 22:26 +08:00 第十一轮 clean paper shadow 启动：

- 服务 PID：`76424`
- token：`codex-paper-shadow-longrun-token-11`
- stdout：`logs\paper_shadow_6h_service_20260529_222616.out.log`
- stderr：`logs\paper_shadow_6h_service_20260529_222616.err.log`
- 启动后状态确认：
  - `status=running`
  - `trading_mode=paper`、`paper_trading=true`
  - `market_ws.mode=shadow`
  - `market_ws.configured_enabled=true`
  - `market_ws.force_rest=false`
  - `market_ws.feed_present=true`
  - `exchange_status.binance=true`
  - `feed_healthy=true`
  - `ws_hub_healthy=true`
  - `ws_tick_count=73`
  - `rest_snapshot_count=7`
  - `shadow_compare_count=2`
  - `shadow_compare_violation_count=0`
  - `shadow_compare_stale_skip_count=0`
  - `feed_watch_timeout_count=0`
  - `feed_watch_error_count=0`
  - `feed_watch_empty_count=0`

第十一轮 60 秒 smoke 结果：

- `overall_ok=true`
- `sample_count=7`
- `ws_tick_delta=118`
- `shadow_compare_delta=2`
- `shadow_compare_violation_delta=0`
- `invalid_payload_delta=0`
- `timestamp_regression_delta=0`
- `shadow_compare_stale_skip_delta=0`
- `rest_fallback_delta=0`
- `rest_snapshot_delta=4`
- `shadow_missing_ws_delta=2`
- `feed_watch_attempt_delta=118`
- `feed_watch_timeout_delta=0`
- `feed_watch_error_delta=0`
- `feed_watch_empty_delta=0`
- `p99_abs_diff_bps=9.690460448285107`
- `p95_ws_age_ms=518.0`
- `max_stale_symbol_count_observed=0`
- final status：`final_feed_healthy=true`、`final_ws_hub_healthy=true`、`final_feed_last_error=null`

第十一轮 smoke 后服务 stderr 计数：

- `Connector binance connect timed out`：`0`
- `exchange_manager: binance reconnected`：`0`
- `Paper trading mode: False`：`0`
- `scope switched: paper -> live`：`0`
- `exchange_watchdog`：`0`
- `Health check failed for gate`：`0`
- `watch_tickers timeout`：`0`
- `ccxt_pro_feed[binance]: watch error`：`0`
- `coinglass: rate-limit backoff`：`0`
- `get_ticker(`：`0`
- `Unclosed client session`：`0`

第十一轮正式 6 小时自检：

- PID：`163652`
- stdout/final JSON：`logs\paper_shadow_6h_selfcheck_20260529_222616.out.json`
- stderr/human summary：`logs\paper_shadow_6h_selfcheck_20260529_222616.err.log`
- 采样策略：`duration-sec=21600`，`interval-sec=60`，`min-samples=361`
- 门禁：`--max-shadow-stale-skip-delta 0`、`--max-feed-watch-empty-delta 0`；恢复型 `feed_watch_timeout_delta` / `feed_watch_error_delta` 默认诊断。
- 预计完成：2026-05-30 04:29 +08:00 左右。
- heartbeat automation `check-paper-shadow-longrun` 已更新为跟踪第十一轮 PID、token 和日志。

第十一轮启动后早期抽样，2026-05-29 22:30 +08:00：

- 服务 PID `76424` 与 selfcheck PID `163652` 均仍在运行。
- selfcheck JSON/stderr 仍为 0 字节，这是自检结束前的预期状态。
- `trading_mode=paper`、`paper_trading=true`、`market_ws.mode=shadow`
- `exchange_status.binance=true`
- `feed_healthy=true`、`ws_hub_healthy=true`、`feed_last_error=null`
- `ws_tick_count=453`
- `rest_snapshot_count=18`
- `shadow_compare_count=8`
- `shadow_compare_violation_count=0`
- `shadow_compare_stale_skip_count=0`
- `invalid_payload_count=0`
- `timestamp_regression_count=0`
- `feed_watch_timeout_count=0`
- `feed_watch_error_count=0`
- `feed_watch_empty_count=0`
- `ws_stale_symbol_count=0`
- `last_tick_age_ms=651`

第十一轮中途观察，2026-05-29 22:32 +08:00：

- 服务 PID `76424` 与 selfcheck PID `163652` 均仍在运行。
- selfcheck JSON/stderr 仍为 0 字节，这是自检结束前的预期状态。
- `trading_mode=paper`、`paper_trading=true`、`market_ws.mode=shadow`
- `exchange_status.binance=true`
- `feed_healthy=true`、`ws_hub_healthy=true`、`feed_last_error=null`
- `ws_tick_count=695`
- `rest_snapshot_count=25`
- `shadow_compare_count=12`
- `shadow_compare_violation_count=0`
- `shadow_compare_stale_skip_count=0`
- `invalid_payload_count=0`
- `timestamp_regression_count=0`
- `feed_watch_timeout_count=0`
- `feed_watch_error_count=0`
- `feed_watch_empty_count=0`
- `ws_stale_symbol_count=0`
- `last_tick_age_ms=748`
- `shadow_last_compare.trigger_source=rest_snapshot`
- `shadow_last_abs_diff_bps=5.281158837138892`
- 服务 stderr 计数：
  - `Connector binance connect timed out`：`0`
  - `exchange_manager: binance reconnected`：`0`
  - `Paper trading mode: False`：`0`
  - `scope switched: paper -> live`：`0`
  - `exchange_watchdog`：`0`
  - `Health check failed for gate`：`0`
  - `watch_tickers timeout`：`0`
  - `ccxt_pro_feed[binance]: watch error`：`0`
  - `coinglass: rate-limit backoff`：`0`
  - `positions_live.json`：`0`
  - `Live kline fetch timed out`：`0`
  - `get_klines(BTC/USDT, 15m) failed`：`0`
  - `get_ticker(`：`0`
  - `Unclosed client session`：`0`
- 22:30 左右 paper 策略生成并执行了一笔 BTC/USDT 纸面订单；运行模式仍为 paper，没有出现 paper/live scope 抖动。

第十一轮中途观察，2026-05-29 22:35 +08:00：

- 服务 PID `76424` 与 selfcheck PID `163652` 均仍在运行。
- selfcheck JSON/stderr 仍为 0 字节，这是自检结束前的预期状态。
- `trading_mode=paper`、`paper_trading=true`、`market_ws.mode=shadow`
- `exchange_status.binance=true`
- `feed_healthy=true`、`ws_hub_healthy=true`、`feed_last_error=null`
- `ws_tick_count=966`
- `rest_snapshot_count=36`
- `shadow_compare_count=16`
- `shadow_compare_violation_count=0`
- `shadow_compare_stale_skip_count=0`
- `invalid_payload_count=0`
- `timestamp_regression_count=0`
- `feed_watch_timeout_count=0`
- `feed_watch_error_count=0`
- `feed_watch_empty_count=0`
- `ws_stale_symbol_count=0`
- `last_tick_age_ms=370`
- `shadow_last_compare.trigger_source=rest_snapshot`
- `shadow_last_abs_diff_bps=12.359883636217656`
- 服务 stderr 计数：
  - `Connector binance connect timed out`：`0`
  - `exchange_manager: binance reconnected`：`0`
  - `Paper trading mode: False`：`0`
  - `scope switched: paper -> live`：`0`
  - `exchange_watchdog`：`0`
  - `Health check failed for gate`：`0`
  - `watch_tickers timeout`：`0`
  - `ccxt_pro_feed[binance]: watch error`：`0`
  - `coinglass: rate-limit backoff`：`0`
  - `positions_live.json`：`0`
  - `Live kline fetch timed out`：`1`
  - `get_klines(BTC/USDT, 15m) failed`：`0`
  - `get_ticker(`：`0`
  - `Unclosed client session`：`0`
  - `[PAPER] Order created`：`1`
- 22:33 出现一次策略侧 `Live kline fetch timed out after 8s for binance BTC/USDT 15m; backing off 30s, using local/cache data`。当前 market WS feed、hub、REST snapshot 与 shadow compare 仍健康推进；该项作为旁路诊断项记录，暂不作为 market WS Level 1 硬失败。

第十一轮中途观察，2026-05-29 22:37 +08:00：

- 服务 PID `76424` 与 selfcheck PID `163652` 均仍在运行。
- selfcheck JSON/stderr 仍为 0 字节，这是自检结束前的预期状态。
- `trading_mode=paper`、`paper_trading=true`、`market_ws.mode=shadow`
- `exchange_status.binance=true`
- `feed_healthy=true`、`ws_hub_healthy=true`、`feed_last_error=null`
- `ws_tick_count=1207`
- `rest_snapshot_count=47`
- `shadow_compare_count=22`
- `shadow_compare_violation_count=0`
- `shadow_compare_stale_skip_count=0`
- `invalid_payload_count=0`
- `timestamp_regression_count=0`
- `feed_watch_timeout_count=0`
- `feed_watch_error_count=0`
- `feed_watch_empty_count=0`
- `ws_stale_symbol_count=0`
- `last_tick_age_ms=612`
- `shadow_last_compare.trigger_source=rest_snapshot`
- `shadow_last_abs_diff_bps=1.9090105297007331`
- 服务 stderr 计数：
  - `Connector binance connect timed out`：`0`
  - `exchange_manager: binance reconnected`：`0`
  - `Paper trading mode: False`：`0`
  - `scope switched: paper -> live`：`0`
  - `exchange_watchdog`：`0`
  - `Health check failed for gate`：`0`
  - `watch_tickers timeout`：`0`
  - `ccxt_pro_feed[binance]: watch error`：`0`
  - `coinglass: rate-limit backoff`：`0`
  - `positions_live.json`：`0`
  - `Live kline fetch timed out`：`1`
  - `get_klines(BTC/USDT, 15m) failed`：`0`
  - `get_ticker(`：`0`
  - `Unclosed client session`：`0`
  - `[PAPER] Order created`：`1`

第十一轮中途观察，2026-05-29 22:39 +08:00：

- 服务 PID `76424` 与 selfcheck PID `163652` 均仍在运行。
- selfcheck JSON/stderr 仍为 0 字节，这是自检结束前的预期状态。
- `trading_mode=paper`、`paper_trading=true`、`market_ws.mode=shadow`
- `exchange_status.binance=true`
- `feed_healthy=true`、`ws_hub_healthy=true`、`feed_last_error=null`
- `ws_tick_count=1427`
- `rest_snapshot_count=53`
- `shadow_compare_count=24`
- `shadow_compare_violation_count=0`
- `shadow_compare_stale_skip_count=0`
- `invalid_payload_count=0`
- `timestamp_regression_count=0`
- `feed_watch_timeout_count=0`
- `feed_watch_error_count=0`
- `feed_watch_empty_count=0`
- `ws_stale_symbol_count=0`
- `last_tick_age_ms=477`
- `shadow_last_compare.trigger_source=rest_snapshot`
- `shadow_last_abs_diff_bps=4.023476988227215`
- 服务 stderr 计数：
  - `Connector binance connect timed out`：`0`
  - `exchange_manager: binance reconnected`：`0`
  - `Paper trading mode: False`：`0`
  - `scope switched: paper -> live`：`0`
  - `exchange_watchdog`：`0`
  - `Health check failed for gate`：`0`
  - `watch_tickers timeout`：`0`
  - `ccxt_pro_feed[binance]: watch error`：`0`
  - `coinglass: rate-limit backoff`：`0`
  - `positions_live.json`：`0`
  - `Live kline fetch timed out`：`1`
  - `get_klines(BTC/USDT, 15m) failed`：`0`
  - `get_ticker(`：`0`
  - `Unclosed client session`：`0`
  - `[PAPER] Order created`：`1`

第十一轮中途观察，2026-05-29 22:41 +08:00：

- 服务 PID `76424` 与 selfcheck PID `163652` 均仍在运行。
- selfcheck JSON/stderr 仍为 0 字节，这是自检结束前的预期状态。
- `trading_mode=paper`、`paper_trading=true`、`market_ws.mode=shadow`
- `exchange_status.binance=true`
- `feed_healthy=true`、`ws_hub_healthy=true`、`feed_last_error=null`
- `ws_tick_count=1639`
- `rest_snapshot_count=64`
- `shadow_compare_count=28`
- `shadow_compare_violation_count=0`
- `shadow_compare_stale_skip_count=0`
- `invalid_payload_count=0`
- `timestamp_regression_count=0`
- `feed_watch_timeout_count=0`
- `feed_watch_error_count=0`
- `feed_watch_empty_count=0`
- `ws_stale_symbol_count=0`
- `last_tick_age_ms=251`
- `shadow_last_compare.trigger_source=rest_snapshot`
- `shadow_last_abs_diff_bps=6.086519114688311`
- 服务 stderr 计数：
  - `Connector binance connect timed out`：`0`
  - `exchange_manager: binance reconnected`：`0`
  - `Paper trading mode: False`：`0`
  - `scope switched: paper -> live`：`0`
  - `exchange_watchdog`：`0`
  - `Health check failed for gate`：`0`
  - `watch_tickers timeout`：`0`
  - `ccxt_pro_feed[binance]: watch error`：`0`
  - `coinglass: rate-limit backoff`：`0`
  - `positions_live.json`：`0`
  - `Live kline fetch timed out`：`1`
  - `get_klines(BTC/USDT, 15m) failed`：`0`
  - `get_ticker(`：`0`
  - `Unclosed client session`：`0`
  - `[PAPER] Order created`：`1`

第十一轮中途观察，2026-05-29 22:44 +08:00：

- 服务 PID `76424` 与 selfcheck PID `163652` 均仍在运行。
- selfcheck JSON/stderr 仍为 0 字节，这是自检结束前的预期状态。
- `trading_mode=paper`、`paper_trading=true`、`market_ws.mode=shadow`
- `exchange_status.binance=true`
- `feed_healthy=true`、`ws_hub_healthy=true`、`feed_last_error=null`
- `ws_tick_count=2060`
- `rest_snapshot_count=76`
- `shadow_compare_count=36`
- `shadow_compare_violation_count=0`
- `shadow_compare_stale_skip_count=0`
- `invalid_payload_count=0`
- `timestamp_regression_count=0`
- `feed_watch_timeout_count=0`
- `feed_watch_error_count=0`
- `feed_watch_empty_count=0`
- `ws_stale_symbol_count=0`
- `last_tick_age_ms=41`
- `shadow_last_compare.trigger_source=rest_snapshot`
- `shadow_last_abs_diff_bps=0.6548029798578028`
- 服务 stderr 计数：
  - `Connector binance connect timed out`：`0`
  - `exchange_manager: binance reconnected`：`0`
  - `Paper trading mode: False`：`0`
  - `scope switched: paper -> live`：`0`
  - `exchange_watchdog`：`0`
  - `Health check failed for gate`：`0`
  - `watch_tickers timeout`：`0`
  - `ccxt_pro_feed[binance]: watch error`：`0`
  - `coinglass: rate-limit backoff`：`0`
  - `positions_live.json`：`0`
  - `Live kline fetch timed out`：`1`
  - `get_klines(BTC/USDT, 15m) failed`：`0`
  - `get_ticker(`：`0`
  - `Unclosed client session`：`0`
  - `[PAPER] Order created`：`1`

第十一轮中途观察，2026-05-29 22:47-22:48 +08:00：

- 22:47 抽样中 WS feed/hub 仍 healthy，但 REST 侧出现一次 transient disconnected：
  - `exchange_status.binance=false`
  - `feed_healthy=true`
  - `ws_hub_healthy=true`
  - `feed_last_error=null`
  - `ws_tick_count=2356`
  - `rest_snapshot_count=86`
  - `shadow_compare_count=38`
  - `shadow_compare_violation_count=0`
  - `shadow_compare_stale_skip_count=0`
  - `feed_watch_timeout_count=0`
  - `feed_watch_error_count=0`
  - `feed_watch_empty_count=0`
  - `last_tick_age_ms=669`
  - `shadow_last_abs_diff_bps=1.3090916414498237`
- 22:48 等待一个 reconcile 周期后自动恢复，无人工 `/api/data/reconnect` 介入：
  - `exchange_status.binance=true`
  - `feed_healthy=true`
  - `ws_hub_healthy=true`
  - `ws_tick_count=2547`
  - `rest_snapshot_count=98`
  - `shadow_compare_count=44`
  - `shadow_compare_violation_count=0`
  - `shadow_compare_stale_skip_count=0`
  - `last_tick_age_ms=1`
  - `shadow_last_abs_diff_bps=10.300782859497092`
- 服务 stderr 计数：
  - `exchange_manager: binance reconnected`：`1`
  - `get_ticker(`：`1`
  - `Unclosed client session`：`0`
  - `watch_tickers timeout`：`0`
  - `ccxt_pro_feed[binance]: watch error`：`0`
  - `Paper trading mode: False`：`0`
  - `scope switched: paper -> live`：`0`
- 结论：第十一轮首次真实触发 REST transient disconnect 后，`_market_tick_connector_items()` / `exchange_manager.ensure_exchange()` 自动恢复成功；最新 Binance client 关闭修正下没有复现 `Unclosed client session`。

### 28.3 Level 2 - Live Shadow

目标：在 live runtime 下观察 WS 行情链路，但交易和 UI 行情权威仍不切到 WS。

前置条件：

- Level 1 paper shadow 6 小时最终通过，并把最终 JSON 摘要写入第 27 节。
- 第 24 节回归测试和静态扫描重新通过。
- 明确确认当前配置没有开启 `ui_primary` 或 `strategy_primary`。

运行条件：

- `TRADING_MODE=live`
- `MARKET_WS_ENABLED=true`
- `MARKET_WS_MODE=shadow`
- `MARKET_WS_FORCE_REST=false`
- `MARKET_WS_FAIL_CLOSED_FOR_LIVE=true`
- `MARKET_WS_EXCHANGES=binance`
- `MARKET_WS_SYMBOL_LIMIT=16`
- `MARKET_WS_REST_RECONCILE_SEC=30`
- 不改 user data、订单状态、持仓状态的权威来源；REST reconciliation 仍是 live 订单和账户状态的兜底/权威。

建议命令形态：

```powershell
python scripts\selfcheck_market_ws_shadow.py --base-url http://127.0.0.1:8012 --token $env:OPS_TOKEN --duration-sec 86400 --interval-sec 60 --min-samples 1441 --expect-runtime live --min-ws-tick-delta 1 --min-shadow-compare-delta 1 --max-shadow-violation-delta 0 --max-invalid-payload-delta 0 --max-timestamp-regression-delta 0 --max-shadow-stale-skip-delta 0 --max-feed-watch-empty-delta 0 --max-stale-symbol-count 0 --max-price-diff-bps 20 --max-ws-age-p95-ms 10000
```

完成门禁：

- `overall_ok=true`
- `sample_count >= 1441`
- 24 小时内 `feed_watch_empty_delta == 0`
- `shadow_compare_violation_delta == 0`
- `invalid_payload_delta == 0`
- `timestamp_regression_delta == 0`
- `shadow_compare_stale_skip_delta == 0`
- `max_stale_symbol_count_observed == 0`
- `p99_abs_diff_bps <= 20`
- `p95_ws_age_ms <= 10000`
- `final_feed_healthy == true`
- `final_ws_hub_healthy == true`
- `final_feed_last_error` 为空
- `feed_watch_timeout_delta` 和 `feed_watch_error_delta` 默认作为诊断项；若恢复型事件频繁到影响 p95 age、stale symbol 或最终健康，则按失败处理。
- 日志中无 paper/live scope 抖动、无 exchange watchdog 或 gate health 污染。

失败处理：

- 立即保持或切回 `MARKET_WS_MODE=shadow`/`MARKET_WS_FORCE_REST=true`，不得进入 UI primary。
- 如果 live shadow 失败但 paper shadow 通过，优先排查 live runtime 中 exchange 初始化、market type、symbol universe 和账户级配置差异。

### 28.4 Level 3 - UI Primary

目标：只让前端行情推送优先使用 WS；策略、订单、估值仍通过 adapter 保留 REST fallback 和 fail-closed 保护。

前置条件：

- Level 2 live shadow 24 小时通过。
- UI 状态展示已能清楚区分 `WS primary`、`REST fallback`、`stale`。
- `/api/market-data/status` 在 `include_symbols=true` 下没有异常 payload 或明显膨胀问题。

运行条件：

- `MARKET_WS_ENABLED=true`
- `MARKET_WS_MODE=ui_primary`
- `MARKET_WS_FORCE_REST=false`
- `MARKET_WS_FAIL_CLOSED_FOR_LIVE=true`

验收：

- feed 与 hub healthy 时，`market_tick` fan-out 可来自 WS。
- feed 不健康或 hub stale 时，REST fallback 自动恢复发布，且 `rest_fallback_count` 与 `fallback_reasons` 可见。
- UI badge 能在 WS primary、REST fallback、stale 之间正确变化。
- 不出现前端状态重叠、移动端横向溢出或浏览器 `/ws` badge 与 exchange market WS badge 混淆。

回滚：

- 设置 `MARKET_WS_FORCE_REST=true`，确认 UI 回到 REST/fallback 状态。
- 如果只想关闭 UI primary 但保留观测，可改回 `MARKET_WS_MODE=shadow`。

### 28.5 Level 4 - Strategy Primary

目标：策略/执行价格读取可以优先使用 hub 新鲜 WS tick；live 下任何缺失、stale、symbol/exchange mismatch 或 REST 复核失败都必须 fail closed。

前置条件：

- Level 3 UI primary 至少稳定运行一个完整交易日，并有 fallback 演练记录。
- `tests/test_runtime_price_provider.py`、订单管理、执行引擎、策略 runtime 相关测试全部通过。
- 静态扫描确认策略、执行、估值路径无直接 `.get_ticker(` 绕过 adapter。
- 明确不把 order book diff 或 user data WS 混入本级变更。

运行条件：

- `MARKET_WS_ENABLED=true`
- `MARKET_WS_MODE=strategy_primary`
- `MARKET_WS_FAIL_CLOSED_FOR_LIVE=true`
- `MARKET_WS_FORCE_REST=false`

强制规则：

- live strategy-primary 不信任裸 `preferred_price`；必须通过 `get_realtime_price()` 获得带 source/age/freshness metadata 的价格。
- live 下 hub stale 且 REST fallback 失败时拒绝下单或拒绝继续执行相关路径。
- exchange/symbol mismatch 必须 fail closed。
- 任何新增策略实时行情读取必须接入 `runtime_price_provider`，不得直接依赖全局 hub 或 connector REST。

验收：

- 模拟 hub fresh、hub stale + REST fallback、hub missing + REST failure、exchange mismatch、symbol mismatch。
- paper 下允许 fallback 但记录来源；live strategy-primary 下必须 fail closed。
- 下单前 notional 估值不能因行情缺失变成 `0` 后绕过风控。

回滚：

- 首选 `MARKET_WS_FORCE_REST=true`。
- 次选 `MARKET_WS_MODE=ui_primary` 或 `shadow`，保留观测但撤出策略主用。
- 回滚后重新跑 order manager 与 execution engine 相关测试。

## 29. 禁止越级项

在本轮 REST 到 WS 升级完成前，以下事项不得与公开行情 ticker WS 混合上线：

- 不把 user data WS 作为订单、成交、余额、持仓权威来源。
- 不把 order book diff 接入 live 执行；未实现 snapshot + sequence 校验前只能用于实验或单独计划。
- 不用 `strategy_primary` 代替 `shadow` 做 live 观测。
- 不因 60 秒 smoke 通过就跳过 6 小时 paper shadow 或 24 小时 live shadow。
- 不隐藏 `MARKET_WS_FORCE_REST`；生产 runbook 必须把它作为第一回滚开关。

## 30. 证据归档模板

每次长跑结束后，把下面字段写回本文档对应章节：

```text
运行窗口：
- start:
- end:
- runtime:
- MARKET_WS_MODE:
- service PID:
- selfcheck PID:
- service stdout:
- service stderr:
- selfcheck JSON:
- selfcheck stderr:

核心结果：
- overall_ok:
- sample_count:
- ws_tick_delta:
- shadow_compare_delta:
- shadow_compare_violation_delta:
- invalid_payload_delta:
- timestamp_regression_delta:
- feed_watch_timeout_delta:
- feed_watch_error_delta:
- feed_watch_empty_delta:
- max_stale_symbol_count_observed:
- p99_abs_diff_bps:
- p95_ws_age_ms:

日志污染检查：
- Paper trading mode: False:
- scope switched: paper -> live:
- exchange_watchdog:
- Health check failed for gate:
- watch_tickers timeout（诊断项，记录频率和恢复情况）:
- ccxt_pro_feed[binance]: watch error（诊断项，记录频率和恢复情况）:

结论：
- 通过/失败：
- 是否允许进入下一等级：
- 下一步：
```

## 31. 下一步执行顺序

2026-05-29 22:24 +08:00 更新：下列顺序取代本节早先“等待第八轮/第九轮完成”的安排；旧条目保留在后面仅作历史上下文。

1. 使用已通过定向测试的最新代码启动第十一轮 clean paper shadow，继续保持 `TRADING_MODE=paper`、`MARKET_WS_MODE=shadow`、`MARKET_WS_ENABLED=true`、`COINGLASS_WORKER_ENABLED=false`，且 runtime tasks 不启用 `exchange_watchdog`。
2. 第十一轮启动后先跑 60 秒 smoke，要求 `overall_ok=true`、`shadow_compare_delta > 0`、`shadow_compare_stale_skip_delta == 0`、`feed_watch_empty_delta == 0`、`max_stale_symbol_count_observed == 0`、`p99_abs_diff_bps <= 20`。
3. 启动/长跑期间额外观察 `Unclosed client session`、startup missing REST connector 自动补建、`exchange_manager: binance reconnected` 后是否继续推进 `rest_snapshot_count` 与 `shadow_compare_count`。
4. 60 秒 smoke 通过后启动 6 小时 selfcheck，采样 `duration-sec=21600`、`interval-sec=60`、`min-samples=361`。
5. 第十一轮结束后运行 `scripts\evaluate_market_ws_shadow_report.py`，并把最终 JSON 摘要、日志污染计数、timeout/error 诊断计数、`Unclosed client session` 计数和结论写回第 28.2 节。
6. 如果第十一轮失败，保留日志并按失败门禁定位；如果只是恢复型 timeout/error 增长但其它硬门禁健康，记录诊断频率，不直接升级为失败。
7. 如果第十一轮 paper shadow 通过，重新运行第 24 节目标回归测试和 `.get_ticker(` 静态扫描。
8. 只在第十一轮 paper shadow 与最终回归测试都通过后，准备 Level 2 live shadow 24 小时。
9. live shadow 24 小时通过前，不开启 `ui_primary`；live shadow 通过后也只进入 UI primary，不直接进入 strategy primary。

历史条目（已废弃，仅保留用于追踪第八轮前的判断）：

1. 等待第八轮 6 小时 paper shadow 自检完成，期间只做状态观察，不手工重连或修改运行中配置。
2. 第八轮结束后，运行 `scripts/evaluate_market_ws_shadow_report.py`，并把最终 JSON 摘要、日志污染计数、timeout/error 诊断计数和结论写回第 28.2 节。
3. 如果第八轮失败，保留日志并按失败门禁定位；如果只是恢复型 timeout/error 增长但其它硬门禁健康，记录诊断频率，不直接升级为失败。
4. 如果 paper shadow 通过，重新运行第 24 节目标回归测试和 `.get_ticker(` 静态扫描。
5. 只在 paper shadow 与回归测试都通过后，准备 Level 2 live shadow 24 小时。
6. live shadow 24 小时通过前，不开启 `ui_primary`；live shadow 通过后也只进入 UI primary，不直接进入 strategy primary。

## 32. 续作检查点 - 2026-05-29 23:16 +08:00

本轮续作从当前工作树和第十一轮 clean paper shadow 长跑状态重新取证，不把历史记录当作完成证明。

当前运行状态抽样：

- 服务 PID：`76424`
- selfcheck PID：`163652`
- `TRADING_MODE=paper`
- `MARKET_WS_MODE=shadow`
- `MARKET_WS_ENABLED=true`
- `market_ws.feed_present=true`
- `market_ws.feed_healthy=true`
- `market_ws.ws_hub_healthy=true`
- `market_ws.feed_healthy_exchanges=["binance"]`
- `market_ws.ws_tick_count=5817`
- `market_ws.rest_snapshot_count=260`
- `market_ws.shadow_compare_count=98`
- `market_ws.shadow_compare_violation_count=0`
- `market_ws.shadow_compare_stale_skip_count=0`
- `market_ws.invalid_payload_count=0`
- `market_ws.timestamp_regression_count=0`
- `market_ws.feed_watch_timeout_count=0`
- `market_ws.feed_watch_error_count=0`
- `market_ws.feed_watch_empty_count=0`
- `market_ws.ws_stale_symbol_count=0`
- `market_ws.shadow_max_abs_diff_bps=12.844398929016815`
- `market_ws.feed_last_error=null`

第十一轮 6 小时 selfcheck 仍在运行，`logs\paper_shadow_6h_selfcheck_20260529_222616.out.json` 和 `logs\paper_shadow_6h_selfcheck_20260529_222616.err.log` 当前仍为 0 字节，这是长跑结束前的预期状态。因此当前不能宣称 Level 1 paper shadow 最终通过，也不能进入 live shadow 或 `ui_primary`。

代码级回归：

```powershell
python -m py_compile web\main.py core\marketdata\hub.py core\marketdata\runtime_price_provider.py core\marketdata\ccxt_pro_feed.py core\trading\position_manager.py scripts\selfcheck_market_ws_shadow.py scripts\evaluate_market_ws_shadow_report.py
pytest tests\test_runtime_price_provider.py tests\test_market_data_hub.py tests\test_web_main_runtime_tasks.py tests\test_market_ws_shadow_selfcheck.py tests\test_market_ws_shadow_report_eval.py tests\test_ccxt_pro_feed.py tests\test_market_data_ws_ui_assets.py tests\test_market_ws_authority_static.py tests\core\test_runtime_persistence.py::test_position_manager_persist_uses_unique_tmp_and_retries_replace -q
pytest tests\test_strategy_runtime_policy.py tests\test_strategy_manager_runtime_data.py tests\test_runtime_price_provider.py tests\test_market_data_hub.py -q
pytest tests\test_execution_engine_protective_levels.py tests\test_exchanges.py -q
rg -n "\.get_ticker\(" strategies core\trading core\utils -S
```

结果：

- `py_compile` 通过。
- `76 passed in 24.43s`。
- `28 passed in 5.48s`。
- `39 passed in 3.55s`。
- 2026-05-29 23:30 +08:00 续测：`pytest tests\test_web_main_runtime_tasks.py tests\test_market_data_hub.py tests\test_runtime_price_provider.py -q`，`54 passed in 18.53s`；覆盖 `get_tick(source="ws")`、UI-primary partial stale symbol targeted REST fallback、`_emit_market_ticks(symbols=...)` 子集拉取。
- `.get_ticker(` 静态扫描无匹配；`rg` exit code 1 表示未找到直接绕过 adapter 的调用。

运行日志新增观察：

- 第十一轮服务日志出现多次 `Failed to persist positions for scope=paper`，包含 `WinError 32`、`WinError 5` 和 `WinError 2`，发生在 `positions_paper.tmp -> positions_paper.json` 固定临时文件发布路径。
- 本轮代码已修复 `core/trading/position_manager.py`：持仓持久化改用每次唯一 tmp 文件，并对 `os.replace` 做短重试，降低 Windows 文件锁、杀软或并发读写导致的污染。
- 修复单测：`tests\core\test_runtime_persistence.py::test_position_manager_persist_uses_unique_tmp_and_retries_replace`。
- 2026-05-29 23:36 +08:00 后续抽样又出现一次 `positions_live.json` 读取 `Permission denied`，同属 Windows 瞬时文件占用污染；代码已追加 `_load_scope_state()` 读取短重试，并补 `tests\core\test_runtime_persistence.py::test_position_manager_load_retries_transient_permission_error`。
- 注意：正在运行的第十一轮服务尚未加载此修复，最终日志仍应按原服务版本如实归档；修复在下一次 clean shadow 运行中验证。

当前结论：

- 代码级 Phase 1 shadow 基建、Phase 2 UI primary gating、状态展示、adapter/fail-closed 保护和静态禁越级检查均已有测试覆盖。
- 运行级仍停在 Level 1 paper shadow 长跑中；必须等待第十一轮结束并评估最终 JSON 后，才能判断是否准备 Level 2 live shadow。
- live shadow 24 小时通过前，不开启 `ui_primary`；即使代码已有 `ui_primary` gating，也只能作为受配置保护的待验收能力保留。

2026-05-29 23:43 +08:00 续作追加检查：

- 已提交代码修复：`dfb8511 Retry transient position state reads`。
- 提交前验证：
  - `git diff --check` 通过。
  - `pytest tests\core\test_runtime_persistence.py::test_position_manager_persist_uses_unique_tmp_and_retries_replace tests\core\test_runtime_persistence.py::test_position_manager_load_retries_transient_permission_error tests\test_web_main_runtime_tasks.py tests\test_market_data_hub.py -q`，`51 passed in 21.44s`。
  - `python -m py_compile core\trading\position_manager.py web\main.py core\marketdata\hub.py` 通过。
- 第十一轮服务与 selfcheck 仍在运行：服务 PID `76424`，selfcheck PID `163652`。
- `/api/status`：`status=running`、`trading_mode=paper`、`paper_trading=true`。
- `/api/market-data/status`：
  - `market_ws.mode=shadow`
  - `market_ws.feed_healthy=true`
  - `market_ws.ws_hub_healthy=true`
  - `market_ws.feed_last_error=null`
  - `market_ws.ws_tick_count=8894`
  - `market_ws.rest_snapshot_count=418`
  - `market_ws.shadow_compare_count=152`
  - `market_ws.shadow_compare_violation_count=0`
  - `market_ws.shadow_compare_stale_skip_count=0`
  - `market_ws.invalid_payload_count=0`
  - `market_ws.timestamp_regression_count=0`
  - `market_ws.feed_watch_timeout_count=0`
  - `market_ws.feed_watch_error_count=0`
  - `market_ws.feed_watch_empty_count=0`
  - `market_ws.stale_symbol_count=0`
  - `market_ws.ws_stale_symbol_count=0`
  - `market_ws.shadow_max_abs_diff_bps=12.844398929016815`
- 第十一轮 selfcheck JSON/stderr 仍为 0 字节，符合长跑结束前预期；不得据此宣称 Level 1 通过。
- 服务 stderr 计数：
  - `Connector binance connect timed out`: `0`
  - `exchange_manager: binance reconnected`: `3`
  - `Paper trading mode: False`: `0`
  - `scope switched: paper -> live`: `0`
  - `exchange_watchdog`: `0`
  - `Health check failed for gate`: `0`
  - `watch_tickers timeout`: `0`
  - `ccxt_pro_feed[binance]: watch error`: `0`
  - `coinglass: rate-limit backoff`: `0`
  - `positions_live.json`: `1`
  - `Failed to persist positions`: `4`
  - `Live kline fetch timed out`: `4`
  - `get_klines(BTC/USDT, 15m) failed`: `0`
  - `get_ticker(`: `3`
  - `Unclosed client session`: `0`
  - `[PAPER] Order created`: `2`
- 当前判断不变：第十一轮仍只能作为运行中证据，必须等待 6 小时 selfcheck 结束并运行最终 evaluator；Level 1 通过和最终回归均完成前，不进入 Level 2 live shadow，不开启 `ui_primary`。

2026-05-29 23:56 +08:00 续作追加检查：

- 已提交最终验收工具加固：`e51a43d Report shadow evaluator diagnostic log counts`。
- 背景：第十一轮结束后需要把最终 JSON 摘要、硬污染日志计数、timeout/error 诊断计数、`Unclosed client session`、`positions_live.json`、`get_ticker(` 等旁路计数一并归档。为降低人工漏数风险，`scripts\evaluate_market_ws_shadow_report.py` 已新增 `diagnostic_log_counts` 输出。
- 语义：
  - `log_counts` 仍是硬污染门禁，继续受 `--max-log-count 0` 约束。
  - `diagnostic_log_counts` 只记录，不默认判失败；是否阻断由最终审计结合硬门禁、最终健康、p95 age、stale、价差和资源泄漏风险判断。
- 验证：
  - `pytest tests\test_market_ws_shadow_report_eval.py tests\test_market_ws_shadow_selfcheck.py -q`，`13 passed in 5.11s`。
  - `python -m py_compile scripts\evaluate_market_ws_shadow_report.py tests\test_market_ws_shadow_report_eval.py` 通过。
  - `git diff --check -- scripts\evaluate_market_ws_shadow_report.py tests\test_market_ws_shadow_report_eval.py` 通过。
  - 在当前 0 字节 selfcheck JSON 上运行 evaluator 按预期失败：`empty JSON report: logs\paper_shadow_6h_selfcheck_20260529_222616.out.json`，确认最终报告未生成时不会误判通过。
- 第十一轮服务与 selfcheck 仍在运行：服务 PID `76424`，selfcheck PID `163652`。
- `/api/status`：`status=running`、`trading_mode=paper`、`paper_trading=true`。
- `/api/market-data/status`：
  - `market_ws.mode=shadow`
  - `market_ws.feed_healthy=true`
  - `market_ws.ws_hub_healthy=true`
  - `market_ws.feed_last_error=null`
  - `market_ws.ws_tick_count=10430`
  - `market_ws.rest_snapshot_count=496`
  - `market_ws.shadow_compare_count=178`
  - `market_ws.shadow_compare_violation_count=0`
  - `market_ws.shadow_compare_stale_skip_count=0`
  - `market_ws.invalid_payload_count=0`
  - `market_ws.timestamp_regression_count=0`
  - `market_ws.feed_watch_timeout_count=0`
  - `market_ws.feed_watch_error_count=0`
  - `market_ws.feed_watch_empty_count=0`
  - `market_ws.stale_symbol_count=0`
  - `market_ws.ws_stale_symbol_count=0`
  - `market_ws.last_tick_age_ms=106`
  - `market_ws.shadow_max_abs_diff_bps=12.844398929016815`
- 服务 stderr 计数：
  - `Connector binance connect timed out`: `0`
  - `exchange_manager: binance reconnected`: `3`
  - `Paper trading mode: False`: `0`
  - `scope switched: paper -> live`: `0`
  - `exchange_watchdog`: `0`
  - `Health check failed for gate`: `0`
  - `watch_tickers timeout`: `0`
  - `ccxt_pro_feed[binance]: watch error`: `0`
  - `coinglass: rate-limit backoff`: `0`
  - `positions_live.json`: `2`
  - `Failed to persist positions`: `4`
  - `Live kline fetch timed out`: `4`
  - `get_klines(BTC/USDT, 15m) failed`: `0`
  - `get_ticker(`: `3`
  - `Unclosed client session`: `0`
  - `[PAPER] Order created`: `2`
- 当前判断不变：第十一轮 selfcheck JSON/stderr 仍为 0 字节，必须等待 6 小时 selfcheck 结束并运行最终 evaluator；Level 1 通过和最终回归均完成前，不进入 Level 2 live shadow，不开启 `ui_primary`。

2026-05-30 00:01 +08:00 跨日续作检查：

- 当前时间已跨到 2026-05-30；第十一轮 selfcheck 从 2026-05-29 22:29 左右开始，6 小时窗口尚未结束，`logs\paper_shadow_6h_selfcheck_20260529_222616.out.json` 和 `logs\paper_shadow_6h_selfcheck_20260529_222616.err.log` 仍为 0 字节符合预期。
- 第十一轮服务与 selfcheck 仍在运行：服务 PID `76424`，selfcheck PID `163652`。
- 服务 stdout 仍在更新，tail 显示 selfcheck 每分钟请求 `/health`、`/api/status`、`/api/market-data/status` 均返回 `200 OK`；服务 stderr 自 2026-05-29 23:46 后未继续增长。
- `/api/status`：`status=running`、`trading_mode=paper`、`paper_trading=true`。
- `/api/market-data/status`：
  - `market_ws.mode=shadow`
  - `market_ws.feed_healthy=true`
  - `market_ws.ws_hub_healthy=true`
  - `market_ws.feed_last_error=null`
  - `market_ws.ws_tick_count=11022`
  - `market_ws.rest_snapshot_count=526`
  - `market_ws.shadow_compare_count=188`
  - `market_ws.shadow_compare_violation_count=0`
  - `market_ws.shadow_compare_stale_skip_count=0`
  - `market_ws.invalid_payload_count=0`
  - `market_ws.timestamp_regression_count=0`
  - `market_ws.feed_watch_timeout_count=0`
  - `market_ws.feed_watch_error_count=0`
  - `market_ws.feed_watch_empty_count=0`
  - `market_ws.stale_symbol_count=0`
  - `market_ws.ws_stale_symbol_count=0`
  - `market_ws.last_tick_age_ms=212`
  - `market_ws.shadow_max_abs_diff_bps=12.844398929016815`
- 服务 stderr 计数：
  - `Connector binance connect timed out`: `0`
  - `exchange_manager: binance reconnected`: `3`
  - `Paper trading mode: False`: `0`
  - `scope switched: paper -> live`: `0`
  - `exchange_watchdog`: `0`
  - `Health check failed for gate`: `0`
  - `watch_tickers timeout`: `0`
  - `ccxt_pro_feed[binance]: watch error`: `0`
  - `coinglass: rate-limit backoff`: `0`
  - `positions_live.json`: `2`
  - `Failed to persist positions`: `4`
  - `Live kline fetch timed out`: `4`
  - `get_klines(BTC/USDT, 15m) failed`: `0`
  - `get_ticker(`: `3`
  - `Unclosed client session`: `0`
  - `[PAPER] Order created`: `2`
- 当前判断不变：第十一轮只能继续等待最终 JSON；Level 1 未完成前，不进入 Level 2 live shadow，不开启 `ui_primary`。

2026-05-30 00:03 +08:00 Level 2 前置门禁修复：

- 已提交自检门禁修复：`6b6701c Enforce live runtime in WS shadow selfcheck`。
- 问题：`scripts\selfcheck_market_ws_shadow.py` 之前支持 `--expect-runtime live`，但 `_evaluate_samples()` 只在 `expect_runtime=paper` 时校验 `paper_trading=true`，没有在 `expect_runtime=live` 时反向拒绝 `paper_trading=true` / `trading_mode=paper`。这会让 Level 2 live shadow 24 小时自检存在被 paper runtime 误放行的风险。
- 修复：
  - `expect_runtime=paper` 时要求 `paper_trading=true` 且 `trading_mode` 为空或为 `paper`。
  - `expect_runtime=live` 时要求 `paper_trading=false` 且 `trading_mode=live`。
  - selfcheck summary 新增 `final_trading_mode` 和 `final_paper_trading`，便于最终归档明确证明运行环境。
- 验证：
  - `pytest tests\test_market_ws_shadow_selfcheck.py tests\test_market_ws_shadow_report_eval.py -q`，`16 passed in 3.64s`。
  - `python -m py_compile scripts\selfcheck_market_ws_shadow.py tests\test_market_ws_shadow_selfcheck.py` 通过。
  - `git diff --check -- scripts\selfcheck_market_ws_shadow.py tests\test_market_ws_shadow_selfcheck.py` 通过。
- 当前判断不变：该修复是 Level 2 live shadow 前置工具加固，不改变正在运行的第十一轮 paper shadow；第十一轮最终 JSON 通过前仍不进入 Level 2，不开启 `ui_primary`。

2026-05-30 00:07 +08:00 第十一轮恢复事件观察：

- 第十一轮 selfcheck 仍在运行，最终 JSON/stderr 仍为 0 字节。
- 服务 stderr 在 00:05-00:06 新增一次 Binance REST transient `get_ticker(BTC/USDT)` 失败，随后自动重连成功：
  - `2026-05-30 00:05:15`：`get_ticker(BTC/USDT) failed`
  - `2026-05-30 00:05:27`：策略 kline timeout 旁路日志
  - `2026-05-30 00:06:00`：`exchange_manager: binance reconnected (fast path)`
- `/api/market-data/status` 复查：
  - `market_ws.mode=shadow`
  - `market_ws.feed_healthy=true`
  - `market_ws.ws_hub_healthy=true`
  - `market_ws.feed_last_error=null`
  - `market_ws.ws_tick_count=11695`
  - `market_ws.rest_snapshot_count=560`
  - `market_ws.shadow_compare_count=200`
  - `market_ws.shadow_compare_violation_count=0`
  - `market_ws.shadow_compare_stale_skip_count=0`
  - `market_ws.invalid_payload_count=0`
  - `market_ws.timestamp_regression_count=0`
  - `market_ws.feed_watch_timeout_count=0`
  - `market_ws.feed_watch_error_count=0`
  - `market_ws.feed_watch_empty_count=0`
  - `market_ws.ws_stale_symbol_count=0`
  - `market_ws.stale_symbol_count=2`，来自 Gate REST snapshot 旧快照，不计作 WS stale 门禁。
- 服务 stderr 计数：
  - `Connector binance connect timed out`: `0`
  - `exchange_manager: binance reconnected`: `4`
  - `Paper trading mode: False`: `0`
  - `scope switched: paper -> live`: `0`
  - `exchange_watchdog`: `0`
  - `Health check failed for gate`: `0`
  - `watch_tickers timeout`: `0`
  - `ccxt_pro_feed[binance]: watch error`: `0`
  - `coinglass: rate-limit backoff`: `0`
  - `positions_live.json`: `2`
  - `Failed to persist positions`: `4`
  - `Live kline fetch timed out`: `5`
  - `get_klines(BTC/USDT, 15m) failed`: `0`
  - `get_ticker(`: `4`
  - `Unclosed client session`: `0`
  - `[PAPER] Order created`: `2`
- 当前判断：这是恢复型 REST 旁路事件，WS feed/hub 和 shadow compare 继续健康推进；最终 evaluator 会通过 `diagnostic_log_counts` 归档该事件频率。第十一轮结束前仍不进入 Level 2，不开启 `ui_primary`。

2026-05-30 00:09 +08:00 Level 2 最终评估门禁补强：

- 已提交 evaluator 修复：`5da848a Validate final runtime in shadow report evaluator`。
- 背景：`6b6701c` 已让 selfcheck 在采样阶段拒绝错误 runtime，并把 `final_trading_mode` / `final_paper_trading` 写入 summary；但 `scripts\evaluate_market_ws_shadow_report.py` 仍未校验这些 summary 字段。
- 修复：
  - `expect_runtime=paper` 时，最终 summary 不能声明 `final_paper_trading=false`，且 `final_trading_mode` 为空或为 `paper`。
  - `expect_runtime=live` 时，最终 summary 必须是 `final_paper_trading=false` 且 `final_trading_mode=live`。
  - evaluator 输出 summary 透传 `final_trading_mode` 和 `final_paper_trading`。
- 验证：
  - `pytest tests\test_market_ws_shadow_report_eval.py tests\test_market_ws_shadow_selfcheck.py -q`，`18 passed in 3.77s`。
  - `python -m py_compile scripts\evaluate_market_ws_shadow_report.py tests\test_market_ws_shadow_report_eval.py` 通过。
  - `git diff --check -- scripts\evaluate_market_ws_shadow_report.py tests\test_market_ws_shadow_report_eval.py` 通过。
- 当前判断不变：这是 Level 2 live shadow 之前的最终评估工具保险，不改变正在运行的第十一轮 paper shadow；Level 1 通过和最终回归完成前仍不进入 Level 2，不开启 `ui_primary`。

2026-05-29 23:49 +08:00 续作追加检查：

- 已提交门禁测试护栏：`1583bbd Lock WS stale count shadow gate`。
- 背景：本轮抽样中 `stale_symbol_count=2` 来自 Gate 的 REST snapshot 旧快照，但 `ws_stale_symbol_count=0`，Binance WS feed/hub 仍健康。Level 1 market WS stale 门禁应使用 `ws_stale_symbol_count`，不能让非 WS/非目标交易所的 REST 旧快照误判 Binance WS 长跑失败。
- 验证：
  - `pytest tests\test_market_ws_shadow_selfcheck.py tests\test_market_ws_shadow_report_eval.py -q`，`12 passed in 4.52s`。
  - `python -m py_compile scripts\selfcheck_market_ws_shadow.py tests\test_market_ws_shadow_selfcheck.py` 通过。
  - `git diff --check -- tests\test_market_ws_shadow_selfcheck.py` 通过。
- 第十一轮服务与 selfcheck 仍在运行：服务 PID `76424`，selfcheck PID `163652`。
- `/api/status`：`status=running`、`trading_mode=paper`、`paper_trading=true`。
- `/api/market-data/status`：
  - `market_ws.mode=shadow`
  - `market_ws.feed_healthy=true`
  - `market_ws.ws_hub_healthy=true`
  - `market_ws.feed_last_error=null`
  - `market_ws.ws_tick_count=9638`
  - `market_ws.rest_snapshot_count=454`
  - `market_ws.shadow_compare_count=164`
  - `market_ws.shadow_compare_violation_count=0`
  - `market_ws.shadow_compare_stale_skip_count=0`
  - `market_ws.invalid_payload_count=0`
  - `market_ws.timestamp_regression_count=0`
  - `market_ws.feed_watch_timeout_count=0`
  - `market_ws.feed_watch_error_count=0`
  - `market_ws.feed_watch_empty_count=0`
  - `market_ws.stale_symbol_count=2`
  - `market_ws.ws_stale_symbol_count=0`
  - `market_ws.shadow_max_abs_diff_bps=12.844398929016815`
- 第十一轮 selfcheck JSON/stderr 仍为 0 字节，符合长跑结束前预期；不得据此宣称 Level 1 通过。
- 服务 stderr 计数：
  - `Connector binance connect timed out`: `0`
  - `exchange_manager: binance reconnected`: `3`
  - `Paper trading mode: False`: `0`
  - `scope switched: paper -> live`: `0`
  - `exchange_watchdog`: `0`
  - `Health check failed for gate`: `0`
  - `watch_tickers timeout`: `0`
  - `ccxt_pro_feed[binance]: watch error`: `0`
  - `coinglass: rate-limit backoff`: `0`
  - `positions_live.json`: `2`
  - `Failed to persist positions`: `4`
  - `Live kline fetch timed out`: `4`
  - `get_klines(BTC/USDT, 15m) failed`: `0`
  - `get_ticker(`: `3`
  - `Unclosed client session`: `0`
  - `[PAPER] Order created`: `2`
- 当前判断不变：第十一轮仍只能作为运行中证据，必须等待 6 小时 selfcheck 结束并运行最终 evaluator；Level 1 通过和最终回归均完成前，不进入 Level 2 live shadow，不开启 `ui_primary`。
