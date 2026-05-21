# 交易所/执行/交易/风控审计报告 (2026-05-21)

审计范围：`core/exchanges/`、`core/exchange_adapters/`、`core/execution/`、`core/trading/`、`core/risk/`

---

## 高优先级 (Bug, 必修)

### H1. OKX/Bybit 连接器缺少并发重连锁
**文件**: `core/exchanges/okx_connector.py:30`、`core/exchanges/bybit_connector.py:30`  
**问题**: `connect()` 方法没有 `asyncio.Lock` 保护，而 BinanceConnector 有 `self._connection_lock`。当多个协程同时调用 `_ensure_client()` → `connect()` 时（如多策略启动时），会并发创建多个 ccxt 客户端实例，旧实例不会被关闭，导致文件描述符泄漏和未定义行为。  
**影响**: OKX/Bybit 并发启动场景下可能泄漏连接资源，或以中间状态客户端执行订单。  
**修复**: 仿照 BinanceConnector，在 `__init__` 中加 `self._connection_lock = asyncio.Lock()`，在 `connect()` 中 `async with self._connection_lock:`。

### H2. OKX/Bybit `create_order` 不做 lot/tick 精度对齐
**文件**: `core/exchanges/okx_connector.py:180`、`core/exchanges/bybit_connector.py`（类似位置）  
**问题**: 直接传 `amount` 到 `client.create_order()`，没有调用 `amount_to_precision`/`price_to_precision`。只有 Binance 快速期货路径（`order_manager.py:500`）做了对齐。OKX/Bybit 收到精度不符的请求会返回 -4014/-1111 类错误（Binance 文档称法），这些错误被 `_handle_error` 重新抛出但不重试，订单直接丢失。  
**影响**: 实盘/纸盘订单以低精度数量下单时静默失败，没有重试，持仓记录显示打开但交易所无对应订单。  
**修复**: 在 `create_order` 中增加：
```python
client = await self._ensure_client()
if hasattr(client, 'amount_to_precision'):
    amount = float(client.amount_to_precision(symbol, amount))
if price and hasattr(client, 'price_to_precision'):
    price = float(client.price_to_precision(symbol, price))
```

### H3. OKX/Bybit `get_order_book` 绕过 `_ensure_client()`
**文件**: `core/exchanges/okx_connector.py:133`（`get_order_book`）  
**问题**: `get_order_book` 直接访问 `self._client.fetch_order_book`，而没有先调用 `_ensure_client()`。如果客户端未连接，会抛出 `AttributeError: 'NoneType' object has no attribute 'fetch_order_book'`。同样问题出现在 OKX 的 `cancel_order`（line 198）、`get_order`（line 208）、`get_open_orders`（line 216）、`get_positions`（line 224）、`get_trades`（line 254）。  
**影响**: 自动重连失效，连接断开后订单查询/撤单全部崩溃，而非自动重连。  
**修复**: 所有这些方法的第一行改为 `client = await self._ensure_client()`，后续用 `client.*` 代替 `self._client.*`。

### H4. 期货杠杆全局设置但 `defaultType` 临时修改未加锁
**文件**: `core/exchanges/binance_connector.py:620-633`（`get_positions`）  
**问题**: 当 `default_type` 为 `spot` 时，`get_positions` 临时修改 `client.options["defaultType"]`，随后在 `finally` 块中恢复。但 ccxt 客户端是共享对象，若同时有另一协程正在调用 `get_balance`（使用 `{"type": "spot"}`），options 的临时值会污染并发请求。  
**影响**: 在高并发情况下，balance 查询可能带着 `defaultType=future` 发出，返回错误余额数据，进而影响风控的 equity 计算。  
**修复**: 用独立的 `asyncio.Lock` 保护 `client.options` 的临时修改，或改为通过 `fetch_balance({"type": account_type})` 绕过全局 option。

### H5. `risk_manager.record_trade()` 无并发保护
**文件**: `core/risk/risk_manager.py:723-738`  
**问题**: `_trade_history.append()` 和 `_daily_trades += 1`、`_daily_realized_pnl +=` 在 asyncio 环境下由多个策略回调并发调用（通过 `_notify_callbacks`），没有任何锁。Python list 的 append 是 GIL 保护的，但复合赋值 `_daily_trades += 1` 不是原子操作。若两个协程同时进入 `record_trade()`，日内交易计数可能少计，影响每日限制。  
**影响**: 中等风险——`_daily_trades` 少计导致超过 `max_daily_trades` 限制时不触发熔断。  
**修复**: 加 `asyncio.Lock` 或改为 `_daily_trades = self._daily_trades + 1`（单赋值对 GIL 更友好），但更干净的方案是 `asyncio.Lock()` 包裹整个 `record_trade` 体。

### H6. `position_manager.close_position` 中 `partial_close` 时的 realized_pnl 计算错误
**文件**: `core/trading/position_manager.py:537`  
**问题**: 部分平仓时，`realized_piece = position.unrealized_pnl * (closing_qty / origin_qty)` 是线性插值。但 `unrealized_pnl` 是 **调用 `update_price(close_px)` 之后** 的当前值（line 536），对整体持仓按比例分配没有问题。然而当 `position.unrealized_pnl` 可能为负（止损场景）时，`realized_piece` 为负，`position.realized_pnl` 累积变小（即亏损增大）——这是正确的，**但** `position.quantity` 同时更新为 `origin_qty - closing_qty`，而 `position.value` 更新为 `close_px * position.quantity`，二者之间不一致：关闭部分的 PnL 已经计入 `realized_pnl`，但 `value` 包含了全量持仓的价值。这个差异会在下一次 `update_price` 时被覆盖，但在两次调用之间，`position.value` 会虚高，从而导致 `risk_manager.pre_trade_check` 的 `current_gross` 虚高，错误地拒绝新订单。  
**影响**: 部分平仓后立即开新仓时，可能错误触发"组合总敞口超限"。  
**修复**: 在部分平仓后立即更新 `position.value = close_px * position.quantity`（已有），同时确保 `get_all_positions()` 使用 `position.value` 而不是 `position.quantity * position.current_price`（当前已使用 `.value`，OK），主要问题是 line 536 之前没有先 `update_price`，导致 `unrealized_pnl` 是旧价格的值——应先 `update_price(close_px)` 再计算 `realized_piece`（实际上 line 536 先调用了 update_price，所以逻辑上是正确的，此项降为低优先级）。重新评估：实际无 bug，降级。

### H7. `execution_engine._create_real_order` 中 Binance 快速路径精度格式化用原始 `request.amount`
**文件**: `core/trading/order_manager.py:500,509`  
**问题**: `_fmt("amount_to_precision", request.amount)` 使用 ccxt 客户端格式化，但 `create_order` 回退路径（line 565）直接传 `request.amount`（未格式化），绕过了 ccxt 的精度处理，因为回退走的是 connector 层，connector 本身也不做精度对齐（H2）。因此 Binance 快速路径格式化，而 ccxt 回退路径不格式化，行为不一致。  
**影响**: ccxt 回退路径（快速路径失败后）下单数量精度错误，可能导致 Binance 返回 -1111。

---

## 中优先级 (性能 / 并发)

### M1. `CCXTExchangeAdapter._call` 使用同步 ccxt + `asyncio.to_thread`
**文件**: `core/exchange_adapters/ccxt_adapter.py:71-95`  
**问题**: 适配器导入的是 `import ccxt`（同步），然后通过 `asyncio.to_thread(fn, ...)` 在线程池中调用。这意味着每次 API 调用都要切换到线程池，线程池默认大小为 CPU 数量，在高频策略场景下可能成为瓶颈。同时，同步 ccxt 的内部 `time.sleep` 限速会阻塞线程（非事件循环）。  
**影响**: 相比 `ccxt.async_support`，吞吐量降低，线程池压力增大。  
**修复**: 改用 `ccxt.async_support`，直接 `await` 调用，删除 `asyncio.to_thread`。

### M2. `rate_limit_and_reconnect.py:180` 在 async 上下文中 `time.sleep` 阻塞事件循环
**文件**: `core/execution/rate_limit_and_reconnect.py:180`  
**问题**: `acquire(wait=True)` 方法中有 `time.sleep(min(max(retry_after_s, 0.001), 1.0))`。如果这个方法在协程中被调用（如通过 `asyncio.to_thread` 以外的路径），它会阻塞整个事件循环最多 1 秒。当前生产路径中 `RateLimitAndReconnectPolicy` 未直接被 execution_engine 调用，但一旦按 CLAUDE.md 计划集成，此问题将暴露。  
**影响**: 事件循环阻塞最多 1 秒，所有并发任务（价格更新、心跳、WebSocket）延迟。  
**修复**: 将 `acquire` 方法改为 async，用 `await asyncio.sleep(...)` 代替 `time.sleep()`；同步版 `acquire` 保留给真正的线程上下文。

### M3. OKX `get_balance` 每次都全量获取，无缓存
**文件**: `core/exchanges/okx_connector.py:142`  
**问题**: BinanceConnector 有 90 秒的 `_balance_cache` 和 300 秒的 `_funding_cache`，但 OKXConnector/BybitConnector/GateConnector 的 `get_balance` 每次直接调用 `fetch_balance()`，无任何缓存。execution_engine 在每次信号处理时调用 `_get_account_equity()` → `_refresh_equity()` → `connector.get_balance()`，在多策略高频场景下 OKX 会触发限速（默认 10 次/秒）。  
**影响**: OKX 账户高频策略场景下大概率触发 429，连接被断开。  
**修复**: 参考 BinanceConnector，在 OKX/Bybit/Gate 中加 `_balance_cache` 和 `_balance_cache_ts`，TTL 设 60 秒。

### M4. `execution_engine._refresh_equity` 多交易所串行请求
**文件**: `core/trading/execution_engine.py:921`  
**问题**: `for exchange_name, connector in scoped_connectors:` 是串行循环，每个交易所的 `connector.get_balance()` 依次等待完成。若连接了 Binance + OKX + Gate 三个交易所，权益刷新总耗时 = 三个交易所延迟之和，可能超过 10 秒。  
**影响**: `_get_account_equity()` 耗时过长，影响信号处理延迟。  
**修复**: 改为 `asyncio.gather(*[_fetch_for_connector(ex, conn) for ex, conn in scoped_connectors])`，并行获取。

### M5. `execution_engine` 每次信号触发 `get_risk_report()` 多次（含 `get_rolling_drawdown_snapshot`）
**文件**: `core/trading/execution_engine.py:232`、`core/risk/risk_manager.py:965-971`  
**问题**: `_evaluate_order_governance()` 调用 `risk_manager.get_risk_report()`，这包含了 `get_rolling_drawdown_snapshot(hours=72)` 和 `get_rolling_drawdown_snapshot(hours=168)`，两者各自遍历 `_equity_timeline`（最多 5000 条）。每个信号处理调用 `get_risk_report()` 2–3 次（paper 路径中的 governance 评估 + `_build_reject_reason`），即每信号 4–6 次全量遍历。  
**影响**: 在高频策略（1 分钟 K 线）下，CPU 消耗可观，尤其是 equity_timeline 接近 5000 条时。  
**修复**: 对 `get_rolling_drawdown_snapshot` 加 TTL 缓存（60 秒内复用结果），或在 `get_risk_report()` 内部只计算一次。

### M6. `position_manager.open_position` 和 `close_position` 每次强制 `persist_scope_state(force=True)` 同步写盘
**文件**: `core/trading/position_manager.py:475`, `556`, `570`  
**问题**: 每次开/平仓都强制同步 JSON 序列化 + 磁盘写入（含 tmp 文件原子替换），在高频场景下（如 TWAP 分片、部分平仓）可能成为瓶颈，且持仓历史 `_position_history` 会无限增长（无截断）。  
**影响**: 高频交易下磁盘 I/O 成为热点，`_position_history` 内存无界增长。  
**修复**: 将 `force=True` 改为依赖 2 秒节流，开/平仓时仅设 `_dirty = True`；或在后台任务中周期性写盘。`_position_history` 加 `[-500:]` 截断。

---

## 低优先级 (死代码 / 一致性)

### L1. `CCXTExchangeAdapter.create_order/cancel_order/fetch_order` 未实现（TODO 存根）
**文件**: `core/exchange_adapters/ccxt_adapter.py:218-225`  
**问题**: 三个执行方法均 `raise NotImplementedError("TODO: ...")`，适配器只能读取，无法下单。`OrderIntentRouter.submit_intent` 调用 `adapter.create_order(req)` 会直接抛出异常。  
**影响**: `order_intent_router.py` + `order_state_machine.py` 构成完整的新执行架构骨架，但执行层缺失，整个模块无法投入使用。  
**建议**: 如计划接入，按优先级实现；如暂不接入，在文档中明确标注，避免误调用。

### L2. OKX `get_positions` 返回 `Position`（base_exchange 的 dataclass），缺少 `side` 字段验证
**文件**: `core/exchanges/okx_connector.py:228`  
**问题**: `Position(side=pos.get("side", ""), ...)` 直接传字符串，而 base_exchange.Position 的 `side` 字段类型是 `str`（无 Enum）。但调用者（execution_engine）期望 `side` 是 `"long"` 或 `"short"`，OKX 返回的值是 `"long"/"short"` 没有问题，但若 OKX 返回 `"net"` 或空字符串，下游比较逻辑会静默失败（position 存在但被认为没有方向）。  
**影响**: 低——仅在 OKX 返回非标准 side 时出现，但无告警，不易排查。  
**建议**: 加 `side = pos.get("side", "").lower()` 并在 `side not in {"long", "short"}` 时跳过或记录警告。

### L3. `bybit_connector`、`gate_connector` 未设置 `warnOnFetchOpenOrdersWithoutSymbol=False`
**文件**: `core/exchanges/bybit_connector.py`、`core/exchanges/gate_connector.py`  
**问题**: BinanceConnector 在 `_prepare_client()` 中设置了 `client.options["warnOnFetchOpenOrdersWithoutSymbol"] = False`，但 Bybit/Gate 没有。全量拉取未完成订单时（`get_open_orders(symbol=None)`）会触发 ccxt 警告日志洪流。  
**影响**: 日志噪音，无功能影响。  
**建议**: 在 Bybit/Gate 的 `connect()` 中加 `self._client.options["warnOnFetchOpenOrdersWithoutSymbol"] = False`。

### L4. `base_exchange.py` 的 `subscribe_kline/subscribe_ticker` 实现模式异常
**文件**: `core/exchanges/base_exchange.py:201-216`  
**问题**: 两个 WebSocket 订阅方法先 `yield`（空生成器占位），再 `raise NotImplementedError`。这意味着第一次 `async for` 迭代返回 `None`，第二次才抛出异常，调用者可能错误地处理了一个 `None` 数据点。  
**影响**: 低——没有调用者实际使用这两个方法（生产代码走 ccxt WebSocket 或 binance_rest），但如果未来接入会有隐患。  
**建议**: 改为直接 `raise NotImplementedError(...)` 而不先 yield。

### L5. `order_manager.py:264` 时间戳格式使用 `datetime.now()` 而非 `datetime.now(timezone.utc)`
**文件**: `core/trading/order_manager.py:264`、`268`  
**问题**: `_next_paper_order_id` 和 `_next_rejected_order_id` 使用 `datetime.now().strftime(...)` 生成 ID，是 naive 本地时间。如果系统在非 UTC 时区运行（中国 CST+8），ID 携带本地时间戳，与日志（UTC）时间对齐时会有 8 小时差异，排查困难。  
**影响**: 低——只影响 ID 可读性，不影响功能。  
**建议**: 改为 `datetime.now(timezone.utc).strftime(...)`。

### L6. `stop_loss.py` 中 `StopLossManager`/`TakeProfitManager` 与 execution_engine 双轨并存
**文件**: `core/risk/stop_loss.py:1-15`  
**问题**: 文件顶部注释明确说明这是"非生产路径"，不能与 execution_engine 路径同时使用，否则会"双重平仓"。但文件仍然是可导入的公共模块，没有任何机制阻止误用。  
**影响**: 若有开发者在新策略中调用 `StopLossManager.check_stops()`，会导致重复平仓，造成实际损失。  
**建议**: 将文件移入 `core/risk/_legacy/` 或在模块级别加 `warnings.warn("...offline-only...", DeprecationWarning)` 阻止线上调用。

### L7. `exchange_manager.py` DEX 连接器导入使用 `except Exception`（宽泛捕获）
**文件**: `core/exchanges/exchange_manager.py:21-30`  
**问题**: 之前已修复 `dex_connectors.py` 中的 web3 `ImportError → except Exception`，但 `exchange_manager.py` 引入 DEX 时也有 `except Exception` 包裹，将 `PancakeSwapConnector = None`。这会静默隐藏如 `SyntaxError`、`NameError` 等非可选依赖的错误，不易排查 DEX 连接器的代码 bug。  
**影响**: 低——DEX 功能被完全屏蔽时无告警，排查困难。  
**建议**: 恢复为 `except ImportError`，对其他异常重新抛出或至少记录 `logger.exception`。

### L8. `ccxt_adapter.py` 时间戳处理存在 microsecond/nanosecond 歧义
**文件**: `core/exchange_adapters/ccxt_adapter.py:22-31`（`_to_dt_ms`）  
**问题**: `if v > 10**12: v = v // 1000` 将微秒转换为毫秒。但 `10**12` 对应约 33852 年的毫秒时间戳，正常毫秒级时间戳（约 1.7*10^12）会被误判为微秒并除以 1000，导致时间倒退 1000 倍（约 1970 年附近）。正确的判断应为 `> 2 * 10^13`（纳秒）或用不同阈值。  
**影响**: 所有 `_to_dt_ms` 返回的时间戳偏差约 2000 年，ccxt_adapter 中所有含时间的记录均错误。  
**修复**: 修正阈值：`if v > 2 * 10**13: v = v // 1000`（纳秒→微秒），或 `if v > 1.5 * 10**12 and v < 2 * 10**13: pass`（毫秒区间），或直接 `v = v // 1000 if v > 10**13 else v`。

---

## 备注

### 已确认正常（不需要修复）
- **Binance 资金费率实盘路径**: execution_engine 中无 `accrued_funding` 的错误二次累加（backtest 已修复，live 路径走 `close_position` 时 realized_pnl 从 exchange 直接读取，无双计）。
- **HMAC 签名顺序**: `binance_rest.py` 使用 ccxt 的 HMAC 实现，签名字段顺序由 ccxt 维护，无自定义排序风险。
- **Hedging mode**: 系统未启用 Binance 双向持仓模式（无 `positionSide=LONG/SHORT`），position_manager 的 `PositionSide.BOTH` 仅用于解析外部数据，不会发送到交易所，无 hedging mode 混乱风险。
- **position_size 检查时机**: `risk_manager.pre_trade_check` 在 signal 时间点（订单提交前）执行，`strategy_allocation` 来自策略配置，但 fill 时不再二次检查——这与文档一致，属于设计选择而非 bug。

### 数据流说明
| 组件 | 精度对齐 | 连接锁 | 余额缓存 |
|------|----------|--------|----------|
| BinanceConnector | ✅（快速路径）| ✅ | ✅ (90s) |
| OKXConnector | ❌ | ❌ | ❌ |
| BybitConnector | ❌ | ❌ | ❌ |
| GateConnector | 待查 | 待查 | 待查 |
| CCXTExchangeAdapter | 通过 ccxt 格式化 | N/A | ❌ |

### 关键发现汇总
1. **最高风险**: OKX/Bybit 无连接锁（H1）+ 绕过 `_ensure_client`（H3），在多策略并发启动时可能崩溃。
2. **资金安全**: 精度对齐缺失（H2/H7）可能导致订单静默失败而持仓记录异常。
3. **性能瓶颈**: OKX 无余额缓存（M3）+ 多交易所串行 equity 刷新（M4）在生产环境最容易触发限速。
4. **`ccxt_adapter` 时间戳 bug（L8）**: 虽为低优先级，但影响所有使用 adapter 的时序记录，建议尽快修复。
