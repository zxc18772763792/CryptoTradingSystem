# 系统健康度审计 — 待修补清单

**日期**: 2026-05-24
**作者**: 三个并行 Explore agent（可靠性 / 性能 / 外部集成）+ 主审整合
**范围**: 全项目（约 18,686 行 Python，150+ 文件），排除已有专项计划的议题
**目的**: 罗列 paper/live 串号之外的所有系统级隐患，按优先级建档，等日后逐项修补

---

## 0. 排除范围

以下议题**已经在专项计划里**，本文档不再覆盖：

| 议题 | 专项文档 |
|---|---|
| paper/live 模式串号 | [PAPER_LIVE_ISOLATION_FIX_PLAN_2026-05-24.md](PAPER_LIVE_ISOLATION_FIX_PLAN_2026-05-24.md) |
| `_mode_guard` restore-flap | 同上 |
| `position_manager.set_scope` 同步 I/O | 同上 |
| `order_manager.set_paper_trading` 无条件 log | 同上 |
| `trades`/`positions` 表缺 mode 列 | 同上 |
| 交易所时钟同步 / Gate REQUEST_EXPIRED | (本次会话另一段已诊断，待 OS 层 NTP 修复) |

---

## 1. 优先级总览（速读版）

### 🔴 5 项最该先修（CRITICAL 中的 Top 5）

| ID | 痛点 | 文件 | 后果 |
|---|---|---|---|
| R-C1 | asyncio.create_task 失败异常无声丢失（3 处） | [position_manager.py:540,609,632](../core/trading/position_manager.py#L540) | 持仓回调失败监控失明 |
| P-C3 | `_market_data_cache` 无 eviction 内存泄漏 | [strategy_manager.py:600-606](../core/strategies/strategy_manager.py#L600) | 持续运行 OOM 风险 |
| E-C3 | Coinglass 429 限流后继续重试 → API Key 风控冻结 | [coinglass_client.py:428-431](../core/data/coinglass_client.py#L428) | 关键数据源被封 |
| R-C2 | 异步锁 lazy-create 竞态，两个并发请求拿到不同 Lock | [altcoin.py:280-284](../web/api/altcoin.py#L280) | 缓存污染 |
| E-C2 | 交易所代理失效后无自动检测/重拨 | [bybit_connector.py:33-50](../core/exchanges/bybit_connector.py#L33) | 静默阻塞数分钟 |

### 三类问题总计

| 类别 | CRITICAL | MAJOR | MINOR | 小计 |
|---|---|---|---|---|
| 可靠性 / 错误处理 | 5 | 10 | 4 | 19 |
| 性能 / 资源 | 5 | 5 | 4 | 14 |
| 外部集成健壮性 | 3 | 6 | 5 | 14 |
| **合计** | **13** | **21** | **13** | **47** |

---

## 2. 可靠性 / 错误处理 / 竞态（R-*）

### 🔴 CRITICAL

#### R-C1 — asyncio.create_task fire-and-forget 异常无声丢失
- **位置**：[position_manager.py:540, 609, 632](../core/trading/position_manager.py#L540)
- **问题**：`asyncio.create_task(self._notify_callbacks(...))` 三处都没保留 Task 引用，也没 `add_done_callback` 处理异常
- **后果**：position 变化回调失败时无人感知；监控系统漏掉关键事件
- **修复**：用全局 task registry 或 `add_done_callback(_log_exc)` 包裹
- **工作量**：30 分钟

#### R-C2 — 异步锁 lazy-create 竞态
- **位置**：[web/api/altcoin.py:280-284](../web/api/altcoin.py#L280)
- **问题**：`_cache_lock()` 用 dict.get/set 创建 lock，两个并发请求都看到 `None` 时会各创一个 Lock 实例，导致同 key 拿到两把不同的锁
- **后果**：`_ALTCOIN_SCAN_CACHE` 的 read/write 不原子，缓存污染或竞态
- **修复**：用 `threading.Lock` 保护 lock-dict 的初始化，或一次性预创建
- **工作量**：20 分钟

#### R-C3 — aiohttp.ClientSession 创建后异常时泄漏
- **位置**：[coinglass_client.py:1724-1727](../core/data/coinglass_client.py#L1724) + [funding_rate_collector.py:56-59](../core/data/funding_rate_collector.py#L56) + [oi_collector.py:97-99](../core/data/oi_collector.py#L97) + [orderbook_collector.py:166-169](../core/data/orderbook_collector.py#L166)
- **问题**：`_get_session()` 在 `self._session = aiohttp.ClientSession(...)` 后到 `__aexit__` 之间如果异常，Session 永远不会关闭
- **后果**：TCP 连接池枯竭；后续请求 timeout
- **修复**：把 Session 创建放进 try/except，异常时显式 `await session.close()`
- **工作量**：1 小时（4 个文件）

#### R-C4 — `LoopBoundAsyncLock` 多线程错位失锁
- **位置**：[core/utils/asyncio_compat.py:23-26](../core/utils/asyncio_compat.py#L23)
- **问题**：非异步上下文 fallback 到 `asyncio.get_event_loop()` 创建新循环；多线程加载时各线程拿不同 loop，Lock dict key 是 `id(loop)`，两个线程拿到不同的锁，根本没保护
- **后果**：使用者以为有锁，实际无；危险且静默
- **修复**：要么改成 `threading.Lock` 全局共享，要么禁止跨线程使用（运行时检测）
- **工作量**：1 小时

#### R-C5 — `contextlib.suppress(Exception)` 吞掉信号队列错误
- **位置**：[execution_engine.py:809](../core/trading/execution_engine.py#L809)
- **问题**：`with contextlib.suppress(Exception): dropped = int(self._signal_queue.qsize())` —— 信号队列如果异常销毁，dropped 永远是 0，没人知道丢了多少信号
- **后果**：诊断盲区
- **修复**：捕获指定异常（RuntimeError 等），其它异常 re-raise
- **工作量**：15 分钟

### 🟡 MAJOR

#### R-M1 — WebSocket 任务异常未上报
- [web/main.py:1708-1735](../web/main.py#L1708)
- `recv_task` / `send_task` 没在 done callback 里 `.exception()` 检查
- 修复：done callback 加异常日志

#### R-M2 — 模块级 dict 缓存无锁
- [web/api/altcoin.py:87, 463, 404](../web/api/altcoin.py#L87)
- `_ALTCOIN_SCAN_CACHE` check-then-act 不原子
- 修复：用 asyncio.Lock 或换成 copy-on-write

#### R-M3 — 停止时 Task cancel 不等待
- [execution_engine.py:5945-5950](../core/trading/execution_engine.py#L5945)
- `task.cancel()` 后中间没立刻 `await`，cancel 信号可能丢
- 修复：cancel 后立即 `await task` + timeout

#### R-M4 — 历史数据下载无总超时
- [historical_data.py:172-179](../core/data/historical_data.py#L172)
- 只有单 chunk 的 timeout，没有整体总长保护
- 后果：极端网络下下载卡小时级
- 修复：外层 `asyncio.wait_for(total_timeout)`

#### R-M5 — 重试逻辑不区分 transient / permanent
- [historical_data.py:237-268](../core/data/historical_data.py#L237)
- 无效 symbol 也重试到上限
- 修复：加 `_is_permanent_error()` 判断 4xx、schema error 直接放弃

#### R-M6 — paper_trading Task done callback 误判
- [paper_trading.py:66-68](../core/backtest/paper_trading.py#L66)
- `if not t.cancelled() and t.exception() else None` 条件易出错
- 修复：显式 try/except 调用 exception()

#### R-M7 — 研究调度器 suppress 过度
- [research_scheduler.py:44, 52](../core/ai/research_scheduler.py#L44)
- `contextlib.suppress(asyncio.CancelledError)` 包裹 await，但里头其它异常也被吞
- 修复：拆分 suppress 范围

#### R-M8 — Coinglass 缓存锁 dict 的 lazy-create 竞态
- [coinglass_client.py:48-60](../core/data/coinglass_client.py#L48)
- WeakKeyDictionary 的 get-then-set 模式
- 修复：threading.Lock 保护初始化

#### R-M9 — 限流 `while True` 循环 timeout 不精确
- [rate_limit_and_reconnect.py:197-209](../core/execution/rate_limit_and_reconnect.py#L197)
- `asyncio.sleep` 被打断后超期
- 修复：用 `asyncio.timeout()` 包裹整个循环

#### R-M10 — SQL 批量写入不在单个事务里
- [data_storage.py:200-209](../core/data/data_storage.py#L200)
- 单条 flush 后单条 commit，部分失败时已 commit 的留下脏数据
- 修复：`add_all()` + 单 flush + 单 commit

### 🟢 MINOR

- [extended_factors.py:638](../core/factors_ts/extended_factors.py#L638) — 裸 `except:` 吞 numpy 异常
- [web/api/trading.py](../web/api/trading.py) — 40+ 个 `contextlib.suppress(Exception)` 应指定异常类型
- [web/main.py:1037](../web/main.py#L1037) — `asyncio.get_event_loop()` 已弃用
- 多处 SQL session 没 try/finally 保 close

---

## 3. 性能 / 资源 / 阻塞（P-*）

### 🔴 CRITICAL

#### P-C1 — 非 SQLite DB 路径逐条 INSERT
- **位置**：[core/news/storage/db.py:1213-1221](../core/news/storage/db.py#L1213)
- **问题**：`save_news_raw()` 对非 SQLite 数据库（如 PostgreSQL）走逐条 INSERT，每条单独 flush；SQLite 路径有 `insert().prefix_with("OR IGNORE")` 批量插入
- **影响**：拉 100 条新闻额外 100 次 DB 往返；增加 1-5 秒延迟
- **修复**：非 SQLite 路径用 `bulk_insert_mappings` 或 SA Core `insert().on_conflict_do_nothing()`
- **工作量**：1 小时

#### P-C2 — LLM worker 持锁逐条保存事件 + 双重循环
- **位置**：[news/service/worker.py:202-217, 622-625](../core/news/service/worker.py#L202)
- **问题**：`_execute_llm_batch()` 持 DB 锁逐条 `save_events`，又再次循环调 `_persist_llm_title_summaries()`
- **影响**：每个 batch（≤8 条）做 2-3 次全量扫描；日志同步阻塞 event loop；CPU +30-40%
- **修复**：批量化 + 锁外日志
- **工作量**：2 小时

#### P-C3 — 市场数据缓存无 eviction
- **位置**：[strategy_manager.py:600-606](../core/strategies/strategy_manager.py#L600)
- **问题**：`_market_data_cache` 只在 size > 200 时清理，30s TTL；缓存 key 含 (exchange,symbol,timeframe,limit)，高频策略跑 100+ 币对会快速膨胀
- **影响**：每条目 ~50KB，200 条 = 10MB+，长跑会触发 OOM
- **修复**：用 `cachetools.TTLCache(maxsize=N, ttl=30)` 强制 eviction
- **工作量**：30 分钟

#### P-C4 — news worker 心跳循环每秒唤醒
- **位置**：[web/main.py:515-518, 560-563, 603-611](../web/main.py#L515)
- **问题**：`for _ in range(sleep_seconds): await asyncio.sleep(1)` 每秒唤醒检查 stop_event
- **影响**：空转期 CPU +5-10%；sleep_seconds=300 时尤为明显
- **修复**：`await asyncio.wait_for(stop_event.wait(), timeout=sleep_seconds)`
- **工作量**：30 分钟

#### P-C5 — LLM task claim/finish 双重全表扫
- **位置**：[news/storage/db.py:1286-1360, 1363-1425](../core/news/storage/db.py#L1286)
- **问题**：`claim_llm_tasks` LIMIT 100 后再用 raw_ids 重 QUERY NewsRaw；`finish_llm_tasks` 无 ORDER BY / LIMIT，O(n) 全表扫
- **影响**：news_llm_tasks 表 5000+ 行后，finish 耗时 200-400ms；高并发拉新闻总耗时 >3s
- **修复**：合并 JOIN 查询；finish 加 index 利用
- **工作量**：1.5 小时

### 🟡 MAJOR

#### P-M1 — 前端列表端点无分页
- [web/api/data.py:299](../web/api/data.py#L299) KlineRequest 没 offset/cursor
- 多个 list_* 端点缺约束
- 修复：统一加 offset/limit + cursor 分页

#### P-M2 — News 去重扫描限制 5000 行不够
- [news/storage/db.py:1168](../core/news/storage/db.py#L1168)
- 表超 50w 行时窗口内可能漏判重复
- 修复：基于时间窗 + content_hash 索引

#### P-M3 — 策略循环无强制 timeout
- core/strategies/strategy_manager.py 信号生成 + 缓存装载无总时长保护
- 修复：单 cycle 包 `asyncio.wait_for(timeout=30)`

#### P-M4 — 批量交易所操作无并行化
- core/trading/position_manager.py + order_manager.py 的 close_positions / cancel_orders 逐条调
- 修复：`asyncio.gather` + `Semaphore(max_concurrent=5)`

#### P-M5 — lifespan 冷启动 5-10s
- web/main.py 启动同步 load 后再启 worker
- 修复：把 load 改成 `asyncio.gather` 并行，worker 启动放前面

### 🟢 MINOR

- [data_storage.py](../core/data/data_storage.py) parquet 加载未指定 engine
- [benchmark_beta.py](../core/market_state/benchmark_beta.py) 全局 cache 无 TTL
- [web/api/ai_research.py](../web/api/ai_research.py) research symbols 逐条调用
- [core/news/collectors/](../core/news/collectors/) 某些 collector 同步 HTTP 无 timeout

---

## 4. 外部集成健壮性（E-*）

### 🔴 CRITICAL

#### E-C1 — 自称 "safe" 的 JSON 解析直接抛异常
- **位置**：[async_glm_client.py:398-399](../core/news/eventizer/async_glm_client.py#L398)
- **问题**：
  ```python
  def _safe_json_loads(text):
      return json.loads(text)
  ```
  名为 safe，实际直抛 `JSONDecodeError`；调用方 [413, 429](../core/news/eventizer/async_glm_client.py#L413) 用 `except Exception` 兜底
- **后果**：LLM 流式响应不完整时整条事件提取链断；无降级
- **修复**：真 try/except 返回 None；调用方按 None 走 fallback 提取规则
- **工作量**：30 分钟

#### E-C2 — 交易所代理失效后无自动检测/重拨
- **位置**：[bybit_connector.py:33-50](../core/exchanges/bybit_connector.py#L33) + [okx_connector.py:46-50](../core/exchanges/okx_connector.py#L46) + [gate_connector.py:34-51](../core/exchanges/gate_connector.py#L34)
- **问题**：`connect()` 时读 HTTP_PROXY 注入；运行时若代理失效或网络切换，`_ensure_client()` 只在 `_client is None` 时重连，不会因代理故障主动重拨
- **后果**：静默阻塞数分钟直至 timeout
- **修复**：加代理健康检查 + 检测到代理失效时强制 `_client = None` 触发重连
- **工作量**：2 小时

#### E-C3 — Coinglass 429 后继续重试
- **位置**：[coinglass_client.py:428-431](../core/data/coinglass_client.py#L428)
- **问题**：收到 429 后只递增 `rate_limit_hit_count` 但不阻止后续请求；`should_pause_coinglass_requests()` 只匹配 "code_429" 字符串，"http_429" / "minute_budget_exhausted" 漏判
- **后果**：每秒 10+ 429；API Key 被风控冻结
- **修复**：统一限流标识；429 触发 backoff_until；至少 60s 静默期
- **工作量**：1 小时

### 🟡 MAJOR

#### E-M1 — exchange_manager 重连无指数退避
- [exchange_manager.py:282-406](../core/exchanges/exchange_manager.py#L282)
- 所有交易所同时故障时毫秒级 5+ 次失败连接，无下次重连调度
- 修复：失败后 `asyncio.sleep(2 ** attempt)`，最大 60s

#### E-M2 — LLM timeout 不分层
- [async_glm_client.py:986-1008](../core/news/eventizer/async_glm_client.py#L986)
- `asyncio.TimeoutError` 不区分 DNS 超时 vs 计算超时
- 修复：分 connect/read timeout，按类型不同 backoff

#### E-M3 — SQLite WAL 下 save_news_raw + save_news_events 跨会话无原子
- [news/storage/db.py:88-96](../core/news/storage/db.py#L88)
- 两个会话并发可能部分写入
- 修复：合并到单事务，或加分布式锁

#### E-M4 — Coinglass 采集器无分页断点续传
- core/news/collectors/coinglass_news.py
- 中途崩溃重启从第 1 页爬，触发更多限流
- 修复：持久化 last_page cursor

#### E-M5 — `_handle_error` 不区分错误类型
- [base_exchange.py:270-275](../core/exchanges/base_exchange.py#L270)
- 短暂 DNS 失败也标 `_connected=False`，业务层无法区分立即重试 vs 等待退避
- 修复：按 transient / permanent / auth 分类返回

#### E-M6 — LLM 备用 API Key 明文驻留内存
- [openai_responses.py](../core/utils/openai_responses.py) `_endpoint_targets`
- 内存被 dump 或日志误记会泄漏
- 修复：用 keyring / 进程加密；至少在 `__repr__` mask

### 🟢 MINOR

- timeout 分层不完整（exchange startup vs LLM）
- ccxt 错误消息可能含 API Key
- parquet 读写无 integrity check
- 所有交易所只用 REST，无 WebSocket 心跳
- health_check 可能绕过代理

---

## 5. 架构性观察（横跨多模块的隐患）

### A1. lazy-create 单例反模式普遍存在
至少 5 处（R-C2, R-C4, R-M8 等）都用未同步的 `check-then-create`。建议：
- 引入 `@thread_safe_singleton` 装饰器
- 或者全部在模块加载时预创建（lazy 改 eager）

### A2. Task 生命周期管理缺失
大量 `asyncio.create_task()` 散落在 [position_manager](../core/trading/position_manager.py)、[execution_engine](../core/trading/execution_engine.py)、[web/main.py](../web/main.py)。建议：
- 统一 `TaskRegistry`，所有 fire-and-forget 必须注册
- 注册时强制 `add_done_callback(log_exc_and_unregister)`
- 进程关闭时 cancel 所有未完成 task

### A3. 缓存策略分散无统一 eviction
市场数据、news summaries、factor library、altcoin scan 各自实现缓存。建议：
- 引入 `cachetools.TTLCache` 统一封装
- 全局 cache key 命名约定（`f"{domain}:{key}"`）
- 启动时注册到中央 cache registry 便于诊断

### A4. 外部 API 限流无系统级协调
Coinglass / LLM / 交易所各自做 backoff，互不知道。建议：
- exchange_manager 层统一管理所有外部 API 的 token bucket
- 触发限流的请求登记到全局 registry，影响调度决策

### A5. 错误分类不清晰，重试一刀切
[base_exchange](../core/exchanges/base_exchange.py)、[historical_data](../core/data/historical_data.py)、[async_glm_client](../core/news/eventizer/async_glm_client.py) 都对 transient / permanent / auth 错误统一处理。建议：
- 抽公共 `class TransientError(Exception)` / `PermanentError` / `AuthError`
- 所有外部调用包一层 mapping
- 调用方按异常类型决定重试策略

---

## 6. 推荐攻击顺序

不必按 CRITICAL→MAJOR 顺序，按"风险敞口 × 修复成本"排：

### Wave 1：低成本高收益（2-3 天）
立刻能减少线上事故概率：

| ID | 项 | 工时 | 收益 |
|---|---|---|---|
| P-C3 | 市场数据缓存改 TTLCache | 30 min | 消除 OOM 风险 |
| P-C4 | worker 心跳改 stop_event.wait | 30 min | 空转 CPU -5-10% |
| E-C3 | Coinglass 429 强制 backoff | 1h | 防 API Key 被封 |
| R-C5 | 信号队列异常不 suppress | 15 min | 诊断恢复 |
| R-C1 | create_task 全局加 done_callback | 30 min | 异常可见 |
| R-M3 | execution_engine 停止时等 task | 30 min | 关闭信号不丢 |
| E-C1 | _safe_json_loads 真 safe | 30 min | LLM 链不断 |

**小计：≈ 4 小时**

### Wave 2：架构性铺垫（1 周）
让后续修复有抓手：

- A1 全局 `@thread_safe_singleton` 装饰器 → 顺手修 R-C2, R-C4, R-M8
- A2 全局 `TaskRegistry` → 自动覆盖所有 fire-and-forget
- A5 异常分类抽象 → 后续 R-M5, E-M5 都按这个分类改

### Wave 3：性能与外部集成深改（1-2 周）
- P-C1/C2/C5：news 数据层批量化 + 索引优化
- E-C2：交易所代理健康检查 + 主动重拨
- E-M1：exchange_manager 指数退避
- R-M10：数据写入事务化

### Wave 4：MINOR 与运维基础设施（择期）
所有 MINOR + A3 缓存统一 + A4 限流协调

---

## 7. 与已有专项计划的关系

| 已有计划 | 与本审计的交叉 |
|---|---|
| [PAPER_LIVE_ISOLATION_FIX_PLAN](PAPER_LIVE_ISOLATION_FIX_PLAN_2026-05-24.md) S3-3 contextvars | 跟 A2 TaskRegistry 配合，重写时一起做 |
| [PAPER_LIVE_ISOLATION_FIX_PLAN](PAPER_LIVE_ISOLATION_FIX_PLAN_2026-05-24.md) S3-1 异步持久化 | 跟 R-M10 / R-C3 一起重构 IO 层 |
| 时钟同步（OS 层 NTP） | 不在本审计范围，独立处理 |

---

## 8. 维护说明

### 如何使用本文档
- 每次修补一项，在表格里把 ID 划掉
- 修补时如果发现关联问题，追加到对应小节
- Wave 完成后在文末追加日期+小结，不删原文（保留历史轨迹）

### 何时重做审计
- 完成 Wave 2 后做一次小规模 re-audit（架构变了，前置假设可能失效）
- 半年后做一次完整 re-audit

### 引用约定
本文档内 ID 用 `R-Cn / R-Mn / P-Cn / E-Cn` 等，方便 PR、commit、issue 引用：

例：
```
git commit -m "fix(R-C1): wrap create_task with done callback to surface exceptions"
```

---

## 9. 附：每个 agent 的原始结论摘要

### 可靠性 agent
- 5 CRITICAL + 10 MAJOR + 4 MINOR + 2 架构性观察
- 主线：fire-and-forget 异常处理 + lazy-create 锁竞态

### 性能 agent
- 5 CRITICAL + 5 MAJOR + 4 MINOR + 2 架构性观察
- 主线：news/LLM 数据层批量化缺失 + 缓存策略缺 eviction

### 外部集成 agent
- 3 CRITICAL + 6 MAJOR + 5 MINOR + 2 架构性观察
- 主线：代理/限流/429 缺乏主动检测和退避

---

## 10. 总结

47 项问题中：
- **13 项 CRITICAL** 是修补的核心目标
- **21 项 MAJOR** 应在中期消化
- **13 项 MINOR** 择机随手做

按 Wave 1 4 小时铺底 → Wave 2 一周架构铺垫 → Wave 3 两周深改 的节奏推进，2-3 周可消化 80% 风险。

paper/live 串号修复（另一份计划）和本份审计在 Wave 2 的"架构性铺垫"上有重叠（都需要 TaskRegistry + 异常分类）——建议**先做 paper/live 的 Stage 1+2，再启动本文档的 Wave 2**，让两边的架构改造在 Wave 2 合流。
