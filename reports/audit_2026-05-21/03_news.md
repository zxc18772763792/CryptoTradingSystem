# 新闻管道审计报告 (2026-05-21)

审计范围：`core/news/`（全子树）、`core/data/news_collector.py`

---

## 高优先级 (Bug, 必修)

### H1. `_event_processor_loop` 配置静态固化，热更新无效
**位置**: `core/news/service/worker.py:501`

```python
cfg = load_service_config()   # 调用一次，之后永远不刷新
```
**问题**: `_event_processor_loop` 在首次启动时加载一次 `cfg`（含 LLM base_url/key/timeout_sec），之后无限循环复用。若运维人员在运行时修改 `news_rules.yaml` 或环境变量，工作进程不重启则新配置永不生效。同时 `worker_loop` 每次迭代调用 `process_llm_batch(cfg, ...)` 传入的也是启动时快照。  
**影响**: 线上修改 LLM key/模型后必须重启进程，否则继续用旧 key，导致 401/403。  
**建议**: 让 `_event_processor_loop` 在每批次前调用 `load_service_config()` 重新读取（yaml 有文件系统缓存，开销可忽略），或至少每 5 分钟刷新一次。

---

### H2. `_event_processor_running` 标志竞态——双重处理器
**位置**: `core/news/service/worker.py:487-494`

```python
async def _ensure_event_processor() -> None:
    global _event_processor_running
    if _event_processor_running:
        return
    _event_processor_running = True       # (A)
    asyncio.create_task(_event_processor_loop())  # (B)
```
**问题**: Python asyncio 虽是单线程，但 `_event_processor_running = True` 在 `create_task` 之前被设置（A→B）。如果 `_event_processor_loop` 崩溃退出（在 `finally` 中将其设为 `False`），下一次调用 `_ensure_event_processor` 会正确重新启动。但若 `worker_loop` 同时也调用 `_ensure_event_processor()`（worker_loop:641），以及 `pull_source_once` 通过 `on_news_inserted` 异步触发（worker.py:586），在极端情况下（loop 重启瞬间）可能同帧内两次进入"未运行"状态，导致两个并发处理器同时抢夺队列任务，同一批新闻被双重 LLM 提取。  
**影响**: 重复事件插入（`save_events` 的语义去重会拦截大部分，但有窗口期），以及额外 LLM API 成本。  
**建议**: 改用 `asyncio.Event` 或持有 `asyncio.Task` 对象，检查 `task.done()` 而不是布尔标志。

---

### H3. `on_news_inserted` 接收原始 DB 字典，但处理路径期望 `news_llm_tasks` 格式
**位置**: `core/news/service/worker.py:463-484` 和 `_process_event_batch:546-555`

`on_news_inserted(news_items)` 接收的是 `save_news_raw` 返回的 `inserted` 列表（字段：`id, source, title, url, content, published_at, ...`，**没有** `payload.importance_score`）。  
`_process_event_batch` 直接调用 `_process_llm_task_batches(batch, cfg)`，后者会调用 `_execute_llm_batch`，读取 `batch[i].get("id")` 作为 raw_id —— 这个字段存在。但 `claim_llm_tasks` 返回的字典含 `llm_task` 子键，而事件驱动路径传入的字典没有，因此 `finish_llm_tasks(raw_ids, ...)` 中的 `raw_ids = [int(item.get("id")) ...]` 可以正常工作。然而 `enqueue_llm_tasks` 仍然**单独**将这些 item 写入 `news_llm_tasks` 表（worker.py:582），事件驱动路径绕过了任务表直接提取，导致任务表中相同 raw_id 的条目永远停留在 `pending` 状态。  
**影响**: `claim_llm_tasks` 周期性轮询会重复处理这些 `pending` 任务（20s 间隔），对同一新闻发起第二次 LLM 请求，浪费 token 和时间。  
**建议**: `on_news_inserted` 在将 item 排队到 `_llm_event_queue` 之前，先调用 `finish_llm_tasks` 将这些 raw_id 的任务标记为 `done`（或在 `_process_event_batch` 返回后标记）；或者让事件驱动路径完全不绕过 `claim_llm_tasks`（改为仅加快轮询周期）。

---

### H4. `save_news_raw` 的 non-SQLite 路径：IntegrityError 后 `rollback` 会撤销整个 session
**位置**: `core/news/storage/db.py:1170-1178`

```python
for item in rows_to_insert:
    obj = NewsRaw(**item)
    session.add(obj)
    try:
        await session.flush()
        objects.append(obj)
    except IntegrityError:
        await session.rollback()   # ← 撤销当前 session 中所有已 flush 的行
        deduped_count += 1
```
**问题**: SQLAlchemy AsyncSession 在 `rollback()` 之后 session 进入关闭状态，之后对 `session.add(...)` 的调用会抛出 `InvalidRequestError: Session is closed`，导致后续所有 item 全部丢失。SQLite 路径使用 `INSERT OR IGNORE` 批量插入，不存在此问题。  
**影响**: PostgreSQL/MySQL 部署时（目前系统以 SQLite 为主，但 `_NEWS_DATABASE_URL` 可配置为 PG），多条新闻中若有一条重复，后续所有新闻都丢失。  
**建议**: 改为 `await session.rollback()` 后 `session.expunge_all()` 并重建状态；或针对 non-SQLite 也使用 `INSERT ... ON CONFLICT DO NOTHING`（SQLAlchemy 2.0 支持）。

---

### H5. `_worker_cfg` 强制截断 LLM timeout，与 YAML 配置冲突
**位置**: `core/news/service/worker.py:378-383`

```python
worker_timeout = max(8, _env_int("NEWS_LLM_WORKER_TIMEOUT_SEC", 16))
...
llm_cfg["timeout_sec"] = min(current_timeout, worker_timeout)   # 取较小值
```
**问题**: YAML 中配置的 `timeout_sec: 90`（per 2026-03-03 fix）会被 `worker_timeout=16` 覆盖截断为 16 秒，导致 LLM 提取超时过快。实际上 `NEWS_LLM_WORKER_TIMEOUT_SEC` 默认 16，而 YAML 中已经设置 90——`min(90, 16) = 16` 使 YAML 配置完全失效。  
**影响**: LLM 请求 timeout 实际上只有 16 秒，与已修复的 90s 配置相悖，导致频繁超时、重试、task 状态混乱。  
**建议**: 将 `NEWS_LLM_WORKER_TIMEOUT_SEC` 默认值改为 90，或使用 `max` 而不是 `min` 让配置文件值优先（worker 应该是 consumer，应遵从 cfg 设定）。

---

### H6. `save_events` 语义去重：`impact_score` 归一化错误
**位置**: `core/news/storage/db.py:1415`

```python
impact_diff = abs(new_impact - prev_impact) / max(abs(prev_impact), 0.01)
```
**问题**: 分母用 `prev_impact` 而非两者均值，当 `prev_impact = 0.01`（极小值）、`new_impact = 0.5` 时，`impact_diff = 49 >> 0.05`，可以插入。但当 `prev_impact = 0.5`、`new_impact = 0.01` 时，`impact_diff = 0.98 >> 0.05`，也可以插入。这与设计意图（"5% 阈值"）一致。  
然而 `impact_score` 字段值域为 `[0, 1]`（`EventSchema` 模型约束），当两者都接近 0（如 `0.001` 和 `0.002`）时，分母取 `0.01`（而非 `prev_impact=0.001`），结果 `impact_diff = 0.1 > 0.05`，本应去重却被插入。  
**影响**: 低影响事件（`impact_score < 0.01`）的重复不被正确去重，可能产生大量噪声事件。  
**建议**: 改为 `impact_diff = abs(new_impact - prev_impact) / max(max(abs(prev_impact), abs(new_impact)), 1e-6)`。

---

### H7. `core/data/news_collector.py` 的 `fetch_rss` 使用 `feedparser`（同步阻塞）在 async 上下文中调用
**位置**: `core/data/news_collector.py:99`

```python
feed = feedparser.parse(url)   # 同步网络阻塞调用
```
**问题**: `fetch_rss` 是 `async def`，但 `feedparser.parse(url)` 发起同步 HTTP 请求，会阻塞 asyncio 事件循环。`collect_all_news` 用 `asyncio.gather(*tasks)` 并发调用所有 RSS 源，但 gather 内每个协程实际都同步阻塞，无法真正并发。  
**影响**: 每个 RSS 源串行等待（即使并发调度），总采集时间 = 所有源 timeout 之和（最坏 8 源 × 20s = 160s），阻塞 web 服务器进程。  
**建议**: 改用 `await asyncio.to_thread(feedparser.parse, url)` 或换成 `aiohttp` + 手动 RSS 解析（`core/news/collectors/rss.py` 已有 aiohttp 版本，建议直接复用）。

---

### H8. `core/data/news_collector.py` 使用 naive datetime（缺少时区）
**位置**: `core/data/news_collector.py:105-109, 110, 173`

```python
published_at = datetime(*entry.published_parsed[:6])   # naive，无时区
...
published_at = datetime.now()                           # naive
```
**问题**: 产生的 `NewsItem.published_at` 是 naive datetime，后续代码若与 UTC-aware datetime 比较会抛出 `TypeError: can't compare offset-naive and offset-aware datetimes`（已在 strategy_manager 中修复过同类问题，此处尚未修复）。  
**影响**: `collect_all_news` 的时间排序（`all_news.sort(key=lambda x: x.published_at)`) 若混有 aware datetime 会崩溃；`fetch_cryptopanic` 的 `datetime.fromisoformat(...replace('Z', '+00:00'))` 是 aware，与 naive 混用时排序崩溃。  
**建议**: 统一改为 `datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)` 和 `datetime.now(timezone.utc)`。

---

### H9. `llm_glm5.py` 同步 `requests.post` 在 async worker 路径中调用
**位置**: `core/news/eventizer/llm_glm5.py:429,475,518,531,600`

`_openai_post_with_failover` 使用同步 `requests.post(...)` 发起 HTTP 调用，被 `extract_events_llm_with_meta` 调用，该函数又被 `extract_events_async_with_meta`（`async_glm_client.py`）在 `asyncio.to_thread` 中调用。

虽然 `async_glm_client.py` 用 `asyncio.to_thread` 包装了这个调用（见 `async_glm_client.py` 中的调用方式），这表明设计者已意识到这一问题。但 `_process_event_batch`（worker.py:546）和 `_process_llm_task_batches`（worker.py:256）直接调用 `extract_events_async_with_meta`（来自 async_glm_client），该函数是 async 的，应该没问题。需要确认 worker 调用链不直接调用 `llm_glm5.extract_events_llm_with_meta`（同步版本）。  
**建议**: 在 `extract_events_llm_with_meta` 上加文档注释，明确标记为"阻塞同步函数，必须从线程调用"，防止未来被误用于 async 上下文。

---

### H10. `_event_processor_loop` 在 `on_news_inserted` 触发后不检查 provider backoff
**位置**: `core/news/service/worker.py:546-555`

```python
async def _process_event_batch(batch, cfg):
    result = await _process_llm_task_batches(batch, cfg)
```
**问题**: `_process_llm_task_batches` 不检查 provider backoff，只有 `process_llm_batch`（轮询路径）才检查。事件驱动路径绕过了 backoff 检查，导致在 provider 处于 backoff 期间仍然向其发送 LLM 请求。  
**影响**: rate-limit 期间仍然触发请求，加重 429 问题，可能加速 provider 封禁。  
**建议**: 在 `_process_event_batch` 中先调用 `process_llm_batch` 的 backoff 检查逻辑，或提取公共函数。

---

## 中优先级 (性能 / 并发)

### M1. `MultiSourceNewsCollector.pull_latest` 在 `ThreadPoolExecutor` 中调用同步 collectors
**位置**: `core/news/collectors/manager.py:298-327`

`pull_latest`（同步版本）使用 `ThreadPoolExecutor` 并发拉取所有 sources，workers 上限 6。若所有 6 个 source 都在等待网络响应，则第 7+ 个 source 排队等待，总耗时由最慢 source 决定。  
**问题**: `pull_latest_incremental`（async 版本，实际由 worker 使用）中调用了 `asyncio.to_thread` 包装同步 collector 的 `pull_incremental`——这是正确的。但 `_run_incremental_source_pull` 对于没有 `pull_incremental` 方法的 collector（fallback 到 `pull_latest`）也正确使用 `to_thread`。无主要性能问题，但注意并发上限为 6 个线程（`NEWS_PULL_WORKERS` 可配置）。  
**影响**: 可接受，建议将默认 `NEWS_PULL_WORKERS` 从 6 增加到 10（sources 最多 15 个）。

---

### M2. `common.py` 的 `_respect_rate_limit` 使用 `time.sleep`（阻塞）
**位置**: `core/news/collectors/common.py:173`

```python
time.sleep(wait)       # 阻塞
time.sleep(min(...))   # 重试退避，也阻塞
```
**问题**: `SyncCollectorBase._request` 在线程池中被调用，`time.sleep` 阻塞的是工作线程，不是事件循环，因此不会直接阻塞 asyncio。但若 `pull_latest`（同步路径）从主线程调用，则阻塞主线程。  
**影响**: `pull_latest` 同步路径（如 CLI、ops API 直接调用）可能有 3-10s 额外等待。async incremental 路径无问题。  
**建议**: 文档注明 `SyncCollectorBase` 不应在主线程直接调用，添加警告日志。

---

### M3. `summarize_news_raw_coverage` 扫描最新 1000 行 payload 做 Python 统计
**位置**: `core/news/storage/db.py:1578-1584`

```python
sample_rows = (await session.execute(
    select(NewsRaw.payload).order_by(NewsRaw.fetched_at.desc(), NewsRaw.id.desc()).limit(1000)
)).all()
```
**问题**: 读取 1000 行 payload（JSON 列）到 Python 做 `provider_counts` 统计。payload 字段可能很大（含 content/evidence），不必要的大数据传输。  
**影响**: 在大量历史数据的 SQLite 上，此查询耗时可达 100-500ms。  
**建议**: 改为 SQL `GROUP BY json_extract(payload, '$.provider')`（SQLite 3.38+ 支持），或缩减 limit 到 200。

---

### M4. `_summary_cache` 存在于两处，互相独立
**位置**: `core/news/eventizer/llm_glm5.py:70-71` 和 `core/news/eventizer/async_glm_client.py:1674-1675`

`llm_glm5.py` 有模块级 `_SUMMARY_CACHE`（4000 条），`async_glm_client.py` 的 `AsyncGLMClient` 实例有 `self._summary_cache`（4000 条/实例）。两者 key 生成方式不同：前者含 `_summary_cache_scope(cfg)`（包含 base_url|model），后者只含 title+max_length。  
**影响**: 同一标题可能被 LLM 重复处理（两次均未命中各自 cache），浪费 API 调用。  
**建议**: 统一使用一个共享 cache，或明确分工（llm_glm5 处理同步路径，async_glm_client 处理 async 路径，两者不互相调用时可以各自持有 cache）。目前不明确哪个路径是实际主路径，需要确认。

---

### M5. `save_news_raw` 中三条独立 `SELECT` 语句用于去重检查
**位置**: `core/news/storage/db.py:1131-1142`

```python
existing_url_rows = await session.execute(select(NewsRaw.url).where(NewsRaw.url.in_(urls)))
existing_hash_rows = await session.execute(select(NewsRaw.content_hash).where(NewsRaw.content_hash.in_(hashes)))
existing_recent_rows = await session.execute(select(NewsRaw.title, ...).where(date_range_filter))
```
**问题**: 三条独立 `SELECT` 会导致三次数据库往返。在 SQLite WAL 模式下，并发读是安全的，但三次往返产生三个独立事务读快照，若这三次查询之间有另一个 writer 并发插入，可能导致 `title_bucket` 一致性错误（理论上，实际 SQLite WAL 并发有保护）。  
**影响**: 轻微性能损耗（3 次 DB round-trip → 可以 1 次）；极低概率的 TOCTOU 竞态。  
**建议**: 三条查询可合并为一次 `WITH` CTE 查询（PostgreSQL）；SQLite 下可通过嵌套 SELECT 或在同一事务快照内执行（已在同一 session 内，基本安全）。

---

### M6. `claim_llm_tasks` 同时做 reclaim stale 和 claim new，无分离事务保证
**位置**: `core/news/storage/db.py:1229-1297`

`claim_llm_tasks` 在一个 `async with news_session_scope() as session` 中先 reclaim stale running tasks，再查询新的 pending/retry tasks 并设为 running。这两个操作共享同一个 SQLAlchemy Session/事务。  
**问题**: `flush()` 不是 `commit()`，若后续代码（如查询 news_raw 关联数据）抛异常，`session_scope` 的 `except/rollback` 会回滚整个操作，导致 stale tasks 既没有被 reclaim 也没有新任务被 claim。  
**影响**: 若 `news_raw` 表查询异常，worker 循环永远无法进展（stale running tasks 累积，新任务被 reclaim 阻塞）。  
**建议**: 将 reclaim stale 和 claim new 分为两个独立 session。

---

### M7. `rules.py` 的 `extract_events_rules` 在规则匹配时 `max_symbols` 字段未使用全局 8 上限
**位置**: `core/news/eventizer/rules.py:188`

```python
symbols = symbols[: max(1, int(rule.get("max_symbols") or 3))]
```
**问题**: 规则内 `max_symbols` 默认 3（非全局上限 8），且每条规则独立控制上限。若某条 rule 设置 `max_symbols: 20`，会超过系统意图的 8 上限。`SymbolMapper.extract_symbols_from_text(limit=8)` 已有 8 上限，但 `rule_symbols` 路径跳过了这个 limit，直接使用 `rule.get("max_symbols") or 3`。  
**影响**: 少量 rule 配置错误时可能产生过多 symbol 绑定事件，增加下游噪声。  
**建议**: `symbols = symbols[: max(1, min(8, int(rule.get("max_symbols") or 3)))]`。

---

## 低优先级 (死代码 / 一致性)

### L1. `core/data/news_collector.py` 与 `core/news/` 管道完全平行，未集成
**位置**: `core/data/news_collector.py`（全文）

`NewsCollector` 类有独立的 RSS 采集逻辑（8个源）、独立的 `analyze_sentiment`、独立的 `feedparser`+`aiohttp` 混用路径。这套逻辑与 `core/news/` 管道（`MultiSourceNewsCollector` + DB + LLM）完全平行，没有任何数据共享。  
**问题**: 两套情感分析逻辑（`core/data/news_collector.py` 的关键词比分 vs `core/news/eventizer/llm_glm5.py` 的 LLM + `_heuristic_sentiment_from_title`）；两套 RSS 采集器（`core/data/news_collector.py` 和 `core/news/collectors/rss.py`）；两套去重逻辑（标题前 50 字符 vs 30 分钟桶哈希）。  
**影响**: 双倍维护成本；`core/data/news_collector.py` 的结果不被写入 `news_raw` 表，策略信号无法消费这些新闻。  
**建议**: 将 `NewsCollector.collect_all_news` 的调用方迁移到 `core/news/` 管道，或将 `core/data/news_collector.py` 标注为"演示/legacy，不用于生产"。

---

### L2. `llm_glm5.py` 与 `async_glm_client.py` 大量代码重复
**位置**: 两个文件共享几乎相同的：
- `_POS_SENTIMENT_HINTS` / `_NEG_SENTIMENT_HINTS` 集合（`llm_glm5.py:72-82`, `async_glm_client.py:73-84`）
- `_openai_endpoint_targets` 逻辑
- `_build_prompt` 函数
- `_validate_events` 函数
- `_RUNTIME_SETTING_NAMES` 元组
- `_summary_cache` 实现

**影响**: 修改一处时需同步另一处（已经出现过 `ts[:16]→ts[:19]` 只改了一处的 bug），测试覆盖困难。  
**建议**: 提取共享逻辑到 `core/news/eventizer/llm_base.py`，两个文件 import 使用。

---

### L3. `_llm_provider` 函数永远返回 `"openai"`
**位置**: `core/news/eventizer/llm_glm5.py:237-242`

```python
def _llm_provider(cfg):
    raw = ...
    if raw in {"openai", "codex", "responses"} or raw in _LEGACY_PROVIDER_ALIASES:
        return "openai"
    return "openai"   # ← 所有分支都返回 "openai"
```
**影响**: 函数无效，占用代码空间，可能误导阅读者认为有多 provider 路由逻辑。  
**建议**: 删除函数或保留并添加注释说明仅支持 OpenAI 兼容 API。

---

### L4. `core/news/service/llm_worker.py` 功能退化为仅 4 行的 shim
**位置**: `core/news/service/llm_worker.py`

整个文件仅是 `from core.news.service.worker import main_async` 的薄封装，且 `--sources` 参数注释为 "Ignored for llm-only mode"。  
**影响**: 用户可能误用此 entrypoint，认为 `--sources` 有效。  
**建议**: 删除该文件，统一使用 `python -m core.news.service.worker --llm-only`；或更新 argparse 移除 `--sources` 参数。

---

### L5. `core/data/news_collector.py:105` `datetime(*entry.published_parsed[:6])` 忽略亚秒+时区
**位置**: `core/data/news_collector.py:105`

`published_parsed` 返回 9 元素 time 结构，`[:6]` 只取年月日时分秒，忽略时区（`published_parsed` 已含 UTC offset，解析后应再设 tzinfo）。  
**建议**: 改为 `datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)`（RSS entry 的 `published_parsed` 已是 UTC 时间戳）。

---

### L6. `_build_prompt` 中 `allowed_symbols[:120]` 硬编码上限可能截断关键符号
**位置**: `core/news/eventizer/llm_glm5.py:963`

```python
"allowed_symbols": allowed_symbols[:120],
```
**问题**: 若 symbols.yaml 配置了超过 120 个符号，LLM prompt 会截断。对于小型系统影响有限，但在 symbols 超过 120 时，超出的 symbol 的新闻事件无法被提取。  
**影响**: 低（当前系统配置的 symbol 数量可能未超限）。  
**建议**: 将 120 改为可配置常量，或在日志中提示截断。

---

### L7. `save_events` 中 `existing_recent_rows` 查询每次扫描 ±12 小时窗口，无索引保证
**位置**: `core/news/storage/db.py:1393-1401`

```python
existing_recent_rows = await session.execute(
    select(NewsEvent.symbol, ...).where(and_(NewsEvent.ts >= min_ts, NewsEvent.ts <= max_ts))
)
```
**问题**: 若 `news_events` 表上 `ts` 列没有索引，此全表范围扫描随数据增长变慢（events 表已有 668 条，随时间增长可达万条）。  
**建议**: 确认 `NewsEvent` 模型定义中 `ts` 列有 `index=True`（在 `core/news/storage/models.py` 中验证）。

---

### L8. `_bootstrap_news_sqlite_from_legacy` 中 SQL 注入风险（低）
**位置**: `core/news/storage/db.py:484`

```python
conn.execute(f"ATTACH DATABASE '{escaped_legacy}' AS legacy")
```
`escaped_legacy` 仅做了 `replace("'", "''")` 处理，但路径中含有 Windows 路径分隔符 `\`，可能在某些 SQLite 实现中导致意外行为。实际上 `legacy_resolved.as_posix()` 已经转为正斜杠，因此 `\` 不是问题。但对于特殊字符路径仍有潜在风险。  
**影响**: 极低（仅在服务启动时执行一次，路径来自配置文件）。  
**建议**: 使用参数化绑定或 `Path` 对象确保安全。

---

## 备注

### 已验证正确的项目（与先前修复一致）

1. **`save_events` 30分钟去重**: `_event_semantic_key` 使用 `ts[:19]`（秒精度）+ 标题哈希桶，`impact_diff <= 0.05 AND sentiment == prev_sentiment` 才跳过 —— 逻辑验证正确。
2. **`get_llm_queue_stats` GROUP BY**: 已使用 `GROUP BY status` SQL 聚合，无全表扫描 —— 已修复。
3. **per-provider backoff**: `set_provider_backoff`/`get_provider_backoff` 在 `db.py:1717-1732` 实现正确，含自动过期清除。
4. **`save_news_raw` OR IGNORE**: SQLite 路径使用 `INSERT OR IGNORE`，正确处理重复。
5. **RSS 错误日志**: `rss.py:177` 使用 `logger.warning(f"RSS feed error for {url}: ...")` —— 已修复。
6. **symbol 提取上限 8**: `SymbolMapper.extract_symbols_from_text(limit=8)` —— 已修复。
7. **情感位置权重**: `core/data/news_collector.py:211-214` 后半段 1.5x 权重 —— 已实现。

### 架构注意

- **事件驱动路径与轮询路径并行**: `_event_processor_loop`（事件驱动，每批 2s 等待）和 `process_llm_batch`（轮询，20s 间隔）同时运行，可能对同一批 raw 新闻发起双重 LLM 请求（见 H3）。
- **`news_collector.py` 生产可用性**: 目前 `NewsCollector` 不写入数据库，仅用于 `main()` 中的 CLI 测试，不应被任何生产代码路径调用。
- **`core/news/service/api.py`**: 未在本次审计中完整阅读，但其使用 `min_importance=35` 的硬编码默认值（`api.py:79`），与 worker.py 的可配置 `NEWS_LLM_MIN_IMPORTANCE` 不一致，建议统一为 `_min_importance()` 调用。
