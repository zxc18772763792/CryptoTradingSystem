# AI 研究与监控审计报告 (2026-05-21)

审计范围：`core/research/orchestrator.py`, `core/research/experiment_registry.py`,
`core/ai/research_planner.py`, `core/ai/research_context_generator.py`,
`core/ai/promotion_narrator.py`, `core/monitoring/cusum_watcher.py`,
`core/monitoring/strategy_monitor.py`, `web/api/ai_research.py`, `web/main.py`

---

## 高优先级 (Bug, 必修)

### B-1 `_JsonRegistry._load()` 持有锁外读缓存，竞态窗口下可能返回脏数据
**文件**: `core/research/experiment_registry.py:31-44`  
**问题**: `_load()` 先检查 `self._cache is not None` 再直接返回，但调用点 `save()`、`get()`、`list()` 都在锁内调用 `_load()`。然而 `list()` 方法（line 90）在锁外调用 `self._load()`（未加锁），`_flush()` 也在锁外调用 `self.list(limit=None)` 生成序列化行。若另一线程在 `_flush()` 执行 `list()` 期间向 `_cache` 写入新项，序列化结果可能不一致。  
**影响**: 在高并发下（多个 API 请求同时触发 save），候选/提案记录可能被截断或不完整写入 `.json` 文件。  
**修复建议**: 将 `_flush()` 中的 `self.list(limit=None)` 改为直接访问已锁内的 `self._cache`：
```python
def _flush(self) -> None:
    # 调用时已持有 self._lock，直接用缓存
    rows = [item.model_dump(mode="json") for item in (self._cache or {}).values()]
    ...
```
同时 `list()` 方法应在锁内调用（或其调用者保证持锁）。

### B-2 `_JsonRegistry._load()` 无锁首次加载竞态
**文件**: `core/research/experiment_registry.py:63-65`  
**问题**: `get()` 在锁内调用 `_load()`，但 `_load()` 本身不是原子的。首次加载时，若两个线程同时到达 `if self._cache is not None` 检查，两者都看到 `None`，都读文件，都赋值 `self._cache`，最后一个赋值胜出（正确），但可能浪费重复 I/O，更严重的是若文件在两次读之间被另一线程的 `_flush()` 更新，则两次读到不同内容，造成缓存不一致。  
**影响**: 轻微，但可能导致刚写入的候选在下次读时消失。  
**修复建议**: 在 `_load()` 首行也持锁（或使用 `threading.RLock` 替换 `Lock` 使 `_load` 可重入）。

### B-3 `os.replace()` 在 Windows 上目标被其他进程持有时会抛 PermissionError
**文件**: `core/research/experiment_registry.py:54`  
**问题**: 注释称 `os.replace` 在 Windows 上是原子的——这**不完全正确**。在 Windows 上，如果目标文件（`.json`）被另一进程打开（例如 UI 读取），`os.replace` 会抛 `PermissionError: [WinError 5]`，不像 POSIX 那样直接替换。  
**影响**: 当 web UI 频繁读取 `candidates.json` 时，`save()` 可能静默失败（异常被上层 `try/except` 吞掉），造成候选状态丢失。  
**修复建议**: 捕获 `PermissionError` 并重试：
```python
import time
for attempt in range(3):
    try:
        os.replace(str(tmp_path), str(self.path))
        break
    except PermissionError:
        if attempt == 2:
            raise
        time.sleep(0.05)
```

### B-4 `strategy_monitor.py` 的 IQR 计算逻辑完全失效——永远走 `except` 分支
**文件**: `core/monitoring/strategy_monitor.py:83-91`  
**问题**: `detect_strategy_decay()` 尝试用 `scipy.stats.iqr` 提供鲁棒 std 估计，但实现有两个严重错误：  
1. `_iqr(returns, rng=(75, 25))` 与 `_iqr(returns, rng=(25, 25))` 两次调用都没有实际使用结果（`q75, q25` 均为 IQR 值，不是分位数），无法计算 std。  
2. 随后无条件 `raise ValueError("use std")` 强制进入 except 分支——scipy IQR 路径**永远不会被使用**。  
**影响**: 代码意图提供比 sample std 更鲁棒的估计，实际上永远退化为 sample std。CUSUM 在 fat-tail 回报序列上会产生过高的触发阈值，漏报真实衰减。  
**修复建议**: 删除 scipy 分支或修复为：
```python
try:
    from scipy.stats import median_abs_deviation
    std_r = median_abs_deviation(returns, scale="normal")
    if std_r < 1e-12:
        raise ValueError("mad too small")
except Exception:
    mean_r = sum(returns) / n
    var_r = sum((r - mean_r) ** 2 for r in returns) / max(n - 1, 1)
    std_r = math.sqrt(max(var_r, 1e-12))
```

### B-5 `CUSUMMonitor.update()` 每次调用重算全历史 std——O(n) 时间复杂度无界增长
**文件**: `core/monitoring/strategy_monitor.py:180-182`  
**问题**: `update()` 每次新 bar 到来都用 `sum(self._returns) / n` 和 `sum((r-mean_r)**2 ...)` 遍历全部历史，时间复杂度 O(n)。长期运行（> 10k bars）时性能下降明显，而 CUSUM watcher 每 5 分钟扫描所有候选，对于每个候选调用一次 `detect_strategy_decay()`（非 `CUSUMMonitor`，这里没问题），但若有代码路径用到 `CUSUMMonitor` 的增量更新，会有性能问题。  
**影响**: 中等，目前 CUSUM watcher 用 `detect_strategy_decay`（批量），不用 `CUSUMMonitor.update()`，但 `CUSUMMonitor` 暴露给用户文档，实际被调用时 O(n) 无界。  
**修复建议**: 使用 Welford 在线算法维护运行均值和方差：
```python
# 在 __post_init__ 初始化
self._n: int = 0
self._mean: float = 0.0
self._M2: float = 0.0

# 在 update():
self._n += 1
delta = bar_return - self._mean
self._mean += delta / self._n
self._M2 += delta * (bar_return - self._mean)
std_r = math.sqrt(max(self._M2 / max(self._n - 1, 1), 1e-12))
```

### B-6 `_recover_stale_jobs_on_startup` 将 `research_queued`/`research_running` 状态改为 `rejected` 而非 `draft`
**文件**: `core/research/orchestrator.py:241`  
**问题**: 服务重启后，仍在 `research_queued`/`research_running` 的提案被强制改为 `rejected`，并写入 `"last_research_error": "service restart — job not completed"`。这对用户而言不友好：用户之前手动提交的研究提案在重启后变成"失败"，而不是可重新运行的"草稿"状态。  
**影响**: 用户体验差，所有未完成研究在重启时被标记为"已拒绝"，需要重新从零创建提案。  
**修复建议**: 将状态改为 `"draft"` 而非 `"rejected"`，并在 metadata 中记录恢复原因，允许用户重新触发运行。

### B-7 `_correlation_filter_candidates` 中 `curves` 字典用 `strategy` 名称作 key 而非 `candidate_id`——同策略不同参数互相覆盖
**文件**: `core/research/orchestrator.py:964`  
**问题**: `curves: Dict[str, Optional[List[float]]] = {c.strategy: _get_curve(c) for c in candidates}` 用策略名称（如 `"MAStrategy"`）作 key。若同批次包含两个 `MAStrategy` 候选（不同参数），后者会覆盖前者的 equity curve，导致相关性计算用错数据。  
**影响**: 同策略不同参数的候选可能被错误地认为与对方高度相关（因为共享同一条曲线），造成错误过滤。  
**修复建议**: 改为 `{c.candidate_id: _get_curve(c) for c in candidates}`，并在相关性循环中用 `candidate_id` 查找。

### B-8 `_send_cusum_notification` 在同步函数中调用 `asyncio.create_task()`——无事件循环时会崩溃
**文件**: `core/monitoring/cusum_watcher.py:128-133`  
**问题**: `_send_cusum_notification()` 是普通同步函数，但在其中调用 `asyncio.create_task(...)`。`create_task()` 要求在运行中的事件循环内调用。`_send_cusum_notification` 由 `run_cusum_checks_for_all_candidates`（async 函数）调用，虽然通常有事件循环，但 `asyncio.create_task()` 在同步函数中是不安全的（`get_running_loop()` 行为取决于调用上下文）。此外，创建的 task 没有被保存引用，可能被 GC 取消。  
**影响**: 若通知在垃圾回收前未完成，通知任务被静默取消；在某些调用路径下可能抛 `RuntimeError: no running event loop`。  
**修复建议**: 将 `_send_cusum_notification` 改为 async，并在调用处 `await` 或用 `asyncio.ensure_future()`；或传入 loop 引用。

### B-9 `promotion_narrator.py` 中 `candidate_dict.get("symbol")` 永远为 `None`——`StrategyCandidate` 无顶级 `symbol` 字段
**文件**: `core/ai/promotion_narrator.py:60`  
**问题**: `candidate_dict.get("symbol", "—")` 和 `candidate_dict.get("timeframe", "—")` 从 `StrategyCandidate.model_dump()` 中取值，但 `StrategyCandidate` 的字段是 `symbol`（单数）和 `timeframe`——需要确认这些字段是否存在。查看 `_create_candidates_from_result` 中 `StrategyCandidate(symbol=..., timeframe=...)` 的初始化（line 1092）确认有这两个字段，所以这里没有 Bug。但 `best.get("opt_method")` 依赖 `metadata["best"]["opt_method"]`，该字段仅在 Phase 3 scipy LHS 优化后才存在；若研究未做参数优化（`opt_method = "none"`），字段缺失时返回 `"none"` 是正确的。  
**重新评级**: 实际为低优先级，但需注意 `metadata["best"]` 可能为空导致 `best.get()` 全返回 `None`，prompt 会有大量 `—`，LLM 生成质量下降。  
**建议**: 在 `generate_promotion_rationale()` 顶部增加对 `best` 为空的 early return。

---

## 中优先级 (性能 / 并发)

### M-1 `_JsonRegistry.list()` 无锁调用 `_load()`
**文件**: `core/research/experiment_registry.py:90-101`  
**问题**: `list()` 方法不持 `_lock` 就调用 `_load()`。若 `list()` 读 `_cache` 时另一线程正在 `_flush()` 替换 `_cache` 内容，可能迭代到半完成状态。  
**影响**: 低概率数据竞争，Python GIL 在 CPython 下缓解了最严重情形，但仍是编程错误。  
**修复建议**: 在 `list()` 中加锁：
```python
def list(self, limit=50):
    with self._lock:
        rows = list(self._load().values())
    rows.sort(...)
```

### M-2 `_cusum_monitor_worker` 停止逻辑：1s 轮询循环低效
**文件**: `web/main.py:1048-1051`  
**问题**: `for _ in range(300): if stop_event.is_set(): break; await asyncio.sleep(1)` 每秒检查一次 stop 事件，共 300 次（5 分钟），产生 300 个无效唤醒。  
**影响**: 资源浪费（轻微），更优雅的模式是 `await asyncio.wait_for(stop_event.wait(), timeout=300)`。  
**修复建议**:
```python
try:
    await asyncio.wait_for(stop_event.wait(), timeout=INTERVAL)
    break  # stop requested
except asyncio.TimeoutError:
    pass   # interval elapsed, continue
```

### M-3 `asyncio.gather(..., return_exceptions=True)` 外层 `asyncio.wait_for` 超时后 task 泄漏
**文件**: `core/research/orchestrator.py:1293-1296`  
**问题**:
```python
results = await asyncio.wait_for(
    asyncio.gather(*[_add_rationale(c) for c in worthy], return_exceptions=True),
    timeout=30.0,
)
```
当 `wait_for` 超时后，`asyncio.gather` 创建的内部 task 不会被自动取消，而是继续在后台运行（泄漏），直到它们自然完成或 GLM 连接超时。  
**影响**: 超时时，最多 3 个 GLM 调用（`timeout=25` 秒）继续在后台占用连接，可能影响后续研究任务的 GLM 限流配额。  
**修复建议**: 捕获 `TimeoutError` 后显式取消任务：
```python
gather_task = asyncio.gather(*[_add_rationale(c) for c in worthy], return_exceptions=True)
try:
    results = await asyncio.wait_for(gather_task, timeout=30.0)
except asyncio.TimeoutError:
    gather_task.cancel()
    logger.warning("LLM rationale timed out; tasks cancelled")
    results = []
```

### M-4 `_correlation_filter_candidates` 在空 equity curve 时跳过但不做 exact-key 检查
**文件**: `core/research/orchestrator.py:1002-1006`  
**问题**: 若 `my_curve is None`（equity_curve_sample < 10 点），跳过相关性检查但不跳过 exact-key 去重检查（line 989 的 `if my_exact_key in accepted_exact_keys` 仍会执行）。但在 `accepted_exact_keys` 集合初始化时（line 966），`existing_candidates` 的 exact key 加入了集合，所以重复候选能被检测到。这里没有直接 Bug，但若 `my_curve is None` 时，该候选会被 `accepted.append(...)` 添加进去（line 1031-1038），后续相关性计算时其他候选与它比较但得不到曲线（`peer_curve is None` → skip），实际上降低了过滤效果。  
**影响**: 无 equity curve 的候选"免疫"相关性过滤，可能导致冗余策略并存。  
**建议**: 在文档/注释中明确此行为，或对无曲线候选给予额外标记。

### M-5 `generate_research_proposal` 调用 `_parse_market_context` 时有大量同步 I/O（文件读取）
**文件**: `core/ai/research_planner.py:344-474`  
**问题**: `_parse_market_context` 在 `generate_research_proposal`（同步函数）中调用，内部通过 `try/import` 的方式加载 `google_trends_collector.load_latest()`、`macro_collector.load_macro_snapshot()` 等，这些函数读取 `.parquet` 文件（`pd.read_parquet`）。在高并发下，这些文件读操作（可能是缓慢的 I/O）在同步路径中执行，阻塞当前线程（FastAPI 默认在 async 路由中，同步调用会阻塞事件循环）。  
**影响**: 当 parquet 文件较大或磁盘慢时，每次调用 planner 都可能短暂阻塞事件循环 10-50ms。  
**建议**: 将 `_parse_market_context` 中的文件读取操作移到 `asyncio.get_event_loop().run_in_executor()` 或预先缓存（TTL 缓存）。

### M-6 `_finalize_research_run` 中 `promotion_result` 变量永远为 `None`
**文件**: `core/research/orchestrator.py:1397, 1468`  
**问题**: `promotion_result = None`（line 1397），之后尝试 `await governance_propose_strategy(...)` 并将结果存入 `candidate.metadata["strategy_spec"]`，但 `promotion_result` 始终为 `None`。最后 `return` 里 `"promotion": promotion_result["promotion"] if promotion_result else promotion`（line 1468）永远走 `else` 分支，`promotion_result` 的值从未被赋值。  
**影响**: 不影响正确性（`else` 分支返回 `promotion` 是正确的），但 `promotion_result` 变量声明和使用不一致，是死代码。  
**修复建议**: 删除 `promotion_result` 变量，简化 return 为 `"promotion": promotion`。

---

## 低优先级 (死代码 / 一致性)

### L-1 `strategy_monitor.py` 的 scipy IQR 分支是死代码（重复 B-4）
**文件**: `core/monitoring/strategy_monitor.py:83-88`  
`from scipy.stats import iqr as _iqr` 导入后，`q75, q25` 两次调用均产生 IQR 值（非分位数），结果完全未使用，然后立即 `raise ValueError("use std")`。整个 try 块是无效代码。建议直接删除 try 块，使用 sample std，或改为正确的 MAD 实现（见 B-4）。

### L-2 `_parse_market_context` 的 `news_events_count >= 10` 和 `>= 5` 逻辑重复
**文件**: `core/ai/research_planner.py:311-315`  
```python
if news_events >= 10:
    boosted.extend(["宏观"])
elif news_events >= 5:
    boosted.extend(["宏观"])
```
两个分支做的是完全相同的操作（boost "宏观"），`>=10` 的分支没有额外效果。应合并为 `if news_events >= 5: boosted.extend(["宏观"])`，或为 `>=10` 设置更强的 boost（如额外推送一次）。

### L-3 `cusum_watcher.py` 中 `_send_cusum_notification` 的 `asyncio.create_task()` 无引用保留
**文件**: `core/monitoring/cusum_watcher.py:128-133`  
`asyncio.create_task(notification_manager.send_message(...))` 的 task 无变量引用，CPython 3.10+ 的文档明确警告此模式会导致 task 被垃圾回收取消。应改为：
```python
task = asyncio.create_task(notification_manager.send_message(...))
# 在 module 或 app.state 中保存引用，或使用 asyncio.ensure_future
```
（此问题也在 B-8 中提及，从并发安全角度属于高优先级，从 GC 角度属于低优先级。）

### L-4 `_recover_stale_jobs_on_startup` 不清理 `app.state.research_job_tasks`（内存泄漏）
**文件**: `core/research/orchestrator.py:263-270`  
重启恢复时，将 `research_jobs` 中 pending/running 状态改为 failed，并调用 `_persist_research_jobs()`，但未清除 `app.state.research_job_tasks`（line 406-407）中对应的 asyncio Task 引用。由于服务刚启动，`research_job_tasks` 应该是空的，但若 `ensure_ai_research_runtime_state` 被多次调用（即不是首次初始化），这可能留下悬空任务引用。  
**影响**: 极低，正常路径下不触发。  
**建议**: 在恢复函数末尾加 `getattr(app.state, 'research_job_tasks', {}).clear()`。

### L-5 `_proposal_state_from_candidate_status` 回退到 `"validated"` 不正确
**文件**: `core/research/orchestrator.py:275-279`  
```python
def _proposal_state_from_candidate_status(status: Any) -> str:
    if text in {"paper_running", "shadow_running", ...}:
        return text
    return "validated"  # 兜底
```
若候选状态为 `"new"` 或 `"rejected"`，回退返回 `"validated"`，可能错误地将提案标记为"已验证"，而实际上该提案对应的候选从未被验证通过。  
**影响**: 在 `_recover_missing_proposals_from_candidates` 中，孤立的 `"new"` 候选被恢复为 `"validated"` 状态的提案，可能误导 UI 显示。  
**修复建议**: 将兜底改为 `"draft"` 或 `"rejected"` 取决于候选状态：
```python
if text in {"new"}:
    return "validated"  # new = 研究完成但未推广
if text in {"rejected", "retired"}:
    return "rejected"
return "validated"
```

### L-6 `research_planner.py` 的分类别名与 JS 侧 `STRATEGY_CATEGORIES` 的同步问题
**文件**: `core/ai/research_planner.py:78-99`  
Python 侧 `_category_aliases` 包含 18 个 regime 映射，对应约 10 个中文分类（趋势、震荡、动量等）。JS 侧的 `STRATEGY_CATEGORIES`（`web/static/js/ai_research.js`）需要同步维护相同的策略-分类映射。审计期间 Python 侧出现了 `"均值回归"` 这一分类，若 JS 侧遗漏则候选卡片的分类徽章颜色会缺失（显示默认灰色）。建议添加自动化测试或生成式 JS 配置，避免手动同步漂移。

### L-7 `promotion_narrator.py` 无超时设置给 `AsyncGLMClient`
**文件**: `core/ai/promotion_narrator.py:77-86`  
`client.chat_completions(timeout=timeout)` 中 `timeout=25`。但 `AsyncGLMClient.chat_completions` 的 `timeout` 参数语义需确认（是总超时还是读超时）。若 GLM 网络延迟 > 25s，整个 `_finalize_research_run` 的等待时长可达 3×25=75s（3个候选串行），而外层 `wait_for(timeout=30)` 会触发超时并取消（参见 M-3）。两者时间设置矛盾（每个候选 25s，但总预算只有 30s），实际上第一个候选超时后整个 gather 就被取消。  
**影响**: 若 GLM 响应慢，实际只有第一个候选（< 30s）有机会获得 rationale，后两个总是被 TimeoutError 取消。  
**建议**: 将每个候选的 GLM timeout 降低到 8-10s，总 `wait_for` 调整到 15s，或改为串行并给每个限制 8s。

### L-8 `_create_candidates_from_result` 在 `best_per_strategy` 为空时只创建单个候选
**文件**: `core/research/orchestrator.py:1057-1058`  
```python
if not best_per_strategy and best and best.get("strategy"):
    best_per_strategy = {str(best["strategy"]): best}
```
这是正确的降级兜底，但注释中没有说明此情形发生的条件（`run_strategy_research` 返回无 `best_per_strategy` 字段时）。若 `strategy_research.py` 未正常填充 `best_per_strategy`（如研究途中出错），则整个批次退化为单策略，相关性过滤和多候选对比功能失效。建议在 `_finalize_research_run` 中记录此情形到 job progress。

---

## 备注

1. **`os.replace` 原子性（Windows 特殊性）**: 在 Windows 上，`os.replace` 对于同文件系统上的同进程操作是原子的，但跨进程文件锁定会导致失败（见 B-3）。鉴于系统在 Windows 10 上运行，此问题有现实风险。

2. **CUSUM math 正确性**: 核心公式 `s = min(0, s + (r - target + allowance))` 与 Page (1954) 下侧 CUSUM 定义一致，触发条件 `s <= -h * std` 正确。唯一问题是 std 估计函数损坏（B-4）。

3. **`_recover_stale_jobs_on_startup` 正确性**: 清理逻辑覆盖了 proposals（→rejected）、experiment runs（→failed）、persisted jobs（→failed）三个层面，语义上完整。主要问题是 `rejected` 而非 `draft` 的设计选择（B-6）。

4. **`asyncio.gather(return_exceptions=True)` 是否吞错误**: 不是——每个 `_add_rationale` 内部已 `try/except` 包裹，错误以 `None` rationale 形式处理。`return_exceptions=True` 额外保证单个任务 Exception 不传播，整体处理正确。但外层 `wait_for` 超时问题（M-3）仍需处理。

5. **`experiment_registry.py` threading.Lock vs RLock**: 当前用 `threading.Lock`，`_flush()` 在已持锁的 `save()` 内部调用 `self.list(limit=None)` → `_load()`，`_load()` 不持锁（直接访问 `_cache`），所以不会死锁。但若未来在锁内调用 `list()` 会产生死锁风险，建议改用 `threading.RLock`。

6. **`_parse_market_context` 信号逻辑评价**: funding_rate/OFI/news_events_count/whale_count 的规则阈值（如 `fr > 0.0001`，`news_events >= 5`）在语义上合理。但多个信号同时 boost 同一分类时（如 sentiment LONG + OI rising + low VIX 同时 boost "趋势"），结果是该分类被多次加入 `boosted` 列表——`Counter` 方法正确处理了重复，net vote 越高优先级越靠前，逻辑正确。

7. **测试覆盖**: Phase 4 已有 46 个测试，建议补充：(a) `experiment_registry` 多线程并发 save 测试；(b) `detect_strategy_decay` 的 IQR 修复后回归测试；(c) Windows 上 `os.replace` PermissionError 的模拟测试。
