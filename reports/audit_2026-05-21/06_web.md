# Web 层 (API + 前端) 审计报告 (2026-05-21)

---

## 高优先级 (Bug, 必修)

### 1. strategies.py — start/stop/pause/update 端点仍直接 `await audit_logger.log()`，阻塞响应
`web/api/strategies.py:2432,2434,2444,2446,2455,2457,2471,2479,2521,2529` — 
register/delete 已改为 `asyncio.create_task`，但 start/stop/pause/update_params/update_allocation 五个高频端点仍直接 `await audit_logger.log()`。audit_logger 底层写 SQLite；如果 news 后台 worker 持有写锁，这些端点会阻塞 5–30s。
**修复**：全部改为 `asyncio.create_task(audit_logger.log(...))`，与 register/delete 保持一致。

### 2. trading_runtime.py — 四个管控端点阻塞 `await audit_logger.log()`
`web/api/trading_runtime.py:91,108,128,199` — `update_risk_params`、`reset_risk_halt`、`reset_paper_trading_state`、`confirm_trading_mode_switch` 四个端点均 `await audit_logger.log()`，且这些端点通常在操作敏感情境下调用，延迟尤为明显。
**修复**：改为 `asyncio.create_task(audit_logger.log(...))`。

### 3. ai_research.js — `pendingLlmContext` 在提案发送失败时才清空，但失败路径（行 5050）在成功分支内
`web/static/js/ai_research.js:4949,5050` — 在 `generateProposal()` 中，`state.pendingLlmContext = null` 在成功调用 API 后（行 4949）先清空一次；然后在 oneclick 成功回调（行 5050）又清一次。但若 `/proposals/generate` 本身抛出网络异常，`pendingLlmContext` **已在行 4949 被清空**（在 `await aiApi(...)` 之前），导致研究思路内容丢失，用户需重新生成。
**修复**：先 `await aiApi(...)` 成功后再清空 `pendingLlmContext`。将行 4947–4953 整体移动到 `await` 之后。

### 4. ai_research.js — 重复的 `#btn-activate-live` 点击监听器注册（死代码注释残留 + 活跃注册各一个）
`web/static/js/ai_research.js:3846–3872,3874–3904` — 被注释掉的旧实现（行 3846–3872）与新实现（行 3874–3904）共存，逻辑完全相同（同为 `activateCandidateLive`），注释块只要被误恢复就会产生双重事件。此外每次 `viewCandidate()` 调用都会对同一个 DOM 节点重新 `addEventListener`（因为 panel 内容是 innerHTML 重写后的新元素，实际无泄漏，但注释残留形成维护风险）。
**修复**：删除行 3846–3872 的死代码注释块。

### 5. ai_research.js — 轮询定时器在 `stopJobPolling` 后 `pollJobStatus` 抛出异常仍继续轮询
`web/static/js/ai_research.js:5431–5432` — `startJobPolling` 使用 `setInterval(() => pollJobStatus(...).catch(() => {}))` 静默捕获所有错误。如果 `/proposals/{id}/job-status` 反复 404（提案被删除后），定时器永不停止，直到用户刷新页面。
**影响**：每 3s 发出无效请求，在频繁创建/删除提案的工作流中累积。
**修复**：在 `.catch()` 中检查错误类型（如 404），若是则调用 `stopJobPolling(proposalId)`。

### 6. ai_research.js — `sort/filter` 变更后重渲染不保留已选中候选
`web/static/js/ai_research.js` — `getVisibleCandidates()` 根据 `state.filterCategory` 和 `state.sortBy` 过滤/排序候选列表，随后 `renderCandidateList()` 重新写入 DOM。若当前选中的候选被过滤掉，`state.selectedCandidateId` 仍为旧值，右侧详情面板与列表不同步（右侧显示旧候选详情，左侧无高亮）。
**修复**：`getVisibleCandidates()` 之后检测 `state.selectedCandidateId` 是否仍在可见集合中，若不在则清空或自动选中首个。

### 7. ai_research.py — `job-status` 端点在提案 `last_research_job_id` 为空时返回 `job=null` 但 `job_status=None`（JS 端对 `None` vs `'None'` 处理不一致）
`web/api/ai_research.py:3647` — `"job_status": job.get("status")` 在 job 字典为空 `{}` 时返回 `None`（Python None，JSON null）。
`web/static/js/ai_research.js:5444` — JS 端用 `const js = data?.job_status` 做字符串比较（`js === 'completed'` 等），`null` 不等于任何字符串，轮询永不停止（除非提案状态变化触发停止）。
**修复**：API 端改为 `"job_status": job.get("status") or None`（已是 None，但注意 JS 端 `js === 'completed'` 条件正确）。真正的问题是没有对"提案已到终态但无 job_id"的情况在 JS 端做 early exit。补充：当 `proposalStatus in {'validated','rejected','retired'}` 时，无论 `js` 为何值都应停止轮询。

---

## 中优先级 (性能 / 并发)

### 8. research_workbench.js — `loadRegimeCalendar()` 在 `renderModule('market_state',...)` 内无条件触发，不管 market_state 数据是否真正有效
`web/static/js/research_workbench.js:911` — 每次 `renderModule('market_state', ...)` 都调用 `loadRegimeCalendar().catch(() => {})`，包括仅刷新 sentiment 子面板时也会触发。若 market_state 数据刚好是空/降级，regime calendar 会发出一个必然失败的 API 请求。
**修复**：加条件守卫 `if (module?.status === 'ok') loadRegimeCalendar().catch(() => {})` 或检查 `payload.analytics_overview` 非空。

### 9. web/main.py — `_data_maintenance_worker` 在单次 `_run_data_maintenance_once()` 内顺序调用所有 `_sync_market_dataset`（两层 for 循环），未并发
`web/main.py:762–769` — 对 7 个 symbol × 9 个 timeframe + 5 个 symbol × 4 个 timeframe ≈ 83 次 `_sync_market_dataset` 调用，全部顺序 `await`，单次维护可能耗时数十分钟阻塞 worker。
**修复**：用 `asyncio.gather(*tasks)` 并发执行，或至少按 symbol 并发（控制并发数以避免交易所限速）。

### 10. ai_research.js — 候选列表每次 `refreshWorkbench()` 都全量重新渲染
全量 `innerHTML` 写入（约 200 候选）在 3s 轮询间隔内会因 DOM 重渲触发 reflow，在低端设备上可能掉帧。目前没有 virtual scroll 或 diff 渲染。
**建议**：短期可按 `candidate_id + score` 做 keyed diff，仅更新变化项（类似 reconciler）。

### 11. research_workbench.js — `setInterval`（`autoRefreshTimer`、`_countdownTimer`、`pollingOwnershipMonitor`）未在页面隐藏/卸载时清理
`web/static/js/research_workbench.js:1665–1673` — 这些定时器通过 `document.hidden` 检查来跳过执行，但 `clearInterval` 只在 `startWorkbenchAutoRefresh()` 前置调用，从未在 `beforeunload` 或 `visibilitychange` 中明确清理。在多 Tab 场景下导致定时器累积。
**修复**：在 `window.addEventListener('beforeunload', ...)` 中调用 `clearInterval` 清理所有定时器。

### 12. ai_research.js — `liveSignalTimer`/`signalTimer`/`refreshTimer` 无清理入口
`web/static/js/ai_research.js:97–100` — 三个主要轮询定时器仅在 `init()` 中设置，无对应 `destroy()` 或 `beforeunload` 清理。SPA 单页多 Tab 场景下存在定时器泄漏。
**修复**：在 `window.addEventListener('beforeunload', cleanup)` 中清除全部定时器。

### 13. ai_research.js — 候选详情面板每次 `viewCandidate()` 都对同一个固定 ID 元素（`#btn-order-preview`、`#btn-autonomy-handoff` 等）重复 addEventListener
`web/static/js/ai_research.js:3830,3833,3874,3908,3926,3944` — 因 detail panel 是 innerHTML 整体替换，旧 DOM 节点被销毁，实际上不会泄漏。但若未来改为 DOM diff 渲染，将产生多重监听器。目前是安全的，但值得关注。

---

## 低优先级 (死代码 / 一致性)

### 14. ai_research_candidates.js — 完整文件未被任何 HTML 模板引用（孤儿文件）
`web/static/js/ai_research_candidates.js` (297行) — 该文件在 `index.html` 中**未加载**，只被 `tests/test_ai_research_phase5_ui_assets.py` 引用。文件内部通过 `aiRoot().util?.statusText` 代理调用 `ai_research.js` 的函数，不能独立运行。
**建议**：确认是否是计划中的插件模块（Phase 5 预留），若是则在 HTML 中引入；若无用则删除，避免维护负担。

### 15. ai_research_patch.js — 同样未被 index.html 引用
`web/static/js/ai_research_patch.js` — Glob 显示该文件存在但 `index.html` 中没有引用。与上一条类似，需确认是孤儿还是延后加载。

### 16. ai_research.js — `btn-activate-live` 注释死代码块（行 3846–3872）
详见 bug #4，这里再次强调清理价值：注释块为旧的 `activateCandidateLive` 实现，与当前新实现（行 3874）功能完全相同，应删除以降低阅读负担。

### 17. ai_research.js — `statusText` 函数定义在 ai_research.js 内，同名函数又出现在 ai_research_candidates.js 和 ai_research_runtime.js（各含代理实现）
若 `ai_research_candidates.js` 被正式引入 HTML，`statusText` 将在模块内单独定义（代理实现），与 `ai_research.js` 中的权威实现逻辑不一致（代理版不包含完整中文映射）。
**建议**：将 `statusText`、`esc`、`getFamilyMeta` 等工具函数集中到一个 `ai_research_utils.js` 或通过 `window.AI.util` 统一暴露，避免重复代理。

### 18. research_workbench.js — 模块状态时间戳 `moduleTimes[name]` 用 `new Date().toISOString()` 记录本地时间，而展示时用 `toLocaleTimeString('zh-CN', {timeZone: UI_TIMEZONE})` 转换
`web/static/js/research_workbench.js:902,498` — `state.moduleTimes[name] = new Date().toISOString()` 记录的是浏览器本地 ISO 字符串（实际为 UTC），展示时再转换为上海时间。逻辑正确但注释不清晰，且若 `UI_TIMEZONE` 环境变量未设置，`ZoneInfo` 降级为 `timezone.utc`（Python 端），而 JS 端的 `AI_UI_TIMEZONE` 默认值为 `'Asia/Shanghai'`，两端不一致。
**建议**：确保 `CTS_UI_TIMEZONE` 环境变量显式设置，使 Python / JS 两端时区一致。

### 19. strategies.py — `start/stop/pause` 端点在成功路径后仍有 `await audit_logger.log`（失败分支）占用响应
与高优先级 #1 相同文件，失败分支的 `await audit_logger.log` 也阻塞（虽然此时不影响成功响应，但 HTTPException 被抛出前仍等待 DB 写入）。

### 20. web/main.py — `_data_maintenance_worker` 每次维护完成后等待 `6h - elapsed`，若单次耗时超过 6h 则立即再次执行（`max(300, ...)`保底 5min）
`web/main.py:1160–1165` — 逻辑正确，但若维护任务因网络问题持续数小时，睡眠时间可能降至最低 300s，导致维护过于频繁。可接受，无需强制修复，建议加日志说明。

### 21. web/main.py — `emit_counter` 变量在 `_news_refresh_worker` 中递增但只在 `should_emit=True` 时重置，而 `should_emit` 固定为 `True`
`web/main.py:488–492` — 代码存在 `emit_counter += 1` 和 `should_emit = True`（硬编码），意味着每次循环都 emit，`emit_counter` 计数无意义。旧逻辑可能意图每 N 次 pull 才 emit 一次（频率控制），现已退化为每次都 emit。
**建议**：若不需要频率控制则删除 `emit_counter`；若需要则修复 `should_emit` 判断逻辑。

### 22. web/api/ai_research.py — `_OPERATING_MODE_CACHE_TASK` 是模块级全局变量，在多 worker 进程（uvicorn `--workers N`）时不共享
`web/api/ai_research.py:64` — 若使用多进程部署，每个 worker 有独立的 `_OPERATING_MODE_CACHE`，缓存不共享，每个 worker 将独立发起后台刷新任务，造成不必要的重复。单 worker 部署（单进程）下无问题。
**建议**：文档/配置中明确"单 worker 运行"要求，或改用 Redis 等进程间共享缓存。

### 23. web/api/trading.py 和 web/api/trading_runtime.py — `await audit_logger.log()` 在异常路径下（cancel_order、create_order 超时/失败分支）也直接 await
`web/api/trading.py:5129,5147,5156,5396` — 与 strategies.py 同类问题，这里是交易下单和撤单的错误路径，audit 写入阻塞会使错误响应延迟返回到前端。
**修复**：同上，改为 `asyncio.create_task`。

---

## 备注

1. **`ai_research.js` 当前行数约 6603 行**（未注明 dedup 是否进一步压缩），未发现新的函数级重复定义（通过 sort+uniq 验证）。先前报告的 9 组重复已清理，本次不存在新重复。

2. **XSS 风险评估**：`ai_research.js` 中 `innerHTML` 赋值为 33 处，主要用服务端返回数据填充。对来自 API 的用户可控字段（`thesis`、`strategy`、`goal`、`reason`、`notes`），JS 代码**一致使用 `esc()` 函数转义**后再插入 HTML，未发现明显未转义路径。`llm_rationale` 字段（行 3684）同样经 `esc()` 处理。

3. **版本标记**：`index.html` 无硬编码 `JS vXX/CSS vXX` 注释；通过 `static_asset_url` Jinja2 filter 实现缓存破坏（可能为哈希或时间戳），无版本不一致风险。

4. **`quick_register_candidate` 端点**（行 4838）在治理模式（`GOVERNANCE_ENABLED=True`）下要求候选处于 `promotion_pending_human_gate=True` 状态，否则返回 400。`_execute_oneclick_candidate_deploy`（行 423）直接调用它，若候选尚未通过研究验证进入待审批状态，oneclick 部署流程将以 400 失败。这是预期行为，但 UI 侧 oneclick 按钮没有对候选状态进行前置校验，可能产生用户困惑。

5. **`ai_research_patch.js`** 文件存在（Glob 可见）但未被 HTML 引入，可能是早期补丁机制遗留，建议确认后删除。
