from pathlib import Path

from jinja2 import Environment

from web.asset_versions import static_asset_url


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (REPO_ROOT / rel_path).read_text(encoding="utf-8-sig")


def _render_template(source: str) -> str:
    return Environment(autoescape=False).from_string(source).render(static_asset_url=static_asset_url)


def test_ai_research_template_loads_phase5_modules():
    template_source = _read("web/templates/index.html")
    news_template_source = _read("web/templates/news.html")
    template = _render_template(template_source)
    news_template = _render_template(news_template_source)

    assert 'id="ai-flow-console"' in template
    assert 'id="ai-chain-summary-grid"' in template
    assert 'id="ai-flow-stage-grid"' in template
    assert 'id="ai-planner-research-mode"' in template
    assert 'id="ai-planner-max-drafts"' in template
    assert 'id="ai-planner-max-backtests"' in template
    assert 'id="ai-auto-goal-btn"' in template
    assert 'id="ai-oneclick-btn"' in template
    assert 'id="ai-oneclick-allocation"' in template
    assert 'id="ai-oneclick-feedback"' in template
    assert 'id="ai-candidate-cards"' in template
    assert 'id="ai-clear-candidates-btn"' in template
    assert 'id="ai-clear-queue-btn"' in template
    assert 'id="ai-exit-running-queue-btn"' in template
    assert 'id="ai-queue-title"' in template
    assert 'id="ai-queue-hint"' in template
    assert '研究目标（可留空自动生成）' in template
    assert 'placeholder="留空自动生成，或写明研究方向"' in template
    assert '判断市场 → 生成目标 → 生成提案 → 后台研究 → 尝试部署' in template
    assert '上海时间 UTC+8' in template
    assert '页面时区：上海时间 (UTC+8)' in news_template
    assert '/static/favicon.svg' in template
    assert '/static/favicon.svg' in news_template
    assert "{{ static_asset_url('js/ai_research.js') }}" in template_source
    assert "{{ static_asset_url('js/news_tab_runtime.js') }}" in news_template_source
    assert static_asset_url("js/ai_research.js") in template
    assert static_asset_url("js/ai_research_diagnostics.js") in template
    assert static_asset_url("js/ai_research_runtime.js") in template
    assert static_asset_url("js/ai_research_agent.js") in template
    assert "/static/js/ai_research_patch.js" not in template
    assert static_asset_url("js/news_tab_runtime.js") in news_template


def test_ai_research_phase5_assets_exist_and_define_flow_styles():
    diagnostics_js = _read("web/static/js/ai_research_diagnostics.js")
    runtime_js = _read("web/static/js/ai_research_runtime.js")
    candidates_js = _read("web/static/js/ai_research_candidates.js")
    agent_js = _read("web/static/js/ai_research_agent.js")
    ai_js = _read("web/static/js/ai_research.js")
    news_runtime_js = _read("web/static/js/news_tab_runtime.js")
    app_js = _read("web/static/js/app.js")
    template = _read("web/templates/index.html")
    style_css = _read("web/static/css/style.css")

    assert "modules.diagnostics" in diagnostics_js
    assert "ai-flow-stage-grid" in runtime_js
    assert "renderChainSummary" in runtime_js
    assert "proposalResearchThesis(proposal)" in runtime_js
    # candidateResultTop must read backtest rows from metadata.top_results (the
    # canonical /candidates serialization), not a non-existent top-level field —
    # otherwise the flow-console Step 3 drawdown metric is always "--".
    assert "Array.isArray(meta.top_results)" in runtime_js
    assert "modules.candidates" in candidates_js
    assert "window.agentStart = agentStart" in agent_js
    assert "renderAgentChainSummary" in agent_js
    assert "buildAgentJournalCurrentSummary" in agent_js
    assert "summarizeAggregatedSignal" in agent_js
    assert "buildAgentExecutionReality" in agent_js
    assert "const modeText = reality.label" in agent_js
    assert "满足纪律后执行" in agent_js
    assert "纸盘提交" in agent_js
    assert "影子/0权重" in agent_js
    assert "provider_live_execution_restricted" in agent_js
    assert "直接执行" not in agent_js
    assert "function describeExecutionCost" in agent_js
    assert "body: JSON.stringify({ force: true })" in agent_js
    assert agent_js.count("async function loadAgentJournal()") == 1
    assert agent_js.count("function renderAgentRanking(") == 1
    assert "function renderAgentStatusLoadError" in agent_js
    assert "next_run_at" in agent_js
    assert "last_latency_ms" in agent_js
    assert "单次试跑已触发" in agent_js
    assert "已有一轮在运行，手动触发已排队" in agent_js
    assert "function selectProposal(" in ai_js
    assert "function renderDecisionTracePanel(" in ai_js
    assert "关键门槛：" in ai_js
    assert "检查链路" in ai_js
    assert "运行模式" in ai_js
    assert "工作队列" in ai_js
    assert "发送到自治观察" in ai_js
    assert "/operating-mode" in ai_js
    assert "/work-queue" in ai_js
    assert "/autonomy-handoff" in ai_js
    assert "operatingModeInFlight" in ai_js
    assert "workQueueInFlight" in ai_js
    assert "refreshOperatingModeBanner({ preserveExisting: true })" in ai_js
    assert "refreshWorkQueuePanel({ preserveExisting: true })" in ai_js
    assert "btn.dataset.registerMode" in ai_js
    assert "target?.dataset" not in ai_js
    assert "toast(" not in ai_js
    assert "notify('已送入 AI 自治观察队列')" in ai_js
    assert "decision_trace" in agent_js
    assert "refreshAgentOperatingModeBanner" in agent_js
    assert "/ai/operating-mode" in agent_js
    assert "function isVirtualProposal(" in ai_js
    assert "function autoSelectCandidateForProposal(" in ai_js
    assert "function sortProposalsForWorkbench(" in ai_js
    assert "buildPlannerConstraints" in ai_js
    assert "buildAiPlannerWorkbenchProfile" in ai_js
    assert "loadAutoResearchRecommendation" in ai_js
    assert "ensureAutoPlannerGoal" in ai_js
    assert "function formatDerivativesContextLine(derivativesContext)" in ai_js
    assert "history_ready: !!derivativesPayload?.history_ready" in ai_js
    assert "funding_zscore: Number(derivativesPayload?.funding_zscore)" in ai_js
    assert "derivatives_labels: Array.isArray(derivativesPayload?.derivatives_labels)" in ai_js
    assert "hist ${historyInterval || '?'}" in ai_js
    assert "AI_PLANNER_GOAL_MAX_CHARS = 600" in ai_js
    assert "function clampPlannerGoalText(" in ai_js
    assert "function resolveAutoPlannerGoal(" in ai_js
    assert "withActionLock('oneclick'" in ai_js
    assert "buildOneClickFailureFeedback" in ai_js
    assert "buildOneClickSuccessFeedback" in ai_js
    assert "renderOneClickFeedback" in ai_js
    assert "function getVisibleCandidates()" in ai_js
    assert "function getVisibleCandidateProposalTargets(" in ai_js
    assert "function getVisibleProposalQueueItems()" in ai_js
    assert "function getVisibleProposalQueueTargets(" in ai_js
    assert "function getVisibleRunningQueueTargets(" in ai_js
    assert "function clearVisibleCandidates()" in ai_js
    assert "function clearVisibleProposalQueue()" in ai_js
    assert "function exitVisibleRunningQueueItems()" in ai_js
    assert "function updateClearCandidatesButton(" in ai_js
    assert "function updateClearQueueButton(" in ai_js
    assert "function updateExitRunningQueueButton(" in ai_js
    assert "function proposalResearchThesis(" in ai_js
    assert "function normalizeProposalPresentation(" in ai_js
    assert "refreshWorkbenchPendingRequest" in ai_js
    assert "function mergeWorkbenchRefreshRequest(" in ai_js
    assert "function takeWorkbenchRefreshRequest(" in ai_js
    assert "while (state.refreshWorkbenchPendingRequest)" in ai_js
    assert "arguments.length >= 1" in ai_js
    assert "arguments.length >= 2" in ai_js
    assert "parseAllocationPercentInput" in ai_js
    assert "completed_without_compatible_runtime_target" in ai_js
    assert "manual_action_required" in ai_js
    assert "async function humanApprove" not in ai_js
    assert "async function humanReject" not in ai_js
    assert "const watchlist = Array.from(new Set([...DEFAULT_SIGNAL_SYMBOLS, selectedSymbol]));" in ai_js
    assert "loadSignal(undefined, { compact: true })" not in ai_js
    assert "liveDecisionActivityLastGood" in ai_js
    assert "hintEl.textContent = busy ? '执行中，请等待当前步骤完成' : '';" in ai_js
    assert "候选回填" in ai_js
    assert "该条目由候选结果回填" in ai_js
    assert "fallback_candidate_created_at" in ai_js
    assert "fallback_candidate_updated_at" in ai_js
    assert "const visibleProposals = getVisibleProposalQueueItems();" in ai_js
    assert "toArray(res?.items).map((item, index) => normalizeProposalPresentation(item, index))" in ai_js
    assert "Asia/Shanghai" in ai_js
    assert "Asia/Shanghai" in agent_js
    assert "失败队列待重试" in news_runtime_js
    assert "不代表历史新闻缺失" in news_runtime_js
    assert "已自动归档已修复失败项" in news_runtime_js
    assert "NIM摘要" in news_runtime_js
    assert "GM摘要" in news_runtime_js
    assert "DS摘要" in news_runtime_js
    assert "function plotlyBucketAxisTs(" in news_runtime_js
    assert "newsPlotlyTimeAxis({ automargin: true })" in news_runtime_js
    assert "plotlyBucketAxisTs(row.bucket_start)" in news_runtime_js
    assert "google/gemma-4-31b-it" in news_runtime_js
    assert "gemma4-local" in news_runtime_js
    assert "deepseek" in news_runtime_js
    assert "window.CTS_UI_TIMEZONE" in ai_js
    assert "window.CTS_UI_TIMEZONE_LABEL" in ai_js
    assert "const TIME_ZONE='Asia/Shanghai';" in app_js
    assert "const TRADING_STATS_TIMEOUT_MS=35000;" in app_js
    assert "const TRADING_POSITIONS_TIMEOUT_MS=30000;" in app_js
    assert "const TRADING_OPEN_ORDERS_TIMEOUT_MS=25000;" in app_js
    assert "modules.agent?.refresh?.({includeDetails:activeTab==='ai-agent'})" in app_js
    assert "else if(tab==='ai-research')refreshAiResearchModules();" in app_js
    assert "provider_fallback" in ai_js
    assert "marketContext?.derivatives_context" in ai_js
    assert "/trading/analytics/history/status?exchange=${encodeURIComponent(exchange)}&symbol=${encodeURIComponent(sym)}" in ai_js
    assert "Derivatives context:" in ai_js
    assert "recommendation?.brief?.derivatives_context" in ai_js
    assert "const hasSignalError = Boolean(String(item?.error || '').trim());" in ai_js
    assert "const signalStateText = hasSignalError ? 'ERR' : '待刷新';" in ai_js
    assert "当前已选中 watchlist，正在等待最新聚合信号快照。" in ai_js
    assert "当前 watchlist 暂无聚合信号快照，后续刷新后会自动显示。" in ai_js
    assert 'option value="codex">OpenAI' in template
    assert "执行模式" in template
    assert "一键退出运行中条目" in template
    assert "一键清空当前候选" in template
    assert "一键清空当前任务" in template
    assert ".ai-sidebar-actions" in style_css
    assert ".ai-queue-exit-btn" in style_css
    assert ".ai-queue-clear-btn" in style_css
    assert ".ai-flow-console" in style_css
    assert ".ai-chain-summary-grid" in style_css
    assert ".ai-flow-stage-grid" in style_css
    assert ".ai-candidate-cards" in style_css
    assert ".ai-hub-candidates-actions" in style_css
    assert ".ai-candidate-clear-btn" in style_css
    assert ".ai-oneclick-entry-card" in style_css
    assert ".ai-oneclick-feedback" in style_css
    assert ".ai-review-panel select" in style_css
    assert ".agent-journal-current" in style_css
    assert ".agent-journal-signal" in style_css
    assert "#ai-agent .ai-agent-cockpit-title" in style_css
    assert 'grid-template-areas:\n        "focus signal"\n        "focus live";' in style_css
    assert "grid-area: focus;" in style_css
    assert "grid-area: signal;" in style_css
    assert "grid-area: live;" in style_css
    assert "white-space: nowrap;" in style_css
    assert "overflow-wrap: break-word;" in style_css
    assert "appearance: none" in style_css
    assert "color-scheme: dark" in style_css
    assert '[data-tone="warn"]' in style_css


def test_ai_research_job_polling_stops_with_workspace_polling():
    ai_js = _read("web/static/js/ai_research.js")

    assert "const JOB_POLL_MAX_ATTEMPTS = 1200;" in ai_js
    assert "pollAttempts: 0" in ai_js
    assert "reason: 'job-status-max-attempts'" in ai_js
    assert "function stopAllJobPolling()" in ai_js

    stop_polling_section = ai_js.split("function stopPolling()", 1)[1].split(
        "function isAiAgentActive()",
        1,
    )[0]
    assert "stopAllJobPolling();" in stop_polling_section
