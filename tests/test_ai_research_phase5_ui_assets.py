from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (REPO_ROOT / rel_path).read_text(encoding="utf-8-sig")


def test_ai_research_template_loads_phase5_modules():
    template = _read("web/templates/index.html")
    news_template = _read("web/templates/news.html")

    assert 'id="ai-flow-console"' in template
    assert 'id="ai-chain-summary-grid"' in template
    assert 'id="ai-flow-stage-grid"' in template
    assert 'id="ai-planner-research-mode"' in template
    assert 'id="ai-planner-max-drafts"' in template
    assert 'id="ai-planner-max-backtests"' in template
    assert 'id="ai-oneclick-btn"' in template
    assert 'id="ai-oneclick-feedback"' in template
    assert 'id="ai-candidate-cards"' in template
    assert 'id="ai-queue-title"' in template
    assert 'id="ai-queue-hint"' in template
    assert '页面时区：上海时间 (UTC+8)' in template
    assert '页面时区：上海时间 (UTC+8)' in news_template
    assert '/static/favicon.svg' in template
    assert '/static/favicon.svg' in news_template
    assert "/static/js/ai_research.js" in template
    assert "/static/js/ai_research_diagnostics.js" in template
    assert "/static/js/ai_research_runtime.js" in template
    assert "/static/js/ai_research_agent.js" in template
    assert "/static/js/ai_research_patch.js" not in template
    assert '/static/js/news_tab_runtime.js?v=14' in news_template


def test_ai_research_phase5_assets_exist_and_define_flow_styles():
    diagnostics_js = _read("web/static/js/ai_research_diagnostics.js")
    runtime_js = _read("web/static/js/ai_research_runtime.js")
    candidates_js = _read("web/static/js/ai_research_candidates.js")
    agent_js = _read("web/static/js/ai_research_agent.js")
    ai_js = _read("web/static/js/ai_research.js")
    app_js = _read("web/static/js/app.js")
    template = _read("web/templates/index.html")
    style_css = _read("web/static/css/style.css")

    assert "modules.diagnostics" in diagnostics_js
    assert "ai-flow-stage-grid" in runtime_js
    assert "renderChainSummary" in runtime_js
    assert "modules.candidates" in candidates_js
    assert "window.agentStart = agentStart" in agent_js
    assert "renderAgentChainSummary" in agent_js
    assert "buildAgentJournalCurrentSummary" in agent_js
    assert "summarizeAggregatedSignal" in agent_js
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
    assert "function isVirtualProposal(" in ai_js
    assert "function autoSelectCandidateForProposal(" in ai_js
    assert "buildPlannerConstraints" in ai_js
    assert "withActionLock('oneclick'" in ai_js
    assert "buildOneClickFailureFeedback" in ai_js
    assert "buildOneClickSuccessFeedback" in ai_js
    assert "renderOneClickFeedback" in ai_js
    assert "liveDecisionActivityLastGood" in ai_js
    assert "FLOW_HINT_QUICK_PATH" in ai_js
    assert "候选回填" in ai_js
    assert "该条目由候选结果回填" in ai_js
    assert "Asia/Shanghai" in ai_js
    assert "Asia/Shanghai" in agent_js
    assert "window.CTS_UI_TIMEZONE" in ai_js
    assert "window.CTS_UI_TIMEZONE_LABEL" in ai_js
    assert "const TIME_ZONE='Asia/Shanghai';" in app_js
    assert "const TRADING_STATS_TIMEOUT_MS=25000;" in app_js
    assert "const TRADING_POSITIONS_TIMEOUT_MS=30000;" in app_js
    assert "const TRADING_OPEN_ORDERS_TIMEOUT_MS=25000;" in app_js
    assert "modules.agent?.refresh?.({includeDetails:activeTab==='ai-agent'})" in app_js
    assert "else if(tab==='ai-research')refreshAiResearchModules();" in app_js
    assert "provider_fallback" in ai_js
    assert 'option value="codex">OpenAI' in template
    assert ".ai-flow-console" in style_css
    assert ".ai-chain-summary-grid" in style_css
    assert ".ai-flow-stage-grid" in style_css
    assert ".ai-candidate-cards" in style_css
    assert ".ai-oneclick-entry-card" in style_css
    assert ".ai-oneclick-feedback" in style_css
    assert ".ai-review-panel select" in style_css
    assert ".agent-journal-current" in style_css
    assert ".agent-journal-signal" in style_css
    assert "appearance: none" in style_css
    assert "color-scheme: dark" in style_css
    assert '[data-tone="warn"]' in style_css
