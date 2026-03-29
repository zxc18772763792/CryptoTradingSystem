from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (REPO_ROOT / rel_path).read_text(encoding="utf-8-sig")


def test_ai_research_template_loads_phase5_modules():
    template = _read("web/templates/index.html")

    assert 'id="ai-flow-console"' in template
    assert 'id="ai-chain-summary-grid"' in template
    assert 'id="ai-flow-stage-grid"' in template
    assert 'id="ai-planner-research-mode"' in template
    assert 'id="ai-planner-max-drafts"' in template
    assert 'id="ai-planner-max-backtests"' in template
    assert 'id="ai-oneclick-btn"' in template
    assert 'id="ai-oneclick-feedback"' in template
    assert 'id="ai-candidate-cards"' in template
    assert '新加坡时间 (SGT)' in template
    assert "/static/js/ai_research.js" in template
    assert "/static/js/ai_research_diagnostics.js" in template
    assert "/static/js/ai_research_runtime.js" in template
    assert "/static/js/ai_research_agent.js" in template
    assert "/static/js/ai_research_patch.js" not in template


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
    assert "function renderAgentStatusLoadError" in agent_js
    assert "执行成本" in agent_js
    assert "next_run_at" in agent_js
    assert "last_latency_ms" in agent_js
    assert "当前周期快照" in agent_js
    assert "startBtn.textContent = running ? '运行中' : '启动'" in agent_js
    assert "function selectProposal(" in ai_js
    assert "buildPlannerConstraints" in ai_js
    assert "withActionLock('oneclick'" in ai_js
    assert "buildOneClickFailureFeedback" in ai_js
    assert "buildOneClickSuccessFeedback" in ai_js
    assert "renderOneClickFeedback" in ai_js
    assert "liveDecisionActivityLastGood" in ai_js
    assert "沿用上次快照" in ai_js
    assert "FLOW_HINT_QUICK_PATH" in ai_js
    assert "AI_UI_TIMEZONE = 'Asia/Singapore'" in ai_js
    assert "AI_UI_TIMEZONE = 'Asia/Singapore'" in agent_js
    assert "const TRADING_STATS_TIMEOUT_MS=25000;" in app_js
    assert "const TRADING_POSITIONS_TIMEOUT_MS=30000;" in app_js
    assert "const TRADING_OPEN_ORDERS_TIMEOUT_MS=25000;" in app_js
    assert "modules.agent?.refresh?.({includeDetails:activeTab==='ai-agent'})" in app_js
    assert "else if(tab==='ai-research')refreshAiResearchModules();" in app_js
    assert "先点“3) 运行研究”单独验证" in ai_js
    assert "provider_fallback" in ai_js
    assert 'option value="codex">OpenAI' in template
    assert "先选研究任务，再点击候选策略卡片" in template
    assert "最后看最近决策日志与聚合信号快照" in template
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
