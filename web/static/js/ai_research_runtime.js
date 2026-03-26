(function () {
  'use strict';

  function aiRoot() {
    return window.AI || {};
  }

  function esc(value) {
    if (typeof aiRoot().util?.esc === 'function') return aiRoot().util.esc(value);
    return String(value ?? '').replace(/[&<>"']/g, (match) => (
      { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[match]
    ));
  }

  function providerDisplayName(provider) {
    if (typeof aiRoot().util?.providerDisplayName === 'function') {
      return aiRoot().util.providerDisplayName(provider);
    }
    const value = String(provider || '').trim().toLowerCase();
    if (value === 'codex' || value === 'openai') return 'OpenAI';
    return String(provider || '-');
  }

  function statusText(status) {
    if (typeof aiRoot().util?.statusText === 'function') return aiRoot().util.statusText(status);
    return String(status || '--');
  }

  function proposalDisplayName(item, index = 0) {
    if (typeof aiRoot().util?.proposalDisplayName === 'function') {
      return aiRoot().util.proposalDisplayName(item, index);
    }
    return String(item?.proposal_id || `proposal-${index + 1}`);
  }

  function researchModeText(mode) {
    if (typeof aiRoot().util?.researchModeText === 'function') return aiRoot().util.researchModeText(mode);
    return String(mode || 'template');
  }

  function tradingModeLabel(mode) {
    const value = String(mode || '').trim().toLowerCase();
    if (value === 'live') return '实盘';
    if (value === 'paper') return '纸盘';
    return String(mode || '--');
  }

  function decisionModeLabel(mode) {
    const value = String(mode || '').trim().toLowerCase();
    if (value === 'shadow') return '只提示';
    if (value === 'enforce') return '可拦截';
    if (value === 'execute') return '直接执行';
    return String(mode || '--');
  }

  function getSnapshot() {
    if (typeof aiRoot().getSnapshot === 'function') return aiRoot().getSnapshot() || {};
    return {};
  }

  function q(id) {
    return document.getElementById(id);
  }

  function plannerInputs() {
    return {
      goal: String(q('ai-planner-goal')?.value || '').trim(),
      regime: String(q('ai-planner-regime')?.value || 'mixed').trim(),
      maxTemplates: Math.max(1, Number(q('ai-planner-max-templates')?.value || 5)),
      researchMode: String(q('ai-planner-research-mode')?.value || 'auto').trim() || 'auto',
      maxDrafts: Math.max(1, Number(q('ai-planner-max-drafts')?.value || 4)),
      maxBacktests: Math.max(8, Number(q('ai-planner-max-backtests')?.value || 80)),
      explorationBias: Math.max(0, Math.min(1, Number(q('ai-planner-exploration-bias')?.value || 0.45))),
      symbols: String(q('ai-planner-symbols')?.value || '').split(',').map((item) => item.trim()).filter(Boolean),
      timeframes: String(q('ai-planner-timeframes')?.value || '').split(',').map((item) => item.trim()).filter(Boolean),
    };
  }

  function proposalMeta(proposal) {
    return proposal?.metadata || {};
  }

  function candidateResultTop(candidate) {
    const topRows = Array.isArray(candidate?.top_results) ? candidate.top_results : [];
    return topRows.length ? topRows[0] : null;
  }

  function findSelectedCandidateRecord(snapshot) {
    const selectedId = String(snapshot?.selectedCandidateId || '').trim();
    if (!selectedId || !Array.isArray(snapshot?.candidates)) return null;
    return snapshot.candidates.find((item) => String(item?.candidate_id || '').trim() === selectedId) || null;
  }

  function virtualProposalFromCandidate(candidate) {
    const proposalId = String(candidate?.proposal_id || '').trim();
    if (!proposalId) return null;
    const proposalName = String(candidate?.metadata?.proposal_display_name || '').trim()
      || `候选链路 · ${String(candidate?.strategy || '--')} @ ${String(candidate?.symbol || '--')} ${String(candidate?.timeframe || '--')}`;
    return {
      proposal_id: proposalId,
      thesis: String(candidate?.metadata?.thesis || candidate?.strategy || proposalName).trim(),
      research_mode: String(candidate?.metadata?.research_mode || 'template').trim() || 'template',
      metadata: {
        display_name: proposalName,
        search_summary: candidate?.metadata?.search_summary || {},
        search_budget: candidate?.metadata?.search_budget || {},
        strategy_drafts: candidate?.metadata?.strategy_drafts || [],
        virtual_context: true,
      },
    };
  }

  function findSelectedProposal(snapshot) {
    const items = Array.isArray(snapshot?.proposals) ? snapshot.proposals : [];
    const selectedId = String(snapshot?.selectedProposalId || '').trim();
    if (selectedId) {
      const matched = items.find((item) => String(item?.proposal_id || '').trim() === selectedId);
      if (matched) return matched;
    }
    const selectedCandidate = findSelectedCandidateRecord(snapshot);
    if (selectedCandidate) {
      const candidateProposalId = String(selectedCandidate?.proposal_id || '').trim();
      if (candidateProposalId) {
        const matched = items.find((item) => String(item?.proposal_id || '').trim() === candidateProposalId);
        if (matched) return matched;
        const virtualProposal = virtualProposalFromCandidate(selectedCandidate);
        if (virtualProposal) return virtualProposal;
      }
    }
    return items[0] || null;
  }

  function candidatePoolForProposal(snapshot, proposal) {
    const items = Array.isArray(snapshot?.candidates) ? snapshot.candidates : [];
    const proposalId = String(proposal?.proposal_id || '').trim();
    if (!proposalId) return items;
    return items.filter((item) => String(item?.proposal_id || '').trim() === proposalId);
  }

  function findSelectedCandidate(snapshot, proposal) {
    const items = candidatePoolForProposal(snapshot, proposal);
    const selectedId = String(snapshot?.selectedCandidateId || '').trim();
    if (selectedId) {
      const matched = items.find((item) => String(item?.candidate_id || '').trim() === selectedId)
        || (Array.isArray(snapshot?.candidates) ? snapshot.candidates.find((item) => String(item?.candidate_id || '').trim() === selectedId) : null);
      if (matched) return matched;
    }
    return [...items].sort((left, right) => Number(right?.score || 0) - Number(left?.score || 0))[0] || null;
  }

  function countRunningCandidates(snapshot) {
    const items = Array.isArray(snapshot?.candidates) ? snapshot.candidates : [];
    const running = new Set(['paper_running', 'shadow_running', 'live_candidate', 'live_running']);
    return items.filter((item) => running.has(String(item?.status || '').trim())).length;
  }

  function nextStepText(snapshot, proposal, candidate) {
    const pendingApprovals = Array.isArray(snapshot?.pendingApprovals) ? snapshot.pendingApprovals.length : 0;
    if (!proposal) return '建议先做第 1 步“生成研究思路”，再执行第 2 步“生成提案”。';
    if (['research_queued', 'research_running'].includes(String(proposal?.status || ''))) {
      return '第 3 步“运行研究”进行中，先等待候选与验证结果。';
    }
    if (!candidate) return '第 2 步提案已就绪，下一步执行第 3 步“运行研究”。';
    if (pendingApprovals > 0) return `进入第 4 步“注册/部署”，当前有 ${pendingApprovals} 个候选待人工确认。`;
    if (['paper_running', 'shadow_running', 'live_candidate', 'live_running'].includes(String(candidate?.status || ''))) {
      return '已进入第 4 步“注册/部署”，可以继续观察运行表现。';
    }
    return '查看候选详情后，在右侧完成第 4 步“注册/部署”。';
  }

  function focusCandidateText(candidate) {
    if (!candidate) return '暂无候选';
    const score = Number(candidate?.score);
    const status = statusText(candidate?.status || '--');
    return Number.isFinite(score)
      ? `${candidate?.strategy || '--'} / ${status} / ${Math.round(score)}分`
      : `${candidate?.strategy || '--'} / ${status}`;
  }

  function requestedResearchModeText(requestedMode) {
    return String(requestedMode || '').trim() === 'auto'
      ? '自动判断'
      : researchModeText(requestedMode || 'template');
  }

  function researchStageText(proposal, candidate) {
    const proposalStatus = String(proposal?.status || '').trim();
    const candidateStatus = String(candidate?.status || '').trim();
    if (!proposal) return '待生成提案';
    if (['research_queued', 'research_running'].includes(proposalStatus)) return '研究运行中';
    if (!candidate) return '待产出候选';
    if (['rejected'].includes(candidateStatus) || ['rejected'].includes(proposalStatus)) return '验证未通过';
    if (['paper_running', 'shadow_running', 'live_candidate', 'live_running'].includes(candidateStatus)) return '已进入注册/部署';
    if (candidateStatus === 'validated') return '候选待注册/部署';
    return statusText(candidateStatus || proposalStatus || '--');
  }

  function researchOutputText(proposal, candidate, strategyDrafts, proposalCandidates) {
    if (candidate) {
      const score = Number(candidate?.score);
      return Number.isFinite(score)
        ? `${candidate?.strategy || '--'} / ${Math.round(score)}分`
        : `${candidate?.strategy || '--'} / ${statusText(candidate?.status || '--')}`;
    }
    if (proposal) {
      const draftCount = Array.isArray(strategyDrafts) ? strategyDrafts.length : 0;
      const candidateCount = Array.isArray(proposalCandidates) ? proposalCandidates.length : 0;
      return `${candidateCount} 个候选 / ${draftCount} 个草案`;
    }
    return '等待生成研究';
  }

  function setChainSummaryText(id, value) {
    const el = q(id);
    if (!el) return;
    el.textContent = String(value || '--');
  }

  function setChainSummaryTag(id, value, tone = '') {
    const el = q(id);
    if (!el) return;
    el.textContent = String(value || '--');
    el.className = `ai-chain-summary-tag${tone ? ` is-${tone}` : ''}`;
  }

  function renderChainSummary(model) {
    const summary = model?.chainSummary || {};
    const research = summary.research || {};
    setChainSummaryTag('ai-chain-research-tag', research.tag || '等待研究', research.tone || '');
    setChainSummaryText('ai-chain-research-mode', research.mode || '等待配置');
    setChainSummaryText('ai-chain-research-stage', research.stage || '待生成');
    setChainSummaryText('ai-chain-research-output', research.output || '暂无产物');
    setChainSummaryText('ai-chain-research-next', research.next || '等待下一步');
  }

  function metric(label, value) {
    return `<div class="ai-flow-metric"><span>${esc(label)}</span><strong>${esc(value)}</strong></div>`;
  }

  function stageCard(index, title, tone, primary, metrics, note) {
    return `<article class="ai-flow-stage ${esc(`is-${tone}`)}">
      <div class="ai-flow-stage-index">Step ${index}</div>
      <div class="ai-flow-stage-title">${esc(title)}</div>
      <div class="ai-flow-stage-primary">${esc(primary)}</div>
      <div class="ai-flow-stage-metrics">${metrics.join('')}</div>
      <div class="ai-flow-stage-note">${esc(note)}</div>
    </article>`;
  }

  function buildFlowModel(snapshot) {
    const proposal = findSelectedProposal(snapshot);
    const candidate = findSelectedCandidate(snapshot, proposal);
    const runtime = snapshot?.runtimeConfig || {};
    const liveDecision = runtime?.ai_live_decision || {};
    const agent = runtime?.ai_autonomous_agent || {};
    const pendingApprovals = Array.isArray(snapshot?.pendingApprovals) ? snapshot.pendingApprovals.length : 0;
    const runningCandidates = countRunningCandidates(snapshot);
    const inputs = plannerInputs();
    const meta = proposalMeta(proposal);
    const searchSummary = meta?.search_summary || {};
    const searchBudget = meta?.search_budget || {};
    const strategyDrafts = Array.isArray(meta?.strategy_drafts) ? meta.strategy_drafts : [];
    const topRow = candidateResultTop(candidate);
    const validation = candidate?.validation_summary || {};
    const proposalCandidates = candidatePoolForProposal(snapshot, proposal);
    const nextAction = nextStepText(snapshot, proposal, candidate);
    const effectiveResearchMode = String(proposal?.research_mode || meta?.research_mode || '').trim() || (inputs.researchMode === 'auto' ? 'template' : inputs.researchMode);

    const hypothesisTone = proposal
      ? (['research_running', 'research_queued'].includes(String(proposal?.status || '')) ? 'active' : 'done')
      : (inputs.goal ? 'active' : 'pending');
    const searchTone = proposal
      ? (proposalCandidates.length > 0 ? 'done' : ['research_running', 'research_queued'].includes(String(proposal?.status || '')) ? 'active' : 'pending')
      : 'pending';
    const validationTone = candidate
      ? (String(proposal?.status || '') === 'rejected' || String(candidate?.status || '') === 'rejected' ? 'blocked' : 'done')
      : (proposal ? 'active' : 'pending');
    const reviewTone = liveDecision?.enabled ? 'active' : (candidate ? 'done' : 'pending');
    const deploymentTone = pendingApprovals > 0
      ? 'blocked'
      : (runningCandidates > 0 ? 'done' : (candidate ? 'active' : 'pending'));

    const badges = [
      proposal ? { text: `提案 ${proposalDisplayName(proposal)}`, tone: 'done' } : null,
      candidate ? { text: `候选 ${candidate?.strategy || '--'}`, tone: 'active' } : null,
      liveDecision?.enabled ? { text: `下单前复核 ${decisionModeLabel(liveDecision?.mode || 'shadow')}`, tone: 'done' } : null,
      pendingApprovals > 0 ? { text: `待确认 ${pendingApprovals}`, tone: 'warn' } : null,
    ].filter(Boolean);

    const stages = [
      stageCard(
        1,
        '研究思路',
        hypothesisTone,
        proposal?.thesis || inputs.goal || '等待输入研究目标',
        [
          metric('模式', researchModeText(effectiveResearchMode)),
          metric('市场', inputs.regime || 'mixed'),
          metric('模板上限', inputs.maxTemplates),
        ],
        proposal ? `当前状态：${statusText(proposal?.status)}` : '目标会先被结构化成研究假设与实验计划。',
      ),
      stageCard(
        2,
        '生成提案',
        searchTone,
        proposalCandidates.length > 0
          ? `${proposalCandidates.length} 个候选 / champion ${searchSummary?.champion_strategy || candidate?.strategy || '--'}`
          : '尚未产出候选',
        [
          metric('草案', strategyDrafts.length || '--'),
          metric('回测预算', searchBudget?.max_backtest_runs || inputs.maxBacktests || '--'),
          metric('淘汰', searchSummary?.rejected_drafts || 0),
        ],
        proposal ? '系统会保留 champion / challenger 关系与搜索摘要。' : '先生成提案，才能开始 hypothesis -> evaluate -> mutate。',
      ),
      stageCard(
        3,
        '运行研究',
        validationTone,
        candidate
          ? `${candidate?.strategy || '--'} / ${Number(candidate?.score || 0).toFixed(0)} 分`
          : '等待候选进入验证',
        [
          metric('OOS', validation?.oos_score != null ? Number(validation.oos_score).toFixed(2) : '--'),
          metric('WF', validation?.wf_stability != null ? `${(Number(validation.wf_stability) * 100).toFixed(0)}%` : '--'),
          metric('回撤', topRow?.max_drawdown != null ? `${Number(topRow.max_drawdown).toFixed(1)}%` : '--'),
        ],
        candidate ? '优先看 OOS、WF 稳定性与回撤，再决定是否推进。' : '没有候选时，这一段会保持待验证状态。',
      ),
      stageCard(
        4,
        '注册前保护',
        reviewTone,
        `${providerDisplayName(liveDecision?.provider || 'codex')} / ${liveDecision?.model || 'default'}`,
        [
          metric('人工确认', runtime?.governance_enabled ? '开启' : '关闭'),
          metric('下单前AI复核', liveDecision?.enabled ? decisionModeLabel(liveDecision?.mode || 'shadow') : '关闭'),
          metric('自动交易代理', agent?.enabled ? '启用' : '关闭'),
        ],
        candidate
          ? `当前聚焦候选：${candidate?.strategy || '--'}。这一步展示的是研究结果上线前后的保护和增强，不负责生成新策略。`
          : '没有候选时，这里只显示上线前后的保护配置，不会形成研究闭环。',
      ),
      stageCard(
        5,
        '注册/部署',
        deploymentTone,
        runningCandidates > 0
          ? `${runningCandidates} 个候选处于运行态`
          : pendingApprovals > 0
            ? `${pendingApprovals} 个候选待人工确认`
            : '尚未进入部署阶段',
        [
          metric('交易模式', tradingModeLabel(runtime?.trading_mode || '--')),
          metric('候选状态', statusText(candidate?.status || proposal?.status || '--')),
          metric('运行中', runningCandidates),
        ],
        nextAction,
      ),
    ];

    return {
      subtitle: nextAction,
      badges,
      stages,
      focus: {
        proposal: proposal ? proposalDisplayName(proposal) : '未选研究任务',
        candidate: focusCandidateText(candidate),
        nextAction,
      },
      chainSummary: {
        research: {
          tag: proposal ? researchModeText(effectiveResearchMode) : requestedResearchModeText(inputs.researchMode),
          tone: proposal ? (effectiveResearchMode === 'autonomous_draft' ? 'active' : 'on') : 'warn',
          mode: proposal
            ? `${researchModeText(effectiveResearchMode)} / 模板上限 ${searchBudget?.max_templates || inputs.maxTemplates || '--'}`
            : `${requestedResearchModeText(inputs.researchMode)} / 模板上限 ${inputs.maxTemplates}`,
          stage: researchStageText(proposal, candidate),
          output: researchOutputText(proposal, candidate, strategyDrafts, proposalCandidates),
          next: nextAction,
        },
      },
    };
  }

  function renderFocus(model) {
    const proposalEl = q('ai-focus-proposal');
    const candidateEl = q('ai-focus-candidate');
    const nextEl = q('ai-focus-next-action');
    if (!proposalEl || !candidateEl || !nextEl) return;
    const focus = model?.focus || {};
    proposalEl.textContent = String(focus.proposal || '未选研究任务');
    candidateEl.textContent = String(focus.candidate || '暂无候选');
    nextEl.textContent = String(focus.nextAction || '等待研究状态');
  }

  function renderFlow(detail = {}) {
    const root = q('ai-flow-console');
    const subtitleEl = q('ai-flow-console-subtitle');
    const badgeEl = q('ai-flow-badges');
    const stageEl = q('ai-flow-stage-grid');
    if (!root || !subtitleEl || !badgeEl || !stageEl) return;
    const snapshot = detail?.snapshot || getSnapshot();
    const model = buildFlowModel(snapshot);
    subtitleEl.textContent = model.subtitle;
    badgeEl.innerHTML = model.badges.length
      ? model.badges.map((badge) => `<span class="ai-flow-badge is-${esc(badge.tone)}">${esc(badge.text)}</span>`).join('')
      : '<span class="ai-flow-badge">等待研究状态</span>';
    stageEl.innerHTML = model.stages.join('');
    renderChainSummary(model);
    renderFocus(model);
  }

  function bindPlannerInputs() {
    ['ai-planner-goal', 'ai-planner-regime', 'ai-planner-max-templates', 'ai-planner-research-mode', 'ai-planner-max-drafts', 'ai-planner-max-backtests', 'ai-planner-exploration-bias', 'ai-planner-symbols', 'ai-planner-timeframes']
      .forEach((id) => {
        q(id)?.addEventListener('input', () => renderFlow());
        q(id)?.addEventListener('change', () => renderFlow());
      });
  }

  function init() {
    if (!q('ai-flow-console')) return;
    const modules = aiRoot().modules || {};
    modules.runtime = { render: () => renderFlow() };
    aiRoot().modules = modules;
    window.addEventListener('ai-research:state', (event) => renderFlow(event.detail || {}));
    bindPlannerInputs();
    renderFlow();
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
