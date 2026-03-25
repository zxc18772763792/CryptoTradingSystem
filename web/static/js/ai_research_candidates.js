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

  function getSnapshot() {
    if (typeof aiRoot().getSnapshot === 'function') return aiRoot().getSnapshot() || {};
    return {};
  }

  function q(id) {
    return document.getElementById(id);
  }

  function score(value) {
    const n = Number(value);
    return Number.isFinite(n) ? n : 0;
  }

  function findSelectedProposal(snapshot) {
    const proposals = Array.isArray(snapshot?.proposals) ? snapshot.proposals : [];
    const selectedId = String(snapshot?.selectedProposalId || '').trim();
    if (selectedId) {
      const matched = proposals.find((item) => String(item?.proposal_id || '').trim() === selectedId);
      if (matched) return matched;
    }
    return proposals[0] || null;
  }

  function candidatePool(snapshot, proposal) {
    const candidates = Array.isArray(snapshot?.candidates) ? snapshot.candidates : [];
    const proposalId = String(proposal?.proposal_id || '').trim();
    if (!proposalId) return candidates.slice();
    return candidates.filter((item) => String(item?.proposal_id || '').trim() === proposalId);
  }

  function selectedCandidate(snapshot, pool) {
    const selectedId = String(snapshot?.selectedCandidateId || '').trim();
    if (selectedId) {
      const matched = pool.find((item) => String(item?.candidate_id || '').trim() === selectedId);
      if (matched) return matched;
    }
    return null;
  }

  function bestCandidate(pool) {
    return pool.slice().sort((left, right) => score(right?.score) - score(left?.score))[0] || null;
  }

  function championName(proposal, pool) {
    const summary = proposal?.metadata?.search_summary || {};
    const strategy = String(summary?.champion_strategy || '').trim();
    if (strategy) return strategy;
    return bestCandidate(pool)?.strategy || '--';
  }

  function candidateTop(candidate) {
    const rows = Array.isArray(candidate?.top_results) ? candidate.top_results : [];
    return rows[0] || null;
  }

  function searchRoleLabel(role) {
    const value = String(role || '').trim().toLowerCase();
    if (value === 'champion') return 'Champion';
    if (value === 'challenger') return 'Challenger';
    if (value === 'explorer') return 'Explorer';
    return 'Candidate';
  }

  function badge(text, tone = '') {
    return `<span class="ai-search-badge${tone ? ` is-${esc(tone)}` : ''}">${esc(text)}</span>`;
  }

  function miniBadge(text, tone = '') {
    return `<span class="ai-search-mini-badge${tone ? ` is-${esc(tone)}` : ''}">${esc(text)}</span>`;
  }

  function metricCard(label, value, note = '') {
    return `<div class="ai-search-metric-card">
      <span>${esc(label)}</span>
      <strong>${esc(value)}</strong>
      ${note ? `<div class="ai-search-health-note">${esc(note)}</div>` : ''}
    </div>`;
  }

  function healthRow(label, value, note, tone = '') {
    return `<div class="ai-search-health-row">
      <div>
        <div class="ai-search-health-label">${esc(label)}</div>
        <div class="ai-search-health-note">${esc(note)}</div>
      </div>
      ${miniBadge(value, tone)}
    </div>`;
  }

  function leaderboardRow(candidate, snapshot) {
    const top = candidateTop(candidate);
    const selected = String(snapshot?.selectedCandidateId || '').trim() === String(candidate?.candidate_id || '').trim();
    const roleLabel = searchRoleLabel(candidate?.metadata?.search_role);
    const roleTone = roleLabel === 'Champion' ? 'good' : roleLabel === 'Challenger' ? 'warn' : '';
    const status = statusText(candidate?.status);
    const sharpe = top?.sharpe_ratio != null ? Number(top.sharpe_ratio).toFixed(2) : '--';
    const ret = top?.total_return != null ? `${Number(top.total_return).toFixed(1)}%` : '--';
    return `<div class="ai-search-leader-row${selected ? ' is-selected' : ''}">
      <div class="ai-search-leader-meta">
        <div class="ai-search-leader-title">${esc(candidate?.strategy || '--')}</div>
        <div class="ai-search-leader-subtitle">
          <span>${esc(candidate?.symbol || '--')}</span>
          <span>${esc(candidate?.timeframe || '--')}</span>
          <span>Sharpe ${esc(sharpe)}</span>
          <span>收益 ${esc(ret)}</span>
        </div>
      </div>
      <div class="ai-search-leader-actions">
        ${miniBadge(roleLabel, roleTone)}
        ${miniBadge(status)}
        <button class="btn btn-sm" data-action="view-overview-candidate" data-candidate-id="${esc(candidate?.candidate_id || '')}">查看</button>
      </div>
    </div>`;
  }

  function renderOverview(detail = {}) {
    const root = q('ai-search-overview-card');
    const subtitleEl = q('ai-search-overview-subtitle');
    const badgeEl = q('ai-search-overview-badges');
    const metricsEl = q('ai-search-overview-metrics');
    const leaderboardEl = q('ai-search-overview-leaderboard');
    const healthEl = q('ai-search-overview-health');
    if (!root || !subtitleEl || !badgeEl || !metricsEl || !leaderboardEl || !healthEl) return;

    const snapshot = detail?.snapshot || getSnapshot();
    const proposal = findSelectedProposal(snapshot);
    const pool = candidatePool(snapshot, proposal);
    const best = bestCandidate(pool);
    const pendingApprovals = Array.isArray(snapshot?.pendingApprovals) ? snapshot.pendingApprovals.length : 0;
    const running = pool.filter((item) => ['paper_running', 'shadow_running', 'live_candidate', 'live_running'].includes(String(item?.status || '').trim())).length;
    const summary = proposal?.metadata?.search_summary || {};
    const budget = proposal?.metadata?.search_budget || {};
    const avgScore = pool.length ? (pool.reduce((acc, item) => acc + score(item?.score), 0) / pool.length).toFixed(1) : '--';
    const subtitle = proposal
      ? `${proposalDisplayName(proposal)} · ${researchModeText(proposal?.metadata?.research_mode || 'template')} · ${pool.length} 个候选`
      : '等待研究提案与候选结果进入工作台。';
    subtitleEl.textContent = subtitle;

    badgeEl.innerHTML = [
      proposal ? badge(`提案 ${proposalDisplayName(proposal)}`, 'info') : null,
      pool.length ? badge(`候选 ${pool.length}`, 'on') : badge('暂无候选'),
      pendingApprovals > 0 ? badge(`待审批 ${pendingApprovals}`, 'warn') : null,
      running > 0 ? badge(`运行中 ${running}`, 'on') : null,
    ].filter(Boolean).join('');

    metricsEl.innerHTML = [
      metricCard('Champion', championName(proposal, pool), summary?.champion_reason || '当前批次综合最优策略'),
      metricCard('平均评分', avgScore, pool.length ? `基于 ${pool.length} 个候选` : '等待候选生成'),
      metricCard('搜索预算', budget?.max_backtest_runs || '--', '最大回测轮数'),
      metricCard('淘汰数量', summary?.rejected_count || 0, '来自搜索摘要的已淘汰草案'),
    ].join('');

    if (!pool.length) {
      leaderboardEl.innerHTML = '<div class="ai-search-empty">当前提案还没有产出候选，先运行研究任务。</div>';
    } else {
      leaderboardEl.innerHTML = pool
        .slice()
        .sort((left, right) => score(right?.score) - score(left?.score))
        .slice(0, 5)
        .map((candidate) => leaderboardRow(candidate, snapshot))
        .join('');
    }

    const selected = selectedCandidate(snapshot, pool) || best;
    const healthRows = [];
    if (selected) {
      healthRows.push(
        healthRow(
          '当前聚焦',
          `${score(selected?.score).toFixed(0)} 分`,
          `${selected?.strategy || '--'} · ${statusText(selected?.status)}`,
          score(selected?.score) >= 70 ? 'good' : score(selected?.score) >= 50 ? 'warn' : '',
        ),
      );
    }
    healthRows.push(
      healthRow(
        '下一步',
        pendingApprovals > 0 ? '审批' : running > 0 ? '观察' : pool.length ? '筛选' : '运行',
        pendingApprovals > 0
          ? `有 ${pendingApprovals} 个候选等待治理审批。`
          : running > 0
            ? `已有 ${running} 个候选进入运行态，继续观察自治代理与 runtime。`
            : pool.length
              ? '优先点击排行榜中的高分候选，检查验证与谱系。'
              : '先运行研究任务，让系统开始 hypothesis -> search -> validate。',
        pendingApprovals > 0 ? 'warn' : running > 0 ? 'good' : '',
      ),
    );
    healthRows.push(
      healthRow(
        '搜索健康',
        `${summary?.rejected_count || 0} 淘汰`,
        summary?.mutation_notes
          ? String(summary.mutation_notes)
          : `budget ${budget?.max_backtest_runs || '--'} / 候选 ${pool.length}`,
        pool.length >= 3 ? 'good' : '',
      ),
    );
    healthEl.innerHTML = healthRows.join('');
  }

  function bindEvents() {
    q('ai-search-overview-card')?.addEventListener('click', (event) => {
      const btn = event.target.closest('[data-action="view-overview-candidate"][data-candidate-id]');
      if (!btn) return;
      const candidateId = String(btn.dataset.candidateId || '').trim();
      if (!candidateId) return;
      if (typeof aiRoot().viewCandidate === 'function') {
        aiRoot().viewCandidate(candidateId);
      }
    });
  }

  function init() {
    if (!q('ai-search-overview-card')) return;
    const modules = aiRoot().modules || {};
    modules.candidates = { render: (detail = {}) => renderOverview(detail) };
    aiRoot().modules = modules;
    bindEvents();
    window.addEventListener('ai-research:state', (event) => renderOverview(event.detail || {}));
    renderOverview();
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
