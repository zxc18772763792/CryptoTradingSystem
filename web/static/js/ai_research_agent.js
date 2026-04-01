(function () {
  'use strict';

  const POLL_MS = 30000;
  const AI_UI_TIMEZONE = 'Asia/Singapore';
  const AGENT_STATUS_API = '/ai/autonomous-agent/status';
  const AGENT_JOURNAL_API = '/ai/autonomous-agent/journal';
  const AGENT_REVIEW_API = '/ai/autonomous-agent/review';
  const AGENT_START_API = '/ai/autonomous-agent/start';
  const AGENT_STOP_API = '/ai/autonomous-agent/stop';
  const AGENT_RUN_ONCE_API = '/ai/autonomous-agent/run-once';
  const AGENT_CONFIG_API = '/ai/runtime-config/autonomous-agent';
  const AGENT_SYMBOL_RANKING_API = '/ai/autonomous-agent/symbol-ranking';

  let pollTimer = null;
  let lastStatusSnapshot = null;
  let lastConfigSnapshot = null;
  let statusInFlight = null;

  function aiRoot() {
    return window.AI || {};
  }

  function esc(value) {
    if (typeof aiRoot().util?.esc === 'function') return aiRoot().util.esc(value);
    return String(value ?? '').replace(/[&<>"']/g, (match) => (
      { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[match]
    ));
  }

  function notify(message, isError = false) {
    if (typeof aiRoot().util?.notify === 'function') {
      aiRoot().util.notify(message, isError);
      return;
    }
    if (isError) console.error(message);
    else console.log(message);
  }

  function compactText(value, maxLen = 120) {
    if (value == null) return '';
    let text = '';
    if (typeof value === 'string') text = value;
    else if (typeof value === 'number' || typeof value === 'boolean') text = String(value);
    else {
      try {
        text = JSON.stringify(value);
      } catch (_) {
        text = String(value);
      }
    }
    return text.length > maxLen ? `${text.slice(0, maxLen - 1)}...` : text;
  }

  function fmtAgentTs(value) {
    if (!value) return '--';
    try {
      const date = new Date(value);
      if (!Number.isFinite(date.getTime())) return String(value || '--');
      return date.toLocaleString('zh-CN', {
        hour12: false,
        timeZone: AI_UI_TIMEZONE,
      });
    } catch (_) {
      return String(value || '--');
    }
  }

  function providerDisplayName(provider) {
    if (typeof aiRoot().util?.providerDisplayName === 'function') {
      return aiRoot().util.providerDisplayName(provider);
    }
    const value = String(provider || '').trim().toLowerCase();
    if (value === 'codex' || value === 'openai') return 'OpenAI';
    if (value === 'claude') return 'Claude';
    if (value === 'glm') return 'GLM';
    return String(provider || '-');
  }

  function decisionModeLabel(mode) {
    const value = String(mode || '').trim().toLowerCase();
    if (value === 'shadow') return '只提示';
    if (value === 'execute') return '直接执行';
    if (value === 'enforce') return '可拦截';
    return String(mode || '--');
  }

  function decisionActionText(action) {
    return {
      buy: '买入',
      sell: '卖出',
      close_long: '平多',
      close_short: '平空',
      hold: '观望',
    }[String(action || '').trim().toLowerCase()] || String(action || '--');
  }

  function modelOutputSourceText(source) {
    const value = String(source || '').trim().toLowerCase();
    if (value === 'provider') return '模型原始输出';
    if (value === 'fallback') return '本地兜底输出';
    return '本地合成输出';
  }

  function symbolSelectionReasonText(reason) {
    return {
      manual_symbol: '固定币种',
      existing_position_priority: '优先处理已有持仓',
      top_ranked_tradable_symbol: '当前最优可交易币种',
      top_ranked_watchlist_symbol: '当前最高分观察币种',
    }[String(reason || '').trim().toLowerCase()] || String(reason || '--');
  }

  function symbolModeLabel(mode) {
    return String(mode || '').trim().toLowerCase() === 'auto' ? '自动选币' : '固定币种';
  }

  function toneClass(tone) {
    const value = String(tone || '').trim().toLowerCase();
    if (value === 'danger' || value === 'error') return 'is-danger';
    if (value === 'warn' || value === 'warning') return 'is-warn';
    if (value === 'good' || value === 'success') return 'is-good';
    return 'is-info';
  }

  function reasonKey(item = {}) {
    const label = String(item.label || item.code || '').trim().toLowerCase();
    const detail = String(item.detail || '').trim().toLowerCase();
    return `${label}::${detail}`;
  }

  function formatRatio(value, digits = 3) {
    const num = Number(value);
    if (!Number.isFinite(num)) return '--';
    return num.toFixed(digits);
  }

  function formatDurationMinutes(seconds) {
    const value = Number(seconds);
    if (!Number.isFinite(value) || value <= 0) return '--';
    if (value < 60) return `${Math.round(value)}s`;
    return `${(value / 60).toFixed(value >= 600 ? 0 : 1)} min`;
  }

  function formatNumber(value, digits = 2) {
    const number = Number(value);
    if (!Number.isFinite(number)) return '--';
    return number.toFixed(digits);
  }

  function formatSigned(value, digits = 2, suffix = '') {
    const number = Number(value);
    if (!Number.isFinite(number)) return '--';
    const prefix = number > 0 ? '+' : '';
    return `${prefix}${number.toFixed(digits)}${suffix}`;
  }

  function formatPct(value, digits = 0) {
    const number = Number(value);
    if (!Number.isFinite(number)) return '--';
    return `${(number * 100).toFixed(digits)}%`;
  }

  function formatLatencyMs(value) {
    const number = Number(value);
    if (!Number.isFinite(number) || number < 0) return '--';
    return number >= 10000 ? `${(number / 1000).toFixed(1)}s` : `${number.toFixed(0)}ms`;
  }

  function summarizeAggregatedSignal(signal = {}) {
    const direction = String(signal.direction || 'FLAT').trim().toUpperCase() || 'FLAT';
    const parts = [`聚合 ${direction} ${formatPct(signal.confidence, 0)}`];
    const components = signal.components && typeof signal.components === 'object' ? signal.components : {};
    const componentParts = ['llm', 'ml', 'factor']
      .map((key) => {
        const item = components[key] || {};
        const itemDirection = String(item.direction || 'FLAT').trim().toUpperCase() || 'FLAT';
        const itemConfidence = Number(item.confidence);
        if (!Number.isFinite(itemConfidence)) return '';
        return `${key.toUpperCase()} ${itemDirection} ${formatPct(itemConfidence, 0)}`;
      })
      .filter(Boolean);
    if (componentParts.length) parts.push(componentParts.join(' / '));
    if (signal.blocked_by_risk) {
      parts.push(`风控 ${compactText(signal.risk_reason || 'blocked', 48)}`);
    }
    return parts.join(' · ');
  }

  function buildAgentJournalCurrentSummary(status = {}, cfg = {}) {
    const diagnostics = status.last_diagnostics || {};
    const aggregatedSignal = diagnostics.aggregated_signal || status.last_execution?.signal || {};
    const summaryParts = [];
    if (aggregatedSignal && Object.keys(aggregatedSignal).length) {
      summaryParts.push(summarizeAggregatedSignal(aggregatedSignal));
    }
    const latencyText = formatLatencyMs(status.last_latency_ms);
    if (latencyText !== '--') summaryParts.push(`上次耗时 ${latencyText}`);
    const intervalSec = Number(cfg.interval_sec || 0);
    if (intervalSec > 0) summaryParts.push(`目标周期 ${intervalSec}s`);
    if (status.next_run_at) summaryParts.push(`下次计划 ${fmtAgentTs(status.next_run_at)}`);
    const subtitle = summaryParts.length ? summaryParts.join(' · ') : '等待首轮决策后显示聚合信号快照';
    return `
      <div class="agent-journal-current">
        <div class="agent-journal-current-title">当前周期快照</div>
        <div class="agent-journal-current-body">${esc(subtitle)}</div>
      </div>
    `;
  }

  function reviewToneClass(tone) {
    return toneClass(tone || 'info');
  }

  function describeModelFeedback(status = {}, diagnostics = {}) {
    const feedback = diagnostics.model_feedback || {};
    const guard = feedback.guard || status.model_feedback_guard || {};
    const activeKind = String(guard.last_failure_kind || feedback.kind || '').trim();
    const activeLabel = String(guard.last_failure_label || feedback.label || '').trim();
    const activeError = String(guard.last_failure_error || feedback.raw_error || '').trim();
    const activeHttpStatus = Number(guard.last_failure_http_status || feedback.http_status || 0);
    const lastSuccessAt = fmtAgentTs(guard.last_success_at);
    const failureStreak = Number(guard.failure_streak || 0);

    if (activeKind) {
      const statusSuffix = Number.isFinite(activeHttpStatus) && activeHttpStatus > 0 ? ` / HTTP ${activeHttpStatus}` : '';
      const streakSuffix = failureStreak > 0 ? ` / 连续 ${failureStreak} 次` : '';
      const successSuffix = lastSuccessAt !== '--' ? ` / 最近成功 ${lastSuccessAt}` : '';
      return {
        summary: `${activeLabel || '模型反馈异常'}${statusSuffix}`,
        detail: `${compactText(activeError || feedback.detail || '模型服务当前未稳定返回', 180)}${streakSuffix}${successSuffix}`,
        tone: 'danger',
      };
    }

    if (lastSuccessAt !== '--') {
      return {
        summary: '模型反馈正常',
        detail: `最近成功 ${lastSuccessAt}`,
        tone: 'good',
      };
    }

    return {
      summary: '模型反馈待建立',
      detail: `本进程里还没有成功模型返回，超时阈值 ${formatDurationMinutes(guard.hard_timeout_sec)}`,
      tone: 'info',
    };
  }

  function describeModelOutput(diagnostics = {}) {
    const output = diagnostics.model_output || {};
    const source = String(output.source || 'synthetic').trim().toLowerCase();
    const rawActionText = decisionActionText(output.raw_action || '--');
    const normalizedActionText = decisionActionText(output.normalized_action || diagnostics.action || '--');
    const changed = Boolean(output.changed);
    const reasonChanged = Boolean(output.reason_changed);
    const sourceText = modelOutputSourceText(source);

    let summary = normalizedActionText;
    if (source === 'provider') {
      summary = output.action_changed ? `${rawActionText} -> ${normalizedActionText}` : rawActionText;
    } else {
      summary = `${sourceText} / ${normalizedActionText}`;
    }

    const detailParts = [`来源：${sourceText}`];
    const rawReason = compactText(output.raw_reason || '', 96);
    const normalizedReason = compactText(output.normalized_reason || diagnostics.decision_reason_raw || '', 96);
    if (rawReason) detailParts.push(`原始理由：${rawReason}`);
    if (reasonChanged && normalizedReason && normalizedReason !== rawReason) {
      detailParts.push(`落地理由：${normalizedReason}`);
    }

    return {
      summary,
      detail: detailParts.join('；'),
      tone: changed ? 'warn' : 'info',
    };
  }

  function describeExecutionCost(diagnostics = {}) {
    const cost = diagnostics.execution_cost || {};
    const oneWayBps = Number(cost.estimated_one_way_cost_bps || 0);
    const roundTripBps = Number(cost.estimated_round_trip_cost_bps || 0);
    const feeBps = Number(cost.fee_bps || 0);
    const slipBps = Number(cost.estimated_slippage_bps || 0);
    const refUsd = Number(cost.notional_reference || 0);
    const oneWayUsd = Number(cost.estimated_one_way_cost_usd_at_reference || 0);
    const roundTripUsd = Number(cost.estimated_round_trip_cost_usd_at_reference || 0);

    if (!Number.isFinite(oneWayBps) || oneWayBps <= 0) {
      return {
        summary: '成本估算待建立',
        detail: '本轮诊断里还没有拿到手续费/滑点估算。',
        tone: 'info',
      };
    }

    const summary = `单边 ${oneWayBps.toFixed(2)} bps / 往返 ${roundTripBps.toFixed(2)} bps`;
    const detailParts = [
      `手续费 ${feeBps.toFixed(2)} bps`,
      `滑点 ${slipBps.toFixed(2)} bps`,
    ];
    if (Number.isFinite(refUsd) && refUsd > 0) {
      detailParts.push(`参考名义 ${refUsd.toFixed(refUsd >= 100 ? 2 : 4)} USD`);
    }
    if (Number.isFinite(oneWayUsd) && oneWayUsd > 0) {
      detailParts.push(`单边约 ${oneWayUsd.toFixed(4)} USD`);
    }
    if (Number.isFinite(roundTripUsd) && roundTripUsd > 0) {
      detailParts.push(`往返约 ${roundTripUsd.toFixed(4)} USD`);
    }
    return {
      summary,
      detail: detailParts.join(' / '),
      tone: oneWayBps >= 12 ? 'warn' : 'info',
    };
  }

  function setChainSummaryText(id, value) {
    const el = document.getElementById(id);
    if (!el) return;
    el.textContent = String(value || '--');
  }

  function setChainSummaryTag(id, value, tone = '') {
    const el = document.getElementById(id);
    if (!el) return;
    el.textContent = String(value || '--');
    el.className = `ai-chain-summary-tag${tone ? ` is-${tone}` : ''}`;
  }

  function emitAgentStatus(status = {}, cfg = {}) {
    if (typeof window === 'undefined' || typeof window.dispatchEvent !== 'function') return;
    try {
      window.dispatchEvent(new CustomEvent('ai-agent:status', {
        detail: { status, config: cfg },
      }));
    } catch (_) {
      // no-op
    }
  }

  function renderAgentChainSummary(status = {}, cfg = {}) {
    const running = Boolean(status.running);
    const lastDecision = status.last_decision || {};
    const lastExecution = status.last_execution || {};
    const intervalSec = Number(cfg.interval_sec || 0);
    const tickCount = Number(status.tick_count || 0);
    const nextRunAt = fmtAgentTs(status.next_run_at);
    const lastLatencyText = formatLatencyMs(status.last_latency_ms);
    const confidence = Number(lastDecision.confidence || 0);
    const latestDecisionText = lastDecision.action
      ? `${decisionActionText(lastDecision.action)} / ${(confidence * 100).toFixed(0)}%`
      : '暂无决策';
    const latestActionText = lastExecution.submitted
      ? `已提交 / ${decisionActionText(lastDecision.action || '')}`
      : compactText(lastExecution.reason || '未提交', 42);
    const modeText = `${decisionModeLabel(cfg.mode || 'execute')} / ${cfg.allow_live ? '允许实盘' : '仅纸盘'}`;
    const statusText = running
      ? `运行中 · ${tickCount} 轮${intervalSec > 0 ? ` / ${intervalSec}s` : ''}${nextRunAt !== '--' ? ` · 下次 ${nextRunAt}` : ''}`
      : '未启动';
    const lastActionSummary = lastLatencyText !== '--'
      ? `${latestActionText} · 上次耗时 ${lastLatencyText}`
      : latestActionText;

    setChainSummaryTag('ai-chain-trading-tag', running ? '运行中' : '未启动', running ? 'active' : 'warn');
    setChainSummaryText('ai-chain-trading-mode', modeText);
    setChainSummaryText('ai-chain-trading-status', statusText);
    setChainSummaryText('ai-chain-trading-decision', latestDecisionText);
    setChainSummaryText('ai-chain-trading-last-action', lastActionSummary);
  }

  async function rootApi(path, options = {}) {
    if (typeof window.api === 'function') return window.api(path, options);
    const response = await fetch(path, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        ...(options.headers || {}),
      },
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload.detail || payload.error || `请求失败(${response.status})`);
    return payload;
  }

  function parseSymbolList(value) {
    return String(value || '')
      .split(/[\s,;\n\r\t]+/)
      .map((item) => item.trim().toUpperCase())
      .filter(Boolean);
  }

  function syncAgentConfigForm(cfg = {}) {
    const modeEl = document.getElementById('ai-agent-symbol-mode');
    const manualEl = document.getElementById('ai-agent-manual-symbol');
    const universeEl = document.getElementById('ai-agent-universe-symbols');
    if (modeEl) modeEl.value = String(cfg.symbol_mode || 'manual').toLowerCase();
    if (manualEl) manualEl.value = String(cfg.symbol || 'BTC/USDT');
    if (universeEl) universeEl.value = Array.isArray(cfg.universe_symbols) ? cfg.universe_symbols.join(', ') : '';
    updateAgentSymbolModeVisibility();
  }

  function updateAgentSymbolModeVisibility() {
    const mode = String(document.getElementById('ai-agent-symbol-mode')?.value || 'manual').toLowerCase();
    const manualWrap = document.getElementById('ai-agent-manual-symbol-wrap');
    const universeWrap = document.getElementById('ai-agent-universe-symbols-wrap');
    if (manualWrap) manualWrap.style.display = mode === 'manual' ? '' : 'none';
    if (universeWrap) universeWrap.style.display = mode === 'auto' ? '' : 'none';
  }

  function activeTabName() {
    return String(
      document.querySelector('.tab-btn.active')?.dataset?.tab
      || document.querySelector('.tab-content.active')?.id
      || ''
    ).trim().toLowerCase();
  }

  function isAgentTabActive() {
    return activeTabName() === 'ai-agent';
  }

  function stopPolling() {
    if (pollTimer) clearInterval(pollTimer);
    pollTimer = null;
  }

  function syncPollingState() {
    if (document.hidden || !isAgentTabActive()) {
      stopPolling();
      return;
    }
    startPolling();
  }

  function renderAgentStatusLoadError(message) {
    const statusText = compactText(message || '状态加载失败', 160) || '状态加载失败';
    const info = document.getElementById('ai-agent-info');
    const reasons = document.getElementById('ai-agent-reasons');
    const badge = document.getElementById('ai-agent-hold-badge');
    const dot = document.getElementById('ai-agent-status-dot');

    if (dot) {
      dot.className = 'agent-dot agent-dot-off';
      dot.title = statusText;
    }
    if (info) {
      info.innerHTML = `<div class="ai-agent-error">${esc(statusText)}</div>`;
    }
    if (reasons) {
      reasons.innerHTML = `<div class="ai-agent-empty">${esc(statusText)}</div>`;
    }
    if (badge) {
      badge.className = 'ai-agent-section-badge is-danger';
      badge.textContent = '状态加载失败';
    }

    setChainSummaryTag('ai-chain-trading-tag', '加载失败', 'warn');
    setChainSummaryText('ai-chain-trading-mode', '--');
    setChainSummaryText('ai-chain-trading-status', statusText);
    setChainSummaryText('ai-chain-trading-decision', '--');
    setChainSummaryText('ai-chain-trading-last-action', '--');
  }

  function buildRuntimeReason(status = {}, cfg = {}) {
    if (Boolean(status.running)) return null;
    if (status.last_run_at) {
      return {
        code: 'agent_stopped',
        label: '代理当前未运行',
        detail: '当前进程里的 autonomous agent 已停止；重新启动后才会持续 tick。',
        tone: 'warn',
      };
    }
    if (!cfg.enabled) {
      return {
        code: 'agent_disabled',
        label: '代理未启用',
        detail: 'enabled=false，当前不会自动决策。',
        tone: 'danger',
      };
    }
    return {
      code: 'agent_not_started',
      label: '代理还没启动',
      detail: '配置已启用，但本次进程里还没有 start。',
      tone: 'warn',
    };
  }

  function renderAgentDiagnostics(status = {}, cfg = {}) {
    const el = document.getElementById('ai-agent-reasons');
    const badgeEl = document.getElementById('ai-agent-hold-badge');
    if (!el) return;

    const diagnostics = status.last_diagnostics || {};
    const items = Array.isArray(diagnostics.items) ? diagnostics.items.slice() : [];
    const runtimeReason = buildRuntimeReason(status, cfg);
    const modelFeedback = describeModelFeedback(status, diagnostics);
    if (runtimeReason) items.unshift(runtimeReason);

    const primary = runtimeReason || diagnostics.primary || null;
    const agg = diagnostics.aggregated_signal || {};
    const research = diagnostics.research || {};
    const modelOutput = describeModelOutput(diagnostics);
    const executionCost = describeExecutionCost(diagnostics);
    const currentAction = String(status.last_decision?.action || diagnostics.action || 'hold').trim().toLowerCase();
    const badgeTone = primary?.tone || (currentAction === 'hold' ? 'warn' : 'info');
    const badgeText = currentAction === 'hold'
      ? '当前动作: Hold / 观望'
      : `当前动作: ${decisionActionText(currentAction)}`;

    if (badgeEl) {
      badgeEl.className = `ai-agent-section-badge ${toneClass(badgeTone)}`;
      badgeEl.textContent = badgeText;
    }

    if (!primary && !items.length) {
      if (badgeEl) {
        badgeEl.className = 'ai-agent-section-badge';
        badgeEl.textContent = '等待诊断';
      }
      el.innerHTML = '<div class="ai-agent-empty">暂无结构化原因，先跑一轮就会生成。</div>';
      return;
    }

    const secondaryItems = items.filter((item) => {
      if (!primary) return true;
      return reasonKey(item) !== reasonKey(primary);
    });

    const primarySummary = primary
      ? `<div class="ai-agent-reason-primary ${toneClass(primary.tone)}">
          <div class="ai-agent-reason-primary-kicker">主因</div>
          <div class="ai-agent-reason-primary-label">${esc(primary.label || '当前状态')}</div>
          <div class="ai-agent-reason-primary-detail">${esc(primary.detail || diagnostics.summary || '--')}</div>
        </div>`
      : '';

    const meta = `
      <div class="ai-agent-diagnostic-meta">
        <div class="ai-agent-diagnostic-item">
          <span>聚合信号</span>
          <strong>${esc(String(agg.direction || '--'))} / ${esc(formatRatio(agg.confidence, 3))}</strong>
        </div>
        <div class="ai-agent-diagnostic-item">
          <span>当前币种</span>
          <strong>${esc(String(diagnostics.selected_symbol || cfg.symbol || '--'))}</strong>
        </div>
        <div class="ai-agent-diagnostic-item">
          <span>研究候选</span>
          <strong>${esc(String(research.strategy || '--'))}</strong>
        </div>
        <div class="ai-agent-diagnostic-item">
          <span>研究状态</span>
          <strong>${esc(String(research.status || '--'))}</strong>
        </div>
        <div class="ai-agent-diagnostic-item">
          <span>模型反馈</span>
          <strong class="${toneClass(modelFeedback.tone)}">${esc(modelFeedback.summary)}</strong>
        </div>
        <div class="ai-agent-diagnostic-item">
          <span>模型动作</span>
          <strong class="${toneClass(modelOutput.tone)}">${esc(modelOutput.summary)}</strong>
        </div>
        <div class="ai-agent-diagnostic-item">
          <span>执行成本</span>
          <strong class="${toneClass(executionCost.tone)}">${esc(executionCost.summary)}</strong>
        </div>
      </div>
    `;

    const noteGrid = (modelOutput.detail || executionCost.detail)
      ? `<div class="ai-agent-note-grid">
          ${modelOutput.detail
            ? `<div class="ai-agent-note-card">
                <div class="ai-agent-note-label">模型补充说明</div>
                <div class="ai-agent-note-body">${esc(modelOutput.detail)}</div>
              </div>`
            : ''}
          ${executionCost.detail
            ? `<div class="ai-agent-note-card">
                <div class="ai-agent-note-label">成本补充说明</div>
                <div class="ai-agent-note-body">${esc(executionCost.detail)}</div>
              </div>`
            : ''}
        </div>`
      : '';

    const detailList = secondaryItems.length
      ? `<div class="ai-agent-reason-section">
          <div class="ai-agent-reason-section-title">仍在限制下单的因素</div>
          <div class="ai-agent-reason-list">${secondaryItems.map((item) => `
          <div class="ai-agent-reason-chip ${toneClass(item.tone)}">
            <div class="ai-agent-reason-chip-label">${esc(item.label || item.code || '--')}</div>
            <div class="ai-agent-reason-chip-detail">${esc(item.detail || '--')}</div>
          </div>
        `).join('')}</div>
        </div>`
      : '';

    el.innerHTML = `${primarySummary}${meta}${noteGrid}${detailList}`;
  }

  function renderAgentRanking(scan = null, cfg = {}) {
    const summaryEl = document.getElementById('ai-agent-ranking-summary');
    const listEl = document.getElementById('ai-agent-ranking');
    if (!summaryEl || !listEl) return;

    const mode = String(cfg.symbol_mode || 'manual').toLowerCase();
    if (!scan) {
      summaryEl.textContent = mode === 'auto'
        ? '还没有自动选币结果，点“刷新排行榜”即可看当前前十。'
        : '当前是固定币种模式，不会在币池里轮换。';
      listEl.innerHTML = mode === 'auto'
        ? '<div class="ai-agent-empty">等待生成排行榜...</div>'
        : '<div class="ai-agent-empty">固定币种模式下只会盯住单一 symbol。</div>';
      return;
    }

    const selected = String(scan.selected_symbol || cfg.symbol || '--');
    const selectionReason = String(scan.selection_reason || '--');
    const count = Number(scan.candidate_count || 0);
    summaryEl.textContent = `模式：${symbolModeLabel(scan.symbol_mode || cfg.symbol_mode)}，当前选中 ${selected}，候选 ${count} 个，原因：${symbolSelectionReasonText(selectionReason)}`;

    const rows = Array.isArray(scan.top_candidates) ? scan.top_candidates : [];
    if (!rows.length) {
      listEl.innerHTML = '<div class="ai-agent-empty">这次没有拿到可用的选币结果。</div>';
      return;
    }

    listEl.innerHTML = rows.map((row) => {
      const tradableText = row.tradable_now ? '可直接交易' : (row.blocked_by_risk ? '被风险拦截' : '暂不达标');
      const tone = row.tradable_now ? 'is-good' : (row.blocked_by_risk ? 'is-warn' : 'is-info');
      const holdingText = row.has_position
        ? `持仓中 / ${row.position_side === 'short' ? '空头' : row.position_side === 'long' ? '多头' : row.position_side || '--'}`
        : '';
      return `
        <div class="ai-agent-ranking-row ${row.selected ? 'is-selected' : ''}">
          <div class="ai-agent-ranking-head">
            <span class="ai-agent-ranking-rank">#${esc(row.rank || '--')}</span>
            <span class="ai-agent-ranking-symbol">${esc(row.symbol || '--')}</span>
            <span class="ai-agent-ranking-score">${esc(formatRatio(row.score, 3))}</span>
          </div>
          <div class="ai-agent-ranking-meta">
            <span>${esc(String(row.direction || '--'))} / ${esc(formatRatio(row.confidence, 3))}</span>
            <span class="ai-agent-mini-tag ${tone}">${esc(tradableText)}</span>
            ${holdingText ? `<span class="ai-agent-mini-tag is-warn">${esc(holdingText)}</span>` : ''}
          </div>
          <div class="ai-agent-ranking-detail">${esc(row.summary || '--')}</div>
        </div>
      `;
    }).join('');
  }

  function renderAgentPanel(status = {}, cfg = {}) {
    const dot = document.getElementById('ai-agent-status-dot');
    const info = document.getElementById('ai-agent-info');
    if (!dot || !info) return;

    const running = Boolean(status.running);
    dot.className = `agent-dot ${running ? 'agent-dot-on' : 'agent-dot-off'}`;
    dot.title = running ? '运行中' : '已停止';

    const researchCtx = status.last_research_context || {};
    const selectedResearch = researchCtx.selected_candidate || {};
    const lastScan = status.last_symbol_scan || {};
    const activeSymbol = String(lastScan.selected_symbol || cfg.symbol || '--');
    const researchLine = selectedResearch.candidate_id
      ? `${selectedResearch.strategy || '--'} / ${selectedResearch.candidate_id}`
      : '--';
    const lastRunAt = fmtAgentTs(status.last_run_at);
    const nextRunAt = fmtAgentTs(status.next_run_at);
    const latencyText = formatLatencyMs(status.last_latency_ms);
    const lastError = String(status.last_error || '').trim();
    const modelText = cfg.model ? `${providerDisplayName(cfg.provider || '-')}/${cfg.model}` : providerDisplayName(cfg.provider || '-');
    const modelFeedback = describeModelFeedback(status, status.last_diagnostics || {});
    const modelOutput = describeModelOutput(status.last_diagnostics || {});
    const executionCost = describeExecutionCost(status.last_diagnostics || {});

    info.innerHTML = `
      <div class="ai-agent-info-grid">
        <span>模型</span>
        <span>${esc(modelText)}</span>
        <span>执行模式</span>
        <span>${esc(decisionModeLabel(cfg.mode || 'execute'))} / ${esc(cfg.allow_live ? '允许实盘' : '仅纸盘')}</span>
        <span>币种模式</span>
        <span>${esc(symbolModeLabel(cfg.symbol_mode || 'manual'))}</span>
        <span>当前盯盘</span>
        <span>${esc(activeSymbol)}</span>
        <span>轮询次数</span>
        <span>${esc(Number(status.tick_count || 0))}</span>
        <span>已提交信号</span>
        <span>${esc(Number(status.submitted_count || 0))}</span>
        <span>参考候选</span>
        <span>${esc(researchLine)}</span>
        <span>模型反馈</span>
        <span class="${toneClass(modelFeedback.tone)}">${esc(modelFeedback.summary)}</span>
        <span>模型动作</span>
        <span class="${toneClass(modelOutput.tone)}">${esc(modelOutput.summary)}</span>
        <span>执行成本</span>
        <span class="${toneClass(executionCost.tone)}">${esc(executionCost.summary)}</span>
        <span>最后运行</span>
        <span>${esc(lastRunAt)}</span>
        <span>下次计划</span>
        <span>${esc(nextRunAt)}</span>
        <span>上次耗时</span>
        <span>${esc(latencyText)}</span>
      </div>
      <div class="ai-agent-muted">${esc(modelFeedback.detail)}</div>
      <div class="ai-agent-muted">${esc(modelOutput.detail)}</div>
      <div class="ai-agent-muted">${esc(executionCost.detail)}</div>
      ${lastError ? `<div class="ai-agent-error">错误：${esc(lastError)}</div>` : ''}
    `;

    const startBtn = document.getElementById('ai-agent-start-btn');
    const stopBtn = document.getElementById('ai-agent-stop-btn');
    if (startBtn) {
      startBtn.disabled = running;
      startBtn.textContent = running ? '运行中' : '启动';
    }
    if (stopBtn) {
      stopBtn.disabled = !running;
      stopBtn.textContent = '停止';
    }

    renderAgentChainSummary(status, cfg);
    renderAgentDiagnostics(status, cfg);
    renderAgentRanking(status.last_symbol_scan || null, cfg);
    syncAgentConfigForm(cfg);
    emitAgentStatus(status, cfg);
  }

  async function loadAgentJournal() {
    const el = document.getElementById('ai-agent-journal');
    if (!el) return;
    try {
      const response = await rootApi(`${AGENT_JOURNAL_API}?limit=15`);
      const rows = Array.isArray(response?.items) ? response.items.slice().reverse() : [];
      const summaryHtml = buildAgentJournalCurrentSummary(lastStatusSnapshot || {}, lastConfigSnapshot || {});
      if (!rows.length) {
        el.innerHTML = `${summaryHtml}<div class="ai-agent-empty">暂无日志</div>`;
        return;
      }
      el.innerHTML = summaryHtml + rows.map((row) => {
        const ts = fmtAgentTs(row.timestamp || row.ts || '');
        const decision = row.decision || {};
        const diagnostics = row.diagnostics || {};
        const context = row.context || {};
        const primary = diagnostics.primary || {};
        const modelOutput = diagnostics.model_output || {};
        const executionCost = describeExecutionCost(diagnostics);
        const tone = toneClass(primary.tone || (row.execution?.submitted ? 'good' : 'info'));
        const actionText = decisionActionText(decision.action || row.action || row.trigger || '?');
        const symbolText = row.config?.symbol || diagnostics.selected_symbol || '--';
        const aggregatedSignal = context.aggregated_signal || diagnostics.aggregated_signal || row.execution?.signal || {};
        const rewriteText = modelOutput.source === 'provider' && modelOutput.action_changed
          ? `原始 ${decisionActionText(modelOutput.raw_action || '--')} -> ${decisionActionText(modelOutput.normalized_action || decision.action || '--')}`
          : '';
        const baseDetailText = primary.label
          ? `${primary.label}${primary.detail ? `：${primary.detail}` : ''}`
          : compactText(decision.reason || diagnostics.summary || row.execution?.reason || '--', 120);
        const detailParts = [rewriteText, baseDetailText];
        if (Number(diagnostics?.execution_cost?.estimated_one_way_cost_bps || 0) > 0) {
          detailParts.push(`执行成本 ${executionCost.summary}`);
        }
        const detailText = detailParts.filter(Boolean).join('；');
        const signalText = aggregatedSignal && Object.keys(aggregatedSignal).length
          ? summarizeAggregatedSignal(aggregatedSignal)
          : '';
        return `
          <div class="agent-journal-row">
            <div class="agent-journal-main">
              <span class="agent-journal-ts">${esc(ts)}</span>
              <span class="agent-journal-action ${tone}">${esc(actionText)}</span>
              <span class="agent-journal-symbol">${esc(symbolText)}</span>
            </div>
            <div class="agent-journal-detail">${esc(detailText)}</div>
            ${signalText ? `<div class="agent-journal-signal">${esc(signalText)}</div>` : ''}
          </div>
        `;
      }).join('');
    } catch (_) {
      el.innerHTML = '<div class="ai-agent-empty">日志加载失败</div>';
    }
  }

  function renderAgentReview(payload = {}) {
    const summaryEl = document.getElementById('ai-agent-review-summary');
    const listEl = document.getElementById('ai-agent-review');
    if (!summaryEl || !listEl) return;

    const summary = payload?.summary || {};
    const items = Array.isArray(payload?.items) ? payload.items : [];
    const insights = Array.isArray(payload?.insights) ? payload.insights : [];
    const learningMemory = payload?.learning_memory || {};
    const adaptiveRisk = learningMemory?.adaptive_risk || {};
    const learningLessons = Array.isArray(learningMemory?.lessons) ? learningMemory.lessons : [];
    const blockedPairs = Array.isArray(learningMemory?.blocked_symbol_sides) ? learningMemory.blocked_symbol_sides : [];
    const guardrails = Array.isArray(learningMemory?.guardrails) ? learningMemory.guardrails : [];
    const rejectionReasons = Array.isArray(summary?.top_rejection_reasons) ? summary.top_rejection_reasons : [];
    const dominantSide = String(summary?.dominant_entry_side || '').trim().toLowerCase();
    const dominantSideText = dominantSide === 'short' ? '做空为主' : (dominantSide === 'long' ? '做多为主' : '--');
    const rejectionText = rejectionReasons.length
      ? rejectionReasons.slice(0, 3).map((item) => `${item.label} x${item.count}`).join(' / ')
      : '暂无明显阻塞';
    const blockedText = blockedPairs.length
      ? blockedPairs.slice(0, 3).map((item) => `${item.symbol} ${item.side}`).join(' / ')
      : '暂无冷静期币种';
    const requireResearch = Boolean(adaptiveRisk?.require_research_for_new_entries);
    const dataOutageExit = Boolean(adaptiveRisk?.force_close_on_data_outage_losing_position);
    const serviceInstabilityGuard = Boolean(adaptiveRisk?.avoid_new_entries_during_service_instability);
    const learningGeneratedAt = fmtAgentTs(learningMemory?.generated_at);

    summaryEl.innerHTML = `
      <div class="ai-agent-review-kpis">
        <article class="ai-agent-review-kpi">
          <span class="ai-agent-review-kpi-label">放行次数</span>
          <strong class="ai-agent-review-kpi-value">${esc(summary?.submitted_count ?? '--')}</strong>
          <span class="ai-agent-review-kpi-note">开仓 ${esc(summary?.entry_count ?? '--')} / 平仓 ${esc(summary?.close_count ?? '--')}</span>
        </article>
        <article class="ai-agent-review-kpi">
          <span class="ai-agent-review-kpi-label">亏损平仓</span>
          <strong class="ai-agent-review-kpi-value">${esc(summary?.losing_close_count ?? '--')}</strong>
          <span class="ai-agent-review-kpi-note">平仓前处于浮亏的次数</span>
        </article>
        <article class="ai-agent-review-kpi">
          <span class="ai-agent-review-kpi-label">研究缺口</span>
          <strong class="ai-agent-review-kpi-value">${esc(summary?.entries_without_research_count ?? '--')}</strong>
          <span class="ai-agent-review-kpi-note">开仓时没有研究候选</span>
        </article>
        <article class="ai-agent-review-kpi">
          <span class="ai-agent-review-kpi-label">同向重复</span>
          <strong class="ai-agent-review-kpi-value">${esc(summary?.repeated_same_direction_entries ?? '--')}</strong>
          <span class="ai-agent-review-kpi-note">同币种同方向连续放行</span>
        </article>
      </div>
      <div class="ai-agent-review-meta">
        <span>最近主做币种：${esc(summary?.dominant_symbol || '--')}</span>
        <span>方向偏好：${esc(dominantSideText)}</span>
        <span>最近一笔：${esc(summary?.latest_entry_symbol || '--')} / ${esc(fmtAgentTs(summary?.latest_entry_at))}</span>
      </div>
      <div class="ai-agent-review-meta ai-agent-review-meta-secondary">
        <span>模型阻塞最多：${esc(rejectionText)}</span>
        <span>异常 hold：${esc(summary?.outage_after_entry_count ?? 0)} 次</span>
        <span>未配对开仓：${esc(summary?.unmatched_entry_count ?? 0)} 笔</span>
      </div>
      <div class="ai-agent-review-insights">
        ${insights.length ? insights.map((item) => `<div class="ai-agent-review-insight">${esc(item)}</div>`).join('') : '<div class="ai-agent-empty">暂无复盘洞察</div>'}
      </div>
      <section class="ai-agent-learning-panel">
        <div class="ai-agent-learning-head">
          <div class="ai-agent-learning-title">AI 复盘记忆</div>
          <div class="ai-agent-learning-subtitle">生成时间：${esc(learningGeneratedAt)}</div>
        </div>
        <div class="ai-agent-learning-grid">
          <article class="ai-agent-learning-card">
            <span class="ai-agent-learning-label">有效开仓阈值</span>
            <strong>${esc(formatRatio(adaptiveRisk?.effective_min_confidence, 3))}</strong>
            <span class="ai-agent-learning-note">高于配置阈值时，说明复盘在主动收紧新单门槛</span>
          </article>
          <article class="ai-agent-learning-card">
            <span class="ai-agent-learning-label">同向加仓上限</span>
            <strong>${esc(formatRatio(adaptiveRisk?.same_direction_max_exposure_ratio, 3))}</strong>
            <span class="ai-agent-learning-note">最近连续亏损或异常越多，这个比例会越保守</span>
          </article>
          <article class="ai-agent-learning-card">
            <span class="ai-agent-learning-label">新单仓位缩放</span>
            <strong>${esc(formatRatio(adaptiveRisk?.entry_size_scale, 3))}</strong>
            <span class="ai-agent-learning-note">会直接压低 agent 新开仓的 signal strength</span>
          </article>
          <article class="ai-agent-learning-card">
            <span class="ai-agent-learning-label">关键防守</span>
            <strong>${esc(blockedText)}</strong>
            <span class="ai-agent-learning-note">${esc(requireResearch ? '无研究候选不开新仓；' : '研究候选非强制；')}${esc(dataOutageExit ? '价格缺失且浮亏会优先平仓；' : '数据缺失时优先观望；')}${esc(serviceInstabilityGuard ? '模型异常期不鼓励新开仓。' : '模型异常期仍以常规规则处理。')}</span>
          </article>
        </div>
        <div class="ai-agent-learning-tags">
          ${guardrails.length ? guardrails.map((item) => `<span class="ai-agent-learning-tag">${esc(item)}</span>`).join('') : '<span class="ai-agent-learning-tag">暂无额外 guardrail</span>'}
        </div>
        <div class="ai-agent-review-insights">
          ${learningLessons.length ? learningLessons.slice(0, 4).map((item) => `<div class="ai-agent-review-insight">${esc(item)}</div>`).join('') : '<div class="ai-agent-empty">复盘记忆还在积累中</div>'}
        </div>
      </section>
    `;

    if (!items.length) {
      listEl.innerHTML = '<div class="ai-agent-empty">暂无放行交易复盘</div>';
      return;
    }

    listEl.innerHTML = items.map((item) => {
      const phaseText = item.phase === 'entry' ? '开仓复盘' : (item.phase === 'exit' ? '平仓复盘' : '事件复盘');
      const statusTone = reviewToneClass(item.review_status_tone);
      const summaryLines = Array.isArray(item.summary_lines) ? item.summary_lines : [];
      const blockers = Array.isArray(item?.follow_up?.blockers) ? item.follow_up.blockers : [];
      const followParts = [];
      if (Number.isFinite(Number(item?.follow_up?.latest_unrealized_pnl))) {
        followParts.push(`最近跟踪盈亏 ${formatSigned(item.follow_up.latest_unrealized_pnl, 4, ' USDT')}`);
      }
      if (Number.isFinite(Number(item?.follow_up?.favorable_markout_bps))) {
        followParts.push(`最好 ${formatSigned(item.follow_up.favorable_markout_bps, 1, ' bps')}`);
      }
      if (Number.isFinite(Number(item?.follow_up?.adverse_markout_bps))) {
        followParts.push(`最差 ${formatSigned(item.follow_up.adverse_markout_bps, 1, ' bps')}`);
      }
      if (Number(item?.follow_up?.outage_hold_count || 0) > 0) {
        followParts.push(`异常 hold ${Number(item.follow_up.outage_hold_count)} 次`);
      }
      if (Number.isFinite(Number(item?.pair?.holding_minutes))) {
        followParts.push(`持有 ${formatNumber(item.pair.holding_minutes, 1)} 分钟`);
      }
      if (Number(item?.pair?.repeat_open_rank || 1) > 1 && item.phase === 'entry') {
        followParts.push(`同向第 ${Number(item.pair.repeat_open_rank)} 次放行`);
      }

      const researchText = item?.research?.available
        ? `${item.research.strategy || '研究候选'} / ${item.research.status || '已关联'}`
        : '无研究候选';
      const signalText = item?.aggregated_signal?.direction
        ? `${item.aggregated_signal.direction} / ${formatNumber(item.aggregated_signal.confidence, 3)}`
        : '--';
      const costText = Number.isFinite(Number(item?.cost?.one_way_bps))
        ? `单边 ${formatNumber(item.cost.one_way_bps, 2)} bps`
        : '成本待补充';
      const signalPrice = formatNumber(item.price, Number(item.price || 0) >= 100 ? 2 : 4);
      const orderText = item?.order
        ? `${String(item.order.side || '--').toUpperCase()} / ${formatNumber(item.order.price, Number(item.order.price || 0) >= 100 ? 2 : 4)}`
        : '未匹配到本进程订单';
      const blockerText = blockers.length
        ? blockers.map((entry) => `${entry.label} x${entry.count}`).join(' / ')
        : '暂无额外阻塞';

      return `
        <article class="ai-agent-review-card">
          <div class="ai-agent-review-card-head">
            <div>
              <div class="ai-agent-review-card-title">${esc(item.action_label || '--')} · ${esc(item.symbol || '--')}</div>
              <div class="ai-agent-review-card-subtitle">${esc(phaseText)} · ${esc(fmtAgentTs(item.timestamp))}</div>
            </div>
            <div class="ai-agent-review-badges">
              <span class="ai-agent-review-badge ${statusTone}">${esc(item.review_status_text || '待观察')}</span>
              <span class="ai-agent-review-badge is-info">${esc(`${formatNumber(Number(item.decision_confidence || 0) * 100, 0)}% 置信度`)}</span>
            </div>
          </div>
          <div class="ai-agent-review-grid">
            <div class="ai-agent-review-cell">
              <span class="ai-agent-review-cell-label">动作原因</span>
              <strong>${esc(compactText(item.reason || item?.primary?.label || '--', 180))}</strong>
            </div>
            <div class="ai-agent-review-cell">
              <span class="ai-agent-review-cell-label">聚合信号</span>
              <strong>${esc(signalText)}</strong>
            </div>
            <div class="ai-agent-review-cell">
              <span class="ai-agent-review-cell-label">研究支撑</span>
              <strong>${esc(researchText)}</strong>
            </div>
            <div class="ai-agent-review-cell">
              <span class="ai-agent-review-cell-label">执行成本</span>
              <strong>${esc(costText)}</strong>
            </div>
            <div class="ai-agent-review-cell">
              <span class="ai-agent-review-cell-label">信号价格</span>
              <strong>${esc(signalPrice)}</strong>
            </div>
            <div class="ai-agent-review-cell">
              <span class="ai-agent-review-cell-label">订单匹配</span>
              <strong>${esc(orderText)}</strong>
            </div>
          </div>
          <div class="ai-agent-review-lines">
            ${summaryLines.length ? summaryLines.map((line) => `<div class="ai-agent-review-line">${esc(line)}</div>`).join('') : '<div class="ai-agent-empty">暂无摘要</div>'}
          </div>
          <div class="ai-agent-review-foot">
            <span>主因：${esc(item?.primary?.label || '--')}</span>
            <span>后续观察：${esc(followParts.length ? followParts.join(' / ') : '暂无')}</span>
            <span>后续阻塞：${esc(blockerText)}</span>
          </div>
        </article>
      `;
    }).join('');
  }

  async function loadAgentReview() {
    const summaryEl = document.getElementById('ai-agent-review-summary');
    const listEl = document.getElementById('ai-agent-review');
    if (!summaryEl || !listEl) return null;
    try {
      const response = await rootApi(`${AGENT_REVIEW_API}?limit=12`);
      renderAgentReview(response || {});
      return response;
    } catch (_) {
      summaryEl.innerHTML = '<div class="ai-agent-empty">复盘摘要加载失败</div>';
      listEl.innerHTML = '<div class="ai-agent-empty">复盘列表加载失败</div>';
      return null;
    }
  }

  async function loadAgentSymbolRanking(force = false, options = {}) {
    const timeoutMs = Math.max(5000, Number(options.timeoutMs || (force ? 90000 : 20000)));
    const notifyOnError = options.notifyOnError !== false;
    const preserveExisting = options.preserveExisting !== false;
    try {
      const response = await rootApi(`${AGENT_SYMBOL_RANKING_API}?limit=10${force ? '&refresh=1' : ''}`, { timeoutMs });
      const cfg = {
        symbol_mode: response?.symbol_mode || document.getElementById('ai-agent-symbol-mode')?.value || 'manual',
        symbol: response?.configured_symbol || document.getElementById('ai-agent-manual-symbol')?.value || 'BTC/USDT',
      };
      renderAgentRanking(response || null, cfg);
      return response;
    } catch (err) {
      if (notifyOnError) {
        notify(`刷新选币排行失败: ${err.message}`, true);
      }
      if (!preserveExisting) {
        renderAgentRanking(null, {
          symbol_mode: document.getElementById('ai-agent-symbol-mode')?.value || 'manual',
          symbol: document.getElementById('ai-agent-manual-symbol')?.value || 'BTC/USDT',
        });
      }
      return null;
    }
  }

  async function saveAgentConfig() {
    const symbolMode = String(document.getElementById('ai-agent-symbol-mode')?.value || 'manual').toLowerCase();
    const symbol = String(document.getElementById('ai-agent-manual-symbol')?.value || 'BTC/USDT').trim().toUpperCase();
    const universeSymbols = parseSymbolList(document.getElementById('ai-agent-universe-symbols')?.value || '');

    try {
      const payload = {
        symbol_mode: symbolMode,
        symbol,
        universe_symbols: universeSymbols,
        selection_top_n: 10,
      };
      const response = await rootApi(AGENT_CONFIG_API, {
        method: 'POST',
        body: JSON.stringify(payload),
      });
      const cfg = response?.config || {};
      syncAgentConfigForm(cfg);
      notify('自动交易代理配置已保存');
      await loadAgentStatus({ includeDetails: isAgentTabActive(), notifyOnError: true });
      if (symbolMode === 'auto') {
        loadAgentSymbolRanking(true, {
          timeoutMs: 90000,
          notifyOnError: true,
          preserveExisting: false,
        }).catch(() => {});
      }
    } catch (err) {
      notify(`保存代理配置失败: ${err.message}`, true);
    }
  }

  async function loadAgentStatus(options = {}) {
    if (!document.getElementById('ai-agent-card')) return null;
    const includeDetails = options.includeDetails !== false && isAgentTabActive();
    const notifyOnError = options.notifyOnError === true;
    if (statusInFlight) {
      if (includeDetails) {
        return statusInFlight.then((response) => {
          if (document.getElementById('ai-agent-journal')) loadAgentJournal().catch(() => {});
          if (document.getElementById('ai-agent-review')) loadAgentReview().catch(() => {});
          return response;
        });
      }
      return statusInFlight;
    }
    const task = (async () => {
      try {
        const response = await rootApi(AGENT_STATUS_API);
        lastStatusSnapshot = response?.status || {};
        lastConfigSnapshot = response?.config || {};
        renderAgentPanel(response?.status || {}, response?.config || {});
        if (includeDetails && document.getElementById('ai-agent-journal')) {
          loadAgentJournal().catch(() => {});
        }
        if (includeDetails && document.getElementById('ai-agent-review')) {
          loadAgentReview().catch(() => {});
        }
        return response;
      } catch (err) {
        if (includeDetails) {
          renderAgentStatusLoadError(err?.message || '状态加载失败');
        }
        if (notifyOnError) {
          notify(`加载自治代理状态失败: ${err?.message || '未知错误'}`, true);
        }
        return null;
      }
    })();
    statusInFlight = task;
    try {
      return await task;
    } finally {
      if (statusInFlight === task) statusInFlight = null;
    }
  }

  async function agentStart() {
    const btn = document.getElementById('ai-agent-start-btn');
    const label = btn ? btn.textContent : '启动';
    if (btn) {
      btn.disabled = true;
      btn.textContent = '启动中...';
    }
    try {
      await rootApi(AGENT_START_API, {
        method: 'POST',
        body: JSON.stringify({ enable: true }),
      });
      notify('自动交易代理已启动');
      await loadAgentStatus({ includeDetails: isAgentTabActive(), notifyOnError: true });
    } catch (err) {
      notify(`启动失败: ${err.message}`, true);
      if (btn) {
        btn.disabled = false;
        btn.textContent = label;
      }
    }
  }

  async function agentStop() {
    const btn = document.getElementById('ai-agent-stop-btn');
    const label = btn ? btn.textContent : '停止';
    if (btn) {
      btn.disabled = true;
      btn.textContent = '停止中...';
    }
    try {
      await rootApi(AGENT_STOP_API, { method: 'POST' });
      notify('自动交易代理已停止');
      await loadAgentStatus({ includeDetails: isAgentTabActive(), notifyOnError: true });
    } catch (err) {
      notify(`停止失败: ${err.message}`, true);
      if (btn) {
        btn.disabled = false;
        btn.textContent = label;
      }
    }
  }

  async function agentRunOnce() {
    const btn = document.getElementById('ai-agent-run-once-btn');
    const label = btn ? btn.textContent : '立即跑一轮';
    if (btn) {
      btn.disabled = true;
      btn.textContent = '运行中...';
    }
    try {
      const response = await rootApi(AGENT_RUN_ONCE_API, {
        method: 'POST',
        body: JSON.stringify({ force: true }),
      });
      const result = response?.result || {};
      if (result?.skipped) {
        const reason = compactText(result?.reason || result?.rejection_reason || 'unknown', 80);
        notify(`单次试跑已跳过: ${reason}`, true);
        await loadAgentStatus({ includeDetails: isAgentTabActive(), notifyOnError: true });
        return;
      }
      const action = decisionActionText(result?.decision?.action || 'hold');
      const symbol = String(result?.effective_symbol || result?.selection?.selected_symbol || '--');
      notify(`单次试跑完成：${symbol} / ${action}`);
      await loadAgentStatus({ includeDetails: isAgentTabActive(), notifyOnError: true });
    } catch (err) {
      notify(`运行失败: ${err.message}`, true);
    } finally {
      if (btn) {
        btn.disabled = false;
        btn.textContent = label;
      }
    }
  }

  function startPolling() {
    if (pollTimer) return;
    pollTimer = setInterval(() => {
      if (document.hidden || !isAgentTabActive()) {
        stopPolling();
        return;
      }
      loadAgentStatus({ includeDetails: true }).catch(() => {});
    }, POLL_MS);
  }

  function init() {
    if (!document.getElementById('ai-agent-card')) return;

    const modules = aiRoot().modules || {};
    modules.agent = {
      refresh: (options = {}) => loadAgentStatus(options),
      refreshJournal: () => loadAgentStatus({ includeDetails: true, notifyOnError: true }),
      refreshReview: () => loadAgentReview(),
      refreshRanking: () => loadAgentSymbolRanking(true, { timeoutMs: 90000, notifyOnError: true, preserveExisting: false }),
      saveConfig: () => saveAgentConfig(),
      start: () => agentStart(),
      stop: () => agentStop(),
      runOnce: () => agentRunOnce(),
    };
    aiRoot().modules = modules;

    window.agentStart = agentStart;
    window.agentStop = agentStop;
    window.agentRunOnce = agentRunOnce;
    window.agentRefreshJournal = () => loadAgentStatus({ includeDetails: true, notifyOnError: true }).catch(() => {});
    window.agentRefreshReview = () => loadAgentReview().catch(() => {});
    window.agentRefreshRanking = () => loadAgentSymbolRanking(true, { timeoutMs: 90000, notifyOnError: true, preserveExisting: false });
    window.agentSaveConfig = () => saveAgentConfig();
    window.agentToggleSymbolMode = () => updateAgentSymbolModeVisibility();

    window.addEventListener('ai-research:state', (event) => {
      const reason = String(event?.detail?.reason || '');
      if (['refresh-workbench', 'runtime-summary', 'candidate-detail'].includes(reason)) {
        loadAgentStatus({ includeDetails: isAgentTabActive() }).catch(() => {});
      }
    });

    document.addEventListener('click', (event) => {
      if (event.target instanceof Element && event.target.closest('.tab-btn')) {
        syncPollingState();
      }
    });
    document.addEventListener('visibilitychange', syncPollingState);
    window.addEventListener('hashchange', syncPollingState);

    syncPollingState();
    if (isAgentTabActive()) {
      loadAgentStatus({ includeDetails: true }).catch(() => {});
    }
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
