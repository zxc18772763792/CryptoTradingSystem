(function () {
  'use strict';

  const POLL_MS = 30000;
  const AI_UI_TIMEZONE = 'Asia/Shanghai';
  const AGENT_STATUS_API = '/ai/autonomous-agent/status';
  const AGENT_JOURNAL_API = '/ai/autonomous-agent/journal';
  const AGENT_START_API = '/ai/autonomous-agent/start';
  const AGENT_STOP_API = '/ai/autonomous-agent/stop';
  const AGENT_RUN_ONCE_API = '/ai/autonomous-agent/run-once';
  const AGENT_CONFIG_API = '/ai/runtime-config/autonomous-agent';
  const AGENT_SYMBOL_RANKING_API = '/ai/autonomous-agent/symbol-ranking';

  let pollTimer = null;

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

  function renderAgentChainSummary(status = {}, cfg = {}) {
    const running = Boolean(status.running);
    const lastDecision = status.last_decision || {};
    const lastExecution = status.last_execution || {};
    const intervalSec = Number(cfg.interval_sec || 0);
    const tickCount = Number(status.tick_count || 0);
    const confidence = Number(lastDecision.confidence || 0);
    const latestDecisionText = lastDecision.action
      ? `${decisionActionText(lastDecision.action)} / ${(confidence * 100).toFixed(0)}%`
      : '暂无决策';
    const latestActionText = lastExecution.submitted
      ? `已提交 / ${decisionActionText(lastDecision.action || '')}`
      : compactText(lastExecution.reason || '未提交', 42);
    const modeText = `${decisionModeLabel(cfg.mode || 'execute')} / ${cfg.allow_live ? '允许实盘' : '仅纸盘'}`;
    const statusText = running
      ? `运行中 · ${tickCount} 轮${intervalSec > 0 ? ` / ${intervalSec}s` : ''}`
      : '未启动';

    setChainSummaryTag('ai-chain-trading-tag', running ? '运行中' : '未启动', running ? 'active' : 'warn');
    setChainSummaryText('ai-chain-trading-mode', modeText);
    setChainSummaryText('ai-chain-trading-status', statusText);
    setChainSummaryText('ai-chain-trading-decision', latestDecisionText);
    setChainSummaryText('ai-chain-trading-last-action', latestActionText);
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

    if (!primary && !items.length) {
      el.innerHTML = '<div class="ai-agent-empty">暂无结构化原因，先跑一轮就会生成。</div>';
      return;
    }

    const primarySummary = primary
      ? `<div class="ai-agent-reason-primary ${toneClass(primary.tone)}">
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
      </div>
    `;

    const detailList = items.length
      ? `<div class="ai-agent-reason-list">${items.map((item) => `
          <div class="ai-agent-reason-chip ${toneClass(item.tone)}">
            <div class="ai-agent-reason-chip-label">${esc(item.label || item.code || '--')}</div>
            <div class="ai-agent-reason-chip-detail">${esc(item.detail || '--')}</div>
          </div>
        `).join('')}</div>`
      : '';

    const debugNote = modelOutput.detail
      ? `<div class="ai-agent-muted">${esc(modelOutput.detail)}</div>`
      : '';

    el.innerHTML = `${primarySummary}${meta}${debugNote}${detailList}`;
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
    const lastError = String(status.last_error || '').trim();
    const modelText = cfg.model ? `${providerDisplayName(cfg.provider || '-')}/${cfg.model}` : providerDisplayName(cfg.provider || '-');
    const modelFeedback = describeModelFeedback(status, status.last_diagnostics || {});
    const modelOutput = describeModelOutput(status.last_diagnostics || {});

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
        <span>最后运行</span>
        <span>${esc(lastRunAt)}</span>
      </div>
      <div class="ai-agent-muted">${esc(modelFeedback.detail)}</div>
      <div class="ai-agent-muted">${esc(modelOutput.detail)}</div>
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
  }

  async function loadAgentJournal() {
    const el = document.getElementById('ai-agent-journal');
    if (!el) return;
    try {
      const response = await rootApi(`${AGENT_JOURNAL_API}?limit=15`);
      const rows = Array.isArray(response?.items) ? response.items.slice().reverse() : [];
      if (!rows.length) {
        el.innerHTML = '<div class="ai-agent-empty">暂无日志</div>';
        return;
      }
      el.innerHTML = rows.map((row) => {
        const ts = fmtAgentTs(row.timestamp || row.ts || '');
        const decision = row.decision || {};
        const diagnostics = row.diagnostics || {};
        const primary = diagnostics.primary || {};
        const modelOutput = diagnostics.model_output || {};
        const tone = toneClass(primary.tone || (row.execution?.submitted ? 'good' : 'info'));
        const actionText = decisionActionText(decision.action || row.action || row.trigger || '?');
        const symbolText = row.config?.symbol || diagnostics.selected_symbol || '--';
        const rewriteText = modelOutput.source === 'provider' && modelOutput.action_changed
          ? `原始 ${decisionActionText(modelOutput.raw_action || '--')} -> ${decisionActionText(modelOutput.normalized_action || decision.action || '--')}`
          : '';
        const baseDetailText = primary.label
          ? `${primary.label}${primary.detail ? `：${primary.detail}` : ''}`
          : compactText(decision.reason || diagnostics.summary || row.execution?.reason || '--', 120);
        const detailText = rewriteText ? `${rewriteText}；${baseDetailText}` : baseDetailText;
        return `
          <div class="agent-journal-row">
            <span class="agent-journal-ts">${esc(ts)}</span>
            <span class="agent-journal-action ${tone}">${esc(actionText)}</span>
            <span class="agent-journal-symbol">${esc(symbolText)}</span>
            <span class="agent-journal-detail">${esc(detailText)}</span>
          </div>
        `;
      }).join('');
    } catch (_) {
      el.innerHTML = '<div class="ai-agent-empty">日志加载失败</div>';
    }
  }

  async function loadAgentSymbolRanking(force = false) {
    try {
      const response = await rootApi(`${AGENT_SYMBOL_RANKING_API}?limit=10${force ? '&refresh=1' : ''}`);
      const cfg = {
        symbol_mode: response?.symbol_mode || document.getElementById('ai-agent-symbol-mode')?.value || 'manual',
        symbol: response?.configured_symbol || document.getElementById('ai-agent-manual-symbol')?.value || 'BTC/USDT',
      };
      renderAgentRanking(response || null, cfg);
      return response;
    } catch (err) {
      notify(`刷新选币排行失败: ${err.message}`, true);
      renderAgentRanking(null, {
        symbol_mode: document.getElementById('ai-agent-symbol-mode')?.value || 'manual',
        symbol: document.getElementById('ai-agent-manual-symbol')?.value || 'BTC/USDT',
      });
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
      await loadAgentStatus();
      if (symbolMode === 'auto') await loadAgentSymbolRanking(true);
    } catch (err) {
      notify(`保存代理配置失败: ${err.message}`, true);
    }
  }

  async function loadAgentStatus() {
    if (!document.getElementById('ai-agent-card')) return null;
    try {
      const response = await rootApi(AGENT_STATUS_API);
      renderAgentPanel(response?.status || {}, response?.config || {});
      if (document.getElementById('ai-agent-journal')) {
        loadAgentJournal().catch(() => {});
      }
      return response;
    } catch (_) {
      return null;
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
      await loadAgentStatus();
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
      await loadAgentStatus();
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
        body: JSON.stringify({}),
      });
      const result = response?.result || {};
      const action = decisionActionText(result?.decision?.action || 'hold');
      const symbol = String(result?.effective_symbol || result?.selection?.selected_symbol || '--');
      notify(`单次试跑完成：${symbol} / ${action}`);
      await loadAgentStatus();
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
    clearInterval(pollTimer);
    pollTimer = setInterval(() => loadAgentStatus().catch(() => {}), POLL_MS);
  }

  function init() {
    if (!document.getElementById('ai-agent-card')) return;

    const modules = aiRoot().modules || {};
    modules.agent = {
      refresh: () => loadAgentStatus(),
      refreshJournal: () => loadAgentJournal(),
      refreshRanking: () => loadAgentSymbolRanking(true),
      saveConfig: () => saveAgentConfig(),
      start: () => agentStart(),
      stop: () => agentStop(),
      runOnce: () => agentRunOnce(),
    };
    aiRoot().modules = modules;

    window.agentStart = agentStart;
    window.agentStop = agentStop;
    window.agentRunOnce = agentRunOnce;
    window.agentRefreshJournal = () => loadAgentJournal().catch(() => {});
    window.agentRefreshRanking = () => loadAgentSymbolRanking(true);
    window.agentSaveConfig = () => saveAgentConfig();
    window.agentToggleSymbolMode = () => updateAgentSymbolModeVisibility();

    window.addEventListener('ai-research:state', (event) => {
      const reason = String(event?.detail?.reason || '');
      if (['refresh-workbench', 'runtime-summary', 'candidate-detail'].includes(reason)) {
        loadAgentStatus().catch(() => {});
      }
    });

    loadAgentStatus().then((response) => {
      const cfg = response?.config || {};
      if (String(cfg.symbol_mode || 'manual').toLowerCase() === 'auto') {
        loadAgentSymbolRanking(false).catch(() => {});
      }
    }).catch(() => {});
    startPolling();
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
