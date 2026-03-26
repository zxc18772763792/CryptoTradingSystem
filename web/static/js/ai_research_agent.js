(function () {
  'use strict';

  const POLL_MS = 30000;
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

  function providerDisplayName(provider) {
    if (typeof aiRoot().util?.providerDisplayName === 'function') {
      return aiRoot().util.providerDisplayName(provider);
    }
    const value = String(provider || '').trim().toLowerCase();
    if (value === 'codex' || value === 'openai') return 'OpenAI';
    return String(provider || '-');
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

  function renderAgentPanel(status = {}, cfg = {}) {
    const dot = document.getElementById('ai-agent-status-dot');
    const info = document.getElementById('ai-agent-info');
    if (!dot || !info) return;

    const running = Boolean(status.running);
    dot.className = `agent-dot ${running ? 'agent-dot-on' : 'agent-dot-off'}`;
    dot.title = running ? '运行中' : '已停止';

    const researchCtx = status.last_research_context || {};
    const selectedResearch = researchCtx.selected_candidate || {};
    const researchLine = selectedResearch.candidate_id
      ? `${selectedResearch.strategy || '--'} / ${selectedResearch.candidate_id}`
      : '--';
    const lastRunAt = status.last_run_at ? String(status.last_run_at).slice(0, 19) : '--';
    const lastError = String(status.last_error || '').trim();
    const modeText = cfg.allow_live ? '允许实盘' : '仅纸盘';
    const modelText = cfg.model ? `${providerDisplayName(cfg.provider || '-')}/${cfg.model}` : providerDisplayName(cfg.provider || '-');

    info.innerHTML = `
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:4px 8px;font-size:11px;">
        <span style="color:var(--text-muted)">模型</span>
        <span>${esc(modelText)}</span>
        <span style="color:var(--text-muted)">模式</span>
        <span>${esc(modeText)}</span>
        <span style="color:var(--text-muted)">轮询次数</span>
        <span>${esc(Number(status.tick_count || 0))}</span>
        <span style="color:var(--text-muted)">已提交信号</span>
        <span>${esc(Number(status.submitted_count || 0))}</span>
        <span style="color:var(--text-muted)">参考候选</span>
        <span>${esc(researchLine)}</span>
        <span style="color:var(--text-muted)">最后运行</span>
        <span>${esc(lastRunAt)}</span>
      </div>
      ${lastError ? `<div style="margin-top:6px;font-size:11px;color:#f87171;">错误：${esc(lastError)}</div>` : ''}
    `;

    const startBtn = document.getElementById('ai-agent-start-btn');
    const stopBtn = document.getElementById('ai-agent-stop-btn');
    if (startBtn) startBtn.disabled = running;
    if (stopBtn) stopBtn.disabled = !running;
  }

  async function loadAgentJournal() {
    const el = document.getElementById('ai-agent-journal');
    if (!el) return;
    try {
      const response = await rootApi('/ai/autonomous-agent/journal?limit=15');
      const rows = Array.isArray(response?.items) ? response.items.slice().reverse() : [];
      if (!rows.length) {
        el.innerHTML = '<div style="color:var(--text-muted)">暂无日志</div>';
        return;
      }
      el.innerHTML = rows.map((row) => {
        const ts = String(row.ts || row.timestamp || '').slice(0, 19);
        const action = String(row.action || row.trigger || row.event || '?');
        const detail = compactText(row.decision || row.result || row.error || '', 120);
        const color = row.error || action.includes('error') ? '#f87171' : 'var(--text-muted)';
        return `<div class="agent-journal-row">
          <span class="agent-journal-ts">${esc(ts)}</span>
          <span class="agent-journal-action" style="color:${color}">${esc(action)}</span>
          <span class="agent-journal-detail">${esc(detail)}</span>
        </div>`;
      }).join('');
    } catch (_) {
      el.innerHTML = '<div style="color:var(--text-muted)">日志加载失败</div>';
    }
  }

  async function loadAgentStatus() {
    if (!document.getElementById('ai-agent-card')) return;
    try {
      const response = await rootApi('/ai/autonomous-agent/status');
      renderAgentPanel(response?.status || {}, response?.config || {});
      if (document.getElementById('ai-agent-journal')) {
        loadAgentJournal().catch(() => {});
      }
    } catch (_) {
      // best-effort
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
      await rootApi('/ai/autonomous-agent/start', {
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
      await rootApi('/ai/autonomous-agent/stop', { method: 'POST' });
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
    const label = btn ? btn.textContent : '单次运行';
    if (btn) {
      btn.disabled = true;
      btn.textContent = '运行中...';
    }
    try {
      const response = await rootApi('/ai/autonomous-agent/run-once', {
        method: 'POST',
        body: JSON.stringify({}),
      });
      const decision = response?.result?.decision || response?.result?.action || 'done';
      notify(`单次运行结果：${decision}`);
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
    };
    aiRoot().modules = modules;

    window.agentStart = agentStart;
    window.agentStop = agentStop;
    window.agentRunOnce = agentRunOnce;
    window.agentRefreshJournal = () => loadAgentJournal().catch(() => {});
    window.addEventListener('ai-research:state', (event) => {
      const reason = String(event?.detail?.reason || '');
      if (['refresh-workbench', 'runtime-summary', 'candidate-detail'].includes(reason)) {
        loadAgentStatus().catch(() => {});
      }
    });

    loadAgentStatus().catch(() => {});
    startPolling();
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
