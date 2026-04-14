(function () {
  'use strict';

  const DIAGNOSTICS_REFRESH_MS = 30000;
  const DIAGNOSTICS_MIN_GAP_MS = 15000;

  let diagnosticsTimer = null;
  let diagnosticsScheduleTimer = null;
  let diagnosticsRefreshInFlight = null;
  let diagnosticsLastRefreshAt = 0;
  let initialized = false;

  function aiRoot() {
    return window.AI || {};
  }

  function esc(value) {
    if (typeof aiRoot().util?.esc === 'function') return aiRoot().util.esc(value);
    return String(value ?? '').replace(/[&<>"']/g, (match) => (
      { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[match]
    ));
  }

  function fmtTs(value) {
    if (typeof aiRoot().util?.fmtTs === 'function') return aiRoot().util.fmtTs(value);
    return String(value || '--');
  }

  function notify(message, isError = false) {
    if (typeof aiRoot().util?.notify === 'function') {
      aiRoot().util.notify(message, isError);
      return;
    }
    if (isError) console.error(message);
    else console.log(message);
  }

  async function api(path, options = {}) {
    if (typeof window.api === 'function') return window.api(path, options);

    const { timeoutMs = 15000, ...rest } = options || {};
    const controller = new AbortController();
    const timer = window.setTimeout(() => controller.abort(), Math.max(1000, Number(timeoutMs || 15000)));
    try {
      const response = await fetch(path, {
        ...rest,
        signal: controller.signal,
        headers: {
          'Content-Type': 'application/json',
          ...(rest.headers || {}),
        },
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(payload.detail || payload.error || `Request failed (${response.status})`);
      return payload;
    } catch (err) {
      if (err?.name === 'AbortError') throw new Error(`Request timed out (${timeoutMs}ms): ${path}`);
      throw err;
    } finally {
      window.clearTimeout(timer);
    }
  }

  function aiApi(path, options = {}) {
    return api(`/ai${path}`, options);
  }

  function rootApi(path, options = {}) {
    return api(path, options);
  }

  function isAiResearchActive() {
    const tab = document.getElementById('ai-research');
    return !!(tab && tab.classList.contains('active'));
  }

  function canRunAiPolling() {
    if (typeof window === 'undefined' || typeof window.__ctsSharedPolling?.canRun !== 'function') return true;
    return window.__ctsSharedPolling.canRun('ai');
  }

  function currentExchange() {
    return String(
      document.getElementById('run-exchange')?.value
      || document.getElementById('research-exchange')?.value
      || document.getElementById('data-exchange')?.value
      || 'binance'
    ).trim() || 'binance';
  }

  function currentSymbol() {
    const planner = String(document.getElementById('ai-planner-symbols')?.value || '').trim();
    const primary = planner.split(',').map((item) => item.trim()).filter(Boolean)[0];
    const fallback = String(
      document.getElementById('research-symbol')?.value
      || document.getElementById('data-symbol')?.value
      || 'BTC/USDT'
    ).trim();
    return primary || fallback || 'BTC/USDT';
  }

  function symbolToNewsKey(symbol) {
    const raw = String(symbol || '').trim().toUpperCase();
    const main = raw.split(':')[0];
    if (main.includes('/')) return main.split('/')[0];
    return main.replace(/(USDT|USDC|FDUSD|BUSD|USD)$/i, '') || main;
  }

  function seedDiagnosticsPlaceholder() {
    const summaryEl = document.getElementById('ai-data-readiness-summary');
    const detailsEl = document.getElementById('ai-data-readiness-details');
    if (!summaryEl || !detailsEl) return;
    if (!String(summaryEl.textContent || '').trim()) {
      summaryEl.textContent = '首屏暂不抢重接口，可稍后自动补齐或手动刷新。';
    }
    if (!String(detailsEl.textContent || '').trim()) {
      detailsEl.innerHTML = '<div style="padding:8px;background:#141f2f;border-radius:6px;">研究工作台会先显示提案、候选和运行信号，再补新闻/宏观诊断，避免首屏长时间空白。</div>';
    }
  }

  function renderDiagnostics(summaryEl, detailsEl, payload) {
    const health = payload.health || {};
    const summary = payload.summary || {};
    const pullStatus = payload.pullStatus || {};
    const workerStatus = payload.workerStatus || {};
    const funding = payload.funding || {};
    const microData = payload.microData || {};
    const communityData = payload.communityData || {};
    const premiumData = payload.premiumData || {};
    const summaryScope = String(payload.summaryScope || '--');
    const issues = Array.isArray(payload.issues) ? payload.issues : [];

    const rawCount = Number(summary.raw_count || 0);
    const feedCount = Number(summary.feed_count || 0);
    const newsEvents = Number(summary.events_count || 0);
    const sourceStates = Array.isArray(health.source_states) && health.source_states.length
      ? health.source_states
      : (Array.isArray(workerStatus.source_states) ? workerStatus.source_states : []);
    const enabledSources = Object.entries(health.sources || {}).filter(([, enabled]) => !!enabled).length;
    const llmQueue = health.llm_queue || workerStatus.llm_queue || pullStatus.llm_queue || {};
    const pendingNewsTasks = Number(llmQueue.pending_total || 0);

    const fundingRows = Number(funding.rows || 0);
    const fundingRate = Number(microData?.funding_rate?.funding_rate);
    const basisPct = Number(microData?.spot_futures_basis?.basis_pct);
    const fundingPath = String(funding.cache_path || '--');
    const coverage = funding.coverage || {};

    const whaleCount = Number(communityData?.whale_transfers?.count || 0);
    const announcementCount = Array.isArray(communityData?.announcements) ? communityData.announcements.length : 0;
    const spreadBps = Number(microData?.orderbook?.spread_bps || 0);

    const premiumRows = Object.entries(premiumData?.sources || {}).map(([name, source]) => ({
      name,
      hasCached: !!source?.has_cached_data,
      configured: !!source?.key_configured,
    }));
    const premiumCachedCount = premiumRows.filter((row) => row.hasCached).length;
    const premiumConfiguredCount = premiumRows.filter((row) => row.configured).length;
    const premiumActiveNames = premiumRows.filter((row) => row.hasCached).map((row) => row.name);

    const lastPull = workerStatus.last_pull || pullStatus.latest_result || {};
    const lastLlm = workerStatus.last_llm_batch || {};

    summaryEl.textContent = issues.length
      ? `待处理: ${issues.join(' / ')}`
      : '新闻、宏观和微观数据已就绪';

    detailsEl.innerHTML = `
      <div style="padding:8px;background:#141f2f;border-radius:6px;">
        <div style="color:#c2d0e8;font-weight:700;margin-bottom:4px;">新闻诊断</div>
        <div>范围 ${esc(summaryScope)} / 结构化事件 ${newsEvents} / 原始新闻 ${rawCount} / Feed ${feedCount}</div>
        <div>启用源 ${enabledSources} / 源状态 ${sourceStates.length} / LLM 队列 ${pendingNewsTasks}</div>
        <div>最近拉取 ${esc(lastPull?.timestamp ? fmtTs(lastPull.timestamp) : '--')} / 最近 LLM ${esc(lastLlm?.timestamp ? fmtTs(lastLlm.timestamp) : '--')}</div>
      </div>
      <div style="padding:8px;background:#141f2f;border-radius:6px;">
        <div style="color:#c2d0e8;font-weight:700;margin-bottom:4px;">宏观 / 资金费率</div>
        <div>缓存行数 ${fundingRows} / Funding ${Number.isFinite(fundingRate) ? fundingRate.toFixed(6) : '--'} / Basis ${Number.isFinite(basisPct) ? `${basisPct.toFixed(3)}%` : '--'}</div>
        <div>覆盖区间 ${esc(coverage?.start || '--')} ~ ${esc(coverage?.end || '--')}</div>
        <div style="margin-top:4px;color:#7e92b2;">Funding 缓存路径: ${esc(fundingPath)}</div>
      </div>
      <div style="padding:8px;background:#141f2f;border-radius:6px;">
        <div style="color:#c2d0e8;font-weight:700;margin-bottom:4px;">社区 / 巨鲸 / 公告</div>
        <div>巨鲸 ${whaleCount} / 公告 ${announcementCount} / 微观点差 ${spreadBps.toFixed(2)} bps</div>
      </div>
      <div style="padding:8px;background:#141f2f;border-radius:6px;">
        <div style="color:#c2d0e8;font-weight:700;margin-bottom:4px;">高级数据源</div>
        <div>缓存 ${premiumCachedCount}/${premiumRows.length} / Key 已配置 ${premiumConfiguredCount}</div>
        <div>${premiumActiveNames.length ? `活跃源 ${esc(premiumActiveNames.join(' / '))}` : '暂无活跃缓存源（可选）'}</div>
      </div>
      <div style="padding:8px;background:#141f2f;border-radius:6px;">
        <div style="color:#c2d0e8;font-weight:700;margin-bottom:4px;">存储说明</div>
        <div style="margin-top:3px;color:#9fb1c9;">新闻库: ./data/crypto_trading.db</div>
        <div style="margin-top:3px;color:#9fb1c9;">Funding 缓存: ${esc(fundingPath)}</div>
        <div style="margin-top:3px;color:#9fb1c9;">高级源缓存: ./data/premium/*</div>
        <div style="margin-top:3px;color:#9fb1c9;">当前币种新闻过少时会自动回退到全市场摘要，避免诊断全 0。</div>
      </div>
    `;
  }

  async function refreshDiagnostics(options = {}) {
    const summaryEl = document.getElementById('ai-data-readiness-summary');
    const detailsEl = document.getElementById('ai-data-readiness-details');
    if (!summaryEl || !detailsEl) return null;

    const { force = false, reason = 'manual' } = options || {};
    if (diagnosticsRefreshInFlight) return diagnosticsRefreshInFlight;
    if (!force && diagnosticsLastRefreshAt && (Date.now() - diagnosticsLastRefreshAt) < DIAGNOSTICS_MIN_GAP_MS) {
      return null;
    }

    const task = (async () => {
      const exchange = currentExchange();
      const symbol = currentSymbol();
      const newsSymbol = symbolToNewsKey(symbol);

      summaryEl.textContent = '正在检查新闻、宏观与微观数据...';

      try {
        const [
          newsHealthRes,
          newsSymbolRes,
          newsGlobalRes,
          newsPullRes,
          newsWorkerRes,
          fundingDiagRes,
          microRes,
          communityRes,
          premiumRes,
        ] = await Promise.allSettled([
          rootApi('/news/health', { timeoutMs: 12000 }),
          rootApi(`/news/summary?symbol=${encodeURIComponent(newsSymbol)}&hours=24`, { timeoutMs: 12000 }),
          rootApi('/news/summary?hours=24', { timeoutMs: 12000 }),
          rootApi('/news/pull_status', { timeoutMs: 12000 }),
          rootApi('/news/worker_status', { timeoutMs: 12000 }),
          aiApi(`/diagnostics/funding-cache?exchange=${encodeURIComponent(exchange)}&symbol=${encodeURIComponent(symbol)}&days=60`, { timeoutMs: 12000 }),
          rootApi(`/trading/analytics/microstructure?exchange=${encodeURIComponent(exchange)}&symbol=${encodeURIComponent(symbol)}&depth_limit=20`, { timeoutMs: 12000 }),
          rootApi(`/trading/analytics/community/overview?exchange=${encodeURIComponent(exchange)}&symbol=${encodeURIComponent(symbol)}`, { timeoutMs: 12000 }),
          aiApi('/premium-data/status', { timeoutMs: 12000 }),
        ]);

        const health = newsHealthRes.status === 'fulfilled' ? (newsHealthRes.value || {}) : {};
        const symbolSummary = newsSymbolRes.status === 'fulfilled' ? (newsSymbolRes.value || {}) : {};
        const globalSummary = newsGlobalRes.status === 'fulfilled' ? (newsGlobalRes.value || {}) : {};
        const pullStatus = newsPullRes.status === 'fulfilled' ? (newsPullRes.value || {}) : {};
        const workerStatus = newsWorkerRes.status === 'fulfilled' ? (newsWorkerRes.value || {}) : {};
        const funding = fundingDiagRes.status === 'fulfilled' ? (fundingDiagRes.value?.funding || {}) : {};
        const microData = microRes.status === 'fulfilled' ? (microRes.value || {}) : {};
        const communityData = communityRes.status === 'fulfilled' ? (communityRes.value || {}) : {};
        const premiumData = premiumRes.status === 'fulfilled' ? (premiumRes.value || {}) : {};

        const summary = Number(symbolSummary?.events_count || 0) > 0 || Number(symbolSummary?.feed_count || 0) > 0
          ? symbolSummary
          : globalSummary;
        const summaryScope = summary === symbolSummary ? `币种 ${newsSymbol}` : '全市场';

        const llmQueue = health?.llm_queue || workerStatus?.llm_queue || pullStatus?.llm_queue || {};
        const pendingNewsTasks = Number(llmQueue?.pending_total || 0);
        const fundingRows = Number(funding?.rows || 0);
        const fundingRate = Number(microData?.funding_rate?.funding_rate);
        const premiumSources = Object.entries(premiumData?.sources || {});
        const premiumConfiguredCount = premiumSources.filter(([, source]) => !!source?.key_configured).length;
        const premiumCachedCount = premiumSources.filter(([, source]) => !!source?.has_cached_data).length;
        const whaleCount = Number(communityData?.whale_transfers?.count || 0);
        const announcementCount = Array.isArray(communityData?.announcements) ? communityData.announcements.length : 0;

        const issues = [];
        if (!Number(summary?.raw_count || 0) && !Number(summary?.feed_count || 0)) issues.push('新闻摘要为空');
        if (pendingNewsTasks > 0 && !Number(health?.sync_pull_llm)) issues.push(`LLM 队列积压 ${pendingNewsTasks} 条`);
        if (!fundingRows) issues.push('资金费率缓存为空，建议人工确认');
        if (!Number.isFinite(fundingRate)) issues.push('实时 funding 不可用');
        if (!whaleCount && !announcementCount) issues.push('社区/巨鲸数据偏弱');
        if (premiumConfiguredCount > 0 && premiumCachedCount === 0) issues.push('高级数据源已配置，但暂无缓存');

        const payload = {
          exchange,
          symbol,
          summaryScope,
          summary,
          health,
          pullStatus,
          workerStatus,
          funding,
          microData,
          communityData,
          premiumData,
          issues,
        };

        renderDiagnostics(summaryEl, detailsEl, payload);
        diagnosticsLastRefreshAt = Date.now();

        if (typeof aiRoot().emitState === 'function') {
          aiRoot().emitState('diagnostics-refresh', { exchange, symbol, reason });
        }

        return {
          exchange,
          symbol,
          issues,
          summary_scope: summaryScope,
        };
      } catch (err) {
        summaryEl.textContent = `数据诊断加载失败: ${String(err?.message || err)}`;
        detailsEl.innerHTML = '<div style="padding:8px;background:#141f2f;border-radius:6px;">请稍后重试，或点击“刷新诊断”。</div>';
        throw err;
      }
    })();

    diagnosticsRefreshInFlight = task;
    try {
      return await task;
    } finally {
      if (diagnosticsRefreshInFlight === task) diagnosticsRefreshInFlight = null;
    }
  }

  async function pullNews() {
    const result = await rootApi('/news/pull_now?background=true', {
      method: 'POST',
      body: JSON.stringify({
        since_minutes: 720,
        max_records: 120,
        query: symbolToNewsKey(currentSymbol()),
      }),
      timeoutMs: 12000,
    });
    if (Number(result?.queued_count || result?.job?.result?.queued_count || 0) > 0) {
      rootApi('/news/worker/run_once?llm_limit=12&background=true', {
        method: 'POST',
        timeoutMs: 8000,
      }).catch(() => ({}));
    }
    notify(result?.queued ? '新闻拉取任务已提交' : '新闻拉取完成');
    await refreshDiagnostics({ force: true, reason: 'post-news-pull' }).catch(() => {});
    return result;
  }

  async function warmFunding() {
    const exchange = currentExchange();
    const symbol = currentSymbol();
    const [fundingResult, macroResult] = await Promise.allSettled([
      aiApi('/diagnostics/funding-cache/warm', {
        method: 'POST',
        body: JSON.stringify({ exchange, symbol, days: 90, source: 'auto' }),
        timeoutMs: 30000,
      }),
      aiApi('/diagnostics/macro-cache/warm', {
        method: 'POST',
        timeoutMs: 45000,
      }),
    ]);
    if (fundingResult.status !== 'fulfilled' && macroResult.status !== 'fulfilled') {
      const errors = [fundingResult, macroResult]
        .filter((item) => item.status === 'rejected')
        .map((item) => String(item.reason?.message || item.reason || '').trim())
        .filter(Boolean);
      throw new Error(errors.join(' / ') || 'research cache warm failed');
    }

    const parts = [];
    const partialErrors = [];
    const fundingPayload = fundingResult.status === 'fulfilled' ? (fundingResult.value || {}) : null;
    const macroPayload = macroResult.status === 'fulfilled' ? (macroResult.value || {}) : null;

    if (fundingPayload) {
      const path = String(fundingPayload?.funding?.cache_path || '');
      parts.push(path ? `Funding ${path}` : 'Funding');
    } else {
      partialErrors.push(String(fundingResult.reason?.message || fundingResult.reason || 'funding warm failed'));
    }

    if (macroPayload) {
      const activeSeriesCount = Number(macroPayload?.macro?.active_series_count || 0);
      parts.push(activeSeriesCount > 0 ? `Macro ${activeSeriesCount} 项` : 'Macro');
    } else {
      partialErrors.push(String(macroResult.reason?.message || macroResult.reason || 'macro warm failed'));
    }

    const suffix = partialErrors.length ? `；部分失败: ${partialErrors.join(' / ')}` : '';
    notify(`研究缓存已预热: ${parts.join(' + ')}${suffix}`);
    await refreshDiagnostics({ force: true, reason: 'post-research-warm' }).catch(() => {});
    return {
      warmed: true,
      funding: fundingPayload?.funding || null,
      macro: macroPayload?.macro || null,
      partial_errors: partialErrors,
    };
  }

  function stopDiagnosticsPolling() {
    if (diagnosticsTimer) clearInterval(diagnosticsTimer);
    if (diagnosticsScheduleTimer) clearTimeout(diagnosticsScheduleTimer);
    diagnosticsTimer = null;
    diagnosticsScheduleTimer = null;
  }

  function scheduleDiagnosticsRefresh(delayMs = 0, options = {}) {
    if (document.hidden || !isAiResearchActive() || !canRunAiPolling()) return;
    if (diagnosticsScheduleTimer) clearTimeout(diagnosticsScheduleTimer);
    diagnosticsScheduleTimer = window.setTimeout(() => {
      diagnosticsScheduleTimer = null;
      if (document.hidden || !isAiResearchActive() || !canRunAiPolling()) return;
      refreshDiagnostics({
        force: !!options.force,
        reason: String(options.reason || 'scheduled'),
      }).catch((err) => notify(`数据诊断刷新失败: ${err.message}`, true));
    }, Math.max(0, Number(delayMs || 0)));
  }

  function syncDiagnosticsPollingState({ immediate = false } = {}) {
    if (document.hidden || !isAiResearchActive() || !canRunAiPolling()) {
      stopDiagnosticsPolling();
      return;
    }

    if (!diagnosticsTimer) {
      diagnosticsTimer = window.setInterval(() => {
        if (document.hidden || !isAiResearchActive() || !canRunAiPolling()) {
          stopDiagnosticsPolling();
          return;
        }
        refreshDiagnostics({ reason: 'interval' }).catch(() => {});
      }, DIAGNOSTICS_REFRESH_MS);
    }

    if (immediate) scheduleDiagnosticsRefresh(3500, { reason: 'activation' });
  }

  function init() {
    if (!document.getElementById('ai-data-readiness-panel') || initialized) return;
    initialized = true;
    seedDiagnosticsPlaceholder();

    const modules = aiRoot().modules || {};
    modules.diagnostics = {
      refresh: (options = {}) => refreshDiagnostics({ force: true, ...(options || {}) }),
      pullNews: () => pullNews(),
      warmFunding: () => warmFunding(),
    };
    aiRoot().modules = modules;

    ['ai-planner-symbols', 'run-exchange', 'research-exchange', 'research-symbol'].forEach((id) => {
      document.getElementById(id)?.addEventListener('change', () => {
        scheduleDiagnosticsRefresh(1500, { reason: 'inputs-changed' });
      });
    });

    window.addEventListener('ai-research:state', (event) => {
      const reason = String(event?.detail?.reason || '');
      if (['refresh-workbench', 'agent-status', 'candidate-detail'].includes(reason)) {
        scheduleDiagnosticsRefresh(2500, { reason: `state:${reason}` });
      }
    });

    document.querySelector('.tab-btn[data-tab="ai-research"]')?.addEventListener('click', () => {
      window.setTimeout(() => {
        canRunAiPolling();
        syncDiagnosticsPollingState({ immediate: true });
      }, 120);
    });

    document.addEventListener('click', (event) => {
      if (!(event.target instanceof Element) || !event.target.closest('.tab-btn')) return;
      window.setTimeout(() => syncDiagnosticsPollingState(), 0);
    });

    document.addEventListener('visibilitychange', () => syncDiagnosticsPollingState({
      immediate: !document.hidden && isAiResearchActive() && canRunAiPolling(),
    }));

    window.addEventListener('hashchange', () => {
      window.setTimeout(() => syncDiagnosticsPollingState({
        immediate: !document.hidden && isAiResearchActive() && canRunAiPolling(),
      }), 0);
    });

    syncDiagnosticsPollingState({ immediate: isAiResearchActive() && canRunAiPolling() });
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
