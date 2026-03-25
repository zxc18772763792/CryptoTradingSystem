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

  function aiApi(path, options = {}) {
    return api(`/ai${path}`, options);
  }

  function rootApi(path, options = {}) {
    return api(path, options);
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

  async function refreshDiagnostics() {
    const summaryEl = document.getElementById('ai-data-readiness-summary');
    const detailsEl = document.getElementById('ai-data-readiness-details');
    if (!summaryEl || !detailsEl) return null;

    const exchange = currentExchange();
    const symbol = currentSymbol();
    const newsSymbol = symbolToNewsKey(symbol);

    summaryEl.textContent = '正在检查新闻、宏观与微观数据...';
    const [newsHealthRes, newsSymbolRes, newsGlobalRes, newsPullRes, newsWorkerRes, fundingDiagRes, microRes, communityRes, premiumRes] = await Promise.allSettled([
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
    const premiumRows = Object.entries(premiumData?.sources || {}).map(([name, source]) => ({
      name,
      hasCached: !!source?.has_cached_data,
      configured: !!source?.key_configured,
    }));
    const premiumCachedCount = premiumRows.filter((row) => row.hasCached).length;
    const premiumConfiguredCount = premiumRows.filter((row) => row.configured).length;
    const premiumActiveNames = premiumRows.filter((row) => row.hasCached).map((row) => row.name);

    const summary = Number(symbolSummary?.events_count || 0) > 0 || Number(symbolSummary?.feed_count || 0) > 0
      ? symbolSummary
      : globalSummary;
    const summaryScope = summary === symbolSummary ? `币种 ${newsSymbol}` : '全市场';
    const rawCount = Number(summary?.raw_count || 0);
    const feedCount = Number(summary?.feed_count || 0);
    const newsEvents = Number(summary?.events_count || 0);
    const sourceStates = Array.isArray(health?.source_states) && health.source_states.length
      ? health.source_states
      : (Array.isArray(workerStatus?.source_states) ? workerStatus.source_states : []);
    const enabledSources = Object.entries(health?.sources || {}).filter(([, enabled]) => !!enabled).length;
    const llmQueue = health?.llm_queue || workerStatus?.llm_queue || pullStatus?.llm_queue || {};
    const pendingNewsTasks = Number(llmQueue?.pending_total || 0);
    const fundingRows = Number(funding?.rows || 0);
    const fundingRate = microData?.funding_rate?.funding_rate;
    const basisPct = microData?.spot_futures_basis?.basis_pct;
    const whaleCount = Number(communityData?.whale_transfers?.count || 0);
    const announcementCount = Array.isArray(communityData?.announcements) ? communityData.announcements.length : 0;
    const issues = [];
    if (!rawCount && !feedCount) issues.push('新闻摘要为空');
    if (pendingNewsTasks > 0 && !Number(health?.sync_pull_llm)) issues.push(`LLM 队列积压 ${pendingNewsTasks} 条`);
    if (!fundingRows) issues.push('资金费率缓存为空，建议人工确认');
    if (!Number.isFinite(Number(fundingRate))) issues.push('实时 funding 不可用');
    if (!whaleCount && !announcementCount) issues.push('社区/巨鲸数据较弱');
    if (premiumConfiguredCount > 0 && premiumCachedCount === 0) issues.push('高级数据源已配置但暂未形成缓存');

    summaryEl.textContent = issues.length ? `待处理: ${issues.join(' / ')}` : '新闻、宏观与微观数据已就绪';

    const fundingPath = String(funding?.cache_path || '--');
    const coverage = funding?.coverage || {};
    const lastPull = workerStatus?.last_pull || pullStatus?.latest_result || {};
    const lastLlm = workerStatus?.last_llm_batch || {};
    detailsEl.innerHTML = `
      <div style="padding:8px;background:#141f2f;border-radius:6px;">
        <div style="color:#c2d0e8;font-weight:700;margin-bottom:4px;">新闻诊断</div>
        <div>摘要范围 ${esc(summaryScope)} / 结构化事件 ${newsEvents} / 原始新闻 ${rawCount} / Feed ${feedCount}</div>
        <div>启用源 ${enabledSources} / 源状态 ${sourceStates.length} / LLM 队列 ${pendingNewsTasks}</div>
        <div>最近拉取 ${esc(lastPull?.timestamp ? fmtTs(lastPull.timestamp) : '--')} / 最近 LLM ${esc(lastLlm?.timestamp ? fmtTs(lastLlm.timestamp) : '--')}</div>
      </div>
      <div style="padding:8px;background:#141f2f;border-radius:6px;">
        <div style="color:#c2d0e8;font-weight:700;margin-bottom:4px;">宏观 / 资金费率</div>
        <div>缓存行数 ${fundingRows} / Funding ${Number.isFinite(Number(fundingRate)) ? Number(fundingRate).toFixed(6) : '--'} / Basis ${Number.isFinite(Number(basisPct)) ? `${Number(basisPct).toFixed(3)}%` : '--'}</div>
        <div>覆盖区间 ${esc(coverage?.start || '--')} ~ ${esc(coverage?.end || '--')}</div>
        <div style="margin-top:4px;color:#7e92b2;">Funding 缓存路径: ${esc(fundingPath)}</div>
      </div>
      <div style="padding:8px;background:#141f2f;border-radius:6px;">
        <div style="color:#c2d0e8;font-weight:700;margin-bottom:4px;">社区 / 巨鲸 / 公告</div>
        <div>巨鲸 ${whaleCount} / 公告 ${announcementCount} / 微观点差 ${Number(microData?.orderbook?.spread_bps || 0).toFixed(2)} bps</div>
      </div>
      <div style="padding:8px;background:#141f2f;border-radius:6px;">
        <div style="color:#c2d0e8;font-weight:700;margin-bottom:4px;">高级数据源</div>
        <div>缓存 ${premiumCachedCount}/${premiumRows.length} / Key 已配置 ${premiumConfiguredCount}</div>
        <div>${premiumActiveNames.length ? `活跃源 ${esc(premiumActiveNames.join(' / '))}` : '暂无活跃缓存源（可选）'}</div>
      </div>
      <div style="padding:8px;background:#141f2f;border-radius:6px;">
        <div style="color:#c2d0e8;font-weight:700;margin-bottom:4px;">存储说明</div>
        <div style="margin-top:3px;color:#9fb1c9;">新闻库: ./data/crypto_trading.db</div>
        <div style="margin-top:3px;color:#9fb1c9;">资金费率缓存: ${esc(fundingPath)}</div>
        <div style="margin-top:3px;color:#9fb1c9;">高级源缓存: ./data/premium/*</div>
        <div style="margin-top:3px;color:#9fb1c9;">当币种新闻过少时会自动回退到全市场摘要，避免诊断全 0。</div>
      </div>
    `;

    if (typeof aiRoot().emitState === 'function') {
      aiRoot().emitState('diagnostics-refresh', { exchange, symbol });
    }

    return {
      exchange,
      symbol,
      issues,
      summary_scope: summaryScope,
    };
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
    await refreshDiagnostics().catch(() => {});
    return result;
  }

  async function warmFunding() {
    const exchange = currentExchange();
    const symbol = currentSymbol();
    const result = await aiApi('/diagnostics/funding-cache/warm', {
      method: 'POST',
      body: JSON.stringify({ exchange, symbol, days: 90, source: 'auto' }),
      timeoutMs: 30000,
    });
    const path = String(result?.funding?.cache_path || '');
    notify(path ? `宏观缓存已预热: ${path}` : '宏观缓存已预热');
    await refreshDiagnostics().catch(() => {});
    return result;
  }

  function init() {
    if (!document.getElementById('ai-data-readiness-panel')) return;
    const modules = aiRoot().modules || {};
    modules.diagnostics = {
      refresh: () => refreshDiagnostics(),
      pullNews: () => pullNews(),
      warmFunding: () => warmFunding(),
    };
    aiRoot().modules = modules;
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
