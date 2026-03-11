(function () {
  const state = {
    initialized: false,
    context: null,
    profile: null,
    overview: null,
    modules: {},
    moduleTimes: {},
    recommendations: null,
    lastDebug: null,
    retryTimers: {},
  };

  const MODULE_NAMES = ['market_state', 'factors', 'cross_asset', 'onchain', 'discipline'];
  const MODULE_LABELS = {
    market_state: '市场状态',
    factors: '因子与风格',
    cross_asset: '多币种轮动',
    onchain: '链上与外生',
    discipline: '纪律与风控',
  };
  const STATUS_LABELS = {
    idle: '待运行',
    loading: '加载中',
    ok: '正常',
    degraded: '降级',
    error: '失败',
  };

  function q(id) {
    return document.getElementById(id);
  }

  function escSafe(value) {
    if (typeof window.esc === 'function') return window.esc(value);
    return String(value ?? '').replace(/[&<>"']/g, (match) => (
      { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[match]
    ));
  }

  function fmtTime(value) {
    try {
      return new Date(value).toLocaleString('zh-CN', { hour12: false });
    } catch {
      return String(value || '-');
    }
  }

  function apiResearch(path, options = {}) {
    if (typeof window.api !== 'function') throw new Error('API 未初始化');
    return window.api(`/research/workbench${path}`, options);
  }

  function listItem(label, value) {
    return `<div class="list-item"><span>${escSafe(label)}</span><span>${escSafe(String(value ?? '-'))}</span></div>`;
  }

  function isResearchActive() {
    return document.querySelector('.tab-content.active')?.id === 'research';
  }

  function normalizeUniverse(primarySymbol, universeSymbols) {
    const normalized = [];
    const seen = new Set();
    [primarySymbol, ...(Array.isArray(universeSymbols) ? universeSymbols : [])].forEach((item) => {
      const symbol = String(item || '').trim().toUpperCase();
      if (!symbol || seen.has(symbol)) return;
      seen.add(symbol);
      normalized.push(symbol);
    });
    return normalized.length ? normalized : [primarySymbol || 'BTC/USDT'];
  }

  function applyWorkbenchDefaults() {
    const timeframeEl = q('research-timeframe');
    const lookbackEl = q('research-lookback');
    const symbolEl = q('research-symbol');
    const universeEl = q('research-symbols');
    if (timeframeEl && (!timeframeEl.value || timeframeEl.value === '1h')) timeframeEl.value = '5m';
    if (lookbackEl && (!lookbackEl.value || Number(lookbackEl.value) === 1000)) lookbackEl.value = 1200;
    if (symbolEl && !String(symbolEl.value || '').trim()) symbolEl.value = 'BTC/USDT';
    if (universeEl) {
      const selected = Array.from(universeEl.selectedOptions || []).map((opt) => opt.value);
      if (selected.length > 30 || selected.length === 0) {
        const allow = new Set(['BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'SOL/USDT', 'XRP/USDT', 'ADA/USDT', 'DOGE/USDT', 'TRX/USDT', 'LINK/USDT', 'AVAX/USDT', 'DOT/USDT', 'POL/USDT', 'LTC/USDT', 'BCH/USDT', 'ETC/USDT', 'ATOM/USDT', 'NEAR/USDT', 'APT/USDT', 'ARB/USDT', 'OP/USDT', 'SUI/USDT', 'INJ/USDT', 'RUNE/USDT', 'AAVE/USDT', 'MKR/USDT', 'UNI/USDT', 'FIL/USDT', 'HBAR/USDT', 'ICP/USDT', 'TON/USDT']);
        Array.from(universeEl.options || []).forEach((opt) => {
          opt.selected = allow.has(opt.value);
        });
      }
    }
  }

  function getProfile() {
    const primarySymbol = String(q('research-symbol')?.value || 'BTC/USDT').trim().toUpperCase() || 'BTC/USDT';
    const selectedUniverse = typeof window.getResearchSymbols === 'function' ? window.getResearchSymbols() : [primarySymbol];
    return {
      exchange: q('research-exchange')?.value || 'binance',
      primary_symbol: primarySymbol,
      universe_symbols: normalizeUniverse(primarySymbol, selectedUniverse),
      timeframe: q('research-timeframe')?.value || '5m',
      lookback: Math.max(120, Number(q('research-lookback')?.value || 1200)),
      exclude_retired: q('research-exclude-retired')?.checked !== false,
      horizon: 'short_intraday',
    };
  }

  function profileQuery(profile = state.profile || getProfile()) {
    const params = new URLSearchParams();
    params.set('exchange', profile.exchange || 'binance');
    params.set('primary_symbol', profile.primary_symbol || 'BTC/USDT');
    params.set('universe_symbols', (profile.universe_symbols || []).join(','));
    params.set('timeframe', profile.timeframe || '5m');
    params.set('lookback', String(Math.max(120, Number(profile.lookback || 1200))));
    params.set('exclude_retired', profile.exclude_retired === false ? 'false' : 'true');
    params.set('horizon', profile.horizon || 'short_intraday');
    return params.toString();
  }

  function setSelectOptions(select, values, selected) {
    if (!select) return;
    const selectedSet = new Set(Array.isArray(selected) ? selected : [selected].filter(Boolean));
    select.innerHTML = (Array.isArray(values) ? values : []).map((value) => {
      const text = String(value || '').trim();
      return `<option value="${escSafe(text)}"${selectedSet.has(text) ? ' selected' : ''}>${escSafe(text)}</option>`;
    }).join('');
  }

  function setDebug(title, payload) {
    state.lastDebug = { title, payload };
    const summary = q('research-debug-summary');
    const output = q('research-output');
    if (summary) summary.textContent = `${title} | ${fmtTime(new Date().toISOString())}`;
    if (output) output.textContent = typeof payload === 'string' ? payload : JSON.stringify(payload, null, 2);
  }

  function buildLocalOverviewFromModules() {
    const marketSummary = state.modules?.market_state?.summary || {};
    const entries = Object.values(state.modules || {});
    return {
      market_regime: marketSummary.market_regime || marketSummary.headline || '待生成',
      direction_bias: marketSummary.direction_bias || 'neutral',
      confidence: Number(marketSummary.confidence || 0),
      coverage: {
        ok_count: entries.filter((item) => item?.status === 'ok').length,
        degraded_count: entries.filter((item) => item?.status === 'degraded').length,
        total: MODULE_NAMES.length,
      },
    };
  }

  function buildLocalRecommendations() {
    const overview = state.overview || buildLocalOverviewFromModules();
    const factors = state.modules?.factors?.summary || {};
    const cross = state.modules?.cross_asset?.summary || {};
    const onchain = state.modules?.onchain?.summary || {};
    const nextActions = [];
    if ((factors.top_symbols || []).length) nextActions.push(`优先观察 ${(factors.top_symbols || []).join(' / ')}`);
    if (cross.leader_symbol) nextActions.push(`确认多币种龙头 ${cross.leader_symbol} 的持续性`);
    if (Number(onchain.whale_count || 0) > 0) nextActions.push(`链上巨鲸 ${onchain.whale_count} 笔，防止外生扰动`);
    if (!nextActions.length) nextActions.push('先补齐因子、多币种和链上模块，再决定执行方向');
    return {
      headline: overview.market_regime || '研究建议待生成',
      direction_bias: overview.direction_bias || 'neutral',
      preferred_strategy_families: overview.direction_bias === 'bullish'
        ? ['趋势跟随', '动量轮动']
        : overview.direction_bias === 'bearish'
          ? ['防守对冲', '回撤控制']
          : ['均值回归', '轻仓观察'],
      next_actions: nextActions,
      avoid_conditions: (state.modules?.market_state?.warnings || []).slice(0, 3),
      backtest_jump_targets: [],
    };
  }

  const MODULE_STATUS_COLORS = { ok: '#20bf78', degraded: '#f59e0b', error: '#e05260', idle: '#6b7fa0', loading: '#3aa6ff' };

  function setStatusItemValue(el, html) {
    if (!el) return;
    const valEl = el.querySelector('.status-value');
    if (valEl) valEl.innerHTML = html;
    else el.innerHTML = html; // fallback
  }

  function renderStatusCards() {
    const profile = state.profile || getProfile();
    const configEl = q('research-config-snapshot');
    const dataEl = q('research-data-snapshot');
    const moduleEl = q('research-module-snapshot');
    const nextEl = q('research-next-step');
    if (!configEl || !dataEl || !moduleEl || !nextEl) return;

    const moduleEntries = Object.values(state.modules || {});
    const okCount = moduleEntries.filter((item) => item?.status === 'ok').length;
    const degradedCount = moduleEntries.filter((item) => item?.status === 'degraded').length;
    const errorCount = moduleEntries.filter((item) => item?.status === 'error').length;
    const newsEvents = Number(
      state.modules?.onchain?.payload?.news_summary?.events_count
      || state.modules?.market_state?.payload?.sentiment_dashboard?.news?.events_count
      || 0
    );
    const whaleCount = Number(state.modules?.onchain?.payload?.onchain?.whale_activity?.count || 0);

    setStatusItemValue(configEl,
      `${escSafe(profile.exchange)} / ${escSafe(profile.primary_symbol)} / ${escSafe(profile.timeframe)}<div style="color:#7a8fa6;font-size:11px;margin-top:2px;">lookback ${profile.lookback} · 币池 ${profile.universe_symbols.length}</div>`
    );
    setStatusItemValue(dataEl,
      `<span style="color:#20bf78;">✔ ${okCount}</span> <span style="color:#f59e0b;">⚡ ${degradedCount}</span> <span style="color:#e05260;">✘ ${errorCount}</span>`
      + `<div style="color:#7a8fa6;font-size:11px;margin-top:2px;">新闻事件 ${newsEvents} · 巨鲸 ${whaleCount}</div>`
    );

    // Per-module colored chips with last-updated time
    const chips = MODULE_NAMES.map((name) => {
      const mod = state.modules?.[name];
      const st = mod?.status || 'idle';
      const color = MODULE_STATUS_COLORS[st] || '#6b7fa0';
      const t = state.moduleTimes?.[name];
      const timeStr = t ? new Date(t).toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' }) : '';
      const label = MODULE_LABELS[name] || name;
      const title = `${label}：${STATUS_LABELS[st] || st}${timeStr ? ' · ' + timeStr : ''}`;
      return `<span class="module-chip" style="color:${color};border-color:${color}40;" title="${escSafe(title)}"><span class="chip-dot" style="background:${color};"></span>${escSafe(label.slice(0, 2))}${timeStr ? `<span class="chip-time">${escSafe(timeStr)}</span>` : ''}</span>`;
    }).join('');
    setStatusItemValue(moduleEl, chips);

    let nextHtml = '先运行研究总览';
    if (state.recommendations?.next_actions?.length) nextHtml = escSafe(state.recommendations.next_actions[0]);
    else if (state.overview && !state.recommendations) nextHtml = '刷新研究建议';
    else if (errorCount > 0) nextHtml = '先修复失败模块，再看综合结论';
    setStatusItemValue(nextEl, nextHtml);
  }

  function renderOverview() {
    const summaryEl = q('research-overview-summary-list');
    const actionEl = q('research-overview-actions');
    if (!summaryEl || !actionEl) return;

    if (!state.overview) {
      summaryEl.innerHTML = listItem('状态', '等待运行研究总览');
      actionEl.innerHTML = listItem('建议动作', '先运行研究总览');
      return;
    }

    const overview = state.overview;
    const jumpText = Array.isArray(state.recommendations?.backtest_jump_targets) && state.recommendations.backtest_jump_targets.length
      ? state.recommendations.backtest_jump_targets.map((item) => item.label).join(' / ')
      : '等待生成';

    summaryEl.innerHTML = [
      listItem('市场状态', overview.market_regime || '-'),
      listItem('方向偏向', overview.direction_bias || '-'),
      listItem('研究可信度', Number(overview.confidence || 0).toFixed(2)),
      listItem('模块覆盖', `${Number(overview.coverage?.ok_count || 0)}/${Number(overview.coverage?.total || 0)}`),
      listItem('降级模块', Number(overview.coverage?.degraded_count || 0)),
    ].join('');

    actionEl.innerHTML = [
      listItem('研究结论', state.recommendations?.headline || overview.market_regime || '-'),
      listItem('推荐方向', state.recommendations?.direction_bias || overview.direction_bias || '-'),
      listItem('推荐跳转', jumpText),
    ].join('');
  }

  function renderRecommendations() {
    const summaryEl = q('research-conclusion-summary');
    const bulletEl = q('research-conclusion-bullets');
    if (!summaryEl || !bulletEl) return;

    if (!state.recommendations) {
      summaryEl.innerHTML = listItem('状态', '等待研究建议');
      bulletEl.innerHTML = '<div class="research-conclusion-empty">暂无建议。先运行“研究总览”。</div>';
      return;
    }

    const rec = state.recommendations;
    summaryEl.innerHTML = [
      listItem('研究结论', rec.headline || '-'),
      listItem('方向偏向', rec.direction_bias || '-'),
      listItem('策略家族', (rec.preferred_strategy_families || []).join(' / ') || '-'),
    ].join('');

    const rows = [];
    (rec.next_actions || []).slice(0, 4).forEach((text) => rows.push({ title: '下一步动作', badge: '执行', body: text }));
    (rec.avoid_conditions || []).slice(0, 4).forEach((text) => rows.push({ title: '回避条件', badge: '风险', body: text }));
    (rec.backtest_jump_targets || []).slice(0, 4).forEach((item) => rows.push({
      title: item.label || '跳转',
      badge: item.target || '页面',
      body: JSON.stringify(item.params || {}),
    }));

    bulletEl.innerHTML = rows.length ? rows.map((item) => `
      <div class="research-conclusion-item">
        <div class="title">
          <span>${escSafe(item.title)}</span>
          <span class="status-badge">${escSafe(item.badge)}</span>
        </div>
        <div class="body">${escSafe(item.body)}</div>
      </div>
    `).join('') : '<div class="research-conclusion-empty">暂无可执行建议。</div>';
  }

  function renderExternalInfo(module) {
    const box = q('external-info-summary');
    if (!box) return;
    if (!module) {
      box.innerHTML = listItem('外生信息', '等待加载');
      return;
    }
    const payload = module.payload || {};
    const news = payload.news_summary || {};
    const community = payload.community || {};
    const history = payload.analytics_history_status || {};
    const collectors = Object.values(history).slice(0, 3).map((item) => `${item.collector}:${item.status}`).join(' | ');
    box.innerHTML = [
      listItem('新闻范围', news.scope || '-'),
      listItem('事件 / 原始新闻', `${Number(news.events_count || 0)} / ${Number(news.raw_count || 0)}`),
      listItem('公告 / 巨鲸', `${Number((community.announcements || []).length || 0)} / ${Number(community.whale_transfers?.count || 0)}`),
      listItem('历史采集', collectors || '暂无'),
    ].join('');
  }

  function renderDiscipline(module) {
    const summaryEl = q('discipline-summary');
    const gridEl = q('discipline-grid');
    if (!summaryEl || !gridEl) return;

    if (!module) {
      summaryEl.innerHTML = listItem('状态', '等待加载');
      gridEl.innerHTML = '<div class="list-item">暂无纪律与风控数据。</div>';
      return;
    }

    const behavior = module.payload?.behavior_report || {};
    const stoploss = module.payload?.stoploss_policy || {};
    const warnings = module.warnings || [];

    summaryEl.innerHTML = [
      listItem('行为记录', Number(behavior.entries || 0)),
      listItem('冲动占比', `${(Number(behavior.impulsive_ratio || 0) * 100).toFixed(2)}%`),
      listItem('过度交易预警', behavior.overtrading_warning ? '是' : '否'),
      listItem('止损建议数', Number((stoploss.position_suggestions || []).length || 0)),
    ].join('');

    const cards = [];
    cards.push(`
      <div class="strategy-card">
        <div class="list-item" style="padding:0 0 6px 0;border-bottom:none;">
          <h4>纪律状态</h4>
          <span class="status-badge ${behavior.overtrading_warning ? 'warning' : 'connected'}">${behavior.overtrading_warning ? '警告' : '正常'}</span>
        </div>
        <p>冲动占比 ${(Number(behavior.impulsive_ratio || 0) * 100).toFixed(2)}%</p>
        <p style="font-size:11px;color:#8fa6c0;">${escSafe(warnings.join('；') || '暂无明显纪律风险。')}</p>
      </div>
    `);

    (stoploss.position_suggestions || []).slice(0, 3).forEach((item) => {
      cards.push(`
        <div class="strategy-card">
          <div class="list-item" style="padding:0 0 6px 0;border-bottom:none;">
            <h4>${escSafe(item.symbol || '-')}</h4>
            <span class="status-badge">${escSafe(item.side || '-')}</span>
          </div>
          <p>R 值 ${Number(item.r_value || 0).toFixed(2)}</p>
          <p>ATR 止损 ${escSafe(item.atr_dynamic_stop ?? '--')}</p>
        </div>
      `);
    });

    gridEl.innerHTML = cards.join('') || '<div class="list-item">暂无纪律卡片。</div>';
  }

  function renderModule(name, module) {
    if (!module) return;
    state.modules[name] = module;
    state.moduleTimes[name] = new Date().toISOString();
    const payload = module.payload || {};

    try {
      if (name === 'market_state') {
        if (typeof window.renderWorkbenchMarketStatePanel === 'function') window.renderWorkbenchMarketStatePanel(module);
        else if (typeof window.renderAnalyticsOverviewPanel === 'function') window.renderAnalyticsOverviewPanel(payload.analytics_overview || {});
        if (typeof window.renderMarketSentimentPanel === 'function') window.renderMarketSentimentPanel(payload.sentiment_dashboard || {});
        // Auto-refresh regime calendar when market_state data is fresh
        loadRegimeCalendar().catch(() => {});
      } else if (name === 'factors') {
        if (typeof window.renderFactorLibraryPanel === 'function') window.renderFactorLibraryPanel(payload.factor_library || {});
        if (typeof window.renderFamaPanel === 'function') window.renderFamaPanel(payload.fama || {});
      } else if (name === 'cross_asset') {
        if (typeof window.renderMultiAssetPanel === 'function') window.renderMultiAssetPanel(payload.cross_asset || {});
      } else if (name === 'onchain') {
        if (typeof window.renderOnchainPanel === 'function') window.renderOnchainPanel(payload.onchain || {});
        renderExternalInfo(module);
        const tvlSeries = Array.isArray(payload.onchain?.defi_tvl?.series) ? payload.onchain.defi_tvl.series : [];
        const whaleRows = Array.isArray(payload.onchain?.whale_activity?.transactions) ? payload.onchain.whale_activity.transactions : [];
        if (!tvlSeries.length && !whaleRows.length && typeof window.loadOnchainOverviewPanel === 'function') {
          window.loadOnchainOverviewPanel({ refresh: true, quiet: true, showLoading: false, timeoutMs: 45000 }).catch((err) => {
            setDebug('research.workbench.onchain.full_refresh', String(err?.message || err));
          });
        }
      } else if (name === 'discipline') {
        renderDiscipline(module);
      }
    } catch (err) {
      setDebug(`research.workbench.render.${name}`, String(err?.message || err));
    }

    renderStatusCards();
  }

  function getModuleTimeoutMs(name) {
    if (name === 'factors') return 70000;
    if (name === 'onchain') return 22000;
    if (name === 'cross_asset') return 40000;
    return 25000;
  }

  async function loadContext(force = false) {
    const profile = getProfile();
    const symbols = Array.from(q('research-symbols')?.options || []).map((opt) => opt.value);
    const context = { profile, available_symbols: symbols };
    if (force || !state.context) state.context = context;
    state.profile = profile;
    renderStatusCards();
    setDebug('research.workbench.context', context);
    return context;
  }

  async function refreshRecommendations(quiet = false) {
    const rec = buildLocalRecommendations();
    state.recommendations = rec;
    renderRecommendations();
    renderOverview();
    renderStatusCards();
    setDebug('research.workbench.recommendations', rec);
    if (!quiet && typeof window.notify === 'function') window.notify('研究建议已更新');
    return rec;
  }

  async function runModule(name, quiet = false) {
    state.profile = getProfile();
    const module = await window.api(`/research/workbench/modules/${encodeURIComponent(name)}?${profileQuery(state.profile)}`, {
      timeoutMs: getModuleTimeoutMs(name),
    });
    renderModule(name, module);
    state.overview = buildLocalOverviewFromModules();
    renderOverview();
    await refreshRecommendations(true).catch(() => {});
    setDebug(`research.workbench.modules.${name}`, module);
    if (!quiet && typeof window.notify === 'function') window.notify(`${MODULE_LABELS[name] || name} 已更新`);
    return module;
  }

  async function runOverview() {
    state.profile = getProfile();
    let overview;
    try {
      overview = await window.api(`/research/workbench/overview?${profileQuery(state.profile)}`, { timeoutMs: 90000 });
      state.overview = overview;
      state.modules = {};
      renderOverview();
      renderStatusCards();
      Object.entries(overview.modules || {}).forEach(([name, module]) => {
        try {
          renderModule(name, module);
        } catch (err) {
          setDebug(`research.workbench.overview.render.${name}`, String(err?.message || err));
        }
      });
    } catch (err) {
      state.modules = {};
      for (const name of MODULE_NAMES) {
        try {
          // Run sequentially to keep fallback requests predictable under slow APIs.
          // eslint-disable-next-line no-await-in-loop
          await runModule(name, true);
        } catch (moduleErr) {
          setDebug(`research.workbench.overview.fallback.${name}`, String(moduleErr?.message || moduleErr));
        }
      }
      overview = buildLocalOverviewFromModules();
      state.overview = overview;
    }
    await refreshRecommendations(true);
    if (typeof window.renderResearchQuickSummary === 'function') {
      window.renderResearchQuickSummary([
        { label: '市场状态', value: overview.market_regime || '-' },
        { label: '方向偏向', value: overview.direction_bias || '-' },
        { label: '研究可信度', value: Number(overview.confidence || 0).toFixed(2) },
        { label: '模块覆盖', value: `${Number(overview.coverage?.ok_count || 0)}/${Number(overview.coverage?.total || 0)}` },
      ]);
    }
    renderStatusCards();
    setDebug('research.workbench.overview', overview);
    if (typeof window.notify === 'function') window.notify('研究总览已更新');
    return overview;
  }

  function estimateProfileDays(profile, multiplier = 2) {
    const tf = String(profile?.timeframe || '5m').toLowerCase();
    const minutes = tf === '1m' ? 1 : tf === '5m' ? 5 : tf === '15m' ? 15 : tf === '1h' ? 60 : tf === '4h' ? 240 : 1440;
    return Math.max(3, Math.min(365, Math.ceil(((Number(profile?.lookback || 1200) * minutes) / 1440) * multiplier)));
  }

  function inferMarketRegimeFromData(analytics, micro, news) {
    const riskLevel = analytics?.risk_level
      || analytics?.risk_dashboard?.risk_level
      || analytics?.modules?.risk_dashboard?.data?.risk_level
      || 'unknown';
    const spreadBps = Number(micro?.orderbook?.spread_bps || 0);
    const imbalance = Number(micro?.aggressor_flow?.imbalance || 0);
    const pos = Number(news?.sentiment?.positive || 0);
    const neg = Number(news?.sentiment?.negative || 0);
    const neu = Number(news?.sentiment?.neutral || 0);
    const newsN = pos + neg + neu;
    const newsBalance = newsN ? (pos - neg) / newsN : 0;
    const signal = imbalance * 0.65 + newsBalance * 0.35;
    const bias = signal > 0.15 ? 'bullish' : signal < -0.15 ? 'bearish' : 'neutral';
    let regime = '低信息震荡';
    if (newsN > 0) regime = '事件驱动观察';
    if (spreadBps > 0 && Math.abs(imbalance) > 0.2) regime = bias === 'bullish' ? '买盘驱动' : bias === 'bearish' ? '卖盘驱动' : regime;
    const confidence = Math.max(0.2, Math.min(0.95, (Math.abs(signal) * 0.6) + (newsN > 0 ? 0.2 : 0) + (spreadBps > 0 ? 0.15 : 0)));
    return { regime, bias, confidence, risk_level: riskLevel };
  }

  function buildCorrelationMatrix(rows, keys) {
    const matrix = {};
    if (!Array.isArray(rows) || !rows.length || !Array.isArray(keys) || !keys.length) return matrix;
    const mean = (arr) => arr.length ? arr.reduce((sum, v) => sum + v, 0) / arr.length : 0;
    const corr = (a, b) => {
      const n = Math.min(a.length, b.length);
      if (n < 2) return 0;
      const ax = a.slice(-n);
      const bx = b.slice(-n);
      const ma = mean(ax);
      const mb = mean(bx);
      let num = 0;
      let da = 0;
      let db = 0;
      for (let i = 0; i < n; i += 1) {
        const va = ax[i] - ma;
        const vb = bx[i] - mb;
        num += va * vb;
        da += va * va;
        db += vb * vb;
      }
      if (!da || !db) return 0;
      return Number((num / Math.sqrt(da * db)).toFixed(4));
    };
    keys.forEach((rowKey) => {
      matrix[rowKey] = {};
      const rowSeries = rows.map((row) => Number(row?.[rowKey])).filter((v) => Number.isFinite(v));
      keys.forEach((colKey) => {
        const colSeries = rows.map((row) => Number(row?.[colKey])).filter((v) => Number.isFinite(v));
        matrix[rowKey][colKey] = rowKey === colKey ? 1 : corr(rowSeries, colSeries);
      });
    });
    return matrix;
  }

  function buildFactorFallbackFromFama(fama, crossAsset, profile) {
    const latest = fama?.latest || {};
    const factors = Object.keys(latest || {});
    const series = Array.isArray(fama?.series) ? fama.series : [];
    const assets = Array.isArray(crossAsset?.assets) ? crossAsset.assets : [];
    return {
      exchange: profile.exchange,
      timeframe: profile.timeframe,
      symbols_used: profile.universe_symbols || [],
      points: Number(fama?.points || 0),
      factors,
      universe_size: Number(fama?.universe_size || assets.length || 0),
      universe_quality: fama?.universe_quality || 'fallback',
      warnings: ['因子库已回退到 Fama 快照与多币种收益近似。'],
      latest,
      mean_24: fama?.mean_24 || {},
      std_24: fama?.std_24 || {},
      correlation: buildCorrelationMatrix(series, factors),
      asset_scores: assets.slice(0, 12).map((item) => {
        const ret = Number(item?.return_pct || 0) / 100;
        const lowVol = Math.max(0, 1 - Math.min(Math.abs(Number(item?.volatility_pct || 0)) / 100, 1));
        return {
          symbol: item?.symbol || '-',
          score: Number(ret.toFixed(6)),
          momentum: Number(ret.toFixed(6)),
          value: 0,
          quality: Number(lowVol.toFixed(6)),
          low_vol: Number(lowVol.toFixed(6)),
          liquidity: 0,
          low_beta: 0,
          size: 0,
        };
      }),
    };
  }

  function hasFactorLibraryContent(data) {
    return Boolean(
      data
      && (
        (Array.isArray(data?.factors) && data.factors.length)
        || Object.keys(data?.latest || {}).length
        || (Array.isArray(data?.asset_scores) && data.asset_scores.length)
        || Number(data?.points || 0) > 0
      )
    );
  }

  function hasFamaContent(data) {
    return Boolean(
      data
      && (
        (Array.isArray(data?.series) && data.series.length)
        || Number(data?.points || 0) > 0
        || Object.values(data?.latest || {}).some((value) => Math.abs(Number(value || 0)) > 0)
      )
    );
  }

  function hasOnchainContent(data) {
    return Boolean(
      data
      && (
        (Array.isArray(data?.defi_tvl?.series) && data.defi_tvl.series.length)
        || (Array.isArray(data?.whale_activity?.transactions) && data.whale_activity.transactions.length)
        || Math.abs(Number(data?.defi_tvl?.latest_tvl || 0)) > 0
        || Number(data?.whale_activity?.count || 0) > 0
      )
    );
  }

  function isAsyncPendingPayload(data, kind = 'generic') {
    if (!data || typeof data !== 'object') return false;
    const mode = String(data?.served_mode || '').toLowerCase();
    const msg = [String(data?.error || ''), ...(Array.isArray(data?.warnings) ? data.warnings : [])].join(' ');
    const modePending = ['fallback', 'bootstrap', 'loading', 'background', 'cache_refresh'].includes(mode) || data?.refreshing === true;
    const textPending = /后台|预热|加载中|refresh|warming/i.test(msg);
    if (kind === 'factor_library') return !hasFactorLibraryContent(data) && (modePending || textPending);
    if (kind === 'fama') return !hasFamaContent(data) && (modePending || textPending);
    if (kind === 'onchain') return !hasOnchainContent(data) && (modePending || textPending);
    return modePending || textPending;
  }

  function clearModuleRetry(name) {
    const timer = state.retryTimers?.[name];
    if (timer) {
      clearTimeout(timer);
      delete state.retryTimers[name];
    }
  }

  function scheduleModuleRetry(name, loader, attempt = 1, delayMs = 3000) {
    if (typeof loader !== 'function' || attempt > 4) return;
    clearModuleRetry(name);
    state.retryTimers[name] = setTimeout(async () => {
      if (!isResearchActive()) return;
      try {
        const module = await loader();
        renderModule(name, module);
        state.overview = buildLocalOverviewFromModules();
        renderOverview();
        await refreshRecommendations(true).catch(() => {});
        setDebug(`research.workbench.retry.${name}.${attempt}`, module);
        const payload = module?.payload || {};
        const stillPending = name === 'factors'
          ? isAsyncPendingPayload(payload.factor_library, 'factor_library') || isAsyncPendingPayload(payload.fama, 'fama')
          : name === 'onchain'
            ? isAsyncPendingPayload(payload.onchain, 'onchain')
            : false;
        if (stillPending) scheduleModuleRetry(name, loader, attempt + 1, delayMs + 1200);
      } catch (err) {
        setDebug(`research.workbench.retry.${name}.${attempt}.error`, String(err?.message || err));
        scheduleModuleRetry(name, loader, attempt + 1, delayMs + 1200);
      }
    }, Math.max(1500, Number(delayMs || 3000)));
  }

  async function buildFactorsModule(profile, lookback, exchange, universe, timeframe, excludeRetired) {
    const [factorRes, famaRes, crossRes] = await Promise.allSettled([
      window.api(`/data/factors/library?exchange=${exchange}&symbols=${universe}&timeframe=${timeframe}&lookback=${Math.min(900, lookback)}&quantile=0.3&series_limit=240&exclude_retired=${excludeRetired}`, { timeoutMs: getModuleTimeoutMs('factors') }),
      window.api(`/data/factors/fama?exchange=${exchange}&symbols=${universe}&timeframe=${timeframe}&lookback=${Math.min(2400, lookback)}&exclude_retired=${excludeRetired}`, { timeoutMs: 45000 }),
      window.api(`/data/multi-assets/overview?exchange=${exchange}&symbols=${universe}&timeframe=${timeframe}&lookback=${Math.min(720, lookback)}&exclude_retired=${excludeRetired}`, { timeoutMs: 30000 }),
    ]);
    let factorLibrary = factorRes.status === 'fulfilled' ? factorRes.value : {};
    const fama = famaRes.status === 'fulfilled' ? famaRes.value : {};
    const crossAsset = crossRes.status === 'fulfilled' ? crossRes.value : {};
    if ((!Array.isArray(factorLibrary?.factors) || !factorLibrary.factors.length) && fama && !fama.error) {
      factorLibrary = buildFactorFallbackFromFama(fama, crossAsset, profile);
    }
    return {
      name: 'factors',
      status: factorLibrary?.error || fama?.error ? 'degraded' : 'ok',
      warnings: [...(factorLibrary?.warnings || []), ...(fama?.warnings || [])].slice(0, 6),
      summary: {
        headline: 'Factor & Style',
        top_symbols: (factorLibrary?.asset_scores || []).slice(0, 3).map((item) => item.symbol).filter(Boolean),
        universe_size: Number(factorLibrary?.universe_size || 0),
        factor_count: Number((factorLibrary?.factors || []).length),
        mkt: Number(fama?.latest?.MKT || 0),
        mom: Number(fama?.latest?.MOM || 0),
      },
      payload: { factor_library: factorLibrary || {}, fama: fama || {}, cross_asset: crossAsset || {} },
    };
  }

  async function buildOnchainModule(profile, exchange, primarySymbol) {
    const newsKey = String(profile.primary_symbol || 'BTC/USDT').split('/')[0];
    const [onchainRes, community, newsScoped, newsGlobal] = await Promise.all([
      window.api(`/data/onchain/overview?exchange=${exchange}&symbol=${primarySymbol}&whale_threshold_btc=10&chain=Ethereum&hours=72&refresh=true`, { timeoutMs: getModuleTimeoutMs('onchain') }).catch(() => ({})),
      window.api(`/trading/analytics/community/overview?exchange=${exchange}&symbol=${primarySymbol}`, { timeoutMs: 15000 }).catch(() => ({})),
      window.api(`/news/summary?symbol=${encodeURIComponent(newsKey)}&hours=72`, { timeoutMs: 15000 }).catch(() => ({})),
      window.api('/news/summary?hours=72', { timeoutMs: 15000 }).catch(() => ({})),
    ]);
    const newsTotal = Number(newsScoped?.events_count || 0) + Number(newsScoped?.feed_count || 0) + Number(newsScoped?.raw_count || 0);
    const news = newsTotal ? newsScoped : { ...newsGlobal, scope: 'global_fallback' };
    const warnings = [...(onchainRes?.warnings || [])];
    if (isAsyncPendingPayload(onchainRes, 'onchain')) warnings.unshift('链上面板正在后台补拉完整数据。');
    return {
      name: 'onchain',
      status: onchainRes?.error || isAsyncPendingPayload(onchainRes, 'onchain') ? 'degraded' : 'ok',
      warnings: warnings.slice(0, 6),
      summary: {
        headline: 'Onchain & Exogenous',
        whale_count: Number(onchainRes?.whale_activity?.count || community?.whale_transfers?.count || 0),
        news_events: Number(news?.events_count || 0),
        tvl_chain: onchainRes?.defi_tvl?.chain || 'Ethereum',
        served_mode: onchainRes?.served_mode || 'background',
      },
      payload: { onchain: onchainRes || {}, community: community || {}, news_summary: news || {}, analytics_history_status: {} },
    };
  }

  async function runWorkbenchModuleDirect(name, quiet = false) {
    const profile = getProfile();
    state.profile = profile;
    const exchange = encodeURIComponent(profile.exchange);
    const primarySymbol = encodeURIComponent(profile.primary_symbol);
    const universe = encodeURIComponent((profile.universe_symbols || []).join(','));
    const timeframe = encodeURIComponent(profile.timeframe);
    const lookback = Math.max(120, Math.min(2400, Number(profile.lookback || 1200)));
    const excludeRetired = profile.exclude_retired === false ? 'false' : 'true';
    let module;

    if (name === 'market_state') {
      try {
        const backendModule = await window.api(`/research/workbench/modules/${encodeURIComponent(name)}?${profileQuery(profile)}`, {
          timeoutMs: getModuleTimeoutMs(name),
        });
        renderModule(name, backendModule);
        state.overview = buildLocalOverviewFromModules();
        renderOverview();
        await refreshRecommendations(true).catch(() => {});
        setDebug(`research.workbench.modules.${name}.backend`, backendModule);
        if (!quiet && typeof window.notify === 'function') window.notify(`${MODULE_LABELS[name] || name} 已更新`);
        return backendModule;
      } catch (backendErr) {
        setDebug(`research.workbench.modules.${name}.backend_fallback`, String(backendErr?.message || backendErr));
      }
      const days = estimateProfileDays(profile, 2);
      const calendarDays = Math.max(7, Math.min(90, estimateProfileDays(profile, 1)));
      const newsKey = String(profile.primary_symbol || 'BTC/USDT').split('/')[0];
      const [analyticsRes, microRes, communityRes, newsScopedRes, newsGlobalRes] = await Promise.allSettled([
        window.api(`/trading/analytics/overview?days=${days}&lookback=${Math.max(120, Math.min(2000, lookback))}&calendar_days=${calendarDays}&exchange=${exchange}&symbol=${primarySymbol}`, { timeoutMs: 12000 }),
        window.api(`/trading/analytics/microstructure?exchange=${exchange}&symbol=${primarySymbol}&depth_limit=20`, { timeoutMs: 15000 }),
        window.api(`/trading/analytics/community/overview?exchange=${exchange}&symbol=${primarySymbol}`, { timeoutMs: 15000 }),
        window.api(`/news/summary?symbol=${encodeURIComponent(newsKey)}&hours=24`, { timeoutMs: 15000 }),
        window.api('/news/summary?hours=24', { timeoutMs: 15000 }),
      ]);
      const analytics = analyticsRes.status === 'fulfilled' ? analyticsRes.value : {};
      const analyticsModules = analytics?.modules || {};
      const micro = microRes.status === 'fulfilled' ? microRes.value : (analyticsModules.microstructure?.data || {});
      const community = communityRes.status === 'fulfilled' ? communityRes.value : (analyticsModules.community?.data || {});
      let news = newsScopedRes.status === 'fulfilled' ? newsScopedRes.value : {};
      const newsTotal = Number(news?.events_count || 0) + Number(news?.feed_count || 0) + Number(news?.raw_count || 0);
      if (!newsTotal && newsGlobalRes.status === 'fulfilled') news = { ...newsGlobalRes.value, scope: 'global_fallback' };
      const calendarWatchlist = Array.isArray(analyticsModules.calendar?.data?.events) ? analyticsModules.calendar.data.events.slice(0, 8) : [];
      const regime = inferMarketRegimeFromData(analytics, micro, news);
      const warnings = [];
      if (String(news?.scope || '') === 'global_fallback') warnings.push('当前标的新闻不足，已回退到全市场摘要。');
      if (micro?.source_error || !Number(micro?.orderbook?.spread_bps || 0)) warnings.push('微观结构深度不足，盘口与主动流解释力受限。');
      module = {
        name,
        status: warnings.length ? 'degraded' : 'ok',
        warnings,
        summary: {
          headline: `${regime.regime} | ${profile.primary_symbol} | ${profile.timeframe}`,
          market_regime: regime.regime,
          direction_bias: regime.bias,
          confidence: regime.confidence,
          risk_level: regime.risk_level,
        },
        payload: {
          analytics_overview: analytics,
          sentiment_dashboard: {
            exchange: profile.exchange,
            symbol: profile.primary_symbol,
            timestamp: new Date().toISOString(),
            microstructure: micro,
            community,
            news,
          },
          calendar_watchlist: calendarWatchlist,
          regime,
        },
      };
    } else if (name === 'factors') {
      module = await buildFactorsModule(profile, lookback, exchange, universe, timeframe, excludeRetired);
      if (isAsyncPendingPayload(module?.payload?.factor_library, 'factor_library') || isAsyncPendingPayload(module?.payload?.fama, 'fama')) {
        scheduleModuleRetry('factors', () => buildFactorsModule(profile, lookback, exchange, universe, timeframe, excludeRetired), 1, 3200);
      } else {
        clearModuleRetry('factors');
      }
    } else if (name === 'cross_asset') {
      const data = await window.api(`/data/multi-assets/overview?exchange=${exchange}&symbols=${universe}&timeframe=${timeframe}&lookback=${Math.min(2000, lookback)}&exclude_retired=${excludeRetired}`, { timeoutMs: getModuleTimeoutMs(name) });
      const leader = Array.isArray(data?.assets) && data.assets.length ? data.assets[0] : {};
      module = {
        name,
        status: Number(data?.count || 0) >= 3 ? 'ok' : 'degraded',
        warnings: Number(data?.count || 0) >= 3 ? [] : ['可用币种不足 3 个，轮动判断可能失真。'],
        summary: {
          headline: '多币种强弱与相关性',
          asset_count: Number(data?.count || 0),
          leader_symbol: leader?.symbol || '-',
          leader_return_pct: Number(leader?.return_pct || 0),
        },
        payload: { cross_asset: data || {} },
      };
    } else if (name === 'onchain') {
      module = await buildOnchainModule(profile, exchange, primarySymbol);
      if (isAsyncPendingPayload(module?.payload?.onchain, 'onchain')) {
        scheduleModuleRetry('onchain', () => buildOnchainModule(profile, exchange, primarySymbol), 1, 3000);
      } else {
        clearModuleRetry('onchain');
      }
    } else if (name === 'discipline') {
      const [behavior, stoploss] = await Promise.all([
        window.api('/trading/analytics/behavior/report?days=7', { timeoutMs: 12000 }).catch(() => ({})),
        window.api('/trading/analytics/stoploss/policy', { timeoutMs: 12000 }).catch(() => ({})),
      ]);
      module = {
        name,
        status: Number(behavior?.entries || 0) > 0 ? 'ok' : 'degraded',
        warnings: Number(behavior?.entries || 0) > 0 ? [] : ['近期没有行为记录，纪律模块仅展示通用建议。'],
        summary: {
          headline: '纪律与风控',
          entries: Number(behavior?.entries || 0),
          impulsive_ratio: Number(behavior?.impulsive_ratio || 0),
          overtrading_warning: Boolean(behavior?.overtrading_warning),
          position_suggestions: Number((stoploss?.position_suggestions || []).length || 0),
        },
        payload: { behavior_report: behavior || {}, stoploss_policy: stoploss || {} },
      };
    } else {
      throw new Error(`unknown module: ${name}`);
    }

    renderModule(name, module);
    state.overview = buildLocalOverviewFromModules();
    renderOverview();
    await refreshRecommendations(true).catch(() => {});
    setDebug(`research.workbench.modules.${name}`, module);
    if (!quiet && typeof window.notify === 'function') window.notify(`${MODULE_LABELS[name] || name} 已更新`);
    return module;
  }

  async function runWorkbenchOverviewDirect() {
    state.profile = getProfile();
    if (typeof window.loadResearchOverview === 'function') await window.loadResearchOverview();
    state.modules = {};
    for (const name of MODULE_NAMES) {
      try {
        // Run sequentially to keep the page responsive under slow upstream APIs.
        // eslint-disable-next-line no-await-in-loop
        await runWorkbenchModuleDirect(name, true);
      } catch (err) {
        setDebug(`research.workbench.overview.direct.${name}`, String(err?.message || err));
      }
    }
    state.overview = buildLocalOverviewFromModules();
    await refreshRecommendations(true);
    setDebug('research.workbench.overview', state.overview);
    if (typeof window.notify === 'function') window.notify('研究总览已更新');
    return state.overview;
  }

  function bindAsyncButton(id, handler) {
    const el = q(id);
    if (!el) return;
    el.onclick = async () => {
      const origHtml = el.innerHTML;
      try {
        el.disabled = true;
        el.classList.add('btn-loading');
        el.innerHTML = '<span class="btn-spinner"></span> 加载中';
        await handler();
      } catch (err) {
        if (typeof window.notify === 'function') window.notify(`高级研究失败: ${err.message || err}`, true);
        setDebug(`research.workbench.error.${id}`, String(err?.message || err));
      } finally {
        el.disabled = false;
        el.classList.remove('btn-loading');
        el.innerHTML = origHtml;
      }
    };
  }

  function bindLegacyButtons() {
    bindAsyncButton('btn-behavior-log', async () => {
      if (typeof window.logBehaviorJournal === 'function') await window.logBehaviorJournal();
    });
    bindAsyncButton('btn-load-behavior-report', async () => {
      if (typeof window.loadAnalyticsPanel === 'function') {
        const days = Math.max(3, Math.min(30, Math.ceil((getProfile().lookback * 60) / 1440)));
        await window.loadAnalyticsPanel(`/trading/analytics/behavior/report?days=${days}`);
      }
    });
    bindAsyncButton('btn-load-stoploss-policy', async () => {
      if (typeof window.loadAnalyticsPanel === 'function') await window.loadAnalyticsPanel('/trading/analytics/stoploss/policy');
    });
    bindAsyncButton('btn-research-preset-hf', async () => {
      if (typeof window.applyResearchPreset === 'function') window.applyResearchPreset('hf30');
      state.profile = getProfile();
      renderStatusCards();
    });
    bindAsyncButton('btn-research-preset-intraday', async () => {
      if (typeof window.applyResearchPreset === 'function') window.applyResearchPreset('intraday');
      state.profile = getProfile();
      renderStatusCards();
    });
    bindAsyncButton('btn-research-preset-swing', async () => {
      if (typeof window.applyResearchPreset === 'function') window.applyResearchPreset('swing');
      state.profile = getProfile();
      renderStatusCards();
    });
    bindAsyncButton('btn-factor-export-json', async () => {
      if (typeof window.exportFactorLibrary === 'function') window.exportFactorLibrary('json');
    });
    bindAsyncButton('btn-factor-export-csv', async () => {
      if (typeof window.exportFactorLibrary === 'function') window.exportFactorLibrary('csv');
    });
    bindAsyncButton('btn-factor-export-report', async () => {
      if (typeof window.exportFactorLibrary === 'function') window.exportFactorLibrary('report');
    });
  }

  /* ── Regime Calendar ─────────────────────────────────────────── */
  const BIAS_CLASS = {
    bullish: 'regime-bias-bullish',
    bearish: 'regime-bias-bearish',
    defensive: 'regime-bias-defensive',
    neutral: 'regime-bias-neutral',
  };

  async function loadRegimeCalendar() {
    const grid = q('regime-calendar-grid');
    if (!grid) return;
    const profile = state.profile || getProfile();
    const days = Number(q('regime-calendar-days')?.value || 7);
    const exchange = profile.exchange || 'binance';
    const symbol = profile.primary_symbol || 'BTC/USDT';
    grid.innerHTML = '<div style="color:#6b7fa0;font-size:12px;">加载中...</div>';
    try {
      const data = await apiResearch(
        `/workbench/regime-calendar?exchange=${encodeURIComponent(exchange)}&symbol=${encodeURIComponent(symbol)}&days=${days}`
      );
      const calendar = Array.isArray(data?.calendar) ? data.calendar : [];
      if (!calendar.length) {
        grid.innerHTML = '<div style="color:#6b7fa0;font-size:12px;">暂无历史快照数据（需先运行市场状态分析采集数据）</div>';
        return;
      }
      grid.innerHTML = calendar.map((item) => {
        const biasClass = BIAS_CLASS[item.bias] || 'regime-bias-neutral';
        const funding = item.avg_funding != null ? item.avg_funding.toFixed(5) : '--';
        const basis = item.avg_basis != null ? `${item.avg_basis.toFixed(3)}%` : '--';
        return `<div class="regime-day-cell ${biasClass}" title="快照 ${item.snapshot_count} 条 | Funding ${funding} | Basis ${basis}">
          <div class="regime-day-date">${escSafe(item.date.slice(5))}</div>
          <div class="regime-day-label">${escSafe(item.regime)}</div>
          <div class="regime-day-meta">F:${funding} B:${basis}</div>
        </div>`;
      }).join('');
    } catch (err) {
      grid.innerHTML = `<div style="color:#e05260;font-size:12px;">加载失败: ${escSafe(String(err?.message || err))}</div>`;
    }
  }

  function bindWorkbenchButtons() {
    bindAsyncButton('btn-workbench-overview', runWorkbenchOverviewDirect);
    bindAsyncButton('btn-workbench-recommendations', () => refreshRecommendations(false));
    bindAsyncButton('btn-workbench-market-state', () => runWorkbenchModuleDirect('market_state', false));
    bindAsyncButton('btn-workbench-factors', () => runWorkbenchModuleDirect('factors', false));
    bindAsyncButton('btn-workbench-factors-secondary', () => runWorkbenchModuleDirect('factors', false));
    bindAsyncButton('btn-workbench-fama-secondary', () => runWorkbenchModuleDirect('factors', false));
    bindAsyncButton('btn-workbench-cross-asset', () => runWorkbenchModuleDirect('cross_asset', false));
    bindAsyncButton('btn-workbench-cross-asset-secondary', () => runWorkbenchModuleDirect('cross_asset', false));
    bindAsyncButton('btn-workbench-onchain', () => runWorkbenchModuleDirect('onchain', false));
    bindAsyncButton('btn-workbench-onchain-secondary', () => runWorkbenchModuleDirect('onchain', false));
    bindAsyncButton('btn-workbench-discipline', () => runWorkbenchModuleDirect('discipline', false));
    bindAsyncButton('btn-load-regime-calendar', loadRegimeCalendar);
    const calDaysEl = q('regime-calendar-days');
    if (calDaysEl) calDaysEl.addEventListener('change', () => loadRegimeCalendar().catch(() => {}));
  }

  function bindConfigWatchers() {
    ['research-timeframe', 'research-lookback', 'research-symbol', 'research-symbols', 'research-exclude-retired'].forEach((id) => {
      const el = q(id);
      if (!el) return;
      el.addEventListener(el.tagName === 'INPUT' ? 'input' : 'change', () => {
        state.profile = getProfile();
        renderStatusCards();
      });
    });

    const exchangeEl = q('research-exchange');
    if (exchangeEl) {
      exchangeEl.addEventListener('change', () => {
        state.profile = getProfile();
        loadContext(true).catch((err) => setDebug('research.workbench.context.error', String(err?.message || err)));
      });
    }
  }

  function patchGlobals() {
    window.workbenchState = state;
    window.renderResearchStatusCards = renderStatusCards;
    window.renderResearchConclusionCard = renderRecommendations;
  }

  async function lazyInit() {
    if (state.initialized || !q('research')) return;
    state.initialized = true;
    patchGlobals();
    bindWorkbenchButtons();
    bindLegacyButtons();
    bindConfigWatchers();
    applyWorkbenchDefaults();
    state.profile = getProfile();
    renderStatusCards();
    renderOverview();
    renderRecommendations();
    renderExternalInfo(null);
    renderDiscipline(null);
    await loadContext(true).catch((err) => {
      state.profile = getProfile();
      renderStatusCards();
      setDebug('research.workbench.context.error', String(err?.message || err));
    });
  }

  function watchResearchTab() {
    const trigger = () => {
      if (isResearchActive()) lazyInit().catch((err) => setDebug('research.workbench.init.error', String(err?.message || err)));
    };

    document.querySelectorAll('.tab-btn[data-tab="research"]').forEach((btn) => {
      btn.addEventListener('click', () => setTimeout(trigger, 0));
    });
    window.addEventListener('hashchange', trigger);
    document.addEventListener('visibilitychange', () => {
      if (!document.hidden) trigger();
    });
    setTimeout(trigger, 0);
  }

  function init() {
    if (!q('research')) return;
    watchResearchTab();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
