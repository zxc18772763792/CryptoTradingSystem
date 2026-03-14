(function () {
  'use strict';

  /* ── 轮询间隔 ── */
  const SIGNAL_INTERVAL_MS  = 30000;
  const REFRESH_INTERVAL_MS = 60000;
  const JOB_POLL_MS         = 3000;

  /* ── 策略类别 & 颜色 ── */
  const STRATEGY_CATEGORIES = {
    // 趋势
    MAStrategy:'趋势', EMAStrategy:'趋势', MACDStrategy:'趋势', MACDHistogramStrategy:'趋势',
    ADXTrendStrategy:'趋势', TrendFollowingStrategy:'趋势', AroonStrategy:'趋势',
    // 震荡
    RSIStrategy:'震荡', RSIDivergenceStrategy:'震荡', StochasticStrategy:'震荡',
    BollingerBandsStrategy:'震荡', WilliamsRStrategy:'震荡', CCIStrategy:'震荡', StochRSIStrategy:'震荡',
    // 动量
    MomentumStrategy:'动量', ROCStrategy:'动量', PriceAccelerationStrategy:'动量',
    // 均值回归
    MeanReversionStrategy:'均值回归', BollingerMeanReversionStrategy:'均值回归',
    VWAPReversionStrategy:'均值回归', VWAPStrategy:'均值回归', MeanReversionHalfLifeStrategy:'均值回归',
    // 突破
    BollingerSqueezeStrategy:'突破', DonchianBreakoutStrategy:'突破',
    // 成交量
    MFIStrategy:'成交量', OBVStrategy:'成交量', TradeIntensityStrategy:'成交量',
    // 风险
    ParkinsonVolStrategy:'风险', UlcerIndexStrategy:'风险', VaRBreakoutStrategy:'风险',
    MaxDrawdownStrategy:'风险', SortinoRatioStrategy:'风险',
    // 套利
    PairsTradingStrategy:'套利', HurstExponentStrategy:'套利',
    // 量化
    OrderFlowImbalanceStrategy:'量化', MultiFactorHFStrategy:'量化',
    // ML
    MLXGBoostStrategy:'ML',
    // 宏观
    MarketSentimentStrategy:'宏观', SocialSentimentStrategy:'宏观', FundFlowStrategy:'宏观', WhaleActivityStrategy:'宏观',
  };
  const CATEGORY_COLORS = {
    '趋势':'#3b82f6', '震荡':'#8b5cf6', '动量':'#20bf78', '均值回归':'#06b6d4',
    '突破':'#f59e0b', '成交量':'#84cc16', '风险':'#f43f5e', '套利':'#e05260',
    '量化':'#a78bfa', 'ML':'#ff6b35', '宏观':'#64748b',
  };

  /* ── 状态 ── */
  const STRATEGY_FAMILIES = {
    MLXGBoostStrategy: 'ml',
    MarketSentimentStrategy: 'ai_glm',
    SocialSentimentStrategy: 'ai_glm',
    FundFlowStrategy: 'ai_glm',
    WhaleActivityStrategy: 'ai_glm',
  };
  const FAMILY_META = {
    traditional: { label: '传统规则', color: '#64748b', accent: 'rgba(100,116,139,.16)' },
    ml: { label: 'ML驱动', color: '#ff6b35', accent: 'rgba(255,107,53,.16)' },
    ai_glm: { label: 'GLM/AI驱动', color: '#38bdf8', accent: 'rgba(56,189,248,.16)' },
  };

  const state = {
    proposals: [],
    candidates: [],
    pendingApprovals: [],   // candidates with human gate
    pendingLlmContext: null, // last AI-generated research context
    pendingMacroContext: null,
    runtimeConfig: null,    // { governance_enabled, decision_mode, trading_mode, ai_live_decision }
    runtimeConfigLoaded: false,
    selectedProposalId: '',
    selectedCandidateId: '',
    latestSignals: {},
    signalTimer: null,
    refreshTimer: null,
    liveSignalTimer: null,
    signalLoading: false,
    signalPanelCollapsed: false,
    jobPollingTimers: {},   // proposalId → intervalId
    sortBy: 'score',        // 'score' | 'sharpe' | 'return' | 'drawdown'
    filterCategory: '',     // '' | '趋势' | '震荡' | ...
    compareCandidateIds: new Set(),
  };

  /* ── 工具函数 ── */
  function esc(v) {
    return String(v ?? '').replace(/[&<>"']/g, m =>
      ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[m]));
  }

  function repairUtf8Mojibake(text) {
    const value = String(text ?? '');
    if (!/[ÃÂÅÆÇÐÑÕÖØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿ]/.test(value)) {
      return value;
    }
    try {
      return decodeURIComponent(escape(value));
    } catch (_) {
      return value;
    }
  }

  function normalizeUiText(text) {
    let value = repairUtf8Mojibake(text);
    const replacements = [
      ['Best Params', '最优参数'],
      ['CSV:', 'CSV 文件：'],
      ['Markdown:', 'Markdown 报告：'],
      ['DSR Score', 'DSR 分数'],
      ['WF Consistency', 'WF 一致性'],
      ['folds+', '折以上'],
      ['OHLCV only', '仅 OHLCV'],
      ['OHLCV + News + Macro', 'OHLCV + 新闻 + 宏观'],
      ['OHLCV + News', 'OHLCV + 新闻'],
      ['OHLCV + Macro', 'OHLCV + 宏观'],
      ['Research Enrichment', '研究增强'],
      ['Decision Engine', '决策引擎'],
      ['News Events', '新闻事件'],
      ['Macro Layer', '宏观层'],
      ['Funding On', '已启用'],
      ['Funding Off', '未启用'],
      ['Replay Mode', '回放模式'],
      ['Research Artifacts', '研究产物'],
      ['Experiment ID', '实验 ID'],
      ['Status:', '状态：'],
      ['Candidate Lifecycle', '候选生命周期'],
      ['Proposal Lifecycle', '方案生命周期'],
      ['No lifecycle records', '暂无生命周期记录'],
      ['No experiment runs', '暂无实验运行记录'],
      ['No equity curve sample.', '暂无资金曲线样本。'],
      ['run:', '运行 ID：'],
      ['Research:', '回放模式：'],
      ['Macro On', '宏观开启'],
      ['Macro Off', '宏观关闭'],
      ['News ', '新闻 '],
    ];
    replacements.forEach(([from, to]) => {
      value = value.split(from).join(to);
    });
    return value;
  }

  function normalizeDomText(root) {
    if (!root) return;
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
    const textNodes = [];
    while (walker.nextNode()) textNodes.push(walker.currentNode);
    textNodes.forEach(node => {
      const next = normalizeUiText(node.nodeValue || '');
      if (next !== node.nodeValue) node.nodeValue = next;
    });
    if (root.querySelectorAll) {
      root.querySelectorAll('[title],[placeholder]').forEach(el => {
        if (el.hasAttribute('title')) {
          el.setAttribute('title', normalizeUiText(el.getAttribute('title') || ''));
        }
        if (el.hasAttribute('placeholder')) {
          el.setAttribute('placeholder', normalizeUiText(el.getAttribute('placeholder') || ''));
        }
      });
    }
  }

  function notify(msg, isError = false) {
    if (typeof window.notify === 'function') { window.notify(msg, !!isError); return; }
    const box = document.getElementById('notification');
    if (!box) return;
    box.textContent = normalizeUiText(msg || '');
    box.className = `notification show${isError ? ' error' : ''}`;
    setTimeout(() => box.classList.remove('show'), 3000);
  }

  function csvInput(id) {
    return String(document.getElementById(id)?.value || '').split(',').map(s => s.trim()).filter(Boolean);
  }

  function parseTs(v) {
    if (!v) return null;
    if (v instanceof Date) {
      return Number.isFinite(v.getTime()) ? v : null;
    }
    let raw = String(v).trim();
    if (!raw) return null;
    raw = raw.replace(' ', 'T');
    if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?$/.test(raw)) {
      raw = `${raw}Z`;
    }
    const d = new Date(raw);
    return Number.isFinite(d.getTime()) ? d : null;
  }

  function fmtTs(v) {
    const d = parseTs(v);
    if (!d) return '--';
    return d.toLocaleString('zh-CN', {
      hour12: false,
      timeZone: 'Asia/Shanghai',
    });
  }

  function fmtNum(v, digits = 2) {
    const n = Number(v);
    return Number.isFinite(n) ? n.toFixed(digits) : '--';
  }

  function toArray(v) { return Array.isArray(v) ? v : []; }
  function joinText(v) { return toArray(v).map(x => String(x || '').trim()).filter(Boolean).join('、') || '--'; }
  function governanceEnabled() {
    return !!(state.runtimeConfig && state.runtimeConfig.governance_enabled);
  }

  function getLiveDecisionRuntimeConfig() {
    return (state.runtimeConfig && state.runtimeConfig.ai_live_decision) || null;
  }

  function renderLiveDecisionRuntimeConfig() {
    const cfg = getLiveDecisionRuntimeConfig();
    const enabledEl = document.getElementById('ai-live-decision-enabled');
    const modeEl = document.getElementById('ai-live-decision-mode');
    const providerEl = document.getElementById('ai-live-decision-provider');
    const modelEl = document.getElementById('ai-live-decision-model');
    const statusEl = document.getElementById('ai-live-decision-status');
    if (!enabledEl || !modeEl || !providerEl || !modelEl || !statusEl) return;
    if (!cfg) {
      statusEl.textContent = '未加载';
      return;
    }
    enabledEl.checked = !!cfg.enabled;
    modeEl.value = String(cfg.mode || 'shadow');
    providerEl.value = String(cfg.provider || 'glm');
    modelEl.value = String(cfg.model || '');
    const selectedProvider = String(cfg.provider || 'glm');
    const providerMeta = (cfg.providers || {})[selectedProvider] || {};
    const available = !!providerMeta.available;
    const modeText = String(cfg.mode || 'shadow');
    const providerText = `${selectedProvider}/${String(cfg.model || '')}`;
    statusEl.textContent = `${cfg.enabled ? '已启用' : '未启用'} | ${modeText} | ${providerText} | ${available ? 'key就绪' : 'key缺失'}`;
    statusEl.style.color = available ? '#9fb1c9' : '#f0b429';
  }

  async function saveLiveDecisionRuntimeConfig() {
    const enabledEl = document.getElementById('ai-live-decision-enabled');
    const modeEl = document.getElementById('ai-live-decision-mode');
    const providerEl = document.getElementById('ai-live-decision-provider');
    const modelEl = document.getElementById('ai-live-decision-model');
    const btn = document.getElementById('ai-live-decision-save-btn');
    if (!enabledEl || !modeEl || !providerEl || !modelEl) return;
    const payload = {
      enabled: !!enabledEl.checked,
      mode: String(modeEl.value || 'shadow'),
      provider: String(providerEl.value || 'glm'),
      model: String(modelEl.value || '').trim(),
    };
    try {
      if (btn) { btn.disabled = true; btn.textContent = '保存中...'; }
      const res = await aiApi('/runtime-config/live-decision', {
        method: 'POST',
        body: JSON.stringify(payload),
        timeoutMs: 15000,
      });
      const nextCfg = res?.config || null;
      state.runtimeConfig = {
        ...(state.runtimeConfig || {}),
        ai_live_decision: nextCfg,
      };
      renderLiveDecisionRuntimeConfig();
      notify('AI实盘决策配置已更新');
    } catch (err) {
      notify(`AI实盘决策配置保存失败: ${err.message}`, true);
    } finally {
      if (btn) { btn.disabled = false; btn.textContent = '保存AI决策配置'; }
    }
  }

  function canRegisterCandidate(cand) {
    if (!cand) return false;
    if (governanceEnabled()) return false;
    const status = String(cand?.status || '').trim();
    return !new Set(['retired', 'paper_running', 'shadow_running']).has(status);
  }
  function isRunnableProposalStatus(status) {
    return new Set(['draft', 'rejected', 'validated']).has(String(status || '').trim());
  }

  function normalizeTopRow(row, fallback = {}) {
    const merged = { ...fallback, ...(row || {}) };
    return {
      strategy: String(merged.strategy || ''),
      timeframe: String(merged.timeframe || ''),
      total_return: merged.total_return != null ? Number(merged.total_return) : null,
      sharpe_ratio: merged.sharpe_ratio != null ? Number(merged.sharpe_ratio) : null,
      max_drawdown: merged.max_drawdown != null ? Number(merged.max_drawdown) : null,
      win_rate: merged.win_rate != null ? Number(merged.win_rate) : null,
      score: merged.score != null ? Number(merged.score) : null,
    };
  }

  function candidateTopResults(cand) {
    const rows = toArray(cand?.metadata?.top_results)
      .map(r => normalizeTopRow(r))
      .filter(r => r.strategy || r.timeframe || r.total_return != null || r.sharpe_ratio != null || r.max_drawdown != null);
    if (rows.length) return rows;
    const bestMeta = cand?.metadata?.best || {};
    const bestVs = cand?.validation_summary?.metrics?.best || {};
    const fallbackRaw = Object.keys(bestMeta || {}).length ? bestMeta : bestVs;
    if (fallbackRaw && Object.keys(fallbackRaw).length) {
      return [normalizeTopRow(fallbackRaw, { strategy: cand?.strategy, timeframe: cand?.timeframe })];
    }
    return [];
  }

  /* 晋级建议文本（人性化） */
  function promotionText(d) {
    return { paper:'先以纸盘模拟（低风险试跑）',
             shadow:'影子模式追踪，观察真实行情',
             live_candidate:'条件成熟，可申请实盘候选',
             reject:'暂不建议注册，需进一步优化', }[String(d || '')] || (d ? String(d) : '待定');
  }

  /* 市场状态中文 */
  function regimeText(r) {
    return { mixed:'混合行情', trend_up:'上涨趋势', trend_down:'下跌趋势',
             mean_reversion:'震荡回归', breakout:'突破行情',
             stat_arb:'统计套利', news_event:'新闻事件' }[String(r || '')] || String(r || '--');
  }

  /* 分数对应颜色等级 */
  function scoreColor(score) {
    const n = Number(score || 0);
    return n >= 70 ? 'green' : n >= 50 ? 'yellow' : 'red';
  }

  function scoreEmoji(score) {
    return Number(score || 0) >= 70 ? '🟢' : Number(score || 0) >= 50 ? '🟡' : '🔴';
  }

  /* ── API 请求 ── */
  function getStrategyFamily(strategy) {
    return STRATEGY_FAMILIES[String(strategy || '').trim()] || 'traditional';
  }

  function getCurrentResearchExchange() {
    return String(document.getElementById('research-exchange')?.value || document.getElementById('data-exchange')?.value || 'binance').trim() || 'binance';
  }

  function getCurrentResearchSymbol() {
    return String(document.getElementById('research-symbol')?.value || document.getElementById('data-symbol')?.value || 'BTC/USDT').trim() || 'BTC/USDT';
  }

  function symbolToNewsKey(sym) {
    const raw = String(sym || '').trim().toUpperCase();
    if (!raw) return '';
    const main = raw.split(':')[0];
    if (main.includes('/')) return main.split('/')[0];
    return main.replace(/(USDT|USDC|FDUSD|BUSD|USD)$/,'') || main;
  }

  async function loadPlannerMacroContext() {
    const exchange = getCurrentResearchExchange();
    const symbol = getCurrentResearchSymbol();
    const newsSym = symbolToNewsKey(symbol);
    const [micro, community, newsScoped, newsGlobal] = await Promise.allSettled([
      rootApi(`/trading/analytics/microstructure?exchange=${encodeURIComponent(exchange)}&symbol=${encodeURIComponent(symbol)}&depth_limit=20`, { timeoutMs: 8000 }),
      rootApi(`/trading/analytics/community/overview?exchange=${encodeURIComponent(exchange)}&symbol=${encodeURIComponent(symbol)}`, { timeoutMs: 12000 }),
      rootApi(`/news/summary?symbol=${encodeURIComponent(newsSym)}&hours=24`, { timeoutMs: 15000 }),
      rootApi(`/news/summary?hours=24`, { timeoutMs: 15000 }),
    ]);
    let newsPayload = newsScoped.status === 'fulfilled'
      ? newsScoped.value
      : { sentiment: { positive: 0, neutral: 0, negative: 0 }, events_count: 0, feed_count: 0, raw_count: 0 };
    const scopedTotal = Number(newsPayload?.events_count || 0) + Number(newsPayload?.feed_count || 0) + Number(newsPayload?.raw_count || 0);
    if ((!scopedTotal || newsScoped.status !== 'fulfilled') && newsGlobal.status === 'fulfilled') {
      newsPayload = { ...newsGlobal.value, scope: 'global_fallback' };
    }
    const microData = micro.status === 'fulfilled' ? micro.value : {};
    const communityData = community.status === 'fulfilled' ? community.value : {};
    const fundingRate = Number(microData?.funding_rate?.funding_rate ?? NaN);
    const basisPct = Number(microData?.spot_futures_basis?.basis_pct ?? NaN);
    const imbalance = Number(microData?.aggressor_flow?.imbalance ?? NaN);
    const whaleCount = Number(communityData?.whale_transfers?.count || 0);
    const positive = Number(newsPayload?.sentiment?.positive || 0);
    const negative = Number(newsPayload?.sentiment?.negative || 0);
    const neutral = Number(newsPayload?.sentiment?.neutral || 0);
    const sentimentDen = positive + negative + neutral;
    const sentimentScore = sentimentDen ? ((positive - negative) / sentimentDen) : 0;
    const macroContext = {
      sentiment: sentimentScore > 0.12 ? 'BULLISH' : (sentimentScore < -0.12 ? 'BEARISH' : 'NEUTRAL'),
      volatility: Math.abs(Number(basisPct || 0)) > 0.3 ? 'high' : 'normal',
      factors: {
        momentum: Number.isFinite(imbalance) ? Math.max(0, imbalance) : 0,
        mean_reversion: Number.isFinite(imbalance) ? Math.max(0, -imbalance) : 0,
        trend_strength: Number.isFinite(basisPct) ? Math.min(1, Math.abs(basisPct) / 0.5) : 0,
      },
      microstructure: {
        order_flow_imbalance: Number.isFinite(imbalance) ? imbalance : 0,
        funding_rate: Number.isFinite(fundingRate) ? fundingRate : null,
        basis_pct: Number.isFinite(basisPct) ? basisPct : null,
      },
      community: {
        whale_count: whaleCount,
      },
      news: {
        events_count: Number(newsPayload?.events_count || 0),
        feed_count: Number(newsPayload?.feed_count || 0),
        raw_count: Number(newsPayload?.raw_count || 0),
        sentiment_score: sentimentScore,
        scope: String(newsPayload?.scope || 'scoped'),
      },
      exchange,
      symbol,
    };
    state.pendingMacroContext = macroContext;
    return macroContext;
  }

  function buildCandidateDedupKey(cand) {
    const strategy = String(cand?.strategy || '').trim();
    const symbol = String(cand?.symbol || '').trim();
    const timeframe = String(cand?.timeframe || '').trim();
    return `${strategy}::${symbol}::${timeframe}`;
  }

  function pickPreferredCandidate(a, b) {
    const aFiltered = !!a?.metadata?.correlation_filtered;
    const bFiltered = !!b?.metadata?.correlation_filtered;
    if (aFiltered !== bFiltered) return aFiltered ? b : a;

    const aStatus = String(a?.status || '');
    const bStatus = String(b?.status || '');
    const activeStatuses = new Set(['paper_running', 'live_candidate']);
    const aActive = activeStatuses.has(aStatus);
    const bActive = activeStatuses.has(bStatus);
    if (aActive !== bActive) return aActive ? a : b;

    const aScore = Number(a?.score || 0);
    const bScore = Number(b?.score || 0);
    if (aScore !== bScore) return aScore > bScore ? a : b;

    const aTs = new Date(String(a?.created_at || a?.updated_at || 0)).getTime() || 0;
    const bTs = new Date(String(b?.created_at || b?.updated_at || 0)).getTime() || 0;
    return aTs >= bTs ? a : b;
  }

  function dedupeCandidatesForDisplay(rows) {
    const grouped = new Map();
    toArray(rows).forEach(cand => {
      const key = buildCandidateDedupKey(cand);
      const existing = grouped.get(key);
      if (!existing) {
        grouped.set(key, { preferred: cand, duplicates: [] });
        return;
      }
      const preferred = pickPreferredCandidate(existing.preferred, cand);
      const duplicate = preferred === cand ? existing.preferred : cand;
      grouped.set(key, {
        preferred,
        duplicates: [...existing.duplicates, duplicate],
      });
    });
    return Array.from(grouped.values()).map(item => ({
      ...item.preferred,
      metadata: {
        ...(item.preferred?.metadata || {}),
        hidden_duplicates_count: item.duplicates.length,
      },
    }));
  }

  function statusText(s) {
    return {
      draft: '草稿',
      research_queued: '排队中',
      research_running: '研究中',
      validated: '已验证',
      rejected: '已拒绝',
      paper_running: '纸盘运行',
      shadow_running: '影子跟踪（未运行）',
      live_candidate: '实盘候选',
      live_running: '实盘运行',
      retired: '已退役',
      new: '新建',
    }[String(s || '')] || String(s || '--');
  }

  function getFamilyMeta(strategy) {
    const family = getStrategyFamily(strategy);
    if (family === 'ml') {
      return { label: 'ML驱动', color: '#ff6b35', accent: 'rgba(255,107,53,.16)' };
    }
    if (family === 'ai_glm') {
      return { label: 'GLM/AI驱动', color: '#38bdf8', accent: 'rgba(56,189,248,.16)' };
    }
    return { label: '传统规则', color: '#64748b', accent: 'rgba(100,116,139,.16)' };
  }

  function getCandidateEnrichment(cand) {
    const meta = cand?.metadata || {};
    const newsCount = Number(meta.news_events_count ?? meta.best?.news_events_count ?? 0);
    const fundingAvailable = !!(meta.funding_available ?? meta.best?.funding_available);
    let mode = '仅 OHLCV';
    if (newsCount > 0 && fundingAvailable) mode = 'OHLCV + 新闻 + 宏观';
    else if (newsCount > 0) mode = 'OHLCV + 新闻';
    else if (fundingAvailable) mode = 'OHLCV + 宏观';
    return {
      newsCount: Number.isFinite(newsCount) ? Math.max(0, Math.round(newsCount)) : 0,
      fundingAvailable,
      mode,
    };
  }

  function getProposalResearchMeta(item) {
    const templates = toArray(item?.strategy_templates);
    const aiTemplateCount = templates.filter(name => getStrategyFamily(name) !== 'traditional').length;
    const lastResearch = item?.metadata?.last_research_result || {};
    const newsCount = Number(lastResearch?.news_events_count || 0);
    const fundingAvailable = !!lastResearch?.funding_available;
    const job = item?.job || {};
    const lastTs = job?.finished_at || job?.started_at || job?.created_at || item?.updated_at || item?.created_at;
    return {
      totalTemplates: templates.length,
      aiTemplateCount,
      newsCount: Number.isFinite(newsCount) ? Math.max(0, Math.round(newsCount)) : 0,
      fundingAvailable,
      lastTs,
    };
  }

  function formatPlannerMacroSummary(macroContext) {
    if (!macroContext) return '宏观摘要：暂无可用外部数据';
    const funding = macroContext?.microstructure?.funding_rate;
    const basis = macroContext?.microstructure?.basis_pct;
    const whales = macroContext?.community?.whale_count ?? 0;
    const news = macroContext?.news?.events_count ?? 0;
    const fundingText = Number.isFinite(Number(funding)) ? Number(funding).toFixed(6) : '--';
    const basisText = Number.isFinite(Number(basis)) ? `${Number(basis).toFixed(3)}%` : '--';
    return normalizeUiText(`宏观摘要：Funding ${fundingText} / Basis ${basisText} / 鲸鱼 ${whales} / 新闻 ${news}`);
  }

  async function aiApi(path, opt = {}) {
    const p = String(path || '').startsWith('/') ? path : `/${path}`;
    if (typeof window.api === 'function') return window.api(`/ai${p}`, opt);
    const { timeoutMs = 15000, ...rest } = opt;
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), Math.max(1000, timeoutMs));
    try {
      const resp = await fetch(`/api/ai${p}`, {
        ...rest, signal: ctrl.signal,
        headers: { 'Content-Type': 'application/json', ...(rest.headers || {}) },
      });
      const ct = String(resp.headers.get('content-type') || '').toLowerCase();
      const data = ct.includes('application/json') ? await resp.json() : { detail: await resp.text() };
      if (!resp.ok) throw new Error(data.detail || data.error || `请求失败(${resp.status})`);
      return data;
    } catch (err) {
      if (err?.name === 'AbortError') throw new Error(`接口超时(${timeoutMs}ms): ${p}`);
      throw err;
    } finally {
      clearTimeout(timer);
    }
  }

  async function rootApi(path, opt = {}) {
    const p = String(path || '').startsWith('/') ? path : `/${path}`;
    if (typeof window.api === 'function') return window.api(p, opt);
    const { timeoutMs = 15000, ...rest } = opt;
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), Math.max(1000, timeoutMs));
    try {
      const resp = await fetch(`/api${p}`, {
        ...rest, signal: ctrl.signal,
        headers: { 'Content-Type': 'application/json', ...(rest.headers || {}) },
      });
      const ct = String(resp.headers.get('content-type') || '').toLowerCase();
      const data = ct.includes('application/json') ? await resp.json() : { detail: await resp.text() };
      if (!resp.ok) throw new Error(data.detail || data.error || `请求失败(${resp.status})`);
      return data;
    } finally {
      clearTimeout(timer);
    }
  }

  /* ══════════════════════════════════════════════════════════════
     信号迷你面板
  ══════════════════════════════════════════════════════════════ */
  function renderSignalMini() {
    const box = document.getElementById('ai-signal-mini');
    if (!box) return;
    const entries = Object.entries(state.latestSignals);
    if (!entries.length) {
      box.innerHTML = '<div style="color:#6b7fa0;font-size:12px;">暂无数据</div>';
      return;
    }
    box.innerHTML = normalizeUiText(entries.map(([sym, data]) => {
      const dir   = String(data?.direction || 'FLAT').toUpperCase();
      const conf  = Math.min(100, Math.round(Number(data?.confidence || 0) * 100));
      const label = { LONG:'看多', SHORT:'看空', FLAT:'持平' }[dir] || dir;
      const blocked = data?.blocked_by_risk;
      const badge  = blocked ? '<span style="color:#e05260;font-size:10px;">风控</span>'
                             : (data?.requires_approval ? '<span style="color:#f0b429;font-size:10px;">审批</span>' : '');
      return `<div class="ai-signal-mini-row">
        <span class="signal-mini-sym">${esc(sym.split('/')[0])}</span>
        <span class="signal-mini-dir ${dir}">${label}${badge}</span>
        <div class="signal-mini-bar"><div class="signal-mini-bar-fill ${dir}" style="width:${conf}%;"></div></div>
        <span class="signal-mini-conf">${conf}%</span>
      </div>`;
    }).join(''));
    normalizeDomText(box);
  }

  async function loadSignal(symbol) {
    if (state.signalLoading) return;
    state.signalLoading = true;
    const statusEl = document.getElementById('signal-status');
    if (statusEl) statusEl.textContent = '刷新中...';
    try {
      const sym = symbol || String(document.getElementById('signal-symbol')?.value || 'BTC/USDT');
      const data = await aiApi(`/signals/latest?symbol=${encodeURIComponent(sym)}`, { timeoutMs: 15000 });
      state.latestSignals[sym] = data;
      renderSignalMini();
      renderCandidateCards();  // 更新卡片上的信号徽章
      if (statusEl) statusEl.textContent = `刷新于 ${fmtTs(data?.timestamp || new Date().toISOString())}`;
    } catch (err) {
      if (statusEl) statusEl.textContent = `信号失败: ${err.message}`;
    } finally {
      state.signalLoading = false;
    }
  }

  /* ══════════════════════════════════════════════════════════════
     候选策略卡片
  ══════════════════════════════════════════════════════════════ */
  function proposalDisplayName(item, index) {
    const metaName = String(item?.metadata?.display_name || '').trim();
    if (metaName) return metaName;
    const seq  = String(item?.metadata?.proposal_sequence || '').trim();
    const mark = seq ? `#${seq}` : `#${String(index + 1).padStart(2, '0')}`;
    const head = String(item?.thesis || '').trim().slice(0, 20);
    return `${mark} ${head || String(item?.proposal_id || '').slice(-6)}`.trim();
  }

  /* ══════════════════════════════════════════════════════════════
     候选策略卡片
  ══════════════════════════════════════════════════════════════ */
  function renderProposalList() {
    const box = document.getElementById('ai-proposal-list');
    const badge = document.getElementById('ai-queue-badge');
    if (!box) return;
    if (badge) badge.textContent = state.proposals.length ? `${state.proposals.length} 项` : '';
    if (!state.proposals.length) {
      box.innerHTML = '<div style="color:#6b7fa0;font-size:12px;padding:8px 0;">暂无研究任务</div>';
      normalizeDomText(box);
      return;
    }
    box.innerHTML = state.proposals.map((item, idx) => {
      const pid = String(item?.proposal_id || '');
      const sel = pid === state.selectedProposalId ? ' selected' : '';
      const st = String(item?.status || 'draft');
      const dotCls = { research_running: 'running', research_queued: 'queued', validated: 'validated', rejected: 'rejected' }[st] || '';
      const name = proposalDisplayName(item, idx);
      const running = ['research_queued', 'research_running'].includes(st);
      const retirable = ['shadow_running', 'live_candidate', 'paper_running'].includes(st);
      const runnable = isRunnableProposalStatus(st);
      const meta = getProposalResearchMeta(item);
      const timeLabel = meta.lastTs ? fmtTs(meta.lastTs) : '--';
      const aiSummary = meta.totalTemplates ? `AI策略 ${meta.aiTemplateCount}/${meta.totalTemplates}` : 'AI策略 0/0';
      const newsSummary = `新闻 ${meta.newsCount}`;
      const macroSummary = meta.fundingAvailable ? '宏观 已启用' : '宏观 未启用';
      return `<div class="proposal-compact-item${sel}" data-proposal-id="${esc(pid)}" data-proposal-status="${esc(st)}" data-action="select-proposal">
        <div class="pci-dot ${dotCls}" title="${esc(statusText(st))}"></div>
        <div style="min-width:0;flex:1;">
          <div class="pci-name" title="${esc(name)}">${esc(name)}</div>
          <div style="font-size:11px;color:#7e92b2;display:flex;gap:8px;flex-wrap:wrap;margin-top:2px;">
            <span>${esc(statusText(st))}</span>
            <span>${esc(timeLabel)}</span>
          </div>
          <div style="font-size:11px;color:#8ea3c2;display:flex;gap:8px;flex-wrap:wrap;margin-top:2px;">
            <span>${esc(aiSummary)}</span>
            <span>${esc(newsSummary)}</span>
            <span>${esc(macroSummary)}</span>
          </div>
        </div>
        <div class="pci-actions">
          ${running
            ? `<button class="btn btn-sm" style="padding:1px 6px;font-size:11px;color:#f0b429;" data-action="cancel-proposal" data-proposal-id="${esc(pid)}" title="取消运行">停</button>`
            : (runnable
              ? `<button class="btn btn-sm" style="padding:1px 6px;font-size:11px;" data-action="run-proposal" data-proposal-id="${esc(pid)}" title="运行研究">跑</button>`
              : '<span style="font-size:10px;color:#7e92b2;">不可运行</span>')}
          ${retirable ? `<button class="btn btn-sm" style="padding:1px 6px;font-size:11px;color:#f59e0b;" data-action="retire-proposal" data-proposal-id="${esc(pid)}" title="退役">退</button>` : ''}
          <button class="btn btn-sm" style="padding:1px 6px;font-size:11px;color:#e05260;" data-action="delete-proposal" data-proposal-id="${esc(pid)}" title="删除">删</button>
        </div>
      </div>`;
    }).join('');
    normalizeDomText(box);
  }

  function renderCandidateCards() {
    const box = document.getElementById('ai-candidate-cards');
    const cnt = document.getElementById('ai-candidate-count');
    if (!box) return;

    const totalCount = state.candidates.length;
    let visible = dedupeCandidatesForDisplay(state.candidates);
    if (state.filterCategory) {
      visible = visible.filter(c => STRATEGY_CATEGORIES[c.strategy] === state.filterCategory);
    }
    visible.sort((a, b) => {
      if (state.sortBy === 'sharpe') {
        return Number(candidateTopResults(b)[0]?.sharpe_ratio ?? 0) - Number(candidateTopResults(a)[0]?.sharpe_ratio ?? 0);
      }
      if (state.sortBy === 'return') {
        return Number(candidateTopResults(b)[0]?.total_return ?? 0) - Number(candidateTopResults(a)[0]?.total_return ?? 0);
      }
      if (state.sortBy === 'drawdown') {
        return Number(candidateTopResults(a)[0]?.max_drawdown ?? 999) - Number(candidateTopResults(b)[0]?.max_drawdown ?? 999);
      }
      return Number(b.score || 0) - Number(a.score || 0);
    });
    const dedupedIds = new Set(dedupeCandidatesForDisplay(state.candidates).map(c => String(c?.candidate_id || '')));
    Array.from(state.compareCandidateIds).forEach((cid) => {
      if (!dedupedIds.has(String(cid))) state.compareCandidateIds.delete(String(cid));
    });

    if (cnt) cnt.textContent = visible.length
      ? `${visible.length}/${state.candidates.length} 个`
      : (state.candidates.length ? `0/${state.candidates.length} (筛选后为空)` : '');

    if (!visible.length) {
      refreshCompareToolbar();
      if (cnt) cnt.textContent = totalCount ? `0/${totalCount}` : '';
      box.innerHTML = state.candidates.length
        ? `<div class="ai-empty-hint">当前类别筛选无结果，请调整筛选条件</div>`
        : `<div class="ai-empty-hint">暂无候选策略。<br>在左侧填写研究目标，点击 <strong>生成研究</strong>，<br>再选中研究任务并点击 <strong>▶ 运行研究</strong> 开始回测。</div>`;
      return;
    }
    box.innerHTML = visible.map(c => buildCandidateCard(c)).join('');
    box.innerHTML = normalizeUiText(box.innerHTML);
    normalizeDomText(box);
    refreshCompareToolbar();
    if (cnt) cnt.textContent = `${visible.length}/${totalCount}`;
  }

  /* ══════════════════════════════════════════════════════════════
     右侧详情面板
  ══════════════════════════════════════════════════════════════ */
  function scoreBar(label, value, max = 100) {
    const n   = Number(value || 0);
    const pct = Math.min(100, (n / max) * 100).toFixed(0);
    const clr = n >= 70 ? '#20bf78' : n >= 50 ? '#f59e0b' : '#e05260';
    return `<div class="score-breakdown-row">
      <div class="score-breakdown-label">${esc(label)}</div>
      <div class="score-breakdown-bar">
        <div class="score-breakdown-fill" style="width:${pct}%;background:${clr};"></div>
      </div>
      <div class="score-breakdown-val">${n.toFixed(0)}</div>
    </div>`;
  }

  function normalizeNumberSeries(values, maxPoints = 240) {
    return toArray(values).map(v => Number(v)).filter(v => Number.isFinite(v)).slice(-Math.max(2, maxPoints));
  }

  function renderSparklineSvg(values) {
    const points = normalizeNumberSeries(values);
    if (points.length < 2) return '';
    const width = 620;
    const height = 130;
    const padX = 6;
    const padY = 10;
    const min = Math.min(...points);
    const max = Math.max(...points);
    const span = Math.max(max - min, 1e-9);
    const stepX = (width - padX * 2) / Math.max(points.length - 1, 1);
    const polyline = points.map((v, i) => {
      const x = padX + i * stepX;
      const y = padY + (height - padY * 2) * (1 - (v - min) / span);
      return `${x.toFixed(2)},${y.toFixed(2)}`;
    }).join(' ');
    const latest = points[points.length - 1];
    const first = points[0];
    const up = latest >= first;
    const lineColor = up ? '#20bf78' : '#e05260';
    const bgTop = up ? 'rgba(32,191,120,.16)' : 'rgba(224,82,96,.16)';
    const bgBottom = 'rgba(18,30,46,.2)';
    return `<svg viewBox="0 0 ${width} ${height}" style="width:100%;height:120px;display:block;background:#111b2a;border:1px solid rgba(255,255,255,.06);border-radius:6px;">
      <defs>
        <linearGradient id="ai-eq-fill" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="${bgTop}" />
          <stop offset="100%" stop-color="${bgBottom}" />
        </linearGradient>
      </defs>
      <rect x="0" y="0" width="${width}" height="${height}" fill="url(#ai-eq-fill)" />
      <polyline fill="none" stroke="${lineColor}" stroke-width="2" points="${polyline}" />
    </svg>`;
  }

  function renderLifecycleRows(rows, emptyText = '暂无生命周期记录') {
    const items = toArray(rows).slice(0, 8);
    if (!items.length) {
      return `<div style="font-size:12px;color:#6b7fa0;">${esc(emptyText)}</div>`;
    }
    return `<div style="display:flex;flex-direction:column;gap:6px;">
      ${items.map(item => `
        <div style="font-size:12px;color:#b7c7e2;padding:6px 8px;background:#141f2f;border-radius:6px;">
          <div style="display:flex;justify-content:space-between;gap:8px;">
            <span>${esc(String(item?.from_state || 'new'))} → ${esc(String(item?.to_state || '--'))}</span>
            <span style="color:#7e92b2;">${esc(fmtTs(item?.ts))}</span>
          </div>
          <div style="color:#7e92b2;margin-top:2px;">${esc(String(item?.actor || 'system'))} · ${esc(String(item?.reason || ''))}</div>
        </div>
      `).join('')}
    </div>`;
  }

  function renderRunRows(rows, emptyText = '暂无实验运行记录') {
    const items = toArray(rows).slice(0, 6);
    if (!items.length) {
      return `<div style="font-size:12px;color:#6b7fa0;">${esc(emptyText)}</div>`;
    }
    return `<div style="display:flex;flex-direction:column;gap:6px;">
      ${items.map(item => `
        <div style="font-size:12px;color:#b7c7e2;padding:6px 8px;background:#141f2f;border-radius:6px;">
          <div style="display:flex;justify-content:space-between;gap:8px;">
            <span>${esc(String(item?.status || '--'))}</span>
            <span style="color:#7e92b2;">${esc(fmtTs(item?.finished_at || item?.started_at || item?.created_at))}</span>
          </div>
          <div style="color:#7e92b2;margin-top:2px;">运行 ID：${esc(String(item?.run_id || '--'))}</div>
        </div>
      `).join('')}
    </div>`;
  }

  const LIFECYCLE_STEPS = [
    { key: 'draft',          label: '研究中' },
    { key: 'validated',      label: '已验证' },
    { key: 'paper_running',  label: '纸盘' },
    { key: 'live_candidate', label: '候选' },
    { key: 'live_running',   label: '实盘' },
  ];

  // Maps every possible status string to its step index in LIFECYCLE_STEPS
  const STATUS_TO_STEP = {
    new: 0, draft: 0, research_queued: 0, research_running: 0,
    validated: 1,
    paper_running: 2, shadow_running: 2,
    live_candidate: 3,
    live_running: 4,
  };

  function renderLifecycleStepper(currentStatus) {
    const status = String(currentStatus || 'draft');
    const retired  = status === 'retired';
    const rejected = status === 'rejected';
    const activeIndex = STATUS_TO_STEP[status] ?? -1;
    return `<div class="lc-stepper">
      ${LIFECYCLE_STEPS.map((step, idx) => {
        let cls = 'lc-step lc-future';
        if (retired || rejected) cls = 'lc-step lc-inactive';
        else if (idx < activeIndex)  cls = 'lc-step lc-done';
        else if (idx === activeIndex) cls = 'lc-step lc-active';
        const doneMark = (!retired && !rejected && idx < activeIndex) ? '✓ ' : '';
        const connector = idx < LIFECYCLE_STEPS.length - 1 ? '<div class="lc-connector"></div>' : '';
        return `<div class="${cls}">${doneMark}${esc(step.label)}</div>${connector}`;
      }).join('')}
      ${retired  ? '<div class="lc-step lc-rejected">已退役</div>' : ''}
      ${rejected ? '<div class="lc-step lc-rejected">已拒绝</div>' : ''}
    </div>`;
  }

  function _renderValidationPipeline(vs) {
    const summary = vs || {};
    const _scoreState = (value, warn = 0.3, ok = 0.6) => {
      if (value == null || !Number.isFinite(Number(value))) return 'vp-na';
      const num = Number(value);
      if (num >= ok) return 'vp-ok';
      if (num >= warn) return 'vp-warn';
      return 'vp-fail';
    };
    const dataReady = (summary.is_score != null || summary.oos_score != null) ? 'vp-ok' : 'vp-na';
    const riskScore = Number(summary.risk_score);
    const riskState = Number.isFinite(riskScore)
      ? (riskScore >= 70 ? 'vp-ok' : riskScore >= 50 ? 'vp-warn' : 'vp-fail')
      : 'vp-na';
    const steps = [
      { label: 'Data', cls: dataReady },
      { label: 'IS', cls: _scoreState(summary.is_score, 0.2, 0.8) },
      { label: 'OOS', cls: _scoreState(summary.oos_score, 0.2, 0.8) },
      { label: 'WF', cls: _scoreState(summary.wf_stability, 0.4, 0.7) },
      { label: 'DSR', cls: _scoreState(summary.dsr_score, 0.25, 0.5) },
      { label: 'Risk', cls: riskState },
    ];
    return `<div class="vp-bar">${steps.map((step, idx) => {
      const connector = idx < steps.length - 1 ? '<span class="vp-arrow">→</span>' : '';
      return `<span class="vp-dot ${step.cls}">${esc(step.label)}</span>${connector}`;
    }).join('')}</div>`;
  }

  function _inlineSparkline(values, width = 140, height = 28) {
    const points = normalizeNumberSeries(values, 80);
    if (points.length < 2) return '';
    const min = Math.min(...points);
    const max = Math.max(...points);
    const span = Math.max(max - min, 1e-9);
    const stepX = width / Math.max(points.length - 1, 1);
    const polyline = points.map((v, i) => {
      const x = i * stepX;
      const y = height - ((v - min) / span) * height;
      return `${x.toFixed(2)},${y.toFixed(2)}`;
    }).join(' ');
    const up = points[points.length - 1] >= points[0];
    const color = up ? '#4ade80' : '#f87171';
    return `<svg viewBox="0 0 ${width} ${height}" class="appr-spark-svg" aria-hidden="true">
      <polyline points="${polyline}" fill="none" stroke="${color}" stroke-width="1.6" />
    </svg>`;
  }

  function _renderApprovalMeta(cand) {
    const summary = cand?.validation_summary || {};
    const top = candidateTopResults(cand)[0] || {};
    const sharpe = summary.is_score ?? top.sharpe_ratio ?? null;
    const oos = summary.oos_score ?? null;
    const dsr = summary.dsr_score ?? null;
    const wf = summary.wf_stability ?? summary.wf_consistency ?? null;
    const fmt2 = (v) => (v == null || !Number.isFinite(Number(v))) ? '--' : Number(v).toFixed(2);
    const fmtPct = (v) => (v == null || !Number.isFinite(Number(v))) ? '--' : `${(Number(v) * 100).toFixed(0)}%`;
    const eq = normalizeNumberSeries(
      cand?.metadata?.best?.equity_curve_sample
      || cand?.metadata?.equity_curve_sample
      || [],
      64,
    );
    return `<div class="appr-meta">
      <div class="appr-metrics">
        <span class="appr-m"><span class="appr-ml">Sharpe</span><b>${fmt2(sharpe)}</b></span>
        <span class="appr-m"><span class="appr-ml">OOS</span><b>${fmt2(oos)}</b></span>
        <span class="appr-m"><span class="appr-ml">DSR</span><b>${fmtPct(dsr)}</b></span>
        <span class="appr-m"><span class="appr-ml">WF</span><b>${fmtPct(wf)}</b></span>
      </div>
      ${eq.length > 1 ? `<div class="appr-spark">${_inlineSparkline(eq)}</div>` : ''}
    </div>`;
  }

  function buildCandidateCard(cand) {
    const score = Number(cand?.score || 0);
    const color = scoreColor(score);
    const emoji = scoreEmoji(score);
    const cid = String(cand?.candidate_id || '');
    const strat = String(cand?.strategy || '--');
    const sym = String(cand?.symbol || '--');
    const tf = String(cand?.timeframe || '--');
    const status = String(cand?.status || 'new');
    const decision = cand?.promotion?.decision || cand?.promotion_target || '';
    const sel = cid === state.selectedCandidateId ? ' selected' : '';
    const top = candidateTopResults(cand)[0] || {};
    const ret = top.total_return != null ? Number(top.total_return) : null;
    const dd = top.max_drawdown != null ? Number(top.max_drawdown) : null;
    const wr = top.win_rate != null ? Number(top.win_rate) : null;
    const sr = top.sharpe_ratio != null ? Number(top.sharpe_ratio) : null;
    const retStr = ret != null ? `<strong style="color:${ret >= 0 ? '#20bf78' : '#e05260'}">${ret >= 0 ? '+' : ''}${ret.toFixed(1)}%</strong>` : '<strong>--</strong>';
    const ddStr = dd != null ? `<strong style="color:#e05260">${dd.toFixed(1)}%</strong>` : '<strong>--</strong>';
    const wrStr = wr != null ? `<strong>${wr.toFixed(0)}%</strong>` : '<strong>--</strong>';
    const srStr = sr != null ? `<strong>${sr.toFixed(2)}</strong>` : '<strong>--</strong>';
    const vs = cand?.validation_summary || {};
    let oosBadge = '';
    if (vs.oos_score != null) {
      const oos = Number(vs.oos_score);
      const oosClr = oos >= 1.0 ? '#20bf78' : oos >= 0.5 ? '#f59e0b' : '#e05260';
      oosBadge = `<span style="font-size:10px;padding:1px 5px;border-radius:3px;background:${oosClr}22;color:${oosClr};border:1px solid ${oosClr}44;">OOS ${oos.toFixed(2)}</span>`;
    }
    let wfBadge = '';
    if (vs.wf_stability != null) {
      const wfs = Number(vs.wf_stability);
      const wfClr = wfs >= 0.7 ? '#20bf78' : wfs >= 0.4 ? '#f59e0b' : '#e05260';
      wfBadge = `<span style="font-size:10px;padding:1px 5px;border-radius:3px;background:${wfClr}22;color:${wfClr};border:1px solid ${wfClr}44;">WF ${(wfs * 100).toFixed(0)}%</span>`;
    }
    const dsrScore = vs.dsr_score;
    const dsrColor = dsrScore != null ? (dsrScore >= 0.5 ? '#2a7a2a' : '#7a2a2a') : '#444';
    const dsrBadge = dsrScore != null
      ? `<span class="cand-badge" style="background:${dsrColor};color:#fff;padding:2px 5px;border-radius:3px;font-size:10px;margin-left:2px;">DSR ${(dsrScore * 100).toFixed(0)}%</span>`
      : '';
    const optMethod = (cand?.metadata && cand.metadata.opt_method) || '';
    const optBadge = optMethod
      ? `<span class="cand-badge" style="background:#1a3a5a;color:#fff;padding:2px 5px;border-radius:3px;font-size:10px;margin-left:2px;">${optMethod === 'scipy_lhs' ? 'Bayes' : 'Grid'}</span>`
      : '';
    const corrFiltered = cand?.metadata?.correlation_filtered;
    const corrWith = cand?.metadata?.correlated_with || '';
    const corrVal = cand?.metadata?.correlation_value;
    const corrIsCross = cand?.metadata?.correlation_is_cross_batch;
    const corrLabel = corrIsCross ? '跨批相关' : '相关';
    const corrBadge = corrFiltered
      ? `<span class="cand-badge" style="background:#7a3a2a;color:#fff;padding:2px 5px;border-radius:3px;font-size:10px;margin-left:2px;" title="与 ${esc(corrWith)} 相关 ρ=${corrVal}">${corrLabel}</span>`
      : '';
    const trials = cand?.metadata?.best?.optimization_trials;
    const paramsBadge = trials > 0
      ? `<span style="font-size:10px;padding:1px 5px;border-radius:3px;background:#a78bfa22;color:#a78bfa;border:1px solid #a78bfa44;">${trials} trials</span>`
      : '';
    let signalBadge = '';
    const sigData = state.latestSignals[sym];
    if (sigData && String(sigData.direction || '') !== 'FLAT') {
      const dir = String(sigData.direction).toUpperCase();
      const conf = Math.round(Number(sigData.confidence || 0) * 100);
      const dirLabel = { LONG: '看多', SHORT: '看空' }[dir] || dir;
      signalBadge = `<span class="cand-signal-badge">${esc(sym.split('/')[0])} ${dirLabel} ${conf}%</span><br>`;
    }
    const canRegister = canRegisterCandidate(cand);
    const compareChecked = state.compareCandidateIds.has(cid) ? 'checked' : '';
    const category = STRATEGY_CATEGORIES[strat] || '';
    const catColor = CATEGORY_COLORS[category] || '#64748b';
    const familyMeta = getFamilyMeta(strat);
    const enrichment = getCandidateEnrichment(cand);
    const catBadge = category
      ? `<span class="cand-category-badge" style="background:${catColor}22;color:${catColor};border:1px solid ${catColor}44;">${esc(category)}</span>`
      : '';
    const familyBadge = `<span class="cand-category-badge" style="background:${familyMeta.accent};color:${familyMeta.color};border:1px solid ${familyMeta.color}44;">${esc(familyMeta.label)}</span>`;
    const hiddenDuplicates = Number(cand?.metadata?.hidden_duplicates_count || 0);
    const enrichmentBadges = [
      `<span class="cand-category-badge" style="background:#1d2b3d;color:#9fb1c9;border:1px solid #32475f;">新闻 ${enrichment.newsCount}</span>`,
      enrichment.fundingAvailable
        ? '<span class="cand-category-badge" style="background:#143224;color:#20bf78;border:1px solid #245b42;">宏观 已启用</span>'
        : '<span class="cand-category-badge" style="background:#2a2330;color:#9a8bb3;border:1px solid #4d4259;">宏观 未启用</span>',
      hiddenDuplicates > 0
        ? `<span class="cand-category-badge" style="background:#3d2b14;color:#f0b429;border:1px solid #6f5321;">去重隐藏 ${hiddenDuplicates}</span>`
        : '',
    ].filter(Boolean).join('');
    const aiCardStyle = getStrategyFamily(strat) === 'traditional'
      ? ''
      : ` style="box-shadow:0 0 0 1px ${familyMeta.color}33 inset, 0 10px 30px ${familyMeta.accent};"`;

    return `<div class="research-candidate-card score-${color}${sel}"${aiCardStyle}
               data-candidate-id="${esc(cid)}" data-action="select-candidate">
      <div class="cand-card-header">
        <div class="cand-card-title">${emoji} ${esc(strat)}</div>
        <div style="display:flex;align-items:center;gap:5px;flex-wrap:wrap;">
          ${familyBadge}${catBadge}<div class="cand-score-badge ${color}">${score.toFixed(0)}</div>
          <label class="cand-compare-toggle" title="加入对比" data-action="toggle-compare" data-candidate-id="${esc(cid)}">
            <input type="checkbox" data-action="toggle-compare" data-candidate-id="${esc(cid)}" ${compareChecked} />
            <span>对比</span>
          </label>
        </div>
      </div>
      <div style="font-size:12px;color:#7e92b2;margin-bottom:5px;">
        ${esc(sym)} / ${esc(tf)} / ${esc(statusText(status))}
      </div>
      <div class="cand-score-bar">
        <div class="cand-score-bar-fill ${color}" style="width:${Math.min(100, score).toFixed(0)}%;"></div>
      </div>
      <div class="cand-metrics">
        <div class="cand-metric-item">收益 ${retStr}</div>
        <div class="cand-metric-item">回撤 ${ddStr}</div>
        <div class="cand-metric-item">胜率 ${wrStr}</div>
        <div class="cand-metric-item">夏普 ${srStr}</div>
      </div>
      <div style="display:flex;gap:4px;flex-wrap:wrap;margin-top:4px;">${enrichmentBadges}</div>
      <div style="font-size:11px;color:#7e92b2;margin-top:4px;">回放模式：${esc(enrichment.mode)}</div>
      ${oosBadge || wfBadge || paramsBadge || dsrBadge || optBadge || corrBadge ? `<div style="display:flex;gap:4px;flex-wrap:wrap;margin-top:4px;">${oosBadge}${wfBadge}${paramsBadge}${dsrBadge}${optBadge}${corrBadge}</div>` : ''}
      ${signalBadge}
      ${_renderValidationPipeline(vs)}
      <div class="cand-recommendation">AI建议：${esc(promotionText(decision))}</div>
      <div class="cand-card-actions">
        <button class="btn btn-sm" data-action="view-candidate" data-candidate-id="${esc(cid)}" style="font-size:12px;">详情</button>
        ${canRegister ? `<button class="btn-register-cta" data-action="open-register" data-candidate-id="${esc(cid)}">一键注册策略</button>` : ''}
      </div>
    </div>`;
  }

  async function viewCandidate(candidateId) {
    if (!candidateId) return;
    const panel = document.getElementById('ai-detail-panel');
    if (panel) panel.innerHTML = '<div style="padding:20px;color:#7e92b2;font-size:13px;">加载中...</div>';
    const resp  = await aiApi(`/candidates/${encodeURIComponent(candidateId)}`, { timeoutMs: 20000 });
    const cand  = resp?.candidate || {};
    state.selectedCandidateId = candidateId;
    renderCandidateCards();   // 更新选中高亮

    if (!panel) return;
    const vs       = cand?.validation_summary || {};
    const promo    = cand?.promotion || {};
    const decision = promo?.decision || cand?.promotion_target || '';
    const top      = candidateTopResults(cand).slice(0, 5);
    const score    = Number(cand?.score || 0);
    const color    = scoreColor(score);
    const showRegisterButton = canRegisterCandidate(cand);
    const governanceGateHint = governanceEnabled() && cand?.metadata?.promotion_pending_human_gate;
    const proposalId = String(cand?.proposal_id || '').trim();
    const experimentId = String(cand?.experiment_id || '').trim();
    const familyMeta = getFamilyMeta(cand?.strategy || '');
    const enrichment = getCandidateEnrichment(cand);

    const [proposalLifecycleResp, candidateLifecycleResp, experimentResp, experimentRunsResp] = await Promise.allSettled([
      proposalId ? aiApi(`/proposals/${encodeURIComponent(proposalId)}/lifecycle?limit=20`, { timeoutMs: 12000 }) : Promise.resolve({ items: [] }),
      aiApi(`/candidates/${encodeURIComponent(candidateId)}/lifecycle?limit=20`, { timeoutMs: 12000 }),
      experimentId ? aiApi(`/experiments/${encodeURIComponent(experimentId)}`, { timeoutMs: 12000 }) : Promise.resolve({ experiment: null }),
      experimentId ? aiApi(`/experiments/${encodeURIComponent(experimentId)}/runs?limit=20`, { timeoutMs: 12000 }) : Promise.resolve({ items: [] }),
    ]);

    const proposalLifecycle = proposalLifecycleResp.status === 'fulfilled' ? toArray(proposalLifecycleResp.value?.items) : [];
    const candidateLifecycle = candidateLifecycleResp.status === 'fulfilled' ? toArray(candidateLifecycleResp.value?.items) : [];
    const experimentInfo = experimentResp.status === 'fulfilled' ? (experimentResp.value?.experiment || null) : null;
    const experimentRuns = experimentRunsResp.status === 'fulfilled' ? toArray(experimentRunsResp.value?.items) : [];

    const topRows = top.map((r, i) => {
      const ret = Number(r?.total_return || 0);
      return `<tr>
        <td>${i + 1}</td>
        <td>${esc(String(r?.strategy || '-'))}</td>
        <td>${esc(String(r?.timeframe || '-'))}</td>
        <td style="color:${ret >= 0 ? '#20bf78' : '#e05260'}">${ret >= 0 ? '+' : ''}${ret.toFixed(1)}%</td>
        <td>${fmtNum(r?.sharpe_ratio, 2)}</td>
        <td>${fmtNum(r?.max_drawdown, 1)}%</td>
      </tr>`;
    }).join('');

    // B: best params section
    const bestParams = cand?.params || {};
    const bestParamsKeys = Object.keys(bestParams);
    const bestParamsHtml = bestParamsKeys.length
      ? `<div style="margin-bottom:14px;">
          <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">最优参数 (Best Params)</div>
          <div style="font-size:12px;color:#c2d0e8;background:#1a2436;border-radius:4px;padding:8px;font-family:monospace;">
            ${bestParamsKeys.map(k => `<span style="color:#a78bfa">${esc(k)}</span>=<span style="color:#20bf78">${esc(String(bestParams[k]))}</span>`).join('  ')}
          </div>
          ${(cand?.metadata?.best?.optimization_trials > 0) ? `<div style="font-size:11px;color:#6b7fa0;margin-top:3px;">共试验 ${cand.metadata.best.optimization_trials} 组参数组合</div>` : ''}
        </div>`
      : '';

    // C: IS/OOS/WF section
    const isScore    = vs?.is_score    != null ? Number(vs.is_score).toFixed(2)    : '--';
    const oosScore   = vs?.oos_score   != null ? Number(vs.oos_score).toFixed(2)   : '--';
    const wfStab     = vs?.wf_stability != null ? `${(Number(vs.wf_stability)*100).toFixed(0)}%` : '--';
    const robustness = vs?.robustness_score != null ? Number(vs.robustness_score).toFixed(0) : '--';
    const oosClr     = vs?.oos_score != null ? (Number(vs.oos_score) >= 1.0 ? '#20bf78' : Number(vs.oos_score) >= 0.5 ? '#f59e0b' : '#e05260') : '#7e92b2';
    const dsrVal    = vs?.dsr_score      != null ? `${(Number(vs.dsr_score)*100).toFixed(1)}%` : '--';
    const wfConsist = vs?.wf_consistency != null ? `${(Number(vs.wf_consistency)*100).toFixed(0)}% folds+` : '--';
    const validationHtml = `
      <div style="margin-bottom:14px;">
        <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">IS / OOS / 滚动验证</div>
        <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:6px;">
          <div style="text-align:center;padding:6px;background:#1a2436;border-radius:4px;">
            <div style="font-size:10px;color:#6b7fa0;">IS夏普</div>
            <div style="font-size:14px;font-weight:700;color:#c2d0e8;">${isScore}</div>
          </div>
          <div style="text-align:center;padding:6px;background:#1a2436;border-radius:4px;">
            <div style="font-size:10px;color:#6b7fa0;">OOS夏普</div>
            <div style="font-size:14px;font-weight:700;color:${oosClr};">${oosScore}</div>
          </div>
          <div style="text-align:center;padding:6px;background:#1a2436;border-radius:4px;">
            <div style="font-size:10px;color:#6b7fa0;">WF稳定性</div>
            <div style="font-size:14px;font-weight:700;color:#c2d0e8;">${wfStab}</div>
          </div>
          <div style="text-align:center;padding:6px;background:#1a2436;border-radius:4px;">
            <div style="font-size:10px;color:#6b7fa0;">鲁棒性分</div>
            <div style="font-size:14px;font-weight:700;color:#c2d0e8;">${robustness}</div>
          </div>
        </div>
        <div style="display:grid;grid-template-columns:repeat(2,1fr);gap:6px;margin-top:6px;">
          <div style="text-align:center;padding:6px;background:#1a2436;border-radius:4px;">
            <div style="font-size:10px;color:#6b7fa0;">DSR 分数</div>
            <div style="font-size:14px;font-weight:700;color:#c2d0e8;">${dsrVal}</div>
          </div>
          <div style="text-align:center;padding:6px;background:#1a2436;border-radius:4px;">
            <div style="font-size:10px;color:#6b7fa0;">WF 一致性</div>
            <div style="font-size:14px;font-weight:700;color:#c2d0e8;">${wfConsist}</div>
          </div>
        </div>
      </div>`;

    const equityCurve = normalizeNumberSeries(cand?.metadata?.best?.equity_curve_sample || []);
    const equityCurveHtml = `
      <div style="margin-bottom:14px;">
        <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">资金曲线样本</div>
        ${equityCurve.length >= 2
          ? renderSparklineSvg(equityCurve)
          : '<div style="font-size:12px;color:#6b7fa0;">暂无资金曲线样本。</div>'}
      </div>`;

    const artifactsHtml = `
      <div style="margin-bottom:14px;">
        <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">研究产物</div>
        <div style="font-size:12px;color:#b7c7e2;background:#141f2f;border-radius:6px;padding:8px;">
          <div>CSV 文件：${esc(String(cand?.metadata?.csv_path || '--'))}</div>
          <div style="margin-top:4px;">Markdown 报告：${esc(String(cand?.metadata?.markdown_path || '--'))}</div>
        </div>
      </div>`;

    const enrichmentHtml = `
      <div style="margin-bottom:14px;">
        <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">研究增强</div>
        <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:6px;">
          <div style="text-align:center;padding:8px;background:#1a2436;border-radius:4px;">
            <div style="font-size:10px;color:#6b7fa0;">决策引擎</div>
            <div style="font-size:13px;font-weight:700;color:${familyMeta.color};">${esc(familyMeta.label)}</div>
          </div>
          <div style="text-align:center;padding:8px;background:#1a2436;border-radius:4px;">
            <div style="font-size:10px;color:#6b7fa0;">新闻事件</div>
            <div style="font-size:13px;font-weight:700;color:#c2d0e8;">${enrichment.newsCount}</div>
          </div>
          <div style="text-align:center;padding:8px;background:#1a2436;border-radius:4px;">
            <div style="font-size:10px;color:#6b7fa0;">宏观层</div>
            <div style="font-size:13px;font-weight:700;color:${enrichment.fundingAvailable ? '#20bf78' : '#9a8bb3'};">${enrichment.fundingAvailable ? '已启用' : '未启用'}</div>
          </div>
        </div>
        <div style="font-size:11px;color:#7e92b2;margin-top:6px;">回放模式：${esc(enrichment.mode)}</div>
      </div>`;

    const experimentHtml = `
      <div style="margin-bottom:14px;">
        <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">实验记录</div>
        <div style="font-size:12px;color:#b7c7e2;background:#141f2f;border-radius:6px;padding:8px;margin-bottom:6px;">
          <div>实验 ID：${esc(String(experimentId || '--'))}</div>
          <div style="margin-top:4px;">状态：${esc(String(experimentInfo?.status || '--'))}</div>
        </div>
        ${renderRunRows(experimentRuns)}
      </div>`;

    const lifecycleHtml = `
      <div style="margin-bottom:14px;">
        <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">候选生命周期</div>
        ${renderLifecycleRows(candidateLifecycle, '暂无候选生命周期记录')}
      </div>
      <div style="margin-bottom:14px;">
        <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">方案生命周期</div>
        ${renderLifecycleRows(proposalLifecycle, '暂无方案生命周期记录')}
      </div>`;
    const paramSensitivityHtml = `
      <details id="ai-param-sensitivity-details" class="ai-param-sensitivity-details">
        <summary>参数敏感性分析</summary>
        <div id="ai-param-sensitivity" class="ai-param-sensitivity-panel">展开后加载参数扰动结果...</div>
      </details>`;

    panel.innerHTML = `
      <div style="margin-bottom:14px;">
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:6px;">
          <span style="font-size:15px;font-weight:700;color:#c2d0e8;">${esc(cand?.strategy || '--')}</span>
          <span class="cand-category-badge" style="background:${familyMeta.accent};color:${familyMeta.color};border:1px solid ${familyMeta.color}44;">${esc(familyMeta.label)}</span>
          <span class="cand-score-badge ${color}" style="font-size:13px;">${score.toFixed(0)} 分</span>
        </div>
        <div style="font-size:12px;color:#7e92b2;">
          ${esc(cand?.symbol || '--')} · ${esc(cand?.timeframe || '--')} · ${esc(statusText(cand?.status))}
        </div>
        ${renderLifecycleStepper(cand?.status)}
      </div>

      <div style="margin-bottom:14px;">
        <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:8px;">评分分解</div>
        ${scoreBar('边际优势', vs?.edge_score)}
        ${scoreBar('风险控制', vs?.risk_score)}
        ${scoreBar('信号稳定', vs?.stability_score)}
        ${scoreBar('执行效率', vs?.efficiency_score)}
        ${scoreBar('综合部署', vs?.deployment_score)}
        ${vs?.reasons?.length ? `<div style="font-size:11px;color:#6b7fa0;margin-top:6px;">说明：${esc(joinText(vs.reasons))}</div>` : ''}
      </div>

      ${validationHtml}
      ${bestParamsHtml}
      ${enrichmentHtml}
      ${equityCurveHtml}
      ${artifactsHtml}
      ${experimentHtml}
      ${lifecycleHtml}
      ${paramSensitivityHtml}
      ${cand?.metadata?.correlation_filtered ? `
      <div style="margin-bottom:12px;padding:8px 10px;background:#3a1a0a;border:1px solid #8b4513;border-radius:6px;font-size:12px;color:#e09060;">
        ⚠ 该候选与 <strong>${esc(cand.metadata.correlated_with || '')}</strong> ${cand.metadata.correlation_is_cross_batch ? '（已运行策略）' : ''}高度相关
        (ρ = ${(cand.metadata.correlation_value || 0).toFixed(2)})，已被相关性过滤器标记为冗余策略。
      </div>` : ''}

      ${cand?.metadata?.llm_rationale ? `
      <div style="margin-bottom:14px;padding:10px 12px;background:#0f1e2e;border:1px solid #1e3a5a;border-radius:6px;">
        <div style="font-size:10px;color:#5b8fc4;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">🤖 AI 分析</div>
        <div style="font-size:12px;color:#b0c4de;line-height:1.6;">${esc(cand.metadata.llm_rationale)}</div>
      </div>` : ''}

      ${(function(){
        const cs = cand?.metadata?.cusum_status;
        const triggered = cs?.triggered;
        const nBars = cs?.n_bars || 0;
        const msg   = cs?.message || '';
        const checkedAt = cs?.checked_at ? new Date(cs.checked_at).toLocaleString() : '';
        const statusHtml = cs
          ? `<div style="display:flex;align-items:center;gap:8px;margin-bottom:4px;">
              <span style="font-size:12px;font-weight:700;color:${triggered ? '#e05260' : '#20bf78'};">${triggered ? '⚠ 已触发衰减' : '✓ 运行正常'}</span>
              <span style="font-size:11px;color:#6b7fa0;">${nBars} 笔交易</span>
            </div>
            <div style="font-size:11px;color:#7e92b2;">${esc(msg)}</div>
            ${checkedAt ? `<div style="font-size:10px;color:#4a5f7a;margin-top:3px;">检测于 ${checkedAt}</div>` : ''}`
          : `<div style="font-size:12px;color:#5b7a9a;">尚未检测。点击按钮对已注册策略执行 CUSUM 衰减分析。</div>`;
        return `<div style="margin-bottom:14px;">
          <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px;">
            <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;">策略衰减检测 (CUSUM)</div>
            <button class="btn btn-sm" id="btn-decay-check" style="font-size:11px;padding:2px 8px;">检查衰减</button>
          </div>
          ${statusHtml}
        </div>`;
      })()}

      <div style="margin-bottom:14px;">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px;">
          <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;">实盘/纸盘性能历史</div>
          <button class="btn btn-sm" id="btn-load-perf-history" style="font-size:11px;padding:2px 8px;" data-candidate-id="${esc(candidateId)}">加载</button>
        </div>
        <div id="perf-history-panel" style="font-size:12px;color:#6b7fa0;">点击加载查看策略运行性能快照</div>
      </div>

      <div style="margin-bottom:14px;">
        <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:8px;">Top 回测结果</div>
        <div style="overflow-x:auto;">
          <table class="data-table" style="font-size:12px;">
            <thead><tr><th>#</th><th>策略</th><th>周期</th><th>年化</th><th>夏普</th><th>回撤</th></tr></thead>
            <tbody>${topRows || '<tr><td colspan="6" style="color:#6b7fa0;">暂无数据</td></tr>'}</tbody>
          </table>
        </div>
      </div>

      ${(function(){
        if (!cand?.metadata?.promotion_pending_human_gate) return '';
        const recTarget = esc(cand?.metadata?.recommended_runtime_target || decision || 'paper');
        return `<div style="margin-bottom:14px;padding:10px 12px;background:#1a0f00;border:2px solid #f59e0b;border-radius:6px;">
          <div style="font-size:11px;color:#f59e0b;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">⏳ 待人工审批</div>
          <div style="font-size:12px;color:#c2d0e8;margin-bottom:8px;">
            AI推荐目标：<strong style="color:#f59e0b;">${recTarget}</strong>
          </div>
          <div class="form-group" style="margin-bottom:8px;">
            <label style="font-size:11px;color:#9fb1c9;">运行目标</label>
            <select id="approval-target-select" style="width:100%;font-size:12px;">
              <option value="paper" ${recTarget === 'paper' ? 'selected' : ''}>纸盘 (paper)</option>
              <option value="live_candidate" ${recTarget === 'live_candidate' ? 'selected' : ''}>实盘候选 (live_candidate)</option>
            </select>
          </div>
          <div class="form-group" style="margin-bottom:8px;">
            <label style="font-size:11px;color:#9fb1c9;">备注</label>
            <input type="text" id="approval-notes-input" placeholder="审批备注（可选）" style="width:100%;font-size:12px;">
          </div>
          <div style="display:flex;gap:8px;">
            <button id="btn-human-approve" class="btn" style="flex:1;font-size:12px;color:#20bf78;border-color:#20bf78;">✓ 批准</button>
            <button id="btn-human-reject" class="btn" style="flex:1;font-size:12px;color:#e05260;border-color:#e05260;">✗ 拒绝</button>
          </div>
        </div>`;
      })()}

      <div style="margin-bottom:16px;">
        <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">AI 推荐</div>
        <div style="font-size:13px;color:#c2d0e8;margin-bottom:3px;">${esc(promotionText(decision))}</div>
        ${promo?.reason ? `<div style="font-size:12px;color:#7e92b2;">${esc(promo.reason)}</div>` : ''}
      </div>

      ${showRegisterButton
        ? `<button class=”btn-register-cta full” data-action=”open-register” data-candidate-id=”${esc(candidateId)}”>
            一键注册策略 →
          </button>`
        : (governanceGateHint
          ? `<div style=”font-size:12px;color:#f0b429;background:#2b1f06;border:1px solid #5c4310;border-radius:6px;padding:8px 10px;”>
              治理模式已开启：请使用上方”待人工审批”进行批准/拒绝。
            </div>`
          : '')}

      <div style=”margin-top:10px;”>
        <button class=”btn btn-sm” id=”btn-order-preview” style=”font-size:12px;width:100%;”>
          生成订单预览
        </button>
        <div id=”ai-order-preview-result” style=”display:none;margin-top:10px;padding:12px;background:#0d1a2a;border:1px solid #1e3a5a;border-radius:8px;”></div>
      </div>
      ${String(cand?.status || '') === 'paper_running' && !governanceEnabled()
        ? `<div style=”margin-top:8px;”>
            <button class=”btn btn-sm” id=”btn-escalate-live”
              style=”font-size:12px;width:100%;color:#f0b429;border-color:#f0b429;”>
              升级为实盘候选 →
            </button>
            <div style=”font-size:10px;color:#6b7fa0;margin-top:3px;”>
              将纸盘标记为实盘候选（不自动下单，需进一步人工确认）
            </div>
           </div>`
        : ''}
      `;
    panel.innerHTML = normalizeUiText(panel.innerHTML)
      .replace(' (Best Params)', '')
      .replace('CSV:', 'CSV 文件：')
      .replace('Markdown:', 'Markdown 报告：')
      .replace('DSR Score', 'DSR 分数')
      .replace('WF Consistency', 'WF 一致性')
      .replace('folds+', '折以上');
    normalizeDomText(panel);
    bindParamSensitivity(candidateId);

    // 订单预览按钮
    panel.querySelector('#btn-order-preview')?.addEventListener('click', () => {
      showOrderPreview(candidateId);
    });

    // 纸盘 → 实盘候选升级按钮
    panel.querySelector('#btn-escalate-live')?.addEventListener('click', async () => {
      if (!confirm(`确认将纸盘候选 ${candidateId.slice(0, 8)} 升级为实盘候选？\n（不会自动下单，后续需要人工确认才能实际启动实盘）`)) return;
      const btn = panel.querySelector('#btn-escalate-live');
      if (btn) { btn.textContent = '升级中...'; btn.disabled = true; }
      try {
        await aiApi(`/candidates/${encodeURIComponent(candidateId)}/promote`, {
          method: 'POST',
          body: JSON.stringify({ target: 'live_candidate' }),
          timeoutMs: 20000,
        });
        notify('已升级为实盘候选，等待进一步审批');
        await refreshWorkbench('', candidateId);
      } catch (err) {
        if (btn) { btn.textContent = '升级为实盘候选 →'; btn.disabled = false; }
        notify(`升级失败: ${err.message}`, true);
      }
    });

    // 绑定详情面板里的按钮
    panel.querySelector('.btn-register-cta')?.addEventListener('click', () => {
      openRegisterModal(candidateId).catch(err => notify(`打开注册失败: ${err.message}`, true));
    });

    // 人工审批按钮
    const approvalSelect = panel.querySelector('#approval-target-select');
    if (approvalSelect) {
      approvalSelect.querySelector('option[value="shadow"]')?.remove();
      if (approvalSelect.value === 'shadow') approvalSelect.value = 'paper';
    }
    const approveBtn = panel.querySelector('#btn-human-approve');
    if (approveBtn) {
      approveBtn.addEventListener('click', async () => {
        const target = document.getElementById('approval-target-select')?.value || 'paper';
        const notes  = document.getElementById('approval-notes-input')?.value || '';
        approveBtn.textContent = '批准中...';
        approveBtn.disabled = true;
        try {
          await aiApi(`/candidates/${encodeURIComponent(candidateId)}/human-approve`, {
            method: 'POST', body: JSON.stringify({ target, notes }), timeoutMs: 30000,
          });
          notify(`已批准策略候选 (${target})`);
          await refreshWorkbench('', candidateId);
        } catch (err) {
          notify(`批准失败: ${err.message}`, true);
          approveBtn.textContent = '✓ 批准';
          approveBtn.disabled = false;
        }
      });
    }
    const rejectBtn = panel.querySelector('#btn-human-reject');
    if (rejectBtn) {
      rejectBtn.addEventListener('click', async () => {
        const notes = document.getElementById('approval-notes-input')?.value || '';
        rejectBtn.textContent = '拒绝中...';
        rejectBtn.disabled = true;
        try {
          await aiApi(`/candidates/${encodeURIComponent(candidateId)}/human-reject`, {
            method: 'POST', body: JSON.stringify({ notes }), timeoutMs: 15000,
          });
          notify('已拒绝策略候选');
          await refreshWorkbench('', '');
        } catch (err) {
          notify(`拒绝失败: ${err.message}`, true);
          rejectBtn.textContent = '✗ 拒绝';
          rejectBtn.disabled = false;
        }
      });
    }

    const perfHistBtn = panel.querySelector('#btn-load-perf-history');
    if (perfHistBtn) {
      perfHistBtn.addEventListener('click', async () => {
        const perfPanel = panel.querySelector('#perf-history-panel');
        if (!perfPanel) return;
        perfHistBtn.disabled = true;
        perfHistBtn.textContent = '加载中...';
        try {
          const data = await aiApi(
            `/performance/snapshots?candidate_id=${encodeURIComponent(candidateId)}&days=30&limit=60`,
            { timeoutMs: 12000 }
          );
          const snaps = Array.isArray(data?.snapshots) ? data.snapshots : [];
          if (!snaps.length) {
            perfPanel.textContent = '暂无性能快照（策略运行后自动记录）';
          } else {
            const reversed = [...snaps].reverse();
            const pnlSeries = reversed.map(s => Number(s.total_pnl_pct || 0));
            perfPanel.innerHTML = `
              <div style="margin-bottom:8px;">${renderSparklineSvg(pnlSeries)}</div>
              <div style="overflow-x:auto;">
                <table class="data-table" style="font-size:11px;width:100%;">
                  <thead><tr><th>时间</th><th>模式</th><th>PnL%</th><th>夏普</th><th>胜率</th><th>交易数</th></tr></thead>
                  <tbody>${reversed.slice(0, 10).map(s => {
                    const pct = Number(s.total_pnl_pct || 0);
                    return `<tr>
                      <td>${esc(String(s.snapshot_at || '').slice(0, 16))}</td>
                      <td>${esc(s.mode || '--')}</td>
                      <td style="color:${pct >= 0 ? '#20bf78' : '#e05260'}">${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%</td>
                      <td>${s.sharpe_ratio != null ? Number(s.sharpe_ratio).toFixed(2) : '--'}</td>
                      <td>${s.win_rate != null ? Number(s.win_rate).toFixed(0) + '%' : '--'}</td>
                      <td>${s.trade_count ?? '--'}</td>
                    </tr>`;
                  }).join('')}</tbody>
                </table>
              </div>
              <div style="font-size:10px;color:#4a5f7a;margin-top:4px;">共 ${snaps.length} 条快照，显示最近 10 条</div>
            `;
          }
        } catch (err) {
          if (perfPanel) perfPanel.textContent = `加载失败: ${String(err?.message || err)}`;
        } finally {
          perfHistBtn.textContent = '加载';
          perfHistBtn.disabled = false;
        }
      });
    }

    const decayBtn = panel.querySelector('#btn-decay-check');
    if (decayBtn) {
      decayBtn.addEventListener('click', async () => {
        decayBtn.textContent = '检测中...';
        decayBtn.disabled = true;
        try {
          await aiApi(`/candidates/${encodeURIComponent(candidateId)}/decay-check`, { timeoutMs: 15000 });
          notify('衰减检测完成');
          viewCandidate(candidateId);   // re-render with fresh data
        } catch (err) {
          notify(`衰减检测失败: ${err.message}`, true);
          decayBtn.textContent = '检查衰减';
          decayBtn.disabled = false;
        }
      });
    }
  }

  /* ══════════════════════════════════════════════════════════════
     一键注册 Modal
  ══════════════════════════════════════════════════════════════ */
  function refreshCompareToolbar() {
    const btn = document.getElementById('ai-compare-btn');
    if (!btn) return;
    const selected = state.compareCandidateIds.size;
    btn.style.display = selected >= 2 ? '' : 'none';
    btn.textContent = selected >= 2 ? `对比选中 (${selected})` : '对比选中';
  }

  function toggleCandidateCompare(candidateId) {
    const cid = String(candidateId || '').trim();
    if (!cid) return;
    if (state.compareCandidateIds.has(cid)) {
      state.compareCandidateIds.delete(cid);
    } else {
      if (state.compareCandidateIds.size >= 4) {
        notify('最多同时对比 4 个候选策略', true);
        return;
      }
      state.compareCandidateIds.add(cid);
    }
    refreshCompareToolbar();
    const list = document.getElementById('ai-candidate-cards');
    if (!list) return;
    list.querySelectorAll('input[data-action="toggle-compare"]').forEach((inputEl) => {
      const id = String(inputEl.getAttribute('data-candidate-id') || '');
      inputEl.checked = state.compareCandidateIds.has(id);
    });
  }

  function openCompareModal() {
    const selectedIds = Array.from(state.compareCandidateIds);
    if (selectedIds.length < 2) return;
    const candidates = selectedIds
      .map((id) => state.candidates.find((cand) => String(cand?.candidate_id || '') === id))
      .filter(Boolean);
    if (candidates.length < 2) {
      notify('可对比的候选策略不足，请重新选择', true);
      return;
    }

    const metrics = [
      ['策略', (c) => esc(String(c?.strategy || '--'))],
      ['状态', (c) => esc(statusText(c?.status || 'new'))],
      ['交易对', (c) => esc(String(c?.symbol || '--'))],
      ['周期', (c) => esc(String(c?.timeframe || '--'))],
      ['综合评分', (c) => Number(c?.score || 0).toFixed(1)],
      ['IS Sharpe', (c) => fmtNum(c?.validation_summary?.is_score, 2)],
      ['OOS Sharpe', (c) => fmtNum(c?.validation_summary?.oos_score, 2)],
      ['WF 稳定', (c) => c?.validation_summary?.wf_stability != null ? `${(Number(c.validation_summary.wf_stability) * 100).toFixed(0)}%` : '--'],
      ['DSR', (c) => c?.validation_summary?.dsr_score != null ? `${(Number(c.validation_summary.dsr_score) * 100).toFixed(0)}%` : '--'],
      ['风险评分', (c) => fmtNum(c?.validation_summary?.risk_score, 0)],
      ['收益率', (c) => {
        const top = candidateTopResults(c)[0] || {};
        const ret = Number(top.total_return);
        return Number.isFinite(ret) ? `${ret >= 0 ? '+' : ''}${ret.toFixed(2)}%` : '--';
      }],
      ['最大回撤', (c) => {
        const top = candidateTopResults(c)[0] || {};
        const dd = Number(top.max_drawdown);
        return Number.isFinite(dd) ? `${dd.toFixed(2)}%` : '--';
      }],
    ];

    const header = candidates.map((c) => {
      const id = String(c?.candidate_id || '').slice(0, 8);
      return `<th>${esc(String(c?.strategy || '--'))}<br><span class="compare-subid">${esc(id)}</span></th>`;
    }).join('');
    const rows = metrics.map(([label, getter]) => {
      const cells = candidates.map((c) => `<td>${getter(c)}</td>`).join('');
      return `<tr><td class="compare-label">${label}</td>${cells}</tr>`;
    }).join('');

    const overlay = document.getElementById('ai-candidate-compare-modal');
    const body = document.getElementById('ai-candidate-compare-body');
    if (!overlay || !body) return;
    body.innerHTML = `<table class="compare-table"><thead><tr><th>指标</th>${header}</tr></thead><tbody>${rows}</tbody></table>`;
    overlay.style.display = 'flex';
  }

  function bindParamSensitivity(candidateId) {
    const details = document.getElementById('ai-param-sensitivity-details');
    if (!details) return;
    const loadIfNeeded = async () => {
      if (details.dataset.loaded === '1' || details.dataset.loading === '1') return;
      details.dataset.loading = '1';
      try {
        await loadParamSensitivity(candidateId);
        details.dataset.loaded = '1';
      } finally {
        details.dataset.loading = '0';
      }
    };
    details.addEventListener('toggle', () => {
      if (details.open) loadIfNeeded().catch(() => {});
    });
    if (details.open) loadIfNeeded().catch(() => {});
  }

  async function loadParamSensitivity(candidateId) {
    const panel = document.getElementById('ai-param-sensitivity');
    if (!panel) return;
    panel.textContent = '计算中...';
    try {
      const payload = await aiApi(`/candidates/${encodeURIComponent(candidateId)}/param-sensitivity?max_params=5`, {
        timeoutMs: 40000,
      });
      const items = toArray(payload?.items);
      if (!items.length) {
        panel.textContent = String(payload?.note || '暂无参数敏感性数据');
        return;
      }
      panel.innerHTML = items.map((row) => {
        const values = [Number(row.sharpe_low), Number(row.sharpe_base), Number(row.sharpe_high)]
          .map((v) => (Number.isFinite(v) ? v : 0));
        const maxAbs = Math.max(0.1, ...values.map((v) => Math.abs(v)));
        const bar = (value, color, label) => {
          const v = Number(value);
          const safe = Number.isFinite(v) ? v : 0;
          const width = Math.max(6, Math.round((Math.abs(safe) / maxAbs) * 120));
          return `<div class="ps-bar-row">
            <span class="ps-lbl">${label}</span>
            <span class="ps-bar-track"><span class="ps-bar-fill" style="width:${width}px;background:${color};"></span></span>
            <span class="ps-val">${Number.isFinite(v) ? v.toFixed(3) : '--'}</span>
          </div>`;
        };
        return `<div class="ps-row">
          <div class="ps-param">${esc(row.param)}</div>
          <div class="ps-bars">
            ${bar(row.sharpe_low, '#f87171', '-20%')}
            ${bar(row.sharpe_base, '#60a5fa', 'Base')}
            ${bar(row.sharpe_high, '#4ade80', '+20%')}
          </div>
        </div>`;
      }).join('');
    } catch (err) {
      panel.textContent = `加载失败: ${String(err?.message || err)}`;
    }
  }

  async function openRegisterModal(candidateId) {
    if (governanceEnabled()) {
      notify('治理模式已开启，请使用“待审批”中的人工批准流程', true);
      return;
    }
    const modal = document.getElementById('ai-register-modal');
    const body  = document.getElementById('ai-register-body');
    if (!modal || !body) return;
    modal.style.display = 'flex';
    body.innerHTML = '<div style="padding:20px;color:#7e92b2;">加载中...</div>';

    const resp   = await aiApi(`/candidates/${encodeURIComponent(candidateId)}`, { timeoutMs: 20000 });
    const cand   = resp?.candidate || {};
    const top    = candidateTopResults(cand)[0] || {};
    const decision = cand?.promotion?.decision || cand?.promotion_target || 'paper';
    const sym    = String(cand?.symbol || '');
    const tf     = String(cand?.timeframe || '');
    const strat  = String(cand?.strategy || 'AI');
    const safeTf = tf.replace(/[^a-zA-Z0-9]/g, '');
    const safeSym = sym.replace(/[^a-zA-Z0-9]/g, '');
    const defaultName = `${strat}_${safeSym}_${safeTf}`;

    const ret = top.total_return   != null ? Number(top.total_return)   : null;
    const dd  = top.max_drawdown   != null ? Number(top.max_drawdown)   : null;
    const wr  = top.win_rate       != null ? Number(top.win_rate) : null;
    const sr  = top.sharpe_ratio   != null ? Number(top.sharpe_ratio)   : null;

    function metricBox(label, value, cls = '') {
      return `<div class="ai-rm-item">
        <div class="ai-rm-label">${label}</div>
        <div class="ai-rm-value ${cls}">${value}</div>
      </div>`;
    }

    body.innerHTML = `
      <div class="form-group" style="margin-bottom:10px;">
        <label>策略名称（可修改）</label>
        <input type="text" id="reg-name" value="${esc(defaultName)}" style="width:100%;">
      </div>
      <div class="form-row" style="margin-bottom:0;">
        <div class="form-group"><label>交易对</label><input readonly value="${esc(sym || '--')}"></div>
        <div class="form-group"><label>时间框</label><input readonly value="${esc(tf || '--')}"></div>
      </div>
      <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin:10px 0 4px;">回测表现</div>
      <div class="ai-register-metrics-grid">
        ${metricBox('年化收益', ret != null ? `${ret >= 0 ? '+' : ''}${ret.toFixed(1)}%` : '--', ret != null && ret >= 0 ? 'positive' : 'negative')}
        ${metricBox('最大回撤', dd != null ? `${dd.toFixed(1)}%` : '--', 'negative')}
        ${metricBox('胜率', wr != null ? `${wr.toFixed(0)}%` : '--')}
        ${metricBox('夏普比率', sr != null ? sr.toFixed(2) : '--')}
      </div>
      <div class="form-group">
        <label>运行模式</label>
        <div class="ai-mode-radio-group">
          <label><input type="radio" name="reg-mode" value="paper" ${decision === 'paper' || !['shadow','live_candidate'].includes(decision) ? 'checked' : ''}> 纸盘（推荐，低风险模拟）</label>
          <label><input type="radio" name="reg-mode" value="shadow" ${decision === 'shadow' ? 'checked' : ''}> 影子追踪（虚拟跟踪）</label>
          <label><input type="radio" name="reg-mode" value="live_candidate" ${decision === 'live_candidate' ? 'checked' : ''}> 实盘候选（待人工确认）</label>
        </div>
      </div>
      <div style="display:flex;justify-content:flex-end;gap:10px;margin-top:16px;padding-top:12px;border-top:1px solid rgba(255,255,255,.07);">
        <button class="btn" id="reg-cancel-btn">取消</button>
        <button class="btn-register-cta" id="reg-confirm-btn" data-candidate-id="${esc(candidateId)}">确认注册</button>
      </div>`;
    body.innerHTML = normalizeUiText(body.innerHTML);
    normalizeDomText(body);

    document.getElementById('reg-cancel-btn').onclick  = () => { modal.style.display = 'none'; };
    const regModeShadow = body.querySelector('input[name="reg-mode"][value="shadow"]');
    if (regModeShadow) {
      regModeShadow.closest('label')?.remove();
    }
    document.getElementById('reg-confirm-btn').onclick = () => {
      const name = String(document.getElementById('reg-name')?.value || '').trim();
      let mode = document.querySelector('input[name="reg-mode"]:checked')?.value || 'paper';
      if (mode === 'shadow') mode = 'paper';
      confirmRegister(candidateId, mode, name);
    };
  }

  async function confirmRegister(candidateId, mode, name) {
    if (governanceEnabled()) {
      notify('治理模式已开启，请改用人工审批流程', true);
      return;
    }
    const btn = document.getElementById('reg-confirm-btn');
    if (btn) { btn.textContent = '注册中...'; btn.disabled = true; }
    try {
      const result = await aiApi(`/candidates/${encodeURIComponent(candidateId)}/register`, {
        method: 'POST',
        body: JSON.stringify({ mode, name: name || undefined }),
        timeoutMs: 30000,
      });
      document.getElementById('ai-register-modal').style.display = 'none';
      const stratName = result?.registered_strategy_name || result?.runtime_status || mode;
      notify(`策略已注册: ${stratName}`);
      await refreshWorkbench('', candidateId);
    } catch (err) {
      if (btn) { btn.textContent = '确认注册'; btn.disabled = false; }
      notify(`注册失败: ${err.message}`, true);
    }
  }

  /* ══════════════════════════════════════════════════════════════
     人工审批队列
  ══════════════════════════════════════════════════════════════ */
  async function loadPendingApprovals() {
    try {
      const res = await aiApi('/candidates/pending-approvals', { timeoutMs: 15000 });
      state.pendingApprovals = toArray(res?.items);
      renderApprovalQueue();
    } catch (err) {
      // Non-fatal — approval queue is best-effort
      console.debug('loadPendingApprovals failed:', err);
    }
  }

  function renderApprovalQueue() {
    const card = document.getElementById('ai-approval-card');
    const list = document.getElementById('ai-approval-list');
    const badge = document.getElementById('ai-approval-badge');
    if (!card || !list) return;

    const items = state.pendingApprovals;
    card.style.display = items.length > 0 ? '' : 'none';
    if (badge) badge.textContent = items.length > 0 ? `(${items.length})` : '';

    if (items.length === 0) { list.innerHTML = ''; return; }

    list.innerHTML = normalizeUiText(items.map(cand => {
      const cid      = esc(cand?.candidate_id || '');
      const strategy = esc(cand?.strategy || '--');
      const target   = esc(cand?.metadata?.recommended_runtime_target || cand?.promotion?.decision || 'paper');
      const score    = Number(cand?.score || 0);
      const color    = scoreColor(score);
      return `<div class="approval-item" style="padding:8px 6px;border-bottom:1px solid rgba(255,255,255,.05);font-size:12px;">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:4px;">
          <span style="color:#c2d0e8;font-weight:600;">${strategy}</span>
          <span class="cand-score-badge ${color}" style="font-size:11px;">${score.toFixed(0)}</span>
        </div>
        <div style="color:#9fb1c9;margin-bottom:5px;">推荐目标：<strong>${target}</strong></div>
        ${_renderApprovalMeta(cand)}
        <div style="display:flex;gap:6px;flex-wrap:wrap;">
          <button class="btn btn-sm" style="font-size:11px;color:#20bf78;border-color:#20bf78;"
            data-action="human-approve" data-candidate-id="${cid}" data-target="${target}">✓ 批准</button>
          <button class="btn btn-sm" style="font-size:11px;color:#e05260;border-color:#e05260;"
            data-action="human-reject" data-candidate-id="${cid}">✗ 拒绝</button>
          <button class="btn btn-sm" style="font-size:11px;color:#f59e0b;border-color:#f59e0b;"
            data-action="quick-register" data-candidate-id="${cid}">纸盘 5%</button>
        </div>
      </div>`;
    }).join(''));
    normalizeDomText(list);
  }

  async function humanApprove(candidateId, target) {
    const notes = window.prompt(`批准候选 ${candidateId.slice(0, 8)} 运行为 [${target}]？\n请输入备注（可留空）：`, '') ?? '';
    if (notes === null) return; // user cancelled
    try {
      await aiApi(`/candidates/${encodeURIComponent(candidateId)}/human-approve`, {
        method: 'POST',
        body: JSON.stringify({ target, notes }),
        timeoutMs: 30000,
      });
      notify(`已批准策略候选 (${target})`);
      await refreshWorkbench('', candidateId);
    } catch (err) {
      notify(`批准失败: ${err.message}`, true);
    }
  }

  async function humanReject(candidateId) {
    const reason = window.prompt(`拒绝候选 ${candidateId.slice(0, 8)}？\n请输入拒绝原因：`, '') ?? '';
    if (reason === null) return; // user cancelled
    try {
      await aiApi(`/candidates/${encodeURIComponent(candidateId)}/human-reject`, {
        method: 'POST',
        body: JSON.stringify({ notes: reason }),
        timeoutMs: 15000,
      });
      notify('已拒绝策略候选');
      await refreshWorkbench('', '');
    } catch (err) {
      notify(`拒绝失败: ${err.message}`, true);
    }
  }

  /* ══════════════════════════════════════════════════════════════
     LLM 辅助研究规划
  ══════════════════════════════════════════════════════════════ */
  async function generateAIContext() {
    const btn = document.getElementById('ai-context-btn');
    if (btn) { btn.textContent = 'AI生成中...'; btn.disabled = true; }
    try {
      const goals = String(document.getElementById('ai-planner-goal')?.value || '').trim();
      if (goals.length < 8) {
        notify('请先填写研究目标（至少8个字符）', true);
        if (btn) { btn.textContent = '🤖 AI辅助'; btn.disabled = false; btn.style.color = ''; }
        return;
      }
      const macroContext = await loadPlannerMacroContext().catch(() => null);
      const marketSummary = { signals: state.latestSignals || {}, macro: macroContext || {} };
      const result = await aiApi('/research/generate-context', {
        method: 'POST',
        body: JSON.stringify({ market_summary: marketSummary, goals, timeout: 30 }),
        timeoutMs: 40000,
      });
      if (result?.llm_research_output) {
        state.pendingLlmContext = result.llm_research_output;
        const hypothesis = String(result.llm_research_output.hypothesis || '').trim();
        const uncertainty = String(result.llm_research_output.uncertainty || '').trim().toLowerCase();
        const suggestedMax = ['高', 'high'].includes(uncertainty) ? 3 : (['低', 'low'].includes(uncertainty) ? 6 : 4);
        if (macroContext) state.pendingMacroContext = macroContext;
        const goalInput = document.getElementById('ai-planner-goal');
        if (goalInput && hypothesis) {
          goalInput.value = `${goals.replace(/\s+/g, ' ').trim()}；AI假设：${hypothesis}`.slice(0, 580);
        }
        const maxTemplatesEl = document.getElementById('ai-planner-max-templates');
        if (maxTemplatesEl) maxTemplatesEl.value = String(suggestedMax);
        if (btn) { btn.textContent = 'AI建议已生成 ✓'; btn.disabled = false; btn.style.color = '#20bf78'; }
        // Show hypothesis in planner notes
        const plannerNotesEl = document.getElementById('ai-planner-notes');
        const macroSummary = macroContext
          ? `宏观摘要：Funding ${macroContext?.microstructure?.funding_rate ?? '--'} / Basis ${macroContext?.microstructure?.basis_pct ?? '--'} / 巨鲸 ${macroContext?.community?.whale_count ?? 0} / News ${macroContext?.news?.events_count ?? 0}`
          : '宏观摘要：暂无';
        if (plannerNotesEl && result.llm_research_output.hypothesis) {
          const existing = plannerNotesEl.innerHTML;
          plannerNotesEl.innerHTML = `<div style="font-size:11px;color:#20bf78;margin-bottom:3px;">🤖 AI假设：${esc(result.llm_research_output.hypothesis)}</div>` + existing;
        }
        notify('AI辅助建议已生成。下一步请点“生成研究”，会自动带上这份建议。');
      } else {
        notify(`AI辅助失败: ${result?.error || 'LLM不可用'}`, true);
        if (btn) { btn.textContent = '🤖 AI辅助'; btn.disabled = false; btn.style.color = ''; }
      }
    } catch (err) {
      notify(`AI辅助失败: ${err.message}`, true);
      if (btn) { btn.textContent = '🤖 AI辅助'; btn.disabled = false; btn.style.color = ''; }
    }
  }

  /* ══════════════════════════════════════════════════════════════
     数据加载
  ══════════════════════════════════════════════════════════════ */
  async function loadRuntimeConfig(force = false) {
    if (!force && state.runtimeConfigLoaded && state.runtimeConfig) return;
    const prevGovernance = !!(state.runtimeConfig && state.runtimeConfig.governance_enabled);
    try {
      const res = await aiApi('/runtime-config', { timeoutMs: 10000 });
      state.runtimeConfig = {
        governance_enabled: !!res?.governance_enabled,
        decision_mode: String(res?.decision_mode || ''),
        trading_mode: String(res?.trading_mode || ''),
        ai_live_decision: res?.ai_live_decision || null,
      };
      state.runtimeConfigLoaded = true;
      renderLiveDecisionRuntimeConfig();
      const nextGovernance = !!state.runtimeConfig.governance_enabled;
      if (prevGovernance !== nextGovernance) {
        renderCandidateCards();
        if (state.selectedCandidateId) {
          viewCandidate(state.selectedCandidateId).catch(() => {});
        }
      }
    } catch (err) {
      // Non-fatal: default to governance disabled for UI guard only.
      if (!state.runtimeConfig) {
        state.runtimeConfig = {
          governance_enabled: false,
          decision_mode: '',
          trading_mode: '',
          ai_live_decision: null,
        };
      }
      state.runtimeConfigLoaded = true;
      renderLiveDecisionRuntimeConfig();
      console.debug('loadRuntimeConfig failed:', err);
    }
  }

  async function loadProposals(selectId = '') {
    const res = await aiApi('/proposals?limit=50', { timeoutMs: 20000 });
    state.proposals = toArray(res?.items);
    if (selectId) state.selectedProposalId = selectId;
    renderProposalList();
    updateRunBtn();
  }

  async function loadCandidates(selectId = '') {
    const res = await aiApi('/candidates?limit=50', { timeoutMs: 20000 });
    state.candidates = toArray(res?.items);
    if (selectId) state.selectedCandidateId = selectId;
    renderCandidateCards();
  }

  async function refreshWorkbench(selectProposalId = '', selectCandidateId = '') {
    await loadRuntimeConfig();
    await Promise.all([loadProposals(selectProposalId), loadCandidates(selectCandidateId), loadPendingApprovals(), loadDataReadiness().catch(() => null)]);
    normalizeDomText(document.getElementById('ai-research'));
  }

  function updateRunBtn() {
    const btn = document.getElementById('run-selected-btn');
    if (!btn) return;
    const has = !!state.selectedProposalId;
    btn.disabled = !has;
    btn.title = has ? `运行研究: ${state.selectedProposalId}` : '请先在左侧选择研究任务';
  }

  /* ══════════════════════════════════════════════════════════════
     操作函数
  ══════════════════════════════════════════════════════════════ */
  /* ── 实时市场上下文采集（生成研究前自动执行）── */
  async function _collectLiveMarketContext(primarySymbol) {
    const sym = primarySymbol || getCurrentResearchSymbol() || 'BTC/USDT';
    const exchange = getCurrentResearchExchange() || 'binance';
    const [signalRes, microRes, newsSummaryRes] = await Promise.allSettled([
      aiApi(`/signals/latest?symbol=${encodeURIComponent(sym)}`, { timeoutMs: 8000 }),
      rootApi(`/trading/analytics/microstructure?exchange=${encodeURIComponent(exchange)}&symbol=${encodeURIComponent(sym)}&depth_limit=10`, { timeoutMs: 8000 }),
      rootApi(`/news/summary?symbol=${encodeURIComponent(sym.split('/')[0])}&hours=6`, { timeoutMs: 8000 }),
    ]);
    const signal  = signalRes.status  === 'fulfilled' ? (signalRes.value  || {}) : {};
    const micro   = microRes.status   === 'fulfilled' ? (microRes.value   || {}) : {};
    const news    = newsSummaryRes.status === 'fulfilled' ? (newsSummaryRes.value || {}) : {};

    const direction   = String(signal.direction || 'FLAT').toUpperCase();
    const confidence  = Number(signal.confidence || 0);
    const fundingRate = micro?.funding_rate?.funding_rate ?? micro?.microstructure?.funding_rate ?? null;
    const imbalance   = micro?.aggressor_flow?.imbalance ?? micro?.microstructure?.order_flow_imbalance ?? null;
    const spreadBps   = micro?.orderbook?.spread_bps ?? null;
    const newsEvents  = Number(news?.events_count ?? 0);
    const whaleCount  = Number(micro?.whale_activity?.count ?? 0);
    const oiChangePct   = micro?.oi?.change_pct_1h ?? micro?.open_interest?.change_pct_1h ?? 0;
    const optionsSkew   = micro?.options?.skew_25d ?? null;
    const optionsPcRatio = micro?.options?.put_call_ratio ?? null;
    const optionsSignal  = micro?.options?.signal ?? null;

    // Derive volatility hint from spread
    let volatility = '';
    if (spreadBps != null) {
      volatility = spreadBps >= 8 ? 'high' : spreadBps <= 2 ? 'low' : 'normal';
    }

    return {
      symbol: sym,
      sentiment: direction,
      confidence,
      volatility,
      factors: {
        momentum:      direction === 'LONG'  ? Math.min(1, confidence * 1.2) : 0,
        mean_reversion: direction === 'FLAT' ? 0.6 : 0,
        trend_strength: direction !== 'FLAT' ? Math.min(1, confidence * 1.5) : 0,
      },
      microstructure: {
        funding_rate:        fundingRate,
        order_flow_imbalance: imbalance,
        spread_bps:          spreadBps,
        volume_surge:        imbalance != null ? Math.abs(imbalance) : 0,
      },
      news:  { events_count: newsEvents },
      whale: { count: whaleCount },
      oi_change_pct: oiChangePct,
      options_skew_25d:   optionsSkew,
      options_pc_ratio:   optionsPcRatio,
      options_signal:     optionsSignal,
    };
  }

  async function generateProposal() {
    const goal = String(document.getElementById('ai-planner-goal')?.value || '').trim();
    if (goal.length < 8) { notify('研究目标太短（至少8个字符）', true); return; }
    const symbols   = csvInput('ai-planner-symbols');
    const primarySym = symbols[0] || getCurrentResearchSymbol() || 'BTC/USDT';

    // ── 自动采集实时市场上下文 ──
    const marketCtxEl = document.getElementById('ai-market-context-hint');
    if (marketCtxEl) marketCtxEl.textContent = '正在采集市场上下文...';
    const liveCtx = await _collectLiveMarketContext(primarySym).catch(() => ({}));
    if (marketCtxEl) {
      const dir   = String(liveCtx.sentiment || 'FLAT');
      const conf  = Math.round(Number(liveCtx.confidence || 0) * 100);
      const fr    = liveCtx.microstructure?.funding_rate;
      const ofi   = liveCtx.microstructure?.order_flow_imbalance;
      const ne    = liveCtx.news?.events_count ?? '--';
      const frTxt  = fr != null ? fr.toFixed(5) : '--';
      const ofiTxt = ofi != null ? ofi.toFixed(3) : '--';
      const oi     = liveCtx.oi_change_pct;
      const oiTxt  = oi != null && oi !== 0 ? (oi > 0 ? '+' : '') + Number(oi).toFixed(1) + '%' : '--';
      const optSkew = liveCtx.options_skew_25d;
      const optSig  = liveCtx.options_signal;
      const optTxt  = optSkew != null
        ? `${Number(optSkew).toFixed(3)}(${optSig || '?'})`
        : '--';
      marketCtxEl.innerHTML = `<span style="color:${dir==='LONG'?'#20bf78':dir==='SHORT'?'#e05260':'#9fb1c9'}">方向 ${dir} ${conf}%</span> · Funding ${frTxt} · OFI ${ofiTxt} · OI ${oiTxt} · 期权偏斜 ${optTxt} · 新闻事件 ${ne}`;
    }

    const payload = {
      goal,
      market_regime: String(document.getElementById('ai-planner-regime')?.value || 'mixed'),
      symbols,
      timeframes:    csvInput('ai-planner-timeframes'),
      constraints:   { max_templates: Number(document.getElementById('ai-planner-max-templates')?.value || 5) },
      market_context: liveCtx,
    };
    // Attach pending LLM context if available, then clear it
    if (state.pendingLlmContext) {
      payload.llm_research_output = state.pendingLlmContext;
      state.pendingLlmContext = null;
      const btn = document.getElementById('ai-context-btn');
      if (btn) { btn.textContent = '🤖 AI辅助'; btn.disabled = false; btn.style.color = ''; }
    }
    const result = await aiApi('/proposals/generate', { method: 'POST', body: JSON.stringify(payload), timeoutMs: 30000 });
    // A: show filtered templates and planner notes
    const filteredTpls = result?.filtered_templates || result?.proposal?.filtered_templates || [];
    const plannerNotes = result?.planner_notes || [];
    let notifMsg = '研究任务已生成';
    if (filteredTpls.length > 0) {
      notifMsg += `（过滤了 ${filteredTpls.length} 个不支持的模板）`;
    }
    // Update planner notes UI if it exists
    const plannerNotesEl = document.getElementById('ai-planner-notes');
    if (plannerNotesEl) {
      let html = '';
      if (plannerNotes.length) {
        html += `<div style="font-size:11px;color:#9fb1c9;margin-bottom:3px;">📋 ${plannerNotes.map(n => esc(n)).join(' · ')}</div>`;
      }
      if (filteredTpls.length) {
        html += `<div style="font-size:11px;color:#f59e0b;margin-top:3px;">⚠️ 过滤模板（${filteredTpls.length}）: ${filteredTpls.slice(0,5).map(t => esc(t)).join(', ')}${filteredTpls.length > 5 ? '...' : ''}</div>`;
      }
      plannerNotesEl.innerHTML = html;
    }
    notify(notifMsg);
    await refreshWorkbench(result?.proposal?.proposal_id || '', '');
  }

  generateAIContext = async function generateAIContextOverride() {
    const btn = document.getElementById('ai-context-btn');
    if (btn) { btn.textContent = 'AI 分析中...'; btn.disabled = true; btn.style.color = ''; }
    try {
      const goalInput = document.getElementById('ai-planner-goal');
      const goals = String(goalInput?.value || '').trim();
      if (goals.length < 8) {
        notify('请先填写足够明确的研究目标。', true);
        if (btn) { btn.textContent = 'AI 辅助'; btn.disabled = false; }
        return;
      }

      const macroContext = await loadPlannerMacroContext().catch(() => null);
      const marketSummary = { signals: state.latestSignals || {}, macro: macroContext || {} };
      const result = await aiApi('/research/generate-context', {
        method: 'POST',
        body: JSON.stringify({ market_summary: marketSummary, goals, timeout: 30 }),
        timeoutMs: 40000,
      });
      if (!result?.llm_research_output) {
        notify(`AI 辅助失败: ${result?.error || 'LLM 未返回有效内容'}`, true);
        if (btn) { btn.textContent = 'AI 辅助'; btn.disabled = false; }
        return;
      }

      state.pendingLlmContext = result.llm_research_output;
      if (macroContext) state.pendingMacroContext = macroContext;

      const hypothesis = String(result.llm_research_output.hypothesis || '').trim();
      const uncertainty = String(result.llm_research_output.uncertainty || '').trim().toLowerCase();
      const suggestedMax = ['high', '高'].includes(uncertainty) ? 3 : (['low', '低'].includes(uncertainty) ? 6 : 4);
      if (goalInput && hypothesis) {
        goalInput.value = `${goals.replace(/\s+/g, ' ').trim()} | AI假设: ${hypothesis}`.slice(0, 580);
      }

      const maxTemplatesEl = document.getElementById('ai-planner-max-templates');
      if (maxTemplatesEl) maxTemplatesEl.value = String(suggestedMax);

      const plannerNotesEl = document.getElementById('ai-planner-notes');
      if (plannerNotesEl) {
        const notes = [];
        if (hypothesis) notes.push(`<div style="font-size:11px;color:#20bf78;margin-bottom:4px;">AI 假设：${esc(hypothesis)}</div>`);
        notes.push(`<div style="font-size:11px;color:#9fb1c9;margin-bottom:4px;">AI 建议策略数上限：${suggestedMax}</div>`);
        notes.push(`<div style="font-size:11px;color:#9fb1c9;">${esc(formatPlannerMacroSummary(macroContext))}</div>`);
        plannerNotesEl.innerHTML = notes.join('');
      }

      if (btn) { btn.textContent = 'AI 建议已应用'; btn.disabled = false; btn.style.color = '#20bf78'; }
      notify('AI 辅助已写回研究目标、策略数量和宏观上下文。');
    } catch (err) {
      notify(`AI 辅助失败: ${err.message}`, true);
      if (btn) { btn.textContent = 'AI 辅助'; btn.disabled = false; btn.style.color = ''; }
    }
  };

  generateProposal = async function generateProposalOverride() {
    const goal = String(document.getElementById('ai-planner-goal')?.value || '').trim();
    if (goal.length < 8) { notify('请先填写研究目标。', true); return; }

    let marketContext = state.pendingMacroContext || null;
    if (!marketContext) {
      marketContext = await loadPlannerMacroContext().catch(() => null);
    }

    const payload = {
      goal,
      market_regime: String(document.getElementById('ai-planner-regime')?.value || 'mixed'),
      symbols: csvInput('ai-planner-symbols'),
      timeframes: csvInput('ai-planner-timeframes'),
      constraints: { max_templates: Number(document.getElementById('ai-planner-max-templates')?.value || 5) },
      market_context: marketContext || {},
    };
    if (state.pendingLlmContext) {
      payload.llm_research_output = state.pendingLlmContext;
      state.pendingLlmContext = null;
      const btn = document.getElementById('ai-context-btn');
      if (btn) { btn.textContent = 'AI 辅助'; btn.disabled = false; btn.style.color = ''; }
    }

    const result = await aiApi('/proposals/generate', {
      method: 'POST',
      body: JSON.stringify(payload),
      timeoutMs: 30000,
    });
    const filteredTpls = result?.filtered_templates || result?.proposal?.filtered_templates || [];
    const plannerNotes = result?.planner_notes || [];
    const plannerNotesEl = document.getElementById('ai-planner-notes');
    if (plannerNotesEl) {
      let html = '';
      if (plannerNotes.length) {
        html += `<div style="font-size:11px;color:#9fb1c9;margin-bottom:4px;">规划说明：${plannerNotes.map(n => esc(n)).join(' ｜ ')}</div>`;
      }
      html += `<div style="font-size:11px;color:#9fb1c9;margin-bottom:4px;">${esc(formatPlannerMacroSummary(marketContext))}</div>`;
      if (filteredTpls.length) {
        html += `<div style="font-size:11px;color:#f59e0b;">已过滤未接入研究引擎的策略 ${filteredTpls.length} 个：${filteredTpls.slice(0, 5).map(t => esc(t)).join(', ')}${filteredTpls.length > 5 ? '...' : ''}</div>`;
      }
      plannerNotesEl.innerHTML = html;
    }

    notify(filteredTpls.length ? `研究方案已生成，过滤了 ${filteredTpls.length} 个未接入策略。` : '研究方案已生成。');
    await refreshWorkbench(result?.proposal?.proposal_id || '', '');
  };

  async function runProposal(proposalId) {
    if (!proposalId) { notify('请先选择研究任务', true); return; }
    const proposal = state.proposals.find(p => String(p?.proposal_id || '') === String(proposalId));
    const proposalStatus = String(proposal?.status || '');
    if (proposal && !isRunnableProposalStatus(proposalStatus)) {
      notify(`当前状态「${statusText(proposalStatus)}」不可运行`, true);
      return;
    }
    const exchange = String(document.getElementById('run-exchange')?.value || 'binance');
    const days     = Math.max(1, Math.min(3650, parseInt(document.getElementById('run-days')?.value || '3', 10) || 3));
    notify(`研究任务已提交，后台运行中...`);
    const result = await aiApi(`/proposals/${encodeURIComponent(proposalId)}/run`, {
      method: 'POST',
      body: JSON.stringify({ exchange, days, background: true }),
      timeoutMs: 15000,
    });
    const jobId = result?.job?.job_id;
    const pid   = result?.proposal?.proposal_id || proposalId;
    if (jobId) {
      startJobPolling(pid, jobId);
    }
    await refreshWorkbench(pid, '');
  }

  async function cancelProposal(proposalId) {
    if (!proposalId) return;
    const proposal = state.proposals.find(p => String(p?.proposal_id || '') === String(proposalId));
    const status = String(proposal?.status || '');
    if (!['research_queued', 'research_running'].includes(status)) {
      notify(`当前状态「${statusText(status)}」无需取消`, true);
      return;
    }
    if (!window.confirm(`确认取消该研究任务运行？\n${proposalId}`)) return;
    const result = await aiApi(`/proposals/${encodeURIComponent(proposalId)}/cancel`, {
      method: 'POST',
      timeoutMs: 15000,
    });
    stopJobPolling(proposalId);
    if (result?.cancelled) {
      notify('研究任务已取消');
    } else {
      notify(result?.reason || '未找到可取消任务', true);
    }
    await refreshWorkbench(proposalId, '');
  }

  function getPrimaryPlannerSymbol() {
    const raw = String(document.getElementById('ai-planner-symbols')?.value || '').trim();
    const first = raw.split(',').map(s => s.trim()).filter(Boolean)[0];
    return first || getCurrentResearchSymbol();
  }

  async function warmFundingForResearch() {
    const exchange = String(document.getElementById('run-exchange')?.value || getCurrentResearchExchange() || 'binance');
    const symbol = getPrimaryPlannerSymbol();
    const result = await aiApi('/diagnostics/funding-cache/warm', {
      method: 'POST',
      body: JSON.stringify({ exchange, symbol, days: 90, source: 'auto' }),
      timeoutMs: 30000,
    });
    const path = String(result?.funding?.cache_path || '');
    notify(path ? `宏观缓存已预热: ${path}` : '宏观缓存已预热');
    await loadDataReadiness().catch(() => {});
  }

  /* ── 任务进度轮询 ── */
  async function retireProposal(proposalId) {
    const item = state.proposals.find(p => String(p?.proposal_id || '') === proposalId);
    const status = String(item?.status || '');
    if (!proposalId) {
      notify('缺少 proposal_id，无法退役', true);
      return;
    }
    if (!['shadow_running', 'live_candidate', 'paper_running'].includes(status)) {
      notify(`当前状态 ${statusText(status)} 不支持退役`, true);
      return;
    }
    if (!window.confirm(`确认将该研究退役？\n${proposalId}\n退役后会退出跟踪并允许删除。`)) return;
    try {
      await aiApi(`/proposals/${encodeURIComponent(proposalId)}/retire`, {
        method: 'POST',
        body: JSON.stringify({ notes: 'retired from AI research queue' }),
        timeoutMs: 15000,
      });
      notify('研究已退役');
    } catch (err) {
      const msg = String(err?.message || '');
      if (/404|not found/i.test(msg)) {
        await deleteProposal(proposalId);
        notify('旧影子记录未命中退役接口，已直接删除');
        return;
      }
      throw err;
    }
    await refreshWorkbench(proposalId, '');
  }

  async function pullNewsForResearch() {
    const symbol = getPrimaryPlannerSymbol();
    const query = symbolToNewsKey(symbol);
    const result = await rootApi('/news/pull_now?background=true', {
      method: 'POST',
      body: JSON.stringify({ since_minutes: 720, max_records: 120, query }),
      timeoutMs: 12000,
    });
    if (Number(result?.queued_count || result?.job?.result?.queued_count || 0) > 0) {
      rootApi('/news/worker/run_once?llm_limit=12&background=true', { method: 'POST', timeoutMs: 8000 }).catch(() => ({}));
    }
    notify(result?.queued ? '新闻拉取任务已提交' : '新闻拉取完成');
    await loadDataReadiness().catch(() => {});
  }

  async function loadDataReadiness() {
    const panel = document.getElementById('ai-data-readiness-panel');
    const summaryEl = document.getElementById('ai-data-readiness-summary');
    const detailsEl = document.getElementById('ai-data-readiness-details');
    if (!panel || !summaryEl || !detailsEl) return;
    const exchange = String(document.getElementById('run-exchange')?.value || getCurrentResearchExchange() || 'binance');
    const symbol = getPrimaryPlannerSymbol();
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
    const premiumSources = premiumData?.sources || {};
    const premiumRows = Object.entries(premiumSources).map(([name, source]) => {
      const hasCached = !!source?.has_cached_data;
      const configured = !!source?.key_configured;
      return {
        name,
        hasCached,
        configured,
        available: !!source?.available,
      };
    });
    const premiumCachedCount = premiumRows.filter(row => row.hasCached).length;
    const premiumConfiguredCount = premiumRows.filter(row => row.configured).length;
    const premiumTotalCount = premiumRows.length;
    const premiumActiveNames = premiumRows.filter(row => row.hasCached).map(row => row.name);

    const summary = Number(symbolSummary?.events_count || 0) > 0 || Number(symbolSummary?.feed_count || 0) > 0
      ? symbolSummary
      : globalSummary;
    const summaryScope = summary === symbolSummary ? `币种 ${newsSymbol}` : '全市场';
    const newsEvents = Number(summary?.events_count || 0);
    const rawCount = Number(summary?.raw_count || 0);
    const feedCount = Number(summary?.feed_count || 0);
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
    if (!fundingRows) issues.push('资金费率缓存为空');
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
        <div>缓存行数 ${fundingRows} / Funding ${Number.isFinite(Number(fundingRate)) ? Number(fundingRate).toFixed(6) : '--'} / Basis ${Number.isFinite(Number(basisPct)) ? Number(basisPct).toFixed(3) + '%' : '--'}</div>
        <div>覆盖区间 ${esc(coverage?.start || '--')} ~ ${esc(coverage?.end || '--')}</div>
        <div style="margin-top:4px;color:#7e92b2;">Funding 缓存路径: ${esc(fundingPath)}</div>
      </div>
      <div style="padding:8px;background:#141f2f;border-radius:6px;">
        <div style="color:#c2d0e8;font-weight:700;margin-bottom:4px;">社区 / 巨鲸 / 公告</div>
        <div>巨鲸 ${whaleCount} / 公告 ${announcementCount} / 微观点差 ${Number(microData?.orderbook?.spread_bps || 0).toFixed(2)} bps</div>
      </div>
      <div style="padding:8px;background:#141f2f;border-radius:6px;">
        <div style="color:#c2d0e8;font-weight:700;margin-bottom:4px;">高级数据源</div>
        <div>缓存 ${premiumCachedCount}/${premiumTotalCount} / Key 已配置 ${premiumConfiguredCount}</div>
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
    normalizeDomText(detailsEl);
  }

  function startJobPolling(proposalId, jobId) {
    stopJobPolling(proposalId);
    state.jobPollingTimers[proposalId] = setInterval(
      () => pollJobStatus(proposalId, jobId).catch(() => {}),
      JOB_POLL_MS,
    );
  }

  function stopJobPolling(proposalId) {
    const t = state.jobPollingTimers[proposalId];
    if (t) { clearInterval(t); delete state.jobPollingTimers[proposalId]; }
  }

  async function pollJobStatus(proposalId, _jobId) {
    const data = await aiApi(`/proposals/${encodeURIComponent(proposalId)}/job-status`, { timeoutMs: 8000 });
    const js   = data?.job_status;
    // Keep proposal list dot up to date
    const idx  = state.proposals.findIndex(p => String(p?.proposal_id || '') === proposalId);
    if (idx >= 0 && state.proposals[idx].status !== data?.proposal_status) {
      state.proposals[idx] = { ...state.proposals[idx], status: data.proposal_status };
      renderProposalList();
    }
    if (js === 'completed') {
      stopJobPolling(proposalId);
      notify(`研究完成，候选策略已更新`);
      await refreshWorkbench(proposalId, '');
    } else if (js === 'cancelled') {
      stopJobPolling(proposalId);
      notify('研究任务已取消');
      await loadProposals(proposalId);
    } else if (js === 'failed') {
      stopJobPolling(proposalId);
      notify(`研究失败: ${data?.error || '未知错误'}`, true);
      await loadProposals(proposalId);
    }
  }

  async function deleteProposal(proposalId) {
    if (!proposalId) return;
    const item = state.proposals.find(p => String(p?.proposal_id || '') === proposalId);
    const blocked = new Set(['paper_running','shadow_running','live_running']);
    if (item && blocked.has(String(item?.status || ''))) {
      notify(`当前状态「${statusText(item.status)}」不可删除，请先停止后再删除。`, true);
      return;
    }
    if (!window.confirm(`确认删除此研究任务？\n${proposalId}\n将级联删除相关候选记录。`)) return;
    await aiApi(`/proposals/${encodeURIComponent(proposalId)}`, { method: 'DELETE', timeoutMs: 20000 });
    notify(`研究任务已删除`);
    if (state.selectedProposalId === proposalId) {
      state.selectedProposalId = '';
      const panel = document.getElementById('ai-detail-panel');
      if (panel) panel.innerHTML = '<div class="ai-detail-placeholder"><div style="font-size:36px;opacity:.3;">📊</div><div style="margin-top:10px;color:#6b7fa0;font-size:13px;">点击候选策略卡片<br>查看详细分析与注册</div></div>';
    }
    await refreshWorkbench('', '');
  }

  /* ══════════════════════════════════════════════════════════════
     事件绑定
  ══════════════════════════════════════════════════════════════ */
  function bindEvents() {
    /* 生成研究 */
    document.getElementById('ai-generate-btn')?.addEventListener('click', () =>
      generateProposal().catch(err => notify(`生成失败: ${err.message}`, true)));

    /* AI辅助研究规划 */
    document.getElementById('ai-context-btn')?.addEventListener('click', () =>
      generateAIContext().catch(err => notify(`AI辅助失败: ${err.message}`, true)));

    /* 待审批队列事件代理 */
    document.getElementById('ai-approval-list')?.addEventListener('click', e => {
      const btn = e.target.closest('[data-action]');
      if (!btn) return;
      const cid    = String(btn.dataset.candidateId || '').trim();
      const action = String(btn.dataset.action || '').trim();
      if (action === 'human-approve' && cid) {
        const target = String(btn.dataset.target || 'paper');
        humanApprove(cid, target).catch(err => notify(`批准失败: ${err.message}`, true));
      }
      if (action === 'human-reject' && cid) {
        humanReject(cid).catch(err => notify(`拒绝失败: ${err.message}`, true));
      }
      if (action === 'quick-register' && cid) {
        e.stopPropagation();
        quickRegister(cid).catch(err => notify(`快速注册失败: ${err.message}`, true));
      }
    });

    /* 刷新 */
    document.getElementById('ai-refresh-btn')?.addEventListener('click', () =>
      refreshWorkbench().catch(err => notify(`刷新失败: ${err.message}`, true)));
    document.getElementById('ai-data-refresh-btn')?.addEventListener('click', () =>
      loadDataReadiness().catch(err => notify(`数据诊断失败: ${err.message}`, true)));
    document.getElementById('ai-news-pull-btn')?.addEventListener('click', () =>
      pullNewsForResearch().catch(err => notify(`新闻拉取失败: ${err.message}`, true)));
    document.getElementById('ai-funding-warm-btn')?.addEventListener('click', () =>
      warmFundingForResearch().catch(err => notify(`宏观缓存预热失败: ${err.message}`, true)));
    document.getElementById('ai-live-decision-save-btn')?.addEventListener('click', () =>
      saveLiveDecisionRuntimeConfig().catch(err => notify(`AI实盘决策保存失败: ${err.message}`, true)));
    document.getElementById('ai-planner-symbols')?.addEventListener('change', () =>
      loadDataReadiness().catch(() => {}));
    document.getElementById('run-exchange')?.addEventListener('change', () =>
      loadDataReadiness().catch(() => {}));

    /* 运行研究 */
    document.getElementById('run-selected-btn')?.addEventListener('click', () =>
      runProposal(state.selectedProposalId).catch(err => notify(`运行失败: ${err.message}`, true)));
    document.getElementById('ai-compare-btn')?.addEventListener('click', () => openCompareModal());

    /* 信号刷新 */
    document.getElementById('signal-refresh-btn')?.addEventListener('click', () =>
      loadSignal().catch(err => notify(`信号失败: ${err.message}`, true)));
    document.getElementById('signal-symbol')?.addEventListener('change', (e) =>
      loadSignal(e.target.value).catch(() => {}));

    /* 信号面板折叠 */
    document.getElementById('signal-panel-toggle')?.addEventListener('click', () => {
      state.signalPanelCollapsed = !state.signalPanelCollapsed;
      const body   = document.getElementById('signal-panel-body');
      const toggle = document.getElementById('signal-panel-toggle');
      if (body)   body.style.display   = state.signalPanelCollapsed ? 'none' : '';
      if (toggle) toggle.classList.toggle('collapsed', state.signalPanelCollapsed);
    });

    /* 注册 Modal 关闭 */
    document.getElementById('ai-register-close')?.addEventListener('click', () => {
      document.getElementById('ai-register-modal').style.display = 'none';
    });
    document.getElementById('ai-register-modal')?.addEventListener('click', e => {
      if (e.target === document.getElementById('ai-register-modal'))
        document.getElementById('ai-register-modal').style.display = 'none';
    });
    document.getElementById('ai-candidate-compare-close')?.addEventListener('click', () => {
      const modal = document.getElementById('ai-candidate-compare-modal');
      if (modal) modal.style.display = 'none';
    });
    document.getElementById('ai-candidate-compare-modal')?.addEventListener('click', (e) => {
      const modal = document.getElementById('ai-candidate-compare-modal');
      if (modal && e.target === modal) modal.style.display = 'none';
    });

    /* 研究队列点击代理 */
    document.getElementById('ai-proposal-list')?.addEventListener('click', e => {
      const btn = e.target.closest('[data-action]');
      if (!btn) return;
      const pid    = String(btn.dataset.proposalId || '').trim();
      const action = String(btn.dataset.action || '').trim();

      if (action === 'select-proposal') {
        const item = e.target.closest('.proposal-compact-item');
        const id   = String(item?.dataset?.proposalId || pid || '').trim();
        if (!id) return;
        state.selectedProposalId = id;
        renderProposalList();
        updateRunBtn();
        return;
      }
      if (action === 'run-proposal' && pid) {
        e.stopPropagation();
        state.selectedProposalId = pid;
        runProposal(pid).catch(err => notify(`运行失败: ${err.message}`, true));
        return;
      }
      if (action === 'cancel-proposal' && pid) {
        e.stopPropagation();
        state.selectedProposalId = pid;
        cancelProposal(pid).catch(err => notify(`取消失败: ${err.message}`, true));
        return;
      }
      if (action === 'delete-proposal' && pid) {
        e.stopPropagation();
        deleteProposal(pid).catch(err => notify(`删除失败: ${err.message}`, true));
        return;
      }
      if (action === 'retire-proposal' && pid) {
        e.stopPropagation();
        retireProposal(pid).catch(err => notify(`退役失败: ${err.message}`, true));
      }
    });

    /* 排序 */
    document.getElementById('cand-sort-select')?.addEventListener('change', e => {
      state.sortBy = String(e.target.value || 'score');
      renderCandidateCards();
    });

    /* 类别筛选 */
    document.getElementById('cand-filter-category')?.addEventListener('change', e => {
      state.filterCategory = String(e.target.value || '');
      renderCandidateCards();
    });

    /* 候选卡片点击代理 */
    document.getElementById('ai-candidate-cards')?.addEventListener('click', e => {
      const btn = e.target.closest('[data-action]');
      if (!btn) return;
      const cid    = String(btn.dataset.candidateId || '').trim();
      const action = String(btn.dataset.action || '').trim();

      if (action === 'select-candidate') {
        const card = e.target.closest('.research-candidate-card');
        const id   = String(card?.dataset?.candidateId || cid || '').trim();
        if (!id) return;
        viewCandidate(id).catch(err => notify(`加载详情失败: ${err.message}`, true));
        return;
      }
      if (action === 'view-candidate' && cid) {
        e.stopPropagation();
        viewCandidate(cid).catch(err => notify(`加载详情失败: ${err.message}`, true));
        return;
      }
      if (action === 'toggle-compare' && cid) {
        e.stopPropagation();
        toggleCandidateCompare(cid);
        return;
      }
      if (action === 'open-register' && cid) {
        e.stopPropagation();
        openRegisterModal(cid).catch(err => notify(`打开注册失败: ${err.message}`, true));
      }
    });
  }

  /* ══════════════════════════════════════════════════════════════
     轮询
  ══════════════════════════════════════════════════════════════ */
  function startPolling() {
    clearInterval(state.signalTimer);
    clearInterval(state.refreshTimer);
    clearInterval(state.liveSignalTimer);
    if (!isAiResearchActive() || document.hidden) return;
    loadSignal().catch(() => {});
    loadLiveSignals().catch(() => {});
    state.signalTimer = setInterval(() => {
      if (!isAiResearchActive() || document.hidden) return;
      loadSignal().catch(() => {});
    }, SIGNAL_INTERVAL_MS);
    state.refreshTimer = setInterval(() => {
      if (!isAiResearchActive() || document.hidden) return;
      refreshWorkbench().catch(() => {});
    }, REFRESH_INTERVAL_MS);
    state.liveSignalTimer = setInterval(() => {
      if (!isAiResearchActive() || document.hidden) return;
      loadLiveSignals().catch(() => {});
    }, 30000);
  }

  function stopPolling() {
    clearInterval(state.signalTimer);
    clearInterval(state.refreshTimer);
    clearInterval(state.liveSignalTimer);
    state.signalTimer = null;
    state.refreshTimer = null;
    state.liveSignalTimer = null;
  }

  function isAiResearchActive() {
    const tab = document.getElementById('ai-research');
    return !!(tab && tab.classList.contains('active'));
  }

  function syncHubLayoutHeight() {
    const hub = document.querySelector('#ai-research .ai-hub-layout');
    if (!hub) return;
    if (window.innerWidth <= 1100) {
      hub.style.height = 'auto';
      return;
    }
    const rect = hub.getBoundingClientRect();
    const viewportH = window.innerHeight || document.documentElement.clientHeight || 0;
    const available = Math.max(620, viewportH - rect.top + 130);
    const target = Math.min(1180, available);
    hub.style.height = `${Math.round(target)}px`;
  }

  function bindLayoutSync() {
    window.addEventListener('resize', () => syncHubLayoutHeight());
    const aiTabBtn = document.querySelector('.tab-btn[data-tab="ai-research"]');
    aiTabBtn?.addEventListener('click', () => {
      setTimeout(syncHubLayoutHeight, 0);
      setTimeout(syncHubLayoutHeight, 120);
      startPolling();
    });
    document.querySelectorAll('.tab-btn').forEach((btn) => {
      btn.addEventListener('click', () => {
        setTimeout(() => {
          if (isAiResearchActive()) startPolling();
          else stopPolling();
        }, 0);
      });
    });
    document.addEventListener('visibilitychange', () => {
      if (document.hidden) {
        stopPolling();
      } else if (isAiResearchActive()) {
        startPolling();
      }
    });
  }

  /* ══════════════════════════════════════════════════════════════
     初始化
  ══════════════════════════════════════════════════════════════ */
  function init() {
    if (!document.getElementById('ai-candidate-cards')) return;  // tab 未激活时跳过
    bindLayoutSync();
    syncHubLayoutHeight();
    bindEvents();
    normalizeDomText(document.getElementById('ai-research'));
    refreshWorkbench().catch(err => console.error('AI研究初始化失败:', err));
    if (isAiResearchActive()) startPolling();
  }

  window.addEventListener('load', init);
  window.addEventListener('beforeunload', () => {
    stopPolling();
    Object.values(state.jobPollingTimers).forEach(t => clearInterval(t));
  });

  /* ══════════════════════════════════════════════════════════════
     Phase A — 实时信号面板（30s 轮询）
  ══════════════════════════════════════════════════════════════ */

  async function loadLiveSignals() {
    try {
      const res = await aiApi('/live-signals', { timeoutMs: 20000 });
      renderLiveSignalPanel(res?.items || [], !!res?.ml_model_loaded);
    } catch (e) {
      /* silent — non-critical */
    }
  }

  function renderLiveSignalPanel(items, mlLoaded) {
    const el = document.getElementById('ai-live-signals-panel');
    if (!el) return;

    // ML 未激活提示（仅在有运行候选时显示）
    const mlNote = (items.length > 0 && !mlLoaded)
      ? '<div style="font-size:10px;color:#78350f;background:#451a03;border-radius:4px;padding:2px 6px;margin-bottom:4px;">ML组件未激活（需训练模型），信号仅用 LLM+Factor</div>'
      : '';

    if (!items.length) {
      el.innerHTML = '<div style="font-size:11px;color:#6b7fa0;padding:6px 0;">暂无运行中候选</div>';
      return;
    }

    const dirIcon  = d => d === 'LONG' ? '▲' : d === 'SHORT' ? '▼' : '─';
    const dirColor = d => d === 'LONG' ? '#4ade80' : d === 'SHORT' ? '#f87171' : '#6b7fa0';
    const pct      = v => ((v || 0) * 100).toFixed(0) + '%';

    el.innerHTML = mlNote + items.map(item => {
      const sig  = item.signal;
      if (!sig) return `<div class="live-sig-row"><span style="color:#6b7fa0;font-size:11px">${esc(item.strategy)} — 信号错误</span></div>`;
      const comp = sig.components || {};
      const blockedBadge   = sig.blocked_by_risk
        ? `<span class="live-sig-badge" style="background:#7f1d1d;color:#fca5a5;" title="${esc(sig.risk_reason)}">风控</span>` : '';
      const approvalBadge  = (sig.requires_approval && !sig.blocked_by_risk)
        ? `<span class="live-sig-badge" style="background:#78350f;color:#fcd34d;">待审</span>` : '';

      return `<div class="live-sig-row">
  <div class="live-sig-header">
    <span class="live-sig-name">${esc(item.strategy)}</span>
    <span style="font-size:10px;color:#6b7fa0;">${esc(item.symbol)}</span>
    <span style="font-weight:700;font-size:13px;margin-left:auto;color:${dirColor(sig.direction)}">${dirIcon(sig.direction)} ${sig.direction}</span>
    ${blockedBadge}${approvalBadge}
  </div>
  <div class="live-sig-bars">
    ${['llm', 'ml', 'factor'].map(k => {
      const c = comp[k] || {};
      const mlOffline = k === 'ml' && !mlLoaded;
      return `<span class="live-sig-bar-label"${mlOffline ? ' style="opacity:.45"' : ''}>${k.toUpperCase()}${mlOffline ? '⊘' : ''}</span>`
           + `<span style="color:${mlOffline ? '#6b7fa0' : dirColor(c.direction)};font-size:10px">${mlOffline ? '─' : dirIcon(c.direction || 'FLAT')}</span>`
           + `<span style="font-size:10px;min-width:26px;text-align:right;${mlOffline ? 'opacity:.45' : ''}">${mlOffline ? '--' : pct(c.confidence)}</span>`;
    }).join('')}
    <span style="font-size:10px;color:#6b7fa0;margin-left:4px">合计</span>
    <span style="font-size:11px;font-weight:600">${pct(sig.confidence)}</span>
  </div>
</div>`;
    }).join('');
  }

  /* ══════════════════════════════════════════════════════════════
     Phase B — 快速注册
  ══════════════════════════════════════════════════════════════ */

  async function quickRegister(candidateId, allocationPct = 0.05) {
    if (!confirm(`确认将候选 ${candidateId.slice(0, 8)} 快速注册为纸盘交易，分配 ${(allocationPct * 100).toFixed(0)}% 仓位？`)) return;
    try {
      const result = await aiApi(`/candidates/${encodeURIComponent(candidateId)}/quick-register`, {
        method: 'POST',
        body: JSON.stringify({ allocation_pct: allocationPct }),
        timeoutMs: 30000,
      });
      const stratName = result?.registered_strategy_name || result?.runtime_status || '纸盘';
      notify(`已快速注册为纸盘: ${stratName}（${(allocationPct * 100).toFixed(0)}%）`);
      await refreshWorkbench('', candidateId);
    } catch (err) {
      notify(`快速注册失败: ${err.message}`, true);
    }
  }

  /* ── Phase D — 订单预览 ───────────────────────────────────────────────────── */

  async function showOrderPreview(candidateId) {
    const btn = document.getElementById('btn-order-preview');
    const resultEl = document.getElementById('ai-order-preview-result');
    if (btn) { btn.disabled = true; btn.textContent = '计算中...'; }
    try {
      const r = await aiApi(`/candidates/${encodeURIComponent(candidateId)}/order-preview`, {
        method: 'POST',
        timeoutMs: 15000,
      });
      const dirColor = r.direction === 'LONG' ? '#4ade80' : r.direction === 'SHORT' ? '#f87171' : '#94a3b8';
      const dirIcon  = d => d === 'LONG' ? '▲' : d === 'SHORT' ? '▼' : '─';
      const pct = v => (v * 100).toFixed(1) + '%';
      const comp = r.components || {};
      const blockedHtml = r.blocked_by_risk
        ? `<div style="color:#f87171;margin-top:8px;font-size:12px;">⚠ 风控拦截：${esc(r.risk_reason || '')}</div>` : '';
      const approvalHtml = (r.requires_approval && !r.blocked_by_risk)
        ? `<div style="color:#fcd34d;margin-top:8px;font-size:12px;">⚠ 置信度不足（${pct(r.confidence)}），建议人工确认</div>` : '';

      const html = `
<div style="font-size:13px;line-height:1.6;">
  <div style="font-size:16px;font-weight:700;color:${dirColor};margin-bottom:10px;">
    ${dirIcon(r.direction)} ${r.direction} &nbsp; <span style="font-size:13px;font-weight:500;">置信度 ${pct(r.confidence)}</span>
  </div>
  <table style="width:100%;border-collapse:collapse;font-size:12px;margin-bottom:10px;">
    <tr><td style="color:#7e92b2;padding:2px 0;">标的</td><td style="font-weight:600;">${esc(r.symbol)}</td></tr>
    <tr><td style="color:#7e92b2;padding:2px 0;">建议仓位</td><td>${r.size_usdt.toLocaleString()} USDT（${pct(r.allocation_pct)}）</td></tr>
    <tr><td style="color:#7e92b2;padding:2px 0;">止损</td><td>${pct(r.stop_loss_pct)}</td></tr>
    <tr><td style="color:#7e92b2;padding:2px 0;">止盈</td><td>${pct(r.take_profit_pct)}</td></tr>
  </table>
  <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">信号分解</div>
  <div style="display:flex;gap:6px;margin-bottom:8px;">
    ${['llm', 'ml', 'factor'].map(k => {
      const c = comp[k] || {};
      const dc = c.direction === 'LONG' ? '#4ade80' : c.direction === 'SHORT' ? '#f87171' : '#94a3b8';
      return `<div style="flex:1;background:#0a1520;border:1px solid #1e3a5a;border-radius:6px;padding:6px 8px;font-size:11px;">
        <div style="font-weight:700;text-transform:uppercase;margin-bottom:3px;">${k}</div>
        <div style="color:${dc};font-size:13px;">${dirIcon(c.direction || 'FLAT')} ${c.direction || 'FLAT'}</div>
        <div style="color:#7e92b2;">${pct(c.confidence || 0)}</div>
      </div>`;
    }).join('')}
  </div>
  ${blockedHtml}${approvalHtml}
  <div style="font-size:10px;color:#4a5f7a;margin-top:8px;font-style:italic;">${esc(r.note)}</div>
</div>`;

      if (resultEl) {
        resultEl.innerHTML = html;
        resultEl.style.display = 'block';
      }
    } catch (err) {
      notify(`订单预览失败: ${err.message}`, true);
    } finally {
      if (btn) { btn.disabled = false; btn.textContent = '生成订单预览'; }
    }
  }

  /* 暴露给外部调用（兼容旧代码） */
  window.AI = {
    viewCandidate:   id => viewCandidate(id).catch(err => notify(`加载详情失败: ${err.message}`, true)),
    openRegister:    id => openRegisterModal(id).catch(err => notify(`打开注册失败: ${err.message}`, true)),
    runProposal:     id => runProposal(id).catch(err => notify(`运行失败: ${err.message}`, true)),
    toggleCompare:   id => toggleCandidateCompare(id),
    showComparePanel: () => openCompareModal(),
    refreshWorkbench,
  };
})();
