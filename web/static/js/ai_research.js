(function () {
  'use strict';

  /* ── 轮询间隔 ── */
  const SIGNAL_INTERVAL_MS  = 30000;
  const REFRESH_INTERVAL_MS = 60000;
  const JOB_POLL_MS         = 3000;
  const PREMIUM_SOURCE_LABEL = '高级数据源';
  const DEFAULT_SIGNAL_SYMBOLS = ['BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'SOL/USDT', 'XRP/USDT'];
  const AGENT_STATUS_API = '/ai/autonomous-agent/status';
  const AGENT_JOURNAL_API = '/ai/autonomous-agent/journal';
  const AGENT_START_API = '/ai/autonomous-agent/start';
  const AGENT_STOP_API = '/ai/autonomous-agent/stop';
  const AGENT_RUN_ONCE_API = '/ai/autonomous-agent/run-once';
  const FLOW_HINT_QUICK_PATH = '当前主流程：0) AI判断市场与策略 → 1) 生成研究思路 → 2) 生成提案 → 3) 运行研究 → 4) 注册/部署。也可以直接点击“⚡ one-click 自动研究+部署”。';
  const AI_UI_TIMEZONE = (typeof window !== 'undefined' && window.CTS_UI_TIMEZONE) || 'Asia/Shanghai';
  const AI_UI_TIMEZONE_LABEL = (typeof window !== 'undefined' && window.CTS_UI_TIMEZONE_LABEL) || '上海时间 (UTC+8)';
  const AI_SHARED_POLL_TAB = 'ai-research';
  const AI_SHARED_POLL_GROUP_FALLBACK = 'ai';
  const AI_SHARED_POLL_SYNC_MS = 5000;
  const LIVE_SIGNALS_TIMEOUT_MS = 45000;
  const AGENT_LIVE_SIGNALS_TIMEOUT_MS = 90000;
  const DEFAULT_ACTION_LOCKS = Object.freeze({ generate: false, run: false, oneclick: false, clear: false, exit: false });
  const DELETE_BLOCKED_PROPOSAL_STATUSES = new Set(['paper_running', 'shadow_running', 'live_running']);
  const DELETE_BLOCKED_CANDIDATE_STATUSES = new Set(['paper_running', 'shadow_running', 'live_running']);

  /* 策略类别与颜色 */
  const STRATEGY_CATEGORIES = {
    // 趋势
    MAStrategy: '趋势', EMAStrategy: '趋势', MACDStrategy: '趋势', MACDHistogramStrategy: '趋势',
    ADXTrendStrategy: '趋势', TrendFollowingStrategy: '趋势', AroonStrategy: '趋势',
    // 震荡
    RSIStrategy: '震荡', RSIDivergenceStrategy: '震荡', StochasticStrategy: '震荡',
    BollingerBandsStrategy: '震荡', WilliamsRStrategy: '震荡', CCIStrategy: '震荡', StochRSIStrategy: '震荡',
    // 动量
    MomentumStrategy: '动量', ROCStrategy: '动量', PriceAccelerationStrategy: '动量',
    // 均值回归
    MeanReversionStrategy: '均值回归', BollingerMeanReversionStrategy: '均值回归',
    VWAPReversionStrategy: '均值回归', VWAPStrategy: '均值回归', MeanReversionHalfLifeStrategy: '均值回归',
    // 突破
    BollingerSqueezeStrategy: '突破', DonchianBreakoutStrategy: '突破',
    // 成交量
    MFIStrategy: '成交量', OBVStrategy: '成交量', TradeIntensityStrategy: '成交量',
    // 风险
    ParkinsonVolStrategy: '风险', UlcerIndexStrategy: '风险', VaRBreakoutStrategy: '风险',
    MaxDrawdownStrategy: '风险', SortinoRatioStrategy: '风险',
    // 套利
    PairsTradingStrategy: '套利', HurstExponentStrategy: '套利',
    // 量化
    OrderFlowImbalanceStrategy: '量化', MultiFactorHFStrategy: '量化',
    // ML
    MLXGBoostStrategy: 'ML',
    // 宏观
    MarketSentimentStrategy: '宏观', SocialSentimentStrategy: '宏观', FundFlowStrategy: '宏观', WhaleActivityStrategy: '宏观',
  };
  const CATEGORY_COLORS = {
    '趋势': '#3b82f6', '震荡': '#8b5cf6', '动量': '#20bf78', '均值回归': '#06b6d4',
    '突破': '#f59e0b', '成交量': '#84cc16', '风险': '#f43f5e', '套利': '#e05260',
    '量化': '#a78bfa', 'ML': '#ff6b35', '宏观': '#64748b',
  };

  /* ── 状态 ── */
  const STRATEGY_FAMILIES = {
    MLXGBoostStrategy: 'ml',
    MarketSentimentStrategy: 'ai_openai',
    SocialSentimentStrategy: 'ai_openai',
    FundFlowStrategy: 'ai_openai',
    WhaleActivityStrategy: 'ai_openai',
  };
  const FAMILY_META = {
    traditional: { label: '传统规则', color: '#64748b', accent: 'rgba(100,116,139,.16)' },
    ml: { label: 'ML驱动', color: '#ff6b35', accent: 'rgba(255,107,53,.16)' },
    ai_glm: { label: 'OpenAI/AI驱动', color: '#38bdf8', accent: 'rgba(56,189,248,.16)' },
    ai_openai: { label: 'OpenAI/AI驱动', color: '#38bdf8', accent: 'rgba(56,189,248,.16)' },
  };

  const state = {
    proposals: [],
    candidates: [],
    pendingApprovals: [],   // candidates with human gate
    pendingLlmContext: null, // last AI-generated research context
    pendingMacroContext: null,
    runtimeConfig: null,    // { governance_enabled, decision_mode, trading_mode, ai_live_decision, ai_autonomous_agent }
    runtimeConfigLoaded: false,
    agentStatus: null,
    agentStatusInFlight: null,
    selectedProposalId: '',
    selectedCandidateId: '',
    latestSignals: {},
    liveDecisionActivity: null,
    liveDecisionActivityLastGood: null,
    liveDecisionActivityRetryTimer: null,
    refreshWorkbenchInFlight: null,
    liveSignalsInFlight: null,
    signalTimer: null,
    refreshTimer: null,
    liveSignalTimer: null,
    signalKickoffTimer: null,
    liveSignalKickoffTimer: null,
    signalLoading: false,
    signalPanelCollapsed: false,
    jobPollingTimers: {},   // proposalId → intervalId
    jobPollingConfigs: {},  // proposalId -> { jobId }
    actionLocks: { ...DEFAULT_ACTION_LOCKS },
    sortBy: 'score',        // 'score' | 'sharpe' | 'return' | 'drawdown'
    filterCategory: '',     // '' | '趋势' | '震荡' | ...
    compareCandidateIds: new Set(),
    candidateDetailReqSeq: 0,
    perfHistoryCache: {},
    pollingOwnershipTimer: null,
    autoPlannerRecommendation: null,
    sharedPollingActive: false,
  };
  let initialized = false;
  let initRetryBound = false;

  function scheduleInitRetry() {
    if (typeof window === 'undefined') return;
    window.setTimeout(() => init(), 0);
    window.setTimeout(() => init(), 120);
  }

  function bindInitRetry() {
    if (initRetryBound || typeof document === 'undefined') return;
    initRetryBound = true;
    document.addEventListener('click', (event) => {
      if (event.target instanceof Element && event.target.closest('.tab-btn')) {
        scheduleInitRetry();
      }
    });
    document.addEventListener('visibilitychange', () => {
      if (!document.hidden) scheduleInitRetry();
    });
    window.addEventListener('load', scheduleInitRetry);
  }

  function sharedPollingRoot() {
    return (typeof window !== 'undefined' && window.__ctsSharedPolling) || null;
  }

  function aiSharedPollGroup() {
    const sharedPolling = sharedPollingRoot();
    const group = typeof sharedPolling?.groupForTab === 'function'
      ? sharedPolling.groupForTab(AI_SHARED_POLL_TAB)
      : AI_SHARED_POLL_GROUP_FALLBACK;
    return String(group || AI_SHARED_POLL_GROUP_FALLBACK).trim() || AI_SHARED_POLL_GROUP_FALLBACK;
  }

  function canRunAiSharedPolling({ requireWorkspace = true, requireResearch = false } = {}) {
    if (typeof document === 'undefined' || document.hidden) return false;
    if (requireResearch && !isAiResearchActive()) return false;
    if (requireWorkspace && !isAiWorkspaceActive()) return false;
    const sharedPolling = sharedPollingRoot();
    if (typeof sharedPolling?.canRun === 'function') return !!sharedPolling.canRun(aiSharedPollGroup());
    return true;
  }

  function hasAiPollingWork() {
    return !!(
      state.signalTimer
      || state.refreshTimer
      || state.liveSignalTimer
      || state.liveDecisionActivityRetryTimer
      || Object.keys(state.jobPollingTimers || {}).length
    );
  }

  function emitAiPollingState(reason, active) {
    const nextActive = !!active;
    if (state.sharedPollingActive === nextActive) return;
    state.sharedPollingActive = nextActive;
    if (typeof window === 'undefined' || typeof window.dispatchEvent !== 'function') return;
    try {
      window.dispatchEvent(new CustomEvent('ai-research:polling', {
        detail: {
          reason: String(reason || 'sync'),
          active: nextActive,
          hidden: !!document.hidden,
          workspaceActive: isAiWorkspaceActive(),
          researchActive: isAiResearchActive(),
          group: aiSharedPollGroup(),
        },
      }));
    } catch (err) {
      console.debug('emitAiPollingState failed:', err);
    }
  }

  function stopPollingOwnershipMonitor() {
    if (!state.pollingOwnershipTimer) return;
    clearInterval(state.pollingOwnershipTimer);
    state.pollingOwnershipTimer = null;
  }

  function startPollingOwnershipMonitor() {
    if (state.pollingOwnershipTimer || typeof window === 'undefined') return;
    state.pollingOwnershipTimer = window.setInterval(() => {
      syncPollingState({ immediate: false, reason: 'ownership-heartbeat' });
    }, AI_SHARED_POLL_SYNC_MS);
  }

  function runStateSingleFlight(slot, taskFactory) {
    if (state[slot]) return state[slot];
    const task = Promise.resolve().then(taskFactory).finally(() => {
      if (state[slot] === task) state[slot] = null;
    });
    state[slot] = task;
    return task;
  }

  /* ── 工具函数 ── */
  function esc(v) {
    return String(v ?? '').replace(/[&<>"']/g, m =>
      ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[m]));
  }

  function repairUtf8Mojibake(text) {
    const value = String(text ?? '');
    if (!/[脙脗脜脝脟脨脩脮脰脴脵脷脹脺脻脼脽脿谩芒茫盲氓忙莽猫茅锚毛矛铆卯茂冒帽貌贸么玫枚酶霉煤没眉媒镁每]/.test(value)) {
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
      ['鍔犲叆瀵规瘮', '加入对比'],
      ['瀵规瘮', '对比'],
      ['鏀剁泭', '收益'],
      ['鍥炴挙', '回撤'],
      ['鑳滅巼', '胜率'],
      ['澶忔櫘', '夏普'],
      ['鏂伴椈', '新闻'],
      ['瀹忚', '宏观'],
      ['鍘婚噸闅愯棌', '去重隐藏'],
      ['鍥炴斁妯″紡', '回放模式'],
      ['AI寤鸿', 'AI建议'],
      ['璇︽儏', '详情'],
      ['鎼滅储瑙掕壊', '搜索角色'],
      ['鐮旂┒瀹屾垚锛屼絾鏈€氳繃楠岃瘉', '研究完成，但未通过验证'],
      ['鐮旂┒浠诲姟宸插畬鎴愶紝宸ヤ綔鍙扮姸鎬佸凡鏇存柊', '研究任务已完成，工作台状态已更新'],
      ['杩愯鐮旂┒', '运行研究'],
      ['鐮旂┒鐩爣锛堝彲鐣欑┖鑷姩鐢熸垚锛?', '研究目标（可留空自动生成）'],
      ['鍏堣 AI 鍒ゆ柇褰撳墠甯傚満鐘舵€佷笌閫傞厤绛栫暐', '先让 AI 判断当前市场状态与适配策略'],
      ['椤甸潰鏃跺尯锛氫笂娴锋椂闂?(UTC+8)', '页面时区：上海时间 (UTC+8)'],
      ['鍊欓€夊洖濉?', '候选回填'],
      ['璇ユ潯鐩敱鍊欓€夌粨鏋滃洖濉?', '该条目由候选结果回填'],
      ['棰勭儹鐮旂┒缂撳瓨', '预热研究缓存'],
      ['鐮旂┒缂撳瓨宸查鐑?', '研究缓存已预热'],
      ['鍗曟璇曡窇宸茶Е鍙?', '单次试跑已触发'],
      ['宸叉湁涓€杞湪杩愯锛屾墜鍔ㄨЕ鍙戝凡鎺掗槦', '已有一轮在运行，手动触发已排队'],
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

  function setAIContextButtonState(mode = 'idle') {
    const btn = document.getElementById('ai-context-btn');
    if (!btn) return;
    if (mode === 'working') {
      btn.textContent = '1) 生成研究思路中...';
      btn.disabled = true;
      btn.style.color = '';
      return;
    }
    if (mode === 'ready') {
      btn.textContent = '1) 研究思路已生成';
      btn.disabled = false;
      btn.style.color = '#20bf78';
      return;
    }
    btn.textContent = '1) 生成研究思路';
    btn.disabled = false;
    btn.style.color = '';
  }

  function providerDisplayName(provider) {
    const value = String(provider || '').trim().toLowerCase();
    if (value === 'codex' || value === 'openai') return 'OpenAI';
    if (value === 'glm') return 'GLM';
    if (value === 'claude') return 'Claude';
    return String(provider || '-');
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

  function plannerNumberInput(id, fallback, minValue = null, maxValue = null) {
    const raw = Number(document.getElementById(id)?.value || fallback);
    let value = Number.isFinite(raw) ? raw : fallback;
    if (Number.isFinite(minValue)) value = Math.max(minValue, value);
    if (Number.isFinite(maxValue)) value = Math.min(maxValue, value);
    return value;
  }

  function pendingStrategyDraftCount() {
    return toArray(state.pendingLlmContext?.proposed_strategy_changes).filter(item => item && typeof item === 'object').length;
  }

  function resolvePlannerResearchMode() {
    const selected = String(document.getElementById('ai-planner-research-mode')?.value || 'auto').trim() || 'auto';
    if (selected !== 'auto') return selected;
    const pendingDrafts = pendingStrategyDraftCount();
    if (pendingDrafts > 0) return 'autonomous_draft';
    if (state.pendingLlmContext && Object.keys(state.pendingLlmContext || {}).length) return 'hybrid';
    return 'template';
  }

  function buildPlannerConstraints() {
    return {
      max_templates: plannerNumberInput('ai-planner-max-templates', 5, 1, 12),
      research_mode: resolvePlannerResearchMode(),
      max_strategy_drafts: plannerNumberInput('ai-planner-max-drafts', 4, 1, 12),
      max_backtest_runs: plannerNumberInput('ai-planner-max-backtests', 80, 8, 500),
      exploration_bias: plannerNumberInput('ai-planner-exploration-bias', 0.45, 0.0, 1.0),
    };
  }

  const PLANNER_REGIME_LABELS = {
    mixed: '混合行情',
    trend_up: '上涨趋势',
    trend_down: '下跌趋势',
    mean_reversion: '震荡回归',
    breakout: '突破行情',
    stat_arb: '统计套利',
    news_event: '新闻事件',
  };

  function updatePlannerModeHint() {
    const hintEl = document.getElementById('ai-planner-mode-hint');
    if (!hintEl) return;
    const requested = String(document.getElementById('ai-planner-research-mode')?.value || 'auto').trim() || 'auto';
    const effective = buildPlannerConstraints();
    const requestedText = requested === 'auto' ? '自动判断' : researchModeText(requested);
    const effectiveText = researchModeText(effective.research_mode || 'template');
    const draftCount = pendingStrategyDraftCount();
    const draftText = draftCount > 0 ? `当前已挂起 ${draftCount} 个 AI 草案，生成提案时会一起进入搜索。` : '当前还没有 AI 草案，系统会先从模板和市场上下文起步。';
    hintEl.textContent = `当前选择：${requestedText}；实际生成：${effectiveText}；模板上限 ${effective.max_templates}，草案预算 ${effective.max_strategy_drafts}，回测预算 ${effective.max_backtest_runs}，探索强度 ${(effective.exploration_bias * 100).toFixed(0)}%。${draftText}`;
  }

  function setPlannerFieldValue(id, value) {
    const el = document.getElementById(id);
    if (!el) return;
    if (el instanceof HTMLSelectElement) {
      const target = String(value || '').trim();
      const hasOption = [...el.options].some((opt) => String(opt.value || '').trim() === target);
      el.value = hasOption ? target : (el.options[0]?.value || '');
    } else {
      el.value = value;
    }
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
  }

  function plannerListOrFallback(id, fallback = []) {
    const parsed = csvInput(id)
      .map((item) => String(item || '').trim())
      .filter(Boolean);
    const base = parsed.length
      ? parsed
      : toArray(fallback)
        .map((item) => String(item || '').trim())
        .filter(Boolean);
    return Array.from(new Set(base));
  }

  function deriveAutoResearchTimeframes(baseTimeframe) {
    const presets = {
      '1m': ['1m', '5m', '15m'],
      '5m': ['5m', '15m', '1h'],
      '15m': ['5m', '15m', '1h', '4h'],
      '1h': ['15m', '1h', '4h'],
      '4h': ['1h', '4h', '1d'],
      '1d': ['4h', '1d'],
    };
    return Array.from(new Set(presets[String(baseTimeframe || '5m').trim().toLowerCase()] || ['5m', '15m', '1h']));
  }

  function buildAiPlannerWorkbenchProfile() {
    const symbols = plannerListOrFallback('ai-planner-symbols', [getCurrentResearchSymbol() || 'BTC/USDT', 'ETH/USDT'])
      .map((item) => String(item || '').trim().toUpperCase())
      .filter(Boolean);
    const primarySymbol = symbols[0] || getCurrentResearchSymbol() || 'BTC/USDT';
    const timeframes = plannerListOrFallback('ai-planner-timeframes', deriveAutoResearchTimeframes('5m'));
    return {
      exchange: getCurrentResearchExchange() || 'binance',
      primary_symbol: primarySymbol,
      universe_symbols: symbols.length ? symbols : [primarySymbol],
      timeframe: timeframes[0] || '5m',
      lookback: 1200,
      exclude_retired: true,
      horizon: 'short_intraday',
    };
  }

  function workbenchProfileQuery(profile) {
    const params = new URLSearchParams();
    Object.entries(profile || {}).forEach(([key, value]) => {
      if (Array.isArray(value)) params.set(key, value.join(','));
      else if (value != null) params.set(key, String(value));
    });
    return params.toString();
  }

  function plannerProfileCacheKey(profile) {
    return JSON.stringify({
      exchange: String(profile?.exchange || 'binance').trim().toLowerCase(),
      primary_symbol: String(profile?.primary_symbol || 'BTC/USDT').trim().toUpperCase(),
      universe_symbols: toArray(profile?.universe_symbols).map((item) => String(item || '').trim().toUpperCase()),
      timeframe: String(profile?.timeframe || '5m').trim().toLowerCase(),
      lookback: Number(profile?.lookback || 1200),
      horizon: String(profile?.horizon || 'short_intraday').trim(),
    });
  }

  function fallbackPlannerRegime(marketContext) {
    const newsEvents = Number(marketContext?.news?.events_count || 0);
    const direction = String(marketContext?.sentiment || 'FLAT').trim().toUpperCase();
    if (newsEvents >= 5) return 'news_event';
    if (direction === 'LONG') return 'trend_up';
    if (direction === 'SHORT') return 'trend_down';
    return 'mixed';
  }

  function preferredFamiliesForPlanner(regime, directionBias = 'neutral') {
    if (regime === 'breakout') return ['突破', '趋势跟随', '动量'];
    if (regime === 'news_event') return ['事件驱动', '突破', '轻仓趋势跟随'];
    if (regime === 'trend_up' || regime === 'trend_bullish' || directionBias === 'bullish') return ['趋势跟随', '动量突破', '低杠杆顺势'];
    if (regime === 'trend_down' || regime === 'trend_bearish' || directionBias === 'bearish') return ['防守型均值回归', '反弹做空', '事件驱动快进快出'];
    if (regime === 'high_risk_chop') return ['均值回归', '轻仓观察', '防守型套利'];
    if (regime === 'low_info_range') return ['均值回归', '震荡反转', '轻仓套利'];
    if (regime === 'event_driven_mixed') return ['事件驱动', '突破', '轻仓趋势跟随'];
    if (regime === 'mean_reversion') return ['均值回归', '震荡反转', '轻仓套利'];
    return ['均值回归', '轻仓观察', '多策略对比'];
  }

  function buildFallbackAutoResearchBrief(profile, marketContext = {}) {
    const direction = String(marketContext?.sentiment || 'FLAT').trim().toUpperCase();
    const directionBias = direction === 'LONG' ? 'bullish' : direction === 'SHORT' ? 'bearish' : 'neutral';
    const plannerRegime = fallbackPlannerRegime(marketContext);
    const regimeLabel = PLANNER_REGIME_LABELS[plannerRegime] || '混合行情';
    const symbols = plannerListOrFallback('ai-planner-symbols', profile?.universe_symbols || [profile?.primary_symbol || 'BTC/USDT'])
      .map((item) => String(item || '').trim().toUpperCase())
      .filter(Boolean)
      .slice(0, 6);
    const timeframes = plannerListOrFallback('ai-planner-timeframes', deriveAutoResearchTimeframes(profile?.timeframe || '5m'));
    const preferred = preferredFamiliesForPlanner(plannerRegime, directionBias);
    const confidencePct = Math.round(Number(marketContext?.confidence || 0) * 100);
    const goal = `围绕 ${symbols.slice(0, 3).join(' / ')} 在${regimeLabel}环境下，优先研究 ${preferred.join(' / ')} 策略，验证触发条件、失效条件和风控边界。`;
    const thesis = [
      `市场当前更接近${regimeLabel}，先比较最匹配的策略族表现。`,
      confidencePct > 0 ? `现有方向信号置信度约 ${confidencePct}% ，需要验证是否能转化为稳定收益。` : '当前方向判断不够强，适合先做多策略对比而不是过早部署。',
    ];
    const riskNotes = [];
    if (Number(marketContext?.microstructure?.spread_bps || 0) >= 8) riskNotes.push('盘口点差偏高，避免过度依赖高频入场。');
    if (Number(marketContext?.news?.events_count || 0) >= 5) riskNotes.push('新闻事件密集，注意策略失效速度和波动放大。');
    if (!riskNotes.length) riskNotes.push('先通过样本外回测和成交质量验证，再决定是否部署。');
    const nextSteps = [
      `优先比较 ${preferred.slice(0, 2).join(' / ')} 两类策略的稳定性。`,
      '确认触发条件、失效条件与风险预算是否清晰。',
      '先看候选验证结果，再决定是否进入注册或部署。',
    ];
    return {
      headline: regimeLabel,
      goal,
      planner_regime: plannerRegime,
      market_regime: regimeLabel,
      direction_bias: directionBias,
      symbols,
      timeframes,
      preferred_strategy_families: preferred,
      thesis,
      risk_notes: riskNotes,
      next_steps: nextSteps,
      prompt_context: [
        `研究任务：${goal}`,
        `市场状态：${regimeLabel} / ${directionBias === 'bullish' ? '看多' : directionBias === 'bearish' ? '看空' : '中性'}`,
        `关注标的：${symbols.join(' / ')}`,
        `观察周期：${timeframes.join(' / ')}`,
        `优先策略：${preferred.join(' / ')}`,
        `研究观察：${thesis.join('；')}`,
        `风险提示：${riskNotes.join('；')}`,
        `下一步：${nextSteps.join('；')}`,
      ].join('\n'),
      source_label: 'fallback',
    };
  }

  function getWorkbenchAutoResearchResult() {
    const workbench = (typeof window !== 'undefined' && window.workbenchState) || null;
    const recommendation = workbench?.recommendations || null;
    if (!recommendation) return null;
    const action = toArray(recommendation?.action_items).find((item) => String(item?.kind || '').trim() === 'ai_prefill') || null;
    const brief = action?.params?.brief || recommendation?.ai_brief || null;
    if (!brief) return null;
    return {
      profile: workbench?.profile || null,
      overview: workbench?.overview || null,
      recommendation,
      action,
      brief,
      source: 'workbench',
    };
  }

  function applyAutoResearchBrief(brief, options = {}) {
    if (!brief || typeof brief !== 'object') return '';
    const source = String(options.source || brief.source_label || 'workbench').trim();
    const goal = String(brief.prompt_context || brief.goal || '').trim();
    const regime = String(brief.planner_regime || 'mixed').trim();
    const symbols = toArray(brief.symbols).map((item) => String(item || '').trim().toUpperCase()).filter(Boolean);
    const timeframes = toArray(brief.timeframes).map((item) => String(item || '').trim()).filter(Boolean);
    const headline = String(brief.headline || brief.market_regime || PLANNER_REGIME_LABELS[regime] || '').trim();

    if (goal) setPlannerFieldValue('ai-planner-goal', goal);
    setPlannerFieldValue('ai-planner-regime', regime);
    if (symbols.length) setPlannerFieldValue('ai-planner-symbols', symbols.join(', '));
    if (timeframes.length) setPlannerFieldValue('ai-planner-timeframes', timeframes.join(', '));

    const plannerNotesEl = document.getElementById('ai-planner-notes');
    if (plannerNotesEl) {
      const notes = [];
      if (headline) notes.push(`市场判断：${headline}`);
      if (toArray(brief.preferred_strategy_families).length) notes.push(`优先策略：${toArray(brief.preferred_strategy_families).join(' / ')}`);
      if (toArray(brief.risk_notes).length) notes.push(`风险提示：${toArray(brief.risk_notes).slice(0, 2).join('；')}`);
      const sourceLabel = source === 'fallback' ? '当前市场快照' : '研究工作台';
      plannerNotesEl.innerHTML = `<div style="font-size:11px;color:#7dd3fc;margin-bottom:3px;">已按${esc(sourceLabel)}自动生成研究目标${notes.length ? ` · ${esc(notes.join(' · '))}` : ''}</div>`;
    }

    const marketHintEl = document.getElementById('ai-market-context-hint');
    if (marketHintEl) {
      const preferredText = toArray(brief.preferred_strategy_families).slice(0, 3).join(' / ');
      const sourceLabel = source === 'fallback' ? 'AI 快照判断' : 'AI 综合判断';
      marketHintEl.textContent = normalizeUiText(`${sourceLabel}：${headline || PLANNER_REGIME_LABELS[regime] || '混合行情'}${preferredText ? ` · 优先 ${preferredText}` : ''}`);
    }

    updatePlannerModeHint();
    clearOneClickFeedback();
    return goal;
  }

  async function loadAutoResearchRecommendation({ forceRefresh = false } = {}) {
    const profile = buildAiPlannerWorkbenchProfile();
    const profileKey = plannerProfileCacheKey(profile);
    if (!forceRefresh) {
      const workbenchResult = getWorkbenchAutoResearchResult();
      const workbenchProfileKey = plannerProfileCacheKey(workbenchResult?.profile || profile);
      if (workbenchResult?.brief && workbenchProfileKey === profileKey) return workbenchResult;
      if (state.autoPlannerRecommendation?.brief && state.autoPlannerRecommendation?.cache_key === profileKey) {
        return state.autoPlannerRecommendation;
      }
    }
    let overview = null;
    try {
      overview = await rootApi(`/research/workbench/overview?${workbenchProfileQuery(profile)}`, { timeoutMs: 90000 });
      const recommendation = await rootApi('/research/workbench/recommendations', {
        method: 'POST',
        body: JSON.stringify({
          profile,
          overview,
          modules: overview?.modules || {},
        }),
        timeoutMs: 30000,
      });
      const action = toArray(recommendation?.action_items).find((item) => String(item?.kind || '').trim() === 'ai_prefill') || null;
      const brief = action?.params?.brief || recommendation?.ai_brief || null;
      if (brief) {
        state.autoPlannerRecommendation = {
          cache_key: profileKey,
          profile,
          overview,
          recommendation,
          action,
          brief,
          source: 'workbench',
        };
        return state.autoPlannerRecommendation;
      }
    } catch (err) {
      console.debug('loadAutoResearchRecommendation(workbench) failed:', err);
    }

    const marketContext = await _collectLiveMarketContext(profile.primary_symbol).catch(() => ({}));
    const brief = buildFallbackAutoResearchBrief(profile, marketContext);
    if (marketContext && Object.keys(marketContext || {}).length) {
      state.pendingMacroContext = marketContext;
    }
    state.autoPlannerRecommendation = {
      cache_key: profileKey,
      profile,
      overview,
      recommendation: null,
      action: {
        kind: 'ai_prefill',
        params: {
          goal: brief.prompt_context,
          regime: brief.planner_regime,
          symbols: brief.symbols,
          timeframes: brief.timeframes,
          brief,
        },
      },
      brief,
      source: 'fallback',
      marketContext,
    };
    return state.autoPlannerRecommendation;
  }

  async function ensureAutoPlannerGoal({ forceRefresh = false, silent = false } = {}) {
    const current = String(document.getElementById('ai-planner-goal')?.value || '').trim();
    if (current.length >= 8 && !forceRefresh) return current;
    const marketHintEl = document.getElementById('ai-market-context-hint');
    if (marketHintEl) {
      marketHintEl.textContent = normalizeUiText('AI 正在判断当前市场状态、适配策略和研究目标...');
    }
    const recommendation = await loadAutoResearchRecommendation({ forceRefresh });
    const goal = applyAutoResearchBrief(recommendation?.brief || recommendation?.action?.params?.brief || null, {
      source: recommendation?.source || 'workbench',
    });
    const nextGoal = String(goal || document.getElementById('ai-planner-goal')?.value || '').trim();
    if (nextGoal.length < 8) {
      throw new Error('AI 未能自动生成可用的研究目标，请稍后重试');
    }
    if (!silent) {
      notify(forceRefresh ? '已重新根据当前市场生成研究目标' : '已根据当前市场自动生成研究目标');
    }
    return nextGoal;
  }

  function uniqueTextItems(values, limit = 5) {
    const seen = new Set();
    return toArray(values)
      .map(value => normalizeUiText(String(value ?? '').trim()))
      .filter(Boolean)
      .filter((value) => {
        const key = value.toLowerCase();
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      })
      .slice(0, Math.max(1, limit));
  }

  function parseAllocationPercentInput(rawValue, fallbackPercent = 5) {
    const parsed = Number.parseFloat(String(rawValue ?? '').trim());
    const normalized = Number.isFinite(parsed) ? parsed : fallbackPercent;
    return Math.max(1, Math.min(100, normalized));
  }

  function summarizeOneClickPayload(payload) {
    const symbols = toArray(payload?.symbols).map(item => String(item || '').trim()).filter(Boolean);
    const timeframes = toArray(payload?.timeframes).map(item => String(item || '').trim()).filter(Boolean);
    const symbolText = symbols.length
      ? `${symbols.slice(0, 2).join(', ')}${symbols.length > 2 ? ` 等 ${symbols.length} 个币种` : ''}`
      : '未选择币种';
    const timeframeText = timeframes.length
      ? `${timeframes.slice(0, 3).join(', ')}${timeframes.length > 3 ? ' 等更多周期' : ''}`
      : '未选择周期';
    const exchangeText = String(payload?.exchange || '--').trim() || '--';
    const days = Number(payload?.days || 0);
    const daysText = Number.isFinite(days) && days > 0 ? `${Math.round(days)} 天` : '--';
    const allocationPct = Number(payload?.allocation_pct || 0);
    const allocationText = Number.isFinite(allocationPct) && allocationPct > 0
      ? `${Math.round(allocationPct * 100)}%`
      : '--';
    return normalizeUiText(`当前设置：${symbolText} · ${timeframeText} · ${exchangeText.toUpperCase()} · ${daysText} · 仓位 ${allocationText}`);
  }

  function renderOneClickFeedback(feedback) {
    const box = document.getElementById('ai-oneclick-feedback');
    if (!box) return;
    if (!feedback) {
      box.innerHTML = '';
      box.className = 'ai-oneclick-feedback is-hidden';
      box.removeAttribute('data-tone');
      box.removeAttribute('role');
      return;
    }
    const tone = ['working', 'success', 'warn', 'error'].includes(String(feedback.tone || ''))
      ? String(feedback.tone || '')
      : 'warn';
    const badge = normalizeUiText(feedback.badge || '');
    const title = normalizeUiText(feedback.title || '一键自动研究状态');
    const summary = normalizeUiText(feedback.summary || '');
    const details = uniqueTextItems(feedback.details || [], 6);
    const suggestions = uniqueTextItems(feedback.suggestions || [], 5);
    box.className = 'ai-oneclick-feedback';
    box.dataset.tone = tone;
    box.setAttribute('role', tone === 'error' ? 'alert' : 'status');
    box.innerHTML = `
      <div class="ai-oneclick-feedback-head">
        <div>
          <div class="ai-oneclick-feedback-title">${esc(title)}</div>
          ${summary ? `<div class="ai-oneclick-feedback-summary">${esc(summary)}</div>` : ''}
        </div>
        ${badge ? `<span class="ai-oneclick-feedback-badge">${esc(badge)}</span>` : ''}
      </div>
      ${details.length ? `
        <div class="ai-oneclick-feedback-section">
          <div class="ai-oneclick-feedback-label">这次发生了什么</div>
          <ul class="ai-oneclick-feedback-list">
            ${details.map(item => `<li>${esc(item)}</li>`).join('')}
          </ul>
        </div>
      ` : ''}
      ${suggestions.length ? `
        <div class="ai-oneclick-feedback-section">
          <div class="ai-oneclick-feedback-label">建议下一步</div>
          <ul class="ai-oneclick-feedback-list">
            ${suggestions.map(item => `<li>${esc(item)}</li>`).join('')}
          </ul>
        </div>
      ` : ''}
    `;
    normalizeDomText(box);
  }

  function clearOneClickFeedback() {
    renderOneClickFeedback(null);
  }

  function buildOneClickFailureFeedback(err, payload) {
    const message = normalizeUiText(err?.message || '一键自动研究执行失败，请稍后重试。');
    const lower = message.toLowerCase();
    const days = Number(payload?.days || 0);
    const suggestions = [];
    const isDataError = (
      message.includes('没有足够数据')
      || message.includes('最小样本')
      || message.includes('历史数据')
      || message.includes('秒级数据')
      || lower.includes('insufficient')
      || lower.includes('not enough data')
      || lower.includes('sample')
      || lower.includes('parquet')
      || lower.includes('data')
    );
    const isTimeoutError = (
      err?.name === 'AbortError'
      || message.includes('超时')
      || lower.includes('timeout')
      || lower.includes('timed out')
    );
    const isNoStrategy = (
      lower.includes('strategy templates')
      || lower.includes('executable strategy')
      || lower.includes('no executable')
    );
    let tone = 'error';
    let badge = '执行失败';
    let title = '这次 one-click 自动研究没有跑起来';

    if (message.includes('研究目标太短') || message.includes('至少8个字符') || lower.includes('goal')) {
      tone = 'warn';
      badge = '目标过短';
      title = '研究目标太短，one-click 还不能开始';
      suggestions.push('把研究目标写得更具体一些，例如“寻找 BTC 在趋势行情中的高胜率 15m 策略”。');
      suggestions.push('最好同时写清楚币种、行情类型和想验证的思路。');
    } else if (isDataError) {
      tone = 'warn';
      badge = '数据不足';
      title = '这次 one-click 没跑起来：研究窗口里没有足够历史数据';
      if (Number.isFinite(days) && days > 0 && days < 365) {
        suggestions.push(`把“回测天数”从 ${Math.round(days)} 调大后重试，短周期研究通常更依赖更长历史。`);
      } else {
        suggestions.push('缩短研究周期范围，或降低最小样本要求后重试。');
      }
      suggestions.push('先只保留数据更完整的周期，例如 15m、1h。');
      suggestions.push('如果涉及子分钟周期，请先在“数据管理”补齐秒级数据。');
    } else if (isTimeoutError) {
      tone = 'warn';
      badge = '请求超时';
      title = '一键自动研究仍在后台执行';
      suggestions.push('研究任务一般需要数分钟，请先在“研究任务队列”观察状态。');
      suggestions.push('下一次可减少币种或周期数量，让结果更快返回。');
    } else if (
      lower.includes('research completed without deployable candidate')
      || lower.includes('proposal_status=rejected')
      || lower.includes('completed_without_deployable_candidate')
    ) {
      tone = 'warn';
      badge = '研究已完成';
      title = '研究已经跑完，但这次没有生成可部署候选';
      suggestions.push('这更像是验证层筛掉了当前结果，不是接口本身崩了。');
      suggestions.push('先查看提案和候选详情里的验证原因，再决定是扩大样本、调目标还是换研究方式。');
    } else if (isNoStrategy) {
      tone = 'warn';
      badge = '策略不可执行';
      title = '研究任务已创建，但策略集合不可执行';
      suggestions.push('先改成更通用的研究目标，避免过窄导致模板被全部过滤。');
      suggestions.push('尝试把研究方式切为“模板 + AI草案”或“只跑模板”再重试。');
    } else if (
      lower.includes('network')
      || lower.includes('failed to fetch')
      || message.includes('502')
      || message.includes('503')
      || message.includes('500')
      || lower.includes('service unavailable')
    ) {
      badge = '服务异常';
      title = '请求已经发出，但服务端暂时没有正常返回';
      suggestions.push('稍后重试一次，观察是否为临时服务波动。');
      suggestions.push('如果连续失败，先检查后端日志或接口状态。');
    } else {
      suggestions.push('先点“3) 运行研究”单独验证数据与参数，再决定是否一键部署。');
      suggestions.push('如果问题稳定复现，再查看后端日志定位具体报错来源。');
    }

    if (!suggestions.some(item => item.includes('3) 运行研究'))) {
      suggestions.push('也可以先点“3) 运行研究”单独验证，再决定是否一键部署。');
    }

    return {
      tone,
      badge,
      title,
      summary: summarizeOneClickPayload(payload),
      details: [message],
      suggestions,
    };
  }

  function buildOneClickSuccessFeedback(result, payload) {
    const proposalId = String(result?.proposal_id || result?.run?.proposal?.proposal_id || '').trim();
    const candidateId = String(result?.candidate_id || result?.run?.candidate?.candidate_id || '').trim();
    const outcome = String(result?.outcome || '').trim();
    const proposalStatus = String(result?.proposal_status || result?.run?.proposal?.status || '').trim();
    const proposalReason = String(result?.proposal_reason || '').trim();
    const runtimeStatus = String(result?.runtime_status || result?.deploy?.runtime_status || '').trim();
    const action = String(result?.deploy?.action || '').trim();
    const currentTradingMode = String(result?.current_trading_mode || '').trim();
    const resolvedTarget = String(result?.target || '').trim();
    const manualActionRequired = Boolean(result?.manual_action_required);
    const candidateStatus = String(result?.run?.candidate?.status || '').trim();
    const validationSummary = result?.run?.candidate?.validation_summary || {};
    const reasonInputs = [
      proposalReason,
      ...toArray(result?.run?.proposal?.validation_summary?.reasons),
      ...toArray(validationSummary?.reasons),
      ...toArray(result?.deploy?.reasons),
    ];
    if (validationSummary?.summary) reasonInputs.push(validationSummary.summary);
    if (validationSummary?.decision_reason) reasonInputs.push(validationSummary.decision_reason);
    const reasons = uniqueTextItems(reasonInputs, 4);

    const details = [];
    if (proposalId) details.push(`提案 ID：${proposalId}`);
    if (candidateId) details.push(`候选 ID：${candidateId}`);
    if (runtimeStatus) details.push(`运行状态：${normalizeUiText(statusText(runtimeStatus))}`);
    if (action) details.push(`部署动作：${action}`);

    let tone = 'success';
    let badge = '已完成';
    let title = '一键自动研究已完成';
    const suggestions = ['右侧候选详情与运行状态已经刷新，可以继续查看表现和验证结果。'];

    if (candidateId && outcome === 'completed_without_compatible_runtime_target' && manualActionRequired) {
      tone = 'warn';
      badge = '需要手动部署';
      title = '研究已完成，但当前模式下未自动部署';
      if (currentTradingMode) details.push(`当前系统模式：${normalizeUiText(currentTradingMode)}`);
      if (resolvedTarget) details.push(`自动推荐目标：${normalizeUiText(resolvedTarget)}`);
      reasons.forEach((reason) => details.push(`阻塞原因：${reason}`));
      suggestions.length = 0;
      suggestions.push('候选已经生成，但自动推荐目标与当前系统模式不兼容，所以本次没有自动注册。');
      suggestions.push('如果要继续实盘链路，可以在右侧候选详情里手动选择 live_candidate。');
      if (resolvedTarget === 'paper') {
        suggestions.push('如果要按 AI 推荐目标部署，需要先将系统切换到 paper 模式。');
      }
    } else if (
      (outcome.startsWith('completed_without') || !candidateId)
      && (proposalStatus === 'rejected' || proposalReason || reasons.length)
    ) {
      tone = 'warn';
      badge = '研究已完成';
      title = '研究已完成，但未生成可部署候选';
      details.push(`提案状态：${normalizeUiText(statusText(proposalStatus || 'rejected'))}`);
      reasons.forEach((reason) => details.push(`未通过原因：${reason}`));
      suggestions.length = 0;
      suggestions.push('这次更像是研究结果被验证层筛掉，而不是 one-click 执行失败。');
      suggestions.push('先看右侧候选详情和验证原因，再决定是否扩大样本、改周期或调整研究目标。');
    } else if (candidateStatus === 'rejected' || candidateStatus === 'failed' || reasons.length) {
      tone = 'warn';
      badge = '已完成待处理';
      title = '研究已完成，但候选还没有达到部署条件';
      reasons.forEach((reason) => details.push(`未通过原因：${reason}`));
      suggestions.length = 0;
      suggestions.push('先查看右侧候选详情里的验证原因，再决定是否扩大样本或调整研究方式。');
      suggestions.push('如果只是数据窗口太短，优先调大“回测天数”后再重试。');
    } else if (!runtimeStatus && !action) {
      badge = '研究完成';
      title = '研究已经跑完，部署结果还需要进一步确认';
      suggestions.push('如果还没有进入运行态，可以继续在右侧执行注册/部署确认。');
    }

    return {
      tone,
      badge,
      title,
      summary: summarizeOneClickPayload(payload),
      details,
      suggestions,
    };
  }

  function extractOneClickJobStatus(snapshot) {
    const jobStatus = String(snapshot?.job_status || snapshot?.job?.status || '').trim();
    const proposalStatus = String(snapshot?.proposal_status || snapshot?.proposal?.status || '').trim();
    const proposalReason = String(snapshot?.proposal_reason || snapshot?.job?.result?.proposal_reason || '').trim();
    const progress = snapshot?.job?.progress || {};
    const candidateId = String(
      snapshot?.job?.result?.candidate?.candidate_id
      || snapshot?.job?.result?.candidate_id
      || snapshot?.candidate_id
      || '',
    ).trim();
    return { jobStatus, proposalStatus, proposalReason, progress, candidateId };
  }

  async function pollOneClickJob(proposalId, jobId, btn, payload) {
    const MAX_WAIT_MS = 20 * 60 * 1000;
    const POLL_INTERVAL_MS = 5000;
    const startAt = Date.now();
    let latest = { proposalReason: '', candidateId: '', proposalStatus: '' };
    while ((Date.now() - startAt) < MAX_WAIT_MS) {
      await new Promise(resolve => setTimeout(resolve, POLL_INTERVAL_MS));
      let statusSnapshot = null;
      try {
        statusSnapshot = await aiApi(`/proposals/${encodeURIComponent(proposalId)}/job-status`, {
          timeoutMs: 10000,
        });
      } catch (networkErr) {
        const message = String(networkErr?.message || '').toLowerCase();
        const isTransient = (
          message.includes('timeout')
          || message.includes('failed to fetch')
          || message.includes('network')
          || message.includes('无法连接')
        );
        if (isTransient) continue;
        throw networkErr;
      }
      const parsed = extractOneClickJobStatus(statusSnapshot || {});
      latest = {
        proposalReason: parsed.proposalReason,
        candidateId: parsed.candidateId,
        proposalStatus: parsed.proposalStatus,
      };
      const elapsedSec = Math.max(1, Math.round((Date.now() - startAt) / 1000));
      if (btn) btn.textContent = `研究中 ${elapsedSec}s...`;

      if (parsed.jobStatus === 'completed') return latest;
      if (parsed.jobStatus === 'failed' || parsed.jobStatus === 'cancelled') {
        const reason = parsed.proposalReason || String(statusSnapshot?.error || statusSnapshot?.job?.error || '').trim() || '未知原因';
        throw new Error(`研究任务${parsed.jobStatus === 'failed' ? '失败' : '已取消'}: ${reason}`);
      }

      const progressMessage = normalizeUiText(
        parsed.progress?.message
        || parsed.progress?.phase
        || `后台任务运行中（已 ${elapsedSec}s）`,
      );
      const progressTimeframes = toArray(parsed.progress?.timeframes).map(tf => String(tf || '').trim()).filter(Boolean);
      renderOneClickFeedback({
        tone: 'working',
        badge: `阶段 2/3 · ${elapsedSec}s`,
        title: '研究任务运行中',
        summary: summarizeOneClickPayload(payload),
        details: [
          `任务 ID：${jobId}`,
          progressMessage,
          progressTimeframes.length ? `时间框架：${progressTimeframes.join(', ')}` : '',
        ].filter(Boolean),
        suggestions: ['可切到“研究任务队列”继续操作，后台会持续执行。'],
      });
    }
    throw new Error('研究任务等待超时（20 分钟），请在研究任务队列里继续查看进度。');
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
      timeZone: AI_UI_TIMEZONE,
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

  function currentTradingMode() {
    return String((state.runtimeConfig && state.runtimeConfig.trading_mode) || '').trim().toLowerCase();
  }

  function getLiveDecisionRuntimeConfig() {
    return (state.runtimeConfig && state.runtimeConfig.ai_live_decision) || null;
  }

  function safeJsonClone(value, fallback = null) {
    try {
      return JSON.parse(JSON.stringify(value));
    } catch (_) {
      return fallback;
    }
  }

  function getWorkbenchSnapshot() {
    return {
      proposals: safeJsonClone(state.proposals, []),
      candidates: safeJsonClone(state.candidates, []),
      pendingApprovals: safeJsonClone(state.pendingApprovals, []),
      runtimeConfig: safeJsonClone(state.runtimeConfig, null),
      runtimeConfigLoaded: !!state.runtimeConfigLoaded,
      agentStatus: safeJsonClone(state.agentStatus, null),
      selectedProposalId: String(state.selectedProposalId || ''),
      selectedCandidateId: String(state.selectedCandidateId || ''),
      latestSignals: safeJsonClone(state.latestSignals, {}),
      actionLocks: safeJsonClone(state.actionLocks, {}),
      sortBy: String(state.sortBy || 'score'),
      filterCategory: String(state.filterCategory || ''),
    };
  }

  function emitWorkbenchState(reason, extra = {}) {
    if (typeof window === 'undefined' || typeof window.dispatchEvent !== 'function') return;
    try {
      window.dispatchEvent(new CustomEvent('ai-research:state', {
        detail: {
          reason: String(reason || 'update'),
          snapshot: getWorkbenchSnapshot(),
          ...extra,
        },
      }));
    } catch (err) {
      console.debug('emitWorkbenchState failed:', err);
    }
  }

  function autonomousAgentRuntimeText(agentCfg = {}, agentStatus = {}) {
    const running = !!agentStatus?.running;
    const enabled = !!agentCfg?.enabled;
    if (running) return { value: '运行中', cls: 'is-on' };
    if (enabled && agentStatus?.last_run_at) return { value: '已停止', cls: 'is-warn' };
    if (enabled) return { value: '待启动', cls: 'is-warn' };
    return { value: '已关闭', cls: '' };
  }

  function renderRuntimeSummary(options = {}) {
    const root = document.getElementById('ai-runtime-summary');
    if (!root) return;
    const cfg = state.runtimeConfig || {};
    const liveCfg = cfg.ai_live_decision || {};
    const agentCfg = cfg.ai_autonomous_agent || {};
    const governanceOn = !!cfg.governance_enabled;
    const tradingMode = String(cfg.trading_mode || '--');
    const decisionMode = String(cfg.decision_mode || '--');
    const liveEnabled = !!liveCfg.enabled;
    const liveMode = String(liveCfg.mode || 'shadow');
    const agentRuntime = autonomousAgentRuntimeText(agentCfg, state.agentStatus || {});
    const chips = [
      { label: '人工确认', value: governanceOn ? '开启' : '关闭', cls: governanceOn ? 'is-warn' : 'is-on' },
      { label: '交易模式', value: tradingModeLabel(tradingMode), cls: tradingMode === 'live' ? 'is-warn' : 'is-on' },
      { label: '执行裁决', value: decisionModeLabel(decisionMode), cls: decisionMode === 'enforce' ? 'is-warn' : '' },
      { label: '下单前AI复核', value: liveEnabled ? `已启用（${decisionModeLabel(liveMode)}）` : '已关闭', cls: liveEnabled ? 'is-on' : '' },
      { label: '自动交易代理', value: agentRuntime.value, cls: agentRuntime.cls },
      { label: '页面时区', value: AI_UI_TIMEZONE_LABEL, cls: '' },
    ];
    const visibleChips = chips.filter((chip) => chip.label !== '\u6267\u884c\u88c1\u51b3');
    root.innerHTML = visibleChips
      .map((chip) => `<span class="ai-runtime-chip ${chip.cls}">${esc(chip.label)}：${esc(chip.value)}</span>`)
      .join('');
    if (!options.silent) emitWorkbenchState('runtime-summary');
  }

  function resolveLiveDecisionProviderState(cfg) {
    const providers = (cfg && typeof cfg.providers === 'object' && cfg.providers) || {};
    const requestedProvider = String(cfg?.provider_requested || cfg?.provider || 'codex').trim() || 'codex';
    const configuredProvider = String(cfg?.provider || requestedProvider).trim() || requestedProvider;
    const availableProviders = Object.entries(providers)
      .filter(([, meta]) => !!meta?.available)
      .map(([name]) => String(name));
    const provider = availableProviders.includes(configuredProvider)
      ? configuredProvider
      : (availableProviders.includes(requestedProvider)
        ? requestedProvider
        : (availableProviders.includes('codex') ? 'codex' : (availableProviders[0] || configuredProvider)));
    const providerMeta = providers[provider] || {};
    return {
      provider,
      providerMeta,
      requestedProvider,
      fallbackUsed: !!cfg?.provider_fallback || provider !== requestedProvider,
      model: String(cfg?.model || providerMeta.default_model || ''),
    };
  }

  function renderLiveDecisionProviderOptions(cfg, activeProvider) {
    const providerEl = document.getElementById('ai-live-decision-provider');
    if (!providerEl) return;
    const providers = (cfg && typeof cfg.providers === 'object' && cfg.providers) || {};
    Array.from(providerEl.options).forEach((option) => {
      const key = String(option.value || '').trim();
      const meta = providers[key] || {};
      const available = !!meta.available;
      option.textContent = `${providerDisplayName(key)}${available ? '' : '（未配置）'}`;
      option.disabled = !available && key !== activeProvider;
    });
    providerEl.dataset.previousProvider = String(activeProvider || '');
  }

  function renderLiveDecisionEffectiveSummary(summary = {}) {
    const scopeEl = document.getElementById('ai-live-decision-effective-scope');
    const modeEl = document.getElementById('ai-live-decision-effective-mode');
    if (!scopeEl || !modeEl) return;
    scopeEl.textContent = String(summary.scope || '未加载');
    modeEl.textContent = String(summary.mode || '未加载');
  }

  function renderLiveDecisionActivitySummary(summary = {}) {
    const hitEl = document.getElementById('ai-live-decision-hit-summary');
    const hitDetailEl = document.getElementById('ai-live-decision-hit-detail');
    const lastEl = document.getElementById('ai-live-decision-last-hit');
    const lastDetailEl = document.getElementById('ai-live-decision-last-hit-detail');
    if (!hitEl || !hitDetailEl || !lastEl || !lastDetailEl) return;

    const hitCount = Math.max(0, Number(summary?.hit_count || 0));
    const blockCount = Math.max(0, Number(summary?.block_count || 0));
    const reduceOnlyCount = Math.max(0, Number(summary?.reduce_only_count || 0));
    const bypassCount = Math.max(0, Number(summary?.bypass_count || 0));
    const lastHit = summary?.last_hit && typeof summary.last_hit === 'object' ? summary.last_hit : null;

    hitEl.textContent = hitCount > 0 ? `已命中 ${hitCount} 次` : '暂未命中';
    hitDetailEl.textContent = hitCount > 0
      ? `直接拦截 ${blockCount} 次 · 减仓拒绝 ${reduceOnlyCount} 次`
      : String(summary?.scope_note || '仅统计策略库/候选执行链');
    if (bypassCount > 0) {
      hitDetailEl.textContent += ` · 自动交易直连 ${bypassCount} 次`;
    }

    if (lastHit) {
      const status = String(lastHit.status || '').trim().toLowerCase();
      const symbolLabel = String(lastHit.symbol || '--').split('/')[0] || '--';
      lastEl.textContent = `${symbolLabel} ${status === 'ai_reduce_only_rejected' ? '减仓拒绝' : '直接拦截'}`;
      lastDetailEl.textContent = `${String(lastHit.strategy || '--')} · ${fmtTs(lastHit.ts || summary?.last_updated_at)}`;
    } else {
      lastEl.textContent = '最近暂无拦截';
      lastDetailEl.textContent = 'AI自动交易不计入这里';
    }
  }

  function buildLiveDecisionEffectiveSummary(cfg, providerState, overrides = {}) {
    const enabled = overrides.enabled != null ? !!overrides.enabled : !!cfg?.enabled;
    const mode = String(overrides.mode || cfg?.mode || 'shadow');
    const provider = String(overrides.provider || providerState?.provider || cfg?.provider || 'codex');
    const model = String(overrides.model || providerState?.model || cfg?.model || '').trim() || 'default';
    const tradingMode = String((state.runtimeConfig && state.runtimeConfig.trading_mode) || '--').trim().toLowerCase();
    const applyInPaper = overrides.applyInPaper != null ? !!overrides.applyInPaper : !!cfg?.apply_in_paper;

    let scopeText = '未启用，当前策略下单不做AI复核';
    if (enabled && applyInPaper) {
      scopeText = '纸盘/实盘策略执行链（AI自动交易不经过此处）';
    } else if (enabled && tradingMode === 'live') {
      scopeText = '当前实盘策略执行链（AI自动交易不经过此处）';
    } else if (enabled) {
      scopeText = '仅实盘策略执行链（纸盘先观察，AI自动交易不经过此处）';
    }

    return {
      scope: scopeText,
      mode: enabled ? `${decisionModeLabel(mode)} / ${providerDisplayName(provider)} / ${model}` : '关闭',
    };
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
      renderLiveDecisionEffectiveSummary({ scope: '未加载', mode: '未加载' });
      return;
    }
    const providerState = resolveLiveDecisionProviderState(cfg);
    renderLiveDecisionProviderOptions(cfg, providerState.provider);
    enabledEl.checked = !!cfg.enabled;
    modeEl.value = String(cfg.mode || 'shadow');
    providerEl.value = providerState.provider;
    modelEl.value = providerState.model;
    providerEl.dataset.previousProvider = providerState.provider;
    const selectedProvider = providerState.provider;
    const available = !!providerState.providerMeta.available;
    const modeText = decisionModeLabel(String(cfg.mode || 'shadow'));
    const providerText = `${providerDisplayName(selectedProvider)}/${String(providerState.model || '--')}`;
    const fallbackText = providerState.fallbackUsed
      ? ` | 已从 ${providerDisplayName(providerState.requestedProvider)} 自动切换`
      : '';
    statusEl.textContent = `${cfg.enabled ? '已启用' : '未启用'} | ${modeText} | ${providerText} | ${available ? 'key就绪' : 'key缺失'}${fallbackText}`;
    statusEl.style.color = available ? '#9fb1c9' : '#f0b429';
    renderLiveDecisionEffectiveSummary(buildLiveDecisionEffectiveSummary(cfg, providerState));
  }

  function previewLiveDecisionProviderSelection() {
    const cfg = getLiveDecisionRuntimeConfig() || {};
    const enabledEl = document.getElementById('ai-live-decision-enabled');
    const modeEl = document.getElementById('ai-live-decision-mode');
    const providerEl = document.getElementById('ai-live-decision-provider');
    const modelEl = document.getElementById('ai-live-decision-model');
    const statusEl = document.getElementById('ai-live-decision-status');
    if (!providerEl || !modelEl || !statusEl) return;
    const providers = (cfg && typeof cfg.providers === 'object' && cfg.providers) || {};
    const nextProvider = String(providerEl.value || 'codex').trim() || 'codex';
    const prevProvider = String(providerEl.dataset.previousProvider || '').trim();
    const prevDefaultModel = String(providers[prevProvider]?.default_model || '');
    const nextDefaultModel = String(providers[nextProvider]?.default_model || '');
    const currentModel = String(modelEl.value || '').trim();
    if (!currentModel || (prevDefaultModel && currentModel === prevDefaultModel)) {
      modelEl.value = nextDefaultModel;
    }
    providerEl.dataset.previousProvider = nextProvider;
    const available = !!providers[nextProvider]?.available;
    const previewModel = String(modelEl.value || nextDefaultModel || '--');
    const providerText = `${providerDisplayName(nextProvider)}/${previewModel}`;
    statusEl.textContent = `${enabledEl?.checked ? '已启用' : '未启用'} | ${decisionModeLabel(String(modeEl?.value || cfg.mode || 'shadow'))} | ${providerText} | ${available ? 'key就绪' : 'key缺失'}`;
    statusEl.style.color = available ? '#9fb1c9' : '#f0b429';
    renderLiveDecisionEffectiveSummary(buildLiveDecisionEffectiveSummary(cfg, {
      provider: nextProvider,
      model: previewModel,
    }, {
      enabled: !!enabledEl?.checked,
      mode: String(modeEl?.value || cfg.mode || 'shadow'),
      provider: nextProvider,
      model: previewModel,
    }));
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
      provider: String(providerEl.value || 'codex'),
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
      renderRuntimeSummary();
      renderLiveDecisionRuntimeConfig();
      await loadLiveDecisionActivitySummary();
      notify('下单前AI复核配置已更新');
    } catch (err) {
      notify(`下单前AI复核配置保存失败: ${err.message}`, true);
    } finally {
      if (btn) { btn.disabled = false; btn.textContent = '保存复核配置'; }
    }
  }

  async function loadLiveDecisionActivitySummary() {
    try {
      clearTimeout(state.liveDecisionActivityRetryTimer);
      state.liveDecisionActivityRetryTimer = null;
      const res = await aiApi('/runtime-config/live-decision/summary', { timeoutMs: 20000 });
      state.liveDecisionActivity = res || {};
      renderLiveDecisionActivitySummary(state.liveDecisionActivity);
    } catch (err) {
      state.liveDecisionActivity = {
        hit_count: 0,
        block_count: 0,
        reduce_only_count: 0,
        last_hit: null,
        scope_note: `摘要加载失败: ${err.message}`,
      };
      renderLiveDecisionActivitySummary(state.liveDecisionActivity);
      clearTimeout(state.liveDecisionActivityRetryTimer);
      state.liveDecisionActivityRetryTimer = setTimeout(() => {
        state.liveDecisionActivityRetryTimer = null;
        if (isAiResearchActive()) {
          loadLiveDecisionActivitySummary().catch(() => {});
        }
      }, 3000);
    }
  }

  function renderLiveDecisionActivitySummary(summary = {}) {
    const hitEl = document.getElementById('ai-live-decision-hit-summary');
    const hitDetailEl = document.getElementById('ai-live-decision-hit-detail');
    const lastEl = document.getElementById('ai-live-decision-last-hit');
    const lastDetailEl = document.getElementById('ai-live-decision-last-hit-detail');
    if (!hitEl || !hitDetailEl || !lastEl || !lastDetailEl) return;

    const hitCount = Math.max(0, Number(summary?.hit_count || 0));
    const blockCount = Math.max(0, Number(summary?.block_count || 0));
    const reduceOnlyCount = Math.max(0, Number(summary?.reduce_only_count || 0));
    const bypassCount = Math.max(0, Number(summary?.bypass_count || 0));
    const lastHit = summary?.last_hit && typeof summary.last_hit === 'object' ? summary.last_hit : null;
    const refreshState = String(summary?.refresh_state || '').trim().toLowerCase();
    const refreshNote = refreshState === 'stale'
      ? `沿用上次快照，刷新失败：${String(summary?.refresh_error || '稍后自动重试')}`
      : refreshState === 'retrying'
        ? `摘要刷新中：${String(summary?.refresh_error || '稍后自动重试')}`
        : '';
    const baseScopeNote = String(summary?.scope_note || '仅统计策略库/候选执行链');
    const detailParts = [];

    hitEl.textContent = hitCount > 0 ? `已命中 ${hitCount} 次` : '暂未命中';
    if (refreshState === 'retrying' && hitCount === 0) {
      detailParts.push(refreshNote || '摘要刷新中，稍后自动重试');
    } else {
      if (hitCount > 0) {
        detailParts.push(`直接拦截 ${blockCount} 次 · 减仓拒绝 ${reduceOnlyCount} 次`);
      } else {
        detailParts.push(baseScopeNote);
      }
      if (bypassCount > 0) detailParts.push(`自动交易直连 ${bypassCount} 次`);
      if (refreshNote) detailParts.push(refreshNote);
    }
    hitDetailEl.textContent = detailParts.join(' · ');

    if (lastHit) {
      const status = String(lastHit.status || '').trim().toLowerCase();
      const symbolLabel = String(lastHit.symbol || '--').split('/')[0] || '--';
      lastEl.textContent = `${symbolLabel} ${status === 'ai_reduce_only_rejected' ? '减仓拒绝' : '直接拦截'}`;
      lastDetailEl.textContent = `${String(lastHit.strategy || '--')} · ${fmtTs(lastHit.ts || summary?.last_updated_at)}${refreshState === 'stale' ? ' · 非实时快照' : ''}`;
    } else {
      lastEl.textContent = refreshState === 'retrying' ? '摘要刷新中' : '最近暂无拦截';
      lastDetailEl.textContent = refreshState === 'retrying'
        ? '正在重试拉取复核摘要'
        : `AI自动交易不计入这里${refreshState === 'stale' ? ' · 当前显示为上次快照' : ''}`;
    }
  }

  async function loadLiveDecisionActivitySummary() {
    try {
      clearTimeout(state.liveDecisionActivityRetryTimer);
      state.liveDecisionActivityRetryTimer = null;
      const res = await aiApi('/runtime-config/live-decision/summary', { timeoutMs: 20000 });
      state.liveDecisionActivity = {
        ...(res || {}),
        refresh_state: 'fresh',
        refresh_error: '',
      };
      state.liveDecisionActivityLastGood = state.liveDecisionActivity;
      renderLiveDecisionActivitySummary(state.liveDecisionActivity);
    } catch (err) {
      const errorMessage = String(err?.message || '绋嶅悗鑷姩閲嶈瘯');
      state.liveDecisionActivity = state.liveDecisionActivityLastGood && typeof state.liveDecisionActivityLastGood === 'object'
        ? {
            ...state.liveDecisionActivityLastGood,
            refresh_state: 'stale',
            refresh_error: errorMessage,
          }
        : {
            hit_count: 0,
            block_count: 0,
            reduce_only_count: 0,
            bypass_count: 0,
            last_hit: null,
            scope_note: '摘要刷新中，稍后自动重试',
            refresh_state: 'retrying',
            refresh_error: errorMessage,
          };
      renderLiveDecisionActivitySummary(state.liveDecisionActivity);
      clearTimeout(state.liveDecisionActivityRetryTimer);
      state.liveDecisionActivityRetryTimer = setTimeout(() => {
        state.liveDecisionActivityRetryTimer = null;
        if (isAiResearchActive()) {
          loadLiveDecisionActivitySummary().catch(() => {});
        }
      }, 3000);
    }
  }

  function canRegisterCandidate(cand) {
    if (!cand) return false;
    if (governanceEnabled()) return false;
    const status = String(cand?.status || '').trim();
    return !new Set(['retired', 'paper_running', 'shadow_running', 'live_candidate', 'live_running']).has(status);
  }

  function canActivateLiveCandidate(cand) {
    if (!cand) return false;
    const status = String(cand?.status || '').trim();
    return new Set(['paper_running', 'live_candidate']).has(status);
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

  /* 晋级建议文本 */
  function promotionText(d) {
    return {
      paper: '先以纸盘模拟（低风险试跑）',
      shadow: '影子模式追踪，观察真实行情',
      live_candidate: '条件成熟，可申请实盘候选',
      reject: '暂不建议注册，需进一步优化',
    }[String(d || '')] || (d ? String(d) : '待定');
  }

  /* 市场状态中文 */
  function regimeText(r) {
    return {
      mixed: '混合行情',
      trend_up: '上涨趋势',
      trend_down: '下跌趋势',
      mean_reversion: '震荡回归',
      breakout: '突破行情',
      stat_arb: '统计套利',
      news_event: '新闻事件',
    }[String(r || '')] || String(r || '--');
  }

  /* 分数对应颜色等级 */
  function researchModeText(mode) {
    return {
      template: '模板研究',
      hybrid: '混合研究',
      autonomous_draft: '开放式草案研究',
      template_seed: '模板种子',
      hybrid_seed: '混合种子',
      dsl_seed: '开放式草案',
    }[String(mode || '').trim()] || String(mode || '--');
  }

  function searchDraftStatusMeta(status) {
    const key = String(status || 'seed').trim() || 'seed';
    return {
      champion: { label: 'Champion', fg: '#20bf78', bg: '#143224', border: '#245b42' },
      challenger: { label: 'Challenger', fg: '#7dd3fc', bg: '#10263a', border: '#294d69' },
      accepted: { label: 'Accepted', fg: '#c2d0e8', bg: '#1d2b3d', border: '#32475f' },
      rejected: { label: 'Rejected', fg: '#f0b429', bg: '#35210f', border: '#6c431b' },
      seed: { label: 'Seed', fg: '#9fb1c9', bg: '#1a2436', border: '#32475f' },
    }[key] || { label: key || 'Seed', fg: '#9fb1c9', bg: '#1a2436', border: '#32475f' };
  }

  function candidateSearchRoleMeta(role) {
    const key = String(role || '').trim();
    if (!key) return null;
    return {
      champion: { label: 'Champion', fg: '#20bf78', bg: '#143224', border: '#245b42' },
      challenger: { label: 'Challenger', fg: '#7dd3fc', bg: '#10263a', border: '#294d69' },
    }[key] || { label: key, fg: '#9fb1c9', bg: '#1a2436', border: '#32475f' };
  }

  function renderStrategyDraftSummary(drafts, limit = 3) {
    const rows = toArray(drafts).slice(0, limit);
    if (!rows.length) {
      return '<div style="font-size:12px;color:#6b7fa0;">暂无 AI 策略草案</div>';
    }
    return rows.map((draft, index) => {
      const features = toArray(draft?.features).slice(0, 4).join(' / ') || '--';
      const entry = toArray(draft?.entry_logic).slice(0, 2).join('；') || '--';
      const exit = toArray(draft?.exit_logic).slice(0, 2).join('；') || '--';
      const risk = toArray(draft?.risk_logic).slice(0, 2).join('；') || '--';
      const templateHint = String(draft?.template_hint || '').trim();
      const statusMeta = searchDraftStatusMeta(draft?.selection_status);
      const generation = Number(draft?.generation);
      const generationText = Number.isFinite(generation) ? `G${Math.max(0, Math.round(generation))}` : 'G0';
      const mutationNotes = joinText(toArray(draft?.mutation_notes).slice(0, 2));
      const critique = joinText(toArray(draft?.critique).slice(0, 2));
      const heuristic = Number(draft?.heuristic_score);
      const novelty = Number(draft?.novelty_score);
      const params = draft?.params && Object.keys(draft.params).length
        ? Object.entries(draft.params).slice(0, 3).map(([k, v]) => `${k}=${String(v)}`).join('  ')
        : '--';
      return `<div style="padding:8px 10px;background:#141f2f;border-radius:6px;margin-bottom:8px;">
        <div style="display:flex;justify-content:space-between;gap:8px;align-items:center;margin-bottom:4px;">
          <span style="font-size:12px;font-weight:700;color:#c2d0e8;">${esc(draft?.name || `Draft ${index + 1}`)}</span>
          <span style="font-size:10px;color:#7e92b2;">${esc(researchModeText(draft?.mode))}</span>
        </div>
        <div style="display:flex;gap:4px;flex-wrap:wrap;margin-bottom:4px;">
          <span style="font-size:10px;padding:1px 6px;border-radius:999px;background:${statusMeta.bg};color:${statusMeta.fg};border:1px solid ${statusMeta.border};">${esc(statusMeta.label)}</span>
          <span style="font-size:10px;padding:1px 6px;border-radius:999px;background:#1a2436;color:#9fb1c9;border:1px solid #32475f;">${esc(generationText)}</span>
          ${Number.isFinite(heuristic) && heuristic > 0 ? `<span style="font-size:10px;padding:1px 6px;border-radius:999px;background:#1d2b3d;color:#c2d0e8;border:1px solid #32475f;">H ${esc(fmtNum(heuristic, 1))}</span>` : ''}
          ${Number.isFinite(novelty) && novelty > 0 ? `<span style="font-size:10px;padding:1px 6px;border-radius:999px;background:#10263a;color:#7dd3fc;border:1px solid #294d69;">N ${(novelty * 100).toFixed(0)}%</span>` : ''}
        </div>
        ${mutationNotes !== '--' ? `<div style="font-size:10px;color:#7dd3fc;margin-bottom:4px;">Mutation: ${esc(mutationNotes)}</div>` : ''}
        ${critique !== '--' ? `<div style="font-size:10px;color:#f0b429;margin-bottom:4px;">Critique: ${esc(critique)}</div>` : ''}
        <div style="font-size:11px;color:#9fb1c9;margin-bottom:4px;">模板种子：${esc(templateHint || '--')}</div>
        <div style="font-size:11px;color:#b7c7e2;line-height:1.55;">${esc(draft?.thesis || draft?.rationale || '--')}</div>
        <div style="font-size:10px;color:#7e92b2;margin-top:6px;">特征：${esc(features)}</div>
        <div style="font-size:10px;color:#7e92b2;margin-top:3px;">入场：${esc(entry)}</div>
        <div style="font-size:10px;color:#7e92b2;margin-top:3px;">出场：${esc(exit)}</div>
        <div style="font-size:10px;color:#7e92b2;margin-top:3px;">风控：${esc(risk)}</div>
        <div style="font-size:10px;color:#7e92b2;margin-top:3px;font-family:monospace;">参数：${esc(params)}</div>
      </div>`;
    }).join('');
  }

  function formatAutonomyBudget(budget) {
    const info = budget && typeof budget === 'object' ? budget : {};
    const maxTemplates = Number(info?.max_templates || 0);
    const maxDrafts = Number(info?.max_strategy_drafts || 0);
    const maxBacktests = Number(info?.max_backtest_runs || 0);
    const explorationBias = Number(info?.exploration_bias);
    const rows = [
      `模板 ${maxTemplates > 0 ? Math.round(maxTemplates) : '--'}`,
      `草案 ${maxDrafts > 0 ? Math.round(maxDrafts) : '--'}`,
      `回测 ${maxBacktests > 0 ? Math.round(maxBacktests) : '--'}`,
    ];
    if (Number.isFinite(explorationBias)) {
      rows.push(`探索 ${(explorationBias * 100).toFixed(0)}%`);
    }
    return rows.join(' | ');
  }

  function renderResearchLineage(lineage) {
    const data = lineage && typeof lineage === 'object' ? lineage : null;
    if (!data) {
      return '<div style="font-size:12px;color:#6b7fa0;">暂无 lineage 信息</div>';
    }
    const lineageId = String(data?.lineage_id || '').trim();
    const parentProposalId = String(data?.parent_proposal_id || '').trim();
    const parentCandidateId = String(data?.parent_candidate_id || '').trim();
    const mutationNotes = joinText(data?.mutation_notes);
    const generation = Number(data?.generation);
    const generationText = Number.isFinite(generation) ? String(Math.max(0, Math.round(generation))) : '--';
    if (!lineageId && !parentProposalId && !parentCandidateId && mutationNotes === '--') {
      return '<div style="font-size:12px;color:#6b7fa0;">暂无 lineage 信息</div>';
    }
    return `<div style="font-size:12px;color:#b7c7e2;background:#141f2f;border-radius:6px;padding:8px;line-height:1.6;">
      <div>Lineage ID：${esc(lineageId || '--')}</div>
      <div style="margin-top:3px;">父方案：${esc(parentProposalId || '--')}</div>
      <div style="margin-top:3px;">父候选：${esc(parentCandidateId || '--')}</div>
      <div style="margin-top:3px;">代际：${esc(generationText)}</div>
      <div style="margin-top:3px;">变异记录：${esc(mutationNotes)}</div>
    </div>`;
  }

  function renderSearchLoopSummary(summary, drafts = []) {
    const info = summary && typeof summary === 'object' ? summary : null;
    if (!info) {
      return '<div style="font-size:12px;color:#6b7fa0;">暂无 Search Loop 信息</div>';
    }
    const evaluations = toArray(info?.draft_evaluations);
    const evaluated = Number(info?.evaluated_drafts || 0);
    const accepted = Number(info?.accepted_drafts || 0);
    const rejected = Number(info?.rejected_drafts || 0);
    const championId = String(info?.champion_draft_id || '').trim();
    const challengers = toArray(info?.challenger_draft_ids);
    const notes = joinText(toArray(info?.notes));
    const draftNameMap = new Map();

    toArray(drafts).forEach(draft => {
      const draftId = String(draft?.draft_id || '').trim();
      const draftName = String(draft?.name || '').trim();
      if (draftId) draftNameMap.set(draftId, draftName || draftId);
    });
    evaluations.forEach(row => {
      const draftId = String(row?.draft_id || '').trim();
      const draftName = String(row?.name || '').trim();
      if (draftId && !draftNameMap.has(draftId)) {
        draftNameMap.set(draftId, draftName || draftId);
      }
    });

    const championLabel = championId ? (draftNameMap.get(championId) || championId) : '--';
    const reasonEntries = info?.rejected_reason_counts && typeof info.rejected_reason_counts === 'object'
      ? Object.entries(info.rejected_reason_counts)
        .filter(([, count]) => Number(count) > 0)
        .sort((a, b) => Number(b[1]) - Number(a[1]))
      : [];
    const rejectedPreview = evaluations
      .filter(row => String(row?.selection_status || '').trim() === 'rejected')
      .slice(0, 3)
      .map(row => {
        const heuristic = Number(row?.heuristic_score);
        const novelty = Number(row?.novelty_score);
        const mutation = joinText(toArray(row?.mutation_notes).slice(0, 1));
        const critique = joinText(toArray(row?.critique).slice(0, 1));
        return `<div style="padding:6px 8px;background:#141f2f;border-radius:6px;margin-top:6px;">
          <div style="display:flex;justify-content:space-between;gap:8px;align-items:center;">
            <span style="font-size:11px;font-weight:700;color:#c2d0e8;">${esc(row?.name || row?.draft_id || '--')}</span>
            <span style="font-size:10px;color:#f0b429;">${esc(row?.rejection_reason || 'rejected')}</span>
          </div>
          <div style="font-size:10px;color:#7e92b2;margin-top:3px;">
            ${esc(String(row?.draft_id || '--'))} · G${Math.max(0, Math.round(Number(row?.generation || 0) || 0))}
            ${Number.isFinite(heuristic) && heuristic > 0 ? ` · H ${esc(fmtNum(heuristic, 1))}` : ''}
            ${Number.isFinite(novelty) && novelty >= 0 ? ` · N ${(novelty * 100).toFixed(0)}%` : ''}
          </div>
          ${mutation !== '--' ? `<div style="font-size:10px;color:#7dd3fc;margin-top:3px;">Mutation: ${esc(mutation)}</div>` : ''}
          ${critique !== '--' ? `<div style="font-size:10px;color:#f0b429;margin-top:3px;">Critique: ${esc(critique)}</div>` : ''}
        </div>`;
      }).join('');

    const statusText = info?.loop_enabled ? 'Enabled' : 'Disabled';
    const statusColor = info?.loop_enabled ? '#20bf78' : '#7e92b2';
    return `<div style="background:#141f2f;border-radius:6px;padding:8px;">
      <div style="display:flex;justify-content:space-between;gap:8px;align-items:center;margin-bottom:8px;">
        <div style="font-size:12px;color:#c2d0e8;font-weight:700;">Search Loop</div>
        <span style="font-size:10px;padding:1px 6px;border-radius:999px;background:${info?.loop_enabled ? '#143224' : '#1a2436'};color:${statusColor};border:1px solid ${info?.loop_enabled ? '#245b42' : '#32475f'};">${statusText}</span>
      </div>
      <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:6px;">
        <div style="text-align:center;padding:8px;background:#1a2436;border-radius:4px;">
          <div style="font-size:10px;color:#6b7fa0;">Evaluated</div>
          <div style="font-size:13px;font-weight:700;color:#c2d0e8;">${evaluated}</div>
        </div>
        <div style="text-align:center;padding:8px;background:#1a2436;border-radius:4px;">
          <div style="font-size:10px;color:#6b7fa0;">Accepted</div>
          <div style="font-size:13px;font-weight:700;color:#20bf78;">${accepted}</div>
        </div>
        <div style="text-align:center;padding:8px;background:#1a2436;border-radius:4px;">
          <div style="font-size:10px;color:#6b7fa0;">Rejected</div>
          <div style="font-size:13px;font-weight:700;color:#f0b429;">${rejected}</div>
        </div>
        <div style="text-align:center;padding:8px;background:#1a2436;border-radius:4px;">
          <div style="font-size:10px;color:#6b7fa0;">Challengers</div>
          <div style="font-size:13px;font-weight:700;color:#7dd3fc;">${challengers.length}</div>
        </div>
      </div>
      <div style="font-size:12px;color:#b7c7e2;line-height:1.6;margin-top:8px;">
        <div>Champion: <span style="color:#20bf78;font-weight:700;">${esc(championLabel)}</span>${championId ? `<span style="color:#7e92b2;"> (${esc(championId)})</span>` : ''}</div>
        ${notes !== '--' ? `<div style="margin-top:4px;color:#7e92b2;">Notes: ${esc(notes)}</div>` : ''}
      </div>
      ${reasonEntries.length ? `<div style="display:flex;gap:4px;flex-wrap:wrap;margin-top:8px;">
        ${reasonEntries.map(([reason, count]) => `<span style="font-size:10px;padding:1px 6px;border-radius:999px;background:#35210f;color:#f0b429;border:1px solid #6c431b;">${esc(reason)} x${esc(String(count))}</span>`).join('')}
      </div>` : ''}
      ${rejectedPreview ? `<div style="margin-top:8px;">
        <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:2px;">Rejected Drafts</div>
        ${rejectedPreview}
      </div>` : ''}
    </div>`;
  }

  function renderAutonomyContext(proposal, fallback = {}) {
    const proposalInfo = proposal && typeof proposal === 'object' ? proposal : {};
    const llmResearchOutput = proposalInfo?.metadata?.llm_research_output || fallback?.llmResearchOutput || {};
    const researchMode = String(proposalInfo?.research_mode || fallback?.researchMode || 'template').trim() || 'template';
    const strategyDrafts = toArray(proposalInfo?.strategy_drafts).length
      ? toArray(proposalInfo?.strategy_drafts)
      : toArray(fallback?.strategyDrafts);
    const searchBudget = proposalInfo?.search_budget || fallback?.searchBudget || {};
    const searchSummary = proposalInfo?.search_summary
      || proposalInfo?.metadata?.search_summary
      || fallback?.searchSummary
      || {};
    const lineage = proposalInfo?.lineage || fallback?.lineage || null;
    const hypothesis = firstMeaningfulText(llmResearchOutput?.hypothesis, proposalResearchThesis(proposalInfo), fallback?.thesis);
    const experimentPlan = joinText(llmResearchOutput?.experiment_plan);
    const evidenceRefs = joinText(llmResearchOutput?.evidence_refs);
    const uncertainty = String(llmResearchOutput?.uncertainty || '').trim();
    const budgetNotes = joinText(searchBudget?.notes);

    return `
      <div style="margin-bottom:14px;">
        <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">自主研究上下文</div>
        <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:6px;margin-bottom:8px;">
          <div style="text-align:center;padding:8px;background:#1a2436;border-radius:4px;">
            <div style="font-size:10px;color:#6b7fa0;">研究模式</div>
            <div style="font-size:13px;font-weight:700;color:#c2d0e8;">${esc(researchModeText(researchMode))}</div>
          </div>
          <div style="text-align:center;padding:8px;background:#1a2436;border-radius:4px;">
            <div style="font-size:10px;color:#6b7fa0;">草案数量</div>
            <div style="font-size:13px;font-weight:700;color:#c2d0e8;">${strategyDrafts.length}</div>
          </div>
          <div style="text-align:center;padding:8px;background:#1a2436;border-radius:4px;">
            <div style="font-size:10px;color:#6b7fa0;">搜索预算</div>
            <div style="font-size:13px;font-weight:700;color:#c2d0e8;">${esc(String(searchBudget?.max_backtest_runs || '--'))}</div>
          </div>
        </div>
        <div style="font-size:12px;color:#b7c7e2;background:#141f2f;border-radius:6px;padding:8px;line-height:1.7;">
          <div style="color:#9fb1c9;margin-bottom:4px;">核心假设</div>
          <div>${esc(hypothesis || '--')}</div>
          <div style="margin-top:6px;color:#9fb1c9;">预算分配</div>
          <div>${esc(formatAutonomyBudget(searchBudget))}</div>
          ${budgetNotes !== '--' ? `<div style="margin-top:4px;color:#7e92b2;">预算备注：${esc(budgetNotes)}</div>` : ''}
          ${experimentPlan !== '--' ? `<div style="margin-top:6px;color:#9fb1c9;">实验计划：<span style="color:#b7c7e2;">${esc(experimentPlan)}</span></div>` : ''}
          ${evidenceRefs !== '--' ? `<div style="margin-top:4px;color:#9fb1c9;">证据引用：<span style="color:#b7c7e2;">${esc(evidenceRefs)}</span></div>` : ''}
          ${uncertainty ? `<div style="margin-top:4px;color:#9fb1c9;">不确定性：<span style="color:#b7c7e2;">${esc(uncertainty)}</span></div>` : ''}
        </div>
        <div style="margin-top:8px;">
          <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">策略草案</div>
          ${renderStrategyDraftSummary(strategyDrafts, 4)}
          <div style="margin-top:8px;">
            <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">Search Loop</div>
            ${renderSearchLoopSummary(searchSummary, strategyDrafts)}
          </div>
        </div>
        <div style="margin-top:8px;">
          <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">Research Lineage</div>
          ${renderResearchLineage(lineage)}
        </div>
      </div>`;
  }

  function scoreColor(score) {
    const n = Number(score || 0);
    return n >= 70 ? 'green' : n >= 50 ? 'yellow' : 'red';
  }

  function scoreEmoji(score) {
    return Number(score || 0) >= 70 ? '🟢' : Number(score || 0) >= 50 ? '🟡' : '🔴';
  }

  /* ── API 请求 ── */
  function getStrategyFamily(item) {
    if (item && typeof item === 'object') {
      const metadataFamily = String(item?.metadata?.strategy_family || item?.strategy_family || '').trim();
      if (metadataFamily) return metadataFamily;
      const strategyName = String(item?.strategy || item?.name || '').trim();
      return STRATEGY_FAMILIES[strategyName] || 'traditional';
    }
    return STRATEGY_FAMILIES[String(item || '').trim()] || 'traditional';
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
    const activeStatuses = new Set(['paper_running', 'live_candidate', 'live_running']);
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

  function getVisibleCandidates() {
    let visible = dedupeCandidatesForDisplay(state.candidates);
    if (state.filterCategory) {
      visible = visible.filter(c => STRATEGY_CATEGORIES[c.strategy] === state.filterCategory);
    }
    visible.sort((a, b) => {
      const contextDelta = (() => {
        const activeProposalId = String(state.selectedProposalId || '').trim();
        if (!activeProposalId) return 0;
        const aDelta = String(a?.proposal_id || '').trim() === activeProposalId ? 0 : 1;
        const bDelta = String(b?.proposal_id || '').trim() === activeProposalId ? 0 : 1;
        return aDelta - bDelta;
      })();
      if (contextDelta) return contextDelta;
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
    return visible;
  }

  function getVisibleCandidateProposalTargets(visibleCandidates = getVisibleCandidates()) {
    const clearableProposalIds = [];
    const skippedBlocked = [];
    const skippedVirtual = [];
    const seen = new Set();
    toArray(visibleCandidates).forEach((candidate) => {
      const proposalId = String(candidate?.proposal_id || '').trim();
      if (!proposalId || seen.has(proposalId)) return;
      seen.add(proposalId);
      const proposal = findProposalById(proposalId);
      if (!proposal || isVirtualProposal(proposal)) {
        skippedVirtual.push(proposalId);
        return;
      }
      const status = String(proposal?.status || '').trim();
      if (DELETE_BLOCKED_PROPOSAL_STATUSES.has(status)) {
        skippedBlocked.push({ proposalId, status });
        return;
      }
      clearableProposalIds.push(proposalId);
    });
    return { clearableProposalIds, skippedBlocked, skippedVirtual };
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
      selected: '当前关注',
      watchlist: '观察列表',
      retired: '已退役',
      new: '新建',
    }[String(s || '')] || String(s || '--');
  }

  function getFamilyMeta(item) {
    const family = getStrategyFamily(item);
    return FAMILY_META[family] || FAMILY_META.traditional;
  }

  function getCandidateEnrichment(cand) {
    const meta = cand?.metadata || {};
    const newsCount = Number(meta.news_events_count ?? meta.best?.news_events_count ?? 0);
    const fundingAvailable = !!(meta.funding_available ?? meta.best?.funding_available);
    let mode = '仅OHLCV';
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
    const drafts = toArray(item?.strategy_drafts);
    const aiTemplateCount = templates.filter(name => getStrategyFamily(name) !== 'traditional').length;
    const lastResearch = item?.metadata?.last_research_result || {};
    const newsCount = Number(lastResearch?.news_events_count || 0);
    const fundingAvailable = !!lastResearch?.funding_available;
    const job = item?.job || {};
    const fallbackCandidateTs =
      item?.metadata?.fallback_candidate_updated_at
      || item?.metadata?.fallback_candidate_created_at
      || '';
    const lastTs = job?.finished_at || job?.started_at || job?.created_at || item?.updated_at || item?.created_at || fallbackCandidateTs;
    return {
      totalTemplates: templates.length,
      draftCount: drafts.length,
      researchMode: String(item?.research_mode || 'template'),
      searchBudget: item?.search_budget || {},
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

  /* 鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹?
     信号杩蜂綘闈㈡澘
  鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹?*/
  function renderSignalMini() {
    const box = document.getElementById('ai-signal-mini');
    if (!box) return;
    const selectedSymbol = String(document.getElementById('signal-symbol')?.value || '').trim();
    const symbols = Array.from(new Set([...DEFAULT_SIGNAL_SYMBOLS, ...(selectedSymbol ? [selectedSymbol] : [])]));
    box.innerHTML = normalizeUiText(symbols.map((sym) => {
      const data = state.latestSignals[sym] || null;
      const hasData = !!(data && (data.timestamp || data.direction || data.confidence != null));
      const dir = hasData ? String(data?.direction || 'FLAT').toUpperCase() : 'EMPTY';
      const barDir = hasData ? (['LONG', 'SHORT'].includes(dir) ? dir : 'FLAT') : 'FLAT';
      const conf = hasData ? Math.min(100, Math.round(Number(data?.confidence || 0) * 100)) : 0;
      const noMarketData = hasData && Number(data?.market_data_rows || 0) <= 0;
      const isStale = !!data?.market_data_stale;
      const blocked = !!data?.blocked_by_risk;
      const hasActionableSignal = dir === 'LONG' || dir === 'SHORT';
      const waitingApproval = !!data?.requires_approval && hasActionableSignal && !noMarketData;
      const displayLabel = !hasData
        ? '待刷新'
        : noMarketData
          ? '缺数据'
          : ({ LONG: '看多', SHORT: '看空', FLAT: '观望' }[dir] || dir);
      const signalFlags = [
        blocked ? '<span class="signal-mini-flag is-risk">风控</span>' : '',
        waitingApproval ? '<span class="signal-mini-flag is-wait">待确认</span>' : '',
        isStale ? '<span class="signal-mini-flag is-stale">数据旧</span>' : '',
      ].filter(Boolean).join('');
      const confText = (hasData && !noMarketData && (hasActionableSignal || conf > 0)) ? `${conf}%` : '--';
      return `<div class="ai-signal-mini-row">
        <span class="signal-mini-sym">${esc(sym.split('/')[0])}</span>
        <span class="signal-mini-dir ${dir}"><span class="signal-mini-dir-label">${displayLabel}</span>${signalFlags}</span>
        <div class="signal-mini-bar"><div class="signal-mini-bar-fill ${barDir}" style="width:${conf}%;"></div></div>
        <span class="signal-mini-conf">${confText}</span>
      </div>`;
    }).join(''));
    normalizeDomText(box);
  }

  async function loadSignal(symbol) {
    if (state.signalLoading) return;
    state.signalLoading = true;
    const statusEl = document.getElementById('signal-status');
    const selectedSymbol = String(symbol || document.getElementById('signal-symbol')?.value || 'BTC/USDT').trim() || 'BTC/USDT';
    const watchlist = Array.from(new Set([...DEFAULT_SIGNAL_SYMBOLS, selectedSymbol]));
    if (statusEl) statusEl.textContent = `刷新 ${watchlist.length} 个币种...`;
    renderSignalMini();
    try {
      const results = await Promise.all(watchlist.map(async (sym) => {
        try {
          const data = await aiApi(`/signals/latest?symbol=${encodeURIComponent(sym)}`, { timeoutMs: 12000 });
          return { sym, data, error: null };
        } catch (err) {
          return { sym, data: null, error: err };
        }
      }));
      let successCount = 0;
      let latestTs = '';
      results.forEach(({ sym, data, error }) => {
        if (error) {
          state.latestSignals[sym] = {
            ...(state.latestSignals[sym] || {}),
            symbol: sym,
            error: String(error?.message || error || ''),
          };
          return;
        }
        successCount += 1;
        state.latestSignals[sym] = {
          ...(data || {}),
          symbol: sym,
          error: '',
        };
        const ts = String(data?.timestamp || '').trim();
        if (!ts) return;
        if (!latestTs) {
          latestTs = ts;
          return;
        }
        const currentTs = parseTs(ts);
        const bestTs = parseTs(latestTs);
        if (currentTs && (!bestTs || currentTs.getTime() > bestTs.getTime())) {
          latestTs = ts;
        }
      });
      renderSignalMini();
      renderCandidateCards();  // 更新卡片上的信号徽章
      emitWorkbenchState('signal-mini', { symbol: selectedSymbol, watchlist });
      if (statusEl) {
        statusEl.textContent = successCount > 0
          ? `已刷新 ${successCount}/${watchlist.length} 个，最近 ${fmtTs(latestTs || new Date().toISOString())}`
          : `信号失败：${watchlist.length} 个币种均未返回`;
      }
    } catch (err) {
      if (statusEl) statusEl.textContent = `信号失败: ${err.message}`;
    } finally {
      state.signalLoading = false;
    }
  }

  /* 鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹?
     鍊欓€夌瓥鐣ュ崱鐗?
  鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹?*/
  function textHasMeaningfulContent(value) {
    const normalized = normalizeUiText(String(value ?? '').trim());
    if (!normalized) return false;
    return /[^0-9\s?？#/:：._\-·,，()[\]{}]/.test(normalized);
  }

  function firstMeaningfulText(...values) {
    for (const value of values) {
      const normalized = normalizeUiText(String(value ?? '').replace(/\s+/g, ' ').trim());
      if (textHasMeaningfulContent(normalized)) return normalized;
    }
    return '';
  }

  function proposalPrimarySymbols(item) {
    return toArray(item?.target_symbols)
      .map((symbol) => String(symbol || '').trim().toUpperCase())
      .filter(Boolean)
      .slice(0, 2)
      .join(' / ');
  }

  function proposalBestStrategy(item) {
    const specId = String(item?.metadata?.latest_strategy_spec?.strategy_id || '').trim();
    const specName = specId ? specId.split('::').pop() : '';
    return firstMeaningfulText(
      item?.metadata?.last_research_result?.best?.strategy,
      specName,
      toArray(item?.strategy_templates)[0],
    );
  }

  function proposalBestTimeframe(item) {
    return firstMeaningfulText(
      item?.metadata?.last_research_result?.best?.timeframe,
      toArray(item?.target_timeframes)[0],
    );
  }

  function buildProposalDisplayFallback(item, index = 0) {
    const seq = String(item?.metadata?.proposal_sequence || '').trim();
    const mark = seq ? `#${seq}` : `#${String(index + 1).padStart(2, '0')}`;
    const detail = [
      proposalPrimarySymbols(item),
      proposalBestStrategy(item),
      proposalBestTimeframe(item),
    ].filter(Boolean).join(' / ');
    return `${mark} ${detail || String(item?.proposal_id || '').slice(-6)}`.trim();
  }

  function proposalResearchThesis(item) {
    const direct = firstMeaningfulText(
      item?.thesis,
      item?.origin_context?.goal,
      item?.metadata?.llm_research_output?.hypothesis,
    );
    if (direct) return direct;
    const symbolText = proposalPrimarySymbols(item) || '当前标的';
    const strategy = proposalBestStrategy(item) || '候选策略';
    const timeframe = proposalBestTimeframe(item);
    const regime = regimeText(item?.market_regime || 'mixed');
    return `${symbolText} 在${regime}下优先验证 ${strategy}${timeframe ? `（${timeframe}）` : ''} 的稳定性与风控边界`;
  }

  function normalizeProposalPresentation(item, index = 0) {
    if (!item || typeof item !== 'object') return item;
    const metadata = item?.metadata && typeof item.metadata === 'object' ? { ...item.metadata } : {};
    const originContext = item?.origin_context && typeof item.origin_context === 'object' ? { ...item.origin_context } : {};
    const normalized = {
      ...item,
      metadata,
      origin_context: originContext,
    };
    normalized.thesis = proposalResearchThesis(normalized);
    originContext.goal = firstMeaningfulText(originContext.goal, normalized.thesis) || normalized.thesis;
    metadata.display_name = firstMeaningfulText(metadata.display_name) || buildProposalDisplayFallback(normalized, index);
    return normalized;
  }

  function proposalDisplayName(item, index) {
    const normalized = normalizeProposalPresentation(item, index);
    return firstMeaningfulText(normalized?.metadata?.display_name) || buildProposalDisplayFallback(normalized, index);
  }

  function isVirtualProposal(item) {
    return !!item?.metadata?.virtual_context;
  }

  function proposalStatusPriority(status) {
    return {
      research_running: 0,
      research_queued: 1,
      paper_running: 2,
      live_running: 3,
      live_candidate: 4,
      shadow_running: 5,
      validated: 6,
      new: 7,
      draft: 8,
      rejected: 9,
      retired: 10,
    }[String(status || '').trim()] ?? 50;
  }

  function sortProposalsForWorkbench(items, activeProposalId = '') {
    const activeId = String(activeProposalId || '').trim();
    return toArray(items)
      .map((item, index) => {
        const meta = getProposalResearchMeta(item);
        const tsMs = new Date(String(meta?.lastTs || '')).getTime() || 0;
        return {
          item,
          index,
          tsMs,
          active: !!(activeId && String(item?.proposal_id || '').trim() === activeId),
          virtual: isVirtualProposal(item),
          statusPriority: proposalStatusPriority(item?.status),
        };
      })
      .sort((a, b) => {
        if (a.active !== b.active) return a.active ? -1 : 1;
        if (a.virtual !== b.virtual) return a.virtual ? 1 : -1;
        if (a.statusPriority !== b.statusPriority) return a.statusPriority - b.statusPriority;
        if (a.tsMs !== b.tsMs) return b.tsMs - a.tsMs;
        return a.index - b.index;
      })
      .map((entry) => entry.item);
  }

  function findProposalById(proposalId) {
    const target = String(proposalId || '').trim();
    if (!target) return null;
    return state.proposals.find((item) => String(item?.proposal_id || '').trim() === target) || null;
  }

  function upsertProposal(proposal) {
    const normalizedProposal = normalizeProposalPresentation(proposal, state.proposals.length);
    const proposalId = String(normalizedProposal?.proposal_id || '').trim();
    if (!proposalId) return null;
    const idx = state.proposals.findIndex((item) => String(item?.proposal_id || '').trim() === proposalId);
    if (idx >= 0) {
      state.proposals[idx] = normalizeProposalPresentation({ ...state.proposals[idx], ...(normalizedProposal || {}) }, idx);
      return state.proposals[idx];
    }
    state.proposals = [normalizedProposal, ...state.proposals];
    return state.proposals[0];
  }

  function proposalFallbackFromCandidate(candidate) {
    const proposalId = String(candidate?.proposal_id || '').trim();
    if (!proposalId) return null;
    const displayName = String(candidate?.metadata?.proposal_display_name || '').trim()
      || `候选链路 · ${String(candidate?.strategy || '--')} @ ${String(candidate?.symbol || '--')} ${String(candidate?.timeframe || '--')}`;
    const candidateCreatedAt = String(candidate?.created_at || '').trim();
    const candidateUpdatedAt = String(candidate?.updated_at || candidate?.created_at || '').trim();
    return {
      proposal_id: proposalId,
      thesis: String(candidate?.metadata?.thesis || candidate?.strategy || displayName).trim(),
      research_mode: String(candidate?.metadata?.research_mode || 'template').trim() || 'template',
      status: String(candidate?.status || 'validated').trim() || 'validated',
      created_at: candidateCreatedAt,
      updated_at: candidateUpdatedAt,
      metadata: {
        display_name: displayName,
        search_summary: candidate?.metadata?.search_summary || {},
        search_budget: candidate?.metadata?.search_budget || {},
        strategy_drafts: candidate?.metadata?.strategy_drafts || [],
        fallback_candidate_id: String(candidate?.candidate_id || '').trim(),
        fallback_candidate_created_at: candidateCreatedAt,
        fallback_candidate_updated_at: candidateUpdatedAt,
        virtual_context: true,
      },
    };
  }

  function mergeCandidateFallbackProposals() {
    const existingIds = new Set(
      state.proposals
        .map((item) => String(item?.proposal_id || '').trim())
        .filter(Boolean)
    );
    const fallbackProposals = [];
    state.candidates.forEach((candidate) => {
      const fallback = proposalFallbackFromCandidate(candidate);
      const proposalId = String(fallback?.proposal_id || '').trim();
      if (!proposalId || existingIds.has(proposalId)) return;
      existingIds.add(proposalId);
      fallbackProposals.push(fallback);
    });
    if (!fallbackProposals.length) return 0;
    state.proposals = sortProposalsForWorkbench([...fallbackProposals, ...state.proposals], state.selectedProposalId);
    return fallbackProposals.length;
  }

  function findCandidateById(candidateId) {
    const target = String(candidateId || '').trim();
    if (!target) return null;
    return state.candidates.find((item) => String(item?.candidate_id || '').trim() === target) || null;
  }

  function fallbackCandidateIdForProposal(item) {
    return String(item?.metadata?.fallback_candidate_id || '').trim();
  }

  function getVisibleProposalQueueItems() {
    return sortProposalsForWorkbench(state.proposals, state.selectedProposalId);
  }

  function getVisibleProposalQueueTargets(visibleProposals = getVisibleProposalQueueItems()) {
    const clearableProposalIds = [];
    const clearableCandidateIds = [];
    const skippedBlocked = [];
    const skippedMissing = [];
    const seenProposalIds = new Set();
    const seenCandidateIds = new Set();

    toArray(visibleProposals).forEach((item) => {
      const proposalId = String(item?.proposal_id || '').trim();
      if (!proposalId || seenProposalIds.has(proposalId)) return;
      seenProposalIds.add(proposalId);

      if (isVirtualProposal(item)) {
        const candidateId = fallbackCandidateIdForProposal(item);
        if (!candidateId || seenCandidateIds.has(candidateId)) {
          if (!candidateId) skippedMissing.push({ type: 'candidate', proposalId });
          return;
        }
        seenCandidateIds.add(candidateId);
        const candidate = findCandidateById(candidateId);
        if (!candidate) {
          skippedMissing.push({ type: 'candidate', proposalId, candidateId });
          return;
        }
        const status = String(candidate?.status || '').trim();
        if (DELETE_BLOCKED_CANDIDATE_STATUSES.has(status)) {
          skippedBlocked.push({ type: 'candidate', candidateId, status });
          return;
        }
        clearableCandidateIds.push(candidateId);
        return;
      }

      const status = String(item?.status || '').trim();
      if (DELETE_BLOCKED_PROPOSAL_STATUSES.has(status)) {
        skippedBlocked.push({ type: 'proposal', proposalId, status });
        return;
      }
      clearableProposalIds.push(proposalId);
    });

    return { clearableProposalIds, clearableCandidateIds, skippedBlocked, skippedMissing };
  }

  function getVisibleRunningQueueTargets(visibleProposals = getVisibleProposalQueueItems()) {
    const proposalIds = [];
    const candidateIds = [];
    const skippedMissing = [];
    const seenProposalIds = new Set();
    const seenCandidateIds = new Set();

    toArray(visibleProposals).forEach((item) => {
      const proposalId = String(item?.proposal_id || '').trim();
      if (!proposalId || seenProposalIds.has(proposalId)) return;
      seenProposalIds.add(proposalId);

      if (isVirtualProposal(item)) {
        const candidateId = fallbackCandidateIdForProposal(item);
        if (!candidateId || seenCandidateIds.has(candidateId)) {
          if (!candidateId) skippedMissing.push({ type: 'candidate', proposalId });
          return;
        }
        seenCandidateIds.add(candidateId);
        const candidate = findCandidateById(candidateId);
        if (!candidate) {
          skippedMissing.push({ type: 'candidate', proposalId, candidateId });
          return;
        }
        const status = String(candidate?.status || '').trim();
        if (DELETE_BLOCKED_CANDIDATE_STATUSES.has(status)) {
          candidateIds.push(candidateId);
        }
        return;
      }

      const status = String(item?.status || '').trim();
      if (DELETE_BLOCKED_PROPOSAL_STATUSES.has(status)) {
        proposalIds.push(proposalId);
      }
    });

    return { proposalIds, candidateIds, skippedMissing };
  }

  function proposalIdForCandidate(candidateId) {
    return String(findCandidateById(candidateId)?.proposal_id || '').trim();
  }

  function candidateCountForProposal(proposalId) {
    const target = String(proposalId || '').trim();
    if (!target) return 0;
    return state.candidates.filter((item) => String(item?.proposal_id || '').trim() === target).length;
  }

  function candidatesForProposal(proposalId) {
    const target = String(proposalId || '').trim();
    if (!target) return [];
    return state.candidates.filter((item) => String(item?.proposal_id || '').trim() === target);
  }

  function autoSelectCandidateForProposal() {
    if (state.selectedCandidateId) return '';
    const scoped = dedupeCandidatesForDisplay(candidatesForProposal(state.selectedProposalId));
    if (scoped.length !== 1) return '';
    const candidateId = String(scoped[0]?.candidate_id || '').trim();
    if (!candidateId) return '';
    state.selectedCandidateId = candidateId;
    return candidateId;
  }

  function candidateDetailPlaceholderHtml(proposalId = '') {
    const activeProposalId = String(proposalId || '').trim();
    const proposal = findProposalById(activeProposalId);
    const virtualProposal = isVirtualProposal(proposal);
    const proposalName = proposal ? proposalDisplayName(proposal, Math.max(0, state.proposals.findIndex((item) => item === proposal))) : '';
    const candidateCount = activeProposalId ? candidateCountForProposal(activeProposalId) : 0;
    /*
      ? `当前研究任务：${proposalName}${candidateCount ? ` · ${candidateCount} 个候选` : ' · 暂无候选'}`
      : '鍏堥€夌爺绌朵换鍔★紝鍐嶇偣鍑诲€欓€夌瓥鐣ュ崱鐗?;
    const hint = proposal
      ? (candidateCount
        ? '鐐瑰嚮鍊欓€夌瓥鐣ュ崱鐗囷紝鍦ㄥ彸渚ц繘鍏ョ 4 姝ユ敞鍐?閮ㄧ讲'
        : '鍏堣繍琛岃研究浠诲姟锛屼骇鍑哄€欓€夊悗鍐嶆煡鐪嬭鎯?)
      : '鏌ョ湅璇︾粏分析中庣 4 姝ユ敞鍐?閮ㄧ讲';
    return `<div class="ai-detail-placeholder">
      <div style="font-size:36px;opacity:.3;">馃搳</div>
      <div style="margin-top:10px;color:#6b7fa0;font-size:13px;">${esc(summary)}<br>${esc(hint)}</div>
    </div>`;
    */
    const summary = proposal
      ? `当前${virtualProposal ? '候选回填' : '研究任务'}：${proposalName}${candidateCount ? ` · ${candidateCount} 个候选` : ' · 暂无候选'}`
      : '先选研究任务，再点击候选策略卡片';
    const hint = proposal
      ? (candidateCount
        ? (virtualProposal
          ? '该条目由候选结果回填，只支持查看候选详情与注册/部署，不支持重新运行研究任务'
          : '点击候选策略卡片，在右侧进入第 4 步注册/部署')
        : (virtualProposal
          ? '当前没有可展示的候选记录，请回到研究链路重新生成提案后再运行'
          : '先运行该研究任务，产出候选后再查看详情'))
      : '查看详细分析与第 4 步注册/部署';
    return `<div class="ai-detail-placeholder">
      <div style="font-size:36px;opacity:.3;">📊</div>
      <div style="margin-top:10px;color:#6b7fa0;font-size:13px;">${esc(summary)}<br>${esc(hint)}</div>
    </div>`;
  }

  function renderCandidateDetailPlaceholder(proposalId = '') {
    const panel = document.getElementById('ai-detail-panel');
    if (!panel) return;
    panel.innerHTML = candidateDetailPlaceholderHtml(proposalId);
    panel.dataset.candidateId = '';
    normalizeDomText(panel);
  }

  function syncSelectedProposal(preferredProposalId = '') {
    const validIds = new Set(
      state.proposals
        .map((item) => String(item?.proposal_id || '').trim())
        .filter(Boolean)
    );
    const candidateProposalId = proposalIdForCandidate(state.selectedCandidateId);
    const nextProposalId = [
      String(preferredProposalId || '').trim(),
      candidateProposalId,
      String(state.selectedProposalId || '').trim(),
      String(state.proposals[0]?.proposal_id || '').trim(),
    ].find((proposalId) => proposalId && validIds.has(proposalId)) || '';
    state.selectedProposalId = nextProposalId;
    const selectedCandidateProposalId = proposalIdForCandidate(state.selectedCandidateId);
    if (state.selectedCandidateId && selectedCandidateProposalId && nextProposalId && selectedCandidateProposalId !== nextProposalId) {
      state.selectedCandidateId = '';
    }
    return nextProposalId;
  }

  function applyWorkbenchSelection(preferredProposalId = '') {
    syncSelectedProposal(preferredProposalId);
    const autoSelectedCandidateId = autoSelectCandidateForProposal();
    renderProposalList();
    updateRunBtn();
    renderCandidateCards();
    if (!state.selectedCandidateId) {
      renderCandidateDetailPlaceholder(state.selectedProposalId);
    }
    return autoSelectedCandidateId;
  }

  function selectProposal(proposalId) {
    const nextProposalId = String(proposalId || '').trim();
    if (!nextProposalId) return;
    state.selectedProposalId = nextProposalId;
    const selectedCandidateProposalId = proposalIdForCandidate(state.selectedCandidateId);
    if (selectedCandidateProposalId && selectedCandidateProposalId !== nextProposalId) {
      state.selectedCandidateId = '';
    }
    const autoSelectedCandidateId = applyWorkbenchSelection(nextProposalId);
    if (autoSelectedCandidateId) {
      viewCandidate(autoSelectedCandidateId, { keepContent: true }).catch(err => notify(`加载详情失败: ${err.message}`, true));
    }
    emitWorkbenchState('proposal-selection', { proposalId: nextProposalId });
  }

  /* 鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹?
     鍊欓€夌瓥鐣ュ崱鐗?
  鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹?*/
  function renderProposalList() {
    const box = document.getElementById('ai-proposal-list');
    const badge = document.getElementById('ai-queue-badge');
    const titleEl = document.getElementById('ai-queue-title');
    const hintEl = document.getElementById('ai-queue-hint');
    const visibleProposals = getVisibleProposalQueueItems();
    if (!box) return;
    const realCount = visibleProposals.filter((item) => !isVirtualProposal(item)).length;
    const virtualCount = visibleProposals.filter((item) => isVirtualProposal(item)).length;
    if (badge) {
      if (realCount && virtualCount) badge.textContent = `${realCount} 项 + ${virtualCount} 条回填`;
      else if (realCount) badge.textContent = `${realCount} 项`;
      else badge.textContent = virtualCount ? `${virtualCount} 条回填` : '';
    }
    if (titleEl) titleEl.textContent = realCount ? '研究任务' : (virtualCount ? '候选回填' : '研究任务');
    if (hintEl) {
      if (realCount && virtualCount) {
        hintEl.textContent = '这里负责切换当前提案。候选回填条目仅支持查看候选详情/注册，不支持重新运行或删除提案。';
      } else if (virtualCount) {
        hintEl.textContent = '当前没有原始研究提案，以下条目由候选记录回填，只支持查看候选详情/注册，不支持重新运行或删除提案。';
      } else {
        hintEl.textContent = '这里负责切换当前提案。选中后去中栏执行第 3 步“运行研究”，再到右侧完成第 4 步“注册/部署”。';
      }
    }
    if (!visibleProposals.length) {
      updateClearQueueButton([]);
      updateExitRunningQueueButton([]);
      box.innerHTML = '<div style="color:#6b7fa0;font-size:12px;padding:8px 0;">暂无研究任务</div>';
      normalizeDomText(box);
      emitWorkbenchState('proposal-list');
      return;
    }
    box.innerHTML = visibleProposals.map((item, idx) => {
      const pid = String(item?.proposal_id || '');
      const sel = pid === state.selectedProposalId ? ' selected' : '';
      const st = String(item?.status || 'draft');
      const virtual = isVirtualProposal(item);
      const dotCls = { research_running: 'running', research_queued: 'queued', validated: 'validated', rejected: 'rejected' }[st] || '';
      const name = proposalDisplayName(item, idx);
      const running = !virtual && ['research_queued', 'research_running'].includes(st);
      const retirable = !virtual && ['shadow_running', 'live_candidate', 'paper_running', 'live_running'].includes(st);
      const meta = getProposalResearchMeta(item);
      const timeLabel = meta.lastTs ? fmtTs(meta.lastTs) : '--';
      const aiSummary = `${researchModeText(meta.researchMode)} | 模板 ${meta.totalTemplates}`;
      const autonomySummary = `草案 ${meta.draftCount} | 预算 ${Number(meta?.searchBudget?.max_backtest_runs || 0) || '--'}`;
      const newsSummary = `新闻 ${meta.newsCount}`;
      const macroSummary = meta.fundingAvailable ? '宏观 已启用' : '宏观 未启用';
      const virtualSummary = virtual
        ? '<span style="color:#f0b429;">候选回填</span><span>只读</span>'
        : '';
      return `<div class="proposal-compact-item${sel}" data-proposal-id="${esc(pid)}" data-proposal-status="${esc(st)}" data-action="select-proposal">
        <div class="pci-dot ${dotCls}" title="${esc(statusText(st))}"></div>
        <div style="min-width:0;flex:1;">
          <div class="pci-name" title="${esc(name)}">${esc(name)}</div>
          <div style="font-size:11px;color:#7e92b2;display:flex;gap:8px;flex-wrap:wrap;margin-top:2px;">
            <span>${esc(statusText(st))}</span>
            <span>${esc(timeLabel)}</span>
          </div>
          <div style="font-size:11px;color:#8ea3c2;display:flex;gap:8px;flex-wrap:wrap;margin-top:2px;">
            ${virtualSummary}
            <span>${esc(aiSummary)}</span>
            <span>${esc(autonomySummary)}</span>
            <span>${esc(newsSummary)}</span>
            <span>${esc(macroSummary)}</span>
          </div>
        </div>
        <div class="pci-actions">
          ${running
            ? `<button class="btn btn-sm" style="padding:1px 6px;font-size:11px;color:#f0b429;" data-action="cancel-proposal" data-proposal-id="${esc(pid)}" title="取消运行">停</button>`
            : ''}
          ${retirable ? `<button class="btn btn-sm" style="padding:1px 6px;font-size:11px;color:#f59e0b;" data-action="retire-proposal" data-proposal-id="${esc(pid)}" title="退役">退</button>` : ''}
          ${virtual ? '' : `<button class="btn btn-sm" style="padding:1px 6px;font-size:11px;color:#e05260;" data-action="delete-proposal" data-proposal-id="${esc(pid)}" title="删除">删</button>`}
        </div>
      </div>`;
    }).join('');
    updateClearQueueButton(visibleProposals);
    updateExitRunningQueueButton(visibleProposals);
    normalizeDomText(box);
    emitWorkbenchState('proposal-list');
  }

  function updateClearQueueButton(visibleProposals = getVisibleProposalQueueItems()) {
    const btn = document.getElementById('ai-clear-queue-btn');
    if (!btn) return;

    const visibleCount = toArray(visibleProposals).length;
    const { clearableProposalIds, clearableCandidateIds, skippedBlocked, skippedMissing } = getVisibleProposalQueueTargets(visibleProposals);
    const clearableCount = clearableProposalIds.length + clearableCandidateIds.length;
    const busy = hasActionLock();

    btn.textContent = visibleCount ? `一键清空当前任务 (${visibleCount})` : '一键清空当前任务';
    btn.disabled = !visibleCount || busy || !clearableCount;

    if (!visibleCount) {
      btn.title = '当前没有可清空的研究任务';
      return;
    }
    if (busy) {
      btn.title = '当前有任务执行中，请等待当前流程完成后再清空任务队列';
      return;
    }
    if (!clearableCount) {
      const reasons = [];
      if (skippedBlocked.length) reasons.push(`运行中条目 ${skippedBlocked.length} 个`);
      if (skippedMissing.length) reasons.push(`无效回填 ${skippedMissing.length} 个`);
      btn.title = reasons.length ? `当前可见任务暂不可清空：${reasons.join('，')}` : '当前没有可清空的研究任务';
      return;
    }

    const hints = [];
    if (clearableProposalIds.length) hints.push(`将删除 ${clearableProposalIds.length} 个研究任务`);
    if (clearableCandidateIds.length) hints.push(`将清理 ${clearableCandidateIds.length} 个候选回填`);
    if (skippedBlocked.length) hints.push(`跳过 ${skippedBlocked.length} 个运行中条目`);
    if (skippedMissing.length) hints.push(`忽略 ${skippedMissing.length} 个无效回填`);
    btn.title = hints.join('；');
  }

  updateClearQueueButton = function updateClearQueueButtonOverride(visibleProposals = getVisibleProposalQueueItems()) {
    const btn = document.getElementById('ai-clear-queue-btn');
    if (!btn) return;

    const visibleCount = toArray(visibleProposals).length;
    const { clearableProposalIds, clearableCandidateIds, skippedBlocked, skippedMissing } = getVisibleProposalQueueTargets(visibleProposals);
    const clearableCount = clearableProposalIds.length + clearableCandidateIds.length;
    const busy = hasActionLock();

    btn.textContent = visibleCount ? `一键清空当前任务 (${visibleCount})` : '一键清空当前任务';
    btn.disabled = !visibleCount || busy || !clearableCount;

    if (!visibleCount) {
      btn.title = '当前没有可清空的研究任务';
      return;
    }
    if (busy) {
      btn.title = '当前有其他操作正在执行，请等待完成后再清空任务队列';
      return;
    }
    if (!clearableCount) {
      const reasons = [];
      if (skippedBlocked.length) reasons.push(`运行/跟踪中的条目 ${skippedBlocked.length} 个，需要先退出`);
      if (skippedMissing.length) reasons.push(`失效回填 ${skippedMissing.length} 个`);
      btn.title = reasons.length ? `当前可见任务暂不可清空：${reasons.join('；')}` : '当前没有可清空的研究任务';
      return;
    }

    const hints = [];
    if (clearableProposalIds.length) hints.push(`将删除 ${clearableProposalIds.length} 个研究任务`);
    if (clearableCandidateIds.length) hints.push(`将清理 ${clearableCandidateIds.length} 个候选回填`);
    if (skippedBlocked.length) hints.push(`另有 ${skippedBlocked.length} 个运行/跟踪中的条目不会被清空`);
    if (skippedMissing.length) hints.push(`忽略 ${skippedMissing.length} 个失效回填`);
    btn.title = hints.join('；');
  };

  function updateExitRunningQueueButton(visibleProposals = getVisibleProposalQueueItems()) {
    const btn = document.getElementById('ai-exit-running-queue-btn');
    if (!btn) return;

    const { proposalIds, candidateIds, skippedMissing } = getVisibleRunningQueueTargets(visibleProposals);
    const exitableCount = proposalIds.length + candidateIds.length;
    const busy = hasActionLock();

    btn.textContent = exitableCount ? `一键退出运行中条目 (${exitableCount})` : '一键退出运行中条目';
    btn.disabled = busy || !exitableCount;

    if (busy) {
      btn.title = '当前有其他操作正在执行，请等待完成后再退出运行中条目';
      return;
    }
    if (!exitableCount) {
      btn.title = skippedMissing.length ? '当前没有可退出的运行中条目，部分回填记录已失效' : '当前没有可退出的运行中条目';
      return;
    }

    const hints = [];
    if (proposalIds.length) hints.push(`将退出 ${proposalIds.length} 个研究任务`);
    if (candidateIds.length) hints.push(`将退出 ${candidateIds.length} 个候选回填`);
    if (skippedMissing.length) hints.push(`忽略 ${skippedMissing.length} 个失效回填`);
    btn.title = hints.join('；');
  }

  function renderCandidateCards() {
    const box = document.getElementById('ai-candidate-cards');
    const cnt = document.getElementById('ai-candidate-count');
    if (!box) return;

    const totalCount = state.candidates.length;
    const visible = getVisibleCandidates();
    const dedupedIds = new Set(dedupeCandidatesForDisplay(state.candidates).map(c => String(c?.candidate_id || '')));
    Array.from(state.compareCandidateIds).forEach((cid) => {
      if (!dedupedIds.has(String(cid))) state.compareCandidateIds.delete(String(cid));
    });

    if (cnt) cnt.textContent = visible.length
      ? `${visible.length}/${state.candidates.length} 个`
      : (state.candidates.length ? `0/${state.candidates.length} (筛选后为空)` : '');

    if (!visible.length) {
      refreshCompareToolbar();
      updateClearCandidatesButton([]);
      if (cnt) cnt.textContent = totalCount ? `0/${totalCount}` : '';
      box.innerHTML = state.candidates.length
        ? `<div class="ai-empty-hint">当前类别筛选无结果，请调整筛选条件。</div>`
        : `<div class="ai-empty-hint">暂无候选策略。<br>先在左侧点击 <strong>2) 生成提案</strong>，<br>再选中研究任务并点击 <strong>3) 运行研究</strong> 开始回测。</div>`;
      emitWorkbenchState('candidate-cards');
      return;
    }
    box.innerHTML = visible.map(c => buildCandidateCard(c)).join('');
    box.innerHTML = normalizeUiText(box.innerHTML);
    normalizeDomText(box);
    refreshCompareToolbar();
    updateClearCandidatesButton(visible);
    if (cnt) cnt.textContent = `${visible.length}/${totalCount}`;
    emitWorkbenchState('candidate-cards');
  }

  function updateClearCandidatesButton(visibleCandidates = getVisibleCandidates()) {
    const btn = document.getElementById('ai-clear-candidates-btn');
    if (!btn) return;
    const visibleCount = toArray(visibleCandidates).length;
    const { clearableProposalIds, skippedBlocked, skippedVirtual } = getVisibleCandidateProposalTargets(visibleCandidates);
    const clearableSet = new Set(clearableProposalIds);
    const clearableVisibleCount = toArray(visibleCandidates)
      .filter((item) => clearableSet.has(String(item?.proposal_id || '').trim()))
      .length;
    const busy = hasActionLock();
    btn.textContent = visibleCount ? `一键清空当前候选 (${visibleCount})` : '一键清空当前候选';
    btn.disabled = !visibleCount || busy || !clearableProposalIds.length;
    if (!visibleCount) {
      btn.title = '当前没有可清空的候选策略';
      return;
    }
    if (busy) {
      btn.title = '当前有任务执行中，请等待当前流程完成后再清空候选';
      return;
    }
    if (!clearableProposalIds.length) {
      const reasons = [];
      if (skippedBlocked.length) reasons.push(`运行中任务 ${skippedBlocked.length} 个`);
      if (skippedVirtual.length) reasons.push(`候选回填条目 ${skippedVirtual.length} 个`);
      btn.title = reasons.length ? `当前可见候选暂不可清空：${reasons.join('，')}` : '当前没有可清空的候选策略';
      return;
    }
    const hints = [`将删除 ${clearableProposalIds.length} 个研究任务，移除 ${clearableVisibleCount} 个当前可见候选`];
    if (skippedBlocked.length) hints.push(`跳过 ${skippedBlocked.length} 个运行中任务`);
    if (skippedVirtual.length) hints.push(`跳过 ${skippedVirtual.length} 个候选回填条目`);
    btn.title = hints.join('；');
  }

  /* 鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹?
     鍙充晶璇︽儏闈㈡澘
  鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹?*/
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
    const proposalId = String(cand?.proposal_id || '').trim();
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
      ? `<span class="cand-badge" style="background:#7a3a2a;color:#fff;padding:2px 5px;border-radius:3px;font-size:10px;margin-left:2px;" title="与 ${esc(corrWith)}${corrIsCross ? '（已运行策略）' : ''} 高度相关 ρ=${corrVal}">${corrLabel}</span>`
      : '';
    const trials = cand?.metadata?.best?.optimization_trials;
    const paramsBadge = trials > 0
      ? `<span style="font-size:10px;padding:1px 5px;border-radius:3px;background:#a78bfa22;color:#a78bfa;border:1px solid #a78bfa44;">${trials} trials</span>`
      : '';
    let signalBadge = '';
    const sigData = state.latestSignals[sym];
    if (sigData) {
      const dir = String(sigData.direction || '').toUpperCase();
      if (!['LONG', 'SHORT'].includes(dir)) {
        signalBadge = '';
      } else {
      const conf = Math.round(Number(sigData.confidence || 0) * 100);
      const dirLabel = { LONG: '看多', SHORT: '看空' }[dir] || dir;
      signalBadge = `<span class="cand-signal-badge">${esc(sym.split('/')[0])} ${dirLabel} ${conf}%</span><br>`;
      }
    }
    const compareChecked = state.compareCandidateIds.has(cid) ? 'checked' : '';
    const category = STRATEGY_CATEGORIES[strat] || '';
    const catColor = CATEGORY_COLORS[category] || '#64748b';
    const familyMeta = getFamilyMeta(cand);
    const enrichment = getCandidateEnrichment(cand);
    const catBadge = category
      ? `<span class="cand-category-badge" style="background:${catColor}22;color:${catColor};border:1px solid ${catColor}44;">${esc(category)}</span>`
      : '';
    const familyBadge = `<span class="cand-category-badge" style="background:${familyMeta.accent};color:${familyMeta.color};border:1px solid ${familyMeta.color}44;">${esc(familyMeta.label)}</span>`;
    const searchRoleMeta = candidateSearchRoleMeta(cand?.metadata?.search_role);
    const searchRoleBadge = searchRoleMeta
      ? `<span class="cand-category-badge" style="background:${searchRoleMeta.bg};color:${searchRoleMeta.fg};border:1px solid ${searchRoleMeta.border};">${esc(searchRoleMeta.label)}</span>`
      : '';
    const proposalScopeBadge = state.selectedProposalId && state.proposals.length > 1
      ? `<span class="cand-category-badge" style="background:${proposalId === state.selectedProposalId ? '#143224' : '#1d2b3d'};color:${proposalId === state.selectedProposalId ? '#20bf78' : '#9fb1c9'};border:1px solid ${proposalId === state.selectedProposalId ? '#245b42' : '#32475f'};">${proposalId === state.selectedProposalId ? '当前提案' : '其他提案'}</span>`
      : '';
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
    const aiCardStyle = getStrategyFamily(cand) === 'traditional'
      ? ''
      : ` style="box-shadow:0 0 0 1px ${familyMeta.color}33 inset, 0 10px 30px ${familyMeta.accent};"`;

    return `<div class="research-candidate-card score-${color}${sel}"${aiCardStyle}
               data-candidate-id="${esc(cid)}" data-proposal-id="${esc(proposalId)}" data-action="select-candidate">
      <div class="cand-card-header">
        <div class="cand-card-title">${emoji} ${esc(strat)}</div>
        <div style="display:flex;align-items:center;gap:5px;flex-wrap:wrap;">
          ${familyBadge}${catBadge}${searchRoleBadge}${proposalScopeBadge}<div class="cand-score-badge ${color}">${score.toFixed(0)}</div>
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
      </div>
    </div>`;
  }

  async function viewCandidate(candidateId, options = {}) {
    if (!candidateId) return;
    const requestSeq = ++state.candidateDetailReqSeq;
    const panel = document.getElementById('ai-detail-panel');
    const keepContent = !!options.keepContent;
    if (panel && !(keepContent && panel.dataset.candidateId === String(candidateId))) {
      panel.innerHTML = '<div style="padding:20px;color:#7e92b2;font-size:13px;">加载中...</div>';
    }
    const resp  = await aiApi(`/candidates/${encodeURIComponent(candidateId)}`, { timeoutMs: 20000 });
    if (requestSeq !== state.candidateDetailReqSeq) return;
    const cand  = resp?.candidate || {};
    const linkedProposalId = String(cand?.proposal_id || '').trim();
    state.selectedCandidateId = candidateId;
    if (linkedProposalId) {
      state.selectedProposalId = linkedProposalId;
    }
    renderProposalList();
    renderCandidateCards();   // 鏇存柊閫変腑楂樹寒
    updateRunBtn();

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
    const familyMeta = getFamilyMeta(cand);
    const enrichment = getCandidateEnrichment(cand);
    const searchRoleMeta = candidateSearchRoleMeta(cand?.metadata?.search_role);
    const championStrategy = String(cand?.metadata?.champion_strategy || '').trim();

    const proposalSnapshot = state.proposals.find(item => String(item?.proposal_id || '').trim() === proposalId) || null;
    const hasVirtualProposalSnapshot = !!proposalSnapshot?.metadata?.virtual_context;
    const [proposalResp, proposalLifecycleResp, candidateLifecycleResp, experimentResp, experimentRunsResp] = await Promise.allSettled([
      proposalId && !hasVirtualProposalSnapshot
        ? aiApi(`/proposals/${encodeURIComponent(proposalId)}`, { timeoutMs: 12000 })
        : Promise.resolve({ proposal: proposalSnapshot }),
      proposalId && !hasVirtualProposalSnapshot
        ? aiApi(`/proposals/${encodeURIComponent(proposalId)}/lifecycle?limit=20`, { timeoutMs: 12000 })
        : Promise.resolve({ items: [] }),
      aiApi(`/candidates/${encodeURIComponent(candidateId)}/lifecycle?limit=20`, { timeoutMs: 12000 }),
      experimentId ? aiApi(`/experiments/${encodeURIComponent(experimentId)}`, { timeoutMs: 12000 }) : Promise.resolve({ experiment: null }),
      experimentId ? aiApi(`/experiments/${encodeURIComponent(experimentId)}/runs?limit=20`, { timeoutMs: 12000 }) : Promise.resolve({ items: [] }),
    ]);
    if (requestSeq !== state.candidateDetailReqSeq) return;

    const proposalInfo = proposalResp.status === 'fulfilled'
      ? (proposalResp.value?.proposal || proposalSnapshot || proposalFallbackFromCandidate(cand))
      : (proposalSnapshot || proposalFallbackFromCandidate(cand));
    if (proposalInfo) {
      upsertProposal(proposalInfo);
      renderProposalList();
      updateRunBtn();
    }
    const proposalLifecycle = proposalLifecycleResp.status === 'fulfilled' ? toArray(proposalLifecycleResp.value?.items) : [];
    const candidateLifecycle = candidateLifecycleResp.status === 'fulfilled' ? toArray(candidateLifecycleResp.value?.items) : [];
    const experimentInfo = experimentResp.status === 'fulfilled' ? (experimentResp.value?.experiment || null) : null;
    const experimentRuns = experimentRunsResp.status === 'fulfilled' ? toArray(experimentRunsResp.value?.items) : [];
    const autonomyHtml = renderAutonomyContext(proposalInfo, {
      thesis: cand?.metadata?.llm_rationale || cand?.strategy || '',
      researchMode: cand?.metadata?.research_mode || experimentInfo?.research_mode || 'template',
      strategyDrafts: cand?.metadata?.strategy_drafts || experimentInfo?.strategy_drafts || [],
      searchBudget: cand?.metadata?.search_budget || experimentInfo?.search_budget || {},
      searchSummary: cand?.metadata?.search_summary || experimentInfo?.search_summary || proposalSnapshot?.search_summary || proposalSnapshot?.metadata?.search_summary || {},
      lineage: cand?.metadata?.lineage || experimentInfo?.lineage || null,
      llmResearchOutput: proposalSnapshot?.metadata?.llm_research_output || {},
    });

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
          <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">鏈€浼樺弬鏁?(Best Params)</div>
          <div style="font-size:12px;color:#c2d0e8;background:#1a2436;border-radius:4px;padding:8px;font-family:monospace;">
            ${bestParamsKeys.map(k => `<span style="color:#a78bfa">${esc(k)}</span>=<span style="color:#20bf78">${esc(String(bestParams[k]))}</span>`).join('  ')}
          </div>
          ${(cand?.metadata?.best?.optimization_trials > 0) ? `<div style="font-size:11px;color:#6b7fa0;margin-top:3px;">鍏辫瘯楠?${cand.metadata.best.optimization_trials} 缁勫弬鏁扮粍鍚?/div>` : ''}
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
        <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">IS / OOS / \u6eda\u52a8\u9a8c\u8bc1</div>
        <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:6px;">
          <div style="text-align:center;padding:6px;background:#1a2436;border-radius:4px;">
            <div style="font-size:10px;color:#6b7fa0;">IS\u590f\u666e</div>
            <div style="font-size:14px;font-weight:700;color:#c2d0e8;">${isScore}</div>
          </div>
          <div style="text-align:center;padding:6px;background:#1a2436;border-radius:4px;">
            <div style="font-size:10px;color:#6b7fa0;">OOS\u590f\u666e</div>
            <div style="font-size:14px;font-weight:700;color:${oosClr};">${oosScore}</div>
          </div>
          <div style="text-align:center;padding:6px;background:#1a2436;border-radius:4px;">
            <div style="font-size:10px;color:#6b7fa0;">WF\u7a33\u5b9a\u6027</div>
            <div style="font-size:14px;font-weight:700;color:#c2d0e8;">${wfStab}</div>
          </div>
          <div style="text-align:center;padding:6px;background:#1a2436;border-radius:4px;">
            <div style="font-size:10px;color:#6b7fa0;">\u9c81\u68d2\u6027\u5206</div>
            <div style="font-size:14px;font-weight:700;color:#c2d0e8;">${robustness}</div>
          </div>
        </div>
        <div style="display:grid;grid-template-columns:repeat(2,1fr);gap:6px;margin-top:6px;">
          <div style="text-align:center;padding:6px;background:#1a2436;border-radius:4px;">
            <div style="font-size:10px;color:#6b7fa0;">DSR \u5206\u6570</div>
            <div style="font-size:14px;font-weight:700;color:#c2d0e8;">${dsrVal}</div>
          </div>
          <div style="text-align:center;padding:6px;background:#1a2436;border-radius:4px;">
            <div style="font-size:10px;color:#6b7fa0;">WF \u4e00\u81f4\u6027</div>
            <div style="font-size:14px;font-weight:700;color:#c2d0e8;">${wfConsist}</div>
          </div>
        </div>
      </div>`;

    const equityCurve = normalizeNumberSeries(cand?.metadata?.best?.equity_curve_sample || []);
    const equityCurveHtml = `
      <div style="margin-bottom:14px;">
        <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">\u8d44\u91d1\u66f2\u7ebf\u6837\u672c</div>
        ${equityCurve.length >= 2
          ? renderSparklineSvg(equityCurve)
          : '<div style="font-size:12px;color:#6b7fa0;">\u6682\u65e0\u8d44\u91d1\u66f2\u7ebf\u6837\u672c\u3002</div>'}
      </div>`;

    const artifactsHtml = `
      <div style="margin-bottom:14px;">
        <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">\u7814\u7a76\u4ea7\u7269</div>
        <div style="font-size:12px;color:#b7c7e2;background:#141f2f;border-radius:6px;padding:8px;">
          <div>CSV \u6587\u4ef6\uff1a${esc(String(cand?.metadata?.csv_path || '--'))}</div>
          <div style="margin-top:4px;">Markdown \u62a5\u544a\uff1a${esc(String(cand?.metadata?.markdown_path || '--'))}</div>
        </div>
      </div>`;

    const enrichmentHtml = `
      <div style="margin-bottom:14px;">
        <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">\u7814\u7a76\u589e\u5f3a</div>
        <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:6px;">
          <div style="text-align:center;padding:8px;background:#1a2436;border-radius:4px;">
            <div style="font-size:10px;color:#6b7fa0;">\u51b3\u7b56\u5f15\u64ce</div>
            <div style="font-size:13px;font-weight:700;color:${familyMeta.color};">${esc(familyMeta.label)}</div>
          </div>
          <div style="text-align:center;padding:8px;background:#1a2436;border-radius:4px;">
            <div style="font-size:10px;color:#6b7fa0;">\u65b0\u95fb\u4e8b\u4ef6</div>
            <div style="font-size:13px;font-weight:700;color:#c2d0e8;">${enrichment.newsCount}</div>
          </div>
          <div style="text-align:center;padding:8px;background:#1a2436;border-radius:4px;">
            <div style="font-size:10px;color:#6b7fa0;">\u5b8f\u89c2\u5f00\u5173</div>
            <div style="font-size:13px;font-weight:700;color:${enrichment.fundingAvailable ? '#20bf78' : '#9a8bb3'};">${enrichment.fundingAvailable ? '\u5df2\u542f\u7528' : '\u672a\u542f\u7528'}</div>
          </div>
        </div>
        <div style="font-size:11px;color:#7e92b2;margin-top:6px;">\u6570\u636e\u6a21\u5f0f\uff1a${esc(enrichment.mode)}</div>
      </div>`;

    const experimentHtml = `
      <div style="margin-bottom:14px;">
        <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">\u5b9e\u9a8c\u8bb0\u5f55</div>
        <div style="font-size:12px;color:#b7c7e2;background:#141f2f;border-radius:6px;padding:8px;margin-bottom:6px;">
          <div>\u8fd0\u884c ID\uff1a${esc(String(experimentId || '--'))}</div>
          <div style="margin-top:4px;">\u72b6\u6001\uff1a${esc(String(experimentInfo?.status || '--'))}</div>
        </div>
        ${renderRunRows(experimentRuns)}
      </div>`;

    const lifecycleHtml = `
      <div style="margin-bottom:14px;">
        <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">\u5019\u9009\u751f\u547d\u5468\u671f</div>
        ${renderLifecycleRows(candidateLifecycle, '\u6682\u65e0\u5019\u9009\u751f\u547d\u5468\u671f\u8bb0\u5f55')}
      </div>
      <div style="margin-bottom:14px;">
        <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">\u65b9\u6848\u751f\u547d\u5468\u671f</div>
        ${renderLifecycleRows(proposalLifecycle, '\u6682\u65e0\u65b9\u6848\u751f\u547d\u5468\u671f\u8bb0\u5f55')}
      </div>`;
    const paramSensitivityHtml = `
      <details id="ai-param-sensitivity-details" class="ai-param-sensitivity-details">
        <summary>\u53c2\u6570\u654f\u611f\u6027\u5206\u6790</summary>
        <div id="ai-param-sensitivity" class="ai-param-sensitivity-panel">\u6b63\u5728\u52a0\u8f7d\u53c2\u6570\u654f\u611f\u6027\u5206\u6790...</div>
      </details>`;
    const liveActivateLabel = String(cand?.status || '') === 'live_candidate'
      ? '\u542f\u52a8\u5b9e\u76d8\u8fd0\u884c ->'
      : '\u5347\u7ea7\u4e3a\u5b9e\u76d8\u8fd0\u884c ->';
    const liveActivateHtml = canActivateLiveCandidate(cand)
      ? `<div style="margin-top:8px;">
          <button class="btn btn-sm" id="btn-activate-live" data-default-label="${esc(liveActivateLabel)}"
            style="font-size:12px;width:100%;color:#f0b429;border-color:#f0b429;">
            ${esc(liveActivateLabel)}
          </button>
          <div style="font-size:10px;color:#6b7fa0;margin-top:3px;">
            \u5c06\u5148\u5207\u6362\u7cfb\u7edf\u5230 live \u6a21\u5f0f\u5e76\u8981\u6c42\u8f93\u5165\u786e\u8ba4\u6587\u672c\uff0c\u786e\u8ba4\u540e\u624d\u4f1a\u771f\u6b63\u542f\u52a8\u8be5\u5019\u9009\u7684\u5b9e\u76d8\u8fd0\u884c\u3002
          </div>
         </div>`
      : '';

    panel.innerHTML = `
      <div style="margin-bottom:14px;">
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:6px;">
          <span style="font-size:15px;font-weight:700;color:#c2d0e8;">${esc(cand?.strategy || '--')}</span>
          <span class="cand-category-badge" style="background:${familyMeta.accent};color:${familyMeta.color};border:1px solid ${familyMeta.color}44;">${esc(familyMeta.label)}</span>
          ${searchRoleMeta ? `<span class="cand-category-badge" style="background:${searchRoleMeta.bg};color:${searchRoleMeta.fg};border:1px solid ${searchRoleMeta.border};">${esc(searchRoleMeta.label)}</span>` : ''}
          <span class="cand-score-badge ${color}" style="font-size:13px;">${score.toFixed(0)} \u5206</span>
        </div>
        <div style="font-size:12px;color:#7e92b2;">
          ${esc(cand?.symbol || '--')} / ${esc(cand?.timeframe || '--')} / ${esc(statusText(cand?.status))}
        </div>
        ${searchRoleMeta && championStrategy ? `<div style="font-size:11px;color:#7e92b2;margin-top:4px;">搜索角色：${esc(searchRoleMeta.label)}${cand?.metadata?.search_role === 'challenger' ? ` · 对照 champion ${esc(championStrategy)}` : ''}</div>` : ''}
        ${renderLifecycleStepper(cand?.status)}
      </div>

      <div style="margin-bottom:14px;">
        <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:8px;">\u7efc\u5408\u8bc4\u5206</div>
        ${scoreBar('\u8fb9\u9645\u4f18\u52bf', vs?.edge_score)}
        ${scoreBar('\u98ce\u9669\u63a7\u5236', vs?.risk_score)}
        ${scoreBar('\u4fe1\u53f7\u7a33\u5b9a\u6027', vs?.stability_score)}
        ${scoreBar('\u6267\u884c\u6548\u7387', vs?.efficiency_score)}
        ${scoreBar('\u7efc\u5408\u90e8\u7f72', vs?.deployment_score)}
        ${vs?.reasons?.length ? `<div style="font-size:11px;color:#6b7fa0;margin-top:6px;">\u8bf4\u660e\uff1a${esc(joinText(vs.reasons))}</div>` : ''}
      </div>

      ${autonomyHtml}
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
        \u26a0 \u8be5\u5019\u9009\u4e0e <strong>${esc(cand.metadata.correlated_with || '')}</strong> ${cand.metadata.correlation_is_cross_batch ? '\uff08\u5df2\u8fd0\u884c\u7b56\u7565\uff09' : ''}\u9ad8\u5ea6\u76f8\u5173
        (\u03c1 = ${(cand.metadata.correlation_value || 0).toFixed(2)})\uff0c\u5df2\u5728\u751f\u6210\u9636\u6bb5\u8fc7\u6ee4\uff0c\u907f\u514d\u91cd\u590d\u90e8\u7f72\u3002
      </div>` : ''}

      ${cand?.metadata?.llm_rationale ? `
      <div style="margin-bottom:14px;padding:10px 12px;background:#0f1e2e;border:1px solid #1e3a5a;border-radius:6px;">
        <div style="font-size:10px;color:#5b8fc4;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">\ud83e\udd16 AI \u89e3\u91ca</div>
        <div style="font-size:12px;color:#b0c4de;line-height:1.6;">${esc(cand.metadata.llm_rationale)}</div>
      </div>` : ''}

      ${(function(){
        const cs = cand?.metadata?.cusum_status;
        const triggered = cs?.triggered;
        const nBars = cs?.n_bars || 0;
        const msg   = cs?.message || '';
        const checkedAt = cs?.checked_at ? fmtTs(cs.checked_at) : '';
        const statusHtml = cs
          ? `<div style="display:flex;align-items:center;gap:8px;margin-bottom:4px;">
              <span style="font-size:12px;font-weight:700;color:${triggered ? '#e05260' : '#20bf78'};">${triggered ? '\u26a0 \u5df2\u89e6\u53d1\u8870\u51cf' : '\u2713 \u8fd0\u884c\u6b63\u5e38'}</span>
              <span style="font-size:11px;color:#6b7fa0;">${nBars} \u6839K\u7ebf</span>
            </div>
            <div style="font-size:11px;color:#7e92b2;">${esc(msg)}</div>
            ${checkedAt ? `<div style="font-size:10px;color:#4a5f7a;margin-top:3px;">\u68c0\u6d4b\u65f6\u95f4 ${checkedAt}</div>` : ''}`
          : `<div style="font-size:12px;color:#5b7a9a;">\u5c1a\u672a\u68c0\u6d4b\u3002\u70b9\u51fb\u6309\u94ae\u5bf9\u5df2\u6ce8\u518c\u7b56\u7565\u6267\u884c CUSUM \u8870\u51cf\u5206\u6790\u3002</div>`;
        return `<div style="margin-bottom:14px;">
          <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px;">
            <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;">\u7b56\u7565\u8870\u51cf\u68c0\u6d4b (CUSUM)</div>
            <button class="btn btn-sm" id="btn-decay-check" style="font-size:11px;padding:2px 8px;">\u68c0\u67e5\u8870\u51cf</button>
          </div>
          ${statusHtml}
        </div>`;
      })()}

      <div style="margin-bottom:14px;">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px;">
          <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;">\u5b9e\u76d8/\u7eb8\u76d8\u6027\u80fd\u5386\u53f2</div>
          <button class="btn btn-sm" id="btn-load-perf-history" style="font-size:11px;padding:2px 8px;" data-candidate-id="${esc(candidateId)}">\u52a0\u8f7d</button>
        </div>
        <div id="perf-history-panel" style="font-size:12px;color:#6b7fa0;">\u70b9\u51fb\u52a0\u8f7d\u67e5\u770b\u7b56\u7565\u8fd0\u884c\u6027\u80fd\u5feb\u7167</div>
      </div>

      <div style="margin-bottom:14px;">
        <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:8px;">Top \u56de\u6d4b\u7ed3\u679c</div>
        <div style="overflow-x:auto;">
          <table class="data-table" style="font-size:12px;">
            <thead><tr><th>#</th><th>\u7b56\u7565</th><th>\u5468\u671f</th><th>\u5e74\u5316</th><th>\u590f\u666e</th><th>\u56de\u64a4</th></tr></thead>
            <tbody>${topRows || '<tr><td colspan="6" style="color:#6b7fa0;">\u6682\u65e0\u6570\u636e</td></tr>'}</tbody>
          </table>
        </div>
      </div>

      ${(function(){
        if (!governanceEnabled() || !cand?.metadata?.promotion_pending_human_gate) return '';
        const recTarget = esc(cand?.metadata?.recommended_runtime_target || decision || 'paper');
        return `<div style="margin-bottom:14px;padding:10px 12px;background:#1a0f00;border:2px solid #f59e0b;border-radius:6px;">
          <div style="font-size:11px;color:#f59e0b;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">\u23f3 \u5f85\u4eba\u5de5\u5ba1\u6279</div>
          <div style="font-size:12px;color:#c2d0e8;margin-bottom:8px;">
            AI\u63a8\u8350\u76ee\u6807\uff1a<strong style="color:#f59e0b;">${recTarget}</strong>
          </div>
          <div class="form-group" style="margin-bottom:8px;">
            <label style="font-size:11px;color:#9fb1c9;">\u8fd0\u884c\u76ee\u6807</label>
            <select id="approval-target-select" style="width:100%;font-size:12px;">
              <option value="paper" ${recTarget === 'paper' ? 'selected' : ''}>\u7eb8\u76d8 (paper)</option>
              <option value="live_candidate" ${recTarget === 'live_candidate' ? 'selected' : ''}>\u5b9e\u76d8\u5019\u9009 (live_candidate)</option>
            </select>
          </div>
          <div class="form-group" style="margin-bottom:8px;">
            <label style="font-size:11px;color:#9fb1c9;">\u5ba1\u6279\u5907\u6ce8</label>
            <input type="text" id="approval-notes-input" placeholder="\u5ba1\u6279\u5907\u6ce8\uff08\u53ef\u9009\uff09" style="width:100%;font-size:12px;">
          </div>
          <div style="display:flex;gap:8px;">
            <button id="btn-human-approve" class="btn" style="flex:1;font-size:12px;color:#20bf78;border-color:#20bf78;">\u2713 \u6279\u51c6</button>
            <button id="btn-human-reject" class="btn" style="flex:1;font-size:12px;color:#e05260;border-color:#e05260;">\u2717 \u62d2\u7edd</button>
          </div>
        </div>`;
      })()}

      <div style="margin-bottom:16px;">
        <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">AI \u5efa\u8bae</div>
        <div style="font-size:13px;color:#c2d0e8;margin-bottom:3px;">${esc(promotionText(decision))}</div>
        ${promo?.reason ? `<div style="font-size:12px;color:#7e92b2;">${esc(promo.reason)}</div>` : ''}
      </div>

      ${showRegisterButton
        ? `<button class="btn-register-cta full" data-action="open-register" data-candidate-id="${esc(candidateId)}">
            4) 注册/部署 →
          </button>`
        : (governanceGateHint
          ? `<div style="font-size:12px;color:#f0b429;background:#2b1f06;border:1px solid #5c4310;border-radius:6px;padding:8px 10px;">
              \u6cbb\u7406\u6a21\u5f0f\u5df2\u5f00\u542f\uff1a\u8bf7\u4f7f\u7528\u4e0a\u65b9\u201c\u5f85\u4eba\u5de5\u5ba1\u6279\u201d\u8fdb\u884c\u6279\u51c6/\u62d2\u7edd\u3002
            </div>`
          : '')}

      <div style="margin-bottom:14px;">
        <button class="btn btn-sm" id="btn-order-preview" style="font-size:12px;width:100%;">
          \u751f\u6210\u8ba2\u5355\u9884\u89c8
        </button>
        <div id="ai-order-preview-result" style="display:none;margin-top:10px;padding:12px;background:#0d1a2a;border:1px solid #1e3a5a;border-radius:8px;"></div>
      </div>
      ${canActivateLiveCandidate(cand)
          ? (() => { /*
          const activateLabel = String(cand?.status || '') === 'live_candidate'
            ? '启动实盘运行 →'
            : '升级为实盘运行 →';
          return `<div style="margin-top:8px;">
            <button class="btn btn-sm" id="btn-activate-live" data-default-label="${esc(activateLabel)}"
              style="font-size:12px;width:100%;color:#f0b429;border-color:#f0b429;">
              ${esc(activateLabel)}
            </button>
            <div style="font-size:10px;color:#6b7fa0;margin-top:3px;">
              灏嗗厛鍒囨崲绯荤粺鍒?live 模式骞惰姹傝緭鍏ョ‘璁ゆ枃鏈紝纭鍚庢墠浼氱湡姝ｅ惎鍔ㄨ鍊欓€夌殑实盘运行銆?
            </div>
           </div>`;
        */ return liveActivateHtml; })()
        : ''}
      `;
    panel.innerHTML = normalizeUiText(panel.innerHTML)
      .replace(' (Best Params)', '')
      .replace('CSV:', 'CSV 文件：')
      .replace('Markdown:', 'Markdown 报告：')
      .replace('DSR Score', 'DSR 分数')
      .replace('WF Consistency', 'WF 一致性')
      .replace('folds+', '折叠中');
    panel.dataset.candidateId = String(candidateId);
    normalizeDomText(panel);
    if (window.innerWidth <= 1280) {
      panel.closest('.ai-hub-detail')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
    const cachedPerf = state.perfHistoryCache[String(candidateId)];
    if (cachedPerf) {
      const perfPanel = panel.querySelector('#perf-history-panel');
      const perfBtn = panel.querySelector('#btn-load-perf-history');
      if (perfPanel) {
        if (cachedPerf.kind === 'html') perfPanel.innerHTML = cachedPerf.content;
        else perfPanel.textContent = cachedPerf.content;
      }
      if (perfBtn) perfBtn.textContent = '刷新';
    }
    bindParamSensitivity(candidateId);

    // 订单预览按钮
    panel.querySelector('#btn-order-preview')?.addEventListener('click', () => {
      showOrderPreview(candidateId);
    });

    /* panel.querySelector('#btn-activate-live')?.addEventListener('click', async () => {
      const btn = panel.querySelector('#btn-activate-live');
      const defaultLabel = String(btn?.dataset?.defaultLabel || btn?.textContent || '鍚姩实盘运行 鈫?);
      if (btn) {
        btn.textContent = '姝ｅ湪鍚姩实盘...';
        btn.disabled = true;
      }
      try {
        const result = await activateCandidateLive(candidateId);
        if (!result || result.cancelled) {
          if (btn) {
            btn.textContent = defaultLabel;
            btn.disabled = false;
          }
          return;
        }
        const strategyName = String(result?.registered_strategy_name || result?.runtime_status || 'live_running');
        notify(`鍊欓€夊凡鍚姩实盘运行: ${strategyName}`);
        await refreshWorkbench('', candidateId);
      } catch (err) {
        if (btn) {
          btn.textContent = defaultLabel;
          btn.disabled = false;
        }
        notify(`鍚姩实盘失败: ${err.message}`, true);
      }
    }); */

    panel.querySelector('#btn-activate-live')?.addEventListener('click', async () => {
      const btn = panel.querySelector('#btn-activate-live');
      const defaultLabel = String(
        btn?.dataset?.defaultLabel
        || btn?.textContent
        || '\u542f\u52a8\u5b9e\u76d8\u8fd0\u884c ->'
      );
      if (btn) {
        btn.textContent = '\u6b63\u5728\u542f\u52a8\u5b9e\u76d8...';
        btn.disabled = true;
      }
      try {
        const result = await activateCandidateLive(candidateId);
        if (!result || result.cancelled) {
          if (btn) {
            btn.textContent = defaultLabel;
            btn.disabled = false;
          }
          return;
        }
        const strategyName = String(result?.registered_strategy_name || result?.runtime_status || 'live_running');
        notify(`\u5019\u9009\u5df2\u542f\u52a8\u5b9e\u76d8\u8fd0\u884c: ${strategyName}`);
        await refreshWorkbench('', candidateId);
      } catch (err) {
        if (btn) {
          btn.textContent = defaultLabel;
          btn.disabled = false;
        }
        notify(`\u542f\u52a8\u5b9e\u76d8\u5931\u8d25: ${err.message}`, true);
      }
    });

    // \u7ed1\u5b9a\u8be6\u60c5\u9762\u677f\u91cc\u7684\u6309\u94ae
    panel.querySelector('.btn-register-cta')?.addEventListener('click', () => {
      openRegisterModal(candidateId).catch(err => notify(`\u6253\u5f00\u6ce8\u518c\u5931\u8d25: ${err.message}`, true));
    });

    // 人工纭鎸夐挳
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
          notify(`已批准策略候选（${target}）`);
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
          notify('候选已拒绝');
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
            const emptyText = '暂无性能快照（策略运行后自动记录）';
            perfPanel.textContent = emptyText;
            state.perfHistoryCache[String(candidateId)] = { kind: 'text', content: emptyText };
          } else {
            const reversed = [...snaps].reverse();
            const pnlSeries = reversed.map(s => Number(s.total_pnl_pct || 0));
            const perfHtml = `
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
            perfPanel.innerHTML = perfHtml;
            state.perfHistoryCache[String(candidateId)] = { kind: 'html', content: perfHtml };
          }
        } catch (err) {
          if (perfPanel) perfPanel.textContent = `加载失败: ${String(err?.message || err)}`;
        } finally {
          perfHistBtn.textContent = '刷新';
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
    emitWorkbenchState('candidate-detail', {
      candidateId: String(candidateId || ''),
      proposalId,
      experimentId,
    });
  }

  /* 鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹?
     涓€閿敞鍐?Modal
  鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹?*/
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
      notify('\u6cbb\u7406\u6a21\u5f0f\u5df2\u5f00\u542f\uff0c\u8bf7\u5148\u5728\u4eba\u5de5\u5ba1\u6279\u4e2d\u6279\u51c6\u540e\u518d\u6ce8\u518c\u3002', true);
      return;
    }
    const modal = document.getElementById('ai-register-modal');
    const body  = document.getElementById('ai-register-body');
    if (!modal || !body) return;
    modal.style.display = 'flex';
    body.innerHTML = '<div style="padding:20px;color:#7e92b2;">加载中...</div>';

    let resp;
    try {
      resp = await aiApi(`/candidates/${encodeURIComponent(candidateId)}`, { timeoutMs: 20000 });
    } catch (err) {
      modal.style.display = 'none';
      throw err;
    }
    const cand   = resp?.candidate || {};
    const top    = candidateTopResults(cand)[0] || {};
    const decision = cand?.promotion?.decision || cand?.promotion_target || 'paper';
    const runtimeTradingMode = currentTradingMode();
    const defaultRegisterMode = runtimeTradingMode === 'live'
      ? 'live_candidate'
      : (decision === 'live_candidate' ? 'live_candidate' : 'paper');
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
    const defaultAllocationPercent = parseAllocationPercentInput(
      Number(cand?.metadata?.allocation_pct || 0.05) * 100,
      5,
    );

    function metricBox(label, value, cls = '') {
      return `<div class="ai-rm-item">
        <div class="ai-rm-label">${label}</div>
        <div class="ai-rm-value ${cls}">${value}</div>
      </div>`;
    }

    const liveModeHint = runtimeTradingMode === 'live'
      ? '当前系统运行在 live 模式，默认改为“实盘候选”，避免直接触发纸盘注册失败。'
      : '';

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
        ${metricBox('夏普偏斜', sr != null ? sr.toFixed(2) : '--')}
      </div>
      <div class="form-group">
        <label>运行模式</label>
        <div class="ai-mode-radio-group">
          <label><input type="radio" name="reg-mode" value="paper" ${decision === 'paper' || !['live_candidate'].includes(decision) ? 'checked' : ''}> 纸盘（推荐，低风险模拟）</label>
          <label><input type="radio" name="reg-mode" value="live_candidate" ${decision === 'live_candidate' ? 'checked' : ''}> 实盘候选（待人工确认）</label>
        </div>
      </div>
      <div class="form-group">
        <label>部署仓位（%）</label>
        <input type="number" id="reg-allocation-percent" value="${esc(String(Math.round(defaultAllocationPercent)))}" min="1" max="100" step="1">
      </div>
      <div style="display:flex;justify-content:flex-end;gap:10px;margin-top:16px;padding-top:12px;border-top:1px solid rgba(255,255,255,.07);">
        <button class="btn" id="reg-cancel-btn">取消</button>
        <button class="btn-register-cta" id="reg-confirm-btn" data-candidate-id="${esc(candidateId)}">确认注册/部署</button>
      </div>`;
    body.innerHTML = normalizeUiText(body.innerHTML);
    normalizeDomText(body);

    document.getElementById('reg-cancel-btn').onclick  = () => { modal.style.display = 'none'; };
    const paperMode = body.querySelector('input[name="reg-mode"][value="paper"]');
    const liveCandidateMode = body.querySelector('input[name="reg-mode"][value="live_candidate"]');
    if (paperMode) paperMode.checked = defaultRegisterMode === 'paper';
    if (liveCandidateMode) liveCandidateMode.checked = defaultRegisterMode === 'live_candidate';
    if (runtimeTradingMode === 'live') {
      const modeGroup = body.querySelector('.ai-mode-radio-group');
      if (modeGroup && !body.querySelector('[data-register-live-hint="true"]')) {
        const hint = document.createElement('div');
        hint.setAttribute('data-register-live-hint', 'true');
        hint.style.marginTop = '10px';
        hint.style.color = '#fcd34d';
        hint.style.fontSize = '12px';
        hint.textContent = liveModeHint;
        modeGroup.insertAdjacentElement('afterend', hint);
      }
    }
    document.getElementById('reg-confirm-btn').onclick = () => {
      const name = String(document.getElementById('reg-name')?.value || '').trim();
      const mode = document.querySelector('input[name="reg-mode"]:checked')?.value || defaultRegisterMode;
      const allocationInput = document.getElementById('reg-allocation-percent');
      const allocationPercent = parseAllocationPercentInput(allocationInput?.value, defaultAllocationPercent);
      if (allocationInput) allocationInput.value = String(Math.round(allocationPercent));
      confirmRegister(candidateId, mode, name, allocationPercent / 100);
    };
  }

  async function confirmRegister(candidateId, mode, name, allocationPct) {
    if (governanceEnabled()) {
      notify('已开启人工确认，请改用人工确认流程', true);
      return;
    }
    const btn = document.getElementById('reg-confirm-btn');
    if (btn) { btn.textContent = '注册中...'; btn.disabled = true; }
    try {
      const result = await aiApi(`/candidates/${encodeURIComponent(candidateId)}/register`, {
        method: 'POST',
        body: JSON.stringify({
          mode,
          name: name || undefined,
          allocation_pct: Number.isFinite(Number(allocationPct)) ? Number(allocationPct) : undefined,
        }),
        timeoutMs: 30000,
      });
      document.getElementById('ai-register-modal').style.display = 'none';
      const stratName = result?.registered_strategy_name || result?.runtime_status || mode;
      notify(`策略已注册: ${stratName}`);
      await refreshWorkbench('', candidateId);
    } catch (err) {
      if (btn) { btn.textContent = '确认注册/部署'; btn.disabled = false; }
      notify(`注册失败: ${err.message}`, true);
    }
  }

  /* 鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹?
     人工纭闃熷垪
  鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹?*/
  async function cancelModeSwitchToken(token) {
    const safeToken = String(token || '').trim();
    if (!safeToken) return false;
    try {
      await rootApi(`/trading/mode/cancel?token=${encodeURIComponent(safeToken)}`, {
        method: 'POST',
        timeoutMs: 10000,
      });
      return true;
    } catch (err) {
      console.debug('cancelModeSwitchToken failed:', err);
      return false;
    }
  }

  /* async function activateCandidateLive(candidateId) {
    const safeCandidateId = String(candidateId || '').trim();
    if (!safeCandidateId) throw new Error('缂哄皯 candidate_id');
    const candidate = state.candidates.find(item => String(item?.candidate_id || '') === safeCandidateId) || null;
    const notePrompt = candidate
      ? `纭灏嗗€欓€?${safeCandidateId.slice(0, 8)} 鍚姩涓哄疄鐩樿繍琛岋紵\n策略锛?{candidate.strategy || '--'} / ${candidate.symbol || '--'} / ${candidate.timeframe || '--'}\n\n璇疯緭鍏ュ娉紙鍙暀绌猴紝鐐瑰彇娑堝垯缁堟鏈鎿嶄綔锛夛細`
      : `纭灏嗗€欓€?${safeCandidateId.slice(0, 8)} 鍚姩涓哄疄鐩樿繍琛岋紵\n\n璇疯緭鍏ュ娉紙鍙暀绌猴紝鐐瑰彇娑堝垯缁堟鏈鎿嶄綔锛夛細`;
    const notes = window.prompt(notePrompt, '');
    if (notes === null) return { cancelled: true };

    const modeSnapshot = await rootApi('/trading/mode', { timeoutMs: 15000 });
    const currentMode = String(modeSnapshot?.mode || 'paper').trim().toLowerCase();
    if (currentMode !== 'live') {
      const switchRequest = await rootApi('/trading/mode/request', {
        method: 'POST',
        body: JSON.stringify({
          target_mode: 'live',
          reason: `activate AI research candidate ${safeCandidateId} for live trading`,
        }),
        timeoutMs: 15000,
      });
      const token = String(switchRequest?.token || '').trim();
      const confirmHint = String(
        switchRequest?.confirm_text
        || modeSnapshot?.confirm_hint
        || 'CONFIRM LIVE TRADING'
      ).trim();
      if (!token) throw new Error('鍒囨崲鍒板疄鐩樻椂鏈繑鍥炵‘璁や护鐗?);
      const confirmInput = window.prompt(
        `绯荤粺褰撳墠浠嶅湪纸盘模式锛屽繀椤诲厛鍒囨崲鍒板疄鐩樻ā寮忋€俓n璇疯緭鍏ョ‘璁ゆ枃鏈互缁х画锛歕n${confirmHint}`,
        confirmHint,
      );
      if (confirmInput === null) {
        await cancelModeSwitchToken(token);
        return { cancelled: true };
      }
      if (String(confirmInput).trim() !== confirmHint) {
        await cancelModeSwitchToken(token);
        throw new Error('纭鏂囨湰涓嶅尮閰嶏紝宸插彇娑堝垏鎹㈠埌实盘');
      }
      try {
        await rootApi('/trading/mode/confirm', {
          method: 'POST',
          body: JSON.stringify({ token, confirm_text: confirmHint }),
          timeoutMs: 45000,
        });
      } catch (err) {
        await cancelModeSwitchToken(token);
        throw err;
      }
    }

    return aiApi(`/candidates/${encodeURIComponent(safeCandidateId)}/activate-live`, {
      method: 'POST',
      body: JSON.stringify({ notes: String(notes || '') }),
      timeoutMs: 45000,
    });
  } */

  async function activateCandidateLive(candidateId) {
    const safeCandidateId = String(candidateId || '').trim();
    if (!safeCandidateId) throw new Error('\u7f3a\u5c11 candidate_id');
    const candidate = state.candidates.find(item => String(item?.candidate_id || '') === safeCandidateId) || null;
    const notePrompt = candidate
      ? `\u786e\u8ba4\u5c06\u5019\u9009 ${safeCandidateId.slice(0, 8)} \u542f\u52a8\u4e3a\u5b9e\u76d8\u8fd0\u884c\uff1f\n\u7b56\u7565\uff1a${candidate.strategy || '--'} / ${candidate.symbol || '--'} / ${candidate.timeframe || '--'}\n\n\u8bf7\u8f93\u5165\u5907\u6ce8\uff08\u53ef\u7559\u7a7a\uff0c\u70b9\u53d6\u6d88\u5219\u7ec8\u6b62\u672c\u6b21\u64cd\u4f5c\uff09\uff1a`
      : `\u786e\u8ba4\u5c06\u5019\u9009 ${safeCandidateId.slice(0, 8)} \u542f\u52a8\u4e3a\u5b9e\u76d8\u8fd0\u884c\uff1f\n\n\u8bf7\u8f93\u5165\u5907\u6ce8\uff08\u53ef\u7559\u7a7a\uff0c\u70b9\u53d6\u6d88\u5219\u7ec8\u6b62\u672c\u6b21\u64cd\u4f5c\uff09\uff1a`;
    const notes = window.prompt(notePrompt, '');
    if (notes === null) return { cancelled: true };

    const modeSnapshot = await rootApi('/trading/mode', { timeoutMs: 15000 });
    const currentMode = String(modeSnapshot?.mode || 'paper').trim().toLowerCase();
    if (currentMode !== 'live') {
      const switchRequest = await rootApi('/trading/mode/request', {
        method: 'POST',
        body: JSON.stringify({
          target_mode: 'live',
          reason: `activate AI research candidate ${safeCandidateId} for live trading`,
        }),
        timeoutMs: 15000,
      });
      const token = String(switchRequest?.token || '').trim();
      const confirmHint = String(
        switchRequest?.confirm_text
        || modeSnapshot?.confirm_hint
        || 'CONFIRM LIVE TRADING'
      ).trim();
      if (!token) throw new Error('\u5207\u6362\u5230\u5b9e\u76d8\u65f6\u672a\u8fd4\u56de\u786e\u8ba4\u4ee4\u724c');
      const confirmInput = window.prompt(
        `\u7cfb\u7edf\u5f53\u524d\u4ecd\u5728\u7eb8\u76d8\u6a21\u5f0f\uff0c\u5fc5\u987b\u5148\u5207\u6362\u5230\u5b9e\u76d8\u6a21\u5f0f\u3002\n\u8bf7\u8f93\u5165\u786e\u8ba4\u6587\u672c\u4ee5\u7ee7\u7eed\uff1a\n${confirmHint}`,
        confirmHint,
      );
      if (confirmInput === null) {
        await cancelModeSwitchToken(token);
        return { cancelled: true };
      }
      if (String(confirmInput).trim() !== confirmHint) {
        await cancelModeSwitchToken(token);
        throw new Error('\u786e\u8ba4\u6587\u672c\u4e0d\u5339\u914d\uff0c\u5df2\u53d6\u6d88\u5207\u6362\u5230\u5b9e\u76d8');
      }
      try {
        await rootApi('/trading/mode/confirm', {
          method: 'POST',
          body: JSON.stringify({ token, confirm_text: confirmHint }),
          timeoutMs: 45000,
        });
      } catch (err) {
        await cancelModeSwitchToken(token);
        throw err;
      }
    }

    return aiApi(`/candidates/${encodeURIComponent(safeCandidateId)}/activate-live`, {
      method: 'POST',
      body: JSON.stringify({ notes: String(notes || '') }),
      timeoutMs: 45000,
    });
  }

  async function loadPendingApprovals() {
    try {
      const res = await aiApi('/candidates/pending-approvals', { timeoutMs: 30000 });
      state.pendingApprovals = toArray(res?.items);
      renderApprovalQueue();
      emitWorkbenchState('pending-approvals');
    } catch (err) {
      // Non-fatal 鈥?approval queue is best-effort
      console.debug('loadPendingApprovals failed:', err);
    }
  }

  function renderApprovalQueue() {
    const card = document.getElementById('ai-approval-card');
    const list = document.getElementById('ai-approval-list');
    const badge = document.getElementById('ai-approval-badge');
    if (!card || !list) return;

    if (!governanceEnabled()) {
      card.style.display = 'none';
      if (badge) badge.textContent = '';
      list.innerHTML = '';
      return;
    }

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
            data-action="view-candidate" data-candidate-id="${cid}">第 4 步处理</button>
        </div>
      </div>`;
    }).join(''));
    normalizeDomText(list);
  }

  /* 鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹?
     LLM 辅助研究鐟欏嫬鍨?
  鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹?*/
  async function generateAIContext() {
    setAIContextButtonState('working');
    try {
      await ensureAutoPlannerGoal({ silent: true });
      const goals = String(document.getElementById('ai-planner-goal')?.value || '').trim();
      if (goals.length < 8) {
        notify('请先填写研究目标（至少8个字符）', true);
        setAIContextButtonState('idle');
        return;
      }
      const macroContext = await loadPlannerMacroContext().catch(() => null);
      const marketSummary = { signals: state.latestSignals || {}, macro: macroContext || {} };
      const result = await aiApi('/research/generate-context', {
        method: 'POST',
        body: JSON.stringify({ market_summary: marketSummary, goals, timeout: 180 }),
        timeoutMs: 240000,
      });
      if (result?.llm_research_output) {
        state.pendingLlmContext = result.llm_research_output;
        const draftCount = toArray(result.llm_research_output?.proposed_strategy_changes).filter(item => item && typeof item === 'object').length;
        const hypothesis = String(result.llm_research_output.hypothesis || '').trim();
        const uncertainty = String(result.llm_research_output.uncertainty || '').trim().toLowerCase();
        const suggestedMax = ['high'].includes(uncertainty) ? 3 : (['low'].includes(uncertainty) ? 6 : 4);
        if (macroContext) state.pendingMacroContext = macroContext;
        const goalInput = document.getElementById('ai-planner-goal');
        if (goalInput && hypothesis) {
          const baseGoals = goals.replace(/；AI假设：[\s\S]*$/, '').replace(/\s+/g, ' ').trim();
          goalInput.value = `${baseGoals}；AI假设：${hypothesis}`.slice(0, 580);
        }
        const maxTemplatesEl = document.getElementById('ai-planner-max-templates');
        if (maxTemplatesEl) maxTemplatesEl.value = String(suggestedMax);
        const maxDraftsEl = document.getElementById('ai-planner-max-drafts');
        if (maxDraftsEl) {
          const nextDraftBudget = Math.max(plannerNumberInput('ai-planner-max-drafts', 4, 1, 12), draftCount || 0, 4);
          maxDraftsEl.value = String(nextDraftBudget);
        }
        const maxBacktestsEl = document.getElementById('ai-planner-max-backtests');
        if (maxBacktestsEl) {
          const suggestedBacktests = Math.max(plannerNumberInput('ai-planner-max-backtests', 80, 8, 500), draftCount * 20, 60);
          maxBacktestsEl.value = String(suggestedBacktests);
        }
        const researchModeEl = document.getElementById('ai-planner-research-mode');
        if (researchModeEl && (researchModeEl.value === 'auto' || !researchModeEl.value)) {
          researchModeEl.value = draftCount > 0 ? 'autonomous_draft' : 'hybrid';
        }
        setAIContextButtonState('ready');
        // Show hypothesis in planner notes
        const plannerNotesEl = document.getElementById('ai-planner-notes');
        const macroSummary = macroContext
          ? `宏观摘要：Funding ${macroContext?.microstructure?.funding_rate ?? '--'} / Basis ${macroContext?.microstructure?.basis_pct ?? '--'} / 巨鲸 ${macroContext?.community?.whale_count ?? 0} / News ${macroContext?.news?.events_count ?? 0}`
          : '未获取宏观数据';
        if (plannerNotesEl && result.llm_research_output.hypothesis) {
          const existing = plannerNotesEl.innerHTML;
          const draftSummary = draftCount > 0
            ? `<div style="font-size:11px;color:#7dd3fc;margin-bottom:3px;">OpenAI 草案：${draftCount} 个，生成提案时将优先进入开放式草案研究。</div>`
            : '<div style="font-size:11px;color:#9fb1c9;margin-bottom:3px;">本轮只生成了研究假设与实验计划，尚未返回可执行草案。</div>';
          plannerNotesEl.innerHTML = `<div style="font-size:11px;color:#20bf78;margin-bottom:3px;">AI假设：${esc(result.llm_research_output.hypothesis)}</div>${draftSummary}` + existing;
        }
        updatePlannerModeHint();
        notify(draftCount > 0 ? `研究思路已生成，并附带 ${draftCount} 个 AI 草案。` : '研究思路已生成，假设已写入规划区。');
      } else {
        notify(`生成研究思路失败: ${result?.error || 'LLM不可用'}`, true);
        setAIContextButtonState('idle');
      }
    } catch (err) {
      notify(`生成研究思路失败: ${err.message}`, true);
      setAIContextButtonState('idle');
    }
  }

  /* ──────────────────────────────
     数据加载
  ────────────────────────────── */
  async function loadRuntimeConfig(force = false) {
    if (!force && state.runtimeConfigLoaded && state.runtimeConfig) return;
    const prevGovernance = !!(state.runtimeConfig && state.runtimeConfig.governance_enabled);
    try {
      const res = await aiApi('/runtime-config', { timeoutMs: 30000 });
      state.runtimeConfig = {
        governance_enabled: !!res?.governance_enabled,
        decision_mode: String(res?.decision_mode || ''),
        trading_mode: String(res?.trading_mode || ''),
        ai_live_decision: res?.ai_live_decision || null,
        ai_autonomous_agent: res?.ai_autonomous_agent || null,
      };
      state.runtimeConfigLoaded = true;
      renderRuntimeSummary();
      renderLiveDecisionRuntimeConfig();
      const nextGovernance = !!state.runtimeConfig.governance_enabled;
      if (prevGovernance !== nextGovernance) {
        renderCandidateCards();
        if (state.selectedCandidateId) {
          viewCandidate(state.selectedCandidateId, { keepContent: true }).catch(() => {});
        } else {
          renderCandidateDetailPlaceholder(state.selectedProposalId);
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
          ai_autonomous_agent: null,
        };
      }
      state.runtimeConfigLoaded = true;
      renderRuntimeSummary();
      renderLiveDecisionRuntimeConfig();
      console.debug('loadRuntimeConfig failed:', err);
    }
  }

  async function loadProposals(selectId = '') {
    const preservedProposalId = String(selectId || state.selectedProposalId || proposalIdForCandidate(state.selectedCandidateId) || '').trim();
    const preservedProposal = findProposalById(preservedProposalId);
    const res = await aiApi('/proposals?limit=50', { timeoutMs: 60000 });
    state.proposals = sortProposalsForWorkbench(
      toArray(res?.items).map((item, index) => normalizeProposalPresentation(item, index)),
    );
    if (preservedProposal && !findProposalById(preservedProposalId)) {
      state.proposals = sortProposalsForWorkbench(
        [normalizeProposalPresentation(preservedProposal, 0), ...state.proposals],
        preservedProposalId,
      );
    }
    if (selectId) state.selectedProposalId = selectId;
    syncSelectedProposal(selectId);
    renderProposalList();
    updateRunBtn();
  }

  async function loadCandidates(selectId = '') {
    const res = await aiApi('/candidates?limit=50', { timeoutMs: 60000 });
    state.candidates = toArray(res?.items);
    if (selectId) state.selectedCandidateId = selectId;
    if (state.selectedCandidateId && !findCandidateById(state.selectedCandidateId)) {
      state.selectedCandidateId = '';
    }
    syncSelectedProposal();
    renderCandidateCards();
  }

  async function refreshWorkbench(selectProposalId = '', selectCandidateId = '') {
    return runStateSingleFlight('refreshWorkbenchInFlight', async () => {
      const ancillaryTasks = [
        loadRuntimeConfig().catch((err) => {
          console.debug('loadRuntimeConfig(refreshWorkbench) failed:', err);
          return null;
        }),
        loadPendingApprovals().catch((err) => {
          console.debug('loadPendingApprovals(refreshWorkbench) failed:', err);
          return null;
        }),
        loadAgentStatus().catch(() => null),
        loadLiveDecisionActivitySummary().catch(() => null),
      ];

      await Promise.allSettled([
        loadProposals(selectProposalId),
        loadCandidates(selectCandidateId),
      ]);
      mergeCandidateFallbackProposals();
      const autoSelectedCandidateId = applyWorkbenchSelection(selectProposalId);
      if (autoSelectedCandidateId) {
        await viewCandidate(autoSelectedCandidateId, { keepContent: true }).catch(() => {
          renderCandidateDetailPlaceholder(state.selectedProposalId);
        });
      }
      normalizeDomText(document.getElementById('ai-research'));
      emitWorkbenchState('refresh-workbench');
      await Promise.allSettled(ancillaryTasks);
      normalizeDomText(document.getElementById('ai-research'));
      emitWorkbenchState('refresh-workbench-meta');
    });
  }

  function hasActionLock() {
    const locks = { ...DEFAULT_ACTION_LOCKS, ...(state.actionLocks || {}) };
    return Object.values(locks).some(Boolean);
  }

  function syncPrimaryActionButtons() {
    const generateBtn = document.getElementById('ai-generate-btn');
    const oneClickBtn = document.getElementById('ai-oneclick-btn');
    const hintEl = document.getElementById('ai-flow-hint');
    const busy = hasActionLock();
    if (generateBtn && !state.actionLocks.generate) generateBtn.disabled = busy;
    if (oneClickBtn && !state.actionLocks.oneclick) oneClickBtn.disabled = busy;
    if (hintEl) {
      hintEl.textContent = busy
        ? '当前有步骤正在执行，请等待当前流程完成后再继续下一步。'
        : FLOW_HINT_QUICK_PATH;
    }
    updateRunBtn();
    updateClearQueueButton();
    updateExitRunningQueueButton();
    updateClearCandidatesButton();
    emitWorkbenchState('action-locks');
  }

  function setActionLock(name, locked) {
    state.actionLocks = { ...DEFAULT_ACTION_LOCKS, ...(state.actionLocks || {}) };
    state.actionLocks[name] = !!locked;
    syncPrimaryActionButtons();
  }

  async function withActionLock(name, fn) {
    setActionLock(name, true);
    try {
      return await fn();
    } finally {
      setActionLock(name, false);
    }
  }

  function updateRunBtn() {
    const btn = document.getElementById('run-selected-btn');
    if (!btn) return;
    const busy = hasActionLock();
    const has = !!state.selectedProposalId;
    const proposal = state.proposals.find((item) => String(item?.proposal_id || '').trim() === String(state.selectedProposalId || '').trim()) || null;
    const status = String(proposal?.status || '').trim();
    const virtualProposal = isVirtualProposal(proposal);
    const runnable = proposal ? isRunnableProposalStatus(status) : has;
    btn.textContent = ['research_queued', 'research_running'].includes(status) ? '3) 运行中...' : '3) 运行研究';
    btn.disabled = !has || busy || !runnable || virtualProposal;
    if (busy) {
      btn.title = '当前有任务执行中，请等待完成后再运行研究';
      return;
    }
    if (!has) {
      btn.title = '请先在左侧选择研究任务';
      return;
    }
    if (virtualProposal) {
      btn.title = '该条目由候选结果回填，缺少原始研究任务，不能直接运行；请查看候选详情或重新生成提案';
      return;
    }
    if (!runnable) {
      btn.title = ['research_queued', 'research_running'].includes(status)
        ? `当前提案正在运行：${state.selectedProposalId}`
        : `当前状态 ${statusText(status)} 不可重复运行`;
      return;
    }
    btn.title = `运行研究：${state.selectedProposalId}`;
  }

  /* 鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹?
     鎿嶄綔鍑芥暟
  鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹?*/
  /* -- \u56de\u653e\u5e02\u573a\u4e0a\u4e0b\u6587\u91c7\u96c6\uff08\u751f\u6210\u7814\u7a76\u524d\u81ea\u52a8\u6267\u884c\uff09 -- */
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
    await ensureAutoPlannerGoal({ silent: true });
    const goal = String(document.getElementById('ai-planner-goal')?.value || '').trim();
    if (goal.length < 8) { notify('研究目标太短（至少8个字符）', true); return; }
    const symbols   = csvInput('ai-planner-symbols');
    const primarySym = symbols[0] || getCurrentResearchSymbol() || 'BTC/USDT';
    const plannerConstraints = buildPlannerConstraints();

    // ── 鑷姩閲囬泦瀹炴椂甯傚満涓婁笅鏂?──
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
      constraints: plannerConstraints,
      market_context: liveCtx,
    };
    // Attach pending LLM context if available, then clear it
    if (state.pendingLlmContext) {
      payload.llm_research_output = state.pendingLlmContext;
      state.pendingLlmContext = null;
      setAIContextButtonState('idle');
      const btn = document.getElementById('ai-context-btn');
        if (btn) { btn.textContent = '1) 生成研究思路'; btn.disabled = false; btn.style.color = ''; }
    }
    const result = await aiApi('/proposals/generate', { method: 'POST', body: JSON.stringify(payload), timeoutMs: 30000 });
    // A: show filtered templates and planner notes
    const filteredTpls = result?.filtered_templates || result?.proposal?.filtered_templates || [];
    const plannerNotes = result?.planner_notes || [];
    // Update planner notes UI if it exists
    const plannerNotesEl = document.getElementById('ai-planner-notes');
    if (plannerNotesEl) {
      let html = '';
      if (plannerNotes.length) {
        html += `<div style="font-size:11px;color:#9fb1c9;margin-bottom:3px;">规划说明：${plannerNotes.map(n => esc(n)).join(' · ')}</div>`;
      }
      html += `<div style="font-size:11px;color:#7dd3fc;margin-bottom:3px;">研究方式：${esc(researchModeText(plannerConstraints.research_mode))} · 模板上限 ${esc(String(plannerConstraints.max_templates))} · 草案预算 ${esc(String(plannerConstraints.max_strategy_drafts))} · 回测预算 ${esc(String(plannerConstraints.max_backtest_runs))}</div>`;
      if (filteredTpls.length) {
        html += `<div style="font-size:11px;color:#f59e0b;margin-top:3px;">过滤模板：${filteredTpls.length}，${filteredTpls.slice(0,5).map(t => esc(t)).join(', ')}${filteredTpls.length > 5 ? '...' : ''}</div>`;
      }
      plannerNotesEl.innerHTML = html;
    }
    updatePlannerModeHint();
    notify(`研究任务已生成：${researchModeText(plannerConstraints.research_mode)}。${filteredTpls.length ? `已过滤 ${filteredTpls.length} 个不支持模板。` : ''}`);
    await refreshWorkbench(result?.proposal?.proposal_id || '', '');
  }

  // generateAIContext / generateProposal 浣跨敤涓婃柟鍞竴瀹炵幇锛岄伩鍏嶉噸澶嶈鐩栥€?

  async function runOneClickResearchDeploy() {
    const btn = document.getElementById('ai-oneclick-btn');
    const daysInput = document.getElementById('ai-oneclick-days');
    const allocationInput = document.getElementById('ai-oneclick-allocation');
    await ensureAutoPlannerGoal({ silent: true });
    const goal = String(document.getElementById('ai-planner-goal')?.value || '').trim();
    const exchange = String(document.getElementById('run-exchange')?.value || getCurrentResearchExchange() || 'binance');
    const days = daysInput
      ? Math.max(7, Math.min(3650, parseInt(daysInput.value || '30', 10) || 30))
      : 30;
    const allocationPercent = parseAllocationPercentInput(allocationInput?.value, 5);
    if (allocationInput) allocationInput.value = String(Math.round(allocationPercent));
    const allocationPct = allocationPercent / 100;
    if (goal.length < 8) {
      renderOneClickFeedback(buildOneClickFailureFeedback(
        new Error('研究目标太短（至少8个字符）'),
        {
          goal,
          symbols: csvInput('ai-planner-symbols'),
          timeframes: csvInput('ai-planner-timeframes'),
          exchange,
          days,
        },
      ));
      notify('\u7814\u7a76\u76ee\u6807\u592a\u77ed\uff08\u81f3\u5c118\u4e2a\u5b57\u7b26\uff09', true);
      return;
    }

    const symbols = csvInput('ai-planner-symbols');
    const timeframes = csvInput('ai-planner-timeframes');
    const plannerConstraints = buildPlannerConstraints();
    const payload = {
      goal,
      market_regime: String(document.getElementById('ai-planner-regime')?.value || 'mixed'),
      symbols: symbols.length ? symbols : [getCurrentResearchSymbol() || 'BTC/USDT'],
      timeframes: timeframes.length ? timeframes : ['15m', '1h'],
      constraints: plannerConstraints,
      metadata: { source: 'ai_research_ui_oneclick' },
      origin_context: {},
      market_context: state.pendingMacroContext || {},
      llm_research_output: state.pendingLlmContext || {},
      exchange,
      days,
      target: 'auto',
      allocation_pct: allocationPct,
      strategy_name: '',
      approval_notes: 'oneclick approve from ui',
      skip_deploy: false,
    };

    try {
      if (btn) { btn.disabled = true; btn.textContent = '提交中...'; }
      renderOneClickFeedback({
        tone: 'working',
        badge: '阶段 1/3',
        title: '正在生成提案并提交后台研究任务',
        summary: summarizeOneClickPayload(payload),
        details: [
          '系统将先创建提案，再把研究任务放入后台队列。',
          '此页面负责研究与候选，不会直接启动自治代理下单。',
        ],
        suggestions: ['提案提交成功后会自动轮询状态，完成后再进入部署阶段。'],
      });
      notify('正在执行：提交后台研究任务');
      const queueResult = await aiApi('/oneclick/research-deploy', {
        method: 'POST',
        body: JSON.stringify(payload),
        timeoutMs: 30000,
      });
      state.pendingLlmContext = null;
      setAIContextButtonState('idle');
      const contextBtn = document.getElementById('ai-context-btn');
      if (contextBtn) { contextBtn.textContent = '1) 生成研究思路'; contextBtn.disabled = false; contextBtn.style.color = ''; }

      const proposalId = String(queueResult?.proposal_id || queueResult?.run?.proposal?.proposal_id || '').trim();
      const jobId = String(queueResult?.job_id || queueResult?.job?.job_id || '').trim();
      if (!jobId) {
        const fallbackCandidate = String(queueResult?.candidate_id || queueResult?.run?.candidate?.candidate_id || '').trim();
        if (proposalId) await refreshWorkbench(proposalId, fallbackCandidate || '');
        if (fallbackCandidate) viewCandidate(fallbackCandidate).catch(() => {});
        updatePlannerModeHint();
        renderOneClickFeedback(buildOneClickSuccessFeedback(queueResult, payload));
        notify('一键研究已完成');
        return;
      }
      if (btn) btn.textContent = '研究中...';
      renderOneClickFeedback({
        tone: 'working',
        badge: '阶段 2/3',
        title: '研究任务已入队，后台运行中',
        summary: summarizeOneClickPayload(payload),
        details: [
          `提案 ID：${proposalId || '--'}`,
          `任务 ID：${jobId}`,
          '正在进行参数搜索、回测与验证，请耐心等待。',
        ],
        suggestions: ['你可以继续浏览研究页其他信息，结果会自动刷新。'],
      });
      notify('研究任务已入队，正在后台运行');
      await refreshWorkbench(proposalId, '');

      const completed = await pollOneClickJob(proposalId, jobId, btn, payload);
      const candidateId = String(completed?.candidateId || '').trim();
      if (!candidateId) {
        await refreshWorkbench(proposalId, '');
        renderOneClickFeedback({
          tone: 'warn',
          badge: '研究完成',
          title: '研究已完成，但未产出可部署候选',
          summary: summarizeOneClickPayload(payload),
          details: [
            `提案状态：${normalizeUiText(statusText(completed?.proposalStatus || 'rejected'))}`,
            completed?.proposalReason || '当前候选未通过验证门槛。',
          ],
          suggestions: [
            '先查看候选列表与验证原因，再决定是否调整目标或周期。',
            '这一步只影响研究候选，不会影响自治代理的实时执行。',
          ],
        });
        notify('研究完成，未生成可部署候选');
        return;
      }

      if (btn) btn.textContent = '部署中...';
      renderOneClickFeedback({
        tone: 'working',
        badge: '阶段 3/3',
        title: '研究完成，正在执行候选部署',
        summary: summarizeOneClickPayload(payload),
        details: [
          `候选 ID：${candidateId}`,
          '系统正在根据 promotion 结果自动选择部署目标。',
        ],
        suggestions: ['部署完成后会自动跳转候选详情。'],
      });

      let deployResult = null;
      try {
        deployResult = await aiApi('/oneclick/deploy-candidate', {
          method: 'POST',
          body: JSON.stringify({
            candidate_id: candidateId,
            target: 'auto',
            allocation_pct: payload.allocation_pct,
            approval_notes: 'oneclick approve from ui',
          }),
          timeoutMs: 30000,
        });
      } catch (deployErr) {
        const deployMsg = String(deployErr?.message || '');
        if (!/404|not found/i.test(deployMsg)) throw deployErr;
        deployResult = {
          proposal_id: proposalId,
          candidate_id: candidateId,
          deploy: { performed: false, action: null, result: null, runtime_status: null },
          outcome: 'completed_no_deploy',
        };
      }

      const finalResult = {
        ...deployResult,
        proposal_id: proposalId || deployResult?.proposal_id,
        candidate_id: candidateId,
      };
      await refreshWorkbench(proposalId, candidateId);
      viewCandidate(candidateId).catch(() => {});
      updatePlannerModeHint();
      renderOneClickFeedback(buildOneClickSuccessFeedback(finalResult, payload));
      const finalAction = String(finalResult?.deploy?.action || '').trim();
      const finalStatus = String(finalResult?.runtime_status || finalResult?.deploy?.runtime_status || '').trim();
      const finalOutcome = String(finalResult?.outcome || '').trim();
      if (finalOutcome === 'completed_without_compatible_runtime_target') {
        const modeHint = String(finalResult?.current_trading_mode || '').trim();
        notify(`一键研究已完成，但${modeHint || '当前'}模式下未自动部署；请手动选择 live_candidate 或切换到 paper`);
      } else if (finalOutcome.startsWith('completed_without')) {
        notify(`一键研究已完成，但未生成可部署候选${finalStatus ? `：${normalizeUiText(statusText(finalStatus))}` : ''}`);
      } else {
        notify(`一键研究+部署完成${finalStatus ? `：${finalStatus}` : ''}${finalAction ? ` (${finalAction})` : ''}`);
      }
    } catch (err) {
      renderOneClickFeedback(buildOneClickFailureFeedback(err, payload));
      notify(`\u4e00\u952e\u7814\u7a76+\u90e8\u7f72\u5931\u8d25: ${err.message}`, true);
    } finally {
      if (btn) { btn.disabled = false; btn.textContent = '\u26a1 \u7814\u7a76+\u90e8\u7f72'; }
    }
  }

  async function runProposal(proposalId) {
    if (!proposalId) { notify('请先选择研究任务', true); return; }
    const proposal = state.proposals.find(p => String(p?.proposal_id || '') === String(proposalId));
    if (isVirtualProposal(proposal)) {
      notify('该条目由候选结果回填，不能直接运行；请重新生成提案后再运行研究', true);
      return;
    }
    const proposalStatus = String(proposal?.status || '');
    if (proposal && !isRunnableProposalStatus(proposalStatus)) {
      notify(`当前状态「${statusText(proposalStatus)}」不可运行`, true);
      return;
    }
    const exchange = String(document.getElementById('run-exchange')?.value || 'binance');
    const days     = Math.max(1, Math.min(3650, parseInt(document.getElementById('run-days')?.value || '30', 10) || 30));
    notify('研究任务已提交，后台运行中...');
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
    if (isVirtualProposal(proposal)) {
      notify('候选回填条目没有可取消的原始研究任务', true);
      return;
    }
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
    if (window.AI?.modules?.diagnostics?.warmFunding) {
      return window.AI.modules.diagnostics.warmFunding();
    }
    throw new Error('diagnostics module unavailable');
  }

  /* ── 任务进度轮询 ── */
  async function retireProposal(proposalId) {
    const item = state.proposals.find(p => String(p?.proposal_id || '') === proposalId);
    if (isVirtualProposal(item)) {
      notify('候选回填条目没有可退役的原始研究任务', true);
      return;
    }
    const status = String(item?.status || '');
    if (!proposalId) {
      notify('缺少 proposal_id，无法退役', true);
      return;
    }
    if (!['shadow_running', 'live_candidate', 'paper_running', 'live_running'].includes(status)) {
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

  async function exitRunningQueueCandidate(candidateId) {
    if (!candidateId) return null;
    return aiApi(`/candidates/${encodeURIComponent(candidateId)}/exit`, {
      method: 'POST',
      body: JSON.stringify({ notes: 'exited from AI research queue' }),
      timeoutMs: 20000,
    });
  }

  async function exitProposal(proposalId) {
    if (!proposalId) return null;
    return aiApi(`/proposals/${encodeURIComponent(proposalId)}/exit`, {
      method: 'POST',
      body: JSON.stringify({ notes: 'exited from AI research queue' }),
      timeoutMs: 20000,
    });
  }

  retireProposal = async function retireProposalOverride(proposalId) {
    const item = state.proposals.find(p => String(p?.proposal_id || '') === proposalId);
    if (isVirtualProposal(item)) {
      notify('候选回填条目没有可退役的原始研究任务', true);
      return;
    }
    const status = String(item?.status || '');
    if (!proposalId) {
      notify('缺少 proposal_id，无法退出/退役', true);
      return;
    }
    if (!['shadow_running', 'live_candidate', 'paper_running', 'live_running'].includes(status)) {
      notify(`当前状态“${statusText(status)}”不支持退出/退役`, true);
      return;
    }

    const shouldExitRuntime = ['shadow_running', 'paper_running', 'live_running'].includes(status);
    const confirmMessage = shouldExitRuntime
      ? `确认退出该运行中条目？\n${proposalId}\n这会停止相关运行并将条目标记为退役，之后才可清空或删除。`
      : `确认将该研究退役？\n${proposalId}\n退役后会退出跟踪并允许删除。`;
    if (!window.confirm(confirmMessage)) return;

    try {
      if (shouldExitRuntime) {
        await exitProposal(proposalId);
        notify('运行中条目已退出');
      } else {
        await aiApi(`/proposals/${encodeURIComponent(proposalId)}/retire`, {
          method: 'POST',
          body: JSON.stringify({ notes: 'retired from AI research queue' }),
          timeoutMs: 15000,
        });
        notify('研究已退役');
      }
    } catch (err) {
      const msg = String(err?.message || '');
      if (/404|not found/i.test(msg)) {
        await deleteProposal(proposalId);
        notify('旧记录未命中退役接口，已直接删除');
        return;
      }
      throw err;
    }
    await refreshWorkbench(proposalId, '');
  };

  async function pullNewsForResearch() {
    if (window.AI?.modules?.diagnostics?.pullNews) {
      return window.AI.modules.diagnostics.pullNews();
    }
    throw new Error('diagnostics module unavailable');
  }

  async function loadPremiumDataStatus() {
    return aiApi('/premium-data/status', { timeoutMs: 20000 });
  }

  async function loadDataReadiness() {
    if (window.AI?.modules?.diagnostics?.refresh) {
      return window.AI.modules.diagnostics.refresh();
    }
    for (let attempt = 0; attempt < 10; attempt += 1) {
      if (window.AI?.modules?.diagnostics?.refresh) {
        return window.AI.modules.diagnostics.refresh();
      }
      await new Promise((resolve) => window.setTimeout(resolve, 60));
    }
    const premiumResult = await loadPremiumDataStatus().catch(() => null);
    const summaryEl = document.getElementById('ai-data-readiness-summary');
    const detailsEl = document.getElementById('ai-data-readiness-details');
    if (summaryEl) summaryEl.textContent = '数据诊断模块初始化中，请稍后重试。';
    if (detailsEl) {
      detailsEl.innerHTML = '<div style="padding:8px;background:#141f2f;border-radius:6px;">当前先跳过首屏数据诊断，避免与实时信号面板重复占用重接口。稍后再次打开研究页，或手动点击“刷新诊断”即可获取完整诊断。</div>';
    }
    return {
      premium_data_status: premiumResult,
      fallback: true,
    };
  }

  function getAgentModule() {
    return window.AI?.modules?.agent || null;
  }

  async function loadAgentStatus() {
    return runStateSingleFlight('agentStatusInFlight', async () => {
      const agent = getAgentModule();
      const response = agent && typeof agent.refresh === 'function'
        ? await agent.refresh()
        : await rootApi(AGENT_STATUS_API, { timeoutMs: 30000 });
      if (response?.status) {
        state.agentStatus = safeJsonClone(response.status, null);
        renderRuntimeSummary({ silent: true });
        emitWorkbenchState('agent-status');
      }
      return response;
    });
  }

  async function agentStart() {
    const agent = getAgentModule();
    if (agent && typeof agent.start === 'function') {
      return agent.start();
    }
    await rootApi(AGENT_START_API, {
      method: 'POST',
      body: JSON.stringify({ enable: true }),
      timeoutMs: 15000,
    });
    notify('autonomous agent start requested');
    return loadAgentStatus();
  }

  async function agentStop() {
    const agent = getAgentModule();
    if (agent && typeof agent.stop === 'function') {
      return agent.stop();
    }
    await rootApi(AGENT_STOP_API, {
      method: 'POST',
      timeoutMs: 15000,
    });
    notify('autonomous agent stop requested');
    return loadAgentStatus();
  }

  async function agentRunOnce() {
    const agent = getAgentModule();
    if (agent && typeof agent.runOnce === 'function') {
      return agent.runOnce();
    }
    const result = await rootApi(AGENT_RUN_ONCE_API, {
      method: 'POST',
      body: JSON.stringify({}),
      timeoutMs: 20000,
    });
    notify('autonomous agent run-once requested');
    return result;
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
    const proposalStatus = String(data?.proposal_status || '');
    const proposalReason = String(data?.proposal_reason || '').trim();
    // Keep proposal list dot up to date
    const idx  = state.proposals.findIndex(p => String(p?.proposal_id || '') === proposalId);
    if (idx >= 0 && state.proposals[idx].status !== proposalStatus) {
      state.proposals[idx] = { ...state.proposals[idx], status: proposalStatus };
      renderProposalList();
    }
    if (js === 'completed') {
      stopJobPolling(proposalId);
      if (proposalStatus === 'rejected') {
        notify(`研究完成，但未通过验证${proposalReason ? `：${proposalReason}` : ''}`);
      } else {
        notify('研究任务已完成，工作台状态已更新');
      }
      await refreshWorkbench(proposalId, '');
    } else if (js === 'cancelled') {
      stopJobPolling(proposalId);
      notify('\u7814\u7a76\u4efb\u52a1\u5df2\u53d6\u6d88');
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
    if (isVirtualProposal(item)) {
      notify('候选回填条目没有可删除的原始研究任务；如需清理，请删除对应候选记录', true);
      return;
    }
    if (item && DELETE_BLOCKED_PROPOSAL_STATUSES.has(String(item?.status || ''))) {
      notify(`当前状态「${statusText(item.status)}」不可删除，请先停止后再删除。`, true);
      return;
    }
    if (!window.confirm(`确认删除此研究任务？\n${proposalId}\n将级联删除相关候选记录。`)) return;
    await aiApi(`/proposals/${encodeURIComponent(proposalId)}`, { method: 'DELETE', timeoutMs: 20000 });
    notify('研究任务已删除');
    if (state.selectedProposalId === proposalId) {
      state.selectedProposalId = '';
    }
    if (proposalIdForCandidate(state.selectedCandidateId) === proposalId) state.selectedCandidateId = '';
    await refreshWorkbench('', '');
  }

  async function clearVisibleProposalQueue() {
    const visibleProposals = getVisibleProposalQueueItems();
    if (!visibleProposals.length) {
      updateClearQueueButton([]);
      notify('当前没有可清空的研究任务', true);
      return;
    }

    const { clearableProposalIds, clearableCandidateIds, skippedBlocked, skippedMissing } = getVisibleProposalQueueTargets(visibleProposals);
    if (!clearableProposalIds.length && !clearableCandidateIds.length) {
      const reasons = [];
      if (skippedBlocked.length) reasons.push(`运行中条目 ${skippedBlocked.length} 个`);
      if (skippedMissing.length) reasons.push(`无效回填 ${skippedMissing.length} 个`);
      notify(reasons.length ? `当前可见任务暂不可清空：${reasons.join('，')}` : '当前没有可清空的研究任务', true);
      updateClearQueueButton(visibleProposals);
      return;
    }

    const clearableProposalSet = new Set(clearableProposalIds);
    const clearableCandidateSet = new Set(clearableCandidateIds);
    const clearableVisibleCount = visibleProposals.filter((item) => {
      if (isVirtualProposal(item)) return clearableCandidateSet.has(fallbackCandidateIdForProposal(item));
      return clearableProposalSet.has(String(item?.proposal_id || '').trim());
    }).length;

    const confirmLines = [
      `确认清空当前可见的 ${visibleProposals.length} 个任务条目吗？`,
      `将移除 ${clearableVisibleCount} 个当前条目。`,
    ];
    if (clearableProposalIds.length) confirmLines.push(`其中删除研究任务 ${clearableProposalIds.length} 个（会级联清理候选记录）`);
    if (clearableCandidateIds.length) confirmLines.push(`清理候选回填 ${clearableCandidateIds.length} 个`);
    if (skippedBlocked.length) confirmLines.push(`运行中条目将跳过：${skippedBlocked.length} 个`);
    if (skippedMissing.length) confirmLines.push(`无效回填将忽略：${skippedMissing.length} 个`);
    if (!window.confirm(confirmLines.join('\n'))) return;

    await withActionLock('clear', async () => {
      const deletedProposalIds = new Set();
      const deletedCandidateIds = new Set();
      const failures = [];

      for (const proposalId of clearableProposalIds) {
        try {
          await aiApi(`/proposals/${encodeURIComponent(proposalId)}`, { method: 'DELETE', timeoutMs: 20000 });
          deletedProposalIds.add(proposalId);
        } catch (err) {
          failures.push({
            type: 'proposal',
            id: proposalId,
            message: String(err?.message || '删除失败'),
          });
        }
      }

      for (const candidateId of clearableCandidateIds) {
        try {
          await aiApi(`/candidates/${encodeURIComponent(candidateId)}`, { method: 'DELETE', timeoutMs: 20000 });
          deletedCandidateIds.add(candidateId);
        } catch (err) {
          failures.push({
            type: 'candidate',
            id: candidateId,
            message: String(err?.message || '清理失败'),
          });
        }
      }

      const selectedProposal = findProposalById(state.selectedProposalId);
      if (deletedProposalIds.has(String(state.selectedProposalId || '').trim())) {
        state.selectedProposalId = '';
      } else if (selectedProposal && isVirtualProposal(selectedProposal) && deletedCandidateIds.has(fallbackCandidateIdForProposal(selectedProposal))) {
        state.selectedProposalId = '';
      }

      if (deletedCandidateIds.has(String(state.selectedCandidateId || '').trim())) {
        state.selectedCandidateId = '';
      } else if (deletedProposalIds.has(proposalIdForCandidate(state.selectedCandidateId))) {
        state.selectedCandidateId = '';
      }

      await refreshWorkbench('', '');

      const summary = [];
      if (clearableVisibleCount) summary.push(`已清空 ${clearableVisibleCount} 个当前任务条目`);
      if (deletedProposalIds.size) summary.push(`删除了 ${deletedProposalIds.size} 个研究任务`);
      if (deletedCandidateIds.size) summary.push(`清理了 ${deletedCandidateIds.size} 个候选回填`);
      if (skippedBlocked.length) summary.push(`跳过 ${skippedBlocked.length} 个运行中条目`);
      if (skippedMissing.length) summary.push(`忽略 ${skippedMissing.length} 个无效回填`);
      if (failures.length) summary.push(`失败 ${failures.length} 个`);
      notify(summary.join('，') || '任务队列已清空', failures.length > 0);
      if (failures.length) console.warn('clearVisibleProposalQueue failures:', failures);
    });
  }

  clearVisibleProposalQueue = async function clearVisibleProposalQueueOverride() {
    const visibleProposals = getVisibleProposalQueueItems();
    if (!visibleProposals.length) {
      updateClearQueueButton([]);
      updateExitRunningQueueButton([]);
      notify('当前没有可清空的研究任务', true);
      return;
    }

    const { clearableProposalIds, clearableCandidateIds, skippedBlocked, skippedMissing } = getVisibleProposalQueueTargets(visibleProposals);
    if (!clearableProposalIds.length && !clearableCandidateIds.length) {
      const reasons = [];
      if (skippedBlocked.length) reasons.push(`运行/跟踪中的条目 ${skippedBlocked.length} 个，请先点“一键退出运行中条目”`);
      if (skippedMissing.length) reasons.push(`失效回填 ${skippedMissing.length} 个`);
      notify(reasons.length ? `当前可见任务暂不可清空：${reasons.join('；')}` : '当前没有可清空的研究任务', true);
      updateClearQueueButton(visibleProposals);
      updateExitRunningQueueButton(visibleProposals);
      return;
    }

    const clearableProposalSet = new Set(clearableProposalIds);
    const clearableCandidateSet = new Set(clearableCandidateIds);
    const clearableVisibleCount = visibleProposals.filter((item) => {
      if (isVirtualProposal(item)) return clearableCandidateSet.has(fallbackCandidateIdForProposal(item));
      return clearableProposalSet.has(String(item?.proposal_id || '').trim());
    }).length;

    const confirmLines = [
      `确认清空当前可见的 ${visibleProposals.length} 个任务条目吗？`,
      `将移除 ${clearableVisibleCount} 个当前条目。`,
    ];
    if (clearableProposalIds.length) confirmLines.push(`其中删除研究任务 ${clearableProposalIds.length} 个（会级联清理候选记录）`);
    if (clearableCandidateIds.length) confirmLines.push(`清理候选回填 ${clearableCandidateIds.length} 个`);
    if (skippedBlocked.length) confirmLines.push(`运行/跟踪中的条目不会被清空，请先点“一键退出运行中条目”：${skippedBlocked.length} 个`);
    if (skippedMissing.length) confirmLines.push(`失效回填将忽略：${skippedMissing.length} 个`);
    if (!window.confirm(confirmLines.join('\n'))) return;

    await withActionLock('clear', async () => {
      const deletedProposalIds = new Set();
      const deletedCandidateIds = new Set();
      const failures = [];

      for (const proposalId of clearableProposalIds) {
        try {
          await aiApi(`/proposals/${encodeURIComponent(proposalId)}`, { method: 'DELETE', timeoutMs: 20000 });
          deletedProposalIds.add(proposalId);
        } catch (err) {
          failures.push({
            type: 'proposal',
            id: proposalId,
            message: String(err?.message || '删除失败'),
          });
        }
      }

      for (const candidateId of clearableCandidateIds) {
        try {
          await aiApi(`/candidates/${encodeURIComponent(candidateId)}`, { method: 'DELETE', timeoutMs: 20000 });
          deletedCandidateIds.add(candidateId);
        } catch (err) {
          failures.push({
            type: 'candidate',
            id: candidateId,
            message: String(err?.message || '清理失败'),
          });
        }
      }

      const selectedProposal = findProposalById(state.selectedProposalId);
      if (deletedProposalIds.has(String(state.selectedProposalId || '').trim())) {
        state.selectedProposalId = '';
      } else if (selectedProposal && isVirtualProposal(selectedProposal) && deletedCandidateIds.has(fallbackCandidateIdForProposal(selectedProposal))) {
        state.selectedProposalId = '';
      }

      if (deletedCandidateIds.has(String(state.selectedCandidateId || '').trim())) {
        state.selectedCandidateId = '';
      } else if (deletedProposalIds.has(proposalIdForCandidate(state.selectedCandidateId))) {
        state.selectedCandidateId = '';
      }

      await refreshWorkbench('', '');

      const summary = [];
      if (clearableVisibleCount) summary.push(`已清空 ${clearableVisibleCount} 个当前任务条目`);
      if (deletedProposalIds.size) summary.push(`删除了 ${deletedProposalIds.size} 个研究任务`);
      if (deletedCandidateIds.size) summary.push(`清理了 ${deletedCandidateIds.size} 个候选回填`);
      if (skippedBlocked.length) summary.push(`保留 ${skippedBlocked.length} 个运行/跟踪中的条目`);
      if (skippedMissing.length) summary.push(`忽略 ${skippedMissing.length} 个失效回填`);
      if (failures.length) summary.push(`失败 ${failures.length} 个`);
      notify(summary.join('；') || '任务队列已清空', failures.length > 0);
      if (failures.length) console.warn('clearVisibleProposalQueue failures:', failures);
    });
  };

  async function exitVisibleRunningQueueItems() {
    const visibleProposals = getVisibleProposalQueueItems();
    if (!visibleProposals.length) {
      updateExitRunningQueueButton([]);
      notify('当前没有可退出的运行中条目', true);
      return;
    }

    const { proposalIds, candidateIds, skippedMissing } = getVisibleRunningQueueTargets(visibleProposals);
    const exitableCount = proposalIds.length + candidateIds.length;
    if (!exitableCount) {
      const reasons = [];
      if (skippedMissing.length) reasons.push(`失效回填 ${skippedMissing.length} 个`);
      notify(reasons.length ? `当前没有可退出的运行中条目：${reasons.join('；')}` : '当前没有可退出的运行中条目', true);
      updateExitRunningQueueButton(visibleProposals);
      return;
    }

    const confirmLines = [
      `确认退出当前可见的 ${exitableCount} 个运行中条目吗？`,
      '这会停止相关运行或跟踪，并把条目标记为退役。',
    ];
    if (proposalIds.length) confirmLines.push(`退出研究任务 ${proposalIds.length} 个`);
    if (candidateIds.length) confirmLines.push(`退出候选回填 ${candidateIds.length} 个`);
    if (skippedMissing.length) confirmLines.push(`忽略失效回填 ${skippedMissing.length} 个`);
    if (!window.confirm(confirmLines.join('\n'))) return;

    await withActionLock('exit', async () => {
      const exitedProposalIds = new Set();
      const exitedCandidateIds = new Set();
      const failures = [];

      for (const proposalId of proposalIds) {
        try {
          await exitProposal(proposalId);
          exitedProposalIds.add(proposalId);
        } catch (err) {
          failures.push({
            type: 'proposal',
            id: proposalId,
            message: String(err?.message || '退出失败'),
          });
        }
      }

      for (const candidateId of candidateIds) {
        try {
          await exitRunningQueueCandidate(candidateId);
          exitedCandidateIds.add(candidateId);
        } catch (err) {
          failures.push({
            type: 'candidate',
            id: candidateId,
            message: String(err?.message || '退出失败'),
          });
        }
      }

      await refreshWorkbench('', '');

      const summary = [];
      if (exitedProposalIds.size) summary.push(`退出了 ${exitedProposalIds.size} 个研究任务`);
      if (exitedCandidateIds.size) summary.push(`退出了 ${exitedCandidateIds.size} 个候选回填`);
      if (skippedMissing.length) summary.push(`忽略 ${skippedMissing.length} 个失效回填`);
      if (failures.length) summary.push(`失败 ${failures.length} 个`);
      notify(summary.join('；') || '运行中条目已退出', failures.length > 0);
      if (failures.length) console.warn('exitVisibleRunningQueueItems failures:', failures);
    });
  }

  async function clearVisibleCandidates() {
    const visibleCandidates = getVisibleCandidates();
    if (!visibleCandidates.length) {
      updateClearCandidatesButton([]);
      notify('当前没有可清空的候选策略', true);
      return;
    }

    const { clearableProposalIds, skippedBlocked, skippedVirtual } = getVisibleCandidateProposalTargets(visibleCandidates);
    if (!clearableProposalIds.length) {
      const reasons = [];
      if (skippedBlocked.length) reasons.push(`运行中任务 ${skippedBlocked.length} 个`);
      if (skippedVirtual.length) reasons.push(`候选回填条目 ${skippedVirtual.length} 个`);
      notify(reasons.length ? `当前可见候选暂不可清空：${reasons.join('，')}` : '当前没有可清空的候选策略', true);
      updateClearCandidatesButton(visibleCandidates);
      return;
    }

    const clearableSet = new Set(clearableProposalIds);
    const clearableVisibleCount = visibleCandidates
      .filter((item) => clearableSet.has(String(item?.proposal_id || '').trim()))
      .length;
    const confirmLines = [
      `确认清空当前可见的 ${visibleCandidates.length} 个候选策略吗？`,
      `将级联删除 ${clearableProposalIds.length} 个对应研究任务，并移除 ${clearableVisibleCount} 个当前可见候选。`,
    ];
    if (skippedBlocked.length) confirmLines.push(`运行中任务将跳过：${skippedBlocked.length} 个`);
    if (skippedVirtual.length) confirmLines.push(`候选回填条目将跳过：${skippedVirtual.length} 个`);
    if (!window.confirm(confirmLines.join('\n'))) return;

    await withActionLock('clear', async () => {
      const deletedProposalIds = new Set();
      const failures = [];
      for (const proposalId of clearableProposalIds) {
        try {
          await aiApi(`/proposals/${encodeURIComponent(proposalId)}`, { method: 'DELETE', timeoutMs: 20000 });
          deletedProposalIds.add(proposalId);
        } catch (err) {
          failures.push({
            proposalId,
            message: String(err?.message || '删除失败'),
          });
        }
      }

      if (deletedProposalIds.has(String(state.selectedProposalId || '').trim())) {
        state.selectedProposalId = '';
      }
      if (deletedProposalIds.has(proposalIdForCandidate(state.selectedCandidateId))) {
        state.selectedCandidateId = '';
      }

      await refreshWorkbench('', '');

      const clearedVisibleCount = visibleCandidates
        .filter((item) => deletedProposalIds.has(String(item?.proposal_id || '').trim()))
        .length;
      const summary = [];
      if (clearedVisibleCount) summary.push(`已清空 ${clearedVisibleCount} 个当前候选`);
      if (deletedProposalIds.size) summary.push(`删除了 ${deletedProposalIds.size} 个研究任务`);
      if (skippedBlocked.length) summary.push(`跳过 ${skippedBlocked.length} 个运行中任务`);
      if (skippedVirtual.length) summary.push(`跳过 ${skippedVirtual.length} 个候选回填条目`);
      if (failures.length) summary.push(`失败 ${failures.length} 个`);
      notify(summary.join('，') || '候选已清空', failures.length > 0);
      if (failures.length) console.warn('clearVisibleCandidates failures:', failures);
    });
  }

  /* 鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹?
     浜嬩欢缁戝畾
  鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹?*/
  function bindEvents() {
    document.getElementById('ai-exit-running-queue-btn')?.addEventListener('click', () =>
      exitVisibleRunningQueueItems().catch(err => notify(`退出运行中条目失败: ${err.message}`, true)));
    /* 生成研究 */
    document.getElementById('ai-generate-btn')?.addEventListener('click', () =>
      withActionLock('generate', () => generateProposal()).catch(err => notify(`生成失败: ${err.message}`, true)));

    /* AI生成研究鎬濊矾 */
    document.getElementById('ai-auto-goal-btn')?.addEventListener('click', () =>
      ensureAutoPlannerGoal({ forceRefresh: true }).catch(err => notify(`自动生成研究目标失败: ${err.message}`, true)));
    document.getElementById('ai-context-btn')?.addEventListener('click', () =>
      generateAIContext().catch(err => notify(`生成研究思路失败: ${err.message}`, true)));
    document.getElementById('ai-clear-queue-btn')?.addEventListener('click', () =>
      clearVisibleProposalQueue().catch(err => notify(`清空任务失败: ${err.message}`, true)));
    document.getElementById('ai-clear-candidates-btn')?.addEventListener('click', () =>
      clearVisibleCandidates().catch(err => notify(`清空候选失败: ${err.message}`, true)));

    /* one-click 鑷姩研究 */
    document.getElementById('ai-oneclick-btn')?.addEventListener('click', () =>
      withActionLock('oneclick', () => runOneClickResearchDeploy()).catch(err => notify(`one-click 执行失败: ${err.message}`, true)));

    /* 寰呬汉宸ョ‘璁ら槦鍒椾簨浠朵唬鐞?*/
    document.getElementById('ai-approval-list')?.addEventListener('click', e => {
      const btn = e.target.closest('[data-action]');
      if (!btn) return;
      const cid    = String(btn.dataset.candidateId || '').trim();
      const action = String(btn.dataset.action || '').trim();
      if (action === 'view-candidate' && cid) {
        e.stopPropagation();
        viewCandidate(cid).catch(err => notify(`加载详情失败: ${err.message}`, true));
      }
    });

    /* 鍒锋柊 */
    document.getElementById('ai-refresh-btn')?.addEventListener('click', () =>
      refreshWorkbench().catch(err => notify(`刷新失败: ${err.message}`, true)));
    document.getElementById('ai-data-refresh-btn')?.addEventListener('click', () =>
      loadDataReadiness().catch(err => notify(`数据诊断失败: ${err.message}`, true)));
    document.getElementById('ai-news-pull-btn')?.addEventListener('click', () =>
      pullNewsForResearch().catch(err => notify(`新闻拉取失败: ${err.message}`, true)));
    document.getElementById('ai-funding-warm-btn')?.addEventListener('click', () =>
      warmFundingForResearch().catch(err => notify(`宏观缓存预热失败: ${err.message}`, true)));
    document.getElementById('ai-live-decision-save-btn')?.addEventListener('click', () =>
      saveLiveDecisionRuntimeConfig().catch(err => notify(`下单前AI复核保存失败: ${err.message}`, true)));
    ['ai-live-decision-enabled', 'ai-live-decision-mode', 'ai-live-decision-provider'].forEach((id) => {
      document.getElementById(id)?.addEventListener('change', () => previewLiveDecisionProviderSelection());
    });
    document.getElementById('ai-live-decision-model')?.addEventListener('input', () => previewLiveDecisionProviderSelection());
    document.getElementById('ai-planner-symbols')?.addEventListener('change', () => {
      state.autoPlannerRecommendation = null;
      loadDataReadiness().catch(() => {});
    });
    [
      'ai-planner-goal',
      'ai-planner-regime',
      'ai-planner-max-templates',
      'ai-planner-research-mode',
      'ai-planner-max-drafts',
      'ai-planner-max-backtests',
      'ai-planner-exploration-bias',
      'ai-planner-timeframes',
    ].forEach((id) => {
      document.getElementById(id)?.addEventListener('input', () => {
        state.autoPlannerRecommendation = null;
        updatePlannerModeHint();
        clearOneClickFeedback();
      });
      document.getElementById(id)?.addEventListener('change', () => {
        state.autoPlannerRecommendation = null;
        updatePlannerModeHint();
        clearOneClickFeedback();
      });
    });
    document.getElementById('run-exchange')?.addEventListener('change', () => {
      state.autoPlannerRecommendation = null;
      clearOneClickFeedback();
      loadDataReadiness().catch(() => {});
    });
    document.getElementById('run-days')?.addEventListener('input', () => clearOneClickFeedback());
    document.getElementById('run-days')?.addEventListener('change', () => clearOneClickFeedback());
    document.getElementById('ai-oneclick-days')?.addEventListener('input', () => clearOneClickFeedback());
    document.getElementById('ai-oneclick-days')?.addEventListener('change', () => clearOneClickFeedback());
    document.getElementById('ai-oneclick-allocation')?.addEventListener('input', () => clearOneClickFeedback());
    document.getElementById('ai-oneclick-allocation')?.addEventListener('change', () => clearOneClickFeedback());
    window.addEventListener('ai-agent:status', (event) => {
      const nextStatus = event?.detail?.status || null;
      if (!nextStatus) return;
      state.agentStatus = safeJsonClone(nextStatus, null);
      renderRuntimeSummary({ silent: true });
      emitWorkbenchState('agent-status');
    });

    /* 运行研究 */
    document.getElementById('run-selected-btn')?.addEventListener('click', () =>
      withActionLock('run', () => runProposal(state.selectedProposalId)).catch(err => notify(`运行失败: ${err.message}`, true)));
    document.getElementById('ai-compare-btn')?.addEventListener('click', () => openCompareModal());

    /* 信号鍒锋柊 */
    document.getElementById('signal-refresh-btn')?.addEventListener('click', () =>
      loadSignal().catch(err => notify(`\u4fe1\u53f7\u5931\u8d25: ${err.message}`, true)));
    document.getElementById('signal-symbol')?.addEventListener('change', (e) =>
      loadSignal(e.target.value).catch(() => {}));

    /* \u4fe1\u53f7\u9762\u677f\u6298\u53e0 */
    document.getElementById('signal-panel-toggle')?.addEventListener('click', () => {
      state.signalPanelCollapsed = !state.signalPanelCollapsed;
      const body   = document.getElementById('signal-panel-body');
      const toggle = document.getElementById('signal-panel-toggle');
      if (body)   body.style.display   = state.signalPanelCollapsed ? 'none' : '';
      if (toggle) toggle.classList.toggle('collapsed', state.signalPanelCollapsed);
    });

    /* 娉ㄥ唽 Modal 鍏抽棴 */
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

    /* 研究闃熷垪鐐瑰嚮浠ｇ悊 */
    document.getElementById('ai-proposal-list')?.addEventListener('click', e => {
      const btn = e.target.closest('[data-action]');
      if (!btn) return;
      const pid    = String(btn.dataset.proposalId || '').trim();
      const action = String(btn.dataset.action || '').trim();

      if (action === 'select-proposal') {
        const item = e.target.closest('.proposal-compact-item');
        const id   = String(item?.dataset?.proposalId || pid || '').trim();
        if (!id) return;
        selectProposal(id);
        return;
      }
      if (action === 'cancel-proposal' && pid) {
        e.stopPropagation();
        selectProposal(pid);
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

    /* 鎺掑簭 */
    document.getElementById('cand-sort-select')?.addEventListener('change', e => {
      state.sortBy = String(e.target.value || 'score');
      renderCandidateCards();
    });

    /* 绫诲埆绛涢€?*/
    document.getElementById('cand-filter-category')?.addEventListener('change', e => {
      state.filterCategory = String(e.target.value || '');
      renderCandidateCards();
    });

    /* 鍊欓€夊崱鐗囩偣鍑讳唬鐞?*/
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

  /* 鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹?
     杞
  鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹?*/
  function startPolling() {
    clearInterval(state.signalTimer);
    clearInterval(state.refreshTimer);
    clearInterval(state.liveSignalTimer);
    clearTimeout(state.signalKickoffTimer);
    clearTimeout(state.liveSignalKickoffTimer);
    if (document.hidden || !isAiWorkspaceActive()) return;
    if (!canRunAiPolling()) return;

    if (isAiResearchActive()) {
      state.signalKickoffTimer = window.setTimeout(() => {
        state.signalKickoffTimer = null;
        if (!isAiResearchActive() || document.hidden || !canRunAiPolling()) return;
        loadSignal().catch(() => {});
      }, 1200);
      state.signalTimer = setInterval(() => {
        if (!isAiResearchActive() || document.hidden || !canRunAiPolling()) return;
        loadSignal().catch(() => {});
      }, SIGNAL_INTERVAL_MS);
      state.refreshTimer = setInterval(() => {
        if (!isAiResearchActive() || document.hidden || !canRunAiPolling()) return;
        refreshWorkbench().catch(() => {});
      }, REFRESH_INTERVAL_MS);
    }

    if (isAiWorkspaceActive()) {
      state.liveSignalKickoffTimer = window.setTimeout(() => {
        state.liveSignalKickoffTimer = null;
        if (!isAiWorkspaceActive() || document.hidden || !canRunAiPolling()) return;
        loadLiveSignals().catch(() => {});
      }, isAiResearchActive() ? 1800 : 600);
      state.liveSignalTimer = setInterval(() => {
        if (!isAiWorkspaceActive() || document.hidden || !canRunAiPolling()) return;
        loadLiveSignals().catch(() => {});
      }, 30000);
    }
  }

  function stopPolling() {
    clearInterval(state.signalTimer);
    clearInterval(state.refreshTimer);
    clearInterval(state.liveSignalTimer);
    clearTimeout(state.liveDecisionActivityRetryTimer);
    clearTimeout(state.signalKickoffTimer);
    clearTimeout(state.liveSignalKickoffTimer);
    state.signalTimer = null;
    state.refreshTimer = null;
    state.liveSignalTimer = null;
    state.liveDecisionActivityRetryTimer = null;
    state.signalKickoffTimer = null;
    state.liveSignalKickoffTimer = null;
  }

  function isAiAgentActive() {
    const tab = document.getElementById('ai-agent');
    return !!(tab && tab.classList.contains('active'));
  }

  function isAiResearchActive() {
    const tab = document.getElementById('ai-research');
    return !!(tab && tab.classList.contains('active'));
  }

  function isAiWorkspaceActive() {
    return isAiResearchActive() || isAiAgentActive();
  }

  function canRunAiPolling() {
    if (typeof window === 'undefined' || typeof window.__ctsSharedPolling?.canRun !== 'function') return true;
    return window.__ctsSharedPolling.canRun('ai');
  }

  function syncHubLayoutHeight() {
    const hub = document.querySelector('#ai-research .ai-hub-layout');
    if (!hub) return;
    hub.style.height = 'auto';
    hub.style.minHeight = '0';
  }

  function bindLayoutSync() {
    window.addEventListener('resize', () => syncHubLayoutHeight());
    const aiTabBtn = document.querySelector('.tab-btn[data-tab="ai-research"]');
    aiTabBtn?.addEventListener('click', () => {
      setTimeout(syncHubLayoutHeight, 0);
      setTimeout(syncHubLayoutHeight, 120);
    });
    document.querySelectorAll('.tab-btn').forEach((btn) => {
      btn.addEventListener('click', () => {
        setTimeout(() => {
          if (isAiWorkspaceActive()) {
            startPolling();
            if (isAiResearchActive()) refreshWorkbench().catch(() => {});
          } else stopPolling();
        }, 0);
      });
    });
    document.addEventListener('visibilitychange', () => {
      if (document.hidden) {
        stopPolling();
      } else if (isAiWorkspaceActive()) {
        if (!canRunAiPolling()) return;
        startPolling();
        if (isAiResearchActive()) refreshWorkbench().catch(() => {});
      }
    });
  }

  /* 鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹?
     初始鍖?
  鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹?*/
  function init() {
    bindInitRetry();
    if (!document.getElementById('ai-candidate-cards')) return;  // tab 未激活时跳过
    if (initialized) {
      syncHubLayoutHeight();
      if (isAiWorkspaceActive()) {
        startPolling();
        if (isAiResearchActive()) refreshWorkbench().catch(() => {});
      }
      return;
    }
    initialized = true;
    bindLayoutSync();
    syncHubLayoutHeight();
    bindEvents();
    syncPrimaryActionButtons();
    updatePlannerModeHint();
    normalizeDomText(document.getElementById('ai-research'));
    if (isAiResearchActive() && canRunAiPolling()) {
      refreshWorkbench().catch(err => console.error('AI研究初始化失败', err));
    }
    if (isAiWorkspaceActive()) startPolling();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
  window.addEventListener('beforeunload', () => {
    stopPolling();
    Object.values(state.jobPollingTimers).forEach(t => clearInterval(t));
  });

  /* 鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹?
     Phase A 鈥?瀹炴椂信号闈㈡澘锛?0s 杞锛?
  鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹?*/

  /* 鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹?
     Phase B 鈥?蹇€熸敞鍐?
  鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹佲攣鈹?*/

  async function loadLiveSignals() {
    if (!document.getElementById('ai-research-live-signals-panel') && !document.getElementById('ai-agent-live-signals-panel')) return;
    if (state.liveSignalsInFlight) return state.liveSignalsInFlight;
    const task = (async () => {
      let researchPayload = {};
      let agentPayload = {};

      const researchTask = aiApi('/live-signals', { timeoutMs: LIVE_SIGNALS_TIMEOUT_MS })
        .then((payload) => {
          researchPayload = payload || {};
          renderLiveSignalPanel(
            'ai-research-live-signals-panel',
            liveSignalSectionById(researchPayload, 'candidates'),
            !!researchPayload?.ml_model_loaded,
          );
          return researchPayload;
        })
        .catch((err) => {
          renderLiveSignalPanelError('ai-research-live-signals-panel', err?.message || 'Live candidate signals failed to load');
          return null;
        });

      const agentTask = aiApi('/autonomous-agent/live-signals', { timeoutMs: AGENT_LIVE_SIGNALS_TIMEOUT_MS })
        .then((payload) => {
          agentPayload = payload || {};
          renderLiveSignalPanel(
            'ai-agent-live-signals-panel',
            liveSignalSectionById(agentPayload, 'watchlist'),
            !!agentPayload?.ml_model_loaded,
          );
          return agentPayload;
        })
        .catch((err) => {
          renderLiveSignalPanelError('ai-agent-live-signals-panel', err?.message || 'Agent watchlist signals failed to load');
          return null;
        });

      await Promise.all([researchTask, agentTask]);
      return { researchPayload, agentPayload };
    })();
    state.liveSignalsInFlight = task;
    try {
      return await task;
    } finally {
      if (state.liveSignalsInFlight === task) state.liveSignalsInFlight = null;
    }
  }

  function liveSignalSections(payload) {
    const sections = Array.isArray(payload?.sections) ? payload.sections.filter(Boolean) : [];
    if (sections.length) return sections;
    return [
      {
        id: 'candidates',
        title: '运行中候选',
        empty_text: '暂无运行中候选',
        items: Array.isArray(payload?.candidate_items) ? payload.candidate_items : [],
      },
      {
        id: 'watchlist',
        title: '自治代理 watchlist',
        empty_text: '暂无自治代理 watchlist',
        items: Array.isArray(payload?.watchlist_items) ? payload.watchlist_items : [],
      },
    ];
  }

  function liveSignalDirIcon(direction) {
    return direction === 'LONG' ? '▲' : direction === 'SHORT' ? '▼' : '•';
  }

  function liveSignalDirColor(direction) {
    return direction === 'LONG' ? '#4ade80' : direction === 'SHORT' ? '#f87171' : '#6b7fa0';
  }

  function liveSignalPct(value) {
    return `${((Number(value) || 0) * 100).toFixed(0)}%`;
  }

  function liveSignalPrimaryTitle(item) {
    const parts = [];
    if (item?.source === 'candidate') {
      parts.push(`<span class="live-sig-name">${esc(item?.strategy || '--')}</span>`);
      if (item?.candidate_id_suffix) parts.push(`<span class="live-sig-pill">#${esc(item.candidate_id_suffix)}</span>`);
      parts.push(`<span class="live-sig-pill">${esc(item?.timeframe || '--')}</span>`);
      parts.push(`<span class="live-sig-pill">${esc(statusText(item?.status || 'unknown'))}</span>`);
      return parts.join('');
    }
    parts.push(`<span class="live-sig-name">${esc(item?.symbol || '--')}</span>`);
    parts.push(`<span class="live-sig-pill">${esc(item?.timeframe || '--')}</span>`);
    parts.push(`<span class="live-sig-pill">${esc(item?.status_display || (item?.selected ? '当前关注' : '观察列表'))}</span>`);
    return parts.join('');
  }

  function liveSignalSecondaryMeta(item) {
    const parts = [];
    if (item?.source === 'candidate') {
      if (item?.symbol) parts.push(item.symbol);
      if (Number(item?.duplicate_count || 0) > 1) parts.push(`去重 ${Number(item.duplicate_count)}`);
      if (Array.isArray(item?.statuses) && item.statuses.length > 1) {
        parts.push(item.statuses.map(status => statusText(status)).join('/'));
      }
      return parts.join(' · ');
    }
    if (item?.strategy) parts.push(item.strategy);
    if (Number(item?.rank || 0) > 0) parts.push(`排名 #${Number(item.rank)}`);
    return parts.join(' · ');
  }

  function renderLiveSignalRow(item, mlLoaded) {
    const sig = item?.signal;
    const metaText = liveSignalSecondaryMeta(item);
    if (!sig) {
      return `<div class="live-sig-row">
  <div class="live-sig-header">
    <div class="live-sig-title-wrap">
      <div class="live-sig-title-row">${liveSignalPrimaryTitle(item)}</div>
      ${metaText ? `<div class="live-sig-meta">${esc(metaText)}</div>` : ''}
    </div>
    <span style="font-weight:700;font-size:12px;margin-left:auto;color:#94a3b8;">ERR</span>
  </div>
  <div class="live-sig-meta">${esc(item?.error || '信号计算失败')}</div>
</div>`;
    }

    const comp = sig.components || {};
    const signalDir = String(sig.direction || 'FLAT').toUpperCase();
    const noMarketData = Number(sig.market_data_rows || 0) <= 0;
    const isStale = !!sig.market_data_stale;
    const blockedBadge = sig.blocked_by_risk
      ? `<span class="live-sig-badge" style="background:#7f1d1d;color:#fca5a5;" title="${esc(sig.risk_reason)}">风控</span>` : '';
    const approvalBadge = (sig.requires_approval && !sig.blocked_by_risk && !noMarketData && ['LONG', 'SHORT'].includes(signalDir))
      ? '<span class="live-sig-badge" style="background:#78350f;color:#fcd34d;">待审</span>' : '';
    const dataBadge = noMarketData
      ? '<span class="live-sig-badge" style="background:#243447;color:#9fb1c9;">缺数据</span>'
      : (isStale ? '<span class="live-sig-badge" style="background:#2a2330;color:#c4b5fd;">数据旧</span>' : '');
    let footerNote = noMarketData
      ? '最近可用 K 线为空，当前仅展示空信号回退。'
      : (sig.market_data_last_bar_at
        ? `行情截至 ${fmtTs(sig.market_data_last_bar_at)}${isStale ? ' · 不是最新快照' : ''}`
        : '');

    const aggregatedAt = String(sig.aggregated_at || sig.timestamp || '').trim();
    const footerParts = [];
    if (aggregatedAt) footerParts.push(`聚合时间 ${fmtTs(aggregatedAt)}`);
    if (noMarketData) {
      footerParts.push('最近可用 K 线为空，当前仅展示聚合信号回退结果。');
    } else if (sig.market_data_last_bar_at) {
      footerParts.push(`行情截至 ${fmtTs(sig.market_data_last_bar_at)}${isStale ? ' / 数据偏旧' : ''}`);
    }
    footerNote = footerParts.join(' · ');

    return `<div class="live-sig-row">
  <div class="live-sig-header">
    <div class="live-sig-title-wrap">
      <div class="live-sig-title-row">${liveSignalPrimaryTitle(item)}</div>
      ${metaText ? `<div class="live-sig-meta">${esc(metaText)}</div>` : ''}
    </div>
    <span style="font-weight:700;font-size:13px;margin-left:auto;color:${liveSignalDirColor(signalDir)}">${liveSignalDirIcon(signalDir)} ${signalDir}</span>
    ${blockedBadge}${approvalBadge}${dataBadge}
  </div>
  <div class="live-sig-bars">
    ${['llm', 'ml', 'factor'].map(k => {
      const c = comp[k] || {};
      const mlOffline = k === 'ml' && !mlLoaded;
      return `<span class="live-sig-bar-label"${mlOffline ? ' style="opacity:.45"' : ''}>${k.toUpperCase()}${mlOffline ? '?' : ''}</span>`
           + `<span style="color:${mlOffline ? '#6b7fa0' : liveSignalDirColor(c.direction)};font-size:10px">${mlOffline ? '•' : liveSignalDirIcon(c.direction || 'FLAT')}</span>`
           + `<span style="font-size:10px;min-width:26px;text-align:right;${mlOffline ? 'opacity:.45' : ''}">${mlOffline ? '--' : liveSignalPct(c.confidence)}</span>`;
    }).join('')}
    <span style="font-size:10px;color:#6b7fa0;margin-left:4px">合计</span>
    <span style="font-size:11px;font-weight:600">${liveSignalPct(sig.confidence)}</span>
  </div>
  ${footerNote ? `<div style="margin-top:6px;font-size:10px;color:#7e92b2;">${esc(footerNote)}</div>` : ''}
</div>`;
  }

  function renderLiveSignalSection(section, mlLoaded) {
    const items = Array.isArray(section?.items) ? section.items : [];
    return `<div class="live-sig-section">
  <div class="live-sig-section-title">
    <span>${esc(section?.title || '--')}</span>
    <span>${items.length}</span>
  </div>
  ${items.length
    ? items.map(item => renderLiveSignalRow(item, mlLoaded)).join('')
    : `<div class="live-sig-empty">${esc(section?.empty_text || '暂无数据')}</div>`}
</div>`;
  }

  function liveSignalSectionById(payload, sectionId) {
    return liveSignalSections(payload).find(section => section?.id === sectionId) || null;
  }

  function renderLiveSignalPanel(targetId, section, mlLoaded) {
    const el = document.getElementById(targetId);
    if (!el) return;

    const resolvedSection = section || { title: '--', empty_text: '暂无数据', items: [] };
    const itemCount = Array.isArray(resolvedSection?.items) ? resolvedSection.items.length : 0;
    const mlNote = (itemCount > 0 && !mlLoaded)
      ? '<div style="font-size:10px;color:#78350f;background:#451a03;border-radius:4px;padding:2px 6px;margin-bottom:4px;">ML 组件未激活，当前信号仅使用 LLM + Factor。</div>'
      : '';

    el.innerHTML = mlNote + renderLiveSignalSection(resolvedSection, mlLoaded);
  }

  function renderLiveSignalPanelError(targetId, message) {
    const el = document.getElementById(targetId);
    if (!el) return;
    const detail = normalizeUiText(compactText(message || 'Signals failed to load', 160));
    el.innerHTML = `<div class="live-sig-empty">${esc(detail)}</div>`;
  }

  function renderLiveSignalPanels(researchPayload, agentPayload) {
    renderLiveSignalPanel(
      'ai-research-live-signals-panel',
      liveSignalSectionById(researchPayload, 'candidates'),
      !!researchPayload?.ml_model_loaded,
    );
    renderLiveSignalPanel(
      'ai-agent-live-signals-panel',
      liveSignalSectionById(agentPayload, 'watchlist'),
      !!agentPayload?.ml_model_loaded,
    );
  }

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

  /* 暴露缁欏閮ㄨ皟鐢紙兼容鏃т唬鐮侊級 */
  window.AI = {
    viewCandidate:   id => viewCandidate(id).catch(err => notify(`加载详情失败: ${err.message}`, true)),
    openRegister:    id => openRegisterModal(id).catch(err => notify(`打开注册失败: ${err.message}`, true)),
    runProposal:     id => withActionLock('run', () => runProposal(id)).catch(err => notify(`运行失败: ${err.message}`, true)),
    toggleCompare:   id => toggleCandidateCompare(id),
    showComparePanel: () => openCompareModal(),
    refreshWorkbench,
    getSnapshot: () => getWorkbenchSnapshot(),
    emitState: (reason, extra = {}) => emitWorkbenchState(reason, extra),
    util: {
      esc,
      fmtTs,
      notify,
      normalizeUiText,
      firstMeaningfulText,
      providerDisplayName,
      proposalResearchThesis,
      researchModeText,
      statusText,
      proposalDisplayName,
    },
    modules: window.AI?.modules || {},
  };

  window.agentStart = agentStart;
  window.agentStop = agentStop;
  window.agentRunOnce = agentRunOnce;

})();
