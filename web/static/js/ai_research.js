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
  const state = {
    proposals: [],
    candidates: [],
    pendingApprovals: [],   // candidates with human gate
    pendingLlmContext: null, // last AI-generated research context
    runtimeConfig: null,    // { governance_enabled, decision_mode, trading_mode }
    runtimeConfigLoaded: false,
    selectedProposalId: '',
    selectedCandidateId: '',
    latestSignals: {},
    signalTimer: null,
    refreshTimer: null,
    signalLoading: false,
    signalPanelCollapsed: false,
    jobPollingTimers: {},   // proposalId → intervalId
    sortBy: 'score',        // 'score' | 'sharpe' | 'return' | 'drawdown'
    filterCategory: '',     // '' | '趋势' | '震荡' | ...
  };

  /* ── 工具函数 ── */
  function esc(v) {
    return String(v ?? '').replace(/[&<>"']/g, m =>
      ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[m]));
  }

  function notify(msg, isError = false) {
    if (typeof window.notify === 'function') { window.notify(msg, !!isError); return; }
    const box = document.getElementById('notification');
    if (!box) return;
    box.textContent = String(msg || '');
    box.className = `notification show${isError ? ' error' : ''}`;
    setTimeout(() => box.classList.remove('show'), 3000);
  }

  function csvInput(id) {
    return String(document.getElementById(id)?.value || '').split(',').map(s => s.trim()).filter(Boolean);
  }

  function fmtTs(v) {
    if (!v) return '--';
    const d = new Date(String(v).replace(' ', 'T'));
    if (!Number.isFinite(d.getTime())) return '--';
    return d.toLocaleString('zh-CN', { hour12: false });
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

  /* 状态文本 */
  function statusText(s) {
    return { draft:'草稿', research_queued:'排队中', research_running:'研究中',
             validated:'已验证', rejected:'已拒绝', paper_running:'纸盘运行',
             shadow_running:'影子追踪', live_candidate:'实盘候选', retired:'已退役',
             new:'新建', }[String(s || '')] || String(s || '--');
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
    box.innerHTML = entries.map(([sym, data]) => {
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
    }).join('');
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
     研究队列（Proposal 紧凑列表）
  ══════════════════════════════════════════════════════════════ */
  function renderProposalList() {
    const box   = document.getElementById('ai-proposal-list');
    const badge = document.getElementById('ai-queue-badge');
    if (!box) return;
    if (badge) badge.textContent = state.proposals.length ? `${state.proposals.length} 项` : '';
    if (!state.proposals.length) {
      box.innerHTML = '<div style="color:#6b7fa0;font-size:12px;padding:8px 0;">暂无研究任务</div>';
      return;
    }
    box.innerHTML = state.proposals.map((item, idx) => {
      const pid  = String(item?.proposal_id || '');
      const sel  = pid === state.selectedProposalId ? ' selected' : '';
      const st   = String(item?.status || 'draft');
      const dotCls = { research_running:'running', research_queued:'queued',
                       validated:'validated', rejected:'rejected' }[st] || '';
      const name = proposalDisplayName(item, idx);
      const running = ['research_queued','research_running'].includes(st);
      const runnable = isRunnableProposalStatus(st);
      return `<div class="proposal-compact-item${sel}" data-proposal-id="${esc(pid)}" data-proposal-status="${esc(st)}" data-action="select-proposal">
        <div class="pci-dot ${dotCls}" title="${esc(statusText(st))}"></div>
        <div class="pci-name" title="${esc(name)}">${esc(name)}</div>
        <div class="pci-actions">
          ${running
            ? `<button class="btn btn-sm" style="padding:1px 6px;font-size:11px;color:#f0b429;" data-action="cancel-proposal" data-proposal-id="${esc(pid)}" title="取消运行">■</button>`
            : (runnable
              ? `<button class="btn btn-sm" style="padding:1px 6px;font-size:11px;" data-action="run-proposal" data-proposal-id="${esc(pid)}" title="运行回测">▶</button>`
              : '<span style="font-size:10px;color:#7e92b2;">不可运行</span>')}
          <button class="btn btn-sm" style="padding:1px 6px;font-size:11px;color:#e05260;" data-action="delete-proposal" data-proposal-id="${esc(pid)}" title="删除">✕</button>
        </div>
      </div>`;
    }).join('');
  }

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
  function renderCandidateCards() {
    const box = document.getElementById('ai-candidate-cards');
    const cnt = document.getElementById('ai-candidate-count');
    if (!box) return;

    let visible = [...state.candidates];
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

    if (cnt) cnt.textContent = visible.length
      ? `${visible.length}/${state.candidates.length} 个`
      : (state.candidates.length ? `0/${state.candidates.length} (筛选后为空)` : '');

    if (!visible.length) {
      box.innerHTML = state.candidates.length
        ? `<div class="ai-empty-hint">当前类别筛选无结果，请调整筛选条件</div>`
        : `<div class="ai-empty-hint">暂无候选策略。<br>在左侧填写研究目标，点击 <strong>生成研究</strong>，<br>再选中研究任务并点击 <strong>▶ 运行研究</strong> 开始回测。</div>`;
      return;
    }
    box.innerHTML = visible.map(c => buildCandidateCard(c)).join('');
  }

  function buildCandidateCard(cand) {
    const score  = Number(cand?.score || 0);
    const color  = scoreColor(score);
    const emoji  = scoreEmoji(score);
    const cid    = String(cand?.candidate_id || '');
    const strat  = String(cand?.strategy || '--');
    const sym    = String(cand?.symbol || '--');
    const tf     = String(cand?.timeframe || '--');
    const status = String(cand?.status || 'new');
    const decision = cand?.promotion?.decision || cand?.promotion_target || '';
    const sel    = cid === state.selectedCandidateId ? ' selected' : '';

    // 从 top_results 提取核心指标
    const top = candidateTopResults(cand)[0] || {};
    const ret    = top.total_return   != null ? Number(top.total_return)   : null;
    const dd     = top.max_drawdown   != null ? Number(top.max_drawdown)   : null;
    const wr     = top.win_rate       != null ? Number(top.win_rate) * 100 : null;
    const sr     = top.sharpe_ratio   != null ? Number(top.sharpe_ratio)   : null;

    const retStr = ret != null ? `<strong style="color:${ret >= 0 ? '#20bf78' : '#e05260'}">${ret >= 0 ? '+' : ''}${ret.toFixed(1)}%</strong>` : '<strong>--</strong>';
    const ddStr  = dd  != null ? `<strong style="color:#e05260">${dd.toFixed(1)}%</strong>` : '<strong>--</strong>';
    const wrStr  = wr  != null ? `<strong>${wr.toFixed(0)}%</strong>` : '<strong>--</strong>';
    const srStr  = sr  != null ? `<strong>${sr.toFixed(2)}</strong>` : '<strong>--</strong>';

    // C: IS/OOS/WF badges
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
      wfBadge = `<span style="font-size:10px;padding:1px 5px;border-radius:3px;background:${wfClr}22;color:${wfClr};border:1px solid ${wfClr}44;">WF ${(wfs*100).toFixed(0)}%</span>`;
    }

    // DSR badge
    const dsrScore = vs.dsr_score;
    const dsrColor = dsrScore != null ? (dsrScore >= 0.5 ? '#2a7a2a' : '#7a2a2a') : '#444';
    const dsrBadge = dsrScore != null
      ? `<span class="cand-badge" style="background:${dsrColor};color:#fff;padding:2px 5px;border-radius:3px;font-size:10px;margin-left:2px;">DSR ${(dsrScore*100).toFixed(0)}%</span>`
      : '';
    // opt_method badge
    const optMethod = (cand?.metadata && cand.metadata.opt_method) || '';
    const optBadge = optMethod
      ? `<span class="cand-badge" style="background:#1a3a5a;color:#fff;padding:2px 5px;border-radius:3px;font-size:10px;margin-left:2px;">${optMethod === 'scipy_lhs' ? '🔬 Bayes' : '📊 Grid'}</span>`
      : '';
    // Correlation filter badge
    const corrFiltered  = cand?.metadata?.correlation_filtered;
    const corrWith      = cand?.metadata?.correlated_with || '';
    const corrVal       = cand?.metadata?.correlation_value;
    const corrIsCross   = cand?.metadata?.correlation_is_cross_batch;
    const corrLabel     = corrIsCross ? '⚠ 跨批相关' : '⚠ 相关';
    const corrBadge = corrFiltered
      ? `<span class="cand-badge" style="background:#7a3a2a;color:#fff;padding:2px 5px;border-radius:3px;font-size:10px;margin-left:2px;" title="与 ${esc(corrWith)} 相关 ρ=${corrVal}">${corrLabel}</span>`
      : '';

    // B: best_params badge (show trial count)
    const bestParams = cand?.params || {};
    const trials = cand?.metadata?.best?.optimization_trials;
    let paramsBadge = '';
    if (trials > 0) {
      paramsBadge = `<span style="font-size:10px;padding:1px 5px;border-radius:3px;background:#a78bfa22;color:#a78bfa;border:1px solid #a78bfa44;">🔧 ${trials} trials</span>`;
    }

    // 信号徽章：若已拉取到该 symbol 的信号则显示
    let signalBadge = '';
    const sigData = state.latestSignals[sym];
    if (sigData && String(sigData.direction || '') !== 'FLAT') {
      const dir  = String(sigData.direction).toUpperCase();
      const conf = Math.round(Number(sigData.confidence || 0) * 100);
      const dirLabel = { LONG:'看多', SHORT:'看空' }[dir] || dir;
      signalBadge = `<span class="cand-signal-badge">📡 ${esc(sym.split('/')[0])} ${dirLabel} ${conf}%</span><br>`;
    }

    const canRegister = canRegisterCandidate(cand);

    const category = STRATEGY_CATEGORIES[strat] || '';
    const catColor  = CATEGORY_COLORS[category] || '#64748b';
    const catBadge  = category
      ? `<span class="cand-category-badge" style="background:${catColor}22;color:${catColor};border:1px solid ${catColor}44;">${esc(category)}</span>`
      : '';

    return `<div class="research-candidate-card score-${color}${sel}"
               data-candidate-id="${esc(cid)}" data-action="select-candidate">
      <div class="cand-card-header">
        <div class="cand-card-title">${emoji} ${esc(strat)}</div>
        <div style="display:flex;align-items:center;gap:5px;">${catBadge}<div class="cand-score-badge ${color}">${score.toFixed(0)}</div></div>
      </div>
      <div style="font-size:12px;color:#7e92b2;margin-bottom:5px;">
        ${esc(sym)} · ${esc(tf)} · ${esc(statusText(status))}
      </div>
      <div class="cand-score-bar">
        <div class="cand-score-bar-fill ${color}" style="width:${Math.min(100, score).toFixed(0)}%;"></div>
      </div>
      <div class="cand-metrics">
        <div class="cand-metric-item">年化 ${retStr}</div>
        <div class="cand-metric-item">回撤 ${ddStr}</div>
        <div class="cand-metric-item">胜率 ${wrStr}</div>
        <div class="cand-metric-item">夏普 ${srStr}</div>
      </div>
      ${oosBadge || wfBadge || paramsBadge || dsrBadge || optBadge || corrBadge ? `<div style="display:flex;gap:4px;flex-wrap:wrap;margin-top:4px;">${oosBadge}${wfBadge}${paramsBadge}${dsrBadge}${optBadge}${corrBadge}</div>` : ''}
      ${signalBadge}
      <div class="cand-recommendation">AI推荐：${esc(promotionText(decision))}</div>
      <div class="cand-card-actions">
        <button class="btn btn-sm" data-action="view-candidate" data-candidate-id="${esc(cid)}" style="font-size:12px;">详情</button>
        ${canRegister ? `<button class="btn-register-cta" data-action="open-register" data-candidate-id="${esc(cid)}">一键注册策略 →</button>` : ''}
      </div>
    </div>`;
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

  function renderLifecycleRows(rows, emptyText = 'No lifecycle records') {
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

  function renderRunRows(rows, emptyText = 'No experiment runs') {
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
          <div style="color:#7e92b2;margin-top:2px;">run: ${esc(String(item?.run_id || '--'))}</div>
        </div>
      `).join('')}
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
            <div style="font-size:10px;color:#6b7fa0;">DSR Score</div>
            <div style="font-size:14px;font-weight:700;color:#c2d0e8;">${dsrVal}</div>
          </div>
          <div style="text-align:center;padding:6px;background:#1a2436;border-radius:4px;">
            <div style="font-size:10px;color:#6b7fa0;">WF Consistency</div>
            <div style="font-size:14px;font-weight:700;color:#c2d0e8;">${wfConsist}</div>
          </div>
        </div>
      </div>`;

    const equityCurve = normalizeNumberSeries(cand?.metadata?.best?.equity_curve_sample || []);
    const equityCurveHtml = `
      <div style="margin-bottom:14px;">
        <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">Equity Curve Sample</div>
        ${equityCurve.length >= 2
          ? renderSparklineSvg(equityCurve)
          : '<div style="font-size:12px;color:#6b7fa0;">No equity curve sample.</div>'}
      </div>`;

    const artifactsHtml = `
      <div style="margin-bottom:14px;">
        <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">Research Artifacts</div>
        <div style="font-size:12px;color:#b7c7e2;background:#141f2f;border-radius:6px;padding:8px;">
          <div>CSV: ${esc(String(cand?.metadata?.csv_path || '--'))}</div>
          <div style="margin-top:4px;">Markdown: ${esc(String(cand?.metadata?.markdown_path || '--'))}</div>
        </div>
      </div>`;

    const experimentHtml = `
      <div style="margin-bottom:14px;">
        <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">Experiment</div>
        <div style="font-size:12px;color:#b7c7e2;background:#141f2f;border-radius:6px;padding:8px;margin-bottom:6px;">
          <div>Experiment ID: ${esc(String(experimentId || '--'))}</div>
          <div style="margin-top:4px;">Status: ${esc(String(experimentInfo?.status || '--'))}</div>
        </div>
        ${renderRunRows(experimentRuns)}
      </div>`;

    const lifecycleHtml = `
      <div style="margin-bottom:14px;">
        <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">Candidate Lifecycle</div>
        ${renderLifecycleRows(candidateLifecycle, 'No candidate lifecycle records')}
      </div>
      <div style="margin-bottom:14px;">
        <div style="font-size:11px;color:#9fb1c9;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:6px;">Proposal Lifecycle</div>
        ${renderLifecycleRows(proposalLifecycle, 'No proposal lifecycle records')}
      </div>`;

    panel.innerHTML = `
      <div style="margin-bottom:14px;">
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:6px;">
          <span style="font-size:15px;font-weight:700;color:#c2d0e8;">${esc(cand?.strategy || '--')}</span>
          <span class="cand-score-badge ${color}" style="font-size:13px;">${score.toFixed(0)} 分</span>
        </div>
        <div style="font-size:12px;color:#7e92b2;">
          ${esc(cand?.symbol || '--')} · ${esc(cand?.timeframe || '--')} · ${esc(statusText(cand?.status))}
        </div>
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
      ${equityCurveHtml}
      ${artifactsHtml}
      ${experimentHtml}
      ${lifecycleHtml}
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
              <option value="shadow" ${recTarget === 'shadow' ? 'selected' : ''}>影子追踪 (shadow)</option>
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
        ? `<button class="btn-register-cta full" data-action="open-register" data-candidate-id="${esc(candidateId)}">
            一键注册策略 →
          </button>`
        : (governanceGateHint
          ? `<div style="font-size:12px;color:#f0b429;background:#2b1f06;border:1px solid #5c4310;border-radius:6px;padding:8px 10px;">
              治理模式已开启：请使用上方“待人工审批”进行批准/拒绝。
            </div>`
          : '')}`;

    // 绑定详情面板里的按钮
    panel.querySelector('.btn-register-cta')?.addEventListener('click', () => {
      openRegisterModal(candidateId).catch(err => notify(`打开注册失败: ${err.message}`, true));
    });

    // 人工审批按钮
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
    const wr  = top.win_rate       != null ? Number(top.win_rate) * 100 : null;
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

    document.getElementById('reg-cancel-btn').onclick  = () => { modal.style.display = 'none'; };
    document.getElementById('reg-confirm-btn').onclick = () => {
      const name = String(document.getElementById('reg-name')?.value || '').trim();
      const mode = document.querySelector('input[name="reg-mode"]:checked')?.value || 'paper';
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

    list.innerHTML = items.map(cand => {
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
        <div style="display:flex;gap:6px;">
          <button class="btn btn-sm" style="font-size:11px;color:#20bf78;border-color:#20bf78;"
            data-action="human-approve" data-candidate-id="${cid}" data-target="${target}">✓ 批准</button>
          <button class="btn btn-sm" style="font-size:11px;color:#e05260;border-color:#e05260;"
            data-action="human-reject" data-candidate-id="${cid}">✗ 拒绝</button>
        </div>
      </div>`;
    }).join('');
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
      const marketSummary = state.latestSignals || {};
      const result = await aiApi('/research/generate-context', {
        method: 'POST',
        body: JSON.stringify({ market_summary: marketSummary, goals, timeout: 30 }),
        timeoutMs: 40000,
      });
      if (result?.llm_research_output) {
        state.pendingLlmContext = result.llm_research_output;
        if (btn) { btn.textContent = 'AI建议已生成 ✓'; btn.disabled = false; btn.style.color = '#20bf78'; }
        // Show hypothesis in planner notes
        const plannerNotesEl = document.getElementById('ai-planner-notes');
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
      };
      state.runtimeConfigLoaded = true;
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
        state.runtimeConfig = { governance_enabled: false, decision_mode: '', trading_mode: '' };
      }
      state.runtimeConfigLoaded = true;
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
    await Promise.all([loadProposals(selectProposalId), loadCandidates(selectCandidateId), loadPendingApprovals()]);
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
  async function generateProposal() {
    const goal = String(document.getElementById('ai-planner-goal')?.value || '').trim();
    if (goal.length < 8) { notify('研究目标太短（至少8个字符）', true); return; }
    const payload = {
      goal,
      market_regime: String(document.getElementById('ai-planner-regime')?.value || 'mixed'),
      symbols:       csvInput('ai-planner-symbols'),
      timeframes:    csvInput('ai-planner-timeframes'),
      constraints:   { max_templates: Number(document.getElementById('ai-planner-max-templates')?.value || 5) },
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

  async function runProposal(proposalId) {
    if (!proposalId) { notify('请先选择研究任务', true); return; }
    const proposal = state.proposals.find(p => String(p?.proposal_id || '') === String(proposalId));
    const proposalStatus = String(proposal?.status || '');
    if (proposal && !isRunnableProposalStatus(proposalStatus)) {
      notify(`当前状态「${statusText(proposalStatus)}」不可运行`, true);
      return;
    }
    const exchange = String(document.getElementById('run-exchange')?.value || 'binance');
    const days     = Math.max(1, Math.min(3650, parseInt(document.getElementById('run-days')?.value || '60', 10) || 60));
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

  /* ── 任务进度轮询 ── */
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
    });

    /* 刷新 */
    document.getElementById('ai-refresh-btn')?.addEventListener('click', () =>
      refreshWorkbench().catch(err => notify(`刷新失败: ${err.message}`, true)));

    /* 运行研究 */
    document.getElementById('run-selected-btn')?.addEventListener('click', () =>
      runProposal(state.selectedProposalId).catch(err => notify(`运行失败: ${err.message}`, true)));

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
    loadSignal().catch(() => {});
    state.signalTimer = setInterval(() => loadSignal().catch(() => {}), SIGNAL_INTERVAL_MS);
    state.refreshTimer = setInterval(() => refreshWorkbench().catch(() => {}), REFRESH_INTERVAL_MS);
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
    refreshWorkbench().catch(err => console.error('AI研究初始化失败:', err));
    startPolling();
  }

  window.addEventListener('load', init);
  window.addEventListener('beforeunload', () => {
    clearInterval(state.signalTimer);
    clearInterval(state.refreshTimer);
    Object.values(state.jobPollingTimers).forEach(t => clearInterval(t));
  });

  /* 暴露给外部调用（兼容旧代码） */
  window.AI = {
    viewCandidate:   id => viewCandidate(id).catch(err => notify(`加载详情失败: ${err.message}`, true)),
    openRegister:    id => openRegisterModal(id).catch(err => notify(`打开注册失败: ${err.message}`, true)),
    runProposal:     id => runProposal(id).catch(err => notify(`运行失败: ${err.message}`, true)),
    refreshWorkbench,
  };
})();
