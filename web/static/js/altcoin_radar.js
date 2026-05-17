(function () {
  const DEFAULT_UNIVERSE = [
    'BTC/USDT',
    'ETH/USDT',
    'BNB/USDT',
    'SOL/USDT',
    'XRP/USDT',
    'ADA/USDT',
    'DOGE/USDT',
    'TRX/USDT',
    'LINK/USDT',
    'AVAX/USDT',
    'DOT/USDT',
    'POL/USDT',
    'LTC/USDT',
    'BCH/USDT',
    'ETC/USDT',
    'ATOM/USDT',
    'NEAR/USDT',
    'APT/USDT',
    'ARB/USDT',
    'OP/USDT',
    'SUI/USDT',
    'INJ/USDT',
    'RUNE/USDT',
    'AAVE/USDT',
    'MKR/USDT',
    'UNI/USDT',
    'FIL/USDT',
    'HBAR/USDT',
    'ICP/USDT',
    'TON/USDT',
  ];

  const PRESET_BY_KIND = {
    anomaly: '点火预警',
    accumulation: '跃升预警',
    control: '拥挤预警',
    narrative: '叙事预警',
  };

  const ALERT_ACTION_LABEL_BY_KIND = {
    anomaly: '点火预警',
    accumulation: '跃升预警',
    control: '拥挤预警',
    narrative: '叙事预警',
  };

  const INSPECTOR_ALERT_BUTTONS = [
    ['btn-altcoin-radar-alert-anomaly', 'anomaly'],
    ['btn-altcoin-radar-alert-layout', 'accumulation'],
    ['btn-altcoin-radar-alert-control', 'control'],
    ['btn-altcoin-radar-alert-narrative', 'narrative'],
  ];

  const CLIENT_FILTER_CONTROL_IDS = ['altcoin-radar-filter', 'altcoin-radar-only-alerted'];
  const SCAN_DEBOUNCE_MS = 250;
  const BACKGROUND_REFRESH_POLL_MS = 4500;
  const BACKGROUND_REFRESH_MAX_POLLS = 6;

  const state = {
    bound: false,
    dom: Object.create(null),
    universeLoadedFor: '',
    universeCatalog: null,
    scan: null,
    detail: null,
    selectedSymbol: '',
    filteredRows: [],
    scanSeq: 0,
    detailSeq: 0,
    eventSeq: 0,
    radarMode: 'combined',  // Phase 1
    watchlist: [],
    eventTimelineBySymbol: new Map(),
    scanDebounceTimer: 0,
    backgroundRefreshTimer: 0,
    backgroundRefreshKey: '',
    backgroundRefreshPolls: 0,
    scanInFlight: false,
    operatingModeInFlight: null,
    operatingModeLoadedAt: 0,
  };

  function q(id) {
    if (!id) return null;
    const cached = state.dom[id];
    if (cached && cached.isConnected) return cached;
    const node = document.getElementById(id);
    if (node) state.dom[id] = node;
    return node;
  }

  function eachInspectorAlertButton(callback) {
    INSPECTOR_ALERT_BUTTONS.forEach(([id, kind]) => {
      const button = q(id);
      if (button) callback(button, kind);
    });
  }

  function escapeHtml(value) {
    return String(value == null ? '' : value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function toNumber(value, fallback = 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : fallback;
  }

  function toPercent(value, digits = 0) {
    const num = toNumber(value, NaN);
    if (!Number.isFinite(num)) return '--';
    return `${(num * 100).toFixed(digits)}%`;
  }

  function shortPercent(value) {
    const num = toNumber(value, NaN);
    if (!Number.isFinite(num)) return '--';
    return num.toFixed(2);
  }

  function shortNumber(value, digits = 2) {
    const num = toNumber(value, NaN);
    if (!Number.isFinite(num)) return '--';
    return num.toFixed(digits);
  }

  function fmtAge(seconds) {
    const sec = Math.max(0, Math.round(toNumber(seconds, 0)));
    if (!Number.isFinite(sec)) return '--';
    if (sec < 60) return `${sec}s`;
    if (sec < 3600) return `${Math.round(sec / 60)}m`;
    if (sec < 86400) return `${Math.round(sec / 3600)}h`;
    return `${Math.round(sec / 86400)}d`;
  }

  function fmtDateTime(value) {
    if (!value) return '--';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value);
    return date.toLocaleString('zh-CN', { hour12: false });
  }

  function normalizeSymbols(values) {
    const out = [];
    const seen = new Set();
    (Array.isArray(values) ? values : [values]).forEach((item) => {
      const text = String(item || '').trim();
      if (!text || seen.has(text)) return;
      seen.add(text);
      out.push(text);
    });
    return out;
  }

  function getSelectedValues(id) {
    const el = q(id);
    if (!(el instanceof HTMLSelectElement)) return [];
    if (!el.multiple) {
      const value = String(el.value || '').trim();
      return value ? [value] : [];
    }
    return Array.from(el.selectedOptions || [])
      .map((opt) => String(opt.value || '').trim())
      .filter(Boolean);
  }

  function setSelectedValues(id, values, fallback = '') {
    const el = q(id);
    if (!(el instanceof HTMLSelectElement)) return;
    const chosen = new Set(normalizeSymbols(values));
    if (el.multiple) {
      Array.from(el.options || []).forEach((opt) => {
        opt.selected = chosen.has(String(opt.value || '').trim());
      });
      if (!Array.from(el.selectedOptions || []).length && el.options.length) {
        const target = normalizeSymbols(fallback ? [fallback] : [el.options[0].value])[0];
        Array.from(el.options || []).forEach((opt, idx) => {
          opt.selected = String(opt.value || '').trim() === target || (!target && idx === 0);
        });
      }
      return;
    }
    const target = normalizeSymbols(values)[0] || fallback || String(el.value || '').trim();
    if (target) el.value = target;
  }

  function readControls() {
    return {
      exchange: String(q('altcoin-radar-exchange')?.value || 'binance').trim().toLowerCase() || 'binance',
      timeframe: String(q('altcoin-radar-timeframe')?.value || '4h').trim() || '4h',
      sortBy: String(q('altcoin-radar-sort')?.value || 'layout').trim() || 'layout',
      filter: String(q('altcoin-radar-filter')?.value || 'all').trim() || 'all',
      onlyAlerted: !!q('altcoin-radar-only-alerted')?.checked,
      excludeRetired: q('altcoin-radar-exclude-retired')?.checked !== false,
      universeSymbols: normalizeSymbols(getSelectedValues('altcoin-radar-universe')).slice(0, 30),
      // Phase 1
      radarMode: state.radarMode || 'combined',
      universeScope: String(q('altcoin-radar-universe-scope')?.value || 'research').trim() || 'research',
    };
  }

  function scanControlKey(controls = readControls()) {
    return JSON.stringify({
      exchange: controls.exchange,
      timeframe: controls.timeframe,
      sortBy: controls.sortBy,
      excludeRetired: controls.excludeRetired,
      universeSymbols: controls.universeSymbols,
      radarMode: controls.radarMode,
      universeScope: controls.universeScope,
    });
  }

  function clearBackgroundRefreshPoll() {
    if (state.backgroundRefreshTimer) {
      window.clearTimeout(state.backgroundRefreshTimer);
      state.backgroundRefreshTimer = 0;
    }
  }

  function setScanBusy(isBusy) {
    state.scanInFlight = Boolean(isBusy);
    ['btn-altcoin-radar-refresh', 'btn-altcoin-radar-force-refresh'].forEach((id) => {
      const btn = q(id);
      if (btn) btn.disabled = state.scanInFlight;
    });
  }

  function scheduleScan(refresh = false, options = {}) {
    if (state.scanDebounceTimer) {
      window.clearTimeout(state.scanDebounceTimer);
      state.scanDebounceTimer = 0;
    }
    const delay = Math.max(0, Number(options.delayMs ?? SCAN_DEBOUNCE_MS));
    state.scanDebounceTimer = window.setTimeout(() => {
      state.scanDebounceTimer = 0;
      scanRadar(refresh, options).catch((error) => {
        if (typeof notify === 'function' && options.notifyMessage) {
          notify(`${options.notifyMessage}: ${error.message}`, true);
        }
      });
    }, delay);
  }

  function scheduleBackgroundRefreshPoll(cache = {}, controls = readControls()) {
    clearBackgroundRefreshPoll();
    if (!cache?.refreshing) {
      state.backgroundRefreshKey = '';
      state.backgroundRefreshPolls = 0;
      return;
    }
    const key = `${String(cache.cache_key || '')}|${scanControlKey(controls)}`;
    if (state.backgroundRefreshKey !== key) {
      state.backgroundRefreshKey = key;
      state.backgroundRefreshPolls = 0;
    }
    if (state.backgroundRefreshPolls >= BACKGROUND_REFRESH_MAX_POLLS) return;
    state.backgroundRefreshPolls += 1;
    state.backgroundRefreshTimer = window.setTimeout(() => {
      state.backgroundRefreshTimer = 0;
      const nextControls = readControls();
      const nextKey = `${String(cache.cache_key || '')}|${scanControlKey(nextControls)}`;
      if (nextKey !== key || document.hidden) return;
      scanRadar(false, {
        backgroundPoll: true,
        preserveStatus: true,
        notifyMessage: '',
      }).catch(() => {});
    }, BACKGROUND_REFRESH_POLL_MS);
  }

  function normalizeWatchlistSymbolInput(symbol) {
    const raw = String(symbol || '').trim().toUpperCase().replace(/\s+/g, '');
    if (!raw) return '';
    if (raw.includes('/')) return raw;
    if (raw.includes('-')) return raw.replace('-', '/');
    if (raw.endsWith('USDT') && raw.length > 4) return `${raw.slice(0, -4)}/USDT`;
    return `${raw}/USDT`;
  }

  function universeScopeLabel(scope) {
    if (scope === 'watchlist') return 'Watchlist 收藏列表';
    if (scope === 'expanded') return '扩展扫描（自动补齐 ~100）';
    return '研究清单（使用下方多选）';
  }

  function buildSelectionPlaceholder(symbol) {
    const normalized = normalizeWatchlistSymbolInput(symbol);
    if (!normalized) return null;
    const inWatchlist = normalizeSymbols(state.watchlist || []).includes(normalized);
    const scopeLabel = universeScopeLabel(readControls().universeScope);
    return {
      symbol: normalized,
      tags: inWatchlist ? ['Watchlist', '榜外观察'] : ['榜外观察'],
      in_watchlist: inWatchlist,
      sector: inWatchlist ? 'watchlist' : '',
      reasons_proxy: [
        `${normalized} 当前不在本次 ${scopeLabel} 榜单中。`,
        '可以继续等待详情补载，或把扫描范围切到 Watchlist 收藏列表。',
      ],
      reasons_chain: [
        '当前先展示 Watchlist 占位信息，链上 / 外生确认稍后补齐。',
      ],
      data_quality: {},
      metrics: {},
      placeholder_reason: `${normalized} 当前不在本次 ${scopeLabel} 榜单中，先展示 Watchlist 占位信息。`,
    };
  }

  function resolveSelectedRow(symbol) {
    const normalized = String(symbol || '').trim().toUpperCase();
    if (!normalized) return null;
    return findRow(normalized) || buildSelectionPlaceholder(normalized);
  }

  function shouldKeepSelectedSymbol(rows, symbol) {
    const normalized = String(symbol || '').trim().toUpperCase();
    if (!normalized) return false;
    if ((Array.isArray(rows) ? rows : []).some((row) => String(row?.symbol || '').trim().toUpperCase() === normalized)) {
      return true;
    }
    return normalizeSymbols(state.watchlist || []).includes(normalized);
  }

  function currentUniverseSymbols() {
    const controls = readControls();
    const used = normalizeSymbols(state.scan?.scan_meta?.symbols_used || state.scan?.summary?.symbols_used || []);
    if (controls.universeScope !== 'research') {
      if (used.length) return used;
      if (controls.universeScope === 'watchlist') return normalizeSymbols(state.watchlist || []);
    }
    const selected = controls.universeSymbols;
    if (selected.length) return selected;
    return used.length ? used : DEFAULT_UNIVERSE.slice(0, 12);
  }

  function alertRulesForRow(row) {
    return Array.isArray(row?.alert_rules) ? row.alert_rules : [];
  }

  function alertKindsForRow(row) {
    return new Set(
      alertRulesForRow(row)
        .map((item) => String(item?.kind || '').trim())
        .filter(Boolean)
    );
  }

  function syncRowAlertState(target, alertRules) {
    if (!target || typeof target !== 'object') return target;
    const normalizedRules = Array.isArray(alertRules) ? alertRules.map((item) => ({ ...(item || {}) })) : [];
    const tags = Array.isArray(target.tags) ? target.tags.filter((tag) => tag !== '已建预警') : [];
    if (normalizedRules.length) tags.push('已建预警');
    return {
      ...target,
      has_alert_rule: normalizedRules.length > 0,
      alert_rules: normalizedRules,
      tags,
    };
  }

  function updateSymbolAlertRules(symbol, alertRules) {
    const normalized = String(symbol || '').trim().toUpperCase();
    if (!normalized) return;
    if (Array.isArray(state.scan?.rows)) {
      state.scan.rows = state.scan.rows.map((row) => {
        if (String(row?.symbol || '').trim().toUpperCase() !== normalized) return row;
        return syncRowAlertState(row, alertRules);
      });
    }
    if (state.detail?.selected_row && String(state.detail.selected_row.symbol || '').trim().toUpperCase() === normalized) {
      state.detail.selected_row = syncRowAlertState(state.detail.selected_row, alertRules);
    }
  }

  function upsertAlertRuleForSymbol(symbol, rule) {
    const normalized = String(symbol || '').trim().toUpperCase();
    if (!normalized) return;
    const currentRow = findRow(normalized) || state.detail?.selected_row || {};
    const existing = alertRulesForRow(currentRow)
      .filter((item) => String(item?.id || '').trim() !== String(rule?.id || '').trim());
    existing.push({ ...(rule || {}) });
    updateSymbolAlertRules(normalized, existing);
  }

  function removeAlertRulesForSymbol(symbol, removedRules = [], kind = '') {
    const normalized = String(symbol || '').trim().toUpperCase();
    if (!normalized) return;
    const removedIds = new Set((Array.isArray(removedRules) ? removedRules : []).map((item) => String(item?.id || '').trim()).filter(Boolean));
    const currentRow = findRow(normalized) || state.detail?.selected_row || {};
    let nextRules = alertRulesForRow(currentRow);
    if (removedIds.size) {
      nextRules = nextRules.filter((item) => !removedIds.has(String(item?.id || '').trim()));
    } else if (kind) {
      nextRules = nextRules.filter((item) => String(item?.kind || '').trim() !== String(kind || '').trim());
    } else {
      nextRules = [];
    }
    updateSymbolAlertRules(normalized, nextRules);
  }

  function rerenderAlertState(symbol) {
    const normalized = String(symbol || state.selectedSymbol || '').trim().toUpperCase();
    const selectedRow = normalized ? findRow(normalized) : findRow(state.selectedSymbol);
    if (state.scan) renderScan(state.scan);
    if (normalized && String(state.selectedSymbol || '').trim().toUpperCase() === normalized) {
      if (state.detail) {
        renderInspector({
          ...state.detail,
          selected_row: selectedRow || state.detail.selected_row || null,
        });
      } else if (selectedRow) {
        renderInspector({ selected_row: selectedRow });
      } else {
        updateInspectorButtonState(findRow(state.selectedSymbol));
      }
      return;
    }
    updateInspectorButtonState(findRow(state.selectedSymbol));
  }

  function renderUniverseManager() {
    const controls = readControls();
    const summaryEl = q('altcoin-radar-universe-summary');
    const customGroup = q('altcoin-radar-custom-universe-group');
    const customNote = q('altcoin-radar-custom-universe-note');
    const watchlistSummary = q('altcoin-radar-watchlist-summary');
    const catalogMeta = state.universeCatalog || {};
    const scopeLabelMap = {
      research: '研究清单（使用下方多选）',
      expanded: '扩展扫描（自动补齐 ~100）',
      watchlist: 'Watchlist 收藏列表',
    };
    const currentScopeLabel = scopeLabelMap[controls.universeScope] || '研究清单（使用下方多选）';
    const selectedCount = controls.universeSymbols.length;
    const watchlistCount = Array.isArray(state.watchlist) ? state.watchlist.length : 0;
    const usedCount = normalizeSymbols(state.scan?.scan_meta?.symbols_used || state.scan?.summary?.symbols_used || []).length;
    if (summaryEl) {
      let text = `当前模式：${currentScopeLabel}`;
      if (controls.universeScope === 'research') {
        text += ` · 已选 ${selectedCount || 0} 个币种`;
      } else if (controls.universeScope === 'watchlist') {
        text += ` · Watchlist ${watchlistCount} 个成员`;
      } else if (usedCount) {
        text += ` · 当前扫描 ${usedCount} 个币种`;
      }
      const sourceText = universeSourceLabel(catalogMeta);
      if (sourceText) {
        text += ` · ${sourceText}`;
      }
      summaryEl.textContent = text;
    }
    if (customGroup) customGroup.hidden = controls.universeScope !== 'research';
    if (customNote) {
      customNote.textContent = controls.universeScope === 'research'
        ? '这里决定“研究清单”模式下具体扫描哪些币；切到其他模式时，这份多选列表会保留，但不会参与本次扫描。'
        : '当前不是“研究清单”模式，这份多选列表不会参与扫描。';
    }
    if (watchlistSummary) {
      watchlistSummary.textContent = `Watchlist 当前 ${watchlistCount} 个成员，用于叙事 / Meme / 板块观察收藏；点击任一标签会在右侧加载该币详情，切到“Watchlist 收藏列表”模式时它们会作为本次扫描来源。`;
    }
  }

  function tagTone(tag) {
    const text = String(tag || '').trim();
    if (!text) return 'muted';
    if (text.includes('吸筹')) return 'layout';
    if (text.includes('异动')) return 'anomaly';
    if (text.includes('高控盘')) return 'control';
    if (text.includes('派发') || text.includes('风险')) return 'danger';
    if (text.includes('预警')) return 'control';
    if (text === 'Perp Ignition') return 'ignition';
    if (text === 'Late Stage') return 'danger';
    if (text === 'Narrative') return 'narrative';
    if (text === 'Multi-Engine') return 'control';
    if (text === 'Watchlist') return 'muted';
    return 'muted';
  }

  // Phase 1+2: signal source display helpers
  function signalSourceLabel(source) {
    const map = {
      'perp_ignition': '点火',
      'perp_continuation': '延续',
      'crowded_late_stage': '末端',
      'narrative_ignition': '叙事',
      'narrative_confirmation': '叙事确认',
    };
    return map[source] || '';
  }

  function signalSourceClass(source) {
    if (source === 'perp_ignition') return 'altcoin-src-ignition';
    if (source === 'perp_continuation') return 'altcoin-src-continuation';
    if (source === 'crowded_late_stage') return 'altcoin-src-crowded';
    if (source === 'narrative_ignition') return 'altcoin-src-narrative';
    if (source === 'narrative_confirmation') return 'altcoin-src-narrative-confirm';
    return 'altcoin-src-empty';
  }

  function signalTone(signalState) {
    return tagTone(signalState);
  }

  function dataFreshnessLabel(row) {
    const market = String(row?.freshness?.market_label || '').trim();
    const snapshot = String(row?.freshness?.snapshot_label || '').trim();
    const degraded = Array.isArray(row?.data_quality?.degraded_reason) && row.data_quality.degraded_reason.length > 0;
    if (degraded) return 'degraded';
    if (market === 'fresh' && snapshot === 'fresh') return 'fresh';
    if (market === 'stale' || snapshot === 'stale') return 'stale';
    return 'watch';
  }

  function pickDefaultPreset(row) {
    if (String(row?.signal_source || '').trim() === 'perp_ignition') return 'anomaly';
    if (String(row?.signal_source || '').trim().startsWith('narrative_')) return 'narrative';
    if (toNumber(row?.rank_jump_score, 0) >= 0.30) return 'accumulation';
    if (toNumber(row?.crowding_late_score, 0) >= 0.65) return 'control';
    const stateText = String(row?.signal_state || '').trim();
    if (stateText.includes('异动')) return 'anomaly';
    if (stateText.includes('吸筹')) return 'accumulation';
    if (stateText.includes('高控盘') || stateText.includes('派发')) return 'control';
    return 'anomaly';
  }

  function setStatus(text, tone = 'neutral') {
    const el = q('altcoin-radar-status-note');
    if (!el) return;
    el.dataset.tone = tone;
    el.textContent = text;
  }

  function setOutput(text) {
    const el = q('altcoin-radar-output');
    if (el) el.textContent = text;
  }

  function emptyScanPayload() {
    return { rows: [], summary: {}, scan_meta: {}, warnings: [] };
  }

  function refreshCurrentScan() {
    renderScan(state.scan || emptyScanPayload());
  }

  function syncSelectedRankingRow() {
    const tbody = q('altcoin-radar-ranking-body');
    if (!tbody) return;
    const selectedSymbol = String(state.selectedSymbol || '').trim().toUpperCase();
    Array.from(tbody.querySelectorAll('tr[data-symbol]')).forEach((row) => {
      const rowSymbol = String(row.dataset.symbol || '').trim().toUpperCase();
      row.classList.toggle('is-selected', !!selectedSymbol && rowSymbol === selectedSymbol);
    });
  }

  function cacheTimelineEvents(symbol, events) {
    const normalized = String(symbol || '').trim().toUpperCase();
    if (!normalized || !Array.isArray(events)) return;
    state.eventTimelineBySymbol.set(normalized, events.map((event) => ({ ...(event || {}) })));
  }

  function getCachedTimelineEvents(symbol) {
    const normalized = String(symbol || '').trim().toUpperCase();
    if (!normalized || !state.eventTimelineBySymbol.has(normalized)) return null;
    return state.eventTimelineBySymbol.get(normalized) || [];
  }

  function universeSourceLabel(meta) {
    const fallbackSource = String(meta?.fallbackSource || '').trim();
    const source = String(meta?.source || '').trim();
    if (fallbackSource === 'coinglass_altcoin_universe_cache') return '候选来源：缓存研究池';
    if (fallbackSource === 'data_symbols') return '候选来源：默认列表';
    if (fallbackSource === 'client_default') return '候选来源：前端默认列表';
    if (source === 'coinglass_altcoin_universe') return '候选来源：CoinGlass 研究池';
    if (source === 'research_universe_fallback') return '候选来源：回退列表';
    return '';
  }

  function requireApi() {
    if (typeof api !== 'function') {
      throw new Error('altcoin radar requires global api() helper');
    }
    return api;
  }

  function renderOperatingModeBanner(snapshot = {}, errorText = '') {
    const root = document.getElementById('altcoin-radar');
    const workspace = root?.querySelector('.altcoin-radar-workspace');
    if (!root || !workspace) return;
    let banner = document.getElementById('altcoin-radar-operating-mode-banner');
    if (!banner) {
      banner = document.createElement('div');
      banner.id = 'altcoin-radar-operating-mode-banner';
      banner.style.cssText = 'margin:0 0 12px;padding:10px 12px;border:1px solid rgba(94,200,255,.24);background:rgba(94,200,255,.08);border-radius:8px;color:#d8e7ff;font-size:12px;';
      root.insertBefore(banner, workspace);
    }
    if (errorText) {
      banner.textContent = `Operating Mode unavailable: ${escapeHtml(errorText)}`;
      return;
    }
    const degradations = Array.isArray(snapshot.degradations) ? snapshot.degradations : [];
    const agent = snapshot.autonomous_agent || {};
    const coinglass = snapshot.coinglass || {};
    const provider = agent.provider || snapshot.ai_live_decision?.provider || '--';
    banner.innerHTML = `
      <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;">
        <strong>Operating Mode</strong>
        <span>trading=${escapeHtml(snapshot.trading_mode || '--')}</span>
        <span>agent=${escapeHtml(agent.mode || '--')}</span>
        <span>provider=${escapeHtml(provider)}</span>
        <span>allow_live=${agent.allow_live ? 'true' : 'false'}</span>
        <span>derivatives=${coinglass.live_gating_enabled ? 'live' : 'shadow'}</span>
        <span>degraded=${escapeHtml(String(degradations.length))}</span>
      </div>`;
  }

  async function refreshOperatingModeBanner(options = {}) {
    const now = Date.now();
    if (!document.getElementById('altcoin-radar')) return null;
    if (!options.force && state.operatingModeInFlight) return state.operatingModeInFlight;
    if (!options.force && state.operatingModeLoadedAt && now - state.operatingModeLoadedAt < 60000) return null;
    const apiFetch = requireApi();
    const task = apiFetch('/ai/operating-mode', { timeoutMs: 20000 })
      .then((snapshot) => {
        state.operatingModeLoadedAt = Date.now();
        renderOperatingModeBanner(snapshot || {});
        return snapshot;
      })
      .catch((error) => {
        renderOperatingModeBanner({}, error?.message || String(error || 'unknown'));
        return null;
      })
      .finally(() => {
        if (state.operatingModeInFlight === task) state.operatingModeInFlight = null;
      });
    state.operatingModeInFlight = task;
    return task;
  }

  async function loadUniverseOptions(force = false) {
    const controls = readControls();
    const cacheKey = `${controls.exchange}`;
    if (!force && state.universeLoadedFor === cacheKey) return;
    const selectEl = q('altcoin-radar-universe');
    if (!(selectEl instanceof HTMLSelectElement)) return;
    const currentSelected = normalizeSymbols(getSelectedValues('altcoin-radar-universe'));
    const apiFetch = requireApi();
    let finalSymbols = DEFAULT_UNIVERSE.slice();
    let defaultCount = Math.min(12, finalSymbols.length);
    try {
      const resp = await apiFetch(`/data/research/symbols?exchange=${encodeURIComponent(controls.exchange)}`, {
        timeoutMs: 15000,
      });
      const symbols = normalizeSymbols(resp?.symbols || []);
      if (symbols.length) finalSymbols = symbols;
      defaultCount = Math.max(1, Math.min(finalSymbols.length, toNumber(resp?.default_count, finalSymbols.length)));
      state.universeCatalog = {
        source: String(resp?.source || '').trim(),
        fallbackSource: String(resp?.fallback_source || '').trim(),
        warning: String(resp?.warning || '').trim(),
        updatedAt: String(resp?.updated_at || '').trim(),
        count: finalSymbols.length,
      };
      if (resp?.warning) {
        const sourceLabel = universeSourceLabel(state.universeCatalog) || '候选列表已回退';
        setStatus(`候选列表加载提示：${sourceLabel}，${resp.warning}`, 'warn');
      }
    } catch (error) {
      console.warn('loadUniverseOptions failed', error?.message || error);
      state.universeCatalog = {
        source: 'client_default',
        fallbackSource: 'client_default',
        warning: String(error?.message || error || '').trim(),
        updatedAt: '',
        count: finalSymbols.length,
      };
      setStatus(`候选列表加载失败，已回退到默认列表：${error.message}`, 'warn');
    }
    selectEl.innerHTML = finalSymbols
      .map((symbol) => `<option value="${escapeHtml(symbol)}">${escapeHtml(symbol)}</option>`)
      .join('');
    const preserveCurrentSelection = state.universeLoadedFor === cacheKey && currentSelected.length;
    const fallbackSelection = preserveCurrentSelection ? currentSelected : finalSymbols.slice(0, defaultCount);
    setSelectedValues('altcoin-radar-universe', fallbackSelection, finalSymbols[0] || 'BTC/USDT');
    state.universeLoadedFor = cacheKey;
    renderUniverseManager();
  }

  function renderWatchlist() {
    const listEl = q('altcoin-radar-watchlist-list');
    const noteEl = q('altcoin-radar-watchlist-note');
    const selectedSymbol = String(state.selectedSymbol || '').trim().toUpperCase();
    if (noteEl) {
      if (!selectedSymbol) {
        noteEl.textContent = '当前未选中候选';
      } else if (state.watchlist.includes(selectedSymbol)) {
        noteEl.textContent = `${selectedSymbol} 已在 Watchlist 中`;
      } else {
        noteEl.textContent = `${selectedSymbol} 当前不在 Watchlist 中`;
      }
    }
    if (!listEl) return;
    if (!Array.isArray(state.watchlist) || !state.watchlist.length) {
      listEl.innerHTML = '<div class="altcoin-radar-watchlist-empty">暂无 Watchlist 成员</div>';
      renderUniverseManager();
      return;
    }
    listEl.innerHTML = state.watchlist
      .map((symbol) => {
        const active = symbol === selectedSymbol;
        return `<button type="button" class="altcoin-radar-watchlist-chip${active ? ' is-active' : ''}" data-watchlist-symbol="${escapeHtml(symbol)}" aria-pressed="${active ? 'true' : 'false'}">${escapeHtml(symbol)}</button>`;
      })
      .join('');
    renderUniverseManager();
  }

  async function loadWatchlist() {
    const apiFetch = requireApi();
    const resp = await apiFetch('/altcoin/radar/watchlist', { timeoutMs: 10000 });
    state.watchlist = normalizeSymbols(resp?.symbols || []);
    renderWatchlist();
    return state.watchlist;
  }

  async function mutateWatchlist(action, symbol) {
    const normalized = normalizeWatchlistSymbolInput(symbol || state.selectedSymbol || '');
    if (!normalized) {
      throw new Error('请先选择一个候选币种');
    }
    const apiFetch = requireApi();
    if (action === 'add') {
      await apiFetch('/altcoin/radar/watchlist', {
        method: 'POST',
        timeoutMs: 15000,
        body: JSON.stringify({ symbol: normalized }),
      });
    } else {
      await apiFetch(`/altcoin/radar/watchlist?symbol=${encodeURIComponent(normalized)}`, {
        method: 'DELETE',
        timeoutMs: 15000,
      });
    }
    await loadWatchlist();
    return normalized;
  }

  function buildScanQuery(options, refresh = false) {
    const params = new URLSearchParams();
    params.set('exchange', options.exchange);
    params.set('timeframe', options.timeframe);
    params.set('view', options.timeframe);
    params.set('sort_by', options.sortBy);
    params.set('limit', options.universeScope === 'expanded' ? '100' : '30');
    params.set('exclude_retired', options.excludeRetired ? 'true' : 'false');
    params.set('refresh', refresh ? 'true' : 'false');
    // Phase 1
    params.set('mode', options.radarMode || 'combined');
    params.set('universe_scope', options.universeScope || 'research');
    if (options.universeSymbols.length && options.universeScope === 'research') {
      params.set('symbols', options.universeSymbols.join(','));
    }
    return params.toString();
  }

  function buildDetailQuery(symbol, refresh = false) {
    const options = readControls();
    const params = new URLSearchParams();
    params.set('exchange', options.exchange);
    params.set('timeframe', options.timeframe);
    params.set('view', options.timeframe);
    params.set('mode', options.radarMode || 'combined');
    params.set('universe_scope', options.universeScope || 'research');
    params.set('symbol', symbol);
    params.set('exclude_retired', options.excludeRetired ? 'true' : 'false');
    params.set('refresh', refresh ? 'true' : 'false');
    const currentUniverse = currentUniverseSymbols();
    const hasSymbolInUniverse = currentUniverse.includes(String(symbol || '').trim().toUpperCase());
    const universe = hasSymbolInUniverse
      ? normalizeSymbols([symbol, ...currentUniverse]).slice(0, 30)
      : normalizeSymbols([symbol]);
    if (!hasSymbolInUniverse) params.set('watchlist_focus', 'true');
    if (universe.length) params.set('symbols', universe.join(','));
    return params.toString();
  }

  function applyClientFilters(rows) {
    const controls = readControls();
    return (Array.isArray(rows) ? rows : []).filter((row) => {
      if (controls.onlyAlerted && !row?.has_alert_rule) return false;
      if (controls.filter === 'alerted' && !row?.has_alert_rule) return false;
      if (controls.filter === 'layout' && !String(row?.signal_state || '').includes('吸筹')) return false;
      if (controls.filter === 'anomaly' && !String(row?.signal_state || '').includes('异动')) return false;
      if (controls.filter === 'control') {
        const stateText = String(row?.signal_state || '').trim();
        if (!stateText.includes('高控盘') && !stateText.includes('派发')) return false;
      }
      if (controls.filter === 'fresh' && dataFreshnessLabel(row) !== 'fresh') return false;
      // Phase 1 new filters
      if (controls.filter === 'perp_ignition' && row?.signal_source !== 'perp_ignition') return false;
      if (controls.filter === 'rank_jump' && toNumber(row?.rank_jump_score, 0) < 0.3) return false;
      // Phase 2 new filters
      if (controls.filter === 'narrative_ignition' && row?.signal_source !== 'narrative_ignition') return false;
      if (controls.filter === 'watchlist' && !row?.in_watchlist) return false;
      return true;
    });
  }

  function renderSummary(summary) {
    const strip = q('altcoin-radar-summary-strip');
    if (!strip) return;
    const leader = summary?.leader || null;
    const metrics = [
      ['扫描币数', String(summary?.scanned_count ?? '--')],
      ['异动启动', String(summary?.anomaly_count ?? '--')],
      ['布局吸筹', String(summary?.accumulation_count ?? '--')],
      ['高控盘', String(summary?.control_count ?? '--')],
      ['降级行数', String(summary?.degraded_count ?? '--')],
      ['当前榜首', leader?.symbol ? `${leader.symbol}` : '--'],
    ];
    strip.innerHTML = metrics
      .map(
        ([label, value]) => `
          <div class="altcoin-radar-summary-metric">
            <span>${escapeHtml(label)}</span>
            <strong>${escapeHtml(value)}</strong>
          </div>
        `
      )
      .join('');
  }

  function renderMeta(scanPayload) {
    const metaBox = q('altcoin-radar-meta-list');
    const warningBox = q('altcoin-radar-warning-list');
    const cacheHint = q('altcoin-radar-cache-hint');
    const tableNote = q('altcoin-radar-table-note');
    if (!metaBox || !warningBox) return;
    const meta = scanPayload?.scan_meta || {};
    const cache = meta?.cache || {};
    const summary = scanPayload?.summary || {};
    const cacheStatus = cache.refreshing
      ? (cache.stale ? '旧结果回显，后台刷新中' : '缓存回显，后台刷新中')
      : (cache.hit ? '缓存命中' : '新鲜计算');
    const rows = [
      ['状态', cacheStatus],
      ['缓存', cache.ttl_sec ? `${toNumber(cache.age_sec, 0).toFixed(1)}s / ${cache.ttl_sec}s` : '--'],
      ['币池', Array.isArray(meta.symbols_used) && meta.symbols_used.length ? `${meta.symbols_used.length} 个币种` : '--'],
      ['更新时间', meta.generated_at ? fmtDateTime(meta.generated_at) : '--'],
    ];
    metaBox.innerHTML = rows
      .map(
        ([label, value]) => `<div class="list-item"><span>${escapeHtml(label)}</span><span>${escapeHtml(value)}</span></div>`
      )
      .join('');
    const warnings = Array.isArray(scanPayload?.warnings) && scanPayload.warnings.length
      ? scanPayload.warnings
      : ['暂无警告'];
    warningBox.innerHTML = warnings
      .slice(0, 4)
      .map((warning) => `<div class="altcoin-radar-warning">${escapeHtml(String(warning || ''))}</div>`)
      .join('');
    if (cacheHint) {
      cacheHint.textContent = cache.cache_key
        ? `cache_key: ${cache.cache_key} · 实际使用币种 ${Array.isArray(summary.symbols_used) ? summary.symbols_used.length : 0} 个`
        : '默认缓存：15m 30s / 1h 60s / 4h 300s；强制刷新会绕过缓存重新计算。';
    }
    if (tableNote) {
      const filtered = Array.isArray(state.filteredRows) ? state.filteredRows.length : 0;
      const total = Array.isArray(scanPayload?.rows) ? scanPayload.rows.length : 0;
      tableNote.textContent = `当前展示 ${filtered} / ${total} 条候选`;
    }
  }

  function renderTagRow(tags) {
    const list = Array.isArray(tags) ? tags : [];
    if (!list.length) {
      return '<span class="altcoin-radar-tag" data-tone="muted">待跟踪</span>';
    }
    return list
      .map((tag) => `<span class="altcoin-radar-tag" data-tone="${escapeHtml(tagTone(tag))}">${escapeHtml(tag)}</span>`)
      .join('');
  }

  function freshnessChip(row) {
    const label = dataFreshnessLabel(row);
    const marketFresh = toPercent(row?.data_quality?.market_data_freshness, 0);
    const snapFresh = toPercent(row?.data_quality?.snapshot_freshness, 0);
    return `
      <span class="altcoin-radar-tag" data-tone="${escapeHtml(tagTone(label))}">${escapeHtml(label)}</span>
      <span class="altcoin-radar-freshness">${escapeHtml(marketFresh)} / ${escapeHtml(snapFresh)}</span>
    `;
  }

  function formatDerivativesStatus(detailPayload, selected) {
    const context = detailPayload?.derivatives_context || selected?.derivatives_context || {};
    const label = String(context.freshness_label || selected?.freshness?.derivatives_label || '').trim() || 'missing';
    const parts = [label];
    if (context.available !== false && context.age_sec != null) parts.push(fmtAge(context.age_sec));
    if (context.capture_status) parts.push(String(context.capture_status));
    return parts.join(' · ');
  }

  function renderRanking(rows) {
    const tbody = q('altcoin-radar-ranking-body');
    if (!tbody) return;
    const filteredRows = applyClientFilters(rows);
    state.filteredRows = filteredRows;
    if (!filteredRows.length) {
      tbody.innerHTML = '<tr><td colspan="12" class="altcoin-radar-empty">当前过滤条件下没有候选，请切换过滤或刷新币池。</td></tr>';
      return;
    }
    tbody.innerHTML = filteredRows
      .map((row) => {
        const symbol = String(row?.symbol || '').trim();
        const selected = symbol && symbol === state.selectedSymbol;
        const defaultPreset = pickDefaultPreset(row);
        const activeKinds = alertKindsForRow(row);
        const hasDefaultAlert = activeKinds.has(defaultPreset);
        // Phase 1: ignition score chip
        const ignScore = toNumber(row?.ignition_score, 0);
        const ignClass = ignScore >= 0.6 ? 'altcoin-radar-score-badge score-high' : 'altcoin-radar-score-badge';
        // Phase 1: rank jump score chip
        const rjScore = toNumber(row?.rank_jump_score, 0);
        const rjClass = rjScore >= 0.3 ? 'altcoin-radar-score-badge score-high' : 'altcoin-radar-score-badge';
        // Phase 1: signal source chip
        const src = String(row?.signal_source || '');
        const srcLabel = signalSourceLabel(src);
        const srcHtml = srcLabel
          ? `<span class="altcoin-signal-src ${signalSourceClass(src)}">${escapeHtml(srcLabel)}</span>`
          : '<span class="altcoin-signal-src altcoin-src-empty">—</span>';
        return `
          <tr class="${selected ? 'is-selected' : ''}" data-symbol="${escapeHtml(symbol)}">
            <td><span class="altcoin-radar-rank-chip">${escapeHtml(String(row?.rank ?? '--'))}</span></td>
            <td>
              <div class="altcoin-radar-symbol-cell">
                <div class="altcoin-radar-symbol-main">${escapeHtml(symbol || '--')}</div>
                <div class="altcoin-radar-symbol-sub">${escapeHtml(String(row?.signal_state || '待跟踪'))}</div>
              </div>
            </td>
            <td><span class="altcoin-radar-score-badge">${escapeHtml(shortPercent(row?.layout_score))}</span></td>
            <td><span class="altcoin-radar-score-badge">${escapeHtml(shortPercent(row?.alert_score))}</span></td>
            <td><span class="altcoin-radar-score-badge">${escapeHtml(shortPercent(row?.accumulation_score))}</span></td>
            <td><span class="altcoin-radar-score-badge">${escapeHtml(shortPercent(row?.control_score))}</span></td>
            <td><span class="${ignClass}">${escapeHtml(shortPercent(row?.ignition_score))}</span></td>
            <td><span class="${rjClass}">${escapeHtml(shortPercent(row?.rank_jump_score))}</span></td>
            <td>${srcHtml}</td>
            <td>${freshnessChip(row)}</td>
            <td><div class="altcoin-radar-tag-row">${renderTagRow(row?.tags)}</div></td>
            <td>
              <div class="altcoin-radar-row-actions">
                <button type="button" class="btn btn-primary btn-sm" data-row-action="inspect" data-symbol="${escapeHtml(symbol)}">查看</button>
                <button type="button" class="btn btn-sm" data-row-action="research-proposal" data-symbol="${escapeHtml(symbol)}">生成研究提案</button>
                <button type="button" class="btn btn-sm" data-row-action="alert" data-symbol="${escapeHtml(symbol)}" data-preset-kind="${escapeHtml(defaultPreset)}">${hasDefaultAlert ? '回收' : '预警'}</button>
              </div>
            </td>
          </tr>
        `;
      })
      .join('');
    syncSelectedRankingRow();
  }

  function findRow(symbol) {
    const normalized = String(symbol || '').trim().toUpperCase();
    return (Array.isArray(state.scan?.rows) ? state.scan.rows : []).find(
      (row) => String(row?.symbol || '').trim().toUpperCase() === normalized
    ) || null;
  }

  function renderSparkline(values) {
    const host = q('altcoin-radar-sparkline');
    if (!host) return;
    const points = (Array.isArray(values) ? values : [])
      .map((value) => toNumber(value, NaN))
      .filter((value) => Number.isFinite(value));
    if (!points.length) {
      host.textContent = '暂无价格路径';
      return;
    }
    const min = Math.min(...points);
    const max = Math.max(...points);
    const width = 420;
    const height = 118;
    const range = Math.max(max - min, 1e-6);
    const path = points
      .map((value, index) => {
        const x = (index / Math.max(points.length - 1, 1)) * width;
        const y = height - ((value - min) / range) * height;
        return `${index === 0 ? 'M' : 'L'} ${x.toFixed(2)} ${y.toFixed(2)}`;
      })
      .join(' ');
    host.innerHTML = `
      <svg viewBox="0 0 ${width} ${height}" width="100%" height="${height}" role="img" aria-label="sparkline">
        <defs>
          <linearGradient id="altcoinRadarSpark" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stop-color="#f59e0b"></stop>
            <stop offset="100%" stop-color="#60a5fa"></stop>
          </linearGradient>
        </defs>
        <path d="${path}" fill="none" stroke="url(#altcoinRadarSpark)" stroke-width="3" stroke-linecap="round"></path>
      </svg>
    `;
  }

  function renderComponentList(containerId, components) {
    const box = q(containerId);
    if (!box) return;
    const list = Array.isArray(components) ? components : [];
    if (!list.length) {
      box.innerHTML = '<div class="altcoin-radar-reason">暂无拆解数据</div>';
      return;
    }
    box.innerHTML = list
      .map((item) => {
        const label = String(item?.label || '--');
        const pctile = item?.pctile == null ? '--' : toPercent(item.pctile, 0);
        const weight = item?.weight == null ? '--' : toPercent(item.weight, 0);
        return `
          <div class="altcoin-radar-component">
            <span>${escapeHtml(label)}</span>
            <small>Pctl ${escapeHtml(String(pctile))} · 权重 ${escapeHtml(String(weight))}</small>
          </div>
        `;
      })
      .join('');
  }

  function renderReasonList(containerId, items, emptyText) {
    const box = q(containerId);
    if (!box) return;
    const list = Array.isArray(items) ? items : [];
    if (!list.length) {
      box.innerHTML = `<div class="altcoin-radar-reason">${escapeHtml(emptyText)}</div>`;
      return;
    }
    box.innerHTML = list.map((item) => `<div class="altcoin-radar-reason">${escapeHtml(String(item || ''))}</div>`).join('');
  }

  function renderListItems(containerId, items) {
    const box = q(containerId);
    if (!box) return;
    box.innerHTML = items
      .map(
        ([label, value]) => `<div class="list-item"><span>${escapeHtml(label)}</span><span>${escapeHtml(String(value))}</span></div>`
      )
      .join('');
  }

  function renderThemePeers(peers) {
    const box = q('altcoin-radar-theme-peers');
    if (!box) return;
    const rows = Array.isArray(peers) ? peers : [];
    if (!rows.length) {
      box.innerHTML = '<div class="altcoin-radar-reason">暂无同板块候选</div>';
      return;
    }
    box.innerHTML = rows
      .map((row) => `
        <button type="button" class="altcoin-radar-related-btn" data-related-symbol="${escapeHtml(row.symbol || '')}">
          <strong>${escapeHtml(row.symbol || '--')}</strong>
          <span>${escapeHtml(String(row.sector || '—'))} · 叙事 ${escapeHtml(shortPercent(row.narrative_heat_score))}</span>
        </button>
      `)
      .join('');
  }

  function renderTimelineEvents(events) {
    const timelineEl = q('altcoin-radar-event-timeline');
    const badgeEl = q('altcoin-radar-events-badge');
    if (!timelineEl) return;
    const rows = Array.isArray(events) ? events : [];
    if (badgeEl) {
      badgeEl.style.display = rows.length ? 'inline-block' : 'none';
      badgeEl.textContent = rows.length ? String(rows.length) : '';
    }
    if (!rows.length) {
      timelineEl.innerHTML = '<span class="text-muted" style="font-size:12px">暂无近期事件</span>';
      return;
    }
    const eventTypeLabel = (t) => ({
      altcoin_ignition_cross_up: '点火',
      altcoin_rank_jump_top_n: '跃升',
      altcoin_perp_burst: 'Perp 爆发',
      altcoin_crowding_risk_spike: '拥挤',
      altcoin_narrative_heat_spike: '叙事热度',
    }[t] || t);
    timelineEl.innerHTML = rows.map((e) => {
      const ts = e.ts_iso ? new Date(e.ts_iso).toLocaleTimeString('zh-CN', { hour12: false }) : '--';
      const detail = e.ignition_score != null
        ? `点火=${Number(e.ignition_score).toFixed(2)}`
        : e.rank_jump != null
        ? `跃升 ${e.prev_rank}→${e.current_rank}`
        : e.crowding_late_score != null
        ? `拥挤=${Number(e.crowding_late_score).toFixed(2)}`
        : e.narrative_heat_score != null
        ? `叙事=${Number(e.narrative_heat_score).toFixed(2)}`
        : '';
      return `<div class="altcoin-event-row">
        <span class="altcoin-event-time">${escapeHtml(ts)}</span>
        <span class="altcoin-event-type">${escapeHtml(eventTypeLabel(e.event_type))}</span>
        <span class="altcoin-event-detail">${escapeHtml(detail)}</span>
      </div>`;
    }).join('');
  }

  function renderInspector(detailPayload, fallbackError = '') {
    const empty = q('altcoin-radar-inspector-empty');
    const shell = q('altcoin-radar-inspector-shell');
    const selected = detailPayload?.selected_row || resolveSelectedRow(state.selectedSymbol);
    if (!selected) {
      if (empty) empty.textContent = fallbackError || '暂无已选候选';
      if (shell) shell.classList.add('is-hidden');
      return;
    }
    if (empty) empty.textContent = '';
    if (shell) shell.classList.remove('is-hidden');
    q('altcoin-radar-selected-symbol').textContent = selected.symbol || '--';
    const subtitleEl = q('altcoin-radar-selected-subtitle');
    if (subtitleEl) {
      const statusNote = String(fallbackError || '').trim();
      const placeholderNote = String(selected?.placeholder_reason || '').trim();
      const isLoading = statusNote === '__loading__';
      subtitleEl.textContent = isLoading
        ? (placeholderNote || `当前聚焦 ${selected.symbol || '--'}，正在加载详情。`)
        : statusNote
          ? (placeholderNote ? `${placeholderNote} 详情接口返回：${statusNote}` : `详情加载失败，已回退到扫描快照：${statusNote}`)
          : (placeholderNote || `当前聚焦 ${selected.symbol || '--'}。先看它为什么上榜，再决定建预警还是带入研究工坊。`);
    }
    q('altcoin-radar-selected-tags').innerHTML = renderTagRow(selected.tags);
    q('altcoin-radar-selected-scores').innerHTML = [
      ['Derivatives Heat', selected.derivatives_heat_score],
      ['Squeeze', selected.squeeze_score],
      ['Crowding Risk', selected.crowding_risk_score],
      ['Flow Confirm', selected.flow_confirmation_score],
      ['布局分', selected.layout_score],
      ['异动分', selected.alert_score],
      ['吸筹分', selected.accumulation_score],
      ['控盘分', selected.control_score],
      ['风险惩罚', selected.risk_penalty],
    ]
      .map(
        ([label, value]) => `
          <div class="altcoin-radar-score-pill">
            <span>${escapeHtml(label)}</span>
            <strong>${escapeHtml(shortPercent(value))}</strong>
          </div>
        `
      )
      .join('');

    renderSparkline(detailPayload?.sparkline || selected.sparkline || []);
    renderComponentList('altcoin-radar-proxy-components', detailPayload?.proxy_breakdown?.components || []);
    renderComponentList('altcoin-radar-chain-components', detailPayload?.chain_breakdown?.components || []);
    renderReasonList(
      'altcoin-radar-proxy-reasons',
      detailPayload?.proxy_breakdown?.reasons || selected.reasons_proxy || [],
      '代理行为侧暂时没有明确上榜理由。'
    );
    renderReasonList(
      'altcoin-radar-chain-reasons',
      detailPayload?.chain_breakdown?.reasons || selected.reasons_chain || [],
      '链上 / 外生侧暂无额外确认。'
    );
    renderReasonList(
      'altcoin-radar-invalidate-list',
      detailPayload?.invalidate_conditions || [],
      '当前没有额外失效条件。'
    );
    const actionPlan = detailPayload?.action_plan || {};
    const actionItems = [];
    if (actionPlan.stance) actionItems.push(`结论：${String(actionPlan.stance)}`);
    if (actionPlan.summary) actionItems.push(String(actionPlan.summary));
    if (Array.isArray(actionPlan.actions)) actionItems.push(...actionPlan.actions.map((item) => String(item || '').trim()).filter(Boolean));
    renderReasonList(
      'altcoin-radar-action-list',
      actionItems,
      '当前暂无观察建议。'
    );
    const actionTags = q('altcoin-radar-action-tags');
    if (actionTags) {
      const tone = String(actionPlan.tone || 'muted').trim() || 'muted';
      const tags = [actionPlan.primary_action, actionPlan.secondary_action]
        .map((item) => String(item || '').trim())
        .filter(Boolean);
      actionTags.innerHTML = tags.length
        ? tags.map((item) => `<span class="altcoin-radar-tag" data-tone="${escapeHtml(tone)}">${escapeHtml(item)}</span>`).join('')
        : '<span class="altcoin-radar-tag" data-tone="muted">暂无动作标签</span>';
    }

    const dataQuality = selected?.data_quality || {};
    const derivativesContext = detailPayload?.derivatives_context || selected?.derivatives_context || {};
    const derivativesError = String(derivativesContext.source_error || '').trim();
    renderListItems('altcoin-radar-data-quality', [
      ['市场新鲜度', toPercent(dataQuality.market_data_freshness, 0)],
      ['快照新鲜度', toPercent(dataQuality.snapshot_freshness, 0)],
      ['Derivatives', formatDerivativesStatus(detailPayload, selected)],
      ['Derivatives 来源', String(derivativesContext.source_name || '--')],
      ['Derivatives 快照', derivativesContext.timestamp ? fmtDateTime(derivativesContext.timestamp) : '--'],
      ['链上质量', toPercent(dataQuality.chain_quality, 0)],
      ['降级原因', Array.isArray(dataQuality.degraded_reason) && dataQuality.degraded_reason.length ? dataQuality.degraded_reason.join(', ') : '无'],
      ['Derivatives 错误', derivativesError || '--'],
    ]);

    const metrics = selected?.metrics || {};
    renderListItems('altcoin-radar-key-metrics', [
      ['OI 1h', shortPercent(metrics.oi_change_1h)],
      ['Funding', shortPercent(metrics.funding_rate)],
      ['Basis', shortPercent(metrics.basis_pct)],
      ['Long/Short', shortNumber(metrics.long_short_ratio)],
      ['Taker Imbalance', shortPercent(metrics.taker_buy_sell_imbalance)],
      ['Crowding', shortPercent(metrics.crowding_score)],
      ['Distribution', shortPercent(metrics.distribution_score)],
      ['Depth Thinness', shortPercent(metrics.depth_thinness_score)],
      ['1 bar', toPercent(metrics.return_1_bar, 1)],
      ['3 bar', toPercent(metrics.return_3_bar, 1)],
      ['6 bar', toPercent(metrics.return_6_bar, 1)],
      ['量能爆发', shortPercent(metrics.volume_burst_ratio)],
      ['ATR 扩张', shortPercent(metrics.range_expansion_ratio)],
      ['价差 bps', shortPercent(metrics.spread_bps)],
      ['订单流失衡', shortPercent(metrics.order_flow_imbalance)],
      ['巨鲸计数', shortPercent(metrics.whale_count)],
    ]);

    const ignitionSummary = q('altcoin-radar-ignition-summary');
    if (ignitionSummary) {
      const summary = String(detailPayload?.ignition_path?.summary || '暂无点火路径说明').trim();
      ignitionSummary.innerHTML = `<div class="altcoin-radar-reason">${escapeHtml(summary)}</div>`;
    }
    const ignitionDrivers = q('altcoin-radar-ignition-drivers');
    if (ignitionDrivers) {
      const drivers = Array.isArray(detailPayload?.ignition_path?.drivers) ? detailPayload.ignition_path.drivers : [];
      ignitionDrivers.innerHTML = drivers.length
        ? drivers.map((item) => `<span class="altcoin-radar-tag" data-tone="ignition">${escapeHtml(item)}</span>`).join('')
        : '<span class="altcoin-radar-tag" data-tone="muted">暂无主驱动</span>';
    }

    const themeSummary = q('altcoin-radar-theme-summary');
    if (themeSummary) {
      const linkage = detailPayload?.narrative_linkage || {};
      const summaryParts = [];
      if (linkage.summary) summaryParts.push(String(linkage.summary));
      if (linkage.sector) summaryParts.push(`板块：${linkage.sector}`);
      summaryParts.push(linkage.in_watchlist ? '当前命中 Watchlist' : '当前未命中 Watchlist');
      themeSummary.innerHTML = summaryParts
        .map((item) => `<div class="altcoin-radar-reason">${escapeHtml(item)}</div>`)
        .join('');
    }
    renderThemePeers(detailPayload?.narrative_linkage?.board_peers || []);

    const related = Array.isArray(detailPayload?.related_candidates) ? detailPayload.related_candidates : [];
    const relatedBox = q('altcoin-radar-related-list');
    if (relatedBox) {
      relatedBox.innerHTML = related.length
        ? related
            .map(
              (row) => `
                <button type="button" class="altcoin-radar-related-btn" data-related-symbol="${escapeHtml(row.symbol || '')}">
                  <strong>${escapeHtml(row.symbol || '--')}</strong>
                  <span>${escapeHtml(String(row.signal_state || '待跟踪'))} · 布局 ${escapeHtml(shortPercent(row.layout_score))}</span>
                </button>
              `
            )
            .join('')
        : '<div class="altcoin-radar-reason">暂无相关候选。</div>';
    }

    // Phase 1: render perp scores panel
    if (selected) {
      const setVal = (id, val) => { const el = q(id); if (el) el.textContent = val; };
      setVal('altcoin-perp-ignition', shortPercent(selected.ignition_score));
      setVal('altcoin-perp-continuation', shortPercent(selected.continuation_score));
      setVal('altcoin-perp-crowding', shortPercent(selected.crowding_late_score));
      setVal('altcoin-perp-rank-jump', shortPercent(selected.rank_jump_score));
      const srcEl = q('altcoin-perp-source');
      if (srcEl) {
        const src = String(selected.signal_source || '');
        srcEl.textContent = signalSourceLabel(src) || '—';
        srcEl.className = `altcoin-signal-src ${signalSourceClass(src)}`;
      }
    }

    // Phase 2: render narrative scores panel
    if (selected) {
      const setVal = (id, val) => { const el = q(id); if (el) el.textContent = val; };
      setVal('altcoin-narrative-heat', shortPercent(selected.narrative_heat_score));
      setVal('altcoin-meme-rotation', shortPercent(selected.meme_rotation_score));
      setVal('altcoin-narrative-sector', selected.sector || '—');
      setVal('altcoin-narrative-watchlist', selected.in_watchlist ? '✓ 是' : '否');
      const narrativeSrcEl = q('altcoin-narrative-source');
      if (narrativeSrcEl) {
        const src = String(selected.signal_source || '');
        const isNarrative = src === 'narrative_ignition' || src === 'narrative_confirmation';
        narrativeSrcEl.textContent = isNarrative ? signalSourceLabel(src) : '—';
        narrativeSrcEl.className = isNarrative ? `altcoin-signal-src ${signalSourceClass(src)}` : 'altcoin-signal-src altcoin-src-empty';
      }
    }

    const shouldBootstrapTimeline = fallbackError !== '__loading__';
    const detailTimeline = Array.isArray(detailPayload?.event_timeline) ? detailPayload.event_timeline : [];
    const recentTimeline = Array.isArray(selected?.recent_events) ? selected.recent_events : [];
    if (detailTimeline.length) {
      cacheTimelineEvents(selected.symbol, detailTimeline);
      renderTimelineEvents(detailTimeline);
    } else {
      const cachedTimeline = getCachedTimelineEvents(selected.symbol);
      renderTimelineEvents(cachedTimeline || recentTimeline);
      if (shouldBootstrapTimeline && selected?.symbol && !cachedTimeline) {
        loadSymbolEvents(String(selected.symbol).trim().toUpperCase());
      }
    }

    renderWatchlist();
    updateInspectorButtonState(selected);
  }

  // Phase 1: load events for a symbol and render timeline
  async function loadSymbolEvents(symbol) {
    const normalized = String(symbol || '').trim().toUpperCase();
    const timelineEl = q('altcoin-radar-event-timeline');
    const badgeEl = q('altcoin-radar-events-badge');
    if (!timelineEl || !normalized) return [];
    const cachedTimeline = getCachedTimelineEvents(normalized);
    if (cachedTimeline) {
      renderTimelineEvents(cachedTimeline);
      return cachedTimeline;
    }
    const requestSeq = ++state.eventSeq;
    timelineEl.innerHTML = '<span class="text-muted" style="font-size:12px">加载中...</span>';
    try {
      const apiFetch = requireApi();
      const resp = await apiFetch(`/altcoin/radar/events?symbol=${encodeURIComponent(normalized)}&limit=10&max_age_sec=3600`, {
        timeoutMs: 8000,
      });
      const events = Array.isArray(resp?.events) ? resp.events : [];
      cacheTimelineEvents(normalized, events);
      if (requestSeq !== state.eventSeq || normalized !== String(state.selectedSymbol || '').trim().toUpperCase()) {
        return events;
      }
      renderTimelineEvents(events);
      return events;
    } catch (_) {
      if (requestSeq === state.eventSeq && normalized === String(state.selectedSymbol || '').trim().toUpperCase()) {
        timelineEl.innerHTML = '<span class="text-muted" style="font-size:12px">事件加载失败</span>';
        if (badgeEl) badgeEl.style.display = 'none';
      }
      return [];
    }
  }

  function updateInspectorButtonState(row) {
    const symbol = String(row?.symbol || state.selectedSymbol || '').trim();
    const disabled = !symbol;
    const normalized = symbol.toUpperCase();
    const activeKinds = alertKindsForRow(row);
    const researchBtn = q('btn-altcoin-radar-open-research');
    if (researchBtn) {
      researchBtn.disabled = disabled;
      if (!disabled) researchBtn.dataset.symbol = symbol;
    }
    const addWatchlistBtn = q('btn-altcoin-radar-watchlist-add');
    const removeWatchlistBtn = q('btn-altcoin-radar-watchlist-remove');
    const inWatchlist = !disabled && state.watchlist.includes(normalized);
    if (addWatchlistBtn) {
      addWatchlistBtn.disabled = disabled || inWatchlist;
      addWatchlistBtn.dataset.symbol = symbol;
    }
    if (removeWatchlistBtn) {
      removeWatchlistBtn.disabled = disabled || !inWatchlist;
      removeWatchlistBtn.dataset.symbol = symbol;
    }
    eachInspectorAlertButton((button, kind) => {
      const label = ALERT_ACTION_LABEL_BY_KIND[kind] || PRESET_BY_KIND[kind] || '预警';
      button.dataset.defaultLabel = `建${label}`;
      button.dataset.kind = kind;
      button.disabled = disabled;
      button.textContent = activeKinds.has(kind) ? `回收${label}` : `建${label}`;
      button.title = activeKinds.has(kind) ? `当前候选已存在${label}` : '';
      button.dataset.symbol = disabled ? '' : symbol;
    });
    const recycleBtn = q('btn-altcoin-radar-alert-recycle');
    if (recycleBtn) {
      recycleBtn.disabled = disabled || !activeKinds.size;
      recycleBtn.dataset.symbol = disabled ? '' : symbol;
      recycleBtn.title = activeKinds.size ? `当前候选共 ${activeKinds.size} 条预警，可一键回收` : '当前候选暂无可回收预警';
    }
  }

  function renderScan(scanPayload) {
    state.scan = scanPayload || null;
    renderSummary(scanPayload?.summary || {});
    renderRanking(scanPayload?.rows || []);
    renderMeta(scanPayload || {});
  }

  function syncUniverseSelection(symbols) {
    const universe = normalizeSymbols(symbols).slice(0, 30);
    if (!universe.length) return;
    const selectEl = q('altcoin-radar-universe');
    if (!(selectEl instanceof HTMLSelectElement)) return;
    const currentOptions = normalizeSymbols(Array.from(selectEl.options || []).map((opt) => opt.value));
    if (!currentOptions.length) return;
    const valid = universe.filter((symbol) => currentOptions.includes(symbol));
    if (valid.length) {
      setSelectedValues('altcoin-radar-universe', valid, valid[0]);
    }
    renderUniverseManager();
  }

  async function selectSymbol(symbol, refresh = false) {
    const normalized = String(symbol || '').trim().toUpperCase();
    if (!normalized) return;
    state.selectedSymbol = normalized;
    renderWatchlist();
    syncSelectedRankingRow();
    const selectedRow = resolveSelectedRow(normalized);
    const inCurrentScan = Boolean(findRow(normalized));
    updateInspectorButtonState(selectedRow);
    const seq = ++state.detailSeq;
    renderInspector({ selected_row: selectedRow }, '__loading__');
    try {
      const apiFetch = requireApi();
      const detail = await apiFetch(`/altcoin/radar/detail?${buildDetailQuery(normalized, refresh)}`, {
        timeoutMs: inCurrentScan ? 30000 : 12000,
      });
      if (seq !== state.detailSeq) return;
      state.detail = detail;
      renderInspector(detail);
      setOutput(JSON.stringify({ selected_symbol: normalized, detail }, null, 2));
    } catch (error) {
      if (seq !== state.detailSeq) return;
      state.detail = null;
      renderInspector({ selected_row: selectedRow }, error.message || '详情加载失败');
      setOutput(`详情加载失败: ${error.message}`);
    }
  }

  async function scanRadar(refresh = false, options = {}) {
    const controls = readControls();
    const seq = ++state.scanSeq;
    const previousScan = state.scan;
    clearBackgroundRefreshPoll();
    if (!options.preserveStatus) {
      setStatus(refresh ? '正在强制刷新雷达，保留上次榜单...' : '正在加载山寨雷达榜单...', 'warn');
    }
    setScanBusy(true);
    try {
      const apiFetch = requireApi();
      const response = await apiFetch(`/altcoin/radar/scan?${buildScanQuery(controls, refresh)}`, {
        timeoutMs: 60000,
      });
      if (seq !== state.scanSeq) return;
      renderScan(response);
      syncUniverseSelection(response?.scan_meta?.symbols_used || response?.summary?.symbols_used || []);
      const cache = response?.scan_meta?.cache || {};
      const leaderSymbol = response?.rows?.[0]?.symbol || '';
      const preferred = shouldKeepSelectedSymbol(response?.rows || [], state.selectedSymbol)
        ? String(state.selectedSymbol || '').trim().toUpperCase()
        : leaderSymbol;
      const statusText = cache.refreshing
        ? `山寨雷达后台刷新中，先展示上一版结果：${controls.exchange} / ${controls.timeframe} · ${response?.summary?.scanned_count || 0} 币`
        : cache.hit
          ? `已加载缓存结果：${controls.exchange} / ${controls.timeframe} · ${response?.summary?.scanned_count || 0} 币`
          : `已完成实时扫描：${controls.exchange} / ${controls.timeframe} · ${response?.summary?.scanned_count || 0} 币`;
      setStatus(
        statusText,
        'ok'
      );
      setOutput(JSON.stringify(response, null, 2));
      scheduleBackgroundRefreshPoll(cache, controls);
      if (preferred) {
        await selectSymbol(preferred, false);
      } else {
        renderInspector({}, '当前没有可检视候选');
      }
    } catch (error) {
      if (seq !== state.scanSeq) return;
      setStatus(`刷新失败，已保留上次结果：${error.message}`, 'danger');
      setOutput(`山寨雷达扫描失败: ${error.message}`);
      if (previousScan) {
        renderScan(previousScan);
        if (state.selectedSymbol) renderInspector({ selected_row: findRow(state.selectedSymbol) }, error.message);
      } else {
        renderRanking([]);
        renderInspector({}, error.message);
      }
      throw error;
    } finally {
      if (seq === state.scanSeq) setScanBusy(false);
    }
  }

  async function createPresetAlert(kind, symbol) {
    const selectedSymbol = String(symbol || state.selectedSymbol || '').trim().toUpperCase();
    if (!selectedSymbol) {
      throw new Error('请先选择一个候选币种');
    }
    const preset = PRESET_BY_KIND[kind];
    if (!preset) {
      throw new Error(`未知预警预设: ${kind}`);
    }
    const controls = readControls();
    const apiFetch = requireApi();
    const payload = {
      preset,
      exchange: controls.exchange,
      timeframe: controls.timeframe,
      symbol: selectedSymbol,
      universe_symbols: currentUniverseSymbols(),
      channels: ['feishu'],
      mode: controls.radarMode || 'combined',
      view: controls.timeframe,
      universe_scope: controls.universeScope || 'research',
    };
    const resp = await apiFetch('/altcoin/alerts/preset', {
      method: 'POST',
      timeoutMs: 30000,
      body: JSON.stringify(payload),
    });
    const rule = resp?.rule || {};
    const params = rule?.params && typeof rule.params === 'object' ? rule.params : {};
    upsertAlertRuleForSymbol(selectedSymbol, {
      id: String(rule?.id || '').trim(),
      name: String(rule?.name || '').trim(),
      rule_type: String(rule?.rule_type || '').trim(),
      score_key: String(params?.score_key || '').trim(),
      symbol: String(params?.symbol || selectedSymbol).trim().toUpperCase(),
      preset: String(resp?.rule_meta?.preset || preset).trim(),
      kind: String(resp?.rule_meta?.kind || kind).trim(),
      exchange: String(params?.exchange || controls.exchange).trim().toLowerCase(),
      timeframe: String(params?.timeframe || controls.timeframe).trim().toLowerCase(),
      mode: String(params?.mode || controls.radarMode || 'combined').trim(),
      view: String(params?.view || controls.timeframe).trim(),
      universe_scope: String(params?.universe_scope || controls.universeScope || 'research').trim(),
      config_key: String(params?.config_key || resp?.rule_meta?.config_key || '').trim(),
      threshold: Number(params?.threshold || 0),
      enabled: rule?.enabled !== false,
    });
    rerenderAlertState(selectedSymbol);
    return resp;
  }

  async function recyclePresetAlert(kind, symbol) {
    const selectedSymbol = String(symbol || state.selectedSymbol || '').trim().toUpperCase();
    if (!selectedSymbol) {
      throw new Error('请先选择一个候选币种');
    }
    const preset = PRESET_BY_KIND[kind];
    if (!preset) {
      throw new Error(`未知预警预设: ${kind}`);
    }
    const controls = readControls();
    const universeSymbols = currentUniverseSymbols();
    const params = new URLSearchParams();
    params.set('exchange', controls.exchange);
    params.set('timeframe', controls.timeframe);
    params.set('symbol', selectedSymbol);
    params.set('preset', preset);
    params.set('mode', controls.radarMode || 'combined');
    params.set('view', controls.timeframe);
    params.set('universe_scope', controls.universeScope || 'research');
    if (universeSymbols.length) params.set('symbols', universeSymbols.join(','));
    const apiFetch = requireApi();
    const resp = await apiFetch(`/altcoin/alerts/preset?${params.toString()}`, {
      method: 'DELETE',
      timeoutMs: 30000,
    });
    removeAlertRulesForSymbol(selectedSymbol, resp?.deleted_rules, kind);
    rerenderAlertState(selectedSymbol);
    return resp;
  }

  async function recycleAllAlerts(symbol) {
    const selectedSymbol = String(symbol || state.selectedSymbol || '').trim().toUpperCase();
    if (!selectedSymbol) {
      throw new Error('请先选择一个候选币种');
    }
    const controls = readControls();
    const universeSymbols = currentUniverseSymbols();
    const params = new URLSearchParams();
    params.set('exchange', controls.exchange);
    params.set('timeframe', controls.timeframe);
    params.set('symbol', selectedSymbol);
    params.set('mode', controls.radarMode || 'combined');
    params.set('view', controls.timeframe);
    params.set('universe_scope', controls.universeScope || 'research');
    if (universeSymbols.length) params.set('symbols', universeSymbols.join(','));
    const apiFetch = requireApi();
    const resp = await apiFetch(`/altcoin/alerts/preset?${params.toString()}`, {
      method: 'DELETE',
      timeoutMs: 30000,
    });
    removeAlertRulesForSymbol(selectedSymbol, resp?.deleted_rules);
    rerenderAlertState(selectedSymbol);
    return resp;
  }

  async function openResearchWorkbench(symbol) {
    const selectedSymbol = String(symbol || state.selectedSymbol || '').trim().toUpperCase();
    if (!selectedSymbol) {
      throw new Error('请先选择一个候选币种');
    }
    const controls = readControls();
    const universeSymbols = currentUniverseSymbols();
    if (typeof activateTab === 'function') {
      activateTab('research');
    }
    if (q('research-exchange')) q('research-exchange').value = controls.exchange;
    if (typeof loadResearchSymbolOptions === 'function') {
      await loadResearchSymbolOptions(controls.exchange, { quiet: true });
    }
    if (q('research-timeframe')) q('research-timeframe').value = controls.timeframe;
    if (q('research-exclude-retired')) q('research-exclude-retired').checked = controls.excludeRetired;
    if (typeof ensureSelectOption === 'function') {
      ensureSelectOption('research-symbol', selectedSymbol);
      universeSymbols.forEach((item) => {
        ensureSelectOption('research-symbol', item);
        ensureSelectOption('research-symbols', item);
      });
    }
    if (typeof setSelectValues === 'function') {
      setSelectValues('research-symbol', [selectedSymbol], selectedSymbol);
      setSelectValues('research-symbols', universeSymbols, selectedSymbol);
    } else {
      const symbolEl = q('research-symbol');
      if (symbolEl) symbolEl.value = selectedSymbol;
    }
    if (typeof renderResearchStatusCards === 'function') {
      renderResearchStatusCards();
    }
    const researchOutput = typeof getResearchOutputEl === 'function' ? getResearchOutputEl() : null;
    if (researchOutput) {
      researchOutput.textContent = `已从山寨雷达带入研究工坊：${selectedSymbol}\nexchange=${controls.exchange}\ntimeframe=${controls.timeframe}\nmode=${controls.radarMode}\nuniverse_scope=${controls.universeScope}\nuniverse=${universeSymbols.join(', ')}`;
    }
    setOutput(`已将 ${selectedSymbol} 带入研究工坊。下一步建议：在研究工坊先运行“研究总览”，再决定是否继续多币种 / 链上验证。`);
  }

  function focusWatchlistSymbol(symbol) {
    const normalized = normalizeWatchlistSymbolInput(symbol);
    if (!normalized) {
      return Promise.reject(new Error('请输入币种，例如 ORDI/USDT 或 ORDI'));
    }
    const controls = readControls();
    if (controls.universeScope !== 'watchlist') {
      setStatus(`已聚焦 Watchlist 候选 ${normalized}；当前榜单仍按 ${universeScopeLabel(controls.universeScope)} 扫描。`, 'ok');
    }
    return selectSymbol(normalized);
  }

  function bindControls() {
    const refreshBtn = q('btn-altcoin-radar-refresh');
    if (refreshBtn) {
      refreshBtn.onclick = () => scheduleScan(false, {
        delayMs: 0,
        notifyMessage: '山寨雷达刷新失败',
      });
    }
    const forceBtn = q('btn-altcoin-radar-force-refresh');
    if (forceBtn) {
      forceBtn.onclick = () => scheduleScan(true, {
        delayMs: 0,
        notifyMessage: '山寨雷达强制刷新失败',
      });
    }
    const exchangeEl = q('altcoin-radar-exchange');
    if (exchangeEl) {
      exchangeEl.addEventListener('change', async () => {
        state.universeLoadedFor = '';
        try {
          await loadUniverseOptions(true);
          await scanRadar(false);
        } catch (error) {
          if (typeof notify === 'function') notify(`山寨雷达币池刷新失败: ${error.message}`, true);
        }
      });
    }
    const timeframeEl = q('altcoin-radar-timeframe');
    if (timeframeEl) {
      timeframeEl.addEventListener('change', () => {
        scheduleScan(false, { notifyMessage: '山寨雷达切周期失败' });
      });
    }
    const sortEl = q('altcoin-radar-sort');
    if (sortEl) {
      sortEl.addEventListener('change', () => {
        scheduleScan(false, { notifyMessage: '山寨雷达排序刷新失败' });
      });
    }
    const universeEl = q('altcoin-radar-universe');
    if (universeEl) {
      universeEl.addEventListener('change', () => {
        renderUniverseManager();
        scheduleScan(false, { notifyMessage: '山寨雷达币池更新失败' });
      });
    }
    CLIENT_FILTER_CONTROL_IDS.forEach((id) => {
      const el = q(id);
      if (el) {
        el.addEventListener('change', () => {
          refreshCurrentScan();
          if (state.selectedSymbol) renderInspector(state.detail || { selected_row: findRow(state.selectedSymbol) });
        });
      }
    });
    const excludeEl = q('altcoin-radar-exclude-retired');
    if (excludeEl) {
      excludeEl.addEventListener('change', () => {
        scheduleScan(false, { notifyMessage: '山寨雷达退市过滤刷新失败' });
      });
    }
    const openResearchBtn = q('btn-altcoin-radar-open-research');
    if (openResearchBtn) {
      openResearchBtn.onclick = () => {
        openResearchWorkbench(openResearchBtn.dataset.symbol || state.selectedSymbol).then(() => {
          if (typeof notify === 'function') notify('已带入研究工坊');
        }).catch((error) => {
          if (typeof notify === 'function') notify(`带入研究工坊失败: ${error.message}`, true);
        });
      };
    }
    const addWatchlistBtn = q('btn-altcoin-radar-watchlist-add');
    if (addWatchlistBtn) {
      addWatchlistBtn.onclick = () => {
        mutateWatchlist('add', addWatchlistBtn.dataset.symbol || state.selectedSymbol)
          .then((symbol) => {
            const row = findRow(symbol);
            if (row) {
              row.in_watchlist = true;
              if (!Array.isArray(row.tags)) row.tags = [];
              if (!row.tags.includes('Watchlist')) row.tags.push('Watchlist');
            }
            if (state.detail?.selected_row && String(state.detail.selected_row.symbol || '').trim().toUpperCase() === symbol) {
              state.detail.selected_row.in_watchlist = true;
              state.detail.selected_row.tags = Array.isArray(state.detail.selected_row.tags) ? state.detail.selected_row.tags : [];
              if (!state.detail.selected_row.tags.includes('Watchlist')) state.detail.selected_row.tags.push('Watchlist');
            }
            refreshCurrentScan();
            renderInspector(state.detail || { selected_row: row || findRow(symbol) });
            if (typeof notify === 'function') notify(`已将 ${symbol} 加入 Watchlist`);
          })
          .catch((error) => {
            if (typeof notify === 'function') notify(`Watchlist 更新失败: ${error.message}`, true);
          });
      };
    }
    const addWatchlistManualBtn = q('btn-altcoin-radar-watchlist-add-manual');
    const watchlistInputEl = q('altcoin-radar-watchlist-input');
    const addWatchlistFromInput = () => {
      const rawValue = watchlistInputEl?.value || '';
      mutateWatchlist('add', rawValue)
        .then((symbol) => {
          if (watchlistInputEl) watchlistInputEl.value = '';
          return focusWatchlistSymbol(symbol);
        })
        .then(() => {
          if (typeof notify === 'function') notify('已手动加入 Watchlist');
        })
        .catch((error) => {
          if (typeof notify === 'function') notify(`Watchlist 鏇存柊澶辫触: ${error.message}`, true);
        });
    };
    if (addWatchlistManualBtn) {
      addWatchlistManualBtn.onclick = addWatchlistFromInput;
    }
    if (watchlistInputEl) {
      watchlistInputEl.addEventListener('keydown', (event) => {
        if (event.key !== 'Enter') return;
        event.preventDefault();
        addWatchlistFromInput();
      });
    }
    const removeWatchlistBtn = q('btn-altcoin-radar-watchlist-remove');
    if (removeWatchlistBtn) {
      removeWatchlistBtn.onclick = () => {
        mutateWatchlist('remove', removeWatchlistBtn.dataset.symbol || state.selectedSymbol)
          .then((symbol) => {
            const row = findRow(symbol);
            if (row) {
              row.in_watchlist = false;
              row.tags = Array.isArray(row.tags) ? row.tags.filter((tag) => tag !== 'Watchlist') : [];
            }
            if (state.detail?.selected_row && String(state.detail.selected_row.symbol || '').trim().toUpperCase() === symbol) {
              state.detail.selected_row.in_watchlist = false;
              state.detail.selected_row.tags = Array.isArray(state.detail.selected_row.tags)
                ? state.detail.selected_row.tags.filter((tag) => tag !== 'Watchlist')
                : [];
            }
            refreshCurrentScan();
            renderInspector(state.detail || { selected_row: row || findRow(symbol) });
            if (typeof notify === 'function') notify(`已将 ${symbol} 移出 Watchlist`);
          })
          .catch((error) => {
            if (typeof notify === 'function') notify(`Watchlist 更新失败: ${error.message}`, true);
          });
      };
    }
    eachInspectorAlertButton((btn, kind) => {
      btn.dataset.kind = kind;
      btn.onclick = () => {
        const symbol = btn.dataset.symbol || state.selectedSymbol;
        const row = findRow(symbol) || state.detail?.selected_row;
        const task = alertKindsForRow(row).has(kind)
          ? recyclePresetAlert(kind, symbol)
          : createPresetAlert(kind, symbol);
        task
          .then((resp) => {
            const message = resp?.deleted_count != null
              ? `${symbol} 的${PRESET_BY_KIND[kind]}已回收`
              : resp?.existing
                ? '该预警已存在，已为你标记到当前候选。'
                : `已创建 ${PRESET_BY_KIND[kind]}。`;
            setOutput(JSON.stringify(resp, null, 2));
            setStatus(message, 'ok');
            if (typeof notify === 'function') notify(message);
          })
          .catch((error) => {
            setOutput(`创建预警失败: ${error.message}`);
            if (typeof notify === 'function') notify(`创建预警失败: ${error.message}`, true);
          });
      };
    });
    const recycleAlertBtn = q('btn-altcoin-radar-alert-recycle');
    if (recycleAlertBtn) {
      recycleAlertBtn.onclick = () => {
        recycleAllAlerts(recycleAlertBtn.dataset.symbol || state.selectedSymbol)
          .then((resp) => {
            const symbol = recycleAlertBtn.dataset.symbol || state.selectedSymbol;
            const message = resp?.deleted_count
              ? `${symbol} 的 ${resp.deleted_count} 条预警已回收`
              : `${symbol} 当前没有可回收预警`;
            setOutput(JSON.stringify(resp, null, 2));
            setStatus(message, 'ok');
            if (typeof notify === 'function') notify(message);
          })
          .catch((error) => {
            setOutput(`回收预警失败: ${error.message}`);
            if (typeof notify === 'function') notify(`回收预警失败: ${error.message}`, true);
          });
      };
    }

    const tbody = q('altcoin-radar-ranking-body');
    if (tbody) {
      tbody.addEventListener('click', (event) => {
        const btn = event.target.closest('[data-row-action]');
        if (btn) {
          const action = String(btn.dataset.rowAction || '').trim();
          const symbol = String(btn.dataset.symbol || '').trim();
          if (action === 'inspect') {
            selectSymbol(symbol).catch((error) => {
              if (typeof notify === 'function') notify(`查看详情失败: ${error.message}`, true);
            });
          } else if (action === 'research-proposal') {
            const apiFetch = requireApi();
            apiFetch(`/altcoin/radar/${encodeURIComponent(symbol)}/research-proposal`, {
              method: 'POST',
            }).then((resp) => {
              setOutput(JSON.stringify(resp, null, 2));
              return openResearchWorkbench(symbol).then(() => resp);
            }).then((resp) => {
              if (typeof notify === 'function') notify(`已为 ${symbol} 生成研究提案 ${resp?.proposal_id || ''}`);
            }).catch((error) => {
              if (typeof notify === 'function') notify(`生成研究提案失败: ${error.message}`, true);
            });
          } else if (action === 'research') {
            openResearchWorkbench(symbol).then(() => {
              if (typeof notify === 'function') notify(`已将 ${symbol} 带入研究工坊`);
            }).catch((error) => {
              if (typeof notify === 'function') notify(`带入研究工坊失败: ${error.message}`, true);
            });
          } else if (action === 'alert') {
            const presetKind = btn.dataset.presetKind || 'anomaly';
            const row = findRow(symbol);
            const task = alertKindsForRow(row).has(presetKind)
              ? recyclePresetAlert(presetKind, symbol)
              : createPresetAlert(presetKind, symbol);
            task.then((resp) => {
              const message = resp?.deleted_count != null
                ? `已回收 ${symbol} 的${PRESET_BY_KIND[presetKind] || '预警'}`
                : resp?.existing
                  ? `${symbol} 的预警已存在`
                  : `已为 ${symbol} 创建预警`;
              setOutput(JSON.stringify(resp, null, 2));
              setStatus(message, 'ok');
              if (typeof notify === 'function') notify(message);
            }).catch((error) => {
              if (typeof notify === 'function') notify(`预警操作失败: ${error.message}`, true);
            });
          }
          return;
        }
        const row = event.target.closest('tr[data-symbol]');
        if (row) {
          const symbol = String(row.dataset.symbol || '').trim();
          selectSymbol(symbol).catch((error) => {
            if (typeof notify === 'function') notify(`查看详情失败: ${error.message}`, true);
          });
        }
      });
    }

    const relatedBox = q('altcoin-radar-related-list');
    if (relatedBox) {
      relatedBox.addEventListener('click', (event) => {
        const btn = event.target.closest('[data-related-symbol]');
        if (!btn) return;
        const symbol = String(btn.dataset.relatedSymbol || '').trim();
        selectSymbol(symbol).catch((error) => {
          if (typeof notify === 'function') notify(`切换相关候选失败: ${error.message}`, true);
        });
      });
    }
    const themePeersBox = q('altcoin-radar-theme-peers');
    if (themePeersBox) {
      themePeersBox.addEventListener('click', (event) => {
        const btn = event.target.closest('[data-related-symbol]');
        if (!btn) return;
        const symbol = String(btn.dataset.relatedSymbol || '').trim();
        selectSymbol(symbol).catch((error) => {
          if (typeof notify === 'function') notify(`切换板块联动候选失败: ${error.message}`, true);
        });
      });
    }
    const watchlistList = q('altcoin-radar-watchlist-list');
    if (watchlistList) {
      watchlistList.addEventListener('click', (event) => {
        const btn = event.target.closest('[data-watchlist-symbol]');
        if (!btn) return;
        const symbol = String(btn.dataset.watchlistSymbol || '').trim();
        if (!symbol) return;
        focusWatchlistSymbol(symbol).catch((error) => {
          if (typeof notify === 'function') notify(`切换 Watchlist 候选失败: ${error.message}`, true);
        });
      });
    }

    // Phase 1: radar mode toggle buttons
    const modeBtns = document.querySelectorAll('#altcoin-radar-mode-btns .altcoin-mode-btn');
    modeBtns.forEach((btn) => {
      btn.addEventListener('click', () => {
        modeBtns.forEach((b) => b.classList.remove('active'));
        btn.classList.add('active');
        state.radarMode = String(btn.dataset.mode || 'combined');
        scanRadar(false).catch((error) => {
          if (typeof notify === 'function') notify(`切换雷达模式失败: ${error.message}`, true);
        });
      });
    });

    // Phase 1: universe scope change triggers re-scan
    const scopeEl = q('altcoin-radar-universe-scope');
    if (scopeEl) {
      scopeEl.addEventListener('change', () => {
        renderUniverseManager();
        scanRadar(false).catch((error) => {
          if (typeof notify === 'function') notify(`Universe范围切换失败: ${error.message}`, true);
        });
      });
    }
  }

  function bindAltcoinRadarPage() {
    if (state.bound) return;
    state.bound = true;
    bindControls();
    renderUniverseManager();
    updateInspectorButtonState(null);
    refreshOperatingModeBanner({ force: true }).catch(() => {});
  }

  async function loadAltcoinRadarTabData(force = false) {
    bindAltcoinRadarPage();
    refreshOperatingModeBanner({ force }).catch(() => {});
    const universePromise = loadUniverseOptions(force).catch((error) => {
      console.warn('loadAltcoinRadarTabData universe bootstrap failed', error?.message || error);
    });
    const watchlistPromise = loadWatchlist().catch((error) => {
      console.warn('loadAltcoinRadarTabData watchlist bootstrap failed', error?.message || error);
    });
    await universePromise;
    await watchlistPromise;
    await scanRadar(force);
  }

  window.bindAltcoinRadarPage = bindAltcoinRadarPage;
  window.__loadAltcoinRadarTabData = loadAltcoinRadarTabData;

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', bindAltcoinRadarPage, { once: true });
  } else {
    bindAltcoinRadarPage();
  }
})();
