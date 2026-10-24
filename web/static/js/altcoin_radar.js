(function () {
  const DEFAULT_UNIVERSE = [
    'LINK/USDT',
    'AVAX/USDT',
    'DOT/USDT',
    'ATOM/USDT',
    'FIL/USDT',
    'TAO/USDT',
    'RENDER/USDT',
    'FET/USDT',
    'GALA/USDT',
    'AAVE/USDT',
    'MKR/USDT',
    'UNI/USDT',
    'NEAR/USDT',
    'INJ/USDT',
    'RUNE/USDT',
    'ARB/USDT',
    'OP/USDT',
    'SUI/USDT',
    'APT/USDT',
    'PYTH/USDT',
    'JTO/USDT',
    'JUP/USDT',
    'PEPE/USDT',
    'WIF/USDT',
    'BONK/USDT',
    'FLOKI/USDT',
    'ORDI/USDT',
    'SATS/USDT',
    'IMX/USDT',
    'CRV/USDT',
  ];
  // Keep in sync with ALTCOIN_WATCHLIST in
  // core/research/altcoin_radar_universe.py — used as offline fallback when the
  // /altcoin/radar/watchlist API call fails (cold-start, network blip).
  const DEFAULT_WATCHLIST = [
    'SOL/USDT', 'AVAX/USDT', 'NEAR/USDT', 'APT/USDT', 'SUI/USDT', 'INJ/USDT', 'SEI/USDT', 'TIA/USDT',
    'ORDI/USDT', 'SATS/USDT', 'RATS/USDT', 'STX/USDT',
    'PEPE/USDT', 'FLOKI/USDT', 'BONK/USDT', 'WIF/USDT', 'BOME/USDT', 'MEME/USDT', 'NEIRO/USDT', 'TURBO/USDT', 'POPCAT/USDT', 'MEW/USDT', 'PNUT/USDT', 'GOAT/USDT',
    'TAO/USDT', 'FET/USDT', 'AGIX/USDT', 'RENDER/USDT', 'AKT/USDT', 'OCEAN/USDT', 'WLD/USDT', 'AI16Z/USDT', 'VIRTUAL/USDT', 'ARKM/USDT',
    'ONDO/USDT', 'HNT/USDT', 'IOTX/USDT', 'GRT/USDT', 'POL/USDT',
    'GMX/USDT', 'GNS/USDT', 'JOE/USDT', 'AAVE/USDT', 'UNI/USDT', 'MKR/USDT', 'CRV/USDT', 'PENDLE/USDT', 'DYDX/USDT', 'ENA/USDT', 'ETHFI/USDT',
    'AXS/USDT', 'SAND/USDT', 'MANA/USDT', 'GALA/USDT', 'IMX/USDT', 'BEAM/USDT', 'PIXEL/USDT',
    'PYTH/USDT', 'JTO/USDT', 'JUP/USDT', 'W/USDT', 'KMNO/USDT', 'DRIFT/USDT',
    'BGB/USDT', 'GT/USDT', 'OKB/USDT', 'BNB/USDT',
    'ARB/USDT', 'OP/USDT', 'STRK/USDT', 'MANTA/USDT', 'ALT/USDT', 'ZETA/USDT', 'ZK/USDT', 'BLAST/USDT',
    'PYUSD/USDT', 'EIGEN/USDT', 'REZ/USDT', 'IO/USDT', 'ZRO/USDT', 'OMNI/USDT', 'USUAL/USDT', 'MOVE/USDT', 'ME/USDT', 'VANA/USDT',
  ];

  // The page only scans Binance 4h in combined mode. OKX/Gate have no local K-lines
  // (0 rows), 15m/1h cold scans outlive any sensible request timeout, and the
  // perp-only mode is a row filter that returns nothing without derivatives data.
  const FIXED_SCAN = Object.freeze({
    exchange: 'binance',
    timeframe: '4h',
    radarMode: 'combined',
    excludeRetired: true,
  });
  // A cold scan takes 16-60s and a forced one ~57s; the old 60s limit tripped on both.
  const SCAN_TIMEOUT_MS = 120000;
  const FORCE_SCAN_TIMEOUT_MS = 180000;

  const PRESET_BY_KIND = {
    anomaly: '点火预警',
    accumulation: '跃升预警',
    control: '拥挤预警',
    narrative: '叙事预警',
  };

  const SORT_OPTIONS = new Set(['priority', 'alert']);
  const FILTER_OPTIONS = new Set(['all', 'anomaly', 'watchlist', 'alpha']);
  const UNIVERSE_SCOPES = new Set(['research', 'watchlist']);
  const UNIVERSE_SCOPE_LABELS = {
    research: '研究池 + Alpha',
    watchlist: 'Watchlist 收藏列表',
  };
  const SCAN_DEBOUNCE_MS = 250;
  const BACKGROUND_REFRESH_POLL_MS = 4500;
  const BACKGROUND_REFRESH_MAX_POLLS = 6;
  const ALTCOIN_RADAR_PREFS_KEY = 'altcoinRadar.controls.v1';

  const state = {
    bound: false,
    dom: Object.create(null),
    universeLoadedFor: '',
    universeCatalog: null,
    universeSymbols: [],
    scan: null,
    detail: null,
    selectedSymbol: '',
    filteredRows: [],
    scanSeq: 0,
    detailSeq: 0,
    eventSeq: 0,
    watchlist: DEFAULT_WATCHLIST.slice(),
    eventTimelineBySymbol: new Map(),
    scanDebounceTimer: 0,
    backgroundRefreshTimer: 0,
    backgroundRefreshKey: '',
    backgroundRefreshPolls: 0,
    scanInFlight: false,
    researchProposalInFlight: new Set(),
    preferencesLoaded: false,
    savedControls: {},
  };

  function q(id) {
    if (!id) return null;
    const cached = state.dom[id];
    if (cached && cached.isConnected) return cached;
    const node = document.getElementById(id);
    if (node) state.dom[id] = node;
    return node;
  }

  function safeLocalStorage() {
    try {
      if (typeof window === 'undefined' || !window.localStorage) return null;
      return window.localStorage;
    } catch (_) {
      return null;
    }
  }

  function normalizeSort(value) {
    const text = String(value || 'priority').trim().toLowerCase();
    return SORT_OPTIONS.has(text) ? text : 'priority';
  }

  function normalizeFilter(value) {
    const text = String(value || 'all').trim().toLowerCase();
    return FILTER_OPTIONS.has(text) ? text : 'all';
  }

  function normalizeUniverseScope(value) {
    const text = String(value || 'research').trim().toLowerCase();
    return UNIVERSE_SCOPES.has(text) ? text : 'research';
  }

  function loadSavedControls() {
    if (state.preferencesLoaded) return state.savedControls || {};
    state.preferencesLoaded = true;
    const storage = safeLocalStorage();
    if (!storage) return {};
    try {
      const raw = storage.getItem(ALTCOIN_RADAR_PREFS_KEY);
      const parsed = raw ? JSON.parse(raw) : {};
      state.savedControls = parsed && typeof parsed === 'object' ? parsed : {};
      return state.savedControls;
    } catch (error) {
      state.savedControls = {};
      try { console.warn('[altcoin-radar] failed to load saved controls:', error); } catch (_) {}
      return {};
    }
  }

  function applySavedControls() {
    const saved = loadSavedControls();
    const setSelectValue = (id, value) => {
      const el = q(id);
      if (!(el instanceof HTMLSelectElement)) return;
      const text = String(value || '').trim();
      if (!text) return;
      // Values saved by older versions of the page (sorts, filters, scopes that no
      // longer exist) simply fail this check and fall back to the defaults.
      const hasOption = Array.from(el.options || []).some((opt) => String(opt.value || '') === text);
      if (hasOption) el.value = text;
    };
    setSelectValue('altcoin-radar-sort', saved.sortBy);
    setSelectValue('altcoin-radar-filter', saved.filter);
    setSelectValue('altcoin-radar-universe-scope', normalizeUniverseScope(saved.universeScope));
    state.selectedSymbol = String(saved.selectedSymbol || state.selectedSymbol || '').trim().toUpperCase();
  }

  function saveControls(extra = {}) {
    const storage = safeLocalStorage();
    if (!storage) return;
    const controls = readControls();
    const payload = {
      sortBy: controls.sortBy,
      filter: controls.filter,
      universeScope: controls.universeScope,
      savedAt: new Date().toISOString(),
      ...extra,
      selectedSymbol: String(extra.selectedSymbol || state.selectedSymbol || '').trim().toUpperCase(),
    };
    try {
      storage.setItem(ALTCOIN_RADAR_PREFS_KEY, JSON.stringify(payload));
      state.savedControls = payload;
    } catch (error) {
      try { console.warn('[altcoin-radar] failed to save controls:', error); } catch (_) {}
    }
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
    // Number(null) is 0, which would turn "no data" into a real-looking zero.
    if (value === null || value === undefined || value === '') return fallback;
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

  function fmtAge(seconds) {
    const sec = Math.max(0, Math.round(toNumber(seconds, 0)));
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
      let text = String(item || '').trim().toUpperCase();
      if (text.includes('/')) {
        text = text.split('/', 1)[0];
      } else {
        const suffix = ['-USDT-SWAP', '-USD-SWAP', '_USDT', '_USD', '-USDT', '-USD', 'USDT', 'PERP']
          .find((candidate) => text.endsWith(candidate) && text.length > candidate.length);
        if (suffix) text = text.slice(0, -suffix.length);
      }
      text = text.replace(/[^A-Z0-9]/g, '');
      if (text === 'RNDR') text = 'RENDER';
      if (text === 'MATIC') text = 'POL';
      if (text) text = `${text}/USDT`;
      if (!text || seen.has(text)) return;
      seen.add(text);
      out.push(text);
    });
    return out;
  }

  function readControls() {
    return {
      exchange: FIXED_SCAN.exchange,
      timeframe: FIXED_SCAN.timeframe,
      radarMode: FIXED_SCAN.radarMode,
      excludeRetired: FIXED_SCAN.excludeRetired,
      sortBy: normalizeSort(q('altcoin-radar-sort')?.value),
      filter: normalizeFilter(q('altcoin-radar-filter')?.value),
      universeScope: normalizeUniverseScope(q('altcoin-radar-universe-scope')?.value || 'research'),
      universeSymbols: normalizeSymbols(state.universeSymbols).slice(0, 60),
    };
  }

  function scanControlKey(controls = readControls()) {
    return JSON.stringify({
      sortBy: controls.sortBy,
      universeSymbols: controls.universeSymbols,
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
    if (raw === 'RNDR' || raw === 'RNDRUSDT' || raw === 'RNDR-USDT' || raw === 'RNDR/USDT') return 'RENDER/USDT';
    if (raw.includes('/')) return raw;
    if (raw.includes('-')) return raw.replace('-', '/');
    if (raw.endsWith('USDT') && raw.length > 4) return `${raw.slice(0, -4)}/USDT`;
    return `${raw}/USDT`;
  }

  function universeScopeLabel(scope) {
    return UNIVERSE_SCOPE_LABELS[scope] || UNIVERSE_SCOPE_LABELS.research;
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
        '可以继续等待详情补载，或把币池切到 Watchlist 收藏列表。',
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
    if (controls.universeScope === 'watchlist') {
      return used.length ? used : normalizeSymbols(state.watchlist || []);
    }
    if (controls.universeSymbols.length) return controls.universeSymbols;
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
    const watchlistSummary = q('altcoin-radar-watchlist-summary');
    const catalogMeta = state.universeCatalog || {};
    const watchlistCount = Array.isArray(state.watchlist) ? state.watchlist.length : 0;
    if (summaryEl) {
      let text = '固定币安 4h；已排除停更 / 退市币种。';
      if (controls.universeScope === 'watchlist') {
        text += ` 币池：Watchlist ${watchlistCount} 个成员。`;
      } else {
        const alphaCount = toNumber(catalogMeta.alphaCount, 0);
        text += ` 币池：研究池 ${controls.universeSymbols.length} 个${alphaCount ? `（含 Alpha 目录 ${alphaCount}）` : ''}。`;
      }
      summaryEl.textContent = text;
    }
    if (watchlistSummary) {
      watchlistSummary.textContent = `Watchlist 当前 ${watchlistCount} 个成员，用于叙事 / Meme / 板块观察收藏；点击任一标签会在右侧加载该币详情，币池切到“Watchlist 收藏列表”时它们就是本次扫描来源。`;
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
    if (text.includes('Alpha')) return 'narrative';
    if (text.includes('上行')) return 'ignition';
    if (text === 'degraded') return 'danger';
    if (text === 'fresh') return 'layout';
    return 'muted';
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

  function marketSourceLabel(row) {
    const freshness = row?.freshness || {};
    const dataQuality = row?.data_quality || {};
    const source = String(freshness.market_source || dataQuality.market_source || '').trim();
    const sourceType = String(freshness.market_source_type || dataQuality.market_source_type || '').trim();
    if (source.includes('binance_alpha') || row?.is_alpha) return 'Binance Alpha';
    if (source.includes('binance_futures')) return 'futures ticker';
    if (source.includes('binance_spot')) return 'spot ticker';
    if (source.includes('coinglass')) return 'CoinGlass';
    if (sourceType === 'local_kline' || source === 'local_kline') return 'local K';
    if (sourceType === 'live_snapshot') return source || 'snapshot';
    return source || '--';
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

  function requireApi() {
    if (typeof api !== 'function') {
      throw new Error('altcoin radar requires global api() helper');
    }
    return api;
  }

  async function loadUniverseOptions(force = false) {
    const controls = readControls();
    const cacheKey = `${controls.exchange}`;
    if (!force && state.universeLoadedFor === cacheKey && state.universeSymbols.length) return;
    const apiFetch = requireApi();
    let finalSymbols = DEFAULT_UNIVERSE.slice();
    let defaultCount = Math.min(12, finalSymbols.length);
    try {
      let resp;
      try {
        resp = await apiFetch(`/altcoin/radar/universe?exchange=${encodeURIComponent(controls.exchange)}&refresh=${force ? 'true' : 'false'}`, {
          timeoutMs: 20000,
        });
      } catch (primaryError) {
        // Keep the older research endpoint as an offline/backward-compatible
        // fallback when the backend has not reloaded the new Alpha route yet.
        resp = await apiFetch(`/data/research/symbols?exchange=${encodeURIComponent(controls.exchange)}&include_major=false`, {
          timeoutMs: 15000,
        });
      }
      const symbols = normalizeSymbols(resp?.symbols || []);
      if (symbols.length) finalSymbols = symbols;
      defaultCount = Math.max(1, Math.min(finalSymbols.length, toNumber(resp?.default_count, Math.min(12, finalSymbols.length))));
      state.universeCatalog = {
        source: String(resp?.source || '').trim(),
        fallbackSource: String(resp?.fallback_source || '').trim(),
        warning: String(resp?.warning || '').trim(),
        count: finalSymbols.length,
        alphaCount: toNumber(resp?.alpha_count, Array.isArray(resp?.alpha_symbols) ? resp.alpha_symbols.length : 0),
      };
      if (resp?.warning) {
        setStatus(`候选列表加载提示：${resp.warning}`, 'warn');
      }
    } catch (error) {
      console.warn('loadUniverseOptions failed', error?.message || error);
      state.universeCatalog = {
        source: 'client_default',
        fallbackSource: 'client_default',
        warning: String(error?.message || error || '').trim(),
        count: finalSymbols.length,
        alphaCount: 0,
      };
      setStatus(`候选列表加载失败，已回退到默认列表：${error.message}`, 'warn');
    }
    state.universeSymbols = finalSymbols.slice(0, Math.max(defaultCount, 1));
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
    // Bumped 10s→20s: cold-start of /altcoin/* on first hit can take >10s while
    // FastAPI imports the chain (web.api.data ~7K lines + CoinGlass/DB deps).
    // On hard failure we fall back to the bundled DEFAULT_WATCHLIST instead of
    // leaving the dashboard with an empty rail.
    try {
      const resp = await apiFetch('/altcoin/radar/watchlist', { timeoutMs: 20000 });
      const symbols = normalizeSymbols(resp?.symbols || []);
      state.watchlist = symbols.length ? symbols : DEFAULT_WATCHLIST.slice();
    } catch (err) {
      state.watchlist = DEFAULT_WATCHLIST.slice();
      try { console.warn('[altcoin-radar] watchlist API failed, using static fallback:', err); } catch (_) {}
    }
    renderWatchlist();
    return state.watchlist;
  }

  async function mutateWatchlist(action, symbol) {
    const normalized = normalizeWatchlistSymbolInput(symbol || state.selectedSymbol || '');
    if (!normalized) {
      throw new Error('请先选择一个候选币种');
    }
    const apiFetch = requireApi();
    let resp = null;
    if (action === 'add') {
      resp = await apiFetch('/altcoin/radar/watchlist', {
        method: 'POST',
        timeoutMs: 15000,
        body: JSON.stringify({ symbol: normalized }),
      });
    } else {
      resp = await apiFetch(`/altcoin/radar/watchlist?symbol=${encodeURIComponent(normalized)}`, {
        method: 'DELETE',
        timeoutMs: 15000,
      });
    }
    const updated = normalizeSymbols(resp?.symbols || []);
    if (updated.length) {
      state.watchlist = updated;
      renderWatchlist();
    } else {
      await loadWatchlist();
    }
    return normalized;
  }

  function buildScanQuery(options, refresh = false) {
    const params = new URLSearchParams();
    params.set('exchange', options.exchange);
    params.set('timeframe', options.timeframe);
    params.set('view', options.timeframe);
    params.set('sort_by', options.sortBy);
    params.set('limit', options.universeScope === 'watchlist' ? '120' : '60');
    params.set('exclude_retired', options.excludeRetired ? 'true' : 'false');
    params.set('refresh', refresh ? 'true' : 'false');
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

  function filterLabel(value) {
    const text = String(value || 'all').trim();
    const filterEl = q('altcoin-radar-filter');
    const option = filterEl instanceof HTMLSelectElement
      ? Array.from(filterEl.options || []).find((item) => String(item.value || '') === text)
      : null;
    return String(option?.textContent || text || '全部').trim();
  }

  function activeFilterSummary(controls = readControls()) {
    return controls.filter && controls.filter !== 'all' ? filterLabel(controls.filter) : '全部';
  }

  function clearClientFilters() {
    const filterEl = q('altcoin-radar-filter');
    if (filterEl instanceof HTMLSelectElement) filterEl.value = 'all';
    saveControls();
    refreshCurrentScan();
    if (state.selectedSymbol) renderInspector(state.detail || { selected_row: findRow(state.selectedSymbol) });
  }

  function applyClientFilters(rows) {
    const controls = readControls();
    return (Array.isArray(rows) ? rows : []).filter((row) => {
      // Alpha rows (new listings with no derivatives and thin liquidity) only show up
      // behind their own filter, so they don't crowd out the main board.
      if (controls.filter === 'alpha') return !!row?.is_alpha;
      if (row?.is_alpha) return false;
      if (controls.filter === 'anomaly') return String(row?.signal_state || '').includes('异动');
      if (controls.filter === 'watchlist') return !!row?.in_watchlist;
      return true;
    });
  }

  function dedupeRowsBySymbol(rows) {
    const out = [];
    const seen = new Set();
    (Array.isArray(rows) ? rows : []).forEach((rawRow) => {
      const row = rawRow && typeof rawRow === 'object' ? { ...rawRow } : {};
      const symbol = normalizeSymbols([row.symbol])[0] || '';
      if (!symbol || seen.has(symbol)) return;
      seen.add(symbol);
      row.symbol = symbol;
      row.tags = Array.from(new Set(
        (Array.isArray(row.tags) ? row.tags : [])
          .map((tag) => String(tag || '').trim())
          .filter(Boolean)
      ));
      out.push(row);
    });
    return out;
  }

  function hasCoinglassOutage(warnings) {
    return (Array.isArray(warnings) ? warnings : []).some((item) => /CoinGlass market snapshot unavailable/i.test(String(item || '')));
  }

  function renderHealth(scanPayload) {
    const chipsBox = q('altcoin-radar-health-chips');
    const warningBox = q('altcoin-radar-warning-list');
    const tableNote = q('altcoin-radar-table-note');
    const meta = scanPayload?.scan_meta || {};
    const cache = meta?.cache || {};
    const summary = scanPayload?.summary || {};
    const warnings = Array.isArray(scanPayload?.warnings) ? scanPayload.warnings : [];
    if (chipsBox) {
      const chips = [];
      const cacheText = cache.refreshing
        ? (cache.stale ? '旧结果回显，后台刷新中' : '缓存回显，后台刷新中')
        : (cache.hit ? `缓存命中 ${toNumber(cache.age_sec, 0).toFixed(0)}s / ${cache.ttl_sec || '--'}s` : '新鲜计算');
      chips.push(['缓存', cacheText, 'neutral']);
      const scanned = toNumber(summary.scanned_count, 0);
      const degraded = toNumber(summary.degraded_count, 0);
      if (scanned) {
        chips.push(['降级行', `${degraded} / ${scanned}`, degraded / scanned > 0.3 ? 'warn' : 'neutral']);
      }
      chips.push(hasCoinglassOutage(warnings)
        ? ['衍生品', 'CoinGlass 快照不可用，已回退公开行情', 'warn']
        : ['衍生品', 'CoinGlass 快照正常', 'ok']);
      if (meta.generated_at) chips.push(['更新', fmtDateTime(meta.generated_at), 'neutral']);
      chipsBox.innerHTML = chips
        .map(([label, value, tone]) => `<span class="altcoin-radar-health-chip" data-tone="${escapeHtml(tone)}"><em>${escapeHtml(label)}</em>${escapeHtml(value)}</span>`)
        .join('');
    }
    if (warningBox) {
      const lines = warnings.length ? warnings : ['暂无警告'];
      warningBox.innerHTML = lines
        .slice(0, 6)
        .map((warning) => `<div class="altcoin-radar-warning">${escapeHtml(String(warning || ''))}</div>`)
        .join('');
      const summaryEl = warningBox.closest('details')?.querySelector('summary');
      if (summaryEl) summaryEl.textContent = warnings.length ? `数据说明（${warnings.length} 条提示）` : '数据说明';
    }
    if (tableNote) {
      const filtered = Array.isArray(state.filteredRows) ? state.filteredRows.length : 0;
      const rows = Array.isArray(scanPayload?.rows) ? scanPayload.rows : [];
      const controls = readControls();
      const hiddenAlpha = controls.filter === 'alpha' ? 0 : rows.filter((row) => row?.is_alpha).length;
      const filterText = activeFilterSummary(controls);
      tableNote.textContent = `当前展示 ${filtered} 条${filterText !== '全部' ? ` · ${filterText}` : ''}${hiddenAlpha ? ` · 已隐藏 ${hiddenAlpha} 个 Alpha 新币` : ''}`;
    }
  }

  function renderTagRow(tags) {
    const list = Array.from(new Set(
      (Array.isArray(tags) ? tags : [])
        .map((tag) => String(tag || '').trim())
        .filter(Boolean)
    ));
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
    const sourceLabel = marketSourceLabel(row);
    return `
      <span class="altcoin-radar-tag" data-tone="${escapeHtml(tagTone(label))}">${escapeHtml(label)}</span>
      <span class="altcoin-radar-freshness">${escapeHtml(marketFresh)} / ${escapeHtml(snapFresh)} · ${escapeHtml(sourceLabel)}</span>
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

  function displaySymbolLabel(row) {
    const symbol = String(row?.symbol || '').trim();
    const alpha = row?.alpha_context || {};
    const display = String(alpha.display_symbol || '').trim();
    return display && display !== symbol ? `${display} · ${symbol}` : symbol;
  }

  function renderRanking(rows) {
    const tbody = q('altcoin-radar-ranking-body');
    if (!tbody) return;
    const filteredRows = applyClientFilters(dedupeRowsBySymbol(rows));
    state.filteredRows = filteredRows;
    if (!filteredRows.length) {
      const filterText = activeFilterSummary();
      tbody.innerHTML = `
        <tr>
          <td colspan="6" class="altcoin-radar-empty">
            当前过滤条件下没有候选：${escapeHtml(filterText)}。
            <button type="button" class="btn btn-sm" data-row-action="clear-filters">清除筛选</button>
          </td>
        </tr>
      `;
      return;
    }
    tbody.innerHTML = filteredRows
      .map((row, index) => {
        const symbol = String(row?.symbol || '').trim();
        const symbolLabel = displaySymbolLabel(row);
        const selected = symbol && symbol === state.selectedSymbol;
        const defaultPreset = pickDefaultPreset(row);
        const hasDefaultAlert = alertKindsForRow(row).has(defaultPreset);
        const upsideClass = toNumber(row?.upside_score, 0) >= 0.70 ? 'altcoin-radar-score-badge score-high' : 'altcoin-radar-score-badge';
        const stateText = String(row?.signal_state || '待跟踪');
        // The state is already the sub-line; only show the other tags (Watchlist,
        // 数据降级, Binance Alpha, 已建预警) as chips so nothing is said twice.
        const extraTags = (Array.isArray(row?.tags) ? row.tags : []).filter((tag) => tag && tag !== stateText && tag !== '待跟踪');
        return `
          <tr class="${selected ? 'is-selected' : ''}" data-symbol="${escapeHtml(symbol)}">
            <td><span class="altcoin-radar-rank-chip">${index + 1}</span></td>
            <td>
              <div class="altcoin-radar-symbol-cell">
                <div class="altcoin-radar-symbol-main">${escapeHtml(symbolLabel || '--')}</div>
                <div class="altcoin-radar-symbol-sub">${escapeHtml(stateText)}</div>
                ${extraTags.length ? `<div class="altcoin-radar-tag-row">${renderTagRow(extraTags)}</div>` : ''}
              </div>
            </td>
            <td><span class="${upsideClass}" title="多因子上行筛选分，不代表收益承诺">${escapeHtml(shortPercent(row?.upside_score))}</span></td>
            <td><span class="altcoin-radar-score-badge">${escapeHtml(shortPercent(row?.alert_score))}</span></td>
            <td>${freshnessChip(row)}</td>
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

  function renderReasonList(containerId, items, emptyText) {
    const box = q(containerId);
    if (!box) return;
    const list = Array.isArray(items) ? items : [];
    if (!list.length) {
      box.innerHTML = emptyText ? `<div class="altcoin-radar-reason">${escapeHtml(emptyText)}</div>` : '';
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
      timelineEl.innerHTML = '<span class="text-muted u-note">暂无近期事件</span>';
      return;
    }
    const eventTypeLabel = (t) => ({
      altcoin_ignition_cross_up: '点火',
      altcoin_rank_jump_top_n: '跃升',
      altcoin_perp_burst: 'Perp 爆发',
      altcoin_crowding_risk_spike: '拥挤',
      altcoin_narrative_heat_spike: '叙事热度',
    }[t] || t);
    const num = (value) => (Number.isFinite(Number(value)) ? Number(value).toFixed(2) : '--');
    timelineEl.innerHTML = rows.map((e) => {
      const ts = e.ts_iso ? new Date(e.ts_iso).toLocaleTimeString('zh-CN', { hour12: false }) : '--';
      const detail = e.ignition_score != null
        ? `点火=${num(e.ignition_score)}`
        : e.rank_jump != null
        ? `跃升 ${e.prev_rank}→${e.current_rank}`
        : e.crowding_late_score != null
        ? `拥挤=${num(e.crowding_late_score)}`
        : e.narrative_heat_score != null
        ? `叙事=${num(e.narrative_heat_score)}`
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
    q('altcoin-radar-selected-symbol').textContent = displaySymbolLabel(selected) || '--';
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
      ['潜在上行', selected.upside_score],
      ['异动分', selected.alert_score],
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

    const actionPlan = detailPayload?.action_plan || {};
    const actionItems = [];
    if (actionPlan.stance) actionItems.push(`结论：${String(actionPlan.stance)}`);
    if (actionPlan.summary) actionItems.push(String(actionPlan.summary));
    if (Array.isArray(actionPlan.actions)) actionItems.push(...actionPlan.actions.map((item) => String(item || '').trim()).filter(Boolean));
    renderReasonList('altcoin-radar-action-list', actionItems, '当前暂无观察建议。');
    const actionTags = q('altcoin-radar-action-tags');
    if (actionTags) {
      const tone = String(actionPlan.tone || 'muted').trim() || 'muted';
      const tags = [actionPlan.primary_action, actionPlan.secondary_action]
        .map((item) => String(item || '').trim())
        .filter(Boolean);
      actionTags.innerHTML = tags.length
        ? tags.map((item) => `<span class="altcoin-radar-tag" data-tone="${escapeHtml(tone)}">${escapeHtml(item)}</span>`).join('')
        : '';
    }
    const invalidate = Array.isArray(detailPayload?.invalidate_conditions) ? detailPayload.invalidate_conditions : [];
    renderReasonList(
      'altcoin-radar-invalidate-list',
      invalidate.map((item) => `失效：${String(item || '')}`),
      ''
    );

    // Only fields that carry real information: the derivatives/chain percentiles and
    // metric placeholders that used to sit here were constants whenever the
    // CoinGlass snapshot was unavailable, which is nearly always.
    const dataQuality = selected?.data_quality || {};
    const selectedFreshness = selected?.freshness || {};
    const alphaContext = selected?.alpha_context || {};
    renderListItems('altcoin-radar-data-quality', [
      ['行情源', marketSourceLabel(selected)],
      ['行情时间', selectedFreshness.as_of ? fmtDateTime(selectedFreshness.as_of) : '--'],
      ['行情新鲜度', toPercent(dataQuality.market_data_freshness, 0)],
      ['快照新鲜度', toPercent(dataQuality.snapshot_freshness, 0)],
      ['衍生品', formatDerivativesStatus(detailPayload, selected)],
      ...(selected?.is_alpha ? [
        ['Alpha 链', alphaContext.chain_name || '--'],
        ['Alpha 流动性', toNumber(alphaContext.liquidity_usd, NaN) >= 0 ? `$${Math.round(toNumber(alphaContext.liquidity_usd, 0)).toLocaleString('en-US')}` : '--'],
        ['Alpha 持有人', alphaContext.holders != null ? String(alphaContext.holders) : '--'],
      ] : []),
      ['降级原因', Array.isArray(dataQuality.degraded_reason) && dataQuality.degraded_reason.length ? dataQuality.degraded_reason.join(', ') : '无'],
    ]);

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
    timelineEl.innerHTML = '<span class="text-muted u-note">加载中...</span>';
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
        timelineEl.innerHTML = '<span class="text-muted u-note">事件加载失败</span>';
        if (badgeEl) badgeEl.style.display = 'none';
      }
      return [];
    }
  }

  function currentAlertKind() {
    const select = q('altcoin-radar-alert-kind');
    const value = String(select?.value || '').trim();
    return PRESET_BY_KIND[value] ? value : 'anomaly';
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
    const kind = currentAlertKind();
    const label = PRESET_BY_KIND[kind] || '预警';
    const kindSelect = q('altcoin-radar-alert-kind');
    if (kindSelect) kindSelect.disabled = disabled;
    const toggleBtn = q('btn-altcoin-radar-alert-toggle');
    if (toggleBtn) {
      toggleBtn.disabled = disabled;
      toggleBtn.dataset.kind = kind;
      toggleBtn.dataset.symbol = disabled ? '' : symbol;
      toggleBtn.textContent = activeKinds.has(kind) ? `回收${label}` : `建${label}`;
      toggleBtn.title = activeKinds.has(kind) ? `当前候选已存在${label}` : '';
    }
    const recycleBtn = q('btn-altcoin-radar-alert-recycle');
    if (recycleBtn) {
      recycleBtn.disabled = disabled || !activeKinds.size;
      recycleBtn.dataset.symbol = disabled ? '' : symbol;
      recycleBtn.title = activeKinds.size ? `当前候选共 ${activeKinds.size} 条预警，可一键回收` : '当前候选暂无可回收预警';
    }
  }

  function renderScan(scanPayload) {
    const normalizedPayload = scanPayload && typeof scanPayload === 'object'
      ? { ...scanPayload, rows: dedupeRowsBySymbol(scanPayload.rows || []) }
      : null;
    state.scan = normalizedPayload;
    renderRanking(normalizedPayload?.rows || []);
    renderHealth(normalizedPayload || {});
  }

  async function selectSymbol(symbol, refresh = false) {
    const normalized = String(symbol || '').trim().toUpperCase();
    if (!normalized) return;
    const changed = normalized !== String(state.selectedSymbol || '').trim().toUpperCase();
    state.selectedSymbol = normalized;
    saveControls({ selectedSymbol: normalized });
    renderWatchlist();
    syncSelectedRankingRow();
    const selectedRow = resolveSelectedRow(normalized);
    const inCurrentScan = Boolean(findRow(normalized));
    const kindSelect = q('altcoin-radar-alert-kind');
    // Start the alert dropdown on the preset that fits the candidate, but never
    // overwrite a choice the user is in the middle of making for the same symbol.
    if (kindSelect && changed && selectedRow) kindSelect.value = pickDefaultPreset(selectedRow);
    updateInspectorButtonState(selectedRow);
    const seq = ++state.detailSeq;
    // Drop the previous symbol's detail right away: watchlist/alert actions taken
    // while this one is still loading must not re-render the old candidate.
    state.detail = null;
    renderInspector({ selected_row: selectedRow }, '__loading__');
    try {
      const apiFetch = requireApi();
      const detail = await apiFetch(`/altcoin/radar/detail?${buildDetailQuery(normalized, refresh)}`, {
        timeoutMs: inCurrentScan ? 30000 : 12000,
      });
      if (seq !== state.detailSeq) return;
      state.detail = detail;
      renderInspector(detail);
    } catch (error) {
      if (seq !== state.detailSeq) return;
      state.detail = null;
      renderInspector({ selected_row: selectedRow }, error.message || '详情加载失败');
    }
  }

  async function scanRadar(refresh = false, options = {}) {
    const controls = readControls();
    const seq = ++state.scanSeq;
    const previousScan = state.scan;
    state.detailSeq += 1;
    clearBackgroundRefreshPoll();
    if (!options.preserveStatus) {
      setStatus(refresh ? '正在强制刷新雷达（约 1 分钟），保留上次榜单...' : '正在加载山寨雷达榜单...', 'warn');
    }
    setScanBusy(true);
    try {
      const apiFetch = requireApi();
      const response = await apiFetch(`/altcoin/radar/scan?${buildScanQuery(controls, refresh)}`, {
        timeoutMs: refresh ? FORCE_SCAN_TIMEOUT_MS : SCAN_TIMEOUT_MS,
      });
      if (seq !== state.scanSeq) return;
      renderScan(response);
      const cache = response?.scan_meta?.cache || {};
      const leaderSymbol = state.filteredRows?.[0]?.symbol || response?.rows?.[0]?.symbol || '';
      const preferred = shouldKeepSelectedSymbol(response?.rows || [], state.selectedSymbol)
        ? String(state.selectedSymbol || '').trim().toUpperCase()
        : leaderSymbol;
      const statusText = cache.refreshing
        ? `山寨雷达后台刷新中，先展示上一版结果：${response?.summary?.scanned_count || 0} 币`
        : cache.hit
          ? `已加载缓存结果：${response?.summary?.scanned_count || 0} 币`
          : `已完成实时扫描：${response?.summary?.scanned_count || 0} 币`;
      setStatus(statusText, 'ok');
      scheduleBackgroundRefreshPoll(cache, controls);
      if (preferred) {
        await selectSymbol(preferred, false);
      } else {
        renderInspector({}, '当前没有可检视候选');
      }
    } catch (error) {
      if (seq !== state.scanSeq) return;
      setStatus(`刷新失败，已保留上次结果：${error.message}`, 'danger');
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
      mode: controls.radarMode,
      view: controls.timeframe,
      universe_scope: controls.universeScope,
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
      mode: String(params?.mode || controls.radarMode).trim(),
      view: String(params?.view || controls.timeframe).trim(),
      universe_scope: String(params?.universe_scope || controls.universeScope).trim(),
      config_key: String(params?.config_key || resp?.rule_meta?.config_key || '').trim(),
      threshold: Number(params?.threshold || 0),
      enabled: rule?.enabled !== false,
    });
    rerenderAlertState(selectedSymbol);
    return resp;
  }

  function alertDeleteParams(selectedSymbol, controls, universeSymbols) {
    const params = new URLSearchParams();
    params.set('exchange', controls.exchange);
    params.set('timeframe', controls.timeframe);
    params.set('symbol', selectedSymbol);
    params.set('mode', controls.radarMode);
    params.set('view', controls.timeframe);
    params.set('universe_scope', controls.universeScope);
    if (universeSymbols.length) params.set('symbols', universeSymbols.join(','));
    return params;
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
    const params = alertDeleteParams(selectedSymbol, readControls(), currentUniverseSymbols());
    params.set('preset', preset);
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
    const params = alertDeleteParams(selectedSymbol, readControls(), currentUniverseSymbols());
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
      researchOutput.textContent = `已从山寨雷达带入研究工坊：${selectedSymbol}\nexchange=${controls.exchange}\ntimeframe=${controls.timeframe}\nuniverse_scope=${controls.universeScope}\nuniverse=${universeSymbols.join(', ')}\n下一步建议：先运行“研究总览”，再决定是否继续多币种 / 链上验证。`;
    }
  }

  async function openAiResearchProposal(response, symbol) {
    const proposalId = String(response?.proposal_id || response?.proposal?.proposal_id || '').trim();
    if (!proposalId) {
      await openResearchWorkbench(symbol);
      return;
    }
    if (typeof activateTab === 'function') {
      activateTab('ai-research');
    }
    await new Promise((resolve) => setTimeout(resolve, 0));
    if (typeof window !== 'undefined' && typeof window.AI?.refreshWorkbench === 'function') {
      await window.AI.refreshWorkbench(proposalId, '');
      return;
    }
    if (typeof refreshAiResearchModules === 'function') {
      await refreshAiResearchModules();
    }
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

  function notifySafe(message, isError = false) {
    if (typeof notify === 'function') notify(message, isError);
  }

  function applyWatchlistFlag(symbol, inWatchlist) {
    const apply = (target) => {
      if (!target) return;
      target.in_watchlist = inWatchlist;
      const tags = Array.isArray(target.tags) ? target.tags.filter((tag) => tag !== 'Watchlist') : [];
      if (inWatchlist) tags.push('Watchlist');
      target.tags = tags;
    };
    apply(findRow(symbol));
    if (state.detail?.selected_row && String(state.detail.selected_row.symbol || '').trim().toUpperCase() === symbol) {
      apply(state.detail.selected_row);
    }
    refreshCurrentScan();
    const detailMatches = String(state.detail?.selected_row?.symbol || '').trim().toUpperCase() === symbol;
    renderInspector(detailMatches ? state.detail : { selected_row: findRow(symbol) || resolveSelectedRow(symbol) });
  }

  function runAlertToggle(kind, symbol) {
    const row = findRow(symbol) || state.detail?.selected_row;
    const label = PRESET_BY_KIND[kind] || '预警';
    const recycling = alertKindsForRow(row).has(kind);
    const task = recycling ? recyclePresetAlert(kind, symbol) : createPresetAlert(kind, symbol);
    return task
      .then((resp) => {
        const message = recycling
          ? `${symbol} 的${label}已回收`
          : resp?.existing
            ? `${symbol} 的${label}已存在，已为你标记到当前候选`
            : `已为 ${symbol} 创建${label}`;
        setStatus(message, 'ok');
        notifySafe(message);
      })
      .catch((error) => {
        notifySafe(`预警操作失败: ${error.message}`, true);
      });
  }

  function handleResearchProposal(btn, symbol) {
    const apiFetch = requireApi();
    const proposalKey = symbol.toUpperCase();
    if (state.researchProposalInFlight.has(proposalKey)) return;
    state.researchProposalInFlight.add(proposalKey);
    const originalText = btn.textContent;
    btn.disabled = true;
    btn.textContent = '生成中...';
    const controls = readControls();
    const params = new URLSearchParams({
      exchange: controls.exchange,
      timeframe: controls.timeframe,
      mode: controls.radarMode,
      universe_scope: controls.universeScope,
    });
    apiFetch(`/altcoin/radar/${encodeURIComponent(symbol)}/research-proposal?${params.toString()}`, {
      method: 'POST',
      timeoutMs: 60000,
    }).then((resp) => openAiResearchProposal(resp, symbol).catch((error) => {
      console.warn('open AI research proposal failed', error?.message || error);
      notifySafe(`提案已生成，但打开 AI 研究队列失败: ${error?.message || error}`, true);
    }).then(() => resp)).then((resp) => {
      notifySafe(`已为 ${symbol} 生成研究提案 ${resp?.proposal_id || ''}`);
    }).catch((error) => {
      notifySafe(`生成研究提案失败: ${error.message}`, true);
    }).finally(() => {
      state.researchProposalInFlight.delete(proposalKey);
      if (btn.isConnected) {
        btn.disabled = false;
        btn.textContent = originalText;
      }
    });
  }

  function bindControls() {
    const refreshBtn = q('btn-altcoin-radar-refresh');
    if (refreshBtn) {
      refreshBtn.onclick = () => {
        saveControls();
        scheduleScan(false, { delayMs: 0, notifyMessage: '山寨雷达刷新失败' });
      };
    }
    const forceBtn = q('btn-altcoin-radar-force-refresh');
    if (forceBtn) {
      forceBtn.onclick = () => {
        saveControls();
        scheduleScan(true, { delayMs: 0, notifyMessage: '山寨雷达强制刷新失败' });
      };
    }
    const sortEl = q('altcoin-radar-sort');
    if (sortEl) {
      sortEl.addEventListener('change', () => {
        saveControls();
        scheduleScan(false, { notifyMessage: '山寨雷达排序刷新失败' });
      });
    }
    const filterEl = q('altcoin-radar-filter');
    if (filterEl) {
      filterEl.addEventListener('change', () => {
        saveControls();
        refreshCurrentScan();
        if (state.selectedSymbol) renderInspector(state.detail || { selected_row: findRow(state.selectedSymbol) });
      });
    }
    const scopeEl = q('altcoin-radar-universe-scope');
    if (scopeEl) {
      scopeEl.addEventListener('change', () => {
        saveControls();
        renderUniverseManager();
        scanRadar(false).catch((error) => {
          notifySafe(`币池切换失败: ${error.message}`, true);
        });
      });
    }
    const kindEl = q('altcoin-radar-alert-kind');
    if (kindEl) {
      kindEl.addEventListener('change', () => {
        updateInspectorButtonState(findRow(state.selectedSymbol) || state.detail?.selected_row || null);
      });
    }
    const openResearchBtn = q('btn-altcoin-radar-open-research');
    if (openResearchBtn) {
      openResearchBtn.onclick = () => {
        openResearchWorkbench(openResearchBtn.dataset.symbol || state.selectedSymbol).then(() => {
          notifySafe('已带入研究工坊');
        }).catch((error) => {
          notifySafe(`带入研究工坊失败: ${error.message}`, true);
        });
      };
    }
    const addWatchlistBtn = q('btn-altcoin-radar-watchlist-add');
    if (addWatchlistBtn) {
      addWatchlistBtn.onclick = () => {
        mutateWatchlist('add', addWatchlistBtn.dataset.symbol || state.selectedSymbol)
          .then((symbol) => {
            applyWatchlistFlag(symbol, true);
            notifySafe(`已将 ${symbol} 加入 Watchlist`);
          })
          .catch((error) => {
            notifySafe(`Watchlist 更新失败: ${error.message}`, true);
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
          notifySafe('已手动加入 Watchlist');
        })
        .catch((error) => {
          notifySafe(`Watchlist 更新失败: ${error.message}`, true);
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
            applyWatchlistFlag(symbol, false);
            notifySafe(`已将 ${symbol} 移出 Watchlist`);
          })
          .catch((error) => {
            notifySafe(`Watchlist 更新失败: ${error.message}`, true);
          });
      };
    }
    const toggleAlertBtn = q('btn-altcoin-radar-alert-toggle');
    if (toggleAlertBtn) {
      toggleAlertBtn.onclick = () => {
        const symbol = toggleAlertBtn.dataset.symbol || state.selectedSymbol;
        if (!symbol) return;
        runAlertToggle(currentAlertKind(), symbol);
      };
    }
    const recycleAlertBtn = q('btn-altcoin-radar-alert-recycle');
    if (recycleAlertBtn) {
      recycleAlertBtn.onclick = () => {
        const symbol = recycleAlertBtn.dataset.symbol || state.selectedSymbol;
        recycleAllAlerts(symbol)
          .then((resp) => {
            const message = resp?.deleted_count
              ? `${symbol} 的 ${resp.deleted_count} 条预警已回收`
              : `${symbol} 当前没有可回收预警`;
            setStatus(message, 'ok');
            notifySafe(message);
          })
          .catch((error) => {
            notifySafe(`回收预警失败: ${error.message}`, true);
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
          if (action === 'clear-filters') {
            clearClientFilters();
          } else if (action === 'inspect') {
            selectSymbol(symbol).catch((error) => {
              notifySafe(`查看详情失败: ${error.message}`, true);
            });
          } else if (action === 'research-proposal') {
            handleResearchProposal(btn, symbol);
          } else if (action === 'alert') {
            runAlertToggle(btn.dataset.presetKind || 'anomaly', symbol);
          }
          return;
        }
        const row = event.target.closest('tr[data-symbol]');
        if (row) {
          const symbol = String(row.dataset.symbol || '').trim();
          selectSymbol(symbol).catch((error) => {
            notifySafe(`查看详情失败: ${error.message}`, true);
          });
        }
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
          notifySafe(`切换 Watchlist 候选失败: ${error.message}`, true);
        });
      });
    }
  }

  const PUMP_DRIVER_LABELS = {
    log_mcap: '小市值',
    oi_mcap: 'OI/市值',
    oi_chg_7d: 'OI 7日增',
    oi_chg_30d: 'OI 30日增',
    funding_7d: '费率偏正',
    ret_7d: '7日动量',
    ret_30d: '30日动量',
    dd_from_ath: '近高点',
    vola_30d: '高波动',
    vol_trend: '量能抬升',
    vol_pctile_90d: '量能高分位',
    range_20d: '宽振幅',
    age_days: '上市新',
    pumped_before_120d: '近期惯犯',
    pumped_ever: '历史惯犯',
    log_vol30: '流动性',
  };

  function fmtPumpMcap(value) {
    const numeric = toNumber(value, 0);
    if (numeric <= 0) return '--';
    if (numeric >= 1e9) return `$${(numeric / 1e9).toFixed(2)}B`;
    return `$${(numeric / 1e6).toFixed(1)}M`;
  }

  function setPumpWatchlistNote(text, tone = 'neutral') {
    const note = q('pump-watchlist-note');
    if (!note) return;
    note.textContent = text;
    note.dataset.tone = tone;
  }

  function renderPumpWatchlist(payload) {
    const body = q('pump-watchlist-body');
    if (!body) return;
    if (!payload || payload.available === false) {
      const reason = payload && payload.reason === 'not_generated'
        ? '名单尚未生成 — 点右上「重新生成」或运行 scripts/generate_pump_watchlist.py'
        : `名单不可用（${escapeHtml(String((payload && payload.reason) || 'unknown'))}）`;
      body.innerHTML = `<tr><td colspan="9" class="altcoin-radar-empty">${reason}</td></tr>`;
      setPumpWatchlistNote('未生成', 'warn');
      return;
    }
    const data = payload.data || {};
    const rows = Array.isArray(data.top) ? data.top : [];
    if (!rows.length) {
      body.innerHTML = '<tr><td colspan="9" class="altcoin-radar-empty">名单为空</td></tr>';
      setPumpWatchlistNote('名单为空', 'warn');
      return;
    }
    body.innerHTML = rows
      .map((row) => {
        const drivers = (Array.isArray(row.drivers) ? row.drivers : [])
          .map((key) => PUMP_DRIVER_LABELS[key] || key)
          .join(' · ');
        const funding = toNumber(row.funding_7d, 0);
        const top10 = row.top10_holder_pct;
        const top10Cell = top10 == null
          ? '--'
          : `${toNumber(top10, 0).toFixed(0)}%`;
        return [
          '<tr>',
          `<td>${toNumber(row.rank, 0)}</td>`,
          `<td><strong>${escapeHtml(String(row.base || ''))}</strong>${row.exchange_notice ? ` <span class="altcoin-radar-tag" style="color:${row.exchange_notice === 'monitoring_tag' ? 'var(--warning)' : 'var(--negative)'};" title="Binance 公告">${row.exchange_notice === 'monitoring_tag' ? '监控标签' : '下架公告'}</span>` : ''}${row.delist_risk_flagged ? ` <span class="altcoin-radar-tag" style="color:var(--warning);" title="下架风险分位 ${(Number(row.delist_risk_percentile) * 100).toFixed(0)}%：成交量低、走弱、深度回撤的币被下架概率约为平均的 5 倍；仅提示，约 9/10 被标记的币 60 天内不会被下架">高下架风险</span>` : ''}</td>`,
          `<td>${toNumber(row.score, 0).toFixed(3)}</td>`,
          `<td>${escapeHtml(drivers || '--')}</td>`,
          `<td>${fmtPumpMcap(row.mcap_usd)}</td>`,
          `<td>${(toNumber(row.oi_mcap, 0) * 100).toFixed(1)}%</td>`,
          `<td>${(toNumber(row.ret_30d, 0) * 100).toFixed(0)}%</td>`,
          `<td>${(funding * 100).toFixed(3)}%</td>`,
          `<td>${top10Cell}</td>`,
          '</tr>',
        ].join('');
      })
      .join('');
    const generated = data.generated_at ? fmtDateTime(data.generated_at) : '--';
    const excluded = Object.keys(data.excluded_by_exchange_notice || {});
    const noticeText = excluded.length ? ` · 因下架公告剔除：${excluded.map((b) => escapeHtml(b)).join('、')}` : '';
    const ageText = (payload.age_days != null ? `${Number(payload.age_days).toFixed(1)} 天前` : '') + noticeText;
    if (payload.refresh && payload.refresh.running) {
      setPumpWatchlistNote('后台生成中（约 15 分钟）...', 'info');
    } else if (payload.stale) {
      setPumpWatchlistNote(`已过期：${generated}（${ageText}），建议重新生成`, 'warn');
    } else {
      setPumpWatchlistNote(`生成于 ${generated}（${ageText}）`, 'neutral');
    }
  }

  async function loadPumpWatchlist() {
    const apiFetch = requireApi();
    if (!apiFetch) return;
    try {
      const resp = await apiFetch('/altcoin/radar/pump-watchlist', { timeoutMs: 20000 });
      state.pumpWatchlist = resp;
      renderPumpWatchlist(resp);
      if (resp && resp.refresh && resp.refresh.running) {
        schedulePumpWatchlistPoll();
      }
    } catch (error) {
      setPumpWatchlistNote(`加载失败：${error?.message || error}`, 'warn');
    }
  }

  function schedulePumpWatchlistPoll() {
    if (state.pumpWatchlistPollTimer) window.clearTimeout(state.pumpWatchlistPollTimer);
    state.pumpWatchlistPollCount = (state.pumpWatchlistPollCount || 0) + 1;
    if (state.pumpWatchlistPollCount > 25) return;
    state.pumpWatchlistPollTimer = window.setTimeout(() => {
      loadPumpWatchlist().catch(() => {});
    }, 60000);
  }

  async function triggerPumpWatchlistRefresh() {
    const apiFetch = requireApi();
    if (!apiFetch) return;
    const button = q('btn-pump-watchlist-refresh');
    if (button) button.disabled = true;
    try {
      const resp = await apiFetch('/altcoin/radar/pump-watchlist/refresh', {
        method: 'POST',
        timeoutMs: 20000,
      });
      if (resp && resp.success) {
        state.pumpWatchlistPollCount = 0;
        setPumpWatchlistNote('后台生成已启动（约 15 分钟），完成后自动刷新', 'info');
        schedulePumpWatchlistPoll();
      } else {
        setPumpWatchlistNote(resp && resp.reason === 'already_running' ? '已在生成中，请稍候' : '启动失败', 'warn');
        schedulePumpWatchlistPoll();
      }
    } catch (error) {
      setPumpWatchlistNote(`刷新失败：${error?.message || error}`, 'warn');
    } finally {
      if (button) button.disabled = false;
    }
  }

  function bindAltcoinRadarPage() {
    if (state.bound) return;
    state.bound = true;
    applySavedControls();
    bindControls();
    renderUniverseManager();
    updateInspectorButtonState(null);
    const pumpRefreshButton = q('btn-pump-watchlist-refresh');
    if (pumpRefreshButton) {
      pumpRefreshButton.addEventListener('click', () => {
        triggerPumpWatchlistRefresh().catch(() => {});
      });
    }
  }

  async function loadAltcoinRadarTabData(force = false) {
    bindAltcoinRadarPage();
    loadPumpWatchlist().catch(() => {});
    const universePromise = loadUniverseOptions(force).catch((error) => {
      console.warn('loadAltcoinRadarTabData universe bootstrap failed', error?.message || error);
    });
    // The scan only needs the research universe. The watchlist endpoint can take
    // several seconds on a cold start, so it fills its panel when it arrives
    // instead of holding the board back.
    loadWatchlist().catch((error) => {
      console.warn('loadAltcoinRadarTabData watchlist bootstrap failed', error?.message || error);
      state.watchlist = normalizeSymbols(state.watchlist || []);
      if (!state.watchlist.length) state.watchlist = DEFAULT_WATCHLIST.slice();
      renderWatchlist();
    });
    await universePromise;
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
