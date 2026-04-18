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
    anomaly: '异动预警',
    accumulation: '吸筹预警',
    control: '高控盘预警',
  };

  const INSPECTOR_ALERT_BUTTONS = [
    ['btn-altcoin-radar-alert-anomaly', 'anomaly'],
    ['btn-altcoin-radar-alert-layout', 'accumulation'],
    ['btn-altcoin-radar-alert-control', 'control'],
  ];

  const CLIENT_FILTER_CONTROL_IDS = ['altcoin-radar-filter', 'altcoin-radar-only-alerted'];

  const state = {
    bound: false,
    universeLoadedFor: '',
    scan: null,
    detail: null,
    selectedSymbol: '',
    filteredRows: [],
    scanSeq: 0,
    detailSeq: 0,
  };

  function q(id) {
    return document.getElementById(id);
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
    };
  }

  function currentUniverseSymbols() {
    const selected = readControls().universeSymbols;
    if (selected.length) return selected;
    const used = normalizeSymbols(state.scan?.scan_meta?.symbols_used || state.scan?.summary?.symbols_used || []);
    return used.length ? used : DEFAULT_UNIVERSE.slice(0, 12);
  }

  function tagTone(tag) {
    const text = String(tag || '').trim();
    if (!text) return 'muted';
    if (text.includes('吸筹')) return 'layout';
    if (text.includes('异动')) return 'anomaly';
    if (text.includes('高控盘')) return 'control';
    if (text.includes('派发') || text.includes('风险')) return 'danger';
    if (text.includes('预警')) return 'control';
    return 'muted';
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

  function requireApi() {
    if (typeof api !== 'function') {
      throw new Error('altcoin radar requires global api() helper');
    }
    return api;
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
    try {
      const resp = await apiFetch(`/data/research/symbols?exchange=${encodeURIComponent(controls.exchange)}`, {
        timeoutMs: 15000,
      });
      const symbols = normalizeSymbols(resp?.symbols || []);
      if (symbols.length) finalSymbols = symbols;
    } catch (error) {
      console.warn('loadUniverseOptions failed', error?.message || error);
      setStatus(`研究币池加载失败，已回退到默认列表：${error.message}`, 'warn');
    }
    selectEl.innerHTML = finalSymbols
      .map((symbol) => `<option value="${escapeHtml(symbol)}">${escapeHtml(symbol)}</option>`)
      .join('');
    const fallbackSelection = currentSelected.length ? currentSelected : finalSymbols.slice(0, Math.min(12, finalSymbols.length));
    setSelectedValues('altcoin-radar-universe', fallbackSelection, finalSymbols[0] || 'BTC/USDT');
    state.universeLoadedFor = cacheKey;
  }

  function buildScanQuery(options, refresh = false) {
    const params = new URLSearchParams();
    params.set('exchange', options.exchange);
    params.set('timeframe', options.timeframe);
    params.set('sort_by', options.sortBy);
    params.set('limit', '30');
    params.set('exclude_retired', options.excludeRetired ? 'true' : 'false');
    params.set('refresh', refresh ? 'true' : 'false');
    if (options.universeSymbols.length) {
      params.set('symbols', options.universeSymbols.join(','));
    }
    return params.toString();
  }

  function buildDetailQuery(symbol, refresh = false) {
    const options = readControls();
    const params = new URLSearchParams();
    params.set('exchange', options.exchange);
    params.set('timeframe', options.timeframe);
    params.set('symbol', symbol);
    params.set('exclude_retired', options.excludeRetired ? 'true' : 'false');
    params.set('refresh', refresh ? 'true' : 'false');
    const universe = currentUniverseSymbols();
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
    const rows = [
      ['状态', cache.hit ? '缓存命中' : '新鲜计算'],
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
        : '默认缓存：1h 120s / 4h 300s / 1d 900s；强制刷新会绕过缓存重新计算。';
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

  function renderRanking(rows) {
    const tbody = q('altcoin-radar-ranking-body');
    if (!tbody) return;
    const filteredRows = applyClientFilters(rows);
    state.filteredRows = filteredRows;
    if (!filteredRows.length) {
      tbody.innerHTML = '<tr><td colspan="10" class="altcoin-radar-empty">当前过滤条件下没有候选，请切换过滤或刷新币池。</td></tr>';
      return;
    }
    tbody.innerHTML = filteredRows
      .map((row) => {
        const symbol = String(row?.symbol || '').trim();
        const selected = symbol && symbol === state.selectedSymbol;
        const defaultPreset = pickDefaultPreset(row);
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
            <td><span class="altcoin-radar-score-badge">${escapeHtml(shortPercent(row?.chain_confirmation_score))}</span></td>
            <td>${freshnessChip(row)}</td>
            <td><div class="altcoin-radar-tag-row">${renderTagRow(row?.tags)}</div></td>
            <td>
              <div class="altcoin-radar-row-actions">
                <button type="button" class="btn btn-primary btn-sm" data-row-action="inspect" data-symbol="${escapeHtml(symbol)}">查看</button>
                <button type="button" class="btn btn-sm" data-row-action="research" data-symbol="${escapeHtml(symbol)}">研究</button>
                <button type="button" class="btn btn-sm" data-row-action="alert" data-symbol="${escapeHtml(symbol)}" data-preset-kind="${escapeHtml(defaultPreset)}"${row?.has_alert_rule ? ' disabled' : ''}>${row?.has_alert_rule ? '已预警' : '预警'}</button>
              </div>
            </td>
          </tr>
        `;
      })
      .join('');
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

  function renderInspector(detailPayload, fallbackError = '') {
    const empty = q('altcoin-radar-inspector-empty');
    const shell = q('altcoin-radar-inspector-shell');
    const selected = detailPayload?.selected_row || findRow(state.selectedSymbol);
    if (!selected) {
      if (empty) empty.textContent = fallbackError || '暂无已选候选';
      if (shell) shell.classList.add('is-hidden');
      return;
    }
    if (empty) empty.textContent = '';
    if (shell) shell.classList.remove('is-hidden');
    q('altcoin-radar-selected-symbol').textContent = selected.symbol || '--';
    q('altcoin-radar-selected-subtitle').textContent = fallbackError
      ? `详情加载失败，已回退到扫描快照：${fallbackError}`
      : `当前聚焦 ${selected.symbol || '--'}。先看它为什么上榜，再决定建预警还是带入研究工坊。`;
    q('altcoin-radar-selected-tags').innerHTML = renderTagRow(selected.tags);
    q('altcoin-radar-selected-scores').innerHTML = [
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

    const dataQuality = selected?.data_quality || {};
    renderListItems('altcoin-radar-data-quality', [
      ['市场新鲜度', toPercent(dataQuality.market_data_freshness, 0)],
      ['快照新鲜度', toPercent(dataQuality.snapshot_freshness, 0)],
      ['链上质量', toPercent(dataQuality.chain_quality, 0)],
      ['降级原因', Array.isArray(dataQuality.degraded_reason) && dataQuality.degraded_reason.length ? dataQuality.degraded_reason.join(', ') : '无'],
    ]);

    const metrics = selected?.metrics || {};
    renderListItems('altcoin-radar-key-metrics', [
      ['1 bar', toPercent(metrics.return_1_bar, 1)],
      ['3 bar', toPercent(metrics.return_3_bar, 1)],
      ['6 bar', toPercent(metrics.return_6_bar, 1)],
      ['量能爆发', shortPercent(metrics.volume_burst_ratio)],
      ['ATR 扩张', shortPercent(metrics.range_expansion_ratio)],
      ['价差 bps', shortPercent(metrics.spread_bps)],
      ['订单流失衡', shortPercent(metrics.order_flow_imbalance)],
      ['巨鲸计数', shortPercent(metrics.whale_count)],
    ]);

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

    updateInspectorButtonState(selected);
  }

  function updateInspectorButtonState(row) {
    const symbol = String(row?.symbol || '').trim();
    const disabled = !symbol;
    const hasAlertRule = !!row?.has_alert_rule;
    const researchBtn = q('btn-altcoin-radar-open-research');
    if (researchBtn) {
      researchBtn.disabled = disabled;
      if (!disabled) researchBtn.dataset.symbol = symbol;
    }
    eachInspectorAlertButton((button) => {
      const defaultLabel = String(button.dataset.defaultLabel || button.textContent || '').trim() || '建预警';
      button.dataset.defaultLabel = defaultLabel;
      button.disabled = disabled || hasAlertRule;
      button.textContent = hasAlertRule ? '已建预警' : defaultLabel;
      button.title = hasAlertRule ? '当前候选已存在山寨雷达预警' : '';
      if (!disabled) button.dataset.symbol = symbol;
    });
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
  }

  function markSymbolAlerted(symbol) {
    const normalized = String(symbol || '').trim().toUpperCase();
    if (!normalized || !Array.isArray(state.scan?.rows)) return;
    const applyAlertState = (row) => {
      const tags = Array.isArray(row?.tags) ? row.tags.slice() : [];
      if (!tags.includes('宸插缓棰勮')) tags.push('宸插缓棰勮');
      return { ...row, has_alert_rule: true, tags };
    };
    state.scan.rows = state.scan.rows.map((row) => {
      if (String(row?.symbol || '').trim().toUpperCase() !== normalized) return row;
      const tags = Array.isArray(row?.tags) ? row.tags.slice() : [];
      if (!tags.includes('已建预警')) tags.push('已建预警');
      return { ...row, has_alert_rule: true, tags };
    });
    if (state.detail?.selected_row && String(state.detail.selected_row.symbol || '').trim().toUpperCase() === normalized) {
      const tags = Array.isArray(state.detail.selected_row.tags) ? state.detail.selected_row.tags.slice() : [];
      if (!tags.includes('已建预警')) tags.push('已建预警');
      state.detail.selected_row = { ...state.detail.selected_row, has_alert_rule: true, tags };
    }
  }

  async function selectSymbol(symbol, refresh = false) {
    const normalized = String(symbol || '').trim().toUpperCase();
    if (!normalized) return;
    state.selectedSymbol = normalized;
    renderRanking(state.scan?.rows || []);
    const selectedRow = findRow(normalized);
    updateInspectorButtonState(selectedRow);
    const seq = ++state.detailSeq;
    renderInspector({ selected_row: selectedRow }, '正在加载详情...');
    try {
      const apiFetch = requireApi();
      const detail = await apiFetch(`/altcoin/radar/detail?${buildDetailQuery(normalized, refresh)}`, {
        timeoutMs: 30000,
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

  async function scanRadar(refresh = false) {
    const controls = readControls();
    const seq = ++state.scanSeq;
    const previousScan = state.scan;
    setStatus(refresh ? '正在强制刷新雷达，保留上次榜单...' : '正在加载山寨雷达榜单...', 'warn');
    try {
      const apiFetch = requireApi();
      const response = await apiFetch(`/altcoin/radar/scan?${buildScanQuery(controls, refresh)}`, {
        timeoutMs: 45000,
      });
      if (seq !== state.scanSeq) return;
      renderScan(response);
      syncUniverseSelection(response?.scan_meta?.symbols_used || response?.summary?.symbols_used || []);
      const cache = response?.scan_meta?.cache || {};
      const leaderSymbol = response?.rows?.[0]?.symbol || '';
      const preferred = findRow(state.selectedSymbol)?.symbol || leaderSymbol;
      setStatus(
        cache.hit
          ? `已加载缓存结果：${controls.exchange} / ${controls.timeframe} · ${response?.summary?.scanned_count || 0} 币`
          : `已完成实时扫描：${controls.exchange} / ${controls.timeframe} · ${response?.summary?.scanned_count || 0} 币`,
        'ok'
      );
      setOutput(JSON.stringify(response, null, 2));
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
    };
    const resp = await apiFetch('/altcoin/alerts/preset', {
      method: 'POST',
      timeoutMs: 30000,
      body: JSON.stringify(payload),
    });
    markSymbolAlerted(selectedSymbol);
    renderScan(state.scan);
    renderInspector(state.detail || { selected_row: findRow(selectedSymbol) });
    return resp;
  }

  async function openResearchWorkbench(symbol) {
    const selectedSymbol = String(symbol || state.selectedSymbol || '').trim().toUpperCase();
    if (!selectedSymbol) {
      throw new Error('请先选择一个候选币种');
    }
    const controls = readControls();
    if (q('research-exchange')) q('research-exchange').value = controls.exchange;
    if (typeof loadResearchSymbolOptions === 'function') {
      await loadResearchSymbolOptions(controls.exchange);
    }
    if (q('research-timeframe')) q('research-timeframe').value = controls.timeframe;
    if (q('research-exclude-retired')) q('research-exclude-retired').checked = controls.excludeRetired;
    if (typeof setSelectValues === 'function') {
      setSelectValues('research-symbol', [selectedSymbol], selectedSymbol);
      setSelectValues('research-symbols', currentUniverseSymbols(), selectedSymbol);
    } else {
      const symbolEl = q('research-symbol');
      if (symbolEl) symbolEl.value = selectedSymbol;
    }
    if (typeof renderResearchStatusCards === 'function') {
      renderResearchStatusCards();
    }
    if (typeof activateTab === 'function') {
      activateTab('research');
    }
    const researchOutput = typeof getResearchOutputEl === 'function' ? getResearchOutputEl() : null;
    if (researchOutput) {
      researchOutput.textContent = `已从山寨雷达带入研究工坊：${selectedSymbol}\nexchange=${controls.exchange}\ntimeframe=${controls.timeframe}\nuniverse=${currentUniverseSymbols().join(', ')}`;
    }
    setOutput(`已将 ${selectedSymbol} 带入研究工坊。下一步建议：在研究工坊先运行“研究总览”，再决定是否继续多币种 / 链上验证。`);
  }

  function bindControls() {
    const refreshBtn = q('btn-altcoin-radar-refresh');
    if (refreshBtn) {
      refreshBtn.onclick = () => scanRadar(false).catch((error) => {
        if (typeof notify === 'function') notify(`山寨雷达刷新失败: ${error.message}`, true);
      });
    }
    const forceBtn = q('btn-altcoin-radar-force-refresh');
    if (forceBtn) {
      forceBtn.onclick = () => scanRadar(true).catch((error) => {
        if (typeof notify === 'function') notify(`山寨雷达强制刷新失败: ${error.message}`, true);
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
        scanRadar(false).catch((error) => {
          if (typeof notify === 'function') notify(`山寨雷达切周期失败: ${error.message}`, true);
        });
      });
    }
    const sortEl = q('altcoin-radar-sort');
    if (sortEl) {
      sortEl.addEventListener('change', () => {
        scanRadar(false).catch((error) => {
          if (typeof notify === 'function') notify(`山寨雷达排序刷新失败: ${error.message}`, true);
        });
      });
    }
    const universeEl = q('altcoin-radar-universe');
    if (universeEl) {
      universeEl.addEventListener('change', () => {
        scanRadar(false).catch((error) => {
          if (typeof notify === 'function') notify(`山寨雷达币池更新失败: ${error.message}`, true);
        });
      });
    }
    CLIENT_FILTER_CONTROL_IDS.forEach((id) => {
      const el = q(id);
      if (el) {
        el.addEventListener('change', () => {
          renderScan(state.scan || { rows: [], summary: {}, scan_meta: {}, warnings: [] });
          if (state.selectedSymbol) renderInspector(state.detail || { selected_row: findRow(state.selectedSymbol) });
        });
      }
    });
    const excludeEl = q('altcoin-radar-exclude-retired');
    if (excludeEl) {
      excludeEl.addEventListener('change', () => {
        scanRadar(false).catch((error) => {
          if (typeof notify === 'function') notify(`山寨雷达退市过滤刷新失败: ${error.message}`, true);
        });
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
    eachInspectorAlertButton((btn, kind) => {
      btn.onclick = () => {
        createPresetAlert(kind, btn.dataset.symbol || state.selectedSymbol)
          .then((resp) => {
            const message = resp?.existing ? '该预警已存在，已为你标记到当前候选。' : `已创建 ${PRESET_BY_KIND[kind]}。`;
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
          } else if (action === 'research') {
            openResearchWorkbench(symbol).then(() => {
              if (typeof notify === 'function') notify(`已将 ${symbol} 带入研究工坊`);
            }).catch((error) => {
              if (typeof notify === 'function') notify(`带入研究工坊失败: ${error.message}`, true);
            });
          } else if (action === 'alert') {
            createPresetAlert(btn.dataset.presetKind || 'anomaly', symbol).then((resp) => {
              const message = resp?.existing ? `${symbol} 的预警已存在` : `已为 ${symbol} 创建预警`;
              setOutput(JSON.stringify(resp, null, 2));
              if (typeof notify === 'function') notify(message);
            }).catch((error) => {
              if (typeof notify === 'function') notify(`创建预警失败: ${error.message}`, true);
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
  }

  function bindAltcoinRadarPage() {
    if (state.bound) return;
    state.bound = true;
    bindControls();
    updateInspectorButtonState(null);
  }

  async function loadAltcoinRadarTabData(force = false) {
    bindAltcoinRadarPage();
    await loadUniverseOptions(force);
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
