/* Presentation only: retain existing controls, IDs and event listeners. */
(() => {
  'use strict';

  function disclosure(nodes, label, {card = false} = {}) {
    const items = nodes.filter(Boolean);
    if (!items.length || items[0].parentElement?.closest('[data-ui-disclosure]')) return null;
    const drawer = document.createElement('details');
    drawer.className = `ui-disclosure${card ? ' ui-disclosure-card' : ''}`;
    drawer.dataset.uiDisclosure = 'true';
    const summary = document.createElement('summary');
    summary.textContent = label;
    const body = document.createElement('div');
    body.className = 'ui-disclosure-body';
    items[0].before(drawer);
    drawer.append(summary, body);
    items.forEach(node => body.append(node));
    return drawer;
  }

  function foldCard(id, label) {
    const card = document.getElementById(id)?.closest('.card');
    if (!card || card.dataset.uiFolded) return;
    // Only plain headings may be replaced; headings with controls stay intact.
    const heading = card.querySelector('h3');
    if (!heading || heading.id || heading.children.length) return;
    heading.remove();
    const drawer = disclosure(Array.from(card.childNodes), label, {card: true});
    if (drawer) card.dataset.uiFolded = 'true';
  }

  const foldById = [
    ['mode-output', '操作回执'],
    ['news-action-output', '操作回执'],
    ['news-source-stats', '来源明细'],
    ['news-archive-note', '历史覆盖'],
    ['news-llm-queue-note', '队列明细'],
    ['news-bucket-list', '统计明细'],
    ['ai-agent-info', '模型与运行配置'],
    ['ai-iteration-history', '迭代记录'],
    ['ai-iteration-evidence', '观察证据'],
  ];

  function prepareStaticContent() {
    [
      ['audit-log-list', '操作审计'],
      ['account-id', '账户管理'],
      ['replay-start-time', '历史回放'],
      ['arbitrage-payload-preview', '运行参数'],
      ['arbitrage-integration-notes', '接入说明'],
    ].forEach(([id, label]) => foldCard(id, label));
    foldById.forEach(([id, label]) => disclosure([document.getElementById(id)], label));
    const notificationStart = document.getElementById('notify-test-msg')?.closest('.form-group');
    const notificationEnd = document.getElementById('notify-output');
    if (notificationStart && notificationEnd) {
      const settings = [];
      for (let node = notificationStart; node; node = node.nextSibling) {
        settings.push(node);
        if (node === notificationEnd) break;
      }
      disclosure(settings, '通知设置与测试');
    }
    disclosure(['news-start-engine-btn', 'news-pull-btn', 'news-archive-backfill-btn', 'news-backfill-btn', 'news-requeue-btn']
      .map(id => document.getElementById(id)), '采集与补拉');
    const exportSection = document.getElementById('btn-strategy-export-all')?.closest('.strategy-console-section');
    const importField = document.getElementById('strategy-import-json')?.closest('.form-group');
    if (exportSection && importField) {
      exportSection.querySelector('.section-title')?.remove();
      disclosure([exportSection, importField], '导入、导出与评分');
    }
    const healthDrawer = disclosure([document.getElementById('strategy-health-output')], '健康检查明细');
    if (healthDrawer) ['btn-strategy-health-check', 'btn-strategy-export-all', 'btn-strategy-import-json', 'btn-strategy-ranking']
      .forEach(id => document.getElementById(id)?.addEventListener('click', () => { healthDrawer.open = true; }));
    [
      ['mode-output', ['btn-mode-request', 'btn-mode-confirm']],
      ['news-action-output', ['news-start-engine-btn', 'news-pull-btn', 'news-archive-backfill-btn', 'news-backfill-btn', 'news-requeue-btn']],
    ].forEach(([output, buttons]) => buttons.forEach(id => document.getElementById(id)?.addEventListener('click', () => {
      const drawer = document.getElementById(output)?.closest('details');
      if (drawer) drawer.open = true;
    })));
    [
      ['.ai-research-use-guide', '使用指南'],
    ].forEach(([selector, label]) => disclosure([document.querySelector(selector)], label));
    // Keep maintenance controls together, away from the primary price workflow.
    const health = document.querySelector('.data-health-card');
    if (health && !health.dataset.uiFolded) {
      const heading = health.querySelector('h3');
      if (heading) {
        heading.remove();
        disclosure(Array.from(health.childNodes), '数据检查与维护', {card: true});
        health.dataset.uiFolded = 'true';
      }
    }
  }

  function prepareOrderFields() {
    const mode = document.getElementById('order-mode');
    if (!mode) return;
    const iceberg = document.getElementById('order-iceberg-parts')?.closest('.form-group');
    const slices = document.getElementById('order-algo-slices')?.parentElement?.closest('.form-group');
    const row = iceberg?.parentElement;
    const update = () => {
      if (iceberg) iceberg.style.display = mode.value === 'iceberg' ? '' : 'none';
      if (slices) slices.style.display = ['twap', 'vwap'].includes(mode.value) ? '' : 'none';
      if (row) row.style.display = ['iceberg', 'twap', 'vwap'].includes(mode.value) ? '' : 'none';
    };
    mode.addEventListener('change', update);
    update();
    const trailingRow = document.getElementById('order-trailing-pct')?.closest('.form-row');
    const trailing = disclosure([trailingRow], '追踪止损（可选）');
    // Existing configured protection must remain visible when rendering a form.
    if (trailing && ['order-trailing-pct', 'order-trailing-dist'].some(id => document.getElementById(id)?.value)) trailing.open = true;
  }

  function prepareResearchLedgers() {
    const root = document.getElementById('ai-research');
    const workspace = root?.querySelector('.ai-workspace-stack');
    if (!workspace) return;
    const definitions = [
      ['ui-factor-ledgers', '因子研究循环', '#ai-research-loop-v2, #ai-research-loop-v3'],
      ['ui-paper-ledgers', '纸面观察账本', '[id^="ai-"][id$="-tracker"]'],
    ];
    definitions.forEach(([id, label, selector]) => {
      const panels = Array.from(root.querySelectorAll(selector));
      if (!panels.length) return;
      let drawer = document.getElementById(id);
      if (!drawer) {
        drawer = document.createElement('details');
        drawer.id = id;
        drawer.dataset.uiDisclosure = 'true';
        drawer.className = 'ui-disclosure ui-ledger-group';
        const summary = document.createElement('summary');
        const title = document.createElement('span');
        title.textContent = label;
        const count = document.createElement('span');
        count.className = 'ui-disclosure-count';
        summary.append(title, count);
        const body = document.createElement('div');
        body.className = 'ui-disclosure-body';
        drawer.append(summary, body);
        root.append(drawer);
      }
      const body = drawer.lastElementChild;
      // Move actual nodes, preserving delegated controls and refresh anchors.
      panels.forEach(panel => { if (panel.parentElement !== body) body.append(panel); });
      const count = drawer.querySelector('.ui-disclosure-count');
      const enabled = panels.filter(panel => panel.querySelector('[data-v2-action="toggle"], [data-v3-action="toggle"]')?.textContent.trim().startsWith('暂停')).length;
      const errors = panels.filter(panel => /最近失败：|加载失败：|读取失败：/.test(panel.textContent)).length;
      const text = `${panels.length} 项${enabled ? ` · ${enabled} 项已启用` : ''}${errors ? ` · ${errors} 项异常` : ''}`;
      if (count.textContent !== text) count.textContent = text;
      count.classList.toggle('ui-disclosure-warning', errors > 0);
    });
  }

  function init() {
    prepareStaticContent();
    prepareOrderFields();
    prepareResearchLedgers();
    const root = document.getElementById('ai-research');
    if (root) {
      let scheduled = false;
      const observer = new MutationObserver(records => {
        const selector = '#ai-research-loop-v2, #ai-research-loop-v3, [id^="ai-"][id$="-tracker"]';
        if (scheduled || !records.some(record => record.target.closest?.(selector) || Array.from(record.addedNodes).some(node =>
          node.nodeType === 1 && node.matches?.(selector)))) return;
        scheduled = true;
        requestAnimationFrame(() => { scheduled = false; prepareResearchLedgers(); });
      });
      observer.observe(root, {childList: true, subtree: true});
    }
    // Resize existing charts when a disclosure exposes one; never refetch data.
    document.addEventListener('toggle', event => {
      if (event.target.matches?.('details[data-ui-disclosure]') && event.target.open) {
        requestAnimationFrame(() => event.target.querySelectorAll('.js-plotly-plot').forEach(chart => {
          window.Plotly?.Plots?.resize(chart);
        }));
      }
    }, true);
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init, {once: true});
  else init();
})();
