/* Run with Node and playwright on NODE_PATH. All API calls use local fixtures;
   this test never connects to the trading service or submits real orders. */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { chromium } = require('playwright');
const root = path.resolve(__dirname, '..');
const source = fs.readFileSync(path.join(root, 'web/templates/index.html'), 'utf8');
const html = source
  .replace(/<script\b[^>]*>[\s\S]*?<\/script>/g, '')
  .replace(/\{\{ static_asset_url\('([^']+)'\) \}\}/g, '/static/$1?v=test')
  .replace(/class="tab-btn active"/g, 'class="tab-btn"')
  .replace('tab-content agent-workspace', 'tab-content active agent-workspace')
  .replace('tab-content active"', 'tab-content"')
  .replace('</body>', '<script src="/static/js/ai_research_agent.js"></script></body>');

(async () => {
  const browser = await chromium.launch({ channel: 'chrome', headless: true });
  try {
    const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    await page.route('**/*', async route => {
      const url = new URL(route.request().url());
      if (url.hostname !== 'agent.test') return route.abort();
      if (url.pathname === '/') return route.fulfill({ contentType: 'text/html', body: html });
      if (url.pathname.startsWith('/static/')) {
        const asset = path.join(root, 'web', url.pathname);
        if (fs.existsSync(asset)) return route.fulfill({ path: asset, contentType: asset.endsWith('.css') ? 'text/css' : asset.endsWith('.js') ? 'application/javascript' : 'image/svg+xml' });
      }
      return route.abort();
    });
    await page.addInitScript(() => {
      window.AI = { modules: {} };
      window.fixture = {
        running: false, failStatus: false, calls: [],
        config: { enabled: true, symbol: 'BTC/USDT', symbol_mode: 'manual', mode: 'execute', trading_mode: 'paper', allow_live: false, interval_sec: 120, provider: 'test', model: 'fixture-model', max_total_exposure_ratio: 0.2 },
        risk: { autonomy_daily_stop_buffer_ratio: 0.01, autonomy_max_drawdown_reduce_only: 0.03 },
      };
      window.api = async (url, options = {}) => {
        const f = window.fixture;
        const endpoint = url.split('?')[0];
        f.calls.push({ endpoint, method: options.method || 'GET', body: options.body && JSON.parse(options.body) });
        if (endpoint.endsWith('/start')) { f.running = true; return {}; }
        if (endpoint.endsWith('/stop')) { f.running = false; return {}; }
        if (endpoint.endsWith('/run-once')) return { accepted: true };
        if (endpoint.endsWith('/status')) {
          if (f.failStatus) throw new Error('测试连接中断');
          return { config: f.config, status: { running: f.running, tick_count: 8, last_run_at: '2026-09-10T08:00:00Z', last_decision: { action: 'hold', confidence: 0.64 }, last_diagnostics: { primary: { label: '信号尚未达到开仓阈值', detail: '等待下一轮信号确认。', tone: 'warn' }, aggregated_signal: { direction: 'LONG', confidence: 0.64 } } } };
        }
        if (endpoint.endsWith('/operating-mode')) return { degradations: [
          { code: 'source_news_gdelt', source_label: 'GDELT', severity: 'warn', detail: 'HTTP 429 Too Many Requests', configured: true, action_required: true },
          { code: 'source_news_optional', source_label: '可选新闻源', severity: 'info', configured: false, action_required: false },
        ] };
        if (endpoint.endsWith('/risk-config')) {
          if (options.method === 'POST') Object.assign(f.risk, JSON.parse(options.body));
          return { config: f.risk };
        }
        if (endpoint.endsWith('/runtime-config/autonomous-agent')) {
          Object.assign(f.config, JSON.parse(options.body)); return { config: f.config };
        }
        if (endpoint.endsWith('/risk-status')) return { effective_fresh_entry_allowed: false, fresh_entry_submission_allowed: false, close_only_effective: true, risk: { risk_level: 'high', max_drawdown: 0.04, discipline: { reduce_only: true } }, fresh_entry_blockers: [{ source: 'risk_discipline', code: 'reduce_only', detail: '达到回撤限制，暂停新开仓。' }], execution_gate: { blocked: false, agent_mode: 'execute', trading_mode: 'paper' } };
        if (endpoint.endsWith('/scorecard')) return { metrics: { trades: 4, closes: 2, net_pnl_usd: 12, cost_drag_usd: 1, win_rate: 0.5 }, window: { hours: 168 } };
        if (endpoint.endsWith('/journal')) return { items: [{ timestamp: '2026-09-10T08:00:00Z', config: { symbol: 'BTC/USDT' }, decision: { action: 'hold', reason: '等待确认 <script>bad()</script>' } }] };
        if (endpoint.endsWith('/review')) return { summary: { submitted_count: 1, entry_count: 1, close_count: 0 }, items: [{ phase: 'entry', symbol: 'BTC/USDT', timestamp: '2026-09-10T08:00:00Z', summary_lines: ['开仓后等待平仓'], primary: { label: '信号确认' } }] };
        return {};
      };
    });
    await page.goto('http://agent.test/');
    await page.waitForFunction(() => document.getElementById('ai-agent-start-btn').disabled === false);
    assert.equal(await page.locator('.agent-metrics').evaluate(el => getComputedStyle(el).display), 'grid');
    assert.equal(await page.locator('.tab-content:visible').count(), 1);
    assert.equal(await page.locator('.agent-view:visible').count(), 1);
    assert.match(await page.locator('#ai-agent-operating-mode-banner summary').innerText(), /1 项异常 · 1 项可选来源未配置/);
    await page.locator('#ai-agent-operating-mode-banner summary').click();
    assert.match(await page.locator('#ai-agent-operating-mode-banner').innerText(), /接口限流，按退避时间自动重试/);
    assert.equal(await page.locator('#ai-agent-operating-mode-banner li').count(), 1);
    await page.locator('#ai-agent-operating-mode-banner summary').click();
    assert.match(await page.locator('#ai-agent-risk-status-panel').innerText(), /达到回撤限制/);
    assert.equal(await page.locator('#ai-agent-stop-btn').isDisabled(), true);
    assert.equal(await page.locator('#ai-agent-cockpit-cycle').innerText(), '等待启动');
    const duplicateIds = await page.locator('#ai-agent [id]').evaluateAll(elements => elements.map(e => e.id).filter((id, i, ids) => ids.indexOf(id) !== i));
    assert.deepEqual(duplicateIds, []);

    // Keyboard navigation, isolated panels and safely rendered journal text.
    await page.locator('#agent-tab-overview').focus();
    await page.keyboard.press('ArrowRight');
    assert.equal(await page.locator('#agent-tab-journal').getAttribute('aria-selected'), 'true');
    assert.match(await page.locator('#ai-agent-journal').innerText(), /<script>bad\(\)<\/script>/);
    assert.equal(await page.locator('#ai-agent-journal script').count(), 0);
    await page.keyboard.press('End');
    assert.equal(await page.locator('#agent-view-settings').isVisible(), true);

    // Polls may update the status while the user is editing, but must not reset drafts.
    await page.locator('#ai-agent-manual-symbol').fill('ETH/USDT');
    await page.locator('#ai-agent-risk-daily-stop').fill('0.025');
    await page.evaluate(() => window.AI.modules.agent.refresh({ includeDetails: false }));
    await page.evaluate(() => window.agentRefreshRisk());
    assert.equal(await page.locator('#ai-agent-manual-symbol').inputValue(), 'ETH/USDT');
    assert.equal(await page.locator('#ai-agent-risk-daily-stop').inputValue(), '0.025');
    await page.getByRole('button', { name: '保存 AI 配置', exact: true }).click();
    await page.waitForFunction(() => window.fixture.config.symbol === 'ETH/USDT');
    await page.getByRole('button', { name: '保存风险纪律', exact: true }).click();
    await page.waitForFunction(() => window.fixture.risk.autonomy_daily_stop_buffer_ratio === 0.025);
    await page.locator('#ai-agent-symbol-mode').selectOption('auto');
    assert.equal(await page.locator('#ai-agent-universe-symbols').isVisible(), true);
    assert.equal(await page.locator('#ai-agent-manual-symbol').isVisible(), false);

    // Existing control endpoints and their loading/disabled states still work.
    await page.locator('#ai-agent-start-btn').click();
    await page.waitForFunction(() => !document.getElementById('ai-agent-stop-btn').disabled);
    assert.equal(await page.locator('#ai-agent-start-btn').isDisabled(), true);
    await page.locator('#ai-agent-run-once-btn').click();
    await page.waitForFunction(() => !document.getElementById('ai-agent-run-once-btn').disabled);
    assert.equal(await page.evaluate(() => window.fixture.calls.some(c => c.endpoint.endsWith('/run-once') && c.body.force === true)), true);
    await page.locator('#ai-agent-stop-btn').click();
    await page.waitForFunction(() => document.getElementById('ai-agent-stop-btn').disabled && !window.fixture.running);

    // Every view must fit desktop, tablet and mobile widths without a nested page scrollbar.
    for (const width of [1920, 1440, 768, 390]) {
      await page.setViewportSize({ width, height: 1000 });
      for (const view of ['overview', 'journal', 'review', 'settings']) {
        await page.locator(`#agent-tab-${view}`).click();
        const geometry = await page.locator('#ai-agent').evaluate(el => ({ client: el.clientWidth, scroll: el.scrollWidth }));
        assert.ok(geometry.scroll <= geometry.client + 1, `${width}px ${view} overflows: ${JSON.stringify(geometry)}`);
        assert.equal(await page.locator('.agent-view:visible').count(), 1);
        if (process.env.AGENT_UI_SCREENSHOTS && [1440, 390].includes(width)) {
          const folder = path.resolve(process.env.AGENT_UI_SCREENSHOTS);
          fs.mkdirSync(folder, { recursive: true });
          await page.screenshot({ path: path.join(folder, `agent-${view}-${width}.png`), fullPage: true });
        }
      }
    }
    await page.locator('#agent-tab-overview').click();
    await page.locator('#ai-agent-reasons > details > summary').click();
    await page.evaluate(() => window.AI.modules.agent.refresh({ includeDetails: false }));
    assert.equal(await page.locator('#ai-agent-reasons > details').getAttribute('open'), '');
    await page.evaluate(async () => { window.fixture.failStatus = true; await window.AI.modules.agent.refresh({ includeDetails: true }); });
    assert.match(await page.locator('#ai-agent-cockpit-state-badge').innerText(), /加载失败/);
    assert.equal(await page.locator('#ai-agent-start-btn').isDisabled(), true);
    assert.deepEqual(errors, []);
    console.log('PASS: tabs, keyboard, risk visibility, escaped history, draft preservation, saves, controls, error state, and 4 viewport sizes.');
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
