const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync('web/static/js/app.js', 'utf8');
const code = source.slice(source.indexOf('function renderRisk(r){'), source.indexOf('function hasRenderableBalanceSnapshot('));

function render(snapshot, open = false) {
  const panel = {innerHTML: '', querySelector: () => ({open})};
  const reset = {style: {}};
  const context = vm.createContext({
    document: {getElementById: id => id === 'risk-panel' ? panel : reset},
    esc: value => String(value ?? '').replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;'),
  });
  vm.runInContext(code, context);
  context.renderRisk(snapshot);
  return {html: panel.innerHTML, reset};
}

test('risk summary keeps alerts and halt state visible, with distinct total and stop basis', () => {
  const {html, reset} = render({risk_level: 'high', trading_halted: true, equity: {
    daily_total_pnl_usd: -12, daily_total_pnl_ratio: -.012,
    daily_stop_basis_usd: -8, daily_stop_basis_ratio: -.008,
  }, alerts: [{title: '<img onerror=alert(1)>', timestamp: '2026-10-23T12:00:00'}]});
  const [summary, details] = html.split('<details');
  assert.match(summary, /已触发/);
  assert.match(summary, /-12\.00 USDT \(-1\.20%\)/);
  assert.match(summary, /&lt;img onerror=alert\(1\)&gt;/);
  assert.doesNotMatch(summary, /<img/);
  assert.match(details, /熔断依据/);
  assert.match(details, /-8\.00 USDT \(-0\.80%\)/);
  assert.equal(reset.style.display, '');
});

test('refresh preserves an expanded risk breakdown and hides irrelevant reset control', () => {
  const {html, reset} = render({risk_level: 'low', trading_halted: false}, true);
  assert.match(html, /data-ui-disclosure="true" open/);
  assert.equal(reset.style.display, 'none');
  assert.match(html, /未触发/);
});
