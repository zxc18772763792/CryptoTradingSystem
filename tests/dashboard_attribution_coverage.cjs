const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const src = fs.readFileSync('web/static/js/app.js', 'utf8');
const slice = (a, b) => src.slice(src.indexOf(a), src.indexOf(b, src.indexOf(a)));
const elements = Object.fromEntries(['data-board-history', 'data-board-history-note', 'equity-attribution'].map(k => [k, {}]));
const context = vm.createContext({
  document: { getElementById: id => elements[id] },
  marketDataState: { loadSeq: 2 },
  fmtDateTime: value => value,
  esc: value => String(value).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;'),
  runRequestSingleFlight: (_key, run) => run(),
});
vm.runInContext(slice('async function loadDataHistoryCoverage(', 'async function loadDataSymbolOptions('), context);
vm.runInContext(slice('async function loadEquityAttribution(', 'async function loadBalances('), context);
(async () => {
  context.api = async () => ({ has_data: true, start: '2023-09-02T16:00:00', end: '2026-09-11T02:00:00+00:00', count: 26506, days: 1104 });
  await context.loadDataHistoryCoverage({ exchange: 'binance', symbol: 'BTC/USDT', timeframe: '1h', loadSeq: 2 });
  assert.match(elements['data-board-history'].textContent, /2023-09-02/);
  assert.match(elements['data-board-history'].textContent, /2023-09-02T16:00:00Z/);
  assert.match(elements['data-board-history'].textContent, /2026-09-11T02:00:00\+00:00$/);
  let release;
  context.api = () => new Promise(resolve => { release = resolve; });
  const stale = context.loadDataHistoryCoverage({ loadSeq: 2 });
  context.marketDataState.loadSeq = 3;
  elements['data-board-history'].textContent = 'new symbol';
  release({ has_data: true, start: 'old symbol' });
  await stale;
  assert.equal(elements['data-board-history'].textContent, 'new symbol');
  context.api = async () => ({ mode: 'paper', initial_equity: 10000, attributed_pnl: 5, carry_forward_difference: 12,
    strategies: [{ strategy: '<script>bad()</script>', realized_pnl: 8, unrealized_pnl: -2, fees: 1, net_pnl: 5, closed_count: 1, open_count: 1 }], scope_note: 'retained ledger' });
  await context.loadEquityAttribution('paper');
  assert.match(elements['equity-attribution'].innerHTML, /\+12\.0000 USDT/);
  assert.match(elements['equity-attribution'].innerHTML, /&lt;script&gt;/);
  assert.doesNotMatch(elements['equity-attribution'].innerHTML, /<script>/);
  context.api = async () => { throw new Error('offline'); };
  await context.loadEquityAttribution('paper');
  assert.match(elements['equity-attribution'].textContent, /offline/);
  console.log('PASS: full-history coverage, stale-response isolation, contribution rendering, escaping, and error state');
})().catch(err => { console.error(err); process.exitCode = 1; });
