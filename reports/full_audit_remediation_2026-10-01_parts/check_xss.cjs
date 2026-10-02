// Offline DOM sink reproduction: no browser, no service, no network.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.resolve(__dirname, '../../web/static/js/app.js'), 'utf8');
const start = source.indexOf('function renderStrategySummary(d){');
const next = source.indexOf('\nfunction ', start + 1);
if (start < 0 || next < 0) throw new Error('Cannot locate audited render function');
const sink = {innerHTML: ''};
const context = {
  state: {strategies: []},
  document: {getElementById: id => id === 'active-strategies' ? sink : null},
  fmtDurationSec: () => '0s', fmtTime: () => '', fmtDateTime: () => '', fmt: String,
  esc: value => String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;'),
  renderStrategyHealthAlerts: () => {}, renderStrategyConsolePanel: () => {},
};
vm.createContext(context);
vm.runInContext(source.slice(start, next), context);
const payload = '<img src=x onerror="globalThis.auditOnlyMarker=1">';
context.renderStrategySummary({running: [{name: payload, strategy_type: 'AuditType'}]});
const result = {payload, stored_name_reaches_innerHTML_unescaped: sink.innerHTML.includes(payload), html: sink.innerHTML};
fs.writeFileSync(path.join(__dirname, 'xss_fixed.json'), JSON.stringify(result, null, 2), 'utf8');
process.stdout.write(JSON.stringify(result, null, 2) + '\n');
