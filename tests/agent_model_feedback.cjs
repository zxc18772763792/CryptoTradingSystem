const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const source = fs.readFileSync(path.join(__dirname, '../web/static/js/ai_research_agent.js'), 'utf8');
const start = source.indexOf('  function describeModelFeedback(');
const end = source.indexOf('  function describeModelOutput(', start);
const context = vm.createContext({ fmtAgentTs: value => value || '--', compactText: text => text, formatDurationMinutes: String });
vm.runInContext(source.slice(start, end), context);
const describe = context.describeModelFeedback;
const status = {
  model_feedback_guard: { last_success_at: '2026-09-11T14:57:00+08:00' },
  learning_memory: { adaptive_risk: { avoid_new_entries_during_service_instability: true }, summary: { recent_model_success_streak: 1 } },
};
assert.equal(describe(status).tone, 'warn');
assert.match(describe(status).detail, /1\/3/);
assert.match(describe(status, { model_feedback: { kind: 'quota_exhausted', label: '供应商额度不足', http_status: 403 } }).summary, /额度不足.*403/);
status.learning_memory.adaptive_risk.avoid_new_entries_during_service_instability = false;
assert.equal(describe(status).summary, '上次模型请求成功');
assert.equal(describe({}).tone, 'info');
console.log('PASS: current failure, historical guard, recovery progress, and timestamped success.');
