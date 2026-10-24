const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync('web/static/js/ai_research_agent.js', 'utf8');
const code = source.slice(source.indexOf('  function scheduleAgentDetails('), source.indexOf('  function shouldAutoRefreshAgentRanking('));

test('leaving agent tab during a status refresh returns a promise without fetching hidden details', async () => {
  const context = vm.createContext({isAgentTabActive: () => false});
  vm.runInContext(code, context);
  const result = context.scheduleAgentDetails({force: true});
  assert.equal(typeof result.catch, 'function');
  assert.equal(await result, null);
});
