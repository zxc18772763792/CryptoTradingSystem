const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const root = path.resolve(__dirname, '..');

test('monthly Coinglass exposes its own universe and keeps missing backtest results empty',()=>{
  const {ctx}=context();
  const html=ctx.window.ResearchTrackers.detail({
    id:'coinglass_composite_7d_monthly_v1',name:'Coinglass · 月度换池 v1',unit:'期',n:0,min_n:364,
    mean_pct:null,backtest_mean_pct:null,
    current_universe:{month:'2026-11',coins:180,added:['NEW'],removed:['OLD'],selected_at:'2026-11-01T00:15:00Z'}
  });
  assert.match(html,/币池 2026-11/);
  assert.match(html,/180 个币/);
  assert.match(html,/新增 1 \/ 移出 1/);
  assert.match(html,/旧持仓沿用入场名单/);
  assert.doesNotMatch(html,/\+0\.00%/);
});
const escapeHtml = v => String(v ?? '').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function context() {
  const elements = {'research-trackers-tbody':{}, 'research-trackers-meta':{}, 'research-tracker-diagnostics':{}};
  const ctx = {window:{}, document:{getElementById:id=>elements[id]}, esc:escapeHtml, fmtDateTime:v=>v||'--'};
  vm.createContext(ctx);
  vm.runInContext(fs.readFileSync(path.join(root,'web/static/js/research_tracker_diagnostics.js'),'utf8'),ctx);
  vm.runInContext(fs.readFileSync(path.join(root,'web/static/js/research_trackers.js'),'utf8'),ctx);
  const app=fs.readFileSync(path.join(root,'web/static/js/app.js'),'utf8');
  vm.runInContext(app.slice(app.indexOf('function renderResearchTrackers(d){'),app.indexOf('async function loadResearchTrackers(){')),ctx);
  return {ctx,elements,render:ctx.window.ResearchTrackerDiagnostics};
}
test('paired increment and absolute shadow return have distinct labels and common dates',()=>{
  const {render}=context();
  const html=render.paired({n_pairs:8,start_date:'2026-10-12',end_date:'2026-10-19',baseline:{mean_pct:.0959},shadow:{mean_pct:.7962},increment:{mean_pp:.7003},sensitivity:{weekly:{n_week_groups:2,status:'insufficient_data',min_days:32,min_week_groups:8}},attribution:{},execution_diagnostics:{}});
  assert.match(html,/影子自身 \+0\.796%/);
  assert.match(html,/增益 \+0\.700 个百分点/);
  assert.match(html,/共同 8 天/);
  assert.match(html,/敏感性样本不足/);
  assert.doesNotMatch(html,/undefined|NaN/);
});
test('table exposes basis and incomplete marks without reporting them as portfolio net or summing samples',()=>{
  const {ctx,elements}=context();
  ctx.renderResearchTrackers({trackers:[{id:'xs_reversal_liq_filter',name:'<script>',metric_kind:'paired_increment',metric_label:'同日配对增益',basis_label:'多空总名义金额',mean_pct:.7,gross_mean_pct:.7,actual_mean_pct:.796,paired_evaluation:{baseline:{mean_pct:.096}},unit:'天',n:8,min_n:180,open_marks:[{label:'APR',mark_pct:37.36,is_partial:true,scope_label:'空头价格毛收益',price_basis:'最近完整日收盘'}]}]});
  const html=elements['research-trackers-tbody'].innerHTML;
  assert.match(html,/同日配对增益/); assert.match(html,/\+0\.70 个百分点/);
  assert.match(html,/影子自身/); assert.match(html,/\+0\.796%/);
  assert.match(html,/组合净浮盈暂无完整数据/);
  assert.match(html,/&lt;script&gt;/);
  assert.doesNotMatch(html,/<script>/);
  assert.match(elements['research-trackers-meta'].title,/样本数不相加/);
  assert.doesNotMatch(elements['research-trackers-meta'].textContent,/累计 =/);
});
test('Coinglass shows all four cells, missing stress returns and entry time confounding',()=>{
  const {render}=context();
  const html=render.composite({status:'insufficient_data',factorial_common:{n_pairs:0},cells:{original_ic:{n_completed:2,stats:{mean_pct:3.316},mean_contributions:{stress_net_pct:3.156}},wide_equal:{n_completed:0,stats:{mean_pct:null}}},timing:{mean_delay_minutes:119.3}});
  assert.match(html,/131币 · IC加权/); assert.match(html,/234币 · 等权/);
  assert.match(html,/\+3\.156%/); assert.match(html,/119\.3 分钟/);
  assert.match(html,/暂不选择胜出版本/); assert.doesNotMatch(html,/undefined|NaN/);
});
test('risk budgets are a prospective trigger, unavailable evidence stays unknown and strings are escaped',()=>{
  const {render}=context();
  const html=render.risk({registered:true,registered_at:'2026-10-20',config:{funding_budget_pct:2,total_loss_budget_pct:10},entry_unavailable:1,positions:[{symbol:'<img>',status:'entry_unavailable',unavailable_reason:'<script>'}]},'<b>');
  assert.match(html,/已有入场交易不回填/); assert.match(html,/可能使实际纸面亏损超过该值/);
  assert.match(html,/&lt;img&gt;/); assert.match(html,/&lt;script&gt;/);
  assert.doesNotMatch(html,/<img>|<script>|undefined|NaN/);
});

test('Alastar exposes receipt review and excess points separately from funded returns',()=>{
  const {ctx,elements}=context();
  ctx.renderResearchTrackers({trackers:[{name:'Alastar',unit:'笔',metric_kind:'excess_return',metric_label:'相对同币零分布超额',mean_pct:.8,actual_mean_pct:1.2,n:1,min_n:66,pending_review:2,completed_without_primary:1,tracker_status_label:'待语义复核 <b>'}]});
  const html=elements['research-trackers-tbody'].innerHTML;
  assert.match(html,/相对同币零分布超额/); assert.match(html,/\+0\.80 个百分点/);
  assert.match(html,/实际含资金费净收益/); assert.match(html,/\+1\.200%\/笔/);
  assert.match(html,/待复核 2/); assert.match(html,/对照待补 1/);
  assert.match(html,/待语义复核 &lt;b&gt;/);
  assert.doesNotMatch(html,/<b>|undefined|NaN/);
});

test('automatic interpretation health and delivery counts do not become completed trades',()=>{
  const {ctx,elements}=context();
  ctx.renderResearchTrackers({trackers:[{name:'Alastar',unit:'笔',n:0,min_n:66,mean_pct:null,signal_intake:{enabled:true,runner_active:true,counts:{label_frozen:18,already_frozen:2,non_signal:30,retry_pending:1}}}]});
  const html=elements['research-trackers-tbody'].innerHTML;
  assert.match(html,/自动语义解析运行中/);
  assert.match(html,/已冻结解释 20 · 非信号 30 · 待重试 1/);
  assert.match(html,/0 \/ 66 笔/);
  assert.doesNotMatch(html,/undefined|NaN/);
});

test('Alastar empty monitor explains prior receipts and actual enable time without inventing trades',()=>{
  const {ctx,elements}=context();
  const render=counts=>ctx.renderResearchTrackers({trackers:[{name:'Alastar',unit:'笔',n:0,min_n:66,mean_pct:null,signal_intake:{enabled:true,runner_active:true,enabled_at_utc:'2026-10-23T11:55:13Z',counts}}]});
  render({predates_auto_intake:2});
  let html=elements['research-trackers-tbody'].innerHTML;
  assert.match(html,/启用前帖子 2（未回填）/);
  assert.match(html,/启用于 2026-10-23T11:55:13Z/);
  assert.match(html,/启用后尚无新帖/);
  assert.match(html,/0 \/ 66 笔/);
  assert.doesNotMatch(html,/undefined|NaN/);
  render({predates_auto_intake:2,processing:1});
  html=elements['research-trackers-tbody'].innerHTML;
  assert.doesNotMatch(html,/启用后尚无新帖/);
});

test('Alastar multi horizon displays common maturity and missing results without picking a winner',()=>{
  const {ctx,elements}=context();
  ctx.renderResearchTrackers({trackers:[{name:'Alastar v3',unit:'组',n:0,min_n:66,metric_kind:'horizon_comparison',horizon_comparison:[{hours:1,completed_n:1,common_n:0,mean_net_pct:null,excess_pp:null,gain_vs_1h_pp:null},{hours:168,completed_n:0,common_n:0,mean_net_pct:null,excess_pp:null,gain_vs_1h_pp:null}]}]});
  const html=elements['research-trackers-tbody'].innerHTML;
  assert.match(html,/168h/); assert.match(html,/1 \/ 0/);
  assert.match(html,/共同样本/); assert.match(html,/不作周期优胜判定/);
  assert.doesNotMatch(html,/NaN|undefined|\+0\.00%/);
});

test('filters and search keep tracker units and never merge sources or results',()=>{
  const {ctx,elements}=context();
  const data={trackers:[{id:'a',name:'Alastar',group:'Telegram信号',n:0,unit:'笔'},{id:'b',name:'Upbit 警示',group:'韩国交易所',n:3,unit:'笔'},{id:'c',name:'横截面反转',group:'因子',n:26,unit:'天'}]};
  assert.deepEqual(Array.from(ctx.window.ResearchTrackers.selectedRows(data,'telegram'),r=>r.id),['a']);
  assert.deepEqual(Array.from(ctx.window.ResearchTrackers.selectedRows(data,'all','UPBIT'),r=>r.id),['b']);
  assert.deepEqual(Array.from(ctx.window.ResearchTrackers.selectedRows(data,'factors'),r=>r.unit),['天']);
  ctx.renderResearchTrackers(data);
  assert.match(elements['research-trackers-tbody'].innerHTML,/0 \/ -- 笔/);
});

test('collection faults remain visible while missing returns stay unknown',()=>{
  const {ctx,elements}=context();
  const row={id:'upbit_caution',name:'Upbit',n:0,min_n:20,mean_pct:null,open:2,collection_health:{status:'degraded'},tracker_status_label:'运行中'};
  assert.equal(ctx.window.ResearchTrackers.status(row)[1],'warn');
  ctx.renderResearchTrackers({trackers:[row]});
  const html=elements['research-trackers-tbody'].innerHTML;
  assert.match(html,/数据延迟/);assert.match(html,/rt-detail-row[^>]+ hidden/);
  assert.doesNotMatch(html,/\+0\.00%|NaN|undefined/);
});

test('Telegram gaps and inaccessible sources remain visible in the compact status',()=>{
  const {ctx}=context();
  const status=ctx.window.ResearchTrackers.status;
  assert.equal(status({source_health:{status:'coverage_gap'}})[0],'采集有缺口');
  assert.equal(status({source_health:{status:'inaccessible'}})[1],'bad');
  assert.equal(status({source_health:{status:'coverage_unverified'}})[1],'warn');
});

test('Binance sentiment distinguishes empty, insufficient and missing windows and exposes four books',()=>{
  const {ctx,elements}=context();
  const windows=[{hours:1,status:'no_directional_views',label:'无有效 BTC 观点',authors:0,score:null},
    {hours:6,status:'insufficient_authors',label:'独立作者不足',authors:3,score:-33.33}];
  const books=['chat_follow','chat_contrarian','chat_6h_follow','chat_6h_contrarian'].map(book=>({book,label:book,decisions:0,mature_settled:0,mean_net_pct:null}));
  ctx.renderResearchTrackers({trackers:[{id:'telegram_binance_chat_sentiment',group:'Telegram信号',name:'币安中文群',n:0,min_n:100,
    chat_sentiment:{windows,min_authors:5,min_abs_score:40,history:[],diagnostics:[]},ledgers:books}]});
  const html=elements['research-trackers-tbody'].innerHTML;
  assert.match(html,/BTC 群情绪观察/);assert.match(html,/独立作者 0 \/ 5 人/);assert.match(html,/独立作者 3 \/ 5 人/);
  assert.match(html,/作者不足，分数仅供观察/);assert.match(html,/分数不是涨跌概率/);
  assert.match(html,/四本纸面账本/);assert.match(html,/主账本/);assert.match(html,/40bp压力/);
  assert.match(html,/样本数不相加/);assert.doesNotMatch(html,/\+0\.00%|NaN|undefined/);
  const detail=ctx.window.ResearchTrackers.detail({chat_sentiment:{min_authors:5,min_abs_score:40,
    windows:[{hours:6,status:'coverage_gap',label:'窗口覆盖不完整 <script>',authors:null,score:null,next_complete_at:'2026-10-31T06:00:00Z'}]}});
  assert.match(detail,/独立作者 -- \/ 5 人/);assert.match(detail,/14:00/);
  assert.match(detail,/&lt;script&gt;/);assert.doesNotMatch(detail,/<script>|独立作者 0 \/|\+0\.00%/);
});
