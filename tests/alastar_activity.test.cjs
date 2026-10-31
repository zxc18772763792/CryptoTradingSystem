const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const path=require('node:path');
function setup(){
  const ctx={window:{fmtDateTime:v=>v}};
  vm.createContext(ctx);
  vm.runInContext(fs.readFileSync(path.join(__dirname,'../web/static/js/alastar_activity.js'),'utf8'),ctx);
  const entries=[];
  const panel={hidden:true,html:'',querySelectorAll(selector){return selector.endsWith('[open]')?entries.filter(e=>e.open):entries;},
    set innerHTML(v){this.html=v;entries.length=0;for(const m of v.matchAll(/data-alastar-detail="([^"]+)"/g))entries.push({open:false,getAttribute:()=>m[1]});},
    get innerHTML(){return this.html;}};
  return {render:ctx.window.AlastarActivity.render,panel,entries};
}
const item={source_id:'telegram:alastartrader1:2052',url:'javascript:bad()',status:'reviewed',classification:'conditional',provenance:'historical_review',published_at_utc:'2026-10-28T12:51:25Z',first_received_at_utc:'2026-10-28T14:33:29Z',summary:'<script>unsafe</script>',plans:[{asset:'ONDO',direction:'long',thesis_quote:'hold <support>',condition_quote:'if breakout',risk_quote:''}]};
const inbox={health:{status:'healthy'},total_posts:28,strategy_posts:10,prospective_receipts:3,historical_review:25,media_review:3,classification_counts:{conditional:6},sources:{alastartrader1:{healthy:true}},items:[item]};
test('activity is visible outside the zero-trade table and links only to source identity',()=>{
  const {render,panel}=setup();render(panel,inbox,[{id:'alastar_horizons_v3',n:0,open:0}]);
  assert.equal(panel.hidden,false);
  assert.match(panel.innerHTML,/策略观察/);
  assert.match(panel.innerHTML,/策略 <b>10<\/b>/);
  assert.match(panel.innerHTML,/收件 <b>28<\/b>/);
  assert.match(panel.innerHTML,/启用后新收件 3 · 历史补读 25/);
  assert.match(panel.innerHTML,/多周期v3：完成 0，在持 0/);
  assert.match(panel.innerHTML,/https:\/\/t.me\/alastartrader1\/2052/);
  assert.match(panel.innerHTML,/&lt;script&gt;unsafe/);
  assert.doesNotMatch(panel.innerHTML,/javascript:|<script>|undefined|NaN/);
});
test('periodic refresh preserves opened receipt and evidence disclosures',()=>{
  const {render,panel,entries}=setup();render(panel,inbox);
  entries.find(e=>e.getAttribute()==='all').open=true;
  entries.find(e=>e.getAttribute()===item.source_id+':0').open=true;
  render(panel,{...inbox,total_posts:29});
  assert.equal(entries.find(e=>e.getAttribute()==='all').open,true);
  assert.equal(entries.find(e=>e.getAttribute()===item.source_id+':0').open,true);
});
test('impersonator posts are labelled and kept out of the Alastar headline',()=>{
  const {render,panel}=setup();
  const fake={...item,impersonator:true};
  const real={...item,source_id:'telegram:alastartrades:464',impersonator:false,plans:[{asset:'UAI',direction:'long',thesis_quote:'still holding',condition_quote:'',risk_quote:'0.20'}]};
  render(panel,{...inbox,impersonator_posts:24,sources:{alastartrades:{healthy:true,impersonator:false,needs_review:0},
    alastartrader1:{healthy:true,impersonator:true,needs_review:3,note:'冒充号，非 Alastar 本人'}},items:[fake,real]});
  const head=panel.innerHTML.split('alastar-activity-foot')[0];
  assert.match(head,/alastartrades\/464/);
  assert.doesNotMatch(head,/alastartrader1|冒充号|待复核/);
  assert.match(panel.innerHTML,/冒充号 24 条不计入/);
  assert.match(panel.innerHTML,/<span class="alastar-impostor">冒充号<\/span>@alastartrader1/);
  assert.match(panel.innerHTML,/冒充号，非 Alastar 本人/);
  assert.match(panel.innerHTML,/alastar-plan-meta"><span class="alastar-impostor">冒充号<\/span>条件策略/);
});
test('stopped observer is not presented as a healthy collector',()=>{
  const {render,panel}=setup();render(panel,{...inbox,health:{status:'stale'}});
  assert.match(panel.innerHTML,/更新超时/);
  assert.doesNotMatch(panel.innerHTML,/data-health="healthy"/);
  render(panel,null);assert.equal(panel.hidden,true);
});
