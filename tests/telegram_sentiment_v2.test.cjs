const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
function api(){
  const ctx={window:{}};vm.createContext(ctx);
  vm.runInContext(fs.readFileSync(path.join(__dirname,'../web/static/js/telegram_sentiment_v2.js'),'utf8'),ctx);
  return ctx.window.TelegramSentimentV2;
}
function fixture(){return {status:'active',activation_at:'2026-10-31T03:00:00Z',updated_at:'2026-10-31T03:05:00Z',
  rolling:[{size:10,views:0,authors:0,bullish:0,bearish:0,score:null,smoothed_score:null,status:'warming_up',label:'条数积累中'},
    {size:20,views:0,authors:0,score:null,status:'warming_up',label:'条数积累中'}],
  background:[{size:10,score:-50,authors:8,bullish:2,bearish:6,span_hours:9,last_view_at:'2026-10-30T22:46:00Z'}],
  attention:{messages:1000,btc_views:3},books:[10,20].flatMap(size=>['follow','contrarian'].map(variant=>({size,variant,decisions:0,settled:0,pending:0,mean_net_pct:null}))),
  quality_checks:{labelled_btc_views:104,retained_btc_views:90},history:[]};}
test('warm-up and historical background are distinct from new paper evidence',()=>{
  const render=api(),s=fixture(),html=render.detail(s);
  assert.match(render.receipt(s),/背景10条 -50\.0分 · 前向预热/);
  assert.match(html,/新观点 0 \/ 10 条/);assert.match(html,/存量背景（回溯）/);
  assert.match(html,/不计入前向样本/);assert.match(html,/实际资金费/);
  assert.match(html,/没有独立人工准确率评估/);assert.match(html,/四账本共享消息/);
  assert.doesNotMatch(html,/\+0\.00%|NaN|undefined/);
});
test('author-share uncertainty does not become price probability or win rate',()=>{
  const s=fixture();s.rolling[0]={size:10,views:10,authors:8,bullish:2,bearish:6,score:-50,smoothed_score:-40,
    bullish_share_ci95:[.0715,.5907],qualified:true,status:'candidate',label:'情绪候选 · 仅观察',span_hours:4,age_hours:1};
  const html=api().detail(s);
  assert.match(html,/作者方向分/);assert.match(html,/看多账号占比的95%区间 \[7\.1%, 59\.1%\]/);
  assert.match(html,/均不是涨跌概率/);assert.match(html,/账号之间可能相关/);
});
test('coverage failures hide current scores and strings remain escaped',()=>{
  const s=fixture();s.status='error';s.label='<script>alert(1)</script>';
  s.rolling[0]={size:10,views:10,authors:8,score:100,status:'unavailable',label:'当前观察不可用'};
  const html=api().detail(s);
  assert.doesNotMatch(html,/>\+100\.0<|<script>/);assert.match(html,/&lt;script&gt;/);
  assert.match(api().detail({status:'unavailable',label:'未初始化',rolling:[]}),/未初始化/);
});
