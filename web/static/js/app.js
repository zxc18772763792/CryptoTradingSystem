const API_BASE='/api';
const state={positions:[],orders:[],strategies:[],availableStrategyTypes:[],strategyLibraryRows:[],summary:{running:[],recent_signals:[],runtime:{}},notifyRules:{},wsConnected:false,modeToken:'',bootCompleted:false,bootFailed:false,strategyHealth:null,lastHealthAlertKey:'',selectedStrategyName:'',closingPositions:{}};
const researchState={lastFactorLibrary:null,lastMultiAsset:null,lastSentiment:null,lastAnalytics:null,lastOnchain:null,lastOverview:null};
const backtestUIState={lastOptimize:null,lastCompare:null,defaultCompareStrategies:[]};
let equityChart=null;
let plotlyResizeSeq=0;
let dataReloadTimer=null;
if(typeof globalThis!=='undefined'&&typeof globalThis.sseError==='undefined')globalThis.sseError=null;

const mapOrderStatus=s=>({open:'未成交',closed:'已成交',canceled:'已撤销',expired:'已过期',rejected:'已拒绝',queued:'待触发'}[s]||s);
const mapSide=s=>s==='buy'?'买':s==='sell'?'卖':s;
const mapState=s=>({running:'运行中',idle:'空闲',paused:'已暂停',stopped:'已停止'}[s]||s);
const fmt=v=>new Intl.NumberFormat('en-US',{style:'currency',currency:'USD'}).format(Number(v||0));
const fmtMaybe=v=>(v===null||v===undefined||Number.isNaN(Number(v)))?'--':fmt(v);
function fmtDurationSec(v){const sec=Math.max(0,Math.floor(Number(v||0)));const d=Math.floor(sec/86400),h=Math.floor((sec%86400)/3600),m=Math.floor((sec%3600)/60),s=sec%60;const out=[];if(d>0)out.push(`${d}d`);if(h>0||d>0)out.push(`${h}h`);if(m>0||h>0||d>0)out.push(`${m}m`);out.push(`${s}s`);return out.join(' ');}
const esc=v=>String(v??'').replace(/[&<>"']/g,m=>({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[m]));
const TIME_LOCALE='zh-CN';
const BACKTEST_COMPARE_PRESET_KEY='cts_backtest_compare_presets_v1';
const TS_TZ_SUFFIX_RE=/(?:[zZ]|[+-]\d{2}:?\d{2})$/;
function normalizeTimestampInput(value){
if(value instanceof Date)return Number.isFinite(value.getTime())?value.toISOString():'';
if(typeof value==='number'){const ms=value>1e12?value:value*1000;const d=new Date(ms);return Number.isFinite(d.getTime())?d.toISOString():'';}
const raw=String(value??'').trim();
if(!raw)return'';
const text=raw.replace(' ','T');
if(TS_TZ_SUFFIX_RE.test(text))return text;
if(/^\d{4}-\d{2}-\d{2}$/.test(text))return`${text}T00:00:00`;
if(/^\d{4}-\d{2}-\d{2}T\d{2}$/.test(text))return`${text}:00:00`;
if(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(text))return`${text}:00`;
return text;
}
function toDate(value){const text=normalizeTimestampInput(value);if(!text)return null;const d=new Date(text);return Number.isFinite(d.getTime())?d:null;}
function toMs(value){const d=toDate(value);return d?d.getTime():NaN;}
function fmtDateTime(value){const d=toDate(value);return d?d.toLocaleString(TIME_LOCALE,{hour12:false}):'--';}
function fmtTime(value){const d=toDate(value);return d?d.toLocaleTimeString(TIME_LOCALE,{hour12:false}):'--';}
function fmtAxisDateTime(value){const d=toDate(value);return d?d.toLocaleString(TIME_LOCALE,{month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hour12:false}):'';}
function parseHeatmapBucketDate(value,bucket){
const text=String(value??'').trim();
if(!text)return null;
if(bucket==='hour'){const m=text.match(/^(\d{4}-\d{2}-\d{2})\s+(\d{2})(?::\d{2})?$/);if(m)return toDate(`${m[1]}T${m[2]}:00:00Z`);}
if(bucket==='day'){const m=text.match(/^(\d{4}-\d{2}-\d{2})$/);if(m)return toDate(`${m[1]}T00:00:00Z`);}
return toDate(text);
}
function fmtHeatmapBucket(value,bucket){
const d=parseHeatmapBucketDate(value,bucket);
if(!d)return String(value??'');
return bucket==='hour'
?d.toLocaleString(TIME_LOCALE,{month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hour12:false})
:d.toLocaleDateString(TIME_LOCALE,{year:'2-digit',month:'2-digit',day:'2-digit'});
}
function plotlyTimeAxis(extra={}){
return{
type:'date',
showgrid:true,
gridcolor:'#283242',
tickformat:'%m-%d %H:%M',
hoverformat:'%Y-%m-%d %H:%M:%S',
tickformatstops:[
{dtickrange:[null,60000],value:'%H:%M:%S'},
{dtickrange:[60000,3600000],value:'%H:%M'},
{dtickrange:[3600000,86400000],value:'%m-%d %H:%M'},
{dtickrange:[86400000,604800000],value:'%m-%d'},
{dtickrange:[604800000,null],value:'%Y-%m-%d'}
],
...extra,
};
}
const STRATEGY_META={
MAStrategy:{cat:'趋势',desc:'双均线金叉死叉顺势'},
EMAStrategy:{cat:'趋势',desc:'EMA 快慢线更灵敏'},
MACDStrategy:{cat:'趋势',desc:'MACD 线与信号线'},
MACDHistogramStrategy:{cat:'趋势',desc:'MACD 柱体拐点'},
ADXTrendStrategy:{cat:'趋势',desc:'ADX + DI 强趋势过滤'},
DonchianBreakoutStrategy:{cat:'突破',desc:'通道突破入场，回落离场'},
BollingerBandsStrategy:{cat:'震荡',desc:'布林带反转交易'},
BollingerSqueezeStrategy:{cat:'突破',desc:'布林收窄后突破'},
RSIStrategy:{cat:'震荡',desc:'RSI 超买超卖反转'},
RSIDivergenceStrategy:{cat:'反转',desc:'价格与 RSI 背离'},
StochasticStrategy:{cat:'震荡',desc:'KDJ/Stochastic 交叉'},
VWAPReversionStrategy:{cat:'均值回归',desc:'偏离 VWAP 后回归'},
MeanReversionStrategy:{cat:'均值回归',desc:'Z-score 回归'},
BollingerMeanReversionStrategy:{cat:'均值回归',desc:'布林偏离回归'},
MomentumStrategy:{cat:'动量',desc:'动量突破跟随'},
TrendFollowingStrategy:{cat:'趋势',desc:'趋势确认跟随'},
PairsTradingStrategy:{cat:'统计套利',desc:'价差回归（需配对数据）'},
CEXArbitrageStrategy:{cat:'套利',desc:'中心化交易所价差套利'},
TriangularArbitrageStrategy:{cat:'套利',desc:'三角路径套利'},
DEXArbitrageStrategy:{cat:'套利',desc:'链上/链下价差套利'},
FlashLoanArbitrageStrategy:{cat:'套利',desc:'闪电贷套利框架'},
MarketSentimentStrategy:{cat:'宏观',desc:'市场情绪因子'},
SocialSentimentStrategy:{cat:'宏观',desc:'社媒情绪因子'},
FundFlowStrategy:{cat:'宏观',desc:'资金流入流出'},
WhaleActivityStrategy:{cat:'宏观',desc:'大户活动跟踪'}
};

function notify(msg,err=false){const n=document.getElementById('notification');if(!n)return;n.textContent=msg;n.className=`notification show ${err?'error':''}`;setTimeout(()=>n.classList.remove('show'),3000);}
function isVisibleEl(el){
if(!el||!(el instanceof HTMLElement))return false;
const cs=getComputedStyle(el);
if(cs.display==='none'||cs.visibility==='hidden')return false;
if(el.offsetParent===null&&cs.position!=='fixed')return false;
return el.clientWidth>0&&el.clientHeight>0;
}
function getLocalJson(key, fallback){try{const raw=localStorage.getItem(key);if(!raw)return fallback;const v=JSON.parse(raw);return v??fallback;}catch{return fallback;}}
function setLocalJson(key, value){try{localStorage.setItem(key, JSON.stringify(value));return true;}catch{return false;}}
function strategyTypeShortName(v){
const s=String(v||'').replace(/Strategy$/,'');
const map={BollingerBands:'布林带',BollingerSqueeze:'布林挤压',BollingerMeanReversion:'布林回归',MeanReversion:'均值回归',TrendFollowing:'趋势跟随',DonchianBreakout:'唐奇安突破',WhaleActivity:'巨鲸',MarketSentiment:'市场情绪',SocialSentiment:'社媒情绪',FundFlow:'资金流',VWAPReversion:'VWAP回归',MACDHistogram:'MACD柱',MACD:'MACD',EMA:'EMA',MA:'MA',RSIDivergence:'RSI背离',RSI:'RSI',Stochastic:'随机指标',ADXTrend:'ADX趋势',Momentum:'动量',PairsTrading:'配对交易',CEXArbitrage:'CEX套利',TriangularArbitrage:'三角套利',DEXArbitrage:'DEX套利',FlashLoanArbitrage:'闪电贷套利',FamaFactorArbitrage:'Fama因子套利'};
return map[s]||s;
}
function strategyInstanceSuffix(name){
const txt=String(name||'');
const m=txt.match(/(?:_|-)(\d{4,})$/);
return m?m[1].slice(-4):txt.slice(-4);
}
function shortInstanceId(name){
const txt=String(name||'');
if(!txt)return'-';
const parts=txt.split('_').filter(Boolean);
const tail=parts.slice(-2).join('_')||txt.slice(-12);
return tail.length>18?tail.slice(-18):tail;
}
function fmtQtyPreview(value){
  const num=Number(value||0);
  if(!Number.isFinite(num))return '--';
  if(num===0)return '0';
  if(Math.abs(num)>=1)return num.toFixed(4).replace(/\.?0+$/,'');
  return num.toFixed(8).replace(/\.?0+$/,'');
}
function buildStrategyShortDisplayLabel(s, typeIndex=1, typeCount=1){
const stype=String(s?.strategy_type||s?.name||'');
const shortType=strategyTypeShortName(stype);
const tf=String(s?.timeframe||'-');
const syms=Array.isArray(s?.symbols)?s.symbols:[];
const symTxt=syms.length?String(syms[0]||'').replace('/USDT','').replace('/USD',''): '全部';
const multiSym=syms.length>1?`+${syms.length-1}`:'';
const instanceTag=typeCount>1?`#${typeIndex}`:`#${strategyInstanceSuffix(s?.name)}`;
return `${shortType}${instanceTag} · ${tf} · ${symTxt}${multiSym}`;
}
function getRegisteredStrategyFilters(){
return{
search:String(document.getElementById('registered-strategy-search')?.value||'').trim().toLowerCase(),
category:String(document.getElementById('registered-strategy-cat-filter')?.value||'').trim(),
state:String(document.getElementById('registered-strategy-state-filter')?.value||'').trim().toLowerCase(),
};
}
function normalizeInstanceSuffixText(v){
return String(v||'').trim().replace(/\s+/g,'_').replace(/[^a-zA-Z0-9_\-]/g,'').slice(0,24);
}
function buildStrategyInstanceName(strategyType,{prefix='inst',suffix=''}={}){
const base=String(strategyType||'Strategy').trim()||'Strategy';
const stamp=new Date().toISOString().replace(/[-:TZ.]/g,'').slice(8,14);
const rnd=Math.floor(Math.random()*1000).toString().padStart(3,'0');
const suffixTxt=normalizeInstanceSuffixText(suffix);
return `${base}_${prefix}_${stamp}_${rnd}${suffixTxt?`_${suffixTxt}`:''}`;
}
function getBacktestRegisterOptions(){
const allocation=Math.max(0,Math.min(1,Number(document.getElementById('backtest-register-allocation')?.value||0.05)));
const autoStart=!!document.getElementById('backtest-register-auto-start')?.checked;
const suffix=normalizeInstanceSuffixText(document.getElementById('backtest-register-suffix')?.value||'');
return{allocation,autoStart,suffix};
}
function resizePlotlyIn(root=document){
if(typeof Plotly==='undefined'||!Plotly?.Plots?.resize)return;
const scope=(root&&root.querySelectorAll)?root:document;
scope.querySelectorAll('.js-plotly-plot').forEach(el=>{
if(!isVisibleEl(el))return;
try{Plotly.Plots.resize(el);}catch{}
});
}
function schedulePlotlyResize(root=document){
const seq=++plotlyResizeSeq;
const run=()=>{if(seq!==plotlyResizeSeq)return;resizePlotlyIn(root);};
try{requestAnimationFrame(run);}catch{setTimeout(run,0);}
setTimeout(run,80);
setTimeout(run,220);
}
function preparePlotlyHost(el){
if(!el)return;
el.style.display='block';
el.style.alignItems='';
el.style.justifyContent='';
el.style.position=el.style.position||'relative';
}
async function api(ep,opt={}){const o=opt||{};const tmo=Math.max(1000,Number(o.timeoutMs||12000));const {timeoutMs,...rest}=o;const c=new AbortController();const timer=setTimeout(()=>c.abort(),tmo);try{const r=await fetch(`${API_BASE}${ep}`,{...rest,signal:c.signal,headers:{'Content-Type':'application/json',...(rest.headers||{})}});const ct=(r.headers.get('content-type')||'').toLowerCase();let d={};if(ct.includes('application/json')){d=await r.json();}else{const t=await r.text();d=t?{detail:t}:{};}if(!r.ok)throw new Error(d.detail||d.error||`接口请求失败(${r.status})`);return d;}catch(e){if(e?.name==='AbortError')throw new Error(`接口超时(${tmo}ms): ${ep}`);throw e;}finally{clearTimeout(timer);}}
function markBootFailure(err){const msg=err?.message||String(err||'未知错误');if(state.bootCompleted){console.error('runtime error:',err);notify(`运行期异常: ${msg}`,true);return;}if(state.bootFailed)return;state.bootFailed=true;console.error('bootstrap failed:',err);const st=document.getElementById('system-status'),m=document.getElementById('trading-mode'),ex=document.getElementById('exchanges-list');if(st)st.textContent='前端初始化失败';if(m)m.textContent='未知';if(ex)ex.innerHTML=`<div class=\"list-item\">页面初始化失败: ${esc(msg)}</div>`;notify(`前端初始化失败: ${msg}`,true);}

function activateTab(tabName){
if(!tabName)return;
const b=document.querySelector(`.tab-btn[data-tab="${tabName}"]`);
if(!b)return;
document.querySelectorAll('.tab-btn').forEach(x=>x.classList.remove('active'));
document.querySelectorAll('.tab-content').forEach(x=>x.classList.remove('active'));
b.classList.add('active');
const panel=document.getElementById(b.dataset.tab);
panel?.classList.add('active');
if(panel)schedulePlotlyResize(panel);
}
function initTabs(){
document.querySelectorAll('.tab-btn').forEach(b=>b.onclick=()=>activateTab(b.dataset.tab));
const qs=new URLSearchParams(window.location.search||'');
const byQuery=String(qs.get('tab')||'').trim();
const byHash=String((window.location.hash||'').replace(/^#/,'')).trim();
if(byQuery)activateTab(byQuery);else if(byHash)activateTab(byHash);
window.addEventListener('hashchange',()=>{const t=String((window.location.hash||'').replace(/^#/,'')).trim();if(t)activateTab(t);});
window.addEventListener('resize',()=>schedulePlotlyResize(document.querySelector('.tab-content.active')||document));
}
function initClock(){const f=()=>{const t=document.getElementById('current-time');if(t)t.textContent=new Date().toLocaleString(TIME_LOCALE,{hour12:false});};f();setInterval(f,1000);}
async function loadSystemStatus(){
if(state._systemStatusInFlight)return;
state._systemStatusInFlight=true;
try{
const s=await api('/status',{timeoutMs:16000});
state._systemStatusLast=s;
state._systemStatusFailCount=0;
state._systemStatusLastOkAt=Date.now();
const st=document.getElementById('system-status'),m=document.getElementById('trading-mode');
if(st)st.textContent=s.status==='running'?'运行中':s.status;
if(m)m.textContent=s.trading_mode==='paper'?'模拟盘':'实盘';
const exCountEl=document.getElementById('exchange-status-count');
if(exCountEl){
const totalRaw=Number(s?.total_exchange_count??Object.keys(s?.exchange_status||{}).length);
const total=Number.isFinite(totalRaw)?Math.max(0,totalRaw):0;
const connectedRaw=Number(s?.exchange_count??(Array.isArray(s?.exchanges)?s.exchanges.length:0));
const connected=Number.isFinite(connectedRaw)?Math.max(0,connectedRaw):0;
if(total>0)exCountEl.textContent=`${connected}/${total}`;
}
}catch(e){
state._systemStatusFailCount=Number(state._systemStatusFailCount||0)+1;
const st=document.getElementById('system-status'),m=document.getElementById('trading-mode');
const last=state._systemStatusLast||null;
const lastOkAt=Number(state._systemStatusLastOkAt||0);
const recentOk=lastOkAt>0 && (Date.now()-lastOkAt)<=45000;
if(last){
  if(st)st.textContent=(state._systemStatusFailCount>=3 || !recentOk)?`运行中(状态延迟，重试中)`:'运行中';
  if(m)m.textContent=last.trading_mode==='paper'?'模拟盘':'实盘';
}else{
  if(st)st.textContent='状态获取失败(自动重试)';
  if(m)m.textContent='未知';
}
if(Number(state._systemStatusFailCount||0)%4===1){
  console.warn('loadSystemStatus failed:', e?.message||e);
}
}finally{
state._systemStatusInFlight=false;
}
}

function initEquity(){
const c=document.getElementById('equity-chart');
if(!c)return;
if(typeof Chart==='undefined'){const p=c.parentElement;if(p)p.innerHTML='<div class="list-item">图表库未加载，净值图暂不可用</div>';return;}
equityChart=new Chart(c.getContext('2d'),{
type:'line',
data:{labels:[],datasets:[{data:[],borderColor:'#3fb950',backgroundColor:'rgba(63,185,80,.15)',fill:true,tension:.2,pointRadius:0}]},
options:{
responsive:true,
maintainAspectRatio:false,
plugins:{legend:{display:false}},
scales:{x:{ticks:{autoSkip:true,maxTicksLimit:8}},y:{ticks:{callback:v=>`$${Number(v||0).toFixed(0)}`}}},
}
});
}
function drawEquity(hist){
if(!equityChart)return;
if(!hist?.length){
equityChart.data.labels=[];
equityChart.data.datasets[0].data=[];
equityChart.update('none');
return;
}
const max=220;
const sampled=hist.length>max?hist.filter((_,i)=>i%Math.ceil(hist.length/max)===0):hist;
const rows=sampled.map(x=>({timestamp:x.timestamp,total:Number(x.total_usd||0)})).filter(x=>Number.isFinite(x.total)&&x.total>0&&toDate(x.timestamp));
if(!rows.length){
equityChart.data.labels=[];
equityChart.data.datasets[0].data=[];
equityChart.update('none');
return;
}
equityChart.data.labels=rows.map(x=>fmtAxisDateTime(x.timestamp));
equityChart.data.datasets[0].data=rows.map(x=>x.total);
equityChart.$rawTs=rows.map(x=>x.timestamp);
equityChart.options.plugins.tooltip={callbacks:{title:items=>{const idx=items?.[0]?.dataIndex;return idx===undefined?'':fmtDateTime(equityChart.$rawTs?.[idx]);}}};
equityChart.update('none');
}
function drawPie(dist,mode){const box=document.getElementById('holdings-pie');if(!box)return;if(!dist?.length){box.innerHTML='<div class="list-item">暂无可视化资产分布</div>';return;}if(typeof Plotly==='undefined'){box.innerHTML='<div class="list-item">图表库未加载，饼图暂不可用</div>';return;}const top=dist.slice(0,10);Plotly.newPlot(box,[{type:'pie',labels:top.map(x=>x.currency),values:top.map(x=>Number(x.usd_value||0)),hole:.45,textinfo:'label+percent'}],{margin:{l:5,r:5,t:5,b:5},paper_bgcolor:'#162232',plot_bgcolor:'#162232',font:{color:'#e8eef9'},showlegend:false},{displaylogo:false,responsive:true});schedulePlotlyResize(document.getElementById('dashboard')||document);}

function renderRisk(r){const p=document.getElementById('risk-panel');if(!p||!r)return;const e=r.equity||{},a=r.alerts||[],c=`risk-${r.risk_level||'low'}`;const dailyTotalRatio=Number((e.daily_total_pnl_ratio??e.daily_pnl_ratio)??0);const stopBasis=Number((e.daily_stop_basis_usd??e.daily_total_pnl_usd??e.daily_pnl_usd)??0);const stopBasisRatio=Number((e.daily_stop_basis_ratio??e.daily_pnl_ratio)??0);p.innerHTML=`
<div class="list-item"><span>风险等级</span><span class="${c}">${(r.risk_level||'low').toUpperCase()}</span></div>
<div class="list-item"><span>风控熔断</span><span>${r.trading_halted?'已触发':'未触发'}</span></div>
<div class="list-item"><span>日内总盈亏</span><span>${Number((e.daily_total_pnl_usd??e.daily_pnl_usd)||0).toFixed(2)} USDT (${(dailyTotalRatio*100).toFixed(2)}%)</span></div>
<div class="list-item"><span>熔断依据</span><span>${stopBasis.toFixed(2)} USDT (${(stopBasisRatio*100).toFixed(2)}%)</span></div>
<div class="list-item"><span>当前持仓浮盈亏</span><span class="${Number(e.current_unrealized_pnl_usd||0)>=0?'positive':'negative'}">${Number(e.current_unrealized_pnl_usd||0).toFixed(2)} USDT</span></div>
<div class="list-item"><span>今日已实现盈亏</span><span class="${Number(e.daily_realized_pnl_usd||0)>=0?'positive':'negative'}">${Number(e.daily_realized_pnl_usd||0).toFixed(2)} USDT</span></div>
<div class="list-item"><span>最大回撤</span><span>${(Number(e.max_drawdown||0)*100).toFixed(2)}%</span></div>
<div class="list-item"><span>杠杆上限</span><span>${Number(r.limits?.max_leverage||0).toFixed(2)}x</span></div>
<div class="list-item"><span>最近告警</span><span>${a.length} 条</span></div>
${a.slice(-3).map(x=>`<div class="list-item"><span>${x.title}</span><span>${x.timestamp?.substring(11,19)||''}</span></div>`).join('')}`;}

function renderExchanges(b){const el=document.getElementById('exchanges-list');if(!el)return;const mode=String(b?.mode||b?.active_account_type||'').toLowerCase();const ex=b?.exchanges||{},keys=Object.keys(ex);const statusRows=keys.map(k=>{const i=ex[k]||{};return `<div class="list-item"><span>${k.toUpperCase()}</span><span class="status-badge ${i.connected?'connected':''}">${i.connected?'已连接':'未连接'}</span></div>`;}).join('')||'<div class="list-item">暂无交易所连接</div>';if(mode==='paper'){const i=b?.paper_account||{};const all=(i.balances||[]).filter(x=>Number(x.total)>0);const stable=all.filter(x=>['USDT','USDC'].includes(String(x.currency||'').toUpperCase()));const others=all.filter(x=>!['USDT','USDC'].includes(String(x.currency||'').toUpperCase()));const pick=[...stable,...others].slice(0,12);const rows=pick.map(v=>`<div class="balance-item"><span class="currency">${v.currency}</span><span class="amount">${Number(v.total||0).toFixed(6)}${Number(v.usd_value||0)>0?` (~$${Number(v.usd_value).toFixed(2)})`:''}</span></div>`).join('')||'<div class="balance-item">无资产</div>';el.innerHTML=`<div class="exchange-section"><div class="exchange-header"><span class="exchange-name">PAPER</span><span class="exchange-usd">$${Number(i.total_usd||0).toFixed(2)}</span><span class="status-badge connected">模拟仓</span></div><div class="balance-list">${rows}</div></div><div class="exchange-section"><div class="exchange-header"><span class="exchange-name">交易所连接状态</span></div><div class="balance-list">${statusRows}</div></div>`;return;}if(!keys.length){el.innerHTML='<div class="list-item">暂无交易所连接</div>';return;}el.innerHTML=keys.map(k=>{const i=ex[k];if(!i.connected)return `<div class="exchange-section"><div class="exchange-header"><span class="exchange-name">${k.toUpperCase()}</span><span class="status-badge">未连接</span></div></div>`;const all=(i.balances||[]).filter(x=>Number(x.total)>0);const stable=all.filter(x=>['USDT','USDC'].includes(String(x.currency||'').toUpperCase()));const others=all.filter(x=>!['USDT','USDC'].includes(String(x.currency||'').toUpperCase()));const pick=[...stable,...others].slice(0,12);const rows=pick.map(v=>`<div class="balance-item"><span class="currency">${v.currency}</span><span class="amount">${Number(v.total||0).toFixed(6)}${Number(v.usd_value||0)>0?` (~$${Number(v.usd_value).toFixed(2)})`:''}</span></div>`).join('')||'<div class="balance-item">无资产</div>';return `<div class="exchange-section"><div class="exchange-header"><span class="exchange-name">${k.toUpperCase()}</span><span class="exchange-usd">$${Number(i.total_usd||0).toFixed(2)}</span><span class="status-badge connected">已连接</span></div><div class="balance-list">${rows}</div></div>`;}).join('');}

async function loadSummary(){
try{
const [sr,br]=await Promise.allSettled([
api('/trading/stats',{timeoutMs:8000}),
api('/trading/balances',{timeoutMs:22000}),
]);
const s=sr.status==='fulfilled'?sr.value:{};
const b=br.status==='fulfilled'?br.value:{};
const statusMode=String(state?._systemStatusLast?.trading_mode||'').toLowerCase();
const activeType=String(b?.active_account_type??b?.mode??statusMode??'paper').toLowerCase();
const h=await api(`/trading/balances/history?hours=72&exchange=all&limit=500&mode=${encodeURIComponent(activeType==='live'?'live':'paper')}`,{timeoutMs:8000}).catch(()=>({history:[]}));
const activeUsd=Number(b?.active_account_usd_estimate??b?.total_usd_estimate??0);
const mergedRisk=(b?.risk_report||s?.risk||null);
const mergedEquity=(mergedRisk?.equity||{});
const livePosCount=Number(b?.live_position_count||0);
const statPosCount=Number(s?.positions?.position_count||0);
document.getElementById('open-positions').textContent=(activeType==='live'?livePosCount:statPosCount)||0;
document.getElementById('open-orders').textContent=s?.orders?.total_orders||0;
const exObj=b?.exchanges||{},exKeys=Object.keys(exObj),exConnected=exKeys.filter(k=>Boolean(exObj[k]?.connected)).length;
const exCountEl=document.getElementById('exchange-status-count');
if(exCountEl)exCountEl.textContent=`${exConnected}/${exKeys.length||0}`;
const modeEl=document.getElementById('active-account-mode');
if(modeEl)modeEl.textContent=activeType==='paper'?'虚拟仓(PAPER)':'实仓(LIVE)';
const activeEl=document.getElementById('active-account-value');
if(activeEl)activeEl.textContent=fmtMaybe(activeUsd);
const pnl=Number((mergedEquity?.current_unrealized_pnl_usd ?? s?.positions?.total_unrealized_pnl) || 0),p=document.getElementById('total-pnl');
p.textContent=fmt(pnl);p.className=`value ${pnl>=0?'positive':'negative'}`;
renderExchanges(b);drawEquity(h?.history||[]);drawPie(b?.distribution||[],activeType);renderRisk(mergedRisk);
if(sr.status==='rejected'&&br.status==='rejected'){const ex=document.getElementById('exchanges-list');if(ex)ex.innerHTML='<div class=\"list-item\">资产接口暂时不可用，系统正在自动重试...</div>';}
}catch(e){console.error(e);const ex=document.getElementById('exchanges-list');if(ex)ex.innerHTML='<div class=\"list-item\">资产加载失败，正在重试...</div>';}
}
async function loadStats(){return loadSummary();}
async function loadBalances(){return loadSummary();}
async function loadBanlances(){return loadSummary();}
async function loadRisk(){try{renderRisk(await api('/trading/risk/report'));}catch{}}
function renderPnlHeatmap(data){
const box=document.getElementById('pnl-heatmap');
if(!box)return;
const note=String(data?.note||'').trim();
if(!data?.times?.length||!data?.symbols?.length||!data?.matrix?.length){box.innerHTML=`<div class="list-item">${esc(note||'暂无已平仓盈亏数据，请先运行策略或手动交易。')}</div>`;return;}
if(typeof Plotly==='undefined'){box.innerHTML='<div class="list-item">图表库未加载，热力图暂不可用。</div>';return;}
const flat=(data.matrix||[]).flat().map(v=>Number(v||0)).filter(v=>Number.isFinite(v));
if(!flat.length){box.innerHTML='<div class="list-item">热力图数据为空。</div>';return;}
const absMax=Math.max(...flat.map(v=>Math.abs(v)),0);
if(absMax===0){box.innerHTML='<div class="list-item">最近区间内有记录，但净盈亏基本为 0。</div>';return;}
const bucket=data.bucket==='hour'?'hour':'day';
const yLabels=(data.times||[]).map(t=>fmtHeatmapBucket(t,bucket));
const xLabels=(data.symbols||[]).map(s=>String(s||'').replace('/USDT','').replace('/USD','').replace('/PERP',''));
const zmax=Math.max(absMax,1e-8),zmin=-zmax;
const chartHeight=Math.max(300,Math.min(680,150 + yLabels.length*30 + (xLabels.length>10?80:40)));
box.innerHTML=`${note?`<div class="list-item" style="margin-bottom:8px;">${esc(note)}</div>`:''}<div id="pnl-heatmap-plot" style="height:${chartHeight}px;"></div>`;
const plotEl=document.getElementById('pnl-heatmap-plot');
if(!plotEl)return;
Plotly.react(plotEl,[{
type:'heatmap',
x:xLabels,
y:yLabels,
z:data.matrix,
colorscale:[[0,'#b13a48'],[0.5,'#1f2a3a'],[1,'#1f9d63']],
zmid:0,
zmin,
zmax,
colorbar:{title:String(data?.value_title||'PnL'),thickness:14,len:.88,y:.5},
xgap:1,
ygap:1,
hovertemplate:`币种=%{x}<br>时间=%{y}<br>${esc(String(data?.value_hover||'PnL'))}=%{z:.6f}<extra></extra>`,
}],{
paper_bgcolor:'#111723',
plot_bgcolor:'#111723',
font:{color:'#d7dde8'},
height:chartHeight,
margin:{l:110,r:36,t:14,b:Math.max(90,Math.min(170,50+xLabels.length*5))},
xaxis:{title:'币种',tickangle:-40,automargin:true,tickfont:{size:10}},
yaxis:{title:bucket==='hour'?'时间(本地时区)':'日期(本地时区)',automargin:true,tickfont:{size:11}},
},{responsive:true,displaylogo:false,scrollZoom:true});
}
async function loadPnlHeatmap(){try{const d=Number(document.getElementById('pnl-heatmap-days')?.value||30),b=document.getElementById('pnl-heatmap-bucket')?.value||'day';const r=await api(`/trading/pnl/heatmap?days=${Math.max(1,d)}&bucket=${encodeURIComponent(b)}`,{timeoutMs:8000});renderPnlHeatmap(r);}catch(e){const box=document.getElementById('pnl-heatmap');if(box)box.innerHTML=`<div class="list-item">热力图加载失败: ${esc(e.message)}</div>`;}}

function positionCloseKey(p){return[String(p.exchange||''),String(p.symbol||''),String(p.side||''),String(p.account_id||''),String(p?.metadata?.source||'local')].join('|');}
async function closePositionFromRow(btn){
try{
  if(!btn)return;
  const p={
    exchange:String(btn.dataset.exchange||'').trim(),
    symbol:String(btn.dataset.symbol||'').trim(),
    side:String(btn.dataset.side||'').trim(),
    account_id:String(btn.dataset.accountId||'').trim()||undefined,
    source:String(btn.dataset.source||'').trim()||'local',
    quantity:Number(btn.dataset.quantity||0)||undefined
  };
  const key=[p.exchange,p.symbol,p.side,p.account_id||'',p.source||'local'].join('|');
  if(state.closingPositions[key])return;
  const sideText=p.side==='long'?'多':(p.side==='short'?'空':p.side);
  const srcText=p.source==='exchange_live'?'交易所实盘持仓':'系统持仓';
  if(!confirm(`确认一键平仓？\\n${p.exchange} ${p.symbol} ${sideText}\\n来源: ${srcText}`))return;
  state.closingPositions[key]=true;
  btn.disabled=true;
  btn.textContent='平仓中...';
  const resp=await api('/trading/positions/close',{method:'POST',timeoutMs:45000,body:JSON.stringify(p)});
  notify(`已提交平仓: ${p.symbol} (${sideText})`);
  await Promise.allSettled([loadPositions(),loadOrders(),loadStats(),loadBalances()]);
  const out=document.getElementById('order-output');
  if(out && resp) out.textContent=JSON.stringify(resp,null,2);
}catch(e){
  notify(`平仓失败: ${e.message}`,true);
}finally{
  try{
    if(btn){
      const p={
        exchange:String(btn.dataset.exchange||'').trim(),
        symbol:String(btn.dataset.symbol||'').trim(),
        side:String(btn.dataset.side||'').trim(),
        account_id:String(btn.dataset.accountId||'').trim()||'',
        source:String(btn.dataset.source||'').trim()||'local',
      };
      const key=[p.exchange,p.symbol,p.side,p.account_id,p.source].join('|');
      delete state.closingPositions[key];
    }
  }catch{}
  loadPositions().catch(()=>{});
}
}
async function loadPositions(){try{const resp=await api('/trading/positions');state.positions=resp.positions||[];const t=document.getElementById('positions-tbody');if(!t)return;if(!state.positions.length){t.innerHTML='<tr><td colspan="6">暂无持仓</td></tr>';return;}t.innerHTML=state.positions.map(p=>{const source=(p?.metadata?.source||'local');const key=positionCloseKey(p);const busy=!!state.closingPositions[key];const sideText=p.side==='long'?'多':p.side==='short'?'空':(p.side||'-');const sourceTag=source==='exchange_live'?'<span class="status-badge" style="margin-left:6px;background:#2f4f7f;">实盘同步</span>':'';const accountId=String(p.account_id||'');return `<tr><td>${p.exchange||'-'} ${p.symbol}${sourceTag}</td><td>${sideText}</td><td>${Number(p.entry_price||0).toFixed(2)}</td><td>${Number(p.current_price||0).toFixed(2)}</td><td class="${Number(p.unrealized_pnl||0)>=0?'positive':'negative'}">${fmt(p.unrealized_pnl||0)}</td><td><button class="btn btn-danger btn-sm" ${busy?'disabled':''} onclick="closePositionFromRow(this)" data-exchange="${esc(p.exchange||'')}" data-symbol="${esc(p.symbol||'')}" data-side="${esc(p.side||'')}" data-account-id="${esc(accountId)}" data-source="${esc(source)}" data-quantity="${Number(p.quantity||0)}">${busy?'平仓中...':'一键平仓'}</button></td></tr>`;}).join('');}catch(e){console.error(e);}}
async function loadOrders(){
try{
state.orders=(await api('/trading/orders?include_history=true&limit=200')).orders||[];
const t=document.getElementById('orders-tbody');
if(!t)return;
const view=document.getElementById('orders-view-filter')?.value||'all';
const rows=state.orders.filter(o=>{
const s=String(o.strategy||'').trim().toLowerCase();
const isManual=!s||s==='manual';
const isStrategy=!isManual;
if(view==='strategy')return isStrategy;
if(view==='manual')return isManual;
return true;
});
if(!rows.length){t.innerHTML='<tr><td colspan="8">暂无订单</td></tr>';return;}
t.innerHTML=rows.map(o=>{
const strategy=(o.strategy&&String(o.strategy).trim())?String(o.strategy):'manual';
const sourceBadge=strategy==='manual'
?'<span class="status-badge">手动</span>'
:`<span class="status-badge connected">策略</span> ${esc(strategy)}`;
const reason=o.reject_reason?esc(o.reject_reason):'';
const feeUsd=Number(o.paper_fee_usd||0);
const slipBps=Number(o.paper_slippage_bps||0);
const slipUsd=Number(o.paper_slippage_cost_usd||0);
const costMemo=(feeUsd>0||slipBps>0)?`费用:$${feeUsd.toFixed(4)} | 滑点:${slipBps.toFixed(2)}bps ($${slipUsd.toFixed(4)})`:'';
const sl=Number(o.stop_loss||0),tp=Number(o.take_profit||0),trig=Number(o.trigger_price||0),trailPct=Number(o.trailing_stop_pct||0),trailDist=Number(o.trailing_stop_distance||0);
const protectMemo=[
  sl>0?`SL:${sl.toFixed(2)}`:'',
  tp>0?`TP:${tp.toFixed(2)}`:'',
  trig>0?`触发:${trig.toFixed(2)}`:'',
  trailPct>0?`追踪:${(trailPct*100).toFixed(2)}%`:'',
  trailDist>0?`追踪距:${trailDist.toFixed(2)}`:''
].filter(Boolean).join(' / ');
const memo=[reason,costMemo,protectMemo].filter(Boolean).join(' | ');
const statusCell=`${mapSide(o.side)}/${mapOrderStatus(o.status)}${reason?`<div class="order-reject-reason">${reason}</div>`:''}`;
return `<tr><td>${o.exchange||'-'} ${o.symbol}</td><td>${sourceBadge}</td><td>${esc(o.account_id||'main')}</td><td>${statusCell}</td><td>${Number(o.price||0).toFixed(2)}</td><td>${Number(o.amount||0)}</td><td>${o.status==='open'?`<button class="btn btn-danger btn-sm" onclick="cancelOrder('${o.id}','${o.symbol}','${o.exchange||'binance'}')">撤销</button>`:'<span style="color:#8b949e">--</span>'}</td><td>${memo||'--'}</td></tr>`;
}).join('');
}catch(e){console.error(e);}
}
async function loadOpenOrders(){
try{
const rows=((await api('/trading/orders?include_history=false&limit=200',{timeoutMs:15000})).orders||[]);
const t=document.getElementById('open-orders-tbody');
if(!t)return;
if(!rows.length){t.innerHTML='<tr><td colspan="8">暂无当前委托</td></tr>';return;}
t.innerHTML=rows.map(o=>{
const strategy=(o.strategy&&String(o.strategy).trim())?String(o.strategy):'manual';
const sourceBadge=strategy==='manual'
?'<span class="status-badge">手动</span>'
:`<span class="status-badge connected">策略</span> ${esc(strategy)}`;
const statusCell=`${mapSide(o.side)}/${mapOrderStatus(o.status)}`;
const sl=Number(o.stop_loss||0),tp=Number(o.take_profit||0),trig=Number(o.trigger_price||0),trailPct=Number(o.trailing_stop_pct||0),trailDist=Number(o.trailing_stop_distance||0);
const protect=[
  sl>0?`SL ${sl.toFixed(2)}`:'',
  tp>0?`TP ${tp.toFixed(2)}`:'',
  trig>0?`触发 ${trig.toFixed(2)}`:'',
  trailPct>0?`追踪 ${(trailPct*100).toFixed(2)}%`:'',
  trailDist>0?`追踪距 ${trailDist.toFixed(2)}`:''
].filter(Boolean);
const protectCell=protect.length?protect.map(x=>`<div>${x}</div>`).join(''):'--';
return `<tr><td>${o.exchange||'-'} ${o.symbol}</td><td>${sourceBadge}</td><td>${esc(o.account_id||'main')}</td><td>${statusCell}</td><td>${Number(o.price||0).toFixed(2)}</td><td>${Number(o.amount||0)}</td><td>${protectCell}</td><td><button class="btn btn-danger btn-sm" onclick="cancelOrder('${o.id}','${o.symbol}','${o.exchange||'binance'}')">撤销</button></td></tr>`;
}).join('');
}catch(e){
console.error(e);
const t=document.getElementById('open-orders-tbody');
if(t)t.innerHTML=`<tr><td colspan="8">当前委托加载失败：${esc(e.message||'未知错误')}</td></tr>`;
}
}
function bindOrderView(){
const v=document.getElementById('orders-view-filter'),b=document.getElementById('btn-refresh-orders');
if(v)v.onchange=()=>loadOrders();
if(b)b.onclick=()=>loadOrders();
const bo=document.getElementById('btn-refresh-open-orders');
if(bo)bo.onclick=()=>loadOpenOrders();
}
async function cancelOrder(id,symbol,exchange){try{await api(`/trading/order/${id}?symbol=${encodeURIComponent(symbol)}&exchange=${exchange}`,{method:'DELETE'});notify('订单已撤销');await Promise.allSettled([loadOrders(),loadOpenOrders()]);}catch(e){notify(`撤销失败: ${e.message}`,true);}}

async function loadStrategies(){
try{
const d=await api('/strategies/list');
state.availableStrategyTypes=Array.isArray(d?.strategies)?d.strategies:[];
state.strategies=d.registered||[];
const pool=document.getElementById('strategies-list');
if(pool){
const catalog=backtestCompareCatalog();
const libraryRows=(d.strategies||[]).map(s=>{
  const m=STRATEGY_META[s]||{cat:'其他',desc:'可注册后在参数面板调整'};
  const groupLabel=(catalog.byValue?.[s]?.groupLabel)||mapStrategyCatToBacktestGroup(m.cat);
  const card=`<div class="strategy-card" onclick="registerStrategy('${s}')"><div class="list-item" style="padding:0 0 6px 0;border-bottom:none;"><h4>${s}</h4><span class="status-badge">${m.cat}</span></div><p>${m.desc}</p><p style="font-size:11px;color:#8fa6c0;">点击卡片注册到策略池（模拟盘）</p></div>`;
  return {strategy:s,groupLabel,card};
});
if(!libraryRows.length){pool.innerHTML='<div class="list-item">暂无可用策略</div>';}
else{
  const groupedLib={};libraryRows.forEach(r=>{(groupedLib[r.groupLabel]||(groupedLib[r.groupLabel]=[])).push(r);});
  const groupOrder=[...(catalog.groups||[]).map(g=>g.label),'其他'].filter((v,i,a)=>a.indexOf(v)===i);
  pool.innerHTML=groupOrder.filter(g=>Array.isArray(groupedLib[g])&&groupedLib[g].length).map(g=>{
    const cards=(groupedLib[g]||[]).sort((a,b)=>{
      const ai=Number(catalog.orderIndex?.[a.strategy]??9999),bi=Number(catalog.orderIndex?.[b.strategy]??9999);
      return ai-bi || String(a.strategy).localeCompare(String(b.strategy),'zh-CN');
    }).map(x=>x.card).join('');
    return `<details class="registered-category-group" open><summary><span class="title">${esc(g)}</span><span class="count">${(groupedLib[g]||[]).length} 个</span></summary><div class="registered-category-content"><div class="strategies-grid">${cards}</div></div></details>`;
  }).join('');
}
}
const grid=document.getElementById('registered-strategies-grid');
const metaEl=document.getElementById('registered-strategy-meta');
if(!grid)return;
const filters=getRegisteredStrategyFilters();
const filteredStrategies=(state.strategies||[]).filter(s=>{
  const stype=String(s?.strategy_type||'');
  const cat=(STRATEGY_META[stype]?.cat)||'其他';
  const sState=String(s?.state||'').toLowerCase();
  if(filters.category && cat!==filters.category)return false;
  if(filters.state && sState!==filters.state)return false;
  if(filters.search){
    const symbols=Array.isArray(s?.symbols)?s.symbols:[];
    const searchBlob=[
      s?.name,
      stype,
      strategyTypeShortName(stype),
      cat,
      s?.timeframe,
      ...symbols,
      buildStrategyShortDisplayLabel(s),
    ].map(x=>String(x||'').toLowerCase()).join(' | ');
    if(!searchBlob.includes(filters.search))return false;
  }
  return true;
});
if(metaEl){
  const runningCount=(state.strategies||[]).filter(x=>String(x?.state||'')==='running').length;
  metaEl.textContent=`已注册 ${state.strategies.length} | 运行中 ${runningCount} | 当前显示 ${filteredStrategies.length} | 点击卡片在右侧编辑`;
}
if(!state.strategies.length){grid.innerHTML='<div class="list-item">暂无已注册策略</div>';return;}
if(!filteredStrategies.length){grid.innerHTML='<div class="list-item">没有匹配筛选条件的策略实例</div>';return;}
const typeCounts=filteredStrategies.reduce((m,s)=>{const k=String(s?.strategy_type||'未知');m[k]=(m[k]||0)+1;return m;},{});
const typeSeen={};
const grouped={};
filteredStrategies.forEach(s=>{const cat=(STRATEGY_META[s?.strategy_type]?.cat)||'其他';(grouped[cat]||(grouped[cat]=[])).push(s);});
const catOrder=['趋势','震荡','突破','均值回归','动量','反转','统计套利','套利','宏观','其他'];
grid.innerHTML=catOrder.filter(cat=>Array.isArray(grouped[cat])&&grouped[cat].length).map(cat=>{
  const cards=(grouped[cat]||[]).map(s=>{
const stype=String(s?.strategy_type||'未知');
typeSeen[stype]=(typeSeen[stype]||0)+1;
const r=s.runtime||{},a=Number(s.allocation||0),m=STRATEGY_META[s.strategy_type]||{cat:'其他'};
const uptime=fmtDurationSec(r.uptime_seconds||0),accountId=String(r.account_id||s.account_id||'main');
const isolated=Boolean(r.isolated_account),runnerAlive=Boolean(r.runner_alive);
const typeCount=Number(typeCounts[stype]||1),typeIndex=Number(typeSeen[stype]||1);
const shortLabel=buildStrategyShortDisplayLabel(s,typeIndex,typeCount);
const shortType=strategyTypeShortName(stype);
const symbolsArr=Array.isArray(s.symbols)?s.symbols:[];
const symbolFull=symbolsArr.length?symbolsArr.join(', '):'全部';
const symbolMain=symbolsArr.length?String(symbolsArr[0]).replace('/USDT','').replace('/USD',''):'全部';
const symbolText=symbolsArr.length>1?`${symbolMain} +${symbolsArr.length-1}`:symbolMain;
const active=String(state.selectedStrategyName||'')===String(s.name||'');
const pnlPerf=(state.summary?.strategy_performance||{})[s.name]||{};
const rp=Number(pnlPerf.return_pct);
const rpText=Number.isFinite(rp)?`${rp.toFixed(2)}%`:'--';
return `<div class="registered-strategy-card ${active?'active':''}" onclick="selectRegisteredStrategy('${esc(String(s.name||''))}')">
  <div class="topline">
    <div class="name" title="${esc(String(s.name||''))}">${esc(shortLabel)}</div>
    <span class="status-badge ${String(s.state||'')==='running'?'connected':''}">${mapState(s.state)}</span>
  </div>
  <div class="sub" title="${esc(String(s.name||''))}">${esc(shortType)} · 实例 ${esc(shortInstanceId(s.name))}</div>
  <div class="meta-grid">
    <div class="meta-chip"><span class="k">交易对</span><span class="v" title="${esc(symbolFull)}">${esc(symbolText)}</span></div>
    <div class="meta-chip"><span class="k">资金占比</span><span class="v">${a.toFixed(2)}</span></div>
    <div class="meta-chip"><span class="k">运行时长</span><span class="v">${esc(uptime)}</span></div>
    <div class="meta-chip"><span class="k">收益率</span><span class="v ${Number.isFinite(rp)?(rp>=0?'positive':'negative'):''}">${esc(rpText)}</span></div>
  </div>
  <div class="badges">
    <span title="run_count">run:${r.run_count||0}</span>
    <span title="signal_count">sig:${r.signal_count||0}</span>
    <span title="error_count">err:${r.error_count||0}</span>
    <span title="${isolated?'独立账户':'共享账户'}">${isolated?'独立':'共享'}:${esc(accountId)}</span>
    <span>${runnerAlive?'runner在线':'runner离线'}</span>
  </div>
</div>`;
  }).join('');
  return `<details class="registered-category-group" open>
    <summary>
      <span class="title">${esc(cat)}</span>
      <span class="count">${(grouped[cat]||[]).length} 个</span>
    </summary>
    <div class="registered-category-content">
      <div class="registered-strategy-grid">${cards}</div>
    </div>
  </details>`;
}).join('');
if(document.getElementById('backtest-compare-strategy-list') && typeof loadBacktestComparePickerSource==='function'){
  const src=backtestCompareCurrentSource();
  if(src==='registered'){
    loadBacktestComparePickerSource('registered',{preserveSelection:true,selectAll:false,useDefault:false}).catch(()=>{});
  }
}
}catch(e){console.error(e);}
}
async function registerStrategy(type){
try{
const profile={
PairsTradingStrategy:{exchange:'binance',timeframe:'1h',symbols:['BTC/USDT','ETH/USDT']},
CEXArbitrageStrategy:{exchange:'binance',timeframe:'5m',symbols:['BTC/USDT']},
TriangularArbitrageStrategy:{exchange:'binance',timeframe:'5m',symbols:['BTC/USDT']},
DEXArbitrageStrategy:{exchange:'binance',timeframe:'5m',symbols:['BTC/USDT']},
FlashLoanArbitrageStrategy:{exchange:'binance',timeframe:'5m',symbols:['BTC/USDT']},
MarketSentimentStrategy:{exchange:'binance',timeframe:'15m',symbols:['BTC/USDT']},
SocialSentimentStrategy:{exchange:'binance',timeframe:'15m',symbols:['BTC/USDT']},
FundFlowStrategy:{exchange:'binance',timeframe:'15m',symbols:['BTC/USDT']},
WhaleActivityStrategy:{exchange:'binance',timeframe:'15m',symbols:['BTC/USDT']},
}[type]||{exchange:'binance',timeframe:'15m',symbols:['BTC/USDT']};
const name=`${type}_${Date.now()}`;
await api('/strategies/register',{method:'POST',body:JSON.stringify({name,strategy_type:type,params:{},symbols:profile.symbols,timeframe:profile.timeframe,exchange:profile.exchange,allocation:.2})});
notify(`策略 ${type} 注册成功`);
await Promise.all([loadStrategies(),loadStrategySummary()]);
activateTab('strategies');
setTimeout(()=>openEditor(name).catch(()=>{}),80);
}catch(e){notify(`策略注册失败: ${e.message}`,true);}
}
async function cloneStrategyInstance(name){
try{
const info=await api(`/strategies/${encodeURIComponent(name)}`);
const baseType=String(info?.strategy_type||'').trim();
if(!baseType){notify('无法识别策略类型',true);return;}
const suffix=new Date().toISOString().replace(/[-:TZ.]/g,'').slice(8,14);
const cloneName=`${baseType}_${suffix}_${Math.floor(Math.random()*1000).toString().padStart(3,'0')}`;
const payload={
  name: cloneName,
  strategy_type: baseType,
  params: info?.params||{},
  symbols: Array.isArray(info?.symbols)?info.symbols:['BTC/USDT'],
  timeframe: String(info?.timeframe||'1h'),
  exchange: String(info?.exchange||'binance'),
  allocation: Number(info?.allocation??0.2),
  runtime_limit_minutes: (info?.runtime?.runtime_limit_minutes??null),
 };
 await api('/strategies/register',{method:'POST',body:JSON.stringify(payload)});
 notify(`已复制策略实例：${cloneName}`);
 await Promise.all([loadStrategies(),loadStrategySummary()]);
 activateTab('strategies');
 setTimeout(()=>openEditor(cloneName).catch(()=>{}),80);
}catch(e){notify(`复制策略实例失败: ${e.message}`,true);}
}
async function deleteStrategyInstance(name, ask=true){
try{
if(ask && !confirm(`确认删除策略实例？\n${name}`))return false;
await api(`/strategies/${encodeURIComponent(name)}`,{method:'DELETE'});
if(state.selectedStrategyName===name){
  state.selectedStrategyName='';
  const panel=document.getElementById('strategy-edit-panel');
  if(panel){panel.classList.remove('strategy-edit-active');panel.innerHTML='点击策略卡片后在此编辑';}
}
notify(`已删除策略实例: ${name}`);
await Promise.all([loadStrategies(),loadStrategySummary(),loadStrategyHealth()]);
return true;
}catch(e){notify(`删除策略实例失败: ${e.message}`,true);return false;}
}
async function clearAllRegisteredStrategies(){
try{
if(!state.strategies.length){notify('当前无已注册策略');return;}
if(!confirm(`确认一键清空全部已注册策略？\n共 ${state.strategies.length} 个实例，将先停止再删除。`))return;
try{await api('/strategies/stop-all',{method:'POST'});}catch{}
const names=(state.strategies||[]).map(s=>String(s.name||'')).filter(Boolean);
let ok=0,fail=0;
for(const n of names){
  try{await api(`/strategies/${encodeURIComponent(n)}`,{method:'DELETE'});ok++;}catch{fail++;}
}
state.selectedStrategyName='';
const panel=document.getElementById('strategy-edit-panel');
if(panel){panel.classList.remove('strategy-edit-active');panel.innerHTML='点击策略卡片后在此编辑';}
notify(`清空完成：成功 ${ok}，失败 ${fail}${fail?'（可重试）':''}`,fail>0);
await Promise.all([loadStrategies(),loadStrategySummary(),loadStrategyHealth()]);
}catch(e){notify(`一键清空失败: ${e.message}`,true);}
}
async function selectRegisteredStrategy(name){
state.selectedStrategyName=String(name||'');
activateTab('strategies');
await openEditor(state.selectedStrategyName);
await loadStrategies();
}
async function saveAllocation(name){const i=document.querySelector(`input[data-alloc='${name}']`);if(!i)return;try{await api(`/strategies/${name}/allocation`,{method:'PUT',body:JSON.stringify({allocation:Number(i.value||0)})});notify(`策略 ${name} 资金占比已更新`);await Promise.all([loadStrategies(),loadStrategySummary()]);}catch(e){notify(`更新资金占比失败: ${e.message}`,true);}}
async function toggleStrategy(name,st){const act=st==='running'?'stop':'start';try{await api(`/strategies/${name}/${act}`,{method:'POST'});notify(`策略已${act==='start'?'启动':'停止'}`);await Promise.all([loadStrategies(),loadStrategySummary()]);}catch(e){notify(`策略${act}失败: ${e.message}`,true);}}

async function loadStrategySummary(){
try{
const d=await api('/strategies/summary?limit=20');
state.summary=d;
const running=d.running||[],signals=(d.recent_signals||[]).slice(0,12),stale=(d.stale_running||[]);
const perf=d.strategy_performance||{};
const a=document.getElementById('active-strategies'),r=document.getElementById('recent-signals'),rt=document.getElementById('strategy-runtime-tbody');
const meta=document.getElementById('active-strategies-meta');
if(meta){
const registered=Number(d.registered_count||0),runningCount=Number(d.running_count||running.length),idle=Number(d.idle_count||0),paused=Number(d.paused_count||0),stopped=Number(d.stopped_count||0);
meta.innerHTML=`<span>已注册 ${registered} | 运行中 ${runningCount} | 空闲 ${idle}</span><span>暂停 ${paused} / 停止 ${stopped}</span>`;
}
if(a){
const staleTip=stale.length?`<div class="list-item"><span style="color:#ffb15f;">运行异常: ${stale.map(x=>x.strategy).join(', ')}</span><span>建议检查数据/连接</span></div>`:'';
a.innerHTML=(running.map(s=>{const p=perf[s.name]||{},rt=s.runtime||{};const rp=Number(p.return_pct),dd=Number(p.max_drawdown_pct),vv=Number(p.variance),up=fmtDurationSec(rt.uptime_seconds||0);const rpTxt=Number.isFinite(rp)?`${rp.toFixed(2)}%`:'--';const ddTxt=Number.isFinite(dd)?`${dd.toFixed(2)}%`:'--';const varTxt=Number.isFinite(vv)?vv.toExponential(2):'--';const acct=esc(rt.account_id||s.account_id||'main');const modeTxt=rt.isolated_account?'独立':'共享';return `<div class="list-item"><span>${s.name} (${s.strategy_type}) | 收益率 ${rpTxt} | 回撤 ${ddTxt} | 方差 ${varTxt} | 运行 ${up} | ${modeTxt}:${acct} ${s.last_run_at?`· ${new Date(s.last_run_at).toLocaleTimeString('zh-CN')}`:''}</span><span class="status-badge connected">运行中</span></div>`;}).join('')||'<div class="list-item">暂无运行中策略</div>')+staleTip;
}
if(r)r.innerHTML=signals.length?signals.map(s=>`<div class="list-item"><span>${s.strategy} | ${s.symbol} | ${s.signal_type.toUpperCase()}</span><span>${new Date(s.timestamp).toLocaleTimeString('zh-CN')}</span></div>`).join(''):`<div class="list-item"><span>${running.length?`实时刷新中（${d.refresh_hint_seconds||5}秒）暂无新信号，可能是策略条件未触发`:'暂无近期信号'}</span><span>${new Date().toLocaleTimeString('zh-CN')}</span></div>`;
if(rt){
rt.innerHTML=running.length?running.map(s=>{const p=perf[s.name]||{},ri=s.runtime||{};const rp=Number(p.return_pct),dd=Number(p.max_drawdown_pct),realized=Number(p.realized_pnl),unrealized=Number(p.unrealized_pnl),absPnl=(Number.isFinite(realized)?realized:0)+(Number.isFinite(unrealized)?unrealized:0),lu=p.last_update;const runtimeTxt=fmtDurationSec(ri.uptime_seconds||0);const lastRunTxt=s.last_run_at?new Date(s.last_run_at).toLocaleString('zh-CN'):'-';const rpTxt=Number.isFinite(rp)?`${rp.toFixed(2)}%`:'--';const ddTxt=Number.isFinite(dd)?`${dd.toFixed(2)}%`:'--';const absTxt=Number.isFinite(absPnl)?fmt(absPnl):'--';const rpCls=Number.isFinite(rp)?(rp>=0?'positive':'negative'):'';const absCls=Number.isFinite(absPnl)?(absPnl>=0?'positive':'negative'):'';const stype=s.strategy_type||s.name;const meta=STRATEGY_META[stype]||{};const desc=meta.desc||s.description||stype;const cat=meta.cat||'';return`<tr><td>${s.name}</td><td style="font-size:12px;color:#9fb1c9;max-width:200px;">${cat?`[${cat}] `:''}${esc(desc)}</td><td class="${rpCls}">${rpTxt}</td><td>${ddTxt}</td><td class="${absCls}">${absTxt}</td><td>${runtimeTxt}</td><td>${lastRunTxt}</td><td>${lu?new Date(lu).toLocaleString('zh-CN'):'-'}</td></tr>`;}).join(''):'<tr><td colspan="8">暂无运行中策略数据</td></tr>';
}
renderStrategyHealthAlerts(d,state.strategyHealth);
}catch(e){console.error(e);}
}
function renderStrategyHealthAlerts(summary,health){
const box=document.getElementById('strategy-health-alerts');if(!box)return;
const stale=(summary?.stale_running||[]);const staleCount=Number(summary?.stale_running_count||stale.length||0);const runningCount=Number(summary?.running_count||0);
const monitor=health||{};const lastCheck=monitor?.last_check_at;const lastAlert=monitor?.last_alert_at;const lastErr=monitor?.last_error;
if(staleCount<=0){
box.innerHTML=`<div class="list-item"><span>状态</span><span class="status-badge connected">健康</span></div><div class="list-item"><span>运行中策略</span><span>${runningCount}</span></div><div class="list-item"><span>最近检查</span><span>${lastCheck?new Date(lastCheck).toLocaleTimeString('zh-CN'):'--'}</span></div>`;
state.lastHealthAlertKey='';
return;
}
const staleRows=stale.slice(0,6).map(x=>{const lag=(x&&x.lag_seconds!==undefined&&x.lag_seconds!==null)?`${x.lag_seconds}s`:'--';return `<div class="list-item"><span>${x.strategy||'未知策略'} (${x.timeframe||'-'})</span><span style="color:#ffb15f;">延迟 ${lag}</span></div>`;}).join('');
box.innerHTML=`<div class="list-item"><span>状态</span><span class="status-badge" style="background:rgba(255,177,95,.15);color:#ffb15f;border-color:rgba(255,177,95,.35);">告警</span></div><div class="list-item"><span>异常策略数</span><span>${staleCount}</span></div>${staleRows||''}<div class="list-item"><span>最近告警</span><span>${lastAlert?new Date(lastAlert).toLocaleTimeString('zh-CN'):'--'}</span></div>${lastErr?`<div class="list-item"><span>监控错误</span><span style="color:#ff9b9b;">${esc(lastErr)}</span></div>`:''}`;
const alertKey=`${staleCount}|${stale.map(x=>x.strategy).join(',')}`;
if(alertKey!==state.lastHealthAlertKey){state.lastHealthAlertKey=alertKey;notify(`【策略健康告警】异常策略 ${staleCount} 个`,true);}
}
function pushRealtimeSignal(sig){try{if(!sig)return;const item={strategy:sig.strategy_name||sig.strategy||'未知策略',symbol:sig.symbol||'-',signal_type:String(sig.signal_type||'').toLowerCase(),timestamp:sig.timestamp||new Date().toISOString()};const cur=state.summary?.recent_signals||[];const key=`${item.strategy}|${item.symbol}|${item.signal_type}|${item.timestamp}`;const map=new Map();[item,...cur].forEach(x=>{const k=`${x.strategy||x.strategy_name}|${x.symbol}|${String(x.signal_type||'').toLowerCase()}|${x.timestamp}`;if(!map.has(k))map.set(k,x);});state.summary.recent_signals=[...map.values()].slice(0,20);const r=document.getElementById('recent-signals');if(r){r.innerHTML=state.summary.recent_signals.slice(0,12).map(s=>`<div class=\"list-item\"><span>${s.strategy||s.strategy_name} | ${s.symbol} | ${String(s.signal_type||'').toUpperCase()}</span><span>${new Date(s.timestamp).toLocaleTimeString('zh-CN')}</span></div>`).join('');}}catch(e){console.error(e);}}
async function loadStrategyHealth(){
const out=document.getElementById('strategy-health-output');
if(!out)return;
const paths=['/strategies/health/monitor','/strategies/health','/strategies/health-monitor','/strategies/runtime'];
let lastErr='未知';
for(const p of paths){
try{
const d=await api(p);
if(d&&Object.prototype.hasOwnProperty.call(d,'running')&&Object.prototype.hasOwnProperty.call(d,'last_result')){
state.strategyHealth=d;
renderStrategyHealthAlerts(state.summary||{},d);
const lr=d.last_result||{};
out.textContent=`策略监控: ${d.running?'运行中':'已停止'}\n检查间隔: ${d.check_interval_seconds||'-'}s\n冷却时间: ${d.alert_cooldown_seconds||'-'}s\n最近检查: ${d.last_check_at||'-'}\n最近告警: ${d.last_alert_at||'-'}\n异常策略数: ${lr.stale_running_count??'-'}\n异常策略: ${(lr.stale_strategies||[]).join(', ')||'无'}\n最近错误: ${d.last_error||'无'}`;
}else{
out.textContent=JSON.stringify(d,null,2);
}
return;
}catch(e){
lastErr=e.message;
}
}
try{
const s=await api('/strategies/summary?limit=20');
renderStrategyHealthAlerts(s,state.strategyHealth);
out.textContent=JSON.stringify({fallback:'summary',running_count:(s.running||[]).length,stale_running:s.stale_running||[],runtime:s.runtime||{},timestamp:s.timestamp||new Date().toISOString(),note:'健康监控接口不可用，已降级显示策略摘要'},null,2);
}catch{
out.textContent=`加载策略健康状态失败: ${lastErr}`;
}
}
function bindStrategyOps(){
const sAll=document.getElementById('btn-strategy-start-all'),pAll=document.getElementById('btn-strategy-stop-all'),chk=document.getElementById('btn-strategy-health-check'),out=document.getElementById('strategy-health-output');
const rr=document.getElementById('btn-registered-refresh'),rs=document.getElementById('btn-registered-stop-all'),rc=document.getElementById('btn-registered-clear-all');
const fSearch=document.getElementById('registered-strategy-search'),fCat=document.getElementById('registered-strategy-cat-filter'),fState=document.getElementById('registered-strategy-state-filter');
let filterTimer=null;
const queueFilterRender=()=>{ if(filterTimer)clearTimeout(filterTimer); filterTimer=setTimeout(()=>{loadStrategies().catch(()=>{});},120); };
if(sAll)sAll.onclick=async()=>{
try{
const res=await api('/strategies/start-all',{method:'POST'});
const autoCount=(res?.auto_registered||[]).length||0;
notify(autoCount?`已启动全部策略（自动注册 ${autoCount} 个）`:'已启动全部策略');
await Promise.all([loadStrategies(),loadStrategySummary(),loadStrategyHealth()]);
}catch(e){notify(`启动全部失败: ${e.message}`,true);}
};
if(pAll)pAll.onclick=async()=>{try{await api('/strategies/stop-all',{method:'POST'});notify('已停止全部策略');await Promise.all([loadStrategies(),loadStrategySummary(),loadStrategyHealth()]);}catch(e){notify(`停止全部失败: ${e.message}`,true);}};
if(chk)chk.onclick=async()=>{try{const r=await api('/strategies/health/check',{method:'POST'});if(out)out.textContent=JSON.stringify(r,null,2);notify('策略健康检查完成');await Promise.all([loadStrategySummary(),loadStrategyHealth(),loadNotificationCenter()]);}catch(e){if(out)out.textContent=`健康检查失败: ${e.message}`;notify(`健康检查失败: ${e.message}`,true);}};
if(rr)rr.onclick=()=>Promise.all([loadStrategies(),loadStrategySummary()]).catch(()=>{});
if(rs)rs.onclick=async()=>{try{await api('/strategies/stop-all',{method:'POST'});notify('已停止全部策略');await Promise.all([loadStrategies(),loadStrategySummary(),loadStrategyHealth()]);}catch(e){notify(`停止全部失败: ${e.message}`,true);}};
if(rc)rc.onclick=clearAllRegisteredStrategies;
if(fSearch)fSearch.addEventListener('input',queueFilterRender);
if(fCat)fCat.addEventListener('change',queueFilterRender);
if(fState)fState.addEventListener('change',queueFilterRender);
}

async function openEditor(name){
const panel=document.getElementById('strategy-edit-panel');if(!panel)return;
try{
state.selectedStrategyName=String(name||'');
const [info,schema,sizing]=await Promise.all([
  api(`/strategies/${name}`),
  api(`/strategies/${name}/params/schema`),
  api(`/strategies/${name}/sizing-preview`).catch(()=>null),
]);
const runtime=info.runtime||{};
const currentSymbols=Array.isArray(info.symbols)&&info.symbols.length?info.symbols:['BTC/USDT'];
const tfOpts=['1s','5s','10s','30s','1m','5m','15m','30m','1h','4h','1d'];
const tfHtml=tfOpts.map(tf=>`<option value="${tf}" ${String(info.timeframe||'')===tf?'selected':''}>${tf}</option>`).join('');
const fields=(schema.params||[]).map(p=>{
if(p.type==='boolean')return `<div class="form-group"><label>${p.name}</label><select data-k="${p.name}" data-t="boolean"><option value="true" ${p.default?'selected':''}>true</option><option value="false" ${!p.default?'selected':''}>false</option></select></div>`;
if(p.type==='json')return `<div class="form-group"><label>${p.name} (JSON)</label><textarea data-k="${p.name}" data-t="json">${JSON.stringify(p.default||{},null,2)}</textarea></div>`;
return `<div class="form-group"><label>${p.name}</label><input data-k="${p.name}" data-t="${p.type||'string'}" type="${p.type==='integer'||p.type==='number'?'number':'text'}" value="${p.default??''}" ${p.step?`step="${p.step}"`:''} ${p.min!==undefined?`min="${p.min}"`:''} ${p.max!==undefined?`max="${p.max}"`:''}></div>`;
}).join('');
const canApplyBestOpt=String(backtestUIState?.lastOptimize?.strategy||'')===String(info.strategy_type||'');
const sizingStatus=String(sizing?.status||'');
const sizingColor=sizingStatus==='ok'?'#3fb950':(sizingStatus==='blocked'?'#f85149':'#f0b429');
const sizingResult=sizingStatus==='ok'?'当前可正常下单':(sizingStatus==='blocked'?'当前会被最小下单门槛拦截':'当前预估数据不足，暂无法判断');
const sizingHtml=sizing?`<div class="form-group" style="margin-top:10px;"><label>下单预估</label><div class="list-item"><span>当前价格 / 账户权益</span><span>${Number(sizing.price||0).toFixed(4)} / ${Number(sizing.account_equity||0).toFixed(2)} USDT</span></div><div class="list-item"><span>价格来源</span><span>${esc(String(sizing.price_source||'unavailable'))}</span></div><div class="list-item"><span>分配资金 / 单笔上限</span><span>${Number(sizing.allocation_cap||0).toFixed(2)} / ${Number(sizing.risk_single_cap||0).toFixed(2)} USDT</span></div><div class="list-item"><span>当前可用名义金额</span><span>${Number(sizing.available_notional||0).toFixed(2)} USDT</span></div><div class="list-item"><span>最小合法数量 / 名义金额</span><span>${fmtQtyPreview(sizing.min_legal_qty||0)} / ${Number(sizing.min_legal_notional||0).toFixed(2)} USDT</span></div><div class="list-item"><span>结果</span><span style="color:${sizingColor};">${sizingResult}</span></div><div class="list-item"><span>说明</span><span>${esc(sizing.note||'-')}</span></div></div>`:'';
panel.innerHTML=`<div class="form-group"><label>策略: ${info.name} (${info.strategy_type})</label><div class="list-item"><span>状态</span><span>${mapState(info.state)}</span></div><div class="list-item"><span>周期</span><span>${esc(info.timeframe||'-')}</span></div><div class="list-item"><span>交易对</span><span>${esc(currentSymbols.join(', '))}</span></div><div class="list-item"><span>最近运行</span><span>${info.last_run_at?new Date(info.last_run_at).toLocaleString('zh-CN'):'-'}</span></div><div class="list-item"><span>运行时长限制</span><span>${runtime.runtime_limit_minutes?`${runtime.runtime_limit_minutes} 分钟`:'不限时'}${runtime.remaining_seconds!==undefined&&runtime.remaining_seconds!==null?` | 剩余 ${fmtDurationSec(runtime.remaining_seconds)}`:''}</span></div></div>${sizingHtml}<div class="inline-actions" style="margin-top:4px;"><button class="btn btn-primary btn-sm" id="edit-toggle">${info.state==='running'?'停止策略':'启动策略'}</button><button class="btn btn-primary btn-sm" id="edit-clone">复制新实例</button><button class="btn btn-danger btn-sm" id="edit-delete">删除实例</button><button class="btn btn-primary btn-sm" id="edit-cmp">刷新对比</button>${canApplyBestOpt?'<button class="btn btn-primary btn-sm" id="edit-apply-best-opt">应用最近优化最佳参数</button>':''}</div><div class="param-grid"><div class="form-group"><label>策略周期（timeframe）</label><select id="edit-timeframe">${tfHtml}</select></div><div class="form-group"><label>交易对（逗号分隔，可多币）</label><input id="edit-symbols" type="text" value="${esc(currentSymbols.join(', '))}" placeholder="例如 ETH/USDT 或 BTC/USDT,ETH/USDT"></div><div class="form-group"><label>策略运行时长（分钟，0=不限）</label><input id="edit-runtime-min" type="number" min="0" max="10080" step="1" value="${Number(runtime.runtime_limit_minutes||0)}"></div><div class="form-group"><label>资金占比 (0~1)</label><input id="edit-alloc" type="number" min="0" max="1" step="0.01" value="${Number(info.allocation||0).toFixed(2)}"></div></div><div class="param-grid">${fields||'<div class="list-item">该策略无可编辑参数</div>'}</div><div class="inline-actions" style="margin-top:10px;"><button class="btn btn-primary btn-sm" id="edit-save">保存参数</button><button class="btn btn-primary btn-sm" id="edit-save-as">另存为新实例（当前编辑值）</button></div><pre id="editor-compare-output" class="output-box">点击“刷新对比”查看实盘与回测差异</pre>`;
panel.classList.add('strategy-edit-active');
panel.dataset.strategyName=String(info.name||name||'');
panel.dataset.strategyType=String(info.strategy_type||'');
const collectEditorDraft=()=>{
  const params={};
  panel.querySelectorAll('[data-k]').forEach(i=>{
    const k=i.getAttribute('data-k'),t=i.getAttribute('data-t'),v=i.value;
    if(!k)return;
    if(t==='integer')params[k]=parseInt(v,10);
    else if(t==='number')params[k]=parseFloat(v);
    else if(t==='boolean')params[k]=v==='true';
    else if(t==='json')params[k]=v?JSON.parse(v):{};
    else params[k]=v;
  });
  const symbols=String(document.getElementById('edit-symbols')?.value||'')
    .split(',')
    .map(v=>String(v||'').trim().toUpperCase())
    .filter(Boolean);
  const runtimeMin=Math.max(0,parseInt(document.getElementById('edit-runtime-min')?.value||'0',10)||0);
  return{
    params,
    symbols:symbols.length?symbols:['BTC/USDT'],
    timeframe:String(document.getElementById('edit-timeframe')?.value||info.timeframe||'1h'),
    runtime_limit_minutes:runtimeMin,
    allocation:Math.max(0,Math.min(1,Number(document.getElementById('edit-alloc')?.value||info.allocation||0))),
  };
};
document.getElementById('edit-save').onclick=async()=>{
try{
const draft=collectEditorDraft();
await api(`/strategies/${name}/config`,{method:'PUT',body:JSON.stringify({timeframe:draft.timeframe,symbols:draft.symbols,runtime_limit_minutes:draft.runtime_limit_minutes})});
await api(`/strategies/${name}/params`,{method:'PUT',body:JSON.stringify({params:draft.params})});
await api(`/strategies/${name}/allocation`,{method:'PUT',body:JSON.stringify({allocation:draft.allocation})});
notify(`策略 ${name} 参数已更新`);
await Promise.all([loadStrategies(),loadStrategySummary()]);
await openEditor(name);
}catch(e){notify(`参数更新失败: ${e.message}`,true);}
};
const saveAsBtn=document.getElementById('edit-save-as');
if(saveAsBtn)saveAsBtn.onclick=async()=>{
  try{
    const draft=collectEditorDraft();
    const suffix=prompt('新实例后缀（可选）', '') ?? '';
    const newName=buildStrategyInstanceName(info.strategy_type,{prefix:'fork',suffix});
    const payload={
      name:newName,
      strategy_type:String(info.strategy_type||'').trim(),
      params:draft.params,
      symbols:draft.symbols,
      timeframe:draft.timeframe,
      exchange:String(info.exchange||'binance'),
      allocation:draft.allocation,
      runtime_limit_minutes:draft.runtime_limit_minutes||null,
    };
    await api('/strategies/register',{method:'POST',body:JSON.stringify(payload)});
    notify(`已另存为新实例：${newName}`);
    await Promise.all([loadStrategies(),loadStrategySummary()]);
    activateTab('strategies');
    setTimeout(()=>openEditor(newName).catch(()=>{}),80);
  }catch(e){notify(`另存为新实例失败: ${e.message}`,true);}
};
document.getElementById('edit-cmp').onclick=()=>compareLive(name);
document.getElementById('edit-toggle').onclick=()=>toggleStrategy(name,info.state);
document.getElementById('edit-clone').onclick=()=>cloneStrategyInstance(name);
document.getElementById('edit-delete').onclick=()=>deleteStrategyInstance(name,true);
if(canApplyBestOpt && document.getElementById('edit-apply-best-opt')){
  document.getElementById('edit-apply-best-opt').onclick=async()=>{
    const prevSel=state.selectedStrategyName;
    state.selectedStrategyName=name;
    await applyBestOptimizeParamsToStrategyEditor();
    state.selectedStrategyName=prevSel||name;
  };
}
loadStrategies().catch(()=>{});
}catch(e){panel.classList.remove('strategy-edit-active');delete panel.dataset.strategyName;delete panel.dataset.strategyType;panel.innerHTML=`<div class="list-item">加载策略参数失败: ${e.message}</div>`;}
}
async function compareLive(name){try{const d=await api(`/strategies/${name}/live-vs-backtest`);(document.getElementById('editor-compare-output')||document.getElementById('backtest-extra-output')).textContent=JSON.stringify(d,null,2);notify(`策略 ${name} 实盘/回测对比已刷新`);}catch(e){notify(`策略对比失败: ${e.message}`,true);}}

const marketDataState={exchange:'',symbol:'',timeframe:'',limit:1200,bars:[],isLoading:false,isLoadingLeft:false,isLoadingRight:false,lastRange:null,realtimeTimer:null,chartBound:false,realtimeInFlight:false,lastRealtimePollAt:0,lastChartKey:'',loadSeq:0};
const autoDataOpsState={downloadAt:new Map(),repairAt:new Map(),lastHintAt:0};
const MARKET_MAX_BARS=14000;
function klinePad2(n){return String(Math.max(0,Number(n)||0)).padStart(2,'0');}
function klineLocalIso(ms){
const d=new Date(ms);
if(!Number.isFinite(d.getTime()))return'';
return `${d.getFullYear()}-${klinePad2(d.getMonth()+1)}-${klinePad2(d.getDate())}T${klinePad2(d.getHours())}:${klinePad2(d.getMinutes())}:${klinePad2(d.getSeconds())}`;
}
function klineToDate(value){
if(value instanceof Date)return Number.isFinite(value.getTime())?value:null;
if(typeof value==='number'){const d=new Date(value>1e12?value:value*1000);return Number.isFinite(d.getTime())?d:null;}
const raw=String(value??'').trim();
if(!raw)return null;
const text=raw.replace(' ','T');
const d=new Date(text);
return Number.isFinite(d.getTime())?d:null;
}
function klineToMs(value){const d=klineToDate(value);return d?d.getTime():NaN;}
function normalizeKlineBar(bar){
if(!bar||typeof bar!=='object')return null;
const ms=klineToMs(bar.timestamp);
if(!Number.isFinite(ms))return null;
return {...bar,timestamp:klineLocalIso(ms)};
}
function timeframeSeconds(tf){if(!tf||tf.length<2)return 60;const unit=tf.slice(-1),val=Math.max(1,parseInt(tf.slice(0,-1),10)||1);if(unit==='s')return val;if(unit==='m')return val*60;if(unit==='h')return val*3600;if(unit==='d')return val*86400;if(unit==='w')return val*86400*7;if(unit==='M')return val*86400*30;return 60;}
function isSubMinuteTf(tf){return Math.max(1,timeframeSeconds(tf))<60;}
function klineRealtimeIntervalMs(tf){const sec=Math.max(1,timeframeSeconds(tf));if(sec<=10)return 2000;if(sec<60)return 3000;if(sec<=300)return 4000;return 5000;}
function isDataTabActive(){return !!document.getElementById('data')?.classList.contains('active');}
function mergeBars(base,incoming){
const m=new Map();
(base||[]).forEach(b=>{const nb=normalizeKlineBar(b);const ms=klineToMs(nb?.timestamp);if(Number.isFinite(ms)&&nb)m.set(String(ms),nb);});
(incoming||[]).forEach(b=>{const nb=normalizeKlineBar(b);const ms=klineToMs(nb?.timestamp);if(Number.isFinite(ms)&&nb)m.set(String(ms),nb);});
return [...m.entries()].sort((a,b)=>Number(a[0])-Number(b[0])).map(([,row])=>row);
}
function cropBars(bars,limit=MARKET_MAX_BARS){if(!bars?.length)return[];if(bars.length<=limit)return bars;return bars.slice(bars.length-limit);}
function cooldownPass(map,key,cooldownMs){const now=Date.now(),last=Number(map.get(key)||0);if(now-last<cooldownMs)return false;map.set(key,now);return true;}
function recommendDownloadDays(tf){const t=String(tf||'1h');if(t==='1s')return 365;if(t==='5s'||t==='10s'||t==='30s')return 120;if(t==='1m')return 365;if(t==='5m'||t==='15m'||t==='30m')return 540;if(t==='1h'||t==='4h')return 900;if(t==='1d'||t==='1w'||t==='1M')return 1200;return 365;}
function hasLargeGap(bars,tf){
const sec=Math.max(1,timeframeSeconds(tf));
if(!bars||bars.length<3)return false;
for(let i=1;i<bars.length;i+=1){
const prev=klineToMs(bars[i-1]?.timestamp),cur=klineToMs(bars[i]?.timestamp);
if(!Number.isFinite(prev)||!Number.isFinite(cur))continue;
if(cur-prev>sec*1000*2.2)return true;
}
return false;
}
function inferBackfillRangeFromBars(bars,tf){
const arr=Array.isArray(bars)?bars:[];
if(!arr.length)return{};
let startMs=NaN,endMs=NaN;
if(marketDataState.lastRange?.start&&marketDataState.lastRange?.end){
  startMs=toMs(marketDataState.lastRange.start);
  endMs=toMs(marketDataState.lastRange.end);
}
if(!Number.isFinite(startMs)||!Number.isFinite(endMs)){
  startMs=klineToMs(arr[0]?.timestamp);
  endMs=klineToMs(arr[arr.length-1]?.timestamp);
}
if(!Number.isFinite(startMs)||!Number.isFinite(endMs)||endMs<=startMs)return{};
const padMs=Math.max(60_000, timeframeSeconds(tf)*1000*120);
return{
  startTime:new Date(Math.max(0,startMs-padMs)).toISOString(),
  endTime:new Date(endMs+padMs).toISOString(),
};
}
async function autoBackfillData({exchange,symbol,timeframe,reason='auto',startTime=null,endTime=null}){
const tf=String(timeframe||'');
const isSubMinute=(tf.endsWith('s') && tf!=='1s');
const requestTf=isSubMinute?'1s':tf;
const key=`${exchange}|${symbol}|${requestTf}|${reason}`;
const isSecond=String(requestTf||'').endsWith('s');
if(!cooldownPass(autoDataOpsState.downloadAt,key,isSecond?6*60*1000:3*60*1000))return false;
try{
const parts=[
  `exchange=${encodeURIComponent(exchange)}`,
  `symbol=${encodeURIComponent(symbol)}`,
  `timeframe=${encodeURIComponent(requestTf)}`,
];
const rangeStart=startTime?String(startTime):'';
const rangeEnd=endTime?String(endTime):'';
if(rangeStart||rangeEnd){
  if(rangeStart)parts.push(`start_time=${encodeURIComponent(rangeStart)}`);
  if(rangeEnd)parts.push(`end_time=${encodeURIComponent(rangeEnd)}`);
}else{
  const days=recommendDownloadDays(requestTf);
  parts.push(`days=${days}`);
}
const r=await api(`/data/download?${parts.join('&')}`,{method:'POST',timeoutMs:isSecond?65000:30000});
if(!r?.async_task?.task_id){
  const repairKey=`${exchange}|${symbol}|${requestTf}|repair`;
  if(cooldownPass(autoDataOpsState.repairAt,repairKey,2*60*1000)){
    try{
      await api(`/data/integrity/repair?exchange=${encodeURIComponent(exchange)}&symbol=${encodeURIComponent(symbol)}&timeframe=${encodeURIComponent(requestTf)}`,{method:'POST',timeoutMs:35000});
    }catch{}
  }
}
const now=Date.now();
if(now-autoDataOpsState.lastHintAt>4500){
  autoDataOpsState.lastHintAt=now;
  const tfHint=(requestTf!==tf)?`${requestTf}基础数据（用于${tf}）`:requestTf;
  notify(r?.async_task?.task_id?`后台补数任务已启动: ${r.async_task.task_id}`:`已自动补全 ${symbol} ${tfHint} 数据`);
}
return true;
}catch(e){console.error('autoBackfillData failed',e);return false;}}
async function fetchKlinesChunk({exchange,symbol,timeframe,limit,startTime,endTime,align='tail',timeoutMs}){let u=`/data/klines?exchange=${exchange}&symbol=${encodeURIComponent(symbol)}&timeframe=${timeframe}&limit=${limit}&align=${align}`;if(startTime)u+=`&start_time=${encodeURIComponent(startTime)}`;if(endTime)u+=`&end_time=${encodeURIComponent(endTime)}`;const r=await api(u,{timeoutMs:Math.max(3000,Number(timeoutMs||22000))});return r.data||[];}
function renderKlineChart(preserveRange=true){
const c=document.getElementById('candlestick-chart');
if(!c)return;
const bars=marketDataState.bars||[];
if(!bars.length){c.innerHTML='<p style="color:#8b949e;text-align:center;padding:50px;">暂无数据，系统会自动后台补数后重试。</p>';return;}
if(typeof Plotly==='undefined'){c.innerHTML='<p style="color:#8b949e;text-align:center;padding:50px;">图表库未加载，K线图暂不可用。</p>';return;}
const rows=bars.map(d=>({timestamp:klineToDate(d.timestamp),open:+d.open,high:+d.high,low:+d.low,close:+d.close,volume:+d.volume||0})).filter(d=>d.timestamp&&Number.isFinite(d.open)&&Number.isFinite(d.high)&&Number.isFinite(d.low)&&Number.isFinite(d.close));
if(!rows.length){c.innerHTML='<p style="color:#8b949e;text-align:center;padding:50px;">时间数据异常，无法渲染K线。</p>';return;}
const chartKey=`${marketDataState.exchange}|${marketDataState.symbol}|${marketDataState.timeframe}`;
const chartChanged=marketDataState.lastChartKey!==chartKey;
if(chartChanged){
try{Plotly.purge(c);}catch{}
marketDataState.chartBound=false;
marketDataState.lastRange=null;
marketDataState.lastChartKey=chartKey;
}
const x=rows.map(d=>d.timestamp),o=rows.map(d=>d.open),h=rows.map(d=>d.high),l=rows.map(d=>d.low),cl=rows.map(d=>d.close),v=rows.map(d=>d.volume),vc=rows.map(d=>d.close>=d.open?'#1f9d63':'#d9534f');
const minLow=Math.min(...l);
const maxHigh=Math.max(...h);
const priceSpan=Math.max(Math.abs(maxHigh-minLow), Math.abs(maxHigh||0)*0.002, 1e-8);
const pricePad=priceSpan*0.08;
const resetView=!preserveRange||chartChanged;
const layout={paper_bgcolor:'#111723',plot_bgcolor:'#111723',font:{color:'#d7dde8'},margin:{l:50,r:62,t:10,b:28},showlegend:false,dragmode:'pan',uirevision:chartKey,xaxis:plotlyTimeAxis({domain:[0,1],anchor:'y',rangeslider:{visible:false}}),yaxis:{domain:[.28,1],side:'right',showgrid:true,gridcolor:'#283242',automargin:true,autorange:resetView,range:resetView?[minLow-pricePad,maxHigh+pricePad]:undefined},xaxis2:plotlyTimeAxis({domain:[0,1],anchor:'y2',matches:'x'}),yaxis2:{domain:[0,.22],side:'right',showgrid:true,gridcolor:'#283242',automargin:true,autorange:true},hovermode:'x unified'};
if(!resetView&&marketDataState.lastRange?.start&&marketDataState.lastRange?.end){layout.xaxis.range=[marketDataState.lastRange.start,marketDataState.lastRange.end];}
Plotly.react(c,[{type:'candlestick',x,open:o,high:h,low:l,close:cl,increasing:{line:{color:'#1f9d63'}},decreasing:{line:{color:'#d9534f'}},xaxis:'x',yaxis:'y'},{type:'bar',x,y:v,marker:{color:vc,opacity:.7},xaxis:'x2',yaxis:'y2'}],layout,{responsive:true,scrollZoom:true,displaylogo:false,modeBarButtonsToAdd:['drawline','drawopenpath','drawrect','eraseshape'],modeBarButtonsToRemove:['lasso2d','select2d']});
schedulePlotlyResize(document.getElementById('data')||document);
}
function resetKlineChartForSwitch(message='正在切换币种并加载新行情...'){
const c=document.getElementById('candlestick-chart');
marketDataState.bars=[];
marketDataState.chartBound=false;
marketDataState.lastRange=null;
if(!c)return;
try{if(typeof Plotly!=='undefined')Plotly.purge(c);}catch{}
c.innerHTML=`<p style="color:#8b949e;text-align:center;padding:50px;">${esc(message)}</p>`;
}
async function loadMoreLeftByViewport(){
if(marketDataState.isLoadingLeft||marketDataState.isLoading)return;
const bars=marketDataState.bars||[];
if(!bars.length)return;
marketDataState.isLoadingLeft=true;
try{
const firstMs=klineToMs(bars[0]?.timestamp);
if(!Number.isFinite(firstMs)){marketDataState.isLoadingLeft=false;return;}
const endTime=new Date(firstMs-1000).toISOString();
const chunk=await fetchKlinesChunk({exchange:marketDataState.exchange,symbol:marketDataState.symbol,timeframe:marketDataState.timeframe,limit:marketDataState.limit,endTime,align:'tail',timeoutMs:18000});
if(chunk.length){marketDataState.bars=cropBars(mergeBars(chunk,marketDataState.bars));renderKlineChart(true);}else{
  const tfSec=timeframeSeconds(marketDataState.timeframe);
  const spanMs=Math.max(10*60*1000, Math.min(6*3600*1000, marketDataState.limit*tfSec*1000));
  await autoBackfillData({
    exchange:marketDataState.exchange,symbol:marketDataState.symbol,timeframe:marketDataState.timeframe,reason:'left-edge',
    startTime:new Date(Math.max(0,firstMs-spanMs)).toISOString(),
    endTime:new Date(firstMs+tfSec*1000).toISOString(),
  });
}
}catch(e){console.error(e);}
marketDataState.isLoadingLeft=false;
}
async function loadMoreRightByViewport(){
if(marketDataState.isLoadingRight||marketDataState.isLoading)return;
const bars=marketDataState.bars||[];
if(!bars.length)return;
marketDataState.isLoadingRight=true;
try{
const lastMs=klineToMs(bars[bars.length-1]?.timestamp);
if(!Number.isFinite(lastMs)){marketDataState.isLoadingRight=false;return;}
const startTime=new Date(lastMs+1000).toISOString();
const chunk=await fetchKlinesChunk({exchange:marketDataState.exchange,symbol:marketDataState.symbol,timeframe:marketDataState.timeframe,limit:marketDataState.limit,startTime,align:'head',timeoutMs:18000});
if(chunk.length){marketDataState.bars=cropBars(mergeBars(marketDataState.bars,chunk));renderKlineChart(true);}else{
  const tfSec=timeframeSeconds(marketDataState.timeframe);
  const spanMs=Math.max(10*60*1000, Math.min(4*3600*1000, marketDataState.limit*tfSec*1000));
  await autoBackfillData({
    exchange:marketDataState.exchange,symbol:marketDataState.symbol,timeframe:marketDataState.timeframe,reason:'right-edge',
    startTime:new Date(Math.max(0,lastMs-tfSec*1000)).toISOString(),
    endTime:new Date(Date.now()+spanMs).toISOString(),
  });
}
}catch(e){console.error(e);}
marketDataState.isLoadingRight=false;
}
function bindKlineChartEvents(){
const c=document.getElementById('candlestick-chart');
if(!c||marketDataState.chartBound)return;
marketDataState.chartBound=true;
c.on('plotly_relayout',evt=>{
const s=evt['xaxis.range[0]'],e=evt['xaxis.range[1]'];
if(!s||!e)return;
marketDataState.lastRange={start:s,end:e};
const bars=marketDataState.bars||[];
if(!bars.length)return;
const minMs=klineToMs(bars[0]?.timestamp);
const maxMs=klineToMs(bars[bars.length-1]?.timestamp);
const leftMs=toMs(s);
const rightMs=toMs(e);
if(!Number.isFinite(minMs)||!Number.isFinite(maxMs)||!Number.isFinite(leftMs)||!Number.isFinite(rightMs))return;
const span=Math.max(1000,rightMs-leftMs);
const edge=Math.max(15000,Math.floor(span*0.12));
if(leftMs-minMs<=edge)loadMoreLeftByViewport();
if(maxMs-rightMs<=edge)loadMoreRightByViewport();
});
}
async function drawK(data){
marketDataState.bars=cropBars(mergeBars([],data||[]));
renderKlineChart(false);
bindKlineChartEvents();
}
async function refreshKlineRealtime(){
if(!isDataTabActive()||marketDataState.isLoading||marketDataState.realtimeInFlight)return;
if(!marketDataState.exchange||!marketDataState.symbol||!marketDataState.timeframe)return;
const refreshKey=`${marketDataState.exchange}|${marketDataState.symbol}|${marketDataState.timeframe}|${marketDataState.loadSeq}`;
const bars=marketDataState.bars||[];
if(!bars.length){try{await loadKlinesByForm();}catch{}return;}
marketDataState.realtimeInFlight=true;
try{
const tfSec=timeframeSeconds(marketDataState.timeframe);
const lastMs=klineToMs(bars[bars.length-1]?.timestamp);
if(!Number.isFinite(lastMs))return;
const startTime=new Date(lastMs-Math.max(1000,tfSec*3000)).toISOString();
const latest=await fetchKlinesChunk({exchange:marketDataState.exchange,symbol:marketDataState.symbol,timeframe:marketDataState.timeframe,limit:Math.min(600,marketDataState.limit),startTime,align:'head',timeoutMs:9000});
if(refreshKey!==`${marketDataState.exchange}|${marketDataState.symbol}|${marketDataState.timeframe}|${marketDataState.loadSeq}`)return;
marketDataState.lastRealtimePollAt=Date.now();
if(latest.length){marketDataState.bars=cropBars(mergeBars(marketDataState.bars,latest));renderKlineChart(true);}else if(hasLargeGap(marketDataState.bars,marketDataState.timeframe)){
  const range=inferBackfillRangeFromBars(marketDataState.bars, marketDataState.timeframe);
  await autoBackfillData({exchange:marketDataState.exchange,symbol:marketDataState.symbol,timeframe:marketDataState.timeframe,reason:'realtime-gap',...range});
}
}catch(e){console.error(e);}
finally{marketDataState.realtimeInFlight=false;}
}
function scheduleKlineRealtime(){
if(marketDataState.realtimeTimer)clearInterval(marketDataState.realtimeTimer);
marketDataState.realtimeTimer=setInterval(refreshKlineRealtime,klineRealtimeIntervalMs(marketDataState.timeframe||document.getElementById('data-timeframe')?.value||'1m'));
setTimeout(()=>{refreshKlineRealtime().catch(()=>{});},300);
}
async function loadKlinesByForm(){
const ex=document.getElementById('data-exchange').value,s=document.getElementById('data-symbol').value,tf=document.getElementById('data-timeframe').value,l=parseInt(document.getElementById('data-limit').value||'1200',10);
const loadSeq=marketDataState.loadSeq+1;
marketDataState.loadSeq=loadSeq;
marketDataState.exchange=ex;
marketDataState.symbol=s;
marketDataState.timeframe=tf;
marketDataState.limit=Math.max(100,Math.min(5000,l));
marketDataState.isLoading=true;
marketDataState.lastRange=null;
resetKlineChartForSwitch(`正在加载 ${s} ${tf} 行情...`);
try{
let actualExchange=ex;
const isSubSecond=String(tf||'').endsWith('s');
let data=await fetchKlinesChunk({exchange:ex,symbol:s,timeframe:tf,limit:marketDataState.limit,align:'tail',timeoutMs:isSubSecond?30000:18000});
if(!data.length&&ex!=='binance'){
const alt='binance';
const altData=await fetchKlinesChunk({exchange:alt,symbol:s,timeframe:tf,limit:marketDataState.limit,align:'tail',timeoutMs:isSubSecond?30000:18000});
if(altData.length){
data=altData;
actualExchange=alt;
const exSel=document.getElementById('data-exchange');
if(exSel)exSel.value=alt;
notify(`当前 ${ex} 数据不足，已自动切换到 ${alt}`);
}
}
if(!data.length){
await autoBackfillData({exchange:actualExchange,symbol:s,timeframe:tf,reason:'initial-load'});
await new Promise(r=>setTimeout(r,isSubSecond?1800:900));
data=await fetchKlinesChunk({exchange:actualExchange,symbol:s,timeframe:tf,limit:marketDataState.limit,align:'tail',timeoutMs:isSubSecond?35000:22000});
}
if(loadSeq!==marketDataState.loadSeq)return;
if(!data.length){throw new Error(`${s} ${tf} 暂无可用数据，已触发后台自动补数，请稍后再试`);}
marketDataState.exchange=actualExchange;
await drawK(data);
if(loadSeq===marketDataState.loadSeq)scheduleKlineRealtime();
if(hasLargeGap(marketDataState.bars,marketDataState.timeframe)){
  const range=inferBackfillRangeFromBars(marketDataState.bars, marketDataState.timeframe);
  await autoBackfillData({exchange:actualExchange,symbol:s,timeframe:tf,reason:'gap-check',...range});
}
}finally{
if(loadSeq===marketDataState.loadSeq)marketDataState.isLoading=false;
}
}
async function loadDataSymbolOptions(exchange, selectIds=['data-symbol','download-symbol']){
try{
const ex=String(exchange||'binance').trim().toLowerCase()||'binance';
const resp=await api(`/data/symbols?exchange=${encodeURIComponent(ex)}`,{timeoutMs:15000});
const symbols=(Array.isArray(resp?.symbols)?resp.symbols:[]).filter(Boolean);
if(!symbols.length)return;
selectIds.forEach(id=>{
const el=document.getElementById(id);
if(!el)return;
const current=String(el.value||'BTC/USDT');
el.innerHTML=symbols.map(sym=>`<option value="${esc(sym)}"${sym===current?' selected':''}>${esc(sym)}</option>`).join('');
el.value=symbols.includes(current)?current:(symbols.includes('BTC/USDT')?'BTC/USDT':symbols[0]);
});
}catch(e){console.warn('loadDataSymbolOptions failed',e?.message||e);}
}
function getSelectValues(id){
const el=document.getElementById(id);
if(!el)return[];
if(el instanceof HTMLSelectElement && el.multiple){
return Array.from(el.selectedOptions||[]).map(opt=>String(opt.value||'').trim()).filter(Boolean);
}
const raw=String(el.value||'').trim();
return raw?[raw]:[];
}
function setSelectValues(id, values, fallback='BTC/USDT'){
const el=document.getElementById(id);
if(!el)return;
const wanted=(Array.isArray(values)?values:[values]).map(v=>String(v||'').trim()).filter(Boolean);
if(el instanceof HTMLSelectElement && el.multiple){
  const chosen=new Set((wanted.length?wanted:[fallback]).map(v=>String(v||'').trim()));
  Array.from(el.options||[]).forEach(opt=>{opt.selected=chosen.has(String(opt.value||'').trim());});
  if(!Array.from(el.selectedOptions||[]).length && el.options.length){
    el.options[0].selected=true;
  }
  return;
}
el.value=String((wanted[0]||fallback)||fallback);
}
async function loadResearchSymbolOptions(exchange){
try{
const ex=String(exchange||getResearchExchange()||'binance').trim().toLowerCase()||'binance';
const resp=await api(`/data/symbols?exchange=${encodeURIComponent(ex)}`,{timeoutMs:15000});
const symbols=(Array.isArray(resp?.symbols)?resp.symbols:[]).filter(Boolean);
if(!symbols.length)return;
const primary=document.getElementById('research-symbol');
if(primary){
  const current=String(primary.value||'BTC/USDT').trim()||'BTC/USDT';
  primary.innerHTML=symbols.map(sym=>`<option value="${esc(sym)}"${sym===current?' selected':''}>${esc(sym)}</option>`).join('');
  primary.value=symbols.includes(current)?current:(symbols.includes('BTC/USDT')?'BTC/USDT':symbols[0]);
}
const multi=document.getElementById('research-symbols');
if(multi){
  const currentSet=new Set(getSelectValues('research-symbols'));
  const default30=['BTC/USDT','ETH/USDT','BNB/USDT','SOL/USDT','XRP/USDT','ADA/USDT','DOGE/USDT','TRX/USDT','LINK/USDT','AVAX/USDT','DOT/USDT','POL/USDT','LTC/USDT','BCH/USDT','ETC/USDT','ATOM/USDT','NEAR/USDT','APT/USDT','ARB/USDT','OP/USDT','SUI/USDT','INJ/USDT','RUNE/USDT','AAVE/USDT','MKR/USDT','UNI/USDT','FIL/USDT','HBAR/USDT','ICP/USDT','TON/USDT'];
  const chosen=currentSet.size?currentSet:new Set(default30.filter(sym=>symbols.includes(sym)));
  multi.innerHTML=symbols.map(sym=>`<option value="${esc(sym)}"${chosen.has(sym)?' selected':''}>${esc(sym)}</option>`).join('');
  if(!Array.from(multi.selectedOptions||[]).length && multi.options.length){
    const fallbackList=(default30.filter(sym=>symbols.includes(sym)).slice(0,Math.min(12,multi.options.length)));
    setSelectValues('research-symbols',fallbackList.length?fallbackList:[multi.options[0].value]);
  }
}
}catch(e){console.warn('loadResearchSymbolOptions failed',e?.message||e);}
}
async function pollDownloadTask(taskId,{timeoutMs=12*60*1000,intervalMs=2500}={}){
const start=Date.now();
while(Date.now()-start<timeoutMs){
const task=await api(`/data/download/tasks/${encodeURIComponent(taskId)}`,{timeoutMs:15000});
if(task?.status==='completed')return task;
if(task?.status==='failed')throw new Error(task?.error||'后台下载失败');
await new Promise(r=>setTimeout(r,intervalMs));
}
throw new Error(`后台下载超时: ${taskId}`);
}
function scheduleDataChartReload(delay=180){
if(dataReloadTimer)clearTimeout(dataReloadTimer);
dataReloadTimer=setTimeout(()=>{
  loadKlinesByForm().catch(err=>console.warn('scheduleDataChartReload failed', err?.message||err));
}, Math.max(0, Number(delay||0)));
}
function bindData(){
const f=document.getElementById('data-form');
if(f)f.onsubmit=async e=>{e.preventDefault();try{await loadKlinesByForm();notify('行情加载完成（可拖动自动加载历史）');}catch(err){marketDataState.isLoading=false;notify(`行情加载失败: ${err.message}`,true);}};
const d=document.getElementById('download-form');
if(d)d.onsubmit=async e=>{e.preventDefault();try{notify('正在创建历史下载任务...');const ex=document.getElementById('download-exchange').value,s=document.getElementById('download-symbol').value,tf=document.getElementById('download-timeframe').value,days=document.getElementById('download-days').value;const r=await api(`/data/download?exchange=${ex}&symbol=${encodeURIComponent(s)}&timeframe=${tf}&days=${days}&background=true`,{method:'POST',timeoutMs:20000});if(r?.task_id){notify(`后台下载已启动: ${r.task_id}`);const task=await pollDownloadTask(r.task_id);const count=Number(task?.result?.count||0);notify(`下载完成: ${count} 根K线`);if(document.getElementById('data-exchange')?.value===ex&&document.getElementById('data-symbol')?.value===s&&document.getElementById('data-timeframe')?.value===tf){loadKlinesByForm().catch(()=>{});}return;}notify(`下载完成: ${r.count||0} 根K线`);}catch(err){notify(`下载失败: ${err.message}`,true);}};
const out=document.getElementById('data-integrity-output');
const formatIntegrityOutput=(action,payload)=>{
  if(!payload)return'无结果';
  if(action==='check'){
    return [
      `完整性检查: ${payload.exchange} ${payload.symbol} ${payload.timeframe}`,
      `状态: ${payload.ok?'正常':'存在问题'}`,
      `数据行数: ${Number(payload.rows||0).toLocaleString('zh-CN')}`,
      `时间范围: ${payload.start||'-'} -> ${payload.end||'-'}`,
      `重复行: ${Number(payload?.quality?.duplicate_rows||0)}`,
      `异常行: ${Number(payload?.quality?.invalid_rows||0)}`,
      `缺失K线: ${Number(payload?.missing?.missing_count||0)}`,
      `${(payload?.missing?.missing_preview||[]).length?`缺失示例: ${(payload.missing.missing_preview||[]).slice(0,8).join(', ')}`:'缺失示例: 无'}`
    ].join('\n');
  }
  if(action==='repair'){
    return [
      `自动补全完成: ${payload.exchange} ${payload.symbol} ${payload.timeframe}`,
      `补全前行数: ${Number(payload?.before?.rows||0).toLocaleString('zh-CN')}`,
      `补全后行数: ${Number(payload?.after?.rows||0).toLocaleString('zh-CN')}`,
      `补全前缺失: ${Number(payload?.before?.missing?.missing_count||0)}`,
      `补全后缺失: ${Number(payload?.after?.missing?.missing_count||0)}`,
      `补全前异常行: ${Number(payload?.before?.quality?.invalid_rows||0)}`,
      `补全后异常行: ${Number(payload?.after?.quality?.invalid_rows||0)}`
    ].join('\n');
  }
  if(action==='cross'){
    return [
      `交叉验证: ${payload.primary_exchange} vs ${payload.secondary_exchange}`,
      `标的/周期: ${payload.symbol} ${payload.timeframe}`,
      `重叠K线: ${Number(payload.overlap_bars||0).toLocaleString('zh-CN')}`,
      `收盘价均值偏差: ${Number(payload?.close_diff?.mean_pct||0).toFixed(4)}%`,
      `收盘价95分位偏差: ${Number(payload?.close_diff?.p95_pct||0).toFixed(4)}%`,
      `成交量均值偏差: ${Number(payload?.volume_diff?.mean_pct||0).toFixed(4)}%`,
      `一致性结论: ${payload.is_consistent?'价格基本一致':'价格差异偏大，建议检查源数据'}`
    ].join('\n');
  }
  if(action==='reconnect'){
    return [
      `行情重连: ${payload.exchange}`,
      `连接状态: ${payload.connected?'已连接':'未连接'}`,
      `说明: ${payload.message||'-'}`
    ].join('\n');
  }
  return JSON.stringify(payload,null,2);
};
const guessDownloadDays=tf=>{
  const t=String(tf||'1h').toLowerCase();
  if(t.endsWith('s'))return 7;
  if(t==='1m')return 30;
  if(t==='5m')return 60;
  if(t==='15m')return 120;
  if(t==='1h')return 365;
  return 365;
};
const run=async(a,btn)=>{
  const ex=document.getElementById('data-exchange').value,s=document.getElementById('data-symbol').value,tf=document.getElementById('data-timeframe').value,p=document.getElementById('integrity-primary').value,sec=document.getElementById('integrity-secondary').value;
  const prevText=btn?btn.textContent:'';
  try{
    if(btn){btn.disabled=true;btn.textContent='执行中...';}
    let r=null;
    if(a==='check')r=await api(`/data/integrity/check?exchange=${ex}&symbol=${encodeURIComponent(s)}&timeframe=${tf}`,{timeoutMs:25000});
    if(a==='repair'){
      try{
        r=await api(`/data/integrity/repair?exchange=${ex}&symbol=${encodeURIComponent(s)}&timeframe=${tf}`,{method:'POST',timeoutMs:90000});
      }catch(err){
        const msg=String(err?.message||'');
        if(msg.includes('无本地数据可修复')||msg.includes('404')){
          if(out)out.textContent=`本地暂无 ${s} ${tf} 数据，正在自动下载最近 ${guessDownloadDays(tf)} 天后再补全...`;
          const dl=await api(`/data/download?exchange=${encodeURIComponent(ex)}&symbol=${encodeURIComponent(s)}&timeframe=${encodeURIComponent(tf)}&days=${guessDownloadDays(tf)}&background=true`,{method:'POST',timeoutMs:20000});
          if(dl?.task_id)await pollDownloadTask(dl.task_id,{timeoutMs:15*60*1000,intervalMs:3000});
          r=await api(`/data/integrity/repair?exchange=${ex}&symbol=${encodeURIComponent(s)}&timeframe=${tf}`,{method:'POST',timeoutMs:90000});
        }else{
          throw err;
        }
      }
    }
    if(a==='cross')r=await api(`/data/cross-validate?symbol=${encodeURIComponent(s)}&timeframe=${tf}&primary_exchange=${p}&secondary_exchange=${sec}&limit=800`,{timeoutMs:45000});
    if(a==='reconnect')r=await api(`/data/reconnect?exchange=${ex}`,{method:'POST',timeoutMs:30000});
    if(out)out.textContent=formatIntegrityOutput(a,r);
    notify('操作完成');
  }catch(err){
    if(out)out.textContent=`${a==='cross'?'交叉验证':a==='repair'?'自动补全':a==='reconnect'?'行情重连':'完整性检查'}失败: ${err.message}`;
    notify(`操作失败: ${err.message}`,true);
  }finally{
    if(btn){btn.disabled=false;btn.textContent=prevText;}
  }
};
[['btn-integrity-check','check'],['btn-integrity-repair','repair'],['btn-cross-validate','cross'],['btn-reconnect','reconnect']].forEach(([id,a])=>{const b=document.getElementById(id);if(b)b.onclick=()=>run(a,b);});
const dataExchange=document.getElementById('data-exchange');
const downloadExchange=document.getElementById('download-exchange');
if(dataExchange)dataExchange.onchange=async()=>{resetKlineChartForSwitch('正在切换交易所并加载新行情...');await loadDataSymbolOptions(dataExchange.value,['data-symbol']);scheduleDataChartReload(220);};
if(downloadExchange)downloadExchange.onchange=()=>loadDataSymbolOptions(downloadExchange.value,['download-symbol']);
const dataSymbol=document.getElementById('data-symbol');
if(dataSymbol)dataSymbol.onchange=()=>{resetKlineChartForSwitch('正在切换币种并加载新行情...');scheduleDataChartReload(120);};
const dataTimeframe=document.getElementById('data-timeframe');
if(dataTimeframe)dataTimeframe.onchange=()=>{resetKlineChartForSwitch('正在切换周期并加载新行情...');scheduleDataChartReload(120);};
loadDataSymbolOptions(document.getElementById('data-exchange')?.value||'binance',['data-symbol']);
loadDataSymbolOptions(document.getElementById('download-exchange')?.value||'binance',['download-symbol']);
loadDataSymbolOptions('binance',['backtest-symbol']);
scheduleKlineRealtime();
setTimeout(()=>{if(document.getElementById('candlestick-chart')&&!marketDataState.bars.length){loadKlinesByForm().catch(()=>{});}},500);
setInterval(()=>{if(isDataTabActive()&&!marketDataState.isLoading&&!(marketDataState.bars||[]).length){loadKlinesByForm().catch(()=>{});}},7000);
}

function renderBacktest(r){
const box=document.getElementById('backtest-results');
if(!box)return;
const c=Number(r.total_return||0)>=0?'#3fb950':'#f85149';
box.innerHTML=`
<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:18px;">
<div class="stat-box"><div class="stat-label">策略</div><div class="stat-value">${r.strategy}</div></div>
<div class="stat-box"><div class="stat-label">交易对</div><div class="stat-value">${r.symbol}</div></div>
<div class="stat-box"><div class="stat-label">周期</div><div class="stat-value">${r.timeframe}</div></div>
<div class="stat-box"><div class="stat-label">样本数</div><div class="stat-value">${r.data_points||0}</div></div>
</div>
<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:18px;">
<div class="stat-box"><div class="stat-label">初始资金</div><div class="stat-value">$${Number(r.initial_capital||0).toLocaleString()}</div></div>
<div class="stat-box"><div class="stat-label">净收益率</div><div class="stat-value" style="color:${c}">${Number(r.total_return||0).toFixed(2)}%</div></div>
<div class="stat-box"><div class="stat-label">毛收益率</div><div class="stat-value">${Number(r.gross_total_return||0).toFixed(2)}%</div></div>
<div class="stat-box"><div class="stat-label">成本拖累</div><div class="stat-value" style="color:#f59f3a">${Number(r.cost_drag_return_pct||0).toFixed(2)}%</div></div>
</div>
<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;">
<div class="stat-box"><div class="stat-label">估算成本</div><div class="stat-value">$${Number(r.estimated_trade_cost_usd||0).toFixed(2)}</div></div>
<div class="stat-box"><div class="stat-label">胜率 / 交易数</div><div class="stat-value">${Number(r.win_rate||0).toFixed(2)}% / ${r.total_trades||0}</div></div>
<div class="stat-box"><div class="stat-label">最大回撤 / 夏普</div><div class="stat-value">${Number(r.max_drawdown||0).toFixed(2)}% / ${Number(r.sharpe_ratio||0).toFixed(2)}</div></div>
<div class="stat-box"><div class="stat-label">手续费/滑点</div><div class="stat-value">${(Number(r.commission_rate||0)*100).toFixed(4)}% / ${Number(r.slippage_bps||0).toFixed(2)}bps</div></div>
</div>
${renderRangeLockIndicatorHtml(r,true)}`;
const ec=document.getElementById('backtest-equity-chart');
if(ec&&r.series?.length){
if(typeof Plotly==='undefined'){ec.innerHTML='<div class="list-item">图表库未加载，回测曲线暂不可用。</div>';return;}
const rows=(r.series||[]).map(i=>({timestamp:toDate(i.timestamp),equity:+i.equity,gross_equity:+i.gross_equity,drawdown:+i.drawdown})).filter(i=>i.timestamp&&Number.isFinite(i.equity)&&Number.isFinite(i.gross_equity)&&Number.isFinite(i.drawdown));
if(!rows.length){ec.innerHTML='<div class="list-item">回测时间序列为空或时间格式异常。</div>';return;}
const x=rows.map(i=>i.timestamp),e=rows.map(i=>i.equity),ge=rows.map(i=>i.gross_equity),dd=rows.map(i=>i.drawdown);
Plotly.newPlot(ec,[{type:'scatter',mode:'lines',x,y:e,name:'净值曲线',line:{color:'#3fb950',width:2},yaxis:'y'},{type:'scatter',mode:'lines',x,y:ge,name:'毛净值曲线',line:{color:'#4da3ff',width:1},yaxis:'y'},{type:'scatter',mode:'lines',x,y:dd,name:'回撤(%)',line:{color:'#f85149',width:1},yaxis:'y2'}],{paper_bgcolor:'#111723',plot_bgcolor:'#111723',font:{color:'#d7dde8'},margin:{l:50,r:40,t:20,b:30},xaxis:plotlyTimeAxis({}),yaxis:{title:'权益',side:'left',showgrid:true,gridcolor:'#283242'},yaxis2:{title:'回撤%',overlaying:'y',side:'right',showgrid:false},legend:{orientation:'h'}},{responsive:true,displaylogo:false});
}
}
function getBacktestStrategyCatalogFromSelect(){
const sel=document.getElementById('backtest-strategy');
if(!sel)return{items:[],groups:[],byValue:{},orderIndex:{}};
const groups=[],items=[],byValue={},orderIndex={};
let idx=0;
const pushItem=(groupLabel,opt)=>{
  const value=String(opt?.value||'').trim();
  const label=String(opt?.textContent||'').trim();
  if(!value)return;
  const item={value,label,groupLabel:groupLabel||'其他'};
  items.push(item);
  byValue[value]=item;
  orderIndex[value]=idx++;
};
[...sel.children].forEach(node=>{
  if(!node||!node.tagName)return;
  const tag=String(node.tagName).toLowerCase();
  if(tag==='optgroup'){
    const groupLabel=String(node.label||'其他').trim()||'其他';
    const gItems=[...node.querySelectorAll('option')].map(opt=>{
      const value=String(opt.value||'').trim();
      const label=String(opt.textContent||'').trim();
      return value?{value,label,groupLabel}:null;
    }).filter(Boolean);
    if(gItems.length){
      groups.push({label:groupLabel,items:gItems});
      gItems.forEach(it=>{items.push(it);byValue[it.value]=it;orderIndex[it.value]=idx++;});
    }
    return;
  }
  if(tag==='option')pushItem('其他',node);
});
return{items,groups,byValue,orderIndex};
}
function backtestCompareCatalog(){if(!backtestUIState.compareCatalog)backtestUIState.compareCatalog=getBacktestStrategyCatalogFromSelect();return backtestUIState.compareCatalog||{items:[],groups:[],byValue:{},orderIndex:{}};}
function backtestCompareCurrentSource(){return String(document.getElementById('backtest-compare-source')?.value||'library').trim()||'library';}
function mapStrategyCatToBacktestGroup(cat){
const c=String(cat||'').trim();
if(['趋势','突破'].includes(c))return'趋势跟踪类';
if(['均值回归','震荡','反转'].includes(c))return'均值回归类';
if(['动量','统计套利','套利'].includes(c))return'动量振荡类';
if(['宏观'].includes(c))return'情绪资金类';
return'其他';
}
async function getRegisteredStrategyTypesForCompare(){
if(!Array.isArray(state.strategies)||!state.strategies.length){
  try{
    const d=await api('/strategies/list',{timeoutMs:12000});
    state.strategies=Array.isArray(d?.registered)?d.registered:[];
  }catch(e){
    console.error(e);
  }
}
const counts={};
for(const s of (state.strategies||[])){
  const t=String(s?.strategy_type||'').trim();
  if(!t)continue;
  counts[t]=(counts[t]||0)+1;
}
return counts;
}
function renderBacktestComparePickerList(items, opts={}){
const box=document.getElementById('backtest-compare-strategy-list');
if(!box)return;
const catalog=backtestCompareCatalog();
const preserveSelection=opts.preserveSelection!==false;
const selectAll=!!opts.selectAll;
const fallbackToDefault=!!opts.useDefault;
const prevSelected=new Set(preserveSelection?getSelectedBacktestCompareStrategies():[]);
const hardDefaultSet=new Set(backtestUIState.defaultCompareStrategies||[]);
const groupsMap={};
for(const it of (items||[])){
  const g=String(it.groupLabel||'其他');
  (groupsMap[g]||(groupsMap[g]=[])).push(it);
}
const groupOrder=[...(catalog.groups||[]).map(g=>g.label), '其他'].filter((v,i,a)=>a.indexOf(v)===i);
const html=groupOrder.filter(g=>Array.isArray(groupsMap[g])&&groupsMap[g].length).map(g=>{
  const rows=(groupsMap[g]||[]).sort((a,b)=>{
    const ai=Number(catalog.orderIndex?.[a.value]??9999),bi=Number(catalog.orderIndex?.[b.value]??9999);
    return ai-bi || String(a.value).localeCompare(String(b.value),'zh-CN');
  }).map(it=>{
    let checked=false;
    if(selectAll) checked=true;
    else if(prevSelected.has(it.value)) checked=true;
    else if(fallbackToDefault && hardDefaultSet.has(it.value)) checked=true;
    const miniParts=[it.label];
    if(Number(it.registeredCount||0)>0)miniParts.push(`已注册${Number(it.registeredCount)}个实例`);
    return `<label class="backtest-compare-item"><input type="checkbox" data-bt-compare-strategy value="${esc(it.value)}" ${checked?'checked':''}><span>${esc(it.value)}</span><span class="mini">${esc(miniParts.join(' ｜ '))}</span></label>`;
  }).join('');
  return `<details class="backtest-compare-group" open><summary><span>${esc(g)}</span><span class="count">${(groupsMap[g]||[]).length} 个</span></summary><div class="backtest-compare-group-grid">${rows}</div></details>`;
}).join('') || '<div class="list-item">暂无可用回测策略</div>';
box.innerHTML=html;
const updateCount=()=>{const c=document.getElementById('backtest-compare-picked-count');if(c)c.textContent=`已选 ${getSelectedBacktestCompareStrategies().length}`;};
box.querySelectorAll('input[data-bt-compare-strategy]').forEach(cb=>cb.addEventListener('change',updateCount));
updateCount();
}
async function loadBacktestComparePickerSource(source='library', opts={}){
const src=String(source||'library').trim();
const catalog=backtestCompareCatalog();
const hardDefault=['MAStrategy','EMAStrategy','RSIStrategy','RSIDivergenceStrategy','MACDStrategy','MACDHistogramStrategy','BollingerBandsStrategy','BollingerSqueezeStrategy','MeanReversionStrategy','BollingerMeanReversionStrategy','MomentumStrategy','TrendFollowingStrategy','PairsTradingStrategy','DonchianBreakoutStrategy','StochasticStrategy','ADXTrendStrategy','VWAPReversionStrategy','MarketSentimentStrategy','SocialSentimentStrategy','FundFlowStrategy','WhaleActivityStrategy'];
backtestUIState.defaultCompareStrategies=catalog.items.map(x=>x.value).filter(v=>hardDefault.includes(v));
let items=[];
if(src==='registered'){
  const counts=await getRegisteredStrategyTypesForCompare();
  const skipped=[];
  items=Object.keys(counts).map(type=>{
    const base=catalog.byValue?.[type];
    if(!base){skipped.push(type);return null;}
    const groupLabel=base.groupLabel || mapStrategyCatToBacktestGroup(STRATEGY_META[type]?.cat);
    const label=(base?.label)||type;
    return {value:type,label,groupLabel,registeredCount:counts[type]};
  }).filter(it=>it&&Boolean(it.value));
  backtestUIState.compareRegisteredSkipped=skipped;
  items.sort((a,b)=>{
    const ai=Number(catalog.orderIndex?.[a.value]??9999),bi=Number(catalog.orderIndex?.[b.value]??9999);
    return ai-bi || String(a.value).localeCompare(String(b.value),'zh-CN');
  });
}else{
  let available=Array.isArray(state.availableStrategyTypes)?state.availableStrategyTypes:[];
  let libraryRows=Array.isArray(state.strategyLibraryRows)?state.strategyLibraryRows:[];
  if(!libraryRows.length){
    try{
      const lib=await api('/strategies/library',{timeoutMs:18000});
      state.strategyLibraryRows=Array.isArray(lib?.library)?lib.library:[];
      libraryRows=state.strategyLibraryRows;
    }catch(e){
      console.error(e);
    }
  }
  if(!available.length){
    try{
      const d=await api('/strategies/list',{timeoutMs:12000});
      state.availableStrategyTypes=Array.isArray(d?.strategies)?d.strategies:[];
      if(Array.isArray(d?.registered))state.strategies=d.registered;
      available=state.availableStrategyTypes;
    }catch(e){
      console.error(e);
    }
  }
  const sourceList=(libraryRows&&libraryRows.length)?libraryRows.map(r=>String(r?.name||'').trim()).filter(Boolean):((available&&available.length)?available:(catalog.items||[]).map(it=>it.value));
  items=sourceList.map(type=>{
    const base=catalog.byValue?.[type];
    const libMeta=(libraryRows||[]).find(r=>String(r?.name||'')===type)||{};
    const meta=STRATEGY_META[type]||{};
    return {
      value:type,
      label:(base?.label)||`${type}${libMeta?.usage?` - ${String(libMeta.usage).slice(0,24)}`:''}`,
      groupLabel:(base?.groupLabel)||mapStrategyCatToBacktestGroup(libMeta?.category||meta.cat||'其他'),
      registeredCount:0
    };
  });
}
backtestUIState.compareSource=src;
renderBacktestComparePickerList(items,{
  preserveSelection: opts.preserveSelection!==false,
  selectAll: !!opts.selectAll,
  useDefault: src==='library' && opts.useDefault!==false
});
}
function initBacktestComparePicker(){
const box=document.getElementById('backtest-compare-strategy-list');
if(!box)return;
if(!backtestUIState._comparePickerBound){
  backtestUIState._comparePickerBound=true;
  const selAll=document.getElementById('btn-backtest-compare-select-all');
  const selNone=document.getElementById('btn-backtest-compare-select-none');
  const selDefault=document.getElementById('btn-backtest-compare-select-default');
  const sourceEl=document.getElementById('backtest-compare-source');
  const loadBtn=document.getElementById('btn-backtest-compare-load-source');
  const updateCount=()=>{const c=document.getElementById('backtest-compare-picked-count');if(c)c.textContent=`已选 ${getSelectedBacktestCompareStrategies().length}`;};
  if(selAll)selAll.onclick=()=>{box.querySelectorAll('input[data-bt-compare-strategy]').forEach(cb=>cb.checked=true);updateCount();};
  if(selNone)selNone.onclick=()=>{box.querySelectorAll('input[data-bt-compare-strategy]').forEach(cb=>cb.checked=false);updateCount();};
  if(selDefault)selDefault.onclick=async()=>{await loadBacktestComparePickerSource(backtestCompareCurrentSource(),{preserveSelection:false,selectAll:false,useDefault:true});};
  if(sourceEl)sourceEl.onchange=()=>{loadBacktestComparePickerSource(backtestCompareCurrentSource(),{preserveSelection:true,selectAll:true,useDefault:false}).catch(e=>notify(`加载策略集合失败: ${e.message}`,true));};
  if(loadBtn)loadBtn.onclick=()=>{loadBacktestComparePickerSource(backtestCompareCurrentSource(),{preserveSelection:true,selectAll:true,useDefault:false}).then(()=>{
    const skipped=Array.isArray(backtestUIState.compareRegisteredSkipped)?backtestUIState.compareRegisteredSkipped.length:0;
    if(backtestCompareCurrentSource()==='registered'&&skipped>0) notify(`已加载已注册策略类型（剔除不可回测 ${skipped} 个）`);
    else notify('策略集合已按来源加载');
  }).catch(e=>notify(`加载策略集合失败: ${e.message}`,true));};
  bindBacktestComparePresetControls();
}
renderBacktestComparePresetOptions();
loadBacktestComparePickerSource(backtestCompareCurrentSource(),{preserveSelection:true,selectAll:false,useDefault:true}).catch(e=>console.error(e));
}
function getSelectedBacktestCompareStrategies(){
const arr=[...document.querySelectorAll('input[data-bt-compare-strategy]:checked')].map(i=>String(i.value||'').trim()).filter(Boolean);
return arr;
}
function getBacktestComparePresets(){const raw=getLocalJson(BACKTEST_COMPARE_PRESET_KEY,{});return(raw&&typeof raw==='object'&&!Array.isArray(raw))?raw:{};}
function saveBacktestComparePresets(presets){setLocalJson(BACKTEST_COMPARE_PRESET_KEY,presets||{});}
function renderBacktestComparePresetOptions(){
const sel=document.getElementById('backtest-compare-preset-select');if(!sel)return;
const presets=getBacktestComparePresets();
const names=Object.keys(presets).sort((a,b)=>a.localeCompare(b,'zh-CN'));
sel.innerHTML=`<option value="">选择预设...</option>${names.map(n=>`<option value="${esc(n)}">${esc(n)} (${Array.isArray(presets[n])?presets[n].length:0})</option>`).join('')}`;
}
function applyBacktestCompareStrategySelection(list){
const set=new Set((Array.isArray(list)?list:[]).map(x=>String(x||'').trim()).filter(Boolean));
document.querySelectorAll('input[data-bt-compare-strategy]').forEach(cb=>{cb.checked=set.has(String(cb.value||''));});
const c=document.getElementById('backtest-compare-picked-count');if(c)c.textContent=`已选 ${getSelectedBacktestCompareStrategies().length}`;
}
function bindBacktestComparePresetControls(){
if(backtestUIState._presetBound)return;
backtestUIState._presetBound=true;
const btnSave=document.getElementById('btn-backtest-compare-preset-save');
const btnLoad=document.getElementById('btn-backtest-compare-preset-load');
const btnDelete=document.getElementById('btn-backtest-compare-preset-delete');
const nameEl=document.getElementById('backtest-compare-preset-name');
const selEl=document.getElementById('backtest-compare-preset-select');
if(btnSave)btnSave.onclick=()=>{
  const name=String(nameEl?.value||'').trim();
  if(!name){notify('请输入预设名称',true);return;}
  const picked=getSelectedBacktestCompareStrategies();
  if(!picked.length){notify('请至少勾选一个策略',true);return;}
  const presets=getBacktestComparePresets();
  presets[name]=picked;
  saveBacktestComparePresets(presets);
  renderBacktestComparePresetOptions();
  if(selEl)selEl.value=name;
  notify(`已保存预设：${name}（${picked.length}个策略）`);
};
if(btnLoad)btnLoad.onclick=()=>{
  const name=String(selEl?.value||'').trim();
  if(!name){notify('请先选择预设',true);return;}
  const presets=getBacktestComparePresets();
  const list=presets[name];
  if(!Array.isArray(list)||!list.length){notify('预设为空或不存在',true);return;}
  applyBacktestCompareStrategySelection(list);
  if(nameEl&&!nameEl.value.trim())nameEl.value=name;
  notify(`已加载预设：${name}`);
};
if(btnDelete)btnDelete.onclick=()=>{
  const name=String(selEl?.value||nameEl?.value||'').trim();
  if(!name){notify('请选择要删除的预设',true);return;}
  const presets=getBacktestComparePresets();
  if(!Object.prototype.hasOwnProperty.call(presets,name)){notify('预设不存在',true);return;}
  delete presets[name];
  saveBacktestComparePresets(presets);
  renderBacktestComparePresetOptions();
  if(nameEl&&nameEl.value.trim()===name)nameEl.value='';
  notify(`已删除预设：${name}`);
};
}
function btNum(v,d=2){const n=Number(v);return Number.isFinite(n)?n.toFixed(d):'--';}
function btPct(v,d=2){return `${btNum(v,d)}%`;}
function btMetricCell(v,type='num'){if(v===null||v===undefined||!Number.isFinite(Number(v)))return '--';if(type==='pct')return btPct(v,2);if(type==='usd')return `$${btNum(v,2)}`;if(type==='int')return `${Math.round(Number(v))}`;return btNum(v,2);}
function getBacktestRangeLockState(data, fallbackFromForm=true){
const formStart=String(document.getElementById('backtest-start-date')?.value||'').trim();
const formEnd=String(document.getElementById('backtest-end-date')?.value||'').trim();
const reqStart=String(data?.requested_start_date||'').trim() || (fallbackFromForm?formStart:'');
const reqEnd=String(data?.requested_end_date||'').trim() || (fallbackFromForm?formEnd:'');
const actualStart=String(data?.start_date||'').trim();
const actualEnd=String(data?.end_date||'').trim();
const hasReq=!!(reqStart||reqEnd);
const autoExpanded=Boolean(data?.auto_expanded_range);
if(!hasReq){
  return {locked:false,status:'未锁定',text:'未锁定（使用当前可用历史范围）',className:''};
}
if(autoExpanded){
  return {locked:false,status:'已解锁',text:`已解锁（区间样本不足，扩展到 ${actualStart||'-'} ~ ${actualEnd||'-'}）`,className:'warn'};
}
return {locked:true,status:'已锁定',text:`已锁定（请求 ${reqStart||'-'} ~ ${reqEnd||'-'}${actualStart&&actualEnd?`；实际 ${actualStart} ~ ${actualEnd}`:''}）`,className:'ok'};
}
function renderRangeLockIndicatorHtml(data, fallbackFromForm=true){
const s=getBacktestRangeLockState(data,fallbackFromForm);
return `<div class="list-item range-lock-row"><span>区间锁定</span><span class="status-badge ${s.className==='ok'?'connected':''}" ${s.className==='warn'?'style="background:rgba(255,177,95,.14);color:#ffb15f;border-color:rgba(255,177,95,.35);"':''}>${esc(s.status)}</span><span class="range-lock-text">${esc(s.text)}</span></div>`;
}
function getBacktestExtraPanel(){return document.getElementById('backtest-extra-output');}
function renderBacktestExtraLoading(title='处理中'){const out=getBacktestExtraPanel();if(!out)return;out.innerHTML=`<div class="list-item"><span>${esc(title)}</span><span>请稍候...</span></div>`;}
function renderBacktestExtraError(err){const out=getBacktestExtraPanel();if(!out)return;out.innerHTML=`<div class="list-item"><span>操作失败</span><span style="color:#ff8b8b">${esc(err?.message||String(err||'未知错误'))}</span></div>`;}
function renderBacktestRawBlock(data,label='原始JSON'){
return `<details><summary>${esc(label)}</summary><pre>${esc(JSON.stringify(data,null,2))}</pre></details>`;
}
function renderBacktestCompareOutput(data){
const out=getBacktestExtraPanel();if(!out)return;
const regCfg=getBacktestRegisterOptions();
const rows=Array.isArray(data?.results)?data.results:[];
const okRows=rows.filter(r=>r&&typeof r==='object'&&!r.error);
const errRows=rows.filter(r=>r&&r.error);
if(!rows.length){out.innerHTML='<div class="list-item"><span>多策略对比</span><span>无结果</span></div>';return;}
const ranked=[...okRows].sort((a,b)=>Number(b?.total_return||-1e9)-Number(a?.total_return||-1e9)||Number(b?.sharpe_ratio||-1e9)-Number(a?.sharpe_ratio||-1e9));
const best=ranked[0]||null;
const stable=[...okRows].sort((a,b)=>((Number(b.total_return||0)-Number(b.max_drawdown||0))- (Number(a.total_return||0)-Number(a.max_drawdown||0))));
const bestBalanced=stable[0]||null;
const avgRet=okRows.length?okRows.reduce((s,x)=>s+Number(x.total_return||0),0)/okRows.length:0;
const avgSharpe=okRows.length?okRows.reduce((s,x)=>s+Number(x.sharpe_ratio||0),0)/okRows.length:0;
const optimizedCount=okRows.filter(r=>r.optimization_applied).length;
backtestUIState.lastCompare={...(data||{}), ranked:[...ranked]};
out.innerHTML=`
<div class="list-item"><span>多策略对比（${esc(data.symbol||'-')} / ${esc(data.timeframe||'-')}）</span><span>成功 ${okRows.length} / 总计 ${rows.length}</span></div>
${renderRangeLockIndicatorHtml(data,false)}
<div class="backtest-subgrid">
  <div class="stat-box"><div class="stat-label">最佳收益策略</div><div class="stat-value">${esc(best?.strategy||'-')}</div><div class="stat-label">${best?`${btPct(best.total_return)} / 夏普 ${btNum(best.sharpe_ratio)}`:'--'}</div></div>
  <div class="stat-box"><div class="stat-label">均衡推荐（收益-回撤）</div><div class="stat-value">${esc(bestBalanced?.strategy||'-')}</div><div class="stat-label">${bestBalanced?`${btPct(bestBalanced.total_return)} / 回撤 ${btPct(bestBalanced.max_drawdown)}`:'--'}</div></div>
  <div class="stat-box"><div class="stat-label">平均收益 / 平均夏普</div><div class="stat-value">${btPct(avgRet)} / ${btNum(avgSharpe)}</div><div class="stat-label">成本: 手续费 ${(Number(data?.commission_rate||0)*100).toFixed(4)}% + 滑点 ${btNum(data?.slippage_bps||0)}bps</div></div>
  <div class="stat-box"><div class="stat-label">结论建议</div><div class="stat-value">${best&&best.total_return>0?'优先回测前3名细化参数':'先降低周期/成本或换策略组'}</div><div class="stat-label">${data?.pre_optimize?`已预优化 ${optimizedCount}/${okRows.length} 个策略（目标: ${esc(data?.optimize_objective||'total_return')}, trials=${Number(data?.optimize_max_trials||0)})`:(bestBalanced?`建议下一步用 ${bestBalanced.strategy} 做参数优化`: '暂无可推荐策略')}</div></div>
</div>
<div class="inline-actions" style="margin-top:10px;">
  <button type="button" class="btn btn-primary btn-sm" id="btn-backtest-register-best">注册收益第一策略（新实例）</button>
  <button type="button" class="btn btn-primary btn-sm" id="btn-backtest-register-top3">注册前3策略（新实例）</button>
  <span style="font-size:12px;color:#9fb1c9;">新实例选项：资金占比 ${regCfg.allocation.toFixed(2)}${regCfg.autoStart?' | 自动启动':''}${regCfg.suffix?` | 后缀 ${regCfg.suffix}`:''}</span>
</div>
<div class="section-title">策略排行榜（按收益率排序，点击行可在上方预览该策略区间回测）</div>
<div class="backtest-table-wrap">
<table class="data-table">
<thead><tr><th>排名</th><th>策略</th><th>参数来源</th><th>收益率</th><th>夏普</th><th>回撤</th><th>胜率</th><th>交易数</th><th>成本拖累</th><th>质量</th><th>操作</th></tr></thead>
<tbody>
${ranked.map((r,i)=>`<tr class="bt-compare-row ${Number(backtestUIState?.lastComparePreviewRank??-1)===i?'active-preview':''}" data-rank-index="${i}" onclick="previewCompareStrategyByRank(${i})" style="cursor:pointer;">
<td>${i+1}</td>
<td>${esc(r.strategy||'-')}</td>
<td>${r.optimization_applied?`已优化 (${esc(r.optimization_objective||'')})`:`默认参数`}</td>
<td class="${Number(r.total_return||0)>=0?'positive':'negative'}">${btPct(r.total_return)}</td>
<td>${btNum(r.sharpe_ratio)}</td>
<td>${btPct(r.max_drawdown)}</td>
<td>${btPct(r.win_rate)}</td>
<td>${btMetricCell(r.total_trades,'int')}</td>
<td>${btPct(r.cost_drag_return_pct)}</td>
<td>${esc(r.quality_flag||'-')}</td>
<td>
  <div class="inline-actions" style="gap:6px;">
    <button type="button" class="btn btn-primary btn-sm" onclick="event.stopPropagation();previewCompareStrategyByRank(${i})">预览</button>
    <button type="button" class="btn btn-primary btn-sm" onclick="event.stopPropagation();registerCompareStrategyByRank(${i})">注册</button>
  </div>
</td>
</tr>`).join('') || '<tr><td colspan="11">无成功结果</td></tr>'}
</tbody></table></div>
<div id="backtest-extra-chart" class="backtest-chart"></div>
${errRows.length?`<div class="section-title">失败策略</div><div class="backtest-table-wrap"><table class="data-table"><thead><tr><th>策略</th><th>错误</th></tr></thead><tbody>${errRows.map(r=>`<tr><td>${esc(r.strategy||'-')}</td><td>${esc(r.error||'')}</td></tr>`).join('')}</tbody></table></div>`:''}
${renderBacktestRawBlock(data,'查看原始对比结果(JSON)')}
`;
const regBest=document.getElementById('btn-backtest-register-best');
if(regBest)regBest.onclick=()=>registerCompareStrategyByRank(0);
const regTop3=document.getElementById('btn-backtest-register-top3');
if(regTop3)regTop3.onclick=()=>registerTopCompareStrategies(3);
renderBacktestCompareChart(ranked.slice(0,12));
}
function renderBacktestCompareChart(rows){
const el=document.getElementById('backtest-extra-chart');
if(!el)return;
if(typeof Plotly==='undefined'){el.innerHTML='<div class="list-item">图表库未加载，无法显示对比图。</div>';return;}
preparePlotlyHost(el);
if(!rows.length){el.innerHTML='<div class="list-item">暂无可视化数据</div>';return;}
const names=rows.map(r=>String(r.strategy||''));
const ret=rows.map(r=>Number(r.total_return||0));
const dd=rows.map(r=>-Math.abs(Number(r.max_drawdown||0)));
const sharpe=rows.map(r=>Number(r.sharpe_ratio||0));
Plotly.react(el,[
{type:'bar',x:names,y:ret,name:'收益率%',marker:{color:ret.map(v=>v>=0?'#1f9d63':'#c94b58')}},
{type:'scatter',mode:'lines+markers',x:names,y:sharpe,name:'夏普',yaxis:'y2',line:{color:'#4da3ff',width:2}},
{type:'bar',x:names,y:dd,name:'-回撤%',marker:{color:'#8b5cf6',opacity:0.35}},
],{
paper_bgcolor:'#111723',plot_bgcolor:'#111723',font:{color:'#d7dde8'},
margin:{l:50,r:46,t:18,b:90},
xaxis:{tickangle:-30,automargin:true},
yaxis:{title:'收益/回撤(%)',showgrid:true,gridcolor:'#283242'},
yaxis2:{title:'夏普',overlaying:'y',side:'right',showgrid:false},
barmode:'group',
legend:{orientation:'h',y:1.12}
},{responsive:true,displaylogo:false});
schedulePlotlyResize(el.parentElement||document);
}
function renderBacktestOptimizeOutput(data){
const out=getBacktestExtraPanel();if(!out)return;
const regCfg=getBacktestRegisterOptions();
backtestUIState.lastOptimize=data||null;
const best=data?.best||null;
const top=Array.isArray(data?.top)?data.top:[];
const allTrials=Array.isArray(data?.all_trials)?data.all_trials:[];
const objective=String(data?.objective||'total_return');
const objectiveMap={total_return:'收益率',sharpe_ratio:'夏普',win_rate:'胜率'};
const objectiveLabel=objectiveMap[objective]||objective;
out.innerHTML=`
<div class="list-item"><span>参数优化（${esc(data?.strategy||'-')} / ${esc(data?.symbol||'-')} / ${esc(data?.timeframe||'-')}）</span><span>试验 ${Number(data?.trials||top.length||0)} 次</span></div>
${renderRangeLockIndicatorHtml(data,false)}
<div class="list-item"><span>回测区间 / 样本数</span><span>${esc(String(data?.requested_start_date||data?.start_date||'-'))} ~ ${esc(String(data?.requested_end_date||data?.end_date||'-'))} | ${Number(data?.data_points||0)} 根</span></div>
<div class="backtest-subgrid">
  <div class="stat-box"><div class="stat-label">优化目标</div><div class="stat-value">${esc(objectiveLabel)}</div><div class="stat-label">手续费 ${(Number(data?.commission_rate||0)*100).toFixed(4)}% | 滑点 ${btNum(data?.slippage_bps||0)}bps</div></div>
  <div class="stat-box"><div class="stat-label">最佳得分</div><div class="stat-value">${btNum(best?.score||0)}</div><div class="stat-label">${best?`收益 ${btPct(best.metrics?.total_return)} / 回撤 ${btPct(best.metrics?.max_drawdown)} / 夏普 ${btNum(best.metrics?.sharpe_ratio)}`:'--'}</div></div>
  <div class="stat-box"><div class="stat-label">推荐参数</div><div class="stat-value">${best&&best.params?Object.keys(best.params).length:0} 项</div><div class="stat-label">${best&&best.params?esc(Object.entries(best.params).map(([k,v])=>`${k}=${v}`).join(', ')):'--'}</div></div>
  <div class="stat-box"><div class="stat-label">建议</div><div class="stat-value">${best&&Number(best.metrics?.max_drawdown||0)<20?'可做滚动验证':'先降低风险参数/换周期'}</div><div class="stat-label">下一步：walk-forward 或分段回测验证稳定性</div></div>
</div>
<div class="inline-actions" style="margin-top:10px;">
  <button type="button" class="btn btn-primary btn-sm" id="btn-apply-opt-best">一键回填最佳参数到策略参数编辑</button>
  <button type="button" class="btn btn-primary btn-sm" id="btn-register-opt-best" onclick="registerOptimizeBestAsNewStrategyInstance()">按最佳参数注册新实例</button>
  <span style="font-size:12px;color:#9fb1c9;">回填仅填前端编辑面板；注册选项：${regCfg.allocation.toFixed(2)}${regCfg.autoStart?' / 自动启动':''}${regCfg.suffix?` / ${regCfg.suffix}`:''}</span>
</div>
<div class="section-title">Top 参数组合</div>
<div class="backtest-table-wrap">
<table class="data-table">
<thead><tr><th>排名</th><th>得分(${esc(objectiveLabel)})</th><th>收益率</th><th>夏普</th><th>回撤</th><th>胜率</th><th>交易数</th><th>参数</th></tr></thead>
<tbody>
${top.map((t,i)=>`<tr>
<td>${i+1}</td>
<td>${btNum(t.score)}</td>
<td class="${Number(t?.metrics?.total_return||0)>=0?'positive':'negative'}">${btPct(t?.metrics?.total_return)}</td>
<td>${btNum(t?.metrics?.sharpe_ratio)}</td>
<td>${btPct(t?.metrics?.max_drawdown)}</td>
<td>${btPct(t?.metrics?.win_rate)}</td>
<td>${btMetricCell(t?.metrics?.total_trades,'int')}</td>
<td>${esc(Object.entries(t.params||{}).map(([k,v])=>`${k}=${v}`).join(', '))}</td>
</tr>`).join('') || '<tr><td colspan="8">无优化结果</td></tr>'}
</tbody></table></div>
<div id="backtest-extra-chart" class="backtest-chart"></div>
<div id="backtest-extra-heatmap" class="backtest-heatmap"></div>
${renderBacktestRawBlock(data,'查看原始优化结果(JSON)')}
`;
const applyBtn=document.getElementById('btn-apply-opt-best');
if(applyBtn)applyBtn.onclick=applyBestOptimizeParamsToStrategyEditor;
const regBtn=document.getElementById('btn-register-opt-best');
if(regBtn)regBtn.onclick=registerOptimizeBestAsNewStrategyInstance;
renderBacktestOptimizeChart(top,objectiveLabel);
renderBacktestOptimizeHeatmap(data,objectiveLabel,allTrials);
}
function renderBacktestOptimizeChart(top,objectiveLabel){
const el=document.getElementById('backtest-extra-chart');
if(!el)return;
if(typeof Plotly==='undefined'){el.innerHTML='<div class="list-item">图表库未加载，无法显示优化图。</div>';return;}
preparePlotlyHost(el);
const rows=(Array.isArray(top)?top:[]).slice(0,10);
if(!rows.length){el.innerHTML='<div class="list-item">暂无可视化优化结果</div>';return;}
const x=rows.map((_,i)=>`#${i+1}`);
const score=rows.map(r=>Number(r.score||0));
const ret=rows.map(r=>Number(r?.metrics?.total_return||0));
const dd=rows.map(r=>Number(r?.metrics?.max_drawdown||0));
const wr=rows.map(r=>Number(r?.metrics?.win_rate||0));
Plotly.react(el,[
{type:'bar',x,y:score,name:`得分(${objectiveLabel})`,marker:{color:'#1f9d63'}},
{type:'scatter',mode:'lines+markers',x,y:ret,name:'收益率%',yaxis:'y2',line:{color:'#4da3ff'}},
{type:'scatter',mode:'lines+markers',x,y:wr,name:'胜率%',yaxis:'y2',line:{color:'#ffd166'}},
{type:'scatter',mode:'lines+markers',x,y:dd,name:'最大回撤%',yaxis:'y2',line:{color:'#f85149',dash:'dot'}},
],{
paper_bgcolor:'#111723',plot_bgcolor:'#111723',font:{color:'#d7dde8'},
margin:{l:50,r:52,t:18,b:42},
xaxis:{title:'Top 组合排名'},
yaxis:{title:'优化得分',showgrid:true,gridcolor:'#283242'},
yaxis2:{title:'收益/胜率/回撤(%)',overlaying:'y',side:'right',showgrid:false},
legend:{orientation:'h',y:1.12}
},{responsive:true,displaylogo:false});
schedulePlotlyResize(el.parentElement||document);
}
function renderBacktestOptimizeHeatmap(data,objectiveLabel,allTrials){
const el=document.getElementById('backtest-extra-heatmap');
if(!el)return;
if(typeof Plotly==='undefined'){el.innerHTML='<div class="list-item">图表库未加载，无法显示参数敏感性热力图。</div>';return;}
preparePlotlyHost(el);
const rows=(Array.isArray(allTrials)&&allTrials.length?allTrials:(data?.top||[]).map(t=>({params:t.params||{},score:t.score,total_return:t?.metrics?.total_return,max_drawdown:t?.metrics?.max_drawdown,win_rate:t?.metrics?.win_rate})));
if(!rows.length){el.innerHTML='<div class="list-item">暂无热力图数据</div>';return;}
const paramKeys=[...new Set(rows.flatMap(r=>Object.keys(r?.params||{})))];
if(paramKeys.length!==2){el.innerHTML=`<div class="list-item">参数敏感性热力图仅在“2参数策略”时显示（当前 ${paramKeys.length} 个参数）</div>`;return;}
const [k1,k2]=paramKeys;
const xVals=[...new Set(rows.map(r=>r?.params?.[k1]).filter(v=>v!==undefined))];
const yVals=[...new Set(rows.map(r=>r?.params?.[k2]).filter(v=>v!==undefined))];
const sortMaybe=(arr)=>arr.sort((a,b)=>(Number.isFinite(Number(a))&&Number.isFinite(Number(b)))?(Number(a)-Number(b)):String(a).localeCompare(String(b)));
sortMaybe(xVals);sortMaybe(yVals);
if(xVals.length<2||yVals.length<2){el.innerHTML='<div class="list-item">热力图需要两个参数都至少有2个取值</div>';return;}
const metricKey=String(data?.objective||'total_return')==='sharpe_ratio'?'score':'score';
const z=yVals.map(y=>xVals.map(x=>{
const cell=rows.filter(r=>String(r?.params?.[k1])===String(x)&&String(r?.params?.[k2])===String(y));
if(!cell.length)return null;
return Math.max(...cell.map(r=>Number(r?.[metricKey]??r?.score??0)).filter(v=>Number.isFinite(v)));
}));
const text=yVals.map(y=>xVals.map(x=>{
const cell=rows.find(r=>String(r?.params?.[k1])===String(x)&&String(r?.params?.[k2])===String(y));
if(!cell)return '';
return `收益 ${btPct(cell.total_return)}<br>胜率 ${btPct(cell.win_rate)}<br>回撤 ${btPct(cell.max_drawdown)}`;
}));
Plotly.react(el,[{
type:'heatmap',
x:xVals.map(v=>String(v)),
y:yVals.map(v=>String(v)),
z,
colorscale:'Viridis',
hovertemplate:`${esc(k1)}=%{x}<br>${esc(k2)}=%{y}<br>得分=${objectiveLabel==='胜率'?'%{z:.2f}%':'%{z:.4f}'}<br>%{text}<extra></extra>`,
text,
colorbar:{title:`得分(${objectiveLabel})`,thickness:14}
}],{
paper_bgcolor:'#111723',plot_bgcolor:'#111723',font:{color:'#d7dde8'},
margin:{l:70,r:34,t:26,b:60},
title:{text:`参数敏感性热力图：${k1} × ${k2}`,font:{size:13,color:'#d7e6fb'}},
xaxis:{title:k1,automargin:true},
yaxis:{title:k2,automargin:true}
},{responsive:true,displaylogo:false});
schedulePlotlyResize(el.parentElement||document);
}
async function applyBestOptimizeParamsToStrategyEditor(){
const opt=backtestUIState.lastOptimize;
const best=opt?.best;
const bestParams=best?.params||null;
if(!opt||!bestParams){notify('暂无可回填的优化结果',true);return;}
const targetType=String(opt.strategy||'').trim();
const panel=document.getElementById('strategy-edit-panel');
if(!panel){notify('未找到策略参数编辑面板',true);return;}
let activeType=String(panel.dataset?.strategyType||'').trim();
let activeName=String(panel.dataset?.strategyName||'').trim();
if(!activeType || activeType!==targetType){
  const running=(state.summary?.running||[]);
  const matched=running.find(x=>String(x?.strategy_type||'').trim()===targetType);
  if(matched?.name){
    activateTab('strategies');
    await openEditor(matched.name);
    await new Promise(r=>setTimeout(r,80));
    activeType=String(panel.dataset?.strategyType||'').trim();
    activeName=String(panel.dataset?.strategyName||'').trim();
  }
}
if(!activeType || activeType!==targetType){
  let regs=Array.isArray(state.strategies)?state.strategies:[];
  if(!regs.length){
    try{await loadStrategies(); regs=Array.isArray(state.strategies)?state.strategies:[];}catch{}
  }
  const matchedReg=regs.find(x=>String(x?.strategy_type||'').trim()===targetType) || regs.find(x=>String(x?.name||'').includes(targetType));
  if(matchedReg?.name){
    activateTab('strategies');
    await openEditor(matchedReg.name);
    await new Promise(r=>setTimeout(r,80));
    activeType=String(panel.dataset?.strategyType||'').trim();
    activeName=String(panel.dataset?.strategyName||'').trim();
  }
}
if(!activeType || activeType!==targetType){
  activateTab('strategies');
  notify(`未找到可编辑的 ${targetType} 实例。请先注册该策略后再回填`,true);
  return;
}
let applied=0,skipped=0;
Object.entries(bestParams).forEach(([k,v])=>{
  const el=panel.querySelector(`[data-k="${CSS.escape(k)}"]`);
  if(!el){skipped++;return;}
  const t=String(el.getAttribute('data-t')||'');
  if(t==='boolean')el.value=String(Boolean(v));
  else if(t==='json')el.value=JSON.stringify(v??{},null,2);
  else el.value=(v??'');
  applied++;
  el.style.outline='1px solid rgba(32,191,120,.65)';
  setTimeout(()=>{el.style.outline='';},1200);
});
const tfEl=panel.querySelector('#edit-timeframe');
if(tfEl&&opt?.timeframe)tfEl.value=String(opt.timeframe);
notify(`已回填最佳参数到 ${activeName||targetType} 编辑面板：${applied} 项${skipped?`（跳过 ${skipped} 项）`:''}`);
activateTab('strategies');
}
function buildBacktestRegisteredName(strategyType, symbol, timeframe, suffix=''){
const base=strategyTypeShortName(strategyType).replace(/\s+/g,'').toLowerCase()||String(strategyType||'strategy').toLowerCase();
const sym=String(symbol||'BTC/USDT').split('/')[0].toLowerCase();
const tf=String(timeframe||'1h').toLowerCase();
const stamp=new Date().toISOString().replace(/[-:TZ.]/g,'').slice(8,14);
const extra=normalizeInstanceSuffixText(suffix);
return `bt_${base}_${sym}_${tf}_${stamp}_${Math.floor(Math.random()*1000).toString().padStart(3,'0')}${extra?`_${extra}`:''}`;
}
async function registerStrategyInstanceFromBacktestSpec(spec){
const strategyType=String(spec?.strategy_type||spec?.strategy||'').trim();
if(!strategyType){notify('缺少策略类型，无法注册',true);return null;}
const symbol=String(spec?.symbol||document.getElementById('backtest-symbol')?.value||'BTC/USDT').trim()||'BTC/USDT';
const timeframe=String(spec?.timeframe||document.getElementById('backtest-timeframe')?.value||'1h').trim()||'1h';
const params=(spec?.params&&typeof spec.params==='object')?spec.params:{};
const defaults=getBacktestRegisterOptions();
const allocation=Math.max(0,Math.min(1,Number(spec?.allocation ?? defaults.allocation)));
const autoStart=(spec?.auto_start!==undefined)?!!spec.auto_start:!!defaults.autoStart;
const nameSuffix=String(spec?.name_suffix ?? defaults.suffix ?? '');
const exchange=String(spec?.exchange||'binance').toLowerCase();
const name=String(spec?.name||'').trim()||buildBacktestRegisteredName(strategyType,symbol,timeframe,nameSuffix);
const payload={name,strategy_type:strategyType,params,symbols:[symbol],timeframe,exchange,allocation};
const r=await api('/strategies/register',{method:'POST',body:JSON.stringify(payload)});
const actualName=String(r?.name||name);
const out=getBacktestExtraPanel();
if(out){
  out.insertAdjacentHTML('afterbegin', `<div class="list-item"><span>已注册新实例</span><span>${esc(actualName)}</span></div>`);
}
if(autoStart){
  try{await api(`/strategies/${encodeURIComponent(actualName)}/start`,{method:'POST'});}catch(e){notify(`实例已注册但自动启动失败: ${e.message}`,true);}
}
notify(`已注册新实例：${actualName}${autoStart?'（已启动）':''}`);
await Promise.all([loadStrategies(),loadStrategySummary()]);
activateTab('strategies');
setTimeout(async()=>{
  try{
    await loadStrategies();
    await openEditor(actualName);
  }catch(e){
    console.warn('openEditor after register failed', e?.message||e);
  }
},180);
return actualName;
}
async function registerOptimizeBestAsNewStrategyInstance(){
try{
const opt=backtestUIState.lastOptimize;
const best=opt?.best;
if(!opt||!best){notify('暂无可注册的优化结果',true);return;}
const btn=document.getElementById('btn-register-opt-best');
const prevText=btn?btn.textContent:'';
if(btn){btn.disabled=true;btn.textContent='注册中...';}
await registerStrategyInstanceFromBacktestSpec({
  strategy_type: String(opt.strategy_type||opt.strategy||'').trim(),
  symbol: opt.symbol,
  timeframe: opt.timeframe,
  params: best.params||{},
  exchange: 'binance',
});
}catch(e){
  notify(`注册优化最佳实例失败: ${e.message}`,true);
}finally{
  const btn=document.getElementById('btn-register-opt-best');
  if(btn){btn.disabled=false;btn.textContent='按最佳参数注册新实例';}
}
}
async function registerCompareStrategyByRank(rankIndex){
const ranked=Array.isArray(backtestUIState?.lastCompare?.ranked)?backtestUIState.lastCompare.ranked:[];
const row=ranked[Number(rankIndex)||0];
if(!row){notify('未找到该排名策略结果',true);return null;}
const params=(row.optimization_applied&&row.optimized_params)?row.optimized_params:{};
try{
return await registerStrategyInstanceFromBacktestSpec({
  strategy_type: row.strategy,
  symbol: backtestUIState?.lastCompare?.symbol||document.getElementById('backtest-symbol')?.value||'BTC/USDT',
  timeframe: backtestUIState?.lastCompare?.timeframe||document.getElementById('backtest-timeframe')?.value||'1h',
  params,
  exchange: 'binance',
});
}catch(e){notify(`注册对比策略失败: ${e.message}`,true);}
return null;
}
async function registerTopCompareStrategies(n=3){
const ranked=Array.isArray(backtestUIState?.lastCompare?.ranked)?backtestUIState.lastCompare.ranked:[];
if(!ranked.length){notify('暂无对比结果',true);return;}
const count=Math.max(1,Math.min(Number(n)||3, ranked.length));
let ok=0,fail=0;
for(let i=0;i<count;i++){
  const name=await registerCompareStrategyByRank(i);
  if(name)ok++; else fail++;
}
notify(`前${count}策略注册完成：成功 ${ok}，失败 ${fail}`,fail>0);
}
async function previewCompareStrategyByRank(rankIndex){
try{
  backtestUIState.lastComparePreviewRank=Number(rankIndex)||0;
  document.querySelectorAll('.bt-compare-row').forEach(row=>{
    row.classList.toggle('active-preview', Number(row.getAttribute('data-rank-index')||-1)===backtestUIState.lastComparePreviewRank);
    if(Number(row.getAttribute('data-rank-index')||-1)===backtestUIState.lastComparePreviewRank){
      row.style.background='rgba(32,191,120,.10)';
      row.style.outline='1px solid rgba(32,191,120,.25)';
    }else{
      row.style.background='';
      row.style.outline='';
    }
  });
  const compare=backtestUIState?.lastCompare||{};
  const ranked=Array.isArray(compare.ranked)?compare.ranked:[];
  const row=ranked[Number(rankIndex)||0];
  if(!row){notify('未找到该排名策略',true);return;}
  const st=String(row.strategy||'').trim();
  const symbol=String(compare.symbol||document.getElementById('backtest-symbol')?.value||'BTC/USDT');
  const tf=String(compare.timeframe||document.getElementById('backtest-timeframe')?.value||'1h');
  const capital=Number(document.getElementById('backtest-capital')?.value||compare.initial_capital||10000);
  const sd=String(document.getElementById('backtest-start-date')?.value||'').trim();
  const ed=String(document.getElementById('backtest-end-date')?.value||'').trim();
  const cr=Number(compare.commission_rate ?? 0.0004);
  const sb=Number(compare.slippage_bps ?? 2);
  const params=(row.optimization_applied&&row.optimized_params&&typeof row.optimized_params==='object')?row.optimized_params:null;
  let u=`/backtest/run_custom?strategy=${encodeURIComponent(st)}&symbol=${encodeURIComponent(symbol)}&timeframe=${encodeURIComponent(tf)}&initial_capital=${encodeURIComponent(capital)}&commission_rate=${encodeURIComponent(cr)}&slippage_bps=${encodeURIComponent(sb)}&include_series=true`;
  if(sd)u+=`&start_date=${encodeURIComponent(sd)}`;
  if(ed)u+=`&end_date=${encodeURIComponent(ed)}`;
  if(params)u+=`&params_json=${encodeURIComponent(JSON.stringify(params))}`;
  notify(`正在预览策略: ${st}`);
  const r=await api(u,{method:'POST',timeoutMs:90000});
  r._from_compare_preview=true;
  r._compare_rank=Number(rankIndex)+1;
  r._compare_optimized=!!params;
  renderBacktest(r);
  const box=document.getElementById('backtest-results');
  if(box){
    const hint=document.createElement('div');
    hint.className='list-item';
    hint.style.marginTop='8px';
    hint.innerHTML=`<span>对比预览来源</span><span>#${Number(rankIndex)+1} ${esc(st)} ${params?'（优化参数）':'（默认参数）'}</span>`;
    box.appendChild(hint);
  }
  notify(`已在上方展示 #${Number(rankIndex)+1} ${st} 的区间回测结果`);
}catch(e){notify(`预览回测失败: ${e.message}`,true);}
}
function buildNotifyRulePayload(){const name=(document.getElementById('notify-rule-name')?.value||'自定义规则').trim(),rule_type=(document.getElementById('notify-rule-type')?.value||'price_above').trim(),symbol=(document.getElementById('notify-rule-symbol')?.value||'BTC/USDT').trim(),thresholdRaw=document.getElementById('notify-rule-threshold')?.value||'0',threshold=Number(thresholdRaw||0);let params={channels:['feishu']};if(rule_type==='price_above'||rule_type==='price_below'){params={...params,symbol,threshold};}if(rule_type==='daily_pnl_below_pct'){params={...params,threshold_pct:threshold||-2};}if(rule_type==='position_count_above'){params={...params,threshold:Math.max(1,parseInt(String(threshold||1),10))};}if(rule_type==='exchange_disconnected'){params={...params,exchanges:symbol.split(',').map(x=>x.trim().toLowerCase()).filter(Boolean)};}if(rule_type==='stale_strategy_count_above'||rule_type==='running_strategy_count_below'){params={...params,threshold:Math.max(1,parseInt(String(threshold||1),10))};}if(rule_type==='strategy_not_running'){params={...params,strategies:symbol.split(',').map(x=>x.trim()).filter(Boolean)};}return{name,rule_type,params,enabled:true,cooldown_seconds:300};}
function renderNotifyRules(rules){const box=document.getElementById('notify-rules-list');if(!box)return;if(!rules?.length){box.innerHTML='<div class="list-item">暂无规则</div>';return;}box.innerHTML=rules.map(r=>`<div class="list-item"><span>${esc(r.name)} | ${esc(r.rule_type)} | ${r.enabled?'启用':'停用'}</span><span class="inline-actions"><button class="btn btn-primary btn-sm" onclick="editNotifyRule('${esc(r.id)}')">编辑</button><button class="btn btn-primary btn-sm" onclick="toggleNotifyRule('${esc(r.id)}')">${r.enabled?'停用':'启用'}</button><button class="btn btn-danger btn-sm" onclick="deleteNotifyRule('${esc(r.id)}')">删除</button></span></div>`).join('');}
async function loadNotificationCenter(){const out=document.getElementById('notify-output');if(!out)return;try{const [ch,rules,events]=await Promise.all([api('/notifications/channels'),api('/notifications/rules'),api('/notifications/events?limit=20')]);const list=rules.rules||[];state.notifyRules=Object.fromEntries(list.map(x=>[x.id,x]));renderNotifyRules(list);out.textContent=JSON.stringify({channels:ch.channels||{},rules:list.slice(-20),recent_events:(events.events||[]).slice(-20)},null,2);}catch(e){out.textContent=`加载通知中心失败: ${e.message}`;}}
async function sendTestNotification(channel){const msg=(document.getElementById('notify-test-msg')?.value||'系统测试通知').trim();const out=document.getElementById('notify-output');try{const r=await api('/notifications/test',{method:'POST',body:JSON.stringify({title:'交易系统测试通知',message:msg,channels:[channel]})});if(out)out.textContent=JSON.stringify(r,null,2);notify(`${channel} 测试通知已发送`);await loadNotificationCenter();}catch(e){if(out)out.textContent=`测试通知失败: ${e.message}`;notify(`测试通知失败: ${e.message}`,true);}}
async function createNotifyRule(){const out=document.getElementById('notify-output');try{const payload=buildNotifyRulePayload();const r=await api('/notifications/rules',{method:'POST',body:JSON.stringify(payload)});if(out)out.textContent=JSON.stringify(r,null,2);notify('通知规则创建成功');await loadNotificationCenter();}catch(e){if(out)out.textContent=`创建规则失败: ${e.message}`;notify(`创建规则失败: ${e.message}`,true);}}
async function runNotifyRules(){const out=document.getElementById('notify-output');try{const r=await api('/notifications/evaluate',{method:'POST',body:JSON.stringify({exchange:'gate',symbols:['BTC/USDT','ETH/USDT','SOL/USDT']})});if(out)out.textContent=JSON.stringify(r,null,2);notify(`规则评估完成，触发 ${r?.result?.triggered_count||0} 条`);await Promise.all([loadNotificationCenter(),loadAuditLogs()]);}catch(e){if(out)out.textContent=`规则评估失败: ${e.message}`;notify(`规则评估失败: ${e.message}`,true);}}
async function editNotifyRule(id){const rule=state.notifyRules[id];if(!rule){notify('规则不存在',true);return;}const out=document.getElementById('notify-output');try{const name=prompt('规则名称',rule.name);if(name===null)return;const updates={name:name.trim()||rule.name};const rt=rule.rule_type,p=rule.params||{};if(rt==='price_above'||rt==='price_below'){const symbol=prompt('交易对',String(p.symbol||'BTC/USDT'));if(symbol===null)return;const threshold=prompt('阈值',String(p.threshold??0));if(threshold===null)return;updates.params={...p,symbol:symbol.trim()||'BTC/USDT',threshold:Number(threshold||0)};}if(rt==='daily_pnl_below_pct'){const v=prompt('阈值(%)',String(p.threshold_pct??-2));if(v===null)return;updates.params={...p,threshold_pct:Number(v||-2)};}if(rt==='position_count_above'){const v=prompt('持仓阈值',String(p.threshold??1));if(v===null)return;updates.params={...p,threshold:Math.max(1,parseInt(v,10)||1)};}if(rt==='exchange_disconnected'){const v=prompt('交易所列表(逗号分隔)',(p.exchanges||[]).join(','));if(v===null)return;updates.params={...p,exchanges:v.split(',').map(x=>x.trim().toLowerCase()).filter(Boolean)};}if(rt==='stale_strategy_count_above'||rt==='running_strategy_count_below'){const v=prompt('阈值',String(p.threshold??1));if(v===null)return;updates.params={...p,threshold:Math.max(1,parseInt(v,10)||1)};}if(rt==='strategy_not_running'){const v=prompt('策略名称列表(逗号分隔)',(p.strategies||[]).join(','));if(v===null)return;updates.params={...p,strategies:v.split(',').map(x=>x.trim()).filter(Boolean)};}const r=await api(`/notifications/rules/${encodeURIComponent(id)}`,{method:'PUT',body:JSON.stringify(updates)});if(out)out.textContent=JSON.stringify(r,null,2);notify('规则已更新');await loadNotificationCenter();}catch(e){if(out)out.textContent=`更新规则失败: ${e.message}`;notify(`更新规则失败: ${e.message}`,true);}}
async function toggleNotifyRule(id){const rule=state.notifyRules[id];if(!rule){notify('规则不存在',true);return;}const out=document.getElementById('notify-output');try{const r=await api(`/notifications/rules/${encodeURIComponent(id)}`,{method:'PUT',body:JSON.stringify({enabled:!rule.enabled})});if(out)out.textContent=JSON.stringify(r,null,2);notify(`规则已${rule.enabled?'停用':'启用'}`);await loadNotificationCenter();}catch(e){if(out)out.textContent=`切换规则失败: ${e.message}`;notify(`切换规则失败: ${e.message}`,true);}}
async function deleteNotifyRule(id){if(!confirm('确认删除该规则吗？'))return;const out=document.getElementById('notify-output');try{const r=await api(`/notifications/rules/${encodeURIComponent(id)}`,{method:'DELETE'});if(out)out.textContent=JSON.stringify(r,null,2);notify('规则已删除');await loadNotificationCenter();}catch(e){if(out)out.textContent=`删除规则失败: ${e.message}`;notify(`删除规则失败: ${e.message}`,true);}}
function bindNotificationCenter(){const f=document.getElementById('btn-test-feishu'),b1=document.getElementById('btn-test-telegram'),b2=document.getElementById('btn-test-email'),b3=document.getElementById('btn-create-rule'),b4=document.getElementById('btn-run-rules'),b5=document.getElementById('btn-refresh-heatmap');if(f)f.onclick=()=>sendTestNotification('feishu');if(b1)b1.onclick=()=>sendTestNotification('telegram');if(b2)b2.onclick=()=>sendTestNotification('email');if(b3)b3.onclick=createNotifyRule;if(b4)b4.onclick=runNotifyRules;if(b5)b5.onclick=loadPnlHeatmap;}

function renderAuditLogs(logs){const box=document.getElementById('audit-log-list');if(!box)return;if(!logs?.length){box.innerHTML='<div class="list-item">暂无审计日志</div>';return;}box.innerHTML=logs.slice(0,100).map(i=>`<div class="list-item"><span>${esc(i.timestamp||'').replace('T',' ').substring(0,19)} | ${esc(i.module)}/${esc(i.action)} | ${esc(i.status)}</span><span>${esc((i.message||'-').substring(0,72))}</span></div>`).join('');}
async function loadAuditLogs(){try{const d=await api('/trading/audit?hours=168&limit=100',{timeoutMs:12000});renderAuditLogs(d.logs||[]);}catch(e){const box=document.getElementById('audit-log-list');if(box)box.innerHTML=`<div class="list-item">审计日志加载失败: ${esc(e.message)}</div>`;}}
function bindAudit(){const b=document.getElementById('btn-refresh-audit');if(b)b.onclick=loadAuditLogs;}

let wsClient=null,wsRetryTimer=null,softRefreshTimer=null,replaySessionId='',lastTickRenderAt=0;
function softRefresh(delay=250){if(softRefreshTimer)clearTimeout(softRefreshTimer);softRefreshTimer=setTimeout(()=>{loadSummary();loadPositions();loadOrders();loadOpenOrders();loadStrategies();loadStrategySummary();loadRisk();loadConditionalOrders();loadAccounts();loadModeInfo();},delay);}
function setWsBadge(connected){state.wsConnected=!!connected;const st=document.getElementById('system-status');if(st)st.textContent=connected?'运行中(WS在线)':'运行中(轮询)';}
function applyMarketTick(payload){try{const ex=marketDataState.exchange||document.getElementById('data-exchange')?.value,sym=marketDataState.symbol||document.getElementById('data-symbol')?.value,tf=marketDataState.timeframe||document.getElementById('data-timeframe')?.value||'1m';if(!ex||!sym||!marketDataState.bars?.length)return;const t=payload?.[ex]?.[sym];if(!t)return;const px=Number(t.last||0);if(px<=0)return;const tfSec=timeframeSeconds(tf);const nowMs=Date.now();const bucketMs=Math.floor(nowMs/(tfSec*1000))*(tfSec*1000);const bars=marketDataState.bars;const last=bars[bars.length-1];const lastMs=klineToMs(last?.timestamp);if(!Number.isFinite(lastMs))return;const lastBucket=Math.floor(lastMs/(tfSec*1000))*(tfSec*1000);if(lastBucket===bucketMs){last.high=Math.max(Number(last.high||px),px);last.low=Math.min(Number(last.low||px),px);if(!Number.isFinite(last.low))last.low=px;if(!Number.isFinite(last.high))last.high=px;last.close=px;}else if(bucketMs>lastBucket){if(isSubMinuteTf(tf)){return;}const openPx=Number(last.close||px);bars.push({timestamp:klineLocalIso(bucketMs),open:openPx,high:Math.max(openPx,px),low:Math.min(openPx,px),close:px,volume:0});marketDataState.bars=cropBars(mergeBars([],bars));}const renderThrottle=isSubMinuteTf(tf)?900:450;const now=Date.now();if(now-lastTickRenderAt>=renderThrottle){lastTickRenderAt=now;renderKlineChart(true);}}catch(e){console.error(e);}}
function initWebSocket(){try{if(wsClient)wsClient.close();const proto=location.protocol==='https:'?'wss':'ws';wsClient=new WebSocket(`${proto}://${location.host}/ws`);wsClient.onopen=()=>{setWsBadge(true);};wsClient.onmessage=e=>{try{const m=JSON.parse(e.data||'{}');const ev=m.event||'';if(['order_event','position_event','execution_event','mode_changed','runtime_snapshot','strategy_signal'].includes(ev)){softRefresh(120);}if(ev==='mode_changed'){notify(`交易模式已切换: ${m?.payload?.mode||'-'}`);}if(ev==='order_event'){const o=m?.payload?.order||{};notify(`订单更新: ${o.symbol||''} ${mapOrderStatus(o.status||'')}`);}if(ev==='strategy_signal'){pushRealtimeSignal(m?.payload||{});}if(ev==='market_tick'){applyMarketTick(m?.payload||{});} }catch{}};wsClient.onclose=()=>{setWsBadge(false);if(wsRetryTimer)clearTimeout(wsRetryTimer);wsRetryTimer=setTimeout(initWebSocket,2000);};wsClient.onerror=()=>{setWsBadge(false);};}catch{setWsBadge(false);}}

async function loadConditionalOrders(){try{const d=await api('/trading/orders/conditional');const t=document.getElementById('conditional-orders-tbody');if(!t)return;const rows=d.orders||[];if(!rows.length){t.innerHTML='<tr><td colspan=\"7\">暂无条件单</td></tr>';return;}t.innerHTML=rows.map(o=>`<tr><td>${o.conditional_id}</td><td>${o.exchange} ${o.symbol}</td><td>${mapSide(o.side)}</td><td>${Number(o.trigger_price||0).toFixed(4)}</td><td>${Number(o.amount||0)}</td><td>${o.account_id||'main'}</td><td><button class=\"btn btn-danger btn-sm\" onclick=\"cancelConditional('${o.conditional_id}')\">取消</button></td></tr>`).join('');}catch(e){console.error(e);}}
async function cancelConditional(id){try{await api(`/trading/orders/conditional/${encodeURIComponent(id)}`,{method:'DELETE'});notify('条件单已取消');await loadConditionalOrders();}catch(e){notify(`取消条件单失败: ${e.message}`,true);}}

async function loadAccounts(){try{const d=await api('/trading/accounts/summary');const out=document.getElementById('accounts-output');if(out)out.textContent=JSON.stringify(d,null,2);}catch(e){const out=document.getElementById('accounts-output');if(out)out.textContent=`账户加载失败: ${e.message}`;}}
async function createAccount(){try{const payload={account_id:document.getElementById('account-id').value.trim(),name:document.getElementById('account-name').value.trim(),exchange:document.getElementById('account-exchange').value,mode:document.getElementById('account-mode').value,parent_account_id:null,enabled:true,metadata:{}};const r=await api('/trading/accounts',{method:'POST',body:JSON.stringify(payload)});notify(`账户 ${r?.account?.account_id||payload.account_id} 已创建`);await loadAccounts();}catch(e){notify(`创建账户失败: ${e.message}`,true);}}

async function loadModeInfo(){try{const d=await api('/trading/mode');const cur=document.getElementById('mode-current-text'),pend=document.getElementById('mode-pending-text');if(cur)cur.textContent=d.mode||'-';if(pend)pend.textContent=(d.pending_switches||[]).length?`待确认 ${d.pending_switches[0].target_mode}`:'无待确认切换';if(d.pending_switches?.length)state.modeToken=d.pending_switches[0].token;const out=document.getElementById('mode-output');if(out)out.textContent=JSON.stringify(d,null,2);}catch(e){const out=document.getElementById('mode-output');if(out)out.textContent=`加载模式失败: ${e.message}`;}}
async function requestModeSwitch(){try{const payload={target_mode:document.getElementById('mode-target').value,reason:document.getElementById('mode-reason').value||''};const r=await api('/trading/mode/request',{method:'POST',body:JSON.stringify(payload)});state.modeToken=r.token||'';const out=document.getElementById('mode-output');if(out)out.textContent=JSON.stringify(r,null,2);notify('模式切换申请已创建，请二次确认');await loadModeInfo();}catch(e){notify(`申请切换失败: ${e.message}`,true);}}
async function confirmModeSwitch(){try{if(!state.modeToken){await loadModeInfo();}if(!state.modeToken){notify('没有待确认切换令牌',true);return;}const text=prompt('请输入确认文本：CONFIRM LIVE TRADING','');if(text===null)return;const r=await api('/trading/mode/confirm',{method:'POST',body:JSON.stringify({token:state.modeToken,confirm_text:text})});const out=document.getElementById('mode-output');if(out)out.textContent=JSON.stringify(r,null,2);notify(`交易模式已切换为 ${r.mode}`);state.modeToken='';await loadModeInfo();await loadSystemStatus();}catch(e){notify(`确认切换失败: ${e.message}`,true);}}

function bindModeControls(){const b1=document.getElementById('btn-mode-request'),b2=document.getElementById('btn-mode-confirm');if(b1)b1.onclick=requestModeSwitch;if(b2)b2.onclick=confirmModeSwitch;}
function bindAccountControls(){const b1=document.getElementById('btn-account-create'),b2=document.getElementById('btn-account-refresh'),b3=document.getElementById('btn-refresh-conditional');if(b1)b1.onclick=createAccount;if(b2)b2.onclick=loadAccounts;if(b3)b3.onclick=loadConditionalOrders;}

async function loadStrategyLibrary(){
const out=document.getElementById('strategy-library-output');
if(!out)return;
try{
const d=await api('/strategies/library',{timeoutMs:18000});
out.textContent=JSON.stringify(d,null,2);
}catch(e){
try{
const d=await api('/strategies/runtime',{timeoutMs:12000});
out.textContent=JSON.stringify({fallback:'runtime',note:'策略库接口异常，已降级展示运行面板',data:d},null,2);
}catch(e2){
out.textContent=`策略库加载失败: ${e.message||e2.message}`;
}
}
}
function bindStrategyAdvanced(){const exp=document.getElementById('btn-strategy-export-all'),imp=document.getElementById('btn-strategy-import-json'),rk=document.getElementById('btn-strategy-ranking'),lib=document.getElementById('btn-strategy-library'),out=document.getElementById('strategy-health-output');if(exp)exp.onclick=async()=>{try{const d=await api('/strategies/export');if(out)out.textContent=JSON.stringify(d,null,2);notify('策略JSON已导出到面板');}catch(e){notify(`导出失败: ${e.message}`,true);}};if(imp)imp.onclick=async()=>{try{const raw=document.getElementById('strategy-import-json').value.trim();if(!raw){notify('请先粘贴JSON',true);return;}const payload=JSON.parse(raw);const d=await api('/strategies/import',{method:'POST',body:JSON.stringify(payload)});if(out)out.textContent=JSON.stringify(d,null,2);notify('策略导入完成');await Promise.all([loadStrategies(),loadStrategySummary(),loadStrategyLibrary()]);}catch(e){notify(`导入失败: ${e.message}`,true);}};if(rk)rk.onclick=async()=>{try{const s=document.getElementById('backtest-symbol')?.value||'BTC/USDT',tf=document.getElementById('backtest-timeframe')?.value||'1h';const d=await api(`/strategies/ranking?symbol=${encodeURIComponent(s)}&timeframe=${tf}&initial_capital=10000&top_n=20`);if(out)out.textContent=JSON.stringify(d,null,2);notify('策略评分完成');}catch(e){notify(`评分失败: ${e.message}`,true);}};if(lib)lib.onclick=loadStrategyLibrary;}

function getResearchOutputEl(){return document.getElementById('research-output')||document.getElementById('analytics-output')||document.getElementById('factor-output');}
function getResearchSummaryEl(){return document.getElementById('research-quick-summary');}
function getResearchExchange(){return document.getElementById('research-exchange')?.value||document.getElementById('data-exchange')?.value||'binance';}
function getResearchSymbol(){return (document.getElementById('research-symbol')?.value||document.getElementById('data-symbol')?.value||'BTC/USDT').trim()||'BTC/USDT';}
function getResearchTimeframe(){return document.getElementById('research-timeframe')?.value||'1h';}
function getResearchLookback(){return Math.max(120,Number(document.getElementById('research-lookback')?.value||1000));}
function getResearchExcludeRetired(){return (document.getElementById('research-exclude-retired')?.checked)!==false;}
function getResearchSymbols(){const raw=getSelectValues('research-symbols');return raw.length?raw:['BTC/USDT','ETH/USDT'];}
function getFactorLookbackForTimeframe(tf,requested){
const q=Math.max(120,Number(requested||1000));
const t=String(tf||'1h').toLowerCase();
if(t.endsWith('s'))return Math.min(q,300);
if(t==='1m')return Math.min(q,480);
if(t==='5m')return Math.min(q,900);
if(t==='15m')return Math.min(q,1400);
if(t==='1h')return Math.min(q,1800);
return Math.min(q,2200);
}
function getFactorApiTimeoutMs(tf,symbolCount=0){
const t=String(tf||'1h').toLowerCase();
const n=Math.max(1,Number(symbolCount||0));
let base=30000;
if(t==='1m')base=45000;
else if(t==='5m')base=55000;
else if(t==='15m')base=65000;
else if(t==='1h')base=70000;
else base=80000;
return Math.min(120000, base + Math.min(30000, n*600));
}
function clamp01(v){return Math.max(0,Math.min(1,Number(v)||0));}
function clamp11(v){return Math.max(-1,Math.min(1,Number(v)||0));}
function symbolToNewsKey(sym){
const raw=String(sym||'').trim().toUpperCase();
if(!raw)return'';
const main=raw.split(':')[0];
if(main.includes('/'))return main.split('/')[0];
return main.replace(/(USDT|USDC|FDUSD|BUSD|USD)$/,'')||main;
}
function renderMarketSentimentChart(metrics){
const el=document.getElementById('market-sentiment-chart');
if(!el||typeof Plotly==='undefined')return;
if(!metrics?.length){el.innerHTML='<div class="list-item">暂无情绪指标图</div>';return;}
preparePlotlyHost(el);
const x=metrics.map(m=>m.name),y=metrics.map(m=>Number(m.score||0)),colors=y.map(v=>v>=0?'#0f766e':'#a61b29');
Plotly.newPlot(el,[{type:'bar',x,y,marker:{color:colors},text:y.map(v=>v.toFixed(3)),textposition:'outside',hovertemplate:'%{x}: %{y:.4f}<extra></extra>'}],[{paper_bgcolor:'#111723',plot_bgcolor:'#111723',font:{color:'#d7dde8'},margin:{l:40,r:20,t:20,b:70},yaxis:{range:[-1,1],gridcolor:'#283242',zerolinecolor:'#415066'},xaxis:{tickangle:-25}}][0],{responsive:true,displaylogo:false});
schedulePlotlyResize(document.getElementById('research')||document);
}
function renderMarketSentimentPanel(payload){
const summary=document.getElementById('market-sentiment-summary'),grid=document.getElementById('market-sentiment-grid');
if(summary)summary.innerHTML='<div class="list-item"><span>加载中...</span><span>-</span></div>';
if(grid)grid.innerHTML='加载中...';
if(!payload||payload.error){
const msg=payload?.error||'市场情绪加载失败';
researchState.lastSentiment={error:msg};
if(summary)summary.innerHTML=`<div class="list-item"><span>${esc(msg)}</span><span>错误</span></div>`;
if(grid)grid.innerHTML='<div class="list-item">暂无情绪细项</div>';
renderMarketSentimentChart([]);
renderResearchConclusionCard();
return;
}
const micro=payload.microstructure||{},community=payload.community||{},news=payload.news||{};
const spreadAvailable=micro?.orderbook?.available!==false&&Number.isFinite(Number(micro?.orderbook?.spread_bps));
const flowAvailable=micro?.aggressor_flow?.available!==false&&Number.isFinite(Number(micro?.aggressor_flow?.imbalance));
const fundingAvailable=micro?.funding_rate?.available!==false&&Number.isFinite(Number(micro?.funding_rate?.funding_rate));
const basisAvailable=micro?.spot_futures_basis?.available!==false&&Number.isFinite(Number(micro?.spot_futures_basis?.basis_pct));
const spreadBps=spreadAvailable?Number(micro?.orderbook?.spread_bps):null;
const imbalance=flowAvailable?Number(micro?.aggressor_flow?.imbalance):null;
const funding=fundingAvailable?Number(micro?.funding_rate?.funding_rate):null;
const basisPct=basisAvailable?Number(micro?.spot_futures_basis?.basis_pct):null;
const whaleCount=Number(community?.whale_transfers?.count||0);
const annCount=Number((community?.announcements||[]).length||0);
const newsEvents=Number(news?.events_count||0);
const newsFeedCount=Number(news?.feed_count||0);
const newsRawCount=Number(news?.raw_count||0);
const newsAvailable=(newsEvents+newsFeedCount+newsRawCount)>0;
const pos=Number(news?.sentiment?.positive||0),neg=Number(news?.sentiment?.negative||0),neu=Number(news?.sentiment?.neutral||0),newsN=pos+neg+neu;
const newsBalance=newsN>0?((pos-neg)/newsN):0;
const spreadScore=spreadAvailable?clamp11((2.5-spreadBps)/2.5):null;
const imbalanceScore=flowAvailable?clamp11(imbalance):null;
const fundingScore=fundingAvailable?clamp11((-funding)/0.0015):null; // contrarian: high positive funding often crowded long
const basisScore=basisAvailable?clamp11((-basisPct)/0.35):null;
const newsScore=newsAvailable&&newsN>0?clamp11(newsBalance):null;
const riskCrowding=clamp11(((whaleCount>=10?0.35:0)+((spreadAvailable&&spreadBps>5)?0.35:0)+((fundingAvailable&&funding>0.001)?0.3:0))*-1);
const metrics=[
{name:'主动买卖失衡',score:imbalanceScore,raw:imbalance,fmt:flowAvailable?imbalance.toFixed(4):'--',hint:flowAvailable?'>0 偏主动买盘':'暂无主动流数据',available:flowAvailable},
{name:'新闻情绪',score:newsScore,raw:newsBalance,fmt:newsN>0?newsBalance.toFixed(4):'--',hint:`结构化${newsEvents} / 当前流${newsFeedCount} / 原始${newsRawCount}`,available:newsAvailable},
{name:'资金费率(反向拥挤)',score:fundingScore,raw:funding,fmt:fundingAvailable?(funding*100).toFixed(4)+'%':'--',hint:fundingAvailable?'费率越高越拥挤':'暂无资金费率数据',available:fundingAvailable},
{name:'期现基差(反向拥挤)',score:basisScore,raw:basisPct,fmt:basisAvailable?basisPct.toFixed(4)+'%':'--',hint:basisAvailable?'正基差高=多头拥挤风险':'暂无期现基差数据',available:basisAvailable},
{name:'点差健康度',score:spreadScore,raw:spreadBps,fmt:spreadAvailable?spreadBps.toFixed(3)+' bps':'--',hint:spreadAvailable?'点差越小流动性越好':'暂无盘口点差数据',available:spreadAvailable},
{name:'拥挤风险',score:riskCrowding,raw:riskCrowding,fmt:riskCrowding.toFixed(3),hint:`巨鲸=${whaleCount}，公告=${annCount}`},
];
const validMetrics=metrics.filter(m=>Number.isFinite(Number(m.score)));
const composite=validMetrics.length?validMetrics.reduce((s,m)=>s+Number(m.score||0),0)/validMetrics.length:0;
const confidence=clamp01(validMetrics.length/metrics.length);
const stance=validMetrics.length===0?'数据不足':composite>0.18?'偏多':composite<-0.18?'偏空':'中性';
const caution=[spreadAvailable&&spreadBps>5?'点差偏大':null,fundingAvailable&&Math.abs(funding)>0.0015?'资金费率极端':null,whaleCount>=12?'巨鲸转账活跃':null,!newsAvailable?'新闻样本不足':null,newsAvailable&&newsN===0&&newsFeedCount>0?'结构化事件仍在补齐':null,!flowAvailable||!spreadAvailable||!fundingAvailable||!basisAvailable?'部分微观结构数据缺失':null,String(news?.scope||'')==='global_fallback'?'新闻已回退到全市场样本':null].filter(Boolean);
researchState.lastSentiment={raw:payload,composite_score:composite,confidence,stance,metrics,spread_bps:spreadBps,imbalance,funding_rate:funding,basis_pct:basisPct,whale_count:whaleCount,news_events:newsEvents,news_feed_count:newsFeedCount,news_raw_count:newsRawCount};
if(summary){
summary.innerHTML=`<div class="list-item"><span>综合情绪 / 置信度</span><span>${stance} (${composite.toFixed(3)}) / ${confidence.toFixed(2)}</span></div><div class="list-item"><span>新闻事件(24h)</span><span>结构化 ${newsEvents} | 当前流 ${newsFeedCount} | 原始 ${newsRawCount}</span></div><div class="list-item"><span>资金费率 / 基差</span><span>${fundingAvailable?(funding*100).toFixed(4)+'%':'--'} / ${basisAvailable?basisPct.toFixed(4)+'%':'--'}</span></div><div class="list-item"><span>点差 / 主动流</span><span>${spreadAvailable?spreadBps.toFixed(3)+' bps':'--'} / ${flowAvailable?imbalance.toFixed(4):'--'}</span></div><div class="list-item"><span>风控提示</span><span>${esc(caution.join('；')||(validMetrics.length?'无明显异常':'数据不足，建议稍后重试'))}</span></div>`;
}
if(grid){
grid.innerHTML=metrics.map(m=>{const hasScore=Number.isFinite(Number(m.score));const positive=hasScore&&Number(m.score)>=0;const badgeText=!hasScore?'缺失':positive?'正向':'负向';const badgeClass=!hasScore?'warning':positive?'connected':'';return `<div class="strategy-card"><div class="list-item" style="padding:0 0 6px 0;border-bottom:none;"><h4>${esc(m.name)}</h4><span class="status-badge ${badgeClass}">${badgeText}</span></div><p>标准化分数：${hasScore?Number(m.score).toFixed(3):'--'}</p><p>原始值：${esc(String(m.fmt))}</p><p style="font-size:11px;color:#8fa6c0;">${esc(m.hint||'')}</p></div>`;}).join('');
}
renderMarketSentimentChart(validMetrics);
renderResearchConclusionCard();
}
async function loadMarketSentimentDashboard(){
const out=getResearchOutputEl();
try{
const ex=getResearchExchange(),sym=getResearchSymbol(),newsSym=symbolToNewsKey(sym);
const [micro,community,newsScoped,newsGlobal]=await Promise.allSettled([
api(`/trading/analytics/microstructure?exchange=${encodeURIComponent(ex)}&symbol=${encodeURIComponent(sym)}&depth_limit=20`,{timeoutMs:8000}),
api(`/trading/analytics/community/overview?exchange=${encodeURIComponent(ex)}&symbol=${encodeURIComponent(sym)}`,{timeoutMs:12000}),
api(`/news/summary?symbol=${encodeURIComponent(newsSym)}&hours=24`,{timeoutMs:15000}),
api(`/news/summary?hours=24`,{timeoutMs:15000}),
]);
let newsPayload=newsScoped.status==='fulfilled'?newsScoped.value:{error:newsScoped.reason?.message||'加载失败',sentiment:{positive:0,neutral:0,negative:0}};
const scopedTotal=Number(newsPayload?.events_count||0)+Number(newsPayload?.feed_count||0)+Number(newsPayload?.raw_count||0);
if((!scopedTotal||newsScoped.status!=='fulfilled')&&newsGlobal.status==='fulfilled'){
newsPayload={...newsGlobal.value,scope:'global_fallback'};
}
const payload={
exchange:ex,
symbol:sym,
timestamp:new Date().toISOString(),
microstructure:micro.status==='fulfilled'?micro.value:{available:false,error:micro.reason?.message||'加载失败'},
community:community.status==='fulfilled'?community.value:{error:community.reason?.message||'加载失败'},
news:newsPayload,
};
renderMarketSentimentPanel(payload);
renderResearchQuickSummary([{label:'情绪模块',value:'市场情绪仪表盘'},{label:'交易所',value:ex},{label:'标的',value:sym},{label:'新闻样本',value:`结构化 ${Number(payload.news?.events_count||0)} / 当前流 ${Number(payload.news?.feed_count||0)}`}]);
if(out)out.textContent=JSON.stringify(payload,null,2);
}catch(e){
renderMarketSentimentPanel({error:e.message});
if(out)out.textContent=`市场情绪加载失败: ${e.message}`;
notify(`市场情绪加载失败: ${e.message}`,true);
}
}
function applyResearchPreset(kind){
const tfEl=document.getElementById('research-timeframe'),lookbackEl=document.getElementById('research-lookback'),symbolsEl=document.getElementById('research-symbols'),symbolEl=document.getElementById('research-symbol');
const default30=['BTC/USDT','ETH/USDT','BNB/USDT','SOL/USDT','XRP/USDT','ADA/USDT','DOGE/USDT','TRX/USDT','LINK/USDT','AVAX/USDT','DOT/USDT','POL/USDT','LTC/USDT','BCH/USDT','ETC/USDT','ATOM/USDT','NEAR/USDT','APT/USDT','ARB/USDT','OP/USDT','SUI/USDT','INJ/USDT','RUNE/USDT','AAVE/USDT','MKR/USDT','UNI/USDT','FIL/USDT','HBAR/USDT','ICP/USDT','TON/USDT'];
if(kind==='hf30'){if(tfEl)tfEl.value='5m';if(lookbackEl)lookbackEl.value='1800';setSelectValues('research-symbols',default30);if(symbolEl&&!symbolEl.value.trim())symbolEl.value='BTC/USDT';notify('已应用预设: 高频30币 (5m / 1800)');return;}
if(kind==='intraday'){if(tfEl)tfEl.value='1m';if(lookbackEl)lookbackEl.value='1200';setSelectValues('research-symbols',default30.slice(0,15));if(symbolEl)symbolEl.value='BTC/USDT';notify('已应用预设: 盘中研究 (1m / 1200)');return;}
if(kind==='swing'){if(tfEl)tfEl.value='1h';if(lookbackEl)lookbackEl.value='1000';setSelectValues('research-symbols',default30);if(symbolEl)symbolEl.value='BTC/USDT';notify('已应用预设: 波段研究 (1h / 1000)');}
}
function bindResearchPresets(){
const b1=document.getElementById('btn-research-preset-hf'),b2=document.getElementById('btn-research-preset-intraday'),b3=document.getElementById('btn-research-preset-swing');
if(b1)b1.onclick=()=>applyResearchPreset('hf30');
if(b2)b2.onclick=()=>applyResearchPreset('intraday');
if(b3)b3.onclick=()=>applyResearchPreset('swing');
}
function bindResearchSentiment(){const b=document.getElementById('btn-load-market-sentiment');if(b)b.onclick=loadMarketSentimentDashboard;}
function renderResearchQuickSummary(rows){const box=getResearchSummaryEl();if(!box)return;if(!rows?.length){box.innerHTML='<div class="list-item"><span>暂无摘要</span><span>-</span></div>';return;}box.innerHTML=rows.map(r=>`<div class="list-item"><span>${esc(r.label||'-')}</span><span>${esc(String(r.value??'-'))}</span></div>`).join('');}
function getResearchConclusionSummaryEl(){return document.getElementById('research-conclusion-summary');}
function getResearchConclusionBulletsEl(){return document.getElementById('research-conclusion-bullets');}
function renderResearchConclusionCard(){
const summaryEl=getResearchConclusionSummaryEl(),bulletsEl=getResearchConclusionBulletsEl();
if(!summaryEl||!bulletsEl)return;
const factorData=(researchState.lastFactorLibrary&&!researchState.lastFactorLibrary.error)?researchState.lastFactorLibrary:null;
const multiData=(researchState.lastMultiAsset&&!researchState.lastMultiAsset.error)?researchState.lastMultiAsset:null;
const sentiment=researchState.lastSentiment&& !researchState.lastSentiment.error ? researchState.lastSentiment : null;
const analytics=(researchState.lastAnalytics&&!researchState.lastAnalytics.error)?researchState.lastAnalytics:null;
const onchain=(researchState.lastOnchain&&!researchState.lastOnchain.error)?researchState.lastOnchain:null;
if(!factorData&&!multiData&&!sentiment&&!analytics){
summaryEl.innerHTML='<div class="list-item"><span>状态</span><span>等待研究结果</span></div>';
bulletsEl.innerHTML='<div class="research-conclusion-empty">暂无结论。建议先运行“研究总览”，或至少执行“刷新因子库 + 多币种概览 + 市场情绪仪表盘”。</div>';
return;
}
const latest=factorData?.latest||{};
const factorMom=Number(latest.MOM??latest.momentum_fast??0);
const factorMkt=Number(latest.MKT??0);
const factorTrend=(Number.isFinite(factorMom)?factorMom:0)*0.65+(Number.isFinite(factorMkt)?factorMkt:0)*0.35;
const factorPoints=Number(factorData?.points||0);
const factorCount=Array.isArray(factorData?.factors)?factorData.factors.length:0;
const factorQuality=String(factorData?.universe_quality||'-');
const retiredExcluded=(Array.isArray(factorData?.retired_filter?.excluded_symbols)?factorData.retired_filter.excluded_symbols:[]);
const assets=Array.isArray(multiData?.assets)?multiData.assets:[];
const posCount=assets.filter(a=>Number(a?.return_pct||0)>0).length;
const breadth=assets.length?(posCount/assets.length):0;
const avgRet=assets.length?assets.reduce((s,a)=>s+Number(a?.return_pct||0),0)/assets.length:0;
const avgVol=assets.length?assets.reduce((s,a)=>s+Number(a?.volatility_pct||0),0)/assets.length:0;
const corrMap=multiData?.correlation||{};
const corrNames=Object.keys(corrMap||{});
let corrSum=0,corrN=0;
for(let i=0;i<corrNames.length;i++){for(let j=i+1;j<corrNames.length;j++){const v=Math.abs(Number(corrMap?.[corrNames[i]]?.[corrNames[j]]??0));if(Number.isFinite(v)){corrSum+=v;corrN++;}}}
const avgAbsCorr=corrN?corrSum/corrN:0;
const sentimentComposite=Number(sentiment?.composite_score??0);
const sentimentConfidence=Number(sentiment?.confidence??0);
const riskLevel=(analytics?.risk_level||analytics?.risk_dashboard?.risk_level||analytics?.risk_dashboard?.data?.risk_level||'').toString();
const whaleCount=Number(onchain?.whale_activity?.count||0);
const timeframe=getResearchTimeframe();
const rows=[
{label:'推荐策略',value:'计算中'},
{label:'研究口径',value:`${getResearchExchange()} / ${timeframe}`},
{label:'因子覆盖',value:`${factorCount||0} 因子 / ${factorPoints||0} 点`},
{label:'停更过滤',value:`${getResearchExcludeRetired()?'开启':'关闭'}${retiredExcluded.length?`（排除${retiredExcluded.length}）`:''}`},
{label:'市场广度',value:assets.length?`${posCount}/${assets.length} 上涨 (${(breadth*100).toFixed(0)}%)`:'未加载'},
{label:'情绪面',value:sentiment?`${sentiment.stance||'中性'} (${sentimentComposite.toFixed(3)} / ${sentimentConfidence.toFixed(2)})`:'未加载'},
];
let strategy='观望/轻仓';
let strategyTag='谨慎';
let horizon=(timeframe==='1m')?'5m-15m':(timeframe==='5m'?'15m-1h':timeframe);
let positionAdvice='基础仓位 20%-35%，等待更清晰信号';
const reasons=[];
if(factorData){reasons.push(`因子趋势偏向 ${(factorTrend>=0?'正':'负')}（MOM=${factorMom.toFixed(6)}, MKT=${factorMkt.toFixed(6)}）`);}
if(multiData){reasons.push(`多币种广度 ${(breadth*100).toFixed(0)}%，平均收益 ${avgRet.toFixed(2)}%，平均波动 ${avgVol.toFixed(2)}%`);}
if(corrN){reasons.push(`平均绝对相关性 ${avgAbsCorr.toFixed(3)}（越高越要控制总仓位）`);}
if(sentiment){reasons.push(`情绪面 ${sentiment.stance||'中性'}，置信度 ${sentimentConfidence.toFixed(2)}`);}
if(whaleCount>0){reasons.push(`链上巨鲸笔数 ${whaleCount}（短时冲击风险需留意）`);}

const highVol=avgVol>6;
const highCorr=avgAbsCorr>0.72;
const strongBull=(factorTrend>0.0005||breadth>=0.6||avgRet>0.8) && sentimentComposite>-0.2;
const strongBear=(factorTrend<-0.0005||breadth<=0.4||avgRet<-0.8) && sentimentComposite<0.2;
const noisyMinute=(timeframe==='1m'||timeframe.endsWith('s'));
if(strongBull && !highVol){
strategy=noisyMinute?'多因子顺势轮动（建议放大到5m/15m执行）':'多因子顺势轮动（偏多）';
strategyTag='进攻';
positionAdvice=highCorr?'基础仓位 25%-40%，分散仓位但降低总杠杆':'基础仓位 35%-60%，按得分分层建仓';
}else if(strongBear && !highVol){
strategy='多因子防守/合约空头（仅在可做空账户）';
strategyTag='防守';
positionAdvice=highCorr?'总风险预算收紧到 20%-30%，优先主流币空头或对冲':'可用 25%-45% 风险预算，优先强弱分化明显标的';
}else if(highVol || (Math.abs(factorTrend)<0.0002 && avgAbsCorr<0.55 && assets.length>=8)){
strategy='均值回归 / 区间交易（轻仓）';
strategyTag='震荡';
positionAdvice='基础仓位 15%-30%，严格止损，避免追涨杀跌';
}else{
strategy='观望或低频确认后再入场';
strategyTag='等待';
positionAdvice='先刷新情绪/微观结构确认，再决定是否切换到顺势或回归';
}
if(String(riskLevel).toLowerCase()==='high' || String(riskLevel).toLowerCase()==='critical'){
strategy='风控优先：降低仓位/停止新开仓';
strategyTag='风控';
positionAdvice='仅允许减仓和风险回收，暂停新增风险敞口';
}
rows[0].value=strategy;
rows.push({label:'建议执行周期',value:horizon});
rows.push({label:'仓位建议',value:positionAdvice});
summaryEl.innerHTML=rows.map(r=>`<div class="list-item"><span>${esc(r.label||'-')}</span><span>${esc(String(r.value??'-'))}</span></div>`).join('');
const bullets=[
{title:'结论',badge:strategyTag,body:strategy},
{title:'依据',badge:'规则汇总',body:reasons.filter(Boolean).join('；')||'暂无足够样本。'},
{title:'执行建议',badge:'风险预算',body:positionAdvice},
];
if(factorData?.warnings?.length){
bullets.push({title:'数据提醒',badge:'注意',body:String(factorData.warnings[0])});
}
if(retiredExcluded.length){
bullets.push({title:'样本过滤',badge:'已排除',body:`已按本地覆盖审计结果排除停更/退市币种：${retiredExcluded.join(', ')}`});
}
bulletsEl.innerHTML=bullets.map(item=>`<div class="research-conclusion-item"><div class="title"><span>${esc(item.title)}</span><span class="status-badge">${esc(item.badge)}</span></div><div class="body">${esc(item.body)}</div></div>`).join('');
}
function downloadTextFile(filename,text,type='text/plain;charset=utf-8'){try{const blob=new Blob([text],{type});const a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download=filename;document.body.appendChild(a);a.click();setTimeout(()=>{URL.revokeObjectURL(a.href);a.remove();},0);}catch(e){notify(`导出失败: ${e.message}`,true);}}
function getFactorTableSearch(){return (document.getElementById('factor-score-search')?.value||'').trim().toUpperCase();}
function getFactorTableSort(){return document.getElementById('factor-score-sort')?.value||'score_desc';}
function getFactorTopN(){return Math.max(10,Math.min(200,Number(document.getElementById('factor-score-topn')?.value||80)));}
function _factorSortValue(row,key){const v=Number(row?.[key]??0);return Number.isFinite(v)?v:0;}
function getFilteredSortedFactorRows(data){
const rows=[...(data?.asset_scores||[])];
const q=getFactorTableSearch();
let out=rows.filter(r=>!q||String(r?.symbol||'').toUpperCase().includes(q));
const sortKey=(getFactorTableSort()||'score_desc');
const [field,dirRaw]=sortKey.split('_');
const dir=(dirRaw||'desc')==='asc'?1:-1;
out.sort((a,b)=>{
const av=_factorSortValue(a,field||'score'),bv=_factorSortValue(b,field||'score');
if(av===bv)return String(a?.symbol||'').localeCompare(String(b?.symbol||''));
return (av-bv)*dir;
});
return out.slice(0,getFactorTopN());
}
function renderFactorCorrelationHeatmap(data){
const el=document.getElementById('factor-corr-chart');
if(!el||typeof Plotly==='undefined')return;
const corr=data?.correlation||{};
const factors=(data?.factors||[]).filter(k=>corr&&corr[k]);
if(!factors.length){el.innerHTML='<div class="list-item">暂无因子相关性矩阵</div>';return;}
preparePlotlyHost(el);
const z=factors.map(r=>factors.map(c=>Number(corr?.[r]?.[c]??0)));
Plotly.newPlot(el,[{type:'heatmap',x:factors,y:factors,z,colorscale:[[0,'#b22222'],[.5,'#1f2937'],[1,'#0ea5a4']],zmin:-1,zmax:1,hovertemplate:'%{y} vs %{x}: %{z:.3f}<extra></extra>'}],{paper_bgcolor:'#111723',plot_bgcolor:'#111723',font:{color:'#d7dde8'},margin:{l:70,r:30,t:20,b:60},xaxis:{tickangle:-35},yaxis:{autorange:'reversed'}},{responsive:true,displaylogo:false});
schedulePlotlyResize(document.getElementById('research')||document);
}
function renderMultiAssetCorrelationHeatmap(data){
const el=document.getElementById('multi-asset-corr-chart');
if(!el||typeof Plotly==='undefined')return;
const corr=data?.correlation||{};
const assets=Object.keys(corr||{}).filter(k=>corr&&typeof corr[k]==='object');
if(!assets.length){el.innerHTML='<div class="list-item">暂无多币种收益相关性矩阵</div>';return;}
preparePlotlyHost(el);
const z=assets.map(r=>assets.map(c=>Number(corr?.[r]?.[c]??0)));
Plotly.newPlot(el,[{type:'heatmap',x:assets,y:assets,z,colorscale:[[0,'#a61b29'],[.5,'#1f2937'],[1,'#0f766e']],zmin:-1,zmax:1,hovertemplate:'%{y} vs %{x}: %{z:.3f}<extra></extra>'}],{paper_bgcolor:'#111723',plot_bgcolor:'#111723',font:{color:'#d7dde8'},margin:{l:80,r:30,t:20,b:80},xaxis:{tickangle:-35},yaxis:{autorange:'reversed'}},{responsive:true,displaylogo:false});
schedulePlotlyResize(document.getElementById('research')||document);
}
function renderMultiAssetPanel(data){
researchState.lastMultiAsset=data&&typeof data==='object'?data:null;
const summary=document.getElementById('multi-asset-summary');
const tbody=document.getElementById('multi-asset-tbody');
if(summary)summary.innerHTML='<div class="list-item"><span>加载中...</span><span>-</span></div>';
if(tbody)tbody.innerHTML='<tr><td colspan="6">加载中...</td></tr>';
if(!data||data.error){
const msg=data?.error||'多币种概览加载失败';
if(summary)summary.innerHTML=`<div class="list-item"><span>${esc(msg)}</span><span>错误</span></div>`;
if(tbody)tbody.innerHTML=`<tr><td colspan="6">${esc(msg)}</td></tr>`;
const corrEl=document.getElementById('multi-asset-corr-chart');if(corrEl)corrEl.innerHTML='<div class="list-item">暂无多币种收益相关性矩阵</div>';
renderResearchConclusionCard();
return;
}
const rows=Array.isArray(data?.assets)?data.assets:[];
const best=rows.length?[...rows].sort((a,b)=>Number(b.return_pct||0)-Number(a.return_pct||0))[0]:null;
const worst=rows.length?[...rows].sort((a,b)=>Number(a.return_pct||0)-Number(b.return_pct||0))[0]:null;
const avgRet=rows.length?rows.reduce((s,r)=>s+Number(r.return_pct||0),0)/rows.length:0;
const avgVol=rows.length?rows.reduce((s,r)=>s+Number(r.volatility_pct||0),0)/rows.length:0;
const corrMap=data?.correlation||{},corrNames=Object.keys(corrMap||{});
let avgAbsCorr=0,corrPairs=0;
for(let i=0;i<corrNames.length;i++){for(let j=i+1;j<corrNames.length;j++){avgAbsCorr+=Math.abs(Number(corrMap?.[corrNames[i]]?.[corrNames[j]]??0));corrPairs++;}}
avgAbsCorr=corrPairs?avgAbsCorr/corrPairs:0;
if(summary){
summary.innerHTML=`<div class="list-item"><span>币种数量 / 周期</span><span>${Number(data?.count||rows.length)} / ${esc(data?.timeframe||'-')}</span></div><div class="list-item"><span>平均收益 / 平均波动</span><span>${avgRet.toFixed(2)}% / ${avgVol.toFixed(2)}%</span></div><div class="list-item"><span>最佳 / 最差</span><span>${esc(best?.symbol||'-')} ${Number(best?.return_pct||0).toFixed(2)}% / ${esc(worst?.symbol||'-')} ${Number(worst?.return_pct||0).toFixed(2)}%</span></div><div class="list-item"><span>平均绝对相关性</span><span>${avgAbsCorr.toFixed(3)}</span></div>`;
}
if(tbody){
tbody.innerHTML=rows.length?rows.map(r=>`<tr><td>${esc(r.symbol||'-')}</td><td>${Number(r.return_pct||0).toFixed(4)}</td><td>${Number(r.volatility_pct||0).toFixed(4)}</td><td>${Number(r.max_drawdown_pct||0).toFixed(4)}</td><td>${Number(r.avg_volume||0).toFixed(4)}</td><td>${Number(r.last||0).toFixed(6)}</td></tr>`).join(''):'<tr><td colspan="6">暂无多币种数据</td></tr>';
}
renderMultiAssetCorrelationHeatmap(data);
renderResearchConclusionCard();
}
function exportFactorLibrary(kind='json'){
const data=researchState.lastFactorLibrary;
if(!data||data.error){notify('请先加载因子库',true);return;}
const stamp=new Date().toISOString().replace(/[:.]/g,'-');
if(kind==='json'){downloadTextFile(`factor_library_${stamp}.json`,JSON.stringify(data,null,2),'application/json;charset=utf-8');return;}
if(kind==='csv'){
const rows=getFilteredSortedFactorRows(data);
const cols=['symbol','score','momentum','value','quality','low_vol','liquidity','low_beta','size'];
const head=cols.join(',');
const body=rows.map(r=>cols.map(c=>{const v=r?.[c]??'';const s=String(v);return /[",\n]/.test(s)?`"${s.replace(/"/g,'""')}"`:s;}).join(',')).join('\n');
downloadTextFile(`factor_scores_${stamp}.csv`,`${head}\n${body}`,'text/csv;charset=utf-8');
return;
}
const corrKeys=(data?.factors||[]);const topRows=getFilteredSortedFactorRows(data).slice(0,20);
const lines=[
`# 因子库报告`,
`生成时间: ${new Date().toLocaleString('zh-CN',{hour12:false})}`,
`交易所/周期: ${data.exchange||'-'} / ${data.timeframe||'-'}`,
`覆盖币种: ${(data.symbols_used||[]).length}`,
`样本点: ${Number(data.points||0)}`,
`因子: ${(data.factors||[]).join(', ')}`,
``,
`## 因子最新值`,
...Object.keys(data.latest||{}).map(k=>`- ${k}: latest=${Number(data.latest[k]||0).toFixed(6)} mean24=${Number((data.mean_24||{})[k]||0).toFixed(6)} std24=${Number((data.std_24||{})[k]||0).toFixed(6)}`),
``,
`## 资产打分 Top20（当前筛选）`,
`| symbol | score | momentum | value | quality | low_vol | liquidity | low_beta | size |`,
`|---|---:|---:|---:|---:|---:|---:|---:|---:|`,
...topRows.map(r=>`| ${r.symbol||'-'} | ${Number(r.score||0).toFixed(6)} | ${Number(r.momentum||0).toFixed(6)} | ${Number(r.value||0).toFixed(6)} | ${Number(r.quality||0).toFixed(6)} | ${Number(r.low_vol||0).toFixed(6)} | ${Number(r.liquidity||0).toFixed(6)} | ${Number(r.low_beta||0).toFixed(6)} | ${Number(r.size||0).toFixed(6)} |`),
``,
`## 因子相关性（摘要）`,
...corrKeys.slice(0,12).map(k=>{const row=data?.correlation?.[k]||{};const peers=corrKeys.filter(x=>x!==k).map(x=>({k:x,v:Number(row?.[x]??0)})).sort((a,b)=>Math.abs(b.v)-Math.abs(a.v)).slice(0,3);return `- ${k}: ${peers.map(p=>`${p.k}=${p.v.toFixed(3)}`).join(', ')}`;})
];
downloadTextFile(`factor_report_${stamp}.md`,lines.join('\n'),'text/markdown;charset=utf-8');
}
function rerenderFactorLibraryFromCache(){if(researchState.lastFactorLibrary&&!researchState.lastFactorLibrary.error)renderFactorLibraryPanel(researchState.lastFactorLibrary);}
function renderFactorLibraryPanel(data){
const summary=document.getElementById('factor-library-summary'),grid=document.getElementById('factor-library-grid'),tbody=document.getElementById('factor-scores-tbody');
if(summary)summary.innerHTML='<div class="list-item"><span>加载中...</span><span>-</span></div>';
if(grid)grid.innerHTML='';
if(tbody)tbody.innerHTML='<tr><td colspan="9">加载中...</td></tr>';
researchState.lastFactorLibrary=data&&typeof data==='object'?data:null;
if(!data||data.error){
const msg=data?.error||'因子库加载失败';
if(summary)summary.innerHTML=`<div class="list-item"><span>${esc(msg)}</span><span>错误</span></div>`;
if(tbody)tbody.innerHTML=`<tr><td colspan="9">${esc(msg)}</td></tr>`;
const corr=document.getElementById('factor-corr-chart');if(corr)corr.innerHTML='<div class="list-item">暂无因子相关性矩阵</div>';
renderResearchConclusionCard();
return;
}
const latest=data.latest||{},mean=data.mean_24||{},std=data.std_24||{};
if(summary){
summary.innerHTML=`
<div class="list-item"><span>交易所 / 周期</span><span>${esc(data.exchange||'-')} / ${esc(data.timeframe||'-')}</span></div>
<div class="list-item"><span>因子数量 / 有效时间点</span><span>${(data.factors||[]).length} / ${Number(data.points||0)}</span></div>
<div class="list-item"><span>币种覆盖</span><span>${(data.symbols_used||[]).length} 个</span></div>
<div class="list-item"><span>相关性矩阵</span><span>${data.correlation?'已加载':'无'}</span></div>
<div class="list-item"><span>说明</span><span>有效时间点=多币种对齐后可计算因子的 bar 数</span></div>
<div class="list-item"><span>风险提示</span><span>${esc((data.warnings||[])[0]||'无')}</span></div>`;
}
const factorKeys=Object.keys(latest||{});
if(grid){
grid.innerHTML=factorKeys.length?factorKeys.map(k=>{
const v=Number(latest[k]||0),m=Number(mean[k]||0),s=Number(std[k]||0);
return `<div class="strategy-card"><div class="list-item" style="padding:0 0 6px 0;border-bottom:none;"><h4>${esc(k)}</h4><span class="status-badge ${v>=0?'connected':'negative'}">${v>=0?'偏多':'偏空'}</span></div><p>最新值: ${v.toFixed(6)}</p><p>24h均值: ${m.toFixed(6)}</p><p>24h波动: ${s.toFixed(6)}</p></div>`;
}).join(''):'<div class="list-item">暂无因子指标</div>';
}
if(tbody){
const rows=getFilteredSortedFactorRows(data);
tbody.innerHTML=rows.length?rows.map(r=>`<tr><td>${esc(r.symbol||'-')}</td><td>${Number(r.score||0).toFixed(6)}</td><td>${Number(r.momentum||0).toFixed(6)}</td><td>${Number(r.value||0).toFixed(6)}</td><td>${Number(r.quality||0).toFixed(6)}</td><td>${Number(r.low_vol||0).toFixed(6)}</td><td>${Number(r.liquidity||0).toFixed(6)}</td><td>${Number(r.low_beta||0).toFixed(6)}</td><td>${Number(r.size||0).toFixed(6)}</td></tr>`).join(''):'<tr><td colspan="9">暂无币种打分</td></tr>';
}
renderFactorCorrelationHeatmap(data);
renderResearchConclusionCard();
}
function normalizeAnalyticsModules(data){
if(!data||typeof data!=='object')return {};
if(data.modules&&typeof data.modules==='object')return data.modules;
return {
performance:{ok:true,data:data.performance||{}},
risk_dashboard:{ok:true,data:data.risk_dashboard||{}},
calendar:{ok:true,data:data.calendar||{}},
microstructure:{ok:true,data:data.microstructure||{}},
equity_rebalance:{ok:true,data:data.equity_rebalance||{}},
community:{ok:true,data:data.community||{}},
behavior_report:{ok:true,data:data.behavior_report||{}},
stoploss_policy:{ok:true,data:data.stoploss_policy||{}},
};
}
function analyticsModuleNameZh(name){
const map={
performance:'绩效分析',
risk_dashboard:'风险仪表盘',
calendar:'交易日历',
microstructure:'微观结构',
equity_rebalance:'资金曲线/再平衡',
community:'信息聚合',
behavior_report:'行为报告',
stoploss_policy:'止损策略'
};
return map[name]||String(name||'未知模块');
}
function moduleBrief(name,payload){
const d=payload?.data||payload||{};
if(name==='performance'){const wr=d?.win_rate_breakdown?.overall??d?.win_rate??0,sh=d?.risk_adjusted?.sharpe??d?.sharpe_ratio??0;return `胜率 ${Number(wr).toFixed(2)}% | Sharpe ${Number(sh).toFixed(2)}`;}
if(name==='risk_dashboard'){return `风险 ${esc(d?.risk_level||'未知')} | VaR95 ${Number(d?.var?.var95_pct??0).toFixed(2)}%`;}
if(name==='calendar'){return `事件 ${Number((d?.count??((d?.events||[]).length||0)))} 条`;}
if(name==='microstructure'){return `点差 ${Number(d?.orderbook?.spread_bps??0).toFixed(2)} bps`;}
if(name==='equity_rebalance'){return `建议 ${Number((d?.rebalance?.suggestions||[]).length)} 条`;}
if(name==='community'){return `巨鲸 ${Number(d?.whale_transfers?.count??0)} | 公告 ${Number((d?.announcements||[]).length)}`;}
if(name==='behavior_report'){return `冲动占比 ${Number((d?.impulsive_ratio||0)*100).toFixed(2)}%`;}
if(name==='stoploss_policy'){return `建议仓位 ${Number((d?.position_suggestions||[]).length)}`;}
return `数据项 ${Object.keys(d||{}).length}`;
}
function renderAnalyticsOverviewPanel(data){
const out=getResearchOutputEl(),summary=document.getElementById('analytics-overview-summary'),grid=document.getElementById('analytics-module-grid');
if(!out)return;
researchState.lastAnalytics=data&&typeof data==='object'?data:null;
if(!data||data.error){if(summary)summary.innerHTML=`<div class="list-item"><span>${esc(data?.error||'分析总览加载失败')}</span><span>错误</span></div>`;if(grid)grid.innerHTML='<div class="list-item">暂无分析模块</div>';out.textContent=`分析总览加载失败: ${data?.error||'未知错误'}`;renderResearchConclusionCard();return;}
const modules=normalizeAnalyticsModules(data),entries=Object.entries(modules);
const okCount=entries.filter(([,v])=>v?.ok!==false).length,totalCount=entries.length;
if(summary){
summary.innerHTML=`
<div class="list-item"><span>模块状态</span><span>${okCount}/${totalCount} 正常</span></div>
<div class="list-item"><span>更新时间</span><span>${new Date(data.timestamp||Date.now()).toLocaleString('zh-CN')}</span></div>
<div class="list-item"><span>总览来源</span><span>${data.all_ok!==undefined?'系统聚合':'模块回落聚合'}</span></div>
<div class="list-item"><span>异常模块</span><span>${entries.filter(([,v])=>v?.ok===false).map(([k])=>analyticsModuleNameZh(k)).join('、')||'无'}</span></div>`;
}
if(grid){
grid.innerHTML=entries.length?entries.map(([name,payload])=>`<div class="strategy-card"><div class="list-item" style="padding:0 0 6px 0;border-bottom:none;"><h4>${esc(analyticsModuleNameZh(name))}</h4><span class="status-badge ${payload?.ok===false?'':'connected'}">${payload?.ok===false?'异常':'正常'}</span></div><p>${esc(moduleBrief(name,payload))}</p><p style="font-size:11px;color:#8fa6c0;">耗时 ${Number(payload?.latency_ms||0).toFixed(1)} ms</p></div>`).join(''):'<div class="list-item">暂无分析模块</div>';
}
const perf=(modules.performance||{}).data||{},risk=(modules.risk_dashboard||{}).data||{},calendar=(modules.calendar||{}).data||{},micro=(modules.microstructure||{}).data||{},equity=(modules.equity_rebalance||{}).data||{},community=(modules.community||{}).data||{};
out.textContent=JSON.stringify({
更新时间:new Date().toISOString(),
绩效摘要:{交易数:perf.trade_count||perf.total_trades,胜率:perf?.win_rate_breakdown?.overall||perf.win_rate,total_return_pct:perf.total_return_pct,sharpe:perf?.risk_adjusted?.sharpe||perf.sharpe_ratio,max_drawdown_pct:perf?.drawdown?.max_drawdown_pct||perf.max_drawdown_pct},
风险摘要:{risk_level:risk.risk_level||data.risk_level,var95:risk?.var?.var95_pct,总风险敞口:risk.exposure_pct_of_equity,隐含杠杆:risk?.leverage?.implicit},
日历事件数:calendar.count||0,
微观结构:{spread_bps:micro?.orderbook?.spread_bps,imbalance:micro?.aggressor_flow?.imbalance,large_orders:(micro?.large_orders||[]).length},
资金曲线:{points:(equity?.equity_curve||[]).length,再平衡建议:(equity?.rebalance?.suggestions||[]).length},
社区信息:{whale_count:community?.whale_transfers?.count,announcements:(community?.announcements||[]).length}
},null,2);
renderResearchConclusionCard();
}
function renderAnalyticsModuleDetail(endpoint,data){
const out=getResearchOutputEl();if(!out)return;
const rows=[];
if(endpoint.includes('/analytics/performance'))rows.push({label:'绩效摘要',value:`交易 ${Number(data?.trade_count||0)} | Sharpe ${Number(data?.risk_adjusted?.sharpe||0).toFixed(2)}`});
if(endpoint.includes('/analytics/risk-dashboard'))rows.push({label:'风险摘要',value:`等级 ${data?.risk_level||'未知'} | VaR95 ${Number(data?.var?.var95_pct||0).toFixed(2)}%`});
if(endpoint.includes('/analytics/calendar'))rows.push({label:'交易日历',value:`事件 ${Number(data?.count||0)} 条`});
if(endpoint.includes('/analytics/microstructure'))rows.push({label:'微观结构',value:`点差 ${Number(data?.orderbook?.spread_bps||0).toFixed(2)} bps`});
if(endpoint.includes('/analytics/equity/rebalance'))rows.push({label:'再平衡',value:`建议 ${(data?.rebalance?.suggestions||[]).length} 条`});
if(endpoint.includes('/analytics/community/overview'))rows.push({label:'社区信息',value:`巨鲸 ${Number(data?.whale_transfers?.count||0)} | 公告 ${(data?.announcements||[]).length}`});
if(endpoint.includes('/analytics/behavior/report'))rows.push({label:'行为报告',value:`冲动占比 ${Number((data?.impulsive_ratio||0)*100).toFixed(2)}%`});
if(endpoint.includes('/analytics/stoploss/policy'))rows.push({label:'止损策略',value:`建议仓位 ${(data?.position_suggestions||[]).length}`});
if(rows.length)renderResearchQuickSummary(rows);
out.textContent=JSON.stringify(data,null,2);
}
async function loadAnalyticsPanel(endpoint){
const out=getResearchOutputEl();
if(!out)return;
try{
const timeoutMs=String(endpoint||'').includes('/trading/analytics/overview')?45000:32000;
const d=await api(endpoint,{timeoutMs});
if(String(endpoint||'').includes('/trading/analytics/overview')){renderAnalyticsOverviewPanel(d);}else{renderAnalyticsModuleDetail(endpoint,d);}
schedulePlotlyResize(document.getElementById('research')||document);
}catch(e){
if(!String(endpoint||'').includes('/trading/analytics/overview')){
try{
const ex=getResearchExchange(),s=getResearchSymbol();
const fb=await api(`/trading/analytics/overview?days=90&lookback=240&calendar_days=30&exchange=${encodeURIComponent(ex)}&symbol=${encodeURIComponent(s)}`,{timeoutMs:45000});
renderAnalyticsOverviewPanel(fb);
notify('目标模块加载失败，已自动降级到分析总览',true);
return;
}catch{}
}
out.textContent=`加载失败: ${e.message}`;
notify(`加载失败: ${e.message}`,true);
}
}
async function loadResearchOverview(){
const out=getResearchOutputEl();
if(!out)return;
try{
const ex=getResearchExchange(),symbol=getResearchSymbol(),timeframe=getResearchTimeframe(),lookback=getResearchLookback(),symbolsArr=getResearchSymbols(),symbols=symbolsArr.join(','),excludeRetired=getResearchExcludeRetired();
const factorLookback=getFactorLookbackForTimeframe(timeframe,lookback);
const factorTimeoutMs=getFactorApiTimeoutMs(timeframe,symbolsArr.length);
const [analytics,multi,factors,onchain]=await Promise.allSettled([
api(`/trading/analytics/overview?days=90&lookback=240&calendar_days=30&exchange=${encodeURIComponent(ex)}&symbol=${encodeURIComponent(symbol)}`,{timeoutMs:45000}),
api(`/data/multi-assets/overview?exchange=${encodeURIComponent(ex)}&symbols=${encodeURIComponent(symbols)}&timeframe=${encodeURIComponent(timeframe)}&lookback=${Math.min(2000,lookback)}&exclude_retired=${excludeRetired?'true':'false'}`,{timeoutMs:25000}),
api(`/data/factors/library?exchange=${encodeURIComponent(ex)}&symbols=${encodeURIComponent(symbols)}&timeframe=${encodeURIComponent(timeframe)}&lookback=${factorLookback}&quantile=0.3&series_limit=500&exclude_retired=${excludeRetired?'true':'false'}`,{timeoutMs:factorTimeoutMs}),
api(`/data/onchain/overview?exchange=${encodeURIComponent(ex)}&symbol=${encodeURIComponent(symbol)}&whale_threshold_btc=100&chain=Ethereum`,{timeoutMs:25000}),
]);
const summary={
timestamp:new Date().toISOString(),
config:{exchange:ex,symbol,timeframe,lookback,symbols:symbols.split(','),factor_lookback:factorLookback,exclude_retired:excludeRetired},
analytics:analytics.status==='fulfilled'?analytics.value:{error:analytics.reason?.message||'加载失败'},
multi_assets:multi.status==='fulfilled'?multi.value:{error:multi.reason?.message||'加载失败'},
factor_library:factors.status==='fulfilled'?factors.value:{error:factors.reason?.message||'加载失败'},
onchain:onchain.status==='fulfilled'?onchain.value:{error:onchain.reason?.message||'加载失败'}
};
researchState.lastOverview=summary;
researchState.lastOnchain=summary.onchain;
renderAnalyticsOverviewPanel(summary.analytics);
renderFactorLibraryPanel(summary.factor_library);
renderMultiAssetPanel(summary.multi_assets);
renderResearchQuickSummary([
{label:'多币种覆盖',value:Number(summary.multi_assets?.count||0)},
{label:'过滤停更',value:summary.config.exclude_retired?'开启':'关闭'},
{label:'排除币种',value:Number(summary.factor_library?.retired_filter?.excluded_symbols?.length||summary.multi_assets?.retired_filter?.excluded_symbols?.length||0)},
{label:'链上巨鲸笔数',value:Number(summary.onchain?.whale_activity?.count||0)},
{label:'交易所',value:summary.config.exchange},
{label:'周期',value:summary.config.timeframe},
]);
const moduleStatus=[
`analytics:${summary.analytics?.error?'失败':'成功'}`,
`multi_assets:${summary.multi_assets?.error?'失败':'成功'}`,
`factor_library:${summary.factor_library?.error?'失败':'成功'}`,
`onchain:${summary.onchain?.error?'失败':'成功'}`
].join(' | ');
out.textContent=[
'研究总览说明：',
'- 用途：一次性刷新研究页核心模块（分析总览 / 因子库 / 多币种概览 / 链上概览）',
`- 参数：${summary.config.exchange} | ${summary.config.symbol} | ${summary.config.timeframe} | lookback=${summary.config.lookback} | 排除停更=${summary.config.exclude_retired?'是':'否'}`,
`- 模块状态：${moduleStatus}`,
'- 下方为完整原始结果（用于排查与导出）',
'',
JSON.stringify(summary,null,2)
].join('\n');
schedulePlotlyResize(document.getElementById('research')||document);
renderResearchConclusionCard();
notify('研究总览已更新');
}catch(e){
out.textContent=`研究总览加载失败: ${e.message}`;
notify(`研究总览加载失败: ${e.message}`,true);
}
}
async function logBehaviorJournal(){const out=getResearchOutputEl();try{const payload={mood:document.getElementById('behavior-mood')?.value||'neutral',confidence:Number(document.getElementById('behavior-confidence')?.value||0.5),plan_adherence:Number(document.getElementById('behavior-plan')?.value||0.5),note:(document.getElementById('behavior-note')?.value||'').trim(),symbol:getResearchSymbol()};const d=await api('/trading/analytics/behavior/journal',{method:'POST',body:JSON.stringify(payload)});renderResearchQuickSummary([{label:'行为记录',value:'已保存'},{label:'情绪',value:payload.mood},{label:'自信度',value:payload.confidence.toFixed(2)}]);if(out)out.textContent=JSON.stringify(d,null,2);notify('行为记录已保存');}catch(e){if(out)out.textContent=`行为记录失败: ${e.message}`;notify(`行为记录失败: ${e.message}`,true);}}
function bindResearchPanel(){
const o0=document.getElementById('btn-load-research-overview');
const o=document.getElementById('btn-load-analytics-overview');
const b1=document.getElementById('btn-load-performance');
const b2=document.getElementById('btn-load-risk-dashboard');
const b3=document.getElementById('btn-load-calendar');
const b4=document.getElementById('btn-load-microstructure');
const b5=document.getElementById('btn-load-equity-rebalance');
const b6=document.getElementById('btn-load-community');
const b7=document.getElementById('btn-behavior-log');
const b8=document.getElementById('btn-load-behavior-report');
const b9=document.getElementById('btn-load-stoploss-policy');
const m1=document.getElementById('btn-multi-asset');
const m2=document.getElementById('btn-fama-factors');
const m3=document.getElementById('btn-onchain-overview');
const m4=document.getElementById('btn-factor-library');
const fxj=document.getElementById('btn-factor-export-json');
const fxc=document.getElementById('btn-factor-export-csv');
const fxr=document.getElementById('btn-factor-export-report');
const fs=document.getElementById('factor-score-search');
const fo=document.getElementById('factor-score-sort');
const fn=document.getElementById('factor-score-topn');
const rex=document.getElementById('research-exchange');

if(rex){
rex.onchange=()=>loadResearchSymbolOptions(rex.value);
loadResearchSymbolOptions(rex.value);
}else{
loadResearchSymbolOptions(getResearchExchange());
}

if(o0)o0.onclick=loadResearchOverview;
if(o)o.onclick=()=>{
const ex=getResearchExchange(),s=getResearchSymbol();
loadAnalyticsPanel(`/trading/analytics/overview?days=90&lookback=240&calendar_days=30&exchange=${encodeURIComponent(ex)}&symbol=${encodeURIComponent(s)}`);
};
if(b1)b1.onclick=()=>loadAnalyticsPanel('/trading/analytics/performance?days=90');
if(b2)b2.onclick=()=>loadAnalyticsPanel('/trading/analytics/risk-dashboard?lookback=240');
if(b3)b3.onclick=()=>loadAnalyticsPanel('/trading/analytics/calendar?days=30');
if(b4)b4.onclick=()=>{
const ex=getResearchExchange(),s=getResearchSymbol();
loadAnalyticsPanel(`/trading/analytics/microstructure?exchange=${encodeURIComponent(ex)}&symbol=${encodeURIComponent(s)}&depth_limit=80`);
};
if(b5)b5.onclick=()=>loadAnalyticsPanel('/trading/analytics/equity/rebalance?hours=168&target_alloc=BTC:0.4,ETH:0.3,USDT:0.3');
if(b6)b6.onclick=()=>{
const ex=getResearchExchange(),s=getResearchSymbol();
loadAnalyticsPanel(`/trading/analytics/community/overview?exchange=${encodeURIComponent(ex)}&symbol=${encodeURIComponent(s)}`);
};
if(b7)b7.onclick=logBehaviorJournal;
if(b8)b8.onclick=()=>loadAnalyticsPanel('/trading/analytics/behavior/report?days=7');
if(b9)b9.onclick=()=>loadAnalyticsPanel('/trading/analytics/stoploss/policy');

if(fxj)fxj.onclick=()=>exportFactorLibrary('json');
if(fxc)fxc.onclick=()=>exportFactorLibrary('csv');
if(fxr)fxr.onclick=()=>exportFactorLibrary('report');
[fs,fo,fn].forEach(el=>{if(!el)return;el.addEventListener(el.tagName==='INPUT'?'input':'change',()=>rerenderFactorLibraryFromCache());});

if(m1)m1.onclick=async()=>{
const out=getResearchOutputEl();
try{
const d=await api(`/data/multi-assets/overview?exchange=${encodeURIComponent(getResearchExchange())}&symbols=${encodeURIComponent(getResearchSymbols().join(','))}&timeframe=${encodeURIComponent(getResearchTimeframe())}&lookback=${Math.min(2000,getResearchLookback())}&exclude_retired=${getResearchExcludeRetired()?'true':'false'}`);
renderMultiAssetPanel(d);
renderResearchQuickSummary([{label:'币种数量',value:Number(d?.count||0)},{label:'排除停更',value:getResearchExcludeRetired()?'开启':'关闭'},{label:'已排除',value:Number(d?.retired_filter?.excluded_symbols?.length||0)},{label:'周期',value:getResearchTimeframe()},{label:'最佳收益',value:`${Number((d?.assets||[])[0]?.return_pct||0).toFixed(2)}%`}]);
if(out)out.textContent=JSON.stringify(d,null,2);
}catch(e){
renderMultiAssetPanel({error:e.message});
if(out)out.textContent=`多币种概览失败: ${e.message}`;
}
};

if(m2)m2.onclick=async()=>{
const out=getResearchOutputEl();
try{
const syms=getResearchSymbols();
if(!syms.includes('ADA/USDT'))syms.push('ADA/USDT');
const d=await api(`/data/factors/fama?exchange=${encodeURIComponent(getResearchExchange())}&symbols=${encodeURIComponent(syms.join(','))}&timeframe=${encodeURIComponent(getResearchTimeframe())}&lookback=${Math.min(2400,getResearchLookback())}&exclude_retired=${getResearchExcludeRetired()?'true':'false'}`);
renderResearchQuickSummary([{label:'Fama样本点',value:Number(d?.points||0)},{label:'MKT',value:Number(d?.latest?.MKT||0).toFixed(6)},{label:'MOM',value:Number(d?.latest?.MOM||0).toFixed(6)}]);
if(out)out.textContent=JSON.stringify(d,null,2);
}catch(e){
if(out)out.textContent=`因子加载失败: ${e.message}`;
}
};

if(m4)m4.onclick=async()=>{
const out=getResearchOutputEl();
try{
const syms=getResearchSymbols();
['ADA/USDT','TRX/USDT','LINK/USDT'].forEach(x=>{if(!syms.includes(x))syms.push(x);});
const tf=getResearchTimeframe();
const factorLookback=getFactorLookbackForTimeframe(tf,getResearchLookback());
const factorTimeoutMs=getFactorApiTimeoutMs(tf,syms.length);
const d=await api(`/data/factors/library?exchange=${encodeURIComponent(getResearchExchange())}&symbols=${encodeURIComponent(syms.join(','))}&timeframe=${encodeURIComponent(tf)}&lookback=${factorLookback}&quantile=0.3&series_limit=500&exclude_retired=${getResearchExcludeRetired()?'true':'false'}`,{timeoutMs:factorTimeoutMs});
renderFactorLibraryPanel(d);
renderResearchQuickSummary([{label:'因子数量',value:(d?.factors||[]).length},{label:'覆盖币种',value:(d?.symbols_used||[]).length},{label:'已排除',value:Number(d?.retired_filter?.excluded_symbols?.length||0)},{label:'有效时间点',value:Number(d?.points||0)},{label:'质量',value:d?.universe_quality||'-'}]);
if(out)out.textContent=JSON.stringify(d,null,2);
}catch(e){
if(out)out.textContent=`多因子加载失败: ${e.message}`;
}
};

if(m3)m3.onclick=async()=>{
const out=getResearchOutputEl();
try{
const d=await api(`/data/onchain/overview?exchange=${encodeURIComponent(getResearchExchange())}&symbol=${encodeURIComponent(getResearchSymbol())}&whale_threshold_btc=100&chain=Ethereum`);
researchState.lastOnchain=d&&typeof d==='object'?d:null;
renderResearchConclusionCard();
renderResearchQuickSummary([{label:'链上窗口',value:`${Number(d?.window_hours||0)}h`},{label:'巨鲸笔数',value:Number(d?.whale_activity?.count||0)},{label:'TVL链',value:d?.defi_tvl?.chain||'-'}]);
if(out)out.textContent=JSON.stringify(d,null,2);
}catch(e){
researchState.lastOnchain={error:e.message};
renderResearchConclusionCard();
if(out)out.textContent=`链上概览失败: ${e.message}`;
}
};

renderResearchConclusionCard();
}

function formatReplayText(d){if(!d)return'无回放数据';const now=new Date().toLocaleString('zh-CN');const first=(d.data&&d.data.length)?new Date(d.data[0].timestamp).toLocaleString('zh-CN'):'-';const last=(d.data&&d.data.length)?new Date(d.data[d.data.length-1].timestamp).toLocaleString('zh-CN'):'-';return[`更新时间: ${now}`,`回放ID: ${d.replay_id||replaySessionId||'-'}`,`进度: ${Number(d.cursor||0)} / ${Number(d.total||0)} ${d.done?'(已结束)':'(进行中)'}`,`本次推进K线: ${(d.data||[]).length}`,`窗口范围: ${first} -> ${last}`].join('\n');}
function bindDataAdvanced(){const rout=document.getElementById('replay-output');const rs=document.getElementById('btn-replay-start'),rn=document.getElementById('btn-replay-next'),rp=document.getElementById('btn-replay-stop');if(rs)rs.onclick=async()=>{try{const ex=document.getElementById('data-exchange').value,s=document.getElementById('data-symbol').value,tf=document.getElementById('data-timeframe').value,st=document.getElementById('replay-start-time').value,et=document.getElementById('replay-end-time').value,w=Number(document.getElementById('replay-window').value||300);const payload={exchange:ex,symbol:s,timeframe:tf,start_time:st?new Date(st).toISOString():null,end_time:et?new Date(et).toISOString():null,window:w,speed:1};const d=await api('/data/replay/start',{method:'POST',body:JSON.stringify(payload)});replaySessionId=d.replay_id||'';if(rout)rout.textContent=formatReplayText({...d,data:d.data||[]});notify('回放会话已启动');}catch(e){if(rout)rout.textContent=`回放启动失败: ${e.message}`;}};if(rn)rn.onclick=async()=>{try{if(!replaySessionId){notify('请先启动回放',true);return;}const steps=Number(document.getElementById('replay-steps').value||60);const d=await api(`/data/replay/${encodeURIComponent(replaySessionId)}/next?steps=${steps}`);if(rout)rout.textContent=formatReplayText({...d,replay_id:replaySessionId});if(d.data?.length){marketDataState.bars=cropBars(mergeBars([],d.data));renderKlineChart(false);} }catch(e){if(rout)rout.textContent=`回放推进失败: ${e.message}`;}};if(rp)rp.onclick=async()=>{try{if(!replaySessionId)return;const d=await api(`/data/replay/${encodeURIComponent(replaySessionId)}`,{method:'DELETE'});if(rout)rout.textContent=`回放已停止\n会话: ${d.replay_id||replaySessionId}\n时间: ${new Date().toLocaleString('zh-CN')}`;replaySessionId='';notify('回放已停止');}catch(e){if(rout)rout.textContent=`回放停止失败: ${e.message}`;}};}

function bindBacktest(){
initBacktestComparePicker();
const f=document.getElementById('backtest-form');
if(f)f.onsubmit=async e=>{
e.preventDefault();
try{
notify('回测运行中...');
const st=document.getElementById('backtest-strategy').value,s=document.getElementById('backtest-symbol').value,tf=document.getElementById('backtest-timeframe').value,c=document.getElementById('backtest-capital').value,sd=document.getElementById('backtest-start-date').value,ed=document.getElementById('backtest-end-date').value,cr=0.0004,sb=2;
let u=`/backtest/run?strategy=${st}&symbol=${encodeURIComponent(s)}&timeframe=${tf}&initial_capital=${c}&commission_rate=${cr}&slippage_bps=${sb}&include_series=true`;
if(sd)u+=`&start_date=${encodeURIComponent(sd)}`;
if(ed)u+=`&end_date=${encodeURIComponent(ed)}`;
renderBacktest(await api(u,{method:'POST'}));
notify('回测完成');
}catch(err){notify(`回测失败: ${err.message}`,true);}
};
const b1=document.getElementById('btn-backtest-compare');
if(b1)b1.onclick=async()=>{
try{
renderBacktestExtraLoading('多策略对比运行中');
const s=document.getElementById('backtest-symbol').value,tf=document.getElementById('backtest-timeframe').value,c=document.getElementById('backtest-capital').value,sd=document.getElementById('backtest-start-date')?.value||'',ed=document.getElementById('backtest-end-date')?.value||'',cr=0.0004,sb=2;
const chosenStrategies=getSelectedBacktestCompareStrategies();
if(!chosenStrategies.length){notify('请至少勾选一个策略',true);return;}
const objective=String(document.getElementById('backtest-opt-objective')?.value||'total_return');
const maxTrials=Math.max(8,Math.min(128,parseInt(document.getElementById('backtest-opt-trials')?.value||'64',10)||64));
const compareTimeoutMs=Math.max(30000,Math.min(8*60*1000, chosenStrategies.length*maxTrials*220 + 25000));
let cu=`/backtest/compare?strategies=${encodeURIComponent(chosenStrategies.join(','))}&symbol=${encodeURIComponent(s)}&timeframe=${tf}&initial_capital=${c}&commission_rate=${cr}&slippage_bps=${sb}&pre_optimize=true&optimize_objective=${encodeURIComponent(objective)}&optimize_max_trials=${maxTrials}`;
if(sd)cu+=`&start_date=${encodeURIComponent(sd)}`;
if(ed)cu+=`&end_date=${encodeURIComponent(ed)}`;
const d=await api(cu,{method:'POST',timeoutMs:compareTimeoutMs});
backtestUIState.lastCompare=d||null;
renderBacktestCompareOutput(d);
notify('多策略对比完成');
}catch(err){renderBacktestExtraError(err);notify(`多策略对比失败: ${err.message}`,true);}
};
const b2=document.getElementById('btn-backtest-optimize');
if(b2)b2.onclick=async()=>{
try{
renderBacktestExtraLoading('参数优化运行中');
const st=document.getElementById('backtest-strategy').value,s=document.getElementById('backtest-symbol').value,tf=document.getElementById('backtest-timeframe').value,c=document.getElementById('backtest-capital').value,sd=document.getElementById('backtest-start-date')?.value||'',ed=document.getElementById('backtest-end-date')?.value||'',cr=0.0004,sb=2;
const objective=String(document.getElementById('backtest-opt-objective')?.value||'total_return');
const maxTrials=Math.max(8,Math.min(512,parseInt(document.getElementById('backtest-opt-trials')?.value||'64',10)||64));
let ou=`/backtest/optimize?strategy=${st}&symbol=${encodeURIComponent(s)}&timeframe=${tf}&initial_capital=${c}&commission_rate=${cr}&slippage_bps=${sb}&objective=${encodeURIComponent(objective)}&max_trials=${maxTrials}&include_all_trials=true`;
if(sd)ou+=`&start_date=${encodeURIComponent(sd)}`;
if(ed)ou+=`&end_date=${encodeURIComponent(ed)}`;
const d=await api(ou,{method:'POST',timeoutMs:90000});
renderBacktestOptimizeOutput(d);
notify('参数优化完成');
}catch(err){renderBacktestExtraError(err);notify(`参数优化失败: ${err.message}`,true);}
};
const b3=document.getElementById('btn-backtest-export');
if(b3)b3.onclick=()=>{
const st=document.getElementById('backtest-strategy').value,s=document.getElementById('backtest-symbol').value,tf=document.getElementById('backtest-timeframe').value,c=document.getElementById('backtest-capital').value,sd=document.getElementById('backtest-start-date')?.value||'',ed=document.getElementById('backtest-end-date')?.value||'',cr=0.0004,sb=2,fmt=document.getElementById('backtest-export-format')?.value||'xlsx';
let eu=`${API_BASE}/backtest/export?strategy=${st}&symbol=${encodeURIComponent(s)}&timeframe=${tf}&initial_capital=${c}&commission_rate=${cr}&slippage_bps=${sb}&format=${fmt}`;
if(sd)eu+=`&start_date=${encodeURIComponent(sd)}`;
if(ed)eu+=`&end_date=${encodeURIComponent(ed)}`;
window.open(eu,'_blank');
};
}

function bindTrade(){
const b=document.getElementById('order-form');
const ot=document.getElementById('order-type');
const om=document.getElementById('order-mode');
const updateVisibility=()=>{
const isLimit=(ot?.value==='limit');
const isConditional=(om?.value==='conditional');
document.getElementById('price-group').style.display=isLimit?'block':'none';
document.getElementById('trigger-price-group').style.display=isConditional?'block':'none';
};
if(ot)ot.onchange=updateVisibility;
if(om)om.onchange=updateVisibility;
updateVisibility();
if(b)b.onsubmit=async e=>{
e.preventDefault();
const submitBtn=b.querySelector('button[type="submit"]');
try{
if(submitBtn){submitBtn.disabled=true;submitBtn.textContent='下单中...';}
const payload={exchange:document.getElementById('order-exchange').value,symbol:document.getElementById('order-symbol').value,side:document.getElementById('order-side').value,order_type:document.getElementById('order-type').value,amount:parseFloat(document.getElementById('order-amount').value),leverage:parseFloat(document.getElementById('order-leverage').value||'1'),price:document.getElementById('order-price').value?parseFloat(document.getElementById('order-price').value):null,stop_loss:document.getElementById('order-stop-loss').value?parseFloat(document.getElementById('order-stop-loss').value):null,take_profit:document.getElementById('order-take-profit').value?parseFloat(document.getElementById('order-take-profit').value):null,trailing_stop_pct:document.getElementById('order-trailing-pct').value?parseFloat(document.getElementById('order-trailing-pct').value):null,trailing_stop_distance:document.getElementById('order-trailing-dist').value?parseFloat(document.getElementById('order-trailing-dist').value):null,trigger_price:document.getElementById('order-trigger-price').value?parseFloat(document.getElementById('order-trigger-price').value):null,order_mode:document.getElementById('order-mode').value,iceberg_parts:parseInt(document.getElementById('order-iceberg-parts').value||'1',10),algo_slices:parseInt(document.getElementById('order-algo-slices').value||'1',10),algo_interval_sec:parseInt(document.getElementById('order-algo-interval').value||'0',10),account_id:document.getElementById('order-account').value||'main',reduce_only:!!document.getElementById('order-reduce-only').checked};
const r=await api('/trading/order',{method:'POST',timeoutMs:60000,body:JSON.stringify(payload)});
notify(r.status==='queued'?'条件单已创建':'下单成功');
await Promise.all([loadOrders(),loadOpenOrders(),loadPositions(),loadSummary(),loadRisk(),loadConditionalOrders(),loadAccounts()]);
}catch(err){notify(`下单失败: ${err.message}`,true);}
finally{if(submitBtn){submitBtn.disabled=false;submitBtn.textContent='提交订单';}}
};
document.querySelectorAll('[data-quick]').forEach(btn=>btn.onclick=async()=>{
try{
await api('/trading/order',{method:'POST',timeoutMs:30000,body:JSON.stringify({exchange:'gate',symbol:document.getElementById('quick-symbol').value||'BTC/USDT',side:btn.dataset.quick,order_type:'market',amount:.01,leverage:1,price:null,account_id:'main',order_mode:'normal'})});
notify(`快捷${btn.dataset.quick==='buy'?'买入':'卖出'}成功`);
await Promise.all([loadOrders(),loadOpenOrders(),loadPositions(),loadSummary(),loadRisk(),loadAccounts()]);
}catch(err){notify(`快捷交易失败: ${err.message}`,true);}
});
const rb=document.getElementById('risk-reset-btn');
if(rb)rb.onclick=async()=>{
try{
await api('/trading/risk/reset',{method:'POST'});
notify('风控熔断已解除');
await loadRisk();
}catch(err){notify(`解除熔断失败: ${err.message}`,true);}
};
const pb=document.getElementById('btn-paper-reset');
if(pb)pb.onclick=async()=>{
if(!confirm('确认清空模拟盘历史订单、持仓、信号与统计吗？'))return;
try{
const r=await api('/trading/paper/reset?clear_snapshots=true',{method:'POST',timeoutMs:30000});
notify('模拟盘历史已清零');
const out=document.getElementById('accounts-output');
if(out)out.textContent=JSON.stringify(r,null,2);
await Promise.all([loadOrders(),loadPositions(),loadSummary(),loadRisk(),loadConditionalOrders(),loadAccounts(),loadStrategySummary(),loadPnlHeatmap()]);
}catch(err){notify(`模拟盘清零失败: ${err.message}`,true);}
};
}

async function init(){initTabs();initClock();initEquity();bindTrade();bindOrderView();bindData();bindDataAdvanced();bindBacktest();bindNotificationCenter();bindAudit();bindStrategyOps();bindStrategyAdvanced();bindResearchPanel();bindResearchPresets();bindResearchSentiment();bindModeControls();bindAccountControls();initWebSocket();await loadSystemStatus();await Promise.all([loadSummary(),loadPositions(),loadOrders(),loadOpenOrders(),loadConditionalOrders(),loadAccounts(),loadModeInfo(),loadStrategies(),loadStrategySummary(),loadStrategyHealth(),loadRisk(),loadPnlHeatmap(),loadNotificationCenter(),loadAuditLogs(),loadStrategyLibrary()]);state.bootCompleted=true;state.bootFailed=false;
// Status polling is lightweight but user-visible; keep it independent from heavier dashboard batches
setInterval(()=>{loadSystemStatus();},8000);
setInterval(()=>{if(!state.wsConnected){loadSummary();loadPositions();loadOrders();loadOpenOrders();loadStrategies();loadStrategySummary();loadRisk();loadConditionalOrders();loadAccounts();loadModeInfo();}},8000);
setInterval(()=>{loadPositions().catch(()=>{});loadBalances().catch(()=>{});loadOrders().catch(()=>{});loadOpenOrders().catch(()=>{});loadRisk().catch(()=>{});},10000);
setInterval(()=>{loadOrders();loadOpenOrders();loadStrategySummary();},10000);
setInterval(()=>{loadPnlHeatmap();loadNotificationCenter();loadAuditLogs();loadStrategyHealth();},20000);}

window.cancelOrder=cancelOrder;window.cancelConditional=cancelConditional;window.registerStrategy=registerStrategy;window.toggleStrategy=toggleStrategy;window.saveAllocation=saveAllocation;window.openEditor=openEditor;window.compareLive=compareLive;window.openStrategyEditor=openEditor;window.compareStrategyLive=compareLive;window.previewCompareStrategyByRank=previewCompareStrategyByRank;window.registerCompareStrategyByRank=registerCompareStrategyByRank;window.registerOptimizeBestAsNewStrategyInstance=registerOptimizeBestAsNewStrategyInstance;window.editNotifyRule=editNotifyRule;window.toggleNotifyRule=toggleNotifyRule;window.deleteNotifyRule=deleteNotifyRule;
window.addEventListener('error',e=>{markBootFailure(e?.error||new Error(e?.message||'前端错误'));});
window.addEventListener('unhandledrejection',e=>{markBootFailure(e?.reason||new Error('未处理的Promise异常'));});
init().catch(markBootFailure);
