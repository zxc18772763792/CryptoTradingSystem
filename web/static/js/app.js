const API_BASE='/api';
const state={positions:[],orders:[],strategies:[],availableStrategyTypes:[],strategyLibraryRows:[],strategyCatalogRows:[],summary:{running:[],recent_signals:[],runtime:{}},notifyRules:{},wsConnected:false,modeToken:'',bootCompleted:false,bootFailed:false,strategyHealth:null,lastHealthAlertKey:'',selectedStrategyName:'',closingPositions:{},lastSummarySnapshot:null,lastWsBackfillAtByTab:{}};
const researchState={lastFactorLibrary:null,lastMultiAsset:null,lastSentiment:null,lastAnalytics:null,lastOnchain:null,lastFama:null,lastOverview:null,pendingTimers:{},lastSentimentReqId:0};
const arbitrageState={catalog:[],selectedStrategy:'PairsTradingStrategy',initialized:false,lastSpec:null,pairRanking:null,pairRankingKey:'',pairRankingNote:'等待筛选：确认周期后点击“一键筛选前十”'};
const backtestUIState={lastOptimize:null,lastCompare:null,lastRenderedBacktest:null,defaultCompareStrategies:[]};
const dataHealthState={last:null};
const dataAnalyticsHealthState={last:null};
const uiLoadState={tabs:{},inFlight:{},requests:{},dataInitialized:false};
const summaryFetchState={statsTask:null,balancesTask:null};
const RESEARCH_DEFAULT_SYMBOLS=['BTC/USDT','ETH/USDT','BNB/USDT','SOL/USDT','XRP/USDT','ADA/USDT','DOGE/USDT','TRX/USDT','LINK/USDT','AVAX/USDT','DOT/USDT','POL/USDT','LTC/USDT','BCH/USDT','ETC/USDT','ATOM/USDT','NEAR/USDT','APT/USDT','ARB/USDT','OP/USDT','SUI/USDT','INJ/USDT','RUNE/USDT','AAVE/USDT','MKR/USDT','UNI/USDT','FIL/USDT','HBAR/USDT','ICP/USDT','TON/USDT'];
const DEFAULT_STRATEGY_ALLOCATION=0.15;
let equityChart=null;
let plotlyResizeSeq=0;
let dataReloadTimer=null;
let summaryLoadPromise=null;
let dashboardSecondaryTimer=null;
let dashboardSlowHintTimer=null;
let tradingSecondaryTimer=null;
const TRADING_STATS_TIMEOUT_MS=25000;
const TRADING_ORDERS_TIMEOUT_MS=20000;
const TRADING_OPEN_ORDERS_TIMEOUT_MS=25000;
const TRADING_POSITIONS_TIMEOUT_MS=30000;
const TAB_BOOTSTRAP_GRACE_MS=12000;
const DASHBOARD_SECONDARY_LOAD_DELAY_MS=4500;
const TRADING_SECONDARY_LOAD_DELAY_MS=2500;
const CORE_TAB_POLL_INTERVAL_MS=12000;
const WS_BACKFILL_INTERVAL_MS=10000;
const SECONDARY_TAB_POLL_INTERVAL_MS=60000;
const tabBootstrapState={startedAt:{}};
const SHARED_POLL_KEY_PREFIX='cts_shared_poll_v1:';
const SHARED_POLL_TTL_MS=15000;
const SHARED_POLL_HEARTBEAT_MS=5000;
const sharedPollState={
tabId:`${Date.now().toString(36)}_${Math.random().toString(36).slice(2,10)}`,
ownedGroups:new Set(),
heartbeatTimer:null,
};

function sharedPollStorageKey(group){return `${SHARED_POLL_KEY_PREFIX}${String(group||'default').trim()||'default'}`;}
function readSharedPollLeader(group){
try{
  if(typeof window==='undefined'||!window.localStorage)return null;
  const raw=window.localStorage.getItem(sharedPollStorageKey(group));
  if(!raw)return null;
  const parsed=JSON.parse(raw);
  return parsed&&typeof parsed==='object'?parsed:null;
}catch{return null;}
}
function writeSharedPollLeader(group,payload){
try{
  if(typeof window==='undefined'||!window.localStorage)return;
  window.localStorage.setItem(sharedPollStorageKey(group),JSON.stringify(payload));
}catch{}
}
function releaseSharedPollGroup(group){
const normalized=String(group||'').trim();
if(!normalized)return;
try{
  const current=readSharedPollLeader(normalized);
  if(current&&String(current.id||'')===sharedPollState.tabId&&typeof window!=='undefined'&&window.localStorage){
    window.localStorage.removeItem(sharedPollStorageKey(normalized));
  }
}catch{}
sharedPollState.ownedGroups.delete(normalized);
}
function releaseAllSharedPollGroups(){
Array.from(sharedPollState.ownedGroups).forEach(group=>releaseSharedPollGroup(group));
}
function sharedPollGroupForTab(tabName){
const tab=String(tabName||'').trim();
if(!tab)return'';
if(tab==='dashboard'||tab==='trading'||tab==='strategies')return tab;
if(tab==='ai-research'||tab==='ai-agent')return'ai';
return'';
}
function canRunSharedPolling(group='default'){
const normalized=String(group||'').trim();
if(!normalized)return true;
if(typeof window==='undefined'||!window.localStorage)return true;
if(document.hidden)return false;
const now=Date.now();
const leader=readSharedPollLeader(normalized);
const leaderId=String(leader?.id||'').trim();
const leaderUpdatedAt=Number(leader?.updatedAt||0);
if(!leaderId||now-leaderUpdatedAt>SHARED_POLL_TTL_MS||leaderId===sharedPollState.tabId){
  writeSharedPollLeader(normalized,{id:sharedPollState.tabId,updatedAt:now});
  sharedPollState.ownedGroups.add(normalized);
  return true;
}
sharedPollState.ownedGroups.delete(normalized);
return false;
}
function ensureSharedPollHeartbeat(){
if(typeof window==='undefined'||sharedPollState.heartbeatTimer)return;
sharedPollState.heartbeatTimer=window.setInterval(()=>{
  if(document.hidden){
    releaseAllSharedPollGroups();
    return;
  }
  canRunSharedPolling('status');
  const group=sharedPollGroupForTab(getActiveTabName());
  if(group)canRunSharedPolling(group);
},SHARED_POLL_HEARTBEAT_MS);
}
if(typeof window!=='undefined'){
window.__ctsSharedPolling={
  canRun:canRunSharedPolling,
  groupForTab:sharedPollGroupForTab,
  releaseAll:releaseAllSharedPollGroups,
};
window.addEventListener('beforeunload',releaseAllSharedPollGroups);
window.addEventListener('pagehide',releaseAllSharedPollGroups);
ensureSharedPollHeartbeat();
}
if(typeof globalThis!=='undefined'&&typeof globalThis.sseError!=='function'){
globalThis.sseError=function sseError(event){
try{
const message=event&&event.message?event.message:event;
if(typeof console!=='undefined'&&typeof console.warn==='function')console.warn('sseError fallback invoked',message);
}catch{}
return false;
};
}

const mapOrderStatus=s=>({open:'未成交',closed:'已成交',canceled:'已撤销',expired:'已过期',rejected:'已拒绝',queued:'待触发'}[s]||s);
const mapSide=s=>s==='buy'?'买':s==='sell'?'卖':s;
const mapState=s=>({running:'运行中',idle:'空闲',paused:'已暂停',stopped:'已停止'}[s]||s);
const fmt=v=>new Intl.NumberFormat('en-US',{style:'currency',currency:'USD'}).format(Number(v||0));
const fmtMaybe=v=>(v===null||v===undefined||Number.isNaN(Number(v)))?'--':fmt(v);
const fmtCompactUsd=v=>(v===null||v===undefined||Number.isNaN(Number(v)))?'--':new Intl.NumberFormat('en-US',{notation:'compact',maximumFractionDigits:2}).format(Number(v||0));
function fmtDurationSec(v){const sec=Math.max(0,Math.floor(Number(v||0)));const d=Math.floor(sec/86400),h=Math.floor((sec%86400)/3600),m=Math.floor((sec%3600)/60),s=sec%60;const out=[];if(d>0)out.push(`${d}d`);if(h>0||d>0)out.push(`${h}h`);if(m>0||h>0||d>0)out.push(`${m}m`);out.push(`${s}s`);return out.join(' ');}
const esc=v=>String(v??'').replace(/[&<>"']/g,m=>({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[m]));
const TIME_LOCALE='zh-CN';
const TIME_ZONE='Asia/Shanghai';
const TIME_ZONE_LABEL='上海时间 (UTC+8)';
if(typeof window!=='undefined'){
window.CTS_UI_LOCALE=TIME_LOCALE;
window.CTS_UI_TIMEZONE=TIME_ZONE;
window.CTS_UI_TIMEZONE_LABEL=TIME_ZONE_LABEL;
}
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
function fmtDateTimeOptions(d,options={}){return d.toLocaleString(TIME_LOCALE,{hour12:false,timeZone:TIME_ZONE,...options});}
function fmtDateTime(value){const d=toDate(value);return d?fmtDateTimeOptions(d):'--';}
function fmtTime(value){const d=toDate(value);return d?d.toLocaleTimeString(TIME_LOCALE,{hour12:false,timeZone:TIME_ZONE}):'--';}
function fmtAxisDateTime(value){const d=toDate(value);return d?fmtDateTimeOptions(d,{month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit'}):'';}
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
?fmtDateTimeOptions(d,{month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit'})
:d.toLocaleDateString(TIME_LOCALE,{year:'2-digit',month:'2-digit',day:'2-digit',timeZone:TIME_ZONE});
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
// ===== 趋势类 =====
MAStrategy:{cat:'趋势',desc:'双均线金叉死叉'},
EMAStrategy:{cat:'趋势',desc:'EMA快慢线交叉'},
MACDStrategy:{cat:'趋势',desc:'MACD趋势跟随'},
MACDHistogramStrategy:{cat:'趋势',desc:'MACD柱体动量'},
ADXTrendStrategy:{cat:'趋势',desc:'ADX趋势强度确认'},
TrendFollowingStrategy:{cat:'趋势',desc:'多均线趋势跟踪'},
AroonStrategy:{cat:'趋势',desc:'Aroon趋势识别'},
// ===== 震荡类 =====
RSIStrategy:{cat:'震荡',desc:'RSI超买超卖'},
RSIDivergenceStrategy:{cat:'震荡',desc:'RSI顶底背离'},
StochasticStrategy:{cat:'震荡',desc:'KDJ随机震荡'},
BollingerBandsStrategy:{cat:'震荡',desc:'布林带回归'},
WilliamsRStrategy:{cat:'震荡',desc:'威廉超买超卖'},
CCIStrategy:{cat:'震荡',desc:'CCI通道指数'},
StochRSIStrategy:{cat:'震荡',desc:'RSI随机震荡'},
// ===== 动量类 =====
MomentumStrategy:{cat:'动量',desc:'价格动量突破'},
ROCStrategy:{cat:'动量',desc:'变化率动量'},
PriceAccelerationStrategy:{cat:'动量',desc:'价格加速度'},
// ===== 均值回归类 =====
MeanReversionStrategy:{cat:'均值回归',desc:'Z-Score回归'},
BollingerMeanReversionStrategy:{cat:'均值回归',desc:'布林带均值回归'},
VWAPReversionStrategy:{cat:'均值回归',desc:'VWAP价格回归'},
VWAPStrategy:{cat:'均值回归',desc:'成交量加权回归'},
MeanReversionHalfLifeStrategy:{cat:'均值回归',desc:'半衰期回归'},
// ===== 突破类 =====
BollingerSqueezeStrategy:{cat:'突破',desc:'布林带收窄突破'},
DonchianBreakoutStrategy:{cat:'突破',desc:'唐奇安通道突破'},
// ===== 成交量类 =====
MFIStrategy:{cat:'成交量',desc:'资金流量指标'},
OBVStrategy:{cat:'成交量',desc:'能量潮背离'},
TradeIntensityStrategy:{cat:'成交量',desc:'成交量异动'},
// ===== 波动率类 =====
ParkinsonVolStrategy:{cat:'波动率',desc:'高低波动率回归'},
// ===== 风险类 =====
UlcerIndexStrategy:{cat:'风险',desc:'下行风险择时'},
VaRBreakoutStrategy:{cat:'风险',desc:'VaR异常突破'},
MaxDrawdownStrategy:{cat:'风险',desc:'回撤反弹买入'},
SortinoRatioStrategy:{cat:'风险',desc:'风险调整趋势'},
// ===== 统计套利类 =====
PairsTradingStrategy:{cat:'统计套利',desc:'配对价差回归'},
FamaFactorArbitrageStrategy:{cat:'统计套利',desc:'多因子横截面'},
HurstExponentStrategy:{cat:'统计套利',desc:'长记忆regime'},
// ===== 微观结构类 =====
OrderFlowImbalanceStrategy:{cat:'微观结构',desc:'订单流失衡'},
// ===== 套利类 =====
CEXArbitrageStrategy:{cat:'套利',desc:'跨所价差套利'},
TriangularArbitrageStrategy:{cat:'套利',desc:'三角路径套利'},
DEXArbitrageStrategy:{cat:'套利',desc:'链上DEX套利'},
FlashLoanArbitrageStrategy:{cat:'套利',desc:'闪电贷套利'},
// ===== 宏观类 =====
MarketSentimentStrategy:{cat:'宏观',desc:'恐慌贪婪指数',family:'ai_openai',decisionEngine:'openai',aiDriven:true},
SocialSentimentStrategy:{cat:'宏观',desc:'社媒情绪分析',family:'ai_openai',decisionEngine:'openai',aiDriven:true},
FundFlowStrategy:{cat:'宏观',desc:'交易所资金流',family:'ai_openai',decisionEngine:'openai',aiDriven:true},
WhaleActivityStrategy:{cat:'宏观',desc:'巨鲸地址追踪',family:'ai_openai',decisionEngine:'openai',aiDriven:true},
// ===== 量化多因子类 =====
MultiFactorHFStrategy:{cat:'量化',desc:'多因子高频组合(5m)'},
// ===== ML 类 =====
MLXGBoostStrategy:{cat:'机器学习',desc:'XGBoost 方向预测',family:'ml',decisionEngine:'ml',aiDriven:true}
};
const ARBITRAGE_STRATEGY_ORDER=['PairsTradingStrategy','FamaFactorArbitrageStrategy','CEXArbitrageStrategy','TriangularArbitrageStrategy','DEXArbitrageStrategy','FlashLoanArbitrageStrategy'];
function getStrategyMeta(name){
const meta=STRATEGY_META[String(name||'').trim()]||{};
return{
cat:String(meta.cat||'其他'),
desc:String(meta.desc||'可注册后在参数面板调整'),
risk:String(meta.risk||'medium'),
family:String(meta.family||'traditional'),
decisionEngine:String(meta.decisionEngine||'rule'),
aiDriven:!!meta.aiDriven,
};
}
function strategyCatalogMap(){
return Object.fromEntries((state.strategyCatalogRows||[]).map(row=>[String(row?.name||'').trim(),row]).filter(([name])=>Boolean(name)));
}
function mergeStrategyCatalogRows(rows){
const normalized=(Array.isArray(rows)?rows:[]).map(row=>{
  const name=String(row?.name||'').trim();
  if(!name)return null;
  const existing=STRATEGY_META[name]||{};
  STRATEGY_META[name]={
    ...existing,
    cat:String(row?.category||existing.cat||'其他'),
    desc:String(row?.usage||existing.desc||name),
    risk:String(row?.risk||existing.risk||'medium'),
    family:String(row?.family||existing.family||'traditional'),
    decisionEngine:String(row?.decision_engine||existing.decisionEngine||'rule'),
    aiDriven:!!(row?.ai_driven ?? existing.aiDriven),
    backtestSupported:!!row?.backtest_supported,
    backtestReason:String(row?.backtest_reason||existing.backtestReason||''),
    defaultStart:!!row?.default_start,
    recommendedTimeframe:String(row?.recommended_timeframe||existing.recommendedTimeframe||''),
    recommendedSymbols:Array.isArray(row?.recommended_symbols)?row.recommended_symbols:[...(existing.recommendedSymbols||[])],
  };
  return{
    ...row,
    name,
    category:String(row?.category||existing.cat||'其他'),
    usage:String(row?.usage||existing.desc||name),
    risk:String(row?.risk||existing.risk||'medium'),
    family:String(row?.family||existing.family||'traditional'),
    decision_engine:String(row?.decision_engine||existing.decisionEngine||'rule'),
    ai_driven:!!(row?.ai_driven ?? existing.aiDriven),
    backtest_supported:!!row?.backtest_supported,
    backtest_reason:String(row?.backtest_reason||existing.backtestReason||''),
    default_start:!!row?.default_start,
  };
}).filter(Boolean);
state.strategyCatalogRows=normalized;
if(normalized.length){
  state.availableStrategyTypes=normalized.map(row=>row.name);
  backtestUIState.defaultCompareStrategies=normalized.filter(row=>row.backtest_supported&&row.default_start).map(row=>row.name);
}
return normalized;
}
async function ensureStrategyCatalog(force=false){
if(!force&&Array.isArray(state.strategyCatalogRows)&&state.strategyCatalogRows.length)return state.strategyCatalogRows;
try{
  const d=await api('/strategies/catalog',{timeoutMs:18000});
  return mergeStrategyCatalogRows(Array.isArray(d?.strategies)?d.strategies:[]);
}catch(e){
  console.error(e);
  return Array.isArray(state.strategyCatalogRows)?state.strategyCatalogRows:[];
}
}
function syncBacktestStrategyMeta(strategyName){
const catalogMap=strategyCatalogMap();
const row=catalogMap[String(strategyName||'').trim()];
if(!row)return;
// Sync recommended timeframe
const tf=String(row.recommended_timeframe||'').trim();
const tfSel=document.getElementById('backtest-timeframe');
if(tf&&tfSel){const opts=[...tfSel.options].map(o=>o.value);if(opts.includes(tf))tfSel.value=tf;}
// Sync recommended symbols into #backtest-symbol
const syms=Array.isArray(row.recommended_symbols)&&row.recommended_symbols.length?row.recommended_symbols:null;
if(!syms)return;
const symSel=document.getElementById('backtest-symbol');
if(!symSel)return;
const currentVal=String(symSel.value||'').trim();
const existingVals=new Set([...symSel.options].map(o=>String(o.value||'').trim()));
// Insert recommended symbols at the top (before existing options) if not already present
const toAdd=syms.filter(s=>String(s||'').trim()&&!existingVals.has(String(s).trim()));
if(toAdd.length){
  const frag=document.createDocumentFragment();
  toAdd.forEach(s=>{const opt=document.createElement('option');opt.value=s;opt.textContent=s;frag.appendChild(opt);});
  symSel.insertBefore(frag,symSel.firstChild);
}
// Select the first recommended symbol if the current selection is not in the recommended list
const recSet=new Set(syms.map(s=>String(s||'').trim()));
if(!recSet.has(currentVal)&&syms.length){symSel.value=String(syms[0]).trim();}
}
function renderBacktestStrategySelect(rows){
const sel=document.getElementById('backtest-strategy');
if(!sel)return;
const supported=(Array.isArray(rows)?rows:[]).filter(row=>row&&row.name&&row.backtest_supported);
if(!supported.length)return;
const prev=String(sel.value||'').trim();
const grouped={};
for(const row of supported){
  const groupLabel=mapStrategyCatToBacktestGroup(row.category||'其他');
  (grouped[groupLabel]||(grouped[groupLabel]=[])).push(row);
}
const groupOrder=['趋势类','震荡类','动量类','均值回归类','突破类','成交量类','波动率类','风险类','统计套利类','微观结构类','套利类','宏观类','其他'];
sel.innerHTML=groupOrder.filter(group=>Array.isArray(grouped[group])&&grouped[group].length).map(group=>{
  const options=grouped[group].sort((a,b)=>String(a.name).localeCompare(String(b.name),'zh-CN')).map(row=>{
    const usage=String(row.usage||'').trim();
    const label=usage?`${strategyTypeShortName(row.name)} - ${usage}`:String(row.name);
    return `<option value="${esc(row.name)}">${esc(label)}</option>`;
  }).join('');
  return `<optgroup label="${esc(group)} (${grouped[group].length})">${options}</optgroup>`;
}).join('');
const fallback=supported.some(row=>row.name===prev)?prev:(supported.find(row=>row.default_start)?.name||supported[0]?.name||'');
if(fallback)sel.value=fallback;
backtestUIState.compareCatalog=null;
// Bind strategy change → auto-sync timeframe & symbols
if(!sel._btMetaBound){
  sel._btMetaBound=true;
  sel.addEventListener('change',()=>syncBacktestStrategyMeta(sel.value));
}
// Only sync recommended meta on first load or when the selected strategy actually changes.
if(!prev || prev!==sel.value)syncBacktestStrategyMeta(sel.value);
}
async function ensureBacktestStrategySelect(force=false){
const rows=await ensureStrategyCatalog(force);
const sel=document.getElementById('backtest-strategy');
const hasLoadedOptions=!!sel&&[...sel.options].some(opt=>String(opt.value||'').trim());
if(!force&&hasLoadedOptions&&Array.isArray(rows)&&rows.length)return rows;
renderBacktestStrategySelect(rows);
return rows;
}
async function ensureSelectedBacktestStrategy(){
await ensureBacktestStrategySelect();
const value=String(document.getElementById('backtest-strategy')?.value||'').trim();
if(!value)throw new Error('回测策略目录尚未加载完成');
return value;
}

function ensureSelectOption(id, value){
const el=document.getElementById(id);
const text=String(value||'').trim();
if(!(el instanceof HTMLSelectElement)||!text)return;
const exists=[...el.options].some(opt=>String(opt.value||'').trim()===text);
if(!exists){
  const opt=document.createElement('option');
  opt.value=text;
  opt.textContent=text;
  el.insertBefore(opt, el.firstChild||null);
}
}

function getBacktestCustomParams(){
const raw=String(document.getElementById('backtest-custom-params')?.value||'').trim();
if(!raw)return null;
let parsed=null;
try{parsed=JSON.parse(raw);}catch(e){throw new Error(`自定义参数 JSON 无效: ${e.message}`);}
if(!parsed||typeof parsed!=='object'||Array.isArray(parsed))throw new Error('自定义参数 JSON 必须是对象');
return parsed;
}

function estimateBacktestWindowDays(){
const startEl=document.getElementById('backtest-start-date');
const endEl=document.getElementById('backtest-end-date');
const start=toDate(startEl?.value||'');
const end=toDate(endEl?.value||'');
if(!start||!end)return 0;
const spanMs=end.getTime()-start.getTime();
if(!Number.isFinite(spanMs)||spanMs<=0)return 0;
return Math.ceil(spanMs/86400000);
}

function estimateBacktestRunTimeoutMs(strategyName, customParams=null){
const strategy=String(strategyName||'').trim();
if(strategy==='FamaFactorArbitrageStrategy'){
  const params=(customParams&&typeof customParams==='object'&&!Array.isArray(customParams))?customParams:{};
  const universeCount=Math.max(
    8,
    Math.min(
      36,
      Array.isArray(params.universe_symbols)
        ? params.universe_symbols.filter(v=>String(v||'').trim()).length
        : (Number(params.max_symbols||0)>0?Number(params.max_symbols):12)
    )
  );
  const lookbackBars=Math.max(240,Math.min(6000,Number(params.lookback_bars||720)||720));
  const windowDays=Math.max(60,Math.min(3650,estimateBacktestWindowDays()||365));
  const timeoutMs=
    18000+
    universeCount*1800+
    Math.ceil(lookbackBars/240)*2200+
    Math.ceil(windowDays/90)*1800;
  return Math.max(40000,Math.min(120000,timeoutMs));
}
return 12000;
}

function setBacktestCustomParams(params=null, note=''){
const box=document.getElementById('backtest-custom-params');
const hint=document.getElementById('backtest-custom-params-hint');
const panel=document.getElementById('backtest-custom-params-panel');
const hasParams=!!(params&&typeof params==='object'&&!Array.isArray(params)&&Object.keys(params).length);
if(box)box.value=hasParams?JSON.stringify(params,null,2):'';
if(hint)hint.textContent=note||'留空时使用策略默认参数。多策略对比 / 参数优化暂不读取这里的 JSON。';
if(panel&&'open' in panel)panel.open=hasParams;
}

async function openBacktestWithSpec(spec={}){
const strategy=String(spec?.strategy_type||spec?.strategy||'').trim();
if(!strategy)throw new Error('缺少回测策略类型');
const row=(strategyCatalogMap()||{})[strategy]||null;
if(row&&!row.backtest_supported){
  throw new Error(String(row.backtest_reason||`${strategy} 当前不支持回测`));
}
activateTab('backtest');
await ensureTabLoaded('backtest',{force:true});
await loadDataSymbolOptions(String(spec?.exchange||'binance').trim().toLowerCase()||'binance',['backtest-symbol']);
await ensureBacktestStrategySelect();
const strategyEl=document.getElementById('backtest-strategy');
if(strategyEl){
  strategyEl.value=strategy;
  syncBacktestStrategyMeta(strategy);
}
const symbol=String(spec?.symbol||(Array.isArray(spec?.symbols)?spec.symbols[0]:'')||'').trim();
if(symbol){
  ensureSelectOption('backtest-symbol', symbol);
  setSelectValues('backtest-symbol',[symbol],symbol);
}
const tf=String(spec?.timeframe||'').trim();
if(tf){
  const tfEl=document.getElementById('backtest-timeframe');
  if(tfEl instanceof HTMLSelectElement&&[...tfEl.options].some(opt=>String(opt.value||'').trim()===tf))tfEl.value=tf;
}
const capital=Number(spec?.initial_capital);
if(Number.isFinite(capital)&&capital>0){
  const capitalEl=document.getElementById('backtest-capital');
  if(capitalEl)capitalEl.value=String(capital);
}
const sd=String(spec?.start_date||'').trim();
const ed=String(spec?.end_date||'').trim();
const sdEl=document.getElementById('backtest-start-date');
const edEl=document.getElementById('backtest-end-date');
if(sdEl&&sd)sdEl.value=sd;
if(edEl&&ed)edEl.value=ed;
const protectionEnabled=spec?.use_stop_take!==undefined?!!spec.use_stop_take:(spec?.stop_loss_pct!=null||spec?.take_profit_pct!=null);
const protectEl=document.getElementById('backtest-use-stop-take');
if(protectEl){
  protectEl.checked=protectionEnabled;
  protectEl.dispatchEvent(new Event('change'));
}
if(spec?.stop_loss_pct!=null){
  const stopEl=document.getElementById('backtest-stop-loss-pct');
  if(stopEl)stopEl.value=String(spec.stop_loss_pct);
}
if(spec?.take_profit_pct!=null){
  const takeEl=document.getElementById('backtest-take-profit-pct');
  if(takeEl)takeEl.value=String(spec.take_profit_pct);
}
const customParams=(spec?.params&&typeof spec.params==='object'&&!Array.isArray(spec.params))?spec.params:null;
setBacktestCustomParams(customParams, customParams&&Object.keys(customParams).length
  ? `已从套利页回填 ${strategyTypeShortName(strategy)} 的完整参数。点击“运行回测”会走自定义回测。多策略对比 / 参数优化暂不读取这里的 JSON。`
  : '');
notify(`已切换到回测：${strategyTypeShortName(strategy)}`);
return true;
}

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
const map={BollingerBands:'布林带',BollingerSqueeze:'布林挤压',BollingerMeanReversion:'布林回归',MeanReversion:'均值回归',TrendFollowing:'趋势跟随',DonchianBreakout:'唐奇安突破',WhaleActivity:'巨鲸',MarketSentiment:'市场情绪',SocialSentiment:'社媒情绪',FundFlow:'资金流',VWAPReversion:'VWAP回归',MACDHistogram:'MACD柱',MACD:'MACD',EMA:'EMA',MA:'MA',RSIDivergence:'RSI背离',RSI:'RSI',Stochastic:'随机指标',ADXTrend:'ADX趋势',Momentum:'动量',PairsTrading:'配对交易',CEXArbitrage:'CEX套利',TriangularArbitrage:'三角套利',DEXArbitrage:'DEX套利',FlashLoanArbitrage:'闪电贷套利',FamaFactorArbitrage:'Fama因子套利',MultiFactorHF:'多因子高频',MeanReversionHalfLife:'半衰期回归',HurstExponent:'Hurst指数',VaRBreakout:'VaR突破',MaxDrawdown:'最大回撤',SortinoRatio:'Sortino比率',OrderFlowImbalance:'订单流失衡',TradeIntensity:'成交强度',ParkinsonVol:'波动率回归',UlcerIndex:'风险指数'};
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
function normalizeStrategyOwnership(strategy){
const own=(strategy&&typeof strategy.ownership==='object'&&strategy.ownership)?strategy.ownership:{};
const source=String(own.source||'').trim().toLowerCase()||'manual';
const label=String(own.label||'').trim()||({ai_research:'AI研究',ai_autonomous_agent:'AI自治代理',backtest_import:'回测导入',manual:'手动注册'}[source]||'手动注册');
const tone=String(own.badge_tone||'').trim()||({ai_research:'ai-research',ai_autonomous_agent:'ai-agent',backtest_import:'backtest',manual:'manual'}[source]||'manual');
let detail=String(own.detail||'').trim();
if(!detail){
  const parts=[];
  if(own.runtime_mode)parts.push(`模式 ${own.runtime_mode}`);
  if(own.candidate_id)parts.push(`候选 ${own.candidate_id}`);
  if(own.proposal_id)parts.push(`提案 ${own.proposal_id}`);
  if(own.search_role)parts.push(`角色 ${own.search_role}`);
  if(own.promotion_target)parts.push(`目标 ${own.promotion_target}`);
  detail=parts.join(' · ');
}
if(!detail){
  detail=({
    ai_research: own.inferred?'AI研究运行实例（自动识别）':'来自 AI 研究候选运行链路',
    ai_autonomous_agent:'由 AI 自治代理运行与执行链路托管',
    backtest_import:'由回测/批量导入生成的策略实例',
    manual:'页面手动注册或编辑生成的策略实例',
  }[source]||'页面手动注册或编辑生成的策略实例');
}
return{source,label,tone,detail,inferred:!!own.inferred};
}
function summarizeStrategyOwnershipCounts(items){
const counts={};
const order=['AI研究','AI自治代理','回测导入','手动注册'];
(Array.isArray(items)?items:[]).forEach(item=>{
  const label=normalizeStrategyOwnership(item).label;
  counts[label]=(counts[label]||0)+1;
});
return order.filter(label=>counts[label]).map(label=>`${label} ${counts[label]}`).join(' | ');
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
const allocation=Math.max(0,Math.min(1,Number(document.getElementById('backtest-register-allocation')?.value||DEFAULT_STRATEGY_ALLOCATION)));
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
function clearPlotlyHost(el){
if(!el)return;
if(typeof Plotly!=='undefined'&&typeof Plotly.purge==='function'){
  try{Plotly.purge(el);}catch{}
}
el.replaceChildren();
}
async function api(ep,opt={}){const o=opt||{};const tmo=Math.max(1000,Number(o.timeoutMs||12000));const {timeoutMs,...rest}=o;const c=new AbortController();const timer=setTimeout(()=>c.abort(),tmo);try{const r=await fetch(`${API_BASE}${ep}`,{...rest,signal:c.signal,headers:{'Content-Type':'application/json',...(rest.headers||{})}});const ct=(r.headers.get('content-type')||'').toLowerCase();let d={};if(ct.includes('application/json')){d=await r.json();}else{const t=await r.text();d=t?{detail:t}:{};}if(!r.ok)throw new Error(d.detail||d.error||`接口请求失败(${r.status})`);return d;}catch(e){if(e?.name==='AbortError')throw new Error(`接口超时(${tmo}ms): ${ep}`);throw e;}finally{clearTimeout(timer);}}
function markBootFailure(err){const msg=err?.message||String(err||'未知错误');if(state.bootCompleted){console.error('runtime error:',err);notify(`运行期异常: ${msg}`,true);return;}if(state.bootFailed)return;state.bootFailed=true;console.error('bootstrap failed:',err);const st=document.getElementById('system-status'),m=document.getElementById('trading-mode'),ex=document.getElementById('exchanges-list');if(st)st.textContent='前端初始化失败';if(m)m.textContent='未知';if(ex)ex.innerHTML=`<div class=\"list-item\">页面初始化失败: ${esc(msg)}</div>`;notify(`前端初始化失败: ${msg}`,true);}
function getActiveTabName(){
return String(document.querySelector('.tab-btn.active')?.dataset?.tab||document.querySelector('.tab-content.active')?.id||'dashboard').trim()||'dashboard';
}
async function ensureDataTabInitialized(){
if(uiLoadState.dataInitialized)return;
uiLoadState.dataInitialized=true;
loadDataSymbolOptions(document.getElementById('data-exchange')?.value||'binance',['data-symbol']);
loadDataSymbolOptions(document.getElementById('download-exchange')?.value||'binance',['download-symbol']);
loadDataSymbolOptions('binance',['backtest-symbol']);
scheduleKlineRealtime();
setTimeout(()=>{loadDataStorageHealth(null,{skipStorage:true}).catch(err=>console.warn('loadDataStorageHealth failed',err?.message||err));},900);
setTimeout(()=>{if(document.getElementById('candlestick-chart')&&!marketDataState.bars.length){loadKlinesByForm().catch(()=>{});}},500);
}
function markTabBootstrap(tabName){
const tab=String(tabName||'').trim();
if(!tab)return;
tabBootstrapState.startedAt[tab]=Date.now();
}
function isTabBootstrapping(tabName,windowMs=TAB_BOOTSTRAP_GRACE_MS){
const tab=String(tabName||'').trim();
if(!tab)return false;
const startedAt=Number(tabBootstrapState.startedAt[tab]||0);
return startedAt>0&&(Date.now()-startedAt)<Math.max(0,Number(windowMs||0));
}
function refreshDashboardCore(){
return Promise.allSettled([loadSummary(),loadPositions(),loadOrders(),loadOpenOrders(),loadRisk(),loadStrategySummary()]);
}
function refreshTradingCore(){
return Promise.allSettled([loadSummary(),loadPositions(),loadOrders(),loadOpenOrders(),loadRisk()]);
}
function refreshTradingSecondary(){
return Promise.allSettled([loadConditionalOrders(),loadAccounts(),loadModeInfo(),loadLiveTradeReview({showLoading:false,minIntervalMs:15000})]);
}
function replaceStuckLoading(containerId,message){
const box=document.getElementById(containerId);
if(!box)return;
const text=String(box.textContent||'').trim();
if(!text||!/(加载中|获取中|显示在这里|请稍候)/.test(text))return;
box.innerHTML=`<div class="list-item">${esc(message)}</div>`;
}
function scheduleDashboardSecondaryLoads(delayMs=DASHBOARD_SECONDARY_LOAD_DELAY_MS){
if(dashboardSecondaryTimer)clearTimeout(dashboardSecondaryTimer);
dashboardSecondaryTimer=setTimeout(()=>{
  dashboardSecondaryTimer=null;
  if(document.hidden||getActiveTabName()!=='dashboard')return;
  const group=sharedPollGroupForTab('dashboard');
  if(group&&!canRunSharedPolling(group))return;
  Promise.allSettled([loadPnlHeatmap(),loadNotificationCenter(),loadAuditLogs()]).catch(()=>{});
},Math.max(0,Number(delayMs||0)));
}
function scheduleDashboardSlowHints(delayMs=18000){
if(dashboardSlowHintTimer)clearTimeout(dashboardSlowHintTimer);
dashboardSlowHintTimer=setTimeout(()=>{
  dashboardSlowHintTimer=null;
  if(document.hidden||getActiveTabName()!=='dashboard')return;
  replaceStuckLoading('risk-panel','风控快照响应较慢，后台稍后重试...');
  replaceStuckLoading('exchanges-list','资产快照响应较慢，后台稍后重试...');
  replaceStuckLoading('holdings-pie','资产分布暂未返回，后台稍后重试...');
  replaceStuckLoading('dashboard-unstructured-list','新闻整理较慢，后台稍后重试...');
  replaceStuckLoading('audit-log-list','审计日志响应较慢，后台稍后重试...');
},Math.max(0,Number(delayMs||0)));
}
function scheduleTradingSecondaryLoads(delayMs=TRADING_SECONDARY_LOAD_DELAY_MS){
if(tradingSecondaryTimer)clearTimeout(tradingSecondaryTimer);
tradingSecondaryTimer=setTimeout(()=>{
  tradingSecondaryTimer=null;
  if(document.hidden||getActiveTabName()!=='trading')return;
  const group=sharedPollGroupForTab('trading');
  if(group&&!canRunSharedPolling(group))return;
  refreshTradingSecondary().catch(()=>{});
},Math.max(0,Number(delayMs||0)));
}
async function loadDashboardTabData(){
await refreshDashboardCore();
scheduleDashboardSecondaryLoads();
scheduleDashboardSlowHints();
}
async function loadTradingTabData(){
await refreshTradingCore();
scheduleTradingSecondaryLoads();
}
async function loadStrategiesTabData(){
await Promise.allSettled([loadStrategies(),loadStrategySummary(),loadStrategyHealth()]);
}
async function loadDataTabData(){
await ensureDataTabInitialized();
}
async function loadResearchTabData(){
  await loadResearchSymbolOptions(getResearchExchange());
  renderResearchStatusCards();
}
async function loadAiResearchTabData(){
  setTimeout(()=>{refreshAiResearchModules().catch(err=>console.warn('loadAiResearchTabData failed:',err?.message||err));},0);
}
async function loadAiAgentTabData(){
  setTimeout(()=>{refreshAiResearchModules().catch(err=>console.warn('loadAiAgentTabData failed:',err?.message||err));},0);
}
async function loadArbitrageTabData(force=false){
  await ensureStrategyCatalog(force);
  await loadArbitrageSymbolOptions(getArbitrageExchange());
  if(arbitrageState.pairRankingKey&&arbitrageState.pairRankingKey!==getArbitragePairRankingKey()){
    resetArbitragePairRanking('周期或交易所已变化，请重新点击“一键筛选前十”');
  }else{
    renderArbitragePairRanking();
  }
  if(!arbitrageState.initialized){
    arbitrageState.initialized=true;
    await applyArbitrageTemplate(getArbitrageSelectedStrategy()||'PairsTradingStrategy');
    return;
  }
  renderArbitragePanel();
  renderArbitragePairRanking();
}
async function loadBacktestTabData(){
  loadDataSymbolOptions('binance',['backtest-symbol']);
  await ensureBacktestStrategySelect().catch(e=>console.error(e));
}
async function ensureTabLoaded(tabName,{force=false}={}){
const tab=String(tabName||'').trim();
if(!tab)return;
if(!force&&uiLoadState.tabs[tab])return;
if(uiLoadState.inFlight[tab])return uiLoadState.inFlight[tab];
const loaders={
  dashboard:loadDashboardTabData,
  trading:loadTradingTabData,
  strategies:loadStrategiesTabData,
  data:loadDataTabData,
  'ai-research':loadAiResearchTabData,
  'ai-agent':loadAiAgentTabData,
  research:loadResearchTabData,
  arbitrage:()=>loadArbitrageTabData(false),
  backtest:loadBacktestTabData,
};
const loader=loaders[tab];
if(!loader){uiLoadState.tabs[tab]=true;return;}
const task=(async()=>{
  try{
    markTabBootstrap(tab);
    await loader();
    uiLoadState.tabs[tab]=true;
  }finally{
    delete uiLoadState.inFlight[tab];
  }
})();
uiLoadState.inFlight[tab]=task;
return task;
}

function runRequestSingleFlight(key,taskFactory){
const requests=uiLoadState.requests||(uiLoadState.requests={});
if(requests[key])return requests[key];
const task=Promise.resolve().then(taskFactory).finally(()=>{if(requests[key]===task)delete requests[key];});
requests[key]=task;
return task;
}
function runSummaryTaskSingleFlight(slot,taskFactory){
if(summaryFetchState[slot])return summaryFetchState[slot];
const task=Promise.resolve().then(taskFactory).finally(()=>{if(summaryFetchState[slot]===task)summaryFetchState[slot]=null;});
summaryFetchState[slot]=task;
return task;
}
async function settleWithin(promise,timeoutMs){
let timer=null;
try{
  return await Promise.race([
    Promise.resolve(promise).then(value=>({status:'fulfilled',value}),reason=>({status:'rejected',reason})),
    new Promise(resolve=>{timer=setTimeout(()=>resolve({status:'pending'}),Math.max(0,Number(timeoutMs||0)));}),
  ]);
}finally{
  if(timer)clearTimeout(timer);
}
}

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
if(tabName==='ai-research'||tabName==='ai-agent'){
  setTimeout(()=>{refreshAiResearchModules();},0);
}
ensureTabLoaded(tabName).catch(err=>console.error(`tab load failed: ${tabName}`,err));
}
function initTabs(){
document.querySelectorAll('.tab-btn').forEach(b=>b.onclick=()=>activateTab(b.dataset.tab));
const qs=new URLSearchParams(window.location.search||'');
const byQuery=String(qs.get('tab')||'').trim();
const byHash=String((window.location.hash||'').replace(/^#/,'')).trim();
if(byQuery)activateTab(byQuery);else if(byHash)activateTab(byHash);
window.addEventListener('hashchange',()=>{const t=String((window.location.hash||'').replace(/^#/,'')).trim();if(t)activateTab(t);});
window.addEventListener('resize',()=>{schedulePlotlyResize(document.querySelector('.tab-content.active')||document);if(equityChart?.type==='fallback')renderEquityFallback(equityChart.rows||[]);});
}
function initClock(){const f=()=>{const t=document.getElementById('current-time');if(t)t.textContent=fmtDateTime(new Date());};f();setInterval(f,1000);}
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
if(typeof Chart==='undefined'){
  c.style.display='none';
  const p=c.parentElement;
  if(!p)return;
  let host=p.querySelector('.equity-chart-fallback');
  if(!host){
    host=document.createElement('div');
    host.className='equity-chart-fallback';
    host.style.height='260px';
    host.style.width='100%';
    p.appendChild(host);
  }
  equityChart={type:'fallback',host,rows:[]};
  renderEquityFallback([]);
  return;
}
c.style.display='';
try{c.parentElement?.querySelector('.equity-chart-fallback')?.remove();}catch{}
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
function buildEquityRows(hist){
if(!hist?.length)return [];
const max=220;
const sampled=hist.length>max?hist.filter((_,i)=>i%Math.ceil(hist.length/max)===0):hist;
return sampled.map(x=>({timestamp:x.timestamp,total:Number(x.total_usd||0)})).filter(x=>Number.isFinite(x.total)&&x.total>0&&toDate(x.timestamp));
}
function renderEquityFallback(rows){
const host=equityChart?.host||document.querySelector('.equity-chart-fallback');
if(!host)return;
if(!rows?.length){host.innerHTML='<div class="list-item">暂无净值数据</div>';return;}
const width=720,height=260,left=18,right=18,top=18,bottom=34;
const innerWidth=Math.max(1,width-left-right);
const innerHeight=Math.max(1,height-top-bottom);
const totals=rows.map(x=>x.total);
const minVal=Math.min(...totals);
const maxVal=Math.max(...totals);
const spanBase=maxVal-minVal;
const span=spanBase>0?spanBase:Math.max(Math.abs(maxVal)*0.08,1);
const yMin=minVal-(spanBase>0?0:span/2);
const yMax=maxVal+(spanBase>0?0:span/2);
const mapX=idx=>left+(rows.length===1?innerWidth/2:(idx/(rows.length-1))*innerWidth);
const mapY=value=>top+((yMax-value)/(yMax-yMin))*innerHeight;
const points=rows.map((row,idx)=>`${mapX(idx).toFixed(2)},${mapY(row.total).toFixed(2)}`).join(' ');
const areaPoints=`${left},${height-bottom} ${points} ${width-right},${height-bottom}`;
const first=rows[0];
const last=rows[rows.length-1];
const delta=last.total-first.total;
const deltaPct=first.total>0?(delta/first.total)*100:0;
const stroke=delta>=0?'#3fb950':'#f85149';
const fill=delta>=0?'rgba(63,185,80,.18)':'rgba(248,81,73,.18)';
const latestX=mapX(rows.length-1).toFixed(2);
const latestY=mapY(last.total).toFixed(2);
host.innerHTML=`<svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" style="display:block;width:100%;height:260px;border-radius:10px;background:linear-gradient(180deg, rgba(22,34,50,.96), rgba(17,25,37,.92));"><defs><linearGradient id="equity-area-gradient" x1="0" x2="0" y1="0" y2="1"><stop offset="0%" stop-color="${fill}" /><stop offset="100%" stop-color="rgba(99,110,123,0.02)" /></linearGradient></defs><line x1="${left}" y1="${top}" x2="${left}" y2="${height-bottom}" stroke="rgba(148,163,184,.18)" stroke-width="1" /><line x1="${left}" y1="${height-bottom}" x2="${width-right}" y2="${height-bottom}" stroke="rgba(148,163,184,.18)" stroke-width="1" /><line x1="${left}" y1="${top}" x2="${width-right}" y2="${top}" stroke="rgba(148,163,184,.08)" stroke-dasharray="4 4" stroke-width="1" /><line x1="${left}" y1="${height-bottom}" x2="${width-right}" y2="${height-bottom}" stroke="rgba(148,163,184,.08)" stroke-dasharray="4 4" stroke-width="1" /><polygon points="${areaPoints}" fill="url(#equity-area-gradient)" /><polyline points="${points}" fill="none" stroke="${stroke}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" /><circle cx="${latestX}" cy="${latestY}" r="4.5" fill="${stroke}" /><text x="${left}" y="14" fill="#8ea3ba" font-size="12">${esc(fmtAxisDateTime(first.timestamp))}</text><text x="${width-right}" y="14" text-anchor="end" fill="#8ea3ba" font-size="12">${esc(fmtAxisDateTime(last.timestamp))}</text><text x="${left}" y="${height-10}" fill="#e8eef9" font-size="14">最新净值 $${last.total.toFixed(2)}</text><text x="${width-right}" y="${height-10}" text-anchor="end" fill="${stroke}" font-size="13">${delta>=0?'+':''}${delta.toFixed(2)} (${deltaPct.toFixed(2)}%)</text><text x="${width-right}" y="${top+14}" text-anchor="end" fill="#8ea3ba" font-size="12">高点 $${maxVal.toFixed(2)}</text><text x="${width-right}" y="${height-bottom-6}" text-anchor="end" fill="#8ea3ba" font-size="12">低点 $${minVal.toFixed(2)}</text></svg>`;
}
function drawEquity(hist){
if(!equityChart)return;
const rows=buildEquityRows(hist);
if(equityChart.type==='fallback'){
equityChart.rows=rows;
renderEquityFallback(rows);
return;
}
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
function drawPie(dist,mode){const box=document.getElementById('holdings-pie');if(!box)return;if(!dist?.length){box.innerHTML='<div class="list-item">暂无可视化资产分布</div>';return;}if(typeof Plotly==='undefined'){box.innerHTML='<div class="list-item">图表库未加载，饼图暂不可用</div>';return;}box.innerHTML='';const top=dist.slice(0,10);Plotly.newPlot(box,[{type:'pie',labels:top.map(x=>x.currency),values:top.map(x=>Number(x.usd_value||0)),hole:.45,textinfo:'label+percent'}],{margin:{l:5,r:5,t:5,b:5},paper_bgcolor:'#162232',plot_bgcolor:'#162232',font:{color:'#e8eef9'},showlegend:false},{displaylogo:false,responsive:true});schedulePlotlyResize(document.getElementById('dashboard')||document);}

function renderRisk(r){const p=document.getElementById('risk-panel');if(!p)return;if(!r){p.innerHTML='<div class="list-item">风控快照暂未返回，稍后自动刷新...</div>';return;}const e=r.equity||{},a=r.alerts||[],c=`risk-${r.risk_level||'low'}`;const dailyTotalRatio=Number((e.daily_total_pnl_ratio??e.daily_pnl_ratio)??0);const stopBasis=Number((e.daily_stop_basis_usd??e.daily_total_pnl_usd??e.daily_pnl_usd)??0);const stopBasisRatio=Number((e.daily_stop_basis_ratio??e.daily_pnl_ratio)??0);p.innerHTML=`
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
if(summaryLoadPromise)return summaryLoadPromise;
summaryLoadPromise=(async()=>{
try{
const prevSnapshot=(state.lastSummarySnapshot&&typeof state.lastSummarySnapshot==='object')?state.lastSummarySnapshot:{};
const prevStats=(prevSnapshot.stats&&typeof prevSnapshot.stats==='object')?prevSnapshot.stats:{};
const prevBalances=(prevSnapshot.balances&&typeof prevSnapshot.balances==='object')?prevSnapshot.balances:{};
const prevHistoryByMode=(prevSnapshot.historyByMode&&typeof prevSnapshot.historyByMode==='object')?prevSnapshot.historyByMode:{};
const statsTask=runSummaryTaskSingleFlight('statsTask',()=>api('/trading/stats',{timeoutMs:TRADING_STATS_TIMEOUT_MS}));
const balancesTask=runSummaryTaskSingleFlight('balancesTask',()=>api('/trading/balances',{timeoutMs:65000}));
const [sr,br]=await Promise.all([
  settleWithin(statsTask,5000),
  settleWithin(balancesTask,5000),
]);
const statsFresh=(sr.status==='fulfilled'&&sr.value&&typeof sr.value==='object')?sr.value:null;
const balancesFresh=(br.status==='fulfilled'&&br.value&&typeof br.value==='object')?br.value:null;
const s=statsFresh||prevStats||{};
const b=balancesFresh||prevBalances||{};
const statusMode=String(state?._systemStatusLast?.trading_mode||'').toLowerCase();
const activeType=String(b?.active_account_type??b?.mode??statusMode??'paper').toLowerCase();
const historyMode=activeType==='live'?'live':'paper';
const historyFresh=(statsFresh||balancesFresh||Object.keys(prevHistoryByMode).length)
  ?await api(`/trading/balances/history?hours=72&exchange=all&limit=500&mode=${encodeURIComponent(historyMode)}`,{timeoutMs:5000}).catch(()=>null)
  :null;
const historyByMode={...prevHistoryByMode};
if(Array.isArray(historyFresh?.history))historyByMode[historyMode]=historyFresh.history;
const historyRows=Array.isArray(historyByMode?.[historyMode])?historyByMode[historyMode]:[];
if(statsFresh||balancesFresh||Array.isArray(historyFresh?.history)){
  state.lastSummarySnapshot={stats:s,balances:b,historyByMode};
}
const hasBalanceSnapshot=!!(b&&typeof b==='object'&&(Object.keys(b?.exchanges||{}).length||b?.paper_account||b?.active_account_type||b?.total_usd_estimate!==undefined));
const hasStatsSnapshot=!!(s&&typeof s==='object'&&Object.keys(s).length);
const activeUsd=Number(b?.active_account_usd_estimate??b?.total_usd_estimate??NaN);
const mergedRisk=(b?.risk_report||s?.risk||null);
const mergedEquity=(mergedRisk?.equity||{});
const livePosCount=Number(b?.live_position_count||0);
const statPosCount=Number(s?.positions?.position_count||0);
document.getElementById('open-positions').textContent=hasStatsSnapshot||hasBalanceSnapshot?((activeType==='live'?livePosCount:statPosCount)||0):'--';
document.getElementById('open-orders').textContent=hasStatsSnapshot?(s?.orders?.total_orders||0):'--';
const exObj=b?.exchanges||{},exKeys=Object.keys(exObj),exConnected=exKeys.filter(k=>Boolean(exObj[k]?.connected)).length;
const exCountEl=document.getElementById('exchange-status-count');
if(exCountEl)exCountEl.textContent=`${exConnected}/${exKeys.length||0}`;
const modeEl=document.getElementById('active-account-mode');
if(modeEl)modeEl.textContent=activeType==='paper'?'虚拟仓(PAPER)':'实仓(LIVE)';
const activeEl=document.getElementById('active-account-value');
if(activeEl)activeEl.textContent=Number.isFinite(activeUsd)?fmtMaybe(activeUsd):'--';
const pnlSource=(mergedEquity?.current_unrealized_pnl_usd ?? s?.positions?.total_unrealized_pnl);
const pnlKnown=hasStatsSnapshot||hasBalanceSnapshot;
const pnl=Number(pnlSource||0),p=document.getElementById('total-pnl');
if(p){p.textContent=pnlKnown?fmt(pnl):'--';p.className=`value ${pnlKnown&&pnl>=0?'positive':pnlKnown?'negative':''}`.trim();}
if(hasBalanceSnapshot){
  renderExchanges(b);
  drawPie(b?.distribution||[],activeType);
}else{
  const ex=document.getElementById('exchanges-list');
  if(ex)ex.innerHTML='<div class=\"list-item\">资产快照暂未返回，稍后自动刷新...</div>';
  const pie=document.getElementById('holdings-pie');
  if(pie)pie.innerHTML='<div class=\"list-item\">资产分布暂未返回，稍后自动刷新...</div>';
}
drawEquity(historyRows);
renderRisk(mergedRisk);
if(sr.status==='rejected'&&br.status==='rejected'){const ex=document.getElementById('exchanges-list');if(ex)ex.innerHTML='<div class=\"list-item\">资产接口暂时不可用，系统正在自动重试...</div>';}
}catch(e){console.error(e);const ex=document.getElementById('exchanges-list');if(ex)ex.innerHTML='<div class=\"list-item\">资产加载失败，正在重试...</div>';}
finally{summaryLoadPromise=null;}
})();
return summaryLoadPromise;
}
async function loadStats(){return loadSummary();}
async function loadBalances(){return loadSummary();}
async function loadBanlances(){return loadSummary();}
async function loadRisk(){try{return await loadSummary();}catch{}}
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
async function loadPnlHeatmap(){return runRequestSingleFlight('pnlHeatmap',async()=>{try{const d=Number(document.getElementById('pnl-heatmap-days')?.value||30),b=document.getElementById('pnl-heatmap-bucket')?.value||'day';const r=await api(`/trading/pnl/heatmap?days=${Math.max(1,d)}&bucket=${encodeURIComponent(b)}`,{timeoutMs:12000});renderPnlHeatmap(r);}catch(e){const box=document.getElementById('pnl-heatmap');if(box)box.innerHTML=`<div class="list-item">热力图加载失败: ${esc(e.message)}</div>`;}});}

function fmtNum(v,digits=4){
const n=Number(v);
if(!Number.isFinite(n))return'--';
return n.toFixed(digits);
}

function renderLiveTradeReview(payload){
const summaryEl=document.getElementById('live-review-summary');
const tbody=document.getElementById('live-review-tbody');
if(!summaryEl||!tbody)return;
const records=Array.isArray(payload?.items)?payload.items:[];
const counts=payload?.strategy_trade_counts&&typeof payload.strategy_trade_counts==='object'?payload.strategy_trade_counts:{};
const strategyPairs=Object.entries(counts).map(([k,v])=>[String(k||''),Number(v||0)]).filter(([k,v])=>k&&Number.isFinite(v));
strategyPairs.sort((a,b)=>b[1]-a[1]);
const topStrategy=strategyPairs.length?`${strategyPairs[0][0]} (${strategyPairs[0][1]})`:'--';
const summaryHtml=`
<div class="live-review-chip"><span class="k">记录数</span><span class="v">${records.length}</span></div>
<div class="live-review-chip"><span class="k">策略数</span><span class="v">${strategyPairs.length}</span></div>
<div class="live-review-chip"><span class="k">Top策略</span><span class="v" title="${esc(topStrategy)}">${esc(topStrategy)}</span></div>`;
if(summaryEl.innerHTML!==summaryHtml)summaryEl.innerHTML=summaryHtml;
if(!records.length){
const mode=String(state?._systemStatusLast?.trading_mode||'').toLowerCase();
const hint=mode==='live'?'暂无实盘成交复盘记录':'当前为模拟盘，暂无实盘复盘记录';
const hintHtml=`<tr><td colspan="9">${esc(hint)}</td></tr>`;
if(tbody.innerHTML!==hintHtml)tbody.innerHTML=hintHtml;
tbody.dataset.hasData='1';
return;
}
const sorted=records.slice().sort((a,b)=>toMs(b?.timestamp)-toMs(a?.timestamp));
const rowsHtml=sorted.map(row=>{
const signal=row?.signal||{};
const strategy=String(row?.strategy||signal?.strategy_name||'--');
const strategyCount=Number(row?.strategy_trade_count||0);
const signalType=String(row?.signal_type||signal?.signal_type||'').toLowerCase();
const side=String(row?.side||'').toLowerCase();
const signalClass=(signalType==='sell'||signalType==='close_short')?'sell':'buy';
const signalTag=signalType?`<span class="live-review-signal-tag ${signalClass}">${esc(signalType)}</span>`:'--';
const pnl=Number(row?.pnl||0);
const pnlClass=Number.isFinite(pnl)?(pnl>=0?'positive':'negative'):'';
const notional=Number(row?.notional||0);
const feeUsd=Number(row?.fee_usd||0);
const slippageUsd=Number(row?.slippage_cost_usd||0);
const orderId=String(row?.order_id||'');
return`<tr>
<td>${esc(fmtDateTime(row?.timestamp))}</td>
<td><div class="live-review-strategy" title="${esc(strategy)}">${esc(strategy)}</div><span class="live-review-count-tag">#${Number.isFinite(strategyCount)?strategyCount:'--'}</span></td>
<td>${esc(String(row?.exchange||'-').toUpperCase())} ${esc(String(row?.symbol||'--'))}</td>
<td>${signalTag}<div style="margin-top:4px;color:#9fb1c9;">${esc(side||'--')}</div></td>
<td>${fmtNum(row?.quantity,6)} @ ${fmtNum(row?.fill_price,4)}</td>
<td>${Number.isFinite(notional)?fmt(notional):'--'}</td>
<td class="${pnlClass}">${Number.isFinite(pnl)?fmt(pnl):'--'}</td>
<td><div>${Number.isFinite(feeUsd)?`fee ${fmt(feeUsd)}`:'fee --'}</div><div style="color:#9fb1c9;">slip ${Number.isFinite(slippageUsd)?fmt(slippageUsd):'--'}</div></td>
<td>${orderId?`<span title="${esc(orderId)}">${esc(orderId.slice(0,18))}${orderId.length>18?'...':''}</span>`:'--'}</td>
</tr>`;
}).join('');
if(tbody.innerHTML!==rowsHtml)tbody.innerHTML=rowsHtml;
tbody.dataset.hasData='1';
}

async function loadLiveTradeReview(){
const opts=arguments[0]||{};
const showLoading=opts.showLoading!==false;
const force=!!opts.force;
const minIntervalMs=Math.max(0,Number(opts.minIntervalMs||0));
const now=Date.now();
const hours=Math.max(1,Math.min(24*365,Number(document.getElementById('live-review-hours')?.value||168)));
const strategy=String(document.getElementById('live-review-strategy')?.value||'').trim();
const reqKey=`${hours}|${strategy}`;
if(!force&&loadLiveTradeReview._inflight){
  return loadLiveTradeReview._inflight;
}
if(!force&&minIntervalMs>0&&loadLiveTradeReview._lastReqKey===reqKey&&now-Number(loadLiveTradeReview._lastSuccessAt||0)<minIntervalMs){
  return;
}
const tbody=document.getElementById('live-review-tbody');
const hadStableData=!!(tbody&&tbody.dataset.hasData==='1');
if(showLoading&&tbody&&!hadStableData)tbody.innerHTML='<tr><td colspan="9">加载中...</td></tr>';
const task=(async()=>{
  try{
    let ep=`/trading/analytics/live-trade-review?hours=${hours}&limit=200`;
    if(strategy)ep+=`&strategy=${encodeURIComponent(strategy)}`;
    const d=await api(ep,{timeoutMs:15000});
    renderLiveTradeReview(d||{});
    loadLiveTradeReview._lastSuccessAt=Date.now();
    loadLiveTradeReview._lastReqKey=reqKey;
  }catch(e){
    if(showLoading&&!hadStableData){
      const summaryEl=document.getElementById('live-review-summary');
      if(summaryEl)summaryEl.innerHTML=`<div class="live-review-chip"><span class="k">状态</span><span class="v">加载失败</span></div><div class="live-review-chip"><span class="k">原因</span><span class="v" title="${esc(e.message)}">${esc(e.message)}</span></div><div class="live-review-chip"><span class="k">建议</span><span class="v">稍后重试</span></div>`;
      if(tbody)tbody.innerHTML=`<tr><td colspan="9">复盘记录加载失败: ${esc(e.message)}</td></tr>`;
    }else{
      console.warn('live trade review refresh failed:',e);
    }
  }
})();
loadLiveTradeReview._inflight=task;
try{
  await task;
}finally{
  if(loadLiveTradeReview._inflight===task)loadLiveTradeReview._inflight=null;
}
}

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
async function loadPositions(){return runRequestSingleFlight('positions',async()=>{try{const resp=await api('/trading/positions',{timeoutMs:TRADING_POSITIONS_TIMEOUT_MS});state.positions=resp.positions||[];const t=document.getElementById('positions-tbody');if(!t)return;if(!state.positions.length){t.innerHTML='<tr><td colspan="6">暂无持仓</td></tr>';return;}t.innerHTML=state.positions.map(p=>{const source=(p?.metadata?.source||'local');const key=positionCloseKey(p);const busy=!!state.closingPositions[key];const sideText=p.side==='long'?'多':p.side==='short'?'空':(p.side||'-');const sourceTag=source==='exchange_live'?'<span class="status-badge" style="margin-left:6px;background:#2f4f7f;">实盘同步</span>':'';const accountId=String(p.account_id||'');return `<tr><td>${p.exchange||'-'} ${p.symbol}${sourceTag}</td><td>${sideText}</td><td>${Number(p.entry_price||0).toFixed(2)}</td><td>${Number(p.current_price||0).toFixed(2)}</td><td class="${Number(p.unrealized_pnl||0)>=0?'positive':'negative'}">${fmt(p.unrealized_pnl||0)}</td><td><button class="btn btn-danger btn-sm" ${busy?'disabled':''} onclick="closePositionFromRow(this)" data-exchange="${esc(p.exchange||'')}" data-symbol="${esc(p.symbol||'')}" data-side="${esc(p.side||'')}" data-account-id="${esc(accountId)}" data-source="${esc(source)}" data-quantity="${Number(p.quantity||0)}">${busy?'平仓中...':'一键平仓'}</button></td></tr>`;}).join('');}catch(e){console.error(e);const t=document.getElementById('positions-tbody');if(t)t.innerHTML=`<tr><td colspan="6">持仓加载失败：${esc(e.message||'未知错误')}</td></tr>`;}});}
async function loadOrders(){return runRequestSingleFlight('orders',async()=>{try{
state.orders=(await api('/trading/orders?include_history=true&limit=200',{timeoutMs:TRADING_ORDERS_TIMEOUT_MS})).orders||[];
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
}catch(e){console.error(e);const t=document.getElementById('orders-tbody');if(t)t.innerHTML=`<tr><td colspan="8">订单加载失败：${esc(e.message||'未知错误')}</td></tr>`;}});}
async function loadOpenOrders(){return runRequestSingleFlight('openOrders',async()=>{try{
const rows=((await api('/trading/orders?include_history=false&limit=200',{timeoutMs:TRADING_OPEN_ORDERS_TIMEOUT_MS})).orders||[]);
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
}});}
function bindOrderView(){
const v=document.getElementById('orders-view-filter'),b=document.getElementById('btn-refresh-orders');
if(v)v.onchange=()=>loadOrders();
if(b)b.onclick=()=>loadOrders();
const bo=document.getElementById('btn-refresh-open-orders');
if(bo)bo.onclick=()=>loadOpenOrders();
}
function bindLiveTradeReview(){
const btn=document.getElementById('btn-refresh-live-review');
const hours=document.getElementById('live-review-hours');
const strategy=document.getElementById('live-review-strategy');
if(btn)btn.onclick=()=>loadLiveTradeReview({showLoading:true,force:true});
if(hours)hours.onchange=()=>loadLiveTradeReview({showLoading:true,force:true});
if(strategy)strategy.addEventListener('keydown',e=>{
  if(e.key==='Enter'){
    e.preventDefault();
    loadLiveTradeReview({showLoading:true,force:true});
  }
});
}
async function cancelOrder(id,symbol,exchange){try{await api(`/trading/order/${id}?symbol=${encodeURIComponent(symbol)}&exchange=${exchange}`,{method:'DELETE'});notify('订单已撤销');await Promise.allSettled([loadOrders(),loadOpenOrders()]);}catch(e){notify(`撤销失败: ${e.message}`,true);}}

async function loadStrategies(){return runRequestSingleFlight('strategies',async()=>{try{
await Promise.allSettled([ensureStrategyCatalog(),ensureBacktestStrategySelect()]);
const d=await api('/strategies/list');
const availableTypes=Array.isArray(d?.strategies)?d.strategies:[];
state.availableStrategyTypes=availableTypes;
state.strategies=d.registered||[];
const pool=document.getElementById('strategies-list');
if(pool){
const catalog=backtestCompareCatalog();
const catalogRows=strategyCatalogMap();
const libraryRows=availableTypes.map(s=>{
  const row=catalogRows[s]||{};
  const m=getStrategyMeta(s);
  const groupLabel=(catalog.byValue?.[s]?.groupLabel)||mapStrategyCatToBacktestGroup(row.category||m.cat);
  const desc=String(row.usage||m.desc||s);
  const card=`<div class="strategy-card" onclick="registerStrategy('${s}')"><div class="list-item" style="padding:0 0 6px 0;border-bottom:none;"><h4>${s}</h4><span class="status-badge">${row.category||m.cat}</span></div><p>${desc}</p><p style="font-size:11px;color:#8fa6c0;">点击卡片注册到策略池（模拟盘）</p></div>`;
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
  const cat=getStrategyMeta(stype).cat;
  const sState=String(s?.state||'').toLowerCase();
  if(filters.category && cat!==filters.category)return false;
  if(filters.state && sState!==filters.state)return false;
  if(filters.search){
    const symbols=Array.isArray(s?.symbols)?s.symbols:[];
    const ownership=normalizeStrategyOwnership(s);
    const searchBlob=[
      s?.name,
      stype,
      strategyTypeShortName(stype),
      cat,
      s?.timeframe,
      ownership.label,
      ownership.detail,
      ...symbols,
      buildStrategyShortDisplayLabel(s),
    ].map(x=>String(x||'').toLowerCase()).join(' | ');
    if(!searchBlob.includes(filters.search))return false;
  }
  return true;
});
if(metaEl){
  const runningCount=(state.strategies||[]).filter(x=>String(x?.state||'')==='running').length;
  const ownershipSummary=summarizeStrategyOwnershipCounts(state.strategies||[]);
  metaEl.textContent=`已注册 ${state.strategies.length} | 运行中 ${runningCount}${ownershipSummary?` | ${ownershipSummary}`:''} | 当前显示 ${filteredStrategies.length} | 点击卡片在右侧编辑`;
}
if(!state.strategies.length){grid.innerHTML='<div class="list-item">暂无已注册策略</div>';return;}
if(!filteredStrategies.length){grid.innerHTML='<div class="list-item">没有匹配筛选条件的策略实例</div>';return;}
const typeCounts=filteredStrategies.reduce((m,s)=>{const k=String(s?.strategy_type||'未知');m[k]=(m[k]||0)+1;return m;},{});
const typeSeen={};
const grouped={};
filteredStrategies.forEach(s=>{const cat=getStrategyMeta(s?.strategy_type).cat;(grouped[cat]||(grouped[cat]=[])).push(s);});
const catOrder=['趋势','震荡','突破','均值回归','动量','反转','统计套利','成交量','波动率','风险','微观结构','套利','量化','宏观','其他'];
grid.innerHTML=catOrder.filter(cat=>Array.isArray(grouped[cat])&&grouped[cat].length).map(cat=>{
  const cards=(grouped[cat]||[]).map(s=>{
const stype=String(s?.strategy_type||'未知');
typeSeen[stype]=(typeSeen[stype]||0)+1;
const r=s.runtime||{},a=Number(s.allocation||0),m=getStrategyMeta(s.strategy_type);
const uptime=fmtDurationSec(r.uptime_seconds||0),accountId=String(r.account_id||s.account_id||'main');
const isolated=Boolean(r.isolated_account),runnerAlive=Boolean(r.runner_alive);
const typeCount=Number(typeCounts[stype]||1),typeIndex=Number(typeSeen[stype]||1);
const shortLabel=buildStrategyShortDisplayLabel(s,typeIndex,typeCount);
const shortType=strategyTypeShortName(stype);
const symbolsArr=Array.isArray(s.symbols)?s.symbols:[];
const symbolFull=symbolsArr.length?symbolsArr.join(', '):'全部';
const symbolMain=symbolsArr.length?String(symbolsArr[0]).replace('/USDT','').replace('/USD',''):'全部';
const symbolText=symbolsArr.length>1?`${symbolMain} +${symbolsArr.length-1}`:symbolMain;
const ownership=normalizeStrategyOwnership(s);
const active=String(state.selectedStrategyName||'')===String(s.name||'');
const pnlPerf=(state.summary?.strategy_performance||{})[s.name]||{};
const rp=Number(pnlPerf.return_pct);
const rpText=Number.isFinite(rp)?`${rp.toFixed(2)}%`:'--';
return `<div class="registered-strategy-card ${active?'active':''}" onclick="selectRegisteredStrategy('${esc(String(s.name||''))}')">
  <div class="topline">
    <div class="name" title="${esc(String(s.name||''))}">${esc(shortLabel)}</div>
    <span class="status-badge ${String(s.state||'')==='running'?'connected':''}">${mapState(s.state)}</span>
  </div>
  <div class="subline">
    <div class="sub" title="${esc(String(s.name||''))}">${esc(shortType)} · 实例 ${esc(shortInstanceId(s.name))}</div>
    <span class="strategy-owner-badge ${ownership.tone}" title="${esc(ownership.detail)}">${esc(ownership.label)}</span>
  </div>
  <div class="owner-note" title="${esc(ownership.detail)}">${esc(ownership.detail)}</div>
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
renderStrategyConsolePanel();
}catch(e){console.error(e);const pool=document.getElementById('strategies-list');if(pool&&/加载中/.test(String(pool.textContent||'')))pool.innerHTML=`<div class="list-item">策略目录加载失败：${esc(e.message||'未知错误')}</div>`;const grid=document.getElementById('registered-strategies-grid');if(grid&&/加载中/.test(String(grid.textContent||'')))grid.innerHTML=`<div class="list-item">已注册策略加载失败：${esc(e.message||'未知错误')}</div>`;const metaEl=document.getElementById('registered-strategy-meta');if(metaEl&&/加载中/.test(String(metaEl.textContent||'')))metaEl.textContent='策略列表加载失败，稍后自动重试';renderStrategyConsolePanel();}});}
function renderStrategyConsolePanel(){
const cards=document.getElementById('strategy-console-cards');
const notes=document.getElementById('strategy-console-notes');
if(!cards)return;
const registered=Array.isArray(state.strategies)?state.strategies.length:0;
const summary=state.summary||{};
const running=Number((summary.running_count ?? (summary.running||[]).length) || 0);
const paused=Number(summary.paused_count||0);
const idle=Number(summary.idle_count||0);
const stopped=Number(summary.stopped_count||0);
const staleCount=Number((summary.stale_running_count ?? (summary.stale_running||[]).length) || 0);
const catalogRows=Array.isArray(state.strategyCatalogRows)?state.strategyCatalogRows:[];
const availableCount=catalogRows.length || (Array.isArray(state.availableStrategyTypes)?state.availableStrategyTypes.length:0);
const backtestableCount=catalogRows.filter(row=>row?.backtest_supported).length;
const autoStartCount=catalogRows.filter(row=>row?.default_start).length;
const health=state.strategyHealth||{};
const lastCheck=health?.last_check_at ? fmtDateTime(health.last_check_at) : '--';
const lastAlert=health?.last_alert_at ? fmtDateTime(health.last_alert_at) : '--';
const recentSignals=Array.isArray(summary.recent_signals)?summary.recent_signals.length:0;
cards.innerHTML=`
<div class="strategy-console-stat">
  <div class="label">策略总览</div>
  <div class="value">${registered}</div>
  <div class="hint">已注册实例 ${registered} 个，最近信号 ${recentSignals} 条</div>
</div>
<div class="strategy-console-stat">
  <div class="label">运行状态</div>
  <div class="value">${running}</div>
  <div class="hint">运行中 ${running} / 空闲 ${idle} / 暂停 ${paused} / 停止 ${stopped}</div>
</div>
<div class="strategy-console-stat">
  <div class="label">可回测策略</div>
  <div class="value">${backtestableCount || availableCount}</div>
  <div class="hint">目录共 ${availableCount} 个，默认推荐 ${autoStartCount} 个</div>
</div>
<div class="strategy-console-stat">
  <div class="label">健康告警</div>
  <div class="value" style="color:${staleCount>0?'#ffb15f':'#dfe9f7'};">${staleCount}</div>
  <div class="hint">最近检查 ${esc(lastCheck)}${lastAlert!=='--' ? ` | 最近告警 ${esc(lastAlert)}` : ''}</div>
</div>`;
if(notes){
  const hints=[
    running>0?`当前有 ${running} 个策略实例在运行`:'当前没有运行中的策略实例',
    staleCount>0?`发现 ${staleCount} 个运行异常实例，建议先执行健康检查再决定是否停机`:'运行器健康，无阻塞告警',
    backtestableCount>0?`可直接从“可用策略”卡片注册 ${backtestableCount} 个可回测策略`:'策略目录尚未完成加载',
  ];
  notes.textContent=hints.join('；');
}
}
async function registerStrategy(type){
try{
// Use catalog metadata first; library metadata is only a fallback.
const libEntry=(state.strategyLibraryRows||[]).find(r=>r.name===type);
const catalogEntry=(strategyCatalogMap()||{})[String(type||'').trim()]||{};
const hardcoded={
PairsTradingStrategy:{exchange:'binance',timeframe:'1h',symbols:['BTC/USDT','ETH/USDT']},
}[type];
const profile={
  exchange:'binance',
  timeframe:hardcoded?.timeframe||catalogEntry?.recommended_timeframe||libEntry?.default_timeframe||'15m',
  symbols:hardcoded?.symbols||(Array.isArray(catalogEntry?.recommended_symbols)&&catalogEntry.recommended_symbols.length?catalogEntry.recommended_symbols:(libEntry?.default_symbols?.length?libEntry.default_symbols:['BTC/USDT'])),
};
const name=`${type}_${Date.now()}`;
await api('/strategies/register',{method:'POST',body:JSON.stringify({name,strategy_type:type,params:{},symbols:profile.symbols,timeframe:profile.timeframe,exchange:profile.exchange,allocation:DEFAULT_STRATEGY_ALLOCATION})});
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
  allocation: Number(info?.allocation??DEFAULT_STRATEGY_ALLOCATION),
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
const panel=document.getElementById('strategy-edit-panel');
if(panel)panel.scrollIntoView({behavior:'smooth',block:'nearest'});
await loadStrategies();
}
async function saveAllocation(name){const i=document.querySelector(`input[data-alloc='${name}']`);if(!i)return;try{await api(`/strategies/${name}/allocation`,{method:'PUT',body:JSON.stringify({allocation:Number(i.value||0)})});notify(`策略 ${name} 资金占比已更新`);await Promise.all([loadStrategies(),loadStrategySummary()]);}catch(e){notify(`更新资金占比失败: ${e.message}`,true);}}
async function toggleStrategy(name,st){const act=st==='running'?'stop':'start';try{await api(`/strategies/${encodeURIComponent(name)}/${act}`,{method:'POST',timeoutMs:15000});notify(`策略已${act==='start'?'启动':'停止'}`);await Promise.all([loadStrategies(),loadStrategySummary()]);}catch(e){notify(`策略${act}失败: ${e.message}`,true);}}

async function loadStrategySummary(){return runRequestSingleFlight('strategySummary',async()=>{try{
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
a.innerHTML=(running.map(s=>{const p=perf[s.name]||{},rt=s.runtime||{};const rp=Number(p.return_pct),dd=Number(p.max_drawdown_pct),vv=Number(p.variance),up=fmtDurationSec(rt.uptime_seconds||0);const rpTxt=Number.isFinite(rp)?`${rp.toFixed(2)}%`:'--';const ddTxt=Number.isFinite(dd)?`${dd.toFixed(2)}%`:'--';const varTxt=Number.isFinite(vv)?vv.toExponential(2):'--';const acct=esc(rt.account_id||s.account_id||'main');const modeTxt=rt.isolated_account?'独立':'共享';return `<div class="list-item"><span>${s.name} (${s.strategy_type}) | 收益率 ${rpTxt} | 回撤 ${ddTxt} | 方差 ${varTxt} | 运行 ${up} | ${modeTxt}:${acct} ${s.last_run_at?`· ${fmtTime(s.last_run_at)}`:''}</span><span class="status-badge connected">运行中</span></div>`;}).join('')||'<div class="list-item">暂无运行中策略</div>')+staleTip;
}
if(r)r.innerHTML=signals.length?signals.map(s=>`<div class="list-item"><span>${s.strategy} | ${s.symbol} | ${s.signal_type.toUpperCase()}</span><span>${fmtTime(s.timestamp)}</span></div>`).join(''):`<div class="list-item"><span>${running.length?`实时刷新中（${d.refresh_hint_seconds||5}秒）暂无新信号，可能是策略条件未触发`:'暂无近期信号'}</span><span>${fmtTime(new Date())}</span></div>`;
if(rt){
rt.innerHTML=running.length?running.map(s=>{const p=perf[s.name]||{},ri=s.runtime||{};const rp=Number(p.return_pct),dd=Number(p.max_drawdown_pct),realized=Number(p.realized_pnl),unrealized=Number(p.unrealized_pnl),absPnl=(Number.isFinite(realized)?realized:0)+(Number.isFinite(unrealized)?unrealized:0),lu=p.last_update;const runtimeTxt=fmtDurationSec(ri.uptime_seconds||0);const lastRunTxt=s.last_run_at?fmtDateTime(s.last_run_at):'-';const rpTxt=Number.isFinite(rp)?`${rp.toFixed(2)}%`:'--';const ddTxt=Number.isFinite(dd)?`${dd.toFixed(2)}%`:'--';const absTxt=Number.isFinite(absPnl)?fmt(absPnl):'--';const rpCls=Number.isFinite(rp)?(rp>=0?'positive':'negative'):'';const absCls=Number.isFinite(absPnl)?(absPnl>=0?'positive':'negative'):'';const stype=s.strategy_type||s.name;const meta=getStrategyMeta(stype);const desc=meta.desc||s.description||stype;const cat=meta.cat||'';return`<tr><td>${s.name}</td><td style="font-size:12px;color:#9fb1c9;max-width:200px;">${cat?`[${cat}] `:''}${esc(desc)}</td><td class="${rpCls}">${rpTxt}</td><td>${ddTxt}</td><td class="${absCls}">${absTxt}</td><td>${runtimeTxt}</td><td>${lastRunTxt}</td><td>${lu?fmtDateTime(lu):'-'}</td></tr>`;}).join(''):'<tr><td colspan="8">暂无运行中策略数据</td></tr>';
}
renderStrategyHealthAlerts(d,state.strategyHealth);
renderStrategyConsolePanel();
}catch(e){console.error(e);const msg=esc(e.message||'未知错误');const a=document.getElementById('active-strategies');if(a&&(/加载中/.test(String(a.textContent||''))||!String(a.innerHTML||'').trim()))a.innerHTML=`<div class="list-item"><span>策略摘要加载失败</span><span>${msg}</span></div>`;const r=document.getElementById('recent-signals');if(r&&(/加载中/.test(String(r.textContent||''))||!String(r.innerHTML||'').trim()))r.innerHTML=`<div class="list-item"><span>近期信号加载失败</span><span>${msg}</span></div>`;const rt=document.getElementById('strategy-runtime-tbody');if(rt&&(/加载中/.test(String(rt.textContent||''))||!String(rt.innerHTML||'').trim()))rt.innerHTML=`<tr><td colspan="8">运行中策略摘要加载失败：${msg}</td></tr>`;const box=document.getElementById('strategy-health-alerts');if(box&&/加载中/.test(String(box.textContent||'')))box.innerHTML=`<div class="list-item"><span>策略健康摘要加载失败</span><span>${msg}</span></div>`;renderStrategyConsolePanel();}});}
function renderStrategyHealthAlerts(summary,health){
const box=document.getElementById('strategy-health-alerts');if(!box)return;
const stale=(summary?.stale_running||[]);const staleCount=Number(summary?.stale_running_count||stale.length||0);const runningCount=Number(summary?.running_count||0);
const monitor=health||{};const lastCheck=monitor?.last_check_at;const lastAlert=monitor?.last_alert_at;const lastErr=monitor?.last_error;
if(staleCount<=0){
box.innerHTML=`<div class="list-item"><span>状态</span><span class="status-badge connected">健康</span></div><div class="list-item"><span>运行中策略</span><span>${runningCount}</span></div><div class="list-item"><span>最近检查</span><span>${lastCheck?fmtTime(lastCheck):'--'}</span></div>`;
state.lastHealthAlertKey='';
return;
}
const staleRows=stale.slice(0,6).map(x=>{const lag=(x&&x.lag_seconds!==undefined&&x.lag_seconds!==null)?`${x.lag_seconds}s`:'--';return `<div class="list-item"><span>${x.strategy||'未知策略'} (${x.timeframe||'-'})</span><span style="color:#ffb15f;">延迟 ${lag}</span></div>`;}).join('');
box.innerHTML=`<div class="list-item"><span>状态</span><span class="status-badge" style="background:rgba(255,177,95,.15);color:#ffb15f;border-color:rgba(255,177,95,.35);">告警</span></div><div class="list-item"><span>异常策略数</span><span>${staleCount}</span></div>${staleRows||''}<div class="list-item"><span>最近告警</span><span>${lastAlert?fmtTime(lastAlert):'--'}</span></div>${lastErr?`<div class="list-item"><span>监控错误</span><span style="color:#ff9b9b;">${esc(lastErr)}</span></div>`:''}`;
const alertKey=`${staleCount}|${stale.map(x=>x.strategy).join(',')}`;
if(alertKey!==state.lastHealthAlertKey){state.lastHealthAlertKey=alertKey;notify(`【策略健康告警】异常策略 ${staleCount} 个`,true);}
}
function pushRealtimeSignal(sig){try{if(!sig)return;const item={strategy:sig.strategy_name||sig.strategy||'未知策略',symbol:sig.symbol||'-',signal_type:String(sig.signal_type||'').toLowerCase(),timestamp:sig.timestamp||new Date().toISOString()};const cur=state.summary?.recent_signals||[];const key=`${item.strategy}|${item.symbol}|${item.signal_type}|${item.timestamp}`;const map=new Map();[item,...cur].forEach(x=>{const k=`${x.strategy||x.strategy_name}|${x.symbol}|${String(x.signal_type||'').toLowerCase()}|${x.timestamp}`;if(!map.has(k))map.set(k,x);});state.summary.recent_signals=[...map.values()].slice(0,20);const r=document.getElementById('recent-signals');if(r){r.innerHTML=state.summary.recent_signals.slice(0,12).map(s=>`<div class=\"list-item\"><span>${s.strategy||s.strategy_name} | ${s.symbol} | ${String(s.signal_type||'').toUpperCase()}</span><span>${fmtTime(s.timestamp)}</span></div>`).join('');}}catch(e){console.error(e);}}
async function loadStrategyHealth(){return runRequestSingleFlight('strategyHealth',async()=>{
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
renderStrategyConsolePanel();
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
renderStrategyConsolePanel();
out.textContent=JSON.stringify({fallback:'summary',running_count:(s.running||[]).length,stale_running:s.stale_running||[],runtime:s.runtime||{},timestamp:s.timestamp||new Date().toISOString(),note:'健康监控接口不可用，已降级显示策略摘要'},null,2);
}catch{
out.textContent=`加载策略健康状态失败: ${lastErr}`;
}
});}
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
  api(`/strategies/${name}/sizing-preview`,{timeoutMs:8000}).catch(()=>null),
]);
const runtime=info.runtime||{};
const ownership=normalizeStrategyOwnership(info);
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
panel.innerHTML=`<div class="form-group"><label>策略: ${info.name} (${info.strategy_type})</label><div class="list-item"><span>归属</span><span class="strategy-owner-inline"><span class="strategy-owner-badge ${ownership.tone}">${esc(ownership.label)}</span><span class="strategy-owner-note" title="${esc(ownership.detail)}">${esc(ownership.detail)}</span></span></div><div class="list-item"><span>状态</span><span>${mapState(info.state)}</span></div><div class="list-item"><span>周期</span><span>${esc(info.timeframe||'-')}</span></div><div class="list-item"><span>交易对</span><span>${esc(currentSymbols.join(', '))}</span></div><div class="list-item"><span>最近运行</span><span>${info.last_run_at?fmtDateTime(info.last_run_at):'-'}</span></div><div class="list-item"><span>运行时长限制</span><span>${runtime.runtime_limit_minutes?`${runtime.runtime_limit_minutes} 分钟`:'不限时'}${runtime.remaining_seconds!==undefined&&runtime.remaining_seconds!==null?` | 剩余 ${fmtDurationSec(runtime.remaining_seconds)}`:''}</span></div></div>${sizingHtml}<div class="inline-actions" style="margin-top:4px;"><button class="btn btn-primary btn-sm" id="edit-toggle">${info.state==='running'?'停止策略':'启动策略'}</button><button class="btn btn-primary btn-sm" id="edit-clone">复制新实例</button><button class="btn btn-danger btn-sm" id="edit-delete">删除实例</button><button class="btn btn-primary btn-sm" id="edit-cmp">刷新对比</button>${canApplyBestOpt?'<button class="btn btn-primary btn-sm" id="edit-apply-best-opt">应用最近优化最佳参数</button>':''}</div><div class="param-grid"><div class="form-group"><label>策略周期（timeframe）</label><select id="edit-timeframe">${tfHtml}</select></div><div class="form-group"><label>交易对（逗号分隔，可多币）</label><input id="edit-symbols" type="text" value="${esc(currentSymbols.join(', '))}" placeholder="例如 ETH/USDT 或 BTC/USDT,ETH/USDT"></div><div class="form-group"><label>策略运行时长（分钟，0=不限）</label><input id="edit-runtime-min" type="number" min="0" max="10080" step="1" value="${Number(runtime.runtime_limit_minutes||0)}"></div><div class="form-group"><label>资金占比 (0~1)</label><input id="edit-alloc" type="number" min="0" max="1" step="0.01" value="${Number(info.allocation||0).toFixed(2)}"></div></div><div class="param-grid">${fields||'<div class="list-item">该策略无可编辑参数</div>'}</div><div class="inline-actions" style="margin-top:10px;"><button class="btn btn-primary btn-sm" id="edit-save">保存参数</button><button class="btn btn-primary btn-sm" id="edit-save-as">另存为新实例（当前编辑值）</button></div><pre id="editor-compare-output" class="output-box">点击“刷新对比”查看实盘与回测差异</pre>`;
panel.classList.add('strategy-edit-active');
openStrategyMonitor(name).catch(() => {});
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
const resetView=!preserveRange||chartChanged||!marketDataState.lastRange;
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
marketDataState.lastRange=null; // Force autoscale on new data load
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
const renderResearchSymbolSelects=symbols=>{
  const normalized=[];
  const seen=new Set();
  (Array.isArray(symbols)?symbols:[]).forEach(sym=>{
    const text=String(sym||'').trim();
    if(!text||seen.has(text))return;
    seen.add(text);
    normalized.push(text);
  });
  const finalSymbols=normalized.length?normalized:[...RESEARCH_DEFAULT_SYMBOLS];
  const primary=document.getElementById('research-symbol');
  if(primary){
    const current=String(primary.value||'BTC/USDT').trim()||'BTC/USDT';
    primary.innerHTML=finalSymbols.map(sym=>`<option value="${esc(sym)}"${sym===current?' selected':''}>${esc(sym)}</option>`).join('');
    primary.value=finalSymbols.includes(current)?current:(finalSymbols.includes('BTC/USDT')?'BTC/USDT':finalSymbols[0]);
  }
  const multi=document.getElementById('research-symbols');
  if(multi){
    const currentSet=new Set(getSelectValues('research-symbols'));
    const chosen=currentSet.size?currentSet:new Set(RESEARCH_DEFAULT_SYMBOLS.filter(sym=>finalSymbols.includes(sym)));
    multi.innerHTML=finalSymbols.map(sym=>`<option value="${esc(sym)}"${chosen.has(sym)?' selected':''}>${esc(sym)}</option>`).join('');
    if(!Array.from(multi.selectedOptions||[]).length && multi.options.length){
      const fallbackList=RESEARCH_DEFAULT_SYMBOLS.filter(sym=>finalSymbols.includes(sym)).slice(0,Math.min(30,multi.options.length));
      setSelectValues('research-symbols',fallbackList.length?fallbackList:[multi.options[0].value]);
    }
  }
};
renderResearchSymbolSelects(RESEARCH_DEFAULT_SYMBOLS);
renderResearchStatusCards();
try{
const ex=String(exchange||getResearchExchange()||'binance').trim().toLowerCase()||'binance';
const resp=await api(`/data/research/symbols?exchange=${encodeURIComponent(ex)}`,{timeoutMs:15000});
const symbols=(Array.isArray(resp?.symbols)?resp.symbols:[]).filter(Boolean);
if(symbols.length)renderResearchSymbolSelects(symbols);
renderResearchStatusCards();
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
async function pollBatchDownloadTasks(taskIds,{timeoutMs=25*60*1000,intervalMs=3000}={}){
const ids=Array.from(new Set((Array.isArray(taskIds)?taskIds:[]).map(v=>String(v||'').trim()).filter(Boolean)));
if(!ids.length)return[];
const start=Date.now();
while(Date.now()-start<timeoutMs){
  const resp=await api(`/data/download/tasks?task_ids=${encodeURIComponent(ids.join(','))}`,{timeoutMs:15000});
  const tasks=Array.isArray(resp?.tasks)?resp.tasks:[];
  const taskMap=new Map(tasks.map(task=>[String(task?.task_id||'').trim(),task]));
  const matched=ids.map(id=>taskMap.get(id)).filter(Boolean);
  if(matched.length===ids.length&&matched.every(task=>['completed','failed'].includes(String(task?.status||'')))){
    return ids.map(id=>taskMap.get(id)).filter(Boolean);
  }
  await new Promise(r=>setTimeout(r,intervalMs));
}
throw new Error(`批量下载超时: ${ids.length} 个任务`);
}
function getDownloadOutputEl(){return document.getElementById('download-output');}
function getResearchRefreshStatusEl(){return document.getElementById('download-research-refresh-status');}
function formatResearchRefreshStatus(payload,{multiline=false}={}){
const task=payload?.task||{};
const summary=payload?.summary||{};
const state=String(task?.state_label||task?.state||'未注册').trim()||'未注册';
const lastRun=task?.last_run_time?fmtDateTime(task.last_run_time):'--';
const nextRun=task?.next_run_time?fmtDateTime(task.next_run_time):'--';
const summaryUpdated=summary?.updated_at?fmtDateTime(summary.updated_at):(summary?.timestamp?fmtDateTime(summary.timestamp):'--');
const summaryState=String(summary?.status||'').trim();
const timeframes=(Array.isArray(summary?.timeframes)?summary.timeframes:[]).filter(Boolean);
const secondsSymbols=(Array.isArray(summary?.seconds_symbols)?summary.seconds_symbols:[]).filter(Boolean);
const lines=[
  `研究币池增量追平: ${state}`,
  `最近执行: ${lastRun} / 下次执行: ${nextRun}`,
  `最近摘要: ${summaryUpdated} / rows ${Number(summary?.downloaded_rows_total||0).toLocaleString('zh-CN')} / failures ${Number(summary?.failures_count||0)}`
];
if(summaryState)lines.push(`摘要状态: ${summaryState}`);
if(timeframes.length||secondsSymbols.length){
  lines.push(`当前配置: ${timeframes.length?timeframes.join(' / '):'--'}${secondsSymbols.length?` / 空闲 1s ${secondsSymbols.join(', ')}`:''}`);
}
if(task?.error)lines.push(`任务错误: ${task.error}`);
if(summary?.error)lines.push(`摘要错误: ${summary.error}`);
return multiline?lines.join('\n'):lines.slice(0,4).join(' ｜ ');
}
async function loadResearchUniverseRefreshStatus({silent=false}={}){
const statusEl=getResearchRefreshStatusEl();
try{
  const payload=await api('/data/research/refresh/status',{timeoutMs:15000});
  if(statusEl)statusEl.textContent=`研究币池增量追平状态：${formatResearchRefreshStatus(payload)}`;
  return payload;
}catch(err){
  if(statusEl)statusEl.textContent=`研究币池增量追平状态读取失败: ${err.message}`;
  if(!silent)notify(`研究币池增量追平状态读取失败: ${err.message}`,true);
  throw err;
}
}
async function triggerResearchUniverseRefresh(btn){
const downloadOut=getDownloadOutputEl();
const statusEl=getResearchRefreshStatusEl();
const prevText=btn?btn.textContent:'';
try{
  if(btn){btn.disabled=true;btn.textContent='启动中...';}
  if(statusEl)statusEl.textContent='研究币池增量追平状态：正在启动...';
  const payload=await api('/data/research/refresh/start',{method:'POST',timeoutMs:90000});
  if(downloadOut)downloadOut.textContent=formatResearchRefreshStatus(payload,{multiline:true});
  if(statusEl)statusEl.textContent=`研究币池增量追平状态：${formatResearchRefreshStatus(payload)}`;
  notify(payload?.message||'研究币池增量追平已触发');
  setTimeout(()=>{loadResearchUniverseRefreshStatus({silent:true}).catch(()=>{});},3000);
  return payload;
}catch(err){
  if(downloadOut)downloadOut.textContent=`研究币池增量追平启动失败: ${err.message}`;
  if(statusEl)statusEl.textContent=`研究币池增量追平状态：启动失败 - ${err.message}`;
  notify(`研究币池增量追平启动失败: ${err.message}`,true);
  throw err;
}finally{
  if(btn){btn.disabled=false;btn.textContent=prevText;}
}
}
function parseDownloadBatchSymbols(raw,fallback=''){
const text=String(raw||'').trim();
const source=text||String(fallback||'').trim();
if(!source)return[];
return Array.from(new Set(source.split(/[\s,;，、]+/).map(item=>String(item||'').trim().toUpperCase()).filter(Boolean).map(item=>item.includes('/')?item:`${item}/USDT`)));
}
function getDownloadDateRange(){
const startRaw=String(document.getElementById('download-start-date')?.value||'').trim();
const endRaw=String(document.getElementById('download-end-date')?.value||'').trim();
return{
  start_time:startRaw?`${startRaw}T00:00:00`:null,
  end_time:endRaw?`${endRaw}T23:59:59`:null,
};
}
function getDownloadRequestedDays(tf,range={}){
const inputEl=document.getElementById('download-days');
const rawValue=Number(inputEl?.value||0);
const fallback=guessDataDownloadDays(tf);
const manualDays=Number.isFinite(rawValue)&&rawValue>0?Math.round(rawValue):fallback;
if(range?.start_time&&range?.end_time)return guessDataDownloadDays(tf,range.start_time,range.end_time);
return Math.max(1,Math.min(1200,manualDays));
}
function formatDownloadBatchSummary(payload,tasks=[]){
const symbols=Array.isArray(payload?.symbols)?payload.symbols:[];
const taskRows=Array.isArray(tasks)?tasks:[];
const completed=taskRows.filter(task=>String(task?.status||'')==='completed');
const failed=taskRows.filter(task=>String(task?.status||'')==='failed');
const totalCount=completed.reduce((sum,task)=>sum+Number(task?.result?.count||0),0);
return [
  `批量下载: ${payload?.exchange||'-'} / ${payload?.timeframe||'-'}`,
  `时间范围: ${(payload?.start_time||'未指定')} -> ${(payload?.end_time||'现在')}`,
  `币种数量: ${symbols.length}`,
  `任务结果: 完成 ${completed.length} / 失败 ${failed.length}`,
  `累计K线: ${Number(totalCount||0).toLocaleString('zh-CN')}`,
  `${symbols.length?`Symbols: ${symbols.join(', ')}`:'Symbols: -'}`,
  `${failed.length?`失败详情: ${failed.map(task=>`${task.symbol||task.task_id}: ${task.error||'unknown error'}`).join(' | ')}`:'失败详情: 无'}`,
].join('\n');
}
function scheduleDataChartReload(delay=180){
if(dataReloadTimer)clearTimeout(dataReloadTimer);
dataReloadTimer=setTimeout(()=>{
  loadKlinesByForm().catch(err=>console.warn('scheduleDataChartReload failed', err?.message||err));
}, Math.max(0, Number(delay||0)));
}
function guessDataDownloadDays(tf,start=null,end=null){
const t=String(tf||'1h').toLowerCase();
let fallback=365;
if(t.endsWith('s'))fallback=7;
else if(t==='1m')fallback=30;
else if(t==='5m')fallback=60;
else if(t==='15m')fallback=120;
else if(t==='1h')fallback=365;
const st=toDate(start),et=toDate(end);
if(st&&et&&et>st){
  const spanDays=Math.ceil((et.getTime()-st.getTime())/86400000)+2;
  return Math.max(1,Math.min(1200,spanDays));
}
return fallback;
}
function getDataHealthActionRows(action){
const rows=Array.isArray(dataHealthState.last?.datasets)?dataHealthState.last.datasets:[];
return rows.filter(row=>String(row?.recommended_action||'').trim()===String(action||'').trim());
}
async function executeDataHealthAction(target,{btn=null,refreshAfter=true}={}){
const action=String(target?.action||'').trim();
const exchange=String(target?.exchange||'').trim();
const symbol=String(target?.symbol||'').trim();
const timeframe=String(target?.timeframe||'').trim();
const start=String(target?.start||'').trim();
const end=String(target?.end||'').trim();
if(!action||!exchange||!symbol||!timeframe)throw new Error('缺少体检动作参数');
const notesEl=document.getElementById('data-storage-health-notes');
const prevText=btn?btn.textContent:'';
try{
  if(btn){btn.disabled=true;btn.textContent='处理中...';}
  if(action==='repair'){
    if(notesEl)notesEl.textContent=`正在补全 ${exchange} ${symbol} ${timeframe} ...`;
    try{
      await api(`/data/integrity/repair?exchange=${encodeURIComponent(exchange)}&symbol=${encodeURIComponent(symbol)}&timeframe=${encodeURIComponent(timeframe)}`,{method:'POST',timeoutMs:120000});
    }catch(err){
      const msg=String(err?.message||'');
      if(msg.includes('无本地数据可修复')||msg.includes('404')){
        const days=guessDataDownloadDays(timeframe,start,end);
        const query=end&&start
          ?`exchange=${encodeURIComponent(exchange)}&symbol=${encodeURIComponent(symbol)}&timeframe=${encodeURIComponent(timeframe)}&start_time=${encodeURIComponent(start)}&end_time=${encodeURIComponent(end)}&background=true`
          :`exchange=${encodeURIComponent(exchange)}&symbol=${encodeURIComponent(symbol)}&timeframe=${encodeURIComponent(timeframe)}&days=${days}&background=true`;
        const dl=await api(`/data/download?${query}`,{method:'POST',timeoutMs:20000});
        if(dl?.task_id)await pollDownloadTask(dl.task_id,{timeoutMs:20*60*1000,intervalMs:3000});
        await api(`/data/integrity/repair?exchange=${encodeURIComponent(exchange)}&symbol=${encodeURIComponent(symbol)}&timeframe=${encodeURIComponent(timeframe)}`,{method:'POST',timeoutMs:120000});
      }else{
        throw err;
      }
    }
    notify(`已补全 ${exchange} ${symbol} ${timeframe}`);
  }else if(action==='redownload'){
    if(notesEl)notesEl.textContent=`正在重拉 ${exchange} ${symbol} ${timeframe} ...`;
    const days=guessDataDownloadDays(timeframe,start,end);
    const query=end&&start
      ?`exchange=${encodeURIComponent(exchange)}&symbol=${encodeURIComponent(symbol)}&timeframe=${encodeURIComponent(timeframe)}&start_time=${encodeURIComponent(start)}&end_time=${encodeURIComponent(end)}&background=true`
      :`exchange=${encodeURIComponent(exchange)}&symbol=${encodeURIComponent(symbol)}&timeframe=${encodeURIComponent(timeframe)}&days=${days}&background=true`;
    const dl=await api(`/data/download?${query}`,{method:'POST',timeoutMs:20000});
    if(dl?.task_id)await pollDownloadTask(dl.task_id,{timeoutMs:20*60*1000,intervalMs:3000});
    notify(`已重拉 ${exchange} ${symbol} ${timeframe}`);
  }else{
    throw new Error(`未知动作: ${action}`);
  }
  if(document.getElementById('data-exchange')?.value===exchange&&document.getElementById('data-symbol')?.value===symbol&&document.getElementById('data-timeframe')?.value===timeframe){
    loadKlinesByForm().catch(()=>{});
  }
  if(refreshAfter)await loadDataStorageHealth(null,{skipStorage:false});
}finally{
  if(btn){btn.disabled=false;btn.textContent=prevText;}
}
}
async function runBatchDataHealthAction(action,btn){
const rows=getDataHealthActionRows(action);
if(!rows.length){notify(action==='redownload'?'当前没有需要重拉的损坏项':'当前没有需要补全的问题项',true);return;}
if(typeof confirm==='function'){
  const ok=confirm(`${action==='redownload'?'批量重拉':'批量补全'} ${rows.length} 个数据集，可能持续较久，是否继续？`);
  if(!ok)return;
}
const notesEl=document.getElementById('data-storage-health-notes');
const prevText=btn?btn.textContent:'';
let okCount=0,failCount=0;
const failures=[];
try{
  if(btn){btn.disabled=true;btn.textContent='批处理中...';}
  for(let i=0;i<rows.length;i++){
    const row=rows[i];
    if(notesEl)notesEl.textContent=`批处理进度 ${i+1}/${rows.length}: ${row.exchange} ${row.symbol} ${row.timeframe}`;
    try{
      await executeDataHealthAction({
        action,
        exchange:row.exchange,
        symbol:row.symbol,
        timeframe:row.timeframe,
        start:row.start,
        end:row.end,
      },{refreshAfter:false});
      okCount+=1;
    }catch(err){
      failCount+=1;
      failures.push(`${row.exchange} ${row.symbol} ${row.timeframe}: ${err.message}`);
    }
  }
  await loadDataStorageHealth(null,{skipStorage:false});
  if(notesEl&&failures.length){
    notesEl.textContent=`批处理完成：成功 ${okCount}，失败 ${failCount}\n${failures.slice(0,8).join('\n')}`;
  }
  notify(`${action==='redownload'?'批量重拉':'批量补全'}完成：成功 ${okCount}，失败 ${failCount}`,failCount>0);
}finally{
  if(btn){btn.disabled=false;btn.textContent=prevText;}
}
}
function bindDataStorageHealthActions(){
const tableEl=document.getElementById('data-storage-health-table');
if(!tableEl||tableEl._healthBound)return;
tableEl._healthBound=true;
tableEl.addEventListener('click',async e=>{
  const btn=e.target?.closest?.('button[data-health-action]');
  if(!btn)return;
  const action=String(btn.dataset.healthAction||'').trim();
  const exchange=String(btn.dataset.exchange||'').trim();
  const symbol=String(btn.dataset.symbol||'').trim();
  const timeframe=String(btn.dataset.timeframe||'').trim();
  const start=String(btn.dataset.start||'').trim();
  const end=String(btn.dataset.end||'').trim();
  if(!action||!exchange||!symbol||!timeframe)return;
  try{
    await executeDataHealthAction({action,exchange,symbol,timeframe,start,end},{btn});
  }catch(err){
    const notesEl=document.getElementById('data-storage-health-notes');
    if(notesEl)notesEl.textContent=`体检动作失败: ${err.message}`;
    notify(`体检动作失败: ${err.message}`,true);
  }
 });
}
function renderDataStorageHealth(data){
dataHealthState.last=data||null;
const summaryEl=document.getElementById('data-storage-health-summary');
const exchangesEl=document.getElementById('data-storage-health-exchanges');
const tableEl=document.getElementById('data-storage-health-table');
const notesEl=document.getElementById('data-storage-health-notes');
const summary=data?.summary||{};
const metrics=[
  ['数据集', Number(summary.dataset_count||0).toLocaleString('zh-CN')],
  ['币种数', Number(summary.symbol_count||0).toLocaleString('zh-CN')],
  ['活跃文件', Number(summary.active_files||0).toLocaleString('zh-CN')],
  ['分片文件', Number(summary.partition_files||0).toLocaleString('zh-CN')],
  ['损坏文件', Number(summary.corrupt_files||0).toLocaleString('zh-CN')],
  ['问题数据集', Number(summary.datasets_with_issues||0).toLocaleString('zh-CN')],
  ['重复目录桶', Number(summary.duplicate_symbol_buckets||0).toLocaleString('zh-CN')],
  ['备份批次', Number(summary.backup_batches||0).toLocaleString('zh-CN')],
  ['总大小', `${Number(summary.total_size_mb||0).toFixed(2)} MB`],
  ['精确扫描', Number(summary.exact_scan_count||0).toLocaleString('zh-CN')],
  ['快扫抑制', Number(summary.suppressed_gap_datasets||0).toLocaleString('zh-CN')],
];
if(summaryEl){
  summaryEl.innerHTML=metrics.map(([label,value])=>`<div class="stat-box"><div class="stat-label">${esc(label)}</div><div class="stat-value">${esc(value)}</div></div>`).join('');
}
const exchanges=Array.isArray(data?.exchanges)?data.exchanges:[];
if(exchangesEl){
  exchangesEl.innerHTML=exchanges.length?exchanges.map(row=>`<div class="list-item"><span>${esc(row.exchange)} ｜ 数据集 ${Number(row.dataset_count||0)} ｜ 币种 ${Number(row.symbol_count||0)} ｜ 问题 ${Number(row.issue_count||0)}</span><span>${esc(`${Number(row.size_mb||0).toFixed(2)} MB ｜ 最近 ${row.latest_modified_at?fmtDateTime(row.latest_modified_at):'--'}`)}</span></div>`).join(''):'<div class="list-item"><span>交易所</span><span>暂无数据</span></div>';
}
const rows=Array.isArray(data?.datasets)?data.datasets:[];
if(tableEl){
  tableEl.innerHTML=!rows.length?'<div class="list-item"><span>明细</span><span>暂无数据</span></div>':`
  <table>
    <thead>
      <tr>
        <th>交易所</th>
        <th>币种</th>
        <th>周期</th>
        <th>覆盖范围</th>
        <th>行数</th>
        <th>缺口</th>
        <th>文件</th>
        <th>来源</th>
        <th>问题</th>
        <th>最近更新</th>
        <th>操作</th>
      </tr>
    </thead>
    <tbody>
      ${rows.map(row=>{
        const issues=Array.isArray(row?.issues)?row.issues:[];
        const issueClass=Number(row?.corrupt_files||0)>0?'data-health-note-bad':(issues.length?'data-health-note-warn':'data-health-note-good');
        const gapPreview=Array.isArray(row?.gap_preview)&&row.gap_preview.length?`<div class="mini">${esc(row.gap_preview.slice(0,3).join(', '))}</div>`:'';
        const scanNote=String(row?.scan_note||'').trim();
        const action=String(row?.recommended_action||'').trim();
        const actionHtml=action
          ?`<div class="data-health-actions"><button type="button" class="btn btn-primary btn-sm" data-health-action="${esc(action)}" data-exchange="${esc(row.exchange||'')}" data-symbol="${esc(row.symbol||'')}" data-timeframe="${esc(row.timeframe||'')}" data-start="${esc(row.start||'')}" data-end="${esc(row.end||'')}">${action==='redownload'?'重拉':'补全'}</button></div>`
          :'<span class="mini">无需操作</span>';
        return `<tr>
          <td>${esc(row.exchange||'-')}</td>
          <td>${esc(row.symbol||'-')}</td>
          <td>${esc(row.timeframe||'-')}</td>
          <td>${esc(`${row.start||'--'} ~ ${row.end||'--'}`)}</td>
          <td>${esc(`${Number(row.rows||0).toLocaleString('zh-CN')} ${row.scan_mode==='fast'?'(快扫)':''}`)}${row.coverage_ratio!==null&&row.coverage_ratio!==undefined?`<div class="mini">覆盖率 ${(Number(row.coverage_ratio||0)*100).toFixed(1)}%</div>`:''}</td>
          <td class="${issueClass}">${esc(String(Number(row.gap_count||0)))}${gapPreview}${scanNote?`<div class="mini">${esc(scanNote)}</div>`:''}</td>
          <td>${esc(`${Number(row.active_files||0)} 文件 / ${Number(row.partition_files||0)} 分片 / ${Number(row.size_mb||0).toFixed(2)} MB`)}</td>
          <td>${esc(`${row.source_type||'-'}${Number(row.duplicate_dirs||0)>0?` / 重复目录 ${Number(row.duplicate_dirs||0)}`:''}`)}</td>
          <td class="${issueClass}">${esc(issues.length?issues.join('、'):'正常')}</td>
          <td>${esc(row.modified_at?fmtDateTime(row.modified_at):'--')}</td>
          <td>${actionHtml}</td>
        </tr>`;
      }).join('')}
    </tbody>
  </table>`;
}
if(notesEl){
  const duplicates=Array.isArray(data?.duplicates)?data.duplicates:[];
  const backups=Array.isArray(data?.backups?.recent)?data.backups.recent:[];
  const lines=[
    `生成时间: ${data?.generated_at?fmtDateTime(data.generated_at):'--'}`,
    `说明: 非秒级且数据量较小的数据集会做精确缺口扫描；其余使用快速估算。秒级快扫在覆盖率过低时会抑制缺口告警，避免误报。`,
    `按钮说明: 刷新体检=重扫并重新生成问题清单；批量补全问题项=对缺口或重复时间轴做本地修复；批量重拉损坏项=对损坏或明显异常的数据重新回源下载。`,
    `重复目录: ${duplicates.length?duplicates.map(item=>`${item.exchange} ${item.symbol} -> ${item.directories.join(', ')}`).join(' | '):'无'}`,
    `最近备份: ${backups.length?backups.map(item=>`${item.batch} (${Number(item.symbol_dirs||0)}个symbol目录)`).join(' | '):'无'}`,
  ];
  notesEl.textContent=lines.join('\n');
}
bindDataStorageHealthActions();
}
function getDataHealthSelection(){
const exchange=(document.getElementById('data-exchange')?.value||'binance').trim()||'binance';
const symbol=(document.getElementById('data-symbol')?.value||'BTC/USDT').trim()||'BTC/USDT';
return{exchange,symbol};
}
function analyticsStatusLabel(status){
const key=String(status||'idle').trim().toLowerCase();
return({ok:'正常',degraded:'降级',failed:'失败',running:'运行中',idle:'空闲'}[key]||status||'--');
}
function analyticsSourceLabel(sourceName){
const raw=String(sourceName||'').trim();
if(!raw)return'--';
return raw.split('+').map(part=>({
  exchange_public:'交易所公开源',
  official_announcements:'官方公告源',
  proxy_layer:'代理源',
  internal_placeholder:'占位源',
  fallback:'降级回退',
  public_chain_proxy:'公开链上代理',
}[part]||part)).join(' + ');
}
function normalizeAnalyticsStatusMap(payload){
const statusPayload=payload?.status;
if(Array.isArray(statusPayload?.collectors)){
  return Object.fromEntries(statusPayload.collectors.map(item=>[String(item?.collector||''),item]).filter(([k])=>Boolean(k)));
}
if(statusPayload&&typeof statusPayload==='object'&&!Array.isArray(statusPayload))return statusPayload;
return{};
}
function formatAnalyticsDatasetSummary(row){
if(!row)return'--';
const latest=row.latest_summary||{};
if(row.key==='microstructure'){
  const spread=Number(latest.spread_bps||0).toFixed(2);
  const large=Number(latest.large_order_count||0);
  const funding=(latest.funding_rate===null||latest.funding_rate===undefined)?'--':Number(latest.funding_rate||0).toFixed(6);
  return `点差 ${spread} bps | 大单 ${large} | 资金费率 ${funding}`;
}
if(row.key==='community'){
  return `买入占比 ${(Number(latest.buy_ratio||0)*100).toFixed(1)}% | 公告 ${Number(latest.announcement_count||0)} | 安全事件 ${Number(latest.security_alert_count||0)}`;
}
if(row.key==='whales'){
  return `巨鲸 ${Number(latest.whale_count||0)} 笔 | 合计 ${Number(latest.total_btc||0).toFixed(2)} BTC | 最大 ${Number(latest.max_btc||0).toFixed(2)} BTC`;
}
return'--';
}
function renderDataAnalyticsHealth(data){
dataAnalyticsHealthState.last=data||null;
const summaryEl=document.getElementById('data-analytics-health-summary');
const tableEl=document.getElementById('data-analytics-health-table');
const notesEl=document.getElementById('data-analytics-health-notes');
const summary=data?.summary||{};
const refreshed=data?.refreshed||null;
const statusMap=normalizeAnalyticsStatusMap(data);
const collectorList=Object.values(statusMap||{});
const latestRun=collectorList.map(item=>item?.finished_at||item?.updated_at||'').filter(Boolean).sort().slice(-1)[0]||'';
const metrics=[
  ['历史数据集',Number(summary.dataset_count||0).toLocaleString('zh-CN')],
  ['总快照数',Number(summary.total_rows||0).toLocaleString('zh-CN')],
  ['正常样本',Number(summary.ok_rows||0).toLocaleString('zh-CN')],
  ['降级样本',Number(summary.degraded_rows||0).toLocaleString('zh-CN')],
  ['失败样本',Number(summary.failed_rows||0).toLocaleString('zh-CN')],
  ['最近更新',summary.latest_at?fmtDateTime(summary.latest_at):'--'],
  ['最近采集',latestRun?fmtDateTime(latestRun):'--'],
];
if(summaryEl){
  summaryEl.innerHTML=metrics.map(([label,value])=>`<div class="stat-box"><div class="stat-label">${esc(label)}</div><div class="stat-value">${esc(value)}</div></div>`).join('');
}
const rows=Array.isArray(data?.datasets)?data.datasets:[];
if(tableEl){
  tableEl.innerHTML=!rows.length?'<div class="list-item"><span>分析历史</span><span>暂无数据</span></div>':`
  <table>
    <thead>
      <tr>
        <th>类型</th>
        <th>累计快照</th>
        <th>近${esc(String(Number(data?.hours||168)))}h</th>
        <th>正常/降级/失败</th>
        <th>覆盖跨度</th>
        <th>最近采样</th>
        <th>最近采集状态</th>
        <th>来源</th>
        <th>最新摘要</th>
        <th>最近序列</th>
      </tr>
    </thead>
    <tbody>
      ${rows.map(row=>{
        const series=Array.isArray(data?.recent?.[row.key])?data.recent[row.key]:[];
        const recentPreview=series.slice(-3).map(item=>`${fmtDateTime(item.timestamp)} ${Number(item.value||0).toFixed(row.key==='whales'?0:4)}`).join(' | ');
        const latest=row.latest_summary||{};
        const status=statusMap?.[row.key]||{};
        return `<tr>
          <td>${esc(row.title||row.key||'-')}</td>
          <td>${esc(Number(row.count||0).toLocaleString('zh-CN'))}</td>
          <td>${esc(Number(row.recent_count||0).toLocaleString('zh-CN'))}</td>
          <td>${esc(`${Number(row.ok_count||0)}/${Number(row.degraded_count||0)}/${Number(row.failed_count||0)}`)}</td>
          <td>${esc(`${Number(row.coverage_hours||0).toFixed(1)} h`)}<div class="mini">${esc(`${row.first_at?fmtDateTime(row.first_at):'--'} -> ${row.latest_at?fmtDateTime(row.latest_at):'--'}`)}</div></td>
          <td>${esc(row.latest_at?fmtDateTime(row.latest_at):'--')}</td>
          <td>${esc(analyticsStatusLabel(status.status||latest.capture_status))}<div class="mini">${esc(status.finished_at?fmtDateTime(status.finished_at):'--')}</div></td>
          <td>${esc(analyticsSourceLabel(latest.source_name||status?.details?.source_name))}<div class="mini">${esc(latest.source_error||status.error||'--')}</div></td>
          <td>${esc(formatAnalyticsDatasetSummary(row))}</td>
          <td>${esc(recentPreview||'暂无')}</td>
        </tr>`;
      }).join('')}
    </tbody>
  </table>`;
}
if(notesEl){
  const sourceLines=(Array.isArray(data?.sources)?data.sources:[]).map(item=>`${item.name}: ${item.acquisition} -> ${item.stored_as}${item?.quality_note?` (${item.quality_note})`:''}`);
  const collectorLines=collectorList.map(item=>`${analyticsStatusLabel(item?.status)} ${item?.collector||'-'}${item?.finished_at?` @ ${fmtDateTime(item.finished_at)}`:''}${item?.scope_warning?` | ${item.scope_warning}`:''}${item?.error?` | ${item.error}`:''}`);
  const lines=[
    `生成时间: ${data?.generated_at?fmtDateTime(data.generated_at):'--'}`,
    `当前标的: ${(data?.exchange||'binance')} ${data?.symbol||'BTC/USDT'}`,
    `数据库: ${data?.storage?.database||'--'}`,
    `表: ${Array.isArray(data?.storage?.tables)?data.storage.tables.join(', '):'--'}`,
    refreshed?.saved?.captured_at?`本次刷新已抓取并入库: ${fmtDateTime(refreshed.saved.captured_at)}`:'本次未执行前台抓取',
    `采集状态: ${collectorLines.length?collectorLines.join(' | '):'--'}`,
    '说明: 页面会先读取历史库，再后台补抓；单次补抓失败不会清空已有历史结果。',
    `数据来源: ${sourceLines.length?sourceLines.join(' | '):'--'}`,
  ];
  notesEl.textContent=lines.join('\n');
}
}
function withApiTiming(label,promise){
const started=performance.now();
return Promise.resolve(promise).then(value=>({ok:true,label,ms:Math.round(performance.now()-started),value})).catch(error=>({ok:false,label,ms:Math.round(performance.now()-started),error}));
}
function mergeAnalyticsStatusPayload(healthPayload,statusPayload){
const payload={...(healthPayload||{})};
if(statusPayload)payload.status=statusPayload;
return payload;
}
function appendDataHealthDiagnostics(el,diagnostics,extraLines=[]){
if(!el)return;
const parts=(Array.isArray(diagnostics)?diagnostics:[]).map(item=>`${item.ok?'OK':'ERR'} ${item.label} ${Number(item.ms||0)}ms${item.ok?'':` | ${item.error?.message||item.error}`}`);
const lines=[String(el.textContent||'').trim(),...extraLines.filter(Boolean),parts.length?`接口诊断: ${parts.join(' | ')}`:''].filter(Boolean);
el.textContent=lines.join('\n');
}
function analyticsHistoryNeedsRefresh(payload){
const latestAt=String(payload?.summary?.latest_at||'').trim();
if(!latestAt)return true;
const ts=Date.parse(latestAt);
if(!Number.isFinite(ts))return true;
return Date.now()-ts>3*60*60*1000;
}
function pickAnalyticsCollectorsForRefresh(payload,{manual=false}={}){
const rows=Array.isArray(payload?.datasets)?payload.datasets:[];
const priority=rows.filter(row=>Number(row?.count||0)<=0||Number(row?.failed_count||0)>0||Number(row?.degraded_count||0)>0).map(row=>String(row?.key||'').trim()).filter(Boolean);
if(manual){
  if(priority.length)return Array.from(new Set(priority));
  return['microstructure','community','whales'];
}
const passivePriority=Array.from(new Set(priority.filter(key=>key!=='microstructure')));
if(passivePriority.length)return passivePriority;
return['community','whales'];
}
function triggerAnalyticsHistoryRefresh({exchange,symbol,depthLimit=80,afterRefresh=true,collectors=[],timeoutMs=15000}={}){
  const analyticsNotesEl=document.getElementById('data-analytics-health-notes');
  const collectorList=Array.isArray(collectors)?collectors.filter(Boolean):[];
  const effectiveTimeoutMs=Math.max(4000,Number(timeoutMs||15000));
  const params=[
    `exchange=${encodeURIComponent(exchange||'binance')}`,
    `symbol=${encodeURIComponent(symbol||'BTC/USDT')}`,
    `depth_limit=${encodeURIComponent(String(depthLimit||80))}`,
  ];
  if(collectorList.length)params.push(`collectors=${encodeURIComponent(collectorList.join(','))}`);
  const query=params.join('&');
  const url=`/api/trading/analytics/history/collect?${query}`;
  const startedAt=Date.now();
  const controller=new AbortController();
  const timer=setTimeout(()=>controller.abort(),effectiveTimeoutMs);
  if(analyticsNotesEl){
    const current=String(analyticsNotesEl.textContent||'').trim();
    analyticsNotesEl.textContent=[current,`后台刷新已启动，正在采集 ${collectorList.length?collectorList.join(' / '):'社区 / 巨鲸 / 微观结构'} 快照...`].filter(Boolean).join('\n');
  }
  fetch(url,{
    method:'POST',
    cache:'no-store',
    keepalive:true,
    headers:{'Cache-Control':'no-cache',Pragma:'no-cache'},
    signal:controller.signal,
  }).then(async res=>{
    const payload=await res.json().catch(()=>({}));
    if(!res.ok)throw new Error(payload.detail||payload.error||`请求失败(${res.status})`);
    if(analyticsNotesEl){
      const spent=Math.max(0,Date.now()-startedAt);
      analyticsNotesEl.textContent=`后台刷新完成，最新入库时间: ${payload?.saved?.captured_at?fmtDateTime(payload.saved.captured_at):'--'} | 用时 ${(spent/1000).toFixed(1)}s`;
    }
    if(afterRefresh){
      setTimeout(()=>{loadDataStorageHealth().catch(err=>console.warn('loadDataStorageHealth failed',err?.message||err));},1200);
    }
  }).catch(err=>{
    console.warn('triggerAnalyticsHistoryRefresh failed',err?.message||err);
    if(analyticsNotesEl){
      const current=String(analyticsNotesEl.textContent||'').trim();
      const message=err?.name==='AbortError'?`后台刷新超时(${effectiveTimeoutMs}ms)`:(err.message||err);
      analyticsNotesEl.textContent=[current,`后台刷新失败: ${message}`].filter(Boolean).join('\n');
    }
  }).finally(()=>{
    clearTimeout(timer);
  });
}
async function loadDataStorageHealth(btn=null,options={}){
const prevText=btn?btn.textContent:'';
const notesEl=document.getElementById('data-storage-health-notes');
const analyticsNotesEl=document.getElementById('data-analytics-health-notes');
const skipStorage=btn?Boolean(options?.skipStorage):(options?.skipStorage!==false);
try{
  if(btn){btn.disabled=true;btn.textContent='刷新中...';}
  if(notesEl)notesEl.textContent=skipStorage?'数据仓体检较重，默认改为按需扫描。点击“刷新体检”可查看完整仓库状态。':'正在扫描数据仓，请稍候...';
  const cachedAnalytics=(dataAnalyticsHealthState.last&&typeof dataAnalyticsHealthState.last==='object')?dataAnalyticsHealthState.last:null;
  if(analyticsNotesEl)analyticsNotesEl.textContent=cachedAnalytics?'正在刷新历史体检（先显示上次快照）...':'正在读取历史体检结果...';
  const {exchange,symbol}=getDataHealthSelection();
  const diagnostics=[];
  let analyticsHealthPayload=cachedAnalytics&&cachedAnalytics.exchange===exchange&&cachedAnalytics.symbol===symbol?cachedAnalytics:null;
  let analyticsStatusPayload=null;
  const renderAnalyticsSnapshot=(extraLines=[])=>{
    if(!analyticsHealthPayload)return;
    renderDataAnalyticsHealth(mergeAnalyticsStatusPayload(analyticsHealthPayload,analyticsStatusPayload));
    appendDataHealthDiagnostics(analyticsNotesEl,diagnostics,extraLines);
  };
  if(analyticsHealthPayload)renderAnalyticsSnapshot(['已显示上次历史快照，正在后台刷新接口状态...']);
  const analyticsHealthTask=withApiTiming('trading/analytics/history/health',api(`/trading/analytics/history/health?exchange=${encodeURIComponent(exchange)}&symbol=${encodeURIComponent(symbol)}&hours=168&refresh=false&depth_limit=80`,{timeoutMs:30000}));
  const analyticsStatusTask=withApiTiming('trading/analytics/history/status',api(`/trading/analytics/history/status?exchange=${encodeURIComponent(exchange)}&symbol=${encodeURIComponent(symbol)}`,{timeoutMs:20000}));

  analyticsHealthTask.then(result=>{
    diagnostics.push(result);
    if(result.ok){
      analyticsHealthPayload=result.value||{};
      renderAnalyticsSnapshot(['页面已显示历史快照。']);
    }else if(analyticsHealthPayload){
      renderAnalyticsSnapshot([`分析历史读取失败，已保留上次快照: ${result.error?.message||result.error}`]);
    }else if(analyticsNotesEl){
      analyticsNotesEl.textContent=`分析历史读取失败: ${result.error?.message||result.error}`;
      appendDataHealthDiagnostics(analyticsNotesEl,diagnostics);
    }
  });
  analyticsStatusTask.then(result=>{
    diagnostics.push(result);
    if(result.ok){
      analyticsStatusPayload=result.value||{};
      if(analyticsHealthPayload)renderAnalyticsSnapshot();
      else appendDataHealthDiagnostics(analyticsNotesEl,diagnostics,['采集状态已更新，正在等待历史快照...']);
    }else if(analyticsHealthPayload){
      renderAnalyticsSnapshot([`采集状态读取失败: ${result.error?.message||result.error}`]);
    }else if(analyticsNotesEl){
      analyticsNotesEl.textContent=`采集状态读取失败: ${result.error?.message||result.error}`;
      appendDataHealthDiagnostics(analyticsNotesEl,diagnostics);
    }
  });

  const [analyticsHealthResult,analyticsStatusResult]=await Promise.all([analyticsHealthTask,analyticsStatusTask]);
  if(!skipStorage){
    const storageTask=withApiTiming('data/storage/health',api('/data/storage/health',{timeoutMs:45000}));
    const storageResult=await storageTask;
    diagnostics.push(storageResult);
    if(storageResult.ok){
      renderDataStorageHealth(storageResult.value);
      appendDataHealthDiagnostics(notesEl,diagnostics);
    }else if(notesEl){
      notesEl.textContent=`数据仓体检失败: ${storageResult.error?.message||storageResult.error}`;
      appendDataHealthDiagnostics(notesEl,diagnostics);
    }
  }else if(notesEl){
    appendDataHealthDiagnostics(notesEl,diagnostics,['自动加载已跳过数据仓深度扫描。']);
  }
  const failedParts=[analyticsHealthResult,analyticsStatusResult].filter(item=>!item.ok).map(item=>`${item.label}: ${item.error?.message||item.error}`);
  if(failedParts.length)notify(failedParts.join(' | '),true);
  const shouldRefresh=!!btn||(analyticsHealthResult.ok&&analyticsHistoryNeedsRefresh(analyticsHealthResult.value));
  if(shouldRefresh){
    const collectors=pickAnalyticsCollectorsForRefresh(analyticsHealthResult.ok?analyticsHealthResult.value:null,{manual:!!btn});
    triggerAnalyticsHistoryRefresh({exchange,symbol,depthLimit:80,collectors,afterRefresh:!!btn,timeoutMs:30000});
  }else if(analyticsNotesEl){
    appendDataHealthDiagnostics(analyticsNotesEl,diagnostics,['历史快照仍较新，本次未触发后台补抓。']);
  }
}catch(err){
  if(notesEl)notesEl.textContent=`数据仓体检失败: ${err.message}`;
  if(analyticsNotesEl)analyticsNotesEl.textContent=`分析历史体检失败: ${err.message}`;
  notify(`数据体检失败: ${err.message}`,true);
}finally{
  if(btn){btn.disabled=false;btn.textContent=prevText||'刷新体检';}
}
}
function bindData(){
const f=document.getElementById('data-form');
if(f)f.onsubmit=async e=>{e.preventDefault();try{await loadKlinesByForm();notify('行情加载完成（可拖动自动加载历史）');}catch(err){marketDataState.isLoading=false;notify(`行情加载失败: ${err.message}`,true);}};
const d=document.getElementById('download-form');
if(d)d.onsubmit=async e=>{
  e.preventDefault();
  const downloadOut=getDownloadOutputEl();
  try{
    const ex=String(document.getElementById('download-exchange')?.value||'binance').trim()||'binance';
    const s=String(document.getElementById('download-symbol')?.value||'BTC/USDT').trim()||'BTC/USDT';
    const tf=String(document.getElementById('download-timeframe')?.value||'1h').trim()||'1h';
    const range=getDownloadDateRange();
    const days=getDownloadRequestedDays(tf,range);
    const batchSymbols=parseDownloadBatchSymbols(document.getElementById('download-symbols-batch')?.value||'',s);
    if(downloadOut)downloadOut.textContent=`正在创建历史下载任务...\n交易所: ${ex}\n周期: ${tf}\n币种数: ${batchSymbols.length}\n时间范围: ${range.start_time||'未指定'} -> ${range.end_time||'现在'}`;
    notify(batchSymbols.length>1?'正在创建批量历史下载任务...':'正在创建历史下载任务...');
    if(batchSymbols.length<=1){
      const parts=[
        `exchange=${encodeURIComponent(ex)}`,
        `symbol=${encodeURIComponent(batchSymbols[0]||s)}`,
        `timeframe=${encodeURIComponent(tf)}`,
        `days=${encodeURIComponent(days)}`,
        'background=true',
      ];
      if(range.start_time)parts.push(`start_time=${encodeURIComponent(range.start_time)}`);
      if(range.end_time)parts.push(`end_time=${encodeURIComponent(range.end_time)}`);
      const r=await api(`/data/download?${parts.join('&')}`,{method:'POST',timeoutMs:20000});
      if(r?.task_id){
        if(downloadOut)downloadOut.textContent=`后台下载已启动\nTask: ${r.task_id}\n交易对: ${batchSymbols[0]||s}\n时间范围: ${range.start_time||'未指定'} -> ${range.end_time||'现在'}`;
        notify(`后台下载已启动: ${r.task_id}`);
        const task=await pollDownloadTask(r.task_id);
        const count=Number(task?.result?.count||0);
        if(downloadOut)downloadOut.textContent=`下载完成\n交易对: ${task?.symbol||batchSymbols[0]||s}\n周期: ${task?.timeframe||tf}\nK线数量: ${count.toLocaleString('zh-CN')}\n范围: ${task?.result?.start||range.start_time||'-'} -> ${task?.result?.end||range.end_time||'现在'}`;
        notify(`下载完成: ${count} 根K线`);
        if(document.getElementById('data-exchange')?.value===ex&&document.getElementById('data-symbol')?.value===(batchSymbols[0]||s)&&document.getElementById('data-timeframe')?.value===tf){loadKlinesByForm().catch(()=>{});}
        return;
      }
      if(downloadOut)downloadOut.textContent=`下载完成\n交易对: ${batchSymbols[0]||s}\nK线数量: ${Number(r?.count||0).toLocaleString('zh-CN')}`;
      notify(`下载完成: ${r.count||0} 根K线`);
      return;
    }
    const payload={
      exchange:ex,
      symbols:batchSymbols,
      timeframe:tf,
      days,
      start_time:range.start_time,
      end_time:range.end_time,
      background:true,
    };
    const r=await api('/data/download/batch',{method:'POST',body:JSON.stringify(payload),timeoutMs:30000});
    if(downloadOut)downloadOut.textContent=[
      `批量下载任务已创建`,
      `Batch: ${r?.batch_id||'-'}`,
      `交易所: ${ex}`,
      `周期: ${tf}`,
      `币种数: ${batchSymbols.length}`,
      `时间范围: ${range.start_time||'未指定'} -> ${range.end_time||'现在'}`,
      `Task IDs: ${(Array.isArray(r?.task_ids)?r.task_ids:[]).join(', ')}`
    ].join('\n');
    notify(`批量下载已排队: ${Number(r?.task_count||0)} 个任务`);
    const tasks=await pollBatchDownloadTasks(Array.isArray(r?.task_ids)?r.task_ids:[]);
    if(downloadOut)downloadOut.textContent=formatDownloadBatchSummary(r,tasks);
    const completed=tasks.filter(task=>String(task?.status||'')==='completed').length;
    const failed=tasks.filter(task=>String(task?.status||'')==='failed').length;
    notify(`批量下载完成: ${completed} 成功 / ${failed} 失败${failed?`，详见下载输出`:''}`,failed>0);
  }catch(err){
    if(downloadOut)downloadOut.textContent=`下载失败: ${err.message}`;
    notify(`下载失败: ${err.message}`,true);
  }
};
const fillResearchBtn=document.getElementById('btn-download-fill-research');
if(fillResearchBtn)fillResearchBtn.onclick=async()=>{
  const ex=String(document.getElementById('download-exchange')?.value||'binance').trim()||'binance';
  const textarea=document.getElementById('download-symbols-batch');
  const downloadOut=getDownloadOutputEl();
  try{
    const resp=await api(`/data/research/symbols?exchange=${encodeURIComponent(ex)}`,{timeoutMs:15000});
    const symbols=(Array.isArray(resp?.symbols)?resp.symbols:[]).slice(0,30);
    if(textarea)textarea.value=symbols.join('\n');
    if(downloadOut)downloadOut.textContent=`已填入研究币池\n交易所: ${ex}\n币种数: ${symbols.length}\nSymbols: ${symbols.join(', ')}`;
    notify(`已填入研究币池: ${symbols.length} 个币种`);
  }catch(err){
    if(downloadOut)downloadOut.textContent=`填入研究币池失败: ${err.message}`;
    notify(`填入研究币池失败: ${err.message}`,true);
  }
};
const refreshResearchBtn=document.getElementById('btn-download-refresh-research');
if(refreshResearchBtn)refreshResearchBtn.onclick=()=>triggerResearchUniverseRefresh(refreshResearchBtn);
  const clearBatchBtn=document.getElementById('btn-download-clear-batch');
if(clearBatchBtn)clearBatchBtn.onclick=()=>{
  const textarea=document.getElementById('download-symbols-batch');
  if(textarea)textarea.value='';
  const downloadOut=getDownloadOutputEl();
  if(downloadOut)downloadOut.textContent='批量币种已清空。留空时将只下载上面的单个交易对。';
};
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
          if(out)out.textContent=`本地暂无 ${s} ${tf} 数据，正在自动下载最近 ${guessDataDownloadDays(tf)} 天后再补全...`;
          const dl=await api(`/data/download?exchange=${encodeURIComponent(ex)}&symbol=${encodeURIComponent(s)}&timeframe=${encodeURIComponent(tf)}&days=${guessDataDownloadDays(tf)}&background=true`,{method:'POST',timeoutMs:20000});
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
const healthBtn=document.getElementById('btn-refresh-storage-health');
if(healthBtn)healthBtn.onclick=()=>loadDataStorageHealth(healthBtn);
const batchRepairBtn=document.getElementById('btn-batch-health-repair');
if(batchRepairBtn)batchRepairBtn.onclick=()=>runBatchDataHealthAction('repair',batchRepairBtn);
const batchRedownloadBtn=document.getElementById('btn-batch-health-redownload');
if(batchRedownloadBtn)batchRedownloadBtn.onclick=()=>runBatchDataHealthAction('redownload',batchRedownloadBtn);
const dataExchange=document.getElementById('data-exchange');
const downloadExchange=document.getElementById('download-exchange');
if(dataExchange)dataExchange.onchange=async()=>{resetKlineChartForSwitch('正在切换交易所并加载新行情...');await loadDataSymbolOptions(dataExchange.value,['data-symbol']);scheduleDataChartReload(220);};
if(downloadExchange)downloadExchange.onchange=()=>loadDataSymbolOptions(downloadExchange.value,['download-symbol']);
const dataSymbol=document.getElementById('data-symbol');
if(dataSymbol)dataSymbol.onchange=()=>{resetKlineChartForSwitch('正在切换币种并加载新行情...');scheduleDataChartReload(120);};
const dataTimeframe=document.getElementById('data-timeframe');
if(dataTimeframe)dataTimeframe.onchange=()=>{resetKlineChartForSwitch('正在切换周期并加载新行情...');scheduleDataChartReload(120);};
loadResearchUniverseRefreshStatus({silent:true}).catch(()=>{});
setInterval(()=>{if(isDataTabActive()&&!marketDataState.isLoading&&!(marketDataState.bars||[]).length){loadKlinesByForm().catch(()=>{});}},7000);
}

function renderBacktest(r){
const box=document.getElementById('backtest-results');
if(!box)return;
backtestUIState.lastRenderedBacktest={
  strategy:String(r?.strategy||'').trim(),
  symbol:String(r?.symbol||'').trim(),
  pairSymbol:String(r?.pair_symbol||'').trim(),
  timeframe:String(r?.timeframe||'').trim(),
  fromComparePreview:!!r?._from_compare_preview,
  compareRank:Number.isFinite(Number(r?._compare_rank))?Number(r._compare_rank):null,
  updatedAt:Date.now(),
};
const c=Number(r.total_return||0)>=0?'#3fb950':'#f85149';
const isPairsMode=String(r?.portfolio_mode||'').trim()==='pairs_spread_dual_leg';
box.innerHTML=`
<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:18px;">
<div class="stat-box"><div class="stat-label">策略</div><div class="stat-value">${r.strategy}</div></div>
<div class="stat-box"><div class="stat-label">交易对</div><div class="stat-value">${r.symbol}</div></div>
<div class="stat-box"><div class="stat-label">周期</div><div class="stat-value">${r.timeframe}</div></div>
<div class="stat-box"><div class="stat-label">样本数</div><div class="stat-value">${r.data_points||0}</div></div>
</div>
${isPairsMode?`<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:18px;">
<div class="stat-box"><div class="stat-label">回测模式</div><div class="stat-value">双腿Spread</div></div>
<div class="stat-box"><div class="stat-label">副腿</div><div class="stat-value">${esc(r.pair_symbol||'--')}</div></div>
<div class="stat-box"><div class="stat-label">最新对冲比</div><div class="stat-value">${Number.isFinite(Number(r.hedge_ratio_last))?Number(r.hedge_ratio_last).toFixed(4):'--'}</div></div>
<div class="stat-box"><div class="stat-label">最新Z / 价差</div><div class="stat-value">${Number.isFinite(Number(r.z_score_last))?Number(r.z_score_last).toFixed(2):'--'} / ${Number.isFinite(Number(r.spread_last))?Number(r.spread_last).toFixed(4):'--'}</div></div>
</div>`:''}
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
<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-top:12px;">
<div class="stat-box"><div class="stat-label">止盈止损回测</div><div class="stat-value">${r.use_stop_take?'已启用':'未启用'}</div></div>
<div class="stat-box"><div class="stat-label">止损/止盈比例</div><div class="stat-value">${Number.isFinite(Number(r.stop_loss_pct))?(Number(r.stop_loss_pct)*100).toFixed(1)+'%':'--'} / ${Number.isFinite(Number(r.take_profit_pct))?(Number(r.take_profit_pct)*100).toFixed(1)+'%':'--'}</div></div>
<div class="stat-box"><div class="stat-label">保护性平仓次数</div><div class="stat-value">${Number(r.forced_protective_exits||0)} 次</div></div>
<div class="stat-box"><div class="stat-label">止损/止盈触发</div><div class="stat-value">${Number(r.forced_stop_exits||0)} / ${Number(r.forced_take_exits||0)}</div></div>
</div>
${(()=>{const slPct=Number(r.stop_loss_pct),tpPct=Number(r.take_profit_pct),exits=Number(r.forced_protective_exits||0),trades=Number(r.total_trades||0);if(r.use_stop_take&&exits===0&&trades>0){return`<div class="list-item" style="margin-top:8px;padding:8px 12px;background:rgba(255,177,95,.1);border:1px solid rgba(255,177,95,.3);border-radius:8px;color:#ffb15f;font-size:12px;">提示：止盈止损已启用，但本次回测中未触发任何强制平仓（止损${Number.isFinite(slPct)?(slPct*100).toFixed(1):'--'}% / 止盈${Number.isFinite(tpPct)?(tpPct*100).toFixed(1):'--'}%）。持仓期间价格波动未达阈值，结果与未启用时相同。如需验证，可适当调大止损/止盈比例。</div>`;}return'';})()}
${renderRangeLockIndicatorHtml(r,true)}`;
const ec=document.getElementById('backtest-equity-chart');
if(ec&&r.series?.length){
if(typeof Plotly==='undefined'){ec.innerHTML='<div class="list-item">图表库未加载，回测曲线暂不可用。</div>';return;}
const rows=(r.series||[]).map(i=>({timestamp:toDate(i.timestamp),equity:+i.equity,gross_equity:+i.gross_equity,drawdown:+i.drawdown,close:+i.close,pair_close:Number(i?.pair_close),spread:Number(i?.spread),z_score:Number(i?.z_score)})).filter(i=>i.timestamp&&Number.isFinite(i.equity)&&Number.isFinite(i.gross_equity)&&Number.isFinite(i.drawdown)&&Number.isFinite(i.close));
if(!rows.length){ec.innerHTML='<div class="list-item">回测时间序列为空或时间格式异常。</div>';return;}
const normalizeTradeDirection=value=>{const raw=String(value||'').trim().toLowerCase();if(raw.startsWith('long'))return'long';if(raw.startsWith('short'))return'short';return'';};
const tradeDirectionText=direction=>direction==='long'?'Long':direction==='short'?'Short':'--';
const tradeDirectionColor=direction=>direction==='long'?'#3fb950':direction==='short'?'#f85149':'#9fb1c9';
const toTradeRows=list=>(list||[]).map(p=>{const direction=normalizeTradeDirection(p?.direction);const reason=String(p?.reason||'').trim();const label=[tradeDirectionText(direction),reason].filter(Boolean).join(' · ')||String(p?.direction||p?.reason||'').trim();return{timestamp:toDate(p?.timestamp),price:Number(p?.price),direction,label};}).filter(p=>p.timestamp&&Number.isFinite(p.price));
const x=rows.map(i=>i.timestamp),e=rows.map(i=>i.equity),ge=rows.map(i=>i.gross_equity),dd=rows.map(i=>i.drawdown),cl=rows.map(i=>i.close);
const tp=r.trade_points||{},buyRows=toTradeRows(tp.buy_points),sellRows=toTradeRows(tp.sell_points),openRows=toTradeRows(tp.open_points),closeRows=toTradeRows(tp.close_points);
const traces=[{type:'scatter',mode:'lines',x,y:e,name:'净值曲线',line:{color:'#3fb950',width:2},yaxis:'y'},{type:'scatter',mode:'lines',x,y:ge,name:'毛净值曲线',line:{color:'#4da3ff',width:1},yaxis:'y'},{type:'scatter',mode:'lines',x,y:dd,name:'回撤(%)',line:{color:'#f85149',width:1},yaxis:'y2'},{type:'scatter',mode:'lines',x,y:cl,name:isPairsMode?'主腿价格':'价格',line:{color:'#9fb1c9',width:1,dash:'dot'},yaxis:'y3',hovertemplate:`%{x|%Y-%m-%d %H:%M:%S}<br>${isPairsMode?'主腿':'价格'}: %{y:.6f}<extra></extra>`}];
const pushDirectionalTradeTrace=(items,phase)=>{const isOpen=phase==='open';const phaseLabel=isOpen?'开仓':'平仓';const symbol=isOpen?'circle':'x';let rendered=0;['long','short'].forEach(direction=>{const matches=items.filter(i=>i.direction===direction);if(!matches.length)return;rendered+=matches.length;const directionLabel=tradeDirectionText(direction);traces.push({type:'scatter',mode:'markers',x:matches.map(i=>i.timestamp),y:matches.map(i=>i.price),name:`${directionLabel} ${phaseLabel}`,marker:{symbol,size:9,color:tradeDirectionColor(direction),line:{color:'#0e1b2a',width:1}},text:matches.map(i=>i.label||directionLabel),yaxis:'y3',hovertemplate:`%{x|%Y-%m-%d %H:%M:%S}<br>${phaseLabel}(${directionLabel}): %{y:.6f}<br>%{text}<extra></extra>`});});if(rendered===0&&items.length)traces.push({type:'scatter',mode:'markers',x:items.map(i=>i.timestamp),y:items.map(i=>i.price),name:`${phaseLabel}点`,marker:{symbol,size:9,color:isOpen?'#3fb950':'#f85149',line:{color:'#0e1b2a',width:1}},text:items.map(i=>i.label||phase),yaxis:'y3',hovertemplate:`%{x|%Y-%m-%d %H:%M:%S}<br>${phaseLabel}: %{y:.6f}<br>%{text}<extra></extra>`});};
if(isPairsMode&&rows.some(i=>Number.isFinite(i.pair_close)))traces.push({type:'scatter',mode:'lines',x,y:rows.map(i=>i.pair_close),name:'副腿价格',line:{color:'#ffb15f',width:1,dash:'dash'},yaxis:'y3',hovertemplate:'%{x|%Y-%m-%d %H:%M:%S}<br>副腿: %{y:.6f}<extra></extra>'});
if(openRows.length||closeRows.length){
pushDirectionalTradeTrace(openRows,'open');
pushDirectionalTradeTrace(closeRows,'close');
}else{
if(buyRows.length)traces.push({type:'scatter',mode:'markers',x:buyRows.map(i=>i.timestamp),y:buyRows.map(i=>i.price),name:'买点',marker:{symbol:'triangle-up',size:9,color:'#3fb950',line:{color:'#0e1b2a',width:1}},yaxis:'y3',hovertemplate:'%{x|%Y-%m-%d %H:%M:%S}<br>买入: %{y:.6f}<extra></extra>'});
if(sellRows.length)traces.push({type:'scatter',mode:'markers',x:sellRows.map(i=>i.timestamp),y:sellRows.map(i=>i.price),name:'卖点',marker:{symbol:'triangle-down',size:9,color:'#f85149',line:{color:'#0e1b2a',width:1}},yaxis:'y3',hovertemplate:'%{x|%Y-%m-%d %H:%M:%S}<br>卖出: %{y:.6f}<extra></extra>'});
}
Plotly.newPlot(ec,traces,{paper_bgcolor:'#111723',plot_bgcolor:'#111723',font:{color:'#d7dde8'},margin:{l:50,r:72,t:20,b:30},xaxis:plotlyTimeAxis({}),yaxis:{title:'权益',side:'left',showgrid:true,gridcolor:'#283242'},yaxis2:{title:'回撤%',overlaying:'y',side:'right',showgrid:false},yaxis3:{title:isPairsMode?'主/副腿价格':'价格',overlaying:'y',side:'right',position:0.9,showgrid:false,tickfont:{color:'#9fb1c9'},titlefont:{color:'#9fb1c9'}},legend:{orientation:'h'}},{responsive:true,displaylogo:false});
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
if(['趋势'].includes(c))return'趋势类';
if(['震荡'].includes(c))return'震荡类';
if(['动量'].includes(c))return'动量类';
if(['均值回归'].includes(c))return'均值回归类';
if(['突破'].includes(c))return'突破类';
if(['成交量'].includes(c))return'成交量类';
if(['波动率'].includes(c))return'波动率类';
if(['风险'].includes(c))return'风险类';
if(['统计套利'].includes(c))return'统计套利类';
if(['微观结构'].includes(c))return'微观结构类';
if(['套利'].includes(c))return'套利类';
if(['宏观'].includes(c))return'宏观类';
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
await ensureBacktestStrategySelect();
const catalog=backtestCompareCatalog();
let items=[];
if(src==='registered'){
  const counts=await getRegisteredStrategyTypesForCompare();
  const skipped=[];
  items=Object.keys(counts).map(type=>{
    const base=catalog.byValue?.[type];
    if(!base){skipped.push(type);return null;}
    const groupLabel=base.groupLabel || mapStrategyCatToBacktestGroup(getStrategyMeta(type).cat);
    const label=(base?.label)||type;
    return {value:type,label,groupLabel,registeredCount:counts[type]};
  }).filter(it=>it&&Boolean(it.value));
  backtestUIState.compareRegisteredSkipped=skipped;
  items.sort((a,b)=>{
    const ai=Number(catalog.orderIndex?.[a.value]??9999),bi=Number(catalog.orderIndex?.[b.value]??9999);
    return ai-bi || String(a.value).localeCompare(String(b.value),'zh-CN');
  });
}else{
  let available=(Array.isArray(state.strategyCatalogRows)&&state.strategyCatalogRows.length)
    ?state.strategyCatalogRows.filter(row=>row?.backtest_supported).map(row=>String(row?.name||'').trim()).filter(Boolean)
    :(Array.isArray(state.availableStrategyTypes)?state.availableStrategyTypes:[]);
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
  const sourceList=(Array.isArray(state.strategyCatalogRows)&&state.strategyCatalogRows.length)
    ?state.strategyCatalogRows.filter(row=>row?.backtest_supported).map(row=>String(row?.name||'').trim()).filter(Boolean)
    :((libraryRows&&libraryRows.length)?libraryRows.map(r=>String(r?.name||'').trim()).filter(Boolean):((available&&available.length)?available:(catalog.items||[]).map(it=>it.value)));
  items=sourceList.map(type=>{
    const base=catalog.byValue?.[type];
    const libMeta=(state.strategyCatalogRows||[]).find(r=>String(r?.name||'')===type)||(libraryRows||[]).find(r=>String(r?.name||'')===type)||{};
    const meta=getStrategyMeta(type);
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
function getBacktestProtectionConfig(){
const enabled=!!document.getElementById('backtest-use-stop-take')?.checked;
const slRaw=Number(document.getElementById('backtest-stop-loss-pct')?.value||0);
const tpRaw=Number(document.getElementById('backtest-take-profit-pct')?.value||0);
const stopLossPct=(Number.isFinite(slRaw)&&slRaw>0&&slRaw<1)?slRaw:null;
const takeProfitPct=(Number.isFinite(tpRaw)&&tpRaw>0&&tpRaw<1)?tpRaw:null;
return{enabled,stopLossPct,takeProfitPct};
}
function appendBacktestProtectionParams(url,cfg=null){
const conf=cfg||getBacktestProtectionConfig();
let out=String(url||'');
out+=`&use_stop_take=${conf.enabled?'true':'false'}`;
if(conf.enabled){
  if(Number.isFinite(conf.stopLossPct))out+=`&stop_loss_pct=${encodeURIComponent(conf.stopLossPct)}`;
  if(Number.isFinite(conf.takeProfitPct))out+=`&take_profit_pct=${encodeURIComponent(conf.takeProfitPct)}`;
}
return out;
}
function bindBacktestProtectionControls(){
const toggle=document.getElementById('backtest-use-stop-take');
const sl=document.getElementById('backtest-stop-loss-pct');
const tp=document.getElementById('backtest-take-profit-pct');
const apply=()=>{
  const on=!!toggle?.checked;
  if(sl)sl.disabled=!on;
  if(tp)tp.disabled=!on;
};
if(toggle)toggle.addEventListener('change',apply);
apply();
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
const compareOpt=(data&&typeof data.compare_optimization==='object'&&data.compare_optimization)?data.compare_optimization:{};
const compareOptSummary=data?.pre_optimize
  ? String(compareOpt?.summary||`已预优化 ${optimizedCount}/${okRows.length} 个策略（目标: ${esc(data?.optimize_objective||'total_return')}, trials=${Number(data?.optimize_max_trials||0)})`)
  : (bestBalanced?`建议下一步用 ${bestBalanced.strategy} 做参数优化`: '暂无可推荐策略');
backtestUIState.lastCompare={...(data||{}), ranked:[...ranked]};
out.innerHTML=`
<div class="list-item"><span>多策略对比（${esc(data.symbol||'-')} / ${esc(data.timeframe||'-')}）</span><span>成功 ${okRows.length} / 总计 ${rows.length}</span></div>
${renderRangeLockIndicatorHtml(data,false)}
<div class="backtest-subgrid">
  <div class="stat-box"><div class="stat-label">最佳收益策略</div><div class="stat-value">${esc(best?.strategy||'-')}</div><div class="stat-label">${best?`${btPct(best.total_return)} / 夏普 ${btNum(best.sharpe_ratio)}`:'--'}</div></div>
  <div class="stat-box"><div class="stat-label">均衡推荐（收益-回撤）</div><div class="stat-value">${esc(bestBalanced?.strategy||'-')}</div><div class="stat-label">${bestBalanced?`${btPct(bestBalanced.total_return)} / 回撤 ${btPct(bestBalanced.max_drawdown)}`:'--'}</div></div>
  <div class="stat-box"><div class="stat-label">平均收益 / 平均夏普</div><div class="stat-value">${btPct(avgRet)} / ${btNum(avgSharpe)}</div><div class="stat-label">成本: 手续费 ${(Number(data?.commission_rate||0)*100).toFixed(4)}% + 滑点 ${btNum(data?.slippage_bps||0)}bps</div></div>
  <div class="stat-box"><div class="stat-label">结论建议</div><div class="stat-value">${best&&best.total_return>0?'优先回测前3名细化参数':'先降低周期/成本或换策略组'}</div><div class="stat-label">${esc(compareOptSummary)}</div></div>
</div>
<div class="inline-actions" style="margin-top:10px;">
  <button type="button" class="btn btn-primary btn-sm" id="btn-backtest-register-best">注册收益第一策略（新实例）</button>
  <button type="button" class="btn btn-primary btn-sm" id="btn-backtest-register-top3">注册前3策略（新实例）</button>
  <span style="font-size:12px;color:#9fb1c9;">新实例选项：资金占比 ${regCfg.allocation.toFixed(2)}${regCfg.autoStart?' | 自动启动':''}${regCfg.suffix?` | 后缀 ${regCfg.suffix}`:''}</span>
</div>
<div class="section-title">策略排行榜（按收益率排序，点击行可在上方预览该策略区间回测）</div>
<div class="backtest-table-wrap">
<table class="data-table">
<thead><tr><th>排名</th><th>策略</th><th>参数来源</th><th>收益率</th><th>夏普</th><th>回撤</th><th>胜率</th><th>交易数</th><th>建议样本</th><th>零交易诊断</th><th>成本拖累</th><th>质量</th><th>操作</th></tr></thead>
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
<td>${btMetricCell(r.recommended_min_bars,'int')}</td>
<td style="max-width:260px;white-space:normal;word-break:break-word;color:${Number(r.total_trades||0)===0?'#f2c96d':'#9fb1c9'};">${esc(r.zero_trade_reason||'--')}</td>
<td>${btPct(r.cost_drag_return_pct)}</td>
<td>${esc(r.quality_flag||'-')}</td>
<td>
  <div class="inline-actions" style="gap:6px;">
    <button type="button" class="btn btn-primary btn-sm" onclick="event.stopPropagation();previewCompareStrategyByRank(${i})">预览</button>
    <button type="button" class="btn btn-primary btn-sm" onclick="event.stopPropagation();registerCompareStrategyByRank(${i})">注册</button>
  </div>
</td>
</tr>`).join('') || '<tr><td colspan="13">无成功结果</td></tr>'}
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
const strategyName=String(data?.strategy_type||data?.strategy||'').trim();
const strategyMeta=getStrategyMeta(strategyName);
const newsCount=Math.max(0,Number(data?.news_events_count||0));
const fundingAvailable=!!data?.funding_available;
const optimizeViewMeta=((meta)=>{
  const family=String(meta?.family||'traditional');
  const decision=String(meta?.decisionEngine||'rule');
  const resolvedDataMode=String(data?.data_mode||'').trim();
  if(family==='ml'){
    return {
      familyLabel:'ML驱动',
      familyColor:'#ff6b35',
      familyBg:'rgba(255,107,53,.14)',
      decisionLabel:'ML / 模型决策',
      dataMode:resolvedDataMode||'OHLCV only',
      dataHint:newsCount>0||fundingAvailable?'当前模型回测已带增强上下文':'当前模型回测以 OHLCV 为主',
    };
  }
  if(family==='ai_glm'||family==='ai_openai'||decision==='glm'||decision==='openai'){
    return {
      familyLabel:'OpenAI/AI驱动',
      familyColor:'#38bdf8',
      familyBg:'rgba(56,189,248,.14)',
      decisionLabel:'OpenAI / AI事件决策',
      dataMode:resolvedDataMode||'OHLCV only',
      dataHint:newsCount>0||fundingAvailable?'本次优化已接入历史新闻/宏观增强':'当前区间未命中可用新闻/宏观增强',
    };
  }
  return {
    familyLabel:'传统规则',
    familyColor:'#94a3b8',
    familyBg:'rgba(148,163,184,.14)',
    decisionLabel:'规则 / 指标决策',
    dataMode:resolvedDataMode||'OHLCV',
    dataHint:'价格与成交量主线回测',
  };
})(strategyMeta);
const familyBadge=`<span style="display:inline-flex;align-items:center;padding:2px 8px;border-radius:999px;background:${optimizeViewMeta.familyBg};border:1px solid ${optimizeViewMeta.familyColor}44;color:${optimizeViewMeta.familyColor};font-size:12px;font-weight:700;">${esc(optimizeViewMeta.familyLabel)}</span>`;
const dataBadge=`<span style="display:inline-flex;align-items:center;padding:2px 8px;border-radius:999px;background:#1d2b3d;border:1px solid #32475f;color:#9fb1c9;font-size:12px;font-weight:700;">${esc(optimizeViewMeta.dataMode)}</span>`;
const newsBadge=`<span style="display:inline-flex;align-items:center;padding:2px 8px;border-radius:999px;background:#162535;border:1px solid #35506d;color:#9fc3ea;font-size:12px;font-weight:700;">News ${newsCount}</span>`;
out.innerHTML=`
<div class="list-item"><span>参数优化（${esc(data?.strategy||'-')} / ${esc(data?.symbol||'-')} / ${esc(data?.timeframe||'-')}） ${familyBadge} ${dataBadge} ${newsBadge}</span><span>试验 ${Number(data?.trials||top.length||0)} 次</span></div>
${renderRangeLockIndicatorHtml(data,false)}
<div class="list-item"><span>回测区间 / 样本数</span><span>${esc(String(data?.requested_start_date||data?.start_date||'-'))} ~ ${esc(String(data?.requested_end_date||data?.end_date||'-'))} | ${Number(data?.data_points||0)} 根</span></div>
<div class="backtest-subgrid">
  <div class="stat-box"><div class="stat-label">优化目标</div><div class="stat-value">${esc(objectiveLabel)}</div><div class="stat-label">手续费 ${(Number(data?.commission_rate||0)*100).toFixed(4)}% | 滑点 ${btNum(data?.slippage_bps||0)}bps</div></div>
  <div class="stat-box"><div class="stat-label">最佳得分</div><div class="stat-value">${btNum(best?.score||0)}</div><div class="stat-label">${best?`收益 ${btPct(best.metrics?.total_return)} / 回撤 ${btPct(best.metrics?.max_drawdown)} / 夏普 ${btNum(best.metrics?.sharpe_ratio)}`:'--'}</div></div>
  <div class="stat-box"><div class="stat-label">推荐参数</div><div class="stat-value">${best&&best.params?Object.keys(best.params).length:0} 项</div><div class="stat-label">${best&&best.params?esc(Object.entries(best.params).map(([k,v])=>`${k}=${v}`).join(', ')):'--'}</div></div>
  <div class="stat-box"><div class="stat-label">决策引擎</div><div class="stat-value">${esc(optimizeViewMeta.familyLabel)}</div><div class="stat-label">${esc(optimizeViewMeta.decisionLabel)} | ${esc(optimizeViewMeta.dataMode)}</div></div>
  <div class="stat-box"><div class="stat-label">增强数据</div><div class="stat-value">News ${newsCount}</div><div class="stat-label">${fundingAvailable?'Funding/Macro 已启用':'Funding/Macro 未命中'}</div></div>
  <div class="stat-box"><div class="stat-label">建议</div><div class="stat-value">${best&&Number(best.metrics?.max_drawdown||0)<20?'可做滚动验证':'先降低风险参数/换周期'}</div><div class="stat-label">${esc(optimizeViewMeta.dataHint)}</div></div>
</div>
<div class="inline-actions" style="margin-top:10px;">
  <button type="button" class="btn btn-primary btn-sm" id="btn-apply-opt-best">一键回填最佳参数到策略参数编辑</button>
  <button type="button" class="btn btn-primary btn-sm" id="btn-register-opt-best" onclick="registerOptimizeBestAsNewStrategyInstance()">按最佳参数注册新实例</button>
  <span style="font-size:12px;color:#9fb1c9;">回填仅填前端编辑面板；注册选项：${regCfg.allocation.toFixed(2)}${regCfg.autoStart?' / 自动启动':''}${regCfg.suffix?` / ${regCfg.suffix}`:''}</span>
</div>
<div class="section-title">Top 参数组合</div>
<div class="backtest-table-wrap">
<table class="data-table">
<thead><tr><th>排名</th><th>引擎</th><th>数据层</th><th>得分(${esc(objectiveLabel)})</th><th>收益率</th><th>夏普</th><th>回撤</th><th>胜率</th><th>交易数</th><th>交易点</th><th>零交易原因</th><th>参数</th><th>操作</th></tr></thead>
<tbody>
${top.map((t,i)=>{
const entrySignals=Number(t?.metrics?.entry_signals||0);
const exitSignals=Number(t?.metrics?.exit_signals||0);
const tradePoints=entrySignals+exitSignals;
const zeroTradeReason=String(t?.metrics?.zero_trade_reason||'').trim();
const tradePointText=tradePoints>0?`${tradePoints} (${entrySignals}/${exitSignals})`:'0';
const zeroTradeHtml=zeroTradeReason
  ? `<span title="${esc(zeroTradeReason)}" style="color:#f0b429;">${esc(zeroTradeReason.length>20?`${zeroTradeReason.slice(0,20)}...`:zeroTradeReason)}</span>`
  : '<span style="color:#6b7fa0;">--</span>';
return `<tr class="bt-optimize-row ${Number(backtestUIState?.lastOptimizePreviewRank??-1)===i?'active-preview':''}" data-rank-index="${i}" onclick="previewOptimizeTrialByRank(${i})" style="cursor:pointer;">
<td>${i+1}</td>
<td><span style="display:inline-flex;align-items:center;padding:2px 8px;border-radius:999px;background:${optimizeViewMeta.familyBg};border:1px solid ${optimizeViewMeta.familyColor}44;color:${optimizeViewMeta.familyColor};font-size:11px;font-weight:700;">${esc(optimizeViewMeta.familyLabel)}</span></td>
<td><span style="display:inline-flex;align-items:center;padding:2px 8px;border-radius:999px;background:#1d2b3d;border:1px solid #32475f;color:#9fb1c9;font-size:11px;font-weight:700;">${esc(optimizeViewMeta.dataMode)}</span></td>
<td>${btNum(t.score)}</td>
<td class="${Number(t?.metrics?.total_return||0)>=0?'positive':'negative'}">${btPct(t?.metrics?.total_return)}</td>
<td>${btNum(t?.metrics?.sharpe_ratio)}</td>
<td>${btPct(t?.metrics?.max_drawdown)}</td>
<td>${btPct(t?.metrics?.win_rate)}</td>
<td>${btMetricCell(t?.metrics?.total_trades,'int')}</td>
<td title="入场/出场 = ${entrySignals}/${exitSignals}">${esc(tradePointText)}</td>
<td>${zeroTradeHtml}</td>
<td>${esc(Object.entries(t.params||{}).map(([k,v])=>`${k}=${v}`).join(', '))}</td>
<td><div class="inline-actions" style="justify-content:flex-end;gap:6px;flex-wrap:wrap;"><button type="button" class="btn btn-primary btn-sm" onclick="event.stopPropagation();previewOptimizeTrialByRank(${i})">预览</button><button type="button" class="btn btn-primary btn-sm" onclick="event.stopPropagation();registerOptimizeTrialByRank(${i}, this)">注册</button></div></td>
</tr>`;}).join('') || '<tr><td colspan="13">无优化结果</td></tr>'}
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
const symbolsRaw=Array.isArray(spec?.symbols)?spec.symbols.map(v=>String(v||'').trim()).filter(Boolean):[];
const symbol=String(spec?.symbol||symbolsRaw[0]||document.getElementById('backtest-symbol')?.value||'BTC/USDT').trim()||'BTC/USDT';
const symbols=symbolsRaw.length?symbolsRaw:[symbol];
const timeframe=String(spec?.timeframe||document.getElementById('backtest-timeframe')?.value||'1h').trim()||'1h';
const params=(spec?.params&&typeof spec.params==='object')?spec.params:{};
const defaults=getBacktestRegisterOptions();
const allocation=Math.max(0,Math.min(1,Number(spec?.allocation ?? defaults.allocation)));
const autoStart=(spec?.auto_start!==undefined)?!!spec.auto_start:!!defaults.autoStart;
const nameSuffix=String(spec?.name_suffix ?? defaults.suffix ?? '');
const exchange=String(spec?.exchange||'binance').toLowerCase();
const name=String(spec?.name||'').trim()||buildBacktestRegisteredName(strategyType,symbol,timeframe,nameSuffix);
const payload={name,strategy_type:strategyType,params,symbols,timeframe,exchange,allocation};
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
async function registerOptimizeTrialByRank(rankIndex,btn=null){
const opt=backtestUIState?.lastOptimize||{};
const top=Array.isArray(opt?.top)?opt.top:[];
const row=top[Number(rankIndex)||0];
if(!row){notify('未找到该参数组合',true);return null;}
const prevText=btn?String(btn.textContent||'注册'):'';
if(btn){btn.disabled=true;btn.textContent='注册中...';}
try{
return await registerStrategyInstanceFromBacktestSpec({
  strategy_type: String(opt.strategy_type||opt.strategy||'').trim(),
  symbol: opt.symbol||document.getElementById('backtest-symbol')?.value||'BTC/USDT',
  timeframe: opt.timeframe||document.getElementById('backtest-timeframe')?.value||'1h',
  params: (row.params&&typeof row.params==='object')?row.params:{},
  exchange: String(opt.exchange||'binance').toLowerCase(),
});
}catch(e){notify(`注册参数组合失败: ${e.message}`,true);}
finally{
  if(btn){btn.disabled=false;btn.textContent=prevText||'注册';}
}
return null;
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
async function previewOptimizeTrialByRank(rankIndex){
try{
  backtestUIState.lastOptimizePreviewRank=Number(rankIndex)||0;
  document.querySelectorAll('.bt-optimize-row').forEach(row=>{
    row.classList.toggle('active-preview', Number(row.getAttribute('data-rank-index')||-1)===backtestUIState.lastOptimizePreviewRank);
    if(Number(row.getAttribute('data-rank-index')||-1)===backtestUIState.lastOptimizePreviewRank){
      row.style.background='rgba(77,163,255,.10)';
      row.style.outline='1px solid rgba(77,163,255,.25)';
    }else{
      row.style.background='';
      row.style.outline='';
    }
  });
  const opt=backtestUIState?.lastOptimize||{};
  const top=Array.isArray(opt.top)?opt.top:[];
  const row=top[Number(rankIndex)||0];
  if(!row){notify('未找到该参数组合',true);return;}
  const st=String(opt.strategy_type||opt.strategy||'').trim();
  const meta=getStrategyMeta(st);
  const familyLabel=meta.family==='ml'?'ML驱动':((meta.family==='ai_glm'||meta.family==='ai_openai')?'OpenAI/AI驱动':'传统规则');
  const symbol=String(opt.symbol||document.getElementById('backtest-symbol')?.value||'BTC/USDT');
  const tf=String(opt.timeframe||document.getElementById('backtest-timeframe')?.value||'1h');
  const capital=Number(document.getElementById('backtest-capital')?.value||opt.initial_capital||10000);
  const sdInput=String(document.getElementById('backtest-start-date')?.value||'').trim();
  const edInput=String(document.getElementById('backtest-end-date')?.value||'').trim();
  const sd=sdInput||String(opt.requested_start_date||opt.start_date||'').trim();
  const ed=edInput||String(opt.requested_end_date||opt.end_date||'').trim();
  const cr=Number(opt.commission_rate ?? 0.0004);
  const sb=Number(opt.slippage_bps ?? 2);
  const params=(row.params&&typeof row.params==='object')?row.params:{};
  let u=`/backtest/run_custom?strategy=${encodeURIComponent(st)}&symbol=${encodeURIComponent(symbol)}&timeframe=${encodeURIComponent(tf)}&initial_capital=${encodeURIComponent(capital)}&commission_rate=${encodeURIComponent(cr)}&slippage_bps=${encodeURIComponent(sb)}&include_series=true`;
  if(sd)u+=`&start_date=${encodeURIComponent(sd)}`;
  if(ed)u+=`&end_date=${encodeURIComponent(ed)}`;
  if(Object.keys(params).length)u+=`&params_json=${encodeURIComponent(JSON.stringify(params))}`;
  u=appendBacktestProtectionParams(u);
  notify(`正在预览参数组合 #${Number(rankIndex)+1}`);
  const r=await api(u,{method:'POST',timeoutMs:90000});
  r._from_optimize_preview=true;
  r._optimize_rank=Number(rankIndex)+1;
  r._optimize_params=params;
  renderBacktest(r);
  const box=document.getElementById('backtest-results');
  if(box){
    const hint=document.createElement('div');
    hint.className='list-item';
    hint.style.marginTop='8px';
    const dataMode=String(r?.data_mode||opt?.data_mode|| (meta.aiDriven?'OHLCV only':'OHLCV'));
    const newsCount=Math.max(0,Number(r?.news_events_count ?? opt?.news_events_count ?? 0));
    const fundingText=(r?.funding_available ?? opt?.funding_available) ? 'Macro On' : 'Macro Off';
    const entrySignals=Number(r?.entry_signals ?? row?.metrics?.entry_signals ?? 0);
    const exitSignals=Number(r?.exit_signals ?? row?.metrics?.exit_signals ?? 0);
    const zeroTradeReason=String(r?.zero_trade_reason ?? row?.metrics?.zero_trade_reason ?? '').trim();
    const tradePointText=`${entrySignals+exitSignals} (${entrySignals}/${exitSignals})`;
    hint.innerHTML=`<span>参数优化预览来源</span><span>#${Number(rankIndex)+1} ${esc(st)} | ${esc(familyLabel)} | ${esc(dataMode)} | News ${newsCount} | ${esc(fundingText)} | 交易点 ${esc(tradePointText)}${zeroTradeReason?` | ${esc(zeroTradeReason)}`:''} | ${esc(Object.entries(params).map(([k,v])=>`${k}=${v}`).join(', ')||'默认参数')}</span>`;
    box.appendChild(hint);
  }
  notify(`已在上方展示参数组合 #${Number(rankIndex)+1} 的回测图`);
}catch(e){notify(`参数组合预览失败: ${e.message}`,true);}
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
  u=appendBacktestProtectionParams(u);
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
async function loadNotificationCenter(){return runRequestSingleFlight('notificationCenter',async()=>{const out=document.getElementById('notify-output');if(!out)return;try{const [ch,rules,events]=await Promise.all([api('/notifications/channels'),api('/notifications/rules'),api('/notifications/events?limit=20')]);const list=rules.rules||[];state.notifyRules=Object.fromEntries(list.map(x=>[x.id,x]));renderNotifyRules(list);out.textContent=JSON.stringify({channels:ch.channels||{},rules:list.slice(-20),recent_events:(events.events||[]).slice(-20)},null,2);}catch(e){out.textContent=`加载通知中心失败: ${e.message}`;}});}
async function sendTestNotification(channel){const msg=(document.getElementById('notify-test-msg')?.value||'系统测试通知').trim();const out=document.getElementById('notify-output');try{const r=await api('/notifications/test',{method:'POST',body:JSON.stringify({title:'交易系统测试通知',message:msg,channels:[channel]})});if(out)out.textContent=JSON.stringify(r,null,2);notify(`${channel} 测试通知已发送`);await loadNotificationCenter();}catch(e){if(out)out.textContent=`测试通知失败: ${e.message}`;notify(`测试通知失败: ${e.message}`,true);}}
async function createNotifyRule(){const out=document.getElementById('notify-output');try{const payload=buildNotifyRulePayload();const r=await api('/notifications/rules',{method:'POST',body:JSON.stringify(payload)});if(out)out.textContent=JSON.stringify(r,null,2);notify('通知规则创建成功');await loadNotificationCenter();}catch(e){if(out)out.textContent=`创建规则失败: ${e.message}`;notify(`创建规则失败: ${e.message}`,true);}}
async function runNotifyRules(){const out=document.getElementById('notify-output');try{const r=await api('/notifications/evaluate',{method:'POST',body:JSON.stringify({exchange:'gate',symbols:['BTC/USDT','ETH/USDT','SOL/USDT']})});if(out)out.textContent=JSON.stringify(r,null,2);notify(`规则评估完成，触发 ${r?.result?.triggered_count||0} 条`);await Promise.all([loadNotificationCenter(),loadAuditLogs()]);}catch(e){if(out)out.textContent=`规则评估失败: ${e.message}`;notify(`规则评估失败: ${e.message}`,true);}}
async function editNotifyRule(id){const rule=state.notifyRules[id];if(!rule){notify('规则不存在',true);return;}const out=document.getElementById('notify-output');try{const name=prompt('规则名称',rule.name);if(name===null)return;const updates={name:name.trim()||rule.name};const rt=rule.rule_type,p=rule.params||{};if(rt==='price_above'||rt==='price_below'){const symbol=prompt('交易对',String(p.symbol||'BTC/USDT'));if(symbol===null)return;const threshold=prompt('阈值',String(p.threshold??0));if(threshold===null)return;updates.params={...p,symbol:symbol.trim()||'BTC/USDT',threshold:Number(threshold||0)};}if(rt==='daily_pnl_below_pct'){const v=prompt('阈值(%)',String(p.threshold_pct??-2));if(v===null)return;updates.params={...p,threshold_pct:Number(v||-2)};}if(rt==='position_count_above'){const v=prompt('持仓阈值',String(p.threshold??1));if(v===null)return;updates.params={...p,threshold:Math.max(1,parseInt(v,10)||1)};}if(rt==='exchange_disconnected'){const v=prompt('交易所列表(逗号分隔)',(p.exchanges||[]).join(','));if(v===null)return;updates.params={...p,exchanges:v.split(',').map(x=>x.trim().toLowerCase()).filter(Boolean)};}if(rt==='stale_strategy_count_above'||rt==='running_strategy_count_below'){const v=prompt('阈值',String(p.threshold??1));if(v===null)return;updates.params={...p,threshold:Math.max(1,parseInt(v,10)||1)};}if(rt==='strategy_not_running'){const v=prompt('策略名称列表(逗号分隔)',(p.strategies||[]).join(','));if(v===null)return;updates.params={...p,strategies:v.split(',').map(x=>x.trim()).filter(Boolean)};}const r=await api(`/notifications/rules/${encodeURIComponent(id)}`,{method:'PUT',body:JSON.stringify(updates)});if(out)out.textContent=JSON.stringify(r,null,2);notify('规则已更新');await loadNotificationCenter();}catch(e){if(out)out.textContent=`更新规则失败: ${e.message}`;notify(`更新规则失败: ${e.message}`,true);}}
async function toggleNotifyRule(id){const rule=state.notifyRules[id];if(!rule){notify('规则不存在',true);return;}const out=document.getElementById('notify-output');try{const r=await api(`/notifications/rules/${encodeURIComponent(id)}`,{method:'PUT',body:JSON.stringify({enabled:!rule.enabled})});if(out)out.textContent=JSON.stringify(r,null,2);notify(`规则已${rule.enabled?'停用':'启用'}`);await loadNotificationCenter();}catch(e){if(out)out.textContent=`切换规则失败: ${e.message}`;notify(`切换规则失败: ${e.message}`,true);}}
async function deleteNotifyRule(id){if(!confirm('确认删除该规则吗？'))return;const out=document.getElementById('notify-output');try{const r=await api(`/notifications/rules/${encodeURIComponent(id)}`,{method:'DELETE'});if(out)out.textContent=JSON.stringify(r,null,2);notify('规则已删除');await loadNotificationCenter();}catch(e){if(out)out.textContent=`删除规则失败: ${e.message}`;notify(`删除规则失败: ${e.message}`,true);}}
function bindNotificationCenter(){const f=document.getElementById('btn-test-feishu'),b1=document.getElementById('btn-test-telegram'),b2=document.getElementById('btn-test-email'),b3=document.getElementById('btn-create-rule'),b4=document.getElementById('btn-run-rules'),b5=document.getElementById('btn-refresh-heatmap');if(f)f.onclick=()=>sendTestNotification('feishu');if(b1)b1.onclick=()=>sendTestNotification('telegram');if(b2)b2.onclick=()=>sendTestNotification('email');if(b3)b3.onclick=createNotifyRule;if(b4)b4.onclick=runNotifyRules;if(b5)b5.onclick=loadPnlHeatmap;}

function renderAuditLogs(logs){const box=document.getElementById('audit-log-list');if(!box)return;if(!logs?.length){box.innerHTML='<div class="list-item">暂无审计日志</div>';return;}box.innerHTML=logs.slice(0,100).map(i=>`<div class="list-item"><span>${esc(i.timestamp||'').replace('T',' ').substring(0,19)} | ${esc(i.module)}/${esc(i.action)} | ${esc(i.status)}</span><span>${esc((i.message||'-').substring(0,72))}</span></div>`).join('');}
async function loadAuditLogs(){return runRequestSingleFlight('auditLogs',async()=>{try{const d=await api('/trading/audit?hours=168&limit=100',{timeoutMs:12000});renderAuditLogs(d.logs||[]);}catch(e){const box=document.getElementById('audit-log-list');if(box)box.innerHTML=`<div class="list-item">审计日志加载失败: ${esc(e.message)}</div>`;}});}
function bindAudit(){const b=document.getElementById('btn-refresh-audit');if(b)b.onclick=loadAuditLogs;}

let wsClient=null,wsRetryTimer=null,softRefreshTimer=null,replaySessionId='',lastTickRenderAt=0;
let aiResearchRefreshPromise=null;
function refreshAiResearchModules(){
const ai=window.AI||{};
const modules=ai.modules||{};
const activeTab=getActiveTabName();
if(activeTab==='ai-research'&&typeof ai.refreshWorkbench==='function'&&!aiResearchRefreshPromise){
  const task=Promise.resolve(ai.refreshWorkbench())
    .catch(err=>console.warn('refreshAiResearchModules failed:',err?.message||err))
    .finally(()=>{if(aiResearchRefreshPromise===task)aiResearchRefreshPromise=null;});
  aiResearchRefreshPromise=task;
}
try{modules.agent?.refresh?.({includeDetails:activeTab==='ai-agent'});}catch{}
try{modules.runtime?.render?.();}catch{}
return aiResearchRefreshPromise||Promise.resolve();
}
function softRefresh(delay=250){
if(softRefreshTimer)clearTimeout(softRefreshTimer);
softRefreshTimer=setTimeout(()=>{
  const tab=getActiveTabName();
  const group=sharedPollGroupForTab(tab);
  if(group&&!canRunSharedPolling(group))return;
  if(tab==='dashboard')Promise.allSettled([loadSummary(),loadPositions(),loadOrders(),loadOpenOrders(),loadStrategies(),loadStrategySummary(),loadRisk()]);
  else if(tab==='trading')Promise.allSettled([loadSummary(),loadPositions(),loadOrders(),loadOpenOrders(),loadConditionalOrders(),loadAccounts(),loadModeInfo(),loadRisk(),loadLiveTradeReview({showLoading:false,minIntervalMs:15000})]);
  else if(tab==='strategies')Promise.allSettled([loadStrategies(),loadStrategySummary()]);
  else if(tab==='ai-research')refreshAiResearchModules();
  else if(tab==='ai-agent')refreshAiResearchModules();
},delay);
}
function setWsBadge(connected){state.wsConnected=!!connected;const st=document.getElementById('system-status');if(st)st.textContent=connected?'运行中(WS在线)':'运行中(轮询)';}
function applyMarketTick(payload){try{const ex=marketDataState.exchange||document.getElementById('data-exchange')?.value,sym=marketDataState.symbol||document.getElementById('data-symbol')?.value,tf=marketDataState.timeframe||document.getElementById('data-timeframe')?.value||'1m';if(!ex||!sym||!marketDataState.bars?.length)return;const t=payload?.[ex]?.[sym];if(!t)return;const px=Number(t.last||0);if(px<=0)return;const tfSec=timeframeSeconds(tf);const nowMs=Date.now();const bucketMs=Math.floor(nowMs/(tfSec*1000))*(tfSec*1000);const bars=marketDataState.bars;const last=bars[bars.length-1];const lastMs=klineToMs(last?.timestamp);if(!Number.isFinite(lastMs))return;const lastBucket=Math.floor(lastMs/(tfSec*1000))*(tfSec*1000);if(lastBucket===bucketMs){last.high=Math.max(Number(last.high||px),px);last.low=Math.min(Number(last.low||px),px);if(!Number.isFinite(last.low))last.low=px;if(!Number.isFinite(last.high))last.high=px;last.close=px;}else if(bucketMs>lastBucket){if(isSubMinuteTf(tf)){return;}const openPx=Number(last.close||px);bars.push({timestamp:klineLocalIso(bucketMs),open:openPx,high:Math.max(openPx,px),low:Math.min(openPx,px),close:px,volume:0});marketDataState.bars=cropBars(mergeBars([],bars));}const renderThrottle=isSubMinuteTf(tf)?900:450;const now=Date.now();if(now-lastTickRenderAt>=renderThrottle){lastTickRenderAt=now;renderKlineChart(true);}}catch(e){console.error(e);}}
function closeWebSocketClient(){
if(wsRetryTimer){clearTimeout(wsRetryTimer);wsRetryTimer=null;}
if(!wsClient){setWsBadge(false);return;}
try{
  wsClient.onopen=null;
  wsClient.onmessage=null;
  wsClient.onclose=null;
  wsClient.onerror=null;
  wsClient.close();
}catch{}
wsClient=null;
setWsBadge(false);
}
function initWebSocket(){
try{
if(document.hidden){closeWebSocketClient();return;}
if(wsClient&&(wsClient.readyState===WebSocket.OPEN||wsClient.readyState===WebSocket.CONNECTING))return;
if(wsClient)closeWebSocketClient();
const proto=location.protocol==='https:'?'wss':'ws';
const socket=new WebSocket(`${proto}://${location.host}/ws`);
wsClient=socket;
socket.onopen=()=>{if(wsClient===socket)setWsBadge(true);};
socket.onmessage=e=>{try{const m=JSON.parse(e.data||'{}');const ev=m.event||'';if(['order_event','position_event','execution_event','mode_changed','runtime_snapshot','strategy_signal'].includes(ev)){softRefresh(120);}if(ev==='mode_changed'){notify(`交易模式已切换: ${m?.payload?.mode||'-'}`);}if(ev==='order_event'){const o=m?.payload?.order||{};notify(`订单更新: ${o.symbol||''} ${mapOrderStatus(o.status||'')}`);}if(ev==='strategy_signal'){pushRealtimeSignal(m?.payload||{});}if(ev==='market_tick'){applyMarketTick(m?.payload||{});} }catch{}};
socket.onclose=()=>{if(wsClient===socket)wsClient=null;setWsBadge(false);if(document.hidden)return;if(wsRetryTimer)clearTimeout(wsRetryTimer);wsRetryTimer=setTimeout(()=>initWebSocket(),2000);};
socket.onerror=()=>{setWsBadge(false);};
}catch{setWsBadge(false);}
}

async function loadConditionalOrders(){return runRequestSingleFlight('conditionalOrders',async()=>{try{const d=await api('/trading/orders/conditional');const t=document.getElementById('conditional-orders-tbody');if(!t)return;const rows=d.orders||[];if(!rows.length){t.innerHTML='<tr><td colspan=\"7\">暂无条件单</td></tr>';return;}t.innerHTML=rows.map(o=>`<tr><td>${o.conditional_id}</td><td>${o.exchange} ${o.symbol}</td><td>${mapSide(o.side)}</td><td>${Number(o.trigger_price||0).toFixed(4)}</td><td>${Number(o.amount||0)}</td><td>${o.account_id||'main'}</td><td><button class=\"btn btn-danger btn-sm\" onclick=\"cancelConditional('${o.conditional_id}')\">取消</button></td></tr>`).join('');}catch(e){console.error(e);}});}
async function cancelConditional(id){try{await api(`/trading/orders/conditional/${encodeURIComponent(id)}`,{method:'DELETE'});notify('条件单已取消');await loadConditionalOrders();}catch(e){notify(`取消条件单失败: ${e.message}`,true);}}

async function loadAccounts(){return runRequestSingleFlight('accounts',async()=>{try{const d=await api('/trading/accounts/summary');const out=document.getElementById('accounts-output');if(out)out.textContent=JSON.stringify(d,null,2);}catch(e){const out=document.getElementById('accounts-output');if(out)out.textContent=`账户加载失败: ${e.message}`;}});}
async function createAccount(){try{const payload={account_id:document.getElementById('account-id').value.trim(),name:document.getElementById('account-name').value.trim(),exchange:document.getElementById('account-exchange').value,mode:document.getElementById('account-mode').value,parent_account_id:null,enabled:true,metadata:{}};const r=await api('/trading/accounts',{method:'POST',body:JSON.stringify(payload)});notify(`账户 ${r?.account?.account_id||payload.account_id} 已创建`);await loadAccounts();}catch(e){notify(`创建账户失败: ${e.message}`,true);}}

async function loadModeInfo(){return runRequestSingleFlight('modeInfo',async()=>{try{const d=await api('/trading/mode');const cur=document.getElementById('mode-current-text'),pend=document.getElementById('mode-pending-text');if(cur)cur.textContent=d.mode||'-';if(pend)pend.textContent=(d.pending_switches||[]).length?`待确认 ${d.pending_switches[0].target_mode}`:'无待确认切换';if(d.pending_switches?.length)state.modeToken=d.pending_switches[0].token;const out=document.getElementById('mode-output');if(out)out.textContent=JSON.stringify(d,null,2);}catch(e){const out=document.getElementById('mode-output');if(out)out.textContent=`加载模式失败: ${e.message}`;}});}
async function requestModeSwitch(){try{const payload={target_mode:document.getElementById('mode-target').value,reason:document.getElementById('mode-reason').value||''};const r=await api('/trading/mode/request',{method:'POST',body:JSON.stringify(payload)});state.modeToken=r.token||'';const out=document.getElementById('mode-output');if(out)out.textContent=JSON.stringify(r,null,2);notify('模式切换申请已创建，请二次确认');await loadModeInfo();}catch(e){notify(`申请切换失败: ${e.message}`,true);}}
async function confirmModeSwitch(){try{if(!state.modeToken){await loadModeInfo();}if(!state.modeToken){notify('没有待确认切换令牌',true);return;}const text=prompt('请输入确认文本：CONFIRM LIVE TRADING','');if(text===null)return;const r=await api('/trading/mode/confirm',{method:'POST',body:JSON.stringify({token:state.modeToken,confirm_text:text})});const out=document.getElementById('mode-output');if(out)out.textContent=JSON.stringify(r,null,2);notify(`交易模式已切换为 ${r.mode}`);state.modeToken='';await loadModeInfo();await loadSystemStatus();}catch(e){notify(`确认切换失败: ${e.message}`,true);}}

function bindModeControls(){const b1=document.getElementById('btn-mode-request'),b2=document.getElementById('btn-mode-confirm');if(b1)b1.onclick=requestModeSwitch;if(b2)b2.onclick=confirmModeSwitch;}
function bindAccountControls(){const b1=document.getElementById('btn-account-create'),b2=document.getElementById('btn-account-refresh'),b3=document.getElementById('btn-refresh-conditional');if(b1)b1.onclick=createAccount;if(b2)b2.onclick=loadAccounts;if(b3)b3.onclick=loadConditionalOrders;}

async function loadStrategyLibrary(){
const out=document.getElementById('strategy-library-output');
try{
const d=await api('/strategies/library',{timeoutMs:18000});
state.strategyLibraryRows=Array.isArray(d?.library)?d.library:[];
if(out)out.textContent=JSON.stringify(d,null,2);
}catch(e){
try{
const d=await api('/strategies/runtime',{timeoutMs:12000});
if(out)out.textContent=JSON.stringify({fallback:'runtime',note:'策略库接口异常，已降级展示运行面板',data:d},null,2);
}catch(e2){
if(out)out.textContent=`策略库加载失败: ${e.message||e2.message}`;
}
}
}
function bindStrategyAdvanced(){const exp=document.getElementById('btn-strategy-export-all'),imp=document.getElementById('btn-strategy-import-json'),rk=document.getElementById('btn-strategy-ranking'),lib=document.getElementById('btn-strategy-library'),out=document.getElementById('strategy-health-output');if(exp)exp.onclick=async()=>{try{const d=await api('/strategies/export');if(out)out.textContent=JSON.stringify(d,null,2);notify('策略JSON已导出到面板');}catch(e){notify(`导出失败: ${e.message}`,true);}};if(imp)imp.onclick=async()=>{try{const raw=document.getElementById('strategy-import-json').value.trim();if(!raw){notify('请先粘贴JSON',true);return;}const payload=JSON.parse(raw);const d=await api('/strategies/import',{method:'POST',body:JSON.stringify(payload)});if(out)out.textContent=JSON.stringify(d,null,2);notify('策略导入完成');await Promise.all([loadStrategies(),loadStrategySummary()]);}catch(e){notify(`导入失败: ${e.message}`,true);}};if(rk)rk.onclick=async()=>{try{const s=document.getElementById('backtest-symbol')?.value||'BTC/USDT',tf=document.getElementById('backtest-timeframe')?.value||'1h';const d=await api(`/strategies/ranking?symbol=${encodeURIComponent(s)}&timeframe=${tf}&initial_capital=10000&top_n=20`);if(out)out.textContent=JSON.stringify(d,null,2);notify('策略评分完成');}catch(e){notify(`评分失败: ${e.message}`,true);}};if(lib)lib.onclick=loadStrategyLibrary;}

function getResearchOutputEl(){return document.getElementById('research-output')||document.getElementById('analytics-output')||document.getElementById('factor-output');}
function getResearchSummaryEl(){return document.getElementById('research-quick-summary');}
function getResearchExchange(){return document.getElementById('research-exchange')?.value||document.getElementById('data-exchange')?.value||'binance';}
function getResearchSymbol(){return (document.getElementById('research-symbol')?.value||document.getElementById('data-symbol')?.value||'BTC/USDT').trim()||'BTC/USDT';}
function getResearchTimeframe(){return document.getElementById('research-timeframe')?.value||'1h';}
function getResearchLookback(){return Math.max(120,Number(document.getElementById('research-lookback')?.value||1000));}
function getResearchExcludeRetired(){return (document.getElementById('research-exclude-retired')?.checked)!==false;}
function getResearchSymbols(){const raw=getSelectValues('research-symbols');return raw.length?raw:['BTC/USDT','ETH/USDT'];}
function timeframeToMinutes(tf){
const raw=String(tf||'1h').trim();
const m=raw.match(/^(\d+)([smhdwM])$/i);
if(!m)return 60;
const n=Math.max(1,Number(m[1]||1));
const unit=String(m[2]||'h').toLowerCase();
if(unit==='s')return n/60;
if(unit==='m')return n;
if(unit==='h')return n*60;
if(unit==='d')return n*1440;
if(unit==='w')return n*10080;
if(unit==='m'&&String(m[2])==='M')return n*43200;
return 60;
}
function estimateResearchWindowHours(){
  const hours=(getResearchLookback()*timeframeToMinutes(getResearchTimeframe()))/60;
  return Math.max(1,Math.min(24*180,Math.round(hours)));
}
function estimateResearchWindowDays(){
  return Math.max(1,Math.min(365,Math.ceil(estimateResearchWindowHours()/24)));
}
function symbolBaseAsset(sym){
  return String(sym||'').trim().toUpperCase().split(':')[0].split('/')[0]||'BTC';
}
function buildResearchTargetAllocations(limit=3){
  const picks=getResearchSymbols().slice(0,Math.max(1,limit)).map(symbolBaseAsset).filter(Boolean);
  if(!picks.length)return 'BTC:0.4,ETH:0.3,USDT:0.3';
  const cashWeight=0.2;
  const each=((1-cashWeight)/picks.length);
  const out=picks.map(asset=>`${asset}:${each.toFixed(2)}`);
  out.push(`USDT:${cashWeight.toFixed(2)}`);
  return out.join(',');
}
function renderResearchStatusCards(){
if(window.workbenchState?.initialized&&typeof window.renderResearchStatusCards==='function'&&window.renderResearchStatusCards!==renderResearchStatusCards)return window.renderResearchStatusCards();
const configEl=document.getElementById('research-config-snapshot');
const dataEl=document.getElementById('research-data-snapshot');
const moduleEl=document.getElementById('research-module-snapshot');
const nextEl=document.getElementById('research-next-step');
if(!configEl||!dataEl||!moduleEl||!nextEl)return;
const symbols=getResearchSymbols();
const configText=`研究配置：${getResearchExchange()} / ${getResearchSymbol()} / ${getResearchTimeframe()} | 观察窗口约 ${estimateResearchWindowDays()} 天 | 多币种 ${symbols.length} 个`;
const factorData=(researchState.lastFactorLibrary&&!researchState.lastFactorLibrary.error)?researchState.lastFactorLibrary:null;
const multiData=(researchState.lastMultiAsset&&!researchState.lastMultiAsset.error)?researchState.lastMultiAsset:null;
const sentiment=researchState.lastSentiment&& !researchState.lastSentiment.error ? researchState.lastSentiment : null;
const analytics=(researchState.lastAnalytics&&!researchState.lastAnalytics.error)?researchState.lastAnalytics:null;
const onchain=(researchState.lastOnchain&&!researchState.lastOnchain.error)?researchState.lastOnchain:null;
const fama=(researchState.lastFama&&!researchState.lastFama.error)?researchState.lastFama:null;
configEl.textContent=configText;
const coverageParts=[
  factorData?`因子 ${(factorData.factors||[]).length} / 样本 ${Number(factorData.points||0)}`:'因子未加载',
  fama?`Fama ${Number(fama.points||0)} 点`:'Fama 未加载',
  multiData?`多币 ${Number(multiData.count||0)}`:'多币未加载',
  sentiment?`新闻 ${Number(sentiment.news_events||0)}`:'情绪未加载',
  onchain?`巨鲸 ${Number(onchain?.whale_activity?.count||0)}`:'链上未加载',
];
dataEl.textContent=`数据覆盖：${coverageParts.join(' | ')}`;
const moduleStates=[
  analytics?'分析总览':'分析待加载',
  factorData?'因子库':'因子待加载',
  fama?'Fama':'Fama待加载',
  multiData?'多币种':'多币待加载',
  sentiment?'情绪':'情绪待加载',
  onchain?'链上':'链上待加载',
];
moduleEl.textContent=`模块状态：${moduleStates.join(' / ')}`;
let nextAction='下一步：先运行研究总览';
if(analytics && !sentiment)nextAction='下一步：补跑市场情绪仪表盘，确认新闻/资金费率方向';
else if(analytics && !factorData)nextAction='下一步：刷新因子库，补齐横截面打分';
else if(analytics && factorData && !multiData)nextAction='下一步：运行多币种概览，确认广度与相关性';
else if(analytics && factorData && multiData && !onchain)nextAction='下一步：补跑链上概览，确认巨鲸与链上资金扰动';
else if(analytics && factorData && multiData && sentiment)nextAction='下一步：查看研究结论，选择顺势/回归方向再执行';
nextEl.textContent=nextAction;
}

function cloneJsonValue(value){
try{return JSON.parse(JSON.stringify(value??{}));}catch{return {};}
}

function symbolQuoteAsset(sym){
const text=String(sym||'').trim().toUpperCase();
if(!text)return'USDT';
const main=text.split(':')[0];
if(main.includes('/'))return main.split('/')[1]||'USDT';
return'USDT';
}

function getArbitrageSelectedStrategy(){
return String(document.getElementById('arbitrage-strategy')?.value||arbitrageState.selectedStrategy||'PairsTradingStrategy').trim()||'PairsTradingStrategy';
}

function getArbitrageExchange(){return String(document.getElementById('arbitrage-exchange')?.value||'binance').trim().toLowerCase()||'binance';}
function getArbitrageTimeframe(){return String(document.getElementById('arbitrage-timeframe')?.value||'1h').trim()||'1h';}
function getArbitragePrimarySymbol(){return String(document.getElementById('arbitrage-primary-symbol')?.value||'BTC/USDT').trim()||'BTC/USDT';}
function getArbitrageSecondarySymbol(){
const current=String(document.getElementById('arbitrage-secondary-symbol')?.value||'').trim();
if(current&&current!==getArbitragePrimarySymbol())return current;
return getArbitrageUniverse().find(sym=>sym!==getArbitragePrimarySymbol())||'ETH/USDT';
}
function getArbitrageLookback(){return Math.max(120,Math.min(5000,Number(document.getElementById('arbitrage-lookback')?.value||720)||720));}
function getArbitrageAllocation(){return Math.max(0,Math.min(1,Number(document.getElementById('arbitrage-allocation')?.value||DEFAULT_STRATEGY_ALLOCATION)||DEFAULT_STRATEGY_ALLOCATION));}
function getArbitrageSuffix(){return normalizeInstanceSuffixText(document.getElementById('arbitrage-suffix')?.value||'');}
function getArbitrageAutoStart(){return !!document.getElementById('arbitrage-auto-start')?.checked;}
function getArbitrageUniverse(){
const raw=getSelectValues('arbitrage-universe');
const out=Array.from(new Set(raw.map(v=>String(v||'').trim()).filter(Boolean)));
if(out.length)return out;
return ['BTC/USDT','ETH/USDT'];
}
function getArbitrageVenues(){
const raw=getSelectValues('arbitrage-venues').map(v=>String(v||'').trim().toLowerCase()).filter(Boolean);
return raw.length?raw:['binance','okx','gate'];
}
function getArbitrageCatalogRows(){
const map=strategyCatalogMap();
const rows=ARBITRAGE_STRATEGY_ORDER.map(name=>map[name]).filter(Boolean);
arbitrageState.catalog=rows;
return rows;
}
function getArbitrageOutputEl(){return document.getElementById('arbitrage-run-output');}
function getArbitragePairRankingKey(){return `${getArbitrageExchange()}|${getArbitrageTimeframe()}`;}
function mapArbitragePairBias(bias){
const key=String(bias||'').trim();
return({
  long_spread_bias:'主腿偏弱',
  short_spread_bias:'主腿偏强',
  balanced:'接近平衡',
  watch:'观察中',
  unknown:'未知',
}[key]||'观察中');
}
function mapArbitragePairRelationship(regime){
const key=String(regime||'').trim();
return({
  positive_corr:'正相关',
  negative_corr:'负相关',
  unknown:'未知',
}[key]||'未知');
}
function resetArbitragePairRanking(note='等待筛选：确认周期后点击“一键筛选前十”'){
arbitrageState.pairRanking=null;
arbitrageState.pairRankingKey='';
arbitrageState.pairRankingNote=String(note||'等待筛选：确认周期后点击“一键筛选前十”').trim()||'等待筛选：确认周期后点击“一键筛选前十”';
renderArbitragePairRanking();
}
function renderArbitragePairRanking(){
const summaryEl=document.getElementById('arbitrage-pair-scan-summary');
const bodyEl=document.getElementById('arbitrage-pair-ranking-body');
const applyTopBtn=document.getElementById('btn-arbitrage-apply-top-pair');
const result=arbitrageState.pairRanking;
if(applyTopBtn)applyTopBtn.disabled=!(Array.isArray(result?.pairs)&&result.pairs.length);
if(!summaryEl||!bodyEl)return;
if(!result||!Array.isArray(result.pairs)||!result.pairs.length){
  const note=String(arbitrageState.pairRankingNote||'等待筛选：确认周期后点击“一键筛选前十”').trim()||'等待筛选：确认周期后点击“一键筛选前十”';
  summaryEl.innerHTML=[
    `<div class="list-item"><span>状态</span><span>${esc(note)}</span></div>`,
    `<div class="list-item"><span>扫描范围</span><span>交易所 ${esc(getArbitrageExchange())} / 周期 ${esc(getArbitrageTimeframe())}</span></div>`,
    '<div class="list-item"><span>提示</span><span>榜单用于挑选配对组合，真实开仓仍由 PairsTradingStrategy 的 z-score 穿越触发。</span></div>',
  ].join('');
  bodyEl.innerHTML='<tr><td colspan="8" class="arbitrage-pair-empty">等待筛选结果...</td></tr>';
  return;
}
const warnings=Array.isArray(result?.warnings)?result.warnings.filter(Boolean):[];
const topRow=result.pairs[0]||null;
  summaryEl.innerHTML=[
    `<div class="list-item"><span>扫描窗口</span><span>${esc(result.exchange||getArbitrageExchange())} / ${esc(result.timeframe||getArbitrageTimeframe())} / lookback ${Number(result.lookback_period||0)}</span></div>`,
    `<div class="list-item"><span>覆盖范围</span><span>候选 ${Number(result.candidate_symbol_count||0)} 个 | 成功加载 ${Number(result.loaded_symbol_count||0)} 个 | 可配对 ${Number(result.eligible_pair_count||0)} 组</span></div>`,
    `<div class="list-item"><span>榜首组合</span><span>${topRow?`${esc(topRow.primary_symbol)} vs ${esc(topRow.pair_symbol)} | ${esc(mapArbitragePairRelationship(topRow.correlation_regime))} | 分数 ${Number(topRow.score||0).toFixed(2)} | ${esc(mapArbitragePairBias(topRow.signal_bias))}`:'暂无'}</span></div>`,
    `<div class="list-item"><span>提示</span><span>${esc(warnings[0]||'当前已支持正相关与负相关 pair；榜单用于挑选组合，真实开仓仍要满足策略自己的入场阈值与穿越条件。')}</span></div>`,
  ].join('');
  bodyEl.innerHTML=result.pairs.map((row,idx)=>{
    const corrText=`L ${Number(row.level_corr||0).toFixed(2)} / R ${Number(row.return_corr||0).toFixed(2)}`;
    const zText=Number(row.current_z_score||0).toFixed(2);
    const halfLife=row.half_life_bars===null||row.half_life_bars===undefined?'--':`${Number(row.half_life_bars).toFixed(1)} bars`;
    return `<tr>
      <td>${idx+1}</td>
    <td><div class="arbitrage-pair-name">${esc(row.primary_symbol||'-')}</div><div class="arbitrage-pair-sub">${esc(row.pair_symbol||'-')} · ${esc(mapArbitragePairRelationship(row.correlation_regime))}</div></td>
    <td>${Number(row.score||0).toFixed(2)}</td>
    <td>${esc(corrText)}</td>
    <td>${esc(zText)}</td>
    <td>${esc(halfLife)}</td>
    <td><span class="arbitrage-chip" data-tone="${Math.abs(Number(row.current_z_score||0))>=2?'ok':'warn'}">${esc(mapArbitragePairBias(row.signal_bias))}</span></td>
    <td><button type="button" class="btn btn-primary btn-sm" data-arbitrage-pair-idx="${idx}">回填</button></td>
  </tr>`;
}).join('');
}
async function applyArbitragePairCandidate(index=0){
const result=arbitrageState.pairRanking;
const rows=Array.isArray(result?.pairs)?result.pairs:[];
const row=rows[Math.max(0,Number(index||0))];
if(!row)throw new Error('当前没有可回填的配对组合');
const primary=String(row.primary_symbol||'').trim();
const secondary=String(row.pair_symbol||'').trim();
if(!primary||!secondary)throw new Error('配对组合数据不完整');
const strategyEl=document.getElementById('arbitrage-strategy');
const tfEl=document.getElementById('arbitrage-timeframe');
const lookbackEl=document.getElementById('arbitrage-lookback');
if(strategyEl)strategyEl.value='PairsTradingStrategy';
arbitrageState.selectedStrategy='PairsTradingStrategy';
if(tfEl&&[...tfEl.options].some(opt=>String(opt.value||'').trim()===String(result?.timeframe||'')))tfEl.value=String(result?.timeframe||'1h').trim()||'1h';
if(lookbackEl)lookbackEl.value=String(Number(row.lookback_period||result?.lookback_period||720)||720);
await loadArbitrageSymbolOptions(getArbitrageExchange());
['arbitrage-primary-symbol','arbitrage-secondary-symbol','arbitrage-universe'].forEach(id=>{
  [primary,secondary,...(Array.isArray(result?.top_symbols)?result.top_symbols:[])].filter(Boolean).forEach(sym=>ensureSelectOption(id,sym));
});
setSelectValues('arbitrage-primary-symbol',[primary],primary);
setSelectValues('arbitrage-secondary-symbol',[secondary],secondary);
const universe=Array.from(new Set([primary,secondary,...(Array.isArray(result?.top_symbols)?result.top_symbols:[])].filter(Boolean))).slice(0,8);
setSelectValues('arbitrage-universe',universe,primary);
renderArbitragePanel();
const out=getArbitrageOutputEl();
if(out)out.textContent=`已从配对扫描榜回填: ${primary} / ${secondary}\n周期: ${getArbitrageTimeframe()}\nlookback: ${getArbitrageLookback()}\n说明: 榜单用于筛 pair，实际开仓仍以策略实时 z-score 穿越为准。`;
notify(`已回填配对组合: ${primary} / ${secondary}`);
}
async function scanArbitragePairsRanking(){
const exchange=getArbitrageExchange();
const timeframe=getArbitrageTimeframe();
const btn=document.getElementById('btn-arbitrage-scan-pairs');
const prevText=btn?.textContent||'一键筛选前十';
if(btn){
  btn.disabled=true;
  btn.textContent='筛选中...';
}
arbitrageState.pairRankingNote=`正在扫描 ${exchange} / ${timeframe} 的候选币种...`;
renderArbitragePairRanking();
try{
  const resp=await api(`/data/research/pairs-ranking?exchange=${encodeURIComponent(exchange)}&timeframe=${encodeURIComponent(timeframe)}&limit=10`,{timeoutMs:45000});
  arbitrageState.pairRanking=resp||null;
  arbitrageState.pairRankingKey=getArbitragePairRankingKey();
  arbitrageState.pairRankingNote=Array.isArray(resp?.pairs)&&resp.pairs.length?'':'当前周期暂无合格配对';
  renderArbitragePairRanking();
  const out=getArbitrageOutputEl();
  if(out){
    const top=Array.isArray(resp?.pairs)&&resp.pairs.length?resp.pairs[0]:null;
    out.textContent=top
      ? `配对扫描完成\n榜首: ${top.primary_symbol} / ${top.pair_symbol} (${mapArbitragePairRelationship(top.correlation_regime)})\n分数: ${Number(top.score||0).toFixed(2)} | 当前Z: ${Number(top.current_z_score||0).toFixed(2)}\n说明: 可点击“回填榜首组合”直接带入 PairsTradingStrategy。`
      : `配对扫描完成\n当前周期 ${timeframe} 暂无满足条件的 pair。\n建议: 补足该周期本地K线，或切换到更高一级周期再试。`;
  }
  return resp;
}catch(e){
  arbitrageState.pairRanking=null;
  arbitrageState.pairRankingKey='';
  arbitrageState.pairRankingNote=`筛选失败: ${e.message}`;
  renderArbitragePairRanking();
  throw e;
}finally{
  if(btn){
    btn.disabled=false;
    btn.textContent=prevText;
  }
}
}

async function loadArbitrageSymbolOptions(exchange){
const renderArbitrageSelects=symbols=>{
  const normalized=[];
  const seen=new Set();
  (Array.isArray(symbols)?symbols:[]).forEach(sym=>{
    const text=String(sym||'').trim();
    if(!text||seen.has(text))return;
    seen.add(text);
    normalized.push(text);
  });
  const finalSymbols=normalized.length?normalized:[...RESEARCH_DEFAULT_SYMBOLS];
  const primaryEl=document.getElementById('arbitrage-primary-symbol');
  const secondaryEl=document.getElementById('arbitrage-secondary-symbol');
  const universeEl=document.getElementById('arbitrage-universe');
  const currentPrimary=getArbitragePrimarySymbol();
  const currentSecondary=String(secondaryEl?.value||'').trim();
  const currentUniverse=getArbitrageUniverse();
  if(primaryEl){
    primaryEl.innerHTML=finalSymbols.map(sym=>`<option value="${esc(sym)}"${sym===currentPrimary?' selected':''}>${esc(sym)}</option>`).join('');
    primaryEl.value=finalSymbols.includes(currentPrimary)?currentPrimary:(finalSymbols.includes('BTC/USDT')?'BTC/USDT':finalSymbols[0]);
  }
  const primaryValue=String(primaryEl?.value||currentPrimary||'BTC/USDT').trim()||'BTC/USDT';
  if(secondaryEl){
    const secondaryFallback=finalSymbols.find(sym=>sym!==primaryValue)||(finalSymbols[1]||finalSymbols[0]||'ETH/USDT');
    secondaryEl.innerHTML=finalSymbols.map(sym=>`<option value="${esc(sym)}"${sym===currentSecondary?' selected':''}>${esc(sym)}</option>`).join('');
    secondaryEl.value=(currentSecondary&&currentSecondary!==primaryValue&&finalSymbols.includes(currentSecondary))?currentSecondary:secondaryFallback;
  }
  if(universeEl){
    const chosen=currentUniverse.length?new Set(currentUniverse):new Set(finalSymbols.slice(0,Math.min(6,finalSymbols.length)));
    universeEl.innerHTML=finalSymbols.map(sym=>`<option value="${esc(sym)}"${chosen.has(sym)?' selected':''}>${esc(sym)}</option>`).join('');
    if(!Array.from(universeEl.selectedOptions||[]).length&&universeEl.options.length){
      setSelectValues('arbitrage-universe', finalSymbols.slice(0,Math.min(6,finalSymbols.length)), finalSymbols[0]);
    }
  }
};
renderArbitrageSelects(RESEARCH_DEFAULT_SYMBOLS);
try{
  const ex=String(exchange||getArbitrageExchange()||'binance').trim().toLowerCase()||'binance';
  const resp=await api(`/data/research/symbols?exchange=${encodeURIComponent(ex)}`,{timeoutMs:15000});
  const symbols=(Array.isArray(resp?.symbols)?resp.symbols:[]).filter(Boolean);
  if(symbols.length)renderArbitrageSelects(symbols);
}catch(e){console.warn('loadArbitrageSymbolOptions failed',e?.message||e);}
}

function buildArbitrageTemplate(strategyType){
const row=(strategyCatalogMap()||{})[String(strategyType||'').trim()]||{};
const recommended=Array.isArray(row?.recommended_symbols)&&row.recommended_symbols.length?row.recommended_symbols:[...RESEARCH_DEFAULT_SYMBOLS.slice(0,6)];
const secondaryFallback=recommended.find(sym=>sym!==recommended[0])||'ETH/USDT';
const template={
  exchange:'binance',
  timeframe:String(row?.recommended_timeframe||'1h').trim()||'1h',
  primary:recommended[0]||'BTC/USDT',
  secondary:secondaryFallback,
  lookback:720,
  universe:recommended.slice(0,Math.min(12,recommended.length||12)),
  venues:['binance','okx','gate'],
};
if(strategyType==='PairsTradingStrategy'){
  template.lookback=720;
  template.universe=[template.primary,template.secondary];
}else if(strategyType==='FamaFactorArbitrageStrategy'){
  template.lookback=720;
  template.universe=(recommended.length?recommended:RESEARCH_DEFAULT_SYMBOLS).slice(0,12);
}else if(strategyType==='CEXArbitrageStrategy'){
  template.timeframe='5m';
  template.lookback=480;
  template.universe=RESEARCH_DEFAULT_SYMBOLS.slice(0,6);
  template.venues=['binance','okx','gate'];
}else if(strategyType==='TriangularArbitrageStrategy'){
  template.timeframe='5m';
  template.lookback=360;
  template.universe=RESEARCH_DEFAULT_SYMBOLS.slice(0,5);
  template.venues=['binance','okx','gate'];
}else if(strategyType==='DEXArbitrageStrategy'){
  template.timeframe='5m';
  template.lookback=360;
  template.universe=RESEARCH_DEFAULT_SYMBOLS.slice(0,5);
  template.venues=['uniswap','sushiswap'];
}else if(strategyType==='FlashLoanArbitrageStrategy'){
  template.timeframe='5m';
  template.lookback=240;
  template.universe=RESEARCH_DEFAULT_SYMBOLS.slice(0,4);
  template.venues=['uniswap','sushiswap'];
}
return template;
}

async function applyArbitrageTemplate(strategyType){
const selected=String(strategyType||getArbitrageSelectedStrategy()||'PairsTradingStrategy').trim()||'PairsTradingStrategy';
const template=buildArbitrageTemplate(selected);
arbitrageState.selectedStrategy=selected;
const stEl=document.getElementById('arbitrage-strategy');
const exEl=document.getElementById('arbitrage-exchange');
const tfEl=document.getElementById('arbitrage-timeframe');
const lookbackEl=document.getElementById('arbitrage-lookback');
if(stEl)stEl.value=selected;
if(exEl)exEl.value=template.exchange;
if(tfEl&&[...tfEl.options].some(opt=>String(opt.value||'').trim()===template.timeframe))tfEl.value=template.timeframe;
if(lookbackEl)lookbackEl.value=String(template.lookback);
await loadArbitrageSymbolOptions(template.exchange);
setSelectValues('arbitrage-primary-symbol',[template.primary],template.primary);
setSelectValues('arbitrage-secondary-symbol',[template.secondary],template.secondary);
setSelectValues('arbitrage-universe',template.universe,template.primary);
setSelectValues('arbitrage-venues',template.venues,template.venues[0]||'binance');
renderArbitragePanel();
if(arbitrageState.pairRankingKey&&arbitrageState.pairRankingKey!==getArbitragePairRankingKey()){
  resetArbitragePairRanking('模板已更新，请重新点击“一键筛选前十”');
}else{
  renderArbitragePairRanking();
}
}

function buildArbitrageStrategySpec(strategyType=getArbitrageSelectedStrategy()){
const selected=String(strategyType||getArbitrageSelectedStrategy()||'PairsTradingStrategy').trim()||'PairsTradingStrategy';
const row=(strategyCatalogMap()||{})[selected]||{};
const params=cloneJsonValue(row?.defaults||{});
const primary=getArbitragePrimarySymbol();
const secondary=getArbitrageSecondarySymbol();
const timeframe=getArbitrageTimeframe();
const lookback=getArbitrageLookback();
const allocation=getArbitrageAllocation();
const exchange=getArbitrageExchange();
const venues=getArbitrageVenues();
const suffix=getArbitrageSuffix();
const autoStart=getArbitrageAutoStart();
let universe=Array.from(new Set([primary,...getArbitrageUniverse()].filter(Boolean)));
if(universe.length<2){
  universe=Array.from(new Set([primary,secondary,...RESEARCH_DEFAULT_SYMBOLS.slice(0,6)].filter(Boolean))).slice(0,6);
}
let symbols=[primary];
params.exchange=exchange;
if(selected==='PairsTradingStrategy'){
  const pairSymbol=(secondary&&secondary!==primary?secondary:(universe.find(sym=>sym!==primary)||'ETH/USDT'));
  params.pair_symbol=pairSymbol;
  params.lookback_period=Math.max(20,Math.min(2400,lookback));
  symbols=[primary,pairSymbol];
}else if(selected==='FamaFactorArbitrageStrategy'){
  const famaUniverse=Array.from(new Set([primary,...universe,...RESEARCH_DEFAULT_SYMBOLS.slice(0,12)].filter(Boolean))).slice(0,12);
  params.exchange=exchange;
  params.factor_timeframe=timeframe;
  params.lookback_bars=Math.max(240,lookback);
  params.universe_symbols=famaUniverse;
  params.max_symbols=Math.max(4,Math.min(100,famaUniverse.length));
  params.min_universe_size=Math.max(4,Math.min(Number(params.min_universe_size||4),famaUniverse.length));
  params.top_n=Math.max(2,Math.min(Number(params.top_n||8),Math.max(2,Math.floor(famaUniverse.length/2))));
  symbols=famaUniverse;
}else if(selected==='CEXArbitrageStrategy'){
  const cexVenues=venues.filter(v=>['binance','okx','gate'].includes(v));
  params.exchanges=cexVenues.length?cexVenues:['binance','okx','gate'];
  params.min_spread=Math.max(0,Number(params.min_spread||0.002));
  params.alpha_threshold=Math.max(0,Number(params.alpha_threshold||params.min_spread||0.002));
  symbols=[primary];
}else if(selected==='TriangularArbitrageStrategy'){
  const quoteAsset=symbolQuoteAsset(primary);
  const baseAsset=symbolBaseAsset(primary);
  const bridges=Array.from(new Set(universe.map(symbolBaseAsset).filter(asset=>asset&&asset!==baseAsset&&asset!==quoteAsset))).slice(0,3);
  params.base_currency=quoteAsset;
  params.bridge_assets=bridges.length?bridges:['ETH','BNB','SOL'];
  params.min_profit=Math.max(0,Number(params.min_profit||params.alpha_threshold||0.002));
  params.alpha_threshold=Math.max(0,Number(params.alpha_threshold||params.min_profit||0.002));
  symbols=[primary];
}else if(selected==='DEXArbitrageStrategy'){
  const dexList=venues.filter(v=>['uniswap','sushiswap'].includes(v));
  params.dex_list=dexList.length?dexList:(Array.isArray(params.dex_list)&&params.dex_list.length?params.dex_list:['uniswap','sushiswap']);
  params.chain=String(params.chain||'ethereum').trim()||'ethereum';
  symbols=[primary];
}else if(selected==='FlashLoanArbitrageStrategy'){
  const dexList=venues.filter(v=>['uniswap','sushiswap'].includes(v));
  params.dex_list=dexList.length?dexList:(Array.isArray(params.dex_list)&&params.dex_list.length?params.dex_list:['uniswap','sushiswap']);
  symbols=[primary];
}
return{
  strategy_type:selected,
  strategy:selected,
  symbol:primary,
  symbols,
  timeframe,
  exchange,
  allocation,
  auto_start:autoStart,
  name_suffix:suffix,
  params,
  initial_capital:Number(document.getElementById('backtest-capital')?.value||10000)||10000,
};
}

function renderArbitragePlanSteps(row,spec){
const box=document.getElementById('arbitrage-plan-steps');
if(!box)return;
const shortName=strategyTypeShortName(spec?.strategy_type||'');
const backtestText=row?.backtest_supported
  ? '支持回测。会把完整参数 JSON 带到回测页，再用自定义回测运行。'
  : `当前不支持回测：${String(row?.backtest_reason||'依赖实时盘口 / 链上执行').trim()}`;
const steps=[
  {label:'1',title:'研究配置',text:`${spec.exchange} / ${spec.timeframe} · 主腿 ${spec.symbol} · 币池 ${spec.symbols.length} 个`},
  {label:'2',title:'策略组装',text:`${shortName} · 参数 ${Object.keys(spec.params||{}).length} 项 · ${String(row?.usage||'套利建模').trim()||'套利建模'}`},
  {label:'3',title:row?.backtest_supported?'回测验证':'实时验证',text:backtestText},
  {label:'4',title:'策略库落地',text:`资金占比 ${(Number(spec.allocation||0)*100).toFixed(0)}%${spec.auto_start?'，注册后自动启动':'，注册后默认待启动'}`},
];
box.innerHTML=steps.map(item=>`<div class="arbitrage-step"><div class="arbitrage-step-label">${esc(item.label)}</div><div class="arbitrage-step-title">${esc(item.title)}</div><div class="arbitrage-step-text">${esc(item.text)}</div></div>`).join('');
}

function renderArbitrageStatusCards(){
const row=(strategyCatalogMap()||{})[getArbitrageSelectedStrategy()]||{};
const spec=buildArbitrageStrategySpec(row?.name||getArbitrageSelectedStrategy());
const configEl=document.getElementById('arbitrage-config-snapshot');
const universeEl=document.getElementById('arbitrage-universe-snapshot');
const backtestEl=document.getElementById('arbitrage-backtest-snapshot');
const nextEl=document.getElementById('arbitrage-next-step');
if(configEl)configEl.textContent=`配置：${spec.exchange} / ${spec.symbol} / ${spec.timeframe} | 参数 ${Object.keys(spec.params||{}).length} 项`;
if(universeEl)universeEl.textContent=`币池：${spec.symbols.length} 个 | 对冲腿 ${getArbitrageSecondarySymbol()} | 场所 ${getArbitrageVenues().length} 个`;
if(backtestEl)backtestEl.textContent=row?.backtest_supported
  ? '回测：支持带完整参数进入回测页'
  : `回测：当前不支持，建议先做${String(row?.backtest_reason||'实时验证').trim()}`;
if(nextEl)nextEl.textContent=row?.backtest_supported
  ? '下一步：进入回测页执行单策略自定义回测，再决定是否注册实例'
  : '下一步：直接注册到策略库，再去策略页观察运行状态与实例编辑';
}

function renderArbitrageStrategyCards(){
const box=document.getElementById('arbitrage-strategy-cards');
if(!box)return;
const rows=getArbitrageCatalogRows();
if(!rows.length){
  box.innerHTML='<div class="list-item"><span>套利策略</span><span>策略目录尚未加载</span></div>';
  return;
}
const selected=getArbitrageSelectedStrategy();
box.innerHTML=rows.map(row=>{
  const shortName=strategyTypeShortName(row.name||'');
  const reason=String(row?.backtest_reason||'').trim();
  const symbols=Array.isArray(row?.recommended_symbols)?row.recommended_symbols.filter(Boolean):[];
  return `<article class="arbitrage-card" data-selected="${row.name===selected?'true':'false'}">
    <div class="arbitrage-card-header">
      <div>
        <div class="arbitrage-card-title">${esc(shortName)}</div>
        <div class="arbitrage-card-subtitle">${esc(String(row.name||''))}</div>
      </div>
      <span class="arbitrage-chip" data-tone="${row.backtest_supported?'ok':'warn'}">${esc(row.backtest_supported?'可回测':'仅实时验证')}</span>
    </div>
    <div class="arbitrage-badges">
      <span class="arbitrage-chip">${esc(String(row.category||'套利'))}</span>
      <span class="arbitrage-chip">${esc(String(row.risk||'medium'))} 风险</span>
      <span class="arbitrage-chip">${esc(String(row.recommended_timeframe||'-'))}</span>
    </div>
    <div class="arbitrage-card-body">${esc(String(row.usage||STRATEGY_META[row.name]?.desc||'多币种套利研究').trim()||'多币种套利研究')}</div>
    <div class="arbitrage-card-meta">
      <div class="list-item"><span>推荐交易对</span><span>${esc(symbols.slice(0,4).join(', ')||'-')}</span></div>
      <div class="list-item"><span>回测说明</span><span>${esc(row.backtest_supported?'支持带参数单策略回测':(reason||'依赖实时盘口 / 链上执行'))}</span></div>
    </div>
    <div class="arbitrage-card-actions">
      <button type="button" class="btn btn-primary btn-sm" data-arbitrage-action="template" data-arbitrage-strategy="${esc(row.name)}">加载模板</button>
      <button type="button" class="btn btn-primary btn-sm" data-arbitrage-action="register" data-arbitrage-strategy="${esc(row.name)}">注册运行</button>
      ${row.backtest_supported
        ? `<button type="button" class="btn btn-primary btn-sm" data-arbitrage-action="backtest" data-arbitrage-strategy="${esc(row.name)}">进入回测</button>`
        : `<button type="button" class="btn btn-sm" disabled style="opacity:.55;cursor:not-allowed;">仅实时验证</button>`}
    </div>
  </article>`;
}).join('');
}

function renderArbitrageIntegrationNotes(row,spec){
const box=document.getElementById('arbitrage-integration-notes');
if(!box)return;
const notes=[
  {label:'策略库',value:`复用 /strategies/register，注册时带入 ${spec.symbols.length} 个 symbols、${spec.timeframe} 周期与完整 params。`},
  {label:'回测页',value:row?.backtest_supported?'进入回测后会回填策略、交易对、周期与“自定义策略参数(JSON)”面板。':'当前策略不适合单策略 K 线回测，建议先在策略页做实时/盘口验证。'},
  {label:'边界说明',value:row?.backtest_supported?'多策略对比 / 参数优化暂不读取 JSON 面板，先完成单策略自定义回测更稳妥。':String(row?.backtest_reason||'该策略依赖实时盘口 / 链上流动性 / 原子执行，回测结果会失真。').trim()},
  ];
box.innerHTML=notes.map(item=>`<div class="list-item"><span>${esc(item.label)}</span><span>${esc(item.value)}</span></div>`).join('');
}

function renderArbitragePanel(){
const rows=getArbitrageCatalogRows();
const selected=getArbitrageSelectedStrategy();
const row=rows.find(item=>item.name===selected)||rows[0]||null;
if(!row)return;
arbitrageState.selectedStrategy=row.name;
const spec=buildArbitrageStrategySpec(row.name);
arbitrageState.lastSpec=spec;
const roadmap=document.getElementById('arbitrage-roadmap-summary');
if(roadmap){
  roadmap.textContent=row.backtest_supported
    ? `当前选择 ${strategyTypeShortName(row.name)}。适合先跑单策略自定义回测，再决定是否注册实例与自动启动。`
    : `当前选择 ${strategyTypeShortName(row.name)}。该策略依赖实时盘口 / 链上流动性，建议直接接入策略库后用运行监控验证。`;
}
const preview=document.getElementById('arbitrage-payload-preview');
if(preview)preview.textContent=JSON.stringify(spec,null,2);
renderArbitragePlanSteps(row,spec);
renderArbitrageStatusCards();
renderArbitrageIntegrationNotes(row,spec);
renderArbitrageStrategyCards();
renderArbitragePairRanking();
}

async function registerArbitrageStrategy(strategyType=null){
const selected=String(strategyType||getArbitrageSelectedStrategy()).trim()||getArbitrageSelectedStrategy();
const row=(strategyCatalogMap()||{})[selected]||{};
const spec=buildArbitrageStrategySpec(selected);
const out=getArbitrageOutputEl();
if(out)out.textContent=`正在注册 ${selected} ...\n交易对: ${spec.symbols.join(', ')}\n周期: ${spec.timeframe}\n资金占比: ${spec.allocation}\n自动启动: ${spec.auto_start?'是':'否'}`;
await registerStrategyInstanceFromBacktestSpec(spec);
if(out)out.textContent=`已提交到策略库: ${selected}\n说明: ${row?.backtest_supported?'该策略可先在回测页验证，再在策略页观察实例':'该策略建议直接走实时验证与运行监控'}`;
return true;
}

async function jumpToBacktestFromArbitrage(strategyType=null){
const selected=String(strategyType||getArbitrageSelectedStrategy()).trim()||getArbitrageSelectedStrategy();
const row=(strategyCatalogMap()||{})[selected]||{};
if(!row?.backtest_supported){
  const msg=String(row?.backtest_reason||`${selected} 当前不支持回测`).trim();
  const out=getArbitrageOutputEl();
  if(out)out.textContent=`无法进入回测: ${selected}\n原因: ${msg}\n建议: 直接注册到策略库后，在策略页做实时 / 盘口 / 链上验证。`;
  notify(msg,true);
  return false;
}
const spec=buildArbitrageStrategySpec(selected);
await openBacktestWithSpec(spec);
const out=getArbitrageOutputEl();
if(out)out.textContent=`已将 ${selected} 的完整参数带入回测页。\n下一步: 在回测页点击“运行回测”即可使用自定义参数 JSON 运行单策略验证。`;
return true;
}

function bindArbitragePage(){
if(arbitrageState._bound)return;
arbitrageState._bound=true;
const rerender=()=>renderArbitragePanel();
const maybeResetPairRanking=(reason='周期或交易所已变化，请重新点击“一键筛选前十”')=>{
  if(arbitrageState.pairRankingKey&&arbitrageState.pairRankingKey!==getArbitragePairRankingKey()){
    resetArbitragePairRanking(reason);
  }else{
    renderArbitragePairRanking();
  }
};
const bindChange=(id,handler='change')=>{
  const el=document.getElementById(id);
  if(!el)return;
  el.addEventListener(handler,()=>{rerender();if(id==='arbitrage-timeframe')maybeResetPairRanking();});
};
const strategyEl=document.getElementById('arbitrage-strategy');
if(strategyEl)strategyEl.addEventListener('change',()=>applyArbitrageTemplate(strategyEl.value).catch(e=>notify(`套利模板加载失败: ${e.message}`,true)));
const exchangeEl=document.getElementById('arbitrage-exchange');
if(exchangeEl)exchangeEl.addEventListener('change',()=>loadArbitrageSymbolOptions(exchangeEl.value).then(()=>{renderArbitragePanel();maybeResetPairRanking();}).catch(e=>notify(`套利币池刷新失败: ${e.message}`,true)));
['arbitrage-timeframe','arbitrage-primary-symbol','arbitrage-secondary-symbol','arbitrage-universe','arbitrage-venues','arbitrage-allocation','arbitrage-auto-start'].forEach(id=>bindChange(id,'change'));
['arbitrage-lookback','arbitrage-suffix'].forEach(id=>bindChange(id,'input'));
const applyBtn=document.getElementById('btn-arbitrage-apply-template');
if(applyBtn)applyBtn.onclick=()=>applyArbitrageTemplate(getArbitrageSelectedStrategy()).catch(e=>notify(`套利模板同步失败: ${e.message}`,true));
const refreshBtn=document.getElementById('btn-arbitrage-refresh');
if(refreshBtn)refreshBtn.onclick=()=>loadArbitrageTabData(true).catch(e=>notify(`套利页刷新失败: ${e.message}`,true));
const scanPairsBtn=document.getElementById('btn-arbitrage-scan-pairs');
if(scanPairsBtn)scanPairsBtn.onclick=()=>scanArbitragePairsRanking().catch(e=>notify(`配对筛选失败: ${e.message}`,true));
const applyTopPairBtn=document.getElementById('btn-arbitrage-apply-top-pair');
if(applyTopPairBtn)applyTopPairBtn.onclick=()=>applyArbitragePairCandidate(0).catch(e=>notify(`配对回填失败: ${e.message}`,true));
const registerBtn=document.getElementById('btn-arbitrage-register');
if(registerBtn)registerBtn.onclick=()=>registerArbitrageStrategy().catch(e=>notify(`套利策略注册失败: ${e.message}`,true));
const backtestBtn=document.getElementById('btn-arbitrage-backtest');
if(backtestBtn)backtestBtn.onclick=()=>jumpToBacktestFromArbitrage().catch(e=>notify(`套利页跳转回测失败: ${e.message}`,true));
const pairBody=document.getElementById('arbitrage-pair-ranking-body');
if(pairBody)pairBody.addEventListener('click',e=>{
  const btn=e.target.closest('[data-arbitrage-pair-idx]');
  if(!btn)return;
  const idx=Number(btn.getAttribute('data-arbitrage-pair-idx')||0);
  const prevText=btn.textContent;
  btn.disabled=true;
  btn.textContent='回填中...';
  applyArbitragePairCandidate(idx).catch(err=>notify(`配对回填失败: ${err.message}`,true)).finally(()=>{
    btn.disabled=false;
    btn.textContent=prevText;
  });
});
const cards=document.getElementById('arbitrage-strategy-cards');
if(cards)cards.addEventListener('click',e=>{
  const btn=e.target.closest('[data-arbitrage-action]');
  if(!btn)return;
  const action=String(btn.getAttribute('data-arbitrage-action')||'').trim();
  const strategy=String(btn.getAttribute('data-arbitrage-strategy')||'').trim();
  const prevText=btn.textContent;
  btn.disabled=true;
  btn.textContent=action==='backtest'?'跳转中...':'处理中...';
  const done=()=>{
    btn.disabled=false;
    btn.textContent=prevText;
  };
  if(action==='template'){
    applyArbitrageTemplate(strategy).catch(err=>notify(`套利模板加载失败: ${err.message}`,true)).finally(done);
  }else if(action==='register'){
    registerArbitrageStrategy(strategy).catch(err=>notify(`套利策略注册失败: ${err.message}`,true)).finally(done);
  }else if(action==='backtest'){
    jumpToBacktestFromArbitrage(strategy).catch(err=>notify(`套利页跳转回测失败: ${err.message}`,true)).finally(done);
  }else{
    done();
  }
 });
}

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
function hasFiniteScore(v){return v!==null&&v!==undefined&&v!==''&&Number.isFinite(Number(v));}
function symbolToNewsKey(sym){
const raw=String(sym||'').trim().toUpperCase();
if(!raw)return'';
const main=raw.split(':')[0];
if(main.includes('/'))return main.split('/')[0];
return main.replace(/(USDT|USDC|FDUSD|BUSD|USD)$/,'')||main;
}
function normalizeSymbolKey(sym){
const text=String(sym||'').trim().toUpperCase();
if(!text)return'';
return text.split(':')[0];
}
function hasUsableSpreadValue(micro){
const spread=Number(micro?.orderbook?.spread_bps);
const mid=Number(micro?.orderbook?.mid_price);
return Number.isFinite(spread)&&Number.isFinite(mid)&&mid>0;
}
function hasUsableFlowValue(micro){
const imbalance=Number(micro?.aggressor_flow?.imbalance);
return Number.isFinite(imbalance);
}
function getSentimentMicroFallback(exchange,symbol,maxAgeMs=15*60*1000){
const last=researchState?.lastSentiment;
const raw=last?.raw||{};
const micro=raw?.microstructure||null;
if(!micro)return null;
const sameExchange=String(raw?.exchange||'').toLowerCase()===String(exchange||'').toLowerCase();
const sameSymbol=normalizeSymbolKey(raw?.symbol)===normalizeSymbolKey(symbol);
if(!sameExchange||!sameSymbol)return null;
const ts=toMs(raw?.timestamp||last?.timestamp);
if(Number.isFinite(ts)&&Date.now()-ts>Math.max(5000,Number(maxAgeMs||0)))return null;
if(!hasUsableSpreadValue(micro)&&!hasUsableFlowValue(micro))return null;
return {
...micro,
orderbook:{...(micro?.orderbook||{})},
aggressor_flow:{...(micro?.aggressor_flow||{})},
};
}
function mergeMicrostructureWithFallback(liveMicro,cachedMicro){
const merged={
...(liveMicro||{}),
orderbook:{...((liveMicro||{})?.orderbook||{})},
aggressor_flow:{...((liveMicro||{})?.aggressor_flow||{})},
};
const usedSpread=!hasUsableSpreadValue(merged)&&hasUsableSpreadValue(cachedMicro);
const usedFlow=!hasUsableFlowValue(merged)&&hasUsableFlowValue(cachedMicro);
if(usedSpread){
  merged.orderbook={...(cachedMicro?.orderbook||{}),available:true,stale:true};
}
if(usedFlow){
  merged.aggressor_flow={...(cachedMicro?.aggressor_flow||{}),available:true,stale:true};
}
if(usedSpread||usedFlow){
  const baseErr=String(merged?.source_error||merged?.error||'').trim();
  const fallbackMsg='实时微观结构波动，已回退最近快照';
  merged.available=true;
  merged.stale=true;
  merged.fallback_mode='recent_snapshot';
  merged.source_error=baseErr?`${baseErr}; ${fallbackMsg}`:fallbackMsg;
}
if(hasUsableSpreadValue(merged)&&merged?.orderbook?.available===undefined){
  merged.orderbook.available=true;
}
if(hasUsableFlowValue(merged)&&merged?.aggressor_flow?.available===undefined){
  merged.aggressor_flow.available=true;
}
return merged;
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
const cachedMicro=getSentimentMicroFallback(payload?.exchange,payload?.symbol);
const micro=mergeMicrostructureWithFallback(payload?.microstructure||{},cachedMicro);
const community=payload.community||{},news=payload.news||{};
const spreadAvailable=micro?.orderbook?.available!==false&&hasUsableSpreadValue(micro);
const flowAvailable=micro?.aggressor_flow?.available!==false&&hasUsableFlowValue(micro);
const spreadBps=spreadAvailable?Number(micro?.orderbook?.spread_bps):null;
const imbalance=flowAvailable?Number(micro?.aggressor_flow?.imbalance):null;
const fundingFromMicro=firstFiniteNumber(micro?.funding_rate?.funding_rate,micro?.funding_rate?.rate);
const fundingFromOnchainPct=firstFiniteNumber(payload?.onchain?.funding_rate_multi_source?.mean_rate_pct,researchState?.lastOnchain?.funding_rate_multi_source?.mean_rate_pct);
const fundingFromOnchain=Number.isFinite(Number(fundingFromOnchainPct))?Number(fundingFromOnchainPct)/100:null;
const funding=firstFiniteNumber(fundingFromMicro,fundingFromOnchain,researchState?.lastSentiment?.funding_rate);
const fundingAvailable=Number.isFinite(Number(funding));
const basisPct=firstFiniteNumber(micro?.spot_futures_basis?.basis_pct,micro?.spot_futures_basis?.basis,researchState?.lastSentiment?.basis_pct);
const basisAvailable=Number.isFinite(Number(basisPct));
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
const fundingScore=fundingAvailable?clamp11((-funding)/0.0015):null;
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
const validMetrics=metrics.filter(m=>hasFiniteScore(m.score));
const composite=validMetrics.length?validMetrics.reduce((s,m)=>s+Number(m.score||0),0)/validMetrics.length:0;
const confidence=clamp01(validMetrics.length/metrics.length);
const stance=validMetrics.length===0?'数据不足':composite>0.18?'偏多':composite<-0.18?'偏空':'中性';
const caution=[spreadAvailable&&spreadBps>5?'点差偏大':null,fundingAvailable&&Math.abs(funding)>0.0015?'资金费率极端':null,whaleCount>=12?'巨鲸转账活跃':null,!newsAvailable?'新闻样本不足':null,newsAvailable&&newsN===0&&newsFeedCount>0?'结构化事件仍在补齐':null,!flowAvailable||!spreadAvailable||!fundingAvailable||!basisAvailable?'部分微观结构数据缺失':null,String(news?.scope||'')==='global_fallback'?'新闻已回退到全市场样本':null,micro?.source_error?`交易所微观结构不可用: ${micro.source_error}`:null].filter(Boolean);
researchState.lastSentiment={raw:payload,composite_score:composite,confidence,stance,metrics,spread_bps:spreadBps,imbalance,funding_rate:funding,basis_pct:basisPct,whale_count:whaleCount,news_events:newsEvents,news_feed_count:newsFeedCount,news_raw_count:newsRawCount};
if(summary){
summary.innerHTML=`<div class="list-item"><span>综合情绪 / 置信度</span><span>${stance} (${composite.toFixed(3)}) / ${confidence.toFixed(2)}</span></div><div class="list-item"><span>新闻事件(24h)</span><span>结构化 ${newsEvents} | 当前流 ${newsFeedCount} | 原始 ${newsRawCount}</span></div><div class="list-item"><span>资金费率 / 基差</span><span>${fundingAvailable?(funding*100).toFixed(4)+'%':'--'} / ${basisAvailable?basisPct.toFixed(4)+'%':'--'}</span></div><div class="list-item"><span>点差 / 主动流</span><span>${spreadAvailable?spreadBps.toFixed(3)+' bps':'--'} / ${flowAvailable?imbalance.toFixed(4):'--'}</span></div><div class="list-item"><span>风控提示</span><span>${esc(caution.join('；')||(validMetrics.length?'无明显异常':'数据不足，建议稍后重试'))}</span></div>`;
}
if(grid){
grid.innerHTML=metrics.map(m=>{const hasScore=hasFiniteScore(m.score);const positive=hasScore&&Number(m.score)>=0;const badgeText=!hasScore?'缺失':positive?'正向':'负向';const badgeClass=!hasScore?'warning':positive?'connected':'';return `<div class="strategy-card"><div class="list-item" style="padding:0 0 6px 0;border-bottom:none;"><h4>${esc(m.name)}</h4><span class="status-badge ${badgeClass}">${badgeText}</span></div><p>标准化分数：${hasScore?Number(m.score).toFixed(3):'--'}</p><p>原始值：${esc(String(m.fmt))}</p><p style="font-size:11px;color:#8fa6c0;">${esc(m.hint||'')}</p></div>`;}).join('');
}
renderMarketSentimentChart(validMetrics);
renderResearchConclusionCard();
}
async function loadMarketSentimentDashboard(){
const out=getResearchOutputEl();
const reqId=Number(researchState.lastSentimentReqId||0)+1;
researchState.lastSentimentReqId=reqId;
try{
const ex=getResearchExchange(),sym=getResearchSymbol(),newsSym=symbolToNewsKey(sym);
const [micro,community,newsScoped,newsGlobal,onchain]=await Promise.allSettled([
api(`/trading/analytics/microstructure?exchange=${encodeURIComponent(ex)}&symbol=${encodeURIComponent(sym)}&depth_limit=20`,{timeoutMs:12000}),
api(`/trading/analytics/community/overview?exchange=${encodeURIComponent(ex)}&symbol=${encodeURIComponent(sym)}`,{timeoutMs:12000}),
api(`/news/summary?symbol=${encodeURIComponent(newsSym)}&hours=24`,{timeoutMs:15000}),
api(`/news/summary?hours=24`,{timeoutMs:15000}),
api(`/data/onchain/overview?exchange=${encodeURIComponent(ex)}&symbol=${encodeURIComponent(sym)}&refresh=false`,{timeoutMs:12000}),
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
onchain:onchain.status==='fulfilled'?onchain.value:{},
};
if(reqId!==researchState.lastSentimentReqId)return;
renderMarketSentimentPanel(payload);
renderResearchQuickSummary([{label:'情绪模块',value:'市场情绪仪表盘'},{label:'交易所',value:ex},{label:'标的',value:sym},{label:'新闻样本',value:`结构化 ${Number(payload.news?.events_count||0)} / 当前流 ${Number(payload.news?.feed_count||0)}`}]);
if(out)out.textContent=JSON.stringify(payload,null,2);
}catch(e){
if(reqId!==researchState.lastSentimentReqId)return;
renderMarketSentimentPanel({error:e.message});
if(out)out.textContent=`市场情绪加载失败: ${e.message}`;
notify(`市场情绪加载失败: ${e.message}`,true);
}
}
function applyResearchPreset(kind){
const tfEl=document.getElementById('research-timeframe'),lookbackEl=document.getElementById('research-lookback'),symbolsEl=document.getElementById('research-symbols'),symbolEl=document.getElementById('research-symbol');
const default30=['BTC/USDT','ETH/USDT','BNB/USDT','SOL/USDT','XRP/USDT','ADA/USDT','DOGE/USDT','TRX/USDT','LINK/USDT','AVAX/USDT','DOT/USDT','POL/USDT','LTC/USDT','BCH/USDT','ETC/USDT','ATOM/USDT','NEAR/USDT','APT/USDT','ARB/USDT','OP/USDT','SUI/USDT','INJ/USDT','RUNE/USDT','AAVE/USDT','MKR/USDT','UNI/USDT','FIL/USDT','HBAR/USDT','ICP/USDT','TON/USDT'];
if(kind==='hf30'){if(tfEl)tfEl.value='5m';if(lookbackEl)lookbackEl.value='1800';setSelectValues('research-symbols',default30);if(symbolEl&&!symbolEl.value.trim())symbolEl.value='BTC/USDT';renderResearchStatusCards();notify('已应用预设: 高频30币 (5m / 1800)');return;}
if(kind==='intraday'){if(tfEl)tfEl.value='1m';if(lookbackEl)lookbackEl.value='1200';setSelectValues('research-symbols',default30.slice(0,15));if(symbolEl)symbolEl.value='BTC/USDT';renderResearchStatusCards();notify('已应用预设: 盘中研究 (1m / 1200)');return;}
if(kind==='swing'){if(tfEl)tfEl.value='1h';if(lookbackEl)lookbackEl.value='1000';setSelectValues('research-symbols',default30);if(symbolEl)symbolEl.value='BTC/USDT';renderResearchStatusCards();notify('已应用预设: 波段研究 (1h / 1000)');return;}
renderResearchStatusCards();
}
function bindResearchPresets(){
const b1=document.getElementById('btn-research-preset-hf'),b2=document.getElementById('btn-research-preset-intraday'),b3=document.getElementById('btn-research-preset-swing');
if(b1)b1.onclick=()=>applyResearchPreset('hf30');
if(b2)b2.onclick=()=>applyResearchPreset('intraday');
if(b3)b3.onclick=()=>applyResearchPreset('swing');
}
function bindResearchSentiment(){
const ids=['btn-load-market-sentiment','btn-refresh-market-sentiment-panel'];
ids.forEach(id=>{
  const btn=document.getElementById(id);
  if(!btn)return;
  btn.onclick=()=>loadMarketSentimentDashboard();
});
}
function renderResearchQuickSummary(rows){const box=getResearchSummaryEl();if(!box)return;if(!rows?.length){box.innerHTML='<div class="list-item"><span>暂无摘要</span><span>-</span></div>';renderResearchStatusCards();return;}box.innerHTML=rows.map(r=>`<div class="list-item"><span>${esc(r.label||'-')}</span><span>${esc(String(r.value??'-'))}</span></div>`).join('');renderResearchStatusCards();}
function getResearchConclusionSummaryEl(){return document.getElementById('research-conclusion-summary');}
function getResearchConclusionBulletsEl(){return document.getElementById('research-conclusion-bullets');}
function renderResearchConclusionCard(){
if(window.workbenchState?.initialized&&typeof window.renderResearchConclusionCard==='function'&&window.renderResearchConclusionCard!==renderResearchConclusionCard)return window.renderResearchConclusionCard();
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
renderResearchStatusCards();
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
function hasFactorLibraryContent(data){
if(!data||typeof data!=='object')return false;
return Boolean((Array.isArray(data?.factors)&&data.factors.length)||Object.keys(data?.latest||{}).length||(Array.isArray(data?.asset_scores)&&data.asset_scores.length)||(Array.isArray(data?.series)&&data.series.length)||Number(data?.points||0)>0);
}
function hasFamaContent(data){
if(!data||typeof data!=='object')return false;
return Boolean((Array.isArray(data?.series)&&data.series.length)||Number(data?.points||0)>0||Object.values(data?.latest||{}).some(v=>Math.abs(Number(v||0))>0));
}
function hasOnchainContent(data){
if(!data||typeof data!=='object')return false;
return Boolean((Array.isArray(data?.defi_tvl?.series)&&data.defi_tvl.series.length)||(Array.isArray(data?.whale_activity?.transactions)&&data.whale_activity.transactions.length)||Math.abs(Number(data?.defi_tvl?.latest_tvl||0))>0||Number(data?.whale_activity?.count||0)>0||Number(data?.premium_external?.summary?.cached_sources||0)>0);
}
function isResearchAsyncPending(data,kind='generic'){
if(!data||typeof data!=='object')return false;
const mode=String(data?.served_mode||'').toLowerCase();
const msg=[String(data?.error||''),...(Array.isArray(data?.warnings)?data.warnings:[])].join(' ');
const modePending=['fallback','bootstrap','loading','background','cache_refresh'].includes(mode)||data?.refreshing===true;
const textPending=/后台|预热|加载中|refresh|warming/i.test(msg);
if(kind==='factor_library')return !hasFactorLibraryContent(data)&&(modePending||textPending);
if(kind==='fama')return !hasFamaContent(data)&&(modePending||textPending);
if(kind==='onchain')return !hasOnchainContent(data)&&(modePending||textPending);
return modePending||textPending;
}
function clearResearchPendingTimer(kind){
const timer=researchState.pendingTimers?.[kind];
if(timer){clearTimeout(timer);delete researchState.pendingTimers[kind];}
}
function queueResearchPendingRefresh(kind,loader,delayMs=3000){
if(typeof loader!=='function')return;
clearResearchPendingTimer(kind);
researchState.pendingTimers[kind]=setTimeout(()=>{Promise.resolve().then(loader).catch(()=>{});},Math.max(1200,Number(delayMs||3000)));
}
function pendingResearchNote(data,fallback='后台计算中'){
if(!data||typeof data!=='object')return fallback;
return String(data?.error||((Array.isArray(data?.warnings)&&data.warnings[0])||fallback));
}
function renderFactorCorrelationHeatmap(data){
const el=document.getElementById('factor-corr-chart');
if(!el)return;
if(typeof Plotly==='undefined'){el.innerHTML='<div class="list-item">图表库未加载，因子相关性矩阵暂不可用。</div>';return;}
const corr=data?.correlation||{};
const factors=((Array.isArray(data?.factors)&&data.factors.length?data.factors:Object.keys(corr||{}))).filter(k=>corr&&corr[k]);
if(!factors.length){el.innerHTML='<div class="list-item">暂无因子相关性矩阵</div>';return;}
clearPlotlyHost(el);
preparePlotlyHost(el);
const z=factors.map(r=>factors.map(c=>Number(corr?.[r]?.[c]??0)));
Plotly.react(el,[{type:'heatmap',x:factors,y:factors,z,colorscale:[[0,'#b22222'],[.5,'#1f2937'],[1,'#0ea5a4']],zmin:-1,zmax:1,hovertemplate:'%{y} vs %{x}: %{z:.3f}<extra></extra>'}],{paper_bgcolor:'#111723',plot_bgcolor:'#111723',font:{color:'#d7dde8'},margin:{l:70,r:30,t:20,b:60},xaxis:{tickangle:-35},yaxis:{autorange:'reversed'}},{responsive:true,displaylogo:false});
schedulePlotlyResize(document.getElementById('research')||document);
}
function renderMultiAssetCorrelationHeatmap(data){
const el=document.getElementById('multi-asset-corr-chart');
if(!el)return;
if(typeof Plotly==='undefined'){el.innerHTML='<div class="list-item">图表库未加载，多币种相关性矩阵暂不可用。</div>';return;}
const corr=data?.correlation||{};
const assets=Object.keys(corr||{}).filter(k=>corr&&typeof corr[k]==='object');
if(!assets.length){el.innerHTML='<div class="list-item">暂无多币种收益相关性矩阵</div>';return;}
clearPlotlyHost(el);
preparePlotlyHost(el);
const z=assets.map(r=>assets.map(c=>Number(corr?.[r]?.[c]??0)));
Plotly.react(el,[{type:'heatmap',x:assets,y:assets,z,colorscale:[[0,'#a61b29'],[.5,'#1f2937'],[1,'#0f766e']],zmin:-1,zmax:1,hovertemplate:'%{y} vs %{x}: %{z:.3f}<extra></extra>'}],{paper_bgcolor:'#111723',plot_bgcolor:'#111723',font:{color:'#d7dde8'},margin:{l:80,r:30,t:20,b:80},xaxis:{tickangle:-35},yaxis:{autorange:'reversed'}},{responsive:true,displaylogo:false});
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
const corrEl=document.getElementById('multi-asset-corr-chart');if(corrEl){clearPlotlyHost(corrEl);corrEl.innerHTML='<div class="list-item">暂无多币种收益相关性矩阵</div>';}
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
function renderFamaChart(data){
const el=document.getElementById('fama-factor-chart');
if(!el||typeof Plotly==='undefined')return;
const rows=Array.isArray(data?.series)?data.series:[];
if(!rows.length){el.innerHTML='<div class="list-item">暂无 Fama 因子图</div>';return;}
clearPlotlyHost(el);
preparePlotlyHost(el);
const x=rows.map(r=>r.timestamp);
const traces=[
  {key:'MKT',name:'MKT',color:'#4da3ff'},
  {key:'MOM',name:'MOM',color:'#20bf78'},
  {key:'SMB',name:'SMB',color:'#ffd166'},
  {key:'HML',name:'HML',color:'#f87171'},
].map(item=>({
  type:'scatter',
  mode:'lines',
  x,
  y:rows.map(r=>Number(r?.[item.key]||0)),
  name:item.name,
  line:{width:2,color:item.color},
}));
Plotly.react(el,traces,{
paper_bgcolor:'#111723',plot_bgcolor:'#111723',font:{color:'#d7dde8'},
margin:{l:48,r:20,t:16,b:42},
xaxis:plotlyTimeAxis({title:'时间'}),
yaxis:{title:'因子值',showgrid:true,gridcolor:'#283242',zerolinecolor:'#415066'},
legend:{orientation:'h',y:1.12}
},{responsive:true,displaylogo:false});
schedulePlotlyResize(document.getElementById('research')||document);
}
function renderFamaPanel(data){
const prevGood=hasFamaContent(researchState.lastFama)?researchState.lastFama:null;
const pending=isResearchAsyncPending(data,'fama');
const renderData=pending&&prevGood?prevGood:data;
if(hasFamaContent(data)||(!pending&&data&&typeof data==='object'&&!data.error))researchState.lastFama=data;
const summary=document.getElementById('fama-factor-summary');
const tbody=document.getElementById('fama-factor-tbody');
if(summary)summary.innerHTML='<div class="list-item"><span>加载中...</span><span>-</span></div>';
if(tbody)tbody.innerHTML='<tr><td colspan="5">加载中...</td></tr>';
if(pending&&!hasFamaContent(renderData)){
  const msg=pendingResearchNote(data,'Fama 因子正在后台计算');
  if(summary)summary.innerHTML=`<div class="list-item"><span>${esc(msg)}</span><span>预热中</span></div>`;
  if(tbody)tbody.innerHTML='<tr><td colspan="5">后台计算中，稍后自动补齐</td></tr>';
  const chart=document.getElementById('fama-factor-chart');if(chart){clearPlotlyHost(chart);chart.innerHTML='<div class="list-item">Fama 因子正在后台计算</div>';}
  renderResearchStatusCards();
  return;
}
if(!renderData||renderData.error){
  const msg=data?.error||'Fama 因子加载失败';
  if(summary)summary.innerHTML=`<div class="list-item"><span>${esc(msg)}</span><span>错误</span></div>`;
  if(tbody)tbody.innerHTML=`<tr><td colspan="5">${esc(msg)}</td></tr>`;
  const chart=document.getElementById('fama-factor-chart');if(chart){clearPlotlyHost(chart);chart.innerHTML='<div class="list-item">暂无 Fama 因子图</div>';}
  renderResearchStatusCards();
  return;
}
const latest=renderData?.latest||{};
const universe=Number(renderData?.universe_size||0);
const points=Number(renderData?.points||0);
const quality=String(renderData?.universe_quality||'-');
if(summary){
  summary.innerHTML=`
  <div class="list-item"><span>样本点 / 币种数</span><span>${points} / ${universe}</span></div>
  <div class="list-item"><span>MKT / MOM</span><span>${Number(latest?.MKT||0).toFixed(6)} / ${Number(latest?.MOM||0).toFixed(6)}</span></div>
  <div class="list-item"><span>SMB / HML</span><span>${Number(latest?.SMB||0).toFixed(6)} / ${Number(latest?.HML||0).toFixed(6)}</span></div>
  <div class="list-item"><span>质量</span><span>${esc(quality)}${pending?` | ${esc(pendingResearchNote(data,'后台刷新中'))}`:(renderData?.warnings?.length?` | ${esc(String(renderData.warnings[0]))}`:'')}</span></div>`;
}
const rows=(Array.isArray(renderData?.series)?renderData.series:[]).slice(-12).reverse();
if(tbody){
  tbody.innerHTML=rows.length?rows.map(r=>`<tr><td>${esc(fmtAxisDateTime(r.timestamp))}</td><td>${Number(r?.MKT||0).toFixed(6)}</td><td>${Number(r?.SMB||0).toFixed(6)}</td><td>${Number(r?.HML||0).toFixed(6)}</td><td>${Number(r?.MOM||0).toFixed(6)}</td></tr>`).join(''):'<tr><td colspan="5">暂无数据</td></tr>';
}
renderFamaChart(renderData);
renderResearchStatusCards();
}
function renderOnchainChart(data){
const el=document.getElementById('onchain-overview-chart');
if(!el||typeof Plotly==='undefined')return;
const series=Array.isArray(data?.defi_tvl?.series)?data.defi_tvl.series:[];
const whales=Array.isArray(data?.whale_activity?.transactions)?data.whale_activity.transactions:[];
if(!series.length && !whales.length){clearPlotlyHost(el);el.innerHTML='<div class="list-item">暂无链上图表数据</div>';return;}
clearPlotlyHost(el);
preparePlotlyHost(el);
const traces=[];
if(series.length){
  traces.push({
    type:'scatter',
    mode:'lines',
    x:series.map(r=>r.timestamp),
    y:series.map(r=>Number(r?.tvl||0)),
    name:'TVL',
    line:{width:2,color:'#4da3ff'},
    yaxis:'y1',
  });
}
if(whales.length){
  const top=whales.slice(0,8).reverse();
  traces.push({
    type:'bar',
    orientation:'h',
    x:top.map(r=>Number(r?.btc||0)),
    y:top.map(r=>String(r?.hash||'').slice(0,10)||'--'),
    name:'巨鲸BTC',
    marker:{color:'#20bf78',opacity:0.72},
    xaxis:'x2',
    yaxis:'y2',
  });
}
Plotly.react(el,traces,{
paper_bgcolor:'#111723',plot_bgcolor:'#111723',font:{color:'#d7dde8'},
margin:{l:56,r:26,t:16,b:42},
grid:{rows:whales.length?2:1,columns:1,pattern:'independent',roworder:'top to bottom'},
xaxis:plotlyTimeAxis({title:'时间'}),
yaxis:{title:'TVL',showgrid:true,gridcolor:'#283242'},
xaxis2:{title:'BTC 数量',showgrid:true,gridcolor:'#283242'},
yaxis2:{title:'巨鲸转账',automargin:true},
legend:{orientation:'h',y:1.12}
},{responsive:true,displaylogo:false});
schedulePlotlyResize(document.getElementById('research')||document);
}
function getOnchainStatusLine(data){
const status=data?.component_status||{};
const labels=[
  ['exchange_flow_proxy','流向'],
  ['defi_tvl','TVL'],
  ['whale_activity','巨鲸'],
  ['funding_rate_multi_source','Funding'],
  ['fear_greed_index','情绪'],
  ['premium_external','高级源'],
];
const parts=[];
for(const [key,label] of labels){
  const item=status?.[key];
  if(!item)continue;
  parts.push(`${label}:${item?.status==='ok'?'正常':'降级'}`);
}
return parts.join(' | ')||'组件状态未知';
}
async function loadOnchainOverviewPanel({refresh=false,quiet=false,showLoading=true,timeoutMs=12000}={}){
const out=getResearchOutputEl();
if(showLoading)setResearchMiniPanelLoading('onchain');
try{
  const d=await api(`/data/onchain/overview?exchange=${encodeURIComponent(getResearchExchange())}&symbol=${encodeURIComponent(getResearchSymbol())}&whale_threshold_btc=10&chain=Ethereum&refresh=${refresh?'true':'false'}`,{timeoutMs});
  renderOnchainPanel(d);
  if(out&&!quiet)out.textContent=JSON.stringify(d,null,2);
  if(isResearchAsyncPending(d,'onchain')||d?.refreshing&&(!d?.cached||d?.served_mode==='bootstrap')){
    queueResearchPendingRefresh('onchain',()=>loadOnchainOverviewPanel({refresh:false,quiet:true,showLoading:false,timeoutMs}),3000);
  }
  if(!quiet)notify(d?.served_mode==='bootstrap'?'链上概览正在后台预热，面板会自动补全':d?.cached&&d?.refreshing?'链上概览已显示缓存并在后台刷新':'链上概览已更新');
  return d;
}catch(e){
  renderOnchainPanel({error:e.message});
  if(out&&!quiet)out.textContent=`链上概览失败: ${e.message}`;
  if(!quiet)notify(`链上概览失败: ${e.message}`,true);
  return {error:e.message};
}
}
function renderOnchainPanel(data){
const prevGood=hasOnchainContent(researchState.lastOnchain)?researchState.lastOnchain:null;
const pending=isResearchAsyncPending(data,'onchain');
const renderData=pending&&prevGood?prevGood:data;
if(hasOnchainContent(data)||(!pending&&data&&typeof data==='object'&&!data.error))researchState.lastOnchain=data;
const summary=document.getElementById('onchain-overview-summary');
const externalSummary=document.getElementById('external-info-summary');
const tbody=document.getElementById('onchain-whale-tbody');
if(summary)summary.innerHTML='<div class="list-item"><span>加载中...</span><span>-</span></div>';
if(externalSummary)externalSummary.innerHTML='<div class="list-item"><span>加载中...</span><span>-</span></div>';
if(tbody)tbody.innerHTML='<tr><td colspan="4">加载中...</td></tr>';
if(pending&&!hasOnchainContent(renderData)){
  const msg=pendingResearchNote(data,'链上面板正在后台预热');
  if(summary)summary.innerHTML=`<div class="list-item"><span>${esc(msg)}</span><span>预热中</span></div>`;
  if(externalSummary)externalSummary.innerHTML=`<div class="list-item"><span>${esc(msg)}</span><span>预热中</span></div>`;
  if(tbody)tbody.innerHTML='<tr><td colspan="4">后台补拉链上数据中，稍后自动更新</td></tr>';
  const chart=document.getElementById('onchain-overview-chart');if(chart){clearPlotlyHost(chart);chart.innerHTML='<div class="list-item">链上图表正在后台加载</div>';}
  renderResearchConclusionCard();
  return;
}
if(!renderData||renderData.error){
  const msg=data?.error||'链上概览加载失败';
  if(summary)summary.innerHTML=`<div class="list-item"><span>${esc(msg)}</span><span>错误</span></div>`;
  if(externalSummary)externalSummary.innerHTML=`<div class="list-item"><span>${esc(msg)}</span><span>错误</span></div>`;
  if(tbody)tbody.innerHTML=`<tr><td colspan="4">${esc(msg)}</td></tr>`;
  const chart=document.getElementById('onchain-overview-chart');if(chart){clearPlotlyHost(chart);chart.innerHTML='<div class="list-item">暂无链上图表数据</div>';}
  renderResearchConclusionCard();
  return;
}
const flow=Number(renderData?.exchange_flow_proxy?.imbalance ?? 0);
const tvl=renderData?.defi_tvl||{};
const whales=renderData?.whale_activity||{};
const fundingMulti=renderData?.funding_rate_multi_source||{};
const fundingCount=Number(fundingMulti?.count||0);
const fundingMeanPct=Number(fundingMulti?.mean_rate_pct);
const fundingSpreadPct=Number(fundingMulti?.spread_rate_pct);
const fearGreed=renderData?.fear_greed_index||{};
const fearGreedValue=Number(fearGreed?.value);
const fearGreedOk=!!fearGreed?.available&&Number.isFinite(fearGreedValue);
const premiumExternal=renderData?.premium_external||{};
const premiumSummary=premiumExternal?.summary||{};
const premiumSources=premiumExternal?.sources||{};
const premiumTotal=Number(premiumSummary?.total_sources||Object.keys(premiumSources||{}).length||0);
const premiumCached=Number(premiumSummary?.cached_sources||0);
const premiumConfigured=Number(premiumSummary?.configured_keys||0);
const premiumActive=Array.isArray(premiumSummary?.active_sources)?premiumSummary.active_sources:Object.keys(premiumSources||{}).filter(k=>premiumSources?.[k]?.has_cached_data);
const premiumLine1=premiumTotal>0?`高级源 ${premiumCached}/${premiumTotal} 已缓存 | Key ${premiumConfigured}`:'高级源未配置';
const premiumLine2=premiumActive.length?`活跃 ${premiumActive.slice(0,4).join(' / ')}`:(premiumConfigured>0?'等待首轮采集':'未配置付费源');
const servedMode=renderData?.served_mode==='cache_refresh'?'缓存+后台刷新':renderData?.served_mode==='cache'?'缓存':'实时';
const generatedAt=renderData?.generated_at?fmtDateTime(renderData.generated_at):'--';
const cacheAgeSec=Number(renderData?.cache_age_sec||0);
const statusLine=getOnchainStatusLine(renderData);
const statusParts=String(statusLine||'').split('|').map(x=>x.trim()).filter(Boolean);
const statusLine1=statusParts.slice(0,2).join(' | ');
const statusLine2=statusParts.slice(2,4).join(' | ');
const statusLine3=[statusParts.slice(4).join(' | '),renderData?.cached?`${cacheAgeSec.toFixed(1)}s`:null,pending?pendingResearchNote(data,'后台刷新中'):null].filter(Boolean).join(' | ');
const fundingLine1=fundingCount>0?`${fundingCount}源 | 均值 ${Number.isFinite(fundingMeanPct)?fundingMeanPct.toFixed(4)+'%':'--'}`:'暂无可用数据';
const fundingLine2=fundingCount>0?`分歧 ${Number.isFinite(fundingSpreadPct)?fundingSpreadPct.toFixed(4)+'%':'--'}`:'';
if(summary){
  summary.innerHTML=`
  <div class="list-item"><span>链 / 观察窗口</span>${formatMetricLines([`${tvl?.chain||'Ethereum'} / ${Number(renderData?.window_hours||0)}h`])}</div>
  <div class="list-item"><span>最新 TVL</span>${formatMetricLines([`${fmtCompactUsd(tvl?.latest_tvl)} | 1d ${Number(tvl?.change_1d_pct||0).toFixed(2)}%`])}</div>
  <div class="list-item"><span>7d 变化 / 交易所流向</span>${formatMetricLines([`${Number(tvl?.change_7d_pct||0).toFixed(2)}% / ${flow.toFixed(4)}`])}</div>
  <div class="list-item"><span>巨鲸数量</span>${formatMetricLines([`${Number(whales?.count||0)} 笔`,`阈值 ${Number(whales?.threshold_btc||0)} BTC`])}</div>
  <div class="list-item"><span>多所 Funding</span>${formatMetricLines([fundingLine1,fundingLine2])}</div>
  <div class="list-item"><span>恐慌贪婪指数</span>${formatMetricLines([fearGreedOk?`${fearGreedValue} (${fearGreed?.classification||'-'})`:'暂无可用数据',fearGreedOk?`信号 ${fearGreed?.signal||'neutral'}`:''])}</div>
  <div class="list-item"><span>高级源快照</span>${formatMetricLines([premiumLine1,premiumLine2])}</div>
  <div class="list-item"><span>返回方式 / 生成时间</span>${formatMetricLines([servedMode,generatedAt])}</div>
  <div class="list-item"><span>组件状态</span>${formatMetricLines([statusLine1,statusLine2,statusLine3])}</div>`;
}
if(externalSummary){
  externalSummary.innerHTML=`
  <div class="list-item"><span>外生资金</span>${formatMetricLines([fundingLine1,fundingLine2])}</div>
  <div class="list-item"><span>风险情绪</span>${formatMetricLines([fearGreedOk?`${fearGreedValue} (${fearGreed?.classification||'-'})`:'暂无可用数据',fearGreedOk?`信号 ${fearGreed?.signal||'neutral'}`:''])}</div>
  <div class="list-item"><span>高级源快照</span>${formatMetricLines([premiumLine1,premiumLine2])}</div>
  <div class="list-item"><span>返回模式</span>${formatMetricLines([servedMode,generatedAt])}</div>
  <div class="list-item"><span>组件健康</span>${formatMetricLines([statusLine1,statusLine2,statusLine3])}</div>`;
}
const txRows=(Array.isArray(whales?.transactions)?whales.transactions:[]).slice(0,24);
if(tbody){
  tbody.innerHTML=txRows.length?txRows.map(r=>`<tr><td>${esc(fmtDateTime(r?.timestamp))}</td><td>${Number(r?.btc||0).toFixed(3)}</td><td>${fmtCompactUsd(r?.usd_estimate)}</td><td title="${esc(r?.hash||'')}">${esc(String(r?.hash||'').slice(0,14) || '--')}</td></tr>`).join(''):'<tr><td colspan="4">暂无巨鲸转账</td></tr>';
}
renderOnchainChart(renderData);
renderResearchConclusionCard();
}
function setResearchMiniPanelLoading(kind='fama'){
const map={
  fama:{summary:'fama-factor-summary',tbody:'fama-factor-tbody',colspan:5,chart:'fama-factor-chart'},
  onchain:{summary:'onchain-overview-summary',tbody:'onchain-whale-tbody',colspan:4,chart:'onchain-overview-chart'},
};
const cfg=map[kind];
if(!cfg)return;
const summary=document.getElementById(cfg.summary);
const tbody=document.getElementById(cfg.tbody);
const chart=document.getElementById(cfg.chart);
if(summary)summary.innerHTML='<div class="list-item"><span>加载中...</span><span>请稍候</span></div>';
if(tbody)tbody.innerHTML=`<tr><td colspan="${cfg.colspan}">加载中...</td></tr>`;
if(chart)chart.innerHTML='<div class="list-item">加载中...</div>';
}
async function loadFamaPanelWithAutoRefresh({quiet=false,attempt=0,timeoutMs=30000}={}){
const syms=getResearchSymbols();
if(!syms.includes('ADA/USDT'))syms.push('ADA/USDT');
const d=await api(`/data/factors/fama?exchange=${encodeURIComponent(getResearchExchange())}&symbols=${encodeURIComponent(syms.join(','))}&timeframe=${encodeURIComponent(getResearchTimeframe())}&lookback=${Math.min(2400,getResearchLookback())}&exclude_retired=${getResearchExcludeRetired()?'true':'false'}`,{timeoutMs});
renderFamaPanel(d);
if(isResearchAsyncPending(d,'fama')&&attempt<4){
  queueResearchPendingRefresh('fama',()=>loadFamaPanelWithAutoRefresh({quiet:true,attempt:attempt+1,timeoutMs:Math.max(timeoutMs,20000)}),2800+(attempt*1200));
}else{
  clearResearchPendingTimer('fama');
}
if(!quiet){
  renderResearchQuickSummary([{label:'Fama样本点',value:Number(d?.points||0)},{label:'MKT',value:Number(d?.latest?.MKT||0).toFixed(6)},{label:'MOM',value:Number(d?.latest?.MOM||0).toFixed(6)}]);
}
return d;
}
async function loadFactorLibraryWithAutoRefresh({quiet=false,attempt=0}={}){
const syms=getResearchSymbols();
['ADA/USDT','TRX/USDT','LINK/USDT'].forEach(x=>{if(!syms.includes(x))syms.push(x);});
const tf=getResearchTimeframe();
const factorLookback=getFactorLookbackForTimeframe(tf,getResearchLookback());
const factorTimeoutMs=getFactorApiTimeoutMs(tf,syms.length);
const d=await api(`/data/factors/library?exchange=${encodeURIComponent(getResearchExchange())}&symbols=${encodeURIComponent(syms.join(','))}&timeframe=${encodeURIComponent(tf)}&lookback=${factorLookback}&quantile=0.3&series_limit=500&exclude_retired=${getResearchExcludeRetired()?'true':'false'}`,{timeoutMs:factorTimeoutMs});
renderFactorLibraryPanel(d);
if(isResearchAsyncPending(d,'factor_library')&&attempt<4){
  queueResearchPendingRefresh('factor_library',()=>loadFactorLibraryWithAutoRefresh({quiet:true,attempt:attempt+1}),3200+(attempt*1400));
}else{
  clearResearchPendingTimer('factor_library');
}
if(!quiet){
  renderResearchQuickSummary([{label:'因子数量',value:(d?.factors||[]).length},{label:'覆盖币种',value:(d?.symbols_used||[]).length},{label:'已排除',value:Number(d?.retired_filter?.excluded_symbols?.length||0)},{label:'有效时间点',value:Number(d?.points||0)},{label:'质量',value:d?.universe_quality||'-'}]);
}
return d;
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
`生成时间: ${fmtDateTime(new Date())}`,
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
const prevGood=hasFactorLibraryContent(researchState.lastFactorLibrary)?researchState.lastFactorLibrary:null;
const pending=isResearchAsyncPending(data,'factor_library');
const renderData=pending&&prevGood?prevGood:data;
if(hasFactorLibraryContent(data)||(!pending&&data&&typeof data==='object'&&!data.error))researchState.lastFactorLibrary=data;
if(pending&&!hasFactorLibraryContent(renderData)){
const msg=pendingResearchNote(data,'因子库正在后台计算');
if(summary)summary.innerHTML=`<div class="list-item"><span>${esc(msg)}</span><span>预热中</span></div>`;
if(grid)grid.innerHTML='<div class="list-item">因子卡片正在后台生成</div>';
if(tbody)tbody.innerHTML='<tr><td colspan="9">后台计算中，稍后自动补齐打分表</td></tr>';
const corr=document.getElementById('factor-corr-chart');if(corr){clearPlotlyHost(corr);corr.innerHTML='<div class="list-item">因子相关性矩阵正在后台生成</div>';}
renderResearchConclusionCard();
return;
}
if(!renderData||renderData.error){
const msg=data?.error||'因子库加载失败';
if(summary)summary.innerHTML=`<div class="list-item"><span>${esc(msg)}</span><span>错误</span></div>`;
if(tbody)tbody.innerHTML=`<tr><td colspan="9">${esc(msg)}</td></tr>`;
const corr=document.getElementById('factor-corr-chart');if(corr){clearPlotlyHost(corr);corr.innerHTML='<div class="list-item">暂无因子相关性矩阵</div>';}
renderResearchConclusionCard();
return;
}
const latest=renderData.latest||{},mean=renderData.mean_24||{},std=renderData.std_24||{};
if(summary){
summary.innerHTML=`
<div class="list-item"><span>交易所 / 周期</span><span>${esc(renderData.exchange||'-')} / ${esc(renderData.timeframe||'-')}</span></div>
<div class="list-item"><span>因子数量 / 有效时间点</span><span>${(renderData.factors||[]).length} / ${Number(renderData.points||0)}</span></div>
<div class="list-item"><span>币种覆盖</span><span>${(renderData.symbols_used||[]).length} 个</span></div>
<div class="list-item"><span>相关性矩阵</span><span>${Object.keys(renderData.correlation||{}).length?'已加载':'无'}</span></div>
<div class="list-item"><span>说明</span><span>有效时间点=多币种对齐后可计算因子的 bar 数</span></div>
<div class="list-item"><span>风险提示</span><span>${esc(pending?pendingResearchNote(data,'后台刷新中'):((renderData.warnings||[])[0]||'无'))}</span></div>`;
}
const factorKeys=Object.keys(latest||{});
if(grid){
grid.innerHTML=factorKeys.length?factorKeys.map(k=>{
const v=Number(latest[k]||0),m=Number(mean[k]||0),s=Number(std[k]||0);
return `<div class="strategy-card"><div class="list-item" style="padding:0 0 6px 0;border-bottom:none;"><h4>${esc(k)}</h4><span class="status-badge ${v>=0?'connected':'negative'}">${v>=0?'偏多':'偏空'}</span></div><p>最新值: ${v.toFixed(6)}</p><p>24h均值: ${m.toFixed(6)}</p><p>24h波动: ${s.toFixed(6)}</p></div>`;
}).join(''):'<div class="list-item">暂无因子指标</div>';
}
if(tbody){
const rows=getFilteredSortedFactorRows(renderData);
tbody.innerHTML=rows.length?rows.map(r=>`<tr><td>${esc(r.symbol||'-')}</td><td>${Number(r.score||0).toFixed(6)}</td><td>${Number(r.momentum||0).toFixed(6)}</td><td>${Number(r.value||0).toFixed(6)}</td><td>${Number(r.quality||0).toFixed(6)}</td><td>${Number(r.low_vol||0).toFixed(6)}</td><td>${Number(r.liquidity||0).toFixed(6)}</td><td>${Number(r.low_beta||0).toFixed(6)}</td><td>${Number(r.size||0).toFixed(6)}</td></tr>`).join(''):'<tr><td colspan="9">暂无币种打分</td></tr>';
}
renderFactorCorrelationHeatmap(renderData);
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
function countWorkbenchNewsSamples(news){
const events=Number(news?.events_count||0);
const feed=Number(news?.feed_count||0);
const raw=Number(news?.raw_count||0);
return {events,feed,raw,total:events+feed+raw};
}
function fmtWorkbenchMetric(value,formatter='raw'){
if(value===null||value===undefined||value==='')return '--';
const num=Number(value);
if(!Number.isFinite(num))return String(value);
if(formatter==='pct4')return `${(num*100).toFixed(4)}%`;
if(formatter==='pct2')return `${num.toFixed(2)}%`;
if(formatter==='fixed4')return num.toFixed(4);
if(formatter==='bps')return `${num.toFixed(3)} bps`;
return String(value);
}
function firstFiniteNumber(...values){
for(const val of values){
  const num=Number(val);
  if(Number.isFinite(num))return num;
}
return null;
}
function formatMetricLines(lines){
const arr=(Array.isArray(lines)?lines:[lines]).map(v=>String(v??'').trim()).filter(Boolean);
if(!arr.length)return '<span class="metric-lines">--</span>';
return `<span class="metric-lines">${arr.map(item=>esc(item)).join('<br>')}</span>`;
}
function renderWorkbenchMarketStatePanel(module){
const out=getResearchOutputEl(),summary=document.getElementById('analytics-overview-summary'),grid=document.getElementById('analytics-module-grid');
if(!summary||!grid)return;
const payload=module?.payload||module||{};
if(!module||module?.error||payload?.error){
const msg=payload?.error||module?.error||'市场状态加载失败';
researchState.lastAnalytics={error:msg};
summary.innerHTML=`<div class="list-item"><span>${esc(msg)}</span><span>错误</span></div>`;
grid.innerHTML='<div class="list-item">暂无市场状态卡片</div>';
if(out)out.textContent=`市场状态加载失败: ${msg}`;
renderResearchConclusionCard();
return;
}
const regime=payload.regime||{};
const sentiment=payload.sentiment_dashboard||{};
const micro=sentiment.microstructure||{};
const community=sentiment.community||{};
const news=sentiment.news||{};
const calendarRows=Array.isArray(payload.calendar_watchlist)?payload.calendar_watchlist:[];
const newsCount=countWorkbenchNewsSamples(news);
const spreadBps=Number.isFinite(Number(micro?.orderbook?.spread_bps))&&Number(micro?.orderbook?.spread_bps)>0?Number(micro.orderbook.spread_bps):null;
const imbalance=Number.isFinite(Number(micro?.aggressor_flow?.imbalance))?Number(micro.aggressor_flow.imbalance):null;
const funding=firstFiniteNumber(micro?.funding_rate?.funding_rate,micro?.funding_rate?.rate,researchState?.lastSentiment?.funding_rate);
const basisPct=firstFiniteNumber(micro?.spot_futures_basis?.basis_pct,micro?.spot_futures_basis?.basis,researchState?.lastSentiment?.basis_pct);
const sourceError=String(micro?.source_error||'').trim();
const announcements=Array.isArray(community?.announcements)?community.announcements:[];
const whaleCount=Number(community?.whale_transfers?.count||0);
const warnings=(Array.isArray(module?.warnings)?module.warnings:[]).filter(Boolean);
const cards=[
{title:'市场状态',badge:regime?.regime||'待判定',status:'connected',lines:[`方向 ${regime?.bias||'neutral'}`,`置信 ${Number(regime?.confidence||0).toFixed(2)} | 风险 ${regime?.risk_level||'unknown'}`]},
{title:'交易日历',badge:calendarRows.length?`${calendarRows.length}条`:'暂无',status:calendarRows.length?'connected':'warning',lines:[calendarRows.length?`事件 ${calendarRows.length} 条`:'当前窗口内暂无重点事件',calendarRows[0]?.title||calendarRows[0]?.event||'等待下一次刷新']},
{title:'情绪与新闻',badge:newsCount.total?`${newsCount.total}样本`:'样本不足',status:newsCount.total?'connected':'warning',lines:[`结构化 ${newsCount.events} | 当前流 ${newsCount.feed}`,`原始新闻 ${newsCount.raw}`]},
{title:'社区与公告',badge:(announcements.length||whaleCount)?'正常':'稀疏',status:(announcements.length||whaleCount)?'connected':'warning',lines:[`公告 ${announcements.length} | 巨鲸 ${whaleCount}`,announcements[0]?.title||announcements[0]?.headline||'暂无公告样本']},
{title:'微观结构',badge:sourceError?'降级':(spreadBps!==null?'正常':'样本不足'),status:sourceError?'warning':'connected',lines:[`点差 ${fmtWorkbenchMetric(spreadBps,'bps')} | 主动流 ${fmtWorkbenchMetric(imbalance,'fixed4')}`,sourceError?`来源 ${sourceError}`:`费率 ${fmtWorkbenchMetric(funding,'pct4')} | 基差 ${fmtWorkbenchMetric(basisPct,'pct2')}`]},
{title:'数据提醒',badge:warnings.length?`${warnings.length}条`:'正常',status:warnings.length?'warning':'connected',lines:warnings.length?[String(warnings[0]||'').slice(0,40),String(warnings[1]||'无额外警告').slice(0,40)]:['当前模块已返回有效摘要','可继续查看因子与链上模块']},
];
researchState.lastAnalytics={workbench:true,risk_level:regime?.risk_level||'unknown',market_regime:regime?.regime||'未知',direction_bias:regime?.bias||'neutral',confidence:Number(regime?.confidence||0),calendar_count:calendarRows.length,news_samples:newsCount.total,whale_count:whaleCount,microstructure_available:!sourceError&&spreadBps!==null};
summary.innerHTML=[
`<div class="list-item"><span>市场状态 / 方向</span><span>${esc(regime?.regime||'未知')} / ${esc(regime?.bias||'neutral')}</span></div>`,
`<div class="list-item"><span>置信度 / 风险等级</span><span>${Number(regime?.confidence||0).toFixed(2)} / ${esc(regime?.risk_level||'unknown')}</span></div>`,
`<div class="list-item"><span>交易日历 / 新闻样本</span><span>${calendarRows.length} / ${newsCount.total}</span></div>`,
`<div class="list-item"><span>公告 / 巨鲸</span><span>${announcements.length} / ${whaleCount}</span></div>`,
`<div class="list-item"><span>微观结构</span><span>${sourceError?esc(sourceError):(spreadBps!==null?`${spreadBps.toFixed(3)} bps / ${fmtWorkbenchMetric(imbalance,'fixed4')}`:'样本不足')}</span></div>`,
].join('');
grid.innerHTML=cards.map(card=>`<div class="strategy-card"><div class="list-item" style="padding:0 0 6px 0;border-bottom:none;"><h4>${esc(card.title)}</h4><span class="status-badge ${esc(card.status)}">${esc(card.badge)}</span></div>${card.lines.map(line=>`<p>${esc(String(line||'--'))}</p>`).join('')}</div>`).join('');
if(out){
out.textContent=JSON.stringify({market_state:regime,calendar_watchlist:calendarRows.slice(0,8),community:{announcements:announcements.length,whale_count:whaleCount},microstructure:{spread_bps:spreadBps,imbalance,funding_rate:funding,basis_pct:basisPct,source_error:sourceError||null},news:newsCount,warnings},null,2);
}
renderResearchConclusionCard();
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
<div class="list-item"><span>更新时间</span><span>${fmtDateTime(data.timestamp||Date.now())}</span></div>
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
const days=Math.max(7,Math.min(365,estimateResearchWindowDays()*2));
const lookback=Math.max(120,Math.min(2000,getResearchLookback()));
const calendarDays=Math.max(7,Math.min(90,estimateResearchWindowDays()));
const fb=await api(`/trading/analytics/overview?days=${days}&lookback=${lookback}&calendar_days=${calendarDays}&exchange=${encodeURIComponent(ex)}&symbol=${encodeURIComponent(s)}`,{timeoutMs:45000});
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
const overviewDays=Math.max(7,Math.min(365,estimateResearchWindowDays()*2));
const overviewCalendarDays=Math.max(7,Math.min(90,estimateResearchWindowDays()));
const [analytics,multi,factors]=await Promise.allSettled([
api(`/trading/analytics/overview?days=${overviewDays}&lookback=${Math.max(120,Math.min(2000,lookback))}&calendar_days=${overviewCalendarDays}&exchange=${encodeURIComponent(ex)}&symbol=${encodeURIComponent(symbol)}`,{timeoutMs:45000}),
api(`/data/multi-assets/overview?exchange=${encodeURIComponent(ex)}&symbols=${encodeURIComponent(symbols)}&timeframe=${encodeURIComponent(timeframe)}&lookback=${Math.min(2000,lookback)}&exclude_retired=${excludeRetired?'true':'false'}`,{timeoutMs:25000}),
api(`/data/factors/library?exchange=${encodeURIComponent(ex)}&symbols=${encodeURIComponent(symbols)}&timeframe=${encodeURIComponent(timeframe)}&lookback=${factorLookback}&quantile=0.3&series_limit=500&exclude_retired=${excludeRetired?'true':'false'}`,{timeoutMs:factorTimeoutMs}),
]);
const prevOnchain=(researchState.lastOnchain&&typeof researchState.lastOnchain==='object')?researchState.lastOnchain:{pending:true};
const summary={
timestamp:new Date().toISOString(),
config:{exchange:ex,symbol,timeframe,lookback,symbols:symbols.split(','),factor_lookback:factorLookback,exclude_retired:excludeRetired},
analytics:analytics.status==='fulfilled'?analytics.value:{error:analytics.reason?.message||'加载失败'},
multi_assets:multi.status==='fulfilled'?multi.value:{error:multi.reason?.message||'加载失败'},
factor_library:factors.status==='fulfilled'?factors.value:{error:factors.reason?.message||'加载失败'},
onchain:prevOnchain
};
researchState.lastOverview=summary;
renderAnalyticsOverviewPanel(summary.analytics);
renderFactorLibraryPanel(summary.factor_library);
renderMultiAssetPanel(summary.multi_assets);
if(isResearchAsyncPending(summary.factor_library,'factor_library')){
  queueResearchPendingRefresh('factor_library',()=>loadFactorLibraryWithAutoRefresh({quiet:true,attempt:1}),3200);
}
if(prevOnchain?.pending){
  setResearchMiniPanelLoading('onchain');
}else{
  renderOnchainPanel(summary.onchain);
}
renderResearchQuickSummary([
{label:'多币种覆盖',value:Number(summary.multi_assets?.count||0)},
{label:'过滤停更',value:summary.config.exclude_retired?'开启':'关闭'},
{label:'排除币种',value:Number(summary.factor_library?.retired_filter?.excluded_symbols?.length||summary.multi_assets?.retired_filter?.excluded_symbols?.length||0)},
{label:'链上巨鲸笔数',value:prevOnchain?.pending?'加载中':Number(summary.onchain?.whale_activity?.count||0)},
{label:'交易所',value:summary.config.exchange},
{label:'周期',value:summary.config.timeframe},
]);
const moduleStatus=[
`analytics:${summary.analytics?.error?'失败':'成功'}`,
`multi_assets:${summary.multi_assets?.error?'失败':'成功'}`,
`factor_library:${summary.factor_library?.error?'失败':'成功'}`,
`onchain:${prevOnchain?.pending?'后台加载中':summary.onchain?.error?'失败':'成功'}`
].join(' | ');
out.textContent=[
'研究总览说明：',
'- 用途：一次性刷新研究页核心模块（分析总览 / 因子库 / 多币种概览 / 链上概览）',
`- 参数：${summary.config.exchange} | ${summary.config.symbol} | ${summary.config.timeframe} | lookback=${summary.config.lookback} | 排除停更=${summary.config.exclude_retired?'是':'否'}`,
`- 模块状态：${moduleStatus}`,
'- 链上概览已改为异步懒加载，主总览会先返回，链上面板随后更新。',
'- 下方为完整原始结果（用于排查与导出）',
'',
JSON.stringify(summary,null,2)
].join('\n');
schedulePlotlyResize(document.getElementById('research')||document);
renderResearchConclusionCard();
notify('研究总览已更新');
loadOnchainOverviewPanel({refresh:false,quiet:true,showLoading:Boolean(prevOnchain?.pending)}).then(d=>{
  const merged={...(researchState.lastOverview||summary),onchain:d};
  researchState.lastOverview=merged;
  const currentOut=getResearchOutputEl();
  if(currentOut){
    const moduleStatus2=[
      `analytics:${merged.analytics?.error?'失败':'成功'}`,
      `multi_assets:${merged.multi_assets?.error?'失败':'成功'}`,
      `factor_library:${merged.factor_library?.error?'失败':'成功'}`,
      `onchain:${d?.error?'失败':d?.cached&&d?.refreshing?'缓存+刷新':'成功'}`
    ].join(' | ');
    currentOut.textContent=[
      '研究总览说明：',
      '- 用途：一次性刷新研究页核心模块（分析总览 / 因子库 / 多币种概览 / 链上概览）',
      `- 参数：${merged.config.exchange} | ${merged.config.symbol} | ${merged.config.timeframe} | lookback=${merged.config.lookback} | 排除停更=${merged.config.exclude_retired?'是':'否'}`,
      `- 模块状态：${moduleStatus2}`,
      '- 链上概览已完成异步更新。',
      '',
      JSON.stringify(merged,null,2)
    ].join('\n');
  }
  renderResearchQuickSummary([
    {label:'多币种覆盖',value:Number(merged.multi_assets?.count||0)},
    {label:'过滤停更',value:merged.config.exclude_retired?'开启':'关闭'},
    {label:'排除币种',value:Number(merged.factor_library?.retired_filter?.excluded_symbols?.length||merged.multi_assets?.retired_filter?.excluded_symbols?.length||0)},
    {label:'链上巨鲸笔数',value:Number(d?.whale_activity?.count||0)},
    {label:'交易所',value:merged.config.exchange},
    {label:'周期',value:merged.config.timeframe},
  ]);
  renderResearchConclusionCard();
}).catch(()=>{});
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
rex.onchange=()=>{loadResearchSymbolOptions(rex.value);renderResearchStatusCards();};
}else{
renderResearchStatusCards();
}
renderResearchStatusCards();
['research-symbol','research-timeframe','research-lookback','research-exclude-retired'].forEach(id=>{
const el=document.getElementById(id);
if(!el)return;
el.addEventListener(el.tagName==='INPUT'?'input':'change',()=>renderResearchStatusCards());
});
const researchSymbolsEl=document.getElementById('research-symbols');
if(researchSymbolsEl)researchSymbolsEl.addEventListener('change',()=>renderResearchStatusCards());

if(o0)o0.onclick=loadResearchOverview;
if(o)o.onclick=()=>{
const ex=getResearchExchange(),s=getResearchSymbol();
const days=Math.max(7,Math.min(365,estimateResearchWindowDays()*2));
const lookback=Math.max(120,Math.min(2000,getResearchLookback()));
const calendarDays=Math.max(7,Math.min(90,estimateResearchWindowDays()));
loadAnalyticsPanel(`/trading/analytics/overview?days=${days}&lookback=${lookback}&calendar_days=${calendarDays}&exchange=${encodeURIComponent(ex)}&symbol=${encodeURIComponent(s)}`);
};
if(b1)b1.onclick=()=>{
const days=Math.max(7,Math.min(365,estimateResearchWindowDays()*2));
loadAnalyticsPanel(`/trading/analytics/performance?days=${days}`);
};
if(b2)b2.onclick=()=>{
const lookback=Math.max(120,Math.min(2000,getResearchLookback()));
loadAnalyticsPanel(`/trading/analytics/risk-dashboard?lookback=${lookback}`);
};
if(b3)b3.onclick=()=>{
const days=Math.max(7,Math.min(90,estimateResearchWindowDays()));
loadAnalyticsPanel(`/trading/analytics/calendar?days=${days}`);
};
if(b4)b4.onclick=()=>{
const ex=getResearchExchange(),s=getResearchSymbol();
loadAnalyticsPanel(`/trading/analytics/microstructure?exchange=${encodeURIComponent(ex)}&symbol=${encodeURIComponent(s)}&depth_limit=80`);
};
if(b5)b5.onclick=()=>{
const hours=Math.max(24,Math.min(24*30,estimateResearchWindowHours()));
const alloc=buildResearchTargetAllocations(3);
loadAnalyticsPanel(`/trading/analytics/equity/rebalance?hours=${hours}&target_alloc=${encodeURIComponent(alloc)}`);
};
if(b6)b6.onclick=()=>{
const ex=getResearchExchange(),s=getResearchSymbol();
loadAnalyticsPanel(`/trading/analytics/community/overview?exchange=${encodeURIComponent(ex)}&symbol=${encodeURIComponent(s)}`);
};
if(b7)b7.onclick=logBehaviorJournal;
if(b8)b8.onclick=()=>{
const days=Math.max(3,Math.min(30,estimateResearchWindowDays()));
loadAnalyticsPanel(`/trading/analytics/behavior/report?days=${days}`);
};
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
setResearchMiniPanelLoading('fama');
const d=await loadFamaPanelWithAutoRefresh({quiet:false,attempt:0,timeoutMs:30000});
if(out)out.textContent=JSON.stringify(d,null,2);
}catch(e){
renderFamaPanel({error:e.message});
if(out)out.textContent=`因子加载失败: ${e.message}`;
}
};

if(m4)m4.onclick=async()=>{
const out=getResearchOutputEl();
try{
const d=await loadFactorLibraryWithAutoRefresh({quiet:false,attempt:0});
if(out)out.textContent=JSON.stringify(d,null,2);
}catch(e){
if(out)out.textContent=`多因子加载失败: ${e.message}`;
}
};

if(m3)m3.onclick=()=>loadOnchainOverviewPanel({refresh:true,quiet:false,showLoading:true});

renderResearchConclusionCard();
}

function formatReplayText(d){if(!d)return'无回放数据';const now=fmtDateTime(new Date());const first=(d.data&&d.data.length)?fmtDateTime(d.data[0].timestamp):'-';const last=(d.data&&d.data.length)?fmtDateTime(d.data[d.data.length-1].timestamp):'-';return[`更新时间: ${now}`,`回放ID: ${d.replay_id||replaySessionId||'-'}`,`进度: ${Number(d.cursor||0)} / ${Number(d.total||0)} ${d.done?'(已结束)':'(进行中)'}`,`本次推进K线: ${(d.data||[]).length}`,`窗口范围: ${first} -> ${last}`].join('\n');}
function bindDataAdvanced(){const rout=document.getElementById('replay-output');const rs=document.getElementById('btn-replay-start'),rn=document.getElementById('btn-replay-next'),rp=document.getElementById('btn-replay-stop');if(rs)rs.onclick=async()=>{try{const ex=document.getElementById('data-exchange').value,s=document.getElementById('data-symbol').value,tf=document.getElementById('data-timeframe').value,st=document.getElementById('replay-start-time').value,et=document.getElementById('replay-end-time').value,w=Number(document.getElementById('replay-window').value||300);const payload={exchange:ex,symbol:s,timeframe:tf,start_time:st?new Date(st).toISOString():null,end_time:et?new Date(et).toISOString():null,window:w,speed:1};const d=await api('/data/replay/start',{method:'POST',body:JSON.stringify(payload)});replaySessionId=d.replay_id||'';if(rout)rout.textContent=formatReplayText({...d,data:d.data||[]});notify('回放会话已启动');}catch(e){if(rout)rout.textContent=`回放启动失败: ${e.message}`;}};if(rn)rn.onclick=async()=>{try{if(!replaySessionId){notify('请先启动回放',true);return;}const steps=Number(document.getElementById('replay-steps').value||60);const d=await api(`/data/replay/${encodeURIComponent(replaySessionId)}/next?steps=${steps}`);if(rout)rout.textContent=formatReplayText({...d,replay_id:replaySessionId});if(d.data?.length){marketDataState.bars=cropBars(mergeBars([],d.data));renderKlineChart(false);} }catch(e){if(rout)rout.textContent=`回放推进失败: ${e.message}`;}};if(rp)rp.onclick=async()=>{try{if(!replaySessionId)return;const d=await api(`/data/replay/${encodeURIComponent(replaySessionId)}`,{method:'DELETE'});if(rout)rout.textContent=`回放已停止\n会话: ${d.replay_id||replaySessionId}\n时间: ${fmtDateTime(new Date())}`;replaySessionId='';notify('回放已停止');}catch(e){if(rout)rout.textContent=`回放停止失败: ${e.message}`;}};}

function bindBacktest(){
initBacktestComparePicker();
bindBacktestProtectionControls();
const f=document.getElementById('backtest-form');
if(f)f.onsubmit=async e=>{
e.preventDefault();
try{
notify('回测运行中...');
const st=await ensureSelectedBacktestStrategy(),s=document.getElementById('backtest-symbol').value,tf=document.getElementById('backtest-timeframe').value,c=document.getElementById('backtest-capital').value,sd=document.getElementById('backtest-start-date').value,ed=document.getElementById('backtest-end-date').value,cr=0.0004,sb=2;
const customParams=getBacktestCustomParams();
let u=`/backtest/${customParams&&Object.keys(customParams).length?'run_custom':'run'}?strategy=${encodeURIComponent(st)}&symbol=${encodeURIComponent(s)}&timeframe=${encodeURIComponent(tf)}&initial_capital=${encodeURIComponent(c)}&commission_rate=${encodeURIComponent(cr)}&slippage_bps=${encodeURIComponent(sb)}&include_series=true`;
if(sd)u+=`&start_date=${encodeURIComponent(sd)}`;
if(ed)u+=`&end_date=${encodeURIComponent(ed)}`;
if(customParams&&Object.keys(customParams).length)u+=`&params_json=${encodeURIComponent(JSON.stringify(customParams))}`;
u=appendBacktestProtectionParams(u);
const runTimeoutMs=estimateBacktestRunTimeoutMs(st, customParams);
renderBacktest(await api(u,{method:'POST',timeoutMs:runTimeoutMs}));
notify('回测完成');
}catch(err){notify(`回测失败: ${err.message}`,true);}
};
const b1=document.getElementById('btn-backtest-compare');
if(b1)b1.onclick=async()=>{
try{
renderBacktestExtraLoading('多策略对比运行中');
await ensureSelectedBacktestStrategy();
const s=document.getElementById('backtest-symbol').value,tf=document.getElementById('backtest-timeframe').value,c=document.getElementById('backtest-capital').value,sd=document.getElementById('backtest-start-date')?.value||'',ed=document.getElementById('backtest-end-date')?.value||'',cr=0.0004,sb=2;
const chosenStrategies=getSelectedBacktestCompareStrategies();
if(!chosenStrategies.length){notify('请至少勾选一个策略',true);return;}
const objective=String(document.getElementById('backtest-opt-objective')?.value||'total_return');
const maxTrials=Math.max(8,Math.min(512,parseInt(document.getElementById('backtest-opt-trials')?.value||'96',10)||96));
const compareTimeoutMs=Math.max(30000,Math.min(8*60*1000, chosenStrategies.length*maxTrials*220 + 25000));
let cu=`/backtest/compare?strategies=${encodeURIComponent(chosenStrategies.join(','))}&symbol=${encodeURIComponent(s)}&timeframe=${tf}&initial_capital=${c}&commission_rate=${cr}&slippage_bps=${sb}&pre_optimize=true&optimize_objective=${encodeURIComponent(objective)}&optimize_max_trials=${maxTrials}`;
if(sd)cu+=`&start_date=${encodeURIComponent(sd)}`;
if(ed)cu+=`&end_date=${encodeURIComponent(ed)}`;
cu=appendBacktestProtectionParams(cu);
const d=await api(cu,{method:'POST',timeoutMs:compareTimeoutMs});
backtestUIState.lastCompare=d||null;
renderBacktestCompareOutput(d);
notify('多策略对比完成');
}catch(err){renderBacktestExtraError(err);notify(`多策略对比失败: ${err.message}`,true);}
};
const b2=document.getElementById('btn-backtest-optimize');
if(b2)b2.onclick=async()=>{
try{
const selectedSt=await ensureSelectedBacktestStrategy();
const displayedSt=String(backtestUIState?.lastRenderedBacktest?.strategy||'').trim();
const st=displayedSt||selectedSt;
const s=document.getElementById('backtest-symbol').value,tf=document.getElementById('backtest-timeframe').value,c=document.getElementById('backtest-capital').value,sd=document.getElementById('backtest-start-date')?.value||'',ed=document.getElementById('backtest-end-date')?.value||'',cr=0.0004,sb=2;
renderBacktestExtraLoading(`参数优化运行中（${st}${displayedSt&&displayedSt!==selectedSt?'，来自当前展示':'，来自下拉选择'}）`);
const objective=String(document.getElementById('backtest-opt-objective')?.value||'total_return');
const maxTrials=Math.max(8,Math.min(1024,parseInt(document.getElementById('backtest-opt-trials')?.value||'96',10)||96));
let ou=`/backtest/optimize?strategy=${st}&symbol=${encodeURIComponent(s)}&timeframe=${tf}&initial_capital=${c}&commission_rate=${cr}&slippage_bps=${sb}&objective=${encodeURIComponent(objective)}&max_trials=${maxTrials}&include_all_trials=true`;
if(sd)ou+=`&start_date=${encodeURIComponent(sd)}`;
if(ed)ou+=`&end_date=${encodeURIComponent(ed)}`;
ou=appendBacktestProtectionParams(ou);
const d=await api(ou,{method:'POST',timeoutMs:90000});
renderBacktestOptimizeOutput(d);
notify('参数优化完成');
}catch(err){renderBacktestExtraError(err);notify(`参数优化失败: ${err.message}`,true);}
};
const b3=document.getElementById('btn-backtest-export');
if(b3)b3.onclick=()=>{
const st=String(document.getElementById('backtest-strategy')?.value||'').trim(),s=document.getElementById('backtest-symbol').value,tf=document.getElementById('backtest-timeframe').value,c=document.getElementById('backtest-capital').value,sd=document.getElementById('backtest-start-date')?.value||'',ed=document.getElementById('backtest-end-date')?.value||'',cr=0.0004,sb=2,fmt=document.getElementById('backtest-export-format')?.value||'xlsx';
if(!st){notify('回测策略目录尚未加载完成',true);return;}
let eu=`${API_BASE}/backtest/export?strategy=${st}&symbol=${encodeURIComponent(s)}&timeframe=${tf}&initial_capital=${c}&commission_rate=${cr}&slippage_bps=${sb}&format=${fmt}`;
if(sd)eu+=`&start_date=${encodeURIComponent(sd)}`;
if(ed)eu+=`&end_date=${encodeURIComponent(ed)}`;
eu=appendBacktestProtectionParams(eu);
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

async function init(){initTabs();initClock();initEquity();bindTrade();bindOrderView();bindLiveTradeReview();bindData();bindDataAdvanced();bindBacktest();bindArbitragePage();bindNotificationCenter();bindAudit();bindStrategyOps();bindStrategyAdvanced();bindResearchPanel();bindResearchPresets();bindResearchSentiment();bindModeControls();bindAccountControls();initWebSocket();document.addEventListener('visibilitychange',()=>{if(document.hidden){releaseAllSharedPollGroups();closeWebSocketClient();}else{canRunSharedPolling('status');const group=sharedPollGroupForTab(getActiveTabName());if(group)canRunSharedPolling(group);initWebSocket();}});renderStrategyConsolePanel();renderResearchStatusCards();state.bootCompleted=true;state.bootFailed=false;loadSystemStatus().catch(err=>console.warn('initial loadSystemStatus failed:',err?.message||err));setTimeout(()=>{ensureTabLoaded(getActiveTabName(),{force:true}).catch(err=>console.error('initial tab load failed:',err));},0);
// Status polling is lightweight but user-visible; keep it independent from heavier dashboard batches
setInterval(()=>{if(document.hidden)return;if(!canRunSharedPolling('status'))return;loadSystemStatus();},20000);
setInterval(()=>{
  if(document.hidden)return;
  if(state.wsConnected)return;
  const tab=getActiveTabName();
  const group=sharedPollGroupForTab(tab);
  if(group&&!canRunSharedPolling(group))return;
  if(isTabBootstrapping(tab))return;
  if(tab==='dashboard')refreshDashboardCore();
  else if(tab==='trading')refreshTradingCore();
  else if(tab==='strategies')Promise.allSettled([loadStrategies(),loadStrategySummary()]);
  else if(tab==='ai-research')refreshAiResearchModules();
  else if(tab==='ai-agent')refreshAiResearchModules();
},CORE_TAB_POLL_INTERVAL_MS);
setInterval(()=>{
  if(document.hidden)return;
  const tab=getActiveTabName();
  if(tab!=='dashboard'&&tab!=='trading')return;
  const group=sharedPollGroupForTab(tab);
  if(group&&!canRunSharedPolling(group))return;
  if(!state.wsConnected||isTabBootstrapping(tab,18000))return;
  const now=Date.now();
  const lastByTab=state.lastWsBackfillAtByTab||(state.lastWsBackfillAtByTab={});
  const last=Number(lastByTab[tab]||0);
  if(now-last<30000)return;
  lastByTab[tab]=now;
  if(tab==='dashboard')refreshDashboardCore();
  else if(tab==='trading')refreshTradingCore();
},WS_BACKFILL_INTERVAL_MS);
setInterval(()=>{
  if(document.hidden)return;
  const tab=getActiveTabName();
  const group=sharedPollGroupForTab(tab);
  if(group&&!canRunSharedPolling(group))return;
  if(isTabBootstrapping(tab,18000))return;
  if(tab==='dashboard')scheduleDashboardSecondaryLoads(0);
  else if(tab==='trading')scheduleTradingSecondaryLoads(0);
  else if(tab==='strategies')Promise.allSettled([loadStrategyHealth()]);
},SECONDARY_TAB_POLL_INTERVAL_MS);}

window.cancelOrder=cancelOrder;window.cancelConditional=cancelConditional;window.registerStrategy=registerStrategy;window.toggleStrategy=toggleStrategy;window.saveAllocation=saveAllocation;window.openEditor=openEditor;window.compareLive=compareLive;window.openStrategyEditor=openEditor;window.compareStrategyLive=compareLive;window.previewCompareStrategyByRank=previewCompareStrategyByRank;window.registerCompareStrategyByRank=registerCompareStrategyByRank;window.registerOptimizeBestAsNewStrategyInstance=registerOptimizeBestAsNewStrategyInstance;window.registerOptimizeTrialByRank=registerOptimizeTrialByRank;window.editNotifyRule=editNotifyRule;window.toggleNotifyRule=toggleNotifyRule;window.deleteNotifyRule=deleteNotifyRule;window.openBacktestWithSpec=openBacktestWithSpec;window.registerArbitrageStrategy=registerArbitrageStrategy;window.jumpToBacktestFromArbitrage=jumpToBacktestFromArbitrage;window.scanArbitragePairsRanking=scanArbitragePairsRanking;window.applyArbitragePairCandidate=applyArbitragePairCandidate;
window.addEventListener('error',e=>{
const runtimeErr=e?.error;
if(!runtimeErr){
  const src=String(e?.target?.src||e?.target?.href||e?.filename||'').trim();
  if(src && !src.endsWith('/favicon.ico')) console.warn(`resource load failed: ${src}`);
  return;
}
markBootFailure(runtimeErr);
});
window.addEventListener('unhandledrejection',e=>{markBootFailure(e?.reason||new Error('未处理的Promise异常'));});
init().catch(markBootFailure);

// ── 策略实时监控面板 ──────────────────────────────────────────────────────────

let _monitorTimer = null;
let _monitorCurrentName = '';

async function openStrategyMonitor(name) {
    const panel = document.getElementById('strategy-monitor-panel');
    if (!panel) return;
    _monitorCurrentName = String(name || '');
    panel.style.display = 'block';
    setTimeout(() => panel.scrollIntoView({ behavior: 'smooth', block: 'nearest' }), 80);
    await _loadMonitorData(name);
    if (_monitorTimer) clearInterval(_monitorTimer);
    _monitorTimer = setInterval(() => {
        if (_monitorCurrentName) _loadMonitorData(_monitorCurrentName).catch(() => {});
    }, 30000);
}

function closeStrategyMonitor() {
    const panel = document.getElementById('strategy-monitor-panel');
    if (panel) panel.style.display = 'none';
    if (_monitorTimer) { clearInterval(_monitorTimer); _monitorTimer = null; }
    _monitorCurrentName = '';
}

async function refreshStrategyMonitor() {
    if (_monitorCurrentName) await _loadMonitorData(_monitorCurrentName);
}

async function _loadMonitorData(name) {
    const badge = document.getElementById('monitor-status-badge');
    const title = document.getElementById('monitor-panel-title');
    if (badge) badge.textContent = '加载中...';

    let data;
    try {
        data = await api(`/strategies/${encodeURIComponent(name)}/monitor-data?bars=200`);
    } catch (e) {
        if (badge) {
            badge.textContent = '加载失败';
            badge.style.background = '#7f1d1d';
            badge.style.color = '#fca5a5';
        }
        return;
    }

    if (title) title.textContent = `策略监控 — ${esc(name)}`;
    if (badge) {
        badge.textContent = data.is_running ? '运行中' : '未运行';
        badge.style.background = data.is_running ? '#14532d' : 'var(--bg-3)';
        badge.style.color     = data.is_running ? '#86efac' : 'var(--text-muted)';
    }

    const metricsEl = document.getElementById('monitor-metrics-row');
    if (metricsEl) {
        const m = data.metrics || {};
        const pairMetrics = (data && typeof data.pair_metrics === 'object' && data.pair_metrics) ? data.pair_metrics : null;
        const pnlColor = c => (c || 0) >= 0 ? '#4ade80' : '#f87171';
        const pct = v => v != null ? (v > 0 ? '+' : '') + v.toFixed(2) + '%' : 'N/A';
        const monitorCards = [
            ['资本基数',   m.equity_base    != null ? m.equity_base.toFixed(2) + ' U' : '--'],
            ['已实现盈亏', m.realized_pnl   != null ? `<span style="color:${pnlColor(m.realized_pnl)}">${m.realized_pnl.toFixed(2)} U</span>` : '--'],
            ['浮动盈亏',   m.unrealized_pnl != null ? `<span style="color:${pnlColor(m.unrealized_pnl)}">${m.unrealized_pnl.toFixed(2)} U</span>` : '--'],
            ['总收益',     m.total_pnl      != null ? `<span style="color:${pnlColor(m.total_pnl)}">${m.total_pnl.toFixed(2)} U (${pct(m.return_pct)})</span>` : '--'],
            ['交易次数',   m.trade_count    != null ? m.trade_count : '--'],
            ['胜率',       m.win_rate       != null ? m.win_rate.toFixed(1) + '%' : '--'],
        ];
        if (String(data?.portfolio_mode || '').trim() === 'pairs_spread_dual_leg' && pairMetrics) {
            const fmtMaybe = (value, digits = 4) => Number.isFinite(Number(value)) ? Number(value).toFixed(digits) : '--';
            monitorCards.push(
                ['副腿', esc(String(data?.pair_symbol || '--'))],
                ['对冲比', fmtMaybe(pairMetrics.hedge_ratio_last, 4)],
                ['Z-Score / 偏置', `${fmtMaybe(pairMetrics.z_score_last, 2)} / ${esc(String(pairMetrics.signal_bias || '--'))}`],
                ['价差 / 相关', `${fmtMaybe(pairMetrics.spread_last, 4)} / ${esc(String(pairMetrics.pair_regime || '--'))}`],
            );
        }
        metricsEl.innerHTML = monitorCards.map(([label, val]) =>
            `<div class="monitor-metric"><div class="monitor-metric-label">${label}</div><div class="monitor-metric-val">${val}</div></div>`
        ).join('');
    }

    _renderMonitorChart(data);

    const posEl = document.getElementById('monitor-positions-row');
    if (posEl) {
        if (!data.positions || !data.positions.length) {
            posEl.innerHTML = '<div style="font-size:11px;color:var(--text-muted);padding:4px 0">当前无持仓</div>';
        } else {
            posEl.innerHTML =
                '<div style="font-size:11px;color:var(--text-muted);margin-bottom:4px">当前持仓</div>' +
                data.positions.map(p => {
                    const c = p.unrealized_pnl >= 0 ? '#4ade80' : '#f87171';
                    return `<div class="monitor-position-row"><span class="monitor-pos-side ${p.side === 'long' ? 'pos-long' : 'pos-short'}">${p.side === 'long' ? '多' : '空'}</span><span>${esc(p.symbol)}</span><span>入场 ${p.entry_price.toFixed(4)}</span><span>现价 ${p.current_price.toFixed(4)}</span><span>数量 ${p.quantity.toFixed(6)}</span><span style="color:${c}">浮盈 ${p.unrealized_pnl.toFixed(2)} U (${(p.unrealized_pnl_pct*100).toFixed(2)}%)</span></div>`;
                }).join('');
        }
    }
}

function monitorChartToMs(value) {
    if (value instanceof Date) return Number.isFinite(value.getTime()) ? value.getTime() : NaN;
    if (typeof value === 'number') {
        const d = new Date(value > 1e12 ? value : value * 1000);
        return Number.isFinite(d.getTime()) ? d.getTime() : NaN;
    }
    const raw = String(value ?? '').trim();
    if (!raw) return NaN;
    const text = raw.replace(' ', 'T');
    const normalized = /(?:[zZ]|[+\-]\d{2}:\d{2})$/.test(text) ? text : `${text}Z`;
    const d = new Date(normalized);
    return Number.isFinite(d.getTime()) ? d.getTime() : NaN;
}

function monitorChartLocalIso(value) {
    const ms = monitorChartToMs(value);
    return Number.isFinite(ms) ? klineLocalIso(ms) : '';
}

function normalizeMonitorOhlcvBar(bar) {
    if (!bar || typeof bar !== 'object') return null;
    const ms = monitorChartToMs(bar.t);
    const t = Number.isFinite(ms) ? klineLocalIso(ms) : '';
    const o = Number(bar.o);
    const h = Number(bar.h);
    const l = Number(bar.l);
    const c = Number(bar.c);
    if (!t || ![o, h, l, c].every(Number.isFinite)) return null;
    const next = { ...bar, t, ms, o, h, l, c };
    if (bar.v != null && Number.isFinite(Number(bar.v))) next.v = Number(bar.v);
    ['pair_close', 'spread', 'z_score', 'hedge_ratio'].forEach((key) => {
        if (bar[key] != null && Number.isFinite(Number(bar[key]))) next[key] = Number(bar[key]);
    });
    return next;
}

function normalizeMonitorEquityPoint(point) {
    if (!point || typeof point !== 'object') return null;
    const ms = monitorChartToMs(point.t);
    const t = Number.isFinite(ms) ? klineLocalIso(ms) : '';
    const v = Number(point.v);
    if (!t || !Number.isFinite(v)) return null;
    return { ...point, t, ms, v };
}

function findMonitorBarForSignal(bars, signalMs, signalPrice, timeframeMs) {
    if (!Array.isArray(bars) || !bars.length || !Number.isFinite(signalMs)) return null;
    const idx = bars.findIndex((bar) => signalMs <= Number(bar?.ms));
    const nearby = [];
    const pushBar = (bar) => {
        if (!bar || nearby.includes(bar)) return;
        nearby.push(bar);
    };
    if (idx === -1) {
        pushBar(bars[bars.length - 2]);
        pushBar(bars[bars.length - 1]);
    } else {
        pushBar(bars[idx - 1]);
        pushBar(bars[idx]);
        pushBar(bars[idx + 1]);
    }
    const priceNum = Number(signalPrice);
    if (Number.isFinite(priceNum)) {
        const rangeMatches = nearby
            .filter((bar) => priceNum >= Math.min(bar.l, bar.h) && priceNum <= Math.max(bar.l, bar.h))
            .sort((a, b) => Math.abs(signalMs - a.ms) - Math.abs(signalMs - b.ms));
        if (rangeMatches.length) return rangeMatches[0];
    }
    for (let i = 0; i < bars.length; i += 1) {
        const bar = bars[i];
        const startMs = Number(bar?.ms);
        const endMs = i < bars.length - 1 ? Number(bars[i + 1]?.ms) : startMs + timeframeMs;
        if (Number.isFinite(startMs) && Number.isFinite(endMs) && signalMs >= startMs && signalMs < endMs) return bar;
    }
    return nearby
        .filter(Boolean)
        .sort((a, b) => Math.abs(signalMs - a.ms) - Math.abs(signalMs - b.ms))[0] || bars[0];
}

function getMonitorSignalPlotPrice(signalType, bar, rawPrice) {
    const priceNum = Number(rawPrice);
    if (!bar) return Number.isFinite(priceNum) ? priceNum : null;
    const low = Math.min(Number(bar.l), Number(bar.h));
    const high = Math.max(Number(bar.l), Number(bar.h));
    if (!Number.isFinite(low) || !Number.isFinite(high)) return Number.isFinite(priceNum) ? priceNum : null;
    const fallback = Number.isFinite(Number(bar.c)) ? Number(bar.c) : (low + high) / 2;
    const bounded = Number.isFinite(priceNum) ? Math.min(high, Math.max(low, priceNum)) : fallback;
    let bodyLow = Math.min(Number(bar.o), Number(bar.c));
    let bodyHigh = Math.max(Number(bar.o), Number(bar.c));
    if (!(bodyHigh > bodyLow)) {
        const totalSpan = Math.max(high - low, Math.abs(fallback) * 0.0008, 0.01);
        const half = totalSpan * 0.18;
        bodyLow = Math.max(low, fallback - half);
        bodyHigh = Math.min(high, fallback + half);
    }
    if (bounded >= bodyLow && bounded <= bodyHigh) return bounded;
    const kind = String(signalType || '').trim().toLowerCase();
    const frac = ['buy', 'close_short'].includes(kind) ? 0.34 : 0.66;
    return bodyLow + (bodyHigh - bodyLow) * frac;
}

function formatMonitorSignalTime(value) {
    const text = String(value || '').trim();
    return text ? text.replace('T', ' ') : '--';
}

function _renderMonitorChart(data) {
    const chartEl = document.getElementById('strategy-monitor-chart');
    if (!chartEl || typeof Plotly === 'undefined') return;
    const showChartMessage = (msg) => {
        clearPlotlyHost(chartEl);
        chartEl.dataset.monitorPlotMode = 'message';
        chartEl.innerHTML = `<div style="color:var(--text-muted);padding:20px;text-align:center">${esc(msg)}</div>`;
    };

    const ohlcvRaw = Array.isArray(data?.ohlcv) ? data.ohlcv : [];
    const signals  = Array.isArray(data?.signals) ? data.signals : [];
    const equityRaw = Array.isArray(data?.equity) ? data.equity : [];
    const pairMetrics = (data && typeof data.pair_metrics === 'object' && data.pair_metrics) ? data.pair_metrics : {};
    const timeframeMs = Math.max(60, timeframeSeconds(data?.timeframe || '1h')) * 1000;
    const ohlcv = ohlcvRaw.map(normalizeMonitorOhlcvBar).filter(Boolean);
    const equityPts = equityRaw
        .map(normalizeMonitorEquityPoint)
        .filter(Boolean);
    const equityBaseRaw = Number(data?.metrics?.equity_base);
    const equityBase = Number.isFinite(equityBaseRaw)
        ? equityBaseRaw
        : (equityPts.length ? Number(equityPts[0].v) : 0);
    const equitySeries = equityPts.map((point) => {
        const equity = Number(point.v);
        const pnl = equity - equityBase;
        const returnPct = equityBase > 0 ? (pnl / equityBase) * 100 : 0;
        return {
            ...point,
            equity,
            pnl,
            returnPct,
        };
    });
    const latestNetPnl = equitySeries.length ? Number(equitySeries[equitySeries.length - 1].pnl || 0) : 0;
    const equityLineColor = latestNetPnl >= 0 ? '#4ade80' : '#f87171';
    const equityFillColor = latestNetPnl >= 0 ? 'rgba(74,222,128,0.12)' : 'rgba(248,113,113,0.12)';
    const plottedSignals = signals
        .map((sig) => {
            const signalMs = monitorChartToMs(sig?.t);
            const signalTime = Number.isFinite(signalMs) ? klineLocalIso(signalMs) : '';
            const rawPrice = Number(sig?.price);
            const bar = findMonitorBarForSignal(ohlcv, signalMs, rawPrice, timeframeMs);
            const plotTime = bar?.t || signalTime;
            const plotPrice = getMonitorSignalPlotPrice(sig?.type, bar, rawPrice);
            return {
                ...sig,
                t: signalTime,
                raw_t: signalTime,
                plot_t: plotTime,
                raw_price: Number.isFinite(rawPrice) ? rawPrice : null,
                plot_price: plotPrice,
                aligned_bar_time: bar?.t || '',
                aligned: !!bar && !!signalTime && plotTime !== signalTime,
            };
        })
        .filter((sig) => sig.plot_t && Number.isFinite(Number(sig.plot_price)));
    const hasPrice = ohlcv.length > 0;
    const hasEquity = equitySeries.length > 0;
    const isPairsMonitor =
        String(data?.portfolio_mode || '').trim() === 'pairs_spread_dual_leg' ||
        ohlcv.some((b) => Number.isFinite(Number(b?.pair_close)) || Number.isFinite(Number(b?.spread)) || Number.isFinite(Number(b?.z_score)));

    if (!hasPrice && !hasEquity) {
        showChartMessage('暂无可绘制的监控数据');
        return;
    }
    const nextMode = !hasPrice
        ? 'equity-only'
        : (isPairsMonitor ? (hasEquity ? 'pairs-price-equity' : 'pairs-price-only') : (hasEquity ? 'price-equity' : 'price-only'));
    const prevMode = chartEl.dataset.monitorPlotMode || '';
    if (prevMode && prevMode !== nextMode) clearPlotlyHost(chartEl);
    preparePlotlyHost(chartEl);
    chartEl.style.height = isPairsMonitor ? '640px' : '520px';

    // Equity-only fallback: avoid constructing multi-axis candlestick layout when price bars are absent.
    if (!hasPrice && hasEquity) {
        try {
            Plotly.react(chartEl, [{
                type: 'scatter',
                mode: 'lines',
                name: '净收益曲线',
                x: equitySeries.map((e) => e.t),
                y: equitySeries.map((e) => e.pnl),
                customdata: equitySeries.map((e) => [e.equity, e.returnPct]),
                line: { color: equityLineColor, width: 1.8 },
                fill: 'tozeroy',
                fillcolor: equityFillColor,
                hovertemplate: '净收益: %{y:.2f} U<br>收益率: %{customdata[1]:.2f}%<br>绝对权益: %{customdata[0]:.2f} U<br>时间: %{x}<extra></extra>',
            }], {
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                font: { color: '#dfe9f7', size: 11 },
                margin: { t: 16, b: 40, l: 60, r: 40 },
                xaxis: { ...plotlyTimeAxis(), domain: [0, 1], rangeslider: { visible: false } },
                yaxis: {
                    gridcolor: '#283242',
                    zeroline: true,
                    zerolinecolor: '#4b5563',
                    zerolinewidth: 1,
                    title: { text: '净收益(U)', font: { size: 10 } },
                },
                showlegend: true,
                legend: { orientation: 'h', y: 1.04, x: 0, font: { size: 10 } },
            }, {
                responsive: true, displayModeBar: true,
                modeBarButtonsToRemove: ['select2d', 'lasso2d'], displaylogo: false,
            });
            chartEl.dataset.monitorPlotMode = nextMode;
        } catch (e) {
            showChartMessage(`图表渲染失败: ${e.message}`);
        }
        return;
    }

    const candleTrace = {
        type: 'candlestick',
        name: data.symbol || '',
        x:     ohlcv.map(b => b.t),
        open:  ohlcv.map(b => b.o),
        high:  ohlcv.map(b => b.h),
        low:   ohlcv.map(b => b.l),
        close: ohlcv.map(b => b.c),
        increasing: { line: { color: '#4ade80', width: 1 }, fillcolor: '#4ade80' },
        decreasing: { line: { color: '#f87171', width: 1 }, fillcolor: '#f87171' },
        xaxis: 'x', yaxis: 'y',
    };

    const buySigs  = plottedSignals.filter(s => ['buy',  'close_short'].includes(String(s.type || '').toLowerCase()));
    const sellSigs = plottedSignals.filter(s => ['sell', 'close_long' ].includes(String(s.type || '').toLowerCase()));
    const buildSignalHover = (sig) => {
        const parts = [
            `${sig.type} | 强度 ${(Number(sig.strength || 0)).toFixed(2)}`,
            sig.raw_price != null ? `成交价 ${Number(sig.raw_price).toFixed(4)}` : null,
            sig.stop_loss != null ? `SL ${Number(sig.stop_loss).toFixed(4)}` : null,
            sig.take_profit != null ? `TP ${Number(sig.take_profit).toFixed(4)}` : null,
        ];
        if (sig.aligned) {
            parts.push(`信号时间 ${formatMonitorSignalTime(sig.raw_t)}`);
            parts.push(`对齐K线 ${formatMonitorSignalTime(sig.plot_t)}`);
        }
        return parts.filter(Boolean).join('<br>');
    };

    const buyMarker = {
        type: 'scatter', mode: 'markers', name: '买入/平空',
        x: buySigs.map(s => s.plot_t),
        y: buySigs.map(s => s.plot_price),
        marker: { symbol: 'triangle-up', size: buySigs.map(s => 8 + (s.strength || 0.5) * 8), color: '#4ade80', line: { color: '#fff', width: 1 } },
        text: buySigs.map(buildSignalHover),
        hovertemplate: '%{text}<extra></extra>',
        xaxis: 'x', yaxis: 'y',
    };

    const sellMarker = {
        type: 'scatter', mode: 'markers', name: '卖出/平多',
        x: sellSigs.map(s => s.plot_t),
        y: sellSigs.map(s => s.plot_price),
        marker: { symbol: 'triangle-down', size: sellSigs.map(s => 8 + (s.strength || 0.5) * 8), color: '#f87171', line: { color: '#fff', width: 1 } },
        text: sellSigs.map(buildSignalHover),
        hovertemplate: '%{text}<extra></extra>',
        xaxis: 'x', yaxis: 'y',
    };

    const equityTrace = {
        type: 'scatter', mode: 'lines', name: '净收益曲线',
        x: equitySeries.map(e => e.t),
        y: equitySeries.map(e => e.pnl),
        customdata: equitySeries.map((e) => [e.equity, e.returnPct]),
        line: { color: equityLineColor, width: 1.6 },
        fill: 'tozeroy', fillcolor: equityFillColor,
        hovertemplate: '净收益: %{y:.2f} U<br>收益率: %{customdata[1]:.2f}%<br>绝对权益: %{customdata[0]:.2f} U<br>时间: %{x}<extra></extra>',
        xaxis: 'x', yaxis: 'y2',
    };

    if (isPairsMonitor) {
        const pairPoints = ohlcv
            .map((b) => ({ t: b.t, v: Number(b.pair_close) }))
            .filter((p) => Number.isFinite(p.v));
        const spreadPoints = ohlcv
            .map((b) => ({ t: b.t, v: Number(b.spread) }))
            .filter((p) => Number.isFinite(p.v));
        const zScorePoints = ohlcv
            .map((b) => ({ t: b.t, v: Number(b.z_score) }))
            .filter((p) => Number.isFinite(p.v));
        const hedgeRatioPoints = ohlcv
            .map((b) => ({ t: b.t, v: Number(b.hedge_ratio) }))
            .filter((p) => Number.isFinite(p.v));
        const hasPairPrice = pairPoints.length > 0;
        const hasSpread = spreadPoints.length > 0;
        const hasZScore = zScorePoints.length > 0;
        const hasHedgeRatio = hedgeRatioPoints.length > 0;
        const entryZ = Math.abs(Number(pairMetrics?.entry_z_score));
        const exitZ = Math.abs(Number(pairMetrics?.exit_z_score));

        const traces = [candleTrace, buyMarker, sellMarker];
        if (hasPairPrice) {
            traces.push({
                type: 'scatter',
                mode: 'lines',
                name: `副腿价格${data?.pair_symbol ? ` (${data.pair_symbol})` : ''}`,
                x: pairPoints.map((p) => p.t),
                y: pairPoints.map((p) => p.v),
                line: { color: '#ffb15f', width: 1.35, dash: 'dash' },
                xaxis: 'x',
                yaxis: 'y4',
                hovertemplate: '副腿: %{y:.6f}<br>时间: %{x}<extra></extra>',
            });
        }
        if (hasSpread) {
            traces.push({
                type: 'scatter',
                mode: 'lines',
                name: '价差',
                x: spreadPoints.map((p) => p.t),
                y: spreadPoints.map((p) => p.v),
                line: { color: '#34d399', width: 1.5 },
                xaxis: 'x',
                yaxis: 'y2',
                hovertemplate: '价差: %{y:.6f}<br>时间: %{x}<extra></extra>',
            });
        }
        if (hasZScore) {
            traces.push({
                type: 'scatter',
                mode: 'lines',
                name: 'Z-Score',
                x: zScorePoints.map((p) => p.t),
                y: zScorePoints.map((p) => p.v),
                line: { color: '#a78bfa', width: 1.5 },
                xaxis: 'x',
                yaxis: 'y3',
                hovertemplate: 'Z-Score: %{y:.4f}<br>时间: %{x}<extra></extra>',
            });
            if (Number.isFinite(entryZ) && entryZ > 0) {
                const zX = zScorePoints.map((p) => p.t);
                traces.push({
                    type: 'scatter',
                    mode: 'lines',
                    name: '+入场阈值',
                    x: zX,
                    y: zX.map(() => entryZ),
                    line: { color: '#f59e0b', width: 1, dash: 'dot' },
                    xaxis: 'x',
                    yaxis: 'y3',
                    hoverinfo: 'skip',
                });
                traces.push({
                    type: 'scatter',
                    mode: 'lines',
                    name: '-入场阈值',
                    x: zX,
                    y: zX.map(() => -entryZ),
                    line: { color: '#f59e0b', width: 1, dash: 'dot' },
                    xaxis: 'x',
                    yaxis: 'y3',
                    hoverinfo: 'skip',
                });
            }
            if (Number.isFinite(exitZ) && exitZ >= 0) {
                const zX = zScorePoints.map((p) => p.t);
                traces.push({
                    type: 'scatter',
                    mode: 'lines',
                    name: '+离场阈值',
                    x: zX,
                    y: zX.map(() => exitZ),
                    line: { color: '#94a3b8', width: 1, dash: 'dash' },
                    xaxis: 'x',
                    yaxis: 'y3',
                    hoverinfo: 'skip',
                });
                traces.push({
                    type: 'scatter',
                    mode: 'lines',
                    name: '-离场阈值',
                    x: zX,
                    y: zX.map(() => -exitZ),
                    line: { color: '#94a3b8', width: 1, dash: 'dash' },
                    xaxis: 'x',
                    yaxis: 'y3',
                    hoverinfo: 'skip',
                });
            }
        }
        if (hasEquity) {
            traces.push({
                ...equityTrace,
                yaxis: 'y5',
            });
        }
        if (hasHedgeRatio) {
            traces.push({
                type: 'scatter',
                mode: 'lines',
                name: '对冲比',
                x: hedgeRatioPoints.map((p) => p.t),
                y: hedgeRatioPoints.map((p) => p.v),
                line: { color: '#f97316', width: 1.4 },
                xaxis: 'x',
                yaxis: hasEquity ? 'y6' : 'y5',
                hovertemplate: '对冲比: %{y:.6f}<br>时间: %{x}<extra></extra>',
            });
        }

        const layout = {
            paper_bgcolor: 'transparent',
            plot_bgcolor: 'transparent',
            font: { color: '#dfe9f7', size: 11 },
            margin: { t: 16, b: 40, l: 60, r: 68 },
            xaxis: { ...plotlyTimeAxis(), domain: [0, 1], rangeslider: { visible: false } },
            yaxis: { domain: [0.56, 1], gridcolor: '#283242', title: { text: '主腿价格', font: { size: 10 } } },
            yaxis2: { domain: [0.28, 0.50], anchor: 'x', gridcolor: '#283242', title: { text: '价差', font: { size: 10 } } },
            yaxis3: { overlaying: 'y2', side: 'right', position: 0.98, showgrid: false, title: { text: 'Z-Score', font: { size: 10 } } },
            yaxis4: { overlaying: 'y', side: 'right', position: 0.92, showgrid: false, title: { text: '副腿价格', font: { size: 10 } } },
            yaxis5: {
                domain: [0, 0.22],
                anchor: 'x',
                gridcolor: '#283242',
                zeroline: hasEquity,
                zerolinecolor: '#4b5563',
                zerolinewidth: 1,
                title: { text: hasEquity ? '净收益(U)' : '对冲比', font: { size: 10 } },
            },
            showlegend: true,
            legend: { orientation: 'h', y: 1.06, x: 0, font: { size: 10 } },
        };
        if (hasEquity && hasHedgeRatio) {
            layout.yaxis6 = {
                overlaying: 'y5',
                side: 'right',
                position: 0.98,
                showgrid: false,
                title: { text: '对冲比', font: { size: 10 } },
            };
        }

        try {
            Plotly.react(chartEl, traces, layout, {
                responsive: true,
                displayModeBar: true,
                modeBarButtonsToRemove: ['select2d', 'lasso2d'],
                displaylogo: false,
            });
            chartEl.dataset.monitorPlotMode = nextMode;
        } catch (e) {
            showChartMessage(`图表渲染失败: ${e.message}`);
        }
        return;
    }

    const layout = {
        paper_bgcolor: 'transparent', plot_bgcolor: 'transparent',
        font: { color: '#dfe9f7', size: 11 },
        margin: { t: 16, b: 40, l: 60, r: 40 },
        xaxis:  { ...plotlyTimeAxis(), domain: [0, 1], rangeslider: { visible: false } },
        yaxis:  { domain: hasEquity ? [0.32, 1] : [0, 1], gridcolor: '#283242', title: { text: '价格', font: { size: 10 } } },
        ...(hasEquity ? {
            yaxis2: {
                domain: [0, 0.28],
                anchor: 'x',
                gridcolor: '#283242',
                zeroline: true,
                zerolinecolor: '#4b5563',
                zerolinewidth: 1,
                title: { text: '净收益(U)', font: { size: 10 } },
            },
        } : {}),
        showlegend: true,
        legend: { orientation: 'h', y: 1.04, x: 0, font: { size: 10 } },
    };

    const traces = [candleTrace, buyMarker, sellMarker];
    if (hasEquity) traces.push(equityTrace);

    try {
        Plotly.react(chartEl, traces, layout, {
            responsive: true, displayModeBar: true,
            modeBarButtonsToRemove: ['select2d', 'lasso2d'], displaylogo: false,
        });
        chartEl.dataset.monitorPlotMode = nextMode;
    } catch (e) {
        showChartMessage(`图表渲染失败: ${e.message}`);
    }
}

