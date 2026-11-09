/* Research desk summary. Shows what the page already loaded; it never changes a research record. */
(function(root){
'use strict';
const doc=root.document;
const state={trackers:null,error:'',loops:{},archive:null};
const LOOP_STATUS={paused:'已暂停',waiting:'等待下一轮',loading:'读取面板',observing:'检查冻结公式',generating:'模型提公式中',evaluating:'检验中',retry_scheduled:'上轮失败，待重试'};

function setText(id,text){
  const el=doc?.getElementById(id);
  if(!el)return;
  el.textContent=text;
  // Tile notes are cut to one line, so keep the full text on hover.
  if(el.tagName==='SMALL')el.title=text;
}
function setTone(id,tone){
  const el=doc?.getElementById(id);
  if(!el)return;
  if(tone)el.dataset.tone=tone;else delete el.dataset.tone;
}
function nameList(items,limit=2){
  const shown=items.slice(0,limit).join('、');
  return items.length>limit?`${shown} 等 ${items.length} 项`:shown;
}
function loopLabel(payload){
  if(!payload)return '读取中';
  if(payload.error)return '不可用';
  return payload.config?.enabled?'运行中':'已暂停';
}

function summarize(trackers,loops={},archive=null,now=Date.now()){
  const api=root.ResearchTrackers;
  const decide=[],check=[];
  let stopped=0,hardFailure=false,nearest=null;
  for(const r of trackers||[]){
    if(api.isStopped(r)){stopped+=1;continue;}
    const name=api.displayName(r);
    const kind=api.attention(r,now);
    if(kind==='decide')decide.push(name);
    if(kind==='check'){check.push(name);hardFailure=hardFailure||api.status(r)[1]==='bad';}
    const eta=api.judgmentEta(r,now);
    if(eta&&!eta.reached&&(!nearest||eta.days<nearest.days))nearest={name,days:eta.days};
  }
  const frozen=Array.isArray(loops.v3?.frozen)?loops.v3.frozen:[];
  frozen.filter(row=>row.status==='forward_passed').forEach(row=>decide.push(`因子 ${row.name}`));
  const awaiting=Number(archive?.awaiting_decision||0);
  if(awaiting>0)decide.push(`${awaiting} 个提案候选`);
  if(loops.v3?.config?.enabled&&loops.v3?.last_error){check.push('研究循环 v3');hardFailure=true;}
  const total=(trackers||[]).length;
  return {decide,check,hardFailure,nearest,total,running:total-stopped,stopped,frozen:frozen.length,awaiting};
}

function renderDecisions(summary){
  setText('rd-decide-value',String(summary.decide.length));
  setTone('rd-decide-tile',summary.decide.length?'warn':'');
  if(summary.decide.length)setText('rd-decide-note',nameList(summary.decide));
  else setText('rd-decide-note',summary.nearest?`最近判定约 ${summary.nearest.days} 天后 · ${summary.nearest.name}`:'今天不用决定');
  setText('rd-check-value',`${summary.check.length} 本`);
  setTone('rd-check-tile',summary.check.length?(summary.hardFailure?'bad':'warn'):'');
  setText('rd-check-note',summary.check.length?nameList(summary.check):'全部正常');
  setText('rd-ledger-value',`${summary.running} / ${summary.total}`);
  setText('rd-ledger-note',`运行中，${summary.stopped} 本已停`);
}

function renderFactorLoop(){
  const v3=state.loops.v3;
  if(!v3)return;
  if(v3.error){
    setText('rd-factor-value','--');
    setText('rd-factor-note','状态读取失败');
    setTone('rd-factor-tile','bad');
    return;
  }
  const frozen=Array.isArray(v3.frozen)?v3.frozen.length:0;
  const status=v3.running||v3.busy?'一轮运行中':(LOOP_STATUS[v3.status]||v3.status||'--');
  setText('rd-factor-value',`${frozen} 冻结`);
  setText('rd-factor-note',`${Number(v3.ledger_size||0)} 个公式 · ${status}`);
  setTone('rd-factor-tile',v3.config?.enabled&&v3.last_error?'warn':'');
}

function renderOtherLoops(){
  setText('ai-other-loops-count',`v1 ${loopLabel(state.loops.v1)} · v2 ${loopLabel(state.loops.v2)}`);
}

function render(){
  if(state.trackers)renderDecisions(summarize(state.trackers,state.loops,state.archive));
  renderFactorLoop();
  renderOtherLoops();
}

function setTrackers(payload){
  state.trackers=payload?.trackers||[];
  state.error='';
  render();
}

function setTrackersError(message){
  state.error=String(message||'账本读取失败');
  if(state.trackers)return;
  ['rd-decide-value','rd-check-value','rd-ledger-value'].forEach(id=>setText(id,'--'));
  ['rd-decide-note','rd-check-note','rd-ledger-note'].forEach(id=>setText(id,'账本读取失败'));
  setTone('rd-check-tile','bad');
}

function setLoop(name,payload){
  state.loops[name]=payload||{};
  render();
}

function setArchive(summary){
  state.archive=summary||null;
  if(summary){
    const last=summary.last_updated_at?new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',year:'numeric',month:'numeric',day:'numeric'}).format(new Date(summary.last_updated_at)):'';
    setText('ai-proposal-archive-count',`${Number(summary.proposals||0)} 个提案 · ${Number(summary.candidates||0)} 个候选 · ${Number(summary.passed||0)} 个通过${last?` · 最近 ${last}`:''} · 展开时加载`);
    setText('rd-archive-value',`${Number(summary.passed||0)} 通过`);
    setText('rd-archive-note',`${Number(summary.proposals||0)} 个提案 · ${Number(summary.candidates||0)} 个候选${last?` · 最近 ${last}`:''}`);
  }
  render();
}

function scrollTo(id){doc?.getElementById(id)?.scrollIntoView({block:'start',behavior:'smooth'});}

function openArchive(){
  const archive=doc?.getElementById('ai-proposal-archive');
  if(archive){archive.open=true;scrollTo('ai-proposal-archive');}
}

// Each panel sits in its own collapsed wrapper (ui_presentation.js); open that one only.
function revealRecords(panelId){
  const item=doc?.querySelector(`details.ui-ledger-item[data-ledger-panel="${panelId}"]`);
  if(!item)return false;
  item.open=true;
  item.scrollIntoView({block:'start',behavior:'smooth'});
  return true;
}

function openRecords(panelId){
  const drawer=doc?.getElementById('ui-paper-ledgers');
  if(!drawer)return;
  drawer.open=true;
  if(revealRecords(panelId))return;
  // The drawer loads its panels on first open and wraps them a tick later, so wait for both.
  root.addEventListener('ai-research:paper-ledgers-loaded',()=>setTimeout(()=>revealRecords(panelId),0),{once:true});
  scrollTo('ui-paper-ledgers');
}

function handleTile(action){
  if(action==='attention'){
    const summary=state.trackers?summarize(state.trackers,state.loops,state.archive):null;
    const onlyArchive=summary&&summary.awaiting>0&&summary.decide.length===1&&!summary.check.length;
    if(onlyArchive){openArchive();return;}
    root.ResearchTrackers?.setFilter('attention');
    scrollTo('research-ledgers');
  }else if(action==='ledgers'){
    root.ResearchTrackers?.setFilter('all');
    scrollTo('research-ledgers');
  }else if(action==='factor'){
    scrollTo('ai-loop-primary');
  }else if(action==='archive'){
    openArchive();
  }
}

function bind(){
  doc?.getElementById('research-desk-status')?.addEventListener('click',event=>{
    const tile=event.target.closest?.('[data-desk-action]');
    if(tile)handleTile(tile.getAttribute('data-desk-action'));
  });
  doc?.getElementById('btn-open-research-ledgers')?.addEventListener('click',()=>{
    doc.querySelector('.tab-btn[data-tab="ai-research"]')?.click();
  });
}

root.ResearchDesk={setTrackers,setTrackersError,setLoop,setArchive,openRecords,summarize};
if(doc){
  if(doc.readyState==='loading')doc.addEventListener('DOMContentLoaded',bind);
  else bind();
}
})(window);
