/* Receipt activity and source evidence are independent of frozen trade counts. */
(function(root){
'use strict';
const kinds={fresh_call:'当前方向观点',conditional:'条件策略',update:'旧仓更新',promotion:'推广复盘',ambiguous:'方向待复核',no_signal:'无策略'};
const directions={long:'偏多',short:'偏空',watch:'观察',two_sided:'双向条件'};
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const num=v=>Number.isFinite(Number(v))?Number(v):0;
const date=v=>v?esc(typeof root.fmtDateTime==='function'?root.fmtDateTime(v):v):'尚未记录';
function shortDate(value){
  const d=new Date(value);
  return Number.isNaN(d.getTime())?'--':new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hour12:false}).format(d);
}
function link(item){
  const match=/^telegram:(alastartrades|alastartrader1):(\d+)$/.exec(item.source_id||'');
  return match?`<a href="https://t.me/${match[1]}/${match[2]}" target="_blank" rel="noopener noreferrer">@${match[1]} / ${match[2]}</a>`:esc(item.source_id);
}
function kind(item){return kinds[item.classification]||({needs_media_review:'纯图待复核',review_failed:'解析失败待复核',pending:'待解析'}[item.status])||'待复核';}
const impostor=item=>item.impersonator?'<span class="alastar-impostor">冒充号</span>':'';
function receipt(item){
  return `<div style="margin-top:7px">${impostor(item)}${link(item)} · ${esc(kind(item))} · ${item.provenance==='prospective_receipt'?'启用后新收件':'历史补读'}<div class="u-hint">发帖 ${date(item.published_at_utc)} · 首收 ${date(item.first_received_at_utc)}</div></div>`;
}
function plan(item){
  const plans=item.plans||[];
  const assets=[...new Set(plans.map(p=>p.asset))].join(' / ');
  const dirs=[...new Set(plans.map(p=>p.direction))];
  const direction=dirs.length===1?(directions[dirs[0]]||dirs[0]):'双向条件';
  return `<details class="alastar-plan" data-alastar-detail="${esc(item.source_id)}:0">
    <summary><span class="alastar-plan-asset">${esc(assets)}<span class="alastar-direction" data-direction="${dirs.length===1?esc(dirs[0]):'two_sided'}">${esc(direction)}</span></span><span class="alastar-plan-copy">${esc(item.summary||kind(item))}</span><span class="alastar-plan-meta">${impostor(item)}${esc(kind(item))}${item.provenance==='historical_review'?' · 历史':''}<time>${esc(shortDate(item.published_at_utc))}</time></span><span class="alastar-chevron" aria-hidden="true">›</span></summary>
    <div class="alastar-evidence">${link(item)}${plans.map(p=>`<div>${esc(p.thesis_quote)}</div>${p.condition_quote?`<div>条件：${esc(p.condition_quote)}</div>`:''}${p.risk_quote?`<div>失效：${esc(p.risk_quote)}</div>`:''}`).join('')}</div></details>`;
}
function render(panel,inbox,books=[]){
  if(!panel)return;
  if(!inbox){panel.hidden=true;return;}
  const opened=new Set(Array.from(panel.querySelectorAll?.('details[data-alastar-detail][open]')||[],el=>el.getAttribute('data-alastar-detail')));
  const health=inbox.health?.status;
  const label={healthy:'采集中',degraded:'采集异常',stale:'更新超时'}[health]||'待检查';
  const items=Array.isArray(inbox.items)?inbox.items:[];
  const strategies=items.filter(i=>!i.impersonator&&Array.isArray(i.plans)&&i.plans.length);
  const counts=inbox.classification_counts||{};
  const official=Object.values(inbox.sources||{}).filter(s=>!s.impersonator&&s.needs_review!=null);
  const bookText=books.map(b=>`${esc(b.id==='alastar_horizons_v3'?'多周期v3':'1h基准v2')}：完成 ${num(b.n)}，在持 ${num(b.open)}，待入场 ${num(b.waiting)}`).join('；');
  const waiting=health==='healthy'&&books.length&&books.every(b=>!num(b.n)&&!num(b.open)&&!num(b.waiting));
  // Impersonator posts stay visible in the history but never count as Alastar's queue.
  const review=official.length?official.reduce((sum,s)=>sum+num(s.needs_review),0):num(inbox.pending)+num(inbox.review_failed)+num(inbox.media_review);
  panel.hidden=false;
  panel.innerHTML=`<div class="alastar-activity-head"><div class="alastar-activity-title"><h4>Alastar</h4><span class="alastar-subtitle">策略观察</span><span class="alastar-status" data-health="${esc(health||'unknown')}">${label}</span></div><div class="alastar-counts"><span>策略 <b>${num(inbox.strategy_posts)}</b></span><span>收件 <b>${num(inbox.total_posts)}</b></span>${review?`<span class="alastar-review">待复核 <b>${review}</b></span>`:''}</div></div>
    <div class="alastar-plans">${strategies.slice(0,2).map(plan).join('')||'<div class="alastar-empty">暂无已复核策略</div>'}</div>
    <div class="alastar-activity-foot"><span>${waiting?'等待新喊单':'纸面结果见下表'}</span><details class="alastar-more" data-alastar-detail="all"><summary>全部记录与采集详情 <span aria-hidden="true">›</span></summary><div class="alastar-history">
      <div class="alastar-source-meta">检查 ${date(inbox.updated_at)} · 启用 ${date(inbox.enabled_at_utc)}<br>启用后新收件 ${num(inbox.prospective_receipts)} · 历史补读 ${num(inbox.historical_review)} · 当前观点 ${num(counts.fresh_call)} · 条件策略 ${num(counts.conditional)} · 旧仓更新 ${num(counts.update)} · 纯图待复核 ${num(inbox.media_review)}${num(inbox.impersonator_posts)?` · 冒充号 ${num(inbox.impersonator_posts)} 条不计入`:''}
      ${Object.entries(inbox.sources||{}).map(([ch,s])=>`<div>${s.impersonator?'<span class="alastar-impostor">冒充号</span>':''}@${esc(ch)} · ${s.healthy?'正常':esc(s.error||'待检查')} · ${date(s.last_observed_at_utc)}${s.note?`<div class="alastar-impostor-note">${esc(s.note)}</div>`:''}</div>`).join('')}<div>${bookText||'账本状态待加载'}</div><div>原文字账本仅收首次方向喊单；历史补读、条件策略和图片待复核不计成交。</div></div>
      ${items.map(i=>i.plans?.length?plan(i):receipt(i)).join('')}</div></details></div>`;
  for(const el of panel.querySelectorAll?.('details[data-alastar-detail]')||[]){
    if(opened.has(el.getAttribute('data-alastar-detail')))el.open=true;
  }
}
root.AlastarActivity={render};
})(window);
