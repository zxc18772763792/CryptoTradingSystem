/* Compact paper-tracker view. Display state never changes a research record. */
(function(root){
'use strict';
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const finite=v=>v!==null&&v!==undefined&&Number.isFinite(Number(v));
const count=v=>finite(v)?Number(v):0;
const pct=(v,d=2)=>finite(v)?`${Number(v)>=0?'+':''}${Number(v).toFixed(d)}%`:'--';
const pp=v=>pct(v).replace('%',' 个百分点');
const date=v=>v?esc(root.fmtDateTime?root.fmtDateTime(v):v):'--';
const tone=v=>!finite(v)?'':Number(v)>=0?'is-positive':'is-negative';
const names={alastar_horizons_v3:'Alastar · 多周期',alastar_fresh_call_1h_v2:'Alastar · 1小时',telegram_cryptoquant_24h:'CryptoQuant · 顺势24h',cryptoquant_alert_shadow_v1:'CryptoQuant Alert · 资金流',next10xgem1_fade_24h_v2:'next10xgem1 · 反向24h v2',next10xgem1_fade_24h_v1:'next10xgem1 · v1（停收）',alastartrader1_as_written_v1:'alastartrader1（冒充号）· 按原文',telegram_binance_chat_sentiment:'币安中文群 · 情绪',unlock_short:'大额解锁 · 提前30天',unlock_short_t7:'大额解锁 · 提前7天',supply_factor:'供给通胀因子',xs_reversal:'横截面反转',xs_reversal_liq_filter:'横截面反转 · 异常过滤',coinglass_composite_7d:'Coinglass · 七日组合',coinglass_composite_7d_wide_eq:'Coinglass · 等权扩池',korea_caution_hedged:'韩国警示 · 合并对冲',upbit_caution:'Upbit · 警示做空',upbit_caution_hedged:'Upbit · 警示对冲',upbit_krw_listing:'Upbit · 韩元上币',bithumb_caution:'Bithumb · 警示做空',bithumb_caution_hedged:'Bithumb · 警示对冲',announcement_short_monitor:'币安 · 监控公告',announcement_short_delist:'币安 · 下架公告',listing_short:'新永续 · 上市做空'};
const groups=[['all','全部'],['telegram','Telegram'],['events','事件'],['factors','因子']];
const view={filter:'all',query:'',opened:new Set(),data:null};
function category(r){return r.group==='Telegram信号'?'telegram':r.group==='因子'?'factors':'events';}
function clock(value){
  const d=new Date(value);
  return value&&!Number.isNaN(d.getTime())?new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hour12:false}).format(d):'--';
}
function status(r){
  const label=r.tracker_status_label||'';
  if(r.error)return ['读取失败','bad'];
  if(/停用|停运|已停止/.test(label))return ['已停用','muted'];
  if(r.strategy_inbox?.health?.status==='stale')return ['更新延迟','warn'];
  if(r.strategy_inbox?.health?.status==='degraded')return ['采集异常','bad'];
  if(r.source_health?.status==='inaccessible')return ['来源不可访问','bad'];
  if(r.source_health?.status==='coverage_gap')return ['采集有缺口','warn'];
  if(r.source_health?.status==='coverage_unverified')return ['覆盖待确认','warn'];
  if(r.collection_health&&r.collection_health.status!=='healthy')return ['数据延迟','warn'];
  if(/异常|失败|不可访问/.test(label)||r.signal_intake?.last_error)return ['采集异常','bad'];
  if(/缺口|待确认/.test(label))return ['覆盖待确认','warn'];
  if(/延迟|待恢复|超时/.test(label)||r.runner_active===false)return ['更新延迟','warn'];
  if(count(r.pending_review)||count(r.signal_intake?.counts?.retry_pending))return ['待复核','warn'];
  if(r.signal_intake?.enabled===false)return ['解析停用','muted'];
  if(r.signal_intake&&r.signal_intake.runner_active===false)return ['解析待恢复','warn'];
  if(r.verdict==='retire')return ['待停用','warn'];
  if(r.verdict==='confirmed')return ['达到门槛','good'];
  if(count(r.open))return ['跟踪中','good'];
  return ['积累样本','muted'];
}
function mainResult(r){
  if(r.horizon_comparison?.length)return '<strong>多周期观察</strong><span class="rt-sub">点击查看周期对照</span>';
  const delta=r.metric_kind==='paired_increment'||r.metric_kind==='excess_return';
  const label=r.metric_kind==='paired_increment'?'配对增益':r.metric_kind==='excess_return'?'同币超额':'净收益';
  return `<strong class="${tone(r.mean_pct)}">${delta?pct(r.mean_pct).replace('%',' pp'):pct(r.mean_pct)}</strong><span class="rt-sub">${label} / ${esc(r.unit||'笔')}</span>`;
}
function kv(label,value){return `<div><span>${esc(label)}</span><strong>${value}</strong></div>`;}
function sentimentReceipt(s){
  if(s?.event_time&&root.TelegramSentimentV2)return root.TelegramSentimentV2.receipt(s.event_time);
  const brief={no_directional_views:'无观点',coverage_gap:'覆盖不足',warming_up:'待完整窗',coverage_unverified:'覆盖待确认',awaiting_observation:'待记录',labels_pending:'待解析',unavailable:'记录不可读',interrupted:'已中断',collection_error:'采集异常',inaccessible:'不可访问',inactive:'未激活'};
  return (s?.windows||[]).map(w=>`${count(w.hours)}h ${finite(w.authors)?`${count(w.authors)}/5人`:brief[w.status]||'待确认'}`).join(' · ');
}
function sentimentDetail(r){
  const s=r.chat_sentiment;if(!s)return '';
  const number=v=>finite(v)?Number(v).toLocaleString('zh-CN'):'--';
  const score=v=>finite(v)?`${Number(v)>0?'+':''}${Number(v).toFixed(2).replace(/\.00$|(?<=\.[0-9])0$/,'')}`:'--';
  const windows=(s.windows||[]).map(w=>`<article class="rt-sentiment-window"><div class="rt-sentiment-head"><h5>${count(w.hours)} 小时窗口</h5><span class="rt-state" data-tone="${w.qualified?'good':finite(w.authors)?'muted':'warn'}">${esc(w.label)}</span></div><strong class="rt-sentiment-score ${w.qualified?tone(w.score):''}">${score(w.score)}<small> / 100</small></strong><p>独立作者 ${number(w.authors)} / ${count(s.min_authors)} 人 · 分数门槛 ±${count(s.min_abs_score)}</p><p class="rt-note">${esc(clock(w.window_start))} — ${esc(clock(w.window_end))}（北京时间）</p>${finite(w.authors)&&count(w.authors)<count(s.min_authors)&&finite(w.score)?'<p class="rt-note">作者不足，分数仅供观察。</p>':''}${w.next_complete_at?`<p class="rt-note">持续完整采集后，最早可评估 ${esc(clock(w.next_complete_at))} 结束的窗口。</p>`:''}${w.available_at?`<p class="rt-note">记录于 ${esc(clock(w.available_at))}</p>`:''}</article>`).join('');
  const a=s.activity||{};
  const activity=finite(a.messages)?`<p class="rt-note">近48小时采集快照：${number(a.messages)} 条群消息 · ${number(a.authors)} 位活跃作者 · ${number(a.directional_messages)} 条全币种方向标签。方向标签须再通过 BTC 及作者门槛；截至 ${esc(clock(a.last))}。</p>`:'';
  const reasons=(s.diagnostics||[]).map(d=>finite(d.recorded_windows)?`<p class="rt-note">${count(d.hours)}h 已记录 ${count(d.recorded_windows)} 个窗口${finite(d.max_authors)?`，最多 ${count(d.max_authors)} 位方向作者`:''}：${(d.reasons||[]).map(v=>`${esc(v.label)} ${count(v.count)}`).join(' · ')||'暂无窗口记录'}。</p>`:`<p class="rt-note">${count(d.hours)}h 窗口记录暂不可读。</p>`).join('');
  const history=(s.history||[]).length?`<details data-telegram-history class="rt-sentiment-history"><summary>最近窗口记录</summary><div class="rt-inner-scroll"><table class="data-table rt-inner-table"><thead><tr><th>窗口结束（北京）</th><th>周期</th><th>独立作者</th><th>观察分</th><th>判定</th></tr></thead><tbody>${s.history.map(w=>`<tr><td>${esc(clock(w.window_end))}</td><td>${count(w.hours)}h</td><td>${number(w.authors)}</td><td>${score(w.score)}</td><td>${esc(w.label)}</td></tr>`).join('')}</tbody></table></div></details>`:'';
  const books=(r.ledgers||[]).length?`<section class="rt-detail-wide"><h4>四本纸面账本</h4><div class="rt-inner-scroll"><table class="data-table rt-inner-table rt-ledger-table"><thead><tr><th>账本</th><th>已冻结</th><th>已结算</th><th>待结算</th><th>净收益 / 笔</th><th>同入场做多超额</th><th>40bp压力 / 笔</th><th>胜率</th></tr></thead><tbody>${r.ledgers.map(b=>`<tr><td>${esc(b.label)}${b.book==='chat_follow'?'<span class="rt-sub">主账本</span>':''}</td><td>${count(b.decisions)}</td><td>${count(b.mature_settled)}</td><td>${count(b.pending)}</td><td>${pct(b.mean_net_pct)}</td><td>${pp(b.mean_excess_pct)}</td><td>${pct(b.mean_stress_pct)}</td><td>${finite(b.win_rate)?`${(Number(b.win_rate)*100).toFixed(0)}%`:'--'}</td></tr>`).join('')}</tbody></table></div><p class="rt-note">四账本共享聊天流，样本数不相加；主表统计小时顺势账本。净收益含20bp往返成本及实际资金费，空样本保持为空。</p></section>`:'';
  return `<section class="rt-detail-wide"><h4>BTC 群情绪观察</h4><p class="rt-note">已复核的 BTC 方向观点按作者等权聚合，观察分范围 −100 至 +100；分数不是涨跌概率。完整覆盖、至少 ${count(s.min_authors)} 位独立作者且绝对分数 ≥${count(s.min_abs_score)} 才达到情绪门槛，模拟交易以账本冻结记录为准。</p><div class="rt-sentiment-windows">${windows}</div>${activity}<p class="rt-note">连续覆盖起点 ${date(r.source_health?.clean_since)} · 最近采集 ${date(r.source_health?.last_collection_at)}</p>${reasons}<p class="rt-note">诊断仅查看最近最多168个小时窗口及28个六小时窗口的已有记录；缺口期间可能没有窗口记录。历史分数保留记录时的状态。</p>${history}</section>${books}`;
}
function detail(r){
  const delta=r.metric_kind==='paired_increment'||r.metric_kind==='excess_return';
  const metric=v=>delta?pp(v):pct(v);
  const intake=r.signal_intake;
  const u=r.current_universe;
  const universe=u?`<p class="rt-note">币池 ${esc(u.month)} · ${count(u.coins)} 个币 · 新增 ${count(u.added?.length)} / 移出 ${count(u.removed?.length)} · 生效于 ${date(u.selected_at)}；旧持仓沿用入场名单。</p>`:'';
  const intakeStatus=intake?(intake.enabled===false?'已停用':intake.last_error?'异常':intake.runner_active?'运行中':'待恢复'):'';
  const frozen=count(intake?.counts?.label_frozen)+count(intake?.counts?.already_frozen);
  const ci=Array.isArray(r.ci90_pct)?`[${metric(r.ci90_pct[0])}, ${metric(r.ci90_pct[1])}]${r.ci_clusters!=null?` · ${count(r.ci_clusters)} 组`:''}`:r.ci_clusters===1&&count(r.n)>=2?'同属 1 组，暂无区间':'--';
  const failures=(r.collection_health?.failures||[]).filter(f=>!['evidence','reason','risk_shadow'].includes(f.phase)).slice(0,3);
  const horizons=r.horizon_comparison?.length?`<section class="rt-detail-wide"><h4>周期对照</h4><div class="rt-inner-scroll"><table class="data-table rt-inner-table"><thead><tr><th>持有</th><th>到期 / 共同</th><th>价格净收益</th><th>同币超额</th><th>较1h增益</th><th>含资金费</th></tr></thead><tbody>${r.horizon_comparison.map(h=>`<tr><td>${count(h.hours)}h</td><td>${count(h.completed_n)} / ${count(h.common_n)}</td><td>${pct(h.mean_net_pct)}</td><td>${pp(h.excess_pp)}</td><td>${pp(h.gain_vs_1h_pp)}</td><td>${pct(h.mean_funded_pct)}<span class="rt-sub">已补齐 ${count(h.funded_common_n)}/${count(h.common_n)}</span></td></tr>`).join('')}</tbody></table></div><p class="rt-note">仅用所有周期已到期且对照完整的共同样本；未成熟不填零，不作周期优胜判定。</p></section>`:'';
  const marks=(r.open_marks||[]).map(m=>`<div class="rt-mark"><strong class="${tone(m.mark_pct)}">${esc(m.label)} ${pct(m.mark_pct)}</strong><span>${esc(m.scope_label||'空头腿')} · ${esc(m.price_basis||'账本标价')} · ${date(m.price_at)}</span>${finite(m.price_return_pct)&&finite(m.funding_pct)?`<span>价格 ${pct(m.price_return_pct)} · 资金费 ${pct(m.funding_pct)} · 成本 ${pct(m.cost_pct)}</span>`:''}${m.is_partial?'<span>组合净浮盈暂无完整数据</span>':''}</div>`).join('');
  const progress=[r.pending_review?`待复核 ${count(r.pending_review)}`:'',r.completed_without_primary?`对照待补 ${count(r.completed_without_primary)}`:'',r.waiting?`待入场 ${count(r.waiting)}`:'',r.late?`晚到 ${count(r.late)}`:''].filter(Boolean).join(' · ');
  return `<div class="rt-detail-grid"><section><h4>${esc(r.name)}</h4><p>${esc(r.rule||'规则暂未记录')}</p><p class="rt-note">${esc(r.group||'')} · ${esc(r.basis_label||'收益分母未记录')}</p><p class="rt-note">${esc(r.cost_scope||'')}</p></section>
    <section class="rt-stat-grid">${kv(r.metric_label||'净收益',`${metric(r.mean_pct)} / ${esc(r.unit||'笔')}`)}${kv('90% 区间',ci)}${kv('胜率',finite(r.win_rate)?`${(Number(r.win_rate)*100).toFixed(0)}%`:'--')}${kv('总名义敞口均值',`${metric(r.gross_mean_pct)} / ${esc(r.unit||'笔')}`)}${kv('回测参照',`${metric(r.backtest_mean_pct)} / ${esc(r.unit||'笔')}`)}${finite(r.actual_mean_pct)?kv(r.metric_kind==='paired_increment'?'影子自身':'实际含资金费净收益',`${pct(r.actual_mean_pct,3)}/${esc(r.unit||'笔')}`):''}${r.metric_kind==='paired_increment'?kv('同期原版',`${pct(r.paired_evaluation?.baseline?.mean_pct,3)}/${esc(r.unit||'天')}`):''}</section>
    <section><h4>运行与判定</h4><p>${esc(r.tracker_status_label||'')}${progress?' · '+esc(progress):''}</p>${universe}<p>${esc(r.verdict_label||'')} · ${esc(r.verdict_reason||'')}</p><p class="rt-note">更新 ${date(r.updated_at)}</p>${r.collection_health&&r.collection_health.status!=='healthy'?`<p class="rt-warning">最近完整更新：${date(r.collection_health.last_success_at)}${failures.map(f=>`<br>${esc(f.phase)}：${esc(f.error)}`).join('')}</p>`:''}
    ${intake?`<p class="rt-note">自动语义解析${intakeStatus} · 已冻结解释 ${frozen} · 非信号 ${count(intake.counts?.non_signal)}${intake.counts?.predates_auto_intake?` · 启用前帖子 ${count(intake.counts.predates_auto_intake)}（未回填）`:''}${intake.counts?.label_queued?` · 旧投递待确认 ${count(intake.counts.label_queued)}`:''}${intake.counts?.retry_pending?` · 待重试 ${count(intake.counts.retry_pending)}`:''}</p>${intake.enabled_at_utc?`<p class="rt-note">启用于 ${date(intake.enabled_at_utc)}</p>`:''}${intake.enabled&&intake.runner_active&&Object.keys(intake.counts||{}).every(k=>k==='predates_auto_intake')?'<p class="rt-note">启用后尚无新帖，暂无可冻结的前向信号。</p>':''}`:''}</section>
    ${marks?`<section><h4>在持标价 · 非实时报价</h4>${marks}</section>`:''}${horizons}${root.TelegramSentimentV2?.detail(r.chat_sentiment?.event_time)||''}${sentimentDetail(r)}
    ${r.strategy_inbox?'<section class="rt-detail-wide"><div id="alastar-strategy-activity" class="alastar-activity" hidden></div></section>':''}</div>`;
}
function selectedRows(data,filter=view.filter,query=view.query){
  const q=query.trim().toLocaleLowerCase();
  return (data?.trackers||[]).map((r,i)=>({...r,_key:String(r.id||i)})).filter(r=>(filter==='all'||category(r)===filter)&&(!q||[r.name,names[r.id],r.group].join(' ').toLocaleLowerCase().includes(q)));
}
function render(d){
  const doc=root.document||document;
  const tb=doc.getElementById('research-trackers-tbody');if(!tb)return;
  view.data=d;
  const nested=Array.from(tb.querySelectorAll?.('details[data-alastar-detail][open]')||[],e=>e.getAttribute('data-alastar-detail'));
  const telegramHistoryOpen=!!tb.querySelector?.('details[data-telegram-history][open]');
  const eventOpened=Array.from(tb.querySelectorAll?.('details[data-event-detail][open]')||[],e=>e.getAttribute('data-event-detail'));
  const rows=selectedRows(d);
  let html='';
  for(const [group,label] of groups.slice(1)){
    const members=rows.filter(r=>category(r)===group);if(!members.length)continue;
    if(view.filter==='all')html+=`<tr class="rt-group"><td colspan="6">${label}<span>${members.length}</span></td></tr>`;
    for(const r of members){
      const [state,kind]=status(r),open=view.opened.has(r._key),id='rt-detail-'+encodeURIComponent(r._key);
      const title=names[r.id]||r.name;
      html+=`<tr class="rt-row${open?' is-open':''}"><td><button type="button" class="rt-name" data-tracker-toggle="${esc(r._key)}" aria-expanded="${open}" aria-controls="${esc(id)}" title="${esc(r.name)}"><span class="rt-row-chevron" aria-hidden="true">›</span><span>${esc(title)}</span></button>${r.strategy_inbox?`<span class="rt-sub rt-receipt">收件 ${count(r.strategy_inbox.total_posts)} · 策略 ${count(r.strategy_inbox.strategy_posts)}</span>`:''}${r.chat_sentiment?`<span class="rt-sub rt-receipt">${esc(sentimentReceipt(r.chat_sentiment))}</span>`:''}</td><td><span class="rt-state" data-tone="${kind}">${state}</span></td><td class="rt-sample">${count(r.n)} / ${finite(r.min_n)?count(r.min_n):'--'} ${esc(r.unit||'笔')}</td><td class="rt-result" title="${esc(r.basis_label||'')}；${esc(r.cost_scope||'')}">${r.error?'--':mainResult(r)}</td><td class="rt-hold">${finite(r.open)?count(r.open):'--'}${r.waiting?`<span class="rt-sub">待入场 ${count(r.waiting)}</span>`:''}</td><td class="rt-time" title="${date(r.updated_at)}">${esc(clock(r.updated_at))}</td></tr>`;
      html+=`<tr class="rt-detail-row" id="${esc(id)}"${open?'':' hidden'}><td colspan="6">${r.error?`<div class="rt-warning">读取失败：${esc(r.error)}</div>`:detail(r)}</td></tr>`;
    }
  }
  tb.innerHTML=html||'<tr><td colspan="6" class="u-empty">没有匹配的跟踪器</td></tr>';
  tb.onclick=e=>{
    const button=e.target.closest?.('[data-tracker-toggle]');if(!button)return;
    const key=button.getAttribute('data-tracker-toggle');
    if(view.opened.has(key))view.opened.delete(key);else view.opened.add(key);
    const opened=view.opened.has(key),row=doc.getElementById('rt-detail-'+encodeURIComponent(key));
    if(row)row.hidden=!opened;
    button.setAttribute('aria-expanded',String(opened));
    button.closest('tr')?.classList.toggle('is-open',opened);
  };
  const filters=doc.getElementById('research-tracker-filters');
  if(filters){
    const buttons=groups.map(([key,label])=>`<button type="button" data-tracker-filter="${key}" aria-pressed="${key===view.filter}" class="rt-filter${key===view.filter?' is-active':''}">${label}<span>${(d?.trackers||[]).filter(r=>key==='all'||category(r)===key).length}</span></button>`).join('');
    if(filters.innerHTML!==buttons)filters.innerHTML=buttons;
    filters.onclick=e=>{const b=e.target.closest?.('[data-tracker-filter]');if(b){view.filter=b.getAttribute('data-tracker-filter');render(view.data);}};
  }
  const search=doc.getElementById('research-tracker-search');
  if(search)search.oninput=()=>{view.query=search.value;render(view.data);};
  const meta=doc.getElementById('research-trackers-meta');
  if(meta){meta.textContent=`${rows.length} 个跟踪器 · 更新 ${clock(d?.generated_at)}`;meta.title='各行单位与周期不同，样本数不相加；不作跨周期排名。点击策略查看口径与完整记录。';}
  const alastarRows=(d?.trackers||[]).filter(r=>['alastar_horizons_v3','alastar_fresh_call_1h_v2'].includes(r.id));
  root.AlastarActivity?.render(doc.getElementById('alastar-strategy-activity'),(d?.trackers||[]).find(r=>r.strategy_inbox)?.strategy_inbox,alastarRows);
  for(const el of tb.querySelectorAll?.('details[data-alastar-detail]')||[])if(nested.includes(el.getAttribute('data-alastar-detail')))el.open=true;
  if(telegramHistoryOpen){const el=tb.querySelector?.('details[data-telegram-history]');if(el)el.open=true;}
  for(const el of tb.querySelectorAll?.('details[data-event-detail]')||[])if(eventOpened.includes(el.getAttribute('data-event-detail')))el.open=true;
  root.ResearchTrackerDiagnostics?.render(doc.getElementById('research-tracker-diagnostics'),d?.diagnostics);
}
root.ResearchTrackers={render,detail,status,selectedRows};
})(window);
