/* Target-specific event-count sentiment. Scores describe text, not return odds. */
(function(root){
'use strict';
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const finite=v=>v!==null&&v!==undefined&&Number.isFinite(Number(v));
const num=(v,d=0)=>finite(v)?Number(v).toFixed(d):'--';
const signed=(v,d=1)=>finite(v)?`${Number(v)>0?'+':''}${Number(v).toFixed(d)}`:'--';
const pct=v=>finite(v)?`${signed(v,2)}%`:'--';
const stamp=v=>{const d=new Date(v);return v&&!Number.isNaN(d.getTime())?new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hour12:false}).format(d):'--';};
function receipt(s){
  if(!s)return '';
  if(s.status!=='active')return s.label||'条数观察待恢复';
  const warm=(s.rolling||[]).every(w=>!Number(w.views));
  if(warm&&finite(s.background?.[0]?.score))return `背景10条 ${signed(s.background[0].score)}分 · 前向预热`;
  return (s.rolling||[]).map(w=>w.qualified?`${num(w.size)}条 ${signed(w.score)}分`:`${num(w.size)}条 ${num(w.views)}/${num(w.size)}`).join(' · ');
}
function card(w){
  const available=!['unavailable','coverage_gap'].includes(w.status);
  const interval=available&&Array.isArray(w.bullish_share_ci95)?`[${num(w.bullish_share_ci95[0]*100,1)}%, ${num(w.bullish_share_ci95[1]*100,1)}%]`:'--';
  return `<article class="rt-event-window"><div class="rt-sentiment-head"><h5>最近 ${num(w.size)} 条 BTC 观点</h5><span class="rt-state" data-tone="${w.qualified?'good':available?'muted':'warn'}">${esc(w.label)}</span></div><strong class="rt-sentiment-score">${available?signed(w.score):'--'}<small> 作者方向分</small></strong><p>新观点 ${num(w.views)} / ${num(w.size)} 条 · 独立作者 ${num(w.authors)} / 5 人</p><p>看多 ${num(w.bullish)} 人 · 看空 ${num(w.bearish)} 人 · 收缩分 ${available?signed(w.smoothed_score):'--'}</p><p class="rt-note">看多账号占比的95%区间 ${interval}</p><p class="rt-note">${w.interval_excludes_balance?'区间未跨过50%均衡点':'区间不足以排除多空均衡'}；账号之间可能相关。</p><p class="rt-note">观点跨度 ${num(w.span_hours,1)}h · 距最新观点 ${num(w.age_hours,1)}h</p><p class="rt-note">同作者旧观点替换 ${num(w.superseded_views)} 条 · 同文复读剔除 ${num(w.echo_authors_removed)} 个账号</p></article>`;
}
function detail(s){
  if(!s)return '';
  if(!(s.rolling||[]).length)return `<section class="rt-detail-wide rt-event-section"><h4>按观点条数观察</h4><p class="rt-warning">${esc(s.label||'条数观察暂不可用')}</p></section>`;
  const background=(s.background||[]).map(w=>`<p class="rt-note">最近 ${num(w.size)} 条：${signed(w.score)} 分 · ${num(w.authors)} 位作者（多 ${num(w.bullish)} / 空 ${num(w.bearish)}）· 跨度 ${num(w.span_hours,1)}h · 最新 ${esc(stamp(w.last_view_at))}。</p>`).join('');
  const prior=(s.background||[]).map(w=>`${num(w.size)}条 ${signed(w.score)}分`).join(' · ');
  const quality=s.quality_checks?`<p class="rt-note">存档中已复核 BTC 观点 ${num(s.quality_checks.labelled_btc_views)} 条；排除长期／混合期限及缺方向的价格愿望后保留 ${num(s.quality_checks.retained_btc_views)} 条。这个文本质量筛查没有独立人工准确率评估。</p>`:'';
  const books=(s.books||[]).map(b=>`<tr><td>${num(b.size)}条 · ${b.variant==='follow'?'顺势':'反向'}</td><td>${num(b.decisions)}</td><td>${num(b.settled)}</td><td>${num(b.pending)}</td><td>${pct(b.mean_net_pct)}</td><td>${pct(b.mean_stress_pct)}</td><td>${finite(b.mean_excess_pct)?`${signed(b.mean_excess_pct,2)} pp`:'--'}</td></tr>`).join('');
  const history=(s.history||[]).map(w=>`<tr><td>${esc(stamp(w.recorded_at))}</td><td>${num(w.size)}条</td><td>${num(w.authors)}</td><td>${signed(w.score)}</td><td>${esc(w.label)}</td></tr>`).join('');
  return `<section class="rt-detail-wide rt-event-section"><h4>按观点条数观察 · 独立前向版</h4><p class="rt-note">每位作者只计最新方向，相同文本复读只计一次；按10／20条观点更新。方向分表示看多／看空倾向，收缩分用于减少小样本极端值，均不是涨跌概率。</p>${s.status!=='active'?`<p class="rt-warning">${esc(s.label)}</p>`:''}<div class="rt-event-grid">${s.rolling.map(card).join('')}</div><p class="rt-note">10条相对20条的情绪变化 ${finite(s.change_10_vs_20)?`${signed(s.change_10_vs_20)} 分`:'--（两组观点尚未凑齐）'} · 最近 ${num(s.attention?.messages)} 条清洗消息含 ${num(s.attention?.btc_views)} 条 BTC 方向观点。</p><p class="rt-note">只使用启用后、连续覆盖期间的新观点；最新观点超过6h或窗口跨度超过24h会停止标为候选。启用于 ${esc(stamp(s.activation_at))}（北京），更新 ${esc(stamp(s.updated_at))}。</p>${quality}<p class="rt-note">存量背景（回溯）：${esc(prior)}；不计入前向样本。</p><details data-event-detail="background"><summary>存量背景 · 回溯，不计入新样本</summary>${background}<p class="rt-note">这些旧观点仅供了解背景；分数沿用本版“每人最新方向”口径，删帖不会回写已冻结的前向记录。</p></details><details data-event-detail="evidence"><summary>条数版前向纸面账与窗口记录</summary><div class="rt-inner-scroll"><table class="data-table rt-inner-table rt-event-books"><thead><tr><th>账本</th><th>冻结</th><th>结算</th><th>待结算</th><th>净收益/笔</th><th>40bp压力/笔</th><th>同入场做多超额</th></tr></thead><tbody>${books}</tbody></table></div><p class="rt-note">只对不重叠完整批次记录候选；顺势／反向持有6h，往返20bp及实际资金费，缺行情或资金费保持待结算。四账本共享消息，不能相加为独立样本；旧小时账本另行保留。</p>${history?`<div class="rt-inner-scroll"><table class="data-table rt-inner-table"><thead><tr><th>记录（北京）</th><th>批次</th><th>作者</th><th>方向分</th><th>状态</th></tr></thead><tbody>${history}</tbody></table></div>`:'<p class="rt-note">尚无启用后完成的条数批次。</p>'}</details></section>`;
}
root.TelegramSentimentV2={detail,receipt};
})(window);
