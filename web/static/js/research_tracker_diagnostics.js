/* Read-only paper research comparisons shared by strategies and research pages. */
(function (root) {
  'use strict';
  const esc = (v) => String(v ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const num = (v, digits = 2) => v === null || v === undefined || !Number.isFinite(Number(v)) ? '--' : Number(v).toFixed(digits);
  const pct = (v, digits = 3) => v === null || v === undefined || !Number.isFinite(Number(v)) ? '--' : `${Number(v) >= 0 ? '+' : ''}${Number(v).toFixed(digits)}%`;
  const pp = (v) => pct(v).replace('%', ' 个百分点');
  const table = (headers, rows) => `<div style="overflow-x:auto;margin-top:8px;"><table class="data-table" style="width:100%;font-size:12px;"><thead><tr>${headers.map(x=>`<th scope="col">${esc(x)}</th>`).join('')}</tr></thead><tbody>${rows.join('')}</tbody></table></div>`;
  const row = (values) => `<tr>${values.map(x=>`<td>${esc(x)}</td>`).join('')}</tr>`;

  function paired(e) {
    if (!e) return '<div class="u-note">同日配对分析暂无数据。</div>';
    const w = e.sensitivity?.weekly || {}, hac = e.sensitivity?.hac || {};
    const best = e.sensitivity?.leave_best_increment_day_out || {}, at = e.attribution || {}, ex = e.execution_diagnostics || {};
    return `<section style="margin-top:10px;">
      <h4>反转过滤 · 相同日期对照</h4>
      <p>${esc(e.start_date || '--')} 至 ${esc(e.end_date || '--')}，共同 ${Number(e.n_pairs || 0)} 天：原版 ${pct(e.baseline?.mean_pct)}/天，影子自身 ${pct(e.shadow?.mean_pct)}/天，增益 ${pp(e.increment?.mean_pp ?? e.increment?.mean_pct)}/天。分母为多空总名义金额。</p>
      <p class="u-note">${Number(w.n_week_groups || 0)} 个日历周组；${w.status === 'insufficient_data' ? `周聚类与 HAC 敏感性样本不足（至少 ${Number(w.min_days || 32)} 个共同日、${Number(w.min_week_groups || 8)} 个周组）` : `周聚类区间 [${pp(w.ci90_pp?.[0])}, ${pp(w.ci90_pp?.[1])}]；HAC区间 [${pp(hac.ci90_pp?.[0])}, ${pp(hac.ci90_pp?.[1])}]`}。这些是敏感性检查，确认仍按原登记规则。</p>
      <p>不计最佳增益日 ${esc(best.excluded_date || '--')} 后，剩余 ${Number(best.n_pairs || 0)} 天的平均增益 ${pp(best.increment_mean_pp)}。归因完整 ${Number(at.complete_days || 0)} 天：避开原多头的价格贡献 ${pp(at.mean_removed_price_pp)}，换入币价格贡献 ${pp(at.mean_replacement_price_pp)}，共享持仓权重变化 ${pp(at.mean_shared_reweight_price_pp)}，资金费净贡献 ${pp(at.mean_funding_delta_pp)}。</p>
      <p class="u-note">报价覆盖：原版 ${Number(ex.baseline_complete_quote_days || 0)}/${Number(e.n_pairs || 0)} 天，影子 ${Number(ex.shadow_complete_quote_days || 0)}/${Number(e.n_pairs || 0)} 天；决策时间未知 ${Number(ex.unknown_decision_time_days || 0)} 天。报价仅用于价格漂移诊断，不当作可成交收益。</p>
      <details><summary>逐日对照、过滤币与额外成本</summary>
        ${table(['日期','原版净收益','影子净收益','配对增益','归因'], (e.daily || []).slice(-30).map(d=>row([d.date,pct(d.baseline_pct),pct(d.shadow_pct),pp(d.increment_pp),d.attribution_status === 'complete' ? '完整' : '缺失或未对齐'])))}
        ${table(['日期','剔除原多头','换入多头','共享持仓'], (at.days || []).slice(-30).map(d=>row([d.date,(d.removed || []).join('、') || '无',(d.replacement || []).join('、') || '无',(d.shared || []).length])))}
        ${table(['额外成本（百分点/天）','原版均值','影子均值','影子额外承担成本后的增益'], (e.sensitivity?.cost || []).map(d=>row([num(d.extra_cost_pp),pct(d.baseline_mean_with_common_extra_cost_pct),pct(d.shadow_mean_with_common_extra_cost_pct),pp(d.increment_with_shadow_only_extra_cost_pp)])))}
      </details>
    </section>`;
  }

  function composite(a) {
    if (!a || a.status === 'unavailable') return '<div class="u-note">Coinglass 四格归因暂不可用。</div>';
    const labels = {original_ic:'131币 · IC加权', original_equal:'131币 · 等权', wide_ic:'234币 · IC加权', wide_equal:'234币 · 等权'};
    const comparisons = {weighting_original:'131币：等权 − IC',weighting_wide:'234币：等权 − IC',expansion_ic:'IC：扩池 − 原池',expansion_equal:'等权：扩池 − 原池'};
    const cells = a.cells || {}, common = a.factorial_common || {};
    return `<section style="margin-top:10px;">
      <h4>Coinglass · 币池与权重的四格归因</h4>
      <p>四格共同完成 ${Number(common.n_pairs || 0)} 个信号日。${a.status === 'insufficient_data' ? '共同样本不足，暂不选择胜出版本。' : '结果只作归因，不自动改变策略。'}收益按单腿名义金额计算，折算多空总名义金额需除以二。</p>
      ${table(['组合','完成期数','净收益均值','价格贡献','资金费贡献','成本拖累','高成本净收益'],Object.keys(labels).map(k=>{const c=cells[k]||{},m=c.mean_contributions||{};return row([labels[k],c.n_completed||0,pct(c.stats?.mean_pct),pct(m.price_pct),pct(m.funding_pct),pct(m.cost_pct),pct(m.stress_net_pct)]);}))}
      <p class="u-note">高成本按每腿来回 0.12% → 0.20% 计算；满换手两腿合计额外拖累 0.16 个百分点。扩池同信号日平均晚形成 ${num(a.timing?.mean_delay_minutes,1)} 分钟，不能把形成和结算时间差算成纯扩池优势。七日持仓重叠，期数不是独立样本数。</p>
      <details><summary>共同日期的权重差、扩池差与时间差</summary>
        ${table(['比较（右减左）','共同期数','结算周组','净收益差','高成本差','时间混杂'],Object.keys(comparisons).map(k=>{const c=a.comparisons?.[k]||{};return row([comparisons[k],c.n_pairs||0,c.weekly_groups||0,pp(c.mean_pct),pp(c.stress_stats?.mean_pct),c.timing_confounded?'有':'未发现或样本不足']);}))}
        ${table(['信号日','扩池晚形成（分钟）'],(a.timing?.rows||[]).slice(-30).map(d=>row([d.signal_date,num(d.wide_minus_original_minutes,1)])))}
      </details>
    </section>`;
  }

  function risk(s, name = '') {
    if (!s) return '';
    if (!s.registered) return `<p class="u-note">${esc(name)} 风险影子等待首次登记。</p>`;
    const c=s.config||{};
    const positions=(s.positions||[]).slice(-20);
    return `<details style="margin-top:8px;"><summary>${esc(name)} 资金费与总亏损影子：完成 ${Number(s.completed||0)}，在持 ${Number(s.open||0)}，资金费拒绝 ${Number(s.funding_rejected||0)}</summary>
      <p>首笔前登记于 ${esc(s.registered_at)}。冻结预算：预计不利资金费 ${num(c.funding_budget_pct)}%，含费用总亏损触发值 ${num(c.total_loss_budget_pct)}%；分母为单腿入场名义金额。均值 ${pct(s.mean_net_pct)}。</p>
      <p class="u-note">使用观察时买卖报价与不利滑点；按合约实际资金费间隔估算。缺数据不按零计。预算是检查时触发条件，跳空、检查间隔和延迟可能使实际纸面亏损超过该值。已有入场交易不回填。</p>
      <p>报价或费率不可用 ${Number(s.entry_unavailable||0)}，错过入场 ${Number(s.missed||0)}，监测缺失 ${Number(s.monitoring_unavailable||0)}，等待资金费结算 ${Number(s.closing_pending_funding||0)}。</p>
      ${positions.length?table(['信号','状态','净收益/含费浮盈','资金费','观察/退出时间','退出原因'],positions.map(p=>row([p.symbol,p.status,pct(p.net_pct??p.mark?.net_pct),pct(p.exit_breakdown?.funding_pct??p.mark?.funding_pct),p.exit_executed_at||p.mark?.observed_at||p.entry_decision_at||'--',p.exit_reason||p.unavailable_reason||'--']))):'<p class="u-note">尚无登记后的新入场信号。</p>'}
    </details>`;
  }

  function render(target, data) {
    if (!target) return;
    target.innerHTML=paired(data?.reversal)+composite(data?.coinglass)+(data?.event_risk||[]).map(s=>risk(s,s.name)).join('');
  }
  root.ResearchTrackerDiagnostics={paired,composite,risk,render};
})(window);
