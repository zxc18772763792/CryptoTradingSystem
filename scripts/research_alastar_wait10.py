"""Historical wait-for-10%-rally short study. Cached public prices only.

No change to the active v2/v3 ledgers. Primary execution observes a completed
bar's first touch then uses the next open. Untriggered signals retain zero PnL;
missing observations remain unknown. A resting-limit fill is a labeled ideal.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from scripts.audit_alastar_short_risk import protected_hashes
from scripts.research_alastar_horizons import HORIZONS, save_bytes, write, week_interval
from scripts.research_alastar_responses import Market, funding_range
from scripts.research_telegram_signal_strategy import LocalMarket
from scripts.review_telegram_signals import FIXED_CUTOFF, entry_hour, load_signals

OUT = ROOT/'reports/alastar_wait10_20261022'
COHORT = ROOT/'data/research/alastar_horizons_20261022/paired_signals_5m.json'
WAIT_HOURS = (1, 4, 24)
CLOCKS = ('after_entry', 'signal_deadline')
STEP = pd.Timedelta(minutes=5)
SEED = 20261022


def freeze_plan(path, plan):
    canonical=json.loads(json.dumps(plan,ensure_ascii=False))
    if path.exists():
        old=json.loads(path.read_text(encoding='utf-8'))
        assert {k:v for k,v in old.items() if k!='created_at_utc'} == {k:v for k,v in canonical.items() if k!='created_at_utc'}, 'analysis plan changed'
    else:
        write(path,canonical)


class Bars:
    def __init__(self, frame, minutes=5):
        self.minutes = minutes
        self.step = pd.Timedelta(minutes=minutes)
        if not frame.index.is_unique:
            raise ValueError('duplicate timestamps')
        self.frame = frame.reindex(pd.date_range(frame.index.min(),frame.index.max(),freq=self.step)) if len(frame) else frame
        self.index = self.frame.index
        if not len(frame):
            self.values = np.empty((0,5)); self.good = np.array([],bool)
        else:
            self.values = self.frame[['open','high','low','close','volume']].to_numpy(float)
            o,h,l,c,v = self.values.T
            self.good = np.isfinite(self.values).all(1) & (self.values > 0).all(1)
            self.good &= (h >= np.maximum(o,c)) & (l <= np.minimum(o,c)) & (h >= l)
        self.bad_prefix = np.r_[0,np.cumsum(~self.good)]

    def position(self, at):
        found = self.index.get_indexer([pd.Timestamp(at)])[0]
        return int(found) if found >= 0 else None

    def complete(self, start, end):
        return 0 <= start <= end < len(self.index) and self.bad_prefix[end+1] == self.bad_prefix[start]

    def outcome(self, entry, end, price=None, risk_start=None):
        if entry is None or end <= entry or entry >= len(self.index) or not self.good[entry]:
            return {'status':'missing_entry_or_exit'}
        price = float(self.values[entry,0]) if price is None else float(price)
        risk_start = entry if risk_start is None else risk_start
        if not np.isfinite(price) or price <= 0 or not 0 <= risk_start <= entry:
            raise ValueError('invalid entry price or risk start')
        high = self.values[risk_start:end,1]
        bad = np.flatnonzero(~self.good[risk_start:end])
        hits = np.flatnonzero((high >= price*1.99) & self.good[risk_start:end])
        first_bad = int(bad[0]) if len(bad) else len(high)
        hit = int(hits[0]) if len(hits) else len(high)
        lost = hit < first_bad
        if not lost and (len(bad) or end >= len(self.index) or not self.good[end]):
            return {'status':'missing_holding_path'}
        full = not len(bad) and end < len(self.index) and self.good[end]
        mae = max(0., max(float(np.max(high)),float(self.values[end,0]))/price-1) if full else None
        net = -1. if lost else max(-1.,1-float(self.values[end,0])/price-.002)
        exposure_end = risk_start+hit+1 if lost else end
        return {'status':'filled','net':net,'entry_price':price,'exit_price':None if lost else float(self.values[end,0]),
                'entry_at':str(self.index[entry]),'exit_at':str(self.index[0]+end*self.step),
                'exposure_start_at':str(self.index[risk_start]),'exposure_end_at':str(self.index[0]+exposure_end*self.step),
                'liquidated_1x_proxy':lost,'liquidation_bar_at':str(self.index[risk_start+hit]) if lost else None,
                'maximum_adverse_rise':mae,'full_risk_path':bool(full)}

    def policy(self, anchor, hours, wait=24, clock='after_entry', execution='next_open'):
        if clock not in CLOCKS or execution not in ('next_open','ideal_limit'):
            raise ValueError('unregistered policy')
        if anchor is None or not self.good[anchor]:
            return {'status':'missing_anchor'}
        hold = int(hours*60/self.minutes); budget = int(wait*60/self.minutes)
        deadline = anchor+hold
        trigger = float(Decimal(str(self.values[anchor,0]))*Decimal('1.10'))
        # Entry at the wait deadline is allowed. Entry at the exit is expired.
        for j in range(anchor,anchor+budget):
            if clock == 'signal_deadline' and j+1 >= deadline:
                return {'status':'unfilled','reason':'signal_exit_before_entry','net':0.,'wait_hours':None}
            if j >= len(self.index) or not self.good[j]:
                return {'status':'missing_wait_path'}
            if self.values[j,1] < trigger:
                continue
            entry = j+1
            if entry >= len(self.index) or not self.good[entry]:
                return {'status':'missing_trigger_entry'}
            end = entry+hold if clock == 'after_entry' else deadline
            # Ideal limit execution has unknown within-bar time. Hold clock is
            # rounded to touch-bar end; that bar's high still counts as exposure.
            result = self.outcome(entry,end,price=trigger if execution=='ideal_limit' else None,
                                  risk_start=j if execution=='ideal_limit' else entry)
            result.update(trigger_bar_at=str(self.index[j]), trigger_price=float(trigger),
                          wait_hours=(entry-anchor)*self.minutes/60,
                          entry_improvement=(result['entry_price']/self.values[anchor,0]-1) if result['status']=='filled' else None,
                          execution=execution)
            return result
        return {'status':'unfilled','reason':'no_10pct_touch_in_budget','net':0.,'wait_hours':None}


def funded(result, market, symbol):
    if result['status'] == 'unfilled':
        return 0.
    if result['status'] != 'filled':
        return None
    a,b = pd.Timestamp(result['exposure_start_at']),pd.Timestamp(result['exposure_end_at'])
    if not market.funding_coverage_ok(symbol,a-pd.Timedelta(seconds=1),b+pd.Timedelta(seconds=1)):
        return None
    f = funding_range(market.funding_for(symbol),a,b,result['entry_price'],-1,complete_query=True)
    if f is None:
        return None
    return -1. if result['liquidated_1x_proxy'] else max(-1.,result['net']+f['lower'])


def cell(rows, group, hours, wait, clock, execution, phase):
    selected = [r for r in rows if (phase=='all' or r['phase']==phase)]
    pairs = [r for r in selected if r['immediate']['status']=='filled' and r['delayed']['status'] in ('filled','unfilled')]
    filled = [r for r in pairs if r['delayed']['status']=='filled']
    unfilled = [r for r in pairs if r['delayed']['status']=='unfilled']
    old = np.array([r['immediate']['net'] for r in pairs]); new = np.array([r['delayed']['net'] for r in pairs])
    delta = new-old
    def mean(values): return float(np.mean(values)) if len(values) else None
    ci,weeks = week_interval(delta,[pd.Timestamp(r['anchor_at']) for r in pairs]) if len(pairs) else (None,0)
    risk0 = [r['immediate']['maximum_adverse_rise'] for r in pairs if r['immediate'].get('maximum_adverse_rise') is not None]
    risk1 = [r['delayed']['maximum_adverse_rise'] for r in filled if r['delayed'].get('maximum_adverse_rise') is not None]
    funds = [r for r in pairs if r['immediate_funded'] is not None and r['delayed_funded'] is not None]
    complete_null = [r for r in pairs if r.get('hourly_control') and r['hourly_control']['null_n']>=100]
    # Exact algebra: execution/timing gain on triggered signals plus avoided
    # baseline returns on untriggered signals, both divided by all paired signals.
    execution_part = sum(r['delayed']['net']-r['immediate']['net'] for r in filled)/len(pairs) if pairs else None
    selection_part = -sum(r['immediate']['net'] for r in unfilled)/len(pairs) if pairs else None
    if pairs: assert abs(execution_part+selection_part-float(delta.mean())) < 1e-12
    return {'cohort':group,'hours':hours,'wait_hours':wait,'clock':clock,'execution':execution,'phase':phase,
            'selected_n':len(selected),'paired_n':len(pairs),'missing_n':len(selected)-len(pairs),
            'filled_n':len(filled),'unfilled_n':len(unfilled),'fill_rate':len(filled)/len(pairs) if pairs else None,
            'immediate_mean':mean(old),'delayed_per_signal_mean':mean(new),'paired_policy_gain':mean(delta),
            'gain_week_ci95':ci,'weeks':weeks,'delayed_per_fill_mean':mean([r['delayed']['net'] for r in filled]),
            'immediate_on_filled_mean':mean([r['immediate']['net'] for r in filled]),
            'immediate_on_unfilled_mean':mean([r['immediate']['net'] for r in unfilled]),
            'execution_timing_contribution':execution_part,'selection_contribution':selection_part,
            'median_wait_hours':float(np.median([r['delayed']['wait_hours'] for r in filled])) if filled else None,
            'mean_entry_improvement':mean([r['delayed']['entry_improvement'] for r in filled]),
            'immediate_proxy_loss_n':sum(r['immediate']['liquidated_1x_proxy'] for r in pairs),
            'delayed_proxy_loss_n':sum(r['delayed']['liquidated_1x_proxy'] for r in filled),
            'immediate_risk_n':len(risk0),'delayed_risk_n':len(risk1),
            'immediate_mae_p90':float(np.quantile(risk0,.9)) if risk0 else None,
            'delayed_mae_p90':float(np.quantile(risk1,.9)) if risk1 else None,
            'immediate_cross20_n':sum(v>=.2 for v in risk0),'delayed_cross20_n':sum(v>=.2 for v in risk1),
            'delayed_mae_max':max(risk1) if risk1 else None,
            'immediate_mean_80bps':mean([max(-1.,r['immediate']['net']-.006) for r in pairs]),
            'delayed_per_signal_mean_80bps':mean([max(-1.,r['delayed']['net']-.006) if r['delayed']['status']=='filled' else 0. for r in pairs]),
            'funding_paired_n':len(funds),'immediate_funded_mean':mean([r['immediate_funded'] for r in funds]),
            'delayed_funded_per_signal_mean':mean([r['delayed_funded'] for r in funds]),
            'hourly_control_n':len(complete_null),
            'hourly_actual_policy_gain':mean([r['hourly_control']['actual_delta'] for r in complete_null]),
            'hourly_random_policy_gain':mean([r['hourly_control']['null_mean_delta'] for r in complete_null]),
            'hourly_policy_gain_excess':mean([r['hourly_control']['actual_delta']-r['hourly_control']['null_mean_delta'] for r in complete_null]),
            'gain_without_best_signal':float(np.delete(delta,delta.argmax()).mean()) if len(delta)>1 else None,
            'gain_without_best_immediate_signal':float(np.delete(delta,old.argmax()).mean()) if len(delta)>1 else None}


def run():
    before = protected_hashes()
    source_raw=Path(__file__).read_bytes(); source_sha=hashlib.sha256(source_raw).hexdigest()
    save_bytes(OUT/'source_snapshots'/f'{source_sha}.py',source_raw)
    legacy = json.loads(COHORT.read_text(encoding='utf-8'))
    cohorts = {k:[r for r in v if r['side']==-1] for k,v in legacy.items()}
    labels,_ = load_signals()
    pool = sorted([s for s in labels if s['channel']=='alastartrades' and s['kind']=='fresh_call' and s['side']==-1],key=lambda s:(s['published_at'],s['signal_id']))
    until,kept,excluded = {},[],[]
    for s in pool:
        at=(pd.Timestamp(s['published_at'])+pd.Timedelta(seconds=60)).ceil('5min')
        if at < until.get(s['symbol'],at):
            excluded.append(s['signal_id']); continue
        kept.append(s); until[s['symbol']]=at+pd.Timedelta(hours=192)
    assert {s['signal_id'] for s in kept} == {s['signal_id'] for s in cohorts['text_fresh']}
    plan={'created_at_utc':datetime.now(timezone.utc).isoformat(),'question':'Short only after price first rallies 10% from a call baseline?',
          'primary_cohort':'text_fresh','fresh_short_label_pool':len(pool),'fresh_192h_nonoverlap_n':len(kept),'excluded_overlap_ids':excluded,
          'secondary_cohorts':['text_broad','image_short'],'horizons':HORIZONS,'waiting_budgets_hours':WAIT_HOURS,'primary_wait_hours':24,
          'threshold':.10,'reference':'next5m open at/after publication+60sec; historical proxy, not real receive time',
          'primary_execution':'first completed5m high>=1.1*baseline, next5m open; max wait includes entry',
          'limit_sensitivity':'ideal resting short at exactly1.1*baseline; ignores queue/partial fill; holding clock rounded to touch-bar end, touch-bar high counts',
          'exit_clocks':CLOCKS,'unfilled':'zero return per original signal, no fee; not an account return','missing':'unknown, never zero',
          'cost_bps':20,'stress_cost_bps':80,'margin':'same approximate1x99% adverse rise as frozen research; no mark-price liquidation',
          'null':'secondary internally consistent hourly model: same next-open policy, 30d hourly starts ending no later than anchor-192h; max192h path completed before real hourly anchor',
          'null_minimum':100,'phases_cutoff':str(FIXED_CUTOFF),'statistics':'week bootstrap5000 descriptive95% intervals; no winning-parameter claim',
          'historical_exploratory_only':True,'active_ledger_changed':False}
    freeze_plan(OUT/'analysis_plan.json',plan)
    market,local = Market(),LocalMarket()
    five,hourly,null_cache = {},{},{}
    records,cells,nulls = [],[],[]
    for group,signals in cohorts.items():
        for s in signals:
            symbol=s['symbol']; at=pd.Timestamp(s['entry_at']); ah=entry_hour(s['published_at'])
            if symbol not in five: five[symbol]=Bars(market.prices_for(symbol))
            if symbol not in hourly: hourly[symbol]=Bars(local.frame(symbol),60)
            b=five[symbol]; a=b.position(at); hb=hourly[symbol]; hp=hb.position(ah)
            # Primary null candidate paths all end before the signal. Require a
            # common complete192h path, including wait, for every hourly start.
            candidates=[] if hp is None else [k for k in range(max(0,hp-720),hp-192+1) if hb.complete(k,k+192)]
            for hours in HORIZONS:
                immediate=b.outcome(a,a+hours*12) if a is not None else {'status':'missing_anchor'}
                funded0=funded(immediate,market,symbol)
                hi=HORIZONS.index(hours)
                assert immediate['status']=='filled' and abs(immediate['net']-s['net'][hi])<1e-10, (group,s['signal_id'],hours,'baseline drift')
                for wait in WAIT_HOURS:
                    for clock in CLOCKS:
                        for execution in ('next_open','ideal_limit') if wait==24 else ('next_open',):
                            delayed=b.policy(a,hours,wait,clock,execution)
                            control=None
                            if wait==24 and execution=='next_open':
                                key=(symbol,str(ah),hours,clock)
                                if key not in null_cache:
                                    real=hb.policy(hp,hours,24,clock) if hp is not None else {'status':'missing_anchor'}
                                    old=hb.outcome(hp,hp+hours) if hp is not None else {'status':'missing_anchor'}
                                    values=[]
                                    for k in candidates:
                                        x=hb.outcome(k,k+hours); y=hb.policy(k,hours,24,clock)
                                        assert x['status']=='filled' and y['status'] in ('filled','unfilled')
                                        values.append(y['net']-x['net'])
                                    valid_real=real['status'] in ('filled','unfilled') and old['status']=='filled'
                                    control={'null_n':len(values),'actual_delta':real['net']-old['net'] if valid_real else None,
                                             'null_mean_delta':float(np.mean(values)) if values else None,
                                             'actual_hourly_anchor':str(ah),'null_latest_end':str(hb.index[candidates[-1]+192]) if candidates else None,
                                             'null_profile_id':len(nulls)}
                                    if not valid_real: control=None
                                    null_cache[key]=control
                                    nulls.append({'symbol':symbol,'actual_hourly_anchor':str(ah),'hours':hours,'clock':clock,
                                                  'candidate_starts':[str(hb.index[k]) for k in candidates], 'paired_deltas':values,'control':control})
                                control=null_cache[key]
                            records.append({'cohort':group,'signal_id':s['signal_id'],'symbol':symbol,'published_at':s['published_at'],
                                            'anchor_at':str(at),'phase':'later' if at>=FIXED_CUTOFF else 'earlier',
                                            'hours':hours,'wait_hours':wait,'clock':clock,'execution':execution,
                                            'immediate':immediate,'delayed':delayed,'immediate_funded':funded0,
                                            'delayed_funded':funded(delayed,market,symbol),'hourly_control':control})
        for hours in HORIZONS:
            for wait in WAIT_HOURS:
                for clock in CLOCKS:
                    for execution in ('next_open','ideal_limit') if wait==24 else ('next_open',):
                        subset=[r for r in records if r['cohort']==group and r['hours']==hours and r['wait_hours']==wait and r['clock']==clock and r['execution']==execution]
                        for phase in ('all','earlier','later'):
                            if any(phase=='all' or r['phase']==phase for r in subset): cells.append(cell(subset,group,hours,wait,clock,execution,phase))
        print(group,'signals',len(signals),'done',flush=True)
    after=protected_hashes(); assert before==after
    write(OUT/'summary.json',{'cells':cells})
    write(OUT/'records.json',records)
    write(OUT/'hourly_null_profiles.json',nulls)
    save_bytes(OUT/'WAIT10_COMPARISON.csv',pd.DataFrame(cells).to_csv(index=False).encode('utf-8-sig'))
    write(OUT/'run_receipt.json',{'completed_at_utc':datetime.now(timezone.utc).isoformat(),
                                 'script_sha256':source_sha,
                                 'cohort_sha256':hashlib.sha256(COHORT.read_bytes()).hexdigest(),
                                 'sources':{**market.sources,**local.sources},'protected_before':before,'protected_after':after,'protected_unchanged':before==after,
                                 'output_sha256':{name:hashlib.sha256((OUT/name).read_bytes()).hexdigest() for name in ('analysis_plan.json','summary.json','records.json','hourly_null_profiles.json')}})
    for c in cells:
        if c['cohort']=='text_fresh' and c['wait_hours']==24 and c['execution']=='next_open' and c['phase']=='all' and c['clock']=='after_entry' and c['hours'] in (24,72,168):
            print(json.dumps({k:c[k] for k in ('hours','paired_n','filled_n','immediate_mean','delayed_per_signal_mean','paired_policy_gain')},ensure_ascii=True),flush=True)
    return cells


if __name__=='__main__':
    run()
