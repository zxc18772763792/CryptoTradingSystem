"""Independent v3 paired horizon observations. Public evidence only, no orders.

All five exits are frozen together before one real-time observed entry. This is
a signal cohort comparison, not five independent trades or an account portfolio.
The previously activated v2 book is neither rewritten nor used as an intake gate.
"""
from __future__ import annotations

import asyncio
from collections import deque
from datetime import timedelta
import json
import os
from pathlib import Path

from core.news.alastar_observation import DEFAULT_OUTPUT, _archive_lock, _atomic_bytes, _json_bytes, _load_runs, _utc
from core.research.alastar_forward_paper import PaperJournal, SYMBOL, bars_by_time, ceil5, now_utc, number, sha
from scripts.observe_alastar_paper import OfficialEvidence, FUNDING_ENDPOINT

ROOT=Path(__file__).resolve().parents[2]
OUTPUT=ROOT/'data/research/alastar_horizons_v3'
PROTOCOL=ROOT/'data/research/alastar_horizons_20261022/prospective_horizon_protocol_v3.json'
HOURS=(1,4,24,72,168)
POLL_SECONDS=5
STEP_MS=300000


def price_outcome(bars, at, hours, side, cost_bps=20):
    if hours not in HOURS or type(side) is not int or side not in(-1,1): raise ValueError('Unsupported horizon/side')
    start=int(_utc(at).timestamp()*1000); steps=hours*12
    if start not in bars: return None
    p0=bars[start][0]
    for i in range(steps):
        stamp=start+i*STEP_MS
        if stamp not in bars: return None
        b=bars[stamp]
        if (b[1]>=p0*1.99 if side==-1 else b[2]<=p0*.005):
            return {'entry_price':p0,'exit_price':None,'net_price_only':-1.,'liquidated':True,'liquidation_bar_start_ms':stamp}
    end=start+steps*STEP_MS
    if end not in bars: return None
    return {'entry_price':p0,'exit_price':bars[end][0], 'net_price_only':max(-1.,side*(bars[end][0]/p0-1)-cost_bps/10000),'liquidated':False}


def null_profile(rows, entry, side, minimum=2000):
    """Linear-time horizon arrays; missing paths and known losses match the live model."""
    if _utc(entry)!=ceil5(entry): raise ValueError('Entry must be aligned to5m')
    bars=bars_by_time(rows); stop=int(_utc(entry).timestamp()*1000); start=stop-30*86400000
    grid=[bars.get(t) for t in range(start,stop+1,STEP_MS)]; n=len(grid)
    next_bad=[n]*n; bad=n
    for i in range(n-1,-1,-1):
        if grid[i] is None: bad=i
        next_bad[i]=bad
    values={}
    for hours in HOURS:
        steps=hours*12; out=[None]*n; q=deque(); right=0
        for i in range(n-steps):
            end=min(i+steps,next_bad[i])
            while right<end:
                x=grid[right]
                if x is not None:
                    value=x[1] if side==-1 else -x[2]
                    while q and q[-1][1]<=value: q.pop()
                    q.append((right,value))
                right+=1
            while q and q[0][0]<i: q.popleft()
            b=grid[i]
            if b is None: continue
            hit=q and (q[0][1]>=b[0]*1.99 if side==-1 else -q[0][1]<=b[0]*.005)
            if hit: out[i]=-1.
            elif next_bad[i]>=i+steps and grid[i+steps] is not None:
                out[i]=max(-1.,side*(grid[i+steps][0]/b[0]-1)-.002)
        values[hours]=out
    common=[i for i in range(n-max(HOURS)*12) if all(values[h][i] is not None for h in HOURS)]
    result={'common_n':len(common),'common_status':'ok' if len(common)>=minimum else 'insufficient_preentry_history',
            'latest_null_price_at':_utc(entry).isoformat(),'cost_bps':20,'funding_in_null':False,'horizons':{}}
    for h in HOURS:
        own=[v for v in values[h] if v is not None]
        result['horizons'][str(h)]={'n':len(own),'mean':sum(own)/len(own) if own else None,
                                  'status':'ok' if len(own)>=minimum else 'insufficient_preentry_history',
                                  'common_mean':sum(values[h][i] for i in common)/len(common) if common else None}
    return result


def funding_net(rows, proof, price, side, start, end, outcome):
    if proof.get('complete_query_coverage') is not True: return None
    a,b=int(_utc(start).timestamp()*1000),int(_utc(end).timestamp()*1000); seen=set(); total=0.
    for r in rows:
        t=int(r['fundingTime'])
        if t in seen or not a<=t<=b: raise ValueError('Duplicate/out-of-window funding')
        seen.add(t); liquidation=outcome.get('liquidation_bar_start_ms')
        if liquidation is not None and t>=liquidation+STEP_MS: continue
        v=-side*number(r['fundingRate'])*number(r['markPrice'],positive=True)/price
        boundary=min(abs(t-a),abs(t-b))<=1000 or (liquidation is not None and t>=liquidation)
        total+=min(0.,v) if boundary else v
    return -1. if outcome['liquidated'] else max(-1.,outcome['net_price_only']+total)


class PublicEvidence(OfficialEvidence):
    def funding(self,symbol,start,end):
        start,end=_utc(start),_utc(end)
        if not SYMBOL.fullmatch(symbol) or end-start not in [timedelta(hours=h) for h in HOURS] or end>now_utc():
            raise ValueError('Invalid completed public funding window')
        a,b=int(start.timestamp()*1000),int(end.timestamp()*1000)
        rows,ref=self._fetch(FUNDING_ENDPOINT,{'symbol':symbol,'startTime':a,'endTime':b,'limit':1000})
        if not isinstance(rows,list) or len(rows)>=1000 or any(r.get('symbol')!=symbol for r in rows):
            raise ValueError('Funding query incomplete or wrong symbol')
        return rows,{**ref,'complete_query_coverage':True,'start_ms':a,'end_ms':b}


def runtime(output):
    p=Path(output)/'runtime.json'
    return json.loads(p.read_bytes()) if p.exists() else {'processed_labels':{},'observations':{},'attempts':{},'errors':[]}


class Journal:
    def __init__(self,output=OUTPUT,archive=DEFAULT_OUTPUT,protocol=PROTOCOL):
        self.output,self.archive,self.protocol_path=map(Path,(output,archive,protocol))
        self.protocol_raw=self.protocol_path.read_bytes(); self.protocol=json.loads(self.protocol_raw)
        if (self.protocol['protocol_id']!='alastar_paired_horizons_v3' or self.protocol['holding_hours']!=list(HOURS)
                or self.protocol['cost_bps']!=20 or self.protocol['minimum_null_starts']!=2000): raise ValueError('Unsupported v3 protocol')
        self.protocol_sha=sha(self.protocol_raw)
        self.paths={'v3':Path(__file__),'v2_core':Path(__file__).with_name('alastar_forward_paper.py'),
                    'public_provider':ROOT/'scripts/observe_alastar_paper.py','v3_cli':ROOT/'scripts/observe_alastar_horizons.py'}
        self.sources={k:sha(p.read_bytes()) for k,p in self.paths.items()}
        self.implementation_sha=sha(_json_bytes(self.sources))

    def health(self): return PaperJournal.health(self)

    def _records(self):
        if sha(self.protocol_path.read_bytes())!=self.protocol_sha or any(sha(p.read_bytes())!=self.sources[k] for k,p in self.paths.items()):
            raise RuntimeError('Source/protocol drift; stop v3 and migrate explicitly')
        rows=[]; previous=None; last=None
        for i,p in enumerate(sorted((self.output/'journal').glob('*.json')),1):
            raw=p.read_bytes(); r=json.loads(raw); at=_utc(r['recorded_at_utc'])
            if (p.name!=f'{i:012d}.json' or r['sequence']!=i or r['previous_sha256']!=previous or r['protocol_sha256']!=self.protocol_sha
                    or r['implementation_sha256']!=self.implementation_sha or (last and at<last)): raise RuntimeError('v3 journal hash/clock mismatch')
            rows.append(r); previous=sha(raw); last=at
        cp=self.output/'checkpoint.json'
        if cp.exists():
            c=json.loads(cp.read_bytes()); p=self.output/'journal'/f"{c['sequence']:012d}.json"
            if c['sequence']>len(rows) or sha(p.read_bytes())!=c['sha256']: raise RuntimeError('v3 journal checkpoint mismatch')
        return rows

    def _append(self,kind,payload):
        rows=self._records(); at=now_utc()
        if rows and at<_utc(rows[-1]['recorded_at_utc']): raise RuntimeError('Local clock moved backwards')
        previous=sha((self.output/'journal'/f'{len(rows):012d}.json').read_bytes()) if rows else None
        r={'sequence':len(rows)+1,'previous_sha256':previous,'protocol_sha256':self.protocol_sha,'implementation_sha256':self.implementation_sha,
           'recorded_at_utc':at.isoformat(),'kind':kind,'payload':payload}
        p=self.output/'journal'/f"{r['sequence']:012d}.json"
        if p.exists(): raise RuntimeError('Refusing to overwrite committed v3 record')
        raw=_json_bytes(r); _atomic_bytes(p,raw); _atomic_bytes(self.output/'checkpoint.json',_json_bytes({'sequence':r['sequence'],'sha256':sha(raw)}))
        return r

    def _state(self):
        records=self._records(); events={}
        for r in records:
            p=r['payload']
            if r['kind']=='freeze':
                for e in p['events']: events[e['event_id']]={**e,'freeze':p,'freeze_committed_at':r['recorded_at_utc'],'outcomes':{}}
            elif r['kind'] in ('entry','missed','null','settlement','funding'):
                e=events[p['event_id']]
                if r['kind']=='entry': e.update(status='observing',entry=p)
                elif r['kind']=='missed': e.update(status='skipped',missed=p)
                elif r['kind']=='null': e['null']=p['profile']
                elif r['kind']=='funding': e['outcomes'][str(p['hours'])].update(net_with_funding=p['net_with_funding'],funding_evidence=p['funding_evidence'])
                else:
                    e['outcomes'][str(p['hours'])]=p
                    if len(e['outcomes'])==len(HOURS): e['status']='completed'
        return records,events

    def activate(self,validation,clock):
        health=self.health()
        if not validation.get('ok') or not validation.get('audit_sha256') or validation.get('implementation_sha256')!=self.implementation_sha or not clock.get('ok') or not health['healthy_for_new_freezes']:
            raise RuntimeError('v3 activation gate failed')
        with _archive_lock(self.output):
            if self._records(): raise RuntimeError('v3 activation already exists; never reset it')
            return self._append('activation',{'protocol_snapshot':self.protocol,'sources':self.sources,'clock_check':clock,'collector_health':health,'validation':validation,'paper_only':True})

    def gate(self,records,events):
        if not records: return 'not_activated'
        activation=_utc(records[0]['recorded_at_utc'])
        try: deadline=activation.replace(year=activation.year+2)
        except ValueError: deadline=activation.replace(year=activation.year+2,day=28)
        if now_utc()>=deadline: return 'maximum24months_reached'
        done=sorted((e for e in events.values() if e['status']=='completed'),key=lambda e:(e['entry']['entry_at'],e['event_id']))
        active=sum(e['status']=='observing' for e in events.values())
        if len(done)>=66: return '66cohort_review_due'
        if len(done)>=40:
            if any(e.get('null',{}).get('common_status')!='ok' for e in done[:40]): return '40cohort_null_incomplete'
            means=[sum(e['outcomes'][str(h)]['net_price_only']-e['null']['horizons'][str(h)]['common_mean'] for e in done[:40])/40 for h in HOURS]
            if max(means)<=0: return '40cohort_all_horizons_futility_stop'
        if len(done)<40 and len(done)+active>=40: return 'await40cohort_maturity'
        if len(done)+active>=66: return 'await66cohort_maturity'
        return None

    def freeze(self,label):
        if not isinstance(label,dict): raise ValueError('Label must be an object')
        if any(k in label for k in ('label_frozen_at_utc','first_received_at_utc','published_at_utc')): raise ValueError('External timestamps forbidden')
        source=label['source_id']; items=label.get('events')
        if not isinstance(items,list) or not 1<=len(items)<=10: raise ValueError('Supply1to10 reviewed events')
        if any(not isinstance(label.get(k),str) or not label[k].strip() for k in ('reviewer_or_model_version','rule_version')): raise ValueError('Missing reviewer/rule version')
        symbols=set(); sides=set()
        for e in items:
            if not isinstance(e,dict): raise ValueError('Event must be an object')
            if type(e.get('direction')) is not int or e['direction'] not in(-1,1) or not SYMBOL.fullmatch(e.get('symbol','')) or e['symbol'] in symbols: raise ValueError('Invalid/duplicate symbol or direction')
            symbols.add(e['symbol']); sides.add(e['direction']); number(e['confidence'])
            if any(type(e.get(k)) is not bool for k in ('is_fresh_entry_view','requires_unavailable_confirmation','opposed_directions_in_message')): raise ValueError('Explicit semantic booleans required')
        health=self.health()
        with _archive_lock(self.archive):
            _,state,_=_load_runs(self.archive,verify_full=True)
            if source not in state: raise ValueError('No verified first receipt')
            item=state[source]; first=item['first']
            if any(not e.get('original_evidence_quote') or e['original_evidence_quote'] not in first['text'] for e in items): raise ValueError('Exact first text quote required')
            receipt_path=next(p for p in sorted((self.archive/'runs').glob('*.json')) if any(x['message_key']==source for x in json.loads(p.read_bytes())['new_revisions']))
            reasons=list(item.get('current_candidate_exclusion_reasons',first['exclusion_reasons']))
            if len(item['revisions'])>1: reasons.append('revised_before_freeze')
            with _archive_lock(self.output):
                records,current=self._state()
                if not records: raise RuntimeError('v3 is not activated')
                if any(r['kind']=='freeze' and r['payload']['source_id']==source for r in records): raise ValueError('Source already frozen')
                frozen=now_utc(); pub,receipt=_utc(first['published_at_utc']),_utc(first['observed_at_utc'])
                if pub>receipt or receipt>frozen: raise ValueError('Future publication/receipt')
                if pub<_utc(records[0]['recorded_at_utc']): reasons.append('predates_v3_activation')
                if not first['prospective_eligible_at_first_observation']: reasons.append('not_timely_live_first_receipt')
                if not health['healthy_for_new_freezes']: reasons.append('collector_health_failed')
                gate=self.gate(records,current)
                if gate: reasons.append(gate)
                entry=ceil5(max(receipt,frozen)+timedelta(seconds=60))
                if entry>pub+timedelta(minutes=30): reasons.append('entry_too_late')
                events=[]
                for i,e in enumerate(items):
                    why=list(dict.fromkeys(reasons))
                    if (e.get('reaction_type')!='immediate_view' or not e['is_fresh_entry_view'] or e['requires_unavailable_confirmation']
                            or e['opposed_directions_in_message'] or len(sides)>1 or e.get('chart_only',False) or not .65<=float(e['confidence'])<=1): why.append('not_unambiguous_text_fresh_call')
                    events.append({**e,'event_id':f'{source}:{i}','planned_entry_at':entry.isoformat(),
                                   'planned_exits':{str(h):(entry+timedelta(hours=h)).isoformat() for h in HOURS},'status':'skipped' if why else 'planned','exclusion_reasons':why})
                return self._append('freeze',{'source_id':source,'published_at_utc':pub.isoformat(),'first_received_at_utc':receipt.isoformat(),
                       'label_frozen_at_utc':frozen.isoformat(),'message_revision_sha256':first['revision_sha256'],'original_text':first['text'],
                       'first_receipt_run_file':receipt_path.relative_to(self.archive).as_posix(),'first_receipt_run_sha256':sha(receipt_path.read_bytes()),
                       'reviewer_or_model_version':label['reviewer_or_model_version'],'rule_version':label['rule_version'],'events':events,'collector_health':health})

    def tick(self,provider_factory,state):
        with _archive_lock(self.output):
            records,events=self._state()
            if not records: return []
            actions=[]; provider=None
            def evidence():
                nonlocal provider
                if provider is None: provider=provider_factory()
                return provider
            clock=None
            for e in sorted(events.values(),key=lambda x:(x['planned_entry_at'],x['event_id'])):
                if e['status']!='planned' or now_utc()<_utc(e['planned_entry_at']): continue
                at=_utc(e['planned_entry_at']); why=None
                if now_utc()>at+timedelta(seconds=30): why='missed_prospective_entry_window'
                elif _utc(e['freeze_committed_at'])>=at: why='freeze_not_committed_before_entry'
                elif self.gate(records,events): why=self.gate(records,events)
                elif any(x['symbol']==e['symbol'] and x.get('entry') and _utc(x['planned_entry_at'])+timedelta(hours=168)>at for x in events.values()): why='common_cohort_same_symbol_168h_overlap'
                if why:
                    actions.append(self._append('missed',{'event_id':e['event_id'],'reason':why})); e['status']='skipped'; continue
                try:
                    if clock is None: clock=evidence().clock_check(); state['last_clock_check']=clock
                    if not clock['ok']: raise RuntimeError('Fresh clock check failed')
                    raw,ref=evidence().klines(e['symbol'],at,at); price=bars_by_time(raw)[int(at.timestamp()*1000)][0]
                    if not at<=now_utc()<=at+timedelta(seconds=30): raise ValueError('Entry response crossed deadline')
                    r=self._append('entry',{'event_id':e['event_id'],'entry_at':at.isoformat(),'price':price,'captured_at_utc':now_utc().isoformat(),'evidence':ref})
                    actions.append(r); e.update(status='observing',entry=r['payload'])
                except (ValueError,KeyError,OSError,RuntimeError) as exc:
                    actions.append(self._append('entry_unavailable',{'event_id':e['event_id'],'reason':type(exc).__name__}))
            # One historical job per short pass; no historical entry reconstruction.
            for e in events.values():
                if not e.get('entry'): continue
                for h in HOURS:
                    key=f"{e['event_id']}:{h}"; end=_utc(e['planned_exits'][str(h)])
                    if str(h) in e['outcomes'] or now_utc()<end+timedelta(minutes=5): continue
                    if key in state['attempts'] and (now_utc()-_utc(state['attempts'][key])).total_seconds()<60: continue
                    state['attempts'][key]=now_utc().isoformat(); at=_utc(e['planned_entry_at'])
                    try:
                        raw,ref=evidence().klines(e['symbol'],at,end); outcome=price_outcome(bars_by_time(raw),at,h,e['direction'])
                        if outcome is None or outcome['entry_price']!=e['entry']['price']: raise ValueError('Incomplete path/changed entry evidence')
                        current_bars=bars_by_time(raw)
                        for old_h,old in e['outcomes'].items():
                            if int(old_h)>=h: continue
                            previous=price_outcome(current_bars,at,int(old_h),e['direction'])
                            if previous is None or previous['net_price_only']!=old['net_price_only'] or previous['liquidated']!=old['liquidated']:
                                raise ValueError('Previously recorded price outcome changed')
                        funded=None; funding_proof=None
                        try:
                            funds,funding_proof=evidence().funding(e['symbol'],at,end)
                            if any(f.get('symbol')!=e['symbol'] for f in funds): raise ValueError('Funding symbol mismatch')
                            funded=funding_net(funds,funding_proof,e['entry']['price'],e['direction'],at,end,outcome)
                        except (ValueError,KeyError,OSError,RuntimeError) as exc: funding_proof={'unavailable':type(exc).__name__}
                        actions.append(self._append('settlement',{'event_id':e['event_id'],'hours':h,**outcome,
                                       'net_price_only_40bps':max(-1.,outcome['net_price_only']-.002),'net_price_only_80bps':max(-1.,outcome['net_price_only']-.006),
                                       'net_with_funding':funded,'funding_evidence':funding_proof,'price_evidence':ref}))
                    except (ValueError,KeyError,OSError,RuntimeError) as exc:
                        actions.append(self._append('settlement_unavailable',{'event_id':e['event_id'],'hours':h,'reason':type(exc).__name__}))
                    return actions
                for h,outcome in e['outcomes'].items():
                    key=e['event_id']+':funding:'+h
                    if outcome['net_with_funding'] is not None or (key in state['attempts'] and (now_utc()-_utc(state['attempts'][key])).total_seconds()<60): continue
                    state['attempts'][key]=now_utc().isoformat(); at=_utc(e['planned_entry_at']); end=_utc(e['planned_exits'][h])
                    try:
                        funds,proof=evidence().funding(e['symbol'],at,end)
                        if any(f.get('symbol')!=e['symbol'] for f in funds): raise ValueError('Funding symbol mismatch')
                        funded=funding_net(funds,proof,e['entry']['price'],e['direction'],at,end,outcome)
                        if funded is None: raise ValueError('Funding coverage unknown')
                        actions.append(self._append('funding',{'event_id':e['event_id'],'hours':int(h),'net_with_funding':funded,'funding_evidence':proof}))
                    except (ValueError,KeyError,OSError,RuntimeError) as exc:
                        actions.append(self._append('funding_unavailable',{'event_id':e['event_id'],'hours':int(h),'reason':type(exc).__name__}))
                    return actions
                key=e['event_id']+':null'
                if e['outcomes'] and not e.get('null') and (key not in state['attempts'] or (now_utc()-_utc(state['attempts'][key])).total_seconds()>=60):
                    state['attempts'][key]=now_utc().isoformat(); at=_utc(e['planned_entry_at'])
                    try:
                        raw,ref=evidence().klines(e['symbol'],at-timedelta(days=30),at)
                        actions.append(self._append('null',{'event_id':e['event_id'],'profile':null_profile(raw,at,e['direction']),'evidence':ref}))
                    except (ValueError,KeyError,OSError,RuntimeError) as exc:
                        actions.append(self._append('null_unavailable',{'event_id':e['event_id'],'reason':type(exc).__name__}))
                    return actions
            return actions


class Tracker:
    def __init__(self,journal=None,provider_factory=None,managed=False):
        self.j=journal or Journal(); self.factory=provider_factory or (lambda:PublicEvidence(self.j.output)); self.managed=managed
    def save(self,r):
        r['errors']=r.get('errors',[])[-20:]; _atomic_bytes(self.j.output/'runtime.json',_json_bytes(r))
    def tick(self):
        with _archive_lock(self.j.output/'pass'):
            r=runtime(self.j.output); records,_=self.j._state()
            if records:
                active=_utc(records[0]['recorded_at_utc']); queue=json.loads((self.j.archive/'candidate_review_queue.json').read_bytes())
                if len(queue['candidates'])>5000: raise ValueError('Unbounded review queue')
                for c in queue['candidates']:
                    if _utc(c['published_at_utc'])<active: continue
                    status='expired_without_interpretation' if ceil5(now_utc()+timedelta(seconds=60))>_utc(c['published_at_utc'])+timedelta(minutes=30) else 'awaiting_interpretation'
                    r['observations'][c['message_key']]={'status':status,'source_id':c['message_key'],'published_at_utc':c['published_at_utc'],'first_received_at_utc':c['first_observed_at_utc'],'revision_sha256':c['revision_sha256'],'url':c['url']}
                seen={c['message_key'] for c in queue['candidates']}
                for key,item in r['observations'].items():
                    if key not in seen and item['status']=='awaiting_interpretation': item['status']='removed_from_current_review_queue'
                handled=0
                for inbox in (self.j.output/'labels_inbox',ROOT/'data/research/alastar_paper_v2/labels_inbox'):
                    for p in sorted(inbox.glob('*.json')):
                        if p.stat().st_size>1024*1024: continue
                        raw=p.read_bytes(); key=sha(raw)
                        if key in r['processed_labels']: continue
                        if handled>=10: break
                        handled+=1; _atomic_bytes(self.j.output/'evidence'/f'label_{key}.json',raw)
                        try:
                            record=self.j.freeze(json.loads(raw)); result={'status':'frozen','sequence':record['sequence']}
                        except RuntimeError as exc:
                            if 'owns the archive lock' in str(exc):
                                r['errors'].append({'at':now_utc().isoformat(),'type':'temporary_lock','file':p.name}); continue
                            raise
                        except (ValueError,KeyError,OSError) as exc: result={'status':'rejected','reason':str(exc)[:180]}
                        r['processed_labels'][key]={'file':p.name,'input_sha256':key,'observed_at_utc':now_utc().isoformat(),**result}
                actions=self.j.tick(self.factory,r)
                r['last_actions']=[{'kind':a['kind'],'event_id':a['payload'].get('event_id')} for a in actions]
                _,events=self.j._state()
                for e in events.values():
                    source=e['freeze']['source_id']
                    if source in r['observations']: r['observations'][source]['status']='label_frozen'
            r.update(last_tick_at_utc=now_utc().isoformat(),continuous_runner=self.managed,process_id=os.getpid(),last_pass_error=None)
            self.save(r); return r


def summary(journal=None):
    j=journal or Journal(); records,events=j._state(); r=runtime(j.output); stamp=r.get('last_tick_at_utc')
    running=bool(records and r.get('continuous_runner') and not r.get('last_pass_error') and stamp and 0<=(now_utc()-_utc(stamp)).total_seconds()<=30)
    common=[e for e in events.values() if len(e['outcomes'])==len(HOURS) and e.get('null',{}).get('common_status')=='ok']
    per=[]
    for h in HOURS:
        complete=[e for e in events.values() if str(h) in e['outcomes']]
        net=[e['outcomes'][str(h)]['net_price_only']*100 for e in common]
        excess=[(e['outcomes'][str(h)]['net_price_only']-e['null']['horizons'][str(h)]['common_mean'])*100 for e in common]
        gains=[((e['outcomes'][str(h)]['net_price_only']-e['null']['horizons'][str(h)]['common_mean'])-
                (e['outcomes']['1']['net_price_only']-e['null']['horizons']['1']['common_mean']))*100 for e in common]
        funded=[e['outcomes'][str(h)]['net_with_funding']*100 for e in common if e['outcomes'][str(h)]['net_with_funding'] is not None]
        avg=lambda x:sum(x)/len(x) if x else None
        per.append({'hours':h,'completed_n':len(complete),'common_n':len(common),'mean_net_pct':avg(net),'excess_pp':avg(excess),
                    'gain_vs_1h_pp':avg(gains),'funded_common_n':len(funded),'mean_funded_pct':avg(funded) if len(funded)==len(common) else None,
                    'mean_net_80bps_pct':avg([e['outcomes'][str(h)]['net_price_only_80bps']*100 for e in common])})
    gate=j.gate(records,events); activated=records[0]['recorded_at_utc'] if records else None
    frozen={e['freeze']['source_id'] for e in events.values()}
    pending=sum(x['status']=='awaiting_interpretation' and key not in frozen for key,x in r['observations'].items())
    return {'retirement':{'n':len(common),'min_n':66,'mean_pct':None,'ci90_pct':None,'verdict':'review_due' if gate and activated else 'collecting',
                          'label':'周期复核待处理' if gate and activated else '多周期积累样本' if activated else '未激活','reason':'1/4/24/72/168小时同信号同入场；仅共同成熟样本可比较，未指定优胜周期。'},
            'started_at':activated,'updated_at':stamp or activated,'runner_active':running,'continuous_runner':running,'paper_only':True,
            'tracker_status_label':'运行异常' if r.get('last_pass_error') else '运行中 · 多周期观察' if running else '运行心跳待恢复' if activated else '未激活',
            'pending_review':pending,'forward_open':sum(e['status']=='observing' for e in events.values()),'forward_waiting':sum(e['status']=='planned' for e in events.values()),
            'forward_late':sum(e.get('missed',{}).get('reason')=='missed_prospective_entry_window' for e in events.values()),
            'anchor_count':sum(bool(e.get('entry')) for e in events.values()),'horizon_comparison':per,'review_gate':gate,
            'not_a_portfolio_or_independent_trades':True,'semantic_review_required':True,'image_book_active':False}


def detail():
    j=Journal(); _,events=j._state(); r=runtime(j.output)
    return {'summary':summary(j),'events':list(events.values())[-100:],'observations':list(r['observations'].values())[-100:],
            'labels':list(r['processed_labels'].values())[-30:]}


async def worker(stop_event,heartbeat=None):
    tracker=Tracker(managed=True)
    with _archive_lock(tracker.j.output/'owner'):
        try:
            while not stop_event.is_set():
                try:
                    await asyncio.to_thread(tracker.tick)
                    if heartbeat: heartbeat(True)
                except Exception as exc:
                    r=runtime(tracker.j.output); r['errors'].append({'at':now_utc().isoformat(),'type':type(exc).__name__,'reason':str(exc)[:180]})
                    r['last_pass_error']=type(exc).__name__; tracker.save(r)
                    if heartbeat: heartbeat(False)
                try: await asyncio.wait_for(stop_event.wait(),timeout=POLL_SECONDS)
                except asyncio.TimeoutError: pass
        finally:
            tracker.managed=False; r=runtime(tracker.j.output); r['continuous_runner']=False; tracker.save(r)
