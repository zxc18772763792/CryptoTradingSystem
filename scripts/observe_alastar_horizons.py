"""Bounded v3 public-data audit, explicit activation and reviewed-label intake."""
from __future__ import annotations
import argparse
import json
import math
from pathlib import Path

from core.news.alastar_observation import _atomic_bytes, _json_bytes, _utc
from core.research.alastar_horizon_tracker import Journal, Tracker, PublicEvidence, HOURS, ROOT, OUTPUT, null_profile, now_utc, sha, summary


def audit(journal):
    """Recompute all five nulls independently from SHA-checked raw public pages."""
    old=ROOT/'data/research/alastar_paper_v2'
    raw=(old/'official_null_audit.json').read_bytes(); saved=json.loads(raw)
    validation=json.loads((old/'official_null_validation.json').read_bytes())
    if not validation.get('ok') or validation['audit_sha256']!=sha(raw): raise ValueError('Original independent audit changed')
    entry=_utc(saved['entry_at']); end=int(entry.timestamp()*1000); first=end-30*86400000
    bars={}; all_rows=[]
    for ref in saved['evidence']:
        p=(old/ref['file']).resolve()
        if not p.is_relative_to(old.resolve()) or sha(p.read_bytes())!=ref['sha256']: raise ValueError('Raw public evidence changed')
        page=json.loads(p.read_bytes())
        if page['endpoint']!='https://fapi.binance.com/fapi/v1/klines' or page['params']['symbol']!=saved['symbol']: raise ValueError('Wrong audit source')
        all_rows.extend(page['rows'])
        for r in page['rows']:
            t=int(r[0]); values=tuple(map(float,r[1:5]))
            if not first<=t<=end or t%300000 or any(not math.isfinite(x) or x<=0 for x in values): raise ValueError('Invalid raw row')
            o,h,lo,c=values
            if not lo<=min(o,c)<=max(o,c)<=h: raise ValueError('Invalid OHLC')
            if len(r)>=9 and (float(r[5])==0 or float(r[8])==0): continue
            if t in bars and bars[t]!=values: raise ValueError('Conflicting raw duplicates')
            bars[t]=values
    # Deliberately simple loops: does not call the production price/rolling code.
    values={h:{} for h in HOURS}; side=saved['direction']
    for h in HOURS:
        steps=h*12
        for t in range(first,end-steps*300000+1,300000):
            if t not in bars: continue
            p0=bars[t][0]; known=True; lost=False
            for k in range(steps):
                b=bars.get(t+k*300000)
                if b is None: known=False; break
                if (b[1]>=1.99*p0 if side==-1 else b[2]<=.005*p0): lost=True; break
            if not known: continue
            if lost: values[h][t]=-1.
            elif t+steps*300000 in bars: values[h][t]=max(-1.,side*(bars[t+steps*300000][0]/p0-1)-.002)
    common=set(values[HOURS[0]])
    for h in HOURS[1:]: common &= set(values[h])
    production=null_profile(all_rows,entry,side)
    differences={}
    for h in HOURS:
        actual=production['horizons'][str(h)]
        expected=math.fsum(values[h].values())/len(values[h])
        expected_common=math.fsum(values[h][t] for t in common)/len(common)
        if len(values[h])!=actual['n'] or abs(expected-actual['mean'])>1e-12 or abs(expected_common-actual['common_mean'])>1e-12: raise ValueError('Independent v3 null mismatch')
        differences[str(h)]={'n':actual['n'],'own_mean_difference':actual['mean']-expected,'common_mean_difference':actual['common_mean']-expected_common}
    if len(common)!=production['common_n'] or abs(production['horizons']['1']['mean']-saved['null']['mean_net_price_only'])>1e-12: raise ValueError('v2 reconciliation mismatch')
    result={'ok':True,'checked_at_utc':now_utc().isoformat(),'source_audit_sha256':sha(raw),'implementation_sha256':journal.implementation_sha,
            'symbol':saved['symbol'],'entry_at':entry.isoformat(),'side':side,'common_n':len(common),'differences':differences,
            'all_five_independently_recomputed':True,'overlapping_null_starts_are_not_independent_samples':True}
    data=_json_bytes(result); _atomic_bytes(journal.output/'null_validation.json',data)
    return {**result,'audit_sha256':sha(data)}


def main():
    p=argparse.ArgumentParser(description=__doc__); p.add_argument('action',choices=('status','audit','activate','freeze','tick'))
    p.add_argument('--label-file',type=Path); args=p.parse_args(); j=Journal()
    if args.action=='status': result={'summary':summary(j),'collector_health':j.health()}
    elif args.action=='audit': result=audit(j)
    elif args.action=='activate':
        path=j.output/'null_validation.json'; v=json.loads(path.read_bytes())
        if v['implementation_sha256']!=j.implementation_sha: raise ValueError('Implementation changed since independent audit')
        snapshots=[]
        for rel,expected in j.protocol['research_reference_sha256'].items():
            target=(ROOT/rel).resolve()
            if not target.is_relative_to(ROOT) or sha(target.read_bytes())!=expected: raise ValueError('Research reference changed')
            filename=f'evidence/research_{expected}{target.suffix}'
            if not (j.output/filename).exists(): _atomic_bytes(j.output/filename,target.read_bytes())
            snapshots.append({'source':rel,'file':filename,'sha256':expected})
        result=j.activate({**v,'audit_sha256':sha(path.read_bytes()),'research_snapshots':snapshots},PublicEvidence(j.output).clock_check())
    elif args.action=='freeze':
        if args.label_file is None or args.label_file.stat().st_size>1024*1024: p.error('A bounded label file is required')
        result=j.freeze(json.loads(args.label_file.read_text(encoding='utf-8-sig')))
    else: result=Tracker(journal=j).tick()
    print(json.dumps(result,ensure_ascii=False,indent=2,allow_nan=False))


if __name__=='__main__': main()
