"""Independent replay: stdlib price paths plus direct parquet reading for nulls.

Deliberately imports none of the production or analysis path/market helpers.
"""
from datetime import datetime,timezone
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
from statistics import mean

import pandas as pd

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'reports/alastar_wait10_20261022'
FIVE=300000
HOUR=3600000


def ms(value): return int(datetime.fromisoformat(value.replace('Z','+00:00')).timestamp()*1000)
def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def bar(values):
    o,h,l,c,v=map(float,values)
    good=all(math.isfinite(x) and x>0 for x in (o,h,l,c,v)) and l<=min(o,c)<=max(o,c)<=h
    return o,h,l,c,v,good


def price_result(prices,entry,end,step,price=None,risk_start=None):
    first=prices.get(entry)
    if not first or not first[5] or end<=entry: return {'status':'missing'}
    price=first[0] if price is None else price
    risk_start=entry if risk_start is None else risk_start
    loss=None; high=0.; complete=True
    for stamp in range(risk_start,end,step):
        row=prices.get(stamp)
        if not row or not row[5]:
            if loss is None: return {'status':'missing'}
            complete=False; break
        high=max(high,row[1])
        if loss is None and row[1]>=price*1.99: loss=stamp
    last=prices.get(end)
    if not last or not last[5]:
        if loss is None: return {'status':'missing'}
        complete=False
    return {'status':'filled','net':-1. if loss is not None else max(-1.,1-last[0]/price-.002),
            'entry_price':price,'maximum_adverse_rise':max(0.,max(high,last[0])/price-1) if complete else None,
            'liquidated_1x_proxy':loss is not None,'entry_ms':entry,'end_ms':end,
            'exposure_start_ms':risk_start,'exposure_end_ms':loss+step if loss is not None else end}


def delayed_result(prices,anchor,hours,wait,clock,execution,step):
    first=prices.get(anchor)
    if not first or not first[5]: return {'status':'missing'}
    threshold=float(Decimal(str(first[0]))*Decimal('1.10'))
    deadline=anchor+hours*HOUR
    for touch in range(anchor,anchor+wait*HOUR,step):
        if clock=='signal_deadline' and touch+step>=deadline: return {'status':'unfilled','net':0.}
        row=prices.get(touch)
        if not row or not row[5]: return {'status':'missing'}
        if row[1]<threshold: continue
        entry=touch+step
        next_bar=prices.get(entry)
        if not next_bar or not next_bar[5]: return {'status':'missing'}
        end=entry+hours*HOUR if clock=='after_entry' else deadline
        result=price_result(prices,entry,end,step,threshold if execution=='ideal_limit' else None,
                            touch if execution=='ideal_limit' else entry)
        if result['status']=='filled':
            result.update(wait_hours=(entry-anchor)/HOUR,entry_improvement=result['entry_price']/first[0]-1,
                          trigger_ms=touch)
        return result
    return {'status':'unfilled','net':0.}


def run():
    receipt=json.loads((OUT/'run_receipt.json').read_text(encoding='utf-8'))
    for name,expected in receipt['output_sha256'].items(): assert sha(OUT/name)==expected,name
    assert sha(ROOT/'scripts/research_alastar_wait10.py')==receipt['script_sha256']
    prices,funds,covers,hourly={},{},{},{}
    for name,proof in receipt['sources'].items():
        path=ROOT/name
        assert sha(path)==proof['sha256'],name
        normalized=name.replace('\\','/')
        if '/official_5m/' in normalized:
            page=json.loads(path.read_text(encoding='utf-8')); assert page['status']=='ok'
            assert page['params']['interval']=='5m'
            dst=prices.setdefault(page['params']['symbol'],{})
            for raw in page['rows']:
                item=bar(raw[1:6]); stamp=int(raw[0])
                if stamp in dst: assert dst[stamp]==item
                dst[stamp]=item
        elif '/official_funding/' in normalized:
            page=json.loads(path.read_text(encoding='utf-8')); assert page['status']=='ok'
            symbol=page['params']['symbol']; dst=funds.setdefault(symbol,{})
            rows=page['rows']; end=page['params']['endTime'] if len(rows)<1000 else rows[-1]['fundingTime']
            covers.setdefault(symbol,[]).append((page['params']['startTime'],end))
            for raw in rows:
                item=float(raw['fundingRate']),float(raw['markPrice']); stamp=int(raw['fundingTime'])
                if stamp in dst: assert dst[stamp]==item
                dst[stamp]=item
        elif name.endswith('.parquet'):
            symbol=path.parent.name.replace('_','') if path.parent.name.endswith('_USDT') else path.parent.parent.name.replace('_','')
            dst=hourly.setdefault(symbol,{})
            frame=pd.read_parquet(path); frame.index=pd.to_datetime(frame.index,utc=True)
            for at,values in zip(frame.index,frame[['open','high','low','close','volume']].itertuples(index=False,name=None)):
                dst[int(at.value//1000000)]=bar(values)
    maximum_error=0.; funding_checked=0
    def close(actual,expected):
        nonlocal maximum_error
        if actual is None or expected is None:
            assert actual is expected,(actual,expected);return
        maximum_error=max(maximum_error,abs(actual-expected))
        assert abs(actual-expected)<1e-12,(actual,expected)
    def compare(actual,expected):
        assert (actual['status']=='missing') == expected['status'].startswith('missing')
        if actual['status']=='missing': return
        assert actual['status']==expected['status']
        close(actual['net'],expected['net'])
        if actual['status']=='filled':
            for field in ('entry_price','maximum_adverse_rise'): close(actual[field],expected[field])
            assert actual['liquidated_1x_proxy']==expected['liquidated_1x_proxy']
            assert actual['entry_ms']==ms(expected['entry_at']) and actual['end_ms']==ms(expected['exit_at'])
            assert actual['exposure_start_ms']==ms(expected['exposure_start_at']) and actual['exposure_end_ms']==ms(expected['exposure_end_at'])
            if 'wait_hours' in actual:
                close(actual['wait_hours'],expected['wait_hours']); close(actual['entry_improvement'],expected['entry_improvement'])
                assert actual['trigger_ms']==ms(expected['trigger_bar_at'])
    def fund(result,symbol):
        if result['status']=='unfilled': return 0.
        if result['status']!='filled': return None
        start,end=result['exposure_start_ms'],result['exposure_end_ms']
        cursor=start-1000
        for a,b in sorted(covers.get(symbol,[])):
            if a>cursor: break
            if b>=cursor: cursor=b+1
            if cursor>end+1000: break
        if cursor<=end+1000: return None
        total=0.
        for stamp,(rate,mark) in sorted(funds.get(symbol,{}).items()):
            if start-1000<=stamp<=end+1000:
                value=rate*mark/result['entry_price']
                total+=value if start+1000<stamp<end-1000 else min(0.,value)
        return -1. if result['liquidated_1x_proxy'] else max(-1.,result['net']+total)
    rows=json.loads((OUT/'records.json').read_text(encoding='utf-8'))
    for r in rows:
        source=prices[r['symbol']]; anchor=ms(r['anchor_at'])
        old=price_result(source,anchor,anchor+r['hours']*HOUR,FIVE)
        new=delayed_result(source,anchor,r['hours'],r['wait_hours'],r['clock'],r['execution'],FIVE)
        compare(old,r['immediate']); compare(new,r['delayed'])
        close(fund(old,r['symbol']),r['immediate_funded']); close(fund(new,r['symbol']),r['delayed_funded'])
        funding_checked+=2
    profiles=json.loads((OUT/'hourly_null_profiles.json').read_text(encoding='utf-8'))
    pseudo_count=0
    for profile in profiles:
        source=hourly[profile['symbol']]; anchor=ms(profile['actual_hourly_anchor'])
        candidates=[at for at in range(anchor-720*HOUR,anchor-192*HOUR+1,HOUR)
                    if all(source.get(t,(0,0,0,0,0,False))[5] for t in range(at,at+193*HOUR,HOUR))]
        assert candidates==[ms(t) for t in profile['candidate_starts']]
        assert len(candidates)==len(profile['paired_deltas'])
        for at,expected in zip(candidates,profile['paired_deltas']):
            assert at+192*HOUR<=anchor
            old=price_result(source,at,at+profile['hours']*HOUR,HOUR)
            new=delayed_result(source,at,profile['hours'],24,profile['clock'],'next_open',HOUR)
            close(new['net']-old['net'],expected); pseudo_count+=1
        if profile['control']:
            c=profile['control']; assert len(candidates)==c['null_n']
            close(mean(profile['paired_deltas']) if candidates else None,c['null_mean_delta'])
            old=price_result(source,anchor,anchor+profile['hours']*HOUR,HOUR)
            new=delayed_result(source,anchor,profile['hours'],24,profile['clock'],'next_open',HOUR)
            close(new['net']-old['net'],c['actual_delta'])
    cells=json.loads((OUT/'summary.json').read_text(encoding='utf-8'))['cells']
    for c in cells:
        selected=[r for r in rows if all(r[k]==c[k] for k in ('cohort','hours','wait_hours','clock','execution')) and (c['phase']=='all' or r['phase']==c['phase'])]
        paired=[r for r in selected if r['immediate']['status']=='filled' and r['delayed']['status'] in ('filled','unfilled')]
        filled=[r for r in paired if r['delayed']['status']=='filled']
        assert (len(selected),len(paired),len(filled))==(c['selected_n'],c['paired_n'],c['filled_n'])
        if paired:
            close(mean(r['immediate']['net'] for r in paired),c['immediate_mean'])
            close(mean(r['delayed']['net'] for r in paired),c['delayed_per_signal_mean'])
            close(mean(r['delayed']['net']-r['immediate']['net'] for r in paired),c['paired_policy_gain'])
            close(c['execution_timing_contribution']+c['selection_contribution'],c['paired_policy_gain'])
        if filled: close(mean(r['delayed']['net'] for r in filled),c['delayed_per_fill_mean'])
    assert all(sha(ROOT/p)==v for p,v in receipt['protected_before'].items())
    result={'verified_at_utc':datetime.now(timezone.utc).isoformat(),'method':'independent raw-page and direct-parquet loops; no analysis/production helpers',
            'signal_policy_rows_checked':len(rows),'funding_results_checked':funding_checked,'hourly_null_profiles_checked':len(profiles),
            'hourly_pseudo_start_policies_checked':pseudo_count,'summary_cells_checked':len(cells),'maximum_absolute_error':maximum_error,
            'protected_unchanged':True,'verifier_sha256':sha(Path(__file__))}
    (OUT/'verification.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(result,ensure_ascii=True))


if __name__=='__main__': run()
