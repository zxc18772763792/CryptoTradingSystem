"""Fixed delayed controls for KRW-listing attention decay; offline research only."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path

from scripts.research_korea_composite import DAY, FEE, result, stamp_episodes, summary, timestamp
from scripts.upbit_short_backtest import score_short_trade

ROOT = Path(__file__).resolve().parents[1]
VARIANTS = ['delayed_control','day1_weak','day1_weak_and_cooling']


def delayed_short(bars,funding,day):
    if len(bars)<8 or any(int(bars[i][0])!=day+i*DAY for i in range(8)):
        raise ValueError('incomplete_daily_path')
    if float(bars[1][7])<=0:
        raise ValueError('no_delayed_entry_volume')
    entry = float(bars[1][4])
    if not math.isfinite(entry) or entry<=0:
        raise ValueError('invalid_entry')
    exit_price,exit_ms,stopped = float(bars[7][4]),day+8*DAY,False
    for i in range(2,8):
        if float(bars[i][2])>=entry*1.4:
            exit_price,exit_ms,stopped = entry*1.4*1.02,day+(i+1)*DAY,True
            break
    entry_ms = day+2*DAY
    rates = [r for r in funding if entry_ms<int(r['fundingTime'])<=exit_ms]
    times = sorted(int(r['fundingTime']) for r in rates)
    if (not rates or len(set(times))!=len(times) or times[0]-entry_ms>DAY
        or exit_ms-times[-1]>DAY or any(b-a>DAY for a,b in zip(times,times[1:]))):
        raise ValueError('incomplete_delayed_funding')
    paid = sum(float(r['fundingRate']) for r in rates)
    return {'short_return':1-exit_price/entry-FEE+paid,
            'stress_return':1-exit_price*1.01/(entry*.99)-FEE+paid,
            'funding_during_hold':paid,'entry_price':entry,'entry_ms':entry_ms,
            'exit_ms':exit_ms,'stopped':stopped,'holding_days_model':(exit_ms-entry_ms)/DAY}


def confirmation(bars,btc,day):
    if len(bars)<2 or int(bars[0][0])!=day or int(bars[1][0])!=day+DAY:
        return {'delayed_control':True,'day1_weak':None,'day1_weak_and_cooling':None}
    a,b = float(bars[0][4]),float(bars[1][4])
    x,y = btc.get(str(day)),btc.get(str(day+DAY))
    if not x or not y or min(a,b,x[0],y[0])<=0:
        return {'delayed_control':True,'day1_weak':None,'day1_weak_and_cooling':None}
    coin,bm = b/a-1,y[0]/x[0]-1
    weak = coin<0 and coin<bm
    cooling = float(bars[1][7])<float(bars[0][7]) if min(float(bars[1][7]),float(bars[0][7]))>0 else None
    return {'delayed_control':True,'day1_weak':weak,
            'day1_weak_and_cooling':False if not weak else cooling,
            '_features':{'day1_coin_return':coin,'day1_btc_return':bm,
                         'day1_turnover_ratio':float(bars[1][7])/float(bars[0][7])}}


def write_new(path,value):
    with path.open('x',encoding='utf-8') as stream:
        json.dump(value,stream,indent=2,ensure_ascii=False,allow_nan=False)


def run(folder,output):
    output.mkdir(parents=True,exist_ok=False)
    files = [folder/'run_02/analysis.json',ROOT/'data/research/upbit/perp_cache.json',
             ROOT/'data/research/upbit/perp_daily_cache.json',folder/'recent_listing_bars.json',
             ROOT/'data/research/korea_composite_20261029/market_bars.json',
             folder/'recent_listing_holding_funding.json',folder/'phase2_protocol.json']
    blobs = {str(p):p.read_bytes() for p in files}
    first = json.loads(blobs[str(files[0])]);cache=json.loads(blobs[str(files[1])]);daily=json.loads(blobs[str(files[2])])
    full_bars = {}
    for p in files[3:5]:
        for rec in json.loads(blobs[str(p)]):
            if rec.get('args',{}).get('interval')!='1d':continue
            data=result(rec)
            if not isinstance(data,list):continue
            symbol=rec['args']['symbol'];dest=daily.setdefault(symbol,{})
            for b in data:dest[str(int(b[0]))]=[float(b[4]),float(b[7])]
            full_bars[symbol]={int(b[0]):b for b in data}
    full_funding = {r['event']['key']:result(r) for r in json.loads(blobs[str(files[5])])}
    events,failures,original_checks=[],[],[]
    for original in first['events']:
        if original['kind']!='listing_krw':continue
        row=dict(original);day=timestamp(row['date'])
        row['original_short_return']=row['short_return'];row['short_return']=row['stress_return']=None
        if row['cohort']=='legacy':
            obj=cache.get('v2|'+row['symbol']+'|'+row['date'][:10],{})
            bars,fr=obj.get('klines',[]),obj.get('funding',[])
        else:
            mapping=full_bars.get(row['symbol'],{})
            bars=[mapping[day+i*DAY] for i in range(8) if day+i*DAY in mapping]
            fr=full_funding.get(row['key'])
        if len(bars)>=2:
            confirm=confirmation(bars,daily.get('BTCUSDT',{}),day)
            row.update(confirm.pop('_features',{}));row['decisions']=confirm
        else:
            row['decisions']={'delayed_control':True,'day1_weak':None,'day1_weak_and_cooling':None}
        if original['short_return'] is not None:
            try:
                if fr is None:raise ValueError('missing_funding_receipt')
                old,_,_=score_short_trade(bars,fr,day)
                delta=old-original['short_return']
                if abs(delta)>0.0000051:raise ValueError('original_outcome_failed_reconciliation')
                original_checks.append({'key':row['key'],'recomputed_original':old,'original':original['short_return'],'difference':delta})
                row.update(delayed_short(bars,fr,day))
            except ValueError as exc:
                failures.append({'key':row['key'],'reason':str(exc)})
        events.append(row)
    stamp_episodes(events)
    comparisons={}
    for name,part in [('legacy',[r for r in events if r['cohort']=='legacy']),
                      ('original_forward_replayed',[r for r in events if r['cohort']!='legacy']),
                      ('all_available_sensitivity',events)]:
        comparisons[name]=[summary(part,v) for v in VARIANTS]
    periods={name:[summary(part,v) for v in VARIANTS] for name,part in [
        ('2023_2024',[r for r in events if r['cohort']=='legacy' and r['date'][:4]<='2024']),
        ('2025_2026',[r for r in events if r['cohort']=='legacy' and r['date'][:4]>='2025'])]}
    report={'generated_at_utc':datetime.now(timezone.utc).isoformat(),'protocol':json.loads(blobs[str(files[6])]),
            'implementation_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'comparisons':comparisons,'periods':periods,'events':events,'failures':failures,
            'original_outcome_reconciliations':original_checks,'manifest':{p:hashlib.sha256(b).hexdigest() for p,b in blobs.items()},
            'interpretation':['six-day delayed control compared fairly; original seven-day is a separate policy',
                              'every new result is retrospective exploration, not forward validation',
                              'a missing/ongoing event is not a zero-return mature sample',
                              'same-token and calendar dependence and current-universe biases persist']}
    write_new(output/'analysis.json',report)
    print(json.dumps({'comparisons':comparisons,'periods':periods,'failures':failures},indent=2,ensure_ascii=False))


if __name__=='__main__':
    import sys
    sys.stdout.reconfigure(encoding='utf-8')
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir',type=Path,default=ROOT/'data/research/korea_factor_selection_20261029')
    parser.add_argument('--output-dir',type=Path,required=True)
    args=parser.parse_args();run(args.input_dir,args.output_dir)
