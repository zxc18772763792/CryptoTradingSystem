"""Consolidate Korean event families and evaluate fixed attention factors offline.

Research artifacts only. No network, orders, scheduler or old-book writes.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import statistics

import pandas as pd

from scripts.research_korea_composite import DAY, FEE, market_features, result, stamp_episodes, summary, timestamp
from scripts.upbit_short_backtest import score_short_trade

ROOT = Path(__file__).resolve().parents[1]
LISTING_VARIANTS = ['baseline','relative_shock_positive','volume_2x','relative_and_volume']


def volume_multiple(daily, symbol, day, current_quote_volume=None):
    history = daily.get(symbol,{})
    previous = [history.get(str(day-i*DAY)) for i in range(1,21)]
    if any(r is None or len(r)<2 or r[1]<=0 for r in previous):
        return None
    current = history.get(str(day))
    value = current_quote_volume if current_quote_volume is not None else (current[1] if current else None)
    if value is None or value<=0:
        return None
    return float(value)/statistics.median(r[1] for r in previous)


def listing_decisions(row):
    relative = None if row.get('relative_day0') is None else row['relative_day0']>0
    volume = None if row.get('volume_20d_multiple') is None else row['volume_20d_multiple']>=2
    together = False if relative is False or volume is False else (True if relative is True and volume is True else None)
    return {'baseline':True,'relative_shock_positive':relative,'volume_2x':volume,'relative_and_volume':together}


def clean_return(bars, funding, day):
    if not bars or len(bars[0])<8 or float(bars[0][7])<=0:
        raise ValueError('zero_or_missing_notice_day_volume')
    net, cost, stopped = score_short_trade(bars,funding,day)
    entry = float(bars[0][4])
    exit_price = entry*1.4*1.02 if stopped else float(bars[7][4])
    stress = 1-exit_price*1.01/(entry*.99)-FEE+cost
    return net,cost,stopped,stress


def write_new(path, obj):
    with path.open('x',encoding='utf-8') as f:
        json.dump(obj,f,ensure_ascii=False,indent=2,allow_nan=False)


def run(folder, output):
    output.mkdir(parents=True,exist_ok=False)
    up = ROOT/'data/research/upbit'
    files = [up/'short_backtest.csv',up/'perp_cache.json',up/'perp_daily_cache.json',
             folder/'protocol.json',folder/'recent_listing_bars.json',
             ROOT/'data/research/korea_composite_20261029/market_bars.json',
             ROOT/'data/research/korea_composite_20261029/run_04/analysis.json']
    raw = {str(p):p.read_bytes() for p in files}
    baseline = pd.read_csv(io.BytesIO(raw[str(up/'short_backtest.csv')]))
    cache = json.loads(raw[str(up/'perp_cache.json')])
    daily = json.loads(raw[str(up/'perp_daily_cache.json')])
    for p in files[4:6]:
        for rec in json.loads(raw[str(p)]):
            if rec.get('args',{}).get('interval')!='1d':continue
            bars = result(rec)
            if not isinstance(bars,list):
                continue
            symbol = rec['args']['symbol']
            target = daily.setdefault(symbol,{})
            for b in bars:
                target[str(int(b[0]))] = [float(b[4]),float(b[7])]
    rows,excluded = [],[]
    for e in baseline.itertuples():
        if e.kind not in ['caution','warning_urged','listing_krw']:
            continue
        if not isinstance(e.symbol,str):
            excluded.append({'kind':e.kind,'token':e.token,'date':e.date,'reason':'no_archived_perp_mapping'})
            continue
        day = timestamp(e.date)
        data = cache.get('v2|'+e.symbol+'|'+e.date[:10],{})
        bars, funding = data.get('klines',[]),data.get('funding',[])
        try:
            net,cost,stopped,stress = clean_return(bars,funding,day)
        except ValueError as exc:
            excluded.append({'kind':e.kind,'token':e.token,'date':e.date,'reason':str(exc)})
            continue
        features = market_features(daily,e.symbol,day)
        entry_close = daily.get(e.symbol,{}).get(str(day))
        quality = []
        if entry_close and abs(entry_close[0]/float(bars[0][4])-1)>1e-8:
            features.pop('relative_day0',None)
            quality.append('price_cache_mismatch_relative_unknown')
        r = {'key':e.kind+'|'+e.token+'|'+e.date[:10],'token':e.token,'symbol':e.symbol,
             'kind':e.kind,'cohort':'legacy','date':e.date,'entry_ms':day+DAY,'short_return':net,
             'funding_during_hold':cost,'stopped':stopped,'stress_return':stress,
             'volume_20d_multiple':volume_multiple(daily,e.symbol,day,float(bars[0][7])),
             'quality':quality,**features}
        r['decisions'] = listing_decisions(r)
        rows.append(r)
    current_path = ROOT/'data/research/upbit_krw_listing/tracker.json'
    current = current_path.read_bytes()
    raw[str(current_path)] = current
    with (output/'current_krw_tracker_snapshot.json').open('xb') as f:
        f.write(current)
    current_excluded = []
    for key,t in json.loads(current)['trades'].items():
        if t.get('backfilled'):
            continue
        if not t.get('symbol') or t.get('late'):
            current_excluded.append({'key':key,'reason':t.get('status'),'late':t.get('late')})
            continue
        entry = int(t['entry_at'])
        r = {'key':key,'token':t['ticker'],'symbol':t['symbol'],'kind':'listing_krw',
             'cohort':'original_forward_replayed','date':datetime.fromtimestamp((entry-DAY)/1000,timezone.utc).isoformat(),
             'entry_ms':entry,'status':t['status'],'short_return':float(t['return_pct'])/100 if t['status'] in ['closed','stopped'] else None,
             'stress_return':None,'original_discovered_at':t['discovered_at'],
             'volume_20d_multiple':volume_multiple(daily,t['symbol'],entry-DAY),
             **market_features(daily,t['symbol'],entry-DAY)}
        r['decisions'] = listing_decisions(r)
        rows.append(r)
    # No independence claim across event families. Cluster within each family.
    for family in ['caution','warning_urged','listing_krw']:
        stamp_episodes([r for r in rows if r['kind']==family])
    families = {family:summary([r for r in rows if r['kind']==family and r['cohort']=='legacy'],'baseline')
                for family in ['caution','warning_urged','listing_krw']}
    hist = [r for r in rows if r['kind']=='listing_krw' and r['cohort']=='legacy']
    forward = [r for r in rows if r['cohort']=='original_forward_replayed']
    listing = {
        'historical':[summary(hist,v) for v in LISTING_VARIANTS],
        'original_forward_replayed':[summary(forward,v) for v in LISTING_VARIANTS],
        'all_available_sensitivity':[summary(hist+forward,v) for v in LISTING_VARIANTS],
        'periods':{name:[summary(part,v) for v in LISTING_VARIANTS] for name,part in [
            ('2023_2024',[r for r in hist if r['date'][:4]<='2024']),
            ('2025_2026',[r for r in hist if r['date'][:4]>='2025'])]},
    }
    caution = json.loads(raw[str(files[6])])
    output_data = {'generated_at_utc':datetime.now(timezone.utc).isoformat(),
                   'implementation_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                   'protocol':json.loads(raw[str(folder/'protocol.json')]),'family_baselines':families,
                   'listing_comparisons':listing,'events':rows,'excluded_legacy':excluded,
                   'excluded_current':current_excluded,
                   'caution_reference':{'source':str(files[6]),'sha256':hashlib.sha256(raw[str(files[6])]).hexdigest(),
                                        'comparison':caution['comparisons']['all_available_sensitivity']},
                   'limits':['all new comparisons retrospective, not pristine OOS',
                             'fixed cutoff at daily close is idealized; equal delayed controls needed',
                             'volumes are Binance perp quote turnover, not observed Korea capital inflow',
                             'legacy current-contract mapping has survivorship limitations',
                             'no simulated portfolio or account return; overlapping event opportunities',
                             'soft-warning family is a comparator, not a guaranteed null',
                             'fixed-window execution stress does not re-evaluate altered stop timing'],
                   'manifest':{p:hashlib.sha256(b).hexdigest() for p,b in raw.items()}}
    write_new(output/'analysis.json',output_data)
    pd.DataFrame([{k:v for k,v in r.items() if k not in ['decisions','quality']}|r['decisions'] for r in rows]).to_csv(output/'event_matrix.csv',index=False)
    print(json.dumps({'family_baselines':families,'listing_comparisons':listing,'excluded_legacy':excluded,
                      'excluded_current':current_excluded},ensure_ascii=False,indent=2))


if __name__=='__main__':
    import sys
    sys.stdout.reconfigure(encoding='utf-8')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir',type=Path,default=ROOT/'data/research/korea_factor_selection_20261029')
    parser.add_argument('--output-dir',type=Path,required=True)
    args = parser.parse_args()
    run(args.input_dir,args.output_dir)
