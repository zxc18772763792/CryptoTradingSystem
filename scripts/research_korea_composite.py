"""Offline Korean-caution combinations with explicit coverage and opportunity cost.

Never collects from accounts, fits parameters, updates a tracker, or trades.
Additional public receipts must already exist in the input directory.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import io
import json
import math
from pathlib import Path
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.upbit_short_backtest import score_short_trade

DAY = 86_400_000
HOUR = 3_600_000
FEE = 0.001
VARIANTS = ['baseline','first_designation','relative_weakness','btc_down_7d',
            'funding_nonnegative','relative_weakness_and_funding']


def read(path):
    return json.loads(path.read_bytes())


def result(record):
    response = record.get('response', {})
    if record.get('error') or response.get('isError'):
        return None
    obj = response.get('structuredContent')
    if obj is None:
        for block in response.get('content', []):
            if block.get('type') == 'text':
                try:
                    obj = json.loads(block['text'])
                    break
                except (ValueError, KeyError):
                    continue
    return obj.get('result', obj) if isinstance(obj, dict) else obj


def timestamp(value):
    t = pd.Timestamp(value)
    if t.tzinfo is None:
        raise ValueError('timezone required')
    return int(t.timestamp()*1000)


def pre_funding(rows, entry_ms):
    """A 24h window ending five minutes before entry, with cadence checks."""
    cutoff = entry_ms-300_000
    if rows is None:
        return None
    seen = {}
    for r in rows:
        t, rate = int(r['fundingTime']), float(r['fundingRate'])
        if cutoff-DAY <= t <= cutoff:
            if not math.isfinite(rate) or (t in seen and seen[t] != rate):
                return None
            seen[t] = rate
    times = sorted(seen)
    if len(times)<2:
        return None
    gaps = np.diff(times)
    step = int(round(float(np.median(gaps))/HOUR))*HOUR
    if not HOUR <= step <= 8*HOUR or any(abs(int(g)-step)>1000 for g in gaps):
        return None
    if times[0]-(cutoff-DAY)>step+1000 or cutoff-times[-1]>step+1000:
        return None
    return {'funding_pre24h':sum(seen.values()), 'funding_pre_n':len(times),
            'funding_pre_last_ms':times[-1], 'funding_pre_cutoff_ms':cutoff,
            'funding_interval_hours':step/HOUR}


def decisions(row):
    def compare(key, op):
        value = row.get(key)
        return None if value is None else bool(op(value))
    relative = compare('relative_day0',lambda x:x<0)
    fund = compare('funding_pre24h',lambda x:x>=0)
    combined = False if relative is False or fund is False else (True if relative is True and fund is True else None)
    return {'baseline':True, 'first_designation':not row['extension'],
            'relative_weakness':relative, 'btc_down_7d':compare('btc_pre7d',lambda x:x<=0),
            'funding_nonnegative':fund, 'relative_weakness_and_funding':combined}


def market_features(daily, symbol, day):
    out = {}
    coin, btc = daily.get(symbol,{}), daily.get('BTCUSDT',{})
    a,b = coin.get(str(day-DAY)),coin.get(str(day))
    x,y,z = btc.get(str(day-DAY)),btc.get(str(day)),btc.get(str(day-7*DAY))
    if a and b and x and y and min(a[0],b[0],x[0],y[0],a[1],b[1],x[1],y[1])>0:
        out.update(coin_day0=b[0]/a[0]-1, btc_day0=y[0]/x[0]-1,
                   relative_day0=b[0]/a[0]-y[0]/x[0])
    if y and z and min(y[0],z[0],y[1],z[1])>0:
        out['btc_pre7d'] = y[0]/z[0]-1
    return out


def month_ci(rows, values, seed=20261029):
    if len(rows)<2:
        return None
    frame = pd.DataFrame({'month':[r['date'][:7] for r in rows], 'value':values})
    groups = frame.groupby('month')['value'].agg(['sum','count']).to_numpy()
    if len(groups)<2:
        return None
    draw = np.random.default_rng(seed).integers(0,len(groups),size=(4000,len(groups)))
    samples = groups[draw,0].sum(axis=1)/groups[draw,1].sum(axis=1)
    return (np.quantile(samples,[.05,.95])*100).tolist()


def summary(rows, variant, return_field='short_return'):
    known_outcomes = [r for r in rows if r.get(return_field) is not None]
    selected = [r for r in known_outcomes if r['decisions'][variant] is True]
    rejected = [r for r in known_outcomes if r['decisions'][variant] is False]
    unknown = [r for r in known_outcomes if r['decisions'][variant] is None]
    val = [r[return_field] for r in selected]
    opportunity = [r[return_field] if r['decisions'][variant] is True else 0 for r in known_outcomes]
    n = len(known_outcomes)
    base = sum(r[return_field] for r in known_outcomes)
    captured = sum(val)
    episode = {}
    for r,v in zip(known_outcomes, opportunity):
        episode.setdefault(r['episode'],[]).append(v)
    return {'variant':variant, 'return_field':return_field, 'opportunities':n,
            'selected':len(selected),'rejected':len(rejected),'unknown_feature':len(unknown),
            'unsettled_or_missing_return':len(rows)-n,
            'mean_selected_pct':float(np.mean(val)*100) if val else None,
            'median_selected_pct':float(np.median(val)*100) if val else None,
            'worst_selected_pct':min(val)*100 if val else None,
            'selected_win_n':sum(v>0 for v in val),
            'selected_month_bootstrap90_pct':month_ci(selected,val),
            'selected_without_best_mean_pct':(sum(val)-max(val))/(len(val)-1)*100 if len(val)>1 else None,
            'per_opportunity_pct_unknown_no_entry':captured/n*100 if n else None,
            'per_opportunity_delta_vs_baseline_pp':(captured-base)/n*100 if n else None,
            'paired_opportunity_delta_month_bootstrap90_pp':month_ci(known_outcomes,[v-r[return_field] for r,v in zip(known_outcomes,opportunity)]),
            'per_opportunity_month_bootstrap90_pct':month_ci(known_outcomes,opportunity),
            'unknown_selection_bounds_per_opportunity_pct':[(captured+sum(min(r[return_field],0) for r in unknown))/n*100,
                                                           (captured+sum(max(r[return_field],0) for r in unknown))/n*100] if n else None,
            'rejected_profitable_n':sum(r[return_field]>0 for r in rejected),
            'rejected_profit_sum_pp':sum(max(r[return_field],0) for r in rejected)*100,
            'rejected_loss_sum_pp':sum(min(r[return_field],0) for r in rejected)*100,
            'episode_n':len(episode),
            'equal_episode_opportunity_mean_pct':float(np.mean([np.mean(v) for v in episode.values()])*100) if episode else None,
            'selected_1pct_each_side_stress_pct':float(np.mean([r['stress_return'] for r in selected if r.get('stress_return') is not None])*100)
                if return_field=='short_return' and any(r.get('stress_return') is not None for r in selected) else None}


def stamp_episodes(rows):
    latest, initial = {}, {}
    for r in sorted(rows,key=lambda x:x['entry_ms']):
        token, t = r['token'], r['entry_ms']
        if token not in latest or t-latest[token]>60*DAY:
            initial[token] = r['key']
        latest[token] = t
        r['episode'] = initial[token]


def news_exit(bars, funding, entry_ms, entry_price, received_ms, *, side_stress=0):
    """Scenario only: next 5m open after receipt; stop wins ambiguous overlap."""
    step = 300_000
    exit_ms = (received_ms//step+1)*step
    if received_ms < entry_ms:
        raise ValueError('message_before_entry_requires_an_entry_veto_study')
    values = {}
    for b in bars:
        t = int(b[0])
        if t in values and values[t] != b:
            raise ValueError('conflicting_5m_bars')
        values[t] = b
    required = range(entry_ms, exit_ms+1, step)
    if any(t not in values for t in required):
        raise ValueError('incomplete_5m_path')
    limit = entry_price*1.4
    first_stop = next((t for t in required if float(values[t][2])>=limit),None)
    if first_stop is not None:
        return {'eligible_news_exit':False,'reason':'stop_before_or_during_candidate_fill_bar',
                'first_stop_5m_ms':first_stop}
    price = float(values[exit_ms][1])
    if price<=0 or float(values[exit_ms][5])<=0:
        raise ValueError('no_tradable_exit_bar')
    rates = [f for f in funding if entry_ms<int(f['fundingTime'])<=exit_ms]
    times = sorted(int(f['fundingTime']) for f in rates)
    if (not rates or len(set(times))!=len(times) or times[0]-entry_ms>8*HOUR+1000
            or exit_ms-times[-1]>8*HOUR+1000
            or any(b-a>8*HOUR+1000 for a,b in zip(times,times[1:]))):
        raise ValueError('missing_holding_funding')
    paid = sum(float(f['fundingRate']) for f in rates)
    ret = 1-price*(1+side_stress)/(entry_price*(1-side_stress))-FEE+paid
    return {'eligible_news_exit':True,'received_ms':received_ms,'candidate_exit_ms':exit_ms,
            'candidate_exit_open':price,'funding':paid,'net_short_leg_pct':ret*100,
            'side_stress_pct':side_stress*100,'execution':'next 5m open proxy; not an observed actual fill'}


def run(folder, output):
    output.mkdir(parents=True, exist_ok=False)
    up = ROOT/'data/research/upbit'
    paths = [up/n for n in ['events.csv','notices.json','reasons.json','short_backtest.csv',
                            'perp_cache.json','perp_daily_cache.json','caution_hedged_backtest.csv']]
    blobs = {str(p):p.read_bytes() for p in paths}
    for p in [folder/'protocol.json',folder/'bridge_scope_note.json',folder/'funding_batch_0_timing_correction.json']:
        blobs[str(p)] = p.read_bytes()
    cache = json.loads(blobs[str(up/'perp_cache.json')])
    daily = json.loads(blobs[str(up/'perp_daily_cache.json')])
    events = pd.read_csv(io.BytesIO(blobs[str(up/'events.csv')]))
    baseline = pd.read_csv(io.BytesIO(blobs[str(up/'short_backtest.csv')]))
    hedge = pd.read_csv(io.BytesIO(blobs[str(up/'caution_hedged_backtest.csv')]))
    hedge_map = {(r.token,str(r.date)[:10]):r for r in hedge.itertuples()}
    notice_map = {r['title']:r for r in json.loads(blobs[str(up/'notices.json')])}
    reasons = json.loads(blobs[str(up/'reasons.json')])
    receipts = sorted(folder.glob('funding_batch_[0-9].json'))
    funding = {}
    for p in receipts:
        blobs[str(p)] = p.read_bytes()
        for r in json.loads(blobs[str(p)]):
            funding[r['event']['key']] = result(r)
    for p in [folder/'market_bars.json',folder/'market_bars_transport_retry.json',folder/'bridge_market_bars.json']:
        blobs[str(p)] = p.read_bytes()
        for r in json.loads(blobs[str(p)]):
            bars = result(r)
            if r['key'].startswith('daily_') and bars is not None:
                symbol = r['args']['symbol']
                target = daily.setdefault(symbol,{})
                for b in bars:
                    target[str(int(b[0]))] = [float(b[4]),float(b[7])]
    rows, excluded, quality = [], [], []
    for e in events[events.kind=='caution'].itertuples():
        day = timestamp(e.date)
        original = baseline[(baseline.kind=='caution') & (baseline.token==e.token) & (baseline.date==e.date)]
        source = original.iloc[0] if len(original)==1 else None
        if source is None or not isinstance(source.symbol,str):
            excluded.append({'token':e.token,'date':e.date,'reason':'no_archived_perp_mapping'})
            continue
        key = e.token+'|'+e.date[:10]
        item = cache.get('v2|'+source.symbol+'|'+e.date[:10],{})
        bars, fr = item.get('klines',[]), item.get('funding',[])
        try:
            if not bars or len(bars[0])<8 or float(bars[0][7])<=0:
                raise ValueError('zero_or_missing_entry_day_volume')
            ret, paid, stopped = score_short_trade(bars,fr,day)
        except ValueError as exc:
            excluded.append({'token':e.token,'date':e.date,'reason':str(exc)})
            continue
        entry = day+DAY
        endbar = next((i for i,b in enumerate(bars[1:8],1) if float(b[2])>=float(bars[0][4])*1.4),7)
        exit_ms = day+(endbar+1)*DAY
        exit_px = float(bars[0][4])*1.4*1.02 if stopped else float(bars[7][4])
        stress = 1-(exit_px*1.01)/(float(bars[0][4])*.99)-FEE+paid
        notice = notice_map.get(e.title,{})
        row = {'key':key,'token':e.token,'symbol':source.symbol,'date':e.date,'entry_ms':entry,
               'cohort':'historical','extension':'연장' in e.title,
               'reason_label':reasons.get(str(notice.get('id')),{}).get('reason','unknown'),
               'reason_label_timing':'retrospective model output; not a frozen historical feature',
               'short_return':ret,'funding_during_hold':paid,'stopped':stopped,
               'stress_return':stress,'hedged_return':None,
               'exit_ms':exit_ms,**market_features(daily,source.symbol,day),**(pre_funding(funding.get(key),entry) or {})}
        archived_close = daily.get(source.symbol,{}).get(str(day))
        if archived_close and abs(archived_close[0]/float(bars[0][4])-1)>1e-8:
            quality.append({'key':key,'issue':'notice_day_close_cache_mismatch'})
            row.pop('relative_day0',None)
        h = hedge_map.get((e.token,e.date[:10]))
        if h is not None:
            old_check = float(h.coin_short_ret)+float(h.basket_ret)-float(h.basket_funding)-FEE
            if abs(old_check-float(h.hedged_ret))>1e-8 or bool(h.stopped)!=stopped:
                quality.append({'key':key,'issue':'unreconciled_hedge_formula_or_exit_window'})
            else:
                row['hedged_return'] = ret+float(h.basket_ret)-float(h.basket_funding)-FEE
                if abs(float(h.coin_short_ret)-ret)>=1e-8:
                    quality.append({'key':key,'issue':'archived_hedge_coin_funding_boundary_reconciled_in_new_output',
                                    'old_minus_recomputed_pp':(float(h.coin_short_ret)-ret)*100})
        row['decisions'] = decisions(row)
        rows.append(row)
    current_path = ROOT/'data/research/korea_caution_hedged/tracker.json'
    current_bytes = current_path.read_bytes()
    blobs[str(current_path)] = current_bytes
    state = json.loads(current_bytes)
    with (output/'current_pooled_tracker_snapshot.json').open('xb') as stream:
        stream.write(current_bytes)
    current_records = dict(state['trades'])
    upbit_current_path = ROOT/'data/research/upbit_caution/tracker.json'
    upbit_current_bytes = upbit_current_path.read_bytes()
    blobs[str(upbit_current_path)] = upbit_current_bytes
    with (output/'current_upbit_tracker_snapshot.json').open('xb') as stream:
        stream.write(upbit_current_bytes)
    for key,t in json.loads(upbit_current_bytes)['trades'].items():
        if t.get('backfilled') and t.get('symbol') and key not in current_records:
            current_records[key] = t
    seen_opportunities = set()
    for key,t in current_records.items():
        if not t.get('symbol') or t.get('late'):
            continue
        entry = int(t['entry_at'])
        identity = (t['symbol'],entry)
        if identity in seen_opportunities:
            quality.append({'key':key,'issue':'duplicate_tracker_opportunity_excluded'})
            continue
        seen_opportunities.add(identity)
        closed = t['status'] in ['closed','stopped']
        row = {'key':key,'token':t['ticker'],'symbol':t['symbol'],
               'date':datetime.fromtimestamp((entry-DAY)/1000,timezone.utc).isoformat(),
               'entry_ms':entry,'cohort':'backfill_bridge' if t.get('backfilled') else 'forward_observed',
               'source_exchange':t.get('source','upbit'),'extension':'연장' in t['title'],
               'original_discovered_at':t['discovered_at'],'status':t['status'],
               'short_return':float(t['return_pct'])/100 if closed else None,
               'hedged_return':float(t['hedged_return_pct'])/100 if closed and t.get('hedged_return_pct') is not None else None,
               'stress_return':None,
               **market_features(daily,t['symbol'],entry-DAY),**(pre_funding(funding.get(key),entry) or {})}
        row['decisions'] = decisions(row)
        rows.append(row)
    stamp_episodes(rows)
    oi_rows = read(folder/'recent_oi.json')
    top_rows = read(folder/'recent_top_positions.json')
    blobs[str(folder/'recent_oi.json')] = (folder/'recent_oi.json').read_bytes()
    blobs[str(folder/'recent_top_positions.json')] = (folder/'recent_top_positions.json').read_bytes()
    for r in rows:
        if r['cohort']!='forward_observed':
            continue
        for kind, records, field in [('oi',oi_rows,'sumOpenInterest'),('top_positions',top_rows,'longAccount')]:
            source = next((x for x in records if x['event']['key']==r['key']),{})
            values = source.get('body') if kind=='top_positions' else result(source)
            if not isinstance(values,list):
                continue
            available = {int(v['timestamp']):v for v in values if int(v['timestamp'])<=r['entry_ms']-300000}
            if not available:
                continue
            t = max(available)
            if r['entry_ms']-t>90*60_000:
                continue
            r[kind+'_last_ms'] = t
            r[kind+'_last'] = float(available[t][field])
            before = available.get(t-6*HOUR)
            if before:
                r[kind+'_pre6h_change'] = float(available[t][field])/float(before[field])-1 if kind=='oi' else float(available[t][field])-float(before[field])
    reports = {}
    for cohort in ['historical','backfill_bridge','forward_observed']:
        part = [r for r in rows if r['cohort']==cohort]
        reports[cohort] = {'events':len(part),'unique_tokens':len({r['token'] for r in part}),
                           'settled':sum(r['short_return'] is not None for r in part),
                           'short':[summary(part,v) for v in VARIANTS],
                           'hedged':[summary(part,v,'hedged_return') for v in VARIANTS]}
        if cohort=='historical':
            reports[cohort]['period_diagnostics'] = {name:[summary(sub,v) for v in VARIANTS]
                for name,sub in [('through_2024',[r for r in part if r['date'][:4]<='2024']),
                                 ('2025_2026',[r for r in part if r['date'][:4]>='2025'])]}
            reports[cohort]['reason_descriptive'] = {name:summary([r for r in part if r['reason_label']==name],'baseline')
                for name in sorted({r['reason_label'] for r in part})}
    reports['all_available_sensitivity'] = {
        'warning':'exploratory union of legacy history, backfill, and original forward observations; not a pristine OOS test',
        'short':[summary(rows,v) for v in VARIANTS],
        'hedged':[summary(rows,v,'hedged_return') for v in VARIANTS]}
    # Preserve separate message and reception clocks; this case was already
    # known during design and cannot establish a general news-exit advantage.
    five = {}
    for p in [folder/'market_bars.json',folder/'market_bars_transport_retry.json']:
        for r in read(p):
            if r['key']=='INIT_5m' and result(r) is not None:
                for b in result(r):
                    five[int(b[0])] = b
    p = folder/'init_early_5m.json'
    blobs[str(p)] = p.read_bytes()
    for b in result(read(p)) or []:
        five[int(b[0])] = b
    p = ROOT/'data/research/init_onchain_guard_20261029/funding.json'
    blobs[str(p)] = p.read_bytes()
    full_funding = read(p)
    full_funding = full_funding.get('result',full_funding)
    private_news_path = Path.home()/'.crypto_trading_system/research/init_news_raw_20261029.json'
    news = read(private_news_path)
    blobs[str(private_news_path)] = private_news_path.read_bytes()
    cases = []
    for n in news:
        if n['id'] not in [326146,328514]:
            continue
        # Storage explicitly declares these naive DB timestamps as UTC.
        received = timestamp(n['fetched_at'].replace(' ','T')+'+00:00')
        base_case = {'news_id':n['id'],'published_at_utc':n['published_at'],
                     'first_received_at_utc':n['fetched_at'],'url':n['url'],'title':n['title'],
                     'development_only':True,'hedged_result':None,
                     'baseline_short_leg_net_pct':-43.077}
        for stress in [0,.01]:
            cases.append({**base_case,**news_exit(list(five.values()),full_funding,1793059200000,.15664,received,side_stress=stress)})
    report = {'generated_at_utc':datetime.now(timezone.utc).isoformat(),'protocol':read(folder/'protocol.json'),
              'implementation_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              'original_caution_events':int(sum(events.kind=='caution')),'excluded':excluded,'quality':quality,
              'comparisons':reports,'events':rows,'init_news_exit_scenarios':cases,
              'limits':['retrospective exploration, not new OOS','existing cache/mapping survivorship remains',
                        'current derivative ratios cannot backfill old years','old reasons were classified retrospectively',
                        'partial forward events excluded from settled metrics','per-opportunity unknown-no-entry is a policy scenario, not zero-filled input data',
                        'calendar-month bootstrap intervals are descriptive and not multiple-test-adjusted',
                        '1% execution stress holds original exit window fixed; does not model changed stop timing'],
              'manifest':{p:hashlib.sha256(b).hexdigest() for p,b in blobs.items()}}
    for name,obj in [('analysis.json',report),('protocol_copy.json',read(folder/'protocol.json'))]:
        with (output/name).open('x',encoding='utf-8') as stream:
            json.dump(obj,stream,ensure_ascii=False,indent=2,allow_nan=False)
    pd.DataFrame([{k:v for k,v in r.items() if k!='decisions'}|r['decisions'] for r in rows]).to_csv(output/'event_matrix.csv',index=False)
    print(json.dumps({'original_events':report['original_caution_events'],'excluded':excluded,'quality':quality,
                      'comparisons':reports},ensure_ascii=False,indent=2))


if __name__=='__main__':
    if hasattr(sys.stdout,'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir',type=Path,default=ROOT/'data/research/korea_composite_20261029')
    parser.add_argument('--output-dir',type=Path,required=True)
    args = parser.parse_args()
    run(args.input_dir,args.output_dir)
