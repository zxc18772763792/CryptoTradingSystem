"""Second evidence-driven repair: native minute witnesses and venue proofs.

No trading rules or paper ledgers are read for choosing a repair. Cross-market
replacement requires a unique clock witnessed by complete official OHLCV.
"""
from __future__ import annotations
import argparse,json,sys
from pathlib import Path
from collections import Counter,defaultdict
import pandas as pd
import numpy as np
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from core.data.parquet_timezone_repair import OHLCV,RepairConflict,StalePlan,sha256,plan_explicit,apply_plan
from scripts.repair_verified_partition_series import build_series_plans
from scripts.repair_verified_history_20261021 import load_reference
OLD=ROOT/'data/research/timezone_repair_20261020'
OUT=ROOT/'data/research/data_tracker_followup_20261022'

def normalize(frame):
    if set(frame.columns)!=set(OHLCV):
        raise RepairConflict('Unexpected columns require independent preservation review')
    out=frame[OHLCV].copy()
    if not isinstance(out.index,pd.DatetimeIndex) or out.index.hasnans or not out.index.is_unique:
        raise RepairConflict('Invalid or duplicate source/reference index')
    if out.index.tz is not None:out.index=out.index.tz_convert('UTC').tz_localize(None)
    out.index=out.index.as_unit('ns')
    if not np.isfinite(out.to_numpy(float)).all() or (out['volume']<0).any():
        raise RepairConflict('Invalid OHLCV values')
    return out

def reference(market,symbol,interval):
    path=OUT/'references'/market/symbol.replace('_','')/f'{interval}.parquet'
    meta_path=path.with_suffix('.meta.json');meta=json.loads(meta_path.read_text())
    if (meta['market']!=market or meta['interval']!=interval or meta['symbol']!=symbol.replace('_','')
        or not meta['complete_fetch'] or meta['parquet_sha256']!=sha256(path)):
        raise RepairConflict('Official reference provenance mismatch')
    return normalize(pd.read_parquet(path)),{'reference':str(path),'reference_sha256':meta['parquet_sha256'],
        'reference_metadata':str(meta_path),'market':market,'interval':interval}

def build_venue_plan(path,spot,perpetual,suspect_stamps,provenance):
    path=Path(path).resolve();pre=sha256(path);raw=pd.read_parquet(path)
    source=normalize(raw)
    if sha256(path)!=pre:raise StalePlan(str(path))
    spot,perpetual=normalize(spot),normalize(perpetual)
    suspects=set(pd.Timestamp(t) for t in suspect_stamps)
    positions=[i for i,t in enumerate(source.index) if t in suspects]
    matched={}
    if positions:
        own=source.iloc[positions].to_numpy(float);stamps=source.index[positions]
        for market,ref in [('spot',spot),('usds_perpetual',perpetual)]:
            for offset in (0,-8):
                target=stamps+pd.Timedelta(hours=offset)
                equal=np.all(own==ref.reindex(target).to_numpy(float),axis=1)
                for pos,key in zip(np.asarray(positions)[equal],target[equal]):
                    matched.setdefault(int(pos),[]).append({'market':market,'shift_hours':offset,'utc':key.isoformat()})
    targets=list(source.index);original_values=source.to_numpy(copy=True);values=original_values.copy();unknown=set();decisions={}
    for pos in positions:
        options=matched.get(pos,[]);keys={x['utc'] for x in options}
        decision={'position':pos,'stored':source.index[pos].isoformat(),'matches':options,'status':'unknown'}
        if len(keys)==1 and pd.Timestamp(next(iter(keys))) in spot.index:
            key=pd.Timestamp(next(iter(keys)));targets[pos]=key
            values[pos]=spot.loc[key].to_numpy()
            decision.update(status='official_venue_and_clock_verified',utc=key.isoformat(),
                source_market='spot' if any(x['market']=='spot' for x in options) else 'usds_perpetual',
                original_ohlcv=source.iloc[pos].tolist(),official_spot_ohlcv=spot.loc[key].tolist())
        else:unknown.add(pos)
        decisions[pos]=decision
    # Unknown rows stay exact; any shift colliding with them is deferred.
    while True:
        groups=defaultdict(list)
        for pos,key in enumerate(targets):groups[key].append(pos)
        defer={pos for group in groups.values() if len(group)>1 and any(i in unknown for i in group)
            for pos in group if pos not in unknown and targets[pos]!=source.index[pos]}
        if not defer:break
        for pos in defer:
            targets[pos]=source.index[pos];values[pos]=source.iloc[pos].to_numpy();unknown.add(pos)
            decisions[pos]['status']='proven_shift_deferred_unknown_collision'
    remove=[];coalesced=[]
    for key,group in groups.items():
        if len(group)<2:continue
        if any(pos in unknown for pos in group):raise RepairConflict('Unresolved canonical collision')
        if key not in spot.index:raise RepairConflict('No authoritative spot key for overlap')
        official=spot.loc[key].to_numpy()
        # Every protected original is also checked against the same-market ref.
        if any(not np.allclose(values[pos],official,rtol=1e-8,atol=1e-10) for pos in group):
            raise RepairConflict('Canonical conflict lacks full spot corroboration')
        for pos in group:values[pos]=official
        coalesced.append({'utc':key.isoformat(),'positions':group,'kind':'official_spot_canonical_overlap'})
        remove.extend(group[1:])
    operations=[]
    changed=np.flatnonzero((pd.DatetimeIndex(targets).asi8!=source.index.asi8)|np.any(values!=original_values,axis=1))
    for pos in changed.tolist():
        operations.append({'position':pos,'stored':source.index[pos].isoformat(),'utc':targets[pos].isoformat(),
            'old_ohlcv':original_values[pos].tolist(),'new_ohlcv':values[pos].tolist()})
    result=pd.DataFrame(values,index=pd.DatetimeIndex(targets,name='timestamp'),columns=OHLCV)
    keep=np.ones(len(source),dtype=bool);keep[remove]=False;result=result.iloc[keep].sort_index()
    if unknown:
        original=source.iloc[sorted(unknown)];after=result.loc[original.index]
        pd.testing.assert_frame_equal(original.reset_index(drop=True),after.reset_index(drop=True))
    evidence={'method':'exact_official_cross_venue_clock_then_authoritative_spot_resolution',
        'provenance':provenance,'decisions':list(decisions.values()),'operations':operations,
        'unknown_positions':sorted(unknown),'unknown_timestamps':[source.index[i].isoformat() for i in sorted(unknown)],
        'venue_corrected_rows':sum(d.get('source_market')=='usds_perpetual' and i not in unknown for i,d in decisions.items()),
        'shifted_rows':sum(x['utc']!=x['stored'] for x in operations),'rows_before':len(source),'rows_after':len(result)}
    plan=plan_explicit(path,result,evidence=evidence,expected_hash=pre,rows_before=len(source),
        coalesced=coalesced,utc_certified=not unknown)
    if raw.equals(plan['result']) and raw.attrs==plan['result'].attrs:
        plan['status']='already_utc' if not unknown else 'already_partial'
    return plan

def main():
    ap=argparse.ArgumentParser(description=__doc__);ap.add_argument('--phase',choices=['minutes','venue'],required=True)
    ap.add_argument('--apply',action='store_true');ap.add_argument('--run-root',type=Path,required=True)
    args=ap.parse_args();args.run_root.mkdir(parents=True,exist_ok=False)
    report={'apply':args.apply,'phase':args.phase,'results':[]}
    if args.phase=='minutes':
        symbols=sorted({x['symbol'] for x in json.loads((OLD/'source_audit.json').read_text())['mispartitioned_files']})
        jobs=[(symbol,None) for symbol in symbols]
    else:
        jobs=[(x['symbol'],x) for x in json.loads((OUT/'remaining_profile.json').read_text())['files']]
    for n,(symbol,entry) in enumerate(jobs):
        result={'symbol':symbol};run=args.run_root/f'{n:03d}_{symbol}'
        try:
            if args.phase=='minutes':
                hourly,hourly_meta=load_reference(symbol,'1h');native,native_meta=reference('spot',symbol,'5m')
                plans=build_series_plans(ROOT/'data/historical/binance'/symbol/'5m_parts',hourly,hourly_meta,
                    through='2026-05-19',native_reference=native,native_provenance=native_meta)
                result.update(plans[0]['evidence']['series_summary'])
            else:
                path=Path(entry['path']);result['interval']=entry['interval']
                if sha256(path)!=entry['current_sha256']:raise StalePlan('Source advanced after profile')
                old=next(x for x in json.loads((OLD/'remaining_ranges.json').read_text())['native'] if x['path']==str(path))
                evidence=json.loads(Path(old['evidence']).read_text());stamps=[evidence['rows'][i]['stored'] for i in evidence['unknown_positions']]
                spot,spotmeta=load_reference(symbol,entry['interval']);perp,perpmeta=reference('usds_perpetual',symbol,entry['interval'])
                plans=[build_venue_plan(path,spot,perp,stamps,{'spot':spotmeta,'usds_perpetual':perpmeta})]
                e=plans[0]['evidence'];result.update(venue_corrected_rows=e['venue_corrected_rows'],shifted_rows=e['shifted_rows'],
                    unknown_rows=len(e['unknown_positions']),rows_before=e['rows_before'],rows_after=e['rows_after'])
            result.update(status=plans[0]['status'],files=len(plans),utc_files=sum(p['utc_certified'] for p in plans))
            if args.apply and all(p['status']=='ready' for p in plans):
                apply_plan(plans,run);result.update(status='applied',run_dir=str(run))
        except Exception as exc:
            result.update(status='unresolved_unchanged',error_type=type(exc).__name__,error=str(exc))
            if (run/'manifest.json').exists():result.update(status=json.loads((run/'manifest.json').read_text())['status'],run_dir=str(run))
        report['results'].append(result);report['counts']=dict(Counter(r['status'] for r in report['results']))
        (args.run_root/'report.json').write_text(json.dumps(report,indent=2),encoding='utf-8');print(json.dumps(result),flush=True)
    return 0
if __name__=='__main__':raise SystemExit(main())
