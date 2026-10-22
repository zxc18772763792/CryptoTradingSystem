"""Bounded official references for unresolved clocks and venue contamination."""
from __future__ import annotations
import gzip,json,hashlib,sys
from pathlib import Path
from datetime import datetime,timezone
from concurrent.futures import ThreadPoolExecutor,as_completed
import pandas as pd
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts.audit_official_timezone_reference import session,Limiter,StopRequested,decode_rows
from core.data.parquet_timezone_repair import sha256
OLD=ROOT/'data/research/timezone_repair_20261020'
OUT=ROOT/'data/research/data_tracker_followup_20261022'

def jobs():
    result=[]
    for x in json.loads((OUT/'remaining_profile.json').read_text())['files']:
        result.append({'market':'usds_perpetual','symbol':x['symbol'].replace('_',''),'interval':x['interval'],
            'low':pd.Timestamp(x['min'],tz='UTC')-pd.Timedelta(hours=8),'high':pd.Timestamp(x['max'],tz='UTC')})
    stamps={}
    for p in OLD.glob('applied_partition_series_*/*/manifest.json'):
        for f in json.loads(p.read_text())['files']:
            e=json.loads((p.parent/f['evidence']).read_text());symbol=Path(f['path']).parent.parent.name.replace('_','')
            for h in e['source_hour_witnesses']:
                if h['status']=='incomplete_hour':
                    for pos in h['positions']:
                        stamps.setdefault(symbol,set()).add(pd.Timestamp(e['source_rows'][pos]['stored'],tz='UTC'))
    for symbol,values in stamps.items():
        result.append({'market':'spot','symbol':symbol,'interval':'5m','low':min(values)-pd.Timedelta(hours=8),'high':max(values)})
    return sorted(result,key=lambda x:(x['market']!='spot',x['symbol'],x['interval']))

def collect(job,limiter):
    folder=OUT/'references'/job['market']/job['symbol'];folder.mkdir(parents=True,exist_ok=True)
    target=folder/f"{job['interval']}.parquet";meta_path=target.with_suffix('.meta.json')
    low,high=(int(job[x].value//1000000) for x in ('low','high'))
    if meta_path.exists():
        meta=json.loads(meta_path.read_text())
        if meta.get('complete_fetch') and meta['parquet_sha256']==sha256(target) and meta['low']<=low and meta['high']>=high:
            return {'market':job['market'],'symbol':job['symbol'],'interval':job['interval'],'status':'cached','rows':meta['rows']}
    step=int(pd.Timedelta(job['interval']).total_seconds()*1000);cursor=low;all_rows=[];pages=[]
    host='https://api.binance.com/api/v3/klines' if job['market']=='spot' else 'https://fapi.binance.com/fapi/v1/klines'
    while cursor<=high:
        params={'symbol':job['symbol'],'interval':job['interval'],'startTime':cursor,'endTime':high,'limit':1000}
        if job['market']=='spot':params['timeZone']='0'
        limiter.wait();started=datetime.now(timezone.utc).isoformat()
        response=session().get(host,params=params,timeout=(8,20))
        if response.status_code in (403,418,429):
            limiter.halt(f'HTTP {response.status_code}');raise StopRequested(limiter.stop_reason)
        if response.status_code!=200:raise ValueError(f'HTTP {response.status_code}')
        raw=response.content;payload=json.loads(raw)
        if not isinstance(payload,list):raise ValueError('Not kline list')
        rawpath=folder/f"{job['interval']}_{cursor}_{high}.json.gz"
        with rawpath.open('xb') as stream:stream.write(gzip.compress(raw))
        pages.append({'url':host,'params':params,'requested_at':started,'received_at':datetime.now(timezone.utc).isoformat(),
            'raw_sha256':hashlib.sha256(raw).hexdigest(),'raw_path':str(rawpath),'rows':len(payload)})
        if not payload:break
        all_rows.extend(payload);next_cursor=int(payload[-1][0])+step
        if next_cursor<=cursor:raise ValueError('Nonadvancing page')
        cursor=next_cursor
        if len(payload)<1000:break
    now_ms=int(datetime.now(timezone.utc).timestamp()*1000)
    if any(int(x[6])>=now_ms for x in all_rows):raise ValueError('Reference includes forming bar')
    times=[x[0] for x in all_rows]
    if len(times)!=len(set(times)):raise ValueError('Duplicate official row')
    frame=decode_rows(all_rows);frame.attrs.update(cts_timestamp_timezone='UTC',cts_timestamp_schema=1,
        cts_timestamp_origin='official_rest',cts_verified_market=job['market'])
    frame.to_parquet(target)
    meta={**{k:v for k,v in job.items() if k not in ('low','high')},'low':low,'high':high,'timeZone':'0' if job['market']=='spot' else 'UTC_epoch_ms',
        'complete_fetch':True,'parquet_sha256':sha256(target),'rows':len(frame),'pages':pages}
    meta_path.write_text(json.dumps(meta,indent=2),encoding='utf-8')
    return {'market':job['market'],'symbol':job['symbol'],'interval':job['interval'],'status':'complete','rows':len(frame)}

def main():
    limiter=Limiter(2.0);report={'sources_unchanged':True,'jobs':[]}
    with ThreadPoolExecutor(max_workers=3) as pool:
        pending={pool.submit(collect,j,limiter):j for j in jobs()}
        for future in as_completed(pending):
            job=pending[future]
            try:row=future.result()
            except Exception as e:
                row={'market':job['market'],'symbol':job['symbol'],'interval':job['interval'],'status':'failed','error_type':type(e).__name__}
            report['jobs'].append(row);(OUT/'reference_collection.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
            print(json.dumps(row),flush=True)
    return 1 if any(x['status']=='failed' for x in report['jobs']) else 0
if __name__=='__main__':raise SystemExit(main())
