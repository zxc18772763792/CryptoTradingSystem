"""Bounded read-only runtime health; no app imports, account APIs or collectors."""
from collections import Counter
from datetime import datetime,timedelta,timezone
import hashlib
import json
from pathlib import Path
import statistics
import requests

OUT=Path(__file__).resolve().parent
ROOT=OUT.parents[1]
now=datetime.now(timezone.utc)
def utc(s):return datetime.fromisoformat(s.replace('Z','+00:00')).astimezone(timezone.utc)
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
result={'checked_at_utc':now.isoformat(),'read_only':True,'private_account_endpoints_called':False,'order_endpoints_called':False}
session=requests.Session();session.trust_env=False
checks={}
for endpoint in ('/livez','/readyz','/','/api/status'):
    try:
        response=session.get('http://127.0.0.1:8000'+endpoint,timeout=5)
        item={'http_status':response.status_code}
        if endpoint in ('/livez','/readyz','/api/status') and response.status_code in (200,503):
            payload=response.json();item['status']=payload.get('status')
            if endpoint=='/readyz':
                c=payload.get('checks',{});item['database']=c.get('db');item['strategy_manager']=c.get('strategy_manager')
                item['runtime']=c.get('runtime')
            elif endpoint=='/api/status':
                item['trading_mode']=payload.get('trading_mode');item['paper_trading']=payload.get('paper_trading')
                item['task_count']=payload.get('runtime',{}).get('task_count')
        checks[endpoint]=item
    except Exception as exc:checks[endpoint]={'error_type':type(exc).__name__}
session.close();result['http']=checks
sources={'core':'core/research/alastar_forward_paper.py','provider_cli':'scripts/observe_alastar_paper.py','tracker':'core/research/alastar_paper_tracker.py',
         'public_provider':'scripts/observe_alastar_paper.py','v2_core':'core/research/alastar_forward_paper.py','v3':'core/research/alastar_horizon_tracker.py','v3_cli':'scripts/observe_alastar_horizons.py'}
books={}
for name,folder,runtime_name,protocol in (
    ('v2','alastar_paper_v2','tracker_runtime.json','alastar_response_20261021/prospective_paper_protocol_v2.json'),
    ('v3','alastar_horizons_v3','runtime.json','alastar_horizons_20261022/prospective_horizon_protocol_v3.json')):
    base=ROOT/'data/research'/folder
    try:
        runtime=json.loads((base/runtime_name).read_bytes());rows=[];previous=None;valid=True;last=None
        for i,path in enumerate(sorted((base/'journal').glob('*.json')),1):
            row=json.loads(path.read_bytes());at=utc(row['recorded_at_utc'])
            valid &= path.name==f'{i:012d}.json' and row['sequence']==i and row['previous_sha256']==previous and (last is None or at>=last)
            previous=sha(path);last=at;rows.append(row)
        activation=rows[0] if rows and rows[0]['kind']=='activation' else None
        pinned=activation['payload'].get('sources',activation['payload'].get('implementation_sources',{})) if activation else {}
        source_matches={k:sha(ROOT/sources[k])==digest for k,digest in pinned.items()}
        protocol_matches=bool(activation and sha(ROOT/'data/research'/protocol)==activation['protocol_sha256'])
        sampled_at=datetime.now(timezone.utc)
        stamp=runtime.get('last_tick_at_utc');age=(sampled_at-utc(stamp)).total_seconds() if stamp else None
        books[name]={'activation_at_utc':activation['recorded_at_utc'] if activation else None,'records':len(rows),'record_kinds':dict(Counter(x['kind'] for x in rows)),
            'journal_sequence_hash_clock_valid':bool(valid),'source_sha_matches_activation':source_matches,'protocol_sha_matches_activation':protocol_matches,
            'last_tick_at_utc':stamp,'sampled_at_utc':sampled_at.isoformat(),'tick_age_seconds':age,'continuous_runner':runtime.get('continuous_runner'),'heartbeat_within_30_seconds':age is not None and 0<=age<=30,
            'last_pass_error':runtime.get('last_pass_error'),'observations':len(runtime.get('observations',{})),'processed_labels':len(runtime.get('processed_labels',{}))}
    except Exception as exc:books[name]={'error_type':type(exc).__name__}
result['alastar_books']=books
try:
    archive=ROOT/'data/research/alastar_observation';times=[];seen=set();first_delays=[];previous=None;valid=True
    for i,path in enumerate(sorted((archive/'runs').glob('*.json')),1):
        row=json.loads(path.read_bytes());valid &= row['sequence']==i and row['previous_run_sha256']==previous
        previous=sha(path)
        if row['mode']=='live_public_preview':times.append(utc(row['observed_at_utc']))
        for revision in row['new_revisions']:
            key=revision['message_key']
            if key not in seen and row['mode']=='live_public_preview' and revision.get('published_at_utc'):
                first_delays.append((utc(row['observed_at_utc'])-utc(revision['published_at_utc'])).total_seconds())
            seen.add(key)
    gaps=[(b-a).total_seconds() for a,b in zip(times,times[1:])]
    recent=[t for t in times if now-t<=timedelta(hours=6)]
    recent_gaps=[(b-a).total_seconds() for a,b in zip(recent,recent[1:])]
    result['alastar_collection']={'run_count':i,'hash_chain_valid':bool(valid),'live_runs':len(times),'last_live_run_at_utc':times[-1].isoformat() if times else None,
        'age_seconds':(now-times[-1]).total_seconds() if times else None,'all_time_max_poll_gap_seconds':max(gaps) if gaps else None,
        'last_6h_live_runs':len(recent),'last_6h_median_poll_gap_seconds':statistics.median(recent_gaps) if recent_gaps else None,
        'last_6h_max_poll_gap_seconds':max(recent_gaps) if recent_gaps else None,'last_6h_gaps_over_600_seconds':sum(g>600 for g in recent_gaps),
        'new_live_first_receipt_count':len(first_delays),'publication_to_receipt_delay_seconds':first_delays,
        'delivery_guarantee_established':False,'note':'Read-only archive snapshot, not a future delivery SLA; incomplete tail run may race the active collector.'}
except Exception as exc:result['alastar_collection']={'error_type':type(exc).__name__}
(OUT/'runtime_readonly.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
print(json.dumps(result,ensure_ascii=True,indent=2))
