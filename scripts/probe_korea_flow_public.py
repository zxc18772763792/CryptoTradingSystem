"""One bounded public Upbit market/ticker snapshot for factor feasibility.

No accounts, orders, credentials, recurring task or old research writes.
"""
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import httpx
from config.settings import settings
from core.utils.shared_ssl import get_shared_ssl_context


def main():
    folder=ROOT/'data/research/korea_factor_selection_20261029/flow_probe_01'
    folder.mkdir(parents=True,exist_ok=False)
    args={'timeout':15,'verify':get_shared_ssl_context()}
    if settings.HTTPS_PROXY or settings.HTTP_PROXY:
        args['proxy']=settings.HTTPS_PROXY or settings.HTTP_PROXY
    def save(name,obj):
        with (folder/name).open('x',encoding='utf-8') as stream:
            json.dump(obj,stream,indent=2,ensure_ascii=False)
    with httpx.Client(**args) as client:
        def get(url,params):
            rec={'url':url,'params':params,'attempts':[]}
            for attempt in range(2):
                row={'started_at_utc':datetime.now(timezone.utc).isoformat()}
                retry=False
                try:
                    r=client.get(url,params=params);row['http_status']=r.status_code
                    retry=r.status_code>=500;r.raise_for_status();rec['body']=r.json()
                    rec.pop('error_type',None)
                except Exception as exc:
                    rec['error_type']=row['error_type']=type(exc).__name__
                    retry=retry or isinstance(exc,httpx.TransportError)
                rec['attempts'].append(row)
                if not retry or attempt==1:break
                time.sleep(1)
            rec['received_at_utc']=datetime.now(timezone.utc).isoformat()
            return rec
        markets=get('https://api.upbit.com/v1/market/all',{'is_details':'true'})
        save('market_list.json',markets)
        if markets.get('error_type') or not isinstance(markets.get('body'),list):
            print(json.dumps({'market_list_error':markets.get('error_type'),'ticker_requests':0}));return
        pairs=[r['market'] for r in markets['body'] if r['market'].startswith('KRW-')]
        batches=[]
        for offset in range(0,len(pairs),100):
            time.sleep(1)
            rows=pairs[offset:offset+100]
            rec=get('https://api.upbit.com/v1/ticker',{'markets':','.join(rows)})
            batches.append(rec)
        save('krw_ticker_batches.json',batches)
        observed={r['market'] for b in batches if isinstance(b.get('body'),list) and not b.get('error_type') for r in b['body']}
        save('coverage.json',{'expected_krw_pairs':pairs,'observed_pairs':sorted(observed),
                              'missing_pairs':sorted(set(pairs)-observed),'snapshot_only':True})
        print(json.dumps({'market_count':len(markets['body']),'krw_pair_count':len(pairs),
                          'observed':len(observed),'missing':len(set(pairs)-observed),
                          'INIT_pairs':[r['market'] for r in markets['body'] if r['market'].endswith('-INIT')]},ensure_ascii=False))


if __name__=='__main__':
    sys.stdout.reconfigure(encoding='utf-8');main()
