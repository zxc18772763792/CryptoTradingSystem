"""One bounded parallel retry for OSV advisory detail transport failures."""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import urllib.request

OUT=Path(__file__).resolve().parent
path=OUT/'osv_advisories.json'
rows=json.loads(path.read_text(encoding='utf-8'))
initial=[x['id'] for x in rows if x.get('fetch_error')]
def fetch(identifier):
    try:
        with urllib.request.urlopen('https://api.osv.dev/v1/vulns/'+identifier,timeout=12) as response:return identifier,json.load(response)
    except Exception as exc:return identifier,{'id':identifier,'fetch_error':str(exc)}
with ThreadPoolExecutor(max_workers=4) as pool:updated=dict(pool.map(fetch,initial))
rows=[updated.get(row['id'],row) for row in rows]
path.write_text(json.dumps(rows,ensure_ascii=False,indent=2),encoding='utf-8')
(OUT/'osv_detail_retry.json').write_text(json.dumps({'attempted':initial,'remaining_errors':[x['id'] for x in rows if x.get('fetch_error')],'retry_rounds':1},indent=2),encoding='utf-8')
print(json.dumps({'attempted':len(initial),'remaining_errors':sum(bool(x.get('fetch_error')) for x in rows)}))
