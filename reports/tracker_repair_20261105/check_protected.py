"""Verify protected bytes and expose only safe tracker/health metadata."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
from urllib.request import urlopen

sys.stdout.reconfigure(encoding="utf-8")
ROOT=Path(__file__).resolve().parents[2]
OUT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT))
from core.research import telegram_forward_tracker as views

baseline=json.loads((OUT/"diagnosis.json").read_text(encoding="utf-8"))["protected_hashes"]
unchanged={p:hashlib.sha256((ROOT/p).read_bytes()).hexdigest()==digest for p,digest in baseline.items()}
result={"captured_at":datetime.now(timezone.utc).isoformat(),"protected_unchanged":unchanged,"trackers":{}}
for kind in ("cryptoquant","binance_chat"):
    state=views.summary(kind)
    result["trackers"][kind]={k:state.get(k) for k in
                              ("runner_active","tracker_status_label","source_health","last_error","runner")}
result["health"]={}
for endpoint in ("livez","readyz"):
    try:
        with urlopen("http://127.0.0.1:8000/"+endpoint,timeout=10) as response:
            result["health"][endpoint]={"http":response.status,"body":json.load(response)}
    except Exception as exc:
        result["health"][endpoint]={"error_type":type(exc).__name__}
(OUT/"protected_and_health.json").write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding="utf-8")
print(json.dumps(result,ensure_ascii=False))
