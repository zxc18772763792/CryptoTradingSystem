"""Observe existing managed cycles; never invoke or compete with the collector."""
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time

ROOT=Path(__file__).resolve().parents[2]
OUT=Path(__file__).resolve().parent
BASE=ROOT/"data/research/telegram_live_20261024"

def read(path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError,ValueError):
        return {}

def stamp(value):
    return datetime.fromisoformat(value.replace("Z","+00:00")) if value else datetime.min.replace(tzinfo=timezone.utc)

prior=read(OUT/"managed_cycles.json")
records=prior.get("records",[])
seen={r["run_at"] for r in records}
deadline=time.monotonic()+720
successes=prior.get("consecutive_healthy_cycles",0)
while successes<3 and time.monotonic()<deadline:
    runner=read(ROOT/"runtime/telegram_forward_runner.json")
    key=runner.get("last_run_at")
    if key and key not in seen:
        diagnostic=read(BASE/"classifier_transport_status.json")
        status=read(BASE/"forward_status.json")
        error=read(BASE/"last_error.json")
        if (diagnostic.get("started_at") and stamp(diagnostic["started_at"])>=stamp("2026-11-05T04:53:00+00:00")
                and stamp(diagnostic.get("at"))<=stamp(key)):
            seen.add(key)
            sources={name:{k:part.get(k) for k in ("complete_batch","difference_gap","clean_since","last_collection_at")}
                     for name,part in (status.get("sources") or {}).items() if name in ("cryptoquant_official","BinanceChinese")}
            healthy=(runner.get("last_outcome")=="ok" and runner.get("last_exit_code")==0
                     and diagnostic.get("outcome")=="ok" and stamp(status.get("checked_at"))>=stamp(diagnostic.get("at"))
                     and stamp(status.get("checked_at"))>=stamp(error.get("at"))
                     and all(s.get("complete_batch") is True and not s.get("difference_gap") for s in sources.values())
                     and len(sources)==2)
            # A short study/reader-lock collision is an expected skip, not a
            # completed failed pipeline cycle. Keep it visible without claiming
            # a new success or erasing completed healthy cycles.
            if healthy:
                successes+=1
            elif runner.get("last_outcome") not in {"busy","cooldown"}:
                successes=0
            records.append({"run_at":key,"outcome":runner.get("last_outcome"),"error_type":runner.get("last_error_type"),
                            "classifier_outcome":diagnostic.get("outcome"),"classifier_events":diagnostic.get("events"),
                            "forward_checked_at":status.get("checked_at"),"sources":sources,"healthy":healthy,
                            "consecutive_healthy_cycles":successes})
            result={"captured_at":datetime.now(timezone.utc).isoformat(),"consecutive_healthy_cycles":successes,
                    "target":3,"complete":successes>=3,"records":records}
            (OUT/"managed_cycles.json").write_text(json.dumps(result,indent=2),encoding="utf-8")
            print(json.dumps({"run_at":key,"outcome":runner.get("last_outcome"),"healthy":healthy,"consecutive":successes}),flush=True)
            if successes>=3:
                break
    time.sleep(2)
else:
    print(json.dumps({"verification":"timed_out","consecutive":successes}),flush=True)
    sys.exit(1)
