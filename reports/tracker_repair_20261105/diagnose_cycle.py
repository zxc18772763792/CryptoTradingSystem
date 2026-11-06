"""One normal locked cycle, tracing only allowlisted static classifier failures."""
from datetime import datetime, timezone
import contextlib
import hashlib
import io
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from scripts import telegram_forward_cycle as wrapper

ALLOWED = {"Model unavailable", "Forward classifier model changed; explicit migration required",
           "Semantic review unavailable", "Incomplete labels; no price evaluation or forward freeze this cycle",
           "Incomplete or duplicate model label batch", "Incomplete semantic review", "Invalid semantic decision"}
failures = []
def local_trace(frame, event, arg):
    if event == "exception":
        kind, error, _ = arg
        message = str(error)
        if message in ALLOWED:
            failures.append({"at": datetime.now(timezone.utc).isoformat(), "type": kind.__name__, "reason": message})
    return local_trace

def trace(frame, event, arg):
    if event == "call" and frame.f_code is wrapper.cli.label.__code__:
        return local_trace
    return None

protected = ["scripts/telegram_forward_research.py", "core/research/telegram_forward.py",
             "scripts/telegram_desktop_bridge.py", "core/research/telegram_text_signals.py",
             "scripts/review_telegram_text.py", "data/research/telegram_live_20261024/activation.json",
             "data/research/telegram_live_20261024/protocol.json", "data/research/telegram_live_20261024/protocol_extension_6h.json"]
hashes = {p: hashlib.sha256((ROOT/p).read_bytes()).hexdigest() for p in protected}
start = datetime.now(timezone.utc).isoformat()
sys.argv = ["telegram_forward_cycle.py", "cycle"]
try:
    sys.settrace(trace)
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        code = wrapper.cli.main()
finally:
    sys.settrace(None)
    wrapper._transport.close()
result = {"started_at": start, "finished_at": datetime.now(timezone.utc).isoformat(),
          "exit_code": code, "classifier_failures": failures, "protected_hashes": hashes,
          "limits": "Normal existing study and reader locks; no raw messages, prompts, response bodies or credentials exported."}
path = Path(__file__).with_name("diagnosis.json")
path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
print(json.dumps({k: v for k,v in result.items() if k != "protected_hashes"}, ensure_ascii=True))
