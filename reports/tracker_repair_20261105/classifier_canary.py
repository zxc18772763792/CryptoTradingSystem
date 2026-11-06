"""One synthetic-only call to the already configured, protocol-pinned Gemini route."""
import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
from types import SimpleNamespace
import sys
import time

sys.stdout.reconfigure(encoding="utf-8")
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from scripts.telegram_classifier_transport import label_with_protocol_client
from core.news.eventizer.rules import load_news_rule_config

activation = json.loads((ROOT/"data/research/telegram_live_20261024/activation.json").read_text(encoding="utf-8"))
output = Path(__file__).with_name("classifier_canary")
output.mkdir(exist_ok=True)
store = SimpleNamespace(root=output, state=lambda *_: activation)

async def probe(store, **kwargs):
    # Imported dynamically, as in the frozen CLI. No source message or journal is read.
    from core.news.eventizer.async_glm_client import AsyncGLMClient
    client = AsyncGLMClient(load_news_rule_config(ROOT/"config/news_rules.yaml", ROOT/"config/symbols.yaml"))
    response, error = await client.chat_completions(
        [{"role":"system","content":"Return only a JSON object."},
         {"role":"user","content":"Return {\"transport_health\":true}. This is a synthetic connectivity check."}],
        temperature=0, max_tokens=64, timeout=25)
    return {"healthy":error=="none", "error_type":error}

started=time.monotonic()
try:
    result=asyncio.run(label_with_protocol_client(probe,store))
except Exception as error:
    result={"healthy":False,"error_type":type(error).__name__}
result.update(at=datetime.now(timezone.utc).isoformat(),elapsed_seconds=time.monotonic()-started,
              synthetic_only=True,frozen_model=activation["classifier_model"])
(output/"result.json").write_text(json.dumps(result,indent=2),encoding="utf-8")
print(json.dumps(result))
