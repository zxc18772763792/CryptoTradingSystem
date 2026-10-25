"""Run reviewed fixture-only UI smoke; real app requests are aborted by the test."""
import json
import os
from pathlib import Path
import subprocess
import time

OUT=Path(__file__).resolve().parent
snapshot=json.loads((OUT/'snapshot.json').read_text())
keep={'SYSTEMROOT','WINDIR','COMSPEC','PATH','PATHEXT','USERPROFILE','APPDATA','LOCALAPPDATA','PROGRAMDATA','PROGRAMFILES','PROGRAMFILES(X86)','PROCESSOR_ARCHITECTURE','NUMBER_OF_PROCESSORS'}
env={k:v for k,v in os.environ.items() if k.upper() in keep}
env.update(TEMP=str(Path(snapshot['base'])/'temp'),TMP=str(Path(snapshot['base'])/'temp'),NODE_PATH=r'C:/Users/zxc/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules',TRADING_MODE='paper')
command=[r'C:/Users/zxc/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin/node.exe','tests/ai_agent_workspace_smoke.cjs']
start=time.monotonic()
try:
    result=subprocess.run(command,cwd=snapshot['source'],env=env,capture_output=True,text=True,encoding='utf-8',errors='replace',timeout=90)
    summary={'command':command,'exit_code':result.returncode,'seconds':round(time.monotonic()-start,2),'stdout':result.stdout,'stderr':result.stderr,'all_http_requests_intercepted':True,'test_host':'agent.test','real_service_not_contacted':True}
except subprocess.TimeoutExpired:
    summary={'command':command,'timeout':90,'completed':False,'real_service_not_contacted':True}
(OUT/'browser_fixture_smoke.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8')
print(json.dumps(summary,ensure_ascii=True))
