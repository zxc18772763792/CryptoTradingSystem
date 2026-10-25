"""Execute the current workspace snapshot with no credentials or real network."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

OUT=Path(__file__).resolve().parent
snapshot=json.loads((OUT/'snapshot.json').read_text(encoding='utf-8'));base=Path(snapshot['base']);source=Path(snapshot['source'])
shutil.copy2(OUT/'test_guard.py',base/'guard/sitecustomize.py')
keep={'SYSTEMROOT','WINDIR','COMSPEC','PATH','PATHEXT','USERPROFILE','APPDATA','LOCALAPPDATA','PROGRAMDATA','PROGRAMFILES','PROGRAMFILES(X86)','PROCESSOR_ARCHITECTURE','NUMBER_OF_PROCESSORS'}
env={k:v for k,v in os.environ.items() if k.upper() in keep}
env.update(CTS_AUDIT_BASE=str(base),TEMP=str(base/'temp'),TMP=str(base/'temp'),PYTHONPATH=str(base/'guard'),PYTHONUTF8='1',PYTHONDONTWRITEBYTECODE='1',TRADING_MODE='paper',OPS_TOKEN='audit-synthetic-ops-token',
 AI_AUTONOMOUS_AGENT_AUTO_START='false',MARKET_WS_ENABLED='false',MARKET_WS_MODE='off',NEWS_WORKER_AUTO_START='false',NEWS_LLM_WORKER_AUTO_START='false',PM_WORKER_AUTO_START='false',
 PREMIUM_EXTERNAL_WORKERS_ENABLED='false',PUBLIC_MACRO_WORKERS_ENABLED='false',COINGLASS_WORKER_ENABLED='false',BINANCE_ALPHA_COLLECTOR_ENABLED='false',
 MPLCONFIGDIR=str(base/'mpl'),HF_HOME=str(base/'hf'),HF_HUB_OFFLINE='1',TRANSFORMERS_OFFLINE='1',XDG_CACHE_HOME=str(base/'cache'),NUMBA_CACHE_DIR=str(base/'numba'),JOBLIB_TEMP_FOLDER=str(base/'temp'),TORCH_HOME=str(base/'torch'),GATE_COUNTERFACTUAL_AUDIT_PATH=str(base/'counterfactual.jsonl'))
stamp=time.strftime('%H%M%S');log=OUT/f'pytest_{stamp}.log';xml=base/f'pytest_{stamp}.xml'
cmd=[sys.executable,'-m','pytest','-q','-m','not live',*sys.argv[1:],'--basetemp='+str(base/('pytest_tmp_'+stamp)),'--junitxml='+str(xml)]
if sys.argv[1:]==['--guard-probe']:
    probe="""import json,os,pathlib,socket
checks={}
def check(name,fn):
    try: fn()
    except PermissionError as e: checks[name]='AUDIT_GUARD' in str(e)
    else: checks[name]=False
check('original_workspace',lambda:pathlib.Path(r'F:/9_Crypto/crypto_trading_system/AGENTS.md').read_text())
check('external_dns',lambda:socket.getaddrinfo('example.com',443))
check('existing_service',lambda:socket.create_connection(('127.0.0.1',8000),timeout=1))
check('external_write',lambda:(pathlib.Path(os.environ['CTS_AUDIT_BASE']).parent/'audit_probe_must_not_write.tmp').write_text('probe'))
checks['paper']=os.environ.get('TRADING_MODE')=='paper'
print(json.dumps(checks));assert all(checks.values()),checks
"""
    cmd=[sys.executable,'-c',probe]
print('Isolated snapshot: '+str(source),flush=True);start=time.monotonic()
with log.open('w',encoding='utf-8') as stream:
    result=subprocess.run(cmd,cwd=source,env=env,stdout=stream,stderr=subprocess.STDOUT)
if xml.exists():shutil.copy2(xml,OUT/xml.name)
summary={'command':cmd[2:],'exit_code':result.returncode,'seconds':round(time.monotonic()-start,2),'source':str(source),'log':str(log),'xml':str(OUT/xml.name),
         'credentials_removed':True,'external_and_existing_service_connections_blocked':True,'writes_confined_to_disposable_directory':True,'original_workspace_blocked':True}
(OUT/f'run_{stamp}.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
print(json.dumps(summary),flush=True)
print(log.read_text(encoding='utf-8',errors='replace')[-6000:])
raise SystemExit(result.returncode)
