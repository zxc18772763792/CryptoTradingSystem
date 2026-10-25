"""Read-only audit checks; execute no application startup or trading code."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import fnmatch
import importlib.metadata
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import urllib.request
import xml.etree.ElementTree as ET

OUT=Path(__file__).resolve().parent
ROOT=OUT.parents[1]
SNAP=json.loads((OUT/'snapshot.json').read_text())
SOURCE=Path(SNAP['source'])
BASE=Path(SNAP['base'])
INVENTORY=json.loads((OUT/'inventory.json').read_text())
NODE=Path(r'C:/Users/zxc/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin/node.exe')

def save(name,data):
    (OUT/name).write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')

def clean_env():
    keep={'SYSTEMROOT','WINDIR','COMSPEC','PATH','PATHEXT','USERPROFILE','APPDATA','LOCALAPPDATA','PROGRAMDATA','PROGRAMFILES','PROGRAMFILES(X86)','PROCESSOR_ARCHITECTURE','NUMBER_OF_PROCESSORS'}
    env={k:v for k,v in os.environ.items() if k.upper() in keep}
    env.update(TEMP=str(BASE/'temp'),TMP=str(BASE/'temp'),PYTHONUTF8='1',PYTHONDONTWRITEBYTECODE='1',TRADING_MODE='paper',OPS_TOKEN='audit-synthetic-only')
    return env

def run(cmd):
    result=subprocess.run([str(x) for x in cmd],cwd=SOURCE,env=clean_env(),capture_output=True,text=True,encoding='utf-8',errors='replace',timeout=60)
    return {'command':[str(x) for x in cmd],'exit_code':result.returncode,'stdout':result.stdout,'stderr':result.stderr}

def local_checks():
    js=[x['path'] for x in INVENTORY['files'] if x.get('suffix') in ('.js','.cjs','.mjs')]
    def check_js(name):
        result=run([NODE,'--check',SOURCE/name]);return {'file':name,**result}
    with ThreadPoolExecutor(max_workers=4) as pool:
        checks=list(pool.map(check_js,js))
    save('javascript_syntax.json',{'files':len(checks),'errors':sum(x['exit_code']!=0 for x in checks),'results':checks})
    save('node_tests.json',run([NODE,'--test','tests/research_tracker_diagnostics.test.cjs']))
    save('config_contract.json',run(['powershell.exe','-NoProfile','-ExecutionPolicy','Bypass','-File','scripts/check_config_contract.ps1','-ProjectRoot',SOURCE]))
    save('pip_check.json',run([sys.executable,'-m','pip','check']))
    save('environment_check.json',run([sys.executable,'scripts/verify_env.py','--strict']))
    from packaging.requirements import Requirement
    contract=[]
    for line in (ROOT/'requirements.txt').read_text(encoding='utf-8-sig').splitlines():
        line=line.split('#',1)[0].strip()
        if not line:continue
        requirement=Requirement(line)
        if requirement.marker and not requirement.marker.evaluate():continue
        try:version=importlib.metadata.version(requirement.name)
        except importlib.metadata.PackageNotFoundError:version=None
        contract.append({'name':requirement.name,'specifier':str(requirement.specifier),'installed':version,'satisfies':version is not None and version in requirement.specifier})
    save('requirements_contract.json',contract)
    ignored=subprocess.check_output(['git','ls-files','--others','--ignored','--exclude-standard','--','scripts','core','web','config','strategies','prediction_markets'],cwd=ROOT,encoding='utf-8').splitlines()
    first_party=[p for p in ignored if Path(p).suffix.lower() in {'.py','.ps1','.js','.ts','.bat','.sh'} and '__pycache__' not in Path(p).parts]
    save('ignored_source_paths.json',{'files':first_party,'count':len(first_party),'contents_not_read':True})
    patterns=[line.strip() for line in (ROOT/'.dockerignore').read_text(encoding='utf-8').splitlines() if line.strip() and not line.startswith('#')]
    candidates=['.env','.env.local','.env.production','.env.backup','.env.test','.env.production.local','.env.example']
    docker=[]
    for name in candidates:
        matched=[];excluded=False
        for pattern in patterns:
            negated=pattern.startswith('!');pattern=pattern.lstrip('!').strip('/')
            if fnmatch.fnmatchcase(name,pattern):excluded=not negated;matched.append(('!' if negated else '')+pattern)
        git=subprocess.run(['git','check-ignore','--no-index','--stdin'],cwd=ROOT,input=(name+'\n').encode(),capture_output=True)
        docker.append({'candidate':name,'docker_context_excluded':excluded,'matching_root_name_patterns':matched,'git_ignored':git.returncode==0})
    save('docker_context_secret_names.json',{'method':'Static root-name pattern analysis only; no real env file read, created, or image built','examples':docker,'copy_all_source':'Dockerfile COPY --chown=cts:cts . /app'})
    xml=ET.parse(OUT/'pytest_215344.xml')
    skips=[{'test':x.get('name'),'class':x.get('classname'),'reason':x.find('skipped').get('message')} for x in xml.iter('testcase') if x.find('skipped') is not None]
    save('full_suite_summary.json',{'suite_attributes':[x.attrib for x in xml.iter('testsuite')],'skip_reasons':skips})
    observations=SOURCE/'audit_observations.jsonl'
    if observations.exists():shutil.copy2(observations,OUT/'observations.jsonl')
    print(json.dumps({'js_files':len(js),'js_errors':sum(x['exit_code']!=0 for x in checks),'requirements_noncompliant':[x for x in contract if not x['satisfies']],'ignored_first_party_source':len(first_party),'skip_reasons':skips},ensure_ascii=True))

def osv_checks():
    packages=json.loads((OUT/'installed_packages.json').read_text())
    queries=[{'package':{'name':x['name'],'ecosystem':'PyPI'},'version':x['version']} for x in packages]
    url='https://api.osv.dev/v1/querybatch'
    try:
        request=urllib.request.Request(url,data=json.dumps({'queries':queries}).encode(),headers={'Content-Type':'application/json','User-Agent':'crypto-project-read-only-audit/2026-10-22'},method='POST')
        with urllib.request.urlopen(request,timeout=45) as response:data=json.load(response)
        rows=[]
        for package,result in zip(packages,data.get('results',[])):
            rows.append({**package,**result})
        ids=sorted({v['id'] for row in rows for v in row.get('vulns',[])})
        save('osv_querybatch.json',{'queried_at_utc':datetime.now(timezone.utc).isoformat(),'endpoint':url,'query_count':len(queries),'result_count':len(rows),'results':rows,'vulnerability_ids':ids,'pagination_required':any(x.get('next_page_token') for x in rows)})
        details=[]
        for identifier in ids:
            try:
                with urllib.request.urlopen('https://api.osv.dev/v1/vulns/'+identifier,timeout=20) as response:details.append(json.load(response))
            except Exception as exc:details.append({'id':identifier,'fetch_error':str(exc)})
        save('osv_advisories.json',details)
        print(json.dumps({'osv_packages':len(queries),'osv_affected_packages':sum(bool(x.get('vulns')) for x in rows),'osv_ids':ids,'pagination_required':any(x.get('next_page_token') for x in rows)}))
    except Exception as exc:
        save('osv_querybatch.json',{'queried_at_utc':datetime.now(timezone.utc).isoformat(),'endpoint':url,'query_count':len(queries),'error':str(exc),'completed':False})
        print('OSV incomplete: '+str(exc))

if __name__=='__main__':
    if sys.argv[1:]==['osv']:osv_checks()
    else:local_checks()
