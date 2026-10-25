"""Read-only workspace inventory and isolated offline test snapshot preparation."""
import ast
from collections import Counter
from datetime import datetime,timezone
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

ROOT=Path(__file__).resolve().parents[2]
OUT=Path(__file__).resolve().parent
CODE={'.py','.js','.cjs','.mjs','.ts','.ps1','.bat','.sh','.yaml','.yml','.json','.toml','.ini','.html','.css'}


def git(*args):
    return subprocess.check_output(['git',*args],cwd=ROOT,encoding='utf-8').splitlines()


def write(name,obj):
    (OUT/name).write_text(json.dumps(obj,ensure_ascii=False,indent=2),encoding='utf-8')


def inventory():
    tracked=git('ls-files'); others=git('ls-files','--others','--exclude-standard')
    names=sorted(set(tracked+others)); rows=[]; routes=[]; candidates=[]; counts=Counter()
    for name in names:
        if name.startswith('reports/full_audit_2026-10-22'): continue
        p=ROOT/name
        if not p.is_file(): continue
        if p.name in ('keys.txt',) or (p.name.startswith('.env') and p.name!='.env.example') or p.suffix in ('.pem','.key'):
            rows.append({'path':name,'tracked':name in tracked,'classification':'secret_filename_excluded'});continue
        raw=p.read_bytes(); info={'path':name,'tracked':name in tracked,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'suffix':p.suffix.lower()}
        counts['files']+=1; counts['bytes']+=len(raw)
        if p.suffix.lower() in CODE or p.suffix=='.md' or p.name in ('Dockerfile','.gitignore','.dockerignore','requirements.txt'):
            try: text=raw.decode('utf-8-sig'); info['lines']=len(text.splitlines())
            except UnicodeDecodeError: info['text_decode']='not_utf8'; rows.append(info);continue
            if p.suffix=='.py':
                counts['python_files']+=1; counts['python_lines']+=info['lines']
                try:
                    tree=ast.parse(text,filename=name); info['syntax']='ok'
                    functions=[n for n in ast.walk(tree) if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef))]
                    info['functions']=len(functions)
                    for f in functions:
                        for d in f.decorator_list:
                            if isinstance(d,ast.Call) and isinstance(d.func,ast.Attribute) and d.func.attr in ('get','post','put','delete','patch','websocket'):
                                routes.append({'file':name,'line':f.lineno,'function':f.name,'method':d.func.attr,'route':ast.unparse(d.args[0]) if d.args else None,'decorator':ast.unparse(d)})
                except SyntaxError as e: info['syntax']={'line':e.lineno,'message':e.msg};counts['python_syntax_errors']+=1
            if p.suffix=='.json':
                try: json.loads(text);info['syntax']='ok'
                except Exception as e: info['syntax']=type(e).__name__;counts['json_syntax_errors']+=1
            if p.suffix in ('.yaml','.yml'):
                try:
                    import yaml
                    yaml.safe_load(text);info['syntax']='ok'
                except Exception as e: info['syntax']=type(e).__name__;counts['yaml_syntax_errors']+=1
            if p.suffix in ('.py','.js','.ts','.ps1','.json','.yaml','.yml') or p.name=='.env.example':
                for i,line in enumerate(text.splitlines(),1):
                    for category,pattern in [('private_key',r'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----'),('provider_key',r'\bsk-(?:proj-)?[A-Za-z0-9_-]{24,}'),('literal_secret',r'''(?i)(?:api_secret|api_key|password|ops_token)\s*[=:]\s*['"][A-Za-z0-9_+/=-]{24,}['"]''')]:
                        if re.search(pattern,line): candidates.append({'file':name,'line':i,'category':category,'test_or_report':name.startswith(('tests/','reports/'))})
        rows.append(info)
    write('inventory.json',{'created_at_utc':datetime.now(timezone.utc).isoformat(),'head':git('rev-parse','HEAD')[0],'tracked_paths':len(tracked),'untracked_paths':len(others),'counts':counts,'files':rows})
    write('routes_static.json',routes);write('secret_candidates_redacted.json',candidates)
    write('installed_packages.json',[{'name':d.metadata['Name'],'version':d.version} for d in importlib.metadata.distributions() if d.metadata.get('Name')])
    write('working_tree_before.json',{'status':git('status','--short'),'source_sha256':{r['path']:r['sha256'] for r in rows if 'sha256' in r and not r['path'].startswith('reports/')}})
    print(json.dumps({'counts':counts,'route_decorators':len(routes),'secret_candidates_no_values':len(candidates)},ensure_ascii=True),flush=True)
    return rows


def prepare(rows):
    base=Path(tempfile.mkdtemp(prefix='crypto_full_audit_20261022_')); source=base/'source';source.mkdir()
    for row in rows:
        if 'sha256' not in row: continue
        name=row['path']; original=ROOT/name
        if not original.resolve().is_relative_to(ROOT): continue
        target=source/name;target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(original,target)
    guard=base/'guard';guard.mkdir();(base/'temp').mkdir()
    shutil.copy2(OUT/'test_guard.py',guard/'sitecustomize.py')
    write('snapshot.json',{'base':str(base),'source':str(source),'files':len(rows),'current_working_tree_included':True,'real_env_and_runtime_data_copied':False,'created_at_utc':datetime.now(timezone.utc).isoformat()})
    print('snapshot '+str(source),flush=True)


if __name__=='__main__': prepare(inventory())
