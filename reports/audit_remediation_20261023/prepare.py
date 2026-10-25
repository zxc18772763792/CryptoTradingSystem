"""Preserve the current source and build a credential-free remediation test copy."""
from datetime import datetime,timezone
import hashlib,json,shutil,subprocess,tempfile
from pathlib import Path

OUT=Path(__file__).resolve().parent
ROOT=OUT.parents[1]
def git(*args):return subprocess.check_output(['git',*args],cwd=ROOT,encoding='utf-8').splitlines()
names=sorted(set(git('ls-files')+git('ls-files','--others','--exclude-standard')))
rows={}
base=Path(tempfile.mkdtemp(prefix='crypto_remediation_20261023_'));source=base/'source';source.mkdir()
for name in names:
    p=ROOT/name
    if name.startswith('reports/audit_remediation_20261023/') or not p.is_file():continue
    if p.name=='keys.txt' or (p.name.startswith('.env') and p.name!='.env.example') or p.suffix in ('.pem','.key'):continue
    rows[name]=hashlib.sha256(p.read_bytes()).hexdigest()
    target=source/name;target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(p,target)
for folder in ('guard','temp'):(base/folder).mkdir()
for name in ('test_guard.py','run_tests.py'):
    shutil.copy2(ROOT/'reports/full_audit_2026-10-22_parts'/name,OUT/name)
shutil.copy2(OUT/'test_guard.py',base/'guard/sitecustomize.py')
(OUT/'snapshot.json').write_text(json.dumps({'base':str(base),'source':str(source)},indent=2),encoding='utf-8')
(OUT/'baseline.json').write_text(json.dumps({'created_at_utc':datetime.now(timezone.utc).isoformat(),'head':git('rev-parse','HEAD')[0],'status':git('status','--short'),'sha256':rows},indent=2),encoding='utf-8')
print(json.dumps({'files':len(rows),'source':str(source),'original_sources_untouched':True}))
