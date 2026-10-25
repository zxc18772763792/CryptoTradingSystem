"""Summarize collected audit evidence without importing the application."""
from collections import Counter
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import subprocess
import xml.etree.ElementTree as ET

OUT=Path(__file__).resolve().parent
ROOT=OUT.parents[1]
def read(name):return json.loads((OUT/name).read_text(encoding='utf-8-sig'))
def save(name,obj):(OUT/name).write_text(json.dumps(obj,ensure_ascii=False,indent=2),encoding='utf-8')

def summarize():
    advisories=read('osv_advisories.json')
    rows=[]
    for item in advisories:
        rows.append({'id':item['id'],'summary':item.get('summary'),'published':item.get('published'),'withdrawn':item.get('withdrawn'),
            'severity':item.get('database_specific',{}).get('severity'),'packages':sorted({a['package']['name'] for a in item.get('affected',[]) if a.get('package',{}).get('ecosystem')=='PyPI'}),
            'fixed':sorted({e['fixed'] for a in item.get('affected',[]) for r in a.get('ranges',[]) for e in r.get('events',[]) if 'fixed' in e}),
            'aliases':item.get('aliases',[]),'references':item.get('references',[]),'error':item.get('fetch_error')})
    save('osv_triage.json',rows)
    print(json.dumps([{k:v for k,v in x.items() if k not in ('references','aliases')} for x in rows if not x['id'].startswith('PYSEC')],ensure_ascii=True,indent=2))
    inv=read('inventory.json');root_counts=Counter(x['path'].split('/')[0] if '/' in x['path'] else '(root)' for x in inv['files'])
    python_counts=Counter(x['path'].split('/')[0] if '/' in x['path'] else '(root)' for x in inv['files'] if x.get('suffix')=='.py')
    suffix_counts=Counter(x.get('suffix','') for x in inv['files'])
    suites=ET.parse(OUT/'pytest_215344.xml')
    cases=list(suites.iter('testcase'))
    legacy=[x for x in cases if any(n in x.get('classname','') for n in ('test_audit_remediation_','test_audit_paper_atomicity'))]
    alastar=[x for x in cases if 'test_alastar' in x.get('classname','')]
    save('coverage_counts.json',{'root_file_counts':root_counts,'python_file_counts':python_counts,'suffix_counts':suffix_counts,
        'legacy_regression_cases':len(legacy),'legacy_failures':sum(x.find('failure') is not None or x.find('error') is not None for x in legacy),
        'alastar_cases':len(alastar),'alastar_failures':sum(x.find('failure') is not None or x.find('error') is not None for x in alastar),
        'python_collected_test_cases':len(cases),'executed_test_modules':len({x.get('classname','').split('.')[1] for x in cases if len(x.get('classname','').split('.'))>1})})

def workspace_check():
    before=read('working_tree_before.json')['source_sha256'];changed=[];missing=[]
    for name,expected in before.items():
        path=ROOT/name
        if not path.is_file():missing.append(name)
        elif hashlib.sha256(path.read_bytes()).hexdigest()!=expected:changed.append(name)
    status=subprocess.check_output(['git','status','--short'],cwd=ROOT,encoding='utf-8').splitlines()
    save('working_tree_after.json',{'checked_at_utc':datetime.now(timezone.utc).isoformat(),'baseline_nonreport_files':len(before),
        'changed_nonreport_files':changed,'missing_nonreport_files':missing,'status':status,'audit_writes_confined_to_reports':not changed and not missing})
    print(json.dumps({'nonreport_baseline':len(before),'changed':changed,'missing':missing}))

if __name__=='__main__':
    summarize();workspace_check()
