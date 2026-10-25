import json, shutil
from pathlib import Path
OUT=Path(__file__).resolve().parent;ROOT=OUT.parents[1]
snapshot=json.loads((OUT/'snapshot.json').read_text(encoding='utf-8'))
names=list(json.loads((OUT/'edit_baseline.json').read_text(encoding='utf-8')))+[
    'tests/test_audit_remediation_20261023.py','scripts/check_dependency_contract.py','tests/test_dependency_contract.py',
    'scripts/setup_local_env.ps1','tests/test_sensitive_api_auth.py','tests/test_ai_research_autonomous_agent_api.py','environment.yml']
for name in names:shutil.copy2(ROOT/name,Path(snapshot['source'])/name)
print('Refreshed isolated snapshot:',len(names),'source files')
