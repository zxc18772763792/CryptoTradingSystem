"""Bounded, recorded conda security update of an isolated validation prefix."""
import json,os,subprocess,sys,time
from pathlib import Path

OUT=Path(__file__).resolve().parent
CONDA=Path(r'F:/9_Crypto/.conda/miniforge3/Scripts/conda.exe')
PREFIX=Path(r'F:/9_Crypto/.conda/audit_security_20261023')
specs=['python=3.11','numpy=1.26.*','pandas=2.3.*','pyarrow=24.0.*',
       'certifi>=2026.10.19','urllib3>=2.8.0,<3','aiohttp>=3.14.4,<4','multidict>=6.9.1,<7',
       'pyjwt>=2.15.0,<3','tornado>=6.5.9,<7','pillow>=13.0.0,<14','pytest>=9.0.3,<10',
       'pytest-asyncio>=1,<2','pytest-cov>=6,<7','mypy>=1.14,<2','types-redis>=4.6,<5','schedule>=1.2,<2','web3>=7.6,<8']
mode=sys.argv[1]
command=[str(CONDA),'install','--prefix',str(PREFIX),'--override-channels','-c','conda-forge','--freeze-installed','--json',*specs]
if mode=='plan':command+=['--dry-run']
elif mode=='apply':command+=['-y']
else:raise SystemExit('plan or apply required')
env=dict(os.environ);env['CONDA_AUTO_UPDATE_CONDA']='false'
start=time.monotonic();log=OUT/f'conda_{mode}.log'
try:
    with log.open('w',encoding='utf-8') as stream:result=subprocess.run(command,env=env,stdout=stream,stderr=subprocess.STDOUT,timeout=900)
    code=result.returncode
except subprocess.TimeoutExpired:code=124
summary={'command':command,'exit_code':code,'seconds':round(time.monotonic()-start,2),'prefix':str(PREFIX)}
(OUT/f'conda_{mode}_receipt.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
print(json.dumps(summary));print(log.read_text(encoding='utf-8',errors='replace')[-1800:])
raise SystemExit(code)
