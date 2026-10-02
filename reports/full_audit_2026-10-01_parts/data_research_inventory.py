"""Read-only source inventory and static checks for the assigned audit boundary."""
from __future__ import annotations
import ast
import collections
import hashlib
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]
SCOPES = ['core/ai','core/ml','core/research','core/backtest','core/data','core/marketdata','core/realtime',
          'core/news','core/monitoring','core/observability','prediction_markets','core/factors_ts',
          'core/indicators','core/notifications','core/deployment']
PATTERNS = {
    'future_or_fill': r'bfill|shift\(\s*-|center\s*=\s*True|interpolate|lookahead|future',
    'persistence': r'write_text|write_bytes|to_parquet|to_csv|os\.replace|commit\(|flush\(',
    'concurrency': r'Lock\(|RLock\(|create_task|gather\(|to_thread|ThreadPool|threading\.local',
    'network': r'requests\.|httpx\.|aiohttp|websocket|ccxt|urlopen',
    'freshness': r'age_ms|max_age|stale|timestamp_exchange|published_at|asof|available_at',
    'security': r'eval\(|exec\(|pickle|subprocess|shell\s*=|logger.*token|logger.*secret',
}
records = []
for scope in SCOPES:
    for p in sorted((ROOT/scope).rglob('*.py')):
        if '__pycache__' in p.parts:
            continue
        source = p.read_text(encoding='utf-8-sig')
        rel = p.relative_to(ROOT).as_posix()
        r = {'path':rel, 'lines':len(source.splitlines()), 'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),
             'parse_error':None, 'definitions':[], 'imports':[], 'top_level_calls':[], 'patterns':{}}
        try:
            tree=ast.parse(source)
            r['definitions'] = [{'name':n.name, 'line':n.lineno, 'end':n.end_lineno, 'kind':type(n).__name__}
                                for n in ast.walk(tree) if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef))]
            r['imports'] = [{'module':n.module or '', 'line':n.lineno} for n in ast.walk(tree) if isinstance(n,ast.ImportFrom)]
            for n in tree.body:
                if isinstance(n,(ast.Assign,ast.AnnAssign,ast.Expr)):
                    for call in ast.walk(n):
                        if isinstance(call,ast.Call):
                            r['top_level_calls'].append({'line':call.lineno,'call':ast.unparse(call.func)})
        except SyntaxError as e:
            r['parse_error'] = str(e)
        for category,pattern in PATTERNS.items():
            r['patterns'][category]=[i for i,line in enumerate(source.splitlines(),1) if re.search(pattern,line,re.I)]
        records.append(r)
tests=[]
for p in sorted((ROOT/'tests').rglob('*.py')):
    text=p.read_text(encoding='utf-8-sig')
    matched=[s for s in SCOPES if s.replace('/','.') in text or s in text]
    if matched:
        tests.append({'path':p.relative_to(ROOT).as_posix(),'scopes':matched,'lines':len(text.splitlines())})
payload={'head':'2fecd05dea8c5ee438b4071e50548b828b7411af','scopes':SCOPES,
         'source_count':len(records),'source_lines':sum(r['lines'] for r in records),
         'parse_errors':[r for r in records if r['parse_error']], 'files':records,'test_candidates':tests}
out=Path(__file__).parent
(out/'data_research_inventory.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
rows=['# 数据、AI、研究子审计逐文件覆盖清单','',
      '下列全部 Python 文件均完成完整文本读取、AST 结构解析、导入/顶层调用与未来数据/持久化/并发/网络/时效/执行危险模式扫描。',
      '此清单区分结构扫描与重点人工审查；并不代表每一函数都经过动态复现。完整定义、行号、SHA256 与匹配位置见 data_research_inventory.json。',
      '', '| 文件 | 行数 | AST | 定义数 | 静态关注面 |','|---|---:|---|---:|---|']
for r in records:
    cats='、'.join(k for k,v in r['patterns'].items() if v) or '纯定义/初始化'
    rows.append(f"| `{r['path']}` | {r['lines']} | {'失败' if r['parse_error'] else '通过'} | {len(r['definitions'])} | {cats} |")
rows.extend(['','## 关联测试文件（按源码模块字符串识别，实际执行由主审计汇总）',''])
rows.extend(f"- `{t['path']}`：{'、'.join(t['scopes'])}" for t in tests)
(out/'data_research_coverage.md').write_text('\n'.join(rows)+'\n',encoding='utf-8')
print(json.dumps({'source_count':len(records),'source_lines':sum(r['lines'] for r in records),
                  'parse_errors':len(payload['parse_errors']),'test_candidates':len(tests),
                  'by_scope':{s:sum(r['path'].startswith(s+'/') for r in records) for s in SCOPES}},ensure_ascii=False))
