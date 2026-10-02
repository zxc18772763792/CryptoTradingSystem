import os, sys, subprocess, pathlib, json, time
base = pathlib.Path(__file__).parent
source = base / 'source'
keep = ('SYSTEMROOT','WINDIR','COMSPEC','PATH','PATHEXT','TEMP','TMP','USERPROFILE','APPDATA','LOCALAPPDATA','PROGRAMDATA','PROGRAMFILES','PROGRAMFILES(X86)','PROCESSOR_ARCHITECTURE','NUMBER_OF_PROCESSORS')
env = {k: v for k, v in os.environ.items() if k.upper() in keep}
env.update(PYTHONPATH=str(base/'guard'), PYTHONUTF8='1', PYTHONDONTWRITEBYTECODE='1', TRADING_MODE='paper', AI_AUTONOMOUS_AGENT_AUTO_START='false', MARKET_WS_ENABLED='false', MARKET_WS_MODE='off', NEWS_WORKER_AUTO_START='false', NEWS_LLM_WORKER_AUTO_START='false', PM_WORKER_AUTO_START='false', PREMIUM_EXTERNAL_WORKERS_ENABLED='false', PUBLIC_MACRO_WORKERS_ENABLED='false', COINGLASS_WORKER_ENABLED='false', BINANCE_ALPHA_COLLECTOR_ENABLED='false', MPLCONFIGDIR=str(base/'mpl'), GATE_COUNTERFACTUAL_AUDIT_PATH=str(base/'counterfactual.jsonl'))
cmd = [sys.executable, '-m', 'pytest', '-q', '-m', 'not live', '--basetemp='+str(base/'pytest_tmp'), '--junitxml='+str(base/'pytest_results.xml')]
start = time.monotonic()
with (base/'pytest_full.log').open('w', encoding='utf-8') as stream:
    result = subprocess.run(cmd, cwd=source, env=env, stdout=stream, stderr=subprocess.STDOUT)
summary = {'command': cmd[2:], 'exit_code': result.returncode, 'elapsed_seconds':round(time.monotonic()-start,2), 'snapshot_commit':'2fecd05dea8c5ee438b4071e50548b828b7411af', 'real_env_files_in_snapshot':False, 'external_connections_blocked':True, 'original_working_tree_blocked':True}
(base/'test_run_summary.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
print(json.dumps(summary, indent=2))
print((base/'pytest_full.log').read_text(encoding='utf-8')[-18000:])
raise SystemExit(result.returncode)
