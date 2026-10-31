"""Re-evaluate sealed labels locally; preserve all original outputs and inputs."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from scripts.research_telegram_new_kols import STUDY, evaluate_study


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


names = ['selection.json', 'PROTOCOL.md', 'events_before_prices.json', 'history_collection.json']
names += [path.name for path in STUDY.glob('*_history.json')]
before = {name: digest(STUDY / name) for name in names}
stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
out = STUDY / ('results_accounting_recheck_' + stamp)
out.mkdir(exist_ok=False)
started = time.monotonic()
print('Re-evaluating sealed labels; original results remain untouched.', flush=True)
result = evaluate_study(STUDY, out)
assert before == {name: digest(STUDY / name) for name in names}
receipt = {'at_utc': datetime.now(timezone.utc).isoformat(), 'seconds': round(time.monotonic() - started, 2),
           'study': str(STUDY), 'output': str(out), 'frozen_inputs_sha256': before,
           'frozen_inputs_unchanged': True, 'original_results_preserved': True,
           'accounting_revision': result['accounting_revision'], 'network_used': False}
(out / 'rerun_receipt.json').write_text(json.dumps(receipt, indent=2), encoding='utf-8')
public = Path(__file__).resolve().parent
(public / 'rerun_receipt.json').write_text(json.dumps(receipt, indent=2), encoding='utf-8')
primary = [{k: cell.get(k) for k in ('channel', 'n_independent', 'hit_rate', 'mean_net_20bps',
    'mean_net_80bps', 'mean_excess_vs_null', 'excess_week_bootstrap_95', 'p_greater_than_null', 'bh_q', 'verdict')}
    for cell in result['cells'] if cell['group'] == 'fresh_call' and cell['side'] == 'all'
    and cell['hours'] == 24 and cell['sample'] == 'primary']
(public / 'primary_summary.json').write_text(json.dumps(primary, indent=2), encoding='utf-8')
print(json.dumps({'seconds': receipt['seconds'], 'frozen_inputs_unchanged': True,
                  'primary_24h': primary}, ensure_ascii=True, indent=2), flush=True)
