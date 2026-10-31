"""Reproduce the INIT case study from bounded public receipts; no tracker writes.

Market history is a retrospective reconstruction, not a forward strategy result.
The guard is a new offline proposal and is deliberately not wired to the runner.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict, replace
from datetime import datetime, timezone, timedelta
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from core.research.korean_onchain_guard import Config, assess

ENTRY_MS = 1793059200000
STOP_PATH_MS = 1793186100000


def data(path):
    obj = json.loads(path.read_bytes())
    return obj.get('result', obj) if isinstance(obj, dict) else obj


def iso(ms):
    return datetime.fromtimestamp(ms/1000, timezone.utc).isoformat()


def market_snapshot(folder, cutoff):
    funding = [x for x in data(folder/'funding.json') if int(x['fundingTime']) <= cutoff]
    oi = [x for x in data(folder/'oi_1h.json') if int(x['timestamp']) <= cutoff]
    prices = [x for x in data(folder/'klines_1h.json') if int(x[6])+1 <= cutoff]
    assert funding and len(oi) >= 7 and prices
    now, before = oi[-1], oi[-7]
    return {'cutoff_utc': iso(cutoff), 'last_published_funding_at_utc': iso(int(funding[-1]['fundingTime'])),
            'last_funding_rate': float(funding[-1]['fundingRate']),
            'oi_contract_units': float(now['sumOpenInterest']),
            'oi_6h_change_pct': (float(now['sumOpenInterest'])/float(before['sumOpenInterest'])-1)*100,
            'last_closed_price': float(prices[-1][4]), 'last_bar_end_utc': iso(int(prices[-1][6])+1),
            'scope': 'historical CEX reconstruction; not on-chain or execution replay'}


def run(folder):
    config = Config()
    receipt = json.loads((folder/'rpc_initia.json').read_bytes())
    result = receipt['body']['result']
    sync = result['sync_info']
    chain_id = result['node_info']['network']
    assert chain_id == 'interwoven-1'
    observation = {'provenance': 'live', 'source': receipt['url'],
                   'asset_chain_id': chain_id, 'asset_denom': 'uinit',
                   'available_at_utc': receipt['observed_at_utc'],
                   'event_at_utc': sync['latest_block_time'],
                   'value': {'chain_id': chain_id, 'denom': 'uinit',
                             'height': int(sync['latest_block_height'])}}
    # The current RPC read cannot be passed off as a receipt at the October 27 entry.
    historical = assess({'chain_tip': observation}, iso(ENTRY_MS), config)
    observed = datetime.fromisoformat(receipt['observed_at_utc']) + timedelta(seconds=1)
    live_example = assess({'chain_tip': observation}, observed.isoformat(), config)
    assert historical['action'] == live_example['action'] == 'defer'
    rest = json.loads((folder/'rest_initia_latest.json').read_bytes())
    header = rest['body']['block']['header']
    assert header['height'] == sync['latest_block_height'] and header['time'] == sync['latest_block_time']
    coverage = data(folder/'feature_coverage.json')
    entry = market_snapshot(folder, ENTRY_MS)
    before_stop = market_snapshot(folder, STOP_PATH_MS)
    assert entry['last_funding_rate'] > 0 and before_stop['last_funding_rate'] > 0
    proposal = {'status': 'offline_candidate_not_activated', 'created_at_utc': datetime.now(timezone.utc).isoformat(),
                'development_case': 'INIT October 2026; outcome known before proposal',
                'implementation_sha256': hashlib.sha256((ROOT/'core/research/korean_onchain_guard.py').read_bytes()).hexdigest(),
                'guard_config': asdict(config), 'baseline_entry_and_exit': 'original paper rules; a pass is not an entry',
                'variants': ['safety_veto_only', 'safety_plus_verified_native_inflow'],
                'flow_variant_config': asdict(replace(config, require_native_exchange_flow=True)),
                'unknown_policy': 'defer; never fill missing native-chain evidence with zero or wrapped-token holders',
                'validation_plan': {'activation': 'requires a separate timestamped forward study; not activated by this script',
                                    'evaluation': 'same candidate events and entry opportunities; count rejected and unknown events',
                                    'minimum_review_events': 20, 'clusters': 'same coin and shared incident are one episode',
                                    'comparisons': ['baseline', 'safety veto', 'native-inflow confirmation'],
                                    'outcomes': ['tail loss', 'mean net return', 'profitable events skipped', 'coverage', 'entry latency'],
                                    'sensitivity': 'exclude INIT; no tuning thresholds on the development case',
                                    'limits': '20 episodes are an initial review, not statistical proof or live permission'}}
    # Preserve earlier proposals; every proposal gets its own content hash filename.
    comparable = {k: v for k, v in proposal.items() if k != 'created_at_utc'}
    for existing in sorted((folder/'proposals').glob('*.json')):
        previous = json.loads(existing.read_bytes())
        if {k: v for k, v in previous.items() if k != 'created_at_utc'} == comparable:
            proposal = previous
            break
    raw = json.dumps(proposal, ensure_ascii=False, sort_keys=True, indent=2).encode('utf-8')
    digest = hashlib.sha256(raw).hexdigest()
    path = folder/'proposals'/f'{digest}.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_bytes(raw)
    summary = {'research_at_utc': datetime.now(timezone.utc).isoformat(), 'proposal_file': str(path),
               'rpc_tip': {'height': sync['latest_block_height'], 'time_utc': sync['latest_block_time'],
                           'rpc_rest_match': True, 'independent_rpc_provider_status': data(folder/'rpc_lavenderfive.json').get('http'),
                           'qualification': 'two interfaces of one operator; not independent global-halt proof'},
               'entry_market_context': entry, 'before_stop_market_context': before_stop,
               'historical_guard_result': historical, 'guard_at_actual_probe_receipt': live_example,
               'feature_coverage': {'events': len(coverage),
                                    'holder_rows': sum(x['holder_record'] is not None for x in coverage),
                                    'INIT_holder_record': next(x['holder_record'] for x in coverage if x['ticker']=='INIT'),
                                    'remaining_row_scope': 'FET BSC contract only; not a unified native/global supply measure'},
               'performance': None, 'performance_reason': 'No point-in-time on-chain receipt coverage for a valid backtest',
               'historical_return_avoided_claim_allowed': False,
               'source_sha256': {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in folder.glob('*.json')
                                 if p.name != 'analysis_summary.json'}}
    (folder/'analysis_summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({k: summary[k] for k in ('rpc_tip', 'entry_market_context', 'before_stop_market_context',
                                             'historical_guard_result', 'feature_coverage', 'performance')}, ensure_ascii=True))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-dir', type=Path, default=ROOT/'data/research/init_onchain_guard_20261029')
    run(parser.parse_args().input_dir)
