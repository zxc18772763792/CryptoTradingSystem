"""Immutable, bounded retrospective follow-up of the four selected channels."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from scripts import research_telegram_new_kols as study


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def collect():
    from scripts import telegram_desktop_bridge as bridge
    original = json.loads((study.STUDY / 'selection.json').read_text(encoding='utf-8'))
    start = pd.Timestamp(original['cutoff']).to_pydatetime()
    end = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    target = study.STUDY.parent / ('telegram_kol_followup_' + end.strftime('%Y%m%dT%H%M%SZ'))
    bridge.protect_directory(target)
    protocol = {'created_at_utc': datetime.now(timezone.utc).isoformat(), 'start': start.isoformat(),
        'cutoff': end.isoformat(), 'collection': 'retrospective_final_surviving_text_not_forward',
        'rules': 'same kol4-v1; unconditional direct_call/trade_plan; edits later than 10min excluded',
        'primary': '24h after next full hourly open >= final-text availability + 60s',
        'cost': '20bps plus actual funding; 80bps stress; same-coin same-side random-time control',
        'eligibility': 'local Binance data; unverified spot-only inputs withheld separately',
        'small_sample': 'No meaningful effect inference below the original 30 independent trade gate',
        'cap_per_channel': 500, 'implementation_sha256': digest(Path(study.__file__)),
        'original_labels_sha256': digest(study.STUDY / 'events_before_prices.json')}
    study.write_new(target / 'followup_protocol.json', protocol)
    study.write_new(target / 'selection.json', {**original, 'cutoff': end.isoformat(),
        'selected_at_utc': protocol['created_at_utc'], 'rule': 'Reuse exactly the four existing selected channels'})
    study.START, study.CAP = start, 500
    study.collect(target)
    sealed = study.label_study(target)
    study.write_new(target / 'events_before_prices.json', sealed)
    pointer = {'study': str(target), 'created_at_utc': protocol['created_at_utc'],
        'original_study': str(study.STUDY), 'mode': protocol['collection']}
    (OUT / 'followup_location.json').write_text(json.dumps(pointer, indent=2), encoding='utf-8')
    print(json.dumps({'sealed': True, 'rows': {slug: channel['label_counts'] for slug, channel in sealed['channels'].items()}}, indent=2))


def evaluate():
    from scripts.review_telegram_signals import SymbolPanel, dedup_independent, summarise
    pointer = json.loads((OUT / 'followup_location.json').read_text())
    target = Path(pointer['study'])
    sealed_path = target / 'events_before_prices.json'
    sealed = json.loads(sealed_path.read_text(encoding='utf-8'))
    signal_rows = study.signal_rows(sealed)
    signals = [row for row in signal_rows if row['group'] == 'fresh_call' and not row['late_edit']]
    # The incident review shows that a spot folder name alone is not venue evidence.
    # Leave spot-only calls unavailable in this small continuation until certified.
    unavailable_spot = [row['signal_id'] for row in signals if row['venue'] == 'spot']
    signals = [row for row in signals if row['venue'] != 'spot']
    panels, input_sha = {}, {}
    for symbol, venue in sorted({(row['symbol'], row['venue']) for row in signals if row['symbol']}):
        folder = ROOT / 'data/historical/binance_futures' / (symbol + '_USDT')
        for name in ('1h.parquet', 'funding_rate.parquet'):
            path = folder / name
            if path.exists():
                input_sha[str(path.relative_to(ROOT))] = digest(path)
        bars, funding = study.load_bars(symbol, venue)
        panels[symbol, venue] = SymbolPanel(bars, funding)
    evaluated, exclusions = study.evaluate_rows(signals, panels, horizons=(24,))
    rows, cells = [], []
    for slug in study.SLUGS:
        base = [row for row in evaluated if row['channel'] == slug]
        independent = dedup_independent(base)
        rows.extend(independent)
        cell = {'channel': slug, 'eligible_calls': sum(row['channel'] == slug for row in signals),
                'evaluated_calls': len(base), 'n_independent': len(independent),
                'verdict': 'insufficient_sample' if len(independent) < 30 else 'descriptive_only',
                'mean_net_20bps': sum(row['net'] for row in independent)/len(independent) if independent else None}
        if len(independent) >= 2:
            cell.update(summarise(independent, {symbol: panel for (symbol, venue), panel in panels.items()}))
        cells.append(cell)
    assert all(digest(ROOT / name) == value for name, value in input_sha.items()), 'Market inputs changed during evaluation'
    trades = [{key: value for key, value in row.items() if key != 'null_values'} for row in rows]
    coverage = {slug: json.loads((target / (slug + '_history.json')).read_text(encoding='utf-8'))['coverage']
                for slug in study.SLUGS}
    result = {'evaluated_at_utc': datetime.now(timezone.utc).isoformat(), 'mode': pointer['mode'],
        'protocol': json.loads((target / 'followup_protocol.json').read_text()),
        'sealed_labels_sha256': digest(sealed_path), 'cells': cells, 'trades': trades,
        'unverified_spot_call_ids': unavailable_spot, 'history_coverage': coverage,
        'status': {'|'.join(map(str, key)): value for key, value in exclusions.items()},
        'input_sha256': input_sha, 'data_cutoff_by_symbol': {symbol: str(panel.index[-1]) for (symbol, _), panel in panels.items()},
        'paper_activation_or_state_changed': False}
    with (target / 'followup_results.json').open('x', encoding='utf-8') as stream:
        json.dump(result, stream, ensure_ascii=False, indent=2, default=str)
    # Public working record contains no private channel identifiers or raw text.
    public = {key: value for key, value in result.items() if key not in ('history_coverage',)}
    public['history_coverage'] = {slug: {key: value for key, value in meta.items() if key not in ('about',)} for slug, meta in coverage.items()}
    (OUT / 'followup_results.json').write_text(json.dumps(public, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    print(json.dumps({'cells': cells, 'status': result['status'],
        'unverified_spot_calls': len(unavailable_spot), 'data_cutoff_by_symbol': result['data_cutoff_by_symbol']}, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=('collect', 'evaluate'))
    args = parser.parse_args()
    collect() if args.action == 'collect' else evaluate()
