"""Independent arithmetic checks of the sealed 24h sample against raw local bars."""
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
receipt = json.loads((OUT / 'rerun_receipt.json').read_text())
study, output = Path(receipt['study']), Path(receipt['output'])
results = json.loads((output / 'results.json').read_text())
sealed = json.loads((study / 'events_before_prices.json').read_text())
assert hashlib.sha256((study / 'events_before_prices.json').read_bytes()).hexdigest() == results['events_sha256']
trades = pd.read_csv(output / 'trades.csv')
trades = trades[(trades['hours'] == 24) & (trades['group'] == 'fresh_call') & ~trades['late_edit']]
kept, open_until = [], {}
for row in sorted(trades.to_dict('records'), key=lambda r: (r['channel'], pd.Timestamp(r['entry_at']), r['signal_id'])):
    key = row['channel'], row['symbol']
    at = pd.Timestamp(row['entry_at'])
    if key in open_until and at < open_until[key]:
        continue
    kept.append(row)
    open_until[key] = at + pd.Timedelta(hours=24)
by_channel = defaultdict(list)
for row in kept:
    by_channel[row['channel']].append(row)
for cell in results['cells']:
    if not (cell['sample'] == 'primary' and cell['side'] == 'all' and cell['group'] == 'fresh_call' and cell['hours'] == 24):
        continue
    rows = by_channel[cell['channel']]
    assert len(rows) == cell['n_independent']
    assert np.isclose(np.mean([r['net'] for r in rows]), cell['mean_net_20bps'])
    assert np.isclose(np.mean([r['net']-r['null_mean'] for r in rows]), cell['mean_excess_vs_null'])

# Fixed deterministic sample: up to ten evenly spaced independent trades per channel.
sample = []
for rows in by_channel.values():
    sample.extend(rows[i] for i in np.linspace(0, len(rows)-1, min(10, len(rows)), dtype=int))
cache, funding_gaps = {}, []
for row in sample:
    key = row['symbol'], row['venue']
    if key not in cache:
        folder = ROOT / ('data/historical/binance_futures' if key[1] == 'futures' else 'data/historical/binance') / (key[0] + '_USDT')
        bars = pd.read_parquet(folder / '1h.parquet')
        bars.index = pd.to_datetime(bars.index, utc=True)
        bars = bars[~bars.index.duplicated(keep='last')].sort_index()
        funding = pd.read_parquet(folder / 'funding_rate.parquet') if key[1] == 'futures' else None
        if funding is not None:
            funding.index = pd.to_datetime(funding.index, utc=True)
        cache[key] = bars, funding
    bars, funding = cache[key]
    at, end = pd.Timestamp(row['entry_at']), pd.Timestamp(row['entry_at']) + pd.Timedelta(hours=24)
    source = next(post for post in sealed['channels'][row['channel']]['posts'] if post['message_id'] == row['message_id'])
    assert at == (pd.Timestamp(source['available_at']) + pd.Timedelta(seconds=60)).ceil('h')
    path = bars.reindex(pd.date_range(at, end, freq='h'))
    prices = path[['open', 'high', 'low', 'close']].to_numpy(float)
    assert np.isfinite(prices).all() and (prices > 0).all()
    entry, exit_price = path['open'].iloc[0], path['open'].iloc[-1]
    gross = row['side'] * (exit_price / entry - 1)
    if row['side'] < 0 and path['high'].iloc[:-1].max() >= 2 * entry:
        gross = -1.
    gross = max(-1., gross)
    paid = 0.
    if funding is not None:
        hold = funding[(funding.index.floor('h') >= at) & (funding.index.floor('h') < end)]
        assert np.isfinite(hold['funding_rate']).all()
        paid = -row['side'] * hold['funding_rate'].sum()
        # A gap is a diagnostic for review, never inferred as a zero settlement.
        nearby = funding[(funding.index >= at-pd.Timedelta(hours=8)) & (funding.index <= end+pd.Timedelta(hours=8))]
        if nearby.index.to_series().diff().max() > pd.Timedelta(hours=8, milliseconds=100):
            funding_gaps.append({'symbol': row['symbol'], 'entry_at': row['entry_at']})
    assert np.isclose(row['funding'], paid, rtol=1e-10, atol=1e-12)
    assert np.isclose(row['gross'], gross, rtol=1e-10, atol=1e-12)
    assert np.isclose(row['net'], max(-1., gross - .002 + paid), rtol=1e-10, atol=1e-12)

old = json.loads((study / 'results/results.json').read_text())
assert old['events_sha256'] == results['events_sha256']
result = {'primary_independent_trades_reaggregated': len(kept), 'raw_bar_funding_sample_checked': len(sample),
          'sample_funding_gap_diagnostics': funding_gaps, 'sealed_labels_match_original_results': True,
          'entry_after_availability_and_net_arithmetic_match': True,
          'all_reported_primary_aggregates_match': True,
          'latest_results_sha256': hashlib.sha256((output / 'results.json').read_bytes()).hexdigest()}
(OUT / 'independent_verification.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
print(json.dumps(result, indent=2))
