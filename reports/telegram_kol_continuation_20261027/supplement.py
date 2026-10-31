"""At most two official public requests for the matured CHIP follow-up trade."""
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from core.research import next10xgem1_fade_tracker as provider
from scripts import research_telegram_new_kols as study
from scripts.review_telegram_signals import SymbolPanel, dedup_independent, entry_hour

target = Path(json.loads((OUT / 'followup_location.json').read_text())['study'])
sealed = json.loads((target / 'events_before_prices.json').read_text(encoding='utf-8'))
signals = [row for row in study.signal_rows(sealed) if row['group'] == 'fresh_call' and not row['late_edit'] and row['venue'] == 'futures']
now = pd.Timestamp.now(tz='UTC')
chip = next(row for row in signals if row['symbol'] == 'CHIP')
at = entry_hour(chip['available_at'])
end = at + pd.Timedelta(hours=24)
assert end <= now
market = provider.PublicMarketData(target / 'matured_trade_supplement', max_calls=2)
from trusted_transport import PlatformTrustAdapter
market.session.mount('https://', PlatformTrustAdapter())
evidence, failure = {}, None
try:
    prices, evidence['price'] = market.klines('CHIPUSDT', '1h', provider.ms(at), provider.ms(end))
    rates, evidence['funding'] = market.funding_since('CHIPUSDT', provider.ms(at))
    assert evidence['funding']['complete_coverage']
except Exception as exc:
    failure = type(exc).__name__ + ': ' + str(exc)
finally:
    market.session.close()
record = {'captured_at_utc': datetime.now(timezone.utc).isoformat(), 'calls': market.calls,
          'failure': failure, 'evidence': evidence, 'historical_store_modified': False}
(target / 'supplement_receipt_platform_trust.json').write_text(json.dumps(record, indent=2), encoding='utf-8')
if failure:
    print(json.dumps(record, indent=2))
    raise SystemExit(1)

panels = {}
for symbol, venue in sorted({(row['symbol'], row['venue']) for row in signals}):
    bars, funding = study.load_bars(symbol, venue)
    if symbol == 'CHIP':
        official = pd.DataFrame([{'at': pd.to_datetime(row[0], unit='ms', utc=True),
            'open': float(row[1]), 'high': float(row[2]), 'low': float(row[3]),
            'close': float(row[4]), 'volume': float(row[5])} for row in prices]).set_index('at')
        required = pd.date_range(at, end, freq='h')
        assert official.index.is_unique and required.isin(official.index).all()
        official_rates = pd.DataFrame([{'at': pd.to_datetime(row['fundingTime'], unit='ms', utc=True),
            'funding_rate': float(row['fundingRate'])} for row in rates]).set_index('at')
        assert official_rates.index.is_unique
        bars = pd.concat([bars, official])
        bars = bars[~bars.index.duplicated(keep='last')].sort_index()
        funding = pd.concat([funding, official_rates]) if funding is not None else official_rates
        funding = funding[~funding.index.duplicated(keep='last')].sort_index()
    panels[symbol, venue] = SymbolPanel(bars, funding)
evaluated, status = study.evaluate_rows(signals, panels, horizons=(24,))
rows, cells = [], []
for slug in study.SLUGS:
    done = dedup_independent([row for row in evaluated if row['channel'] == slug])
    rows.extend(done)
    count = sum(row['channel'] == slug for row in signals)
    cells.append({'channel': slug, 'eligible_calls': count, 'n_independent': len(done),
        'awaiting_or_unavailable': count-len(done), 'verdict': 'insufficient_sample',
        'mean_net_20bps': float(np.mean([row['net'] for row in done])) if done else None})

# Independent raw-series arithmetic for both completed trades, without panel.outcomes.
for row in rows:
    start = pd.Timestamp(row['entry_at'])
    finish = start + pd.Timedelta(hours=24)
    if row['symbol'] == 'CHIP':
        raw, raw_funding = official, official_rates
    else:
        raw, raw_funding = study.load_bars(row['symbol'], row['venue'])
    path = raw.reindex(pd.date_range(start, finish, freq='h'))
    assert np.isfinite(path[['open','high','low','close']].to_numpy(float)).all()
    gross = row['side'] * (path['open'].iloc[-1]/path['open'].iloc[0]-1.)
    held_rates = raw_funding[(raw_funding.index.floor('h') >= start) & (raw_funding.index.floor('h') < finish)]['funding_rate']
    charged = -row['side'] * held_rates.sum()
    assert np.isfinite(held_rates).all()
    assert np.isclose(row['gross'], gross, atol=1e-12)
    assert np.isclose(row['funding'], charged, atol=1e-12)
    assert np.isclose(row['net'], max(-1., gross-.002+charged), atol=1e-12)
result = {'evaluated_at_utc': datetime.now(timezone.utc).isoformat(), 'mode': 'retrospective_followup_not_forward',
    'cells': cells, 'trades': [{key:value for key,value in row.items() if key != 'null_values'} for row in rows],
    'status': {'|'.join(map(str,key)): value for key,value in status.items()},
    'supplement': record, 'independent_raw_trade_arithmetic_matches': True,
    'future_exit_calls': [{'symbol': row['symbol'], 'planned_exit': str(entry_hour(row['available_at'])+pd.Timedelta(hours=24))}
        for row in signals if entry_hour(row['available_at'])+pd.Timedelta(hours=24)>now]}
with (target / 'followup_final_results.json').open('x', encoding='utf-8') as stream:
    json.dump(result, stream, ensure_ascii=False, indent=2, default=str)
(OUT / 'followup_final_results.json').write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
print(json.dumps({'cells': cells, 'matured_trades': [{'symbol':row['symbol'], 'entry_at':str(row['entry_at']), 'net':row['net']} for row in rows],
    'public_requests': market.calls, 'independent_arithmetic_matches': True, 'future_exit_calls': result['future_exit_calls']}, indent=2))
