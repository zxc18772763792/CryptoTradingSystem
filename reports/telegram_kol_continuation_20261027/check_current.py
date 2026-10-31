"""Read-only continuation checks: paper ledger, sealed venues and current inputs."""
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from core.research import next10xgem1_fade_tracker as fade
from scripts.next10xgem1_fade_tracker import report
from scripts.research_telegram_new_kols import STUDY, signal_rows


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


sealed_path = STUDY / 'events_before_prices.json'
sealed = json.loads(sealed_path.read_text(encoding='utf-8'))
signals = signal_rows(sealed)
profiles = []
spot = {row['symbol'] for row in signals if row['symbol'] and row['venue'] == 'spot'}
known_pollution = ROOT / 'data/research/kline_venue_pollution_20261027/missed_by_repair_perp_days.csv'
pollution_columns = pd.read_csv(known_pollution, nrows=0).columns.tolist() if known_pollution.exists() else []
for symbol, venue in sorted({(row['symbol'], row['venue']) for row in signals if row['symbol']}):
    folder = ROOT / ('data/historical/binance_futures' if venue == 'futures' else 'data/historical/binance') / (symbol + '_USDT')
    path = folder / '1h.parquet'
    raw = pd.read_parquet(path)
    index = pd.to_datetime(raw.index, utc=True)
    profiles.append({'symbol': symbol, 'venue': venue, 'rows': len(raw),
        'duplicates': int(index.duplicated().sum()), 'first': str(index.min()), 'last': str(index.max()),
        'invalid_ohlc': int((~np.isfinite(raw[['open','high','low','close']].to_numpy(float))).any(axis=1).sum()),
        'sha256': sha(path), 'source': str(path.relative_to(ROOT))})

journal = fade.Journal()
pin_paths = [fade.PROTOCOL, Path(fade.__file__), fade.LABELS, sealed_path]
pins = {str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else path.name: sha(path) for path in pin_paths}
verified = journal.verify()
status = fade.summary(journal)
statistics = report(journal)
assert pins == {str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else path.name: sha(path) for path in pin_paths}
receipt = {'as_of_utc': datetime.now(timezone.utc).isoformat(), 'network_used': False,
    'frozen_file_sha256': pins, 'frozen_sources_unchanged': True,
    'signals_by_venue': dict(Counter(row['venue'] for row in signals)), 'spot_symbols': sorted(spot),
    'known_pollution_columns': pollution_columns, 'input_profiles': profiles,
    'paper_journal_verification': verified, 'paper_status': status, 'paper_report': statistics}
(OUT / 'current_evidence.json').write_text(json.dumps(receipt, indent=2, ensure_ascii=False, default=str), encoding='utf-8')
print(json.dumps({'as_of_utc': receipt['as_of_utc'], 'signals_by_venue': receipt['signals_by_venue'],
    'spot_symbols': sorted(spot), 'input_last_bar_counts': dict(Counter(p['last'] for p in profiles)),
    'invalid_ohlc': sum(p['invalid_ohlc'] for p in profiles),
    'duplicate_timestamps': sum(p['duplicates'] for p in profiles),
    'paper_journal_verification': verified, 'paper_status': {k: status.get(k) for k in (
        'tracker_status_label','started_at','updated_at','forward_open','forward_waiting','observation_count',
        'primary_sample_count','skipped_count','unavailable_count','posts_seen_after_activation','last_call','last_error')},
    'paper_report': statistics}, ensure_ascii=True, indent=2, default=str))
