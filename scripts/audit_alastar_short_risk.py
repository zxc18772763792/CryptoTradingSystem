"""Offline short-path stress diagnostics; never an exchange liquidation simulator.

Uses the frozen sixth-round cohort and cached public 5m trade-price bars. No
network, accounts, orders, leverage selection, or changes to the active journals.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from scripts.research_alastar_responses import Market
from scripts.research_alastar_horizons import HORIZONS, save_bytes, write

SOURCE = ROOT / 'data/research/alastar_horizons_20261022/paired_signals_5m.json'
OUT = ROOT / 'reports/alastar_short_risk_20261022'
THRESHOLDS = {'10pct': .10, '20pct': .20, '33pct': 1/3, '50pct': .50, '99pct': .99}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def protected_hashes():
    paths = [
        'core/research/alastar_forward_paper.py',
        'core/research/alastar_paper_tracker.py',
        'core/research/alastar_horizon_tracker.py',
        'scripts/observe_alastar_paper.py',
        'scripts/observe_alastar_horizons.py',
        'data/research/alastar_response_20261021/prospective_paper_protocol_v2.json',
        'data/research/alastar_horizons_20261022/prospective_horizon_protocol_v3.json',
        'data/research/alastar_paper_v2/journal/000000000001.json',
        'data/research/alastar_horizons_v3/journal/000000000001.json',
        'data/research/alastar_horizons_20261022/paired_signals.json',
        'data/research/alastar_horizons_20261022/paired_signals_5m.json',
    ]
    return {p: sha(ROOT/p) for p in paths}


def short_path(frame, start, hours):
    """Full hypothetical unstopped path; exit-bar future high is unavailable.

    Exit open is included as a boundary observation. It cannot resolve the order
    of exit versus liquidation at that instant. A missing bar makes full-path
    MAE unknown even if a prior threshold crossing was already known.
    """
    start = pd.Timestamp(start)
    if start.tzinfo is None or start != start.floor('5min') or hours not in HORIZONS:
        raise ValueError('timezone-aware aligned entry and registered horizon required')
    end = start + pd.Timedelta(hours=hours)
    if not frame.index.is_unique:
        raise ValueError('duplicate price timestamps')
    path = frame.reindex(pd.date_range(start, end, freq='5min'))
    risk = path.iloc[:-1]
    cols = ['open', 'high', 'low', 'close', 'volume']
    good = np.isfinite(risk[cols]).all(axis=1) & (risk[cols] > 0).all(axis=1)
    good &= (risk.high >= risk[['open', 'close', 'low']].max(axis=1))
    good &= (risk.low <= risk[['open', 'close', 'high']].min(axis=1))
    last = path.iloc[-1]
    if not good.all():
        return {'status': 'missing_or_invalid_path', 'first_missing_at': str(good[~good].index[0])}
    if not np.isfinite(last['open']) or last['open'] <= 0 or not np.isfinite(last['volume']) or last['volume'] <= 0:
        return {'status': 'missing_or_invalid_exit', 'first_missing_at': str(end)}
    entry = float(risk.iloc[0]['open'])
    # Include the known exit price, never its still-future high/low/close.
    peaks = pd.concat([risk.high, pd.Series([float(last['open'])], index=[end])])
    peak_at = peaks.idxmax()
    result = {'status': 'complete', 'entry_price': entry, 'exit_price': float(last['open']),
              'maximum_adverse_rise': max(0., float(peaks.max())/entry-1),
              'peak_bar_start_at': str(peak_at), 'peak_at_exit_boundary': bool(peak_at == end),
              'terminal_net_without_forced_exit': 1-float(last['open'])/entry-.002}
    for key, level in THRESHOLDS.items():
        hits = peaks[peaks >= entry*(1+level)]
        result['crossed_'+key] = bool(len(hits))
        result['first_cross_'+key] = str(hits.index[0]) if len(hits) else None
        result['first_cross_at_exit_'+key] = bool(len(hits) and hits.index[0] == end)
    return result


def run():
    before = protected_hashes()
    cohorts = json.loads(SOURCE.read_text(encoding='utf-8'))
    market = Market()
    records, cells = [], []
    for cohort, source_rows in cohorts.items():
        shorts = [r for r in source_rows if r['side'] == -1]
        for row in shorts:
            frame = market.prices_for(row['symbol'])
            for i, hours in enumerate(HORIZONS):
                records.append({'cohort': cohort, 'signal_id': row['signal_id'], 'symbol': row['symbol'],
                                'entry_at': row['entry_at'], 'hours': hours,
                                'frozen_v3_proxy_net': row['net'][i],
                                **short_path(frame, row['entry_at'], hours)})
        for hours in HORIZONS:
            selected = [r for r in records if r['cohort'] == cohort and r['hours'] == hours]
            complete = [r for r in selected if r['status'] == 'complete']
            adverse = [r['maximum_adverse_rise'] for r in complete]
            cell = {'cohort': cohort, 'hours': hours, 'selected_short_n': len(shorts),
                    'full_path_n': len(complete), 'missing_path_n': len(selected)-len(complete),
                    'mae_median': float(np.median(adverse)) if adverse else None,
                    'mae_p90': float(np.quantile(adverse, .9)) if adverse else None,
                    'mae_max': max(adverse) if adverse else None,
                    'frozen_proxy_total_loss_n': sum(r['frozen_v3_proxy_net'] == -1 for r in selected)}
            for key in THRESHOLDS:
                cell['crossed_'+key+'_n'] = sum(r['crossed_'+key] for r in complete)
                cell['terminal_profit_after_'+key+'_n'] = sum(r['crossed_'+key] and r['terminal_net_without_forced_exit'] > 0 for r in complete)
            cells.append(cell)
    after = protected_hashes()
    assert before == after, 'active sources or frozen research drifted during audit'
    write(OUT/'summary.json', {'created_at_utc': datetime.now(timezone.utc).isoformat(),
                             'basis': 'cached public 5m trade-price path; not mark-price liquidation',
                             'thresholds': THRESHOLDS, 'cells': cells})
    write(OUT/'paths.json', records)
    save_bytes(OUT/'SHORT_RISK.csv', pd.DataFrame(cells).to_csv(index=False).encode('utf-8-sig'))
    save_bytes(OUT/'SHORT_PATHS.csv', pd.DataFrame(records).to_csv(index=False).encode('utf-8-sig'))
    write(OUT/'run_receipt.json', {'completed_at_utc': datetime.now(timezone.utc).isoformat(),
                                 'script_sha256': sha(Path(__file__)), 'cohort_sha256': sha(SOURCE),
                                 'market_sources': market.sources, 'protected_before': before,
                                 'protected_after': after, 'protected_unchanged': before == after,
                                 'summary_sha256': sha(OUT/'summary.json'), 'paths_sha256': sha(OUT/'paths.json'),
                                 'limits': ['Historical exploratory cohort; overlapping groups are not independent.',
                                            '5m trade-price highs are not mark prices; crossing is not observed liquidation.',
                                            'No maintenance tiers, funding-equity path, liquidation fees, margin topups or cross-account equity.',
                                            '10/20/33.3/50% are stress levels, not proposed stop losses or safe leverage.',
                                            'Missing full paths excluded from MAE only; missing counts remain explicit.',
                                            'Endpoint open included; endpoint future high excluded; intrabar order unknown.',
                                            'No change to frozen sources, protocols, activations, journals or prior results.']})
    for c in cells:
        print(c['cohort'], c['hours'], 'n', c['full_path_n'], 'mae_max', c['mae_max'],
              'cross20', c['crossed_20pct_n'], 'profit_after20', c['terminal_profit_after_20pct_n'],
              'cross99', c['crossed_99pct_n'], flush=True)
    return cells


if __name__ == '__main__':
    run()
