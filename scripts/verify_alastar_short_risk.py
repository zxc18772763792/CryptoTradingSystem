"""Independent stdlib recomputation from SHA-checked raw public kline pages."""
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT/'reports/alastar_short_risk_20261022'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run():
    receipt = json.loads((OUT/'run_receipt.json').read_text(encoding='utf-8'))
    assert sha(OUT/'summary.json') == receipt['summary_sha256']
    assert sha(OUT/'paths.json') == receipt['paths_sha256']
    bars = {}
    for name, proof in receipt['market_sources'].items():
        path = ROOT/name
        assert sha(path) == proof['sha256']
        page = json.loads(path.read_text(encoding='utf-8'))
        assert page['status'] == 'ok' and page['params']['interval'] == '5m'
        symbol = page['params']['symbol']
        dst = bars.setdefault(symbol, {})
        for raw in page['rows']:
            stamp, values = int(raw[0]), tuple(map(float, raw[1:6]))
            if stamp in dst:
                assert values == dst[stamp], (symbol, stamp, 'conflicting duplicate')
            dst[stamp] = values
    paths = json.loads((OUT/'paths.json').read_text(encoding='utf-8'))
    thresholds = {'10pct': .1, '20pct': .2, '33pct': 1/3, '50pct': .5, '99pct': .99}
    maximum_error = 0.
    for row in paths:
        assert row['status'] == 'complete', 'unexpected missingness needs explicit verification'
        at = int(datetime.fromisoformat(row['entry_at']).timestamp()*1000)
        end = at + row['hours']*3600000
        source = bars[row['symbol']]
        observations = []
        for stamp in range(at, end, 300000):
            o, h, l, c, v = source[stamp]
            assert all(math.isfinite(x) and x > 0 for x in (o,h,l,c,v))
            assert l <= min(o,c) <= max(o,c) <= h
            observations.append((stamp,h))
        exit_open, *_, exit_volume = source[end]
        assert math.isfinite(exit_open) and exit_open > 0 and math.isfinite(exit_volume) and exit_volume > 0
        observations.append((end, exit_open))
        entry = source[at][0]
        adverse = max(0., max(x[1] for x in observations)/entry-1)
        net = 1-exit_open/entry-.002
        maximum_error = max(maximum_error, abs(adverse-row['maximum_adverse_rise']),
                            abs(net-row['terminal_net_without_forced_exit']))
        assert maximum_error <= 1e-12
        assert row['entry_price'] == entry and row['exit_price'] == exit_open
        for key, level in thresholds.items():
            hits = [stamp for stamp, high in observations if high >= entry*(1+level)]
            assert bool(hits) == row['crossed_'+key]
            assert bool(hits and hits[0] == end) == row['first_cross_at_exit_'+key]
            if hits:
                actual = int(datetime.fromisoformat(row['first_cross_'+key]).timestamp()*1000)
                assert actual == hits[0]
    cells = json.loads((OUT/'summary.json').read_text(encoding='utf-8'))['cells']
    for cell in cells:
        group = [r for r in paths if r['cohort'] == cell['cohort'] and r['hours'] == cell['hours']]
        assert len(group) == cell['full_path_n'] == cell['selected_short_n']
        assert cell['missing_path_n'] == 0
        assert max(r['maximum_adverse_rise'] for r in group) == cell['mae_max']
        for key in thresholds:
            assert sum(r['crossed_'+key] for r in group) == cell['crossed_'+key+'_n']
            assert sum(r['crossed_'+key] and r['terminal_net_without_forced_exit'] > 0 for r in group) == cell['terminal_profit_after_'+key+'_n']
    unchanged = all(sha(ROOT/p) == v for p,v in receipt['protected_before'].items())
    assert unchanged
    result = {'verified_at_utc': datetime.now(timezone.utc).isoformat(), 'method': 'independent stdlib raw-page loops; no production path/market helpers',
              'raw_pages': len(receipt['market_sources']), 'paths_checked': len(paths), 'cells_checked': len(cells),
              'maximum_absolute_error': maximum_error, 'protected_unchanged': unchanged,
              'verifier_sha256': sha(Path(__file__)), 'scope': 'trade-price stress diagnostics only; no mark-price liquidation claim'}
    (OUT/'verification.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(result,ensure_ascii=True))


if __name__ == '__main__':
    run()
