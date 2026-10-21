"""Economic units and read-only mark conversion at the tracker/UI boundary."""
from __future__ import annotations

import copy
import json

import pytest

from core.research import tracker_registry as registry
from core.research import unlock_short_tracker as unlock
from core.research import xs_reversal_tracker as reversal


def spec_for(tracker_id, loader=None):
    spec = dict(next(spec for spec in registry.TRACKERS if spec['id'] == tracker_id))
    if loader is not None:
        spec['load'] = loader
    return spec


def ledger_summary(mean=4., n=2):
    return {'retirement': {'n': n, 'min_n': 30, 'mean_pct': mean, 'ci90_pct': [-1., 9.],
                           'verdict': 'collecting', 'label': '积累样本', 'reason': '不足', 'clusters': 1},
            'forward_open': 1, 'updated_at': '2026-10-20T05:00:00+00:00'}


@pytest.mark.parametrize(('inverse', 'linear'), [(60., 37.5), (59.63, 37.36), (0., 0.), (-50., -100.), (-80., -400.), ('60', 37.5)])
def test_legacy_inverse_marks_convert_to_linear_short_price_pnl(inverse, linear):
    record = {'mark_return_pct': inverse}
    before = copy.deepcopy(record)
    assert unlock.linear_open_mark(record) == linear
    assert record == before


@pytest.mark.parametrize('mark', [37.36, 0., -100., -150.])
def test_version_two_linear_marks_are_idempotent_even_for_losses_beyond_one_notional(mark):
    record = {'mark_return_pct': mark, 'mark_formula_version': 2}
    assert unlock.linear_open_mark(record) == mark
    normalized = {**record, 'mark_return_pct': unlock.linear_open_mark(record)}
    assert unlock.linear_open_mark(normalized) == mark


@pytest.mark.parametrize('value', [None, 'bad', float('nan'), float('inf'), -float('inf'), [], {}, -100., -110.],
                         ids=['missing', 'text', 'nan', 'positive_inf', 'negative_inf', 'list', 'dict', 'inverse_minus100', 'inverse_below_minus100'])
def test_invalid_legacy_marks_are_unavailable(value):
    assert unlock.linear_open_mark({'mark_return_pct': value}) is None


@pytest.mark.parametrize('value', [True, False, 10**1000], ids=['true_is_not_price_pnl', 'false_is_not_price_pnl', 'numeric_overflow'])
def test_malformed_numeric_marks_never_become_returns_or_raise(value):
    assert unlock.linear_open_mark({'mark_return_pct': value}) is None


@pytest.mark.parametrize('value', [float('nan'), float('inf'), 'bad', None])
def test_invalid_version_two_marks_are_unavailable(value):
    assert unlock.linear_open_mark({'mark_return_pct': value, 'mark_formula_version': 2}) is None


def test_load_state_normalizes_only_open_read_view_without_rewriting_source_or_realized_results(tmp_path):
    path = tmp_path/'tracker.json'
    source = {'started_at': '2026-10-01T00:00:00+00:00', 'trades': {
        'legacy': {'status': 'open', 'mark_return_pct': 59.63, 'funding_pct': -5., 'hedged_return_pct': 3.},
        'linear': {'status': 'open', 'mark_return_pct': 37.36, 'mark_formula_version': 2},
        'invalid': {'status': 'open', 'mark_return_pct': -100.},
        'closed': {'status': 'closed', 'mark_return_pct': 59.63, 'short_return_pct': 21., 'hedged_return_pct': 7.},
        'stopped': {'status': 'stopped', 'mark_return_pct': -50., 'short_return_pct': -43., 'hedged_return_pct': -45.},
    }}
    path.write_text(json.dumps(source, indent=3), encoding='utf8')
    original_bytes, original_time = path.read_bytes(), path.stat().st_mtime_ns
    original_entries = set(tmp_path.iterdir())
    first, second = unlock.load_state(path), unlock.load_state(path)
    assert first == second
    assert first['trades']['legacy']['mark_return_pct'] == 37.36
    assert first['trades']['legacy']['mark_formula_version'] == 2
    assert unlock.linear_open_mark(first['trades']['legacy']) == 37.36
    assert first['trades']['legacy']['funding_pct'] == -5.
    assert first['trades']['legacy']['hedged_return_pct'] == 3.
    for key in ('linear', 'invalid', 'closed', 'stopped'):
        assert first['trades'][key] == source['trades'][key]
    assert path.read_bytes() == original_bytes
    assert path.stat().st_mtime_ns == original_time
    assert set(tmp_path.iterdir()) == original_entries


@pytest.mark.parametrize(('tracker_id', 'multiplier', 'basis', 'kind'), [
    ('xs_reversal', 1, 'total_gross_notional', 'net_return'),
    ('xs_reversal_liq_filter', 1, 'total_gross_notional', 'paired_increment'),
    ('coinglass_composite_7d', 2, 'single_leg_notional', 'net_return'),
    ('coinglass_composite_7d_wide_eq', 2, 'single_leg_notional', 'net_return'),
    ('unlock_short', 2, 'single_leg_notional', 'net_return'),
    ('unlock_short_t7', 2, 'single_leg_notional', 'net_return'),
    ('supply_factor', 2, 'single_leg_notional', 'net_return'),
    ('korea_caution_hedged', 2, 'single_leg_notional', 'net_return'),
    ('upbit_caution_hedged', 2, 'single_leg_notional', 'net_return'),
    ('bithumb_caution_hedged', 2, 'single_leg_notional', 'net_return'),
    ('upbit_caution', 1, 'single_leg_notional', 'net_return'),
    ('upbit_krw_listing', 1, 'single_leg_notional', 'net_return'),
    ('bithumb_caution', 1, 'single_leg_notional', 'net_return'),
    ('announcement_short_monitor', 1, 'single_leg_notional', 'net_return'),
    ('announcement_short_delist', 1, 'single_leg_notional', 'net_return'),
    ('listing_short', 1, 'single_leg_notional', 'net_return'),
])
def test_every_tracker_declares_its_economic_denominator(tracker_id, multiplier, basis, kind):
    measurement = registry.measurement(spec_for(tracker_id))
    assert measurement['gross_notional_multiplier'] == multiplier
    assert measurement['return_basis'] == basis
    assert measurement['metric_kind'] == kind
    assert measurement['is_paper'] is True
    if kind == 'paired_increment':
        assert measurement['metric_unit_label'] == '百分点/天'


@pytest.mark.parametrize(('tracker_id', 'mean', 'expected_gross'), [
    ('coinglass_composite_7d', 6.754, 3.377), ('unlock_short_t7', -6.248, -3.124),
    ('xs_reversal', .182, .182), ('xs_reversal_liq_filter', .7, .7),
    ('upbit_krw_listing', 4.773, 4.773),
])
def test_gross_conversion_does_not_rewrite_ledger_return_or_sample_count(monkeypatch, tracker_id, mean, expected_gross):
    monkeypatch.setattr(registry, '_open_marks', lambda spec: [])
    row = registry.tracker_row(spec_for(tracker_id, lambda: ledger_summary(mean=mean)))
    assert row['mean_pct'] == mean
    assert row['n'] == 2
    assert row['total_pct'] == round(2*mean, 2)
    assert row['gross_mean_pct'] == expected_gross
    assert row['ci90_pct'] == [-1., 9.]
    assert row['ci_clusters'] == 1


def test_filter_row_keeps_actual_return_and_common_period_baseline_separate_from_paired_gain(monkeypatch):
    state = {'started_at': '2026-10-01T00:00:00+00:00', 'days': {
        '2026-10-01': {'status': 'closed', 'late': False, 'net_per_position_pct': 10.},
        '2026-10-02': {'status': 'closed', 'late': False, 'net_per_position_pct': .4,
                       'liq_filter': {'status': 'closed', 'net_per_position_pct': 1.2, 'diff_pct': .8}},
        '2026-10-03': {'status': 'closed', 'late': False, 'net_per_position_pct': -.3,
                       'liq_filter': {'status': 'closed', 'net_per_position_pct': .1, 'diff_pct': .4}},
    }}
    # No position-level data is needed to distinguish ledger return and increment.
    # The paired evaluator must explicitly mark attribution/quotes unavailable.
    monkeypatch.setattr(reversal, 'load_state', lambda: state)
    monkeypatch.setattr(registry, '_open_marks', lambda spec: [])
    filtered = registry._xs_liq_filter()
    assert filtered['actual_mean_pct'] == .65
    assert filtered['retirement']['mean_pct'] == .6
    paired = filtered['paired_evaluation']
    assert paired['n_pairs'] == 2
    assert paired['common_dates'] == ['2026-10-02', '2026-10-03']
    assert paired['baseline']['mean_pct'] == pytest.approx(.05)
    assert paired['shadow']['mean_pct'] == pytest.approx(.65)
    assert paired['increment']['mean_pp'] == pytest.approx(.6)
    row = registry.tracker_row(spec_for('xs_reversal_liq_filter'))
    assert row['mean_pct'] == row['gross_mean_pct'] == .6
    assert row['actual_mean_pct'] == .65
    assert row['metric_kind'] == 'paired_increment'
    assert row['paired_evaluation']['baseline']['n'] == 2
    assert row['n'] == 2


def test_unlock_open_marks_are_partial_linear_price_marks_and_exclude_late_or_closed(monkeypatch):
    state = {'trades': {
        'a': {'token': 'APR', 'status': 'open', 'late': False, 'mark_return_pct': 59.63},
        'b': {'token': 'CLOSED', 'status': 'closed', 'mark_return_pct': 59.63},
        'c': {'token': 'LATE', 'status': 'open', 'late': True, 'mark_return_pct': 59.63},
    }}
    before = copy.deepcopy(state)
    monkeypatch.setattr(unlock, 'load_state', lambda path: state)
    marks = registry._open_marks(spec_for('unlock_short_t7'))
    assert len(marks) == 1
    mark = marks[0]
    assert mark['label'] == 'APR'
    assert mark['mark_pct'] == mark['price_return_pct'] == 37.36
    assert mark['scope'] == 'short_price_only'
    assert mark['is_partial'] is True
    assert mark['strategy_net_pct'] is None
    assert mark['funding_pct'] is mark['cost_pct'] is mark['price_at'] is None
    assert state == before


@pytest.mark.parametrize(('tracker_id', 'partial'), [('bithumb_caution_hedged', True), ('bithumb_caution', False)])
def test_korean_open_marks_separate_short_price_funding_and_unavailable_hedge(monkeypatch, tracker_id, partial):
    from core.research import upbit_caution_tracker as korean
    trade = {'ticker': 'XAI', 'symbol': 'XAIUSDT', 'status': 'open', 'return_pct': 5., 'funding': -.01,
             'updated_at': '2026-10-20T05:00:00+00:00'}
    monkeypatch.setattr(korean, 'load_state', lambda path: {'trades': {'x': trade}})
    mark = registry._open_marks(spec_for(tracker_id))[0]
    assert mark['mark_pct'] == 5.
    assert mark['funding_pct'] == -1.
    assert mark['cost_pct'] == pytest.approx(.1)
    assert mark['price_return_pct'] == pytest.approx(6.1)
    assert mark['is_partial'] is partial
    assert mark['strategy_net_pct'] == (None if partial else 5.)
    assert mark['price_at'] is None  # updated_at cannot silently stand in for the price timestamp
    assert mark['funding_until'] == trade['updated_at']


def test_optional_mark_failure_keeps_the_ledger_row(monkeypatch):
    def unavailable(spec):
        raise OSError('optional quote source missing')
    monkeypatch.setattr(registry, '_open_marks', unavailable)
    row = registry.tracker_row(spec_for('unlock_short_t7', lambda: ledger_summary()))
    assert row['n'] == 2
    assert row['mean_pct'] == 4.
    assert row['gross_mean_pct'] == 2.
    assert row['open_marks'] == []
    assert 'error' not in row


def test_failed_optional_attribution_does_not_blank_reversal_or_event_risk_diagnostics(monkeypatch):
    from core.research import coinglass_attribution
    def unavailable():
        raise TimeoutError('optional source timed out')
    monkeypatch.setattr(coinglass_attribution, 'load_attribution', unavailable)
    paired = {'n_pairs': 8, 'status': 'insufficient_data'}
    risk = {'status': 'collecting', 'n': 1}
    rows = [{'id': 'xs_reversal', 'name': '反转', 'paired_evaluation': paired},
            {'id': 'upbit_krw_listing', 'name': '上币', 'risk_shadow': risk}]
    before = copy.deepcopy(rows)
    diagnostics = registry.diagnostics(rows)
    assert diagnostics['reversal'] == paired
    assert diagnostics['event_risk'] == [{'id': 'upbit_krw_listing', 'name': '上币', **risk}]
    assert diagnostics['coinglass']['status'] == 'unavailable'
    assert 'TimeoutError' in diagnostics['coinglass']['error']
    assert rows == before


def test_one_unavailable_primary_tracker_keeps_measurement_and_other_rows(monkeypatch):
    def unavailable():
        raise FileNotFoundError('state missing')
    monkeypatch.setattr(registry, '_open_marks', lambda spec: [])
    monkeypatch.setattr(registry, 'TRACKERS', [spec_for('unlock_short', unavailable),
                                             spec_for('xs_reversal', lambda: ledger_summary())])
    rows = registry.tracker_rows()
    assert len(rows) == 2
    assert rows[0]['id'] == 'unlock_short'
    assert rows[0]['gross_notional_multiplier'] == 2
    assert 'FileNotFoundError' in rows[0]['error']
    assert rows[1]['mean_pct'] == 4.
    assert rows[1]['gross_notional_multiplier'] == 1
