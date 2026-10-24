import numpy as np
import pandas as pd
import pytest

from scripts.audit_alastar_short_risk import short_path


def flat_frame():
    return pd.DataFrame({'open': 100., 'high': 100., 'low': 100., 'close': 100., 'volume': 1.},
                        index=pd.date_range('2026-01-01', periods=2017, freq='5min', tz='UTC'))


def test_recovery_does_not_erase_adverse_path_or_first_crossing():
    f = flat_frame()
    f.loc[f.index[2], 'high'] = 210.
    f.loc[f.index[12], 'open'] = 70.
    result = short_path(f, f.index[0], 1)
    assert result['maximum_adverse_rise'] == pytest.approx(1.1)
    assert result['terminal_net_without_forced_exit'] == pytest.approx(.298)
    assert result['crossed_99pct']
    assert result['first_cross_99pct'] == str(f.index[2])


def test_exit_future_high_excluded_but_exit_open_is_known_boundary():
    f = flat_frame()
    f.loc[f.index[12], 'high'] = 1e6
    assert short_path(f, f.index[0], 1)['maximum_adverse_rise'] == 0.
    f.loc[f.index[12], 'open'] = 125.
    result = short_path(f, f.index[0], 1)
    assert result['maximum_adverse_rise'] == pytest.approx(.25)
    assert result['first_cross_at_exit_20pct']


@pytest.mark.parametrize('change', ['gap', 'zero_volume', 'bad_geometry', 'nan'])
def test_missing_or_invalid_path_never_becomes_safe(change):
    f = flat_frame()
    f.loc[f.index[1], 'high'] = 220.
    if change == 'gap':
        f = f.drop(f.index[3])
    elif change == 'zero_volume':
        f.loc[f.index[3], 'volume'] = 0.
    elif change == 'bad_geometry':
        f.loc[f.index[3], 'high'] = 99.
    else:
        f.loc[f.index[3], 'low'] = np.nan
    assert short_path(f, f.index[0], 1)['status'] == 'missing_or_invalid_path'


@pytest.mark.parametrize('hours', [1, 4, 24, 72, 168])
def test_risk_path_includes_last_preexit_bar(hours):
    f = flat_frame()
    end = f.index[0] + pd.Timedelta(hours=hours)
    f.loc[end-pd.Timedelta(minutes=5), 'high'] = 150.
    result = short_path(f, f.index[0], hours)
    assert result['crossed_50pct']
    assert result['first_cross_50pct'] == str(end-pd.Timedelta(minutes=5))


def test_unaligned_naive_or_duplicate_entries_are_rejected():
    f = flat_frame()
    for at in [pd.Timestamp('2026-01-01'), f.index[0]+pd.Timedelta(seconds=1)]:
        with pytest.raises(ValueError):
            short_path(f, at, 1)
    with pytest.raises(ValueError, match='duplicate'):
        short_path(pd.concat([f, f.iloc[:1]]), f.index[0], 1)
