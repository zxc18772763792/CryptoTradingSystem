from copy import deepcopy
from dataclasses import replace

import pytest

from core.research.korean_onchain_guard import Config, assess


def safe():
    def observation(value):
        return {'value': value, 'source': 'verified-public-fixture', 'provenance': 'live',
                'asset_chain_id': 'interwoven-1', 'asset_denom': 'uinit',
                'available_at_utc': '2026-10-27T00:00:00Z', 'event_at_utc': '2026-10-26T23:59:00Z'}
    return {'chain_tip': observation({'chain_id': 'interwoven-1', 'denom': 'uinit', 'height': 42}),
            'transfer_status': observation('open')}


def test_fresh_baseline_pass_is_not_an_order_or_paper_fill():
    result = assess(safe(), '2026-10-27T00:00:30Z')
    assert result['action'] == 'pass_to_baseline'
    assert result['creates_trade'] is False and result['paper_only'] is True


def test_deposit_only_suspension_does_not_remove_every_caution_event():
    features = safe(); features['transfer_status']['value'] = 'deposit_only_suspended'
    assert assess(features, '2026-10-27T00:00:30Z')['allows_baseline_review']
    features['transfer_status']['value'] = 'both_suspended'
    assert assess(features, '2026-10-27T00:00:30Z')['action'] == 'veto_short'


@pytest.mark.parametrize('change', [
    {'available_at_utc': '2026-10-29T00:00:00Z'},
    {'provenance': 'historical_reconstruction'},
    {'available_at_utc': '2026-10-26T20:00:00Z', 'event_at_utc': '2026-10-26T20:00:00Z'},
    {'value': {'chain_id': 'wrong-chain', 'denom': 'uinit'}},
    {'asset_denom': 'wrapped-init'},
])
def test_future_reconstructed_stale_or_wrong_chain_evidence_cannot_open_a_short(change):
    features = safe(); features['chain_tip'].update(change)
    assert assess(features, '2026-10-27T00:00:30Z')['action'] == 'defer'


def test_stale_block_does_not_become_zero_sell_pressure_or_confirmed_global_halt():
    features = safe(); features['chain_tip']['event_at_utc'] = '2026-10-23T15:41:30Z'
    result = assess(features, '2026-10-27T00:00:30Z')
    assert result['action'] == 'defer' and 'stale' in result['reason']
    assert assess({}, '2026-10-27T00:00:30Z')['action'] == 'defer'


def test_old_announcement_retrieved_now_is_not_current_wallet_status():
    features = safe(); features['transfer_status'].update(value='both_suspended', event_at_utc='2026-10-24T00:00:00Z')
    assert assess(features, '2026-10-27T00:00:30Z')['action'] == 'defer'


def test_wrapped_assets_and_labels_learned_after_the_window_do_not_confirm_native_flow():
    features = safe(); config = replace(Config(), require_native_exchange_flow=True)
    assert assess(features, '2026-10-27T00:00:30Z', config)['action'] == 'defer'
    features['exchange_flow'] = deepcopy(features['transfer_status'])
    value = {'chain_id': 'interwoven-1', 'denom': 'uinit', 'wrapped': False,
             'window_start_utc': '2026-10-26T00:00:00Z', 'window_end_utc': '2026-10-26T23:59:00Z',
             'labels_available_at_utc': '2026-10-25T00:00:00Z', 'window_complete': True,
             'entities_deduped': True, 'venues': ['specified-exchange'], 'net_inflow_native': 100}
    features['exchange_flow']['value'] = value
    assert assess(features, '2026-10-27T00:00:30Z', config)['action'] == 'pass_to_baseline'
    value['wrapped'] = True
    assert assess(features, '2026-10-27T00:00:30Z', config)['action'] == 'defer'
    value['wrapped'] = False; value['labels_available_at_utc'] = '2026-10-26T12:00:00Z'
    assert assess(features, '2026-10-27T00:00:30Z', config)['action'] == 'defer'


def test_naive_clock_is_rejected_and_inputs_are_not_rewritten():
    features = safe(); before = deepcopy(features)
    with pytest.raises(ValueError):
        assess(features, '2026-10-27T00:00:30')
    assess(features, '2026-10-27T00:00:30Z')
    assert features == before
