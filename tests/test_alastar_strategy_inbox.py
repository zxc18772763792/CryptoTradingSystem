import asyncio
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json

import pytest

from core.news.alastar_observation import observe_records
from core.research import alastar_strategy_inbox as inbox


@pytest.fixture
def setup(tmp_path, monkeypatch):
    clock = [datetime(2026, 10, 28, 14, 0, tzinfo=timezone.utc)]
    monkeypatch.setattr(inbox, 'now', lambda: clock[0])
    sources = {ch: tmp_path/ch for ch in inbox.CHANNELS}
    output = tmp_path/'inbox'
    calls = []
    result = {'classification': 'conditional', 'summary': 'AR breakout setup', 'plans': [
        {'asset': 'AR', 'direction': 'long', 'thesis_quote': 'AR buy after breakout',
         'condition_quote': 'after breakout', 'risk_quote': 'invalid below 20'}]}

    async def model(candidate):
        calls.append(candidate['source_id'])
        return deepcopy(result), 'test-model'

    observer = inbox.Observer(output, sources, model)

    def add(channel, number=1, published=None, text='AR buy after breakout; invalid below 20'):
        record = {'source_id': f'telegram:{channel}:{number}', 'content': text,
                  'published_at': (published or clock[0]-timedelta(minutes=1)).isoformat(),
                  'payload': {'message_id': number, 'channel': channel,
                              **({'media_only': True, 'has_photo': True} if not text else {})}}
        observe_records([record], sources[channel], channel=channel, mode='live_public_preview',
                        observed_at=clock[0], raw_preview=b'bounded public evidence')

    return observer, clock, calls, result, add


def test_same_message_id_in_two_channels_never_merges_and_old_posts_stay_historical(setup):
    observer, _, calls, _, add = setup
    for ch in inbox.CHANNELS:
        add(ch)
    state = asyncio.run(observer.tick())
    assert set(calls) == {'telegram:alastartrades:1', 'telegram:alastartrader1:1'}
    assert len(state['items']) == 2
    assert {x['provenance'] for x in state['items'].values()} == {'historical_review'}
    summary = inbox.detail(observer.output)
    assert summary['strategy_posts'] == 1 and summary['creates_trades'] is False
    before = sorted((observer.output/'evidence').glob('*.json'))
    asyncio.run(observer.tick())
    assert len(calls) == 2 and sorted((observer.output/'evidence').glob('*.json')) == before


def test_post_after_enabled_time_retains_actual_first_receipt_without_synthetic_fills(setup):
    observer, clock, _, _, add = setup
    asyncio.run(observer.tick())
    clock[0] += timedelta(minutes=2)
    add('alastartrader1')
    state = asyncio.run(observer.tick())
    item, = state['items'].values()
    assert item['provenance'] == 'prospective_receipt'
    assert item['first_received_at_utc'] == clock[0].isoformat()
    assert 'entry_at' not in item and 'return_pct' not in item


@pytest.mark.parametrize('change', [
    {'asset': 'ETH'}, {'thesis_quote': 'fabricated buy'}, {'condition_quote': ''},
    {'risk_quote': 'invented stop'}, {'direction': 'execute_order'},
])
def test_invalid_asset_or_quote_cannot_be_saved_as_reviewed(setup, change):
    observer, _, _, result, add = setup
    add('alastartrader1')
    result['plans'][0].update(change)
    state = asyncio.run(observer.tick())
    item, = state['items'].values()
    assert item['status'] == 'pending' and item['error_type'] == 'ValueError'
    assert not list((observer.output/'evidence').glob('*.json'))


def test_failed_model_has_a_cooldown_and_visible_bounded_failure(setup):
    observer, clock, calls, result, add = setup
    add('alastartrader1')
    result['plans'][0]['thesis_quote'] = 'fabricated'
    for _ in range(3):
        asyncio.run(observer.tick())
    assert len(calls) == 1
    for _ in range(2):
        clock[0] += timedelta(seconds=61)
        asyncio.run(observer.tick())
    item, = inbox.read_state(observer.output)['items'].values()
    assert item['status'] == 'review_failed' and len(calls) == 3
    assert inbox.detail(observer.output)['review_failed'] == 1
    asyncio.run(observer.tick())
    assert len(calls) == 3


def test_directional_view_can_be_retained_beside_alternative_levels():
    text = 'JCT: if 0.009 breaks, new buyers. The next move looks lower.'
    result = {'classification': 'fresh_call', 'summary': 'Current bearish thesis', 'plans': [
        {'asset': 'JCT', 'direction': 'short', 'thesis_quote': 'The next move looks lower.',
         'condition_quote': '', 'risk_quote': ''}]}
    assert inbox.validate(result, text)['classification'] == 'fresh_call'
    result['plans'][0]['condition_quote'] = 'if 0.009 breaks'
    with pytest.raises(ValueError):
        inbox.validate(result, text)


def test_impersonator_stays_visible_but_never_counts_as_alastar(setup):
    observer, _, _, _, add = setup
    add('alastartrades')
    add('alastartrader1', number=2)
    add('alastartrader1', number=3, text='')
    asyncio.run(observer.tick())
    summary = inbox.detail(observer.output)
    assert summary['total_posts'] == 1 and summary['strategy_posts'] == 1
    assert summary['impersonator_posts'] == 2 and summary['impersonator_strategy_posts'] == 1
    assert summary['classification_counts'] == {'conditional': 1}
    fake, real = summary['sources']['alastartrader1'], summary['sources']['alastartrades']
    assert fake['impersonator'] is True and '@alastartrader（无 1）' in fake['note'] and fake['needs_review'] == 1
    assert real['impersonator'] is False and real['note'] is None and real['needs_review'] == 0
    assert {x['source_id']: x['impersonator'] for x in summary['items']} == {
        'telegram:alastartrades:1': False, 'telegram:alastartrader1:2': True, 'telegram:alastartrader1:3': True}
    # The label is applied on read; the stored receipts are not rewritten.
    assert all('impersonator' not in x for x in inbox.read_state(observer.output)['items'].values())


def test_empty_or_invalid_archive_is_visible_without_erasing_other_source(setup):
    observer, _, _, _, add = setup
    add('alastartrades')
    state = asyncio.run(observer.tick())
    assert state['sources']['alastartrader1']['error'] == 'FileNotFoundError'
    assert len(state['items']) == 1


def test_non_strategy_content_cannot_gain_plans(setup):
    _, _, _, result, _ = setup
    result['classification'] = 'promotion'
    with pytest.raises(ValueError):
        inbox.validate(result, 'AR buy after breakout; invalid below 20')


def test_new_media_receipt_is_not_historical_or_text_trade_and_correction_is_idempotent(setup):
    observer, clock, calls, _, add = setup
    asyncio.run(observer.tick())
    clock[0] += timedelta(minutes=2)
    add('alastartrader1', text='')
    state = asyncio.run(observer.tick())
    item, = state['items'].values()
    assert item['provenance'] == 'prospective_receipt'
    assert item['status'] == 'needs_media_review' and calls == []
    assert item['paper_eligible_at_first_observation'] is False
    assert item['paper_exclusion_reasons'] == ['media_only_no_text']
    item['provenance'] = 'historical_review'  # emulate the pre-fix derived cache
    observer.save(state)
    state = asyncio.run(observer.tick())
    item, = state['items'].values()
    correction = deepcopy(item['receipt_metadata_correction'])
    assert correction['previous_provenance'] == 'historical_review'
    assert item['provenance'] == 'prospective_receipt'
    state = asyncio.run(observer.tick())
    assert next(iter(state['items'].values()))['receipt_metadata_correction'] == correction
    summary = inbox.detail(observer.output)
    assert summary['prospective_receipts'] == 1 and summary['historical_review'] == 0
    assert summary['strategy_posts'] == 0 and summary['media_review'] == 1


def test_late_new_receipt_keeps_its_real_time_and_is_not_paper_eligible(setup):
    observer, clock, _, _, add = setup
    asyncio.run(observer.tick())
    clock[0] += timedelta(hours=1)
    add('alastartrader1', published=clock[0]-timedelta(minutes=20))
    state = asyncio.run(observer.tick())
    item, = state['items'].values()
    assert item['provenance'] == 'prospective_receipt'
    assert item['receipt_delay_seconds'] == 1200
    assert item['paper_eligible_at_first_observation'] is False


def test_read_only_health_detects_stopped_worker_and_stale_sources(setup):
    observer, clock, _, _, add = setup
    for ch in inbox.CHANNELS:
        add(ch)
    asyncio.run(observer.tick())
    summary = inbox.detail(observer.output)
    assert summary['health']['status'] == 'healthy'
    assert summary['sources']['alastartrader1']['strategy_posts'] == 1
    clock[0] += timedelta(minutes=4)
    assert inbox.detail(observer.output)['health']['status'] == 'stale'
    # A recent worker tick cannot hide an archive that stopped receiving polls.
    state = inbox.read_state(observer.output)
    state['last_tick_at_utc'] = clock[0].isoformat()
    observer.save(state)
    clock[0] += timedelta(minutes=7)
    state['last_tick_at_utc'] = clock[0].isoformat()
    observer.save(state)
    summary = inbox.detail(observer.output)
    assert summary['health']['status'] == 'degraded'
    assert summary['sources']['alastartrades']['healthy'] is False
