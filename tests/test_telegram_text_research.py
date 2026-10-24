from copy import deepcopy
import json

import pandas as pd
import pytest

from core.research.telegram_text_signals import (
    PROTOCOL, chat_sentiment_features, enforce_eligibility, export_messages, freeze_protocol, prepare_messages,
    redact_sensitive_text, sha, validate_labels,
)
from scripts.research_telegram_text import Market, entry_hour, funding_return, independent_events


def message(text='Bitcoin could rise', source_id='telegram:cryptoquant_official:1', **extra):
    return {'source_id': source_id, 'content': text, 'published_at': '2025-04-01T00:00:00+00:00',
            'payload': {'channel': 'cryptoquant_official'}, **extra}


def test_quality_deduplicates_reposts_and_rejects_edit_truncation_and_naive_dates():
    row = message()
    rows, counts = prepare_messages([
        row, row, message(' Bitcoin  could rise ', source_id='telegram:cryptoquant_official:2'),
        message('Bitcoin down', source_id='telegram:cryptoquant_official:3', payload={'channel': 'cryptoquant_official', 'telegram_edited': True}),
        message('Bitcoin moon', source_id='telegram:cryptoquant_official:4', payload={'channel': 'cryptoquant_official', 'text_truncated': True}),
        message('Bitcoin up', source_id='telegram:cryptoquant_official:5', published_at='2025-05-01T00:00:00'),
    ])
    assert len(rows) == 1
    assert counts['duplicate_id'] == counts['duplicate_text'] == 1
    assert counts['edited_excluded'] == counts['truncated_excluded'] == counts['missing_timezone'] == 1


def test_desktop_export_uses_epoch_and_flattens_entities_without_retaining_senders(tmp_path):
    path = tmp_path / 'result.json'
    path.write_text(json.dumps({'messages': [
        {'id': 1, 'type': 'message', 'date': 'wrong', 'date_unixtime': '1735689600', 'from': 'Private Name',
         'from_id': 'user123', 'text': ['我看好', {'type': 'bold', 'text': '比特币'}, '上涨']},
        {'id': 2, 'type': 'service', 'date_unixtime': '1735689600', 'text': 'joined'},
        {'id': 3, 'type': 'message', 'date': '2025-01-01', 'text': 'no epoch'},
    ]}), encoding='utf-8')
    rows = export_messages(path, 'BinanceChinese')
    assert len(rows) == 1 and rows[0]['content'] == '我看好比特币上涨'
    assert rows[0]['published_at'] == '2025-01-01T00:00:00+00:00'
    assert 'Private Name' not in json.dumps(rows)
    assert 'user123' not in json.dumps(rows)


def test_chat_credentials_and_contact_fields_are_redacted():
    result = redact_sensitive_text('api_key="AbcDEf1234567890" secret_key=TOPSECRET @SomePerson a@b.com +8613812345678 BTC看多')
    assert 'AbcDEf' not in result and 'TOPSECRET' not in result
    assert '@SomePerson' not in result and 'a@b.com' not in result
    assert 'BTC看多' in result


def test_validation_requires_every_row_exact_evidence_and_explicit_asset():
    row = {**message(), 'text_sha256': sha('Bitcoin could rise')}
    valid = {'labels': [{'id': '0', 'kind': 'directional', 'events': [
        {'symbol': 'BTCUSDT', 'side': 1, 'confidence': .8, 'quote': 'Bitcoin could rise'}]}]}
    assert validate_labels(valid, [row])[0]['events'][0]['side'] == 1
    for field, value in [('symbol', 'ETHUSDT'), ('side', True), ('confidence', float('nan')), ('quote', 'invented')]:
        bad = deepcopy(valid)
        bad['labels'][0]['events'][0][field] = value
        with pytest.raises(ValueError):
            validate_labels(bad, [row])
    with pytest.raises(ValueError):
        validate_labels({'labels': []}, [row])
    bad = deepcopy(valid)
    bad['labels'][0]['kind'] = 'technical'
    with pytest.raises(ValueError):
        validate_labels(bad, [row])


def test_entry_never_uses_publication_bar_or_naive_timezone():
    assert str(entry_hour('2025-01-01T08:00:00+08:00')) == '2025-01-01 01:00:00+00:00'
    assert str(entry_hour('2025-01-01T00:59:30+00:00')) == '2025-01-01 02:00:00+00:00'
    with pytest.raises(ValueError):
        entry_hour('2025-01-01T00:00:00')


def test_model_asset_inference_and_nondirectional_events_cannot_create_trades():
    hallucinated = {'kind': 'directional', 'events': [{'symbol': 'BTCUSDT', 'side': 1}]}
    fixed = enforce_eligibility(hallucinated, message('Bulls are in control'))
    assert fixed['kind'] == 'other' and fixed['events'] == []
    conditional = enforce_eligibility({**hallucinated, 'kind': 'conditional'}, message())
    assert conditional['kind'] == 'conditional' and conditional['events'] == []
    explicit = enforce_eligibility(hallucinated, message('Bitcoin could rise'))
    assert explicit == hallucinated


def test_overlaps_and_purge_do_not_inflate_holdout_sample():
    def label(i, at):
        return {'source_id': str(i), 'published_at': at, 'channel': 'cryptoquant_official', 'events': [
            {'symbol': 'BTCUSDT', 'side': 1, 'confidence': .8}]}
    labels = [label(1, '2026-01-01T00:01:00Z'), label(2, '2026-01-01T01:01:00Z'), label(3, '2025-12-30T00:00:00Z')]
    rows, skipped = independent_events(labels, 'cryptoquant_official', 'BTCUSDT', 24, 'holdout')
    assert len(rows) == 1 and rows[0]['source_id'] == '1' and skipped['overlapping_event'] == 1
    rows, _ = independent_events(labels, 'cryptoquant_official', 'BTCUSDT', 24, 'development')
    assert not rows


def test_missing_funding_is_unknown_and_boundary_payments_use_mark_not_entry_notional():
    start = pd.Timestamp('2025-01-01T00:00:00Z')
    frame = pd.DataFrame({'funding_rate': [.001, .002, .003, .004], 'mark_price': [100, 110, 120, 130]},
                         index=pd.date_range(start, periods=4, freq='8h'))
    value, why = funding_return(frame, start, start + pd.Timedelta(hours=16), 100, 1)
    assert why is None and value == pytest.approx(-.002 * 1.1 - .003 * 1.2)
    assert funding_return(frame.iloc[[0, 3]], start, start + pd.Timedelta(hours=24), 100, 1)[0] is None
    assert funding_return(frame, start, start + pd.Timedelta(hours=32), 100, 1)[0] is None


def test_frozen_protocol_cannot_silently_change(tmp_path):
    freeze_protocol(tmp_path)
    path = tmp_path / 'protocol.json'
    doc = json.loads(path.read_text(encoding='utf-8'))
    doc['protocol']['cost_roundtrip'] = 0
    path.write_text(json.dumps(doc), encoding='utf-8')
    with pytest.raises(ValueError):
        freeze_protocol(tmp_path)


def test_chat_features_balance_authors_exclude_technical_and_only_close_hours():
    def label(author, side=1, **extra):
        return {'channel': 'Binance_api_Chinese', 'published_at': '2026-04-01T10:30:00Z',
                'author_hash': author, 'kind': 'directional',
                'events': [{'symbol': 'BTCUSDT', 'side': side, 'confidence': .8}], **extra}
    rows = [label('spam', -1) for _ in range(100)] + [label(str(i)) for i in range(4)]
    rows += [label('support', -1, kind='technical'), label('forward', -1, forwarded=True)]
    feature = chat_sentiment_features(rows)[0]
    assert feature['directional_authors'] == 5
    assert feature['imbalance'] == pytest.approx(.6)
    assert feature['qualified'] and feature['follow_side'] == 1
    assert feature['available_at_utc'] == '2026-04-01T11:00:00+00:00'
    assert feature['earliest_hourly_entry_utc'] == '2026-04-01T12:00:00+00:00'
    assert chat_sentiment_features([label('one')])[0]['qualified'] is False


def test_event_returns_charge_fees_and_directional_funding_without_filling_price_gaps():
    from types import SimpleNamespace
    start = pd.Timestamp('2025-01-01T00:00:00Z')
    index = pd.date_range(start, periods=25, freq='h')
    prices = pd.Series(range(100, 125), index=index, dtype=float)
    bars = pd.DataFrame({'open': prices, 'high': prices + 1, 'low': prices - 1, 'close': prices + .5, 'volume': 1})
    funding = pd.DataFrame({'funding_rate': .001, 'mark_price': [92, 100, 108, 116, 124, 132]},
                           index=pd.date_range(start - pd.Timedelta(hours=8), periods=6, freq='8h'))
    market = Market.__new__(Market)
    market.cache = {}
    market.store = SimpleNamespace(frame=lambda symbol, kind='1h': funding if kind == 'funding_rate' else bars)
    long = market.outcome('BTCUSDT', start, 24, 1)
    short = market.outcome('BTCUSDT', start, 24, -1)
    assert long['net'] == pytest.approx(.24 - .002 - .001 * (1.08 + 1.16 + 1.24))
    assert short['net'] == pytest.approx(-.24 - .002 + .001 * (1.08 + 1.16 + 1.24))
    assert long['net_stress'] == pytest.approx(long['net'] - .002)
    bars.drop(index[3], inplace=True)
    market.cache.clear()
    assert market.outcome('BTCUSDT', start, 24, 1)['status'] == 'missing_price'
