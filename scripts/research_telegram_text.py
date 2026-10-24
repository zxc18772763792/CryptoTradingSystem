"""Collect-free research runner: import, price-blind labels, then offline backtest.

Use the existing public-history collector for acquisition. This runner never
registers strategies, sends chat messages or touches the live news database.
"""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.research.telegram_text_signals import (
    PROMPT, PROMPT_SHA, PROTOCOL, CHAT_PROTOCOL, chat_sentiment_features, export_messages, freeze_protocol,
    enforce_eligibility, prepare_messages, sha, validate_labels, write_json,
)


def read_messages(output: Path) -> list[dict]:
    archive = output / 'archive'
    rows = []
    if (archive / 'messages.jsonl').exists():
        rows += [json.loads(line) for line in (archive / 'messages.jsonl').read_text(encoding='utf-8').splitlines() if line]
    else:
        # Allows classification of completed pages while the collector continues.
        from core.news.collectors.telegram import TelegramPublicChannelCollector
        parser = TelegramPublicChannelCollector({'defaults': {'telegram_alastar_channels': ['cryptoquant_official']}})
        try:
            for path in sorted((archive / 'pages').glob('*.html')):
                rows += parser._from_html(path.read_text(encoding='utf-8'), channel='cryptoquant_official')
        finally:
            parser.close()
    for path in sorted((output / 'imports').glob('*.jsonl')):
        rows += [json.loads(line) for line in path.read_text(encoding='utf-8').splitlines() if line]
    return rows


async def label_messages(output: Path, batch_size: int) -> None:
    from loguru import logger
    logger.remove()  # Provider failures are recorded by type, never raw transport bodies.
    from core.news.eventizer.async_glm_client import AsyncGLMClient
    from core.news.eventizer.rules import load_news_rule_config
    from core.utils.openai_responses import extract_response_text
    cfg = load_news_rule_config(ROOT / 'config/news_rules.yaml', ROOT / 'config/symbols.yaml')
    client = AsyncGLMClient(cfg)
    rows, quality = prepare_messages(read_messages(output))
    write_json(output / 'quality.json', quality)
    cache = output / 'labels'
    cache.mkdir(exist_ok=True)
    by_id = {row['source_id']: row for row in rows}
    recovered = 0
    # Reuse source-bound outputs after conservative validation fixes; no prices
    # or model calls participate in recovering previously rejected records.
    for path in sorted((output / 'label_attempts').glob('*.json'), key=lambda p: p.stat().st_mtime_ns):
        attempt = json.loads(path.read_text(encoding='utf-8'))
        source_ids = attempt['source_ids']
        labels = attempt.get('response', {}).get('labels', [])
        ids = Counter(str(label.get('id')) for label in labels if isinstance(label, dict))
        for item in labels:
            if not isinstance(item, dict) or not str(item.get('id')).isdigit():
                continue
            i = int(item['id'])
            if i >= len(source_ids) or ids[str(i)] != 1 or source_ids[i] not in by_id:
                continue
            row = by_id[source_ids[i]]
            dest = cache / f'{sha(row["source_id"])}.json'
            if dest.exists():
                continue
            try:
                eligible = enforce_eligibility(item, row)
                label = validate_labels({'labels': [{**eligible, 'id': '0'}]}, [row])[0]
                if eligible.get('eligibility_rejection'):
                    label['eligibility_rejection'] = eligible['eligibility_rejection']
                label.update({'model': attempt['model'], 'labelled_at_utc': datetime.now(timezone.utc).isoformat(),
                              'recovered_from': path.name})
                write_json(dest, label)
                recovered += 1
            except ValueError:
                pass
    pending = []
    for row in rows:
        path = cache / f'{sha(row["source_id"])}.json'
        if path.exists():
            old = json.loads(path.read_text(encoding='utf-8'))
            if old['text_sha256'] != row['text_sha256'] or old['prompt_sha256'] != PROMPT_SHA:
                raise ValueError('Cached source wording or prompt changed')
        else:
            pending.append(row)
    batches = [pending[i:i + batch_size] for i in range(0, len(pending), batch_size)]
    if len(batches) > 200:
        raise ValueError('More than 200 batches: split the input deliberately')
    print(json.dumps({'usable': len(rows), 'recovered': recovered, 'pending': len(pending), 'batches': len(batches)}), flush=True)
    semaphore = asyncio.Semaphore(2)
    failures = []

    async def one(number, batch):
        async with semaphore:
            work = list(batch)
            errors = {}
            for attempt in range(2):
                try:
                    response, error = await client.chat_completions([
                        {'role': 'system', 'content': PROMPT},
                        {'role': 'user', 'content': json.dumps({'messages': [
                            {'id': str(i), 'text': row['content']} for i, row in enumerate(work)
                        ]}, ensure_ascii=False)},
                    ], temperature=0, max_tokens=6000, timeout=60)
                    if error != 'none':
                        raise RuntimeError('model_' + error)
                    choices = response.get('choices') or []
                    if choices and choices[0].get('finish_reason') not in (None, 'stop'):
                        raise ValueError('Truncated model response')
                    content = extract_response_text(response).strip()
                    content = re.sub(r'^```(?:json)?\s*|\s*```$', '', content)
                    payload = json.loads(content)
                    target = response.get('_cts_openai_target') or {}
                    model = str(target.get('model') or response.get('model') or client._model)
                    write_json(output / 'label_attempts' / f'{sha(json.dumps([r["source_id"] for r in work]))}_{attempt}.json',
                               {'source_ids': [r['source_id'] for r in work], 'response': payload, 'model': model})
                    labels = payload.get('labels')
                    if not isinstance(labels, list):
                        raise ValueError('Missing labels')
                    ids = Counter(str(label.get('id')) for label in labels if isinstance(label, dict))
                    candidates = {str(label.get('id')): label for label in labels if isinstance(label, dict)}
                    retry, saved = [], 0
                    for i, row in enumerate(work):
                        try:
                            key = str(i)
                            if ids[key] != 1:
                                raise ValueError('Missing or duplicate model ID')
                            eligible = enforce_eligibility(candidates[key], row)
                            label = validate_labels({'labels': [{**eligible, 'id': '0'}]}, [row])[0]
                            if eligible.get('eligibility_rejection'):
                                label['eligibility_rejection'] = eligible['eligibility_rejection']
                            label.update({'model': model, 'labelled_at_utc': datetime.now(timezone.utc).isoformat()})
                            write_json(cache / f'{sha(label["source_id"])}.json', label)
                            saved += 1
                        except ValueError as exc:
                            retry.append(row)
                            errors[row['source_id']] = str(exc)
                    print(json.dumps({'batch': number, 'attempt': attempt + 1, 'labelled': saved, 'retry': len(retry), 'model': model}), flush=True)
                    work = retry
                    if not work:
                        return
                except Exception as exc:
                    for row in work:
                        errors[row['source_id']] = type(exc).__name__
                if attempt == 0:
                    await asyncio.sleep(2)
            failures.append({'batch': number, 'source_ids': [r['source_id'] for r in work],
                             'errors': {r['source_id']: errors.get(r['source_id'], 'unknown') for r in work}})
            print(json.dumps({'batch': number, 'unresolved': len(work)}), flush=True)

    await asyncio.gather(*(one(i + 1, batch) for i, batch in enumerate(batches)))
    write_json(output / 'label_failures.json', failures)
    print(json.dumps({'completed_batches': len(batches) - len(failures), 'failed_batches': len(failures)}), flush=True)


def freeze_labels(output: Path) -> tuple[list[dict], dict]:
    rows, quality = prepare_messages(read_messages(output))
    all_labels, missing = [], []
    for row in rows:
        path = output / 'labels' / f'{sha(row["source_id"])}.json'
        if not path.exists():
            missing.append(row['source_id'])
            continue
        label = json.loads(path.read_text(encoding='utf-8'))
        if label['prompt_sha256'] != PROMPT_SHA or label['text_sha256'] != row['text_sha256']:
            raise ValueError('Unmatched annotation')
        all_labels.append({**label, 'channel': row['channel'], 'published_at': row['published_at'], 'url': row['url'],
                           'author_hash': row.get('author_hash'), 'forwarded': bool(row.get('payload', {}).get('forwarded'))})
    if missing:
        write_json(output / 'unlabelled.json', missing)
        raise ValueError(f'{len(missing)} messages have no valid frozen annotation; rerun labels')
    blob = '\n'.join(json.dumps(x, ensure_ascii=False, sort_keys=True) for x in all_labels).encode()
    path = output / 'frozen_labels.jsonl'
    if path.exists() and path.read_bytes() != blob:
        raise ValueError('Frozen labels changed: use a separate evaluation output')
    path.write_bytes(blob)
    quality.update({'labelled': len(all_labels), 'label_classes': dict(Counter(x['kind'] for x in all_labels)),
                    'eligibility_rejections': dict(Counter(x['eligibility_rejection'] for x in all_labels if x.get('eligibility_rejection'))),
                    'first_publication': min((x['published_at'] for x in all_labels), default=None),
                    'last_publication': max((x['published_at'] for x in all_labels), default=None),
                    'labels_sha256': sha(blob), 'labels_frozen_before_price_join': True})
    write_json(output / 'quality.json', quality)
    return all_labels, quality


def entry_hour(value):
    import pandas as pd
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        raise ValueError('Publication time requires timezone')
    return (stamp.tz_convert('UTC') + pd.Timedelta(seconds=60)).ceil('h')


def independent_events(labels: list[dict], channel: str, symbol: str, hours: int, phase: str) -> tuple[list[dict], dict]:
    import pandas as pd
    split = pd.Timestamp(PROTOCOL['holdout_start_utc'])
    available, skipped = [], Counter()
    for label in labels:
        if label['channel'] != channel:
            continue
        for event in label['events']:
            if event['symbol'] != symbol or event['confidence'] < .7:
                continue
            start = entry_hour(label['published_at'])
            end = start + pd.Timedelta(hours=hours)
            if phase == 'development' and end >= split - pd.Timedelta(hours=168):
                continue
            if phase == 'holdout' and start < split:
                continue
            available.append({**label, **event, 'entry_at': start, 'exit_at': end, 'hours': hours})
    selected, next_free = [], None
    for row in sorted(available, key=lambda r: (r['entry_at'], r['published_at'], r['source_id'])):
        if next_free is not None and row['entry_at'] < next_free:
            skipped['overlapping_event'] += 1
            continue
        selected.append(row)
        next_free = row['exit_at']
    return selected, dict(skipped)


class Market:
    def __init__(self, root=ROOT):
        from scripts.research_telegram_signal_strategy import LocalMarket
        self.store = LocalMarket(root)
        self.cache = {}

    def outcome(self, symbol, start, hours, side):
        key = symbol, start, hours, side
        if key not in self.cache:
            self.cache[key] = self._outcome(symbol, start, hours, side)
        return self.cache[key]

    def _outcome(self, symbol, start, hours, side):
        import numpy as np
        import pandas as pd
        bars = self.store.frame(symbol)
        end = start + pd.Timedelta(hours=hours)
        wanted = pd.date_range(start, end, freq='h')
        if bars.empty or not wanted.isin(bars.index).all():
            return {'status': 'missing_price'}
        frame = bars.loc[wanted]
        values = frame[['open', 'high', 'low', 'close', 'volume']].to_numpy(float)
        if not np.isfinite(values).all() or (values <= 0).any():
            return {'status': 'invalid_price'}
        if (frame.high < frame[['open', 'close', 'low']].max(axis=1)).any() or (frame.low > frame[['open', 'close', 'high']].min(axis=1)).any():
            return {'status': 'invalid_ohlc_relationship'}
        p0, p1 = float(frame.iloc[0].open), float(frame.iloc[-1].open)
        gross = side * (p1 / p0 - 1)
        liquidated = side < 0 and float(frame.iloc[:-1].high.max()) >= 1.99 * p0
        if liquidated:
            return {'status': 'liquidation_approximation_excluded'}
        fund = self.store.frame(symbol, 'funding_rate')
        amount, why = funding_return(fund, start, end, p0, side)
        return {'status': 'ok', 'gross': gross, 'price_only_net': gross - .002,
                'funding': amount, 'funding_missing_reason': why,
                'net': None if amount is None else gross - .002 + amount,
                'net_stress': None if amount is None else gross - .004 + amount}


def funding_return(frame, start, end, entry_price, side):
    import numpy as np
    import pandas as pd
    if frame.empty or not {'funding_rate', 'mark_price'}.issubset(frame.columns):
        return None, 'unavailable'
    before, after = frame.index[frame.index <= start], frame.index[frame.index >= end]
    if not len(before) or not len(after):
        return None, 'boundary_uncovered'
    window = frame.loc[before[-1]:after[0]]
    if window.index.to_series().diff().max() > pd.Timedelta(hours=12):
        return None, 'settlement_gap'
    paid = window[(window.index > start) & (window.index <= end)]
    if not np.isfinite(paid[['funding_rate', 'mark_price']].to_numpy(float)).all() or (paid.mark_price <= 0).any():
        return None, 'invalid_settlement'
    return float((-side * paid.funding_rate * paid.mark_price / entry_price).sum()), None


def metrics(rows: list[dict], field='net') -> dict:
    import numpy as np
    from scripts.review_telegram_signals import week_bootstrap
    values = np.array([r[field] for r in rows if r.get(field) is not None], dtype=float)
    kept = [r for r in rows if r.get(field) is not None]
    if not len(values):
        return {'n': 0, 'mean': None, 'median': None, 'win_rate': None, 'ci95': None}
    losses = float(-values[values < 0].sum())
    return {'n': len(values), 'mean': float(values.mean()), 'median': float(np.median(values)),
            'win_rate': float((values > 0).mean()), 'profit_factor': float(values[values > 0].sum() / losses) if losses else None,
            'mean_without_best': float(np.delete(values, values.argmax()).mean()) if len(values) > 1 else None,
            'calendar_weeks': len({r['entry_at'].isocalendar()[:2] for r in kept}),
            'ci95': week_bootstrap(values, [r['entry_at'] for r in kept])}


def random_null(rows, market, hours, phase):
    import numpy as np
    import pandas as pd
    from scripts.review_telegram_signals import permutation_test
    nulls, observed = [], []
    split = pd.Timestamp(PROTOCOL['holdout_start_utc'])
    for row in rows:
        candidates = []
        window = 90 if hours == 720 else 28
        for days in range(-window, window + 1):
            if abs(days * 24) <= hours:
                continue
            at = row['entry_at'] + pd.Timedelta(days=days)
            if phase == 'holdout' and at < split:
                continue
            if phase == 'development' and at + pd.Timedelta(hours=hours + 168) >= split:
                continue
            result = market.outcome(row['symbol'], at, hours, row['side'])
            if result.get('net') is not None:
                candidates.append(result['net'])
        if len(candidates) >= 10:
            nulls.append(np.array(candidates))
            observed.append(row['net'])
    if not nulls:
        return {'matched_n': 0, 'p_two_sided': None}
    test = permutation_test(np.array(observed), nulls, draws=5000, seed=710)
    test.update({'matched_n': len(observed), 'observed_matched_mean': float(np.mean(observed)),
                 'excess_over_null': float(np.mean(observed) - test['null_mean'])})
    return test


def run_backtest(output: Path, market_root=ROOT):
    import pandas as pd
    from scripts.review_telegram_signals import bh_fdr
    labels, quality = freeze_labels(output)  # This must happen BEFORE market construction.
    reviewed = []
    review_counts = Counter()
    for label in labels:
        accepted = []
        for event in label['events']:
            if event['confidence'] < .7:
                continue
            key = sha(label['source_id'] + '|' + event['symbol'])
            path = output / 'semantic_review' / f'{key}.json'
            if not path.exists():
                raise ValueError('Price-blind semantic review incomplete')
            review = json.loads(path.read_text(encoding='utf-8'))
            if review['text_sha256'] != label['text_sha256'] or review['side'] != event['side'] or review['quote'] != event['quote']:
                raise ValueError('Semantic review source mismatch')
            review_counts[review['reason']] += 1
            if review['accept']:
                accepted.append(event)
        reviewed.append({**label, 'events': accepted})
    market = Market(market_root)
    summaries, outcomes = [], []
    for selection, selected_labels in [('raw', labels), ('semantic_reviewed', reviewed)]:
      print(json.dumps({'signal_set': selection}), flush=True)
      for channel in sorted({r['channel'] for r in selected_labels}):
        for symbol in ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']:
            for hours in [1, 6, 24, 72, 168, 720]:
                for phase in ['development', 'holdout']:
                    events, skipped = independent_events(selected_labels, channel, symbol, hours, phase)
                    valid, priced = [], []
                    for row in events:
                        result = market.outcome(symbol, row['entry_at'], hours, row['side'])
                        if result['status'] != 'ok':
                            skipped[result['status']] = skipped.get(result['status'], 0) + 1
                            continue
                        item = {**row, **result, 'phase': phase, 'signal_set': selection}
                        priced.append(item)
                        if result['net'] is not None:
                            always_long = market.outcome(symbol, row['entry_at'], hours, 1)
                            reverse = market.outcome(symbol, row['entry_at'], hours, -row['side'])
                            item.update({'always_long_net': always_long.get('net'), 'reverse_net': reverse.get('net'),
                                         'excess_long': result['net'] - always_long['net']})
                            valid.append(item)
                        else:
                            reason = 'funding_' + result['funding_missing_reason']
                            skipped[reason] = skipped.get(reason, 0) + 1
                        outcomes.append(item)
                    row = {'channel': channel, 'symbol': symbol, 'hours': hours, 'phase': phase, 'signal_set': selection,
                           'candidate_events': len(events), 'skipped': skipped, 'funded': metrics(valid),
                           'price_only': metrics(priced, 'price_only_net'), 'stress': metrics(valid, 'net_stress'),
                           'same_entry_long': metrics(valid, 'always_long_net'), 'reverse': metrics(valid, 'reverse_net'),
                           'excess_long': metrics(valid, 'excess_long')}
                    row['random_time'] = random_null(valid, market, hours, phase) if phase == 'holdout' else {'matched_n': 0, 'p_two_sided': None}
                    row['holdout_halves'] = {name: metrics([r for r in valid if (r['entry_at'] < pd.Timestamp('2026-07-01T00:00:00Z')) == first])
                                              for name, first in [('H1', True), ('H2', False)]} if phase == 'holdout' else {}
                    summaries.append(row)
                    print(json.dumps({'channel': channel, 'symbol': symbol, 'hours': hours, 'phase': phase,
                                      'n': row['funded']['n'], 'mean': row['funded']['mean']}), flush=True)
    tests = [r for r in summaries if r['phase'] == 'holdout' and r['random_time'].get('p_two_sided') is not None]
    if tests:
        for row, q in zip(tests, bh_fdr([r['random_time']['p_two_sided'] for r in tests])):
            row['random_time']['q_fdr'] = float(q)
    result = {'generated_at_utc': datetime.now(timezone.utc).isoformat(), 'quality': quality,
              'protocol': PROTOCOL, 'results': summaries, 'market_sources': market.store.sources,
              'fdr_family_size': len(tests), 'semantic_review_counts': dict(review_counts),
              'review_protocol': json.loads((output / 'review_protocol.json').read_text(encoding='utf-8')),
              'evidence_status': 'retrospective_exploratory_not_forward_validation'}
    for relative, meta in market.store.sources.items():
        blob = (market_root / relative).read_bytes()
        if sha(blob) != meta['sha256']:
            raise ValueError('Price input changed during evaluation')
        snapshot = output / 'market_snapshot' / relative
        snapshot.parent.mkdir(parents=True, exist_ok=True)
        snapshot.write_bytes(blob)
    write_json(output / 'results.json', result)
    with (output / 'event_outcomes.jsonl').open('w', encoding='utf-8') as stream:
        for row in outcomes:
            stream.write(json.dumps(row, ensure_ascii=False, default=str, allow_nan=False) + '\n')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('stage', choices=['init', 'import', 'labels', 'backtest', 'chat-features'])
    parser.add_argument('--output', type=Path, default=ROOT / 'data/research/cryptoquant_text_20261023')
    parser.add_argument('--batch-size', type=int, default=20)
    parser.add_argument('--channel', choices=['BinanceChinese', 'Binance_api_Chinese'])
    parser.add_argument('--export', type=Path)
    parser.add_argument('--market-root', type=Path, default=ROOT)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    freeze_protocol(output)
    chat_path = output / 'chat_protocol.json'
    if chat_path.exists() and json.loads(chat_path.read_text(encoding='utf-8'))['protocol'] != CHAT_PROTOCOL:
        raise ValueError('Chat protocol changed: use a new output directory')
    if not chat_path.exists():
        write_json(chat_path, {'frozen_at_utc': datetime.now(timezone.utc).isoformat(), 'protocol': CHAT_PROTOCOL})
    if args.stage == 'import':
        if not args.export or not args.channel:
            parser.error('import requires --export and --channel')
        import secrets
        salt_path = output / 'author_salt.txt'
        if not salt_path.exists():
            salt_path.write_text(secrets.token_hex(32), encoding='utf-8')
        rows = export_messages(args.export, args.channel, author_salt=salt_path.read_text(encoding='utf-8'))
        dest = output / 'imports' / f'{args.channel}_{sha(args.export.read_bytes())[:12]}.jsonl'
        dest.parent.mkdir(exist_ok=True)
        dest.write_text('\n'.join(json.dumps(r, ensure_ascii=False) for r in rows), encoding='utf-8')
        print(json.dumps({'imported': len(rows), 'channel': args.channel}))
    elif args.stage == 'labels':
        if not 1 <= args.batch_size <= 30:
            parser.error('batch size must be 1..30')
        asyncio.run(label_messages(output, args.batch_size))
    elif args.stage == 'backtest':
        run_backtest(output, args.market_root.resolve())
    elif args.stage == 'chat-features':
        labels, quality = freeze_labels(output)
        sources = sorted({r['channel'] for r in labels} & {'BinanceChinese', 'Binance_api_Chinese'})
        features = chat_sentiment_features(labels)
        write_json(output / 'chat_features.json', {'status': 'ready' if sources else 'awaiting_group_data',
                   'sources': sources, 'features': features, 'protocol': CHAT_PROTOCOL})
        print(json.dumps({'status': 'ready' if sources else 'awaiting_group_data', 'features': len(features)}))
    else:
        print(json.dumps({'protocol_frozen': str(output / 'protocol.json')}))


if __name__ == '__main__':
    main()
