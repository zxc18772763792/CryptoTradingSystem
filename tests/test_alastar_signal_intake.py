import asyncio
from copy import deepcopy
from datetime import timedelta
import json
from types import SimpleNamespace

import pytest

from core.news.alastar_observation import observe_records
from core.research import alastar_signal_intake as intake
from tests.test_alastar_forward_paper import setup as paper_setup, setup as v2_setup, record
from tests.test_alastar_horizon_tracker import setup as horizon_setup


@pytest.fixture
def configured(paper_setup, monkeypatch):
    journal, label, clock = paper_setup
    monkeypatch.setattr(intake, 'now_utc', lambda: clock[0])
    output = journal.output.parent/'intake'
    output.mkdir()
    (output/'state.json').write_text(json.dumps({'items': {}, 'enabled_at_utc': '2026-10-21T00:59:00Z'}))
    result = {'classification': 'fresh_call', 'reason': 'Explicit new long thesis',
              'events': [{**label['events'][0], 'chart_only': False}]}
    calls = []

    async def model(candidate):
        calls.append(candidate['message_key'])
        return deepcopy(result), 'test-news-model'

    runner = intake.Interpreter(output=output, archive=journal.archive, books=lambda: [journal], model=model)
    return runner, journal, clock, calls, result


def tick(runner):
    return asyncio.run(runner.tick())


def labels(journal):
    return list((journal.output/'labels_inbox').glob('auto_*.json'))


def submission_label(runner):
    return next((runner.output/'submissions').glob('*/label.json'))


def clone_book(journal):
    import shutil
    from core.research.alastar_forward_paper import PaperJournal
    output = journal.output.parent/'second'
    shutil.copytree(journal.output, output)
    return PaperJournal(output, journal.archive, journal.protocol_path)


def test_new_text_reaches_real_v2_tracker_once(configured, monkeypatch):
    from core.research import alastar_paper_tracker as tracker
    runner, journal, clock, calls, _ = configured
    monkeypatch.setattr(tracker, 'now_utc', lambda: clock[0])
    state = tick(runner)
    assert state['items']['telegram:alastartrades:1']['status'] == 'label_frozen'
    assert len(calls) == 1 and not labels(journal)
    runtime = tracker.Tracker(journal, lambda: pytest.fail('No entry due yet')).tick()
    assert runtime['processed_labels'] == {}
    assert journal.status()['counts'] == {'planned': 1}
    # Both disk persistence and manual bookkeeping must prevent another request.
    restarted = intake.Interpreter(output=runner.output, archive=journal.archive, books=lambda: [journal], model=runner.model)
    tick(restarted)
    assert len(calls) == 1
    assert journal.status()['journal_records'] == 2


def test_same_label_is_accepted_by_frozen_v3(horizon_setup, monkeypatch):
    journal, label, clock, _ = horizon_setup
    monkeypatch.setattr(intake, 'now_utc', lambda: clock[0])
    result = {'classification': 'fresh_call', 'reason': 'New long thesis',
              'events': [{**label['events'][0], 'chart_only': False}]}

    async def model(candidate):
        return result, 'test-news-model'

    output = journal.output.parent/'intake'
    runner = intake.Interpreter(output=output, archive=journal.archive, books=lambda: [journal], model=model)
    runner.save({'items': {}, 'enabled_at_utc': '2026-10-21T00:59:00Z'})
    assert tick(runner)['items']['telegram:alastartrades:1']['status'] == 'label_frozen'
    records, _ = journal._state()
    accepted = records[-1]
    assert accepted['payload']['events'][0]['status'] == 'planned'
    assert len(accepted['payload']['events'][0]['planned_exits']) == 5


@pytest.mark.parametrize('kind', ['conditional', 'update', 'promotion', 'ambiguous', 'no_signal'])
def test_non_signals_record_reason_without_entering_inbox(configured, kind):
    runner, journal, _, calls, result = configured
    result.update(classification=kind, events=[])
    item = tick(runner)['items']['telegram:alastartrades:1']
    assert item['status'] == 'non_signal' and item['classification'] == kind
    assert not labels(journal) and len(calls) == 1
    tick(runner)
    assert len(calls) == 1


@pytest.mark.parametrize('change', [
    {'symbol': 'ETHUSDT'}, {'symbol': '../BTCUSDT'}, {'direction': True}, {'direction': 0},
    {'confidence': True}, {'confidence': float('nan')}, {'confidence': .64}, {'confidence': 1.1},
    {'original_evidence_quote': 'invented quote'}, {'chart_only': True},
    {'is_fresh_entry_view': 'true'}, {'requires_unavailable_confirmation': True},
    {'opposed_directions_in_message': True}, {'reaction_type': 'conditional'},
    {'published_at_utc': '2020-01-01Z'},
])
def test_malformed_or_ineligible_model_events_cannot_queue(configured, change):
    runner, journal, _, _, result = configured
    result['events'][0].update(change)
    item = tick(runner)['items']['telegram:alastartrades:1']
    assert item['status'] == 'retry_pending' and item['error_type'] == 'ValueError'
    assert not labels(journal)


def test_expired_receipt_never_calls_model_or_backfills(configured):
    runner, journal, clock, calls, _ = configured
    clock[0] += timedelta(minutes=31)
    assert tick(runner)['items']['telegram:alastartrades:1']['status'] == 'expired'
    assert not calls and not labels(journal)


def test_first_enable_does_not_adopt_existing_messages(configured):
    runner, journal, _, calls, _ = configured
    (runner.output/'state.json').unlink()
    assert tick(runner)['items']['telegram:alastartrades:1']['status'] == 'predates_auto_intake'
    assert not calls and not labels(journal)


def test_late_model_response_never_queues_a_past_entry(configured):
    runner, journal, clock, _, result = configured

    async def slow(candidate):
        clock[0] += timedelta(minutes=31)
        return result, 'model'

    runner.model = slow
    assert tick(runner)['items']['telegram:alastartrades:1']['status'] == 'expired'
    assert not labels(journal)


def test_revision_during_model_request_invalidates_interpretation(configured):
    runner, journal, clock, _, result = configured

    async def revised(candidate):
        changed = record()
        changed['content'] += ' changed view'
        clock[0] += timedelta(seconds=1)
        observe_records([changed], journal.archive, mode='live_public_preview', observed_at=clock[0], raw_preview=b'changed source')
        return result, 'model'

    runner.model = revised
    assert tick(runner)['items']['telegram:alastartrades:1']['status'] == 'revised'
    assert not labels(journal)


def test_failure_is_redacted_throttled_and_retried(configured):
    runner, journal, clock, calls, _ = configured
    good_model = runner.model

    async def failed(candidate):
        calls.append('failed')
        raise RuntimeError('secret-token-do-not-persist')

    runner.model = failed
    tick(runner)
    assert 'secret-token' not in (runner.output/'state.json').read_text()
    tick(runner)
    assert calls == ['failed']
    clock[0] += timedelta(seconds=31)
    runner.model = good_model
    assert tick(runner)['items']['telegram:alastartrades:1']['status'] == 'label_frozen'
    assert len(calls) == 2 and journal.status()['journal_records'] == 2


def test_failure_remains_unhealthy_during_retry_cooldown_then_recovers(configured):
    runner, _, clock, _, _ = configured
    good_model = runner.model

    async def failed(candidate):
        raise RuntimeError('model unavailable')

    runner.model = failed
    assert tick(runner)['last_error'] == 'RuntimeError'
    clock[0] += timedelta(seconds=5)
    state = tick(runner)
    assert state['last_error'] == 'RuntimeError'
    assert state['items']['telegram:alastartrades:1']['attempts'] == 1
    clock[0] += timedelta(seconds=26)
    runner.model = good_model
    state = tick(runner)
    assert state['last_error'] is None
    assert 'error_type' not in state['items']['telegram:alastartrades:1']


@pytest.mark.parametrize('enabled,error,active', [(True, None, True), (False, None, False),
                                                (True, 'RuntimeError', False)])
def test_detail_reports_disabled_or_failed_worker_as_inactive(configured, monkeypatch, enabled, error, active):
    runner, _, clock, _, _ = configured
    state = {'items': {}, 'continuous_runner': True, 'last_tick_at_utc': clock[0].isoformat(),
             'last_error': error}
    monkeypatch.setattr(intake, 'read_state', lambda: state)
    monkeypatch.setattr(intake, 'env_bool', lambda *args: enabled)
    assert intake.detail()['runner_active'] is active


def test_disabled_worker_clears_persisted_runner_and_sends_unhealthy_heartbeat(configured, monkeypatch):
    runner, _, clock, calls, _ = configured
    state = intake.read_state(runner.output)
    state.update(continuous_runner=True, last_tick_at_utc=clock[0].isoformat())
    runner.save(state)
    original_read = intake.read_state
    monkeypatch.setattr(intake, 'Interpreter', lambda: runner)
    monkeypatch.setattr(intake, 'read_state', lambda output=runner.output: original_read(output))
    monkeypatch.setattr(intake, 'env_bool', lambda *args: False)
    heartbeats = []

    async def run():
        stop = asyncio.Event()

        async def finish_after_first_poll():
            await asyncio.sleep(0)
            stop.set()

        stopper = asyncio.create_task(finish_after_first_poll())
        await intake.worker(stop, heartbeat=heartbeats.append)
        await stopper

    asyncio.run(run())
    assert heartbeats == [False]
    assert not original_read(runner.output)['continuous_runner']
    assert not calls


def test_inactive_books_and_already_frozen_source_never_call_model(configured):
    runner, journal, _, calls, result = configured
    books = runner.books
    runner.books = lambda: [SimpleNamespace(archive=journal.archive, output=journal.output, _state=lambda: ([], {}))]
    assert tick(runner)['items']['telegram:alastartrades:1']['status'] == 'no_active_unfrozen_book'
    runner.books = books
    journal.freeze({'source_id': 'telegram:alastartrades:1', 'reviewer_or_model_version': 'human',
                    'rule_version': 'v2', 'events': result['events']})
    tick(runner)
    assert not calls and not labels(journal)
    assert intake.read_state(runner.output)['items']['telegram:alastartrades:1']['status'] == 'already_frozen'


def test_partial_submission_reuses_result_and_never_freezes_first_book_twice(configured, monkeypatch):
    runner, journal, clock, calls, _ = configured
    second = clone_book(journal)
    runner.books = lambda: [journal, second]
    first_freeze, second_freeze = journal.freeze, second.freeze
    attempts = [0, 0]

    def first(label):
        attempts[0] += 1
        return first_freeze(label)

    def second_with_lock_contention(label):
        attempts[1] += 1
        if attempts[1] == 1:
            with intake._archive_lock(second.archive):
                return second_freeze(label)
        return second_freeze(label)

    monkeypatch.setattr(journal, 'freeze', first)
    monkeypatch.setattr(second, 'freeze', second_with_lock_contention)
    assert tick(runner)['items']['telegram:alastartrades:1']['status'] == 'retry_pending'
    assert journal.status()['journal_records'] == 2
    assert second.status()['journal_records'] == 1
    original_label = submission_label(runner).read_bytes()
    clock[0] += timedelta(seconds=31)
    item = tick(runner)['items']['telegram:alastartrades:1']
    assert item['status'] == 'label_frozen' and len(item['freeze_receipts']) == 2
    tick(runner)
    assert len(calls) == 1 and attempts == [1, 2]
    assert second.status()['journal_records'] == 2
    assert submission_label(runner).read_bytes() == original_label
    label = json.loads(original_label)
    assert label['rule_version'].endswith(intake.PROMPT_SHA)
    assert label['events'][0]['original_evidence_quote'] == 'fresh long'
    assert not labels(journal) and not labels(second)


def test_frozen_v2_inbox_permanently_rejects_a_temporary_archive_lock(configured):
    from core.research.alastar_paper_tracker import Tracker
    runner, journal, _, _, result = configured
    label = {'source_id': 'telegram:alastartrades:1', 'reviewer_or_model_version': 'test',
             'rule_version': 'test', 'events': result['events']}
    path = journal.output/'labels_inbox'/'old_auto.json'
    path.parent.mkdir()
    path.write_bytes(intake._json_bytes(label))
    runtime = {'processed_labels': {}}
    tracker = Tracker(journal)
    with intake._archive_lock(journal.archive):
        tracker._consume_labels(runtime)
    rejected = next(iter(runtime['processed_labels'].values()))
    assert rejected['status'] == 'rejected' and rejected['type'] == 'RuntimeError'
    tracker._consume_labels(runtime)
    assert next(iter(runtime['processed_labels'].values())) == rejected
    assert journal.status()['journal_records'] == 1


def test_commit_survives_receipt_write_failure_without_second_freeze(configured, monkeypatch):
    runner, journal, clock, calls, _ = configured
    write, freeze = intake._atomic_bytes, journal.freeze
    attempts = []
    failed = []

    def count_freeze(label):
        attempts.append(True)
        return freeze(label)

    def fail_receipt_once(path, raw):
        if path.name.startswith('book_') and not failed:
            failed.append(True)
            raise OSError('temporary receipt disk failure')
        return write(path, raw)

    monkeypatch.setattr(journal, 'freeze', count_freeze)
    monkeypatch.setattr(intake, '_atomic_bytes', fail_receipt_once)
    assert tick(runner)['items']['telegram:alastartrades:1']['status'] == 'retry_pending'
    assert journal.status()['journal_records'] == 2
    clock[0] += timedelta(seconds=31)
    item = tick(runner)['items']['telegram:alastartrades:1']
    assert item['status'] == 'already_frozen' and len(item['freeze_receipts']) == 1
    assert len(calls) == len(attempts) == 1


def test_partial_submission_expiry_keeps_success_and_never_backfills_remaining_book(configured, monkeypatch):
    runner, journal, clock, calls, _ = configured
    second = clone_book(journal)
    runner.books = lambda: [journal, second]
    attempts = []

    def unavailable(label):
        attempts.append(True)
        raise RuntimeError('temporary busy book')

    monkeypatch.setattr(second, 'freeze', unavailable)
    assert tick(runner)['items']['telegram:alastartrades:1']['status'] == 'retry_pending'
    clock[0] += timedelta(minutes=31)
    item = tick(runner)['items']['telegram:alastartrades:1']
    assert item['status'] == 'expired' and len(item['freeze_receipts']) == 1
    assert journal.status()['journal_records'] == 2 and second.status()['journal_records'] == 1
    assert len(calls) == len(attempts) == 1


@pytest.mark.parametrize('already_committed', [False, True])
def test_legacy_queued_state_migrates_only_exact_auto_input(configured, monkeypatch, already_committed):
    runner, journal, clock, calls, _ = configured
    freeze = journal.freeze
    if not already_committed:
        monkeypatch.setattr(journal, 'freeze', lambda label: (_ for _ in ()).throw(RuntimeError('busy')))
    tick(runner)
    monkeypatch.setattr(journal, 'freeze', freeze)
    raw = submission_label(runner).read_bytes()
    legacy = journal.output/'labels_inbox'/f'auto_{intake.digest(b"telegram:alastartrades:1")}.json'
    legacy.parent.mkdir()
    legacy.write_bytes(raw)
    manual = legacy.parent/'human_review.json'
    manual.write_text('{"human": true}')
    state = intake.read_state(runner.output)
    state['items']['telegram:alastartrades:1']['status'] = 'label_queued'
    runner.save(state)
    clock[0] += timedelta(seconds=31)
    item = tick(runner)['items']['telegram:alastartrades:1']
    assert item['status'] == ('already_frozen' if already_committed else 'label_frozen')
    assert not legacy.exists() and manual.read_text() == '{"human": true}'
    assert len(calls) == 1 and journal.status()['journal_records'] == 2
    archived = next((runner.output/'submissions').glob('*/legacy_*.json'))
    assert archived.read_bytes() == raw


def test_legacy_automatic_filename_does_not_authorize_removing_changed_input(configured, monkeypatch):
    runner, journal, clock, calls, _ = configured
    freeze = journal.freeze
    monkeypatch.setattr(journal, 'freeze', lambda label: (_ for _ in ()).throw(RuntimeError('busy')))
    tick(runner)
    monkeypatch.setattr(journal, 'freeze', freeze)
    legacy = journal.output/'labels_inbox'/f'auto_{intake.digest(b"telegram:alastartrades:1")}.json'
    legacy.parent.mkdir()
    changed = submission_label(runner).read_bytes() + b'\n'
    legacy.write_bytes(changed)
    clock[0] += timedelta(seconds=31)
    item = tick(runner)['items']['telegram:alastartrades:1']
    assert item['status'] == 'retry_pending'
    assert legacy.read_bytes() == changed
    assert len(calls) == 1 and journal.status()['journal_records'] == 1


def test_deadline_is_rechecked_between_books(configured, monkeypatch):
    runner, journal, clock, calls, _ = configured
    second = clone_book(journal)
    runner.books = lambda: [journal, second]
    freeze = journal.freeze

    def first_commit_then_clock_advances(label):
        committed = freeze(label)
        clock[0] += timedelta(minutes=31)
        return committed

    monkeypatch.setattr(journal, 'freeze', first_commit_then_clock_advances)
    item = tick(runner)['items']['telegram:alastartrades:1']
    assert item['status'] == 'expired' and len(item['freeze_receipts']) == 1
    assert journal.status()['journal_records'] == 2 and second.status()['journal_records'] == 1
    assert len(calls) == 1


@pytest.mark.parametrize('partial_submission', [False, True])
def test_revised_candidate_removed_from_queue_clears_pending_error_without_losing_receipts(
        configured, monkeypatch, partial_submission):
    runner, journal, clock, calls, _ = configured
    good_model = runner.model
    if partial_submission:
        second = clone_book(journal)
        runner.books = lambda: [journal, second]
        monkeypatch.setattr(second, 'freeze', lambda label: (_ for _ in ()).throw(RuntimeError('busy')))
    else:
        async def failed(candidate):
            calls.append(candidate['message_key'])
            raise RuntimeError('temporary model failure')
        runner.model = failed
    item = tick(runner)['items']['telegram:alastartrades:1']
    assert item['status'] == 'retry_pending'
    receipts = deepcopy(item.get('freeze_receipts', {}))
    assert bool(receipts) is partial_submission
    changed = record()
    changed['content'] += ' edited directional view'
    clock[0] += timedelta(seconds=1)
    observe_records([changed], journal.archive, mode='live_public_preview', observed_at=clock[0],
                    raw_preview=b'edited source')
    assert json.loads((journal.archive/'candidate_review_queue.json').read_bytes())['candidates'] == []
    runner.model = good_model
    state = tick(runner)
    item = state['items']['telegram:alastartrades:1']
    assert item['status'] == 'removed_from_current_review_queue'
    assert item.get('freeze_receipts', {}) == receipts
    assert state['last_error'] is None and 'error_type' not in item
    tick(runner)
    assert len(calls) == 1
