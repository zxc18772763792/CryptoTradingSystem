import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import numpy as np
import pandas as pd
import pytest

from core.ai import research_loop_evaluation as ev
from core.ai.research_loop_service import AutonomousResearchLoop, ResearchLoopConfig
from core.ai.proposal_schemas import StrategyDraft, StrategyProgram


class Registry:
    def __init__(self):
        self.rows = {}

    def save(self, row):
        self.rows[next(getattr(row, key) for key in ('candidate_id', 'run_id', 'experiment_id', 'proposal_id') if hasattr(row, key))] = row

    def get(self, key):
        return self.rows.get(key)


@pytest.fixture
def app(tmp_path):
    return SimpleNamespace(state=SimpleNamespace(ai_research_dir=tmp_path, research_jobs={},
        ai_proposal_registry=Registry(), ai_candidate_registry=Registry(), ai_experiment_registry=Registry(), ai_experiment_run_registry=Registry()))


def frame(rows=720, end='2026-09-09T08:00:00Z'):
    index = pd.date_range(end=end, periods=rows, freq='h')
    prices = 100 + np.sin(np.arange(rows) / 4) * 4 + np.arange(rows) * .005
    return pd.DataFrame({'open': prices, 'high': prices + 1, 'low': prices - 1, 'close': prices, 'volume': np.ones(rows) * 100}, index=index)


def program():
    return StrategyProgram(name='Mean reversion', indicators=[{'name':'z', 'kind':'zscore', 'period':12}],
        entry_conditions=[{'left':'z','op':'lt','right':-1}], exit_conditions=[{'left':'z','op':'gt','right':1}])


def good_metrics(**overrides):
    return {'total_return':2, 'max_drawdown':1, 'sharpe_ratio':2, 'total_trades':8, 'quality_flag':'ok', **overrides}


def output():
    return {'hypothesis':'Reversion hypothesis', 'proposed_strategy_changes':[{'name':'Mean reversion','program':program().model_dump()}]}


def setup_round(monkeypatch, app):
    import core.ai.research_context_generator as generator
    loop = AutonomousResearchLoop(app)
    loop.configure(ResearchLoopConfig(enabled=True, symbols=['BTC/USDT']))
    monkeypatch.setattr(ev, 'prepare_history', AsyncMock(return_value=(frame(), {'ready':True})))
    model = AsyncMock(return_value=output())
    monkeypatch.setattr(generator, 'generate_research_context', model)
    return loop, model


def test_data_failure_does_not_charge_model_budget_and_backs_off(app, monkeypatch):
    loop, model = setup_round(monkeypatch, app)
    monkeypatch.setattr(ev, 'prepare_history', AsyncMock(side_effect=ev.ResearchStageError('data_not_ready', 'missing data')))
    asyncio.run(loop.tick())
    assert loop.status()['attempts_today'] == 0
    assert loop.state['status'] == 'data_blocked'
    assert loop.state['last_error_code'] == 'data_not_ready'
    model.assert_not_awaited()
    asyncio.run(loop.tick())
    assert ev.prepare_history.await_count == 1


def test_completed_round_persists_feedback_and_does_not_claim_champion(app, monkeypatch):
    loop, model = setup_round(monkeypatch, app)
    monkeypatch.setattr(ev, 'evaluate_program', lambda *a, **kw: good_metrics())
    asyncio.run(loop.tick())
    result = loop.status()
    assert result['completed_rounds'] == 1
    assert result['rounds'][0]['backtest_runs'] == 3
    assert result['champions'] == {}
    assert result['observations'][0]['status'] == 'observing'
    candidate = next(iter(app.state.ai_candidate_registry.rows.values()))
    assert candidate.promotion.decision == 'reject'
    assert candidate.metadata['manual_register_required']
    context = model.call_args.args[0]
    assert context['market']['as_of'] < frame().index[-1].isoformat()
    restored = AutonomousResearchLoop(app)
    assert restored.state['last_feedback'] == loop.state['last_feedback']
    # A subsequent round receives the prior measured reasons/metrics.
    asyncio.run(restored.tick(force=True))
    assert model.call_args.args[0]['previous_research_lessons']


def test_daily_cap_survives_restart_and_manual_run(app, monkeypatch):
    loop, model = setup_round(monkeypatch, app)
    loop.configure(ResearchLoopConfig(enabled=True, max_rounds_per_day=1))
    loop.state['rounds'] = [{'started_at':ev.utc_now().isoformat(), 'status':'failed'}]
    loop._save()
    restored = AutonomousResearchLoop(app)
    asyncio.run(restored.tick(force=True))
    assert restored.state['status'] == 'daily_budget_reached'
    assert restored.status()['next_run_at'] == restored.status()['budget_reset_at']
    model.assert_not_awaited()


def test_pause_during_model_call_prevents_publication(app, monkeypatch):
    import core.ai.research_context_generator as generator
    loop, _ = setup_round(monkeypatch, app)
    async def pause(*args, **kwargs):
        loop.configure(ResearchLoopConfig(enabled=False))
        return output()
    monkeypatch.setattr(generator, 'generate_research_context', pause)
    asyncio.run(loop.tick())
    assert not app.state.ai_candidate_registry.rows
    assert loop.state['rounds'][-1]['status'] == 'cancelled_before_evaluation'
    assert not loop.state['config']['enabled']


def test_failed_generation_has_typed_failure_and_consumes_attempt(app, monkeypatch):
    from core.ai.research_context_generator import ResearchGenerationError
    loop, model = setup_round(monkeypatch, app)
    model.side_effect = ResearchGenerationError('provider_http_429', 'rate limited')
    asyncio.run(loop.tick())
    assert loop.status()['attempts_today'] == 1
    assert loop.state['rounds'][-1]['error_code'] == 'provider_http_429'
    assert not app.state.ai_candidate_registry.rows


def test_real_evaluation_has_disjoint_windows_and_hard_run_limit(monkeypatch):
    samples = []
    original = ev.evaluate_program
    def capture(program, data, *args, **kwargs):
        samples.append(data.index)
        return original(program, data, *args, **kwargs)
    monkeypatch.setattr(ev, 'evaluate_program', capture)
    drafts = [StrategyDraft(name='one', program=program()), StrategyDraft(name='same', program=program())]
    other = program().model_copy(deep=True)
    other.entry_conditions[0].right = -.5
    drafts.append(StrategyDraft(name='other', program=other))
    report = ev.evaluate_drafts(drafts, frame(), '1h', 3, 5, 10)
    assert len(samples) == report['backtest_runs'] == 3
    assert samples[0].max() < samples[1].min()
    assert samples[1].equals(samples[2])
    assert report['candidates'][1]['reasons'] == ['duplicate_program']
    assert report['candidates'][2]['reasons'] == ['evaluation_budget']


def test_cost_sensitive_strategy_cannot_enter_observation(monkeypatch):
    monkeypatch.setattr(ev, 'evaluate_program', lambda *args, stress=False, **kwargs: good_metrics(total_return=-1 if stress else 2))
    report = ev.evaluate_drafts([StrategyDraft(name='costly', program=program())], frame(), '1h', 8, 5, 10)
    assert report['candidates'][0]['accepted'] is False
    assert 'cost_sensitive' in report['candidates'][0]['reasons']


def test_open_candle_and_pre_freeze_candles_are_excluded():
    clean = ev.clean_frame(frame(), '1h', datetime(2026,9,9,8,30,tzinfo=timezone.utc))
    assert clean.index[-1] == pd.Timestamp('2026-09-09T07:00Z')


def test_forward_observation_uses_only_future_bars_and_retires_loser(app, monkeypatch):
    import core.research.strategy_research as research
    loop, _ = setup_round(monkeypatch, app)
    now = datetime(2026,9,9,9,tzinfo=timezone.utc)
    frozen = now - timedelta(hours=101)
    loop.state['observations'] = [{'candidate_id':'c', 'scope':'BTC/USDT|1h', 'symbol':'BTC/USDT','timeframe':'1h',
        'program':program().model_dump(), 'frozen_at':frozen.isoformat(), 'status':'observing', 'min_bars':72,'min_trades':5,'max_drawdown_pct':10,'max_days':30}]
    monkeypatch.setattr(ev, 'utc_now', lambda:now)
    monkeypatch.setattr(research, '_load_research_timeframe_df', AsyncMock(return_value=frame()))
    captured = []
    def evaluate(p, data, timeframe):
        captured.append(data.index.min())
        return good_metrics()
    monkeypatch.setattr(ev, 'evaluate_program', evaluate)
    asyncio.run(loop._observe(ResearchLoopConfig()))
    assert captured[0] >= frozen
    assert loop.state['champions']['BTC/USDT|1h']['candidate_id'] == 'c'
    # The next completed bar reports a real risk failure, removing the champion.
    loop.state['observations'][0]['last_evaluated_bar'] = None
    monkeypatch.setattr(ev, 'evaluate_program', lambda *args: good_metrics(total_return=-2, max_drawdown=12))
    asyncio.run(loop._observe(ResearchLoopConfig()))
    assert loop.state['observations'][0]['status'] == 'rejected'
    assert loop.state['champions'] == {}


def test_invalid_symbol_cannot_become_a_storage_path():
    with pytest.raises(ValueError):
        ResearchLoopConfig(symbols=['../../secrets'])


def test_bad_draft_does_not_discard_valid_sibling(app, monkeypatch):
    loop, model = setup_round(monkeypatch, app)
    model.return_value['proposed_strategy_changes'].insert(0, {'program': {'indicators': []}})
    asyncio.run(loop.tick())
    assert loop.state['rounds'][-1]['status'] == 'completed'
    assert loop.state['rounds'][-1]['backtest_runs'] == 3
    assert loop.state['last_feedback']['rejection_counts']['invalid_program'] == 1
    assert len(app.state.ai_candidate_registry.rows) == 2


def test_refresh_retries_once_and_closes_bounded_failure(monkeypatch):
    refresh = AsyncMock(side_effect=[ConnectionError(), None])
    monkeypatch.setattr(ev, '_refresh', refresh)
    monkeypatch.setattr(ev.asyncio, 'sleep', AsyncMock())
    now = ev.utc_now()
    asyncio.run(ev.refresh_history('gate', 'ETH/USDT', '1h', now-timedelta(days=30), now))
    assert refresh.await_count == 2
    refresh.side_effect = ConnectionError()
    with pytest.raises(ev.ResearchStageError, match='ConnectionError'):
        asyncio.run(ev.refresh_history('gate', 'ETH/USDT', '1h', now-timedelta(days=30), now))
    assert refresh.await_count == 4


def test_existing_observation_monitored_even_after_daily_budget(app, monkeypatch):
    loop, model = setup_round(monkeypatch, app)
    loop.configure(ResearchLoopConfig(enabled=True, max_rounds_per_day=1))
    loop.state['rounds'] = [{'started_at': ev.utc_now().isoformat(), 'status': 'failed'}]
    monitor = AsyncMock()
    monkeypatch.setattr(loop, '_observe', monitor)
    asyncio.run(loop.tick())
    monitor.assert_awaited_once()
    model.assert_not_awaited()


def test_expired_observation_removes_recommendation_without_evaluation(app, monkeypatch):
    loop, _ = setup_round(monkeypatch, app)
    loop.state['observations'] = [{'candidate_id': 'expired', 'scope': 'BTC/USDT|1h', 'symbol': 'BTC/USDT',
        'timeframe': '1h', 'status': 'qualified', 'data_status': 'ready', 'max_days': 30,
        'frozen_at': (ev.utc_now() - timedelta(days=31)).isoformat()}]
    loop.state['champions'] = {'BTC/USDT|1h': {'candidate_id': 'expired'}}
    asyncio.run(loop._observe(ResearchLoopConfig()))
    assert loop.state['observations'][0]['status'] == 'expired'
    assert loop.state['champions'] == {}
    assert loop.state['monitoring_backtest_runs'] == 0


def test_program_allows_ohlcv_but_rejects_unresolvable_indicator_alias():
    p = program()
    p.exit_conditions[0].left = 'close'
    ev.validate_program(p)
    p.indicators[0].name = 'Z score'
    with pytest.raises(ev.ResearchStageError):
        ev.validate_program(p)


def test_provider_quota_is_not_a_rate_limit_and_does_not_expose_body():
    from core.ai import research_context_generator as gen
    gen._record_provider_error(429, '{"error":{"code":"insufficient_user_quota","balance":"private-value"}}')
    code, message = gen._last_generation_error.get()
    assert code == 'provider_quota_exhausted'
    assert 'private-value' not in message
    gen._record_provider_error(429, '{"error":{"code":"rate_limit_exceeded"}}')
    assert gen._last_generation_error.get()[0] == 'provider_rate_limited'


def test_manual_route_respects_cap_and_duplicate_request(app, monkeypatch):
    from fastapi import HTTPException
    from web.api.ai_research import run_ai_research_loop_once
    from core.ai import autonomous_research_loop as public
    loop, model = setup_round(monkeypatch, app)
    monkeypatch.setattr(public, 'get_research_loop', lambda app: loop)
    request = SimpleNamespace(app=app)
    async def run():
        first = await run_ai_research_loop_once(request)
        second = await run_ai_research_loop_once(request)
        assert first['requested'] and not second['requested']
        await loop.task
        loop.configure(ResearchLoopConfig(enabled=True, max_rounds_per_day=1))
        with pytest.raises(HTTPException) as error:
            await run_ai_research_loop_once(request)
        assert error.value.status_code == 409
    asyncio.run(run())
    assert model.await_count == 1


def test_forward_monitoring_and_comparisons_share_a_hard_budget(app, monkeypatch):
    import core.research.strategy_research as research
    loop, _ = setup_round(monkeypatch, app)
    now = datetime(2026, 9, 9, 9, tzinfo=timezone.utc)
    monkeypatch.setattr(ev, 'utc_now', lambda: now)
    monkeypatch.setattr(research, '_load_research_timeframe_df', AsyncMock(return_value=frame()))
    calls = []
    def evaluate(p, data, tf):
        calls.append(data.index)
        return good_metrics()
    monkeypatch.setattr(ev, 'evaluate_program', evaluate)
    for i in range(10):
        loop.state['observations'].append({'candidate_id': str(i), 'scope': 'BTC/USDT|1h', 'symbol': 'BTC/USDT',
            'timeframe': '1h', 'program': program().model_dump(), 'status': 'observing',
            'frozen_at': (now - timedelta(hours=100-i)).isoformat(), 'min_bars':72,
            'min_trades':5, 'max_drawdown_pct':10, 'max_days':30})
    asyncio.run(loop._observe(ResearchLoopConfig(max_backtest_runs=8)))
    assert len(calls) == loop.state['monitoring_backtest_runs'] == 8
    assert calls[-1].equals(calls[-2])
    assert calls[-1].min() >= now - timedelta(hours=99)


def test_stale_forward_data_repairs_with_cooldown_and_withdraws_champion(app, monkeypatch):
    import core.research.strategy_research as research
    loop, _ = setup_round(monkeypatch, app)
    now = datetime(2026, 9, 9, 9, tzinfo=timezone.utc)
    monkeypatch.setattr(ev, 'utc_now', lambda: now)
    monkeypatch.setattr(research, '_load_research_timeframe_df', AsyncMock(return_value=frame(end='2026-09-09T03:00Z')))
    refresh = AsyncMock(side_effect=ev.ResearchStageError('data_refresh_failed', 'unavailable'))
    monkeypatch.setattr(ev, 'refresh_history', refresh)
    loop.state['observations'] = [{'candidate_id': 'c', 'scope': 'BTC/USDT|1h', 'symbol': 'BTC/USDT',
        'timeframe': '1h', 'program': program().model_dump(), 'status': 'qualified',
        'frozen_at': (now - timedelta(hours=100)).isoformat(), 'min_bars':72,
        'min_trades':5, 'max_drawdown_pct':10, 'max_days':30}]
    loop.state['champions'] = {'BTC/USDT|1h': {'candidate_id': 'c'}}
    asyncio.run(loop._observe(ResearchLoopConfig()))
    assert loop.state['champions'] == {}
    assert loop.state['observations'][0]['data_status'] == 'stale'
    asyncio.run(loop._observe(ResearchLoopConfig()))
    refresh.assert_awaited_once()


def test_paused_manual_round_does_not_enable_automatic_research(app, monkeypatch):
    loop, _ = setup_round(monkeypatch, app)
    loop.configure(ResearchLoopConfig(enabled=False))
    asyncio.run(loop.tick(force=True))
    assert loop.state['rounds'][-1]['status'] == 'completed'
    assert loop.status()['status'] == 'paused'
    assert not loop.status()['config']['enabled']
