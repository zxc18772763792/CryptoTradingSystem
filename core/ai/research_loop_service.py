"""Persistent bounded research with forward observation and failure feedback.

All evaluations are research-only. This service never registers strategies,
starts trading, or changes account/execution settings.
"""
from __future__ import annotations

import asyncio
import json
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator

from config.settings import settings
from core.ai import research_loop_evaluation as ev
from core.ai.proposal_schemas import ResearchProposal, StrategyDraft, ProposalValidationSummary
from core.research.experiment_schemas import ExperimentSpec, ExperimentRun, StrategyCandidate, PromotionDecision


class ResearchLoopConfig(BaseModel):
    model_config = ConfigDict(extra='forbid')
    enabled: bool = False
    interval_seconds: int = Field(default=3600, ge=600, le=86400)
    max_rounds_per_day: int = Field(default=6, ge=1, le=24)
    symbols: list[str] = Field(default_factory=lambda: ['BTC/USDT', 'ETH/USDT'], min_length=1, max_length=8)
    timeframes: list[Literal['15m', '1h', '4h']] = Field(default_factory=lambda: ['1h'], min_length=1, max_length=3)
    days: int = Field(default=30, ge=7, le=180)
    max_backtest_runs: int = Field(default=24, ge=8, le=80)
    model_timeout_seconds: int = Field(default=180, ge=30, le=300)
    auto_refresh_data: bool = True
    min_validation_trades: int = Field(default=5, ge=3, le=100)
    max_drawdown_pct: float = Field(default=10, gt=0, le=30)
    forward_min_bars: int = Field(default=72, ge=50, le=1000)
    forward_min_trades: int = Field(default=5, ge=3, le=100)
    forward_max_days: int = Field(default=30, ge=7, le=90)

    @field_validator('symbols')
    @classmethod
    def validate_symbols(cls, values):
        values = list(dict.fromkeys(v.strip().upper() for v in values))
        if any(not re.fullmatch(r'[A-Z0-9]{2,16}/USDT', v) for v in values):
            raise ValueError('研究币种须使用 BTC/USDT 格式')
        return values


class AutonomousResearchLoop:
    def __init__(self, app):
        self.app, self.lock, self.revision, self.task = app, asyncio.Lock(), 0, None
        self.path = Path(app.state.ai_research_dir) / 'autonomous_loop.json'
        self.state = {'config': ResearchLoopConfig().model_dump(), 'rounds': [], 'status': 'paused', 'observations': [], 'champions': {}}
        if self.path.exists():
            self.state.update(json.loads(self.path.read_text(encoding='utf-8')))
            self.state['config'] = ResearchLoopConfig.model_validate(self.state['config']).model_dump()
            if self.state.get('status') in {'generating', 'evaluating', 'checking_data', 'observing'}:
                self.state.update(status='interrupted', last_error='上次工作中断，已消耗预算和历史结果保留')
                for row in self.state['rounds']:
                    if row.get('status') in {'generating', 'evaluating'}:
                        row.update(status='interrupted', error_code='interrupted')
                        self._retire_incomplete_proposal(row)
        self.state['version'] = 2

    def _retire_incomplete_proposal(self, row):
        registry = getattr(self.app.state, 'ai_proposal_registry', None)
        proposal = registry.get(row.get('proposal_id', '')) if registry else None
        if proposal and proposal.status == 'research_running' and proposal.metadata.get('bounded_loop_v2'):
            proposal.status = 'rejected'
            proposal.metadata['last_research_error'] = '研究循环中断，未将未完成结果选为候选'
            registry.save(proposal)

    def _save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_name(f'{self.path.name}.{uuid4().hex}.tmp')
        tmp.write_text(json.dumps(self.state, ensure_ascii=False, indent=2, allow_nan=False), encoding='utf-8')
        tmp.replace(self.path)

    def status(self):
        now = ev.utc_now()
        today_rounds = [r for r in self.state['rounds'] if str(r.get('started_at', '')).startswith(now.date().isoformat())]
        attempts = sum(not r.get('budget_reset_id') for r in today_rounds)
        reset = datetime.combine(now.date() + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
        payload = json.loads(json.dumps(self.state))
        payload.update(rounds=payload['rounds'][-10:], attempts_today=attempts, total_attempts_today=len(today_rounds), budget_reset_at=reset.isoformat(),
                       configured_model=settings.AI_RESEARCH_MODEL, busy=self.lock.locked(),
                       completed_rounds=sum(r.get('status') == 'completed' for r in self.state['rounds']),
                       round_count=len(self.state['rounds']),
                       scope='AI假设 → 有限回测 → 成本压力验证 → 新增行情观察 → 反馈；不自动注册或下单')
        if attempts >= self.state['config']['max_rounds_per_day']:
            payload['next_run_at'] = reset.isoformat()
        return payload

    def reset_daily_budget(self):
        """Explicit operator reset; keep attempts, outcomes and reset audit intact."""
        if self.lock.locked() or (self.task and not self.task.done()):
            raise ev.ResearchStageError('research_busy', '研究正在执行，请完成后再重置额度')
        now = ev.utc_now()
        reset_id, released = uuid4().hex, 0
        for row in self.state['rounds']:
            if str(row.get('started_at', '')).startswith(now.date().isoformat()) and not row.get('budget_reset_id'):
                row['budget_reset_id'] = reset_id
                released += 1
        if released:
            self.state.setdefault('budget_resets', []).append({
                'reset_id': reset_id, 'reset_at': now.isoformat(), 'released_attempts': released,
            })
        self.state.update(status='waiting' if self.state['config']['enabled'] else 'paused',
                          next_run_at=now.isoformat(), last_error=None, last_error_code=None)
        self._save()
        return self.status()

    def configure(self, config):
        self.revision += 1
        self.state.update(config=config.model_dump(), status='waiting' if config.enabled else 'paused')
        self._save()
        return self.status()

    def request_run(self):
        if self.lock.locked() or (self.task and not self.task.done()):
            return {**self.status(), 'requested': False}
        self.task = asyncio.create_task(self.tick(force=True), name='ai_research_loop_manual')
        return {**self.status(), 'requested': True}

    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)

    def _record_candidates(self, proposal, report, record, cfg):
        now, accepted_ids = ev.utc_now(), []
        experiment_id = f'loop-experiment-{record["round_id"]}'
        self.app.state.ai_experiment_registry.save(ExperimentSpec(
            experiment_id=experiment_id, proposal_id=proposal.proposal_id, created_at=now,
            exchange='binance', symbol=record['symbol'], research_mode='autonomous_draft',
            timeframes=[record['timeframe']], days=cfg.days, status='completed',
            metadata={'autonomous_research_loop': True, 'bounded_evaluation': True}))
        for index, row in enumerate(report['candidates']):
            candidate_id = f'loop-candidate-{record["round_id"]}-{index}'
            row['candidate_id'] = candidate_id
            reasons = list(row.get('reasons', [])) or ['forward_observation_pending']
            metrics = row.get('validation', {})
            candidate = StrategyCandidate(
                candidate_id=candidate_id, proposal_id=proposal.proposal_id, experiment_id=experiment_id,
                created_at=now, strategy=row['name'], timeframe=record['timeframe'], symbol=record['symbol'],
                score=row.get('score', 0), params=dict((row.get('program') or {}).get('params', {})),
                validation_summary=ProposalValidationSummary(computed_at=now, decision='reject', reasons=reasons,
                    metrics={'best': metrics, 'development_validation': report['validation_role']},
                    effective_sharpe_source='development_validation', score_explanation='历史开发验证参与选优；独立证据须等待冻结后新增行情',
                    outcome_type='forward_observation_pending' if row.get('accepted') else 'research_rejected'),
                promotion=PromotionDecision(candidate_id=candidate_id, decision='reject', reason='; '.join(reasons), created_at=now),
                metadata={'autonomous_research_loop': True, 'loop_round_id': record['round_id'],
                          'loop_evaluation': row, 'manual_register_required': True, 'strategy_program': row.get('program'),
                          'best': metrics, 'top_results': [metrics] if metrics else [], 'research_mode': 'autonomous_draft',
                          'strategy_drafts': [d.model_dump(mode='json') for d in proposal.strategy_drafts if d.draft_id == row.get('draft_id')],
                          'research_only': True})
            self.app.state.ai_candidate_registry.save(candidate)
            if row.get('accepted'):
                scope = f'{candidate.symbol}|{candidate.timeframe}'
                active = [o for o in self.state['observations'] if o['scope'] == scope and o['status'] in {'observing', 'qualified'}]
                if any(o['fingerprint'] == row['fingerprint'] for o in active) or len(active) >= 3:
                    row['observation_note'] = '已有相同程序或观察队列已满，保留结果供反馈'
                    candidate.validation_summary.outcome_type = 'observation_not_admitted'
                    candidate.validation_summary.reasons = ['observation_duplicate_or_full']
                    candidate.promotion.reason = row['observation_note']
                    self.app.state.ai_candidate_registry.save(candidate)
                    continue
                self.state['observations'].append({
                    'candidate_id': candidate_id, 'scope': scope, 'symbol': candidate.symbol, 'timeframe': candidate.timeframe,
                    'program': row['program'], 'fingerprint': row['fingerprint'], 'frozen_at': now.isoformat(),
                    'status': 'observing', 'validation': metrics, 'last_evaluated_bar': None,
                    'min_bars': cfg.forward_min_bars, 'min_trades': cfg.forward_min_trades,
                    'max_drawdown_pct': cfg.max_drawdown_pct, 'max_days': cfg.forward_max_days})
                accepted_ids.append(candidate_id)
        self.app.state.ai_experiment_run_registry.save(ExperimentRun(
            run_id=f'loop-run-{record["round_id"]}', experiment_id=experiment_id,
            started_at=datetime.fromisoformat(record['started_at']), finished_at=now, status='completed', result=report))
        proposal.latest_experiment_id = experiment_id
        proposal.latest_candidate_id = accepted_ids[0] if accepted_ids else None
        proposal.status, proposal.updated_at = ('validated' if accepted_ids else 'rejected'), now
        proposal.metadata.update(loop_report=report, manual_register_required=True)
        self.app.state.ai_proposal_registry.save(proposal)
        return accepted_ids

    def _save_observation_candidate(self, item):
        candidate = self.app.state.ai_candidate_registry.get(item['candidate_id'])
        if not candidate:
            return
        passed = item['status'] == 'qualified' and item.get('data_status') == 'ready'
        candidate.metadata['forward_observation'] = dict(item)
        candidate.validation_summary.reasons = [item.get('reason') or item['status']]
        candidate.validation_summary.decision = 'paper' if passed else 'reject'
        candidate.validation_summary.outcome_type = 'forward_validated' if passed else f'forward_{item["status"]}'
        candidate.validation_summary.computed_at = ev.utc_now()
        candidate.validation_summary.metrics['forward_observation'] = item.get('metrics', {})
        if passed:
            candidate.validation_summary.oos_score = item.get('metrics', {}).get('sharpe_ratio')
            candidate.validation_summary.effective_sharpe_source = 'forward_observation'
        candidate.promotion = PromotionDecision(candidate_id=candidate.candidate_id, decision='paper' if passed else 'reject',
            reason=item.get('reason') or '等待新增行情验证', created_at=ev.utc_now())
        self.app.state.ai_candidate_registry.save(candidate)

    async def _observe(self, cfg):
        import pandas as pd
        from core.research.strategy_research import _load_research_timeframe_df
        remaining, refreshes = cfg.max_backtest_runs, 0
        refreshed_scopes = set()
        refresh_cooldowns = self.state.setdefault('observation_refresh_due', {})
        observations = self.state['observations']
        offset = self.state.get('observation_cursor', 0) % max(1, len(observations))
        ordered = observations[offset:] + observations[:offset]
        self.state['observation_cursor'] = offset + max(1, cfg.max_backtest_runs // 2)
        for item in ordered:
            if item['status'] not in {'observing', 'qualified'}:
                continue
            frozen, now = datetime.fromisoformat(item['frozen_at']), ev.utc_now()
            age = (now - frozen).total_seconds()
            if age >= item['max_days'] * 86400:
                item.update(status='expired', reason='观察期已结束，重新研究当前市场')
                self._save_observation_candidate(item)
                continue
            frame = ev.clean_frame(await _load_research_timeframe_df('binance', item['symbol'], item['timeframe'], frozen, now), item['timeframe'])
            frame = frame.loc[frame.index >= pd.Timestamp(frozen)] if not frame.empty else frame
            stale = frame.empty or (pd.Timestamp(now) - frame.index[-1]).total_seconds() > ev.SECONDS[item['timeframe']] * 2
            has_gaps = not frame.empty and (frame.index.to_series().diff().dt.total_seconds() > ev.SECONDS[item['timeframe']] * 1.5).any()
            refresh_due = refresh_cooldowns.get(item['scope'], '') <= now.isoformat()
            if (stale or has_gaps) and age > ev.SECONDS[item['timeframe']] * 2 and cfg.auto_refresh_data and refresh_due and refreshes < 2 and item['scope'] not in refreshed_scopes:
                refreshed_scopes.add(item['scope'])
                refreshes += 1
                item['next_refresh_at'] = (now + timedelta(minutes=10)).isoformat()
                refresh_cooldowns[item['scope']] = item['next_refresh_at']
                try:
                    await ev.refresh_history('binance', item['symbol'], item['timeframe'], frozen if frame.empty or has_gaps else frame.index[-1].to_pydatetime(), now)
                    frame = ev.clean_frame(await _load_research_timeframe_df('binance', item['symbol'], item['timeframe'], frozen, now), item['timeframe'])
                    frame = frame.loc[frame.index >= pd.Timestamp(frozen)] if not frame.empty else frame
                except ev.ResearchStageError:
                    item['reason'] = '新增行情补数失败，稍后重试'
            item['bars'] = len(frame)
            if frame.empty or (pd.Timestamp(now) - frame.index[-1]).total_seconds() > ev.SECONDS[item['timeframe']] * 2:
                item['data_status'] = 'stale'
                self._save_observation_candidate(item)
                continue
            if (frame.index.to_series().diff().dt.total_seconds().dropna() > ev.SECONDS[item['timeframe']] * 1.5).any():
                item.update(data_status='gaps', reason='观察行情有缺口，暂不推荐')
                self._save_observation_candidate(item)
                continue
            item['data_status'] = 'ready'
            if len(frame) < item['min_bars']:
                continue
            latest = frame.index[-1].isoformat()
            if item.get('last_evaluated_bar') == latest:
                continue
            if remaining <= 2:
                item['data_status'] = 'evaluation_pending'
                self._save_observation_candidate(item)
                continue
            try:
                remaining -= 1
                metrics = await asyncio.to_thread(ev.evaluate_program, item['program'], frame, item['timeframe'])
                item.update(metrics=metrics, last_evaluated_bar=latest)
                reasons = ev.rejection_reasons(metrics, item['min_trades'], item['max_drawdown_pct'])
                if metrics['total_trades'] < item['min_trades'] and age < item['max_days'] * 86400 and metrics['max_drawdown'] <= item['max_drawdown_pct']:
                    item['reason'] = '等待足够的完整交易样本'
                    self._save_observation_candidate(item)
                    continue
                item['status'] = 'rejected' if reasons else 'qualified'
                item['reason'] = '; '.join(reasons) if reasons else '新增行情验证通过，仍需人工决定是否注册'
            except Exception as exc:
                item.update(data_status='evaluation_error', reason=f'观察计算失败（{type(exc).__name__}），暂不推荐')
            self._save_observation_candidate(item)
        remaining = await self._select_champions(remaining)
        self.state['monitoring_backtest_runs'] = cfg.max_backtest_runs - remaining

    async def _select_champions(self, remaining=24):
        import pandas as pd
        from core.research.strategy_research import _load_research_timeframe_df
        qualified = [o for o in self.state['observations'] if o['status'] == 'qualified' and o.get('data_status') == 'ready']
        for scope, champion in list(self.state['champions'].items()):
            if not any(o['candidate_id'] == champion['candidate_id'] for o in qualified):
                self.state['champions'].pop(scope, None)
        for item in qualified:
            incumbent = self.state['champions'].get(item['scope'])
            if incumbent and incumbent['candidate_id'] == item['candidate_id']:
                continue
            if incumbent:
                old = next(o for o in qualified if o['candidate_id'] == incumbent['candidate_id'])
                start = max(datetime.fromisoformat(item['frozen_at']), datetime.fromisoformat(old['frozen_at']))
                frame = ev.clean_frame(await _load_research_timeframe_df('binance', item['symbol'], item['timeframe'], start, ev.utc_now()), item['timeframe'])
                frame = frame.loc[frame.index >= pd.Timestamp(start)] if not frame.empty else frame
                if len(frame) < max(item['min_bars'], old['min_bars']):
                    continue
                signature = f'{old["candidate_id"]}|{frame.index[-1].isoformat()}'
                if item.get('comparison_signature') == signature:
                    continue
                if remaining < 2:
                    continue
                remaining -= 2
                challenger = await asyncio.to_thread(ev.evaluate_program, item['program'], frame, item['timeframe'])
                defending = await asyncio.to_thread(ev.evaluate_program, old['program'], frame, old['timeframe'])
                item.update(comparison_signature=signature, comparison={'start': frame.index[0].isoformat(), 'end': frame.index[-1].isoformat(), 'challenger': challenger, 'incumbent': defending})
                if ev.rejection_reasons(challenger, item['min_trades'], item['max_drawdown_pct']):
                    continue
                if challenger['total_return'] <= defending['total_return'] + .2 or challenger['max_drawdown'] > defending['max_drawdown'] + .5:
                    continue
            self.state['champions'][item['scope']] = {'candidate_id': item['candidate_id'], 'selected_at': ev.utc_now().isoformat(),
                'status': 'forward_validated', 'manual_registration_required': True}
        return remaining

    async def tick(self, force=False):
        if self.lock.locked():
            return
        async with self.lock:
            cfg = ResearchLoopConfig.model_validate(self.state['config'])
            if not cfg.enabled and not force:
                return
            record, revision = None, self.revision
            try:
                await self._observe(cfg)
                now = ev.utc_now()
                if self.status()['attempts_today'] >= cfg.max_rounds_per_day:
                    self.state['status'] = 'daily_budget_reached'
                    return
                due = self.state.get('next_run_at')
                if due and now < datetime.fromisoformat(due) and not force:
                    return
                if any(j.get('status') in {'pending', 'running'} for j in self.app.state.research_jobs.values()):
                    self.state['status'] = 'waiting_for_research'
                    return
                index = len(self.state['rounds'])
                symbol = cfg.symbols[index % len(cfg.symbols)]
                timeframe = cfg.timeframes[(index // len(cfg.symbols)) % len(cfg.timeframes)]
                self.state['status'] = 'checking_data'
                self._save()
                frame, readiness = await ev.prepare_history(symbol, timeframe, cfg.days, cfg.auto_refresh_data)
                self.state['data_readiness'] = {**readiness, 'symbol': symbol, 'timeframe': timeframe, 'checked_at': now.isoformat()}
                if revision != self.revision:
                    return
                record = {'round_id': uuid4().hex[:12], 'started_at': ev.utc_now().isoformat(), 'status': 'generating', 'symbol': symbol, 'timeframe': timeframe}
                self.state['rounds'].append(record)
                self.state['rounds'] = self.state['rounds'][-200:]
                self.state.update(status='generating', last_error=None, last_error_code=None,
                                  next_run_at=(now + timedelta(seconds=cfg.interval_seconds)).isoformat())
                self._save()
                train, _ = ev.split_development(frame)
                market = ev.market_summary(train, symbol, timeframe)
                current_market = ev.market_summary(frame.tail(96), symbol, timeframe)
                lessons = [r['feedback'] for r in self.state['rounds'][:-1] if r.get('feedback') and r.get('symbol') == symbol and r.get('timeframe') == timeframe][-4:]
                context = {'market': market, 'current_market': current_market, 'previous_research_lessons': lessons,
                           'forward_observations': [{k: o.get(k) for k in ('candidate_id', 'status', 'reason', 'metrics')} for o in self.state['observations'] if o['symbol'] == symbol and o['timeframe'] == timeframe][-4:],
                           'evaluation_contract': '当前市场摘要和历史开发验证均用于构思、选优和反馈，历史验证不是独立样本；最终独立观察只使用候选冻结后新产生的行情。指标最多96周期，名称只用小写英文下划线。'}
                from core.ai.research_context_generator import generate_research_context
                output = await asyncio.wait_for(generate_research_context(context,
                    goals='结合最新市场状态、以往程序和实测淘汰原因提出1至2个可证伪假设。优先针对失败原因修改指标、阈值或退出条件，保留不同思路。每个草案只需name、简短thesis和完整program，不重复输出features、entry_logic、exit_logic、risk_logic或parameter_space。program须有明确退出条件。避免原样重复失败组合，不承诺收益。',
                    timeout=cfg.model_timeout_seconds, raise_on_error=True), timeout=cfg.model_timeout_seconds * 2 + 30)
                if revision != self.revision:
                    record['status'] = 'cancelled_before_evaluation'
                    return
                if not output or not output.get('hypothesis') or not output.get('proposed_strategy_changes'):
                    raise ev.ResearchStageError('invalid_model_output', '模型未返回有效假设与可执行草案')
                drafts, invalid_rows = [], []
                for i, change in enumerate(output['proposed_strategy_changes'][:3]):
                    try:
                        program = ev.validate_program(change.get('program') or {})
                    except (ValueError, ev.ResearchStageError, AttributeError):
                        invalid_rows.append({'name': f'AI草案{i + 1}', 'accepted': False, 'reasons': ['invalid_program']})
                        continue
                    drafts.append(StrategyDraft(draft_id=f'{record["round_id"]}-{i}', name=change.get('name') or program.name or f'AI草案{i + 1}',
                        thesis=change.get('thesis') or output['hypothesis'], rationale=change.get('rationale', ''), program=program, source='llm'))
                proposal = ResearchProposal(proposal_id=f'loop-proposal-{record["round_id"]}', created_at=now, updated_at=now,
                    source='ai', research_mode='autonomous_draft', status='research_running', thesis=output['hypothesis'],
                    market_regime=current_market['regime'], target_symbols=[symbol], target_timeframes=[timeframe], strategy_drafts=drafts,
                    metadata={'autonomous_research_loop': True, 'bounded_loop_v2': True, 'generation_method': 'llm', 'llm_used': True,
                              'loop_generation': output.get('_generation', {}), 'loop_market': current_market, 'research_only': True})
                record.update(status='evaluating', proposal_id=proposal.proposal_id, generation=output.get('_generation', {}))
                self.state['status'] = 'evaluating'
                self._save()
                self.app.state.ai_proposal_registry.save(proposal)
                report = await asyncio.to_thread(ev.evaluate_drafts, drafts, frame, timeframe, cfg.max_backtest_runs, cfg.min_validation_trades, cfg.max_drawdown_pct)
                report['candidates'].extend(invalid_rows)
                if revision != self.revision:
                    self._retire_incomplete_proposal(record)
                    record['status'] = 'cancelled_before_publish'
                    return
                accepted = self._record_candidates(proposal, report, record, cfg)
                feedback = {'hypothesis': output['hypothesis'], 'outcome': 'forward_observation' if accepted else 'no_qualified_candidate',
                            'rejection_counts': dict(Counter(reason for row in report['candidates'] for reason in row.get('reasons', []))),
                            'evaluations': [{'name': row['name'], 'thesis': row.get('thesis'), 'program': row.get('program'), 'reasons': row.get('reasons'),
                                **{stage: {k: row.get(stage, {}).get(k) for k in ('total_return', 'max_drawdown', 'sharpe_ratio', 'total_trades')} for stage in ('validation', 'stress')}} for row in report['candidates']]}
                record.update(status='completed', finished_at=ev.utc_now().isoformat(), feedback=feedback,
                              backtest_runs=report['backtest_runs'], observation_candidate_ids=accepted, outcome=feedback['outcome'])
                self.state.update(status='waiting', last_feedback=feedback)
            except asyncio.CancelledError:
                if record:
                    record.update(status='interrupted', error_code='interrupted')
                    self._retire_incomplete_proposal(record)
                self.state['status'] = 'interrupted'
                raise
            except Exception as exc:
                code = getattr(exc, 'code', 'research_timeout' if isinstance(exc, asyncio.TimeoutError) else 'implementation_error')
                message = str(exc)[:500] if getattr(exc, 'code', None) else f'研究执行异常（{type(exc).__name__}）'
                if record:
                    record.update(status='failed', error=message, error_code=code)
                    self._retire_incomplete_proposal(record)
                self.state.update(status='data_blocked' if record is None else 'retry_scheduled', last_error=message,
                                  last_error_code=code, next_run_at=(ev.utc_now() + timedelta(seconds=cfg.interval_seconds)).isoformat())
            finally:
                if not self.state['config']['enabled']:
                    self.state['status'] = 'paused'
                self._save()


def get_research_loop(app):
    from core.research.orchestrator import ensure_ai_research_runtime_state
    ensure_ai_research_runtime_state(app)
    if getattr(app.state, 'autonomous_research_loop', None) is None:
        app.state.autonomous_research_loop = AutonomousResearchLoop(app)
    return app.state.autonomous_research_loop
