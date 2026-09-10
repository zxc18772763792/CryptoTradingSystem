"""Bounded research evaluation and public-history readiness for the research loop.

Development validation is used for selection and feedback, never advertised as
unseen forward evidence. Only bars arriving after a frozen candidate count toward
forward observation. This module has no registration or order execution calls.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
from datetime import datetime, timedelta, timezone

import pandas as pd

from core.ai.proposal_schemas import StrategyProgram

SECONDS = {"15m": 900, "1h": 3600, "4h": 14400}


class ResearchStageError(RuntimeError):
    def __init__(self, code, message):
        self.code = code
        super().__init__(message)


def utc_now():
    return datetime.now(timezone.utc)


def clean_frame(frame, timeframe, now=None):
    """Drop open candles and reject duplicate/invalid prices before evaluation."""
    if frame is None or frame.empty:
        return pd.DataFrame()
    frame = frame.copy()
    frame.index = pd.to_datetime(frame.index, utc=True)
    frame = frame.sort_index()
    frame = frame.loc[~frame.index.duplicated(keep="last")]
    cutoff = pd.Timestamp(now or utc_now()).floor(f'{SECONDS[timeframe]}s')
    frame = frame.loc[frame.index < cutoff]
    columns = ["open", "high", "low", "close", "volume"]
    if any(column not in frame for column in columns):
        raise ResearchStageError("data_invalid", "历史数据缺少 OHLCV 列")
    values = frame[columns].apply(pd.to_numeric, errors="coerce")
    if not values.map(math.isfinite).all().all() or (values[["open", "high", "low", "close"]] <= 0).any().any():
        raise ResearchStageError("data_invalid", "历史数据含无效价格或缺失值")
    if (values.volume < 0).any() or (values.high < values[['open', 'close', 'low']].max(axis=1)).any() or (values.low > values[['open', 'close']].min(axis=1)).any():
        raise ResearchStageError("data_invalid", "历史数据含不一致的最高/最低价或负成交量")
    frame[columns] = values
    return frame


async def _refresh(exchange, symbol, timeframe, start, end):
    # A dedicated public client avoids touching execution connections and is
    # closed even when the bounded download is cancelled.
    from core.data.historical_data import _PublicCCXTKlineConnector
    from core.data import data_storage

    client = _PublicCCXTKlineConnector(exchange)
    candles = []
    cursor = start.astimezone(timezone.utc)
    end_naive = end.replace(tzinfo=None)
    try:
        while cursor < end:
            rows = await client.get_klines(symbol=symbol, timeframe=timeframe, since=cursor, limit=500)
            if not rows:
                break
            rows = sorted(rows, key=lambda row: row.timestamp)
            last = pd.to_datetime(rows[-1].timestamp, utc=True).to_pydatetime()
            if last < cursor:
                break
            candles.extend(row for row in rows if pd.Timestamp(row.timestamp).to_pydatetime().replace(tzinfo=None) < end_naive)
            cursor = last + timedelta(milliseconds=1)
            if len(candles) > 20000:
                raise ResearchStageError("data_limit", "单次补数超过 20000 根上限")
        if candles:
            await data_storage.save_klines_to_parquet(klines=candles, exchange=exchange, symbol=symbol, timeframe=timeframe)
    finally:
        await client.close()


async def refresh_history(exchange, symbol, timeframe, start, end):
    """Two bounded attempts; public exchange outages never create model calls."""
    async def retry():
        for attempt in range(2):
            try:
                return await _refresh(exchange, symbol, timeframe, start, end)
            except ResearchStageError:
                raise
            except Exception:
                if attempt:
                    raise
                await asyncio.sleep(1)
    try:
        await asyncio.wait_for(retry(), timeout=90)
    except Exception as exc:
        raise ResearchStageError("data_refresh_failed", f"{exchange} {symbol} {timeframe} 补数失败（{type(exc).__name__}）") from exc


async def prepare_history(symbol, timeframe, days, auto_refresh=True):
    from core.research.strategy_research import _load_research_timeframe_df

    now = utc_now()
    requested_days = days
    days = max(days, math.ceil(241 * SECONDS[timeframe] / 86400))
    start = now - timedelta(days=days)
    expected = int(days * 86400 / SECONDS[timeframe])
    reports, frames = [], {}
    for exchange in ("binance", "gate"):
        frame = clean_frame(await _load_research_timeframe_df(exchange, symbol, timeframe, start, now), timeframe, now)
        stale = frame.empty or (pd.Timestamp(now) - frame.index[-1]).total_seconds() > SECONDS[timeframe] * 2
        enough = len(frame) >= max(240, int(expected * .9))
        has_gaps = not frame.empty and (frame.index.to_series().diff().dt.total_seconds() > SECONDS[timeframe] * 1.5).any()
        refreshed = False
        if (stale or not enough or has_gaps) and auto_refresh:
            refresh_start = start if not enough or has_gaps else max(start, frame.index[-1].to_pydatetime() - timedelta(seconds=SECONDS[timeframe]))
            await refresh_history(exchange, symbol, timeframe, refresh_start, now)
            refreshed = True
            frame = clean_frame(await _load_research_timeframe_df(exchange, symbol, timeframe, start, now), timeframe, now)
            stale = frame.empty or (pd.Timestamp(now) - frame.index[-1]).total_seconds() > SECONDS[timeframe] * 2
            enough = len(frame) >= max(240, int(expected * .9))
        if stale or not enough:
            raise ResearchStageError("data_not_ready", f"{exchange} {symbol} {timeframe} 数据不足或过期：{len(frame)}/{expected} 根")
        gaps = frame.index.to_series().diff().dt.total_seconds().dropna()
        if (gaps > SECONDS[timeframe] * 1.5).any():
            raise ResearchStageError("data_gaps", f"{exchange} {symbol} {timeframe} 历史K线不连续")
        frames[exchange] = frame
        reports.append({"exchange": exchange, "rows": len(frame), "end": frame.index[-1].isoformat(), "refreshed": refreshed})
    overlap = frames['binance'][['close']].join(frames['gate'][['close']], how='inner', lsuffix='_a', rsuffix='_b')
    difference = (overlap.close_a - overlap.close_b).abs() / overlap.close_a * 100
    if len(overlap) < 120 or difference.mean() > 1 or difference.quantile(.95) > 5:
        raise ResearchStageError("cross_exchange_mismatch", "跨交易所共同样本不足或价格偏差过大")
    return frames['binance'], {"ready": True, "requested_days": requested_days, "effective_days": days, "sources": reports, "overlap_bars": len(overlap), "price_difference_pct": round(float(difference.mean()), 4)}


def split_development(frame):
    split = int(len(frame) * .7)
    # Purge one bar at the boundary. No validation candles go into the prompt.
    train, validation = frame.iloc[:split], frame.iloc[split + 1:]
    if len(train) < 160 or len(validation) < 60:
        raise ResearchStageError("sample_too_short", "训练或验证样本不足")
    return train, validation


def market_summary(frame, symbol, timeframe):
    close = frame.close
    recent_return = float(close.iloc[-1] / close.iloc[-min(72, len(close))] - 1)
    volatility = float(close.pct_change().std())
    regime = "trend_up" if recent_return > volatility * 3 else "trend_down" if recent_return < -volatility * 3 else "range"
    return {"symbol": symbol, "timeframe": timeframe, "as_of": frame.index[-1].isoformat(), "bars": len(frame),
            "regime": regime, "recent_return": recent_return, "window_return": float(close.iloc[-1] / close.iloc[0] - 1),
            "bar_volatility": volatility, "last_close": float(close.iloc[-1]),
            "volume_ratio": float(frame.volume.tail(24).mean() / max(frame.volume.mean(), 1e-12))}


def program_fingerprint(program):
    payload = program.model_dump(mode="json") if isinstance(program, StrategyProgram) else dict(program)
    for key in ('name', 'description', 'program_id', 'tags', 'source', 'parameter_space'):
        payload.pop(key, None)
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def validate_program(program):
    program = StrategyProgram.model_validate(program)
    if not 1 <= len(program.indicators) <= 12 or not 1 <= len(program.entry_conditions) <= 12 or not 1 <= len(program.exit_conditions) <= 12:
        raise ResearchStageError("invalid_program", "草案需要有限指标及明确的进入、退出条件")
    names = {indicator.name for indicator in program.indicators}
    if len(names) != len(program.indicators):
        raise ResearchStageError("invalid_program", "指标名称重复")
    names |= {'open', 'high', 'low', 'close', 'volume'}
    for indicator in program.indicators:
        if not re.fullmatch(r'[a-z_][a-z0-9_]*', indicator.name):
            raise ResearchStageError('invalid_program', '指标名称须为小写英文及下划线')
        if indicator.source not in {'open', 'high', 'low', 'close', 'volume'} or (indicator.period is not None and not 1 <= indicator.period <= 96):
            raise ResearchStageError("invalid_program", "指标来源或周期超出研究范围（1–96）")
    for condition in program.entry_conditions + program.exit_conditions:
        if condition.left not in names or (isinstance(condition.right, str) and condition.right not in names):
            raise ResearchStageError("invalid_program", "条件引用了不存在的指标")
        if not isinstance(condition.right, str) and (isinstance(condition.right, bool) or not isinstance(condition.right, (int, float)) or not math.isfinite(condition.right)):
            raise ResearchStageError("invalid_program", "条件比较值无效")
    return program


def evaluate_program(program, frame, timeframe, *, stress=False):
    from core.research.strategy_research import _run_backtest_core
    from core.research.strategy_program import program_strategy_name

    program = validate_program(program)
    name = program_strategy_name(program, fallback='Research draft')
    result = _run_backtest_core(name, frame, timeframe, 10000, params=program.params,
                               commission_rate=.0008 if stress else .0004, slippage_bps=4 if stress else 2,
                               strategy_programs={name: program})
    if any(not math.isfinite(float(result[key])) for key in ('total_return', 'sharpe_ratio', 'max_drawdown')):
        raise ResearchStageError('invalid_metrics', '回测输出含无效指标')
    return result


def rejection_reasons(metrics, min_trades, max_drawdown):
    reasons = []
    if metrics.get('quality_flag') != 'ok':
        reasons.append('data_outliers')
    if metrics['total_trades'] < min_trades:
        reasons.append('insufficient_trades')
    if metrics['total_return'] <= 0:
        reasons.append('no_net_edge')
    if metrics['max_drawdown'] > max_drawdown:
        reasons.append('excess_drawdown')
    if metrics['sharpe_ratio'] < .5:
        reasons.append('weak_risk_adjusted_return')
    return reasons


def evaluate_drafts(drafts, frame, timeframe, budget, min_trades, max_drawdown):
    train, validation = split_development(frame)
    rows, seen, used = [], set(), 0
    for draft in drafts[:3]:
        row = {'name': draft.name, 'draft_id': draft.draft_id, 'thesis': draft.thesis, 'accepted': False}
        try:
            if draft.program is None:
                raise ResearchStageError('invalid_program', '草案没有可执行研究程序')
            program = validate_program(draft.program)
            fingerprint = program_fingerprint(program)
            if fingerprint in seen:
                raise ResearchStageError('duplicate_program', '同一轮出现重复程序')
            seen.add(fingerprint)
            if used + 3 > budget:
                raise ResearchStageError('evaluation_budget', '本轮回测预算不足')
            # Reserve all three runs before computation; errors cannot overspend.
            used += 3
            training = evaluate_program(program, train, timeframe)
            metrics = evaluate_program(program, validation, timeframe)
            stress = evaluate_program(program, validation, timeframe, stress=True)
            reasons = rejection_reasons(metrics, min_trades, max_drawdown)
            if training['total_return'] <= 0:
                reasons.append('training_no_edge')
            if stress['total_return'] <= 0:
                reasons.append('cost_sensitive')
            score = metrics['sharpe_ratio'] + metrics['total_return'] / max(metrics['max_drawdown'], 1) - metrics['max_drawdown'] * .1
            row.update(program=program.model_dump(mode='json'), fingerprint=fingerprint, training=training,
                       validation=metrics, stress=stress, reasons=reasons, accepted=not reasons, score=round(score, 4))
        except Exception as exc:
            row.update(reasons=[getattr(exc, 'code', 'evaluation_error')], error=str(exc)[:300])
        rows.append(row)
    return {'candidates': rows, 'backtest_runs': used,
            'training_start': train.index[0].isoformat(), 'training_end': train.index[-1].isoformat(),
            'validation_start': validation.index[0].isoformat(), 'validation_end': validation.index[-1].isoformat(),
            'validation_role': 'development_selection_not_final_holdout'}
