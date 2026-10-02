import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import numpy as np
import pandas as pd
import pytest


def test_old_source_tick_stays_stale_even_when_repeated():
    from core.marketdata.hub import MarketDataHub
    hub = MarketDataHub(symbol_max_age_sec=10)
    payload = {"last": 100, "timestamp": (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()}
    hub.upsert_ws_tick("binance", "BTC/USDT", payload)
    hub.upsert_ws_tick("binance", "BTC/USDT", payload)
    assert hub.get_tick("binance", "BTC/USDT")["meta"]["is_stale"]


@pytest.mark.parametrize("value", [float("inf"), float("-inf"), float("nan"), "inf"])
def test_nonfinite_market_price_rejected(value):
    from core.marketdata.hub import _coerce_float
    from core.marketdata.runtime_price_provider import _float_or_none
    assert _coerce_float(value) is None
    assert _float_or_none(value) is None


@pytest.mark.parametrize("payload", [{}, {"action": "ALLOWISH"}, {"action": None}, []])
def test_invalid_ai_response_fails_closed(monkeypatch, tmp_path, payload):
    import core.ai.live_decision_router as module
    from tests.test_ai_live_decision_router import _evaluate
    monkeypatch.setattr(module, "_OVERLAY_PATH", tmp_path / "overlay.json")
    router = module.LiveAIDecisionRouter()
    router._override.update(AI_LIVE_DECISION_ENABLED=True, AI_LIVE_DECISION_MODE="enforce", AI_LIVE_DECISION_FAIL_OPEN=False)
    monkeypatch.setattr(router, "_call_provider", AsyncMock(return_value=payload))
    result = asyncio.run(_evaluate(router))
    assert not result["allowed"]
    assert result["action"] == "block"


def test_training_labels_end_before_test_features():
    from core.ml import pipeline
    from tests.test_ml_pipeline import _sample_ohlcv
    data = _sample_ohlcv(160)
    dataset = pipeline.build_dataset(data, forward_bars=4, min_rows=20)
    split = pipeline.split_dataset(dataset, test_size=0.2)
    assert data.index.get_loc(split.X_train.index[-1]) + 4 < data.index.get_loc(split.X_test.index[0])


def test_factor_cache_invalidates_changed_middle_and_extra_columns():
    from core.factors_ts.cache import _fingerprint as _data_fingerprint
    frame = pd.DataFrame({"close": range(100), "volume": range(100), "funding_rate": 0.01}, index=pd.date_range("2026-01-01", periods=100))
    initial = _data_fingerprint(frame)
    frame.iloc[50, 2] = 0.02
    assert initial != _data_fingerprint(frame)
    initial = _data_fingerprint(frame)
    frame.index = frame.index.where(np.arange(100) != 50, frame.index + pd.Timedelta(seconds=1))
    assert initial != _data_fingerprint(frame)


def test_strategy_data_is_causal_and_missing_identifiers_fail_closed():
    from core.research.strategy_program import _series_for_indicator, _resolve_operand
    frame = pd.DataFrame({"close": [np.nan, np.nan, 100.0]})
    spec = SimpleNamespace(source="close", period=1, kind="price")
    result = _series_for_indicator(frame, spec)
    assert result.iloc[:2].isna().all()
    pd.testing.assert_series_equal(_series_for_indicator(frame.iloc[:2], spec), result.iloc[:2])
    with pytest.raises(ValueError, match="missing strategy source"):
        _series_for_indicator(frame, SimpleNamespace(source="missing", period=1, kind="price"))
    with pytest.raises(ValueError, match="unknown strategy operand"):
        _resolve_operand("typo", {}, frame.index)


def test_backtest_entry_fee_affects_trade_classification_once():
    from core.backtest.backtest_engine import BacktestConfig, BacktestEngine
    from core.strategies import Signal, SignalType
    engine = BacktestEngine(BacktestConfig(initial_capital=1000, position_size_pct=0.1, commission_rate=0.01, slippage=0))
    now = datetime.now(timezone.utc)
    frame = pd.DataFrame({"close": [100.0], "high": [100.0], "low": [100.0]})
    signal = Signal(symbol="BTC/USDT", signal_type=SignalType.BUY, price=100, strength=1, strategy_name="test", timestamp=now)
    asyncio.run(engine._execute_buy(signal, 100, now, frame))
    asyncio.run(engine._close_position("BTC/USDT", 101.5, now, "long", frame))
    engine._update_positions(101.5, "BTC/USDT")
    trade = engine._trades[-1]
    assert trade.net_pnl == pytest.approx(-0.515)
    assert engine._capital - 1000 == pytest.approx(trade.net_pnl)
    assert engine._calculate_result().winning_trades == 0


def test_hourly_annualization_and_calmar_fraction_units():
    from core.backtest.backtest_engine import BacktestResult
    from core.backtest.performance_analyzer import PerformanceAnalyzer
    equity = [100, 102, 99, 103]
    result = BacktestResult(initial_capital=100, final_capital=103, total_return=3, total_return_pct=.03,
        max_drawdown=.03, max_drawdown_pct=3, sharpe_ratio=0, win_rate=0, total_trades=0,
        winning_trades=0, losing_trades=0, profit_factor=0, avg_trade_return=0,
        equity_curve=equity, equity_timestamps=list(pd.date_range("2026-01-01", periods=4, freq="h")))
    analyzer = PerformanceAnalyzer(risk_free_rate=0)
    metrics = analyzer.analyze(result)
    returns = np.diff(equity) / np.array(equity[:-1])
    assert metrics.sharpe_ratio == pytest.approx(np.mean(returns) / np.std(returns) * np.sqrt(365 * 24))
    assert metrics.calmar_ratio == pytest.approx(metrics.annual_return / .03)


def test_cusum_cannot_trigger_before_minimum_bars():
    from core.monitoring.strategy_monitor import detect_strategy_decay
    result = detect_strategy_decay([-0.1] * 19, min_bars=20)
    assert not result["triggered"]


def test_decay_lifecycle_demotes_one_step_without_forced_retirement(monkeypatch):
    from core.monitoring.cusum_watcher import _demote_on_decay
    from core.deployment import promotion_engine
    records = []
    app = SimpleNamespace(state=SimpleNamespace(ai_lifecycle_registry=SimpleNamespace(append=records.append)))
    candidate = SimpleNamespace(status="paper_running", candidate_id="test", metadata={})
    assert asyncio.run(_demote_on_decay(app, candidate)) == "shadow_running"
    assert candidate.status == "shadow_running"
    assert len(records) == 1
    def invalid(*args, **kwargs):
        raise ValueError("injected invalid transition")
    monkeypatch.setattr(promotion_engine, "transition_candidate", invalid)
    assert asyncio.run(_demote_on_decay(app, candidate)) == "shadow_running"
    assert candidate.status == "shadow_running"


def test_news_timestamp_boundary_has_overlap_for_deduplication():
    from core.news.collectors.common import BaseNewsCollector
    items = [{"published_at": datetime.fromtimestamp(ts, timezone.utc).isoformat()} for ts in (1000, 999, 699)]
    assert len(BaseNewsCollector.filter_incremental(items, "1000")) == 2


def test_news_delivery_not_truncated_or_acknowledged_before_storage(monkeypatch):
    from tests.test_news_collectors_manager import _DummyCollector, _DummyManager
    from core.news.storage import db
    items = [dict(title=f"unique item {i}", url=f"https://example.test/{i}", published_at="2026-10-01T00:00:00Z") for i in range(8)]
    monkeypatch.setattr(db, "get_source_state", AsyncMock(return_value=None))
    acknowledge = AsyncMock()
    monkeypatch.setattr(db, "set_source_state", acknowledge)
    result = asyncio.run(_DummyManager(_DummyCollector(items=items, cursor="100")).pull_latest_incremental(max_records=2))
    assert len(result["items"]) == 8
    assert result["source_cursors"] == {"dummy": "100"}
    acknowledge.assert_not_awaited()


def test_news_cursor_and_rows_rollback_together(tmp_path, monkeypatch):
    from tests.test_news_storage_llm_tasks import _with_temp_news_db
    from core.news.storage import db
    from core.news.storage.models import NewsRaw, NewsSourceState
    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import AsyncSession
    async def run():
        engine = await _with_temp_news_db(tmp_path, monkeypatch)
        items = [dict(title="test", url="https://example.test/test", source="rss", published_at="2026-10-01T00:00:00Z")]
        try:
            with monkeypatch.context() as patch:
                patch.setattr(AsyncSession, "commit", AsyncMock(side_effect=RuntimeError("injected commit failure")))
                with pytest.raises(RuntimeError, match="injected"):
                    await db.save_news_raw(items, source_cursors={"rss": "100"})
            async with db.news_session_scope() as session:
                assert (await session.execute(select(NewsRaw))).scalars().all() == []
                assert (await session.execute(select(NewsSourceState))).scalars().all() == []
            await db.save_news_raw(items, source_cursors={"rss": "100"})
            await db.save_news_raw(items, source_cursors={"rss": "99"})
            state = await db.get_source_state("rss")
            assert float(state["cursor_value"]) == 100
            async with db.news_session_scope() as session:
                assert len((await session.execute(select(NewsRaw))).scalars().all()) == 1
        finally:
            await engine.dispose()
    asyncio.run(run())
