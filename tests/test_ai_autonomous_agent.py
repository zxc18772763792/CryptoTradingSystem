from __future__ import annotations

import asyncio
import importlib
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pandas as pd
import pytest

from config.settings import settings


@pytest.fixture(autouse=True)
def _isolate_agent_overlay(tmp_path, monkeypatch):
    """Redirect agent overlay path so tests don't pollute cache/ or each other."""
    import core.ai.autonomous_agent as _mod
    monkeypatch.setattr(
        _mod.autonomous_trading_agent, "_overlay_path",
        tmp_path / "agent_runtime_config.json",
    )
    monkeypatch.setattr(_mod.news_db, "get_recent_events", AsyncMock(return_value=[]))
    monkeypatch.setattr(_mod.execution_engine, "get_account_equity_snapshot", AsyncMock(return_value=0.0))
    monkeypatch.setattr(_mod.execution_engine, "get_strategy_position_cap_notional", lambda **kwargs: 0.0)
    monkeypatch.setattr(_mod.execution_engine, "get_live_trade_review", lambda **kwargs: {"items": []})
    monkeypatch.setattr(_mod.strategy_manager, "get_strategy_allocation", lambda name: 0.0)
    monkeypatch.setattr(_mod.position_manager, "get_all_positions", lambda: [])
    monkeypatch.setattr(
        _mod.risk_manager,
        "get_risk_report",
        lambda: {
            "risk_level": "low",
            "trading_halted": False,
            "halt_reason": "",
            "discipline": {
                "fresh_entry_allowed": True,
                "reduce_only": False,
                "degrade_mode": "normal",
                "reasons": [],
            },
            "equity": {
                "daily_pnl_ratio": 0.0,
                "daily_stop_basis_ratio": 0.0,
                "max_drawdown": 0.0,
            },
        },
    )


def _sample_df(*, freq: str = "15min", periods: int = 120, start: str = "2025-01-01") -> pd.DataFrame:
    idx = pd.date_range(start, periods=periods, freq=freq)
    close = [100.0 + i * 0.2 for i in range(len(idx))]
    return pd.DataFrame(
        {
            "open": close,
            "high": [v + 0.1 for v in close],
            "low": [v - 0.1 for v in close],
            "close": close,
            "volume": [10.0] * len(close),
        },
        index=idx,
    )


def _sample_df_for_timeframe(timeframe: str, *, periods: int = 240, start: str = "2025-01-01") -> pd.DataFrame:
    freq_map = {
        "5m": "5min",
        "15m": "15min",
        "1h": "1h",
    }
    return _sample_df(freq=freq_map.get(str(timeframe or "").strip(), "15min"), periods=periods, start=start)


def test_risk_report_exposes_reduce_only_discipline_contract(monkeypatch):
    risk_module = importlib.import_module("core.risk.risk_manager")

    manager = risk_module.RiskManager()
    manager.max_daily_loss_ratio = 0.02
    manager._day_start_equity = 1000.0
    manager._current_equity = 985.0
    manager._daily_realized_pnl = -10.0
    manager._current_unrealized_pnl = -5.0
    manager._equity_curve = [1000.0, 985.0]

    monkeypatch.setattr(
        risk_module,
        "_position_manager",
        lambda: SimpleNamespace(
            get_total_pnl=lambda: -5.0,
            get_total_exposure=lambda: 0.0,
            get_position_count=lambda: 0,
            get_all_positions=lambda: [],
        ),
    )

    report = manager.get_risk_report()

    assert report["discipline"]["fresh_entry_allowed"] is False
    assert report["discipline"]["reduce_only"] is True
    assert report["discipline"]["degrade_mode"] == "reduce_only"
    assert report["discipline"]["reasons"]
    assert report["discipline"]["reasons"][0].startswith("daily_stop_buffer_reached(")


def test_risk_report_exposes_drawdown_reduce_only_discipline_contract(monkeypatch):
    risk_module = importlib.import_module("core.risk.risk_manager")

    manager = risk_module.RiskManager()
    manager.max_daily_loss_ratio = 0.02
    manager._day_start_equity = 1000.0
    manager._current_equity = 940.0
    manager._daily_realized_pnl = -5.0
    manager._current_unrealized_pnl = 0.0
    manager._equity_curve = [1000.0, 980.0, 950.0, 940.0]

    monkeypatch.setattr(
        risk_module,
        "_position_manager",
        lambda: SimpleNamespace(
            get_total_pnl=lambda: 0.0,
            get_total_exposure=lambda: 0.0,
            get_position_count=lambda: 0,
            get_all_positions=lambda: [],
        ),
    )

    report = manager.get_risk_report()

    assert report["discipline"]["fresh_entry_allowed"] is False
    assert report["discipline"]["reduce_only"] is True
    assert report["discipline"]["degrade_mode"] == "reduce_only"
    assert "max_drawdown_limit_exceeded(0.060000>=0.050000)" in report["discipline"]["reasons"]
    assert report["discipline"]["thresholds"]["max_drawdown_reduce_only"] == pytest.approx(0.05)


def test_risk_report_exposes_rolling_drawdown_reduce_only_contract(monkeypatch):
    risk_module = importlib.import_module("core.risk.risk_manager")

    manager = risk_module.RiskManager()
    manager.max_daily_loss_ratio = 0.02
    manager._day_start_equity = 1000.0
    manager._current_equity = 990.0
    manager._daily_realized_pnl = -2.0
    manager._current_unrealized_pnl = 0.0
    manager._equity_curve = [1000.0, 995.0, 990.0]
    now = datetime.now(timezone.utc)
    manager._equity_timeline = [
        {"timestamp": (now - timedelta(hours=60)).isoformat(), "equity": 1000.0},
        {"timestamp": (now - timedelta(hours=36)).isoformat(), "equity": 940.0},
        {"timestamp": (now - timedelta(hours=6)).isoformat(), "equity": 930.0},
        {"timestamp": (now - timedelta(hours=2)).isoformat(), "equity": 990.0},
    ]

    monkeypatch.setattr(
        risk_module,
        "_position_manager",
        lambda: SimpleNamespace(
            get_total_pnl=lambda: 0.0,
            get_total_exposure=lambda: 0.0,
            get_position_count=lambda: 0,
            get_all_positions=lambda: [],
        ),
    )

    report = manager.get_risk_report()

    assert report["drawdown"]["rolling_3d"]["drawdown"] == pytest.approx(0.07)
    assert report["drawdown"]["rolling_7d"]["drawdown"] == pytest.approx(0.07)
    assert report["equity"]["rolling_3d_drawdown"] == pytest.approx(0.07)
    assert report["equity"]["rolling_7d_drawdown"] == pytest.approx(0.07)
    assert report["discipline"]["reduce_only"] is True
    assert "rolling_3d_drawdown_limit_exceeded(0.070000>=0.060000)" in report["discipline"]["reasons"]
    assert report["discipline"]["thresholds"]["rolling_3d_drawdown_reduce_only"] == pytest.approx(0.06)
    assert report["discipline"]["thresholds"]["rolling_7d_drawdown_reduce_only"] == pytest.approx(0.09)


def test_risk_report_uses_configurable_autonomy_thresholds(monkeypatch):
    risk_module = importlib.import_module("core.risk.risk_manager")

    manager = risk_module.RiskManager()
    manager.update_parameters(
        {
            "max_daily_loss_ratio": 0.02,
            "autonomy_daily_stop_buffer_ratio": 0.01,
            "autonomy_max_drawdown_reduce_only": 0.03,
            "autonomy_rolling_3d_drawdown_reduce_only": 0.05,
            "autonomy_rolling_7d_drawdown_reduce_only": 0.08,
        }
    )
    manager._day_start_equity = 1000.0
    manager._current_equity = 995.0
    manager._daily_realized_pnl = -2.0
    manager._current_unrealized_pnl = 0.0
    manager._equity_curve = [1000.0, 960.0]
    now = datetime.now(timezone.utc)
    manager._equity_timeline = [
        {"timestamp": (now - timedelta(hours=60)).isoformat(), "equity": 1000.0},
        {"timestamp": (now - timedelta(hours=24)).isoformat(), "equity": 945.0},
        {"timestamp": (now - timedelta(hours=2)).isoformat(), "equity": 995.0},
    ]

    monkeypatch.setattr(
        risk_module,
        "_position_manager",
        lambda: SimpleNamespace(
            get_total_pnl=lambda: 0.0,
            get_total_exposure=lambda: 0.0,
            get_position_count=lambda: 0,
            get_all_positions=lambda: [],
        ),
    )

    report = manager.get_risk_report()

    assert report["discipline"]["reduce_only"] is True
    assert "max_drawdown_limit_exceeded(0.040000>=0.030000)" in report["discipline"]["reasons"]
    assert "rolling_3d_drawdown_limit_exceeded(0.055000>=0.050000)" in report["discipline"]["reasons"]
    assert report["discipline"]["thresholds"]["daily_stop_buffer_ratio"] == pytest.approx(-0.01)
    assert report["discipline"]["thresholds"]["max_drawdown_reduce_only"] == pytest.approx(0.03)
    assert report["discipline"]["thresholds"]["rolling_3d_drawdown_reduce_only"] == pytest.approx(0.05)
    assert report["discipline"]["thresholds"]["rolling_7d_drawdown_reduce_only"] == pytest.approx(0.08)
    assert report["limits"]["autonomy_daily_stop_buffer_ratio"] == pytest.approx(0.01)
    assert report["limits"]["autonomy_thresholds"]["autonomy_max_drawdown_reduce_only"] == pytest.approx(0.03)


def test_risk_manager_clamps_autonomy_daily_stop_buffer_to_daily_loss_limit():
    risk_module = importlib.import_module("core.risk.risk_manager")

    manager = risk_module.RiskManager()
    manager.update_parameters(
        {
            "max_daily_loss_ratio": 0.02,
            "autonomy_daily_stop_buffer_ratio": 0.08,
        }
    )

    assert manager.get_autonomy_risk_config()["autonomy_daily_stop_buffer_ratio"] == pytest.approx(0.02)


def test_risk_manager_persists_autonomy_threshold_overlay(monkeypatch, tmp_path: Path):
    risk_module = importlib.import_module("core.risk.risk_manager")
    overlay_path = tmp_path / "autonomous_agent_risk_config.json"
    monkeypatch.setenv("AI_AGENT_RISK_CONFIG_PATH", str(overlay_path))

    manager = risk_module.RiskManager()
    manager.update_parameters(
        {
            "autonomy_daily_stop_buffer_ratio": 0.011,
            "autonomy_max_drawdown_reduce_only": 0.032,
            "autonomy_rolling_3d_drawdown_reduce_only": 0.054,
            "autonomy_rolling_7d_drawdown_reduce_only": 0.081,
        }
    )

    persisted = json.loads(overlay_path.read_text(encoding="utf-8"))
    assert set(persisted) == {
        "autonomy_daily_stop_buffer_ratio",
        "autonomy_max_drawdown_reduce_only",
        "autonomy_rolling_3d_drawdown_reduce_only",
        "autonomy_rolling_7d_drawdown_reduce_only",
    }
    assert persisted["autonomy_daily_stop_buffer_ratio"] == pytest.approx(0.011)
    assert persisted["autonomy_max_drawdown_reduce_only"] == pytest.approx(0.032)
    assert persisted["autonomy_rolling_3d_drawdown_reduce_only"] == pytest.approx(0.054)
    assert persisted["autonomy_rolling_7d_drawdown_reduce_only"] == pytest.approx(0.081)

    reloaded = risk_module.RiskManager()
    assert reloaded.get_autonomy_risk_config()["autonomy_daily_stop_buffer_ratio"] == pytest.approx(0.011)
    assert reloaded.get_autonomy_risk_config()["autonomy_max_drawdown_reduce_only"] == pytest.approx(0.032)
    assert reloaded.get_autonomy_risk_config()["autonomy_rolling_3d_drawdown_reduce_only"] == pytest.approx(0.054)
    assert reloaded.get_autonomy_risk_config()["autonomy_rolling_7d_drawdown_reduce_only"] == pytest.approx(0.081)


def test_autonomous_agent_run_once_submit_signal(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    class _Agg:
        def to_dict(self):
            return {"direction": "LONG", "confidence": 0.72}

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "live")
    submit_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(module.execution_engine, "submit_signal", submit_mock)
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(
            return_value={
                "action": "buy",
                "confidence": 0.83,
                "strength": 0.76,
                "leverage": 4,
                "stop_loss_pct": 0.02,
                "take_profit_pct": 0.05,
                "reason": "trend_following",
            }
        ),
    )
    monkeypatch.setattr(module.time, "perf_counter", Mock(side_effect=[100.0, 100.25]))

    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute", allow_live=True, cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="test", force=True))
    status = agent.get_status()

    assert result["decision"]["action"] == "buy"
    assert result["execution"]["submitted"] is True
    assert submit_mock.await_count == 1
    assert status["last_latency_ms"] == 250
    assert status["last_run_at"] == result["timestamp"]
    signal = submit_mock.await_args.args[0]
    assert signal.strategy_name == "AI_AutonomousAgent"
    assert result["decision"]["leverage"] == 1.0
    assert signal.metadata["leverage"] == 1.0
    assert signal.stop_loss is not None
    assert signal.take_profit is not None


def test_autonomous_agent_run_once_low_confidence_forces_hold(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    class _Agg:
        def to_dict(self):
            return {"direction": "LONG", "confidence": 0.60}

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "live")
    submit_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(module.execution_engine, "submit_signal", submit_mock)
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(
            return_value={
                "action": "buy",
                "confidence": 0.32,
                "strength": 0.7,
                "leverage": 3,
                "reason": "weak_conviction",
            }
        ),
    )

    asyncio.run(
        agent.update_runtime_config(
            enabled=True,
            mode="execute",
            min_confidence=0.7,
            cooldown_sec=0,
        )
    )
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    assert result["decision"]["action"] == "hold"
    assert result["decision"]["leverage"] == 1.0
    assert result["execution"]["submitted"] is False
    assert submit_mock.await_count == 0


def test_load_market_data_persists_live_bars_when_local_cache_is_stale(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_live_refresh")
    stale_df = _sample_df().copy()
    stale_df.index = pd.date_range("2026-01-01", periods=len(stale_df), freq="15min")
    live_start = datetime.now(timezone.utc).replace(second=0, microsecond=0) - pd.Timedelta(minutes=15 * 5)
    live_klines = [
        SimpleNamespace(
            timestamp=(live_start + pd.Timedelta(minutes=15 * idx)).isoformat(),
            open=100.0 + idx,
            high=100.2 + idx,
            low=99.8 + idx,
            close=100.1 + idx,
            volume=10.0 + idx,
        )
        for idx in range(6)
    ]
    save_mock = AsyncMock(return_value="saved")
    connector = SimpleNamespace(get_klines=AsyncMock(return_value=live_klines))

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=stale_df))
    monkeypatch.setattr(module.data_storage, "save_klines_to_parquet", save_mock)
    monkeypatch.setattr(module.exchange_manager, "get_exchange", lambda name: connector)

    result = asyncio.run(
        agent._load_market_data(
            {
                "exchange": "binance",
                "symbol": "BTC/USDT",
                "timeframe": "15m",
                "lookback_bars": 60,
            }
        )
    )

    assert not result.empty
    assert save_mock.await_count == 1


def test_load_market_data_skips_live_fetch_when_local_cache_is_fresh(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_fresh_cache")
    fresh_end = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    fresh_df = _sample_df().copy()
    fresh_df.index = pd.date_range(end=fresh_end, periods=len(fresh_df), freq="15min")
    connector = SimpleNamespace(get_klines=AsyncMock(return_value=[]))

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=fresh_df))
    monkeypatch.setattr(module.exchange_manager, "get_exchange", lambda name: connector)

    result = asyncio.run(
        agent._load_market_data(
            {
                "exchange": "binance",
                "symbol": "BTC/USDT",
                "timeframe": "15m",
                "lookback_bars": 60,
            }
        )
    )

    assert not result.empty
    assert connector.get_klines.await_count == 0


def test_load_market_data_fetches_live_when_recent_cache_is_incomplete(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_recent_incomplete")
    fixed_now = datetime(2026, 1, 2, 12, 5, tzinfo=timezone.utc)
    local_df = _sample_df(freq="15min", periods=40, start="2026-01-02 02:00").copy()
    live_start = datetime(2026, 1, 1, 20, 45, tzinfo=timezone.utc)
    live_klines = [
        SimpleNamespace(
            timestamp=(live_start + pd.Timedelta(minutes=15 * idx)).isoformat(),
            open=100.0 + idx,
            high=100.2 + idx,
            low=99.8 + idx,
            close=100.1 + idx,
            volume=10.0 + idx,
        )
        for idx in range(61)
    ]
    save_mock = AsyncMock(return_value="saved")
    connector = SimpleNamespace(get_klines=AsyncMock(return_value=live_klines))

    monkeypatch.setattr(module, "_utc_now", lambda: fixed_now)
    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=local_df))
    monkeypatch.setattr(module.data_storage, "save_klines_to_parquet", save_mock)
    monkeypatch.setattr(module.exchange_manager, "get_exchange", lambda name: connector)

    result = asyncio.run(
        agent._load_market_data(
            {
                "exchange": "binance",
                "symbol": "BTC/USDT",
                "timeframe": "15m",
                "lookback_bars": 60,
            }
        )
    )

    assert len(result) == 60
    assert connector.get_klines.await_count == 1
    assert save_mock.await_count == 1


def test_load_market_data_live_timeout_falls_back_to_local(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_live_timeout")
    stale_df = _sample_df().copy()
    stale_df.index = pd.date_range("2026-01-01", periods=len(stale_df), freq="15min")

    async def _slow_klines(symbol, timeframe, limit):
        await asyncio.sleep(0.05)
        return []

    connector = SimpleNamespace(get_klines=AsyncMock(side_effect=_slow_klines))

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=stale_df))
    monkeypatch.setattr(module.exchange_manager, "get_exchange", lambda name: connector)

    result = asyncio.run(
        agent._load_market_data(
            {
                "exchange": "binance",
                "symbol": "BTC/USDT",
                "timeframe": "15m",
                "lookback_bars": 60,
                "_live_market_timeout_sec": 0.01,
            }
        )
    )

    assert not result.empty
    assert result.index.max() == stale_df.index.max()
    assert connector.get_klines.await_count == 1


def test_load_market_data_drops_incomplete_latest_live_bar(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_drop_incomplete")
    fixed_now = datetime(2026, 1, 1, 14, 5, tzinfo=timezone.utc)
    live_klines = [
        SimpleNamespace(
            timestamp=(fixed_now - pd.Timedelta(minutes=45)).isoformat(),
            open=100.0,
            high=100.2,
            low=99.8,
            close=100.1,
            volume=10.0,
        ),
        SimpleNamespace(
            timestamp=(fixed_now - pd.Timedelta(minutes=30)).isoformat(),
            open=101.0,
            high=101.2,
            low=100.8,
            close=101.1,
            volume=11.0,
        ),
        SimpleNamespace(
            timestamp=(fixed_now - pd.Timedelta(minutes=15)).isoformat(),
            open=102.0,
            high=102.2,
            low=101.8,
            close=102.1,
            volume=12.0,
        ),
        # This is the currently forming 15m bar and should be dropped.
        SimpleNamespace(
            timestamp=fixed_now.replace(minute=0, second=0, microsecond=0).isoformat(),
            open=103.0,
            high=103.2,
            low=102.8,
            close=103.1,
            volume=13.0,
        ),
    ]
    connector = SimpleNamespace(get_klines=AsyncMock(return_value=live_klines))

    monkeypatch.setattr(module, "_utc_now", lambda: fixed_now)
    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=pd.DataFrame()))
    monkeypatch.setattr(module.data_storage, "save_klines_to_parquet", AsyncMock(return_value="saved"))
    monkeypatch.setattr(module.exchange_manager, "get_exchange", lambda name: connector)

    result = asyncio.run(
        agent._load_market_data_for_timeframe(
            {
                "exchange": "binance",
                "symbol": "BTC/USDT",
                "_force_live_market": True,
            },
            timeframe="15m",
            lookback_bars=4,
        )
    )

    assert not result.empty
    assert pd.Timestamp("2026-01-01T14:00:00+00:00") not in result.index
    assert result.index.max() == pd.Timestamp("2026-01-01T13:50:00+00:00")


def test_build_context_includes_multi_scale_features(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_multiscale")

    class _Agg:
        def __init__(self, direction: str, confidence: float):
            self._payload = {
                "direction": direction,
                "confidence": confidence,
                "blocked_by_risk": False,
                "risk_reason": "",
                "components": {
                    "factor": {
                        "direction": direction,
                        "confidence": confidence,
                        "effective_weight": 0.25,
                        "available": True,
                        "status": "active",
                        "reason": "",
                    }
                },
            }

        def to_dict(self):
            return dict(self._payload)

    async def _load_for_timeframe(cfg, *, timeframe, lookback_bars):
        return _sample_df_for_timeframe(timeframe, periods=max(lookback_bars, 240), start="2026-01-01")

    aggregate_mock = AsyncMock(
        side_effect=[
            _Agg("LONG", 0.81),
            _Agg("LONG", 0.74),
            _Agg("SHORT", 0.67),
        ]
    )

    monkeypatch.setattr(agent, "_load_market_data", AsyncMock(return_value=_sample_df_for_timeframe("15m", periods=240, start="2026-01-01")))
    monkeypatch.setattr(agent, "_load_market_data_for_timeframe", AsyncMock(side_effect=_load_for_timeframe))
    monkeypatch.setattr(agent, "_resolve_last_price", AsyncMock(return_value=123.0))
    monkeypatch.setattr(agent, "_resolve_account_risk_base", AsyncMock(return_value={
        "account_equity": 1000.0,
        "strategy_allocation": 0.0,
        "position_cap_notional": 100.0,
        "max_total_exposure_ratio": 0.4,
        "total_strategy_open_notional": 0.0,
        "total_exposure_limit_notional": 400.0,
        "trading_mode": "paper",
    }))
    monkeypatch.setattr(agent, "_resolve_position_payload", AsyncMock(return_value={}))
    monkeypatch.setattr(agent, "_annotate_position_payload", AsyncMock(return_value={}))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=aggregate_mock))
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")

    context, market_data = asyncio.run(
        agent._build_context(
            {
                "exchange": "binance",
                "symbol": "BTC/USDT",
                "timeframe": "15m",
                "lookback_bars": 240,
                "account_id": "main",
                "mode": "execute",
                "min_confidence": 0.58,
                "_skip_event_summary": True,
                "_skip_research_context": True,
                "_include_multi_scale_context": True,
            }
        )
    )

    assert not market_data.empty
    assert context["decision_timeframes"] == {
        "trigger": "5m",
        "setup": "15m",
        "regime": "1h",
    }
    assert set(context["multi_scale_features"]) == {"5m", "15m", "1h"}
    assert context["multi_scale_features"]["5m"]["bars"] == 240
    assert context["multi_scale_features"]["15m"]["aggregated_signal"]["direction"] == "LONG"
    assert context["multi_scale_features"]["1h"]["aggregated_signal"]["direction"] == "SHORT"
    assert context["multi_scale_features"]["1h"]["data_quality"]["requested_bars"] == 240


def test_market_data_quality_uses_bar_close_time_for_freshness(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_market_quality")
    fixed_now = datetime(2026, 1, 1, 12, 5, tzinfo=timezone.utc)
    market_data = _sample_df(freq="15min", periods=4, start="2026-01-01 11:00").copy()

    monkeypatch.setattr(module, "_utc_now", lambda: fixed_now)

    payload = agent._build_market_data_quality_payload(
        timeframe="15m",
        timeframe_sec=900,
        lookback_bars=4,
        market_data=market_data,
    )

    assert payload["last_bar_at"] == "2026-01-01T11:45:00"
    assert payload["last_bar_closed_at"] == "2026-01-01T12:00:00+00:00"
    assert payload["freshness_age_sec"] == 300.0
    assert payload["fresh"] is True
    assert payload["realtime_ready"] is True


def test_build_prompt_includes_multi_scale_features(tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_prompt_multiscale")
    system_prompt, user_prompt = agent._build_prompt(
        {
            "mode": "execute",
            "allow_live": True,
            "min_confidence": 0.58,
            "default_stop_loss_pct": 0.02,
            "default_take_profit_pct": 0.04,
            "same_direction_max_exposure_ratio": 0.35,
            "max_total_exposure_ratio": 0.4,
            "entry_size_scale": 0.8,
        },
        {
            "exchange": "binance",
            "symbol": "BTC/USDT",
            "timeframe": "15m",
            "trading_mode": "paper",
            "price": 123.0,
            "bars": 240,
            "returns": {"r_15m": 0.01},
            "realized_vol_annualized": 0.25,
            "market_structure": {
                "available": True,
                "last_bar_at": "2026-01-01T13:45:00+00:00",
                "trend": {"label": "uptrend", "ema_gap_pct": 0.01, "close_vs_ema_slow_pct": 0.02},
                "microstructure": {"atr_pct": 0.01, "realized_vol": 0.02, "spread_proxy": 0.001},
                "volume": {"ratio_20": 1.2, "zscore_20": 0.6},
                "range": {"position_pct": 0.75},
            },
            "aggregated_signal": {
                "direction": "LONG",
                "confidence": 0.8,
                "components": {
                    "factor": {
                        "direction": "LONG",
                        "confidence": 0.8,
                        "effective_weight": 0.25,
                        "available": True,
                        "status": "active",
                        "reason": "",
                    }
                },
            },
            "event_summary": {},
            "position": {},
            "account_risk": {},
            "execution_cost": {},
            "research_context": {},
            "decision_timeframes": {"trigger": "5m", "setup": "15m", "regime": "1h"},
            "multi_scale_features": {
                "5m": {
                    "bars": 240,
                    "returns": {"r_15m": 0.005},
                    "realized_vol_annualized": 0.3,
                    "market_structure": {
                        "available": True,
                        "last_bar_at": "2026-01-01T14:00:00+00:00",
                        "trend": {"label": "uptrend", "ema_gap_pct": 0.006, "close_vs_ema_slow_pct": 0.008},
                        "microstructure": {"atr_pct": 0.008, "realized_vol": 0.03, "spread_proxy": 0.0012},
                        "volume": {"ratio_20": 1.1, "zscore_20": 0.4},
                        "range": {"position_pct": 0.8},
                    },
                    "data_quality": {
                        "status": "ready",
                        "realtime_ready": True,
                        "fresh": True,
                        "complete_bars": True,
                        "bars": 240,
                        "requested_bars": 240,
                        "missing_bar_count": 0,
                        "freshness_age_sec": 120.0,
                        "freshness_limit_sec": 660.0,
                        "last_bar_at": "2026-01-01T14:00:00+00:00",
                    },
                    "aggregated_signal": {"direction": "LONG", "confidence": 0.72, "components": {}},
                }
            },
            "learning_memory": {},
        },
    )

    payload = json.loads(user_prompt)

    assert "autonomous crypto trading agent" in system_prompt.lower()
    assert payload["runtime_constraints"]["decision_timeframes"]["trigger"] == "5m"
    assert payload["input"]["multi_scale_features"]["5m"]["data_quality"]["realtime_ready"] is True
    assert payload["input"]["decision_timeframes"]["regime"] == "1h"


def test_build_context_light_symbol_scan_skips_expensive_runtime_calls(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_light_scan")

    class _Agg:
        def to_dict(self):
            return {"direction": "LONG", "confidence": 0.64, "blocked_by_risk": False, "risk_reason": ""}

    monkeypatch.setattr(agent, "_load_market_data", AsyncMock(return_value=_sample_df().tail(60)))
    monkeypatch.setattr(agent, "_resolve_last_price", AsyncMock(return_value=123.0))
    monkeypatch.setattr(agent, "_resolve_account_risk_base", AsyncMock(side_effect=AssertionError("skip account risk base")))
    monkeypatch.setattr(agent, "_resolve_position_payload", AsyncMock(side_effect=AssertionError("use prefetched scan position map")))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")

    context, market_data = asyncio.run(
        agent._build_context(
            {
                "exchange": "binance",
                "symbol": "BTC/USDT",
                "timeframe": "15m",
                "account_id": "main",
                "mode": "execute",
                "min_confidence": 0.58,
                "default_stop_loss_pct": 0.02,
                "default_take_profit_pct": 0.04,
                "_light_symbol_scan": True,
                "_skip_event_summary": True,
                "_skip_research_context": True,
                "_scan_position_map": {
                    "BTC/USDT": {
                        "side": "long",
                        "quantity": 1.0,
                        "entry_price": 120.0,
                        "current_price": 123.0,
                        "unrealized_pnl": 3.0,
                        "source": "prefetched_live",
                    }
                },
            }
        )
    )

    assert not market_data.empty
    assert context["position"]["side"] == "long"
    assert context["position"]["source"] == "prefetched_live"
    assert context["research_context"]["available"] is False
    assert context["research_context"]["selection_reason"] == "agent_research_decoupled_scan"
    assert context["account_risk"]["trading_mode"] == "paper"


def test_build_context_coarse_scan_uses_fast_aggregate(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_fast_aggregate")

    class _Agg:
        def to_dict(self):
            return {"direction": "LONG", "confidence": 0.64, "blocked_by_risk": False, "risk_reason": ""}

    aggregate_mock = AsyncMock(return_value=_Agg())
    monkeypatch.setattr(agent, "_load_market_data", AsyncMock(return_value=_sample_df().tail(60)))
    monkeypatch.setattr(agent, "_resolve_last_price", AsyncMock(return_value=123.0))
    monkeypatch.setattr(agent, "_resolve_position_payload", AsyncMock(return_value={}))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=aggregate_mock))
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")

    asyncio.run(
        agent._build_context(
            {
                "exchange": "binance",
                "symbol": "BTC/USDT",
                "timeframe": "15m",
                "account_id": "main",
                "mode": "execute",
                "min_confidence": 0.58,
                "_light_symbol_scan": True,
                "_scan_skip_live_market": True,
                "_skip_event_summary": True,
                "_skip_research_context": True,
            }
        )
    )

    kwargs = aggregate_mock.await_args.kwargs
    assert kwargs["include_llm"] is False
    assert kwargs["include_ml"] is False


def test_autonomous_agent_run_once_force_bypasses_disabled_guard(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    class _Agg:
        def to_dict(self):
            return {"direction": "LONG", "confidence": 0.79}

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")
    monkeypatch.setattr(module.execution_engine, "submit_signal", AsyncMock(return_value=False))
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(
            return_value={
                "action": "buy",
                "confidence": 0.82,
                "strength": 0.7,
                "leverage": 3,
                "reason": "force_run_smoke_test",
            }
        ),
    )

    asyncio.run(agent.update_runtime_config(enabled=False, mode="shadow", cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="manual_smoke", force=True))

    assert result.get("skipped") is not True
    assert result["decision"]["action"] == "buy"
    assert result["execution"]["reason"] == "shadow_mode"


def test_autonomous_agent_auto_start_is_env_controlled(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)
    monkeypatch.setattr(module.settings, "AI_AUTONOMOUS_AGENT_AUTO_START", True, raising=False)
    agent._override["AI_AUTONOMOUS_AGENT_AUTO_START"] = False

    initial = agent.get_runtime_config()
    asyncio.run(agent.update_runtime_config(auto_start=False))
    updated = agent.get_runtime_config()

    assert initial["auto_start"] is True
    assert updated["auto_start"] is True
    assert not agent._overlay_path.exists()


def test_autonomous_agent_runtime_config_exposes_paper_longrun_safety(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")

    asyncio.run(
        agent.update_runtime_config(
            enabled=True,
            mode="execute",
            allow_live=False,
            symbol_mode="auto",
        )
    )

    cfg = agent.get_runtime_config()

    assert cfg["runtime_profile"] == "paper_longrun"
    assert cfg["safety"]["status"] == "ready"
    assert cfg["safety"]["safe_for_paper_longrun"] is True
    assert cfg["safety"]["paper_longrun_profile_ready"] is True
    assert cfg["safety"]["reason_codes"] == []


def test_autonomous_agent_paper_longrun_profile_applies_safe_overrides(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")

    asyncio.run(agent.update_runtime_config(profile="paper_longrun"))

    cfg = agent.get_runtime_config()

    assert cfg["enabled"] is True
    assert cfg["mode"] == "execute"
    assert cfg["allow_live"] is False
    assert cfg["symbol_mode"] == "auto"
    assert cfg["runtime_profile"] == "paper_longrun"
    assert cfg["safety"]["paper_longrun_profile_ready"] is True


def test_autonomous_agent_status_marks_live_mode_unsafe_for_paper_longrun(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "live")

    asyncio.run(
        agent.update_runtime_config(
            enabled=True,
            mode="execute",
            allow_live=False,
            symbol_mode="auto",
        )
    )

    status = agent.get_status()

    assert status["runtime_profile"] == "paper_longrun"
    assert status["safety"]["status"] == "unsafe"
    assert status["safety"]["safe_for_paper_longrun"] is False
    assert status["safety"]["paper_longrun_profile_ready"] is False
    assert "trading_mode_live" in status["safety"]["reason_codes"]


def test_autonomous_agent_auto_mode_uses_expanded_default_universe(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)
    monkeypatch.setattr(module.settings, "AI_AUTONOMOUS_AGENT_SYMBOL_MODE", "auto", raising=False)
    monkeypatch.setattr(module.settings, "AI_AUTONOMOUS_AGENT_UNIVERSE_SYMBOLS", "", raising=False)

    cfg = agent.get_runtime_config()

    assert cfg["symbol_mode"] == "auto"
    assert len(cfg["universe_symbols"]) == 30
    assert "TRX/USDT" in cfg["universe_symbols"]
    assert "SUI/USDT" in cfg["universe_symbols"]
    assert "PEPE/USDT" in cfg["universe_symbols"]


def test_autonomous_agent_exposes_raw_model_action_rewrite(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    class _Agg:
        def to_dict(self):
            return {"direction": "LONG", "confidence": 0.81}

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")
    submit_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(module.execution_engine, "submit_signal", submit_mock)
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(
            return_value={
                "action": "exit",
                "confidence": 0.81,
                "strength": 0.6,
                "reason": "take_profit_hit",
            }
        ),
    )

    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute", allow_live=True, cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    model_output = result["diagnostics"]["model_output"]
    codes = {item.get("code") for item in (result["diagnostics"].get("items") or [])}
    assert result["decision"]["action"] == "hold"
    assert model_output["source"] == "provider"
    assert model_output["raw_action"] == "exit"
    assert model_output["normalized_action"] == "hold"
    assert model_output["action_changed"] is True
    assert "model_action_rewritten" in codes
    assert submit_mock.await_count == 0


def test_autonomous_agent_run_once_blocks_live_when_not_allowed(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    class _Agg:
        def to_dict(self):
            return {"direction": "SHORT", "confidence": 0.75}

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "live")
    submit_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(module.execution_engine, "submit_signal", submit_mock)
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(
            return_value={
                "action": "sell",
                "confidence": 0.86,
                "strength": 0.8,
                "leverage": 5,
                "reason": "live_block_guard",
            }
        ),
    )

    asyncio.run(
        agent.update_runtime_config(
            enabled=True,
            mode="execute",
            allow_live=False,
            cooldown_sec=0,
        )
    )
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    assert result["decision"]["action"] == "sell"
    assert result["decision"]["leverage"] == 1.0
    assert result["execution"]["submitted"] is False
    assert result["execution"]["reason"] == "live_mode_blocked"
    assert submit_mock.await_count == 0


def test_autonomous_agent_same_side_signal_allows_add_when_below_half_cap(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    class _Agg:
        def to_dict(self):
            return {"direction": "SHORT", "confidence": 0.78}

    fake_connector = SimpleNamespace(
        config=SimpleNamespace(default_type="future"),
        get_positions=AsyncMock(
            return_value=[
                {
                    "symbol": "BTCUSDT",
                    "side": "short",
                    "amount": -0.3,
                    "entry_price": 100.0,
                    "current_price": 100.0,
                    "unrealizedPnl": 0.03,
                    "leverage": 2.0,
                }
            ]
        ),
    )

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.exchange_manager, "get_exchange", lambda exchange: fake_connector)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "live")
    monkeypatch.setattr(module.execution_engine, "get_account_equity_snapshot", AsyncMock(return_value=1000.0))
    monkeypatch.setattr(module.execution_engine, "get_strategy_position_cap_notional", lambda **kwargs: 100.0)
    monkeypatch.setattr(module.strategy_manager, "get_strategy_allocation", lambda name: 0.0)
    submit_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(module.execution_engine, "submit_signal", submit_mock)
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(
            return_value={
                "action": "sell",
                "confidence": 0.84,
                "strength": 0.76,
                "leverage": 1,
                "reason": "stay_short",
            }
        ),
    )

    asyncio.run(
        agent.update_runtime_config(
            enabled=True,
            mode="execute",
            symbol="BTC/USDT",
            symbol_mode="manual",
            allow_live=True,
            cooldown_sec=0,
        )
    )
    context_payload, _ = asyncio.run(agent._build_context(agent.get_runtime_config()))
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    assert result["decision"]["action"] == "sell"
    assert context_payload["position"]["side"] == "short"
    assert context_payload["position"]["leverage"] == 2.0
    assert context_payload["position"]["position_notional"] == 30.0
    assert context_payload["position"]["position_cap_notional"] == 100.0
    assert context_payload["position"]["same_direction_exposure_ratio"] == 0.3
    assert result["execution"]["submitted"] is True
    assert submit_mock.await_count == 1
    signal = submit_mock.await_args.args[0]
    assert signal.metadata["same_direction_max_exposure_ratio"] == 0.5
    assert signal.metadata["same_direction_existing_notional"] == 30.0


def test_autonomous_agent_same_side_signal_holds_when_exposure_reaches_half_cap(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    class _Agg:
        def to_dict(self):
            return {"direction": "SHORT", "confidence": 0.78}

    fake_connector = SimpleNamespace(
        config=SimpleNamespace(default_type="future"),
        get_positions=AsyncMock(
            return_value=[
                {
                    "symbol": "BTCUSDT",
                    "side": "short",
                    "amount": -0.6,
                    "entry_price": 100.0,
                    "current_price": 100.0,
                    "unrealizedPnl": 0.06,
                    "leverage": 2.0,
                }
            ]
        ),
    )

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.exchange_manager, "get_exchange", lambda exchange: fake_connector)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "live")
    monkeypatch.setattr(module.execution_engine, "get_account_equity_snapshot", AsyncMock(return_value=1000.0))
    monkeypatch.setattr(module.execution_engine, "get_strategy_position_cap_notional", lambda **kwargs: 100.0)
    monkeypatch.setattr(module.strategy_manager, "get_strategy_allocation", lambda name: 0.0)
    submit_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(module.execution_engine, "submit_signal", submit_mock)
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(
            return_value={
                "action": "sell",
                "confidence": 0.84,
                "strength": 0.76,
                "leverage": 1,
                "reason": "stay_short",
            }
        ),
    )

    asyncio.run(
        agent.update_runtime_config(
            enabled=True,
            mode="execute",
            symbol="BTC/USDT",
            symbol_mode="manual",
            allow_live=True,
            cooldown_sec=0,
        )
    )
    context_payload, _ = asyncio.run(agent._build_context(agent.get_runtime_config()))
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    assert result["decision"]["action"] == "hold"
    assert result["decision"]["reason"].startswith("existing_short_position_limit_reached")
    assert context_payload["position"]["position_notional"] == 60.0
    assert context_payload["position"]["same_direction_exposure_ratio"] == 0.6
    assert result["execution"]["submitted"] is False
    assert submit_mock.await_count == 0


def test_autonomous_agent_holds_when_total_exposure_reaches_ratio_cap(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)
    submit_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(module.execution_engine, "submit_signal", submit_mock)
    monkeypatch.setattr(
        agent,
        "get_symbol_scan",
        AsyncMock(
            return_value={
                "selected_symbol": "BTC/USDT",
                "selection_reason": "manual_symbol",
                "top_candidates": [],
            }
        ),
    )
    monkeypatch.setattr(
        agent,
        "_build_context",
        AsyncMock(
            return_value=(
                {
                    "exchange": "binance",
                    "symbol": "BTC/USDT",
                    "timeframe": "15m",
                    "price": 100.0,
                    "bars": 120,
                    "returns": {"r_1h": 0.01, "r_24h": 0.03},
                    "realized_vol_annualized": 0.25,
                    "market_structure": {"available": True},
                    "aggregated_signal": {
                        "direction": "LONG",
                        "confidence": 0.82,
                        "blocked_by_risk": False,
                        "risk_reason": "",
                    },
                    "event_summary": {"available": False},
                    "position": {},
                    "account_risk": {
                        "trading_mode": "live",
                        "allow_live": True,
                        "execution_permitted_now": True,
                        "min_confidence": 0.58,
                        "account_equity": 1000.0,
                        "position_cap_notional": 100.0,
                        "max_total_exposure_ratio": 0.4,
                        "total_strategy_open_notional": 400.0,
                        "total_exposure_ratio": 0.4,
                        "total_exposure_limit_notional": 400.0,
                        "total_remaining_notional": 0.0,
                        "can_open_more_total": False,
                    },
                    "execution_cost": {"estimated_one_way_cost_bps": 5.0, "estimated_round_trip_cost_bps": 10.0},
                    "research_context": {"available": False},
                    "profile": {},
                    "learning_memory": {},
                    "trading_mode": "live",
                },
                pd.DataFrame(),
            )
        ),
    )
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(
            return_value={
                "action": "buy",
                "confidence": 0.82,
                "strength": 0.7,
                "leverage": 1,
                "reason": "fresh_long",
            }
        ),
    )

    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute", allow_live=True, cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    assert result["decision"]["action"] == "hold"
    assert str(result["decision"]["reason"]).startswith("total_exposure_limit_reached(")
    assert result["execution"]["submitted"] is False
    assert submit_mock.await_count == 0


def test_autonomous_agent_holds_when_total_exposure_reaches_fixed_budget(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)
    submit_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(module.execution_engine, "submit_signal", submit_mock)
    monkeypatch.setattr(
        agent,
        "get_symbol_scan",
        AsyncMock(
            return_value={
                "selected_symbol": "BTC/USDT",
                "selection_reason": "manual_symbol",
                "top_candidates": [],
            }
        ),
    )
    monkeypatch.setattr(
        agent,
        "_build_context",
        AsyncMock(
            return_value=(
                {
                    "exchange": "binance",
                    "symbol": "BTC/USDT",
                    "timeframe": "15m",
                    "price": 100.0,
                    "bars": 120,
                    "returns": {"r_1h": 0.01, "r_24h": 0.03},
                    "realized_vol_annualized": 0.25,
                    "market_structure": {"available": True},
                    "aggregated_signal": {
                        "direction": "LONG",
                        "confidence": 0.82,
                        "blocked_by_risk": False,
                        "risk_reason": "",
                    },
                    "event_summary": {"available": False},
                    "position": {},
                    "account_risk": {
                        "trading_mode": "live",
                        "allow_live": True,
                        "execution_permitted_now": True,
                        "min_confidence": 0.58,
                        "account_equity": 1000.0,
                        "position_cap_notional": 100.0,
                        "max_total_exposure_ratio": 0.4,
                        "max_total_exposure_usdt": 300.0,
                        "total_exposure_limit_mode": "fixed_amount",
                        "total_strategy_open_notional": 300.0,
                        "total_exposure_ratio": 0.3,
                        "total_exposure_limit_notional": 300.0,
                        "total_remaining_notional": 0.0,
                        "can_open_more_total": False,
                    },
                    "execution_cost": {"estimated_one_way_cost_bps": 5.0, "estimated_round_trip_cost_bps": 10.0},
                    "research_context": {"available": False},
                    "profile": {},
                    "learning_memory": {},
                    "trading_mode": "live",
                },
                pd.DataFrame(),
            )
        ),
    )
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(
            return_value={
                "action": "buy",
                "confidence": 0.82,
                "strength": 0.7,
                "leverage": 1,
                "reason": "fresh_long",
            }
        ),
    )

    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute", allow_live=True, cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    assert result["decision"]["action"] == "hold"
    assert "fixed_300.000_usdt" in str(result["decision"]["reason"])
    assert result["execution"]["submitted"] is False
    assert submit_mock.await_count == 0


def test_resolve_account_risk_base_uses_observed_position_map_as_exposure_floor(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    monkeypatch.setattr(module.execution_engine, "get_account_equity_snapshot", AsyncMock(return_value=1000.0))
    monkeypatch.setattr(module.execution_engine, "get_strategy_position_cap_notional", lambda **kwargs: 100.0)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "live")
    monkeypatch.setattr(module.strategy_manager, "get_strategy_allocation", lambda name: 0.0)
    monkeypatch.setattr(
        module.risk_manager,
        "get_risk_report",
        lambda: {
            "risk_level": "high",
            "trading_halted": True,
            "halt_reason": "daily stop triggered",
            "discipline": {
                "fresh_entry_allowed": False,
                "reduce_only": True,
                "degrade_mode": "halted",
                "reasons": ["daily stop triggered"],
            },
            "equity": {
                "daily_pnl_ratio": -0.014,
                "daily_stop_basis_ratio": -0.011,
                "max_drawdown": 0.083,
                "rolling_3d_drawdown": 0.061,
                "rolling_7d_drawdown": 0.094,
            },
        },
    )
    monkeypatch.setattr(agent, "_positions_for_learning_memory", lambda strategy_name: [])
    monkeypatch.setattr(
        agent,
        "_scan_position_map",
        AsyncMock(
            return_value={
                "BTC/USDT": {
                    "side": "long",
                    "quantity": 1.5,
                    "current_price": 100.0,
                },
                "ETH/USDT": {
                    "side": "short",
                    "quantity": 1.0,
                    "entry_price": 250.0,
                },
            }
        ),
    )

    payload = asyncio.run(
        agent._resolve_account_risk_base(
            {
                "strategy_name": "AI_AutonomousAgent",
                "exchange": "binance",
                "account_id": "main",
                "max_total_exposure_ratio": 0.4,
            }
        )
    )

    assert payload["account_equity"] == 1000.0
    assert payload["position_cap_notional"] == 100.0
    assert payload["observed_account_open_notional"] == 400.0
    assert payload["total_strategy_open_notional"] == 400.0
    assert payload["total_exposure_limit_notional"] == 400.0
    assert payload["risk_level"] == "high"
    assert payload["risk_trading_halted"] is True
    assert payload["risk_halt_reason"] == "daily stop triggered"
    assert payload["risk_fresh_entry_allowed"] is False
    assert payload["risk_reduce_only"] is True
    assert payload["risk_degrade_mode"] == "halted"
    assert payload["risk_discipline_reasons"] == ["daily stop triggered"]
    assert payload["risk_daily_pnl_ratio"] == pytest.approx(-0.014)
    assert payload["risk_daily_stop_basis_ratio"] == pytest.approx(-0.011)
    assert payload["risk_max_drawdown"] == pytest.approx(0.083)
    assert payload["risk_rolling_3d_drawdown"] == pytest.approx(0.061)
    assert payload["risk_rolling_7d_drawdown"] == pytest.approx(0.094)


def test_resolve_account_risk_base_prefers_fixed_budget_over_ratio(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    monkeypatch.setattr(module.execution_engine, "get_account_equity_snapshot", AsyncMock(return_value=1000.0))
    monkeypatch.setattr(module.execution_engine, "get_strategy_position_cap_notional", lambda **kwargs: 100.0)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "live")
    monkeypatch.setattr(module.strategy_manager, "get_strategy_allocation", lambda name: 0.0)
    monkeypatch.setattr(agent, "_positions_for_learning_memory", lambda strategy_name: [])
    monkeypatch.setattr(
        agent,
        "_scan_position_map",
        AsyncMock(
            return_value={
                "BTC/USDT": {
                    "side": "long",
                    "quantity": 2.0,
                    "current_price": 100.0,
                },
            }
        ),
    )

    payload = asyncio.run(
        agent._resolve_account_risk_base(
            {
                "strategy_name": "AI_AutonomousAgent",
                "exchange": "binance",
                "account_id": "main",
                "max_total_exposure_ratio": 0.4,
                "max_total_exposure_usdt": 250.0,
            }
        )
    )

    assert payload["account_equity"] == 1000.0
    assert payload["max_total_exposure_ratio"] == pytest.approx(0.4)
    assert payload["max_total_exposure_usdt"] == pytest.approx(250.0)
    assert payload["total_exposure_limit_mode"] == "fixed_amount"
    assert payload["total_exposure_limit_notional"] == pytest.approx(250.0)


def test_agent_runtime_config_leverage_is_fixed_to_one(tmp_path):
    from core.ai.autonomous_agent import AutonomousTradingAgent

    agent = AutonomousTradingAgent(cache_root=tmp_path / "agent_fixed_leverage")
    cfg_before = agent.get_runtime_config()
    assert cfg_before["default_leverage"] == 1.0
    assert cfg_before["max_leverage"] == 1.0

    cfg_after = asyncio.run(
        agent.update_runtime_config(
            default_leverage=9.0,
            max_leverage=12.0,
        )
    )
    assert cfg_after["default_leverage"] == 1.0
    assert cfg_after["max_leverage"] == 1.0


def test_agent_model_feedback_outage_alerts_feishu_after_prolonged_429(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module
    from core.notifications import notification_manager

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    class _Agg:
        def to_dict(self):
            return {"direction": "LONG", "confidence": 0.72}

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "live")
    monkeypatch.setattr(module.execution_engine, "submit_signal", AsyncMock(return_value=True))
    send_mock = AsyncMock(return_value={"feishu": True})
    monkeypatch.setattr(notification_manager, "send_message", send_mock)
    agent._last_model_feedback_at = time.time() - (module._MODEL_FEEDBACK_OUTAGE_ALERT_SEC + 65)
    agent._model_feedback_outage_started_at = time.time() - (module._MODEL_FEEDBACK_OUTAGE_ALERT_SEC + 5)
    agent._model_feedback_failure_streak = 1
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(side_effect=RuntimeError('codex_http_429:{"code":"USAGE_LIMIT_EXCEEDED"}')),
    )

    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute", allow_live=True, cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    assert result["decision"]["action"] == "hold"
    assert "model_error:codex_http_429" in result["decision"]["reason"]
    assert send_mock.await_count == 1
    assert agent.get_status()["model_feedback_guard"]["last_failure_kind"] == "rate_limit"


def test_agent_model_feedback_first_failure_after_idle_does_not_alert_immediately(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module
    from core.notifications import notification_manager

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    class _Agg:
        def to_dict(self):
            return {"direction": "LONG", "confidence": 0.72}

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "live")
    monkeypatch.setattr(module.execution_engine, "submit_signal", AsyncMock(return_value=True))
    send_mock = AsyncMock(return_value={"feishu": True})
    monkeypatch.setattr(notification_manager, "send_message", send_mock)
    agent._last_model_feedback_at = time.time() - (module._MODEL_FEEDBACK_OUTAGE_ALERT_SEC + 65)
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(side_effect=RuntimeError('codex_http_429:{"code":"USAGE_LIMIT_EXCEEDED"}')),
    )

    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute", allow_live=True, cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    assert result["decision"]["action"] == "hold"
    assert send_mock.await_count == 0
    guard = agent.get_status()["model_feedback_guard"]
    assert guard["last_failure_kind"] == "rate_limit"
    assert guard["failure_streak"] == 1
    assert guard["alert_sent_at"] is None


def test_agent_model_feedback_outage_alert_is_suppressed_in_paper_mode(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module
    from core.notifications import notification_manager

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    class _Agg:
        def to_dict(self):
            return {"direction": "LONG", "confidence": 0.72}

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")
    monkeypatch.setattr(module.execution_engine, "submit_signal", AsyncMock(return_value=True))
    send_mock = AsyncMock(return_value={"feishu": True})
    monkeypatch.setattr(notification_manager, "send_message", send_mock)
    agent._last_model_feedback_at = time.time() - (module._MODEL_FEEDBACK_OUTAGE_ALERT_SEC + 65)
    agent._model_feedback_outage_started_at = time.time() - (module._MODEL_FEEDBACK_OUTAGE_ALERT_SEC + 5)
    agent._model_feedback_failure_streak = 1
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(side_effect=RuntimeError('codex_http_429:{"code":"USAGE_LIMIT_EXCEEDED"}')),
    )

    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute", allow_live=False, cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    assert result["decision"]["action"] == "hold"
    assert send_mock.await_count == 0
    guard = agent.get_status()["model_feedback_guard"]
    assert guard["last_failure_kind"] == "rate_limit"
    assert guard["alert_sent_at"] is None


def test_agent_live_execution_policy_restriction_is_classified_locally(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    class _Agg:
        def to_dict(self):
            return {"direction": "LONG", "confidence": 0.78}

    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-openai", raising=False)
    monkeypatch.setattr(settings, "ANTHROPIC_API_KEY", "", raising=False)
    monkeypatch.setattr(settings, "ZHIPU_API_KEY", "", raising=False)
    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "live")
    monkeypatch.setattr(module.execution_engine, "submit_signal", AsyncMock(return_value=True))

    provider_call = AsyncMock(return_value={"action": "buy", "confidence": 0.91, "strength": 0.9, "reason": "should_not_run"})
    monkeypatch.setattr(agent, "_call_provider", provider_call)

    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute", provider="codex", allow_live=True, cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    diagnostics = result["diagnostics"]
    assert provider_call.await_count == 0
    assert result["decision"]["action"] == "hold"
    assert "live_trading_not_permitted" in str(result["decision"]["reason"] or "")
    assert diagnostics["primary"]["code"] == "model_policy_restricted"
    assert diagnostics["model_feedback"]["kind"] == "policy_restricted"


def test_agent_live_execution_falls_back_to_alternative_provider(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    class _Agg:
        def to_dict(self):
            return {"direction": "LONG", "confidence": 0.81}

    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-openai", raising=False)
    monkeypatch.setattr(settings, "ANTHROPIC_API_KEY", "sk-claude", raising=False)
    monkeypatch.setattr(settings, "ZHIPU_API_KEY", "", raising=False)
    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "live")
    monkeypatch.setattr(module.execution_engine, "submit_signal", AsyncMock(return_value=True))

    seen = {}

    async def _fake_call_provider(**kwargs):
        seen.update(kwargs)
        return {
            "action": "buy",
            "confidence": 0.93,
            "strength": 0.9,
            "leverage": 1.0,
            "stop_loss_pct": 0.02,
            "take_profit_pct": 0.04,
            "reason": "fallback_provider",
        }

    monkeypatch.setattr(agent, "_call_provider", _fake_call_provider)

    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute", provider="codex", allow_live=True, cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    assert seen["provider"] == "claude"
    assert result["decision"]["action"] == "buy"
    assert result["execution"]["submitted"] is True


def test_agent_model_feedback_classifies_503_service_unavailable(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    class _Agg:
        def to_dict(self):
            return {"direction": "SHORT", "confidence": 0.79}

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")
    monkeypatch.setattr(module.execution_engine, "submit_signal", AsyncMock(return_value=True))
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(side_effect=RuntimeError('codex_http_503:{"error":{"message":"Service temporarily unavailable","type":"api_error"}}')),
    )

    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute", cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    diagnostics = result["diagnostics"]
    assert result["decision"]["action"] == "hold"
    assert diagnostics["primary"]["code"] == "model_service_unavailable"
    assert diagnostics["primary"]["label"] == "模型服务暂时不可用 (503)"
    assert "本轮已回退为 hold" in diagnostics["primary"]["detail"]
    assert "503" in diagnostics["primary"]["label"]
    assert diagnostics["model_feedback"]["kind"] == "service_unavailable"
    assert diagnostics["model_feedback"]["http_status"] == 503
    assert result["status"]["model_feedback_guard"]["last_failure_kind"] == "service_unavailable"


def test_agent_model_feedback_guard_hard_timeout_alerts_and_ends_round(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module
    from core.notifications import notification_manager

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    class _Agg:
        def to_dict(self):
            return {"direction": "SHORT", "confidence": 0.74}

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "live")
    monkeypatch.setattr(module.execution_engine, "submit_signal", AsyncMock(return_value=True))
    monkeypatch.setattr(module, "_MODEL_FEEDBACK_HARD_TIMEOUT_SEC", 0.01)
    monkeypatch.setattr(module, "_MODEL_FEEDBACK_OUTAGE_ALERT_SEC", 0.0)
    send_mock = AsyncMock(return_value={"feishu": True})
    monkeypatch.setattr(notification_manager, "send_message", send_mock)

    async def _slow_call_provider(**kwargs):
        await asyncio.sleep(0.05)
        return {
            "action": "sell",
            "confidence": 0.81,
            "strength": 0.72,
            "leverage": 1,
            "reason": "should_not_complete",
        }

    monkeypatch.setattr(agent, "_call_provider", _slow_call_provider)

    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute", allow_live=True, cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    assert result["decision"]["action"] == "hold"
    assert "model_feedback_guard_timeout" in result["decision"]["reason"]
    assert send_mock.await_count == 1
    assert agent.get_status()["model_feedback_guard"]["last_failure_kind"] == "timeout"


# ── Overlay persistence ───────────────────────────────────────────────────────

def test_symbol_scan_reuses_recent_cached_snapshot(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_scan_cache")
    monkeypatch.setattr(
        agent,
        "get_runtime_config",
        lambda: {
            "exchange": "binance",
            "symbol": "BTC/USDT",
            "symbol_mode": "auto",
            "universe_symbols": ["BTC/USDT", "ETH/USDT"],
            "selection_top_n": 10,
            "timeframe": "15m",
            "lookback_bars": 240,
            "account_id": "main",
        },
    )
    monkeypatch.setattr(agent, "_cfg_with_learning_overlays", lambda cfg, force_learning_refresh=False: dict(cfg))
    scan_position_map_mock = AsyncMock(return_value={})
    build_context_mock = AsyncMock(side_effect=AssertionError("cached scan should not rebuild contexts"))
    monkeypatch.setattr(agent, "_scan_position_map", scan_position_map_mock)
    monkeypatch.setattr(agent, "_build_context", build_context_mock)

    agent._last_symbol_scan = {
        "generated_at": module._utc_now().isoformat(),
        "symbol_mode": "auto",
        "configured_symbol": "BTC/USDT",
        "selected_symbol": "ETH/USDT",
        "selection_reason": "top_ranked_tradable_symbol",
        "candidate_count": 2,
        "top_n": 10,
        "top_candidates": [
            {"rank": 1, "symbol": "ETH/USDT", "score": 0.88},
            {"rank": 2, "symbol": "BTC/USDT", "score": 0.51},
        ],
    }

    result = asyncio.run(agent.get_symbol_scan(limit=10, force=False))

    assert result["selected_symbol"] == "ETH/USDT"
    assert scan_position_map_mock.await_count == 1
    assert build_context_mock.await_count == 0


def test_run_once_loop_uses_non_forced_symbol_scan(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_loop_scan")
    get_symbol_scan_mock = AsyncMock(
        return_value={
            "generated_at": "2026-01-01T00:00:00+00:00",
            "symbol_mode": "manual",
            "configured_symbol": "BTC/USDT",
            "selected_symbol": "BTC/USDT",
            "selection_reason": "manual_symbol",
            "candidate_count": 1,
            "top_n": 10,
            "top_candidates": [],
        }
    )
    monkeypatch.setattr(agent, "get_symbol_scan", get_symbol_scan_mock)
    monkeypatch.setattr(
        agent,
        "_build_context",
        AsyncMock(
            return_value=(
                {
                    "exchange": "binance",
                    "symbol": "BTC/USDT",
                    "timeframe": "15m",
                    "price": 100.0,
                    "bars": 240,
                    "returns": {"r_1h": 0.0, "r_24h": 0.0},
                    "realized_vol_annualized": 0.1,
                    "market_structure": {"available": True},
                    "aggregated_signal": {"direction": "LONG", "confidence": 0.82, "blocked_by_risk": False, "risk_reason": ""},
                    "event_summary": {"available": False},
                    "position": {},
                    "account_risk": {"trading_mode": "paper", "min_confidence": 0.58},
                    "execution_cost": {"estimated_one_way_cost_bps": 2.0, "estimated_round_trip_cost_bps": 4.0},
                    "research_context": {"available": False},
                    "profile": {},
                    "learning_memory": {},
                    "trading_mode": "paper",
                },
                pd.DataFrame(),
            )
        ),
    )
    monkeypatch.setattr(agent, "_build_event_summary", AsyncMock(return_value={"available": True, "events_count": 0}))
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(
            return_value={
                "action": "hold",
                "confidence": 0.6,
                "strength": 0.2,
                "leverage": 1.0,
                "stop_loss_pct": 0.02,
                "take_profit_pct": 0.04,
                "reason": "wait",
            }
        ),
    )
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")
    monkeypatch.setattr(module.execution_engine, "submit_signal", AsyncMock(return_value=True))

    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute", cooldown_sec=0))
    asyncio.run(agent.run_once(trigger="loop", force=False))

    assert get_symbol_scan_mock.await_args.kwargs["force"] is False


def test_build_context_light_scan_reuses_empty_position_map_without_live_lookup(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_light_scan_positions")

    class _Agg:
        def to_dict(self):
            return {"direction": "FLAT", "confidence": 0.0}

    monkeypatch.setattr(agent, "_load_market_data", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(agent, "_resolve_last_price", AsyncMock(return_value=100.0))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    resolve_position_mock = AsyncMock(side_effect=AssertionError("light scan should reuse empty position map"))
    monkeypatch.setattr(agent, "_resolve_position_payload", resolve_position_mock)

    context, market_data = asyncio.run(
        agent._build_context(
            {
                "exchange": "binance",
                "symbol": "BTC/USDT",
                "timeframe": "15m",
                "mode": "execute",
                "allow_live": True,
                "min_confidence": 0.58,
                "default_stop_loss_pct": 0.02,
                "default_take_profit_pct": 0.04,
                "strategy_name": "AI_AutonomousAgent",
                "_light_symbol_scan": True,
                "_scan_position_map": {},
                "_skip_event_summary": True,
            }
        )
    )

    assert market_data is not None
    assert context["position"] == {}
    assert resolve_position_mock.await_count == 0


def test_get_symbol_scan_auto_uses_two_stage_refresh(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_two_stage_scan")
    symbols = [f"SYM{i}/USDT" for i in range(20)]
    call_log = []

    monkeypatch.setattr(
        agent,
        "get_runtime_config",
        lambda: {
            "exchange": "binance",
            "symbol": "SYM0/USDT",
            "symbol_mode": "auto",
            "universe_symbols": symbols,
            "selection_top_n": 3,
            "timeframe": "15m",
            "lookback_bars": 240,
            "account_id": "main",
            "min_confidence": 0.58,
        },
    )
    monkeypatch.setattr(agent, "_cfg_with_learning_overlays", lambda cfg, force_learning_refresh=False: dict(cfg))
    monkeypatch.setattr(agent, "_scan_position_map", AsyncMock(return_value={}))

    async def _fake_build_context(local_cfg):
        call_log.append((str(local_cfg.get("symbol")), bool(local_cfg.get("_scan_skip_live_market"))))
        return {"symbol": str(local_cfg.get("symbol") or "")}, pd.DataFrame()

    def _fake_score(local_cfg, context_payload):
        symbol = str(context_payload.get("symbol") or "")
        idx = int(symbol.split("/", 1)[0].replace("SYM", ""))
        return {
            "symbol": symbol,
            "price": 1.0 + idx,
            "direction": "LONG",
            "confidence": round(0.9 - idx * 0.01, 6),
            "score": float(100 - idx),
            "tradable_now": True,
            "blocked_by_risk": False,
            "risk_reason": "",
            "bars": 240,
            "realized_vol_annualized": 0.2,
            "threshold_gap": 0.1,
            "summary": f"rank {idx}",
            "has_position": False,
            "position_side": "",
            "position_source": "",
            "position_unrealized_pnl": 0.0,
            "position_unrealized_pnl_pct": 0.0,
            "research": {
                "candidate_id": "",
                "strategy": "",
                "status": "",
                "promotion_target": "",
                "validation_reasons": [],
            },
        }

    monkeypatch.setattr(agent, "_build_context", _fake_build_context)
    monkeypatch.setattr(agent, "_score_symbol_candidate", _fake_score)

    result = asyncio.run(agent.get_symbol_scan(limit=3, force=True))

    prescan_calls = [symbol for symbol, skip_live in call_log if skip_live]
    rerank_calls = [symbol for symbol, skip_live in call_log if not skip_live]

    assert result["selected_symbol"] == "SYM0/USDT"
    assert len(prescan_calls) == len(symbols)
    assert len(rerank_calls) == 8
    assert prescan_calls.count("SYM0/USDT") == 1
    assert rerank_calls.count("SYM0/USDT") == 1
    assert rerank_calls.count("SYM19/USDT") == 0


def test_get_symbol_scan_preview_uses_fast_mode_without_caching(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_preview_scan")
    call_log = []

    monkeypatch.setattr(
        agent,
        "get_runtime_config",
        lambda: {
            "exchange": "binance",
            "symbol": "BTC/USDT",
            "symbol_mode": "manual",
            "timeframe": "15m",
            "lookback_bars": 240,
            "account_id": "main",
            "min_confidence": 0.58,
            "selection_top_n": 5,
        },
    )
    monkeypatch.setattr(agent, "_cfg_with_learning_overlays", lambda cfg, force_learning_refresh=False: dict(cfg))

    async def _fake_build_context(local_cfg):
        call_log.append(
            (
                bool(local_cfg.get("_preview_symbol_scan")),
                bool(local_cfg.get("_scan_skip_live_market")),
            )
        )
        return {"symbol": str(local_cfg.get("symbol") or "BTC/USDT")}, pd.DataFrame()

    monkeypatch.setattr(agent, "_build_context", _fake_build_context)
    monkeypatch.setattr(
        agent,
        "_score_symbol_candidate",
        lambda local_cfg, context_payload: {
            "symbol": "BTC/USDT",
            "price": 1.0,
            "direction": "LONG",
            "confidence": 0.7,
            "score": 0.7,
            "tradable_now": True,
            "blocked_by_risk": False,
            "risk_reason": "",
            "bars": 240,
            "realized_vol_annualized": 0.2,
            "threshold_gap": 0.1,
            "summary": "preview",
            "has_position": False,
            "position_side": "",
            "position_source": "",
            "position_unrealized_pnl": 0.0,
            "position_unrealized_pnl_pct": 0.0,
            "research": {
                "candidate_id": "",
                "strategy": "",
                "status": "",
                "promotion_target": "",
                "validation_reasons": [],
            },
        },
    )

    result = asyncio.run(agent.get_symbol_scan_preview(limit=5, force=True))

    assert result["selected_symbol"] == "BTC/USDT"
    assert call_log == [(True, True)]
    assert agent._last_symbol_scan is None
    assert agent._last_preview_symbol_scan is not None


def test_get_symbol_scan_preview_auto_mode_uses_single_pass_scan(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_preview_auto_scan")
    symbols = [f"SYM{i}/USDT" for i in range(12)]
    call_log = []

    monkeypatch.setattr(
        agent,
        "get_runtime_config",
        lambda: {
            "exchange": "binance",
            "symbol": "BTC/USDT",
            "symbol_mode": "auto",
            "universe_symbols": symbols,
            "selection_top_n": 5,
            "timeframe": "15m",
            "lookback_bars": 240,
            "account_id": "main",
            "min_confidence": 0.58,
        },
    )
    monkeypatch.setattr(agent, "_cfg_with_learning_overlays", lambda cfg, force_learning_refresh=False: dict(cfg))
    monkeypatch.setattr(agent, "_scan_position_map", AsyncMock(return_value={}))

    async def _fake_build_context(local_cfg):
        call_log.append((str(local_cfg.get("symbol") or ""), bool(local_cfg.get("_scan_skip_live_market"))))
        return {"symbol": str(local_cfg.get("symbol") or "")}, pd.DataFrame()

    def _fake_score(local_cfg, context_payload):
        symbol = str(context_payload.get("symbol") or "")
        idx = int(symbol.split("/", 1)[0].replace("SYM", ""))
        return {
            "symbol": symbol,
            "price": 1.0 + idx,
            "direction": "LONG",
            "confidence": round(0.9 - idx * 0.01, 6),
            "score": float(100 - idx),
            "tradable_now": True,
            "blocked_by_risk": False,
            "risk_reason": "",
            "bars": 240,
            "realized_vol_annualized": 0.2,
            "threshold_gap": 0.1,
            "summary": f"rank {idx}",
            "has_position": False,
            "position_side": "",
            "position_source": "",
            "position_unrealized_pnl": 0.0,
            "position_unrealized_pnl_pct": 0.0,
            "research": {
                "candidate_id": "",
                "strategy": "",
                "status": "",
                "promotion_target": "",
                "validation_reasons": [],
            },
        }

    monkeypatch.setattr(agent, "_build_context", _fake_build_context)
    monkeypatch.setattr(agent, "_score_symbol_candidate", _fake_score)

    result = asyncio.run(agent.get_symbol_scan_preview(limit=5, force=True))

    assert result["selected_symbol"] == "SYM0/USDT"
    assert len(call_log) == len(symbols) + 1
    assert call_log[0][0] == "BTC/USDT"
    assert all(skip_live for _, skip_live in call_log)
    assert agent._last_preview_symbol_scan is not None


def test_get_symbol_scan_preview_reuses_recent_cache_during_force_refresh(tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_preview_cache")
    agent._last_preview_symbol_scan = {
        "generated_at": module._utc_now().isoformat(),
        "symbol_mode": "manual",
        "configured_symbol": "BTC/USDT",
        "selected_symbol": "BTC/USDT",
        "selection_reason": "manual_symbol",
        "candidate_count": 1,
        "top_n": 5,
        "top_candidates": [
            {
                "rank": 1,
                "symbol": "BTC/USDT",
                "score": 0.8,
            }
        ],
    }

    result = asyncio.run(agent.get_symbol_scan_preview(limit=5, force=True))

    assert result["selected_symbol"] == "BTC/USDT"
    assert result["scan_meta"]["stale"] is False
    assert result["scan_meta"]["max_age_sec"] == float(module._PREVIEW_SYMBOL_SCAN_CACHE_MAX_AGE_SEC)


def test_get_symbol_scan_preview_snapshot_falls_back_to_last_symbol_scan(tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_preview_snapshot")
    agent._last_symbol_scan = {
        "generated_at": module._utc_now().isoformat(),
        "symbol_mode": "manual",
        "configured_symbol": "BTC/USDT",
        "selected_symbol": "BTC/USDT",
        "selection_reason": "manual_symbol",
        "candidate_count": 1,
        "top_n": 5,
        "top_candidates": [{"rank": 1, "symbol": "BTC/USDT", "score": 0.9}],
    }

    result = agent.get_symbol_scan_preview_snapshot(limit=5)

    assert result is not None
    assert result["selected_symbol"] == "BTC/USDT"
    assert result["scan_meta"]["actual_scan_fallback"] is True
    assert result["scan_meta"]["max_age_sec"] == float(module._PREVIEW_SYMBOL_SCAN_CACHE_MAX_AGE_SEC)


def test_get_symbol_scan_preview_snapshot_reuses_recent_stale_preview(tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_preview_stale_snapshot")
    stale_generated_at = (module._utc_now() - timedelta(seconds=25)).isoformat()
    agent._last_preview_symbol_scan = {
        "generated_at": stale_generated_at,
        "symbol_mode": "auto",
        "configured_symbol": "BTC/USDT",
        "selected_symbol": "ETH/USDT",
        "selection_reason": "top_ranked_tradable_symbol",
        "candidate_count": 2,
        "top_n": 5,
        "top_candidates": [
            {"rank": 1, "symbol": "ETH/USDT", "score": 0.9},
            {"rank": 2, "symbol": "BTC/USDT", "score": 0.8},
        ],
    }

    result = agent.get_symbol_scan_preview_snapshot(limit=5)

    assert result is not None
    assert result["selected_symbol"] == "ETH/USDT"
    assert result["scan_meta"]["stale"] is True
    assert result["scan_meta"]["stale_preview_fallback"] is True


def test_get_status_exposes_preview_symbol_scan(tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_preview_status")
    agent._last_preview_symbol_scan = {
        "generated_at": module._utc_now().isoformat(),
        "symbol_mode": "auto",
        "configured_symbol": "BTC/USDT",
        "selected_symbol": "ETH/USDT",
        "selection_reason": "top_ranked_tradable_symbol",
        "candidate_count": 2,
        "top_n": 5,
        "top_candidates": [
            {"rank": 1, "symbol": "ETH/USDT", "score": 0.9},
            {"rank": 2, "symbol": "BTC/USDT", "score": 0.8},
        ],
    }

    status = agent.get_status()

    assert status["preview_symbol_scan"] is not None
    assert status["preview_symbol_scan"]["selected_symbol"] == "ETH/USDT"
    assert status["preview_symbol_scan_meta"]["stale"] is False
    assert status["preview_symbol_scan_meta"]["max_age_sec"] == float(module._PREVIEW_SYMBOL_SCAN_CACHE_MAX_AGE_SEC)


def test_ensure_symbol_scan_preview_warm_deduplicates_background_task(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_preview_warm")
    started = 0
    release = asyncio.Event()

    async def _fake_preview(*, limit=None, force=False):
        nonlocal started
        started += 1
        await release.wait()
        return {"selected_symbol": "BTC/USDT"}

    monkeypatch.setattr(agent, "get_symbol_scan_preview", _fake_preview)

    async def _exercise():
        assert agent.ensure_symbol_scan_preview_warm(limit=5) is True
        assert agent.ensure_symbol_scan_preview_warm(limit=5) is False
        release.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    asyncio.run(_exercise())

    assert started == 1
    assert agent._preview_symbol_scan_task is None


def test_build_symbol_scan_preview_pending_payload_marks_pending(tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_preview_pending")

    payload = agent.build_symbol_scan_preview_pending_payload(limit=5, reason="preview timeout")

    assert payload["selection_reason"] == "preview_pending"
    assert payload["top_n"] == 5
    assert payload["top_candidates"] == []
    assert payload["scan_meta"]["pending"] is True
    assert "preview timeout" in payload["scan_meta"]["fallback_reason"]


def test_run_once_holds_when_market_data_is_stale_with_live_connector(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_stale_guard")
    stale_ts = "2026-01-01T00:00:00+00:00"
    submit_mock = AsyncMock(return_value=False)

    monkeypatch.setattr(agent, "get_symbol_scan", AsyncMock(return_value={"selected_symbol": "BTC/USDT"}))
    monkeypatch.setattr(
        agent,
        "_build_context",
        AsyncMock(
            return_value=(
                {
                    "exchange": "binance",
                    "symbol": "BTC/USDT",
                    "price": 123.0,
                    "market_structure": {
                        "available": True,
                        "last_bar_at": stale_ts,
                    },
                    "position": {},
                    "aggregated_signal": {
                        "direction": "LONG",
                        "confidence": 0.9,
                        "blocked_by_risk": False,
                        "risk_reason": "",
                    },
                    "research_context": {"available": False},
                    "execution_cost": {},
                },
                pd.DataFrame(),
            )
        ),
    )
    monkeypatch.setattr(
        module.exchange_manager,
        "get_exchange",
        lambda name: SimpleNamespace(get_klines=AsyncMock(return_value=[])),
    )
    monkeypatch.setattr(module.execution_engine, "submit_signal", submit_mock)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")
    monkeypatch.setattr(agent, "_call_provider", AsyncMock(side_effect=AssertionError("stale-data guard should skip provider")))

    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute", cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    assert result["decision"]["action"] == "hold"
    assert "stale_market_data" in str(result["decision"]["reason"] or "")
    assert submit_mock.await_count == 0


def test_run_once_allows_small_close_latency_before_stale_guard(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_stale_grace")
    provider_mock = AsyncMock(
        return_value={
            "action": "hold",
            "confidence": 0.61,
            "strength": 0.2,
            "leverage": 1.0,
            "reason": "provider_called",
        }
    )

    monkeypatch.setattr(agent, "get_symbol_scan", AsyncMock(return_value={"selected_symbol": "BTC/USDT"}))
    monkeypatch.setattr(
        agent,
        "_build_context",
        AsyncMock(
            return_value=(
                {
                    "exchange": "binance",
                    "symbol": "BTC/USDT",
                    "timeframe": "15m",
                    "price": 123.0,
                    "market_structure": {
                        "available": True,
                        "last_bar_at": "2026-01-01T11:45:00+00:00",
                        "bar_interval_sec": 900,
                    },
                    "position": {},
                    "aggregated_signal": {
                        "direction": "LONG",
                        "confidence": 0.9,
                        "blocked_by_risk": False,
                        "risk_reason": "",
                    },
                    "research_context": {"available": False},
                    "execution_cost": {},
                },
                pd.DataFrame(),
            )
        ),
    )
    monkeypatch.setattr(
        module.exchange_manager,
        "get_exchange",
        lambda name: SimpleNamespace(get_klines=AsyncMock(return_value=[])),
    )
    monkeypatch.setattr(module.execution_engine, "submit_signal", AsyncMock(return_value=False))
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")
    monkeypatch.setattr(agent, "_call_provider", provider_mock)
    monkeypatch.setattr(module, "_utc_now", lambda: datetime(2026, 1, 1, 12, 15, 6, tzinfo=timezone.utc))

    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute", cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    assert provider_mock.await_count == 1
    assert result["decision"]["reason"] == "provider_called"


def test_context_market_data_age_sec_uses_closed_bar_time(tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_age_guard")
    context_payload = {
        "timeframe": "15m",
        "market_structure": {
            "available": True,
            "last_bar_at": "2026-01-01T11:45:00+00:00",
            "bar_interval_sec": 900,
        },
    }

    original_now = module._utc_now
    try:
        module._utc_now = lambda: datetime(2026, 1, 1, 12, 5, tzinfo=timezone.utc)
        assert agent._context_market_data_age_sec(context_payload) == 300.0
    finally:
        module._utc_now = original_now


def test_update_runtime_config_clears_cached_symbol_scan(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_scan_reset")
    monkeypatch.setattr(agent, "_save_overlay", lambda: None)
    agent._last_symbol_scan = {
        "generated_at": module._utc_now().isoformat(),
        "symbol_mode": "manual",
        "configured_symbol": "BTC/USDT",
        "selected_symbol": "BTC/USDT",
        "selection_reason": "manual_symbol",
        "candidate_count": 1,
        "top_n": 10,
        "top_candidates": [{"rank": 1, "symbol": "BTC/USDT", "score": 0.5}],
    }
    agent._last_preview_symbol_scan = {
        "generated_at": module._utc_now().isoformat(),
        "symbol_mode": "manual",
        "configured_symbol": "BTC/USDT",
        "selected_symbol": "BTC/USDT",
        "selection_reason": "manual_symbol",
        "candidate_count": 1,
        "top_n": 10,
        "top_candidates": [{"rank": 1, "symbol": "BTC/USDT", "score": 0.5}],
    }

    asyncio.run(agent.update_runtime_config(symbol="ETH/USDT"))

    assert agent.get_status()["last_symbol_scan"] is None
    assert agent.get_status()["preview_symbol_scan"] is None


def test_get_status_hides_stale_symbol_scan(tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_stale_scan")
    agent._last_symbol_scan = {
        "generated_at": "2026-01-01T00:00:00+00:00",
        "symbol_mode": "auto",
        "configured_symbol": "BTC/USDT",
        "selected_symbol": "BTC/USDT",
        "selection_reason": "top_ranked_watchlist_symbol",
        "candidate_count": 1,
        "top_n": 10,
        "top_candidates": [{"rank": 1, "symbol": "BTC/USDT", "score": 0.5}],
    }

    status = agent.get_status()

    assert status["last_symbol_scan"] is None
    assert status["last_symbol_scan_meta"]["stale"] is True
    assert status["last_symbol_scan_meta"]["available"] is True


def test_trigger_run_once_queues_background_run_and_exposes_manual_status(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_manual_trigger")
    release_run = asyncio.Event()

    async def _fake_run_once_impl(*, trigger: str = "manual", force: bool = False, request_id: str):
        await release_run.wait()
        return {
            "request_id": request_id,
            "timestamp": module._utc_now().isoformat(),
            "trigger": trigger,
            "decision": {"action": "hold", "reason": "queued_test"},
            "execution": {"submitted": False, "reason": "hold"},
            "selection": {"selected_symbol": "BTC/USDT", "selection_reason": "manual_symbol"},
        }

    monkeypatch.setattr(agent, "_run_once_impl", _fake_run_once_impl)

    async def _exercise():
        response = await agent.trigger_run_once(trigger="api_manual", force=True)
        assert response["accepted"] is True
        assert response["request"]["request_id"]
        assert response["status"]["manual_run"]["request_id"] == response["request"]["request_id"]

        await asyncio.sleep(0)
        status_running = agent.get_status()
        assert status_running["manual_run"]["active"] is True
        assert status_running["manual_run"]["state"] == "running"
        assert status_running["run_cycle"]["active"] is True

        release_run.set()
        await asyncio.wait_for(agent._manual_run_task, timeout=1.0)

        status_done = agent.get_status()
        assert status_done["manual_run"]["active"] is False
        assert status_done["manual_run"]["state"] == "completed"
        assert status_done["last_manual_run_result"]["request_id"] == response["request"]["request_id"]
        assert status_done["last_manual_run_result"]["selection"]["selected_symbol"] == "BTC/USDT"

    asyncio.run(_exercise())


def test_trigger_run_once_returns_pending_when_manual_run_already_exists(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_manual_pending")
    gate = asyncio.Event()

    async def _fake_run_once_impl(*, trigger: str = "manual", force: bool = False, request_id: str):
        await gate.wait()
        return {
            "request_id": request_id,
            "timestamp": module._utc_now().isoformat(),
            "trigger": trigger,
            "decision": {"action": "hold", "reason": "pending_test"},
            "execution": {"submitted": False, "reason": "hold"},
            "selection": {"selected_symbol": "BTC/USDT", "selection_reason": "manual_symbol"},
        }

    monkeypatch.setattr(agent, "_run_once_impl", _fake_run_once_impl)

    async def _exercise():
        first = await agent.trigger_run_once(trigger="api_manual", force=True)
        await asyncio.sleep(0)
        second = await agent.trigger_run_once(trigger="api_manual", force=True)
        assert first["accepted"] is True
        assert second["accepted"] is False
        assert second["busy"] is True
        assert second["reason"] == "manual_run_pending"
        gate.set()
        await asyncio.wait_for(agent._manual_run_task, timeout=1.0)

    asyncio.run(_exercise())


def test_agent_config_persists_to_overlay(tmp_path):
    """update_runtime_config writes overlay that is reloaded by a new agent instance."""
    from core.ai.autonomous_agent import AutonomousTradingAgent

    agent = AutonomousTradingAgent(cache_root=tmp_path / "agent_a")
    asyncio.run(
        agent.update_runtime_config(
            enabled=True,
            allow_live=True,
            cooldown_sec=60,
            max_total_exposure_ratio=0.35,
            max_total_exposure_usdt=250.0,
        )
    )

    overlay_path = agent._overlay_path
    assert overlay_path.exists(), "overlay file should have been written"
    data = json.loads(overlay_path.read_text())
    assert data["AI_AUTONOMOUS_AGENT_ENABLED"] is True
    assert data["AI_AUTONOMOUS_AGENT_ALLOW_LIVE"] is True
    assert data["AI_AUTONOMOUS_AGENT_COOLDOWN_SEC"] == 60
    assert data["AI_AUTONOMOUS_AGENT_MAX_TOTAL_EXPOSURE_RATIO"] == pytest.approx(0.35)
    assert data["AI_AUTONOMOUS_AGENT_MAX_TOTAL_EXPOSURE_USDT"] == pytest.approx(250.0)

    # New agent reading same overlay
    agent2 = AutonomousTradingAgent(cache_root=tmp_path / "agent_b")
    agent2._overlay_path = overlay_path
    agent2._load_overlay()
    cfg = agent2.get_runtime_config()
    assert cfg["enabled"] is True
    assert cfg["allow_live"] is True
    assert cfg["cooldown_sec"] == 60
    assert cfg["max_total_exposure_ratio"] == pytest.approx(0.35)
    assert cfg["max_total_exposure_usdt"] == pytest.approx(250.0)
    assert cfg["total_exposure_limit_mode"] == "fixed_amount"


def test_agent_corrupt_overlay_safe_start(tmp_path):
    """A corrupt overlay must not prevent agent startup."""
    from core.ai.autonomous_agent import AutonomousTradingAgent

    agent = AutonomousTradingAgent(cache_root=tmp_path / "agent_corrupt")
    agent._overlay_path.parent.mkdir(parents=True, exist_ok=True)
    agent._overlay_path.write_text("{ corrupt json", encoding="utf-8")
    agent._load_overlay()  # must not raise
    cfg = agent.get_runtime_config()
    assert isinstance(cfg, dict)
    assert "enabled" in cfg


def test_agent_runtime_config_falls_back_to_openai_when_glm_unavailable(tmp_path, monkeypatch):
    """Agent runtime should auto-switch off stale GLM config when only OpenAI is available."""
    from core.ai.autonomous_agent import AutonomousTradingAgent

    agent = AutonomousTradingAgent(cache_root=tmp_path / "agent_fallback")
    agent._overlay_path.parent.mkdir(parents=True, exist_ok=True)
    agent._overlay_path.write_text(
        json.dumps(
            {
                "AI_AUTONOMOUS_AGENT_ENABLED": True,
                "AI_AUTONOMOUS_AGENT_PROVIDER": "glm",
                "AI_AUTONOMOUS_AGENT_MODEL": "GLM-4.5-Air",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(settings, "OPENAI_API_KEY", "sk-openai", raising=False)
    monkeypatch.setattr(settings, "OPENAI_MODEL", "gpt-5.4", raising=False)
    monkeypatch.setattr(settings, "ANTHROPIC_API_KEY", "", raising=False)
    monkeypatch.setattr(settings, "ZHIPU_API_KEY", "", raising=False)

    agent._load_overlay()
    cfg = agent.get_runtime_config()

    assert cfg["provider"] == "codex"
    assert cfg["model"] == "gpt-5.4"
    assert cfg["provider_requested"] == "glm"
    assert cfg["provider_fallback"] is True


def test_agent_decision_diagnostics_keep_aggregated_signal_timestamps(tmp_path):
    """Diagnostics should preserve aggregated signal timing for the UI."""
    from core.ai.autonomous_agent import AutonomousTradingAgent

    agent = AutonomousTradingAgent(cache_root=tmp_path / "agent_diag_ts")
    diagnostics = agent._build_decision_diagnostics(
        cfg={"symbol_mode": "manual", "symbol": "BTC/USDT", "min_confidence": 0.58},
        context_payload={
            "aggregated_signal": {
                "direction": "LONG",
                "confidence": 0.73,
                "blocked_by_risk": False,
                "risk_reason": "",
                "timestamp": "2026-04-06T00:16:00+00:00",
                "components": {
                    "llm": {
                        "direction": "LONG",
                        "confidence": 0.81,
                        "available": True,
                        "status": "active",
                        "reason": "",
                        "effective_weight": 0.4,
                    }
                },
            },
            "market_structure": {
                "last_bar_at": "2026-04-06T00:15:00+00:00",
            },
            "execution_cost": {},
        },
        raw_decision=None,
        raw_decision_source="fallback",
        decision={"action": "hold", "reason": "below_min_confidence(0.58)"},
        execution={"submitted": False, "reason": "hold"},
        selection={"selected_symbol": "BTC/USDT", "configured_symbol": "BTC/USDT"},
    )

    assert diagnostics["aggregated_signal"]["timestamp"] == "2026-04-06T00:16:00+00:00"
    assert diagnostics["aggregated_signal"]["market_data_last_bar_at"] == "2026-04-06T00:15:00+00:00"
    assert diagnostics["aggregated_signal"]["components"]["llm"]["direction"] == "LONG"


def test_agent_journal_contains_request_id(tmp_path, monkeypatch):
    """Journal rows must have request_id, execution_allowed, and rejection_reason fields."""
    from core.ai.autonomous_agent import AutonomousTradingAgent

    agent = AutonomousTradingAgent(cache_root=tmp_path / "agent_journal")
    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute"))

    # Inject a pre-built journal row to verify schema
    agent._append_journal({
        "request_id": "abc12345",
        "timestamp": "2026-01-01T00:00:00",
        "trigger": "test",
        "execution_allowed": False,
        "rejection_reason": "shadow_mode",
        "decision": {"action": "hold"},
        "execution": {"submitted": False, "reason": "shadow_mode"},
    })
    rows = agent.read_journal(limit=5)
    assert any(r.get("request_id") for r in rows)
    assert any("rejection_reason" in r for r in rows)


def test_compute_next_loop_sleep_uses_fixed_cycle_start():
    from core.ai.autonomous_agent import _compute_next_loop_sleep

    remaining, next_run_at = _compute_next_loop_sleep(
        100.0,
        120,
        now_monotonic=145.0,
        now_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    assert remaining == pytest.approx(75.0)
    assert next_run_at == "2026-01-01T00:01:15+00:00"

    remaining_overdue, next_run_at_overdue = _compute_next_loop_sleep(
        100.0,
        120,
        now_monotonic=235.0,
        now_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    assert remaining_overdue == 0.0
    assert next_run_at_overdue is None


def test_agent_run_once_disabled_reports_zero_latency(tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    asyncio.run(agent.update_runtime_config(enabled=False))
    result = asyncio.run(agent.run_once(trigger="test"))
    status = agent.get_status()

    assert result["skipped"] is True
    assert result["reason"] == "agent_disabled"
    assert status["last_latency_ms"] == 0
    assert status["last_run_at"] == result["timestamp"]
    assert status["next_run_at"] is None


def test_agent_run_once_exposes_structured_diagnostics(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    class _Agg:
        def to_dict(self):
            return {
                "direction": "LONG",
                "confidence": 0.62,
                "blocked_by_risk": False,
                "risk_reason": "",
            }

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")
    monkeypatch.setattr(module.execution_engine, "submit_signal", AsyncMock(return_value=True))
    monkeypatch.setattr(
        agent,
        "get_symbol_scan",
        AsyncMock(
            return_value={
                "generated_at": "2026-01-01T00:00:00+00:00",
                "symbol_mode": "manual",
                "configured_symbol": "BTC/USDT",
                "selected_symbol": "BTC/USDT",
                "selection_reason": "manual_symbol",
                "candidate_count": 1,
                "top_n": 10,
                "top_candidates": [
                    {
                        "rank": 1,
                        "symbol": "BTC/USDT",
                        "direction": "LONG",
                        "confidence": 0.62,
                        "score": 0.71,
                        "tradable_now": False,
                        "blocked_by_risk": False,
                        "risk_reason": "",
                        "summary": "LONG 0.620; below threshold 0.620 < 0.700",
                        "research": {"status": "paper_running", "validation_reasons": []},
                    }
                ],
            }
        ),
    )
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(
            return_value={
                "action": "buy",
                "confidence": 0.32,
                "strength": 0.7,
                "leverage": 3,
                "reason": "weak_conviction",
            }
        ),
    )

    asyncio.run(
        agent.update_runtime_config(
            enabled=True,
            mode="execute",
            min_confidence=0.7,
            cooldown_sec=0,
        )
    )
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    codes = {item.get("code") for item in (result.get("diagnostics", {}).get("items") or [])}
    assert "below_min_confidence" in codes
    assert result["status"]["last_diagnostics"]["primary"]["code"] == "below_min_confidence"


def test_agent_run_once_fast_path_hold_skips_provider(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)
    monkeypatch.setattr(
        agent,
        "get_symbol_scan",
        AsyncMock(
            return_value={
                "generated_at": "2026-01-01T00:00:00+00:00",
                "symbol_mode": "manual",
                "configured_symbol": "BTC/USDT",
                "selected_symbol": "BTC/USDT",
                "selection_reason": "manual_symbol",
                "candidate_count": 1,
                "top_n": 10,
                "top_candidates": [],
            }
        ),
    )
    monkeypatch.setattr(
        agent,
        "_build_context",
        AsyncMock(
            return_value=(
                {
                    "exchange": "binance",
                    "symbol": "BTC/USDT",
                    "timeframe": "15m",
                    "price": 100.0,
                        "bars": 240,
                        "returns": {"r_1h": 0.0, "r_24h": 0.0},
                        "realized_vol_annualized": 0.15,
                        "market_structure": {"available": True},
                        "aggregated_signal": {
                            "direction": "LONG",
                            "confidence": 0.62,
                            "blocked_by_risk": False,
                            "risk_reason": "",
                            "components": {
                                "llm": {"effective_weight": 0.4},
                                "ml": {"effective_weight": 0.3},
                                "factor": {"effective_weight": 0.3},
                            },
                        },
                        "event_summary": {"available": False},
                        "position": {},
                        "account_risk": {"trading_mode": "paper", "min_confidence": 0.7},
                        "execution_cost": {"estimated_one_way_cost_bps": 3.0, "estimated_round_trip_cost_bps": 6.0},
                        "research_context": {"available": False},
                    "profile": {},
                    "learning_memory": {},
                    "trading_mode": "paper",
                },
                pd.DataFrame(),
            )
        ),
    )
    provider_mock = AsyncMock(side_effect=AssertionError("fast-path hold should skip provider"))
    monkeypatch.setattr(agent, "_call_provider", provider_mock)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")
    monkeypatch.setattr(module.execution_engine, "submit_signal", AsyncMock(return_value=True))

    asyncio.run(
        agent.update_runtime_config(
            enabled=True,
            mode="execute",
            min_confidence=0.7,
            cooldown_sec=0,
        )
    )
    result = asyncio.run(agent.run_once(trigger="loop", force=False))

    assert result["decision"]["action"] == "hold"
    assert str(result["decision"]["reason"]).startswith("below_min_confidence(")
    assert provider_mock.await_count == 0


def test_build_context_includes_market_event_and_account_risk_payloads(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)
    now = module._utc_now()

    class _Agg:
        def to_dict(self):
            return {
                "direction": "LONG",
                "confidence": 0.68,
                "blocked_by_risk": False,
                "risk_reason": "",
                "components": {
                    "llm": {"direction": "LONG", "confidence": 0.61, "weight": 0.4},
                    "ml": {"direction": "LONG", "confidence": 0.72, "weight": 0.35},
                    "factor": {"direction": "LONG", "confidence": 0.66, "weight": 0.25},
                },
            }

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")
    monkeypatch.setattr(module.execution_engine, "get_account_equity_snapshot", AsyncMock(return_value=1500.0))
    monkeypatch.setattr(module.execution_engine, "get_strategy_position_cap_notional", lambda **kwargs: 150.0)
    monkeypatch.setattr(module.strategy_manager, "get_strategy_allocation", lambda name: 0.12)
    monkeypatch.setattr(
        module.risk_manager,
        "get_risk_report",
        lambda: {
            "risk_level": "medium",
            "trading_halted": False,
            "halt_reason": "",
            "discipline": {
                "fresh_entry_allowed": False,
                "reduce_only": True,
                "degrade_mode": "reduce_only",
                "reasons": ["daily_stop_buffer_reached(-0.007000<=-0.007000)"],
            },
            "equity": {
                "daily_pnl_ratio": -0.009,
                "daily_stop_basis_ratio": -0.007,
                "max_drawdown": 0.052,
                "rolling_3d_drawdown": 0.041,
                "rolling_7d_drawdown": 0.067,
            },
        },
    )
    monkeypatch.setattr(
        module.news_db,
        "get_recent_events",
        AsyncMock(
            return_value=[
                {
                    "event_id": "evt-1",
                    "ts": (now - module.timedelta(minutes=12)).isoformat(),
                    "symbol": "BTCUSDT",
                    "event_type": "etf",
                    "sentiment": 1,
                    "impact_score": 0.9,
                    "half_life_min": 180,
                    "evidence": {"title": "ETF inflow accelerates", "source": "coindesk"},
                },
                {
                    "event_id": "evt-2",
                    "ts": (now - module.timedelta(minutes=35)).isoformat(),
                    "symbol": "BTCUSDT",
                    "event_type": "macro",
                    "sentiment": -1,
                    "impact_score": 0.3,
                    "half_life_min": 120,
                    "evidence": {"title": "Macro headwind", "source": "reuters"},
                },
            ]
        ),
    )

    asyncio.run(agent.update_runtime_config(symbol="BTC/USDT", timeframe="15m"))
    context_payload, _ = asyncio.run(agent._build_context(agent.get_runtime_config()))

    assert context_payload["returns"]["r_4h"] > 0
    assert context_payload["market_structure"]["available"] is True
    assert context_payload["market_structure"]["trend"]["label"] == "uptrend"
    assert context_payload["market_structure"]["microstructure"]["atr_pct"] >= 0.0
    assert context_payload["event_summary"]["events_count"] == 2
    assert context_payload["event_summary"]["top_events"][0]["event_id"] == "evt-1"
    assert context_payload["event_summary"]["news_alpha_proxy"] > 0
    assert context_payload["event_summary"]["generated_at_utc"].endswith("+00:00")
    assert context_payload["event_summary"]["window_since_utc"].endswith("+00:00")
    assert context_payload["event_summary"]["latest_event_at_utc"].endswith("+00:00")
    assert context_payload["event_summary"]["ui_timezone"] == "Asia/Shanghai"
    assert "UTC storage" in context_payload["event_summary"]["timezone_basis"]
    assert context_payload["account_risk"]["account_equity"] == 1500.0
    assert context_payload["account_risk"]["position_cap_notional"] == 150.0
    assert context_payload["account_risk"]["max_total_exposure_ratio"] == 0.4
    assert context_payload["account_risk"]["total_exposure_limit_notional"] == 600.0
    assert context_payload["account_risk"]["fixed_leverage"] == 1.0
    assert context_payload["account_risk"]["risk_level"] == "medium"
    assert context_payload["account_risk"]["risk_trading_halted"] is False
    assert context_payload["account_risk"]["risk_fresh_entry_allowed"] is False
    assert context_payload["account_risk"]["risk_reduce_only"] is True
    assert context_payload["account_risk"]["risk_degrade_mode"] == "reduce_only"
    assert context_payload["account_risk"]["risk_discipline_reasons"] == [
        "daily_stop_buffer_reached(-0.007000<=-0.007000)"
    ]
    assert context_payload["account_risk"]["risk_daily_pnl_ratio"] == pytest.approx(-0.009)
    assert context_payload["account_risk"]["risk_daily_stop_basis_ratio"] == pytest.approx(-0.007)
    assert context_payload["account_risk"]["risk_max_drawdown"] == pytest.approx(0.052)
    assert context_payload["account_risk"]["risk_rolling_3d_drawdown"] == pytest.approx(0.041)
    assert context_payload["account_risk"]["risk_rolling_7d_drawdown"] == pytest.approx(0.067)
    assert context_payload["execution_cost"]["fee_bps"] > 0
    assert context_payload["execution_cost"]["estimated_slippage_bps"] >= 2.0
    assert context_payload["execution_cost"]["estimated_round_trip_cost_bps"] >= (
        context_payload["execution_cost"]["estimated_one_way_cost_bps"] * 2.0 - 1e-9
    )
    assert "llm" in context_payload["aggregated_signal"]["components"]


def test_agent_run_once_journal_includes_structured_context(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)
    now = module._utc_now()

    class _Agg:
        def to_dict(self):
            return {
                "direction": "LONG",
                "confidence": 0.71,
                "blocked_by_risk": False,
                "risk_reason": "",
                "components": {
                    "llm": {"direction": "LONG", "confidence": 0.71, "weight": 0.4},
                    "ml": {"direction": "LONG", "confidence": 0.69, "weight": 0.35},
                    "factor": {"direction": "LONG", "confidence": 0.67, "weight": 0.25},
                },
            }

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")
    monkeypatch.setattr(module.execution_engine, "get_account_equity_snapshot", AsyncMock(return_value=1200.0))
    monkeypatch.setattr(module.execution_engine, "get_strategy_position_cap_notional", lambda **kwargs: 120.0)
    monkeypatch.setattr(module.strategy_manager, "get_strategy_allocation", lambda name: 0.1)
    monkeypatch.setattr(module.execution_engine, "submit_signal", AsyncMock(return_value=True))
    monkeypatch.setattr(
        module.news_db,
        "get_recent_events",
        AsyncMock(
            return_value=[
                {
                    "event_id": "evt-journal",
                    "ts": (now - module.timedelta(minutes=8)).isoformat(),
                    "symbol": "BTCUSDT",
                    "event_type": "institution",
                    "sentiment": 1,
                    "impact_score": 0.8,
                    "half_life_min": 180,
                    "evidence": {"title": "Institutional accumulation", "source": "bloomberg"},
                }
            ]
        ),
    )
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(
            return_value={
                "action": "buy",
                "confidence": 0.84,
                "strength": 0.73,
                "leverage": 5,
                "stop_loss_pct": 0.02,
                "take_profit_pct": 0.05,
                "reason": "aligned_context",
            }
        ),
    )

    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute", cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="test", force=True))
    rows = agent.read_journal(limit=1)

    assert result["execution"]["submitted"] is True
    assert rows, "journal should contain at least one row"
    context = rows[0].get("context") or {}
    assert "market_structure" in context
    assert context["event_summary"]["events_count"] == 1
    assert context["account_risk"]["account_equity"] == 1200.0
    assert context["execution_cost"]["fee_bps"] > 0
    assert context["execution_cost"]["estimated_one_way_cost_bps"] >= context["execution_cost"]["fee_bps"]
    assert context["market_structure"]["trend"]["label"] == "uptrend"


def test_execution_cost_payload_uses_live_defaults_and_liquidity_adjustment(tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)
    payload = agent._build_execution_cost_payload(
        cfg={"exchange": "binance"},
        market_structure={
            "microstructure": {
                "atr_pct": 0.0026,
                "realized_vol": 0.0019,
                "spread_proxy": 0.0013,
            },
            "volume": {
                "last": 150000.0,
                "avg_20": 3000000.0,
            },
        },
        account_risk={
            "trading_mode": "live",
            "position_cap_notional": 800.0,
            "same_direction_remaining_notional": 400.0,
            "current_position_notional": 0.0,
            "last_price": 1.33,
        },
    )

    assert payload["trading_mode"] == "live"
    assert payload["fee_source"] == "live_default_fee_rate"
    assert payload["fee_bps"] == pytest.approx(4.0)
    assert payload["dynamic_slippage_raw_bps"] > payload["dynamic_slippage_bps"]
    assert payload["liquidity_reference_notional"] > 0.0
    assert payload["liquidity_adjustment_factor"] < 1.0
    assert payload["estimated_slippage_bps"] >= 2.0
    assert payload["estimated_one_way_cost_bps"] < (payload["dynamic_slippage_raw_bps"] + 4.0)


def test_build_signal_embeds_profit_management_metadata(tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)
    signal = agent._build_signal(
        decision={
            "action": "buy",
            "confidence": 0.84,
            "strength": 0.73,
            "stop_loss_pct": 0.02,
            "take_profit_pct": 0.05,
            "reason": "trend_following",
        },
        cfg={
            "exchange": "binance",
            "account_id": "main",
            "timeframe": "15m",
            "provider": "codex",
            "model": "gpt-test",
            "strategy_name": "AI_AutonomousAgent",
        },
        context_payload={
            "price": 100.0,
            "execution_cost": {
                "estimated_one_way_cost_bps": 6.0,
                "estimated_round_trip_cost_bps": 12.0,
            },
            "market_structure": {
                "microstructure": {
                    "atr_pct": 0.0028,
                }
            },
        },
    )

    assert signal is not None
    metadata = signal.metadata
    assert metadata["profit_protect_enabled"] is True
    assert metadata["profit_protect_trigger_pct"] >= module._AI_PROFIT_PROTECT_TRIGGER_PCT_MIN
    assert metadata["profit_protect_lock_pct"] > 0.0
    assert metadata["partial_take_profit_enabled"] is True
    assert metadata["partial_take_profit_trigger_pct"] > metadata["profit_protect_trigger_pct"]
    assert metadata["partial_take_profit_fraction"] == pytest.approx(0.5)
    assert metadata["post_partial_trailing_stop_pct"] > 0.0
    assert metadata["outage_tight_trailing_stop_pct"] > 0.0
    assert metadata["outage_tight_trailing_stop_pct"] <= metadata["post_partial_trailing_stop_pct"]


def test_agent_model_outage_tightens_profitable_local_position(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    class _Agg:
        def to_dict(self):
            return {"direction": "LONG", "confidence": 0.76}

    local_position = SimpleNamespace(
        side=SimpleNamespace(value="long"),
        quantity=1.0,
        entry_price=100.0,
        current_price=101.2,
        unrealized_pnl=1.2,
        leverage=1.0,
        strategy="AI_AutonomousAgent",
    )

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=_sample_df()))
    monkeypatch.setattr(module, "signal_aggregator", SimpleNamespace(aggregate=AsyncMock(return_value=_Agg())))
    monkeypatch.setattr(module.position_manager, "get_position", lambda *args, **kwargs: local_position)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")
    monkeypatch.setattr(module.execution_engine, "submit_signal", AsyncMock(return_value=True))
    tighten_mock = AsyncMock(return_value={"applied": True, "reason": "outage_protection_armed"})
    monkeypatch.setattr(module.execution_engine, "tighten_profitable_position_protection", tighten_mock)
    agent._last_model_feedback_at = time.time() - 3600.0
    agent._model_feedback_outage_started_at = time.time() - (module._MODEL_FEEDBACK_OUTAGE_ALERT_SEC + 5)
    agent._model_feedback_failure_streak = 1
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(side_effect=RuntimeError('codex_http_429:{"code":"USAGE_LIMIT_EXCEEDED"}')),
    )

    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute", cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    assert result["decision"]["action"] == "hold"
    assert "profit_protection_armed" in result["decision"]["reason"]
    assert tighten_mock.await_count == 1


def test_build_signal_marks_strategy_position_isolation(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_signal_isolation")

    signal = agent._build_signal(
        decision={
            "action": "buy",
            "confidence": 0.82,
            "strength": 0.7,
            "reason": "isolation_guard",
        },
        cfg={
            "exchange": "binance",
            "account_id": "main",
            "timeframe": "15m",
            "strategy_name": "AI_AutonomousAgent",
            "provider": "codex",
            "model": "gpt-5.4",
            "effective_min_confidence": 0.58,
            "min_confidence": 0.58,
            "same_direction_max_exposure_ratio": 0.5,
            "max_total_exposure_ratio": 0.4,
            "entry_size_scale": 1.0,
            "default_stop_loss_pct": 0.02,
            "default_take_profit_pct": 0.04,
        },
        context_payload={
            "symbol": "BTC/USDT",
            "price": 100.0,
            "position": {},
            "account_risk": {
                "max_total_exposure_ratio": 0.4,
                "total_exposure_limit_mode": "ratio",
            },
        },
    )

    assert signal is not None
    assert signal.metadata["source"] == "ai_autonomous_agent"
    assert signal.metadata["strategy_position_isolation"] is True


def test_agent_symbol_scan_prefers_trade_ready_symbol(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    async def _fake_build_context(cfg):
        symbol = str(cfg.get("symbol") or "BTC/USDT")
        if symbol == "ETH/USDT":
            confidence = 0.74
            direction = "LONG"
        else:
            confidence = 0.41
            direction = "SHORT"
        return (
            {
                "exchange": "binance",
                "symbol": symbol,
                "timeframe": "15m",
                "price": 100.0,
                "returns": {"r_1h": 0.0, "r_24h": 0.0},
                "realized_vol_annualized": 0.25,
                "bars": 240,
                "aggregated_signal": {
                    "direction": direction,
                    "confidence": confidence,
                    "blocked_by_risk": False,
                    "risk_reason": "",
                },
                "position": {},
                "research_context": {
                    "selected_candidate": {
                        "candidate_id": f"candidate-{symbol}",
                        "strategy": "MAStrategy",
                        "status": "paper_running",
                        "promotion_target": "paper",
                        "validation": {"reasons": []},
                    }
                },
                "profile": {},
                "trading_mode": "paper",
            },
            pd.DataFrame(),
        )

    monkeypatch.setattr(agent, "_build_context", _fake_build_context)
    asyncio.run(
        agent.update_runtime_config(
            enabled=True,
            symbol_mode="auto",
            universe_symbols=["BTC/USDT", "ETH/USDT"],
            min_confidence=0.58,
            selection_top_n=5,
        )
    )

    scan = asyncio.run(agent.get_symbol_scan(limit=5, force=True))

    assert scan["selected_symbol"] == "ETH/USDT"
    assert scan["top_candidates"][0]["symbol"] == "ETH/USDT"


def test_agent_symbol_scan_prioritizes_existing_positions(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)

    async def _fake_build_context(cfg):
        symbol = str(cfg.get("symbol") or "BTC/USDT")
        has_position = symbol == "BTC/USDT"
        if has_position:
            confidence = 0.18
            direction = "FLAT"
            position = {
                "side": "short",
                "quantity": 1.0,
                "entry_price": 100.0,
                "current_price": 98.0,
                "unrealized_pnl": 2.0,
                "source": "local",
            }
        else:
            confidence = 0.79
            direction = "LONG"
            position = {}
        return (
            {
                "exchange": "binance",
                "symbol": symbol,
                "timeframe": "15m",
                "price": 100.0,
                "returns": {"r_1h": 0.0, "r_24h": 0.0},
                "realized_vol_annualized": 0.25,
                "bars": 240,
                "aggregated_signal": {
                    "direction": direction,
                    "confidence": confidence,
                    "blocked_by_risk": False,
                    "risk_reason": "",
                },
                "position": position,
                "research_context": {
                    "selected_candidate": {
                        "candidate_id": f"candidate-{symbol}",
                        "strategy": "MAStrategy",
                        "status": "paper_running",
                        "promotion_target": "paper",
                        "validation": {"reasons": []},
                    }
                },
                "profile": {},
                "trading_mode": "paper",
            },
            pd.DataFrame(),
        )

    monkeypatch.setattr(agent, "_build_context", _fake_build_context)
    monkeypatch.setattr(
        module.position_manager,
        "get_all_positions",
        lambda: [
            SimpleNamespace(
                exchange="binance",
                account_id="main",
                quantity=1.0,
                symbol="BTC/USDT",
                strategy="AI_AutonomousAgent",
            )
        ],
    )
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")

    asyncio.run(
        agent.update_runtime_config(
            enabled=True,
            symbol="ETH/USDT",
            symbol_mode="auto",
            universe_symbols=["ETH/USDT"],
            min_confidence=0.58,
            selection_top_n=5,
        )
    )

    scan = asyncio.run(agent.get_symbol_scan(limit=5, force=True))

    assert scan["selected_symbol"] == "BTC/USDT"
    assert scan["selection_reason"] == "existing_position_priority"
    assert [row["symbol"] for row in scan["top_candidates"][:2]] == ["BTC/USDT", "ETH/USDT"]
    assert scan["top_candidates"][0]["has_position"] is True


def test_agent_symbol_scan_ignores_scan_error_rows_for_selection(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent_scan_error_filter")
    monkeypatch.setattr(
        agent,
        "get_runtime_config",
        lambda: {
            "exchange": "binance",
            "symbol": "BTC/USDT",
            "symbol_mode": "auto",
            "universe_symbols": ["ETH/USDT", "SOL/USDT"],
            "selection_top_n": 5,
            "timeframe": "15m",
            "lookback_bars": 240,
            "account_id": "main",
            "min_confidence": 0.58,
        },
    )
    monkeypatch.setattr(agent, "_cfg_with_learning_overlays", lambda cfg, force_learning_refresh=False: dict(cfg))
    monkeypatch.setattr(agent, "_scan_position_map", AsyncMock(return_value={}))
    monkeypatch.setattr(agent, "_build_context", AsyncMock(side_effect=RuntimeError("market data unavailable")))

    scan = asyncio.run(agent.get_symbol_scan(limit=5, force=True))

    assert scan["selected_symbol"] == "BTC/USDT"
    assert scan["selection_reason"] == "no_viable_candidates"
    assert scan["candidate_count"] == 0
    assert scan["scan_error_count"] == 3
    assert scan["top_candidates"] == []


def test_agent_no_price_closes_losing_position_when_learning_memory_requires(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)
    monkeypatch.setattr(
        agent,
        "_refresh_learning_memory",
        lambda cfg=None, force=False: {
            "adaptive_risk": {
                "effective_min_confidence": 0.62,
                "same_direction_max_exposure_ratio": 0.35,
                "entry_size_scale": 0.7,
                "force_close_on_data_outage_losing_position": True,
            },
            "guardrails": ["close losing positions when market data is unavailable"],
            "lessons": ["近期价格缺失时不再继续被动 hold。"],
            "blocked_symbol_sides": [],
        },
    )
    monkeypatch.setattr(
        agent,
        "get_symbol_scan",
        AsyncMock(
            return_value={
                "selected_symbol": "BTC/USDT",
                "selection_reason": "manual_symbol",
                "top_candidates": [],
            }
        ),
    )
    monkeypatch.setattr(
        agent,
        "_build_context",
        AsyncMock(
            return_value=(
                {
                    "exchange": "binance",
                    "symbol": "BTC/USDT",
                    "timeframe": "15m",
                    "price": 0.0,
                    "bars": 0,
                    "returns": {"r_1h": 0.0, "r_24h": 0.0},
                    "realized_vol_annualized": 0.0,
                    "market_structure": {"available": False},
                    "aggregated_signal": {"direction": "SHORT", "confidence": 0.61, "blocked_by_risk": False, "risk_reason": ""},
                    "event_summary": {"available": False},
                    "position": {
                        "side": "short",
                        "quantity": 1.0,
                        "entry_price": 100.0,
                        "current_price": 101.5,
                        "unrealized_pnl": -1.5,
                        "position_notional": 101.5,
                        "position_cap_notional": 400.0,
                        "same_direction_exposure_ratio": 0.25375,
                        "same_direction_exposure_limit_ratio": 0.35,
                        "same_direction_remaining_notional": 38.5,
                    },
                    "account_risk": {
                        "trading_mode": "paper",
                        "min_confidence": 0.58,
                        "position_cap_notional": 400.0,
                    },
                    "execution_cost": {"estimated_one_way_cost_bps": 5.0, "estimated_round_trip_cost_bps": 10.0},
                    "research_context": {"available": False},
                    "profile": {},
                    "learning_memory": {},
                    "trading_mode": "paper",
                },
                pd.DataFrame(),
            )
        ),
    )
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")
    submit_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(module.execution_engine, "submit_signal", submit_mock)

    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute", cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    assert result["decision"]["action"] == "close_short"
    assert result["execution"]["submitted"] is True
    assert submit_mock.await_count == 1


def test_agent_learning_guard_blocks_fresh_entry_when_service_instability_flag_active(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)
    monkeypatch.setattr(
        agent,
        "_refresh_learning_memory",
        lambda cfg=None, force=False: {
            "window": {"recent_journal_rows": 120},
            "summary": {
                "recent_model_issue_count": 36,
                "recent_latency_avg_ms": 28450.0,
            },
            "adaptive_risk": {
                "effective_min_confidence": 0.64,
                "same_direction_max_exposure_ratio": 0.35,
                "entry_size_scale": 0.7,
                "avoid_new_entries_during_service_instability": True,
            },
            "guardrails": ["avoid fresh entries while model service is unstable"],
            "lessons": ["recent model instability should block new entries"],
            "blocked_symbol_sides": [],
        },
    )
    monkeypatch.setattr(
        agent,
        "get_symbol_scan",
        AsyncMock(
            return_value={
                "selected_symbol": "ETH/USDT",
                "selection_reason": "top_ranked_tradable_symbol",
                "top_candidates": [],
            }
        ),
    )
    monkeypatch.setattr(
        agent,
        "_build_context",
        AsyncMock(
            return_value=(
                {
                    "exchange": "binance",
                    "symbol": "ETH/USDT",
                    "timeframe": "15m",
                    "price": 2000.0,
                    "bars": 240,
                    "returns": {"r_1h": 0.01, "r_24h": 0.03},
                    "realized_vol_annualized": 0.2,
                    "market_structure": {"available": True, "microstructure": {"atr_pct": 0.003}},
                    "aggregated_signal": {
                        "direction": "LONG",
                        "confidence": 0.74,
                        "blocked_by_risk": False,
                        "risk_reason": "",
                        "components": {
                            "llm": {"effective_weight": 0.4},
                            "ml": {"effective_weight": 0.3},
                            "factor": {"effective_weight": 0.3},
                        },
                    },
                    "event_summary": {"available": False},
                    "position": {},
                    "account_risk": {
                        "trading_mode": "paper",
                        "min_confidence": 0.58,
                        "position_cap_notional": 400.0,
                    },
                    "execution_cost": {"estimated_one_way_cost_bps": 4.0, "estimated_round_trip_cost_bps": 8.0},
                    "research_context": {"available": False},
                    "profile": {},
                    "learning_memory": {},
                    "trading_mode": "paper",
                },
                pd.DataFrame(),
            )
        ),
    )
    provider_mock = AsyncMock(
        side_effect=AssertionError("service instability fast-path hold should skip provider")
    )
    monkeypatch.setattr(
        agent,
        "_call_provider",
        provider_mock,
    )
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")
    submit_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(module.execution_engine, "submit_signal", submit_mock)

    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute", cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    assert result["decision"]["action"] == "hold"
    assert result["decision"]["reason"] == "review_service_instability"
    assert result["execution"]["submitted"] is False
    assert provider_mock.await_count == 0
    assert submit_mock.await_count == 0


def test_agent_learning_guard_blocks_fresh_entry_when_loss_streak_flag_active(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)
    monkeypatch.setattr(
        agent,
        "_refresh_learning_memory",
        lambda cfg=None, force=False: {
            "window": {"recent_live_trades": 8},
            "summary": {
                "recent_close_loss_count": 3,
                "recent_close_loss_streak_count": 3,
            },
            "adaptive_risk": {
                "effective_min_confidence": 0.63,
                "same_direction_max_exposure_ratio": 0.25,
                "entry_size_scale": 0.4,
                "avoid_new_entries_during_loss_streak": True,
            },
            "guardrails": ["block fresh entries during active loss streak"],
            "lessons": ["recent loss streak is 3; keep fresh entries in defensive mode"],
            "blocked_symbol_sides": [],
        },
    )
    monkeypatch.setattr(
        agent,
        "get_symbol_scan",
        AsyncMock(
            return_value={
                "selected_symbol": "ETH/USDT",
                "selection_reason": "top_ranked_tradable_symbol",
                "top_candidates": [],
            }
        ),
    )
    monkeypatch.setattr(
        agent,
        "_build_context",
        AsyncMock(
            return_value=(
                {
                    "exchange": "binance",
                    "symbol": "ETH/USDT",
                    "timeframe": "15m",
                    "price": 2010.0,
                    "bars": 240,
                    "returns": {"r_1h": 0.01, "r_24h": 0.02},
                    "realized_vol_annualized": 0.19,
                    "market_structure": {"available": True, "microstructure": {"atr_pct": 0.0032}},
                    "aggregated_signal": {
                        "direction": "LONG",
                        "confidence": 0.76,
                        "blocked_by_risk": False,
                        "risk_reason": "",
                        "components": {
                            "llm": {"effective_weight": 0.4},
                            "ml": {"effective_weight": 0.3},
                            "factor": {"effective_weight": 0.3},
                        },
                    },
                    "event_summary": {"available": False},
                    "position": {},
                    "account_risk": {
                        "trading_mode": "paper",
                        "min_confidence": 0.58,
                        "position_cap_notional": 400.0,
                        "risk_trading_halted": False,
                    },
                    "execution_cost": {"estimated_one_way_cost_bps": 4.0, "estimated_round_trip_cost_bps": 8.0},
                    "research_context": {"available": False},
                    "profile": {},
                    "learning_memory": {},
                    "trading_mode": "paper",
                },
                pd.DataFrame(),
            )
        ),
    )
    provider_mock = AsyncMock(side_effect=AssertionError("loss streak fast-path hold should skip provider"))
    monkeypatch.setattr(agent, "_call_provider", provider_mock)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")
    submit_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(module.execution_engine, "submit_signal", submit_mock)

    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute", cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    codes = {item.get("code") for item in (result.get("diagnostics", {}).get("items") or [])}
    assert result["decision"]["action"] == "hold"
    assert result["decision"]["reason"] == "review_loss_streak(3)"
    assert result["execution"]["submitted"] is False
    assert "review_loss_streak" in codes
    assert provider_mock.await_count == 0
    assert submit_mock.await_count == 0


def test_agent_learning_guard_blocks_fresh_entry_when_account_risk_halted(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)
    monkeypatch.setattr(
        agent,
        "_refresh_learning_memory",
        lambda cfg=None, force=False: {
            "summary": {},
            "adaptive_risk": {
                "effective_min_confidence": 0.58,
                "same_direction_max_exposure_ratio": 0.5,
                "entry_size_scale": 1.0,
            },
            "guardrails": [],
            "lessons": [],
            "blocked_symbol_sides": [],
        },
    )
    monkeypatch.setattr(
        agent,
        "get_symbol_scan",
        AsyncMock(
            return_value={
                "selected_symbol": "ETH/USDT",
                "selection_reason": "top_ranked_tradable_symbol",
                "top_candidates": [],
            }
        ),
    )
    monkeypatch.setattr(
        agent,
        "_build_context",
        AsyncMock(
            return_value=(
                {
                    "exchange": "binance",
                    "symbol": "ETH/USDT",
                    "timeframe": "15m",
                    "price": 1995.0,
                    "bars": 240,
                    "returns": {"r_1h": -0.01, "r_24h": -0.02},
                    "realized_vol_annualized": 0.22,
                    "market_structure": {"available": True},
                    "aggregated_signal": {
                        "direction": "SHORT",
                        "confidence": 0.75,
                        "blocked_by_risk": False,
                        "risk_reason": "",
                        "components": {
                            "llm": {"effective_weight": 0.4},
                            "ml": {"effective_weight": 0.3},
                            "factor": {"effective_weight": 0.3},
                        },
                    },
                    "event_summary": {"available": False},
                    "position": {},
                    "account_risk": {
                        "trading_mode": "paper",
                        "min_confidence": 0.58,
                        "position_cap_notional": 400.0,
                        "risk_level": "critical",
                        "risk_trading_halted": True,
                        "risk_halt_reason": "daily stop triggered",
                    },
                    "execution_cost": {"estimated_one_way_cost_bps": 4.0, "estimated_round_trip_cost_bps": 8.0},
                    "research_context": {"available": False},
                    "profile": {},
                    "learning_memory": {},
                    "trading_mode": "paper",
                },
                pd.DataFrame(),
            )
        ),
    )
    provider_mock = AsyncMock(side_effect=AssertionError("risk halt fast-path hold should skip provider"))
    monkeypatch.setattr(agent, "_call_provider", provider_mock)
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")
    submit_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(module.execution_engine, "submit_signal", submit_mock)

    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute", cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    codes = {item.get("code") for item in (result.get("diagnostics", {}).get("items") or [])}
    assert result["decision"]["action"] == "hold"
    assert result["decision"]["reason"] == "review_risk_halt"
    assert result["execution"]["submitted"] is False
    assert "review_risk_halt" in codes
    assert provider_mock.await_count == 0
    assert submit_mock.await_count == 0


def test_apply_learning_entry_guards_blocks_provider_entry_when_risk_halt_active(tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)
    guarded = agent._apply_learning_entry_guards(
        decision={
            "action": "buy",
            "confidence": 0.81,
            "strength": 0.65,
            "reason": "provider_signal",
        },
        cfg={
            "min_confidence": 0.58,
            "learning_memory": {
                "summary": {},
                "adaptive_risk": {},
                "blocked_symbol_sides": [],
            },
        },
        context_payload={
            "symbol": "BTC/USDT",
            "position": {},
            "account_risk": {
                "risk_trading_halted": True,
                "risk_halt_reason": "daily stop triggered",
            },
        },
    )

    assert guarded["action"] == "hold"
    assert guarded["reason"] == "review_risk_halt"


def test_apply_learning_entry_guards_blocks_provider_entry_when_reduce_only_contract_active(tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)
    guarded = agent._apply_learning_entry_guards(
        decision={
            "action": "buy",
            "confidence": 0.81,
            "strength": 0.65,
            "reason": "provider_signal",
        },
        cfg={
            "min_confidence": 0.58,
            "learning_memory": {
                "summary": {},
                "adaptive_risk": {},
                "blocked_symbol_sides": [],
            },
        },
        context_payload={
            "symbol": "BTC/USDT",
            "position": {},
            "account_risk": {
                "risk_trading_halted": False,
                "risk_fresh_entry_allowed": False,
                "risk_reduce_only": True,
                "risk_degrade_mode": "reduce_only",
                "risk_discipline_reasons": ["daily_stop_buffer_reached(-0.014500<=-0.014000)"],
            },
        },
    )

    assert guarded["action"] == "hold"
    assert guarded["reason"] == "review_risk_halt"


def test_apply_learning_entry_guards_allows_close_when_reduce_only_contract_active(tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)
    guarded = agent._apply_learning_entry_guards(
        decision={
            "action": "close_long",
            "confidence": 0.74,
            "strength": 0.51,
            "reason": "take_profit",
        },
        cfg={
            "min_confidence": 0.58,
            "learning_memory": {
                "summary": {},
                "adaptive_risk": {},
                "blocked_symbol_sides": [],
            },
        },
        context_payload={
            "symbol": "BTC/USDT",
            "position": {"side": "long"},
            "account_risk": {
                "risk_trading_halted": False,
                "risk_fresh_entry_allowed": False,
                "risk_reduce_only": True,
                "risk_degrade_mode": "reduce_only",
                "risk_discipline_reasons": ["daily_stop_buffer_reached(-0.014500<=-0.014000)"],
            },
        },
    )

    assert guarded["action"] == "close_long"
    assert guarded["reason"] == "take_profit"


def test_agent_learning_guard_no_longer_requires_research_for_fresh_entry(monkeypatch, tmp_path: Path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path)
    monkeypatch.setattr(
        agent,
        "_refresh_learning_memory",
        lambda cfg=None, force=False: {
            "adaptive_risk": {
                "effective_min_confidence": 0.64,
                "same_direction_max_exposure_ratio": 0.4,
                "entry_size_scale": 0.8,
                "require_research_for_new_entries": True,
            },
            "guardrails": ["require research context before fresh entries"],
            "lessons": ["近期无研究支撑的新单表现偏弱。"],
            "blocked_symbol_sides": [],
        },
    )
    monkeypatch.setattr(
        agent,
        "get_symbol_scan",
        AsyncMock(
            return_value={
                "selected_symbol": "ETH/USDT",
                "selection_reason": "top_ranked_tradable_symbol",
                "top_candidates": [],
            }
        ),
    )
    monkeypatch.setattr(
        agent,
        "_build_context",
        AsyncMock(
            return_value=(
                {
                    "exchange": "binance",
                    "symbol": "ETH/USDT",
                    "timeframe": "15m",
                    "price": 2000.0,
                    "bars": 240,
                    "returns": {"r_1h": 0.01, "r_24h": 0.03},
                    "realized_vol_annualized": 0.2,
                    "market_structure": {"available": True, "microstructure": {"atr_pct": 0.003}},
                    "aggregated_signal": {"direction": "LONG", "confidence": 0.74, "blocked_by_risk": False, "risk_reason": ""},
                    "event_summary": {"available": False},
                    "position": {},
                    "account_risk": {
                        "trading_mode": "paper",
                        "min_confidence": 0.58,
                        "position_cap_notional": 400.0,
                    },
                    "execution_cost": {"estimated_one_way_cost_bps": 4.0, "estimated_round_trip_cost_bps": 8.0},
                    "research_context": {"available": False},
                    "profile": {},
                    "learning_memory": {},
                    "trading_mode": "paper",
                },
                pd.DataFrame(),
            )
        ),
    )
    monkeypatch.setattr(
        agent,
        "_call_provider",
        AsyncMock(
            return_value={
                "action": "buy",
                "confidence": 0.88,
                "strength": 0.72,
                "leverage": 1.0,
                "stop_loss_pct": 0.02,
                "take_profit_pct": 0.04,
                "reason": "trend_following",
            }
        ),
    )
    monkeypatch.setattr(module.execution_engine, "get_trading_mode", lambda: "paper")
    submit_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(module.execution_engine, "submit_signal", submit_mock)

    asyncio.run(agent.update_runtime_config(enabled=True, mode="execute", cooldown_sec=0))
    result = asyncio.run(agent.run_once(trigger="test", force=True))

    assert result["decision"]["action"] == "buy"
    assert result["execution"]["submitted"] is True
    assert submit_mock.await_count == 1


def test_build_prompt_compacts_runtime_context(tmp_path: Path):
    from core.ai.autonomous_agent import AutonomousTradingAgent

    agent = AutonomousTradingAgent(cache_root=tmp_path)
    context_payload = {
        "exchange": "binance",
        "symbol": "ETH/USDT",
        "timeframe": "15m",
        "trading_mode": "live",
        "price": 2012.5,
        "bars": 240,
        "returns": {"r_15m": 0.001, "r_1h": 0.012, "r_4h": 0.021, "r_24h": 0.055},
        "realized_vol_annualized": 0.42,
        "market_structure": {
            "available": True,
            "last_bar_at": "2026-04-03T18:30:00",
            "trend": {
                "label": "uptrend",
                "ema_gap_pct": 0.004,
                "close_vs_ema_slow_pct": 0.009,
                "ema_fast": 2001.0,
                "ema_slow": 1993.0,
            },
            "microstructure": {
                "atr_pct": 0.0042,
                "realized_vol": 0.0021,
                "spread_proxy": 0.0014,
            },
            "volume": {"ratio_20": 1.3, "zscore_20": 1.1, "last": 999999.0},
            "range": {"position_pct": 0.84, "high": 2020.0, "low": 1900.0},
        },
        "aggregated_signal": {
            "direction": "LONG",
            "confidence": 0.71,
            "blocked_by_risk": False,
            "risk_reason": "",
            "components": {
                "llm": {
                    "direction": "LONG",
                    "confidence": 0.75,
                    "effective_weight": 0.4,
                    "available": True,
                    "status": "active",
                    "reason": "macro_tailwind",
                    "weight": 0.4,
                },
                "factor": {
                    "direction": "LONG",
                    "confidence": 0.66,
                    "effective_weight": 0.25,
                    "available": True,
                    "status": "active",
                    "reason": "",
                },
            },
        },
        "event_summary": {
            "available": True,
            "events_count": 5,
            "source_diversity": 3,
            "dominant_sentiment": "positive",
            "dominant_sentiment_ratio": 0.66,
            "net_sentiment": 0.42,
            "news_alpha_proxy": 0.38,
            "event_concentration": 0.51,
            "generated_at_utc": "2026-04-06T10:00:00+00:00",
            "window_since_utc": "2026-04-06T06:00:00+00:00",
            "latest_event_at_utc": "2026-04-06T09:52:00+00:00",
            "ui_timezone": "Asia/Shanghai",
            "timezone_basis": "UTC storage, Asia/Shanghai display",
            "top_event_types": ["macro", "etf", "institution", "listing", "hack", "extra"],
            "top_sources": ["jin10", "rss", "gdelt", "newsapi", "extra"],
            "top_events": [
                {
                    "title": "ETF inflow remains strong",
                    "sentiment": 1,
                    "impact_score": 0.82,
                    "event_type": "etf",
                    "source": "rss",
                    "body": "very long body that should not survive prompt compaction",
                }
            ],
        },
        "position": {
            "side": "long",
            "quantity": 0.8,
            "entry_price": 1988.0,
            "current_price": 2012.5,
            "unrealized_pnl": 19.6,
            "unrealized_pnl_pct": 0.012,
            "source": "exchange_live",
            "position_notional": 1610.0,
            "same_direction_exposure_ratio": 0.18,
            "same_direction_exposure_limit_ratio": 0.3,
            "position_cap_notional": 3000.0,
        },
        "account_risk": {
            "allow_live": True,
            "execution_permitted_now": True,
            "min_confidence": 0.58,
            "default_stop_loss_pct": 0.02,
            "default_take_profit_pct": 0.04,
            "account_equity": 5000.0,
            "position_cap_notional": 2500.0,
            "risk_level": "high",
            "risk_trading_halted": True,
            "risk_halt_reason": "daily stop triggered",
            "risk_daily_pnl_ratio": -0.015,
            "risk_daily_stop_basis_ratio": -0.012,
            "risk_max_drawdown": 0.09,
            "has_position": True,
            "current_position_side": "long",
            "current_position_notional": 1610.0,
            "same_direction_limit_ratio": 0.3,
            "same_direction_exposure_ratio": 0.18,
            "same_direction_remaining_notional": 890.0,
            "can_add_same_direction": True,
        },
        "execution_cost": {
            "fee_bps": 4.0,
            "estimated_slippage_bps": 12.0,
            "estimated_one_way_cost_bps": 16.0,
            "estimated_round_trip_cost_bps": 32.0,
            "notional_reference": 1000.0,
            "min_strategy_order_usd": 100.0,
            "notes": ["large verbose explanation that should not stay in compact prompt"],
        },
        "research_context": {
            "available": False,
            "candidate_count": 0,
            "selection_reason": "agent_research_decoupled",
            "references": [{"id": "abc", "title": "should not remain"}],
        },
        "profile": {
            "decision_count": 1200,
            "executed_count": 18,
            "action_counts": {"hold": 1000},
            "avg_confidence": 0.4,
        },
        "learning_memory": {
            "summary": {
                "recent_close_loss_streak_count": 3,
                "recent_model_issue_count": 17,
                "recent_latency_avg_ms": 21535.0,
                "current_open_position_count": 1,
            },
            "adaptive_risk": {
                "effective_min_confidence": 0.61,
                "same_direction_max_exposure_ratio": 0.3,
                "entry_size_scale": 0.7,
                "avoid_new_entries_during_service_instability": True,
                "avoid_new_entries_during_loss_streak": True,
                "force_close_on_data_outage_losing_position": False,
            },
            "blocked_symbol_sides": ["BTC/USDT:long", "ETH/USDT:short"],
            "guardrails": ["fresh entry min confidence >= 0.61", "avoid fresh entries while model service is unstable"],
            "lessons": ["recent model outages were frequent"],
        },
    }
    cfg = {
        "min_confidence": 0.58,
        "effective_min_confidence": 0.61,
        "default_stop_loss_pct": 0.02,
        "default_take_profit_pct": 0.04,
        "mode": "execute",
        "allow_live": True,
        "same_direction_max_exposure_ratio": 0.3,
        "entry_size_scale": 0.7,
    }

    _, user_prompt = agent._build_prompt(cfg, context_payload)
    parsed = json.loads(user_prompt)
    compact_input = parsed["input"]

    assert compact_input["scope"] == "compact_runtime_v2"
    assert compact_input["aggregated_signal"]["direction"] == "LONG"
    assert compact_input["account_risk"]["min_confidence"] == 0.58
    assert compact_input["account_risk"]["risk_level"] == "high"
    assert compact_input["account_risk"]["risk_trading_halted"] is True
    assert compact_input["account_risk"]["risk_halt_reason"] == "daily stop triggered"
    assert compact_input["account_risk"]["risk_max_drawdown"] == 0.09
    assert compact_input["learning_memory"]["adaptive_risk"]["effective_min_confidence"] == 0.61
    assert compact_input["learning_memory"]["adaptive_risk"]["avoid_new_entries_during_loss_streak"] is True
    assert compact_input["learning_memory"]["recent_close_loss_streak_count"] == 3
    assert compact_input["event_summary"]["generated_at_utc"] == "2026-04-06T10:00:00+00:00"
    assert compact_input["event_summary"]["window_since_utc"] == "2026-04-06T06:00:00+00:00"
    assert compact_input["event_summary"]["latest_event_at_utc"] == "2026-04-06T09:52:00+00:00"
    assert compact_input["event_summary"]["ui_timezone"] == "Asia/Shanghai"
    assert compact_input["event_summary"]["top_event_types"] == ["macro", "etf", "institution", "listing", "hack"]
    assert compact_input["event_summary"]["top_events"][0]["title"] == "ETF inflow remains strong"
    assert "body" not in compact_input["event_summary"]["top_events"][0]
    assert "profile" not in compact_input
    assert "references" not in compact_input["research_context"]
    assert "notes" not in compact_input["execution_cost"]
    assert len(json.dumps(compact_input, ensure_ascii=False)) < len(json.dumps(context_payload, ensure_ascii=False))
