from __future__ import annotations

import asyncio
import importlib
from types import SimpleNamespace
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from core.risk.risk_manager import RiskManager
from web.api import trading as trading_api

risk_module = importlib.import_module("core.risk.risk_manager")


def _reset_realized_cache() -> None:
    trading_api._LIVE_DAILY_REALIZED_PNL_CACHE["ts"] = 0.0
    trading_api._LIVE_DAILY_REALIZED_PNL_CACHE["day_start"] = ""
    trading_api._LIVE_DAILY_REALIZED_PNL_CACHE["payload"] = {}


def test_live_risk_report_uses_resolved_realized_pnl_not_current_unrealized_backsolve():
    report = {
        "risk_level": "low",
        "trading_halted": False,
        "limits": {"max_daily_loss_ratio": 0.02},
        "equity": {
            "current": 5039.4058,
            "day_start": 5076.3858,
            "daily_pnl_usd": 0.0,
            "daily_realized_pnl_usd": 1.0164,
            "daily_stop_basis_usd": -234.33,
        },
    }
    live_snapshot = {
        "unrealized_pnl_usd": 197.35,
        "position_count": 2,
        "by_exchange": {"binance": {"position_count": 2}},
    }

    out = trading_api._apply_live_snapshot_to_risk_report(
        report,
        live_snapshot,
        live_daily_total_pnl=-36.98,
        live_day_start_equity=5076.38581276,
        live_daily_realized_pnl=1.01643,
        live_daily_realized_source="runtime_trade_history",
    )

    equity = out["equity"]
    assert equity["daily_total_pnl_usd"] == pytest.approx(-36.98)
    assert equity["current_unrealized_pnl_usd"] == pytest.approx(197.35)
    assert equity["daily_realized_pnl_usd"] == pytest.approx(1.0164)
    assert equity["daily_realized_pnl_source"] == "runtime_trade_history"
    assert equity["daily_stop_basis_usd"] == pytest.approx(1.0164)
    assert equity["daily_unrealized_component_usd"] == pytest.approx(-37.9964)
    assert out["risk_level"] == "low"


def test_live_risk_report_fallback_preserves_existing_realized_pnl():
    report = {
        "risk_level": "low",
        "trading_halted": False,
        "limits": {"max_daily_loss_ratio": 0.02},
        "equity": {
            "current": 5039.4058,
            "day_start": 5076.3858,
            "daily_pnl_usd": -36.98,
            "daily_realized_pnl_usd": 1.0,
            "daily_stop_basis_usd": -234.33,
        },
    }
    live_snapshot = {"unrealized_pnl_usd": 197.35, "position_count": 2}

    out = trading_api._apply_live_snapshot_to_risk_report(
        report,
        live_snapshot,
        live_daily_total_pnl=-36.98,
        live_day_start_equity=5076.38581276,
    )

    equity = out["equity"]
    assert equity["daily_realized_pnl_usd"] == pytest.approx(1.0)
    assert equity["daily_stop_basis_usd"] == pytest.approx(1.0)
    assert equity["daily_realized_pnl_usd"] != pytest.approx(-234.33)


def test_live_risk_report_excludes_external_unrealized_from_stop_basis():
    report = {
        "risk_level": "low",
        "trading_halted": False,
        "limits": {"max_daily_loss_ratio": 0.02},
        "equity": {
            "current": 10000.0,
            "day_start": 10000.0,
            "daily_pnl_usd": -314.17,
            "daily_realized_pnl_usd": 0.0,
            "daily_stop_basis_usd": -314.17,
        },
    }
    live_snapshot = {
        "unrealized_pnl_usd": -314.17,
        "system_unrealized_pnl_usd": 0.0,
        "external_unrealized_pnl_usd": -314.17,
        "position_count": 1,
        "system_position_count": 0,
        "external_position_count": 1,
    }

    out = trading_api._apply_live_snapshot_to_risk_report(
        report,
        live_snapshot,
        live_daily_total_pnl=-314.17,
        live_day_start_equity=10000.0,
        live_daily_realized_pnl=0.0,
        live_daily_realized_source="binance_income",
    )

    equity = out["equity"]
    assert equity["total_open_unrealized_pnl_usd"] == pytest.approx(-314.17)
    assert equity["current_unrealized_pnl_usd"] == pytest.approx(0.0)
    assert equity["system_unrealized_pnl_usd"] == pytest.approx(0.0)
    assert equity["external_unrealized_pnl_usd"] == pytest.approx(-314.17)
    assert equity["daily_stop_basis_usd"] == pytest.approx(0.0)
    assert equity["daily_stop_basis_ratio"] == pytest.approx(0.0)
    assert out["risk_level"] == "low"
    assert out["live_positions"]["external_position_count"] == 1


def test_live_position_snapshot_prorates_system_and_external_unrealized(monkeypatch):
    trading_api._LIVE_POSITION_SNAPSHOT_CACHE["ts"] = 0.0
    trading_api._LIVE_POSITION_SNAPSHOT_CACHE["data"] = {}
    monkeypatch.setattr(trading_api.exchange_manager, "get_connected_exchanges", lambda: ["binance"])
    monkeypatch.setattr(trading_api.exchange_manager, "get_exchange", lambda name: object())
    monkeypatch.setattr(
        trading_api,
        "_fetch_binance_positions_fast",
        AsyncMock(
            return_value=[
                {
                    "symbol": "NEAR/USDT:USDT",
                    "side": "long",
                    "amount": 10.0,
                    "current_price": 2.0,
                    "entry_price": 2.1,
                    "unrealized_pnl": -100.0,
                }
            ]
        ),
    )
    monkeypatch.setattr(
        trading_api.position_manager,
        "get_all_positions",
        lambda scope=None: [
            SimpleNamespace(
                exchange="binance",
                symbol="NEAR/USDT",
                side=SimpleNamespace(value="long"),
                quantity=4.0,
                strategy="ma_live",
                metadata={"source": "strategy"},
            )
        ],
    )

    snapshot = asyncio.run(trading_api._collect_live_position_snapshot_refresh())

    assert snapshot["unrealized_pnl_usd"] == pytest.approx(-100.0)
    assert snapshot["system_unrealized_pnl_usd"] == pytest.approx(-40.0)
    assert snapshot["external_unrealized_pnl_usd"] == pytest.approx(-60.0)
    assert snapshot["system_position_count"] == 1
    assert snapshot["external_position_count"] == 1


def test_live_position_snapshot_treats_manual_strategy_position_as_external(monkeypatch):
    trading_api._LIVE_POSITION_SNAPSHOT_CACHE["ts"] = 0.0
    trading_api._LIVE_POSITION_SNAPSHOT_CACHE["data"] = {}
    monkeypatch.setattr(trading_api.exchange_manager, "get_connected_exchanges", lambda: ["binance"])
    monkeypatch.setattr(trading_api.exchange_manager, "get_exchange", lambda name: object())
    monkeypatch.setattr(
        trading_api,
        "_fetch_binance_positions_fast",
        AsyncMock(
            return_value=[
                {
                    "symbol": "SOL/USDT:USDT",
                    "side": "long",
                    "amount": 3.0,
                    "current_price": 100.0,
                    "entry_price": 110.0,
                    "unrealized_pnl": -30.0,
                }
            ]
        ),
    )
    monkeypatch.setattr(
        trading_api.position_manager,
        "get_all_positions",
        lambda scope=None: [
            SimpleNamespace(
                exchange="binance",
                symbol="SOL/USDT",
                side=SimpleNamespace(value="long"),
                quantity=3.0,
                strategy="manual_demo",
                metadata={},
            )
        ],
    )

    snapshot = asyncio.run(trading_api._collect_live_position_snapshot_refresh())

    assert snapshot["system_unrealized_pnl_usd"] == pytest.approx(0.0)
    assert snapshot["external_unrealized_pnl_usd"] == pytest.approx(-30.0)
    assert snapshot["system_position_count"] == 0
    assert snapshot["external_position_count"] == 1


def test_risk_manager_does_not_halt_on_external_equity_drop_when_system_pnl_flat(tmp_path):
    manager = RiskManager(use_persisted_overlay=False)
    manager.configure_storage(tmp_path)
    manager.set_account_scope("live", reset_baseline=False)
    manager.max_daily_loss_ratio = 0.02
    manager._daily_stop_required_breaches_live = 1
    manager._daily_stop_guard_until = None

    manager.update_equity(
        9600.0,
        day_start_equity=10000.0,
        current_unrealized_pnl=0.0,
        daily_realized_pnl=0.0,
        scope="live",
    )

    report = manager.get_risk_report(scope="live")
    assert report["trading_halted"] is False
    assert report["risk_level"] == "low"
    assert report["equity"]["daily_total_pnl_ratio"] == pytest.approx(-0.04)
    assert report["equity"]["daily_stop_basis_ratio"] == pytest.approx(0.0)
    assert report["discipline"]["fresh_entry_allowed"] is True


def test_risk_manager_catastrophic_backstop_halts_zero_trade_live_loss(tmp_path, monkeypatch):
    manager = RiskManager(use_persisted_overlay=False)
    manager.configure_storage(tmp_path)
    manager.set_account_scope("live", reset_baseline=False)
    manager.max_daily_loss_ratio = 0.02
    manager._daily_stop_required_breaches_live = 1
    manager._daily_stop_guard_until = None

    monkeypatch.setattr(
        risk_module,
        "_position_manager",
        lambda: SimpleNamespace(
            get_position_count=lambda: 0,
            get_all_positions=lambda *args, **kwargs: [],
            get_total_pnl=lambda: 0.0,
        ),
    )

    manager.update_equity(
        9500.0,
        day_start_equity=10000.0,
        current_unrealized_pnl=-500.0,
        daily_realized_pnl=0.0,
        scope="live",
    )

    report = manager.get_risk_report(scope="live")
    assert report["trading_halted"] is True
    assert report["equity"]["daily_stop_basis_ratio"] == pytest.approx(-0.05)
    assert report["discipline"]["fresh_entry_allowed"] is False


def test_resolve_live_daily_realized_pnl_prefers_binance_income(monkeypatch):
    _reset_realized_cache()
    day_start = datetime(2026, 5, 20, tzinfo=timezone.utc)
    monkeypatch.setattr(trading_api, "_current_utc_day_start", lambda: day_start)
    monkeypatch.setattr(trading_api, "_binance_has_credentials", lambda: True)
    fetch_income = AsyncMock(
        return_value=[
            {
                "symbol": "BTC/USDT",
                "timestamp": datetime(2026, 5, 20, 1, tzinfo=timezone.utc),
                "pnl": 4.5,
            },
            {
                "symbol": "ETH/USDT",
                "timestamp": datetime(2026, 5, 19, 23, tzinfo=timezone.utc),
                "pnl": 99.0,
            },
        ]
    )
    monkeypatch.setattr(trading_api, "_fetch_binance_realized_pnl_income", fetch_income)

    payload = asyncio.run(
        trading_api._resolve_live_daily_realized_pnl(force_refresh=True)
    )

    assert payload["source"] == "binance_income"
    assert payload["pnl"] == pytest.approx(4.5)
    assert payload["row_count"] == 1


def test_resolve_live_daily_realized_pnl_falls_back_to_runtime_history(monkeypatch):
    _reset_realized_cache()
    day_start = datetime(2026, 5, 20, tzinfo=timezone.utc)
    monkeypatch.setattr(trading_api, "_current_utc_day_start", lambda: day_start)
    monkeypatch.setattr(trading_api, "_binance_has_credentials", lambda: False)
    monkeypatch.setattr(
        trading_api.risk_manager,
        "get_trade_history",
        lambda limit=50000, scope="live": [
            {
                "timestamp": "2026-05-20T04:49:22+00:00",
                "symbol": "BNB/USDT",
                "pnl": 1.6864,
            },
            {
                "timestamp": "2026-05-20T04:50:15+00:00",
                "symbol": "XRP/USDT",
                "pnl": -0.66997,
            },
            {
                "timestamp": "2026-05-19T10:00:00+00:00",
                "symbol": "BTC/USDT",
                "pnl": -100.0,
            },
        ],
    )

    payload = asyncio.run(
        trading_api._resolve_live_daily_realized_pnl(force_refresh=True)
    )

    assert payload["source"] == "runtime_trade_history"
    assert payload["pnl"] == pytest.approx(1.01643)
    assert payload["row_count"] == 2


def test_resolve_live_system_daily_realized_pnl_excludes_manual_and_paper_rows(monkeypatch):
    day_start = datetime(2026, 5, 20, tzinfo=timezone.utc)
    monkeypatch.setattr(trading_api, "_current_utc_day_start", lambda: day_start)
    monkeypatch.setattr(
        trading_api.risk_manager,
        "get_trade_history",
        lambda limit=50000, scope="live": [
            {
                "timestamp": "2026-05-20T04:49:22+00:00",
                "strategy": "ma_live",
                "action": "close",
                "mode": "live",
                "pnl": -12.0,
            },
            {
                "timestamp": "2026-05-20T05:10:00+00:00",
                "strategy": "ManualDesk",
                "action": "manual_order",
                "mode": "live",
                "pnl": -300.0,
            },
            {
                "timestamp": "2026-05-20T05:30:00+00:00",
                "strategy": "manual_demo",
                "action": "close",
                "mode": "live",
                "pnl": -55.0,
            },
            {
                "timestamp": "2026-05-20T06:00:00+00:00",
                "strategy": "paper_ma",
                "mode": "paper",
                "order_id": "paper_123",
                "pnl": -99.0,
            },
        ],
    )

    payload = trading_api._resolve_live_system_daily_realized_pnl()

    assert payload["source"] == "system_runtime_trade_history"
    assert payload["pnl"] == pytest.approx(-12.0)
    assert payload["row_count"] == 1
