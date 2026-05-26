import asyncio
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

from core.trading.execution_engine import ExecutionEngine
from core.trading.order_manager import OrderManager, OrderRequest, OrderSide, OrderType

execution_engine_module = importlib.import_module("core.trading.execution_engine")
order_manager_module = importlib.import_module("core.trading.order_manager")


def test_order_manager_binance_fast_path_uses_account_credentials(monkeypatch):
    manager = OrderManager()
    manager.set_paper_trading(False)

    fake_exchange = SimpleNamespace(config=SimpleNamespace(default_type="future"))
    monkeypatch.setattr(manager, "_ensure_exchange_connector", AsyncMock(return_value=fake_exchange))
    monkeypatch.setattr(manager, "_sync_binance_futures_leverage", AsyncMock(return_value=True))
    monkeypatch.setattr(order_manager_module.risk_manager, "get_risk_report", lambda: {"equity": {"current": 1000.0}})
    monkeypatch.setattr(
        order_manager_module.decision_engine,
        "evaluate_order_intent",
        AsyncMock(
            return_value=SimpleNamespace(
                allowed=True,
                reason="",
                reduce_only=False,
                trace_id="trace-123",
            )
        ),
    )

    signed_request = AsyncMock(
        return_value={
            "orderId": "12345",
            "symbol": "BTCUSDT",
            "status": "FILLED",
            "avgPrice": "100.0",
            "origQty": "1.0",
            "executedQty": "1.0",
            "cumQuote": "100.0",
            "updateTime": 0,
        }
    )
    monkeypatch.setattr(order_manager_module, "binance_signed_request", signed_request)

    request = OrderRequest(
        symbol="BTC/USDT",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        amount=1.0,
        price=100.0,
        exchange="binance",
        account_id="acct_live",
        params={"market_type": "future", "leverage": 5},
    )

    order = asyncio.run(manager._create_real_order(request))

    assert order is not None
    assert order.id == "12345"
    assert signed_request.await_args.kwargs["account_id"] == "acct_live"


def test_execution_engine_refresh_equity_uses_account_scoped_snapshot(monkeypatch):
    engine = ExecutionEngine()
    engine._paper_trading = False

    fake_connector = SimpleNamespace(get_balance=AsyncMock(return_value=[]))
    monkeypatch.setattr(engine, "_ensure_exchange_connector", AsyncMock(return_value=fake_connector))
    monkeypatch.setattr(execution_engine_module.account_manager, "resolve_exchange", lambda account_id, default: "binance")

    snapshot_mock = AsyncMock(return_value={"total_usd": 321.0})
    monkeypatch.setattr(execution_engine_module, "fetch_binance_live_wallet_snapshot_fast", snapshot_mock)

    equity = asyncio.run(engine._refresh_equity(account_id="acct_live"))

    assert equity == 321.0
    assert snapshot_mock.await_args.kwargs["account_id"] == "acct_live"


def test_execution_engine_skips_incomplete_low_shared_live_equity(monkeypatch):
    engine = ExecutionEngine()
    engine._paper_trading = False

    gate_connector = SimpleNamespace(
        get_balance=AsyncMock(
            return_value=[SimpleNamespace(currency="USDT", total=1.1574)]
        )
    )
    binance_connector = SimpleNamespace(
        get_balance=AsyncMock(side_effect=RuntimeError("binance down"))
    )
    connectors = {"gate": gate_connector, "binance": binance_connector}

    monkeypatch.setattr(
        execution_engine_module.exchange_manager,
        "get_connected_exchanges",
        lambda: ["gate", "binance"],
    )
    monkeypatch.setattr(
        engine,
        "_resolve_cached_exchange",
        lambda exchange_name, account_id=None: connectors.get(exchange_name),
    )
    monkeypatch.setattr(
        execution_engine_module,
        "fetch_binance_live_wallet_snapshot_fast",
        AsyncMock(side_effect=TimeoutError()),
    )
    monkeypatch.setattr(
        execution_engine_module.risk_manager,
        "get_risk_report",
        lambda: {"equity": {"current": 0.0}},
    )
    published = []
    monkeypatch.setattr(
        execution_engine_module.risk_manager,
        "update_equity",
        lambda value, **kwargs: published.append(value),
    )

    equity = asyncio.run(engine._refresh_equity())

    assert equity == 0.0
    assert published == []


def test_execution_engine_unknown_account_inherits_active_live_mode(monkeypatch):
    engine = ExecutionEngine()
    engine.set_paper_trading(False, sync_runtime_state=False)
    monkeypatch.setattr(
        execution_engine_module.account_manager,
        "get_account",
        lambda account_id: None,
    )

    assert engine._resolve_account_trading_mode("acct_A") == "live"
