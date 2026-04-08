from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from core.strategies.strategy_base import StrategyBase


class _DummyStrategy(StrategyBase):
    def generate_signals(self, data):
        return []

    def get_required_data(self):
        return {}

    def get_info(self):
        return {
            "name": self.name,
            "state": self.state.value,
            "positions_count": len(self.positions),
            "signals_count": len(self.signals_history),
        }


def test_strategy_manager_roundtrips_runtime_metadata(monkeypatch):
    from core.strategies.strategy_manager import StrategyManager

    manager = StrategyManager()
    monkeypatch.setattr(manager, "_ensure_strategy_account", lambda name, params: None)

    ok = manager.register_strategy(
        name="unit_metadata_strategy",
        strategy_class=_DummyStrategy,
        params={"exchange": "binance"},
        symbols=["BTC/USDT"],
        timeframe="15m",
        allocation=0.15,
        metadata={"source": "ai_research", "candidate_id": "cand-unit", "proposal_id": "prop-unit"},
    )

    assert ok is True
    info = manager.get_strategy_info("unit_metadata_strategy")
    assert info is not None
    assert info["metadata"]["source"] == "ai_research"
    assert info["metadata"]["candidate_id"] == "cand-unit"
    assert info["metadata"]["proposal_id"] == "prop-unit"


def test_build_payload_preserves_strategy_metadata():
    from core.strategies.persistence import _build_payload

    payload = _build_payload(
        {
            "params": {"fast_period": 8},
            "symbols": ["BTC/USDT"],
            "timeframe": "1h",
            "exchange": "binance",
            "allocation": 0.15,
            "runtime": {"runtime_limit_minutes": 720},
            "metadata": {"source": "ai_research", "candidate_id": "cand-persist"},
        }
    )

    assert payload["metadata"]["source"] == "ai_research"
    assert payload["metadata"]["candidate_id"] == "cand-persist"


def test_restore_strategies_from_db_passes_metadata(monkeypatch):
    from core.strategies import persistence

    row = SimpleNamespace(
        name="restored_ai_strategy",
        type="MAStrategy",
        params={
            "user_params": {"exchange": "binance"},
            "symbols": ["BTC/USDT"],
            "timeframe": "15m",
            "exchange": "binance",
            "allocation": 0.15,
            "state": "idle",
            "metadata": {
                "source": "ai_research",
                "candidate_id": "cand-restore",
                "proposal_id": "prop-restore",
            },
        },
        is_active=False,
    )

    class _FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def scalars(self):
            return self

        def all(self):
            return self._rows

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def execute(self, stmt):
            return _FakeResult([row])

    register_mock = MagicMock(return_value=True)

    monkeypatch.setattr(persistence, "_get_strategy_classes", lambda: {"MAStrategy": object})
    monkeypatch.setattr(persistence, "async_session_maker", lambda: _FakeSession())
    monkeypatch.setattr(persistence.strategy_manager, "get_strategy", lambda name: None)
    monkeypatch.setattr(persistence.strategy_manager, "register_strategy", register_mock)
    monkeypatch.setattr(persistence.strategy_manager, "start_strategy", AsyncMock(return_value=True))

    result = asyncio.run(persistence.restore_strategies_from_db())

    assert result["restored"] == 1
    assert register_mock.call_args.kwargs["metadata"]["source"] == "ai_research"
    assert register_mock.call_args.kwargs["metadata"]["candidate_id"] == "cand-restore"
    assert register_mock.call_args.kwargs["metadata"]["proposal_id"] == "prop-restore"


def test_list_strategies_backfills_ai_research_ownership(monkeypatch):
    from web.api import strategies as strategies_api

    strategy_info = {
        "name": "MAStrategy_ai_1775653089_c722",
        "strategy_type": "MAStrategy",
        "state": "running",
        "symbols": ["BTC/USDT"],
        "timeframe": "15m",
        "exchange": "binance",
        "params": {"account_id": "ai_mastrategy_ai_1775653089_c722"},
        "account_id": "ai_mastrategy_ai_1775653089_c722",
        "metadata": {},
    }

    monkeypatch.setattr(strategies_api, "_get_strategy_classes", lambda: {"MAStrategy": object})
    monkeypatch.setattr(strategies_api.strategy_manager, "list_strategies", lambda: [strategy_info])
    monkeypatch.setattr(
        strategies_api,
        "resolve_runtime_research_context",
        lambda **kwargs: {
            "available": True,
            "selected_candidate": {
                "candidate_id": "cand-live",
                "proposal_id": "prop-live",
                "promotion_target": "live",
                "search_role": "champion",
            },
        },
    )

    result = asyncio.run(strategies_api.list_strategies())
    ownership = result["registered"][0]["ownership"]

    assert ownership["source"] == "ai_research"
    assert ownership["candidate_id"] == "cand-live"
    assert ownership["proposal_id"] == "prop-live"
    assert ownership["inferred"] is True


def test_get_strategy_enriches_explicit_metadata(monkeypatch):
    from web.api import strategies as strategies_api

    strategy_info = {
        "name": "research_runtime",
        "strategy_type": "MAStrategy",
        "state": "idle",
        "symbols": ["ETH/USDT"],
        "timeframe": "1h",
        "exchange": "binance",
        "params": {"account_id": "ai_research_runtime"},
        "account_id": "ai_research_runtime",
        "metadata": {
            "source": "ai_research",
            "candidate_id": "cand-explicit",
            "proposal_id": "prop-explicit",
            "runtime_mode": "paper",
        },
    }

    monkeypatch.setattr(
        strategies_api.strategy_manager,
        "get_strategy_info",
        lambda name: strategy_info if name == "research_runtime" else None,
    )

    result = asyncio.run(strategies_api.get_strategy("research_runtime"))
    ownership = result["ownership"]

    assert ownership["source"] == "ai_research"
    assert ownership["candidate_id"] == "cand-explicit"
    assert ownership["proposal_id"] == "prop-explicit"
    assert ownership["runtime_mode"] == "paper"
    assert ownership["inferred"] is False


def test_ensure_candidate_runtime_strategy_registers_ownership_metadata(monkeypatch):
    import core.strategies as strategies_pkg
    from web.api import ai_research as ai_module

    candidate = SimpleNamespace(
        candidate_id="cand-runtime",
        proposal_id="prop-runtime",
        experiment_id="exp-runtime",
        strategy="MAStrategy",
        timeframe="15m",
        symbol="BTC/USDT",
        params={"fast_period": 8, "slow_period": 26},
        metadata={"exchange": "binance", "search_role": "champion"},
        promotion=SimpleNamespace(constraints={"allocation_cap": 0.1}, decision="paper"),
    )

    register_mock = MagicMock(return_value=True)
    start_mock = AsyncMock(return_value=True)
    persist_mock = AsyncMock(return_value=True)

    monkeypatch.setattr("config.strategy_registry.get_strategy_defaults", lambda _: {"fast_period": 20})
    monkeypatch.setattr("core.deployment.promotion_engine._resolve_strategy_class", lambda _: object)
    monkeypatch.setattr("core.deployment.promotion_engine._resolve_observed_trades_per_day", lambda app, cand: 2.0)
    monkeypatch.setattr(
        "core.strategies.runtime_policy.build_runtime_limit_policy",
        lambda **kwargs: {"runtime_limit_minutes": 720, "source": "observed"},
    )
    monkeypatch.setattr(strategies_pkg.strategy_manager, "get_strategy", lambda name: None)
    monkeypatch.setattr(strategies_pkg.strategy_manager, "register_strategy", register_mock)
    monkeypatch.setattr(strategies_pkg.strategy_manager, "start_strategy", start_mock)
    monkeypatch.setattr("core.strategies.persistence.persist_strategy_snapshot", persist_mock)

    result = asyncio.run(
        ai_module._ensure_candidate_runtime_strategy(
            SimpleNamespace(state=SimpleNamespace()),
            candidate,
            target_mode="paper",
        )
    )

    metadata = register_mock.call_args.kwargs["metadata"]
    assert metadata["source"] == "ai_research"
    assert metadata["candidate_id"] == "cand-runtime"
    assert metadata["proposal_id"] == "prop-runtime"
    assert metadata["runtime_mode"] == "paper"
    assert metadata["search_role"] == "champion"
    assert result["registered_strategy_name"] == candidate.metadata["registered_strategy_name"]
