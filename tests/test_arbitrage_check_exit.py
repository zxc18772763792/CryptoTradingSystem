"""check_exit coverage for the arbitrage strategies (Bug C, CODE_AUDIT_2026-06-10).

The arbitrage strategies historically emitted only BUY/SELL entry signals, so
once the spread reverted the position could exit only via global SL/TP and
capital stayed locked. These tests pin the new convergence exits:

    - spread converged below exit_spread_ratio * entry threshold → CLOSE signal
    - spread still wide → no exit
    - stale/missing price observation → no exit (never close blind)

check_exit is synchronous (called by strategy_manager._collect_exit_signals),
so the strategies read spread state cached by their own async pricing runs —
tests seed those caches directly or drive them through the real code paths.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pytest

from core.strategies.strategy_base import SignalType
from strategies.arbitrage.cex_arbitrage import (
    CEXArbitrageStrategy,
    TriangularArbitrageStrategy,
)
from strategies.arbitrage.dex_arbitrage import DEXArbitrageStrategy


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _make_df(closes: List[float], symbol: str = "BTC/USDT") -> pd.DataFrame:
    n = len(closes)
    df = pd.DataFrame(
        {
            "open": closes,
            "high": [c * 1.002 for c in closes],
            "low": [c * 0.998 for c in closes],
            "close": closes,
            "volume": [1_000.0] * n,
            "symbol": [symbol] * n,
        }
    )
    df.index = pd.date_range("2026-01-01", periods=n, freq="1h")
    return df


def _pos(
    side: str,
    *,
    symbol: str = "BTC/USDT",
    entry_price: float = 100.0,
    metadata: Optional[dict] = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        side=side,
        symbol=symbol,
        entry_price=entry_price,
        current_price=entry_price,
        metadata=metadata or {},
    )


# ───────────────────────────── CEX cross-exchange ─────────────────────────────


class TestCEXArbitrageCheckExit:
    """min_spread=0.5%, fee_drag=2*0.1%=0.2%, exit ratio 0.5 → threshold 0.25%."""

    def _strategy(self, **params) -> CEXArbitrageStrategy:
        base = {
            "min_spread": 0.005,
            "fee_rate": 0.001,
            "consider_fees": True,
            "exit_spread_ratio": 0.5,
            "exit_price_max_age_sec": 300.0,
        }
        base.update(params)
        return CEXArbitrageStrategy(params=base)

    def _seed_book(
        self,
        strategy: CEXArbitrageStrategy,
        book: Dict[str, Dict[str, float]],
        symbol: str = "BTC/USDT",
        age_sec: float = 0.0,
    ) -> None:
        strategy._price_cache[symbol] = book
        strategy._price_cache_at[symbol] = _utcnow() - timedelta(seconds=age_sec)

    @staticmethod
    def _converged_book() -> Dict[str, Dict[str, float]]:
        # All venues quote within ~2bp — no pair offers a tradable spread.
        return {
            "binance": {"bid": 100.00, "ask": 100.02, "last": 100.01},
            "okx": {"bid": 100.01, "ask": 100.03, "last": 100.02},
        }

    @staticmethod
    def _wide_book() -> Dict[str, Dict[str, float]]:
        # buy binance @100.00 → sell okx @101.00: ~1% raw, 0.8% effective.
        return {
            "binance": {"bid": 99.98, "ask": 100.00, "last": 99.99},
            "okx": {"bid": 101.00, "ask": 101.02, "last": 101.01},
        }

    def test_close_long_when_spread_converged(self):
        strategy = self._strategy()
        self._seed_book(strategy, self._converged_book())
        sig = strategy.check_exit(_make_df([100.0] * 10), _pos("long"))
        assert sig is not None
        assert sig.signal_type == SignalType.CLOSE_LONG
        assert sig.metadata["close_reason"] == "arbitrage_spread_converged"
        assert sig.metadata["close_only"] is True
        assert sig.metadata["current_effective_spread"] <= sig.metadata["exit_threshold"]
        assert sig.price == pytest.approx(100.0)

    def test_close_short_when_spread_converged(self):
        strategy = self._strategy()
        self._seed_book(strategy, self._converged_book())
        sig = strategy.check_exit(_make_df([100.0] * 10), _pos("short"))
        assert sig is not None
        assert sig.signal_type == SignalType.CLOSE_SHORT

    def test_no_close_when_spread_still_wide(self):
        strategy = self._strategy()
        self._seed_book(strategy, self._wide_book())
        assert strategy.check_exit(_make_df([100.0] * 10), _pos("long")) is None

    def test_no_close_on_stale_price_book(self):
        strategy = self._strategy()
        self._seed_book(strategy, self._converged_book(), age_sec=3600.0)
        assert strategy.check_exit(_make_df([100.0] * 10), _pos("long")) is None

    def test_no_close_without_price_book(self):
        strategy = self._strategy()
        assert strategy.check_exit(_make_df([100.0] * 10), _pos("long")) is None

    def test_no_close_with_single_exchange_book(self):
        strategy = self._strategy()
        self._seed_book(strategy, {"binance": {"bid": 100.0, "ask": 100.02, "last": 100.01}})
        assert strategy.check_exit(_make_df([100.0] * 10), _pos("long")) is None

    def test_no_close_without_position_side(self):
        strategy = self._strategy()
        self._seed_book(strategy, self._converged_book())
        assert strategy.check_exit(_make_df([100.0] * 10), _pos("")) is None

    def test_entry_pair_metadata_takes_precedence_over_best_pair(self):
        # binance/okx (the entry pair) has converged, but a third venue still
        # shows a wide spread — the position must close on its own pair.
        strategy = self._strategy()
        book = self._converged_book()
        book["gate"] = {"bid": 102.00, "ask": 102.05, "last": 102.02}
        self._seed_book(strategy, book)

        pair_meta = {"buy_exchange": "binance", "sell_exchange": "okx"}
        sig = strategy.check_exit(_make_df([100.0] * 10), _pos("long", metadata=pair_meta))
        assert sig is not None
        assert sig.metadata["spread_scope"] == "entry_pair"
        assert sig.metadata["buy_exchange"] == "binance"
        assert sig.metadata["sell_exchange"] == "okx"

        # Without pair metadata the best cross-venue spread (vs gate) is still
        # wide, so the conservative best-pair fallback keeps the position open.
        assert strategy.check_exit(_make_df([100.0] * 10), _pos("long")) is None

    def test_exit_spread_ratio_controls_threshold(self):
        # Effective spread 0.4%: above the 0.25% threshold at ratio 0.5,
        # below the 0.5% threshold at ratio 1.0.
        book = {
            "binance": {"bid": 99.90, "ask": 100.00, "last": 99.95},
            "okx": {"bid": 100.60, "ask": 100.70, "last": 100.65},
        }
        tight = self._strategy(exit_spread_ratio=0.5)
        self._seed_book(tight, book)
        assert tight.check_exit(_make_df([100.0] * 10), _pos("long")) is None

        loose = self._strategy(exit_spread_ratio=1.0)
        self._seed_book(loose, book)
        sig = loose.check_exit(_make_df([100.0] * 10), _pos("long"))
        assert sig is not None
        assert sig.signal_type == SignalType.CLOSE_LONG

    def test_empty_kline_data_still_closes_using_position_price(self):
        # The arbitrage exit depends on the live spread, not on klines.
        strategy = self._strategy()
        self._seed_book(strategy, self._converged_book())
        sig = strategy.check_exit(pd.DataFrame(), _pos("long", entry_price=100.5))
        assert sig is not None
        assert sig.price == pytest.approx(100.5)

    def test_entry_signals_carry_pair_exchanges_and_refresh_cache(self, monkeypatch):
        # Drive the real update_prices path so the entry metadata and the
        # check_exit price cache come from the same pipeline.
        quotes: Dict[str, Tuple[float, float]] = {
            "binance": (100.0, 100.1),
            "okx": (101.5, 101.6),
        }

        async def fake_realtime_price(exchange_name, symbol, connector=None, allow_rest_fallback=True):
            bid, ask = quotes[exchange_name]
            return SimpleNamespace(bid=bid, ask=ask, price=(bid + ask) / 2)

        monkeypatch.setattr(
            "strategies.arbitrage.cex_arbitrage.get_realtime_price", fake_realtime_price
        )
        connector = SimpleNamespace(is_connected=True)
        monkeypatch.setattr(
            "strategies.arbitrage.cex_arbitrage.exchange_manager",
            SimpleNamespace(get_exchange=lambda name: connector),
        )

        strategy = self._strategy(exchanges=["binance", "okx"], cooldown_min=0)
        signals = asyncio.run(strategy.generate_signals_async("BTC/USDT"))

        assert signals, "1.4% spread should produce entry signals"
        for sig in signals:
            assert sig.metadata["buy_exchange"] == "binance"
            assert sig.metadata["sell_exchange"] == "okx"
        assert strategy._price_cache_at.get("BTC/USDT") is not None
        # The freshly cached wide book must not trigger an immediate exit.
        assert strategy.check_exit(_make_df([100.0] * 10), _pos("long")) is None


# ───────────────────────────── CEX triangular ─────────────────────────────


class TestTriangularArbitrageCheckExit:
    """min_profit=0.3%, exit ratio 0.5 → raw-edge threshold 0.15%."""

    def _strategy(self, **params) -> TriangularArbitrageStrategy:
        base = {
            "min_profit": 0.003,
            "fee_rate": 0.001,
            "exit_spread_ratio": 0.5,
            "exit_price_max_age_sec": 300.0,
        }
        base.update(params)
        return TriangularArbitrageStrategy(params=base)

    def test_close_when_edge_converged(self):
        strategy = self._strategy()
        strategy._last_edge_obs["ETH/USDT"] = {"edge_abs": 0.0005, "at": _utcnow()}
        sig = strategy.check_exit(
            _make_df([3000.0] * 5, symbol="ETH/USDT"), _pos("long", symbol="ETH/USDT")
        )
        assert sig is not None
        assert sig.signal_type == SignalType.CLOSE_LONG
        assert sig.metadata["close_reason"] == "triangular_edge_converged"
        assert sig.metadata["close_only"] is True

    def test_no_close_while_edge_persists(self):
        strategy = self._strategy()
        strategy._last_edge_obs["ETH/USDT"] = {"edge_abs": 0.004, "at": _utcnow()}
        assert (
            strategy.check_exit(
                _make_df([3000.0] * 5, symbol="ETH/USDT"), _pos("short", symbol="ETH/USDT")
            )
            is None
        )

    def test_no_close_on_stale_observation(self):
        strategy = self._strategy()
        strategy._last_edge_obs["ETH/USDT"] = {
            "edge_abs": 0.0005,
            "at": _utcnow() - timedelta(seconds=3600),
        }
        assert (
            strategy.check_exit(
                _make_df([3000.0] * 5, symbol="ETH/USDT"), _pos("long", symbol="ETH/USDT")
            )
            is None
        )

    def test_generate_records_raw_edge_observation(self, monkeypatch):
        # Direct 3000 vs implied 3001.5 → raw edge 0.05% (below the 0.3% entry
        # gate, so no signals) — but the observation must still be recorded so
        # check_exit can close an open position on convergence.
        prices = {"ETH/USDT": 3000.0, "BNB/ETH": 0.2, "BNB/USDT": 600.3}

        async def fake_realtime_price(exchange_name, symbol, connector=None, allow_rest_fallback=True):
            return SimpleNamespace(price=prices[symbol], bid=None, ask=None)

        monkeypatch.setattr(
            "strategies.arbitrage.cex_arbitrage.get_realtime_price", fake_realtime_price
        )
        connector = SimpleNamespace(is_connected=True)
        monkeypatch.setattr(
            "strategies.arbitrage.cex_arbitrage.exchange_manager",
            SimpleNamespace(get_exchange=lambda name: connector),
        )

        strategy = self._strategy(bridge_assets=["BNB"], cooldown_min=0)
        signals = asyncio.run(strategy.generate_signals_async("ETH/USDT"))
        assert signals == []

        obs = strategy._last_edge_obs.get("ETH/USDT")
        assert obs is not None
        assert obs["edge_abs"] == pytest.approx(0.0005, rel=1e-6)

        sig = strategy.check_exit(
            _make_df([3000.0] * 5, symbol="ETH/USDT"), _pos("long", symbol="ETH/USDT")
        )
        assert sig is not None
        assert sig.signal_type == SignalType.CLOSE_LONG


# ───────────────────────────── DEX ─────────────────────────────


class _FakeDexConnector:
    def __init__(self, rates: Dict[Tuple[str, str], Decimal]):
        self._rates = rates

    async def get_quote(self, token_in: str, token_out: str, amount) -> Decimal:
        return Decimal(amount) * self._rates[(token_in, token_out)]


class TestDEXArbitrageCheckExit:
    """min_spread=1%, exit ratio 0.5 → round-trip threshold 0.5%."""

    def _strategy(
        self,
        uni_rates: Dict[Tuple[str, str], Decimal],
        sushi_rates: Dict[Tuple[str, str], Decimal],
        **params,
    ) -> DEXArbitrageStrategy:
        base = {
            "min_spread": 0.01,
            "exit_spread_ratio": 0.5,
            "exit_price_max_age_sec": 600.0,
        }
        base.update(params)
        strategy = DEXArbitrageStrategy(params=base)
        strategy._dex_connectors = {
            "uni": _FakeDexConnector(uni_rates),
            "sushi": _FakeDexConnector(sushi_rates),
        }
        return strategy

    def test_converged_spread_records_observation_and_closes(self):
        # Both venues quote identically; the round trip loses ~0.33% → the
        # arbitrage premise is gone and the position should close.
        rates = {
            ("ETH", "USDC"): Decimal("3000"),
            ("USDC", "ETH"): Decimal(1) / Decimal(3010),
        }
        strategy = self._strategy(dict(rates), dict(rates))

        opportunities = asyncio.run(
            strategy.find_arbitrage_opportunities("ETH", "USDC", Decimal("1"))
        )
        assert opportunities == []

        obs = strategy._last_spread_obs.get("ETH/USDC")
        assert obs is not None
        assert obs["profit_pct"] < 0

        df = _make_df([3000.0] * 5, symbol="ETH/USDC")
        sig = strategy.check_exit(df, _pos("long", symbol="ETH/USDC", entry_price=3000.0))
        assert sig is not None
        assert sig.signal_type == SignalType.CLOSE_LONG
        assert sig.metadata["close_reason"] == "dex_arbitrage_spread_converged"
        assert sig.metadata["close_only"] is True

    def test_short_leg_with_reversed_symbol_closes_on_same_observation(self):
        rates = {
            ("ETH", "USDC"): Decimal("3000"),
            ("USDC", "ETH"): Decimal(1) / Decimal(3010),
        }
        strategy = self._strategy(dict(rates), dict(rates))
        asyncio.run(strategy.find_arbitrage_opportunities("ETH", "USDC", Decimal("1")))

        # The SELL leg of the entry uses the reversed pair symbol.
        sig = strategy.check_exit(
            _make_df([3000.0] * 5, symbol="USDC/ETH"),
            _pos("short", symbol="USDC/ETH", entry_price=3000.0),
        )
        assert sig is not None
        assert sig.signal_type == SignalType.CLOSE_SHORT

    def test_directional_observations_do_not_overwrite_each_other(self):
        uni = {
            ("ETH", "USDC"): Decimal("3000"),
            ("USDC", "ETH"): Decimal(1) / Decimal("3200"),
        }
        sushi = {
            ("ETH", "USDC"): Decimal("2800"),
            ("USDC", "ETH"): Decimal(1) / Decimal("2600"),
        }
        strategy = self._strategy(uni, sushi)

        strategy._last_spread_obs["ETH/USDC"] = {"profit_pct": -0.123, "at": _utcnow()}

        asyncio.run(strategy.find_arbitrage_opportunities("USDC", "ETH", Decimal("1")))

        assert strategy._last_spread_obs["ETH/USDC"]["profit_pct"] == pytest.approx(-0.123)
        assert "USDC/ETH" in strategy._last_spread_obs

    def test_short_leg_prefers_entry_pair_metadata_over_reverse_symbol_key(self):
        strategy = self._strategy({}, {})
        strategy._last_spread_obs["ETH/USDC"] = {"profit_pct": -0.002, "at": _utcnow()}
        strategy._last_spread_obs["USDC/ETH"] = {"profit_pct": 0.08, "at": _utcnow()}

        sig = strategy.check_exit(
            _make_df([3000.0] * 5, symbol="USDC/ETH"),
            _pos(
                "short",
                symbol="USDC/ETH",
                entry_price=3000.0,
                metadata={"dex_pair_key": "ETH/USDC"},
            ),
        )
        assert sig is not None
        assert sig.signal_type == SignalType.CLOSE_SHORT
        assert sig.metadata["current_profit_pct"] < 0

    def test_no_close_while_spread_persists(self):
        # sushi sells ETH→USDC at 2900 but buys back at 2700 → the (uni buy,
        # sushi sell) round trip still nets ~7.4%, well above entry threshold.
        uni = {
            ("ETH", "USDC"): Decimal("3000"),
            ("USDC", "ETH"): Decimal(1) / Decimal(3000),
        }
        sushi = {
            ("ETH", "USDC"): Decimal("2900"),
            ("USDC", "ETH"): Decimal(1) / Decimal(2700),
        }
        strategy = self._strategy(uni, sushi)

        opportunities = asyncio.run(
            strategy.find_arbitrage_opportunities("ETH", "USDC", Decimal("1"))
        )
        assert opportunities, "wide spread should still surface an opportunity"

        obs = strategy._last_spread_obs.get("ETH/USDC")
        assert obs is not None
        assert obs["profit_pct"] > 0.05

        df = _make_df([3000.0] * 5, symbol="ETH/USDC")
        assert strategy.check_exit(df, _pos("long", symbol="ETH/USDC")) is None

    def test_no_close_on_stale_observation(self):
        strategy = self._strategy({}, {})
        strategy._last_spread_obs["ETH/USDC"] = {
            "profit_pct": -0.01,
            "at": _utcnow() - timedelta(days=1),
        }
        df = _make_df([3000.0] * 5, symbol="ETH/USDC")
        assert strategy.check_exit(df, _pos("long", symbol="ETH/USDC")) is None

    def test_no_close_without_observation(self):
        strategy = self._strategy({}, {})
        df = _make_df([3000.0] * 5, symbol="ETH/USDC")
        assert strategy.check_exit(df, _pos("long", symbol="ETH/USDC")) is None

    def test_no_close_for_unparseable_symbol(self):
        strategy = self._strategy({}, {})
        strategy._last_spread_obs["ETH/USDC"] = {"profit_pct": -0.01, "at": _utcnow()}
        assert strategy.check_exit(_make_df([1.0] * 5), _pos("long", symbol="ETHUSDC")) is None


# ───────────────────────────── shared invariants ─────────────────────────────


def _armed_strategies():
    cex = CEXArbitrageStrategy(params={"min_spread": 0.005, "fee_rate": 0.001})
    cex._price_cache["BTC/USDT"] = {
        "binance": {"bid": 100.00, "ask": 100.02, "last": 100.01},
        "okx": {"bid": 100.01, "ask": 100.03, "last": 100.02},
    }
    cex._price_cache_at["BTC/USDT"] = _utcnow()

    tri = TriangularArbitrageStrategy(params={"min_profit": 0.003})
    tri._last_edge_obs["BTC/USDT"] = {"edge_abs": 0.0001, "at": _utcnow()}

    dex = DEXArbitrageStrategy(params={"min_spread": 0.01})
    dex._last_spread_obs["BTC/USDT"] = {"profit_pct": -0.002, "at": _utcnow()}

    return [cex, tri, dex]


@pytest.mark.parametrize("strategy", _armed_strategies(), ids=lambda s: type(s).__name__)
def test_exit_signal_invariants_and_side_mapping(strategy):
    df = _make_df([100.0] * 10)
    expected = {"long": SignalType.CLOSE_LONG, "short": SignalType.CLOSE_SHORT}
    for side, signal_type in expected.items():
        sig = strategy.check_exit(df, _pos(side))
        assert sig is not None, f"{type(strategy).__name__} should close a {side} position"
        assert sig.signal_type == signal_type
        assert sig.metadata.get("close_only") is True
        assert sig.metadata.get("close_reason"), "close_reason must be non-empty"
        assert sig.strategy_name == strategy.name
    # Unknown side never closes.
    assert strategy.check_exit(df, _pos("both")) is None
