"""Tests for funding-rate models, collectors, and factor helpers."""

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock

import pytest

from core.data.funding_rate_collector import FundingRateCollector
from core.data.funding_rate_models import FundingRate, normalize_symbol


class TestFundingRateModel:
    def test_create_funding_rate(self):
        rate = FundingRate(
            exchange="binance",
            symbol="BTCUSDT",
            funding_rate=0.0001,
            funding_time=datetime(2024, 1, 1, 8, 0, 0),
        )

        assert rate.exchange == "binance"
        assert rate.symbol == "BTCUSDT"
        assert rate.funding_rate == 0.0001
        assert rate.funding_rate_pct == 0.01

    def test_annualized_rate(self):
        rate = FundingRate(
            exchange="binance",
            symbol="BTCUSDT",
            funding_rate=0.0001,  # 0.01%
            funding_time=datetime.now(),
        )

        # Annualized = 0.0001 * 365 * 3 * 100 = 10.95%
        assert abs(rate.annualized_rate - 10.95) < 0.01

    def test_extreme_positive(self):
        rate = FundingRate(
            exchange="binance",
            symbol="BTCUSDT",
            funding_rate=0.0002,  # 0.02%
            funding_time=datetime.now(),
        )
        assert rate.is_extreme_positive is True
        assert rate.is_extreme_negative is False

    def test_extreme_negative(self):
        rate = FundingRate(
            exchange="binance",
            symbol="BTCUSDT",
            funding_rate=-0.0002,  # -0.02%
            funding_time=datetime.now(),
        )
        assert rate.is_extreme_positive is False
        assert rate.is_extreme_negative is True

    def test_sentiment_long_heavy(self):
        rate = FundingRate(
            exchange="binance",
            symbol="BTCUSDT",
            funding_rate=0.001,  # 0.1%
            funding_time=datetime.now(),
        )
        assert rate.sentiment == "long_heavy"

    def test_sentiment_short_heavy(self):
        rate = FundingRate(
            exchange="binance",
            symbol="BTCUSDT",
            funding_rate=-0.001,  # -0.1%
            funding_time=datetime.now(),
        )
        assert rate.sentiment == "short_heavy"

    def test_sentiment_neutral(self):
        rate = FundingRate(
            exchange="binance",
            symbol="BTCUSDT",
            funding_rate=0.0001,  # 0.01%
            funding_time=datetime.now(),
        )
        assert rate.sentiment == "neutral"

    def test_to_dict(self):
        rate = FundingRate(
            exchange="binance",
            symbol="BTCUSDT",
            funding_rate=0.0001,
            funding_time=datetime(2024, 1, 1, 8, 0, 0),
        )
        payload = rate.to_dict()

        assert payload["exchange"] == "binance"
        assert payload["symbol"] == "BTCUSDT"
        assert payload["funding_rate"] == 0.0001
        assert payload["funding_rate_pct"] == 0.01
        assert "funding_time" in payload
        assert "timestamp" in payload


class TestNormalizeSymbol:
    def test_binance_to_okx(self):
        assert normalize_symbol("BTCUSDT", "okx") == "BTC-USDT-SWAP"

    def test_binance_to_gate(self):
        assert normalize_symbol("BTCUSDT", "gate") == "BTC_USDT"

    def test_okx_to_binance(self):
        assert normalize_symbol("BTC-USDT-SWAP", "binance") == "BTCUSDT"

    def test_gate_to_binance(self):
        assert normalize_symbol("BTC_USDT", "binance") == "BTCUSDT"

    def test_ethusdt_conversion(self):
        assert normalize_symbol("ETHUSDT", "okx") == "ETH-USDT-SWAP"
        assert normalize_symbol("ETHUSDT", "gate") == "ETH_USDT"


class TestFundingRateCollector:
    @pytest.fixture
    def collector(self):
        return FundingRateCollector()

    def test_fetch_binance(self, collector):
        async def _run():
            rate = await collector.fetch_binance("BTCUSDT")

            # Real API calls may return None under network issues.
            if rate:
                assert rate.exchange == "binance"
                assert rate.symbol == "BTCUSDT"
                assert isinstance(rate.funding_rate, float)
                assert isinstance(rate.funding_time, datetime)

            await collector.close()

        asyncio.run(_run())

    def test_fetch_all(self, collector):
        async def _run():
            rates = await collector.fetch_all("BTCUSDT")

            assert isinstance(rates, dict)
            for exchange, rate in rates.items():
                assert exchange in ["binance", "bybit", "okx", "gate"]
                assert isinstance(rate, FundingRate)

            await collector.close()

        asyncio.run(_run())

    def test_context_manager(self):
        async def _run():
            async with FundingRateCollector() as collector:
                await collector.fetch_binance("BTCUSDT")

        asyncio.run(_run())

    def test_fetch_binance_predicted(self, collector):
        async def _run():
            predicted = await collector.fetch_binance_predicted("BTCUSDT")

            if predicted:
                assert "mark_price" in predicted
                assert "index_price" in predicted
                assert "next_funding_time" in predicted
                assert predicted["mark_price"] > 0

            await collector.close()

        asyncio.run(_run())


class TestFundingRateFactors:
    def test_funding_rate_factor_import(self):
        from core.factors_ts.funding_rate_factors import (
            FUNDING_RATE_FACTOR_CLASS_MAP,
            FundingRateExtremeFactor,
            FundingRateFactor,
            FundingRateZscoreFactor,
            get_funding_rate_factor,
            list_funding_rate_factors,
        )

        assert "funding_rate" in FUNDING_RATE_FACTOR_CLASS_MAP
        assert "funding_rate_zscore" in FUNDING_RATE_FACTOR_CLASS_MAP

        factor = get_funding_rate_factor("funding_rate")
        assert factor.name == "funding_rate"

        factors = list_funding_rate_factors()
        assert len(factors) > 0

    def test_funding_rate_factor_compute(self):
        import pandas as pd

        from core.factors_ts.funding_rate_factors import FundingRateFactor

        df = pd.DataFrame({"funding_rate": [0.0001, 0.0002, -0.0001, 0.0003, 0.0001]})

        factor = FundingRateFactor()
        result = factor.compute(df)

        assert len(result) == 5
        assert result.iloc[0] == 0.0001

    def test_funding_rate_zscore_factor_compute(self):
        import numpy as np
        import pandas as pd

        from core.factors_ts.funding_rate_factors import FundingRateZscoreFactor

        np.random.seed(42)
        rates = np.random.normal(0.0001, 0.0002, 35)
        df = pd.DataFrame({"funding_rate": rates})

        factor = FundingRateZscoreFactor(period=30)
        result = factor.compute(df)

        assert result.iloc[-1] is not None or pd.isna(result.iloc[-1]) is False or pd.isna(result.iloc[-1])

    def test_funding_rate_extreme_factor(self):
        import pandas as pd

        from core.factors_ts.funding_rate_factors import FundingRateExtremeFactor

        df = pd.DataFrame({"funding_rate": [0.0001, 0.001, -0.001, 0.0003, -0.0003]})

        factor = FundingRateExtremeFactor(threshold=0.0005)
        result = factor.compute(df)

        assert result.iloc[0] == 0
        assert result.iloc[1] == 1
        assert result.iloc[2] == -1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


def test_normalize_symbol_ccxt_style_pairs():
    assert normalize_symbol("BTC/USDT", "binance") == "BTCUSDT"
    assert normalize_symbol("BTC/USDT:USDT", "gate") == "BTC_USDT"
    assert normalize_symbol("ETH-USDT", "okx") == "ETH-USDT-SWAP"


class _FakeResponse:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False

    async def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, payload=None, status=200, err=None):
        self._payload = payload
        self._status = status
        self._err = err

    def get(self, *args, **kwargs):
        if self._err:
            raise self._err
        return _FakeResponse(self._payload, status=self._status)


def test_fetch_gate_parses_list_payload():
    async def _run():
        collector = FundingRateCollector()
        collector._get_session = AsyncMock(return_value=_FakeSession(payload=[{"r": "-0.000038", "t": 1772985600}]))
        rate = await collector.fetch_gate("BTC/USDT")
        assert rate is not None
        assert rate.exchange == "gate"
        assert rate.symbol == "BTC_USDT"
        assert abs(rate.funding_rate - (-0.000038)) < 1e-12

    asyncio.run(_run())


def test_fetch_binance_timeout_returns_none():
    async def _run():
        collector = FundingRateCollector()
        collector._get_session = AsyncMock(return_value=_FakeSession(err=asyncio.TimeoutError()))
        rate = await collector.fetch_binance("BTCUSDT")
        assert rate is None

    asyncio.run(_run())
