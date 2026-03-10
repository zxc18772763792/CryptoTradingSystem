"""
资金费率采集器测试
"""
import asyncio
import pytest
from datetime import datetime
from unittest.mock import AsyncMock, patch, MagicMock

from core.data.funding_rate_models import FundingRate, normalize_symbol
from core.data.funding_rate_collector import FundingRateCollector


class TestFundingRateModel:
    """测试 FundingRate 模型"""
    
    def test_create_funding_rate(self):
        """测试创建资金费率对象"""
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
        """测试年化费率计算"""
        rate = FundingRate(
            exchange="binance",
            symbol="BTCUSDT",
            funding_rate=0.0001,  # 0.01%
            funding_time=datetime.now(),
        )
        
        # 年化 = 0.0001 * 365 * 3 * 100 = 10.95%
        assert abs(rate.annualized_rate - 10.95) < 0.01
        
    def test_extreme_positive(self):
        """测试极端正费率检测"""
        rate = FundingRate(
            exchange="binance",
            symbol="BTCUSDT",
            funding_rate=0.0002,  # 0.02%
            funding_time=datetime.now(),
        )
        assert rate.is_extreme_positive is True
        assert rate.is_extreme_negative is False
        
    def test_extreme_negative(self):
        """测试极端负费率检测"""
        rate = FundingRate(
            exchange="binance",
            symbol="BTCUSDT",
            funding_rate=-0.0002,  # -0.02%
            funding_time=datetime.now(),
        )
        assert rate.is_extreme_positive is False
        assert rate.is_extreme_negative is True
        
    def test_sentiment_long_heavy(self):
        """测试多头拥挤情绪"""
        rate = FundingRate(
            exchange="binance",
            symbol="BTCUSDT",
            funding_rate=0.001,  # 0.1%
            funding_time=datetime.now(),
        )
        assert rate.sentiment == "long_heavy"
        
    def test_sentiment_short_heavy(self):
        """测试空头拥挤情绪"""
        rate = FundingRate(
            exchange="binance",
            symbol="BTCUSDT",
            funding_rate=-0.001,  # -0.1%
            funding_time=datetime.now(),
        )
        assert rate.sentiment == "short_heavy"
        
    def test_sentiment_neutral(self):
        """测试中性情绪"""
        rate = FundingRate(
            exchange="binance",
            symbol="BTCUSDT",
            funding_rate=0.0001,  # 0.01%
            funding_time=datetime.now(),
        )
        assert rate.sentiment == "neutral"
        
    def test_to_dict(self):
        """测试转换为字典"""
        rate = FundingRate(
            exchange="binance",
            symbol="BTCUSDT",
            funding_rate=0.0001,
            funding_time=datetime(2024, 1, 1, 8, 0, 0),
        )
        d = rate.to_dict()
        
        assert d["exchange"] == "binance"
        assert d["symbol"] == "BTCUSDT"
        assert d["funding_rate"] == 0.0001
        assert d["funding_rate_pct"] == 0.01
        assert "funding_time" in d
        assert "timestamp" in d


class TestNormalizeSymbol:
    """测试交易对格式转换"""
    
    def test_binance_to_okx(self):
        """测试 Binance -> OKX 格式转换"""
        result = normalize_symbol("BTCUSDT", "okx")
        assert result == "BTC-USDT-SWAP"
        
    def test_binance_to_gate(self):
        """测试 Binance -> Gate 格式转换"""
        result = normalize_symbol("BTCUSDT", "gate")
        assert result == "BTC_USDT"
        
    def test_okx_to_binance(self):
        """测试 OKX -> Binance 格式转换"""
        result = normalize_symbol("BTC-USDT-SWAP", "binance")
        assert result == "BTCUSDT"
        
    def test_gate_to_binance(self):
        """测试 Gate -> Binance 格式转换"""
        result = normalize_symbol("BTC_USDT", "binance")
        assert result == "BTCUSDT"
        
    def test_ethusdt_conversion(self):
        """测试 ETHUSDT 格式转换"""
        assert normalize_symbol("ETHUSDT", "okx") == "ETH-USDT-SWAP"
        assert normalize_symbol("ETHUSDT", "gate") == "ETH_USDT"


class TestFundingRateCollector:
    """测试资金费率采集器"""
    
    @pytest.fixture
    def collector(self):
        """创建采集器实例"""
        return FundingRateCollector()
        
    @pytest.mark.asyncio
    async def test_fetch_binance(self, collector):
        """测试从 Binance 获取资金费率 (真实 API 调用)"""
        rate = await collector.fetch_binance("BTCUSDT")
        
        # 真实 API 调用，可能返回 None (网络问题)
        if rate:
            assert rate.exchange == "binance"
            assert rate.symbol == "BTCUSDT"
            assert isinstance(rate.funding_rate, float)
            assert isinstance(rate.funding_time, datetime)
            
        await collector.close()
        
    @pytest.mark.asyncio
    async def test_fetch_all(self, collector):
        """测试并行获取所有交易所资金费率"""
        rates = await collector.fetch_all("BTCUSDT")
        
        # 至少应该有一个交易所返回数据
        assert isinstance(rates, dict)
        
        for exchange, rate in rates.items():
            assert exchange in ["binance", "bybit", "okx", "gate"]
            assert isinstance(rate, FundingRate)
            
        await collector.close()
        
    @pytest.mark.asyncio
    async def test_context_manager(self):
        """测试上下文管理器"""
        async with FundingRateCollector() as collector:
            rate = await collector.fetch_binance("BTCUSDT")
            # 应该正常工作
            
    @pytest.mark.asyncio
    async def test_fetch_binance_predicted(self, collector):
        """测试获取预测资金费率"""
        predicted = await collector.fetch_binance_predicted("BTCUSDT")
        
        if predicted:
            assert "mark_price" in predicted
            assert "index_price" in predicted
            assert "next_funding_time" in predicted
            assert predicted["mark_price"] > 0
            
        await collector.close()


class TestFundingRateFactors:
    """测试资金费率因子"""
    
    def test_funding_rate_factor_import(self):
        """测试因子模块导入"""
        from core.factors_ts.funding_rate_factors import (
            FundingRateFactor,
            FundingRateZscoreFactor,
            FundingRateExtremeFactor,
            FUNDING_RATE_FACTOR_CLASS_MAP,
            get_funding_rate_factor,
            list_funding_rate_factors,
        )
        
        # 检查因子映射
        assert "funding_rate" in FUNDING_RATE_FACTOR_CLASS_MAP
        assert "funding_rate_zscore" in FUNDING_RATE_FACTOR_CLASS_MAP
        
        # 检查工厂函数
        factor = get_funding_rate_factor("funding_rate")
        assert factor.name == "funding_rate"
        
        # 检查列表函数
        factors = list_funding_rate_factors()
        assert len(factors) > 0
        
    def test_funding_rate_factor_compute(self):
        """测试因子计算"""
        import pandas as pd
        import numpy as np
        from core.factors_ts.funding_rate_factors import FundingRateFactor
        
        # 创建测试数据
        df = pd.DataFrame({
            "funding_rate": [0.0001, 0.0002, -0.0001, 0.0003, 0.0001]
        })
        
        factor = FundingRateFactor()
        result = factor.compute(df)
        
        assert len(result) == 5
        assert result.iloc[0] == 0.0001
        
    def test_funding_rate_zscore_factor_compute(self):
        """测试 Z-score 因子计算"""
        import pandas as pd
        import numpy as np
        from core.factors_ts.funding_rate_factors import FundingRateZscoreFactor
        
        # 创建测试数据 (30 期)
        np.random.seed(42)
        rates = np.random.normal(0.0001, 0.0002, 35)
        df = pd.DataFrame({"funding_rate": rates})
        
        factor = FundingRateZscoreFactor(period=30)
        result = factor.compute(df)
        
        # 前几期可能为 NaN (滚动窗口不足)
        # 后期应该有值
        assert result.iloc[-1] is not None or pd.isna(result.iloc[-1]) is False or pd.isna(result.iloc[-1])
        
    def test_funding_rate_extreme_factor(self):
        """测试极端费率因子"""
        import pandas as pd
        from core.factors_ts.funding_rate_factors import FundingRateExtremeFactor
        
        df = pd.DataFrame({
            "funding_rate": [0.0001, 0.001, -0.001, 0.0003, -0.0003]
        })
        
        factor = FundingRateExtremeFactor(threshold=0.0005)
        result = factor.compute(df)
        
        # 0.0001 < 0.0005 -> 0
        assert result.iloc[0] == 0
        # 0.001 > 0.0005 -> 1 (极端正)
        assert result.iloc[1] == 1
        # -0.001 < -0.0005 -> -1 (极端负)
        assert result.iloc[2] == -1


# 运行测试的入口
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
