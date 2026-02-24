"""
策略测试
"""
import pytest
from datetime import datetime
import pandas as pd
import numpy as np

from core.strategies.strategy_base import (
    StrategyBase,
    Signal,
    SignalType,
    StrategyState,
)
from strategies.technical import MAStrategy, RSIStrategy, MACDStrategy
from strategies.quantitative import MeanReversionStrategy, MomentumStrategy


class TestStrategyBase:
    """策略基类测试"""

    def test_signal_creation(self):
        """测试信号创建"""
        signal = Signal(
            symbol="BTC/USDT",
            signal_type=SignalType.BUY,
            price=50000.0,
            timestamp=datetime.now(),
            strategy_name="test",
            strength=0.8,
        )
        assert signal.symbol == "BTC/USDT"
        assert signal.signal_type == SignalType.BUY
        assert signal.strength == 0.8

    def test_signal_to_dict(self):
        """测试信号转字典"""
        signal = Signal(
            symbol="BTC/USDT",
            signal_type=SignalType.SELL,
            price=50000.0,
            timestamp=datetime.now(),
            strategy_name="test",
        )
        data = signal.to_dict()
        assert data["symbol"] == "BTC/USDT"
        assert data["signal_type"] == "sell"

    def test_strategy_initialization(self):
        """测试策略初始化"""
        class TestStrategy(StrategyBase):
            def generate_signals(self, data):
                return []
            def get_required_data(self):
                return {}

        strategy = TestStrategy("test", {"param1": 1})
        assert strategy.name == "test"
        assert strategy.params["param1"] == 1
        assert strategy.state == StrategyState.IDLE

    def test_strategy_state_transitions(self):
        """测试策略状态转换"""
        class TestStrategy(StrategyBase):
            def generate_signals(self, data):
                return []
            def get_required_data(self):
                return {}

        strategy = TestStrategy("test")
        assert strategy.state == StrategyState.IDLE

        strategy.initialize()
        assert strategy.state == StrategyState.IDLE

        strategy.start()
        assert strategy.state == StrategyState.RUNNING
        assert strategy.is_running

        strategy.pause()
        assert strategy.state == StrategyState.PAUSED

        strategy.resume()
        assert strategy.state == StrategyState.RUNNING

        strategy.stop()
        assert strategy.state == StrategyState.STOPPED


class TestMAStrategy:
    """MA策略测试"""

    @pytest.fixture
    def strategy(self):
        return MAStrategy("MA_Test", {
            "fast_period": 5,
            "slow_period": 10,
        })

    @pytest.fixture
    def sample_data(self):
        """生成测试数据"""
        dates = pd.date_range(start="2024-01-01", periods=50, freq="h")
        np.random.seed(42)

        # 生成趋势价格数据
        trend = np.linspace(100, 120, 50)
        noise = np.random.randn(50) * 2
        close = trend + noise

        df = pd.DataFrame({
            "close": close,
        }, index=dates)
        df["symbol"] = "BTC/USDT"

        return df

    def test_strategy_initialization(self, strategy):
        """测试策略初始化"""
        assert strategy.name == "MA_Test"
        assert strategy.params["fast_period"] == 5
        assert strategy.params["slow_period"] == 10

    def test_generate_signals(self, strategy, sample_data):
        """测试信号生成"""
        strategy.start()
        signals = strategy.generate_signals(sample_data)

        assert isinstance(signals, list)
        for signal in signals:
            assert isinstance(signal, Signal)
            assert signal.strategy_name == "MA_Test"

    def test_get_required_data(self, strategy):
        """测试获取所需数据"""
        required = strategy.get_required_data()
        assert "type" in required
        assert "min_length" in required


class TestRSIStrategy:
    """RSI策略测试"""

    @pytest.fixture
    def strategy(self):
        return RSIStrategy("RSI_Test", {"period": 14})

    @pytest.fixture
    def oversold_data(self):
        """生成超卖数据"""
        dates = pd.date_range(start="2024-01-01", periods=50, freq="h")

        # 生成下跌趋势
        close = np.linspace(100, 70, 50)

        df = pd.DataFrame({
            "close": close,
        }, index=dates)

        return df

    def test_rsi_calculation(self, strategy, oversold_data):
        """测试RSI计算"""
        rsi = strategy._calculate_rsi(oversold_data, 14)
        assert isinstance(rsi, pd.Series)
        assert len(rsi) == len(oversold_data)

    def test_strategy_generates_signals(self, strategy, oversold_data):
        """测试策略生成信号"""
        strategy.start()
        signals = strategy.generate_signals(oversold_data)
        assert isinstance(signals, list)


class TestMACDStrategy:
    """MACD策略测试"""

    @pytest.fixture
    def strategy(self):
        return MACDStrategy("MACD_Test")

    @pytest.fixture
    def sample_data(self):
        dates = pd.date_range(start="2024-01-01", periods=100, freq="h")
        np.random.seed(42)

        close = 100 + np.cumsum(np.random.randn(100))

        df = pd.DataFrame({
            "close": close,
        }, index=dates)

        return df

    def test_macd_calculation(self, strategy, sample_data):
        """测试MACD计算"""
        macd, signal, histogram = strategy._calculate_macd(sample_data)

        assert isinstance(macd, pd.Series)
        assert isinstance(signal, pd.Series)
        assert isinstance(histogram, pd.Series)

    def test_strategy_initialization(self, strategy):
        """测试策略初始化"""
        assert strategy.name == "MACD_Test"
        assert strategy.params["fast_period"] == 12
        assert strategy.params["slow_period"] == 26


class TestMeanReversionStrategy:
    """均值回归策略测试"""

    @pytest.fixture
    def strategy(self):
        return MeanReversionStrategy("MR_Test", {
            "lookback_period": 20,
            "entry_z_score": 2.0,
        })

    @pytest.fixture
    def mean_reverting_data(self):
        """生成均值回归数据"""
        dates = pd.date_range(start="2024-01-01", periods=100, freq="h")
        np.random.seed(42)

        # 生成围绕均值波动的数据
        mean = 100
        close = mean + np.sin(np.linspace(0, 4*np.pi, 100)) * 10 + np.random.randn(100) * 2

        df = pd.DataFrame({
            "close": close,
        }, index=dates)

        return df

    def test_z_score_calculation(self, strategy, mean_reverting_data):
        """测试Z分数计算"""
        z_score = strategy._calculate_z_score(mean_reverting_data)
        assert isinstance(z_score, pd.Series)

    def test_strategy_signals(self, strategy, mean_reverting_data):
        """测试策略信号"""
        strategy.start()
        signals = strategy.generate_signals(mean_reverting_data)
        assert isinstance(signals, list)


class TestMomentumStrategy:
    """动量策略测试"""

    @pytest.fixture
    def strategy(self):
        return MomentumStrategy("Mom_Test")

    @pytest.fixture
    def trending_data(self):
        """生成趋势数据"""
        dates = pd.date_range(start="2024-01-01", periods=50, freq="h")

        # 生成上涨趋势
        close = np.linspace(100, 120, 50)

        df = pd.DataFrame({
            "close": close,
        }, index=dates)

        return df

    def test_momentum_calculation(self, strategy, trending_data):
        """测试动量计算"""
        momentum = strategy._calculate_momentum(trending_data)
        assert isinstance(momentum, pd.Series)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
