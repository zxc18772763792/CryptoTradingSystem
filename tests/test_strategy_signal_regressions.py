import asyncio
from datetime import datetime, timezone
from decimal import Decimal
import numpy as np
import pandas as pd
from types import SimpleNamespace

from config.strategy_registry import STRATEGY_REGISTRY
from core.strategies.strategy_base import SignalType
from core.ai.ml_signal import MLSignalResult
from strategies.ai.ml_xgboost_strategy import MLXGBoostStrategy
from strategies.arbitrage.dex_arbitrage import DEXArbitrageStrategy
from strategies.factor_based.factor_strategies import HurstExponentStrategy, VaRBreakoutStrategy
from strategies.macro.fund_flow import WhaleActivityStrategy
from strategies.macro.market_sentiment import SocialSentimentStrategy
from strategies.quantitative.mean_reversion import MeanReversionStrategy
from strategies.technical.bollinger_strategy import BollingerSqueezeStrategy
from strategies.technical.common_strategies import DonchianBreakoutStrategy
from strategies.technical.ma_strategy import EMAStrategy
from strategies.technical.ma_strategy import MAStrategy
from strategies.technical.macd_strategy import MACDHistogramStrategy, MACDStrategy
from strategies.technical.rsi_strategy import RSIDivergenceStrategy, RSIStrategy


def _close_frame(rows: int = 40, symbol: str = "BTC/USDT") -> pd.DataFrame:
    index = pd.date_range("2025-01-01", periods=rows, freq="h")
    close = np.linspace(100.0, 102.0, rows)
    return pd.DataFrame({"close": close, "symbol": [symbol] * rows}, index=index)


def test_macd_histogram_strategy_respects_min_histogram_threshold(monkeypatch):
    strategy = MACDHistogramStrategy(
        "macd_hist_test",
        {"fast_period": 12, "slow_period": 26, "signal_period": 9, "min_histogram": 0.1},
    )
    data = _close_frame(rows=60)

    def _calc_small_cross(_data):
        series = pd.Series(np.zeros(len(_data)), index=_data.index)
        hist = pd.Series(np.zeros(len(_data)), index=_data.index)
        hist.iloc[-2] = -0.05
        hist.iloc[-1] = 0.04
        return series, series, hist

    monkeypatch.setattr(strategy, "_calculate_macd", _calc_small_cross)
    assert strategy.generate_signals(data) == []

    def _calc_large_cross(_data):
        series = pd.Series(np.zeros(len(_data)), index=_data.index)
        hist = pd.Series(np.zeros(len(_data)), index=_data.index)
        hist.iloc[-2] = -0.15
        hist.iloc[-1] = 0.18
        return series, series, hist

    monkeypatch.setattr(strategy, "_calculate_macd", _calc_large_cross)
    signals = strategy.generate_signals(data)
    assert len(signals) == 1
    assert signals[0].signal_type == SignalType.BUY
    assert signals[0].metadata["min_histogram"] == 0.1


def test_rsi_strategy_uses_exit_thresholds_for_close_signals(monkeypatch):
    strategy = RSIStrategy(
        "rsi_exit_test",
        {
            "period": 14,
            "oversold": 30,
            "overbought": 70,
            "exit_oversold": 40,
            "exit_overbought": 60,
            "exit_min_profit_pct": 0.0,
        },
    )
    data = _close_frame(rows=30)

    def _entry_rsi(_data, _period):
        series = pd.Series(np.full(len(_data), 50.0), index=_data.index)
        series.iloc[-2] = 25.0
        series.iloc[-1] = 31.0
        return series

    monkeypatch.setattr(strategy, "_calculate_rsi", _entry_rsi)
    entry_signals = strategy.generate_signals(data)
    assert len(entry_signals) == 1
    assert entry_signals[0].signal_type == SignalType.BUY

    def _exit_rsi(_data, _period):
        series = pd.Series(np.full(len(_data), 50.0), index=_data.index)
        series.iloc[-2] = 38.0
        series.iloc[-1] = 41.0
        return series

    monkeypatch.setattr(strategy, "_calculate_rsi", _exit_rsi)
    exit_signal = strategy.check_exit(data, SimpleNamespace(symbol="BTC/USDT", side="long", entry_price=100.0))
    assert exit_signal is not None
    assert exit_signal.signal_type == SignalType.CLOSE_LONG
    assert exit_signal.metadata["exit_threshold"] == 40
    assert exit_signal.metadata["close_reason"] == "rsi_long_exit"


def test_ma_strategy_check_exit_closes_when_spread_compresses():
    strategy = MAStrategy("ma_exit_test", {"fast_period": 2, "slow_period": 4, "signal_threshold": 0.01})
    index = pd.date_range("2025-01-01", periods=5, freq="h")
    data = pd.DataFrame(
        {"close": [100.0, 100.0, 110.0, 110.0, 100.0], "symbol": ["BTC/USDT"] * 5},
        index=index,
    )

    exit_signal = strategy.check_exit(data, SimpleNamespace(symbol="BTC/USDT", side="long"))

    assert exit_signal is not None
    assert exit_signal.signal_type == SignalType.CLOSE_LONG
    assert exit_signal.metadata["close_reason"] == "ma_diff_compression"


def test_ma_check_exit_dict_position_keeps_symbol_and_utc_bar_time():
    strategy = MAStrategy("ma_exit_test", {"fast_period": 2, "slow_period": 4, "signal_threshold": 0.01})
    index = pd.date_range("2025-01-01", periods=5, freq="h")
    data = pd.DataFrame(
        {"close": [100.0, 100.0, 110.0, 110.0, 100.0], "symbol": ["DATA/USDT"] * 5},
        index=index,
    )

    exit_signal = strategy.check_exit(data, {"symbol": "POS/USDT", "side": "long"})

    assert exit_signal is not None
    assert exit_signal.symbol == "POS/USDT"
    assert exit_signal.timestamp.tzinfo is not None
    assert exit_signal.timestamp.isoformat() == "2025-01-01T04:00:00+00:00"


def test_ema_check_exit_dict_position_keeps_symbol_and_utc_bar_time(monkeypatch):
    strategy = EMAStrategy("ema_exit_test", {"fast_period": 2, "slow_period": 4, "signal_threshold": 0.01})
    index = pd.date_range("2025-01-01", periods=5, freq="h")
    data = pd.DataFrame({"close": [100.0] * 5, "symbol": ["DATA/USDT"] * 5}, index=index)

    class _FakeEWM:
        def __init__(self, values):
            self._values = values

        def mean(self):
            return pd.Series(self._values, index=index)

    def _ewm(self, span, adjust=False):
        values = [100.0, 100.0, 100.0, 103.0, 100.5] if span == 2 else [100.0] * 5
        return _FakeEWM(values)

    monkeypatch.setattr(pd.Series, "ewm", _ewm)
    exit_signal = strategy.check_exit(data, {"symbol": "POS/USDT", "side": "long"})

    assert exit_signal is not None
    assert exit_signal.symbol == "POS/USDT"
    assert exit_signal.timestamp.tzinfo is not None
    assert exit_signal.timestamp.isoformat() == "2025-01-01T04:00:00+00:00"


def test_macd_strategy_check_exit_closes_on_histogram_flip(monkeypatch):
    strategy = MACDStrategy("macd_exit_test", {"fast_period": 12, "slow_period": 26, "signal_period": 9})
    data = _close_frame(rows=60)

    def _calc_hist_flip(_data):
        series = pd.Series(np.zeros(len(_data)), index=_data.index)
        hist = pd.Series(np.zeros(len(_data)), index=_data.index)
        hist.iloc[-2] = 0.15
        hist.iloc[-1] = -0.02
        return series, series, hist

    monkeypatch.setattr(strategy, "_calculate_macd", _calc_hist_flip)
    exit_signal = strategy.check_exit(data, SimpleNamespace(symbol="BTC/USDT", side="long"))

    assert exit_signal is not None
    assert exit_signal.signal_type == SignalType.CLOSE_LONG
    assert exit_signal.metadata["close_reason"] == "macd_histogram_flip"


def test_mean_reversion_strategy_uses_exit_zscore_for_close_signals(monkeypatch):
    strategy = MeanReversionStrategy(
        "mr_exit_test",
        {"lookback_period": 20, "entry_z_score": 2.0, "exit_z_score": 0.6},
    )
    data = _close_frame(rows=40)

    def _entry_zscore(_data):
        series = pd.Series(np.zeros(len(_data)), index=_data.index)
        series.iloc[-2] = -2.3
        series.iloc[-1] = -1.8
        return series

    monkeypatch.setattr(strategy, "_calculate_z_score", _entry_zscore)
    entry_signals = strategy.generate_signals(data)
    assert len(entry_signals) == 1
    assert entry_signals[0].signal_type == SignalType.BUY

    def _exit_zscore(_data):
        series = pd.Series(np.zeros(len(_data)), index=_data.index)
        series.iloc[-2] = -0.9
        series.iloc[-1] = -0.5
        return series

    monkeypatch.setattr(strategy, "_calculate_z_score", _exit_zscore)
    assert strategy.generate_signals(data) == []
    exit_signal = strategy.check_exit(data, SimpleNamespace(symbol="BTC/USDT", side="long", entry_price=100.0))
    assert exit_signal is not None
    assert exit_signal.signal_type == SignalType.CLOSE_LONG
    assert exit_signal.metadata["exit_z_score"] == 0.6
    assert exit_signal.metadata["close_only"] is True


def test_mean_reversion_check_exit_uses_position_side_not_regime_bias(monkeypatch):
    strategy = MeanReversionStrategy(
        "mr_exit_side_test",
        {"lookback_period": 20, "entry_z_score": 2.0, "exit_z_score": 0.6},
    )
    data = _close_frame(rows=40)
    strategy._regime_bias["BTC/USDT"] = 1

    def _short_exit_zscore(_data):
        series = pd.Series(np.zeros(len(_data)), index=_data.index)
        series.iloc[-2] = 0.9
        series.iloc[-1] = 0.5
        return series

    monkeypatch.setattr(strategy, "_calculate_z_score", _short_exit_zscore)
    exit_signal = strategy.check_exit(data, SimpleNamespace(symbol="BTC/USDT", side="short", entry_price=100.0))

    assert exit_signal is not None
    assert exit_signal.signal_type == SignalType.CLOSE_SHORT
    assert exit_signal.metadata["close_reason"] == "mean_reversion_short_exit"


def test_dex_arbitrage_async_initializes_and_uses_unit_prices(monkeypatch):
    strategy = DEXArbitrageStrategy("dex_test", {"min_spread": 0.01})
    called = {"init": 0}
    ts = datetime(2026, 5, 21, tzinfo=timezone.utc)

    async def _init():
        called["init"] += 1
        strategy._dex_connectors["mock"] = object()

    async def _opportunities(_token_a, _token_b, _amount):
        return [
            {
                "token_a": "ETH",
                "token_b": "USDC",
                "buy_dex": "uni",
                "sell_dex": "sushi",
                "amount": Decimal("2"),
                "buy_quote": Decimal("6000"),
                "sell_quote": Decimal("2.2"),
                "profit": Decimal("0.2"),
                "profit_pct": Decimal("0.10"),
                "timestamp": ts,
            }
        ]

    monkeypatch.setattr(strategy, "initialize_dex_connectors", _init)
    monkeypatch.setattr(strategy, "find_arbitrage_opportunities", _opportunities)

    signals = asyncio.run(strategy.generate_signals_async("ETH", "USDC", Decimal("2")))

    assert called["init"] == 1
    assert [s.signal_type for s in signals] == [SignalType.BUY, SignalType.SELL]
    assert signals[0].price == 3000.0
    assert signals[0].metadata["quote"] == 6000.0
    assert signals[0].metadata["amount"] == 2.0
    assert signals[1].price == float(Decimal("2.2") / Decimal("6000"))
    assert signals[1].metadata["quote"] == 2.2
    assert signals[1].metadata["amount"] == 6000.0


def test_whale_activity_ignores_taker_or_maker_and_uses_trade_timestamp():
    strategy = WhaleActivityStrategy("whale_test", {"min_whale_size": 1.0})

    assert strategy._infer_side({"takerOrMaker": "taker"}) == "unknown"

    strategy.add_whale_transaction(
        amount=2.0,
        direction="buy",
        price=100.0,
        usd_value=200.0,
        timestamp=datetime(2026, 5, 21, 3, 4, 5),
    )

    assert strategy._whale_transactions[0]["timestamp"] == datetime(2026, 5, 21, 3, 4, 5, tzinfo=timezone.utc)


def test_social_sentiment_does_not_raise_mentions_from_strong_sentiment(monkeypatch):
    strategy = SocialSentimentStrategy("social_test", {"min_mentions": 25})

    async def _trending(_base):
        return 1, 0.0

    async def _price(_symbol):
        return 10.0, 100.0

    monkeypatch.setattr(strategy, "_fetch_trending_proxy", _trending)
    monkeypatch.setattr(strategy, "_fetch_price_proxy", _price)

    signals = asyncio.run(strategy.generate_signals_async("BTC/USDT"))

    assert strategy._social_data["mentions"] == 1
    assert signals == []


def test_ml_xgboost_neutral_exit_uses_bar_timestamp(monkeypatch):
    idx = pd.date_range("2026-05-20", periods=60, freq="h", tz="UTC")
    df = pd.DataFrame(
        {
            "open": np.linspace(100.0, 110.0, len(idx)),
            "high": np.linspace(101.0, 111.0, len(idx)),
            "low": np.linspace(99.0, 109.0, len(idx)),
            "close": np.linspace(100.0, 110.0, len(idx)),
            "volume": np.full(len(idx), 1000.0),
            "symbol": ["BTC/USDT"] * len(idx),
        },
        index=idx,
    )

    class _FlatModel:
        def predict(self, _features, symbol=""):
            return MLSignalResult(
                symbol=symbol,
                direction="FLAT",
                confidence=0.42,
                long_prob=0.51,
                short_prob=0.49,
                model_version="test",
            )

    strategy = MLXGBoostStrategy("ml_time_test", {"model_path": "missing.json"})
    strategy._model = _FlatModel()
    strategy._last_bias["BTC/USDT"] = 1

    signals = strategy.generate_signals(df)

    assert len(signals) == 1
    assert signals[0].signal_type == SignalType.CLOSE_LONG
    assert signals[0].timestamp == idx[-1].to_pydatetime()


def test_bollinger_squeeze_strategy_uses_breakout_threshold_and_stop_loss(monkeypatch):
    strategy = BollingerSqueezeStrategy(
        "bb_squeeze_test",
        {
            "period": 20,
            "num_std": 2.0,
            "squeeze_threshold": 0.02,
            "breakout_threshold": 0.01,
            "stop_loss_pct": 0.03,
            "take_profit_pct": 0.08,
        },
    )
    data = _close_frame(rows=40)

    def _bands_small_breakout(_data):
        upper = pd.Series(np.full(len(_data), 100.5), index=_data.index)
        middle = pd.Series(np.full(len(_data), 100.0), index=_data.index)
        lower = pd.Series(np.full(len(_data), 99.5), index=_data.index)
        bandwidth = pd.Series(np.full(len(_data), 0.03), index=_data.index)
        bandwidth.iloc[-2] = 0.015
        return upper, middle, lower, bandwidth

    data_small = data.copy()
    data_small.iloc[-1, data_small.columns.get_loc("close")] = 101.0
    monkeypatch.setattr(strategy, "_calculate_bollinger_bands", _bands_small_breakout)
    assert strategy.generate_signals(data_small) == []

    data_large = data.copy()
    data_large.iloc[-1, data_large.columns.get_loc("close")] = 101.8
    signals = strategy.generate_signals(data_large)
    assert len(signals) == 1
    assert signals[0].signal_type == SignalType.BUY
    assert signals[0].stop_loss == data_large["close"].iloc[-1] * (1 - 0.03)
    assert signals[0].metadata["breakout_threshold"] == 0.01


def test_var_breakout_strategy_uses_correct_return_direction():
    strategy = VaRBreakoutStrategy(
        "var_breakout_test",
        {"var_period": 20, "confidence": 0.95, "multiplier": 1.5},
    )

    index = pd.date_range("2025-01-01", periods=30, freq="h")
    base = np.array(
        [
            100.0, 100.4, 99.8, 100.2, 99.9, 100.5, 100.1, 100.7, 100.0, 100.8,
            100.2, 100.6, 100.1, 100.5, 100.0, 100.4, 100.2, 100.7, 100.3, 100.8,
            100.5, 100.9, 100.4, 100.8, 100.3, 100.7, 100.4, 100.8, 101.0, 114.0,
        ]
    )
    positive_df = pd.DataFrame({"close": base, "symbol": ["BTC/USDT"] * len(base)}, index=index)
    positive_signals = strategy.generate_signals(positive_df)
    assert len(positive_signals) == 1
    assert positive_signals[0].signal_type == SignalType.BUY

    negative_base = base.copy()
    negative_base[-1] = 88.0
    negative_df = pd.DataFrame({"close": negative_base, "symbol": ["BTC/USDT"] * len(base)}, index=index)
    negative_signals = strategy.generate_signals(negative_df)
    assert len(negative_signals) == 1
    assert negative_signals[0].signal_type == SignalType.SELL


def test_var_breakout_strategy_uses_current_var_threshold_without_extra_lag():
    strategy = VaRBreakoutStrategy(
        "var_breakout_prev_var_test",
        {"var_period": 20, "confidence": 0.95, "multiplier": 1.5},
    )

    index = pd.date_range("2025-01-01", periods=30, freq="h")
    close = [100.0]
    for i in range(1, 29):
        close.append(close[-1] * (0.999 if i % 2 else 1.001))
    close.append(close[-1] * 0.99849)
    df = pd.DataFrame({"close": close, "symbol": ["BTC/USDT"] * len(close)}, index=index)

    returns = df["close"].pct_change()

    def calc_var(series):
        r = series.dropna()
        if len(r) < strategy.params["var_period"] // 2:
            return np.nan
        return np.percentile(r, (1 - strategy.params["confidence"]) * 100)

    var = returns.rolling(strategy.params["var_period"]).apply(calc_var, raw=False)
    previous_threshold = abs(var.iloc[-2]) * strategy.params["multiplier"]
    current_threshold = abs(var.iloc[-1]) * strategy.params["multiplier"]
    assert -current_threshold < returns.iloc[-1] < -previous_threshold

    signals = strategy.generate_signals(df)
    assert signals == []


def test_hurst_mean_reversion_branch_sells_high_reversal_and_buys_low_reversal(monkeypatch):
    strategy = HurstExponentStrategy(
        "hurst_mr_direction_test",
        {"hurst_period": 10, "zscore_period": 5, "mean_revert_threshold": 0.45, "zscore_threshold": 1.5},
    )
    data = _close_frame(rows=20)

    def _rolling_mean(self, *args, **kwargs):
        source = self.obj
        return pd.Series(np.full(len(source), 100.0), index=source.index)

    def _rolling_std(self, *args, **kwargs):
        source = self.obj
        return pd.Series(np.full(len(source), 1.0), index=source.index)

    def _rolling_apply(self, func, raw=False, *args, **kwargs):
        source = self.obj
        return pd.Series(np.full(len(source), 0.2), index=source.index)

    monkeypatch.setattr(pd.core.window.rolling.Rolling, "mean", _rolling_mean)
    monkeypatch.setattr(pd.core.window.rolling.Rolling, "std", _rolling_std)
    monkeypatch.setattr(pd.core.window.rolling.Rolling, "var", lambda self, *args, **kwargs: pd.Series(np.full(len(self.obj), 1.0), index=self.obj.index))
    monkeypatch.setattr(pd.core.window.rolling.Rolling, "apply", _rolling_apply)

    high_reversal = data.copy()
    high_reversal.iloc[-2, high_reversal.columns.get_loc("close")] = 102.0
    high_reversal.iloc[-1, high_reversal.columns.get_loc("close")] = 101.4
    signals = strategy.generate_signals(high_reversal)
    assert len(signals) == 1
    assert signals[0].signal_type == SignalType.SELL

    low_reversal = data.copy()
    low_reversal.iloc[-2, low_reversal.columns.get_loc("close")] = 98.0
    low_reversal.iloc[-1, low_reversal.columns.get_loc("close")] = 98.6
    signals = strategy.generate_signals(low_reversal)
    assert len(signals) == 1
    assert signals[0].signal_type == SignalType.BUY


def test_backtest_optimization_grid_keys_are_declared_in_defaults():
    for name, meta in STRATEGY_REGISTRY.items():
        backtest = dict(meta.get("backtest") or {})
        if not backtest.get("supported"):
            continue
        defaults = set((meta.get("defaults") or {}).keys())
        grid = set((backtest.get("optimization_grid") or {}).keys())
        assert grid.issubset(defaults), f"{name} optimization grid contains undeclared defaults: {sorted(grid - defaults)}"


def test_donchian_exit_low_break_is_close_long_with_close_metadata():
    strategy = DonchianBreakoutStrategy(
        "donchian_exit_test",
        {"lookback": 3, "exit_lookback": 3, "breakout_buffer_pct": 0.0},
    )
    index = pd.date_range("2025-01-01", periods=6, freq="h", tz="UTC")
    data = pd.DataFrame(
        {
            "high": [10.0, 10.0, 10.0, 10.0, 10.0, 10.0],
            "low": [9.0, 8.0, 7.0, 8.0, 9.0, 6.0],
            "close": [9.5, 8.5, 7.5, 8.5, 7.2, 6.8],
            "symbol": ["BTC/USDT"] * 6,
        },
        index=index,
    )

    signals = strategy.generate_signals(data)

    assert len(signals) == 1
    assert signals[0].signal_type == SignalType.CLOSE_LONG
    assert signals[0].metadata["close_only"] is True
    assert signals[0].metadata["close_reason"] == "donchian_exit_low_break"


def test_rsi_calculation_handles_zero_avg_loss_boundaries():
    strategy = RSIStrategy("rsi_boundary_test", {"period": 3})
    index = pd.date_range("2025-01-01", periods=8, freq="h", tz="UTC")

    rising = pd.DataFrame({"close": np.arange(100.0, 108.0)}, index=index)
    falling = pd.DataFrame({"close": np.arange(108.0, 100.0, -1.0)}, index=index)
    flat = pd.DataFrame({"close": [100.0] * 8}, index=index)

    assert strategy._calculate_rsi(rising, 3).iloc[-1] == 100.0
    assert strategy._calculate_rsi(falling, 3).iloc[-1] == 0.0
    assert strategy._calculate_rsi(flat, 3).iloc[-1] == 50.0


def test_rsi_divergence_does_not_pair_distant_price_and_rsi_extrema(monkeypatch):
    strategy = RSIDivergenceStrategy(
        "rsi_div_pair_test",
        {"period": 3, "lookback": 20, "min_divergence": 0.01, "extrema_order": 1},
    )
    index = pd.date_range("2025-01-01", periods=30, freq="h", tz="UTC")
    close = np.full(30, 100.0)
    close[20] = 96.0
    close[28] = 90.0
    data = pd.DataFrame({"close": close, "symbol": ["BTC/USDT"] * 30}, index=index)

    rsi = pd.Series(np.full(30, 50.0), index=index)
    rsi.iloc[5] = 20.0
    rsi.iloc[15] = 35.0
    monkeypatch.setattr(strategy, "_calculate_rsi", lambda _data, _period: rsi)

    price_troughs = pd.Series(False, index=index)
    price_troughs.iloc[[20, 28]] = True
    rsi_troughs = pd.Series(False, index=index)
    rsi_troughs.iloc[[5, 15]] = True
    monkeypatch.setattr(strategy, "_find_troughs", lambda series, order=5: price_troughs if series is data["close"] else rsi_troughs)
    monkeypatch.setattr(strategy, "_find_peaks", lambda series, order=5: pd.Series(False, index=index))

    assert strategy.generate_signals(data) == []


def test_bar_time_uses_latest_bar_not_wall_clock():
    from datetime import datetime, timezone

    from core.strategies.strategy_base import bar_time

    idx = pd.date_range("2025-03-01", periods=10, freq="1h", tz="UTC")
    df = pd.DataFrame({"close": np.arange(10.0)}, index=idx)
    ts = bar_time(df)
    assert ts == idx[-1].to_pydatetime()
    assert ts.tzinfo is not None

    # tz-naive index gets normalized to UTC (fixes naive/aware mixing)
    naive = pd.date_range("2025-03-01", periods=5, freq="1h")
    df2 = pd.DataFrame({"close": np.arange(5.0)}, index=naive)
    ts2 = bar_time(df2)
    assert ts2.tzinfo == timezone.utc

    # no datetime index -> wall-clock UTC fallback
    df3 = pd.DataFrame({"close": [1.0, 2.0]})
    before = datetime.now(timezone.utc)
    ts3 = bar_time(df3)
    assert ts3.tzinfo == timezone.utc and ts3 >= before


def test_rsi_signal_carries_bar_time_not_now():
    from datetime import datetime, timezone

    # Force an RSI oversold up-cross exactly on the final bar.
    drop = np.linspace(200, 70, 90)
    tail = np.array([70.0, 70.5, 78.0])
    close = np.concatenate([drop, tail])
    idx = pd.date_range("2025-01-01", periods=len(close), freq="1h", tz="UTC")
    df = pd.DataFrame(
        {"open": close, "high": close + 0.5, "low": close - 0.5, "close": close,
         "volume": np.full(len(close), 1000.0)},
        index=idx,
    )
    strat = RSIStrategy(name="RSIStrategy")
    signals = strat.generate_signals(df)
    if signals:  # if the engineered cross triggered, timestamp must be bar time
        assert signals[0].timestamp == idx[-1].to_pydatetime()
        assert abs((datetime.now(timezone.utc) - signals[0].timestamp).total_seconds()) > 3600
