"""Unified strategy registry metadata for API, UI, and backtest adapters."""
from __future__ import annotations

from copy import deepcopy
from importlib import util as importlib_util
from pathlib import Path
from typing import Any, Dict, List


DEFAULT_START_ALL_STRATEGIES: List[str] = [
    "MAStrategy",
    "EMAStrategy",
    "RSIStrategy",
    "MACDStrategy",
    "BollingerBandsStrategy",
    "MeanReversionStrategy",
    "MomentumStrategy",
    "DonchianBreakoutStrategy",
    "StochasticStrategy",
    "ADXTrendStrategy",
    "VWAPReversionStrategy",
    "MarketSentimentStrategy",
    "SocialSentimentStrategy",
    "FundFlowStrategy",
    "WhaleActivityStrategy",
]


STRATEGY_REGISTRY: Dict[str, Dict[str, Any]] = {
    "MAStrategy": {
        "category": "趋势",
        "risk": "low",
        "usage": "双均线金叉死叉",
        "defaults": {"fast_period": 20, "slow_period": 60, "signal_threshold": 0.0015, "stop_loss_pct": 0.03, "take_profit_pct": 0.08},
        "timeframe": "15m",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "双均线趋势策略（Long/Flat）", "optimization_grid": {"fast_period": [5, 8, 10, 12, 20], "slow_period": [20, 30, 40, 60]}},
    },
    "EMAStrategy": {
        "category": "趋势",
        "risk": "low",
        "usage": "EMA快慢线交叉",
        "defaults": {"fast_period": 12, "slow_period": 26, "signal_threshold": 0.0012, "stop_loss_pct": 0.025, "take_profit_pct": 0.06},
        "timeframe": "15m",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "EMA双均线趋势策略", "optimization_grid": {"fast_period": [8, 12, 16], "slow_period": [21, 26, 34, 55]}},
    },
    "MACDStrategy": {
        "category": "趋势",
        "risk": "medium",
        "usage": "MACD趋势跟随",
        "defaults": {"fast_period": 12, "slow_period": 26, "signal_period": 9, "stop_loss_pct": 0.025, "take_profit_pct": 0.06},
        "timeframe": "15m",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "MACD上穿持有、下穿离场", "optimization_grid": {"fast_period": [8, 12, 16], "slow_period": [21, 26, 34], "signal_period": [7, 9, 12]}},
    },
    "MACDHistogramStrategy": {
        "category": "趋势",
        "risk": "medium",
        "usage": "MACD柱体动量",
        "defaults": {"fast_period": 12, "slow_period": 26, "signal_period": 9, "min_histogram": 0.0002, "stop_loss_pct": 0.025, "take_profit_pct": 0.06},
        "timeframe": "15m",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "MACD柱体拐点策略", "optimization_grid": {"fast_period": [8, 12, 16], "slow_period": [21, 26, 34], "signal_period": [7, 9, 12], "min_histogram": [0.00005, 0.0001, 0.0002]}},
    },
    "ADXTrendStrategy": {
        "category": "趋势",
        "risk": "medium",
        "usage": "ADX趋势强度确认",
        "defaults": {"period": 14, "adx_threshold": 23.0, "stop_loss_pct": 0.025, "take_profit_pct": 0.07},
        "timeframe": "1h",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "ADX趋势策略", "optimization_grid": {"period": [10, 14, 20], "adx_threshold": [20, 25, 30]}},
    },
    "TrendFollowingStrategy": {
        "category": "趋势",
        "risk": "medium",
        "usage": "多均线趋势跟踪",
        "defaults": {"short_period": 20, "long_period": 55, "adx_threshold": 23, "stop_loss_pct": 0.03, "take_profit_pct": 0.09},
        "timeframe": "1h",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "趋势跟随策略", "optimization_grid": {"short_period": [10, 20, 30], "long_period": [40, 50, 80], "adx_threshold": [20, 25, 30]}},
    },
    "AroonStrategy": {
        "category": "趋势",
        "risk": "medium",
        "usage": "Aroon趋势识别",
        "defaults": {"period": 25, "buy_threshold": 50, "sell_threshold": -50, "stop_loss_pct": 0.03, "take_profit_pct": 0.06},
        "timeframe": "1h",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "Aroon趋势策略", "optimization_grid": {"period": [14, 25, 35], "buy_threshold": [30, 50, 70], "sell_threshold": [-70, -50, -30]}},
    },
    "RSIStrategy": {
        "category": "震荡",
        "risk": "medium",
        "usage": "RSI超买超卖",
        "defaults": {"period": 14, "oversold": 30, "overbought": 70, "exit_oversold": 42, "exit_overbought": 58, "stop_loss_pct": 0.025, "take_profit_pct": 0.055},
        "timeframe": "15m",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "RSI超卖入场、超买离场", "optimization_grid": {"period": [10, 14, 21], "oversold": [20, 25, 30], "overbought": [65, 70, 75], "exit_oversold": [38, 42, 46], "exit_overbought": [54, 58, 62]}},
    },
    "RSIDivergenceStrategy": {
        "category": "震荡",
        "risk": "medium",
        "usage": "RSI顶底背离",
        "defaults": {"period": 14, "lookback": 34, "min_divergence": 0.015, "extrema_order": 5, "stop_loss_pct": 0.03, "take_profit_pct": 0.08},
        "timeframe": "15m",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "RSI背离策略", "optimization_grid": {"period": [10, 14, 21], "lookback": [12, 20, 30], "min_divergence": [0.01, 0.02, 0.03], "extrema_order": [3, 5, 7]}},
    },
    "StochasticStrategy": {
        "category": "震荡",
        "risk": "medium",
        "usage": "KDJ随机震荡",
        "defaults": {"k_period": 14, "d_period": 3, "smooth_k": 3, "oversold": 20.0, "overbought": 80.0, "stop_loss_pct": 0.02, "take_profit_pct": 0.05},
        "timeframe": "15m",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "随机指标策略", "optimization_grid": {"k_period": [9, 14, 21], "d_period": [3, 5], "oversold": [15, 20, 25], "overbought": [75, 80, 85]}},
    },
    "BollingerBandsStrategy": {
        "category": "震荡",
        "risk": "medium",
        "usage": "布林带回归",
        "defaults": {"period": 20, "num_std": 2.0, "stop_loss_pct": 0.025, "take_profit_pct": 0.05},
        "timeframe": "15m",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "布林带均值回归策略", "optimization_grid": {"period": [14, 20, 26], "num_std": [1.8, 2.0, 2.2, 2.5]}},
    },
    "WilliamsRStrategy": {
        "category": "震荡",
        "risk": "medium",
        "usage": "威廉超买超卖",
        "defaults": {"period": 14, "oversold": -80, "overbought": -20, "stop_loss_pct": 0.025, "take_profit_pct": 0.05},
        "timeframe": "15m",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "威廉%R策略", "optimization_grid": {"period": [10, 14, 21], "oversold": [-90, -80, -70], "overbought": [-30, -20, -10]}},
    },
    "CCIStrategy": {
        "category": "震荡",
        "risk": "medium",
        "usage": "CCI通道指数",
        "defaults": {"period": 20, "constant": 0.015, "oversold": -100, "overbought": 100, "stop_loss_pct": 0.025, "take_profit_pct": 0.05},
        "timeframe": "15m",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "CCI通道策略", "optimization_grid": {"period": [14, 20, 30], "oversold": [-150, -100, -80], "overbought": [80, 100, 150]}},
    },
    "StochRSIStrategy": {
        "category": "震荡",
        "risk": "medium",
        "usage": "RSI随机震荡",
        "defaults": {"rsi_period": 14, "stoch_period": 14, "oversold": 20, "overbought": 80, "stop_loss_pct": 0.025, "take_profit_pct": 0.05},
        "timeframe": "15m",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "随机RSI策略", "optimization_grid": {"rsi_period": [10, 14, 21], "stoch_period": [10, 14, 21], "oversold": [15, 20, 25], "overbought": [75, 80, 85]}},
    },
    "MomentumStrategy": {
        "category": "动量",
        "risk": "medium",
        "usage": "价格动量突破",
        "defaults": {"lookback_period": 20, "momentum_threshold": 0.015, "stop_loss_pct": 0.03, "take_profit_pct": 0.07},
        "timeframe": "1h",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "动量突破策略", "optimization_grid": {"lookback_period": [10, 14, 20, 30], "momentum_threshold": [0.01, 0.015, 0.02, 0.03]}},
    },
    "ROCStrategy": {
        "category": "动量",
        "risk": "medium",
        "usage": "变化率动量",
        "defaults": {"period": 14, "buy_threshold": 5.0, "sell_threshold": -5.0, "stop_loss_pct": 0.03, "take_profit_pct": 0.06},
        "timeframe": "15m",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "变化率动量策略", "optimization_grid": {"period": [10, 14, 21], "buy_threshold": [3.0, 5.0, 8.0], "sell_threshold": [-8.0, -5.0, -3.0]}},
    },
    "PriceAccelerationStrategy": {
        "category": "动量",
        "risk": "medium",
        "usage": "价格加速度",
        "defaults": {"fast": 5, "slow": 15, "accel_threshold": 0.1, "stop_loss_pct": 0.025, "take_profit_pct": 0.05},
        "timeframe": "15m",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "价格加速度策略", "optimization_grid": {"fast": [3, 5, 8], "slow": [10, 15, 20], "accel_threshold": [0.05, 0.1, 0.15]}},
    },
    "MeanReversionStrategy": {
        "category": "均值回归",
        "risk": "medium",
        "usage": "Z-Score回归",
        "defaults": {"lookback_period": 24, "entry_z_score": 2.1, "exit_z_score": 0.6, "stop_loss_pct": 0.03, "take_profit_pct": 0.06},
        "timeframe": "1h",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "Z-Score均值回归策略", "optimization_grid": {"lookback_period": [14, 20, 30], "entry_z_score": [1.5, 2.0, 2.5], "exit_z_score": [0.3, 0.5, 0.8]}},
    },
    "BollingerMeanReversionStrategy": {
        "category": "均值回归",
        "risk": "medium",
        "usage": "布林带均值回归",
        "defaults": {"period": 20, "num_std": 2.2, "stop_loss_pct": 0.02, "take_profit_pct": 0.04},
        "timeframe": "1h",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "布林均值回归策略", "optimization_grid": {"period": [14, 20, 26], "num_std": [1.8, 2.0, 2.2, 2.5]}},
    },
    "VWAPReversionStrategy": {
        "category": "均值回归",
        "risk": "low",
        "usage": "VWAP价格回归",
        "defaults": {"window": 48, "entry_deviation_pct": 0.012, "exit_deviation_pct": 0.003, "stop_loss_pct": 0.02, "take_profit_pct": 0.035},
        "timeframe": "15m",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "VWAP偏离回归策略", "optimization_grid": {"window": [24, 48, 72], "entry_deviation_pct": [0.006, 0.01, 0.015], "exit_deviation_pct": [0.001, 0.002, 0.003]}},
    },
    "VWAPStrategy": {
        "category": "均值回归",
        "risk": "low",
        "usage": "成交量加权回归",
        "defaults": {"period": 20, "buy_threshold": -0.02, "sell_threshold": 0.02, "stop_loss_pct": 0.02, "take_profit_pct": 0.03},
        "timeframe": "15m",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "VWAP均值回归策略", "optimization_grid": {"period": [14, 20, 30], "buy_threshold": [-0.03, -0.02, -0.01], "sell_threshold": [0.01, 0.02, 0.03]}},
    },
    "MeanReversionHalfLifeStrategy": {
        "category": "均值回归",
        "risk": "medium",
        "usage": "半衰期回归",
        "defaults": {"lookback": 60, "zscore_entry": 2.0, "zscore_exit": 0.5, "stop_loss_pct": 0.03, "take_profit_pct": 0.05},
        "timeframe": "1h",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "半衰期均值回归策略", "optimization_grid": {"lookback": [30, 60, 90], "zscore_entry": [1.5, 2.0, 2.5], "zscore_exit": [0.3, 0.5, 0.8]}},
    },
    "BollingerSqueezeStrategy": {
        "category": "突破",
        "risk": "medium",
        "usage": "布林带收窄突破",
        "defaults": {"period": 20, "num_std": 2.0, "squeeze_threshold": 0.018, "breakout_threshold": 0.008, "stop_loss_pct": 0.03, "take_profit_pct": 0.08},
        "timeframe": "15m",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "布林挤压突破策略", "optimization_grid": {"period": [14, 20, 26], "num_std": [1.8, 2.0, 2.2], "squeeze_threshold": [0.01, 0.02, 0.03], "breakout_threshold": [0.005, 0.01, 0.015]}},
    },
    "DonchianBreakoutStrategy": {
        "category": "突破",
        "risk": "medium",
        "usage": "唐奇安通道突破",
        "defaults": {"lookback": 20, "exit_lookback": 10, "breakout_buffer_pct": 0.001, "stop_loss_pct": 0.025, "take_profit_pct": 0.08},
        "timeframe": "1h",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "Donchian通道突破策略", "optimization_grid": {"lookback": [14, 20, 30], "exit_lookback": [7, 10, 14]}},
    },
    "MFIStrategy": {
        "category": "成交量",
        "risk": "medium",
        "usage": "资金流量指标",
        "defaults": {"period": 14, "oversold": 20, "overbought": 80, "stop_loss_pct": 0.025, "take_profit_pct": 0.05},
        "timeframe": "15m",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "资金流量指标策略", "optimization_grid": {"period": [10, 14, 21], "oversold": [15, 20, 25], "overbought": [75, 80, 85]}},
    },
    "OBVStrategy": {
        "category": "成交量",
        "risk": "medium",
        "usage": "能量潮背离",
        "defaults": {"smooth": 20, "divergence_threshold": 1.5, "stop_loss_pct": 0.025, "take_profit_pct": 0.05},
        "timeframe": "15m",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "能量潮背离策略", "optimization_grid": {"smooth": [10, 20, 30], "divergence_threshold": [1.0, 1.5, 2.0]}},
    },
    "TradeIntensityStrategy": {
        "category": "成交量",
        "risk": "medium",
        "usage": "成交量异动",
        "defaults": {"fast": 5, "slow": 20, "intensity_threshold": 1.5, "stop_loss_pct": 0.025, "take_profit_pct": 0.05},
        "timeframe": "5m",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "交易强度策略", "optimization_grid": {"fast": [3, 5, 8], "slow": [15, 20, 30], "intensity_threshold": [1.2, 1.5, 2.0]}},
    },
    "ParkinsonVolStrategy": {
        "category": "波动率",
        "risk": "medium",
        "usage": "高低波动率回归",
        "defaults": {"period": 20, "vol_percentile_low": 20, "vol_percentile_high": 80, "stop_loss_pct": 0.03, "take_profit_pct": 0.05},
        "timeframe": "1h",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "Parkinson波动率策略", "optimization_grid": {"period": [14, 20, 30], "vol_percentile_low": [15, 20, 25], "vol_percentile_high": [75, 80, 85]}},
    },
    "UlcerIndexStrategy": {
        "category": "风险",
        "risk": "low",
        "usage": "下行风险择时",
        "defaults": {"period": 14, "high_risk_threshold": 10, "low_risk_threshold": 3, "stop_loss_pct": 0.03, "take_profit_pct": 0.06},
        "timeframe": "1h",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "Ulcer风险择时策略", "optimization_grid": {"period": [10, 14, 21], "high_risk_threshold": [8, 10, 12], "low_risk_threshold": [2, 3, 5]}},
    },
    "VaRBreakoutStrategy": {
        "category": "风险",
        "risk": "medium",
        "usage": "VaR异常突破",
        "defaults": {"var_period": 20, "confidence": 0.95, "multiplier": 1.5, "stop_loss_pct": 0.02, "take_profit_pct": 0.04},
        "timeframe": "15m",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "VaR异常突破策略", "optimization_grid": {"var_period": [15, 20, 30], "confidence": [0.90, 0.95, 0.99], "multiplier": [1.2, 1.5, 2.0]}},
    },
    "MaxDrawdownStrategy": {
        "category": "风险",
        "risk": "low",
        "usage": "回撤反弹买入",
        "defaults": {"lookback": 30, "dd_threshold": -0.10, "recovery_threshold": 0.3, "stop_loss_pct": 0.03, "take_profit_pct": 0.08},
        "timeframe": "1h",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "回撤恢复策略", "optimization_grid": {"lookback": [20, 30, 45], "dd_threshold": [-0.15, -0.10, -0.08], "recovery_threshold": [0.2, 0.3, 0.4]}},
    },
    "SortinoRatioStrategy": {
        "category": "风险",
        "risk": "medium",
        "usage": "风险调整趋势",
        "defaults": {"period": 30, "sortino_threshold": 1.0, "lookback_trend": 5, "stop_loss_pct": 0.03, "take_profit_pct": 0.06},
        "timeframe": "1h",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "Sortino风险调整策略", "optimization_grid": {"period": [20, 30, 45], "sortino_threshold": [0.5, 1.0, 1.5]}},
    },
    "PairsTradingStrategy": {
        "category": "统计套利",
        "risk": "high",
        "usage": "配对价差回归",
        "defaults": {"lookback_period": 48, "entry_z_score": 2.0, "exit_z_score": 0.6, "hedge_ratio_method": "ols", "allow_negative_hedge_ratio": True, "min_hedge_ratio": -5.0, "max_hedge_ratio": 5.0, "stop_loss_pct": 0.04, "pair_symbol": "ETH/USDT", "market_type": "future", "allow_long": True, "allow_short": True, "reverse_on_signal": True, "allow_pyramiding": False},
        "timeframe": "1h",
        "symbols": ["BTC/USDT", "ETH/USDT"],
        "backtest": {"supported": True, "description": "配对交易策略（近似单腿回测）", "optimization_grid": {"lookback_period": [14, 20, 30, 40], "entry_z_score": [1.5, 2.0, 2.5], "exit_z_score": [0.3, 0.5, 0.8]}},
    },
    "FamaFactorArbitrageStrategy": {
        "category": "统计套利",
        "risk": "high",
        "usage": "多因子横截面",
        "defaults": {"exchange": "binance", "factor_timeframe": "1h", "universe_symbols": ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT", "DOGE/USDT", "ADA/USDT", "AVAX/USDT", "LINK/USDT", "DOT/USDT", "MATIC/USDT", "LTC/USDT"], "max_symbols": 100, "lookback_bars": 720, "min_symbol_bars": 300, "min_universe_size": 12, "quantile": 0.25, "top_n": 8, "min_abs_score": 0.15, "alpha_threshold": 0.15, "rebalance_interval_minutes": 60, "cooldown_min": 60, "max_vol": 0.20, "max_spread": 0.08, "stop_loss_pct": 0.03, "take_profit_pct": 0.06, "market_type": "future", "allow_long": True, "allow_short": True, "reverse_on_signal": True, "allow_pyramiding": False},
        "timeframe": "1h",
        "symbols": ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "XRP/USDT", "DOGE/USDT"],
        "backtest": {"supported": True, "description": "多因子横截面多空策略", "optimization_grid": {"quantile": [0.2, 0.25, 0.33], "top_n": [4, 6, 8], "min_abs_score": [0.05, 0.10, 0.15], "rebalance_interval_minutes": [30, 60, 120]}},
    },
    "HurstExponentStrategy": {
        "category": "统计套利",
        "risk": "medium",
        "usage": "长记忆regime",
        "defaults": {"hurst_period": 100, "zscore_period": 20, "trending_threshold": 0.55, "mean_revert_threshold": 0.45, "zscore_threshold": 1.5, "stop_loss_pct": 0.03, "take_profit_pct": 0.06},
        "timeframe": "1h",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "Hurst长记忆策略", "optimization_grid": {"hurst_period": [50, 100, 150], "zscore_threshold": [1.0, 1.5, 2.0]}},
    },
    "OrderFlowImbalanceStrategy": {
        "category": "微观结构",
        "risk": "medium",
        "usage": "订单流失衡",
        "defaults": {"period": 10, "imbalance_threshold": 1.0, "stop_loss_pct": 0.02, "take_profit_pct": 0.04},
        "timeframe": "5m",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "订单流失衡策略", "optimization_grid": {"period": [5, 10, 15], "imbalance_threshold": [0.8, 1.0, 1.5]}},
    },
    "MultiFactorHFStrategy": {
        "category": "量化",
        "risk": "medium",
        "usage": "多因子高频组合策略(5m)",
        "defaults": {"enter_th": 0.75, "exit_th": 0.25, "cooldown_bars": 2, "stop_loss_pct": 0.025, "take_profit_pct": 0.05},
        "timeframe": "5m",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "多因子高频策略(5m)", "optimization_grid": {"enter_th": [0.5, 0.65, 0.75, 0.9], "exit_th": [0.1, 0.2, 0.3], "cooldown_bars": [1, 2, 3]}},
    },
    "MLXGBoostStrategy": {
        "category": "机器学习",
        "risk": "medium",
        "usage": "XGBoost 方向预测",
        "family": "ml",
        "decision_engine": "ml",
        "ai_driven": True,
        "defaults": {"threshold": 0.55, "neutral_exit_enabled": True, "stop_loss_pct": 0.025, "take_profit_pct": 0.06},
        "timeframe": "1h",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "XGBoost 价格方向分类策略", "optimization_grid": {"threshold": [0.52, 0.55, 0.58, 0.62]}},
    },
    "CEXArbitrageStrategy": {
        "category": "套利",
        "risk": "high",
        "usage": "跨所价差套利",
        "defaults": {"min_spread": 0.002, "alpha_threshold": 0.002, "min_volume": 50000, "exchanges": ["binance", "okx", "gate"], "max_position_size": 2000, "consider_fees": True, "fee_rate": 0.0008, "max_opportunities": 2, "cooldown_min": 1, "max_vol": 0.03, "max_spread": 0.03},
        "timeframe": "5m",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": False, "description": "跨交易所套利策略", "reason": "依赖多交易所盘口/买卖价差，非单一OHLCV回测模型"},
    },
    "TriangularArbitrageStrategy": {
        "category": "套利",
        "risk": "high",
        "usage": "三角路径套利",
        "defaults": {"exchange": "binance", "base_currency": "USDT", "min_profit": 0.002, "alpha_threshold": 0.002, "consider_fees": True, "fee_rate": 0.0007, "bridge_assets": ["ETH", "BNB", "SOL"], "max_opportunities": 2, "cooldown_min": 1, "max_spread": 0.03},
        "timeframe": "5m",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": False, "description": "三角套利策略", "reason": "依赖同交易所多交易对实时报价，不适用单一K线回测"},
    },
    "DEXArbitrageStrategy": {
        "category": "套利",
        "risk": "high",
        "usage": "链上DEX套利",
        "defaults": {"min_spread": 0.008, "min_profit_usd": 30, "max_gas_cost": 20, "dex_list": ["uniswap", "sushiswap"], "chain": "ethereum"},
        "timeframe": "5m",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": False, "description": "DEX套利策略", "reason": "依赖链上流动性池与实时路由报价"},
    },
    "FlashLoanArbitrageStrategy": {
        "category": "套利",
        "risk": "high",
        "usage": "闪电贷套利",
        "defaults": {"min_profit": 0.004, "loan_amount": 100000, "dex_list": ["uniswap", "sushiswap"]},
        "timeframe": "5m",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": False, "description": "闪电贷套利策略", "reason": "依赖链上原子交易执行，K线回测无法刻画"},
    },
    "MarketSentimentStrategy": {
        "category": "宏观",
        "risk": "medium",
        "usage": "恐慌贪婪指数",
        "defaults": {"fear_threshold": 25, "greed_threshold": 75, "neutral_exit_enabled": True, "neutral_exit_buffer": 5, "lookback_period": 7, "exchange": "binance", "stop_loss_pct": 0.04, "take_profit_pct": 0.09, "timeout_sec": 6},
        "timeframe": "15m",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "宏观情绪策略", "optimization_grid": {"fear_threshold": [20, 25, 30], "greed_threshold": [70, 75, 80], "neutral_exit_buffer": [3, 5, 8]}},
    },
    "SocialSentimentStrategy": {
        "category": "宏观",
        "risk": "medium",
        "usage": "社媒情绪分析",
        "defaults": {"positive_threshold": 0.2, "negative_threshold": -0.2, "neutral_exit_enabled": True, "neutral_exit_buffer": 0.08, "min_mentions": 30, "stop_loss_pct": 0.04, "take_profit_pct": 0.09, "timeout_sec": 6},
        "timeframe": "15m",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "社媒情绪策略", "optimization_grid": {"positive_threshold": [0.15, 0.2, 0.25], "negative_threshold": [-0.25, -0.2, -0.15], "min_mentions": [20, 30, 50]}},
    },
    "FundFlowStrategy": {
        "category": "宏观",
        "risk": "medium",
        "usage": "交易所资金流",
        "defaults": {"inflow_threshold": 150000.0, "outflow_threshold": -150000.0, "min_imbalance_ratio": 0.03, "neutral_exit_enabled": True, "neutral_exit_imbalance_ratio": 0.01, "lookback_period": 7, "book_depth": 80, "exchange": "binance", "stop_loss_pct": 0.04, "take_profit_pct": 0.09},
        "timeframe": "15m",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "资金流策略", "optimization_grid": {"min_imbalance_ratio": [0.05, 0.08, 0.12], "inflow_threshold": [200000, 500000, 1000000], "outflow_threshold": [-200000, -500000, -1000000]}},
    },
    "WhaleActivityStrategy": {
        "category": "宏观",
        "risk": "high",
        "usage": "巨鲸地址追踪",
        "defaults": {"min_whale_size": 100000.0, "accumulation_threshold": 2, "distribution_threshold": 2, "neutral_exit_enabled": True, "lookback_hours": 24, "trade_limit": 600, "exchange": "binance", "stop_loss_pct": 0.04, "take_profit_pct": 0.09},
        "timeframe": "15m",
        "symbols": ["BTC/USDT"],
        "backtest": {"supported": True, "description": "巨鲸活动策略", "optimization_grid": {"min_whale_size": [100000, 150000, 300000], "accumulation_threshold": [3, 4, 5], "distribution_threshold": [3, 4, 5]}},
    },
}


_STRATEGY_META_OVERRIDES: Dict[str, Dict[str, Any]] = {
    "MLXGBoostStrategy": {
        "family": "ml",
        "decision_engine": "ml",
        "ai_driven": True,
    },
    "MarketSentimentStrategy": {
        "family": "ai_glm",
        "decision_engine": "glm",
        "ai_driven": True,
    },
    "SocialSentimentStrategy": {
        "family": "ai_glm",
        "decision_engine": "glm",
        "ai_driven": True,
    },
    "FundFlowStrategy": {
        "family": "ai_glm",
        "decision_engine": "glm",
        "ai_driven": True,
    },
    "WhaleActivityStrategy": {
        "family": "ai_glm",
        "decision_engine": "glm",
        "ai_driven": True,
    },
}


def get_strategy_registry_entry(name: str) -> Dict[str, Any]:
    key = str(name)
    item = deepcopy(STRATEGY_REGISTRY.get(key, {}))
    if item:
        item.update(_STRATEGY_META_OVERRIDES.get(key, {}))
    return item


def get_strategy_library_meta(name: str) -> Dict[str, Any]:
    item = get_strategy_registry_entry(name)
    return {
        k: item.get(k)
        for k in ("category", "risk", "usage", "family", "decision_engine", "ai_driven")
        if k in item
    }


def get_strategy_defaults(name: str) -> Dict[str, Any]:
    return deepcopy(STRATEGY_REGISTRY.get(str(name), {}).get("defaults", {}))


def get_strategy_recommended_timeframe(name: str) -> str:
    return str(STRATEGY_REGISTRY.get(str(name), {}).get("timeframe", "1h"))


_MULTI_SYMBOL_CATEGORIES = {"趋势", "震荡", "动量", "均值回归", "突破", "成交量", "波动率", "风险"}
_COMMON_SYMBOLS = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"]


def get_strategy_recommended_symbols(name: str) -> List[str]:
    item = STRATEGY_REGISTRY.get(str(name), {})
    syms = item.get("symbols", [])
    # Strategies that only declare ["BTC/USDT"] but belong to a generic technical category
    # get expanded to the common multi-symbol list for better UI discoverability.
    if (not syms or syms == ["BTC/USDT"]) and item.get("category", "") in _MULTI_SYMBOL_CATEGORIES:
        return list(_COMMON_SYMBOLS)
    return list(syms) if syms else ["BTC/USDT"]


def _mlxgboost_model_candidates() -> List[Path]:
    defaults = STRATEGY_REGISTRY.get("MLXGBoostStrategy", {}).get("defaults", {})
    configured_path = str(defaults.get("model_path") or "").strip()
    repo_model = Path(__file__).resolve().parents[1] / "models" / "ml_signal_xgb.json"
    raw_candidates = [configured_path, "models/ml_signal_xgb.json", str(repo_model)]

    candidates: List[Path] = []
    for raw in raw_candidates:
        if not raw:
            continue
        path = Path(raw)
        if path not in candidates:
            candidates.append(path)
    return candidates


def _mlxgboost_backtest_support_status() -> tuple[bool, str | None]:
    if importlib_util.find_spec("xgboost") is None:
        return False, "当前环境缺少 xgboost，MLXGBoostStrategy 暂不可回测"
    if not any(path.exists() for path in _mlxgboost_model_candidates()):
        return False, "当前环境缺少 MLXGBoostStrategy 模型文件，暂不可回测"
    return True, None


def _resolve_backtest_support(name: str) -> tuple[bool, str | None]:
    item = STRATEGY_REGISTRY.get(str(name), {})
    bt = dict(item.get("backtest") or {})
    if not bool(bt.get("supported", False)):
        reason = str(bt.get("reason") or "").strip() or None
        return False, reason

    if str(name) == "MLXGBoostStrategy":
        return _mlxgboost_backtest_support_status()

    return True, None


def get_backtest_strategy_catalog(names: List[str] | None = None) -> List[Dict[str, Any]]:
    selected = list(names or STRATEGY_REGISTRY.keys())
    rows: List[Dict[str, Any]] = []
    for name in selected:
        item = STRATEGY_REGISTRY.get(str(name), {})
        bt = dict(item.get("backtest") or {})
        supported, reason = _resolve_backtest_support(name)
        rows.append(
            {
                "name": str(name),
                "description": str(bt.get("description") or item.get("usage") or name),
                "backtest_supported": supported,
                **({"reason": reason} if reason else {}),
            }
        )
    return rows


def get_backtest_strategy_info(name: str) -> Dict[str, Any]:
    item = STRATEGY_REGISTRY.get(str(name), {})
    bt = dict(item.get("backtest") or {})
    if not item:
        return {}
    supported, reason = _resolve_backtest_support(name)
    return {
        "name": str(name),
        "description": str(bt.get("description") or item.get("usage") or name),
        "backtest_supported": supported,
        **({"reason": reason} if reason else {}),
    }


def is_strategy_backtest_supported(name: str) -> bool:
    supported, _ = _resolve_backtest_support(name)
    return supported


def get_backtest_optimization_grid(name: str) -> Dict[str, List[Any]]:
    return deepcopy(STRATEGY_REGISTRY.get(str(name), {}).get("backtest", {}).get("optimization_grid", {}))
