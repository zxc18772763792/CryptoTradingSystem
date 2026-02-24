"""
回测模块
"""
from core.backtest.backtest_engine import (
    BacktestEngine,
    BacktestConfig,
    BacktestTrade,
    BacktestResult,
    backtest_engine,
)
from core.backtest.performance_analyzer import (
    PerformanceAnalyzer,
    PerformanceMetrics,
    performance_analyzer,
)
from core.backtest.report_generator import (
    ReportGenerator,
    report_generator,
)
from core.backtest.paper_trading import (
    PaperTradingEngine,
    RealTimeSimulator,
    paper_trading_engine,
    realtime_simulator,
)

__all__ = [
    "BacktestEngine",
    "BacktestConfig",
    "BacktestTrade",
    "BacktestResult",
    "backtest_engine",
    "PerformanceAnalyzer",
    "PerformanceMetrics",
    "performance_analyzer",
    "ReportGenerator",
    "report_generator",
    "PaperTradingEngine",
    "RealTimeSimulator",
    "paper_trading_engine",
    "realtime_simulator",
]
