"""
套利策略模块
"""
from strategies.arbitrage.cex_arbitrage import (
    CEXArbitrageStrategy,
    TriangularArbitrageStrategy,
)
try:
    from strategies.arbitrage.dex_arbitrage import (
        DEXArbitrageStrategy,
        FlashLoanArbitrageStrategy,
    )
except Exception:  # pragma: no cover - optional dependency (web3 etc.)
    DEXArbitrageStrategy = None
    FlashLoanArbitrageStrategy = None

__all__ = [
    "CEXArbitrageStrategy",
    "TriangularArbitrageStrategy",
    "DEXArbitrageStrategy",
    "FlashLoanArbitrageStrategy",
]
