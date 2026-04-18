"""
DEX套利策略
"""
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
import asyncio
from decimal import Decimal
from loguru import logger

from core.strategies.strategy_base import (
    StrategyBase,
    Signal,
    SignalType,
)
from core.exchanges.dex_connectors import (
    UniswapConnector,
    SushiSwapConnector,
    PancakeSwapConnector,
)
from config.exchanges import ExchangeConfig, ExchangeType


class DEXArbitrageStrategy(StrategyBase):
    """DEX套利策略"""

    def __init__(
        self,
        name: str = "DEX_Arbitrage",
        params: Optional[Dict[str, Any]] = None,
    ):
        default_params = {
            "min_spread": 0.01,  # 最小价差（1%）
            "min_profit_usd": 50,  # 最小利润（USD）
            "max_gas_cost": 30,  # 最大Gas费用（USD）
            "dex_list": ["uniswap", "sushiswap"],
            "chain": "ethereum",
        }
        if params:
            default_params.update(params)

        super().__init__(name, default_params)
        self._dex_connectors: Dict[str, Any] = {}

    async def initialize_dex_connectors(self) -> None:
        """初始化DEX连接器"""
        config = ExchangeConfig(
            name="dex",
            exchange_type=ExchangeType.DEX,
        )

        dex_classes = {
            "uniswap": UniswapConnector,
            "sushiswap": SushiSwapConnector,
            "pancakeswap": PancakeSwapConnector,
        }

        for dex_name in self.params["dex_list"]:
            if dex_name in dex_classes:
                connector = dex_classes[dex_name](config)
                if await connector.connect():
                    self._dex_connectors[dex_name] = connector
                    logger.info(f"DEX {dex_name} connected")

    async def get_quotes(
        self,
        token_in: str,
        token_out: str,
        amount: Decimal,
    ) -> Dict[str, Decimal]:
        """从各DEX获取报价"""
        quotes = {}

        for dex_name, connector in self._dex_connectors.items():
            try:
                quote = await connector.get_quote(token_in, token_out, amount)
                quotes[dex_name] = quote
            except Exception as e:
                logger.warning(f"Failed to get quote from {dex_name}: {e}")

        return quotes

    async def find_arbitrage_opportunities(
        self,
        token_a: str,
        token_b: str,
        amount: Decimal,
    ) -> List[Dict]:
        """寻找套利机会"""
        opportunities = []

        # 获取 A -> B 的报价
        quotes_ab = await self.get_quotes(token_a, token_b, amount)

        # 获取 B -> A 的报价
        quotes_ba = {}
        for dex_name, quote in quotes_ab.items():
            try:
                connector = self._dex_connectors[dex_name]
                quote_ba = await connector.get_quote(token_b, token_a, quote)
                quotes_ba[dex_name] = quote_ba
            except Exception as e:
                logger.warning(f"Failed to get reverse quote from {dex_name}: {e}")

        # 比较不同DEX间的价格
        for buy_dex, buy_quote in quotes_ab.items():
            for sell_dex, sell_quote in quotes_ba.items():
                if buy_dex == sell_dex:
                    continue

                profit = sell_quote - amount
                profit_pct = profit / amount

                if profit_pct >= Decimal(str(self.params["min_spread"])):
                    opportunities.append({
                        "token_a": token_a,
                        "token_b": token_b,
                        "buy_dex": buy_dex,
                        "sell_dex": sell_dex,
                        "amount": amount,
                        "buy_quote": buy_quote,
                        "sell_quote": sell_quote,
                        "profit": profit,
                        "profit_pct": profit_pct,
                        "timestamp": datetime.now(timezone.utc),
                    })

        return opportunities

    def generate_signals(self, data) -> List[Signal]:
        """生成交易信号"""
        return []

    async def generate_signals_async(
        self,
        token_a: str,
        token_b: str,
        amount: Decimal,
    ) -> List[Signal]:
        """异步生成交易信号"""
        signals = []

        opportunities = await self.find_arbitrage_opportunities(
            token_a, token_b, amount
        )

        for opp in opportunities:
            # 买入信号
            buy_signal = Signal(
                symbol=f"{token_a}/{token_b}",
                signal_type=SignalType.BUY,
                price=float(opp["buy_quote"]),
                timestamp=opp["timestamp"],
                strategy_name=self.name,
                strength=min(float(opp["profit_pct"]) / self.params["min_spread"], 1.0),
                metadata={
                    "dex": opp["buy_dex"],
                    "arbitrage_type": "dex_buy",
                    "profit": float(opp["profit"]),
                }
            )
            signals.append(buy_signal)

            # 卖出信号
            sell_signal = Signal(
                symbol=f"{token_b}/{token_a}",
                signal_type=SignalType.SELL,
                price=float(opp["sell_quote"]),
                timestamp=opp["timestamp"],
                strategy_name=self.name,
                strength=min(float(opp["profit_pct"]) / self.params["min_spread"], 1.0),
                metadata={
                    "dex": opp["sell_dex"],
                    "arbitrage_type": "dex_sell",
                    "profit": float(opp["profit"]),
                }
            )
            signals.append(sell_signal)

            logger.info(
                f"DEX arbitrage opportunity: {token_a}/{token_b} "
                f"buy@{opp['buy_dex']} sell@{opp['sell_dex']} "
                f"profit={float(opp['profit_pct'])*100:.2f}%"
            )

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        """获取所需数据"""
        return {
            "type": "dex_quotes",
            "dex_list": self.params["dex_list"],
        }


class FlashLoanArbitrageStrategy(StrategyBase):
    """闪电贷套利策略"""

    def __init__(
        self,
        name: str = "Flash_Loan_Arbitrage",
        params: Optional[Dict[str, Any]] = None,
    ):
        default_params = {
            "min_profit": 0.005,  # 最小利润率（0.5%）
            "loan_amount": 100000,  # 借款金额（USD）
            "dex_list": ["uniswap", "sushiswap"],
        }
        if params:
            default_params.update(params)

        super().__init__(name, default_params)

    def generate_signals(self, data) -> List[Signal]:
        """生成交易信号"""
        # 闪电贷套利需要智能合约支持，这里只提供框架
        return []

    def get_required_data(self) -> Dict[str, Any]:
        """获取所需数据"""
        return {
            "type": "dex_quotes",
            "requires_flash_loan": True,
        }
