"""
DEX套利策略
"""
import math
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
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


def _position_side_text(position: Any) -> str:
    side = getattr(position, "side", None)
    return str(getattr(side, "value", side) or "").strip().lower()


def _resolve_exit_ratio(params: Dict[str, Any], default: float = 0.5) -> float:
    try:
        ratio = float(params.get("exit_spread_ratio", default))
    except (TypeError, ValueError):
        ratio = default
    if not math.isfinite(ratio):
        ratio = default
    return min(max(ratio, 0.0), 1.0)


def _close_signal_price(data: Any, position: Any) -> float:
    try:
        if data is not None and "close" in getattr(data, "columns", []) and len(data):
            px = float(data["close"].iloc[-1])
            if math.isfinite(px) and px > 0:
                return px
    except Exception:
        pass
    for attr in ("current_price", "entry_price"):
        try:
            px = float(getattr(position, attr, 0.0) or 0.0)
        except (TypeError, ValueError):
            continue
        if math.isfinite(px) and px > 0:
            return px
    return 0.0


def _observation_age_sec(observed_at: Any, now: datetime) -> Optional[float]:
    try:
        return max(0.0, (now - observed_at).total_seconds())
    except (TypeError, AttributeError):
        return None


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
            # 价差收敛到 exit_spread_ratio * min_spread 以下时主动平仓
            "exit_spread_ratio": 0.5,
            # 价差观察超过此秒数视为过期，不触发平仓（<=0 关闭守卫）
            "exit_price_max_age_sec": 600.0,
        }
        if params:
            default_params.update(params)

        super().__init__(name, default_params)
        self._dex_connectors: Dict[str, Any] = {}
        self._last_spread_obs: Dict[str, Dict[str, Any]] = {}

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
        # 无论是否达到入场阈值都记录最优价差观察，供 check_exit 判断价差是否已回归
        best_observed_pct: Optional[Decimal] = None
        for buy_dex, buy_quote in quotes_ab.items():
            for sell_dex, sell_quote in quotes_ba.items():
                if buy_dex == sell_dex:
                    continue

                profit = sell_quote - amount
                profit_pct = profit / amount

                if best_observed_pct is None or profit_pct > best_observed_pct:
                    best_observed_pct = profit_pct

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

        if best_observed_pct is not None:
            directional_key = self._pair_key(token_a, token_b, directional=True)
            self._last_spread_obs[directional_key] = {
                "profit_pct": float(best_observed_pct),
                "at": datetime.now(timezone.utc),
            }

        return opportunities

    @staticmethod
    def _pair_key(token_a: Any, token_b: Any, directional: bool = False) -> str:
        tokens = [
            str(token or "").strip().upper() for token in (token_a, token_b)
        ]
        if not directional:
            tokens = sorted(tokens)
        return "/".join(tokens)

    def check_exit(self, data, position) -> Optional[Signal]:
        """价差回归后主动平仓。

        check_exit 是同步调用，无法 await DEX 报价，因此读取最近一次
        find_arbitrage_opportunities 记录的价差观察；观察过期时不平仓。
        """
        side = _position_side_text(position)
        if side not in {"long", "short"}:
            return None
        symbol = str(getattr(position, "symbol", "") or "").strip()
        raw = symbol.split(":", 1)[0].upper()
        tokens = [token for token in raw.split("/") if token]
        if len(tokens) != 2:
            return None

        metadata = getattr(position, "metadata", None)
        entry_pair_key = ""
        if isinstance(metadata, dict):
            entry_pair_key = str(metadata.get("dex_pair_key") or "").strip().upper()

        directional_key = self._pair_key(tokens[0], tokens[1], directional=True)
        reverse_key = self._pair_key(tokens[1], tokens[0], directional=True)
        unordered_key = self._pair_key(tokens[0], tokens[1])

        obs = None
        for key in (entry_pair_key, directional_key, reverse_key, unordered_key):
            if not key:
                continue
            candidate = self._last_spread_obs.get(key)
            if isinstance(candidate, dict):
                obs = candidate
                break
        if not isinstance(obs, dict):
            return None
        now = datetime.now(timezone.utc)
        age_sec = _observation_age_sec(obs.get("at"), now)
        max_age = float(self.params.get("exit_price_max_age_sec", 600.0) or 0.0)
        if age_sec is None or (max_age > 0 and age_sec > max_age):
            return None
        try:
            profit_pct = float(obs.get("profit_pct"))
        except (TypeError, ValueError):
            return None
        if not math.isfinite(profit_pct):
            return None

        min_spread = max(0.0, float(self.params.get("min_spread", 0.01) or 0.0))
        ratio = _resolve_exit_ratio(self.params)
        threshold = min_spread * ratio
        if profit_pct > threshold:
            return None

        return Signal(
            symbol=symbol,
            signal_type=SignalType.CLOSE_LONG if side == "long" else SignalType.CLOSE_SHORT,
            price=_close_signal_price(data, position),
            timestamp=self._bar_time(data),
            strategy_name=self.name,
            strength=0.7,
            metadata={
                "close_reason": "dex_arbitrage_spread_converged",
                "close_only": True,
                "current_profit_pct": float(profit_pct),
                "exit_threshold": float(threshold),
                "entry_threshold": float(min_spread),
                "exit_spread_ratio": float(ratio),
                "observation_age_sec": float(age_sec),
            },
        )

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
        if not self._dex_connectors:
            await self.initialize_dex_connectors()
        if not self._dex_connectors:
            return signals

        opportunities = await self.find_arbitrage_opportunities(
            token_a, token_b, amount
        )

        for opp in opportunities:
            amount_in = Decimal(opp["amount"])
            buy_quote = Decimal(opp["buy_quote"])
            sell_quote = Decimal(opp["sell_quote"])
            buy_unit_price = buy_quote / amount_in if amount_in else Decimal("0")
            sell_unit_price = sell_quote / buy_quote if buy_quote else Decimal("0")
            # 买入信号
            buy_signal = Signal(
                symbol=f"{token_a}/{token_b}",
                signal_type=SignalType.BUY,
                price=float(buy_unit_price),
                timestamp=opp["timestamp"],
                strategy_name=self.name,
                strength=min(float(opp["profit_pct"]) / self.params["min_spread"], 1.0),
                metadata={
                    "dex": opp["buy_dex"],
                    "arbitrage_type": "dex_buy",
                    "dex_pair_key": self._pair_key(token_a, token_b, directional=True),
                    "profit": float(opp["profit"]),
                    "amount": float(amount_in),
                    "quote": float(buy_quote),
                    "unit_price": float(buy_unit_price),
                }
            )
            signals.append(buy_signal)

            # 卖出信号
            sell_signal = Signal(
                symbol=f"{token_b}/{token_a}",
                signal_type=SignalType.SELL,
                price=float(sell_unit_price),
                timestamp=opp["timestamp"],
                strategy_name=self.name,
                strength=min(float(opp["profit_pct"]) / self.params["min_spread"], 1.0),
                metadata={
                    "dex": opp["sell_dex"],
                    "arbitrage_type": "dex_sell",
                    "dex_pair_key": self._pair_key(token_a, token_b, directional=True),
                    "profit": float(opp["profit"]),
                    "amount": float(buy_quote),
                    "quote": float(sell_quote),
                    "unit_price": float(sell_unit_price),
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
