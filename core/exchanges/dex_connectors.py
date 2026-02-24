"""
DEX连接器模块
支持Uniswap、SushiSwap、PancakeSwap等主流DEX
"""
from abc import abstractmethod
from datetime import datetime
from typing import Optional, Any, List
from decimal import Decimal
from loguru import logger
from web3 import Web3
from web3.contract import Contract

from config.exchanges import ExchangeConfig, ExchangeType
from config.settings import settings
from core.exchanges.base_exchange import (
    BaseExchange,
    Ticker,
    Kline,
    Order,
    Balance,
    Position,
    OrderSide,
    OrderType,
    OrderStatus,
)


# 常用RPC端点
RPC_ENDPOINTS = {
    "ethereum": "https://eth-mainnet.g.alchemy.com/v2/demo",
    "bsc": "https://bsc-dataseed.binance.org",
    "polygon": "https://polygon-rpc.com",
    "arbitrum": "https://arb1.arbitrum.io/rpc",
}

# 常用DEX路由合约地址
DEX_ROUTER_ADDRESSES = {
    "uniswap_v2": {
        "ethereum": "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D",
    },
    "sushiswap": {
        "ethereum": "0xd9e1cE17f2641f24aE83637ab66a2cca9C378B9F",
    },
    "pancakeswap": {
        "bsc": "0x10ED43C718714eb63d5aA57B78B54704E256024E",
    },
}

# ERC20 ABI (最小化版本)
ERC20_ABI = [
    {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "symbol", "outputs": [{"name": "", "type": "string"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "name", "outputs": [{"name": "", "type": "string"}], "type": "function"},
    {"constant": True, "inputs": [{"name": "_owner", "type": "address"}], "name": "balanceOf", "outputs": [{"name": "balance", "type": "uint256"}], "type": "function"},
    {"constant": False, "inputs": [{"name": "_spender", "type": "address"}, {"name": "_value", "type": "uint256"}], "name": "approve", "outputs": [{"name": "", "type": "bool"}], "type": "function"},
    {"constant": True, "inputs": [{"name": "_owner", "type": "address"}, {"name": "_spender", "type": "address"}], "name": "allowance", "outputs": [{"name": "", "type": "uint256"}], "type": "function"},
]

# Uniswap V2 Router ABI
UNISWAP_V2_ROUTER_ABI = [
    {"inputs": [{"internalType": "uint256", "name": "amountIn", "type": "uint256"}, {"internalType": "address[]", "name": "path", "type": "address[]"}], "name": "getAmountsOut", "outputs": [{"internalType": "uint256[]", "name": "amounts", "type": "uint256[]"}], "stateMutability": "view", "type": "function"},
    {"inputs": [{"internalType": "uint256", "name": "amountIn", "type": "uint256"}, {"internalType": "uint256", "name": "amountOutMin", "type": "uint256"}, {"internalType": "address[]", "name": "path", "type": "address[]"}, {"internalType": "address", "name": "to", "type": "address"}, {"internalType": "uint256", "name": "deadline", "type": "uint256"}], "name": "swapExactTokensForTokens", "outputs": [{"internalType": "uint256[]", "name": "amounts", "type": "uint256[]"}], "stateMutability": "nonpayable", "type": "function"},
    {"inputs": [{"internalType": "uint256", "name": "amountOutMin", "type": "uint256"}, {"internalType": "address[]", "name": "path", "type": "address[]"}, {"internalType": "address", "name": "to", "type": "address"}, {"internalType": "uint256", "name": "deadline", "type": "uint256"}], "name": "swapExactETHForTokens", "outputs": [{"internalType": "uint256[]", "name": "amounts", "type": "uint256[]"}], "stateMutability": "payable", "type": "function"},
    {"inputs": [{"internalType": "uint256", "name": "amountIn", "type": "uint256"}, {"internalType": "uint256", "name": "amountOutMin", "type": "uint256"}, {"internalType": "address[]", "name": "path", "type": "address[]"}, {"internalType": "address", "name": "to", "type": "address"}, {"internalType": "uint256", "name": "deadline", "type": "uint256"}], "name": "swapExactTokensForETH", "outputs": [{"internalType": "uint256[]", "name": "amounts", "type": "uint256[]"}], "stateMutability": "nonpayable", "type": "function"},
]


class BaseDEXConnector(BaseExchange):
    """DEX基类"""

    def __init__(self, config: ExchangeConfig, chain: str = "ethereum"):
        super().__init__(config)
        self.chain = chain
        self.w3: Optional[Web3] = None
        self.router_contract: Optional[Contract] = None
        self.router_address: Optional[str] = None

    async def connect(self) -> bool:
        """连接DEX"""
        try:
            rpc_url = RPC_ENDPOINTS.get(self.chain)
            if not rpc_url:
                raise ValueError(f"Unsupported chain: {self.chain}")

            self.w3 = Web3(Web3.HTTPProvider(rpc_url))

            if not self.w3.is_connected():
                raise ConnectionError(f"Failed to connect to {self.chain} RPC")

            self._connected = True
            logger.info(f"[{self.name}] Connected to {self.chain}")
            return True

        except Exception as e:
            self._handle_error(e, "connect")
            return False

    async def disconnect(self) -> None:
        """断开连接"""
        self._connected = False
        logger.info(f"[{self.name}] Disconnected")

    async def get_token_info(self, token_address: str) -> dict:
        """获取代币信息"""
        try:
            contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(token_address),
                abi=ERC20_ABI,
            )

            return {
                "address": token_address,
                "symbol": contract.functions.symbol().call(),
                "name": contract.functions.name().call(),
                "decimals": contract.functions.decimals().call(),
            }
        except Exception as e:
            self._handle_error(e, f"get_token_info({token_address})")

    async def get_token_balance(self, token_address: str, wallet_address: str) -> Decimal:
        """获取代币余额"""
        try:
            contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(token_address),
                abi=ERC20_ABI,
            )

            balance = contract.functions.balanceOf(
                Web3.to_checksum_address(wallet_address)
            ).call()

            decimals = contract.functions.decimals().call()
            return Decimal(balance) / Decimal(10 ** decimals)

        except Exception as e:
            self._handle_error(e, f"get_token_balance({token_address})")

    async def get_quote(
        self,
        token_in: str,
        token_out: str,
        amount_in: Decimal,
    ) -> Decimal:
        """获取兑换报价"""
        try:
            token_in_contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(token_in),
                abi=ERC20_ABI,
            )
            decimals_in = token_in_contract.functions.decimals().call()

            amount_in_wei = int(amount_in * Decimal(10 ** decimals_in))

            amounts = self.router_contract.functions.getAmountsOut(
                amount_in_wei,
                [
                    Web3.to_checksum_address(token_in),
                    Web3.to_checksum_address(token_out),
                ]
            ).call()

            token_out_contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(token_out),
                abi=ERC20_ABI,
            )
            decimals_out = token_out_contract.functions.decimals().call()

            return Decimal(amounts[-1]) / Decimal(10 ** decimals_out)

        except Exception as e:
            self._handle_error(e, f"get_quote({token_in}, {token_out})")

    async def get_ticker(self, symbol: str) -> Ticker:
        """获取行情数据（DEX实现）"""
        raise NotImplementedError("Use get_quote() for DEX price queries")

    async def get_klines(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> List[Kline]:
        """获取K线数据（DEX通常不支持）"""
        logger.warning(f"[{self.name}] DEX does not support klines directly")
        return []

    async def get_order_book(self, symbol: str, limit: int = 20) -> dict:
        """获取订单簿（DEX通常不支持）"""
        logger.warning(f"[{self.name}] DEX does not support order book directly")
        return {"bids": [], "asks": []}

    async def get_balance(self) -> List[Balance]:
        """获取账户余额"""
        raise NotImplementedError("Use get_token_balance() for DEX balance queries")

    @abstractmethod
    async def create_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        amount: float,
        price: Optional[float] = None,
        params: Optional[dict] = None,
    ) -> Order:
        """创建订单"""
        pass

    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        """取消订单（DEX不支持）"""
        logger.warning(f"[{self.name}] DEX orders cannot be cancelled once submitted")
        return False

    async def get_order(self, order_id: str, symbol: str) -> Order:
        """获取订单信息"""
        raise NotImplementedError("DEX transactions are immutable")

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """获取未完成订单（DEX不支持）"""
        return []

    async def get_positions(self) -> List[Position]:
        """获取持仓信息"""
        return []


class UniswapConnector(BaseDEXConnector):
    """Uniswap连接器"""

    def __init__(self, config: ExchangeConfig):
        super().__init__(config, chain="ethereum")
        self.router_address = DEX_ROUTER_ADDRESSES["uniswap_v2"]["ethereum"]

    async def connect(self) -> bool:
        """连接Uniswap"""
        success = await super().connect()
        if success:
            self.router_contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(self.router_address),
                abi=UNISWAP_V2_ROUTER_ABI,
            )
            self.name = "uniswap"
        return success

    async def create_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        amount: float,
        price: Optional[float] = None,
        params: Optional[dict] = None,
    ) -> Order:
        """创建交换订单"""
        # DEX交易需要私钥签名，这里提供框架
        raise NotImplementedError("DEX trading requires wallet private key")


class SushiSwapConnector(BaseDEXConnector):
    """SushiSwap连接器"""

    def __init__(self, config: ExchangeConfig):
        super().__init__(config, chain="ethereum")
        self.router_address = DEX_ROUTER_ADDRESSES["sushiswap"]["ethereum"]

    async def connect(self) -> bool:
        """连接SushiSwap"""
        success = await super().connect()
        if success:
            self.router_contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(self.router_address),
                abi=UNISWAP_V2_ROUTER_ABI,
            )
            self.name = "sushiswap"
        return success

    async def create_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        amount: float,
        price: Optional[float] = None,
        params: Optional[dict] = None,
    ) -> Order:
        """创建交换订单"""
        raise NotImplementedError("DEX trading requires wallet private key")


class PancakeSwapConnector(BaseDEXConnector):
    """PancakeSwap连接器"""

    def __init__(self, config: ExchangeConfig):
        super().__init__(config, chain="bsc")
        self.router_address = DEX_ROUTER_ADDRESSES["pancakeswap"]["bsc"]

    async def connect(self) -> bool:
        """连接PancakeSwap"""
        success = await super().connect()
        if success:
            self.router_contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(self.router_address),
                abi=UNISWAP_V2_ROUTER_ABI,
            )
            self.name = "pancakeswap"
        return success

    async def create_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        amount: float,
        price: Optional[float] = None,
        params: Optional[dict] = None,
    ) -> Order:
        """创建交换订单"""
        raise NotImplementedError("DEX trading requires wallet private key")
