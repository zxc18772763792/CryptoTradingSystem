"""
交易所配置模块
"""
from dataclasses import dataclass, field
from typing import Optional, List, Dict
from enum import Enum


class ExchangeType(Enum):
    """交易所类型"""
    CEX = "cex"  # 中心化交易所
    DEX = "dex"  # 去中心化交易所


class ExchangeName(Enum):
    """交易所名称"""
    BINANCE = "binance"
    OKX = "okx"
    GATE = "gate"
    BYBIT = "bybit"
    UNISWAP = "uniswap"
    SUSHISWAP = "sushiswap"
    PANCAKESWAP = "pancakeswap"


@dataclass
class ExchangeConfig:
    """交易所配置"""
    name: str
    exchange_type: ExchangeType
    api_key: str = ""
    api_secret: str = ""
    passphrase: Optional[str] = None  # OKX需要
    sandbox: bool = False  # 测试网模式
    rate_limit: int = 1200  # 请求限制（毫秒）
    timeout: int = 30000  # 超时时间（毫秒）
    enable_rate_limit: bool = True
    default_type: str = "spot"  # spot, future, margin
    proxy: Optional[str] = None

    # 支持的交易对
    supported_symbols: List[str] = field(default_factory=list)

    # 支持的时间框架
    supported_timeframes: List[str] = field(default_factory=lambda: [
        "1m", "3m", "5m", "15m", "30m",
        "1h", "2h", "4h", "6h", "12h",
        "1d", "3d", "1w", "1M"
    ])


# 预定义的交易所配置
EXCHANGE_CONFIGS: Dict[str, ExchangeConfig] = {
    "binance": ExchangeConfig(
        name="binance",
        exchange_type=ExchangeType.CEX,
        default_type="spot",
        rate_limit=1200,
        supported_symbols=[
            "BTC/USDT", "ETH/USDT", "BNB/USDT",
            "SOL/USDT", "XRP/USDT", "ADA/USDT",
            "DOGE/USDT", "AVAX/USDT", "DOT/USDT",
            "MATIC/USDT", "LINK/USDT", "UNI/USDT"
        ]
    ),
    "okx": ExchangeConfig(
        name="okx",
        exchange_type=ExchangeType.CEX,
        default_type="spot",
        rate_limit=1000,
        supported_symbols=[
            "BTC/USDT", "ETH/USDT", "SOL/USDT",
            "XRP/USDT", "DOGE/USDT", "ADA/USDT",
            "AVAX/USDT", "DOT/USDT", "MATIC/USDT"
        ]
    ),
    "gate": ExchangeConfig(
        name="gate",
        exchange_type=ExchangeType.CEX,
        default_type="spot",
        rate_limit=100,
        supported_symbols=[
            "BTC/USDT", "ETH/USDT", "SOL/USDT",
            "XRP/USDT", "DOGE/USDT"
        ]
    ),
    "bybit": ExchangeConfig(
        name="bybit",
        exchange_type=ExchangeType.CEX,
        default_type="spot",
        rate_limit=1200,
        supported_symbols=[
            "BTC/USDT", "ETH/USDT", "SOL/USDT",
            "XRP/USDT", "ADA/USDT", "DOGE/USDT"
        ]
    ),
}


def get_exchange_config(exchange_name: str) -> Optional[ExchangeConfig]:
    """获取交易所配置"""
    return EXCHANGE_CONFIGS.get(exchange_name.lower())
