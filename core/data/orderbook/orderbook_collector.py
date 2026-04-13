"""
订单簿 Level 2 采集器

从各大交易所获取订单簿深度数据，计算买卖压力、流动性等指标。
完全免费，无需 API Key。
"""
import asyncio
import aiohttp
from datetime import datetime
from typing import List, Optional, Dict, Tuple
from dataclasses import dataclass, field
from loguru import logger


@dataclass
class PriceLevel:
    """价格档位"""
    price: float
    amount: float
    
    @property
    def total(self) -> float:
        """总价值"""
        return self.price * self.amount


@dataclass
class OrderBookSnapshot:
    """
    订单簿快照
    
    Attributes:
        bids: 买单列表 [(price, amount), ...]
        asks: 卖单列表 [(price, amount), ...]
        timestamp: 快照时间
    """
    symbol: str
    exchange: str
    bids: List[PriceLevel]
    asks: List[PriceLevel]
    timestamp: datetime = field(default_factory=datetime.now)
    
    @property
    def best_bid(self) -> Optional[PriceLevel]:
        """最优买价"""
        return self.bids[0] if self.bids else None
    
    @property
    def best_ask(self) -> Optional[PriceLevel]:
        """最优卖价"""
        return self.asks[0] if self.asks else None
    
    @property
    def mid_price(self) -> Optional[float]:
        """中间价"""
        if self.best_bid and self.best_ask:
            return (self.best_bid.price + self.best_ask.price) / 2
        return None
    
    @property
    def spread(self) -> Optional[float]:
        """价差 (绝对值)"""
        if self.best_bid and self.best_ask:
            return self.best_ask.price - self.best_bid.price
        return None
    
    @property
    def spread_pct(self) -> Optional[float]:
        """价差百分比"""
        if self.spread and self.mid_price:
            return self.spread / self.mid_price
        return None
    
    def bid_depth(self, levels: int = 10) -> float:
        """买单深度 (总金额)"""
        return sum(level.total for level in self.bids[:levels])
    
    def ask_depth(self, levels: int = 10) -> float:
        """卖单深度 (总金额)"""
        return sum(level.total for level in self.asks[:levels])
    
    @property
    def depth_imbalance(self) -> Optional[float]:
        """
        深度不平衡 (-1 到 1)
        
        正值 = 买单压力大 (看涨)
        负值 = 卖单压力大 (看跌)
        """
        bid_total = self.bid_depth(20)
        ask_total = self.ask_depth(20)
        total = bid_total + ask_total
        
        if total == 0:
            return None
        return (bid_total - ask_total) / total
    
    def bid_volume_at_range(self, pct_range: float = 0.01) -> float:
        """
        指定百分比范围内的买单量
        
        Args:
            pct_range: 价格范围百分比 (默认1%)
        """
        if not self.best_bid:
            return 0
        
        price_limit = self.best_bid.price * (1 - pct_range)
        return sum(
            level.amount 
            for level in self.bids 
            if level.price >= price_limit
        )
    
    def ask_volume_at_range(self, pct_range: float = 0.01) -> float:
        """
        指定百分比范围内的卖单量
        
        Args:
            pct_range: 价格范围百分比 (默认1%)
        """
        if not self.best_ask:
            return 0
        
        price_limit = self.best_ask.price * (1 + pct_range)
        return sum(
            level.amount 
            for level in self.asks 
            if level.price <= price_limit
        )
    
    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "timestamp": self.timestamp.isoformat(),
            "best_bid": self.best_bid.price if self.best_bid else None,
            "best_ask": self.best_ask.price if self.best_ask else None,
            "spread": self.spread,
            "spread_pct": self.spread_pct,
            "mid_price": self.mid_price,
            "depth_imbalance": self.depth_imbalance,
            "bid_depth_10": self.bid_depth(10),
            "ask_depth_10": self.ask_depth(10),
        }


class OrderBookCollector:
    """
    订单簿采集器
    
    Example:
        collector = OrderBookCollector()
        
        # 获取订单簿
        ob = await collector.fetch_binance("BTCUSDT", limit=20)
        print(f"Spread: {ob.spread_pct*100:.2f}%")
        print(f"Imbalance: {ob.depth_imbalance:.2f}")
    """
    
    def __init__(self, timeout: int = 10):
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: Optional[aiohttp.ClientSession] = None
        
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session
    
    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
            
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
    
    @staticmethod
    def _parse_levels(data: List, is_bid: bool = True) -> List[PriceLevel]:
        """解析价格档位"""
        levels = []
        for item in data:
            if len(item) >= 2:
                price = float(item[0])
                amount = float(item[1])
                levels.append(PriceLevel(price=price, amount=amount))
        
        # 买单降序，卖单升序
        levels.sort(key=lambda x: x.price, reverse=is_bid)
        return levels
    
    # ============================================================
    # Binance
    # ============================================================
    async def fetch_binance(
        self, 
        symbol: str, 
        limit: int = 100
    ) -> Optional[OrderBookSnapshot]:
        """
        获取 Binance 订单簿
        
        Args:
            symbol: 交易对 (如 BTCUSDT)
            limit: 深度档位 (5, 10, 20, 50, 100, 500, 1000, 5000)
        """
        session = await self._get_session()
        
        try:
            url = "https://fapi.binance.com/fapi/v1/depth"
            params = {"symbol": symbol, "limit": limit}
            
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    return None
                
                data = await resp.json()
                
                return OrderBookSnapshot(
                    symbol=symbol,
                    exchange="binance",
                    bids=self._parse_levels(data.get("bids", []), is_bid=True),
                    asks=self._parse_levels(data.get("asks", []), is_bid=False),
                    timestamp=datetime.now(),
                )
                
        except Exception as e:
            logger.error(f"Binance orderbook fetch error: {e}")
            return None
    
    # ============================================================
    # Bybit
    # ============================================================
    async def fetch_bybit(
        self, 
        symbol: str, 
        limit: int = 100
    ) -> Optional[OrderBookSnapshot]:
        """获取 Bybit 订单簿"""
        session = await self._get_session()
        
        try:
            url = "https://api.bybit.com/v5/market/orderbook"
            params = {
                "category": "linear",
                "symbol": symbol,
                "limit": limit
            }
            
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    return None
                
                data = await resp.json()
                
                if data.get("retCode") != 0:
                    return None
                
                result = data.get("result", {})
                
                # Bybit 返回格式不同
                bids = [
                    [item["price"], item["size"]]
                    for item in result.get("b", [])
                ]
                asks = [
                    [item["price"], item["size"]]
                    for item in result.get("a", [])
                ]
                
                return OrderBookSnapshot(
                    symbol=symbol,
                    exchange="bybit",
                    bids=self._parse_levels(bids, is_bid=True),
                    asks=self._parse_levels(asks, is_bid=False),
                    timestamp=datetime.now(),
                )
                
        except Exception as e:
            logger.error(f"Bybit orderbook fetch error: {e}")
            return None
    
    # ============================================================
    # OKX
    # ============================================================
    async def fetch_okx(
        self, 
        instId: str, 
        depth: int = 100
    ) -> Optional[OrderBookSnapshot]:
        """获取 OKX 订单簿"""
        session = await self._get_session()
        
        try:
            url = "https://www.okx.com/api/v5/market/books"
            params = {"instId": instId, "sz": depth}
            
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    return None
                
                data = await resp.json()
                
                if not data.get("data"):
                    return None
                
                book = data["data"][0]
                
                # OKX 格式: [price, size, _, _]
                bids = [[item[0], item[1]] for item in book.get("bids", [])]
                asks = [[item[0], item[1]] for item in book.get("asks", [])]
                
                return OrderBookSnapshot(
                    symbol=instId,
                    exchange="okx",
                    bids=self._parse_levels(bids, is_bid=True),
                    asks=self._parse_levels(asks, is_bid=False),
                    timestamp=datetime.now(),
                )
                
        except Exception as e:
            logger.error(f"OKX orderbook fetch error: {e}")
            return None
    
    # ============================================================
    # Gate.io
    # ============================================================
    async def fetch_gate(
        self, 
        contract: str, 
        limit: int = 100
    ) -> Optional[OrderBookSnapshot]:
        """获取 Gate.io 订单簿"""
        session = await self._get_session()
        
        try:
            url = f"https://api.gateio.io/api/v4/futures/usdt/order_book"
            params = {"contract": contract, "limit": limit}
            
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    return None
                
                data = await resp.json()
                
                bids = data.get("bids", [])
                asks = data.get("asks", [])
                
                return OrderBookSnapshot(
                    symbol=contract,
                    exchange="gate",
                    bids=self._parse_levels(bids, is_bid=True),
                    asks=self._parse_levels(asks, is_bid=False),
                    timestamp=datetime.now(),
                )
                
        except Exception as e:
            logger.error(f"Gate orderbook fetch error: {e}")
            return None
    
    # ============================================================
    # 便捷方法
    # ============================================================
    async def fetch_all(
        self, 
        symbol: str, 
        limit: int = 50
    ) -> Dict[str, OrderBookSnapshot]:
        """
        从所有交易所获取订单簿
        
        Args:
            symbol: 交易对 (如 BTCUSDT)
            limit: 深度档位
            
        Returns:
            {exchange: OrderBookSnapshot}
        """
        results = {}
        
        # 并行获取
        tasks = [
            ("binance", self.fetch_binance(symbol, limit)),
            ("bybit", self.fetch_bybit(symbol, limit)),
        ]
        
        # OKX 使用不同的交易对格式
        okx_symbol = symbol.replace("USDT", "-USDT-SWAP")
        tasks.append(("okx", self.fetch_okx(okx_symbol, limit)))
        
        # Gate.io 格式
        gate_symbol = symbol.replace("USDT", "_USDT")
        tasks.append(("gate", self.fetch_gate(gate_symbol, limit)))
        
        for name, task in tasks:
            try:
                ob = await task
                if ob:
                    results[name] = ob
            except Exception:
                pass
                
        return results
    
    def compare_spreads(
        self, 
        snapshots: Dict[str, OrderBookSnapshot]
    ) -> List[Dict]:
        """
        比较各交易所的价差
        
        Returns:
            按价差排序的列表
        """
        results = []
        
        for exchange, ob in snapshots.items():
            if ob.spread_pct is not None:
                results.append({
                    "exchange": exchange,
                    "spread_pct": ob.spread_pct,
                    "best_bid": ob.best_bid.price if ob.best_bid else None,
                    "best_ask": ob.best_ask.price if ob.best_ask else None,
                    "imbalance": ob.depth_imbalance,
                })
        
        # 按价差升序排列 (最小价差在前)
        results.sort(key=lambda x: x["spread_pct"])
        return results


# 全局实例
orderbook_collector = OrderBookCollector()


# ==================== 快速测试 ====================

async def _test_collector():
    """测试采集器"""
    async with OrderBookCollector() as collector:
        print("=" * 60)
        print("Testing Binance OrderBook")
        print("=" * 60)
        
        ob = await collector.fetch_binance("BTCUSDT", limit=20)
        if ob:
            print(f"Best Bid: ${ob.best_bid.price:,.2f}")
            print(f"Best Ask: ${ob.best_ask.price:,.2f}")
            print(f"Spread:   {ob.spread_pct*100:.4f}%")
            print(f"Mid:      ${ob.mid_price:,.2f}")
            print(f"Imbalance: {ob.depth_imbalance:+.3f}")
            print(f"Bid Depth (10): ${ob.bid_depth(10):,.0f}")
            print(f"Ask Depth (10): ${ob.ask_depth(10):,.0f}")
        else:
            print("Failed to fetch")


if __name__ == "__main__":
    asyncio.run(_test_collector())