"""
未平仓合约 (Open Interest) 采集器

从各大交易所获取 OI 数据，用于分析市场情绪和潜在波动。
完全免费，无需 API Key。
"""
import asyncio
import aiohttp
from datetime import datetime
from typing import List, Optional, Dict
from dataclasses import dataclass, field
from loguru import logger


@dataclass
class OpenInterest:
    """
    未平仓合约数据
    
    Attributes:
        value: OI 价值 (USDT)
        volume: OI 数量 (张/币)
        timestamp: 数据时间
    """
    symbol: str
    exchange: str
    value: float  # USDT
    volume: float  # 数量
    timestamp: datetime = field(default_factory=datetime.now)
    
    @property
    def value_millions(self) -> float:
        """以百万计的 OI 价值"""
        return self.value / 1_000_000
    
    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "value": self.value,
            "value_millions": self.value_millions,
            "volume": self.volume,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class OIHistory:
    """OI 历史数据"""
    symbol: str
    exchange: str
    data: List[Dict]  # [{timestamp, value, volume}, ...]
    
    @property
    def latest(self) -> Optional[float]:
        """最新 OI 价值"""
        return self.data[0]["value"] if self.data else None
    
    @property
    def change_24h(self) -> Optional[float]:
        """24小时变化率"""
        if len(self.data) < 2:
            return None
        current = self.data[0]["value"]
        prev = self.data[-1]["value"] if len(self.data) >= 24 else self.data[-1]["value"]
        if prev == 0:
            return None
        return (current - prev) / prev
    
    @property
    def change_24h_pct(self) -> Optional[float]:
        """24小时变化百分比"""
        change = self.change_24h
        return change * 100 if change is not None else None


class OICollector:
    """
    未平仓合约采集器
    
    Example:
        collector = OICollector()
        
        # 获取当前 OI
        oi = await collector.fetch_binance("BTCUSDT")
        print(f"OI: ${oi.value_millions:.1f}M")
        
        # 获取历史
        history = await collector.fetch_binance_history("BTCUSDT")
        print(f"24h Change: {history.change_24h_pct:.2f}%")
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
    
    # ============================================================
    # Binance
    # ============================================================
    async def fetch_binance(self, symbol: str) -> Optional[OpenInterest]:
        """获取 Binance 当前 OI"""
        session = await self._get_session()
        
        try:
            url = "https://fapi.binance.com/fapi/v1/openInterest"
            params = {"symbol": symbol}
            
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    return None
                
                data = await resp.json()
                
                return OpenInterest(
                    symbol=symbol,
                    exchange="binance",
                    value=float(data.get("notionalValue", 0)),
                    volume=float(data.get("openInterest", 0)),
                    timestamp=datetime.now(),
                )
                
        except Exception as e:
            logger.error(f"Binance OI fetch error: {e}")
            return None
    
    async def fetch_binance_history(
        self, 
        symbol: str, 
        limit: int = 30
    ) -> Optional[OIHistory]:
        """获取 Binance OI 历史"""
        session = await self._get_session()
        
        try:
            url = "https://fapi.binance.com/fapi/v1/openInterestHist"
            params = {
                "symbol": symbol,
                "period": "5m",
                "limit": limit
            }
            
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    return None
                
                data = await resp.json()
                
                history = []
                for item in data:
                    history.append({
                        "timestamp": datetime.fromtimestamp(item["timestamp"] / 1000),
                        "value": float(item["sumOpenInterestValue"]),
                        "volume": float(item["sumOpenInterest"]),
                    })
                
                # 按时间降序排列
                history.sort(key=lambda x: x["timestamp"], reverse=True)
                
                return OIHistory(
                    symbol=symbol,
                    exchange="binance",
                    data=history,
                )
                
        except Exception as e:
            logger.error(f"Binance OI history fetch error: {e}")
            return None
    
    # ============================================================
    # Bybit
    # ============================================================
    async def fetch_bybit(self, symbol: str) -> Optional[OpenInterest]:
        """获取 Bybit 当前 OI"""
        session = await self._get_session()
        
        try:
            url = "https://api.bybit.com/v5/market/open-interest"
            params = {
                "category": "linear",
                "symbol": symbol,
            }
            
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    return None
                
                data = await resp.json()
                
                if data.get("retCode") != 0:
                    return None
                
                result = data.get("result", {})
                
                return OpenInterest(
                    symbol=symbol,
                    exchange="bybit",
                    value=float(result.get("openInterestValue", 0)),
                    volume=float(result.get("openInterest", 0)),
                    timestamp=datetime.now(),
                )
                
        except Exception as e:
            logger.error(f"Bybit OI fetch error: {e}")
            return None
    
    # ============================================================
    # OKX
    # ============================================================
    async def fetch_okx(self, instId: str) -> Optional[OpenInterest]:
        """获取 OKX 当前 OI"""
        session = await self._get_session()
        
        try:
            url = "https://www.okx.com/api/v5/public/open-interest"
            params = {"instId": instId}
            
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    return None
                
                data = await resp.json()
                
                if not data.get("data"):
                    return None
                
                item = data["data"][0]
                
                return OpenInterest(
                    symbol=instId,
                    exchange="okx",
                    value=float(item.get("oiUsd", 0)),
                    volume=float(item.get("oi", 0)),
                    timestamp=datetime.now(),
                )
                
        except Exception as e:
            logger.error(f"OKX OI fetch error: {e}")
            return None
    
    # ============================================================
    # Gate.io
    # ============================================================
    async def fetch_gate(self, contract: str) -> Optional[OpenInterest]:
        """获取 Gate.io 当前 OI"""
        session = await self._get_session()
        
        try:
            url = f"https://api.gateio.io/api/v4/futures/usdt/contracts/{contract}"
            
            async with session.get(url) as resp:
                if resp.status != 200:
                    return None
                
                data = await resp.json()
                
                return OpenInterest(
                    symbol=contract,
                    exchange="gate",
                    value=float(data.get("position_size", 0)) * float(data.get("last_price", 1)),
                    volume=float(data.get("position_size", 0)),
                    timestamp=datetime.now(),
                )
                
        except Exception as e:
            logger.error(f"Gate OI fetch error: {e}")
            return None
    
    # ============================================================
    # 便捷方法
    # ============================================================
    async def fetch_all(self, symbol: str) -> Dict[str, OpenInterest]:
        """
        从所有交易所获取 OI
        
        Args:
            symbol: 交易对 (如 BTCUSDT)
            
        Returns:
            {exchange: OpenInterest}
        """
        results = {}
        
        tasks = [
            ("binance", self.fetch_binance(symbol)),
            ("bybit", self.fetch_bybit(symbol)),
        ]
        
        # OKX 格式
        okx_symbol = symbol.replace("USDT", "-USDT-SWAP")
        tasks.append(("okx", self.fetch_okx(okx_symbol)))
        
        # Gate.io 格式
        gate_symbol = symbol.replace("USDT", "_USDT")
        tasks.append(("gate", self.fetch_gate(gate_symbol)))
        
        for name, task in tasks:
            try:
                oi = await task
                if oi:
                    results[name] = oi
            except Exception:
                pass
                
        return results
    
    async def compare_oi(self, symbol: str) -> List[Dict]:
        """
        比较各交易所的 OI
        
        Returns:
            按 OI 价值排序的列表
        """
        all_oi = await self.fetch_all(symbol)
        
        results = []
        for exchange, oi in all_oi.items():
            results.append({
                "exchange": exchange,
                "value": oi.value,
                "value_millions": oi.value_millions,
                "volume": oi.volume,
            })
        
        # 按 OI 价值降序排列
        results.sort(key=lambda x: x["value"], reverse=True)
        return results


# 全局实例
oi_collector = OICollector()


# ==================== 快速测试 ====================

async def _test_collector():
    """测试采集器"""
    async with OICollector() as collector:
        print("=" * 60)
        print("Testing OI Collector")
        print("=" * 60)
        
        # 测试当前 OI
        print("\n[Current OI - BTCUSDT]")
        oi = await collector.fetch_binance("BTCUSDT")
        if oi:
            print(f"  Value: ${oi.value_millions:.1f}M")
            print(f"  Volume: {oi.volume:,.0f}")
        else:
            print("  Failed to fetch")
        
        # 测试历史
        print("\n[OI History - BTCUSDT]")
        history = await collector.fetch_binance_history("BTCUSDT", 30)
        if history:
            print(f"  Latest: ${history.latest/1e6:.1f}M")
            print(f"  24h Change: {history.change_24h_pct:.2f}%")
        else:
            print("  Failed to fetch")


if __name__ == "__main__":
    asyncio.run(_test_collector())