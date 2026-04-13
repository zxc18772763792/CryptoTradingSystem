"""
璧勯噾璐圭巼閲囬泦鍣?

鏀寔浠?Binance, Bybit, OKX, Gate 鍥涘ぇ浜ゆ槗鎵€閲囬泦璧勯噾璐圭巼鏁版嵁銆?
瀹屽叏鍏嶈垂锛屾棤闇€ API Key銆?
"""
import asyncio
import aiohttp
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from loguru import logger

from core.data.funding_rate_models import FundingRate, normalize_symbol


class FundingRateCollector:
    """
    璧勯噾璐圭巼閲囬泦鍣?
    
    鏀寔浠庡涓氦鏄撴墍骞惰閲囬泦璧勯噾璐圭巼鏁版嵁銆?
    
    Example:
        collector = FundingRateCollector()
        
        # 鑾峰彇鍗曚釜浜ゆ槗鎵€
        rate = await collector.fetch_binance("BTCUSDT")
        
        # 鑾峰彇鎵€鏈変氦鏄撴墍
        rates = await collector.fetch_all("BTCUSDT")
        
        # 鍚姩瀹氭椂閲囬泦
        await collector.start_collection(["BTCUSDT", "ETHUSDT"], interval=60)
    """
    
    # API 绔偣
    BINANCE_URL = "https://fapi.binance.com/fapi/v1/fundingRate"
    BYBIT_URL = "https://api.bybit.com/v5/market/funding/history"
    OKX_URL = "https://www.okx.com/api/v5/public/funding-rate"
    GATE_URL = "https://api.gateio.ws/api/v4/futures/usdt/funding_rate"
    
    # 棰勬祴璐圭巼绔偣
    BINANCE_PREMIUM_INDEX_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"
    
    def __init__(self, timeout: int = 10):
        """
        鍒濆鍖栭噰闆嗗櫒
        
        Args:
            timeout: HTTP 璇锋眰瓒呮椂鏃堕棿 (绉?
        """
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._collected_data: Dict[str, List[FundingRate]] = {}
        
    async def _get_session(self) -> aiohttp.ClientSession:
        """鑾峰彇鎴栧垱寤?HTTP session"""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout, trust_env=True)
        return self._session
    
    async def close(self):
        """鍏抽棴 session"""
        if self._session and not self._session.closed:
            await self._session.close()
            
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        
    # ==================== Binance ====================
    
    async def fetch_binance(
        self, 
        symbol: str, 
        limit: int = 1
    ) -> Optional[FundingRate]:
        """
        浠?Binance 鑾峰彇璧勯噾璐圭巼
        
        Args:
            symbol: 浜ゆ槗瀵?(濡?BTCUSDT)
            limit: 杩斿洖璁板綍鏁?
            
        Returns:
            FundingRate 鎴?None
        """
        session = await self._get_session()
        symbol = normalize_symbol(symbol, "binance")
        
        try:
            params = {"symbol": symbol, "limit": limit}
            async with session.get(self.BINANCE_URL, params=params) as resp:
                if resp.status != 200:
                    logger.warning(f"Binance funding rate API returned {resp.status}")
                    return None
                    
                data = await resp.json()
                
                if not data:
                    return None
                    
                # 鍙栨渶鏂颁竴鏉?
                latest = data[-1] if isinstance(data, list) else data
                
                return FundingRate(
                    exchange="binance",
                    symbol=symbol,
                    funding_rate=float(latest["fundingRate"]),
                    funding_time=datetime.fromtimestamp(latest["fundingTime"] / 1000),
                    timestamp=datetime.now(),
                )
                
        except (aiohttp.ClientError, asyncio.TimeoutError, TimeoutError) as e:
            logger.error(f"Binance funding rate fetch error: {e}")
            return None
        except (KeyError, ValueError) as e:
            logger.error(f"Binance funding rate parse error: {e}")
            return None
            
    async def fetch_binance_history(
        self,
        symbol: str,
        limit: int = 100
    ) -> List[FundingRate]:
        """
        浠?Binance 鑾峰彇鍘嗗彶璧勯噾璐圭巼
        
        Args:
            symbol: 浜ゆ槗瀵?
            limit: 杩斿洖璁板綍鏁?(鏈€澶?1000)
            
        Returns:
            FundingRate 鍒楄〃
        """
        session = await self._get_session()
        symbol = normalize_symbol(symbol, "binance")
        
        try:
            params = {"symbol": symbol, "limit": min(limit, 1000)}
            async with session.get(self.BINANCE_URL, params=params) as resp:
                if resp.status != 200:
                    return []
                    
                data = await resp.json()
                
                rates = []
                for item in data:
                    rates.append(FundingRate(
                        exchange="binance",
                        symbol=symbol,
                        funding_rate=float(item["fundingRate"]),
                        funding_time=datetime.fromtimestamp(item["fundingTime"] / 1000),
                        timestamp=datetime.now(),
                    ))
                    
                return rates
                
        except Exception as e:
            logger.error(f"Binance funding rate history fetch error: {e}")
            return []
            
    async def fetch_binance_predicted(
        self,
        symbol: str
    ) -> Optional[Dict]:
        """
        浠?Binance 鑾峰彇棰勬祴璧勯噾璐圭巼
        
        Returns:
            鍖呭惈 mark_price, index_price, estimated_settle_price 鐨勫瓧鍏?
        """
        session = await self._get_session()
        symbol = normalize_symbol(symbol, "binance")
        
        try:
            params = {"symbol": symbol}
            async with session.get(self.BINANCE_PREMIUM_INDEX_URL, params=params) as resp:
                if resp.status != 200:
                    return None
                    
                data = await resp.json()
                
                if isinstance(data, list):
                    data = data[0] if data else {}
                    
                return {
                    "symbol": symbol,
                    "mark_price": float(data.get("markPrice", 0)),
                    "index_price": float(data.get("indexPrice", 0)),
                    "estimated_settle_price": float(data.get("estimatedSettlePrice", 0)),
                    "last_funding_rate": float(data.get("lastFundingRate", 0)),
                    "next_funding_time": datetime.fromtimestamp(data.get("nextFundingTime", 0) / 1000),
                    "timestamp": datetime.now(),
                }
                
        except Exception as e:
            logger.error(f"Binance predicted funding rate fetch error: {e}")
            return None

    # ==================== Bybit ====================
    
    async def fetch_bybit(
        self,
        symbol: str,
        limit: int = 1
    ) -> Optional[FundingRate]:
        """
        浠?Bybit 鑾峰彇璧勯噾璐圭巼
        
        Args:
            symbol: 浜ゆ槗瀵?(濡?BTCUSDT)
            limit: 杩斿洖璁板綍鏁?
            
        Returns:
            FundingRate 鎴?None
        """
        session = await self._get_session()
        symbol = normalize_symbol(symbol, "bybit")
        
        try:
            params = {
                "category": "linear",
                "symbol": symbol,
                "limit": limit
            }
            async with session.get(self.BYBIT_URL, params=params) as resp:
                if resp.status != 200:
                    logger.warning(f"Bybit funding rate API returned {resp.status}")
                    return None
                    
                data = await resp.json()
                
                # 妫€鏌ュ搷搴?
                if data.get("retCode") != 0:
                    logger.warning(f"Bybit API error: {data.get('retMsg')}")
                    return None
                    
                list_data = data.get("result", {}).get("list", [])
                if not list_data:
                    return None
                    
                latest = list_data[0]
                
                return FundingRate(
                    exchange="bybit",
                    symbol=symbol,
                    funding_rate=float(latest["fundingRate"]),
                    funding_time=datetime.fromtimestamp(int(latest["fundingRateTimestamp"]) / 1000),
                    timestamp=datetime.now(),
                )
                
        except (aiohttp.ClientError, asyncio.TimeoutError, TimeoutError) as e:
            logger.error(f"Bybit funding rate fetch error: {e}")
            return None
        except (KeyError, ValueError) as e:
            logger.error(f"Bybit funding rate parse error: {e}")
            return None
            
    async def fetch_bybit_history(
        self,
        symbol: str,
        limit: int = 200
    ) -> List[FundingRate]:
        """
        浠?Bybit 鑾峰彇鍘嗗彶璧勯噾璐圭巼
        
        Args:
            symbol: 浜ゆ槗瀵?
            limit: 杩斿洖璁板綍鏁?(鏈€澶?200)
            
        Returns:
            FundingRate 鍒楄〃
        """
        session = await self._get_session()
        symbol = normalize_symbol(symbol, "bybit")
        
        try:
            params = {
                "category": "linear",
                "symbol": symbol,
                "limit": min(limit, 200)
            }
            async with session.get(self.BYBIT_URL, params=params) as resp:
                if resp.status != 200:
                    return []
                    
                data = await resp.json()
                
                if data.get("retCode") != 0:
                    return []
                    
                list_data = data.get("result", {}).get("list", [])
                
                rates = []
                for item in list_data:
                    rates.append(FundingRate(
                        exchange="bybit",
                        symbol=symbol,
                        funding_rate=float(item["fundingRate"]),
                        funding_time=datetime.fromtimestamp(int(item["fundingRateTimestamp"]) / 1000),
                        timestamp=datetime.now(),
                    ))
                    
                return rates
                
        except Exception as e:
            logger.error(f"Bybit funding rate history fetch error: {e}")
            return []

    # ==================== OKX ====================
    
    async def fetch_okx(
        self,
        symbol: str
    ) -> Optional[FundingRate]:
        """
        浠?OKX 鑾峰彇璧勯噾璐圭巼
        
        Args:
            symbol: 浜ゆ槗瀵?(濡?BTCUSDT 鎴?BTC-USDT-SWAP)
            
        Returns:
            FundingRate 鎴?None
        """
        session = await self._get_session()
        symbol = normalize_symbol(symbol, "okx")
        
        try:
            params = {"instId": symbol}
            async with session.get(self.OKX_URL, params=params) as resp:
                if resp.status != 200:
                    logger.warning(f"OKX funding rate API returned {resp.status}")
                    return None
                    
                data = await resp.json()
                
                # 妫€鏌ュ搷搴?
                if data.get("code") != "0":
                    logger.warning(f"OKX API error: {data.get('msg')}")
                    return None
                    
                list_data = data.get("data", [])
                if not list_data:
                    return None
                    
                latest = list_data[0]
                
                return FundingRate(
                    exchange="okx",
                    symbol=symbol,
                    funding_rate=float(latest["fundingRate"]),
                    funding_time=datetime.fromtimestamp(int(latest["fundingTime"]) / 1000),
                    timestamp=datetime.now(),
                    mark_price=float(latest.get("markPx", 0)) or None,
                    index_price=float(latest.get("idxPx", 0)) or None,
                )
                
        except (aiohttp.ClientError, asyncio.TimeoutError, TimeoutError) as e:
            logger.error(f"OKX funding rate fetch error: {e}")
            return None
        except (KeyError, ValueError) as e:
            logger.error(f"OKX funding rate parse error: {e}")
            return None

    # ==================== Gate ====================
    
    async def fetch_gate(
        self,
        symbol: str
    ) -> Optional[FundingRate]:
        """
        浠?Gate 鑾峰彇璧勯噾璐圭巼
        
        Args:
            symbol: 浜ゆ槗瀵?(濡?BTCUSDT 鎴?BTC_USDT)
            
        Returns:
            FundingRate 鎴?None
        """
        session = await self._get_session()
        symbol = normalize_symbol(symbol, "gate")
        
        try:
            params = {"contract": symbol}
            async with session.get(self.GATE_URL, params=params) as resp:
                if resp.status != 200:
                    logger.warning(f"Gate funding rate API returned {resp.status}")
                    return None
                    
                data = await resp.json()
                
                if not data:
                    return None

                latest = data[0] if isinstance(data, list) else data
                if not isinstance(latest, dict):
                    return None

                funding_rate = latest.get("funding_rate")
                if funding_rate is None:
                    funding_rate = latest.get("r")
                if funding_rate is None:
                    funding_rate = latest.get("funding_rate_indicative", 0)

                funding_ts = latest.get("t") or latest.get("fundingTime") or latest.get("funding_time") or 0
                funding_ts = float(funding_ts or 0)
                if funding_ts > 1e12:
                    funding_ts = funding_ts / 1000.0

                return FundingRate(
                    exchange="gate",
                    symbol=symbol,
                    funding_rate=float(funding_rate),
                    funding_time=datetime.fromtimestamp(funding_ts),
                    timestamp=datetime.now(),
                    estimated_rate=float(latest.get("funding_rate_indicative", funding_rate or 0)) or None,
                )
                
        except (aiohttp.ClientError, asyncio.TimeoutError, TimeoutError) as e:
            logger.error(f"Gate funding rate fetch error: {e}")
            return None
        except (KeyError, ValueError) as e:
            logger.error(f"Gate funding rate parse error: {e}")
            return None

    # ==================== 缁煎悎鏂规硶 ====================
    
    async def fetch_all(
        self,
        symbol: str
    ) -> Dict[str, FundingRate]:
        """
        浠庢墍鏈変氦鏄撴墍骞惰鑾峰彇璧勯噾璐圭巼
        
        Args:
            symbol: 浜ゆ槗瀵?(鑷姩杞崲鏍煎紡)
            
        Returns:
            Dict[exchange_name, FundingRate]
        """
        tasks = {
            "binance": self.fetch_binance(symbol),
            "bybit": self.fetch_bybit(symbol),
            "okx": self.fetch_okx(symbol),
            "gate": self.fetch_gate(symbol),
        }
        
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        
        output = {}
        for (exchange, _), result in zip(tasks.items(), results):
            if isinstance(result, Exception):
                logger.error(f"{exchange} fetch exception: {result}")
            elif result is not None:
                output[exchange] = result
                
        return output
        
    async def fetch_all_with_predicted(
        self,
        symbol: str
    ) -> Dict:
        """
        鑾峰彇鎵€鏈変氦鏄撴墍鐨勮祫閲戣垂鐜囷紝鍖呮嫭棰勬祴璐圭巼
        
        Returns:
            {
                "rates": Dict[str, FundingRate],
                "predicted": Dict (浠?Binance)
            }
        """
        rates = await self.fetch_all(symbol)
        predicted = await self.fetch_binance_predicted(symbol)
        
        return {
            "rates": rates,
            "predicted": predicted,
            "symbol": symbol,
            "timestamp": datetime.now(),
        }
        
    async def fetch_history_all(
        self,
        symbol: str,
        limit: int = 100
    ) -> Dict[str, List[FundingRate]]:
        """
        浠庢墍鏈夋敮鎸佸巻鍙叉煡璇㈢殑浜ゆ槗鎵€鑾峰彇鍘嗗彶璧勯噾璐圭巼
        
        Args:
            symbol: 浜ゆ槗瀵?
            limit: 姣忎釜浜ゆ槗鎵€杩斿洖鐨勮褰曟暟
            
        Returns:
            Dict[exchange_name, List[FundingRate]]
        """
        tasks = {
            "binance": self.fetch_binance_history(symbol, limit),
            "bybit": self.fetch_bybit_history(symbol, limit),
        }
        
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        
        output = {}
        for (exchange, _), result in zip(tasks.items(), results):
            if isinstance(result, Exception):
                logger.error(f"{exchange} history fetch exception: {result}")
            elif result:
                output[exchange] = result
                
        return output
        
    # ==================== 瀹氭椂閲囬泦 ====================
    
    async def start_collection(
        self,
        symbols: List[str],
        interval: int = 60,
        callback: Optional[callable] = None
    ):
        """
        鍚姩瀹氭椂閲囬泦
        
        Args:
            symbols: 瑕佺洃鎺х殑浜ゆ槗瀵瑰垪琛?
            interval: 閲囬泦闂撮殧 (绉?
            callback: 鏁版嵁鍥炶皟鍑芥暟 async callback(symbol, rates)
        """
        self._running = True
        logger.info(f"Starting funding rate collection for {symbols} every {interval}s")
        
        while self._running:
            try:
                for symbol in symbols:
                    rates = await self.fetch_all(symbol)
                    
                    # 瀛樺偍鏁版嵁
                    if symbol not in self._collected_data:
                        self._collected_data[symbol] = []
                    
                    for exchange, rate in rates.items():
                        self._collected_data[symbol].append(rate)
                    
                    # 瑙﹀彂鍥炶皟
                    if callback:
                        try:
                            await callback(symbol, rates)
                        except Exception as e:
                            logger.error(f"Callback error: {e}")
                            
                await asyncio.sleep(interval)
                
            except Exception as e:
                logger.error(f"Collection loop error: {e}")
                await asyncio.sleep(5)
                
    def stop_collection(self):
        """鍋滄瀹氭椂閲囬泦"""
        self._running = False
        logger.info("Funding rate collection stopped")
        
    def get_collected_data(self, symbol: str) -> List[FundingRate]:
        """鑾峰彇宸查噰闆嗙殑鏁版嵁"""
        return self._collected_data.get(symbol, [])
        
    def clear_collected_data(self, symbol: Optional[str] = None):
        """娓呴櫎宸查噰闆嗙殑鏁版嵁"""
        if symbol:
            self._collected_data[symbol] = []
        else:
            self._collected_data.clear()
            
    @property
    def is_running(self) -> bool:
        """鏄惁姝ｅ湪杩愯"""
        return self._running


# 鍏ㄥ眬瀹炰緥
funding_rate_collector = FundingRateCollector()


# ==================== 蹇€熸祴璇?====================

async def _test_collector():
    """Quick manual test."""
    async with FundingRateCollector() as collector:
        # 娴嬭瘯鍗曚釜浜ゆ槗鎵€
        print("=" * 50)
        print("Testing individual exchanges...")
        
        binance_rate = await collector.fetch_binance("BTCUSDT")
        print(f"Binance: {binance_rate.funding_rate_pct:.4f}% (annualized: {binance_rate.annualized_rate:.2f}%)")
        
        bybit_rate = await collector.fetch_bybit("BTCUSDT")
        print(f"Bybit: {bybit_rate.funding_rate_pct:.4f}%")
        
        okx_rate = await collector.fetch_okx("BTCUSDT")
        print(f"OKX: {okx_rate.funding_rate_pct:.4f}%")
        
        gate_rate = await collector.fetch_gate("BTCUSDT")
        print(f"Gate: {gate_rate.funding_rate_pct:.4f}%")
        
        # 娴嬭瘯骞惰鑾峰彇
        print("\n" + "=" * 50)
        print("Testing parallel fetch...")
        
        all_rates = await collector.fetch_all("ETHUSDT")
        for exchange, rate in all_rates.items():
            print(f"{exchange}: {rate.funding_rate_pct:.4f}%")
            
        # 娴嬭瘯棰勬祴璐圭巼
        print("\n" + "=" * 50)
        print("Testing predicted rate...")
        
        predicted = await collector.fetch_binance_predicted("BTCUSDT")
        if predicted:
            print(f"Mark Price: ${predicted['mark_price']:.2f}")
            print(f"Index Price: ${predicted['index_price']:.2f}")
            print(f"Next Funding: {predicted['last_funding_rate']*100:.4f}%")
            print(f"Next Funding Time: {predicted['next_funding_time']}")


if __name__ == "__main__":
    asyncio.run(_test_collector())



