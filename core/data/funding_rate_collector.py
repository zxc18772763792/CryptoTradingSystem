"""
Funding rate collector.

Collects funding rate data from Binance, Bybit, OKX, and Gate.
All endpoints used here are public and do not require API keys.
"""
import asyncio
import aiohttp
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from loguru import logger

from core.data.funding_rate_models import FundingRate, normalize_symbol


class FundingRateCollector:
    """
    Funding rate collector.

    Supports parallel collection from multiple exchanges.
    
    Example:
        collector = FundingRateCollector()
        
        # Fetch a single exchange.
        rate = await collector.fetch_binance("BTCUSDT")

        # Fetch all exchanges.
        rates = await collector.fetch_all("BTCUSDT")

        # Start scheduled collection.
        await collector.start_collection(["BTCUSDT", "ETHUSDT"], interval=60)
    """

    # API endpoints
    BINANCE_URL = "https://fapi.binance.com/fapi/v1/fundingRate"
    BYBIT_URL = "https://api.bybit.com/v5/market/funding/history"
    OKX_URL = "https://www.okx.com/api/v5/public/funding-rate"
    GATE_URL = "https://api.gateio.ws/api/v4/futures/usdt/funding_rate"
    
    # Predicted funding rate endpoint
    BINANCE_PREMIUM_INDEX_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"
    
    def __init__(self, timeout: int = 10):
        """
        Initialize the collector.

        Args:
            timeout: HTTP request timeout in seconds.
        """
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._collected_data: Dict[str, List[FundingRate]] = {}
        
    async def _get_session(self) -> aiohttp.ClientSession:
        """Return an existing HTTP session or create a new one."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout, trust_env=True)
        return self._session
    
    async def close(self):
        """Close the HTTP session."""
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
        Fetch the latest funding rate from Binance.

        Args:
            symbol: Trading pair, for example BTCUSDT.
            limit: Number of records to request.

        Returns:
            A FundingRate record, or None when no data is available.
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
                    
                # Use the latest row.
                latest = data[-1] if isinstance(data, list) else data
                
                return FundingRate(
                    exchange="binance",
                    symbol=symbol,
                    funding_rate=float(latest["fundingRate"]),
                    funding_time=datetime.fromtimestamp(latest["fundingTime"] / 1000),
                    timestamp=datetime.now(timezone.utc),
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
        Fetch historical funding rates from Binance.

        Args:
            symbol: Trading pair.
            limit: Number of records to request, capped by Binance at 1000.

        Returns:
            List of FundingRate records.
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
                        timestamp=datetime.now(timezone.utc),
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
        Fetch Binance premium index data used as predicted funding context.

        Returns:
            Dict with mark price, index price, and estimated settle price.
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
                    "timestamp": datetime.now(timezone.utc),
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
        Fetch the latest funding rate from Bybit.

        Args:
            symbol: Trading pair, for example BTCUSDT.
            limit: Number of records to request.

        Returns:
            A FundingRate record, or None when no data is available.
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
                
                # Validate response payload.
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
                    timestamp=datetime.now(timezone.utc),
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
        Fetch historical funding rates from Bybit.

        Args:
            symbol: Trading pair.
            limit: Number of records to request, capped by Bybit at 200.

        Returns:
            List of FundingRate records.
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
                        timestamp=datetime.now(timezone.utc),
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
        Fetch the latest funding rate from OKX.

        Args:
            symbol: Trading pair, for example BTCUSDT or BTC-USDT-SWAP.

        Returns:
            A FundingRate record, or None when no data is available.
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
                
                # Validate response payload.
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
                    timestamp=datetime.now(timezone.utc),
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
        Fetch the latest funding rate from Gate.

        Args:
            symbol: Trading pair, for example BTCUSDT or BTC_USDT.

        Returns:
            A FundingRate record, or None when no data is available.
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
                    timestamp=datetime.now(timezone.utc),
                    estimated_rate=float(latest.get("funding_rate_indicative", funding_rate or 0)) or None,
                )
                
        except (aiohttp.ClientError, asyncio.TimeoutError, TimeoutError) as e:
            logger.error(f"Gate funding rate fetch error: {e}")
            return None
        except (KeyError, ValueError) as e:
            logger.error(f"Gate funding rate parse error: {e}")
            return None

    # ==================== Aggregation methods ====================
    
    async def fetch_all(
        self,
        symbol: str
    ) -> Dict[str, FundingRate]:
        """
        Fetch funding rates from all supported exchanges in parallel.

        Args:
            symbol: Trading pair. Exchange-specific formats are normalized internally.

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
        Fetch funding rates from all exchanges, including predicted funding context.

        Returns:
            {
                "rates": Dict[str, FundingRate],
                "predicted": Dict from Binance premium index
            }
        """
        rates = await self.fetch_all(symbol)
        predicted = await self.fetch_binance_predicted(symbol)
        
        return {
            "rates": rates,
            "predicted": predicted,
            "symbol": symbol,
            "timestamp": datetime.now(timezone.utc),
        }
        
    async def fetch_history_all(
        self,
        symbol: str,
        limit: int = 100
    ) -> Dict[str, List[FundingRate]]:
        """
        Fetch historical funding rates from all exchanges that support history.

        Args:
            symbol: Trading pair.
            limit: Number of records requested from each exchange.

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
        
    # ==================== Scheduled collection ====================
    
    async def start_collection(
        self,
        symbols: List[str],
        interval: int = 60,
        callback: Optional[callable] = None
    ):
        """
        Start scheduled collection.

        Args:
            symbols: Trading pairs to monitor.
            interval: Collection interval in seconds.
            callback: Optional async callback with signature callback(symbol, rates).
        """
        self._running = True
        logger.info(f"Starting funding rate collection for {symbols} every {interval}s")
        
        while self._running:
            try:
                for symbol in symbols:
                    rates = await self.fetch_all(symbol)
                    
                    # Store data.
                    if symbol not in self._collected_data:
                        self._collected_data[symbol] = []
                    
                    for exchange, rate in rates.items():
                        self._collected_data[symbol].append(rate)
                    
                    # Trigger callback.
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
        """Stop scheduled collection."""
        self._running = False
        logger.info("Funding rate collection stopped")
        
    def get_collected_data(self, symbol: str) -> List[FundingRate]:
        """Return collected data for a symbol."""
        return self._collected_data.get(symbol, [])
        
    def clear_collected_data(self, symbol: Optional[str] = None):
        """Clear collected data."""
        if symbol:
            self._collected_data[symbol] = []
        else:
            self._collected_data.clear()
            
    @property
    def is_running(self) -> bool:
        """Whether scheduled collection is running."""
        return self._running


# Global instance.
funding_rate_collector = FundingRateCollector()


# ==================== Quick test ====================

async def _test_collector():
    """Quick manual test."""
    async with FundingRateCollector() as collector:
        # Test individual exchanges.
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
        
        # Test parallel fetch.
        print("\n" + "=" * 50)
        print("Testing parallel fetch...")
        
        all_rates = await collector.fetch_all("ETHUSDT")
        for exchange, rate in all_rates.items():
            print(f"{exchange}: {rate.funding_rate_pct:.4f}%")
            
        # Test predicted funding rate.
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



