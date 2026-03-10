"""
直接测试API数据源（不依赖项目模块）

测试资金费率和恐惧贪婪指数API是否正常工作。
"""
import asyncio
import aiohttp
import json
from datetime import datetime


async def test_binance_funding_rate():
    """直接测试Binance资金费率API"""
    print("\n" + "=" * 60)
    print("Testing Binance Funding Rate API")
    print("=" * 60)
    
    url = "https://fapi.binance.com/fapi/v1/fundingRate"
    params = {"symbol": "BTCUSDT", "limit": 1}
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data:
                        item = data[0]
                        rate = float(item["fundingRate"]) * 100
                        print(f"  Symbol:        {item['symbol']}")
                        print(f"  Funding Rate:  {rate:.4f}%")
                        print(f"  Funding Time:  {datetime.fromtimestamp(item['fundingTime']/1000)}")
                        print("  ✅ SUCCESS")
                        return True
                else:
                    print(f"  ❌ HTTP {resp.status}")
                    return False
        except Exception as e:
            print(f"  ❌ Error: {e}")
            return False


async def test_bybit_funding_rate():
    """直接测试Bybit资金费率API"""
    print("\n" + "=" * 60)
    print("Testing Bybit Funding Rate API")
    print("=" * 60)
    
    url = "https://api.bybit.com/v5/market/funding/history"
    params = {"category": "linear", "symbol": "BTCUSDT", "limit": 1}
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("result", {}).get("list"):
                        item = data["result"]["list"][0]
                        rate = float(item["fundingRate"]) * 100
                        print(f"  Symbol:        {item['symbol']}")
                        print(f"  Funding Rate:  {rate:.4f}%")
                        print(f"  Funding Time:  {datetime.fromtimestamp(int(item['fundingRateTimestamp'])/1000)}")
                        print("  ✅ SUCCESS")
                        return True
                else:
                    print(f"  ❌ HTTP {resp.status}")
                    return False
        except Exception as e:
            print(f"  ❌ Error: {e}")
            return False


async def test_okx_funding_rate():
    """直接测试OKX资金费率API"""
    print("\n" + "=" * 60)
    print("Testing OKX Funding Rate API")
    print("=" * 60)
    
    url = "https://www.okx.com/api/v5/public/funding-rate"
    params = {"instId": "BTC-USDT-SWAP"}
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("data"):
                        item = data["data"][0]
                        rate = float(item["fundingRate"]) * 100
                        print(f"  Symbol:        {item['instId']}")
                        print(f"  Funding Rate:  {rate:.4f}%")
                        print(f"  Next Rate:     {item.get('nextFundingRate', 'N/A')}")
                        print("  ✅ SUCCESS")
                        return True
                else:
                    print(f"  ❌ HTTP {resp.status}")
                    return False
        except Exception as e:
            print(f"  ❌ Error: {e}")
            return False


async def test_gate_funding_rate():
    """直接测试Gate.io资金费率API"""
    print("\n" + "=" * 60)
    print("Testing Gate.io Funding Rate API")
    print("=" * 60)
    
    url = "https://api.gateio.io/api/v4/futures/usdt/contracts/BTC_USDT"
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if "funding_rate" in data:
                        rate = float(data["funding_rate"]) * 100
                        print(f"  Symbol:        {data['name']}")
                        print(f"  Funding Rate:  {rate:.4f}%")
                        print(f"  Funding Rate Indicative: {float(data.get('funding_rate_indicative', 0))*100:.4f}%")
                        print("  ✅ SUCCESS")
                        return True
                else:
                    print(f"  ❌ HTTP {resp.status}")
                    return False
        except Exception as e:
            print(f"  ❌ Error: {e}")
            return False


async def test_fear_greed_index():
    """直接测试恐惧贪婪指数API"""
    print("\n" + "=" * 60)
    print("Testing Fear & Greed Index API")
    print("=" * 60)
    
    url = "https://api.alternative.me/fng/"
    params = {"limit": 1}
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if "data" in data and data["data"]:
                        item = data["data"][0]
                        print(f"  Value:          {item['value']}")
                        print(f"  Classification: {item['value_classification']}")
                        print(f"  Timestamp:      {datetime.fromtimestamp(int(item['timestamp']))}")
                        print("  ✅ SUCCESS")
                        return True
                else:
                    print(f"  ❌ HTTP {resp.status}")
                    return False
        except Exception as e:
            print(f"  ❌ Error: {e}")
            return False


async def main():
    """运行所有测试"""
    print("=" * 60)
    print("  Direct API Test (No Project Dependencies)")
    print("=" * 60)
    
    results = {}
    
    # 测试所有资金费率API
    results["Binance FR"] = await test_binance_funding_rate()
    results["Bybit FR"] = await test_bybit_funding_rate()
    results["OKX FR"] = await test_okx_funding_rate()
    results["Gate FR"] = await test_gate_funding_rate()
    
    # 测试恐惧贪婪指数
    results["Fear & Greed"] = await test_fear_greed_index()
    
    # 汇总
    print("\n" + "=" * 60)
    print("  Summary")
    print("=" * 60)
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for name, success in results.items():
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"  {name:20s}: {status}")
    
    print(f"\n  Total: {passed}/{total} passed")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())