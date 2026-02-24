"""Quick Binance test"""
import asyncio
import aiohttp

async def test():
    print('Testing Binance Data API...')

    async with aiohttp.ClientSession() as session:
        # Test ping
        url = 'https://data-api.binance.vision/api/v3/ping'
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                print('[OK] Binance Data API is accessible!')
            else:
                print('[FAIL] HTTP', resp.status)
                return

        # Get BTC price
        url = 'https://data-api.binance.vision/api/v3/ticker/price'
        params = {'symbol': 'BTCUSDT'}
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                data = await resp.json()
                print('[OK] BTC Price: $' + str(round(float(data['price']), 2)))

        # Get klines
        url = 'https://data-api.binance.vision/api/v3/klines'
        params = {'symbol': 'BTCUSDT', 'interval': '1h', 'limit': 10}
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                data = await resp.json()
                print('[OK] Got', len(data), 'klines from Binance')

asyncio.run(test())
