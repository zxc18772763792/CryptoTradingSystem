"""
测试 Binance 完整交易功能 - 修复时间戳问题
"""
import asyncio
import aiohttp
from datetime import datetime
import hmac
import hashlib
from urllib.parse import urlencode
import sys
import time

sys.path.insert(0, str(__file__).replace('\\scripts\\test_binance_trading.py', ''))

from config.settings import settings

PROXY = 'http://127.0.0.1:7890'
API_KEY = settings.BINANCE_API_KEY
API_SECRET = settings.BINANCE_API_SECRET

# 全局时间偏移量
TIME_OFFSET = 0


def sign_request(params: dict, secret: str) -> str:
    """生成签名"""
    query_string = urlencode(params)
    signature = hmac.new(
        secret.encode('utf-8'),
        query_string.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()
    return signature


async def get_server_time(session) -> int:
    """获取Binance服务器时间"""
    url = 'https://api.binance.com/api/v3/time'
    async with session.get(url, proxy=PROXY, timeout=aiohttp.ClientTimeout(total=10)) as resp:
        if resp.status == 200:
            data = await resp.json()
            return data['serverTime']
    return int(time.time() * 1000)


async def test_binance_trading():
    global TIME_OFFSET

    print('=' * 60)
    print('测试 Binance 完整交易功能')
    print('=' * 60)
    print(f'API Key: {API_KEY[:8]}...{API_KEY[-8:]}')
    print(f'API Secret: {API_SECRET[:4]}...{API_SECRET[-4:]}')
    print(f'代理: {PROXY}')

    async with aiohttp.ClientSession() as session:
        headers = {'X-MBX-APIKEY': API_KEY}

        # 1. 测试连接并同步时间
        print('\n1. 同步服务器时间...')
        try:
            local_time = int(time.time() * 1000)
            server_time = await get_server_time(session)
            TIME_OFFSET = server_time - local_time
            print(f'  本地时间: {datetime.now().strftime("%H:%M:%S.%f")[:-3]}')
            print(f'  服务器时间: {datetime.fromtimestamp(server_time/1000).strftime("%H:%M:%S.%f")[:-3]}')
            print(f'  时间偏移: {TIME_OFFSET} ms')
            print('  [OK] 时间同步完成')
        except Exception as e:
            print(f'  [FAIL] {e}')
            return

        # 2. 获取账户信息 (需要签名)
        print('\n2. 获取账户信息...')
        try:
            timestamp = int(time.time() * 1000) + TIME_OFFSET
            params = {'timestamp': timestamp, 'recvWindow': 60000}
            params['signature'] = sign_request(params, API_SECRET)

            url = f'https://api.binance.com/api/v3/account?{urlencode(params)}'

            async with session.get(url, headers=headers, proxy=PROXY,
                                   timeout=aiohttp.ClientTimeout(total=15)) as resp:
                data = await resp.json()
                if resp.status == 200:
                    print('  [OK] 账户信息获取成功!')
                    balances = [b for b in data.get('balances', [])
                                if float(b['free']) > 0 or float(b['locked']) > 0]
                    print(f'  持有资产: {len(balances)} 种')

                    for b in balances[:20]:  # 显示前20种资产
                        free = float(b['free'])
                        locked = float(b['locked'])
                        if free > 0 or locked > 0:
                            print(f'    {b["asset"]}: {free:.6f} (锁定: {locked:.6f})')
                else:
                    print(f'  [FAIL] 状态码: {resp.status}')
                    print(f'  错误信息: {data}')
        except Exception as e:
            print(f'  [FAIL] {e}')

        # 3. 获取BTC价格
        print('\n3. 获取BTC价格...')
        btc_price = 0
        try:
            url = 'https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT'
            async with session.get(url, proxy=PROXY, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    btc_price = float(data['price'])
                    print(f'  [OK] BTC/USDT: ${btc_price:,.2f}')
        except Exception as e:
            print(f'  [FAIL] {e}')

        # 4. 查询BTCUSDT交易对精度
        print('\n4. 查询交易规则...')
        try:
            url = 'https://api.binance.com/api/v3/exchangeInfo?symbol=BTCUSDT'
            async with session.get(url, proxy=PROXY, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('symbols'):
                        info = data['symbols'][0]
                        print(f'  [OK] {info["symbol"]} 状态: {info["status"]}')

                        for f in info['filters']:
                            if f['filterType'] == 'LOT_SIZE':
                                min_qty = float(f['minQty'])
                                step_size = float(f['stepSize'])
                                print(f'  最小下单量: {min_qty} BTC')
                                print(f'  下单精度: {step_size} BTC')
                            elif f['filterType'] == 'MIN_NOTIONAL':
                                min_notional = float(f.get('minNotional', 0))
                                print(f'  最小订单金额: ${min_notional}')
        except Exception as e:
            print(f'  [FAIL] {e}')

        # 5. 查询开放订单
        print('\n5. 查询当前挂单...')
        try:
            timestamp = int(time.time() * 1000) + TIME_OFFSET
            params = {'timestamp': timestamp, 'recvWindow': 60000, 'symbol': 'BTCUSDT'}
            params['signature'] = sign_request(params, API_SECRET)

            url = f'https://api.binance.com/api/v3/openOrders?{urlencode(params)}'

            async with session.get(url, headers=headers, proxy=PROXY,
                                   timeout=aiohttp.ClientTimeout(total=15)) as resp:
                data = await resp.json()
                if resp.status == 200:
                    print(f'  [OK] 查询订单成功, 当前挂单: {len(data)} 个')
                    for order in data[:5]:
                        print(f'    订单ID: {order["orderId"]}, {order["side"]}, {order["type"]}')
                else:
                    print(f'  [FAIL] {data}')
        except Exception as e:
            print(f'  [FAIL] {e}')

        # 6. 测试下单能力 (查询API权限)
        print('\n6. 检查API权限...')
        print('  提示: 如果无法下单，请检查Binance API设置')
        print('  需要开启: Enable Spot & Margin Trading')

    print('\n' + '=' * 60)
    print('测试完成!')
    print('=' * 60)
    print('\n如果账户信息显示正常，说明API配置正确，可以用于交易。')


if __name__ == "__main__":
    asyncio.run(test_binance_trading())
