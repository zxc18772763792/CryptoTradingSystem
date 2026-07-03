"""Manual Binance live-account connectivity check.

This script calls real Binance account and open-order read endpoints. It never
places, cancels, or modifies orders, but it is gated so account reads cannot run
by accident.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.settings import settings


ALLOW_LIVE_ENV = "ALLOW_BINANCE_LIVE_CHECK"
DEFAULT_PROXY = os.environ.get("BINANCE_TEST_PROXY", "http://127.0.0.1:7890")
TIME_OFFSET = 0


def sign_request(params: dict, secret: str) -> str:
    query_string = urlencode(params)
    return hmac.new(
        secret.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


async def get_server_time(session: aiohttp.ClientSession, *, proxy: str | None) -> int:
    url = "https://api.binance.com/api/v3/time"
    async with session.get(url, proxy=proxy, timeout=aiohttp.ClientTimeout(total=10)) as resp:
        if resp.status == 200:
            data = await resp.json()
            return int(data["serverTime"])
    return int(time.time() * 1000)


async def run_binance_live_account_check(*, proxy: str | None) -> int:
    global TIME_OFFSET

    api_key = str(settings.BINANCE_API_KEY or "").strip()
    api_secret = str(settings.BINANCE_API_SECRET or "").strip()
    if not api_key or not api_secret:
        print("BINANCE_API_KEY and BINANCE_API_SECRET must be configured.")
        return 2

    print("=" * 60)
    print("Binance live-account read check")
    print("=" * 60)
    print("API key configured: yes")
    print("API secret configured: yes")
    print(f"Proxy: {proxy or 'disabled'}")

    async with aiohttp.ClientSession() as session:
        headers = {"X-MBX-APIKEY": api_key}

        print("\n1. Syncing server time...")
        try:
            local_time = int(time.time() * 1000)
            server_time = await get_server_time(session, proxy=proxy)
            TIME_OFFSET = server_time - local_time
            print(f"  Local time: {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
            print(f"  Server time: {datetime.fromtimestamp(server_time / 1000).strftime('%H:%M:%S.%f')[:-3]}")
            print(f"  Time offset: {TIME_OFFSET} ms")
        except Exception as exc:
            print(f"  [FAIL] {exc}")
            return 1

        print("\n2. Reading account balances...")
        try:
            timestamp = int(time.time() * 1000) + TIME_OFFSET
            params = {"timestamp": timestamp, "recvWindow": 60000}
            params["signature"] = sign_request(params, api_secret)
            url = f"https://api.binance.com/api/v3/account?{urlencode(params)}"

            async with session.get(url, headers=headers, proxy=proxy, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                data = await resp.json()
                if resp.status == 200:
                    balances = [
                        item
                        for item in data.get("balances", [])
                        if float(item.get("free") or 0) > 0 or float(item.get("locked") or 0) > 0
                    ]
                    print(f"  [OK] Account read succeeded; non-zero assets: {len(balances)}")
                    for item in balances[:20]:
                        free = float(item.get("free") or 0)
                        locked = float(item.get("locked") or 0)
                        print(f"    {item.get('asset')}: {free:.6f} (locked: {locked:.6f})")
                else:
                    print(f"  [FAIL] Status: {resp.status}")
                    print(f"  Response: {data}")
        except Exception as exc:
            print(f"  [FAIL] {exc}")

        print("\n3. Reading BTC price...")
        try:
            url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
            async with session.get(url, proxy=proxy, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    print(f"  [OK] BTC/USDT: ${float(data['price']):,.2f}")
        except Exception as exc:
            print(f"  [FAIL] {exc}")

        print("\n4. Reading BTCUSDT trading rules...")
        try:
            url = "https://api.binance.com/api/v3/exchangeInfo?symbol=BTCUSDT"
            async with session.get(url, proxy=proxy, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("symbols"):
                        info = data["symbols"][0]
                        print(f"  [OK] {info['symbol']} status: {info['status']}")
                        for item in info.get("filters", []):
                            if item.get("filterType") == "LOT_SIZE":
                                print(f"  Min quantity: {float(item.get('minQty') or 0)} BTC")
                                print(f"  Step size: {float(item.get('stepSize') or 0)} BTC")
                            elif item.get("filterType") == "MIN_NOTIONAL":
                                print(f"  Min notional: ${float(item.get('minNotional') or 0)}")
        except Exception as exc:
            print(f"  [FAIL] {exc}")

        print("\n5. Reading current BTCUSDT open orders...")
        try:
            timestamp = int(time.time() * 1000) + TIME_OFFSET
            params = {"timestamp": timestamp, "recvWindow": 60000, "symbol": "BTCUSDT"}
            params["signature"] = sign_request(params, api_secret)
            url = f"https://api.binance.com/api/v3/openOrders?{urlencode(params)}"

            async with session.get(url, headers=headers, proxy=proxy, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                data = await resp.json()
                if resp.status == 200:
                    print(f"  [OK] Open-order read succeeded; current open orders: {len(data)}")
                    for order in data[:5]:
                        print(f"    Order ID: {order.get('orderId')}, {order.get('side')}, {order.get('type')}")
                else:
                    print(f"  [FAIL] {data}")
        except Exception as exc:
            print(f"  [FAIL] {exc}")

    print("\n" + "=" * 60)
    print("Read-only Binance live-account check complete.")
    print("=" * 60)
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a gated, read-only Binance live-account check.")
    parser.add_argument(
        "--allow-live-account-check",
        action="store_true",
        help=f"Allow real Binance account read endpoints. Alternatively set {ALLOW_LIVE_ENV}=1.",
    )
    parser.add_argument("--proxy", default=DEFAULT_PROXY, help="HTTP proxy URL; pass an empty string to disable.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if not args.allow_live_account_check and os.environ.get(ALLOW_LIVE_ENV) != "1":
        print(
            "Refusing to call live Binance account endpoints. "
            "Re-run with --allow-live-account-check or set ALLOW_BINANCE_LIVE_CHECK=1."
        )
        return 2
    return asyncio.run(run_binance_live_account_check(proxy=str(args.proxy or "").strip() or None))


if __name__ == "__main__":
    raise SystemExit(main())
