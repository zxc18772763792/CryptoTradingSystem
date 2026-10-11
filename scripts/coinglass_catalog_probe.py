"""Which Coinglass data can we get through the vip2 relay? Probe every endpoint once and build the table.

docs/COINGLASS_DATA_CATALOG.md is generated from this. For each endpoint in the official v4 list
(data/premium/coinglass/api_spec_v4.json plus endpoints added on docs.coinglass.com since) and the
relay's own LSR / module / account endpoints it records: whether our plan serves it, whether it
covers an alt (WLD), and how deep its daily history goes (BTC, asked from 2018).

Goes through the project's CoinglassClient (the shared budget ledger), one request every 15 s so
the relay's rolling 11-per-minute limit keeps room for the collector and the archives. Results
are saved after every request (data/premium/coinglass/catalog_probe.json, local only); a rerun
asks again only what failed with a timeout or connection error. Never prints the key, and keeps
only the field names of the account endpoint.

    python scripts/coinglass_catalog_probe.py           # probe (resumable)
    python scripts/coinglass_catalog_probe.py --table   # print the markdown table from the results
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

SPEC = PROJECT_ROOT / "data" / "premium" / "coinglass" / "api_spec_v4.json"
OUT = PROJECT_ROOT / "data" / "premium" / "coinglass" / "catalog_probe.json"
SPACING = 15.0
NOW = int(time.time() * 1000)
DAY = 86_400_000
START_2018 = int(datetime(2018, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
# listed on docs.coinglass.com on 2026-10-11 but missing from the April spec
ADDED_ENDPOINTS = [
    {"path": "/api/futures/indicators/td", "title": "TD Sequential", "category": "Indicator > Futures",
     "parameters": [{"name": "exchange", "required": True}, {"name": "symbol", "required": True},
                    {"name": "interval", "required": True}, {"name": "limit"}, {"name": "start_time"}, {"name": "end_time"}]},
]

RANGE = {
    "/api/futures/open-interest/exchange-history-chart": "4h",
    "/api/futures/funding-rate/accumulated-exchange-list": "7d",
    "/api/futures/taker-buy-sell-volume/exchange-list": "24h",
    "/api/futures/liquidation/exchange-list": "24h",
    "/api/futures/liquidation/map": "7d",
    "/api/futures/liquidation/aggregated-map": "7d",
    "/api/futures/liquidation/max-pain": "24h",
    "/api/etf/bitcoin/price/history": "1d",
    "/api/option/exchange-oi-history": "4h",
    "/api/option/exchange-vol-history": "4h",
}
for model in ("model1", "model2", "model3"):
    RANGE[f"/api/futures/liquidation/heatmap/{model}"] = "3d"
    RANGE[f"/api/futures/liquidation/aggregated-heatmap/{model}"] = "3d"
for side in ("futures", "spot"):  # order book depth within +/- this percent of the price
    RANGE[f"/api/{side}/orderbook/ask-bids-history"] = "1"
    RANGE[f"/api/{side}/orderbook/aggregated-ask-bids-history"] = "1"
RANGE["/api/option/exchange-oi-history"] = "all"
RANGE["/api/option/exchange-vol-history"] = "all"
LIMIT_1000 = {"/api/hyperliquid/global-long-short-account-ratio/history", "/api/bitfinex-margin-long-short",
              "/api/futures/basis/history"}  # these reject or come back empty above 1000 rows
EXCHANGE_LIST = "Binance,OKX,Bybit"  # aggregated endpoints answer 400 without it, though the spec says optional
SKIP = {"/api/hyperliquid/user-position": "needs a wallet address"}
# confirmed for alts on 10/11 (WLD/VANA): no second request needed
ALT_CONFIRMED = {
    "/api/futures/coins-markets", "/api/futures/open-interest/history", "/api/futures/open-interest/aggregated-history",
    "/api/futures/open-interest/exchange-list", "/api/futures/funding-rate/history", "/api/futures/funding-rate/oi-weight-history",
    "/api/futures/liquidation/history", "/api/futures/liquidation/aggregated-history", "/api/futures/liquidation/map",
    "/api/futures/liquidation/aggregated-map", "/api/futures/global-long-short-account-ratio/history",
    "/api/futures/top-long-short-position-ratio/history", "/api/futures/aggregated-taker-buy-sell-volume/history",
}
BTC_ONLY_PREFIXES = ("/api/etf", "/api/hk-etf", "/api/index", "/api/grayscale", "/api/option", "/api/coinbase-premium",
                     "/api/bull-market", "/api/hyperliquid/whale", "/api/hyperliquid/wallet", "/api/exchange_assets",
                     "/api/calendar", "/api/article", "/api/user")
LSR_PROBES = [
    ("/api/gateway/account", {}),
    ("/api/v1/modules/", {}),
    ("/api/lsr/signal/WLD", {"mode": "trader"}),
    ("/api/lsr/signal/WLD", {"mode": "whale"}),
    ("/api/lsr/signals", {"symbols": "BTC,WLD,VANA", "mode": "trader"}),
    ("/api/lsr/ranking", {"mode": "trader", "limit": 5, "offset": 0}),
    ("/api/lsr/status", {}),
    ("/api/lsr/health", {}),
    ("/api/lsr/funding-rate/arbitrage", {"sort": "apr", "order": "desc"}),
    ("/api/lsr/consensus/healthz", {}),
    ("/api/lsr/consensus/v1/symbols", {}),
]


TRANSIENT = ("Timeout", "Connect", "budget never", "ServerDisconnected", "http_5")


def finished(row, retry_failed: bool = False) -> bool:
    """Results worth keeping: anything but a timeout / connection failure, which is asked again.
    With retry_failed, also ask again whatever came back with an error code or no rows."""
    if "skipped" in row:
        return True
    if retry_failed and "btc" in row:
        b = row.get("btc") or {}
        if str(b.get("code")) not in {"0", "40300"} or not b.get("rows"):
            return False
    for key in ("btc", "alt"):
        err = (row.get(key) or {}).get("error") or ""
        if any(t in err for t in TRANSIENT):
            return False
    return True


def params_for(path: str, plist, coin: str, pair: str, depth: bool):
    names = {p.rstrip("*") for p in plist}
    required = {p.rstrip("*") for p in plist if p.endswith("*")}
    p = {}
    is_option = path.startswith("/api/option")
    if "exchange" in names:
        p["exchange"] = "Deribit" if is_option else "Binance"
    if "symbol" in names:
        if path == "/api/coin/vesting":
            p["symbol"] = "ARB" if coin == "BTC" else coin
        elif "exchange" in names and not is_option and path != "/api/borrow-interest-rate/history":
            p["symbol"] = pair
        else:
            p["symbol"] = coin
    if "exchange_list" in names:
        p["exchange_list"] = EXCHANGE_LIST
    if "interval" in names:
        p["interval"] = ("1h" if "footprint" in path or "orderbook/history" in path
                         else "4h" if "ask-bids-history" in path else "1d")
    if "unit" in names and is_option:
        p["unit"] = "USD"
    if "range" in names:
        p["range"] = RANGE.get(path, "24h")
    if "usd" in names:
        p["usd"] = 10000
    if "ticker" in required:
        p["ticker"] = "GBTC"
    if "current_page" in names:
        p["current_page"] = 1
    if "per_page" in names:
        p["per_page"] = 10
    if "min_liquidation_amount" in names:
        p["min_liquidation_amount"] = 10000
    if "start_time" in required or path == "/api/futures/liquidation/order":
        p["start_time"], p["end_time"] = NOW - DAY, NOW
        if path == "/api/futures/liquidation/order":
            p["symbol"] = coin  # the order list is keyed by coin, not by pair
    elif depth and "start_time" in names and "interval" in names:
        p.update(start_time=START_2018, end_time=NOW, limit=1000 if path in LIMIT_1000 else 4500)
    elif "limit" in names:
        p["limit"] = 5
    return p


def summarize(payload):
    if not isinstance(payload, (dict, list)):
        return {"kind": type(payload).__name__}
    code = payload.get("code") if isinstance(payload, dict) else None
    success = payload.get("success") if isinstance(payload, dict) else None
    msg = str(payload.get("msg") or payload.get("message") or "")[:120] if isinstance(payload, dict) else ""
    data = payload.get("data") if isinstance(payload, dict) and "data" in payload else payload
    rows = data if isinstance(data, list) else ([data] if isinstance(data, dict) else [])
    stamps = []
    for r in rows:
        if isinstance(r, dict):
            for k in ("time", "t", "timestamp", "date", "createTime"):
                if r.get(k) not in (None, ""):
                    try:
                        v = int(float(r[k]))
                        stamps.append(v * 1000 if v < 10 ** 11 else v)
                    except (TypeError, ValueError):
                        pass
                    break
    first = rows[0] if rows else None
    keys = sorted(first.keys())[:40] if isinstance(first, dict) else (sorted(data.keys())[:40] if isinstance(data, dict) else [])
    fmt = lambda ms: datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d") if ms else None
    return {"code": code, "success": success, "msg": msg, "rows": len(rows), "keys": keys,
            "first": fmt(min(stamps)) if stamps else None, "last": fmt(max(stamps)) if stamps else None,
            "sample": json.dumps(first, ensure_ascii=False, default=str)[:600] if first is not None else None}


async def ask(client, path, params, last):
    from core.data.coinglass_client import CoinglassBudgetExceeded, CoinglassError

    for _ in range(10):
        wait = SPACING - (time.monotonic() - last[0])
        if wait > 0:
            await asyncio.sleep(wait)
        last[0] = time.monotonic()
        try:
            resp = await client.request_json(path, params=params, manual=False)
            out = {"http": resp.get("status_code"), **summarize(resp.get("payload"))}
            if path.startswith("/api/gateway/"):
                out.pop("sample", None)  # account details stay out of the file
            return out
        except CoinglassBudgetExceeded:
            await asyncio.sleep(15)
        except CoinglassError as exc:
            text = str(exc)
            if "429" in text:
                await asyncio.sleep(75)
                continue
            return {"error": text[:200]}
        except Exception as exc:  # noqa: BLE001
            return {"error": f"{type(exc).__name__}: {str(exc)[:160]}"}
    return {"error": "budget never freed"}


async def probe(out: Path = OUT, retry_failed: bool = False) -> None:
    from core.data.coinglass_client import CoinglassClient

    spec = json.loads(SPEC.read_text(encoding="utf-8"))
    done = json.loads(out.read_text(encoding="utf-8")) if out.exists() else {}
    seen, endpoints = set(), []
    for e in spec["endpoints"]:
        if e["path"] not in seen:
            seen.add(e["path"])
            endpoints.append(e)
    endpoints += [e for e in ADDED_ENDPOINTS if e["path"] not in seen]
    seen |= {e["path"] for e in ADDED_ENDPOINTS}
    last = [0.0]
    async with CoinglassClient() as client:
        health = await ask(client, "/api/lsr/health", {}, last)
        if "error" in health:
            print("relay unreachable, stopping:", health["error"][:120], flush=True)
            return
        # the relay's own copies of the specs: saved whole; Coinglass endpoints new to us get probed too
        for module in ("coinglass", "lsr"):
            await asyncio.sleep(SPACING)
            last[0] = time.monotonic()
            try:
                resp = await client.request_json(f"/api/v1/modules/{module}/api-spec", params={})
            except Exception as exc:  # noqa: BLE001 - the probe goes on with the spec we have
                done[f"spec {module}"] = {"error": f"{type(exc).__name__}: {str(exc)[:160]}"}
                continue
            payload = resp.get("payload")
            relay_spec = payload.get("data") if isinstance(payload, dict) and isinstance(payload.get("data"), dict) else payload
            listed = (relay_spec or {}).get("endpoints") if isinstance(relay_spec, dict) else None
            (SPEC.parent / f"api_spec_vip2_{module}.json").write_text(
                json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
            new = [e for e in listed or [] if isinstance(e, dict) and e.get("path") and e["path"] not in seen]
            done[f"spec {module}"] = {"endpoints": len(listed or []), "new_paths": [e["path"] for e in new]}
            if module == "coinglass":
                endpoints += new
                seen |= {e["path"] for e in new}
            print(f"relay spec {module}: {len(listed or [])} endpoints, {len(new)} new", flush=True)
        for path, params in LSR_PROBES:
            key = f"LSR {path} {json.dumps(params, sort_keys=True)}"
            if key not in done or not finished(done[key]):
                done[key] = {"path": path, "params": params, "btc": await ask(client, path, params, last)}
                out.write_text(json.dumps(done, ensure_ascii=False, indent=1), encoding="utf-8")
                print(key, done[key]["btc"].get("code"), done[key]["btc"].get("success"), done[key]["btc"].get("error", ""), flush=True)
        for e in endpoints:
            path = e["path"]
            if path in done and finished(done[path], retry_failed):
                continue
            plist = [p.get("name") + ("*" if p.get("required") else "") for p in e.get("parameters") or []]
            row = {"title": e["title"], "category": e["category"], "params_spec": plist}
            if path in SKIP:
                row["skipped"] = SKIP[path]
            else:
                p1 = params_for(path, plist, "BTC", "BTCUSDT", depth=True)
                row["btc_params"] = p1
                row["btc"] = await ask(client, path, p1, last)
                per_coin = any(p.rstrip("*") == "symbol" for p in plist)
                ok = row["btc"].get("code") in ("0", 0) or row["btc"].get("success") is True
                if per_coin and ok and path not in ALT_CONFIRMED and not path.startswith(BTC_ONLY_PREFIXES):
                    p2 = params_for(path, plist, "WLD", "WLDUSDT", depth=False)
                    row["alt_params"] = p2
                    row["alt"] = await ask(client, path, p2, last)
            done[path] = row
            out.write_text(json.dumps(done, ensure_ascii=False, indent=1), encoding="utf-8")
            b = row.get("btc") or {}
            print(f"{path} | code {b.get('code')} rows {b.get('rows')} {b.get('first')}..{b.get('last')} "
                  f"{b.get('error', '')[:80]} | alt {((row.get('alt') or {}).get('code'), (row.get('alt') or {}).get('rows'))}",
                  flush=True)



# (category, data, paths, what it gives / signal use, ours)
ROWS = [
    ("合约行情", "全币快照（约 1500 个币，74 个字段）", ["/api/futures/coins-markets"],
     "价格、持仓量及 5 分钟到 24 小时变化、持仓/市值、多空爆仓、主动多空成交、加权资金费率", "雷达、周度名单在用；10/11 起每天存 4 份"),
    ("合约行情", "单币各交易所行情", ["/api/futures/pairs-markets"], "一个币在各交易所的价格、持仓、成交、资金费率", ""),
    ("合约行情", "全币涨跌幅", ["/api/futures/coins-price-change"], "各币多个窗口的涨跌幅", ""),
    ("合约行情", "价格 K 线", ["/api/futures/price/history"], "交易对开高低收", "用币安 K 线代替"),
    ("合约行情", "支持的币、交易所、交易对、已下架交易对、交易所排名",
     ["/api/futures/supported-coins", "/api/futures/supported-exchanges", "/api/futures/supported-exchange-pairs",
      "/api/futures/delisted-exchange-pairs", "/api/futures/exchange-rank"], "目录类，可补回已下架的币（减少幸存者偏差）", ""),
    ("持仓量", "交易对持仓量 K 线", ["/api/futures/open-interest/history"], "单交易所持仓量开高低收", "周度名单在用"),
    ("持仓量", "合计持仓量 K 线", ["/api/futures/open-interest/aggregated-history"], "多交易所合计", "10/11 已检验（持仓量 3 日变化）；后台采集器可选项"),
    ("持仓量", "U 本位 / 币本位持仓", ["/api/futures/open-interest/aggregated-stablecoin-history",
                                 "/api/futures/open-interest/aggregated-coin-margin-history"], "两种保证金的持仓分开看", "U 本位为后台采集器可选项；未检验"),
    ("持仓量", "各交易所持仓分布", ["/api/futures/open-interest/exchange-list", "/api/futures/open-interest/exchange-history-chart"],
     "各交易所持仓占比及变化", "后台采集器（BTC/ETH/SOL）"),
    ("资金费率", "交易对资金费率 K 线", ["/api/futures/funding-rate/history"], "单交易所", "周度名单、后台采集器在用"),
    ("资金费率", "持仓加权 / 成交加权资金费率", ["/api/futures/funding-rate/oi-weight-history", "/api/futures/funding-rate/vol-weight-history"],
     "多交易所加权", "10/11 已检验（资金费率偏离）"),
    ("资金费率", "各交易所当前与累计费率、套利机会", ["/api/futures/funding-rate/exchange-list",
                                       "/api/futures/funding-rate/accumulated-exchange-list", "/api/futures/funding-rate/arbitrage"],
     "跨交易所费率差", "后台采集器（当前费率、套利）"),
    ("多空比", "散户账户多空比", ["/api/futures/global-long-short-account-ratio/history"], "按交易对", "10/11 已检验（散户多空偏离）；后台采集器"),
    ("多空比", "大户账户 / 大户持仓多空比", ["/api/futures/top-long-short-account-ratio/history",
                                   "/api/futures/top-long-short-position-ratio/history"], "按交易对", "持仓口径 10/11 已检验"),
    ("多空比", "净多空持仓", ["/api/futures/net-position/history", "/api/futures/v2/net-position/history"], "多空净头寸变化", "后台采集器可选项；未检验"),
    ("多空比", "各交易所主动买卖比", ["/api/futures/taker-buy-sell-volume/exchange-list"], "快照", "后台采集器"),
    ("爆仓", "交易对 / 合计爆仓 K 线", ["/api/futures/liquidation/history", "/api/futures/liquidation/aggregated-history"],
     "多头、空头爆仓额", "10/11 已检验（爆仓异常、爆仓多空占比）；后台采集器"),
    ("爆仓", "爆仓排行、各交易所爆仓", ["/api/futures/liquidation/coin-list", "/api/futures/liquidation/exchange-list"], "快照", ""),
    ("爆仓", "逐笔爆仓单", ["/api/futures/liquidation/order"], "单笔强平订单", ""),
    ("爆仓", "爆仓地图", ["/api/futures/liquidation/map", "/api/futures/liquidation/aggregated-map"], "按价位估算的爆仓量（无历史）",
     "10/11 起每天存 131 个币"),
    ("爆仓", "合计爆仓热力图（3 个模型）", ["/api/futures/liquidation/aggregated-heatmap/model1",
                                    "/api/futures/liquidation/aggregated-heatmap/model2",
                                    "/api/futures/liquidation/aggregated-heatmap/model3"],
     "时间 × 价位的爆仓密度，多交易所合计；只能取截止到现在的最近 90 天（6 小时一格）", "后台采集器可选项（模型 1）"),
    ("爆仓", "交易对爆仓热力图（3 个模型）", ["/api/futures/liquidation/heatmap/model1", "/api/futures/liquidation/heatmap/model2",
                                     "/api/futures/liquidation/heatmap/model3"], "单交易所版本", ""),
    ("爆仓", "爆仓最大痛点", ["/api/futures/liquidation/max-pain"], "各币爆仓最集中的价位", ""),
    ("订单簿", "挂单深度历史（交易对 / 合计）", ["/api/futures/orderbook/ask-bids-history", "/api/futures/orderbook/aggregated-ask-bids-history"],
     "价格上下一定范围内的买卖挂单量", "合计版为后台采集器可选项；未检验"),
    ("订单簿", "订单簿热力图", ["/api/futures/orderbook/history"], "挂单分布随时间变化", ""),
    ("订单簿", "大额挂单（当前 / 历史）", ["/api/futures/orderbook/large-limit-order", "/api/futures/orderbook/large-limit-order-history"],
     "大单的位置与成交/撤单", ""),
    ("主动买卖", "主动买卖量 K 线（交易对 / 合计）", ["/api/futures/v2/taker-buy-sell-volume/history",
                                         "/api/futures/aggregated-taker-buy-sell-volume/history"], "主动买、主动卖", "合计口径 10/11 已检验"),
    ("主动买卖", "主动买卖差累计 CVD（交易对 / 合计）", ["/api/futures/cvd/history", "/api/futures/aggregated-cvd/history"], "累计净主动买", "未检验"),
    ("主动买卖", "成交足迹（90 天）", ["/api/futures/volume/footprint-history"], "每根 K 线内按价位的主动买卖", ""),
    ("主动买卖", "合约资金净流入", ["/api/futures/netflow-list", "/api/futures/coin/netflow"], "按币的净流入", "未检验"),
    ("现货", "现货全币快照、交易对行情、K 线", ["/api/spot/coins-markets", "/api/spot/pairs-markets", "/api/spot/price/history",
                                     "/api/spot/supported-coins", "/api/spot/supported-exchange-pairs"], "现货版的行情", ""),
    ("现货", "现货挂单深度、热力图、大额挂单", ["/api/spot/orderbook/ask-bids-history", "/api/spot/orderbook/aggregated-ask-bids-history",
                                     "/api/spot/orderbook/history", "/api/spot/orderbook/large-limit-order",
                                     "/api/spot/orderbook/large-limit-order-history"], "现货订单簿", ""),
    ("现货", "现货主动买卖、CVD、足迹、净流入", ["/api/spot/taker-buy-sell-volume/history", "/api/spot/aggregated-taker-buy-sell-volume/history",
                                      "/api/spot/volume/footprint-history", "/api/spot/cvd/history", "/api/spot/aggregated-cvd/history",
                                      "/api/spot/netflow-list", "/api/spot/coin/netflow"], "现货资金面", "现货净流入：数据页链上面板在用；未检验"),
    ("Hyperliquid", "鲸鱼提醒、鲸鱼持仓、按币持仓、持仓与盈亏分布、多空比",
     ["/api/hyperliquid/whale-alert", "/api/hyperliquid/whale-position", "/api/hyperliquid/position",
      "/api/hyperliquid/wallet/position-distribution", "/api/hyperliquid/wallet/pnl-distribution",
      "/api/hyperliquid/global-long-short-account-ratio/history"], "链上永续交易所的大户动向", ""),
    ("期权", "最大痛点、期权概况、各交易所持仓/成交历史", ["/api/option/max-pain", "/api/option/info",
                                          "/api/option/exchange-oi-history", "/api/option/exchange-vol-history"], "主要是 BTC/ETH", "后台采集器"),
    ("链上", "交易所资产与余额", ["/api/exchange/assets", "/api/exchange/balance/list", "/api/exchange/balance/chart"],
     "交易所持币量变化（流入流出）", "数据页链上面板在用"),
    ("链上", "交易所链上转账、鲸鱼转账", ["/api/exchange/chain/tx/list", "/api/chain/v2/whale-transfer"], "大额链上转账", "数据页链上面板在用"),
    ("链上", "代币解锁列表、解锁排期", ["/api/coin/unlock-list", "/api/coin/vesting"], "未来解锁", "解锁跟踪器用的是 DefiLlama"),
    ("技术指标", "RSI、MA、EMA、布林、MACD、ATR、TD 序列（单币与全币列表）",
     ["/api/futures/indicators/rsi", "/api/futures/rsi/list", "/api/futures/indicators/ma", "/api/futures/ma/list",
      "/api/futures/indicators/ema", "/api/futures/ema/list", "/api/futures/indicators/boll", "/api/futures/indicators/macd",
      "/api/futures/macd/list", "/api/futures/indicators/avg-true-range", "/api/futures/avg-true-range/list",
      "/api/futures/indicators/td"],
     "用 K 线自己能算", ""),
    ("合约指标", "基差、鲸鱼指数、CGDI / CDRI 指数", ["/api/futures/basis/history", "/api/futures/whale-index/history",
                                         "/api/futures/cgdi-index/history", "/api/futures/cdri-index/history"], "期现价差、大户活跃度", "未检验"),
    ("市场指标", "Coinbase 溢价、Bitfinex 杠杆多空、借贷利率", ["/api/coinbase-premium-index", "/api/bitfinex-margin-long-short",
                                                  "/api/borrow-interest-rate/history"], "美国资金需求、杠杆", "Coinbase 溢价：后台采集器"),
    ("市场指标", "恐慌贪婪、稳定币市值、山寨季、BTC 占比、期权/期货持仓比、合约/现货成交比、交易所资产透明度",
     ["/api/index/fear-greed-history", "/api/index/stableCoin-marketCap-history", "/api/index/altcoin-season",
      "/api/index/bitcoin-dominance", "/api/index/option-vs-futures-oi-ratio", "/api/futures_spot_volume_ratio",
      "/api/exchange_assets_transparency/list"], "大盘体制", "期权/期货持仓比：后台采集器；恐慌贪婪另有免费源在用"),
    ("比特币指标", "AHR999、彩虹图、Pi 周期、S2F、SOPR、RHODL、NUPL、M2 对比等 26 项",
     ["/api/index/ahr999", "/api/bull-market-peak-indicator", "/api/index/puell-multiple", "/api/index/stock-flow",
      "/api/index/pi-cycle-indicator", "/api/index/bitcoin/rainbow-chart", "/api/index/bitcoin-sth-sopr",
      "/api/index/bitcoin-net-unrealized-profit-loss", "/api/index/bitcoin-vs-global-m2-growth"], "比特币周期", ""),
    ("ETF", "BTC / ETH / SOL / XRP ETF 资金流、净资产、溢价，灰度持仓",
     ["/api/etf/bitcoin/flow-history", "/api/etf/bitcoin/list", "/api/etf/ethereum/flow-history", "/api/etf/solana/flow-history",
      "/api/etf/xrp/flow-history", "/api/hk-etf/bitcoin/flow-history", "/api/grayscale/holdings-list"], "机构资金流", "BTC ETF 资金流：后台采集器"),
    ("其他", "经济日历、新闻", ["/api/calendar/economic-data", "/api/article/list"], "", ""),
]
LSR_ROWS = [
    ("中转站 LSR 专业版", "单币聪明钱 / 大户多空结构", ["LSR /api/lsr/signal/WLD"], "实时，无历史", "10/9 用币安同类数据检验，无信号"),
    ("中转站 LSR 专业版", "批量查询、排行榜（约 740 个币）", ["LSR /api/lsr/signals", "LSR /api/lsr/ranking"], "实时，无历史", "雷达拥挤度卡片在用"),
    ("中转站 LSR 专业版", "资金费率套利", ["LSR /api/lsr/funding-rate/arbitrage"], "实时", ""),
    ("中转站 LSR 专业版", "KOL 共识（只有 5 个大币）", ["LSR /api/lsr/consensus/v1/symbols"], "实时；9/26 起快照过期", "不建议用"),
]
# entitlements read from /api/gateway/account, not probed as endpoints: (category, data, access, use)
ACCOUNT_ROWS = [
    ("中转站", "WebSocket 实时推送：逐笔爆仓、合约行情", "未开通（1 个连接、5 个频道）", "需单独开通；推送不占请求额度"),
    ("中转站", "MCP 接入（/mcp，供 AI 工具直接调用）", "未开通", "需单独开通；数据仍是上面两套"),
]


# endpoints that return every coin in one list: no per-coin alt test needed
ALL_COIN_LISTS = {
    "/api/futures/coins-markets", "/api/futures/coins-price-change", "/api/futures/supported-coins",
    "/api/futures/funding-rate/exchange-list", "/api/futures/funding-rate/accumulated-exchange-list",
    "/api/futures/funding-rate/arbitrage", "/api/futures/liquidation/coin-list", "/api/futures/liquidation/max-pain",
    "/api/futures/rsi/list", "/api/futures/macd/list", "/api/futures/ma/list", "/api/futures/ema/list",
    "/api/futures/avg-true-range/list", "/api/futures/netflow-list", "/api/spot/coins-markets", "/api/spot/supported-coins",
    "/api/spot/netflow-list", "/api/coin/unlock-list", "LSR /api/lsr/ranking",
}
EARLIEST_PLAUSIBLE = "2009-01-01"  # option rows carry expiry fields that parse as 1970


def status(done: Dict[str, Any], keys: List[str]) -> Tuple[str, str, str]:
    oks, denied, missing, errors, firsts, alts = 0, 0, 0, 0, [], []
    for key in keys:
        row = done.get(key) if not key.startswith("LSR ") else next((v for k, v in done.items() if k.startswith(key)), None)
        if row is None:
            missing += 1
            continue
        b = row.get("btc") or {}
        code, success, err = str(b.get("code")), b.get("success"), b.get("error") or ""
        # LSR answers carry success=true or, for health/consensus, a bare object without a code
        ok = code == "0" or success is True or (key.startswith("LSR ") and not err and code in {"None", "0"})
        if ok:
            oks += 1
            params = row.get("btc_params") or {}
            # a depth request from 2018, or an endpoint that returns its whole history unasked
            full_history = params.get("start_time") == START_2018 or (
                "limit" not in params and "start_time" not in params and (b.get("rows") or 0) >= 100)
            if full_history and (b.get("first") or "") >= EARLIEST_PLAUSIBLE:
                firsts.append(b["first"])
            if key in ALT_CONFIRMED:
                alts.append(True)
            elif key in ALL_COIN_LISTS:
                alts.append("all")
        elif code == "40300" or "40300" in err or "权限" in (b.get("msg") or ""):
            denied += 1
        else:
            errors += 1
        a = row.get("alt") or {}
        if a:
            alts.append(str(a.get("code")) == "0" and (a.get("rows") or 0) > 0)
    if missing == len(keys):
        access = "未实测"
    elif oks == len(keys):
        access = "可用"
    elif oks and (oks + denied + errors) < len(keys):
        access = f"可用（{oks}/{len(keys)} 已测）"
    elif oks:
        access = f"部分可用（{oks}/{len(keys)}）"
    elif denied:
        access = "无权限（需专业版）"
    else:
        access = "出错"
    history = min(firsts) if firsts else ""
    per_coin = [a for a in alts if a != "all"]
    if alts and not per_coin:
        alt = "全部币"
    elif per_coin:
        alt = "有" if all(per_coin) else ("部分" if any(per_coin) else "无")
    else:
        alt = ""
    return access, history, alt


def table(done: Dict[str, Any]) -> str:
    lines = ["| 类别 | 数据 | 能否用 | 日线最早 | 山寨币 | 内容 / 用途 | 我们 |", "|---|---|---|---|---|---|---|"]
    for cat, data, keys, use, ours in ROWS + LSR_ROWS:
        access, history, alt = status(done, keys)
        lines.append(f"| {cat} | {data} | {access} | {history} | {alt} | {use} | {ours} |")
    for cat, data, access, use in ACCOUNT_ROWS:
        lines.append(f"| {cat} | {data} | {access} |  |  | {use} |  |")
    return "\n".join(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--table", action="store_true", help="print the markdown table from saved results")
    parser.add_argument("--out", type=Path, default=OUT)
    parser.add_argument("--retry-failed", action="store_true", help="ask again what failed or came back empty")
    args = parser.parse_args(argv)
    if args.table:
        done = json.loads(args.out.read_text(encoding="utf-8")) if args.out.exists() else {}
        print(table(done))
        return 0
    args.out.parent.mkdir(parents=True, exist_ok=True)
    asyncio.run(probe(args.out, retry_failed=args.retry_failed))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
