"""Collect Coinglass's market-wide history - regime indicators, Bitcoin cycle indicators, ETF flows,
options, exchange balances - into data/research/coinglass_macro/ (research only).

Why (2026-10-13): the user asked to gather, as completely as possible, every kind of historical
data locally for later research. These series are not per alt but describe the market around it
(risk appetite, stablecoin liquidity, alt season, Bitcoin's cycle, institutional flows), and each
is one request: about 60 in all. The endpoints and parameters are the ones the 10/11 catalog probe
found working (docs/COINGLASS_DATA_CATALOG.md); every answer is kept as returned, one JSON file per
series with the time it was fetched. Rerunning refetches every series (they are small).

    python scripts/collect_coinglass_macro.py
"""
from __future__ import annotations

import asyncio
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts import coinglass_positioning_study as single  # noqa: E402

ROOT = PROJECT_ROOT / "data" / "research" / "coinglass_macro"
SPACING_SEC = 30.0  # gentle: the full-history collector runs beside it on the same relay budget
# without start/end the relay returns only the newest 1000 daily rows (10/13: the futures/spot volume ratio)
DAILY = {"interval": "1d", "limit": 4500, "start_time": int(datetime(2018, 1, 1, tzinfo=timezone.utc).timestamp() * 1000),
         "end_time": int(datetime.now(timezone.utc).timestamp() * 1000)}
BTC_INDICATORS = (
    "ahr999", "puell-multiple", "pi-cycle-indicator", "golden-ratio-multiplier", "bitcoin/profitable-days",
    "bitcoin/rainbow-chart", "bitcoin/bubble-index", "2-year-ma-multiplier", "200-week-moving-average-heatmap",
    "bitcoin-sth-sopr", "bitcoin-lth-sopr", "bitcoin-sth-realized-price", "bitcoin-lth-realized-price",
    "bitcoin-short-term-holder-supply", "bitcoin-long-term-holder-supply", "bitcoin-rhodl-ratio", "bitcoin-reserve-risk",
    "bitcoin-active-addresses", "bitcoin-new-addresses", "bitcoin-net-unrealized-profit-loss", "bitcoin-correlation",
    "bitcoin-macro-oscillator", "bitcoin-vs-global-m2-growth", "bitcoin-vs-us-m2-growth",
)
# file name -> (path, params)
SERIES: List[Tuple[str, str, Dict[str, Any]]] = [
    ("market_fear_greed", "/api/index/fear-greed-history", {}),
    ("market_stablecoin_mcap", "/api/index/stableCoin-marketCap-history", {}),
    ("market_altcoin_season", "/api/index/altcoin-season", {}),
    ("market_bitcoin_dominance", "/api/index/bitcoin-dominance", {}),
    ("market_option_vs_futures_oi", "/api/index/option-vs-futures-oi-ratio", {}),
    ("market_coinbase_premium", "/api/coinbase-premium-index", DAILY),
    ("market_cgdi_index", "/api/futures/cgdi-index/history", {}),
    ("market_cdri_index", "/api/futures/cdri-index/history", {}),
    ("market_economic_calendar", "/api/calendar/economic-data", {}),
    ("catalog_futures_delisted_pairs", "/api/futures/delisted-exchange-pairs", {}),
    ("catalog_futures_supported_coins", "/api/futures/supported-coins", {}),
    ("catalog_spot_supported_coins", "/api/spot/supported-coins", {}),
    ("catalog_futures_exchange_rank", "/api/futures/exchange-rank", {}),
    *[(f"btc_{name.replace('/', '_').replace('-', '_')}", f"/api/index/{name}", {}) for name in BTC_INDICATORS],
    ("etf_btc_flow", "/api/etf/bitcoin/flow-history", {}),
    ("etf_btc_net_assets", "/api/etf/bitcoin/net-assets/history", {}),
    ("etf_btc_premium_discount", "/api/etf/bitcoin/premium-discount/history", {}),
    ("etf_btc_list", "/api/etf/bitcoin/list", {}),
    ("etf_btc_hk_flow", "/api/hk-etf/bitcoin/flow-history", {}),
    ("etf_eth_flow", "/api/etf/ethereum/flow-history", {}),
    ("etf_eth_net_assets", "/api/etf/ethereum/net-assets/history", {}),
    ("etf_eth_list", "/api/etf/ethereum/list", {}),
    ("etf_sol_flow", "/api/etf/solana/flow-history", {}),
    ("etf_xrp_flow", "/api/etf/xrp/flow-history", {}),
    ("grayscale_holdings", "/api/grayscale/holdings-list", {}),
    *[(f"grayscale_premium_{c.lower()}", "/api/grayscale/premium-history", {"symbol": c}) for c in ("BTC", "ETH")],
    *[(f"options_oi_{c.lower()}", "/api/option/exchange-oi-history", {"symbol": c, "unit": "USD", "range": "all"}) for c in ("BTC", "ETH")],
    *[(f"options_volume_{c.lower()}", "/api/option/exchange-vol-history", {"symbol": c, "range": "all"}) for c in ("BTC", "ETH")],
    *[(f"options_max_pain_{c.lower()}", "/api/option/max-pain", {"exchange": "Deribit", "symbol": c}) for c in ("BTC", "ETH")],
    *[(f"options_info_{c.lower()}", "/api/option/info", {"symbol": c}) for c in ("BTC", "ETH")],
    *[(f"exchange_balance_chart_{c.lower()}", "/api/exchange/balance/chart", {"symbol": c}) for c in ("BTC", "ETH", "USDT")],
    *[(f"exchange_balance_list_{c.lower()}", "/api/exchange/balance/list", {"symbol": c}) for c in ("BTC", "ETH", "USDT")],
    *[(f"futures_spot_volume_ratio_{c.lower()}", "/api/futures_spot_volume_ratio",
       {"symbol": c, "exchange_list": "Binance,OKX,Bybit", **DAILY}) for c in ("BTC", "ETH")],
    *[(f"borrow_rate_binance_{c.lower()}", "/api/borrow-interest-rate/history", {"exchange": "Binance", "symbol": c, **DAILY})
      for c in ("BTC", "ETH", "USDT")],
    *[(f"whale_index_{c.lower()}", "/api/futures/whale-index/history", {"exchange": "Binance", "symbol": f"{c}USDT", **DAILY})
      for c in ("BTC", "ETH")],
]


async def main_async() -> Dict[str, Any]:
    from core.data.coinglass_client import CoinglassBudgetExceeded, CoinglassClient, CoinglassError

    ROOT.mkdir(parents=True, exist_ok=True)
    summary: Dict[str, Any] = {}
    async with CoinglassClient() as client:
        for i, (name, path, params) in enumerate(SERIES):
            if i:
                await asyncio.sleep(SPACING_SEC)
            resp = None
            for _ in range(8):
                try:
                    resp = await client.request_json(path, params=params)
                    break
                except CoinglassBudgetExceeded:
                    await asyncio.sleep(20)
                except CoinglassError as exc:
                    if "429" not in str(exc):
                        summary[name] = f"error: {str(exc)[:100]}"
                        break
                    await asyncio.sleep(75)
                except Exception as exc:  # noqa: BLE001 - a timeout: asked once more, then recorded
                    summary[name] = f"{type(exc).__name__}: {str(exc)[:100]}"
                    await asyncio.sleep(15)
            payload = (resp or {}).get("payload") if isinstance(resp, dict) else None
            if not isinstance(payload, dict):
                summary.setdefault(name, "no answer")
                continue
            data = payload.get("data")
            size = len(data) if isinstance(data, (list, dict)) else 0
            (ROOT / f"{name}.json").write_text(json.dumps({"path": path, "params": params, "fetched_at": datetime.now(timezone.utc).isoformat(),
                                                           "code": payload.get("code"), "data": data}, ensure_ascii=False),
                                               encoding="utf-8")
            summary[name] = f"code {payload.get('code')}, {size} items"
            print(f"[{i + 1}/{len(SERIES)}] {name}: {summary[name]}", flush=True)
    (ROOT / "_summary.json").write_text(json.dumps({"fetched_at": datetime.now(timezone.utc).isoformat(), "series": summary},
                                                   ensure_ascii=False, indent=1), encoding="utf-8")
    return summary


def main() -> int:
    started = time.monotonic()
    summary = asyncio.run(main_async())
    bad = {k: v for k, v in summary.items() if not str(v).startswith("code 0")}
    print(f"{len(summary) - len(bad)}/{len(summary)} series saved in {(time.monotonic() - started) / 60:.0f} min; problems: {bad}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
