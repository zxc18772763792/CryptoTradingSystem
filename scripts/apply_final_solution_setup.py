"""Apply the final paper-trading strategy setup via HTTP API."""
from __future__ import annotations

import argparse
import json
from typing import Any, Dict, List

import requests


DEFAULT_BASE_URL = "http://127.0.0.1:8000/api"


def _request_json(method: str, url: str, timeout: float = 10.0, **kwargs) -> Dict[str, Any]:
    resp = requests.request(method=method, url=url, timeout=timeout, **kwargs)
    try:
        payload = resp.json()
    except Exception:
        payload = {"raw": resp.text}
    if resp.status_code >= 400:
        raise RuntimeError(f"{method} {url} failed: {resp.status_code} {payload}")
    return payload


def _build_payloads() -> List[Dict[str, Any]]:
    return [
        {
            "name": "final_vwap_btc_10s",
            "strategy_type": "VWAPReversionStrategy",
            "params": {
                "exchange": "binance",
                "window": 48,
                "entry_deviation_pct": 0.01,
                "exit_deviation_pct": 0.002,
                "stop_loss_pct": 0.02,
                "take_profit_pct": 0.03,
            },
            "symbols": ["BTC/USDT"],
            "timeframe": "10s",
            "exchange": "binance",
            "allocation": 0.35,
        },
        {
            "name": "final_vwap_eth_10s",
            "strategy_type": "VWAPReversionStrategy",
            "params": {
                "exchange": "binance",
                "window": 48,
                "entry_deviation_pct": 0.01,
                "exit_deviation_pct": 0.002,
                "stop_loss_pct": 0.02,
                "take_profit_pct": 0.03,
            },
            "symbols": ["ETH/USDT"],
            "timeframe": "10s",
            "exchange": "binance",
            "allocation": 0.20,
        },
        {
            "name": "final_momentum_eth_30s",
            "strategy_type": "MomentumStrategy",
            "params": {
                "exchange": "binance",
                "lookback_period": 14,
                "momentum_threshold": 0.015,
                "stop_loss_pct": 0.03,
                "take_profit_pct": 0.06,
            },
            "symbols": ["ETH/USDT"],
            "timeframe": "30s",
            "exchange": "binance",
            "allocation": 0.15,
        },
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply final strategy setup to running web API.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="API base URL, e.g. http://127.0.0.1:8000/api")
    parser.add_argument("--no-risk-update", action="store_true", help="Skip risk parameter update")
    parser.add_argument("--keep-existing", action="store_true", help="Do not delete existing same-name strategies")
    args = parser.parse_args()

    base = str(args.base_url or DEFAULT_BASE_URL).rstrip("/")
    status = _request_json("GET", f"{base}/status")
    print("status:", json.dumps(status, ensure_ascii=False))
    if not bool(status.get("paper_trading", False)):
        raise RuntimeError("Refuse to apply setup: current mode is not paper_trading=true")

    if not args.no_risk_update:
        risk_payload = {
            "max_position_size": 0.12,
            "max_daily_loss_ratio": 0.025,
            "max_daily_trades": 400,
            "max_leverage": 2.0,
            "balance_volatility_alert_pct": 0.08,
        }
        risk_resp = _request_json("POST", f"{base}/trading/risk/params", json=risk_payload)
        print("risk_update:", json.dumps(risk_resp, ensure_ascii=False))
        _request_json("POST", f"{base}/trading/risk/reset")

    payloads = _build_payloads()
    listed = _request_json("GET", f"{base}/strategies/list")
    existing = {str(item.get("name")) for item in (listed.get("registered") or [])}

    for item in payloads:
        name = str(item["name"])
        if name in existing and not args.keep_existing:
            _request_json("POST", f"{base}/strategies/{name}/stop")
            _request_json("DELETE", f"{base}/strategies/{name}")

        reg = _request_json("POST", f"{base}/strategies/register", json=item)
        print("register:", json.dumps(reg, ensure_ascii=False))
        started = _request_json("POST", f"{base}/strategies/{name}/start")
        print("start:", json.dumps(started, ensure_ascii=False))

    summary = _request_json("GET", f"{base}/strategies/summary?limit=20")
    print("summary_running_count:", summary.get("running_count"))
    print("summary_stale_count:", summary.get("stale_running_count"))


if __name__ == "__main__":
    main()
