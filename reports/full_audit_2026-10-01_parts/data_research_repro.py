"""Offline audit reproductions. Executes extracted definitions; no settings/.env/DB/network imports."""
from __future__ import annotations

import ast
import asyncio
import contextlib
import json
import logging
import math
import re
import hashlib
import sys
import time
import threading
import types
import uuid
from abc import ABC, abstractmethod
from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from dateutil import parser as dt_parser
from pathlib import Path
from typing import *

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]


def definitions(path, extra=None):
    ns = dict(globals())
    ns.update(extra or {})
    tree = ast.parse((ROOT / path).read_text(encoding="utf-8-sig"))
    tree.body = [ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)] + [
        node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    ]
    exec(compile(ast.fix_missing_locations(tree), str(ROOT / path), "exec"), ns)
    return ns


class FakePMDB:
    def __init__(self):
        self.cash = 1000.0
        self.orders = {}
        self.fills = []
        self.size = 0.0
        self.fail_update_once = False

    async def get_or_create_paper_account(self, *args, **kwargs):
        return {"cash": self.cash}

    async def list_paper_positions(self, *args, **kwargs):
        return [] if self.size == 0 else [{"token_id": "yes", "size": self.size, "avg_price": 0.5}]

    async def list_paper_orders(self, *args, **kwargs):
        return [dict(o) for o in self.orders.values() if o["status"] == "OPEN"]

    async def create_paper_order(self, order):
        order = dict(order, filled_size=0.0, avg_fill_price=None, fee_paid=0.0)
        self.orders[order["order_id"]] = order
        return dict(order)

    async def get_paper_order(self, oid):
        return dict(self.orders[oid])

    async def get_latest_quote(self, *args):
        return {"ask": 0.5, "bid": 0.49, "ts": datetime(2020, 1, 1, tzinfo=timezone.utc)}

    async def record_paper_fill(self, fill):
        self.fills.append(dict(fill))
        return dict(fill)

    async def apply_paper_fill_to_account(self, fill):
        self.cash -= fill["price"] * fill["size"] + fill["fee_paid"]
        self.size += fill["size"]

    async def update_paper_order(self, oid, updates):
        if self.fail_update_once:
            self.fail_update_once = False
            raise RuntimeError("simulated process failure after fill/account commits")
        self.orders[oid].update(updates)
        return dict(self.orders[oid])


async def main():
    out = {}
    common = definitions("core/news/collectors/common.py")
    news_rows = [{"id": str(i), "title": f"Audit unique article {i}", "source": "test", "url": f"https://example.invalid/{i}",
                  "published_at": datetime(2025, 1, 1, 0, i, tzinfo=timezone.utc).isoformat()} for i in range(16)]
    class FakeNewsDB:
        cursor = None
        calls = []
        async def get_source_state(self, name):
            return {"cursor_value": self.cursor}
        async def set_source_state(self, name, **kw):
            self.cursor = kw.get("cursor_value", self.cursor)
            self.calls.append("cursor_committed")
            return {"cursor_value": self.cursor}
    newsdb = FakeNewsDB()
    manager_ns = definitions("core/news/collectors/manager.py", {"news_db": newsdb, "logger": logging.getLogger("audit"),
                            "junk_reason": lambda item: None})
    manager_cls = manager_ns["MultiSourceNewsCollector"]
    manager = object.__new__(manager_cls)
    manager.defaults = {}
    manager._build_collectors = lambda source_names=None: ([types.SimpleNamespace(name="test", collector=None)], [])
    manager._close_collectors = lambda specs: None
    async def fake_source(**kwargs):
        items = common["BaseNewsCollector"].filter_incremental(news_rows, kwargs["cursor"])
        return items, common["BaseNewsCollector"].build_ts_cursor(news_rows, kwargs["cursor"])
    manager._run_incremental_source_pull = fake_source
    first = await manager.pull_latest_incremental(max_records=10)
    second = await manager.pull_latest_incremental(max_records=10)
    out["news_cursor_before_save_and_truncation"] = {"source_rows": 16, "first_saved_candidates": len(first["items"]),
        "second_saved_candidates": len(second["items"]), "permanently_omitted": 16-len(first["items"]), "cursor_write_before_caller_can_save": newsdb.calls}

    monitor = definitions("core/monitoring/strategy_monitor.py")
    short_result = monitor["detect_strategy_decay"](returns=[-.1] * 12, min_bars=20)
    out["cusum_short_warmup"] = {"n_bars": short_result["n_bars"], "min_bars": 20, "triggered": short_result["triggered"]}
    ml = definitions("core/ml/pipeline.py", {"FEATURE_COLUMNS": [
        "rsi", "macd_pct", "macd_signal_pct", "macd_hist_pct", "ema_fast_gap", "ema_slow_gap",
        "bb_position", "bb_width", "atr_pct", "volume_ratio", "momentum", "ret_1", "ret_4", "body_pct", "range_pct"
    ], "FEATURE_SET_VERSION": "ml_signal_v2", "MODEL_FILE_NAME": "model.json",
        "MANIFEST_FILE_NAME": "manifest.json", "METRICS_FILE_NAME": "metrics.json",
        "FEATURE_IMPORTANCES_FILE_NAME": "feature_importances.json", "MAX_ONE_SIDE_SHARE": 0.9})
    idx = pd.date_range("2025-01-01", periods=200, freq="h", tz="UTC")
    price = 100 + np.arange(200) + np.sin(np.arange(200))
    data = pd.DataFrame({"open": price - .1, "high": price + 1, "low": price - 1,
                         "close": price, "volume": 100 + np.arange(200)}, index=idx)
    ds = ml["build_dataset"](data, forward_bars=4)
    split = ml["split_dataset"](ds, test_size=.2)
    first_test = split.X_test.index[0]
    leaking = [(t.isoformat(), idx[idx.get_loc(t) + 4].isoformat()) for t in split.X_train.index
               if idx[idx.get_loc(t) + 4] >= first_test]
    out["single_symbol_label_leakage"] = {"first_test": first_test.isoformat(), "training_labels_using_test_closes": leaking}

    fake = FakePMDB()
    pm = definitions("prediction_markets/polymarket/paper_trading.py", {"pm_db": fake, "utc_now": lambda: datetime.now(timezone.utc)})
    trader = pm["PolymarketPaperTrader"](limits=pm["PaperRiskLimits"](max_order_notional=50, max_position_notional=60))
    await trader.place_limit(market_id="m", token_id="yes", price=.5, size=100, client_order_id="one", fill_immediately=False)
    await trader.place_limit(market_id="m", token_id="yes", price=.5, size=100, client_order_id="two", fill_immediately=False)
    out["position_limit_missing_buy_reservations"] = {"accepted_open_buy_notional": 100, "configured_position_limit": 60}
    fake.fail_update_once = True
    try:
        await trader.try_fill_order("one")
    except RuntimeError:
        pass
    await trader.try_fill_order("one")
    out["non_atomic_fill_retry"] = {"order_filled_size": fake.orders["one"]["filled_size"], "position_size": fake.size,
                                   "fill_count": len(fake.fills), "cash": fake.cash}
    out["stale_quote_filled"] = {"quote_timestamp": "2020-01-01T00:00:00Z", "order_status": fake.orders["one"]["status"]}

    worker = definitions("prediction_markets/polymarket/worker.py", {"utc_now": lambda: datetime.now(timezone.utc)})
    market = {"payload": {"market": {"bestBid": .79, "bestAsk": .81, "lastTradePrice": .80, "updatedAt": "2020-01-01"}}}
    yes = worker["_fallback_quote_from_market_snapshot"]({"market_id": "m", "token_id": "yes", "outcome": "YES"}, market)
    no = worker["_fallback_quote_from_market_snapshot"]({"market_id": "m", "token_id": "no", "outcome": "NO"}, market)
    out["gamma_fallback"] = {"yes_price": yes["price"], "no_price": no["price"], "market_updatedAt": "2020-01-01", "quote_ts": yes["ts"].isoformat()}

    bt = definitions("core/backtest/backtest_engine.py", {"logger": logging.getLogger("audit"), "cost_models": types.SimpleNamespace()})
    cfg = bt["BacktestConfig"](initial_capital=1000, position_size_pct=.1, commission_rate=.001, slippage=0)
    engine = bt["BacktestEngine"](cfg)
    engine._fee_rate = lambda signal=None: .001
    engine._slippage_rate = lambda window=None: 0
    signal = types.SimpleNamespace(symbol="BTC/USDT", strategy_name="audit", stop_loss=None, take_profit=None, metadata={})
    await engine._execute_buy(signal, 100, datetime.now(timezone.utc), None)
    await engine._close_position("BTC/USDT", 100.15, datetime.now(timezone.utc), "long", None)
    engine._update_positions(100.15, "BTC/USDT")
    engine._equity_curve = [1000, engine._equity]
    result = engine._calculate_result()
    out["entry_fee_trade_metrics"] = {"portfolio_pnl": result.total_return, "reported_net_pnl": result.cost_breakdown["net_pnl"],
                                     "winning_trades": result.winning_trades, "fee_cost": result.cost_breakdown["fee"]}
    pa = definitions("core/backtest/performance_analyzer.py")
    analyzer = pa["PerformanceAnalyzer"](risk_free_rate=0)
    out["performance_analyzer_hourly_annualization"] = {
        "reported_10pct_over_365_hourly_bars": analyzer._annualize_return(.1, 365),
        "correct_365days_hourly": (1.1 ** (8760 / 365)) - 1,
        "reported_calmar_at_10pct_return_5pct_drawdown": analyzer._calculate_calmar(.1, 5.0),
        "correct_calmar": 2.0}

    logger = types.SimpleNamespace(debug=lambda *a, **kw: None)
    hubs = definitions("core/marketdata/hub.py", {"logger": logger})
    hub = hubs["MarketDataHub"]()
    old_ts = datetime(2020, 1, 1, tzinfo=timezone.utc)
    hub.upsert_ws_tick("binance", "BTC/USDT", {"last": 100, "timestamp": old_ts})
    hub.upsert_ws_tick("binance", "BTC/USDT", {"last": 100, "timestamp": old_ts})
    providers = definitions("core/marketdata/runtime_price_provider.py", {
        "MarketDataHub": hubs["MarketDataHub"], "market_data_hub": hub,
        "normalize_exchange_name": hubs["normalize_exchange_name"],
        "normalize_market_symbol": hubs["normalize_market_symbol"]})
    stale_read = await providers["get_realtime_price"]("binance", "BTC/USDT", hub=hub, allow_rest_fallback=False, fail_closed=True)
    hub.upsert_ws_tick("binance", "ETH/USDT", {"last": float("inf")})
    inf_read = await providers["get_realtime_price"]("binance", "ETH/USDT", hub=hub, allow_rest_fallback=False, fail_closed=True)
    out["hub_invalid_freshness"] = {"old_exchange_ts": old_ts.isoformat(), "read_ok": stale_read.ok,
                                   "age_ms": stale_read.age_ms, "inf_read_ok": inf_read.ok, "inf_price": str(inf_read.price)}

    program = definitions("core/research/strategy_program.py")
    source_data = pd.DataFrame({"close": [100, 100, 100], "flow": [np.nan, np.nan, 99]})
    spec = types.SimpleNamespace(source="flow", kind="price", period=1)
    full = program["_series_for_indicator"](source_data, spec)
    prefix = program["_series_for_indicator"](source_data.iloc[:2], spec)
    missing = program["_series_for_indicator"](source_data.drop(columns="flow"), spec)
    out["dsl_future_and_missing_source"] = {"full_prefix": full.iloc[:2].tolist(), "prefix_only": prefix.tolist(),
                                            "missing_flow_reads_close": missing.tolist()}

    factor_base = definitions("core/factors_ts/base.py")
    factors = definitions("core/factors_ts/impl.py", {"TimeSeriesFactor": factor_base["TimeSeriesFactor"]})
    cache = definitions("core/factors_ts/cache.py", {"logger": logger, "_REL_TOL": 0, "_RECHECK_EVERY": 500,
        "_local": threading.local(), "_STORE_MAX": 512, "_store": OrderedDict(), "_store_status": OrderedDict(),
        "_store_lock": threading.Lock()})
    a = pd.DataFrame({"close": [100, 100, 100], "high": [101, 101, 101], "low": [99, 99, 99]}, index=idx[:3])
    b = a.copy()
    b.loc[b.index[1], "high"] = 121
    def compute(name, df, params):
        return factors["SpreadProxyFactor"]()(df)
    with cache["factor_cache_scope"](a):
        cache["try_cached"]("spread_proxy", a.iloc[:1], None, compute)
        cache["try_cached"]("spread_proxy", a.iloc[:2], None, compute)
    with cache["factor_cache_scope"](b):
        cache["try_cached"]("spread_proxy", b.iloc[:1], None, compute)
        wrong = cache["try_cached"]("spread_proxy", b.iloc[:2], None, compute)
    out["factor_cache_collision"] = {"fingerprints_equal": cache["_fingerprint"](a) == cache["_fingerprint"](b),
                                     "cached": float(wrong.iloc[-1]), "actual": float(compute("spread_proxy", b.iloc[:2], None).iloc[-1])}

    router = definitions("core/ai/live_decision_router.py", {"_SUPPORTED_ACTIONS": {"allow", "block", "reduce_only"},
        "resolve_provider_for_runtime_capability": lambda **kw: {},
        "resolve_runtime_eligibility_context": lambda **kw: {"available": True}})
    obj = object.__new__(router["LiveAIDecisionRouter"])
    obj.get_runtime_config = lambda: {"enabled": True, "mode": "enforce", "provider": "openai", "fail_open": False}
    obj._build_prompt = lambda payload: ("", "")
    async def malformed(**kwargs):
        return {"action": "BLOCKED", "reason": "malformed block spelling"}
    obj._call_provider = malformed
    decision = await obj.evaluate_signal(trading_mode="live", strategy="test", symbol="BTC/USDT", signal_type="buy",
                                         signal_strength=1, price=100, account_equity=1000, order_value=10, leverage=1)
    out["live_router_invalid_action"] = {"fail_open": False, "mode": "enforce", "provider_action": "BLOCKED",
                                         "allowed": decision["allowed"], "action": decision["action"]}
    payload = json.dumps(out, ensure_ascii=False, indent=2)
    (Path(__file__).parent / "data_research_repro.json").write_text(payload + "\n", encoding="utf-8")
    print(payload)


if __name__ == "__main__":
    asyncio.run(main())
