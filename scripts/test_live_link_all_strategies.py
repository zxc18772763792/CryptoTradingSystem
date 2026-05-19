"""Per-strategy live trading link smoke test.

For every registered strategy this script dispatches ONE synthetic, deeply
non-marketable BUY limit order through the real execution pipeline
(``execution_engine.execute_signal``) on the Binance main *live* account,
confirms the exchange acknowledged it, then immediately cancels it.

Safety model
------------
* Order is a LIMIT order priced at ``LIMIT_FACTOR`` x current price (default
  0.5) so it can never fill at market.
* Quantity is forced to the exchange minimum (``respect_quantity``) and the
  notional is hard-capped by ``MAX_NOTIONAL_USD``; the run aborts if exceeded.
* Strategies are processed serially; each order is cancelled before the next.
* If any order reports a non-zero fill the run aborts immediately and attempts
  to flatten the accidental position.
* DRY RUN by default. Real orders only when env ``CONFIRM_LIVE=1``.

Usage
-----
    python scripts/test_live_link_all_strategies.py            # dry run
    CONFIRM_LIVE=1 python scripts/test_live_link_all_strategies.py   # live
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.strategy_registry import STRATEGY_REGISTRY
from core.strategies.strategy_base import Signal, SignalType
from core.trading.execution_engine import execution_engine
from core.trading.order_manager import order_manager
from core.exchanges.exchange_manager import exchange_manager

SYMBOL = os.environ.get("LINK_TEST_SYMBOL", "BTC/USDT")
EXCHANGE = "binance"
ACCOUNT_ID = "main"
LIMIT_FACTOR = 0.5            # buy limit at 50% of market -> never fills
MAX_NOTIONAL_USD = 210.0      # hard ceiling per order; abort if exceeded
MIN_NOTIONAL_USD = 100.0      # baseline; raised to exchange MIN_NOTIONAL filter
LIVE = os.environ.get("CONFIRM_LIVE") == "1"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _resolve_price_and_qty(connector) -> tuple[float, float, float]:
    ticker = await connector.get_ticker(SYMBOL)
    price = float(getattr(ticker, "last", 0.0) or 0.0)
    if price <= 0:
        raise RuntimeError(f"could not resolve {SYMBOL} price")

    limit_price = round(price * LIMIT_FACTOR, 2)

    market_min = 0.0
    cost_min = 0.0
    decimals = 3
    client = getattr(connector, "_client", None)
    if client is not None:
        try:
            market = client.market(SYMBOL)
            limits = market.get("limits") or {}
            market_min = float(((limits.get("amount") or {}).get("min")) or 0.0)
            cost_min = float(((limits.get("cost") or {}).get("min")) or 0.0)
            prec = (market.get("precision") or {}).get("amount")
            if isinstance(prec, int):
                decimals = max(0, prec)
            elif prec:
                pv = float(prec)
                decimals = 0 if pv >= 1 else max(0, int(round(-math.log10(pv))))
        except Exception:
            pass

    # Binance MIN_NOTIONAL is evaluated at the order (limit) price. Use the
    # exchange-reported cost.min when available, else the configured baseline,
    # plus a 2% buffer so precision rounding cannot drop below the filter.
    min_notional = max(MIN_NOTIONAL_USD, cost_min) * 1.02
    qty_for_notional = min_notional / limit_price
    qty = max(market_min, qty_for_notional)
    print(f"exchange filters: min_amount={market_min} cost_min={cost_min} "
          f"applied_min_notional={min_notional:.2f}")
    step = 10 ** (-decimals)
    qty = math.ceil(qty / step) * step
    qty = round(qty, decimals)

    notional = qty * limit_price
    if notional > MAX_NOTIONAL_USD:
        raise RuntimeError(
            f"computed notional {notional:.2f} USDT exceeds cap "
            f"{MAX_NOTIONAL_USD} -- aborting for safety"
        )
    if limit_price >= price * 0.6:
        raise RuntimeError("limit price not safely below market -- aborting")
    return price, limit_price, qty


def _build_signal(strategy_name: str, limit_price: float, qty: float) -> Signal:
    return Signal(
        symbol=SYMBOL,
        signal_type=SignalType.BUY,
        price=limit_price,
        timestamp=datetime.now(timezone.utc),
        strategy_name=strategy_name,
        strength=0.5,
        quantity=qty,
        metadata={
            "mode": "live",
            "account_id": ACCOUNT_ID,
            "order_type": "limit",
            "respect_quantity": True,
            "market_type": "future",
            "leverage": 1,
            "link_test": True,
        },
    )


async def _flatten_emergency(connector, strategy_name: str) -> None:
    print(f"  !! UNEXPECTED FILL on {strategy_name} -- attempting flatten")
    try:
        sig = Signal(
            symbol=SYMBOL,
            signal_type=SignalType.CLOSE_LONG,
            price=0.0,
            timestamp=datetime.now(timezone.utc),
            strategy_name=strategy_name,
            strength=1.0,
            metadata={"mode": "live", "account_id": ACCOUNT_ID, "market_type": "future"},
        )
        await execution_engine.execute_signal(sig)
    except Exception as exc:  # noqa: BLE001
        print(f"  !! flatten failed: {exc} -- MANUAL INTERVENTION REQUIRED")


async def _sweep_residual_orders(connector) -> list[str]:
    cancelled = []
    try:
        open_orders = await order_manager.get_open_orders(symbol=SYMBOL, exchange=EXCHANGE)
        for o in open_orders:
            ok = await order_manager.cancel_order(o.id, SYMBOL, EXCHANGE)
            cancelled.append(f"{o.id}:{'ok' if ok else 'FAIL'}")
    except Exception as exc:  # noqa: BLE001
        print(f"residual sweep error: {exc}")
    return cancelled


async def main() -> int:
    print(f"=== Live link smoke test ({'LIVE' if LIVE else 'DRY RUN'}) @ {_now()} ===")
    print(f"symbol={SYMBOL} exchange={EXCHANGE} account={ACCOUNT_ID} "
          f"limit_factor={LIMIT_FACTOR} max_notional={MAX_NOTIONAL_USD}")

    await exchange_manager.initialize(["binance"])
    connector = exchange_manager.get_exchange(EXCHANGE)
    if connector is None:
        try:
            connector = await exchange_manager.ensure_exchange(EXCHANGE, account_id=ACCOUNT_ID)
        except TypeError:
            connector = await exchange_manager.ensure_exchange(EXCHANGE)
    if connector is None:
        print("FATAL: no Binance connector (check live credentials in .env)")
        return 2

    price, limit_price, qty = await _resolve_price_and_qty(connector)
    notional = qty * limit_price
    print(f"market={price} limit={limit_price} qty={qty} notional~={notional:.2f} USDT")

    strategies = list(STRATEGY_REGISTRY.keys())
    limit = int(os.environ.get("LINK_TEST_LIMIT", "0") or 0)
    if limit > 0:
        strategies = strategies[:limit]
    print(f"strategies to test: {len(strategies)}")

    if not LIVE:
        print("\nDRY RUN -- no orders will be placed. Per-strategy plan:")
        for name in strategies:
            print(f"  {name}: BUY {SYMBOL} limit {limit_price} x {qty} (mode=live)")
        print("\nSet CONFIRM_LIVE=1 to execute for real.")
        return 0

    results: list[dict] = []
    aborted = False

    # Force the shared singletons into live mode for the whole run.
    execution_engine.set_paper_trading(False)
    order_manager.set_paper_trading(False)
    try:
        for idx, name in enumerate(strategies, 1):
            row = {"strategy": name, "ts": _now()}
            print(f"[{idx}/{len(strategies)}] {name} ...", flush=True)
            try:
                signal = _build_signal(name, limit_price, qty)
                result = await execution_engine.execute_signal(signal)
                if not result:
                    row["status"] = "no_order"
                    row["detail"] = (
                        execution_engine._signal_diagnostics.get("last_result")
                    )
                    results.append(row)
                    print(f"  -> no order ({row['detail']})")
                    continue

                order = result.get("order") or {}
                order_id = order.get("id")
                filled = float(result.get("executed_quantity") or 0.0)
                row["order_id"] = order_id
                row["order_status"] = order.get("status")
                row["filled"] = filled

                if filled > 0:
                    row["status"] = "UNEXPECTED_FILL"
                    results.append(row)
                    await _flatten_emergency(connector, name)
                    aborted = True
                    print("  !! aborting remaining strategies after unexpected fill")
                    break

                cancelled = False
                if order_id:
                    cancelled = await order_manager.cancel_order(order_id, SYMBOL, EXCHANGE)
                row["cancelled"] = cancelled
                row["status"] = "ok" if (order_id and cancelled) else "ack_no_cancel"
                results.append(row)
                print(f"  -> order {order_id} status={order.get('status')} "
                      f"cancelled={cancelled}")
            except Exception as exc:  # noqa: BLE001
                row["status"] = "exception"
                row["error"] = str(exc)
                results.append(row)
                print(f"  -> EXCEPTION: {exc}")
            await asyncio.sleep(0.4)
    finally:
        residual = await _sweep_residual_orders(connector)
        execution_engine.set_paper_trading(True)
        order_manager.set_paper_trading(True)

    ok = sum(1 for r in results if r.get("status") == "ok")
    summary = {
        "generated_at": _now(),
        "live": LIVE,
        "symbol": SYMBOL,
        "total": len(strategies),
        "ok": ok,
        "aborted": aborted,
        "residual_orders_cancelled": residual,
        "results": results,
    }
    out_path = ROOT / "output" / "live_link_test_report.json"
    out_path.parent.mkdir(exist_ok=True)
    out_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"\n=== SUMMARY: {ok}/{len(strategies)} link OK, aborted={aborted} ===")
    for r in results:
        if r.get("status") != "ok":
            print(f"  [{r.get('status')}] {r['strategy']}: "
                  f"{r.get('detail') or r.get('error') or r.get('order_status')}")
    print(f"report written: {out_path}")
    return 0 if (ok == len(strategies) and not aborted) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
