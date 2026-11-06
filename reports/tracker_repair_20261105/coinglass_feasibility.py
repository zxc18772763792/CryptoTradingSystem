"""Read-only execution-sizing diagnostics, not an order plan or return forecast."""
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
import hashlib
import json
from pathlib import Path
from statistics import mean, median
import sys

sys.stdout.reconfigure(encoding="utf-8")
ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from core.research import coinglass_composite_tracker as tracker


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


rules = {r["symbol"]: r for r in read(OUT/"coinglass_market_rules.json")["rows"]}
public = read(OUT/"coinglass_public_quotes.json")
quotes = {r["symbol"]: r for r in public["quotes"]}
marks = {r["symbol"]: r for r in public["marks"]}


def constraints(symbol):
    rule = rules[symbol]
    lot = rule["lot"] or {}
    market = rule["marketLot"] or {}
    step = max(Decimal(lot.get("stepSize", "0")), Decimal(market.get("stepSize", "0")))
    minimum = max(Decimal(lot.get("minQty", "0")), Decimal(market.get("minQty", "0")))
    notional = Decimal((rule.get("minNotional") or {}).get("notional", (rule.get("minNotional") or {}).get("minNotional", "0")))
    mark = Decimal(marks[symbol]["markPrice"])
    required = max(minimum, notional/mark)
    quantity = (required/step).to_integral_value(rounding=ROUND_CEILING)*step if step else required
    quote = quotes[symbol]
    bid, ask = Decimal(quote["bidPrice"]), Decimal(quote["askPrice"])
    return {"symbol": symbol, "mark": float(mark), "step": float(step), "min_quantity": float(quantity),
            "min_order_usdt_at_mark": float(quantity*mark),
            "spread_bps": float((ask-bid)/((ask+bid)/2)*10000),
            "best_bid_usdt": float(bid*Decimal(quote["bidQty"])),
            "best_ask_usdt": float(ask*Decimal(quote["askQty"]))}


result = {"captured_at": datetime.now(timezone.utc).isoformat(), "source": "public exchange rules and saved paper books",
          "scenarios": "Total gross budget E, no leverage assumption: seven tranches, each long E/14 and short E/14.",
          "limits": ["Public-rule sizing estimates are not order validation or an account margin check.",
                     "Price requests are separate snapshots; minimum notional uses the saved mark price.",
                     "No order, account mode, allocation or ledger was changed.",
                     "The equal-tranche order threshold is not a universal minimum capital for a netted executor."]}

for variant, directory in (("main", "coinglass_composite"), ("monthly", "coinglass_composite_monthly_v1")):
    path = ROOT/"data/research"/directory/"tracker.json"
    state = read(path)
    summary = tracker.summary(state, variant)
    opened = [(day, r) for day,r in sorted(state["books"].items()) if (r.get("main") or {}).get("status") == "open"]
    closed = [(day,r["main"]) for day,r in sorted(state["books"].items())
              if (r.get("main") or {}).get("status") == "closed" and not r["main"].get("missing_positions",0)]
    latest_day, latest = opened[-1]
    book = latest["main"]
    positions = [(side,symbol,p) for side in ("longs","shorts") for symbol,p in book[side].items()]
    symbols = {symbol for _,symbol,_ in positions}
    checks = [constraints(s) for s in sorted(symbols)]
    blocked = [s for s in symbols if rules[s]["status"] != "TRADING" or rules[s]["contractType"] != "PERPETUAL"
               or "MARKET" not in rules[s]["orderTypes"]
               or (rules[s].get("positionControl") or {}).get("positionControlSide") not in (None,"NONE")]
    sizes = []
    for capital in (1000, 5000, 10000, 30000):
        failed, deviations = [], []
        for side,symbol,_ in positions:
            target = Decimal(capital)/14/len(book[side])
            c = constraints(symbol)
            price, step = Decimal(str(c["mark"])), Decimal(str(c["step"]))
            qty = (target/price/step).to_integral_value(rounding=ROUND_FLOOR)*step if step else target/price
            if qty < Decimal(str(c["min_quantity"])):
                failed.append(symbol)
            else:
                deviations.append(float(1-qty*price/target))
        sizes.append({"total_gross_usdt":capital, "target_usdt_per_position":capital/14/len(book["longs"]),
                      "below_min_order_positions":len(failed), "failed_symbols":failed,
                      "max_rounding_shortfall_on_valid_orders_pct":100*max(deviations) if deviations else None})
    target_min = max(14*len(book[side])*constraints(symbol)["min_order_usdt_at_mark"] for side,symbol,_ in positions)
    all_min = max(14*len(record["main"][side])*constraints(symbol)["min_order_usdt_at_mark"]
                  for _,record in opened for side in ("longs","shorts") for symbol in record["main"][side])
    quantities = defaultdict(float)
    directions = defaultdict(set)
    virtual_gross = 0.0
    for _,record in opened:
        for side, sign in (("longs",1),("shorts",-1)):
            members = record["main"][side]
            for symbol,position in members.items():
                quantity = sign/len(members)/float(position["entry"])
                quantities[symbol] += quantity
                directions[symbol].add(side)
                virtual_gross += abs(quantity)*float(marks[symbol]["markPrice"])
    net_gross = sum(abs(quantity)*float(marks[symbol]["markPrice"]) for symbol,quantity in quantities.items())
    netting_scenarios = []
    for capital in (1000,5000,10000,30000):
        error_notional, target_notional, omitted = 0.,0.,0
        for symbol, coefficient in quantities.items():
            desired = Decimal(str(abs(coefficient)))*Decimal(capital)/14
            c = constraints(symbol)
            price, step = Decimal(str(c["mark"])), Decimal(str(c["step"]))
            rounded = (desired/step).to_integral_value(rounding=ROUND_FLOOR)*step if step else desired
            if rounded < Decimal(str(c["min_quantity"])):
                rounded = Decimal(0)
                omitted += desired>0
            target_notional += float(desired*price)
            error_notional += float((desired-rounded)*price)
        netting_scenarios.append({"total_gross_budget_usdt":capital,"net_target_gross_usdt":target_notional,
                                  "symbols_omitted_from_flat":omitted,
                                  "gross_weight_tracking_error_pct":100*error_notional/target_notional,
                                  "scope":"Current net snapshot opened from flat; not a daily rollover/fill simulation."})
    closed_rows = [{"day": day, "net_pct_single_leg": b["net_pct"], "cost_pct_single_leg": b.get("cost_pct"),
                    "turnover":b.get("turnover"),"settled_at":b.get("settled_at")} for day,b in closed]
    returns = [r["net_pct_single_leg"] for r in closed_rows]
    result[variant] = {"summary":summary, "state_sha256":hashlib.sha256(path.read_bytes()).hexdigest(),
                       "latest_day":latest_day,"formed_at":latest["formed_at"], "longs":len(book["longs"]),"shorts":len(book["shorts"]),
                       "blocked_latest_symbols":blocked,"order_checks":checks,"capital_scenarios":sizes,
                       "latest_equal_tranche_gross_threshold_usdt":target_min,
                       "all_open_equal_tranche_gross_threshold_usdt":all_min,
                       "mean_net_total_gross_pct":mean(returns)/2 if returns else None,
                       "median_net_single_leg_pct":median(returns) if returns else None,
                       "worst_book_single_leg_pct":min(returns) if returns else None,
                       "closed":closed_rows,
                       "netting_diagnostic":{"open_tranches":len(opened),"virtual_position_slots":sum(len(r["main"][s]) for _,r in opened for s in ("longs","shorts")),
                                             "unique_symbols":len(quantities),"symbols_with_opposed_tranches":sum(len(s)>1 for s in directions.values()),
                                             "virtual_gross_per_unit_daily_leg":virtual_gross,"net_gross_per_unit_daily_leg":net_gross,
                                             "gross_reduction_fraction_if_netted":1-net_gross/virtual_gross},
                       "netting_scenarios":netting_scenarios,
                       "worst_spreads":sorted(checks,key=lambda r:r["spread_bps"],reverse=True)[:5]}

path = OUT/"coinglass_feasibility.json"
path.write_text(json.dumps(result,ensure_ascii=False,indent=2,allow_nan=False),encoding="utf-8")
print(json.dumps({v:{k:r[k] for k in ("latest_day","longs","shorts","blocked_latest_symbols","capital_scenarios",
                                    "latest_equal_tranche_gross_threshold_usdt","all_open_equal_tranche_gross_threshold_usdt",
                                    "mean_net_total_gross_pct","worst_book_single_leg_pct","netting_diagnostic","worst_spreads")}
                  for v,r in result.items() if v in {"main","monthly"}},ensure_ascii=True))
