from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scripts.research_telegram_signal_strategy import (
    choose_book, cryptoquant_signal, entry_time, equity_curve, evaluate, funding_cost, utc,
)


class Market:
    def __init__(self, frame, funding=None):
        self.price = frame
        self.funding = funding if funding is not None else pd.DataFrame()

    def frame(self, _symbol, kind="1h"):
        return self.price if kind == "1h" else self.funding


def bars():
    index = pd.date_range("2026-01-01", periods=60, freq="h", tz="UTC")
    return pd.DataFrame({"open":100.,"high":101.,"low":99.,"close":100.,"volume":1000.},index=index)


def signal(time="2026-01-01T00:30:00Z", direction=1, source_id="a"):
    return {"source_id":source_id,"symbol":"BTCUSDT","direction":direction,"published_at":time}


def test_entry_uses_next_full_bar_after_latency_not_publication_bar():
    assert entry_time("2026-01-01T00:30:00Z") == utc("2026-01-01T01:00:00Z")
    assert entry_time("2026-01-01T00:59:30Z") == utc("2026-01-01T02:00:00Z")


def test_gap_is_not_filled_or_silently_traded():
    price = bars().drop(utc("2026-01-01T03:00:00Z"))
    assert evaluate(signal(),Market(price),6)["status"] == "price_coverage_gap"


def test_funding_side_and_mark_price_notional_are_used():
    frame=pd.DataFrame({"funding_rate":[.001,.001,.001],"mark_price":[100.,110.,100.]},
                       index=pd.to_datetime(["2026-01-01T00:00:00Z","2026-01-01T08:00:00Z","2026-01-01T16:00:00Z"]))
    cost,_,error=funding_cost(frame,utc("2026-01-01T01:00:00Z"),utc("2026-01-01T09:00:00Z"),100.,1)
    assert error is None and cost == pytest.approx(-.0011)
    benefit,_,_=funding_cost(frame,utc("2026-01-01T01:00:00Z"),utc("2026-01-01T09:00:00Z"),100.,-1)
    assert benefit == pytest.approx(.0011)


def test_missing_funding_is_unknown_not_zero():
    result=evaluate(signal(),Market(bars()),6)
    assert result["net_return"] is None
    assert result["net_before_funding"] == pytest.approx(-.002)


def test_large_adverse_short_move_counts_as_full_collateral_loss():
    price=bars(); price.loc[utc("2026-01-01T03:00:00Z"),"high"]=210.
    result=evaluate(signal(direction=-1),Market(price),24)
    assert result["liquidated_1x_approx"]
    assert result["net_before_funding"] == pytest.approx(-1.002)
    assert utc(result["exit_at"]) == utc("2026-01-01T04:00:00Z")


def test_overlap_not_counted_as_independent_and_drawdown_includes_open_loss():
    price=bars(); price.loc[utc("2026-01-01T03:00:00Z"),"open"]=80.
    market=Market(price)
    a=evaluate(signal(),market,6)
    b=evaluate(signal(time="2026-01-01T02:30:00Z",source_id="b"),market,6)
    book,skips=choose_book([a,b],market,funded=False)
    assert len(book)==1 and skips["overlapping_symbol"]==1
    curve=equity_curve(book,market,funded=False)
    assert min(x["equity"] for x in curve)<.961
    assert curve[-1]["equity"] == pytest.approx(1-.2*.002)


def test_cryptoquant_exchange_tag_is_not_traded_as_asset_and_internal_correction_skipped():
    message={"source_id":"cq1","url":"https://t.me/cryptoquant_alert/1","published_at":"2026-01-01T00:00:00Z",
             "payload":{},"content":"1,500 #BTC aggregated inflows to #Binance"}
    parsed=cryptoquant_signal(message)
    assert parsed["symbol"]=="BTCUSDT" and parsed["direction"]==-1
    message["content"] = "It was an internal transfer. The #BTC alert was inaccurate."
    assert cryptoquant_signal(message) is None


def test_cryptoquant_outflow_disclaimer_does_not_delete_the_signal():
    message={"source_id":"cq2","url":"https://t.me/cryptoquant_alert/2","published_at":"2026-01-01T00:00:00Z",
             "payload":{},"content":"Unusual Exchange Outflows: 1,500 #BTC aggregated outflows from multiple exchanges. Please note that it could be an internal transfer."}
    parsed=cryptoquant_signal(message)
    assert parsed["symbol"]=="BTCUSDT" and parsed["direction"]==1
    assert parsed["has_uncertainty_disclaimer"] is True
    message["content"]="1,500 #USDT aggregated inflows to #Binance"
    assert cryptoquant_signal(message) is None
