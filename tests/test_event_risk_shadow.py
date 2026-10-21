from __future__ import annotations

import asyncio
import copy
from datetime import datetime, timedelta, timezone

import pytest

from core.research import event_risk_shadow as risk
from core.research import upbit_caution_tracker as korean

START = datetime(2026, 10, 21, tzinfo=timezone.utc)
HOUR = 3_600_000


class Response:
    def __init__(self, payload, status=200):
        self.payload, self.status_code = payload, status

    def json(self):
        return self.payload


class Market:
    def __init__(self):
        self.now = START
        self.prices = {"AAAUSDT": 100.0, "BTCUSDT": 100.0}
        self.rate = 0.0
        self.interval_hours = 1
        self.next_ms = int(START.timestamp() * 1000) + HOUR
        self.rows = []
        self.funding_down = False
        self.missing_rate = False
        self.explicit_interval = True
        self.stale = False
        self.calls = []

    async def get(self, url, params=None):
        params = params or {}
        name = url.rsplit("/", 1)[-1]
        self.calls.append((name, copy.deepcopy(params)))
        now_ms = int(self.now.timestamp() * 1000)
        if url.endswith("ticker/bookTicker"):
            symbol = params["symbol"]
            return Response({"symbol": symbol, "bidPrice": self.prices[symbol], "askPrice": self.prices[symbol],
                             "time": now_ms - (60_000 if self.stale else 0)})
        if name == "premiumIndex":
            interval = self.interval_hours * HOUR
            next_ms = self.next_ms + max(0, (now_ms - self.next_ms) // interval + 1) * interval
            return Response({"symbol": params["symbol"], "lastFundingRate": None if self.missing_rate else self.rate,
                             "nextFundingTime": next_ms, "time": now_ms})
        if name == "fundingInfo":
            return Response([{"symbol": s, "fundingIntervalHours": self.interval_hours} for s in self.prices]
                            if self.explicit_interval else [])
        if name == "fundingRate":
            if self.funding_down:
                return Response({}, 503)
            if "startTime" not in params:
                return Response([{"fundingTime": self.next_ms - k * self.interval_hours * HOUR, "fundingRate": self.rate}
                                 for k in (2, 1)])
            return Response([r for r in self.rows if params["startTime"] <= r["fundingTime"] <= params["endTime"]])
        raise AssertionError(url)


def source(**changes):
    result = {"source_id": "AAA|notice1", "symbol": "AAAUSDT", "source_entry_ms": int(START.timestamp() * 1000),
              "source_exit_due_ms": int((START + timedelta(hours=12)).timestamp() * 1000),
              "source_status": "waiting_entry", "discovered_at": (START - timedelta(hours=1)).isoformat(),
              "eligible": True}
    result.update(changes)
    return result


def registered():
    state = {"trades": {"baseline": {"unchanged": True}}, "retirement_rule": {"frozen": True}}
    risk.register(state, START - timedelta(minutes=5))
    return state


def run(market, state, sources=None):
    return asyncio.run(risk.apply_to_tracker(market, state, sources or [source()], market.now, clock=lambda: market.now))


def row(state):
    return state[risk.STATE_KEY]["trades"]["AAA|notice1"]


def test_prospective_registration_freezes_config_and_excludes_only_entered(monkeypatch):
    market, state = Market(), {"trades": {}}
    old = source(source_id="old", source_entry_ms=int((START - timedelta(days=1)).timestamp() * 1000), source_status="open")
    future = source(source_entry_ms=int((START + timedelta(minutes=10)).timestamp() * 1000))
    run(market, state, [old, future])
    assert state[risk.STATE_KEY]["excluded_source_ids"] == ["old"]
    assert not state[risk.STATE_KEY]["trades"]
    monkeypatch.setitem(risk.CONFIG, "total_loss_budget_pct", 99)
    market.now += timedelta(minutes=10)
    run(market, state, [old, future])
    assert row(state)["status"] == "open"
    assert state[risk.STATE_KEY]["config"]["total_loss_budget_pct"] == 10
    assert row(state)["source_entry_ms"] == future["source_entry_ms"]
    assert row(state)["entry_ms"] >= future["source_entry_ms"]
    assert "old" not in state[risk.STATE_KEY]["trades"]


@pytest.mark.parametrize("explicit", [True, False])
def test_funding_gate_uses_actual_hourly_schedule_not_eight_hour_assumption(explicit):
    market, state = Market(), registered()
    market.rate, market.explicit_interval = -0.002, explicit
    run(market, state)
    record = row(state)
    assert record["status"] == "funding_rejected"
    assert record["funding_gate"]["projected_adverse_pct"] == pytest.approx(2.4)
    ref = record["funding_gate"]["references"]["AAAUSDT"]
    assert ref["interval_ms"] == HOUR and ref["projected_payments"] == 12
    assert not any(name == "bookTicker" for name, _ in market.calls)


def test_missing_rate_is_terminal_unavailable_not_zero_or_later_rate_selection():
    market, state = Market(), registered()
    market.missing_rate = True
    run(market, state)
    assert row(state)["status"] == "entry_unavailable"
    assert "lastFundingRate" in row(state)["unavailable_reason"]
    market.missing_rate = False
    run(market, state)
    assert row(state)["status"] == "entry_unavailable" and "legs" not in row(state)


def test_stale_quote_cannot_open_paper_position():
    market, state = Market(), registered()
    market.stale = True
    run(market, state)
    assert row(state)["status"] == "entry_unavailable"
    assert "stale" in row(state)["unavailable_reason"]


def test_total_loss_includes_settled_funding_and_executes_observed_gap_not_stop_price():
    market, state = Market(), registered()
    original = copy.deepcopy(state["trades"]), copy.deepcopy(state["retirement_rule"])
    run(market, state)
    entry = row(state)["legs"][0]["entry_price"]
    assert entry == pytest.approx(99.9)  # bid minus 10 bp, not baseline close
    market.now += timedelta(hours=1, minutes=1)
    market.prices["AAAUSDT"] = 101.0
    market.rows = [{"fundingTime": market.next_ms, "fundingRate": -0.10, "markPrice": "105"}]
    result = run(market, state)
    record = row(state)
    assert record["status"] == "closed" and record["exit_reason"] == "total_loss_budget"
    leg = record["legs"][0]
    assert leg["exit_price"] == pytest.approx(101.101)  # newly observed ask plus 10 bp
    cash = -0.10 * 105 / 99.9  # actual mark-price scaled funding cash, short pays
    expected = (1 - 101.101 / 99.9 + cash - 0.0005 * (1 + 101.101 / 99.9)) * 100
    assert record["net_pct"] == pytest.approx(expected, abs=1e-6)
    assert record["net_pct"] < -10  # a risk budget is not a guaranteed fill
    assert record["exit_detected_at"] == market.now.isoformat()
    assert record["exit_executed_at"] == market.now.isoformat()
    assert result["paired_source_ids"] == ["AAA|notice1"]
    market.prices["AAAUSDT"] = 900
    run(market, state)
    assert leg["exit_price"] == pytest.approx(101.101)  # idempotent, never re-price a finished exit
    assert (state["trades"], state["retirement_rule"]) == original


def test_hedged_risk_uses_both_legs_and_actual_executable_quotes():
    market, state = Market(), registered()
    candidate = source(hedged=True, hedge_symbols=["BTCUSDT"])
    run(market, state, [candidate])
    market.now += timedelta(minutes=10)
    market.prices.update(AAAUSDT=112.0, BTCUSDT=120.0)
    run(market, state, [candidate])
    assert row(state)["status"] == "open" and row(state)["mark"]["net_pct"] > 0
    market.prices["BTCUSDT"] = 90.0
    run(market, state, [candidate])
    assert row(state)["status"] == "closed"
    assert len(row(state)["legs"]) == 2
    assert all("exit_quote" in leg and "entry_quote" in leg for leg in row(state)["legs"])
    assert sum(name == "fundingInfo" for name, _ in market.calls) == 3  # once per apply, shared by both legs


def test_scheduled_exit_locks_current_fill_while_funding_unavailable_then_finalizes():
    market, state = Market(), registered()
    run(market, state)
    market.now += timedelta(hours=13)  # outage: one hour beyond scheduled close
    market.prices["AAAUSDT"], market.funding_down = 95, True
    run(market, state)
    record = row(state)
    assert record["status"] == "closing_pending_funding"
    assert record["exit_reason"] == "scheduled_end" and "net_pct" not in record
    assert record["exit_delay_sec"] == 3600
    assert record["exit_detection_net_pct"] is None
    assert record["legs"][0]["exit_price"] == pytest.approx(95.095)
    market.funding_down = False
    market.rows = [{"fundingTime": market.next_ms + k * HOUR, "fundingRate": 0, "markPrice": 100} for k in range(13)]
    market.prices["AAAUSDT"] = 80
    run(market, state)
    assert record["status"] == "closed"
    assert record["legs"][0]["exit_price"] == pytest.approx(95.095)
    assert record["funding_pct"] == 0  # known zero from valid settlement, never an outage fallback


def test_missing_settlement_mark_does_not_become_zero_and_monitoring_retries():
    market, state = Market(), registered()
    run(market, state)
    market.now += timedelta(hours=1, minutes=1)
    market.rows = [{"fundingTime": market.next_ms, "fundingRate": -0.2}]
    run(market, state)
    assert row(state)["status"] == "open" and row(state)["monitoring_status"] == "unavailable"
    market.rows[0]["markPrice"] = 100
    run(market, state)
    assert row(state)["status"] == "closed"


def test_missed_entry_never_backfills_historical_close():
    market, state = Market(), registered()
    market.now += timedelta(hours=3)
    run(market, state)
    assert row(state)["status"] == "missed" and "legs" not in row(state)
    assert not market.calls


def test_korean_adapter_preserves_baseline_and_mirrors_frozen_hedge():
    baseline = {"trades": {"AAA|n": {"symbol": "AAAUSDT", "day0_ms": int(START.timestamp() * 1000),
                "status": "waiting_entry", "discovered_at": START.isoformat(),
                "evidence": {"market_basket": {"symbols": ["BTCUSDT"]}}}}}
    original = copy.deepcopy(baseline)
    adapted = korean.risk_shadow_sources(baseline, "korea_caution_hedged")
    assert adapted[0]["hedged"] and adapted[0]["hedge_symbols"] == ["BTCUSDT"]
    assert adapted[0]["source_entry_ms"] == int(START.timestamp() * 1000) + 86_400_000
    assert baseline == original
    result = korean.summary(baseline, "korea_caution_hedged")
    assert result["event_risk_shadow"]["registered"] is False


def test_invalidated_source_exits_at_new_quote_even_when_no_longer_eligible():
    market, state = Market(), registered()
    run(market, state)
    market.now += timedelta(minutes=10)
    market.prices["AAAUSDT"] = 103
    run(market, state, [source(source_status="schedule_changed", eligible=False)])
    record = row(state)
    assert record["status"] == "closed" and record["exit_reason"] == "source_invalidated"
    assert record["legs"][0]["exit_price"] == pytest.approx(103.103)


def test_slow_funding_read_is_followed_by_fresh_quotes_before_loss_decision():
    market, state = Market(), registered()
    run(market, state)
    market.now += timedelta(minutes=10)
    market.prices["AAAUSDT"] = 120
    real_get = market.get

    async def slow_settlements(url, params=None):
        result = await real_get(url, params)
        if url.endswith("fundingRate"):
            market.now += timedelta(seconds=45)
        return result

    market.get = slow_settlements
    run(market, state)
    assert row(state)["status"] == "closed"
    mark = row(state)["mark"]
    assert mark["quotes"][0]["observed_ms"] > int(datetime.fromisoformat(mark["funding_cash_through_at"]).timestamp() * 1000)


def test_missing_tail_settlement_does_not_finalize_as_known_zero():
    market, state = Market(), registered()
    run(market, state)
    market.now += timedelta(hours=13)
    market.rows = [{"fundingTime": market.next_ms, "fundingRate": 0, "markPrice": 100}]
    run(market, state)
    assert row(state)["status"] == "closing_pending_funding"
    assert "latest expected settlement" in row(state)["unavailable_reason"]


def test_existing_position_keeps_monitoring_when_source_collection_is_empty():
    market, state = Market(), registered()
    run(market, state)
    market.now += timedelta(minutes=10)
    market.prices["AAAUSDT"] = 120
    asyncio.run(risk.apply_to_tracker(market, state, [], market.now, clock=lambda: market.now))
    assert row(state)["status"] == "closed" and row(state)["exit_reason"] == "total_loss_budget"


@pytest.mark.parametrize("status", ["no_perp", "delisted", "cancelled", "schedule_changed", "entry_failed"])
def test_invalid_source_retaining_symbol_cannot_open_shadow(status):
    market, state = Market(), registered()
    run(market, state, [source(source_status=status)])
    assert row(state)["status"] == "source_invalidated" and "legs" not in row(state)
    assert not market.calls


def test_korean_shadow_failure_preserves_baseline_and_freeze_precedes_requests(tmp_path, monkeypatch):
    path = tmp_path / "tracker.json"
    korean.save_state({"started_at": START.isoformat(), "trades": {}}, path)
    monkeypatch.setattr(korean, "_now", lambda: START)

    class BaselineClient:
        async def get(self, url, params=None, headers=None):
            import json
            frozen = json.loads(path.read_text(encoding="utf-8"))
            assert frozen[risk.STATE_KEY]["registered_at"] == START.isoformat()
            assert frozen[risk.STATE_KEY]["config"]["total_loss_budget_pct"] == 10
            if url.endswith("exchangeInfo"):
                return BaselineResponse({"symbols": []})
            if url == korean.UPBIT:
                return BaselineResponse({"data": {"notices": []}})
            raise AssertionError(url)

    class BaselineResponse(Response):
        text = "{}"

        def raise_for_status(self):
            pass

    async def shadow_down(*args, **kwargs):
        raise RuntimeError("shadow failed")

    monkeypatch.setattr(risk, "apply_to_tracker", shadow_down)
    result = asyncio.run(korean.tick(BaselineClient(), state_path=path))
    assert result["forward_completed"] == 0 and result["retirement"]["verdict"] == "collecting"
    assert result["event_risk_shadow"]["registered"]
