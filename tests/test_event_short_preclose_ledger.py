import asyncio
import gzip
import json
from datetime import datetime, timedelta, timezone

import pytest

from core.research import event_short_preclose_ledger as L
from core.research import retirement, tracker_registry as registry

UTC = timezone.utc
ACTIVATED = datetime(2026, 11, 6, 1, tzinfo=UTC)
MEMBERS = [f"M{i}USDT" for i in range(35)]


def at(day, hour=0, minute=0, second=0):
    return datetime.fromisoformat(f"{day}T{hour:02d}:{minute:02d}:{second:02d}+00:00")


def window(day, second=0):
    return at(day, 23, 55, second)


def unlock_trade(token="AAA", size=12.0, entry_day="2026-11-08", exit_day="2026-11-14", **kw):
    return {"token": token, "symbol": f"{token}USDT", "unlock_date": str((at(exit_day) + timedelta(days=1)).date()), "size_pct": size,
            "entry_day": entry_day, "exit_day": exit_day, "late": False, "status": "scheduled", "discovered_at": "2026-09-28T15:41:00+00:00", **kw}


def krw_trade(ticker="KKK", day="2026-11-08", **kw):
    return {"ticker": ticker, "symbol": f"{ticker}USDT", "notice_id": 1, "notice_at": f"{day}T06:00:00+00:00", "day0_ms": L.ms(at(day)),
            "backfilled": False, "late": False, "status": "waiting_entry", "discovered_at": f"{day}T06:10:00+00:00", **kw}


class Response:
    def __init__(self, data, status=200):
        self._data, self.status_code = data, status

    def json(self):
        return self._data


class Exchange:
    """A fake Binance: fixed books, klines and funding that tests can change between passes."""

    def __init__(self):
        self.bid, self.ask = {}, {}
        self.hours = {}
        self.funding = {}
        self.dailies = {}
        self.fail = set()
        self.calls = []
        for i, m in enumerate(MEMBERS):
            self.bid[m], self.ask[m] = 10.0, 10.01
        self.volume = {m: 1e9 - i for i, m in enumerate(MEMBERS)}
        self.dead = set()

    def coin(self, symbol, bid=1.0, ask=1.001, volume=2e7):
        self.bid[symbol], self.ask[symbol], self.volume[symbol] = bid, ask, volume

    async def get(self, url, params=None):
        endpoint = url.split("/fapi/v1/")[1]
        params = params or {}
        self.calls.append((endpoint, params.get("symbol")))
        if endpoint in self.fail or f"{endpoint}:{params.get('symbol')}" in self.fail:
            return Response({}, 503)
        symbol = params.get("symbol")
        if endpoint == "exchangeInfo":
            return Response({"symbols": [{"symbol": s, "contractType": "PERPETUAL", "quoteAsset": "USDT", "status": "SETTLING" if s in self.dead else "TRADING",
                                          "underlyingType": "COIN", "baseAsset": s[:-4]} for s in self.bid]})
        if endpoint == "ticker/bookTicker":
            return Response([{"symbol": s, "bidPrice": str(self.bid[s]), "askPrice": str(self.ask[s])} for s in self.bid])
        if endpoint == "ticker/24hr":
            return Response([{"symbol": s, "quoteVolume": str(v)} for s, v in self.volume.items()])
        if endpoint == "depth":
            return Response({"bids": [[str(self.bid[symbol]), "1000000"]], "asks": [[str(self.ask[symbol]), "1000000"]]})
        if endpoint == "klines":
            if params["interval"] == "1h":
                return Response(self.hours.get(symbol, []))
            if params["interval"] == "1m":
                return Response([[0, 1, 1, 1, 1, 0, 0, 5.0]])
            if "startTime" in params:
                price = self.dailies.get((symbol, params["startTime"]))
                return Response([[params["startTime"], price, price, price, price]] if price else [])
            return Response([[L.ms(at("2026-10-01")) + i * L.DAY_MS, 1, 1, 1, 1 + (i % 2) / 100] for i in range(33)])
        if endpoint == "fundingRate":
            rows = self.funding.get(symbol)
            if rows is None:
                start = L.ms(at("2026-11-01"))
                rows = [{"fundingTime": start + i * 8 * L.HOUR_MS + 3, "fundingRate": "0", "markPrice": "1"} for i in range(150)]
            return Response(rows)
        raise AssertionError(endpoint)


class World:
    def __init__(self, tmp_path, monkeypatch):
        self.now = ACTIVATED
        self.tmp = tmp_path
        self.unlock_path, self.krw_path = tmp_path / "unlock.json", tmp_path / "krw.json"
        self.cache = tmp_path / "cache"
        (self.cache / "emissions").mkdir(parents=True)
        self.unlock, self.krw = {}, {}
        self.exchange = Exchange()
        self.index = []
        monkeypatch.setattr(L, "now_utc", lambda: self.now)
        protocol = tmp_path / "protocol.json"
        protocol.write_text(json.dumps({"protocol_id": L.PROTOCOL_ID}), encoding="utf-8")
        monkeypatch.setattr(L, "PROTOCOL", protocol)
        self.protocol = protocol
        self.write()
        self.ledger = L.Ledger(tmp_path / "out", self.unlock_path, self.krw_path, self.cache, tmp_path / "panel", clock=lambda: self.now)

    def write(self):
        stamp = self.now.isoformat()
        self.unlock_path.write_text(json.dumps({"updated_at": stamp, "trades": self.unlock}), encoding="utf-8")
        self.krw_path.write_text(json.dumps({"updated_at": stamp, "trades": self.krw}), encoding="utf-8")
        (self.cache / "emissions_index.json").write_text(json.dumps({"data": self.index}), encoding="utf-8")

    def schedule(self, token, day, amount, base=100.0):
        """A cached schedule whose only cliff is amount tokens on day, with base tokens unlocked before it."""
        slug = token.lower()
        stamp = int(at(day).timestamp())
        self.index = [e for e in self.index if e["protocolSlug"] != slug] + [{
            "protocolSlug": slug, "tokenPrice": [{"symbol": token, "price": 1}],
            "unlockEvents": [{"timestamp": stamp, "cliffAllocations": [{"amount": amount, "category": "insiders"}]}]}]
        (self.cache / "emissions" / f"{slug}.json").write_text(json.dumps({"documentedData": {"data": [{"data": [
            {"timestamp": stamp - 200 * 86400, "unlocked": base}, {"timestamp": stamp, "unlocked": base + amount}]}]}}), encoding="utf-8")
        self.write()

    def activate(self):
        self.write()
        return self.ledger.activate()

    def run(self, now=None):
        if now is not None:
            self.now = now
        self.write()
        return asyncio.run(self.ledger.cycle(self.exchange))

    def state(self):
        return L.state(L.Journal(self.tmp / "out"))

    def event(self, source_id):
        return self.state()["events"][source_id]


@pytest.fixture
def world(tmp_path, monkeypatch):
    return World(tmp_path, monkeypatch)


def test_protocol_and_module_agree():
    protocol = json.loads(L.PROTOCOL.read_text(encoding="utf-8"))
    p = protocol["parameters"]
    assert (p["min_unlock_pct"], p["revision_min_pct"], p["revision_date_days"]) == (L.MIN_UNLOCK_PCT, L.REVISION_MIN_PCT, L.REVISION_DATE_DAYS)
    assert (p["capture_open_seconds_before_close"], p["capture_last_seconds_before_close"]) == (L.CAPTURE_OPEN_SEC, L.CAPTURE_LAST_SEC)
    assert (p["order_usd"], p["basket_size"], p["min_basket"]) == (L.ORDER_USD, L.BASKET_SIZE, L.MIN_BASKET)
    assert (p["coin_round_trip"], p["basket_round_trip"]) == (L.COIN_ROUND_TRIP, L.BASKET_ROUND_TRIP)
    assert (p["impact_y"], p["default_sigma"], p["sigma_returns"], p["min_sigma_returns"]) == (L.IMPACT_Y, L.DEFAULT_SIGMA, L.SIGMA_RETURNS, L.MIN_SIGMA_RETURNS)
    assert (p["stop_pct"], p["stop_slippage"], p["member_cap"]) == (L.STOP_PCT, L.STOP_SLIPPAGE, L.MEMBER_CAP)
    assert (p["review_batches"], p["min_batches"], p["review_after_days"]) == (L.REVIEW_BATCHES, L.MIN_BATCHES, L.REVIEW_AFTER_DAYS)
    assert (p["reject_mean_pct"], p["stream_min_trades"], p["stream_reject_pct"]) == (L.REJECT_MEAN_PCT, L.STREAM_MIN_TRADES, L.STREAM_REJECT_PCT)
    assert (p["entry_gap_trades"], p["entry_gap_min_trades"], p["entry_gap_reject_pct"]) == (L.ENTRY_GAP_TRADES, L.ENTRY_GAP_MIN_TRADES, L.ENTRY_GAP_REJECT_PCT)
    assert (p["revision_share"], p["revision_min_events"]) == (L.REVISION_SHARE, L.REVISION_MIN_EVENTS)
    assert (p["uptrend_prior"], p["uptrend_min_trades"], p["top_coins"], p["without_top_coins_min_pct"]) == (L.UPTREND_PRIOR, L.UPTREND_MIN_TRADES, L.TOP_COINS, L.WITHOUT_TOP_COINS_MIN_PCT)
    assert (p["permutations"], p["seed"], p["alpha"]) == (L.PERMUTATIONS, L.SEED, L.ALPHA)
    assert (p["alert_drawdown_pct"], p["alert_trade_pct"]) == (L.ALERT_DRAWDOWN_PCT, L.ALERT_TRADE_PCT)


def test_the_window_opens_five_minutes_before_the_close_of_the_day():
    assert L.day_close("2026-11-08") == at("2026-11-09")
    assert L.capture_open("2026-11-08") == at("2026-11-08", 23, 55)
    assert L.capture_last("2026-11-08") == at("2026-11-08", 23, 59, 30)
    assert L.sleep_seconds(at("2026-11-08", 12)) == L.POLL_SECONDS
    assert L.sleep_seconds(at("2026-11-08", 23, 50)) == 300
    assert L.sleep_seconds(at("2026-11-08", 23, 56)) == L.WINDOW_POLL_SECONDS
    assert L.sleep_seconds(at("2026-11-08", 23, 59, 45)) == L.POLL_SECONDS


@pytest.mark.parametrize(("size", "kept"), [(10.0, True), (45.56, True), (9.99, False), (float("nan"), False), (None, False), ("x", False)])
def test_only_unlocks_of_at_least_ten_percent_qualify(size, kept):
    assert (L.unlock_candidate("A|1", unlock_trade(size=size)) is not None) == kept


def test_krw_candidates_keep_the_trackers_universe():
    assert L.krw_candidate("K|1", krw_trade())["exit_day"] == "2026-11-15"
    assert L.krw_candidate("K|1", krw_trade(symbol=None)) is None
    assert L.krw_candidate("K|1", krw_trade(status="no_perp")) is None
    assert L.krw_candidate("K|1", krw_trade(backfilled=True)) is None


def test_vwap_walks_the_book_and_flags_what_it_cannot_fill():
    assert L.vwap([(2.0, 1000.0)], 1000.0) == (2.0, False)
    price, short = L.vwap([(2.0, 250.0), (1.0, 500.0)], 1000.0)
    assert price == pytest.approx(1000 / (250 + 500)) and not short
    price, short = L.vwap([(2.0, 100.0)], 1000.0)
    assert price == 2.0 and short
    with pytest.raises(L.Unavailable):
        L.vwap([], 1000.0)


def test_sigma_uses_the_last_thirty_returns_and_a_default_for_young_coins():
    assert L.sigma_of([1.0] * 5) == L.DEFAULT_SIGMA
    assert L.sigma_of([1.0] * 11) == 0.0
    closes = [100.0] * 40 + [100.0 + (i % 2) for i in range(31)]
    assert L.sigma_of(closes) == L.sigma_of(closes[-31:])


def test_impact_grows_with_order_and_falls_with_volume():
    assert L.impact_side(0.1, 10_000, 1e8) == pytest.approx(0.001)
    assert L.impact_side(0.1, 10_000, 0) == 0.1


def test_funding_sign_and_window():
    rows = [{"fundingTime": 1000, "fundingRate": "0.01", "markPrice": "2"}, {"fundingTime": 2000, "fundingRate": "-0.01", "markPrice": ""}]
    # a short receives a positive rate on the mark's share of the entry; the empty mark falls back to the entry price
    assert L.funding_cash(rows, 500, 3000, 1.0, -1)[0] == pytest.approx(0.02 - 0.01)
    assert L.funding_cash(rows, 500, 3000, 1.0, 1)[0] == pytest.approx(-0.02 + 0.01)
    assert L.funding_cash(rows, 1000, 3000, 1.0, -1)[1] == [2000]  # the start is open
    assert L.funding_cash(rows, 500, 1000, 1.0, -1)[1] == [1000]  # the end is closed


def test_funding_gap_is_detected_at_either_end_and_in_the_middle():
    hour = L.HOUR_MS
    assert not L.has_funding_gap([8 * hour, 16 * hour], 0, 24 * hour)
    assert L.has_funding_gap([], 0, 24 * hour)
    assert L.has_funding_gap([12 * hour], 0, 24 * hour)
    assert L.has_funding_gap([8 * hour], 0, 24 * hour)


def test_stop_fill_is_two_percent_beyond_the_level_or_the_gapped_open():
    bar = lambda o, h: [0, str(o), str(h)]
    assert L.first_stop([bar(1, 1.39)], 1.0) is None
    assert L.first_stop([bar(1, 1.4)], 1.0)["fill"] == pytest.approx(1.4 * 1.02)
    assert L.first_stop([bar(1.5, 1.6)], 1.0)["fill"] == pytest.approx(1.5 * 1.02)


def test_activation_records_upcoming_trades_and_only_runs_once(world):
    world.unlock = {"AAA|2026-11-15": unlock_trade(), "OLD|2026-10-01": unlock_trade(token="OLD", entry_day="2026-09-20", exit_day="2026-09-30")}
    rec = world.activate()
    assert rec["data"]["upcoming_at_activation"] == ["U|AAA|2026-11-15"]
    assert rec["data"]["protocol"] == L.PROTOCOL_ID
    with pytest.raises(RuntimeError):
        world.ledger.activate()


def test_stale_sources_block_activation(world):
    world.unlock_path.write_text(json.dumps({"updated_at": "2026-11-01T00:00:00+00:00", "trades": {}}), encoding="utf-8")
    with pytest.raises(RuntimeError, match="stale"):
        world.ledger.activate()


def test_a_full_time_exit_settles_with_book_prices_costs_and_funding(world):
    entry_close = at("2026-11-09")
    world.unlock = {"AAA|2026-11-15": unlock_trade()}
    world.exchange.coin("AAAUSDT", bid=1.0, ask=1.002, volume=2e7)
    world.schedule("AAA", "2026-11-15", 12.0)
    world.activate()
    assert world.run(at("2026-11-07", 12))["new_events"] == 1
    # the entry settlement at the close counts, the one at the exit close does not
    world.exchange.funding["AAAUSDT"] = [
        {"fundingTime": L.ms(entry_close) - L.HOUR_MS * 8 + 2, "fundingRate": "0.5", "markPrice": "1"},
        *[{"fundingTime": L.ms(entry_close) + i * 8 * L.HOUR_MS + 2, "fundingRate": "-0.001", "markPrice": "1"} for i in range(19)]]
    assert world.run(at("2026-11-08", 23, 54, 59))["entries"] == 0
    report = world.run(window("2026-11-08", 5))
    assert report["entries"] == 1 and not report["errors"]
    entry = world.event("U|AAA|2026-11-15")["entry"]
    assert entry["entry_price"] == 1.0  # sold into the bid
    assert entry["entry_close_ms"] == L.ms(entry_close)
    assert [m["s"] for m in entry["basket"]] == MEMBERS[:30]  # by volume, and the coin itself is left out
    # nothing happens after the entry until the exit window, and the entry is not repeated
    assert world.run(at("2026-11-09", 12))["entries"] == 0
    world.exchange.coin("AAAUSDT", bid=0.899, ask=0.9, volume=2e7)
    for m in MEMBERS:
        world.exchange.bid[m] = 11.011  # members up 10% from their 10.01 asks
    assert world.run(window("2026-11-14", 5))["exits"] == 1
    assert world.event("U|AAA|2026-11-15")["exit"]["reason"] == "time"
    assert world.run(at("2026-11-14", 23, 59))["settled"] == 0  # not until the close has printed
    world.exchange.dailies[("AAAUSDT", L.ms(at("2026-11-08")))] = 1.01
    world.exchange.dailies[("AAAUSDT", L.ms(at("2026-11-14")))] = 0.91
    report = world.run(at("2026-11-15", 0, 10))
    assert report["settled"] == 1 and not report["errors"]
    out = world.event("U|AAA|2026-11-15")["outcome"]
    assert out["price_pct"] == pytest.approx(10.0)
    # 18 settlements fall between 23:55 on the 8th and 23:55 on the 14th; the last row (00:00 on the 15th) is out
    assert out["funding_pct"] == pytest.approx(-1.8)
    assert out["fixed_cost_pct"] == pytest.approx(0.2)
    assert out["coin_net_pct"] == pytest.approx(10 - 1.8 - 0.2 - out["impact_coin_pct"])
    assert out["basket_price_pct"] == pytest.approx(10.0)
    assert out["basket_funding_pct"] == pytest.approx(0.0)
    assert out["hedged_pct"] == pytest.approx(out["coin_net_pct"] + out["basket_net_pct"], abs=1e-3)
    assert out["entry_gap_pct"] == pytest.approx((1.0 / 1.01 - 1) * 100, abs=1e-3)
    assert out["members"] == 30 and not out["funding_gap"] and not out["stopped"]


def test_a_stop_closes_the_coin_at_once_and_the_basket_at_the_next_window(world):
    world.krw = {"KKK|1": krw_trade()}
    world.exchange.coin("KKKUSDT")
    world.activate()
    world.run(at("2026-11-08", 12))
    world.run(window("2026-11-08"))
    assert "entry" in world.event("K|KKK|1")
    stop_open = L.ms(at("2026-11-10", 5))
    world.exchange.hours["KKKUSDT"] = [[stop_open - L.HOUR_MS, "1", "1.1", "1", "1"], [stop_open, "1.2", "1.5", "1.2", "1.3"]]
    assert world.run(at("2026-11-10", 6, 10))["stops"] == 1
    assert "exit" not in world.event("K|KKK|1")
    report = world.run(window("2026-11-10", 2))
    assert report["exits"] == 1
    leaving = world.event("K|KKK|1")["exit"]
    assert leaving["reason"] == "stop" and leaving["coin"]["exit_price"] == pytest.approx(1.4 * 1.02)
    assert leaving["coin"]["exit_ms"] == stop_open + L.HOUR_MS
    assert world.run(at("2026-11-11", 0, 10))["settled"] == 1
    assert world.event("K|KKK|1")["outcome"]["stopped"]
    assert world.event("K|KKK|1")["outcome"]["price_pct"] == pytest.approx(-42.8)


def test_late_events_and_missed_windows_are_void_not_dropped(world):
    world.activate()
    world.unlock = {"LAT|2026-11-20": unlock_trade(token="LAT", entry_day="2026-11-08", exit_day="2026-11-14", late=True)}
    world.krw = {"KKK|1": krw_trade()}
    world.exchange.coin("LATUSDT"), world.exchange.coin("KKKUSDT")
    world.run(at("2026-11-08", 12))
    assert world.event("U|LAT|2026-11-20")["void"]["reason"] == "late"
    # the service was down through the whole window
    report = world.run(at("2026-11-09", 3))
    assert report["entries"] == 0 and world.event("K|KKK|1")["void"]["reason"] == "entry_missed"
    world.krw["NEW|2"] = krw_trade(ticker="NEW", day="2026-11-08")
    world.exchange.coin("NEWUSDT")
    world.run(at("2026-11-09", 4))
    assert world.event("K|NEW|2")["void"]["reason"] == "late"
    assert L.summary(world.tmp / "out", world.now)["voids"] == {"late": 2, "entry_missed": 1}


def test_an_event_first_seen_inside_the_window_is_late(world):
    world.activate()
    world.krw = {"KKK|1": krw_trade()}
    world.exchange.coin("KKKUSDT")
    world.run(window("2026-11-08", 30))
    assert world.event("K|KKK|1")["void"]["reason"] == "late"


def test_a_failed_read_retries_inside_the_window_then_voids(world):
    world.krw = {"KKK|1": krw_trade()}
    world.exchange.coin("KKKUSDT")
    world.activate()
    world.run(at("2026-11-08", 12))
    world.exchange.fail.add("depth:KKKUSDT")
    report = world.run(window("2026-11-08", 0))
    assert report["entries"] == 0 and report["outcome"] == "degraded"
    assert L.is_waiting(world.event("K|KKK|1"))
    world.exchange.fail.clear()
    assert world.run(window("2026-11-08", 25))["entries"] == 1
    assert L.is_open(world.event("K|KKK|1"))


def test_a_missed_exit_is_captured_late_and_says_so(world):
    world.krw = {"KKK|1": krw_trade()}
    world.exchange.coin("KKKUSDT")
    world.activate()
    world.run(at("2026-11-08", 12))
    world.run(window("2026-11-08"))
    assert world.run(at("2026-11-16", 3))["exits"] == 1
    leaving = world.event("K|KKK|1")["exit"]
    assert leaving["late_sec"] > 3600 and leaving["reason"] == "time"


def test_a_time_exit_waits_for_a_readable_stop_check_until_its_window_ends(world):
    world.krw = {"KKK|1": krw_trade()}
    world.exchange.coin("KKKUSDT")
    world.activate()
    world.run(at("2026-11-08", 12))
    world.run(window("2026-11-08"))
    world.exchange.hours["KKKUSDT"] = [[L.ms(at("2026-11-15", 23)), "1", "1.5", "1", "1"]]
    world.exchange.fail.add("klines:KKKUSDT")
    report = world.run(window("2026-11-15", 1))
    assert report["exits"] == 0 and any(x.startswith("stop:") for x in report["errors"])
    world.exchange.fail.clear()
    report = world.run(window("2026-11-15", 21))
    assert report["stops"] == 1 and report["exits"] == 1  # a stop in the last hour beats the time exit
    assert world.event("K|KKK|1")["exit"]["reason"] == "stop"
    world.krw["BBB|2"] = krw_trade(ticker="BBB", day="2026-11-20")
    world.exchange.coin("BBBUSDT")
    world.run(at("2026-11-20", 12))
    world.run(window("2026-11-20"))
    world.exchange.fail.add("klines:BBBUSDT")
    assert world.run(window("2026-11-27", 1))["exits"] == 0
    assert world.run(at("2026-11-28", 0, 5))["exits"] == 1
    assert world.event("K|BBB|2")["exit"]["late_sec"] > 0


def test_a_lost_close_does_not_void_a_settled_trade(world):
    world.krw = {"KKK|1": krw_trade()}
    world.exchange.coin("KKKUSDT")
    world.activate()
    world.run(at("2026-11-08", 12))
    world.run(window("2026-11-08"))
    world.run(window("2026-11-15"))
    world.exchange.fail.add("klines:KKKUSDT")
    assert world.run(at("2026-11-16", 1))["settled"] == 0  # still patient
    assert world.run(at("2026-11-23", 1))["settled"] == 1
    out = world.event("K|KKK|1")["outcome"]
    assert out["hedged_pct"] is not None and out["entry_gap_pct"] is None


def test_unreadable_coin_funding_voids_the_trade_only_after_a_week(world):
    world.krw = {"KKK|1": krw_trade()}
    world.exchange.coin("KKKUSDT")
    world.activate()
    world.run(at("2026-11-08", 12))
    world.run(window("2026-11-08"))
    world.run(window("2026-11-15"))
    world.exchange.fail.add("fundingRate:KKKUSDT")
    assert world.run(at("2026-11-16", 1))["settled"] == 0
    assert world.run(at("2026-11-23", 1))["settled"] == 1
    assert world.event("K|KKK|1")["outcome"]["void"] == "unresolved"
    summary = L.summary(world.tmp / "out", world.now)
    assert summary["unresolved"] == 1 and summary["settled"] == 0 and summary["batches"] == 0


def test_a_funding_gap_waits_a_week_and_then_shows_on_the_outcome(world):
    world.krw = {"KKK|1": krw_trade()}
    world.exchange.coin("KKKUSDT")
    world.activate()
    world.run(at("2026-11-08", 12))
    world.run(window("2026-11-08"))
    world.run(window("2026-11-15"))
    world.exchange.funding["KKKUSDT"] = [{"fundingTime": L.ms(at("2026-11-09")) + 2, "fundingRate": "0.001", "markPrice": "1"}]
    assert world.run(at("2026-11-16", 1))["settled"] == 0
    assert world.run(at("2026-11-23", 1))["settled"] == 1
    out = world.event("K|KKK|1")["outcome"]
    assert out["funding_gap"] and out["funding_pct"] == pytest.approx(0.1)


def test_the_pool_drops_stablecoins_gold_and_non_coin_contracts():
    class Fake:
        async def get(self, url, params=None):
            endpoint = url.split("/fapi/v1/")[1]
            contract = lambda s, kind="PERPETUAL", under="COIN": {"symbol": f"{s}USDT", "contractType": kind, "quoteAsset": "USDT", "status": "TRADING", "underlyingType": under, "baseAsset": s}
            if endpoint == "exchangeInfo":
                return Response({"symbols": [contract("BTC"), contract("PAXG"), contract("USDC"), contract("TSLA", "TRADIFI_PERPETUAL", "EQUITY"), contract("BTCDOM", under="INDEX"), contract("PREDICT", under="PREMARKET")]})
            names = ["BTC", "PAXG", "USDC", "TSLA", "BTCDOM", "PREDICT"]
            if endpoint == "ticker/bookTicker":
                return Response([{"symbol": f"{n}USDT", "bidPrice": "1", "askPrice": "1"} for n in names])
            return Response([{"symbol": f"{n}USDT", "quoteVolume": "5"} for n in names])

    market = asyncio.run(L.market_snapshot(Fake()))
    assert market["ranked"] == ["BTCUSDT"]
    assert "PAXGUSDT" in market["live"] and "TSLAUSDT" not in market["live"]


def test_fills_are_the_best_quotes_and_the_walked_price_is_only_recorded():
    book = L.book_record({"bids": [["2.04", "1000"], ["1.0", "100000"]], "asks": [["2.06", "1000"], ["3.0", "100000"]]})
    assert book["best_bid"] == 2.04 and book["best_ask"] == 2.06
    assert book["vwap_sell"] < 2.04 and book["vwap_buy"] > 2.06  # 10,000 USDT walks past the first level
    assert book["bid_usdt_1pct"] == 2040.0 and book["ask_usdt_1pct"] == 2060.0


def test_the_same_coin_is_not_shorted_twice_and_is_free_again_after_its_exit(world):
    world.unlock = {"KKK|2026-11-15": unlock_trade(token="KKK")}
    world.krw = {"KKK|1": krw_trade(day="2026-11-10"), "KKK|2": krw_trade(day="2026-11-14")}
    world.exchange.coin("KKKUSDT")
    world.activate()
    world.run(at("2026-11-08", 12))
    world.run(window("2026-11-08"))
    assert "entry" in world.event("U|KKK|2026-11-15")
    world.run(window("2026-11-10"))
    assert world.event("K|KKK|1")["void"]["reason"] == "coin_already_held"
    report = world.run(window("2026-11-14"))
    assert report["exits"] == 1 and report["entries"] == 1  # exits go first, so the coin is free in the same window
    assert "entry" in world.event("K|KKK|2")


def test_a_symbol_that_stopped_trading_is_void_at_entry(world):
    world.krw = {"KKK|1": krw_trade()}
    world.exchange.coin("KKKUSDT")
    world.exchange.dead.add("KKKUSDT")
    world.activate()
    world.run(at("2026-11-08", 12))
    world.run(window("2026-11-08"))
    assert world.event("K|KKK|1")["void"]["reason"] == "not_trading"


def test_a_delisted_coin_exits_at_its_last_price(world):
    world.krw = {"KKK|1": krw_trade()}
    world.exchange.coin("KKKUSDT")
    world.activate()
    world.run(at("2026-11-08", 12))
    world.run(window("2026-11-08"))
    world.exchange.dead.add("KKKUSDT")
    world.exchange.hours["KKKUSDT"] = [[L.ms(at("2026-11-15", 22)), "0.5", "0.5", "0.5", "0.5"]]
    assert world.run(window("2026-11-15"))["exits"] == 1
    leaving = world.event("K|KKK|1")["exit"]
    assert leaving["reason"] == "delisted" and leaving["coin"]["exit_price"] == 0.5


def test_a_moved_unlock_voids_before_entry_and_exits_after_it(world):
    world.unlock = {"AAA|2026-11-15": unlock_trade(), "BBB|2026-11-15": unlock_trade(token="BBB")}
    world.exchange.coin("AAAUSDT"), world.exchange.coin("BBBUSDT")
    world.schedule("AAA", "2026-11-15", 12.0)
    world.schedule("BBB", "2026-11-15", 12.0)
    world.activate()
    world.run(at("2026-11-07", 1))
    assert L.is_waiting(world.event("U|AAA|2026-11-15"))
    world.schedule("AAA", "2026-11-25", 12.0)  # moved ten days before its entry
    world.run(at("2026-11-08", 1))
    assert world.event("U|AAA|2026-11-15")["void"]["reason"] == "revised_before_entry"
    assert world.event("U|AAA|2026-11-15")["revision"]["near_cliffs"] == []
    world.run(window("2026-11-08"))
    assert "entry" in world.event("U|BBB|2026-11-15")
    world.schedule("BBB", "2026-11-15", 6.0)  # shrunk after its entry
    world.run(at("2026-11-10", 1))
    assert "revision" in world.event("U|BBB|2026-11-15")
    report = world.run(window("2026-11-10"))
    assert report["exits"] == 1 and world.event("U|BBB|2026-11-15")["exit"]["reason"] == "revised"


def test_a_small_size_change_or_a_nearby_date_is_not_a_revision(world):
    event = {"stream": "U", "token": "AAA", "unlock_date": "2026-11-15", "size_pct": 10.4}
    near = lambda day, size: {"AAA": {"cliffs": [{"date": day, "size_pct": size}]}}
    assert L.revision_of(event, near("2026-11-15", 9.9)) is None
    assert L.revision_of(event, near("2026-11-18", 12.0)) is None
    assert L.revision_of(event, near("2026-11-15", 7.4)) is not None
    assert L.revision_of(event, near("2026-11-19", 12.0)) is not None
    assert L.revision_of(event, {}) is None  # no schedule file proves nothing
    assert L.revision_of({**event, "stream": "K"}, near("2026-12-30", 1.0)) is None


def test_the_daily_schedule_snapshot_is_written_once_and_hashed(world):
    world.schedule("AAA", "2026-11-15", 12.0)
    world.activate()
    world.run(at("2026-11-07", 1))
    world.run(at("2026-11-07", 2))
    records = [r for r in L.Journal(world.tmp / "out").records() if r["kind"] == "schedule_snapshot"]
    assert len(records) == 1
    packed = world.tmp / "out" / "schedule_snapshots" / "2026-11-07.json.gz"
    raw = gzip.decompress(packed.read_bytes())
    assert L.sha(raw) == records[0]["data"]["sha256"]
    tokens = json.loads(raw)["tokens"]
    assert tokens["AAA"]["cliffs"] == [{"date": "2026-11-15", "size_pct": 12.0}] and "file_sha256" in tokens["AAA"]
    # a new day's snapshot waits while the capture window is open
    assert world.run(window("2026-11-08"))["outcome"] == "ok"
    assert len([r for r in L.Journal(world.tmp / "out").records() if r["kind"] == "schedule_snapshot"]) == 1


def test_a_changed_rule_file_stops_the_journal(world):
    world.activate()
    world.protocol.write_text(json.dumps({"protocol_id": "changed"}), encoding="utf-8")
    assert world.run(at("2026-11-08", 12))["outcome"] == "drift_stopped"
    assert L.summary(world.tmp / "out", world.now)["status"] == "drift_stopped"


def test_a_tampered_journal_fails_the_chain(world):
    world.activate()
    path = world.tmp / "out" / "journal.jsonl"
    path.write_bytes(path.read_bytes().replace(L.PROTOCOL_ID.encode(), b"tampered"))
    assert L.summary(world.tmp / "out", world.now)["status"] == "journal_broken"


def event_row(stream="U", day="2026-11-08", hedged=5.0, capped=None, symbol=None, gap=None, **kw):
    return {"event": {"stream": stream, "entry_day": day, "symbol": symbol or f"{stream}{day}USDT", "source_id": f"{stream}|{day}|{symbol}"}, "entry": {},
            "outcome": {"hedged_pct": hedged, "hedged_capped_pct": hedged if capped is None else capped, "entry_gap_pct": gap, "basket_exit_ms": 1, "reason": "time"}, **kw}


def series(n=30, hedged=5.0, stream=None, **kw):
    start = at("2026-11-08")
    out = []
    for i in range(n):
        day = str((start + i * L.DAY).date())
        # enough spread for the sign-flip test to mean something
        value = hedged + (3.0 if i % 2 else -3.0)
        out.append(event_row(stream or ("U" if i % 2 else "K"), day, value, **kw))
    return out


def test_no_verdict_and_no_p_value_before_the_review():
    out = L.evaluate(series(29), ACTIVATED.isoformat(), at("2026-12-01"))
    assert out["verdict"] == "collecting" and out["p_sign_flip"] is None and out["batches"] == 29


def test_supported_needs_mean_capped_mean_significance_and_a_covered_rise():
    rows = series()
    moves = {r["event"]["entry_day"]: 0.0 for r in rows}
    out = L.evaluate(rows, ACTIVATED.isoformat(), at("2027-01-01"), moves)
    assert out["verdict"] == "supported_uptrend_uncovered" and out["p_sign_flip"] <= L.ALPHA
    up = {r["event"]["entry_day"]: 0.2 for r in rows}
    assert L.evaluate(rows, ACTIVATED.isoformat(), at("2027-01-01"), up)["verdict"] == "supported"
    assert L.evaluate(rows, ACTIVATED.isoformat(), at("2027-01-01"), None)["verdict"] == "supported_uptrend_uncovered"


def test_the_mean_and_the_capped_mean_each_reject():
    assert L.evaluate(series(hedged=2.0), ACTIVATED.isoformat(), at("2027-01-01"))["verdict"] == "rejected"
    capped = series(hedged=5.0, capped=1.0)
    assert L.evaluate(capped, ACTIVATED.isoformat(), at("2027-01-01"))["verdict"] == "rejected"


def test_a_weak_stream_rejects_only_that_part():
    rows = series(15, hedged=8.0, stream="K") + [event_row("U", str((at("2026-12-01") + i * L.DAY).date()), 0.5 + (i % 2) * 0.1) for i in range(15)]
    out = L.evaluate(rows, ACTIVATED.isoformat(), at("2027-01-01"))
    assert out["verdict"] == "partly_rejected"
    assert out["streams"]["U"]["verdict"] == "rejected" and out["streams"]["U"]["reasons"] == ["mean"]
    assert out["streams"]["K"]["verdict"] == "ok"


def test_too_few_trades_cannot_reject_a_stream():
    rows = series(25, hedged=8.0, stream="K") + [event_row("U", str((at("2027-01-01") + i * L.DAY).date()), -1.0) for i in range(5)]
    out = L.evaluate(rows, ACTIVATED.isoformat(), at("2027-02-01"))
    assert out["streams"]["U"]["verdict"] == "too_few"


def test_revised_or_late_unlocks_over_fifteen_percent_reject_the_unlock_stream():
    rows = series()
    bad = [{"event": {"stream": "U", "entry_day": rows[i]["event"]["entry_day"], "symbol": f"X{i}USDT", "source_id": f"X{i}"}, "void": {"reason": "late"}} for i in range(5)]
    out = L.evaluate(rows + bad, ACTIVATED.isoformat(), at("2027-01-01"))
    assert out["streams"]["U"]["revised_or_late"] == 5 and "schedule" in out["streams"]["U"]["reasons"]
    assert out["verdict"] == "partly_rejected"


def test_unlocks_that_lose_after_a_rise_reject_the_unlock_stream():
    rows = series(20, hedged=8.0, stream="K") + [event_row("U", str((at("2027-01-01") + i * L.DAY).date()), 6.0) for i in range(10)]
    moves = {r["event"]["entry_day"]: (0.3 if r["event"]["stream"] == "U" else 0.0) for r in rows}
    assert L.evaluate(rows, ACTIVATED.isoformat(), at("2027-02-01"), moves)["streams"]["U"]["uptrend_covered"]
    for r in rows:
        if r["event"]["stream"] == "U":
            r["outcome"].update(hedged_pct=-3.0, hedged_capped_pct=-3.0)
    out = L.evaluate(rows, ACTIVATED.isoformat(), at("2027-02-01"), moves)
    assert "uptrend" in out["streams"]["U"]["reasons"]


def test_a_bad_entry_gap_on_the_first_krw_trades_rejects_the_entry_rule():
    rows = series(hedged=8.0, stream="K", gap=-1.5)
    out = L.evaluate(rows, ACTIVATED.isoformat(), at("2027-01-01"))
    assert out["entry_rule_rejected"] and out["verdict"] == "partly_rejected"
    assert out["entry_gap_trades"] == L.ENTRY_GAP_TRADES
    assert not L.evaluate(series(hedged=8.0, stream="K", gap=-0.5), ACTIVATED.isoformat(), at("2027-01-01"))["entry_rule_rejected"]


def test_two_coins_making_the_money_is_not_a_pass():
    rows = series(hedged=1.5)
    rows[0]["outcome"].update(hedged_pct=60.0, hedged_capped_pct=60.0)
    rows[1]["outcome"].update(hedged_pct=60.0, hedged_capped_pct=60.0)
    out = L.evaluate(rows, ACTIVATED.isoformat(), at("2027-01-01"))
    assert out["without_top_coins_mean_pct"] == pytest.approx(1.5, abs=0.3) and out["concentrated"]
    assert out["verdict"] == "inconclusive" and sorted(out["top_coins"]) == sorted(r["event"]["symbol"] for r in rows[:2])
    healthy = L.evaluate(series(hedged=5.0), ACTIVATED.isoformat(), at("2027-01-01"))
    assert healthy["without_top_coins_mean_pct"] > L.WITHOUT_TOP_COINS_MIN_PCT and not healthy["concentrated"]


def test_a_book_of_only_two_coins_cannot_pass():
    rows = series(hedged=8.0)
    for i, r in enumerate(rows):
        r["event"]["symbol"] = "AUSDT" if i % 2 else "BUSDT"
    assert L.evaluate(rows, ACTIVATED.isoformat(), at("2027-01-01"))["concentrated"]


def test_the_review_by_count_uses_only_the_first_thirty_entry_days():
    rows = series(40)
    out = L.evaluate(rows, ACTIVATED.isoformat(), at("2027-01-01"))
    assert out["batches"] == 30 and out["cutoff_entry_day"] == rows[29]["event"]["entry_day"]


def test_a_running_trade_in_the_first_thirty_days_holds_the_review_back():
    rows = series(31)
    del rows[3]["outcome"]
    out = L.evaluate(rows, ACTIVATED.isoformat(), at("2027-01-01"))
    assert out["verdict"] == "collecting"


def test_the_review_by_time_uses_what_is_complete_and_needs_fifteen_batches():
    assert L.evaluate(series(14), ACTIVATED.isoformat(), ACTIVATED + timedelta(days=366))["verdict"] == "insufficient"
    assert L.evaluate(series(16), ACTIVATED.isoformat(), ACTIVATED + timedelta(days=366))["review_due"]


def test_a_batch_waits_for_every_trade_of_its_day():
    rows = [event_row("U", "2026-11-08", 4.0, symbol="A"), event_row("K", "2026-11-08", 8.0, symbol="B")]
    assert L.batches(rows) == [("2026-11-08", 6.0, 6.0)]
    del rows[1]["outcome"]
    assert L.batches(rows) == []
    assert L.batches([event_row(), {"event": {"entry_day": "2026-11-09"}, "void": {"reason": "late"}}]) == [("2026-11-08", 5.0, 5.0)]


def test_drawdown_runs_in_exit_order_at_five_percent_per_trade():
    rows = [event_row(hedged=-100.0), event_row(hedged=-100.0)]
    rows[0]["outcome"]["basket_exit_ms"], rows[1]["outcome"]["basket_exit_ms"] = 2, 1
    assert L.realized_drawdown_pct(rows) == pytest.approx((0.95 ** 2 - 1) * 100, abs=1e-3)
    assert L.realized_drawdown_pct([]) == 0.0


def test_alerts_warn_once_and_the_ledger_keeps_recording(world):
    world.activate()
    st = world.state()
    for i in range(2):
        source_id = f"U|X{i}"
        st["events"][source_id] = {"event": {"source_id": source_id, "stream": "U", "symbol": "XUSDT", "entry_day": "2026-11-08"}, "entry": {},
                                   "outcome": {"hedged_pct": -90.0, "hedged_capped_pct": -90.0, "basket_exit_ms": i}}
    world.ledger._alerts(st)
    world.ledger._alerts(st)
    kinds = [r["data"]["key"] for r in L.Journal(world.tmp / "out").records() if r["kind"] == "safety_alert"]
    assert kinds == ["drawdown", "trade:U|X0", "trade:U|X1"]  # two -90% trades at 5% each is -8.8%


def test_worker_idles_before_activation_and_runs_after_it(world, tmp_path):
    state_path = tmp_path / "runtime.json"
    beats = []

    class Factory:
        async def __aenter__(self):
            return world.exchange

        async def __aexit__(self, *exc):
            return False

    async def go():
        stop = asyncio.Event()

        def beat(ok):
            beats.append(ok)
            stop.set()

        await L.worker(stop, beat, Factory, world.tmp / "out", state_path)

    asyncio.run(go())
    assert json.loads(state_path.read_text(encoding="utf-8"))["outcome"] == "not_activated" and beats == [True]


def test_registry_and_retirement_know_the_ledger(monkeypatch, tmp_path):
    spec = next(s for s in registry.TRACKERS if s["id"] == L.TRACKER_ID)
    assert spec["unit"] == "批" and spec["rule_key"] == L.TRACKER_ID
    rule = retirement.RULES[L.TRACKER_ID]
    assert rule["min_n"] == L.REVIEW_BATCHES and rule["diagnostic_only"]
    measure = registry.measurement(spec)
    assert measure["gross_notional_multiplier"] == 2 and measure["is_paper"]
    monkeypatch.setattr(L, "OUTPUT", tmp_path / "none")
    monkeypatch.setattr(L, "STATE_PATH", tmp_path / "none.json")
    monkeypatch.setattr(L.summary, "__defaults__", (tmp_path / "none", None))
    assert L.registry_summary()["retirement"]["verdict"] == "not_activated"
