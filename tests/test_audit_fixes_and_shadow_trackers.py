"""2026-09-28 audit fixes and the two new research measurements."""
from __future__ import annotations

import asyncio
import json
import os
import time
from types import SimpleNamespace

import pandas as pd
import pytest
from fastapi import HTTPException

from core.ops.service import auth as ops_auth
from core.research import supply_factor_tracker as sf
from core.research import unlock_short_tracker as ut
from core.utils import openai_responses as oar


# ------------------------------------------------------------------ auth fallback
def test_request_without_auth_context_gets_no_permissions():
    request = SimpleNamespace(state=SimpleNamespace(), headers={}, client=SimpleNamespace(host="127.0.0.1"))
    ctx = ops_auth.get_request_auth(request)
    assert ctx.role == ops_auth.UNAUTHENTICATED_ROLE
    with pytest.raises(HTTPException) as exc:
        ops_auth.require_ops_permissions(request, "approve_live")
    assert exc.value.status_code == 403


# ------------------------------------------------------- position state cache
def test_scope_state_is_reread_only_when_the_file_changes(tmp_path, monkeypatch):
    from core.trading.position_manager import PositionManager

    pm = PositionManager.__new__(PositionManager)
    pm._scope_state_cache = {}
    path = tmp_path / "positions_live.json"
    monkeypatch.setattr(pm, "_scope_state_path", lambda scope=None: path, raising=False)
    path.write_text(json.dumps({"open_positions": [1]}), encoding="utf-8")
    reads = []
    import importlib

    pm_module = importlib.import_module("core.trading.position_manager")  # the package re-exports an instance under this name

    real = pm_module._read_text_with_retry
    monkeypatch.setattr(pm_module, "_read_text_with_retry", lambda p, encoding="utf-8": reads.append(p) or real(p, encoding=encoding))
    assert pm._load_scope_state("live") == {"open_positions": [1]}
    assert pm._load_scope_state("live") == {"open_positions": [1]}
    assert len(reads) == 1  # unchanged file: served from memory
    path.write_text(json.dumps({"open_positions": [1, 2]}), encoding="utf-8")
    later = time.time() + 5
    os.utime(path, (later, later))
    assert pm._load_scope_state("live") == {"open_positions": [1, 2]} and len(reads) == 2


# -------------------------------------------------- failover state write skipping
def test_failover_state_is_written_only_when_it_changes(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENAI_FAILOVER_STATE_PATH", str(tmp_path / "failover.json"))
    saves = []
    real = oar._save_openai_failover_state
    monkeypatch.setattr(oar, "_save_openai_failover_state", lambda path, state: saves.append(1) or real(path, state))
    targets = [{"base_url": "https://primary.example/v1", "role": "primary"}, {"base_url": "https://backup.example/v1", "role": "backup"}]
    for _ in range(5):
        oar.remember_openai_target_success(targets, "https://primary.example/v1", scope="research")
        oar.prioritize_openai_targets(targets, scope="research")
    first = len(saves)
    assert first <= 1  # at most the initial record; repeated successes change only the stamp
    oar.remember_openai_target_failure(targets, "https://primary.example/v1", scope="research")
    assert len(saves) == first + 1  # a real change (switch to backup) is still persisted


def test_failover_change_detection_ignores_only_the_stamp():
    base = {"day": "2026-09-28", "mode": "primary", "updated_at": "a"}
    assert oar._failover_entry_changed({**base, "updated_at": "b"}, base) is False
    assert oar._failover_entry_changed({**base, "mode": "backup"}, base) is True


# ----------------------------------------------------- unlock last-week variant
def test_unlock_t7_variant_enters_seven_days_before_with_its_own_rule(tmp_path, monkeypatch):
    import tests.test_unlock_short_tracker as base

    world = base.FakeWorld()
    monkeypatch.setattr(ut, "CACHE_DIR", tmp_path / "cache")
    monkeypatch.setattr(ut, "SCHEDULE_TTL_SEC", 0)
    monkeypatch.setattr(ut, "INDEX_TTL_SEC", 0)
    monkeypatch.setattr(ut, "_now", lambda: world.now)
    path = tmp_path / "t7.json"
    summary = asyncio.run(ut.tick(world, state_path=path, variant="t7"))
    state = json.loads(path.read_text(encoding="utf-8"))
    trade = state["trades"]["AAA|2026-11-15"]
    assert trade["entry_day"] == "2026-11-08" and trade["exit_day"] == "2026-11-14" and trade["min_size_pct"] == 5.0
    assert state["retirement_rule"]["min_n"] == 30 and summary["variant"] == "t7"
    assert "t-7" in summary["backtest_reference"]


# ------------------------------------------------------ weekly supply measurement
class _WeekWorld:
    """12 tokens releasing 0..11 x 1000/day; over the week low-growth ones rise, high-growth ones fall."""

    def __init__(self):
        self.tokens = [f"T{i}" for i in range(12)]
        self.monday = pd.Timestamp("2026-10-05", tz="UTC")

    def price(self, token, day):
        i = int(token[1:])
        if day < self.monday + pd.Timedelta(days=7):
            return 1.0
        return 1.0 + (6 - i) * 0.01  # monotone in growth rank

    async def get(self, url, params=None):
        import tests.test_supply_factor_tracker as base

        params = params or {}
        if url.endswith("/emissionsIndex"):
            return base._Resp({"data": [{"protocolSlug": t.lower(), "tokenPrice": [{"symbol": t, "price": 1.0}]} for t in self.tokens]})
        if "/emissions/" in url:
            i = int(url.rsplit("/", 1)[-1][1:])
            return base._Resp(base._schedule(1000.0 * i))
        if url.endswith("/ticker/price"):
            return base._Resp([{"symbol": f"{t}USDT", "price": "1.0"} for t in self.tokens])
        if url.endswith("/klines"):
            day = pd.Timestamp(params["startTime"], unit="ms", tz="UTC")
            return base._Resp([[int(day.timestamp() * 1000), "0", "0", "0", str(self.price(params["symbol"][:-4], day))]])
        raise AssertionError(url)


def test_weekly_supply_ic_is_measured_without_trading(tmp_path, monkeypatch):
    monkeypatch.setattr(ut, "CACHE_DIR", tmp_path / "cache")
    monkeypatch.setattr(ut, "SCHEDULE_TTL_SEC", 0)
    monkeypatch.setattr(ut, "INDEX_TTL_SEC", 0)
    world = _WeekWorld()
    path = tmp_path / "state.json"
    sf.save_state({"started_at": "2026-10-01T00:00:00+00:00", "months": {}}, path)
    asyncio.run(sf.tick(world, path, now=pd.Timestamp("2026-10-06 01:00", tz="UTC")))
    week = json.loads(path.read_text(encoding="utf-8"))["weekly_ic"]["2026-10-05"]
    assert week["status"] == "open" and week["backfill"] is False and len(week["entry"]) == 12
    summary = asyncio.run(sf.tick(world, path, now=pd.Timestamp("2026-10-13 01:00", tz="UTC")))
    week = json.loads(path.read_text(encoding="utf-8"))["weekly_ic"]["2026-10-05"]
    assert week["status"] == "closed" and week["ic"] == pytest.approx(-1.0) and "entry" not in week
    assert summary["weekly_ic"]["weeks_completed"] == 1 and summary["weekly_ic"]["weeks_ic_negative"] == 1
