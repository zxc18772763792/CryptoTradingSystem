from pathlib import Path

import pytest

from core.research import altcoin_radar_universe as universe


@pytest.fixture(autouse=True)
def _reset_watchlist_cache():
    """The 5s in-memory cache would otherwise leak read results across tests
    (each test monkeypatches `_WATCHLIST_STORAGE_PATH` but read-only tests
    don't trigger an invalidation, so the next test sees the previous file's
    payload). Clear before and after to isolate."""
    universe._invalidate_watchlist_cache()
    yield
    universe._invalidate_watchlist_cache()


def test_watchlist_persistence_round_trip(tmp_path, monkeypatch):
    storage = tmp_path / "altcoin_watchlist.json"
    monkeypatch.setattr(universe, "_WATCHLIST_STORAGE_PATH", storage)

    added = universe.add_watchlist_symbol("WIF/USDT")
    assert "WIF/USDT" in added
    assert storage.exists()

    loaded = universe.get_watchlist_symbols()
    assert "WIF/USDT" in loaded

    removed = universe.remove_watchlist_symbol("WIF/USDT")
    assert "WIF/USDT" not in removed


def test_universe_meta_exposes_board_membership(tmp_path, monkeypatch):
    storage = tmp_path / "altcoin_watchlist.json"
    storage.write_text('{"symbols":["PEPE/USDT","WIF/USDT"]}\n', encoding="utf-8")
    monkeypatch.setattr(universe, "_WATCHLIST_STORAGE_PATH", storage)

    meta = universe.universe_meta(["PEPE/USDT", "ORDI/USDT", "AVAX/USDT"], "watchlist")

    assert meta["watchlist_hit_count"] == 1
    assert meta["board_membership"]["PEPE/USDT"] == "meme"
    assert meta["board_membership"]["ORDI/USDT"] == "ordinals"
    assert Path(meta["watchlist_storage"]).name == "altcoin_watchlist.json"


def test_empty_persisted_watchlist_falls_back_to_defaults(tmp_path, monkeypatch):
    storage = tmp_path / "altcoin_watchlist.json"
    storage.write_text('{"symbols":[]}\n', encoding="utf-8")
    monkeypatch.setattr(universe, "_WATCHLIST_STORAGE_PATH", storage)

    loaded = universe.get_watchlist_symbols()

    assert loaded
    assert "ORDI/USDT" in loaded


def test_watchlist_normalizes_rndr_to_render(tmp_path, monkeypatch):
    storage = tmp_path / "altcoin_watchlist.json"
    storage.write_text('{"symbols":["RNDR/USDT"]}\n', encoding="utf-8")
    monkeypatch.setattr(universe, "_WATCHLIST_STORAGE_PATH", storage)

    loaded = universe.get_watchlist_symbols()
    meta = universe.universe_meta(["RENDER/USDT"], "watchlist")

    assert "RENDER/USDT" in loaded
    assert "RNDR/USDT" not in loaded
    assert meta["watchlist_hit_count"] == 1
    assert meta["board_membership"]["RENDER/USDT"] == "ai"


def test_get_watchlist_symbols_caches_repeat_reads(tmp_path, monkeypatch):
    """Dashboard hits this on every focus event — without caching each hit
    would re-read the JSON file and recompute the normalize pipeline. The
    cache must serve the second call without touching disk."""
    storage = tmp_path / "altcoin_watchlist.json"
    storage.write_text('{"symbols":["PEPE/USDT","WIF/USDT"]}\n', encoding="utf-8")
    monkeypatch.setattr(universe, "_WATCHLIST_STORAGE_PATH", storage)

    first = universe.get_watchlist_symbols()
    # Mutate the file directly behind the cache's back. The second call must
    # still return the cached value (within the 5s TTL) to prove caching works.
    storage.write_text('{"symbols":["TAO/USDT"]}\n', encoding="utf-8")
    second = universe.get_watchlist_symbols()

    assert first == second == ["PEPE/USDT", "WIF/USDT"]
    # And the cache returns a copy — callers can't pollute the cached list.
    second.append("MUTATE/USDT")
    third = universe.get_watchlist_symbols()
    assert "MUTATE/USDT" not in third


def test_watchlist_mutation_invalidates_cache(tmp_path, monkeypatch):
    """Adding or removing a symbol must invalidate the cache immediately so
    the UI sees its own write reflected on the next GET."""
    storage = tmp_path / "altcoin_watchlist.json"
    storage.write_text('{"symbols":["PEPE/USDT"]}\n', encoding="utf-8")
    monkeypatch.setattr(universe, "_WATCHLIST_STORAGE_PATH", storage)

    universe.get_watchlist_symbols()  # Prime the cache.
    universe.add_watchlist_symbol("TAO/USDT")
    refreshed = universe.get_watchlist_symbols()
    assert "TAO/USDT" in refreshed

    universe.remove_watchlist_symbol("TAO/USDT")
    after_remove = universe.get_watchlist_symbols()
    assert "TAO/USDT" not in after_remove


def test_watchlist_reentrant_lock_in_universe_meta(tmp_path, monkeypatch):
    """universe_meta calls get_watchlist_symbols while a caller may already
    hold ``_WATCHLIST_LOCK``. A regular Lock would deadlock here — the
    RLock conversion is the fix."""
    storage = tmp_path / "altcoin_watchlist.json"
    storage.write_text('{"symbols":["PEPE/USDT","WIF/USDT"]}\n', encoding="utf-8")
    monkeypatch.setattr(universe, "_WATCHLIST_STORAGE_PATH", storage)

    # Acquire the same lock manually, then re-enter via universe_meta.
    with universe._WATCHLIST_LOCK:
        meta = universe.universe_meta(["PEPE/USDT", "WIF/USDT"], "watchlist")
    assert meta["watchlist_hit_count"] == 2


def test_sector_map_covers_new_2025_additions():
    """The expanded watchlist added DePIN / restaking / RWA labels — guard
    against silent regressions where a new symbol is added to the watchlist
    but its sector mapping is forgotten."""
    expected = {
        "EIGEN/USDT": "restaking",
        "ONDO/USDT": "rwa",
        "HNT/USDT": "depin",
        "BNB/USDT": "cex",
        "WLD/USDT": "ai",
        "TIA/USDT": "l1",
        "POPCAT/USDT": "meme",
        "ZK/USDT": "l2",
    }
    for symbol, sector in expected.items():
        assert universe.get_sector(symbol) == sector, f"{symbol} missing sector {sector}"


def test_matic_alias_normalizes_to_pol(tmp_path, monkeypatch):
    """MATIC was rebranded to POL. Persisted entries from older deployments
    must surface under the new ticker."""
    storage = tmp_path / "altcoin_watchlist.json"
    storage.write_text('{"symbols":["MATIC/USDT","PEPE/USDT"]}\n', encoding="utf-8")
    monkeypatch.setattr(universe, "_WATCHLIST_STORAGE_PATH", storage)

    loaded = universe.get_watchlist_symbols()
    assert "POL/USDT" in loaded
    assert "MATIC/USDT" not in loaded
