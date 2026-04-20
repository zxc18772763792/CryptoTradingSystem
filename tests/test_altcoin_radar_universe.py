from pathlib import Path

from core.research import altcoin_radar_universe as universe


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
