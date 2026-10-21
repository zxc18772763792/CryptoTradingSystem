from __future__ import annotations

import json

import scripts.research_alastar_images as images


def page(*wraps: tuple[int, str]) -> str:
    return "".join(
        f'<a class="tgme_widget_message_photo_wrap 1 2" href="https://t.me/AlastarTrades/{mid}" '
        f"style=\"width:800px;background-image:url('{url}')\"></a>"
        for mid, url in wraps
    )


def test_inventory_maps_photos_to_messages_dedups_pages_and_rejects_foreign_hosts(tmp_path, monkeypatch):
    pages, out = tmp_path / "pages", tmp_path / "out"
    pages.mkdir()
    a, b = "https://cdn4.telesco.pe/file/a.jpg", "https://cdn5.telesco.pe/file/b.jpg"
    (pages / "0001.html").write_text(page((10, a), (11, b), (11, "https://cdn4.telesco.pe/file/c.jpg")), encoding="utf-8")
    (pages / "0002.html").write_text(page((10, a), (12, "https://evil.example/x.jpg")), encoding="utf-8")
    monkeypatch.setattr(images, "PAGES", pages)
    monkeypatch.setattr(images, "OUT", out)
    items = images.inventory()
    assert [(i["message_id"], i["index"]) for i in items] == [(10, 0), (11, 0), (11, 1)]
    assert json.loads((out / "inventory.json").read_text(encoding="utf-8")) == items


def test_symbol_normalisation():
    assert images._symbol("EDEN/USDT") == "EDENUSDT"
    assert images._symbol("EDENUSDT.P") == "EDENUSDT"
    assert images._symbol("TRADOOR") == "TRADOORUSDT"
    assert images._symbol("ARIAUSDT PERP") == "ARIAUSDT"
    assert images._symbol(None) is None and images._symbol("") is None
