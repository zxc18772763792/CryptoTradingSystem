"""The frozen kol4-v1 label copy must behave exactly like the study script that sealed the 10/25 labels."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from core.research import kol4_call_labels as frozen
from scripts import research_telegram_new_kols as study

STUDY = Path.home() / ".crypto_trading_system/research/telegram_new_kols_20261025"
FUTURES = {"POLYX", "1000PEPE", "BTC", "ETH", "SOL", "EDU", "ABC"}
TEXTS = [
    "POLYXUSDT buy and hold big Move soon",
    "PEPE usdt buy and hold big move soon",
    "FUTURES LONG Below 0.52 #ABC\nTP 0.58\nSL 0.47",
    "If #SOL breaks 200, long #SOL",
    "#ABC profit 75% 🚀",
    "Bitcoin looks bullish, $70,000 next",
    "Will long here #BTC $65,690 and #SOL",
    "Join our VIP for more signals #ABC",
    "Still holding my #EDU long",
    "",
]


@pytest.mark.parametrize("text", TEXTS)
def test_the_copy_labels_like_the_study_script(text):
    post = {"message_id": 1, "text": text}
    assert frozen.classify_post(post, FUTURES, set()) == study.classify_post(post, FUTURES, set())


def test_template_calls_and_exclusions():
    call = frozen.classify_post({"message_id": 1, "text": TEXTS[0]}, FUTURES, set())
    assert call["label"] == "direct_call" and call["signals"] == [{"raw_symbol": "POLYX", "symbol": "POLYX",
                                                                  "venue": "futures", "side": 1}]
    assert frozen.classify_post({"message_id": 1, "text": TEXTS[1]}, FUTURES, set())["signals"][0]["symbol"] == "1000PEPE"
    assert frozen.classify_post({"message_id": 1, "text": TEXTS[2]}, FUTURES, set())["label"] == "trade_plan"
    assert frozen.classify_post({"message_id": 1, "text": TEXTS[3]}, FUTURES, set())["conditional"] is True
    assert frozen.classify_post({"message_id": 1, "text": TEXTS[4]}, FUTURES, set())["label"] == "recap_or_promo"


def test_every_sealed_next10xgem1_label_is_reproduced():
    history, sealed = STUDY / "next10xgem1_history.json", STUDY / "events_before_prices.json"
    if not (history.exists() and sealed.exists()):
        pytest.skip("the 10/25 Telegram history is local, restricted data")
    posts = json.loads(history.read_text(encoding="utf-8"))["posts"]
    labels = {p["message_id"]: p for p in json.loads(sealed.read_text(encoding="utf-8"))["channels"]["next10xgem1"]["posts"]}
    futures, spot = study.tradable_universe()
    for post in posts:
        mine, theirs = frozen.classify_post(post, futures, spot), study.classify_post(post, futures, spot)
        assert mine == theirs
        want = labels[post["message_id"]]
        assert (mine["label"], mine["reason"], mine["conditional"]) == (want["label"], want["reason"], want["conditional"])
        assert [(s["raw_symbol"], s["side"]) for s in mine["signals"]] == [(s["raw_symbol"], s["side"]) for s in want["signals"]]
