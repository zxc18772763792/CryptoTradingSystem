"""Alastar chart images: inventory, fetch, image-only labelling with gpt-6-luna, null-tested evaluation.

The text studies left the images out: 398 of 422 posts carry a picture and only four
were reviewed by hand. This script reads what the pictures themselves say.

    python -m scripts.research_alastar_images inventory   # saved preview pages -> image list
    python -m scripts.research_alastar_images fetch       # public CDN images, sha256 receipts
    python -m scripts.research_alastar_images label       # gpt-6-luna, image only, cached per image
    python -m scripts.research_alastar_images evaluate    # same-coin random-hour null, all horizons

Labelling sees the picture only, never the caption, so image-derived direction can be
compared with the frozen text labels. The model is asked what is DRAWN (arrows/paths),
not where price went; it may still know later prices of these coins, so labels are
historical research, not prospective evidence. No orders, no Telegram messages.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlsplit

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

PAGES = ROOT / "data/research/alastar_20261021/pages"
MESSAGES = ROOT / "data/research/alastar_20261021/messages.jsonl"
OUT = ROOT / "data/research/alastar_images_20261021"
REPORT = ROOT / "reports/alastar_images_20261021"
MODEL = "gpt-6-luna"
MAX_IMAGE_BYTES = 2 * 1024 * 1024
CDN_HOST = re.compile(r"cdn[0-9]+\.telesco\.pe")
PHOTO = re.compile(r"tgme_widget_message_photo_wrap[^\"]*\"[^>]*href=\"https://t\.me/AlastarTrades/(\d+)[^\"]*\"[^>]*"
                   r"style=\"[^\"]*background-image:url\('(https://[^']+)'\)", re.IGNORECASE)
PROMPT = (
    "This image was posted by a crypto trading analyst. Describe only what the image shows. "
    "Return only JSON with keys: "
    "is_price_chart (bool: a candlestick/line price chart of one asset), "
    "symbol (ticker pair printed on the chart such as EDENUSDT, else null), "
    "timeframe (e.g. 15m, 1h, 4h, 1D, else null), "
    "drawn_projection (bool: the author drew arrows, a projected path or zig-zag beyond the last candle), "
    "segments (list in drawn order of {\"direction\": \"up\"|\"down\", \"target_price\": number or null}; empty if no projection), "
    "first_leg_direction (\"up\"|\"down\"|\"none\"), net_direction (\"up\"|\"down\"|\"none\": end of the projection versus the last candle), "
    "marked_levels (list of numbers labelled on the chart), "
    "other_content (short phrase if not a price chart, e.g. pnl screenshot, liquidation heatmap, open interest chart, text screenshot), "
    "confidence (0-1 that the reading of the drawn projection is right). "
    "Do not use any knowledge of what the price did after this chart."
)
PROMPT_SHA = hashlib.sha256(PROMPT.encode("utf-8")).hexdigest()


def inventory() -> list[dict]:
    """Every photo wrap on the saved pages, de-duplicated by (message, url position)."""
    seen: dict[tuple, dict] = {}
    for page in sorted(PAGES.glob("*.html")):
        html = page.read_text(encoding="utf-8")
        counters: dict[int, int] = {}
        for message_id, url in PHOTO.findall(html):
            mid = int(message_id)
            k = counters.get(mid, 0)
            counters[mid] = k + 1
            host = urlsplit(url).hostname or ""
            if not CDN_HOST.fullmatch(host):
                continue
            seen.setdefault((mid, k), {"message_id": mid, "index": k, "url": url, "page": page.name})
    items = sorted(seen.values(), key=lambda x: (x["message_id"], x["index"]))
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "inventory.json").write_text(json.dumps(items, indent=1), encoding="utf-8")
    return items


def fetch() -> dict:
    import requests
    from config.settings import settings
    from core.utils.proxy_env import ensure_proxy_env

    ensure_proxy_env(settings)
    items = json.loads((OUT / "inventory.json").read_text(encoding="utf-8"))
    images = OUT / "images"
    images.mkdir(parents=True, exist_ok=True)
    receipts, status = [], {}
    session = requests.Session()
    for item in items:
        target = images / f"{item['message_id']}_{item['index']}.jpg"
        if not target.exists():
            try:
                with session.get(item["url"], timeout=(10, 30), stream=True) as response:
                    if response.status_code != 200 or not CDN_HOST.fullmatch(urlsplit(response.url).hostname or ""):
                        status[f"http_{response.status_code}"] = status.get(f"http_{response.status_code}", 0) + 1
                        continue
                    raw = response.raw.read(MAX_IMAGE_BYTES + 1, decode_content=True)
                    if len(raw) > MAX_IMAGE_BYTES:
                        status["too_large"] = status.get("too_large", 0) + 1
                        continue
                target.write_bytes(raw)
                time.sleep(0.2)
            except requests.RequestException as exc:
                status[type(exc).__name__] = status.get(type(exc).__name__, 0) + 1
                continue
        raw = target.read_bytes()
        receipts.append({**{k: item[k] for k in ("message_id", "index")}, "file": target.name,
                         "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw),
                         "limitation": "current public preview asset; publication-time version unknown"})
        status["ok"] = status.get("ok", 0) + 1
    (OUT / "fetch_receipt.json").write_text(json.dumps({"status": status, "images": receipts}, indent=1), encoding="utf-8")
    return status


def _label_one(path: Path, base: str, key: str) -> dict:
    import requests

    cache = OUT / "labels" / (path.stem + ".json")
    if cache.exists():
        cached = json.loads(cache.read_text(encoding="utf-8"))
        if cached.get("prompt_sha256") == PROMPT_SHA and cached.get("model") == MODEL:
            return cached
    raw = path.read_bytes()
    body = {"model": MODEL, "max_tokens": 900, "messages": [{"role": "user", "content": [
        {"type": "text", "text": PROMPT},
        {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64," + base64.b64encode(raw).decode()}}]}]}
    session = requests.Session()
    session.trust_env = False
    last = None
    for attempt in range(4):
        try:
            response = session.post(base.rstrip("/") + "/chat/completions", json=body, timeout=150,
                                    proxies={"http": None, "https": None},
                                    headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
        except requests.RequestException as exc:
            last = type(exc).__name__
            time.sleep(5 * (attempt + 1))
            continue
        if response.status_code in (429, 500, 502, 503, 504):
            last = f"http_{response.status_code}"
            time.sleep(10 * (attempt + 1))
            continue
        if response.status_code != 200:
            last = f"http_{response.status_code}"
            break
        payload = response.json()
        text = payload["choices"][0]["message"]["content"] or ""
        try:
            parsed = json.loads(text[text.index("{"): text.rindex("}") + 1])
        except ValueError:
            parsed = None
        record = {"image": path.name, "image_sha256": hashlib.sha256(raw).hexdigest(), "model": MODEL,
                  "response_model": payload.get("model"), "prompt_sha256": PROMPT_SHA,
                  "labelled_at_utc": pd.Timestamp.now(tz="UTC").isoformat(), "raw_text": text, "label": parsed,
                  "usage": payload.get("usage")}
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_text(json.dumps(record, ensure_ascii=False, indent=1), encoding="utf-8")
        return record
    return {"image": path.name, "error": last}


def label(workers: int = 3) -> dict:
    from config.settings import settings

    base, key = settings.AI_MODEL_BACKUP_BASE_URL, settings.AI_MODEL_BACKUP_API_KEY  # vpsairobot: fast gpt-6-luna
    paths = sorted((OUT / "images").glob("*.jpg"))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(lambda p: _label_one(p, base, key), paths))
    errors = [r for r in results if "error" in r]
    return {"images": len(paths), "labelled": len(results) - len(errors), "errors": errors[:10]}


def _symbol(raw) -> str | None:
    if not raw:
        return None
    s = str(raw).upper()
    s = re.sub(r"\.P\b|PERPETUAL|PERP", "", s)  # TradingView "EDENUSDT.P" / "ARIAUSDT PERP"
    s = re.sub(r"[^A-Z0-9]", "", s)
    if not s:
        return None
    return s if s.endswith("USDT") else s + "USDT"


def build_signals() -> tuple[list[dict], dict]:
    """One image signal per message: the first chart image with a drawn projection."""
    from scripts.research_alastar_responses import all_events, exclusion

    messages = {int(json.loads(l)["payload"]["message_id"]): json.loads(l)
                for l in MESSAGES.read_text(encoding="utf-8").splitlines() if l.strip()}
    labels = {}
    for f in sorted((OUT / "labels").glob("*.json")):
        rec = json.loads(f.read_text(encoding="utf-8"))
        mid, idx = map(int, f.stem.split("_"))
        labels.setdefault(mid, []).append((idx, rec.get("label") or {}))
    text_side: dict[tuple, set] = {}
    for e in all_events():
        if exclusion(e) is None and e.get("direction") in (1, -1) and not e.get("evidence_type"):
            mid = int(str(e["source_id"]).rsplit(":", 1)[1])
            text_side.setdefault((mid, e["symbol"]), set()).add(int(e["direction"]))
    counts = {"messages_with_labels": len(labels), "chart": 0, "projection": 0, "symbol_resolved": 0, "edited_excluded": 0}
    signals = []
    for mid, items in sorted(labels.items()):
        msg = messages.get(mid)
        if msg is None:
            continue
        charts = [lab for _, lab in sorted(items, key=lambda x: x[0]) if lab.get("is_price_chart")]
        if not charts:
            continue
        counts["chart"] += 1
        drawn = [lab for lab in charts if lab.get("drawn_projection") and lab.get("first_leg_direction") in ("up", "down")]
        if not drawn:
            continue
        counts["projection"] += 1
        if (msg.get("payload") or {}).get("telegram_edited"):
            counts["edited_excluded"] += 1
            continue
        lab = drawn[0]
        text_symbols = [s if s.endswith("USDT") else s + "USDT" for s in (msg.get("symbols") or [])]
        symbol = _symbol(lab.get("symbol")) or (text_symbols[0] if len(set(text_symbols)) == 1 else None)
        if symbol is None:
            continue
        counts["symbol_resolved"] += 1
        sides = {"first_leg": 1 if lab["first_leg_direction"] == "up" else -1}
        if lab.get("net_direction") in ("up", "down"):
            sides["net"] = 1 if lab["net_direction"] == "up" else -1
        text = text_side.get((mid, symbol), set())
        for name, side in sides.items():
            relation = "no_text_direction" if not text else ("agrees_text" if text == {side} else "differs_from_text")
            signals.append({"signal_id": f"image:{mid}:{name}", "channel": "alastar_image", "kind": f"image_{name}",
                            "relation": relation, "symbol": symbol, "side": side, "published_at": msg["published_at"],
                            "segments": len(lab.get("segments") or []), "confidence": lab.get("confidence"),
                            "timeframe": lab.get("timeframe")})
    return signals, counts


def evaluate() -> None:
    from scripts.research_alastar_selection_timing import familywise
    from scripts.research_telegram_signal_strategy import LocalMarket
    from scripts.review_telegram_signals import HORIZONS, SymbolPanel, bh_fdr, dedup_independent, evaluate_signals, summarise

    REPORT.mkdir(parents=True, exist_ok=True)
    signals, counts = build_signals()
    market = LocalMarket()
    panels = {s: SymbolPanel(market.frame(s), market.frame(s, "funding_rate")) for s in {x["symbol"] for x in signals}}
    missing = sorted(s for s, p in panels.items() if len(p.index) == 0)
    signals = [s for s in signals if s["symbol"] not in missing]
    cells = []
    for window, label_ in ((30 * 24, "30d"), (7 * 24, "7d")):
        rows, _ = evaluate_signals(signals, panels, window)
        for kind in ("image_first_leg", "image_net"):
            for relation in ("all", "no_text_direction", "agrees_text", "differs_from_text"):
                for side in ("all", "long", "short"):
                    for hours in HORIZONS:
                        base = [r for r in rows if r["kind"] == kind and r["hours"] == hours
                                and (relation == "all" or r["relation"] == relation)
                                and (side == "all" or r["side"] == (1 if side == "long" else -1))]
                        kept = dedup_independent(base)
                        cell = {"null_window": label_, "kind": kind, "relation": relation, "side": side, "hours": hours,
                                "n_events": len(base)}
                        if len(kept) >= 5:
                            cell.update({k: v for k, v in summarise(kept, panels).items()
                                         if k in ("n_independent", "hit_rate", "null_hit_rate", "mean_net_20bps", "median_net_20bps",
                                                  "mean_net_80bps", "mean_without_best", "null_mean_net_20bps", "mean_excess_vs_null",
                                                  "excess_week_bootstrap_95", "p_greater_than_null", "p_two_sided",
                                                  "top_symbol", "top_symbol_share_of_gross_profit")})
                        else:
                            cell["n_independent"] = len(kept)
                        cells.append(cell)
    tested = [c for c in cells if c["null_window"] == "30d" and c.get("n_independent", 0) >= 10 and "p_two_sided" in c]
    for c, q in zip(tested, bh_fdr([c["p_two_sided"] for c in tested])):
        c["bh_q_two_sided"] = q
    pd.DataFrame(cells).to_csv(REPORT / "image_metrics.csv", index=False, encoding="utf-8-sig")
    fw = familywise(signals, panels, kinds=("image_first_leg", "image_net"))
    pd.DataFrame(fw).to_csv(REPORT / "image_familywise.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(signals).to_csv(OUT / "image_signals.csv", index=False, encoding="utf-8-sig")
    notes = {**counts, "signals": len(signals), "symbols_missing_local_history": missing, "model": MODEL,
             "prompt_sha256": PROMPT_SHA, "horizons": HORIZONS}
    (OUT / "run_notes.json").write_text(json.dumps(notes, indent=1), encoding="utf-8")
    print(json.dumps(notes, indent=1))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("step", choices=("inventory", "fetch", "label", "evaluate"))
    parser.add_argument("--workers", type=int, default=3)
    args = parser.parse_args()
    if args.step == "inventory":
        items = inventory()
        print(len(items), "images on", len({i["message_id"] for i in items}), "messages")
    elif args.step == "fetch":
        print(fetch())
    elif args.step == "label":
        print(json.dumps(label(args.workers), indent=1))
    else:
        evaluate()


if __name__ == "__main__":
    main()
