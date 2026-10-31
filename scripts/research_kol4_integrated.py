"""Four joined KOL channels read message by message, merged into trades, then backtested (2026-10-30).

Channels: Wise Advice Signal, The Rich Trader Reborn, Zetoshi X, Market Dealer. Every message is
read and labelled by hand (no keyword extraction); labels live in the restricted study directory.
This script only does the mechanical parts:

  collect   append messages newer than the 10/25 snapshots (one short reader.lock hold per channel)
  render    chronological reading sheets, one message per block with its reply parent
  media     download the chart image of listed message ids (short lock holds, paced for other readers)

The shared Telethon session is also polled by the HUB ledger and the three-source runner, so every
lock hold here is bounded to a few seconds and followed by a pause. Read-only: no joins, no sends.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
HOME = Path.home() / ".crypto_trading_system/research"
OUT = HOME / "kol4_integrated_20261030"
CHANNELS = {
    "wise": {"title": "Wise Advice Signal", "channel_id": 1341948340, "files": [
        "telegram_new_kols_20261025/wiseadvicebysumit_history.json",
        "telegram_kol_followup_20261027T151200Z/wiseadvicebysumit_history.json"]},
    "rich": {"title": "The Rich Trader Reborn", "channel_id": 2176770489, "files": [
        "telegram_new_kols_20261025/therichtrader_reborn_history.json",
        "telegram_kol_followup_20261027T151200Z/therichtrader_reborn_history.json"]},
    "zetoshi": {"title": "Zetoshi X", "channel_id": 2910104179, "files": [
        "multi_channel_backtest_180d_20261025/zetoshix_history.json"]},
    "market_dealer": {"title": "Market Dealer", "channel_id": 1786156359, "files": [
        "telegram_new_kols_20261025/market_dealer_history.json",
        "telegram_kol_followup_20261027T151200Z/market_dealer_history.json"]},
}
LOCK_PAUSE = 45  # seconds between lock holds, so the HUB ledger's ~55 s poll is never starved


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def load(slug: str) -> dict:
    """Existing snapshots plus anything collected here, keyed by message id (first seen version kept)."""
    posts = {}
    for rel in CHANNELS[slug]["files"] + [str(OUT / f"{slug}_new.json")]:
        path = Path(rel) if Path(rel).is_absolute() else HOME / rel
        if path.exists():
            data = json.loads(path.read_text(encoding="utf-8"))
            for p in data["posts"]:
                posts.setdefault(int(p["message_id"]), {**p, "source_file": path.name})
    return dict(sorted(posts.items()))


async def _with_client(fn):
    from telethon.sessions import StringSession
    import scripts.telegram_desktop_bridge as bridge
    with bridge.session_lock():
        payload = json.loads(bridge.dpapi(bridge.store_path().read_bytes(), decrypt=True))
        client = bridge.make_client(StringSession(payload["session"]), payload["api_id"], payload["api_hash"])
        try:
            await bridge.connect(client)
            me = await client.get_me()
            if not me or me.id != payload["account_id"]:
                raise RuntimeError("independent account identity mismatch")
            return await fn(client)
        finally:
            await client.disconnect()


async def _entity(client, channel_id: int):
    peers = OUT / "peers.json"
    cache = json.loads(peers.read_text(encoding="utf-8")) if peers.exists() else {}
    if str(channel_id) in cache:
        from telethon import types
        return types.InputPeerChannel(channel_id, cache[str(channel_id)])
    async for dialog in client.iter_dialogs(limit=500):
        if dialog.entity.id == channel_id:
            cache[str(channel_id)] = dialog.entity.access_hash
            peers.write_text(json.dumps(cache), encoding="utf-8")
            return dialog.entity
    raise RuntimeError(f"channel {channel_id} not among joined dialogs")


def _row(m) -> dict:
    text = m.message or ""
    fwd = m.fwd_from
    return {"message_id": m.id, "published_at": m.date.astimezone(timezone.utc).isoformat(),
            "edited_at": m.edit_date.astimezone(timezone.utc).isoformat() if m.edit_date else None,
            "text": text, "text_sha256": sha(text), "has_media": bool(m.media),
            "media_type": type(m.media).__name__ if m.media else None, "reply_to_id": m.reply_to_msg_id,
            "forwarded": bool(fwd), "forward_origin_date": fwd.date.astimezone(timezone.utc).isoformat() if fwd and fwd.date else None,
            "views": m.views, "post_author": m.post_author, "grouped_id": m.grouped_id}


def collect() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for i, (slug, spec) in enumerate(CHANNELS.items()):
        if i:
            time.sleep(LOCK_PAUSE)
        last = max(load(slug))

        async def fetch(client):
            entity = await _entity(client, spec["channel_id"])
            return [_row(m) async for m in client.iter_messages(entity, min_id=last, limit=500, reverse=True)]

        rows = asyncio.run(_with_client(fetch))
        path = OUT / f"{slug}_new.json"
        old = json.loads(path.read_text(encoding="utf-8"))["posts"] if path.exists() else []
        seen = {p["message_id"] for p in old}
        merged = old + [r for r in rows if r["message_id"] not in seen]
        path.write_text(json.dumps({"collected_at": datetime.now(timezone.utc).isoformat(), "after_message_id": last,
                                    "posts": merged}, ensure_ascii=False, indent=1), encoding="utf-8")
        print(slug, "new", len(rows), "latest", rows[-1]["published_at"] if rows else None, flush=True)


def render(slug: str, chunk_chars: int = 26000) -> None:
    posts = load(slug)
    folder = OUT / "sheets"
    folder.mkdir(parents=True, exist_ok=True)
    chunks, cur, size = [], [], 0
    for mid, p in posts.items():
        text = (p.get("text") or "").strip()
        parent = posts.get(int(p["reply_to_id"])) if p.get("reply_to_id") else None
        edit = ""
        if p.get("edited_at"):
            lag = (datetime.fromisoformat(p["edited_at"]) - datetime.fromisoformat(p["published_at"])).total_seconds()
            edit = f" edit+{int(lag)}s"
        head = (f"#{mid} {p['published_at'][:16]}Z{edit}"
                f"{' MEDIA:' + (p.get('media_type') or '').replace('MessageMedia', '') if p.get('has_media') else ''}"
                f"{' FWD' if p.get('forwarded') else ''}"
                f"{' reply->#' + str(p['reply_to_id']) if p.get('reply_to_id') else ''}")
        body = text if len(text) <= 900 else text[:900] + f" [...{len(text) - 900} more chars]"
        if parent is not None:
            ptext = (parent.get("text") or "").strip().replace("\n", " ")[:160]
            head += f'  (parent: "{ptext}")'
        block = head + "\n" + (body if body else "(no text)") + "\n"
        if size + len(block) > chunk_chars and cur:
            chunks.append(cur)
            cur, size = [], 0
        cur.append(block)
        size += len(block)
    if cur:
        chunks.append(cur)
    for i, blocks in enumerate(chunks):
        (folder / f"{slug}_{i:02d}.txt").write_text("\n".join(blocks), encoding="utf-8")
    print(slug, "messages", len(posts), "sheets", len(chunks))


def media(slug: str, ids: list[int], batch: int = 8) -> None:
    folder = OUT / "media" / slug
    folder.mkdir(parents=True, exist_ok=True)
    todo = [i for i in ids if not list(folder.glob(f"{i}.*"))]
    for start in range(0, len(todo), batch):
        part = todo[start:start + batch]

        async def fetch(client):
            entity = await _entity(client, CHANNELS[slug]["channel_id"])
            got = []
            for m in await client.get_messages(entity, ids=part):
                if m is None or not m.photo:
                    continue
                path = await client.download_media(m, file=str(folder / f"{m.id}"), thumb=-1)
                got.append(Path(path).name)
            return got

        got = asyncio.run(_with_client(fetch))
        print(slug, "downloaded", got, flush=True)
        if start + batch < len(todo):
            time.sleep(LOCK_PAUSE)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["collect", "render", "media"])
    ap.add_argument("--channel")
    ap.add_argument("--ids", default="")
    args = ap.parse_args()
    if args.cmd == "collect":
        collect()
    elif args.cmd == "render":
        for slug in ([args.channel] if args.channel else CHANNELS):
            render(slug)
    else:
        media(args.channel, [int(x) for x in args.ids.split(",") if x])


if __name__ == "__main__":
    main()
