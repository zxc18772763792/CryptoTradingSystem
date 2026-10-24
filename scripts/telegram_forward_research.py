"""Three-source research and scheduled paper observations, using the existing session."""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter, defaultdict
from contextlib import contextmanager
from datetime import timedelta
import hashlib
import hmac
import json
import logging
import math
import os
from pathlib import Path
import re
import sys
from loguru import logger as _private_logger
_private_logger.remove()

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import telegram_desktop_bridge as bridge
from core.research.telegram_forward import EXPLORATORY_PROTOCOL, PROTOCOL, SOURCES, STRESS, Store, digest, features, next_boundary, outcome, rule_kind, utc
from core.research.telegram_text_signals import ASSETS, PROMPT, PROMPT_SHA, validate_labels, enforce_eligibility

DEFAULT_OUTPUT = ROOT / "data/research/telegram_live_20261024"


class CooldownActive(RuntimeError):
    def __init__(self, seconds):
        self.seconds = seconds
        super().__init__("Server cooldown is still active")


@contextmanager
def study_lock(output):
    import msvcrt
    with (output/"study.lock").open("a+b") as stream:
        if stream.tell()==0:
            stream.write(b"\0"); stream.flush()
        stream.seek(0)
        msvcrt.locking(stream.fileno(),msvcrt.LK_NBLCK,1)
        try:
            yield
        finally:
            stream.seek(0)
            msvcrt.locking(stream.fileno(),msvcrt.LK_UNLCK,1)


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name+".tmp")
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    temp.replace(path)


def setup(output):
    if output == (ROOT/"data/research").resolve() or not output.is_relative_to((ROOT/"data/research").resolve()):
        raise ValueError("Study output must be a dedicated directory under data/research")
    bridge.protect_directory(output)
    store = Store(output)
    path = output / "protocol.json"
    if path.exists():
        if json.loads(path.read_text(encoding="utf-8"))["protocol"] != PROTOCOL:
            raise ValueError("Frozen protocol changed; create a new study")
    else:
        write_json(path, {"frozen_at": utc().isoformat(), "protocol": PROTOCOL, "protocol_sha": digest(PROTOCOL)})
    authorization = output / "authorization.json"
    if not authorization.exists():
        raise PermissionError("Local source-purpose authorization record required")
    auth = json.loads(authorization.read_text(encoding="utf-8"))
    if auth.get("status") != "user_attested" or set(auth.get("sources", [])) != set(SOURCES):
        raise PermissionError("Authorization scope does not match fixed sources")
    previous_error = output/"last_error.json"
    if previous_error.exists():
        old = json.loads(previous_error.read_text(encoding="utf-8"))
        if old.get("retry_at") and utc(old["retry_at"])>utc():
            raise CooldownActive(math.ceil((utc(old["retry_at"])-utc()).total_seconds()))
    return store


def author_salt():
    path = bridge.store_path().with_name("research_author_salt.dpapi")
    if not path.exists():
        with path.open("xb") as stream:
            stream.write(bridge.dpapi(os.urandom(32)))
    return bridge.dpapi(path.read_bytes(), decrypt=True)


async def collect(store, *, historical=False):
    from telethon import functions, types
    from telethon.sessions import StringSession
    payload = json.loads(bridge.dpapi(bridge.store_path().read_bytes(), decrypt=True))
    if payload.get("pending_password"):
        raise RuntimeError("Complete local Telegram login first")
    salt = author_salt()
    client = bridge.make_client(StringSession(payload["session"]), payload["api_id"], payload["api_hash"])
    summary = {}
    try:
        await bridge.connect(client)
        me = await client.get_me()
        if not me or me.id != payload["account_id"]:
            raise RuntimeError("Independent account identity mismatch")
        for channel in SOURCES:
            state = store.state("source:"+channel, {})
            entity = await client.get_entity(channel)
            if state.get("channel_id") not in (None, entity.id):
                raise RuntimeError("Allowlisted username now resolves to a different channel")
            await client(functions.channels.GetParticipantRequest(entity, "me"))
            received = utc().isoformat()
            minimum = 0 if historical else state.get("last_message_id", 0)
            cutoff = utc()-timedelta(days=PROTOCOL["history_days"])
            count, changed, oldest, maximum = 0, 0, None, minimum
            cap = PROTOCOL["history_cap_per_source"] if historical else 2000
            iterator = client.iter_messages(entity, limit=cap, min_id=minimum,
                                            reverse=not historical, wait_time=1)
            async for message in iterator:
                if historical and message.date < cutoff:
                    break
                received = utc().isoformat()
                sender_id = message.sender_id
                author = hmac.new(salt, str(sender_id).encode(), hashlib.sha256).hexdigest()[:24] if sender_id else None
                sender = message.sender
                row = {"source_id": f"telegram:{channel}:{message.id}", "channel": channel,
                       "message_id": message.id, "published_at": message.date.isoformat(),
                       "received_at": received, "first_received_at": received,
                       "mode": "historical" if historical else "forward",
                       "content": message.message or "", "author_hash": author,
                       "sender_is_bot": bool(getattr(sender, "bot", False)) if sender else True,
                       "sender_unknown": sender is None, "forwarded": message.fwd_from is not None,
                       "edited": message.edit_date is not None,
                       "service": isinstance(message, types.MessageService)}
                changed += store.ingest(row, commit=False)
                count += 1
                if count % 100 == 0:
                    store.db.commit()
                maximum = max(maximum, message.id)
                oldest = message.date.isoformat()
            store.db.commit()
            update_gap, deleted, pts = False, 0, state.get("pts")
            if not historical:
                # Refresh a bounded tail for visible edits as well as channel differences.
                edit_ids = set()
                if pts:
                    for _ in range(20):
                        difference = await client(functions.updates.GetChannelDifferenceRequest(
                            channel=entity, filter=types.ChannelMessagesFilterEmpty(), pts=pts, limit=100))
                        if isinstance(difference, types.updates.ChannelDifferenceTooLong):
                            update_gap = True
                            pts = getattr(difference.dialog, "pts", None)
                            break
                        if difference.pts < pts:
                            raise RuntimeError("Channel pts regressed")
                        old_pts, pts = pts, difference.pts
                        for update in getattr(difference, "other_updates", []):
                            if isinstance(update, types.UpdateEditChannelMessage):
                                edit_ids.add(update.message.id)
                            elif isinstance(update, types.UpdateDeleteChannelMessages) and update.channel_id == entity.id:
                                for message_id in update.messages:
                                    store.delete_message(f"telegram:{channel}:{message_id}")
                                    deleted += 1
                        if getattr(difference, "final", True):
                            break
                        if old_pts == pts:
                            raise RuntimeError("Channel difference made no progress")
                    else:
                        update_gap = True
                tail = await client.get_messages(entity, limit=100)
                if edit_ids:
                    tail += [m for m in await client.get_messages(entity, ids=sorted(edit_ids)) if m is not None]
                for message in tail:
                    old = store.db.execute("SELECT row_json FROM messages WHERE source_id=?", (f"telegram:{channel}:{message.id}",)).fetchone()
                    if not old:
                        continue
                    row = json.loads(old[0])
                    row.update(content=message.message or "", received_at=utc().isoformat(), edited=message.edit_date is not None)
                    changed += store.ingest(row, commit=False)
                store.db.commit()
            if not pts:
                from telethon import utils
                dialogs = await client(functions.messages.GetPeerDialogsRequest([types.InputDialogPeer(utils.get_input_peer(entity))]))
                pts = next((getattr(d,"pts",None) for d in dialogs.dialogs if getattr(d.peer,"channel_id",None)==entity.id),None)
            at = utc()
            clean_since = state.get("clean_since") or store.state("activation", {}).get("at")
            if (update_gap or count >= cap or (state.get("last_collection_at")
                    and (at-utc(state["last_collection_at"])).total_seconds() > 1800)):
                clean_since = at.isoformat()
            stats = {"member": True, "channel_id": entity.id, "pts": pts,
                     "received_rows": count, "new_or_revised": changed, "deleted": deleted,
                     "history_cap_reached": count >= cap, "last_message_id": maximum,
                     "last_collection_at": at.isoformat(), "oldest_in_batch": oldest,
                     "historical": historical, "complete_batch": count < cap and not update_gap,
                     "difference_gap": update_gap, "clean_since": clean_since}
            store.set_state("source:"+channel, dict(state, **stats))
            summary[channel] = stats
            print(json.dumps({"source": channel, "collected": count, "revised": changed, "cap_reached": count>=cap}), flush=True)
    finally:
        await client.disconnect()
    receipt = {"at": utc().isoformat(), "sources": summary}
    write_json(store.root / "collection.json", receipt)
    if historical and not (store.root/"history_collection.json").exists():
        write_json(store.root/"history_collection.json", receipt)
    return summary


async def label(store, *, historical=False):
    from loguru import logger
    logger.remove()
    from core.news.eventizer.async_glm_client import AsyncGLMClient
    from core.news.eventizer.rules import load_news_rule_config
    from core.utils.openai_responses import extract_response_text
    from scripts.review_telegram_text import PROMPT as REVIEW_PROMPT
    since = None if historical else (utc()-timedelta(days=2)).isoformat()
    rows = store.rows(since)
    cached = store.labels(since)
    candidates, per_source = [], Counter()
    for row in rows:
        old = cached.get(row["source_id"])
        if not historical and row["mode"] != "forward":
            continue
        kind = rule_kind(row)
        if row.get("revision_observed") or (historical and row.get("edited")) or row.get("forwarded"):
            kind = "other"
        if (old and old.get("text_sha256") == row["text_sha256"]
                and (old.get("model") != "deterministic_prefilter_v1" or old.get("kind") == kind)):
            continue
        if kind != "needs_model":
            store.save_label(row, {"kind": kind, "events": [], "model": "deterministic_prefilter_v1", "prompt_sha256": PROMPT_SHA}, commit=False)
        else:
            candidates.append(row)
    store.db.commit()
    if historical:
        selected = store.state("historical_label_selection")
        if selected is None:
            selected = []
            for channel in SOURCES:
                source_rows = [r for r in candidates if r["channel"] == channel]
                count = min(len(source_rows), PROTOCOL["research_label_cap_per_source"])
                indexes = [int(i*(len(source_rows)-1)/max(1,count-1)) for i in range(count)]
                selected.extend(source_rows[i]["source_id"] for i in indexes)
            store.set_state("historical_label_selection", selected)
            write_json(store.root/"historical_label_selection.json", {"selected_at": utc().isoformat(), "source_ids": selected, "sha": digest(selected)})
        selection = set(selected)
        candidates = [r for r in candidates if r["source_id"] in selection]
    else:
        bounded = []
        for row in candidates:
            if per_source[row["channel"]] < 200:
                bounded.append(row)
                per_source[row["channel"]] += 1
        candidates = bounded
    # Deterministic text-only selection is frozen before price outcomes are read.
    client = AsyncGLMClient(load_news_rule_config(ROOT/"config/news_rules.yaml", ROOT/"config/symbols.yaml"))
    errors, models = [], Counter()
    for offset in range(0, len(candidates), 16):
        batch = candidates[offset:offset+16]
        try:
            response, error = await client.chat_completions([
                {"role": "system", "content": PROMPT},
                {"role": "user", "content": json.dumps({"messages": [{"id": str(i), "text": row["content"]} for i,row in enumerate(batch)]}, ensure_ascii=False)}
            ], temperature=0, max_tokens=6500, timeout=75)
            if error != "none":
                raise RuntimeError("Model unavailable")
            text = re.sub(r"^```(?:json)?\s*|\s*```$", "", extract_response_text(response).strip())
            doc = json.loads(text)
            write_json(store.root/"label_attempts"/f"{digest([r['source_id'] for r in batch])}.json",
                       {"at": utc().isoformat(), "source_ids": [r["source_id"] for r in batch], "response": doc})
            mapping = {str(item.get("id")): item for item in doc.get("labels", [])}
            if len(doc.get("labels", [])) != len(batch) or len(mapping) != len(batch) or set(mapping) != {str(i) for i in range(len(batch))}:
                raise ValueError("Incomplete or duplicate model label batch")
            model = str((response.get("_cts_openai_target") or {}).get("model") or response.get("model") or client._model)
            expected_model = store.state("activation",{}).get("classifier_model")
            if expected_model and model != expected_model:
                raise RuntimeError("Forward classifier model changed; explicit migration required")
            accepted = []
            for i, row in enumerate(batch):
                value = enforce_eligibility(mapping[str(i)], row)
                if value.get("kind") == "directional":
                    original_events = value.get("events", [])
                    seen_symbols, explicit = set(), []
                    for event in original_events:
                        symbol = event.get("symbol")
                        if (symbol in ASSETS and symbol not in seen_symbols
                                and re.search(ASSETS[symbol], row["content"], re.I)):
                            explicit.append(event)
                            seen_symbols.add(symbol)
                    value = dict(value, events=explicit)
                    if not explicit:
                        value["kind"] = "other"
                validated = validate_labels({"labels": [dict(value, id="0")]}, [row])[0]
                accepted.append(validated)
            review_items = []
            for i, item in enumerate(accepted):
                for event in item["events"]:
                    if event["confidence"] >= .7:
                        review_items.append({"id": str(len(review_items)), "text": batch[i]["content"],
                                             "symbol": event["symbol"], "candidate_side": event["side"], "evidence_quote": event["quote"], "row_index": i})
            if review_items:
                review, error = await client.chat_completions([
                    {"role": "system", "content": REVIEW_PROMPT},
                    {"role": "user", "content": json.dumps({"candidates": [{k:v for k,v in item.items() if k!="row_index"} for item in review_items]}, ensure_ascii=False)}
                ], temperature=0, max_tokens=3500, timeout=75)
                if error != "none":
                    raise RuntimeError("Semantic review unavailable")
                doc = json.loads(re.sub(r"^```(?:json)?\s*|\s*```$", "", extract_response_text(review).strip()))
                checks = {str(r.get("id")): r for r in doc.get("reviews", [])}
                if len(doc.get("reviews", [])) != len(review_items) or set(checks) != {item["id"] for item in review_items}:
                    raise ValueError("Incomplete semantic review")
                for item in review_items:
                    check = checks[item["id"]]
                    if type(check.get("accept")) is not bool:
                        raise ValueError("Invalid semantic decision")
                    if not check["accept"] or check.get("reason") != "present_thesis":
                        row_label = accepted[item["row_index"]]
                        row_label["events"] = [e for e in row_label["events"] if not (e["symbol"] == item["symbol"] and e["side"] == item["candidate_side"])]
                        if not row_label["events"]:
                            row_label["kind"] = "other"
            for row, item in zip(batch, accepted):
                store.save_label(row, dict(item, model=model, review_prompt_sha=digest(REVIEW_PROMPT)))
            models[model] += len(batch)
            print(json.dumps({"labelled": offset+len(batch), "total": len(candidates), "model": model}), flush=True)
        except Exception as exc:
            known = {"Incomplete or duplicate model label batch", "Unexpected label ID/classification",
                     "Only directional labels may contain events", "Invalid events", "Asset absent/duplicated in source",
                     "Invalid side", "Invalid confidence", "Evidence must be an exact source excerpt",
                     "Incomplete semantic review", "Invalid semantic decision", "Incomplete label batch"}
            reason = str(exc) if type(exc) is ValueError and str(exc) in known else type(exc).__name__
            errors.append({"offset": offset, "count": len(batch), "error_type": type(exc).__name__, "reason": reason})
            print(json.dumps({"label_batch_failed": offset, "reason": reason}), flush=True)
    result = {"at": utc().isoformat(), "model_rows": len(candidates), "models": dict(models), "failures": errors}
    write_json(store.root/"label_status.json", result)
    if errors:
        raise RuntimeError("Incomplete labels; no price evaluation or forward freeze this cycle")
    return result


def profile(store, since=None, filename="profile.json"):
    labels, result, all_rows = store.labels(since), {}, store.rows(since)
    for source in SOURCES:
        rows = [r for r in all_rows if r["channel"] == source]
        texts = [r for r in rows if r["content"]]
        selected = [r for r in rows if labels.get(r["source_id"], {}).get("kind") == "directional"]
        result[source] = {"messages": len(rows), "text_messages": len(texts),
                          "authors": len({r["author_hash"] for r in rows if r.get("author_hash")}),
                          "bot_or_unknown": sum(r["sender_is_bot"] for r in rows),
                          "edited": sum(r["edited"] for r in rows), "forwarded": sum(r["forwarded"] for r in rows),
                          "first": min((r["published_at"] for r in rows), default=None),
                          "last": max((r["published_at"] for r in rows), default=None),
                          "kinds": dict(Counter(labels.get(r["source_id"], {}).get("kind", "unlabelled") for r in rows)),
                          "directional_messages": len(selected),
                          "stress_messages": sum(bool(STRESS.search(r["content"])) for r in rows if not r["sender_is_bot"])}
    write_json(store.root/filename, result)
    return result


def public_json(endpoint, params=None):
    """Public market-data GETs only; no exchange credentials or order API."""
    import requests
    from urllib.parse import quote
    if endpoint not in ("/fapi/v1/klines", "/fapi/v1/fundingRate", "/fapi/v1/time"):
        raise ValueError("Non-market endpoint forbidden")
    protocol, host, port, _, username, password = bridge.proxy_config()
    scheme = "socks5h" if protocol == "socks5" else protocol
    auth = f"{quote(username,safe='')}:{quote(password or '',safe='')}@" if username else ""
    proxy = f"{scheme}://{auth}{host}:{port}"
    response = requests.get("https://fapi.binance.com"+endpoint, params=params,
                            proxies={"http":proxy,"https":proxy}, timeout=25)
    response.raise_for_status()
    return response.json()


def market_snapshot(store, start, *, historical=False):
    path = store.root/("research_market.json" if historical else "forward_market.json")
    if historical and path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    now = utc()
    server = public_json("/fapi/v1/time")["serverTime"]/1000
    if abs(server-now.timestamp()) > 10:
        raise RuntimeError("Local/exchange clock difference exceeds 10 seconds")
    interval = "1h" if historical else "5m"
    step = 3600 if historical else 300
    cursor = int(utc(start).timestamp()//step*step*1000)
    end = int(now.timestamp()*1000)
    bars = []
    while cursor < end:
        response = public_json("/fapi/v1/klines", {"symbol":"BTCUSDT", "interval":interval,
                                                  "startTime":cursor,"endTime":end,"limit":1000})
        if not response:
            break
        for item in response:
            if int(item[6]) < end:
                bars.append({"at":utc_from_ms(item[0]), "open":float(item[1]), "close":float(item[4])})
        following = int(response[-1][0])+step*1000
        if following <= cursor:
            raise RuntimeError("Market pagination did not advance")
        cursor = following
        if len(response) < 1000:
            break
    rates = public_json("/fapi/v1/fundingRate", {"symbol":"BTCUSDT", "startTime":int(utc(start).timestamp()*1000),
                                               "endTime":end,"limit":1000})
    if len(rates) >= 1000:
        raise RuntimeError("Funding range capped; do not treat it as complete")
    funding = [{"at":utc_from_ms(r["fundingTime"]), "rate":float(r["fundingRate"]),
                "mark":float(r["markPrice"]) if r.get("markPrice") else None} for r in rates]
    result = {"received_at":utc().isoformat(), "symbol":"BTCUSDT", "interval":interval,
              "base_url":"https://fapi.binance.com", "request_start":utc(start).isoformat(),
              "server_clock_difference_seconds":server-now.timestamp(), "bars":bars, "funding":funding}
    result["sha"] = digest(result)
    write_json(path,result)
    return result


def utc_from_ms(value):
    from datetime import datetime, timezone
    return datetime.fromtimestamp(int(value)/1000,timezone.utc).isoformat()


def event_metrics(events):
    completed = [e for e in events if e["outcome"]["status"] == "settled"]
    values = [e["outcome"]["net"] for e in completed]
    excess = [e["outcome"]["net"]-e["outcome"]["same_entry_long_net"] for e in completed]
    return {"events":len(events), "settled":len(values), "pending_or_unavailable":len(events)-len(values),
            "mean_net":sum(values)/len(values) if values else None,
            "win_rate":sum(v>0 for v in values)/len(values) if values else None,
            "mean_excess_over_long":sum(excess)/len(excess) if excess else None,
            "inference":"exploratory only; no significance claim; forward promotion requires frozen gate"}


def research(store):
    rows, labels = store.rows(), store.labels()
    pending = [key for key in store.state("historical_label_selection", []) if key not in labels]
    if pending:
        raise RuntimeError("Selected historical texts remain unlabelled")
    selected = features(rows, labels)
    # Freeze all feature definitions/labels before the first historical price query.
    feature_path = store.root/"research_features.json"
    if feature_path.exists():
        frozen = json.loads(feature_path.read_text(encoding="utf-8"))
        if frozen["features"] != selected:
            raise RuntimeError("Historical feature snapshot changed; preserve original study")
    else:
        frozen = {"frozen_at":utc().isoformat(), "protocol_sha":digest(PROTOCOL), "features":selected,
                  "labels_sha":digest(labels), "profile":profile(store)}
        write_json(feature_path,frozen)
    start = min(utc(r["published_at"]) for r in rows)-timedelta(hours=12)
    market = market_snapshot(store,start,historical=True)
    last_exit, events = {}, []
    for feature in selected:
        entry = next_boundary(utc(feature["available_at"])+timedelta(seconds=60),60)
        key = feature["policy"],feature["source"]
        if key in last_exit and entry < last_exit[key]:
            continue
        end = entry+timedelta(hours=feature["holding_hours"])
        event = dict(feature,entry_at=entry.isoformat(),exit_at=end.isoformat())
        event["outcome"] = outcome(event,market["bars"],market["funding"])
        source_rows = [r for r in rows if r["channel"] == feature["source"]]
        first,last = min(utc(r["published_at"]) for r in source_rows),max(utc(r["published_at"]) for r in source_rows)
        split = first+(last-first)*.7
        event["phase"] = "development" if entry < split else "validation"
        if entry < split < end:
            event["phase"] = "purged_split_overlap"
        events.append(event)
        last_exit[key] = end
    metrics = {}
    for policy in ("cq_follow","chat_follow","chat_contrarian"):
        metrics[policy] = {phase:event_metrics([e for e in events if e["policy"]==policy and e["phase"]==phase])
                           for phase in ("development","validation","purged_split_overlap")}
    result = {"computed_at":utc().isoformat(), "protocol_sha":digest(PROTOCOL), "market_sha":market["sha"],
              "metrics":metrics,"events":events,"feature_count":len(selected),
              "conclusion":"No promotion on this bounded retrospective study; forward paper observation only"}
    write_json(store.root/"research_results.json",result)
    return result


def implementation_sha():
    return digest({"cli":hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                   "core":hashlib.sha256((ROOT/"core/research/telegram_forward.py").read_bytes()).hexdigest(),
                   "bridge":hashlib.sha256((ROOT/"scripts/telegram_desktop_bridge.py").read_bytes()).hexdigest(),
                   "text_rules":hashlib.sha256((ROOT/"core/research/telegram_text_signals.py").read_bytes()).hexdigest(),
                   "review_rules":hashlib.sha256((ROOT/"scripts/review_telegram_text.py").read_bytes()).hexdigest(),
                   "prompt":PROMPT_SHA})


def activate(store):
    old = store.state("activation")
    if old:
        if old["implementation_sha"] != implementation_sha():
            raise RuntimeError("Active forward implementation changed; explicit protocol migration required")
        return old
    if not (store.root/"research_results.json").exists():
        raise RuntimeError("Finish the source study before forward activation")
    models = {v["model"] for v in store.labels().values() if v.get("model")!="deterministic_prefilter_v1"}
    if len(models)!=1:
        raise RuntimeError("A single audited classifier model is required for activation")
    value = {"at":utc().isoformat(), "protocol_sha":digest(PROTOCOL), "implementation_sha":implementation_sha(),
             "paper_only":True, "policies":["cq_follow","chat_follow","chat_contrarian","chat_6h_follow","chat_6h_contrarian"],
             "exploratory_protocol":EXPLORATORY_PROTOCOL,
             "classifier_model":next(iter(models)),
             "first_observed_revision_allowed_forward":True, "subsequent_edits_do_not_rewrite_frozen_signals":True}
    store.db.execute("INSERT INTO state VALUES (?,?)",("activation",json.dumps(value)))
    store.append_journal("activation",value)
    store.db.commit()
    write_json(store.root/"protocol_extension_6h.json", {"frozen_at":value["at"],"protocol":EXPLORATORY_PROTOCOL})
    write_json(store.root/"activation.json",value)
    return value


def technical_features(rows, at):
    buckets = defaultdict(lambda: {"all":set(),"stress":set()})
    for row in rows:
        if row["channel"] != "Binance_api_Chinese" or row["mode"] != "forward" or row["sender_is_bot"] or not row.get("author_hash"):
            continue
        stamp = utc(row["published_at"]).replace(minute=0,second=0,microsecond=0)
        if stamp+timedelta(hours=1) > utc(at):
            continue
        bucket = buckets[stamp.isoformat()]
        bucket["all"].add(row["author_hash"])
        if STRESS.search(row["content"]):
            bucket["stress"].add(row["author_hash"])
    return [{"hour":hour,"active_authors":len(v["all"]),"stress_authors":len(v["stress"]),
             "flag":len(v["stress"])>=3 and len(v["stress"])/len(v["all"])>=.5,
             "use":"technical-risk observation; no price direction"} for hour,v in sorted(buckets.items())]


def observe_windows(store,rows,labels,collected,activation,at,pending):
    for hours in (1,6):
        close = at.replace(hour=at.hour//hours*hours,minute=0,second=0,microsecond=0)
        start = close-timedelta(hours=hours)
        health = collected["BinanceChinese"]
        if start < utc(activation["at"]) or not health["clean_since"] or start < utc(health["clean_since"]):
            continue
        authors = defaultdict(list)
        for row in rows:
            if row["channel"]!="BinanceChinese" or row["mode"]!="forward" or not start<=utc(row["published_at"])<close:
                continue
            if row.get("forwarded") or row.get("revision_observed") or row["sender_is_bot"] or not row.get("author_hash"):
                continue
            value = labels.get(row["source_id"],{})
            if value.get("kind")!="directional" or value.get("text_sha256")!=row["text_sha256"]:
                continue
            if utc(value["labelled_at"])>at or (utc(value["labelled_at"])-utc(row["first_received_at"])).total_seconds()>1800:
                continue
            for event in value.get("events",[]):
                if event["symbol"]=="BTCUSDT" and event["confidence"]>=.7:
                    authors[row["author_hash"]].append(event["side"])
        score = sum(sum(v)/len(v) for v in authors.values())/len(authors) if authors else None
        complete = health["complete_batch"] and not pending["BinanceChinese"]
        reason = "coverage_gap" if not complete else "no_directional_views" if not authors else "insufficient_authors" if len(authors)<5 else "low_consensus" if abs(score)<.4 else "qualified"
        store.observe({"feature_id":f"BinanceChinese:{hours}h:{close.isoformat()}","source":"BinanceChinese", "bucket_hours":hours,
                       "window_start":start.isoformat(),"window_end":close.isoformat(),"available_at":at.isoformat(),
                       "directional_authors":len(authors),"imbalance":score,"reason":reason,"qualified":reason=="qualified"})


async def cycle(store):
    activation = activate(store)
    store.verify_journal()
    with bridge.session_lock():
        collected = await collect(store)
    await label(store)
    at = utc()
    since = (at-timedelta(days=2)).isoformat()
    rows, labels = store.rows(since), store.labels(since)
    frozen = 0
    pending_by_source = Counter(r["channel"] for r in rows if r["mode"]=="forward"
                                and rule_kind(r)=="needs_model"
                                and (r["source_id"] not in labels or labels[r["source_id"]]["text_sha256"]!=r["text_sha256"]))
    observe_windows(store,rows,labels,collected,activation,at,pending_by_source)
    candidates = features(rows,labels,forward=True,activation=activation["at"],now=at.isoformat())
    candidates += [f for f in features(rows,labels,forward=True,activation=activation["at"],
                                      now=at.isoformat(),chat_bucket_hours=6) if f["source"]=="BinanceChinese"]
    for feature in candidates:
        source = feature["source"]
        health = collected[source]
        if not health["complete_batch"] or pending_by_source[source]:
            continue
        available = utc(feature["available_at"])
        if (at-available).total_seconds() > 1800:
            continue
        if source == "BinanceChinese":
            hour_start = utc(feature["feature_id"].split(":",1)[1])-timedelta(hours=feature.get("bucket_hours",1))
            if not health["clean_since"] or hour_start < utc(health["clean_since"]):
                continue
        elif available < utc(health["clean_since"] or activation["at"]):
            continue
        frozen += store.freeze(feature,at.isoformat())
    decisions = [json.loads(r[0]) for r in store.db.execute("SELECT payload_json FROM decisions")]
    pending = [e for e in decisions if not store.db.execute("SELECT 1 FROM outcomes WHERE event_id=?",(e["event_id"],)).fetchone()]
    for event in list(pending):
        if (at-utc(event["exit_at"])).total_seconds()>7*86400:
            payload = {"event_id":event["event_id"],"status":"data_unavailable_after_7days","net":None}
            store.db.execute("INSERT INTO outcomes VALUES (?,?,?)",(event["event_id"],utc().isoformat(),json.dumps(payload)))
            store.append_journal("outcome",payload)
            store.db.commit()
            pending.remove(event)
    start = min([utc(e["entry_at"])-timedelta(hours=12) for e in pending]+[at-timedelta(hours=24)])
    market = market_snapshot(store,start)
    for event in decisions:
        if store.db.execute("SELECT 1 FROM outcomes WHERE event_id=?",(event["event_id"],)).fetchone():
            continue
        if utc(event["exit_at"]) > at:
            continue
        result = outcome(event,market["bars"],market["funding"])
        if result["status"] != "settled":
            continue
        payload = dict(event_id=event["event_id"],**result)
        store.db.execute("INSERT INTO outcomes VALUES (?,?,?)",(event["event_id"],utc().isoformat(),json.dumps(payload)))
        store.append_journal("outcome",payload)
        store.db.commit()
    store.expire_text(at.isoformat())
    for path in (store.root/"label_attempts").glob("*.json"):
        if path.stat().st_mtime < (at-timedelta(days=30)).timestamp():
            path.unlink()
    write_json(store.root/"technical_features.json",technical_features(rows,at.isoformat()))
    settled = store.db.execute("SELECT count(*) FROM outcomes WHERE json_extract(payload_json,'$.status')='settled'").fetchone()[0]
    closed = store.db.execute("SELECT count(*) FROM outcomes").fetchone()[0]
    observations = store.db.execute("SELECT count(*) FROM observations").fetchone()[0]
    status = {"checked_at":utc().isoformat(),"status":"active_paper_observation","activation":activation,
              "sources":collected,"pending_labels":dict(pending_by_source),"new_decisions":frozen,
              "total_decisions":len(decisions),"settled":settled,"pending_or_unsettled":len(decisions)-closed,
              "unavailable_outcomes":closed-settled,"feature_observations":observations,
              "journal":store.verify_journal(), "market_received_at":market["received_at"],
              "live_trading":False}
    write_json(store.root/"forward_status.json",status)
    profile(store,since,"rolling_profile.json")
    return status


async def main_async(args):
    output = Path(args.output).resolve()
    store = setup(output)
    with study_lock(output):
        await dispatch(args,store)


async def dispatch(args,store):
    if args.command == "collect":
        with bridge.session_lock():
            await collect(store, historical=args.history)
        print(json.dumps(profile(store)))
    elif args.command == "label":
        await label(store, historical=args.history)
        print(json.dumps(profile(store)))
    elif args.command == "profile":
        print(json.dumps(profile(store)))
    elif args.command == "research":
        result = research(store)
        print(json.dumps({k:v for k,v in result.items() if k!="events"}))
    elif args.command == "activate":
        print(json.dumps(activate(store)))
    elif args.command == "cycle":
        print(json.dumps(await cycle(store)))
    else:
        raise ValueError("Command not implemented yet")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["collect", "label", "profile", "research", "activate", "cycle"])
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--history", action="store_true")
    args = parser.parse_args()
    logging.disable(logging.CRITICAL)
    try:
        asyncio.run(main_async(args))
    except Exception as exc:
        result = {"status": "failed", "error_type": type(exc).__name__,"at":utc().isoformat()}
        if type(exc).__name__ in ("FloodWaitError","CooldownActive"):
            result["retry_after_seconds"] = exc.seconds
            result["retry_at"] = (utc()+timedelta(seconds=exc.seconds)).isoformat()
        output = args.output.resolve()
        if output.is_relative_to((ROOT/"data/research").resolve()) and output.exists():
            write_json(output/"last_error.json",result)
        print(json.dumps(result), flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
