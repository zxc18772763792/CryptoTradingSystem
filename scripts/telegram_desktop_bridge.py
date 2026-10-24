"""Local desktop-session feasibility bridge; never prints messages or credentials.

Requires the separately reviewed reader source at its pinned runtime path.
Bootstrap uses a desktop authorization once to approve a distinct MTProto key.
The independent key is saved with Windows CurrentUser DPAPI, outside the repo.
This is an access probe, not an AI collector or a trading worker.
"""
from __future__ import annotations

import argparse
import asyncio
import ctypes
from contextlib import contextmanager
from ctypes import wintypes
from datetime import datetime, timezone
import getpass
import hashlib
import json
import logging
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
READER_COMMIT = "364a8479110bdc70badef891760ebf883da1527a"
READER_ROOT = ROOT / "runtime/telegram_tool_audit/tdata_reader" / READER_COMMIT
SOURCE_HASHES = {
    "__init__.py": "20ae86128d289958870a4368b0d828478b7ae45b1d99747b6797eb2b4ac6a258",
    "api.py": "a94da8c5455076ffec8b94234d489ff493cf1d9c0f760f5ca7cd1a437f9b8965",
    "crypto.py": "9ef6370d3f4b0d1a591362f25b52fe5ca7056b54655fdaeda0d26b6afc468e6f",
    "exceptions.py": "25690137eb486d6b73434487c87fc1507f9e8c5ac016e7d3542d62613f741912",
    "qt_reader.py": "30d386f52ab6228cd0cc57e7560d0c79516390b939e39eb88796b65ae6056f87",
    "reader.py": "c87b00fc37e6a5fc1be5abd7146fca381f8af0c1e017b3a0771446b23ccb52e3",
    "tdata.py": "ba2c7255ac6a748c08019f17efbc8bdd9ae568e06f0c7aabe6d446a14d1eb043",
}
SOURCES = ("cryptoquant_official", "BinanceChinese", "Binance_api_Chinese")
ENTROPY = b"CryptoTradingSystem.TelegramBridge.v1"


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def load_reader():
    folder = READER_ROOT / "src/tdata_reader"
    actual = {p.name for p in folder.glob("*.py")}
    if actual != set(SOURCE_HASHES):
        raise RuntimeError("Reviewed reader source inventory mismatch")
    for name, digest in SOURCE_HASHES.items():
        if hashlib.sha256((folder / name).read_bytes()).hexdigest() != digest:
            raise RuntimeError("Reviewed reader source integrity mismatch")
    sys.dont_write_bytecode = True
    sys.path.insert(0, str((READER_ROOT / "src").resolve()))
    from tdata_reader import read_all_tdata
    return read_all_tdata


def dpapi(data: bytes, *, decrypt=False):
    if os.name != "nt":
        raise RuntimeError("Windows CurrentUser DPAPI required")

    class Blob(ctypes.Structure):
        _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_ubyte))]

    def blob(value):
        buffer = (ctypes.c_ubyte * len(value)).from_buffer_copy(value)
        return Blob(len(value), buffer), buffer

    incoming, keep_input = blob(data)
    entropy, keep_entropy = blob(ENTROPY)
    output = Blob()
    crypto = ctypes.WinDLL("crypt32", use_last_error=True)
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.LocalFree.argtypes = [wintypes.HLOCAL]
    kernel.LocalFree.restype = wintypes.HLOCAL
    fn = crypto.CryptUnprotectData if decrypt else crypto.CryptProtectData
    fn.argtypes = [ctypes.POINTER(Blob), ctypes.c_void_p, ctypes.POINTER(Blob),
                   ctypes.c_void_p, ctypes.c_void_p, wintypes.DWORD, ctypes.POINTER(Blob)]
    fn.restype = wintypes.BOOL
    # No LOCAL_MACHINE flag. UI_FORBIDDEN avoids any invisible password dialog.
    if not fn(ctypes.byref(incoming), None, ctypes.byref(entropy), None, None, 1, ctypes.byref(output)):
        raise RuntimeError("DPAPI operation failed")
    try:
        return ctypes.string_at(output.pbData, output.cbData)
    finally:
        kernel.LocalFree(ctypes.cast(output.pbData, wintypes.HLOCAL))


def store_path():
    if os.name != "nt":
        raise RuntimeError("Windows user directory required")
    # AppData writes from the packaged Codex host may be MSIX-virtualized.
    # A fixed user-profile directory is shared by this host and ordinary terminals.
    return Path.home() / ".crypto_trading_system/telegram/reader_session.dpapi"


@contextmanager
def session_lock():
    """Serialize local consumers of the same independent authorization."""
    import msvcrt
    path = store_path().with_name("reader.lock")
    protect_directory(path.parent)
    with path.open("a+b") as stream:
        if stream.tell() == 0:
            stream.write(b"\0")
            stream.flush()
        stream.seek(0)
        msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        try:
            yield
        finally:
            stream.seek(0)
            msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)


def protect_directory(folder):
    folder.mkdir(parents=True, exist_ok=True)
    if folder.resolve() != folder.absolute():
        raise RuntimeError("Session directory must not redirect elsewhere")
    env = dict(os.environ, CTS_TELEGRAM_STORE=str(folder))
    command = r"""
$ErrorActionPreference = 'Stop'
$folder = $env:CTS_TELEGRAM_STORE
$owner = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
$acl = New-Object System.Security.AccessControl.DirectorySecurity
$acl.SetOwner($owner)
$acl.SetAccessRuleProtection($true, $false)
foreach ($sid in @($owner, (New-Object System.Security.Principal.SecurityIdentifier('S-1-5-18')))) {
    $rule = New-Object System.Security.AccessControl.FileSystemAccessRule($sid, 'FullControl', 'ContainerInherit,ObjectInherit', 'None', 'Allow')
    $acl.AddAccessRule($rule)
}
[System.IO.Directory]::SetAccessControl($folder, $acl)
"""
    result = subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", command],
                            env=env, capture_output=True, timeout=15)
    if result.returncode:
        raise RuntimeError("Could not protect session directory")


def save_session(client, account_id, *, pending_password=False, replace=False):
    from telethon.sessions import StringSession
    path = store_path()
    protect_directory(path.parent)
    payload = {"session": StringSession.save(client.session), "account_id": account_id,
               "api_id": client.api_id, "api_hash": client.api_hash,
               "pending_password": pending_password, "created_at": utcnow()}
    encrypted = dpapi(json.dumps(payload).encode("utf-8"))
    with path.open("wb" if replace else "xb") as stream:
        stream.write(encrypted)
    if json.loads(dpapi(path.read_bytes(), decrypt=True)) != payload:
        raise RuntimeError("Encrypted session roundtrip failed")


def proxy_config():
    from dotenv import dotenv_values
    from urllib.parse import urlsplit, unquote
    values = {}
    for filename in (".env", ".env.local"):
        values.update({k: v for k, v in dotenv_values(ROOT / filename).items() if v})
    values.update({k: v for k, v in os.environ.items() if v})
    value = next((values[k] for k in ("ALL_PROXY", "HTTPS_PROXY", "HTTP_PROXY") if values.get(k)), "")
    parsed = urlsplit(value)
    protocol = {"http": "http", "socks5": "socks5", "socks5h": "socks5", "socks4": "socks4"}.get(parsed.scheme)
    if not protocol or not parsed.hostname or not parsed.port:
        raise RuntimeError("Explicit supported proxy required; direct fallback disabled")
    return (protocol, parsed.hostname, parsed.port, True,
            unquote(parsed.username) if parsed.username else None,
            unquote(parsed.password) if parsed.password else None)


def make_client(session, api_id, api_hash, *, updates=False):
    from telethon import TelegramClient
    return TelegramClient(session, api_id, api_hash, device_model="CryptoTradingSystem reader",
                          app_version="0.1", proxy=proxy_config(), receive_updates=updates,
                          auto_reconnect=False, connection_retries=0, request_retries=0,
                          flood_sleep_threshold=0, timeout=12)


async def connect(client):
    await asyncio.wait_for(client.connect(), timeout=25)


def desktop_running():
    result = subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive", "-Command",
                             "if (Get-Process -Name Telegram -ErrorAction SilentlyContinue) { exit 1 }"],
                            capture_output=True, timeout=10)
    return result.returncode != 0


class MetadataObserver:
    """Count a source event once across push and channel-difference delivery."""

    def __init__(self, sources):
        self.counts = {name: {"new": 0, "edited": 0, "push": 0, "difference": 0,
                              "last_received_at": None} for name in sources}
        self.seen = {name: set() for name in sources}

    def record(self, source, message, kind, route):
        if source not in self.counts:
            return
        marker = (kind, message.id, getattr(message, "edit_date", None) if kind == "edited" else None)
        if marker in self.seen[source]:
            return
        self.seen[source].add(marker)
        self.counts[source][kind] += 1
        self.counts[source][route] += 1
        self.counts[source]["last_received_at"] = utcnow()


async def observe_sources(client, entities, seconds, report):
    """Use official channel differences as well as passive push.

    Telegram may stop passive delivery for a channel. Its documented fallback
    is getChannelDifference at the returned timeout, not getHistory polling.
    Only explicitly selected, joined sources are short-polled during this probe.
    """
    from telethon import events, functions, types, utils
    observer = MetadataObserver(entities)
    source_ids = {entity.id: name for name, entity in entities.items()}

    async def pushed(event, kind):
        peer_id = getattr(event.message.peer_id, "channel_id", None)
        observer.record(source_ids.get(peer_id), event.message, kind, "push")

    async def new_message(event):
        await pushed(event, "new")

    async def edited_message(event):
        await pushed(event, "edited")

    client.add_event_handler(new_message, events.NewMessage())
    client.add_event_handler(edited_message, events.MessageEdited())
    report["watch_started_at"] = utcnow()
    await client.set_receive_updates(True)
    dialogs = await client(functions.messages.GetPeerDialogsRequest([
        types.InputDialogPeer(utils.get_input_peer(entity)) for entity in entities.values()]))
    states = {getattr(dialog.peer, "channel_id", None): getattr(dialog, "pts", None)
              for dialog in dialogs.dialogs}
    deadline = asyncio.get_running_loop().time() + seconds
    difference_calls = {name: 0 for name in entities}

    async def channel_differences(name, entity):
        pts = states.get(entity.id)
        if not pts:
            raise RuntimeError("Missing initial channel update state")
        while asyncio.get_running_loop().time() < deadline:
            result = await client(functions.updates.GetChannelDifferenceRequest(
                channel=entity, filter=types.ChannelMessagesFilterEmpty(), pts=pts, limit=100))
            difference_calls[name] += 1
            if isinstance(result, types.updates.ChannelDifferenceTooLong):
                report["channel_gap_too_long"] = name
                raise RuntimeError("Channel update gap requires explicit recovery")
            new_pts = result.pts
            if new_pts < pts:
                raise RuntimeError("Channel update state regressed")
            for message in getattr(result, "new_messages", []):
                if getattr(message.peer_id, "channel_id", None) == entity.id:
                    observer.record(name, message, "new", "difference")
            for update in getattr(result, "other_updates", []):
                if isinstance(update, types.UpdateEditChannelMessage):
                    if getattr(update.message.peer_id, "channel_id", None) == entity.id:
                        observer.record(name, update.message, "edited", "difference")
            final = getattr(result, "final", True)
            if not final and new_pts == pts:
                raise RuntimeError("Non-final channel difference made no progress")
            pts = new_pts
            if final:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining > 0:
                    await asyncio.sleep(min(max(1, getattr(result, "timeout", None) or 1), remaining))

    tasks = [asyncio.create_task(channel_differences(name, entity))
             for name, entity in entities.items()]
    try:
        done, _ = await asyncio.wait([*tasks, client.disconnected], timeout=seconds,
                                     return_when=asyncio.FIRST_COMPLETED)
        if not client.is_connected():
            raise ConnectionError("Disconnected during source observation")
        for task in done:
            task.result()
        report["watch_completed"] = True
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        client.remove_event_handler(new_message)
        client.remove_event_handler(edited_message)
        report["watch_seconds"] = seconds
        report["event_counts"] = observer.counts
        report["channel_difference_calls"] = difference_calls
    report["watch_outcome"] = "source_events_received" if any(
        row["new"] or row["edited"] for row in observer.counts.values()) else "no_source_events_observed"


async def bootstrap(args, report):
    from telethon import errors, functions
    from telethon.crypto import AuthKey
    from telethon.sessions import MemorySession
    if store_path().exists():
        raise FileExistsError("Independent session already exists; use --probe")
    if desktop_running():
        raise RuntimeError("Close Telegram Desktop before bootstrap")
    report["stage"] = "storage_preflight"
    protect_directory(store_path().parent)
    fixture = b"non-secret storage preflight"
    if dpapi(dpapi(fixture), decrypt=True) != fixture:
        raise RuntimeError("DPAPI preflight failed")
    accounts = load_reader()(args.tdata)
    if len(accounts) != 1:
        raise RuntimeError("Exactly one desktop account required; explicit selection needed otherwise")
    account = accounts[0]
    original_session = MemorySession()
    original_session.set_dc(account.dc_id, account.dc_ip, account.dc_port)
    original_session.auth_key = AuthKey(account.auth_key)
    original = make_client(original_session, account.api_id, account.api_hash)
    independent = make_client(MemorySession(), account.api_id, account.api_hash, updates=True)
    created_authorization = None
    persisted = False
    try:
        report["stage"] = "desktop_authentication"
        await connect(original)
        me = await original.get_me()
        if me is None or me.id != account.user_id:
            raise RuntimeError("Desktop identity verification failed")
        report["desktop_identity_verified"] = True
        report["stage"] = "independent_authorization"
        await connect(independent)
        qr = await independent.qr_login()
        waiting = asyncio.create_task(qr.wait(timeout=35))
        await asyncio.sleep(0)  # Register the event handler before accepting the token.
        try:
            accepted = await original(functions.auth.AcceptLoginTokenRequest(qr.token))
            if getattr(accepted, "current", False):
                raise RuntimeError("Refusing to treat the desktop authorization as a new session")
            created_authorization = accepted.hash
            try:
                new_me = await waiting
            except errors.SessionPasswordNeededError:
                save_session(independent, account.user_id, pending_password=True)
                persisted = True
                report["status"] = "local_two_step_password_required"
                return
        finally:
            if not waiting.done():
                waiting.cancel()
                await asyncio.gather(waiting, return_exceptions=True)
        if new_me.id != account.user_id:
            raise RuntimeError("Independent identity mismatch")
        if independent.session.auth_key.key == account.auth_key:
            raise RuntimeError("Independent authorization key was not distinct")
        report["stage"] = "encrypted_session_storage"
        save_session(independent, account.user_id)
        persisted = True
        report.update(status="independent_session_created", distinct_authorization=True,
                      encrypted_at_rest=True)
    finally:
        if created_authorization is not None and not persisted:
            try:
                await original(functions.account.ResetAuthorizationRequest(created_authorization))
                report["unused_authorization_removed"] = True
            except Exception as exc:
                report["cleanup_error_type"] = type(exc).__name__
        await independent.disconnect()
        await original.disconnect()  # Never log out the desktop authorization.


async def probe(args, report):
    from telethon import errors, functions, types
    from telethon.sessions import StringSession
    payload = json.loads(dpapi(store_path().read_bytes(), decrypt=True))
    client = make_client(StringSession(payload["session"]), payload["api_id"], payload["api_hash"],
                         updates=args.watch_seconds > 0)
    try:
        await connect(client)
        if payload.get("pending_password"):
            if not args.complete_login or not sys.stdin.isatty():
                report["status"] = "local_two_step_password_required"
                return
            password = getpass.getpass("Telegram two-step password (local only): ")
            try:
                await client.sign_in(password=password)
            finally:
                password = None
        me = await client.get_me()
        if me is None or me.id != payload["account_id"]:
            raise RuntimeError("Saved session identity verification failed")
        if payload.get("pending_password"):
            save_session(client, me.id, replace=True)
        report.update(status="authenticated", identity_verified=True, sources=[])
        watched_entities = {}
        history_baseline = {}
        for username in SOURCES:
            item = {"source": username}
            try:
                resolved = await client(functions.contacts.ResolveUsernameRequest(username))
                entity = next(c for c in resolved.chats if isinstance(c, types.Channel))
                item["resolved"] = True
                item["source_type"] = "group" if entity.megagroup else "channel"
                try:
                    await client(functions.channels.GetParticipantRequest(entity, "me"))
                    item["member"] = True
                    watched_entities[username] = entity
                except errors.UserNotParticipantError:
                    item["member"] = False
                latest = await client.get_messages(entity, limit=1)
                item.update(history_readable=True, has_message=bool(latest),
                            latest_published_at=latest[0].date.isoformat() if latest else None)
                history_baseline[username] = latest[0].id if latest else None
            except errors.FloodWaitError:
                raise
            except Exception as exc:
                item["error_type"] = type(exc).__name__
            report["sources"].append(item)
        if args.watch_seconds:
            await observe_sources(client, watched_entities, args.watch_seconds, report)
            # Distinguish a quiet source from messages advancing without events.
            report["history_advanced_during_watch"] = {}
            for username, entity in watched_entities.items():
                latest = await client.get_messages(entity, limit=1)
                report["history_advanced_during_watch"][username] = bool(
                    latest and latest[0].id > (history_baseline.get(username) or 0))
                if (report["history_advanced_during_watch"][username]
                        and report["event_counts"][username]["new"] == 0):
                    report["watch_outcome"] = "history_advanced_without_message_event"
                    report["status"] = "watch_gap_detected"
    finally:
        await client.disconnect()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--offline", action="store_true")
    mode.add_argument("--bootstrap", action="store_true")
    mode.add_argument("--probe", action="store_true")
    parser.add_argument("--tdata", type=Path)
    parser.add_argument("--complete-login", action="store_true")
    parser.add_argument("--watch-seconds", type=int, default=0, choices=range(0, 301), metavar="0..300")
    args = parser.parse_args()
    if (args.offline or args.bootstrap) and not args.tdata:
        parser.error("--tdata is required for offline/bootstrap modes")
    logging.disable(logging.CRITICAL)
    report = {"checked_at": utcnow(), "reader_commit": READER_COMMIT}
    exit_code = 0
    try:
        if args.offline:
            accounts = load_reader()(args.tdata)
            report.update(status="offline_ok", account_count=len(accounts),
                          authorization_lengths_valid=all(len(a.auth_key) == 256 for a in accounts))
        else:
            with session_lock():
                asyncio.run(asyncio.wait_for(bootstrap(args, report) if args.bootstrap else probe(args, report),
                                            timeout=150 + args.watch_seconds))
            if report.get("status") == "watch_gap_detected":
                exit_code = 1
    except Exception as exc:
        report.update(status="failed", error_type=type(exc).__name__)
        if type(exc).__name__ == "FloodWaitError":
            report["retry_after_seconds"] = exc.seconds
        exit_code = 1
    # Intentionally omit exception strings, user IDs, titles, senders, text and credentials.
    folder = ROOT / "runtime"
    folder.mkdir(exist_ok=True)
    (folder / "telegram_bridge_status.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
