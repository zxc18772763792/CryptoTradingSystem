"""Credential boundaries for the local Telegram feasibility bridge."""
import asyncio
import hashlib
import importlib.util
import os
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture
def bridge():
    pytest.importorskip("telethon", reason="Optional local Telegram bridge dependency")
    path = Path(__file__).resolve().parents[1] / "scripts/telegram_desktop_bridge.py"
    spec = importlib.util.spec_from_file_location("telegram_desktop_bridge_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.skipif(os.name != "nt", reason="Windows CurrentUser DPAPI")
def test_encrypted_data_roundtrips_and_rejects_wrong_context(bridge, monkeypatch):
    value = b"synthetic-session-fixture-not-an-account"
    encrypted = bridge.dpapi(value)
    assert value not in encrypted
    assert bridge.dpapi(encrypted, decrypt=True) == value
    monkeypatch.setattr(bridge, "ENTROPY", b"different-application")
    with pytest.raises(RuntimeError, match="DPAPI operation failed"):
        bridge.dpapi(encrypted, decrypt=True)


@pytest.mark.parametrize("extra_file", [False, True])
def test_unreviewed_reader_code_is_rejected_before_import(bridge, monkeypatch, tmp_path, extra_file):
    folder = tmp_path / "src/tdata_reader"
    folder.mkdir(parents=True)
    original = b"raise AssertionError('fixture must never execute')"
    (folder / "__init__.py").write_bytes(original if extra_file else b"tampered")
    if extra_file:
        (folder / "unexpected.py").write_bytes(b"pass")
    monkeypatch.setattr(bridge, "READER_ROOT", tmp_path)
    monkeypatch.setattr(bridge, "SOURCE_HASHES", {"__init__.py": hashlib.sha256(original).hexdigest()})
    with pytest.raises(RuntimeError, match="source (inventory|integrity) mismatch"):
        bridge.load_reader()


def test_storage_failure_stops_before_reading_desktop_credentials(bridge, monkeypatch, tmp_path):
    monkeypatch.setattr(bridge, "store_path", lambda: tmp_path / "session.dpapi")
    monkeypatch.setattr(bridge, "desktop_running", lambda: False)

    def denied(_):
        raise PermissionError("synthetic ACL denial")

    def forbidden():
        raise AssertionError("Must not read credentials before storage preflight")

    monkeypatch.setattr(bridge, "protect_directory", denied)
    monkeypatch.setattr(bridge, "load_reader", forbidden)
    with pytest.raises(PermissionError):
        asyncio.run(bridge.bootstrap(SimpleNamespace(tdata=tmp_path), {}))


def test_failed_save_revokes_new_authorization_and_preserves_desktop(bridge, monkeypatch, tmp_path):
    from telethon import functions

    account = SimpleNamespace(user_id=1, dc_id=2, dc_ip="127.0.0.1", dc_port=443,
                              auth_key=b"a" * 256, api_id=12345, api_hash="synthetic")
    calls = []

    class Client:
        def __init__(self, independent):
            self.independent = independent
            self.session = SimpleNamespace(auth_key=SimpleNamespace(key=b"b" * 256))
            self.disconnected = False

        async def connect(self):
            pass

        async def disconnect(self):
            self.disconnected = True

        async def get_me(self):
            return SimpleNamespace(id=1)

        async def qr_login(self):
            async def wait(timeout):
                return SimpleNamespace(id=1)
            return SimpleNamespace(token=b"synthetic-token", wait=wait)

        async def __call__(self, request):
            calls.append(request)
            if isinstance(request, functions.auth.AcceptLoginTokenRequest):
                return SimpleNamespace(hash=12345)
            if isinstance(request, functions.account.ResetAuthorizationRequest):
                return True
            raise AssertionError("Unexpected account operation")

    original, independent = Client(False), Client(True)
    clients = iter((original, independent))
    monkeypatch.setattr(bridge, "store_path", lambda: tmp_path / "session.dpapi")
    monkeypatch.setattr(bridge, "desktop_running", lambda: False)
    monkeypatch.setattr(bridge, "protect_directory", lambda _: None)
    monkeypatch.setattr(bridge, "dpapi", lambda data, **kwargs: data)
    monkeypatch.setattr(bridge, "load_reader", lambda: lambda _: [account])
    monkeypatch.setattr(bridge, "make_client", lambda *args, **kwargs: next(clients))

    def fail_save(*args, **kwargs):
        raise OSError("synthetic storage failure")

    monkeypatch.setattr(bridge, "save_session", fail_save)
    report = {}
    with pytest.raises(OSError):
        asyncio.run(bridge.bootstrap(SimpleNamespace(tdata=tmp_path), report))
    assert [type(request) for request in calls] == [
        functions.auth.AcceptLoginTokenRequest, functions.account.ResetAuthorizationRequest]
    assert calls[-1].hash == 12345
    assert original.disconnected and independent.disconnected
    assert report["unused_authorization_removed"] is True


def test_event_metadata_is_deduplicated_without_reading_content(bridge):
    class Message:
        id = 42
        edit_date = None

        @property
        def message(self):
            raise AssertionError("Message body must not be accessed")

        @property
        def sender(self):
            raise AssertionError("Sender must not be accessed")

    observer = bridge.MetadataObserver(["allowed"])
    message = Message()
    observer.record("allowed", message, "new", "push")
    observer.record("allowed", message, "new", "difference")
    observer.record("unrelated-private-chat", message, "new", "push")
    observer.record("allowed", message, "edited", "difference")
    assert observer.counts["allowed"]["new"] == 1
    assert observer.counts["allowed"]["edited"] == 1
    assert observer.counts["allowed"]["push"] == 1
    assert observer.counts["allowed"]["difference"] == 1
    assert set(observer.counts) == {"allowed"}


def test_missing_channel_state_cannot_be_reported_as_successful_watch(bridge):
    from datetime import datetime, timezone
    from telethon import types

    async def run():
        class Client:
            disconnected = asyncio.get_running_loop().create_future()

            def add_event_handler(self, *args):
                pass

            def remove_event_handler(self, *args):
                pass

            async def set_receive_updates(self, enabled):
                assert enabled

            async def __call__(self, request):
                return SimpleNamespace(dialogs=[])

            def is_connected(self):
                return True

        entity = types.Channel(id=1, title="synthetic fixture", photo=types.ChatPhotoEmpty(),
                               date=datetime.now(timezone.utc), access_hash=123, megagroup=True)
        report = {}
        with pytest.raises(RuntimeError, match="Missing initial channel update state"):
            await bridge.observe_sources(Client(), {"allowed": entity}, 1, report)
        assert not report.get("watch_completed")
        assert report["channel_difference_calls"] == {"allowed": 0}

    asyncio.run(run())


def test_channel_difference_delivers_message_when_passive_push_is_absent(bridge):
    from datetime import datetime, timezone
    from telethon import functions, types

    async def run():
        message = SimpleNamespace(id=42, peer_id=types.PeerChannel(1))

        class Client:
            disconnected = asyncio.get_running_loop().create_future()

            def add_event_handler(self, *args):
                pass  # Simulate the observed failure: no passive push callbacks.

            def remove_event_handler(self, *args):
                pass

            async def set_receive_updates(self, enabled):
                assert enabled

            async def __call__(self, request):
                if isinstance(request, functions.messages.GetPeerDialogsRequest):
                    return SimpleNamespace(dialogs=[SimpleNamespace(peer=types.PeerChannel(1), pts=100)])
                assert isinstance(request, functions.updates.GetChannelDifferenceRequest)
                assert request.pts == 100
                return SimpleNamespace(pts=101, new_messages=[message], other_updates=[],
                                       final=True, timeout=60)

            def is_connected(self):
                return True

        entity = types.Channel(id=1, title="synthetic fixture", photo=types.ChatPhotoEmpty(),
                               date=datetime.now(timezone.utc), access_hash=123, megagroup=True)
        report = {}
        await bridge.observe_sources(Client(), {"allowed": entity}, 0.02, report)
        assert report["watch_outcome"] == "source_events_received"
        assert report["event_counts"]["allowed"]["new"] == 1
        assert report["event_counts"]["allowed"]["push"] == 0
        assert report["event_counts"]["allowed"]["difference"] == 1
        assert report["channel_difference_calls"] == {"allowed": 1}

    asyncio.run(run())
