"""Tests for the ProactorEventLoop accept hardening (WinError-64 listener kill).

The live incident: a client RST on a connection queued in the accept backlog
made AcceptEx complete with ERROR_NETNAME_DELETED; CPython's _start_serving
closed the LISTENING socket on that transient error, leaving the service
"alive but headless". These tests pin the transient-vs-fatal classification
and drive the patched accept loop directly with fake futures.
"""
from __future__ import annotations

import sys
from typing import Any, Callable, List, Optional

import pytest

from core.utils.proactor_accept_hardening import (
    _patched_start_serving,
    install_proactor_accept_hardening,
    is_transient_accept_error,
)


def _oserror_with_winerror(winerror: int) -> OSError:
    exc = OSError(22, "synthetic")
    exc.winerror = winerror  # plain OSError carries winerror on Windows
    return exc


def test_transient_classification_covers_the_incident_and_friends():
    # The exact live incident: ERROR_NETNAME_DELETED from AcceptEx.
    assert is_transient_accept_error(_oserror_with_winerror(64)) is True
    # Aborted overlapped op / winsock resets while queued.
    assert is_transient_accept_error(_oserror_with_winerror(995)) is True
    assert is_transient_accept_error(_oserror_with_winerror(10053)) is True
    assert is_transient_accept_error(_oserror_with_winerror(10054)) is True
    # Mapped forms (finish_send-style mapping, or POSIX).
    assert is_transient_accept_error(ConnectionResetError()) is True
    assert is_transient_accept_error(ConnectionAbortedError()) is True


def test_fatal_oserrors_are_not_classified_transient():
    assert is_transient_accept_error(OSError(24, "too many open files")) is False
    assert is_transient_accept_error(_oserror_with_winerror(10038)) is False  # WSAENOTSOCK
    assert is_transient_accept_error(ValueError("not an oserror")) is False


class _FakeFuture:
    def __init__(self, result: Any = None, exc: Optional[BaseException] = None):
        self._result = result
        self._exc = exc
        self.callbacks: List[Callable[["_FakeFuture"], None]] = []

    def result(self):
        if self._exc is not None:
            raise self._exc
        return self._result

    def add_done_callback(self, cb):
        self.callbacks.append(cb)


class _FakeProactor:
    def __init__(self):
        self.accept_calls = 0
        self.pending: List[_FakeFuture] = []
        self.accept_raises: Optional[BaseException] = None

    def accept(self, sock):
        self.accept_calls += 1
        if self.accept_raises is not None:
            raise self.accept_raises
        fut = _FakeFuture()
        self.pending.append(fut)
        return fut


class _FakeSock:
    def __init__(self):
        self.closed = False

    def fileno(self):
        return -1 if self.closed else 5

    def close(self):
        self.closed = True


class _FakeLoop:
    """Just enough of BaseProactorEventLoop for the patched closure."""

    def __init__(self):
        self._proactor = _FakeProactor()
        self._accept_futures = {}
        self._debug = False
        self.exception_reports: List[dict] = []
        self.transports_made = 0

    def call_soon(self, fn):
        fn()

    def is_closed(self):
        return False

    def call_exception_handler(self, context):
        self.exception_reports.append(context)

    def _make_socket_transport(self, conn, protocol, extra=None, server=None):
        self.transports_made += 1

    def _make_ssl_transport(self, *args: Any, **kwargs: Any):  # pragma: no cover
        self.transports_made += 1


def _drive_accept_chain(loop: _FakeLoop, sock: _FakeSock):
    """Install the patched accept chain and return the current pending future."""
    _patched_start_serving(loop, protocol_factory=lambda: object(), sock=sock)
    assert loop._proactor.accept_calls == 1
    return loop._proactor.pending[-1]


def test_patched_loop_keeps_listener_alive_on_transient_accept_error():
    loop = _FakeLoop()
    sock = _FakeSock()
    fut = _drive_accept_chain(loop, sock)

    # Simulate AcceptEx completing with the live-incident error (WinError 64).
    failed = _FakeFuture(exc=_oserror_with_winerror(64))
    fut.callbacks[0](failed)

    assert sock.closed is False, "transient accept error must not close the listener"
    assert loop._proactor.accept_calls == 2, "accept must be re-armed"
    assert loop._proactor.pending[-1].callbacks, "re-armed accept must chain the loop"
    assert loop.exception_reports == []


def test_patched_loop_still_closes_listener_on_fatal_accept_error():
    loop = _FakeLoop()
    sock = _FakeSock()
    fut = _drive_accept_chain(loop, sock)

    failed = _FakeFuture(exc=OSError(24, "too many open files"))
    fut.callbacks[0](failed)

    assert sock.closed is True, "fatal errors keep the original close semantics"
    assert len(loop.exception_reports) == 1
    assert loop.exception_reports[0]["message"] == "Accept failed on a socket"


def test_patched_loop_closes_listener_when_rearm_itself_fails():
    loop = _FakeLoop()
    sock = _FakeSock()
    fut = _drive_accept_chain(loop, sock)

    # Transient completion error, but the re-arm accept() also fails hard.
    loop._proactor.accept_raises = OSError(24, "totally broken")
    failed = _FakeFuture(exc=_oserror_with_winerror(64))
    fut.callbacks[0](failed)

    assert sock.closed is True
    assert len(loop.exception_reports) == 1


def test_patched_loop_accepts_connections_normally():
    loop = _FakeLoop()
    sock = _FakeSock()
    fut = _drive_accept_chain(loop, sock)

    ok = _FakeFuture(result=(object(), ("127.0.0.1", 55555)))
    fut.callbacks[0](ok)

    assert loop.transports_made == 1
    assert loop._proactor.accept_calls == 2  # next accept armed
    assert sock.closed is False


@pytest.mark.skipif(sys.platform != "win32", reason="proactor hardening is Windows-only")
def test_install_is_idempotent_and_targets_current_python():
    import core.utils.proactor_accept_hardening as hardening
    from asyncio import proactor_events

    original_method = proactor_events.BaseProactorEventLoop._start_serving
    original_flag = hardening._INSTALLED
    try:
        hardening._INSTALLED = False
        # Force a re-install so this test is deterministic regardless of
        # whether web.main (which installs at import) was imported earlier.
        proactor_events.BaseProactorEventLoop._start_serving = (
            original_method if original_method is not _patched_start_serving else original_method
        )
        assert install_proactor_accept_hardening() is True
        assert proactor_events.BaseProactorEventLoop._start_serving is _patched_start_serving
        # Second call is a no-op success.
        assert install_proactor_accept_hardening() is True
    finally:
        proactor_events.BaseProactorEventLoop._start_serving = original_method
        hardening._INSTALLED = original_flag
