"""Harden asyncio's Windows ProactorEventLoop against transient accept errors.

Root cause (observed live 2026-07-02, plan §33.x "alive but headless" incident):
a client that aborts (RST) its connection while it is still queued in the
listen backlog makes the pending ``AcceptEx`` completion fail with
``ERROR_NETNAME_DELETED`` (WinError 64). CPython's
``BaseProactorEventLoop._start_serving`` handles *any* ``OSError`` from the
accept chain by closing the LISTENING socket permanently::

    except OSError as exc:
        if sock.fileno() != -1:
            self.call_exception_handler({'message': 'Accept failed on a socket', ...})
            sock.close()          # <- one RST kills the whole HTTP server

The process keeps running (feed, strategies, executors) but the API is dead
and never comes back. ``IocpProactor.finish_send`` maps WinError 64/995 to
``ConnectionResetError`` but ``finish_accept`` does not, so the error reaches
``_start_serving`` as a plain ``OSError`` either way — and even a mapped
``ConnectionResetError`` *is* an ``OSError``, so the listener would still be
closed.

Fix: install a patched ``_start_serving`` whose error handling distinguishes
transient per-connection failures (client aborted while queued) from genuine
listener-fatal errors. Transient -> log a warning and re-arm ``accept`` on the
same listening socket; anything else -> original behavior (report + close).

The patch is deliberately conservative:
* Only installs on Windows, only on CPython 3.11/3.12, and only when the
  original function's source still matches the shape we hardened against
  (guarded by a source fingerprint), otherwise it logs and does nothing.
* The patched function is a verbatim copy of CPython 3.11's implementation
  with the narrow error-classification change.
"""
from __future__ import annotations

import sys

from loguru import logger

# Windows error codes that mean "the *accepted/queued* connection died", not
# "the listening socket is broken". Retrying accept on the listener is safe.
_TRANSIENT_ACCEPT_WINERRORS = {
    64,     # ERROR_NETNAME_DELETED — peer reset while queued (the live incident)
    995,    # ERROR_OPERATION_ABORTED
    10053,  # WSAECONNABORTED
    10054,  # WSAECONNRESET
}

_INSTALLED = False


def is_transient_accept_error(exc: BaseException) -> bool:
    """True when an accept-chain failure only killed one queued connection."""
    if isinstance(exc, (ConnectionResetError, ConnectionAbortedError)):
        return True
    if isinstance(exc, OSError):
        winerror = getattr(exc, "winerror", None)
        if winerror in _TRANSIENT_ACCEPT_WINERRORS:
            return True
    return False


def _patched_start_serving(self, protocol_factory, sock,
                           sslcontext=None, server=None, backlog=100,
                           ssl_handshake_timeout=None,
                           ssl_shutdown_timeout=None):
    """Copy of BaseProactorEventLoop._start_serving (3.11) with transient-error
    retry: a per-connection abort re-arms accept instead of closing the
    listener."""
    from asyncio import exceptions, trsock  # same modules the original uses

    def loop(f=None):
        try:
            if f is not None:
                conn, addr = f.result()
                if self._debug:
                    logger.debug(
                        "{!r} got a new connection from {!r}: {!r}", server, addr, conn
                    )
                protocol = protocol_factory()
                if sslcontext is not None:
                    self._make_ssl_transport(
                        conn, protocol, sslcontext, server_side=True,
                        extra={'peername': addr}, server=server,
                        ssl_handshake_timeout=ssl_handshake_timeout,
                        ssl_shutdown_timeout=ssl_shutdown_timeout)
                else:
                    self._make_socket_transport(
                        conn, protocol,
                        extra={'peername': addr}, server=server)
            if self.is_closed():
                return
            f = self._proactor.accept(sock)
        except OSError as exc:
            # ── hardening: transient per-connection failure must NOT close the
            # listening socket. Re-arm accept so the server keeps serving.
            if sock.fileno() != -1 and is_transient_accept_error(exc):
                logger.warning(
                    "proactor accept: transient connection error ({}); "
                    "listener kept alive, re-arming accept",
                    exc,
                )
                try:
                    f = self._proactor.accept(sock)
                except OSError as rearm_exc:
                    # Re-arm itself failed -> genuinely broken listener.
                    self.call_exception_handler({
                        'message': 'Accept failed on a socket',
                        'exception': rearm_exc,
                        'socket': trsock.TransportSocket(sock),
                    })
                    sock.close()
                else:
                    self._accept_futures[sock.fileno()] = f
                    f.add_done_callback(loop)
                return
            # original behavior for genuine listener-fatal errors
            if sock.fileno() != -1:
                self.call_exception_handler({
                    'message': 'Accept failed on a socket',
                    'exception': exc,
                    'socket': trsock.TransportSocket(sock),
                })
                sock.close()
            elif self._debug:
                logger.debug("Accept failed on socket {!r}", sock)
        except exceptions.CancelledError:
            sock.close()
        else:
            self._accept_futures[sock.fileno()] = f
            f.add_done_callback(loop)

    self.call_soon(loop)


def install_proactor_accept_hardening() -> bool:
    """Install the hardened ``_start_serving``. Returns True when installed.

    No-ops (with a log line) on non-Windows, non-CPython-3.11/3.12, or when the
    stdlib implementation no longer matches the shape this patch was written
    against — better to run unpatched than to patch blind.
    """
    global _INSTALLED
    if _INSTALLED:
        return True
    if sys.platform != "win32":
        return False
    if sys.version_info[:2] not in ((3, 11), (3, 12)):
        logger.warning(
            "proactor accept hardening skipped: untested Python {}.{}",
            sys.version_info[0], sys.version_info[1],
        )
        return False
    try:
        from asyncio import proactor_events
        import inspect

        original = proactor_events.BaseProactorEventLoop._start_serving
        source = inspect.getsource(original)
        # Fingerprint of the flaw we are replacing: close-on-any-OSError.
        if "Accept failed on a socket" not in source or "sock.close()" not in source:
            logger.warning(
                "proactor accept hardening skipped: stdlib _start_serving no "
                "longer matches the patched shape (Python {}); running unpatched",
                sys.version.split()[0],
            )
            return False
        proactor_events.BaseProactorEventLoop._start_serving = _patched_start_serving
        _INSTALLED = True
        logger.info(
            "proactor accept hardening installed: transient accept errors "
            "(WinError 64/995, conn reset/abort) no longer close the listener"
        )
        return True
    except Exception as exc:  # never let the hardening break startup
        logger.warning("proactor accept hardening failed to install: {}", exc)
        return False
