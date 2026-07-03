"""Self-probe watchdog for the HTTP listener ("alive but headless" detector).

Layer-2 defense behind :mod:`core.utils.proactor_accept_hardening`: if anything
still silently kills the uvicorn listener (an unpatched code path, a future
Python version where the hardening declines to install, an OS-level socket
failure), the app must not keep running headless with live positions under
app-managed protective stops. The watchdog probes the app's own listening port
with a raw TCP connect (loopback, proxy-free); after the listener has been seen
alive once, a run of consecutive probe failures means the listener is gone —
the worker logs CRITICAL and exits the process with
:data:`LISTENER_DEAD_EXIT_CODE` so the launcher's bounded restart loop can
bring the service back.

A TCP connect succeeds off the backlog even when the event loop is busy, so a
slow-but-alive app does not false-trip this; only a dead listener does.
"""
from __future__ import annotations

import socket
from dataclasses import dataclass, field

# Chosen to be distinctive in cmd's %ERRORLEVEL% and memorable next to the
# WinError 64 incident that motivated this. The launchers restart ONLY on this
# exit code, so ordinary crashes/stops keep their existing semantics.
LISTENER_DEAD_EXIT_CODE = 64


def probe_listener(host: str, port: int, *, timeout_sec: float = 3.0) -> bool:
    """True when a raw TCP connect to (host, port) succeeds (listener alive)."""
    try:
        with socket.create_connection((host, int(port)), timeout=max(0.5, float(timeout_sec))):
            return True
    except OSError:
        return False


@dataclass
class ListenerWatchdogPolicy:
    """Pure decision logic: arm after first success, trip on N consecutive fails.

    ``observe(probe_ok)`` returns True exactly when the process should exit
    (listener confirmed dead). Never trips before the listener has been seen
    alive once, so a slow cold start cannot false-trip it.
    """

    max_consecutive_failures: int = 3
    armed: bool = field(default=False, init=False)
    consecutive_failures: int = field(default=0, init=False)

    def observe(self, probe_ok: bool) -> bool:
        if probe_ok:
            self.armed = True
            self.consecutive_failures = 0
            return False
        if not self.armed:
            return False  # still cold-starting; listener has never been up
        self.consecutive_failures += 1
        return self.consecutive_failures >= max(1, int(self.max_consecutive_failures))
