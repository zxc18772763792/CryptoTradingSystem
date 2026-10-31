"""Small cross-process lock for read/modify/replace state files."""
from __future__ import annotations

from contextlib import contextmanager
import os
from pathlib import Path
import threading

_guard = threading.Lock()
_threads: dict[str, threading.Lock] = {}


@contextmanager
def advisory_file_lock(path: Path):
    """Serialize cooperating writers using a stable sidecar, even when ``path`` is replaced."""
    lock_path = Path(path).with_name(Path(path).name + ".lock")
    key = os.path.normcase(str(lock_path.resolve()))
    with _guard:
        thread_lock = _threads.setdefault(key, threading.Lock())
    with thread_lock:
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT)
        with os.fdopen(fd, "r+b") as handle:
            if os.fstat(handle.fileno()).st_size == 0:
                handle.write(b"0")
                handle.flush()
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
                try:
                    yield
                finally:
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
