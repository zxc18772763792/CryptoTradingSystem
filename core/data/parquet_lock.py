"""Cross-process locking for parquet read-modify-write transactions."""

from __future__ import annotations

import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


class ParquetPartitionLockTimeout(TimeoutError):
    """Raised when a parquet partition remains locked past the deadline."""


def partition_lock_path(partition_path: Path) -> Path:
    """Return the stable lock-file path shared by every parquet writer."""

    return partition_path.with_name(f"{partition_path.name}.lock")


@contextmanager
def parquet_partition_lock(
    partition_path: Path,
    *,
    timeout_seconds: float = 30.0,
    poll_seconds: float = 0.05,
) -> Iterator[None]:
    """Lock one parquet partition across processes.

    The lock file intentionally persists. Removing it after unlock creates a
    race where a waiter can hold an unlinked inode while a new writer locks a
    newly-created file with the same name.
    """

    lock_path = partition_lock_path(Path(partition_path))
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    timeout_seconds = max(0.0, float(timeout_seconds))
    poll_seconds = max(0.01, float(poll_seconds))
    deadline = time.monotonic() + timeout_seconds
    open_flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_BINARY"):
        open_flags |= os.O_BINARY
    fd: int | None = None
    acquired = False
    try:
        while True:
            try:
                if fd is None:
                    fd = os.open(lock_path, open_flags, 0o666)

                # msvcrt.locking requires an existing byte and locks from the
                # current file position. A racing process may already hold that
                # byte, so initialize it with unbuffered I/O inside the retry
                # loop and treat PermissionError like ordinary contention.
                if os.path.getsize(lock_path) == 0:
                    os.lseek(fd, 0, os.SEEK_SET)
                    os.write(fd, b"\0")

                os.lseek(fd, 0, os.SEEK_SET)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
                break
            except (OSError, BlockingIOError):
                if fd is not None:
                    try:
                        os.close(fd)
                    except OSError:
                        pass
                    fd = None
                if time.monotonic() >= deadline:
                    raise ParquetPartitionLockTimeout(
                        "Timed out after "
                        f"{timeout_seconds:.2f}s waiting for parquet partition lock "
                        f"{lock_path} (pid={os.getpid()})"
                    ) from None
                time.sleep(min(poll_seconds, max(0.0, deadline - time.monotonic())))

        yield
    finally:
        if fd is not None and acquired:
            try:
                os.lseek(fd, 0, os.SEEK_SET)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(fd, fcntl.LOCK_UN)
            except OSError:
                # Closing the handle releases the OS lock even if an explicit
                # unlock races with process shutdown.
                pass
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass
