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
    handle = open(lock_path, "a+b")
    acquired = False
    try:
        # msvcrt.locking requires an existing byte and locks from the current
        # file position. Do not use append writes after this initialization.
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()

        while True:
            try:
                handle.seek(0)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
                break
            except (OSError, BlockingIOError):
                if time.monotonic() >= deadline:
                    raise ParquetPartitionLockTimeout(
                        "Timed out after "
                        f"{timeout_seconds:.2f}s waiting for parquet partition lock "
                        f"{lock_path} (pid={os.getpid()})"
                    ) from None
                time.sleep(min(poll_seconds, max(0.0, deadline - time.monotonic())))

        yield
    finally:
        if acquired:
            try:
                handle.seek(0)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                # Closing the handle releases the OS lock even if an explicit
                # unlock races with process shutdown.
                pass
        handle.close()
