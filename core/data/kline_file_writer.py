"""Shared UTC guard for archive and second-level file writers."""
from __future__ import annotations
import os
from contextlib import ExitStack
from pathlib import Path
from uuid import uuid4
import pandas as pd
from core.data.data_storage import prepare_utc_kline_frame, _heal_local_existing_against_utc
from core.data.parquet_lock import parquet_partition_lock
from core.data.kline_partitions import rows_match_partition, series_locks_for


def write_utc_kline_file(path: Path, frame: pd.DataFrame, *, origin: str,
                         timestamp_timezone: str | None = None) -> int:
    if frame.empty:
        return 0
    incoming = prepare_utc_kline_frame(frame, origin=origin, timestamp_timezone=timestamp_timezone)
    path = Path(path)
    if path.parent.name.endswith("_parts") and not rows_match_partition(incoming.index, path):
        raise ValueError("Incoming UTC rows do not belong to the partition date (day, month or year in the file name)")
    path.parent.mkdir(parents=True, exist_ok=True)
    with ExitStack() as locks:
        for series_lock in series_locks_for([path]):
            locks.enter_context(parquet_partition_lock(series_lock))
        locks.enter_context(parquet_partition_lock(path))
        previous = None
        combined = incoming
        if path.exists():
            previous = _heal_local_existing_against_utc(pd.read_parquet(path), incoming)
            combined = pd.concat([previous, incoming])
        combined = combined.loc[~combined.index.duplicated(keep="last")].sort_index()
        combined.attrs = {**(previous.attrs if previous is not None else {}), **incoming.attrs}
        if path.parent.name.endswith("_parts") and not rows_match_partition(combined.index, path):
            raise ValueError("Stored rows violate UTC partition date; explicit repair required")
        stage = path.with_name(f"{path.name}.{uuid4().hex}.tmp")
        try:
            with stage.open("xb") as stream:
                combined.to_parquet(stream, compression="zstd")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(stage, path)
        finally:
            stage.unlink(missing_ok=True)
        return len(combined) - (len(previous) if previous is not None else 0)
