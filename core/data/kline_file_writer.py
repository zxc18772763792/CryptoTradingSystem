"""Shared UTC guard for archive and second-level file writers."""
from __future__ import annotations
import os
from pathlib import Path
from uuid import uuid4
import pandas as pd
from core.data.data_storage import prepare_utc_kline_frame, _heal_local_existing_against_utc
from core.data.parquet_lock import parquet_partition_lock


def write_utc_kline_file(path: Path, frame: pd.DataFrame, *, origin: str,
                         timestamp_timezone: str | None = None) -> int:
    if frame.empty:
        return 0
    incoming = prepare_utc_kline_frame(frame, origin=origin, timestamp_timezone=timestamp_timezone)
    path = Path(path)
    if path.parent.name.endswith("_parts") and any(incoming.index.strftime("%Y-%m-%d") != path.stem):
        raise ValueError("Incoming UTC rows do not belong to the partition date")
    path.parent.mkdir(parents=True, exist_ok=True)
    with parquet_partition_lock(path):
        previous = None
        combined = incoming
        if path.exists():
            previous = _heal_local_existing_against_utc(pd.read_parquet(path), incoming)
            combined = pd.concat([previous, incoming])
        combined = combined.loc[~combined.index.duplicated(keep="last")].sort_index()
        combined.attrs = {**(previous.attrs if previous is not None else {}), **incoming.attrs}
        if path.parent.name.endswith("_parts") and any(combined.index.strftime("%Y-%m-%d") != path.stem):
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
