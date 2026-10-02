"""Worker liveness only; source errors remain visible through news source status."""
import argparse
import os
from pathlib import Path
import time


def heartbeat_is_fresh(path: Path, max_age: float, *, now: float | None = None) -> bool:
    try:
        age = (time.time() if now is None else now) - path.stat().st_mtime
        return 0 <= age <= max_age
    except OSError:
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-age", type=float, default=900.0)
    args = parser.parse_args()
    path = os.environ.get("NEWS_WORKER_HEARTBEAT_FILE", "")
    raise SystemExit(0 if path and heartbeat_is_fresh(Path(path), args.max_age) else 1)
