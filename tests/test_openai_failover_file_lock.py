from pathlib import Path

from core.utils.openai_responses import _openai_failover_file_lock


def test_failover_lock_file_remains_one_byte(tmp_path: Path):
    state_path = tmp_path / "failover_state.json"

    for _ in range(25):
        with _openai_failover_file_lock(state_path):
            pass

    lock_path = state_path.with_suffix(state_path.suffix + ".lock")
    assert lock_path.read_bytes() == b"0"

