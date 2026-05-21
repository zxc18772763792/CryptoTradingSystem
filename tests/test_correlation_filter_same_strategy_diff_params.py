from datetime import datetime, timezone

from core.research.experiment_schemas import StrategyCandidate
from core.research.orchestrator import _correlation_filter_candidates


def _candidate(candidate_id: str, params: dict, curve: list[float]) -> StrategyCandidate:
    return StrategyCandidate(
        candidate_id=candidate_id,
        proposal_id="proposal-corr",
        experiment_id="experiment-corr",
        created_at=datetime.now(timezone.utc),
        strategy="MAStrategy",
        timeframe="1h",
        symbol="BTC/USDT",
        params=params,
        score=80.0,
        metadata={"best": {"equity_curve_sample": curve}},
    )


def test_correlation_filter_handles_same_strategy_different_candidate_ids():
    curve_a = [float(i) for i in range(50)]
    curve_b = [float(i) * 1.01 for i in range(50)]

    first = _candidate("candidate-a", {"fast": 5, "slow": 20}, curve_a)
    second = _candidate("candidate-b", {"fast": 8, "slow": 34}, curve_b)

    _correlation_filter_candidates([first, second], corr_threshold=0.85)

    assert not first.metadata.get("correlation_filtered")
    assert second.metadata.get("correlation_filtered") is True
    assert second.metadata["correlated_with"] == "candidate-a"
    assert second.metadata["duplicate_signature"] is True
    assert second.metadata["correlation_is_cross_batch"] is False


def test_correlation_filter_exact_duplicate_points_at_existing_candidate_id():
    existing = _candidate("existing-candidate", {"fast": 5, "slow": 20}, [])
    new = _candidate("new-candidate", {"fast": 5, "slow": 20}, [])

    _correlation_filter_candidates([new], corr_threshold=0.85, existing_candidates=[existing])

    assert new.metadata.get("correlation_filtered") is True
    assert new.metadata["correlated_with"] == "existing-candidate"
    assert new.metadata["correlation_is_cross_batch"] is True
