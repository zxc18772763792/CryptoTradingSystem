from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
root_str = str(ROOT)
if root_str not in sys.path:
    sys.path.insert(0, root_str)

from prediction_markets.polymarket.paper_strategy import ProfileGuardrails, profile_from_walk_forward_report, save_paper_strategy_profile


def main() -> None:
    parser = argparse.ArgumentParser(description="Promote a Polymarket walk-forward report into a local paper strategy profile.")
    parser.add_argument("--report", required=True, help="Path to a walk-forward report JSON")
    parser.add_argument("--token-ids", default="", help="Comma-separated token ids for live paper strategy selection")
    parser.add_argument("--account-id", default="default")
    parser.add_argument("--output", default="data/reports/polymarket_paper_strategy_profile.json")
    parser.add_argument("--execute-default", action="store_true", help="Store dry_run=false in the profile. CLI/Ops can still override it.")
    parser.add_argument("--min-segments", type=int, default=2)
    parser.add_argument("--min-positive-segments", type=int, default=1)
    parser.add_argument("--min-total-test-net-pnl", type=float, default=0.0)
    parser.add_argument("--min-worst-test-net-pnl", type=float, default=-100.0)
    parser.add_argument("--allow-unsafe", action="store_true", help="Write the profile even if guardrails fail; profile is marked safe_to_execute=false.")
    args = parser.parse_args()

    report = json.loads(Path(args.report).read_text(encoding="utf-8"))
    token_ids = [token.strip() for token in str(args.token_ids or "").split(",") if token.strip()]
    profile = profile_from_walk_forward_report(
        report,
        token_ids=token_ids,
        account_id=args.account_id,
        dry_run=not args.execute_default,
        guardrails=ProfileGuardrails(
            min_segments=args.min_segments,
            min_positive_segments=args.min_positive_segments,
            min_total_test_net_pnl=args.min_total_test_net_pnl,
            min_worst_test_net_pnl=args.min_worst_test_net_pnl,
        ),
        allow_unsafe=args.allow_unsafe,
    )
    paths = save_paper_strategy_profile(profile, Path(args.output))
    print(json.dumps({"paths": paths, "profile": profile}, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
