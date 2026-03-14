"""One-click pipeline for AI research + decision deployment via web API.

Flow:
1) Generate proposal
2) Run proposal (sync)
3) Deploy candidate according to governance mode
   - governance off: /candidates/{id}/register
   - governance on:
       - paper: /candidates/{id}/quick-register
       - live_candidate: /candidates/{id}/human-approve
"""
from __future__ import annotations

import argparse
import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_csv(value: str) -> List[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def _normalize_symbol(value: str) -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        return "BTC/USDT"
    if "/" in raw:
        return raw
    if raw.endswith("USDT") and len(raw) > 4:
        return f"{raw[:-4]}/USDT"
    return raw


@dataclass
class ApiClient:
    base_url: str
    timeout_sec: int = 180

    def request(self, method: str, path: str, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        method = str(method or "GET").strip().upper()
        url = f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
        data = None
        headers: Dict[str, str] = {}
        if payload is not None:
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url=url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=max(1, int(self.timeout_sec))) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                return json.loads(raw) if raw.strip() else {}
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {exc.code} {method} {path}: {body}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"HTTP request failed: {method} {path}: {exc}") from exc


def _choose_target(preferred: str, run_result: Dict[str, Any]) -> str:
    preferred = str(preferred or "auto").strip().lower()
    if preferred in {"paper", "live_candidate"}:
        return preferred
    decision = str(((run_result or {}).get("promotion") or {}).get("decision") or "").strip().lower()
    if decision == "shadow":
        return "paper"
    if decision in {"paper", "live_candidate"}:
        return decision
    return "paper"


def main() -> None:
    parser = argparse.ArgumentParser(description="One-click AI research and decision deployment.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000/api/ai", help="AI API base URL")
    parser.add_argument("--goal", required=True, help="Research goal text")
    parser.add_argument("--market-regime", default="mixed")
    parser.add_argument("--symbols", default="BTC/USDT")
    parser.add_argument("--timeframes", default="15m,1h")
    parser.add_argument("--exchange", default="binance")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--commission-rate", type=float, default=0.0004)
    parser.add_argument("--slippage-bps", type=float, default=2.0)
    parser.add_argument("--initial-capital", type=float, default=10000.0)
    parser.add_argument("--target", default="auto", choices=["auto", "paper", "live_candidate"])
    parser.add_argument("--allocation-pct", type=float, default=0.05)
    parser.add_argument("--strategy-name", default="")
    parser.add_argument("--approval-notes", default="oneclick approve")
    parser.add_argument("--timeout-sec", type=int, default=180)
    parser.add_argument("--skip-deploy", action="store_true")
    parser.add_argument("--output-dir", default="runtime/oneclick")
    args = parser.parse_args()

    symbols = [_normalize_symbol(item) for item in _parse_csv(args.symbols)] or ["BTC/USDT"]
    timeframes = _parse_csv(args.timeframes) or ["15m", "1h"]
    client = ApiClient(base_url=args.base_url, timeout_sec=max(5, int(args.timeout_sec)))

    generated = client.request(
        "POST",
        "/proposals/generate",
        {
            "goal": str(args.goal),
            "market_regime": str(args.market_regime),
            "symbols": symbols,
            "timeframes": timeframes,
            "constraints": {},
            "metadata": {"source": "oneclick_script"},
            "origin_context": {},
            "market_context": {},
            "llm_research_output": {},
        },
    )
    proposal = generated.get("proposal") or {}
    proposal_id = str(proposal.get("proposal_id") or "").strip()
    if not proposal_id:
        raise RuntimeError(f"proposal_id missing in response: {generated}")

    run_result = client.request(
        "POST",
        f"/proposals/{proposal_id}/run",
        {
            "exchange": str(args.exchange).strip().lower() or "binance",
            "symbol": symbols[0],
            "days": max(1, int(args.days)),
            "commission_rate": max(0.0, float(args.commission_rate)),
            "slippage_bps": max(0.0, float(args.slippage_bps)),
            "initial_capital": max(10.0, float(args.initial_capital)),
            "background": False,
            "timeframes": timeframes,
            "strategies": [],
        },
    )
    candidate = run_result.get("candidate") or {}
    candidate_id = str(candidate.get("candidate_id") or "").strip()
    if not candidate_id:
        raise RuntimeError(f"candidate_id missing in run response: {run_result}")

    runtime_cfg = client.request("GET", "/runtime-config")
    governance_enabled = bool(runtime_cfg.get("governance_enabled"))
    target = _choose_target(str(args.target), run_result)

    deploy_result: Dict[str, Any] | None = None
    deploy_endpoint = ""
    if not args.skip_deploy:
        if governance_enabled:
            if target == "paper":
                deploy_endpoint = f"/candidates/{candidate_id}/quick-register"
                deploy_result = client.request(
                    "POST",
                    deploy_endpoint,
                    {"allocation_pct": max(0.001, min(1.0, float(args.allocation_pct)))},
                )
            else:
                deploy_endpoint = f"/candidates/{candidate_id}/human-approve"
                deploy_result = client.request(
                    "POST",
                    deploy_endpoint,
                    {"target": "live_candidate", "notes": str(args.approval_notes)},
                )
        else:
            deploy_endpoint = f"/candidates/{candidate_id}/register"
            deploy_result = client.request(
                "POST",
                deploy_endpoint,
                {"mode": target, "name": str(args.strategy_name)},
            )

    summary = {
        "timestamp": _now_iso(),
        "base_url": args.base_url,
        "governance_enabled": governance_enabled,
        "proposal_id": proposal_id,
        "candidate_id": candidate_id,
        "target": target,
        "deploy_skipped": bool(args.skip_deploy),
        "deploy_endpoint": deploy_endpoint,
        "proposal_status": (run_result.get("proposal") or {}).get("status"),
        "runtime_status": (deploy_result or {}).get("runtime_status"),
        "registered_strategy_name": (deploy_result or {}).get("registered_strategy_name"),
        "generated": generated,
        "run_result": run_result,
        "deploy_result": deploy_result,
    }

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = f"ai_oneclick_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    out_path = output_dir / filename
    out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps({"saved": str(out_path), **summary}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
