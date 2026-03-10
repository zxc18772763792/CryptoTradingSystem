from __future__ import annotations

import asyncio
import json
import secrets
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request

from core.audit.ops_audit import ops_audit_scope
from core.ops.service import api as ops_api
from core.ops.service.auth import get_request_auth


router = APIRouter()


@router.post("/research/run")
async def research_run(request: Request, payload: ops_api.ResearchRunRequest):
    auth = get_request_auth(request)
    params = payload.model_dump()
    async with ops_audit_scope(actor=auth.actor, endpoint="/ops/research/run", method="POST", params=params, ip=auth.client_ip) as audit_state:
        try:
            ops_api._ensure_ops_runtime_state(request.app)
            symbol = ops_api._normalize_symbol(payload.symbol)
            default_research = ops_api.ResearchConfig()
            config = ops_api.ResearchConfig(
                exchange=str(payload.exchange or "binance").strip().lower(),
                symbol=symbol,
                days=int(payload.days),
                initial_capital=float(payload.initial_capital),
                timeframes=list(payload.timeframes or ["1m", "5m", "15m"]),
                strategies=list(payload.strategies or default_research.strategies),
                commission_rate=float(payload.commission_rate),
                slippage_bps=float(payload.slippage_bps),
                output_dir=default_research.output_dir,
            )
            if not payload.background:
                result = await ops_api.run_strategy_research(config)
                latest_payload = {
                    "job_id": None,
                    "request_summary": params,
                    "output_dir": str(Path(result.get("csv_path") or "").resolve().parent) if result.get("csv_path") else str(config.output_dir.resolve()),
                    "csv_path": result.get("csv_path"),
                    "markdown_path": result.get("markdown_path"),
                    "top_result_summary": result.get("best"),
                    "finished_at": ops_api._now_utc().isoformat(),
                }
                latest_path = Path(request.app.state.research_latest_path)
                latest_path.parent.mkdir(parents=True, exist_ok=True)
                latest_path.write_text(json.dumps(latest_payload, ensure_ascii=False, indent=2), encoding="utf-8")
                return ops_api._ok(result)
            job_id = f"research-{int(ops_api._now_utc().timestamp())}-{secrets.token_hex(4)}"
            job = {
                "job_id": job_id,
                "status": "pending",
                "created_at": ops_api._now_utc().isoformat(),
                "started_at": None,
                "finished_at": None,
                "request": params,
                "result": None,
                "error": None,
            }
            request.app.state.research_jobs[job_id] = job
            asyncio.create_task(ops_api._run_research_job(request.app, job_id, config, params), name=f"ops_research_{job_id}")
            audit_state["extra"] = {"job_id": job_id}
            return ops_api._ok(job)
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))


@router.get("/research/job/{job_id}")
async def research_job(request: Request, job_id: str = ops_api.FPath(...)):
    ops_api._ensure_ops_runtime_state(request.app)
    job = request.app.state.research_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="research job not found")
    return ops_api._ok(job)


@router.get("/research/latest")
async def research_latest(request: Request):
    ops_api._ensure_ops_runtime_state(request.app)
    latest_path = Path(request.app.state.research_latest_path)
    if not latest_path.exists():
        return ops_api._err("latest research not found")
    try:
        payload = json.loads(latest_path.read_text(encoding="utf-8"))
        return ops_api._ok(payload)
    except Exception as exc:
        return ops_api._err(f"failed to read latest research: {exc}")
