from __future__ import annotations

from fastapi import APIRouter, Request

from core.audit.ops_audit import ops_audit_scope
from core.ops.service import api as ops_api
from core.ops.service.auth import get_request_auth


router = APIRouter()


@router.post("/news/pull_now")
async def news_pull_now(request: Request, payload: ops_api.OpsNewsPullRequest):
    auth = get_request_auth(request)
    params = payload.model_dump()
    async with ops_audit_scope(actor=auth.actor, endpoint="/ops/news/pull_now", method="POST", params=params, ip=auth.client_ip) as audit_state:
        try:
            cfg = ops_api.load_service_config()
            result = await ops_api.run_ingest_pull_now(cfg=cfg, payload=ops_api.IngestRequest(**params))
            audit_state["extra"] = {"queued_count": result.get("queued_count", 0), "events_count": result.get("events_count", 0)}
            return ops_api._ok(result)
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))


@router.post("/news/worker_run_once")
async def news_worker_run_once(request: Request, payload: ops_api.OpsWorkerRunRequest):
    auth = get_request_auth(request)
    params = payload.model_dump()
    async with ops_audit_scope(actor=auth.actor, endpoint="/ops/news/worker_run_once", method="POST", params=params, ip=auth.client_ip) as audit_state:
        try:
            cfg = ops_api.load_service_config()
            sources = [str(x).strip().lower() for x in payload.sources if str(x).strip()]
            out = {}
            if not payload.llm_only:
                out["pull"] = await ops_api.run_pull_cycle(cfg, sources or (cfg.get("defaults") or {}).get("news_sources") or [])
            if not payload.pull_only:
                out["llm"] = await ops_api.process_llm_batch(cfg, limit=payload.llm_limit)
            out["source_states"] = await ops_api.news_db.list_source_states()
            out["llm_queue"] = await ops_api.news_db.get_llm_queue_stats()
            return ops_api._ok(out)
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))
