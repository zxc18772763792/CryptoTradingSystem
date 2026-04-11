"""Autonomous-agent API routes mounted under /api/ai."""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Request

from web.api.auth import require_sensitive_ops_auth
import web.api.ai_research as ai_research_module
from web.api.ai_research import (
    AIAutonomousAgentConfigUpdateRequest,
    AIAutonomousAgentRiskConfigUpdateRequest,
    AIAutonomousAgentRunOnceRequest,
    AIAutonomousAgentStartRequest,
)


router = APIRouter()


@router.get("/runtime-config/autonomous-agent")
async def get_ai_autonomous_agent_runtime_config(request: Request):
    return await ai_research_module.get_ai_autonomous_agent_runtime_config(request)


@router.post("/runtime-config/autonomous-agent", dependencies=[Depends(require_sensitive_ops_auth)])
async def update_ai_autonomous_agent_runtime_config(
    request: Request,
    payload: AIAutonomousAgentConfigUpdateRequest,
):
    return await ai_research_module.update_ai_autonomous_agent_runtime_config(request, payload)


@router.get("/autonomous-agent/risk-config")
async def get_ai_autonomous_agent_risk_config(request: Request):
    return await ai_research_module.get_ai_autonomous_agent_risk_config(request)


@router.post("/autonomous-agent/risk-config", dependencies=[Depends(require_sensitive_ops_auth)])
async def update_ai_autonomous_agent_risk_config(
    request: Request,
    payload: AIAutonomousAgentRiskConfigUpdateRequest,
):
    return await ai_research_module.update_ai_autonomous_agent_risk_config(request, payload)


@router.get("/autonomous-agent/status")
async def get_ai_autonomous_agent_status(request: Request):
    return await ai_research_module.get_ai_autonomous_agent_status(request)


@router.post("/autonomous-agent/start", dependencies=[Depends(require_sensitive_ops_auth)])
async def start_ai_autonomous_agent(
    request: Request,
    payload: AIAutonomousAgentStartRequest = AIAutonomousAgentStartRequest(),
):
    return await ai_research_module.start_ai_autonomous_agent(request, payload)


@router.post("/autonomous-agent/stop", dependencies=[Depends(require_sensitive_ops_auth)])
async def stop_ai_autonomous_agent(request: Request):
    return await ai_research_module.stop_ai_autonomous_agent(request)


@router.post("/autonomous-agent/run-once", dependencies=[Depends(require_sensitive_ops_auth)])
async def run_ai_autonomous_agent_once(
    request: Request,
    payload: AIAutonomousAgentRunOnceRequest = AIAutonomousAgentRunOnceRequest(),
):
    return await ai_research_module.run_ai_autonomous_agent_once(request, payload)


@router.get("/autonomous-agent/journal")
async def get_ai_autonomous_agent_journal(request: Request, limit: int = 50):
    return await ai_research_module.get_ai_autonomous_agent_journal(request, limit=limit)


@router.get("/autonomous-agent/review")
async def get_ai_autonomous_agent_review(request: Request, limit: int = 12):
    return await ai_research_module.get_ai_autonomous_agent_review(request, limit=limit)


@router.get("/autonomous-agent/scorecard")
async def get_ai_autonomous_agent_scorecard(request: Request, limit: int = 200, hours: int = 24 * 7):
    return await ai_research_module.get_ai_autonomous_agent_scorecard(
        request,
        limit=limit,
        hours=hours,
    )


@router.get("/autonomous-agent/risk-status")
async def get_ai_autonomous_agent_risk_status(request: Request):
    return await ai_research_module.get_ai_autonomous_agent_risk_status(request)


@router.get("/autonomous-agent/symbol-ranking")
async def get_ai_autonomous_agent_symbol_ranking(request: Request, limit: int = 10, refresh: bool = False):
    return await ai_research_module.get_ai_autonomous_agent_symbol_ranking(
        request,
        limit=limit,
        refresh=refresh,
    )


@router.get("/autonomous-agent/live-signals")
async def get_autonomous_agent_live_signals(request: Request, symbol: Optional[str] = None):
    return await ai_research_module.get_autonomous_agent_live_signals(request, symbol=symbol)
