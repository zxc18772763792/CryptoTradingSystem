"""LSR crowding + KOL consensus radar routes (positioning / macro context).

These endpoints expose long/short-ratio crowding and the 5-major KOL consensus
as *context*, not takeoff predictions. Self-contained: no shared scan-pipeline
state, so nothing here is monkeypatched by the route tests.
"""
from __future__ import annotations

import asyncio

from fastapi import APIRouter

from .helpers import _safe_float, _utcnow

router = APIRouter()


@router.get("/radar/lsr-crowding")
async def get_lsr_crowding(mode: str = "trader", limit: int = 40):
    """Current long/short-ratio crowding ranking (most crowded first). Positioning,
    NOT a takeoff signal — for the real-time scan's crowding/risk dimension."""
    from core.data.coinglass_lsr import crowding_label, fetch_lsr_ranking

    normalized_mode = "whale" if str(mode or "").lower() == "whale" else "trader"
    result = await asyncio.wait_for(
        fetch_lsr_ranking(mode=normalized_mode, limit=max(5, min(int(limit or 40), 100))),
        timeout=15.0,
    )
    rows = []
    for row in result.get("rows") or []:
        ratio = _safe_float(row.get("ratio"), 0.0)
        rows.append(
            {
                "symbol": str(row.get("symbol") or ""),
                "ratio": round(ratio, 3),
                "ratio_pct": row.get("ratio_pct"),
                "whale_ratio": _safe_float(row.get("whale_ratio"), 0.0),
                "delta_2m_pct": row.get("delta_2m_pct"),
                "delta_30m_pct": row.get("delta_30m_pct"),
                "delta_4h_pct": row.get("delta_4h_pct"),
                "crowding": crowding_label(ratio),
                "oi_mcap_ratio": _safe_float(row.get("oi_mcap_ratio"), 0.0),
            }
        )
    return {
        "available": bool(result.get("available")),
        "mode": normalized_mode,
        "error": result.get("error"),
        "rows": rows,
        "note": "多空持仓拥挤度（仓位，非预测）；比值越极端越拥挤，回撤/挤压风险越高。",
        "ts": _utcnow().isoformat(),
    }


@router.get("/radar/kol-consensus")
async def get_kol_consensus():
    """KOL consensus for the 5 majors (BTC/ETH/SOL/DOGE/BNB) + an aggregate risk
    tone. A macro / risk-regime read, NOT an altcoin discovery signal."""
    from core.data.coinglass_lsr import fetch_kol_consensus

    result = await asyncio.wait_for(fetch_kol_consensus(), timeout=15.0)
    rows = []
    for row in result.get("rows") or []:
        rows.append(
            {
                "symbol": str(row.get("display_symbol") or row.get("symbol") or ""),
                "decision": str(row.get("decision") or "neutral"),
                "decision_label": str(row.get("decision_label") or "中性"),
                "confidence": round(_safe_float(row.get("confidence"), 0.0), 3),
                "bias": round(_safe_float(row.get("trust_adjusted_bias"), 0.0), 4),
                "snapshot_date": row.get("snapshot_date"),
                "snapshot_age_hours": row.get("snapshot_age_hours"),
            }
        )
    return {
        "available": bool(result.get("available")),
        "error": result.get("error"),
        "risk_tone": result.get("risk_tone"),
        "rows": rows,
        "coverage": "BTC/ETH/SOL/DOGE/BNB",
        "note": "KOL 共识仅覆盖 5 大币，是大盘方向/风险体制参考，不是山寨选币信号。",
        "ts": _utcnow().isoformat(),
    }
