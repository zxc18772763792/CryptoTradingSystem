from __future__ import annotations

import secrets
import json
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Request

from core.audit.ops_audit import ops_audit_scope
from core.ops.service import api as ops_api
from core.ops.service.auth import get_request_auth, require_ops_permissions_dependency
from prediction_markets.polymarket.paper_strategy import (
    PaperStrategyConfig,
    ProfileGuardrails,
    load_paper_strategy_profile,
    profile_from_walk_forward_report,
    run_paper_strategy_once,
    save_paper_strategy_profile,
)
from prediction_markets.polymarket.paper_trading import PaperRiskLimits, PolymarketPaperTrader
from prediction_markets.polymarket.replay_report import (
    ReplayBatchConfig,
    ReplayWalkForwardConfig,
    momentum_param_grid,
    run_replay_batch,
    run_replay_grid,
    run_replay_walk_forward,
    threshold_param_grid,
    write_replay_batch_report,
    write_replay_grid_report,
    write_replay_walk_forward_report,
)


router = APIRouter()


@router.get("/polymarket/status")
async def polymarket_status(request: Request):
    try:
        return ops_api._ok(await ops_api._build_polymarket_status(request.app))
    except Exception as exc:
        return ops_api._err(str(exc))


@router.post(
    "/polymarket/subscribe",
    dependencies=[Depends(require_ops_permissions_dependency("manage_data_sources"))],
)
async def polymarket_subscribe(request: Request, payload: ops_api.PolymarketSubscribeRequest):
    auth = get_request_auth(request)
    params = payload.model_dump()
    async with ops_audit_scope(actor=auth.actor, endpoint="/ops/polymarket/subscribe", method="POST", params=params, ip=auth.client_ip) as audit_state:
        try:
            ops_api._ensure_ops_runtime_state(request.app)
            cfg = dict(request.app.state.polymarket_cfg or ops_api.load_polymarket_config())
            categories = dict(cfg.get("categories") or {})
            cat = str(payload.category or "").strip().upper()
            if cat not in categories:
                return ops_api._err(f"unknown category: {cat}")
            if payload.mode == "manual":
                item = dict(categories.get(cat) or {})
                if payload.keywords:
                    item["keywords"] = list(payload.keywords)
                if payload.tags:
                    item["tags"] = [int(x) for x in payload.tags]
                if payload.max_markets:
                    item["max_markets"] = int(payload.max_markets)
                categories[cat] = item
                cfg["categories"] = categories
                request.app.state.polymarket_cfg = cfg
            result = await ops_api.pm_refresh_markets_once(cfg, categories=[cat])
            audit_state["extra"] = {"category": cat}
            return ops_api._ok({"category": cat, "mode": payload.mode, "result": result})
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))


@router.post(
    "/polymarket/unsubscribe",
    dependencies=[Depends(require_ops_permissions_dependency("manage_data_sources"))],
)
async def polymarket_unsubscribe(request: Request, payload: ops_api.PolymarketUnsubscribeRequest):
    auth = get_request_auth(request)
    params = payload.model_dump()
    async with ops_audit_scope(actor=auth.actor, endpoint="/ops/polymarket/unsubscribe", method="POST", params=params, ip=auth.client_ip) as audit_state:
        try:
            result = await ops_api.pm_db.disable_subscriptions(payload.market_ids)
            return ops_api._ok(result)
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))


@router.post(
    "/polymarket/worker_run_once",
    dependencies=[Depends(require_ops_permissions_dependency("manage_data_sources"))],
)
async def polymarket_worker_run_once(request: Request, payload: ops_api.PolymarketWorkerRunRequest):
    auth = get_request_auth(request)
    params = payload.model_dump()
    async with ops_audit_scope(actor=auth.actor, endpoint="/ops/polymarket/worker_run_once", method="POST", params=params, ip=auth.client_ip) as audit_state:
        try:
            ops_api._ensure_ops_runtime_state(request.app)
            cfg = request.app.state.polymarket_cfg or ops_api.load_polymarket_config()
            result = await ops_api.pm_run_worker_once(
                cfg,
                refresh_markets=bool(payload.refresh_markets),
                refresh_quotes=bool(payload.refresh_quotes),
                categories=[str(x).strip().upper() for x in payload.categories if str(x).strip()] or None,
            )
            return ops_api._ok(result)
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))


@router.get("/polymarket/alerts")
async def polymarket_alerts(since: Optional[str] = None, category: Optional[str] = None, limit: int = 200):
    try:
        since_ts = ops_api.parse_any_datetime(since) if since else None
        rows = await ops_api.pm_db.list_alerts(since=since_ts, category=category, limit=limit)
        return ops_api._ok({"count": len(rows), "items": rows})
    except Exception as exc:
        return ops_api._err(str(exc))


@router.get("/polymarket/features")
async def polymarket_features(symbol: str, tf: str = "1m", since: Optional[str] = None):
    try:
        since_ts = ops_api.parse_any_datetime(since) if since else (ops_api._now_utc() - timedelta(hours=24))
        rows = await ops_api.pm_db.get_features_range(
            symbol=str(symbol or "").strip().upper(),
            since=since_ts,
            until=ops_api._now_utc(),
            timeframe=str(tf or "1m").strip().lower(),
        )
        return ops_api._ok({"count": len(rows), "items": rows})
    except Exception as exc:
        return ops_api._err(str(exc))


def _paper_limits_from_app(request: Request) -> PaperRiskLimits:
    ops_api._ensure_ops_runtime_state(request.app)
    cfg = request.app.state.polymarket_cfg or ops_api.load_polymarket_config()
    paper_cfg = (((cfg.get("defaults") or {}).get("trading") or {}).get("paper") or {})
    return PaperRiskLimits(
        initial_cash=float(paper_cfg.get("initial_cash") or 1000.0),
        max_order_notional=float(paper_cfg.get("max_order_notional") or 50.0),
        max_position_notional=float(paper_cfg.get("max_position_notional") or 200.0),
        fee_rate=float(paper_cfg.get("fee_rate") or 0.0),
    )


def _paper_trader(request: Request, account_id: str = "default") -> PolymarketPaperTrader:
    return PolymarketPaperTrader(account_id=account_id, limits=_paper_limits_from_app(request))


def _sanitize_report_name(value: str, default: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(value or "").strip())
    return (cleaned or default)[:120]


def _resolve_under_repo(value: str, *, default: str = "") -> Path:
    base_dir = Path(ops_api.settings.BASE_DIR).resolve()
    text = str(value or default).strip() or default
    if not text:
        raise ValueError("path must not be empty")
    raw = Path(text)
    candidate = raw.resolve() if raw.is_absolute() else (base_dir / raw).resolve()
    try:
        candidate.relative_to(base_dir)
    except ValueError as exc:
        raise ValueError(f"path escapes repository root: {text}") from exc
    return candidate


def _resolve_profile_output(value: str) -> Path:
    candidate = _resolve_under_repo(value)
    profile_root = (Path(ops_api.settings.BASE_DIR) / "data" / "profiles" / "polymarket").resolve()
    if not candidate.is_relative_to(profile_root) or candidate.suffix.lower() != ".json":
        raise ValueError("profile output must be a .json file under data/profiles/polymarket")
    if candidate.exists():
        raise ValueError("profile output already exists; choose a new versioned filename")
    return candidate


def _resolve_report_dir(value: str) -> Path:
    return _resolve_under_repo(value, default="data/reports")


def _resolve_repo_path(value: str) -> Path:
    return _resolve_under_repo(value)


def _replay_batch_config_from_payload(payload: Any) -> ReplayBatchConfig:
    return ReplayBatchConfig(
        strategy=str(payload.strategy or "threshold").strip().lower(),
        account_prefix=str(payload.account_prefix or "opsbatch").strip(),
        initial_cash=float(payload.initial_cash),
        order_size=float(payload.order_size),
        buy_below=float(getattr(payload, "buy_below", 0.45)),
        sell_above=float(getattr(payload, "sell_above", 0.60)),
        momentum_window=int(getattr(payload, "momentum_window", 3)),
        momentum_buy_delta=float(getattr(payload, "momentum_buy_delta", 0.04)),
        momentum_sell_delta=float(getattr(payload, "momentum_sell_delta", 0.04)),
        max_order_notional=float(payload.max_order_notional),
        max_position_notional=float(payload.max_position_notional),
        fee_rate=float(payload.fee_rate),
    )


def _compact_replay_report(report: Dict[str, Any], *, include_details: bool) -> Dict[str, Any]:
    if include_details:
        return dict(report)
    return {
        key: value
        for key, value in dict(report).items()
        if key not in {"results", "reports"}
    }


def _replay_param_grid_from_payload(payload: Any) -> List[Dict[str, Any]]:
    strategy = str(payload.strategy or "threshold").strip().lower()
    if strategy == "momentum":
        return momentum_param_grid(payload.momentum_windows, payload.momentum_buy_deltas, payload.momentum_sell_deltas)
    return threshold_param_grid(payload.buy_below_grid, payload.sell_above_grid)


async def _resolve_replay_token_ids(payload: Any) -> Dict[str, Any]:
    since = ops_api.parse_any_datetime(payload.since)
    until = ops_api.parse_any_datetime(payload.until)
    if until < since:
        raise ValueError("until must be greater than or equal to since")
    manual = []
    seen = set()
    for token in getattr(payload, "token_ids", []) or []:
        token_id = str(token or "").strip()
        if not token_id or token_id in seen:
            continue
        seen.add(token_id)
        manual.append(token_id)
    universe: List[Dict[str, Any]] = []
    token_ids = manual
    if not token_ids and int(getattr(payload, "top_n", 0) or 0) > 0:
        universe = await ops_api.pm_db.list_active_quote_tokens(
            since,
            until,
            limit=int(payload.top_n),
            min_quotes=int(payload.min_quotes),
        )
        token_ids = [str(item.get("token_id") or "") for item in universe if str(item.get("token_id") or "").strip()]
    if not token_ids:
        raise ValueError("token_ids must contain at least one token, or set top_n to select tokens from stored quotes")
    return {"since": since, "until": until, "token_ids": token_ids, "universe": universe}


@router.get("/polymarket/paper/account")
async def polymarket_paper_account(request: Request, account_id: str = "default"):
    try:
        trader = _paper_trader(request, account_id=account_id)
        return ops_api._ok(await trader.get_account())
    except Exception as exc:
        return ops_api._err(str(exc))


@router.get("/polymarket/paper/summary")
async def polymarket_paper_summary(request: Request, account_id: str = "default"):
    try:
        trader = _paper_trader(request, account_id=account_id)
        return ops_api._ok(await trader.get_summary())
    except Exception as exc:
        return ops_api._err(str(exc))


@router.post(
    "/polymarket/paper/reset",
    dependencies=[Depends(require_ops_permissions_dependency("reset_paper_runtime"))],
)
async def polymarket_paper_reset(request: Request, payload: ops_api.PolymarketPaperResetRequest):
    auth = get_request_auth(request)
    params = payload.model_dump()
    async with ops_audit_scope(actor=auth.actor, endpoint="/ops/polymarket/paper/reset", method="POST", params=params, ip=auth.client_ip) as audit_state:
        try:
            trader = _paper_trader(request, account_id=payload.account_id)
            return ops_api._ok(await trader.reset(initial_cash=payload.initial_cash))
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))


@router.post(
    "/polymarket/paper/order",
    dependencies=[Depends(require_ops_permissions_dependency("manage_orders"))],
)
async def polymarket_paper_order(request: Request, payload: ops_api.PolymarketPaperOrderRequest):
    auth = get_request_auth(request)
    params = payload.model_dump()
    async with ops_audit_scope(actor=auth.actor, endpoint="/ops/polymarket/paper/order", method="POST", params=params, ip=auth.client_ip) as audit_state:
        try:
            trader = _paper_trader(request, account_id=payload.account_id)
            order = await trader.place_limit(
                market_id=payload.market_id,
                token_id=payload.token_id,
                outcome=payload.outcome,
                side=payload.side,
                price=payload.price,
                size=payload.size,
                client_order_id=payload.client_order_id,
                metadata=payload.metadata,
                fill_immediately=payload.fill_immediately,
            )
            audit_state["extra"] = {"account_id": payload.account_id, "order_id": order.get("order_id")}
            return ops_api._ok(order)
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))


@router.post(
    "/polymarket/paper/sweep",
    dependencies=[Depends(require_ops_permissions_dependency("manage_orders"))],
)
async def polymarket_paper_sweep(request: Request, account_id: str = "default"):
    auth = get_request_auth(request)
    async with ops_audit_scope(actor=auth.actor, endpoint="/ops/polymarket/paper/sweep", method="POST", params={"account_id": account_id}, ip=auth.client_ip) as audit_state:
        try:
            trader = _paper_trader(request, account_id=account_id)
            result = await trader.sweep_open_orders()
            audit_state["extra"] = {"account_id": account_id, "filled": result.get("filled")}
            return ops_api._ok(result)
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))


@router.post(
    "/polymarket/paper/cancel",
    dependencies=[Depends(require_ops_permissions_dependency("manage_orders"))],
)
async def polymarket_paper_cancel(request: Request, payload: ops_api.PolymarketPaperCancelRequest):
    auth = get_request_auth(request)
    params = payload.model_dump()
    async with ops_audit_scope(actor=auth.actor, endpoint="/ops/polymarket/paper/cancel", method="POST", params=params, ip=auth.client_ip) as audit_state:
        try:
            trader = _paper_trader(request, account_id=payload.account_id)
            order = await trader.cancel(payload.order_id)
            audit_state["extra"] = {"account_id": payload.account_id, "order_id": payload.order_id}
            return ops_api._ok(order)
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))


@router.get("/polymarket/paper/orders")
async def polymarket_paper_orders(request: Request, account_id: str = "default", status: Optional[str] = None):
    try:
        trader = _paper_trader(request, account_id=account_id)
        rows = await trader.get_orders(status=status)
        return ops_api._ok({"count": len(rows), "items": rows})
    except Exception as exc:
        return ops_api._err(str(exc))


@router.get("/polymarket/paper/positions")
async def polymarket_paper_positions(request: Request, account_id: str = "default"):
    try:
        trader = _paper_trader(request, account_id=account_id)
        rows = await trader.get_positions()
        return ops_api._ok({"count": len(rows), "items": rows})
    except Exception as exc:
        return ops_api._err(str(exc))


@router.post(
    "/polymarket/paper/strategy_once",
    dependencies=[Depends(require_ops_permissions_dependency("manage_ai_research"))],
)
async def polymarket_paper_strategy_once(request: Request, payload: ops_api.PolymarketPaperStrategyOnceRequest):
    auth = get_request_auth(request)
    params = payload.model_dump()
    async with ops_audit_scope(actor=auth.actor, endpoint="/ops/polymarket/paper/strategy_once", method="POST", params=params, ip=auth.client_ip) as audit_state:
        try:
            profile: Optional[Dict[str, Any]] = None
            if payload.profile_path:
                profile = load_paper_strategy_profile(_resolve_repo_path(payload.profile_path))
            if payload.profile:
                profile = dict(payload.profile)
            result = await run_paper_strategy_once(
                token_ids=payload.token_ids,
                config=PaperStrategyConfig(
                    account_id=payload.account_id,
                    strategy=payload.strategy,
                    order_size=payload.order_size,
                    buy_below=payload.buy_below,
                    sell_above=payload.sell_above,
                    momentum_window=payload.momentum_window,
                    momentum_buy_delta=payload.momentum_buy_delta,
                    momentum_sell_delta=payload.momentum_sell_delta,
                    initial_cash=payload.initial_cash,
                    max_order_notional=payload.max_order_notional,
                    max_position_notional=payload.max_position_notional,
                    fee_rate=payload.fee_rate,
                    max_tokens=payload.max_tokens,
                    min_quotes=payload.min_quotes,
                    dry_run=not payload.execute,
                ),
                profile=profile,
                dry_run_override=not payload.execute,
            )
            audit_state["extra"] = {
                "account_id": payload.account_id,
                "dry_run": not payload.execute,
                "decisions": len(result.get("decisions") or []),
                "orders": len(result.get("orders") or []),
            }
            return ops_api._ok(result)
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))


@router.post(
    "/polymarket/paper/profile/promote",
    dependencies=[Depends(require_ops_permissions_dependency("manage_ai_research"))],
)
async def polymarket_paper_profile_promote(request: Request, payload: ops_api.PolymarketPaperProfilePromoteRequest):
    auth = get_request_auth(request)
    params = payload.model_dump()
    async with ops_audit_scope(actor=auth.actor, endpoint="/ops/polymarket/paper/profile/promote", method="POST", params=params, ip=auth.client_ip) as audit_state:
        try:
            report_path = _resolve_repo_path(payload.report_path)
            report = json.loads(report_path.read_text(encoding="utf-8"))
            token_ids = payload.token_ids or [str(token or "").strip() for token in report.get("token_ids") or [] if str(token or "").strip()]
            profile = profile_from_walk_forward_report(
                report,
                token_ids=token_ids,
                account_id=payload.account_id,
                dry_run=not payload.execute_default,
                guardrails=ProfileGuardrails(
                    min_segments=payload.min_segments,
                    min_positive_segments=payload.min_positive_segments,
                    min_total_test_net_pnl=payload.min_total_test_net_pnl,
                    min_worst_test_net_pnl=payload.min_worst_test_net_pnl,
                ),
                allow_unsafe=payload.allow_unsafe,
            )
            paths = save_paper_strategy_profile(profile, _resolve_profile_output(payload.output_path), overwrite=False)
            audit_state["extra"] = {"profile_path": paths.get("profile_path"), "tokens": len(profile.get("token_ids") or [])}
            return ops_api._ok({"paths": paths, "profile": profile})
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))


@router.post(
    "/polymarket/replay/batch",
    dependencies=[Depends(require_ops_permissions_dependency("manage_ai_research"))],
)
async def polymarket_replay_batch(request: Request, payload: ops_api.PolymarketReplayBatchRequest):
    auth = get_request_auth(request)
    params = payload.model_dump()
    async with ops_audit_scope(actor=auth.actor, endpoint="/ops/polymarket/replay/batch", method="POST", params=params, ip=auth.client_ip) as audit_state:
        try:
            resolved = await _resolve_replay_token_ids(payload)
            report = await run_replay_batch(
                token_ids=resolved["token_ids"],
                since=resolved["since"],
                until=resolved["until"],
                config=_replay_batch_config_from_payload(payload),
            )
            paths = {}
            if payload.write_report:
                paths = write_replay_batch_report(
                    report,
                    _resolve_report_dir(payload.output_dir),
                    name=_sanitize_report_name(payload.name, "polymarket_replay_batch"),
                )
            audit_state["extra"] = {"tokens": len(resolved["token_ids"]), "best": (report.get("best") or {}).get("token_id")}
            data = _compact_replay_report(report, include_details=payload.include_details)
            data["token_ids"] = resolved["token_ids"]
            data["universe"] = resolved["universe"]
            data["paths"] = paths
            return ops_api._ok(data)
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))


@router.post(
    "/polymarket/replay/grid",
    dependencies=[Depends(require_ops_permissions_dependency("manage_ai_research"))],
)
async def polymarket_replay_grid(request: Request, payload: ops_api.PolymarketReplayGridRequest):
    auth = get_request_auth(request)
    params = payload.model_dump()
    async with ops_audit_scope(actor=auth.actor, endpoint="/ops/polymarket/replay/grid", method="POST", params=params, ip=auth.client_ip) as audit_state:
        try:
            resolved = await _resolve_replay_token_ids(payload)
            grid = _replay_param_grid_from_payload(payload)
            if not grid:
                raise ValueError("parameter grid is empty")
            report = await run_replay_grid(
                token_ids=resolved["token_ids"],
                since=resolved["since"],
                until=resolved["until"],
                base_config=_replay_batch_config_from_payload(payload),
                param_grid=grid,
            )
            paths = {}
            if payload.write_report:
                paths = write_replay_grid_report(
                    report,
                    _resolve_report_dir(payload.output_dir),
                    name=_sanitize_report_name(payload.name, "polymarket_replay_grid"),
                )
            audit_state["extra"] = {"tokens": len(resolved["token_ids"]), "grid_size": len(grid)}
            data = _compact_replay_report(report, include_details=payload.include_details)
            data["token_ids"] = resolved["token_ids"]
            data["universe"] = resolved["universe"]
            data["paths"] = paths
            return ops_api._ok(data)
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))


@router.post(
    "/polymarket/replay/walk_forward",
    dependencies=[Depends(require_ops_permissions_dependency("manage_ai_research"))],
)
async def polymarket_replay_walk_forward(request: Request, payload: ops_api.PolymarketReplayWalkForwardRequest):
    auth = get_request_auth(request)
    params = payload.model_dump()
    async with ops_audit_scope(actor=auth.actor, endpoint="/ops/polymarket/replay/walk_forward", method="POST", params=params, ip=auth.client_ip) as audit_state:
        try:
            resolved = await _resolve_replay_token_ids(payload)
            grid = _replay_param_grid_from_payload(payload)
            if not grid:
                raise ValueError("parameter grid is empty")
            report = await run_replay_walk_forward(
                token_ids=resolved["token_ids"],
                since=resolved["since"],
                until=resolved["until"],
                base_config=_replay_batch_config_from_payload(payload),
                param_grid=grid,
                walk_config=ReplayWalkForwardConfig(
                    train_minutes=payload.train_minutes,
                    test_minutes=payload.test_minutes,
                    step_minutes=payload.step_minutes,
                ),
            )
            paths = {}
            if payload.write_report:
                paths = write_replay_walk_forward_report(
                    report,
                    _resolve_report_dir(payload.output_dir),
                    name=_sanitize_report_name(payload.name, "polymarket_replay_walk_forward"),
                )
            audit_state["extra"] = {"tokens": len(resolved["token_ids"]), "grid_size": len(grid), "segments": (report.get("summary") or {}).get("segments")}
            data = _compact_replay_report(report, include_details=payload.include_details)
            data["token_ids"] = resolved["token_ids"]
            data["universe"] = resolved["universe"]
            data["paths"] = paths
            return ops_api._ok(data)
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))


@router.post(
    "/polymarket/arm_trading",
    dependencies=[Depends(require_ops_permissions_dependency("request_live"))],
)
async def polymarket_arm_trading(request: Request):
    auth = get_request_auth(request)
    async with ops_audit_scope(actor=auth.actor, endpoint="/ops/polymarket/arm_trading", method="POST", params={}, ip=auth.client_ip) as audit_state:
        try:
            ops_api._ensure_ops_runtime_state(request.app)
            code = secrets.token_urlsafe(8).replace("-", "").replace("_", "")[:10]
            expires_at = ops_api._now_utc() + timedelta(seconds=120)
            request.app.state.polymarket_trading_approvals[code] = {
                "approval_code": code,
                "issued_at": ops_api._now_utc(),
                "expires_at": expires_at,
                "actor": auth.actor,
                "used": False,
            }
            return ops_api._ok({"approval_code": code, "expires_at": expires_at.isoformat(), "note": "Call /ops/polymarket/enable_trading with X-OPS-APPROVAL within 120s"})
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))


@router.post(
    "/polymarket/enable_trading",
    dependencies=[Depends(require_ops_permissions_dependency("approve_live"))],
)
async def polymarket_enable_trading(request: Request, x_ops_approval: Optional[str] = Header(default=None, alias="X-OPS-APPROVAL")):
    auth = get_request_auth(request)
    params = {"approval_code": x_ops_approval or ""}
    async with ops_audit_scope(actor=auth.actor, endpoint="/ops/polymarket/enable_trading", method="POST", params=params, ip=auth.client_ip) as audit_state:
        try:
            ops_api._ensure_ops_runtime_state(request.app)
            code = str(x_ops_approval or "").strip()
            approval = request.app.state.polymarket_trading_approvals.get(code)
            if not approval:
                audit_state["status"] = "denied"
                audit_state["error"] = "invalid approval code"
                raise HTTPException(status_code=403, detail="invalid approval code")
            if approval.get("expires_at") <= ops_api._now_utc():
                request.app.state.polymarket_trading_approvals.pop(code, None)
                audit_state["status"] = "denied"
                audit_state["error"] = "approval code expired"
                raise HTTPException(status_code=403, detail="approval code expired")
            request.app.state.polymarket_cfg.setdefault("defaults", {}).setdefault("trading", {})["enabled"] = True
            request.app.state.polymarket_trading_approvals.pop(code, None)
            return ops_api._ok({"trading_enabled": True})
        except HTTPException:
            raise
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))
