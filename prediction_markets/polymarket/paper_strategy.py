from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from prediction_markets.polymarket import db as pm_db
from prediction_markets.polymarket.paper_replay import MomentumReplayStrategy, ReplayConfig, ThresholdReplayStrategy, quote_midpoint
from prediction_markets.polymarket.paper_trading import PaperRiskLimits, PolymarketPaperTrader


@dataclass
class PaperStrategyConfig:
    account_id: str = "default"
    strategy: str = "threshold"
    order_size: float = 10.0
    buy_below: float = 0.45
    sell_above: float = 0.60
    momentum_window: int = 3
    momentum_buy_delta: float = 0.04
    momentum_sell_delta: float = 0.04
    initial_cash: float = 1000.0
    max_order_notional: float = 50.0
    max_position_notional: float = 200.0
    fee_rate: float = 0.0
    max_tokens: int = 20
    min_quotes: int = 1
    dry_run: bool = True


@dataclass
class ProfileGuardrails:
    min_segments: int = 2
    min_positive_segments: int = 1
    min_total_test_net_pnl: float = 0.0
    min_worst_test_net_pnl: float = -100.0
    require_params: bool = True


PROFILE_CONFIG_FIELDS = {
    "account_id",
    "strategy",
    "order_size",
    "buy_below",
    "sell_above",
    "momentum_window",
    "momentum_buy_delta",
    "momentum_sell_delta",
    "initial_cash",
    "max_order_notional",
    "max_position_notional",
    "fee_rate",
    "max_tokens",
    "min_quotes",
    "dry_run",
}


def _unique_tokens(values: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values or []:
        token = str(value or "").strip()
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def apply_profile_to_config(config: PaperStrategyConfig, profile: Dict[str, Any], *, dry_run: Optional[bool] = None) -> PaperStrategyConfig:
    data = asdict(config)
    profile_config = dict(profile.get("config") or {})
    params = dict(profile.get("params") or {})
    for key, value in profile_config.items():
        if key in PROFILE_CONFIG_FIELDS:
            data[key] = value
    strategy = profile.get("strategy")
    if strategy:
        data["strategy"] = str(strategy)
    for key, value in params.items():
        if key in PROFILE_CONFIG_FIELDS:
            data[key] = value
    if dry_run is not None:
        data["dry_run"] = bool(dry_run)
    return PaperStrategyConfig(**data)


def load_paper_strategy_profile(path: Path) -> Dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def save_paper_strategy_profile(profile: Dict[str, Any], path: Path) -> Dict[str, str]:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(profile, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return {"profile_path": str(target)}


def validate_profile_promotion(
    report: Dict[str, Any],
    *,
    params: Dict[str, Any],
    token_ids: List[str],
    guardrails: Optional[ProfileGuardrails] = None,
) -> Dict[str, Any]:
    guard = guardrails or ProfileGuardrails()
    summary = dict(report.get("summary") or {})
    metrics = {
        "segments": int(summary.get("segments") or 0),
        "positive_segments": int(summary.get("positive_segments") or 0),
        "total_test_net_pnl": float(summary.get("total_test_net_pnl") or 0.0),
        "worst_test_net_pnl": float(summary.get("worst_test_net_pnl") or 0.0),
        "tokens": len(_unique_tokens(token_ids)),
    }
    failures: List[str] = []
    if metrics["tokens"] <= 0:
        failures.append("token_ids is empty")
    if metrics["segments"] < int(guard.min_segments):
        failures.append(f"segments {metrics['segments']} < min_segments {guard.min_segments}")
    if metrics["positive_segments"] < int(guard.min_positive_segments):
        failures.append(f"positive_segments {metrics['positive_segments']} < min_positive_segments {guard.min_positive_segments}")
    if metrics["total_test_net_pnl"] < float(guard.min_total_test_net_pnl):
        failures.append(f"total_test_net_pnl {metrics['total_test_net_pnl']:.8f} < min_total_test_net_pnl {guard.min_total_test_net_pnl:.8f}")
    if metrics["worst_test_net_pnl"] < float(guard.min_worst_test_net_pnl):
        failures.append(f"worst_test_net_pnl {metrics['worst_test_net_pnl']:.8f} < min_worst_test_net_pnl {guard.min_worst_test_net_pnl:.8f}")
    if bool(guard.require_params) and not params:
        failures.append("selected params is empty")
    return {
        "ok": not failures,
        "failures": failures,
        "metrics": metrics,
        "guardrails": asdict(guard),
    }


def profile_from_walk_forward_report(
    report: Dict[str, Any],
    *,
    token_ids: List[str],
    account_id: str = "default",
    dry_run: bool = True,
    guardrails: Optional[ProfileGuardrails] = None,
    allow_unsafe: bool = False,
) -> Dict[str, Any]:
    base_config = dict(report.get("base_config") or {})
    summary = dict(report.get("summary") or {})
    selection_counts = list(summary.get("selection_counts") or [])
    selected_params: Dict[str, Any] = {}
    if selection_counts:
        selected_params = dict(selection_counts[0].get("params") or {})
    elif report.get("rows"):
        selected_params = dict((report.get("rows") or [{}])[0].get("selected_params") or {})
    clean_tokens = _unique_tokens(token_ids)
    validation = validate_profile_promotion(report, params=selected_params, token_ids=clean_tokens, guardrails=guardrails)
    if not validation["ok"] and not allow_unsafe:
        raise ValueError("profile promotion rejected: " + "; ".join(validation["failures"]))
    strategy = str(base_config.get("strategy") or "threshold")
    config = {
        "account_id": account_id,
        "strategy": strategy,
        "order_size": float(base_config.get("order_size") or 10.0),
        "initial_cash": float(base_config.get("initial_cash") or 1000.0),
        "max_order_notional": float(base_config.get("max_order_notional") or 50.0),
        "max_position_notional": float(base_config.get("max_position_notional") or 200.0),
        "fee_rate": float(base_config.get("fee_rate") or 0.0),
        "dry_run": bool(dry_run),
    }
    return {
        "kind": "polymarket_paper_strategy_profile",
        "source": "walk_forward",
        "safe_to_execute": bool(validation["ok"]),
        "validation": validation,
        "strategy": strategy,
        "params": selected_params,
        "token_ids": clean_tokens,
        "config": config,
        "metrics": {
            "segments": int(summary.get("segments") or 0),
            "total_test_net_pnl": float(summary.get("total_test_net_pnl") or 0.0),
            "avg_test_net_pnl": float(summary.get("avg_test_net_pnl") or 0.0),
            "worst_test_net_pnl": float(summary.get("worst_test_net_pnl") or 0.0),
            "positive_segments": int(summary.get("positive_segments") or 0),
            "selection_counts": selection_counts,
        },
    }


async def resolve_strategy_tokens(token_ids: Optional[List[str]] = None, *, max_tokens: int = 20, min_quotes: int = 1) -> List[str]:
    manual = _unique_tokens(token_ids or [])
    if manual:
        return manual[: max(1, int(max_tokens or len(manual)))]
    status = await pm_db.get_pm_status()
    now = None
    states = status.get("source_states") or []
    for state in states:
        if str(state.get("source") or "") in {"clob_rest", "clob_ws"} and state.get("last_ts"):
            now = state.get("last_ts")
            break
    if now is None:
        from prediction_markets.polymarket.utils import utc_now

        now = utc_now()
    from datetime import timedelta
    from prediction_markets.polymarket.utils import parse_ts_any

    until = parse_ts_any(now)
    since = until - timedelta(days=3650)
    rows = await pm_db.list_active_quote_tokens(since, until, limit=max_tokens, min_quotes=min_quotes)
    return [str(row.get("token_id") or "") for row in rows if str(row.get("token_id") or "").strip()]


async def _momentum_history(token_id: str, window: int) -> List[Dict[str, Any]]:
    from datetime import timedelta
    from prediction_markets.polymarket.utils import parse_ts_any

    latest = await pm_db.get_latest_quote(token_id)
    if not latest:
        return []
    until = parse_ts_any(latest["ts"])
    since = until - timedelta(days=7)
    return await pm_db.get_token_quotes(token_id, since, until, limit=max(2, int(window or 1) + 1))


async def _decision_for_quote(quote: Dict[str, Any], position: Optional[Dict[str, Any]], config: PaperStrategyConfig):
    replay_cfg = ReplayConfig(
        account_id=config.account_id,
        initial_cash=config.initial_cash,
        order_size=config.order_size,
        buy_below=config.buy_below,
        sell_above=config.sell_above,
        momentum_window=config.momentum_window,
        momentum_buy_delta=config.momentum_buy_delta,
        momentum_sell_delta=config.momentum_sell_delta,
        max_order_notional=config.max_order_notional,
        max_position_notional=config.max_position_notional,
        fee_rate=config.fee_rate,
    )
    strategy_name = str(config.strategy or "threshold").strip().lower()
    if strategy_name == "momentum":
        strategy = MomentumReplayStrategy()
        for item in await _momentum_history(str(quote.get("token_id") or ""), config.momentum_window):
            if item.get("id") == quote.get("id"):
                continue
            await strategy.decide(quote=item, position=position, config=replay_cfg)
        return await strategy.decide(quote=quote, position=position, config=replay_cfg)
    if strategy_name in {"threshold", "basic", "default"}:
        return await ThresholdReplayStrategy().decide(quote=quote, position=position, config=replay_cfg)
    if strategy_name in {"hold", "none", "noop"}:
        return None
    raise ValueError(f"unknown paper strategy: {config.strategy}")


async def run_paper_strategy_once(
    token_ids: Optional[List[str]] = None,
    config: Optional[PaperStrategyConfig] = None,
    profile: Optional[Dict[str, Any]] = None,
    dry_run_override: Optional[bool] = None,
) -> Dict[str, Any]:
    cfg = config or PaperStrategyConfig()
    profile_tokens: List[str] = []
    if profile:
        cfg = apply_profile_to_config(cfg, profile, dry_run=dry_run_override)
        profile_tokens = [str(token or "").strip() for token in profile.get("token_ids") or [] if str(token or "").strip()]
    elif dry_run_override is not None:
        cfg = PaperStrategyConfig(**{**asdict(cfg), "dry_run": bool(dry_run_override)})
    tokens = await resolve_strategy_tokens(token_ids or profile_tokens, max_tokens=cfg.max_tokens, min_quotes=cfg.min_quotes)
    trader = PolymarketPaperTrader(
        account_id=cfg.account_id,
        limits=PaperRiskLimits(
            initial_cash=cfg.initial_cash,
            max_order_notional=cfg.max_order_notional,
            max_position_notional=cfg.max_position_notional,
            fee_rate=cfg.fee_rate,
        ),
    )
    await trader.ensure_account()
    positions = await pm_db.list_paper_positions(cfg.account_id, include_flat=True)
    positions_by_token = {str(item.get("token_id") or ""): item for item in positions}
    decisions: List[Dict[str, Any]] = []
    orders: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    quote_overrides: Dict[str, Dict[str, Any]] = {}
    for token_id in tokens:
        quote = await pm_db.get_latest_quote(token_id)
        if not quote:
            skipped.append({"token_id": token_id, "reason": "missing_quote"})
            continue
        quote_overrides[token_id] = quote
        position = positions_by_token.get(token_id)
        decision = await _decision_for_quote(quote, position, cfg)
        if not decision:
            skipped.append({"token_id": token_id, "reason": "no_signal", "midpoint": quote_midpoint(quote)})
            continue
        plan = {
            "token_id": token_id,
            "market_id": quote.get("market_id"),
            "outcome": quote.get("outcome") or "YES",
            "action": decision.action,
            "price": decision.price,
            "size": decision.size,
            "reason": decision.reason,
            "midpoint": quote_midpoint(quote),
            "quote_ts": quote.get("ts"),
        }
        decisions.append(plan)
        if cfg.dry_run:
            continue
        try:
            order = await trader.place_limit(
                market_id=str(quote.get("market_id") or ""),
                token_id=token_id,
                outcome=str(quote.get("outcome") or "YES"),
                side=decision.action,
                price=decision.price,
                size=decision.size,
                metadata={"paper_strategy": True, "signal": decision.reason, "quote_ts": quote.get("ts")},
                fill_immediately=False,
            )
            filled = await trader.try_fill_order(order["order_id"], quote=quote)
            orders.append(filled or order)
        except Exception as exc:
            skipped.append({"token_id": token_id, "reason": "order_rejected", "error": str(exc), "plan": plan})
    summary = await trader.get_summary(quote_overrides=quote_overrides)
    return {
        "config": asdict(cfg),
        "profile": profile or None,
        "tokens": tokens,
        "dry_run": bool(cfg.dry_run),
        "decisions": decisions,
        "orders": orders,
        "skipped": skipped,
        "summary": summary,
    }
