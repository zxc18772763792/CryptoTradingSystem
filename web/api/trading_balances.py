from __future__ import annotations

import asyncio
import contextlib
import copy
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter

from web.api import trading as trading_api


router = APIRouter()
_BALANCE_RESPONSE_CACHE_TTL_SEC = 10.0
_BALANCE_RESPONSE_STALE_TTL_SEC = 90.0
_BALANCE_RESPONSE_TIMEOUT_SEC = 18.0
_BALANCE_RESPONSE_CACHE: Dict[str, Dict[str, Any]] = {}
_BALANCE_RESPONSE_TASKS: Dict[str, asyncio.Task] = {}


def _clone_balance_payload(mode_name: str, *, max_age_sec: float, stale_note: Optional[str] = None) -> Optional[Dict[str, Any]]:
    cached = _BALANCE_RESPONSE_CACHE.get(str(mode_name or "").strip().lower())
    if not cached:
        return None
    age_sec = max(0.0, time.time() - float(cached.get("ts") or 0.0))
    if age_sec > max(0.0, float(max_age_sec or 0.0)):
        return None
    payload = copy.deepcopy(cached.get("payload") or {})
    payload["from_cache"] = True
    payload["cache_age_sec"] = round(age_sec, 2)
    if stale_note:
        payload["stale"] = True
        payload["warning"] = stale_note
    return payload


def _minimal_balance_payload(mode_name: str, note: str) -> Dict[str, Any]:
    resolved_mode = str(mode_name or ("paper" if trading_api.execution_engine.is_paper_mode() else "live")).strip().lower()
    is_paper_mode = resolved_mode == "paper"
    risk_report = trading_api.risk_manager.get_risk_report()
    exchanges = {}
    for exchange_name in ["gate", "binance", "okx"]:
        connector = trading_api.exchange_manager.get_exchange(exchange_name)
        exchanges[exchange_name] = {
            "connected": bool(getattr(connector, "is_connected", False)),
            "balances": [],
            "total_usd": 0.0,
            "error": note,
            "from_cache": False,
        }
    return {
        "exchanges": exchanges,
        "distribution": [],
        "total_usd_estimate": 0.0,
        "market_total_usd_estimate": 0.0,
        "binance_total_usd_estimate": 0.0,
        "paper_equity_estimate": 0.0 if is_paper_mode else None,
        "real_account_usd_estimate": 0.0,
        "virtual_account_usd_estimate": 0.0 if is_paper_mode else None,
        "active_account_type": "paper" if is_paper_mode else "live",
        "active_account_usd_estimate": 0.0,
        "inactive_account_usd_estimate": None,
        "paper_account": None,
        "risk_equity_input": 0.0,
        "live_day_start_equity": None,
        "live_daily_total_pnl_usd": None,
        "live_unrealized_pnl_usd": 0.0,
        "live_position_count": int(trading_api.position_manager.get_position_count() or 0) if is_paper_mode else 0,
        "unpriced_assets": 0,
        "connected_exchanges": trading_api.exchange_manager.get_connected_exchanges(),
        "mode": resolved_mode,
        "risk_report": risk_report,
        "risk": {
            "trading_halted": risk_report.get("trading_halted", False),
            "risk_level": risk_report.get("risk_level", "low"),
        },
        "notifications": {"triggered_count": 0},
        "warning": note,
        "stale": True,
        "from_cache": False,
    }


def _balance_response_fallback(mode_name: str, note: str) -> Dict[str, Any]:
    stale_payload = _clone_balance_payload(
        mode_name,
        max_age_sec=_BALANCE_RESPONSE_STALE_TTL_SEC,
        stale_note=note,
    )
    if stale_payload is not None:
        return stale_payload
    return _minimal_balance_payload(mode_name, note)


@router.get("/balance")
async def get_balance(exchange: str = "gate"):
    connector = trading_api.exchange_manager.get_exchange(exchange)
    if not connector:
        return {
            "exchange": exchange,
            "balances": [],
            "error": "Exchange not connected",
        }

    try:
        balances = await connector.get_balance()
        return {
            "exchange": exchange,
            "balances": [
                {
                    "currency": b.currency,
                    "free": b.free,
                    "used": b.used,
                    "total": b.total,
                }
                for b in balances
            ],
        }
    except Exception as e:
        return {
            "exchange": exchange,
            "balances": [],
            "error": str(e),
        }


async def _build_all_balances_payload():
    results: Dict[str, Dict[str, Any]] = {}
    total_usd = 0.0
    distribution_map: Dict[str, float] = {}
    exchange_total_map: Dict[str, float] = {}
    total_unpriced_assets = 0
    mode_name = trading_api.execution_engine.get_trading_mode()
    is_paper_mode = trading_api.execution_engine.is_paper_mode()
    trading_api.risk_manager.set_account_scope("paper" if is_paper_mode else "live", reset_baseline=False)
    paper_account: Optional[Dict[str, Any]] = None

    async def _collect_exchange(exchange_name: str):
        now_ts = time.time()
        cached = trading_api._BALANCE_SNAPSHOT_CACHE.get(exchange_name)
        if cached and (now_ts - float(cached.get("ts", 0.0))) <= trading_api._BALANCE_SNAPSHOT_FAST_AGE_SEC:
            age = max(0.0, now_ts - float(cached.get("ts", 0.0)))
            cached_result = dict(cached.get("result") or {})
            cached_result["from_cache"] = True
            cached_result["cache_age_sec"] = round(age, 2)
            return (
                exchange_name,
                cached_result,
                float(cached.get("total_usd", 0.0) or 0.0),
                dict(cached.get("distribution") or {}),
            )

        if exchange_name == "binance" and not is_paper_mode:
            try:
                fast_snapshot = await trading_api.asyncio.wait_for(
                    trading_api._fetch_binance_live_wallet_snapshot_fast(),
                    timeout=max(trading_api._BALANCE_FETCH_TIMEOUT_SEC, 10.5),
                )
                exchange_result = {
                    "connected": True,
                    "balances": list(fast_snapshot.get("balances") or []),
                    "total_usd": round(trading_api._safe_float(fast_snapshot.get("total_usd"), default=0.0), 2),
                    "valuation_coverage": dict(fast_snapshot.get("valuation_coverage") or {}),
                    "from_cache": False,
                    "fallback_used": True,
                    "wallet_components": dict(fast_snapshot.get("components") or {}),
                }
                warnings = [str(x) for x in (fast_snapshot.get("warnings") or []) if str(x).strip()]
                if warnings:
                    exchange_result["warning"] = " | ".join(warnings[:3])
                local_distribution = dict(fast_snapshot.get("distribution") or {})
                exchange_total_usd = float(fast_snapshot.get("total_usd") or 0.0)
                trading_api._BALANCE_SNAPSHOT_CACHE[exchange_name] = {
                    "ts": time.time(),
                    "result": {
                        "connected": exchange_result["connected"],
                        "balances": exchange_result["balances"],
                        "total_usd": exchange_result["total_usd"],
                        "wallet_components": exchange_result.get("wallet_components"),
                    },
                    "total_usd": exchange_total_usd,
                    "distribution": dict(local_distribution),
                }
                return (
                    exchange_name,
                    exchange_result,
                    exchange_total_usd,
                    local_distribution,
                )
            except Exception as fast_err:
                trading_api.logger.warning(f"[binance] live fast wallet snapshot failed: {fast_err}")
                if cached and (time.time() - float(cached.get("ts", 0.0))) <= trading_api._BALANCE_SNAPSHOT_CACHE_TTL_SEC:
                    age = max(0.0, time.time() - float(cached.get("ts", 0.0)))
                    cached_result = dict(cached.get("result") or {})
                    cached_result["from_cache"] = True
                    cached_result["cache_age_sec"] = round(age, 2)
                    cached_result["warning"] = str(fast_err)
                    return (
                        exchange_name,
                        cached_result,
                        float(cached.get("total_usd", 0.0) or 0.0),
                        dict(cached.get("distribution") or {}),
                    )

        connector = trading_api.exchange_manager.get_exchange(exchange_name)
        if not connector:
            return (
                exchange_name,
                {
                    "connected": False,
                    "balances": [],
                    "total_usd": 0,
                    "from_cache": False,
                },
                0.0,
                {},
            )

        try:
            balances = await trading_api.asyncio.wait_for(
                connector.get_balance(),
                timeout=trading_api._BALANCE_FETCH_TIMEOUT_SEC,
            )
            exchange_balances: List[Dict[str, Any]] = []
            exchange_total_usd = 0.0
            local_distribution: Dict[str, float] = {}

            quote_map: Dict[str, float] = {}
            price_candidates: List[str] = []
            last_unit_usd: Dict[str, float] = {}
            if cached:
                for row in (cached.get("result") or {}).get("balances", []):
                    if not isinstance(row, dict):
                        continue
                    currency = str(row.get("currency") or "").upper()
                    total_prev = float(row.get("total") or 0.0)
                    usd_prev = float(row.get("usd_value") or 0.0)
                    if currency and total_prev > 0 and usd_prev > 0:
                        last_unit_usd[currency] = usd_prev / total_prev
            for b in balances:
                ccy = str(b.currency or "").upper()
                total = float(b.total or 0.0)
                if total <= 0:
                    continue
                if ccy in trading_api.STABLE_COINS:
                    continue
                if ccy not in price_candidates:
                    price_candidates.append(ccy)

            if price_candidates:
                quote_map = await trading_api.build_currency_usd_quotes(
                    connector=connector,
                    currencies=price_candidates,
                    timeout_sec=trading_api._TICKER_FETCH_TIMEOUT_SEC,
                    max_parallel=2,
                )

            priced_assets = 0
            unpriced_assets = 0
            for b in balances:
                currency = str(b.currency or "").upper()
                total = float(b.total or 0.0)
                unit_usd = 1.0 if currency in trading_api.STABLE_COINS else float(quote_map.get(currency, 0.0) or 0.0)
                valuation_source = "live" if unit_usd > 0 and currency not in trading_api.STABLE_COINS else "stable"
                if unit_usd <= 0 and currency not in trading_api.STABLE_COINS:
                    fallback_unit = float(last_unit_usd.get(currency, 0.0) or 0.0)
                    if fallback_unit > 0:
                        unit_usd = fallback_unit
                        valuation_source = "cache"
                usd_value = float(total) * float(unit_usd) if total > 0 and unit_usd > 0 else 0.0
                if total > 0:
                    if usd_value > 0:
                        priced_assets += 1
                    else:
                        unpriced_assets += 1
                exchange_total_usd += usd_value
                local_distribution[currency] = local_distribution.get(currency, 0.0) + usd_value
                exchange_balances.append(
                    {
                        "currency": currency,
                        "free": float(b.free or 0.0),
                        "used": float(b.used or 0.0),
                        "total": total,
                        "usd_value": round(usd_value, 4),
                        "unit_usd": round(float(unit_usd), 8) if unit_usd > 0 else 0.0,
                        "valuation_source": valuation_source,
                    }
                )

            exchange_balances.sort(key=lambda item: item["usd_value"], reverse=True)
            exchange_result = {
                "connected": True,
                "balances": exchange_balances,
                "total_usd": round(exchange_total_usd, 2),
                "valuation_coverage": {
                    "priced_assets": priced_assets,
                    "unpriced_assets": unpriced_assets,
                },
                "from_cache": False,
            }
            trading_api._BALANCE_SNAPSHOT_CACHE[exchange_name] = {
                "ts": time.time(),
                "result": {
                    "connected": exchange_result["connected"],
                    "balances": exchange_result["balances"],
                    "total_usd": exchange_result["total_usd"],
                },
                "total_usd": exchange_total_usd,
                "distribution": dict(local_distribution),
            }
            return (
                exchange_name,
                exchange_result,
                exchange_total_usd,
                local_distribution,
            )
        except Exception as e:
            cached = trading_api._BALANCE_SNAPSHOT_CACHE.get(exchange_name)
            if cached and (time.time() - float(cached.get("ts", 0.0))) <= trading_api._BALANCE_SNAPSHOT_CACHE_TTL_SEC:
                err_msg = (
                    f"balance request timeout after {trading_api._BALANCE_FETCH_TIMEOUT_SEC:.0f}s"
                    if isinstance(e, trading_api.asyncio.TimeoutError)
                    else str(e)
                )
                age = max(0.0, time.time() - float(cached.get("ts", 0.0)))
                cached_result = dict(cached.get("result") or {})
                cached_result["from_cache"] = True
                cached_result["cache_age_sec"] = round(age, 2)
                cached_result["warning"] = err_msg
                return (
                    exchange_name,
                    cached_result,
                    float(cached.get("total_usd", 0.0) or 0.0),
                    dict(cached.get("distribution") or {}),
                )

            if exchange_name == "binance":
                try:
                    trading_api.logger.warning(f"[binance] primary balance fetch failed, trying readonly fallback: {e}")
                    balances = await trading_api.asyncio.wait_for(
                        trading_api._fetch_binance_balances_via_fallback(),
                        timeout=min(max(trading_api._BALANCE_FETCH_TIMEOUT_SEC * 0.5, 5.0), 8.0),
                    )
                    if balances:
                        exchange_balances: List[Dict[str, Any]] = []
                        exchange_total_usd = 0.0
                        local_distribution: Dict[str, float] = {}
                        quote_map: Dict[str, float] = {}
                        price_candidates: List[str] = []
                        for b in balances:
                            ccy = str(getattr(b, "currency", "") or "").upper()
                            total = float(getattr(b, "total", 0.0) or 0.0)
                            if total > 0 and ccy not in trading_api.STABLE_COINS and ccy not in price_candidates:
                                price_candidates.append(ccy)
                        if price_candidates:
                            quote_map = await trading_api.build_currency_usd_quotes(
                                connector=connector,
                                currencies=price_candidates,
                                timeout_sec=trading_api._TICKER_FETCH_TIMEOUT_SEC,
                                max_parallel=2,
                            )
                        priced_assets = 0
                        unpriced_assets = 0
                        for b in balances:
                            currency = str(getattr(b, "currency", "") or "").upper()
                            total = float(getattr(b, "total", 0.0) or 0.0)
                            unit_usd = 1.0 if currency in trading_api.STABLE_COINS else float(quote_map.get(currency, 0.0) or 0.0)
                            valuation_source = "live" if unit_usd > 0 and currency not in trading_api.STABLE_COINS else "stable"
                            usd_value = float(total) * float(unit_usd) if total > 0 and unit_usd > 0 else 0.0
                            if total > 0:
                                if usd_value > 0:
                                    priced_assets += 1
                                else:
                                    unpriced_assets += 1
                            exchange_total_usd += usd_value
                            local_distribution[currency] = local_distribution.get(currency, 0.0) + usd_value
                            exchange_balances.append(
                                {
                                    "currency": currency,
                                    "free": float(getattr(b, "free", 0.0) or 0.0),
                                    "used": float(getattr(b, "used", 0.0) or 0.0),
                                    "total": total,
                                    "usd_value": round(usd_value, 4),
                                    "unit_usd": round(float(unit_usd), 8) if unit_usd > 0 else 0.0,
                                    "valuation_source": valuation_source,
                                }
                            )
                        exchange_balances.sort(key=lambda item: item["usd_value"], reverse=True)
                        exchange_result = {
                            "connected": True,
                            "balances": exchange_balances,
                            "total_usd": round(exchange_total_usd, 2),
                            "valuation_coverage": {
                                "priced_assets": priced_assets,
                                "unpriced_assets": unpriced_assets,
                            },
                            "from_cache": False,
                            "fallback_used": True,
                        }
                        trading_api._BALANCE_SNAPSHOT_CACHE[exchange_name] = {
                            "ts": time.time(),
                            "result": {
                                "connected": exchange_result["connected"],
                                "balances": exchange_result["balances"],
                                "total_usd": exchange_result["total_usd"],
                            },
                            "total_usd": exchange_total_usd,
                            "distribution": dict(local_distribution),
                        }
                        return (
                            exchange_name,
                            exchange_result,
                            exchange_total_usd,
                            local_distribution,
                        )
                except Exception as fallback_err:
                    trading_api.logger.error(f"[binance] readonly fallback balance fetch failed: {fallback_err}")

            err_msg = (
                f"balance request timeout after {trading_api._BALANCE_FETCH_TIMEOUT_SEC:.0f}s"
                if isinstance(e, trading_api.asyncio.TimeoutError)
                else str(e)
            )
            trading_api.logger.error(f"[{exchange_name}] Failed to get balances: {err_msg}")
            cached = trading_api._BALANCE_SNAPSHOT_CACHE.get(exchange_name)
            if cached and (time.time() - float(cached.get("ts", 0.0))) <= trading_api._BALANCE_SNAPSHOT_CACHE_TTL_SEC:
                age = max(0.0, time.time() - float(cached.get("ts", 0.0)))
                cached_result = dict(cached.get("result") or {})
                cached_result["from_cache"] = True
                cached_result["cache_age_sec"] = round(age, 2)
                cached_result["warning"] = err_msg
                return (
                    exchange_name,
                    cached_result,
                    float(cached.get("total_usd", 0.0) or 0.0),
                    dict(cached.get("distribution") or {}),
                )
            return (
                exchange_name,
                {
                    "connected": bool(getattr(connector, "is_connected", False)),
                    "error": err_msg,
                    "balances": [],
                    "total_usd": 0,
                    "from_cache": False,
                },
                0.0,
                {},
            )

    rows = await trading_api.asyncio.gather(
        *[_collect_exchange(exchange_name) for exchange_name in ["gate", "binance", "okx"]],
        return_exceptions=False,
    )
    for exchange_name, exchange_result, exchange_total_usd, local_distribution in rows:
        results[exchange_name] = exchange_result
        total_usd += float(exchange_total_usd or 0.0)
        exchange_total_map[exchange_name] = float(exchange_total_usd or 0.0)
        coverage = exchange_result.get("valuation_coverage") if isinstance(exchange_result, dict) else None
        total_unpriced_assets += int(((coverage or {}).get("unpriced_assets") or 0))
        for ccy, val in local_distribution.items():
            distribution_map[ccy] = distribution_map.get(ccy, 0.0) + float(val or 0.0)

    market_total_usd = float(total_usd or 0.0)
    risk_report_before = trading_api.risk_manager.get_risk_report()
    prev_equity = float(((risk_report_before.get("equity") or {}).get("current") or 0.0))
    risk_equity_input = float(market_total_usd)
    paper_equity = 0.0
    live_position_snapshot: Dict[str, Any] = {"unrealized_pnl_usd": 0.0, "position_count": 0, "by_exchange": {}}
    live_equity_baseline: Dict[str, Any] = {}
    live_day_start_equity = 0.0
    live_daily_total_pnl = 0.0
    balance_warning_present = any(
        isinstance(v, dict) and (v.get("error") or v.get("warning"))
        for v in results.values()
    )
    binance_balance_issue = bool(
        isinstance(results.get("binance"), dict)
        and ((results["binance"].get("error")) or (results["binance"].get("warning")))
    )

    if is_paper_mode:
        try:
            paper_equity = float(await trading_api.execution_engine.get_account_equity_snapshot() or 0.0)
            if paper_equity > 0:
                risk_equity_input = paper_equity
        except Exception as e:
            trading_api.logger.warning(f"Failed to refresh paper equity snapshot: {e}")
    else:
        try:
            live_position_snapshot = await trading_api._collect_live_position_snapshot(force_refresh=False)
        except Exception as e:
            trading_api.logger.debug(f"Failed to collect live position snapshot before risk update: {e}")
        try:
            live_equity_baseline = await trading_api._resolve_live_equity_baseline(
                current_total_usd=market_total_usd,
                exchange_totals=exchange_total_map,
                live_snapshot=live_position_snapshot,
            )
            live_day_start_equity = trading_api._safe_float(
                live_equity_baseline.get("portfolio_total_usd"),
                default=0.0,
            )
            if live_day_start_equity > 0 and market_total_usd > 0:
                live_daily_total_pnl = float(market_total_usd) - float(live_day_start_equity)
        except Exception as e:
            trading_api.logger.warning(f"Failed to resolve live equity baseline: {e}")

        for label, usd_value in (live_position_snapshot.get("distribution") or {}).items():
            key = str(label or "").strip()
            val = float(usd_value or 0.0)
            if key and val > 0:
                distribution_map[key] = distribution_map.get(key, 0.0) + val

    if (
        (not is_paper_mode)
        and prev_equity > 0
        and risk_equity_input > 0
        and risk_equity_input < prev_equity * 0.6
        and (
            total_unpriced_assets > 0
            or balance_warning_present
            or binance_balance_issue
        )
    ):
        trading_api.logger.warning(
            f"Skip abnormal equity drop for risk update: prev={prev_equity:.4f}, "
            f"new={risk_equity_input:.4f}, unpriced_assets={total_unpriced_assets}, "
            f"balance_warning_present={balance_warning_present}"
        )
        risk_equity_input = prev_equity

    if (not is_paper_mode) and prev_equity > 0 and risk_equity_input > 0:
        delta_usd = risk_equity_input - prev_equity
        move_ratio = abs(delta_usd) / max(prev_equity, 1e-6)
        live_unrealized_abs = abs(float(live_position_snapshot.get("unrealized_pnl_usd") or 0.0))
        pnl_explained = live_unrealized_abs >= abs(delta_usd) * 0.45
        if move_ratio >= 0.55 and (not pnl_explained):
            trading_api.logger.warning(
                "Skip abnormal equity move likely transfer/cashflow: "
                f"prev={prev_equity:.4f}, new={risk_equity_input:.4f}, "
                f"delta={delta_usd:.4f}, live_unrealized={live_unrealized_abs:.4f}, "
                f"warnings={balance_warning_present}, unpriced={total_unpriced_assets}"
            )
            risk_equity_input = prev_equity

    if (
        (not is_paper_mode)
        and prev_equity > 0
        and risk_equity_input <= 0
        and (balance_warning_present or binance_balance_issue)
    ):
        trading_api.logger.warning(
            f"Skip zero/negative equity update for risk: prev={prev_equity:.4f}, "
            f"new={risk_equity_input:.4f}, balance_warning_present={balance_warning_present}"
        )
        risk_equity_input = prev_equity

    display_total_usd = risk_equity_input if (is_paper_mode and risk_equity_input > 0) else market_total_usd
    if (not is_paper_mode) and display_total_usd <= 0 and risk_equity_input > 0:
        display_total_usd = risk_equity_input

    trading_api.risk_manager.update_equity(
        risk_equity_input,
        day_start_equity=(
            live_day_start_equity
            if (not is_paper_mode and live_day_start_equity > 0)
            else None
        ),
        current_unrealized_pnl=(
            float(live_position_snapshot.get("unrealized_pnl_usd") or 0.0)
            if not is_paper_mode
            else float(trading_api.position_manager.get_total_pnl() or 0.0)
        ),
    )

    if is_paper_mode:
        asset_map: Dict[str, Dict[str, float]] = {}
        long_value_sum = 0.0
        for pos in trading_api.position_manager.get_all_positions():
            side_name = str(getattr(getattr(pos, "side", None), "value", getattr(pos, "side", "")) or "").lower()
            if side_name != "long":
                continue
            symbol = str(getattr(pos, "symbol", "") or "").upper()
            base = symbol.split("/")[0].strip() if "/" in symbol else symbol.strip()
            if not base:
                continue
            qty = abs(float(getattr(pos, "quantity", 0.0) or 0.0))
            px = float(getattr(pos, "current_price", 0.0) or 0.0)
            if px <= 0:
                px = float(getattr(pos, "entry_price", 0.0) or 0.0)
            if qty <= 0 or px <= 0:
                continue
            usd_val = qty * px
            long_value_sum += usd_val
            slot = asset_map.setdefault(base, {"total": 0.0, "usd_value": 0.0, "unit_usd": 0.0})
            slot["total"] += qty
            slot["usd_value"] += usd_val
            slot["unit_usd"] = px

        cash_usdt = max(0.0, float(display_total_usd) - float(long_value_sum))
        if cash_usdt > 0:
            slot = asset_map.setdefault("USDT", {"total": 0.0, "usd_value": 0.0, "unit_usd": 1.0})
            slot["total"] += cash_usdt
            slot["usd_value"] += cash_usdt
            slot["unit_usd"] = 1.0

        paper_balances: List[Dict[str, Any]] = []
        paper_distribution_map: Dict[str, float] = {}
        for ccy, row in asset_map.items():
            usd_val = float(row.get("usd_value", 0.0) or 0.0)
            if usd_val <= 0:
                continue
            total_val = float(row.get("total", 0.0) or 0.0)
            unit_usd = float(row.get("unit_usd", 0.0) or 0.0)
            paper_distribution_map[ccy] = paper_distribution_map.get(ccy, 0.0) + usd_val
            paper_balances.append(
                {
                    "currency": ccy,
                    "free": total_val,
                    "used": 0.0,
                    "total": total_val,
                    "usd_value": round(usd_val, 4),
                    "unit_usd": round(unit_usd, 8) if unit_usd > 0 else 0.0,
                    "valuation_source": "paper",
                }
            )
        paper_balances.sort(key=lambda item: item["usd_value"], reverse=True)
        paper_account = {
            "connected": True,
            "balances": paper_balances,
            "total_usd": round(float(display_total_usd), 2),
            "valuation_coverage": {
                "priced_assets": len([x for x in paper_balances if float(x.get("usd_value", 0.0) or 0.0) > 0]),
                "unpriced_assets": 0,
            },
        }
        distribution_map = paper_distribution_map

    await trading_api.account_snapshot_manager.record_snapshot(
        total_usd=display_total_usd,
        exchanges=results,
        mode=mode_name,
    )

    distribution_total = float(display_total_usd if is_paper_mode else market_total_usd)
    if not is_paper_mode:
        dist_sum = sum(float(v or 0.0) for v in distribution_map.values())
        if dist_sum > 0:
            distribution_total = float(dist_sum)
    distribution = [
        {
            "currency": ccy,
            "usd_value": round(val, 4),
            "weight": round((val / distribution_total), 6) if distribution_total > 0 else 0,
        }
        for ccy, val in sorted(distribution_map.items(), key=lambda x: x[1], reverse=True)
        if val > 0
    ]
    if not is_paper_mode and not live_position_snapshot:
        live_position_snapshot = await trading_api._collect_live_position_snapshot(force_refresh=False)
    risk_report = trading_api._apply_live_snapshot_to_risk_report(
        trading_api.risk_manager.get_risk_report(),
        live_position_snapshot,
        live_daily_total_pnl=live_daily_total_pnl,
        live_day_start_equity=live_day_start_equity,
    ) if not is_paper_mode else trading_api.risk_manager.get_risk_report()
    rule_prices = await trading_api._load_rule_prices()
    rule_eval = await trading_api.notification_manager.evaluate_rules(
        {
            "total_usd": display_total_usd,
            "prices": rule_prices,
            "risk_report": risk_report,
            "position_count": trading_api.position_manager.get_position_count(),
            "connected_exchanges": trading_api.exchange_manager.get_connected_exchanges(),
            "strategy_summary": trading_api.strategy_manager.get_dashboard_summary(signal_limit=10),
        }
    )

    return {
        "exchanges": results,
        "distribution": distribution,
        "total_usd_estimate": round(display_total_usd, 2),
        "market_total_usd_estimate": round(market_total_usd, 2),
        "binance_total_usd_estimate": round(trading_api._safe_float(exchange_total_map.get("binance"), default=0.0), 2),
        "paper_equity_estimate": round(paper_equity, 2) if is_paper_mode else None,
        "real_account_usd_estimate": round(market_total_usd, 2),
        "virtual_account_usd_estimate": round(paper_equity, 2) if is_paper_mode else None,
        "active_account_type": "paper" if is_paper_mode else "live",
        "active_account_usd_estimate": round(display_total_usd, 2),
        "inactive_account_usd_estimate": (
            round(market_total_usd, 2) if is_paper_mode else (round(paper_equity, 2) if paper_equity > 0 else None)
        ),
        "paper_account": paper_account,
        "risk_equity_input": round(risk_equity_input, 2),
        "live_day_start_equity": round(live_day_start_equity, 2) if not is_paper_mode else None,
        "live_daily_total_pnl_usd": round(live_daily_total_pnl, 2) if not is_paper_mode else None,
        "live_unrealized_pnl_usd": (
            round(float(live_position_snapshot.get("unrealized_pnl_usd") or 0.0), 4)
            if not is_paper_mode else 0.0
        ),
        "live_position_count": (
            int(live_position_snapshot.get("position_count") or 0)
            if not is_paper_mode else int(trading_api.position_manager.get_position_count() or 0)
        ),
        "unpriced_assets": total_unpriced_assets,
        "connected_exchanges": trading_api.exchange_manager.get_connected_exchanges(),
        "mode": mode_name,
        "risk_report": risk_report,
        "risk": {
            "trading_halted": risk_report.get("trading_halted", False),
            "risk_level": risk_report.get("risk_level", "low"),
        },
        "notifications": {
            "triggered_count": rule_eval.get("triggered_count", 0),
        },
    }


@router.get("/balances")
async def get_all_balances(force_refresh: bool = False):
    mode_name = str(trading_api.execution_engine.get_trading_mode() or "paper").strip().lower()
    if mode_name not in {"paper", "live"}:
        mode_name = "paper" if trading_api.execution_engine.is_paper_mode() else "live"

    if not force_refresh:
        fresh_payload = _clone_balance_payload(mode_name, max_age_sec=_BALANCE_RESPONSE_CACHE_TTL_SEC)
        if fresh_payload is not None:
            return fresh_payload

        in_flight = _BALANCE_RESPONSE_TASKS.get(mode_name)
        if in_flight and not in_flight.done():
            stale_payload = _clone_balance_payload(
                mode_name,
                max_age_sec=_BALANCE_RESPONSE_STALE_TTL_SEC,
                stale_note="资产快照刷新中，已先返回最近缓存。",
            )
            if stale_payload is not None:
                return stale_payload
            try:
                return copy.deepcopy(
                    await asyncio.wait_for(asyncio.shield(in_flight), timeout=_BALANCE_RESPONSE_TIMEOUT_SEC)
                )
            except asyncio.CancelledError:
                if not in_flight.cancelled():
                    raise
            except Exception:
                pass

    task = asyncio.create_task(_build_all_balances_payload())
    _BALANCE_RESPONSE_TASKS[mode_name] = task
    try:
        payload = await asyncio.wait_for(asyncio.shield(task), timeout=_BALANCE_RESPONSE_TIMEOUT_SEC)
        _BALANCE_RESPONSE_CACHE[mode_name] = {
            "ts": time.time(),
            "payload": copy.deepcopy(payload),
        }
        return payload
    except asyncio.TimeoutError:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        return _balance_response_fallback(mode_name, "资产快照刷新超时，正在后台重试。")
    except asyncio.CancelledError:
        if not task.cancelled():
            raise
        return _balance_response_fallback(mode_name, "资产快照刷新任务已取消，已返回最近缓存。")
    except Exception as exc:
        return _balance_response_fallback(mode_name, f"资产快照刷新失败: {exc}")
    finally:
        if _BALANCE_RESPONSE_TASKS.get(mode_name) is task:
            _BALANCE_RESPONSE_TASKS.pop(mode_name, None)


@router.get("/balances/history")
async def get_balance_history(
    hours: int = 24,
    exchange: str = "all",
    limit: int = 500,
    mode: Optional[str] = None,
):
    resolved_mode = str(mode or ("paper" if trading_api.execution_engine.is_paper_mode() else "live")).strip().lower()
    if resolved_mode not in {"paper", "live"}:
        resolved_mode = "paper" if trading_api.execution_engine.is_paper_mode() else "live"
    history = await trading_api.account_snapshot_manager.get_history(
        hours=hours,
        exchange=exchange,
        limit=limit,
        mode=resolved_mode,
    )
    return {
        "exchange": exchange,
        "hours": hours,
        "points": len(history),
        "mode": resolved_mode,
        "history": history,
    }
