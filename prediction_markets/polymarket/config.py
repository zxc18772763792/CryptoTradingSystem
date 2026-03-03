from __future__ import annotations

import copy
import os
from pathlib import Path
from typing import Any, Dict

import yaml

DEFAULT_CONFIG_PATH = Path("config/polymarket.yaml")


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "on", "y"}


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name) or default)
    except Exception:
        return float(default)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name) or default)
    except Exception:
        return int(default)


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = copy.deepcopy(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = copy.deepcopy(value)
    return out


def load_polymarket_config(path: Path | str | None = None) -> Dict[str, Any]:
    cfg_path = Path(path or DEFAULT_CONFIG_PATH)
    data: Dict[str, Any] = {}
    if cfg_path.exists():
        data = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    defaults = data.get("defaults") or {}
    defaults["enabled"] = _env_bool("PM_ENABLE", bool(defaults.get("enabled", False)))
    defaults.setdefault("gamma", {})["refresh_interval_sec"] = _env_int(
        "PM_GAMMA_REFRESH_SEC",
        int((defaults.get("gamma") or {}).get("refresh_interval_sec", 1200)),
    )
    defaults.setdefault("worker", {})["quote_loop_sec"] = _env_int(
        "PM_QUOTE_LOOP_SEC",
        int((defaults.get("worker") or {}).get("quote_loop_sec", 10)),
    )
    defaults.setdefault("storage", {})["raw_quote_retention_days"] = _env_int(
        "PM_RAW_RETENTION_DAYS",
        int((defaults.get("storage") or {}).get("raw_quote_retention_days", 14)),
    )
    defaults.setdefault("signal", {})["enable"] = _env_bool(
        "PM_FEATURES_ENABLE",
        bool((defaults.get("signal") or {}).get("enable", False)),
    )
    defaults.setdefault("signal", {})["alpha_weight"] = _env_float(
        "PM_SIGNAL_ALPHA",
        float((defaults.get("signal") or {}).get("alpha_weight", 0.35)),
    )
    defaults.setdefault("signal", {})["global_risk_penalty"] = _env_float(
        "PM_RISK_BETA",
        float((defaults.get("signal") or {}).get("global_risk_penalty", 0.40)),
    )
    defaults.setdefault("trading", {})["enabled"] = _env_bool(
        "POLY_ENABLE_TRADING",
        bool((defaults.get("trading") or {}).get("enabled", False)),
    )
    defaults.setdefault("trading", {})["require_approval"] = _env_bool(
        "POLY_REQUIRE_APPROVAL",
        bool((defaults.get("trading") or {}).get("require_approval", True)),
    )
    defaults.setdefault("clob", {})["max_spread"] = _env_float(
        "PM_MAX_SPREAD",
        float((defaults.get("clob") or {}).get("max_spread", 0.10) or 0.10),
    )
    categories = data.get("categories") or {}
    liquidity_override = _env_float("PM_LIQUIDITY_MIN", 0.0)
    spread_override = _env_float("PM_MAX_SPREAD", 0.0)
    if liquidity_override > 0:
        for item in categories.values():
            item["min_liquidity"] = liquidity_override
    if spread_override > 0:
        for item in categories.values():
            item["max_spread"] = spread_override
    merged = _deep_merge(data, {"defaults": defaults, "categories": categories})
    merged["config_path"] = str(cfg_path)
    return merged
