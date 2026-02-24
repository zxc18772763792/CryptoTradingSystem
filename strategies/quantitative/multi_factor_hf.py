"""Config-driven multi-factor high-frequency strategy (5m oriented)."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import yaml
from loguru import logger

from core.factors_ts.registry import compute_factor
from core.strategies.strategy_base import Signal, SignalType, StrategyBase


def _clip01(x: float) -> float:
    return float(max(0.0, min(1.0, x)))


def _rolling_z_last(series: pd.Series, lookback: int = 120) -> float:
    s = pd.to_numeric(series, errors="coerce")
    if s.empty:
        return 0.0
    tail = s.dropna().tail(max(5, int(lookback)))
    if len(tail) < 5:
        v = float(s.iloc[-1]) if pd.notna(s.iloc[-1]) else 0.0
        return v
    mu = float(tail.mean())
    sd = float(tail.std(ddof=0) or 0.0)
    if sd <= 1e-12:
        return 0.0
    return float((tail.iloc[-1] - mu) / sd)


def _rolling_rank_last(series: pd.Series, lookback: int = 120) -> float:
    s = pd.to_numeric(series, errors="coerce")
    tail = s.dropna().tail(max(5, int(lookback)))
    if len(tail) < 5:
        return 0.0
    last = float(tail.iloc[-1])
    pct = float((tail <= last).mean())
    return pct * 2.0 - 1.0


def _apply_transform(series: pd.Series, transform: str) -> float:
    t = str(transform or "none").lower()
    if t == "none":
        v = pd.to_numeric(series.iloc[-1], errors="coerce")
        return 0.0 if pd.isna(v) else float(v)
    if t == "zscore":
        return _rolling_z_last(series)
    if t == "rank":
        return _rolling_rank_last(series)
    raise ValueError(f"Unknown transform: {transform}")


def _default_config() -> Dict[str, Any]:
    return {
        "timeframe": "5m",
        "factors": [
            {"name": "ema_slope", "params": {"fast": 8, "slow": 21}, "weight": 0.65, "transform": "zscore"},
            {"name": "zscore_price", "params": {"lookback": 30}, "weight": -0.35, "transform": "none"},
        ],
        "enter_th": 0.75,
        "exit_th": 0.25,
        "gates": {"max_rv": 0.012, "max_atr_pct": 0.02, "max_spread_proxy": 0.004, "min_volume_z": -1.5},
        "cooldown_bars": 2,
        "position_sizing": {"base_pct": 0.15, "max_pct": 0.4, "leverage": 2.0, "scale_by_score": True},
        "risk": {"stop_loss_atr_mult": 1.8, "take_profit_atr_mult": 2.6},
        "news_gate": {"enabled": False, "risk_scalar": 1.0, "threshold_bias": 0.0},
    }


def _load_yaml_strategy_config(path: Optional[str]) -> Dict[str, Any]:
    cfg = _default_config()
    p = Path(path) if path else Path("config/strategy_multi_factor_hf.yaml")
    if not p.exists():
        return cfg
    with p.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        return cfg
    # shallow merge with nested dict merge for commonly used sections
    for k, v in data.items():
        if isinstance(v, dict) and isinstance(cfg.get(k), dict):
            merged = dict(cfg[k])
            merged.update(v)
            cfg[k] = merged
        else:
            cfg[k] = v
    return cfg


class MultiFactorHFStrategy(StrategyBase):
    """5m-oriented multi-factor long/short strategy.

    Notes:
    - Emits BUY/SELL/CLOSE_* signals only.
    - Uses internal virtual position state for hysteresis/cooldown; does not touch live execution interfaces.
    """

    def __init__(self, name: str = "MultiFactorHFStrategy", params: Optional[Dict[str, Any]] = None):
        params = dict(params or {})
        cfg_path = params.pop("config_path", None)
        cfg = _load_yaml_strategy_config(cfg_path)
        # Allow direct param overrides while preserving nested sections.
        for k, v in params.items():
            if isinstance(v, dict) and isinstance(cfg.get(k), dict):
                merged = dict(cfg[k])
                merged.update(v)
                cfg[k] = merged
            else:
                cfg[k] = v
        super().__init__(name=name, params=cfg)
        self._cooldown_left = 0
        self._virtual_side: str = "flat"  # flat|long|short
        self._last_bar_key: Optional[str] = None

    def initialize(self) -> None:
        super().initialize()
        self._cooldown_left = 0
        self._virtual_side = "flat"
        self._last_bar_key = None

    def _bar_key(self, data: pd.DataFrame) -> str:
        if data.empty:
            return "empty"
        ts = pd.Timestamp(data.index[-1]).isoformat()
        return f"{ts}|{len(data)}"

    def _compute_factor_block(self, data: pd.DataFrame) -> Dict[str, Any]:
        factor_specs = list(self.params.get("factors") or [])
        factor_values: Dict[str, float] = {}
        factor_raw_last: Dict[str, float] = {}
        score_terms = []
        for spec in factor_specs:
            if not isinstance(spec, dict):
                continue
            fname = str(spec.get("name") or "").strip()
            fparams = dict(spec.get("params") or {})
            weight = float(spec.get("weight") or 0.0)
            transform = str(spec.get("transform") or "none")
            series = compute_factor(fname, data, params=fparams)
            raw_last = pd.to_numeric(series.iloc[-1], errors="coerce")
            transformed = _apply_transform(series, transform)
            key = fname if not fparams else f"{fname}({','.join(f'{k}={v}' for k, v in sorted(fparams.items()))})"
            factor_values[key] = float(transformed if np.isfinite(transformed) else 0.0)
            factor_raw_last[key] = float(raw_last) if pd.notna(raw_last) else 0.0
            score_terms.append((key, weight, factor_values[key]))
        score = float(sum(w * v for _, w, v in score_terms))
        return {
            "factor_values": factor_values,
            "factor_raw_last": factor_raw_last,
            "score_terms": [{"factor": k, "weight": float(w), "value": float(v), "contrib": float(w * v)} for k, w, v in score_terms],
            "score": score,
        }

    def _compute_gates(self, data: pd.DataFrame) -> Dict[str, Any]:
        gates_cfg = dict(self.params.get("gates") or {})
        rv = compute_factor("realized_vol", data, params={"lookback": 60})
        atr_pct = compute_factor("atr_pct", data, params={"lookback": 30})
        spread = compute_factor("spread_proxy", data, params={})
        vol_z = compute_factor("volume_z", data, params={"lookback": 60})
        latest = {
            "realized_vol": float(pd.to_numeric(rv.iloc[-1], errors="coerce") or 0.0),
            "atr_pct": float(pd.to_numeric(atr_pct.iloc[-1], errors="coerce") or 0.0),
            "spread_proxy": float(pd.to_numeric(spread.iloc[-1], errors="coerce") or 0.0),
            "volume_z": float(pd.to_numeric(vol_z.iloc[-1], errors="coerce") or 0.0),
        }
        checks = {
            "rv_ok": latest["realized_vol"] <= float(gates_cfg.get("max_rv", np.inf)),
            "atr_ok": latest["atr_pct"] <= float(gates_cfg.get("max_atr_pct", np.inf)),
            "spread_ok": latest["spread_proxy"] <= float(gates_cfg.get("max_spread_proxy", np.inf)),
            "volume_ok": latest["volume_z"] >= float(gates_cfg.get("min_volume_z", -np.inf)),
            "cooldown_ok": self._cooldown_left <= 0,
        }
        checks["all_ok"] = all(checks.values())
        return {"metrics": latest, "checks": checks}

    def _position_strength(self, score: float) -> float:
        enter_th = float(self.params.get("enter_th", 0.75))
        if enter_th <= 0:
            return 0.0
        return _clip01(abs(score) / max(enter_th, 1e-9))

    def _size_meta(self, score: float) -> Dict[str, Any]:
        sizing = dict(self.params.get("position_sizing") or {})
        base_pct = float(sizing.get("base_pct", 0.1))
        max_pct = float(sizing.get("max_pct", base_pct))
        leverage = float(sizing.get("leverage", 1.0))
        strength = self._position_strength(score)
        if sizing.get("scale_by_score", True):
            scale = float(np.tanh(max(0.0, abs(score))))
        else:
            scale = 1.0
        alloc_pct = min(max_pct, max(base_pct, base_pct + (max_pct - base_pct) * scale))
        return {"base_pct": base_pct, "max_pct": max_pct, "alloc_pct": alloc_pct, "leverage": leverage, "score_strength": strength}

    def _risk_levels(self, data: pd.DataFrame, side: SignalType) -> Dict[str, float]:
        close = float(pd.to_numeric(data["close"].iloc[-1], errors="coerce"))
        atr_pct_series = compute_factor("atr_pct", data, params={"lookback": 30})
        atr_pct = float(pd.to_numeric(atr_pct_series.iloc[-1], errors="coerce") or 0.0)
        risk_cfg = dict(self.params.get("risk") or {})
        sl_mult = float(risk_cfg.get("stop_loss_atr_mult", 1.5))
        tp_mult = float(risk_cfg.get("take_profit_atr_mult", 2.0))
        atr_abs = close * max(0.0, atr_pct)
        if side == SignalType.BUY:
            return {"stop_loss": close - sl_mult * atr_abs, "take_profit": close + tp_mult * atr_abs}
        if side == SignalType.SELL:
            return {"stop_loss": close + sl_mult * atr_abs, "take_profit": close - tp_mult * atr_abs}
        return {"stop_loss": 0.0, "take_profit": 0.0}

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        min_len = 120
        if data is None or data.empty or len(data) < min_len:
            return []

        bar_key = self._bar_key(data)
        if bar_key == self._last_bar_key:
            return []
        self._last_bar_key = bar_key

        work = data.copy()
        if "symbol" not in work.columns:
            work["symbol"] = self.get_param("symbol", "BTC/USDT")

        symbol = str(work["symbol"].iloc[-1]) if len(work) else "BTC/USDT"
        price = float(pd.to_numeric(work["close"].iloc[-1], errors="coerce"))
        if not np.isfinite(price) or price <= 0:
            return []

        factor_block = self._compute_factor_block(work)
        gates = self._compute_gates(work)
        score = float(factor_block["score"])
        enter_th = float(self.params.get("enter_th", 0.75))
        exit_th = float(self.params.get("exit_th", 0.25))
        cooldown_bars = int(self.params.get("cooldown_bars", 0))
        if exit_th > enter_th:
            exit_th = enter_th * 0.6

        signals: List[Signal] = []
        desired = "flat"
        if gates["checks"]["all_ok"]:
            if score > enter_th:
                desired = "long"
            elif score < -enter_th:
                desired = "short"
            elif abs(score) < exit_th:
                desired = "flat"
            else:
                desired = self._virtual_side
        else:
            desired = "flat"

        gate_status = {
            **gates["checks"],
            "metrics": gates["metrics"],
            "reason": "ok" if gates["checks"]["all_ok"] else " / ".join([k for k, v in gates["checks"].items() if k != "all_ok" and not v]) or "gate_block",
        }
        cost_estimate = {
            "spread_proxy": float(gates["metrics"]["spread_proxy"]),
            "realized_vol": float(gates["metrics"]["realized_vol"]),
            "atr_pct": float(gates["metrics"]["atr_pct"]),
        }
        size_meta = self._size_meta(score)
        metadata = {
            "factor_values": factor_block["factor_values"],
            "factor_raw_last": factor_block["factor_raw_last"],
            "score_terms": factor_block["score_terms"],
            "score": score,
            "gate_status": gate_status,
            "cost_estimate": cost_estimate,
            "position_sizing": size_meta,
        }

        if self._cooldown_left > 0:
            self._cooldown_left -= 1

        now = datetime.now()

        def _emit(sig_type: SignalType, extra: Optional[Dict[str, Any]] = None) -> None:
            md = dict(metadata)
            if extra:
                md.update(extra)
            risk_lv = self._risk_levels(work, sig_type) if sig_type in {SignalType.BUY, SignalType.SELL} else {"stop_loss": None, "take_profit": None}
            signals.append(
                Signal(
                    symbol=symbol,
                    signal_type=sig_type,
                    price=price,
                    timestamp=now,
                    strategy_name=self.name,
                    strength=_clip01(size_meta["score_strength"]),
                    quantity=None,
                    stop_loss=risk_lv.get("stop_loss"),
                    take_profit=risk_lv.get("take_profit"),
                    metadata=md,
                )
            )

        if desired != self._virtual_side:
            if self._virtual_side == "long":
                _emit(SignalType.CLOSE_LONG, {"transition": f"{self._virtual_side}->{desired}"})
            elif self._virtual_side == "short":
                _emit(SignalType.CLOSE_SHORT, {"transition": f"{self._virtual_side}->{desired}"})

            if desired == "long" and gates["checks"]["all_ok"]:
                _emit(SignalType.BUY, {"transition": f"{self._virtual_side}->{desired}"})
            elif desired == "short" and gates["checks"]["all_ok"]:
                _emit(SignalType.SELL, {"transition": f"{self._virtual_side}->{desired}"})

            if desired != self._virtual_side:
                self._cooldown_left = max(self._cooldown_left, cooldown_bars)
                self._virtual_side = desired
        else:
            # Optional hold marker in history only, no execution side effect.
            if gates["checks"]["all_ok"] and abs(score) >= exit_th:
                logger.debug(f"{self.name} hold {symbol}: score={score:.4f}, side={self._virtual_side}")

        for s in signals:
            self.add_signal_to_history(s)
        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "kline",
            "columns": ["open", "high", "low", "close", "volume"],
            "min_length": 180,
            "timeframe": str(self.params.get("timeframe", "5m")),
        }

