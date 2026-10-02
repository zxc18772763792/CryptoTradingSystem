"""Deterministic verification for the exit-logic overhaul.

This script does not pretend to create 30 days of new live fills. It verifies
the code paths that can be checked offline and reports whether post-change live
samples are available for maker/fallback statistics.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.backtest.backtest_engine import BacktestConfig, BacktestEngine, BacktestResult
from core.strategies import Signal, SignalType, StrategyBase
from core.trading.execution_engine import ExecutionEngine
from scripts.audit_exit_reasons import audit_live


def _ts(data: pd.DataFrame) -> datetime:
    stamp = pd.Timestamp(data.index[-1])
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize("UTC")
    return stamp.to_pydatetime()


def _ohlcv(rows: int = 32) -> pd.DataFrame:
    idx = pd.date_range("2026-01-01", periods=rows, freq="h", tz="UTC")
    out = pd.DataFrame(
        {
            "open": [100.0] * rows,
            "high": [101.0] * rows,
            "low": [99.0] * rows,
            "close": [100.0] * rows,
            "volume": [1000.0] * rows,
            "symbol": ["BTC/USDT"] * rows,
        },
        index=idx,
    )
    # Protective exits after scripted entries.
    out.iloc[3, out.columns.get_loc("low")] = 94.0       # stop_loss
    out.iloc[7, out.columns.get_loc("high")] = 107.0     # take_profit
    out.iloc[11, out.columns.get_loc("high")] = 110.0    # trailing anchor
    out.iloc[11, out.columns.get_loc("low")] = 104.0     # trailing stop
    out.iloc[11, out.columns.get_loc("close")] = 105.0
    out.iloc[19, out.columns.get_loc("close")] = 104.0   # signal close profit
    out.iloc[19, out.columns.get_loc("high")] = 105.0
    out.iloc[19, out.columns.get_loc("low")] = 103.0
    return out


class FrequentEntryStrategy(StrategyBase):
    def __init__(self) -> None:
        super().__init__("verify_frequent_entry", {"use_atr_stops": False})
        self._emitted: set[int] = set()

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        bar_len = len(data)
        if bar_len < 2 or bar_len % 4 != 2 or bar_len in self._emitted:
            return []
        self._emitted.add(bar_len)
        price = float(data["close"].iloc[-1])
        return [
            Signal(
                symbol="BTC/USDT",
                signal_type=SignalType.BUY,
                price=price,
                timestamp=_ts(data),
                strategy_name=self.name,
                stop_loss=price * 0.99,
                take_profit=price * 1.03,
                metadata={"use_atr_stops": False},
            )
        ]

    def get_required_data(self) -> Dict[str, Any]:
        return {"type": "kline", "min_length": 2}


class ScriptedExitStrategy(StrategyBase):
    def __init__(self) -> None:
        super().__init__("verify_scripted_exits", {"use_atr_stops": False})
        self._emitted: set[str] = set()

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        bar_len = len(data)
        scripts: Dict[int, Dict[str, Any]] = {
            2: {"key": "stop", "stop_loss": 95.0, "take_profit": 120.0},
            6: {"key": "take", "stop_loss": 90.0, "take_profit": 106.0},
            10: {"key": "trail", "metadata": {"trailing_stop_pct": 0.05}},
            14: {"key": "time", "metadata": {"time_stop_enabled": True, "max_bars_in_trade": 2}},
            18: {"key": "signal", "metadata": {"scripted_signal_close_at_len": 20}},
        }
        spec = scripts.get(bar_len)
        if not spec or spec["key"] in self._emitted:
            return []
        self._emitted.add(str(spec["key"]))
        price = float(data["close"].iloc[-1])
        metadata = {"use_atr_stops": False}
        metadata.update(dict(spec.get("metadata") or {}))
        return [
            Signal(
                symbol="BTC/USDT",
                signal_type=SignalType.BUY,
                price=price,
                timestamp=_ts(data),
                strategy_name=self.name,
                stop_loss=spec.get("stop_loss"),
                take_profit=spec.get("take_profit"),
                metadata=metadata,
            )
        ]

    def check_exit(self, data: pd.DataFrame, position: Any) -> Optional[Signal]:
        metadata = dict(getattr(position, "metadata", {}) or {})
        close_at = metadata.get("scripted_signal_close_at_len")
        if close_at is None or len(data) < int(close_at):
            return None
        return Signal(
            symbol=str(getattr(position, "symbol", "BTC/USDT")),
            signal_type=SignalType.CLOSE_LONG,
            price=float(data["close"].iloc[-1]),
            timestamp=_ts(data),
            strategy_name=self.name,
            metadata={"close_reason": "signal_close", "close_only": True},
        )

    def get_required_data(self) -> Dict[str, Any]:
        return {"type": "kline", "min_length": 2}


class ATRProbeStrategy(StrategyBase):
    def __init__(self) -> None:
        super().__init__("verify_atr_probe", {"use_atr_stops": True, "atr_stop_loss_mult": 1.5})

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        price = float(data["close"].iloc[-1])
        return [
            Signal(
                symbol="BTC/USDT",
                signal_type=SignalType.BUY,
                price=price,
                timestamp=_ts(data),
                strategy_name=self.name,
                stop_loss=price * 0.98,
                take_profit=price * 1.04,
            )
        ]

    def get_required_data(self) -> Dict[str, Any]:
        return {"type": "kline", "min_length": 14}


def _config(**overrides: Any) -> BacktestConfig:
    values = {
        "initial_capital": 10000.0,
        "commission_rate": 0.0,
        "slippage": 0.0,
        "position_size_pct": 0.1,
        "max_positions": 1,
        "enable_shorting": True,
    }
    values.update(overrides)
    return BacktestConfig(**values)


def _run(strategy: StrategyBase, data: pd.DataFrame, config: BacktestConfig) -> BacktestResult:
    return asyncio.run(BacktestEngine(config).run_backtest(strategy, data, symbol="BTC/USDT"))


def _close_reason_counts(result: BacktestResult) -> Counter:
    return Counter(
        str(trade.exit_reason or "unknown")
        for trade in result.trades
        if trade.trade_stage == "close"
    )


def _load_local_btc_1h(days: int = 30) -> pd.DataFrame:
    path = REPO_ROOT / "data/historical/binance/BTC_USDT/1h.parquet"
    if not path.exists():
        return pd.DataFrame()
    data = pd.read_parquet(path)
    if not isinstance(data.index, pd.DatetimeIndex):
        if "timestamp" in data.columns:
            data.index = pd.to_datetime(data["timestamp"])
        elif "open_time" in data.columns:
            data.index = pd.to_datetime(data["open_time"])
    if not isinstance(data.index, pd.DatetimeIndex):
        return pd.DataFrame()
    data = data.sort_index()
    cutoff = data.index.max() - pd.Timedelta(days=days)
    return data[data.index >= cutoff].copy()


def _pct(part: float, total: float) -> str:
    if total <= 0:
        return "0.0%"
    return f"{(part / total) * 100.0:.1f}%"


def build_report() -> str:
    data = _ohlcv()

    legacy = _run(
        FrequentEntryStrategy(),
        data,
        _config(
            honor_signal_stop_loss=False,
            honor_signal_take_profit=False,
            enable_protective_check=False,
            enable_trailing_stop=False,
            enable_time_stop=False,
        ),
    )
    protected = _run(FrequentEntryStrategy(), data, _config())
    trade_delta = protected.total_trades - legacy.total_trades
    trade_delta_pct = (trade_delta / max(legacy.total_trades, 1)) * 100.0

    scripted = _run(ScriptedExitStrategy(), data, _config())
    reasons = _close_reason_counts(scripted)
    closes = sum(reasons.values())
    time_stop_net_pnls = [
        float(trade.net_pnl or 0.0)
        for trade in scripted.trades
        if trade.trade_stage == "close" and trade.exit_reason == "time_stop"
    ]
    time_stop_avg_net_pnl = sum(time_stop_net_pnls) / max(len(time_stop_net_pnls), 1)

    profit_probe = SimpleNamespace(
        strategy="unit_test_strategy",
        metadata={"source": "strategy", "timeframe": "1h", "profit_management_atr_pct": 0.01},
    )
    profit_metadata = ExecutionEngine()._effective_profit_management_metadata(profit_probe)
    profit_defaults_pass = (
        abs(float(profit_metadata.get("profit_protect_trigger_pct") or 0.0) - 0.01) <= 1e-9
        and abs(float(profit_metadata.get("profit_protect_lock_pct") or 0.0) - 0.001) <= 1e-9
        and abs(float(profit_metadata.get("partial_take_profit_trigger_pct") or 0.0) - 0.015) <= 1e-9
        and abs(float(profit_metadata.get("post_partial_trailing_activation_pct") or 0.0) - 0.02) <= 1e-9
    )

    btc_1h = _load_local_btc_1h(days=30)
    bollinger_reasons: Counter = Counter()
    bollinger_win_rate: Optional[float] = None
    if not btc_1h.empty:
        from strategies.technical.bollinger_strategy import BollingerBandsStrategy

        bollinger = _run(
            BollingerBandsStrategy("BollingerBandsStrategy", {"period": 20, "num_std": 2.0}),
            btc_1h,
            _config(),
        )
        bollinger_reasons = _close_reason_counts(bollinger)
        bollinger_win_rate = float(bollinger.win_rate)
    protective_reasons = {"stop_loss", "take_profit", "trailing_stop", "time_stop", "signal_reversal"}
    bollinger_closes = sum(bollinger_reasons.values())
    bollinger_active_closes = sum(
        count for reason, count in bollinger_reasons.items() if reason not in protective_reasons
    )
    bollinger_active_share = bollinger_active_closes / max(bollinger_closes, 1)

    multi_strategy_rows: List[Dict[str, Any]] = []
    multi_strategy_reasons: Counter = Counter()
    if not btc_1h.empty:
        from strategies.quantitative import MeanReversionStrategy, MomentumStrategy
        from strategies.technical import MACDStrategy, MAStrategy, RSIStrategy, VWAPReversionStrategy

        strategy_specs = [
            ("MAStrategy", MAStrategy("verify_ma", {"use_atr_stops": True})),
            ("RSIStrategy", RSIStrategy("verify_rsi", {"use_atr_stops": True})),
            ("MACDStrategy", MACDStrategy("verify_macd", {"use_atr_stops": True})),
            (
                "BollingerBandsStrategy",
                BollingerBandsStrategy("verify_bollinger", {"period": 20, "num_std": 2.0, "use_atr_stops": True}),
            ),
            ("VWAPReversionStrategy", VWAPReversionStrategy("verify_vwap", {"use_atr_stops": True})),
            ("MeanReversionStrategy", MeanReversionStrategy("verify_mean_reversion", {"use_atr_stops": True})),
            ("MomentumStrategy", MomentumStrategy("verify_momentum", {"use_atr_stops": True})),
        ]
        for strategy_name, strategy in strategy_specs:
            result = _run(strategy, btc_1h, _config())
            strategy_reasons = _close_reason_counts(result)
            strategy_closes = sum(strategy_reasons.values())
            multi_strategy_reasons.update(strategy_reasons)
            multi_strategy_rows.append(
                {
                    "strategy": strategy_name,
                    "closes": strategy_closes,
                    "stop_loss": int(strategy_reasons.get("stop_loss", 0)),
                    "stop_loss_share": (
                        float(strategy_reasons.get("stop_loss", 0)) / max(strategy_closes, 1)
                    ),
                }
            )
    multi_closes = sum(multi_strategy_reasons.values())
    multi_stop_loss_share = float(multi_strategy_reasons.get("stop_loss", 0)) / max(multi_closes, 1)

    atr_data = _ohlcv(rows=20)
    atr_signal = ATRProbeStrategy().generate_signals(atr_data)[0]
    atr_pct = float(atr_signal.metadata.get("atr_pct") or 0.0)
    sl_ratio = ((float(atr_signal.price) - float(atr_signal.stop_loss)) / float(atr_signal.price)) / max(atr_pct, 1e-12)

    live = audit_live(days=30)
    known_close_modes = sum(
        count for mode, count in live.close_order_modes.items() if str(mode) != "unknown"
    )

    lines = [
        f"# Exit Logic Verification ({datetime.now(timezone.utc).date().isoformat()})",
        "",
        "## Phase 1 Backtest Alignment",
        "",
        "| Scenario | Trades | Close Trades |",
        "|---|---:|---:|",
        f"| Legacy protective exits disabled | {legacy.total_trades} | {sum(1 for t in legacy.trades if t.trade_stage == 'close')} |",
        f"| Current protective exits enabled | {protected.total_trades} | {sum(1 for t in protected.trades if t.trade_stage == 'close')} |",
        "",
        f"- Trade count delta: `{trade_delta}` (`{trade_delta_pct:.1f}%`)",
        f"- Acceptance `>=30%`: `{'PASS' if trade_delta_pct >= 30 else 'FAIL'}`",
        "",
        "## Phase 1/2/3/4 Exit Reason Coverage",
        "",
        "| Exit Reason | Trades | Share |",
        "|---|---:|---:|",
    ]
    for reason, count in sorted(reasons.items()):
        lines.append(f"| `{reason}` | {count} | {_pct(count, closes)} |")
    required = {"stop_loss", "take_profit", "trailing_stop", "time_stop", "signal_close"}
    missing = sorted(required - set(reasons))
    lines.extend(
        [
            "",
            f"- Required reasons present: `{'PASS' if not missing else 'FAIL'}`",
            f"- Missing reasons: `{', '.join(missing) if missing else 'NONE'}`",
            f"- `trailing_stop` share: `{_pct(reasons.get('trailing_stop', 0), closes)}`",
            f"- `signal_close` share: `{_pct(reasons.get('signal_close', 0), closes)}`",
            f"- `time_stop` average net PnL: `{time_stop_avg_net_pnl:.6f}`",
            "",
            "## Phase 2 Profit Management Defaults",
            "",
            "| Metric | Value |",
            "|---|---:|",
            f"| `profit_management_atr_pct` | {float(profit_metadata.get('profit_management_atr_pct') or 0.0):.4f} |",
            f"| `profit_protect_trigger_pct` | {float(profit_metadata.get('profit_protect_trigger_pct') or 0.0):.4f} |",
            f"| `profit_protect_lock_pct` | {float(profit_metadata.get('profit_protect_lock_pct') or 0.0):.4f} |",
            f"| `partial_take_profit_trigger_pct` | {float(profit_metadata.get('partial_take_profit_trigger_pct') or 0.0):.4f} |",
            f"| `post_partial_trailing_activation_pct` | {float(profit_metadata.get('post_partial_trailing_activation_pct') or 0.0):.4f} |",
            "",
            f"- Acceptance 1x/1.5x/2x ATR defaults: `{'PASS' if profit_defaults_pass else 'FAIL'}`",
            "",
            "## Local 30-Day BTC/USDT 1h Bollinger Backtest",
            "",
            "| Exit Reason | Trades | Share |",
            "|---|---:|---:|",
        ]
    )
    if bollinger_reasons:
        for reason, count in sorted(bollinger_reasons.items()):
            lines.append(f"| `{reason}` | {count} | {_pct(count, bollinger_closes)} |")
    else:
        lines.append("| `NO_LOCAL_DATA` | 0 | 0.0% |")
    lines.extend(
        [
            "",
            f"- Active strategy close share: `{_pct(bollinger_active_closes, bollinger_closes)}`",
            f"- Win rate: `{0.0 if bollinger_win_rate is None else bollinger_win_rate:.4f}`",
            f"- Acceptance active close share `>=20%`: `{'UNVERIFIED_NO_LOCAL_DATA' if btc_1h.empty else ('PASS' if bollinger_active_share >= 0.20 else 'FAIL')}`",
            "",
            "## Local 30-Day Multi-Strategy SL Share",
            "",
            "| Strategy | Closes | Stop Loss | Stop Loss Share |",
            "|---|---:|---:|---:|",
        ]
    )
    if multi_strategy_rows:
        for row in multi_strategy_rows:
            lines.append(
                f"| `{row['strategy']}` | {row['closes']} | {row['stop_loss']} | {_pct(row['stop_loss'], row['closes'])} |"
            )
    else:
        lines.append("| `NO_LOCAL_DATA` | 0 | 0 | 0.0% |")
    lines.extend(
        [
            "",
            f"- Aggregate stop_loss share: `{_pct(multi_strategy_reasons.get('stop_loss', 0), multi_closes)}`",
            f"- Acceptance stop_loss share `<=30%`: `{'UNVERIFIED_NO_LOCAL_DATA' if btc_1h.empty else ('PASS' if multi_stop_loss_share <= 0.30 and multi_closes > 0 else 'FAIL')}`",
            "",
            "## Phase 7 ATR Protection",
            "",
            "| Metric | Value |",
            "|---|---:|",
            f"| `atr_pct` | {atr_pct:.8f} |",
            f"| stop-loss distance / ATR | {sl_ratio:.4f} |",
            f"| metadata has `atr_protection_applied` | {bool(atr_signal.metadata.get('atr_protection_applied'))} |",
            "",
            f"- Acceptance SL distance about `1.5x ATR`: `{'PASS' if abs(sl_ratio - 1.5) <= 0.05 else 'FAIL'}`",
            "",
            "## Phase 5 Live Maker/Fallback Audit",
            "",
            "| Mode | Journal Closes | Share |",
            "|---|---:|---:|",
        ]
    )
    for mode, count in sorted(live.close_order_modes.items()):
        lines.append(f"| `{mode}` | {count} | {_pct(count, live.closes)} |")
    if not live.close_order_modes:
        lines.append("| `NONE` | 0 | 0.0% |")
    lines.extend(
        [
            "",
            f"- Known post-change `close_order_mode` samples: `{known_close_modes}`",
            "- LIMIT share and average close slippage require post-deploy live close samples; this report verifies instrumentation and keeps the KPI explicit.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify exit logic overhaul behavior.")
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    report = build_report()
    print(report)
    if args.output is not None:
        output = args.output
        if not output.is_absolute():
            output = REPO_ROOT / output
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(report, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
