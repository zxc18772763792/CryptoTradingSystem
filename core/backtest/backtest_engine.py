"""Backtest engine with cost decomposition and optional funding support."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

import numpy as np
import pandas as pd
from loguru import logger

from core.backtest.cost_models import fee_rate as resolve_fee_rate
from core.backtest.cost_models import microstructure_proxies
from core.backtest.cost_models import slippage_rate as resolve_slippage_rate
from core.backtest.funding_provider import FundingRateProvider
from core.strategies import Signal, SignalType, StrategyBase


@dataclass
class BacktestConfig:
    """Backtest configuration.

    Legacy fields `commission_rate` and `slippage` are preserved for compatibility.
    New fields enable maker/taker fees and dynamic slippage.
    """

    initial_capital: float = 10000.0
    commission_rate: float = 0.001  # legacy flat fee rate (one-way)
    slippage: float = 0.0005  # legacy flat slippage rate (one-way)
    position_size_pct: float = 0.1
    max_positions: int = 5
    enable_shorting: bool = False
    leverage: float = 1.0

    fee_model: str = "flat"  # flat | maker_taker
    maker_fee: float = 0.0002
    taker_fee: float = 0.0005
    default_execution_role: str = "taker"  # maker | taker

    slippage_model: str = "flat"  # flat | dynamic
    dynamic_slip: Dict[str, float] = field(
        default_factory=lambda: {
            "min_slip": 0.00005,
            "k_atr": 0.15,
            "k_rv": 0.80,
            "k_spread": 0.50,
        }
    )

    include_funding: bool = False
    funding_source: str = "binance"
    funding_interval_hours: int = 8


@dataclass
class BacktestTrade:
    timestamp: datetime
    symbol: str
    side: str
    quantity: float
    price: float
    commission: float
    slippage: float  # absolute price delta vs reference price (legacy field)
    pnl: float = 0.0  # realized net pnl (legacy field)
    strategy: str = ""

    # New decomposition fields
    gross_pnl: float = 0.0
    fee: float = 0.0
    slippage_cost: float = 0.0
    funding_pnl: float = 0.0
    net_pnl: float = 0.0
    notional: float = 0.0
    execution_role: str = "taker"
    trade_stage: str = "unknown"  # open | close | funding


@dataclass
class BacktestResult:
    initial_capital: float
    final_capital: float
    total_return: float
    total_return_pct: float
    max_drawdown: float
    max_drawdown_pct: float
    sharpe_ratio: float
    win_rate: float
    total_trades: int
    winning_trades: int
    losing_trades: int
    profit_factor: float
    avg_trade_return: float
    trades: List[BacktestTrade] = field(default_factory=list)
    equity_curve: List[float] = field(default_factory=list)
    daily_returns: List[float] = field(default_factory=list)

    # New summary fields
    cost_breakdown: Dict[str, float] = field(default_factory=dict)
    turnover_notional: float = 0.0


class BacktestEngine:
    """Simple event-driven backtest engine for StrategyBase-compatible strategies."""

    def __init__(self, config: Optional[BacktestConfig] = None, funding_provider: Optional[FundingRateProvider] = None):
        self.config = config or BacktestConfig()
        self.funding_provider = funding_provider
        self._reset()

    def _reset(self) -> None:
        self._capital = float(self.config.initial_capital)
        self._equity = float(self.config.initial_capital)
        self._positions: Dict[str, Dict[str, Any]] = {}
        self._trades: List[BacktestTrade] = []
        self._equity_curve: List[float] = []
        self._daily_returns: List[float] = []
        self._turnover_notional: float = 0.0
        self._last_bar_ts: Optional[pd.Timestamp] = None

    async def run_backtest(
        self,
        strategy: StrategyBase,
        data: pd.DataFrame,
        symbol: str = "UNKNOWN",
        progress_callback: Optional[Callable] = None,
    ) -> BacktestResult:
        self._reset()
        data = self._prepare_backtest_data(data=data, symbol=symbol)

        if data is None or data.empty:
            logger.warning("No data for backtest")
            return self._create_empty_result()

        data = data.copy()
        if not isinstance(data.index, pd.DatetimeIndex):
            if "timestamp" in data.columns:
                data.index = pd.to_datetime(data["timestamp"])
            else:
                raise ValueError("Backtest data must use DatetimeIndex or have timestamp column")
        data = data.sort_index()

        logger.info(f"Starting backtest for {symbol} with {len(data)} bars")

        strategy.initialize()
        strategy.start()

        total_bars = len(data)
        for i in range(total_bars):
            current_data = data.iloc[: i + 1]
            row = current_data.iloc[-1]
            current_price = float(pd.to_numeric(row.get("close"), errors="coerce"))
            current_time = pd.Timestamp(current_data.index[-1]).to_pydatetime()
            if not np.isfinite(current_price) or current_price <= 0:
                self._equity_curve.append(self._equity)
                continue

            self._apply_funding_for_bar(current_data, current_time)
            self._update_positions(current_price, symbol)

            try:
                signals = strategy.generate_signals(current_data)
            except Exception as e:
                logger.error(f"Strategy generate_signals failed at {current_time}: {e}")
                signals = []

            for signal in signals:
                await self._execute_signal(signal, current_price, current_time, current_data)

            self._update_positions(current_price, symbol)
            self._equity_curve.append(self._equity)
            self._last_bar_ts = pd.Timestamp(current_data.index[-1])

            if progress_callback and i % 100 == 0:
                progress_callback(i / max(total_bars, 1))

        strategy.stop()
        return self._calculate_result()

    def _prepare_backtest_data(self, data: pd.DataFrame, symbol: str) -> pd.DataFrame:
        if data is None:
            return pd.DataFrame()
        out = data.copy()
        if not isinstance(out.index, pd.DatetimeIndex):
            out.index = pd.to_datetime(out.index)
        out = out.sort_index()
        if self.config.include_funding and self.funding_provider is not None:
            has_valid = False
            if "funding_rate" in out.columns:
                try:
                    has_valid = pd.to_numeric(out["funding_rate"], errors="coerce").notna().any()
                except Exception:
                    has_valid = False
            if not has_valid:
                try:
                    self.funding_provider.ensure_history(
                        symbol=symbol,
                        start_time=out.index.min().to_pydatetime(),
                        end_time=out.index.max().to_pydatetime(),
                        source=self.config.funding_source,
                        save=True,
                    )
                    out = self.funding_provider.attach_to_ohlcv_df(
                        out,
                        symbol=symbol,
                        column="funding_rate",
                        fill_forward=True,
                        overwrite=True,
                    )
                except Exception as e:
                    logger.warning(f"funding provider attach failed ({symbol}): {e}")
        return out

    def _fee_rate(self, signal: Optional[Signal] = None) -> float:
        role = str(
            (signal.metadata.get("execution_role") if signal and isinstance(signal.metadata, dict) else None)
            or self.config.default_execution_role
            or "taker"
        ).lower()
        return resolve_fee_rate(self.config, role=role)

    def _slippage_rate(self, window: Optional[pd.DataFrame]) -> float:
        return resolve_slippage_rate(self.config, window=window)

    def _microstructure_proxies(self, window: pd.DataFrame) -> Dict[str, float]:
        return microstructure_proxies(window)

    async def _execute_signal(
        self,
        signal: Signal,
        current_price: float,
        timestamp: datetime,
        window: Optional[pd.DataFrame] = None,
    ) -> None:
        if signal.signal_type == SignalType.BUY:
            await self._execute_buy(signal, current_price, timestamp, window)
        elif signal.signal_type == SignalType.SELL:
            await self._execute_sell(signal, current_price, timestamp, window)
        elif signal.signal_type == SignalType.CLOSE_LONG:
            await self._close_position(signal.symbol, current_price, timestamp, "long", window, signal)
        elif signal.signal_type == SignalType.CLOSE_SHORT:
            await self._close_position(signal.symbol, current_price, timestamp, "short", window, signal)

    async def _execute_buy(
        self,
        signal: Signal,
        current_price: float,
        timestamp: datetime,
        window: Optional[pd.DataFrame],
    ) -> None:
        symbol = signal.symbol
        if symbol in self._positions:
            return
        if len(self._positions) >= self.config.max_positions:
            return

        margin = self._capital * float(self.config.position_size_pct)
        margin = max(0.0, margin)
        if margin <= 0:
            return
        notional = margin * max(1.0, float(self.config.leverage))
        quantity = notional / current_price

        slip_rate = self._slippage_rate(window)
        exec_price = current_price * (1 + slip_rate)
        fee_rate = self._fee_rate(signal)
        fee = notional * fee_rate
        slippage_cost = abs(exec_price - current_price) * quantity

        if self._capital < margin + fee:
            return

        self._capital -= (margin + fee)
        self._turnover_notional += notional

        self._positions[symbol] = {
            "side": "long",
            "quantity": quantity,
            "entry_price": exec_price,
            "margin": margin,
            "notional_entry": notional,
            "timestamp": timestamp,
            "strategy": signal.strategy_name,
            "funding_pnl": 0.0,
            "last_funding_boundary": None,
        }

        self._trades.append(
            BacktestTrade(
                timestamp=timestamp,
                symbol=symbol,
                side="buy",
                quantity=quantity,
                price=exec_price,
                commission=fee,
                slippage=abs(exec_price - current_price),
                pnl=0.0,
                strategy=signal.strategy_name,
                gross_pnl=0.0,
                fee=fee,
                slippage_cost=slippage_cost,
                funding_pnl=0.0,
                net_pnl=-(fee + slippage_cost),
                notional=notional,
                execution_role=str(signal.metadata.get("execution_role") if isinstance(signal.metadata, dict) else self.config.default_execution_role),
                trade_stage="open",
            )
        )

    async def _execute_sell(
        self,
        signal: Signal,
        current_price: float,
        timestamp: datetime,
        window: Optional[pd.DataFrame],
    ) -> None:
        symbol = signal.symbol

        if symbol in self._positions and self._positions[symbol]["side"] == "long":
            await self._close_position(symbol, current_price, timestamp, "long", window, signal)
            return

        if not self.config.enable_shorting:
            return
        if symbol in self._positions:
            return
        if len(self._positions) >= self.config.max_positions:
            return

        margin = self._capital * float(self.config.position_size_pct)
        margin = max(0.0, margin)
        if margin <= 0:
            return
        notional = margin * max(1.0, float(self.config.leverage))
        quantity = notional / current_price

        slip_rate = self._slippage_rate(window)
        exec_price = current_price * (1 - slip_rate)
        fee_rate = self._fee_rate(signal)
        fee = notional * fee_rate
        slippage_cost = abs(exec_price - current_price) * quantity

        if self._capital < margin + fee:
            return

        self._capital -= (margin + fee)
        self._turnover_notional += notional

        self._positions[symbol] = {
            "side": "short",
            "quantity": quantity,
            "entry_price": exec_price,
            "margin": margin,
            "notional_entry": notional,
            "timestamp": timestamp,
            "strategy": signal.strategy_name,
            "funding_pnl": 0.0,
            "last_funding_boundary": None,
        }

        self._trades.append(
            BacktestTrade(
                timestamp=timestamp,
                symbol=symbol,
                side="sell",
                quantity=quantity,
                price=exec_price,
                commission=fee,
                slippage=abs(exec_price - current_price),
                pnl=0.0,
                strategy=signal.strategy_name,
                gross_pnl=0.0,
                fee=fee,
                slippage_cost=slippage_cost,
                funding_pnl=0.0,
                net_pnl=-(fee + slippage_cost),
                notional=notional,
                execution_role=str(signal.metadata.get("execution_role") if isinstance(signal.metadata, dict) else self.config.default_execution_role),
                trade_stage="open",
            )
        )

    async def _close_position(
        self,
        symbol: str,
        current_price: float,
        timestamp: datetime,
        side: str,
        window: Optional[pd.DataFrame],
        signal: Optional[Signal] = None,
    ) -> None:
        pos = self._positions.get(symbol)
        if not pos or pos.get("side") != side:
            return

        quantity = float(pos["quantity"])
        entry_price = float(pos["entry_price"])
        margin = float(pos.get("margin", 0.0))
        accrued_funding = float(pos.get("funding_pnl", 0.0))

        slip_rate = self._slippage_rate(window)
        if side == "long":
            exec_price = current_price * (1 - slip_rate)
            gross_pnl = (exec_price - entry_price) * quantity
            trade_side = "sell"
        else:
            exec_price = current_price * (1 + slip_rate)
            gross_pnl = (entry_price - exec_price) * quantity
            trade_side = "buy"

        notional = abs(exec_price * quantity)
        fee_rate = self._fee_rate(signal)
        fee = notional * fee_rate
        slippage_cost = abs(exec_price - current_price) * quantity
        net_pnl = gross_pnl + accrued_funding - fee - slippage_cost

        self._capital += margin + net_pnl
        self._turnover_notional += notional

        trade = BacktestTrade(
            timestamp=timestamp,
            symbol=symbol,
            side=trade_side,
            quantity=quantity,
            price=exec_price,
            commission=fee,
            slippage=abs(exec_price - current_price),
            pnl=net_pnl,
            strategy=str(pos.get("strategy") or ""),
            gross_pnl=gross_pnl,
            fee=fee,
            slippage_cost=slippage_cost,
            funding_pnl=accrued_funding,
            net_pnl=net_pnl,
            notional=notional,
            execution_role=str((signal.metadata.get("execution_role") if signal and isinstance(signal.metadata, dict) else self.config.default_execution_role) or "taker"),
            trade_stage="close",
        )
        self._trades.append(trade)
        del self._positions[symbol]

    def _update_positions(self, current_price: float, symbol: str) -> None:
        total_position_value = 0.0
        for sym, pos in self._positions.items():
            mark = current_price if sym == symbol else float(pos.get("mark_price", pos["entry_price"]))
            pos["mark_price"] = mark
            quantity = float(pos["quantity"])
            margin = float(pos.get("margin", 0.0))
            funding_pnl = float(pos.get("funding_pnl", 0.0))
            if pos["side"] == "long":
                unrealized = (mark - float(pos["entry_price"])) * quantity
            else:
                unrealized = (float(pos["entry_price"]) - mark) * quantity
            pos["unrealized_pnl"] = unrealized + funding_pnl
            pos["value"] = margin + unrealized + funding_pnl
            total_position_value += float(pos["value"])
        self._equity = self._capital + total_position_value

    def _funding_boundary(self, ts: datetime) -> Optional[pd.Timestamp]:
        if not self.config.include_funding:
            return None
        try:
            t = pd.Timestamp(ts)
        except Exception:
            return None
        interval = max(1, int(self.config.funding_interval_hours))
        if t.minute != 0 or t.second != 0:
            return None
        if t.hour % interval != 0:
            return None
        return t.floor("h")

    def _current_funding_rate(self, window: pd.DataFrame) -> float:
        if "funding_rate" not in window.columns:
            return 0.0
        v = pd.to_numeric(window["funding_rate"].iloc[-1], errors="coerce")
        if pd.isna(v):
            return 0.0
        return float(v)

    def _apply_funding_for_bar(self, window: pd.DataFrame, timestamp: datetime) -> None:
        if not self.config.include_funding or not self._positions:
            return
        boundary = self._funding_boundary(timestamp)
        if boundary is None:
            return
        rate = self._current_funding_rate(window)
        if abs(rate) <= 0:
            return
        for symbol, pos in self._positions.items():
            if pos.get("last_funding_boundary") == boundary:
                continue
            mark_price = float(pos.get("mark_price", pos.get("entry_price", 0.0)) or 0.0)
            quantity = float(pos.get("quantity", 0.0) or 0.0)
            notional = abs(mark_price * quantity)
            if notional <= 0:
                continue
            # Positive funding: longs pay shorts receive.
            # Funding is accumulated in pos["funding_pnl"] and only realized when the
            # position is closed (added to net_pnl in _close_position). Do NOT credit
            # _capital here to avoid double-counting with the close settlement.
            funding_cash = -rate * notional if pos.get("side") == "long" else rate * notional
            pos["funding_pnl"] = float(pos.get("funding_pnl", 0.0)) + funding_cash
            pos["last_funding_boundary"] = boundary
            self._trades.append(
                BacktestTrade(
                    timestamp=boundary.to_pydatetime(),
                    symbol=symbol,
                    side="funding",
                    quantity=quantity,
                    price=mark_price,
                    commission=0.0,
                    slippage=0.0,
                    pnl=funding_cash,
                    strategy=str(pos.get("strategy") or ""),
                    gross_pnl=0.0,
                    fee=0.0,
                    slippage_cost=0.0,
                    funding_pnl=funding_cash,
                    net_pnl=funding_cash,
                    notional=notional,
                    execution_role="funding",
                    trade_stage="funding",
                )
            )

    def _calculate_result(self) -> BacktestResult:
        initial_capital = float(self.config.initial_capital)
        final_capital = float(self._equity)
        total_return = final_capital - initial_capital
        total_return_pct = (total_return / initial_capital) if initial_capital else 0.0

        equity_array = np.asarray(self._equity_curve or [initial_capital], dtype=float)
        if equity_array.size == 0:
            equity_array = np.asarray([initial_capital], dtype=float)
        peak = np.maximum.accumulate(equity_array)
        with np.errstate(divide="ignore", invalid="ignore"):
            drawdown = (peak - equity_array) / np.where(peak == 0, np.nan, peak)
        drawdown = np.nan_to_num(drawdown, nan=0.0, posinf=0.0, neginf=0.0)
        max_drawdown = float(np.max(drawdown)) if drawdown.size else 0.0
        max_drawdown_pct = max_drawdown * 100.0

        close_trades = [t for t in self._trades if t.trade_stage == "close"]
        winning_trades = [t for t in close_trades if t.net_pnl > 0]
        losing_trades = [t for t in close_trades if t.net_pnl < 0]
        total_wins = float(sum(t.net_pnl for t in winning_trades))
        total_losses = abs(float(sum(t.net_pnl for t in losing_trades)))
        profit_factor = total_wins / total_losses if total_losses > 0 else float("inf")
        win_rate = len(winning_trades) / len(close_trades) if close_trades else 0.0

        if len(equity_array) > 1:
            base = np.where(equity_array[:-1] == 0, np.nan, equity_array[:-1])
            returns = np.diff(equity_array) / base
            returns = returns[np.isfinite(returns)]
            sharpe = float(np.mean(returns) / np.std(returns) * np.sqrt(365)) if returns.size > 1 and float(np.std(returns)) > 0 else 0.0
        else:
            sharpe = 0.0

        avg_trade_return = float(np.mean([t.net_pnl for t in close_trades])) if close_trades else 0.0
        cost_breakdown = {
            "gross_pnl": float(sum(float(t.gross_pnl or 0.0) for t in self._trades)),
            "fee": float(sum(float(t.fee or t.commission or 0.0) for t in self._trades)),
            "slippage_cost": float(sum(float(t.slippage_cost or 0.0) for t in self._trades)),
            "funding_pnl": float(sum(float(t.funding_pnl or 0.0) for t in self._trades if t.trade_stage == "funding")),
            "net_pnl": float(sum(float(t.net_pnl if t.trade_stage != "funding" else 0.0) for t in self._trades if t.trade_stage == "close")),
        }
        # Include funding on close if accrued there; compute realized total more robustly:
        cost_breakdown["realized_total"] = float(sum(float(t.pnl or 0.0) for t in self._trades if t.trade_stage in {"close", "funding"}))

        return BacktestResult(
            initial_capital=initial_capital,
            final_capital=final_capital,
            total_return=total_return,
            total_return_pct=total_return_pct,
            max_drawdown=max_drawdown,
            max_drawdown_pct=max_drawdown_pct,
            sharpe_ratio=sharpe,
            win_rate=win_rate,
            total_trades=len(close_trades),
            winning_trades=len(winning_trades),
            losing_trades=len(losing_trades),
            profit_factor=profit_factor,
            avg_trade_return=avg_trade_return,
            trades=self._trades,
            equity_curve=self._equity_curve,
            daily_returns=self._daily_returns,
            cost_breakdown=cost_breakdown,
            turnover_notional=self._turnover_notional,
        )

    def _create_empty_result(self) -> BacktestResult:
        return BacktestResult(
            initial_capital=float(self.config.initial_capital),
            final_capital=float(self.config.initial_capital),
            total_return=0.0,
            total_return_pct=0.0,
            max_drawdown=0.0,
            max_drawdown_pct=0.0,
            sharpe_ratio=0.0,
            win_rate=0.0,
            total_trades=0,
            winning_trades=0,
            losing_trades=0,
            profit_factor=0.0,
            avg_trade_return=0.0,
            cost_breakdown={"gross_pnl": 0.0, "fee": 0.0, "slippage_cost": 0.0, "funding_pnl": 0.0, "net_pnl": 0.0, "realized_total": 0.0},
            turnover_notional=0.0,
        )


backtest_engine = BacktestEngine()
