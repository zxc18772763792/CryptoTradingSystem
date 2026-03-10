"""
性能分析器模块
"""
from datetime import datetime
from typing import Optional, List, Dict, Any
import pandas as pd
import numpy as np
from dataclasses import dataclass

from core.backtest.backtest_engine import BacktestResult, BacktestTrade


@dataclass
class PerformanceMetrics:
    """性能指标"""
    # 收益指标
    total_return: float
    annual_return: float
    monthly_return: float
    daily_return: float

    # 风险指标
    volatility: float
    max_drawdown: float
    var_95: float
    cvar_95: float

    # 风险调整收益
    sharpe_ratio: float
    sortino_ratio: float
    calmar_ratio: float

    # 交易指标
    win_rate: float
    profit_factor: float
    avg_trade: float
    avg_winning_trade: float
    avg_losing_trade: float
    max_consecutive_wins: int
    max_consecutive_losses: int

    # 其他
    total_trades: int
    trading_days: int
    avg_trades_per_day: float
    gross_pnl: float = 0.0
    fee_cost: float = 0.0
    slippage_cost: float = 0.0
    funding_pnl: float = 0.0
    cost_to_gross_ratio: float = 0.0
    turnover_notional: float = 0.0


class PerformanceAnalyzer:
    """性能分析器"""

    def __init__(self, risk_free_rate: float = 0.02):
        self.risk_free_rate = risk_free_rate

    def analyze(self, result: BacktestResult) -> PerformanceMetrics:
        """
        分析回测结果

        Args:
            result: 回测结果

        Returns:
            性能指标
        """
        equity_curve = np.array(result.equity_curve)
        trades = result.trades

        # 计算收益率
        returns = self._calculate_returns(equity_curve)

        # 计算各项指标
        total_return = result.total_return_pct
        annual_return = self._annualize_return(total_return, len(equity_curve))
        monthly_return = annual_return / 12
        daily_return = self._calculate_daily_return(returns)

        # 风险指标
        volatility = self._calculate_volatility(returns)
        max_drawdown = result.max_drawdown_pct
        var_95 = self._calculate_var(returns, 0.95)
        cvar_95 = self._calculate_cvar(returns, 0.95)

        # 风险调整收益
        sharpe = self._calculate_sharpe(returns)
        sortino = self._calculate_sortino(returns)
        calmar = self._calculate_calmar(annual_return, max_drawdown)

        # 交易指标
        trade_stats = self._analyze_trades(trades)
        cost = dict(result.cost_breakdown or {})
        gross_pnl = float(cost.get("gross_pnl", result.total_return))
        fee_cost = float(cost.get("fee", 0.0))
        slippage_cost = float(cost.get("slippage_cost", 0.0))
        funding_pnl = float(cost.get("funding_pnl", 0.0))
        gross_abs = abs(gross_pnl) if abs(gross_pnl) > 1e-12 else 0.0
        cost_to_gross_ratio = ((fee_cost + slippage_cost) / gross_abs) if gross_abs > 0 else 0.0

        return PerformanceMetrics(
            total_return=total_return,
            annual_return=annual_return,
            monthly_return=monthly_return,
            daily_return=daily_return,
            volatility=volatility,
            max_drawdown=max_drawdown,
            var_95=var_95,
            cvar_95=cvar_95,
            sharpe_ratio=sharpe,
            sortino_ratio=sortino,
            calmar_ratio=calmar,
            win_rate=trade_stats["win_rate"],
            profit_factor=trade_stats["profit_factor"],
            avg_trade=trade_stats["avg_trade"],
            avg_winning_trade=trade_stats["avg_winning_trade"],
            avg_losing_trade=trade_stats["avg_losing_trade"],
            max_consecutive_wins=trade_stats["max_consecutive_wins"],
            max_consecutive_losses=trade_stats["max_consecutive_losses"],
            total_trades=len(trades),
            trading_days=len(equity_curve),
            avg_trades_per_day=len(trades) / len(equity_curve) if len(equity_curve) > 0 else 0,
            gross_pnl=gross_pnl,
            fee_cost=fee_cost,
            slippage_cost=slippage_cost,
            funding_pnl=funding_pnl,
            cost_to_gross_ratio=cost_to_gross_ratio,
            turnover_notional=float(getattr(result, "turnover_notional", 0.0) or 0.0),
        )

    def _calculate_returns(self, equity_curve: np.ndarray) -> np.ndarray:
        """计算收益率"""
        if len(equity_curve) < 2:
            return np.array([])
        return np.diff(equity_curve) / equity_curve[:-1]

    def _annualize_return(self, total_return: float, periods: int) -> float:
        """年化收益率"""
        if periods <= 0:
            return 0
        # 加密货币 7×24 全年交易，使用 365 天
        years = periods / 365
        if years <= 0:
            return total_return
        return (1 + total_return) ** (1 / years) - 1

    def _calculate_daily_return(self, returns: np.ndarray) -> float:
        """计算日均收益率"""
        if len(returns) == 0:
            return 0
        return float(np.mean(returns))

    def _calculate_volatility(self, returns: np.ndarray) -> float:
        """计算年化波动率"""
        if len(returns) < 2:
            return 0
        return float(np.std(returns) * np.sqrt(365))

    def _calculate_var(self, returns: np.ndarray, confidence: float) -> float:
        """计算VaR"""
        if len(returns) == 0:
            return 0
        return float(np.percentile(returns, (1 - confidence) * 100))

    def _calculate_cvar(self, returns: np.ndarray, confidence: float) -> float:
        """计算CVaR（条件VaR）"""
        if len(returns) == 0:
            return 0
        var = self._calculate_var(returns, confidence)
        return float(np.mean(returns[returns <= var]))

    def _calculate_sharpe(self, returns: np.ndarray) -> float:
        """计算夏普比率"""
        if len(returns) < 2:
            return 0
        mean_return = np.mean(returns) * 365
        std_return = np.std(returns) * np.sqrt(365)
        if std_return == 0:
            return 0
        return float((mean_return - self.risk_free_rate) / std_return)

    def _calculate_sortino(self, returns: np.ndarray) -> float:
        """计算索提诺比率"""
        if len(returns) < 2:
            return 0
        mean_return = np.mean(returns) * 365
        downside_returns = returns[returns < 0]
        if len(downside_returns) == 0:
            return float("inf")
        downside_std = np.std(downside_returns) * np.sqrt(365)
        if downside_std == 0:
            return 0
        return float((mean_return - self.risk_free_rate) / downside_std)

    def _calculate_calmar(self, annual_return: float, max_drawdown: float) -> float:
        """计算卡玛比率"""
        if max_drawdown == 0:
            return 0.0
        return annual_return / max_drawdown

    def _analyze_trades(self, trades: List[BacktestTrade]) -> Dict:
        """分析交易"""
        if not trades:
            return {
                "win_rate": 0,
                "profit_factor": 0,
                "avg_trade": 0,
                "avg_winning_trade": 0,
                "avg_losing_trade": 0,
                "max_consecutive_wins": 0,
                "max_consecutive_losses": 0,
            }

        close_like = [
            t for t in trades
            if getattr(t, "trade_stage", "") in {"close", "funding"} or float(getattr(t, "pnl", 0.0) or 0.0) != 0.0
        ]
        pnls = [float(t.pnl) for t in close_like if float(getattr(t, "pnl", 0.0) or 0.0) != 0]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p < 0]

        win_rate = len(wins) / len(pnls) if pnls else 0

        total_wins = sum(wins)
        total_losses = abs(sum(losses))
        profit_factor = total_wins / total_losses if total_losses > 0 else float("inf")

        avg_trade = np.mean(pnls) if pnls else 0
        avg_winning_trade = np.mean(wins) if wins else 0
        avg_losing_trade = np.mean(losses) if losses else 0

        # 计算连续盈亏
        consecutive_wins = 0
        consecutive_losses = 0
        max_consecutive_wins = 0
        max_consecutive_losses = 0

        for pnl in pnls:
            if pnl > 0:
                consecutive_wins += 1
                consecutive_losses = 0
                max_consecutive_wins = max(max_consecutive_wins, consecutive_wins)
            else:
                consecutive_losses += 1
                consecutive_wins = 0
                max_consecutive_losses = max(max_consecutive_losses, consecutive_losses)

        return {
            "win_rate": win_rate,
            "profit_factor": profit_factor,
            "avg_trade": avg_trade,
            "avg_winning_trade": avg_winning_trade,
            "avg_losing_trade": avg_losing_trade,
            "max_consecutive_wins": max_consecutive_wins,
            "max_consecutive_losses": max_consecutive_losses,
        }

    def generate_monthly_returns(
        self,
        result: BacktestResult,
    ) -> pd.DataFrame:
        """生成月度收益表"""
        if not result.trades:
            return pd.DataFrame()

        # 按月分组交易
        trades_df = pd.DataFrame([
            {"date": t.timestamp, "pnl": t.pnl}
            for t in result.trades
        ])
        trades_df["date"] = pd.to_datetime(trades_df["date"])
        trades_df = trades_df.set_index("date")

        monthly = trades_df.resample("M").agg({
            "pnl": "sum"
        })

        return monthly

    def compare_strategies(
        self,
        results: Dict[str, BacktestResult],
    ) -> pd.DataFrame:
        """
        比较多个策略

        Args:
            results: {strategy_name: BacktestResult}

        Returns:
            比较表格
        """
        comparison = []

        for name, result in results.items():
            metrics = self.analyze(result)
            comparison.append({
                "strategy": name,
                "total_return": metrics.total_return,
                "annual_return": metrics.annual_return,
                "sharpe_ratio": metrics.sharpe_ratio,
                "max_drawdown": metrics.max_drawdown,
                "win_rate": metrics.win_rate,
                "profit_factor": metrics.profit_factor,
                "total_trades": metrics.total_trades,
            })

        return pd.DataFrame(comparison)


# 全局性能分析器实例
performance_analyzer = PerformanceAnalyzer()
