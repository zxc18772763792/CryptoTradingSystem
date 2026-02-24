"""
报告生成器模块
"""
from datetime import datetime
from typing import Optional, List, Dict, Any
import json
from pathlib import Path
from loguru import logger

from core.backtest.backtest_engine import BacktestResult
from core.backtest.performance_analyzer import PerformanceAnalyzer, PerformanceMetrics


class ReportGenerator:
    """报告生成器"""

    def __init__(self):
        self.analyzer = PerformanceAnalyzer()

    def generate_report(
        self,
        result: BacktestResult,
        strategy_name: str = "Strategy",
        output_path: Optional[str] = None,
    ) -> str:
        """
        生成回测报告

        Args:
            result: 回测结果
            strategy_name: 策略名称
            output_path: 输出路径

        Returns:
            报告内容
        """
        metrics = self.analyzer.analyze(result)

        # 生成文本报告
        report = self._generate_text_report(result, metrics, strategy_name)

        # 保存到文件
        if output_path:
            path = Path(output_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                f.write(report)
            logger.info(f"Report saved to {output_path}")

        return report

    def _generate_text_report(
        self,
        result: BacktestResult,
        metrics: PerformanceMetrics,
        strategy_name: str,
    ) -> str:
        """生成文本报告"""
        lines = [
            "=" * 60,
            f"回测报告: {strategy_name}",
            f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "=" * 60,
            "",
            "--- 收益指标 ---",
            f"初始资金: ${result.initial_capital:,.2f}",
            f"最终资金: ${result.final_capital:,.2f}",
            f"总收益: ${result.total_return:,.2f} ({result.total_return_pct*100:.2f}%)",
            f"年化收益: {metrics.annual_return*100:.2f}%",
            f"月均收益: {metrics.monthly_return*100:.2f}%",
            f"日均收益: {metrics.daily_return*100:.4f}%",
            "",
            "--- 风险指标 ---",
            f"年化波动率: {metrics.volatility*100:.2f}%",
            f"最大回撤: ${result.max_drawdown:,.2f} ({metrics.max_drawdown:.2f}%)",
            f"VaR (95%): {metrics.var_95*100:.2f}%",
            f"CVaR (95%): {metrics.cvar_95*100:.2f}%",
            "",
            "--- 风险调整收益 ---",
            f"夏普比率: {metrics.sharpe_ratio:.2f}",
            f"索提诺比率: {metrics.sortino_ratio:.2f}",
            f"卡玛比率: {metrics.calmar_ratio:.2f}",
            "",
            "--- 交易统计 ---",
            f"总交易次数: {metrics.total_trades}",
            f"盈利交易: {result.winning_trades}",
            f"亏损交易: {result.losing_trades}",
            f"胜率: {metrics.win_rate*100:.2f}%",
            f"盈亏比: {metrics.profit_factor:.2f}",
            f"平均交易收益: ${metrics.avg_trade:,.2f}",
            f"平均盈利交易: ${metrics.avg_winning_trade:,.2f}",
            f"平均亏损交易: ${metrics.avg_losing_trade:,.2f}",
            f"最大连续盈利: {metrics.max_consecutive_wins} 次",
            f"最大连续亏损: {metrics.max_consecutive_losses} 次",
            "",
            "--- 交易频率 ---",
            f"交易天数: {metrics.trading_days}",
            f"日均交易: {metrics.avg_trades_per_day:.2f}",
            "",
            "--- 成本分解 ---",
            f"Gross PnL: ${metrics.gross_pnl:,.2f}",
            f"手续费: ${metrics.fee_cost:,.2f}",
            f"滑点成本: ${metrics.slippage_cost:,.2f}",
            f"资金费率PnL: ${metrics.funding_pnl:,.2f}",
            f"成本/毛利占比: {metrics.cost_to_gross_ratio*100:.2f}%",
            f"换手名义金额: ${metrics.turnover_notional:,.2f}",
            "",
            "--- 评价 ---",
        ]

        # 添加评价
        if metrics.sharpe_ratio > 2:
            lines.append("夏普比率优秀 (>2)")
        elif metrics.sharpe_ratio > 1:
            lines.append("夏普比率良好 (>1)")
        elif metrics.sharpe_ratio > 0.5:
            lines.append("夏普比率一般 (>0.5)")
        else:
            lines.append("夏普比率较低 (<0.5)")

        if metrics.max_drawdown < 10:
            lines.append("风险控制优秀 (回撤<10%)")
        elif metrics.max_drawdown < 20:
            lines.append("风险控制良好 (回撤<20%)")
        else:
            lines.append("风险较高 (回撤>20%)")

        lines.extend(["", "=" * 60])

        return "\n".join(lines)

    def generate_json_report(
        self,
        result: BacktestResult,
        strategy_name: str = "Strategy",
    ) -> Dict:
        """生成JSON报告"""
        metrics = self.analyzer.analyze(result)

        return {
            "meta": {
                "strategy_name": strategy_name,
                "generated_at": datetime.now().isoformat(),
            },
            "returns": {
                "initial_capital": result.initial_capital,
                "final_capital": result.final_capital,
                "total_return": result.total_return,
                "total_return_pct": result.total_return_pct,
                "annual_return": metrics.annual_return,
                "monthly_return": metrics.monthly_return,
                "daily_return": metrics.daily_return,
            },
            "risk": {
                "volatility": metrics.volatility,
                "max_drawdown": metrics.max_drawdown,
                "var_95": metrics.var_95,
                "cvar_95": metrics.cvar_95,
            },
            "risk_adjusted": {
                "sharpe_ratio": metrics.sharpe_ratio,
                "sortino_ratio": metrics.sortino_ratio,
                "calmar_ratio": metrics.calmar_ratio,
            },
            "trading": {
                "total_trades": metrics.total_trades,
                "winning_trades": result.winning_trades,
                "losing_trades": result.losing_trades,
                "win_rate": metrics.win_rate,
                "profit_factor": metrics.profit_factor,
                "avg_trade": metrics.avg_trade,
                "avg_winning_trade": metrics.avg_winning_trade,
                "avg_losing_trade": metrics.avg_losing_trade,
                "max_consecutive_wins": metrics.max_consecutive_wins,
                "max_consecutive_losses": metrics.max_consecutive_losses,
            },
            "cost_breakdown": {
                "gross_pnl": metrics.gross_pnl,
                "fee_cost": metrics.fee_cost,
                "slippage_cost": metrics.slippage_cost,
                "funding_pnl": metrics.funding_pnl,
                "cost_to_gross_ratio": metrics.cost_to_gross_ratio,
                "turnover_notional": metrics.turnover_notional,
            },
            "equity_curve": result.equity_curve,
        }

    def generate_comparison_report(
        self,
        results: Dict[str, BacktestResult],
        output_path: Optional[str] = None,
    ) -> str:
        """生成策略比较报告"""
        comparison_df = self.analyzer.compare_strategies(results)

        lines = [
            "=" * 80,
            "策略比较报告",
            f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "=" * 80,
            "",
            comparison_df.to_string(index=False),
            "",
            "=" * 80,
        ]

        report = "\n".join(lines)

        if output_path:
            path = Path(output_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                f.write(report)

        return report


# 全局报告生成器实例
report_generator = ReportGenerator()
