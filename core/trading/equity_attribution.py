"""Reconcile the displayed paper equity without combining duplicate trade feeds."""
from collections import defaultdict


def build_paper_equity_attribution(*, equity, initial_equity, closed_positions, open_positions, fee_rows, total_fees):
    rows = defaultdict(lambda: {"realized_pnl": 0.0, "unrealized_pnl": 0.0, "fees": 0.0, "closed_count": 0, "open_count": 0})
    for pos in closed_positions:
        row = rows[str(pos.strategy or "未归属策略")]
        row["realized_pnl"] += float(pos.realized_pnl or 0)
        row["closed_count"] += 1
    for pos in open_positions:
        row = rows[str(pos.strategy or "未归属策略")]
        row["realized_pnl"] += float(getattr(pos, "realized_pnl", 0) or 0)
        row["unrealized_pnl"] += float(pos.unrealized_pnl or 0)
        row["open_count"] += 1
    for fee in fee_rows:
        rows[str(fee.get("strategy") or "未归属策略")]["fees"] += float(fee.get("paper_fee_usd") or 0)
    known_fees = sum(row["fees"] for row in rows.values())
    if abs(float(total_fees) - known_fees) > 1e-8:
        rows["未归属手续费"]["fees"] += float(total_fees) - known_fees
    result = []
    for strategy, row in rows.items():
        row["net_pnl"] = row["realized_pnl"] + row["unrealized_pnl"] - row["fees"]
        result.append({"strategy": strategy, **row})
    result.sort(key=lambda row: abs(row["net_pnl"]), reverse=True)
    attributed = sum(row["net_pnl"] for row in result)
    return {
        "mode": "paper", "equity": float(equity), "initial_equity": float(initial_equity),
        "attributed_pnl": attributed,
        "carry_forward_difference": float(equity) - float(initial_equity) - attributed,
        "strategies": result,
        "scope_note": "当前保留的持仓账本，已实现盈亏包含部分平仓；手续费按已保存的费用账本扣除，旧记录缺失的费用无法还原。与72小时曲线范围不同；旧净值快照中的重复结转不代表策略收益。",
    }
