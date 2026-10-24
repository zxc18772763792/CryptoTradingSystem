"""Summarize committed paper outcomes; never submits orders or modifies decisions."""
from __future__ import annotations
from collections import defaultdict
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from loguru import logger
logger.remove()
from core.research.telegram_forward import Store,utc


def summarize(root):
    import numpy as np
    store=Store(root)
    journal=store.verify_journal()
    activation=store.state("activation")
    if not activation:
        raise RuntimeError("Paper observation has not been activated")
    outcomes={key:json.loads(blob) for key,blob in store.db.execute("SELECT event_id,payload_json FROM outcomes")}
    groups=defaultdict(list)
    for (blob,) in store.db.execute("SELECT payload_json FROM decisions ORDER BY entry_at"):
        row=json.loads(blob)
        groups[row["policy"]].append(dict(row,outcome=outcomes.get(row["event_id"])))
    result={"as_of":utc().isoformat(),"paper_only":True,"activation_at":activation["at"],"journal":journal,"books":{}}
    for policy in activation["policies"]:
        rows=groups[policy]
        complete=[r for r in rows if r["outcome"] and r["outcome"].get("status")=="settled"]
        values=[r["outcome"]["net"] for r in complete]
        excess=[r["outcome"]["net"]-r["outcome"]["same_entry_long_net"] for r in complete]
        weeks=defaultdict(list)
        for row in complete:
            at=utc(row["entry_at"])
            week=f"{at.isocalendar().year}-W{at.isocalendar().week:02d}"
            weeks[week].append((row["outcome"]["net"],row["outcome"]["net"]-row["outcome"]["same_entry_long_net"]))
        intervals=None
        if len(weeks)>=8 and len(complete)>=30:
            rng=np.random.default_rng(20261024)
            buckets=[np.array(v) for v in weeks.values()]
            draws=[np.concatenate([buckets[i] for i in rng.integers(0,len(buckets),len(buckets))]).mean(axis=0) for _ in range(2000)]
            intervals=np.quantile(draws,[.025,.975],axis=0).tolist()
        book={"decisions":len(rows),"mature_settled":len(complete),
              "pending":sum(r["outcome"] is None for r in rows),
              "unavailable":sum(bool(r["outcome"] and r["outcome"].get("status")!="settled") for r in rows),
              "mean_net_per_notional":sum(values)/len(values) if values else None,
              "mean_excess_vs_same_entry_long":sum(excess)/len(excess) if excess else None,
              "mean_net_stress_40bps":sum(r["outcome"]["net_stress"] for r in complete)/len(complete) if complete else None,
              "win_rate":sum(v>0 for v in values)/len(values) if values else None,
              "mean_without_best":(sum(values)-max(values))/(len(values)-1) if len(values)>1 else None,
              "calendar_weeks":len(weeks),"weekly_cluster_intervals_net_and_excess":intervals,
              "status":"collecting_forward_evidence","promotion_allowed":False}
        # These are descriptive screens for a later review, never an execution gate.
        book["screen_pass"] = bool(len(complete)>=100 and len(weeks)>=8 and intervals
                                   and intervals[0][0]>0 and intervals[0][1]>0
                                   and book["mean_without_best"]>0 and book["mean_net_stress_40bps"]>0)
        result["books"][policy]=book
    path=Path(root)/"forward_summary.json"
    temporary=path.with_name(path.name+".tmp")
    temporary.write_text(json.dumps(result,indent=2,allow_nan=False),encoding="utf-8")
    temporary.replace(path)
    names={"cq_follow":"CryptoQuant 顺势24h","chat_follow":"主群小时顺势6h",
           "chat_contrarian":"主群小时反向6h","chat_6h_follow":"主群六小时顺势6h",
           "chat_6h_contrarian":"主群六小时反向6h"}
    lines=["# Telegram 模拟前向观察", "",f"更新：{result['as_of']}（UTC）", "",
           "| 观察账本 | 冻结事件 | 成熟结算 | 待结算 | 平均净收益/笔 | 相对同入场做多超额/笔 |",
           "|---|---:|---:|---:|---:|---:|---:|"]
    for policy,value in result["books"].items():
        net="—" if value["mean_net_per_notional"] is None else f"{value['mean_net_per_notional']:.2%}"
        excess="—" if value["mean_excess_vs_same_entry_long"] is None else f"{value['mean_excess_vs_same_entry_long']:.2%}"
        lines.append(f"| {names.get(policy,policy)} | {value['decisions']} | {value['mature_settled']} | {value['pending']} | {net} | {excess} |")
    lines += ["", "收益按单笔入场名义本金计算，含约定往返成本和实际资金费；不是账户收益。—表示尚无成熟样本。", "",
              "API中文群另做技术压力观察，不生成看空交易。全部账本仅供研究，筛查没有进行跨账本多重比较调整，禁止自动放行实盘。"]
    (Path(root)/"forward_readout.md").write_text("\n".join(lines)+"\n",encoding="utf-8")
    return result


if __name__=="__main__":
    output=Path(sys.argv[1]) if len(sys.argv)>1 else ROOT/"data/research/telegram_live_20261024"
    print(json.dumps(summarize(output),allow_nan=False))
