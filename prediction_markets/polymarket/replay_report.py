from __future__ import annotations

import json
import itertools
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from prediction_markets.polymarket.paper_replay import (
    PolymarketPaperReplay,
    ReplayConfig,
    build_replay_strategy,
)
from prediction_markets.polymarket.utils import parse_ts_any


@dataclass
class ReplayBatchConfig:
    strategy: str = "threshold"
    account_prefix: str = "batch"
    initial_cash: float = 1000.0
    order_size: float = 10.0
    buy_below: float = 0.45
    sell_above: float = 0.60
    momentum_window: int = 3
    momentum_buy_delta: float = 0.04
    momentum_sell_delta: float = 0.04
    max_order_notional: float = 100.0
    max_position_notional: float = 500.0
    fee_rate: float = 0.0


@dataclass
class ReplayWalkForwardConfig:
    train_minutes: int = 120
    test_minutes: int = 60
    step_minutes: int = 60


def _safe_token_label(token_id: str) -> str:
    text = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(token_id or "token"))
    return text[:48] or "token"


def summarize_replay(token_id: str, result: Dict[str, Any], initial_cash: float) -> Dict[str, Any]:
    summary = result.get("summary") or {}
    fills = result.get("fills") or []
    buy_fills = [fill for fill in fills if str(fill.get("side") or "").upper() == "BUY"]
    sell_fills = [fill for fill in fills if str(fill.get("side") or "").upper() == "SELL"]
    equity = float(summary.get("equity") or 0.0)
    realized_pnl = float(summary.get("realized_pnl") or 0.0)
    unrealized_pnl = float(summary.get("unrealized_pnl") or 0.0)
    return {
        "token_id": token_id,
        "account_id": result.get("account_id"),
        "quotes_seen": int(result.get("quotes_seen") or 0),
        "orders_count": int(result.get("orders_count") or 0),
        "fills_count": int(result.get("fills_count") or 0),
        "buy_fills": len(buy_fills),
        "sell_fills": len(sell_fills),
        "decisions_count": len(result.get("decisions") or []),
        "equity": round(equity, 8),
        "net_pnl": round(equity - float(initial_cash or 0.0), 8),
        "realized_pnl": round(realized_pnl, 8),
        "unrealized_pnl": round(unrealized_pnl, 8),
        "open_positions": len(summary.get("positions") or []),
    }


async def run_replay_batch(
    *,
    token_ids: List[str],
    since: datetime,
    until: datetime,
    config: Optional[ReplayBatchConfig] = None,
) -> Dict[str, Any]:
    cfg = config or ReplayBatchConfig()
    rows: List[Dict[str, Any]] = []
    results: Dict[str, Any] = {}
    for index, token_id in enumerate([str(token).strip() for token in token_ids if str(token).strip()], start=1):
        account_id = f"{cfg.account_prefix}-{_safe_token_label(token_id)}-{index}"
        replay = PolymarketPaperReplay(
            ReplayConfig(
                account_id=account_id,
                initial_cash=cfg.initial_cash,
                order_size=cfg.order_size,
                buy_below=cfg.buy_below,
                sell_above=cfg.sell_above,
                momentum_window=cfg.momentum_window,
                momentum_buy_delta=cfg.momentum_buy_delta,
                momentum_sell_delta=cfg.momentum_sell_delta,
                max_order_notional=cfg.max_order_notional,
                max_position_notional=cfg.max_position_notional,
                fee_rate=cfg.fee_rate,
            ),
            strategy=build_replay_strategy(cfg.strategy),
        )
        result = await replay.run_token(token_id, parse_ts_any(since), parse_ts_any(until), reset=True)
        results[token_id] = result
        rows.append(summarize_replay(token_id, result, cfg.initial_cash))
    rows.sort(key=lambda item: (float(item.get("net_pnl") or 0.0), int(item.get("fills_count") or 0)), reverse=True)
    return {
        "config": asdict(cfg),
        "since": parse_ts_any(since).isoformat(),
        "until": parse_ts_any(until).isoformat(),
        "rows": rows,
        "results": results,
        "best": rows[0] if rows else None,
    }


def write_replay_batch_report(report: Dict[str, Any], output_dir: Path, name: str = "polymarket_replay_batch") -> Dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{name}.json"
    md_path = output_dir / f"{name}.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    rows = report.get("rows") or []
    lines = [
        "# Polymarket Replay Batch",
        "",
        f"- strategy: `{(report.get('config') or {}).get('strategy')}`",
        f"- since: `{report.get('since')}`",
        f"- until: `{report.get('until')}`",
        f"- tokens: `{len(rows)}`",
        "",
        "| rank | token_id | net_pnl | equity | fills | decisions | open_positions |",
        "|---:|---|---:|---:|---:|---:|---:|",
    ]
    for idx, row in enumerate(rows, start=1):
        lines.append(
            "| {rank} | `{token}` | {pnl:.6f} | {equity:.6f} | {fills} | {decisions} | {positions} |".format(
                rank=idx,
                token=row.get("token_id"),
                pnl=float(row.get("net_pnl") or 0.0),
                equity=float(row.get("equity") or 0.0),
                fills=int(row.get("fills_count") or 0),
                decisions=int(row.get("decisions_count") or 0),
                positions=int(row.get("open_positions") or 0),
            )
        )
    lines.append("")
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return {"json_path": str(json_path), "markdown_path": str(md_path)}


def _parse_float_grid(value: str) -> List[float]:
    return [float(item.strip()) for item in str(value or "").split(",") if item.strip()]


def _parse_int_grid(value: str) -> List[int]:
    return [int(item.strip()) for item in str(value or "").split(",") if item.strip()]


def threshold_param_grid(buy_below: str, sell_above: str) -> List[Dict[str, Any]]:
    return [
        {"buy_below": buy, "sell_above": sell}
        for buy, sell in itertools.product(_parse_float_grid(buy_below), _parse_float_grid(sell_above))
        if buy < sell
    ]


def momentum_param_grid(windows: str, buy_deltas: str, sell_deltas: str) -> List[Dict[str, Any]]:
    return [
        {"momentum_window": window, "momentum_buy_delta": buy_delta, "momentum_sell_delta": sell_delta}
        for window, buy_delta, sell_delta in itertools.product(
            _parse_int_grid(windows),
            _parse_float_grid(buy_deltas),
            _parse_float_grid(sell_deltas),
        )
    ]


def _copy_batch_config(config: ReplayBatchConfig, **updates: Any) -> ReplayBatchConfig:
    data = asdict(config)
    data.update(updates)
    return ReplayBatchConfig(**data)


async def run_replay_grid(
    *,
    token_ids: List[str],
    since: datetime,
    until: datetime,
    base_config: ReplayBatchConfig,
    param_grid: List[Dict[str, Any]],
) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    reports: List[Dict[str, Any]] = []
    for idx, params in enumerate(param_grid, start=1):
        cfg = _copy_batch_config(base_config, account_prefix=f"{base_config.account_prefix}-g{idx}", **params)
        report = await run_replay_batch(token_ids=token_ids, since=parse_ts_any(since), until=parse_ts_any(until), config=cfg)
        token_rows = report.get("rows") or []
        total_pnl = sum(float(row.get("net_pnl") or 0.0) for row in token_rows)
        total_fills = sum(int(row.get("fills_count") or 0) for row in token_rows)
        traded_tokens = sum(1 for row in token_rows if int(row.get("fills_count") or 0) > 0)
        row = {
            "grid_id": idx,
            "params": params,
            "strategy": cfg.strategy,
            "tokens": len(token_rows),
            "traded_tokens": traded_tokens,
            "total_net_pnl": round(total_pnl, 8),
            "avg_net_pnl": round(total_pnl / len(token_rows), 8) if token_rows else 0.0,
            "total_fills": total_fills,
            "best_token": (report.get("best") or {}).get("token_id"),
            "best_token_pnl": (report.get("best") or {}).get("net_pnl"),
        }
        rows.append(row)
        reports.append({"grid_id": idx, "params": params, "batch": report})
    rows.sort(key=lambda item: (float(item.get("total_net_pnl") or 0.0), int(item.get("total_fills") or 0)), reverse=True)
    return {
        "base_config": asdict(base_config),
        "since": parse_ts_any(since).isoformat(),
        "until": parse_ts_any(until).isoformat(),
        "grid_size": len(param_grid),
        "rows": rows,
        "reports": reports,
        "best": rows[0] if rows else None,
    }


def _walk_forward_segments(since: datetime, until: datetime, config: ReplayWalkForwardConfig) -> List[Dict[str, datetime]]:
    start = parse_ts_any(since)
    end = parse_ts_any(until)
    train_delta = timedelta(minutes=max(1, int(config.train_minutes or 1)))
    test_delta = timedelta(minutes=max(1, int(config.test_minutes or 1)))
    step_delta = timedelta(minutes=max(1, int(config.step_minutes or config.test_minutes or 1)))
    epsilon = timedelta(microseconds=1)
    segments: List[Dict[str, datetime]] = []
    cursor = start
    while cursor + train_delta + test_delta <= end:
        train_since = cursor
        test_since = cursor + train_delta
        test_until_exclusive = test_since + test_delta
        segments.append(
            {
                "train_since": train_since,
                "train_until": test_since - epsilon,
                "test_since": test_since,
                "test_until": test_until_exclusive - epsilon,
            }
        )
        cursor += step_delta
    return segments


def _total_batch_pnl(report: Dict[str, Any]) -> float:
    return round(sum(float(row.get("net_pnl") or 0.0) for row in report.get("rows") or []), 8)


def _total_batch_fills(report: Dict[str, Any]) -> int:
    return sum(int(row.get("fills_count") or 0) for row in report.get("rows") or [])


async def run_replay_walk_forward(
    *,
    token_ids: List[str],
    since: datetime,
    until: datetime,
    base_config: ReplayBatchConfig,
    param_grid: List[Dict[str, Any]],
    walk_config: Optional[ReplayWalkForwardConfig] = None,
) -> Dict[str, Any]:
    wf_cfg = walk_config or ReplayWalkForwardConfig()
    segments = _walk_forward_segments(parse_ts_any(since), parse_ts_any(until), wf_cfg)
    rows: List[Dict[str, Any]] = []
    reports: List[Dict[str, Any]] = []
    for idx, segment in enumerate(segments, start=1):
        train_base = _copy_batch_config(base_config, account_prefix=f"{base_config.account_prefix}-wf{idx}-train")
        train_report = await run_replay_grid(
            token_ids=token_ids,
            since=segment["train_since"],
            until=segment["train_until"],
            base_config=train_base,
            param_grid=param_grid,
        )
        best_train = train_report.get("best") or {}
        selected_params = dict(best_train.get("params") or {})
        test_report: Dict[str, Any] = {"rows": [], "best": None}
        if selected_params:
            test_config = _copy_batch_config(
                base_config,
                account_prefix=f"{base_config.account_prefix}-wf{idx}-test",
                **selected_params,
            )
            test_report = await run_replay_batch(
                token_ids=token_ids,
                since=segment["test_since"],
                until=segment["test_until"],
                config=test_config,
            )
        test_total_pnl = _total_batch_pnl(test_report)
        row = {
            "segment_id": idx,
            "train_since": segment["train_since"].isoformat(),
            "train_until": segment["train_until"].isoformat(),
            "test_since": segment["test_since"].isoformat(),
            "test_until": segment["test_until"].isoformat(),
            "selected_params": selected_params,
            "train_total_net_pnl": float(best_train.get("total_net_pnl") or 0.0),
            "train_avg_net_pnl": float(best_train.get("avg_net_pnl") or 0.0),
            "train_total_fills": int(best_train.get("total_fills") or 0),
            "test_total_net_pnl": test_total_pnl,
            "test_avg_net_pnl": round(test_total_pnl / len(token_ids), 8) if token_ids else 0.0,
            "test_total_fills": _total_batch_fills(test_report),
            "test_traded_tokens": sum(1 for item in test_report.get("rows") or [] if int(item.get("fills_count") or 0) > 0),
            "test_best_token": (test_report.get("best") or {}).get("token_id"),
            "test_best_token_pnl": (test_report.get("best") or {}).get("net_pnl"),
        }
        rows.append(row)
        reports.append({"segment_id": idx, "selected_params": selected_params, "train_grid": train_report, "test_batch": test_report})
    selected_counts = Counter(json.dumps(row.get("selected_params") or {}, sort_keys=True) for row in rows)
    test_pnls = [float(row.get("test_total_net_pnl") or 0.0) for row in rows]
    summary = {
        "segments": len(rows),
        "tokens": len(token_ids),
        "total_test_net_pnl": round(sum(test_pnls), 8),
        "avg_test_net_pnl": round(sum(test_pnls) / len(test_pnls), 8) if test_pnls else 0.0,
        "worst_test_net_pnl": round(min(test_pnls), 8) if test_pnls else 0.0,
        "positive_segments": sum(1 for value in test_pnls if value > 0),
        "selection_counts": [
            {"params": json.loads(key), "segments": count}
            for key, count in selected_counts.most_common()
        ],
    }
    return {
        "base_config": asdict(base_config),
        "walk_config": asdict(wf_cfg),
        "token_ids": [str(token) for token in token_ids],
        "since": parse_ts_any(since).isoformat(),
        "until": parse_ts_any(until).isoformat(),
        "grid_size": len(param_grid),
        "rows": rows,
        "reports": reports,
        "summary": summary,
    }


def write_replay_grid_report(report: Dict[str, Any], output_dir: Path, name: str = "polymarket_replay_grid") -> Dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{name}.json"
    md_path = output_dir / f"{name}.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    rows = report.get("rows") or []
    lines = [
        "# Polymarket Replay Grid Search",
        "",
        f"- strategy: `{(report.get('base_config') or {}).get('strategy')}`",
        f"- since: `{report.get('since')}`",
        f"- until: `{report.get('until')}`",
        f"- grid_size: `{report.get('grid_size')}`",
        "",
        "| rank | grid_id | params | total_net_pnl | avg_net_pnl | fills | traded_tokens | best_token |",
        "|---:|---:|---|---:|---:|---:|---:|---|",
    ]
    for rank, row in enumerate(rows, start=1):
        lines.append(
            "| {rank} | {grid_id} | `{params}` | {total:.6f} | {avg:.6f} | {fills} | {traded} | `{best}` |".format(
                rank=rank,
                grid_id=int(row.get("grid_id") or 0),
                params=json.dumps(row.get("params") or {}, sort_keys=True),
                total=float(row.get("total_net_pnl") or 0.0),
                avg=float(row.get("avg_net_pnl") or 0.0),
                fills=int(row.get("total_fills") or 0),
                traded=int(row.get("traded_tokens") or 0),
                best=row.get("best_token") or "",
            )
        )
    lines.append("")
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return {"json_path": str(json_path), "markdown_path": str(md_path)}


def write_replay_walk_forward_report(report: Dict[str, Any], output_dir: Path, name: str = "polymarket_replay_walk_forward") -> Dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{name}.json"
    md_path = output_dir / f"{name}.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    rows = report.get("rows") or []
    summary = report.get("summary") or {}
    lines = [
        "# Polymarket Replay Walk Forward",
        "",
        f"- strategy: `{(report.get('base_config') or {}).get('strategy')}`",
        f"- since: `{report.get('since')}`",
        f"- until: `{report.get('until')}`",
        f"- grid_size: `{report.get('grid_size')}`",
        f"- segments: `{summary.get('segments', len(rows))}`",
        f"- total_test_net_pnl: `{float(summary.get('total_test_net_pnl') or 0.0):.6f}`",
        "",
        "| segment | selected_params | train_pnl | test_pnl | test_fills | test_best_token |",
        "|---:|---|---:|---:|---:|---|",
    ]
    for row in rows:
        lines.append(
            "| {segment} | `{params}` | {train:.6f} | {test:.6f} | {fills} | `{best}` |".format(
                segment=int(row.get("segment_id") or 0),
                params=json.dumps(row.get("selected_params") or {}, sort_keys=True),
                train=float(row.get("train_total_net_pnl") or 0.0),
                test=float(row.get("test_total_net_pnl") or 0.0),
                fills=int(row.get("test_total_fills") or 0),
                best=row.get("test_best_token") or "",
            )
        )
    lines.append("")
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return {"json_path": str(json_path), "markdown_path": str(md_path)}
