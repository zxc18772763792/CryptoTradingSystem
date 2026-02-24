"""Shared helpers for 5m perpetual HF research scripts."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.backtest.funding_provider import FundingProviderConfig, FundingRateProvider
from core.backtest.backtest_engine import BacktestConfig, BacktestEngine, BacktestResult
from core.data.data_storage import data_storage
from strategies.quantitative.multi_factor_hf import MultiFactorHFStrategy


DEFAULT_SYMBOL = "BTC/USDT"
DEFAULT_TIMEFRAME = "5m"


def default_reports_dir(prefix: str = "hf_research") -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    out = Path("data/reports") / f"{stamp}_{prefix}"
    out.mkdir(parents=True, exist_ok=True)
    return out


def add_common_args(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument("--exchange", default="binance")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--timeframe", default=DEFAULT_TIMEFRAME)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--config", default="config/strategy_multi_factor_hf.yaml")
    return parser


async def _load_df_async(exchange: str, symbol: str, timeframe: str, days: int) -> pd.DataFrame:
    end = pd.Timestamp.utcnow().to_pydatetime().replace(tzinfo=None)
    start = (pd.Timestamp.utcnow() - pd.Timedelta(days=max(1, int(days)))).to_pydatetime().replace(tzinfo=None)
    df = await data_storage.load_klines_from_parquet(exchange=exchange, symbol=symbol, timeframe=timeframe, start_time=start, end_time=end)
    if df is None or df.empty:
        raise RuntimeError(f"No local data found for {exchange} {symbol} {timeframe}")
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=[c for c in ["open", "high", "low", "close"] if c in df.columns])
    if "symbol" not in df.columns:
        df["symbol"] = symbol
    return df


def load_df(exchange: str, symbol: str, timeframe: str, days: int) -> pd.DataFrame:
    return asyncio.run(_load_df_async(exchange=exchange, symbol=symbol, timeframe=timeframe, days=days))


def build_strategy(config_path: str) -> MultiFactorHFStrategy:
    st = MultiFactorHFStrategy(name="Research_MultiFactorHF", params={"config_path": config_path})
    st.initialize()
    return st


def run_backtest(df: pd.DataFrame, config_path: str, bt_cfg: Optional[BacktestConfig] = None) -> BacktestResult:
    strategy = build_strategy(config_path=config_path)
    cfg = bt_cfg or BacktestConfig(enable_shorting=True, leverage=2.0)
    funding_provider = None
    if bool(getattr(cfg, "include_funding", False)):
        funding_provider = FundingRateProvider(
            FundingProviderConfig(
                source="auto",
                exchange="binance",
                cache_dir="data/funding",
            )
        )
    engine = BacktestEngine(config=cfg, funding_provider=funding_provider)
    return asyncio.run(engine.run_backtest(strategy=strategy, data=df, symbol=str(df["symbol"].iloc[-1])))


def result_to_dict(result: BacktestResult) -> Dict[str, Any]:
    if is_dataclass(result):
        raw = asdict(result)
    else:
        raw = dict(result)
    # limit payload size
    raw["equity_curve"] = raw.get("equity_curve", [])[-2000:]
    raw["trades"] = raw.get("trades", [])[-200:]
    return raw


def save_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def ensure_output_dir(path_arg: str, prefix: str) -> Path:
    if path_arg:
        p = Path(path_arg)
        p.mkdir(parents=True, exist_ok=True)
        return p
    return default_reports_dir(prefix=prefix)
