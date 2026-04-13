"""Train and package an ML signal model."""
from __future__ import annotations

import argparse
import asyncio
import shutil
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Add project root to path so relative imports work when run as a script
# ---------------------------------------------------------------------------
_HERE = Path(__file__).resolve()
_ROOT = _HERE.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd
from loguru import logger

from core.ml.pipeline import PipelineError, diagnose_environment, run_signal_training_pipeline


async def load_ohlcv(
    exchange: str,
    symbol: str,
    timeframe: str,
    days: int,
) -> pd.DataFrame:
    """Load OHLCV klines from the local parquet store."""
    from datetime import datetime, timedelta, timezone

    from core.data import data_storage

    await data_storage.initialize()
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=days)
    df = await data_storage.load_klines_from_parquet(
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        start_time=start_time,
        end_time=end_time,
    )
    return df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train and package an XGBoost ML signal model from local OHLCV data"
    )
    parser.add_argument("--exchange", default="binance", help="Exchange name (default: binance)")
    parser.add_argument("--symbol", default="BTC/USDT", help="Symbol (default: BTC/USDT)")
    parser.add_argument("--timeframe", default="1h", help="Timeframe (default: 1h)")
    parser.add_argument("--days", type=int, default=365, help="Lookback days (default: 365)")
    parser.add_argument(
        "--forward-bars",
        type=int,
        default=4,
        dest="forward_bars",
        help="N bars ahead for label generation (default: 4)",
    )
    parser.add_argument(
        "--output",
        default="models/ml_signal_xgb",
        help="Output model directory (default: models/ml_signal_xgb)",
    )
    parser.add_argument("--test-size", type=float, default=0.2, dest="test_size")
    parser.add_argument("--n-estimators", type=int, default=300, dest="n_estimators")
    parser.add_argument("--max-depth", type=int, default=5, dest="max_depth")
    parser.add_argument("--learning-rate", type=float, default=0.05, dest="learning_rate")
    parser.add_argument(
        "--scale-pos-weight",
        type=float,
        default=0.0,
        dest="scale_pos_weight",
        help="XGBoost scale_pos_weight (0 = auto)",
    )
    parser.add_argument(
        "--prediction-threshold",
        type=float,
        default=0.55,
        dest="prediction_threshold",
        help="Probability threshold used for evaluation and gate checks (default: 0.55)",
    )
    return parser.parse_args()


async def main_async(args: argparse.Namespace) -> None:
    diagnostics = diagnose_environment()
    logger.info(
        "ML environment: "
        f"xgboost={diagnostics.xgboost.available} "
        f"sklearn={diagnostics.sklearn.available}"
    )

    logger.info(
        "Loading OHLCV: "
        f"exchange={args.exchange} symbol={args.symbol} timeframe={args.timeframe} days={args.days}"
    )
    df = await load_ohlcv(
        exchange=args.exchange,
        symbol=args.symbol,
        timeframe=args.timeframe,
        days=args.days,
    )

    if df is None or df.empty:
        raise PipelineError(
            "data",
            "no OHLCV data found",
            details={
                "hint": "download historical data first via the data downloader before training",
                "exchange": args.exchange,
                "symbol": args.symbol,
                "timeframe": args.timeframe,
            },
        )

    logger.info(f"Loaded {len(df)} rows. Starting ML pipeline...")
    run = run_signal_training_pipeline(
        df=df,
        output_root=Path(args.output),
        symbol=args.symbol,
        timeframe=args.timeframe,
        exchange=args.exchange,
        forward_bars=args.forward_bars,
        test_size=args.test_size,
        n_estimators=args.n_estimators,
        max_depth=args.max_depth,
        learning_rate=args.learning_rate,
        scale_pos_weight=args.scale_pos_weight,
        prediction_threshold=args.prediction_threshold,
    )

    report = dict(run.metrics.get("classification_report") or {})
    positive = dict(report.get("1") or {})
    logger.info(
        f"Train samples={run.metrics.get('train_samples')} "
        f"Test samples={run.metrics.get('test_samples')} "
        f"AUC={run.metrics.get('auc')}"
    )
    logger.info(
        f"Precision={positive.get('precision', 0.0):.4f} "
        f"Recall={positive.get('recall', 0.0):.4f} "
        f"F1={positive.get('f1-score', 0.0):.4f}"
    )

    top_features = sorted(run.feature_importances.items(), key=lambda item: item[1], reverse=True)[:5]
    logger.info("Top-5 features: " + "  ".join(f"{key}={value:.4f}" for key, value in top_features))
    logger.info(f"Model artifacts saved to {run.artifact_dir}")
    logger.info(f"Manifest model_id={run.manifest['model_id']}")
    try:
        artifact_model_path = Path(run.artifact_dir) / "model.json"
        legacy_model_path = _ROOT / "models" / "ml_signal_xgb.json"
        legacy_model_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(artifact_model_path, legacy_model_path)
        logger.info(f"Legacy model path updated: {legacy_model_path}")
    except Exception as exc:
        logger.warning(f"Failed to update legacy model path: {exc}")


def main() -> None:
    args = parse_args()
    try:
        asyncio.run(main_async(args))
    except PipelineError as exc:
        logger.error(str(exc))
        sys.exit(1)
    except Exception as exc:
        logger.exception(f"unexpected training failure: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
