"""ML signal model training script.

Offline training pipeline:
  OHLCV data → compute 17 technical features → generate N-bar forward labels
  → train XGBoost binary classifier → evaluate → save model

Usage::

    python scripts/train_ml_signal.py \\
        --exchange binance --symbol BTC/USDT --timeframe 1h \\
        --forward-bars 4 --days 365 \\
        --output models/ml_signal_xgb.json

Requires: xgboost, scikit-learn (for metrics)
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional

# ---------------------------------------------------------------------------
# Add project root to path so relative imports work when run as a script
# ---------------------------------------------------------------------------
_HERE = Path(__file__).resolve()
_ROOT = _HERE.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import numpy as np
import pandas as pd
from loguru import logger


# ---------------------------------------------------------------------------
# Feature engineering
# ---------------------------------------------------------------------------

def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False, min_periods=max(1, span // 2)).mean()


def _sma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window, min_periods=max(1, window // 2)).mean()


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute 17 technical indicator features from OHLCV DataFrame."""
    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    volume = df["volume"].astype(float)

    out = pd.DataFrame(index=df.index)
    out["open"] = df["open"].astype(float)
    out["high"] = high
    out["low"] = low
    out["close"] = close
    out["volume"] = volume

    # RSI-14
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=13, adjust=False, min_periods=14).mean()
    avg_loss = loss.ewm(com=13, adjust=False, min_periods=14).mean()
    rs = avg_gain / (avg_loss + 1e-9)
    out["rsi"] = 100.0 - (100.0 / (1.0 + rs))

    # MACD (12, 26, 9)
    ema12 = _ema(close, 12)
    ema26 = _ema(close, 26)
    macd_line = ema12 - ema26
    macd_signal = _ema(macd_line, 9)
    out["macd"] = macd_line
    out["macd_signal"] = macd_signal
    out["macd_hist"] = macd_line - macd_signal

    # EMA fast/slow
    out["ema_fast"] = _ema(close, 8)
    out["ema_slow"] = _ema(close, 21)

    # Bollinger Bands (20, 2)
    bb_mid = _sma(close, 20)
    bb_std = close.rolling(20, min_periods=10).std()
    out["bb_mid"] = bb_mid
    out["bb_upper"] = bb_mid + 2.0 * bb_std
    out["bb_lower"] = bb_mid - 2.0 * bb_std

    # ATR-14
    hl = high - low
    hpc = (high - close.shift(1)).abs()
    lpc = (low - close.shift(1)).abs()
    tr = pd.concat([hl, hpc, lpc], axis=1).max(axis=1)
    out["atr"] = tr.ewm(com=13, adjust=False, min_periods=14).mean()

    # Volume ratio (current / 20-bar avg)
    vol_ma = _sma(volume, 20)
    out["volume_ratio"] = volume / (vol_ma + 1e-9)

    # Momentum (14-bar)
    out["momentum"] = close.pct_change(14)

    return out


def generate_labels(close: pd.Series, forward_bars: int = 4) -> pd.Series:
    """Label = 1 if price is higher after *forward_bars* bars, else 0."""
    fwd_return = close.shift(-forward_bars) / close - 1.0
    return (fwd_return > 0).astype(int)


# ---------------------------------------------------------------------------
# Async data loading
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------

def train_and_evaluate(
    df: pd.DataFrame,
    forward_bars: int,
    test_size: float,
    n_estimators: int,
    max_depth: int,
    learning_rate: float,
    scale_pos_weight: float,
) -> Dict[str, Any]:
    try:
        import xgboost as xgb  # noqa: PLC0415
    except ImportError:
        raise ImportError("xgboost is required: pip install xgboost")
    try:
        from sklearn.metrics import (  # noqa: PLC0415
            classification_report,
            roc_auc_score,
        )
        from sklearn.model_selection import train_test_split  # noqa: PLC0415
    except ImportError:
        raise ImportError("scikit-learn is required: pip install scikit-learn")

    from core.ai.ml_signal import FEATURE_COLS  # noqa: PLC0415

    features = compute_features(df)
    labels = generate_labels(df["close"].astype(float), forward_bars=forward_bars)

    combined = features.copy()
    combined["_label"] = labels

    # drop rows where features or labels are NaN (first N bars, last N bars)
    combined = combined.dropna()
    X = combined[FEATURE_COLS].astype(float)
    y = combined["_label"].astype(int)

    if len(X) < 100:
        raise ValueError(
            f"Insufficient training samples after cleaning: {len(X)}.  "
            "Need at least 100 rows – try --days with a larger value."
        )

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, shuffle=False
    )

    pos = int(y_train.sum())
    neg = int(len(y_train) - pos)
    actual_spw = neg / max(pos, 1) if scale_pos_weight <= 0 else scale_pos_weight

    model = xgb.XGBClassifier(
        n_estimators=n_estimators,
        max_depth=max_depth,
        learning_rate=learning_rate,
        scale_pos_weight=actual_spw,
        use_label_encoder=False,
        eval_metric="logloss",
        verbosity=0,
        random_state=42,
    )
    eval_set = [(X_test, y_test)]
    model.fit(X_train, y_train, eval_set=eval_set, verbose=False)

    proba = model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, proba)
    preds = (proba >= 0.55).astype(int)
    report = classification_report(y_test, preds, output_dict=True)

    importances = {
        name: round(float(score), 6)
        for name, score in zip(FEATURE_COLS, model.feature_importances_)
    }

    return {
        "model": model,
        "train_samples": len(X_train),
        "test_samples": len(X_test),
        "auc": round(auc, 4),
        "classification_report": report,
        "feature_importances": importances,
        "pos_weight_used": round(actual_spw, 4),
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Train XGBoost ML signal model from local OHLCV data"
    )
    p.add_argument("--exchange", default="binance", help="Exchange name (default: binance)")
    p.add_argument("--symbol", default="BTC/USDT", help="Symbol (default: BTC/USDT)")
    p.add_argument("--timeframe", default="1h", help="Timeframe (default: 1h)")
    p.add_argument("--days", type=int, default=365, help="Lookback days (default: 365)")
    p.add_argument(
        "--forward-bars",
        type=int,
        default=4,
        dest="forward_bars",
        help="N bars ahead for label generation (default: 4)",
    )
    p.add_argument(
        "--output",
        default="models/ml_signal_xgb.json",
        help="Output model path (default: models/ml_signal_xgb.json)",
    )
    p.add_argument("--test-size", type=float, default=0.2, dest="test_size")
    p.add_argument("--n-estimators", type=int, default=300, dest="n_estimators")
    p.add_argument("--max-depth", type=int, default=5, dest="max_depth")
    p.add_argument("--learning-rate", type=float, default=0.05, dest="learning_rate")
    p.add_argument(
        "--scale-pos-weight",
        type=float,
        default=0.0,
        dest="scale_pos_weight",
        help="XGBoost scale_pos_weight (0 = auto)",
    )
    return p.parse_args()


async def main_async(args: argparse.Namespace) -> None:
    logger.info(
        f"Loading OHLCV: exchange={args.exchange} symbol={args.symbol} "
        f"timeframe={args.timeframe} days={args.days}"
    )
    df = await load_ohlcv(
        exchange=args.exchange,
        symbol=args.symbol,
        timeframe=args.timeframe,
        days=args.days,
    )

    if df is None or df.empty:
        logger.error(
            "No OHLCV data found.  "
            "Make sure you have downloaded historical data first "
            "(web UI → Data → Download Historical Data)."
        )
        sys.exit(1)

    logger.info(f"Loaded {len(df)} rows.  Training...")

    result = train_and_evaluate(
        df,
        forward_bars=args.forward_bars,
        test_size=args.test_size,
        n_estimators=args.n_estimators,
        max_depth=args.max_depth,
        learning_rate=args.learning_rate,
        scale_pos_weight=args.scale_pos_weight,
    )

    # ---------- metrics ----------
    rep = result["classification_report"]
    logger.info(
        f"Train samples={result['train_samples']}  "
        f"Test samples={result['test_samples']}  "
        f"AUC={result['auc']}"
    )
    logger.info(
        f"Precision={rep.get('1', {}).get('precision', 0):.4f}  "
        f"Recall={rep.get('1', {}).get('recall', 0):.4f}  "
        f"F1={rep.get('1', {}).get('f1-score', 0):.4f}"
    )

    top5 = sorted(
        result["feature_importances"].items(), key=lambda x: x[1], reverse=True
    )[:5]
    logger.info("Top-5 features: " + "  ".join(f"{k}={v:.4f}" for k, v in top5))

    # ---------- save ----------
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result["model"].save_model(str(output_path))
    logger.info(f"Model saved to {output_path}")


def main() -> None:
    args = parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
