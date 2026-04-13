"""Cross-sectional crypto factor library."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd


FACTOR_CATALOG: List[Dict[str, Any]] = [
    {"id": "MKT", "name": "市场因子", "description": "全市场等权收益", "direction": "long_market"},
    {"id": "SMB", "name": "规模因子", "description": "小市值(低成交额)减大市值(高成交额)", "direction": "small_minus_big"},
    {"id": "HML", "name": "价值因子(HML)", "description": "低估减高估（High minus Low）", "direction": "value_minus_growth"},
    {"id": "MOM", "name": "动量因子", "description": "强势币减弱势币", "direction": "winners_minus_losers"},
    {"id": "REV", "name": "反转因子", "description": "短期反转(前一周期弱势减强势)", "direction": "losers_minus_winners"},
    {"id": "VOL", "name": "低波因子", "description": "低波组合减高波组合", "direction": "low_vol_minus_high_vol"},
    {"id": "LIQ", "name": "流动性因子", "description": "高流动性减低流动性", "direction": "liquid_minus_illiquid"},
    {"id": "VAL", "name": "价值因子", "description": "低估(低于长期均线)减高估", "direction": "cheap_minus_expensive"},
    {"id": "RMW", "name": "盈利质量因子(RMW)", "description": "高盈利质量减低盈利质量（Robust minus Weak）", "direction": "robust_minus_weak"},
    {"id": "CMA", "name": "投资强度因子(CMA)", "description": "保守扩张减激进扩张（Conservative minus Aggressive）", "direction": "conservative_minus_aggressive"},
    {"id": "QMJ", "name": "质量因子", "description": "高质量(高滚动夏普)减低质量", "direction": "quality_minus_junk"},
    {"id": "BAB", "name": "低贝塔因子", "description": "低贝塔减高贝塔", "direction": "low_beta_minus_high_beta"},
    {"id": "MOMF", "name": "快动量", "description": "短周期强势币减弱势币", "direction": "fast_momentum"},
    {"id": "MOMS", "name": "慢动量", "description": "中长周期动量延续", "direction": "slow_momentum"},
    {"id": "REVF", "name": "快反转", "description": "超短期弱势币反转", "direction": "fast_reversal"},
    {"id": "TRND", "name": "趋势强度", "description": "EMA快慢线趋势强度", "direction": "trend_strength"},
    {"id": "ACC", "name": "动量加速度", "description": "短周期动量相对中周期动量的加速", "direction": "momentum_acceleration"},
    {"id": "PERS", "name": "收益持续性", "description": "正收益命中率持续性", "direction": "return_persistence"},
    {"id": "SKEW", "name": "偏度因子", "description": "偏度更优资产减偏度较差资产", "direction": "higher_skew"},
    {"id": "KURT", "name": "尾部风险因子", "description": "低峰度(尾部风险低)减高峰度", "direction": "low_kurtosis"},
    {"id": "DSV", "name": "下行波动因子", "description": "低下行波动减高下行波动", "direction": "low_downside_vol"},
    {"id": "USV", "name": "上行下行比", "description": "上行收益相对下行波动更优", "direction": "upside_vs_downside"},
    {"id": "IVOL", "name": "特质波动因子", "description": "低特质波动减高特质波动", "direction": "low_idiosyncratic_vol"},
    {"id": "BSTB", "name": "贝塔稳定因子", "description": "贝塔更稳定减波动更大的贝塔", "direction": "beta_stability"},
    {"id": "CORR", "name": "低相关因子", "description": "低市场相关减高市场相关", "direction": "low_market_correlation"},
    {"id": "VOV", "name": "波动率之波动", "description": "低波动率变化减高波动率变化", "direction": "low_vol_of_vol"},
    {"id": "RVOL", "name": "相对成交量", "description": "短期成交量相对长期成交量放大", "direction": "relative_volume"},
    {"id": "TURN", "name": "换手趋势", "description": "成交额变化趋势", "direction": "turnover_trend"},
    {"id": "FLOW", "name": "量价流向", "description": "方向化成交量流向强弱", "direction": "signed_volume_flow"},
    {"id": "MDD", "name": "回撤韧性", "description": "低滚动回撤减高滚动回撤", "direction": "low_drawdown"},
    {"id": "RPOS", "name": "区间位置", "description": "价格位于滚动区间上沿程度", "direction": "range_position"},
]


@dataclass
class FactorResult:
    factors: pd.DataFrame
    asset_scores: pd.DataFrame


def _safe_numeric(df: pd.DataFrame) -> pd.DataFrame:
    out = df.apply(pd.to_numeric, errors="coerce")
    out = out.replace([np.inf, -np.inf], np.nan)
    return out


def _long_short_factor(
    metric_df: pd.DataFrame,
    returns_df: pd.DataFrame,
    quantile: float = 0.3,
    long_high: bool = True,
) -> pd.Series:
    quantile = max(0.05, min(float(quantile or 0.3), 0.49))
    values: List[float] = []
    idx = metric_df.index.intersection(returns_df.index)

    for ts in idx:
        mrow = metric_df.loc[ts]
        rrow = returns_df.loc[ts]
        valid = mrow.dropna().index.intersection(rrow.dropna().index)

        if len(valid) < 2:
            values.append(0.0)
            continue

        k = max(1, int(len(valid) * quantile))
        ranked = mrow.loc[valid].sort_values(ascending=True)
        low = list(ranked.head(k).index)
        high = list(ranked.tail(k).index)

        if long_high:
            long_leg, short_leg = high, low
        else:
            long_leg, short_leg = low, high

        long_ret = float(rrow.loc[long_leg].mean()) if long_leg else 0.0
        short_ret = float(rrow.loc[short_leg].mean()) if short_leg else 0.0
        values.append(long_ret - short_ret)

    return pd.Series(values, index=idx, dtype=float)


def _rolling_beta(returns_df: pd.DataFrame, market_ret: pd.Series, window: int) -> pd.DataFrame:
    m = pd.to_numeric(market_ret, errors="coerce").fillna(0.0)
    mvar = m.rolling(window, min_periods=max(10, window // 2)).var().replace(0, np.nan)

    out = pd.DataFrame(index=returns_df.index, columns=returns_df.columns, dtype=float)
    for col in returns_df.columns:
        cov = returns_df[col].rolling(window, min_periods=max(10, window // 2)).cov(m)
        out[col] = cov / mvar
    return out


def _rolling_corr(returns_df: pd.DataFrame, market_ret: pd.Series, window: int) -> pd.DataFrame:
    m = pd.to_numeric(market_ret, errors="coerce").fillna(0.0)
    minp = max(10, window // 2)
    out = pd.DataFrame(index=returns_df.index, columns=returns_df.columns, dtype=float)
    for col in returns_df.columns:
        out[col] = returns_df[col].rolling(window, min_periods=minp).corr(m)
    return out


def _timeframe_to_seconds(timeframe: str) -> int:
    tf = str(timeframe or "1h").strip().lower()
    if not tf:
        return 3600
    unit = tf[-1]
    try:
        val = max(1, int(tf[:-1]))
    except Exception:
        return 3600
    if unit == "s":
        return val
    if unit == "m":
        return val * 60
    if unit == "h":
        return val * 3600
    if unit == "d":
        return val * 86400
    return 3600


def _factor_window_config(timeframe: str = "1h", n_obs: int = 0) -> Dict[str, int]:
    tf_sec = max(1, _timeframe_to_seconds(timeframe))
    n = max(120, int(n_obs or 0))

    def bars(hours: float, *, floor: int = 2, cap_ratio: float = 0.45) -> int:
        raw = int(round(float(hours) * 3600.0 / tf_sec))
        cap = max(floor + 1, int(n * cap_ratio))
        return max(floor, min(max(floor, raw), cap))

    cfg = {
        "mom_fast": bars(4, floor=2),
        "mom_mid": bars(24, floor=4),
        "mom_slow": bars(24 * 7, floor=8, cap_ratio=0.38),
        "rev_fast": bars(1, floor=2),
        "vol_short": bars(6, floor=4),
        "vol_mid": bars(24, floor=8),
        "vol_long": bars(72, floor=12, cap_ratio=0.38),
        "beta": bars(48, floor=10, cap_ratio=0.40),
        "beta_stab": bars(24, floor=8),
        "ema_fast": bars(8, floor=3),
        "ema_slow": bars(48, floor=8, cap_ratio=0.35),
        "value": bars(72, floor=12, cap_ratio=0.38),
        "quality": bars(48, floor=10),
        "profitability": bars(72, floor=12, cap_ratio=0.38),
        "investment": bars(24, floor=8),
        "size": bars(24, floor=8),
        "persistence": bars(12, floor=6),
        "drawdown": bars(72, floor=12, cap_ratio=0.38),
        "range": bars(48, floor=10),
        "liq_short": bars(6, floor=4),
        "liq_long": bars(48, floor=8, cap_ratio=0.35),
        "vov": bars(24, floor=6),
        "idio": bars(48, floor=10, cap_ratio=0.35),
    }
    for k, v in list(cfg.items()):
        cfg[k] = int(max(2, min(v, max(3, n - 2))))
    return cfg


def _zscore_series(series: pd.Series) -> pd.Series:
    x = pd.to_numeric(series, errors="coerce")
    mu = float(x.mean()) if len(x.dropna()) else 0.0
    sd = float(x.std()) if len(x.dropna()) else 0.0
    if sd <= 1e-12:
        return pd.Series(0.0, index=x.index)
    return (x - mu) / sd


def _latest_asset_scores(
    close_df: pd.DataFrame,
    volume_df: pd.DataFrame,
    returns_df: pd.DataFrame,
    market_ret: pd.Series,
    timeframe: str = "1h",
) -> pd.DataFrame:
    if close_df.empty:
        return pd.DataFrame(columns=["symbol", "score"])

    cfg = _factor_window_config(timeframe=timeframe, n_obs=len(close_df))
    mom = close_df / close_df.shift(cfg["mom_mid"]) - 1.0
    value = -(close_df / close_df.rolling(cfg["value"], min_periods=max(6, cfg["value"] // 2)).mean() - 1.0)
    quality = returns_df.rolling(cfg["quality"], min_periods=max(6, cfg["quality"] // 2)).mean() / returns_df.rolling(
        cfg["quality"], min_periods=max(6, cfg["quality"] // 2)
    ).std().replace(0, np.nan)
    profitability = returns_df.rolling(cfg["profitability"], min_periods=max(6, cfg["profitability"] // 2)).mean()
    low_vol = -returns_df.rolling(cfg["vol_mid"], min_periods=max(6, cfg["vol_mid"] // 2)).std()
    dollar_vol = (close_df * volume_df).replace(0, np.nan)
    investment = -(dollar_vol.rolling(cfg["investment"], min_periods=max(6, cfg["investment"] // 2)).mean().pct_change(cfg["investment"]))
    illiq = (returns_df.abs() / dollar_vol).rolling(cfg["liq_short"], min_periods=max(4, cfg["liq_short"] // 2)).mean()
    liquidity = -illiq
    beta = -_rolling_beta(returns_df, market_ret, window=cfg["beta"])
    size = -np.log(dollar_vol.rolling(cfg["size"], min_periods=max(6, cfg["size"] // 2)).mean())

    latest = {
        "MOM": mom.iloc[-1],
        "VAL": value.iloc[-1],
        "HML": value.iloc[-1],
        "QMJ": quality.iloc[-1],
        "RMW": profitability.iloc[-1],
        "CMA": investment.iloc[-1],
        "VOL": low_vol.iloc[-1],
        "LIQ": liquidity.iloc[-1],
        "BAB": beta.iloc[-1],
        "SMB": size.iloc[-1],
    }

    z = {k: _zscore_series(v) for k, v in latest.items()}
    symbols = close_df.columns

    score = (
        z["MOM"] * 0.20
        + z["VAL"] * 0.15
        + z["HML"] * 0.05
        + z["QMJ"] * 0.15
        + z["RMW"] * 0.15
        + z["CMA"] * 0.10
        + z["VOL"] * 0.08
        + z["LIQ"] * 0.07
        + z["BAB"] * 0.05
    )

    out = pd.DataFrame(
        {
            "symbol": symbols,
            "score": score.reindex(symbols).fillna(0.0).values,
            "momentum": z["MOM"].reindex(symbols).fillna(0.0).values,
            "value": z["VAL"].reindex(symbols).fillna(0.0).values,
            "value_hml": z["HML"].reindex(symbols).fillna(0.0).values,
            "quality": z["QMJ"].reindex(symbols).fillna(0.0).values,
            "profitability": z["RMW"].reindex(symbols).fillna(0.0).values,
            "investment": z["CMA"].reindex(symbols).fillna(0.0).values,
            "low_vol": z["VOL"].reindex(symbols).fillna(0.0).values,
            "liquidity": z["LIQ"].reindex(symbols).fillna(0.0).values,
            "low_beta": z["BAB"].reindex(symbols).fillna(0.0).values,
            "size": z["SMB"].reindex(symbols).fillna(0.0).values,
        }
    )
    out = out.sort_values("score", ascending=False).reset_index(drop=True)
    return out


def build_factor_library(
    close_df: pd.DataFrame,
    volume_df: pd.DataFrame,
    quantile: float = 0.3,
    timeframe: str = "1h",
) -> FactorResult:
    close_df = _safe_numeric(close_df).sort_index()
    volume_df = _safe_numeric(volume_df).sort_index()

    common_cols = [c for c in close_df.columns if c in volume_df.columns]
    if not common_cols:
        return FactorResult(factors=pd.DataFrame(), asset_scores=pd.DataFrame())

    close_df = close_df[common_cols]
    volume_df = volume_df[common_cols]

    returns_df = close_df.pct_change(fill_method=None).replace([np.inf, -np.inf], np.nan)
    market_ret = returns_df.mean(axis=1).fillna(0.0)

    dollar_vol = (close_df * volume_df).replace(0, np.nan)

    cfg = _factor_window_config(timeframe=timeframe, n_obs=len(close_df))
    mps = lambda w, floor=4: max(floor, int(w) // 2)  # noqa: E731

    metric_size = -np.log(dollar_vol.rolling(cfg["size"], min_periods=mps(cfg["size"], 6)).mean())
    metric_mom = close_df / close_df.shift(cfg["mom_mid"]) - 1.0
    metric_mom_fast = close_df / close_df.shift(cfg["mom_fast"]) - 1.0
    metric_mom_slow = close_df / close_df.shift(cfg["mom_slow"]) - 1.0
    metric_rev = -returns_df.shift(1)
    metric_rev_fast = -(close_df / close_df.shift(cfg["rev_fast"]) - 1.0)
    metric_vol = -returns_df.rolling(cfg["vol_mid"], min_periods=mps(cfg["vol_mid"], 6)).std()
    amihud = (returns_df.abs() / dollar_vol).rolling(cfg["liq_short"], min_periods=mps(cfg["liq_short"], 4)).mean()
    metric_liq = -amihud
    metric_val = -(close_df / close_df.rolling(cfg["value"], min_periods=mps(cfg["value"], 8)).mean() - 1.0)
    metric_qmj = returns_df.rolling(cfg["quality"], min_periods=mps(cfg["quality"], 8)).mean() / returns_df.rolling(
        cfg["quality"], min_periods=mps(cfg["quality"], 8)
    ).std().replace(0, np.nan)
    metric_rmw = returns_df.rolling(cfg["profitability"], min_periods=mps(cfg["profitability"], 8)).mean()
    metric_cma = -(dollar_vol.rolling(cfg["investment"], min_periods=mps(cfg["investment"], 6)).mean().pct_change(cfg["investment"]))
    beta_df = _rolling_beta(returns_df, market_ret, window=cfg["beta"])
    corr_df = _rolling_corr(returns_df, market_ret, window=cfg["beta"])
    metric_bab = -beta_df

    ema_fast = close_df.ewm(span=cfg["ema_fast"], adjust=False, min_periods=max(3, cfg["ema_fast"] // 2)).mean()
    ema_slow = close_df.ewm(span=cfg["ema_slow"], adjust=False, min_periods=max(4, cfg["ema_slow"] // 2)).mean()
    metric_trnd = (ema_fast / ema_slow.replace(0, np.nan)) - 1.0
    metric_acc = metric_mom_fast - metric_mom
    metric_pers = returns_df.gt(0).astype(float).rolling(cfg["persistence"], min_periods=mps(cfg["persistence"], 4)).mean() - 0.5
    metric_skew = returns_df.rolling(cfg["vol_mid"], min_periods=mps(cfg["vol_mid"], 8)).skew()
    metric_kurt = -returns_df.rolling(cfg["vol_mid"], min_periods=mps(cfg["vol_mid"], 8)).kurt()
    downside = returns_df.clip(upper=0).abs()
    metric_dsv = -np.sqrt((downside**2).rolling(cfg["vol_mid"], min_periods=mps(cfg["vol_mid"], 8)).mean())
    upside_mean = returns_df.clip(lower=0).rolling(cfg["vol_mid"], min_periods=mps(cfg["vol_mid"], 8)).mean()
    downside_mean = downside.rolling(cfg["vol_mid"], min_periods=mps(cfg["vol_mid"], 8)).mean().replace(0, np.nan)
    metric_usv = upside_mean / downside_mean
    resid = returns_df.sub(beta_df.mul(market_ret, axis=0), fill_value=0.0)
    metric_ivol = -resid.rolling(cfg["idio"], min_periods=mps(cfg["idio"], 8)).std()
    metric_bstb = -(beta_df.rolling(cfg["beta_stab"], min_periods=mps(cfg["beta_stab"], 6)).std())
    metric_corr = -corr_df
    rolling_vol_short = returns_df.rolling(cfg["vol_short"], min_periods=mps(cfg["vol_short"], 4)).std()
    metric_vov = -(rolling_vol_short.rolling(cfg["vov"], min_periods=mps(cfg["vov"], 4)).std())
    dv_short = dollar_vol.rolling(cfg["liq_short"], min_periods=mps(cfg["liq_short"], 4)).mean()
    dv_long = dollar_vol.rolling(cfg["liq_long"], min_periods=mps(cfg["liq_long"], 6)).mean()
    metric_rvol = (dv_short / dv_long.replace(0, np.nan)) - 1.0
    metric_turn = dv_short.pct_change(max(1, cfg["liq_short"] // 2))
    signed_flow = np.sign(returns_df.fillna(0.0)) * np.log1p(volume_df.clip(lower=0.0))
    metric_flow = signed_flow.rolling(cfg["liq_short"], min_periods=mps(cfg["liq_short"], 4)).mean()
    rolling_peak = close_df.rolling(cfg["drawdown"], min_periods=mps(cfg["drawdown"], 8)).max()
    metric_mdd = (close_df / rolling_peak.replace(0, np.nan)) - 1.0
    rolling_low = close_df.rolling(cfg["range"], min_periods=mps(cfg["range"], 8)).min()
    rolling_high = close_df.rolling(cfg["range"], min_periods=mps(cfg["range"], 8)).max()
    metric_rpos = ((close_df - rolling_low) / (rolling_high - rolling_low).replace(0, np.nan)) - 0.5

    factors = pd.DataFrame(index=returns_df.index)
    factors["MKT"] = market_ret
    factors["SMB"] = _long_short_factor(metric_size, returns_df, quantile=quantile, long_high=True)
    factors["HML"] = _long_short_factor(metric_val, returns_df, quantile=quantile, long_high=True)
    factors["MOM"] = _long_short_factor(metric_mom, returns_df, quantile=quantile, long_high=True)
    factors["REV"] = _long_short_factor(metric_rev, returns_df, quantile=quantile, long_high=True)
    factors["VOL"] = _long_short_factor(metric_vol, returns_df, quantile=quantile, long_high=True)
    factors["LIQ"] = _long_short_factor(metric_liq, returns_df, quantile=quantile, long_high=True)
    factors["VAL"] = _long_short_factor(metric_val, returns_df, quantile=quantile, long_high=True)
    factors["RMW"] = _long_short_factor(metric_rmw, returns_df, quantile=quantile, long_high=True)
    factors["CMA"] = _long_short_factor(metric_cma, returns_df, quantile=quantile, long_high=True)
    factors["QMJ"] = _long_short_factor(metric_qmj, returns_df, quantile=quantile, long_high=True)
    factors["BAB"] = _long_short_factor(metric_bab, returns_df, quantile=quantile, long_high=True)
    factors["MOMF"] = _long_short_factor(metric_mom_fast, returns_df, quantile=quantile, long_high=True)
    factors["MOMS"] = _long_short_factor(metric_mom_slow, returns_df, quantile=quantile, long_high=True)
    factors["REVF"] = _long_short_factor(metric_rev_fast, returns_df, quantile=quantile, long_high=True)
    factors["TRND"] = _long_short_factor(metric_trnd, returns_df, quantile=quantile, long_high=True)
    factors["ACC"] = _long_short_factor(metric_acc, returns_df, quantile=quantile, long_high=True)
    factors["PERS"] = _long_short_factor(metric_pers, returns_df, quantile=quantile, long_high=True)
    factors["SKEW"] = _long_short_factor(metric_skew, returns_df, quantile=quantile, long_high=True)
    factors["KURT"] = _long_short_factor(metric_kurt, returns_df, quantile=quantile, long_high=True)
    factors["DSV"] = _long_short_factor(metric_dsv, returns_df, quantile=quantile, long_high=True)
    factors["USV"] = _long_short_factor(metric_usv, returns_df, quantile=quantile, long_high=True)
    factors["IVOL"] = _long_short_factor(metric_ivol, returns_df, quantile=quantile, long_high=True)
    factors["BSTB"] = _long_short_factor(metric_bstb, returns_df, quantile=quantile, long_high=True)
    factors["CORR"] = _long_short_factor(metric_corr, returns_df, quantile=quantile, long_high=True)
    factors["VOV"] = _long_short_factor(metric_vov, returns_df, quantile=quantile, long_high=True)
    factors["RVOL"] = _long_short_factor(metric_rvol, returns_df, quantile=quantile, long_high=True)
    factors["TURN"] = _long_short_factor(metric_turn, returns_df, quantile=quantile, long_high=True)
    factors["FLOW"] = _long_short_factor(metric_flow, returns_df, quantile=quantile, long_high=True)
    factors["MDD"] = _long_short_factor(metric_mdd, returns_df, quantile=quantile, long_high=True)
    factors["RPOS"] = _long_short_factor(metric_rpos, returns_df, quantile=quantile, long_high=True)
    factors = factors.fillna(0.0)

    asset_scores = _latest_asset_scores(close_df, volume_df, returns_df, market_ret, timeframe=timeframe)
    return FactorResult(factors=factors, asset_scores=asset_scores)
