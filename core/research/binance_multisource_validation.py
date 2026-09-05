"""Leakage-aware helpers for Binance extreme-run-up multisource research.

This module is deliberately isolated from the live strategy stack.  It maps
raw news with conservative token identifiers, constructs time-lagged features,
and compares each information family with the frozen price family on identical
rows and walk-forward folds.  It cannot place or route orders.
"""

from __future__ import annotations

import re
import importlib.util
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

_VALIDATION_PATH = Path(__file__).with_name("binance_runup_validation.py")
_SPEC = importlib.util.spec_from_file_location("binance_runup_validation_for_multisource", _VALIDATION_PATH)
if _SPEC is None or _SPEC.loader is None:
    raise RuntimeError(f"Unable to load {_VALIDATION_PATH}")
_VALIDATION = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _VALIDATION
_SPEC.loader.exec_module(_VALIDATION)

PRICE_FACTORS = _VALIDATION.PRICE_FACTORS
benjamini_hochberg = _VALIDATION.benjamini_hochberg
expanding_walk_forward_splits = _VALIDATION.expanding_walk_forward_splits
fit_frozen_logistic = _VALIDATION.fit_frozen_logistic
score_metrics = _VALIDATION.score_metrics


NEWS_FACTORS: tuple[str, ...] = (
    "news_count_1d",
    "news_count_3d",
    "news_count_7d",
    "news_source_breadth_7d",
    "news_official_7d",
    "news_listing_7d",
    "news_risk_7d",
    "news_acceleration_3d",
)

ONCHAIN_FACTORS: tuple[str, ...] = (
    "log_tvl_lag1d",
    "tvl_change_1d_lag1d",
    "tvl_change_7d_lag1d",
    "tvl_change_30d_lag1d",
)

_PREFIXES = ("1000000", "1000")
_PLAIN_CODE_STOPWORDS = {
    "ABOUT", "AFTER", "AGAIN", "BELOW", "BLACK", "BLOCK", "BREAK", "BUY",
    "CHAIN", "DAILY", "FIRST", "FOR", "FROM", "GREEN", "GROUP", "HOLD",
    "INTO", "LARGE", "LATER", "LEVEL", "MARKET", "MONEY", "MONTH", "MORE",
    "NEWS", "NEXT", "ONE", "OPEN", "PEOPLE", "PRICE", "PUBLIC", "RIGHT",
    "SHORT", "SMALL", "SPACE", "STATE", "STOCK", "THREE", "TOKEN", "TRADE",
    "TRUMP", "VALUE", "WORLD", "YEAR", "YIELD",
}
_GENERIC_NAMES = {
    "bitcoin", "crypto", "dollar", "ethereum", "money", "official", "token",
    "world", "yield",
}
_LISTING_RE = re.compile(
    r"\b(list(?:ed|ing)?|launchpool|launchpad|pre[- ]?market|spot listing|perpetual)\b|"
    r"上线|上币|首发|开放交易|永续合约|现货交易",
    re.IGNORECASE,
)
_OFFICIAL_CATALYST_RE = re.compile(
    r"\b(partner(?:ship)?|integration|mainnet|testnet|airdrop|staking|token burn|"
    r"fundrais(?:e|ing)|investment|acquisition|upgrade|migration)\b|"
    r"合作|集成|主网|测试网|空投|质押|销毁|融资|投资|收购|升级|迁移",
    re.IGNORECASE,
)
_RISK_RE = re.compile(
    r"\b(hack(?:ed)?|exploit|breach|delist(?:ed|ing)?|lawsuit|fraud|scam|"
    r"bankrupt|suspend(?:ed)?|rug pull)\b|"
    r"黑客|攻击|漏洞|下架|退市|诉讼|欺诈|骗局|破产|暂停|跑路",
    re.IGNORECASE,
)
_SUPPLY_FLOW_RE = re.compile(
    r"\b(token unlock|unlock(?:ed|ing)? tokens?|circulating supply|dealer.*transfer|"
    r"transfer(?:red|ring)?.{0,40}(?:binance|exchange)|deposit(?:ed|ing)?.{0,40}(?:binance|exchange))\b|"
    r"代币解锁|解锁代币|流通量|庄家.{0,20}转|转入.{0,20}(?:币安|交易所)|充值.{0,20}(?:币安|交易所)",
    re.IGNORECASE,
)


def normalized_base(symbol: str) -> str:
    """Return the economic token code for a Binance USDT contract symbol."""

    value = str(symbol).upper().strip()
    if value.endswith("USDT"):
        value = value[:-4]
    for prefix in _PREFIXES:
        if value.startswith(prefix) and len(value) > len(prefix):
            return value[len(prefix):]
    return value


@dataclass(frozen=True)
class TokenIdentity:
    contract_symbol: str
    base: str
    coingecko_id: str | None
    token_name: str | None
    mapping_status: str


def build_token_identities(
    contract_symbols: Iterable[str],
    coingecko_coins: Sequence[Mapping[str, Any]],
) -> pd.DataFrame:
    """Build conservative CoinGecko identities from unique symbol matches."""

    by_symbol: dict[str, list[Mapping[str, Any]]] = {}
    for coin in coingecko_coins:
        code = str(coin.get("symbol") or "").upper().strip()
        if code:
            by_symbol.setdefault(code, []).append(coin)
    records: list[dict[str, Any]] = []
    for contract_symbol in sorted(set(str(item) for item in contract_symbols)):
        base = normalized_base(contract_symbol)
        matches = by_symbol.get(base, [])
        unique = matches[0] if len(matches) == 1 else None
        records.append(
            {
                "contract_symbol": contract_symbol,
                "base": base,
                "coingecko_id": str(unique.get("id")) if unique else None,
                "token_name": str(unique.get("name")) if unique else None,
                "mapping_status": "unique" if unique else ("ambiguous" if matches else "missing"),
                "candidate_count": len(matches),
            }
        )
    return pd.DataFrame(records)


class StrictNewsMatcher:
    """Conservative title matcher that avoids short-code substring leakage."""

    def __init__(self, identities: pd.DataFrame):
        self.base_to_contracts: dict[str, list[str]] = {}
        self.name_to_contracts: dict[str, list[str]] = {}
        for row in identities.itertuples(index=False):
            self.base_to_contracts.setdefault(str(row.base).upper(), []).append(
                str(row.contract_symbol)
            )
            if row.mapping_status == "unique" and isinstance(row.token_name, str):
                name = row.token_name.strip()
                if len(name) >= 6 and name.casefold() not in _GENERIC_NAMES:
                    self.name_to_contracts.setdefault(name.casefold(), []).append(
                        str(row.contract_symbol)
                    )

        self._plain_codes = {
            code for code in self.base_to_contracts
            if len(code) >= 3 and code not in _PLAIN_CODE_STOPWORDS
        }
        self._uppercase_token_re = re.compile(
            r"(?<![A-Za-z0-9])([A-Z][A-Z0-9]{2,14})(?![A-Za-z0-9])"
        )
        self._pair_re = re.compile(r"(?<![A-Z0-9])([A-Z0-9]{2,15})USDT(?![A-Z0-9])", re.I)
        self._dollar_re = re.compile(r"\$([A-Z0-9]{2,15})(?![A-Z0-9])", re.I)
        self._paren_re = re.compile(r"\(([A-Z0-9]{2,15})\)")

    def match(self, title: str) -> list[str]:
        text = str(title or "")
        bases: set[str] = set()
        for pattern in (self._pair_re, self._dollar_re, self._paren_re):
            bases.update(match.group(1).upper() for match in pattern.finditer(text))
        bases.update(
            match.group(1) for match in self._uppercase_token_re.finditer(text)
            if match.group(1) in self._plain_codes
        )

        contracts: set[str] = set()
        for base in bases:
            contracts.update(self.base_to_contracts.get(base, []))
        return sorted(contracts)


def _alternation_pattern(values: Iterable[str], *, case_sensitive: bool) -> re.Pattern[str] | None:
    cleaned = sorted({str(value) for value in values if str(value)}, key=len, reverse=True)
    if not cleaned:
        return None
    alternatives = "|".join(re.escape(value) for value in cleaned)
    flags = 0 if case_sensitive else re.IGNORECASE
    return re.compile(rf"(?<![A-Za-z0-9])({alternatives})(?![A-Za-z0-9])", flags)


def classify_news(title: str, source: str, url: str) -> dict[str, bool]:
    text = str(title or "")
    source_text = str(source or "").casefold()
    url_text = str(url or "").casefold()
    official_exchange = "binance" in source_text or "binance.com" in url_text
    listing = bool(_LISTING_RE.search(text))
    supply_flow = bool(_SUPPLY_FLOW_RE.search(text))
    return {
        "is_official": official_exchange,
        "is_listing": listing,
        "is_catalyst": bool(listing or _OFFICIAL_CATALYST_RE.search(text)),
        "is_risk": bool(_RISK_RE.search(text) or supply_flow),
        "is_supply_flow": supply_flow,
    }


def map_raw_news(raw: pd.DataFrame, matcher: StrictNewsMatcher) -> pd.DataFrame:
    """Map deduplicated raw news using fetched time as the availability clock."""

    if raw.empty:
        return pd.DataFrame()
    news = raw.copy()
    news["available_at"] = pd.to_datetime(news["fetched_at"], utc=True, errors="coerce")
    news["published_at"] = pd.to_datetime(news["published_at"], utc=True, errors="coerce")
    news = news.dropna(subset=["available_at", "title"])
    dedupe_columns = [column for column in ("content_hash", "url") if column in news]
    if dedupe_columns:
        news = news.sort_values("available_at").drop_duplicates(dedupe_columns, keep="first")
    records: list[dict[str, Any]] = []
    for row in news.itertuples(index=False):
        symbols = matcher.match(str(row.title))
        if not symbols:
            continue
        flags = classify_news(str(row.title), str(row.source), str(row.url))
        source_key = re.sub(r"[^a-z0-9]+", "_", str(row.source).casefold()).strip("_") or "unknown"
        for symbol in symbols:
            records.append(
                {
                    "news_id": int(row.id),
                    "symbol": symbol,
                    "available_at": row.available_at,
                    "published_at": row.published_at,
                    "source": str(row.source),
                    "source_key": source_key,
                    "title": str(row.title),
                    "url": str(row.url),
                    **flags,
                }
            )
    return pd.DataFrame(records)


def add_news_features(rows: pd.DataFrame, mentions: pd.DataFrame) -> pd.DataFrame:
    """Attach rolling news features observable by the 00:20 UTC signal cutoff."""

    output = rows.copy()
    output["date"] = pd.to_datetime(output["date"], utc=True).dt.normalize()
    feature_columns = list(NEWS_FACTORS) + ["news_catalyst_7d", "news_history_ready"]
    if mentions.empty:
        for column in feature_columns:
            output[column] = False if column == "news_history_ready" else 0.0
        return output

    first_available = pd.Timestamp(mentions["available_at"].min())
    events = mentions.copy()
    events["date"] = (
        pd.to_datetime(events["available_at"], utc=True) - pd.Timedelta(minutes=20)
    ).dt.normalize()
    daily = events.groupby(["symbol", "date"], as_index=False).agg(
        news_count_1d=("news_id", "size"),
        official_daily=("is_official", "sum"),
        listing_daily=("is_listing", "sum"),
        catalyst_daily=("is_catalyst", "sum"),
        risk_daily=("is_risk", "sum"),
    )
    grid = output[["symbol", "date"]].drop_duplicates().sort_values(["symbol", "date"])
    grid = grid.merge(daily, on=["symbol", "date"], how="left")
    daily_columns = ["news_count_1d", "official_daily", "listing_daily", "catalyst_daily", "risk_daily"]
    grid[daily_columns] = grid[daily_columns].fillna(0.0)
    grouped = grid.groupby("symbol", sort=False)
    grid["news_count_3d"] = grouped["news_count_1d"].transform(
        lambda values: values.rolling(3, min_periods=1).sum()
    )
    grid["news_count_7d"] = grouped["news_count_1d"].transform(
        lambda values: values.rolling(7, min_periods=1).sum()
    )
    grid["news_official_7d"] = grouped["official_daily"].transform(
        lambda values: values.rolling(7, min_periods=1).sum()
    )
    grid["news_listing_7d"] = grouped["listing_daily"].transform(
        lambda values: values.rolling(7, min_periods=1).sum()
    )
    grid["news_catalyst_7d"] = grouped["catalyst_daily"].transform(
        lambda values: values.rolling(7, min_periods=1).sum()
    )
    grid["news_risk_7d"] = grouped["risk_daily"].transform(
        lambda values: values.rolling(7, min_periods=1).sum()
    )
    prior_three = grouped["news_count_1d"].transform(
        lambda values: values.shift(3).rolling(3, min_periods=1).sum()
    ).fillna(0.0)
    grid["news_acceleration_3d"] = grid["news_count_3d"] - prior_three

    source_daily = events[["symbol", "source_key", "date"]].drop_duplicates()
    expanded = pd.concat(
        [source_daily.assign(date=source_daily["date"] + pd.Timedelta(days=offset)) for offset in range(7)],
        ignore_index=True,
    ).drop_duplicates(["symbol", "source_key", "date"])
    breadth = expanded.groupby(["symbol", "date"])["source_key"].nunique().rename(
        "news_source_breadth_7d"
    ).reset_index()
    grid = grid.merge(breadth, on=["symbol", "date"], how="left")
    grid["news_source_breadth_7d"] = grid["news_source_breadth_7d"].fillna(0.0)
    grid["news_history_ready"] = (
        grid["date"] + pd.Timedelta(days=1, minutes=20)
        >= first_available + pd.Timedelta(days=7)
    )
    return output.merge(
        grid[["symbol", "date", *feature_columns]],
        on=["symbol", "date"],
        how="left",
        validate="many_to_one",
    )


def build_lagged_tvl_features(tvl: pd.DataFrame) -> pd.DataFrame:
    """Create one-day-lagged DefiLlama TVL features without filling gaps as zero."""

    if tvl.empty:
        return pd.DataFrame()
    output: list[pd.DataFrame] = []
    for symbol, raw_group in tvl.groupby("symbol", sort=False):
        group = raw_group.copy()
        group["source_date"] = pd.to_datetime(group["source_date"], utc=True).dt.normalize()
        group = group.sort_values("source_date").drop_duplicates("source_date", keep="last")
        group["tvl"] = pd.to_numeric(group["tvl"], errors="coerce")
        group["log_tvl_lag1d"] = np.log1p(group["tvl"].clip(lower=0))
        for days in (1, 7, 30):
            group[f"tvl_change_{days}d_lag1d"] = group["tvl"].pct_change(days, fill_method=None)
        group["date"] = group["source_date"] + pd.Timedelta(days=1)
        group["symbol"] = symbol
        output.append(group[["symbol", "date", *ONCHAIN_FACTORS]])
    return pd.concat(output, ignore_index=True)


def add_tvl_features(rows: pd.DataFrame, tvl_features: pd.DataFrame) -> pd.DataFrame:
    output = rows.copy()
    output["date"] = pd.to_datetime(output["date"], utc=True).dt.normalize()
    if tvl_features.empty:
        for factor in ONCHAIN_FACTORS:
            output[factor] = np.nan
        return output
    return output.merge(tvl_features, on=["symbol", "date"], how="left", validate="many_to_one")


def run_family_walk_forward(
    rows: pd.DataFrame,
    family_factors: Sequence[str],
    *,
    family_name: str,
    eligibility_column: str | None = None,
    minimum_family_train_days: int = 30,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Compare price, family-only and combined models on identical fold rows."""

    data = rows.copy().reset_index(drop=True)
    data["date"] = pd.to_datetime(data["date"], utc=True)
    folds = expanding_walk_forward_splits(data)
    scored_frames: list[pd.DataFrame] = []
    records: list[dict[str, Any]] = []
    family_factors = tuple(family_factors)
    for fold in folds:
        train = data.loc[fold["train_index"]].copy()
        test = data.loc[fold["test_index"]].copy()
        if eligibility_column:
            train = train[train[eligibility_column].fillna(False)]
            test = test[test[eligibility_column].fillna(False)]
        else:
            required = list(family_factors)
            train = train.dropna(subset=required)
            test = test.dropna(subset=required)
        if train.empty or test.empty:
            continue
        train_span = int((train["date"].max() - train["date"].min()).days) + 1
        if (
            train_span < minimum_family_train_days
            or int(train["label_primary_int"].sum()) < 10
            or int(test["label_primary_int"].sum()) < 3
        ):
            continue
        price_model = fit_frozen_logistic(train, PRICE_FACTORS)
        family_model = fit_frozen_logistic(train, family_factors)
        combo_model = fit_frozen_logistic(train, tuple(PRICE_FACTORS) + family_factors)
        test["price_same_score"] = price_model.predict_score(test)
        test[f"{family_name}_score"] = family_model.predict_score(test)
        test[f"price_{family_name}_score"] = combo_model.predict_score(test)
        for column in ("price_same_score", f"{family_name}_score", f"price_{family_name}_score"):
            test[f"{column}_pctile"] = test[column].groupby(test["date"]).rank(
                pct=True, method="first"
            )
        test["fold"] = int(fold["fold"])
        scored_frames.append(test)
        price_metrics = score_metrics(test, "price_same_score", "label_primary_int")
        family_metrics = score_metrics(test, f"{family_name}_score", "label_primary_int")
        combo_metrics = score_metrics(test, f"price_{family_name}_score", "label_primary_int")
        records.append(
            {
                "family": family_name,
                "fold": int(fold["fold"]),
                "train_start": train["date"].min(),
                "train_end": train["date"].max(),
                "test_start": test["date"].min(),
                "test_end": test["date"].max(),
                "train_rows": int(len(train)),
                "train_positives": int(train["label_primary_int"].sum()),
                "test_rows": int(len(test)),
                "test_positives": int(test["label_primary_int"].sum()),
                **{f"price_{key}": value for key, value in price_metrics.items()},
                **{f"family_{key}": value for key, value in family_metrics.items()},
                **{f"combo_{key}": value for key, value in combo_metrics.items()},
                "delta_average_precision": _difference(
                    combo_metrics.get("average_precision"), price_metrics.get("average_precision")
                ),
                "delta_top_2pct_lift": _difference(
                    combo_metrics.get("top_2pct_lift"), price_metrics.get("top_2pct_lift")
                ),
            }
        )
    scored = pd.concat(scored_frames, ignore_index=True) if scored_frames else pd.DataFrame()
    metrics = pd.DataFrame(records)
    decision = family_increment_decision(scored, metrics, family_name=family_name)
    return scored, metrics, decision


def _difference(left: Any, right: Any) -> float | None:
    if left is None or right is None:
        return None
    value = float(left) - float(right)
    return value if np.isfinite(value) else None


def bootstrap_family_increment(
    scored: pd.DataFrame,
    *,
    family_name: str,
    samples: int = 2000,
    seed: int = 20260719,
) -> dict[str, Any]:
    """Bootstrap AP and top-2% lift increments in seven-day time blocks."""

    if scored.empty:
        return {"samples": 0}
    data = scored.copy()
    data["week"] = data["date"].dt.tz_localize(None).dt.to_period("W-SUN").dt.start_time
    weeks = list(data["week"].drop_duplicates())
    week_codes = pd.Categorical(data["week"], categories=weeks).codes
    target = data["label_primary_int"].to_numpy(dtype=float)
    price_score = data["price_same_score"].to_numpy(dtype=float)
    combo_score = data[f"price_{family_name}_score"].to_numpy(dtype=float)
    price_selected = (data["price_same_score_pctile"].to_numpy(dtype=float) >= 0.98)
    combo_selected = (
        data[f"price_{family_name}_score_pctile"].to_numpy(dtype=float) >= 0.98
    )
    price_order = np.argsort(-price_score, kind="stable")
    combo_order = np.argsort(-combo_score, kind="stable")
    rng = np.random.default_rng(seed)
    ap_deltas: list[float] = []
    lift_deltas: list[float] = []
    for _ in range(samples):
        sampled_codes = rng.integers(0, len(weeks), len(weeks))
        counts = np.bincount(sampled_codes, minlength=len(weeks)).astype(float)
        weights = counts[week_codes]
        total_weight = weights.sum()
        positive_weight = float(np.dot(weights, target))
        if total_weight <= 0 or positive_weight <= 0:
            continue
        price_ap = _weighted_average_precision(target, weights, price_order)
        combo_ap = _weighted_average_precision(target, weights, combo_order)
        ap_deltas.append(combo_ap - price_ap)
        base_rate = positive_weight / total_weight
        price_selected_weight = float(weights[price_selected].sum())
        combo_selected_weight = float(weights[combo_selected].sum())
        if price_selected_weight <= 0 or combo_selected_weight <= 0 or base_rate <= 0:
            continue
        price_lift = float(np.dot(weights[price_selected], target[price_selected])) / price_selected_weight / base_rate
        combo_lift = float(np.dot(weights[combo_selected], target[combo_selected])) / combo_selected_weight / base_rate
        lift_deltas.append(combo_lift - price_lift)
    return {
        "samples": min(len(ap_deltas), len(lift_deltas)),
        "delta_average_precision": _quantiles(ap_deltas),
        "delta_top_2pct_lift": _quantiles(lift_deltas),
    }


def _weighted_average_precision(
    target: np.ndarray,
    weights: np.ndarray,
    descending_order: np.ndarray,
) -> float:
    ordered_target = target[descending_order]
    ordered_weight = weights[descending_order]
    weighted_positive = ordered_target * ordered_weight
    positive_total = float(weighted_positive.sum())
    if positive_total <= 0:
        return float("nan")
    cumulative_weight = np.cumsum(ordered_weight)
    cumulative_positive = np.cumsum(weighted_positive)
    precision = np.divide(
        cumulative_positive,
        cumulative_weight,
        out=np.zeros_like(cumulative_positive),
        where=cumulative_weight > 0,
    )
    return float(np.dot(precision, weighted_positive) / positive_total)


def _quantiles(values: Sequence[float]) -> dict[str, float | None]:
    if not values:
        return {"median": None, "lower_95pct": None, "upper_95pct": None}
    array = np.asarray(values, dtype=float)
    return {
        "median": float(np.median(array)),
        "lower_95pct": float(np.quantile(array, 0.025)),
        "upper_95pct": float(np.quantile(array, 0.975)),
    }


def family_increment_decision(
    scored: pd.DataFrame,
    fold_metrics: pd.DataFrame,
    *,
    family_name: str,
) -> dict[str, Any]:
    bootstrap = bootstrap_family_increment(scored, family_name=family_name)
    if fold_metrics.empty:
        majority = 0.0
    else:
        majority = float(
            (
                (fold_metrics["delta_average_precision"] > 0)
                & (fold_metrics["delta_top_2pct_lift"] > 0)
            ).mean()
        )
    ap_lower = bootstrap.get("delta_average_precision", {}).get("lower_95pct")
    lift_lower = bootstrap.get("delta_top_2pct_lift", {}).get("lower_95pct")
    eligible = bool(
        len(fold_metrics) >= 3
        and majority > 0.5
        and ap_lower is not None and ap_lower > 0
        and lift_lower is not None and lift_lower > 0
    )
    return {
        "family": family_name,
        "folds": int(len(fold_metrics)),
        "fold_majority_ap_and_lift_positive": majority,
        "bootstrap_7d_blocks": bootstrap,
        "ranking_eligible": eligible,
        "rule": "at least 3 folds, majority improve AP and lift, both 95% increment lower bounds above zero",
    }


def evaluate_catalyst_rules(scored: pd.DataFrame) -> pd.DataFrame:
    """Evaluate pre-registered interpretable news gates with BH correction."""

    if scored.empty:
        return pd.DataFrame()
    rules = {
        "price_top2": scored["price_same_score_pctile"] >= 0.98,
        "price_top10_any_news3d": (scored["price_same_score_pctile"] >= 0.90) & (scored["news_count_3d"] > 0),
        "price_top10_catalyst7d": (scored["price_same_score_pctile"] >= 0.90) & (scored["news_catalyst_7d"] > 0),
        "price_top10_listing7d": (scored["price_same_score_pctile"] >= 0.90) & (scored["news_listing_7d"] > 0),
        "price_top10_official7d": (scored["price_same_score_pctile"] >= 0.90) & (scored["news_official_7d"] > 0),
        "price_top5_any_news7d": (scored["price_same_score_pctile"] >= 0.95) & (scored["news_count_7d"] > 0),
        "price_top10_accelerating": (scored["price_same_score_pctile"] >= 0.90) & (scored["news_acceleration_3d"] > 0),
    }
    target = scored["label_primary_int"].astype(int)
    base_rate = float(target.mean())
    records: list[dict[str, Any]] = []
    from scipy.stats import fisher_exact

    for name, mask in rules.items():
        selected = target[mask]
        selected_positive = int(selected.sum())
        selected_negative = int(len(selected) - selected_positive)
        other_positive = int(target.sum() - selected_positive)
        other_negative = int((1 - target).sum() - selected_negative)
        precision = float(selected.mean()) if len(selected) else np.nan
        p_value = np.nan
        if len(selected) and selected_positive:
            _, p_value = fisher_exact(
                [[selected_positive, selected_negative], [other_positive, other_negative]],
                alternative="greater",
            )
        records.append(
            {
                "rule": name,
                "rows": int(len(target)),
                "positives": int(target.sum()),
                "selected_rows": int(len(selected)),
                "selected_positives": selected_positive,
                "base_rate": base_rate,
                "precision": precision,
                "lift": precision / base_rate if base_rate > 0 and np.isfinite(precision) else np.nan,
                "p_value_one_sided": p_value,
            }
        )
    result = pd.DataFrame(records)
    result["p_value_bh"] = benjamini_hochberg(result["p_value_one_sided"])
    return result
