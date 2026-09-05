"""Mine lagged news, on-chain and cached exchange evidence for Binance run-ups.

The script is research-only.  It reads the existing frozen price study, uses
public endpoints or immutable local caches, and never imports trading or order
execution modules.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_MULTISOURCE_PATH = ROOT / "core" / "research" / "binance_multisource_validation.py"
_MULTISOURCE_SPEC = importlib.util.spec_from_file_location(
    "binance_multisource_validation_standalone", _MULTISOURCE_PATH
)
if _MULTISOURCE_SPEC is None or _MULTISOURCE_SPEC.loader is None:
    raise RuntimeError(f"Unable to load {_MULTISOURCE_PATH}")
_MULTISOURCE = importlib.util.module_from_spec(_MULTISOURCE_SPEC)
sys.modules[_MULTISOURCE_SPEC.name] = _MULTISOURCE
_MULTISOURCE_SPEC.loader.exec_module(_MULTISOURCE)

NEWS_FACTORS = _MULTISOURCE.NEWS_FACTORS
ONCHAIN_FACTORS = _MULTISOURCE.ONCHAIN_FACTORS
StrictNewsMatcher = _MULTISOURCE.StrictNewsMatcher
add_news_features = _MULTISOURCE.add_news_features
add_tvl_features = _MULTISOURCE.add_tvl_features
build_lagged_tvl_features = _MULTISOURCE.build_lagged_tvl_features
build_token_identities = _MULTISOURCE.build_token_identities
evaluate_catalyst_rules = _MULTISOURCE.evaluate_catalyst_rules
map_raw_news = _MULTISOURCE.map_raw_news
run_family_walk_forward = _MULTISOURCE.run_family_walk_forward

_VALIDATION = _MULTISOURCE._VALIDATION
json_ready = _VALIDATION.json_ready
prepare_modeling_panel = _VALIDATION.prepare_modeling_panel
score_metrics = _VALIDATION.score_metrics
stable_hash = _VALIDATION.stable_hash


DEFAULT_BASELINE = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures"
DEFAULT_OUTPUT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
NEWS_DB = ROOT / "data" / "news.db"
COINGLASS_NORMALIZED = ROOT / "data" / "premium" / "coinglass" / "normalized"
COINGECKO_LIST_URL = "https://api.coingecko.com/api/v3/coins/list?include_platform=false"
DEFILLAMA_PROTOCOLS_URL = "https://api.llama.fi/protocols"
DEFILLAMA_PROTOCOL_URL = "https://api.llama.fi/protocol/{slug}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-dir", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--no-external-refresh", action="store_true")
    parser.add_argument("--max-onchain-workers", type=int, default=6)
    parser.add_argument("--rebuild-news-map", action="store_true")
    return parser.parse_args()


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(json_ready(payload), ensure_ascii=False, indent=2), encoding="utf-8")


def read_or_fetch_json(
    path: Path,
    url: str,
    *,
    no_external_refresh: bool,
    timeout: int = 45,
) -> Any:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    if no_external_refresh:
        raise FileNotFoundError(f"Missing immutable cache while refresh is disabled: {path}")
    response = requests.get(url, timeout=timeout, headers={"User-Agent": "binance-runup-research/1.0"})
    response.raise_for_status()
    payload = response.json()
    write_json(path, payload)
    return payload


def load_raw_news(minimum_fetched_at: pd.Timestamp) -> pd.DataFrame:
    query = """
        SELECT id, source, title, url, published_at, fetched_at, content_hash
        FROM news_raw
        WHERE fetched_at >= ?
        ORDER BY fetched_at, id
    """
    with sqlite3.connect(NEWS_DB) as connection:
        return pd.read_sql_query(query, connection, params=[minimum_fetched_at.tz_convert(None).isoformat()])


def fetch_protocol_detail(slug: str, path: Path) -> tuple[str, Any | None, str | None]:
    if path.exists():
        try:
            return slug, json.loads(path.read_text(encoding="utf-8")), None
        except Exception as exc:  # noqa: BLE001
            return slug, None, f"cache_decode:{type(exc).__name__}:{exc}"
    try:
        response = requests.get(
            DEFILLAMA_PROTOCOL_URL.format(slug=slug),
            timeout=45,
            headers={"User-Agent": "binance-runup-research/1.0"},
        )
        response.raise_for_status()
        payload = response.json()
        write_json(path, payload)
        return slug, payload, None
    except Exception as exc:  # noqa: BLE001
        return slug, None, f"fetch:{type(exc).__name__}:{exc}"


def load_defillama_tvl(
    identities: pd.DataFrame,
    protocols: list[dict[str, Any]],
    cache_dir: Path,
    *,
    no_external_refresh: bool,
    max_workers: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    by_gecko = {
        str(item.get("gecko_id")): item
        for item in protocols
        if item.get("gecko_id") and item.get("slug")
    }
    mapping = identities[identities["mapping_status"] == "unique"].copy()
    mapping["protocol_slug"] = mapping["coingecko_id"].map(
        lambda value: by_gecko.get(str(value), {}).get("slug")
    )
    mapping["protocol_name"] = mapping["coingecko_id"].map(
        lambda value: by_gecko.get(str(value), {}).get("name")
    )
    mapping["protocol_category"] = mapping["coingecko_id"].map(
        lambda value: by_gecko.get(str(value), {}).get("category")
    )
    mapping["defillama_match"] = mapping["protocol_slug"].notna()
    requested = mapping[mapping["defillama_match"]][
        ["contract_symbol", "coingecko_id", "protocol_slug"]
    ].drop_duplicates("protocol_slug")

    cache_dir.mkdir(parents=True, exist_ok=True)
    details: dict[str, Any] = {}
    errors: dict[str, str] = {}
    if no_external_refresh:
        for slug in requested["protocol_slug"]:
            path = cache_dir / f"{slug}.json"
            if path.exists():
                try:
                    details[str(slug)] = json.loads(path.read_text(encoding="utf-8"))
                except Exception as exc:  # noqa: BLE001
                    errors[str(slug)] = f"cache_decode:{type(exc).__name__}:{exc}"
            else:
                errors[str(slug)] = "missing_cache_refresh_disabled"
    else:
        with ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor:
            futures = {
                executor.submit(fetch_protocol_detail, str(slug), cache_dir / f"{slug}.json"): str(slug)
                for slug in requested["protocol_slug"]
            }
            for future in as_completed(futures):
                slug, payload, error = future.result()
                if payload is not None:
                    details[slug] = payload
                if error:
                    errors[slug] = error
                time.sleep(0.01)

    tvl_records: list[dict[str, Any]] = []
    for row in mapping[mapping["defillama_match"]].itertuples(index=False):
        payload = details.get(str(row.protocol_slug))
        if not payload:
            continue
        series = payload.get("tvl") or []
        for item in series:
            timestamp = item.get("date")
            value = item.get("totalLiquidityUSD")
            if timestamp is None or value is None:
                continue
            tvl_records.append(
                {
                    "symbol": str(row.contract_symbol),
                    "coingecko_id": str(row.coingecko_id),
                    "protocol_slug": str(row.protocol_slug),
                    "source_date": pd.to_datetime(timestamp, unit="s", utc=True),
                    "tvl": value,
                }
            )
    mapping["detail_loaded"] = mapping["protocol_slug"].astype(str).isin(details)
    mapping["detail_error"] = mapping["protocol_slug"].astype(str).map(errors)
    return pd.DataFrame(tvl_records), mapping


def coinglass_cache_audit(root: Path) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for path in sorted(root.glob("*.parquet")):
        try:
            frame = pd.read_parquet(path)
            symbol_column = "normalized_symbol" if "normalized_symbol" in frame else None
            source_times = pd.to_datetime(frame.get("source_ts"), utc=True, errors="coerce")
            ingest_times = pd.to_datetime(frame.get("ingested_at"), utc=True, errors="coerce")
            records.append(
                {
                    "dataset": path.stem,
                    "rows": int(len(frame)),
                    "symbols": int(frame[symbol_column].nunique()) if symbol_column else 0,
                    "source_min": source_times.min(),
                    "source_max": source_times.max(),
                    "ingested_min": ingest_times.min(),
                    "ingested_max": ingest_times.max(),
                    "selection_bias": "requested-symbol cache; not all-market",
                    "primary_model_eligible": False,
                }
            )
        except Exception as exc:  # noqa: BLE001
            records.append({"dataset": path.stem, "error": f"{type(exc).__name__}:{exc}"})
    return pd.DataFrame(records)


def recent_oi_sensitivity(baseline: Path, modeling: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    path = baseline / "oi_daily_panel_30d.csv.gz"
    if not path.exists():
        return pd.DataFrame(), {"available": False}
    recent = pd.read_csv(path, compression="gzip")
    recent["date"] = pd.to_datetime(recent["date"], utc=True)
    keep = [
        column for column in (
            "symbol", "date", "oi_usd", "oi_change_1d", "oi_change_3d", "oi_change_7d",
            "funding_rate_daily", "oi_to_cmc_mcap",
        ) if column in recent
    ]
    merged = modeling.merge(recent[keep], on=["symbol", "date"], how="inner")
    merged = merged[merged["oi_usd"].notna()].copy()
    if merged.empty:
        return merged, {"available": True, "observed_rows": 0}
    records: list[dict[str, Any]] = []
    for factor in [column for column in keep if column not in {"symbol", "date"}]:
        sample = merged.dropna(subset=[factor]).copy()
        if sample.empty:
            continue
        sample["factor_pctile"] = sample[factor].groupby(sample["date"]).rank(pct=True, method="first")
        high = sample[sample["factor_pctile"] >= 0.90]
        low = sample[sample["factor_pctile"] <= 0.10]
        records.append(
            {
                "factor": factor,
                "rows": int(len(sample)),
                "positives": int(sample["label_primary_int"].sum()),
                "high_decile_rows": int(len(high)),
                "high_decile_rate": float(high["label_primary_int"].mean()) if len(high) else np.nan,
                "low_decile_rows": int(len(low)),
                "low_decile_rate": float(low["label_primary_int"].mean()) if len(low) else np.nan,
                "base_rate": float(sample["label_primary_int"].mean()),
            }
        )
    summary = {
        "available": True,
        "observed_rows": int(len(merged)),
        "symbols": int(merged["symbol"].nunique()),
        "min_date": merged["date"].min(),
        "max_date": merged["date"].max(),
        "role": "recent directional sensitivity only; window is too short for walk-forward ranking validation",
    }
    return pd.DataFrame(records), summary


def current_event_news_case_study(
    output: Path,
    mentions: pd.DataFrame,
) -> pd.DataFrame:
    path = output / "current_30d_runups_verified.csv"
    if not path.exists() or mentions.empty:
        return pd.DataFrame()
    events = pd.read_csv(path)
    events["entry_time"] = pd.to_datetime(events["entry_time"], utc=True, errors="coerce")
    records: list[dict[str, Any]] = []
    for event in events.itertuples(index=False):
        prior_30d = mentions[
            (mentions["symbol"] == event.symbol)
            & (mentions["available_at"] <= event.entry_time)
            & (mentions["available_at"] > event.entry_time - pd.Timedelta(days=30))
        ].sort_values("available_at")
        record: dict[str, Any] = {
            "symbol": event.symbol,
            "classification": event.classification,
            "entry_time": event.entry_time,
            "close_return": event.close_return,
            "first_prior_title_30d": str(prior_30d.iloc[0]["title"]) if not prior_30d.empty else None,
            "first_prior_available_at_30d": prior_30d.iloc[0]["available_at"] if not prior_30d.empty else None,
        }
        for days in (7, 14, 30):
            prior = prior_30d[
                prior_30d["available_at"] > event.entry_time - pd.Timedelta(days=days)
            ]
            record.update(
                {
                    f"news_mentions_{days}d": int(len(prior)),
                    f"news_sources_{days}d": int(prior["source_key"].nunique()),
                    f"listing_mentions_{days}d": int(prior["is_listing"].sum()),
                    f"official_mentions_{days}d": int(prior["is_official"].sum()),
                    f"catalyst_mentions_{days}d": int(prior["is_catalyst"].sum()),
                    f"risk_mentions_{days}d": int(prior["is_risk"].sum()),
                    f"supply_flow_mentions_{days}d": int(prior["is_supply_flow"].sum())
                    if "is_supply_flow" in prior else 0,
                }
            )
        records.append(record)
    return pd.DataFrame(records)


def pooled_model_metrics(scored: pd.DataFrame, family: str) -> dict[str, Any]:
    if scored.empty:
        return {}
    return {
        "price_same_rows": score_metrics(scored, "price_same_score", "label_primary_int"),
        "family_only": score_metrics(scored, f"{family}_score", "label_primary_int"),
        "price_plus_family": score_metrics(scored, f"price_{family}_score", "label_primary_int"),
    }


def main() -> None:
    args = parse_args()
    baseline = args.baseline_dir.resolve()
    output = args.output_dir.resolve()
    cache = output / "multisource_cache"
    cache.mkdir(parents=True, exist_ok=True)

    daily = pd.read_csv(baseline / "daily_feature_panel.csv.gz", compression="gzip")
    daily["date"] = pd.to_datetime(daily["date"], utc=True)
    modeling = prepare_modeling_panel(daily)
    universe = sorted(modeling["symbol"].unique())

    coingecko = read_or_fetch_json(
        cache / "coingecko_coins_list.json",
        COINGECKO_LIST_URL,
        no_external_refresh=args.no_external_refresh,
    )
    identities = build_token_identities(universe, coingecko)
    identities.to_csv(output / "multisource_token_identity.csv", index=False)

    raw_news = load_raw_news(pd.Timestamp("2026-02-21", tz="UTC"))
    mentions_path = output / "multisource_news_mentions.csv.gz"
    if mentions_path.exists() and not args.rebuild_news_map:
        mentions = pd.read_csv(mentions_path, compression="gzip")
        mentions["available_at"] = pd.to_datetime(mentions["available_at"], utc=True)
        mentions["published_at"] = pd.to_datetime(mentions["published_at"], utc=True, errors="coerce")
    else:
        matcher = StrictNewsMatcher(identities)
        mentions = map_raw_news(raw_news, matcher)
        mentions.to_csv(mentions_path, index=False, compression="gzip")
    news_rows = add_news_features(modeling, mentions)
    news_scored, news_folds, news_decision = run_family_walk_forward(
        news_rows,
        NEWS_FACTORS,
        family_name="news",
        eligibility_column="news_history_ready",
        minimum_family_train_days=30,
    )
    news_scored.to_csv(output / "multisource_news_walk_forward_scored.csv.gz", index=False, compression="gzip")
    news_folds.to_csv(output / "multisource_news_fold_metrics.csv", index=False)
    catalyst_rules = evaluate_catalyst_rules(news_scored)
    catalyst_rules.to_csv(output / "multisource_news_catalyst_rules.csv", index=False)

    protocols = read_or_fetch_json(
        cache / "defillama_protocols.json",
        DEFILLAMA_PROTOCOLS_URL,
        no_external_refresh=args.no_external_refresh,
    )
    tvl, protocol_mapping = load_defillama_tvl(
        identities,
        protocols,
        cache / "defillama_protocols",
        no_external_refresh=args.no_external_refresh,
        max_workers=args.max_onchain_workers,
    )
    protocol_mapping.to_csv(output / "multisource_onchain_mapping.csv", index=False)
    tvl.to_csv(output / "multisource_onchain_tvl_raw.csv.gz", index=False, compression="gzip")
    tvl_features = build_lagged_tvl_features(tvl)
    onchain_rows = add_tvl_features(modeling, tvl_features)
    onchain_scored, onchain_folds, onchain_decision = run_family_walk_forward(
        onchain_rows,
        ONCHAIN_FACTORS,
        family_name="onchain",
        minimum_family_train_days=180,
    )
    onchain_scored.to_csv(output / "multisource_onchain_walk_forward_scored.csv.gz", index=False, compression="gzip")
    onchain_folds.to_csv(output / "multisource_onchain_fold_metrics.csv", index=False)

    exchange_audit = coinglass_cache_audit(COINGLASS_NORMALIZED)
    exchange_audit.to_csv(output / "multisource_exchange_cache_coverage.csv", index=False)
    recent_oi, recent_oi_summary = recent_oi_sensitivity(baseline, modeling)
    recent_oi.to_csv(output / "multisource_recent_oi_sensitivity.csv", index=False)
    event_news = current_event_news_case_study(output, mentions)
    event_news.to_csv(output / "multisource_current_event_news.csv", index=False)

    news_coverage = {
        "raw_rows": int(len(raw_news)),
        "raw_min_fetched_at": pd.to_datetime(raw_news["fetched_at"], utc=True, errors="coerce").min(),
        "raw_max_fetched_at": pd.to_datetime(raw_news["fetched_at"], utc=True, errors="coerce").max(),
        "strict_mapped_mentions": int(len(mentions)),
        "mapped_news_items": int(mentions["news_id"].nunique()) if not mentions.empty else 0,
        "mapped_symbols": int(mentions["symbol"].nunique()) if not mentions.empty else 0,
        "mapped_source_keys": int(mentions["source_key"].nunique()) if not mentions.empty else 0,
        "eligible_walk_forward_folds": int(len(news_folds)),
        "limitation": "local archive begins 2026-02-21; only later folds have enough pre-test news history",
    }
    onchain_coverage = {
        "unique_coingecko_identities": int((identities["mapping_status"] == "unique").sum()),
        "ambiguous_identities": int((identities["mapping_status"] == "ambiguous").sum()),
        "missing_identities": int((identities["mapping_status"] == "missing").sum()),
        "defillama_matches": int(protocol_mapping["defillama_match"].sum()) if not protocol_mapping.empty else 0,
        "details_loaded": int(protocol_mapping["detail_loaded"].sum()) if not protocol_mapping.empty else 0,
        "tvl_rows": int(len(tvl)),
        "tvl_symbols": int(tvl["symbol"].nunique()) if not tvl.empty else 0,
        "eligible_walk_forward_folds": int(len(onchain_folds)),
        "limitation": "current identity maps create survivorship risk; only unique CoinGecko-to-DefiLlama matches are admitted",
    }

    oi_decision_path = output / "oi_decision.json"
    prior_oi = json.loads(oi_decision_path.read_text(encoding="utf-8")) if oi_decision_path.exists() else {}
    news_metrics = pooled_model_metrics(news_scored, "news")
    onchain_metrics = pooled_model_metrics(onchain_scored, "onchain")
    any_incremental = bool(news_decision.get("ranking_eligible") or onchain_decision.get("ranking_eligible"))
    summary = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "scope": "Binance USD-M perpetual +200% within future 14 days; historical multisource incremental mining",
        "price_baseline": "fixed eight-factor family, refitted inside each training fold; comparison rows are identical",
        "news": {
            "factors": list(NEWS_FACTORS),
            "coverage": news_coverage,
            "pooled_metrics": news_metrics,
            "decision": news_decision,
        },
        "onchain": {
            "factors": list(ONCHAIN_FACTORS),
            "coverage": onchain_coverage,
            "pooled_metrics": onchain_metrics,
            "decision": onchain_decision,
        },
        "exchange": {
            "prior_long_window_oi_decision": prior_oi,
            "recent_oi": recent_oi_summary,
            "richer_cache_datasets": int(len(exchange_audit)),
            "richer_cache_primary_model_eligible": False,
            "reason": "funding/long-short/liquidation/taker caches are narrow requested-symbol cohorts and therefore selection-biased",
        },
        "current_event_news": {
            "events": int(len(event_news)),
            "events_with_any_prior_7d_news": int((event_news["news_mentions_7d"] > 0).sum()) if not event_news.empty else 0,
            "events_with_listing_prior_7d": int((event_news["listing_mentions_7d"] > 0).sum()) if not event_news.empty else 0,
            "events_with_official_prior_7d": int((event_news["official_mentions_7d"] > 0).sum()) if not event_news.empty else 0,
        },
        "strict_strategy_decision": {
            "new_family_ranking_eligible": any_incremental,
            "auto_trade_eligible": False,
            "classification": "candidate_requires_exit_backtest" if any_incremental else "watchlist_only",
            "reason": "a family must pass same-row incremental gates before any entry rule is sent to the frozen 4h exit and portfolio simulator",
        },
        "data_hashes": {
            "identities": stable_hash(identities.to_dict(orient="records")),
            "news_mentions": stable_hash(mentions[["news_id", "symbol", "available_at"]].astype(str).to_dict(orient="records")) if not mentions.empty else None,
            "protocol_mapping": stable_hash(protocol_mapping.fillna("").astype(str).to_dict(orient="records")),
        },
    }
    write_json(output / "multisource_analysis_summary.json", summary)
    write_json(output / "multisource_news_decision.json", news_decision)
    write_json(output / "multisource_onchain_decision.json", onchain_decision)
    write_json(
        output / "multisource_data_quality.json",
        {"news": news_coverage, "onchain": onchain_coverage, "recent_oi": recent_oi_summary},
    )
    print(json.dumps(json_ready(summary), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
