"""Rule-based AI research planner with catalog-aware template selection."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import secrets
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field

from config.strategy_registry import STRATEGY_REGISTRY, get_backtest_optimization_grid
from core.ai.proposal_schemas import ResearchProposal
from core.governance.schemas import LLMResearchOutput


class PlannerGenerateRequest(BaseModel):
    goal: str = Field(..., min_length=8, max_length=600)
    market_regime: str = "mixed"
    symbols: List[str] = Field(default_factory=lambda: ["BTC/USDT"])
    timeframes: List[str] = Field(default_factory=lambda: ["15m", "1h"])
    constraints: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    origin_context: Dict[str, Any] = Field(default_factory=dict)
    # E: Market context signals (sentiment, factors, microstructure)
    market_context: Dict[str, Any] = Field(default_factory=dict)
    llm_research_output: Dict[str, Any] = Field(default_factory=dict)


@dataclass
class PlannerOutput:
    proposal: ResearchProposal
    planner_notes: List[str]


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_symbol(symbol: str) -> str:
    raw = str(symbol or "").strip().upper()
    if not raw:
        return ""
    if "/" in raw:
        return raw
    if "_" in raw:
        left, right = raw.split("_", 1)
        return f"{left}/{right}"
    if raw.endswith("USDT") and len(raw) > 4:
        return f"{raw[:-4]}/USDT"
    return raw


def _dedupe_keep_order(values: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for item in values or []:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _normalize_timeframes(values: List[str]) -> List[str]:
    cleaned = _dedupe_keep_order([str(item or "").strip() for item in values or []])
    return cleaned or ["15m", "1h"]


def _category_aliases() -> Dict[str, List[str]]:
    return {
        "trend_up":      ["趋势", "动量", "突破"],
        "trend_down":    ["趋势", "动量", "风险"],
        "trend":         ["趋势", "动量"],
        "mean_reversion":["震荡", "均值回归", "成交量"],
        "reversion":     ["震荡", "均值回归"],
        "breakout":      ["突破", "动量", "成交量"],
        "stat_arb":      ["统计套利", "量化"],
        "cross_sectional":["统计套利"],
        "news_event":    ["宏观", "趋势"],
        "event":         ["宏观", "趋势"],
        "volatile":      ["波动率", "风险", "突破"],
        "ranging":       ["震荡", "均值回归", "成交量"],
        # mixed = all categories, let catalog sort by priority
        "mixed":         ["趋势", "震荡", "动量", "均值回归", "突破", "成交量", "统计套利", "量化", "波动率", "风险"],
    }


def _default_strategy_templates(market_regime: str, symbols: List[str]) -> List[str]:
    regime = str(market_regime or "").strip().lower()
    if regime in {"trend", "trending", "trend_up", "trend_down"}:
        return ["MAStrategy", "EMAStrategy", "MACDStrategy", "ADXTrendStrategy", "AroonStrategy", "TrendFollowingStrategy"]
    if regime in {"mean_reversion", "reversion", "ranging"}:
        return ["RSIStrategy", "StochasticStrategy", "CCIStrategy", "BollingerBandsStrategy", "MeanReversionStrategy", "VWAPReversionStrategy"]
    if regime in {"breakout"}:
        return ["DonchianBreakoutStrategy", "BollingerSqueezeStrategy", "MomentumStrategy", "ROCStrategy", "OBVStrategy"]
    if regime in {"stat_arb", "statarb", "cross_sectional"}:
        return ["PairsTradingStrategy", "HurstExponentStrategy", "MultiFactorHFStrategy"]
    if regime in {"volatile"}:
        return ["ParkinsonVolStrategy", "UlcerIndexStrategy", "VaRBreakoutStrategy", "BollingerSqueezeStrategy"]
    if regime in {"news", "news_event", "event"}:
        return ["MarketSentimentStrategy", "SocialSentimentStrategy", "FundFlowStrategy", "WhaleActivityStrategy"]
    if len(symbols) >= 2:
        return ["PairsTradingStrategy", "HurstExponentStrategy", "MAStrategy", "MultiFactorHFStrategy"]
    # mixed / default: balanced set across all working categories
    return [
        "MAStrategy", "RSIStrategy", "MACDStrategy", "BollingerBandsStrategy",
        "ADXTrendStrategy", "StochasticStrategy", "DonchianBreakoutStrategy",
        "MLXGBoostStrategy", "MarketSentimentStrategy", "FundFlowStrategy",
        "MFIStrategy", "OBVStrategy", "HurstExponentStrategy",
        "MultiFactorHFStrategy", "WhaleActivityStrategy",
    ]


def _ensure_family_diversity(templates: List[str], market_regime: str, max_templates: int) -> List[str]:
    regime = str(market_regime or "").strip().lower()
    selected = _dedupe_keep_order(list(templates or []))
    if not selected or max_templates <= 0:
        return selected[:max_templates]

    preferred: List[str] = []
    if regime in {"mixed", "news", "news_event", "event"}:
        preferred.extend(["MLXGBoostStrategy", "MarketSentimentStrategy"])
    elif regime in {"trend", "trending", "trend_up", "trend_down"}:
        preferred.append("MLXGBoostStrategy")

    for name in reversed(preferred):
        if name in selected:
            continue
        if len(selected) >= max_templates:
            selected.pop()
        selected.append(name)
    return _dedupe_keep_order(selected)[:max_templates]


def _derive_required_features(strategy_templates: List[str], provided: List[str]) -> List[str]:
    features = set(_dedupe_keep_order(provided))
    if not features:
        features.add("ohlcv")
    for name in strategy_templates:
        if name in {"PairsTradingStrategy"}:
            features.update({"pair_prices", "spread"})
        elif name in {"FamaFactorArbitrageStrategy"}:
            features.update({"cross_sectional_close", "cross_sectional_volume", "factor_scores"})
        elif name in {"MarketSentimentStrategy", "SocialSentimentStrategy", "FundFlowStrategy", "WhaleActivityStrategy"}:
            features.update({"news_events", "sentiment", "onchain_or_flow"})
    return sorted(features)


def _derive_parameter_space(strategy_templates: List[str]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for name in strategy_templates:
        grid = get_backtest_optimization_grid(name)
        if grid:
            out[name] = dict(grid)
    return out


def _parse_market_context(market_context: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    """E: Extract boosted / suppressed category hints from market context signals."""
    boosted: List[str] = []
    suppressed: List[str] = []
    if not market_context:
        return boosted, suppressed

    # Sentiment direction
    sentiment = str(market_context.get("sentiment") or market_context.get("direction") or "").strip().upper()
    if sentiment in {"LONG", "BULL", "BULLISH", "UP"}:
        boosted.extend(["趋势", "动量", "突破"])
        suppressed.extend(["均值回归", "震荡"])
    elif sentiment in {"SHORT", "BEAR", "BEARISH", "DOWN"}:
        boosted.extend(["风险", "震荡", "均值回归"])
        suppressed.extend(["趋势", "动量"])
    elif sentiment in {"NEUTRAL", "FLAT", "MIXED"}:
        boosted.extend(["震荡", "均值回归", "统计套利"])

    # Volatility regime
    vol_regime = str(market_context.get("volatility") or "").strip().lower()
    if vol_regime in {"high", "spike", "elevated"}:
        boosted.extend(["波动率", "风险"])
    elif vol_regime in {"low", "compressed", "quiet"}:
        boosted.extend(["突破", "震荡"])

    # Factor signals: momentum, mean_reversion, trend strength
    factors = market_context.get("factors") or {}
    if isinstance(factors, dict):
        if float(factors.get("momentum", 0) or 0) > 0.5:
            boosted.extend(["动量", "趋势"])
        if float(factors.get("mean_reversion", 0) or 0) > 0.5:
            boosted.extend(["均值回归", "震荡"])
        if float(factors.get("trend_strength", 0) or 0) > 0.7:
            boosted.extend(["趋势"])

    # Microstructure signals
    microstructure = market_context.get("microstructure") or {}
    if isinstance(microstructure, dict):
        ofi = float(microstructure.get("order_flow_imbalance", 0) or 0)
        if ofi > 0.3:
            boosted.extend(["量化", "成交量"])
        elif ofi < -0.3:
            boosted.extend(["震荡", "均值回归"])
        if float(microstructure.get("volume_surge", 0) or 0) > 0.5:
            boosted.extend(["成交量", "突破"])
        # Funding rate: high positive = crowded long = risk; high negative = bearish trend
        funding_rate = microstructure.get("funding_rate")
        if funding_rate is not None:
            fr = float(funding_rate or 0)
            if fr > 0.0001:          # >0.01% per 8h = crowded long, risky
                boosted.extend(["风险", "震荡"])
                suppressed.extend(["趋势"])
            elif fr < -0.0001:       # negative funding = bearish momentum
                boosted.extend(["趋势"])
                suppressed.extend(["震荡"])

    # News event count: many events = macro-driven
    news = market_context.get("news") or {}
    news_events = int(news.get("events_count", 0) or 0)
    if news_events >= 10:
        boosted.extend(["宏观"])
    elif news_events >= 5:
        boosted.extend(["宏观"])

    # Whale activity: large whale movements = external shock risk
    whale = market_context.get("whale") or {}
    whale_count = int(whale.get("count", 0) or 0)
    if whale_count >= 5:
        boosted.extend(["宏观", "风险"])
        suppressed.extend(["统计套利"])

    # F0b: OI change rate — rising OI = adding leverage (trend); falling OI = deleveraging (chop)
    oi_change_pct = float(market_context.get("oi_change_pct") or 0.0)
    if oi_change_pct > 10:
        boosted.extend(["趋势"])
        suppressed.extend(["震荡"])
    elif oi_change_pct < -10:
        boosted.extend(["震荡"])
        suppressed.extend(["趋势"])

    # F1: Deribit options skew — put premium = downside hedging; call premium = FOMO
    options_skew = float(market_context.get("options_skew_25d") or 0.0)
    if options_skew > 0.08:      # strong put IV premium → hedging against downside
        boosted.extend(["风险"])
        suppressed.extend(["趋势", "动量"])
    elif options_skew > 0.04:    # mild put premium → cautious
        boosted.extend(["震荡"])
    elif options_skew < -0.05:   # call IV premium → FOMO / upside demand
        boosted.extend(["趋势", "动量"])

    # F2: Google Trends — high retail interest = crowded/FOMO; low = disinterest/mean-reversion
    try:
        from core.data.google_trends_collector import load_latest  # noqa: PLC0415
        trend_val = load_latest("bitcoin")
        if trend_val is not None:
            if trend_val > 75:
                boosted.extend(["风险"])
                suppressed.extend(["趋势"])
            elif trend_val < 20:
                boosted.extend(["均值回归"])
    except Exception:
        pass

    # F3: Macro (yfinance primary + FRED supplement)
    try:
        from core.data.macro_collector import load_macro_snapshot  # noqa: PLC0415
        macro = load_macro_snapshot()
        vix = macro.get("vix")
        if vix is not None:
            if vix > 30:        # elevated fear — defensive
                boosted.extend(["风险"])
                suppressed.extend(["趋势", "动量"])
            elif vix < 15:      # complacency — trend-friendly
                boosted.extend(["趋势"])
        dxy = macro.get("dxy")
        if dxy is not None:     # strong USD historically pressures crypto
            if dxy > 108:
                boosted.extend(["风险", "震荡"])
            elif dxy < 98:
                boosted.extend(["趋势"])
        tnx = macro.get("tnx_10y")
        if tnx is not None:
            if tnx > 4.5:       # high real rates → risk-off for crypto
                boosted.extend(["风险"])
                suppressed.extend(["动量"])
            elif tnx < 3.5:     # low rates → risk-on
                boosted.extend(["动量"])
    except Exception:
        pass

    # F4a: Glassnode on-chain (no-op if key absent / cache cold)
    try:
        from core.data.glassnode_collector import load_glassnode_snapshot  # noqa: PLC0415
        gn = load_glassnode_snapshot()
        sopr = gn.get("sopr")
        if sopr is not None:
            if sopr < 0.97:       # loss realisation — capitulation → contrarian buy
                boosted.extend(["均值回归", "趋势"])
            elif sopr > 1.05:     # profit taking → potential sell pressure
                boosted.extend(["风险"])
                suppressed.extend(["动量"])
        mvrv_z = gn.get("mvrv_z")
        if mvrv_z is not None:
            if mvrv_z > 6:        # historically overvalued
                boosted.extend(["风险"])
                suppressed.extend(["趋势", "动量"])
            elif mvrv_z < 0:      # historically undervalued
                boosted.extend(["趋势"])
        netflow = gn.get("exchange_netflow")
        if netflow is not None:
            if netflow > 0:       # net inflow to exchanges = potential selling
                boosted.extend(["风险", "震荡"])
            elif netflow < 0:     # outflow = accumulation signal
                boosted.extend(["趋势"])
    except Exception:
        pass

    # F4b: CryptoQuant flows (no-op if key absent / cache cold)
    try:
        from core.data.cryptoquant_collector import load_cryptoquant_snapshot  # noqa: PLC0415
        cq = load_cryptoquant_snapshot()
        cq_netflow = cq.get("exchange_netflow")
        if cq_netflow is not None:
            if cq_netflow > 0:
                boosted.extend(["风险"])
            elif cq_netflow < 0:
                boosted.extend(["趋势"])
        ffr = cq.get("fund_flow_ratio")
        if ffr is not None and ffr > 0.2:   # >20% of supply on exchanges = sell risk
            boosted.extend(["风险"])
    except Exception:
        pass

    # F4c: Nansen smart money (no-op if key absent / cache cold)
    try:
        from core.data.nansen_collector import load_nansen_snapshot  # noqa: PLC0415
        ns = load_nansen_snapshot()
        sm_net = ns.get("smart_money_netflow")
        if sm_net is not None:
            if sm_net > 0:        # smart money accumulating
                boosted.extend(["趋势", "动量"])
            elif sm_net < 0:      # smart money distributing
                boosted.extend(["风险"])
                suppressed.extend(["动量"])
        lp_chg = ns.get("dex_lp_tvl_change")
        if lp_chg is not None:
            if lp_chg < -5:       # LP withdrawing = risk-off
                boosted.extend(["风险"])
    except Exception:
        pass

    # F4d: Kaiko microstructure quality (no-op if key absent / cache cold)
    try:
        from core.data.kaiko_collector import load_kaiko_snapshot  # noqa: PLC0415
        kk = load_kaiko_snapshot()
        spread_bps = kk.get("cross_exchange_spread_bps")
        if spread_bps is not None:
            if spread_bps > 6.0:   # cross-exchange spread widening = fragmented liquidity
                boosted.extend(["风险", "震荡"])
                suppressed.extend(["趋势"])
            elif spread_bps < 2.0:
                boosted.extend(["趋势"])
        depth_1pct = kk.get("liquidity_depth_1pct")
        if depth_1pct is not None and depth_1pct < 2_000_000:
            boosted.extend(["风险"])
        trade_cnt = kk.get("trade_count_1h")
        if trade_cnt is not None and trade_cnt > 50_000:
            boosted.extend(["动量"])
    except Exception:
        pass

    return _dedupe_keep_order(boosted), _dedupe_keep_order(suppressed)


def _apply_llm_guidance(
    llm_output: Dict[str, Any],
    *,
    selected: List[str],
    max_templates: int,
    planner_notes: List[str],
    boost_categories: List[str],
) -> tuple[List[str], int, List[str]]:
    if not llm_output:
        return selected, max_templates, boost_categories

    hypothesis = str(llm_output.get("hypothesis") or "").strip()
    experiment_plan = [str(x).strip() for x in (llm_output.get("experiment_plan") or []) if str(x).strip()]
    metrics_to_check = [str(x).strip() for x in (llm_output.get("metrics_to_check") or []) if str(x).strip()]
    failure_modes = [str(x).strip() for x in (llm_output.get("expected_failure_modes") or []) if str(x).strip()]
    uncertainty = str(llm_output.get("uncertainty") or "").strip().lower()
    strategy_changes = list(llm_output.get("proposed_strategy_changes") or [])
    llm_text = " ".join([hypothesis, *experiment_plan, *metrics_to_check, *failure_modes]).lower()

    llm_boost = list(boost_categories or [])
    forced_templates: List[str] = []
    if any(token in llm_text for token in ["新闻", "宏观", "资金费率", "情绪", "链上", "巨鲸", "event", "macro", "sentiment"]):
        llm_boost.append("宏观")
        forced_templates.extend(["MarketSentimentStrategy", "FundFlowStrategy", "WhaleActivityStrategy"])
    if any(token in llm_text for token in ["机器学习", "模型", "xgboost", "ml", "分类", "预测"]):
        llm_boost.append("机器学习")
        forced_templates.append("MLXGBoostStrategy")
    if any(token in llm_text for token in ["趋势", "突破", "trend", "momentum", "breakout"]):
        llm_boost.extend(["趋势", "动量"])
    if any(token in llm_text for token in ["回归", "震荡", "mean reversion", "reversion", "range"]):
        llm_boost.extend(["震荡", "均值回归"])

    for change in strategy_changes:
        if not isinstance(change, dict):
            continue
        template = str(change.get("strategy") or change.get("strategy_template") or "").strip()
        if template:
            forced_templates.append(template)

    selected = _dedupe_keep_order([*forced_templates, *selected])
    if uncertainty in {"高", "high"}:
        llm_cap = 3
    elif uncertainty in {"低", "low"}:
        llm_cap = 6
    else:
        llm_cap = 4
    effective_max = max(1, min(max_templates, llm_cap))
    if hypothesis:
        planner_notes.append(f"AI假设影响规划: {hypothesis[:80]}")
    if effective_max != max_templates:
        planner_notes.append(f"AI将策略数量上限调整为 {effective_max}")
    if forced_templates:
        planner_notes.append(f"AI优先模板: {', '.join(_dedupe_keep_order(forced_templates)[:4])}")
    return selected[:effective_max], effective_max, _dedupe_keep_order(llm_boost)


def _catalog_candidates(
    market_regime: str,
    exclude_categories: List[str],
    boost_categories: Optional[List[str]] = None,
    suppress_categories: Optional[List[str]] = None,
) -> List[str]:
    regime_key = str(market_regime or "mixed").strip().lower() or "mixed"
    preferred_categories = _category_aliases().get(regime_key, _category_aliases()["mixed"])
    excluded = {str(item or "").strip() for item in exclude_categories or [] if str(item or "").strip()}
    boost_set = set(boost_categories or [])
    suppress_set = set(suppress_categories or [])
    rows: List[Tuple[int, str]] = []
    for name, item in STRATEGY_REGISTRY.items():
        backtest = dict(item.get("backtest") or {})
        if not backtest.get("supported", False):
            continue
        if not get_backtest_optimization_grid(name):
            continue
        category = str(item.get("category") or "")
        if category in excluded:
            continue
        # Base priority from regime
        priority = preferred_categories.index(category) if category in preferred_categories else len(preferred_categories) + 1
        # E: market context adjustments
        if category in boost_set:
            priority = max(0, priority - 3)   # move earlier
        if category in suppress_set:
            priority = priority + 3            # move later
        rows.append((priority, str(name)))
    rows.sort(key=lambda item: (item[0], item[1]))
    return [name for _, name in rows]


def _filter_supported_research_templates(templates: List[str]) -> tuple[List[str], List[str]]:
    try:
        from core.research.strategy_research import get_supported_research_strategies

        supported = set(get_supported_research_strategies())
    except Exception:
        supported = set()
    if not supported:
        return list(templates or []), []
    selected: List[str] = []
    dropped: List[str] = []
    for name in templates or []:
        key = str(name or "").strip()
        if not key:
            continue
        if key in supported:
            selected.append(key)
        else:
            dropped.append(key)
    return selected, dropped


def generate_research_proposal(request: PlannerGenerateRequest, actor: str = "ai_planner") -> PlannerOutput:
    now = _now_utc()
    symbols = _dedupe_keep_order([_normalize_symbol(item) for item in request.symbols])
    if not symbols:
        symbols = ["BTC/USDT"]
    timeframes = _normalize_timeframes(request.timeframes)
    constraints = dict(request.constraints or {})
    planner_notes: List[str] = []
    llm_output_validated: Dict[str, Any] = {}
    if request.llm_research_output:
        llm_output_validated = LLMResearchOutput.model_validate(request.llm_research_output).model_dump(mode="json")
        planner_notes.append("llm research output schema validated")
    max_templates = max(1, min(int(constraints.get("max_templates", 5) or 5), 12))
    exclude_categories = [str(item) for item in (constraints.get("exclude_categories") or [])]

    # E: process market context to get category boost/suppress hints
    market_context = dict(request.market_context or {})
    boost_categories, suppress_categories = _parse_market_context(market_context)

    # E: record what signals were used
    if market_context:
        signal_parts: List[str] = []
        sentiment = str(market_context.get("sentiment") or market_context.get("direction") or "").strip()
        confidence = float(market_context.get("confidence", 0) or 0)
        if sentiment:
            conf_str = f"{confidence*100:.0f}%" if confidence > 0 else ""
            signal_parts.append(f"方向={sentiment}{(' ' + conf_str) if conf_str else ''}")
        vol = str(market_context.get("volatility") or "").strip()
        if vol:
            signal_parts.append(f"波动={vol}")
        micro = market_context.get("microstructure") or {}
        if isinstance(micro, dict):
            fr = micro.get("funding_rate")
            if fr is not None:
                signal_parts.append(f"Funding={float(fr):.5f}")
            ofi = micro.get("order_flow_imbalance")
            if ofi is not None:
                signal_parts.append(f"OFI={float(ofi):.3f}")
        news_ev = int((market_context.get("news") or {}).get("events_count", 0) or 0)
        if news_ev:
            signal_parts.append(f"新闻事件={news_ev}")
        whale_c = int((market_context.get("whale") or {}).get("count", 0) or 0)
        if whale_c:
            signal_parts.append(f"巨鲸={whale_c}")
        oi_chg = float(market_context.get("oi_change_pct") or 0)
        if abs(oi_chg) > 5:
            signal_parts.append(f"OI变化={oi_chg:+.1f}%")
        opt_skew = market_context.get("options_skew_25d")
        if opt_skew is not None:
            opt_sig = market_context.get("options_signal") or ""
            signal_parts.append(f"期权偏斜={float(opt_skew):.3f}({opt_sig})")
        try:
            from core.data.google_trends_collector import load_latest as _gt_latest  # noqa: PLC0415
            _tv = _gt_latest("bitcoin")
            if _tv is not None:
                signal_parts.append(f"谷歌趋势={_tv:.0f}")
        except Exception:
            pass
        try:
            from core.data.macro_collector import load_macro_snapshot as _macro_snap  # noqa: PLC0415
            _m = _macro_snap()
            _macro_parts = []
            if _m.get("vix") is not None:
                _macro_parts.append(f"VIX={_m['vix']:.1f}")
            if _m.get("dxy") is not None:
                _macro_parts.append(f"DXY={_m['dxy']:.1f}")
            if _m.get("tnx_10y") is not None:
                _macro_parts.append(f"10Y={_m['tnx_10y']:.2f}%")
            if _m.get("fed_rate") is not None:
                _macro_parts.append(f"FF={_m['fed_rate']:.2f}%")
            if _macro_parts:
                signal_parts.append("宏观=[" + " · ".join(_macro_parts) + "]")
        except Exception:
            pass
        try:
            from core.data.kaiko_collector import load_kaiko_snapshot as _kaiko_snap  # noqa: PLC0415
            _k = _kaiko_snap()
            _kaiko_parts = []
            if _k.get("cross_exchange_spread_bps") is not None:
                _kaiko_parts.append(f"跨所点差={float(_k['cross_exchange_spread_bps']):.2f}bps")
            if _k.get("liquidity_depth_1pct") is not None:
                _kaiko_parts.append(f"1%深度={float(_k['liquidity_depth_1pct']):.0f}")
            if _k.get("trade_count_1h") is not None:
                _kaiko_parts.append(f"成交数={float(_k['trade_count_1h']):.0f}")
            if _kaiko_parts:
                signal_parts.append("Kaiko=[" + " · ".join(_kaiko_parts) + "]")
        except Exception:
            pass
        factors = market_context.get("factors") or {}
        if isinstance(factors, dict) and factors:
            top_factors = [f"{k}={float(v):.2f}" for k, v in factors.items() if float(v or 0) > 0.3]
            if top_factors:
                signal_parts.append("因子=[" + ", ".join(top_factors[:3]) + "]")
        if signal_parts:
            planner_notes.append(f"市场上下文 (market context): {' · '.join(signal_parts)}")
        if boost_categories:
            planner_notes.append(f"提升分类 (boosted): {' / '.join(boost_categories[:5])}")
        if suppress_categories:
            planner_notes.append(f"降权分类 (suppressed): {' / '.join(suppress_categories[:4])}")

    selected = _catalog_candidates(
        request.market_regime, exclude_categories,
        boost_categories=boost_categories,
        suppress_categories=suppress_categories,
    )[:max_templates]
    if exclude_categories:
        planner_notes.append(f"excluded categories: {', '.join(exclude_categories)}")
    if not selected:
        selected = _default_strategy_templates(request.market_regime, symbols)[:max_templates]
        planner_notes.append("fell back to default regime templates")
    else:
        planner_notes.append("selected catalog-backed, backtestable templates")

    if llm_output_validated:
        selected, max_templates, boost_categories = _apply_llm_guidance(
            llm_output_validated,
            selected=selected,
            max_templates=max_templates,
            planner_notes=planner_notes,
            boost_categories=boost_categories,
        )

    selected = _ensure_family_diversity(selected, request.market_regime, max_templates)

    # A: filter at planning time — record dropped templates with reasons
    selected, dropped = _filter_supported_research_templates(selected)
    filtered_templates = list(dropped)
    filtered_reasons: Dict[str, str] = {name: "not_supported_by_research_engine" for name in dropped}
    if dropped:
        planner_notes.append(f"dropped unsupported templates: {', '.join(dropped[:5])}")
    if not selected:
        fallback_selected, fallback_dropped = _filter_supported_research_templates(
            _default_strategy_templates(request.market_regime, symbols)[:max_templates]
        )
        filtered_templates.extend(fallback_dropped)
        filtered_reasons.update({name: "not_supported_by_research_engine" for name in fallback_dropped})
        selected = fallback_selected
        if selected:
            planner_notes.append("replaced by research-supported fallback templates")

    required_features = _derive_required_features(selected, constraints.get("required_features") or [])
    if "news_events" in required_features:
        planner_notes.append("included event/news-sensitive templates")
    if any(name in {"PairsTradingStrategy", "FamaFactorArbitrageStrategy"} for name in selected):
        planner_notes.append("included cross-sectional/stat-arb templates")

    proposal = ResearchProposal(
        proposal_id=f"proposal-{int(now.timestamp())}-{secrets.token_hex(4)}",
        created_at=now,
        updated_at=now,
        status="draft",
        source="ai",
        thesis=str(request.goal).strip(),
        market_regime=str(request.market_regime or "mixed").strip() or "mixed",
        target_symbols=symbols,
        target_timeframes=timeframes,
        strategy_templates=selected,
        # A: store filtered templates so UI can display them
        filtered_templates=filtered_templates,
        filtered_reasons=filtered_reasons,
        parameter_space=_derive_parameter_space(selected),
        required_features=required_features,
        risk_hypothesis=str(constraints.get("risk_hypothesis") or "").strip(),
        invalidation_rules=_dedupe_keep_order(constraints.get("invalidation_rules") or []),
        expected_holding_period=str(constraints.get("expected_holding_period") or "1d").strip() or "1d",
        planner_version="planner_v1",
        origin_context={
            "goal": str(request.goal).strip(),
            "constraints": constraints,
            **dict(request.origin_context or {}),
        },
        notes=planner_notes[:],
        metadata={
            "created_by": actor,
            "planner_notes": planner_notes[:],
            "planner_constraints": constraints,
            "market_context": market_context,
            "llm_research_output": llm_output_validated,
            **dict(request.metadata or {}),
        },
    )
    return PlannerOutput(proposal=proposal, planner_notes=planner_notes)
