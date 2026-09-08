"""Global application settings."""
from pathlib import Path
from typing import List, Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


_PROJECT_ROOT = Path(__file__).parent.parent
_SQLITE_ASYNC_PREFIX = "sqlite+aiosqlite:///"


def _default_database_url() -> str:
    return f"{_SQLITE_ASYNC_PREFIX}{(_PROJECT_ROOT / 'data' / 'crypto_trading.db').resolve().as_posix()}"


def _default_news_database_url() -> str:
    return f"{_SQLITE_ASYNC_PREFIX}{(_PROJECT_ROOT / 'data' / 'news.db').resolve().as_posix()}"


def _normalize_sqlite_database_url(value: str) -> str:
    text = str(value or "").strip()
    if not text.startswith(_SQLITE_ASYNC_PREFIX):
        return text
    raw_path = Path(text[len(_SQLITE_ASYNC_PREFIX):])
    if raw_path.is_absolute():
        return text
    return f"{_SQLITE_ASYNC_PREFIX}{(_PROJECT_ROOT / raw_path).resolve().as_posix()}"


class Settings(BaseSettings):
    """Application settings loaded from environment and .env."""

    model_config = SettingsConfigDict(
        env_file=(
            str(_PROJECT_ROOT / ".env"),
            str(_PROJECT_ROOT / ".env.local"),
        ),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Project paths
    BASE_DIR: Path = Field(default_factory=lambda: _PROJECT_ROOT)
    DATA_STORAGE_PATH: Path = Field(default=Path("./data/historical"))
    CACHE_PATH: Path = Field(default=Path("./data/cache"))
    LOG_PATH: Path = Field(default=Path("./logs"))

    # API credentials
    BINANCE_API_KEY: str = ""
    BINANCE_API_SECRET: str = ""
    OKX_API_KEY: str = ""
    OKX_API_SECRET: str = ""
    OKX_PASSPHRASE: str = ""
    GATE_API_KEY: str = ""
    GATE_API_SECRET: str = ""
    BYBIT_API_KEY: str = ""
    BYBIT_API_SECRET: str = ""
    COINGLASS_ENABLED: bool = True
    # 2026-09-06: migrated from the keystore relay (started returning HTTP 403)
    # to the vip2.coinglass.site relay. This relay serves NON-versioned paths
    # (/api/futures/..., /api/lsr/...), so COINGLASS_STRIP_API_VERSION strips the
    # /v3 /v4 prefixes the dataset manifests carry. Key in config/coinglass_api_key.txt
    # (gitignored). Auth header is X-Api-Key (already used by the client).
    COINGLASS_BASE_URL: str = "https://vip2.coinglass.site"
    COINGLASS_SPEC_ROOT_URL: str = "https://vip2.coinglass.site"
    COINGLASS_API_KEY: str = ""
    # vip2 relay: 11 req/60s per data type; keep the local limiter at/under it.
    COINGLASS_RATE_LIMIT_PER_MIN: int = 11
    # vip2 relay uses non-versioned endpoint paths; strip /v3 /v4 from manifest paths.
    COINGLASS_STRIP_API_VERSION: bool = True
    COINGLASS_DAILY_BUDGET: int = 50000
    COINGLASS_MONTHLY_BUDGET: int = 500000
    COINGLASS_INCLUDE_AI: bool = True
    COINGLASS_INCLUDE_RADAR: bool = True
    COINGLASS_INCLUDE_STRATEGIES: bool = False
    COINGLASS_LIVE_GATING_ENABLED: bool = False
    COINGLASS_WORKER_ENABLED: bool = True
    # Binance Alpha is a public discovery feed used by the altcoin radar.
    # It never enables trading and can be disabled when outbound access is not
    # desired; the radar will continue using its normal local/spot universe.
    BINANCE_ALPHA_ENABLED: bool = True
    BINANCE_ALPHA_TIMEOUT_SEC: float = 10.0
    # Persistent Alpha research collector.  The worker stores catalog
    # snapshots, incremental klines, sampled aggregated trades, and order-book
    # snapshots in data/research/binance_alpha/alpha_market.db.
    BINANCE_ALPHA_COLLECTOR_ENABLED: bool = True
    BINANCE_ALPHA_COLLECTOR_INTERVAL_SEC: int = 60
    BINANCE_ALPHA_COLLECTOR_CATALOG_INTERVAL_SEC: int = 300
    BINANCE_ALPHA_COLLECTOR_MAX_TOKENS: int = 400
    BINANCE_ALPHA_COLLECTOR_CONCURRENCY: int = 8
    BINANCE_ALPHA_COLLECTOR_TIMEOUT_SEC: float = 10.0
    BINANCE_ALPHA_COLLECTOR_KLINE_LIMIT: int = 300
    BINANCE_ALPHA_COLLECTOR_INCREMENTAL_KLINE_LIMIT: int = 50
    BINANCE_ALPHA_COLLECTOR_KLINE_INTERVALS: str = "1m,5m,15m,1h,4h,1d"
    BINANCE_ALPHA_COLLECTOR_AUX_ENABLED: bool = True
    BINANCE_ALPHA_COLLECTOR_AUX_INTERVAL_SEC: int = 60
    BINANCE_ALPHA_COLLECTOR_AUX_TOP_N: int = 60
    BINANCE_ALPHA_COLLECTOR_TRADES_LIMIT: int = 1000
    BINANCE_ALPHA_COLLECTOR_ORDERBOOK_LIMIT: int = 20
    BINANCE_ALPHA_COLLECTOR_ERROR_BACKOFF_SEC: int = 900
    BINANCE_ALPHA_COLLECTOR_STARTUP_DELAY_SEC: int = 15
    BINANCE_ALPHA_COLLECTOR_PRUNE_INTERVAL_SEC: int = 3600
    BINANCE_ALPHA_COLLECTOR_KLINE_RETENTION_DAYS: int = 14
    BINANCE_ALPHA_COLLECTOR_KLINE_MIN_BARS: int = 1200
    BINANCE_ALPHA_COLLECTOR_TRADE_RETENTION_HOURS: int = 6
    BINANCE_ALPHA_COLLECTOR_ORDERBOOK_RETENTION_HOURS: int = 24
    BINANCE_ALPHA_COLLECTOR_MARKET_RETENTION_HOURS: int = 24
    BINANCE_ALPHA_COLLECTOR_RUN_RETENTION_DAYS: int = 30
    BINANCE_ALPHA_HISTORY_MAX_MB: int = 256
    PUBLIC_MACRO_WORKERS_ENABLED: bool = False
    PREMIUM_EXTERNAL_WORKERS_ENABLED: bool = False

    # LLM API
    ZHIPU_API_KEY: str = ""
    ZHIPU_BASE_URL: str = ""
    ZHIPU_MODEL: str = ""
    OPENAI_API_KEY: str = ""
    OPENAI_BASE_URL: str = "https://nowcoding.ai/v1"
    OPENAI_BACKUP_BASE_URL: str = "https://fast.vpsairobot.com"
    OPENAI_BACKUP_API_KEY: str = ""
    OPENAI_MODEL: str = "gpt-5.5"
    OPENAI_BACKUP_MODEL: str = "gpt-5.5"
    AI_RESEARCH_MODEL: str = "gpt-5.6-sol"
    AI_RESEARCH_BACKUP_MODEL: str = "gpt-5.6-sol"
    ANTHROPIC_API_KEY: str = ""
    ANTHROPIC_BASE_URL: str = ""
    ANTHROPIC_MODEL: str = ""
    NEWS_LLM_PROVIDER: str = "openai"
    NEWS_LLM_API_KEY: str = ""
    NEWS_LLM_BASE_URL: str = ""
    NEWS_LLM_MODEL: str = ""
    NEWS_LLM_BACKUP_API_KEY: str = ""
    NEWS_LLM_BACKUP_BASE_URL: str = ""
    NEWS_LLM_BACKUP_MODEL: str = ""
    NEWS_LLM_FORCE_CHAT_COMPLETIONS: bool = False

    # Storage
    DATABASE_URL: str = Field(default_factory=_default_database_url)
    NEWS_DATABASE_URL: str = Field(default_factory=_default_news_database_url)
    REDIS_URL: str = "redis://localhost:6379/0"

    # Network proxy
    HTTP_PROXY: Optional[str] = None
    HTTPS_PROXY: Optional[str] = None

    # Trading
    TRADING_MODE: str = "paper"  # paper/live
    ALLOW_PERSISTED_LIVE_MODE_START: bool = False
    MAX_POSITION_SIZE: float = 0.1
    MAX_DAILY_LOSS: float = 0.02
    MAX_OPEN_POSITIONS: int = 100
    POSITION_HISTORY_LIMIT: int = 5000
    MIN_STRATEGY_ORDER_USD: float = 100.0
    DEFAULT_STRATEGY_ALLOCATION: float = 0.15
    STRATEGY_DEFAULT_STOP_LOSS_PCT: float = 0.03
    STRATEGY_DEFAULT_TAKE_PROFIT_PCT: float = 0.06
    PAPER_INITIAL_EQUITY: float = 10000.0
    PAPER_FEE_RATE: float = 0.001
    PAPER_SLIPPAGE_BPS: float = 2.0
    LIVE_FEE_RATE: float = 0.0004
    LIVE_SLIPPAGE_BPS: float = 2.0
    RISK_FREE_RATE: float = 0.02
    GOVERNANCE_ENABLED: bool = False
    DECISION_MODE: str = "shadow"  # shadow/paper/live
    REQUIRE_DUAL_APPROVAL_FOR_LIVE: bool = True
    AUDIT_LEVEL: str = "full"  # full/minimal
    AI_LIVE_DECISION_ENABLED: bool = False
    AI_LIVE_DECISION_MODE: str = "shadow"  # shadow/enforce
    AI_LIVE_DECISION_PROVIDER: str = "codex"  # glm/codex(openai-compatible)/claude
    AI_LIVE_DECISION_MODEL: str = ""  # optional provider-specific override
    AI_LIVE_DECISION_TIMEOUT_MS: int = 6000
    AI_LIVE_DECISION_MAX_TOKENS: int = 220
    AI_LIVE_DECISION_TEMPERATURE: float = 0.0
    AI_LIVE_DECISION_FAIL_OPEN: bool = False
    AI_LIVE_DECISION_APPLY_IN_PAPER: bool = False
    AI_MARKET_STATE_RISK_POSTURE_ENABLED: bool = True
    AI_MARKET_STATE_RISK_POSTURE_LIVE_ENFORCE: bool = False
    AI_AUTONOMOUS_AGENT_ENABLED: bool = False
    AI_AUTONOMOUS_AGENT_AUTO_START: bool = False
    AI_AUTONOMOUS_AGENT_MODE: str = "shadow"  # shadow/execute
    AI_AUTONOMOUS_AGENT_PROVIDER: str = "codex"  # glm/codex(openai-compatible)/claude
    AI_AUTONOMOUS_AGENT_MODEL: str = ""
    AI_AUTONOMOUS_AGENT_EXCHANGE: str = "binance"
    AI_AUTONOMOUS_AGENT_SYMBOL: str = "BTC/USDT"
    AI_AUTONOMOUS_AGENT_SYMBOL_MODE: str = "manual"  # manual/auto
    AI_AUTONOMOUS_AGENT_UNIVERSE_SYMBOLS: str = (
        "BTC/USDT,ETH/USDT,BNB/USDT,SOL/USDT,XRP/USDT,DOGE/USDT,ADA/USDT,LINK/USDT,"
        "AVAX/USDT,DOT/USDT,LTC/USDT,BCH/USDT,TRX/USDT,UNI/USDT,ATOM/USDT,FIL/USDT,"
        "ETC/USDT,ICP/USDT,APT/USDT,NEAR/USDT,ARB/USDT,OP/USDT,SUI/USDT,INJ/USDT,"
        "AAVE/USDT,RUNE/USDT,SEI/USDT,TIA/USDT,SHIB/USDT,PEPE/USDT"
    )
    AI_AUTONOMOUS_AGENT_SELECTION_TOP_N: int = 10
    AI_AUTONOMOUS_AGENT_TIMEFRAME: str = "15m"
    AI_AUTONOMOUS_AGENT_INTERVAL_SEC: int = 120
    AI_AUTONOMOUS_AGENT_LOOKBACK_BARS: int = 240
    AI_AUTONOMOUS_AGENT_MIN_CONFIDENCE: float = 0.58
    AI_AUTONOMOUS_AGENT_DEFAULT_LEVERAGE: float = 1.0
    AI_AUTONOMOUS_AGENT_MAX_LEVERAGE: float = 1.0
    AI_AUTONOMOUS_AGENT_STOP_LOSS_PCT: float = 0.02
    AI_AUTONOMOUS_AGENT_TAKE_PROFIT_PCT: float = 0.04
    AI_AUTONOMOUS_AGENT_TIMEOUT_MS: int = 30000
    AI_AUTONOMOUS_AGENT_MAX_TOKENS: int = 420
    AI_AUTONOMOUS_AGENT_TEMPERATURE: float = 0.15
    AI_AUTONOMOUS_AGENT_COOLDOWN_SEC: int = 180
    AI_AUTONOMOUS_AGENT_MAX_TOTAL_EXPOSURE_RATIO: float = 0.4
    AI_AUTONOMOUS_AGENT_MAX_TOTAL_EXPOSURE_USDT: Optional[float] = None
    AI_AUTONOMOUS_AGENT_ALLOW_LIVE: bool = False
    AI_AUTONOMOUS_AGENT_ACCOUNT_ID: str = "main"
    AI_AUTONOMOUS_AGENT_STRATEGY_NAME: str = "AI_AutonomousAgent"
    AI_AUTONOMOUS_AGENT_DAILY_STOP_BUFFER_RATIO: Optional[float] = None
    AI_AUTONOMOUS_AGENT_MAX_DRAWDOWN_REDUCE_ONLY: Optional[float] = None
    AI_AUTONOMOUS_AGENT_ROLLING_3D_DRAWDOWN_REDUCE_ONLY: Optional[float] = None
    AI_AUTONOMOUS_AGENT_ROLLING_7D_DRAWDOWN_REDUCE_ONLY: Optional[float] = None

    # Portfolio / per-strategy circuit breaker (Phase 4.2)
    CIRCUIT_BREAKER_ENABLED: bool = True
    CB_STRATEGY_DAILY_DD_PCT: float = 0.05
    CB_STRATEGY_WEEKLY_DD_PCT: float = 0.10
    CB_PORTFOLIO_DAILY_DD_PCT: float = 0.03
    CB_PORTFOLIO_WEEKLY_DD_PCT: float = 0.06
    CB_MONITOR_INTERVAL_SEC: int = 60

    # Exchange market type (spot/future/swap/margin)
    BINANCE_DEFAULT_TYPE: str = "spot"
    OKX_DEFAULT_TYPE: str = "spot"
    GATE_DEFAULT_TYPE: str = "spot"
    BYBIT_DEFAULT_TYPE: str = "spot"
    EXCHANGE_STARTUP_CONNECT_TIMEOUT_SEC: float = 18.0
    EXCHANGE_WATCHDOG_ENABLED: bool = True
    # Live kline fetch (strategy runtime). On timeout/failure the (exchange,
    # symbol, timeframe) is put in backoff and served from local/cache data
    # instead of re-hammering a slow endpoint every cycle.
    # 8s/30s (was 12s/90s): fail-fast on a slow endpoint and recover within a
    # couple of bars instead of pulling strategies off the live feed for 90s.
    LIVE_KLINE_FETCH_TIMEOUT_SEC: float = 8.0
    LIVE_KLINE_FETCH_BACKOFF_SEC: float = 30.0
    # Real-time market-data WebSocket feed (ccxt.pro). When enabled, ticker
    # updates are pushed over a persistent socket instead of polled via REST;
    # the REST fan-out becomes an automatic fallback while the socket is down.
    MARKET_WS_ENABLED: bool = True
    MARKET_WS_MODE: str = "strategy_primary"  # off/shadow/ui_primary/strategy_primary
    MARKET_WS_FORCE_REST: bool = False
    MARKET_WS_EXCHANGES: str = "binance"  # comma-separated; blank = all connected
    MARKET_WS_WATCH_TIMEOUT_SEC: float = 25.0
    MARKET_WS_HEALTH_MAX_AGE_SEC: float = 15.0
    MARKET_WS_SYMBOL_LIMIT: int = 16
    MARKET_WS_SYMBOL_MAX_AGE_SEC: float = 60.0
    MARKET_WS_RECONNECT_MIN_SEC: float = 1.0
    MARKET_WS_RECONNECT_MAX_SEC: float = 30.0
    MARKET_WS_REST_RECONCILE_SEC: float = 30.0
    MARKET_WS_MAX_PRICE_DIFF_BPS: float = 20.0
    MARKET_WS_FAIL_CLOSED_FOR_LIVE: bool = True
    MARKET_WS_MARK_PRICE_ENABLED: bool = False
    # Auto-degrade WS->REST on sustained quality loss in ui_primary/strategy_primary.
    MARKET_WS_QUALITY_GUARD_ENABLED: bool = True
    # When True, a parquet index that looks local-stamped (runs ahead of real
    # UTC) raises instead of being silently shifted — use to flush out any
    # remaining non-UTC kline writer in CI / debugging.
    PARQUET_TZ_STRICT: bool = False
    # When True, the backtest page builds positions by replaying the real
    # strategy class's generate_signals (matches live runtime). Set False to
    # revert to the legacy vectorized _build_positions model for emergency
    # rollback only — historical numbers from that path do not predict live.
    BACKTEST_USE_REAL_STRATEGY: bool = True

    # When True, the real-strategy replay loop builds its trailing window via
    # an iloc slice (a view) rather than .tail().copy() — but ONLY for strategy
    # classes that explicitly declare `mutates_input = False`. Strategies that
    # write into the input DataFrame (e.g. MultiFactorHFStrategy) keep the
    # safe copy path regardless of this flag. Set False to force the legacy
    # copy-everywhere behavior if a strategy bug surfaces.
    BACKTEST_REPLAY_VIEW_FAST_PATH: bool = True

    # Phase 2 fast_exact strategy paths. When True, strategies with a
    # vectorized batch implementation AND a passing bar-by-bar parity test
    # use the fast path; otherwise the trusted per-bar replay runs as
    # before. Default off so the trusted path remains the source of truth
    # until the user explicitly opts in. Currently supports:
    #     MultiFactorHFStrategy (strategies/quantitative/multi_factor_hf_fast.py)
    # The fast path is locked by tests/test_multi_factor_hf_parity.py
    # against the trusted replay on five regime fixtures.
    BACKTEST_FAST_EXACT_STRATEGIES: bool = False

    # Phase 3 array execution simulator. When True and the resolved
    # exit-engine config falls into the supported subset (no stops, no
    # take profits, no breakeven, no partial TP, no trailing, no time
    # stop — i.e. signal_reversal_exit only), the backtest page uses
    # core/backtest/execution_arrays.simulate_execution_arrays instead
    # of run_exit_engine. Any other config falls back to the trusted
    # engine. Locked by tests/test_execution_arrays_parity.py.
    BACKTEST_FAST_EXIT_ARRAYS: bool = False

    # Phase 5 optimize parallelism. Number of worker processes for
    # _optimize_strategy_on_df trials. Default 1 = serial (current
    # behavior); set higher to spread trials across CPU cores. Bench
    # numbers: at 32 trials, scaling is near-linear up to 4 workers
    # on a 4-core machine before the per-worker spawn cost dominates.
    # Each pool spawn pays ~1s of Python startup cost on Windows, so
    # below BACKTEST_OPTIMIZE_PARALLEL_MIN_TRIALS we stay serial.
    BACKTEST_OPTIMIZE_WORKERS: int = 1
    BACKTEST_OPTIMIZE_PARALLEL_MIN_TRIALS: int = 8

    # Web server
    WEB_HOST: str = "127.0.0.1"
    WEB_PORT: int = 8000
    WEB_SECRET_KEY: str = "change_this_secret_key_in_production"
    OPS_TOKEN: str = ""
    RBAC_SECRET: str = ""
    # CORS whitelist; explicit origins, never "*" when allow_credentials=True
    WEB_ALLOWED_ORIGINS: List[str] = [
        "http://localhost:8000",
        "http://127.0.0.1:8000",
    ]

    # Notification
    TELEGRAM_BOT_TOKEN: Optional[str] = None
    TELEGRAM_CHAT_ID: Optional[str] = None
    WECHAT_WEBHOOK_URL: Optional[str] = None
    FEISHU_BOT_WEBHOOK_URL: Optional[str] = None
    FEISHU_BOT_SECRET: Optional[str] = None
    FEISHU_APP_ID: Optional[str] = None
    FEISHU_APP_SECRET: Optional[str] = None
    FEISHU_RECEIVE_ID: Optional[str] = None
    FEISHU_RECEIVE_ID_TYPE: str = "chat_id"  # chat_id/open_id/user_id/email/union_id
    EMAIL_SMTP_SERVER: Optional[str] = None
    EMAIL_SMTP_PORT: int = 587
    EMAIL_USE_TLS: bool = True
    EMAIL_USE_SSL: bool = False
    EMAIL_TIMEOUT_SEC: int = 15
    EMAIL_REQUIRE_AUTH: bool = True
    EMAIL_SENDER: Optional[str] = None
    EMAIL_PASSWORD: Optional[str] = None
    EMAIL_RECEIVER: Optional[str] = None

    # Data
    DEFAULT_TIMEFRAME: str = "1h"
    SUPPORTED_TIMEFRAMES: List[str] = [
        "1m",
        "3m",
        "5m",
        "15m",
        "30m",
        "1h",
        "2h",
        "4h",
        "6h",
        "12h",
        "1d",
        "3d",
        "1w",
        "1M",
    ]
    MAX_CANDLES_PER_REQUEST: int = 1000
    DATA_DOWNLOAD_MAX_CONCURRENT_TASKS: int = 2
    DATA_DOWNLOAD_TASK_RETENTION: int = 400
    DATA_DOWNLOAD_TASK_TIMEOUT_SEC: int = 1800
    ANALYTICS_HISTORY_ENABLED: bool = False
    ANALYTICS_HISTORY_MICRO_INTERVAL_SEC: int = 300
    ANALYTICS_HISTORY_COMMUNITY_INTERVAL_SEC: int = 900
    ANALYTICS_HISTORY_WHALE_INTERVAL_SEC: int = 600

    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_ROTATION: str = "10 MB"
    LOG_RETENTION: str = "30 days"

    @field_validator("TRADING_MODE")
    @classmethod
    def validate_trading_mode(cls, v: str) -> str:
        if v not in ("paper", "live"):
            raise ValueError("TRADING_MODE must be 'paper' or 'live'")
        return v

    @field_validator("DECISION_MODE")
    @classmethod
    def validate_decision_mode(cls, v: str) -> str:
        text = str(v or "shadow").strip().lower()
        if text not in {"shadow", "paper", "live"}:
            raise ValueError("DECISION_MODE must be one of: shadow/paper/live")
        return text

    @field_validator("AUDIT_LEVEL")
    @classmethod
    def validate_audit_level(cls, v: str) -> str:
        text = str(v or "full").strip().lower()
        if text not in {"full", "minimal"}:
            raise ValueError("AUDIT_LEVEL must be one of: full/minimal")
        return text

    @field_validator("AI_LIVE_DECISION_MODE")
    @classmethod
    def validate_ai_live_decision_mode(cls, v: str) -> str:
        text = str(v or "shadow").strip().lower()
        if text not in {"shadow", "enforce"}:
            raise ValueError("AI_LIVE_DECISION_MODE must be one of: shadow/enforce")
        return text

    @field_validator("AI_LIVE_DECISION_PROVIDER")
    @classmethod
    def validate_ai_live_decision_provider(cls, v: str) -> str:
        text = str(v or "codex").strip().lower()
        aliases = {"openai": "codex"}
        text = aliases.get(text, text)
        if text not in {"glm", "codex", "claude"}:
            raise ValueError("AI_LIVE_DECISION_PROVIDER must be one of: glm/codex(openai)/claude")
        return text

    @field_validator("AI_AUTONOMOUS_AGENT_MODE")
    @classmethod
    def validate_ai_autonomous_agent_mode(cls, v: str) -> str:
        text = str(v or "shadow").strip().lower()
        if text not in {"shadow", "execute"}:
            raise ValueError("AI_AUTONOMOUS_AGENT_MODE must be one of: shadow/execute")
        return text

    @field_validator("AI_AUTONOMOUS_AGENT_PROVIDER")
    @classmethod
    def validate_ai_autonomous_agent_provider(cls, v: str) -> str:
        text = str(v or "codex").strip().lower()
        aliases = {"openai": "codex"}
        text = aliases.get(text, text)
        if text not in {"glm", "codex", "claude"}:
            raise ValueError("AI_AUTONOMOUS_AGENT_PROVIDER must be one of: glm/codex(openai)/claude")
        return text

    @field_validator("DATABASE_URL", "NEWS_DATABASE_URL")
    @classmethod
    def normalize_database_url(cls, v: str) -> str:
        return _normalize_sqlite_database_url(v)

    @field_validator(
        "BINANCE_DEFAULT_TYPE",
        "OKX_DEFAULT_TYPE",
        "GATE_DEFAULT_TYPE",
        "BYBIT_DEFAULT_TYPE",
    )
    @classmethod
    def validate_exchange_default_type(cls, v: str) -> str:
        text = str(v or "spot").strip().lower()
        aliases = {
            "futures": "future",
            "perp": "swap",
            "perpetual": "swap",
        }
        text = aliases.get(text, text)
        if text not in {"spot", "future", "swap", "margin"}:
            raise ValueError("exchange default type must be one of: spot/future/swap/margin")
        return text

    @field_validator("MARKET_WS_MODE")
    @classmethod
    def validate_market_ws_mode(cls, v: str) -> str:
        text = str(v or "off").strip().lower()
        if text not in {"off", "shadow", "ui_primary", "strategy_primary"}:
            raise ValueError("MARKET_WS_MODE must be one of: off/shadow/ui_primary/strategy_primary")
        return text

    @field_validator("EXCHANGE_STARTUP_CONNECT_TIMEOUT_SEC")
    @classmethod
    def validate_exchange_startup_connect_timeout_sec(cls, v: float) -> float:
        value = float(v or 0.0)
        if value < 0:
            raise ValueError("EXCHANGE_STARTUP_CONNECT_TIMEOUT_SEC must be >= 0")
        return value

    @field_validator("DATA_STORAGE_PATH", "CACHE_PATH", "LOG_PATH", mode="before")
    @classmethod
    def normalize_path_fields(cls, v: object) -> Path:
        if isinstance(v, Path):
            path = v
        elif isinstance(v, str):
            path = Path(v)
        else:
            raise TypeError(f"unsupported path value type: {type(v)!r}")
        if not path.is_absolute():
            path = (_PROJECT_ROOT / path).resolve()
        else:
            path = path.resolve()
        return path


settings = Settings()
