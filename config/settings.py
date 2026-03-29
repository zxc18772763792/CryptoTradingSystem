"""Global application settings."""
from pathlib import Path
from typing import List, Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


_PROJECT_ROOT = Path(__file__).parent.parent
_SQLITE_ASYNC_PREFIX = "sqlite+aiosqlite:///"


def _default_database_url() -> str:
    return f"{_SQLITE_ASYNC_PREFIX}{(_PROJECT_ROOT / 'data' / 'crypto_trading.db').resolve().as_posix()}"


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

    # LLM API
    ZHIPU_API_KEY: str = ""
    ZHIPU_BASE_URL: str = "https://open.bigmodel.cn/api/coding/paas/v4"
    ZHIPU_MODEL: str = "GLM-4.5-Air"
    OPENAI_API_KEY: str = ""
    OPENAI_BASE_URL: str = "https://vpsairobot.com/v1"
    OPENAI_BACKUP_BASE_URL: str = ""
    OPENAI_BACKUP_API_KEY: str = ""
    OPENAI_MODEL: str = "gpt-5.4"
    ANTHROPIC_API_KEY: str = ""
    ANTHROPIC_BASE_URL: str = "https://api.anthropic.com"
    ANTHROPIC_MODEL: str = "claude-3-5-sonnet-latest"

    # Storage
    DATABASE_URL: str = Field(default_factory=_default_database_url)
    REDIS_URL: str = "redis://localhost:6379/0"

    # Network proxy
    HTTP_PROXY: Optional[str] = None
    HTTPS_PROXY: Optional[str] = None

    # Trading
    TRADING_MODE: str = "paper"  # paper/live
    MAX_POSITION_SIZE: float = 0.1
    MAX_DAILY_LOSS: float = 0.02
    MAX_OPEN_POSITIONS: int = 100
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
    AI_LIVE_DECISION_FAIL_OPEN: bool = True
    AI_LIVE_DECISION_APPLY_IN_PAPER: bool = False
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
    AI_AUTONOMOUS_AGENT_ALLOW_LIVE: bool = False
    AI_AUTONOMOUS_AGENT_ACCOUNT_ID: str = "main"
    AI_AUTONOMOUS_AGENT_STRATEGY_NAME: str = "AI_AutonomousAgent"

    # Exchange market type (spot/future/swap/margin)
    BINANCE_DEFAULT_TYPE: str = "spot"
    OKX_DEFAULT_TYPE: str = "spot"
    GATE_DEFAULT_TYPE: str = "spot"
    BYBIT_DEFAULT_TYPE: str = "spot"

    # Web server
    WEB_HOST: str = "0.0.0.0"
    WEB_PORT: int = 8000
    WEB_SECRET_KEY: str = "change_this_secret_key_in_production"

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
    ANALYTICS_HISTORY_ENABLED: bool = True
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

    @field_validator("DATABASE_URL")
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

    @field_validator("DATA_STORAGE_PATH", "CACHE_PATH", "LOG_PATH", mode="before")
    @classmethod
    def normalize_path_fields(cls, v: object) -> Path:
        if isinstance(v, Path):
            return v
        if isinstance(v, str):
            return Path(v)
        raise TypeError(f"unsupported path value type: {type(v)!r}")


settings = Settings()
