"""Global application settings."""
from pathlib import Path
from typing import List, Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment and .env."""

    model_config = SettingsConfigDict(
        env_file=str(Path(__file__).parent.parent / ".env"),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Project paths
    BASE_DIR: Path = Field(default_factory=lambda: Path(__file__).parent.parent)
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

    # Storage
    DATABASE_URL: str = "sqlite+aiosqlite:///./data/crypto_trading.db"
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
    PAPER_INITIAL_EQUITY: float = 10000.0
    PAPER_FEE_RATE: float = 0.001
    PAPER_SLIPPAGE_BPS: float = 2.0
    RISK_FREE_RATE: float = 0.02

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
