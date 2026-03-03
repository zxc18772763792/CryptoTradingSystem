"""News eventization module."""
from core.news.eventizer.rate_limiter import (
    RateLimiter,
    rate_limit,
)

__all__ = ["RateLimiter", "rate_limit"]
