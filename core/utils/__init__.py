"""
工具模块
"""
from datetime import datetime, timezone
from typing import Optional
import hashlib
import secrets
import string


def generate_id(length: int = 16) -> str:
    """生成随机ID"""
    chars = string.ascii_letters + string.digits
    return ''.join(secrets.choice(chars) for _ in range(length))


def hash_string(s: str) -> str:
    """哈希字符串"""
    return hashlib.sha256(s.encode()).hexdigest()


def utc_now() -> datetime:
    """获取UTC时间"""
    return datetime.now(timezone.utc)


def format_timestamp(dt: datetime, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """格式化时间戳"""
    return dt.strftime(fmt)


def parse_timestamp(s: str, fmt: str = "%Y-%m-%d %H:%M:%S") -> datetime:
    """解析时间戳"""
    return datetime.strptime(s, fmt)


def safe_float(value, default: float = 0.0) -> float:
    """安全转换为浮点数"""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value, default: int = 0) -> int:
    """安全转换为整数"""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
