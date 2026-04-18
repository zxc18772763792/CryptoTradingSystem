from __future__ import annotations

import asyncio
import re
from typing import Any, Dict, Optional


def extract_model_feedback_http_status(exc: Any) -> Optional[int]:
    text = str(exc or "").strip().lower()
    if not text:
        return None
    match = re.search(r"_http_(\d{3})", text)
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


def classify_model_feedback_error(exc: BaseException) -> Optional[str]:
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return "timeout"

    text = str(exc or "").strip().lower()
    if not text:
        return None

    status = extract_model_feedback_http_status(text)

    policy_markers = (
        "live trading is not permitted",
        "live_trading_not_permitted",
        "live trading not permitted",
        "investment transactions",
        "policy_violation",
        "policy violation",
        "safety",
        "not permitted",
        "not allowed",
        "disallowed",
    )
    auth_markers = (
        "invalid api key",
        "invalid_api_key",
        "api key not valid",
        "unauthorized",
        "authentication",
        "auth failed",
        "api_key_missing",
    )
    unsupported_model_markers = (
        "not supported model",
        "unsupported model",
        "model_not_found",
        "unknown model",
        "does not exist",
    )
    permission_markers = (
        "permission denied",
        "forbidden",
        "access denied",
        "insufficient permissions",
    )
    bad_request_markers = (
        "param incorrect",
        "invalid request",
        "bad request",
        "unsupported parameter",
        "invalid parameter",
    )

    if "timeout" in text:
        return "timeout"
    if status == 429 or "_http_429" in text or "usage_limit_exceeded" in text or "rate limit" in text or "too many requests" in text or "insufficient_quota" in text:
        return "rate_limit"
    if any(marker in text for marker in policy_markers):
        return "policy_restricted"
    if status == 401 or any(marker in text for marker in auth_markers):
        return "auth_error"
    if any(marker in text for marker in unsupported_model_markers):
        return "unsupported_model"
    if status == 403 or any(marker in text for marker in permission_markers):
        return "permission_denied"
    if status == 400 or any(marker in text for marker in bad_request_markers):
        return "bad_request"
    if status in {502, 503, 504} or "service temporarily unavailable" in text or "service unavailable" in text:
        return "service_unavailable"
    return None


def describe_model_feedback_issue(
    raw_error: Any,
    *,
    fallback_action: str = "hold",
    max_error_chars: int = 220,
) -> Dict[str, Any]:
    normalized = str(raw_error or "").strip()
    if normalized.startswith("model_error:"):
        normalized = normalized.split("model_error:", 1)[1].strip()

    kind = classify_model_feedback_error(RuntimeError(normalized or ""))
    http_status = extract_model_feedback_http_status(normalized)
    action_text = str(fallback_action or "hold").strip() or "hold"

    if kind == "rate_limit":
        label = "模型限流或额度受限 (429)"
        detail = f"上游模型接口触发了频率或额度限制，本轮已回退为 {action_text}。"
        code = "model_rate_limit"
    elif kind == "service_unavailable":
        status_suffix = f" ({http_status})" if http_status else ""
        label = f"模型服务暂时不可用{status_suffix}"
        detail = f"上游模型服务或代理网关暂时不可用，本轮已回退为 {action_text}，稍后会自动重试。"
        code = "model_service_unavailable"
    elif kind == "timeout":
        label = "模型响应超时"
        detail = f"等待模型返回超过超时阈值，本轮已回退为 {action_text}。"
        code = "model_timeout"
    elif kind == "auth_error":
        status_suffix = f" ({http_status})" if http_status else ""
        label = f"模型鉴权失败{status_suffix}"
        detail = f"模型接口认证失败，通常是 API Key 无效、缺失或权限不匹配，本轮已回退为 {action_text}。"
        code = "model_auth_failed"
    elif kind == "permission_denied":
        status_suffix = f" ({http_status})" if http_status else ""
        label = f"模型权限不足或请求被拒绝{status_suffix}"
        detail = f"模型接口拒绝了当前请求，可能是账户权限、网关白名单或访问策略限制，本轮已回退为 {action_text}。"
        code = "model_permission_denied"
    elif kind == "policy_restricted":
        label = "模型策略限制：不允许实盘交易"
        detail = f"当前模型提供方或中间网关拒绝了实盘交易相关请求，本轮已回退为 {action_text}。"
        code = "model_policy_restricted"
    elif kind == "unsupported_model":
        status_suffix = f" ({http_status})" if http_status else ""
        label = f"模型不受支持或未开通{status_suffix}"
        detail = f"当前配置的模型名称无效、未开通，或当前接口不支持该模型，本轮已回退为 {action_text}。"
        code = "model_unsupported"
    elif kind == "bad_request":
        status_suffix = f" ({http_status})" if http_status else ""
        label = f"模型请求参数错误{status_suffix}"
        detail = f"模型接口拒绝了当前请求参数，请检查模型名、参数格式和兼容接口，本轮已回退为 {action_text}。"
        code = "model_bad_request"
    else:
        label = "模型接口异常"
        detail = f"模型接口返回了未分类异常，本轮已回退为 {action_text}。"
        code = "model_error"

    if normalized:
        detail = f"{detail} 原始错误: {normalized[:max(32, int(max_error_chars or 220))]}"

    return {
        "kind": kind,
        "http_status": http_status,
        "label": label,
        "detail": detail,
        "code": code,
        "raw_error": normalized[:300],
    }
