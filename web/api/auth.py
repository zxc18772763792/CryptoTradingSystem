from __future__ import annotations

import hashlib
import hmac
from urllib.parse import urlparse

from fastapi import Request, Response

from core.ops.service.auth import OpsAuthContext, get_ops_token, require_ops_auth

_LOCAL_UI_COOKIE_NAME = "cts_local_ui_session"
_LOCAL_UI_COOKIE_MAX_AGE_SEC = 8 * 3600
_LOCAL_UI_ACTOR = "web_ui_local"
_LOOPBACK_HOSTS = {"127.0.0.1", "::1", "localhost"}


def _request_client_ip(request: Request) -> str:
    try:
        if request.client:
            return str(request.client.host or "").strip().lower()
    except Exception:
        return ""
    return ""


def _request_host(request: Request) -> str:
    raw_host = str(request.headers.get("host") or "").strip().lower()
    if raw_host.startswith("[") and "]" in raw_host:
        return raw_host[1 : raw_host.find("]")]
    if ":" in raw_host:
        return raw_host.split(":", 1)[0]
    return raw_host or str(getattr(request.url, "hostname", "") or "").strip().lower()


def _is_loopback_request(request: Request) -> bool:
    return _request_client_ip(request) in _LOOPBACK_HOSTS and _request_host(request) in _LOOPBACK_HOSTS


def _local_ui_cookie_value(request: Request) -> str:
    secret = str(get_ops_token(required=False) or "").strip()
    if not secret:
        return ""
    host = _request_host(request) or "localhost"
    payload = f"local-ui-session:{host}"
    return hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()


def _same_origin_loopback(request: Request, value: str) -> bool:
    text = str(value or "").strip()
    if not text:
        return True
    parsed = urlparse(text)
    host = str(parsed.hostname or "").strip().lower()
    if not host:
        return False
    return host == _request_host(request) and host in _LOOPBACK_HOSTS


def set_local_ui_session_cookie(request: Request, response: Response) -> None:
    cookie_value = _local_ui_cookie_value(request)
    if not cookie_value or not _is_loopback_request(request):
        return
    response.set_cookie(
        _LOCAL_UI_COOKIE_NAME,
        cookie_value,
        httponly=True,
        samesite="strict",
        secure=str(getattr(request.url, "scheme", "") or "").lower() == "https",
        max_age=_LOCAL_UI_COOKIE_MAX_AGE_SEC,
        path="/",
    )


def _has_valid_local_ui_session(request: Request) -> bool:
    if not _is_loopback_request(request):
        return False
    expected = _local_ui_cookie_value(request)
    received = str(request.cookies.get(_LOCAL_UI_COOKIE_NAME) or "").strip()
    if not expected or not received or not hmac.compare_digest(received, expected):
        return False
    origin = str(request.headers.get("origin") or "").strip()
    if origin and not _same_origin_loopback(request, origin):
        return False
    referer = str(request.headers.get("referer") or "").strip()
    if referer and not _same_origin_loopback(request, referer):
        return False
    return True


async def require_sensitive_ops_auth(request: Request) -> OpsAuthContext:
    if _has_valid_local_ui_session(request):
        ctx = OpsAuthContext(
            actor=_LOCAL_UI_ACTOR,
            role="SYSTEM",
            token_present=False,
            api_key_present=False,
            client_ip=_request_client_ip(request),
        )
        request.state.ops_auth = ctx
        return ctx
    return await require_ops_auth(request)

