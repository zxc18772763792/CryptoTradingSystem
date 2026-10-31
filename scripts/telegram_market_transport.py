"""Bounded public-market transport outside the frozen Telegram decision code.

Same Binance endpoints and configured proxy; certificate checks stay enabled.
No account API, proxy fallback, retries on refusals, or private message logging.
"""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time
from urllib.parse import quote
from uuid import uuid4

import httpx

ROOT = Path(__file__).resolve().parents[1]
ENDPOINTS = {"/fapi/v1/time", "/fapi/v1/klines", "/fapi/v1/fundingRate"}
PUBLIC_PARAMS = {"symbol", "interval", "startTime", "endTime", "limit"}


class PublicMarketTransport:
    def __init__(self, proxy_factory, *, client_factory=None, sleep=time.sleep, evidence_root=None):
        self.proxy_factory = proxy_factory
        self.client_factory = client_factory or httpx.Client
        self.sleep = sleep
        self.root = evidence_root or ROOT / "data/research/telegram_market_transport"
        self.client = None
        self.proxy_url = None
        self.folder = None
        self.sequence = 0

    def _client(self):
        if self.client is None:
            from core.utils.shared_ssl import get_shared_ssl_context

            if self.proxy_url is None:
                protocol, host, port, _, username, password = self.proxy_factory()
                if protocol not in ("http", "socks5"):
                    raise ValueError("Unsupported explicit market proxy; direct fallback disabled")
                host = f"[{host}]" if ":" in host else host
                auth = f"{quote(username, safe='')}:{quote(password or '', safe='')}@" if username else ""
                scheme = "socks5h" if protocol == "socks5" else protocol
                self.proxy_url = f"{scheme}://{auth}{host}:{port}"
            self.client = self.client_factory(
                proxy=self.proxy_url, verify=get_shared_ssl_context(),
                timeout=httpx.Timeout(10.0, connect=5.0), trust_env=False,
                limits=httpx.Limits(max_connections=2, max_keepalive_connections=2, keepalive_expiry=60))
        return self.client

    def _receipt(self, endpoint, params, attempt, started, response=None, error=None):
        if self.folder is None:
            self.folder = Path(self.root) / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid4().hex[:12])
            self.folder.mkdir(parents=True, exist_ok=False)
        self.sequence += 1
        stem = f"{self.sequence:04d}"
        rec = {"url": "https://fapi.binance.com" + endpoint, "params": params,
               "attempt": attempt, "started_at_utc": started,
               "received_at_utc": datetime.now(timezone.utc).isoformat(),
               "tls_verification": True, "proxy": "existing_explicit_configuration",
               "error_type": type(error).__name__ if error else None}
        if response is not None:
            body = response.content
            rec.update(http_status=response.status_code, raw_sha256=hashlib.sha256(body).hexdigest(),
                       raw_path=stem + ".raw", raw_bytes=len(body))
            with (self.folder / (stem + ".raw")).open("xb") as f:
                f.write(body)
        with (self.folder / (stem + ".json")).open("x", encoding="utf-8") as f:
            json.dump(rec, f, ensure_ascii=False, indent=2, allow_nan=False)

    def get_json(self, endpoint, params=None):
        params = dict(params or {})
        if endpoint not in ENDPOINTS or set(params) - PUBLIC_PARAMS:
            raise ValueError("Only frozen public market endpoints/parameters allowed")
        for attempt in range(1, 4):
            started = datetime.now(timezone.utc).isoformat()
            response = None
            try:
                # The frozen caller compares server time with its pre-request
                # clock. Bound all three time probes below its 10s clock gate.
                extra = {"timeout": httpx.Timeout(2.0)} if endpoint == "/fapi/v1/time" else {}
                response = self._client().get("https://fapi.binance.com" + endpoint, params=params, **extra)
                response.raise_for_status()
                result = response.json()
            except Exception as exc:
                self._receipt(endpoint, params, attempt, started, response, exc)
                retryable = isinstance(exc, httpx.TransportError) or (
                    isinstance(exc, httpx.HTTPStatusError) and response.status_code in (408, 500, 502, 503, 504))
                if not retryable or attempt == 3:
                    raise
                if isinstance(exc, httpx.TransportError):
                    # Broken proxy/TLS handshakes can retain pool slots. Reap
                    # that pool before retrying, keeping the identical proxy.
                    self.close()
                self.sleep(.5 * attempt)
            else:
                self._receipt(endpoint, params, attempt, started, response)
                return result

    def close(self):
        if self.client is not None:
            self.client.close()
            self.client = None
