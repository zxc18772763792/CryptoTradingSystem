"""Notification channels and alert rule evaluation."""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import smtplib
import ssl
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import Any, Dict, List, Optional
from uuid import uuid4

import httpx
from loguru import logger
from sqlalchemy import select

from config.database import NotificationRule as NotificationRuleModel
from config.database import async_session_maker
from config.settings import settings


def _normalize_datetime(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


ALTCOIN_SCORE_FIELD_MAP = {
    "layout": "layout_score",
    "alert": "alert_score",
    "anomaly": "anomaly_score",
    "accumulation": "accumulation_score",
    "control": "control_score",
}
ALTCOIN_RANK_SCORE_KEYS = frozenset({"layout", "alert", "control"})


def _normalize_altcoin_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def _altcoin_score_field(
    metric_key: Any, *, allowed_keys: Optional[frozenset[str]] = None
) -> str:
    normalized_key = str(metric_key or "layout").strip().lower()
    if allowed_keys is not None and normalized_key not in allowed_keys:
        return "layout_score"
    return ALTCOIN_SCORE_FIELD_MAP.get(normalized_key, "layout_score")


def _altcoin_rank_text(rank: Any) -> str:
    if not str(rank or "").strip():
        return "当前排名 --"
    try:
        return f"当前排名 {int(rank)}"
    except (TypeError, ValueError):
        return "当前排名 --"


@dataclass
class AlertRule:
    id: str
    name: str
    rule_type: str
    params: Dict[str, Any]
    enabled: bool = True
    cooldown_seconds: int = 300
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    last_triggered_at: Optional[datetime] = None
    trigger_count: int = 0

    @classmethod
    def from_model(cls, model: NotificationRuleModel) -> "AlertRule":
        return cls(
            id=str(model.id),
            name=str(model.name),
            rule_type=str(model.rule_type),
            params=dict(model.params or {}),
            enabled=bool(model.enabled),
            cooldown_seconds=int(model.cooldown_seconds or 300),
            created_at=_normalize_datetime(model.created_at)
            or datetime.now(timezone.utc),
            updated_at=_normalize_datetime(model.updated_at)
            or datetime.now(timezone.utc),
            last_triggered_at=_normalize_datetime(model.last_triggered_at),
            trigger_count=int(model.trigger_count or 0),
        )

    def to_model(self) -> NotificationRuleModel:
        return NotificationRuleModel(
            id=self.id,
            name=self.name,
            rule_type=self.rule_type,
            params=self.params,
            enabled=self.enabled,
            cooldown_seconds=self.cooldown_seconds,
            created_at=_normalize_datetime(self.created_at),
            updated_at=_normalize_datetime(self.updated_at),
            last_triggered_at=_normalize_datetime(self.last_triggered_at),
            trigger_count=self.trigger_count,
        )

    def to_dict(self) -> Dict[str, Any]:
        created_at = _normalize_datetime(self.created_at)
        updated_at = _normalize_datetime(self.updated_at)
        last_triggered_at = _normalize_datetime(self.last_triggered_at)
        return {
            "id": self.id,
            "name": self.name,
            "rule_type": self.rule_type,
            "params": self.params,
            "enabled": self.enabled,
            "cooldown_seconds": self.cooldown_seconds,
            "created_at": created_at.isoformat() if created_at else None,
            "updated_at": updated_at.isoformat() if updated_at else None,
            "last_triggered_at": last_triggered_at.isoformat()
            if last_triggered_at
            else None,
            "trigger_count": self.trigger_count,
        }


class NotificationManager:
    def __init__(self):
        self._rules: Dict[str, AlertRule] = {}
        self._events: List[Dict[str, Any]] = []
        self._loaded = False
        self._load_lock = asyncio.Lock()
        self._feishu_tenant_access_token: Optional[str] = None
        self._feishu_token_expire_at: float = 0.0

    async def _ensure_loaded(self) -> None:
        if self._loaded:
            return

        async with self._load_lock:
            if self._loaded:
                return

            try:
                async with async_session_maker() as session:
                    result = await session.execute(select(NotificationRuleModel))
                    rows = result.scalars().all()
                self._rules = {str(row.id): AlertRule.from_model(row) for row in rows}
                logger.info(f"Notification rules loaded: {len(self._rules)}")
            except Exception as e:
                logger.warning(f"Failed to load notification rules from DB: {e}")
            finally:
                self._loaded = True

    async def _upsert_rule(self, rule: AlertRule) -> None:
        try:
            async with async_session_maker() as session:
                await session.merge(rule.to_model())
                await session.commit()
        except Exception as e:
            logger.warning(f"Failed to persist notification rule {rule.id}: {e}")

    async def _delete_rule(self, rule_id: str) -> None:
        try:
            async with async_session_maker() as session:
                row = await session.get(NotificationRuleModel, rule_id)
                if row is not None:
                    await session.delete(row)
                    await session.commit()
        except Exception as e:
            logger.warning(f"Failed to delete notification rule {rule_id}: {e}")

    def channel_status(self) -> Dict[str, Any]:
        receiver = str(settings.EMAIL_RECEIVER or "")
        receiver_list = [
            x.strip() for x in receiver.replace(";", ",").split(",") if x.strip()
        ]
        email_basic_ready = bool(
            settings.EMAIL_SMTP_SERVER and settings.EMAIL_SENDER and receiver_list
        )
        email_auth_ready = bool(
            (not settings.EMAIL_REQUIRE_AUTH) or settings.EMAIL_PASSWORD
        )
        feishu_webhook_ready = bool(settings.FEISHU_BOT_WEBHOOK_URL)
        feishu_app_ready = bool(
            settings.FEISHU_APP_ID
            and settings.FEISHU_APP_SECRET
            and settings.FEISHU_RECEIVE_ID
        )
        return {
            "telegram": bool(settings.TELEGRAM_BOT_TOKEN and settings.TELEGRAM_CHAT_ID),
            "email": bool(email_basic_ready and email_auth_ready),
            "wechat": bool(settings.WECHAT_WEBHOOK_URL),
            "feishu": bool(feishu_webhook_ready or feishu_app_ready),
        }

    def _record_event(
        self,
        channel: str,
        status: str,
        title: str,
        message: str,
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._events.append(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "channel": channel,
                "status": status,
                "title": title,
                "message": message,
                "meta": meta or {},
            }
        )
        self._events = self._events[-1000:]

    async def _send_telegram(self, title: str, message: str) -> bool:
        token = settings.TELEGRAM_BOT_TOKEN
        chat_id = settings.TELEGRAM_CHAT_ID
        if not token or not chat_id:
            return False

        text = f"【{title}】\n{message}"
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": text}

        try:
            async with httpx.AsyncClient(timeout=12) as client:
                resp = await client.post(url, json=payload)
                ok = resp.status_code == 200
                self._record_event(
                    "telegram",
                    "success" if ok else "failed",
                    title,
                    message,
                    {"status_code": resp.status_code},
                )
                return ok
        except Exception as e:
            self._record_event("telegram", "failed", title, message, {"error": str(e)})
            return False

    def _send_email_sync(self, title: str, message: str) -> bool:
        server = settings.EMAIL_SMTP_SERVER
        sender = settings.EMAIL_SENDER
        password = settings.EMAIL_PASSWORD
        receiver = str(settings.EMAIL_RECEIVER or "")
        receivers = [
            x.strip() for x in receiver.replace(";", ",").split(",") if x.strip()
        ]
        if not server or not sender or not receivers:
            raise RuntimeError("邮箱配置不完整：需要 SMTP 服务器、发件人、收件人")
        if bool(settings.EMAIL_REQUIRE_AUTH) and not password:
            raise RuntimeError("邮箱配置不完整：当前开启鉴权但未配置 EMAIL_PASSWORD")

        msg = EmailMessage()
        msg["Subject"] = title
        msg["From"] = sender
        msg["To"] = ", ".join(receivers)
        msg.set_content(message)

        context = ssl.create_default_context()
        port = int(settings.EMAIL_SMTP_PORT or 587)
        timeout = max(3, int(settings.EMAIL_TIMEOUT_SEC or 15))
        use_ssl = bool(settings.EMAIL_USE_SSL or port == 465)
        use_tls = bool(settings.EMAIL_USE_TLS and not use_ssl)

        if use_ssl:
            with smtplib.SMTP_SSL(
                server, port, timeout=timeout, context=context
            ) as smtp:
                if bool(settings.EMAIL_REQUIRE_AUTH):
                    smtp.login(sender, str(password or ""))
                smtp.send_message(msg)
        else:
            with smtplib.SMTP(server, port, timeout=timeout) as smtp:
                smtp.ehlo()
                if use_tls:
                    smtp.starttls(context=context)
                    smtp.ehlo()
                if bool(settings.EMAIL_REQUIRE_AUTH):
                    smtp.login(sender, str(password or ""))
                smtp.send_message(msg)
        return True

    async def _send_email(self, title: str, message: str) -> bool:
        try:
            ok = await asyncio.to_thread(self._send_email_sync, title, message)
            self._record_event(
                "email",
                "success" if ok else "failed",
                title,
                message,
                {
                    "smtp_server": settings.EMAIL_SMTP_SERVER,
                    "smtp_port": int(settings.EMAIL_SMTP_PORT or 587),
                    "use_tls": bool(settings.EMAIL_USE_TLS),
                    "use_ssl": bool(settings.EMAIL_USE_SSL),
                },
            )
            return bool(ok)
        except smtplib.SMTPAuthenticationError as e:
            self._record_event(
                "email",
                "failed",
                title,
                message,
                {"error": f"SMTP 认证失败: {e}", "smtp_server": settings.EMAIL_SMTP_SERVER},
            )
            return False
        except smtplib.SMTPConnectError as e:
            self._record_event(
                "email",
                "failed",
                title,
                message,
                {"error": f"SMTP 连接失败: {e}", "smtp_server": settings.EMAIL_SMTP_SERVER},
            )
            return False
        except Exception as e:
            self._record_event("email", "failed", title, message, {"error": str(e)})
            return False

    async def _send_wechat(self, title: str, message: str) -> bool:
        webhook = settings.WECHAT_WEBHOOK_URL
        if not webhook:
            return False

        payload = {
            "msgtype": "text",
            "text": {"content": f"【{title}】\n{message}"},
        }
        try:
            async with httpx.AsyncClient(timeout=12) as client:
                resp = await client.post(webhook, json=payload)
                ok = resp.status_code == 200
                self._record_event(
                    "wechat",
                    "success" if ok else "failed",
                    title,
                    message,
                    {"status_code": resp.status_code},
                )
                return ok
        except Exception as e:
            self._record_event("wechat", "failed", title, message, {"error": str(e)})
            return False

    @staticmethod
    def _feishu_sign(secret: str, timestamp: str) -> str:
        raw = f"{timestamp}\n{secret}".encode("utf-8")
        digest = hmac.new(raw, b"", digestmod=hashlib.sha256).digest()
        return base64.b64encode(digest).decode("utf-8")

    async def _send_feishu(self, title: str, message: str) -> bool:
        webhook = settings.FEISHU_BOT_WEBHOOK_URL
        if webhook:
            payload: Dict[str, Any] = {
                "msg_type": "text",
                "content": {
                    "text": f"【{title}】\n{message}",
                },
            }
            secret = str(settings.FEISHU_BOT_SECRET or "").strip()
            if secret:
                timestamp = str(int(time.time()))
                payload["timestamp"] = timestamp
                payload["sign"] = self._feishu_sign(secret=secret, timestamp=timestamp)

            try:
                async with httpx.AsyncClient(timeout=12) as client:
                    resp = await client.post(webhook, json=payload)
                    ok = False
                    body: Dict[str, Any] = {}
                    try:
                        body = resp.json() or {}
                    except Exception:
                        body = {}
                    if resp.status_code == 200:
                        code = body.get("code")
                        ok = code in (0, "0", None)
                    self._record_event(
                        "feishu",
                        "success" if ok else "failed",
                        title,
                        message,
                        {
                            "mode": "webhook",
                            "status_code": resp.status_code,
                            "code": body.get("code"),
                            "msg": body.get("msg"),
                        },
                    )
                    return ok
            except Exception as e:
                self._record_event(
                    "feishu",
                    "failed",
                    title,
                    message,
                    {"mode": "webhook", "error": str(e)},
                )
                return False

        return await self._send_feishu_app(title=title, message=message)

    async def _get_feishu_tenant_access_token(self) -> Optional[str]:
        now = time.time()
        if self._feishu_tenant_access_token and now < self._feishu_token_expire_at:
            return self._feishu_tenant_access_token

        app_id = str(settings.FEISHU_APP_ID or "").strip()
        app_secret = str(settings.FEISHU_APP_SECRET or "").strip()
        if not app_id or not app_secret:
            return None

        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        payload = {"app_id": app_id, "app_secret": app_secret}
        try:
            async with httpx.AsyncClient(timeout=12) as client:
                resp = await client.post(url, json=payload)
            data = resp.json() if resp.status_code == 200 else {}
            code = data.get("code")
            token = data.get("tenant_access_token")
            expire = int(data.get("expire", 0) or 0)
            if resp.status_code == 200 and code in (0, "0") and token:
                self._feishu_tenant_access_token = str(token)
                self._feishu_token_expire_at = time.time() + max(60, expire - 120)
                return self._feishu_tenant_access_token
            self._record_event(
                "feishu",
                "failed",
                "获取飞书 token 失败",
                "tenant_access_token/internal 返回异常",
                {
                    "mode": "app",
                    "status_code": resp.status_code,
                    "code": code,
                    "msg": data.get("msg"),
                },
            )
            return None
        except Exception as e:
            self._record_event(
                "feishu", "failed", "获取飞书 token 异常", str(e), {"mode": "app"}
            )
            return None

    async def _send_feishu_app(self, title: str, message: str) -> bool:
        token = await self._get_feishu_tenant_access_token()
        receive_id = str(settings.FEISHU_RECEIVE_ID or "").strip()
        receive_id_type = str(settings.FEISHU_RECEIVE_ID_TYPE or "chat_id").strip()
        if not token or not receive_id:
            return False

        url = f"https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type={receive_id_type}"
        text = f"【{title}】\n{message}"
        payload = {
            "receive_id": receive_id,
            "msg_type": "text",
            "content": json.dumps({"text": text}, ensure_ascii=False),
        }
        headers = {"Authorization": f"Bearer {token}"}
        try:
            async with httpx.AsyncClient(timeout=12) as client:
                resp = await client.post(url, json=payload, headers=headers)
            body: Dict[str, Any] = {}
            try:
                body = resp.json() if resp.status_code == 200 else {}
            except Exception:
                body = {}
            ok = resp.status_code == 200 and body.get("code") in (0, "0")
            self._record_event(
                "feishu",
                "success" if ok else "failed",
                title,
                message,
                {
                    "mode": "app",
                    "status_code": resp.status_code,
                    "code": body.get("code"),
                    "msg": body.get("msg"),
                    "receive_id_type": receive_id_type,
                },
            )
            return ok
        except Exception as e:
            self._record_event(
                "feishu", "failed", title, message, {"mode": "app", "error": str(e)}
            )
            return False

    async def send_message(
        self,
        title: str,
        message: str,
        channels: Optional[List[str]] = None,
    ) -> Dict[str, bool]:
        targets = channels or ["feishu"]
        result: Dict[str, bool] = {}
        for channel in targets:
            ch = channel.lower()
            if ch == "telegram":
                result[ch] = await self._send_telegram(title, message)
            elif ch == "email":
                result[ch] = await self._send_email(title, message)
            elif ch == "wechat":
                result[ch] = await self._send_wechat(title, message)
            elif ch in {"feishu", "lark"}:
                result[ch] = await self._send_feishu(title, message)
            else:
                result[ch] = False
        return result

    async def list_rules(self) -> List[Dict[str, Any]]:
        await self._ensure_loaded()
        values = list(self._rules.values())
        values.sort(key=lambda r: r.created_at)
        return [rule.to_dict() for rule in values]

    async def add_rule(
        self,
        name: str,
        rule_type: str,
        params: Dict[str, Any],
        enabled: bool = True,
        cooldown_seconds: int = 300,
    ) -> Dict[str, Any]:
        await self._ensure_loaded()

        now = datetime.now(timezone.utc)
        rule = AlertRule(
            id=str(uuid4()),
            name=name,
            rule_type=rule_type,
            params=params or {},
            enabled=enabled,
            cooldown_seconds=max(1, int(cooldown_seconds)),
            created_at=now,
            updated_at=now,
        )

        self._rules[rule.id] = rule
        await self._upsert_rule(rule)
        return rule.to_dict()

    async def update_rule(
        self, rule_id: str, updates: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        await self._ensure_loaded()
        rule = self._rules.get(rule_id)
        if not rule:
            return None

        if "name" in updates:
            rule.name = str(updates["name"])
        if "rule_type" in updates:
            rule.rule_type = str(updates["rule_type"])
        if "params" in updates and isinstance(updates["params"], dict):
            rule.params = updates["params"]
        if "enabled" in updates:
            rule.enabled = bool(updates["enabled"])
        if "cooldown_seconds" in updates and updates["cooldown_seconds"] is not None:
            rule.cooldown_seconds = max(1, int(updates["cooldown_seconds"]))

        rule.updated_at = datetime.now(timezone.utc)
        await self._upsert_rule(rule)
        return rule.to_dict()

    async def delete_rule(self, rule_id: str) -> bool:
        await self._ensure_loaded()
        existed = self._rules.pop(rule_id, None) is not None
        if existed:
            await self._delete_rule(rule_id)
        return existed

    def get_events(self, limit: int = 100) -> List[Dict[str, Any]]:
        limit = max(1, min(limit, 1000))
        return self._events[-limit:]

    def _cooldown_ok(self, rule: AlertRule) -> bool:
        if not rule.last_triggered_at:
            return True
        normalized_last_triggered = _normalize_datetime(rule.last_triggered_at)
        if normalized_last_triggered is None:
            return True
        rule.last_triggered_at = normalized_last_triggered
        elapsed = (
            datetime.now(timezone.utc) - normalized_last_triggered
        ).total_seconds()
        return elapsed >= max(1, rule.cooldown_seconds)

    @staticmethod
    def _altcoin_scan_for_rule(
        rule: AlertRule, context: Dict[str, Any]
    ) -> Dict[str, Any]:
        altcoin_ctx = context.get("altcoin") or {}
        scans = altcoin_ctx.get("scans") or {}
        params = rule.params or {}
        config_key = str(params.get("config_key") or "").strip()
        if config_key and config_key in scans:
            scan = scans.get(config_key) or {}
            return scan if isinstance(scan, dict) else {}
        return {}

    @staticmethod
    def _altcoin_row_for_rule(
        rule: AlertRule, context: Dict[str, Any]
    ) -> Dict[str, Any]:
        scan = NotificationManager._altcoin_scan_for_rule(rule, context)
        params = rule.params or {}
        symbol = _normalize_altcoin_symbol(params.get("symbol"))
        rows = scan.get("rows") or []
        for row in rows:
            if _normalize_altcoin_symbol((row or {}).get("symbol")) == symbol:
                return dict(row or {})
        return {}

    def _eval_altcoin_score_above(
        self, rule: AlertRule, context: Dict[str, Any]
    ) -> Optional[str]:
        params = rule.params or {}
        row = self._altcoin_row_for_rule(rule, context)
        if not row:
            return None
        score_key = str(params.get("score_key") or "layout").strip().lower()
        score_field = _altcoin_score_field(score_key)
        threshold = float(params.get("threshold", 0.0) or 0.0)
        score = float(row.get(score_field, 0.0) or 0.0)
        if threshold <= 0 or score < threshold:
            return None
        symbol = _normalize_altcoin_symbol(row.get("symbol") or params.get("symbol"))
        signal_state = str(row.get("signal_state") or "待跟踪").strip()
        rank_text = _altcoin_rank_text(row.get("rank"))
        return (
            f"山寨雷达 {symbol} 的 {score_key} 分数达到 {score:.4f}，"
            f"超过阈值 {threshold:.4f}；{rank_text}，状态 {signal_state}"
        )

    def _eval_altcoin_rank_top_n(
        self, rule: AlertRule, context: Dict[str, Any]
    ) -> Optional[str]:
        params = rule.params or {}
        scan = self._altcoin_scan_for_rule(rule, context)
        if not scan:
            return None
        symbol = _normalize_altcoin_symbol(params.get("symbol"))
        if not symbol:
            return None
        sort_by = str(params.get("sort_by") or "layout").strip().lower()
        rank_n = max(1, int(params.get("rank_n", 0) or 0))
        sort_indexes = (scan.get("sort_indexes") or {}).get(sort_by) or {}
        rank = int(sort_indexes.get(symbol, 0) or 0)
        if rank <= 0 or rank > rank_n:
            return None
        row = self._altcoin_row_for_rule(rule, context)
        score_field = _altcoin_score_field(
            sort_by, allowed_keys=ALTCOIN_RANK_SCORE_KEYS
        )
        score = float((row or {}).get(score_field, 0.0) or 0.0)
        signal_state = str((row or {}).get("signal_state") or "待跟踪").strip()
        return (
            f"山寨雷达 {symbol} 进入 {sort_by} 排序前 {rank_n}，"
            f"当前排名 {rank}，{score_field}={score:.4f}，状态 {signal_state}"
        )

    def _eval_rule(self, rule: AlertRule, context: Dict[str, Any]) -> Optional[str]:
        prices = context.get("prices", {}) or {}
        risk = context.get("risk_report", {}) or {}
        total_usd = float(context.get("total_usd", 0.0) or 0.0)
        position_count = int(context.get("position_count", 0) or 0)
        connected_exchanges = context.get("connected_exchanges", []) or []
        strategy_summary = context.get("strategy_summary", {}) or {}
        stale_running = strategy_summary.get("stale_running", []) or []
        stale_running_count = int(strategy_summary.get("stale_running_count", 0) or 0)
        running_count = int(strategy_summary.get("running_count", 0) or 0)
        running_names = {
            str(item.get("name", ""))
            for item in (strategy_summary.get("running") or [])
            if item.get("name")
        }

        rt = rule.rule_type
        p = rule.params or {}

        if rt == "price_above":
            symbol = str(p.get("symbol", ""))
            threshold = float(p.get("threshold", 0.0) or 0.0)
            price = float(prices.get(symbol, 0.0) or 0.0)
            if symbol and threshold > 0 and price >= threshold:
                return f"{symbol} 现价 {price:.6f} >= 阈值 {threshold:.6f}"

        if rt == "price_below":
            symbol = str(p.get("symbol", ""))
            threshold = float(p.get("threshold", 0.0) or 0.0)
            price = float(prices.get(symbol, 0.0) or 0.0)
            if symbol and threshold > 0 and price <= threshold:
                return f"{symbol} 现价 {price:.6f} <= 阈值 {threshold:.6f}"

        if rt == "daily_pnl_below_pct":
            threshold = float(p.get("threshold_pct", -2.0))
            daily_pct = float(
                (risk.get("equity") or {}).get("daily_pnl_ratio", 0.0) * 100
            )
            if daily_pct <= threshold:
                return f"日内收益率 {daily_pct:.2f}% <= 阈值 {threshold:.2f}%"

        if rt == "position_count_above":
            threshold = int(p.get("threshold", 0) or 0)
            if threshold > 0 and position_count >= threshold:
                return f"持仓数量 {position_count} >= 阈值 {threshold}"

        if rt == "risk_level_is":
            level = str(risk.get("risk_level", "low")).lower()
            targets = [str(x).lower() for x in (p.get("levels") or [])]
            if targets and level in targets:
                return f"当前风险等级 {level}"

        if rt == "trading_halted":
            halted = bool(risk.get("trading_halted", False))
            if halted:
                reason = str(risk.get("halt_reason", "") or "")
                return f"交易已熔断: {reason}"

        if rt == "equity_below":
            threshold = float(p.get("threshold_usd", 0.0) or 0.0)
            if threshold > 0 and total_usd <= threshold:
                return f"总资产 {total_usd:.2f} <= 阈值 {threshold:.2f}"

        if rt == "exchange_disconnected":
            watch = [str(x).lower() for x in (p.get("exchanges") or [])]
            connected = {str(x).lower() for x in connected_exchanges}
            missing = [x for x in watch if x not in connected]
            if missing:
                return f"交易所离线: {', '.join(missing)}"

        if rt == "stale_strategy_count_above":
            threshold = int(p.get("threshold", 0) or 0)
            if threshold > 0 and stale_running_count >= threshold:
                names = [
                    str(item.get("strategy", ""))
                    for item in stale_running[:8]
                    if item.get("strategy")
                ]
                return (
                    f"策略卡住数量 {stale_running_count} >= 阈值 {threshold}，"
                    f"异常策略: {', '.join(names) if names else 'unknown'}"
                )

        if rt == "running_strategy_count_below":
            threshold = int(p.get("threshold", 0) or 0)
            if threshold > 0 and running_count < threshold:
                return f"运行中策略数 {running_count} < 阈值 {threshold}"

        if rt == "strategy_not_running":
            targets = [
                str(x).strip() for x in (p.get("strategies") or []) if str(x).strip()
            ]
            missing = [name for name in targets if name not in running_names]
            if missing:
                return f"策略未运行: {', '.join(missing)}"

        if rt == "altcoin_score_above":
            return self._eval_altcoin_score_above(rule, context)

        if rt == "altcoin_rank_top_n":
            return self._eval_altcoin_rank_top_n(rule, context)

        return None

    async def evaluate_rules(self, context: Dict[str, Any]) -> Dict[str, Any]:
        await self._ensure_loaded()

        triggered: List[Dict[str, Any]] = []
        dirty_rules: List[AlertRule] = []

        for rule in self._rules.values():
            if not rule.enabled:
                continue
            if not self._cooldown_ok(rule):
                continue

            reason = self._eval_rule(rule, context)
            if not reason:
                continue

            title = f"告警规则触发: {rule.name}"
            channels = rule.params.get("channels") or ["feishu"]
            result = await self.send_message(
                title=title,
                message=reason,
                channels=channels,
            )

            rule.last_triggered_at = datetime.now(timezone.utc)
            rule.trigger_count += 1
            rule.updated_at = datetime.now(timezone.utc)
            dirty_rules.append(rule)

            triggered.append(
                {
                    "rule_id": rule.id,
                    "rule_name": rule.name,
                    "reason": reason,
                    "channels": channels,
                    "result": result,
                }
            )

        for rule in dirty_rules:
            await self._upsert_rule(rule)

        return {
            "triggered_count": len(triggered),
            "triggered": triggered,
        }


notification_manager = NotificationManager()
