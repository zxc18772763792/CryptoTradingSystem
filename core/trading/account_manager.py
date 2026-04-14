"""Multi-account profile management for paper/live routing."""
from __future__ import annotations

import json
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger

from config.settings import settings


@dataclass
class TradingAccount:
    account_id: str
    name: str
    exchange: str
    mode: str = "paper"
    parent_account_id: Optional[str] = None
    enabled: bool = True
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    metadata: Dict[str, Any] = field(default_factory=dict)


class AccountManager:
    def __init__(self) -> None:
        self._accounts: Dict[str, TradingAccount] = {}
        self._file = Path(settings.BASE_DIR) / "data" / "config" / "accounts.json"
        self._file.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_default()
        self._load()

    @staticmethod
    def _normalize_mode(mode: Any, default: str = "paper") -> str:
        text = str(mode or default).strip().lower()
        return "live" if text == "live" else "paper"

    def _ensure_default(self) -> None:
        if "main" not in self._accounts:
            self._accounts["main"] = TradingAccount(
                account_id="main",
                name="主账户",
                exchange="binance",
                mode="paper",
                parent_account_id=None,
                enabled=True,
            )

    def _load(self) -> None:
        if not self._file.exists():
            self._save()
            return
        try:
            data = json.loads(self._file.read_text(encoding="utf-8"))
            rows = data.get("accounts", []) if isinstance(data, dict) else []
            loaded: Dict[str, TradingAccount] = {}
            for row in rows:
                aid = str(row.get("account_id") or "").strip()
                if not aid:
                    continue
                loaded[aid] = TradingAccount(
                    account_id=aid,
                    name=str(row.get("name") or aid),
                    exchange=str(row.get("exchange") or "binance").lower(),
                    mode=self._normalize_mode(row.get("mode"), default="paper"),
                    parent_account_id=row.get("parent_account_id"),
                    enabled=bool(row.get("enabled", True)),
                    created_at=str(row.get("created_at") or datetime.now(timezone.utc).isoformat()),
                    updated_at=str(row.get("updated_at") or datetime.now(timezone.utc).isoformat()),
                    metadata=dict(row.get("metadata") or {}),
                )
            self._accounts = loaded
        except Exception as e:
            logger.warning(f"Failed to load accounts.json: {e}")
        self._ensure_default()
        self._save()

    def _save(self) -> None:
        try:
            payload = {
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "accounts": [asdict(v) for v in self._accounts.values()],
            }
            self._file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as e:
            logger.error(f"Failed to save accounts.json: {e}")

    def list_accounts(self) -> List[Dict[str, Any]]:
        return [asdict(v) for v in sorted(self._accounts.values(), key=lambda x: x.account_id)]

    def get_account(self, account_id: str) -> Optional[Dict[str, Any]]:
        item = self._accounts.get(account_id)
        return asdict(item) if item else None

    def create_account(
        self,
        account_id: str,
        name: str,
        exchange: str,
        mode: str = "paper",
        parent_account_id: Optional[str] = None,
        enabled: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        aid = str(account_id or "").strip()
        if not aid:
            raise ValueError("account_id 不能为空")
        if aid in self._accounts:
            raise ValueError(f"账户 {aid} 已存在")
        now = datetime.now(timezone.utc).isoformat()
        self._accounts[aid] = TradingAccount(
            account_id=aid,
            name=str(name or aid),
            exchange=str(exchange or "binance").lower(),
            mode=self._normalize_mode(mode, default="paper"),
            parent_account_id=parent_account_id,
            enabled=bool(enabled),
            created_at=now,
            updated_at=now,
            metadata=dict(metadata or {}),
        )
        self._save()
        return asdict(self._accounts[aid])

    def update_account(self, account_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        item = self._accounts.get(account_id)
        if not item:
            raise ValueError("账户不存在")
        if "name" in updates:
            item.name = str(updates["name"] or item.name)
        if "exchange" in updates:
            item.exchange = str(updates["exchange"] or item.exchange).lower()
        if "mode" in updates:
            item.mode = self._normalize_mode(updates["mode"], default=item.mode)
        if "parent_account_id" in updates:
            item.parent_account_id = updates["parent_account_id"]
        if "enabled" in updates:
            item.enabled = bool(updates["enabled"])
        if "metadata" in updates and isinstance(updates["metadata"], dict):
            merged = dict(item.metadata)
            merged.update(updates["metadata"])
            item.metadata = merged
        item.updated_at = datetime.now(timezone.utc).isoformat()
        self._save()
        return asdict(item)

    def delete_account(self, account_id: str) -> bool:
        if account_id == "main":
            raise ValueError("主账户不可删除")
        if account_id not in self._accounts:
            return False
        del self._accounts[account_id]
        self._save()
        return True

    def resolve_exchange(self, account_id: Optional[str], default_exchange: str) -> str:
        if not account_id:
            return default_exchange
        item = self._accounts.get(account_id)
        if not item or not item.enabled:
            return default_exchange
        return item.exchange or default_exchange

    def get_account_mode(self, account_id: Optional[str], default: str = "paper") -> str:
        if account_id:
            item = self._accounts.get(str(account_id))
            if item and item.enabled:
                return self._normalize_mode(item.mode, default=default)
        return self._normalize_mode(default, default="paper")

    def is_enabled(self, account_id: Optional[str]) -> bool:
        if not account_id:
            return True
        item = self._accounts.get(account_id)
        return bool(item and item.enabled)

    def set_mode(self, account_id: str, mode: str) -> bool:
        aid = str(account_id or "").strip()
        if not aid:
            raise ValueError("account_id must not be empty")
        item = self._accounts.get(aid)
        if not item:
            raise ValueError(f"account not found: {aid}")
        target = self._normalize_mode(mode, default=item.mode)
        if item.mode == target:
            return False
        item.mode = target
        item.updated_at = datetime.now(timezone.utc).isoformat()
        self._save()
        return True

    def set_mode_for_all(self, mode: str) -> int:
        target = self._normalize_mode(mode, default="paper")
        updated = 0
        now = datetime.now(timezone.utc).isoformat()
        for item in self._accounts.values():
            if item.mode != target:
                item.mode = target
                item.updated_at = now
                updated += 1
        if updated > 0:
            self._save()
        return updated


account_manager = AccountManager()
