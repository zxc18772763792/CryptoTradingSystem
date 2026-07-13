"""Multi-account profile management for paper/live routing."""
from __future__ import annotations

import json
import os
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar
from uuid import uuid4

from loguru import logger

from config.settings import settings


MutationResult = TypeVar("MutationResult")


def _replace_with_retry(source: Path, target: Path) -> None:
    """Atomically publish *source*, tolerating transient Windows file locks."""
    delay = 0.02
    for attempt in range(7):
        try:
            os.replace(str(source), str(target))
            return
        except OSError:
            if attempt == 6:
                raise
            time.sleep(delay)
            delay *= 2


@contextmanager
def _interprocess_file_lock(path: Path):
    """Dependency-free advisory lock shared by AccountManager processes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a+b") as handle:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            try:
                yield
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


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
        self._thread_lock = threading.RLock()
        self._accounts: Dict[str, TradingAccount] = {}
        self._file = Path(settings.BASE_DIR) / "data" / "config" / "accounts.json"
        self._file_signature: Optional[Tuple[str, int, int]] = None
        self._file.parent.mkdir(parents=True, exist_ok=True)
        self._load()

    @staticmethod
    def _normalize_mode(mode: Any, default: str = "paper") -> str:
        text = str(mode or default).strip().lower()
        return "live" if text == "live" else "paper"

    def _runtime_lock(self) -> threading.RLock:
        # Some focused tests construct the manager with ``__new__``.
        lock = getattr(self, "_thread_lock", None)
        if lock is None:
            lock = threading.RLock()
            self._thread_lock = lock
        return lock

    def _ensure_default(self, accounts: Optional[Dict[str, TradingAccount]] = None) -> bool:
        target = self._accounts if accounts is None else accounts
        if "main" not in target:
            target["main"] = TradingAccount(
                account_id="main",
                name="主账户",
                exchange="binance",
                mode="paper",
                parent_account_id=None,
                enabled=True,
            )
            return True
        return False

    @property
    def _lock_file(self) -> Path:
        return self._file.with_suffix(self._file.suffix + ".lock")

    @property
    def _backup_file(self) -> Path:
        return self._file.with_suffix(self._file.suffix + ".bak")

    def _signature(self) -> Optional[Tuple[str, int, int]]:
        try:
            stat = self._file.stat()
        except OSError:
            return None
        return (str(self._file.resolve()), int(stat.st_mtime_ns), int(stat.st_size))

    @staticmethod
    def _decode_accounts(data: Any) -> Dict[str, TradingAccount]:
        rows = data.get("accounts", []) if isinstance(data, dict) else []
        loaded: Dict[str, TradingAccount] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            aid = str(row.get("account_id") or "").strip()
            if not aid:
                continue
            loaded[aid] = TradingAccount(
                account_id=aid,
                name=str(row.get("name") or aid),
                exchange=str(row.get("exchange") or "binance").lower(),
                mode=AccountManager._normalize_mode(row.get("mode"), default="paper"),
                parent_account_id=row.get("parent_account_id"),
                enabled=bool(row.get("enabled", True)),
                created_at=str(row.get("created_at") or datetime.now(timezone.utc).isoformat()),
                updated_at=str(row.get("updated_at") or datetime.now(timezone.utc).isoformat()),
                metadata=dict(row.get("metadata") or {}),
            )
        return loaded

    def _read_accounts_locked(self) -> Tuple[Dict[str, TradingAccount], bool]:
        """Load a complete snapshot; fall back to the last-known-good copy."""
        errors: List[str] = []
        for candidate in (self._file, self._backup_file):
            if not candidate.exists():
                continue
            for attempt in range(3):
                try:
                    payload = json.loads(candidate.read_text(encoding="utf-8"))
                    if not isinstance(payload, dict) or not isinstance(payload.get("accounts", []), list):
                        raise ValueError("accounts payload must contain a list")
                    return self._decode_accounts(payload), candidate == self._backup_file
                except Exception as exc:
                    if attempt < 2:
                        time.sleep(0.02 * (attempt + 1))
                        continue
                    errors.append(f"{candidate.name}: {exc}")
        if not self._file.exists() and not self._backup_file.exists():
            return {}, False
        # Fail closed instead of silently replacing live routing/credential
        # metadata with a default account.
        raise RuntimeError("unable to load accounts configuration; " + "; ".join(errors))

    @staticmethod
    def _serialize_accounts(accounts: Dict[str, TradingAccount]) -> bytes:
        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "accounts": [asdict(accounts[key]) for key in sorted(accounts)],
        }
        return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")

    def _atomic_write(self, target: Path, content: bytes) -> None:
        tmp = target.with_name(f"{target.name}.{os.getpid()}.{uuid4().hex}.tmp")
        try:
            with open(tmp, "wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            _replace_with_retry(tmp, target)
        finally:
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass

    def _save_accounts_locked(self, accounts: Dict[str, TradingAccount]) -> None:
        content = self._serialize_accounts(accounts)
        self._atomic_write(self._file, content)
        self._atomic_write(self._backup_file, content)
        self._file_signature = self._signature()

    def _load(self) -> None:
        with self._runtime_lock(), _interprocess_file_lock(self._lock_file):
            loaded, recovered = self._read_accounts_locked()
            changed = self._ensure_default(loaded)
            self._accounts = loaded
            if changed or recovered or not self._file.exists():
                if recovered:
                    logger.warning("Recovered accounts.json from last-known-good backup")
                self._save_accounts_locked(loaded)
            else:
                self._file_signature = self._signature()

    def _save(self) -> None:
        with self._runtime_lock(), _interprocess_file_lock(self._lock_file):
            self._save_accounts_locked(self._accounts)

    def _refresh_if_changed(self) -> None:
        if not hasattr(self, "_file"):
            return
        if self._signature() == getattr(self, "_file_signature", None):
            return
        with self._runtime_lock(), _interprocess_file_lock(self._lock_file):
            latest, recovered = self._read_accounts_locked()
            self._ensure_default(latest)
            self._accounts = latest
            if recovered:
                logger.warning("Recovered accounts.json from last-known-good backup")
                self._save_accounts_locked(latest)
            else:
                self._file_signature = self._signature()

    def _mutate(self, callback: Callable[[Dict[str, TradingAccount]], MutationResult]) -> MutationResult:
        with self._runtime_lock(), _interprocess_file_lock(self._lock_file):
            latest, recovered = self._read_accounts_locked()
            self._ensure_default(latest)
            result = callback(latest)
            self._save_accounts_locked(latest)
            self._accounts = latest
            if recovered:
                logger.warning("Updated accounts configuration after backup recovery")
            return result

    def list_accounts(self) -> List[Dict[str, Any]]:
        self._refresh_if_changed()
        return [asdict(v) for v in sorted(self._accounts.values(), key=lambda x: x.account_id)]

    def get_account(self, account_id: str) -> Optional[Dict[str, Any]]:
        self._refresh_if_changed()
        item = self._accounts.get(account_id)
        return asdict(item) if item else None

    def requires_live_connector_isolation(self, account_id: Optional[str]) -> bool:
        self._refresh_if_changed()
        aid = str(account_id or "").strip()
        if not aid:
            return False
        item = self._accounts.get(aid)
        if not item or not item.enabled:
            return False
        if self._normalize_mode(item.mode, default="paper") != "live":
            return False
        metadata = dict(item.metadata or {})
        explicit_connector_isolation = bool(
            metadata.get("require_live_credentials")
            or metadata.get("require_connector_isolation")
        )
        if aid == "main":
            return explicit_connector_isolation
        if explicit_connector_isolation:
            return True
        parent_account_id = str(getattr(item, "parent_account_id", "") or "").strip()
        if parent_account_id:
            return False
        return bool(metadata.get("isolated", True))

    def get_exchange_credentials(
        self,
        account_id: Optional[str],
        exchange: Optional[str] = None,
    ) -> Dict[str, Any]:
        self._refresh_if_changed()
        aid = str(account_id or "main").strip() or "main"
        item = self._accounts.get(aid)
        exchange_name = str(
            exchange or getattr(item, "exchange", None) or "binance"
        ).strip().lower() or "binance"

        metadata = dict(getattr(item, "metadata", {}) or {})
        credentials: Dict[str, Any] = {}

        for container_key in ("credentials", "exchange_credentials", "exchanges"):
            container = metadata.get(container_key)
            if not isinstance(container, dict):
                continue
            exchange_payload = container.get(exchange_name)
            if isinstance(exchange_payload, dict):
                credentials.update(dict(exchange_payload))

        legacy_key_map = {
            "api_key": (
                f"{exchange_name}_api_key",
                f"{exchange_name}_key",
                "api_key",
            ),
            "api_secret": (
                f"{exchange_name}_api_secret",
                f"{exchange_name}_secret",
                "api_secret",
                "secret",
            ),
            "passphrase": (
                f"{exchange_name}_passphrase",
                "passphrase",
            ),
            "default_type": (
                f"{exchange_name}_default_type",
                "default_type",
                "market_type",
            ),
            "sandbox": (
                f"{exchange_name}_sandbox",
                "sandbox",
            ),
            "proxy": (
                f"{exchange_name}_proxy",
                "proxy",
            ),
        }
        for target_key, source_keys in legacy_key_map.items():
            if target_key in credentials:
                continue
            for source_key in source_keys:
                value = metadata.get(source_key)
                if value not in (None, ""):
                    credentials[target_key] = value
                    break

        allow_parent_fallback = not self.requires_live_connector_isolation(aid)
        parent_account_id = str(getattr(item, "parent_account_id", "") or "").strip()
        if not credentials and allow_parent_fallback and parent_account_id and parent_account_id != aid:
            parent_credentials = self.get_exchange_credentials(parent_account_id, exchange_name)
            if parent_credentials:
                return parent_credentials

        if not credentials and aid == "main":
            settings_map = {
                "binance": {
                    "api_key": settings.BINANCE_API_KEY,
                    "api_secret": settings.BINANCE_API_SECRET,
                },
                "okx": {
                    "api_key": settings.OKX_API_KEY,
                    "api_secret": settings.OKX_API_SECRET,
                    "passphrase": settings.OKX_PASSPHRASE,
                },
                "gate": {
                    "api_key": settings.GATE_API_KEY,
                    "api_secret": settings.GATE_API_SECRET,
                },
                "bybit": {
                    "api_key": settings.BYBIT_API_KEY,
                    "api_secret": settings.BYBIT_API_SECRET,
                },
            }
            credentials.update(settings_map.get(exchange_name, {}))

        normalized: Dict[str, Any] = {}
        for key in ("api_key", "api_secret", "passphrase", "default_type", "proxy"):
            value = credentials.get(key)
            if value not in (None, ""):
                normalized[key] = str(value).strip()
        if "sandbox" in credentials:
            normalized["sandbox"] = bool(credentials.get("sandbox"))
        return normalized

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
        def apply(accounts: Dict[str, TradingAccount]) -> Dict[str, Any]:
            if aid in accounts:
                raise ValueError(f"账户 {aid} 已存在")
            now = datetime.now(timezone.utc).isoformat()
            accounts[aid] = TradingAccount(
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
            return asdict(accounts[aid])

        return self._mutate(apply)

    def update_account(self, account_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        def apply(accounts: Dict[str, TradingAccount]) -> Dict[str, Any]:
            item = accounts.get(account_id)
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
            return asdict(item)

        return self._mutate(apply)

    def delete_account(self, account_id: str) -> bool:
        if account_id == "main":
            raise ValueError("主账户不可删除")
        def apply(accounts: Dict[str, TradingAccount]) -> bool:
            if account_id not in accounts:
                return False
            del accounts[account_id]
            return True

        return self._mutate(apply)

    def resolve_exchange(self, account_id: Optional[str], default_exchange: str) -> str:
        self._refresh_if_changed()
        if not account_id:
            return default_exchange
        item = self._accounts.get(account_id)
        if not item or not item.enabled:
            return default_exchange
        return item.exchange or default_exchange

    def get_account_mode(self, account_id: Optional[str], default: str = "paper") -> str:
        self._refresh_if_changed()
        if account_id:
            item = self._accounts.get(str(account_id))
            if item and item.enabled:
                return self._normalize_mode(item.mode, default=default)
        return self._normalize_mode(default, default="paper")

    def is_enabled(self, account_id: Optional[str]) -> bool:
        self._refresh_if_changed()
        if not account_id:
            return True
        item = self._accounts.get(account_id)
        return bool(item and item.enabled)

    def set_mode(self, account_id: str, mode: str) -> bool:
        aid = str(account_id or "").strip()
        if not aid:
            raise ValueError("account_id must not be empty")
        def apply(accounts: Dict[str, TradingAccount]) -> bool:
            item = accounts.get(aid)
            if not item:
                raise ValueError(f"account not found: {aid}")
            target = self._normalize_mode(mode, default=item.mode)
            if item.mode == target:
                return False
            item.mode = target
            item.updated_at = datetime.now(timezone.utc).isoformat()
            return True

        return self._mutate(apply)

    def set_mode_for_all(self, mode: str) -> int:
        target = self._normalize_mode(mode, default="paper")
        def apply(accounts: Dict[str, TradingAccount]) -> int:
            updated = 0
            now = datetime.now(timezone.utc).isoformat()
            for item in accounts.values():
                if item.mode != target:
                    item.mode = target
                    item.updated_at = now
                    updated += 1
            return updated

        return self._mutate(apply)

    def set_mode_for_auto_strategy_accounts(self, mode: str) -> int:
        target = self._normalize_mode(mode, default="paper")
        def apply(accounts: Dict[str, TradingAccount]) -> int:
            updated = 0
            now = datetime.now(timezone.utc).isoformat()
            for item in accounts.values():
                if item.account_id == "main":
                    continue
                metadata = dict(item.metadata or {})
                if not metadata.get("auto_created") or not metadata.get("strategy_name"):
                    continue
                if item.mode == target and metadata.get("runtime_mode") == target:
                    continue
                item.mode = target
                metadata["runtime_mode"] = target
                item.metadata = metadata
                item.updated_at = now
                updated += 1
            return updated

        return self._mutate(apply)


account_manager = AccountManager()
