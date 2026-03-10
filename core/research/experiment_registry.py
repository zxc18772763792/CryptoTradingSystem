"""Persistent registry helpers for AI proposals, experiments, candidates, and lifecycle."""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Dict, Generic, List, Optional, Type, TypeVar

from core.ai.proposal_schemas import ResearchProposal
from core.research.experiment_schemas import (
    ExperimentRun,
    ExperimentSpec,
    LifecycleRecord,
    StrategyCandidate,
)


ModelT = TypeVar("ModelT")


class _JsonRegistry(Generic[ModelT]):
    def __init__(self, path: Path, root_key: str, model_cls: Type[ModelT], key_field: str):
        self.path = Path(path)
        self.root_key = str(root_key)
        self.model_cls = model_cls
        self.key_field = str(key_field)
        self._cache: Optional[Dict[str, ModelT]] = None
        self._lock = threading.Lock()  # D: concurrent safety

    def _load(self) -> Dict[str, ModelT]:
        if self._cache is not None:
            return self._cache
        if not self.path.exists():
            self._cache = {}
            return self._cache
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        rows = payload.get(self.root_key) if isinstance(payload, dict) else []
        items: Dict[str, ModelT] = {}
        for row in rows or []:
            item = self.model_cls.model_validate(row)
            items[str(getattr(item, self.key_field))] = item
        self._cache = items
        return items

    def _flush(self) -> None:
        """D: Atomic write — write to .tmp then rename (same filesystem)."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        rows = [item.model_dump(mode="json") for item in self.list(limit=None)]
        content = json.dumps({self.root_key: rows}, ensure_ascii=False, indent=2)
        tmp_path = self.path.with_suffix(".tmp")
        tmp_path.write_text(content, encoding="utf-8")
        # os.replace is atomic on the same filesystem on both POSIX and Windows
        os.replace(str(tmp_path), str(self.path))

    def save(self, item: ModelT) -> ModelT:
        with self._lock:
            items = self._load()
            items[str(getattr(item, self.key_field))] = item
            self._flush()
        return item

    def get(self, item_id: str) -> Optional[ModelT]:
        with self._lock:
            return self._load().get(str(item_id))

    def delete(self, item_id: str) -> bool:
        with self._lock:
            items = self._load()
            key = str(item_id)
            if key not in items:
                return False
            del items[key]
            self._flush()
        return True

    def delete_many(self, item_ids: List[str]) -> int:
        with self._lock:
            items = self._load()
            removed = 0
            for raw in item_ids or []:
                key = str(raw)
                if key in items:
                    del items[key]
                    removed += 1
            if removed > 0:
                self._flush()
        return removed

    def list(self, limit: int | None = 50) -> List[ModelT]:
        rows = list(self._load().values())
        rows.sort(
            key=lambda item: (
                getattr(item, "updated_at", None) or getattr(item, "created_at", None) or getattr(item, "ts", None),
                getattr(item, "created_at", None) or getattr(item, "ts", None),
            ),
            reverse=True,
        )
        if limit is None:
            return rows
        return rows[: max(0, int(limit))]


class ProposalRegistry(_JsonRegistry[ResearchProposal]):
    def __init__(self, path: Path):
        super().__init__(path=path, root_key="proposals", model_cls=ResearchProposal, key_field="proposal_id")


class ExperimentRegistry(_JsonRegistry[ExperimentSpec]):
    def __init__(self, path: Path):
        super().__init__(path=path, root_key="experiments", model_cls=ExperimentSpec, key_field="experiment_id")


class ExperimentRunRegistry(_JsonRegistry[ExperimentRun]):
    def __init__(self, path: Path):
        super().__init__(path=path, root_key="runs", model_cls=ExperimentRun, key_field="run_id")

    def list_for_experiment(self, experiment_id: str, limit: int | None = 100) -> List[ExperimentRun]:
        rows = [row for row in self._load().values() if row.experiment_id == str(experiment_id)]
        rows.sort(key=lambda item: item.started_at or item.finished_at, reverse=True)
        if limit is None:
            return rows
        return rows[: max(0, int(limit))]


class CandidateRegistry(_JsonRegistry[StrategyCandidate]):
    def __init__(self, path: Path):
        super().__init__(path=path, root_key="candidates", model_cls=StrategyCandidate, key_field="candidate_id")


class LifecycleRegistry:
    def __init__(self, path: Path):
        self.path = Path(path)
        self._cache: Optional[List[LifecycleRecord]] = None
        self._lock = threading.Lock()  # D: concurrent safety

    def _load(self) -> List[LifecycleRecord]:
        if self._cache is not None:
            return self._cache
        if not self.path.exists():
            self._cache = []
            return self._cache
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        rows = payload.get("lifecycle") if isinstance(payload, dict) else []
        self._cache = [LifecycleRecord.model_validate(row) for row in rows or []]
        return self._cache

    def _flush(self) -> None:
        """D: Atomic write — write to .tmp then rename."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        rows = [item.model_dump(mode="json") for item in self.list(limit=None)]
        content = json.dumps({"lifecycle": rows}, ensure_ascii=False, indent=2)
        tmp_path = self.path.with_suffix(".tmp")
        tmp_path.write_text(content, encoding="utf-8")
        os.replace(str(tmp_path), str(self.path))

    def append(self, item: LifecycleRecord) -> LifecycleRecord:
        with self._lock:
            rows = self._load()
            rows.append(item)
            self._flush()
        return item

    def list(self, limit: int | None = 200) -> List[LifecycleRecord]:
        rows = sorted(self._load(), key=lambda item: item.ts, reverse=True)
        if limit is None:
            return rows
        return rows[: max(0, int(limit))]

    def list_for_object(self, object_type: str, object_id: str, limit: int | None = 200) -> List[LifecycleRecord]:
        rows = [
            item
            for item in self._load()
            if item.object_type == str(object_type) and item.object_id == str(object_id)
        ]
        rows.sort(key=lambda item: item.ts, reverse=True)
        if limit is None:
            return rows
        return rows[: max(0, int(limit))]

    def delete_for_object(self, object_type: str, object_id: str) -> int:
        with self._lock:
            rows = self._load()
            before = len(rows)
            target_type = str(object_type)
            target_id = str(object_id)
            rows[:] = [item for item in rows if not (item.object_type == target_type and item.object_id == target_id)]
            removed = before - len(rows)
            if removed > 0:
                self._flush()
        return removed

    def delete_for_objects(self, object_type: str, object_ids: List[str]) -> int:
        ids = {str(item) for item in (object_ids or []) if str(item)}
        if not ids:
            return 0
        with self._lock:
            rows = self._load()
            before = len(rows)
            target_type = str(object_type)
            rows[:] = [item for item in rows if not (item.object_type == target_type and item.object_id in ids)]
            removed = before - len(rows)
            if removed > 0:
                self._flush()
        return removed
