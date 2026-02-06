from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


@dataclass(slots=True)
class Lease:
    task_id: str
    worker_name: str
    owner: str
    acquired_at: datetime
    expires_at: datetime

    def to_dict(self) -> dict[str, str]:
        return {
            "task_id": self.task_id,
            "worker_name": self.worker_name,
            "owner": self.owner,
            "acquired_at": self.acquired_at.isoformat(),
            "expires_at": self.expires_at.isoformat(),
        }


class LeaseManager:
    def __init__(self, lock_dir: Path):
        self.lock_dir = lock_dir
        self.lock_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    def _lock_path(self, task_id: str) -> Path:
        return self.lock_dir / f"{task_id}.lock"

    def try_acquire(self, *, task_id: str, worker_name: str, owner: str, lease_seconds: int) -> Lease | None:
        now = self._now()
        expires_at = now + timedelta(seconds=max(1, lease_seconds))
        lock_path = self._lock_path(task_id)

        if lock_path.exists() and not self._is_expired(lock_path, now):
            return None

        lease = Lease(task_id, worker_name, owner, now, expires_at)

        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(lease.to_dict(), f)
            return lease
        except FileExistsError:
            if self._is_expired(lock_path, now):
                self.break_lease(task_id)
                return self.try_acquire(
                    task_id=task_id,
                    worker_name=worker_name,
                    owner=owner,
                    lease_seconds=lease_seconds,
                )
            return None

    def release(self, task_id: str) -> None:
        lock_path = self._lock_path(task_id)
        if lock_path.exists():
            lock_path.unlink()

    def break_lease(self, task_id: str) -> None:
        self.release(task_id)

    def _is_expired(self, lock_path: Path, now: datetime) -> bool:
        try:
            raw = json.loads(lock_path.read_text(encoding="utf-8"))
            expires_at = datetime.fromisoformat(raw["expires_at"])
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
            return expires_at <= now
        except Exception:
            return True
