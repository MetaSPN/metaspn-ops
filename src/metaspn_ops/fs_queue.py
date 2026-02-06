from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from .lease import LeaseManager
from .scheduler import TaskScheduler
from .types import Result, RunRecord, Task


def _ts_for_name(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc).replace(microsecond=0)
    return dt.strftime("%Y-%m-%dT%H%M%SZ")


def _safe_task_id(task_id: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in task_id)


class FilesystemQueue:
    def __init__(
        self,
        *,
        workspace: str | Path,
        worker_name: str,
        scheduler: TaskScheduler | None = None,
    ):
        self.workspace = Path(workspace)
        self.worker_name = worker_name
        self.scheduler = scheduler or TaskScheduler()

        self.inbox_dir = self.workspace / "inbox" / worker_name
        self.outbox_dir = self.workspace / "outbox" / worker_name
        self.runs_dir = self.workspace / "runs" / worker_name
        self.deadletter_dir = self.workspace / "deadletter" / worker_name
        self.lock_dir = self.workspace / "locks" / worker_name

        for folder in [
            self.inbox_dir,
            self.outbox_dir,
            self.runs_dir,
            self.deadletter_dir,
            self.lock_dir,
        ]:
            folder.mkdir(parents=True, exist_ok=True)

        self.leases = LeaseManager(self.lock_dir)
        self._io_lock = threading.RLock()

    def enqueue_task(self, task: Task, *, scheduled_for: datetime | None = None) -> Path:
        ts = scheduled_for or datetime.now(timezone.utc)
        name = f"{_ts_for_name(ts)}__t_{_safe_task_id(task.task_id)}.json"
        path = self.inbox_dir / name
        with self._io_lock:
            self._write_json(path, task.to_dict())
        return path

    def lease_next_task(self, *, owner: str, lease_seconds: int) -> tuple[Task, Path] | None:
        now_name = _ts_for_name(datetime.now(timezone.utc))
        for path in sorted(self.inbox_dir.glob("*.json")):
            scheduled_name = path.name.split("__", 1)[0]
            if scheduled_name > now_name:
                continue
            raw = self._read_json(path)
            task = Task.from_dict(raw)
            lease = self.leases.try_acquire(
                task_id=task.task_id,
                worker_name=self.worker_name,
                owner=owner,
                lease_seconds=lease_seconds,
            )
            if lease is not None:
                return task, path
        return None

    def ack_task(self, leased_path: Path) -> None:
        with self._io_lock:
            if leased_path.exists():
                raw = self._read_json(leased_path)
                task_id = raw.get("task_id")
                leased_path.unlink()
                if task_id:
                    self.leases.release(str(task_id))

    def fail_task(self, leased_path: Path, task: Task, error: str) -> None:
        task.attempt_count += 1
        can_retry = task.attempt_count < task.max_attempts
        with self._io_lock:
            if leased_path.exists():
                leased_path.unlink()
            if can_retry:
                retry_at = self.scheduler.next_retry_at(attempt_count=task.attempt_count)
                self.enqueue_task(task, scheduled_for=retry_at)
            else:
                deadletter_name = f"{_ts_for_name(datetime.now(timezone.utc))}__t_{_safe_task_id(task.task_id)}.json"
                self._write_json(
                    self.deadletter_dir / deadletter_name,
                    {
                        "task": task.to_dict(),
                        "final_error": error,
                        "deadlettered_at": datetime.now(timezone.utc).isoformat(),
                    },
                )
            self.leases.release(task.task_id)

    def write_result(self, result: Result) -> Path:
        name = f"{_ts_for_name(datetime.now(timezone.utc))}__r_{_safe_task_id(result.task_id)}__{uuid4().hex[:8]}.json"
        path = self.outbox_dir / name
        with self._io_lock:
            self._write_json(path, result.to_dict())
        return path

    def write_run_record(self, record: RunRecord) -> Path:
        name = f"{_ts_for_name(datetime.now(timezone.utc))}__run_{record.run_id}.json"
        path = self.runs_dir / name
        with self._io_lock:
            self._write_json(path, record.to_dict())
        return path

    def stats(self) -> dict[str, int]:
        return {
            "inbox": len(list(self.inbox_dir.glob("*.json"))),
            "outbox": len(list(self.outbox_dir.glob("*.json"))),
            "runs": len(list(self.runs_dir.glob("*.json"))),
            "deadletter": len(list(self.deadletter_dir.glob("*.json"))),
            "locks": len(list(self.lock_dir.glob("*.lock"))),
        }

    def deadletter_items(self) -> list[Path]:
        return sorted(self.deadletter_dir.glob("*.json"))

    def retry_deadletter(self, *, task_id: str | None = None) -> int:
        retried = 0
        for item in self.deadletter_items():
            raw = self._read_json(item)
            task = Task.from_dict(raw["task"])
            if task_id and task.task_id != task_id:
                continue
            task.attempt_count = 0
            self.enqueue_task(task)
            item.unlink()
            retried += 1
        return retried

    @staticmethod
    def _read_json(path: Path) -> dict:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def _write_json(path: Path, payload: dict) -> None:
        with path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=True)
